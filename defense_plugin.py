# -*- coding: utf-8 -*-
"""
# MODIFIED: defense_plugin.py
순수 롱(SOXL) 전용 '오버나이트 지층 원장(soxl_stratum_ledger.json) ↔ 토스 실보유 잔고(/api/v1/holdings)
무결성 화해(Case 29 유령 잔고 방어 · Case 58 재기동/수동매매 정합성 복원 · Case 60 수동 전량청산 시 기억저가 리셋 ·
Case 61 15초 인덱싱 시차 그레이스 쉴드 · Case 63 부분 수동매매 시 리앵커링 단가 이점 보존) ·
04:00 ~ 04:06 EST 프리장 초기 호가 안정화 동기화 ·
16:00 ~ 16:59 EST 정규장 마감 일일 결산 리포트 및 오버나이트 지층 안전 보존(강제 MOC 없음)' 코어 플러그인 (Step 03 / 04)
- MODIFIED (제3헌법): 구버전 08:15 EST 거시지표 컷오프, 15:00 EST 정규장 VWAP 덤핑 탈출,
  15:59 EST 제로-오버나이트 강제 손절 MOC 및 NQ=F/QQQ 매크로 손절 덤핑 데드코드를 100% 영구 소각
- MODIFIED (Case 58, 60, 63): 프로세스 재기동 또는 운용자의 토스 앱 수동 매수/매도 발생 시
  수동 전량 매도(0주)는 cycle_min_buy_price = 0.0 까지 완벽히 리셋하고, 부분 수동 매매 시에는
  기존 리앵커링 누적 차감 크레딧과 장부단가(book_price)를 훼손하지 않고 실보유 수량(holdings_qty) 및
  실투입 원금(actual_cost_usd)을 원자적으로 화해(Reconcile)
- MODIFIED (Case 64): 10분 고정 시드보호 매수 게이트 단일화에 맞춰 알림 및 일일 롤오버 상태 동기화
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 64 완벽 준수
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import (
    StratumBucket,
    buckets_from_dicts,
    buckets_to_dicts,
    calc_sell_realized_pnl,
    summarize_stratum_portfolio,
)
from quant_indicators_plugin import (
    get_logical_trading_date_str,
    is_daily_settlement_window,
    is_premarket_stabilization_window,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import update_exit_snapshot_on_hold
from sniper_plugin import is_within_auto_fill_grace_shield
from toss_api import (
    EST_TZ,
    LOCKED_BUY_INTERVAL_MIN,
    TARGET_SYMBOL,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.defense_plugin")


def _get_live_soxl_price(
    context: Any,
    prices: Dict[str, float],
    holdings: Dict[str, Dict[str, Any]],
    now_est: datetime,
) -> float:
    lp = safe_float((prices or {}).get(TARGET_SYMBOL), 0.0)
    if lp > 0.0:
        return lp
    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = (
        raw_cache.get(TARGET_SYMBOL) if isinstance(raw_cache, dict) else None
    )
    if isinstance(base_met, dict):
        met = refresh_metrics_with_live_tick(base_met, 0.0, now_est=now_est)
        lp = safe_float(met.get("live_price"), 0.0)
        if lp > 0.0:
            return lp
    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    return safe_float(h_info.get("last_price"), 0.0)


# ---------------------------------------------------------------------------
# [핵심 1] 스나이퍼 OFF ➔ ON 재가동 시 지층 원장 및 실보유 잔고 즉시 동기화
# ---------------------------------------------------------------------------
async def relock_stratum_on_bot_resume(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    운용자가 텔레그램 전술 제어반(/settlement)에서 스나이퍼를 OFF 했다가 ON 으로 재가동할 때 호출되어,
    현재 토스 계좌의 SOXL 실보유 잔고와 실시간 현재가를 지층 원장(soxl_stratum_ledger.json)과 즉시 화해시킨다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    holdings = await context.api.get_holdings_all()
    prices = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
    rec_res = await reconcile_stratum_with_broker_holdings_plugin(
        context=context,
        holdings=holdings,
        prices=prices,
        now_est=dt_now,
        force_reconcile=True,
    )
    return {
        "status": "RELOCKED_STRATUM_ON_RESUME",
        "reconcile_result": rec_res,
    }


# ---------------------------------------------------------------------------
# [핵심 2] Case 29, 58, 60, 61, 63 : 로컬 지층 원장(buckets) ↔ 토스 실보유 잔고 무결성 화해
# ---------------------------------------------------------------------------
async def reconcile_stratum_with_broker_holdings_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
    force_reconcile: bool = False,
) -> Dict[str, Any]:
    """
    🚨 Case 29 (유령 잔고 방어) & Case 58 (재기동/수동매매 지층 복원) & Case 60 ~ 63:
    로컬 원장(soxl_stratum_ledger.json)의 지층 수량 합계(ledger_qty)와
    토스 실제 보유 잔고(/api/v1/holdings 의 h_qty, avg_price)를 대조한다:
    1) 미체결 주문(buy_order_id / sell_order_id)이 진행 중이거나 매수 직후 15.0초 인덱싱 시차
       그레이스 쉴드 구간(Case 61)이면 화해를 보류하여 정상 체결 프로세스를 보호한다.
    2) [케이스 A : 원장 0주인데 실제 토스 잔고 h_qty > 0 존재 (재기동 복원 또는 수동 매수)]
       즉시 실보유 수량(h_qty)과 평단가(avg_price)로 1층(StratumBucket)을 생성하여 지층 관리에 편입한다.
    3) [케이스 B : 원장 > 0주인데 실제 토스 잔고 h_qty == 0 (토스 앱에서 수동 전량 매도)]
       실시간 가격(또는 직전 스냅샷 가격)으로 수동 청산 손익을 정산하고 지층 큐를 비우며,
       기억된 최저 매수가(cycle_min_buy_price = 0.0)까지 완벽히 리셋(Case 60)하여 유령 잔고 에러와
       차기 사이클 기준가 왜곡을 원천 차단한다.
    4) [케이스 C : 원장 수량 != 실제 토스 잔고 수량 (부분 수동 매수/매도, Case 63)]
       기존 지층에 누적된 리앵커링 원금 차감액(reanchor_credit)과 리앵커링 단가(book_price) 이점을
       증발시키지 않고 보존하면서 총 수량과 실제 원금(actual_cost_usd)을 토스 실잔고 팩트에 맞게 정밀 보정한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    st = context.ledger.get_state(TARGET_SYMBOL)

    buy_oid = str(st.get("buy_order_id") or "").strip()
    sell_oid = str(st.get("sell_order_id") or "").strip()
    if (buy_oid or sell_oid) and not force_reconcile:
        return {"status": "SKIP_RECONCILE_ACTIVE_ORDER"}

    if (
        is_within_auto_fill_grace_shield(context, TARGET_SYMBOL)
        and not force_reconcile
    ):
        return {"status": "SKIP_RECONCILE_GRACE_SHIELD"}

    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    h_qty = max(0, safe_int(h_info.get("qty"), 0))
    avg_p = max(0.0, safe_float(h_info.get("avg_price"), 0.0))
    live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    if live_p <= 0.0:
        live_p = avg_p

    if h_qty > 0 and avg_p > 0.0:
        update_exit_snapshot_on_hold(
            context, TARGET_SYMBOL, h_qty, avg_p, live_p
        )

    buckets = buckets_from_dicts(st.get("buckets", []))
    ledger_qty = sum(b.qty for b in buckets)

    # 수량이 정확히 일치하는 정상 상태
    if ledger_qty == h_qty:
        if h_qty > 0:
            cur_anchor = safe_float(st.get("anchor_price"), 0.0)
            cur_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
            min_bucket_p = min(b.book_price for b in buckets)
            if cur_anchor <= 0.0 or cur_min_p <= 0.0:
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    anchor_price=(
                        cur_anchor if cur_anchor > 0.0 else buckets[0].book_price
                    ),
                    cycle_min_buy_price=(
                        min(cur_min_p, min_bucket_p)
                        if cur_min_p > 0.0
                        else min_bucket_p
                    ),
                    ratchet_anchor_price=buckets[0].book_price,
                )
        return {"status": "STRATUM_HOLDINGS_MATCHED", "qty": h_qty}

    # -----------------------------------------------------------------------
    # [케이스 A] 원장은 0주인데 토스 계좌에 실제 보유 잔고(h_qty > 0)가 존재하는 경우
    # -----------------------------------------------------------------------
    if ledger_qty == 0 and h_qty > 0 and avg_p > 0.0:
        actual_cost = round(float(h_qty) * avg_p, 4)
        new_b1 = StratumBucket(
            qty=h_qty,
            book_price=round(avg_p, 4),
            book_invested_usd=actual_cost,
            actual_cost_usd=actual_cost,
            lot_count=1,
            lot_type="SYNCED_1FL",
        )
        await context.ledger.update_state(
            TARGET_SYMBOL,
            buckets=buckets_to_dicts([new_b1]),
            anchor_price=round(avg_p, 4),
            cycle_min_buy_price=round(avg_p, 4),
            ratchet_anchor_price=round(avg_p, 4),
            entry_ts=time.time(),
        )
        logger.info(
            "🔗 [SOXL 실잔고 지층 편입 완료] %d주 @ 평단 $%.2f (원금 $%.2f)",
            h_qty,
            avg_p,
            actual_cost,
        )
        if hasattr(context, "send_slim_alert"):
            await context.send_slim_alert(
                title=f"🔗 [SOXL 실보유 잔고 지층 자동 편입] {h_qty}주",
                body=(
                    f"평단가 ${avg_p:.2f} (원금 ${actual_cost:,.2f}) · "
                    f"1층 목표 +1.50% 스나이퍼 매도 및 10분 고정 시드보호 매수게이트 결속 완료"
                ),
            )
        return {
            "status": "RECONCILED_NEW_HOLDINGS_TO_1FL",
            "qty": h_qty,
            "avg_price": avg_p,
        }

    # -----------------------------------------------------------------------
    # [케이스 B] 원장에는 지층이 있으나 토스 계좌 실잔고가 0주(h_qty == 0)인 경우 (수동 전량 매도)
    # -----------------------------------------------------------------------
    if ledger_qty > 0 and h_qty == 0:
        sold_cost = sum(b.actual_cost_usd for b in buckets)
        ref_exit_p = (
            live_p
            if live_p > 0.0
            else (
                sold_cost / float(ledger_qty)
                if ledger_qty > 0
                else 0.0
            )
        )
        pnl_info = calc_sell_realized_pnl(
            sold_qty=ledger_qty,
            sell_exec_price=ref_exit_p,
            sold_actual_cost_usd=sold_cost,
        )
        net_pnl = pnl_info["net_pnl_usd"]
        gross_pnl = pnl_info["gross_pnl_usd"]
        fee_usd = pnl_info["fee_usd"]

        cum_gross = round(
            safe_float(st.get("cumulative_realized_gross_usd"), 0.0)
            + gross_pnl,
            4,
        )
        cum_fee = round(
            safe_float(st.get("cumulative_realized_fee_usd"), 0.0) + fee_usd, 4
        )
        cum_net = round(
            safe_float(st.get("cumulative_realized_net_usd"), 0.0) + net_pnl, 4
        )
        day_net = round(
            safe_float(st.get("daily_realized_net_usd"), 0.0) + net_pnl, 4
        )

        # MODIFIED (Case 60): 수동 전량 매도(0주) 시 cycle_min_buy_price = 0.0 으로 완벽 리셋!
        updated_st = await context.ledger.update_state(
            TARGET_SYMBOL,
            buckets=[],
            anchor_price=round(ref_exit_p, 4) if ref_exit_p > 0.0 else 0.0,
            cycle_min_buy_price=0.0,
            ratchet_anchor_price=(
                round(ref_exit_p, 4) if ref_exit_p > 0.0 else 0.0
            ),
            sell_order_id="",
            buy_order_id="",
            ordered_qty=0,
            pending_sell_mode="",
            cumulative_realized_gross_usd=cum_gross,
            cumulative_realized_fee_usd=cum_fee,
            cumulative_realized_net_usd=cum_net,
            daily_realized_net_usd=day_net,
        )
        logger.info(
            "🧹 [SOXL 수동 전량매도 감지 ➔ 원장 0주 & 기억저가 0점 동기화] %d주 정산 (세후 %+.2f$, 가용현금 $%.2f)",
            ledger_qty,
            net_pnl,
            safe_float(updated_st.get("free_cash_usd"), 0.0),
        )
        return {
            "status": "RECONCILED_MANUAL_ZERO_EXIT",
            "settled_qty": ledger_qty,
            "net_pnl_usd": net_pnl,
        }

    # -----------------------------------------------------------------------
    # [케이스 C] 원장 수량(ledger_qty)과 토스 실잔고(h_qty)가 부분 불일치하는 경우 (Case 63)
    # -----------------------------------------------------------------------
    if h_qty > 0 and avg_p > 0.0:
        total_actual_cost = round(float(h_qty) * avg_p, 4)
        if len(buckets) >= 2 and h_qty > buckets[0].qty:
            # 1층 수량은 그대로 유지하고 수량 차이와 리앵커링 크레딧을 2층(MERGED_UPPER)에 정밀 반영
            b1 = buckets[0]
            b2_old = buckets[1]
            new_b2_q = h_qty - b1.qty
            b1_cost = min(total_actual_cost - 0.01, b1.actual_cost_usd)
            b2_cost = max(0.01 * float(new_b2_q), total_actual_cost - b1_cost)

            # Case 63: 기존 2층에 누적되어 있던 리앵커링 원금 차감액(credit) 보존
            old_b2_credit = max(
                0.0, b2_old.actual_cost_usd - b2_old.book_invested_usd
            )
            b2_book_inv_from_credit = max(
                0.01 * float(new_b2_q), b2_cost - old_b2_credit
            )
            b2_book_p = min(
                b2_old.book_price,
                b2_book_inv_from_credit / float(new_b2_q),
            )
            new_b2 = StratumBucket(
                qty=new_b2_q,
                book_price=round(b2_book_p, 4),
                book_invested_usd=round(b2_book_p * float(new_b2_q), 4),
                actual_cost_usd=round(b2_cost, 4),
                lot_count=max(1, b2_old.lot_count),
                lot_type="MERGED_UPPER",
            )
            adj_buckets = [b1, new_b2]
        else:
            # 단일 지층으로 수량 및 원금 동기화 (기존 누적 리앵커링 차감액 및 book_price 이점 100% 보존)
            old_b0 = buckets[0]
            old_total_credit = sum(
                max(0.0, b.actual_cost_usd - b.book_invested_usd)
                for b in buckets
            )
            book_inv_after_credit = max(
                0.01 * float(h_qty), total_actual_cost - old_total_credit
            )
            credit_book_p = book_inv_after_credit / float(h_qty)
            adj_book_p = min(old_b0.book_price, credit_book_p, avg_p)
            adj_buckets = [
                StratumBucket(
                    qty=h_qty,
                    book_price=round(adj_book_p, 4),
                    book_invested_usd=round(adj_book_p * float(h_qty), 4),
                    actual_cost_usd=total_actual_cost,
                    lot_count=max(1, sum(b.lot_count for b in buckets)),
                    lot_type=old_b0.lot_type,
                )
            ]

        old_cycle_min = safe_float(st.get("cycle_min_buy_price"), 0.0)
        min_adj_p = min(b.book_price for b in adj_buckets)
        next_cycle_min = (
            min(old_cycle_min, min_adj_p) if old_cycle_min > 0.0 else min_adj_p
        )

        await context.ledger.update_state(
            TARGET_SYMBOL,
            buckets=buckets_to_dicts(adj_buckets),
            cycle_min_buy_price=round(next_cycle_min, 4),
        )
        logger.info(
            "🔄 [SOXL 지층 수량 정합성 보정] 원장 %d주 -> 실잔고 %d주 (실평단 $%.2f · 1층장부 $%.2f)",
            ledger_qty,
            h_qty,
            avg_p,
            adj_buckets[0].book_price,
        )
        return {
            "status": "RECONCILED_PARTIAL_DIFF",
            "old_qty": ledger_qty,
            "new_qty": h_qty,
        }

    return {"status": "RECONCILE_NO_ACTION"}


# ---------------------------------------------------------------------------
# [핵심 3] 04:00 ~ 04:06 EST 프리장 초기 호가 안정화 및 오버나이트 이월 동기화
# ---------------------------------------------------------------------------
async def execute_0400_premarket_stabilization_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🔹 04:00 ~ 04:06 EST:
    프리장 개장 초기 호가 안정화 대기 구간에서:
    1) 전일 세션에서 미처 정리되지 않은 잔류 주문 ID 가 있으면 거래소 OPEN 목록과 대조하여 청소한다.
    2) 날짜가 변경되었으면 일일 결산 카운터(daily_*)를 0으로 초기화한다.
    3) 오버나이트 이월된 SOXL 지층(1층 / 2층)과 토스 실보유 잔고의 무결성을 대조 동기화한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not is_premarket_stabilization_window(hm):
        return {"status": "OUTSIDE_0400_STABILIZATION_WINDOW"}

    today_str = dt_now.strftime("%Y-%m-%d")
    st = context.ledger.get_state(TARGET_SYMBOL)

    # 1) 전일 세션 잔류 미체결 주문 ID 가 있으면 거래소 OPEN 주문과 대조하여 청소
    buy_oid = str(st.get("buy_order_id") or "").strip()
    sell_oid = str(st.get("sell_order_id") or "").strip()
    if buy_oid or sell_oid:
        open_orders = await context.api.get_orders(
            status="OPEN", symbol=TARGET_SYMBOL
        )
        open_ids = {
            str(o.get("orderId") or "").strip()
            for o in open_orders
            if isinstance(o, dict)
        }
        clear_kwargs: Dict[str, Any] = {}
        if buy_oid and buy_oid not in open_ids:
            clear_kwargs["buy_order_id"] = ""
            clear_kwargs["ordered_qty"] = 0
            clear_kwargs["buy_price"] = 0.0
            clear_kwargs["pending_portion_budget"] = 0.0
            clear_kwargs["pending_lot_type"] = ""
        if sell_oid and sell_oid not in open_ids:
            clear_kwargs["sell_order_id"] = ""
            clear_kwargs["ordered_qty"] = 0
            clear_kwargs["target_sell_price"] = 0.0
            clear_kwargs["pending_sell_mode"] = ""
        if clear_kwargs:
            st = await context.ledger.update_state(
                TARGET_SYMBOL, **clear_kwargs
            )

    # 2) 새 거래일 진입 시 일일 통계 카운터 초기화 및 10분 게이트 하드 고정 확인
    if str(st.get("last_evaluated_date") or "") != today_str:
        await context.ledger.update_state(
            TARGET_SYMBOL,
            buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
            portion_mode="COMP_08PCT",
            threshold_mode="08_VREV_PROG",
            is_compound=True,
            last_evaluated_date=today_str,
            last_evaluated_buy_bucket_id=-1,
            daily_realized_net_usd=0.0,
            daily_buy_events=0,
            daily_sell_events=0,
            daily_seed_protect_pass_cnt=0,
            daily_leftover_saved_usd=0.0,
            daily_reanchor_cnt=0,
        )

    # 3) 오버나이트 지층 ↔ 실잔고 무결성 화해
    rec_res = await reconcile_stratum_with_broker_holdings_plugin(
        context=context,
        holdings=holdings,
        prices=prices,
        now_est=dt_now,
    )
    return {
        "status": "PREMARKET_0400_STABILIZED",
        "reconcile": rec_res,
    }


# ---------------------------------------------------------------------------
# [핵심 4] 16:00 ~ 16:59 EST 정규장 마감 일일 결산 보고 및 오버나이트 지층 보존 (MOC 없음!)
# ---------------------------------------------------------------------------
async def execute_1600_daily_closing_settlement_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🔹 16:00 ~ 16:59 EST:
    정규장 마감 후:
    1) 절대로 보유 지층을 강제 손절(MOC)하지 않고 100% 오버나이트 안전 보존한다!
    2) 만약 장부에 미체결된 당일(DAY) 주문 ID 가 남아 있으면 안전하게 취소 정리한다.
    3) 당일 세후 실현수익금, 자투리 절약액, 리앵커링 횟수, 🛡️시드보호패스 횟수 및
       오버나이트 이월 지층 현황을 일일 결산 알림으로 단 1회 타전한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not is_daily_settlement_window(hm):
        return {"status": "OUTSIDE_1600_SETTLEMENT_WINDOW"}

    today_str = get_logical_trading_date_str(dt_now, rollover_hour=18)
    st = context.ledger.get_state(TARGET_SYMBOL)

    # 미체결 잔류 주문이 있으면 취소 및 클리어
    for oid_key in ("buy_order_id", "sell_order_id"):
        oid = str(st.get(oid_key) or "").strip()
        if oid:
            try:
                await context.api.cancel_order(oid)
            except Exception:
                pass
            st = await context.ledger.update_state(
                TARGET_SYMBOL,
                buy_order_id=(
                    ""
                    if oid_key == "buy_order_id"
                    else st.get("buy_order_id", "")
                ),
                sell_order_id=(
                    ""
                    if oid_key == "sell_order_id"
                    else st.get("sell_order_id", "")
                ),
                ordered_qty=0,
                pending_sell_mode="",
            )

    if str(st.get("last_daily_report_date") or "") == today_str:
        return {"status": "DAILY_REPORT_ALREADY_SENT", "date": today_str}

    live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    st_latest = await context.ledger.update_state(
        TARGET_SYMBOL,
        last_daily_report_date=today_str,
    )
    summary = summarize_stratum_portfolio(st_latest, live_price=live_p)

    day_net = safe_float(st_latest.get("daily_realized_net_usd"), 0.0)
    day_buys = safe_int(st_latest.get("daily_buy_events"), 0)
    day_sells = safe_int(st_latest.get("daily_sell_events"), 0)
    day_pass = safe_int(st_latest.get("daily_seed_protect_pass_cnt"), 0)
    day_saved = safe_float(st_latest.get("daily_leftover_saved_usd"), 0.0)
    day_reanchor = safe_int(st_latest.get("daily_reanchor_cnt"), 0)

    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    tot_eq = safe_float(summary.get("total_mark_equity_usd"), 0.0)
    tot_roi = safe_float(summary.get("total_roi_pct"), 0.0)

    if hasattr(context, "send_slim_alert"):
        d_sign = "+" if day_net >= 0.0 else "-"
        r_sign = "+" if tot_roi >= 0.0 else ""
        await context.send_slim_alert(
            title=(
                f"🌙 [16:00 EST 일일 결산 & 오버나이트 지층 보존] "
                f"금일실현 {d_sign}${abs(day_net):,.2f}"
            ),
            body=(
                f"매수 {day_buys}회 · 매도 {day_sells}회 · ♻️리앵커 {day_reanchor}회 · "
                f"🛡️시드보호패스 {day_pass}회 · 자투리절약 +${day_saved:,.2f} · "
                f"이월보유 {held_sh}주({held_lots}회차) · 가용현금 ${free_cash:,.2f} "
                f"· 총자산 ${tot_eq:,.2f}({r_sign}{tot_roi:.1f}%)"
            ),
        )

    return {
        "status": "DAILY_CLOSING_REPORT_SENT",
        "date": today_str,
        "daily_realized_net_usd": day_net,
        "held_shares": held_sh,
        "free_cash_usd": free_cash,
    }


# 하위 호환 별칭 (타 모듈에서 구 함수명으로 호출하더라도 안전하게 작동)
relock_qqq_baseline_on_bot_resume = relock_stratum_on_bot_resume
execute_macro_3stage_defense_plugin = reconcile_stratum_with_broker_holdings_plugin
