# -*- coding: utf-8 -*-
"""
# MODIFIED: sniper_exit_plugin.py
순수 롱(SOXL) 전용 '04:07 ~ 16:00 EST 시간 제약 없는 1분 상시(실시간) 스나이퍼 매도 게이트(오팔 :
08_VREV_PROG 기준 1층 +1.50% / 2층 통합층 +1.00% · 01_PROG_LOT 기준 +1.0% / +1.5% / +2.0%) ·
단일 지층 수익금 리앵커링(vrev_reanchor_single_tier_quota · 0.994 버퍼) ·
무팽창 True Always-L1(ANTI_BLOAT 상승 래칫 계단 익절 & 2층 연쇄 졸업) ·
2단계 방탄 매도 스윕(Case 24) 및 실물 원가(actual_cost_usd) 분리 회계 복리 정산' 코어 플러그인 (Step 05 / 09)
- MODIFIED: 제3헌법에 의거하여 구버전 하이킨 아시 콜백 트레일링, 07:15 후반부 VWAP 탈출(PRE_VWAP_ESC),
  1주 휩소 익절 및 RE_BOT_TRAIL 데드코드를 100% 영구 소각
- NEW: 1층 단독 익절(SELL_L1_ONLY) 시 세후 실현 순수익금(net_pnl * 0.994)으로 2층 장부 단가(book_price)를
  즉시 강제 인하하고, 원금 + 세후 순수익금 전액을 가용 현금 시드(free_cash_usd)에 실시간 복리 산입
- NEW: 2층(MERGED_UPPER) 단독 보유 중 무팽창 True Always-L1(ANTI_BLOAT) 상승 래칫 계단 수확 및
  리앵커링 직후 2층 목표가 도달 시 동일 틱 즉시 전량 졸업(SELL_L2_GRAD) 구현
- 제1헌법 ~ 제6헌법 및 Case 19 ~ 24, 36 ~ 58 완벽 준수
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from quant_engine import (
    ROUND_TRIP_FEE_RATE_PCT,
    StratumBucket,
    buckets_from_dicts,
    buckets_to_dicts,
    calc_sell_realized_pnl,
    compute_golden8_quota_qty,
    get_golden8_prog_thresholds,
    resolve_affordable_buy_qty,
    vrev_reanchor_single_tier_quota,
)
from quant_indicators_plugin import (
    is_active_trading_window,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    EST_TZ,
    TARGET_SYMBOL,
    _NonRetryableApiError,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_exit_plugin")

# 미체결 스나이퍼 매도 주문이 호가 변동으로 체결 지연될 때 매수 1호가(Bid 1)로 즉시 하향 정정하는 주기(초)
SELL_ORDER_SWEEP_CHASE_SEC: float = 3.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 실시간 SOXL 현재가 조회 및 보유 스냅샷 백업
# ---------------------------------------------------------------------------
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


def update_exit_snapshot_on_hold(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    Case 19: 보유 중 매 틱마다 _EXIT_SNAPSHOT_CACHE["SOXL"] 에 실보유 수량과 평단가,
    현재가를 백업하여 체결 확인 시 데이터 결측을 방어한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    snap_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    prev = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}

    lp = safe_float(live_price, 0.0)
    ap = safe_float(avg_price, safe_float(prev.get("last_avg_price"), 0.0))
    qty = max(
        0, safe_int(holdings_qty, safe_int(prev.get("last_holdings_qty"), 0))
    )

    updated = {
        "symbol": sym,
        "last_holdings_qty": qty,
        "last_avg_price": ap,
        "last_live_price": (
            lp if lp > 0.0 else safe_float(prev.get("last_live_price"), ap)
        ),
        "last_sweep_price": safe_float(prev.get("last_sweep_price"), 0.0),
        "last_sweep_reason": str(prev.get("last_sweep_reason") or ""),
        "updated_ts": time.time(),
    }
    if isinstance(snap_map, dict):
        snap_map[sym] = updated
    return updated


# ---------------------------------------------------------------------------
# [핵심 1] Case 24 : 2단계 방탄 스나이퍼 매도 발사 및 정정 스윕 (execute_bulletproof_sell_sweep)
# ---------------------------------------------------------------------------
async def execute_bulletproof_sell_sweep(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    holdings_qty: int = 1,
    sweep_price: float = 0.0,
    reason_tag: str = "SELL_L1_ONLY",
) -> Dict[str, Any]:
    """
    🚨 Case 24:
    1분 상시 스나이퍼 매도(1층 단독 익절 SELL_L1_ONLY / 1·2층 동시 졸업 SELL_ALL_GRAD / 2층 단독 졸업 SELL_L2_GRAD) 발사 시:
    - [1단계] 이미 동일 수량의 OPEN 매도 주문(sell_order_id)이 존재하면
      POST /api/v1/orders/{orderId}/modify 로 0.1초 만에 가격을 sweep_price 로 즉시 하향 정정한다!
      (US 주식은 quantity 생략 강제 · 409 already-modified 시 최신 OPEN 매도 주문 ID 자동 추적 정정)
    - [2단계] 기존 매도 주문이 없거나 수량이 다르거나 정정이 거절된 경우,
      기존 미체결 매도 주문을 선취소하고 GET /api/v1/sellable-quantity 를 확인한 뒤
      목표 매도 수량(holdings_qty)만큼 신규 LIMIT 매도 주문을 발사한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    target_sell_qty = max(1, safe_int(holdings_qty, 1))
    sp = max(0.01, math.floor((safe_float(sweep_price, 0.0) + 1e-9) * 100.0) / 100.0)

    snap_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    if isinstance(snap_map, dict):
        snap_entry = (
            snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        )
        snap_entry["last_sweep_price"] = sp
        snap_entry["last_sweep_reason"] = str(reason_tag or "SELL_L1_ONLY")
        snap_entry["last_holdings_qty"] = target_sell_qty
        snap_map[sym] = snap_entry

    async with context.in_memory_ordering_lock:
        st = context.ledger.get_state(sym)
        sell_oid = str(st.get("sell_order_id") or "").strip()
        prev_ord_qty = safe_int(st.get("ordered_qty"), 0)

        # [1단계] 기존 매도 주문이 존재하고 주문 수량이 일치할 때 즉시 가격 정정(modify_order)
        if sell_oid and prev_ord_qty == target_sell_qty:
            try:
                mod_res = await context.api.modify_order(
                    order_id=sell_oid,
                    new_price=sp,
                    order_type="LIMIT",
                    symbol=sym,
                    side="SELL",
                )
                new_oid = str(mod_res.get("orderId") or sell_oid).strip()
                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    target_sell_price=sp,
                    pending_sell_mode=str(reason_tag or "SELL_L1_ONLY"),
                    last_modify_mono=time.monotonic(),
                )
                context.wakeup_event.set()
                logger.info(
                    "⚡ [%s] 1단계 방탄 매도 정정 스윕(%s) 완료: %d주 @ $%.2f (orderId=%s)",
                    sym,
                    reason_tag,
                    target_sell_qty,
                    sp,
                    new_oid,
                )
                return {
                    "status": "MODIFIED_SWEEP_OK",
                    "symbol": sym,
                    "orderId": new_oid,
                    "sweep_price": sp,
                    "qty": target_sell_qty,
                    "reason": reason_tag,
                }
            except _NonRetryableApiError as exc:
                if exc.error_code == "already-filled":
                    context.wakeup_event.set()
                    return {
                        "status": "ALREADY_FILLED",
                        "symbol": sym,
                        "orderId": sell_oid,
                        "sweep_price": sp,
                        "qty": target_sell_qty,
                    }
                logger.warning(
                    "⚠️ [%s] 1단계 매도 정정 거절(%s) -> 2단계 재발사 전환: %s",
                    sym,
                    exc.error_code,
                    exc.error_message,
                )
            except Exception as exc:
                logger.warning(
                    "⚠️ [%s] 1단계 매도 정정 예외 -> 2단계 전환: %s", sym, exc
                )

        # 기존 미체결 매도 주문이 있으면 선취소
        if sell_oid:
            try:
                await context.api.cancel_order(sell_oid)
            except Exception:
                pass

        latest_open = await context.api.find_latest_open_order_id(
            sym, side="SELL"
        )
        if latest_open and latest_open != sell_oid:
            try:
                await context.api.cancel_order(latest_open)
            except Exception:
                pass

        # [2단계] sellableQuantity 최대 2.0초 폴링 확인 후 목표 매도 수량(target_sell_qty)만큼 LIMIT 발사
        sellable_q = 0
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            sellable_q = await context.api.get_sellable_quantity(sym)
            if sellable_q >= target_sell_qty or sellable_q > 0:
                break
            await asyncio.sleep(0.20)

        final_qty = (
            min(target_sell_qty, sellable_q)
            if sellable_q > 0
            else target_sell_qty
        )
        cid = generate_client_order_id(prefix="S1M")
        try:
            ord_res = await context.api.create_order(
                symbol=sym,
                side="SELL",
                quantity=final_qty,
                price=sp,
                order_type="LIMIT",
                time_in_force="DAY",
                client_order_id=cid,
                enforce_daymarket_block=False,
            )
            new_oid = str(ord_res.get("orderId") or "").strip()
            await context.ledger.update_state(
                sym,
                sell_order_id=new_oid,
                client_order_id=cid,
                ordered_qty=final_qty,
                target_sell_price=sp,
                pending_sell_mode=str(reason_tag or "SELL_L1_ONLY"),
                last_modify_mono=time.monotonic(),
            )
            context.wakeup_event.set()
            logger.info(
                "🔥 [%s] 1분 상시 스나이퍼 매도 발사(%s) 완료: %d주 @ $%.2f (orderId=%s)",
                sym,
                reason_tag,
                final_qty,
                sp,
                new_oid,
            )
            return {
                "status": "NEW_SELL_SWEEP_OK",
                "symbol": sym,
                "orderId": new_oid,
                "sweep_price": sp,
                "qty": final_qty,
                "reason": reason_tag,
            }
        except Exception as exc:
            logger.error(
                "🚨 [%s] 스나이퍼 매도 발사(%s) 실패: %s", sym, reason_tag, exc
            )
            return {"status": "SWEEP_FAILED", "symbol": sym, "error": str(exc)}


# ---------------------------------------------------------------------------
# [핵심 2] 매도 FILLED 정산 · 세후 순익 시드 복리 산입 · 단일지층(2층) 리앵커링 커밋
# ---------------------------------------------------------------------------
async def _commit_filled_sell_and_reanchor_ledger(
    context: Any,
    filled_qty: int,
    avg_fill_price: float,
    sell_mode: str,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & 🎯 4 & Case 19, 47:
    스나이퍼 매도 주문이 체결(FILLED)되었을 때:
    1) 매도 모드(SELL_L1_ONLY vs SELL_ALL_GRAD / SELL_L2_GRAD)에 따라 매도된 지층(sold_buckets)과
       남은 지층(rem_buckets)을 분리한다.
    2) 매도된 지층의 실제 매수 원금(sold_actual_cost_usd)과 실제 매도 체결가(avg_fill_price)로
       왕복 수수료(0.14%) 차감 후 세후 실현 순수익금(net_pnl_usd)을 산출한다.
    3) 만약 1층만 팔리고 2층(단일 지층)이 남은 경우(len(rem_buckets) == 1 and net_pnl_usd > 0),
       vrev_reanchor_single_tier_quota() 를 호출하여 net_pnl_usd * 0.994 만큼 2층의 장부 투자원금
       (book_invested_usd)을 즉시 차감해 2층 목표 탈출 단가(book_price)를 강제 인하한다!
    4) 회수된 원금과 세후 순수익금은 AssassinLedger.update_state() 의 회계 항등식을 통해
       가용 현금 시드(free_cash_usd)에 100% 실시간 복리 귀속된다.
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    buckets = buckets_from_dicts(st.get("buckets", []))
    fill_p = round(max(0.01, safe_float(avg_fill_price, 0.01)), 4)
    q_filled = max(1, safe_int(filled_qty, 1))
    mode = str(sell_mode or st.get("pending_sell_mode") or "SELL_L1_ONLY").upper()

    if not buckets:
        await context.ledger.update_state(
            TARGET_SYMBOL,
            sell_order_id="",
            ordered_qty=0,
            target_sell_price=0.0,
            pending_sell_mode="",
        )
        return {"status": "NO_BUCKETS_TO_SETTLE"}

    sold_buckets: List[StratumBucket] = []
    rem_buckets: List[StratumBucket] = []

    if mode in ("SELL_ALL_GRAD", "SELL_L2_GRAD") or len(buckets) == 1:
        # 전량 졸업 또는 단일 지층 매도
        total_b_qty = sum(b.qty for b in buckets)
        if q_filled >= total_b_qty:
            sold_buckets = list(buckets)
            rem_buckets = []
        else:
            # 부분 체결된 경우 비율만큼 원금 분할 정산
            b0 = buckets[0]
            ratio = min(1.0, float(q_filled) / float(b0.qty))
            sold_cost_part = round(b0.actual_cost_usd * ratio, 4)
            sold_book_part = round(b0.book_invested_usd * ratio, 4)
            sold_buckets = [
                StratumBucket(
                    qty=q_filled,
                    book_price=b0.book_price,
                    book_invested_usd=sold_book_part,
                    actual_cost_usd=sold_cost_part,
                    lot_count=1,
                    lot_type=b0.lot_type,
                )
            ]
            rem_q = b0.qty - q_filled
            if rem_q > 0:
                rem_b0 = StratumBucket(
                    qty=rem_q,
                    book_price=b0.book_price,
                    book_invested_usd=max(0.01 * rem_q, b0.book_invested_usd - sold_book_part),
                    actual_cost_usd=max(0.01 * rem_q, b0.actual_cost_usd - sold_cost_part),
                    lot_count=max(1, b0.lot_count),
                    lot_type=b0.lot_type,
                )
                rem_buckets = [rem_b0] + buckets[1:]
            else:
                rem_buckets = buckets[1:]
    else:
        # SELL_L1_ONLY : 1층(buckets[0])만 단독 익절하고 2층(buckets[1:])은 남김
        b1 = buckets[0]
        if q_filled >= b1.qty:
            sold_buckets = [b1]
            rem_buckets = buckets[1:]
        else:
            ratio = min(1.0, float(q_filled) / float(b1.qty))
            sold_cost_part = round(b1.actual_cost_usd * ratio, 4)
            sold_book_part = round(b1.book_invested_usd * ratio, 4)
            sold_buckets = [
                StratumBucket(
                    qty=q_filled,
                    book_price=b1.book_price,
                    book_invested_usd=sold_book_part,
                    actual_cost_usd=sold_cost_part,
                    lot_count=1,
                    lot_type=b1.lot_type,
                )
            ]
            rem_q = b1.qty - q_filled
            rem_b1 = StratumBucket(
                qty=rem_q,
                book_price=b1.book_price,
                book_invested_usd=max(0.01 * rem_q, b1.book_invested_usd - sold_book_part),
                actual_cost_usd=max(0.01 * rem_q, b1.actual_cost_usd - sold_cost_part),
                lot_count=1,
                lot_type=b1.lot_type,
            )
            rem_buckets = [rem_b1] + buckets[1:]

    sold_q = sum(b.qty for b in sold_buckets)
    sold_actual_cost = sum(b.actual_cost_usd for b in sold_buckets)
    pnl_info = calc_sell_realized_pnl(
        sold_qty=sold_q,
        sell_exec_price=fill_p,
        sold_actual_cost_usd=sold_actual_cost,
    )
    gross_pnl = pnl_info["gross_pnl_usd"]
    fee_usd = pnl_info["fee_usd"]
    net_pnl = pnl_info["net_pnl_usd"]
    net_rate = pnl_info["net_rate_pct"]

    reanchor_drop_amt = 0.0
    old_l2_book_p = 0.0
    new_l2_book_p = 0.0
    reanchor_inc = 0

    # ♻️ [단일 지층 마법 : 1층 익절 후 2층 단독 잔존 시 세후 순익으로 2층 장부단가 즉시 리앵커링!]
    if len(rem_buckets) == 1 and net_pnl > 0.0:
        old_l2_book_p = rem_buckets[0].book_price
        rem_buckets, reanchor_drop_amt = vrev_reanchor_single_tier_quota(
            queue=rem_buckets,
            net_realized_cash_profit=net_pnl,
        )
        new_l2_book_p = rem_buckets[0].book_price
        if reanchor_drop_amt > 0.0:
            reanchor_inc = 1

    tot_sell_ev = safe_int(st.get("total_sell_events"), 0) + 1
    tot_sold_sh = safe_int(st.get("total_sold_shares"), 0) + sold_q
    win_ev = safe_int(st.get("win_sell_events"), 0) + (
        1 if net_pnl > 0.0 else 0
    )
    day_sell_ev = safe_int(st.get("daily_sell_events"), 0) + 1

    reanchor_cnt = safe_int(st.get("reanchor_cnt"), 0) + reanchor_inc
    day_reanchor_cnt = safe_int(st.get("daily_reanchor_cnt"), 0) + reanchor_inc
    cum_reanchor_drop = round(
        safe_float(st.get("cumulative_reanchor_drop_usd"), 0.0)
        + reanchor_drop_amt,
        4,
    )

    cum_gross = round(
        safe_float(st.get("cumulative_realized_gross_usd"), 0.0) + gross_pnl, 4
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

    updated_st = await context.ledger.update_state(
        TARGET_SYMBOL,
        buckets=buckets_to_dicts(rem_buckets),
        anchor_price=fill_p,
        ratchet_anchor_price=fill_p,
        sell_order_id="",
        ordered_qty=0,
        target_sell_price=0.0,
        pending_sell_mode="",
        total_sell_events=tot_sell_ev,
        total_sold_shares=tot_sold_sh,
        win_sell_events=win_ev,
        daily_sell_events=day_sell_ev,
        reanchor_cnt=reanchor_cnt,
        daily_reanchor_cnt=day_reanchor_cnt,
        cumulative_reanchor_drop_usd=cum_reanchor_drop,
        cumulative_realized_gross_usd=cum_gross,
        cumulative_realized_fee_usd=cum_fee,
        cumulative_realized_net_usd=cum_net,
        daily_realized_net_usd=day_net,
    )

    context.wakeup_event.set()
    free_cash_now = safe_float(updated_st.get("free_cash_usd"), 0.0)

    if hasattr(context, "send_slim_alert"):
        sign = "+" if net_pnl >= 0.0 else "-"
        r_sign = "+" if net_rate >= 0.0 else ""
        if len(rem_buckets) == 0:
            note_str = "🎓 전량 졸업 완료(0주 ➔ 즉시 현재가 1층 재산입 가동!)"
        elif reanchor_drop_amt > 0.0:
            note_str = (
                f"♻️ 2층({rem_buckets[0].qty}주) 단가 리앵커링: "
                f"${old_l2_book_p:.2f}➔${new_l2_book_p:.2f}(-${reanchor_drop_amt:.2f})"
            )
        else:
            note_str = f"잔여 {sum(b.qty for b in rem_buckets)}주 보유"

        await context.send_slim_alert(
            title=f"🔴 [SOXL 1분 스나이퍼 익절 체결] {sold_q}주 @ ${fill_p:.2f} ({r_sign}{net_rate:.2f}%)",
            body=(
                f"세후순익 {sign}${abs(net_pnl):,.2f} 시드 복리산입! · "
                f"{note_str} · 가용현금 ${free_cash_now:,.2f}"
            ),
        )

    return {
        "status": "SELL_FILLED_AND_SETTLED",
        "did_sell_or_ratchet": True,
        "sold_qty": sold_q,
        "fill_price": fill_p,
        "net_pnl_usd": net_pnl,
        "reanchor_drop_usd": reanchor_drop_amt,
        "remaining_tiers": len(rem_buckets),
        "free_cash_usd": free_cash_now,
    }


async def sync_pending_sell_order_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Case 19 & Case 24:
    장부에 대기 중인 미체결 매도 주문(sell_order_id)이 존재할 때:
    1) GET /api/v1/orders/{orderId} 로 FILLED 여부를 조회하여 체결 완료 시
       즉시 _commit_filled_sell_and_reanchor_ledger() 로 복리 정산 및 2층 리앵커링을 수행한다.
    2) PENDING 상태로 3.0초 이상 경과 시 최신 매수 1호가(Bid 1)로 즉시 하향 정정하여
       1분 상시 스나이퍼 매도가 미체결로 남지 않도록 끝까지 추격 체결시킨다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    st = context.ledger.get_state(TARGET_SYMBOL)
    sell_oid = str(st.get("sell_order_id") or "").strip()
    if not sell_oid:
        return {"status": "NO_PENDING_SELL", "did_sell_or_ratchet": False}

    ordered_qty = max(1, safe_int(st.get("ordered_qty"), 1))
    target_p = safe_float(st.get("target_sell_price"), 0.0)
    sell_mode = str(st.get("pending_sell_mode") or "SELL_L1_ONLY")

    detail = await context.api.get_order_detail(sell_oid)
    if isinstance(detail, dict):
        ord_status = str(detail.get("status") or "").upper().strip()
        exec_obj = (
            detail.get("execution")
            if isinstance(detail.get("execution"), dict)
            else {}
        )
        filled_q = safe_int(exec_obj.get("filledQuantity"), 0)
        avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)

        # Case 19: 오직 FILLED 체결 데이터의 averageFilledPrice 와 filledQuantity 만 반영!
        if ord_status == "FILLED" or (
            filled_q > 0 and ord_status in ("CANCELED", "REPLACED", "REJECTED")
        ):
            eff_q = filled_q if filled_q > 0 else ordered_qty
            eff_p = avg_fill_p if avg_fill_p > 0.0 else target_p
            return await _commit_filled_sell_and_reanchor_ledger(
                context=context,
                filled_qty=eff_q,
                avg_fill_price=eff_p,
                sell_mode=sell_mode,
            )

        if ord_status in ("CANCELED", "REJECTED") and filled_q == 0:
            await context.ledger.update_state(
                TARGET_SYMBOL,
                sell_order_id="",
                ordered_qty=0,
                target_sell_price=0.0,
                pending_sell_mode="",
            )
            return {
                "status": f"SELL_ORDER_{ord_status}_CLEARED",
                "did_sell_or_ratchet": False,
            }

    # 3초 이상 PENDING 표류 시 실시간 매수 1호가(Bid 1)로 정정 스윕
    last_mod = safe_float(st.get("last_modify_mono"), 0.0)
    if (time.monotonic() - last_mod) >= SELL_ORDER_SWEEP_CHASE_SEC:
        live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
        bid1_p = await context.api.get_best_bid_price(
            TARGET_SYMBOL, fallback_price=live_p
        )
        chase_p = (
            math.floor((min(live_p, bid1_p) + 1e-9) * 100.0) / 100.0
            if (live_p > 0.0 and bid1_p > 0.0)
            else math.floor((max(live_p, bid1_p) + 1e-9) * 100.0) / 100.0
        )
        if chase_p > 0.0 and abs(chase_p - target_p) >= 0.02:
            await execute_bulletproof_sell_sweep(
                context=context,
                symbol=TARGET_SYMBOL,
                holdings_qty=ordered_qty,
                sweep_price=chase_p,
                reason_tag=sell_mode,
            )
            return {
                "status": "SELL_ORDER_CHASE_MODIFIED",
                "did_sell_or_ratchet": True,
                "new_price": chase_p,
            }

    return {
        "status": "SELL_ORDER_PENDING",
        "did_sell_or_ratchet": True,
        "orderId": sell_oid,
    }


# ---------------------------------------------------------------------------
# [핵심 3] 04:07 ~ 16:00 EST 1분 상시 스나이퍼 매도 게이트 & 무팽창 True Always-L1
# ---------------------------------------------------------------------------
async def execute_1m_sniper_sell_and_reanchor_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 [Step 2] & 🎯 3 & 🎯 4 & Case 24, 44, 46 ~ 58:
    04:07:00 ~ 16:00:00 EST 전 구간에서 매 루프(실시간 상시)마다 작동하는 스나이퍼 매도(오팔) 엔진:
    1) 대기 중인 미체결 매도 주문(sell_order_id)이 있으면 즉시 체결/정정 동기화를 수행한다.
    2) [모드 A : 2층(MERGED_UPPER) 단독 보유 중 무팽창 True Always-L1 (ANTI_BLOAT)]
       - A-1) 현재가(live_p)가 리앵커링된 2층 장부단가 대비 +rise_sell_l2_pct (+1.00% 또는 +1~2%) 이상 도달 시:
              즉시 2층 전량 스나이퍼 매도(SELL_L2_GRAD)를 발사하여 사이클 완전 졸업을 집행한다!
       - A-2) 현재가(live_p)가 ratchet_anchor_price 대비 +rise_sell_l1_pct (+1.50% 또는 +1~2%) 이상 도달 시:
              가용 현금(free_cash_usd) 한도 내에서 ratchet_anchor_price 기준 1회분 정량(step_q)의
              계단 상승 차익(net_pnl)을 즉시 실현하여 free_cash_usd 에 복리 산입하고,
              그 수익금으로 2층 장부 단가를 즉시 리앵커링한 뒤 ratchet_anchor_price = live_p 로 한 계단 상향한다!
              만약 이 리앵커링으로 낮아진 2층 단가 대비 현재가가 +rise_sell_l2_pct 에 도달하면
              동일 틱에서 즉시 2층 전량 졸업 매도(SELL_L2_GRAD)까지 연쇄 발사한다!
    3) [모드 B : 일반 1층(buckets[0]) 보유 중 1분 상시 스나이퍼 매도]
       - 현재가(live_p)가 1층 장부단가(buckets[0].book_price) 대비 +rise_sell_l1_pct 이상 도달 시:
         * 만약 2층(buckets[1])도 존재하고 2층 장부단가 대비 +rise_sell_l2_pct 이상 도달했다면:
           (1층 + 2층) 전량 동시 스나이퍼 매도(SELL_ALL_GRAD)를 발사한다!
         * 그렇지 않으면 1층 수량(buckets[0].qty)만 단독 스나이퍼 매도(SELL_L1_ONLY)를 발사한다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute

    st = context.ledger.get_state(TARGET_SYMBOL)

    # 1) 미체결 매도 주문이 있으면 먼저 체결 확인 및 추격 정정 수행
    if str(st.get("sell_order_id") or "").strip():
        return await sync_pending_sell_order_plugin(
            context=context,
            holdings=holdings,
            prices=prices,
            now_est=dt_now,
        )

    if not is_active_trading_window(hm):
        return {
            "status": "OUTSIDE_TRADING_WINDOW",
            "did_sell_or_ratchet": False,
        }

    if not bool(st.get("is_active", True)):
        return {"status": "BOT_INACTIVE", "did_sell_or_ratchet": False}

    # 미체결 매수 주문이 진행 중이면 매수 체결 완료까지 대기
    if str(st.get("buy_order_id") or "").strip():
        return {
            "status": "BUY_ORDER_IN_PROGRESS",
            "did_sell_or_ratchet": False,
        }

    buckets = buckets_from_dicts(st.get("buckets", []))
    if not buckets:
        return {"status": "NO_BUCKETS_HELD", "did_sell_or_ratchet": False}

    live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    if live_p <= 0.0:
        return {"status": "NO_LIVE_PRICE", "did_sell_or_ratchet": False}

    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    h_qty = safe_int(h_info.get("qty"), sum(b.qty for b in buckets))
    avg_p = safe_float(h_info.get("avg_price"), buckets[0].actual_avg_price)
    update_exit_snapshot_on_hold(context, TARGET_SYMBOL, h_qty, avg_p, live_p)

    held_lots = sum(b.lot_count for b in buckets)
    threshold_mode = str(st.get("threshold_mode") or "08_VREV_PROG").upper()
    portion_mode = str(st.get("portion_mode") or "COMP_08PCT").upper()
    init_seed = max(
        100.0,
        safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD),
    )
    free_cash = max(0.0, safe_float(st.get("free_cash_usd"), 0.0))
    actual_invested = sum(b.actual_cost_usd for b in buckets)
    cost_equity = free_cash + actual_invested

    _, rise_sell_l1_pct, rise_sell_l2_pct = get_golden8_prog_thresholds(
        held_lots=held_lots, threshold_mode=threshold_mode
    )

    # -----------------------------------------------------------------------
    # [분기 A : 2층(MERGED_UPPER) 단독 보유 상태 — 무팽창 True Always-L1 (ANTI_BLOAT)]
    # -----------------------------------------------------------------------
    if len(buckets) == 1 and buckets[0].lot_type == "MERGED_UPPER":
        upper_b = buckets[0]
        upper_gain_pct = (
            ((live_p - upper_b.book_price) / upper_b.book_price) * 100.0
            if upper_b.book_price > 0.0
            else 0.0
        )
        ratchet_anchor_p = safe_float(
            st.get("ratchet_anchor_price"),
            safe_float(st.get("anchor_price"), upper_b.book_price),
        )
        ratchet_gain_pct = (
            ((live_p - ratchet_anchor_p) / ratchet_anchor_p) * 100.0
            if ratchet_anchor_p > 0.0
            else 0.0
        )

        # A-1) 2층 통합층 자체가 목표 수익률(+rise_sell_l2_pct)에 도달하면 즉시 2층 전량 매도 졸업!
        if upper_gain_pct >= rise_sell_l2_pct:
            bid1_p = await context.api.get_best_bid_price(
                TARGET_SYMBOL, fallback_price=live_p
            )
            sweep_p = (
                math.floor((min(live_p, bid1_p) + 1e-9) * 100.0) / 100.0
                if bid1_p > 0.0
                else math.floor((live_p + 1e-9) * 100.0) / 100.0
            )
            sweep_res = await execute_bulletproof_sell_sweep(
                context=context,
                symbol=TARGET_SYMBOL,
                holdings_qty=upper_b.qty,
                sweep_price=sweep_p,
                reason_tag="SELL_L2_GRAD",
            )
            return {
                "status": "L2_GRADUATION_SELL_ORDERED",
                "did_sell_or_ratchet": True,
                "qty": upper_b.qty,
                "sweep_price": sweep_p,
                "result": sweep_res,
            }

        # A-2) 무팽창 상승 래칫 계단 익절(ratchet_gain_pct >= rise_sell_l1_pct) & 2층 연속 리앵커링!
        if ratchet_gain_pct >= rise_sell_l1_pct and ratchet_anchor_p > 0.0:
            raw_step_q, _step_bud = compute_golden8_quota_qty(
                anchor_price=ratchet_anchor_p,
                current_total_equity=cost_equity,
                initial_seed_usd=init_seed,
                portion_mode=portion_mode,
            )
            step_q, step_cost = resolve_affordable_buy_qty(
                target_qty=raw_step_q,
                price_t=ratchet_anchor_p,
                free_cash_usd=free_cash,
            )

            if step_q > 0:
                pnl_step = calc_sell_realized_pnl(
                    sold_qty=step_q,
                    sell_exec_price=live_p,
                    sold_actual_cost_usd=step_cost,
                )
                gross_pnl = pnl_step["gross_pnl_usd"]
                fee_usd = pnl_step["fee_usd"]
                net_pnl = pnl_step["net_pnl_usd"]

                old_l2_bp = upper_b.book_price
                reanchor_drop = 0.0
                if net_pnl > 0.0:
                    buckets, reanchor_drop = vrev_reanchor_single_tier_quota(
                        queue=buckets,
                        net_realized_cash_profit=net_pnl,
                    )
                new_l2_bp = buckets[0].book_price

                tot_buy_ev = safe_int(st.get("total_buy_events"), 0) + 1
                tot_buy_sh = safe_int(st.get("total_buy_shares"), 0) + step_q
                day_buy_ev = safe_int(st.get("daily_buy_events"), 0) + 1

                tot_sell_ev = safe_int(st.get("total_sell_events"), 0) + 1
                tot_sold_sh = safe_int(st.get("total_sold_shares"), 0) + step_q
                win_ev = safe_int(st.get("win_sell_events"), 0) + (
                    1 if net_pnl > 0.0 else 0
                )
                day_sell_ev = safe_int(st.get("daily_sell_events"), 0) + 1

                re_inc = 1 if reanchor_drop > 0.0 else 0
                reanchor_cnt = safe_int(st.get("reanchor_cnt"), 0) + re_inc
                day_reanchor_cnt = (
                    safe_int(st.get("daily_reanchor_cnt"), 0) + re_inc
                )
                cum_reanchor_drop = round(
                    safe_float(st.get("cumulative_reanchor_drop_usd"), 0.0)
                    + reanchor_drop,
                    4,
                )

                cum_gross = round(
                    safe_float(st.get("cumulative_realized_gross_usd"), 0.0)
                    + gross_pnl,
                    4,
                )
                cum_fee = round(
                    safe_float(st.get("cumulative_realized_fee_usd"), 0.0)
                    + fee_usd,
                    4,
                )
                cum_net = round(
                    safe_float(st.get("cumulative_realized_net_usd"), 0.0)
                    + net_pnl,
                    4,
                )
                day_net = round(
                    safe_float(st.get("daily_realized_net_usd"), 0.0)
                    + net_pnl,
                    4,
                )

                updated_st = await context.ledger.update_state(
                    TARGET_SYMBOL,
                    buckets=buckets_to_dicts(buckets),
                    anchor_price=live_p,
                    ratchet_anchor_price=live_p,
                    total_buy_events=tot_buy_ev,
                    total_buy_shares=tot_buy_sh,
                    daily_buy_events=day_buy_ev,
                    total_sell_events=tot_sell_ev,
                    total_sold_shares=tot_sold_sh,
                    win_sell_events=win_ev,
                    daily_sell_events=day_sell_ev,
                    reanchor_cnt=reanchor_cnt,
                    daily_reanchor_cnt=day_reanchor_cnt,
                    cumulative_reanchor_drop_usd=cum_reanchor_drop,
                    cumulative_realized_gross_usd=cum_gross,
                    cumulative_realized_fee_usd=cum_fee,
                    cumulative_realized_net_usd=cum_net,
                    daily_realized_net_usd=day_net,
                )

                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🔥 [무팽창 Always-L1 계단익절 & 리앵커링] {step_q}주 (+{ratchet_gain_pct:.2f}%)",
                        body=(
                            f"기준 ${ratchet_anchor_p:.2f}➔${live_p:.2f} · 세후 +${net_pnl:.2f} 시드산입 · "
                            f"♻️2층단가 ${old_l2_bp:.2f}➔${new_l2_bp:.2f}(-${reanchor_drop:.2f})"
                        ),
                    )

                # 🎯 [핵심 4-2] 리앵커링 직후 낮아진 2층 단가 대비 +rise_sell_l2_pct 도달 시 동일 틱 즉시 졸업!
                new_upper_gain_pct = (
                    ((live_p - new_l2_bp) / new_l2_bp) * 100.0
                    if new_l2_bp > 0.0
                    else 0.0
                )
                if new_upper_gain_pct >= rise_sell_l2_pct:
                    bid1_p = await context.api.get_best_bid_price(
                        TARGET_SYMBOL, fallback_price=live_p
                    )
                    sweep_p = (
                        math.floor((min(live_p, bid1_p) + 1e-9) * 100.0) / 100.0
                        if bid1_p > 0.0
                        else math.floor((live_p + 1e-9) * 100.0) / 100.0
                    )
                    await execute_bulletproof_sell_sweep(
                        context=context,
                        symbol=TARGET_SYMBOL,
                        holdings_qty=buckets[0].qty,
                        sweep_price=sweep_p,
                        reason_tag="SELL_L2_GRAD",
                    )
                    return {
                        "status": "RATCHET_AND_L2_GRADUATION_ORDERED",
                        "did_sell_or_ratchet": True,
                        "net_pnl_usd": net_pnl,
                        "sweep_price": sweep_p,
                    }

                return {
                    "status": "ANTI_BLOAT_RATCHET_STEP_EXECUTED",
                    "did_sell_or_ratchet": True,
                    "step_qty": step_q,
                    "net_pnl_usd": net_pnl,
                    "new_l2_book_price": new_l2_bp,
                    "free_cash_usd": safe_float(
                        updated_st.get("free_cash_usd"), 0.0
                    ),
                }
            else:
                # 가용 현금이 부족하여 래칫 수량이 0인 경우 기준점만 현재가로 갱신
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    ratchet_anchor_price=live_p,
                    anchor_price=live_p,
                )
                return {
                    "status": "RATCHET_ANCHOR_SHIFTED_NO_CASH",
                    "did_sell_or_ratchet": True,
                }

        return {
            "status": "MONITORING_L2_ANTI_BLOAT",
            "did_sell_or_ratchet": False,
        }

    # -----------------------------------------------------------------------
    # [분기 B : 일반 1층(buckets[0]) 보유 상태 — 1분 상시 스나이퍼 매도 판정]
    # -----------------------------------------------------------------------
    floor1_b = buckets[0]
    chg_from_l1_pct = (
        ((live_p - floor1_b.book_price) / floor1_b.book_price) * 100.0
        if floor1_b.book_price > 0.0
        else 0.0
    )

    if chg_from_l1_pct >= rise_sell_l1_pct:
        # 상위 2층(buckets[1:])도 목표 수익률(+rise_sell_l2_pct)에 도달했는지 동시 검사
        sell_all_grad = True
        for b_up in buckets[1:]:
            up_gain = (
                ((live_p - b_up.book_price) / b_up.book_price) * 100.0
                if b_up.book_price > 0.0
                else 0.0
            )
            if up_gain < rise_sell_l2_pct:
                sell_all_grad = False
                break

        if sell_all_grad and len(buckets) >= 2:
            sell_qty = sum(b.qty for b in buckets)
            reason_tag = "SELL_ALL_GRAD"
        elif len(buckets) == 1:
            sell_qty = floor1_b.qty
            reason_tag = "SELL_ALL_GRAD"
        else:
            sell_qty = floor1_b.qty
            reason_tag = "SELL_L1_ONLY"

        bid1_p = await context.api.get_best_bid_price(
            TARGET_SYMBOL, fallback_price=live_p
        )
        min_target_p = (
            math.ceil(
                (floor1_b.book_price * (1.0 + rise_sell_l1_pct / 100.0) - 1e-9)
                * 100.0
            )
            / 100.0
        )
        raw_sweep_p = (
            math.floor((min(live_p, bid1_p) + 1e-9) * 100.0) / 100.0
            if bid1_p > 0.0
            else math.floor((live_p + 1e-9) * 100.0) / 100.0
        )
        sweep_p = max(min_target_p, raw_sweep_p)

        sweep_res = await execute_bulletproof_sell_sweep(
            context=context,
            symbol=TARGET_SYMBOL,
            holdings_qty=sell_qty,
            sweep_price=sweep_p,
            reason_tag=reason_tag,
        )
        return {
            "status": "SNIPER_1M_SELL_ORDERED",
            "did_sell_or_ratchet": True,
            "sell_mode": reason_tag,
            "qty": sell_qty,
            "sweep_price": sweep_p,
            "chg_from_l1_pct": round(chg_from_l1_pct, 2),
            "result": sweep_res,
        }

    return {
        "status": "MONITORING_1M_SNIPER_SELL",
        "did_sell_or_ratchet": False,
        "chg_from_l1_pct": round(chg_from_l1_pct, 2),
        "target_l1_pct": rise_sell_l1_pct,
    }


# 하위 호환 별칭 (타 모듈에서 호출 시 안전 직결)
manage_mechanical_profit_trap_plugin = execute_1m_sniper_sell_and_reanchor_plugin
reconcile_zero_holdings_exit_plugin = sync_pending_sell_order_plugin
