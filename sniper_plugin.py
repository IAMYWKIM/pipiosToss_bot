# -*- coding: utf-8 -*-
"""
# MODIFIED: sniper_plugin.py
순수 롱(SOXL) 전용 '04:07 ~ 16:00 EST 매도 체결 후 60초 강제 타임쉴드(POST_SELL_TO_BUY_SHIELD_SEC = 60.0) &
직전 매수 후 60초 타임쉴드 · 60초 경과 후 실시간 현재가 직접 조회 및 순수 현재가 지정가(LIMIT) 매수 발주 ·
매도 1호가(Ask 1) 참조 및 10초 후 상향 추격 정정 100% 영구 소각(Case 65) ·
전술 A 챔피언(🛡️ 05_AF_WIDE_10_01) : 0주 상태 현재가 지정가 실물 1층(REENTRY_1FL) 산입 ·
2층(MERGED_UPPER) 단독 잔존 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 실물 1층(REENTRY_1FL) 재장전 ·
기억된 사이클 최저 매수가(cycle_min_buy_price) 잠금 기반 10분 고정 4단 와이드 활주로 시드보호 매수 게이트(WIDE_RUNWAY :
1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5%) 및 체결 시 2-Tier 병합·자투리 시드 귀속' 코어 플러그인 (Step 03 / 03)
- MODIFIED (Case 65 핵심 1): 매도 체결 후 60.0초(is_within_post_sell_time_shield) 및 직전 매수 체결 후 60.0초
  (is_within_post_buy_time_shield) 타임쉴드가 경과하기 전에는 일체의 매수 주문 발사를 원천 차단!
  (10분 게이트 도래 시 60초 타임쉴드 중이면 버킷을 소모하지 않고 대기했다가 60초 경과 직후 현재가를 조회해 즉시 판정!)
- MODIFIED (Case 65 핵심 2): 60.0초 타임쉴드 경과 후 매수 조건 판정 및 주문 발주 시 토스 API(/api/v1/prices)로
  실시간 현재가(fresh_live_p)를 직접 조회하여, 매도 1호가(ask1_p) 참조 없이 오직 '현재가 지정가(LIMIT)'로만 매수 발주!
- MODIFIED (Case 65 핵심 3): 미체결 매수 주문을 10초 뒤 매도 1호가(ask1_p)로 상향 추격 정정하던 루틴을 100% 영구 소각하고,
  60.0초(BUY_ORDER_MAX_WAIT_SEC = 60.0) 미체결 시 안전하게 주문 취소(cancel_order) 후 복귀!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 65 완벽 준수
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from quant_engine import (
    BASE_MAX_LOTS_8PCT,
    buckets_from_dicts,
    buckets_to_dicts,
    compute_golden8_quota_qty,
    evaluate_l2_only_hybrid_reentry_signal,
    get_golden8_prog_thresholds,
    resolve_affordable_buy_qty,
    summarize_stratum_portfolio,
    vrev_add_quota_lot_and_merge_2tier,
)
from quant_indicators_plugin import (
    evaluate_buy_gate_bucket_status,
    is_active_trading_window,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    POST_BUY_TO_SELL_SHIELD_SEC,
    POST_SELL_TO_BUY_SHIELD_SEC,
    is_within_post_buy_time_shield,
    is_within_post_sell_time_shield,
)
from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    EST_TZ,
    LOCKED_BUY_INTERVAL_MIN,
    TARGET_SYMBOL,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_plugin")

# 순수 현재가 지정가 매수 주문 발주 후 미체결 최대 대기 시간(초) — 상향 추격 정정 없이 60초 미체결 시 안전 취소
BUY_ORDER_MAX_WAIT_SEC: float = 60.0

# Case 61: 매수 FILLED 직후 토스 잔고 API(/api/v1/holdings) 인덱싱 시차(0.1 ~ 0.4초) 보호 그레이스 쉴드(초)
AUTO_FILL_GRACE_SHIELD_SEC: float = 15.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 매수 체결 직후 잔고 인덱싱 시차 보호 그레이스 쉴드 & 60초 타임쉴드 각인 (Case 61, 65)
# ---------------------------------------------------------------------------
def mark_auto_fill_grace_shield(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    lot_type: str = "QUOTA_L1",
    fill_price: float = 0.0,
    filled_qty: int = 0,
) -> None:
    """
    🚨 Case 61 & Case 65:
    1) 매수 주문이 거래소에서 FILLED 된 직후 토스 잔고 API(/api/v1/holdings) 인덱싱 시차(0.1 ~ 0.4초) 동안
       잔고 동기화 모듈이 방금 산 1층을 수동 매도(0주)로 오판하지 않도록 15.0초 그레이스 쉴드를 각인한다.
    2) 동시에 last_auto_fill_ts(단조 시계)와 last_buy_fill_ts(Epoch)를 기록하여,
       매수 체결 후 60.0초(POST_BUY_TO_SELL_SHIELD_SEC) 동안 매도 및 중복 매수가 발사되지 않도록 타임쉴드를 건다!
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()
    now_epoch = time.time()
    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        prev = guard_map.get(sym) if isinstance(guard_map.get(sym), dict) else {}
        updated = dict(prev)
        updated.update(
            {
                "last_auto_fill_ts": now_mono,
                "last_buy_fill_ts": now_epoch,
                "lot_type": str(lot_type or "QUOTA_L1"),
                "fill_price": safe_float(fill_price, 0.0),
                "filled_qty": safe_int(filled_qty, 0),
            }
        )
        guard_map[sym] = updated


def is_within_auto_fill_grace_shield(
    context: Any, symbol: str = TARGET_SYMBOL
) -> bool:
    """
    Case 61: 최근 15.0초 이내에 SOXL 매수 주문 체결 이력이 있으면 True 를 반환한다 (잔고 화해 보호용).
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    guard_map = getattr(context, "indexing_grace_cache", None)
    if not isinstance(guard_map, dict):
        return False
    info = guard_map.get(sym)
    if not isinstance(info, dict):
        return False
    last_ts = safe_float(info.get("last_auto_fill_ts"), 0.0)
    if last_ts <= 0.0:
        return False
    return (time.monotonic() - last_ts) <= AUTO_FILL_GRACE_SHIELD_SEC


def _get_live_soxl_price(
    context: Any,
    prices: Dict[str, float],
    holdings: Dict[str, Dict[str, Any]],
    now_est: datetime,
) -> float:
    """
    실시간 가격 딕셔너리(prices) -> 캔들 캐시 -> 보유 잔고 현재가 순으로 SOXL 현재가를 추출한다.
    """
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


async def _fetch_fresh_current_soxl_price(
    context: Any,
    fallback_price: float = 0.0,
) -> float:
    """
    🚨 Case 65:
    60초 타임쉴드가 경과한 뒤 매수 조건 판정 및 지정가 주문 발주 직전에
    토스 Open API(/api/v1/prices)를 직접 호출하여 가장 최신의 실시간 현재가를 조회한다.
    """
    try:
        fresh_map = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
        fresh_p = safe_float((fresh_map or {}).get(TARGET_SYMBOL), 0.0)
        if fresh_p > 0.0:
            if isinstance(getattr(context, "candle_metrics_cache", None), dict):
                base_met = context.candle_metrics_cache.get(TARGET_SYMBOL)
                if isinstance(base_met, dict):
                    context.candle_metrics_cache[TARGET_SYMBOL] = (
                        refresh_metrics_with_live_tick(base_met, fresh_p)
                    )
            return fresh_p
    except Exception as exc:
        logger.debug("매수 직전 실시간 현재가 재조회 폴백 사용: %s", exc)
    return safe_float(fallback_price, 0.0)


# ---------------------------------------------------------------------------
# [핵심 1] 매수 체결(FILLED) 커밋 · 2-Tier 배치 · 기억된 저가 잠금 · 매수 후 60초 타임쉴드 각인
# ---------------------------------------------------------------------------
async def _commit_filled_buy_to_stratum_ledger(
    context: Any,
    filled_qty: int,
    avg_fill_price: float,
    portion_budget: float,
    lot_type: str,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & 🎯 4 & Case 25, 46, 53, 59, 60, 61, 65:
    매수 주문 체결(FILLED)이 확인된 즉시:
    1) 실제 소요 원금(actual_spent = filled_qty * avg_fill_price)과
       1회분 정액 예산 대비 절약된 자투리 금액(saved_from_portion = max(0, portion_budget - actual_spent))을 산출한다.
    2) vrev_add_quota_lot_and_merge_2tier() 를 호출하여:
       - 2층(MERGED_UPPER) 단독 보유 중 REENTRY_1FL 체결 시에는 2층을 병합하지 않고 [L1(REENTRY_1FL), L2] 로 독립 배치(Case 59)하고,
       - 4단 와이드 신저가 추매(DIP_1FL / QUOTA_L1) 체결 시에는 신규 매수분을 1층에 두고 기존 지층들을 단일 2층으로 가중평균 병합한다.
    3) 🚨 Case 60: 기억된 사이클 최저 매수가(cycle_min_buy_price)는 0주에서 첫 진입(was_empty)이거나
       기존 최저가보다 더 낮게 산 경우에만 갱신(min(old_min_p, fill_p))하며,
       REENTRY_1FL 재장전 매수가로 절대 상향 덮어쓰지 않고 기존 최저가를 철벽 잠금 유지한다!
    4) 🚨 Case 65: mark_auto_fill_grace_shield() 및 원장 last_buy_fill_ts / entry_ts 를 각인하여,
       매수 체결 직후 60.0초 동안 매도가 나가지 않고 60초 후에 현재가를 조회하여 매도하도록 타임쉴드를 가동한다!
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    old_buckets = buckets_from_dicts(st.get("buckets", []))

    q_int = max(1, safe_int(filled_qty, 1))
    fill_p = round(max(0.01, safe_float(avg_fill_price, 0.01)), 4)
    actual_spent = round(float(q_int) * fill_p, 4)
    tag = str(lot_type or "QUOTA_L1").upper().strip()

    eff_portion_budget = safe_float(portion_budget, 0.0)
    if eff_portion_budget <= 0.0:
        eff_portion_budget = actual_spent
    saved_from_portion = round(max(0.0, eff_portion_budget - actual_spent), 4)

    was_empty = len(old_buckets) == 0
    was_l2_only = bool(
        len(old_buckets) == 1
        and str(old_buckets[0].lot_type).upper() == "MERGED_UPPER"
    )

    new_buckets = vrev_add_quota_lot_and_merge_2tier(
        queue=old_buckets,
        buy_qty=q_int,
        buy_price=fill_p,
        actual_spent_usd=actual_spent,
        lot_type=tag,
    )

    # 🚨 Case 60: 기억된 최저 매수가(cycle_min_buy_price) 잠금!
    old_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    if was_empty or old_min_p <= 0.0:
        new_cycle_min_p = fill_p
    else:
        new_cycle_min_p = min(old_min_p, fill_p)

    new_held_lots = sum(b.lot_count for b in new_buckets)
    new_held_shares = sum(b.qty for b in new_buckets)

    tot_buy_ev = safe_int(st.get("total_buy_events"), 0) + 1
    tot_buy_sh = safe_int(st.get("total_buy_shares"), 0) + q_int
    day_buy_ev = safe_int(st.get("daily_buy_events"), 0) + 1
    extra_13 = safe_int(st.get("extra_13plus_buy_events"), 0) + (
        1 if new_held_lots > BASE_MAX_LOTS_8PCT else 0
    )
    cum_saved = round(
        safe_float(st.get("cumulative_leftover_saved_usd"), 0.0)
        + saved_from_portion,
        4,
    )
    day_saved = round(
        safe_float(st.get("daily_leftover_saved_usd"), 0.0)
        + saved_from_portion,
        4,
    )

    now_buy_epoch = time.time()
    mark_auto_fill_grace_shield(
        context=context,
        symbol=TARGET_SYMBOL,
        lot_type=tag,
        fill_price=fill_p,
        filled_qty=q_int,
    )

    updated_st = await context.ledger.update_state(
        TARGET_SYMBOL,
        buckets=buckets_to_dicts(new_buckets),
        anchor_price=fill_p,
        cycle_min_buy_price=round(new_cycle_min_p, 4),
        ratchet_anchor_price=fill_p,
        buy_order_id="",
        ordered_qty=0,
        buy_price=0.0,
        pending_portion_budget=0.0,
        pending_lot_type="",
        total_buy_events=tot_buy_ev,
        total_buy_shares=tot_buy_sh,
        daily_buy_events=day_buy_ev,
        extra_13plus_buy_events=extra_13,
        cumulative_leftover_saved_usd=cum_saved,
        daily_leftover_saved_usd=day_saved,
        entry_ts=now_buy_epoch,
        last_buy_fill_ts=now_buy_epoch,
    )

    context.wakeup_event.set()
    free_cash_now = safe_float(updated_st.get("free_cash_usd"), 0.0)
    summary = summarize_stratum_portfolio(updated_st, live_price=fill_p)
    l1_target = safe_float(summary.get("l1_target_price"), 0.0)
    trig_buy_p = safe_float(summary.get("trigger_buy_price"), 0.0)
    drop_req_p = safe_float(summary.get("drop_buy_pct"), -1.0)

    if hasattr(context, "send_slim_alert"):
        if was_l2_only and tag == "REENTRY_1FL" and len(new_buckets) >= 2:
            b2 = new_buckets[1]
            title_str = (
                f"🔥 [SOXL 현재가 지정가 1층 재장전 체결] {q_int}주 @ ${fill_p:.2f} (2층 비병합)"
            )
            body_str = (
                f"🛡️60초 타임쉴드 후 1층목표 ${l1_target:.2f}(+1.5%) 감시 · 2층({b2.qty}주@${b2.book_price:.2f}) 유지 · "
                f"기억저가 ${new_cycle_min_p:.2f}(와이드 {drop_req_p:.1f}% 추매가 ${trig_buy_p:.2f}) · "
                f"가용현금 ${free_cash_now:,.2f}"
            )
        else:
            l2_note = ""
            if len(new_buckets) >= 2:
                b2 = new_buckets[1]
                l2_note = f" · 2층({b2.qty}주@${b2.book_price:.2f}) 병합"
            title_str = (
                f"🔵 [SOXL {new_held_lots}회차 현재가 지정가 매수 체결] {q_int}주 @ ${fill_p:.2f}"
            )
            body_str = (
                f"실사용 ${actual_spent:,.2f} (자투리 +${saved_from_portion:.2f} 시드보존) "
                f"· 🛡️60초 타임쉴드 후 1층목표 ${l1_target:.2f} 감시{l2_note} · 차기단차 {drop_req_p:.1f}%(${trig_buy_p:.2f}) "
                f"· 가용현금 ${free_cash_now:,.2f} (총 {new_held_shares}주)"
            )
        await context.send_slim_alert(title=title_str, body=body_str)

    return {
        "status": "BUY_FILLED_AND_MERGED",
        "symbol": TARGET_SYMBOL,
        "lot_type": tag,
        "filled_qty": q_int,
        "fill_price": fill_p,
        "actual_spent_usd": actual_spent,
        "saved_from_portion_usd": saved_from_portion,
        "cycle_min_buy_price": round(new_cycle_min_p, 4),
        "held_lots": new_held_lots,
        "held_shares": new_held_shares,
        "free_cash_usd": free_cash_now,
    }


async def sync_pending_buy_order_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 5 & Case 19, 24, 59, 60, 61, 65:
    장부에 대기 중인 미체결 매수 주문(buy_order_id)이 존재할 때:
    1) GET /api/v1/orders/{orderId} 로 실제 체결 상태(FILLED / PARTIAL_FILLED / PENDING / CANCELED)를 조회한다.
    2) FILLED 확인 즉시 실제 평균 체결가(execution.averageFilledPrice)와 체결 수량(filledQuantity)으로
       2-Tier 지층 배치/병합, 자투리 시드 귀속 및 매수 후 60초 타임쉴드 각인을 완료한다.
    3) 🚨 [Case 65 핵심 수술]: 10초 후 매도 1호가(Ask 1)로 가격을 높여 추격 정정하던 루틴을 100% 영구 소각!
       현재가 지정가로 발주된 매수 주문이 60.0초(BUY_ORDER_MAX_WAIT_SEC) 동안 체결되지 않으면,
       가격을 올리지 않고 미체결 주문을 안전하게 취소(cancel_order)한 뒤 관망 복귀한다.
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    buy_oid = str(st.get("buy_order_id") or "").strip()
    if not buy_oid:
        return {"status": "NO_PENDING_BUY"}

    ordered_qty = max(1, safe_int(st.get("ordered_qty"), 1))
    ord_price = safe_float(st.get("buy_price"), 0.0)
    portion_budget = safe_float(
        st.get("pending_portion_budget"), float(ordered_qty) * ord_price
    )
    lot_type = str(st.get("pending_lot_type") or "QUOTA_L1")

    detail = await context.api.get_order_detail(buy_oid)
    if isinstance(detail, dict):
        ord_status = str(detail.get("status") or "").upper().strip()
        exec_obj = (
            detail.get("execution")
            if isinstance(detail.get("execution"), dict)
            else {}
        )
        filled_q = safe_int(exec_obj.get("filledQuantity"), 0)
        avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)

        # 1) 전량 체결 완료(FILLED) 또는 부분 체결 후 종료된 경우
        if ord_status == "FILLED" or (
            filled_q > 0 and ord_status in ("CANCELED", "REJECTED", "REPLACED")
        ):
            eff_q = filled_q if filled_q > 0 else ordered_qty
            eff_p = avg_fill_p if avg_fill_p > 0.0 else ord_price
            return await _commit_filled_buy_to_stratum_ledger(
                context=context,
                filled_qty=eff_q,
                avg_fill_price=eff_p,
                portion_budget=portion_budget,
                lot_type=lot_type,
            )

        # 2) 부분 체결(PARTIAL_FILLED) 진행 중인 경우
        if ord_status == "PARTIAL_FILLED" or filled_q > 0:
            elapsed = time.monotonic() - safe_float(
                st.get("last_modify_mono"), time.monotonic()
            )
            if elapsed >= BUY_ORDER_MAX_WAIT_SEC:
                try:
                    await context.api.cancel_order(buy_oid)
                except Exception:
                    pass
                eff_p = avg_fill_p if avg_fill_p > 0.0 else ord_price
                return await _commit_filled_buy_to_stratum_ledger(
                    context=context,
                    filled_qty=filled_q,
                    avg_fill_price=eff_p,
                    portion_budget=portion_budget,
                    lot_type=lot_type,
                )
            return {
                "status": "BUY_PARTIAL_FILLED_WAITING",
                "filled_qty": filled_q,
            }

        # 3) 미체결 상태로 취소/거부된 경우 장부 락 해제
        if ord_status in ("CANCELED", "REJECTED") and filled_q == 0:
            await context.ledger.update_state(
                TARGET_SYMBOL,
                buy_order_id="",
                ordered_qty=0,
                buy_price=0.0,
                pending_portion_budget=0.0,
                pending_lot_type="",
            )
            return {"status": f"BUY_ORDER_{ord_status}_CLEARED"}

    # 🚨 4) [Case 65] 매도 1호가(Ask 1) 상향 추격 정정 완전 소각!
    # 현재가 지정가 주문이 60.0초 이상 미체결 표류 시 안전하게 취소하고 다음 타점을 대기한다.
    last_mod = safe_float(st.get("last_modify_mono"), 0.0)
    elapsed_mod = time.monotonic() - last_mod if last_mod > 0.0 else 0.0
    if elapsed_mod >= BUY_ORDER_MAX_WAIT_SEC:
        try:
            await context.api.cancel_order(buy_oid)
        except Exception:
            pass

        # 취소 직전 체결되었는지 최종 확인
        final_detail = await context.api.get_order_detail(buy_oid)
        if isinstance(final_detail, dict):
            f_status = str(final_detail.get("status") or "").upper().strip()
            f_exec = (
                final_detail.get("execution")
                if isinstance(final_detail.get("execution"), dict)
                else {}
            )
            f_qty = safe_int(f_exec.get("filledQuantity"), 0)
            f_avg = safe_float(f_exec.get("averageFilledPrice"), 0.0)
            if f_status == "FILLED" or f_qty > 0:
                return await _commit_filled_buy_to_stratum_ledger(
                    context=context,
                    filled_qty=(f_qty if f_qty > 0 else ordered_qty),
                    avg_fill_price=(f_avg if f_avg > 0.0 else ord_price),
                    portion_budget=portion_budget,
                    lot_type=lot_type,
                )

        await context.ledger.update_state(
            TARGET_SYMBOL,
            buy_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            pending_portion_budget=0.0,
            pending_lot_type="",
        )
        logger.info(
            "⏳ [SOXL 현재가 지정가 매수 60초 미체결 안전 취소] 지정가 $%.2f 유지 종료 ➔ 관망 복귀",
            ord_price,
        )
        return {"status": "BUY_ORDER_UNFILLED_CANCELED_SAFE"}

    return {"status": "BUY_ORDER_PENDING", "orderId": buy_oid}


# ---------------------------------------------------------------------------
# [핵심 2] 전술 A (05_AF_WIDE_10_01) : 매도 후 60초 타임쉴드 & 현재가 지정가 매수 게이트
# ---------------------------------------------------------------------------
async def execute_seed_protected_buy_gate_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1, 🎯 2, 🎯 4 & Case 25 ~ 65 (전술 A 챔피언 : 🛡️ 05_AF_WIDE_10_01 + 60초 타임쉴드 & 현재가 지정가):
    04:07:00 ~ 16:00:00 EST 운용 구간에서:
    1) 미체결 매수/매도 주문이 대기 중이면 중복 매수 발사를 차단한다.
    2) 🛡️ [Case 65 : 매도 후 60.0초 타임쉴드 & 직전 매수 후 60.0초 타임쉴드 강제 검문]
       - 직전 매도 체결로부터 60.0초(POST_SELL_TO_BUY_SHIELD_SEC) 또는 직전 매수 체결로부터 60.0초
         (POST_BUY_TO_SELL_SHIELD_SEC)가 경과하지 않았으면, 0주 진입 및 10분 게이트 매수를 포함한
         모든 매수를 즉시 보류(HOLD_POST_SELL_60S_TIME_SHIELD / HOLD_POST_BUY_60S_TIME_SHIELD)한다!
       - 타임쉴드 중에는 10분 버킷(last_evaluated_buy_bucket_id)을 소모하지 않으므로,
         60.0초가 경과하는 즉시 최신 현재가를 조회하여 매수 판정을 수행한다!
    3) 🔥 [분기 A : 보유 잔고 0주 상태 (is_zero_shares) — 60초 타임쉴드 경과 후 현재가 조회 및 현재가 지정가 실물 1층 산입]
       - 60초 경과 후 _fetch_fresh_current_soxl_price() 로 실시간 현재가(fresh_live_p)를 직접 조회하여,
         매도 1호가(Ask 1) 참조 없이 오직 '현재가(fresh_live_p)를 지정가(LIMIT)'로 8% 정량을 매수 발주한다!
    4) 🛡️ [분기 B : 지층 보유 상태 (len(buckets) > 0) — 10분 고정 시드보호 매수 게이트 검문 (Case 36, 64, 65)]
       - 60초 타임쉴드가 경과한 상태에서 10분 게이트 오픈 시 _fetch_fresh_current_soxl_price() 로
         실시간 현재가(fresh_live_p)를 새로 조회한다.
       - ♻️ [하위 분기 B-1 : 2층(MERGED_UPPER) 단독 보유 상태 (Case 59)]
         * 조회된 현재가(fresh_live_p)가 직전 익절가 대비 -1.00% 이하 눌림목 OR +1.00% 이상 상승돌파 시에만
           매도 1호가 참조 없이 '현재가(fresh_live_p)를 지정가(LIMIT)'로 REENTRY_1FL 독립 1층 주문을 발주한다!
       - 📐 [하위 분기 B-2 : 실물 1층 보유 중 — 기억된 최저가 기준 4단 와이드 활주로 추매 (WIDE_RUNWAY, Case 60)]
         * 조회된 현재가(fresh_live_p)가 기억된 최저가 대비 4단 와이드 단차(-1.0% / -2.0% / -3.5% / -5.5%) 이하
           신저가에 도달했을 때만 '현재가(fresh_live_p)를 지정가(LIMIT)'로 DIP_1FL 추매 주문을 발주한다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute

    # 1) 미체결 매수 주문이 있으면 먼저 체결 확인 수행
    st = context.ledger.get_state(TARGET_SYMBOL)
    if str(st.get("buy_order_id") or "").strip():
        return await sync_pending_buy_order_plugin(
            context=context,
            holdings=holdings,
            prices=prices,
            now_est=dt_now,
        )

    # 2) 04:07 ~ 16:00 EST 운용 시간대 및 마스터 전원(is_active) 확인
    if not is_active_trading_window(hm):
        return {"status": "OUTSIDE_TRADING_WINDOW"}

    if not bool(st.get("is_active", True)):
        return {"status": "BOT_INACTIVE"}

    # 미체결 매도 주문이 진행 중이면 매도 완료까지 대기 (동일 틱 자전거래 방지, Case 44)
    if str(st.get("sell_order_id") or "").strip():
        return {"status": "SELL_ORDER_IN_PROGRESS"}

    # 🛡️ 3) [Case 65] 매도 체결 후 60.0초 타임쉴드 & 직전 매수 체결 후 60.0초 타임쉴드 강제 검문!
    # 60초가 지나기 전에는 10분 게이트 버킷을 소모하지 않고 대기했다가, 60초가 지난 직후 현재가를 조회해 매수한다!
    in_sell_shield, remain_sell_sec = is_within_post_sell_time_shield(
        context, st, TARGET_SYMBOL
    )
    if in_sell_shield:
        return {
            "status": "HOLD_POST_SELL_60S_TIME_SHIELD",
            "remain_shield_sec": remain_sell_sec,
        }

    in_buy_shield, remain_buy_sec = is_within_post_buy_time_shield(
        context, st, TARGET_SYMBOL
    )
    if in_buy_shield:
        return {
            "status": "HOLD_POST_BUY_60S_TIME_SHIELD",
            "remain_shield_sec": remain_buy_sec,
        }

    live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    if live_p <= 0.0:
        return {"status": "NO_LIVE_PRICE"}

    buckets = buckets_from_dicts(st.get("buckets", []))
    h_qty = safe_int((holdings or {}).get(TARGET_SYMBOL, {}).get("qty"), 0)

    init_seed = max(
        100.0,
        safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD),
    )
    free_cash = max(0.0, safe_float(st.get("free_cash_usd"), 0.0))
    actual_invested = sum(b.actual_cost_usd for b in buckets)
    cost_equity = free_cash + actual_invested
    portion_mode = str(st.get("portion_mode") or "COMP_08PCT").upper()
    threshold_mode = str(st.get("threshold_mode") or "08_VREV_PROG").upper()
    buy_interval_min = LOCKED_BUY_INTERVAL_MIN

    is_zero_shares = bool(len(buckets) == 0 and h_qty == 0)
    is_l2_only_vacant_l1 = bool(
        len(buckets) == 1
        and str(buckets[0].lot_type).upper() == "MERGED_UPPER"
    )

    # -----------------------------------------------------------------------
    # [분기 A : 보유 잔고 0주 상태 (is_zero_shares)]
    # -> 60초 타임쉴드 경과 확인 완료 후 실시간 현재가를 직접 조회하여 '현재가 지정가(LIMIT)'로 1층 산입!
    # -----------------------------------------------------------------------
    if is_zero_shares:
        async with context.in_memory_ordering_lock:
            async with context.dual_strike_mutex:
                st_now = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_now.get("buy_order_id") or "").strip() or str(
                    st_now.get("sell_order_id") or ""
                ).strip():
                    return {"status": "ALREADY_ORDERING_IN_MUTEX"}

                now_buckets = buckets_from_dicts(st_now.get("buckets", []))
                if len(now_buckets) > 0:
                    return {"status": "L1_ALREADY_FILLED_IN_MUTEX"}

                # 60초 타임쉴드 재검증 (뮤텍스 내부)
                in_s_sh, _ = is_within_post_sell_time_shield(
                    context, st_now, TARGET_SYMBOL
                )
                in_b_sh, _ = is_within_post_buy_time_shield(
                    context, st_now, TARGET_SYMBOL
                )
                if in_s_sh or in_b_sh:
                    return {"status": "HOLD_60S_TIME_SHIELD_IN_MUTEX"}

                # 🔍 60초 경과 후 실시간 현재가(fresh_live_p) 직접 조회 (매도 1호가 ask1_p 참조 없음!)
                fresh_live_p = await _fetch_fresh_current_soxl_price(
                    context, fallback_price=live_p
                )
                if fresh_live_p <= 0.0:
                    return {"status": "NO_FRESH_LIVE_PRICE"}

                exec_order_p = max(
                    0.01, math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0
                )

                target_qty, portion_budget = compute_golden8_quota_qty(
                    anchor_price=exec_order_p,
                    current_total_equity=cost_equity,
                    initial_seed_usd=init_seed,
                    portion_mode=portion_mode,
                )
                bp_usd = await context.api.get_buying_power_usd(
                    safety_buffer_pct=0.2
                )
                buy_qty, _est_spent = resolve_affordable_buy_qty(
                    target_qty=target_qty,
                    price_t=exec_order_p,
                    free_cash_usd=free_cash,
                    broker_buying_power_usd=(bp_usd if bp_usd > 0.0 else None),
                )

                if buy_qty <= 0:
                    if hasattr(context, "send_strike_hold_alert_once"):
                        await context.send_strike_hold_alert_once(
                            symbol=TARGET_SYMBOL,
                            reason_key="INIT_CASH_SHORT",
                            message=(
                                f"0주 실물 1층 산입 보류: 가용현금(${free_cash:.2f}) / "
                                f"현재가 지정가(${exec_order_p:.2f}) 부족"
                            ),
                        )
                    return {"status": "INSUFFICIENT_CASH_FOR_0SH_ENTRY"}

                _, cur_bucket_id, today_str, _ = evaluate_buy_gate_bucket_status(
                    now_est=dt_now,
                    buy_interval_min=buy_interval_min,
                    last_evaluated_bucket_id=safe_int(
                        st_now.get("last_evaluated_buy_bucket_id"), -1
                    ),
                    last_evaluated_date=str(
                        st_now.get("last_evaluated_date") or ""
                    ),
                )

                cid = generate_client_order_id(prefix="INI1FL")
                try:
                    ord_res = await context.api.create_order(
                        symbol=TARGET_SYMBOL,
                        side="BUY",
                        quantity=buy_qty,
                        price=exec_order_p,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        client_order_id=cid,
                        enforce_daymarket_block=True,
                    )
                except Exception as exc:
                    logger.warning(
                        "0주 현재가 지정가 실물 1층(REENTRY_1FL) 매수 주문 예외: %s",
                        exc,
                    )
                    return {"status": "ORDER_ERROR", "error": str(exc)}

                oid = str(ord_res.get("orderId") or "").strip()
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    buy_order_id=oid,
                    client_order_id=cid,
                    ordered_qty=buy_qty,
                    buy_price=exec_order_p,
                    pending_portion_budget=portion_budget,
                    pending_lot_type="REENTRY_1FL",
                    last_evaluated_buy_bucket_id=cur_bucket_id,
                    last_evaluated_date=today_str,
                    last_modify_mono=time.monotonic(),
                )
                context.wakeup_event.set()
                logger.info(
                    "🔵 [SOXL 0주 현재가 지정가 1층 매수 발주] %d주 @ 지정가 $%.2f (orderId=%s)",
                    buy_qty,
                    exec_order_p,
                    oid,
                )
                return {
                    "status": "ZERO_SHARES_IMMEDIATE_1FL_ORDERED",
                    "orderId": oid,
                    "qty": buy_qty,
                    "price": exec_order_p,
                }

    # -----------------------------------------------------------------------
    # [분기 B : 지층 보유 중 (len(buckets) > 0)]
    # -> 오직 10분 고정 매수 게이트 신규 버킷 도래 시에만 검문 수행! (Case 36, 64)
    # -----------------------------------------------------------------------
    is_gate_open, cur_bucket_id, today_str, b_info = (
        evaluate_buy_gate_bucket_status(
            now_est=dt_now,
            buy_interval_min=buy_interval_min,
            last_evaluated_bucket_id=safe_int(
                st.get("last_evaluated_buy_bucket_id"), -1
            ),
            last_evaluated_date=str(st.get("last_evaluated_date") or ""),
        )
    )

    if not is_gate_open:
        return {
            "status": "WAIT_NEXT_BUY_GATE",
            "next_gate": b_info.get("next_gate_hm_str", ""),
        }

    # 🔍 60초 타임쉴드 경과 후 10분 매수 게이트 판정을 위해 최신 실시간 현재가(fresh_live_p) 직접 조회!
    fresh_live_p = await _fetch_fresh_current_soxl_price(
        context, fallback_price=live_p
    )
    if fresh_live_p <= 0.0:
        return {"status": "NO_FRESH_LIVE_PRICE"}

    # -----------------------------------------------------------------------
    # [하위 분기 B-1 : 2층(MERGED_UPPER) 단독 보유 상태 — 눌림(-1.0%) OR 돌파(+1.0%) 독립 1층 재장전 (Case 59, 65)]
    # -----------------------------------------------------------------------
    if is_l2_only_vacant_l1:
        anchor_p = safe_float(st.get("anchor_price"), buckets[0].book_price)
        reentry_sig = evaluate_l2_only_hybrid_reentry_signal(
            live_price=fresh_live_p,
            anchor_price=anchor_p,
            l2_book_price=buckets[0].book_price,
        )

        # 고점 부근 어정쩡한 횡보 구간(-1.00% < chg < +1.00%)이면 매수를 패스하여 데드캣 이중 소진 차단!
        if not bool(reentry_sig.get("should_buy")):
            new_pass_cnt = safe_int(st.get("seed_protect_pass_cnt"), 0) + 1
            new_day_pass = (
                safe_int(st.get("daily_seed_protect_pass_cnt"), 0) + 1
            )
            await context.ledger.update_state(
                TARGET_SYMBOL,
                last_evaluated_buy_bucket_id=cur_bucket_id,
                last_evaluated_date=today_str,
                seed_protect_pass_cnt=new_pass_cnt,
                daily_seed_protect_pass_cnt=new_day_pass,
            )
            return {
                "status": "L2_ONLY_REENTRY_HOLD_PASSED",
                "ref_anchor": reentry_sig.get("ref_anchor"),
                "chg_from_exit_pct": reentry_sig.get("chg_from_exit_pct"),
                "seed_protect_pass_cnt": new_pass_cnt,
            }

        async with context.in_memory_ordering_lock:
            async with context.dual_strike_mutex:
                st_now = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_now.get("buy_order_id") or "").strip() or str(
                    st_now.get("sell_order_id") or ""
                ).strip():
                    return {"status": "ALREADY_ORDERING_IN_MUTEX"}

                # 매도 1호가(ask1_p) 참조 없이 오직 조회된 실시간 현재가(fresh_live_p)를 지정가로 사용!
                exec_order_p = max(
                    0.01, math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0
                )

                quota_ref_p = max(
                    0.01,
                    safe_float(
                        reentry_sig.get("quota_ref_price"), exec_order_p
                    ),
                )
                target_qty, portion_budget = compute_golden8_quota_qty(
                    anchor_price=quota_ref_p,
                    current_total_equity=cost_equity,
                    initial_seed_usd=init_seed,
                    portion_mode=portion_mode,
                )

                bp_usd = await context.api.get_buying_power_usd(
                    safety_buffer_pct=0.2
                )
                buy_qty, _est_spent = resolve_affordable_buy_qty(
                    target_qty=target_qty,
                    price_t=exec_order_p,
                    free_cash_usd=free_cash,
                    broker_buying_power_usd=(bp_usd if bp_usd > 0.0 else None),
                )

                if buy_qty <= 0:
                    new_pass_cnt = (
                        safe_int(st_now.get("seed_protect_pass_cnt"), 0) + 1
                    )
                    new_day_pass = (
                        safe_int(st_now.get("daily_seed_protect_pass_cnt"), 0)
                        + 1
                    )
                    await context.ledger.update_state(
                        TARGET_SYMBOL,
                        last_evaluated_buy_bucket_id=cur_bucket_id,
                        last_evaluated_date=today_str,
                        seed_protect_pass_cnt=new_pass_cnt,
                        daily_seed_protect_pass_cnt=new_day_pass,
                    )
                    return {"status": "HOLD_L2_ONLY_NO_CASH_FOR_REENTRY"}

                cid = generate_client_order_id(prefix="RE1FL")
                try:
                    ord_res = await context.api.create_order(
                        symbol=TARGET_SYMBOL,
                        side="BUY",
                        quantity=buy_qty,
                        price=exec_order_p,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        client_order_id=cid,
                        enforce_daymarket_block=True,
                    )
                except Exception as exc:
                    logger.warning(
                        "2층 보유 중 하이브리드 현재가 지정가 1층(REENTRY_1FL) 재장전 주문 예외: %s",
                        exc,
                    )
                    return {"status": "ORDER_ERROR", "error": str(exc)}

                oid = str(ord_res.get("orderId") or "").strip()
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    buy_order_id=oid,
                    client_order_id=cid,
                    ordered_qty=buy_qty,
                    buy_price=exec_order_p,
                    pending_portion_budget=portion_budget,
                    pending_lot_type="REENTRY_1FL",
                    last_evaluated_buy_bucket_id=cur_bucket_id,
                    last_evaluated_date=today_str,
                    last_modify_mono=time.monotonic(),
                )
                context.wakeup_event.set()
                logger.info(
                    "🔥 [SOXL 2층 보유 중 현재가 지정가 1층 재장전(%s) 발주] %d주 @ 지정가 $%.2f (orderId=%s)",
                    reentry_sig.get("signal_type"),
                    buy_qty,
                    exec_order_p,
                    oid,
                )
                return {
                    "status": "L2_HELD_HYBRID_REAL_1FL_ORDERED",
                    "signal_type": reentry_sig.get("signal_type"),
                    "orderId": oid,
                    "qty": buy_qty,
                    "price": exec_order_p,
                }

    # -----------------------------------------------------------------------
    # [하위 분기 B-2 : 실물 1층 보유 중 — 기억된 최저가 기준 4단 와이드 신저가 추매 (WIDE_RUNWAY, Case 60, 65)]
    # -----------------------------------------------------------------------
    held_lots = sum(b.lot_count for b in buckets)
    drop_buy_pct, _, _ = get_golden8_prog_thresholds(
        held_lots=held_lots, threshold_mode=threshold_mode
    )

    anchor_p = safe_float(st.get("anchor_price"), buckets[0].book_price)
    cycle_min_p = safe_float(st.get("cycle_min_buy_price"), anchor_p)

    # 🚨 Case 60: 추가 매수(물타기) 판정 기준가는 반드시 기억된 사이클 최저 매수가로 잠금!
    valid_refs = [
        p for p in (buckets[0].book_price, anchor_p, cycle_min_p) if p > 0.0
    ]
    ref_buy_anchor = min(valid_refs) if valid_refs else buckets[0].book_price
    lot_tag = "DIP_1FL"

    chg_for_buy_pct = (
        ((fresh_live_p - ref_buy_anchor) / ref_buy_anchor) * 100.0
        if ref_buy_anchor > 0.0
        else 0.0
    )

    # 🛡️ [시드보호 패스 판정] 기억된 최저가 대비 4단 와이드 임계치(-1.0% / -2.0% / -3.5% / -5.5%) 미충족 시 100% 패스!
    if chg_for_buy_pct > drop_buy_pct:
        new_pass_cnt = safe_int(st.get("seed_protect_pass_cnt"), 0) + 1
        new_day_pass = safe_int(st.get("daily_seed_protect_pass_cnt"), 0) + 1
        await context.ledger.update_state(
            TARGET_SYMBOL,
            last_evaluated_buy_bucket_id=cur_bucket_id,
            last_evaluated_date=today_str,
            seed_protect_pass_cnt=new_pass_cnt,
            daily_seed_protect_pass_cnt=new_day_pass,
        )
        return {
            "status": "SEED_PROTECT_PASSED",
            "ref_buy_anchor": round(ref_buy_anchor, 4),
            "chg_pct": round(chg_for_buy_pct, 2),
            "required_drop_pct": drop_buy_pct,
            "seed_protect_pass_cnt": new_pass_cnt,
        }

    # 🎯 [4단 와이드 신저가 하락 조건 충족] 기억된 최저가(ref_buy_anchor)로 8% 목표 정량(target_qty)을 확정한 뒤
    # 실제 매수는 매도 1호가(ask1_p) 참조 없이 '조회된 현재가(fresh_live_p)를 지정가(LIMIT)'로 발주한다!
    async with context.in_memory_ordering_lock:
        async with context.dual_strike_mutex:
            st_now = context.ledger.get_state(TARGET_SYMBOL)
            if str(st_now.get("buy_order_id") or "").strip():
                return {"status": "ALREADY_ORDERING_IN_MUTEX"}

            target_qty, portion_budget = compute_golden8_quota_qty(
                anchor_price=ref_buy_anchor,
                current_total_equity=cost_equity,
                initial_seed_usd=init_seed,
                portion_mode=portion_mode,
            )

            max_trigger_p = (
                math.floor(
                    (
                        ref_buy_anchor * (1.0 + drop_buy_pct / 100.0)
                        + 1e-9
                    )
                    * 100.0
                )
                / 100.0
            )
            cur_limit_p = (
                math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0
            )
            exec_order_p = max(0.01, min(cur_limit_p, max_trigger_p))

            bp_usd = await context.api.get_buying_power_usd(
                safety_buffer_pct=0.2
            )
            buy_qty, _est_spent = resolve_affordable_buy_qty(
                target_qty=target_qty,
                price_t=exec_order_p,
                free_cash_usd=free_cash,
                broker_buying_power_usd=(bp_usd if bp_usd > 0.0 else None),
            )

            if buy_qty <= 0:
                new_pass_cnt = (
                    safe_int(st_now.get("seed_protect_pass_cnt"), 0) + 1
                )
                new_day_pass = (
                    safe_int(st_now.get("daily_seed_protect_pass_cnt"), 0) + 1
                )
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    last_evaluated_buy_bucket_id=cur_bucket_id,
                    last_evaluated_date=today_str,
                    seed_protect_pass_cnt=new_pass_cnt,
                    daily_seed_protect_pass_cnt=new_day_pass,
                )
                if hasattr(context, "send_strike_hold_alert_once"):
                    await context.send_strike_hold_alert_once(
                        symbol=TARGET_SYMBOL,
                        reason_key="CASH_EXHAUSTED_SKIP",
                        message=(
                            f"기억저가(${ref_buy_anchor:.2f}) 대비 와이드 신저가({chg_for_buy_pct:.2f}% <= {drop_buy_pct:.2f}%) 도달했으나 "
                            f"가용현금(${free_cash:.2f}) 부족으로 추매 보류 (보유 {held_lots}회차)"
                        ),
                    )
                return {"status": "CASH_EXHAUSTED_SKIP"}

            cid = generate_client_order_id(prefix=f"B{held_lots + 1}L")
            try:
                ord_res = await context.api.create_order(
                    symbol=TARGET_SYMBOL,
                    side="BUY",
                    quantity=buy_qty,
                    price=exec_order_p,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                    enforce_daymarket_block=True,
                )
            except Exception as exc:
                logger.error(
                    "SOXL 4단 와이드 현재가 지정가 추매 주문 발사 예외: %s", exc
                )
                return {"status": "BUY_ORDER_ERROR", "error": str(exc)}

            oid = str(ord_res.get("orderId") or "").strip()
            await context.ledger.update_state(
                TARGET_SYMBOL,
                buy_order_id=oid,
                client_order_id=cid,
                ordered_qty=buy_qty,
                buy_price=exec_order_p,
                pending_portion_budget=portion_budget,
                pending_lot_type=lot_tag,
                last_evaluated_buy_bucket_id=cur_bucket_id,
                last_evaluated_date=today_str,
                last_modify_mono=time.monotonic(),
            )
            context.wakeup_event.set()
            logger.info(
                "🔵 [SOXL %d회차 현재가 지정가 추매 발주] %d주 @ 지정가 $%.2f (기억저가 $%.2f 대비 %.2f%% · orderId=%s)",
                held_lots + 1,
                buy_qty,
                exec_order_p,
                ref_buy_anchor,
                chg_for_buy_pct,
                oid,
            )
            return {
                "status": "QUOTA_DIP_BUY_ORDERED",
                "orderId": oid,
                "qty": buy_qty,
                "price": exec_order_p,
                "ref_buy_anchor": round(ref_buy_anchor, 4),
                "chg_pct": round(chg_for_buy_pct, 2),
                "drop_threshold_pct": drop_buy_pct,
            }


# 하위 호환 별칭 (외부 모듈에서 구 함수명으로 호출하더라도 신규 게이트 엔진으로 안전 직결)
execute_premarket_vwap_sniper_plugin = execute_seed_protected_buy_gate_plugin
sync_dynamic_vwap_buy_order_modify = sync_pending_buy_order_plugin
