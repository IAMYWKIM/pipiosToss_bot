# -*- coding: utf-8 -*-
"""
# MODIFIED: sniper_exit_plugin.py
순수 롱(SOXL) 전용 '04:07 ~ 16:00 EST 매수 체결 후 60초 강제 타임쉴드(POST_BUY_TO_SELL_SHIELD_SEC = 60.0) ·
60초 경과 후 실시간 현재가 직접 조회 및 현재가 >= 목표가(1층 +1.50% / 2층 +1.00%)일 때만 현재가 지정가(LIMIT) 매도 발주 ·
매수 1호가(Bid 1) 참조 및 하향 추격 정정 100% 영구 소각(Case 65) ·
단일 지층 실물 수익금 리앵커링(vrev_reanchor_single_tier_quota · 0.994 버퍼) ·
매도 체결 즉시 매도 후 60초 타임쉴드(last_sell_fill_ts) 각인 및 분리 회계 복리 정산' 코어 플러그인 (Step 02 / 03)
- MODIFIED (Case 65 핵심 1): 매수 체결 후 60.0초 동안은 일체의 매도 판정을 물리적으로 차단하는
  is_within_post_buy_time_shield() 타임쉴드를 장착하여 매수 직후 휩소 매도를 100% 원천 차단!
- MODIFIED (Case 65 핵심 2): 60.0초 타임쉴드 경과 후 매도 조건 도달 시 실시간 현재가(get_current_prices_batch)를
  직접 재조회하여 현재가가 목표가(min_target_p) 이상일 때만 '현재가 지정가(LIMIT)'로 매도 주문을 발주!
  (매수 1호가 bid1_p 참조 및 3초 후 매수 1호가 하향 추격 정정 루틴을 100% 영구 소각하여 손절 체결을 0원으로 봉쇄!)
- MODIFIED (Case 65 핵심 3): 매도 체결 완료 시 원장(last_sell_fill_ts) 및 인메모리 캐시에 매도 시각을 각인하여
  sniper_plugin.py 가 매도 후 60.0초 타임쉴드 경과 뒤에 현재가를 조회하고 매수하도록 양방향 결속!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 65 완벽 준수
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from quant_engine import (
    StratumBucket,
    buckets_from_dicts,
    buckets_to_dicts,
    calc_sell_realized_pnl,
    get_golden8_prog_thresholds,
    vrev_reanchor_single_tier_quota,
)
from quant_indicators_plugin import (
    is_active_trading_window,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    EST_TZ,
    TARGET_SYMBOL,
    _NonRetryableApiError,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_exit_plugin")

# NEW (Case 65): 매수 체결 후 60.0초 동안 매도 금지 타임쉴드 & 매도 체결 후 60.0초 동안 매수 금지 타임쉴드 상수
POST_BUY_TO_SELL_SHIELD_SEC: float = 60.0
POST_SELL_TO_BUY_SHIELD_SEC: float = 60.0

# 순수 현재가 지정가 매도 주문 발주 후 미체결 최대 대기 시간(초) — 하향 정정 없이 60초 미체결 시 안전 취소
SELL_ORDER_MAX_WAIT_SEC: float = 60.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 실시간 SOXL 현재가 조회 · 60초 양방향 타임쉴드 판별 · 스냅샷 백업
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


async def _fetch_fresh_current_soxl_price(
    context: Any,
    fallback_price: float = 0.0,
) -> float:
    """
    🚨 Case 65:
    60초 타임쉴드 경과 후 매도 주문 발주 직전에 토스 Open API(/api/v1/prices)를 직접 호출하여
    가장 최신의 실시간 현재가를 조회한다.
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
        logger.debug("실시간 현재가 재조회 폴백 사용: %s", exc)
    return safe_float(fallback_price, 0.0)


def is_within_post_buy_time_shield(
    context: Any,
    st: Optional[Dict[str, Any]] = None,
    symbol: str = TARGET_SYMBOL,
) -> Tuple[bool, float]:
    """
    🚨 Case 65 [매수 체결 후 60.0초 강제 타임쉴드]:
    매수가 체결된 시점(last_auto_fill_ts 단조 시계 및 원장 last_buy_fill_ts / entry_ts)으로부터
    60.0초가 경과하기 전에는 True 와 남은 초(remain_sec)를 반환하여 일체의 매도 시도를 차단한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        info = guard_map.get(sym)
        if isinstance(info, dict):
            last_buy_mono = safe_float(info.get("last_auto_fill_ts"), 0.0)
            if last_buy_mono > 0.0:
                elapsed_mono = time.monotonic() - last_buy_mono
                if 0.0 <= elapsed_mono < POST_BUY_TO_SELL_SHIELD_SEC:
                    return True, round(
                        POST_BUY_TO_SELL_SHIELD_SEC - elapsed_mono, 1
                    )

    state_obj = (
        st
        if isinstance(st, dict)
        else (
            context.ledger.get_state(sym)
            if hasattr(context, "ledger")
            else {}
        )
    )
    last_buy_epoch = max(
        safe_float(state_obj.get("last_buy_fill_ts"), 0.0),
        safe_float(state_obj.get("entry_ts"), 0.0),
    )
    if last_buy_epoch > 0.0:
        elapsed_epoch = time.time() - last_buy_epoch
        if 0.0 <= elapsed_epoch < POST_BUY_TO_SELL_SHIELD_SEC:
            return True, round(POST_BUY_TO_SELL_SHIELD_SEC - elapsed_epoch, 1)

    return False, 0.0


def mark_post_sell_time_shield(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    fill_price: float = 0.0,
    sold_qty: int = 0,
) -> None:
    """
    🚨 Case 65 [매도 체결 후 60.0초 강제 타임쉴드 각인]:
    매도가 체결된 즉시 인메모리 캐시(exit_snapshot_cache 및 indexing_grace_cache)에
    last_sell_fill_mono 와 last_sell_fill_ts 를 기록하여 60.0초 동안 재매수를 차단한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()
    now_epoch = time.time()

    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        entry["last_sell_fill_mono"] = now_mono
        entry["last_sell_fill_ts"] = now_epoch
        entry["last_sell_price"] = safe_float(fill_price, 0.0)
        entry["last_sold_qty"] = safe_int(sold_qty, 0)
        snap_map[sym] = entry

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        g_entry = (
            guard_map.get(sym) if isinstance(guard_map.get(sym), dict) else {}
        )
        g_entry["last_sell_fill_mono"] = now_mono
        g_entry["last_sell_fill_ts"] = now_epoch
        guard_map[sym] = g_entry


def is_within_post_sell_time_shield(
    context: Any,
    st: Optional[Dict[str, Any]] = None,
    symbol: str = TARGET_SYMBOL,
) -> Tuple[bool, float]:
    """
    🚨 Case 65 [매도 체결 후 60.0초 강제 타임쉴드 판별]:
    매도가 체결된 시점으로부터 60.0초가 경과하기 전에는 True 와 남은 초(remain_sec)를 반환한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym)
        if isinstance(entry, dict):
            last_sell_mono = safe_float(entry.get("last_sell_fill_mono"), 0.0)
            if last_sell_mono > 0.0:
                elapsed_mono = time.monotonic() - last_sell_mono
                if 0.0 <= elapsed_mono < POST_SELL_TO_BUY_SHIELD_SEC:
                    return True, round(
                        POST_SELL_TO_BUY_SHIELD_SEC - elapsed_mono, 1
                    )

    state_obj = (
        st
        if isinstance(st, dict)
        else (
            context.ledger.get_state(sym)
            if hasattr(context, "ledger")
            else {}
        )
    )
    last_sell_epoch = safe_float(state_obj.get("last_sell_fill_ts"), 0.0)
    if last_sell_epoch > 0.0:
        elapsed_epoch = time.time() - last_sell_epoch
        if 0.0 <= elapsed_epoch < POST_SELL_TO_BUY_SHIELD_SEC:
            return True, round(POST_SELL_TO_BUY_SHIELD_SEC - elapsed_epoch, 1)

    return False, 0.0


def update_exit_snapshot_on_hold(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    Case 19: 보유 중 매 틱마다 exit_snapshot_cache["SOXL"] 에 실보유 수량과 평단가,
    현재가를 백업하되 기존의 last_sell_fill_mono / last_sell_fill_ts 타임쉴드 시각을 보존한다.
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

    updated = dict(prev)
    updated.update(
        {
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
    )
    if isinstance(snap_map, dict):
        snap_map[sym] = updated
    return updated


# ---------------------------------------------------------------------------
# [핵심 1] Case 24 & Case 65 : 순수 현재가 지정가(LIMIT) 스나이퍼 매도 발주 (1호가 하향정정 금지!)
# ---------------------------------------------------------------------------
async def execute_bulletproof_sell_sweep(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    holdings_qty: int = 1,
    sweep_price: float = 0.0,
    min_floor_price: float = 0.0,
    reason_tag: str = "SELL_L1_ONLY",
) -> Dict[str, Any]:
    """
    🚨 Case 24 & Case 65:
    스나이퍼 매도(1층 단독 익절 SELL_L1_ONLY / 1·2층 동시 졸업 SELL_ALL_GRAD / 2층 단독 졸업 SELL_L2_GRAD) 발사 시:
    - 오직 목표 익절 지정가(min_floor_price) 이상인 '현재가 지정가(sweep_price)'로만 LIMIT 매도 주문을 발주한다!
    - 절대로 목표가 미만의 매수 1호가(Bid 1)로 가격을 낮추어 발주하거나 하향 정정하지 않는다!
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    target_sell_qty = max(1, safe_int(holdings_qty, 1))
    floor_p = max(
        0.01,
        math.ceil((safe_float(min_floor_price, 0.0) - 1e-9) * 100.0) / 100.0,
    )
    raw_sp = max(
        0.01,
        math.floor((safe_float(sweep_price, 0.0) + 1e-9) * 100.0) / 100.0,
    )
    # 매도 지정가는 어떠한 경우에도 목표 익절 단가(floor_p) 미만으로 내려갈 수 없음!
    sp = max(floor_p, raw_sp)

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

        # 이미 진행 중인 매도 주문이 있으면 중복 발주하지 않고 기존 지정가 주문 체결을 대기
        if sell_oid:
            return {
                "status": "SELL_ORDER_ALREADY_ACTIVE",
                "symbol": sym,
                "orderId": sell_oid,
                "sweep_price": safe_float(st.get("target_sell_price"), sp),
                "qty": target_sell_qty,
                "reason": reason_tag,
            }

        latest_open = await context.api.find_latest_open_order_id(
            sym, side="SELL"
        )
        if latest_open:
            try:
                await context.api.cancel_order(latest_open)
            except Exception:
                pass

        # sellableQuantity 최대 2.0초 폴링 확인 후 목표 매도 수량(target_sell_qty)만큼 순수 현재가 지정가(LIMIT) 발사
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
                "🔥 [%s] 현재가 지정가 스나이퍼 매도 발사(%s) 완료: %d주 @ 지정가 $%.2f (목표하한 $%.2f · orderId=%s)",
                sym,
                reason_tag,
                final_qty,
                sp,
                floor_p,
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
# [핵심 2] 실물 매도 FILLED 정산 · 세후 순익 복리 산입 · 2층 리앵커링 · 매도 후 60초 타임쉴드 각인
# ---------------------------------------------------------------------------
async def _commit_filled_sell_and_reanchor_ledger(
    context: Any,
    filled_qty: int,
    avg_fill_price: float,
    sell_mode: str,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & 🎯 4 & Case 19, 47, 59, 60, 65:
    스나이퍼 실물 매도 주문이 체결(FILLED)되었을 때:
    1) 매도된 지층의 실제 매수 원금(sold_actual_cost_usd)과 실제 매도 체결가(avg_fill_price)로
       왕복 수수료(0.14%) 차감 후 세후 실물 실현 순수익금(net_pnl_usd)을 산출한다.
    2) 1층만 팔리고 2층(단일 지층)이 남은 경우(len(rem_buckets) == 1 and net_pnl_usd > 0),
       vrev_reanchor_single_tier_quota() 를 호출하여 net_pnl_usd * 0.994 만큼 2층 장부 단가를 강제 인하한다!
    3) 🚨 [Case 65 매도 후 60.0초 타임쉴드 각인]:
       mark_post_sell_time_shield() 및 원장 last_sell_fill_ts = time.time() 을 각인하여,
       매도 체결 직후 60.0초 동안은 일체의 재매수를 차단하고 60초 경과 후 현재가를 조회해 매수하도록 강제한다!
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    buckets = buckets_from_dicts(st.get("buckets", []))
    fill_p = round(max(0.01, safe_float(avg_fill_price, 0.01)), 4)
    q_filled = max(1, safe_int(filled_qty, 1))
    mode = str(
        sell_mode or st.get("pending_sell_mode") or "SELL_L1_ONLY"
    ).upper()

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
        total_b_qty = sum(b.qty for b in buckets)
        if q_filled >= total_b_qty:
            sold_buckets = list(buckets)
            rem_buckets = []
        else:
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
                    book_invested_usd=max(
                        0.01 * rem_q, b0.book_invested_usd - sold_book_part
                    ),
                    actual_cost_usd=max(
                        0.01 * rem_q, b0.actual_cost_usd - sold_cost_part
                    ),
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
                book_invested_usd=max(
                    0.01 * rem_q, b1.book_invested_usd - sold_book_part
                ),
                actual_cost_usd=max(
                    0.01 * rem_q, b1.actual_cost_usd - sold_cost_part
                ),
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

    # ♻️ [Case 59 단일 지층 마법 : 1층 실물 익절 후 2층 단독 잔존 시 실물 세후 순익으로 2층 장부단가 즉시 리앵커링!]
    if len(rem_buckets) == 1 and net_pnl > 0.0:
        old_l2_book_p = rem_buckets[0].book_price
        rem_buckets, reanchor_drop_amt = vrev_reanchor_single_tier_quota(
            queue=rem_buckets,
            net_realized_cash_profit=net_pnl,
        )
        new_l2_book_p = rem_buckets[0].book_price
        if reanchor_drop_amt > 0.0:
            reanchor_inc = 1

    # 🚨 Case 60: 2층이 남아 있으면 기억된 최저 매수가(cycle_min_buy_price)를 보존하고,
    # 전량 졸업(len(rem_buckets) == 0) 시에만 0.0 으로 리셋한다!
    old_cycle_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    next_cycle_min_p = (
        round(old_cycle_min_p, 4) if len(rem_buckets) > 0 else 0.0
    )

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

    # 🚨 Case 65: 매도 체결 직후 60.0초 매수 금지 타임쉴드 각인!
    now_sell_epoch = time.time()
    mark_post_sell_time_shield(
        context=context,
        symbol=TARGET_SYMBOL,
        fill_price=fill_p,
        sold_qty=sold_q,
    )

    updated_st = await context.ledger.update_state(
        TARGET_SYMBOL,
        buckets=buckets_to_dicts(rem_buckets),
        anchor_price=fill_p,
        cycle_min_buy_price=next_cycle_min_p,
        ratchet_anchor_price=fill_p,
        last_sell_fill_ts=now_sell_epoch,
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
            note_str = "🎓 전량 졸업 완료(0주 ➔ 🛡️60초 타임쉴드 후 현재가 조회 및 1층 재산입!)"
        elif reanchor_drop_amt > 0.0:
            note_str = (
                f"♻️ 2층({rem_buckets[0].qty}주) 실물수익 리앵커링: "
                f"${old_l2_book_p:.2f}➔${new_l2_book_p:.2f}(-${reanchor_drop_amt:.2f}) "
                f"· 🛡️60초 타임쉴드 & 10분게이트 대기(기억저가 ${next_cycle_min_p:.2f} 잠금)"
            )
        else:
            note_str = f"잔여 {sum(b.qty for b in rem_buckets)}주 보유 (🛡️60초 타임쉴드 가동)"

        await context.send_slim_alert(
            title=f"🔴 [SOXL 현재가 지정가 익절 체결] {sold_q}주 @ ${fill_p:.2f} ({r_sign}{net_rate:.2f}%)",
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
    🚨 Case 19, Case 24 & Case 65:
    장부에 대기 중인 미체결 매도 주문(sell_order_id)이 존재할 때:
    1) GET /api/v1/orders/{orderId} 로 FILLED 여부를 조회하여 체결 완료 시
       즉시 _commit_filled_sell_and_reanchor_ledger() 로 복리 정산 및 60초 타임쉴드 각인을 수행한다.
    2) [핵심 수술] 3초 후 매수 1호가(Bid 1)로 주문 단가를 낮추어 강제 손절시키던 추격 정정 로직을 100% 영구 폐기!
       현재가 지정가로 발주된 매도 주문이 60.0초(SELL_ORDER_MAX_WAIT_SEC) 동안 체결되지 않으면,
       가격을 낮추지 않고 미체결 주문을 안전하게 취소(cancel_order)한 뒤 지층 보유 감시 상태로 복귀한다.
    """
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

    # 🚨 Case 65: 매수 1호가(Bid 1) 하향 정정 추격 완전 소각!
    # 60.0초 이상 미체결 시 안전하게 취소하고 다음 목표가 도달을 재감시한다.
    last_mod = safe_float(st.get("last_modify_mono"), 0.0)
    if last_mod > 0.0 and (time.monotonic() - last_mod) >= SELL_ORDER_MAX_WAIT_SEC:
        try:
            await context.api.cancel_order(sell_oid)
        except Exception:
            pass

        # 취소 직전 혹시 체결되었는지 최종 확인
        final_detail = await context.api.get_order_detail(sell_oid)
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
                return await _commit_filled_sell_and_reanchor_ledger(
                    context=context,
                    filled_qty=(f_qty if f_qty > 0 else ordered_qty),
                    avg_fill_price=(f_avg if f_avg > 0.0 else target_p),
                    sell_mode=sell_mode,
                )

        await context.ledger.update_state(
            TARGET_SYMBOL,
            sell_order_id="",
            ordered_qty=0,
            target_sell_price=0.0,
            pending_sell_mode="",
        )
        logger.info(
            "⏳ [SOXL 지정가 매도 60초 미체결 안전 취소] 지정가 $%.2f 유지 종료 ➔ 지층 보유 감시 복귀",
            target_p,
        )
        return {
            "status": "SELL_ORDER_UNFILLED_CANCELED_SAFE",
            "did_sell_or_ratchet": False,
        }

    return {
        "status": "SELL_ORDER_PENDING",
        "did_sell_or_ratchet": True,
        "orderId": sell_oid,
    }


# ---------------------------------------------------------------------------
# [핵심 3] 04:07 ~ 16:00 EST 매수 후 60초 타임쉴드 & 현재가 지정가 스나이퍼 매도 게이트
# ---------------------------------------------------------------------------
async def execute_1m_sniper_sell_and_reanchor_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 [Step 2] & 🎯 3 & 🎯 4 & Case 24, 44, 46 ~ 65:
    04:07:00 ~ 16:00:00 EST 전 구간에서 작동하는 순수 현재가 지정가 스나이퍼 매도(오팔) 엔진:
    1) 대기 중인 미체결 매도 주문(sell_order_id)이 있으면 먼저 체결 확인을 수행한다.
    2) 🛡️ [Case 65 매수 후 60.0초 강제 타임쉴드 검문]:
       직전 매수 체결로부터 60.0초(POST_BUY_TO_SELL_SHIELD_SEC)가 지나지 않았으면
       일체의 매도 판정을 보류(HOLD_POST_BUY_60S_TIME_SHIELD)하여 휩소를 원천 차단한다!
    3) 🔍 [60.0초 경과 후 실시간 현재가 재조회 & 현재가 >= 목표가일 때만 현재가 지정가 매도]:
       60초 타임쉴드 경과 후 현재가가 1층(+1.50%) 또는 2층(+1.00%) 목표가 이상으로 감지되면,
       즉시 _fetch_fresh_current_soxl_price() 로 실시간 현재가를 다시 조회하여
       재조회된 현재가(fresh_live_p)가 목표가(min_target_p) 이상일 때만
       매수 1호가(Bid 1) 참조 없이 오직 '현재가(fresh_live_p)를 지정가(LIMIT)'로 매도 주문을 발주한다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute

    st = context.ledger.get_state(TARGET_SYMBOL)

    # 1) 미체결 매도 주문이 있으면 먼저 체결 확인 수행
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

    # 🛡️ 2) [Case 65] 매수 체결 후 60.0초 타임쉴드 강제 검문!
    # 매수가 체결된 후 60초가 지나기 전에는 절대로 매도 판정을 수행하지 않는다!
    in_buy_shield, remain_buy_sec = is_within_post_buy_time_shield(
        context, st, TARGET_SYMBOL
    )
    if in_buy_shield:
        return {
            "status": "HOLD_POST_BUY_60S_TIME_SHIELD",
            "did_sell_or_ratchet": False,
            "remain_shield_sec": remain_buy_sec,
        }

    live_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    if live_p <= 0.0:
        return {"status": "NO_LIVE_PRICE", "did_sell_or_ratchet": False}

    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    h_qty = safe_int(h_info.get("qty"), sum(b.qty for b in buckets))
    avg_p = safe_float(h_info.get("avg_price"), buckets[0].actual_avg_price)
    update_exit_snapshot_on_hold(context, TARGET_SYMBOL, h_qty, avg_p, live_p)

    held_lots = sum(b.lot_count for b in buckets)
    threshold_mode = str(st.get("threshold_mode") or "08_VREV_PROG").upper()
    _, rise_sell_l1_pct, rise_sell_l2_pct = get_golden8_prog_thresholds(
        held_lots=held_lots, threshold_mode=threshold_mode
    )

    # -----------------------------------------------------------------------
    # [분기 A : 2층(MERGED_UPPER) 단독 보유 상태 — 2층 졸업(+1.00%) 감시]
    # -----------------------------------------------------------------------
    if len(buckets) == 1 and str(buckets[0].lot_type).upper() == "MERGED_UPPER":
        upper_b = buckets[0]
        min_l2_target_p = (
            math.ceil(
                (
                    upper_b.book_price * (1.0 + rise_sell_l2_pct / 100.0)
                    - 1e-9
                )
                * 100.0
            )
            / 100.0
        )

        if live_p >= min_l2_target_p:
            # 60초 타임쉴드 경과 후 실시간 현재가를 다시 조회하여 목표가 이상인지 재검증!
            fresh_live_p = await _fetch_fresh_current_soxl_price(
                context, fallback_price=live_p
            )
            if fresh_live_p >= min_l2_target_p:
                # 매수 1호가(bid1_p) 참조 없이 오직 '현재가(fresh_live_p)'를 지정가로 매도 발주!
                order_limit_p = max(
                    min_l2_target_p,
                    math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0,
                )
                sweep_res = await execute_bulletproof_sell_sweep(
                    context=context,
                    symbol=TARGET_SYMBOL,
                    holdings_qty=upper_b.qty,
                    sweep_price=order_limit_p,
                    min_floor_price=min_l2_target_p,
                    reason_tag="SELL_L2_GRAD",
                )
                return {
                    "status": "L2_GRADUATION_SELL_ORDERED",
                    "did_sell_or_ratchet": True,
                    "qty": upper_b.qty,
                    "sweep_price": order_limit_p,
                    "result": sweep_res,
                }

        upper_gain_pct = (
            ((live_p - upper_b.book_price) / upper_b.book_price) * 100.0
            if upper_b.book_price > 0.0
            else 0.0
        )
        return {
            "status": "AWAITING_REAL_L1_REENTRY",
            "did_sell_or_ratchet": False,
            "l2_gain_pct": round(upper_gain_pct, 2),
        }

    # -----------------------------------------------------------------------
    # [분기 B : 실물 1층(buckets[0]) 보유 상태 — 1층(+1.50%) 익절 및 2층(+1.00%) 동시 졸업 판정]
    # -----------------------------------------------------------------------
    floor1_b = buckets[0]
    min_l1_target_p = (
        math.ceil(
            (floor1_b.book_price * (1.0 + rise_sell_l1_pct / 100.0) - 1e-9)
            * 100.0
        )
        / 100.0
    )

    # B-1) 2층이 존재할 때 리앵커링된 2층 목표가(+1.00%)에 도달했고 1층도 본전 이상이면 전량 동시 졸업!
    if len(buckets) >= 2 and live_p >= floor1_b.book_price:
        min_l2_target_p = max(
            math.ceil(
                (b_up.book_price * (1.0 + rise_sell_l2_pct / 100.0) - 1e-9)
                * 100.0
            )
            / 100.0
            for b_up in buckets[1:]
        )
        req_grad_p = max(round(floor1_b.book_price, 2), min_l2_target_p)
        if live_p >= req_grad_p:
            fresh_live_p = await _fetch_fresh_current_soxl_price(
                context, fallback_price=live_p
            )
            if fresh_live_p >= req_grad_p:
                sell_qty = sum(b.qty for b in buckets)
                order_limit_p = max(
                    req_grad_p,
                    math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0,
                )
                sweep_res = await execute_bulletproof_sell_sweep(
                    context=context,
                    symbol=TARGET_SYMBOL,
                    holdings_qty=sell_qty,
                    sweep_price=order_limit_p,
                    min_floor_price=req_grad_p,
                    reason_tag="SELL_ALL_GRAD",
                )
                return {
                    "status": "SNIPER_1M_ALL_GRAD_SELL_ORDERED",
                    "did_sell_or_ratchet": True,
                    "sell_mode": "SELL_ALL_GRAD",
                    "qty": sell_qty,
                    "sweep_price": order_limit_p,
                    "result": sweep_res,
                }

    # B-2) 1층 목표 지정가(min_l1_target_p, +1.50%) 이상 도달 시 실시간 현재가 재조회 후 현재가 지정가 매도!
    if live_p >= min_l1_target_p:
        fresh_live_p = await _fetch_fresh_current_soxl_price(
            context, fallback_price=live_p
        )
        if fresh_live_p >= min_l1_target_p:
            sell_all_grad = True
            for b_up in buckets[1:]:
                up_target_p = (
                    math.ceil(
                        (
                            b_up.book_price * (1.0 + rise_sell_l2_pct / 100.0)
                            - 1e-9
                        )
                        * 100.0
                    )
                    / 100.0
                )
                if fresh_live_p < up_target_p:
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

            # 매수 1호가(bid1_p) 참조 없이 오직 '재조회된 현재가(fresh_live_p)'를 지정가로 매도 발주!
            order_limit_p = max(
                min_l1_target_p,
                math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0,
            )
            chg_from_l1_pct = (
                ((fresh_live_p - floor1_b.book_price) / floor1_b.book_price)
                * 100.0
                if floor1_b.book_price > 0.0
                else 0.0
            )

            sweep_res = await execute_bulletproof_sell_sweep(
                context=context,
                symbol=TARGET_SYMBOL,
                holdings_qty=sell_qty,
                sweep_price=order_limit_p,
                min_floor_price=min_l1_target_p,
                reason_tag=reason_tag,
            )
            return {
                "status": "SNIPER_1M_SELL_ORDERED",
                "did_sell_or_ratchet": True,
                "sell_mode": reason_tag,
                "qty": sell_qty,
                "sweep_price": order_limit_p,
                "chg_from_l1_pct": round(chg_from_l1_pct, 2),
                "result": sweep_res,
            }

    chg_now_pct = (
        ((live_p - floor1_b.book_price) / floor1_b.book_price) * 100.0
        if floor1_b.book_price > 0.0
        else 0.0
    )
    return {
        "status": "MONITORING_1M_SNIPER_SELL",
        "did_sell_or_ratchet": False,
        "chg_from_l1_pct": round(chg_now_pct, 2),
        "target_l1_pct": rise_sell_l1_pct,
    }


# 하위 호환 별칭 (타 모듈에서 호출 시 안전 직결)
manage_mechanical_profit_trap_plugin = execute_1m_sniper_sell_and_reanchor_plugin
reconcile_zero_holdings_exit_plugin = sync_pending_sell_order_plugin
