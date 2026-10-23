# -*- coding: utf-8 -*-
"""
# MODIFIED: sniper_exit_plugin.py
순수 롱(SOXL) 전용 '04:07 ~ 16:00 EST 매수 체결 후 60초 강제 타임쉴드(POST_BUY_TO_SELL_SHIELD_SEC = 60.0) &
보유 중 매 60초 타임쉴드 주기 3중 다중 익절 매도 모니터(SELL_MONITOR_TIME_SHIELD_SEC = 60.0 · Case 70, Case 71) ·
60초 타임쉴드 경과 시점에만 실시간 현재가 직접 조회 및 3중 다중 익절 조건(① 1층 +1.50% 이상 / ② 2~12층 개별 그리드 +1.00% 이상 /
③ 2~12층 통합 이동평균 +1.00% 이상) 동시 스캔 후 그 높아진 현재가 지정가(LIMIT) 매도 발주(목표 미달 시 즉시 다음 60초 매도 타임쉴드 자동 재장전) ·
매수 1호가(Bid 1) 참조 및 하향 추격 정정 100% 영구 소각(Case 65) ·
리앵커링 원가 차감 영구 폐기 및 순수 실물 원가(book_price == actual_avg_price) 100% 보존(Case 69, Case 71) ·
1층 익절 후 상단 2~12층 잔존 시 1층 공석(GRID_L2 ~ GRID_L12) 보존 및 싸이클 최저가(cycle_min_buy_price) 잠금 유지(Case 59, Case 60, Case 71) ·
매도 체결(부분 익절 및 0주 전량 졸업) 즉시 60초 매도타임쉴드(last_sell_fill_ts / last_cycle_end_ts) &
15초 잔고 인덱싱 그레이스 쉴드(last_auto_fill_ts) 동시 각인' 코어 플러그인 (Step 03 / 07)
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 71 완벽 준수
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from quant_engine import (
    REANCHOR_NET_BUFFER_MULT,
    StratumBucket,
    buckets_from_dicts,
    buckets_to_dicts,
    calc_sell_realized_pnl,
    compute_upper_2_to_12_moving_avg,
    evaluate_multi_grid_sell_targets,
    get_golden8_prog_thresholds,
    has_active_l1_bucket,
    relabel_upper_grid_tiers,
    vrev_reanchor_single_tier_quota,
)
from quant_indicators_plugin import (
    is_active_trading_window,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    EST_TZ,
    MAX_GRID_TIERS_12,
    TARGET_SYMBOL,
    _NonRetryableApiError,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_exit_plugin")

# Case 65 & Case 70: 매수 후 60.0초 타임쉴드 · 보유 중 매 60.0초 주기 매도 모니터 타임쉴드 · 매도(0주 종료) 후 60.0초 타임쉴드 상수
POST_BUY_TO_SELL_SHIELD_SEC: float = 60.0
SELL_MONITOR_TIME_SHIELD_SEC: float = 60.0
POST_SELL_TO_BUY_SHIELD_SEC: float = 60.0

# 순수 현재가 지정가 매도 주문 발주 후 미체결 최대 대기 시간(초) — 하향 정정 없이 60초 미체결 시 안전 취소
SELL_ORDER_MAX_WAIT_SEC: float = 60.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 실시간 SOXL 현재가 조회 · 60초 양방향/주기 타임쉴드 판별 · Case 69/71 순수 원가 하한선
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
    🚨 Case 65 & Case 70:
    60초 타임쉴드(매수 후 60초 또는 보유 중 60초 매도 모니터 주기) 경과 시점에
    토스 Open API(/api/v1/prices)를 직접 호출하여 가장 최신의 실시간 현재가를 조회한다.
    """
    try:
        fresh_map = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
        fresh_p = safe_float((fresh_map or {}).get(TARGET_SYMBOL), 0.0)
        if fresh_p > 0.0:
            if isinstance(getattr(context, "latest_prices", None), dict):
                context.latest_prices[TARGET_SYMBOL] = fresh_p
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


def _resolve_safe_l2_book_price(
    bucket: StratumBucket,
    st: Optional[Dict[str, Any]] = None,
) -> float:
    """
    🚨 MODIFIED [Case 69 & Case 71 진성 손익분기점(순수 매수 원가) 하한선 가드]:
    신규 챔피언(🔥 03_12G_PURE_10_01)에서는 인위적 리앵커링 차감을 수행하지 않으므로,
    모든 그리드 지층의 기준 단가는 언제나 순수 실물 매수 평단가(actual_avg_price) 이상으로 강제된다.
    """
    if bucket.qty <= 0:
        return max(0.01, bucket.book_price)
    return round(max(0.01, bucket.actual_avg_price, bucket.book_price), 4)


def is_within_post_buy_time_shield(
    context: Any,
    st: Optional[Dict[str, Any]] = None,
    symbol: str = TARGET_SYMBOL,
) -> Tuple[bool, float]:
    """
    🚨 Case 65 [매수 체결 직후 최초 60.0초 강제 타임쉴드]:
    매수가 체결된 시점(last_buy_fill_mono 단조 시계 및 원장 last_buy_fill_ts / entry_ts)으로부터
    60.0초가 경과하기 전에는 True 와 남은 초(remain_sec)를 반환하여 일체의 매도(및 중복 매수) 시도를 차단한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        info = guard_map.get(sym)
        if isinstance(info, dict):
            last_buy_mono = safe_float(info.get("last_buy_fill_mono"), 0.0)
            if (
                last_buy_mono <= 0.0
                and str(info.get("lot_type") or "") != "SELL_FILL"
            ):
                last_buy_mono = safe_float(info.get("last_auto_fill_ts"), 0.0)
            if last_buy_mono > 0.0:
                elapsed_mono = now_mono - last_buy_mono
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


def mark_sell_monitor_60s_cycle_shield(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    eval_price: float = 0.0,
    clear_shield: bool = False,
) -> float:
    """
    🚨 Case 70 [보유 중 매 60.0초 타임쉴드 주기 매도 모니터 재장전기]:
    - 60.0초 매도 모니터 검문 시 현재가가 3중 다중 익절 조건(1층 +1.50% / 2~12층 개별 +1.00% / 2~12층 이동평균 +1.00%)에
      모두 미달하거나, 발주된 매도 주문이 60.0초 미체결로 안전 취소된 직후, 또는 부분 익절 후 상단 지층이 남았을 때 호출되어
      인메모리 캐시(exit_snapshot_cache 및 indexing_grace_cache)에 last_sell_gate_eval_mono 와
      last_sell_gate_eval_ts 를 기록하고 현재 Epoch 시각을 반환한다.
    - 0주 전량 졸업(clear_shield=True) 시에는 0.0 으로 즉시 초기화한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = 0.0 if clear_shield else time.monotonic()
    now_epoch = 0.0 if clear_shield else time.time()

    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        entry["last_sell_gate_eval_mono"] = now_mono
        entry["last_sell_gate_eval_ts"] = now_epoch
        if eval_price > 0.0:
            entry["last_sell_gate_eval_price"] = round(
                safe_float(eval_price, 0.0), 4
            )
        snap_map[sym] = entry

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        g_entry = (
            guard_map.get(sym) if isinstance(guard_map.get(sym), dict) else {}
        )
        g_entry["last_sell_gate_eval_mono"] = now_mono
        g_entry["last_sell_gate_eval_ts"] = now_epoch
        guard_map[sym] = g_entry

    return now_epoch


def is_within_sell_monitor_time_shield(
    context: Any,
    st: Optional[Dict[str, Any]] = None,
    symbol: str = TARGET_SYMBOL,
) -> Tuple[bool, float]:
    """
    🚨 Case 70 [보유 중 매 60.0초 타임쉴드 주기 매도 모니터 작동 여부 판별]:
    1) 먼저 매수 체결 직후 최초 60.0초 타임쉴드(is_within_post_buy_time_shield)를 확인한다.
    2) 이후 보유 중 직전 60.0초 매도 모니터 검문 시각(last_sell_gate_eval_mono / last_sell_gate_eval_ts)으로부터
       60.0초(SELL_MONITOR_TIME_SHIELD_SEC)가 경과하지 않았으면 True 와 잔여 초(remain_sec)를 반환하여,
       1.5초 / 0.4초 주기의 상시 턱걸이 매도 감시를 원천 차단한다!
    """
    in_post_buy, rem_buy = is_within_post_buy_time_shield(context, st, symbol)
    if in_post_buy:
        return True, rem_buy

    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()

    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym)
        if isinstance(entry, dict):
            eval_mono = safe_float(entry.get("last_sell_gate_eval_mono"), 0.0)
            if eval_mono > 0.0:
                elapsed_mono = now_mono - eval_mono
                if 0.0 <= elapsed_mono < SELL_MONITOR_TIME_SHIELD_SEC:
                    return True, round(
                        SELL_MONITOR_TIME_SHIELD_SEC - elapsed_mono, 1
                    )

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        g_entry = guard_map.get(sym)
        if isinstance(g_entry, dict):
            g_eval_mono = safe_float(
                g_entry.get("last_sell_gate_eval_mono"), 0.0
            )
            if g_eval_mono > 0.0:
                elapsed_g = now_mono - g_eval_mono
                if 0.0 <= elapsed_g < SELL_MONITOR_TIME_SHIELD_SEC:
                    return True, round(
                        SELL_MONITOR_TIME_SHIELD_SEC - elapsed_g, 1
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
    eval_epoch = safe_float(state_obj.get("last_sell_gate_eval_ts"), 0.0)
    if eval_epoch > 0.0:
        elapsed_epoch = time.time() - eval_epoch
        if 0.0 <= elapsed_epoch < SELL_MONITOR_TIME_SHIELD_SEC:
            return True, round(SELL_MONITOR_TIME_SHIELD_SEC - elapsed_epoch, 1)

    return False, 0.0


def mark_post_sell_time_shield(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    fill_price: float = 0.0,
    sold_qty: int = 0,
    is_cycle_end: bool = False,
) -> None:
    """
    🚨 Case 61, Case 65, Case 69 & Case 70 [매도 체결 및 0주 싸이클 종료 후 60.0초 타임쉴드 & 15.0초 잔고 인덱싱 그레이스 쉴드 동시 각인]:
    1) 매도가 체결된 즉시(또는 0주 싸이클이 종료된 즉시) 인메모리 캐시(exit_snapshot_cache 및 indexing_grace_cache)에
       last_sell_fill_mono 와 last_sell_fill_ts 를 기록하고, 0주 졸업(is_cycle_end=True) 시에는
       last_cycle_end_mono 와 last_cycle_end_ts 도 함께 기록하여 60.0초 동안 다음 첫 진입 및 재매수를 원천 차단한다.
    2) 부분 익절 후 상단 그리드가 남아 있는 경우(is_cycle_end=False)에는 다음 매도 모니터도 60.0초 타임쉴드 뒤에 작동하도록
       last_sell_gate_eval_mono / last_sell_gate_eval_ts 를 동시 재장전하고, 0주 졸업(is_cycle_end=True) 시에는 0.0 으로 리셋한다.
    3) 동시에 indexing_grace_cache[sym]["last_auto_fill_ts"] = now_mono 를 각인하여,
       매도 체결 직후 토스 잔고 API(/api/v1/holdings) 인덱싱 시차(0.1 ~ 0.4초) 동안
       reconcile 모듈이 유령 추매/부분매도나 과거 백업 원장 복원을 수행하지 못하도록 15.0초 그레이스 쉴드를 작동시킨다!
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()
    now_epoch = time.time()

    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        entry["last_sell_fill_mono"] = now_mono
        entry["last_sell_fill_ts"] = now_epoch
        if is_cycle_end:
            entry["last_cycle_end_mono"] = now_mono
            entry["last_cycle_end_ts"] = now_epoch
            entry["last_sell_gate_eval_mono"] = 0.0
            entry["last_sell_gate_eval_ts"] = 0.0
        else:
            entry["last_sell_gate_eval_mono"] = now_mono
            entry["last_sell_gate_eval_ts"] = now_epoch
        entry["last_sell_price"] = safe_float(fill_price, 0.0)
        entry["last_sold_qty"] = safe_int(sold_qty, 0)
        snap_map[sym] = entry

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        g_entry = (
            guard_map.get(sym) if isinstance(guard_map.get(sym), dict) else {}
        )
        prev_lot_type = str(g_entry.get("lot_type") or "")
        if (
            prev_lot_type != "SELL_FILL"
            and safe_float(g_entry.get("last_auto_fill_ts"), 0.0) > 0.0
        ):
            g_entry["last_buy_fill_mono"] = safe_float(
                g_entry.get("last_auto_fill_ts"), 0.0
            )
        g_entry["last_auto_fill_ts"] = now_mono
        g_entry["last_sell_fill_mono"] = now_mono
        g_entry["last_sell_fill_ts"] = now_epoch
        if is_cycle_end:
            g_entry["last_cycle_end_mono"] = now_mono
            g_entry["last_cycle_end_ts"] = now_epoch
            g_entry["last_sell_gate_eval_mono"] = 0.0
            g_entry["last_sell_gate_eval_ts"] = 0.0
        else:
            g_entry["last_sell_gate_eval_mono"] = now_mono
            g_entry["last_sell_gate_eval_ts"] = now_epoch
        g_entry["lot_type"] = "SELL_FILL"
        g_entry["fill_price"] = safe_float(fill_price, 0.0)
        g_entry["filled_qty"] = safe_int(sold_qty, 0)
        guard_map[sym] = g_entry


def is_within_post_sell_time_shield(
    context: Any,
    st: Optional[Dict[str, Any]] = None,
    symbol: str = TARGET_SYMBOL,
) -> Tuple[bool, float]:
    """
    🚨 Case 65 [매도 체결 및 0주 싸이클 종료 후 60.0초 강제 타임쉴드 판별]:
    직전 매도 체결 시각(last_sell_fill_mono / last_sell_fill_ts) 또는
    0주 싸이클 종료 시각(last_cycle_end_mono / last_cycle_end_ts)으로부터
    60.0초(POST_SELL_TO_BUY_SHIELD_SEC)가 경과하기 전에는 True 와 남은 초(remain_sec)를 반환한다.
    """
    sym = str(symbol or TARGET_SYMBOL).upper().strip()
    now_mono = time.monotonic()

    snap_map = getattr(context, "exit_snapshot_cache", None)
    if isinstance(snap_map, dict):
        entry = snap_map.get(sym)
        if isinstance(entry, dict):
            last_sell_mono = max(
                safe_float(entry.get("last_sell_fill_mono"), 0.0),
                safe_float(entry.get("last_cycle_end_mono"), 0.0),
            )
            if last_sell_mono > 0.0:
                elapsed_mono = now_mono - last_sell_mono
                if 0.0 <= elapsed_mono < POST_SELL_TO_BUY_SHIELD_SEC:
                    return True, round(
                        POST_SELL_TO_BUY_SHIELD_SEC - elapsed_mono, 1
                    )

    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        g_entry = guard_map.get(sym)
        if isinstance(g_entry, dict):
            g_sell_mono = max(
                safe_float(g_entry.get("last_sell_fill_mono"), 0.0),
                safe_float(g_entry.get("last_cycle_end_mono"), 0.0),
            )
            if g_sell_mono > 0.0:
                elapsed_g = now_mono - g_sell_mono
                if 0.0 <= elapsed_g < POST_SELL_TO_BUY_SHIELD_SEC:
                    return True, round(
                        POST_SELL_TO_BUY_SHIELD_SEC - elapsed_g, 1
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
    last_sell_epoch = max(
        safe_float(state_obj.get("last_sell_fill_ts"), 0.0),
        safe_float(state_obj.get("last_cycle_end_ts"), 0.0),
    )
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
    Case 19 & Case 70: 보유 중 exit_snapshot_cache["SOXL"] 에 실보유 수량과 평단가,
    현재가를 백업하되 기존의 타임쉴드 시각을 안전하게 보존한다.
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
# [핵심 1] Case 24, Case 65, Case 70 & Case 71 : 순수 현재가 지정가(LIMIT) 다중 그리드 스나이퍼 매도 발주
# ---------------------------------------------------------------------------
async def execute_bulletproof_sell_sweep(
    context: Any,
    symbol: str = TARGET_SYMBOL,
    holdings_qty: int = 1,
    sweep_price: float = 0.0,
    min_floor_price: float = 0.0,
    reason_tag: str = "SELL_L1_ONLY",
    pending_sell_indices: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    """
    🚨 Case 24, Case 65, Case 70 & Case 71:
    60초 타임쉴드 주기 3중 다중 익절 스나이퍼 매도 발사 시:
    - 오직 목표 익절 지정가(min_floor_price) 이상인 '그 시점의 실시간 현재가 지정가(sweep_price)'로만 LIMIT 매도 주문을 발주한다!
    - 매도 대상 그리드 인덱스 리스트(pending_sell_indices)를 원장에 기록하여 체결 시 정확히 해당 층들만 정산한다.
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
    sp = max(floor_p, raw_sp)
    clean_indices = sorted(
        {safe_int(x, -1) for x in (pending_sell_indices or []) if safe_int(x, -1) >= 0}
    )

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
            eval_epoch = mark_sell_monitor_60s_cycle_shield(
                context=context, symbol=sym, eval_price=sp
            )
            await context.ledger.update_state(
                sym,
                sell_order_id=new_oid,
                client_order_id=cid,
                ordered_qty=final_qty,
                target_sell_price=sp,
                pending_sell_mode=str(reason_tag or "SELL_L1_ONLY"),
                pending_sell_indices=clean_indices,
                last_modify_mono=time.monotonic(),
                last_sell_gate_eval_ts=eval_epoch,
            )
            context.wakeup_event.set()
            logger.info(
                "🔥 [%s] 60초 타임쉴드 3중 다중익절 스나이퍼 매도 발사(%s · indices=%s) 완료: %d주 @ 지정가 $%.2f (목표하한 $%.2f · orderId=%s)",
                sym,
                reason_tag,
                clean_indices,
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
                "sold_indices": clean_indices,
            }
        except Exception as exc:
            eval_epoch = mark_sell_monitor_60s_cycle_shield(
                context=context, symbol=sym, eval_price=sp
            )
            await context.ledger.update_state(
                sym, last_sell_gate_eval_ts=eval_epoch
            )
            logger.error(
                "🚨 [%s] 스나이퍼 매도 발사(%s) 실패 (60초 타임쉴드 재장전): %s",
                sym,
                reason_tag,
                exc,
            )
            return {"status": "SWEEP_FAILED", "symbol": sym, "error": str(exc)}


# ---------------------------------------------------------------------------
# [핵심 2] 실물 매도 FILLED 정산 · 세후 순익 복리 산입 · 1~12층 순수 원가 보존 · 양방향 쉴드 각인 (Case 69, 70, 71)
# ---------------------------------------------------------------------------
async def _commit_filled_sell_and_reanchor_ledger(
    context: Any,
    filled_qty: int,
    avg_fill_price: float,
    sell_mode: str,
) -> Dict[str, Any]:
    """
    🎯 1 ~ 🎯 4 & Case 19, 59, 60, 61, 65, 69, 70, 71:
    12층 멀티 그리드 스나이퍼 매도 주문이 체결(FILLED)되었을 때:
    1) pending_sell_indices (또는 sell_mode)에 해당하는 매도 지층들의 순수 매수 원금(sold_actual_cost_usd)과
       실제 매도 체결가(avg_fill_price)로 왕복 수수료(0.14%) 차감 후 세후 실현 순수익금(net_pnl_usd)을 산출한다.
    2) 🚨 [Case 71 리앵커링 원가 차감 영구 폐기 및 1층 공석 보존]:
       - 남은 지층들의 원가를 인위적으로 차감하지 않고 순수 매수 원가(book_price == actual_avg_price)를 100% 보존한다.
       - 1층이 팔리고 상단 그리드(2층 ~ 12층)만 남은 경우, 상단 2층을 1층으로 승격시키지 않고
         relabel_upper_grid_tiers() 를 통해 GRID_L2, GRID_L3, ... (1층 공석 상태)로 보존하여
         반등 고점 재매수로 인한 현금 고갈을 원천 차단하고, 싸이클 최저가(cycle_min_buy_price) 잠금을 유지한다!
       - 만약 1층은 팔리지 않고 상단 개별 그리드만 팔린 경우, buckets[0] 은 활성 1층으로 유지하고
         나머지 상단 지층들만 GRID_L2, GRID_L3, ... 로 정렬한다.
    3) 🚨 [Case 61, Case 65, Case 69 & Case 70 매도/0주 싸이클 종료 후 60.0초 타임쉴드 & 15.0초 그레이스 쉴드 동시 각인]
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    buckets = buckets_from_dicts(st.get("buckets", []))
    fill_p = round(max(0.01, safe_float(avg_fill_price, 0.01)), 4)
    q_filled = max(1, safe_int(filled_qty, 1))
    mode = str(
        sell_mode or st.get("pending_sell_mode") or "SELL_L1_ONLY"
    ).upper()

    if not buckets:
        now_empty_epoch = time.time()
        mark_post_sell_time_shield(
            context=context,
            symbol=TARGET_SYMBOL,
            fill_price=fill_p,
            sold_qty=q_filled,
            is_cycle_end=True,
        )
        await context.ledger.update_state(
            TARGET_SYMBOL,
            last_sell_fill_ts=now_empty_epoch,
            last_cycle_end_ts=now_empty_epoch,
            last_sell_gate_eval_ts=0.0,
            sell_order_id="",
            ordered_qty=0,
            target_sell_price=0.0,
            pending_sell_mode="",
            pending_sell_indices=[],
        )
        return {"status": "NO_BUCKETS_TO_SETTLE"}

    was_active_l1 = has_active_l1_bucket(buckets)
    raw_p_indices = st.get("pending_sell_indices")
    target_indices: List[int] = []
    if isinstance(raw_p_indices, list) and raw_p_indices:
        target_indices = sorted(
            {
                safe_int(idx, -1)
                for idx in raw_p_indices
                if 0 <= safe_int(idx, -1) < len(buckets)
            }
        )

    if not target_indices:
        if mode in ("SELL_ALL_GRAD", "SELL_L2_GRAD") or len(buckets) == 1:
            target_indices = list(range(len(buckets)))
        elif mode == "SELL_WMA_UPPER" and len(buckets) >= 2:
            start_i = 1 if was_active_l1 else 0
            target_indices = list(range(start_i, len(buckets)))
        else:
            target_indices = [0]

    target_set: Set[int] = set(target_indices)
    sold_l1_flag = bool(was_active_l1 and (0 in target_set))

    sold_buckets: List[StratumBucket] = []
    rem_buckets: List[StratumBucket] = []
    rem_fill_to_alloc = q_filled

    for idx, b in enumerate(buckets):
        if idx not in target_set or rem_fill_to_alloc <= 0:
            rem_buckets.append(b)
            continue

        if rem_fill_to_alloc >= b.qty:
            sold_buckets.append(b)
            rem_fill_to_alloc -= b.qty
        else:
            # 부분 체결된 경우 비율만큼만 분할 정산하고 잔여 수량은 원가 그대로 보존
            part_q = rem_fill_to_alloc
            rem_fill_to_alloc = 0
            ratio = min(1.0, float(part_q) / float(b.qty))
            sold_cost_part = round(b.actual_cost_usd * ratio, 4)
            rem_q = b.qty - part_q
            rem_cost_part = round(
                max(0.01 * float(rem_q), b.actual_cost_usd - sold_cost_part), 4
            )
            sold_buckets.append(
                StratumBucket(
                    qty=part_q,
                    book_price=b.book_price,
                    book_invested_usd=sold_cost_part,
                    actual_cost_usd=sold_cost_part,
                    lot_count=1,
                    lot_type=b.lot_type,
                )
            )
            rem_buckets.append(
                StratumBucket(
                    qty=rem_q,
                    book_price=b.book_price,
                    book_invested_usd=rem_cost_part,
                    actual_cost_usd=rem_cost_part,
                    lot_count=max(1, b.lot_count),
                    lot_type=b.lot_type,
                )
            )
            if idx == 0 and was_active_l1:
                sold_l1_flag = False

    # 🚨 Case 71: 남은 그리드 지층 라벨 정규화 (1층이 팔렸으면 남은 층들은 GRID_L2 ~ GRID_L12 로 1층 공석 유지!)
    if rem_buckets:
        if was_active_l1 and not sold_l1_flag:
            active_b1 = rem_buckets[0]
            re_uppers = relabel_upper_grid_tiers(
                rem_buckets[1:], max_tiers=MAX_GRID_TIERS_12
            )
            rem_buckets = [active_b1] + re_uppers
        else:
            rem_buckets = relabel_upper_grid_tiers(
                rem_buckets, max_tiers=MAX_GRID_TIERS_12
            )

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

    is_graduated_zero = bool(len(rem_buckets) == 0)

    # 🚨 Case 60: 상단 지층이 남아 있으면 기억된 최저 매수가(cycle_min_buy_price)를 보존하고,
    # 전량 졸업(len(rem_buckets) == 0) 시에만 0.0 으로 리셋한다!
    old_cycle_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    next_cycle_min_p = (
        round(old_cycle_min_p, 4) if not is_graduated_zero else 0.0
    )

    tot_sell_ev = safe_int(st.get("total_sell_events"), 0) + 1
    tot_sold_sh = safe_int(st.get("total_sold_shares"), 0) + sold_q
    win_ev = safe_int(st.get("win_sell_events"), 0) + (
        1 if net_pnl > 0.0 else 0
    )
    day_sell_ev = safe_int(st.get("daily_sell_events"), 0) + 1

    indiv_inc = (
        1 if mode in ("SELL_GRID_INDIV", "SELL_L1_AND_GRID") else 0
    )
    wma_inc = (
        1
        if (
            mode == "SELL_WMA_UPPER"
            or (mode == "SELL_ALL_GRAD" and len(buckets) > 1)
        )
        else 0
    )
    indiv_cnt = safe_int(st.get("indiv_grid_sell_cnt"), 0) + indiv_inc
    day_indiv_cnt = (
        safe_int(st.get("daily_indiv_grid_sell_cnt"), 0) + indiv_inc
    )
    wma_cnt = safe_int(st.get("wma_grad_sell_cnt"), 0) + wma_inc
    day_wma_cnt = safe_int(st.get("daily_wma_grad_sell_cnt"), 0) + wma_inc

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

    now_sell_epoch = time.time()
    mark_post_sell_time_shield(
        context=context,
        symbol=TARGET_SYMBOL,
        fill_price=fill_p,
        sold_qty=sold_q,
        is_cycle_end=is_graduated_zero,
    )

    update_kwargs: Dict[str, Any] = {
        "buckets": buckets_to_dicts(rem_buckets),
        "anchor_price": fill_p,
        "cycle_min_buy_price": next_cycle_min_p,
        "ratchet_anchor_price": fill_p,
        "cycle_reanchor_credit_usd": 0.0,
        "last_sell_fill_ts": now_sell_epoch,
        "last_sell_gate_eval_ts": (
            0.0 if is_graduated_zero else now_sell_epoch
        ),
        "sell_order_id": "",
        "ordered_qty": 0,
        "target_sell_price": 0.0,
        "pending_sell_mode": "",
        "pending_sell_indices": [],
        "total_sell_events": tot_sell_ev,
        "total_sold_shares": tot_sold_sh,
        "win_sell_events": win_ev,
        "daily_sell_events": day_sell_ev,
        "indiv_grid_sell_cnt": indiv_cnt,
        "daily_indiv_grid_sell_cnt": day_indiv_cnt,
        "wma_grad_sell_cnt": wma_cnt,
        "daily_wma_grad_sell_cnt": day_wma_cnt,
        "cumulative_realized_gross_usd": cum_gross,
        "cumulative_realized_fee_usd": cum_fee,
        "cumulative_realized_net_usd": cum_net,
        "daily_realized_net_usd": day_net,
    }
    if is_graduated_zero:
        update_kwargs["last_cycle_end_ts"] = now_sell_epoch

    updated_st = await context.ledger.update_state(
        TARGET_SYMBOL, **update_kwargs
    )

    context.wakeup_event.set()
    free_cash_now = safe_float(updated_st.get("free_cash_usd"), 0.0)

    if hasattr(context, "send_slim_alert"):
        sign = "+" if net_pnl >= 0.0 else "-"
        r_sign = "+" if net_rate >= 0.0 else ""
        if is_graduated_zero:
            note_str = "🎓 전 지층 흑자 졸업 완료(0주 싸이클 종료 ➔ 🛡️60초 타임쉴드 경과 후 현재가 조회 및 첫 1층 진입!)"
        else:
            wma_info = compute_upper_2_to_12_moving_avg(rem_buckets)
            u_cnt = safe_int(wma_info.get("upper_tier_count"), len(rem_buckets))
            u_qty = safe_int(
                wma_info.get("upper_qty"), sum(b.qty for b in rem_buckets)
            )
            u_wma_p = safe_float(wma_info.get("upper_wma_price"), 0.0)
            u_wma_t = safe_float(wma_info.get("upper_wma_target_price"), 0.0)
            note_str = (
                f"📦 잔여 2~{u_cnt + 1}층({u_cnt}개층·{u_qty}주) 순수이평 ${u_wma_p:.2f}(목표 ${u_wma_t:.2f}) 보존 "
                f"· 🛡️60초 타임쉴드 & 10분게이트 대기(기억저가 ${next_cycle_min_p:.2f} 잠금)"
            )

        await context.send_slim_alert(
            title=f"🔴 [SOXL 현재가 지정가 익절 체결 ({mode})] {sold_q}주 @ ${fill_p:.2f} ({r_sign}{net_rate:.2f}%)",
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
        "reanchor_drop_usd": 0.0,
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
    🚨 Case 19, Case 24, Case 65, Case 70 & Case 71:
    장부에 대기 중인 미체결 매도 주문(sell_order_id)이 존재할 때:
    1) GET /api/v1/orders/{orderId} 로 FILLED 여부를 조회하여 체결 완료 시
       즉시 _commit_filled_sell_and_reanchor_ledger() 로 복리 정산 및 60초 타임쉴드/15초 그레이스 쉴드 각인을 수행한다.
    2) 현재가 지정가로 발주된 매도 주문이 60.0초(SELL_ORDER_MAX_WAIT_SEC) 동안 체결되지 않으면,
       가격을 낮추지 않고 미체결 주문을 안전하게 취소(cancel_order)한 뒤
       mark_sell_monitor_60s_cycle_shield() 로 다음 60.0초 매도 모니터 타임쉴드를 재장전하고 복귀한다.
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
            eval_epoch = mark_sell_monitor_60s_cycle_shield(
                context=context, symbol=TARGET_SYMBOL, eval_price=target_p
            )
            await context.ledger.update_state(
                TARGET_SYMBOL,
                sell_order_id="",
                ordered_qty=0,
                target_sell_price=0.0,
                pending_sell_mode="",
                pending_sell_indices=[],
                last_sell_gate_eval_ts=eval_epoch,
            )
            return {
                "status": f"SELL_ORDER_{ord_status}_CLEARED",
                "did_sell_or_ratchet": False,
            }

    last_mod = safe_float(st.get("last_modify_mono"), 0.0)
    if (
        last_mod > 0.0
        and (time.monotonic() - last_mod) >= SELL_ORDER_MAX_WAIT_SEC
    ):
        try:
            await context.api.cancel_order(sell_oid)
        except Exception:
            pass

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

        eval_epoch = mark_sell_monitor_60s_cycle_shield(
            context=context, symbol=TARGET_SYMBOL, eval_price=target_p
        )
        await context.ledger.update_state(
            TARGET_SYMBOL,
            sell_order_id="",
            ordered_qty=0,
            target_sell_price=0.0,
            pending_sell_mode="",
            pending_sell_indices=[],
            last_sell_gate_eval_ts=eval_epoch,
        )
        logger.info(
            "⏳ [SOXL 지정가 매도 60초 미체결 안전 취소] 지정가 $%.2f 유지 종료 ➔ 🛡️60초 매도 모니터 타임쉴드 재장전",
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
# [핵심 3] 04:07 ~ 16:00 EST 매수 후 60초 & 보유 중 매 60초 타임쉴드 주기 3중 다중 익절 매도 모니터 (Case 69, 70, 71)
# ---------------------------------------------------------------------------
async def execute_1m_sniper_sell_and_reanchor_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 [Step 2] & 🎯 3 & 🎯 4 & Case 24, 44, 46 ~ 71:
    04:07:00 ~ 16:00:00 EST 전 구간에서 매 60.0초 타임쉴드 주기로 작동하는 3중 다중 익절 스나이퍼 매도(오팔) 모니터:
    1) 대기 중인 미체결 매도 주문(sell_order_id)이 있으면 먼저 체결 확인을 수행한다.
    2) 🛡️ [Case 65 매수 후 최초 60.0초 강제 타임쉴드 검문]:
       직전 매수 체결로부터 60.0초(POST_BUY_TO_SELL_SHIELD_SEC)가 지나지 않았으면
       일체의 매도 판정을 보류(HOLD_POST_BUY_60S_TIME_SHIELD)하여 휩소를 원천 차단한다!
    3) 🛡️ [Case 70 보유 중 매 60.0초 주기 매도 모니터 타임쉴드 검문]:
       직전 매도 모니터 검문 시각(last_sell_gate_eval_mono / last_sell_gate_eval_ts)으로부터
       60.0초(SELL_MONITOR_TIME_SHIELD_SEC)가 지나지 않았으면 일체의 매도 판정을 보류(HOLD_SELL_MONITOR_60S_TIME_SHIELD)하여
       1.5초 / 0.4초 상시 감시로 인한 +1.50% / +1.00% 턱걸이 조기 매도를 100% 원천 차단한다!
    4) 🔍 [Case 71 : 60.0초 타임쉴드 해제 시점 실시간 현재가 직접 조회 & 3중 다중 익절 동시 판정]:
       60.0초 타임쉴드가 경과하여 매도 모니터가 깨어난 순간 _fetch_fresh_current_soxl_price() 로 실시간 현재가를 새로 조회한 뒤,
       evaluate_multi_grid_sell_targets() 를 호출하여:
       - ① 활성 1층(buckets[0]) 단가 대비 +1.50% 이상 도래
       - ② 상단 2층 ~ 12층 개별 그리드 단가 대비 +1.00% 이상 도래
       - ③ 상단 2층 ~ 12층 통합 이동평균 단가(wma_2_to_12) 대비 +1.00% 이상 도래 (1층 본전 이상 시 전량 졸업)
       조건이 충족된 모든 지층(sold_indices)을 그 높아진 실시간 현재가 지정가(LIMIT)로 즉시 매도 발주한다!
       - 만약 3대 조건 모두 미달이면 즉시 mark_sell_monitor_60s_cycle_shield() 로 다음 60.0초 매도 타임쉴드를 재장전한다!
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

    if str(st.get("buy_order_id") or "").strip():
        return {
            "status": "BUY_ORDER_IN_PROGRESS",
            "did_sell_or_ratchet": False,
        }

    buckets = buckets_from_dicts(st.get("buckets", []))
    if not buckets:
        return {"status": "NO_BUCKETS_HELD", "did_sell_or_ratchet": False}

    # 🛡️ 2) [Case 65] 매수 체결 직후 최초 60.0초 타임쉴드 강제 검문!
    in_buy_shield, remain_buy_sec = is_within_post_buy_time_shield(
        context, st, TARGET_SYMBOL
    )
    if in_buy_shield:
        return {
            "status": "HOLD_POST_BUY_60S_TIME_SHIELD",
            "did_sell_or_ratchet": False,
            "remain_shield_sec": remain_buy_sec,
        }

    # 🛡️ 3) [Case 70] 보유 중 매 60.0초 주기 매도 모니터 타임쉴드 강제 검문!
    in_mon_shield, remain_mon_sec = is_within_sell_monitor_time_shield(
        context, st, TARGET_SYMBOL
    )
    if in_mon_shield:
        return {
            "status": "HOLD_SELL_MONITOR_60S_TIME_SHIELD",
            "did_sell_or_ratchet": False,
            "remain_shield_sec": remain_mon_sec,
        }

    # 🔍 4) [Case 65, Case 70 & Case 71] 60.0초 타임쉴드가 해제된 시점 -> 최신 실시간 현재가 직접 조회!
    fallback_p = _get_live_soxl_price(context, prices, holdings, dt_now)
    fresh_live_p = await _fetch_fresh_current_soxl_price(
        context, fallback_price=fallback_p
    )
    if fresh_live_p <= 0.0:
        return {"status": "NO_LIVE_PRICE", "did_sell_or_ratchet": False}

    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    h_qty = safe_int(h_info.get("qty"), sum(b.qty for b in buckets))
    avg_p = safe_float(h_info.get("avg_price"), buckets[0].actual_avg_price)
    update_exit_snapshot_on_hold(
        context, TARGET_SYMBOL, h_qty, avg_p, fresh_live_p
    )

    held_lots = sum(b.lot_count for b in buckets)
    threshold_mode = str(st.get("threshold_mode") or "WIDE_RUNWAY").upper()
    _, rise_sell_l1_pct, rise_sell_upper_pct = get_golden8_prog_thresholds(
        held_lots=held_lots, threshold_mode=threshold_mode
    )

    # 🎯 5) [Case 71] 1층(+1.50%) · 2~12층 개별(+1.00%) · 2~12층 통합 이동평균(+1.00%) 3중 다중 익절 판정!
    sell_eval = evaluate_multi_grid_sell_targets(
        buckets=buckets,
        live_price=fresh_live_p,
        rise_l1_pct=rise_sell_l1_pct,
        rise_upper_pct=rise_sell_upper_pct,
    )

    if bool(sell_eval.get("should_sell")):
        sold_indices = list(sell_eval.get("sold_indices") or [])
        sell_qty = max(1, safe_int(sell_eval.get("sell_qty"), 1))
        min_floor_p = max(
            0.01, safe_float(sell_eval.get("min_floor_price"), fresh_live_p)
        )
        reason_tag = str(sell_eval.get("reason_tag") or "SELL_L1_ONLY")
        order_limit_p = max(
            min_floor_p,
            math.floor((fresh_live_p + 1e-9) * 100.0) / 100.0,
        )

        sweep_res = await execute_bulletproof_sell_sweep(
            context=context,
            symbol=TARGET_SYMBOL,
            holdings_qty=sell_qty,
            sweep_price=order_limit_p,
            min_floor_price=min_floor_p,
            reason_tag=reason_tag,
            pending_sell_indices=sold_indices,
        )
        return {
            "status": "SNIPER_MULTI_GRID_SELL_ORDERED",
            "did_sell_or_ratchet": True,
            "sell_mode": reason_tag,
            "sold_indices": sold_indices,
            "qty": sell_qty,
            "sweep_price": order_limit_p,
            "result": sweep_res,
        }

    # 🚨 6) [Case 70] 60초 매도 모니터 검문 결과 3대 조건 모두 미달 -> 즉시 다음 60.0초 매도 모니터 타임쉴드 재장전!
    eval_epoch = mark_sell_monitor_60s_cycle_shield(
        context=context, symbol=TARGET_SYMBOL, eval_price=fresh_live_p
    )
    await context.ledger.update_state(
        TARGET_SYMBOL, last_sell_gate_eval_ts=eval_epoch
    )
    b0_p = buckets[0].book_price
    chg_now_pct = (
        ((fresh_live_p - b0_p) / b0_p) * 100.0 if b0_p > 0.0 else 0.0
    )
    return {
        "status": "MONITORING_60S_SNIPER_SELL_SHIELD_RELOADED",
        "did_sell_or_ratchet": False,
        "chg_from_first_tier_pct": round(chg_now_pct, 2),
        "target_l1_pct": rise_sell_l1_pct,
        "next_sell_check_sec": SELL_MONITOR_TIME_SHIELD_SEC,
    }


# 하위 호환 별칭 (타 모듈에서 호출 시 안전 직결)
manage_mechanical_profit_trap_plugin = execute_1m_sniper_sell_and_reanchor_plugin
reconcile_zero_holdings_exit_plugin = sync_pending_sell_order_plugin
