# -*- coding: utf-8 -*-
"""
# MODIFIED: sniper_exit_plugin.py
실전형 '+1.00% 하드보존 하이킨 아시 트레일링(🔥 HARD_TRAIL_100 : 🕯️HA양봉 홀딩 & 🌑HA음봉 + 고점 -0.20% 콜백 스윕
× 🚀 NQ_VEL_BYPASS)' & 상단 안전 캡(+5.00% LIMIT) 선장전 & 프리장 후반(07:15 ~ 08:14 EST) 미익절 동적 VWAP 선제 탈출
(🛟 PRE_VWAP_ESC) & 2단계 방탄 매도 스윕(Case 24) & 통합 실제 발생 순수익금(Realized Net PnL) 복리/단리 정산 플러그인 (Step 01 / 02)
- MODIFIED: defense_plugin.py, daymarket_sim_plugin.py, whipsaw_plugin.py, main.py 와 연동되는
  모든 핵심 함수(ensure_safety_cap_sell_order, execute_bulletproof_sell_sweep, manage_mechanical_profit_trap_plugin,
  reconcile_zero_holdings_exit_plugin, update_exit_snapshot_on_hold, unlock_all_symbols_for_reentry 등) 및
  상수를 100% 누락 없이 익스포트하여 ImportError 원천 소각
- MODIFIED: 제3헌법 및 최신 프롬프트.txt에 의거하여 RE_BOT_TRAIL 및 bottom_track_cache 잔재를 100% 영구 소각하고,
  04:07 ~ 07:45 EST 이전 익절(TP_WIN) / 안전탈출(PRE_VWAP_ESC) 완료 시 다회전(Multi-Cycle) 자동 해금 직결
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: 신규 매도 주문 발사 시 고유 clientOrderId 주입
- 🎯 4 & Case 19, 24, 49, 52, 58, 74, 75, 76, 99, 100, 102 완벽 준수
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from quant_engine import NQ_OVERHEAT_THRESHOLD
from quant_indicators_plugin import refresh_metrics_with_live_tick
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    _NonRetryableApiError,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_exit_plugin")

# ---------------------------------------------------------------------------
# 🎯 4 : 하드보존 하이킨 아시 트레일링 & 안전 캡 & 후반부 VWAP 탈출 & 수수료 상수
# ---------------------------------------------------------------------------
SAFETY_CAP_PROFIT_PCT: float = 5.00         # 본진/수동 매수 체결 즉시 선장전하는 상단 안전 캡 (+5.00% LIMIT)
WHIPSAW_TARGET_PROFIT_PCT: float = 1.00     # 03:58 ~ 04:07 1주 휩소 작전 전용 목표 익절률 (+1.00% LIMIT)
TRAIL_ARM_PROFIT_PCT: float = 1.00          # 하드보존 트레일링 기본 점화 수익률 (+1.00%)
TRAIL_RATCHET_LOCK_PCT: float = 1.30        # 최소 +1.00% 보존선 영구 하드 잠금 트리거 (+1.30%)
TRAIL_CUSHION_FLOOR_PCT: float = 0.75       # +1.30% 도달 전 초입(+1.00% ~ +1.29%) 비상 쿠션 바닥선 (+0.75%)
TRAIL_DEFAULT_CALLBACK_PCT: float = 0.20    # 경유 최고가 대비 기본 콜백 스탑폭 (-0.20%)
LATE_VWAP_ESC_MIN_HOLD_MIN: float = 25.0    # 후반부 동적 VWAP 선제 탈출 최소 보유 경과 시간 (25.0분)
LATE_VWAP_ESC_MIN_PROFIT_PCT: float = 0.20  # 후반부 탈출 시 최소 수수료 보전 수익률 (+0.20%)
ROUND_TRIP_FEE_RATE_PCT: float = 0.14       # 토스증권 해외주식 왕복 수수료율 (0.14% = 편도 0.07% * 2)

# 매수 FILLED 직후 잔고 API 인덱싱 시차(0.1 ~ 0.4초) 보호 그레이스 쉴드 시간 (Case 102)
INDEXING_GRACE_SHIELD_SEC: float = 15.0


# ---------------------------------------------------------------------------
# 시간대 판별 및 보유 스냅샷 / 복리 정산 순수 함수
# ---------------------------------------------------------------------------
def is_premarket_late_vwap_esc_window(hm: int) -> bool:
    """
    🎯 4: 07:15 ~ 08:14 EST 프리장 후반 미익절 표류 물량 동적 프리장 VWAP 선제 탈출(PRE_VWAP_ESC) 구간.
    """
    return bool(715 <= hm <= 814)


def is_premarket_multicycle_unlock_window(hm: int) -> bool:
    """
    🎯 3 [Step 2-3]: 04:07 ~ 07:45 EST 프리장 익절/탈출 완료 시 다회전(Multi-Cycle) 재출동 해금 구간.
    """
    return bool(407 <= hm <= 745)


def update_exit_snapshot_on_hold(
    context: Any,
    symbol: str,
    holdings_qty: int,
    avg_price: float,
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 4 [1단계] & Case 19:
    보유 중 매 틱마다 _EXIT_SNAPSHOT_CACHE[symbol] 에 실보유 수량(last_holdings_qty),
    팩트 평단가(last_avg_price), 경유 최고가(peak_price)를 상시 백업하여,
    잔고 0주 복귀 시 수량 증발이나 미체결 안전 캡 단가 오인을 원천 봉쇄한다.
    """
    sym = str(symbol or "").upper().strip()
    snap_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    prev = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}

    lp = safe_float(live_price, 0.0)
    ap = safe_float(avg_price, safe_float(prev.get("last_avg_price"), 0.0))
    qty = max(0, safe_int(holdings_qty, safe_int(prev.get("last_holdings_qty"), 0)))
    prev_peak = safe_float(prev.get("peak_price"), ap)
    cur_peak = max(prev_peak, lp, ap)

    updated = {
        "symbol": sym,
        "last_holdings_qty": qty,
        "last_avg_price": ap,
        "last_live_price": lp if lp > 0.0 else safe_float(prev.get("last_live_price"), ap),
        "peak_price": cur_peak,
        "last_sweep_price": safe_float(prev.get("last_sweep_price"), 0.0),
        "last_sweep_reason": str(prev.get("last_sweep_reason") or ""),
        "updated_ts": time.time(),
    }
    if isinstance(snap_map, dict):
        snap_map[sym] = updated
    return updated


def calc_compound_next_budget(
    current_budget: float,
    avg_buy_price: float,
    avg_sell_price: float,
    filled_qty: int,
    is_compound: bool = True,
    fee_rate_pct: float = ROUND_TRIP_FEE_RATE_PCT,
) -> Dict[str, float]:
    """
    🎯 4 & Case 76:
    실제 매수 평단가(avg_buy_price), 실제 매도 체결가(avg_sell_price), 실제 체결 수량(filled_qty)을
    기반으로 세전 손익금(gross_pnl_usd)에서 왕복 수수료(0.14%)를 차감한
    실제 발생 순수익금(net_delta_usd)과 차기 통합 시드(next_budget)를 산출한다.
    """
    cur_b = max(10.0, safe_float(current_budget, 1000.0))
    bp = safe_float(avg_buy_price, 0.0)
    sp = safe_float(avg_sell_price, 0.0)
    qty = max(0, safe_int(filled_qty, 0))

    if bp <= 0.0 or sp <= 0.0 or qty <= 0:
        return {
            "gross_pnl_usd": 0.0,
            "fee_usd": 0.0,
            "net_delta_usd": 0.0,
            "gross_rate_pct": 0.0,
            "net_rate_pct": 0.0,
            "next_budget": round(cur_b, 2),
        }

    gross_pnl = (sp - bp) * float(qty)
    total_turnover = (bp + sp) * float(qty)
    fee_usd = total_turnover * ((fee_rate_pct / 100.0) * 0.5)
    net_delta = gross_pnl - fee_usd

    gross_rate = ((sp - bp) / bp) * 100.0
    net_rate = gross_rate - fee_rate_pct

    next_b = max(10.0, cur_b + net_delta) if is_compound else cur_b
    return {
        "gross_pnl_usd": round(gross_pnl, 2),
        "fee_usd": round(fee_usd, 2),
        "net_delta_usd": round(net_delta, 2),
        "gross_rate_pct": round(gross_rate, 2),
        "net_rate_pct": round(net_rate, 2),
        "next_budget": round(next_b, 2),
    }


async def unlock_all_symbols_for_reentry(
    context: Any,
    macro: Optional[Dict[str, Any]] = None,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 & 🎯 3 & Case 52, 95, 96:
    04:07 EST 1주 휩소 작전 종료 후 본진 인계 또는 프리장 다회전(Multi-Cycle) 해금 시 호출된다.
    - 당일 NQ=F 진폭이 2.0% 미만이면 양쪽 장부의 is_session_done = False 로 해제하여 프리장 본진 진입을 허용한다.
    - 당일 NQ=F 진폭이 2.0% 이상(과열일)이면 즉시 양쪽 장부를 CANCELED_MACRO 및 is_session_done = True 로 잠근다.
    """
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    if nq_amp <= 0.0 and hasattr(context, "macro_cache"):
        try:
            snap = await context.macro_cache.get_macro_snapshot()
            nq_amp = safe_float(snap.get("nq_amp"), 0.0)
        except Exception:
            pass

    is_overheated = bool(nq_amp >= NQ_OVERHEAT_THRESHOLD)
    for sym in REAL_ALLOWED_SYMBOLS:
        if is_overheated:
            await context.ledger.update_state(
                sym,
                buy_order_id="",
                sell_order_id="",
                ordered_qty=0,
                buy_price=0.0,
                target_sell_price=0.0,
                entry_session="CANCELED_MACRO",
                is_session_done=True,
            )
        else:
            await context.ledger.update_state(
                sym,
                buy_order_id="",
                sell_order_id="",
                ordered_qty=0,
                buy_price=0.0,
                target_sell_price=0.0,
                entry_session="",
                entry_trend="",
                is_session_done=False,
            )

    return {
        "status": "LOCKED_MACRO" if is_overheated else "UNLOCKED_FOR_REENTRY",
        "nq_amp": nq_amp,
    }


# ---------------------------------------------------------------------------
# [핵심 1] 상단 안전 캡(+5.00% LIMIT / 1주 휩소 +1.00% LIMIT) 매도 주문 자동 선장전
# ---------------------------------------------------------------------------
async def ensure_safety_cap_sell_order(
    context: Any,
    symbol: str,
    holdings_qty: int,
    avg_price: float,
) -> Dict[str, Any]:
    """
    🎯 4 [1단계] & Case 49, 58:
    - SOXS 또는 SOXL 실보유 잔고(holdings_qty > 0) 확인 즉시 호출된다.
    - 만약 매수 주문(buy_order_id)이 장부에 남아 있었다면 체결 완료된 것이므로 깔끔히 비운다.
    - 거래소에 이미 OPEN 상태인 해당 종목 SELL 주문이 존재하면 해당 orderId 를 장부에 재결속한다.
    - 없을 경우 평단가 기준 +5.00% 상단 안전 캡(1주 휩소 WHIPSAW_1SH 는 +1.00% 익절가)에
      LIMIT 매도 주문을 선장전하여 sell_order_id 를 확보하고 target_sell_price 에 +1.00% 점화가를 기록한다.
    """
    sym = str(symbol or "").upper().strip()
    qty = safe_int(holdings_qty, 0)
    ap = safe_float(avg_price, 0.0)
    if sym not in REAL_ALLOWED_SYMBOLS or qty <= 0 or ap <= 0.0:
        return {"status": "INVALID_HOLDINGS"}

    st = context.ledger.get_state(sym)
    if not bool(st.get("is_active", True)):
        return {"status": "BOT_INACTIVE"}

    # 매수 체결이 완료되어 보유 잔고로 잡혔으므로 잔존 buy_order_id 정리
    if str(st.get("buy_order_id") or "").strip():
        await context.ledger.update_state(sym, buy_order_id="")

    cfg_rate = safe_float(st.get("target_profit_rate"), TRAIL_ARM_PROFIT_PCT)
    if cfg_rate <= 0.0:
        cfg_rate = TRAIL_ARM_PROFIT_PCT

    arm_price = math.ceil((ap * (1.0 + cfg_rate / 100.0)) * 100.0) / 100.0
    entry_sess = str(st.get("entry_session") or "MANUAL")
    cap_pct = (
        WHIPSAW_TARGET_PROFIT_PCT
        if entry_sess == "WHIPSAW_1SH"
        else SAFETY_CAP_PROFIT_PCT
    )
    safety_cap_price = math.ceil((ap * (1.0 + cap_pct / 100.0)) * 100.0) / 100.0

    existing_sell_oid = str(st.get("sell_order_id") or "").strip()
    if existing_sell_oid:
        if safe_float(st.get("target_sell_price"), 0.0) <= 0.0:
            await context.ledger.update_state(sym, target_sell_price=arm_price)
        return {
            "status": "ALREADY_HAS_SELL_ORDER",
            "orderId": existing_sell_oid,
            "arm_price": arm_price,
        }

    async with context.in_memory_ordering_lock:
        st_now = context.ledger.get_state(sym)
        if str(st_now.get("sell_order_id") or "").strip():
            return {
                "status": "ALREADY_LOCKED",
                "orderId": st_now.get("sell_order_id"),
            }

        # 거래소에 이미 걸려 있는 미체결 SELL 주문이 있는지 확인 (수동/재기동 시 중복 발사 방지)
        open_sell_oid = await context.api.find_latest_open_order_id(
            sym, side="SELL"
        )
        if open_sell_oid:
            await context.ledger.update_state(
                sym,
                sell_order_id=open_sell_oid,
                ordered_qty=qty,
                buy_price=ap,
                target_sell_price=arm_price,
            )
            logger.info(
                "🔗 [%s] 기존 거래소 OPEN 매도 주문 자동 재결속 완료 (orderId=%s, 점화가=$%.2f)",
                sym,
                open_sell_oid,
                arm_price,
            )
            return {
                "status": "REBOUND_EXISTING_OPEN_SELL",
                "orderId": open_sell_oid,
                "arm_price": arm_price,
            }

        sellable_qty = await context.api.get_sellable_quantity(sym)
        order_qty = min(qty, sellable_qty) if sellable_qty > 0 else qty
        if order_qty <= 0:
            return {"status": "NO_SELLABLE_QTY"}

        cid = generate_client_order_id(prefix=f"CAP{sym}")
        try:
            ord_res = await context.api.create_order(
                symbol=sym,
                side="SELL",
                quantity=order_qty,
                price=safety_cap_price,
                order_type="LIMIT",
                time_in_force="DAY",
                client_order_id=cid,
                enforce_daymarket_block=False,
            )
            new_sell_oid = str(ord_res.get("orderId") or "").strip()
            await context.ledger.update_state(
                sym,
                sell_order_id=new_sell_oid,
                ordered_qty=order_qty,
                buy_price=ap,
                target_sell_price=arm_price,
            )
            logger.info(
                "🛡️ [%s] 상단 안전 캡(+%.2f%%) 매도 주문 선장전 완료: %d주 @ $%.2f (점화가=$%.2f, orderId=%s)",
                sym,
                cap_pct,
                order_qty,
                safety_cap_price,
                arm_price,
                new_sell_oid,
            )
            return {
                "status": "SAFETY_CAP_PLACED",
                "symbol": sym,
                "orderId": new_sell_oid,
                "safety_cap_price": safety_cap_price,
                "arm_price": arm_price,
            }
        except Exception as exc:
            logger.warning(
                "⚠️ [%s] 상단 안전 캡 매도 선장전 보류: %s", sym, exc
            )
            return {"status": "ERROR", "error": str(exc)}


# ---------------------------------------------------------------------------
# [핵심 2] Case 24 : 2단계 방탄 매도 스윕 (1단계 modify_order 0.1s 정정 + 2단계 선취소 재발사)
# ---------------------------------------------------------------------------
async def execute_bulletproof_sell_sweep(
    context: Any,
    symbol: str,
    holdings_qty: int = 1,
    sweep_price: float = 0.0,
    reason_tag: str = "SWEEP",
) -> Dict[str, Any]:
    """
    🚨 Case 24:
    트레일링 익절 스윕, 후반부 VWAP 탈출 스윕, 2·3차 매크로 손절 스윕, 04:07 휩소 컷오프, 15:59 MOC 청산 시:
    - [1단계] 장부(또는 거래소 OPEN 목록)에 기존 매도 주문(sell_order_id)이 존재하면
      POST /api/v1/orders/{orderId}/modify 로 0.1초 만에 가격을 sweep_price 로 즉시 하향 정정한다!
      (409 already-modified 시 최신 OPEN 매도 주문 ID 자동 추적 정정, already-filled 시 즉시 성공 처리)
    - [2단계] 정정 실패 또는 기존 주문 부재 시, 미체결 매도 주문을 선취소하고
      GET /api/v1/sellable-quantity 를 최대 2.0초 폴링 대기한 뒤 신규 LIMIT 매도 주문을 발사한다.
    """
    sym = str(symbol or "").upper().strip()
    qty = max(1, safe_int(holdings_qty, 1))
    sp = max(0.01, math.floor(safe_float(sweep_price, 0.0) * 100.0) / 100.0)

    # 스냅샷 캐시에 실제 스윕 단가와 사유를 즉시 기록하여 정산 정확도 100% 보장 (Case 19)
    snap_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    if isinstance(snap_map, dict):
        snap_entry = snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        snap_entry["last_sweep_price"] = sp
        snap_entry["last_sweep_reason"] = str(reason_tag or "SWEEP")
        snap_entry["last_holdings_qty"] = max(
            qty, safe_int(snap_entry.get("last_holdings_qty"), qty)
        )
        snap_map[sym] = snap_entry

    async with context.in_memory_ordering_lock:
        st = context.ledger.get_state(sym)
        sell_oid = str(st.get("sell_order_id") or "").strip()
        if not sell_oid:
            sell_oid = await context.api.find_latest_open_order_id(
                sym, side="SELL"
            )

        # [1단계] 기존 매도 주문(sell_order_id) 즉시 가격 하향 정정 (US 주식은 quantity 생략 강제)
        if sell_oid:
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
                )
                context.wakeup_event.set()
                logger.info(
                    "⚡ [%s] 1단계 방탄 정정 스윕(%s) 성공: $%.2f (orderId=%s)",
                    sym,
                    reason_tag,
                    sp,
                    new_oid,
                )
                return {
                    "status": "MODIFIED_SWEEP_OK",
                    "symbol": sym,
                    "orderId": new_oid,
                    "sweep_price": sp,
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
                    }
                logger.warning(
                    "⚠️ [%s] 1단계 정정 스윕 거절(%s) -> 2단계 선취소 재발사 전환: %s",
                    sym,
                    exc.error_code,
                    exc.error_message,
                )
            except Exception as exc:
                logger.warning(
                    "⚠️ [%s] 1단계 정정 스윕 예외 -> 2단계 전환: %s", sym, exc
                )

            # 2단계 진입 전 기존 미체결 매도 주문 선취소
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

        # [2단계] sellableQuantity 최대 2.0초 폴링 대기 후 신규 LIMIT 매도 발사
        sellable_q = 0
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            sellable_q = await context.api.get_sellable_quantity(sym)
            if sellable_q > 0:
                break
            await asyncio.sleep(0.25)

        final_qty = sellable_q if sellable_q > 0 else qty
        cid = generate_client_order_id(prefix=f"SWP{sym}")
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
                target_sell_price=sp,
            )
            context.wakeup_event.set()
            logger.info(
                "🔥 [%s] 2단계 방탄 신규 매도 스윕(%s) 성공: %d주 @ $%.2f (orderId=%s)",
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
                "🚨 [%s] 2단계 방탄 매도 스윕(%s) 실패: %s",
                sym,
                reason_tag,
                exc,
            )
            return {"status": "SWEEP_FAILED", "symbol": sym, "error": str(exc)}


# ---------------------------------------------------------------------------
# [핵심 3] +1.00% 하드보존 하이킨 아시 트레일링(HARD_TRAIL_100) & 프리장 후반 VWAP 선제 탈출
# ---------------------------------------------------------------------------
async def manage_mechanical_profit_trap_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 4 [1단계 ~ 3단계] & Case 24, 49, 91, 99, 100:
    1) 실보유 잔고(holdings_qty > 0) 확인 시 _EXIT_SNAPSHOT_CACHE 상시 백업 및 상단 안전 캡(+5.00% LIMIT) 선장전 보장
    2) 현재가 >= arm_price(+1.00%) 도달 시 하드보존 하이킨 아시 트레일링(HARD_TRAIL_100) 점화:
       - 경유 최고가(peak_price)가 +1.30% 도달 전에는 +0.75% 비상 쿠션을 바닥으로 설정
       - 경유 최고가가 +1.30% 돌파 시 최소 +1.00%(arm_price) 보존선을 영구 하드 잠금
       - 🕯️HA양봉 유지 중 또는 실시간 QQQ 10m/3m ±0.20% 급등락(🚀 NQ_VEL_BYPASS) 중에는 콜백 유예 홀딩
       - 🌑HA음봉 전환 + 최고가 대비 -0.20% 콜백 도달(또는 보존선 하회) 시 0.1초 즉시 정정 스윕!
    3) 프리장 후반(07:15 ~ 08:14 EST) 진입 25분 경과 미익절 자동 포지션이
       동적 프리장 VWAP(최소 평단 +0.20%) 도달 시 즉시 PRE_VWAP_ESC 선제 탈출!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    raw_cache = getattr(context, "candle_metrics_cache", {})
    trail_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        raw_lp = safe_float(
            (prices or {}).get(sym), safe_float(h_info.get("last_price"), avg_p)
        )
        met = refresh_metrics_with_live_tick(base_met, raw_lp, now_est=dt_now)
        live_p = safe_float(met.get("live_price"), raw_lp)
        if live_p <= 0.0:
            continue

        # 매 틱 보유 스냅샷 상시 갱신 및 상단 안전 캡 매도 주문 선장전 확인
        update_exit_snapshot_on_hold(context, sym, h_qty, avg_p, live_p)
        await ensure_safety_cap_sell_order(context, sym, h_qty, avg_p)

        entry_sess = str(st.get("entry_session") or "MANUAL")
        # 1주 휩소 작전(WHIPSAW_1SH)은 이미 +1.00% 지정가 매도 주문이 장전되어 있으므로 거래소 체결 대기
        if entry_sess == "WHIPSAW_1SH":
            continue

        cfg_rate = safe_float(
            st.get("target_profit_rate"), TRAIL_ARM_PROFIT_PCT
        )
        if cfg_rate <= 0.0:
            cfg_rate = TRAIL_ARM_PROFIT_PCT
        arm_price = (
            math.ceil((avg_p * (1.0 + cfg_rate / 100.0)) * 100.0) / 100.0
        )

        t_state = (
            trail_map.get(sym) if isinstance(trail_map.get(sym), dict) else {}
        )
        if not t_state or safe_float(t_state.get("avg_price"), 0.0) != avg_p:
            t_state = {
                "symbol": sym,
                "armed": False,
                "ratchet_130_locked": False,
                "avg_price": avg_p,
                "arm_price": arm_price,
                "peak_price": max(avg_p, live_p),
                "floor_price": 0.0,
                "effective_stop_p": 0.0,
                "callback_pct": TRAIL_DEFAULT_CALLBACK_PCT,
                "sweep_sent": False,
            }
            trail_map[sym] = t_state

        # -------------------------------------------------------------------
        # [2단계] +1.00%(arm_price) 도달 시 하드보존 하이킨 아시 트레일링 점화
        # -------------------------------------------------------------------
        if live_p >= arm_price and not bool(t_state.get("armed")):
            t_state["armed"] = True
            t_state["peak_price"] = max(
                safe_float(t_state.get("peak_price"), live_p), live_p
            )
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🔥 [+1.00% 하드보존 HA 트레일링 점화] {sym} {h_qty}주",
                    body=(
                        f"현가 ${live_p:.2f} >= 점화가 ${arm_price:.2f} (평단 ${avg_p:.2f}) "
                        f"· 🕯️HA양봉 홀딩 & 🌑HA음봉 -0.20% 콜백 감시"
                    ),
                )

        # -------------------------------------------------------------------
        # [3단계] 트레일링 점화 상태 : 최소 보존선 하드 잠금 & HA음봉 + -0.20% 콜백 스윕
        # -------------------------------------------------------------------
        if bool(t_state.get("armed")):
            peak_p = max(safe_float(t_state.get("peak_price"), live_p), live_p)
            t_state["peak_price"] = peak_p

            if peak_p >= avg_p * (1.0 + TRAIL_RATCHET_LOCK_PCT / 100.0):
                t_state["ratchet_130_locked"] = True

            cushion_floor = (
                math.ceil(
                    (avg_p * (1.0 + TRAIL_CUSHION_FLOOR_PCT / 100.0)) * 100.0
                )
                / 100.0
            )
            floor_p = (
                arm_price
                if bool(t_state.get("ratchet_130_locked"))
                else cushion_floor
            )
            cb_pct = safe_float(
                t_state.get("callback_pct"), TRAIL_DEFAULT_CALLBACK_PCT
            )
            cb_stop_p = (
                math.floor((peak_p * (1.0 - cb_pct / 100.0)) * 100.0) / 100.0
            )
            eff_stop_p = max(floor_p, cb_stop_p)

            t_state["floor_price"] = floor_p
            t_state["effective_stop_p"] = eff_stop_p

            is_ha_bull = bool(met.get("is_ha_bull", False))
            nq_vel_bypass = bool(
                (macro or {}).get("bypass_soxs")
                if sym == "SOXS"
                else (macro or {}).get("bypass_soxl")
            )

            breached_hard_floor = bool(live_p < floor_p)
            triggered_ha_callback = bool(
                not is_ha_bull and not nq_vel_bypass and live_p <= eff_stop_p
            )

            if breached_hard_floor or triggered_ha_callback:
                bid1_p = await context.api.get_best_bid_price(
                    sym, fallback_price=live_p
                )
                raw_sweep_p = math.floor(min(live_p, bid1_p) * 100.0) / 100.0
                sweep_p = max(floor_p, raw_sweep_p)
                t_state["sweep_sent"] = True

                sweep_res = await execute_bulletproof_sell_sweep(
                    context=context,
                    symbol=sym,
                    holdings_qty=h_qty,
                    sweep_price=sweep_p,
                    reason_tag="TP_WIN_HARD_TRAIL",
                )
                return {
                    "status": "TRAIL_SWEEP_EXECUTED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "peak_price": peak_p,
                    "result": sweep_res,
                }
            continue

        # -------------------------------------------------------------------
        # [프리장 후반 07:15 ~ 08:14 EST] 미익절 표류 물량 동적 프리장 VWAP 선제 탈출 (PRE_VWAP_ESC)
        # -------------------------------------------------------------------
        if entry_sess != "MANUAL" and is_premarket_late_vwap_esc_window(hm):
            entry_ts = safe_float(st.get("entry_ts"), 0.0)
            hold_min = (
                (time.time() - entry_ts) / 60.0 if entry_ts > 0.0 else 30.0
            )
            pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
            if hold_min >= LATE_VWAP_ESC_MIN_HOLD_MIN and pre_vwap > 0.0:
                min_fee_p = (
                    math.ceil(
                        (avg_p * (1.0 + LATE_VWAP_ESC_MIN_PROFIT_PCT / 100.0))
                        * 100.0
                    )
                    / 100.0
                )
                esc_target_p = max(
                    math.floor(pre_vwap * 100.0) / 100.0, min_fee_p
                )
                if live_p >= esc_target_p:
                    bid1_p = await context.api.get_best_bid_price(
                        sym, fallback_price=live_p
                    )
                    sweep_p = max(
                        min_fee_p,
                        math.floor(min(live_p, bid1_p) * 100.0) / 100.0,
                    )
                    await context.ledger.update_state(
                        sym, entry_session="PRE_VWAP_ESC"
                    )
                    sweep_res = await execute_bulletproof_sell_sweep(
                        context=context,
                        symbol=sym,
                        holdings_qty=h_qty,
                        sweep_price=sweep_p,
                        reason_tag="PRE_VWAP_ESC",
                    )
                    return {
                        "status": "PRE_VWAP_ESC_EXECUTED",
                        "symbol": sym,
                        "sweep_price": sweep_p,
                        "pre_vwap": pre_vwap,
                        "result": sweep_res,
                    }

    return {"status": "PROFIT_TRAP_MONITORED"}


# ---------------------------------------------------------------------------
# [핵심 4] 잔고 0주 복귀 시 실발생 순수익금 통합 복리 정산 및 다회전(Multi-Cycle) 해금
# ---------------------------------------------------------------------------
async def _extract_verified_fill_price_and_qty(
    context: Any,
    symbol: str,
    sell_order_id: str,
    snap_info: Dict[str, Any],
    override_exit_price: float = 0.0,
) -> Tuple[float, int]:
    """
    🚨 Case 19:
    REPLACED / CANCELED 상태인 원주문의 미체결 상단 안전 캡 가격(+5.00%)을 체결가로 오인하는 것을
    100% 금지하고, 오직 FILLED 상태 주문의 execution.averageFilledPrice (또는 실제 스윕 단가)와
    실제 보유 수량만 추출한다.
    """
    snap_qty = max(1, safe_int(snap_info.get("last_holdings_qty"), 1))
    snap_sweep_p = safe_float(snap_info.get("last_sweep_price"), 0.0)
    snap_live_p = safe_float(snap_info.get("last_live_price"), 0.0)

    if override_exit_price > 0.0:
        return override_exit_price, snap_qty

    if sell_order_id:
        detail = await context.api.get_order_detail(sell_order_id)
        if isinstance(detail, dict):
            ord_status = str(detail.get("status") or "").upper().strip()
            exec_obj = (
                detail.get("execution")
                if isinstance(detail.get("execution"), dict)
                else {}
            )
            filled_q = safe_int(exec_obj.get("filledQuantity"), 0)
            avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
            if (
                ord_status in ("FILLED", "PARTIAL_FILLED")
                and filled_q > 0
                and avg_fill_p > 0.0
            ):
                return avg_fill_p, filled_q

    # 최근 CLOSED 된 SELL 주문 중 FILLED 레코드 탐색
    try:
        closed_orders = await context.api.get_orders(
            status="CLOSED", symbol=symbol, limit=10
        )
        for ord_item in closed_orders:
            if not isinstance(ord_item, dict):
                continue
            if str(ord_item.get("side") or "").upper() != "SELL":
                continue
            st_code = str(ord_item.get("status") or "").upper()
            exec_obj = (
                ord_item.get("execution")
                if isinstance(ord_item.get("execution"), dict)
                else {}
            )
            fq = safe_int(exec_obj.get("filledQuantity"), 0)
            afp = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
            if st_code == "FILLED" and fq > 0 and afp > 0.0:
                return afp, fq
    except Exception:
        pass

    if snap_sweep_p > 0.0:
        return snap_sweep_p, snap_qty
    if snap_live_p > 0.0:
        return snap_live_p, snap_qty
    return safe_float(snap_info.get("last_avg_price"), 0.0), snap_qty


async def reconcile_zero_holdings_exit_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    macro: Optional[Dict[str, Any]] = None,
    now_est: Optional[datetime] = None,
    override_exit_price: float = 0.0,
    override_exit_reason: str = "",
) -> Dict[str, Any]:
    """
    🎯 3 [Step 2-3] & 🎯 4 & Case 19, 50, 76, 99, 100, 102:
    - 실보유 잔고가 0주(holdings_qty == 0)로 복귀했을 때:
    1) 매수 FILLED 직후 15.0초 잔고 인덱싱 시차 그레이스 쉴드 구간이면 장부 초기화를 100% 차단한다(Case 102).
    2) 미체결 매수 주문(buy_order_id)만 대기 중인 상태이면 허위 익절 정산을 억제한다(Case 50).
    3) 실제 보유 포지션이 청산되어 0주가 된 경우, 검증된 매도 체결가와 수량으로
       실제 발생 순수익금(세전손익 - 왕복수수료 0.14%)을 산출하여 SOXS 와 SOXL 양쪽 장부 시드에 동시 반영한다.
    4) 04:07 ~ 07:45 EST 이전에 트레일링 익절(TP_WIN) 또는 후반부 안전탈출(PRE_VWAP_ESC)로 청산되고
       당일 NQ=F 진폭이 2.0% 미만이면 즉시 양쪽 장부의 is_session_done = False 로 해금하여
       프리장 다회전(Multi-Cycle) 무한 재출동을 가동한다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)

    snap_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    trail_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )
    grace_map: Dict[str, Dict[str, Any]] = getattr(
        context, "indexing_grace_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)
        if h_qty > 0:
            continue

        # Case 102: 매수 FILLED 직후 15.0초 인덱싱 시차 그레이스 쉴드 구간에는 절대 초기화 금지!
        grace_info = (
            grace_map.get(sym) if isinstance(grace_map.get(sym), dict) else {}
        )
        last_fill_mono = safe_float(grace_info.get("last_auto_fill_ts"), 0.0)
        if (
            last_fill_mono > 0.0
            and (time.monotonic() - last_fill_mono) <= INDEXING_GRACE_SHIELD_SEC
            and override_exit_price <= 0.0
        ):
            continue

        st = context.ledger.get_state(sym)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        sell_oid = str(st.get("sell_order_id") or "").strip()
        entry_sess = str(st.get("entry_session") or "").strip()
        snap_info = (
            snap_map.get(sym) if isinstance(snap_map.get(sym), dict) else {}
        )

        # Case 50: 아직 매수 덫 주문(buy_order_id)이 미체결 대기 중이고 매도 주문/보유 스냅샷이 없으면 패스
        if buy_oid and not sell_oid and not snap_info and override_exit_price <= 0.0:
            continue

        # 정산할 보유 이력이 전혀 없으면 패스
        if not sell_oid and not snap_info and override_exit_price <= 0.0:
            continue

        avg_buy_p = safe_float(
            snap_info.get("last_avg_price"), safe_float(st.get("buy_price"), 0.0)
        )
        if avg_buy_p <= 0.0:
            snap_map.pop(sym, None)
            trail_map.pop(sym, None)
            await context.ledger.update_state(
                sym, sell_order_id="", target_sell_price=0.0
            )
            continue

        exit_p, filled_qty = await _extract_verified_fill_price_and_qty(
            context=context,
            symbol=sym,
            sell_order_id=sell_oid,
            snap_info=snap_info,
            override_exit_price=override_exit_price,
        )
        if exit_p <= 0.0:
            exit_p = avg_buy_p

        cur_budget = safe_float(st.get("budget"), 1000.0)
        is_comp = bool(st.get("is_compound", True))
        pnl_res = calc_compound_next_budget(
            current_budget=cur_budget,
            avg_buy_price=avg_buy_p,
            avg_sell_price=exit_p,
            filled_qty=filled_qty,
            is_compound=is_comp,
        )
        next_budget = pnl_res["next_budget"]
        net_delta = pnl_res["net_delta_usd"]
        net_rate = pnl_res["net_rate_pct"]

        reason_label = (
            override_exit_reason
            or str(snap_info.get("last_sweep_reason") or "")
            or ("PRE_VWAP_ESC" if entry_sess == "PRE_VWAP_ESC" else "TP_WIN")
        )

        # 프리장 04:07 ~ 07:45 EST 이전 익절/안전탈출 완료 시 다회전(Multi-Cycle) 해금 판정
        is_stop_loss = any(
            k in reason_label.upper()
            for k in ("STAGE2", "STAGE3", "MOC", "OVERHEAT", "STOP")
        )
        can_multicycle = bool(
            not is_stop_loss
            and is_premarket_multicycle_unlock_window(hm)
            and nq_amp < NQ_OVERHEAT_THRESHOLD
            and entry_sess in ("PRE_VWAP_M050", "PRE_VWAP_ESC", "WHIPSAW_1SH")
        )

        # 양쪽 장부 시드(budget) 동시 동기화 및 포지션 필드 초기화
        other_sym = "SOXL" if sym == "SOXS" else "SOXS"
        next_entry_sess = (
            "CANCELED_MACRO"
            if nq_amp >= NQ_OVERHEAT_THRESHOLD
            else (
                "CANCELED_STAGE3"
                if "STAGE3" in reason_label.upper()
                else ""
            )
        )
        next_session_done = not can_multicycle

        await context.ledger.update_state(
            sym,
            budget=next_budget,
            buy_order_id="",
            sell_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            target_sell_price=0.0,
            entry_session=next_entry_sess,
            entry_trend="",
            is_session_done=next_session_done,
        )
        await context.ledger.update_state(
            other_sym,
            budget=next_budget,
            is_session_done=next_session_done,
            entry_session=(
                next_entry_sess
                if next_entry_sess in ("CANCELED_MACRO", "CANCELED_STAGE3")
                else context.ledger.get_state(other_sym).get("entry_session", "")
            ),
        )

        snap_map.pop(sym, None)
        trail_map.pop(sym, None)
        grace_map.pop(sym, None)

        if hasattr(context, "send_slim_alert"):
            sign = "+" if net_delta >= 0.0 else "-"
            r_sign = "+" if net_rate >= 0.0 else ""
            icon = "🎯💰" if net_delta >= 0.0 else "🛑💸"
            cycle_note = " · ♻️다회전 재출동 해금" if can_multicycle else ""
            await context.send_slim_alert(
                title=f"{icon} [{sym} 청산 정산 완료 · {reason_label}] ({r_sign}{net_rate:.2f}%)",
                body=(
                    f"{filled_qty}주 (${avg_buy_p:.2f}➔${exit_p:.2f}) · "
                    f"세후순익 {sign}${abs(net_delta):,.2f} · 차기시드 ${next_budget:,.2f}{cycle_note}"
                ),
            )

        return {
            "status": "RECONCILED_EXIT",
            "symbol": sym,
            "exit_price": exit_p,
            "filled_qty": filled_qty,
            "net_delta_usd": net_delta,
            "next_budget": next_budget,
            "multicycle_unlocked": can_multicycle,
        }

    return {"status": "NO_ZERO_EXIT_TO_RECONCILE"}
