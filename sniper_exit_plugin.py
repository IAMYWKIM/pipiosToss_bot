# -*- coding: utf-8 -*-
"""
# NEW: sniper_exit_plugin.py
실전 포지션 상단 안전 캡(+5.00% LIMIT) 선장전, +1.00% 점화 하드보존 하이킨 아시 트레일링(HARD_TRAIL_100),
프리장 후반(07:15 ~ 08:14 EST) 미익절 동적 VWAP 선제 탈출(PRE_VWAP_ESC), 07:45 EST 이전 다회전 해금 및
실제 발생 순수익금(Realized Net PnL) 통합 복리/단리 정산 플러그인 (Step 05 / 10)
- 제3헌법: 프리장/정규장 실전 익절 트레일링, 후반부 VWAP 탈출, 0주 정산 및 복리 동기화 전담 분리
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 4 & Case 19, 24, 46, 49, 51, 58, 68, 70, 76, 90, 99, 100, 101, 102 완벽 구현
"""

from __future__ import annotations

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
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_exit_plugin")

# 왕복 매매 수수료율 (매수 0.07% + 매도 0.07% = 0.14% 하드 락온, Case 76)
ROUND_TRIP_FEE_RATE_PCT: float = 0.14
# 상단 안전 캡 기본 수익률 (+5.00% LIMIT, 🎯 4)
SAFETY_CAP_PROFIT_PCT: float = 5.00
# 하드보존 트레일링 점화 수익률 (+1.00%, 🎯 4)
TRAIL_ARM_PROFIT_PCT: float = 1.00
# 하드보존 트레일링 영구 +1.00% 보존 잠금 트리거 (+1.30%, 🎯 4)
TRAIL_RATCHET_LOCK_PCT: float = 1.30
# +1.00% ~ +1.29% 구간 호가 흔들림 방어용 비상 쿠션 (+0.75%, 🎯 4)
TRAIL_CUSHION_FLOOR_PCT: float = 0.75
# 최고가 대비 기본 콜백 스탑 폭 (-0.20%, 🎯 4)
TRAIL_DEFAULT_CALLBACK_PCT: float = 0.20
# 프리장 후반 동적 VWAP 선제 탈출 최소 보유 경과 시간 (25.0분, 🎯 4)
LATE_VWAP_ESC_MIN_HOLD_MIN: float = 25.0
# 프리장 후반 동적 VWAP 선제 탈출 최소 수수료 보전 수익률 (+0.20%, 🎯 4)
LATE_VWAP_ESC_MIN_PROFIT_PCT: float = 0.20


# ---------------------------------------------------------------------------
# 통합 시드 풀(Unified Seed Pool) 실제 발생 순수익금 복리/단리 정산 공식 (🎯 4 & Case 76)
# ---------------------------------------------------------------------------
def calc_compound_next_budget(
    current_budget: float,
    avg_buy_price: float,
    avg_sell_price: float,
    filled_qty: int,
    is_compound: bool = True,
    fee_rate_pct: float = ROUND_TRIP_FEE_RATE_PCT,
) -> Dict[str, float]:
    """
    🎯 4 & Case 19, 76:
    실제 매수 평단가(avg_buy_price), 실제 매도 체결가(avg_sell_price), 실제 보유/체결 수량(filled_qty)을
    기반으로 세전 손익금(gross_pnl_usd)에서 왕복 수수료(0.14%)를 차감한 실제 발생 순수익금(net_delta_usd)을
    산출하고, 복리 모드(is_compound=True)일 경우 차기 통합 시드 예산(next_budget)을 반환한다.
    """
    cur_b = max(100.0, safe_float(current_budget, 1000.0))
    buy_p = safe_float(avg_buy_price, 0.0)
    sell_p = safe_float(avg_sell_price, 0.0)
    qty = max(0, safe_int(filled_qty, 0))

    if buy_p <= 0.0 or sell_p <= 0.0 or qty <= 0:
        return {
            "gross_pnl_usd": 0.0,
            "fee_usd": 0.0,
            "net_delta_usd": 0.0,
            "gross_rate_pct": 0.0,
            "net_rate_pct": 0.0,
            "next_budget": round(cur_b, 2),
        }

    buy_amt = buy_p * float(qty)
    sell_amt = sell_p * float(qty)
    gross_pnl = sell_amt - buy_amt
    # 왕복 수수료: 매수대금과 매도대금 평균(또는 매수대금 기준 왕복 0.14%) 적용
    fee_usd = ((buy_amt + sell_amt) * 0.5) * (safe_float(fee_rate_pct, 0.14) / 100.0)
    net_delta = gross_pnl - fee_usd

    gross_rate = ((sell_p - buy_p) / buy_p) * 100.0
    net_rate = (net_delta / buy_amt) * 100.0 if buy_amt > 0.0 else 0.0

    if is_compound:
        next_b = max(100.0, round(cur_b + net_delta, 2))
    else:
        next_b = round(cur_b, 2)

    return {
        "gross_pnl_usd": round(gross_pnl, 2),
        "fee_usd": round(fee_usd, 2),
        "net_delta_usd": round(net_delta, 2),
        "gross_rate_pct": round(gross_rate, 2),
        "net_rate_pct": round(net_rate, 2),
        "next_budget": next_b,
    }


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 매수 주문 진행 중(PENDING / PARTIAL_FILLED) 여부 점검 (Case 58)
# ---------------------------------------------------------------------------
async def _should_skip_trap_while_buy_pending(
    context: Any,
    symbol: str,
    holdings_qty: int,
    ledger_state: Dict[str, Any],
) -> bool:
    """
    Case 58:
    매수 주문이 아직 진행 중(PENDING / PARTIAL_FILLED)이거나 주문 수량 전체가
    원장 잔고(holdings_qty)에 반영되지 않은 상태에서는 부분 체결 수량 증발을 막기 위해
    매도 덫 장전을 대기(skip_trap = True)한다.
    """
    buy_oid = str(ledger_state.get("buy_order_id") or "").strip()
    if not buy_oid:
        return False

    ordered_qty = safe_int(ledger_state.get("ordered_qty"), 0)
    detail = await context.api.get_order_detail(buy_oid)
    if isinstance(detail, dict):
        st_str = str(detail.get("status") or "").upper().strip()
        exec_obj = (
            detail.get("execution")
            if isinstance(detail.get("execution"), dict)
            else {}
        )
        filled_qty = safe_int(exec_obj.get("filledQuantity"), 0)

        if st_str in ("PENDING", "PARTIAL_FILLED", "PENDING_REPLACE", "PENDING_CANCEL"):
            return True

        if st_str == "FILLED":
            target_q = filled_qty if filled_qty > 0 else ordered_qty
            if target_q > 0 and holdings_qty < target_q:
                # 아직 잔고 API 인덱싱에 전량 반영되지 않았으므로 대기
                return True
            # 전량 반영 확증 -> buy_order_id 소각하여 매도 덫 장전 단계로 진입
            await context.ledger.update_state(symbol, buy_order_id="")
            return False

        if st_str in ("CANCELED", "REJECTED"):
            await context.ledger.update_state(symbol, buy_order_id="")
            return False

    # 상세 조회 실패 시 ordered_qty 대비 holdings_qty 로 방어
    if ordered_qty > 0 and holdings_qty < ordered_qty:
        return True
    await context.ledger.update_state(symbol, buy_order_id="")
    return False


# ---------------------------------------------------------------------------
# [핵심 1] 상단 안전 캡(+5.00% LIMIT) 선장전 & +1.00% 점화 하드보존 HA 트레일링
# ---------------------------------------------------------------------------
async def manage_mechanical_profit_trap_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 4 & Case 24, 58, 68, 70, 90, 99:
    1) [1단계] 매수 전량 체결 확인 즉시 토스 팩트 평단가(avg_price) 기준
       +5.00% LIMIT 상단 안전 캡(1주 휩소 WHIPSAW_1SH 는 +1.00% LIMIT) 매도 주문을 선장전하고,
       최초 장전 시(if not is_rearm:) 실시간 QQQ 현재가(nq_entry_price)와 NQ=F 진폭(nq_entry_amp)을
       3차 방어 기준점으로 동기화(오버라이드)하며, 매 틱 _EXIT_SNAPSHOT_CACHE 에 실보유 수량/평단가를 백업한다.
    2) [2단계] 실시간 현재가(live_p)가 +1.00% 점화가(arm_price) 도달 시 트레일링을 점화(armed=True)하고,
       경유 최고가(peak_price)가 +1.30% 도달 전에는 +0.75% 비상 쿠션을, +1.30% 돌파 시에는 +1.00% 보존선을
       영구 하드 잠금한다.
    3) [3단계] 🕯️HA양봉 유지 중 또는 토스 실시간 QQQ 10m/3m ±0.20% 유리 방향 급등락(🚀 NQ_VEL_BYPASS) 중에는
       콜백 스탑을 유예하고 홀딩하다가, 🌑HA음봉 전환 + 최고가 대비 -0.20% 콜백 스탑선 이하 도달(또는 바닥 보존선 이탈) 시
       0.1초 즉시 정정 스윕(sweep_sell_position_bulletproof)을 발사한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    trail_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )
    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
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

        entry_sess = str(st.get("entry_session") or "PRE_VWAP_M050")
        # 1주 휩소 작전(WHIPSAW_1SH)의 매도 덫은 whipsaw_plugin 이 +1.00% 고정 익절로 관리하되
        # 여기서는 스냅샷 백업 및 안전 캡 누락 시 보조 장전을 수행한다.
        is_whipsaw_1sh = bool(entry_sess == "WHIPSAW_1SH")

        # Case 58: 매수 주문이 아직 부분 체결 진행 중이면 매도 덫 장전 대기(skip_trap)
        if await _should_skip_trap_while_buy_pending(context, sym, h_qty, st):
            continue

        raw_met_cache = getattr(context, "candle_metrics_cache", {})
        base_met = raw_met_cache.get(sym) if isinstance(raw_met_cache, dict) else None
        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p <= 0.0:
            live_p = safe_float(h_info.get("last_price"), avg_p)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_now)

        # 트레일링 점화가(+1.00% 기본, /settlement 제어반에서 target_profit_rate 토글 시 반영)
        cfg_rate = safe_float(st.get("target_profit_rate"), TRAIL_ARM_PROFIT_PCT)
        if cfg_rate <= 0.0:
            cfg_rate = TRAIL_ARM_PROFIT_PCT
        arm_price = math.ceil((avg_p * (1.0 + cfg_rate / 100.0)) * 100.0) / 100.0
        safety_cap_price = (
            arm_price
            if is_whipsaw_1sh
            else (math.ceil((avg_p * (1.0 + SAFETY_CAP_PROFIT_PCT / 100.0)) * 100.0) / 100.0)
        )

        # 트레일링 런타임 캐시 초기화/갱신
        t_state = trail_cache_map.get(sym)
        if not isinstance(t_state, dict) or abs(safe_float(t_state.get("avg_price"), 0.0) - avg_p) > 0.005:
            t_state = {
                "symbol": sym,
                "avg_price": avg_p,
                "arm_price": arm_price,
                "safety_cap_price": safety_cap_price,
                "armed": False,
                "ratchet_130_locked": False,
                "peak_price": max(avg_p, live_p),
                "floor_price": 0.0,
                "effective_stop_p": 0.0,
                "callback_pct": TRAIL_DEFAULT_CALLBACK_PCT,
                "sweep_triggered": False,
                "last_sweep_ts": 0.0,
            }
            trail_cache_map[sym] = t_state

        if live_p > safe_float(t_state.get("peak_price"), 0.0):
            t_state["peak_price"] = live_p

        # Case 19: 매 틱마다 _EXIT_SNAPSHOT_CACHE 에 실보유 수량/평단가/최고가 상시 백업
        prev_snap = snap_cache_map.get(sym) if isinstance(snap_cache_map.get(sym), dict) else {}
        snap_cache_map[sym] = {
            "symbol": sym,
            "last_holdings_qty": h_qty,
            "last_avg_price": avg_p,
            "peak_price": t_state["peak_price"],
            "entry_session": entry_sess,
            "safety_cap_price": safety_cap_price,
            "arm_price": arm_price,
            "last_sweep_price": safe_float(prev_snap.get("last_sweep_price"), 0.0),
            "exit_tag": str(prev_snap.get("exit_tag") or ""),
            "updated_ts": time.time(),
        }

        # -------------------------------------------------------------------
        # [1단계] 상단 안전 캡(+5.00% LIMIT) 선장전 및 최초 체결 시점 QQQ 기준가 동기화
        # -------------------------------------------------------------------
        sell_oid = str(st.get("sell_order_id") or "").strip()
        need_new_trap = not bool(sell_oid)
        is_rearm = bool(safe_float(st.get("target_sell_price"), 0.0) > 0.0)

        if sell_oid and not t_state.get("sweep_triggered"):
            # 기장전된 안전 캡 매도 주문이 외부 취소되었는지 주기적 점검
            s_detail = await context.api.get_order_detail(sell_oid)
            if isinstance(s_detail, dict):
                s_status = str(s_detail.get("status") or "").upper().strip()
                if s_status in ("CANCELED", "REJECTED"):
                    need_new_trap = True
                    is_rearm = True

        if need_new_trap and not t_state.get("sweep_triggered"):
            async with context.in_memory_ordering_lock:
                # Case 70: 최초로 덫을 장전하는 원자적 순간(if not is_rearm:)에 실시간 QQQ 기준가와 진폭 동기화
                update_kwargs: Dict[str, Any] = {
                    "holdings_qty": h_qty,
                    "avg_price": avg_p,
                    "target_sell_price": arm_price,
                }
                if not is_rearm or safe_float(st.get("nq_entry_price"), 0.0) <= 0.0:
                    qqq_now = await context.macro_cache.ensure_qqq_live_price(
                        safe_float((macro or {}).get("qqq_price"), 0.0)
                    )
                    if qqq_now <= 0.0:
                        qqq_now = safe_float((macro or {}).get("nq_price"), 0.0)
                    nq_amp_now = safe_float((macro or {}).get("nq_amp"), 0.0)
                    update_kwargs["nq_entry_price"] = qqq_now
                    if safe_float(st.get("nq_entry_amp"), 0.0) <= 0.0:
                        update_kwargs["nq_entry_amp"] = nq_amp_now
                    if safe_float(st.get("entry_ts"), 0.0) <= 0.0:
                        update_kwargs["entry_ts"] = time.time()

                try:
                    cid = generate_client_order_id(prefix=f"CAP{sym}")
                    ord_res = await context.api.create_order(
                        symbol=sym,
                        side="SELL",
                        quantity=h_qty,
                        price=safety_cap_price,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        client_order_id=cid,
                        enforce_daymarket_block=False,
                    )
                    new_sell_oid = str(ord_res.get("orderId") or "").strip()
                    update_kwargs["sell_order_id"] = new_sell_oid
                    await context.ledger.update_state(sym, **update_kwargs)
                    sell_oid = new_sell_oid

                    if not is_rearm and hasattr(context, "send_slim_alert"):
                        cap_label = "+1.00% 익절덫" if is_whipsaw_1sh else "+5.00% 안전캡"
                        await context.send_slim_alert(
                            title=f"🛡️ [{sym} 매수 체결 확증 · {cap_label} 장전] {h_qty}주",
                            body=(
                                f"평단 ${avg_p:.2f} · 점화가 ${arm_price:.2f}(+{cfg_rate:.2f}%) "
                                f"· 안전캡 ${safety_cap_price:.2f} · QQQ기준 ${update_kwargs.get('nq_entry_price', st.get('nq_entry_price', 0.0)):.2f}"
                            ),
                        )
                except Exception as exc:
                    logger.warning("⚠️ [%s] 상단 안전캡 매도 주문 장전 실패: %s", sym, exc)

        # 1주 휩소 작전(WHIPSAW_1SH)은 +1.00% 지정가 체결을 대기하므로 HA 트레일링에서 제외
        if is_whipsaw_1sh:
            continue

        # -------------------------------------------------------------------
        # [2단계] +1.00% 돌파 시 하드보존 하이킨 아시 트레일링 점화 및 최소 보존선 잠금
        # -------------------------------------------------------------------
        if live_p >= arm_price and not t_state.get("armed"):
            t_state["armed"] = True
            t_state["peak_price"] = max(safe_float(t_state.get("peak_price"), live_p), live_p)
            logger.info(
                "🔥 [HARD_TRAIL_100 점화] %s 현재가 $%.2f >= 점화가 $%.2f (평단 $%.2f)",
                sym,
                live_p,
                arm_price,
                avg_p,
            )
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🔥 [{sym} +{cfg_rate:.2f}% 하드보존 HA 트레일링 점화]",
                    body=(
                        f"현가 ${live_p:.2f} (평단 ${avg_p:.2f}) · 🕯️HA양봉 홀딩 & "
                        f"🌑HA음봉 -0.20% 콜백 감시 개시"
                    ),
                )

        if not t_state.get("armed"):
            continue

        peak_p = max(safe_float(t_state.get("peak_price"), live_p), live_p)
        t_state["peak_price"] = peak_p

        ratchet_130_p = avg_p * (1.0 + TRAIL_RATCHET_LOCK_PCT / 100.0)
        if peak_p >= ratchet_130_p:
            t_state["ratchet_130_locked"] = True

        cushion_floor = math.ceil((avg_p * (1.0 + TRAIL_CUSHION_FLOOR_PCT / 100.0)) * 100.0) / 100.0
        floor_price = arm_price if t_state.get("ratchet_130_locked") else cushion_floor
        active_cb_pct = safe_float(t_state.get("callback_pct"), TRAIL_DEFAULT_CALLBACK_PCT)
        cb_stop_p = math.floor((peak_p * (1.0 - active_cb_pct / 100.0)) * 100.0) / 100.0
        effective_stop_p = max(floor_price, cb_stop_p)

        t_state["floor_price"] = floor_price
        t_state["effective_stop_p"] = effective_stop_p

        # -------------------------------------------------------------------
        # [3단계] 🕯️HA양봉 홀딩 & 🚀 NQ_VEL_BYPASS vs 🌑HA음봉 + -0.20% 콜백 스윕 판정
        # -------------------------------------------------------------------
        is_ha_bull = bool(met.get("is_ha_bull", False))
        nq_vel_bypass = bool(
            (macro or {}).get("bypass_soxs")
            if sym == "SOXS"
            else (macro or {}).get("bypass_soxl")
        )

        # 바닥 보존선(비상 쿠션 +0.75% 또는 +1.30% 돌파 후 +1.00% 하드 보존선) 이탈 시 양봉 불문 즉시 스윕
        breached_hard_floor = bool(live_p < floor_price)
        # 🌑HA음봉 전환(및 QQQ 급등락 바이패스 아님) + 최고가 대비 -0.20% 콜백 스탑선 도달
        triggered_ha_callback = bool(
            not is_ha_bull and not nq_vel_bypass and live_p <= effective_stop_p
        )

        if not (breached_hard_floor or triggered_ha_callback):
            continue

        # 중복 스윕 발사 쿨다운(1.5초) 방어
        if (time.monotonic() - safe_float(t_state.get("last_sweep_ts"), 0.0)) < 1.5:
            continue

        async with context.in_memory_ordering_lock:
            t_state["sweep_triggered"] = True
            t_state["last_sweep_ts"] = time.monotonic()
            try:
                sweep_res = await context.api.sweep_sell_position_bulletproof(
                    symbol=sym,
                    holdings_qty=h_qty,
                    sell_order_id=sell_oid,
                    fallback_price=live_p,
                    min_floor_price=floor_price if not breached_hard_floor else 0.0,
                )
                new_oid = str(sweep_res.get("orderId") or sell_oid).strip()
                sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)

                snap_cache_map[sym]["last_sweep_price"] = sweep_p
                snap_cache_map[sym]["exit_tag"] = "TP_WIN"

                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    target_sell_price=sweep_p,
                )
                context.wakeup_event.set()

                reason_str = (
                    f"보존선(${floor_price:.2f}) 수호"
                    if breached_hard_floor
                    else f"🌑HA음봉 + 고점(${peak_p:.2f}) -0.20% 콜백"
                )
                logger.info(
                    "🎯 [HARD_TRAIL_100 스윕 발사] %s %d주 @ $%.2f (%s)",
                    sym,
                    h_qty,
                    sweep_p,
                    reason_str,
                )
                return {
                    "status": "TRAIL_SWEEP_FIRED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "peak_price": peak_p,
                    "reason": reason_str,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 하드보존 HA 트레일링 스윕 실패: %s", sym, exc)

    return {"status": "OK"}


# ---------------------------------------------------------------------------
# [핵심 2] 프리장 후반 (07:15 ~ 08:14 EST) 미익절 표류 물량 동적 VWAP 선제 탈출 (🛟 PRE_VWAP_ESC)
# ---------------------------------------------------------------------------
async def execute_premarket_late_vwap_escape(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 4 & Case 100:
    - 프리장 후반부(07:15:00 ~ 08:14:59 EST) 동안 진입 후 25분 이상 경과하고
      아직 +1.00% 트레일링(armed)을 점화하지 못한 채 표류 중인 자동 포지션에 대해,
    - 실시간 현재가(live_p)가 실시간 동적 프리장 VWAP_t 이상이면서 동시에
      최소 수수료 보전 단가(esc_target_p = max(floor(pre_vwap*100)/100, ceil(avg_price*1.0020*100)/100))
      이상에 도달하면 즉시 안전 캡 매도 주문을 정정 스윕하여 세후 흑자 상태로 안전 탈출(PRE_VWAP_ESC)한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (715 <= hm <= 814):
        return {"status": "OUTSIDE_WINDOW"}

    trail_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )
    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
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

        entry_sess = str(st.get("entry_session") or "")
        # 수동 진입(MANUAL)이 아닌 프리장 자동 진입 포지션만 대상
        if entry_sess not in ("PRE_VWAP_M050", "RE_BOT_TRAIL"):
            continue

        # 이미 +1.00% 트레일링이 점화된 상태라면 HARD_TRAIL_100 이 전담
        t_state = trail_cache_map.get(sym, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            continue

        entry_ts = safe_float(st.get("entry_ts"), 0.0)
        hold_minutes = (time.time() - entry_ts) / 60.0 if entry_ts > 0.0 else 30.0
        if hold_minutes < LATE_VWAP_ESC_MIN_HOLD_MIN:
            continue

        raw_met_cache = getattr(context, "candle_metrics_cache", {})
        base_met = raw_met_cache.get(sym) if isinstance(raw_met_cache, dict) else None
        live_p = safe_float((prices or {}).get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_now)

        pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
        live_p = safe_float(met.get("live_price"), 0.0)
        if pre_vwap <= 0.0 or live_p <= 0.0:
            continue

        min_fee_p = math.ceil((avg_p * (1.0 + LATE_VWAP_ESC_MIN_PROFIT_PCT / 100.0)) * 100.0) / 100.0
        vwap_floor_p = math.floor(pre_vwap * 100.0) / 100.0
        esc_target_p = max(vwap_floor_p, min_fee_p)

        if live_p < esc_target_p:
            continue

        sell_oid = str(st.get("sell_order_id") or "").strip()
        async with context.in_memory_ordering_lock:
            try:
                sweep_res = await context.api.sweep_sell_position_bulletproof(
                    symbol=sym,
                    holdings_qty=h_qty,
                    sell_order_id=sell_oid,
                    fallback_price=live_p,
                    min_floor_price=min_fee_p,
                )
                sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)
                new_oid = str(sweep_res.get("orderId") or sell_oid).strip()

                if sym not in snap_cache_map or not isinstance(snap_cache_map.get(sym), dict):
                    snap_cache_map[sym] = {
                        "symbol": sym,
                        "last_holdings_qty": h_qty,
                        "last_avg_price": avg_p,
                    }
                snap_cache_map[sym]["last_sweep_price"] = sweep_p
                snap_cache_map[sym]["exit_tag"] = "PRE_VWAP_ESC"

                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    entry_session="PRE_VWAP_ESC",
                    target_sell_price=sweep_p,
                )
                context.wakeup_event.set()

                logger.info(
                    "🛟 [PRE_VWAP_ESC 선제 탈출] %s %d주 @ $%.2f (평단 $%.2f, 동적VWAP $%.2f, 보유 %.1f분)",
                    sym,
                    h_qty,
                    sweep_p,
                    avg_p,
                    pre_vwap,
                    hold_minutes,
                )
                return {
                    "status": "PRE_VWAP_ESC_FIRED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "pre_vwap": pre_vwap,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 프리장 후반 동적 VWAP 선제 탈출 실패: %s", sym, exc)

    return {"status": "NO_ESCAPE_NEEDED"}


# ---------------------------------------------------------------------------
# [핵심 3] 잔고 0주 복귀 시 실발생 순수익금(Realized Net PnL) 정산 및 프리장 다회전 해금
# ---------------------------------------------------------------------------
async def _extract_verified_exit_fill(
    context: Any,
    symbol: str,
    sell_order_id: str,
    snap: Dict[str, Any],
    fallback_live_price: float,
) -> Tuple[float, int, str]:
    """
    Case 19 & Case 51:
    1) REPLACED / CANCELED 상태인 원주문의 미체결 상단 안전 캡 단가(+5.00%)를 체결가로 오인하는 행위를
       100% 금지한다!
    2) 오직 status == 'FILLED' (또는 체결 수량이 존재하는 종료 주문)의 execution.averageFilledPrice 만
       신뢰하며, 조회 불가 시 _EXIT_SNAPSHOT_CACHE[symbol]['last_sweep_price'] 와
       실보유 수량('last_holdings_qty')을 폴백으로 사용한다.
    """
    snap_qty = max(1, safe_int(snap.get("last_holdings_qty"), 1))
    snap_sweep_p = safe_float(snap.get("last_sweep_price"), 0.0)
    snap_avg_p = safe_float(snap.get("last_avg_price"), 0.0)

    # 1) sell_order_id 상세 조회
    if sell_order_id:
        detail = await context.api.get_order_detail(sell_order_id)
        if isinstance(detail, dict):
            st_str = str(detail.get("status") or "").upper().strip()
            exec_obj = (
                detail.get("execution")
                if isinstance(detail.get("execution"), dict)
                else {}
            )
            f_qty = safe_int(exec_obj.get("filledQuantity"), 0)
            f_avg_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
            if st_str == "FILLED" and f_avg_p > 0.0:
                return f_avg_p, (f_qty if f_qty > 0 else snap_qty), "ORDER_DETAIL_FILLED"

    # 2) 최근 CLOSED 매도 주문 목록에서 FILLED 주문 검색 (REPLACED / CANCELED 의 price 필드는 절대 사용 금지!)
    closed_orders = await context.api.get_orders(status="CLOSED", symbol=symbol, limit=15)
    for o in closed_orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("side") or "").upper() != "SELL":
            continue
        st_str = str(o.get("status") or "").upper().strip()
        exec_obj = o.get("execution") if isinstance(o.get("execution"), dict) else {}
        f_qty = safe_int(exec_obj.get("filledQuantity"), 0)
        f_avg_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
        if st_str == "FILLED" and f_qty > 0 and f_avg_p > 0.0:
            return f_avg_p, f_qty, "CLOSED_HISTORY_FILLED"

    # 3) 스냅샷의 실제 스윕 단가 또는 현재가 폴백
    eff_sell_p = (
        snap_sweep_p
        if snap_sweep_p > 0.0
        else (fallback_live_price if fallback_live_price > 0.0 else snap_avg_p)
    )
    return eff_sell_p, snap_qty, "SNAPSHOT_FALLBACK"


async def reconcile_zero_holdings_exit_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Optional[Dict[str, Any]] = None,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 2-3] & 🎯 4 & Case 19, 46, 49, 51, 76, 99, 102:
    - 매 틱 호출되어 보유 잔고가 0주(holdings_qty == 0)로 돌아온 종목을 감지한다.
    - [Case 102 방어] 매수 FILLED 직후 잔고 API 인덱싱 시차(0.1 ~ 0.4초, indexing_grace_cache 15초 내)
      동안에는 절대로 장부를 초기화하지 않고 보호한다!
    - [Case 51 방어] 만약 잔고가 0주인데 거래소에 OPEN 상태인 매도 주문이 여전히 살아있다면
      일시적 API 결측치이므로 허위 익절(False Positive) 정산을 보류한다.
    - 실제 전량 청산이 확증되면:
      1) 잔존 OPEN 매도 주문을 깨끗이 정리하고,
      2) REPLACED 안전캡 단가를 배제한 실제 체결가(execution.averageFilledPrice)로
         왕복 수수료(0.14%) 차감 실발생 순수익금(net_delta_usd)을 산출하여
         SOXS 와 SOXL 양쪽 시드(budget)에 동시에 복리/단리 동기화하며,
      3) 04:07 ~ 07:45 EST 이전에 익절(TP_WIN) 또는 안전탈출(PRE_VWAP_ESC)로 청산되고
         당일 NQ=F < 2.0% 이면 즉시 양쪽 장부의 is_session_done = False 로 해금하여
         프리장 다회전(Multi-Cycle) 무한 재출동을 가동한다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)

    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    trail_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )
    grace_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "indexing_grace_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)
        if h_qty > 0:
            # 실잔고가 정상 확인되면 인덱싱 그레이스 쉴드 해제
            if sym in grace_cache_map:
                grace_cache_map.pop(sym, None)
            continue

        st = context.ledger.get_state(sym)
        snap = snap_cache_map.get(sym) if isinstance(snap_cache_map.get(sym), dict) else {}

        had_position = bool(
            safe_int(st.get("holdings_qty"), 0) > 0
            or safe_float(st.get("avg_price"), 0.0) > 0.0
            or str(st.get("sell_order_id") or "").strip()
            or safe_int(snap.get("last_holdings_qty"), 0) > 0
        )
        if not had_position:
            continue

        # -------------------------------------------------------------------
        # 🚨 Case 102 절대 방어: 매수 FILLED 직후 잔고 인덱싱 시차(15초 이내)이거나
        # 아직 buy_order_id 가 살아있는 동안에는 0주 정산 초기화를 100% 차단한다!
        # -------------------------------------------------------------------
        if str(st.get("buy_order_id") or "").strip():
            continue

        grace_info = grace_cache_map.get(sym)
        if isinstance(grace_info, dict):
            elapsed_grace = time.monotonic() - safe_float(
                grace_info.get("last_auto_fill_ts"), 0.0
            )
            if elapsed_grace < 15.0:
                logger.debug(
                    "🛡️ [Case 102 인덱싱 시차 방어] %s 매수 체결 후 %.2fs 경과 -> 0주 정산 보류",
                    sym,
                    elapsed_grace,
                )
                continue
            grace_cache_map.pop(sym, None)

        # Case 51: 잔고 0주이나 거래소에 OPEN 매도 주문이 아직 살아있고 체결수량 0이면 일시 결측치 방어
        open_orders = await context.api.get_orders(status="OPEN", symbol=sym)
        has_unfilled_open_sell = False
        for o in open_orders:
            if isinstance(o, dict) and str(o.get("side") or "").upper() == "SELL":
                exec_obj = o.get("execution") if isinstance(o.get("execution"), dict) else {}
                if safe_int(exec_obj.get("filledQuantity"), 0) == 0:
                    has_unfilled_open_sell = True
                    break

        if has_unfilled_open_sell and not bool(trail_cache_map.get(sym, {}).get("sweep_triggered")):
            # 단, 사용자가 수동으로 시장가 매도하여 잔고가 0주가 된 경우에는 남은 안전캡 주문을 취소해야 함
            sellable_q = await context.api.get_sellable_quantity(sym)
            if sellable_q > 0:
                continue
            for o in open_orders:
                if isinstance(o, dict) and str(o.get("side") or "").upper() == "SELL":
                    oid = str(o.get("orderId") or "").strip()
                    if oid:
                        try:
                            await context.api.cancel_order(oid)
                        except Exception:
                            pass

        # 실제 매수 평단가 및 매도 체결가/수량 확정
        buy_avg_p = safe_float(snap.get("last_avg_price"), safe_float(st.get("avg_price"), 0.0))
        sell_oid = str(st.get("sell_order_id") or "").strip()
        live_p = safe_float((prices or {}).get(sym), buy_avg_p)

        sell_fill_p, filled_qty, fill_src = await _extract_verified_exit_fill(
            context=context,
            symbol=sym,
            sell_order_id=sell_oid,
            snap=snap,
            fallback_live_price=live_p,
        )

        cur_budget = safe_float(st.get("budget"), 1000.0)
        is_comp = bool(st.get("is_compound", True))
        pnl_info = calc_compound_next_budget(
            current_budget=cur_budget,
            avg_buy_price=buy_avg_p,
            avg_sell_price=sell_fill_p,
            filled_qty=filled_qty,
            is_compound=is_comp,
        )

        entry_sess = str(
            snap.get("entry_session") or st.get("entry_session") or "PRE_VWAP_M050"
        )
        exit_tag = str(snap.get("exit_tag") or "")
        if not exit_tag:
            if entry_sess == "PRE_VWAP_ESC":
                exit_tag = "PRE_VWAP_ESC"
            elif entry_sess == "VWAP_ESCAPE":
                exit_tag = "VWAP_ESCAPE"
            elif pnl_info["net_delta_usd"] >= 0.0:
                exit_tag = "TP_WIN"
            else:
                exit_tag = "EXIT_LOSS"

        # -------------------------------------------------------------------
        # 🎯 3 [Step 2-3]: 프리장 다회전(Multi-Cycle) 무한 재출동 판정
        # 04:07 ~ 07:45 EST 이전에 TP_WIN 또는 PRE_VWAP_ESC 로 청산 완료되고
        # NQ=F < 2.0% 이면 즉시 양쪽 장부의 is_session_done = False 로 해금!
        # -------------------------------------------------------------------
        bottom_active = bool(getattr(context, "bottom_track_cache", {}).get("active"))
        is_multicycle_eligible = bool(
            407 <= hm <= 745
            and nq_amp < NQ_OVERHEAT_THRESHOLD
            and exit_tag in ("TP_WIN", "PRE_VWAP_ESC")
            and entry_sess in ("PRE_VWAP_M050", "RE_BOT_TRAIL", "PRE_VWAP_ESC")
        )
        is_whipsaw_done = bool(entry_sess == "WHIPSAW_1SH" and hm < 815)

        if bottom_active or is_multicycle_eligible or (is_whipsaw_done and nq_amp < NQ_OVERHEAT_THRESHOLD):
            next_session_done = False
            next_entry_session = "STAGE3_STOP_TRACK" if bottom_active else ""
        elif nq_amp >= NQ_OVERHEAT_THRESHOLD and entry_sess != "MANUAL":
            next_session_done = True
            next_entry_session = "CANCELED_MACRO"
        else:
            next_session_done = bool(st.get("is_session_done", False))
            next_entry_session = "" if not next_session_done else "DONE_TODAY"

        other_sym = "SOXL" if sym == "SOXS" else "SOXS"
        new_budget = pnl_info["next_budget"]

        # 양쪽 원장 시드(budget) 동시 동기화 및 포지션 필드 원자적 클리어
        await context.ledger.update_state(
            sym,
            budget=new_budget,
            holdings_qty=0,
            avg_price=0.0,
            buy_order_id="",
            sell_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            target_sell_price=0.0,
            nq_entry_price=0.0,
            nq_entry_amp=0.0,
            entry_session=next_entry_session,
            is_session_done=next_session_done,
            realized_pnl_usd=round(
                safe_float(st.get("realized_pnl_usd"), 0.0) + pnl_info["net_delta_usd"],
                2,
            ),
            trade_count=safe_int(st.get("trade_count"), 0) + 1,
            win_count=safe_int(st.get("win_count"), 0)
            + (1 if pnl_info["net_delta_usd"] >= 0.0 else 0),
        )
        await context.ledger.update_state(
            other_sym,
            budget=new_budget,
            is_session_done=next_session_done,
            entry_session="CANCELED_MACRO" if next_entry_session == "CANCELED_MACRO" else "",
        )

        # 인메모리 트레일링/스냅샷 캐시 정리
        trail_cache_map.pop(sym, None)
        snap_cache_map.pop(sym, None)

        # Case 46 & Case 49: 청산 손익(PnL) 텔레그램 2줄 초슬림 타전
        if hasattr(context, "send_slim_alert"):
            sign = "+" if pnl_info["net_delta_usd"] >= 0 else ""
            mode_str = "복리" if is_comp else "단리"
            reentry_str = " · ♻️다회전 재출동 해금" if is_multicycle_eligible else ""
            title_icon = "🎯" if pnl_info["net_delta_usd"] >= 0 else "🛑"
            await context.send_slim_alert(
                title=(
                    f"{title_icon} [{sym} 청산 정산 완료 · {exit_tag}] "
                    f"{filled_qty}주 ({sign}${pnl_info['net_delta_usd']:.2f} · {sign}{pnl_info['net_rate_pct']:.2f}%)"
                ),
                body=(
                    f"매수 ${buy_avg_p:.2f} ➔ 매도 ${sell_fill_p:.2f} (수수료 ${pnl_info['fee_usd']:.2f}) "
                    f"· 차기 {mode_str}시드 ${new_budget:,.2f}{reentry_str}"
                ),
            )

        return {
            "status": "RECONCILED_EXIT",
            "symbol": sym,
            "exit_tag": exit_tag,
            "fill_source": fill_src,
            "pnl": pnl_info,
            "multicycle_unlocked": is_multicycle_eligible,
        }

    return {"status": "NO_EXIT_DETECTED"}
