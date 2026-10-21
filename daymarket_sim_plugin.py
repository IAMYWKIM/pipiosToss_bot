# -*- coding: utf-8 -*-
"""
# NEW: daymarket_sim_plugin.py
데이장 (19:00 ~ 03:57 EST) 100% [모의] 전용 프리장 동형(Isomorphic) 스나이퍼 플러그인 (Step 07 / 10)
- 🚨 데이장 실거래 매수 100% 물리적 절대 차단 (create_order 호출 0건!)
- 🎯 1 & Case 02, 60, 77 ~ 94, 97 ~ 100: 프리장 본진(🎯 3 & 🎯 4 & 🎯 5)을 절대기준으로 100% 동일한 전략 구동
  1) [Step 1] 19:00 ~ 19:07 EST 누적 데이장 VWAP(VWAP_1907) 산출 후 19:07 EST 기준
     현재가 > VWAP_1907 (disp_1907 > 0.0%, 양쪽 상회 시 이격도 상위 1위) 데이장 추세 주도주 자동 선정/복원
  2) [Step 2] 19:07 ~ 03:35 EST 실시간 동적 데이장 VWAP_day_t 대비 -0.50% 언더슈팅 하단가
     (VWAP_day_t * 0.9950 또는 하이브리드 min(VWAP_1907, VWAP_day_t * 0.9950))에 [모의] 매수 덫 장전,
     15초 & $0.03 괴리 시 자동 추적 정정, 현재가 도달 시 즉시 [모의] 체결 및 QQQ 0.00% 기준가 락온,
     03:25 EST 이전 익절/탈출 완료 시 다회전(Multi-Cycle) 무한 [모의] 재출동
  3) [Step 3] [모의] 체결 즉시 +5.00% 상단 안전 캡 & +1.00% 점화 하드보존 하이킨 아시 트레일링
     (HARD_TRAIL_100 : +1.30% 전 +0.75% 비상 쿠션 / +1.30% 돌파 시 +1.00% 하드 잠금,
     🕯️HA양봉 홀딩 & 🌑HA음봉 + 최고가 대비 -0.20% 콜백 스윕 × 실시간 QQQ ±0.20% NQ_VEL_BYPASS)
  4) [Step 4] 데이장 후반(02:45 ~ 03:57 EST) 진입 25분 경과 미익절 표류 물량 동적 데이장 VWAP
     (최소 평단 +0.20% 수수료 보전선) 도달 즉시 [모의] 선제 탈출(DAY_VWAP_ESC)
  5) [Step 5] 1·2차 NQ=F >= 2.0% 과열 차단/덤핑 & 3차 실시간 QQQ 최소 0.80% 역행 [모의] 손절 후
     손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 [모의] 재진입(RE_BOT_TRAIL) 및 실시간 승률/세후수익률 집계
  6) 03:58 ~ 03:59 EST 데이장 마감 2분 전 묵음 바이패스 및 미결 모의 포지션 자동 클리어
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import NQ_OVERHEAT_THRESHOLD, calc_qqq_adverse_status
from quant_indicators_plugin import (
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
    select_vwap_trend_leader,
)
from sniper_exit_plugin import (
    LATE_VWAP_ESC_MIN_HOLD_MIN,
    LATE_VWAP_ESC_MIN_PROFIT_PCT,
    ROUND_TRIP_FEE_RATE_PCT,
    SAFETY_CAP_PROFIT_PCT,
    TRAIL_ARM_PROFIT_PCT,
    TRAIL_CUSHION_FLOOR_PCT,
    TRAIL_DEFAULT_CALLBACK_PCT,
    TRAIL_RATCHET_LOCK_PCT,
)
from sniper_plugin import (
    TRAP_MODIFY_COOLDOWN_SEC,
    TRAP_MODIFY_MIN_DIFF_PCT,
    TRAP_MODIFY_MIN_DIFF_USD,
)
from toss_api import EST_TZ, REAL_ALLOWED_SYMBOLS, safe_float, safe_int

logger = logging.getLogger("toss_sniper.daymarket_sim_plugin")


# ---------------------------------------------------------------------------
# 시간대 판별 유틸리티 (데이장 19:00 ~ 03:57 EST 및 세부 전술 구간)
# ---------------------------------------------------------------------------
def is_daymarket_sim_active_window(hm: int) -> bool:
    """
    19:00 ~ 03:57 EST 데이장 [모의] 활성 구간 여부 반환.
    """
    return bool(hm >= 1900 or hm <= 357)


def is_daymarket_leader_lock_window(hm: int) -> bool:
    """
    19:07 ~ 03:57 EST 데이장 추세 주도주 확정 및 타격/감시 구간 여부 반환.
    """
    return bool(hm >= 1907 or hm <= 357)


def is_daymarket_trap_entry_window(hm: int) -> bool:
    """
    19:07 ~ 03:35 EST 데이장 신규 [모의] 매수 덫 장전 및 추적 정정 허용 구간.
    """
    return bool(hm >= 1907 or hm <= 335)


def is_daymarket_multicycle_unlock_window(hm: int) -> bool:
    """
    19:07 ~ 03:25 EST 데이장 익절/안전탈출 완료 시 다회전(Multi-Cycle) 재출동 해금 허용 구간.
    """
    return bool(hm >= 1907 or hm <= 325)


def is_daymarket_late_vwap_esc_window(hm: int) -> bool:
    """
    02:45 ~ 03:57 EST 데이장 후반 미익절 표류 물량 동적 데이장 VWAP 선제 탈출(DAY_VWAP_ESC) 구간.
    """
    return bool(245 <= hm <= 357)


# ---------------------------------------------------------------------------
# 데이장 [모의] 상태 초기화 및 조회 헬퍼 (Case 02, Case 77)
# ---------------------------------------------------------------------------
def create_default_day_sim_state(logical_date: str = "") -> Dict[str, Any]:
    """
    데이장 100% [모의] 전용 상태 머신 기본 딕셔너리를 생성한다.
    """
    return {
        "logical_date": str(logical_date or ""),
        "phase": "IDLE",  # IDLE | TRAP_WAITING | HOLDING | BOTTOM_TRACK | DONE_OVERHEAT | CLEARED
        "symbol": "",
        "trig_mode": "",  # DAY_VWAP_M050 | RE_BOT_TRAIL
        # [모의] 매수 덫 대기 필드
        "sim_trap_buy_price": 0.0,
        "last_modify_mono": 0.0,
        # [모의] 보유 포지션 필드
        "entry_price": 0.0,
        "entry_ts": 0.0,
        "safety_cap_price": 0.0,
        "arm_price": 0.0,
        "day_sim_trail_armed": False,
        "ratchet_130_locked": False,
        "peak_price": 0.0,
        "floor_price": 0.0,
        "effective_stop_p": 0.0,
        "callback_pct": TRAIL_DEFAULT_CALLBACK_PCT,
        # 3차 매크로 방어 기준점 (실시간 QQQ 기준가 및 진입 시점 NQ=F 진폭)
        "nq_entry_price": 0.0,
        "nq_entry_amp": 0.0,
        # 3차 손절 후 최저점 역트레일링 반등(RE_BOT_TRAIL) 추적 필드
        "bottom_track_active": False,
        "stopped_sym": "",
        "last_stop_price": 0.0,
        "post_stop_low": 0.0,
        "dipped_below_stop": False,
        # 당일 데이장 [모의] 누적 성과 집계 필드
        "sim_trade_count": 0,
        "sim_win_count": 0,
        "sim_loss_count": 0,
        "sim_esc_count": 0,
        "sim_cum_net_rate_pct": 0.0,
        "sim_cum_gross_rate_pct": 0.0,
        "sim_last_exit_tag": "",
        "sim_last_exit_rate_pct": 0.0,
        "cleared_before_premarket": False,
    }


def _ensure_day_sim_caches(
    context: Any, logical_date: str
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    context.day_leader_cache 및 context.day_sim_state 의 논리적 거래일 롤오버(18:00 EST)를 점검하고
    유효한 딕셔너리 참조를 반환한다.
    """
    day_leader_cache: Dict[str, Any] = getattr(context, "day_leader_cache", {})
    if day_leader_cache.get("logical_date") != logical_date:
        hybrid_keep = bool(day_leader_cache.get("hybrid_mode", False))
        day_leader_cache.clear()
        day_leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": False,
                "leader_sym": "",
                "vwap_1907": 0.0,
                "c_1907": 0.0,
                "disp_1907": 0.0,
                "notified": False,
                "hybrid_mode": hybrid_keep,
            }
        )

    day_sim_state: Dict[str, Any] = getattr(context, "day_sim_state", {})
    if day_sim_state.get("logical_date") != logical_date:
        day_sim_state.clear()
        day_sim_state.update(create_default_day_sim_state(logical_date))

    return day_leader_cache, day_sim_state


def _get_live_day_metrics(
    context: Any,
    symbol: str,
    prices: Dict[str, float],
    now_est: datetime,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
    live_p = safe_float((prices or {}).get(sym), 0.0)
    if live_p <= 0.0 and isinstance(base_met, dict):
        live_p = safe_float(base_met.get("live_price"), 0.0)
    return refresh_metrics_with_live_tick(base_met, live_p, now_est=now_est)


# ---------------------------------------------------------------------------
# [Step 1] 19:07 EST 데이장 추세추종 주도주(👑 day_vwap_leader_sym) 선정 및 재기동 복원
# ---------------------------------------------------------------------------
async def update_daymarket_trend_leader(
    context: Any,
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1 [Step 1] & Case 77, 97:
    - 19:00:00 ~ 19:07:59 EST 구간 동안 SOXL 과 SOXS 의 데이장 초기 누적 VWAP_1907 을 산출하고,
    - 19:07:00 ~ 03:57:59 EST 구간 진입 시 C_1907 > VWAP_1907 (disp_1907 > 0.0%, 양쪽 상회 시 이격도 1위)
      종목을 당일 데이장 추세추종 주도주(_DAY_LEADER_CACHE["leader_sym"])로 불변 각인한다.
    - 데이장 도중 프로세스 재기동 시에도 19:00 ~ 19:07 EST 1분봉 벡터 스캔으로 100% 자동 복원한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)

    day_leader_cache, _ = _ensure_day_sim_caches(context, logical_date)

    if not is_daymarket_leader_lock_window(hm):
        return day_leader_cache

    if (
        day_leader_cache.get("locked")
        and str(day_leader_cache.get("leader_sym") or "") in REAL_ALLOWED_SYMBOLS
    ):
        return day_leader_cache

    soxs_met = _get_live_day_metrics(context, "SOXS", prices, dt_now)
    soxl_met = _get_live_day_metrics(context, "SOXL", prices, dt_now)

    # 기동 직후 캔들 캐시가 비어 있으면 1회 즉시 캔들 갱신 후 복원
    if (
        safe_float(soxs_met.get("vwap_1907"), 0.0) <= 0.0
        and safe_float(soxl_met.get("vwap_1907"), 0.0) <= 0.0
        and hasattr(context, "refresh_symbol_candles_once")
    ):
        try:
            await context.refresh_symbol_candles_once(("SOXS", "SOXL"))
            soxs_met = _get_live_day_metrics(context, "SOXS", prices, dt_now)
            soxl_met = _get_live_day_metrics(context, "SOXL", prices, dt_now)
        except Exception as exc:
            logger.warning("⚠️ 19:07 데이장 주도주 복원용 캔들 갱신 실패: %s", exc)

    res = select_vwap_trend_leader(soxs_met, soxl_met, session_mode="DAY")
    win_sym = str(res.get("leader_sym") or "").upper().strip()

    if win_sym in REAL_ALLOWED_SYMBOLS:
        day_leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": True,
                "leader_sym": win_sym,
                "vwap_1907": safe_float(res.get("vwap_init"), 0.0),
                "c_1907": safe_float(res.get("c_init"), 0.0),
                "disp_1907": safe_float(res.get("disp_init"), 0.0),
            }
        )
        # 03:58 ~ 03:59 묵음 구간이 아닐 때만 1회 알림
        if (
            not day_leader_cache.get("notified")
            and is_daymarket_sim_active_window(hm)
            and hasattr(context, "send_slim_alert")
        ):
            day_leader_cache["notified"] = True
            await context.send_slim_alert(
                title=f"👑 [데이장 [모의] 추세 주도주 락온] {win_sym}",
                body=(
                    f"19:07가 ${day_leader_cache['c_1907']:.2f} > VWAP_1907 ${day_leader_cache['vwap_1907']:.2f} "
                    f"(이격 +{day_leader_cache['disp_1907']:.2f}%) · 동적 VWAP -0.50% [모의] 매수덫 가동"
                ),
            )
    return day_leader_cache


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 데이장 [모의] 포지션 청산 정산 및 다회전/최저점 반등 상태 전이
# ---------------------------------------------------------------------------
async def _finalize_daymarket_sim_exit(
    context: Any,
    day_sim_state: Dict[str, Any],
    exit_price: float,
    exit_tag: str,
    nq_amp: float,
    hm: int,
    silent: bool = False,
) -> Dict[str, Any]:
    """
    데이장 [모의] 포지션 청산 시 왕복 수수료(0.14%) 차감 세후 수익률 및 승률을 실시간 집계하고,
    - 03:25 EST 이전 익절(TP_WIN) / 안전탈출(DAY_VWAP_ESC) 완료 시 다회전 재출동(IDLE) 해금
    - 3차 역행 손절(STAGE3_QQQ_STOP) 시 NQ < 2.0% 이면 최저점 +0.30% 반등 추적(BOTTOM_TRACK) 가동
    - 2차 과열 손절(STAGE2_OVERHEAT_DUMP) 시 DONE_OVERHEAT 로 마감한다.
    """
    sym = str(day_sim_state.get("symbol") or "").upper().strip()
    ent_p = safe_float(day_sim_state.get("entry_price"), 0.0)
    ex_p = safe_float(exit_price, ent_p)

    gross_rate_pct = (
        ((ex_p - ent_p) / ent_p) * 100.0 if (ent_p > 0.0 and ex_p > 0.0) else 0.0
    )
    net_rate_pct = gross_rate_pct - ROUND_TRIP_FEE_RATE_PCT

    day_sim_state["sim_trade_count"] = safe_int(day_sim_state.get("sim_trade_count"), 0) + 1
    if net_rate_pct >= 0.0:
        day_sim_state["sim_win_count"] = safe_int(day_sim_state.get("sim_win_count"), 0) + 1
    else:
        day_sim_state["sim_loss_count"] = safe_int(day_sim_state.get("sim_loss_count"), 0) + 1

    if exit_tag == "DAY_VWAP_ESC":
        day_sim_state["sim_esc_count"] = safe_int(day_sim_state.get("sim_esc_count"), 0) + 1

    day_sim_state["sim_cum_gross_rate_pct"] = round(
        safe_float(day_sim_state.get("sim_cum_gross_rate_pct"), 0.0) + gross_rate_pct,
        2,
    )
    day_sim_state["sim_cum_net_rate_pct"] = round(
        safe_float(day_sim_state.get("sim_cum_net_rate_pct"), 0.0) + net_rate_pct,
        2,
    )
    day_sim_state["sim_last_exit_tag"] = exit_tag
    day_sim_state["sim_last_exit_rate_pct"] = round(net_rate_pct, 2)

    # 보유 관련 필드 초기화
    day_sim_state.update(
        {
            "entry_price": 0.0,
            "entry_ts": 0.0,
            "safety_cap_price": 0.0,
            "arm_price": 0.0,
            "day_sim_trail_armed": False,
            "ratchet_130_locked": False,
            "peak_price": 0.0,
            "floor_price": 0.0,
            "effective_stop_p": 0.0,
            "sim_trap_buy_price": 0.0,
            "nq_entry_price": 0.0,
            "nq_entry_amp": 0.0,
        }
    )

    reentry_note = ""
    if nq_amp >= NQ_OVERHEAT_THRESHOLD or exit_tag == "STAGE2_OVERHEAT_DUMP":
        day_sim_state["phase"] = "DONE_OVERHEAT"
        day_sim_state["bottom_track_active"] = False
    elif exit_tag == "STAGE3_QQQ_STOP" and is_daymarket_trap_entry_window(hm):
        # 3차 QQQ 역행 손절 후 최저점 +0.30% 양봉 반등 [모의] 재진입(RE_BOT_TRAIL) 활성화
        day_sim_state["phase"] = "BOTTOM_TRACK"
        day_sim_state["bottom_track_active"] = True
        day_sim_state["stopped_sym"] = sym
        day_sim_state["last_stop_price"] = ex_p
        day_sim_state["post_stop_low"] = ex_p
        day_sim_state["dipped_below_stop"] = False
        reentry_note = " · 🪤최저점 +0.30% 반등 [모의] 감시"
    elif exit_tag in ("TP_WIN", "DAY_VWAP_ESC") and is_daymarket_multicycle_unlock_window(hm):
        # 03:25 EST 이전 익절/탈출 완료 시 다회전 무한 [모의] 재출동 해금
        day_sim_state["phase"] = "IDLE"
        day_sim_state["bottom_track_active"] = False
        reentry_note = " · ♻️다회전 [모의] 재출동 해금"
    else:
        day_sim_state["phase"] = "IDLE"
        day_sim_state["bottom_track_active"] = False

    tot_cnt = safe_int(day_sim_state.get("sim_trade_count"), 0)
    win_cnt = safe_int(day_sim_state.get("sim_win_count"), 0)
    win_rate = (float(win_cnt) / float(tot_cnt)) * 100.0 if tot_cnt > 0 else 0.0
    cum_net = safe_float(day_sim_state.get("sim_cum_net_rate_pct"), 0.0)

    if not silent and is_daymarket_sim_active_window(hm) and hasattr(context, "send_slim_alert"):
        sign = "+" if net_rate_pct >= 0.0 else ""
        cum_sign = "+" if cum_net >= 0.0 else ""
        icon = "🧪🎯" if net_rate_pct >= 0.0 else "🧪🛑"
        await context.send_slim_alert(
            title=f"{icon} [데이장 [모의] 청산 · {exit_tag}] {sym} ({sign}{net_rate_pct:.2f}%)",
            body=(
                f"진입 ${ent_p:.2f}➔청산 ${ex_p:.2f} · 당일 {tot_cnt}전 {win_cnt}승"
                f"(승률 {win_rate:.1f}% · 세후누적 {cum_sign}{cum_net:.2f}%){reentry_note}"
            ),
        )

    return {
        "status": "SIM_EXIT_FINALIZED",
        "symbol": sym,
        "exit_tag": exit_tag,
        "net_rate_pct": round(net_rate_pct, 2),
        "cum_net_rate_pct": cum_net,
        "win_rate_pct": round(win_rate, 1),
    }


# ---------------------------------------------------------------------------
# [핵심] 데이장 (19:00 ~ 03:57 EST) 100% [모의] 동형 스나이퍼 메인 실행기
# ---------------------------------------------------------------------------
async def execute_daymarket_sim_plugin(
    context: Any,
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1 [Step 1 ~ Step 5] & Case 77 ~ 94, 97 ~ 100:
    19:00:00 ~ 03:57:59 EST 데이장 구간에서 실거래 매수 없이 100% [모의]로
    프리장 본진과 완전히 동일한 5단계 전략을 실행한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)

    day_leader_cache, day_sim_state = _ensure_day_sim_caches(context, logical_date)

    if not is_daymarket_sim_active_window(hm):
        return {"status": "OUTSIDE_DAYMARKET_WINDOW"}

    # 19:00 ~ 19:06 EST 는 초기 7분 VWAP_1907 수집 구간
    if not is_daymarket_leader_lock_window(hm):
        return {"status": "COLLECTING_1907_VWAP"}

    # [Step 1] 19:07 EST 데이장 추세추종 주도주 선정/복원
    await update_daymarket_trend_leader(context, prices, dt_now)
    leader_sym = str(day_leader_cache.get("leader_sym") or "").upper().strip()
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    qqq_curr = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    if qqq_curr <= 0.0:
        qqq_curr = safe_float((macro or {}).get("nq_price"), 0.0)

    phase = str(day_sim_state.get("phase") or "IDLE")

    # -----------------------------------------------------------------------
    # [Step 5-A] 1·2차 매크로 과열 방어선 (NQ=F 당일 진폭 >= 2.0%)
    # -----------------------------------------------------------------------
    if nq_amp >= NQ_OVERHEAT_THRESHOLD:
        day_sim_state["bottom_track_active"] = False
        if phase == "TRAP_WAITING":
            day_sim_state["sim_trap_buy_price"] = 0.0
            day_sim_state["phase"] = "DONE_OVERHEAT"
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🧪🛑 [데이장 [모의] 1차 과열방어] {day_sim_state.get('symbol')}",
                    body=f"NQ=F 진폭 {nq_amp:.2f}% >= 2.00% 도달 · [모의] 미체결 매수덫 취소",
                )
            return {"status": "SIM_TRAP_CANCELED_OVERHEAT", "nq_amp": nq_amp}

        if phase == "HOLDING":
            sym_h = str(day_sim_state.get("symbol") or leader_sym)
            met_h = _get_live_day_metrics(context, sym_h, prices, dt_now)
            live_p = safe_float(met_h.get("live_price"), 0.0)
            ent_p = safe_float(day_sim_state.get("entry_price"), 0.0)
            # 손실권(0 < live_p < ent_p)일 때 즉시 2차 과열 [모의] 덤핑
            if 0.0 < live_p < ent_p:
                return await _finalize_daymarket_sim_exit(
                    context=context,
                    day_sim_state=day_sim_state,
                    exit_price=math.floor(live_p * 100.0) / 100.0,
                    exit_tag="STAGE2_OVERHEAT_DUMP",
                    nq_amp=nq_amp,
                    hm=hm,
                )
        elif phase != "HOLDING":
            day_sim_state["phase"] = "DONE_OVERHEAT"
            return {"status": "SIM_BLOCKED_OVERHEAT", "nq_amp": nq_amp}

    # -----------------------------------------------------------------------
    # [분기 1] 3차 역행 손절 후 최저점 +0.30% 양봉 반등 [모의] 재진입 (RE_BOT_TRAIL)
    # -----------------------------------------------------------------------
    if phase == "BOTTOM_TRACK" and day_sim_state.get("bottom_track_active"):
        if not is_daymarket_trap_entry_window(hm):
            day_sim_state["bottom_track_active"] = False
            day_sim_state["phase"] = "IDLE"
            return {"status": "SIM_BOTTOM_TRACK_EXPIRED"}

        sym_b = str(day_sim_state.get("stopped_sym") or leader_sym).upper().strip()
        met_b = _get_live_day_metrics(context, sym_b, prices, dt_now)
        cur_p = safe_float(met_b.get("live_price"), 0.0)
        if cur_p <= 0.0:
            return {"status": "NO_LIVE_PRICE"}

        last_stop_p = safe_float(day_sim_state.get("last_stop_price"), 0.0)
        post_low = safe_float(day_sim_state.get("post_stop_low"), last_stop_p)

        # 신저가 경신 중에는 칼날 회피 (매수 보류 및 최저점 갱신)
        if cur_p < post_low:
            day_sim_state["post_stop_low"] = cur_p
            if cur_p < last_stop_p:
                day_sim_state["dipped_below_stop"] = True
            return {"status": "SIM_NEW_LOW_TRACKING", "post_stop_low": cur_p}

        if post_low < last_stop_p:
            day_sim_state["dipped_below_stop"] = True

        if not day_sim_state.get("dipped_below_stop"):
            return {"status": "SIM_WAITING_DIP_BELOW_STOP"}

        rebound_p = math.ceil((post_low * 1.0030) * 100.0) / 100.0
        is_bull = bool(met_b.get("is_raw_bull") or met_b.get("is_ha_bull"))
        live_rsi = safe_float(met_b.get("live_rsi"), 50.0)
        cur_p_fl = math.floor(cur_p * 100.0) / 100.0

        if (
            cur_p_fl >= rebound_p
            and cur_p_fl <= last_stop_p
            and is_bull
            and live_rsi <= 38.0
        ):
            arm_p = math.ceil((cur_p_fl * (1.0 + TRAIL_ARM_PROFIT_PCT / 100.0)) * 100.0) / 100.0
            cap_p = math.ceil((cur_p_fl * (1.0 + SAFETY_CAP_PROFIT_PCT / 100.0)) * 100.0) / 100.0
            day_sim_state.update(
                {
                    "phase": "HOLDING",
                    "symbol": sym_b,
                    "trig_mode": "RE_BOT_TRAIL",
                    "bottom_track_active": False,
                    "entry_price": cur_p_fl,
                    "entry_ts": time.time(),
                    "safety_cap_price": cap_p,
                    "arm_price": arm_p,
                    "day_sim_trail_armed": False,
                    "ratchet_130_locked": False,
                    "peak_price": cur_p_fl,
                    "floor_price": 0.0,
                    "effective_stop_p": 0.0,
                    "nq_entry_price": qqq_curr,
                    "nq_entry_amp": nq_amp,
                }
            )
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🧪🪤 [데이장 [모의] 최저점 +0.30% 반등 재진입] {sym_b}",
                    body=(
                        f"[모의] 체결가 ${cur_p_fl:.2f} (최저점 ${post_low:.2f} · 직전손절 ${last_stop_p:.2f}) "
                        f"· RSI {live_rsi:.1f} · QQQ기준 ${qqq_curr:.2f}"
                    ),
                )
            return {"status": "SIM_RE_BOT_TRAIL_FILLED", "symbol": sym_b, "price": cur_p_fl}

        return {"status": "SIM_MONITORING_REBOUND", "symbol": sym_b}

    # -----------------------------------------------------------------------
    # [분기 2 : Step 2] 미보유(IDLE / TRAP_WAITING) 시 동적 데이장 VWAP -0.50% [모의] 매수 덫
    # -----------------------------------------------------------------------
    if phase in ("IDLE", "TRAP_WAITING"):
        if leader_sym not in REAL_ALLOWED_SYMBOLS:
            return {"status": "NO_DAY_LEADER"}

        st_leader = context.ledger.get_state(leader_sym)
        if not bool(st_leader.get("is_active", True)):
            return {"status": "BOT_INACTIVE"}

        met = _get_live_day_metrics(context, leader_sym, prices, dt_now)
        day_vwap = safe_float(met.get("day_vwap"), 0.0)
        live_p = safe_float(met.get("live_price"), 0.0)
        if day_vwap <= 0.0 or live_p <= 0.0:
            return {"status": "DAY_METRICS_NOT_READY"}

        cur_p_fl = math.floor(live_p * 100.0) / 100.0
        use_hybrid = bool(day_leader_cache.get("hybrid_mode", False))
        raw_trap_p = (
            safe_float(met.get("day_hyb_trap_m050"), 0.0)
            if use_hybrid
            else safe_float(met.get("day_dyn_trap_m050"), 0.0)
        )
        if raw_trap_p <= 0.0:
            raw_trap_p = math.floor((day_vwap * 0.9950) * 100.0) / 100.0

        target_trap_p = min(cur_p_fl, raw_trap_p)

        # 1) IDLE 상태에서 19:07 ~ 03:35 EST 구간이면 신규 [모의] 매수 덫 장전
        if phase == "IDLE":
            if not is_daymarket_trap_entry_window(hm):
                return {"status": "PAST_DAY_TRAP_ENTRY_CUTOFF"}

            day_sim_state.update(
                {
                    "phase": "TRAP_WAITING",
                    "symbol": leader_sym,
                    "trig_mode": "DAY_VWAP_M050",
                    "sim_trap_buy_price": target_trap_p,
                    "last_modify_mono": time.monotonic(),
                }
            )
            logger.info(
                "🧪🪤 [데이장 [모의] 매수덫 장전] %s @ $%.2f (데이장VWAP $%.2f, 현가 $%.2f)",
                leader_sym,
                target_trap_p,
                day_vwap,
                live_p,
            )

        # 2) TRAP_WAITING 상태에서 동적 VWAP_day_t 변화 반영 자동 추적 정정 (15초 & $0.03 괴리)
        sim_trap_p = safe_float(day_sim_state.get("sim_trap_buy_price"), 0.0)
        if is_daymarket_trap_entry_window(hm) and sim_trap_p > 0.0 and raw_trap_p > 0.0:
            elapsed_mod = time.monotonic() - safe_float(
                day_sim_state.get("last_modify_mono"), 0.0
            )
            diff_usd = abs(raw_trap_p - sim_trap_p)
            diff_pct = (diff_usd / sim_trap_p) * 100.0
            if (
                elapsed_mod >= TRAP_MODIFY_COOLDOWN_SEC
                and diff_usd >= TRAP_MODIFY_MIN_DIFF_USD
                and diff_pct >= TRAP_MODIFY_MIN_DIFF_PCT
            ):
                sim_trap_p = raw_trap_p
                day_sim_state["sim_trap_buy_price"] = sim_trap_p
                day_sim_state["last_modify_mono"] = time.monotonic()

        # 3) 현재가(cur_p_fl)가 장전된 [모의] 매수 덫 단가 이하 도달 시 즉시 [모의] 체결 확정!
        if sim_trap_p > 0.0 and cur_p_fl <= sim_trap_p:
            fill_p = min(cur_p_fl, sim_trap_p)
            arm_p = math.ceil((fill_p * (1.0 + TRAIL_ARM_PROFIT_PCT / 100.0)) * 100.0) / 100.0
            cap_p = math.ceil((fill_p * (1.0 + SAFETY_CAP_PROFIT_PCT / 100.0)) * 100.0) / 100.0

            day_sim_state.update(
                {
                    "phase": "HOLDING",
                    "symbol": leader_sym,
                    "trig_mode": "DAY_VWAP_M050",
                    "entry_price": fill_p,
                    "entry_ts": time.time(),
                    "safety_cap_price": cap_p,
                    "arm_price": arm_p,
                    "day_sim_trail_armed": False,
                    "ratchet_130_locked": False,
                    "peak_price": max(fill_p, live_p),
                    "floor_price": 0.0,
                    "effective_stop_p": 0.0,
                    "nq_entry_price": qqq_curr,
                    "nq_entry_amp": nq_amp,
                }
            )
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🧪🛡️ [데이장 [모의] VWAP -0.50% 매수 체결] {leader_sym}",
                    body=(
                        f"[모의] 체결가 ${fill_p:.2f} (동적VWAP ${day_vwap:.2f}) · "
                        f"점화가 ${arm_p:.2f}(+1.00%) · 안전캡 ${cap_p:.2f} · QQQ기준 ${qqq_curr:.2f}"
                    ),
                )
            return {
                "status": "SIM_TRAP_FILLED",
                "symbol": leader_sym,
                "fill_price": fill_p,
                "day_vwap": day_vwap,
            }

        return {
            "status": "SIM_TRAP_WAITING",
            "symbol": leader_sym,
            "trap_price": sim_trap_p,
            "live_price": live_p,
        }

    # -----------------------------------------------------------------------
    # [분기 3 : Step 3 ~ Step 5] [모의] 보유 중(HOLDING) :
    #   +5.00% 캡 & +1.00% 하드보존 HA 트레일링 & 02:45 후반부 VWAP 탈출 & 3차 QQQ 역행 손절
    # -----------------------------------------------------------------------
    if phase == "HOLDING":
        sym = str(day_sim_state.get("symbol") or leader_sym).upper().strip()
        ent_p = safe_float(day_sim_state.get("entry_price"), 0.0)
        if sym not in REAL_ALLOWED_SYMBOLS or ent_p <= 0.0:
            day_sim_state["phase"] = "IDLE"
            return {"status": "INVALID_SIM_HOLDING_RESET"}

        met = _get_live_day_metrics(context, sym, prices, dt_now)
        live_p = safe_float(met.get("live_price"), 0.0)
        day_vwap = safe_float(met.get("day_vwap"), 0.0)
        if live_p <= 0.0:
            return {"status": "NO_LIVE_PRICE"}

        arm_p = safe_float(
            day_sim_state.get("arm_price"),
            math.ceil((ent_p * (1.0 + TRAIL_ARM_PROFIT_PCT / 100.0)) * 100.0) / 100.0,
        )
        cap_p = safe_float(
            day_sim_state.get("safety_cap_price"),
            math.ceil((ent_p * (1.0 + SAFETY_CAP_PROFIT_PCT / 100.0)) * 100.0) / 100.0,
        )

        # 1) 상단 안전 캡(+5.00%) 도달 시 즉시 익절 확정
        if live_p >= cap_p:
            return await _finalize_daymarket_sim_exit(
                context=context,
                day_sim_state=day_sim_state,
                exit_price=cap_p,
                exit_tag="TP_WIN",
                nq_amp=nq_amp,
                hm=hm,
            )

        # 2) +1.00% 점화가 도달 시 하드보존 하이킨 아시 트레일링(HARD_TRAIL_100) 점화 및 판정
        if live_p >= arm_p and not day_sim_state.get("day_sim_trail_armed"):
            day_sim_state["day_sim_trail_armed"] = True
            day_sim_state["peak_price"] = max(
                safe_float(day_sim_state.get("peak_price"), live_p), live_p
            )
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🧪🔥 [데이장 [모의] +1.00% HA 트레일링 점화] {sym}",
                    body=f"현가 ${live_p:.2f} >= 점화가 ${arm_p:.2f} (진입 ${ent_p:.2f}) · 🕯️HA양봉 홀딩 감시",
                )

        if day_sim_state.get("day_sim_trail_armed"):
            peak_p = max(safe_float(day_sim_state.get("peak_price"), live_p), live_p)
            day_sim_state["peak_price"] = peak_p

            if peak_p >= ent_p * (1.0 + TRAIL_RATCHET_LOCK_PCT / 100.0):
                day_sim_state["ratchet_130_locked"] = True

            cushion_floor = (
                math.ceil((ent_p * (1.0 + TRAIL_CUSHION_FLOOR_PCT / 100.0)) * 100.0)
                / 100.0
            )
            floor_p = (
                arm_p if day_sim_state.get("ratchet_130_locked") else cushion_floor
            )
            cb_pct = safe_float(
                day_sim_state.get("callback_pct"), TRAIL_DEFAULT_CALLBACK_PCT
            )
            cb_stop_p = math.floor((peak_p * (1.0 - cb_pct / 100.0)) * 100.0) / 100.0
            eff_stop_p = max(floor_p, cb_stop_p)

            day_sim_state["floor_price"] = floor_p
            day_sim_state["effective_stop_p"] = eff_stop_p

            is_ha_bull = bool(met.get("is_ha_bull", False))
            nq_vel_bypass = bool(
                (macro or {}).get("bypass_soxs")
                if sym == "SOXS"
                else (macro or {}).get("bypass_soxl")
            )

            breached_floor = bool(live_p < floor_p)
            triggered_ha_cb = bool(
                not is_ha_bull and not nq_vel_bypass and live_p <= eff_stop_p
            )
            if breached_floor or triggered_ha_cb:
                sweep_p = max(floor_p, math.floor(live_p * 100.0) / 100.0)
                return await _finalize_daymarket_sim_exit(
                    context=context,
                    day_sim_state=day_sim_state,
                    exit_price=sweep_p,
                    exit_tag="TP_WIN",
                    nq_amp=nq_amp,
                    hm=hm,
                )

        # 3) [Step 4] 데이장 후반(02:45 ~ 03:57 EST) 미익절 표류 물량 동적 데이장 VWAP 선제 탈출(DAY_VWAP_ESC)
        else:
            ent_ts = safe_float(day_sim_state.get("entry_ts"), 0.0)
            hold_min = (time.time() - ent_ts) / 60.0 if ent_ts > 0.0 else 30.0
            if (
                is_daymarket_late_vwap_esc_window(hm)
                and hold_min >= LATE_VWAP_ESC_MIN_HOLD_MIN
                and day_vwap > 0.0
            ):
                min_fee_p = (
                    math.ceil(
                        (ent_p * (1.0 + LATE_VWAP_ESC_MIN_PROFIT_PCT / 100.0)) * 100.0
                    )
                    / 100.0
                )
                esc_target_p = max(math.floor(day_vwap * 100.0) / 100.0, min_fee_p)
                if live_p >= esc_target_p:
                    return await _finalize_daymarket_sim_exit(
                        context=context,
                        day_sim_state=day_sim_state,
                        exit_price=math.floor(live_p * 100.0) / 100.0,
                        exit_tag="DAY_VWAP_ESC",
                        nq_amp=nq_amp,
                        hm=hm,
                    )

        # 4) [Step 5-B] 3차 방향성 인지 실시간 QQQ 최소 0.80% 역행 방어 (자체 손실률 즉시 손절 금지!)
        if 0.0 < live_p < ent_p and qqq_curr > 0.0:
            adv = calc_qqq_adverse_status(
                symbol=sym,
                qqq_entry_price=safe_float(day_sim_state.get("nq_entry_price"), qqq_curr),
                qqq_curr_price=qqq_curr,
                nq_entry_amp=safe_float(day_sim_state.get("nq_entry_amp"), nq_amp),
                position_avg_price=ent_p,
                position_live_price=live_p,
            )
            if not adv.get("scale_valid"):
                day_sim_state["nq_entry_price"] = qqq_curr
            elif adv.get("should_stop"):
                return await _finalize_daymarket_sim_exit(
                    context=context,
                    day_sim_state=day_sim_state,
                    exit_price=math.floor(live_p * 100.0) / 100.0,
                    exit_tag="STAGE3_QQQ_STOP",
                    nq_amp=nq_amp,
                    hm=hm,
                )

        return {
            "status": "SIM_HOLDING_MONITORED",
            "symbol": sym,
            "entry_price": ent_p,
            "live_price": live_p,
            "armed": bool(day_sim_state.get("day_sim_trail_armed")),
        }

    return {"status": f"SIM_PHASE_{phase}"}


# ---------------------------------------------------------------------------
# [03:58 ~ 03:59 EST] 데이장 마감 휩소 묵음 바이패스 및 프리장 진입 전 자동 클리어
# ---------------------------------------------------------------------------
async def settle_daymarket_sim_before_premarket(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1 [03:58 ~ 03:59 EST 데이장 마감 휩소 묵음 바이패스 및 04:00 EST 자동 클리어]:
    - 03:58:00 EST 도달 시 데이장 [모의] 미체결 매수 덫이나 미청산 [모의] 포지션, 최저점 반등 추적을
      텔레그램 알림 없이 100% 묵음(Silent)으로 즉시 클리어하여,
      03:58 EST 1주 휩소 노출 작전과 04:00 EST 프리장 본진 스나이퍼를 보호한다.
    - 단, 당일 데이장 누적 승률/세후수익률 통계(sim_trade_count, sim_win_count, sim_cum_net_rate_pct)는
      /avwap 및 /settlement 보드 브리핑을 위해 그대로 보존한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
    _, day_sim_state = _ensure_day_sim_caches(context, logical_date)

    if day_sim_state.get("cleared_before_premarket"):
        return {"status": "ALREADY_CLEARED"}

    day_sim_state.update(
        {
            "phase": "CLEARED",
            "symbol": "",
            "trig_mode": "",
            "sim_trap_buy_price": 0.0,
            "entry_price": 0.0,
            "entry_ts": 0.0,
            "safety_cap_price": 0.0,
            "arm_price": 0.0,
            "day_sim_trail_armed": False,
            "ratchet_130_locked": False,
            "peak_price": 0.0,
            "floor_price": 0.0,
            "effective_stop_p": 0.0,
            "bottom_track_active": False,
            "stopped_sym": "",
            "last_stop_price": 0.0,
            "post_stop_low": 0.0,
            "dipped_below_stop": False,
            "cleared_before_premarket": True,
        }
    )
    logger.info("🧹 [03:58 EST 데이장 [모의] 묵음 클리어] 미결 모의 상태 초기화 완료 (통계 보존)")
    return {"status": "CLEARED_SILENTLY"}
