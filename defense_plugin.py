# =====================================================================
# FILE: defense_plugin.py
# 목적: 방향성 인지(Direction-Aware) 매크로 3단 방어망 (선택지 B [👑 HYB_35_1ST] & 초과 Case 98, 99 완결)
#       — 1·2차 대세 과열 방어(>=2.0%) 및 3차 동적 임계치 공식 max(진폭*0.5, 0.80%)의 기준은 나스닥 100 선물(NQ=F),
#         해당 3차 임계치(최소 0.80% QQQ 역행 하한 강제)를 적용하는 진입 기준가 및 실시간 역행 비교는
#         토스 Open API QQQ 0.0초 실시간 현재가(1.5초 프로빙 · 포지션 자체 손실률 기반 즉시 손절 절대 금지),
#       수동 진입(MANUAL) 및 1주 휩소 작전(WHIPSAW_1SH) 시 1·2차 과열 방어 100% 패스(04:07 정각 정리는 휩소 컷오프 전담),
#       1차 과열 미체결 매수 파기 시 buy_order_id="" 원자적 즉시 소각 및 중복 알림 영구 차단,
#       스나이퍼 재가동(OFF->ON)/미결속 시 현재 QQQ 0.00% 영점 재락온,
#       04:07 EST 이후 첫 RSI>=79.9 달성 상승 추세 종목(_PRE_LEADER_CACHE) 불변 각인 및 벡터 자가 복원,
#       04:07 ~ 08:14 EST 중 NQ<2.0% 내 3차 손절 시 최저점 역트레일링 반등 재진입(RE_BOT_TRAIL) 캐시 점화,
#       08:15 EST 거시지표 발표 15분 전 선제 컷오프,
#       15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 표류 물량 당일 정규장 VWAP 도달 즉시 덤핑 탈출(🛟 VWAP_ESCAPE) 및
#       15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산 코어
# =====================================================================
# NEW: 제3헌법 및 초과 Case 88 의거 sniper_plugin.py에서 3단 매크로 방어·08:15 컷오프·15:00 VWAP 탈출·15:59 MOC 청산 모듈을 독립 분리
# NEW: 초과 Case 85 & Case 89 & Case 99 — 이원화 하이킨 아시 트레일링 런타임 캐시(_TRAIL_RUNTIME_CACHE) 및 NQ_VELOCITY_SURGE_PCT(0.20%) 슬롯 관리
# NEW: 초과 Case 86 — 수동 진입(MANUAL) 시 첫 틱 기준점 선행 락온 및 2차 과열 방어선(NQ>=2.0%) 100% 패스
# NEW: 초과 Case 87 — 04:07 ~ 08:14 EST 중 NQ<2.0% 내 3차 역행 손절 시 당일 퇴근 대신 최저점 역트레일링(_BOTTOM_TRACK_CACHE) 점화
# NEW: 초과 Case 92 — 3차 역행 안전망의 진입 임계치 max(nq_entry_amp * 0.5, 0.80%)는 나스닥 100 선물지수(NQ=F) 진폭을 기준으로 산출하고,
#      해당 임계치를 적용하는 진입 기준가(nq_entry_price) 및 실시간 역행 비교가(ref_curr)는 토스 Open API QQQ 0.0초 실시간 현재가로 전면 대체
# MODIFIED: 제3헌법 및 초과 Case 93 — 프리장 선행 역산 버퍼(_AMP_BUFFER_CACHE) 잔재를 실전 코어에서 100% 영구 소각
# NEW: 초과 Case 94 — 포지션 자체 손실률(-0.80% 초과 평가손실 등) 기반 즉시 손절 절대 금지, 최소 0.80% 실시간 QQQ 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT) 강제,
#      스나이퍼 OFF->ON 재가동 또는 지난주 만료 주문(CANCELED) 감지 시 현재 실시간 QQQ 가격으로 0.00% 영점 재락온(_MANUAL_STAGE3_ANCHOR_CACHE) 및 양방향 수동 보유 보호
# MODIFIED: 초과 Case 95 & Case 96 — 1주 휩소 노출 작전(WHIPSAW_1SH)의 미체결 1주 매수덫 및 1주 보유 물량은 시간대와 무관하게
#           1·2차 과열 방어선(NQ>=2.0% 주문 파기 및 손실권 즉시 덤핑)을 100% 바이패스하여 04:07 정각 정리를 enforce_0407_whipsaw_cutoff_plugin에 전담 위임하고,
#           본진 미체결 매수 1차 파기 시 buy_order_id="" 즉시 소각 및 already-canceled 중복 텔레그램 알림 폭탄을 원천 차단
# NEW: 초과 Case 97 — 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 종목(_PRE_LEADER_CACHE · FIRST_799) 불변 각인·복원
# NEW: 초과 Case 98 — 정규장 장마감 1시간 전(15:00 ~ 15:58 EST)까지 +1.00% 익절하지 못한 미익절 보유 물량이
#      당일 정규장 누적 VWAP 가격에 도달하면 즉시 2단계 방탄 스윕으로 덤핑 탈출하는 execute_late_regular_vwap_escape_plugin 탑재
# NEW: 초과 Case 99 — 기술적 반등(⚡ OLD_IMM_CB) × 진성 상승종목 추세추종(🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 이원화 매도 상수 및 슬롯 연동

import math
import time
import asyncio
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
)

EST_TZ = ZoneInfo("America/New_York")

# 이원화 하이킨 아시 + 고점 트레일링(⚡ OLD_IMM_CB × 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 × 🚀 NQ_VEL_BYPASS) 상수 동기화 (Case 85, 89, 91, 99)
TRAIL_ARM_PROFIT_RATE = AssassinLedger.TRAIL_ARM_PROFIT_RATE          # +1.00% 도달 시 트레일링 점화
TRAIL_FLOOR_LOCK_RATE = AssassinLedger.TRAIL_FLOOR_LOCK_RATE          # 기본 최소 +1.00% 익절 보존선 하드 락온
TRAIL_DROP_CALLBACK_PCT = AssassinLedger.TRAIL_DROP_CALLBACK_PCT      # 기술적 반등(PRE_SNIPER) 최고가 대비 -0.30% 콜백 스탑
TRAIL_SAFETY_CAP_RATE = AssassinLedger.TRAIL_SAFETY_CAP_RATE          # 체결 직후 거래소 선장전 안전 캡 (+5.00% LIMIT)
NQ_VELOCITY_SURGE_PCT = AssassinLedger.NQ_VELOCITY_SURGE_PCT          # QQQ/NQ 10분/3분 변화율 ±0.20% HA음봉 바이패스 임계치

# 초과 Case 87 — 3차 안전망 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 상수
BOTTOM_REBOUND_PCT = 0.30          # 최저점(post_stop_low) 대비 +0.30% 이상 양봉 반등 시 재진입 요격
BOTTOM_MAX_RSI_CAP = 38.0          # 최저점 반등 진입 허용 최대 RSI 상한선 (과매도 반등권 확인)

# 스나이퍼 무음 불발 및 매크로 1차 과열 차단 알림 쿨다운 캐시 (Case 74, 96)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_macro1_date": "",
    "last_macro1_cancel_id": "",
    "last_err_msg": "",
}

# 자동 진입 세션 태그 화이트리스트 (수동 MANUAL 오분류 방어용)
_AUTO_ENTRY_SESSION_TAGS = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)


def _make_empty_trail_slot() -> dict:
    return {
        "armed": False,
        "armed_ts": 0.0,
        "armed_hm": 0,
        "exit_regime": "OLD_IMM_CB",
        "bear_candle_seen": False,
        "candle_bull": True,
        "active_callback_pct": TRAIL_DROP_CALLBACK_PCT,
        "avg_price": 0.0,
        "arm_price": 0.0,
        "floor_price": 0.0,
        "safety_cap_price": 0.0,
        "peak_price": 0.0,
        "stop_price": 0.0,
        "ha_bull": True,
        "ha_open": 0.0,
        "ha_close": 0.0,
        "nq_vel_bypass": False,
        "nq_vel_10m": 0.0,
        "nq_vel_3m": 0.0,
        "sweep_fired": False,
        "last_sweep_ts": 0.0,
    }


# 인메모리 하이킨 아시 트레일링 런타임 상태 캐시 (Case 02, 85, 89, 99)
_TRAIL_RUNTIME_CACHE = {
    "SOXS": _make_empty_trail_slot(),
    "SOXL": _make_empty_trail_slot(),
}

# 초과 Case 94 — 수동 보유(MANUAL) 물량의 스나이퍼 OFF->ON 재가동 및 지난주 만료 주문 감지 시
# 현재 실시간 QQQ 가격으로 0.00% 영점 재락온을 보장하기 위한 인메모리 앵커 검증 캐시
_MANUAL_STAGE3_ANCHOR_CACHE = {
    "SOXS": {"anchored": False, "verified_sell_id": ""},
    "SOXL": {"anchored": False, "verified_sell_id": ""},
}

# 인메모리 3차 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 추적 캐시 (Case 02, 87)
_BOTTOM_TRACK_CACHE = {
    "active": False,
    "mode": "STAGE3_STOP",
    "symbol": "",
    "date": "",
    "last_stop_price": 0.0,
    "post_stop_low": 0.0,
    "rebound_trig_price": 0.0,
    "dipped_below_stop": False,
    "climax_seen": True,
    "activated_ts": 0.0,
    "last_new_low_ts": 0.0,
}

# 초과 Case 97 — 선택지 B (👑 HYB_35_1ST) : 04:07 EST 이후 가장 먼저 RSI >= 79.9 를 달성한
# 당일 첫 상승 추세 종목(FIRST_799 · leader_sym) 인메모리 추세 원장 캐시
_PRE_LEADER_CACHE = {
    "date": "",
    "leader_sym": "",
    "leader_rsi": 0.0,
    "locked_ts": 0.0,
    "locked_time_str": "",
    "pb_rearm_ready": True,
}


def _reset_pre_leader_state(logical_date: str = ""):
    """프리장 상승 추세 주도주 캐시(_PRE_LEADER_CACHE)를 원자적으로 0점 초기화합니다 (Case 02, 22, 48, 97)."""
    _PRE_LEADER_CACHE["date"] = str(logical_date or "")
    _PRE_LEADER_CACHE["leader_sym"] = ""
    _PRE_LEADER_CACHE["leader_rsi"] = 0.0
    _PRE_LEADER_CACHE["locked_ts"] = 0.0
    _PRE_LEADER_CACHE["locked_time_str"] = ""
    _PRE_LEADER_CACHE["pb_rearm_ready"] = True


def mark_pre_pullback_dispatched(symbol: str):
    """07:00 ~ 08:14 EST 선행 상승종목 추세눌림목(RSI <= 35.0) 매수 덫 발사 시 동일 눌림목 중복 진입을 잠급니다 (초과 Case 97)."""
    if _PRE_LEADER_CACHE.get("leader_sym") == symbol:
        _PRE_LEADER_CACHE["pb_rearm_ready"] = False


def get_pre_leader_status() -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 메인 루프에서 당일 프리장 첫 상승 추세 종목 상태를 조회합니다 (초과 Case 97)."""
    return dict(_PRE_LEADER_CACHE)


def update_premarket_trend_leader(
    symbol: str,
    live_rsi_val: float,
    now_est: datetime = None,
    shared_rsi_state: dict = None,
) -> dict:
    """
    초과 Case 97 — 04:07 ~ 08:14 EST 구간에서 당일 가장 먼저 RSI >= 79.9 를 달성한 첫 번째 종목을
    당일 프리장의 상승 추세 주도주(_PRE_LEADER_CACHE["leader_sym"])로 불변 각인하며,
    장중 프로세스 재기동 시에도 shared_rsi_state(200봉 벡터 스캔 결과 first_0407_799_ts)를 대조하여 100% 자동 복원합니다.
    """
    if symbol not in ("SOXS", "SOXL"):
        return dict(_PRE_LEADER_CACHE)

    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if 400 <= est_hm <= 814 and _PRE_LEADER_CACHE.get("date") != today_str:
        _reset_pre_leader_state(logical_date=today_str)

    if not (407 <= est_hm <= 814):
        return dict(_PRE_LEADER_CACHE)

    cur_leader = str(_PRE_LEADER_CACHE.get("leader_sym", ""))

    # 1) 장중 재기동 시 200봉 1분봉 벡터 스캔 타임스탬프(first_0407_799_ts) 비교로 첫 상승 추세 종목 자동 복원
    if not cur_leader and isinstance(shared_rsi_state, dict):
        ewm_s = ((shared_rsi_state.get("SOXS") or {}).get("ewm_state")) or {}
        ewm_l = ((shared_rsi_state.get("SOXL") or {}).get("ewm_state")) or {}
        ts_s = float(ewm_s.get("first_0407_799_ts", 0.0)) if ewm_s.get("logical_date") == today_str else 0.0
        ts_l = float(ewm_l.get("first_0407_799_ts", 0.0)) if ewm_l.get("logical_date") == today_str else 0.0

        if ts_s > 0.0 and (ts_l <= 0.0 or ts_s <= ts_l):
            _PRE_LEADER_CACHE["leader_sym"] = "SOXS"
            _PRE_LEADER_CACHE["leader_rsi"] = float(ewm_s.get("first_0407_799_rsi", PRE_TREND_LEADER_SELECT_RSI))
            _PRE_LEADER_CACHE["locked_ts"] = ts_s
            _PRE_LEADER_CACHE["locked_time_str"] = str(ewm_s.get("first_0407_799_time_str", now_est.strftime("%H:%M:%S")))
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
            cur_leader = "SOXS"
            print(
                f"👑 [프리장 첫 상승추세 종목 벡터 복원 SOXS] 시각: {_PRE_LEADER_CACHE['locked_time_str']} EST | "
                f"피크 RSI: {_PRE_LEADER_CACHE['leader_rsi']:.1f} ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 감시 대기",
                flush=True,
            )
        elif ts_l > 0.0 and (ts_s <= 0.0 or ts_l < ts_s):
            _PRE_LEADER_CACHE["leader_sym"] = "SOXL"
            _PRE_LEADER_CACHE["leader_rsi"] = float(ewm_l.get("first_0407_799_rsi", PRE_TREND_LEADER_SELECT_RSI))
            _PRE_LEADER_CACHE["locked_ts"] = ts_l
            _PRE_LEADER_CACHE["locked_time_str"] = str(ewm_l.get("first_0407_799_time_str", now_est.strftime("%H:%M:%S")))
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
            cur_leader = "SOXL"
            print(
                f"👑 [프리장 첫 상승추세 종목 벡터 복원 SOXL] 시각: {_PRE_LEADER_CACHE['locked_time_str']} EST | "
                f"피크 RSI: {_PRE_LEADER_CACHE['leader_rsi']:.1f} ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 감시 대기",
                flush=True,
            )

    # 2) 실시간 틱 RSI >= 79.9 최초 도달 종목 불변 각인 (FIRST_799)
    if not cur_leader and live_rsi_val >= PRE_TREND_LEADER_SELECT_RSI:
        _PRE_LEADER_CACHE["leader_sym"] = symbol
        _PRE_LEADER_CACHE["leader_rsi"] = float(live_rsi_val)
        _PRE_LEADER_CACHE["locked_ts"] = time.time()
        _PRE_LEADER_CACHE["locked_time_str"] = now_est.strftime("%H:%M:%S")
        _PRE_LEADER_CACHE["pb_rearm_ready"] = True
        print(
            f"👑 [프리장 첫 상승추세 종목 각인 {symbol}] {now_est.strftime('%H:%M:%S')} EST | "
            f"RSI {live_rsi_val:.1f} (≥{PRE_TREND_LEADER_SELECT_RSI:.1f}) ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 타겟 락온",
            flush=True,
        )
    elif cur_leader == symbol:
        if live_rsi_val > float(_PRE_LEADER_CACHE.get("leader_rsi", 0.0)):
            _PRE_LEADER_CACHE["leader_rsi"] = float(live_rsi_val)
        if not bool(_PRE_LEADER_CACHE.get("pb_rearm_ready", True)) and live_rsi_val > PRE_TREND_REARM_RESET_RSI:
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True

    return dict(_PRE_LEADER_CACHE)


def _reset_manual_stage3_anchor(symbol: str = None):
    """수동 포지션 3차 방어 QQQ 기준가 영점 앵커 캐시를 초기화합니다 (초과 Case 94)."""
    if symbol and symbol in _MANUAL_STAGE3_ANCHOR_CACHE:
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {"anchored": False, "verified_sell_id": ""}
    else:
        for sym in ("SOXS", "SOXL"):
            _MANUAL_STAGE3_ANCHOR_CACHE[sym] = {"anchored": False, "verified_sell_id": ""}


def mark_manual_stage3_anchored(symbol: str, sell_order_id: str = ""):
    """수동 포지션의 상단 안전 캡(+5.00% LIMIT) 장전 및 실시간 QQQ 기준가 락온 완료를 기록합니다 (초과 Case 94)."""
    if symbol in _MANUAL_STAGE3_ANCHOR_CACHE:
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
            "anchored": True,
            "verified_sell_id": str(sell_order_id or ""),
        }


def _reset_trailing_runtime_state(symbol: str = None):
    """하이킨 아시 트레일링 인메모리 캐시 및 수동 3차 방어 앵커 캐시를 원자적으로 초기화합니다 (Case 02, 22, 85, 94)."""
    _reset_manual_stage3_anchor(symbol)
    if symbol and symbol in _TRAIL_RUNTIME_CACHE:
        _TRAIL_RUNTIME_CACHE[symbol] = _make_empty_trail_slot()
    else:
        for sym in ("SOXS", "SOXL"):
            _TRAIL_RUNTIME_CACHE[sym] = _make_empty_trail_slot()


def get_trailing_runtime_status(symbol: str) -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 정산 모듈에서 트레일링 상태를 조회합니다."""
    slot = _TRAIL_RUNTIME_CACHE.get(str(symbol or "").upper()) or _make_empty_trail_slot()
    return dict(slot)


def _reset_bottom_track_state(symbol: str = None):
    """
    3차 손절 후 최저점 역트레일링 캐시(_BOTTOM_TRACK_CACHE)를 원자적으로 초기화합니다 (Case 02, 22, 87).
    """
    if symbol and _BOTTOM_TRACK_CACHE.get("symbol") not in ("", symbol):
        return
    _BOTTOM_TRACK_CACHE["active"] = False
    _BOTTOM_TRACK_CACHE["mode"] = "STAGE3_STOP"
    _BOTTOM_TRACK_CACHE["symbol"] = ""
    _BOTTOM_TRACK_CACHE["date"] = ""
    _BOTTOM_TRACK_CACHE["last_stop_price"] = 0.0
    _BOTTOM_TRACK_CACHE["post_stop_low"] = 0.0
    _BOTTOM_TRACK_CACHE["rebound_trig_price"] = 0.0
    _BOTTOM_TRACK_CACHE["dipped_below_stop"] = False
    _BOTTOM_TRACK_CACHE["climax_seen"] = True
    _BOTTOM_TRACK_CACHE["activated_ts"] = 0.0
    _BOTTOM_TRACK_CACHE["last_new_low_ts"] = 0.0


def _activate_bottom_track_state(
    symbol: str,
    stop_price: float,
    current_price: float = 0.0,
    logical_date: str = "",
):
    """
    04:07 ~ 08:14 EST 골든타임 내 NQ=F < 2.0% 상태에서 3차 역행 손절 발생 시
    최저점 역트레일링 반등 감시(_BOTTOM_TRACK_CACHE)를 점화합니다 (초과 Case 87).
    """
    if symbol not in ("SOXS", "SOXL") or stop_price <= 0.0:
        return
    sp_fl = math.floor(float(stop_price) * 100.0) / 100.0
    cp_fl = math.floor(float(current_price) * 100.0) / 100.0 if current_price > 0.0 else sp_fl
    init_low = min(sp_fl, cp_fl) if cp_fl > 0.0 else sp_fl
    trig_p = math.ceil(init_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    today_s = logical_date or datetime.now(EST_TZ).strftime("%Y-%m-%d")

    _BOTTOM_TRACK_CACHE["active"] = True
    _BOTTOM_TRACK_CACHE["mode"] = "STAGE3_STOP"
    _BOTTOM_TRACK_CACHE["symbol"] = symbol
    _BOTTOM_TRACK_CACHE["date"] = today_s
    _BOTTOM_TRACK_CACHE["last_stop_price"] = sp_fl
    _BOTTOM_TRACK_CACHE["post_stop_low"] = init_low
    _BOTTOM_TRACK_CACHE["rebound_trig_price"] = trig_p
    _BOTTOM_TRACK_CACHE["dipped_below_stop"] = bool(init_low < sp_fl)
    _BOTTOM_TRACK_CACHE["climax_seen"] = True
    _BOTTOM_TRACK_CACHE["activated_ts"] = time.time()
    _BOTTOM_TRACK_CACHE["last_new_low_ts"] = time.time()


def get_bottom_track_status(symbol: str = None) -> dict:
    """
    UI 관제탑(/avwap, /sync, /settlement) 및 main.py 0.4초 부스터 루프에서
    3차 손절 후 최저점 역트레일링 추적(STAGE3_STOP) 상태를 조회합니다 (초과 Case 87, 93).
    """
    if _BOTTOM_TRACK_CACHE.get("active", False):
        if not symbol or _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
            res = dict(_BOTTOM_TRACK_CACHE)
            res["mode"] = "STAGE3_STOP"
            return res

    return {
        "active": False,
        "mode": "NONE",
        "symbol": symbol or "",
        "date": "",
        "last_stop_price": 0.0,
        "post_stop_low": 0.0,
        "rebound_trig_price": 0.0,
        "dipped_below_stop": False,
    }


# Case 24: MOC, 매크로 덤핑, 15:00 VWAP 탈출 스윕, 트레일링 스윕 및 04:07 휩소 정리 시 미체결 주문을 선취소하고
#          최대 2.0초(0.25초 × 8회) 동안 매도 가능 수량(sellableQuantity) 해방을 폴링 대기하는 방탄 헬퍼
async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    sell_order_id: str = "",
    cancel_all_sides: bool = True,
) -> tuple[int, list[str]]:
    canceled_ids = set()
    if sell_order_id:
        try:
            await ctx.client.cancel_order(sell_order_id)
            canceled_ids.add(sell_order_id)
        except Exception:
            pass

    try:
        open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
        for o in open_orders:
            oid = str(o.get("orderId") or "")
            side = str(o.get("side") or "")
            if not oid or oid in canceled_ids:
                continue
            if cancel_all_sides or side == "SELL":
                try:
                    await ctx.client.cancel_order(oid)
                    canceled_ids.add(oid)
                except Exception:
                    pass
    except Exception:
        pass

    sellable_q_int = 0
    for _ in range(8):
        try:
            sq = await ctx.client.get_sellable_quantity(symbol)
            sellable_q_int = int(math.floor(float(sq or 0.0)))
            if sellable_q_int > 0:
                break
        except Exception:
            pass
        await asyncio.sleep(0.25)

    surviving_sell_ids: list[str] = []
    if sellable_q_int <= 0:
        try:
            open_after = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for o in open_after:
                if o.get("side") == "SELL" and o.get("orderId"):
                    surviving_sell_ids.append(str(o["orderId"]))
        except Exception:
            pass
        if not surviving_sell_ids and sell_order_id:
            surviving_sell_ids.append(sell_order_id)

    return sellable_q_int, surviving_sell_ids


# NEW: 초과 Case 98 — 정규장 장마감 1시간 전(15:00:00 ~ 15:58:59 EST)까지 +1.00% 익절하지 못한
#      미익절 표류 물량이 당일 정규장 누적 VWAP 가격에 도달하면 즉시 덤핑 탈출하는 2단계 방탄 플러그인 (🛟 VWAP_ESCAPE)
async def execute_late_regular_vwap_escape_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    holdings_detail: dict = None,
    avg_price_dump: float = 0.0,
    last_buy_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    runtime_state: dict = None,
    current_price: float = 0.0,
    vwap_val: float = 0.0,
    **kwargs,
) -> bool:
    if runtime_state is None:
        runtime_state = {}

    est_hm = now_est.hour * 100 + now_est.minute
    if symbol not in ("SOXS", "SOXL") or not is_active or not (1500 <= est_hm <= 1558):
        return False

    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))
    if holdings_qty <= 0 or ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    # 이미 +1.00% 돌파로 하이킨 아시 트레일링이 점화(armed == True)된 수익 포지션은 트레일링 엔진에 100% 위임!
    if _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False):
        return False

    avg_p = (
        float(avg_price_dump or 0.0)
        or (float(holdings_detail.get("avg_price", 0.0)) if isinstance(holdings_detail, dict) else 0.0)
        or float(last_buy_price or 0.0)
    )

    live_p = current_price
    if live_p <= 0.0:
        try:
            live_p = await ctx.client.get_current_price(symbol)
        except Exception:
            live_p = 0.0
    if live_p <= 0.0:
        return False

    # 현재가가 이미 +1.00% 트레일링 점화가(arm_p) 이상에 도달한 경우 즉시 하단 트레일링 엔진이 점화하도록 바이패스
    if avg_p > 0.0:
        arm_p = math.ceil(avg_p * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        if live_p >= arm_p:
            return False

    # 당일 정규장 누적 VWAP 조회 (reg_vwap 최우선, 폴백 vwap_val)
    eff_vwap = float(vwap_val or 0.0)
    if ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
        sym_st = ctx.shared_rsi_state.get(symbol) or {}
        ewm_st = sym_st.get("ewm_state") or {}
        reg_vwap = float(ewm_st.get("reg_vwap", 0.0))
        if reg_vwap > 0.0:
            eff_vwap = reg_vwap
        elif eff_vwap <= 0.0:
            eff_vwap = float(sym_st.get("vwap", 0.0))

    vwap_target_p = math.floor(eff_vwap * 100.0) / 100.0 if eff_vwap > 0.0 else 0.0
    if vwap_target_p <= 0.0:
        return False

    # [핵심 조건] 실시간 현재가(live_p)가 당일 정규장 VWAP 가격(vwap_target_p)에 도달했는지 판별!
    if live_p < vwap_target_p:
        return False

    now_ts = time.time()
    last_vwap_ts = float(runtime_state.get("last_vwap_escape_tick", 0.0))
    if now_ts - last_vwap_ts < 1.5:
        return False

    ctx.in_memory_ordering_lock[symbol] = True
    runtime_state["last_vwap_escape_tick"] = now_ts
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else vwap_target_p

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = vwap_target_p

        dispatched = False
        new_s_id = ""
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"VWAP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_s_id = str((res or {}).get("result", {}).get("orderId") or "")
            dispatched = True
        elif surviving_sell_ids:
            new_s_id = surviving_sell_ids[0]
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=new_s_id,
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            dispatched = True

        if dispatched:
            _reset_trailing_runtime_state(symbol)
            _reset_bottom_track_state()
            _reset_pre_leader_state()
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=new_s_id,
                target_sell_price=bid_price,
                is_session_done=True,
                entry_session="VWAP_ESCAPE",
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            if not runtime_state.get("vwap_escape_alert_sent", False):
                runtime_state["vwap_escape_alert_sent"] = True
                pnl_pct_est = ((bid_price / avg_p - 1.0) * 100.0) if avg_p > 0.0 else 0.0
                avg_str = f"${avg_p:.2f}" if avg_p > 0.0 else "집계중"
                await ctx.notify_tg(
                    f"🛟 <b>[장마감 1시간 전 VWAP 탈출 스윕] {sym_icon}{symbol} 미익절 물량 VWAP 도달 즉시 덤핑</b>\n"
                    f"▫️ 발동 사유: 정규장 마감 1시간 전(<code>15:00 ~ 15:58 EST</code>) 미익절 표류 중 당일 정규장 VWAP(<code>${vwap_target_p:.2f}</code>) 도달!\n"
                    f"▫️ 탈출 단가(Bid 1 LIMIT): 평단 <code>{avg_str}</code> ➡️ 현재 <code>${live_p:.2f}</code> / 처분가 <code>${bid_price:.2f}</code> "
                    f"(<code>{holdings_qty}주</code> · 예상 <code>{pnl_pct_est:+.2f}%</code> · 15:59 MOC 대량 손절 선제 회피)"
                )
            return True
    except Exception as e:
        print(f"🚨 [장마감 1시간 전 VWAP 탈출 스윕 방어 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return False


# Case 16 & Case 24: 15:59 ~ 16:01 EST 절대 제로-오버나이트 MOC 2단계 방탄 강제 청산 플러그인 (main.py 호출 시그니처 100% 호환)
async def execute_moc_zero_overnight_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int = None,
    buy_order_id: str = None,
    sell_order_id: str = "",
    runtime_state: dict = None,
    current_price: float = 0.0,
    **kwargs,
) -> bool:
    if runtime_state is None:
        runtime_state = {}

    is_moc_window = (
        (now_est.hour == 15 and now_est.minute >= 59)
        or (now_est.hour == 16 and 0 <= now_est.minute <= 1)
    )
    if not is_moc_window or not is_active:
        return False

    _reset_bottom_track_state()
    _reset_pre_leader_state()
    _reset_trailing_runtime_state(symbol)

    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))
    if buy_order_id is None:
        buy_order_id = await AssassinLedger.get_buy_order_id(symbol)

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1) 미체결 매수 주문 잔존 시 최우선 선취소
    if buy_order_id:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            await ctx.client.cancel_order(buy_order_id)
        except Exception:
            pass
        finally:
            await AssassinLedger.save_state(
                symbol, buy_order_id="", is_session_done=True
            )
            ctx.in_memory_ordering_lock[symbol] = False

    if holdings_qty <= 0:
        return True

    now_ts = time.time()
    last_moc_ts = float(runtime_state.get("last_moc_dump_tick", runtime_state.get("last_moc_tick", 0.0)))
    if now_ts - last_moc_ts < 1.5:
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    runtime_state["last_moc_dump_tick"] = now_ts
    runtime_state["last_moc_tick"] = now_ts
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = current_price
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else 0.01

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = 0.01

        dispatched = False
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"MOC_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_s_id = str((res or {}).get("result", {}).get("orderId") or "")
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=new_s_id,
                target_sell_price=bid_price,
                is_session_done=True,
            )
            dispatched = True
        elif surviving_sell_ids:
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=surviving_sell_ids[0],
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=surviving_sell_ids[0],
                target_sell_price=bid_price,
                is_session_done=True,
            )
            dispatched = True

        if dispatched and not runtime_state.get("moc_alert_sent", False):
            runtime_state["moc_alert_sent"] = True
            runtime_state["moc_dump_active"] = True
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🛑 <b>[제로-오버나이트 MOC 발동] {sym_icon}{symbol} 장마감 강제 청산 스윕</b>\n"
                f"▫️ 보유 수량: <code>{holdings_qty}주</code> ➡️ 매수 1호가(LIMIT): <code>${bid_price:.2f}</code> 즉시 투하"
            )
    except Exception as e:
        ctx.idempotency_keys[symbol]["MOC"] = None
        print(f"🚨 [MOC 강제 청산 방어 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return True


# MODIFIED: 초과 Case 66, 67, 82, 86, 87, 92, 93, 94, 95, 96, 97, 98, 99 —
#           1) 포지션 자체의 현재 평가손실률(예: 지난주 보유분 -3% ~ -10% 손실 등)을 3차 임계치에 바로 적용하여 즉시 손절하는 것을 100% 금지!
#           2) 1·2차 대세 과열 방어(>=2.0%) 및 3차 동적 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)의 기준은 나스닥 100 선물(NQ=F)이되,
#              최소 0.80% 실시간 QQQ 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80)을 하드 락온!
#           3) 스나이퍼 OFF->ON 재가동 시 또는 수동 물량 미결속(not sell_order_id 또는 만료된 구버전 sell_order_id) 감지 첫 틱에
#              과거 nq_entry_price를 폐기하고 현재 실시간 QQQ 가격(ref_curr_man)으로 0.00% 영점 재조준!
#           4) 1주 휩소 노출 작전(WHIPSAW_1SH)은 시간대와 무관하게 1·2차 과열 방어선(주문 취소 및 손실권 즉시 덤핑)을 100% 바이패스하여
#              04:07 정각 정리를 enforce_0407_whipsaw_cutoff_plugin에 전담 위임하고,
#              본진 미체결 매수 1차 파기 시 buy_order_id="" 즉시 소각 및 중복 알림 폭탄을 원천 차단!
async def execute_macro_defense_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str = None,
    is_active: bool = True,
    is_session_done: bool = False,
    holdings_qty: int = 0,
    holdings_detail: dict = None,
    avg_price_dump: float = 0.0,
    last_buy_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    nq_entry_price: float = 0.0,
    nq_entry_amp: float = 0.0,
    runtime_state: dict = None,
    current_price: float = 0.0,
    **kwargs,
) -> bool:
    if symbol not in ("SOXS", "SOXL"):
        return False

    if holdings_qty <= 0 or not is_active:
        _reset_manual_stage3_anchor(symbol)
        if not is_active:
            return False

    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1. 1·2차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준인 나스닥 100 선물지수(NQ=F) 조회
    nq_curr, nq_high, nq_low = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if nq_low > 0.0 else 0.0

    # MODIFIED: 초과 Case 95 & Case 96 — 1주 휩소 노출 작전(WHIPSAW_1SH)은 시간대(03:58~04:06 및 04:07+ 정리 시점 포함)와 무관하게
    # execute_macro_defense_plugin의 1·2차 과열 방어선을 100% 패스하고 04:07 정각 정리는 enforce_0407_whipsaw_cutoff_plugin이 전담!
    is_whipsaw_1sh_active = (entry_session == "WHIPSAW_1SH")

    # =====================================================================
    # [선행 처리] 수동 진입(MANUAL) 포지션 첫 감지 틱 및 재가동(OFF->ON) 시
    #            실시간 QQQ 기준가 0.00% 영점 재락온 및 NQ=F 진폭 동기화 (초과 Case 50, 86, 92, 94, 97, 98, 99)
    # =====================================================================
    is_untagged_manual_hold = (
        holdings_qty > 0
        and not buy_order_id
        and entry_session not in _AUTO_ENTRY_SESSION_TAGS
    )
    if is_untagged_manual_hold:
        ref_curr_man, ref_src_man = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
        anchor_slot = _MANUAL_STAGE3_ANCHOR_CACHE.setdefault(
            symbol, {"anchored": False, "verified_sell_id": ""}
        )

        # 초과 Case 94 — 장부에 지난주/전세션의 만료된 sell_order_id가 남아있는 경우 실제 유효 여부를 1회 검증하여
        # CANCELED / REJECTED / 404 이면 즉시 폐기하고 현재 실시간 QQQ 가격으로 0.00% 영점 재조준!
        effective_sell_id = str(sell_order_id or "")
        if effective_sell_id and anchor_slot.get("verified_sell_id") != effective_sell_id:
            try:
                od_chk = await ctx.client.get_order_detail(effective_sell_id)
                st_chk = str(od_chk.get("status") or "")
                if st_chk in ("CANCELED", "REJECTED"):
                    effective_sell_id = ""
                else:
                    anchor_slot["verified_sell_id"] = effective_sell_id
                    anchor_slot["anchored"] = True
            except Exception as e_chk:
                if "404" in str(e_chk).lower() or "not-found" in str(e_chk).lower():
                    effective_sell_id = ""

        # 안전 캡 매도 주문(effective_sell_id)이 아직 장전되지 않았거나(신규 수동 매수 / 스나이퍼 OFF->ON 직후 / 지난주 주문 만료),
        # 기준가가 없거나 스케일이 불일치하면 무조건 현재 실시간 QQQ 가격(ref_curr_man)으로 0.00% 영점 재락온!
        need_fresh_reanchor = bool(
            not effective_sell_id
            or not anchor_slot.get("anchored", False)
            or nq_entry_price <= 0.0
            or (
                nq_entry_price > 0.0
                and ref_curr_man > 0.0
                and ((nq_entry_price > 3000.0) != (ref_curr_man > 3000.0))
            )
        )

        need_man_save = False
        save_kwargs = {}

        if effective_sell_id != sell_order_id:
            sell_order_id = ""
            save_kwargs["sell_order_id"] = ""
            save_kwargs["target_sell_price"] = 0.0
            need_man_save = True

        if entry_session != "MANUAL":
            entry_session = "MANUAL"
            save_kwargs["entry_session"] = "MANUAL"
            save_kwargs["entry_trend"] = "SHORT" if symbol == "SOXS" else "LONG"
            save_kwargs["is_session_done"] = True
            need_man_save = True

        if need_fresh_reanchor and ref_curr_man > 0.0:
            nq_entry_price = ref_curr_man
            save_kwargs["nq_entry_price"] = nq_entry_price
            need_man_save = True
            if nq_amp > 0.0:
                nq_entry_amp = nq_amp
                save_kwargs["nq_entry_amp"] = nq_entry_amp
            anchor_slot["anchored"] = True
            anchor_slot["verified_sell_id"] = effective_sell_id
            print(
                f"🛡️ [수동 보유 3차 안전망 0.00% 영점 락온 {symbol}] 기준({ref_src_man}): {nq_entry_price:.2f} "
                f"| NQ진폭: {nq_entry_amp:.2f}% | 최소 역행 한도: +{max(nq_entry_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT):.2f}%",
                flush=True,
            )
        elif nq_entry_amp <= 0.0 and nq_amp > 0.0:
            nq_entry_amp = nq_amp
            save_kwargs["nq_entry_amp"] = nq_entry_amp
            need_man_save = True

        if need_man_save:
            await AssassinLedger.save_state(symbol, **save_kwargs)

    # =====================================================================
    # [1차 방어선] NQ=F 당일 진폭 >= 2.0% 대세 과열 시 본진 미체결 매수 즉시 파기 및 본진 신규 진입 잠금
    #             (단, 초과 Case 95 & 96 의거 1주 휩소 작전 WHIPSAW_1SH 매수덫은 절대 파기하지 않고 04:07 컷오프에 위임하며,
    #              본진 미체결 주문 파기 시에는 buy_order_id=""를 원자적으로 즉시 소각하고 중복 알림을 차단!)
    # =====================================================================
    if nq_amp >= 2.0:
        if _BOTTOM_TRACK_CACHE.get("active", False):
            _reset_bottom_track_state()
        if _PRE_LEADER_CACHE.get("leader_sym"):
            _reset_pre_leader_state()

        if buy_order_id and not is_whipsaw_1sh_active and not ctx.in_memory_ordering_lock.get(symbol, False):
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                already_canceled = False
                try:
                    await ctx.client.cancel_order(buy_order_id)
                except Exception as cancel_e:
                    err_low = str(cancel_e).lower()
                    if "already-filled" in err_low:
                        return False
                    if "already-canceled" in err_low or "404" in err_low or "order-not-found" in err_low:
                        already_canceled = True

                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id="",
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                ctx.wakeup_event.set()

                if not already_canceled and _STRIKE_ALERT_CACHE.get("last_macro1_cancel_id") != buy_order_id:
                    _STRIKE_ALERT_CACHE["last_macro1_cancel_id"] = buy_order_id
                    await ctx.notify_tg(
                        f"🛑 <b>[매크로 1차 방어선 발동] {sym_icon}{symbol} 미체결 매수 주문 즉시 파기</b>\n"
                        f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp:.2f}%</code> (2.0% 대세 과열 돌파)\n"
                        "▫️ 조치: 미체결 매수 취소 완료 및 금일 양 종목 본진 자동 매수 권한 영구 소각"
                    )
                return True
            finally:
                ctx.in_memory_ordering_lock[symbol] = False

        if holdings_qty <= 0 and not buy_order_id:
            if entry_session not in ("CANCELED_MACRO", "WHIPSAW_1SH"):
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
            if (407 <= est_hm <= 814) and _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[매크로 1차 방어선 작동] 나스닥 100 선물(NQ=F) 대세 과열 감지</b>\n"
                    f"▫️ 현재 NQ=F 진폭: <code>{nq_amp:.2f}%</code> (기준 ≥ 2.00%)\n"
                    "▫️ 조치: 금일 SOXS·SOXL 본진 자동 신규 진입 잠금 (수동 진입 시 2차 과열방어 패스 · 3차 QQQ 실시간 최소 0.80% 역행방어 가동)"
                )
            return False

    # =====================================================================
    # [2차 & 3차 방어선] 보유 포지션(holdings_qty > 0) 손실 구간 방어 프로빙 (초과 Case 66, 67, 86, 87, 92, 94, 95, 96, 97, 98)
    # =====================================================================
    if holdings_qty <= 0 or ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    avg_p = (
        float(avg_price_dump or 0.0)
        or (float(holdings_detail.get("avg_price", 0.0)) if isinstance(holdings_detail, dict) else 0.0)
        or float(last_buy_price or 0.0)
    )
    if avg_p <= 0.0:
        return False

    live_p = current_price
    if live_p <= 0.0:
        try:
            live_p = await ctx.client.get_current_price(symbol)
        except Exception:
            live_p = 0.0

    # 수익권(live_p >= avg_p)이거나 이미 하이킨 아시 트레일링이 점화(armed)된 상태에서는 익절 트레일링 엔진이 전담
    # 주의(초과 Case 94): live_p < avg_p 는 단지 손실 구간 전제 조건일 뿐, 포지션 자체 손실률((avg_p - live_p)/avg_p)로 손절하지 않는다!
    if live_p <= 0.0 or live_p >= avg_p:
        return False
    if _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False):
        return False

    # 초과 Case 92 & Case 94 —
    # 1) 동적 임계치 max(eff_entry_amp * 0.5, 0.80%)는 나스닥 100 선물(NQ=F) 진폭을 기준으로 산출하되 최소 0.80% 하한 강제!
    # 2) 산출된 임계치를 적용할 진입 기준가(nq_entry_price) 및 실시간 비교가(ref_curr)는
    #    토스 Open API QQQ 0.0초 실시간 현재가(1.5초 주기 실시간 프로빙)로 비교!
    ref_curr, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)

    # 스케일 불일치 자가 치유 가드 (기존 NQ=F 지수 스케일 > 3000.0 <-> QQQ 스케일 < 3000.0 전환 시 자동 재동기화)
    if nq_entry_price <= 0.0 and ref_curr > 0.0:
        nq_entry_price = ref_curr
        await AssassinLedger.save_state(symbol, nq_entry_price=nq_entry_price)
    elif nq_entry_price > 0.0 and ref_curr > 0.0 and ((nq_entry_price > 3000.0) != (ref_curr > 3000.0)):
        print(
            f"🔄 [3차 방어 기준점 스케일 자가 치유 {symbol}] 기존 {nq_entry_price:.2f} -> 신규({ref_src}) {ref_curr:.2f}",
            flush=True,
        )
        nq_entry_price = ref_curr
        await AssassinLedger.save_state(symbol, nq_entry_price=nq_entry_price)

    eff_entry_amp = nq_entry_amp if nq_entry_amp > 0.0 else nq_amp
    dynamic_threshold = max(
        eff_entry_amp * 0.5,
        float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
        MIN_STAGE3_QQQ_ADVERSE_PCT,
    )

    if nq_entry_price > 0.0 and ref_curr > 0.0:
        if symbol == "SOXS":
            nq_adverse_pct = (ref_curr - nq_entry_price) / nq_entry_price * 100.0
            adverse_dir_str = "상승(숏 역행)"
        else:
            nq_adverse_pct = (nq_entry_price - ref_curr) / nq_entry_price * 100.0
            adverse_dir_str = "하락(롱 역행)"
    else:
        nq_adverse_pct = 0.0
        adverse_dir_str = "역행"

    # 초과 Case 86, 94, 95, 96, 97, 98, 99: 수동 진입(MANUAL) 포지션과 1주 휩소 작전(WHIPSAW_1SH) 포지션은
    # 2차 과열 방어선(NQ=F >= 2.0%)을 100% 패스(Bypass)하며,
    # 오직 진입/락온 시점 QQQ 기준가 대비 실시간 QQQ가 최소 0.80%(dynamic_threshold) 이상 역행했을 때만 3차 손절 격발!
    is_manual_pos = (
        entry_session == "MANUAL"
        or is_untagged_manual_hold
        or (
            nq_entry_amp >= 2.0
            and entry_session not in _AUTO_ENTRY_SESSION_TAGS
        )
    )
    effective_stage2_breach = (
        (nq_amp >= 2.0)
        and (not is_manual_pos)
        and (entry_session not in ("WHIPSAW_1SH", "VWAP_ESCAPE"))
    )
    is_stage3_breach = (
        nq_entry_price > 0.0
        and ref_curr > 0.0
        and nq_adverse_pct >= dynamic_threshold
        and nq_adverse_pct >= MIN_STAGE3_QQQ_ADVERSE_PCT
    )

    if not effective_stage2_breach and not is_stage3_breach:
        return False

    now_ts = time.time()
    if runtime_state is not None:
        if now_ts - float(runtime_state.get("last_macro_dump_tick", 0.0)) < 1.5:
            return False
        runtime_state["last_macro_dump_tick"] = now_ts

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else 0.01

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = 0.01

        dump_dispatched = False
        new_sell_id = ""
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"MDMP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_sell_id = str((res or {}).get("result", {}).get("orderId") or "")
            dump_dispatched = True
        elif surviving_sell_ids:
            new_sell_id = surviving_sell_ids[0]
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=new_sell_id,
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            dump_dispatched = True

        if dump_dispatched:
            _reset_trailing_runtime_state(symbol)
            can_bottom_track = (
                not effective_stage2_breach
                and is_stage3_breach
                and (407 <= est_hm <= 814)
                and (nq_amp < 2.0)
            )
            ref_unit = "$" if ref_src == "QQQ실시간" else ""

            if can_bottom_track:
                # 초과 Case 87: 04:07 ~ 08:14 EST 골든타임 내 NQ=F < 2.0% 상태에서 3차 역행 손절 시
                #               당일 퇴근하지 않고 최저점 역트레일링 반등 감시(STAGE3_STOP_TRACK) 점화!
                _activate_bottom_track_state(
                    symbol=symbol,
                    stop_price=bid_price,
                    current_price=min(live_p, bid_price),
                    logical_date=today_str,
                )
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_sell_id,
                    target_sell_price=bid_price,
                    is_session_done=True,
                    entry_session="STAGE3_STOP_TRACK",
                    whipsaw_target_price=0.0,
                )
                ctx.wakeup_event.set()
                trig_rebound_p = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
                await ctx.notify_tg(
                    f"🚨 <b>[매크로 3차 실시간 역행 손절 ➡️ 🪤최저점 반등 대기] {sym_icon}{symbol} 칼손절 스윕 발사</b>\n"
                    f"▫️ 역행 사유({ref_src}): 진입 기준 <code>{ref_unit}{nq_entry_price:.2f}</code> ➡️ 현재 <code>{ref_unit}{ref_curr:.2f}</code> "
                    f"(<code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_str} · NQ진폭 <code>{eff_entry_amp:.2f}%</code> 기반 한도 <code>+{dynamic_threshold:.2f}%</code> 돌파)\n"
                    f"▫️ 손절 단가(Bid 1 LIMIT): 평단 <code>${avg_p:.2f}</code> ➡️ <code>${bid_price:.2f}</code> (<code>{holdings_qty}주</code>)\n"
                    f"▫️ 🪤 최저점 역트레일링 점화: <code>손절 체결 즉시 최저점 추적 개시 (초기 반등목표 +{BOTTOM_REBOUND_PCT:.2f}% = ${trig_rebound_p:.2f} · 08:14 EST까지 재출격 대기)</code>"
                )
            else:
                _reset_bottom_track_state()
                _reset_pre_leader_state()
                # 초과 Case 94 ③ — force_all=False를 전달하여 반대편 종목이 수동 보유(MANUAL) 중일 때
                # 반대편 종목의 MANUAL 표식과 QQQ 기준가가 소각되지 않도록 100% 보호!
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_sell_id,
                    target_sell_price=bid_price,
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    whipsaw_target_price=0.0,
                )
                ctx.wakeup_event.set()
                if effective_stage2_breach:
                    reason_msg = f"2차 대세장 과열 방어 (NQ=F 당일 진폭 <code>{nq_amp:.2f}%</code> ≥ 2.00%)"
                else:
                    mode_tag_s = "수동 3차 역행 방어" if is_manual_pos else "3차 역행 방어"
                    reason_msg = (
                        f"{mode_tag_s} ({ref_src} 기준 <code>{ref_unit}{nq_entry_price:.2f}</code> ➡️ 현재 <code>{ref_unit}{ref_curr:.2f}</code> · "
                        f"<code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_str} / NQ진폭 기반 한도 <code>+{dynamic_threshold:.2f}%</code>)"
                    )
                await ctx.notify_tg(
                    f"🛑 <b>[매크로 방어선 손절 덤핑 &amp; 당일 마감] {sym_icon}{symbol} 매수 1호가 즉시 스윕</b>\n"
                    f"▫️ 발동 사유: {reason_msg}\n"
                    f"▫️ 처분 단가(Bid 1 LIMIT): 평단 <code>${avg_p:.2f}</code> ➡️ <code>${bid_price:.2f}</code> (<code>{holdings_qty}주</code>)\n"
                    "▫️ 후속 조치: 금일 자동 신규 진입 권한 영구 소각 완료"
                )
            return True
    except Exception as e:
        ctx.idempotency_keys[symbol]["M_DUMP"] = None
        print(f"🚨 [매크로 방어 덤핑 에러 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return False


# 초과 Case 48, 84 & 97: 08:15:00 EST 거시지표 발표 15분 전 선제 컷오프
# (08:15 ~ 15:58 EST 전 구간 신규 매수 봉쇄, 최저점 추적 캐시 및 상승 주도주 캐시 파기, 미체결 매수 즉시 선취소)
async def enforce_0930_regular_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime = None,
    est_time_int: int = None,
    is_active: bool = True,
    is_session_done: bool = False,
    holdings_qty: int = None,
    buy_order_id: str = "",
    **kwargs,
) -> tuple[bool, str]:
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = int(est_time_int) if est_time_int is not None else (now_est.hour * 100 + now_est.minute)
    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))

    if not is_active or not (815 <= est_hm <= 1559):
        return is_session_done, buy_order_id

    # 08:15 EST 진입 즉시 최저점 역트레일링 추적 캐시 및 프리장 상승 주도주 캐시 파기 (Case 48, 84, 97)
    if _BOTTOM_TRACK_CACHE.get("active", False):
        _reset_bottom_track_state()
    if _PRE_LEADER_CACHE.get("leader_sym"):
        _reset_pre_leader_state()

    if buy_order_id and not ctx.in_memory_ordering_lock.get(symbol, False):
        ctx.in_memory_ordering_lock[symbol] = True
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"
        try:
            canceled_ok = False
            try:
                await ctx.client.cancel_order(buy_order_id)
                canceled_ok = True
            except Exception as ce:
                if "already-filled" in str(ce):
                    return is_session_done, buy_order_id

            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                for o in open_orders:
                    if o.get("side") == "BUY" and o.get("orderId"):
                        try:
                            await ctx.client.cancel_order(str(o["orderId"]))
                            canceled_ok = True
                        except Exception:
                            pass
            except Exception:
                pass

            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                is_session_done=True,
                entry_session="",
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            if canceled_ok:
                await ctx.notify_tg(
                    f"🔒 <b>[08:15 거시지표 선제 컷오프] {sym_icon}{symbol} 미체결 매수 주문 즉시 파기</b>\n"
                    "▫️ 사유: 08:15 EST 거시경제 지표 발표 15분 전 리스크 방어 컷오프 도달\n"
                    "▫️ 조치: 미체결 매수 주문 선취소 및 금일 신규 자동 진입 마감"
                )
            return True, ""
        except Exception as e:
            print(f"🚨 [08:15 컷오프 매수 취소 방어 {symbol}] {html.escape(str(e))}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return is_session_done, buy_order_id

    if not is_session_done and holdings_qty <= 0:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        return True, buy_order_id

    return is_session_done, buy_order_id


# 하위 호환 별칭 유지 (Case 84, 88)
enforce_0815_pre_macro_cutoff_plugin = enforce_0930_regular_cutoff_plugin
