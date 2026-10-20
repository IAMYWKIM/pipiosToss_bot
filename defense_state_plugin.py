# =====================================================================
# FILE: defense_state_plugin.py
# 목적: 인메모리 트레일링 캐시(_TRAIL_RUNTIME_CACHE), 3차 손절 후 최저점 역트레일링 캐시(_BOTTOM_TRACK_CACHE),
#       04:07 EST 이후 첫 RSI>=79.9 상승 주도주 캐시(_PRE_LEADER_CACHE),
#       옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 스윙·꺾임목 캐시(_SWING_10M_CACHE),
#       수동 진입 3차 역행 앵커 캐시 및 2단계 방탄 수량 해방 폴링 헬퍼 코어 플러그인
#       (defense_plugin.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 방어·트레일링·추세주도주 상태 관리 코어를 defense_state_plugin.py로 분리
# MODIFIED: 초과 Case 85, 87, 94, 97 — 트레일링, 최저점 역트레일링, 프리장 상승 주도주(FIRST_799), 수동 3차 기준가 앵커 상태 관리
# NEW: 초과 Case 101 — 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 스윙 추적(_SWING_10M_CACHE) 및
#      직전 10분봉 시가/고가 돌파 꺾임목 판별기(evaluate_premarket_10m_pivot_breakout) 탑재

import math
import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    PRE_PIVOT_10M_MIN_SWING_AMP_PCT,
    PRE_PIVOT_10M_MAX_TROUGH_RSI,
    PRE_PIVOT_10M_REBOUND_MIN_PCT,
    PRE_PIVOT_10M_REBOUND_MAX_PCT,
    PRE_PIVOT_10M_MIN_BODY_PCT,
    compute_realtime_tick_heikin_ashi,
    compute_realtime_tick_10m_state,
)

EST_TZ = ZoneInfo("America/New_York")

# 초과 Case 85, 87, 89, 94, 99 — 트레일링 및 매크로 방어 상수
TRAIL_ARM_PROFIT_RATE = 1.00            # +1.00% 도달 시 트레일링 점화
TRAIL_FLOOR_LOCK_RATE = 1.00            # 최소 익절 보존 하한선 (+1.00%)
TRAIL_DROP_CALLBACK_PCT = 0.30          # 기술적 반등(OLD_IMM_CB) 최고가 대비 콜백 스탑폭 (-0.30%)
TRAIL_SAFETY_CAP_RATE = 5.00            # 상단 안전 캡 지정가 (+5.00% LIMIT)
NQ_VELOCITY_SURGE_PCT = 0.20            # 토스 QQQ 10m/3m 변화율 ±0.20% 급등락 시 HA음봉 바이패스 임계치

BOTTOM_REBOUND_PCT = 0.30               # 3차 손절 후 최저점 대비 +0.30% 양봉 반등 재진입 폭
BOTTOM_MAX_RSI_CAP = 38.0               # 최저점 반등 재진입 허용 최대 RSI (<= 38.0)

# 초과 Case 41, 86, 99, 101 — 자동 진입 세션 태그 화이트리스트 (수동 MANUAL 판별용)
_AUTO_ENTRY_SESSION_TAGS = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_PIVOT_10M",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "PRE_PIVOT_10M_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)

# 초과 Case 74 & 96 — 스나이퍼 타격 보류 및 매크로 1차 중복 알림 방지 캐시
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_err_msg": "",
    "last_macro1_date": "",
    "last_macro1_cancel_id": "",
    "last_vwap_esc_date": "",
}


def _default_trail_symbol_state() -> dict:
    return {
        "armed": False,
        "peak_price": 0.0,
        "stop_price": 0.0,
        "floor_price": 0.0,
        "armed_time": 0.0,
        "armed_hm": -1,
        "bear_candle_seen": False,
        "nq_vel_bypass": False,
        "exit_regime": "OLD_IMM_CB",
        "last_sweep_ts": 0.0,
    }


_TRAIL_RUNTIME_CACHE = {
    "SOXS": _default_trail_symbol_state(),
    "SOXL": _default_trail_symbol_state(),
}

_BOTTOM_TRACK_CACHE = {
    "active": False,
    "date": "",
    "symbol": "",
    "last_stop_price": 0.0,
    "post_stop_low": 0.0,
    "rebound_trig_price": 0.0,
    "dipped_below_stop": False,
    "climax_seen": True,
    "activated_ts": 0.0,
    "last_new_low_ts": 0.0,
}

_PRE_LEADER_CACHE = {
    "date": "",
    "leader_sym": "",
    "leader_rsi": 0.0,
    "locked_ts": 0.0,
    "locked_time_str": "",
    "pb_rearm_ready": True,
    "pb_dispatch_count": 0,
}


def _default_swing_10m_symbol_state() -> dict:
    return {
        "swing_peak": 0.0,
        "swing_trough": 0.0,
        "swing_min_rsi": 100.0,
        "swing_drop_pct": 0.0,
        "rebound_pct": 0.0,
        "made_new_low": False,
        "pivot_dispatched": False,
        "pivot_ready": False,
    }


# NEW: 초과 Case 101 — 프리장 본진 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 스윙 추적 캐시
_SWING_10M_CACHE = {
    "date": "",
    "SOXS": _default_swing_10m_symbol_state(),
    "SOXL": _default_swing_10m_symbol_state(),
}

# 초과 Case 86 & 94 — 수동 보유(MANUAL) 물량 스나이퍼 가동 ON 시 첫 틱 실시간 QQQ 기준가 앵커 캐시
_MANUAL_STAGE3_ANCHOR_CACHE = {
    "SOXS": {"anchored": False, "ref_price": 0.0, "ref_amp": 0.0, "ts": 0.0},
    "SOXL": {"anchored": False, "ref_price": 0.0, "ref_amp": 0.0, "ts": 0.0},
}


def _reset_manual_stage3_anchor(symbol: str = None):
    if symbol in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
            "anchored": False,
            "ref_price": 0.0,
            "ref_amp": 0.0,
            "ts": 0.0,
        }
    else:
        for s in ("SOXS", "SOXL"):
            _MANUAL_STAGE3_ANCHOR_CACHE[s] = {
                "anchored": False,
                "ref_price": 0.0,
                "ref_amp": 0.0,
                "ts": 0.0,
            }


def mark_manual_stage3_anchored(symbol: str, ref_price: float = 0.0, ref_amp: float = 0.0):
    if symbol in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
            "anchored": True,
            "ref_price": float(ref_price or 0.0),
            "ref_amp": float(ref_amp or 0.0),
            "ts": time.time(),
        }


def is_manual_stage3_anchored(symbol: str) -> bool:
    if symbol not in ("SOXS", "SOXL"):
        return False
    return bool(_MANUAL_STAGE3_ANCHOR_CACHE.get(symbol, {}).get("anchored", False))


def _reset_trailing_runtime_state(symbol: str = None):
    if symbol in ("SOXS", "SOXL"):
        _TRAIL_RUNTIME_CACHE[symbol] = _default_trail_symbol_state()
    else:
        for s in ("SOXS", "SOXL"):
            _TRAIL_RUNTIME_CACHE[s] = _default_trail_symbol_state()


def get_trailing_runtime_status(symbol: str) -> dict:
    if symbol not in ("SOXS", "SOXL"):
        return _default_trail_symbol_state()
    return dict(_TRAIL_RUNTIME_CACHE.get(symbol) or _default_trail_symbol_state())


def _reset_bottom_track_state():
    _BOTTOM_TRACK_CACHE.update({
        "active": False,
        "date": "",
        "symbol": "",
        "last_stop_price": 0.0,
        "post_stop_low": 0.0,
        "rebound_trig_price": 0.0,
        "dipped_below_stop": False,
        "climax_seen": True,
        "activated_ts": 0.0,
        "last_new_low_ts": 0.0,
    })


def _activate_bottom_track_state(symbol: str, stop_price: float, now_est: datetime = None):
    if symbol not in ("SOXS", "SOXL") or stop_price <= 0.0:
        return
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    sp_fl = math.floor(float(stop_price) * 100.0) / 100.0
    trig_p = math.ceil(sp_fl * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    _BOTTOM_TRACK_CACHE.update({
        "active": True,
        "date": now_est.strftime("%Y-%m-%d"),
        "symbol": symbol,
        "last_stop_price": sp_fl,
        "post_stop_low": sp_fl,
        "rebound_trig_price": trig_p,
        "dipped_below_stop": False,
        "climax_seen": True,
        "activated_ts": time.time(),
        "last_new_low_ts": time.time(),
    })


def get_bottom_track_status(symbol: str = None) -> dict:
    st = dict(_BOTTOM_TRACK_CACHE)
    if symbol is not None and symbol in ("SOXS", "SOXL"):
        if not (st.get("active", False) and st.get("symbol") == symbol):
            st["active"] = False
    return st


def _reset_pre_leader_state():
    _PRE_LEADER_CACHE.update({
        "date": "",
        "leader_sym": "",
        "leader_rsi": 0.0,
        "locked_ts": 0.0,
        "locked_time_str": "",
        "pb_rearm_ready": True,
        "pb_dispatch_count": 0,
    })


def mark_pre_pullback_dispatched(symbol: str):
    if _PRE_LEADER_CACHE.get("leader_sym") == symbol:
        _PRE_LEADER_CACHE["pb_rearm_ready"] = False
        _PRE_LEADER_CACHE["pb_dispatch_count"] = int(_PRE_LEADER_CACHE.get("pb_dispatch_count", 0)) + 1


def get_pre_leader_status() -> dict:
    return dict(_PRE_LEADER_CACHE)


def update_premarket_trend_leader(
    symbol: str,
    live_rsi_val: float,
    now_est: datetime = None,
    shared_rsi_state: dict = None,
) -> dict:
    """
    초과 Case 97:
    04:07 ~ 08:14 EST 구간에서 당일 04:07 EST 이후 가장 먼저 RSI >= 79.9를 달성한 종목을
    당일 프리장의 진성 상승 추세 주도주(leader_sym)로 불변 각인하며, 장중 재기동 시에도
    200봉 벡터 연산의 first_799_ts를 비교하여 자동 복원합니다.
    """
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if _PRE_LEADER_CACHE.get("date") != today_str:
        _reset_pre_leader_state()
        _PRE_LEADER_CACHE["date"] = today_str

    if not (407 <= est_hm <= 814):
        return dict(_PRE_LEADER_CACHE)

    # 1) 장중 재기동 시 200봉 벡터 스캔(first_799_ts) 기반 선행 상승 주도주 자동 복원
    if not _PRE_LEADER_CACHE.get("leader_sym") and isinstance(shared_rsi_state, dict):
        ewm_s = (shared_rsi_state.get("SOXS") or {}).get("ewm_state") or {}
        ewm_l = (shared_rsi_state.get("SOXL") or {}).get("ewm_state") or {}
        ts_s = float(ewm_s.get("first_799_ts", 0.0) or 0.0)
        ts_l = float(ewm_l.get("first_799_ts", 0.0) or 0.0)

        if ts_s > 0.0 and ts_l > 0.0:
            win_sym = "SOXS" if ts_s <= ts_l else "SOXL"
            win_ewm = ewm_s if win_sym == "SOXS" else ewm_l
        elif ts_s > 0.0:
            win_sym = "SOXS"
            win_ewm = ewm_s
        elif ts_l > 0.0:
            win_sym = "SOXL"
            win_ewm = ewm_l
        else:
            win_sym = ""
            win_ewm = {}

        if win_sym:
            _PRE_LEADER_CACHE.update({
                "leader_sym": win_sym,
                "leader_rsi": float(win_ewm.get("first_799_rsi", PRE_TREND_LEADER_SELECT_RSI) or PRE_TREND_LEADER_SELECT_RSI),
                "locked_ts": float(win_ewm.get("first_799_ts", time.time())),
                "locked_time_str": str(win_ewm.get("first_799_time_str") or now_est.strftime("%H:%M:%S")),
                "pb_rearm_ready": True,
            })

    # 2) 실시간 틱 RSI >= 79.9 최초 도달 시 불변 각인 (FIRST_799)
    if symbol in ("SOXS", "SOXL") and live_rsi_val >= PRE_TREND_LEADER_SELECT_RSI:
        if not _PRE_LEADER_CACHE.get("leader_sym"):
            _PRE_LEADER_CACHE.update({
                "leader_sym": symbol,
                "leader_rsi": round(float(live_rsi_val), 1),
                "locked_ts": time.time(),
                "locked_time_str": now_est.strftime("%H:%M:%S"),
                "pb_rearm_ready": True,
            })
        elif _PRE_LEADER_CACHE.get("leader_sym") == symbol:
            if live_rsi_val > float(_PRE_LEADER_CACHE.get("leader_rsi", 0.0)):
                _PRE_LEADER_CACHE["leader_rsi"] = round(float(live_rsi_val), 1)

    # 3) 직전 추세눌림목 출격 후 RSI > 40.0 회복 시 재장전 (pb_rearm_ready = True)
    if (
        _PRE_LEADER_CACHE.get("leader_sym") == symbol
        and not bool(_PRE_LEADER_CACHE.get("pb_rearm_ready", True))
        and live_rsi_val > PRE_TREND_REARM_RESET_RSI
    ):
        _PRE_LEADER_CACHE["pb_rearm_ready"] = True

    return dict(_PRE_LEADER_CACHE)


# NEW: 초과 Case 101 — 프리장 본진 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 스윙·꺾임목 상태 관리 함수군
def _reset_swing_10m_state():
    _SWING_10M_CACHE["date"] = ""
    _SWING_10M_CACHE["SOXS"] = _default_swing_10m_symbol_state()
    _SWING_10M_CACHE["SOXL"] = _default_swing_10m_symbol_state()


def mark_pre_pivot_10m_dispatched(symbol: str):
    if symbol in ("SOXS", "SOXL"):
        st = _SWING_10M_CACHE.get(symbol)
        if isinstance(st, dict):
            st["pivot_dispatched"] = True
            st["pivot_ready"] = False


def get_swing_10m_status(symbol: str = None) -> dict:
    if symbol in ("SOXS", "SOXL"):
        return dict(_SWING_10M_CACHE.get(symbol) or _default_swing_10m_symbol_state())
    return {
        "date": _SWING_10M_CACHE.get("date", ""),
        "SOXS": dict(_SWING_10M_CACHE.get("SOXS") or _default_swing_10m_symbol_state()),
        "SOXL": dict(_SWING_10M_CACHE.get("SOXL") or _default_swing_10m_symbol_state()),
    }


def update_premarket_swing_10m_wave(
    symbol: str,
    live_price: float,
    live_rsi: float,
    ewm_state: dict = None,
    now_est: datetime = None,
) -> dict:
    """
    NEW: 초과 Case 101 — 04:00 ~ 08:14 EST 동안 SOXS와 SOXL의 스윙 고점(swing_peak) -> 스윙 저점(swing_trough)
    하락 진폭(swing_drop_pct)과 파동 바닥 경유 최저 RSI(swing_min_rsi)를 실시간 추적합니다.
    """
    if symbol not in ("SOXS", "SOXL") or live_price <= 0.0:
        return get_swing_10m_status(symbol)

    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if _SWING_10M_CACHE.get("date") != today_str:
        _reset_swing_10m_state()
        _SWING_10M_CACHE["date"] = today_str

    if not (400 <= est_hm <= 814):
        return get_swing_10m_status(symbol)

    st = _SWING_10M_CACHE[symbol]
    ewm = ewm_state if isinstance(ewm_state, dict) else {}

    # 장중 재기동 시 캔들 벡터 연산에서 추출한 당일 세션 스윙 파동 기저로 즉시 시드 복원
    if float(st.get("swing_peak", 0.0)) <= 0.0:
        seed_peak = float(ewm.get("sess_swing_peak", 0.0) or 0.0) or live_price
        seed_trough = float(ewm.get("sess_swing_trough", 0.0) or 0.0) or live_price
        seed_min_rsi = float(ewm.get("sess_swing_min_rsi", 100.0) or 100.0)
        if live_rsi > 0.0:
            seed_min_rsi = min(seed_min_rsi, live_rsi)
        st["swing_peak"] = seed_peak
        st["swing_trough"] = min(seed_trough, live_price)
        st["swing_min_rsi"] = seed_min_rsi

    cur_bar_h = max(float(ewm.get("cur_bar_high", 0.0) or 0.0), live_price)
    cur_bar_l_raw = float(ewm.get("cur_bar_low", 0.0) or 0.0)
    cur_bar_l = min(cur_bar_l_raw, live_price) if cur_bar_l_raw > 0.0 else live_price

    prev_peak = float(st.get("swing_peak", live_price))
    prev_trough = float(st.get("swing_trough", live_price))

    made_new_low = False
    if cur_bar_h >= prev_peak:
        st["swing_peak"] = cur_bar_h
        st["swing_trough"] = cur_bar_l
        st["swing_min_rsi"] = live_rsi if live_rsi > 0.0 else 50.0
        st["pivot_dispatched"] = False
    else:
        if cur_bar_l <= prev_trough:
            st["swing_trough"] = cur_bar_l
            made_new_low = True
        if 0.0 < live_rsi < float(st.get("swing_min_rsi", 100.0)):
            st["swing_min_rsi"] = round(live_rsi, 2)

        trough_now = float(st.get("swing_trough", cur_bar_l))
        # 바닥 대비 +1.50% 이상 이미 반등 완료 시 해당 하락 파동 소멸 처리 및 새 파동 고점 리셋
        if trough_now > 0.0 and ((live_price - trough_now) / trough_now * 100.0) >= 1.50:
            st["swing_peak"] = cur_bar_h
            st["swing_trough"] = cur_bar_l
            st["swing_min_rsi"] = live_rsi if live_rsi > 0.0 else 50.0
            st["pivot_dispatched"] = False

    pk_f = float(st.get("swing_peak", live_price))
    tr_f = float(st.get("swing_trough", live_price))
    drop_pct = ((pk_f - tr_f) / tr_f * 100.0) if tr_f > 0.0 else 0.0
    reb_pct = ((live_price - tr_f) / tr_f * 100.0) if tr_f > 0.0 else 0.0

    st["swing_drop_pct"] = round(drop_pct, 2)
    st["rebound_pct"] = round(reb_pct, 2)
    st["made_new_low"] = made_new_low
    st["pivot_ready"] = bool(
        not st.get("pivot_dispatched", False)
        and drop_pct >= PRE_PIVOT_10M_MIN_SWING_AMP_PCT
        and float(st.get("swing_min_rsi", 100.0)) <= PRE_PIVOT_10M_MAX_TROUGH_RSI
    )
    return dict(st)


def evaluate_premarket_10m_pivot_breakout(
    symbol: str,
    live_price: float,
    live_rsi: float,
    opp_live_price: float,
    opp_live_rsi: float,
    shared_rsi_state: dict,
    now_est: datetime = None,
) -> tuple[bool, dict]:
    """
    NEW: 초과 Case 101 — 프리장 본진 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 돌파 꺾임목(PRE_PIVOT_10M) 판별기.
    조건:
    1) 당일 선행 10분봉 스윙 하락 진폭 swing_drop_pct >= 2.40% (NQ ~0.57%, 횡보장 100% 차단)
    2) 스윙 바닥 경유 swing_min_rsi <= 38.0 및 현재 틱 신저가 멈춤 (not made_new_low)
    3) 스윙 최저점 대비 +0.45% <= rebound_pct <= +1.30% 반등 & 반대종목 최고점 대비 -0.38% 이상 하락 & 현재 RSI <= 48.0
    4) 매수종목 10분봉 양봉 몸통(>= +0.20%) 또는 아랫꼬리 망치(>= +0.20% & 양봉) & 현재 1분봉 양봉 &
       반대종목 10분봉 파란 음봉 전환 & 직전 10분봉 시가/고가 상향 돌파(또는 반대종목 직전 10분봉 저가 하향 돌파)
    """
    if symbol not in ("SOXS", "SOXL") or live_price <= 0.0 or not isinstance(shared_rsi_state, dict):
        return False, {}

    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return False, {}

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    my_dict = shared_rsi_state.get(symbol) or {}
    opp_dict = shared_rsi_state.get(opp_sym) or {}
    my_ewm = my_dict.get("ewm_state") or {}
    opp_ewm = opp_dict.get("ewm_state") or {}

    my_wave = update_premarket_swing_10m_wave(
        symbol=symbol,
        live_price=live_price,
        live_rsi=live_rsi,
        ewm_state=my_ewm,
        now_est=now_est,
    )
    if opp_live_price > 0.0:
        opp_wave = update_premarket_swing_10m_wave(
            symbol=opp_sym,
            live_price=opp_live_price,
            live_rsi=opp_live_rsi,
            ewm_state=opp_ewm,
            now_est=now_est,
        )
    else:
        opp_wave = get_swing_10m_status(opp_sym)

    my_10m = compute_realtime_tick_10m_state(live_price=live_price, ewm_state=my_ewm, now_est=now_est)
    opp_p_eff = opp_live_price if opp_live_price > 0.0 else float(opp_ewm.get("prev_close", 0.0) or 0.0)
    opp_10m = compute_realtime_tick_10m_state(live_price=opp_p_eff, ewm_state=opp_ewm, now_est=now_est)

    swing_drop_pct = float(my_wave.get("swing_drop_pct", 0.0))
    swing_min_rsi = float(my_wave.get("swing_min_rsi", 100.0))
    rebound_pct = float(my_wave.get("rebound_pct", 0.0))
    made_new_low = bool(my_wave.get("made_new_low", False))
    pivot_dispatched = bool(my_wave.get("pivot_dispatched", False))

    opp_peak = float(opp_wave.get("swing_peak", 0.0))
    opp_drop_from_peak_pct = (
        ((opp_peak - opp_p_eff) / opp_peak * 100.0)
        if (opp_peak > 0.0 and opp_p_eff > 0.0)
        else 0.0
    )

    cur_bar_open = float(my_ewm.get("cur_bar_open", 0.0) or live_price)
    is_ha_bull, _, _ = compute_realtime_tick_heikin_ashi(live_price, my_ewm)
    is_1m_bull = bool((live_price > cur_bar_open) or is_ha_bull)

    body_10m = float(my_10m.get("body_10m_pct", 0.0))
    wick_10m = float(my_10m.get("wick_low_10m_pct", 0.0))
    o_10m = float(my_10m.get("o_10m", live_price))

    opp_body_10m = float(opp_10m.get("body_10m_pct", 0.0))
    opp_o_10m = float(opp_10m.get("o_10m", opp_p_eff))

    is_10m_bull_shape = bool(
        (body_10m >= PRE_PIVOT_10M_MIN_BODY_PCT)
        or (wick_10m >= PRE_PIVOT_10M_MIN_BODY_PCT and live_price >= o_10m)
    )
    is_opp_10m_bear_shape = bool(
        (opp_body_10m <= -PRE_PIVOT_10M_MIN_BODY_PCT)
        or (opp_p_eff > 0.0 and opp_p_eff < opp_o_10m)
    )
    is_prev10m_break = bool(
        my_10m.get("broke_prev10m_open", False)
        or my_10m.get("broke_prev10m_high", False)
        or opp_10m.get("broke_prev10m_low", False)
    )

    is_pivot_ok = bool(
        not pivot_dispatched
        and not made_new_low
        and (swing_drop_pct >= PRE_PIVOT_10M_MIN_SWING_AMP_PCT)
        and (swing_min_rsi <= PRE_PIVOT_10M_MAX_TROUGH_RSI)
        and (PRE_PIVOT_10M_REBOUND_MIN_PCT <= rebound_pct <= PRE_PIVOT_10M_REBOUND_MAX_PCT)
        and (opp_drop_from_peak_pct >= PRE_PIVOT_10M_REBOUND_MIN_PCT * 0.85)
        and (0.0 < live_rsi <= 48.0)
        and is_1m_bull
        and is_10m_bull_shape
        and is_opp_10m_bear_shape
        and is_prev10m_break
    )

    info = {
        "swing_peak": float(my_wave.get("swing_peak", 0.0)),
        "swing_trough": float(my_wave.get("swing_trough", 0.0)),
        "swing_drop_pct": swing_drop_pct,
        "swing_min_rsi": swing_min_rsi,
        "rebound_pct": rebound_pct,
        "opp_drop_pct": round(opp_drop_from_peak_pct, 2),
        "body_10m_pct": round(body_10m, 2),
        "opp_body_10m_pct": round(opp_body_10m, 2),
        "prev_o_10m": float(my_10m.get("prev_o_10m", 0.0)),
        "prev_h_10m": float(my_10m.get("prev_h_10m", 0.0)),
    }
    return is_pivot_ok, info


async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    sell_order_id: str = "",
    cancel_all_sides: bool = False,
) -> tuple[int, list[str]]:
    """
    Case 24:
    미체결 매도(또는 매수/매도 전체) 주문을 선취소하고 최대 2.0초(0.25초 * 8회) 동안
    매도 가능 수량(sellableQuantity > 0) 해방을 폴링 대기합니다.
    취소 병목 시 modify_order 즉시 가격 정정 스윕에 사용할 생존 매도 주문 ID 리스트(surviving_sell_ids)를 함께 반환합니다.
    """
    canceled_ids = set()
    surviving_sell_ids: list[str] = []

    if sell_order_id:
        try:
            await ctx.client.cancel_order(sell_order_id)
            canceled_ids.add(sell_order_id)
        except Exception as e:
            err_l = str(e).lower()
            if "already-canceled" not in err_l and "404" not in err_l and "order-not-found" not in err_l:
                surviving_sell_ids.append(sell_order_id)

    try:
        open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
        for o in open_orders:
            oid = str(o.get("orderId") or "")
            side = str(o.get("side") or "").upper()
            if not oid:
                continue
            if side == "SELL" and oid not in surviving_sell_ids:
                surviving_sell_ids.append(oid)
            if oid in canceled_ids:
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

    if sellable_q_int == 0 and not surviving_sell_ids:
        try:
            open_after = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for o in open_after:
                if str(o.get("side") or "").upper() == "SELL" and o.get("orderId"):
                    surviving_sell_ids.append(str(o["orderId"]))
        except Exception:
            pass

    return sellable_q_int, surviving_sell_ids
