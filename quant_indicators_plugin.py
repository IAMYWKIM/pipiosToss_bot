# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_indicators_plugin.py
순수 롱(SOXL) 전용 '04:07 ~ 16:00 EST 10분 고정(bucket_10m) 시드보호 매수 게이트 버킷 산출기',
100% 벡터화 시계열 지표(프리장·정규장 고/저가·진폭·VWAP·하이킨 아시) & 0.0ms 실시간 틱 합성,
일봉 5MA 진폭 및 예상 밴드 산출 코어 플러그인 (Step 02 / 04)
- MODIFIED (Case 01, Case 36, Case 64): 04:07 ~ 16:00 EST 구간에서 mins_from_0407 = (hour * 60 + minute) - 247 기반
  10분 고정 매수 게이트 버킷(bucket_10m = mins_from_0407 // 10, 16:00 마감봉 72) 정밀 산출,
  비운용 구간 반환 딕셔너리의 "is_exact_boundary_min" 키 오타 교정 및 동일 버킷 내 중복 매수 판정 차단 검증기 탑재
- MODIFIED (제3헌법): 폐기된 데이장(19:00 ~ 03:59 EST) 지표, SOXS/SOXL 추세 주도주 선정기,
  구버전 VWAP -0.50% 매수 덫 단가 데드코드를 100% 영구 소각
- 제4헌법: 모든 시계열 연산 ZoneInfo('America/New_York') 단일 소스 락온
- Case 01 ~ 64 완벽 준수
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from toss_api import EST_TZ, LOCKED_BUY_INTERVAL_MIN, safe_float, safe_int

logger = logging.getLogger("toss_sniper.quant_indicators_plugin")

# 04:07 EST 기준 분 오프셋 상수 (4시간 * 60분 + 7분 = 247분) 및 16:00 EST 마감 분 상수 (960분)
BASE_OPEN_MINS_0407: int = 4 * 60 + 7  # 247
FINAL_CLOSE_MINS_1600: int = 16 * 60   # 960


# ---------------------------------------------------------------------------
# Case 01 : 논리적 거래일 산출기 및 운용 세션 윈도우 판별기 (04:07 ~ 16:00 EST)
# ---------------------------------------------------------------------------
def get_logical_trading_date_str(
    now_est: Optional[datetime] = None,
    rollover_hour: int = 18,
) -> str:
    """
    🚨 Case 01:
    모든 날짜 연산 시 ZoneInfo('America/New_York') 기준을 강제한다.
    금요일 17:00 EST 이후 및 토요일·일요일(18:00 이전)은 직전 금요일 거래일을 반환하고,
    평일 장중(04:00 ~ 16:59 EST)에는 당일 미국 동부 날짜(YYYY-MM-DD)를 정확히 반환한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun

    if wd == 5:
        target_date = (dt - timedelta(days=1)).date()
    elif wd == 6:
        if dt.hour >= rollover_hour:
            target_date = (dt + timedelta(days=1)).date()
        else:
            target_date = (dt - timedelta(days=2)).date()
    elif wd in (0, 1, 2, 3):
        if dt.hour >= rollover_hour:
            target_date = (dt + timedelta(days=1)).date()
        else:
            target_date = dt.date()
    else:
        target_date = dt.date()

    return target_date.strftime("%Y-%m-%d")


def is_premarket_stabilization_window(hm: int) -> bool:
    """
    🔹 04:00 ~ 04:06 EST: 프리장 개장 초기 호가 안정화 대기 및 전일 오버나이트 지층 동기화 구간.
    """
    return bool(400 <= int(hm) <= 406)


def is_active_trading_window(hm: int) -> bool:
    """
    🔹 04:07 ~ 16:00 EST: 순수 롱(SOXL) 지층관리 스나이퍼 연속 가동 구간 (프리장 + 정규장 통합).
    (데이장 19:00 ~ 03:59 EST 및 16:01 이후 애프터마켓은 매매 완전 휴지)
    """
    return bool(407 <= int(hm) <= 1600)


def is_daily_settlement_window(hm: int) -> bool:
    """
    🔹 16:00 ~ 16:59 EST: 정규장 마감 후 일일 결산 보고 및 보유 지층 오버나이트 안전 보존 구간.
    """
    return bool(1600 <= int(hm) <= 1659)


# ---------------------------------------------------------------------------
# Case 01, Case 36, Case 64 : 04:07 ~ 16:00 EST 10분 고정 시드보호 매수 게이트 버킷 산출기
# ---------------------------------------------------------------------------
def compute_buy_gate_bucket_info(
    now_est: Optional[datetime] = None,
    buy_interval_min: int = LOCKED_BUY_INTERVAL_MIN,
) -> Dict[str, Any]:
    """
    MODIFIED [Case 01, Case 36, Case 64 : 10분 고정 매수 게이트 버킷 산출 공식]:
    - 30분 옵션을 완전히 배제하고 buy_interval_min = 10 으로 하드 고정한다.
    - 04:07:00 ~ 16:00:59 EST 구간에서:
      mins_from_0407 = (hour * 60 + minute) - 247
      * bucket_10m = mins_from_0407 // 10  (04:07->0, 04:17->1, ..., 15:57->71)
    - 단, 16:00 EST 정각 마감봉(hm == 1600)은 백테스트(is_day_last)와 100% 동일하게
      독립 마감 버킷(72)을 부여하여 장 마감 스냅샷 판정을 보장한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt.hour * 100 + dt.minute
    today_str = dt.strftime("%Y-%m-%d")
    interval = LOCKED_BUY_INTERVAL_MIN  # Case 64: 10분 하드 고정

    if not is_active_trading_window(hm):
        return {
            "in_trading_window": False,
            "today_str": today_str,
            "hm": hm,
            "mins_from_0407": -1,
            "bucket_10m": -1,
            "bucket_30m": -1,
            "active_bucket_id": -1,
            "buy_interval_min": interval,
            "is_exact_boundary_min": False,  # MODIFIED: 키 공백 오타 완벽 교정
            "next_gate_hm_str": "04:07 EST",
        }

    total_mins = dt.hour * 60 + dt.minute
    mins_from_0407 = max(0, total_mins - BASE_OPEN_MINS_0407)

    if hm == 1600:
        bucket_10m = 72
        bucket_30m = 24
        is_exact_min = True
    else:
        bucket_10m = int(mins_from_0407 // 10)
        bucket_30m = int(mins_from_0407 // 30)
        is_exact_min = bool((mins_from_0407 % interval) == 0)

    active_bucket_id = bucket_10m

    # 다음 10분 매수 게이트 예정 시각(HH:MM EST) 산출 (텔레그램 UI 표출용)
    if hm >= 1600:
        next_gate_str = "익일 04:07 EST"
    else:
        next_bucket_idx = (mins_from_0407 // interval) + 1
        next_total_mins = BASE_OPEN_MINS_0407 + (next_bucket_idx * interval)
        if next_total_mins >= FINAL_CLOSE_MINS_1600:
            next_gate_str = "16:00 EST"
        else:
            nh, nm = divmod(next_total_mins, 60)
            next_gate_str = f"{nh:02d}:{nm:02d} EST"

    return {
        "in_trading_window": True,
        "today_str": today_str,
        "hm": hm,
        "mins_from_0407": mins_from_0407,
        "bucket_10m": bucket_10m,
        "bucket_30m": bucket_30m,
        "active_bucket_id": active_bucket_id,
        "buy_interval_min": interval,
        "is_exact_boundary_min": is_exact_min,
        "next_gate_hm_str": next_gate_str,
    }


def evaluate_buy_gate_bucket_status(
    now_est: Optional[datetime],
    buy_interval_min: int,
    last_evaluated_bucket_id: int,
    last_evaluated_date: str,
) -> Tuple[bool, int, str, Dict[str, Any]]:
    """
    🚨 Case 36 & Case 64 [동일 10분 버킷 내 중복 매수 판정 원천 차단]:
    현재 시각(now_est)의 10분 고정 매수 버킷(active_bucket_id = bucket_10m)을 산출하여,
    오늘(today_str) 아직 평가되지 않은 새로운 버킷일 때만 is_buy_gate_open = True 를 반환한다.
    반환값: (is_buy_gate_open, active_bucket_id, today_str, bucket_info)
    """
    b_info = compute_buy_gate_bucket_info(
        now_est=now_est, buy_interval_min=LOCKED_BUY_INTERVAL_MIN
    )
    if not b_info.get("in_trading_window"):
        return False, -1, str(b_info.get("today_str") or ""), b_info

    today_str = str(b_info.get("today_str") or "")
    cur_bucket_id = safe_int(b_info.get("active_bucket_id"), -1)
    if cur_bucket_id < 0:
        return False, -1, today_str, b_info

    prev_date = str(last_evaluated_date or "").strip()
    prev_bucket_id = safe_int(last_evaluated_bucket_id, -1)

    # 날짜가 바뀌었거나 10분 버킷 인덱스가 이전 평가 버킷과 다르면 게이트 오픈!
    is_open = bool(
        (today_str != prev_date) or (cur_bucket_id != prev_bucket_id)
    )
    return is_open, cur_bucket_id, today_str, b_info


def _get_trading_day_anchors_est(
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    당일 거래일 기준 프리장(04:00 ~ 09:29:59 EST) 및 정규장(09:30 ~ 16:00:59 EST) 타임스탬프 앵커 반환.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_str = get_logical_trading_date_str(dt, rollover_hour=18)
    logical_date = datetime.strptime(logical_str, "%Y-%m-%d").date()

    pre_start = datetime.combine(logical_date, dt_time(4, 0, 0), tzinfo=EST_TZ)
    pre_0407_start = datetime.combine(
        logical_date, dt_time(4, 7, 0), tzinfo=EST_TZ
    )
    pre_end = datetime.combine(logical_date, dt_time(9, 29, 59), tzinfo=EST_TZ)

    reg_start = datetime.combine(
        logical_date, dt_time(9, 30, 0), tzinfo=EST_TZ
    )
    reg_end = datetime.combine(
        logical_date, dt_time(16, 0, 59), tzinfo=EST_TZ
    )

    return {
        "logical_date": logical_str,
        "pre_start": pre_start,
        "pre_0407_start": pre_0407_start,
        "pre_end": pre_end,
        "reg_start": reg_start,
        "reg_end": reg_end,
    }


# ---------------------------------------------------------------------------
# 100% 벡터화 하이킨 아시(Heikin-Ashi) 기저 산출 및 0.0ms 실시간 틱 합성기
# ---------------------------------------------------------------------------
def _compute_vectorized_heikin_ashi(df: pd.DataFrame) -> Dict[str, Any]:
    """
    🛠️ 벡터화 강제 (Pandas for 루프 금지):
    ewm(alpha=0.5, adjust=False).mean() 단일 벡터 연산으로 하이킨 아시 시계열을 계산한다.
    """
    if df is None or df.empty:
        return {
            "prev_ha_open": 0.0,
            "prev_ha_close": 0.0,
            "cur_bar_open": 0.0,
            "cur_bar_high": 0.0,
            "cur_bar_low": 0.0,
            "cur_bar_close": 0.0,
            "ha_open": 0.0,
            "ha_close": 0.0,
            "ha_bull": False,
        }

    opens = df["Open"].to_numpy(dtype=float)
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    closes = df["Close"].to_numpy(dtype=float)

    ha_closes = (opens + highs + lows + closes) * 0.25
    n = len(ha_closes)
    first_ha_open = (opens[0] + closes[0]) * 0.5

    if n == 1:
        ha_opens = np.array([first_ha_open], dtype=float)
        prev_ha_open = first_ha_open
        prev_ha_close = float(ha_closes[0])
    else:
        seed_arr = np.empty(n, dtype=float)
        seed_arr[0] = first_ha_open
        seed_arr[1:] = ha_closes[:-1]
        ha_opens = (
            pd.Series(seed_arr).ewm(alpha=0.5, adjust=False).mean().to_numpy()
        )
        prev_ha_open = float(ha_opens[-2])
        prev_ha_close = float(ha_closes[-2])

    cur_ha_open = float(ha_opens[-1])
    cur_ha_close = float(ha_closes[-1])
    is_bull = bool(cur_ha_close > cur_ha_open)

    return {
        "prev_ha_open": round(prev_ha_open, 4),
        "prev_ha_close": round(prev_ha_close, 4),
        "cur_bar_open": round(float(opens[-1]), 4),
        "cur_bar_high": round(float(highs[-1]), 4),
        "cur_bar_low": round(float(lows[-1]), 4),
        "cur_bar_close": round(float(closes[-1]), 4),
        "ha_open": round(cur_ha_open, 4),
        "ha_close": round(cur_ha_close, 4),
        "ha_bull": is_bull,
    }


def compute_realtime_tick_heikin_ashi(
    ha_state: Optional[Dict[str, Any]],
    live_price: float,
) -> Dict[str, Any]:
    """
    직전 확정 분봉의 하이킨 아시 기저와 실시간 현재가(live_price)를 합성하여
    0.0ms 시차의 실시간 틱 하이킨 아시 양봉(🕯️HA양봉) / 음봉(🌑HA음봉)을 판별한다.
    """
    lp = safe_float(live_price, 0.0)
    if not isinstance(ha_state, dict) or not ha_state:
        return {
            "ha_open": lp,
            "ha_close": lp,
            "is_ha_bull": False,
        }

    prev_ho = safe_float(ha_state.get("prev_ha_open"), 0.0)
    prev_hc = safe_float(ha_state.get("prev_ha_close"), 0.0)
    cur_o = safe_float(ha_state.get("cur_bar_open"), lp)
    cur_h = safe_float(ha_state.get("cur_bar_high"), lp)
    cur_l = safe_float(ha_state.get("cur_bar_low"), lp)

    if lp <= 0.0:
        ho = safe_float(ha_state.get("ha_open"), 0.0)
        hc = safe_float(ha_state.get("ha_close"), 0.0)
        return {
            "ha_open": ho,
            "ha_close": hc,
            "is_ha_bull": bool(hc > ho),
        }

    eff_o = cur_o if cur_o > 0.0 else lp
    eff_h = max(cur_h, lp) if cur_h > 0.0 else lp
    eff_l = min(cur_l, lp) if cur_l > 0.0 else lp

    live_ha_open = (
        (prev_ho + prev_hc) * 0.5
        if (prev_ho > 0.0 and prev_hc > 0.0)
        else (eff_o + lp) * 0.5
    )
    live_ha_close = (eff_o + eff_h + eff_l + lp) * 0.25
    is_bull = bool(live_ha_close > live_ha_open)

    return {
        "ha_open": round(live_ha_open, 4),
        "ha_close": round(live_ha_close, 4),
        "is_ha_bull": is_bull,
    }


def _calc_slice_vwap_and_range(
    df_slice: pd.DataFrame,
) -> Tuple[float, float, float, float, float, float]:
    """
    주어진 분봉 슬라이스(df_slice)에 대해 벡터 연산으로:
    (vwap, high, low, amp_pct, sum_tp_v, sum_v) 를 반환한다.
    """
    if df_slice is None or df_slice.empty:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

    highs = df_slice["High"].to_numpy(dtype=float)
    lows = df_slice["Low"].to_numpy(dtype=float)
    closes = df_slice["Close"].to_numpy(dtype=float)
    vols = df_slice["Volume"].to_numpy(dtype=float)

    tp = (highs + lows + closes) / 3.0
    eff_v = np.where(vols > 0.0, vols, 1.0)

    sum_tp_v = float(np.sum(tp * eff_v))
    sum_v = float(np.sum(eff_v))
    vwap = (sum_tp_v / sum_v) if sum_v > 0.0 else 0.0

    s_high = float(np.max(highs))
    s_low = float(np.min(lows))
    amp_pct = (
        ((s_high - s_low) / s_low) * 100.0
        if (s_low > 0.0 and s_high >= s_low)
        else 0.0
    )
    return (
        round(vwap, 4),
        round(s_high, 4),
        round(s_low, 4),
        round(amp_pct, 2),
        sum_tp_v,
        sum_v,
    )


# ---------------------------------------------------------------------------
# [핵심 1] SOXL 04:00 ~ 16:00 EST 프리장·정규장 시계열 지표 및 10m 게이트 산출기
# ---------------------------------------------------------------------------
def compute_session_vwap_metrics(
    candles: List[Dict[str, Any]],
    live_price: float = 0.0,
    now_est: Optional[datetime] = None,
    prev_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    토스 Open API 1분봉 리스트(candles)와 실시간 현재가(live_price)를 받아
    100% 벡터 연산으로 당일(04:00 ~ 16:00 EST) 프리장/정규장 고·저가, 진폭, 누적 VWAP,
    10분 고정 매수 게이트 버킷 정보 및 하이킨 아시 기저를 산출한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    anchors = _get_trading_day_anchors_est(dt_now)
    logical_date = anchors["logical_date"]
    lp = safe_float(live_price, 0.0)
    gate_info = compute_buy_gate_bucket_info(
        dt_now, buy_interval_min=LOCKED_BUY_INTERVAL_MIN
    )

    default_out: Dict[str, Any] = {
        "logical_date": logical_date,
        "live_price": lp,
        "session_high": lp,
        "session_low": lp,
        "session_vwap": lp,
        "session_amp_pct": 0.0,
        # 프리장 (04:00 ~ 09:29 EST)
        "pre_vwap": 0.0,
        "pre_high": 0.0,
        "pre_low": 0.0,
        "pre_amp_pct": 0.0,
        "pre_sum_tp_v": 0.0,
        "pre_sum_v": 0.0,
        # 정규장 (09:30 ~ 16:00 EST)
        "reg_vwap": 0.0,
        "reg_high": 0.0,
        "reg_low": 0.0,
        "reg_amp_pct": 0.0,
        "reg_sum_tp_v": 0.0,
        "reg_sum_v": 0.0,
        # 10분 매수 게이트 버킷 상태
        "mins_from_0407": gate_info["mins_from_0407"],
        "bucket_10m": gate_info["bucket_10m"],
        "bucket_30m": gate_info["bucket_30m"],
        "next_gate_hm_str": gate_info["next_gate_hm_str"],
        # 하이킨 아시
        "ha_open": lp,
        "ha_close": lp,
        "is_ha_bull": False,
        "ha_state": {},
    }

    if not isinstance(candles, list) or not candles:
        return default_out

    df_raw = pd.DataFrame(candles)
    required_cols = {
        "timestamp",
        "openPrice",
        "highPrice",
        "lowPrice",
        "closePrice",
        "volume",
    }
    if not required_cols.issubset(set(df_raw.columns)):
        return default_out

    ts_series = pd.to_datetime(df_raw["timestamp"], utc=True, errors="coerce")
    valid_mask = ts_series.notna()
    if not valid_mask.any():
        return default_out

    df = pd.DataFrame(
        {
            "Open": pd.to_numeric(
                df_raw.loc[valid_mask, "openPrice"], errors="coerce"
            ).fillna(0.0),
            "High": pd.to_numeric(
                df_raw.loc[valid_mask, "highPrice"], errors="coerce"
            ).fillna(0.0),
            "Low": pd.to_numeric(
                df_raw.loc[valid_mask, "lowPrice"], errors="coerce"
            ).fillna(0.0),
            "Close": pd.to_numeric(
                df_raw.loc[valid_mask, "closePrice"], errors="coerce"
            ).fillna(0.0),
            "Volume": pd.to_numeric(
                df_raw.loc[valid_mask, "volume"], errors="coerce"
            ).fillna(0.0),
        }
    )
    df.index = ts_series[valid_mask].dt.tz_convert(EST_TZ)
    df = df[
        (df["Close"] > 0.0) & (df["High"] > 0.0) & (df["Low"] > 0.0)
    ].sort_index()
    df = df[~df.index.duplicated(keep="last")]

    if df.empty:
        return default_out

    if lp <= 0.0:
        lp = float(df["Close"].iloc[-1])

    ha_base = _compute_vectorized_heikin_ashi(df)
    live_ha = compute_realtime_tick_heikin_ashi(ha_base, lp)

    # 당일 04:00 EST 이후(프리장 + 정규장) 분봉만 슬라이싱 (데이장 19:00~03:59 제외)
    df_today = df[
        (df.index >= anchors["pre_start"]) & (df.index <= anchors["reg_end"])
    ]
    if df_today.empty:
        df_today = df.between_time("04:00", "16:00")
    if df_today.empty:
        df_today = df

    sess_vwap, sess_h, sess_l, sess_amp, _, _ = _calc_slice_vwap_and_range(
        df_today
    )

    # 프리장 (04:00:00 ~ 09:29:59 EST)
    df_pre = df[
        (df.index >= anchors["pre_start"]) & (df.index <= anchors["pre_end"])
    ]
    pre_vwap, pre_h, pre_l, pre_amp, pre_sum_tp_v, pre_sum_v = (
        _calc_slice_vwap_and_range(df_pre)
    )

    # 정규장 (09:30:00 ~ 16:00:59 EST)
    df_reg = df[
        (df.index >= anchors["reg_start"]) & (df.index <= anchors["reg_end"])
    ]
    reg_vwap, reg_h, reg_l, reg_amp, reg_sum_tp_v, reg_sum_v = (
        _calc_slice_vwap_and_range(df_reg)
    )

    out = dict(default_out)
    out.update(
        {
            "logical_date": logical_date,
            "live_price": round(lp, 4),
            "session_high": round(sess_h, 4),
            "session_low": round(sess_l, 4),
            "session_vwap": round(sess_vwap, 4),
            "session_amp_pct": sess_amp,
            "pre_vwap": pre_vwap,
            "pre_high": pre_h,
            "pre_low": pre_l,
            "pre_amp_pct": pre_amp,
            "pre_sum_tp_v": pre_sum_tp_v,
            "pre_sum_v": pre_sum_v,
            "reg_vwap": reg_vwap,
            "reg_high": reg_h,
            "reg_low": reg_l,
            "reg_amp_pct": reg_amp,
            "reg_sum_tp_v": reg_sum_tp_v,
            "reg_sum_v": reg_sum_v,
            "ha_open": live_ha["ha_open"],
            "ha_close": live_ha["ha_close"],
            "is_ha_bull": live_ha["is_ha_bull"],
            "ha_state": ha_base,
        }
    )

    return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)


# ---------------------------------------------------------------------------
# [핵심 2] 실시간 틱 현재가(live_price) 결합 0.0ms 지표 합성기
# ---------------------------------------------------------------------------
def refresh_metrics_with_live_tick(
    base_metrics: Optional[Dict[str, Any]],
    live_price: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    캔들 기저 지표(base_metrics)에 실시간 현재가(live_price)를 결합하여
    당일 고/저가, 프리장/정규장 VWAP, 하이킨 아시 상태 및 10분 버킷 정보를 즉시 갱신한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    lp = safe_float(live_price, 0.0)
    gate_info = compute_buy_gate_bucket_info(
        dt_now, buy_interval_min=LOCKED_BUY_INTERVAL_MIN
    )

    if not isinstance(base_metrics, dict) or not base_metrics:
        return {
            "logical_date": get_logical_trading_date_str(
                dt_now, rollover_hour=18
            ),
            "live_price": lp,
            "session_high": lp,
            "session_low": lp,
            "session_vwap": lp,
            "session_amp_pct": 0.0,
            "pre_vwap": 0.0,
            "pre_high": 0.0,
            "pre_low": 0.0,
            "pre_amp_pct": 0.0,
            "reg_vwap": 0.0,
            "reg_high": 0.0,
            "reg_low": 0.0,
            "reg_amp_pct": 0.0,
            "mins_from_0407": gate_info["mins_from_0407"],
            "bucket_10m": gate_info["bucket_10m"],
            "bucket_30m": gate_info["bucket_30m"],
            "next_gate_hm_str": gate_info["next_gate_hm_str"],
            "ha_open": lp,
            "ha_close": lp,
            "is_ha_bull": False,
            "ha_state": {},
        }

    out = dict(base_metrics)
    if lp <= 0.0:
        lp = safe_float(out.get("live_price"), 0.0)
    out["live_price"] = round(lp, 4)
    out["mins_from_0407"] = gate_info["mins_from_0407"]
    out["bucket_10m"] = gate_info["bucket_10m"]
    out["bucket_30m"] = gate_info["bucket_30m"]
    out["next_gate_hm_str"] = gate_info["next_gate_hm_str"]

    if lp > 0.0 and (400 <= hm <= 1600):
        sh = safe_float(out.get("session_high"), lp)
        sl = safe_float(out.get("session_low"), lp)
        sh = max(sh, lp) if sh > 0.0 else lp
        sl = min(sl, lp) if sl > 0.0 else lp
        out["session_high"] = round(sh, 4)
        out["session_low"] = round(sl, 4)
        out["session_amp_pct"] = (
            round(((sh - sl) / sl) * 100.0, 2) if sl > 0.0 else 0.0
        )

    in_pre_sess = bool(400 <= hm <= 929)
    in_reg_sess = bool(930 <= hm <= 1600)

    if in_pre_sess and lp > 0.0:
        ph = safe_float(out.get("pre_high"), lp)
        pl = safe_float(out.get("pre_low"), lp)
        ph = max(ph, lp) if ph > 0.0 else lp
        pl = min(pl, lp) if pl > 0.0 else lp
        out["pre_high"] = round(ph, 4)
        out["pre_low"] = round(pl, 4)
        out["pre_amp_pct"] = (
            round(((ph - pl) / pl) * 100.0, 2) if pl > 0.0 else 0.0
        )
        sum_tp_v = safe_float(out.get("pre_sum_tp_v"), 0.0)
        sum_v = safe_float(out.get("pre_sum_v"), 0.0)
        if sum_v > 0.0:
            out["pre_vwap"] = round((sum_tp_v + lp) / (sum_v + 1.0), 4)
        elif safe_float(out.get("pre_vwap"), 0.0) <= 0.0:
            out["pre_vwap"] = round(lp, 4)

    elif in_reg_sess and lp > 0.0:
        rh = safe_float(out.get("reg_high"), lp)
        rl = safe_float(out.get("reg_low"), lp)
        rh = max(rh, lp) if rh > 0.0 else lp
        rl = min(rl, lp) if rl > 0.0 else lp
        out["reg_high"] = round(rh, 4)
        out["reg_low"] = round(rl, 4)
        out["reg_amp_pct"] = (
            round(((rh - rl) / rl) * 100.0, 2) if rl > 0.0 else 0.0
        )
        sum_tp_v = safe_float(out.get("reg_sum_tp_v"), 0.0)
        sum_v = safe_float(out.get("reg_sum_v"), 0.0)
        if sum_v > 0.0:
            out["reg_vwap"] = round((sum_tp_v + lp) / (sum_v + 1.0), 4)
        elif safe_float(out.get("reg_vwap"), 0.0) <= 0.0:
            out["reg_vwap"] = round(lp, 4)

    ha_state = out.get("ha_state")
    if isinstance(ha_state, dict) and ha_state and lp > 0.0:
        live_ha = compute_realtime_tick_heikin_ashi(ha_state, lp)
        out["ha_open"] = live_ha["ha_open"]
        out["ha_close"] = live_ha["ha_close"]
        out["is_ha_bull"] = live_ha["is_ha_bull"]

    return out


# ---------------------------------------------------------------------------
# [핵심 3] 일봉 5MA 진폭 · 어제 진폭 · 세션별 예상 고저 밴드 산출기 (/sync & /avwap 용)
# ---------------------------------------------------------------------------
def calc_session_expected_band(
    session_high: float,
    session_low: float,
    amp_5ma_pct: float,
) -> Tuple[float, float]:
    """
    /avwap 보드의 프리장 및 정규장 예상 밴드 산출:
    - 예상 저가 = session_high / (1 + amp_5ma_pct / 100)
    - 예상 고가 = session_low * (1 + amp_5ma_pct / 100)
    """
    sh = safe_float(session_high, 0.0)
    sl = safe_float(session_low, 0.0)
    amp = max(0.0, safe_float(amp_5ma_pct, 0.0))

    if sh <= 0.0 or sl <= 0.0 or amp <= 0.0:
        return round(sl, 2), round(sh, 2)

    ratio = 1.0 + (amp / 100.0)
    exp_low = sh / ratio
    exp_high = sl * ratio
    return round(exp_low, 2), round(exp_high, 2)


def compute_daily_5ma_and_expected_band(
    daily_candles: List[Dict[str, Any]],
    session_high: float,
    session_low: float,
    live_price: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, float]:
    """
    /sync 및 /avwap 보드 표출을 위해 일봉(1d) 시계열로부터:
    - 직전 거래일 종가(prev_close) 대비 금일 고/저가 등락률(high_chg_pct, low_chg_pct)
    - 최근 5거래일 평균 진폭(amp_5ma_pct) 및 어제 진폭(yesterday_amp_pct)
    을 100% 벡터 연산으로 산출한다.
    """
    sh = safe_float(session_high, safe_float(live_price, 0.0))
    sl = safe_float(session_low, safe_float(live_price, 0.0))
    lp = safe_float(live_price, sh)

    out = {
        "prev_close": lp,
        "high_chg_pct": 0.0,
        "low_chg_pct": 0.0,
        "amp_5ma_pct": 10.0,
        "yesterday_amp_pct": 10.0,
    }
    if not isinstance(daily_candles, list) or not daily_candles:
        return out

    df_d = pd.DataFrame(daily_candles)
    req = {"timestamp", "highPrice", "lowPrice", "closePrice"}
    if not req.issubset(set(df_d.columns)):
        return out

    ts_s = pd.to_datetime(df_d["timestamp"], utc=True, errors="coerce")
    valid = ts_s.notna()
    if not valid.any():
        return out

    df = pd.DataFrame(
        {
            "High": pd.to_numeric(
                df_d.loc[valid, "highPrice"], errors="coerce"
            ).fillna(0.0),
            "Low": pd.to_numeric(
                df_d.loc[valid, "lowPrice"], errors="coerce"
            ).fillna(0.0),
            "Close": pd.to_numeric(
                df_d.loc[valid, "closePrice"], errors="coerce"
            ).fillna(0.0),
        }
    )
    df.index = ts_s[valid].dt.tz_convert(EST_TZ)
    df = df[(df["Low"] > 0.0) & (df["High"] >= df["Low"])].sort_index()
    if df.empty:
        return out

    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_str = get_logical_trading_date_str(dt_now, rollover_hour=18)

    date_strs = df.index.strftime("%Y-%m-%d")
    df_completed = df[date_strs < logical_str]
    if df_completed.empty:
        df_completed = df.iloc[:-1] if len(df) > 1 else df

    prev_close = float(df_completed["Close"].iloc[-1])
    amps = (
        (df_completed["High"] - df_completed["Low"])
        / df_completed["Low"]
        * 100.0
    ).to_numpy(dtype=float)

    yest_amp = float(amps[-1]) if len(amps[-1:]) >= 1 else 10.0
    amp_5ma = float(np.mean(amps[-5:])) if len(amps) >= 1 else 10.0

    high_chg = (
        ((sh - prev_close) / prev_close) * 100.0
        if (prev_close > 0.0 and sh > 0.0)
        else 0.0
    )
    low_chg = (
        ((sl - prev_close) / prev_close) * 100.0
        if (prev_close > 0.0 and sl > 0.0)
        else 0.0
    )

    return {
        "prev_close": round(prev_close, 4),
        "high_chg_pct": round(high_chg, 2),
        "low_chg_pct": round(low_chg, 2),
        "amp_5ma_pct": round(amp_5ma, 1),
        "yesterday_amp_pct": round(yest_amp, 1),
    }
