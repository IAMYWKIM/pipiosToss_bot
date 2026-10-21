# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_indicators_plugin.py
듀얼 5.0초 캔들 기저 동기화, 개장 후 7분(19:00 ~ 19:07 EST / 04:00 ~ 04:07 EST) 초기 누적 VWAP 및
추세 주도주 선정(C > VWAP), 실시간 동적 누적 VWAP_t 및 -0.50% 언더슈팅 매수 덫 단가(DYN_VWAP_M050),
100% Pandas EWM 벡터화 하이킨 아시(Heikin-Ashi) & 0.0ms 실시간 틱 합성,
일봉 5MA 진폭 및 예상 밴드 산출 코어 플러그인 (Step 01 / 04)
- MODIFIED: 데이장 후반(01:40 ~ 03:57 EST) 400분봉 조회 창 밖으로 19:00 ~ 19:07 EST 초기 분봉이
  밀려났을 때 현재가(live_price)를 19:07 기준가로 둔갑시켜 추세 주도주가 바뀌던 오염 폴백을 100% 영구 제거
- MODIFIED: 이미 산출된 당일 초기 7분 지표(vwap_1907, c_1907, disp_1907 / vwap_0407, c_0407, disp_0407)를
  prev_metrics 로부터 불변 계승하도록 방어벽 구축
- MODIFIED: 제3헌법 및 최신 프롬프트.txt에 의거하여 폐기된 Wilder's RSI(14) 데드코드 영구 소각 및
  Pandas ewm(alpha=0.5, adjust=False) 기반 100% 벡터화 하이킨 아시 엔진 적용 (for 루프 금지 엄수)
- 제4헌법: 모든 시계열 연산 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 1 & 🎯 3 & 🎯 4 & Case 01, 15, 23, 35, 97 ~ 100 완벽 구현
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from toss_api import EST_TZ, REAL_ALLOWED_SYMBOLS, safe_float

logger = logging.getLogger("toss_sniper.quant_indicators_plugin")

# 동적 VWAP 대비 -0.50% 언더슈팅 매수 덫 배수 (VWAP_t * 0.9950)
VWAP_UNDERSHOOT_MULT: float = 0.9950


# ---------------------------------------------------------------------------
# Case 01 : 논리적 거래일 (일 ~ 목 18:00 EST 신세션 롤오버) 산출기
# ---------------------------------------------------------------------------
def get_logical_trading_date_str(
    now_est: Optional[datetime] = None,
    rollover_hour: int = 18,
) -> str:
    """
    🚨 Case 01:
    모든 날짜 연산 시 ZoneInfo('America/New_York') 기준을 강제하며,
    일 ~ 목요일 18:00 EST (CME 선물 신세션 오픈) 진입 시 논리적 거래일을 익일(+1일)로 롤오버한다.
    금요일 17:00 EST 이후 및 토요일은 금요일 거래일로 고정하고, 일요일 18:00 EST 부터 월요일 거래일로 롤오버한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun

    if wd == 5:
        # 토요일은 직전 금요일 날짜 유지
        target_date = (dt - timedelta(days=1)).date()
    elif wd == 6:
        # 일요일 18:00 EST 이전은 금요일 유지, 18:00 EST 이후는 월요일(+1일)로 롤오버
        if dt.hour >= rollover_hour:
            target_date = (dt + timedelta(days=1)).date()
        else:
            target_date = (dt - timedelta(days=2)).date()
    elif wd in (0, 1, 2, 3):
        # 월 ~ 목요일 18:00 EST 이후는 익일(+1일) 논리 거래일로 롤오버
        if dt.hour >= rollover_hour:
            target_date = (dt + timedelta(days=1)).date()
        else:
            target_date = dt.date()
    else:
        # 금요일은 당일 날짜 유지
        target_date = dt.date()

    return target_date.strftime("%Y-%m-%d")


def _get_logical_session_anchors_est(
    now_est: Optional[datetime] = None,
) -> Dict[str, datetime]:
    """
    현재 논리적 거래일(YYYY-MM-DD)을 기준으로:
    - 데이장 시작/초기7분마감/종료 (전일 19:00 EST ~ 당일 03:59:59 EST)
    - 프리장 시작/초기7분마감/종료 (당일 04:00 EST ~ 당일 09:29:59 EST)
    - 정규장 시작/종료 (당일 09:30 EST ~ 당일 16:00:00 EST)
    의 정확한 EST 타임스탬프 앵커를 반환한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_str = get_logical_trading_date_str(dt, rollover_hour=18)
    logical_date = datetime.strptime(logical_str, "%Y-%m-%d").date()
    prev_date = logical_date - timedelta(days=1)

    day_start = datetime.combine(prev_date, dt_time(19, 0, 0), tzinfo=EST_TZ)
    day_1907_end = datetime.combine(
        prev_date, dt_time(19, 7, 59), tzinfo=EST_TZ
    )
    # 만약 19:00 ~ 19:07 에 체결 봉이 전혀 없고 19:08 ~ 19:15 에 첫 봉이 찍힌 저유동성 날의 안전한 상한선
    day_early_fallback_end = datetime.combine(
        prev_date, dt_time(19, 15, 59), tzinfo=EST_TZ
    )
    day_end = datetime.combine(logical_date, dt_time(3, 59, 59), tzinfo=EST_TZ)

    pre_start = datetime.combine(logical_date, dt_time(4, 0, 0), tzinfo=EST_TZ)
    pre_0407_end = datetime.combine(
        logical_date, dt_time(4, 7, 59), tzinfo=EST_TZ
    )
    pre_early_fallback_end = datetime.combine(
        logical_date, dt_time(4, 15, 59), tzinfo=EST_TZ
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
        "day_start": day_start,
        "day_1907_end": day_1907_end,
        "day_early_fallback_end": day_early_fallback_end,
        "day_end": day_end,
        "pre_start": pre_start,
        "pre_0407_end": pre_0407_end,
        "pre_early_fallback_end": pre_early_fallback_end,
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
    HA_Close[t] = (O[t] + H[t] + L[t] + C[t]) / 4.0
    HA_Open[0]  = (O[0] + C[0]) / 2.0
    HA_Open[t]  = 0.5 * HA_Open[t-1] + 0.5 * HA_Close[t-1]  (t >= 1)
    위 점화식은 시드 수열 S = [(O[0]+C[0])/2.0, HA_Close[0], HA_Close[1], ..., HA_Close[n-2]] 에 대한
    ewm(alpha=0.5, adjust=False).mean() 과 수학적으로 100% 동치이므로 단일 벡터 연산으로 계산한다.
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
    🎯 3 & 🎯 4:
    5.0초 주기로 갱신된 직전 확정 분봉의 하이킨 아시 기저(prev_ha_open, prev_ha_close)와
    현재 진행 중인 분봉의 시/고/저가에 0.4초 실시간 틱 현재가(live_price)를 합성하여
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


# ---------------------------------------------------------------------------
# 내부 벡터 헬퍼 : 구간 DataFrame 의 누적 VWAP, 고/저가, 진폭 산출
# ---------------------------------------------------------------------------
def _calc_slice_vwap_and_range(
    df_slice: pd.DataFrame,
) -> Tuple[float, float, float, float, float, float]:
    """
    주어진 분봉 슬라이스(df_slice)에 대해 벡터 연산으로:
    (vwap, high, low, amp_pct, sum_tp_v, sum_v) 를 반환한다.
    거래량(Volume)이 0 이하인 봉은 1.0 등가중 폴백을 적용한다(🎯 3).
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


def _calc_initial_7m_vwap_strict(
    df_session: pd.DataFrame,
    init_start_est: datetime,
    init_end_est: datetime,
    early_fallback_end_est: datetime,
) -> Tuple[float, float, float]:
    """
    # MODIFIED [오염 폴백 영구 차단]:
    개장 후 첫 7분간(19:00:00 ~ 19:07:59 EST 또는 04:00:00 ~ 04:07:59 EST)의 실제 분봉으로만
    (vwap_init, c_init, disp_init)을 산출한다.
    - 만약 19:00 ~ 19:07 에 체결이 0건이어서 봉이 비어 있더라도, 조회된 첫 봉이 19:08 ~ 19:15 EST 이내일 때만
      해당 첫 봉으로 초기 기준을 잡고,
    - 400분봉 슬라이딩으로 인해 조회된 가장 오래된 봉이 19:16 EST (또는 04:16 EST) 이후라면
      절대로 현재가로 둔갑시키지 않고 (0.0, 0.0, 0.0)을 반환하여 기존 락온 캐시 유지 또는 600분봉 딥스캔을 유도한다!
    """
    if df_session is None or df_session.empty:
        return 0.0, 0.0, 0.0

    df_7m = df_session[
        (df_session.index >= init_start_est)
        & (df_session.index <= init_end_est)
    ]
    if df_7m.empty:
        # 개장 직후 7분간 체결이 없었으나 15분 이내(19:08~19:15 / 04:08~04:15) 첫 봉이 존재하는 경우만 제한적 허용
        earliest_ts = df_session.index[0]
        if init_start_est <= earliest_ts <= early_fallback_end_est:
            df_7m = df_session.iloc[:1]
        else:
            return 0.0, 0.0, 0.0

    vwap_init, _, _, _, _, _ = _calc_slice_vwap_and_range(df_7m)
    c_init = safe_float(df_7m["Close"].iloc[-1], 0.0)
    if vwap_init <= 0.0 or c_init <= 0.0:
        return 0.0, 0.0, 0.0

    disp_init = ((c_init - vwap_init) / vwap_init) * 100.0
    return round(vwap_init, 4), round(c_init, 4), round(disp_init, 4)


# ---------------------------------------------------------------------------
# [핵심 1] 세션별(데이장/프리장/정규장) 동적 VWAP · 초기 7분 VWAP · HA 통합 산출기
# ---------------------------------------------------------------------------
def compute_session_vwap_metrics(
    candles: List[Dict[str, Any]],
    live_price: float = 0.0,
    now_est: Optional[datetime] = None,
    prev_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & Case 15, 23, 35, 97 ~ 100:
    토스 Open API 1분봉 리스트(candles)와 실시간 현재가(live_price)를 받아
    100% 벡터 연산으로 데이장(19:00~03:59), 프리장(04:00~09:29), 정규장(09:30~16:00)의
    초기 7분 VWAP, 실시간 동적 누적 VWAP_t, -0.50% 매수 덫 단가, 하이킨 아시 기저를 산출한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    anchors = _get_logical_session_anchors_est(dt_now)
    logical_date = anchors["logical_date"]
    lp = safe_float(live_price, 0.0)

    # 기본 반환 템플릿
    default_out: Dict[str, Any] = {
        "logical_date": logical_date,
        "live_price": lp,
        "session_high": lp,
        "session_low": lp,
        "session_vwap": lp,
        # 데이장 (19:00 ~ 03:59 EST)
        "day_vwap": 0.0,
        "day_high": 0.0,
        "day_low": 0.0,
        "day_amp_pct": 0.0,
        "day_sum_tp_v": 0.0,
        "day_sum_v": 0.0,
        "vwap_1907": 0.0,
        "c_1907": 0.0,
        "disp_1907": 0.0,
        "day_disp_curr": 0.0,
        "day_dyn_trap_m050": 0.0,
        "day_hyb_trap_m050": 0.0,
        # 프리장 (04:00 ~ 09:29 EST)
        "pre_vwap": 0.0,
        "pre_high": 0.0,
        "pre_low": 0.0,
        "pre_amp_pct": 0.0,
        "pre_sum_tp_v": 0.0,
        "pre_sum_v": 0.0,
        "vwap_0407": 0.0,
        "c_0407": 0.0,
        "disp_0407": 0.0,
        "pre_disp_curr": 0.0,
        "pre_dyn_trap_m050": 0.0,
        "pre_hyb_trap_m050": 0.0,
        # 정규장 (09:30 ~ 16:00 EST)
        "reg_vwap": 0.0,
        "reg_high": 0.0,
        "reg_low": 0.0,
        "reg_amp_pct": 0.0,
        "reg_sum_tp_v": 0.0,
        "reg_sum_v": 0.0,
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

    # 벡터화된 타임스탬프 파싱 및 EST 변환, 수치형 변환
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

    # 1) 하이킨 아시 벡터 산출 및 실시간 틱 반영
    ha_base = _compute_vectorized_heikin_ashi(df)
    live_ha = compute_realtime_tick_heikin_ashi(ha_base, lp)

    # 2) 당일 전체 논리 세션(전일 19:00 EST ~) 슬라이스
    df_logical = df[df.index >= anchors["day_start"]]
    if df_logical.empty:
        df_logical = df

    sess_vwap, sess_h, sess_l, _, _, _ = _calc_slice_vwap_and_range(df_logical)
    if lp > 0.0:
        sess_h = max(sess_h, lp) if sess_h > 0.0 else lp
        sess_l = min(sess_l, lp) if sess_l > 0.0 else lp

    # 3) 데이장 (19:00:00 ~ 03:59:59 EST) 지표 산출
    df_day = df[
        (df.index >= anchors["day_start"]) & (df.index <= anchors["day_end"])
    ]
    day_vwap, day_h, day_l, day_amp, day_sum_tp_v, day_sum_v = (
        _calc_slice_vwap_and_range(df_day)
    )
    vwap_1907, c_1907, disp_1907 = _calc_initial_7m_vwap_strict(
        df_session=df_day,
        init_start_est=anchors["day_start"],
        init_end_est=anchors["day_1907_end"],
        early_fallback_end_est=anchors["day_early_fallback_end"],
    )

    # 만약 400분봉 슬라이딩으로 인해 이번 캔들 묶음에 19:07 봉이 없더라도,
    # 동일 논리 거래일의 이전 캐시(prev_metrics)에 이미 검증된 vwap_1907 이 있으면 100% 보존 계승!
    if (
        vwap_1907 <= 0.0
        and isinstance(prev_metrics, dict)
        and str(prev_metrics.get("logical_date") or "") == logical_date
        and safe_float(prev_metrics.get("vwap_1907"), 0.0) > 0.0
    ):
        vwap_1907 = safe_float(prev_metrics.get("vwap_1907"), 0.0)
        c_1907 = safe_float(prev_metrics.get("c_1907"), 0.0)
        disp_1907 = safe_float(prev_metrics.get("disp_1907"), 0.0)

    # 4) 프리장 (04:00:00 ~ 09:29:59 EST) 지표 산출
    df_pre = df[
        (df.index >= anchors["pre_start"]) & (df.index <= anchors["pre_end"])
    ]
    pre_vwap, pre_h, pre_l, pre_amp, pre_sum_tp_v, pre_sum_v = (
        _calc_slice_vwap_and_range(df_pre)
    )
    vwap_0407, c_0407, disp_0407 = _calc_initial_7m_vwap_strict(
        df_session=df_pre,
        init_start_est=anchors["pre_start"],
        init_end_est=anchors["pre_0407_end"],
        early_fallback_end_est=anchors["pre_early_fallback_end"],
    )

    if (
        vwap_0407 <= 0.0
        and isinstance(prev_metrics, dict)
        and str(prev_metrics.get("logical_date") or "") == logical_date
        and safe_float(prev_metrics.get("vwap_0407"), 0.0) > 0.0
    ):
        vwap_0407 = safe_float(prev_metrics.get("vwap_0407"), 0.0)
        c_0407 = safe_float(prev_metrics.get("c_0407"), 0.0)
        disp_0407 = safe_float(prev_metrics.get("disp_0407"), 0.0)

    # 5) 정규장 (09:30:00 ~ 16:00:59 EST) 지표 산출
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
            "day_vwap": day_vwap,
            "day_high": day_h,
            "day_low": day_l,
            "day_amp_pct": day_amp,
            "day_sum_tp_v": day_sum_tp_v,
            "day_sum_v": day_sum_v,
            "vwap_1907": vwap_1907,
            "c_1907": c_1907,
            "disp_1907": disp_1907,
            "pre_vwap": pre_vwap,
            "pre_high": pre_h,
            "pre_low": pre_l,
            "pre_amp_pct": pre_amp,
            "pre_sum_tp_v": pre_sum_tp_v,
            "pre_sum_v": pre_sum_v,
            "vwap_0407": vwap_0407,
            "c_0407": c_0407,
            "disp_0407": disp_0407,
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

    # 현재 틱 가격(lp)을 반영한 실시간 이격도 및 -0.50% 매수 덫 단가 최종 보정
    return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)


# ---------------------------------------------------------------------------
# [핵심 2] 0.4초 실시간 틱 현재가(live_price) 결합 0.0ms 지표 합성기
# ---------------------------------------------------------------------------
def refresh_metrics_with_live_tick(
    base_metrics: Optional[Dict[str, Any]],
    live_price: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [듀얼 5.0초 캔들 기저 + 0.4초 실시간 틱 합성]:
    5.0초 주기로 계산된 base_metrics 에 실시간 현재가(live_price)를 결합하여
    실시간 동적 VWAP_t, 실시간 이격도(disp_curr), -0.50% 매수 덫 단가(dyn_trap_m050),
    세션 고/저가 및 실시간 틱 하이킨 아시(is_ha_bull)를 0.0ms 시차로 즉시 갱신한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    lp = safe_float(live_price, 0.0)

    if not isinstance(base_metrics, dict) or not base_metrics:
        return {
            "logical_date": get_logical_trading_date_str(
                dt_now, rollover_hour=18
            ),
            "live_price": lp,
            "session_high": lp,
            "session_low": lp,
            "session_vwap": lp,
            "day_vwap": 0.0,
            "day_high": 0.0,
            "day_low": 0.0,
            "day_amp_pct": 0.0,
            "vwap_1907": 0.0,
            "c_1907": 0.0,
            "disp_1907": 0.0,
            "day_disp_curr": 0.0,
            "day_dyn_trap_m050": 0.0,
            "day_hyb_trap_m050": 0.0,
            "pre_vwap": 0.0,
            "pre_high": 0.0,
            "pre_low": 0.0,
            "pre_amp_pct": 0.0,
            "vwap_0407": 0.0,
            "c_0407": 0.0,
            "disp_0407": 0.0,
            "pre_disp_curr": 0.0,
            "pre_dyn_trap_m050": 0.0,
            "pre_hyb_trap_m050": 0.0,
            "reg_vwap": 0.0,
            "reg_high": 0.0,
            "reg_low": 0.0,
            "reg_amp_pct": 0.0,
            "ha_open": lp,
            "ha_close": lp,
            "is_ha_bull": False,
            "ha_state": {},
        }

    out = dict(base_metrics)
    if lp <= 0.0:
        lp = safe_float(out.get("live_price"), 0.0)
    out["live_price"] = round(lp, 4)

    if lp > 0.0:
        sh = safe_float(out.get("session_high"), lp)
        sl = safe_float(out.get("session_low"), lp)
        out["session_high"] = round(max(sh, lp) if sh > 0.0 else lp, 4)
        out["session_low"] = round(min(sl, lp) if sl > 0.0 else lp, 4)

    # 현재 진행 중인 세션의 고/저가 및 동적 VWAP_t 틱 반영
    in_day_sess = bool(hm >= 1900 or hm <= 359)
    in_pre_sess = bool(400 <= hm <= 929)
    in_reg_sess = bool(930 <= hm <= 1600)

    if in_day_sess and lp > 0.0:
        dh = safe_float(out.get("day_high"), lp)
        dl = safe_float(out.get("day_low"), lp)
        dh = max(dh, lp) if dh > 0.0 else lp
        dl = min(dl, lp) if dl > 0.0 else lp
        out["day_high"] = round(dh, 4)
        out["day_low"] = round(dl, 4)
        out["day_amp_pct"] = (
            round(((dh - dl) / dl) * 100.0, 2) if dl > 0.0 else 0.0
        )
        sum_tp_v = safe_float(out.get("day_sum_tp_v"), 0.0)
        sum_v = safe_float(out.get("day_sum_v"), 0.0)
        if sum_v > 0.0:
            out["day_vwap"] = round((sum_tp_v + lp) / (sum_v + 1.0), 4)
        elif safe_float(out.get("day_vwap"), 0.0) <= 0.0:
            out["day_vwap"] = round(lp, 4)

    elif in_pre_sess and lp > 0.0:
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

    # 데이장 동적 VWAP 대비 이격도 및 -0.50% 매수 덫 단가 산출
    day_vwap = safe_float(out.get("day_vwap"), 0.0)
    vwap_1907 = safe_float(out.get("vwap_1907"), 0.0)
    if day_vwap > 0.0 and lp > 0.0:
        out["day_disp_curr"] = round(((lp - day_vwap) / day_vwap) * 100.0, 4)
        dyn_d = (
            math.floor(((day_vwap * VWAP_UNDERSHOOT_MULT) + 1e-9) * 100.0)
            / 100.0
        )
        out["day_dyn_trap_m050"] = dyn_d
        if vwap_1907 > 0.0:
            init_d_fl = math.floor((vwap_1907 + 1e-9) * 100.0) / 100.0
            out["day_hyb_trap_m050"] = min(init_d_fl, dyn_d)
        else:
            out["day_hyb_trap_m050"] = dyn_d

    # 프리장 동적 VWAP 대비 이격도 및 -0.50% 매수 덫 단가 산출
    pre_vwap = safe_float(out.get("pre_vwap"), 0.0)
    vwap_0407 = safe_float(out.get("vwap_0407"), 0.0)
    if pre_vwap > 0.0 and lp > 0.0:
        out["pre_disp_curr"] = round(((lp - pre_vwap) / pre_vwap) * 100.0, 4)
        dyn_p = (
            math.floor(((pre_vwap * VWAP_UNDERSHOOT_MULT) + 1e-9) * 100.0)
            / 100.0
        )
        out["pre_dyn_trap_m050"] = dyn_p
        if vwap_0407 > 0.0:
            init_p_fl = math.floor((vwap_0407 + 1e-9) * 100.0) / 100.0
            out["pre_hyb_trap_m050"] = min(init_p_fl, dyn_p)
        else:
            out["pre_hyb_trap_m050"] = dyn_p

    # 실시간 틱 하이킨 아시 재합성
    ha_state = out.get("ha_state")
    if isinstance(ha_state, dict) and ha_state and lp > 0.0:
        live_ha = compute_realtime_tick_heikin_ashi(ha_state, lp)
        out["ha_open"] = live_ha["ha_open"]
        out["ha_close"] = live_ha["ha_close"]
        out["is_ha_bull"] = live_ha["is_ha_bull"]

    return out


# ---------------------------------------------------------------------------
# [핵심 3] 개장 7분(04:07 / 19:07 EST) 추세추종 주도주(C > VWAP) 선정기
# ---------------------------------------------------------------------------
def select_vwap_trend_leader(
    soxs_metrics: Optional[Dict[str, Any]],
    soxl_metrics: Optional[Dict[str, Any]],
    session_mode: str = "PRE",
) -> Dict[str, Any]:
    """
    🎯 1 [Step 1] & 🎯 3 [Step 1] & Case 97:
    - session_mode == "PRE" : 04:00 ~ 04:07 EST 누적 VWAP(vwap_0407) 대비 04:07 종가(c_0407) 비교
    - session_mode == "DAY" : 19:00 ~ 19:07 EST 누적 VWAP(vwap_1907) 대비 19:07 종가(c_1907) 비교
    - C > VWAP (disp_init > 0.0%) 조건을 만족하는 종목을 추세추종 주도주(leader_sym)로 선정한다.
      양쪽 모두 상회(disp_init > 0.0%)할 경우 초기 이격도(disp_init)가 더 큰 상위 1위 종목을 선정한다.
      (만약 양쪽 모두 정확히 0.0% 이하에 머문 특수 보합장일 때만 상대 이격도가 더 높은 종목으로 폴백하되,
       vwap_init 자체가 0.0 인 미수신 상태에서는 절대 선정하지 않고 빈 문자열을 반환한다!)
    """
    mode = str(session_mode or "PRE").upper().strip()
    vwap_key = "vwap_1907" if mode == "DAY" else "vwap_0407"
    close_key = "c_1907" if mode == "DAY" else "c_0407"
    disp_key = "disp_1907" if mode == "DAY" else "disp_0407"

    candidates: List[Tuple[float, str, float, float]] = []
    for sym, met in (("SOXS", soxs_metrics), ("SOXL", soxl_metrics)):
        if not isinstance(met, dict):
            continue
        v_init = safe_float(met.get(vwap_key), 0.0)
        c_init = safe_float(met.get(close_key), 0.0)
        d_init = safe_float(met.get(disp_key), 0.0)
        if v_init > 0.0 and c_init > 0.0:
            candidates.append((d_init, sym, v_init, c_init))

    if not candidates:
        return {
            "leader_sym": "",
            "vwap_init": 0.0,
            "c_init": 0.0,
            "disp_init": 0.0,
        }

    # 1순위: C_init > VWAP_init (disp_init > 0.0%) 만족 종목 중 이격도 1위
    above_vwap = [item for item in candidates if item[0] > 0.0]
    if above_vwap:
        above_vwap.sort(key=lambda x: x[0], reverse=True)
        best_disp, best_sym, best_vwap, best_c = above_vwap[0]
        return {
            "leader_sym": best_sym,
            "vwap_init": round(best_vwap, 4),
            "c_init": round(best_c, 4),
            "disp_init": round(best_disp, 2),
        }

    # 양쪽 모두 초기 7분 VWAP 데이터가 정상 존재하나 미세하게 <= 0.0% 인 경우 상대 우위 1위 선정
    if len(candidates) == 2:
        candidates.sort(key=lambda x: x[0], reverse=True)
        best_disp, best_sym, best_vwap, best_c = candidates[0]
        return {
            "leader_sym": best_sym,
            "vwap_init": round(best_vwap, 4),
            "c_init": round(best_c, 4),
            "disp_init": round(best_disp, 2),
        }

    return {
        "leader_sym": "",
        "vwap_init": 0.0,
        "c_init": 0.0,
        "disp_init": 0.0,
    }


# ---------------------------------------------------------------------------
# [핵심 4] 일봉 5MA 진폭 · 어제 진폭 · 세션별 예상 고저 밴드 산출기 (/sync & /avwap 용)
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

    # 당일 진행 중인 일봉을 제외한 과거 확정 일봉 분리
    date_strs = df.index.strftime("%Y-%m-%d")
    df_Completed = df[date_strs < logical_str]
    if df_Completed.empty:
        df_Completed = df.iloc[:-1] if len(df) > 1 else df

    prev_close = float(df_Completed["Close"].iloc[-1])
    amps = (
        (df_Completed["High"] - df_Completed["Low"])
        / df_Completed["Low"]
        * 100.0
    ).to_numpy(dtype=float)

    yest_amp = float(amps[-1]) if len(amps) >= 1 else 10.0
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
