# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_indicators_plugin.py
100% 벡터화 퀀트 지표 연산 및 0.0ms 실시간 틱 하이킨 아시 합성 플러그인 (Step 01 / 04)
- MODIFIED: RSI(1m) 자료 미사용 결정에 따라 Wilder's RSI(1m) 연산 및 상태를 전면 폐기(소각)하여 연산 속도 극대화
- NEW: /avwap 레이더 보드의 '🌅 프리장' 및 '🔥 정규장' 개별 팩트 고가/저가/진폭(pre_high, pre_low, pre_amp_pct,
  reg_high, reg_low, reg_amp_pct) 벡터 추출 및 실시간 틱 갱신 추가
- NEW: /sync 듀얼 지시서의 금일 고가/저가 전일 대비 등락률(%) 표출을 위해 직전 확정 일봉 종가(prev_close) 및
  세션별 5MA 예상 밴드(pre_exp_high/low, reg_exp_high/low) 산출 추가
- 제3헌법 & 벡터화 강제: Pandas/NumPy 벡터 연산 기반 VWAP 및 Heikin-Ashi 산출
- 제4헌법 & Case 01, 08: 모든 시계열 인덱스를 ZoneInfo('America/New_York') 단일 타임존으로 락온
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from toss_api import EST_TZ, safe_float


# ---------------------------------------------------------------------------
# 논리적 거래일(18:00 / 19:00 EST 롤오버) 및 세션 경계 시각 산출 유틸리티 (Case 01)
# ---------------------------------------------------------------------------
def get_logical_trading_date_str(
    now_est: Optional[datetime] = None, rollover_hour: int = 18
) -> str:
    """
    Case 01: 미국 동부 시간(EST_TZ) 기준으로 논리적 거래일(YYYY-MM-DD)을 반환한다.
    일 ~ 목요일 18:00 EST (CME 선물 오픈) 또는 19:00 EST (데이마켓 오픈) 이후에는
    익일(다음 영업일) 거래 세션으로 롤오버한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    if wd == 6 and dt.hour >= rollover_hour:
        return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    if wd in (0, 1, 2, 3) and dt.hour >= rollover_hour:
        return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")


def get_session_anchor_datetimes(
    now_est: Optional[datetime] = None,
) -> Dict[str, datetime]:
    """
    현재 시각(EST)이 속한 논리적 거래일의 세션별 시작/종료 기준 시각(EST)을 반환한다.
    - day_start    : 전일 19:00:00 EST (데이마켓 시작)
    - day_1907_end : 전일 19:07:59 EST (데이장 초기 7분 VWAP_1907 마감)
    - day_end      : 당일 03:59:59 EST (데이마켓 종료)
    - pre_start    : 당일 04:00:00 EST (프리마켓 시작)
    - pre_0407_end : 당일 04:07:59 EST (프리장 초기 7분 VWAP_0407 마감)
    - pre_end      : 당일 09:29:59 EST (프리마켓 종료)
    - reg_start    : 당일 09:30:00 EST (정규장 시작)
    - reg_end      : 당일 15:59:59 EST (정규장 종료)
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_date_str = get_logical_trading_date_str(dt, rollover_hour=18)
    trade_day = datetime.strptime(logical_date_str, "%Y-%m-%d").replace(
        tzinfo=EST_TZ
    )
    prev_day = trade_day - timedelta(days=1)

    return {
        "trade_day": trade_day,
        "day_start": prev_day.replace(
            hour=19, minute=0, second=0, microsecond=0
        ),
        "day_1907_end": prev_day.replace(
            hour=19, minute=7, second=59, microsecond=999999
        ),
        "day_end": trade_day.replace(
            hour=3, minute=59, second=59, microsecond=999999
        ),
        "pre_start": trade_day.replace(
            hour=4, minute=0, second=0, microsecond=0
        ),
        "pre_0407_end": trade_day.replace(
            hour=4, minute=7, second=59, microsecond=999999
        ),
        "pre_end": trade_day.replace(
            hour=9, minute=29, second=59, microsecond=999999
        ),
        "reg_start": trade_day.replace(
            hour=9, minute=30, second=0, microsecond=0
        ),
        "reg_end": trade_day.replace(
            hour=15, minute=59, second=59, microsecond=999999
        ),
    }


# ---------------------------------------------------------------------------
# 1분봉 캔들 리스트 -> EST 시계열 DataFrame 100% 벡터 변환기
# ---------------------------------------------------------------------------
def candles_to_est_dataframe(candles: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    토스 Open API /api/v1/candles 응답 리스트를 시간 오름차순 정렬된
    America/New_York 타임존 인덱스 DataFrame 으로 변환한다 (for 루프 배제).
    """
    if not candles or not isinstance(candles, list):
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume", "tp", "pv"]
        )

    df = pd.DataFrame(candles)
    required_cols = {
        "timestamp",
        "openPrice",
        "highPrice",
        "lowPrice",
        "closePrice",
        "volume",
    }
    if not required_cols.issubset(set(df.columns)):
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume", "tp", "pv"]
        )

    ts_series = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.assign(dt_est=ts_series.dt.tz_convert(EST_TZ))
    df = df.dropna(subset=["dt_est"]).drop_duplicates(
        subset=["dt_est"], keep="first"
    )
    if df.empty:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume", "tp", "pv"]
        )

    df = df.sort_values("dt_est", ascending=True).set_index("dt_est")

    open_s = (
        pd.to_numeric(df["openPrice"], errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    high_s = (
        pd.to_numeric(df["highPrice"], errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    low_s = (
        pd.to_numeric(df["lowPrice"], errors="coerce").fillna(0.0).astype(float)
    )
    close_s = (
        pd.to_numeric(df["closePrice"], errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    vol_s = (
        pd.to_numeric(df["volume"], errors="coerce").fillna(0.0).astype(float)
    )

    # 대표가격 TP = (H + L + C) / 3.0, 거래량 0인 봉은 1.0 등가중 폴백 (🎯 3)
    tp_s = (high_s + low_s + close_s) / 3.0
    eff_vol_s = np.where(vol_s > 0.0, vol_s, 1.0)
    pv_s = tp_s * eff_vol_s

    out_df = pd.DataFrame(
        {
            "open": open_s,
            "high": high_s,
            "low": low_s,
            "close": close_s,
            "volume": vol_s,
            "eff_vol": eff_vol_s,
            "tp": tp_s,
            "pv": pv_s,
        },
        index=df.index,
    )
    out_df = out_df[out_df["close"] > 0.0]
    return out_df


# ---------------------------------------------------------------------------
# 하이킨 아시(Heikin-Ashi) 기저 산출 및 0.0ms 실시간 틱 합성기 (🎯 4)
# ---------------------------------------------------------------------------
def compute_heikin_ashi_state(df: pd.DataFrame) -> Dict[str, Any]:
    """
    1분봉 OHLC DataFrame 으로부터 하이킨 아시 시계열을 산출하고,
    직전 확정 봉(iloc[-2])의 HA 기저(prev_ha_open, prev_ha_close) 및
    현재 진행 봉(iloc[-1])의 OHLC 기저를 반환한다.
    """
    default_ha = {
        "prev_ha_open": 0.0,
        "prev_ha_close": 0.0,
        "cur_bar_open": 0.0,
        "cur_bar_high": 0.0,
        "cur_bar_low": 0.0,
        "cur_bar_close": 0.0,
        "ha_open": 0.0,
        "ha_close": 0.0,
        "ha_bull": False,
        "raw_bull": False,
    }
    if df is None or df.empty:
        return default_ha

    o_arr = df["open"].to_numpy(dtype=float)
    h_arr = df["high"].to_numpy(dtype=float)
    l_arr = df["low"].to_numpy(dtype=float)
    c_arr = df["close"].to_numpy(dtype=float)
    n = len(c_arr)

    ha_close_arr = (o_arr + h_arr + l_arr + c_arr) * 0.25
    ha_open_arr = np.empty(n, dtype=float)
    ha_open_arr[0] = (o_arr[0] + c_arr[0]) * 0.5

    for i in range(1, n):
        ha_open_arr[i] = (ha_open_arr[i - 1] + ha_close_arr[i - 1]) * 0.5

    prev_idx = -2 if n >= 2 else -1
    cur_o = float(o_arr[-1])
    cur_h = float(h_arr[-1])
    cur_l = float(l_arr[-1])
    cur_c = float(c_arr[-1])
    cur_ha_o = float(ha_open_arr[-1])
    cur_ha_c = float(ha_close_arr[-1])

    return {
        "prev_ha_open": float(ha_open_arr[prev_idx]),
        "prev_ha_close": float(ha_close_arr[prev_idx]),
        "cur_bar_open": cur_o,
        "cur_bar_high": cur_h,
        "cur_bar_low": cur_l,
        "cur_bar_close": cur_c,
        "ha_open": cur_ha_o,
        "ha_close": cur_ha_c,
        "ha_bull": bool(cur_ha_c > cur_ha_o),
        "raw_bull": bool(cur_c > cur_o),
    }


def compute_realtime_tick_heikin_ashi(
    ha_state: Optional[Dict[str, Any]], live_price: float
) -> Dict[str, Any]:
    """
    🎯 4: 0.4초 고주파 부스터 루프에서 실시간 현재가(live_price)를 직전 분봉 HA 기저와 결합해
    0.0ms 시차로 실시간 틱 하이킨 아시(🕯️HA양봉 / 🌑HA음봉) 및 일반 양봉 여부를 합성한다.
    """
    lp = safe_float(live_price, 0.0)
    if not isinstance(ha_state, dict) or lp <= 0.0:
        return {
            "ha_open": lp,
            "ha_close": lp,
            "is_ha_bull": False,
            "is_raw_bull": False,
            "cur_open": lp,
            "cur_high": lp,
            "cur_low": lp,
        }

    prev_ha_o = safe_float(ha_state.get("prev_ha_open"), lp)
    prev_ha_c = safe_float(ha_state.get("prev_ha_close"), lp)
    cur_o = safe_float(ha_state.get("cur_bar_open"), lp)
    cur_h = max(safe_float(ha_state.get("cur_bar_high"), lp), lp)
    cur_l_raw = safe_float(ha_state.get("cur_bar_low"), lp)
    cur_l = min(cur_l_raw, lp) if cur_l_raw > 0.0 else lp

    live_ha_open = (prev_ha_o + prev_ha_c) * 0.5 if prev_ha_o > 0.0 else cur_o
    live_ha_close = (cur_o + cur_h + cur_l + lp) * 0.25

    return {
        "ha_open": round(live_ha_open, 4),
        "ha_close": round(live_ha_close, 4),
        "is_ha_bull": bool(live_ha_close > live_ha_open),
        "is_raw_bull": bool(lp > cur_o),
        "cur_open": cur_o,
        "cur_high": cur_h,
        "cur_low": cur_l,
    }


# ---------------------------------------------------------------------------
# 세션별 초기 7분 VWAP (VWAP_1907 / VWAP_0407) 및 실시간 동적 VWAP_t 통합 산출기
# ---------------------------------------------------------------------------
def _calc_vwap_from_slice(df_slice: pd.DataFrame) -> Tuple[float, float, float]:
    """
    주어진 구간 DataFrame 에서 (vwap, cum_pv, cum_vol)을 벡터 연산으로 반환한다.
    """
    if df_slice is None or df_slice.empty:
        return 0.0, 0.0, 0.0
    cum_pv = float(df_slice["pv"].sum())
    cum_vol = float(df_slice["eff_vol"].sum())
    if cum_vol <= 0.0:
        return 0.0, 0.0, 0.0
    return cum_pv / cum_vol, cum_pv, cum_vol


def _calc_high_low_amp_from_slice(
    df_slice: pd.DataFrame, in_session: bool, live_price: float
) -> Tuple[float, float, float]:
    """
    # NEW: 특정 세션(데이장/프리장/정규장) 슬라이스의 팩트 고가, 저가, 진폭(%)을 산출한다.
    세션 시작 전이거나 데이터가 없으면 (0.0, 0.0, 0.0)을 반환한다.
    """
    if df_slice is None or df_slice.empty:
        if in_session and live_price > 0.0:
            return live_price, live_price, 0.0
        return 0.0, 0.0, 0.0

    s_high = float(df_slice["high"].max())
    s_low = float(df_slice["low"].min())
    if in_session and live_price > 0.0:
        s_high = max(s_high, live_price)
        s_low = min(s_low, live_price) if s_low > 0.0 else live_price

    s_amp = (
        ((s_high - s_low) / s_low) * 100.0
        if (s_low > 0.0 and s_high >= s_low)
        else 0.0
    )
    return s_high, s_low, s_amp


def compute_session_vwap_metrics(
    candles: List[Dict[str, Any]],
    live_price: float = 0.0,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & Case 97 ~ 101:
    400개 다중 페이지 1분봉 시계열과 실시간 현재가(live_price)를 결합하여
    데이장 [모의], 03:58 1주 휩소, 프리장 [실전], 정규장 VWAP 탈출 및 텔레그램 보드에 필요한 모든 지표를
    100% 벡터 연산으로 일괄 산출한다 (RSI(1m) 연산 전면 폐기 완료).
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    anchors = get_session_anchor_datetimes(dt_now)
    df = candles_to_est_dataframe(candles)

    lp = safe_float(live_price, 0.0)
    if lp <= 0.0 and not df.empty:
        lp = float(df["close"].iloc[-1])

    hm = dt_now.hour * 100 + dt_now.minute
    in_day_session = bool(hm >= 1900 or hm <= 359)
    in_pre_session = bool(400 <= hm <= 929)
    in_reg_session = bool(930 <= hm <= 1559)

    # 1. 데이장 (19:00 ~ 03:59 EST) 슬라이스 및 초기 7분(19:00 ~ 19:07 EST) VWAP_1907
    df_day = (
        df[(df.index >= anchors["day_start"]) & (df.index <= anchors["day_end"])]
        if not df.empty
        else pd.DataFrame()
    )
    df_day_1907 = (
        df[
            (df.index >= anchors["day_start"])
            & (df.index <= anchors["day_1907_end"])
        ]
        if not df.empty
        else pd.DataFrame()
    )
    vwap_1907, _, _ = _calc_vwap_from_slice(df_day_1907)
    c_1907 = (
        float(df_day_1907["close"].iloc[-1]) if not df_day_1907.empty else 0.0
    )
    disp_1907 = (
        ((c_1907 - vwap_1907) / vwap_1907) * 100.0
        if (vwap_1907 > 0.0 and c_1907 > 0.0)
        else 0.0
    )

    day_vwap, day_cum_pv, day_cum_vol = _calc_vwap_from_slice(df_day)
    if in_day_session and lp > 0.0:
        if day_cum_vol > 0.0:
            day_vwap = (day_cum_pv + lp) / (day_cum_vol + 1.0)
        else:
            day_vwap = lp
            if vwap_1907 <= 0.0:
                vwap_1907 = lp
                c_1907 = lp

    day_disp_curr = (
        ((lp - day_vwap) / day_vwap) * 100.0
        if (day_vwap > 0.0 and lp > 0.0)
        else 0.0
    )
    day_dyn_trap_m050 = (
        math.floor((day_vwap * 0.9950) * 100.0) / 100.0
        if day_vwap > 0.0
        else 0.0
    )
    day_hyb_trap_m050 = (
        min(math.floor(vwap_1907 * 100.0) / 100.0, day_dyn_trap_m050)
        if vwap_1907 > 0.0 and day_dyn_trap_m050 > 0.0
        else day_dyn_trap_m050
    )
    whipsaw_buy_p = (
        math.floor((day_vwap / 1.005) * 100.0) / 100.0
        if day_vwap > 0.0
        else 0.0
    )
    day_high, day_low, day_amp_pct = _calc_high_low_amp_from_slice(
        df_day, in_day_session, lp
    )

    # 2. 프리장 (04:00 ~ 09:29 EST) 슬라이스 및 초기 7분(04:00 ~ 04:07 EST) VWAP_0407
    df_pre = (
        df[(df.index >= anchors["pre_start"]) & (df.index <= anchors["pre_end"])]
        if not df.empty
        else pd.DataFrame()
    )
    df_pre_0407 = (
        df[
            (df.index >= anchors["pre_start"])
            & (df.index <= anchors["pre_0407_end"])
        ]
        if not df.empty
        else pd.DataFrame()
    )
    vwap_0407, _, _ = _calc_vwap_from_slice(df_pre_0407)
    c_0407 = (
        float(df_pre_0407["close"].iloc[-1]) if not df_pre_0407.empty else 0.0
    )
    disp_0407 = (
        ((c_0407 - vwap_0407) / vwap_0407) * 100.0
        if (vwap_0407 > 0.0 and c_0407 > 0.0)
        else 0.0
    )

    pre_vwap, pre_cum_pv, pre_cum_vol = _calc_vwap_from_slice(df_pre)
    if in_pre_session and lp > 0.0:
        if pre_cum_vol > 0.0:
            pre_vwap = (pre_cum_pv + lp) / (pre_cum_vol + 1.0)
        else:
            pre_vwap = lp
            if vwap_0407 <= 0.0:
                vwap_0407 = lp
                c_0407 = lp

    pre_disp_curr = (
        ((lp - pre_vwap) / pre_vwap) * 100.0
        if (pre_vwap > 0.0 and lp > 0.0)
        else 0.0
    )
    pre_dyn_trap_m050 = (
        math.floor((pre_vwap * 0.9950) * 100.0) / 100.0
        if pre_vwap > 0.0
        else 0.0
    )
    pre_hyb_trap_m050 = (
        min(math.floor(vwap_0407 * 100.0) / 100.0, pre_dyn_trap_m050)
        if vwap_0407 > 0.0 and pre_dyn_trap_m050 > 0.0
        else pre_dyn_trap_m050
    )
    pre_high, pre_low, pre_amp_pct = _calc_high_low_amp_from_slice(
        df_pre, in_pre_session, lp
    )

    # 3. 정규장 (09:30 ~ 15:59 EST) 슬라이스 및 누적 reg_vwap (15:00 ~ 15:58 VWAP_ESCAPE 용)
    df_reg = (
        df[(df.index >= anchors["reg_start"]) & (df.index <= anchors["reg_end"])]
        if not df.empty
        else pd.DataFrame()
    )
    reg_vwap, reg_cum_pv, reg_cum_vol = _calc_vwap_from_slice(df_reg)
    if in_reg_session and lp > 0.0:
        if reg_cum_vol > 0.0:
            reg_vwap = (reg_cum_pv + lp) / (reg_cum_vol + 1.0)
        else:
            reg_vwap = lp

    reg_disp_curr = (
        ((lp - reg_vwap) / reg_vwap) * 100.0
        if (reg_vwap > 0.0 and lp > 0.0)
        else 0.0
    )
    reg_high, reg_low, reg_amp_pct = _calc_high_low_amp_from_slice(
        df_reg, in_reg_session, lp
    )

    # 4. 하이킨 아시(Heikin-Ashi) 1분봉 기저 및 실시간 틱 합성
    ha_state = compute_heikin_ashi_state(df)
    live_ha = compute_realtime_tick_heikin_ashi(ha_state, lp)

    # 5. 당일 전체 세션(19:00 EST ~) 통합 고가 / 저가 / 진폭 집계 (Case 40, /sync 보드용)
    df_full_session = (
        df[df.index >= anchors["day_start"]] if not df.empty else pd.DataFrame()
    )
    if not df_full_session.empty:
        sess_open = float(df_full_session["open"].iloc[0])
        sess_high = float(df_full_session["high"].max())
        sess_low = float(df_full_session["low"].min())
        if lp > 0.0:
            sess_high = max(sess_high, lp)
            sess_low = min(sess_low, lp) if sess_low > 0.0 else lp
    else:
        sess_open = lp
        sess_high = lp
        sess_low = lp

    sess_amp_pct = (
        ((sess_high - sess_low) / sess_low) * 100.0
        if (sess_low > 0.0 and sess_high >= sess_low)
        else 0.0
    )

    return {
        "logical_date": anchors["trade_day"].strftime("%Y-%m-%d"),
        "live_price": lp,
        # 데이장 [모의] & 03:58 휩소 지표
        "vwap_1907": round(vwap_1907, 4),
        "c_1907": round(c_1907, 4),
        "disp_1907": round(disp_1907, 3),
        "day_vwap": round(day_vwap, 4),
        "day_cum_pv": day_cum_pv,
        "day_cum_vol": day_cum_vol,
        "day_disp_curr": round(day_disp_curr, 3),
        "day_dyn_trap_m050": day_dyn_trap_m050,
        "day_hyb_trap_m050": day_hyb_trap_m050,
        "whipsaw_buy_p": whipsaw_buy_p,
        "day_high": round(day_high, 4),
        "day_low": round(day_low, 4),
        "day_amp_pct": round(day_amp_pct, 2),
        # 프리장 [실전] 지표
        "vwap_0407": round(vwap_0407, 4),
        "c_0407": round(c_0407, 4),
        "disp_0407": round(disp_0407, 3),
        "pre_vwap": round(pre_vwap, 4),
        "pre_cum_pv": pre_cum_pv,
        "pre_cum_vol": pre_cum_vol,
        "pre_disp_curr": round(pre_disp_curr, 3),
        "pre_dyn_trap_m050": pre_dyn_trap_m050,
        "pre_hyb_trap_m050": pre_hyb_trap_m050,
        "pre_high": round(pre_high, 4),
        "pre_low": round(pre_low, 4),
        "pre_amp_pct": round(pre_amp_pct, 2),
        # 정규장 지표
        "reg_vwap": round(reg_vwap, 4),
        "reg_cum_pv": reg_cum_pv,
        "reg_cum_vol": reg_cum_vol,
        "reg_disp_curr": round(reg_disp_curr, 3),
        "reg_high": round(reg_high, 4),
        "reg_low": round(reg_low, 4),
        "reg_amp_pct": round(reg_amp_pct, 2),
        # 하이킨 아시(Heikin-Ashi) 1분봉 기저 및 실시간 상태
        "ha_state": ha_state,
        "is_ha_bull": bool(live_ha["is_ha_bull"]),
        "is_raw_bull": bool(live_ha["is_raw_bull"]),
        "ha_open": live_ha["ha_open"],
        "ha_close": live_ha["ha_close"],
        "cur_bar_open": live_ha["cur_open"],
        # 당일 통합 세션(19:00 EST~) 시가/고가/저가/진폭
        "session_open": round(sess_open, 4),
        "session_high": round(sess_high, 4),
        "session_low": round(sess_low, 4),
        "session_amp_pct": round(sess_amp_pct, 2),
        "updated_ts": dt_now.timestamp(),
    }


def refresh_metrics_with_live_tick(
    cached_metrics: Optional[Dict[str, Any]],
    live_price: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    5.0초 캔들 폴링 사이의 0.4초 고주파 부스터 틱마다 호출되어,
    실시간 현재가(live_price)를 반영한 동적 VWAP_t, 실시간 이격도(disp_curr),
    -0.50% 매수 덫 단가, 세션별 고가/저가, 실시간 틱 하이킨 아시(🕯️HA양봉/🌑HA음봉)를
    0.0ms 에 즉시 재합성한다 (RSI(1m) 연산 전면 폐기 완료).
    """
    lp = safe_float(live_price, 0.0)
    if not isinstance(cached_metrics, dict):
        return compute_session_vwap_metrics([], live_price=lp, now_est=now_est)
    if lp <= 0.0:
        return cached_metrics

    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    m = dict(cached_metrics)
    m["live_price"] = lp

    # 데이장 동적 VWAP 및 고저가 즉시 갱신
    day_pv = safe_float(m.get("day_cum_pv"), 0.0)
    day_vol = safe_float(m.get("day_cum_vol"), 0.0)
    day_vwap = safe_float(m.get("day_vwap"), 0.0)
    if hm >= 1900 or hm <= 359:
        day_vwap = (day_pv + lp) / (day_vol + 1.0) if day_vol > 0.0 else lp
        m["day_vwap"] = round(day_vwap, 4)
        dh = max(safe_float(m.get("day_high"), lp), lp)
        dl_prev = safe_float(m.get("day_low"), lp)
        dl = min(dl_prev, lp) if dl_prev > 0.0 else lp
        m["day_high"] = round(dh, 4)
        m["day_low"] = round(dl, 4)
        m["day_amp_pct"] = (
            round(((dh - dl) / dl) * 100.0, 2) if dl > 0.0 and dh >= dl else 0.0
        )
    if day_vwap > 0.0:
        m["day_disp_curr"] = round(((lp - day_vwap) / day_vwap) * 100.0, 3)
        m["day_dyn_trap_m050"] = math.floor((day_vwap * 0.9950) * 100.0) / 100.0
        vwap_1907 = safe_float(m.get("vwap_1907"), 0.0)
        m["day_hyb_trap_m050"] = (
            min(math.floor(vwap_1907 * 100.0) / 100.0, m["day_dyn_trap_m050"])
            if vwap_1907 > 0.0
            else m["day_dyn_trap_m050"]
        )
        m["whipsaw_buy_p"] = math.floor((day_vwap / 1.005) * 100.0) / 100.0

    # 프리장 동적 VWAP 및 고저가 즉시 갱신
    pre_pv = safe_float(m.get("pre_cum_pv"), 0.0)
    pre_vol = safe_float(m.get("pre_cum_vol"), 0.0)
    pre_vwap = safe_float(m.get("pre_vwap"), 0.0)
    if 400 <= hm <= 929:
        pre_vwap = (pre_pv + lp) / (pre_vol + 1.0) if pre_vol > 0.0 else lp
        m["pre_vwap"] = round(pre_vwap, 4)
        ph = max(safe_float(m.get("pre_high"), lp), lp)
        pl_prev = safe_float(m.get("pre_low"), lp)
        pl = min(pl_prev, lp) if pl_prev > 0.0 else lp
        m["pre_high"] = round(ph, 4)
        m["pre_low"] = round(pl, 4)
        m["pre_amp_pct"] = (
            round(((ph - pl) / pl) * 100.0, 2) if pl > 0.0 and ph >= pl else 0.0
        )
    if pre_vwap > 0.0:
        m["pre_disp_curr"] = round(((lp - pre_vwap) / pre_vwap) * 100.0, 3)
        m["pre_dyn_trap_m050"] = math.floor((pre_vwap * 0.9950) * 100.0) / 100.0
        vwap_0407 = safe_float(m.get("vwap_0407"), 0.0)
        m["pre_hyb_trap_m050"] = (
            min(math.floor(vwap_0407 * 100.0) / 100.0, m["pre_dyn_trap_m050"])
            if vwap_0407 > 0.0
            else m["pre_dyn_trap_m050"]
        )

    # 정규장 동적 VWAP 및 고저가 즉시 갱신
    reg_pv = safe_float(m.get("reg_cum_pv"), 0.0)
    reg_vol = safe_float(m.get("reg_cum_vol"), 0.0)
    reg_vwap = safe_float(m.get("reg_vwap"), 0.0)
    if 930 <= hm <= 1559:
        reg_vwap = (reg_pv + lp) / (reg_vol + 1.0) if reg_vol > 0.0 else lp
        m["reg_vwap"] = round(reg_vwap, 4)
        rh = max(safe_float(m.get("reg_high"), lp), lp)
        rl_prev = safe_float(m.get("reg_low"), lp)
        rl = min(rl_prev, lp) if rl_prev > 0.0 else lp
        m["reg_high"] = round(rh, 4)
        m["reg_low"] = round(rl, 4)
        m["reg_amp_pct"] = (
            round(((rh - rl) / rl) * 100.0, 2) if rl > 0.0 and rh >= rl else 0.0
        )
    if reg_vwap > 0.0:
        m["reg_disp_curr"] = round(((lp - reg_vwap) / reg_vwap) * 100.0, 3)

    # 실시간 틱 하이킨 아시 즉시 합성
    live_ha = compute_realtime_tick_heikin_ashi(m.get("ha_state"), lp)
    m["is_ha_bull"] = bool(live_ha["is_ha_bull"])
    m["is_raw_bull"] = bool(live_ha["is_raw_bull"])
    m["ha_open"] = live_ha["ha_open"]
    m["ha_close"] = live_ha["ha_close"]
    m["cur_bar_open"] = live_ha["cur_open"]

    # 통합 세션 고가/저가 갱신
    sh = max(safe_float(m.get("session_high"), lp), lp)
    sl_prev = safe_float(m.get("session_low"), lp)
    sl = min(sl_prev, lp) if sl_prev > 0.0 else lp
    m["session_high"] = round(sh, 4)
    m["session_low"] = round(sl, 4)
    m["session_amp_pct"] = (
        round(((sh - sl) / sl) * 100.0, 2) if sl > 0.0 and sh >= sl else 0.0
    )
    return m


# ---------------------------------------------------------------------------
# 프리장 [실전] (04:07 EST) & 데이장 [모의] (19:07 EST) 공용 추세 주도주 선정기
# ---------------------------------------------------------------------------
def select_vwap_trend_leader(
    soxs_metrics: Dict[str, Any],
    soxl_metrics: Dict[str, Any],
    session_mode: str = "PRE",
) -> Dict[str, Any]:
    """
    🎯 1 & 🎯 3 & Case 97:
    데이장(session_mode="DAY", 19:07 EST 기준)과 프리장(session_mode="PRE", 04:07 EST 기준)
    양쪽에서 100% 동일한 공식으로 추세추종 주도주(leader_sym)를 선정한다.
    - 조건: C_init > VWAP_init (disp_init > 0.0%)
    - 양쪽 모두 disp_init > 0.0% 만족 시 초기 이격도(disp_init)가 더 큰 상위 1위 종목 선정
    """
    is_day = str(session_mode or "PRE").upper().startswith("DAY")
    vwap_key = "vwap_1907" if is_day else "vwap_0407"
    c_key = "c_1907" if is_day else "c_0407"
    disp_key = "disp_1907" if is_day else "disp_0407"
    fallback_vwap_key = "day_vwap" if is_day else "pre_vwap"
    fallback_disp_key = "day_disp_curr" if is_day else "pre_disp_curr"

    candidates: List[Tuple[str, float, float, float]] = []
    for sym, met in (("SOXS", soxs_metrics), ("SOXL", soxl_metrics)):
        if not isinstance(met, dict):
            continue
        vwap_val = safe_float(met.get(vwap_key), 0.0)
        c_val = safe_float(met.get(c_key), 0.0)
        disp_val = safe_float(met.get(disp_key), 0.0)

        if vwap_val <= 0.0 or c_val <= 0.0:
            vwap_val = safe_float(met.get(fallback_vwap_key), 0.0)
            c_val = safe_float(met.get("live_price"), 0.0)
            disp_val = safe_float(met.get(fallback_disp_key), 0.0)

        if vwap_val > 0.0 and c_val > vwap_val and disp_val > 0.0:
            candidates.append((sym, disp_val, vwap_val, c_val))

    if not candidates:
        return {
            "leader_sym": "",
            "vwap_init": 0.0,
            "c_init": 0.0,
            "disp_init": 0.0,
            "reason": "NO_SYMBOL_ABOVE_VWAP",
        }

    candidates.sort(key=lambda x: x[1], reverse=True)
    win_sym, win_disp, win_vwap, win_c = candidates[0]
    return {
        "leader_sym": win_sym,
        "vwap_init": round(win_vwap, 4),
        "c_init": round(win_c, 4),
        "disp_init": round(win_disp, 3),
        "reason": "LOCKED_TOP_DISP",
    }


# ---------------------------------------------------------------------------
# Case 39, 47, 55, 56 : 일봉 6개(count=6) 기반 5MA 진폭 · 예상 밴드 · 전일종가 등락률 연산
# ---------------------------------------------------------------------------
def compute_daily_5ma_and_expected_band(
    daily_candles: List[Dict[str, Any]],
    session_high: float,
    session_low: float,
    live_price: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Case 39 & Case 47 & Case 55 & Case 56:
    토스 Open API 일봉 6개(count=6)에서 당일 미완성 일봉을 배제하고
    직전 확정된 5영업일의 일평균 진폭(5MA, %), 전일 진폭(yesterday_amp), 직전 확정 일봉 종가(prev_close)를
    산출하여, 당일 예상 고가/저가 밴드 및 금일 고가/저가의 전일 종가 대비 등락률(%)을 반환한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_today = get_logical_trading_date_str(dt_now, rollover_hour=18)

    default_out = {
        "amp_5ma_pct": 6.50,
        "yesterday_amp_pct": 0.0,
        "prev_close": live_price,
        "high_chg_pct": 0.0,
        "low_chg_pct": 0.0,
        "expected_high": max(session_high, live_price),
        "expected_low": (
            min(session_low, live_price) if session_low > 0 else live_price
        ),
        "market_regime": "횡보장",
    }
    if not daily_candles or not isinstance(daily_candles, list):
        return default_out

    df_d = pd.DataFrame(daily_candles)
    if not {"timestamp", "highPrice", "lowPrice", "closePrice"}.issubset(
        set(df_d.columns)
    ):
        return default_out

    ts_s = pd.to_datetime(df_d["timestamp"], utc=True, errors="coerce")
    df_d = df_d.assign(
        date_str=ts_s.dt.tz_convert(EST_TZ).dt.strftime("%Y-%m-%d")
    )
    df_d = df_d.dropna(subset=["date_str"]).drop_duplicates(subset=["date_str"])
    df_d = df_d.sort_values("date_str", ascending=False)

    # Case 39 & Case 55: 첫 번째 봉(c0_dt)이 당일(logical_today) 미완성 캔들이면 배제
    completed_df = df_d[df_d["date_str"] < logical_today].head(5)
    if completed_df.empty:
        completed_df = df_d.iloc[1:6] if len(df_d) > 1 else df_d.head(5)

    highs = pd.to_numeric(completed_df["highPrice"], errors="coerce").fillna(
        0.0
    )
    lows = pd.to_numeric(completed_df["lowPrice"], errors="coerce").fillna(0.0)
    closes = pd.to_numeric(completed_df["closePrice"], errors="coerce").fillna(
        0.0
    )
    opens = (
        pd.to_numeric(completed_df["openPrice"], errors="coerce").fillna(closes)
        if "openPrice" in completed_df.columns
        else closes
    )

    valid_mask = lows > 0.0
    if not valid_mask.any():
        return default_out

    amps = ((highs[valid_mask] - lows[valid_mask]) / lows[valid_mask]) * 100.0
    amp_5ma = float(amps.mean()) if not amps.empty else 6.50
    yest_amp = float(amps.iloc[0]) if not amps.empty else amp_5ma

    # Case 56: 어제 등락 기반 상승/하락/횡보장 판별 및 전일 종가(prev_close) 추출
    yest_open = float(opens[valid_mask].iloc[0])
    yest_close = float(closes[valid_mask].iloc[0])
    yest_chg_pct = (
        ((yest_close - yest_open) / yest_open) * 100.0
        if yest_open > 0.0
        else 0.0
    )
    if yest_chg_pct >= 1.0:
        regime = "상승장"
    elif yest_chg_pct <= -1.0:
        regime = "하락장"
    else:
        regime = "횡보장"

    # 금일 세션 고가/저가의 전일 종가(yest_close) 대비 등락률(%) 산출 (/sync 보드용)
    eff_high = session_high if session_high > 0.0 else live_price
    eff_low = session_low if session_low > 0.0 else live_price
    high_chg_pct = (
        ((eff_high - yest_close) / yest_close) * 100.0
        if (yest_close > 0.0 and eff_high > 0.0)
        else 0.0
    )
    low_chg_pct = (
        ((eff_low - yest_close) / yest_close) * 100.0
        if (yest_close > 0.0 and eff_low > 0.0)
        else 0.0
    )

    # Case 47: 5MA 기반 동적 예상 고가/저가 연산
    exp_high = eff_low * (1.0 + (amp_5ma / 100.0)) if eff_low > 0.0 else 0.0
    exp_low = eff_high / (1.0 + (amp_5ma / 100.0)) if eff_high > 0.0 else 0.0

    return {
        "amp_5ma_pct": round(amp_5ma, 2),
        "yesterday_amp_pct": round(yest_amp, 2),
        "prev_close": round(yest_close, 4),
        "high_chg_pct": round(high_chg_pct, 2),
        "low_chg_pct": round(low_chg_pct, 2),
        "expected_high": round(exp_high, 2),
        "expected_low": round(exp_low, 2),
        "market_regime": regime,
    }


def calc_session_expected_band(
    sess_high: float, sess_low: float, amp_5ma_pct: float
) -> Tuple[float, float]:
    """
    # NEW: /avwap 레이더 보드의 프리장/정규장 세션별 팩트 고/저가와 5MA 진폭을 결합하여
    해당 세션의 동적 예상 저가 ~ 예상 고가 밴드를 반환한다 (미개장 시 0.00, 0.00 반환).
    """
    sh = safe_float(sess_high, 0.0)
    sl = safe_float(sess_low, 0.0)
    amp = max(0.0, safe_float(amp_5ma_pct, 0.0))
    if sh <= 0.0 or sl <= 0.0 or amp <= 0.0:
        return 0.0, 0.0
    exp_high = sl * (1.0 + (amp / 100.0))
    exp_low = sh / (1.0 + (amp / 100.0))
    return round(exp_low, 2), round(exp_high, 2)
