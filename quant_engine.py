# =====================================================================
# FILE: quant_engine.py
# 목적: 양방향 대칭 클라이맥스 듀얼 스나이퍼(SOXS/SOXL) 통합 시드 원장,
#       NQ=F 18:00 EST 신세션 하드 컷오프 + fast_info 당일 실제 고/저가 강제 병합(해법 1 · 1/2차 방어 및 3차 임계치 max(진폭*0.5, 0.8%) 산출 기준),
#       토스 Open API QQQ 0.0초 실시간 3차 역행 비교 기준가(1.5초 프로빙) + 10분/3분 단기 변화율 산출(해법 2 방법 A) 및
#       Wilder's RSI(14) + 하이킨 아시(Heikin-Ashi) 1분봉 벡터/틱 합성 엔진
# =====================================================================
# MODIFIED: RSI 연산을 세션 절단 전 전체 캔들(df)에서 선행하여 EWM 워밍업 무결성 100% 락온 (Case 72)
# NEW: 폴링 사각지대(원인 D) 소각을 위한 Wilder's EWM 기저 상태(ewm_state_out) 추출 및 O(1) 실시간 틱 RSI 점화식(compute_realtime_tick_rsi) 탑재 (Case 73)
# NEW: 200봉 전체 100% 벡터화 하이킨 아시(Heikin-Ashi, ewm alpha=0.5) 기저 추출 및 O(1) 실시간 틱 하이킨 아시 양봉 판별식(compute_realtime_tick_heikin_ashi) 탑재 (Case 85)
# MODIFIED: 기존 +0.64% / +1.00% 선택 모드를 전면 폐기하고 단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0)' 상수로 100% 단일화 (Case 85)
# NEW: 초과 Case 61 & Case 89 — MacroDataCache 갱신 주기를 15.0초로 가속하고 최근 10분 변화율(nq_vel_10m) 및 3분 변화율(nq_vel_3m) 벡터 산출 탑재 (🚀 NQ_VEL_BYPASS 지원)
# NEW: 초과 Case 62 & Case 90 — CME 나스닥 100 선물(NQ=F) 18:00 EST 신세션 경계 하드 컷오프(get_cme_session_window_est)로 금요일 과열 진폭 이월 원천 소각
# NEW: 초과 Case 91 [해법 1] — NQ=F fast_info의 당일 실제 고가(day_high)·저가(day_low)를 강제 병합하여 개장 첫 틱 밑꼬리 저가(예: 29,226) 누락 원천 차단
# NEW: 초과 Case 91 [해법 2 방법 A] — 토스 Open API QQQ 실시간 현재가·1분봉으로 최근 10분/3분 변화율(±0.20% 동일 임계치)을 최우선 산출
# NEW: 초과 Case 92 — 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.8%)은 NQ=F 선물 진폭(nq_entry_amp)을 기준으로 산출하고,
#      해당 임계치를 적용하는 진입 기준가 및 실시간 역행 비교가는 토스 Open API QQQ 0.0초 실시간 현재가(get_stage3_reference_price, 보유중 1.5초 프로빙)로 전면 대체
# MODIFIED: 듀얼 스나이퍼 화이트리스트 ALLOWED_SYMBOLS = ("SOXS", "SOXL") 확장 및 임시 파일 os.replace 원자적 저장 (Case 06, 09, 30)
# MODIFIED: 통합 시드 풀(Unified Seed Pool) 및 실제 발생 순수익금((매도가-매수가)*체결수량 - 왕복수수료 0.14%) 기반 복리 정산 동기화 (Case 76)
# MODIFIED: lock_all_symbols_macro 호출 시 수동 보유 종목(MANUAL) 표식 보존 및 과열장(NQ>=2.0%) 수동 청산 후 자동 매매 오작동 해금 차단 (Case 50, 66, 82)

import os
import json
import time
import math
import asyncio
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from toss_api import GlobalThrottle

EST_TZ = ZoneInfo("America/New_York")


def get_cme_session_window_est(now_est: datetime = None) -> tuple[bool, datetime]:
    """
    현재 미국 동부 시각(now_est) 기준으로 CME 나스닥 100 선물(NQ=F) 시장 개장 여부(is_cme_open)와
    현재 활성 선물 세션의 시작 시각(nq_session_start_est, 18:00:00 EST)을 반환합니다 (초과 Case 62, 90).
    - 개장 구간: 일요일 18:00 EST ~ 금요일 16:59 EST (단, 월~목 17:00 ~ 17:59 EST 일일 점검 휴장 제외)
    - 주말 휴장: 금요일 17:00 EST ~ 일요일 17:59 EST
    """
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    wk = now_est.weekday()  # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    est_hm = now_est.hour * 100 + now_est.minute

    # 1) 주말 휴장: 금요일 17:00 이후, 토요일 종일, 일요일 18:00 이전
    if (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800):
        days_back = (wk - 3) if wk >= 4 else (wk + 4)
        last_start = (now_est - timedelta(days=days_back)).replace(
            hour=18, minute=0, second=0, microsecond=0
        )
        return False, last_start

    # 2) 평일(월~목) 17:00 ~ 17:59 EST 일일 점검 휴장 (1시간)
    if wk in (0, 1, 2, 3) and (1700 <= est_hm < 1800):
        last_start = (now_est - timedelta(days=1)).replace(
            hour=18, minute=0, second=0, microsecond=0
        )
        return False, last_start

    # 3) 일~목 18:00 ~ 23:59 EST: 당일 18:00:00 EST에 개장한 신세션 활성 중
    if est_hm >= 1800:
        cur_start = now_est.replace(hour=18, minute=0, second=0, microsecond=0)
        return True, cur_start

    # 4) 월~금 00:00 ~ 16:59 EST: 전일 18:00:00 EST에 개장한 현세션 활성 중
    cur_start = (now_est - timedelta(days=1)).replace(
        hour=18, minute=0, second=0, microsecond=0
    )
    return True, cur_start


def _compute_qqq_realtime_velocity_sync(
    q_price: float, q_candles: list, now_est: datetime
) -> tuple[bool, float, float, float]:
    """
    NEW: 초과 Case 91 & Case 92 [해법 2 방법 A] — 토스 Open API에서 조회한 나스닥 100 현물 ETF(QQQ)의
    0.0초 실시간 현재가(q_price)와 당일 세션 1분봉(q_candles)을 결합하여,
    롱(SOXL)·숏(SOXS) 자체 노이즈에 전혀 영향받지 않고 지연 0.0초로
    최근 10분 변화율(q_vel_10m), 최근 3분 변화율(q_vel_3m) 및 유효 실시간 QQQ 가격(curr_q)을 벡터 산출합니다.
    """
    if not q_candles:
        return False, 0.0, 0.0, 0.0

    wk = now_est.weekday()
    est_hm = now_est.hour * 100 + now_est.minute

    # 주말 휴장(금 17:00 ~ 일 18:59 EST) 또는 평일 장외/선물단독 구간(17:00 ~ 18:59 EST)에는
    # QQQ 미국 데이장이 열리지 않으므로 False를 반환하여 NQ=F로 자동 폴백
    if (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1900):
        return False, 0.0, 0.0, 0.0
    if 1700 <= est_hm < 1900:
        return False, 0.0, 0.0, 0.0

    if now_est.hour >= 19:
        etf_session_start = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
    else:
        etf_session_start = (now_est - timedelta(days=1)).replace(
            hour=19, minute=0, second=0, microsecond=0
        )

    df = pd.DataFrame(q_candles)
    if df.empty or "timestamp" not in df.columns:
        return False, 0.0, 0.0, 0.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="last")]
    df.sort_index(ascending=True, inplace=True)

    for col in ["openPrice", "closePrice"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[df["closePrice"] > 0.0].copy()
    if df.empty:
        return False, 0.0, 0.0, 0.0

    df["openPrice"] = np.where(df["openPrice"] > 0.0, df["openPrice"], df["closePrice"])

    # 당일 데이마켓 개장(19:00 EST) 이후 캔들만 분리하여 전일 종가 간섭 원천 차단
    df_sess = df[df.index >= etf_session_start].copy()
    if df_sess.empty:
        return False, 0.0, 0.0, 0.0

    # 휴장일 등으로 인해 최신 1분봉이 현재 시각보다 30분 이상 오래된 경우 무효 처리
    newest_bar_ts = df_sess.index[-1]
    if (now_est - newest_bar_ts) > timedelta(minutes=30):
        return False, 0.0, 0.0, 0.0

    # 실시간 현재가(q_price)를 최신 진행 분봉 종가에 즉시 동기화 (지연 0.0초)
    if q_price is not None and float(q_price) > 0.0:
        last_idx = df_sess.index[-1]
        df_sess.at[last_idx, "closePrice"] = float(q_price)

    curr_q = float(df_sess["closePrice"].iloc[-1])
    if curr_q <= 0.0:
        return False, 0.0, 0.0, 0.0

    first_open = float(df_sess["openPrice"].iloc[0]) or float(df_sess["closePrice"].iloc[0])
    ref_10m = float(df_sess["closePrice"].iloc[-11]) if len(df_sess) >= 11 else first_open
    ref_3m = float(df_sess["closePrice"].iloc[-4]) if len(df_sess) >= 4 else first_open

    vel_10m = ((curr_q - ref_10m) / ref_10m * 100.0) if ref_10m > 0.0 else 0.0
    vel_3m = ((curr_q - ref_3m) / ref_3m * 100.0) if ref_3m > 0.0 else 0.0
    return True, float(vel_10m), float(vel_3m), float(curr_q)


def calculate_rsi_1m(
    candles_json: list,
    session_start_est: datetime,
    period: int = 14,
    current_price: float = 0.0,
    ewm_state_out: dict = None,
) -> tuple[float, float]:
    """
    Pandas 기반 백그라운드 벡터 연산.
    VWAP, Wilder's RSI(14), 하이킨 아시(Heikin-Ashi) 1분봉을 1개의 데이터프레임에서 동시 산출하여
    CPU 블로킹 및 API 중복 호출을 원천 차단합니다.
    current_price 실시간 종가 보정 및 직전 확정 분봉(iloc[-2])의 Wilder's EWM + Heikin-Ashi 기저 상태를 ewm_state_out에 기록합니다.
    """
    if not candles_json:
        return 0.0, 0.0

    df = pd.DataFrame(candles_json)
    if df.empty or "timestamp" not in df.columns:
        return 0.0, 0.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="last")]
    df.sort_index(ascending=True, inplace=True)

    # 하이킨 아시(Heikin-Ashi) 벡터 연산을 위해 openPrice 컬럼을 포함하여 5대 OHLCV 정규화
    for col in ["openPrice", "highPrice", "lowPrice", "closePrice", "volume"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[df["closePrice"] > 0.0].copy()
    if df.empty:
        return 0.0, 0.0

    # openPrice 결측 시 종가로 안전 폴백
    df["openPrice"] = np.where(df["openPrice"] > 0.0, df["openPrice"], df["closePrice"])
    df["highPrice"] = np.where(df["highPrice"] > 0.0, df["highPrice"], df["closePrice"])
    df["lowPrice"] = np.where(df["lowPrice"] > 0.0, df["lowPrice"], df["closePrice"])

    # 실시간 현재가(current_price)가 주입된 경우 최신 진행 분봉(iloc[-1])의 종가·고가·저가 즉시 동기화
    if current_price is not None and current_price > 0.0:
        last_idx = df.index[-1]
        cur_p_float = float(current_price)
        df.at[last_idx, "closePrice"] = cur_p_float
        if float(df.at[last_idx, "openPrice"]) <= 0.0:
            df.at[last_idx, "openPrice"] = cur_p_float
        if float(df.at[last_idx, "highPrice"]) < cur_p_float:
            df.at[last_idx, "highPrice"] = cur_p_float
        cur_low = float(df.at[last_idx, "lowPrice"])
        if cur_low <= 0.0 or cur_low > cur_p_float:
            df.at[last_idx, "lowPrice"] = cur_p_float

    # 1. 전체 데이터(df)를 대상으로 Wilder's RSI(14) 선행 벡터 연산 (Case 23, 72)
    delta = df["closePrice"].diff()
    gain = delta.where(delta > 0.0, 0.0)
    loss = -delta.where(delta < 0.0, 0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()

    # 2. 전체 데이터(df)를 대상으로 하이킨 아시(Heikin-Ashi) 1분봉 100% 벡터 연산 (for 루프 금지)
    ha_close = (df["openPrice"] + df["highPrice"] + df["lowPrice"] + df["closePrice"]) / 4.0
    ha_seed = ha_close.shift(1)
    ha_seed.iloc[0] = (float(df["openPrice"].iloc[0]) + float(ha_close.iloc[0])) / 2.0
    ha_open = ha_seed.ewm(alpha=0.5, adjust=False).mean()

    # 메인 루프에서 O(1) 실시간 틱 RSI 및 실시간 틱 하이킨 아시를 합성할 수 있도록 직전 확정 분봉(iloc[-2]) 기저 추출 (Case 73, 85)
    if isinstance(ewm_state_out, dict):
        cur_o = float(df["openPrice"].iloc[-1])
        cur_h = float(df["highPrice"].iloc[-1])
        cur_l = float(df["lowPrice"].iloc[-1])
        last_ha_o = float(ha_open.iloc[-1])
        last_ha_c = float(ha_close.iloc[-1])

        if len(df) >= 2:
            ewm_state_out["prev_close"] = float(df["closePrice"].iloc[-2])
            ewm_state_out["prev_avg_gain"] = float(avg_gain.iloc[-2])
            ewm_state_out["prev_avg_loss"] = float(avg_loss.iloc[-2])
            ewm_state_out["prev_ha_open"] = float(ha_open.iloc[-2])
            ewm_state_out["prev_ha_close"] = float(ha_close.iloc[-2])
        else:
            ewm_state_out["prev_close"] = float(df["closePrice"].iloc[-1])
            ewm_state_out["prev_avg_gain"] = float(avg_gain.iloc[-1])
            ewm_state_out["prev_avg_loss"] = float(avg_loss.iloc[-1])
            ewm_state_out["prev_ha_open"] = last_ha_o
            ewm_state_out["prev_ha_close"] = last_ha_c

        ewm_state_out["cur_bar_open"] = cur_o
        ewm_state_out["cur_bar_high"] = cur_h
        ewm_state_out["cur_bar_low"] = cur_l
        ewm_state_out["ha_open"] = last_ha_o
        ewm_state_out["ha_close"] = last_ha_c
        ewm_state_out["ha_bull"] = bool(last_ha_c > last_ha_o)
        ewm_state_out["ready"] = True

    rs = avg_gain / avg_loss
    rsi_series = 100.0 - (100.0 / (1.0 + rs))
    rsi_series = np.where(
        (avg_loss <= 0.0) & (avg_gain <= 0.0),
        50.0,
        np.where(avg_loss <= 0.0, 100.0, rsi_series),
    )
    rsi_series = pd.Series(rsi_series, index=df.index).replace([np.inf, -np.inf], 100.0).fillna(50.0)

    raw_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 0.0
    rsi_val = max(0.01, min(100.0, raw_rsi)) if not rsi_series.empty else 0.0

    # VWAP 산출 시에만 논리적 거래일 기준(session_df)으로 분리
    session_df = df[df.index >= session_start_est].copy()
    vwap_val = 0.0
    if not session_df.empty:
        tp = (session_df["highPrice"] + session_df["lowPrice"] + session_df["closePrice"]) / 3.0
        pv = tp * session_df["volume"]
        cum_vol = session_df["volume"].sum()
        vwap_val = float(pv.sum() / cum_vol) if cum_vol > 0.0 else 0.0

    return rsi_val, vwap_val


def compute_realtime_tick_rsi(
    live_price: float, ewm_state: dict, fallback_rsi: float = 0.0, period: int = 14
) -> float:
    """
    메인 루프(assassin_loop) 전용 O(1) 무부하 실시간 틱 RSI 합성 엔진 (Case 73).
    직전 확정 분봉(t-1)의 Wilder's EWM 기저 상태에 실시간 현재가(live_price)를 대입하여
    추가 API 호출 0회로 순간적인 대칭 클라이맥스(매수종목 RSI <= 30.0 & 반대종목 RSI >= 79.9)를 즉시 포착합니다.
    """
    if live_price is None or live_price <= 0.0 or not isinstance(ewm_state, dict):
        return float(fallback_rsi or 0.0)

    if not ewm_state.get("ready", False):
        return float(fallback_rsi or 0.0)

    prev_close = float(ewm_state.get("prev_close", 0.0))
    prev_avg_gain = float(ewm_state.get("prev_avg_gain", 0.0))
    prev_avg_loss = float(ewm_state.get("prev_avg_loss", 0.0))

    if prev_close <= 0.0 or not math.isfinite(prev_avg_gain) or not math.isfinite(prev_avg_loss):
        return float(fallback_rsi or 0.0)

    alpha = 1.0 / float(period)
    delta = float(live_price) - prev_close
    cur_gain = delta if delta > 0.0 else 0.0
    cur_loss = -delta if delta < 0.0 else 0.0

    live_avg_gain = (1.0 - alpha) * prev_avg_gain + alpha * cur_gain
    live_avg_loss = (1.0 - alpha) * prev_avg_loss + alpha * cur_loss

    if live_avg_loss <= 0.0:
        if live_avg_gain <= 0.0:
            return 50.0
        return 100.0

    rs = live_avg_gain / live_avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    if not math.isfinite(rsi):
        return float(fallback_rsi or 50.0)

    return max(0.01, min(100.0, float(rsi)))


def compute_realtime_tick_heikin_ashi(
    live_price: float, ewm_state: dict
) -> tuple[bool, float, float]:
    """
    메인 루프(assassin_loop) 전용 O(1) 무부하 실시간 틱 하이킨 아시(Heikin-Ashi) 1분봉 합성 엔진 (Case 85).
    직전 확정 분봉(t-1)의 HA_Open·HA_Close 기저 상태와 현재 진행 분봉의 시가·고가·저가에
    실시간 현재가(live_price)를 대입하여 추가 API 호출 0회로 실시간 양봉 유지 여부(is_ha_bull)를 판별합니다.
    """
    if not isinstance(ewm_state, dict) or not ewm_state.get("ready", False):
        return True, 0.0, 0.0

    if live_price is None or live_price <= 0.0:
        return (
            bool(ewm_state.get("ha_bull", True)),
            float(ewm_state.get("ha_open", 0.0)),
            float(ewm_state.get("ha_close", 0.0)),
        )

    p_float = float(live_price)
    prev_ha_o = float(ewm_state.get("prev_ha_open", p_float))
    prev_ha_c = float(ewm_state.get("prev_ha_close", p_float))
    cur_o = float(ewm_state.get("cur_bar_open", p_float)) or p_float
    cur_h = max(float(ewm_state.get("cur_bar_high", p_float)) or p_float, p_float)
    cur_l_raw = float(ewm_state.get("cur_bar_low", p_float))
    cur_l = min(cur_l_raw, p_float) if cur_l_raw > 0.0 else p_float

    ewm_state["cur_bar_high"] = cur_h
    ewm_state["cur_bar_low"] = cur_l

    live_ha_open = (prev_ha_o + prev_ha_c) / 2.0 if (prev_ha_o > 0.0 and prev_ha_c > 0.0) else cur_o
    live_ha_close = (cur_o + cur_h + cur_l + p_float) / 4.0
    is_ha_bull = bool(live_ha_close > live_ha_open)

    ewm_state["ha_open"] = live_ha_open
    ewm_state["ha_close"] = live_ha_close
    ewm_state["ha_bull"] = is_ha_bull
    return is_ha_bull, live_ha_open, live_ha_close


class MacroDataCache:
    # MODIFIED: 초과 Case 61, 62, 89, 90, 91, 92 —
    #           1) [해법 1] CME 18:00 EST 신세션 하드 컷오프 + fast_info 당일 실제 고가(day_high)/저가(day_low) 강제 병합으로
    #              1·2차 과열 방어선(>=2.0%) 및 3차 동적 임계치 max(nq_entry_amp * 0.5, 0.8%)의 기준인 NQ=F 당일 진폭을 100% 정확히 산출
    #           2) [해법 2 방법 A & Case 92] 3차 역행 안전망의 진입 기준가·실시간 비교가(get_stage3_reference_price) 및
    #              10분/3분 변화율은 토스 Open API QQQ 0.0초 실시간 시세로 산출 (보유 중 1.5초 프로빙, 폴백 NQ=F)
    # 튜플 구조: (curr_val, high_val, low_val, updated_ts, nq_vel_10m, nq_vel_3m)
    _nq_cache_data = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    _nq_session_id = ""
    _nq_vel_source = "NQ선물"
    _qqq_curr_price = 0.0
    _qqq_price_ts = 0.0
    _nq_cache_lock = asyncio.Lock()

    @classmethod
    def _is_refresh_needed(cls, now_ts: float, last_ts: float) -> bool:
        if last_ts <= 0.0:
            return True
        return (now_ts - last_ts) > 15.0

    @classmethod
    async def _fetch_qqq_velocity_async(cls, now_est: datetime) -> tuple[bool, float, float, float]:
        """토스 Open API(GlobalThrottle.active_client)를 통해 QQQ 0.0초 실시간 현재가 및 10분/3분 변화율을 산출합니다 (초과 Case 91, 92)."""
        client = getattr(GlobalThrottle, "active_client", None)
        if client is None or not hasattr(client, "get_qqq_realtime_snapshot"):
            return False, 0.0, 0.0, 0.0
        try:
            q_price, q_candles = await asyncio.wait_for(
                client.get_qqq_realtime_snapshot(count=25), timeout=3.5
            )
            if not q_candles:
                return False, 0.0, 0.0, float(q_price or 0.0)
            return await asyncio.to_thread(
                _compute_qqq_realtime_velocity_sync, q_price, q_candles, now_est
            )
        except Exception as e:
            print(f"🚨 [QQQ 실시간 변화율 산출 방어] {e}", flush=True)
            return False, 0.0, 0.0, 0.0

    @classmethod
    async def _refresh_cache_if_needed(cls, force_refresh: bool = False):
        async with cls._nq_cache_lock:
            now = time.time()
            now_est = datetime.now(EST_TZ)
            is_cme_open, nq_session_start_est = get_cme_session_window_est(now_est)
            cur_sess_id = nq_session_start_est.strftime("%Y-%m-%d_%H%M")

            # 초과 Case 90 — 18:00 EST CME 선물 신세션 롤오버 감지 시 이전 세션(금요일/전일) 캐시 즉시 0점 소각
            session_rolled = (cls._nq_session_id != cur_sess_id)
            if session_rolled:
                cls._nq_session_id = cur_sess_id
                cls._nq_cache_data = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                cls._qqq_curr_price = 0.0
                cls._qqq_price_ts = 0.0

            need_refresh = force_refresh or session_rolled or cls._is_refresh_needed(now, cls._nq_cache_data[3])
            if not need_refresh:
                return

            def _fetch_nq():
                tkr = yf.Ticker("NQ=F")
                fast_last_p = 0.0
                fast_day_high = 0.0
                fast_day_low = 0.0
                try:
                    fi = getattr(tkr, "fast_info", None)
                    if fi is not None:
                        raw_lp = fi.get("lastPrice") if hasattr(fi, "get") else getattr(fi, "last_price", 0.0)
                        raw_dh = fi.get("dayHigh") if hasattr(fi, "get") else getattr(fi, "day_high", 0.0)
                        raw_dl = fi.get("dayLow") if hasattr(fi, "get") else getattr(fi, "day_low", 0.0)
                        if raw_lp is not None and float(raw_lp) > 0.0:
                            fast_last_p = float(raw_lp)
                        if raw_dh is not None and float(raw_dh) > 0.0:
                            fast_day_high = float(raw_dh)
                        if raw_dl is not None and float(raw_dl) > 0.0:
                            fast_day_low = float(raw_dl)
                except Exception:
                    pass

                df = None
                try:
                    df = tkr.history(period="2d", interval="1m", prepost=True)
                except Exception:
                    df = None

                if df is None or df.empty:
                    df = tkr.history(period="5d", interval="1m", prepost=True)

                if df is None or df.empty:
                    if fast_last_p > 0.0:
                        h_init = max(fast_last_p, fast_day_high) if fast_day_high > 0.0 else fast_last_p
                        l_init = min(fast_last_p, fast_day_low) if fast_day_low > 0.0 else fast_last_p
                        return fast_last_p, h_init, l_init, 0.0, 0.0
                    return 0.0, 0.0, 0.0, 0.0, 0.0

                df.index = pd.to_datetime(df.index, utc=True).tz_convert(EST_TZ)
                df = df[~df.index.duplicated(keep="last")]
                df.sort_index(ascending=True, inplace=True)

                for col in ["Open", "High", "Low", "Close"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

                df = df[df["Close"] > 0.0].copy()
                if df.empty:
                    if fast_last_p > 0.0:
                        return fast_last_p, fast_last_p, fast_last_p, 0.0, 0.0
                    return 0.0, 0.0, 0.0, 0.0, 0.0

                if "Open" in df.columns:
                    df["Open"] = np.where(df["Open"] > 0.0, df["Open"], df["Close"])
                else:
                    df["Open"] = df["Close"]

                # 직전 구세션(금요일/전일) 고가·저가 추출 (18:00 EST 직후 fast_info 미갱신 여부 대조용)
                df_before = df[df.index < nq_session_start_est]
                prev_sess_h, prev_sess_l = 0.0, 0.0
                if not df_before.empty:
                    td_prev = df_before.index.to_series().diff()
                    gaps_prev = td_prev[td_prev >= pd.Timedelta(minutes=45)]
                    df_prev_sess = df_before[df_before.index >= gaps_prev.index[-1]] if not gaps_prev.empty else df_before
                    if not df_prev_sess.empty:
                        prev_sess_h = float(df_prev_sess["High"].max())
                        prev_sess_l = float(df_prev_sess["Low"].min())

                # 초과 Case 62 & Case 90 — 현재 활성 CME 선물 세션 시작 시각(18:00:00 EST) 이전의 전일/금요일 캔들 100% 절삭
                df_session = df[df.index >= nq_session_start_est].copy()

                # fast_info의 day_high/day_low가 구세션(금요일) 값 그대로인지 판별하여 신세션 값일 때만 병합 허용
                is_fast_hl_stale = (
                    prev_sess_h > 0.0
                    and prev_sess_l > 0.0
                    and abs(fast_day_high - prev_sess_h) < 0.01
                    and abs(fast_day_low - prev_sess_l) < 0.01
                )

                if df_session.empty:
                    if is_cme_open:
                        seed_p = fast_last_p if fast_last_p > 0.0 else float(df["Close"].iloc[-1])
                        h_seed, l_seed = seed_p, seed_p
                        if not is_fast_hl_stale and seed_p > 0.0:
                            if fast_day_high > 0.0 and abs(fast_day_high - seed_p) / seed_p < 0.10:
                                h_seed = max(h_seed, fast_day_high)
                            if fast_day_low > 0.0 and abs(fast_day_low - seed_p) / seed_p < 0.10:
                                l_seed = min(l_seed, fast_day_low)
                        return seed_p, h_seed, l_seed, 0.0, 0.0
                    else:
                        time_diffs = df.index.to_series().diff()
                        gaps = time_diffs[time_diffs >= pd.Timedelta(minutes=45)]
                        if not gaps.empty:
                            df_session = df[df.index >= gaps.index[-1]].copy()
                        else:
                            df_session = df.copy()

                if df_session.empty:
                    return 0.0, 0.0, 0.0, 0.0, 0.0

                # 세션 내부에 45분 이상 추가 휴장 갭이 존재하는 경우 최신 구간만 분리 (Case 62)
                time_diffs_inner = df_session.index.to_series().diff()
                gaps_inner = time_diffs_inner[time_diffs_inner >= pd.Timedelta(minutes=45)]
                if not gaps_inner.empty:
                    df_session = df_session[df_session.index >= gaps_inner.index[-1]].copy()

                curr_val = float(df_session["Close"].iloc[-1])
                high_val = float(df_session["High"].max())
                low_val = float(df_session["Low"].min())

                # 초과 Case 91 [해법 1] — CME 개장 중 fast_info의 최신가(last_price) 및
                # 거래소 집계 당일 실제 고가(day_high)·저가(day_low)를 강제 병합하여
                # 개장 첫 틱 밑꼬리 저가(예: 29,226.00) 누락으로 인한 진폭 왜곡을 100% 원천 소각!
                if is_cme_open and curr_val > 0.0:
                    if fast_last_p > 0.0 and abs(fast_last_p - curr_val) / curr_val < 0.05:
                        curr_val = fast_last_p
                        high_val = max(high_val, fast_last_p)
                        low_val = min(low_val, fast_last_p) if low_val > 0.0 else fast_last_p

                    if not is_fast_hl_stale:
                        if fast_day_high > 0.0 and abs(fast_day_high - curr_val) / curr_val < 0.12:
                            high_val = max(high_val, fast_day_high)
                        if fast_day_low > 0.0 and abs(fast_day_low - curr_val) / curr_val < 0.12:
                            low_val = min(low_val, fast_day_low) if low_val > 0.0 else fast_day_low

                # NQ=F 자체 10분 변화율 및 3분 단기 변화율 (QQQ 미개장 시간대 폴백용)
                first_open = float(df_session["Open"].iloc[0]) or float(df_session["Close"].iloc[0])
                ref_10m = float(df_session["Close"].iloc[-11]) if len(df_session) >= 11 else first_open
                ref_3m = float(df_session["Close"].iloc[-4]) if len(df_session) >= 4 else first_open

                vel_10m = ((curr_val - ref_10m) / ref_10m * 100.0) if ref_10m > 0.0 else 0.0
                vel_3m = ((curr_val - ref_3m) / ref_3m * 100.0) if ref_3m > 0.0 else 0.0
                return curr_val, high_val, low_val, vel_10m, vel_3m

            try:
                nq_task = asyncio.wait_for(asyncio.to_thread(_fetch_nq), timeout=5.0)
                qqq_task = cls._fetch_qqq_velocity_async(now_est)
                (c, h, l, nq_v10, nq_v3), (qqq_ok, q_v10, q_v3, q_curr) = await asyncio.gather(
                    nq_task, qqq_task
                )

                # 초과 Case 91 & Case 92 — 토스 Open API QQQ 0.0초 실시간 변화율 및 현재가 캐시 동기화
                if qqq_ok:
                    final_v10, final_v3 = q_v10, q_v3
                    cls._nq_vel_source = "QQQ실시간"
                    if q_curr > 0.0:
                        cls._qqq_curr_price = float(q_curr)
                        cls._qqq_price_ts = now
                else:
                    final_v10, final_v3 = nq_v10, nq_v3
                    cls._nq_vel_source = "NQ선물"

                if l > 0.0:
                    cls._nq_cache_data = (c, h, l, now, final_v10, final_v3)
                else:
                    prev = cls._nq_cache_data
                    cls._nq_cache_data = (prev[0], prev[1], prev[2], now, final_v10, final_v3)
            except Exception as e:
                prev = cls._nq_cache_data
                cls._nq_cache_data = (prev[0], prev[1], prev[2], now, prev[4], prev[5])
                print(f"🚨 [NQ=F/QQQ 캐시 갱신 방어] {e}", flush=True)

    @classmethod
    async def get_cached_nq_data(cls, force_refresh: bool = False) -> tuple[float, float, float]:
        """
        1·2차 과열 방어선(>=2.0%) 및 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.8%)의 기준인
        나스닥 100 선물지수(NQ=F)의 (curr, high, low)를 반환합니다 (초과 Case 61, 62, 91, 92).
        """
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]

    @classmethod
    async def get_stage3_reference_price(
        cls, force_refresh: bool = False, realtime_probing: bool = False
    ) -> tuple[float, str]:
        """
        NEW: 초과 Case 92 — 나스닥 100 선물(NQ=F) 진폭으로 산출된 3차 진입 임계치 max(nq_entry_amp * 0.5, 0.8%)를
        10분 지연 없이 시차 0.0초로 적용하기 위한 토스 Open API QQQ 실시간 현재가 반환 헬퍼.
        - 포지션 보유 중(realtime_probing=True)에는 1.5초 주기로 토스 QQQ 실시간 현재가를 직접 프로빙합니다.
        - 평시 대기 중(realtime_probing=False)에는 15.0초 캐시를 사용하며, QQQ 미수신 시간대(18:00~18:59 EST 등)에만 NQ=F로 폴백합니다.
        """
        now_est = datetime.now(EST_TZ)
        wk = now_est.weekday()
        est_hm = now_est.hour * 100 + now_est.minute
        is_etf_closed_window = (
            (wk == 4 and est_hm >= 1700)
            or (wk == 5)
            or (wk == 6 and est_hm < 1900)
            or (1700 <= est_hm < 1900)
        )

        if not is_etf_closed_window:
            now_ts = time.time()
            ttl = 1.5 if realtime_probing else 15.0
            if force_refresh or cls._qqq_curr_price <= 0.0 or (now_ts - cls._qqq_price_ts) >= ttl:
                client = getattr(GlobalThrottle, "active_client", None)
                if client is not None and hasattr(client, "get_current_price"):
                    try:
                        q_p = await asyncio.wait_for(client.get_current_price("QQQ"), timeout=2.5)
                        if q_p is not None and float(q_p) > 0.0:
                            cls._qqq_curr_price = float(q_p)
                            cls._qqq_price_ts = now_ts
                    except Exception:
                        pass
            if cls._qqq_curr_price > 0.0:
                return float(cls._qqq_curr_price), "QQQ실시간"

        nq_c, _, _ = await cls.get_cached_nq_data(force_refresh=force_refresh)
        return float(nq_c), "NQ선물"

    @classmethod
    async def get_cached_nq_velocity(cls, force_refresh: bool = False) -> tuple[float, float]:
        """최근 10분 변화율(nq_vel_10m)과 3분 변화율(nq_vel_3m)을 반환합니다 (초과 Case 89, 91)."""
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return float(cls._nq_cache_data[4]), float(cls._nq_cache_data[5])

    @classmethod
    def get_cached_nq_velocity_source(cls) -> str:
        """현재 단기 변화율 산출에 사용 중인 실시간 데이터 소스('QQQ실시간' 또는 'NQ선물')를 반환합니다 (초과 Case 91)."""
        return str(cls._nq_vel_source or "NQ선물")

    @classmethod
    async def is_nq_velocity_favorable_surge(
        cls, symbol: str, surge_threshold: float = 0.20, force_refresh: bool = False
    ) -> tuple[bool, float, float]:
        """
        보유 종목(SOXS 또는 SOXL) 기준으로 나스닥 100(토스 QQQ 0.0초 실시간 우선, 폴백 NQ=F)이
        유리한 방향으로 ±surge_threshold(기본 0.20%) 이상 급등 또는 급락 중인지 판별하여
        하이킨 아시 음봉 바이패스(🚀 NQ_VEL_BYPASS) 여부를 반환합니다 (초과 Case 89, 91).
        """
        vel_10m, vel_3m = await cls.get_cached_nq_velocity(force_refresh=force_refresh)
        th = abs(float(surge_threshold or 0.20))
        if symbol == "SOXS":
            is_surge = (min(vel_10m, vel_3m) <= -th)
        elif symbol == "SOXL":
            is_surge = (max(vel_10m, vel_3m) >= th)
        else:
            is_surge = False
        return bool(is_surge), vel_10m, vel_3m

    @classmethod
    async def get_nq_sniper_signal(
        cls, force_refresh: bool = False
    ) -> tuple[str, float, str, float, float, float]:
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        c, h, l = cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]
        return ("DUAL80", float(cls._nq_cache_data[4]), f"{c:.2f}", c, h, l)


class AssassinLedger:
    # 양방향 대칭 클라이맥스 듀얼 스나이퍼 화이트리스트 락온 (Case 09, 54)
    ALLOWED_SYMBOLS = ("SOXS", "SOXL")
    # 단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0 × 🚀 NQ_VEL_BYPASS)' 상수로 100% 단일화 (Case 68, 85, 89)
    DEFAULT_TARGET_PROFIT_RATE = 1.00
    ALLOWED_TARGET_RATES = (1.00,)
    TRAIL_ARM_PROFIT_RATE = 1.00          # +1.00% 도달 시 하이킨 아시 트레일링 점화(Arming)
    TRAIL_FLOOR_LOCK_RATE = 1.00          # 트레일링 점화 즉시 최소 +1.00% 익절 보존선 하드 락온
    TRAIL_DROP_CALLBACK_PCT = 0.30        # 최고가(Peak) 대비 -0.30% 콜백 발생 시 즉시 익절 스윕
    TRAIL_SAFETY_CAP_RATE = 5.00          # 매수 체결 직후 거래소에 선장전하는 상단 안전 캡 (+5.00% LIMIT)
    NQ_VELOCITY_SURGE_PCT = 0.20          # QQQ/NQ 10분/3분 변화율 ±0.20% 이상 급등락 시 HA음봉 바이패스 임계치 (Case 89, 91)
    ROUND_TRIP_COMM_PCT = 0.14
    ONE_WAY_COMM_RATE = 0.0007
    MIN_BUDGET_FLOOR = 10.0

    @classmethod
    def _validate_symbol(cls, symbol: str) -> str:
        sym = str(symbol).strip().upper()
        if sym not in cls.ALLOWED_SYMBOLS:
            raise ValueError(f"🚨 [Case 09 위반] 허용되지 않은 제3종목 접근 영구 차단: {sym}")
        return sym

    @classmethod
    def _get_file_path(cls, symbol: str) -> str:
        sym = cls._validate_symbol(symbol)
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"AssassinLedger_{sym}.json")

    # 단일 하이킨 아시 트레일링 모드 락온에 따라 점화 기준 수익률 1.00% 단일 반환 (Case 68)
    @classmethod
    def normalize_target_rate(cls, rate_val=None) -> float:
        return 1.00

    # 최소 익절 보존선(+1.00%)에서 왕복 협의 수수료(0.14%)를 공제한 최소 보장 순수익률 (+0.86%) 반환
    @classmethod
    def get_net_compound_rate(cls, target_profit_rate: float = 1.00) -> float:
        return round(cls.TRAIL_FLOOR_LOCK_RATE - cls.ROUND_TRIP_COMM_PCT, 2)

    @classmethod
    async def get_state(
        cls, symbol: str
    ) -> tuple[float, float, float, str, bool, bool, float, str, str, float, float, float]:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath):
                    return (
                        0.0, 100.0, cls.DEFAULT_TARGET_PROFIT_RATE, "", False, True, 0.0, "", "", 0.0, 0.0, 0.0,
                    )
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            data = {}
                        raw_rate = cls.normalize_target_rate(
                            data.get("target_profit_rate", cls.DEFAULT_TARGET_PROFIT_RATE)
                        )
                        return (
                            float(data.get("last_buy_price", 0.0)),
                            float(data.get("budget", 100.0)),
                            raw_rate,
                            str(data.get("last_session_id", "")),
                            bool(data.get("is_session_done", False)),
                            bool(data.get("is_active", True)),
                            float(data.get("target_sell_price", 0.0)),
                            str(data.get("sell_order_id", "")),
                            str(data.get("entry_session", "")),
                            float(data.get("entry_time", 0.0)),
                            float(data.get("nq_entry_price", 0.0)),
                            float(data.get("nq_entry_amp", 0.0)),
                        )
                except Exception:
                    return (
                        0.0, 100.0, cls.DEFAULT_TARGET_PROFIT_RATE, "", False, True, 0.0, "", "", 0.0, 0.0, 0.0,
                    )
            return await asyncio.to_thread(_read)

    @classmethod
    async def get_buy_order_id(cls, symbol: str) -> str:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath):
                    return ""
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        return str(data.get("buy_order_id", "")) if isinstance(data, dict) else ""
                except Exception:
                    return ""
            return await asyncio.to_thread(_read)

    @classmethod
    async def get_entry_trend(cls, symbol: str) -> str:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath):
                    return ""
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        return str(data.get("entry_trend", "")) if isinstance(data, dict) else ""
                except Exception:
                    return ""
            return await asyncio.to_thread(_read)

    # 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 전용 목표 매도가(VWAP) 조회
    @classmethod
    async def get_whipsaw_target_price(cls, symbol: str) -> float:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath):
                    return 0.0
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            return 0.0
                        return float(data.get("whipsaw_target_price", 0.0))
                except Exception:
                    return 0.0
            return await asyncio.to_thread(_read)

    # 복리 모드(is_compound) 활성화 상태 조회 (기본값 True)
    @classmethod
    async def get_is_compound(cls, symbol: str) -> bool:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath):
                    return True
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            return True
                        return bool(data.get("is_compound", True))
                except Exception:
                    return True
            return await asyncio.to_thread(_read)

    # Case 76: 하이킨 아시 트레일링 익절(+1.00% ~ +5.00%+) / 손절 / 수동개입 / 1주휩소 작전 모두
    #          실제 발생한 순수익금((실제매도가 - 실제매수가) * 실제체결수량 - 왕복수수료 0.14%)을 기반으로 통합 복리 시드(budget) 정밀 갱신
    @classmethod
    def calc_compound_next_budget(
        cls,
        current_budget: float,
        is_take_profit: bool,
        entry_price: float = 0.0,
        exit_price: float = 0.0,
        filled_qty: float = 0.0,
        target_profit_rate: float = 1.00,
    ) -> tuple[float, float, float]:
        base_budget = float(current_budget) if (current_budget and math.isfinite(current_budget)) else 100.0
        if base_budget < cls.MIN_BUDGET_FLOOR:
            base_budget = cls.MIN_BUDGET_FLOOR

        # 1순위 (핵심): 실제 체결 단가(entry_price, exit_price)와 실제 체결 수량(filled_qty)이 확인된 경우
        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            invested_usd = float(entry_price) * float(filled_qty)
            returned_usd = float(exit_price) * float(filled_qty)
            gross_pnl_usd = returned_usd - invested_usd
            comm_usd = (invested_usd + returned_usd) * cls.ONE_WAY_COMM_RATE
            net_delta_usd = round(gross_pnl_usd - comm_usd, 2)
            new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
            return new_budget, net_delta_usd, round(comm_usd, 2)

        # 2순위: 체결 단가(entry_price, exit_price)만 확보되고 수량이 누락된 경우 시드 기준 추정 수량(최소 1주)으로 실수익 역산
        if entry_price > 0.0 and exit_price > 0.0:
            est_qty = max(1, math.floor(base_budget / float(entry_price)))
            invested_usd = float(entry_price) * est_qty
            returned_usd = float(exit_price) * est_qty
            gross_pnl_usd = returned_usd - invested_usd
            comm_usd = (invested_usd + returned_usd) * cls.ONE_WAY_COMM_RATE
            net_delta_usd = round(gross_pnl_usd - comm_usd, 2)
            new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
            return new_budget, net_delta_usd, round(comm_usd, 2)

        # 3순위 폴백: API 체결 내역 인덱싱이 완전히 지연된 경우 최소 보존 익절률(+1.00%, 순+0.86%)/수수료 기준 정산
        if is_take_profit:
            net_comp_rate = cls.get_net_compound_rate(target_profit_rate)
            net_delta_usd = round(base_budget * (net_comp_rate / 100.0), 2)
            comm_usd = round(base_budget * (cls.ONE_WAY_COMM_RATE * 2.0), 2)
            new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
            return new_budget, net_delta_usd, comm_usd

        comm_usd = round(base_budget * (cls.ONE_WAY_COMM_RATE * 2.0), 2)
        net_delta_usd = -comm_usd
        new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
        return new_budget, net_delta_usd, comm_usd

    @classmethod
    async def save_state(
        cls,
        symbol: str,
        price: float = None,
        budget: float = None,
        target_profit_rate: float = None,
        last_session_id: str = None,
        is_session_done: bool = None,
        is_active: bool = None,
        target_sell_price: float = None,
        buy_order_id: str = None,
        sell_order_id: str = None,
        entry_session: str = None,
        entry_time: float = None,
        nq_entry_price: float = None,
        nq_entry_amp: float = None,
        entry_trend: str = None,
        is_compound: bool = None,
        whipsaw_target_price: float = None,
    ):
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _write():
                data = {}
                if os.path.exists(filepath):
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            loaded = json.load(f)
                            if isinstance(loaded, dict):
                                data = loaded
                    except Exception:
                        pass

                data["target_profit_rate"] = cls.DEFAULT_TARGET_PROFIT_RATE

                if "is_compound" not in data:
                    data["is_compound"] = True

                if price is not None:
                    data["last_buy_price"] = price
                if budget is not None:
                    data["budget"] = round(float(budget), 2)
                if last_session_id is not None:
                    data["last_session_id"] = last_session_id
                if is_session_done is not None:
                    data["is_session_done"] = is_session_done
                if is_active is not None:
                    data["is_active"] = is_active
                if target_sell_price is not None:
                    data["target_sell_price"] = target_sell_price
                if buy_order_id is not None:
                    data["buy_order_id"] = buy_order_id
                if sell_order_id is not None:
                    data["sell_order_id"] = sell_order_id
                if entry_session is not None:
                    data["entry_session"] = entry_session
                if entry_time is not None:
                    data["entry_time"] = entry_time
                if nq_entry_price is not None:
                    data["nq_entry_price"] = nq_entry_price
                if nq_entry_amp is not None:
                    data["nq_entry_amp"] = nq_entry_amp
                if entry_trend is not None:
                    data["entry_trend"] = entry_trend
                if is_compound is not None:
                    data["is_compound"] = bool(is_compound)
                if whipsaw_target_price is not None:
                    data["whipsaw_target_price"] = float(whipsaw_target_price)

                for obsolete_key in [
                    "pre_first_flag", "force_downgrade", "force_downgrade_0_6", "is_stage_3",
                    "cond_order_id", "session_mode", "profit_mode",
                ]:
                    data.pop(obsolete_key, None)

                tmp_path = f"{filepath}.tmp.{os.getpid()}"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp_path, filepath)

            await asyncio.to_thread(_write)

    # 통합 시드 풀(Unified Seed Pool) 및 전술 제어반 공통 설정(시드, 복리여부, 마스터전원) 양쪽 원장 동시 동기화 (Case 76)
    @classmethod
    async def sync_global_settings(
        cls,
        budget: float = None,
        target_profit_rate: float = None,
        is_compound: bool = None,
        is_active: bool = None,
    ):
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.save_state(
                sym,
                budget=budget,
                target_profit_rate=cls.DEFAULT_TARGET_PROFIT_RATE,
                is_compound=is_compound,
                is_active=is_active,
            )

    # 매크로 1·2·3차 방어선 작동 시 SOXS와 SOXL 양쪽 장부 동시 잠금하되,
    # 현재 수동 보유 중(entry_session == "MANUAL")인 종목은 force_all=False일 때 MANUAL 표식을 보존 (Case 50, 82)
    @classmethod
    async def lock_all_symbols_macro(cls, last_session_id: str = None, force_all: bool = False):
        for sym in cls.ALLOWED_SYMBOLS:
            st = await cls.get_state(sym)
            is_active_manual_hold = (
                not force_all
                and str(st[8] or "") == "MANUAL"
                and (float(st[0] or 0.0) > 0.0 or float(st[6] or 0.0) > 0.0 or bool(st[7]))
            )
            kwargs = {
                "buy_order_id": "",
                "is_session_done": True,
                "entry_session": "MANUAL" if is_active_manual_hold else "CANCELED_MACRO",
                "whipsaw_target_price": 0.0,
            }
            if last_session_id is not None:
                kwargs["last_session_id"] = last_session_id
            await cls.save_state(sym, **kwargs)

    # 프리장 골든타임 내 익절 완료 또는 04:07 휩소 정리 완료 시 양쪽 장부 동시 무한 재출동 락 해제하되,
    # 당일 NQ=F 진폭이 이미 2.0% 이상인 과열 장세라면 자동 스나이퍼 재해금을 차단하고 CANCELED_MACRO 유지 (Case 52, 66, 80, 84)
    @classmethod
    async def unlock_all_symbols_for_reentry(cls):
        nq_h = float(MacroDataCache._nq_cache_data[1] or 0.0)
        nq_l = float(MacroDataCache._nq_cache_data[2] or 0.0)
        nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0
        if nq_amp >= 2.0:
            await cls.lock_all_symbols_macro(force_all=False)
            return

        for sym in cls.ALLOWED_SYMBOLS:
            st = await cls.get_state(sym)
            if st[8] != "CANCELED_MACRO":
                await cls.save_state(sym, is_session_done=False, whipsaw_target_price=0.0)

    @classmethod
    async def reset_all_ledgers(cls):
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.save_state(
                sym, price=0.0, target_sell_price=0.0, buy_order_id="", sell_order_id="",
                is_session_done=False, entry_session="", entry_time=0.0,
                nq_entry_price=0.0, nq_entry_amp=0.0, entry_trend="",
                whipsaw_target_price=0.0,
            )
