# =====================================================================
# FILE: quant_indicators_plugin.py
# 목적: 퀀트 상수 및 벡터/틱 지표 합성 플러그인 (quant_engine.py 1/2 분할본)
#       — 기술적 반등(⚡ OLD_IMM_CB) × 진성 상승종목(🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 이원화 매도 상수(초과 Case 99),
#         데이장 100% [모의] 전용 개시 7분 RSI>=50.0 · +1.30% 락온 · 40.0<=RSI<=70.0 리셋 상수(초과 Case 78, 93),
#         CME 18:00 EST 선물 세션 윈도우 산출(get_cme_session_window_est · 초과 Case 62, 90, 91),
#         토스 Open API QQQ 0.0초 실시간 10분/3분 변화율 벡터 산출(_compute_qqq_realtime_velocity_sync · 초과 Case 91, 92),
#         Wilder's RSI(14) + 하이킨 아시(Heikin-Ashi) 1분봉 + 세션별(데이/프리/정규장 09:30~) 정밀 누적 VWAP(초과 Case 98) 및
#         04:07 / 19:00 EST 이후 첫 RSI>=79.9 달성 타임스탬프(초과 Case 97) 벡터/틱 합성 엔진
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 quant_engine.py의 상수 및 벡터/틱 지표 연산 엔진을 quant_indicators_plugin.py로 무손실 분리
# MODIFIED: RSI 연산을 세션 절단 전 전체 캔들(df)에서 선행하여 EWM 워밍업 무결성 100% 락온 (Case 72)
# NEW: 폴링 사각지대(원인 D) 소각을 위한 Wilder's EWM 기저 상태(ewm_state_out) 추출 및 O(1) 실시간 틱 RSI 점화식(compute_realtime_tick_rsi) 탑재 (Case 73)
# NEW: 200봉/400봉 전체 100% 벡터화 하이킨 아시(Heikin-Ashi, ewm alpha=0.5) 기저 추출 및 O(1) 실시간 틱 하이킨 아시 양봉 판별식(compute_realtime_tick_heikin_ashi) 탑재 (Case 85)

import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

EST_TZ = ZoneInfo("America/New_York")

# 하위 호환 유지용 상수
AMP_BUFFER_ACTIVATE_NQ_PCT = 0.80
AMP_BUFFER_LEVERAGE_MULT = 3.0
AMP_BUFFER_PARTIAL_MULT = 2.1
AMP_BUFFER_REBOUND_PCT = 0.30

# 초과 Case 78 & Case 93 — 데이장(19:00 ~ 03:57 EST) 100% [모의] 스나이퍼 전용 추세·트레일링·리셋 전역 상수
DAY_OPEN_7M_RSI_THRESHOLD = 50.0       # 데이장 개시 후 7분간(09:00~09:06:59 KST / 19:00~19:06:59 EST) 1차 락온 종목의 최종 상승 판별 RSI 기준 (>= 50.0)
DAY_SIM_LOCK_PROFIT_RATE = 1.30        # 데이장 [모의] 추세추종 시 +1.30% 상승가 도달 시 트레일링 락온 (1.0130 * 0.9970 = +1.00% 보장)
DAY_SIM_FLOOR_PROFIT_RATE = 1.00       # 데이장 [모의] +1.30% 락온 후 보장되는 최소 익절 보존선 (+1.00%)
DAY_SIM_CALLBACK_PCT = 0.30            # 데이장 [모의] +1.30% 락온 후 최고가(Peak) 대비 콜백 하락폭 (-0.30%)
DAY_REARM_RESET_RSI_MIN = 40.0         # 데이장 [모의] 1회 조건 달성 후 리셋 허용 RSI 하한 (40.0 이상)
DAY_REARM_RESET_RSI_MAX = 70.0         # 데이장 [모의] 1회 조건 달성 후 리셋 허용 RSI 상한 (70.0 이하)

# 초과 Case 94 — 3차 안전망 최소 실시간 QQQ 역행 하한선 (0.80% 하드 락온)
MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80

# 초과 Case 97 — 선택지 B (👑 HYB_35_1ST) : 04:07 EST 이후 첫 RSI >= 79.9 상승 추세 종목 각인 및 07:00 ~ 08:14 EST RSI <= 35.0 추세추종 매수 상수
PRE_TREND_LEADER_SELECT_RSI = 79.9         # 04:07 EST / 19:00 EST 이후 당일 첫 상승 추세 종목 선정 임계치 (79.9 [80.0 클라이맥스 인지])
PRE_TREND_PULLBACK_RSI_THRESHOLD = 35.0    # 선정된 상승 추세 종목의 눌림목 매수 임계치 (RSI <= 35.0)
PRE_TREND_REARM_RESET_RSI = 40.0           # 프리장 추세눌림목 매매 익절 후 동일 눌림목 고점 재진입 방지를 위한 RSI 회복 기준 (> 40.0)

# 초과 Case 99 — 기술적 반등(⚡ OLD_IMM_CB) × 진성 상승종목 추세추종(🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 이원화 매도 전역 상수
TREND_PB_CALLBACK_PCT = 0.20               # 진성 상승종목 추세추종 매도 시 최고가(Peak) 대비 타이트 콜백 스탑 (-0.20%)
TREND_PB_LAUNCH_RATCHET_PCT = 1.30         # 진성 상승종목 양봉(C>=O & HA양봉) 상승 초입 스탑 유예 상한 (+1.30% 돌파 시 +1.00% 보존 잠금)
TREND_PB_CUSHION_FLOOR_RATE = 0.75         # 진성 상승종목 양봉 상승 초입(+1.00%~+1.29%) 후방 비상 쿠션 하한선 (+0.75%)


def compute_amp_buffer_target_price(
    anchor_high_price: float,
    spot_price: float,
    nq_amp: float,
    leverage_mult: float = AMP_BUFFER_LEVERAGE_MULT,
) -> tuple[float, float, float, float]:
    """하위 호환 유지를 위한 3배 레버리지 기대 하락폭 역산 헬퍼."""
    sp_fl = math.floor(float(spot_price or 0.0) * 100.0) / 100.0
    ah_fl = math.floor(float(anchor_high_price or 0.0) * 100.0) / 100.0
    nq_a = max(0.0, float(nq_amp or 0.0))

    if sp_fl <= 0.0 or nq_a <= 0.0:
        return 0.0, 0.0, 0.0, max(ah_fl, sp_fl)

    eff_anchor = max(ah_fl, sp_fl)
    expected_drop_pct = min(15.0, nq_a * float(leverage_mult or AMP_BUFFER_LEVERAGE_MULT))

    if eff_anchor > sp_fl * 1.003:
        target_drop_p = math.floor(eff_anchor * (1.0 - expected_drop_pct / 100.0) * 100.0) / 100.0
    else:
        rem_drop_pct = min(8.0, max(1.00, nq_a * 1.5))
        target_drop_p = math.floor(sp_fl * (1.0 - rem_drop_pct / 100.0) * 100.0) / 100.0
        eff_anchor = round(sp_fl / max(0.85, 1.0 - (expected_drop_pct - rem_drop_pct) / 100.0), 2)

    partial_drop_pct = min(12.0, nq_a * AMP_BUFFER_PARTIAL_MULT)
    partial_from_anchor = math.floor(eff_anchor * (1.0 - partial_drop_pct / 100.0) * 100.0) / 100.0
    partial_from_spot = math.floor(sp_fl * 0.9900 * 100.0) / 100.0
    partial_drop_p = max(0.01, min(partial_from_anchor, partial_from_spot))

    return (
        max(0.01, target_drop_p),
        partial_drop_p,
        round(expected_drop_pct, 2),
        round(eff_anchor, 2),
    )


def get_cme_session_window_est(now_est: datetime = None) -> tuple[bool, datetime]:
    """
    현재 미국 동부 시각(now_est) 기준으로 CME 나스닥 100 선물(NQ=F) 시장 개장 여부(is_cme_open)와
    현재 활성(또는 17:00~17:59 EST 마감 휴장 중 직전 마감된) 선물 세션의 시작 시각(nq_session_start_est, 18:00:00 EST)을 반환합니다 (초과 Case 62, 90, 91).
    - 개장 구간: 일요일 18:00 EST ~ 금요일 16:59 EST (단, 월~목 17:00 ~ 17:59 EST 일일 점검 휴장 제외)
    - 주말 휴장: 금요일 17:00 EST ~ 일요일 17:59 EST
    """
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    wk = now_est.weekday()  # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    est_hm = now_est.hour * 100 + now_est.minute

    # 1) 주말 휴장: 금요일 17:00 이후, 토요일 종일, 일요일 18:00 이전 -> 직전 목요일 18:00 EST 개장(금요일 마감) 세션 반환
    if (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800):
        days_back = (wk - 3) if wk >= 4 else (wk + 4)
        last_start = (now_est - timedelta(days=days_back)).replace(
            hour=18, minute=0, second=0, microsecond=0
        )
        return False, last_start

    # 2) 평일(월~목) 17:00 ~ 17:59 EST 일일 점검 휴장 (한국시간 06:00 ~ 06:59 KST, 1시간)
    #    -> 방금 17:00 EST(06:00 KST)에 마감된 전일 18:00 EST 개장 세션 시작 시각을 그대로 유지!
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
    초과 Case 91 & Case 92 [해법 2 방법 A] — 토스 Open API에서 조회한 나스닥 100 현물 ETF(QQQ)의
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
    세션별 누적 VWAP(데이/프리/정규장 09:30~), Wilder's RSI(14), 하이킨 아시(Heikin-Ashi) 1분봉,
    기준 고가(session_anchor_high) 및 당일 04:07 EST / 19:00 EST 이후 첫 RSI >= 79.9 달성 타임스탬프(초과 Case 97, 98)를
    1개의 데이터프레임에서 동시 산출하여 CPU 블로킹 및 API 중복 호출을 원천 차단합니다.
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

    rs = avg_gain / avg_loss
    rsi_series = 100.0 - (100.0 / (1.0 + rs))
    rsi_series = np.where(
        (avg_loss <= 0.0) & (avg_gain <= 0.0),
        50.0,
        np.where(avg_loss <= 0.0, 100.0, rsi_series),
    )
    rsi_series = pd.Series(rsi_series, index=df.index).replace([np.inf, -np.inf], 100.0).fillna(50.0)

    # 2. 전체 데이터(df)를 대상으로 하이킨 아시(Heikin-Ashi) 1분봉 100% 벡터 연산 (for 루프 금지)
    ha_close = (df["openPrice"] + df["highPrice"] + df["lowPrice"] + df["closePrice"]) / 4.0
    ha_seed = ha_close.shift(1)
    ha_seed.iloc[0] = (float(df["openPrice"].iloc[0]) + float(ha_close.iloc[0])) / 2.0
    ha_open = ha_seed.ewm(alpha=0.5, adjust=False).mean()

    # VWAP 및 당일 세션/직전 세션 기준 고가 분리
    session_df = df[df.index >= session_start_est].copy()
    pre_session_df = df[df.index < session_start_est]

    recent_60_high = float(df["highPrice"].tail(60).max()) if not df.empty else 0.0
    sess_high = float(session_df["highPrice"].max()) if not session_df.empty else 0.0
    prev_sess_ref = (
        max(
            float(pre_session_df["highPrice"].tail(15).max()),
            float(pre_session_df["closePrice"].iloc[-1]),
        )
        if not pre_session_df.empty
        else 0.0
    )
    anchor_high_val = max(recent_60_high, sess_high, prev_sess_ref)

    # 초과 Case 97 — 당일 04:07 ~ 08:14 EST 프리장 및 19:00 ~ 03:57 EST 데이장에서
    # RSI >= 79.9 를 최초로 달성한 분봉 시각 및 피크 RSI 100% 벡터 추출 (장중 봇 재기동 시 자동 복원용)
    pre_0407_start = session_start_est + timedelta(hours=9, minutes=7)
    pre_0814_end = session_start_est + timedelta(hours=13, minutes=14, seconds=59)
    day_0357_end = session_start_est + timedelta(hours=8, minutes=57, seconds=59)

    first_0407_799_ts = 0.0
    first_0407_799_time_str = ""
    first_0407_799_rsi = 0.0
    first_day_799_ts = 0.0
    first_day_799_time_str = ""
    first_day_799_rsi = 0.0

    if not session_df.empty:
        rsi_sess = rsi_series.loc[session_df.index]
        rsi_0407_win = rsi_sess[(rsi_sess.index >= pre_0407_start) & (rsi_sess.index <= pre_0814_end)]
        climax_0407 = rsi_0407_win[rsi_0407_win >= PRE_TREND_LEADER_SELECT_RSI]
        if not climax_0407.empty:
            first_idx_0407 = climax_0407.index[0]
            first_0407_799_ts = float(first_idx_0407.timestamp())
            first_0407_799_time_str = first_idx_0407.strftime("%H:%M:%S")
            first_0407_799_rsi = float(climax_0407.max())

        rsi_day_win = rsi_sess[(rsi_sess.index >= session_start_est) & (rsi_sess.index <= day_0357_end)]
        climax_day = rsi_day_win[rsi_day_win >= PRE_TREND_LEADER_SELECT_RSI]
        if not climax_day.empty:
            first_idx_day = climax_day.index[0]
            first_day_799_ts = float(first_idx_day.timestamp())
            first_day_799_time_str = first_idx_day.strftime("%H:%M:%S")
            first_day_799_rsi = float(climax_day.max())

    # 초과 Case 98 — 토스 차트의 세션별 보라색 VWAP 선과 100% 일치하도록
    # 데이장(19:00~03:59), 프리장(04:00~09:29), 정규장(09:30~15:59) 누적 VWAP을 각각 벡터 내적으로 산출!
    def _calc_vwap_vec(sub_df: pd.DataFrame) -> float:
        if sub_df is None or sub_df.empty:
            return 0.0
        vol_sum = float(sub_df["volume"].sum())
        if vol_sum <= 0.0:
            return float(sub_df["closePrice"].iloc[-1])
        tp_s = (sub_df["highPrice"] + sub_df["lowPrice"] + sub_df["closePrice"]) / 3.0
        return float((tp_s * sub_df["volume"]).sum() / vol_sum)

    vwap_val = 0.0
    day_vwap_val = 0.0
    pre_vwap_val = 0.0
    reg_vwap_val = 0.0

    if not session_df.empty:
        full_sess_vwap = _calc_vwap_vec(session_df)
        day_end_est = session_start_est + timedelta(hours=8, minutes=59, seconds=59)
        day_sub = session_df[session_df.index <= day_end_est]
        pre_sub = session_df.between_time("04:00", "09:29")
        reg_sub = session_df.between_time("09:30", "15:59")

        day_vwap_val = _calc_vwap_vec(day_sub) or full_sess_vwap
        pre_vwap_val = _calc_vwap_vec(pre_sub)
        reg_vwap_val = _calc_vwap_vec(reg_sub)

        latest_ts = session_df.index[-1]
        latest_hm = latest_ts.hour * 100 + latest_ts.minute
        # 1) 09:30 ~ 15:59 EST 정규장 구간: 09:30 EST 개장 이후 정규장 누적 VWAP 반환 (초과 Case 98 15:00~15:58 VWAP 탈출용)
        if 930 <= latest_hm <= 1559:
            vwap_val = reg_vwap_val if reg_vwap_val > 0.0 else full_sess_vwap
        # 2) 04:07 ~ 09:29 EST 프리장 본진 구간: 프리장 누적 VWAP 반환
        elif 407 <= latest_hm <= 929:
            vwap_val = pre_vwap_val if pre_vwap_val > 0.0 else full_sess_vwap
        # 3) 19:00 ~ 04:06 EST 데이장 및 03:58~04:06 1주 휩소 노출 작전 구간: 데이장 누적 VWAP 반환
        else:
            vwap_val = day_vwap_val if day_vwap_val > 0.0 else full_sess_vwap

    # 메인 루프에서 O(1) 실시간 틱 RSI 및 실시간 틱 하이킨 아시를 합성할 수 있도록 직전 확정 분봉(iloc[-2]) 기저 추출 (Case 73, 85, 93, 97, 98)
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
        ewm_state_out["session_anchor_high"] = float(anchor_high_val)
        ewm_state_out["recent_60m_high"] = float(recent_60_high)
        ewm_state_out["day_vwap"] = float(day_vwap_val)
        ewm_state_out["pre_vwap"] = float(pre_vwap_val)
        ewm_state_out["reg_vwap"] = float(reg_vwap_val)
        ewm_state_out["logical_date"] = pre_0407_start.strftime("%Y-%m-%d")
        ewm_state_out["first_0407_799_ts"] = first_0407_799_ts
        ewm_state_out["first_0407_799_time_str"] = first_0407_799_time_str
        ewm_state_out["first_0407_799_rsi"] = first_0407_799_rsi
        ewm_state_out["first_day_799_ts"] = first_day_799_ts
        ewm_state_out["first_day_799_time_str"] = first_day_799_time_str
        ewm_state_out["first_day_799_rsi"] = first_day_799_rsi
        ewm_state_out["ready"] = True

    raw_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 0.0
    rsi_val = max(0.01, min(100.0, raw_rsi)) if not rsi_series.empty else 0.0

    return rsi_val, vwap_val


def compute_realtime_tick_rsi(
    live_price: float, ewm_state: dict, fallback_rsi: float = 0.0, period: int = 14
) -> float:
    """
    메인 루프(assassin_loop) 전용 O(1) 무부하 실시간 틱 RSI 합성 엔진 (Case 73).
    직전 확정 분봉(t-1)의 Wilder's EWM 기저 상태에 실시간 현재가(live_price)를 대입하여
    추가 API 호출 0회로 순간적인 대칭 클라이맥스(매수종목 RSI <= 30.0 & 반대종목 RSI >= 79.9) 및
    07:00 ~ 08:14 EST 선행 상승종목 추세눌림목(RSI <= 35.0)을 즉시 포착합니다.
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
    if p_float > float(ewm_state.get("session_anchor_high", 0.0)):
        ewm_state["session_anchor_high"] = p_float

    live_ha_open = (prev_ha_o + prev_ha_c) / 2.0 if (prev_ha_o > 0.0 and prev_ha_c > 0.0) else cur_o
    live_ha_close = (cur_o + cur_h + cur_l + p_float) / 4.0
    is_ha_bull = bool(live_ha_close > live_ha_open)

    ewm_state["ha_open"] = live_ha_open
    ewm_state["ha_close"] = live_ha_close
    ewm_state["ha_bull"] = is_ha_bull
    return is_ha_bull, live_ha_open, live_ha_close
