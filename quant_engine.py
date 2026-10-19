# =====================================================================
# FILE: quant_engine.py
# 목적: 양방향 대칭 클라이맥스 + 07:00~08:14 선행 상승종목 추세눌림목(👑 HYB_35_1ST) 듀얼 스나이퍼(SOXS/SOXL) 통합 시드 원장,
#       기술적 반등(⚡ OLD_IMM_CB) × 진성 상승종목(🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 이원화 매도 상수(초과 Case 99),
#       NQ=F 18:00 EST 신세션 하드 컷오프 + fast_info 당일 실제 고/저가 강제 병합(해법 1 · 1/2차 방어 및 3차 임계치 max(진폭*0.5, 0.80%) 산출 기준),
#       데이장 개시 1분(09:00:00~09:00:59 KST) [모의] 전용 진폭×3.0배 역산 하락가 버퍼 산출 엔진(초과 Case 93 · 🛡️ BUFFER_WAIT),
#       토스 Open API QQQ 0.0초 실시간 3차 역행 비교 기준가(1.5초 프로빙 · 최소 0.80% 역행 하한 강제) + 10분/3분 단기 변화율 산출(해법 2 방법 A) 및
#       Wilder's RSI(14) + 하이킨 아시(Heikin-Ashi) 1분봉 + 세션별(데이/프리/정규장 09:30~) 정밀 누적 VWAP(초과 Case 98) 벡터/틱 합성 엔진
# =====================================================================
# MODIFIED: RSI 연산을 세션 절단 전 전체 캔들(df)에서 선행하여 EWM 워밍업 무결성 100% 락온 (Case 72)
# NEW: 폴링 사각지대(원인 D) 소각을 위한 Wilder's EWM 기저 상태(ewm_state_out) 추출 및 O(1) 실시간 틱 RSI 점화식(compute_realtime_tick_rsi) 탑재 (Case 73)
# NEW: 200봉/400봉 전체 100% 벡터화 하이킨 아시(Heikin-Ashi, ewm alpha=0.5) 기저 추출 및 O(1) 실시간 틱 하이킨 아시 양봉 판별식(compute_realtime_tick_heikin_ashi) 탑재 (Case 85)
# MODIFIED: 기존 +0.64% / +1.00% 선택 모드를 전면 폐기하고 단일 +1.00% 점화 기반 이원화 하이킨 아시 트레일링 상수로 락온 (Case 85, 99)
# NEW: 초과 Case 61 & Case 89 — MacroDataCache 갱신 주기를 15.0초로 가속하고 최근 10분 변화율(nq_vel_10m) 및 3분 변화율(nq_vel_3m) 벡터 산출 탑재 (🚀 NQ_VEL_BYPASS 지원)
# NEW: 초과 Case 62 & Case 90 — CME 나스닥 100 선물(NQ=F) 18:00 EST 신세션 경계 하드 컷오프(get_cme_session_window_est)로 금요일 과열 진폭 이월 원천 소각
# NEW: 초과 Case 91 [해법 1] — NQ=F fast_info의 당일 실제 고가(day_high)·저가(day_low)를 강제 병합하여 개장 첫 틱 밑꼬리 저가(예: 29,226) 누락 원천 차단
# NEW: 초과 Case 91 [해법 2 방법 A] — 토스 Open API QQQ 실시간 현재가·1분봉으로 최근 10분/3분 변화율(±0.20% 동일 임계치)을 최우선 산출
# NEW: 초과 Case 92 — 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)은 NQ=F 선물 진폭(nq_entry_amp)을 기준으로 산출하고,
#      해당 임계치를 적용하는 진입 기준가 및 실시간 역행 비교가는 토스 Open API QQQ 0.0초 실시간 현재가(get_stage3_reference_price, 보유중 1.5초 프로빙)로 전면 대체
# MODIFIED: 초과 Case 93 — 데이장 개시 1분(09:00:00~09:00:59 KST) [모의] 전용 충격 전 기준 고가(session_anchor_high) 벡터 추출 및
#           3배 레버리지 역산 예상 하락가 버퍼 산출 함수(compute_amp_buffer_target_price) 유지
# NEW: 초과 Case 94 — 포지션 자체 손실률 기반 즉시 손절 절대 금지 및 실시간 QQQ 최소 0.80% 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80) 강제
# MODIFIED: 초과 Case 82, 95 & 96 — lock_all_symbols_macro 호출 시 수동 보유 종목(MANUAL)과 03:58 ~ 04:06 EST 진행 중인
#           1주 휩소 노출 작전(WHIPSAW_1SH)을 보호하되, 04:07 EST 이후 미체결 buy_order_id 잔재는 즉시 ""로 소각하여 장부 고착 무한 루프를 원천 차단
# NEW: 초과 Case 97 — 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 종목(FIRST_799 · leader_sym) 자동 각인·복원용 벡터 타임스탬프 추출 및
#      07:00 ~ 08:14 EST 해당 상승 종목 RSI <= 35.0 추세추종 눌림목 매수(👑 HYB_35_1ST) 상수 탑재
# NEW: 초과 Case 98 — 정규장 마감 1시간 전(15:00 ~ 15:58 EST) 미익절 물량 VWAP 도달 즉시 덤핑 탈출(🛟 VWAP_ESCAPE)을 위해
#      calculate_rsi_1m 내부에서 세션별(19:00 데이장 / 04:00 프리장 / 09:30 정규장) 정밀 누적 VWAP 분리 벡터 연산 탑재
# NEW: 초과 Case 99 — 기술적 반등(⚡ OLD_IMM_CB : -0.30% 콜백) vs 진성 상승종목 추세추종(🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020 : 초입 +1.30% 전 양봉 홀딩 후 -0.20% 타이트 스탑) 이원화 매도 상수 탑재
# MODIFIED: 듀얼 스나이퍼 화이트리스트 ALLOWED_SYMBOLS = ("SOXS", "SOXL") 확장 및 임시 파일 os.replace 원자적 저장 (Case 06, 09, 30)
# MODIFIED: 통합 시드 풀(Unified Seed Pool) 및 실제 발생 순수익금((매도가-매수가)*체결수량 - 왕복수수료 0.14%) 기반 복리 정산 동기화 (Case 76)

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

# 초과 Case 93 — 데이장 개시 1분(09:00:00 ~ 09:00:59 KST) [모의] 스나이퍼 전용 진폭 역산 하락가 버퍼(BUFFER_WAIT) 전역 상수
AMP_BUFFER_ACTIVATE_NQ_PCT = 0.80      # 데이장 개시 1분 중 NQ=F 당일 진폭이 0.80% 이상일 때 [모의] 역산 버퍼 가동
AMP_BUFFER_LEVERAGE_MULT = 3.0         # NQ=F 진폭 대비 3배 레버리지 ETF 기대 하락폭 역산 배수 (3.0x)
AMP_BUFFER_PARTIAL_MULT = 2.1          # 최소 부분 흡수 인정 배수 (2.1x, 포착가 대비 최소 -1.00% 추가 하락 병행)
AMP_BUFFER_REBOUND_PCT = 0.30          # 역산 하락가 도달 후 최저점 대비 +0.30% 양봉 반등 시 모의 가상 체결

# 초과 Case 94 — 3차 안전망 최소 실시간 QQQ 역행 하한선 (0.80% 하드 락온)
MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80

# 초과 Case 97 — 선택지 B (👑 HYB_35_1ST) : 04:07 EST 이후 첫 RSI >= 79.9 상승 추세 종목 각인 및 07:00 ~ 08:14 EST RSI <= 35.0 추세추종 매수 상수
PRE_TREND_LEADER_SELECT_RSI = 79.9         # 04:07 EST 이후 당일 첫 상승 추세 종목(leader_sym) 선정 임계치 (79.9 [80.0 클라이맥스 인지])
PRE_TREND_PULLBACK_RSI_THRESHOLD = 35.0    # 07:00 ~ 08:14 EST 선정된 상승 추세 종목의 눌림목 매수 임계치 (RSI <= 35.0)
PRE_TREND_REARM_RESET_RSI = 40.0           # 추세눌림목 매매 익절 후 동일 눌림목 고점 재진입 방지를 위한 RSI 회복 기준 (> 40.0)

# NEW: 초과 Case 99 — 기술적 반등(⚡ OLD_IMM_CB) × 진성 상승종목 추세추종(🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 이원화 매도 전역 상수
TREND_PB_CALLBACK_PCT = 0.20               # 진성 상승종목 추세추종 매도 시 최고가(Peak) 대비 타이트 콜백 스탑 (-0.20%)
TREND_PB_LAUNCH_RATCHET_PCT = 1.30         # 진성 상승종목 양봉(C>=O & HA양봉) 상승 초입 스탑 유예 상한 (+1.30% 돌파 시 +1.00% 보존 잠금)
TREND_PB_CUSHION_FLOOR_RATE = 0.75         # 진성 상승종목 양봉 상승 초입(+1.00%~+1.29%) 후방 비상 쿠션 하한선 (+0.75%)


def compute_amp_buffer_target_price(
    anchor_high_price: float,
    spot_price: float,
    nq_amp: float,
    leverage_mult: float = AMP_BUFFER_LEVERAGE_MULT,
) -> tuple[float, float, float, float]:
    """
    초과 Case 93 — 데이장 개시 시각(09:00:00 ~ 09:00:59 KST / 19:00:00 ~ 19:00:59 EST) 1분간
    나스닥 100 선물(NQ=F)이 이미 급등·급락(nq_amp >= 0.80%)한 상태에서 [모의] 스나이퍼가
    오프닝 휩소 고가에 즉시 모의 체결하지 않도록, 충격 전 기준 고가(anchor_high_price)에서
    3배 레버리지 기대 하락률(expected_drop_pct = min(15.0, nq_amp * 3.0))을 역산하여
    예상 하락가 버퍼(target_drop_p)와 유효 흡수 하한선(partial_drop_p)을 산출합니다.
    반환값: (target_drop_price, partial_drop_price, expected_drop_pct, effective_anchor_high)
    """
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
    데이장 개시 1분 [모의] 버퍼용 기준 고가(session_anchor_high) 및
    당일 04:07 EST / 19:00 EST 이후 첫 RSI >= 79.9 달성 타임스탬프(초과 Case 97, 98)를
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

    # 초과 Case 93 — 데이장 개시 1분(09:00 KST) [모의] 역산 버퍼용 충격 전 기준 고가(session_anchor_high) 추출
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

    # MODIFIED: 초과 Case 98 — 토스 차트의 세션별 보라색 VWAP 선과 100% 일치하도록
    #           데이장(19:00~03:59), 프리장(04:00~09:29), 정규장(09:30~15:59) 누적 VWAP을 각각 벡터 내적으로 산출!
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


class MacroDataCache:
    # MODIFIED: 초과 Case 61, 62, 89, 90, 91, 92, 94 —
    #           1) [해법 1] CME 18:00 EST 신세션 하드 컷오프 + fast_info 당일 실제 고가(day_high)/저가(day_low) 강제 병합으로
    #              1·2차 과열 방어선(>=2.0%) 및 3차 동적 임계치 max(nq_entry_amp * 0.5, 0.80%)의 기준인 NQ=F 당일 진폭을 100% 정확히 산출
    #           2) [해법 2 방법 A & Case 92, 94] 3차 역행 안전망의 진입 기준가·실시간 비교가(get_stage3_reference_price) 및
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
        1·2차 과열 방어선(>=2.0%) 및 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)의 기준인
        나스닥 100 선물지수(NQ=F)의 (curr, high, low)를 반환합니다 (초과 Case 61, 62, 91, 92, 94).
        """
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]

    @classmethod
    async def get_stage3_reference_price(
        cls, force_refresh: bool = False, realtime_probing: bool = False
    ) -> tuple[float, str]:
        """
        초과 Case 92 & Case 94 — 나스닥 100 선물(NQ=F) 진폭으로 산출된 3차 진입 임계치 max(nq_entry_amp * 0.5, 0.80%)를
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
        return ("HYB35", float(cls._nq_cache_data[4]), f"{c:.2f}", c, h, l)


class AssassinLedger:
    # 양방향 대칭 클라이맥스 + 추세추종 눌림목 듀얼 스나이퍼 화이트리스트 락온 (Case 09, 54, 97)
    ALLOWED_SYMBOLS = ("SOXS", "SOXL")
    # 이원화 하이킨 아시 트레일링(기술적 반등 ⚡ OLD_IMM_CB × 진성 상승종목 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 상수 (Case 68, 85, 89, 99)
    DEFAULT_TARGET_PROFIT_RATE = 1.00
    ALLOWED_TARGET_RATES = (1.00,)
    TRAIL_ARM_PROFIT_RATE = 1.00          # +1.00% 도달 시 하이킨 아시 트레일링 점화(Arming)
    TRAIL_FLOOR_LOCK_RATE = 1.00          # 기본 최소 +1.00% 익절 보존선 하드 락온
    TRAIL_DROP_CALLBACK_PCT = 0.30        # 기술적 반등(PRE_SNIPER) 최고가 대비 -0.30% 콜백 스탑
    TREND_PB_CALLBACK_PCT = TREND_PB_CALLBACK_PCT              # 진성 상승종목(PRE_PULLBACK) 최고가 대비 -0.20% 타이트 콜백 스탑 (Case 99)
    TREND_PB_LAUNCH_RATCHET_PCT = TREND_PB_LAUNCH_RATCHET_PCT  # 진성 상승종목 양봉 상승 초입 스탑 유예 상한 +1.30% (Case 99)
    TREND_PB_CUSHION_FLOOR_RATE = TREND_PB_CUSHION_FLOOR_RATE  # 진성 상승종목 양봉 상승 초입 후방 비상 쿠션 +0.75% (Case 99)
    TRAIL_SAFETY_CAP_RATE = 5.00          # 매수 체결 직후 거래소에 선장전하는 상단 안전 캡 (+5.00% LIMIT)
    NQ_VELOCITY_SURGE_PCT = 0.20          # QQQ/NQ 10분/3분 변화율 ±0.20% 이상 급등락 시 HA음봉 바이패스 임계치 (Case 89, 91)
    MIN_STAGE3_QQQ_ADVERSE_PCT = MIN_STAGE3_QQQ_ADVERSE_PCT  # 3차 안전망 최소 실시간 QQQ 역행 하한선 0.80% (Case 94)
    AMP_BUFFER_ACTIVATE_NQ_PCT = AMP_BUFFER_ACTIVATE_NQ_PCT  # 데이장 09:00 KST 1분 한정 역산 버퍼 가동 기준 (Case 93)
    AMP_BUFFER_LEVERAGE_MULT = AMP_BUFFER_LEVERAGE_MULT      # 진폭 × 3.0배 레버리지 기대 하락폭 역산 배수 (Case 93)
    AMP_BUFFER_REBOUND_PCT = AMP_BUFFER_REBOUND_PCT          # 최저점 대비 +0.30% 양봉 반등 시 모의 진입 (Case 93)
    PRE_TREND_LEADER_SELECT_RSI = PRE_TREND_LEADER_SELECT_RSI          # 04:07 EST 이후 첫 상승 추세 종목 각인 기준 79.9 (Case 97)
    PRE_TREND_PULLBACK_RSI_THRESHOLD = PRE_TREND_PULLBACK_RSI_THRESHOLD# 07:00~08:14 EST 추세추종 눌림목 매수 기준 35.0 (Case 97)
    PRE_TREND_REARM_RESET_RSI = PRE_TREND_REARM_RESET_RSI              # 추세눌림목 익절 후 재장전 회복 기준 40.0 (Case 97)
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

    # 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 전용 목표 매도가 조회
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

    # Case 76: 하이킨 아시 트레일링 익절(+1.00% ~ +5.00%+) / 15:00~15:58 VWAP 탈출 / 손절 / 수동개입 / 1주휩소 작전 모두
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

    # MODIFIED: 매크로 1·2·3차 방어선 작동 시 SOXS와 SOXL 양쪽 장부 동시 잠금하되,
    #           현재 수동 보유 중(entry_session == "MANUAL")인 종목과
    #           03:58 ~ 04:06 EST 진행 중인 1주 휩소 작전(entry_session == "WHIPSAW_1SH") 종목은
    #           표식과 목표가(whipsaw_target_price), 미체결 buy_order_id를 보존하며,
    #           04:07 EST 이후에는 미체결 buy_order_id 잔재를 즉시 ""로 소각하여 무한 루프를 원천 차단 (Case 50, 82, 94, 95, 96)
    @classmethod
    async def lock_all_symbols_macro(cls, last_session_id: str = None, force_all: bool = False):
        now_est = datetime.now(EST_TZ)
        est_hm = now_est.hour * 100 + now_est.minute
        for sym in cls.ALLOWED_SYMBOLS:
            st = await cls.get_state(sym)
            buy_id = await cls.get_buy_order_id(sym)
            cur_es = str(st[8] or "")

            is_active_manual_hold = (
                cur_es == "MANUAL"
                and (float(st[0] or 0.0) > 0.0 or float(st[6] or 0.0) > 0.0 or bool(st[7]))
            )
            is_whipsaw_holding = (
                float(st[0] or 0.0) > 0.0
                or float(st[6] or 0.0) > 0.0
                or bool(st[7])
            )
            is_active_whipsaw_1sh = (
                cur_es == "WHIPSAW_1SH"
                and (
                    (358 <= est_hm <= 406)
                    or is_whipsaw_holding
                )
            )

            if is_active_whipsaw_1sh:
                kwargs = {"is_session_done": True}
                if not (358 <= est_hm <= 406) and buy_id:
                    kwargs["buy_order_id"] = ""
            elif is_active_manual_hold:
                kwargs = {
                    "buy_order_id": "",
                    "is_session_done": True,
                    "entry_session": "MANUAL",
                    "whipsaw_target_price": 0.0,
                }
            else:
                kwargs = {
                    "buy_order_id": "",
                    "is_session_done": True,
                    "entry_session": "CANCELED_MACRO",
                    "whipsaw_target_price": 0.0,
                }

            if last_session_id is not None:
                kwargs["last_session_id"] = last_session_id
            await cls.save_state(sym, **kwargs)

    # 프리장 골든타임 내 익절 완료 또는 04:07 휩소 정리 완료 시 양쪽 장부 동시 무한 재출동 락 해제하되,
    # 당일 NQ=F 진폭이 이미 2.0% 이상인 과열 장세라면 자동 스나이퍼 재해금을 차단하고 CANCELED_MACRO 복원 (Case 52, 66, 80, 84, 95)
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
