# =====================================================================
# FILE: quant_engine.py
# 목적: 매크로 캐시(MacroDataCache) 및 듀얼 스나이퍼 통합 시드 원장(AssassinLedger) 코어
#       (quant_engine.py 2/2 분할본 · quant_indicators_plugin 100% 리익스포트 결속)
#       — NQ=F 18:00 EST 신세션 하드 컷오프 + 전일 1d 일봉 대조 3중 Stale 필터 +
#         fast_info 및 1d 일봉 당일 실제 고/저가·마감가 강제 병합 + 동일 세션 단조 보존(초과 Case 62, 90, 91),
#         데이장 개시 1분 전 NQ=F 10분봉 추세 판별(get_nq_10m_candle_trend · 초과 Case 78, 93),
#         토스 Open API QQQ 0.0초 실시간 3차 역행 비교 기준가(1.5초 프로빙 · 최소 0.80% 역행 하한 강제 · 초과 Case 92, 94) 및
#         통합 시드 풀(Unified Seed Pool) 실제 발생 순수익금 복리 정산 원장(Case 06, 09, 30, 76, 82, 95, 96)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 상수 및 벡터/틱 지표 연산 엔진을 quant_indicators_plugin.py로 분리하고 100% 리익스포트 결속
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
from toss_api import GlobalThrottle

from quant_indicators_plugin import (
    EST_TZ,
    AMP_BUFFER_ACTIVATE_NQ_PCT,
    AMP_BUFFER_LEVERAGE_MULT,
    AMP_BUFFER_PARTIAL_MULT,
    AMP_BUFFER_REBOUND_PCT,
    DAY_OPEN_7M_RSI_THRESHOLD,
    DAY_SIM_LOCK_PROFIT_RATE,
    DAY_SIM_FLOOR_PROFIT_RATE,
    DAY_SIM_CALLBACK_PCT,
    DAY_REARM_RESET_RSI_MIN,
    DAY_REARM_RESET_RSI_MAX,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
    compute_amp_buffer_target_price,
    get_cme_session_window_est,
    _compute_qqq_realtime_velocity_sync,
    calculate_rsi_1m,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
)


class MacroDataCache:
    # MODIFIED: 초과 Case 61, 62, 78, 89, 90, 91, 92, 93, 94 —
    #           1) [해법 1 & 3중 Stale 필터] CME 18:00 EST 신세션 하드 컷오프 + 전일 1d 일봉 대조 3중 Stale 필터로
    #              신세션 진행 중 야후 분산 CDN이 반환하는 어제 fast_info(예: 29,983.50 / 29,226.00 = 2.59%) 오염을 100% 원천 차단하고,
    #              17:00~17:59 EST(06:00~06:59 KST) 마감 휴장 중에는 직전 마감 세션 시세를 100% 무결하게 보존!
    #           2) [해법 2 방법 A & Case 92, 94] 3차 역행 안전망의 진입 기준가·실시간 비교가(get_stage3_reference_price) 및
    #              10분/3분 변화율은 토스 Open API QQQ 0.0초 실시간 시세로 산출 (보유 중 1.5초 프로빙, 폴백 NQ=F)
    #           3) [Case 78 & Case 93] 데이장 개시 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물지수(NQ=F) 순수 10분봉
    #              시가·종가·등락률 캐시(_nq_pure_10m_data) 및 추세 판별 메서드(get_nq_10m_candle_trend) 탑재
    # 튜플 구조: (curr_val, high_val, low_val, updated_ts, nq_vel_10m, nq_vel_3m)
    _nq_cache_data = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    # 순수 NQ=F 선물 10분봉 데이터: (open_10m, close_10m, nq_vel_10m_pure, nq_vel_3m_pure)
    _nq_pure_10m_data = (0.0, 0.0, 0.0, 0.0)
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
            target_session_date = (nq_session_start_est + timedelta(hours=6)).date()

            # 초과 Case 90 — 18:00:00 EST(한국시간 07:00 KST) CME 선물 신세션 롤오버 감지 시에만 이전 세션 캐시 0점 소각!
            session_rolled = (cls._nq_session_id != cur_sess_id)
            if session_rolled:
                cls._nq_session_id = cur_sess_id
                cls._nq_cache_data = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                cls._nq_pure_10m_data = (0.0, 0.0, 0.0, 0.0)
                cls._qqq_curr_price = 0.0
                cls._qqq_price_ts = 0.0

            need_refresh = force_refresh or session_rolled or cls._is_refresh_needed(now, cls._nq_cache_data[3])
            if not need_refresh:
                return

            prev_c, prev_h, prev_l = (
                float(cls._nq_cache_data[0]),
                float(cls._nq_cache_data[1]),
                float(cls._nq_cache_data[2]),
            )

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

                # MODIFIED: 초과 Case 91 — 1일봉(interval="1d")에서 당일 세션(target_session_date) 공식 고가/저가/종가뿐만 아니라,
                # 이전 구세션(idx_dates < target_session_date, 예: 어제 10-19)의 공식 고가/저가 리스트(prev_1d_highs, prev_1d_lows)를 함께 추출하여
                # 야후 분산 CDN이 신세션 중에 어제의 fast_info(29,983.50 / 29,226.00 = 2.59%)를 반환할 때 100% Stale로 식별!
                daily_sess_high = 0.0
                daily_sess_low = 0.0
                daily_sess_close = 0.0
                prev_1d_highs = []
                prev_1d_lows = []
                try:
                    df_1d = tkr.history(period="5d", interval="1d")
                    if df_1d is not None and not df_1d.empty:
                        for col_d in ["High", "Low", "Close"]:
                            if col_d in df_1d.columns:
                                df_1d[col_d] = pd.to_numeric(df_1d[col_d], errors="coerce").fillna(0.0)
                        idx_dates = pd.to_datetime(df_1d.index).date
                        matched_1d = df_1d[idx_dates == target_session_date]
                        if not matched_1d.empty:
                            h_1d = float(matched_1d["High"].max())
                            l_1d = float(matched_1d["Low"].min())
                            c_1d = float(matched_1d["Close"].iloc[-1])
                            if h_1d > 0.0:
                                daily_sess_high = h_1d
                            if l_1d > 0.0:
                                daily_sess_low = l_1d
                            if c_1d > 0.0:
                                daily_sess_close = c_1d

                        prev_1d_df = df_1d[idx_dates < target_session_date]
                        if not prev_1d_df.empty:
                            prev_1d_highs = [float(x) for x in prev_1d_df["High"].tolist() if float(x) > 0.0]
                            prev_1d_lows = [float(x) for x in prev_1d_df["Low"].tolist() if float(x) > 0.0]
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
                    seed_c = fast_last_p or daily_sess_close
                    if seed_c > 0.0:
                        h_init = max(seed_c, fast_day_high, daily_sess_high)
                        l_cands = [v for v in (seed_c, fast_day_low, daily_sess_low) if v > 0.0]
                        l_init = min(l_cands) if l_cands else seed_c
                        return seed_c, h_init, l_init, 0.0, 0.0, seed_c, prev_1d_highs, prev_1d_lows
                    return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, prev_1d_highs, prev_1d_lows

                df.index = pd.to_datetime(df.index, utc=True).tz_convert(EST_TZ)
                df = df[~df.index.duplicated(keep="last")]
                df.sort_index(ascending=True, inplace=True)

                for col in ["Open", "High", "Low", "Close"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

                df = df[df["Close"] > 0.0].copy()
                if df.empty:
                    seed_c = fast_last_p or daily_sess_close
                    if seed_c > 0.0:
                        return seed_c, seed_c, seed_c, 0.0, 0.0, seed_c, prev_1d_highs, prev_1d_lows
                    return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, prev_1d_highs, prev_1d_lows

                if "Open" in df.columns:
                    df["Open"] = np.where(df["Open"] > 0.0, df["Open"], df["Close"])
                else:
                    df["Open"] = df["Close"]

                # 직전 구세션(nq_session_start_est 이전 세션) 1분봉 고가·저가 추출
                df_before = df[df.index < nq_session_start_est]
                prev_sess_h, prev_sess_l = 0.0, 0.0
                if not df_before.empty:
                    td_prev = df_before.index.to_series().diff()
                    gaps_prev = td_prev[td_prev >= pd.Timedelta(minutes=45)]
                    df_prev_sess = df_before[df_before.index >= gaps_prev.index[-1]] if not gaps_prev.empty else df_before
                    if not df_prev_sess.empty:
                        prev_sess_h = float(df_prev_sess["High"].max())
                        prev_sess_l = float(df_prev_sess["Low"].min())

                # 초과 Case 62 & Case 90 — 현재 대상 CME 선물 세션 시작 시각(18:00:00 EST) 이전의 전일/금요일 캔들 100% 절삭
                df_session = df[df.index >= nq_session_start_est].copy()

                # MODIFIED: 고가/저가 개별 Stale 판정 분리 + 전일 1d 일봉 고/저가(prev_1d_highs, prev_1d_lows) 교차 대조 (# NEW)
                is_fast_high_stale = bool(
                    (prev_sess_h > 0.0 and abs(fast_day_high - prev_sess_h) < 0.5)
                    or (is_cme_open and any(abs(fast_day_high - ph) < 0.5 for ph in prev_1d_highs))
                )
                is_fast_low_stale = bool(
                    (prev_sess_l > 0.0 and abs(fast_day_low - prev_sess_l) < 0.5)
                    or (is_cme_open and any(abs(fast_day_low - pl) < 0.5 for pl in prev_1d_lows))
                )

                is_daily_high_stale = bool(
                    is_cme_open and daily_sess_high > 0.0 and (
                        (prev_sess_h > 0.0 and abs(daily_sess_high - prev_sess_h) < 0.5)
                        or any(abs(daily_sess_high - ph) < 0.5 for ph in prev_1d_highs)
                    )
                )
                is_daily_low_stale = bool(
                    is_cme_open and daily_sess_low > 0.0 and (
                        (prev_sess_l > 0.0 and abs(daily_sess_low - prev_sess_l) < 0.5)
                        or any(abs(daily_sess_low - pl) < 0.5 for pl in prev_1d_lows)
                    )
                )

                if df_session.empty:
                    seed_p = fast_last_p or daily_sess_close or float(df["Close"].iloc[-1])
                    h_seed, l_seed = seed_p, seed_p
                    if not is_daily_high_stale and daily_sess_high > 0.0 and abs(daily_sess_high - seed_p) / seed_p < 0.12:
                        h_seed = max(h_seed, daily_sess_high)
                    if not is_daily_low_stale and daily_sess_low > 0.0 and abs(daily_sess_low - seed_p) / seed_p < 0.12:
                        l_seed = min(l_seed, daily_sess_low)
                    if not is_fast_high_stale and fast_day_high > 0.0 and seed_p > 0.0 and abs(fast_day_high - seed_p) / seed_p < 0.12:
                        h_seed = max(h_seed, fast_day_high)
                    if not is_fast_low_stale and fast_day_low > 0.0 and seed_p > 0.0 and abs(fast_day_low - seed_p) / seed_p < 0.12:
                        l_seed = min(l_seed, fast_day_low)
                    return seed_p, h_seed, l_seed, 0.0, 0.0, seed_p, prev_1d_highs, prev_1d_lows

                curr_val = float(df_session["Close"].iloc[-1])
                sess_1m_high = float(df_session["High"].max())
                sess_1m_low = float(df_session["Low"].min())
                high_val = sess_1m_high
                low_val = sess_1m_low

                # NEW: 양방향 동시 이탈 Stale 판정 — 신세션 진행 중(is_cme_open == True)에
                # fast_info의 고가가 당일 1분봉 최고가보다 10pt 이상 높으면서 동시에 저가도 당일 1분봉 최저가보다 10pt 이상 낮다면,
                # 이는 개장 첫 틱 밑꼬리가 아니라 전일 24시간 범위(예: 29,983.50 ~ 29,226.00)가 미초기화된 것이므로 즉시 Stale 처리!
                if (
                    is_cme_open
                    and fast_day_high > sess_1m_high + 10.0
                    and 0.0 < fast_day_low < sess_1m_low - 10.0
                ):
                    is_fast_high_stale = True
                    is_fast_low_stale = True

                if (
                    is_cme_open
                    and daily_sess_high > sess_1m_high + 10.0
                    and 0.0 < daily_sess_low < sess_1m_low - 10.0
                ):
                    is_daily_high_stale = True
                    is_daily_low_stale = True

                # 1) 1일봉(interval="1d")의 해당 세션 공식 고가/저가 병합 (Stale 아닌 경우에만)
                if curr_val > 0.0:
                    if not is_daily_high_stale and daily_sess_high > 0.0 and abs(daily_sess_high - curr_val) / curr_val < 0.12:
                        high_val = max(high_val, daily_sess_high)
                    if not is_daily_low_stale and daily_sess_low > 0.0 and abs(daily_sess_low - curr_val) / curr_val < 0.12:
                        low_val = min(low_val, daily_sess_low) if low_val > 0.0 else daily_sess_low

                # 2) 초과 Case 91 [해법 1] — 개장 중 및 마감 휴장 중 fast_info 마감가·고가·저가 병합 (3중 Stale 필터 통과 시에만!)
                if curr_val > 0.0:
                    if fast_last_p > 0.0 and abs(fast_last_p - curr_val) / curr_val < 0.05:
                        curr_val = fast_last_p
                        high_val = max(high_val, fast_last_p)
                        low_val = min(low_val, fast_last_p) if low_val > 0.0 else fast_last_p

                    if not is_fast_high_stale and fast_day_high > 0.0 and abs(fast_day_high - curr_val) / curr_val < 0.12:
                        high_val = max(high_val, fast_day_high)
                    if not is_fast_low_stale and fast_day_low > 0.0 and abs(fast_day_low - curr_val) / curr_val < 0.12:
                        low_val = min(low_val, fast_day_low) if low_val > 0.0 else fast_day_low

                # NQ=F 자체 10분봉 시가(ref_10m) 대비 현재가(curr_val) 변화율 및 3분 변화율 산출
                first_open = float(df_session["Open"].iloc[0]) or float(df_session["Close"].iloc[0])
                ref_10m = float(df_session["Close"].iloc[-11]) if len(df_session) >= 11 else first_open
                ref_3m = float(df_session["Close"].iloc[-4]) if len(df_session) >= 4 else first_open

                vel_10m = ((curr_val - ref_10m) / ref_10m * 100.0) if ref_10m > 0.0 else 0.0
                vel_3m = ((curr_val - ref_3m) / ref_3m * 100.0) if ref_3m > 0.0 else 0.0
                return curr_val, high_val, low_val, vel_10m, vel_3m, ref_10m, prev_1d_highs, prev_1d_lows

            try:
                nq_task = asyncio.wait_for(asyncio.to_thread(_fetch_nq), timeout=5.0)
                qqq_task = cls._fetch_qqq_velocity_async(now_est)
                (c, h, l, nq_v10, nq_v3, nq_ref10, p1d_h_list, p1d_l_list), (qqq_ok, q_v10, q_v3, q_curr) = await asyncio.gather(
                    nq_task, qqq_task
                )

                if c > 0.0:
                    cls._nq_pure_10m_data = (float(nq_ref10 or c), float(c), float(nq_v10), float(nq_v3))

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
                    # 초과 Case 62, 90, 91 — 동일 선물 세션(not session_rolled) 내 누적 최고가(max)·최저가(min) 단조 보존 락!
                    # (단, 이전 캐시 prev_h / prev_l 이 전일 1d 고/저가와 일치하는 오염 값이면 단조 보존에서 즉시 배제하여 자가 치유!)
                    if not session_rolled and prev_l > 0.0:
                        prev_h_contaminated = is_cme_open and any(abs(prev_h - ph) < 0.5 for ph in (p1d_h_list or []))
                        prev_l_contaminated = is_cme_open and any(abs(prev_l - pl) < 0.5 for pl in (p1d_l_list or []))
                        if not prev_h_contaminated and prev_h > 0.0 and h > 0.0 and abs(prev_h - c) / max(c, 1.0) < 0.12:
                            h = max(h, prev_h)
                        if not prev_l_contaminated and prev_l > 0.0 and l > 0.0 and abs(prev_l - c) / max(c, 1.0) < 0.12:
                            l = min(l, prev_l)
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
    async def get_nq_10m_candle_trend(
        cls, force_refresh: bool = False
    ) -> tuple[str, str, float, float, float]:
        """
        초과 Case 78 & Case 93 — 데이장 시작 1분 전(08:59 KST / 18:59 EST) 및 개시 7분 윈도우에서
        나스닥 100 선물지수(NQ=F)의 직전 10분봉을 판별하여 상승(양봉)이면 ('LONG', 'SOXL'),
        하락(음봉)이면 ('SHORT', 'SOXS')를 반환합니다.
        반환값: (trend_dir, target_symbol, vel_10m_pct, open_10m_val, close_10m_val)
        """
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        open_10m, close_10m, nq_v10_pure, nq_v3_pure = cls._nq_pure_10m_data
        c_nq, h_nq, l_nq = cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]

        if close_10m <= 0.0:
            close_10m = float(c_nq)
        if open_10m <= 0.0 and close_10m > 0.0:
            v10_fallback = float(cls._nq_cache_data[4])
            open_10m = close_10m / (1.0 + v10_fallback / 100.0) if abs(v10_fallback) > 1e-6 else close_10m
            nq_v10_pure = v10_fallback

        if close_10m > open_10m or nq_v10_pure > 0.0:
            return "LONG", "SOXL", float(nq_v10_pure), float(open_10m), float(close_10m)
        if close_10m < open_10m or nq_v10_pure < 0.0:
            return "SHORT", "SOXS", float(nq_v10_pure), float(open_10m), float(close_10m)

        if nq_v3_pure > 0.0:
            return "LONG", "SOXL", float(nq_v3_pure), float(open_10m), float(close_10m)
        if nq_v3_pure < 0.0:
            return "SHORT", "SOXS", float(nq_v3_pure), float(open_10m), float(close_10m)

        mid_nq = ((h_nq + l_nq) / 2.0) if (h_nq > 0.0 and l_nq > 0.0) else close_10m
        if close_10m >= mid_nq and close_10m > 0.0:
            return "LONG", "SOXL", 0.0, float(open_10m), float(close_10m)
        return "SHORT", "SOXS", 0.0, float(open_10m), float(close_10m)

    @classmethod
    async def get_stage3_reference_price(
        cls, force_refresh: bool = False, realtime_probing: bool = False
    ) -> tuple[float, str]:
        """
        초과 Case 92 & Case 94 — 나스닥 100 선물(NQ=F) 진폭으로 산출된 3차 진입 임계치 max(nq_entry_amp * 0.5, 0.80%)를
        10분 지연 없이 시차 0.0초로 적용하기 위한 토스 Open API QQQ 실시간 현재가 반환 헬퍼.
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
    TRAIL_ARM_PROFIT_RATE = 1.00          # 프리장/정규장 본진 +1.00% 도달 시 하이킨 아시 트레일링 점화(Arming)
    TRAIL_FLOOR_LOCK_RATE = 1.00          # 기본 최소 +1.00% 익절 보존선 하드 락온
    TRAIL_DROP_CALLBACK_PCT = 0.30        # 기술적 반등(PRE_SNIPER) 최고가 대비 -0.30% 콜백 스탑
    TREND_PB_CALLBACK_PCT = TREND_PB_CALLBACK_PCT              # 진성 상승종목(PRE_PULLBACK) 최고가 대비 -0.20% 타이트 콜백 스탑 (Case 99)
    TREND_PB_LAUNCH_RATCHET_PCT = TREND_PB_LAUNCH_RATCHET_PCT  # 진성 상승종목 양봉 상승 초입 스탑 유예 상한 +1.30% (Case 99)
    TREND_PB_CUSHION_FLOOR_RATE = TREND_PB_CUSHION_FLOOR_RATE  # 진성 상승종목 양봉 상승 초입 후방 비상 쿠션 +0.75% (Case 99)
    TRAIL_SAFETY_CAP_RATE = 5.00          # 매수 체결 직후 거래소에 선장전하는 상단 안전 캡 (+5.00% LIMIT)
    NQ_VELOCITY_SURGE_PCT = 0.20          # QQQ/NQ 10분/3분 변화율 ±0.20% 이상 급등락 시 HA음봉 바이패스 임계치 (Case 89, 91)
    MIN_STAGE3_QQQ_ADVERSE_PCT = MIN_STAGE3_QQQ_ADVERSE_PCT  # 3차 안전망 최소 실시간 QQQ 역행 하한선 0.80% (Case 94)
    # 데이장 100% [모의] 전용 추세·트레일링·리셋 상수 바인딩 (Case 78, 93)
    DAY_OPEN_7M_RSI_THRESHOLD = DAY_OPEN_7M_RSI_THRESHOLD
    DAY_SIM_LOCK_PROFIT_RATE = DAY_SIM_LOCK_PROFIT_RATE
    DAY_SIM_FLOOR_PROFIT_RATE = DAY_SIM_FLOOR_PROFIT_RATE
    DAY_SIM_CALLBACK_PCT = DAY_SIM_CALLBACK_PCT
    DAY_REARM_RESET_RSI_MIN = DAY_REARM_RESET_RSI_MIN
    DAY_REARM_RESET_RSI_MAX = DAY_REARM_RESET_RSI_MAX
    AMP_BUFFER_ACTIVATE_NQ_PCT = AMP_BUFFER_ACTIVATE_NQ_PCT
    AMP_BUFFER_LEVERAGE_MULT = AMP_BUFFER_LEVERAGE_MULT
    AMP_BUFFER_REBOUND_PCT = AMP_BUFFER_REBOUND_PCT
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

    @classmethod
    def normalize_target_rate(cls, rate_val=None) -> float:
        return 1.00

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

        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            invested_usd = float(entry_price) * float(filled_qty)
            returned_usd = float(exit_price) * float(filled_qty)
            gross_pnl_usd = returned_usd - invested_usd
            comm_usd = (invested_usd + returned_usd) * cls.ONE_WAY_COMM_RATE
            net_delta_usd = round(gross_pnl_usd - comm_usd, 2)
            new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
            return new_budget, net_delta_usd, round(comm_usd, 2)

        if entry_price > 0.0 and exit_price > 0.0:
            est_qty = max(1, math.floor(base_budget / float(entry_price)))
            invested_usd = float(entry_price) * est_qty
            returned_usd = float(exit_price) * est_qty
            gross_pnl_usd = returned_usd - invested_usd
            comm_usd = (invested_usd + returned_usd) * cls.ONE_WAY_COMM_RATE
            net_delta_usd = round(gross_pnl_usd - comm_usd, 2)
            new_budget = round(max(cls.MIN_BUDGET_FLOOR, base_budget + net_delta_usd), 2)
            return new_budget, net_delta_usd, round(comm_usd, 2)

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
