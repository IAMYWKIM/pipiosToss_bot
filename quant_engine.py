# =====================================================================
# FILE: quant_engine.py
# 목적: 동기(with) 및 비동기(async with) 양방향 100% 호환 파일 뮤텍스(_HybridFileLock),
#       나스닥 100 선물(NQ=F 18:00 EST 신세션 하드 컷오프 + fast_info/1d 고저가 강제 병합) &
#       토스 Open API QQQ 0.0초 실시간 3차 역행 기준가/단기 변화율(±0.20% NQ_VEL_BYPASS) 브릿지(MacroDataCache),
#       데이장 개시 1분 전(08:59 KST / 18:59 EST) NQ=F 10분봉 실시간 fast_info 병합 추세 판별기,
#       통합 복리 시드 풀 원자적 듀얼 원장(AssassinLedger) 및 quant_indicators_plugin 리익스포트 코어
#       (quant_engine.py 2/2 분할본)
# =====================================================================
# MODIFIED: 제1헌법 ~ 제6헌법 및 플러그인 분할 아키텍처 의거 지표 벡터 연산 코어를 quant_indicators_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 초과 Case 61, 62, 89, 90, 91, 92, 93, 94 — NQ=F 18:00 EST 신세션 컷오프, fast_info+1d 고/저가 단조 보존,
#           토스 Open API QQQ 0.0초 실시간 현재가(1.5초 프로빙) 및 10m/3m 변화율 산출
# NEW: 1) GlobalThrottle.get_file_lock(filepath) 호출 시 동기 `with` 문과 비동기 `async with` 문을 모두 지원하는
#         `_HybridFileLock` 을 바인딩하여 `'Lock' object does not support the context manager protocol` 에러 및
#         텔레그램 명령어 무응답 현상을 100% 원천 소각 (제1헌법 & 초과 Case 30)
#      2) _LedgerStateTuple 탑재로 13-변수 언패킹 및 state[13](whipsaw_target_price) 인덱스 접근 양방향 완벽 호환

import os
import json
import math
import time
import asyncio
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf

from toss_api import GlobalThrottle
from quant_indicators_plugin import (
    EST_TZ,
    KST_TZ,
    SINGLE_TARGET_PROFIT_RATE,
    ROUND_TRIP_COMMISSION_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
    PRE_PIVOT_10M_MIN_SWING_AMP_PCT,
    PRE_PIVOT_10M_MAX_TROUGH_RSI,
    PRE_PIVOT_10M_REBOUND_MIN_PCT,
    PRE_PIVOT_10M_REBOUND_MAX_PCT,
    PRE_PIVOT_10M_MIN_BODY_PCT,
    PRE_PIVOT_10M_SHIELD_REBOUND_RSI,
    DAY_PIVOT_10M_MIN_SWING_AMP_PCT,
    DAY_PIVOT_10M_MAX_TROUGH_RSI,
    DAY_PIVOT_10M_REBOUND_MIN_PCT,
    DAY_PIVOT_10M_REBOUND_MAX_PCT,
    DAY_PIVOT_10M_MIN_BODY_PCT,
    _AwaitableRsiTuple,
    normalize_target_rate,
    calc_compound_next_budget,
    get_cme_session_window_est,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
    compute_realtime_tick_10m_state,
    _compute_rsi_and_vwap_sync,
    calculate_rsi_1m,
)


class _HybridFileLock:
    """
    제1헌법 및 초과 Case 30 완벽 방어:
    동기 스레드 내부의 `with GlobalThrottle.get_file_lock(filepath):` 구문과
    비동기 코루틴 내부의 `async with GlobalThrottle.get_file_lock(filepath):` 구문을
    동시에 100% 지원하는 재진입 가능 하이브리드 파일 락 클래스.
    """
    def __init__(self):
        self._rlock = threading.RLock()

    def acquire(self, blocking: bool = True, timeout: float = -1.0) -> bool:
        if timeout is None or timeout < 0.0:
            return self._rlock.acquire(blocking=blocking)
        return self._rlock.acquire(blocking=blocking, timeout=timeout)

    def release(self):
        try:
            self._rlock.release()
        except RuntimeError:
            pass

    def __enter__(self):
        self._rlock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

    async def __aenter__(self):
        await asyncio.to_thread(self._rlock.acquire)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


_HYBRID_FILE_LOCKS_GUARD = threading.Lock()
_HYBRID_FILE_LOCKS_MAP: dict[str, _HybridFileLock] = {}


def _get_hybrid_file_lock(filepath: str) -> _HybridFileLock:
    key = os.path.abspath(str(filepath or "default.lock"))
    with _HYBRID_FILE_LOCKS_GUARD:
        lock_obj = _HYBRID_FILE_LOCKS_MAP.get(key)
        if lock_obj is None:
            lock_obj = _HybridFileLock()
            _HYBRID_FILE_LOCKS_MAP[key] = lock_obj
        return lock_obj


# GlobalThrottle.get_file_lock 을 하이브리드 락 팩토리로 즉시 오버라이드 바인딩
GlobalThrottle.get_file_lock = staticmethod(_get_hybrid_file_lock)


class _LedgerStateTuple(tuple):
    """
    main_loops_plugin.py 등의 13-변수 언패킹(`a, b, ..., m = await AssassinLedger.get_state(sym)`)과
    확장 인덱스 조회(`state[13]` -> whipsaw_target_price)를 동시에 지원하는 튜플 서브클래스.
    """
    def __new__(cls, core_13: tuple, whipsaw_target_price: float = 0.0):
        obj = super().__new__(cls, core_13)
        obj._whipsaw_target_price = float(whipsaw_target_price or 0.0)
        return obj

    def __getitem__(self, item):
        if isinstance(item, int) and item == 13:
            return self._whipsaw_target_price
        return super().__getitem__(item)


class MacroDataCache:
    """
    초과 Case 61, 62, 89, 90, 91, 92, 93, 94:
    1) CME 나스닥 100 선물(NQ=F) 18:00 EST 신세션 하드 컷오프 + fast_info / 1d 일봉 당일 실제 고/저가 강제 병합 및 단조 보존
    2) 토스 Open API QQQ 0.0초 실시간 현재가(3차 역행 안전망 기준가/비교가, 보유 중 1.5초 프로빙) 및 10분/3분 변화율(±0.20% HA음봉 바이패스)
    3) 데이장 개시 1분 전(08:59 KST / 18:59 EST) NQ=F 10분봉 실시간 fast_info 병합 양/음봉 판별
    """
    _lock = asyncio.Lock()
    _qqq_lock = asyncio.Lock()
    _nq_session_id: str = ""
    _last_fetch_ts: float = 0.0
    _cached_close: float = 0.0
    _cached_high: float = 0.0
    _cached_low: float = 0.0
    _cached_vel_10m: float = 0.0
    _cached_vel_3m: float = 0.0

    _qqq_last_price: float = 0.0
    _qqq_last_ts: float = 0.0
    _qqq_vel_ts: float = 0.0
    _qqq_vel_10m: float = 0.0
    _qqq_vel_3m: float = 0.0

    _nq_10m_trend_ts: float = 0.0
    _nq_10m_trend_sym: str = ""
    _nq_10m_open: float = 0.0
    _nq_10m_close: float = 0.0
    _nq_10m_body_pct: float = 0.0

    CACHE_TTL_SEC = 15.0
    QQQ_PROBE_TTL_SEC = 1.5

    @classmethod
    def reset_session_cache(cls):
        cls._nq_session_id = ""
        cls._last_fetch_ts = 0.0
        cls._cached_close = 0.0
        cls._cached_high = 0.0
        cls._cached_low = 0.0
        cls._cached_vel_10m = 0.0
        cls._cached_vel_3m = 0.0
        cls._qqq_last_price = 0.0
        cls._qqq_last_ts = 0.0
        cls._qqq_vel_ts = 0.0
        cls._qqq_vel_10m = 0.0
        cls._qqq_vel_3m = 0.0
        cls._nq_10m_trend_ts = 0.0
        cls._nq_10m_trend_sym = ""
        cls._nq_10m_open = 0.0
        cls._nq_10m_close = 0.0
        cls._nq_10m_body_pct = 0.0

    @staticmethod
    def _fetch_nq_sync(
        nq_session_start_est: datetime,
        is_cme_open: bool,
        prev_h: float,
        prev_l: float,
        prev_c: float,
    ) -> tuple[float, float, float, float, float]:
        close_val, high_val, low_val = 0.0, 0.0, 0.0
        vel_10m, vel_3m = 0.0, 0.0
        target_session_date = (nq_session_start_est + timedelta(hours=6)).date()

        try:
            tkr = yf.Ticker("NQ=F")

            # 1) fast_info 실시간 현재가 및 당일 고/저가 추출 (초과 Case 90, 91)
            fast_last, fast_high, fast_low = 0.0, 0.0, 0.0
            try:
                fi = tkr.fast_info
                if fi is not None:
                    fast_last = float(getattr(fi, "last_price", 0.0) or fi.get("lastPrice", 0.0) or 0.0)
                    fast_high = float(getattr(fi, "day_high", 0.0) or fi.get("dayHigh", 0.0) or 0.0)
                    fast_low = float(getattr(fi, "day_low", 0.0) or fi.get("dayLow", 0.0) or 0.0)
            except Exception:
                pass

            # 2) 1분봉 시계열 조회 및 18:00:00 EST 신세션 경계 하드 컷오프 (초과 Case 62, 90)
            df_1m = tkr.history(period="5d", interval="1m")
            df_session = pd.DataFrame()
            if df_1m is not None and not df_1m.empty:
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize("UTC").tz_convert(EST_TZ)
                else:
                    df_1m.index = df_1m.index.tz_convert(EST_TZ)
                df_1m = df_1m[(df_1m["High"] > 0.0) & (df_1m["Low"] > 0.0) & (df_1m["Close"] > 0.0)]

                sess_end_est = nq_session_start_est + timedelta(hours=23)
                df_session = df_1m[(df_1m.index >= nq_session_start_est) & (df_1m.index < sess_end_est)]

            if not df_session.empty:
                close_val = float(df_session["Close"].iloc[-1])
                high_val = float(df_session["High"].max())
                low_val = float(df_session["Low"].min())

                closes = df_session["Close"]
                cur_c = float(closes.iloc[-1])
                c_10m_ago = float(closes.iloc[-11]) if len(closes) >= 11 else float(closes.iloc[0])
                if c_10m_ago > 0.0:
                    vel_10m = (cur_c - c_10m_ago) / c_10m_ago * 100.0

                c_3m_ago = float(closes.iloc[-4]) if len(closes) >= 4 else float(closes.iloc[0])
                if c_3m_ago > 0.0:
                    vel_3m = (cur_c - c_3m_ago) / c_3m_ago * 100.0

            # 3) 1d 공식 일봉에서 해당 세션 기준일(target_session_date)의 고가/저가 추출 (초과 Case 91)
            daily_sess_high, daily_sess_low, daily_sess_close = 0.0, 0.0, 0.0
            try:
                df_1d = tkr.history(period="5d", interval="1d")
                if df_1d is not None and not df_1d.empty:
                    for idx_dt, row in df_1d.iterrows():
                        r_date = idx_dt.date() if hasattr(idx_dt, "date") else pd.to_datetime(idx_dt).date()
                        if r_date == target_session_date:
                            dh = float(row.get("High", 0.0) or 0.0)
                            dl = float(row.get("Low", 0.0) or 0.0)
                            dc = float(row.get("Close", 0.0) or 0.0)
                            if dh > 0.0 and dl > 0.0:
                                daily_sess_high = dh
                                daily_sess_low = dl
                                daily_sess_close = dc
                            break
            except Exception:
                pass

            # 4) 18:00~18:15 EST 신세션 직후 fast_info 잔재 여부 검증 후 고/저가 강제 병합
            now_est = datetime.now(EST_TZ)
            mins_since_open = (now_est - nq_session_start_est).total_seconds() / 60.0
            is_fast_hl_stale = False
            if is_cme_open and (0.0 <= mins_since_open <= 20.0) and high_val > 0.0 and low_val > 0.0:
                if fast_high > 0.0 and fast_low > 0.0:
                    fast_amp = (fast_high - fast_low) / fast_low * 100.0
                    bar_amp = (high_val - low_val) / low_val * 100.0
                    if fast_amp > bar_amp + 0.50:
                        is_fast_hl_stale = True

            if fast_last > 0.0 and (is_cme_open or close_val <= 0.0):
                close_val = fast_last
            elif daily_sess_close > 0.0 and close_val <= 0.0:
                close_val = daily_sess_close

            if not is_fast_hl_stale:
                for cand_h in (fast_high, daily_sess_high, close_val):
                    if cand_h > 0.0:
                        high_val = max(high_val, cand_h) if high_val > 0.0 else cand_h
                for cand_l in (fast_low, daily_sess_low, close_val):
                    if cand_l > 0.0:
                        low_val = min(low_val, cand_l) if low_val > 0.0 else cand_l
            else:
                if close_val > 0.0:
                    high_val = max(high_val, close_val) if high_val > 0.0 else close_val
                    low_val = min(low_val, close_val) if low_val > 0.0 else close_val

            # 5) 동일 CME 선물 세션(_nq_session_id) 내 누적 최고가/최저가 단조 보존 (초과 Case 62, 91)
            if prev_h > 0.0:
                high_val = max(high_val, prev_h) if high_val > 0.0 else prev_h
            if prev_l > 0.0:
                low_val = min(low_val, prev_l) if low_val > 0.0 else prev_l
            if close_val <= 0.0 and prev_c > 0.0:
                close_val = prev_c

        except Exception as e:
            print(f"🚨 [NQ=F 선물 시세 조회 방어] {e}", flush=True)
            close_val, high_val, low_val = prev_c, prev_h, prev_l

        return (
            round(float(close_val or 0.0), 2),
            round(float(high_val or 0.0), 2),
            round(float(low_val or 0.0), 2),
            round(float(vel_10m or 0.0), 4),
            round(float(vel_3m or 0.0), 4),
        )

    @classmethod
    async def _refresh_qqq_snapshot_if_needed(cls, force_refresh: bool = False, realtime_probing: bool = False):
        client = getattr(GlobalThrottle, "active_client", None)
        if client is None:
            return

        now_ts = time.time()
        now_est = datetime.now(EST_TZ)
        est_hm = now_est.hour * 100 + now_est.minute
        wk = now_est.weekday()
        if (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800) or (1700 <= est_hm <= 1859):
            return

        ttl_p = cls.QQQ_PROBE_TTL_SEC if realtime_probing else cls.CACHE_TTL_SEC
        need_price = force_refresh or (now_ts - cls._qqq_last_ts >= ttl_p) or (cls._qqq_last_price <= 0.0)
        need_vel = force_refresh or (now_ts - cls._qqq_vel_ts >= cls.CACHE_TTL_SEC)

        if not need_price and not need_vel:
            return

        async with cls._qqq_lock:
            now_ts = time.time()
            need_price = force_refresh or (now_ts - cls._qqq_last_ts >= ttl_p) or (cls._qqq_last_price <= 0.0)
            need_vel = force_refresh or (now_ts - cls._qqq_vel_ts >= cls.CACHE_TTL_SEC)
            try:
                if hasattr(client, "get_qqq_realtime_snapshot") and need_vel:
                    snap = await asyncio.wait_for(client.get_qqq_realtime_snapshot(), timeout=3.5)
                    if isinstance(snap, dict):
                        qp = float(snap.get("price", 0.0) or 0.0)
                        if qp > 0.0:
                            cls._qqq_last_price = qp
                            cls._qqq_last_ts = time.time()
                        cls._qqq_vel_10m = float(snap.get("vel_10m", 0.0) or 0.0)
                        cls._qqq_vel_3m = float(snap.get("vel_3m", 0.0) or 0.0)
                        cls._qqq_vel_ts = time.time()
                        return

                if need_price:
                    p_map = await asyncio.wait_for(client.get_current_prices_batch(("QQQ",)), timeout=2.5)
                    qp = float((p_map or {}).get("QQQ", 0.0) or 0.0)
                    if qp > 0.0:
                        cls._qqq_last_price = qp
                        cls._qqq_last_ts = time.time()

                if need_vel:
                    candles_q = await asyncio.wait_for(client.get_1m_candles("QQQ", count=25), timeout=3.0)
                    if candles_q and isinstance(candles_q, list):
                        df_q = pd.DataFrame(candles_q)
                        if not df_q.empty and "closePrice" in df_q.columns and "timestamp" in df_q.columns:
                            df_q["timestamp"] = pd.to_datetime(
                                df_q["timestamp"], format="ISO8601", utc=True
                            ).dt.tz_convert(EST_TZ)
                            df_q.sort_values("timestamp", ascending=True, inplace=True)
                            df_q["closePrice"] = pd.to_numeric(df_q["closePrice"], errors="coerce").fillna(0.0)
                            df_q = df_q[df_q["closePrice"] > 0.0]
                            if not df_q.empty:
                                cur_q = cls._qqq_last_price if cls._qqq_last_price > 0.0 else float(df_q["closePrice"].iloc[-1])
                                c10 = float(df_q["closePrice"].iloc[-11]) if len(df_q) >= 11 else float(df_q["closePrice"].iloc[0])
                                c3 = float(df_q["closePrice"].iloc[-4]) if len(df_q) >= 4 else float(df_q["closePrice"].iloc[0])
                                cls._qqq_vel_10m = round(((cur_q - c10) / c10 * 100.0), 4) if c10 > 0.0 else 0.0
                                cls._qqq_vel_3m = round(((cur_q - c3) / c3 * 100.0), 4) if c3 > 0.0 else 0.0
                                cls._qqq_vel_ts = time.time()
            except Exception:
                pass

    @classmethod
    async def get_cached_nq_data(cls, force_refresh: bool = False) -> tuple[float, float, float]:
        now_est = datetime.now(EST_TZ)
        is_cme_open, nq_sess_start = get_cme_session_window_est(now_est)
        sess_id = nq_sess_start.strftime("%Y-%m-%d_%H%M")

        now_ts = time.time()
        if (
            not force_refresh
            and cls._nq_session_id == sess_id
            and (now_ts - cls._last_fetch_ts < cls.CACHE_TTL_SEC)
            and cls._cached_high > 0.0
            and cls._cached_low > 0.0
        ):
            return cls._cached_close, cls._cached_high, cls._cached_low

        async with cls._lock:
            now_ts = time.time()
            if cls._nq_session_id != sess_id:
                cls._nq_session_id = sess_id
                cls._cached_close = 0.0
                cls._cached_high = 0.0
                cls._cached_low = 0.0
                cls._cached_vel_10m = 0.0
                cls._cached_vel_3m = 0.0
            elif (
                not force_refresh
                and (now_ts - cls._last_fetch_ts < cls.CACHE_TTL_SEC)
                and cls._cached_high > 0.0
                and cls._cached_low > 0.0
            ):
                return cls._cached_close, cls._cached_high, cls._cached_low

            try:
                c_val, h_val, l_val, v10, v3 = await asyncio.wait_for(
                    asyncio.to_thread(
                        cls._fetch_nq_sync,
                        nq_sess_start,
                        is_cme_open,
                        cls._cached_high,
                        cls._cached_low,
                        cls._cached_close,
                    ),
                    timeout=8.0,
                )
                if h_val > 0.0 and l_val > 0.0:
                    cls._cached_close = c_val
                    cls._cached_high = h_val
                    cls._cached_low = l_val
                    cls._cached_vel_10m = v10
                    cls._cached_vel_3m = v3
                    cls._last_fetch_ts = time.time()
            except Exception as e:
                print(f"🚨 [MacroDataCache 갱신 타임아웃 방어] {e}", flush=True)

            return cls._cached_close, cls._cached_high, cls._cached_low

    @classmethod
    async def get_nq_short_velocity(cls, force_refresh: bool = False) -> tuple[float, float]:
        """초과 Case 89 & Case 91: 토스 Open API QQQ 0.0초 실시간 10분/3분 변화율 최우선 반환 (미수신 시 NQ=F 폴백)."""
        await cls._refresh_qqq_snapshot_if_needed(force_refresh=force_refresh, realtime_probing=False)
        if time.time() - cls._qqq_vel_ts < 60.0 and cls._qqq_last_price > 0.0:
            return cls._qqq_vel_10m, cls._qqq_vel_3m

        await cls.get_cached_nq_data(force_refresh=force_refresh)
        return cls._cached_vel_10m, cls._cached_vel_3m

    @classmethod
    async def get_stage3_reference_price(
        cls, force_refresh: bool = False, realtime_probing: bool = False
    ) -> tuple[float, str]:
        """
        초과 Case 92 & Case 94:
        3차 역행 안전망의 진입 기준가(nq_entry_price) 및 실시간 비교가(ref_curr)로
        토스 Open API QQQ 0.0초 실시간 현재가를 최우선 반환합니다 (미수신 시 NQ=F 폴백).
        """
        await cls._refresh_qqq_snapshot_if_needed(
            force_refresh=force_refresh, realtime_probing=realtime_probing
        )
        if cls._qqq_last_price > 0.0 and (time.time() - cls._qqq_last_ts < 120.0):
            return round(cls._qqq_last_price, 4), "QQQ실시간"

        nq_c, _, _ = await cls.get_cached_nq_data(force_refresh=force_refresh)
        return round(nq_c, 2), "NQ선물"

    @staticmethod
    def _fetch_nq_10m_trend_sync() -> tuple[str, float, float, float]:
        """
        초과 Case 93:
        데이장 개시 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물지수(NQ=F) 10분봉 양/음봉 판별.
        yfinance 무료 1분봉의 10분 시세 지연으로 인해 08:50~08:59 KST 10분봉(예: 08:50 KST 음봉)이
        누락되는 현상을 차단하기 위해 tkr.fast_info.last_price (0.0초 실시간 시세)를 현재 10분봉 종가에 강제 병합합니다.
        """
        try:
            tkr = yf.Ticker("NQ=F")
            fast_last = 0.0
            try:
                fi = tkr.fast_info
                if fi is not None:
                    fast_last = float(getattr(fi, "last_price", 0.0) or fi.get("lastPrice", 0.0) or 0.0)
            except Exception:
                pass

            df_1m = tkr.history(period="2d", interval="1m")
            if df_1m is None or df_1m.empty:
                return "", 0.0, 0.0, 0.0

            if df_1m.index.tz is None:
                df_1m.index = df_1m.index.tz_localize("UTC").tz_convert(EST_TZ)
            else:
                df_1m.index = df_1m.index.tz_convert(EST_TZ)

            df_1m = df_1m[(df_1m["Open"] > 0.0) & (df_1m["Close"] > 0.0)].copy()
            if df_1m.empty:
                return "", 0.0, 0.0, 0.0

            now_est = datetime.now(EST_TZ)
            _, nq_sess_start = get_cme_session_window_est(now_est)
            df_sess = df_1m[df_1m.index >= nq_sess_start].copy()
            if df_sess.empty:
                df_sess = df_1m.tail(30).copy()

            df_sess["b10"] = df_sess.index.floor("10min")
            df_10m = df_sess.groupby("b10", sort=True).agg(
                o10=("Open", "first"),
                h10=("High", "max"),
                l10=("Low", "min"),
                c10=("Close", "last"),
            )
            if df_10m.empty:
                return "", 0.0, 0.0, 0.0

            target_b10 = now_est.replace(hour=18, minute=50, second=0, microsecond=0)
            if now_est.hour < 18:
                target_b10 = (now_est - timedelta(days=1)).replace(hour=18, minute=50, second=0, microsecond=0)

            if target_b10 in df_10m.index:
                o_10 = float(df_10m.loc[target_b10, "o10"])
                c_10 = float(df_10m.loc[target_b10, "c10"])
                if fast_last > 0.0 and (now_est.hour == 18 and now_est.minute >= 50):
                    c_10 = fast_last
            else:
                last_b10_dt = df_10m.index[-1]
                age_mins = (now_est - last_b10_dt).total_seconds() / 60.0
                if fast_last > 0.0 and age_mins >= 10.0:
                    o_10 = float(df_10m["c10"].iloc[-1])
                    c_10 = fast_last
                else:
                    o_10 = float(df_10m["o10"].iloc[-1])
                    c_10 = fast_last if fast_last > 0.0 else float(df_10m["c10"].iloc[-1])

            if o_10 <= 0.0 or c_10 <= 0.0:
                return "", 0.0, 0.0, 0.0

            body_pct = (c_10 - o_10) / o_10 * 100.0
            trend_sym = "SOXL" if c_10 >= o_10 else "SOXS"
            return trend_sym, round(o_10, 2), round(c_10, 2), round(body_pct, 3)
        except Exception as e:
            print(f"🚨 [NQ=F 10분봉 추세 판별 방어] {e}", flush=True)
            return "", 0.0, 0.0, 0.0

    @classmethod
    async def get_nq_10m_candle_trend(cls, force_refresh: bool = False) -> tuple[str, float, float, float]:
        now_ts = time.time()
        if (
            not force_refresh
            and cls._nq_10m_trend_sym in ("SOXL", "SOXS")
            and (now_ts - cls._nq_10m_trend_ts < 20.0)
        ):
            return cls._nq_10m_trend_sym, cls._nq_10m_open, cls._nq_10m_close, cls._nq_10m_body_pct

        try:
            t_sym, o_10, c_10, b_pct = await asyncio.wait_for(
                asyncio.to_thread(cls._fetch_nq_10m_trend_sync),
                timeout=6.0,
            )
            if t_sym in ("SOXL", "SOXS"):
                cls._nq_10m_trend_sym = t_sym
                cls._nq_10m_open = o_10
                cls._nq_10m_close = c_10
                cls._nq_10m_body_pct = b_pct
                cls._nq_10m_trend_ts = time.time()
        except Exception:
            pass
        return cls._nq_10m_trend_sym, cls._nq_10m_open, cls._nq_10m_close, cls._nq_10m_body_pct


class AssassinLedger:
    """
    제1헌법, Case 02, 06, 09, 30, 41, 52, 54, 76, 82, 86, 94, 95, 96:
    듀얼 스나이퍼(SOXS / SOXL) 허용 종목 화이트리스트 강제 및 원자적 로컬 JSON 원장 통제 코어.
    """
    ALLOWED_SYMBOLS = ("SOXS", "SOXL")

    @classmethod
    def _validate_symbol(cls, symbol: str):
        if symbol not in cls.ALLOWED_SYMBOLS:
            raise ValueError(f"🚨 [화이트리스트 위반 차단] 비허용 종목 접근 거부: {symbol}")

    @classmethod
    def _get_filepath(cls, symbol: str) -> str:
        cls._validate_symbol(symbol)
        return f"assassin_ledger_{symbol}.json"

    @classmethod
    def _default_payload(cls) -> dict:
        return {
            "price": 0.0,
            "budget": 100.0,
            "target_profit_rate": SINGLE_TARGET_PROFIT_RATE,
            "is_compound": True,
            "is_session_done": False,
            "is_active": True,
            "target_sell_price": 0.0,
            "buy_order_id": "",
            "sell_order_id": "",
            "entry_session": "",
            "entry_time": 0.0,
            "nq_entry_price": 0.0,
            "nq_entry_amp": 0.0,
            "entry_trend": "",
            "last_session_id": "",
            "whipsaw_target_price": 0.0,
        }

    @classmethod
    def _read_sync(cls, symbol: str) -> dict:
        filepath = cls._get_filepath(symbol)
        with _get_hybrid_file_lock(filepath):
            if not os.path.exists(filepath):
                return cls._default_payload()
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    return cls._default_payload()
                base = cls._default_payload()
                base.update(data)
                base["target_profit_rate"] = normalize_target_rate(base.get("target_profit_rate", 1.0))
                return base
            except Exception as e:
                print(f"🚨 [원장 읽기 방어 {symbol}] {e}", flush=True)
                return cls._default_payload()

    @classmethod
    def _write_sync(cls, symbol: str, updates: dict) -> dict:
        filepath = cls._get_filepath(symbol)
        with _get_hybrid_file_lock(filepath):
            cur = cls._default_payload()
            if os.path.exists(filepath):
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        loaded = json.load(f)
                    if isinstance(loaded, dict):
                        cur.update(loaded)
                except Exception:
                    pass

            for k, v in updates.items():
                if v is not None:
                    cur[k] = v

            cur["target_profit_rate"] = normalize_target_rate(cur.get("target_profit_rate", 1.0))
            tmp_path = f"{filepath}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(cur, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, filepath)
            return cur

    @classmethod
    async def get_state(cls, symbol: str) -> _LedgerStateTuple:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        core_13 = (
            float(data.get("price") or 0.0),
            float(data.get("budget") or 100.0),
            normalize_target_rate(data.get("target_profit_rate", 1.0)),
            bool(data.get("is_compound", True)),
            bool(data.get("is_session_done", False)),
            bool(data.get("is_active", True)),
            float(data.get("target_sell_price") or 0.0),
            str(data.get("sell_order_id") or ""),
            str(data.get("entry_session") or ""),
            float(data.get("entry_time") or 0.0),
            float(data.get("nq_entry_price") or 0.0),
            float(data.get("nq_entry_amp") or 0.0),
            str(data.get("entry_trend") or ""),
        )
        return _LedgerStateTuple(
            core_13,
            whipsaw_target_price=float(data.get("whipsaw_target_price") or 0.0),
        )

    @classmethod
    async def get_raw_dict(cls, symbol: str) -> dict:
        return await asyncio.to_thread(cls._read_sync, symbol)

    @classmethod
    async def save_state(cls, symbol: str, **kwargs) -> dict:
        return await asyncio.to_thread(cls._write_sync, symbol, kwargs)

    @classmethod
    async def get_buy_order_id(cls, symbol: str) -> str:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        return str(data.get("buy_order_id") or "")

    @classmethod
    async def get_sell_order_id(cls, symbol: str) -> str:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        return str(data.get("sell_order_id") or "")

    @classmethod
    async def get_last_session_id(cls, symbol: str) -> str:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        return str(data.get("last_session_id") or "")

    @classmethod
    async def get_whipsaw_target_price(cls, symbol: str) -> float:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        return float(data.get("whipsaw_target_price") or 0.0)

    @classmethod
    async def get_is_compound(cls, symbol: str = "SOXS") -> bool:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        return bool(data.get("is_compound", True))

    @classmethod
    async def set_is_compound(cls, is_compound: bool, symbol: str = None):
        if isinstance(is_compound, str) and symbol is not None:
            is_compound = bool(symbol)
        for sym in cls.ALLOWED_SYMBOLS:
            await asyncio.to_thread(cls._write_sync, sym, {"is_compound": bool(is_compound)})

    @classmethod
    async def toggle_compound(cls, symbol: str = "SOXS") -> bool:
        cur = await cls.get_is_compound(symbol)
        new_val = not cur
        for sym in cls.ALLOWED_SYMBOLS:
            await asyncio.to_thread(cls._write_sync, sym, {"is_compound": new_val})
        return new_val

    @classmethod
    async def toggle_active(cls, symbol: str = "SOXS") -> bool:
        data = await asyncio.to_thread(cls._read_sync, symbol)
        new_act = not bool(data.get("is_active", True))
        for sym in cls.ALLOWED_SYMBOLS:
            await asyncio.to_thread(cls._write_sync, sym, {"is_active": new_act})
        return new_act

    @classmethod
    async def sync_global_settings(
        cls,
        budget: float = None,
        is_active: bool = None,
        is_compound: bool = None,
        target_profit_rate: float = None,
    ):
        updates = {}
        if budget is not None and math.isfinite(float(budget)) and float(budget) > 0.0:
            updates["budget"] = round(float(budget), 2)
        if is_active is not None:
            updates["is_active"] = bool(is_active)
        if is_compound is not None:
            updates["is_compound"] = bool(is_compound)
        if target_profit_rate is not None:
            updates["target_profit_rate"] = SINGLE_TARGET_PROFIT_RATE

        if updates:
            for sym in cls.ALLOWED_SYMBOLS:
                await asyncio.to_thread(cls._write_sync, sym, updates)

    @classmethod
    async def reset_ledger(cls, symbol: str):
        cur = await asyncio.to_thread(cls._read_sync, symbol)
        preserved_budget = float(cur.get("budget") or 100.0)
        preserved_comp = bool(cur.get("is_compound", True))
        preserved_active = bool(cur.get("is_active", True))
        clean = cls._default_payload()
        clean.update({
            "budget": preserved_budget,
            "is_compound": preserved_comp,
            "is_active": preserved_active,
        })
        await asyncio.to_thread(cls._write_sync, symbol, clean)

    @classmethod
    async def reset_all_ledgers(cls):
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.reset_ledger(sym)

    @classmethod
    async def unlock_all_symbols_for_reentry(cls):
        """
        초과 Case 52, 86, 95:
        프리장 골든타임(04:00 ~ 08:14 EST) 내 익절 또는 NQ<2.0% 3차 손절 후 재출동을 위해
        양쪽 장부의 is_session_done을 해제하되, 당일 NQ>=2.0% 과열차단(CANCELED_MACRO) 상태는 보호합니다.
        """
        _, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
        nq_amp = (((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0)
        is_overheated = (nq_amp >= 2.0)

        for sym in cls.ALLOWED_SYMBOLS:
            cur = await asyncio.to_thread(cls._read_sync, sym)
            es = str(cur.get("entry_session") or "")
            if is_overheated:
                if es != "MANUAL":
                    await asyncio.to_thread(
                        cls._write_sync,
                        sym,
                        {
                            "is_session_done": True,
                            "entry_session": "CANCELED_MACRO",
                            "whipsaw_target_price": 0.0,
                        },
                    )
            else:
                new_es = es if es in ("MANUAL", "STAGE3_STOP_TRACK") else ""
                await asyncio.to_thread(
                    cls._write_sync,
                    sym,
                    {
                        "is_session_done": False,
                        "entry_session": new_es,
                        "whipsaw_target_price": 0.0,
                    },
                )

    @classmethod
    async def lock_all_symbols_macro(cls, last_session_id: str = None):
        """
        초과 Case 82, 94, 95, 96:
        1·2차 매크로 과열 발생 시 양쪽 장부를 CANCELED_MACRO로 동기화 잠금하되,
        수동 보유(MANUAL) 중인 종목과 03:58~04:06 EST 진행 중인 1주 휩소(WHIPSAW_1SH)는 보존하고,
        04:07 EST 이후 미체결 WHIPSAW_1SH의 buy_order_id는 즉시 ""로 소각합니다.
        """
        now_est = datetime.now(EST_TZ)
        est_hm = now_est.hour * 100 + now_est.minute

        for sym in cls.ALLOWED_SYMBOLS:
            cur = await asyncio.to_thread(cls._read_sync, sym)
            es = str(cur.get("entry_session") or "")
            has_pos = (
                float(cur.get("price") or 0.0) > 0.0
                or float(cur.get("target_sell_price") or 0.0) > 0.0
                or bool(cur.get("sell_order_id"))
            )

            if es == "MANUAL" and has_pos:
                continue

            if es == "WHIPSAW_1SH":
                if 358 <= est_hm <= 406 or has_pos:
                    continue
                updates = {
                    "buy_order_id": "",
                    "is_session_done": True,
                    "entry_session": "CANCELED_MACRO",
                    "whipsaw_target_price": 0.0,
                }
                if last_session_id:
                    updates["last_session_id"] = last_session_id
                await asyncio.to_thread(cls._write_sync, sym, updates)
                continue

            updates = {
                "is_session_done": True,
                "entry_session": "CANCELED_MACRO",
                "whipsaw_target_price": 0.0,
            }
            if last_session_id:
                updates["last_session_id"] = last_session_id
            await asyncio.to_thread(cls._write_sync, sym, updates)
