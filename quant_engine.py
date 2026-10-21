# -*- coding: utf-8 -*-
"""
# NEW: quant_engine.py
매크로 선물(NQ=F) 18:00 EST 신세션 방어/자가치유 캐시, 토스 QQQ 0.0초 실시간 속도/역행 연산 및
1분봉 아카이빙/GC 통합 엔진 (Step 03 / 10)
- 제1헌법: 모든 yfinance 조회 및 로컬 파일 I/O의 asyncio.to_thread 비동기 격리 및 GlobalThrottle 통제
- 제4헌법: 모든 세션 경계 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 4 & Case 90: 토스 QQQ 0.0초 실시간 10m/3m ±0.20% 급등락 감지 시 HA음봉 바이패스(NQ_VEL_BYPASS)
- 🎯 5 & Case 61, 62, 91 ~ 94:
  1) 18:00 EST CME 선물 신세션 오픈 시 전일 fast_info/1d 잔재 혼입 100% 차단 및 0.00% 영점 출발
  2) 18:10 EST 이후 오염된 prev_h / prev_l 감지 시 신세션 1m + fast_last 범위로 자가 치유(Self-Healing)
  3) 17:00 ~ 17:59 EST 마감 휴장 및 주말(금 17:00 ~ 일 17:59 EST) 직전 마감 세션 고/저가 단조 보존
  4) 포지션 자체 손실률 즉시 손절 금지 & NQ 진폭 기반 동적 임계치 max(nq_amp*0.5, 0.80%) × QQQ 역행 판정
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

from quant_indicators_plugin import (
    candles_to_est_dataframe,
    get_logical_trading_date_str,
)
from toss_api import EST_TZ, GlobalThrottle, TossInvestAPI, safe_float

logger = logging.getLogger("toss_sniper.quant_engine")

# 3차 매크로 방어선 동적 임계치 최소 하한선 (0.80% 하드 락온, Case 69 & Case 94)
NQ_DYNAMIC_FLOOR: float = 0.80
# 1·2차 매크로 과열 방어선 절대 진폭 임계치 (2.00% 하드 락온, Case 66)
NQ_OVERHEAT_THRESHOLD: float = 2.00
# HA음봉 바이패스용 QQQ 10m / 3m 유리 방향 급등락 임계치 (±0.20%, Case 90)
QQQ_VELOCITY_BYPASS_THRESHOLD: float = 0.20


# ---------------------------------------------------------------------------
# CME 나스닥 100 선물(NQ=F) 세션 경계 시각 산출 유틸리티 (Case 01, 62, 91)
# ---------------------------------------------------------------------------
def get_cme_futures_session_bounds(
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    현재 시각(EST) 기준 CME 나스닥 100 선물(NQ=F)의 현행 논리 세션 시작 시각(18:00 EST)과
    직전 마감 세션의 시작/종료 시각, 그리고 마감 휴장(17:00 ~ 17:59 EST) 및 주말 여부를 반환한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    hm = dt.hour * 100 + dt.minute

    # 금요일 17:00 EST ~ 일요일 17:59 EST 주말 휴장 판정 (Case 01)
    is_weekend_closed = bool(
        (wd == 4 and hm >= 1700)
        or (wd == 5)
        or (wd == 6 and hm < 1800)
    )
    # 평일/일요일 17:00 ~ 17:59 EST 선물 일일 마감 휴장 구간
    is_daily_maintenance = bool(1700 <= hm <= 1759)

    if is_weekend_closed:
        # 주말에는 직전 금요일 마감 세션(목 18:00 ~ 금 17:00 EST)을 보존 기준으로 삼는다
        days_since_fri = (wd - 4) % 7
        fri_date = (dt - timedelta(days=days_since_fri)).replace(
            hour=17, minute=0, second=0, microsecond=0
        )
        curr_sess_start = fri_date - timedelta(hours=23)  # 목요일 18:00 EST
        prev_sess_end = curr_sess_start - timedelta(hours=1)
        prev_sess_start = prev_sess_end - timedelta(hours=23)
        session_id = fri_date.strftime("%Y-%m-%d")
    elif hm >= 1800:
        # 18:00 ~ 23:59 EST: 당일 18:00 EST 에 신세션 오픈 (논리 거래일은 익일)
        curr_sess_start = dt.replace(hour=18, minute=0, second=0, microsecond=0)
        prev_sess_end = dt.replace(hour=17, minute=0, second=0, microsecond=0)
        prev_sess_start = prev_sess_end - timedelta(hours=23)
        session_id = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # 00:00 ~ 17:59 EST: 전일 18:00 EST 에 오픈한 세션 (논리 거래일은 당일)
        curr_sess_start = (dt - timedelta(days=1)).replace(
            hour=18, minute=0, second=0, microsecond=0
        )
        prev_sess_end = curr_sess_start - timedelta(hours=1)
        prev_sess_start = prev_sess_end - timedelta(hours=23)
        session_id = dt.strftime("%Y-%m-%d")

    is_early_new_session_window = bool(
        not is_weekend_closed and 1800 <= hm <= 1810
    )

    return {
        "now_est": dt,
        "session_id": session_id,
        "curr_sess_start": curr_sess_start,
        "prev_sess_start": prev_sess_start,
        "prev_sess_end": prev_sess_end,
        "is_weekend_closed": is_weekend_closed,
        "is_daily_maintenance": is_daily_maintenance,
        "is_early_new_session_window": is_early_new_session_window,
    }


# ---------------------------------------------------------------------------
# MacroDataCache : NQ=F 18:00 EST 신세션 방어/자가치유 & QQQ 0.0초 실시간 캐시
# ---------------------------------------------------------------------------
class MacroDataCache:
    """
    🎯 5 & Case 61, 62, 90 ~ 94:
    1) NQ=F 선물 진폭(nq_amp) 및 고/저가 추적:
       - 18:00 ~ 18:10 EST 신세션 초입 df_session 미수신 시 fast_info.day_high/low 및 1d 병합 100% 차단,
         오직 fast_last 단일 영점(0.00%)으로 출발
       - 18:10 EST 이후 전일 잔재로 오염된 prev_h / prev_l 감지 시 신세션 1m + fast_last 로 자가 치유
       - 17:00 ~ 17:59 EST 마감 휴장 및 주말에는 직전 마감 세션 고/저가/마감가 100% 단조 보존
    2) 토스 Open API QQQ 0.0초 실시간 현재가 및 10m/3m 단기 급등락 속도(±0.20% NQ_VEL_BYPASS) 추적:
       - 17:00 ~ 18:59 EST 구간에도 초기 1회 즉시 QQQ 시세를 확보하여 선물 스케일 오염 차단
    """

    def __init__(self, api: TossInvestAPI, nq_ttl_sec: float = 15.0) -> None:
        self.api = api
        self.nq_ttl_sec = float(nq_ttl_sec)

        self._lock = threading.RLock()
        self._nq_state: Dict[str, Any] = {
            "session_id": "",
            "nq_price": 0.0,
            "nq_high": 0.0,
            "nq_low": 0.0,
            "nq_amp": 0.0,
            "prev_sess_high": 0.0,
            "prev_sess_low": 0.0,
            "updated_mono": 0.0,
            "updated_est_str": "",
            "is_self_healed": False,
        }

        # QQQ 실시간 틱 히스토리 (최근 20분치 (epoch_sec, qqq_price) 링버퍼)
        self._qqq_tick_history: Deque[Tuple[float, float]] = deque(maxlen=3600)
        self._qqq_last_price: float = 0.0
        self._qqq_last_fetch_mono: float = 0.0
        self._qqq_1m_cache_mono: float = 0.0
        self._qqq_1m_df: pd.DataFrame = pd.DataFrame()

    def reset_runtime_cache(self) -> None:
        """
        Case 02: /reset 명령 시 QQQ 직전 시세는 보존하되 오염 가능한 세션 고/저가 캐시를 초기화한다.
        """
        with self._lock:
            self._nq_state["updated_mono"] = 0.0

    # -----------------------------------------------------------------------
    # 내부 동기 NQ=F 조회 및 18:00 EST 전일 잔재 차단 / 자가 치유 코어
    # -----------------------------------------------------------------------
    def _fetch_nq_sync(self, now_est: Optional[datetime] = None) -> Dict[str, Any]:
        """
        백그라운드 스레드(asyncio.to_thread)에서 실행되는 NQ=F 조회 및 세션 정합성 검증 코어.
        """
        bounds = get_cme_futures_session_bounds(now_est)
        dt_now: datetime = bounds["now_est"]
        sess_id: str = bounds["session_id"]
        curr_start: datetime = bounds["curr_sess_start"]
        prev_start: datetime = bounds["prev_sess_start"]
        prev_end: datetime = bounds["prev_sess_end"]
        is_closed_preserve: bool = (
            bounds["is_weekend_closed"] or bounds["is_daily_maintenance"]
        )

        with self._lock:
            prev_state = dict(self._nq_state)

        # 세션 롤오버(18:00 EST 신세션 진입) 감지 시 이전 세션 고/저가 분리 보관 후 즉시 0점 리셋
        if prev_state.get("session_id") != sess_id:
            old_h = safe_float(prev_state.get("nq_high"), 0.0)
            old_l = safe_float(prev_state.get("nq_low"), 0.0)
            prev_state = {
                "session_id": sess_id,
                "nq_price": 0.0,
                "nq_high": 0.0,
                "nq_low": 0.0,
                "nq_amp": 0.0,
                "prev_sess_high": old_h if old_h > 0.0 else safe_float(prev_state.get("prev_sess_high"), 0.0),
                "prev_sess_low": old_l if old_l > 0.0 else safe_float(prev_state.get("prev_sess_low"), 0.0),
                "updated_mono": 0.0,
                "updated_est_str": dt_now.strftime("%H:%M:%S"),
                "is_self_healed": False,
            }

        # 17:00 ~ 17:59 EST 마감 휴장 또는 주말에 이미 유효한 마감 세션 데이터가 있으면 단조 보존
        if (
            is_closed_preserve
            and safe_float(prev_state.get("nq_price"), 0.0) > 0.0
            and safe_float(prev_state.get("nq_high"), 0.0) > 0.0
            and safe_float(prev_state.get("nq_low"), 0.0) > 0.0
        ):
            prev_state["updated_mono"] = time.monotonic()
            prev_state["updated_est_str"] = dt_now.strftime("%H:%M:%S")
            with self._lock:
                self._nq_state = dict(prev_state)
            return prev_state

        if yf is None:
            return prev_state

        GlobalThrottle.wait_api_sync("MARKET_INFO")
        ticker = yf.Ticker("NQ=F")

        # 1) fast_info 추출
        fast_last = 0.0
        fast_high = 0.0
        fast_low = 0.0
        try:
            fi = ticker.fast_info
            if fi is not None:
                fast_last = safe_float(getattr(fi, "last_price", None), 0.0)
                fast_high = safe_float(getattr(fi, "day_high", None), 0.0)
                fast_low = safe_float(getattr(fi, "day_low", None), 0.0)
        except Exception:
            pass

        # 2) 1분봉(5d, prepost=True) 조회 후 America/New_York 타임존 변환 (Case 62)
        df_1m = pd.DataFrame()
        try:
            raw_1m = ticker.history(period="5d", interval="1m", prepost=True)
            if isinstance(raw_1m, pd.DataFrame) and not raw_1m.empty:
                idx = raw_1m.index
                if idx.tz is None:
                    idx = idx.tz_localize("UTC").tz_convert(EST_TZ)
                else:
                    idx = idx.tz_convert(EST_TZ)
                df_1m = raw_1m.copy()
                df_1m.index = idx
                df_1m = df_1m[df_1m["Close"] > 0.0]
        except Exception as exc:
            logger.warning("⚠️ yfinance NQ=F 1m 조회 실패: %s", exc)

        # 직전 마감 세션(prev_start ~ prev_end) 및 현재 신세션(index >= curr_start) 분리
        df_prev_sess = (
            df_1m[(df_1m.index >= prev_start) & (df_1m.index <= prev_end)]
            if not df_1m.empty
            else pd.DataFrame()
        )
        df_session = (
            df_1m[df_1m.index >= curr_start]
            if not df_1m.empty
            else pd.DataFrame()
        )

        prev_sess_h = (
            float(df_prev_sess["High"].max())
            if not df_prev_sess.empty
            else safe_float(prev_state.get("prev_sess_high"), 0.0)
        )
        prev_sess_l = (
            float(df_prev_sess["Low"].min())
            if not df_prev_sess.empty
            else safe_float(prev_state.get("prev_sess_low"), 0.0)
        )

        prev_h = safe_float(prev_state.get("nq_high"), 0.0)
        prev_l = safe_float(prev_state.get("nq_low"), 0.0)
        self_healed = False

        # -------------------------------------------------------------------
        # 🎯 5 & Case 91:
        # [분기 A] 신세션 1분봉(df_session)이 아직 비어 있는 경우 (18:00 ~ 18:10 EST 지연 구간)
        # -------------------------------------------------------------------
        if df_session.empty:
            if is_closed_preserve and not df_prev_sess.empty:
                # 마감 휴장/주말 중 기동하여 당일 세션이 곧 직전 마감 세션인 경우
                cur_c = float(df_prev_sess["Close"].iloc[-1])
                cur_h = float(df_prev_sess["High"].max())
                cur_l = float(df_prev_sess["Low"].min())
            else:
                # 18:00 EST 신세션 직후: 전일 fast_info.day_high/low 및 1d 병합 100% 금지!
                # 오직 실시간 체결가 fast_last 단일 영점(진폭 0.00%)으로 출발한다.
                if fast_last <= 0.0 and not df_1m.empty:
                    fast_last = float(df_1m["Close"].iloc[-1])
                cur_c = fast_last

                # 기존 prev_h / prev_l 이 직전 마감 세션 고/저가와 일치하거나
                # fast_last 대비 0.20% 이상 벌어져 있으면 전일 잔재 오염이므로 즉시 폐기!
                is_prev_contaminated = False
                if prev_h > 0.0 and prev_l > 0.0 and cur_c > 0.0:
                    if (
                        (prev_sess_h > 0.0 and abs(prev_h - prev_sess_h) < 1.0)
                        or (prev_sess_l > 0.0 and abs(prev_l - prev_sess_l) < 1.0)
                        or ((prev_h - cur_c) / cur_c) * 100.0 >= 0.20
                        or ((cur_c - prev_l) / cur_c) * 100.0 >= 0.20
                    ):
                        is_prev_contaminated = True
                        self_healed = True

                if is_prev_contaminated or prev_h <= 0.0 or prev_l <= 0.0:
                    cur_h = cur_c
                    cur_l = cur_c
                else:
                    cur_h = max(prev_h, cur_c)
                    cur_l = min(prev_l, cur_c)

        # -------------------------------------------------------------------
        # [분기 B] 신세션 1분봉(df_session)이 존재하는 경우 (18:10 EST 이후 정상 세션)
        # -------------------------------------------------------------------
        else:
            sess_bar_h = float(df_session["High"].max())
            sess_bar_l = float(df_session["Low"].min())
            sess_bar_c = float(df_session["Close"].iloc[-1])

            cur_c = fast_last if fast_last > 0.0 else sess_bar_c
            true_sess_h = max(sess_bar_h, cur_c)
            true_sess_l = min(sess_bar_l, cur_c) if sess_bar_l > 0.0 else cur_c

            # 1) 기존 캐시(prev_h, prev_l)의 전일 잔재 오염 검증 및 자가 치유(Self-Healing)
            if prev_h > 0.0 and prev_l > 0.0 and true_sess_l > 0.0:
                exceed_high_pct = ((prev_h - true_sess_h) / true_sess_h) * 100.0
                exceed_low_pct = ((true_sess_l - prev_l) / true_sess_l) * 100.0
                matches_prev_sess = bool(
                    (prev_sess_h > 0.0 and abs(prev_h - prev_sess_h) < 1.0 and prev_h > true_sess_h)
                    or (prev_sess_l > 0.0 and abs(prev_l - prev_sess_l) < 1.0 and prev_l < true_sess_l)
                )
                if matches_prev_sess or exceed_high_pct >= 0.20 or exceed_low_pct >= 0.20:
                    logger.info(
                        "🩹 [Self-Healing] NQ=F 오염 캐시 자가 치유 발동: 기존(H=%.2f, L=%.2f) -> 신세션(H=%.2f, L=%.2f)",
                        prev_h,
                        prev_l,
                        true_sess_h,
                        true_sess_l,
                    )
                    prev_h = true_sess_h
                    prev_l = true_sess_l
                    self_healed = True

            # 2) fast_info.day_high / day_low 병합 시 전일 잔재 여부 엄격 검증
            cand_h = max(true_sess_h, prev_h) if prev_h > 0.0 else true_sess_h
            cand_l = min(true_sess_l, prev_l) if prev_l > 0.0 else true_sess_l

            if fast_high > 0.0 and true_sess_h > 0.0:
                is_fast_h_prev_remnant = bool(
                    (prev_sess_h > 0.0 and abs(fast_high - prev_sess_h) < 1.0 and fast_high > true_sess_h)
                    or (((fast_high - true_sess_h) / true_sess_h) * 100.0 >= 0.25)
                )
                if not is_fast_h_prev_remnant:
                    cand_h = max(cand_h, fast_high)

            if fast_low > 0.0 and true_sess_l > 0.0:
                is_fast_l_prev_remnant = bool(
                    (prev_sess_l > 0.0 and abs(fast_low - prev_sess_l) < 1.0 and fast_low < true_sess_l)
                    or (((true_sess_l - fast_low) / true_sess_l) * 100.0 >= 0.25)
                )
                if not is_fast_l_prev_remnant:
                    cand_l = min(cand_l, fast_low)

            cur_h = cand_h
            cur_l = cand_l

        nq_amp = (
            ((cur_h - cur_l) / cur_l) * 100.0
            if (cur_l > 0.0 and cur_h >= cur_l)
            else 0.0
        )

        new_state = {
            "session_id": sess_id,
            "nq_price": round(cur_c, 2),
            "nq_high": round(cur_h, 2),
            "nq_low": round(cur_l, 2),
            "nq_amp": round(nq_amp, 2),
            "prev_sess_high": round(prev_sess_h, 2),
            "prev_sess_low": round(prev_sess_l, 2),
            "updated_mono": time.monotonic(),
            "updated_est_str": dt_now.strftime("%H:%M:%S"),
            "is_self_healed": self_healed,
        }
        with self._lock:
            self._nq_state = dict(new_state)
        return new_state

    # -----------------------------------------------------------------------
    # 토스 Open API QQQ 0.0초 실시간 시세 및 10m / 3m 단기 속도 산출 (Case 90, 93)
    # -----------------------------------------------------------------------
    def record_qqq_tick(self, qqq_price: float, now_ts: Optional[float] = None) -> None:
        """
        메인 루프에서 배치 조회된 실시간 QQQ 현재가를 링버퍼에 즉시 적재한다.
        """
        qp = safe_float(qqq_price, 0.0)
        if qp <= 0.0:
            return
        ts = float(now_ts if now_ts is not None else time.time())
        with self._lock:
            self._qqq_last_price = qp
            self._qqq_last_fetch_mono = time.monotonic()
            self._qqq_tick_history.append((ts, qp))

    async def ensure_qqq_live_price(
        self, provided_qqq_price: float = 0.0, force_refresh_sec: float = 1.5
    ) -> float:
        """
        Case 61 & Case 93:
        - 외부에서 이미 배치 조회된 provided_qqq_price(> 0)가 있으면 즉시 기록 후 반환한다.
        - 17:00 ~ 18:59 EST 구간을 포함하여 _qqq_last_price 가 0.0 이거나
          force_refresh_sec 이상 경과했으면 토스 Open API GET /api/v1/prices?symbols=QQQ 를
          즉시 호출하여 선물 지수 스케일(20,000pt) 오염을 원천 차단한다.
        """
        qp = safe_float(provided_qqq_price, 0.0)
        if qp > 0.0:
            self.record_qqq_tick(qp)
            return qp

        with self._lock:
            cached_qp = self._qqq_last_price
            elapsed = time.monotonic() - self._qqq_last_fetch_mono

        if cached_qp > 0.0 and elapsed < force_refresh_sec:
            return cached_qp

        fetched_qp = await self.api.get_current_price("QQQ")
        if fetched_qp > 0.0:
            self.record_qqq_tick(fetched_qp)
            return fetched_qp
        return cached_qp

    async def get_qqq_velocity_metrics(
        self, live_qqq_price: float = 0.0
    ) -> Dict[str, Any]:
        """
        🎯 4 & Case 90:
        토스 Open API QQQ 실시간 틱 히스토리(부족 시 QQQ 1분봉 15개 보충)를 기반으로
        최근 10분 변화율(qqq_10m_chg_pct)과 3분 변화율(qqq_3m_chg_pct)을 산출하여,
        보유 포지션 유리 방향 ±0.20% 급등락 시 HA음봉을 바이패스(🚀 NQ_VEL_BYPASS)할지 판정한다.
        """
        qp = await self.ensure_qqq_live_price(live_qqq_price)
        now_epoch = time.time()

        # 링버퍼가 아직 10분치를 채우지 못한 기동 직후에는 60초 주기로 QQQ 1분봉 15개를 보충 조회
        with self._lock:
            oldest_epoch = (
                self._qqq_tick_history[0][0] if self._qqq_tick_history else now_epoch
            )
            need_candle_backfill = bool((now_epoch - oldest_epoch) < 540.0)
            candle_cache_age = time.monotonic() - self._qqq_1m_cache_mono

        if need_candle_backfill and candle_cache_age >= 60.0:
            try:
                qqq_candles, _ = await self.api.get_candles(
                    symbol="QQQ", interval="1m", count=20, adjusted=True
                )
                df_q = candles_to_est_dataframe(qqq_candles)
                with self._lock:
                    self._qqq_1m_df = df_q
                    self._qqq_1m_cache_mono = time.monotonic()
            except Exception:
                pass

        p_3m_ago = 0.0
        p_10m_ago = 0.0
        target_3m = now_epoch - 180.0
        target_10m = now_epoch - 600.0

        with self._lock:
            for ts_val, price_val in reversed(self._qqq_tick_history):
                if p_3m_ago <= 0.0 and ts_val <= target_3m:
                    p_3m_ago = price_val
                if p_10m_ago <= 0.0 and ts_val <= target_10m:
                    p_10m_ago = price_val
                    break

            df_q_ref = self._qqq_1m_df

        if (p_3m_ago <= 0.0 or p_10m_ago <= 0.0) and isinstance(df_q_ref, pd.DataFrame) and not df_q_ref.empty:
            closes = df_q_ref["close"]
            if p_3m_ago <= 0.0 and len(closes) >= 4:
                p_3m_ago = float(closes.iloc[-4])
            elif p_3m_ago <= 0.0:
                p_3m_ago = float(closes.iloc[0])

            if p_10m_ago <= 0.0 and len(closes) >= 11:
                p_10m_ago = float(closes.iloc[-11])
            elif p_10m_ago <= 0.0:
                p_10m_ago = float(closes.iloc[0])

        chg_3m_pct = (
            ((qp - p_3m_ago) / p_3m_ago) * 100.0
            if (qp > 0.0 and p_3m_ago > 0.0)
            else 0.0
        )
        chg_10m_pct = (
            ((qp - p_10m_ago) / p_10m_ago) * 100.0
            if (qp > 0.0 and p_10m_ago > 0.0)
            else 0.0
        )

        # SOXS(숏) 보유 시 QQQ -0.20% 이하 급락이 유리 방향 급등락
        bypass_soxs = bool(
            chg_3m_pct <= -QQQ_VELOCITY_BYPASS_THRESHOLD
            or chg_10m_pct <= -QQQ_VELOCITY_BYPASS_THRESHOLD
        )
        # SOXL(롱) 보유 시 QQQ +0.20% 이상 급등이 유리 방향 급등락
        bypass_soxl = bool(
            chg_3m_pct >= QQQ_VELOCITY_BYPASS_THRESHOLD
            or chg_10m_pct >= QQQ_VELOCITY_BYPASS_THRESHOLD
        )

        return {
            "qqq_price": round(qp, 4),
            "qqq_3m_chg_pct": round(chg_3m_pct, 3),
            "qqq_10m_chg_pct": round(chg_10m_pct, 3),
            "bypass_soxs": bypass_soxs,
            "bypass_soxl": bypass_soxl,
        }

    async def get_macro_snapshot(
        self,
        live_qqq_price: float = 0.0,
        force_nq_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        메인 타격 루프 및 UI 관제 보드에서 호출하는 통합 매크로 스냅샷 반환기:
        - NQ=F 진폭/고저가(15초 TTL 캐시) + 토스 QQQ 0.0초 실시간 현재가 및 10m/3m 속도 통합
        """
        now_mono = time.monotonic()
        with self._lock:
            cached_nq = dict(self._nq_state)
            nq_age = now_mono - safe_float(cached_nq.get("updated_mono"), 0.0)

        if (
            force_nq_refresh
            or safe_float(cached_nq.get("nq_price"), 0.0) <= 0.0
            or nq_age >= self.nq_ttl_sec
        ):
            try:
                cached_nq = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_nq_sync),
                    timeout=10.0,
                )
            except Exception as exc:
                logger.warning("⚠️ NQ=F 스냅샷 갱신 타임아웃/실패 (캐시 폴백): %s", exc)

        vel = await self.get_qqq_velocity_metrics(live_qqq_price=live_qqq_price)
        nq_amp = safe_float(cached_nq.get("nq_amp"), 0.0)
        dyn_thresh = max(round(nq_amp * 0.5, 2), NQ_DYNAMIC_FLOOR)

        return {
            "session_id": str(cached_nq.get("session_id") or ""),
            "nq_price": safe_float(cached_nq.get("nq_price"), 0.0),
            "nq_high": safe_float(cached_nq.get("nq_high"), 0.0),
            "nq_low": safe_float(cached_nq.get("nq_low"), 0.0),
            "nq_amp": nq_amp,
            "is_overheated_200": bool(nq_amp >= NQ_OVERHEAT_THRESHOLD),
            "dynamic_threshold_pct": dyn_thresh,
            "qqq_price": safe_float(vel.get("qqq_price"), 0.0),
            "qqq_3m_chg_pct": safe_float(vel.get("qqq_3m_chg_pct"), 0.0),
            "qqq_10m_chg_pct": safe_float(vel.get("qqq_10m_chg_pct"), 0.0),
            "bypass_soxs": bool(vel.get("bypass_soxs")),
            "bypass_soxl": bool(vel.get("bypass_soxl")),
            "updated_est_str": str(cached_nq.get("updated_est_str") or ""),
            "is_self_healed": bool(cached_nq.get("is_self_healed")),
        }


# ---------------------------------------------------------------------------
# 3차 방향성 인지(Direction-Aware) QQQ 실시간 역행 판정 함수 (🎯 5 & Case 67, 69, 93, 94)
# ---------------------------------------------------------------------------
def is_qqq_scale_valid(ref_price: float, curr_qqq_price: float) -> bool:
    """
    Case 93: 장부의 nq_entry_price(ref_price)가 NQ=F 선물 스케일(예: 20,000pt)로
    오염되어 있는지 검증한다. 현재 QQQ 가격 대비 0.3배 ~ 3.0배 범위 내에 있어야 유효한 QQQ 스케일이다.
    """
    rp = safe_float(ref_price, 0.0)
    cp = safe_float(curr_qqq_price, 0.0)
    if rp <= 0.0 or cp <= 0.0:
        return False
    ratio = rp / cp
    return bool(0.30 <= ratio <= 3.00)


def calc_qqq_adverse_status(
    symbol: str,
    qqq_entry_price: float,
    qqq_curr_price: float,
    nq_entry_amp: float,
    position_avg_price: float = 0.0,
    position_live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 67, 69, 94:
    3차 방향성 인지 매크로 방어선 판정기:
    - 포지션 자체 평가손실률이 -0.80% 이상이더라도 즉시 손절 절대 금지!
    - 동적 임계치: dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%)
    - 역행률(adverse_move_pct):
      * SOXS(숏) 보유 시: QQQ 상승률 = ((qqq_curr - qqq_entry) / qqq_entry) * 100.0
      * SOXL(롱) 보유 시: QQQ 하락률 = ((qqq_entry - qqq_curr) / qqq_entry) * 100.0
    - 오직 포지션이 손실 구간(0 < position_live_price < position_avg_price)이면서
      adverse_move_pct >= dynamic_threshold 일 때만 should_stop = True 를 반환한다.
    """
    sym = str(symbol or "").upper().strip()
    q_ent = safe_float(qqq_entry_price, 0.0)
    q_cur = safe_float(qqq_curr_price, 0.0)
    e_amp = max(0.0, safe_float(nq_entry_amp, 0.0))
    avg_p = safe_float(position_avg_price, 0.0)
    live_p = safe_float(position_live_price, 0.0)

    dyn_threshold = max(round(e_amp * 0.5, 2), NQ_DYNAMIC_FLOOR)
    in_loss_zone = bool(avg_p > 0.0 and 0.0 < live_p < avg_p)

    if not is_qqq_scale_valid(q_ent, q_cur):
        return {
            "scale_valid": False,
            "dynamic_threshold_pct": dyn_threshold,
            "qqq_raw_chg_pct": 0.0,
            "adverse_move_pct": 0.0,
            "in_loss_zone": in_loss_zone,
            "should_stop": False,
        }

    raw_chg_pct = ((q_cur - q_ent) / q_ent) * 100.0
    # SOXS 는 QQQ 상승(+)이 역행, SOXL 은 QQQ 하락(-)이 역행
    adverse_move_pct = raw_chg_pct if sym == "SOXS" else (-raw_chg_pct)
    should_stop = bool(in_loss_zone and adverse_move_pct >= dyn_threshold)

    return {
        "scale_valid": True,
        "dynamic_threshold_pct": dyn_threshold,
        "qqq_raw_chg_pct": round(raw_chg_pct, 3),
        "adverse_move_pct": round(adverse_move_pct, 3),
        "in_loss_zone": in_loss_zone,
        "should_stop": should_stop,
    }


# ---------------------------------------------------------------------------
# CandleRecorder : SOXS · SOXL · QQQ 1분봉 비동기 로컬 아카이빙 & 17:00 EST GC (Case 22)
# ---------------------------------------------------------------------------
class CandleRecorder:
    """
    제1헌법 & Case 05, 06, 22:
    백그라운드 캔들 레이더 루프에서 수집된 SOXS, SOXL, QQQ 1분봉 데이터를
    asyncio.to_thread 와 GlobalThrottle.get_file_lock 으로 안전하게 로컬 JSONL 파일에 아카이빙하고,
    매일 17:00 EST 가비지 컬렉션(GC) 시 보존 기한이 지난 오래된 아카이브/임시 파일을 정리한다.
    """

    def __init__(
        self, base_dir: str = "candle_archive", retention_days: int = 14
    ) -> None:
        self.base_dir = os.path.abspath(str(base_dir or "candle_archive"))
        self.retention_days = max(3, int(retention_days))
        self._last_saved_ts: Dict[str, str] = {}

    def _append_candles_sync(
        self, symbol: str, candles: List[Dict[str, Any]], logical_date: str
    ) -> int:
        os.makedirs(self.base_dir, exist_ok=True)
        sym = str(symbol or "").upper().strip()
        filepath = os.path.join(self.base_dir, f"{sym}_{logical_date}_1m.jsonl")

        last_ts = self._last_saved_ts.get(sym, "")
        # 시간 오름차순 정렬 후 아직 기록하지 않은 최신 분봉만 필터링
        sorted_c = sorted(
            [c for c in candles if isinstance(c, dict) and c.get("timestamp")],
            key=lambda x: str(x.get("timestamp")),
        )
        new_rows = [
            c for c in sorted_c if str(c.get("timestamp")) > last_ts
        ]
        if not new_rows:
            return 0

        with GlobalThrottle.get_file_lock(filepath):
            with open(filepath, "a", encoding="utf-8") as fp:
                for row in new_rows:
                    fp.write(json.dumps(row, ensure_ascii=False) + "\n")

        self._last_saved_ts[sym] = str(new_rows[-1].get("timestamp"))
        return len(new_rows)

    async def record_candles_async(
        self,
        symbol: str,
        candles: List[Dict[str, Any]],
        now_est: Optional[datetime] = None,
    ) -> int:
        if not candles:
            return 0
        logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._append_candles_sync, symbol, candles, logical_date
                ),
                timeout=5.0,
            )
        except Exception as exc:
            logger.debug("캔들 아카이빙 스킵 (%s): %s", symbol, exc)
            return 0

    def _cleanup_old_files_sync(self) -> int:
        if not os.path.isdir(self.base_dir):
            return 0
        cutoff_ts = time.time() - (self.retention_days * 86400.0)
        removed = 0
        for fname in os.listdir(self.base_dir):
            fpath = os.path.join(self.base_dir, fname)
            if not os.path.isfile(fpath):
                continue
            try:
                if os.path.getmtime(fpath) < cutoff_ts or fname.endswith(".tmp"):
                    with GlobalThrottle.get_file_lock(fpath):
                        os.remove(fpath)
                    removed += 1
            except Exception:
                pass
        return removed

    async def cleanup_old_files_async(self) -> int:
        """
        Case 22: 17:00 EST 일일 GC 파이프라인에서 호출되어 오래된 캔들 아카이브 및 .tmp 파일을 청소한다.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._cleanup_old_files_sync), timeout=8.0
            )
        except Exception as exc:
            logger.warning("⚠️ 캔들 아카이브 GC 실패: %s", exc)
            return 0
