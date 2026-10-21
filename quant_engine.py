# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_engine.py
나스닥 100 선물(NQ=F · 18:00 EST 신세션 하드 컷오프 시 전일 fast_info/1d 잔재 혼입 100% 차단·자가 치유
및 17:00 ~ 17:59 EST 마감 휴장 중 직전 마감 세션 고/저가·마감가 단조 보존) &
토스 Open API QQQ 0.0초 실시간 시세 기반 3차 방향성 인지 역행 방어망 &
실시간 QQQ 10m/3m ±0.20% 급등락 시 HA음봉 바이패스(🚀 NQ_VEL_BYPASS) 코어 엔진 (Step 02 / 03)
- 제1헌법: yfinance 외부 I/O의 asyncio.to_thread 비동기 격리 및 타임아웃 강제
- 제4헌법: 모든 논리 시계열 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 4 & 🎯 5 & Case 61, 62, 66, 91, 92, 93, 94 완벽 구현
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Deque, Dict, Optional, Tuple

import pandas as pd
import yfinance as yf

from quant_indicators_plugin import get_logical_trading_date_str
from toss_api import EST_TZ, safe_float

logger = logging.getLogger("toss_sniper.quant_engine")

# ---------------------------------------------------------------------------
# 🎯 5 : 매크로 3단 방어망 및 실시간 QQQ 급등락 바이패스 핵심 상수
# ---------------------------------------------------------------------------
NQ_OVERHEAT_THRESHOLD: float = 2.00          # 1·2차 매크로 과열 방어 임계치 (NQ=F 당일 진폭 2.00%)
MIN_QQQ_ADVERSE_STOP_PCT: float = 0.80       # 3차 실시간 QQQ 역행 방어 최소 하한선 (0.80%)
STAGE3_AMP_MULTIPLIER: float = 0.50          # 3차 동적 임계치 산출용 선물 진폭 가중치 (진폭 * 0.50)
NQ_VEL_BYPASS_THRESHOLD_PCT: float = 0.20    # 실시간 QQQ 10m/3m 급등락 HA음봉 바이패스 임계치 (±0.20%)

# 캐시 유효 주기 상수 (Case 61: NQ=F 15.0초 캐싱, 보유 중 QQQ 1.5초 프로빙)
NQ_CACHE_TTL_SEC: float = 15.0
QQQ_ACTIVE_PROBE_TTL_SEC: float = 1.5
QQQ_IDLE_PROBE_TTL_SEC: float = 5.0


# ---------------------------------------------------------------------------
# 순수 함수 : QQQ 스케일 검증 · 3차 동적 임계치 · 방향성 인지 3차 역행 판정기
# ---------------------------------------------------------------------------
def is_qqq_scale_valid(price: float) -> bool:
    """
    🚨 Case 93:
    3차 방어 기준가(nq_entry_price)에 NQ=F 선물 지수(약 15,000 ~ 25,000)나 0.0 이
    오염되어 들어가는 것을 차단하기 위해 QQQ 유효 가격 스케일(100.0 ~ 2,000.0 USD)을 검증한다.
    """
    p = safe_float(price, 0.0)
    return bool(100.0 <= p < 2000.0)


def calc_dynamic_stage3_threshold(nq_entry_amp: float) -> float:
    """
    🎯 5: 3차 역행 방어 동적 임계치 산출 공식
    dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%)
    """
    amp = max(0.0, safe_float(nq_entry_amp, 0.0))
    return round(max(amp * STAGE3_AMP_MULTIPLIER, MIN_QQQ_ADVERSE_STOP_PCT), 4)


def calc_qqq_adverse_status(
    symbol: str,
    qqq_entry_price: float,
    qqq_curr_price: float,
    nq_entry_amp: float,
    position_avg_price: float = 0.0,
    position_live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 66, 92, 93, 94:
    - 보유 포지션 자체의 평가손실률(-0.80% 등)로 즉시 손절하는 것을 100% 금지한다!
    - 오직 포지션이 평가손실 구간(0 < position_live_price < position_avg_price)에 있다는 전제 하에,
      진입(또는 수동 ON 락온) 시점의 실시간 QQQ 기준가(qqq_entry_price) 대비
      현재 실시간 QQQ 가격(qqq_curr_price)이 반대 방향으로 dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%)
      이상 역행했을 때만 should_stop = True 를 반환한다.
      * SOXS (숏) 보유 시: QQQ 가 기준가 대비 +dynamic_threshold 이상 상승하면 역행
      * SOXL (롱) 보유 시: QQQ 가 기준가 대비 -dynamic_threshold 이하 하락하면 역행 (역행폭 양수 환산)
    """
    sym = str(symbol or "SOXS").upper().strip()
    ref_entry = safe_float(qqq_entry_price, 0.0)
    ref_curr = safe_float(qqq_curr_price, 0.0)
    dyn_thr = calc_dynamic_stage3_threshold(nq_entry_amp)

    avg_p = safe_float(position_avg_price, 0.0)
    live_p = safe_float(position_live_price, 0.0)
    in_loss_zone = bool(0.0 < live_p < avg_p) if (avg_p > 0.0 and live_p > 0.0) else True

    scale_ok = bool(is_qqq_scale_valid(ref_entry) and is_qqq_scale_valid(ref_curr))
    if not scale_ok or ref_entry <= 0.0 or ref_curr <= 0.0:
        return {
            "symbol": sym,
            "scale_valid": False,
            "in_loss_zone": in_loss_zone,
            "qqq_entry_price": ref_entry,
            "qqq_curr_price": ref_curr,
            "raw_qqq_chg_pct": 0.0,
            "adverse_move_pct": 0.0,
            "dynamic_threshold_pct": dyn_thr,
            "should_stop": False,
        }

    raw_chg_pct = ((ref_curr - ref_entry) / ref_entry) * 100.0
    # SOXS 는 QQQ 상승(+)이 역행, SOXL 은 QQQ 하락(-)이 역행
    adverse_pct = raw_chg_pct if sym == "SOXS" else (-raw_chg_pct)
    should_stop = bool(in_loss_zone and (adverse_pct >= dyn_thr))

    return {
        "symbol": sym,
        "scale_valid": True,
        "in_loss_zone": in_loss_zone,
        "qqq_entry_price": round(ref_entry, 4),
        "qqq_curr_price": round(ref_curr, 4),
        "raw_qqq_chg_pct": round(raw_chg_pct, 4),
        "adverse_move_pct": round(adverse_pct, 4),
        "dynamic_threshold_pct": round(dyn_thr, 4),
        "should_stop": should_stop,
    }


# ---------------------------------------------------------------------------
# CME 선물 세션(18:00 EST 오픈 ~ 익일 17:00 EST 마감) 경계 시각 계산 헬퍼
# ---------------------------------------------------------------------------
def _get_cme_session_bounds_est(now_est: datetime) -> Tuple[datetime, datetime, bool]:
    """
    현재 EST 시각을 기준으로:
    - 현재 진행 중인 CME 선물 세션의 시작 시각(session_start_est, 18:00:00 EST),
    - 직전 마감된 CME 선물 세션의 시작 시각(prev_session_start_est),
    - 현재가 17:00 ~ 17:59 EST 마감 휴장 또는 주말 휴장 구간인지 여부(is_break_or_weekend)를 반환한다.
    """
    dt = now_est.astimezone(EST_TZ)
    wd = dt.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    hm = dt.hour * 100 + dt.minute

    is_weekend = bool(
        (wd == 4 and hm >= 1700) or (wd == 5) or (wd == 6 and hm < 1800)
    )
    is_daily_break = bool(1700 <= hm <= 1759)
    is_break_or_weekend = bool(is_weekend or is_daily_break)

    if dt.hour >= 18:
        sess_start_date = dt.date()
    elif dt.hour == 17 or is_weekend:
        # 17:00 ~ 17:59 마감 휴장 또는 주말에는 직전 마감 세션의 시작일(전일 18:00 EST)을 기준으로 조회
        days_back = 1
        if wd == 5:
            days_back = 2
        elif wd == 6 and dt.hour < 18:
            days_back = 3
        sess_start_date = dt.date() - timedelta(days=days_back)
    else:
        sess_start_date = dt.date() - timedelta(days=1)

    session_start_est = datetime.combine(
        sess_start_date, dt_time(18, 0, 0), tzinfo=EST_TZ
    )
    prev_session_start_est = session_start_est - timedelta(days=1)
    return session_start_est, prev_session_start_est, is_break_or_weekend


# ---------------------------------------------------------------------------
# MacroDataCache : NQ=F 18:00 신세션 자가 치유 캐시 & 토스 QQQ 0.0초 실시간 시세/속도 추적기
# ---------------------------------------------------------------------------
class MacroDataCache:
    """
    🎯 4 & 🎯 5 & Case 61, 62, 91, 93:
    1) yfinance NQ=F 15초 백그라운드 캐싱 및 18:00 EST 신세션 전일 잔재 혼입 100% 차단/자가 치유
    2) 17:00 ~ 17:59 EST 마감 휴장 및 주말 동안 직전 마감 세션 고/저가/마감가 단조 보존
    3) 토스 Open API QQQ 0.0초 실시간 시세 연동 및 10분/3분 변화율 기반 NQ_VEL_BYPASS(±0.20%) 산출
    """

    def __init__(self, toss_api: Optional[Any] = None) -> None:
        self.toss_api = toss_api
        self._lock = threading.RLock()

        now_est = datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)

        self._nq_state: Dict[str, Any] = {
            "logical_date": logical_date,
            "nq_price": 0.0,
            "nq_high": 0.0,
            "nq_low": 0.0,
            "nq_amp": 0.0,
            "prev_session_high": 0.0,
            "prev_session_low": 0.0,
            "updated_mono": 0.0,
        }
        self._qqq_price: float = 0.0
        self._qqq_updated_mono: float = 0.0
        # (epoch_sec, qqq_price) 링버퍼 (최근 15분 보관)
        self._qqq_history: Deque[Tuple[float, float]] = deque(maxlen=600)

    def reset_runtime_cache(self) -> None:
        """
        Case 02: /reset 명령 시 매크로 런타임 캐시를 안전하게 재초기화한다.
        """
        now_est = datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)
        with self._lock:
            self._nq_state["logical_date"] = logical_date
            self._nq_state["updated_mono"] = 0.0
            self._qqq_updated_mono = 0.0
            self._qqq_history.clear()

    def _record_qqq_tick(
        self, qqq_price: float, now_epoch: Optional[float] = None
    ) -> None:
        qp = safe_float(qqq_price, 0.0)
        if not is_qqq_scale_valid(qp):
            return
        ts = float(now_epoch if now_epoch is not None else time.time())
        with self._lock:
            self._qqq_price = qp
            self._qqq_updated_mono = time.monotonic()
            self._qqq_history.append((ts, qp))
            cutoff = ts - 900.0
            while self._qqq_history and self._qqq_history[0][0] < cutoff:
                self._qqq_history.popleft()

    def _compute_qqq_velocities(self) -> Tuple[float, float, bool, bool]:
        """
        🎯 4: 최근 10분(600초) 및 3분(180초) 동안의 실시간 QQQ 변화율(%)을 계산하여
        - bypass_soxl: QQQ 10m 또는 3m 변화율 >= +0.20% (SOXL 급등 중 HA음봉 바이패스)
        - bypass_soxs: QQQ 10m 또는 3m 변화율 <= -0.20% (SOXS 급등 중 HA음봉 바이패스)
        를 반환한다.
        """
        with self._lock:
            if len(self._qqq_history) < 2 or self._qqq_price <= 0.0:
                return 0.0, 0.0, False, False

            now_ts, cur_p = self._qqq_history[-1]
            p_10m = cur_p
            p_3m = cur_p

            target_10m = now_ts - 600.0
            target_3m = now_ts - 180.0

            for ts, p in self._qqq_history:
                if ts >= target_10m:
                    p_10m = p
                    break

            for ts, p in self._qqq_history:
                if ts >= target_3m:
                    p_3m = p
                    break

        vel_10m = ((cur_p - p_10m) / p_10m) * 100.0 if p_10m > 0.0 else 0.0
        vel_3m = ((cur_p - p_3m) / p_3m) * 100.0 if p_3m > 0.0 else 0.0

        bypass_soxl = bool(
            vel_10m >= NQ_VEL_BYPASS_THRESHOLD_PCT
            or vel_3m >= NQ_VEL_BYPASS_THRESHOLD_PCT
        )
        bypass_soxs = bool(
            vel_10m <= -NQ_VEL_BYPASS_THRESHOLD_PCT
            or vel_3m <= -NQ_VEL_BYPASS_THRESHOLD_PCT
        )
        return round(vel_10m, 4), round(vel_3m, 4), bypass_soxs, bypass_soxl

    async def ensure_qqq_live_price(self, fallback_qqq: float = 0.0) -> float:
        """
        🚨 Case 93:
        17:00 ~ 18:59 EST 구간을 포함하여 전 시간대에서 3차 역행 방어 기준가용
        토스 실시간 QQQ 현재가를 즉시 확보한다 (현재가 API 실패 시 QQQ 1분봉 종가 폴백).
        """
        fb = safe_float(fallback_qqq, 0.0)
        if is_qqq_scale_valid(fb):
            self._record_qqq_tick(fb)
            return fb

        with self._lock:
            cached_q = self._qqq_price
            age = time.monotonic() - self._qqq_updated_mono
        if is_qqq_scale_valid(cached_q) and age < QQQ_ACTIVE_PROBE_TTL_SEC:
            return cached_q

        if self.toss_api is not None:
            try:
                prices = await self.toss_api.get_current_prices_batch(("QQQ",))
                live_q = safe_float((prices or {}).get("QQQ"), 0.0)
                if is_qqq_scale_valid(live_q):
                    self._record_qqq_tick(live_q)
                    return live_q
            except Exception as exc:
                logger.debug("QQQ 실시간 현재가 조회 예외: %s", exc)

            # 17:00 ~ 18:59 EST 등에서 lastPrice 가 비어 있을 경우 QQQ 최신 1분봉 종가 폴백
            try:
                c_list, _ = await self.toss_api.get_candles(
                    symbol="QQQ", interval="1m", count=5, adjusted=True
                )
                if isinstance(c_list, list) and c_list:
                    for c_item in c_list:
                        if isinstance(c_item, dict):
                            cp = safe_float(c_item.get("closePrice"), 0.0)
                            if is_qqq_scale_valid(cp):
                                self._record_qqq_tick(cp)
                                return cp
            except Exception as exc:
                logger.debug("QQQ 1분봉 폴백 조회 예외: %s", exc)

        return cached_q if is_qqq_scale_valid(cached_q) else 0.0

    def _fetch_nq_sync(self, now_est: Optional[datetime] = None) -> Dict[str, Any]:
        """
        🎯 5 & Case 61, 62, 91:
        yfinance NQ=F(나스닥 100 선물) 조회 및 18:00 EST 신세션 전일 잔재 혼입 100% 차단·자가 치유 엔진.
        - 18:00:00 EST 신세션 롤오버 감지 시 이전 캐시(prev_h, prev_l)를 즉시 초기화한다.
        - 18:00 ~ 18:10 EST 구간에 yfinance 1분봉 지연으로 df_session 이 비어 있을 때는
          전일 세션 잔재가 남아 있는 fast_info.day_high / day_low 병합을 100% 금지하고
          오직 실시간 체결가 fast_last 단일 영점(high = low = close = fast_last, 진폭 0.00%)으로 출발한다!
        - 18:10 EST 이후 df_session 이 존재할 때도 fast_high / fast_low 나 기존 캐시가
          직전 마감 세션 고/저가와 일치하거나 신세션 1분봉+현재가 범위를 0.20% 이상 벗어나면
          전일 잔재 오염으로 판정하여 즉시 폐기하고 신세션 실데이터 범위로 자가 치유(Self-Healing)한다.
        - 17:00 ~ 17:59 EST 마감 휴장 및 주말에는 직전 마감 세션의 고/저가/마감가를 단조 보존한다.
        """
        dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
        sess_start_est, prev_sess_start_est, is_break_or_weekend = (
            _get_cme_session_bounds_est(dt_now)
        )

        with self._lock:
            prev_logical = str(self._nq_state.get("logical_date") or "")
            if prev_logical != logical_date and not is_break_or_weekend:
                # 18:00 EST 신세션 진입 시 이전 세션 고/저가 잔재를 prev_session 으로 이관하고 당일 캐시 영점 리셋
                self._nq_state = {
                    "logical_date": logical_date,
                    "nq_price": 0.0,
                    "nq_high": 0.0,
                    "nq_low": 0.0,
                    "nq_amp": 0.0,
                    "prev_session_high": safe_float(
                        self._nq_state.get("nq_high"), 0.0
                    ),
                    "prev_session_low": safe_float(
                        self._nq_state.get("nq_low"), 0.0
                    ),
                    "updated_mono": 0.0,
                }
            cached_snapshot = dict(self._nq_state)

        ticker = yf.Ticker("NQ=F")
        fast_last = 0.0
        fast_high = 0.0
        fast_low = 0.0
        try:
            fi = ticker.fast_info
            if fi is not None:
                fast_last = safe_float(getattr(fi, "last_price", 0.0), 0.0)
                fast_high = safe_float(getattr(fi, "day_high", 0.0), 0.0)
                fast_low = safe_float(getattr(fi, "day_low", 0.0), 0.0)
        except Exception:
            pass

        df_1m = pd.DataFrame()
        try:
            raw_df = ticker.history(period="2d", interval="1m", prepost=True)
            if isinstance(raw_df, pd.DataFrame) and not raw_df.empty:
                idx = raw_df.index
                if idx.tz is None:
                    idx = idx.tz_localize("UTC").tz_convert(EST_TZ)
                else:
                    idx = idx.tz_convert(EST_TZ)
                df_1m = raw_df.copy()
                df_1m.index = idx
        except Exception as exc:
            logger.debug("yfinance NQ=F 1m history 조회 예외: %s", exc)

        # 1) 17:00 ~ 17:59 EST 마감 휴장 또는 주말 휴장 구간: 직전 마감 세션 데이터 보존
        if is_break_or_weekend:
            if not df_1m.empty:
                df_closed = df_1m[df_1m.index >= sess_start_est]
                if not df_closed.empty:
                    c_last = safe_float(df_closed["Close"].iloc[-1], fast_last)
                    c_high = safe_float(df_closed["High"].max(), c_last)
                    c_low = safe_float(df_closed["Low"].min(), c_last)
                    if c_low > 0.0 and c_high >= c_low:
                        c_amp = ((c_high - c_low) / c_low) * 100.0
                        with self._lock:
                            self._nq_state.update(
                                {
                                    "nq_price": round(c_last, 2),
                                    "nq_high": round(c_high, 2),
                                    "nq_low": round(c_low, 2),
                                    "nq_amp": round(c_amp, 2),
                                    "updated_mono": time.monotonic(),
                                }
                            )
                            return dict(self._nq_state)
            # 이미 보존된 캐시가 있으면 그대로 유지
            if safe_float(cached_snapshot.get("nq_price"), 0.0) > 0.0:
                with self._lock:
                    self._nq_state["updated_mono"] = time.monotonic()
                    return dict(self._nq_state)

        # 2) 직전 세션(prev_sess_start_est ~ sess_start_est) 고/저가 추출 (전일 잔재 대조용)
        prev_sess_h = safe_float(cached_snapshot.get("prev_session_high"), 0.0)
        prev_sess_l = safe_float(cached_snapshot.get("prev_session_low"), 0.0)
        if not df_1m.empty:
            df_prev = df_1m[
                (df_1m.index >= prev_sess_start_est)
                & (df_1m.index < sess_start_est)
            ]
            if not df_prev.empty:
                prev_sess_h = max(
                    prev_sess_h, safe_float(df_prev["High"].max(), 0.0)
                )
                pl_cand = safe_float(df_prev["Low"].min(), 0.0)
                if pl_cand > 0.0:
                    prev_sess_l = (
                        min(prev_sess_l, pl_cand)
                        if prev_sess_l > 0.0
                        else pl_cand
                    )

        # 3) 현재 신세션(index >= 18:00:00 EST) 1분봉 슬라이싱
        df_session = (
            df_1m[df_1m.index >= sess_start_est]
            if not df_1m.empty
            else pd.DataFrame()
        )

        # 🚨 [핵심 방어 1] 18:00 ~ 18:10 EST 신세션 직후 df_session 이 비어 있을 때:
        # 전일 fast_info.day_high / day_low 병합을 100% 금지하고 fast_last 단일 영점(0.00%)으로 출발!
        if df_session.empty:
            ref_p = (
                fast_last
                if fast_last > 0.0
                else safe_float(cached_snapshot.get("nq_price"), 0.0)
            )
            if ref_p <= 0.0 and not df_1m.empty:
                ref_p = safe_float(df_1m["Close"].iloc[-1], 0.0)

            prev_h = safe_float(cached_snapshot.get("nq_high"), 0.0)
            prev_l = safe_float(cached_snapshot.get("nq_low"), 0.0)

            # 기존 캐시가 비어 있거나 직전 마감 세션 고/저가와 일치(오염)하거나 현재가 대비 0.20% 이상 괴리 시 영점 리셋
            cache_contaminated = bool(
                prev_h <= 0.0
                or prev_l <= 0.0
                or (prev_sess_h > 0.0 and abs(prev_h - prev_sess_h) < 1.0)
                or (prev_sess_l > 0.0 and abs(prev_l - prev_sess_l) < 1.0)
                or (ref_p > 0.0 and ((prev_h - prev_l) / ref_p) * 100.0 > 0.20)
            )
            if cache_contaminated or (
                dt_now.hour == 18 and dt_now.minute <= 12
            ):
                cur_h = ref_p
                cur_l = ref_p
            else:
                cur_h = max(prev_h, ref_p) if ref_p > 0.0 else prev_h
                cur_l = min(prev_l, ref_p) if ref_p > 0.0 else prev_l

            cur_amp = (
                ((cur_h - cur_l) / cur_l) * 100.0
                if (cur_l > 0.0 and cur_h >= cur_l)
                else 0.0
            )
            with self._lock:
                self._nq_state.update(
                    {
                        "logical_date": logical_date,
                        "nq_price": round(ref_p, 2),
                        "nq_high": round(cur_h, 2),
                        "nq_low": round(cur_l, 2),
                        "nq_amp": round(cur_amp, 2),
                        "prev_session_high": prev_sess_h,
                        "prev_session_low": prev_sess_l,
                        "updated_mono": time.monotonic(),
                    }
                )
                return dict(self._nq_state)

        # 🚨 [핵심 방어 2] 신세션 1분봉(df_session)이 존재할 때:
        # 신세션 1분봉 고/저가 + 실시간 fast_last 로 순수 신세션 기준 범위(pure_h, pure_l) 산출 후
        # 오염된 fast_high / fast_low 나 오염된 이전 캐시(prev_h, prev_l)를 자가 치유(Self-Healing)한다!
        bar_last = safe_float(df_session["Close"].iloc[-1], 0.0)
        cur_p = fast_last if fast_last > 0.0 else bar_last
        bar_h = safe_float(df_session["High"].max(), cur_p)
        bar_l = safe_float(df_session["Low"].min(), cur_p)

        pure_h = max(bar_h, cur_p) if cur_p > 0.0 else bar_h
        pure_l = (
            min(bar_l, cur_p) if (cur_p > 0.0 and bar_l > 0.0) else max(bar_l, cur_p)
        )

        # fast_high / fast_low 가 직전 세션 고/저가와 일치하지 않고 pure_h/pure_l 대비 0.20% 이내일 때만 병합 허용
        if (
            fast_high > pure_h
            and (prev_sess_h <= 0.0 or abs(fast_high - prev_sess_h) > 1.5)
            and ((fast_high - pure_h) / pure_h) * 100.0 <= 0.20
        ):
            pure_h = fast_high

        if (
            0.0 < fast_low < pure_l
            and (prev_sess_l <= 0.0 or abs(fast_low - prev_sess_l) > 1.5)
            and ((pure_l - fast_low) / pure_l) * 100.0 <= 0.20
        ):
            pure_l = fast_low

        # 기존 인메모리 캐시(prev_h, prev_l)가 오염되지 않았을 때만 단조 병합(Self-Healing)
        prev_h = safe_float(cached_snapshot.get("nq_high"), 0.0)
        prev_l = safe_float(cached_snapshot.get("nq_low"), 0.0)
        if (
            prev_h > pure_h
            and (prev_sess_h <= 0.0 or abs(prev_h - prev_sess_h) > 1.5)
            and ((prev_h - pure_h) / pure_h) * 100.0 <= 0.25
        ):
            pure_h = prev_h

        if (
            0.0 < prev_l < pure_l
            and (prev_sess_l <= 0.0 or abs(prev_l - prev_sess_l) > 1.5)
            and ((pure_l - prev_l) / pure_l) * 100.0 <= 0.25
        ):
            pure_l = prev_l

        cur_amp = (
            ((pure_h - pure_l) / pure_l) * 100.0
            if (pure_l > 0.0 and pure_h >= pure_l)
            else 0.0
        )

        with self._lock:
            self._nq_state.update(
                {
                    "logical_date": logical_date,
                    "nq_price": round(cur_p, 2),
                    "nq_high": round(pure_h, 2),
                    "nq_low": round(pure_l, 2),
                    "nq_amp": round(cur_amp, 2),
                    "prev_session_high": prev_sess_h,
                    "prev_session_low": prev_sess_l,
                    "updated_mono": time.monotonic(),
                }
            )
            return dict(self._nq_state)

    async def get_macro_snapshot(
        self,
        live_qqq_price: float = 0.0,
        has_active_position: bool = False,
        now_est: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        제1헌법 & Case 61:
        - NQ=F 선물 데이터는 15.0초 주기로 백그라운드 스레드(asyncio.to_thread)에서 갱신하고,
        - QQQ 실시간 시세는 외부 주입값(live_qqq_price) 또는 보유 중 1.5초 프로빙으로 즉시 갱신하여
          통합 매크로 스냅샷을 반환한다.
        """
        dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)

        # 1) 실시간 QQQ 가격 갱신
        if is_qqq_scale_valid(live_qqq_price):
            self._record_qqq_tick(live_qqq_price)
            qqq_p = safe_float(live_qqq_price, 0.0)
        else:
            with self._lock:
                qqq_age = time.monotonic() - self._qqq_updated_mono
                qqq_p = self._qqq_price
            probe_ttl = (
                QQQ_ACTIVE_PROBE_TTL_SEC
                if has_active_position
                else QQQ_IDLE_PROBE_TTL_SEC
            )
            if not is_qqq_scale_valid(qqq_p) or qqq_age >= probe_ttl:
                qqq_p = await self.ensure_qqq_live_price()

        # 2) NQ=F 선물 캐시 TTL(15.0초) 또는 18:00 EST 신세션 롤오버 확인
        with self._lock:
            nq_age = time.monotonic() - safe_float(
                self._nq_state.get("updated_mono"), 0.0
            )
            nq_logical = str(self._nq_state.get("logical_date") or "")
            nq_ready = safe_float(self._nq_state.get("nq_price"), 0.0) > 0.0

        if not nq_ready or nq_age >= NQ_CACHE_TTL_SEC or nq_logical != logical_date:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_nq_sync, dt_now),
                    timeout=10.0,
                )
            except Exception as exc:
                logger.debug("NQ=F 백그라운드 갱신 폴백(기존 캐시 유지): %s", exc)

        with self._lock:
            nq_snap = dict(self._nq_state)

        nq_amp = safe_float(nq_snap.get("nq_amp"), 0.0)
        dyn_thr = calc_dynamic_stage3_threshold(nq_amp)
        vel_10m, vel_3m, bypass_soxs, bypass_soxl = (
            self._compute_qqq_velocities()
        )

        return {
            "logical_date": logical_date,
            "nq_price": safe_float(nq_snap.get("nq_price"), 0.0),
            "nq_high": safe_float(nq_snap.get("nq_high"), 0.0),
            "nq_low": safe_float(nq_snap.get("nq_low"), 0.0),
            "nq_amp": nq_amp,
            "is_overheated": bool(nq_amp >= NQ_OVERHEAT_THRESHOLD),
            "qqq_price": round(qqq_p, 4) if is_qqq_scale_valid(qqq_p) else 0.0,
            "dynamic_threshold_pct": dyn_thr,
            "qqq_vel_10m_pct": vel_10m,
            "qqq_vel_3m_pct": vel_3m,
            "bypass_soxs": bypass_soxs,
            "bypass_soxl": bypass_soxl,
            "updated_at": dt_now.strftime("%Y-%m-%d %H:%M:%S EST"),
        }
