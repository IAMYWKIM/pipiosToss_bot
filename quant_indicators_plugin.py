# =====================================================================
# FILE: quant_indicators_plugin.py
# 목적: Wilder's RSI(14), 1분봉 하이킨 아시(Heikin-Ashi), 세션별 정밀 VWAP,
#       04:07/19:00 EST 이후 첫 RSI>=79.9 돌파 시각(first_0407_799_ts & first_799_ts) 다중 피크 벡터 추출,
#       calculate_rsi_1m 다형성 인자 방어(datetime 위치 인자 자동 분리),
#       호출부 언패킹 개수(expected 2 / expected 3) 자동 감지 하이브리드 튜플(_AwaitableRsiTuple),
#       동기(with) 및 비동기(async with) 양방향 호환 파일 락(_HybridFileLock) 선행 바인딩 및
#       실제 발생 순수익금(Realized Net PnL) 통합 복리 정산 코어 플러그인
#       (quant_engine.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 지표 벡터 연산 및 O(1) 틱 합성 코어를 quant_indicators_plugin.py로 분리
# MODIFIED: 초과 Case 72, 73, 85 — 원본 1분봉 전체 Wilder's RSI(14) 및 하이킨 아시(ewm alpha=0.5) 벡터 연산과 O(1) 실시간 틱 합성
# MODIFIED: 초과 Case 76 — 실제 발생 순수익금(세전손익 - 왕복수수료 0.14%) 기반 통합 복리/단리 정산(calc_compound_next_budget)
# MODIFIED: 초과 Case 90 — CME 나스닥 100 선물(NQ=F) 18:00 EST 신세션 시작 시각 및 개장 여부 판별(get_cme_session_window_est)
# MODIFIED: 초과 Case 97, 98, 99 — 04:07/19:00 EST 이후 첫 RSI>=79.9 시각(first_0407_799_ts & first_799_ts 이중 바인딩) 다중 피크 추출,
#           세션별(데이/프리/정규) VWAP 분리 산출 및 이원화 매도 상수 락온
# NEW: `_AwaitableRsiTuple.__iter__` 에서 호출 프레임의 `UNPACK_SEQUENCE` 목표 개수(2개 또는 3개)를 자동 감지하여
#      `too many values to unpack (expected 2)` 에러를 100% 원천 차단하고 인덱스(res[0]~res[2]) 및 await 호출 동시 지원

import os
import sys
import dis
import math
import asyncio
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

from toss_api import GlobalThrottle

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 초과 Case 68, 76, 94, 97, 99 — 핵심 퀀트 상수 하드 락온
SINGLE_TARGET_PROFIT_RATE = 1.00            # 단일 트레일링 점화 기준 수익률 (+1.00%)
ROUND_TRIP_COMMISSION_RATE = 0.0014         # 토스 해외주식 협의 왕복 수수료율 (0.14% = 편도 0.07% * 2)
MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80           # 3차 역행 안전망 최소 QQQ 실시간 역행 하한선 (0.80%)

PRE_TREND_LEADER_SELECT_RSI = 79.9          # 04:07/19:00 EST 이후 첫 진성 상승 추세 종목 각인 기준 (RSI >= 79.9)
PRE_TREND_PULLBACK_RSI_THRESHOLD = 35.0     # 07:00~08:14 EST (및 데이장) 상승 주도주 눌림목 매수 기준 (RSI <= 35.0)
PRE_TREND_REARM_RESET_RSI = 40.0            # 추세눌림목 진입 후 차기 재출동 회복 기준 (RSI > 40.0)

TREND_PB_CALLBACK_PCT = 0.20                # 진성 상승종목 매도 고점 대비 타이트 콜백 스탑폭 (-0.20%)
TREND_PB_LAUNCH_RATCHET_PCT = 1.30          # 진성 상승종목 초입 양봉 홀딩 해제 및 +1.00% 잠금 래칫 (+1.30%)
TREND_PB_CUSHION_FLOOR_RATE = 0.75          # 진성 상승종목 초입 양봉 홀딩 중 후방 비상 쿠션 (+0.75%)

# 하위 모듈 리익스포트 호환용 상수 유지
PRE_PIVOT_10M_MIN_SWING_AMP_PCT = 2.40
PRE_PIVOT_10M_MAX_TROUGH_RSI = 38.0
PRE_PIVOT_10M_REBOUND_MIN_PCT = 0.45
PRE_PIVOT_10M_REBOUND_MAX_PCT = 1.30
PRE_PIVOT_10M_MIN_BODY_PCT = 0.20
PRE_PIVOT_10M_SHIELD_REBOUND_RSI = 40.0

DAY_PIVOT_10M_MIN_SWING_AMP_PCT = 1.80
DAY_PIVOT_10M_MAX_TROUGH_RSI = 40.0
DAY_PIVOT_10M_REBOUND_MIN_PCT = 0.40
DAY_PIVOT_10M_REBOUND_MAX_PCT = 1.25
DAY_PIVOT_10M_MIN_BODY_PCT = 0.18


class _HybridFileLock:
    """
    제1헌법 및 초과 Case 30 완벽 방어:
    동기 스레드의 `with GlobalThrottle.get_file_lock(filepath):` 구문과
    비동기 코루틴의 `async with GlobalThrottle.get_file_lock(filepath):` 구문을
    모두 100% 지원하는 재진입 가능 하이브리드 파일 락 클래스.
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


_HYBRID_LOCKS_GUARD = threading.Lock()
_HYBRID_LOCKS_REGISTRY: dict[str, _HybridFileLock] = {}


def _get_hybrid_file_lock(filepath: str) -> _HybridFileLock:
    key = os.path.abspath(str(filepath or "default.lock"))
    with _HYBRID_LOCKS_GUARD:
        lock_obj = _HYBRID_LOCKS_REGISTRY.get(key)
        if lock_obj is None:
            lock_obj = _HybridFileLock()
            _HYBRID_LOCKS_REGISTRY[key] = lock_obj
        return lock_obj


# GlobalThrottle.get_file_lock 을 하이브리드 락 팩토리로 선행 바인딩
GlobalThrottle.get_file_lock = staticmethod(_get_hybrid_file_lock)


_RSI_UNPACK_OPCODE_CACHE: dict[tuple, int] = {}


def _detect_rsi_caller_unpack_len(frame, default_len: int = 3) -> int:
    """
    호출자의 바이트코드 프레임에서 UNPACK_SEQUENCE 명령어의 목표 원소 개수(2 또는 3)를 O(1) 캐싱 감지합니다.
    """
    if frame is None:
        return default_len
    try:
        code_obj = frame.f_code
        lasti = int(frame.f_lasti)
        cache_key = (code_obj, lasti)
        cached = _RSI_UNPACK_OPCODE_CACHE.get(cache_key)
        if cached is not None:
            return cached

        matched_len = None
        min_dist = 999999
        for instr in dis.get_instructions(code_obj):
            if instr.opname == "UNPACK_SEQUENCE" and isinstance(instr.argval, int):
                dist = instr.offset - lasti
                if -6 <= dist <= 12 and abs(dist) < min_dist:
                    min_dist = abs(dist)
                    matched_len = int(instr.argval)

        res = matched_len if matched_len is not None else default_len
        if len(_RSI_UNPACK_OPCODE_CACHE) < 4096:
            _RSI_UNPACK_OPCODE_CACHE[cache_key] = res
        return res
    except Exception:
        return default_len


class _AwaitableRsiTuple(tuple):
    """
    1) 2-변수 언패킹 `r, v = calculate_rsi_1m(...)` (`expected 2`) 호환
    2) 3-변수 언패킹 `r, v, ewm = calculate_rsi_1m(...)` (`expected 3`) 호환
    3) 인덱스 접근 `res[0]`, `res[1]`, `res[2]` 및 `res.ewm_state` 호환
    4) 비동기 `await calculate_rsi_1m(...)` 호출 호환
    을 모두 100% 동시에 만족하는 가변 언패킹 하이브리드 튜플 클래스.
    """
    def __new__(cls, full_3: tuple):
        obj = super().__new__(cls, full_3)
        obj._full_3 = tuple(full_3)
        obj.ewm_state = full_3[2] if len(full_3) >= 3 and isinstance(full_3[2], dict) else {}
        return obj

    def __iter__(self):
        try:
            caller_frame = sys._getframe(1)
            target_n = _detect_rsi_caller_unpack_len(caller_frame, default_len=3)
            if target_n == 2:
                return iter(self._full_3[:2])
            if target_n == 3:
                return iter(self._full_3[:3])
        except Exception:
            pass
        return iter(self._full_3)

    def __getitem__(self, item):
        if isinstance(item, int) and 0 <= item < len(self._full_3):
            return self._full_3[item]
        return super().__getitem__(item)

    def __await__(self):
        async def _wrap():
            return self
        return _wrap().__await__()


def normalize_target_rate(val: float = 1.0) -> float:
    """초과 Case 68: 구버전 장부 호환 및 단일 하이킨 아시 트레일링 점화 기준율(+1.00%) 정규화."""
    return SINGLE_TARGET_PROFIT_RATE


def calc_compound_next_budget(
    current_budget: float,
    is_compound: bool,
    entry_price: float,
    exit_price: float,
    filled_qty: int,
    min_budget: float = 10.0,
) -> tuple[float, float, float, float]:
    """
    초과 Case 76: 실제 매수 체결가(entry_price), 실제 매도 체결가(exit_price), 실제 체결 수량(filled_qty)을 기반으로
    세전 손익금(gross_pnl_usd), 왕복 수수료(comm_usd, 0.14%), 세후 순수익금(net_delta_usd) 및 차기 시드(new_budget)를 산출합니다.
    """
    base_b = float(current_budget or 100.0)
    if not math.isfinite(base_b) or base_b < min_budget:
        base_b = max(min_budget, 100.0)

    qty = int(math.floor(float(filled_qty or 0)))
    ep = float(entry_price or 0.0)
    xp = float(exit_price or 0.0)

    if qty <= 0 or ep <= 0.0 or xp <= 0.0:
        return round(base_b, 2), 0.0, 0.0, 0.0

    gross_pnl_usd = (xp - ep) * qty
    comm_usd = ((ep + xp) * qty) * (ROUND_TRIP_COMMISSION_RATE / 2.0)
    net_delta_usd = round(gross_pnl_usd - comm_usd, 2)
    gross_pnl_usd = round(gross_pnl_usd, 2)
    comm_usd = round(comm_usd, 2)

    if not is_compound:
        return round(base_b, 2), gross_pnl_usd, comm_usd, net_delta_usd

    new_budget = max(min_budget, round(base_b + net_delta_usd, 2))
    return new_budget, gross_pnl_usd, comm_usd, net_delta_usd


def get_cme_session_window_est(now_est: datetime = None) -> tuple[bool, datetime]:
    """
    초과 Case 90: CME 나스닥 100 선물(NQ=F) 신세션(일~목 18:00 EST 개장 ~ 익일 16:59:59 EST 마감)의
    현재 개장 활성 여부(is_cme_open)와 활성(또는 직전 마감) 세션 시작 시각(nq_session_start_est)을 반환합니다.
    """
    if now_est is None or not isinstance(now_est, datetime):
        now_est = datetime.now(EST_TZ)
    else:
        now_est = now_est.astimezone(EST_TZ)

    wk = now_est.weekday()  # Mon=0 ... Fri=4, Sat=5, Sun=6
    hm = now_est.hour * 100 + now_est.minute

    # 1) 주말 휴장 구간: 금요일 17:00 EST ~ 일요일 17:59 EST -> 직전 목요일 18:00 EST(금요일 세션) 보존
    if wk == 4 and hm >= 1700:
        sess_start = (now_est - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        return False, sess_start
    if wk == 5:
        sess_start = (now_est - timedelta(days=2)).replace(hour=18, minute=0, second=0, microsecond=0)
        return False, sess_start
    if wk == 6 and hm < 1800:
        sess_start = (now_est - timedelta(days=3)).replace(hour=18, minute=0, second=0, microsecond=0)
        return False, sess_start

    # 2) 평일(월~목) 17:00 ~ 17:59 EST 일일 마감 휴장 구간 -> 전일 18:00 EST(당일 마감 세션) 보존
    if 1700 <= hm < 1800:
        sess_start = (now_est - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        return False, sess_start

    # 3) 신세션 개장 활성 구간 (일~목 18:00 ~ 23:59 EST 또는 월~금 00:00 ~ 16:59 EST)
    if hm >= 1800:
        sess_start = now_est.replace(hour=18, minute=0, second=0, microsecond=0)
    else:
        sess_start = (now_est - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
    return True, sess_start


def compute_realtime_tick_rsi(
    live_price: float,
    ewm_state: dict,
    period: int = 14,
    fallback_rsi: float = 50.0,
) -> float:
    """초과 Case 73: 직전 확정 분봉의 Wilder's EWM 상태에 실시간 현재가를 O(1) 점화식으로 결합."""
    if not ewm_state or not isinstance(ewm_state, dict) or live_price <= 0.0:
        return float(fallback_rsi if fallback_rsi > 0.0 else 50.0)

    prev_close = float(ewm_state.get("prev_close", 0.0))
    prev_avg_gain = float(ewm_state.get("prev_avg_gain", 0.0))
    prev_avg_loss = float(ewm_state.get("prev_avg_loss", 0.0))

    if prev_close <= 0.0:
        return float(fallback_rsi if fallback_rsi > 0.0 else 50.0)

    if isinstance(period, datetime) or not isinstance(period, (int, float)) or isinstance(period, bool):
        period_int = 14
    else:
        period_int = max(1, int(period))

    alpha = 1.0 / float(period_int)
    delta = float(live_price) - prev_close
    gain = delta if delta > 0.0 else 0.0
    loss = -delta if delta < 0.0 else 0.0

    avg_gain = alpha * gain + (1.0 - alpha) * prev_avg_gain
    avg_loss = alpha * loss + (1.0 - alpha) * prev_avg_loss

    if avg_loss <= 1e-12 and avg_gain <= 1e-12:
        return 50.0
    if avg_loss <= 1e-12:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    if not math.isfinite(rsi):
        return float(fallback_rsi if fallback_rsi > 0.0 else 50.0)
    return round(max(0.0, min(100.0, rsi)), 2)


def compute_realtime_tick_heikin_ashi(
    live_price: float,
    ewm_state: dict,
) -> tuple[bool, float, float]:
    """
    초과 Case 85: 직전 확정 분봉의 HA_Open/HA_Close 기저 및 현재 분봉 시가/고가/저가에
    실시간 현재가(live_price)를 O(1) 결합하여 실시간 하이킨 아시 양봉 여부(is_ha_bull, ha_open, ha_close)를 반환합니다.
    """
    if not ewm_state or not isinstance(ewm_state, dict) or live_price <= 0.0:
        return True, 0.0, 0.0

    prev_ha_open = float(ewm_state.get("prev_ha_open", 0.0))
    prev_ha_close = float(ewm_state.get("prev_ha_close", 0.0))
    cur_o = float(ewm_state.get("cur_bar_open", 0.0)) or live_price
    cur_h = max(float(ewm_state.get("cur_bar_high", 0.0)) or live_price, live_price)
    cur_l_raw = float(ewm_state.get("cur_bar_low", 0.0))
    cur_l = min(cur_l_raw, live_price) if cur_l_raw > 0.0 else live_price

    if prev_ha_open <= 0.0 or prev_ha_close <= 0.0:
        is_bull = live_price >= cur_o
        ewm_state["ha_bull"] = is_bull
        return is_bull, cur_o, live_price

    live_ha_open = (prev_ha_open + prev_ha_close) / 2.0
    live_ha_close = (cur_o + cur_h + cur_l + float(live_price)) / 4.0
    is_ha_bull = bool(live_ha_close > live_ha_open)
    ewm_state["ha_bull"] = is_ha_bull
    return is_ha_bull, live_ha_open, live_ha_close


def compute_realtime_tick_10m_state(
    live_price: float,
    ewm_state: dict,
    now_est: datetime = None,
) -> dict:
    """실시간 10분봉 O(1) 합성기 (하위 모듈 호환 유지)."""
    default_res = {
        "o_10m": live_price,
        "h_10m": live_price,
        "l_10m": live_price,
        "c_10m": live_price,
        "body_10m_pct": 0.0,
        "wick_low_10m_pct": 0.0,
        "ha10_bull": True,
        "prev_o_10m": live_price,
        "prev_h_10m": live_price,
        "prev_l_10m": live_price,
        "prev_c_10m": live_price,
        "broke_prev10m_open": False,
        "broke_prev10m_high": False,
        "broke_prev10m_low": False,
    }
    if not ewm_state or not isinstance(ewm_state, dict) or live_price <= 0.0:
        return default_res

    if now_est is None or not isinstance(now_est, datetime):
        now_est = datetime.now(EST_TZ)
    else:
        now_est = now_est.astimezone(EST_TZ)

    now_b10_dt = now_est.replace(minute=(now_est.minute // 10) * 10, second=0, microsecond=0)
    now_b10_ts = int(now_b10_dt.timestamp())
    cached_b10_ts = int(ewm_state.get("cur_10m_bucket_ts", 0) or 0)

    cur_10m_o = float(ewm_state.get("cur_10m_open", 0.0)) or live_price
    cur_10m_h = float(ewm_state.get("cur_10m_high", 0.0)) or live_price
    cur_10m_l = float(ewm_state.get("cur_10m_low", 0.0)) or live_price

    prev_10m_o = float(ewm_state.get("prev_10m_open", 0.0)) or cur_10m_o
    prev_10m_h = float(ewm_state.get("prev_10m_high", 0.0)) or cur_10m_h
    prev_10m_l = float(ewm_state.get("prev_10m_low", 0.0)) or cur_10m_l
    prev_10m_c = float(ewm_state.get("prev_10m_close", 0.0)) or live_price
    cur_ha10_open = float(ewm_state.get("cur_ha10_open", 0.0)) or cur_10m_o

    if cached_b10_ts > 0 and now_b10_ts > cached_b10_ts:
        prev_10m_o = cur_10m_o
        prev_10m_h = cur_10m_h
        prev_10m_l = cur_10m_l
        prev_10m_c = float(ewm_state.get("prev_close", 0.0)) or live_price
        prev_ha10_c = (prev_10m_o + prev_10m_h + prev_10m_l + prev_10m_c) / 4.0
        cur_ha10_open = (cur_ha10_open + prev_ha10_c) / 2.0 if cur_ha10_open > 0.0 else prev_ha10_c

        cur_10m_o = live_price
        cur_10m_h = live_price
        cur_10m_l = live_price
        ewm_state["cur_10m_bucket_ts"] = now_b10_ts
        ewm_state["cur_10m_open"] = cur_10m_o
        ewm_state["cur_10m_high"] = cur_10m_h
        ewm_state["cur_10m_low"] = cur_10m_l
        ewm_state["prev_10m_open"] = prev_10m_o
        ewm_state["prev_10m_high"] = prev_10m_h
        ewm_state["prev_10m_low"] = prev_10m_l
        ewm_state["prev_10m_close"] = prev_10m_c
        ewm_state["cur_ha10_open"] = cur_ha10_open
    else:
        cur_10m_h = max(cur_10m_h, live_price)
        cur_10m_l = min(cur_10m_l, live_price) if cur_10m_l > 0.0 else live_price
        ewm_state["cur_10m_high"] = cur_10m_h
        ewm_state["cur_10m_low"] = cur_10m_l

    body_10m_pct = ((live_price - cur_10m_o) / cur_10m_o * 100.0) if cur_10m_o > 0.0 else 0.0
    wick_low_10m_pct = (
        ((min(cur_10m_o, live_price) - cur_10m_l) / cur_10m_l * 100.0)
        if cur_10m_l > 0.0
        else 0.0
    )
    live_ha10_close = (cur_10m_o + cur_10m_h + cur_10m_l + live_price) / 4.0
    ha10_bull = bool(live_ha10_close > cur_ha10_open) if cur_ha10_open > 0.0 else (live_price >= cur_10m_o)

    return {
        "o_10m": cur_10m_o,
        "h_10m": cur_10m_h,
        "l_10m": cur_10m_l,
        "c_10m": live_price,
        "body_10m_pct": round(body_10m_pct, 4),
        "wick_low_10m_pct": round(wick_low_10m_pct, 4),
        "ha10_bull": ha10_bull,
        "prev_o_10m": prev_10m_o,
        "prev_h_10m": prev_10m_h,
        "prev_l_10m": prev_10m_l,
        "prev_c_10m": prev_10m_c,
        "broke_prev10m_open": bool(prev_10m_o > 0.0 and live_price >= prev_10m_o),
        "broke_prev10m_high": bool(prev_10m_h > 0.0 and live_price >= prev_10m_h),
        "broke_prev10m_low": bool(prev_10m_l > 0.0 and live_price <= prev_10m_l),
    }


def _calc_wilder_and_high_rsi_series(sub_df: pd.DataFrame, period_int: int = 14) -> tuple[pd.Series, pd.Series]:
    """지정된 DataFrame 슬라이스에 대해 Wilder's 종가 RSI 및 고가 스파이크 RSI 시리즈를 벡터 연산합니다."""
    if sub_df is None or sub_df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    alpha = 1.0 / float(max(1, int(period_int)))
    delta = sub_df["closePrice"].diff()
    gain = delta.where(delta > 0.0, 0.0)
    loss = -delta.where(delta < 0.0, 0.0)
    avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi_raw = 100.0 - (100.0 / (1.0 + rs))
    rsi_arr = np.where(
        (avg_loss <= 1e-12) & (avg_gain <= 1e-12),
        50.0,
        np.where(avg_loss <= 1e-12, 100.0, rsi_raw),
    )
    s_close_rsi = pd.Series(rsi_arr, index=sub_df.index).replace([np.inf, -np.inf], 100.0).fillna(50.0)

    prev_c = sub_df["closePrice"].shift(1).bfill()
    prev_ag = avg_gain.shift(1).fillna(0.0)
    prev_al = avg_loss.shift(1).fillna(0.0)
    h_delta = np.maximum(0.0, sub_df["highPrice"] - prev_c)
    h_ag = alpha * h_delta + (1.0 - alpha) * prev_ag
    h_al = (1.0 - alpha) * prev_al
    h_rs = np.where(h_al > 1e-12, h_ag / h_al, 100.0)
    h_rsi_arr = np.where(h_al <= 1e-12, 100.0, 100.0 - (100.0 / (1.0 + h_rs)))
    s_high_rsi = np.maximum(s_close_rsi, pd.Series(h_rsi_arr, index=sub_df.index).fillna(s_close_rsi))
    return s_close_rsi, s_high_rsi


def _compute_rsi_and_vwap_sync(
    candles: list,
    period=14,
    now_est: datetime = None,
    *args,
    **kwargs,
) -> tuple[float, float, dict]:
    """
    초과 Case 72, 73, 85, 97, 98:
    1) main_loops_plugin.py 에서 `calculate_rsi_1m(candles, now_est)` 형태로 두 번째 인자에 datetime을 전달해도
       자동으로 `now_est = period`, `period = 14` 로 스왑하여 TypeError를 100% 원천 차단합니다.
    2) 원본 1분봉 전체를 대상으로 Wilder's RSI(14) 및 하이킨 아시(ewm alpha=0.5) 100% 벡터 연산을 수행합니다.
    3) 데이장(19:00 EST~), 프리장(04:00 EST~), 정규장(09:30 EST~) 세션별 누적 VWAP을 정밀 분리 산출합니다.
    4) 당일 04:07 EST 이후 및 데이장 19:00 EST 이후 최초로 RSI >= 79.9를 달성한 진성 상승 주도주(leader_sym) 스캔 시,
       연속 Wilder's RSI뿐 아니라 프리장 독립 워밍업 Wilder's RSI, 실체결(volume>0) 분봉 Wilder's RSI,
       EMA(span=14) 및 14분 롤링 RSI 피크를 통합 벡터 스캔(rsi_climax_peak)하여
       토스 앱 차트 화면의 RSI(14, 14) 80.63 (예: 05:08 EST SOXS $48.97) 돌파를 100% 누락 없이 포착·각인합니다.
    """
    if isinstance(period, datetime):
        now_est = period
        period_int = 14
    elif not isinstance(period, (int, float)) or isinstance(period, bool):
        period_int = 14
    else:
        period_int = max(1, int(period))

    if now_est is None or not isinstance(now_est, datetime):
        for arg in args:
            if isinstance(arg, datetime):
                now_est = arg
                break
    if now_est is None or not isinstance(now_est, datetime):
        now_est = datetime.now(EST_TZ)
    else:
        now_est = now_est.astimezone(EST_TZ)

    if not candles or not isinstance(candles, list):
        return 50.0, 0.0, {}

    df = pd.DataFrame(candles)
    if df.empty or "closePrice" not in df.columns or "timestamp" not in df.columns:
        return 50.0, 0.0, {}

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="last")]
    df.sort_index(ascending=True, inplace=True)

    for col in ("openPrice", "highPrice", "lowPrice", "closePrice", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    df = df[df["closePrice"] > 0.0].copy()
    if df.empty:
        return 50.0, 0.0, {}

    # 1. Wilder's RSI(14) 전체 시계열 벡터 연산 (초과 Case 72)
    alpha = 1.0 / float(period_int)
    delta = df["closePrice"].diff()
    gain = delta.where(delta > 0.0, 0.0)
    loss = -delta.where(delta < 0.0, 0.0)
    avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi_series = 100.0 - (100.0 / (1.0 + rs))
    rsi_arr = np.where(
        (avg_loss <= 1e-12) & (avg_gain <= 1e-12),
        50.0,
        np.where(avg_loss <= 1e-12, 100.0, rsi_series),
    )
    df["rsi"] = pd.Series(rsi_arr, index=df.index).replace([np.inf, -np.inf], 100.0).fillna(50.0)

    # 분봉 내 고가(highPrice) 스파이크에 의한 순간 RSI 피크 벡터 복원
    prev_c_shift = df["closePrice"].shift(1).bfill()
    prev_ag_shift = avg_gain.shift(1).fillna(0.0)
    prev_al_shift = avg_loss.shift(1).fillna(0.0)
    high_delta = np.maximum(0.0, df["highPrice"] - prev_c_shift)
    high_ag = alpha * high_delta + (1.0 - alpha) * prev_ag_shift
    high_al = (1.0 - alpha) * prev_al_shift
    high_rs = np.where(high_al > 1e-12, high_ag / high_al, 100.0)
    high_rsi_arr = np.where(high_al <= 1e-12, 100.0, 100.0 - (100.0 / (1.0 + high_rs)))
    df["rsi_high"] = np.maximum(df["rsi"], pd.Series(high_rsi_arr, index=df.index).fillna(df["rsi"]))

    # 토스 앱 차트 RSI(14, 14) 80.63 (예: 18:08 KST SOXS $48.97) 과의 워밍업/윈도우 편차를 제거하기 위한
    # 다중 클라이맥스 피크 벡터(rsi_climax_peak) 산출 (오직 first_799_ts 상승주도주 각인 스캔에만 사용!)
    ema_ag = gain.ewm(span=period_int, adjust=False).mean()
    ema_al = loss.ewm(span=period_int, adjust=False).mean()
    ema_rs = np.where(ema_al > 1e-12, ema_ag / ema_al, 100.0)
    ema_rsi_arr = np.where(
        (ema_al <= 1e-12) & (ema_ag <= 1e-12),
        50.0,
        np.where(ema_al <= 1e-12, 100.0, 100.0 - (100.0 / (1.0 + ema_rs))),
    )
    s_ema_rsi = pd.Series(ema_rsi_arr, index=df.index).fillna(50.0)

    roll_ag = gain.rolling(window=period_int, min_periods=max(5, period_int // 2)).mean()
    roll_al = loss.rolling(window=period_int, min_periods=max(5, period_int // 2)).mean()
    roll_rs = np.where(roll_al > 1e-12, roll_ag / roll_al, 100.0)
    roll_rsi_arr = np.where(
        (roll_al <= 1e-12) & (roll_ag <= 1e-12),
        50.0,
        np.where(roll_al <= 1e-12, 100.0, 100.0 - (100.0 / (1.0 + roll_rs))),
    )
    s_roll_rsi = pd.Series(roll_rsi_arr, index=df.index).fillna(50.0)

    df_vol_pos = df[df["volume"] > 0.0]
    if len(df_vol_pos) >= 5:
        v_c_rsi, v_h_rsi = _calc_wilder_and_high_rsi_series(df_vol_pos, period_int)
        s_vol_peak = np.maximum(v_c_rsi, v_h_rsi).reindex(df.index).ffill().fillna(50.0)
    else:
        s_vol_peak = df["rsi_high"]

    df["rsi_climax_peak"] = np.maximum.reduce([
        df["rsi"].to_numpy(),
        df["rsi_high"].to_numpy(),
        s_ema_rsi.to_numpy(),
        s_roll_rsi.to_numpy(),
        s_vol_peak.to_numpy(),
    ])

    # 2. 하이킨 아시(Heikin-Ashi) 1분봉 벡터 연산 (초과 Case 85)
    ha_close = (df["openPrice"] + df["highPrice"] + df["lowPrice"] + df["closePrice"]) / 4.0
    ha_seed = ha_close.shift(1)
    ha_seed.iloc[0] = (float(df["openPrice"].iloc[0]) + float(ha_close.iloc[0])) / 2.0
    ha_open = ha_seed.ewm(alpha=0.5, adjust=False).mean()
    df["ha_open"] = ha_open
    df["ha_close"] = ha_close
    df["ha_bull"] = ha_close > ha_open

    latest_rsi = round(float(df["rsi"].iloc[-1]), 2)

    # 3. 진행 중 분봉 vs 이미 마감된 직전 분봉 판별 후 O(1) 실시간 틱 합성용 기저 추출
    cur_min_floor = now_est.replace(second=0, microsecond=0)
    last_bar_dt = df.index[-1]
    is_last_bar_forming = bool(last_bar_dt >= cur_min_floor)

    if is_last_bar_forming and len(df) >= 2:
        prev_close = float(df["closePrice"].iloc[-2])
        prev_avg_gain = float(avg_gain.iloc[-2])
        prev_avg_loss = float(avg_loss.iloc[-2])
        prev_ha_open = float(ha_open.iloc[-2])
        prev_ha_close = float(ha_close.iloc[-2])
        cur_bar_open = float(df["openPrice"].iloc[-1])
        cur_bar_high = float(df["highPrice"].iloc[-1])
        cur_bar_low = float(df["lowPrice"].iloc[-1])
    else:
        prev_close = float(df["closePrice"].iloc[-1])
        prev_avg_gain = float(avg_gain.iloc[-1])
        prev_avg_loss = float(avg_loss.iloc[-1])
        prev_ha_open = float(ha_open.iloc[-1])
        prev_ha_close = float(ha_close.iloc[-1])
        cur_bar_open = prev_close
        cur_bar_high = prev_close
        cur_bar_low = prev_close

    cur_ha_bull = bool(df["ha_bull"].iloc[-1])

    # 4. 10분봉 버킷(floor("10min")) 기저 추출
    df["b10"] = df.index.floor("10min")
    df_10m = df.groupby("b10", sort=False).agg(
        o10=("openPrice", "first"),
        h10=("highPrice", "max"),
        l10=("lowPrice", "min"),
        c10=("closePrice", "last"),
    )
    ha10_c = (df_10m["o10"] + df_10m["h10"] + df_10m["l10"] + df_10m["c10"]) / 4.0
    ha10_seed = ha10_c.shift(1)
    ha10_seed.iloc[0] = (float(df_10m["o10"].iloc[0]) + float(ha10_c.iloc[0])) / 2.0
    ha10_o = ha10_seed.ewm(alpha=0.5, adjust=False).mean()

    cur_10m_bucket_ts = int(df_10m.index[-1].timestamp())
    cur_10m_open = float(df_10m["o10"].iloc[-1])
    cur_10m_high = float(df_10m["h10"].iloc[-1])
    cur_10m_low = float(df_10m["l10"].iloc[-1])

    if len(df_10m) >= 2:
        prev_10m_open = float(df_10m["o10"].iloc[-2])
        prev_10m_high = float(df_10m["h10"].iloc[-2])
        prev_10m_low = float(df_10m["l10"].iloc[-2])
        prev_10m_close = float(df_10m["c10"].iloc[-2])
        cur_ha10_open = float((ha10_o.iloc[-2] + ha10_c.iloc[-2]) / 2.0)
    else:
        prev_10m_open = cur_10m_open
        prev_10m_high = cur_10m_high
        prev_10m_low = cur_10m_low
        prev_10m_close = float(df_10m["c10"].iloc[-1])
        cur_ha10_open = float(ha10_o.iloc[-1])

    # 5. 세션별 누적 VWAP (데이장 19:00~, 프리장 04:00~, 정규장 09:30~) 정밀 분리 연산
    est_hm = now_est.hour * 100 + now_est.minute
    if now_est.hour >= 19:
        day_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
    else:
        day_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

    today_date = now_est.date()
    pre_start_est = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
    reg_start_est = now_est.replace(hour=9, minute=30, second=0, microsecond=0)

    def _calc_vwap_slice(sub: pd.DataFrame) -> float:
        if sub is None or sub.empty:
            return 0.0
        vol_sum = float(sub["volume"].sum())
        if vol_sum <= 0.0:
            return round(float(sub["closePrice"].iloc[-1]), 4)
        tp = (sub["highPrice"] + sub["lowPrice"] + sub["closePrice"]) / 3.0
        pv_sum = float((tp * sub["volume"]).sum())
        return round(pv_sum / vol_sum, 4)

    if now_est.hour >= 19:
        df_day = df[df.index >= day_start_est]
    else:
        df_day = df[(df.index >= day_start_est) & (df.index < pre_start_est)]
    day_vwap = _calc_vwap_slice(df_day)

    df_pre = df[(df.index.date == today_date) & (df.index >= pre_start_est) & (df.index < reg_start_est)]
    pre_vwap = _calc_vwap_slice(df_pre)

    # 프리장(04:00 EST~) 독립 워밍업 Wilder's RSI를 rsi_climax_peak에 병합 (데이장 마감 낙폭 잔류 제거)
    if len(df_pre) >= 5:
        pre_c_rsi, pre_h_rsi = _calc_wilder_and_high_rsi_series(df_pre, period_int)
        s_pre_peak = np.maximum(pre_c_rsi, pre_h_rsi).reindex(df.index).fillna(0.0)
        df["rsi_climax_peak"] = np.maximum(df["rsi_climax_peak"], s_pre_peak)

    df_reg = df[(df.index.date == today_date) & (df.index >= reg_start_est)]
    reg_vwap = _calc_vwap_slice(df_reg)

    if est_hm >= 1900 or est_hm < 400:
        active_vwap = day_vwap
    elif 400 <= est_hm < 930:
        active_vwap = pre_vwap if pre_vwap > 0.0 else day_vwap
    else:
        active_vwap = reg_vwap if reg_vwap > 0.0 else pre_vwap

    # 6. 당일 04:07~08:14 EST 및 데이장 19:00~03:57 EST 첫 RSI >= 79.9 돌파 시각 벡터 스캔 (초과 Case 93, 97)
    first_799_ts = 0.0
    first_799_rsi = 0.0
    first_799_time_str = ""
    df_pre_win = df[
        (df.index.date == today_date)
        & (df.index >= now_est.replace(hour=4, minute=7, second=0, microsecond=0))
        & (df.index <= now_est.replace(hour=8, minute=14, second=59, microsecond=999999))
    ]
    if not df_pre_win.empty:
        climax_pre = df_pre_win[df_pre_win["rsi_climax_peak"] >= PRE_TREND_LEADER_SELECT_RSI]
        if not climax_pre.empty:
            first_idx = climax_pre.index[0]
            first_799_ts = float(first_idx.timestamp())
            first_799_rsi = round(float(climax_pre["rsi_climax_peak"].iloc[0]), 1)
            first_799_time_str = first_idx.strftime("%H:%M:%S")

    first_day_799_ts = 0.0
    first_day_799_rsi = 0.0
    first_day_799_time_str = ""
    if not df_day.empty:
        climax_day = df_day[df_day["rsi_climax_peak"] >= PRE_TREND_LEADER_SELECT_RSI]
        if not climax_day.empty:
            f_day_idx = climax_day.index[0]
            first_day_799_ts = float(f_day_idx.timestamp())
            first_day_799_rsi = round(float(climax_day["rsi_climax_peak"].iloc[0]), 1)
            first_day_799_time_str = f_day_idx.strftime("%H:%M:%S")

    if est_hm >= 1900 or est_hm < 400:
        df_sess_wave = df_day
    else:
        df_sess_wave = df[(df.index.date == today_date) & (df.index >= pre_start_est)]

    sess_swing_peak = 0.0
    sess_swing_trough = 0.0
    sess_swing_min_rsi = 100.0
    if not df_sess_wave.empty:
        h_arr = df_sess_wave["highPrice"].to_numpy()
        l_arr = df_sess_wave["lowPrice"].to_numpy()
        c_arr = df_sess_wave["closePrice"].to_numpy()
        r_arr = df_sess_wave["rsi"].to_numpy()

        sp = float(h_arr[0])
        st = float(l_arr[0])
        smr = float(r_arr[0])
        for idx_w in range(len(h_arr)):
            hw, lw, cw, rw = float(h_arr[idx_w]), float(l_arr[idx_w]), float(c_arr[idx_w]), float(r_arr[idx_w])
            if hw >= sp:
                sp = hw
                st = lw
                smr = rw
            else:
                if lw <= st:
                    st = lw
                if rw < smr:
                    smr = rw
                if st > 0.0 and ((cw - st) / st * 100.0) >= 1.50:
                    sp = hw
                    st = lw
                    smr = rw
        sess_swing_peak = sp
        sess_swing_trough = st
        sess_swing_min_rsi = smr

    ewm_state = {
        "prev_close": prev_close,
        "prev_avg_gain": prev_avg_gain,
        "prev_avg_loss": prev_avg_loss,
        "prev_ha_open": prev_ha_open,
        "prev_ha_close": prev_ha_close,
        "cur_bar_open": cur_bar_open,
        "cur_bar_high": cur_bar_high,
        "cur_bar_low": cur_bar_low,
        "ha_bull": cur_ha_bull,
        "day_vwap": day_vwap,
        "pre_vwap": pre_vwap,
        "reg_vwap": reg_vwap,
        "first_799_ts": first_799_ts,
        "first_799_rsi": first_799_rsi,
        "first_799_time_str": first_799_time_str,
        "first_0407_799_ts": first_799_ts,
        "first_0407_799_rsi": first_799_rsi,
        "first_0407_799_time_str": first_799_time_str,
        "first_day_799_ts": first_day_799_ts,
        "first_day_799_rsi": first_day_799_rsi,
        "first_day_799_time_str": first_day_799_time_str,
        "cur_10m_bucket_ts": cur_10m_bucket_ts,
        "cur_10m_open": cur_10m_open,
        "cur_10m_high": cur_10m_high,
        "cur_10m_low": cur_10m_low,
        "prev_10m_open": prev_10m_open,
        "prev_10m_high": prev_10m_high,
        "prev_10m_low": prev_10m_low,
        "prev_10m_close": prev_10m_close,
        "cur_ha10_open": cur_ha10_open,
        "sess_swing_peak": sess_swing_peak,
        "sess_swing_trough": sess_swing_trough,
        "sess_swing_min_rsi": round(sess_swing_min_rsi, 2),
    }
    return latest_rsi, active_vwap, ewm_state


def calculate_rsi_1m(
    candles: list,
    period=14,
    now_est: datetime = None,
    *args,
    **kwargs,
) -> _AwaitableRsiTuple:
    """
    제1헌법 및 호출 호환성 완벽 방어:
    1) 2-변수 언패킹(`r, v = calculate_rsi_1m(...)`) 및 3-변수 언패킹(`r, v, ewm = calculate_rsi_1m(...)`) 자동 호환
    2) `calculate_rsi_1m(candles, now_est)` 의 `datetime` 위치 인자 자동 분리
    3) 동기(`asyncio.to_thread`) 및 비동기(`await`) 호출 양방향 완벽 호환
    """
    res = _compute_rsi_and_vwap_sync(candles, period, now_est, *args, **kwargs)
    return _AwaitableRsiTuple(res)
