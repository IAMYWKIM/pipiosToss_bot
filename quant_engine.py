# =====================================================================
# FILE: quant_engine.py
# 목적: 프리장 SOXS 단방향 실거래 스나이퍼 독립 장부, NQ=F 진폭/10분봉 추세 캐싱 및 Wilder's RSI(14) 벡터/틱 합성 엔진
# =====================================================================
# MODIFIED: RSI 연산을 세션 절단 전 전체 캔들(df)에서 선행하여 EWM 워밍업 무결성 100% 락온 (Case 72)
# NEW: 20초 폴링 사각지대(원인 D) 소각을 위한 Wilder's EWM 기저 상태(ewm_state_out) 추출 및 1.5초 O(1) 실시간 틱 RSI 점화식(compute_realtime_tick_rsi) 신설
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 03:58/04:07 임계 윈도우 데드코드 및 AVWAPEngine 영구 소각
# MODIFIED: NQ=F 45분 세션 갭(Gap) 기준 당일 세션 분리 및 60초 캐싱/타격 시 강제 갱신 파이프라인 결속 (Case 61, 62)
# NEW: 데이장 진성 타점 판별을 위한 NQ=F 10분봉(10min) 리샘플링 기반 EMA(3)/EMA(6) 실시간 추세("UP"/"DOWN") 벡터 연산 탑재
# MODIFIED: SOXS 단일 타겟 하드 락온 및 임시 파일 os.replace 원자적 저장 (Case 06, 09, 30)
# MODIFIED: 복리 모드(is_compound) 정산 시 수동 매매·1주 휩소 작전·본진 스나이퍼 모두 실제 발생 순수익금((매도가-매수가)*체결수량 - 왕복수수료 0.14%)을 계산하여 시드에 가감하도록 전면 개선 (Case 76)
# NEW: 03:58 ~ 04:07 EST 1주 휩소 노출 작전용 목표 매도가(whipsaw_target_price) 원장 보존 슬롯 탑재

import os
import json
import time
import math
import asyncio
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
from toss_api import GlobalThrottle

EST_TZ = ZoneInfo("America/New_York")


def calculate_rsi_1m(
    candles_json: list,
    session_start_est: datetime,
    period: int = 14,
    current_price: float = 0.0,
    ewm_state_out: dict = None,
) -> tuple[float, float]:
    """
    Pandas 기반 백그라운드 벡터 연산.
    VWAP와 RSI를 1개의 데이터프레임에서 동시 산출하여 CPU 블로킹 및 API 중복 호출 원천 차단.
    NEW: current_price 실시간 종가 보정 및 직전 확정 분봉(iloc[-2])의 Wilder's EWM 기저 상태를 ewm_state_out에 기록.
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

    for col in ["highPrice", "lowPrice", "closePrice", "volume"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[df["closePrice"] > 0.0].copy()
    if df.empty:
        return 0.0, 0.0

    # 실시간 현재가(current_price)가 주입된 경우 최신 진행 분봉(iloc[-1])의 종가·고가·저가 즉시 동기화
    if current_price is not None and current_price > 0.0:
        last_idx = df.index[-1]
        df.at[last_idx, "closePrice"] = float(current_price)
        if float(df.at[last_idx, "highPrice"]) < float(current_price):
            df.at[last_idx, "highPrice"] = float(current_price)
        cur_low = float(df.at[last_idx, "lowPrice"])
        if cur_low <= 0.0 or cur_low > float(current_price):
            df.at[last_idx, "lowPrice"] = float(current_price)

    # 전체 데이터(df)를 대상으로 RSI를 선행 연산하여 과거 데이터 단절 방어 및 ZeroDivision(inf) 안전 처리 (Case 23, 72)
    delta = df["closePrice"].diff()
    gain = delta.where(delta > 0.0, 0.0)
    loss = -delta.where(delta < 0.0, 0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()

    # 1.5초 메인 루프에서 O(1) 실시간 틱 RSI를 합성할 수 있도록 직전 확정 분봉(iloc[-2])의 EWM 기저 상태 추출
    if isinstance(ewm_state_out, dict):
        if len(df) >= 2:
            ewm_state_out["prev_close"] = float(df["closePrice"].iloc[-2])
            ewm_state_out["prev_avg_gain"] = float(avg_gain.iloc[-2])
            ewm_state_out["prev_avg_loss"] = float(avg_loss.iloc[-2])
            ewm_state_out["ready"] = True
        elif len(df) == 1:
            ewm_state_out["prev_close"] = float(df["closePrice"].iloc[-1])
            ewm_state_out["prev_avg_gain"] = float(avg_gain.iloc[-1])
            ewm_state_out["prev_avg_loss"] = float(avg_loss.iloc[-1])
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
    1.5초 메인 루프(assassin_loop) 전용 O(1) 무부하 실시간 틱 RSI 합성 엔진.
    직전 확정 분봉(t-1)의 Wilder's EWM 기저 상태에 실시간 현재가(live_price)를 대입하여
    추가 API 호출 0회로 순간적인 급락 타점(RSI <= 30.0)을 1.5초 이내에 즉시 포착합니다.
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


class MacroDataCache:
    _nq_cache_data = (0.0, 0.0, 0.0, "WAIT", 0.0, "--:--", 0.0)
    _nq_cache_lock = asyncio.Lock()

    @classmethod
    def _is_refresh_needed(cls, now_ts: float, last_ts: float) -> bool:
        if last_ts <= 0.0:
            return True
        return (now_ts - last_ts) > 60.0

    @classmethod
    async def _refresh_cache_if_needed(cls, force_refresh: bool = False):
        async with cls._nq_cache_lock:
            now = time.time()
            need_refresh = force_refresh or cls._is_refresh_needed(now, cls._nq_cache_data[6])
            if not need_refresh:
                return

            def _fetch():
                tkr = yf.Ticker("NQ=F")
                df = tkr.history(period="5d", interval="1m")
                if df is None or df.empty:
                    return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                df.index = pd.to_datetime(df.index, utc=True).tz_convert(EST_TZ)
                df = df[~df.index.duplicated(keep="last")]
                df.sort_index(ascending=True, inplace=True)

                for col in ["High", "Low", "Close"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

                df = df[df["Close"] > 0.0].copy()
                if df.empty:
                    return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                # Case 62: 45분 이상 휴장 갭(Gap) 기준 당일 세션 분리로 자정 롤오버 왜곡 방어
                time_diffs = df.index.to_series().diff()
                gaps = time_diffs[time_diffs >= pd.Timedelta(minutes=45)]
                if not gaps.empty:
                    last_gap_time = gaps.index[-1]
                    df_session = df[df.index >= last_gap_time].copy()
                else:
                    df_session = df.copy()

                if df_session.empty:
                    return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                curr_val = float(df_session["Close"].iloc[-1])
                high_val = float(df_session["High"].max())
                low_val = float(df_session["Low"].min())

                # NQ=F 5일 1분봉 시계열에서 10분봉(10min) 리샘플링 및 EMA(3)/EMA(6) 기반 실시간 추세("UP"/"DOWN") 벡터 연산
                df_10m_close = df["Close"].resample("10min").last().dropna()
                if len(df_10m_close) >= 6:
                    ema3_series = df_10m_close.ewm(span=3, adjust=False).mean()
                    ema6_series = df_10m_close.ewm(span=6, adjust=False).mean()
                    ema3_val = float(ema3_series.iloc[-1])
                    ema6_val = float(ema6_series.iloc[-1])
                    is_up_trend = (curr_val >= ema6_val) and (ema3_val >= ema6_val)
                    trend_str = "UP" if is_up_trend else "DOWN"
                    ema_diff_pct = ((curr_val - ema6_val) / ema6_val * 100.0) if ema6_val > 0.0 else 0.0
                    base_ema_str = f"{ema6_val:.2f}"
                elif not df_10m_close.empty:
                    ema_last = float(df_10m_close.iloc[-1])
                    trend_str = "UP" if curr_val >= ema_last else "DOWN"
                    ema_diff_pct = ((curr_val - ema_last) / ema_last * 100.0) if ema_last > 0.0 else 0.0
                    base_ema_str = f"{ema_last:.2f}"
                else:
                    trend_str, ema_diff_pct, base_ema_str = "DOWN", 0.0, f"{curr_val:.2f}"

                return curr_val, high_val, low_val, trend_str, ema_diff_pct, base_ema_str

            try:
                c, h, l, tr, chg, b_str = await asyncio.wait_for(
                    asyncio.to_thread(_fetch), timeout=5.0
                )
                if l > 0.0:
                    cls._nq_cache_data = (c, h, l, tr, chg, b_str, now)
                else:
                    prev = cls._nq_cache_data
                    cls._nq_cache_data = (prev[0], prev[1], prev[2], prev[3], prev[4], prev[5], now)
            except Exception as e:
                prev = cls._nq_cache_data
                cls._nq_cache_data = (prev[0], prev[1], prev[2], prev[3], prev[4], prev[5], now)
                print(f"🚨 [NQ=F 캐시 갱신 방어] {e}", flush=True)

    @classmethod
    async def get_cached_nq_data(cls, force_refresh: bool = False) -> tuple[float, float, float]:
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]

    @classmethod
    async def get_nq_sniper_signal(
        cls, force_refresh: bool = False
    ) -> tuple[str, float, str, float, float, float]:
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return (
            cls._nq_cache_data[3],
            cls._nq_cache_data[4],
            cls._nq_cache_data[5],
            cls._nq_cache_data[0],
            cls._nq_cache_data[1],
            cls._nq_cache_data[2],
        )


class AssassinLedger:
    ALLOWED_SYMBOLS = ("SOXS",)
    DEFAULT_TARGET_PROFIT_RATE = 0.64
    # 전술 제어반 선택 허용 목표 익절률 (+0.64% 고승률 회전형 vs +1.00% 고수익 복리형)
    ALLOWED_TARGET_RATES = (0.64, 1.00)
    ROUND_TRIP_COMM_PCT = 0.14
    ONE_WAY_COMM_RATE = 0.0007
    MIN_BUDGET_FLOOR = 10.0

    @classmethod
    def _validate_symbol(cls, symbol: str) -> str:
        sym = str(symbol).strip().upper()
        if sym not in cls.ALLOWED_SYMBOLS:
            raise ValueError(f"🚨 [Case 09 위반] 허용되지 않은 타 종목 접근 영구 차단: {sym}")
        return sym

    @classmethod
    def _get_file_path(cls, symbol: str) -> str:
        sym = cls._validate_symbol(symbol)
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"AssassinLedger_{sym}.json")

    # 목표 익절률 정규화 검증 (0.64% 또는 1.00% 외 비정상 값 유입 원천 차단)
    @classmethod
    def normalize_target_rate(cls, rate_val) -> float:
        try:
            r = float(rate_val)
            if math.isfinite(r) and abs(r - 1.00) < 1e-4:
                return 1.00
        except Exception:
            pass
        return 0.64

    # 선택된 목표 익절률에서 왕복 협의 수수료(0.14%)를 공제한 순수익 복리 증액률 반환 (0.64% -> 0.50%, 1.00% -> 0.86%)
    @classmethod
    def get_net_compound_rate(cls, target_profit_rate: float) -> float:
        norm_r = cls.normalize_target_rate(target_profit_rate)
        return round(norm_r - cls.ROUND_TRIP_COMM_PCT, 2)

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

    # NEW: 03:58 ~ 04:07 EST 1주 휩소 노출 작전 전용 목표 매도가 조회
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

    # MODIFIED: 익절/손절/수동개입/1주휩소 작전 모두 실제 발생한 수익금((매도가 - 매수가) * 실제체결수량 - 왕복수수료 0.14%)을
    #           기반으로 복리 시드(budget)를 정밀 갱신하도록 전면 개선 (Case 76)
    @classmethod
    def calc_compound_next_budget(
        cls,
        current_budget: float,
        is_take_profit: bool,
        entry_price: float = 0.0,
        exit_price: float = 0.0,
        filled_qty: float = 0.0,
        target_profit_rate: float = 0.64,
    ) -> tuple[float, float, float]:
        base_budget = float(current_budget) if (current_budget and math.isfinite(current_budget)) else 100.0
        if base_budget < cls.MIN_BUDGET_FLOOR:
            base_budget = cls.MIN_BUDGET_FLOOR

        # 1순위 (핵심): 실제 체결 단가(entry_price, exit_price)와 실제 체결 수량(filled_qty)이 확인된 경우
        # -> 시드 규모나 1주 테스트/수동 매매 여부와 무관하게 실제 계좌에서 발생한 순수익금(세전손익 - 왕복수수료)만 정확히 가감!
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

        # 3순위 폴백: API 체결 내역 인덱싱이 완전히 지연된 경우 설정 익절률/수수료 기준 이론치 정산
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

                if target_profit_rate is not None:
                    data["target_profit_rate"] = cls.normalize_target_rate(target_profit_rate)
                else:
                    data["target_profit_rate"] = cls.normalize_target_rate(
                        data.get("target_profit_rate", cls.DEFAULT_TARGET_PROFIT_RATE)
                    )

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
    async def reset_all_ledgers(cls):
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.save_state(
                sym, price=0.0, target_sell_price=0.0, buy_order_id="", sell_order_id="",
                is_session_done=False, entry_session="", entry_time=0.0,
                nq_entry_price=0.0, nq_entry_amp=0.0, entry_trend="",
                whipsaw_target_price=0.0,
            )
