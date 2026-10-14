# =====================================================================
# FILE: quant_engine.py
# 목적: SOXS 단방향 독립 장부 및 NQ=F 실시간 진폭 조회 캐싱, RSI 레이더 엔진
# =====================================================================
# MODIFIED: 스나이핑 임계 구간을 02:58에서 03:58 (데이장 마감 2분 전)로 정상화 결속

import os
import json
import time
import asyncio
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
from toss_api import GlobalThrottle

EST_TZ = ZoneInfo("America/New_York")

def calculate_rsi_1m(candles_json: list, session_start_est: datetime, period: int = 14) -> tuple[float, float]:
    """
    Pandas 기반 백그라운드 벡터 연산.
    VWAP와 RSI를 1개의 데이터프레임에서 동시 산출하여 CPU 블로킹 및 API 중복 호출 원천 차단.
    """
    if not candles_json:
        return 0.0, 0.0
        
    df = pd.DataFrame(candles_json)
    if df.empty:
        return 0.0, 0.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df.sort_index(ascending=True, inplace=True)

    for col in ["highPrice", "lowPrice", "closePrice", "volume"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    session_df = df[df.index >= session_start_est].copy()
    if session_df.empty:
        return 0.0, 0.0

    tp = (session_df["highPrice"] + session_df["lowPrice"] + session_df["closePrice"]) / 3.0
    pv = tp * session_df["volume"]
    cum_vol = session_df["volume"].sum()
    vwap_val = float(pv.sum() / cum_vol) if cum_vol > 0.0 else 0.0

    delta = session_df["closePrice"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi_series = 100.0 - (100.0 / (1.0 + rs))
    rsi_series = rsi_series.fillna(0.0)
    
    rsi_val = float(rsi_series.iloc[-1])
    
    return rsi_val, vwap_val

class MacroDataCache:
    _nq_cache_data = (0.0, 0.0, 0.0, "WAIT", 0.0, "--:--", 0.0)
    _nq_cache_lock = asyncio.Lock()

    @classmethod
    def _is_sniper_critical_refresh_needed(cls, now_ts: float, last_ts: float) -> bool:
        if last_ts <= 0.0: return True

        now_est = datetime.fromtimestamp(now_ts, tz=EST_TZ)
        last_est = datetime.fromtimestamp(last_ts, tz=EST_TZ)

        # MODIFIED: 03:58 임계 구간으로 변경 결속
        today_0357_50 = now_est.replace(hour=3, minute=57, second=50, microsecond=0)
        today_0358_00 = now_est.replace(hour=3, minute=58, second=0, microsecond=0)
        today_0358_10 = now_est.replace(hour=3, minute=58, second=10, microsecond=0)

        if today_0357_50 <= now_est <= today_0358_10:
            if now_est >= today_0358_00 and last_est < today_0358_00:
                return True
            if now_ts - last_ts >= 2.5:
                return True

        return (now_ts - last_ts) > 60.0

    @classmethod
    async def _refresh_cache_if_needed(cls, force_refresh: bool = False):
        async with cls._nq_cache_lock:
            now = time.time()
            need_refresh = force_refresh or cls._is_sniper_critical_refresh_needed(
                now, cls._nq_cache_data[6]
            )
            if not need_refresh:
                return

            def _fetch():
                tkr = yfinance.Ticker("NQ=F") # type: ignore (handled by yfinance import) # local fix for variable name if needed, but module is yf
                df = yf.Ticker("NQ=F").history(period="5d", interval="1m")
                if df is None or df.empty: return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                df.index = pd.to_datetime(df.index, utc=True).tz_convert(EST_TZ)
                df.sort_index(ascending=True, inplace=True)

                time_diffs = df.index.to_series().diff()
                gaps = time_diffs[time_diffs >= pd.Timedelta(minutes=45)]
                if not gaps.empty:
                    last_gap_time = gaps.index[-1]
                    df_session = df[df.index >= last_gap_time].copy()
                else:
                    df_session = df.copy()

                if df_session.empty: return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                curr_val = float(df_session["Close"].iloc[-1])
                high_val = float(df_session["High"].max())
                low_val = float(df_session["Low"].min())
                
                return curr_val, high_val, low_val, "WAIT", 0.0, "--:--"

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
                        if not isinstance(data, dict): data = {}
                        raw_rate = cls.DEFAULT_TARGET_PROFIT_RATE
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
                if not os.path.exists(filepath): return ""
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        return str(data.get("buy_order_id", "")) if isinstance(data, dict) else ""
                except Exception: return ""
            return await asyncio.to_thread(_read)

    @classmethod
    async def get_entry_trend(cls, symbol: str) -> str:
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _read():
                if not os.path.exists(filepath): return ""
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        return str(data.get("entry_trend", "")) if isinstance(data, dict) else ""
                except Exception: return ""
            return await asyncio.to_thread(_read)

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
    ):
        filepath = cls._get_file_path(symbol)
        async with GlobalThrottle.get_file_lock(filepath):
            def _write():
                data = {}
                if os.path.exists(filepath):
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            loaded = json.load(f)
                            if isinstance(loaded, dict): data = loaded
                    except Exception: pass

                data["target_profit_rate"] = cls.DEFAULT_TARGET_PROFIT_RATE

                if price is not None: data["last_buy_price"] = price
                if budget is not None: data["budget"] = budget
                if last_session_id is not None: data["last_session_id"] = last_session_id
                if is_session_done is not None: data["is_session_done"] = is_session_done
                if is_active is not None: data["is_active"] = is_active
                if target_sell_price is not None: data["target_sell_price"] = target_sell_price
                if buy_order_id is not None: data["buy_order_id"] = buy_order_id
                if sell_order_id is not None: data["sell_order_id"] = sell_order_id
                if entry_session is not None: data["entry_session"] = entry_session
                if entry_time is not None: data["entry_time"] = entry_time
                if nq_entry_price is not None: data["nq_entry_price"] = nq_entry_price
                if nq_entry_amp is not None: data["nq_entry_amp"] = nq_entry_amp
                if entry_trend is not None: data["entry_trend"] = entry_trend

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
            )

class AVWAPEngine:
    @staticmethod
    def calculate_vwap(candles_json: list, session_start_est: datetime) -> float:
        if not candles_json: return 0.0
        df = pd.DataFrame(candles_json)
        if df.empty: return 0.0

        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(EST_TZ)
        df.set_index("timestamp", inplace=True)
        df.sort_index(ascending=True, inplace=True)

        for col in ["highPrice", "lowPrice", "closePrice", "volume"]:
            if col not in df.columns: df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        session_df = df[df.index >= session_start_est]
        if session_df.empty: return 0.0

        typical_price = (session_df["highPrice"] + session_df["lowPrice"] + session_df["closePrice"]) / 3.0
        pv = typical_price * session_df["volume"]

        cumulative_pv = pv.sum()
        cumulative_vol = session_df["volume"].sum()

        if cumulative_vol <= 0: return 0.0
        return float(cumulative_pv / cumulative_vol)
