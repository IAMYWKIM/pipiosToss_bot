# =====================================================================
# FILE: quant_engine.py
# 목적: SOXL/SOXS 양방향 듀얼 장부 격리, NQ=F 10분봉 추세 판별 및 세션별 aVWAP 연산 엔진 (I/O 통제 결속)
# =====================================================================
# MODIFIED: Case 09 & Case 52 방어 - SOXL/SOXS 양방향 듀얼 타겟 락온 및 배타적 단일 진입 교차 통제 메서드 주입
# NEW: Case 43 방어 - 09:20~09:30 EST NQ=F 10분봉(closed='left', label='right') 벡터 추세(DOWN/UP) 판별 엔진 탑재
# MODIFIED: Case 61 방어 - 평시 60초 TTL 유지 및 09:29:50~09:30:10 EST 오프닝 스나이핑 임계 구간 즉시 갱신 파이프라인 결속
# MODIFIED: Case 62 방어 - NQ=F 5d 1m 스코프 및 45분 갭(>= 45분) 기반 논리 세션 시프트 계승
# MODIFIED: Case 41 & Case 68 방어 - profit_mode 데드코드 영구 적출, 기본 익절률 +1.0% 하드 락온 및 entry_trend 기록 결속

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


class MacroDataCache:
    """
    NQ=F 인메모리 캐싱 및 09:20~09:30 EST 10분봉 추세(DOWN/UP) 벡터 판별 중앙 파이프라인.
    평소 60초 TTL로 IP 밴 및 통신 병목을 방어하되, 09:29:50~09:30:10 EST 스나이핑 임계 구간에서는
    09:20~09:30 EST 10분봉 확정 종가를 지연 없이 즉시 조회합니다 (Case 43, Case 61, Case 62).
    """
    # (current, high, low, trend_10m, chg_10m_pct, bar_time_str, timestamp)
    _nq_cache_data = (0.0, 0.0, 0.0, "WAIT", 0.0, "--:--", 0.0)
    _nq_cache_lock = asyncio.Lock()

    @classmethod
    def _is_sniper_critical_refresh_needed(cls, now_ts: float, last_ts: float) -> bool:
        """
        NEW: Case 61 방어 - 09:29:50 ~ 09:30:10 EST 오프닝 스나이핑 임계 구간에서
        09:30:00 EST 정각 돌파 직후 캐시가 09:30 이전 데이터이거나 2초 이상 경과 시 즉시 갱신을 강제한다.
        """
        if last_ts <= 0.0:
            return True

        now_est = datetime.fromtimestamp(now_ts, tz=EST_TZ)
        last_est = datetime.fromtimestamp(last_ts, tz=EST_TZ)

        today_0929_50 = now_est.replace(hour=9, minute=29, second=50, microsecond=0)
        today_0930_00 = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        today_0930_10 = now_est.replace(hour=9, minute=30, second=10, microsecond=0)

        if today_0929_50 <= now_est <= today_0930_10:
            # 09:30:00 EST 정각을 넘겼는데 마지막 캐시가 09:30:00 이전이면 즉시 갱신
            if now_est >= today_0930_00 and last_est < today_0930_00:
                return True
            # 임계 구간 내에서는 2.5초 주기로 최신화 허용 (동시 호출은 Lock으로 병합)
            if now_ts - last_ts >= 2.5:
                return True

        return (now_ts - last_ts) > 60.0

    @staticmethod
    def _compute_10m_trend_vectorized(df_session: pd.DataFrame, now_est: datetime) -> tuple[str, float, str]:
        """
        NEW: Case 43 방어 - 당일 04:00 ~ 09:29:59 EST 구간의 NQ=F 1분봉을
        closed='left', label='right' 기준 10분봉으로 벡터 리샘플링하여
        09:30 EST 라벨(09:20~09:30 EST)의 하락(DOWN) / 상승(UP) 추세를 판정한다.
        """
        if df_session is None or df_session.empty:
            return "WAIT", 0.0, "--:--"

        today_date = now_est.date()
        df_today = df_session[df_session.index.date == today_date].copy()
        if df_today.empty:
            df_today = df_session.copy()

        # 09:30 EST 오프닝 스나이퍼를 위해 04:00 ~ 09:29:59 EST 프리장 분봉만 추출 (미래 참조 0%)
        df_pre = df_today.between_time("04:00", "09:29").copy()
        if df_pre.empty:
            return "WAIT", 0.0, "--:--"

        df_10m = (
            df_pre.resample("10min", closed="left", label="right")
            .agg(
                {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                }
            )
            .dropna(subset=["Open", "High", "Low", "Close"])
            .copy()
        )

        if df_10m.empty:
            return "WAIT", 0.0, "--:--"

        df_10m["Prev_Close"] = df_10m["Close"].shift(1)

        is_down = (df_10m["Close"] < df_10m["Open"]) | (
            (df_10m["Close"] == df_10m["Open"]) & (df_10m["Close"] < df_10m["Prev_Close"])
        )
        is_up = (df_10m["Close"] > df_10m["Open"]) | (
            (df_10m["Close"] == df_10m["Open"]) & (df_10m["Close"] > df_10m["Prev_Close"])
        )

        raw_trend = np.select([is_down, is_up], ["DOWN", "UP"], default="FLAT")
        trend_series = pd.Series(raw_trend, index=df_10m.index, dtype="object")
        df_10m["NQ_Trend"] = (
            trend_series.mask(trend_series == "FLAT").ffill().fillna("FLAT")
        )

        # 09:30 EST 정각 스나이핑 시 09:30 라벨(09:20~09:29:59) 봉을 최우선 타겟팅
        target_0930_bars = df_10m.between_time("09:30", "09:30")
        if not target_0930_bars.empty:
            last_row = target_0930_bars.iloc[-1]
            last_idx = target_0930_bars.index[-1]
        else:
            last_row = df_10m.iloc[-1]
            last_idx = df_10m.index[-1]

        bar_open = float(last_row["Open"])
        bar_close = float(last_row["Close"])
        bar_trend = str(last_row["NQ_Trend"])

        # 만약 모든 선행 봉이 완전 보합(FLAT)이었을 경우 당일 프리장 시가 대비로 폴백 판정
        if bar_trend == "FLAT":
            session_open = float(df_pre["Open"].iloc[0])
            if bar_close < session_open:
                bar_trend = "DOWN"
            elif bar_close > session_open:
                bar_trend = "UP"
            else:
                bar_trend = "WAIT"

        chg_pct = ((bar_close - bar_open) / bar_open * 100.0) if bar_open > 0.0 else 0.0
        bar_time_str = last_idx.strftime("%H:%M")
        return bar_trend, round(float(chg_pct), 4), bar_time_str

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
                tkr = yf.Ticker("NQ=F")
                df = tkr.history(period="5d", interval="1m")
                if df is None or df.empty:
                    return 0.0, 0.0, 0.0, "WAIT", 0.0, "--:--"

                df.index = pd.to_datetime(df.index, utc=True).tz_convert(EST_TZ)
                df.sort_index(ascending=True, inplace=True)

                # Case 62: 45분 이상 갭(롤오버/휴장) 기준 당일 논리 세션 필터링
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

                now_est_local = datetime.now(EST_TZ)
                trend_10m, chg_10m_pct, bar_time_str = cls._compute_10m_trend_vectorized(
                    df_session, now_est_local
                )
                return curr_val, high_val, low_val, trend_10m, chg_10m_pct, bar_time_str

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
        """기존 호출부 호환용: (현재가, 당일고가, 당일저가) 반환"""
        await cls._refresh_cache_if_needed(force_refresh=force_refresh)
        return cls._nq_cache_data[0], cls._nq_cache_data[1], cls._nq_cache_data[2]

    @classmethod
    async def get_nq_10m_sniper_signal(
        cls, force_refresh: bool = False
    ) -> tuple[str, float, str, float, float, float]:
        """
        NEW: 09:30 EST 오프닝 양방향 스나이퍼 및 관제탑 전용 시그널 반환.
        반환값: (nq_10m_trend['DOWN'|'UP'|'WAIT'], nq_10m_chg_pct, nq_10m_bar_str, nq_curr, nq_high, nq_low)
        """
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
    """
    SOXL(롱) 및 SOXS(숏) 양방향 독립 로컬 장부 통제소 (Case 02, Case 06, Case 09, Case 41, Case 52, Case 68).
    기본 목표 익절률은 +1.0%(DEFAULT_TARGET_PROFIT_RATE = 1.0)로 하드 락온됩니다.
    """
    ALLOWED_SYMBOLS = ("SOXL", "SOXS")
    DEFAULT_TARGET_PROFIT_RATE = 1.0

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
                        0.0,
                        100.0,
                        cls.DEFAULT_TARGET_PROFIT_RATE,
                        "",
                        False,
                        True,
                        0.0,
                        "",
                        "",
                        0.0,
                        0.0,
                        0.0,
                    )
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            data = {}
                        raw_rate = float(
                            data.get("target_profit_rate", cls.DEFAULT_TARGET_PROFIT_RATE)
                        )
                        if raw_rate not in (0.5, 1.0):
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
                        0.0,
                        100.0,
                        cls.DEFAULT_TARGET_PROFIT_RATE,
                        "",
                        False,
                        True,
                        0.0,
                        "",
                        "",
                        0.0,
                        0.0,
                        0.0,
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
        """NEW: Case 41 방어 - 진입 방향(entry_trend = 'UP' | 'DOWN') 팩트 조회"""
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
                            if isinstance(loaded, dict):
                                data = loaded
                    except Exception:
                        pass

                # 초기 생성 시 기본 목표 수익률 +1.0% 보장 (Case 68)
                if "target_profit_rate" not in data:
                    data["target_profit_rate"] = cls.DEFAULT_TARGET_PROFIT_RATE

                if price is not None:
                    data["last_buy_price"] = price
                if budget is not None:
                    data["budget"] = budget
                if target_profit_rate is not None:
                    data["target_profit_rate"] = target_profit_rate
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

                # 제3헌법 & Case 37, Case 68: 구버전 데드코드 키 영구 소각
                for obsolete_key in [
                    "pre_first_flag",
                    "force_downgrade",
                    "force_downgrade_0_6",
                    "is_stage_3",
                    "cond_order_id",
                    "session_mode",
                    "profit_mode",
                ]:
                    data.pop(obsolete_key, None)

                tmp_path = f"{filepath}.tmp.{os.getpid()}"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp_path, filepath)

            await asyncio.to_thread(_write)

    @classmethod
    async def lock_opposite_symbol(cls, active_symbol: str, current_session_id: str = None):
        """
        NEW: Case 09, Case 50, Case 52 방어 - SOXL 또는 SOXS 중 한 종목 진입/보유 감지 즉시
        반대 종목의 당일 매수 권한(is_session_done = True)을 원자적으로 영구 소각한다.
        """
        sym = cls._validate_symbol(active_symbol)
        opposite_sym = "SOXS" if sym == "SOXL" else "SOXL"
        kwargs = {"is_session_done": True}
        if current_session_id is not None:
            kwargs["last_session_id"] = current_session_id
        await cls.save_state(opposite_sym, **kwargs)

    @classmethod
    async def reset_all_ledgers(cls):
        """
        NEW: Case 02 방어 - /reset 명령 시 SOXL 및 SOXS 양방향 장부를 모두 원자적으로 0점 소각한다.
        """
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.save_state(
                sym,
                price=0.0,
                target_sell_price=0.0,
                buy_order_id="",
                sell_order_id="",
                is_session_done=False,
                entry_session="",
                entry_time=0.0,
                nq_entry_price=0.0,
                nq_entry_amp=0.0,
                entry_trend="",
            )


class AVWAPEngine:
    @staticmethod
    def calculate_vwap(candles_json: list, session_start_est: datetime) -> float:
        if not candles_json:
            return 0.0

        df = pd.DataFrame(candles_json)
        if df.empty:
            return 0.0

        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(EST_TZ)
        df.set_index("timestamp", inplace=True)
        df.sort_index(ascending=True, inplace=True)

        for col in ["highPrice", "lowPrice", "closePrice", "volume"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        session_df = df[df.index >= session_start_est]
        if session_df.empty:
            return 0.0

        typical_price = (
            session_df["highPrice"] + session_df["lowPrice"] + session_df["closePrice"]
        ) / 3.0
        pv = typical_price * session_df["volume"]

        cumulative_pv = pv.sum()
        cumulative_vol = session_df["volume"].sum()

        if cumulative_vol <= 0:
            return 0.0

        return float(cumulative_pv / cumulative_vol)
