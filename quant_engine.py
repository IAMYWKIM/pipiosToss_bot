# =====================================================================
# FILE: quant_engine.py
# 목적: 나스닥 100 선물(NQ=F · 18:00 EST 신세션 + fast_info/1d 일봉 병합 단조 보존) &
#       토스 Open API QQQ 0.0초 실시간 변화율/3차 역행 기준가 브릿지(MacroDataCache) 및
#       SOXS/SOXL 듀얼 원자적 로컬 장부(AssassinLedger) 코어 엔진
#       (quant_engine.py 2/2 분할본 · quant_indicators_plugin 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제1헌법 및 Case 05, 06, 30 의거 모든 파일 I/O 및 외부 시세 조회를 asyncio.to_thread + GlobalThrottle로 비동기 격리
# MODIFIED: 초과 Case 62, 90, 91 — CME 18:00 EST 신세션 경계 절삭, fast_info + 1d 일봉 고/저가 강제 병합 및 17:00~17:59 EST 마감 시세 보존
# MODIFIED: 초과 Case 89, 91, 92, 94 — 토스 Open API QQQ 0.0초 실시간 10분/3분 변화율 및 3차 역행 안전망 실시간 기준가(get_stage3_reference_price) 탑재
# MODIFIED: 초과 Case 82, 95, 96 — lock_all_symbols_macro에서 MANUAL 수동 보유 및 03:58~04:06 WHIPSAW_1SH 보호, 04:07+ 미체결 buy_order_id 즉시 소각
# NEW: 초과 Case 93 & Case 101 — 데이장 개시 1분 전 NQ=F 10분봉 추세 판별(get_nq_10m_candle_trend) 및 옵션 A 10분봉 스윙·꺾임목 상수/함수 리익스포트 결속

import os
import json
import time
import math
import asyncio
from datetime import datetime, timedelta
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
    normalize_target_rate,
    calc_compound_next_budget,
    get_cme_session_window_est,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
    compute_realtime_tick_10m_state,
    _compute_rsi_and_vwap_sync,
    calculate_rsi_1m,
)


class MacroDataCache:
    """
    초과 Case 61, 62, 89, 90, 91, 92, 93, 94:
    1) 나스닥 100 선물(NQ=F) 당일 고가·저가·현재가를 18:00 EST 신세션 기준으로 산출하고 동일 세션 내 단조 보존합니다.
    2) 토스 Open API QQQ 0.0초 실시간 스냅샷을 결합하여 단기 10분/3분 변화율(nq_vel_10m, nq_vel_3m) 및
       3차 역행 안전망 실시간 기준가(get_stage3_reference_price)를 제공합니다.
    3) 데이장 개시 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물 10분봉 양/음봉 판별(get_nq_10m_candle_trend)을 수행합니다.
    """
    _lock = asyncio.Lock()
    _last_fetch_ts: float = 0.0
    _nq_session_id: str = ""
    _nq_data: tuple[float, float, float] = (0.0, 0.0, 0.0)  # (curr, high, low)
    _nq_vel: tuple[float, float] = (0.0, 0.0)               # (vel_10m, vel_3m)
    _qqq_rt_price: float = 0.0
    _qqq_rt_ts: float = 0.0
    _nq_10m_trend_cache: dict = {
        "ts": 0.0,
        "trend_sym": "",
        "open_10m": 0.0,
        "close_10m": 0.0,
        "body_pct": 0.0,
    }

    @staticmethod
    def _fetch_yfinance_nq_sync(
        nq_session_start_est: datetime,
        is_cme_open: bool,
        prev_session_id: str,
        prev_data: tuple[float, float, float],
    ) -> tuple[str, float, float, float, float, float]:
        session_id = nq_session_start_est.strftime("%Y-%m-%d_%H%M")
        target_session_date = (nq_session_start_est + timedelta(hours=6)).date()
        prev_c, prev_h, prev_l = prev_data if prev_session_id == session_id else (0.0, 0.0, 0.0)

        tkr = yf.Ticker("NQ=F")

        fast_last = 0.0
        fast_day_high = 0.0
        fast_day_low = 0.0
        try:
            fi = tkr.fast_info
            if fi is not None:
                fast_last = float(getattr(fi, "last_price", 0.0) or fi.get("lastPrice", 0.0) or 0.0)
                fast_day_high = float(getattr(fi, "day_high", 0.0) or fi.get("dayHigh", 0.0) or 0.0)
                fast_day_low = float(getattr(fi, "day_low", 0.0) or fi.get("dayLow", 0.0) or 0.0)
        except Exception:
            pass

        daily_sess_high = 0.0
        daily_sess_low = 0.0
        daily_sess_close = 0.0
        try:
            df_1d = tkr.history(period="5d", interval="1d")
            if df_1d is not None and not df_1d.empty:
                for idx_dt, row_1d in df_1d.iterrows():
                    r_date = idx_dt.date() if hasattr(idx_dt, "date") else pd.to_datetime(idx_dt).date()
                    if r_date == target_session_date:
                        daily_sess_high = float(row_1d.get("High", 0.0) or 0.0)
                        daily_sess_low = float(row_1d.get("Low", 0.0) or 0.0)
                        daily_sess_close = float(row_1d.get("Close", 0.0) or 0.0)
        except Exception:
            pass

        df_1m = pd.DataFrame()
        try:
            df_1m = tkr.history(period="5d", interval="1m")
        except Exception:
            pass

        curr_val = 0.0
        high_val = 0.0
        low_val = 0.0
        vel_10m_yf = 0.0
        vel_3m_yf = 0.0

        now_est = datetime.now(EST_TZ)
        est_hm = now_est.hour * 100 + now_est.minute
        # 신세션 개장 직후 15분간(18:00 ~ 18:14 EST) 이전 세션의 낡은 fast_info 고/저가가 섞이는 것을 방어
        is_fast_hl_stale = bool(is_cme_open and (1800 <= est_hm <= 1814))

        if df_1m is not None and not df_1m.empty:
            if df_1m.index.tz is None:
                df_1m.index = df_1m.index.tz_localize("UTC").tz_convert(EST_TZ)
            else:
                df_1m.index = df_1m.index.tz_convert(EST_TZ)

            df_1m = df_1m[~df_1m.index.duplicated(keep="last")].sort_index(ascending=True)
            sess_end_est = nq_session_start_est + timedelta(hours=23)
            df_session = df_1m[(df_1m.index >= nq_session_start_est) & (df_1m.index <= sess_end_est)].copy()

            if not df_session.empty:
                df_session = df_session[df_session["Close"] > 0.0]
                if not df_session.empty:
                    curr_val = float(df_session["Close"].iloc[-1])
                    high_val = float(df_session["High"].max())
                    low_val = float(df_session["Low"].min())

                    closes = df_session["Close"].to_numpy()
                    if len(closes) >= 11 and closes[-11] > 0.0:
                        vel_10m_yf = float((closes[-1] - closes[-11]) / closes[-11] * 100.0)
                    elif len(closes) >= 2 and closes[0] > 0.0:
                        vel_10m_yf = float((closes[-1] - closes[0]) / closes[0] * 100.0)

                    if len(closes) >= 4 and closes[-4] > 0.0:
                        vel_3m_yf = float((closes[-1] - closes[-4]) / closes[-4] * 100.0)
                    elif len(closes) >= 2 and closes[0] > 0.0:
                        vel_3m_yf = float((closes[-1] - closes[0]) / closes[0] * 100.0)

        # 초과 Case 91 [해법 1]: fast_info 및 1d 일봉 당일 실제 고/저가/마감가 강제 병합 + 동일 세션 단조 보존
        if fast_last > 0.0:
            curr_val = fast_last
        elif curr_val <= 0.0 and daily_sess_close > 0.0:
            curr_val = daily_sess_close
        elif curr_val <= 0.0 and prev_c > 0.0:
            curr_val = prev_c

        cand_highs = [v for v in (high_val, daily_sess_high, prev_h, curr_val) if v > 0.0]
        cand_lows = [v for v in (low_val, daily_sess_low, prev_l, curr_val) if v > 0.0]
        if not is_fast_hl_stale:
            if fast_day_high > 0.0:
                cand_highs.append(fast_day_high)
            if fast_day_low > 0.0:
                cand_lows.append(fast_day_low)

        high_val = max(cand_highs) if cand_highs else 0.0
        low_val = min(cand_lows) if cand_lows else 0.0

        return session_id, curr_val, high_val, low_val, round(vel_10m_yf, 4), round(vel_3m_yf, 4)

    @staticmethod
    def _compute_qqq_velocity_sync(q_price: float, q_candles: list) -> tuple[float, float]:
        if not q_candles or not isinstance(q_candles, list):
            return 0.0, 0.0
        df = pd.DataFrame(q_candles)
        if df.empty or "closePrice" not in df.columns or "timestamp" not in df.columns:
            return 0.0, 0.0
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
        df.set_index("timestamp", inplace=True)
        df = df[~df.index.duplicated(keep="last")].sort_index(ascending=True)
        df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce").fillna(0.0)
        df = df[df["closePrice"] > 0.0]
        if df.empty:
            return 0.0, 0.0

        closes = df["closePrice"].to_numpy()
        live_c = float(q_price) if q_price > 0.0 else float(closes[-1])

        if len(closes) >= 11 and closes[-11] > 0.0:
            v10 = float((live_c - closes[-11]) / closes[-11] * 100.0)
        elif len(closes) >= 2 and closes[0] > 0.0:
            v10 = float((live_c - closes[0]) / closes[0] * 100.0)
        else:
            v10 = 0.0

        if len(closes) >= 4 and closes[-4] > 0.0:
            v3 = float((live_c - closes[-4]) / closes[-4] * 100.0)
        elif len(closes) >= 2 and closes[0] > 0.0:
            v3 = float((live_c - closes[0]) / closes[0] * 100.0)
        else:
            v3 = 0.0

        return round(v10, 4), round(v3, 4)

    @classmethod
    async def get_cached_nq_data(cls, force_refresh: bool = False) -> tuple[float, float, float]:
        now_ts = time.time()
        now_est = datetime.now(EST_TZ)
        is_cme_open, nq_sess_start = get_cme_session_window_est(now_est)
        expected_sess_id = nq_sess_start.strftime("%Y-%m-%d_%H%M")

        if (
            not force_refresh
            and cls._nq_session_id == expected_sess_id
            and (now_ts - cls._last_fetch_ts) < 15.0
            and cls._nq_data[0] > 0.0
        ):
            return cls._nq_data

        async with cls._lock:
            now_ts = time.time()
            if (
                not force_refresh
                and cls._nq_session_id == expected_sess_id
                and (now_ts - cls._last_fetch_ts) < 15.0
                and cls._nq_data[0] > 0.0
            ):
                return cls._nq_data

            try:
                sess_id, c_val, h_val, l_val, v10_yf, v3_yf = await asyncio.wait_for(
                    asyncio.to_thread(
                        cls._fetch_yfinance_nq_sync,
                        nq_sess_start,
                        is_cme_open,
                        cls._nq_session_id,
                        cls._nq_data,
                    ),
                    timeout=8.0,
                )
                if c_val > 0.0 and h_val > 0.0 and l_val > 0.0:
                    cls._nq_session_id = sess_id
                    cls._nq_data = (c_val, h_val, l_val)
                    cls._last_fetch_ts = time.time()

                # 초과 Case 91 [해법 2 방법 A]: 토스 Open API QQQ 0.0초 실시간 변화율 최우선 적용
                est_hm = now_est.hour * 100 + now_est.minute
                active_cli = getattr(GlobalThrottle, "active_client", None)
                used_qqq_vel = False
                if active_cli is not None and not (1700 <= est_hm <= 1859):
                    q_price, q_candles = await active_cli.get_qqq_realtime_snapshot(count=25)
                    if q_price > 0.0:
                        cls._qqq_rt_price = q_price
                        cls._qqq_rt_ts = time.time()
                    if q_candles:
                        v10_q, v3_q = await asyncio.to_thread(
                            cls._compute_qqq_velocity_sync, q_price, q_candles
                        )
                        cls._nq_vel = (v10_q, v3_q)
                        used_qqq_vel = True

                if not used_qqq_vel:
                    cls._nq_vel = (v10_yf, v3_yf)

            except Exception as e:
                print(f"🚨 [MacroDataCache NQ/QQQ 갱신 방어] {e}", flush=True)

            return cls._nq_data

    @classmethod
    async def get_nq_short_velocity(cls, force_refresh: bool = False) -> tuple[float, float]:
        """초과 Case 89 & 91: 보유 중 🚀 NQ_VEL_BYPASS 판별용 최근 10분/3분 변화율 반환."""
        await cls.get_cached_nq_data(force_refresh=force_refresh)
        return cls._nq_vel

    @classmethod
    async def get_stage3_reference_price(
        cls, force_refresh: bool = False, realtime_probing: bool = False
    ) -> tuple[float, str]:
        """
        초과 Case 92 & Case 94:
        3차 역행 안전망 전용 0.0초 실시간 QQQ 현재가 프로빙(보유 중 1.5초 TTL, 평시 15.0초 TTL).
        QQQ 조회 성공 시 (qqq_price, "QQQ실시간")을 반환하고, 미수신 시 (nq_curr, "NQ선물폴백")으로 안전 폴백합니다.
        """
        now_ts = time.time()
        ttl = 1.5 if realtime_probing else 15.0
        active_cli = getattr(GlobalThrottle, "active_client", None)

        if not force_refresh and cls._qqq_rt_price > 0.0 and (now_ts - cls._qqq_rt_ts) < ttl:
            return cls._qqq_rt_price, "QQQ실시간"

        if active_cli is not None:
            try:
                q_p = await active_cli.get_current_price("QQQ")
                if q_p > 0.0:
                    cls._qqq_rt_price = float(q_p)
                    cls._qqq_rt_ts = time.time()
                    return cls._qqq_rt_price, "QQQ실시간"
            except Exception:
                pass

        if cls._qqq_rt_price > 0.0 and (now_ts - cls._qqq_rt_ts) < 60.0:
            return cls._qqq_rt_price, "QQQ실시간"

        nq_c, _, _ = await cls.get_cached_nq_data(force_refresh=force_refresh)
        return float(nq_c or 0.0), "NQ선물폴백"

    @staticmethod
    def _fetch_nq_10m_trend_sync() -> tuple[str, float, float, float]:
        try:
            tkr = yf.Ticker("NQ=F")
            df_1m = tkr.history(period="2d", interval="1m")
            if df_1m is None or df_1m.empty:
                return "", 0.0, 0.0, 0.0
            if df_1m.index.tz is None:
                df_1m.index = df_1m.index.tz_localize("UTC").tz_convert(EST_TZ)
            else:
                df_1m.index = df_1m.index.tz_convert(EST_TZ)
            df_1m = df_1m[~df_1m.index.duplicated(keep="last")].sort_index(ascending=True)
            df_1m = df_1m[df_1m["Close"] > 0.0]
            if df_1m.empty:
                return "", 0.0, 0.0, 0.0

            df_10m = (
                df_1m.resample("10min", label="left", closed="left")
                .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
                .dropna(subset=["Open", "Close"])
            )
            if df_10m.empty:
                return "", 0.0, 0.0, 0.0

            last_bar = df_10m.iloc[-1]
            o_10 = float(last_bar["Open"])
            c_10 = float(last_bar["Close"])
            if len(df_10m) >= 2 and abs(c_10 - o_10) < 1e-6:
                prev_bar = df_10m.iloc[-2]
                o_10 = float(prev_bar["Open"])
                c_10 = float(prev_bar["Close"])

            if o_10 <= 0.0:
                return "", 0.0, 0.0, 0.0

            body_pct = (c_10 - o_10) / o_10 * 100.0
            trend_sym = "SOXL" if c_10 >= o_10 else "SOXS"
            return trend_sym, round(o_10, 2), round(c_10, 2), round(body_pct, 4)
        except Exception:
            return "", 0.0, 0.0, 0.0

    @classmethod
    async def get_nq_10m_candle_trend(cls, force_refresh: bool = False) -> tuple[str, float, float, float]:
        """초과 Case 93: 데이장 개시 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물 10분봉 양/음봉 판별."""
        now_ts = time.time()
        if (
            not force_refresh
            and cls._nq_10m_trend_cache.get("trend_sym")
            and (now_ts - float(cls._nq_10m_trend_cache.get("ts", 0.0))) < 20.0
        ):
            c = cls._nq_10m_trend_cache
            return c["trend_sym"], c["open_10m"], c["close_10m"], c["body_pct"]

        try:
            t_sym, o_10, c_10, b_pct = await asyncio.wait_for(
                asyncio.to_thread(cls._fetch_nq_10m_trend_sync),
                timeout=6.0,
            )
            if t_sym in ("SOXL", "SOXS"):
                cls._nq_10m_trend_cache.update({
                    "ts": now_ts,
                    "trend_sym": t_sym,
                    "open_10m": o_10,
                    "close_10m": c_10,
                    "body_pct": b_pct,
                })
                return t_sym, o_10, c_10, b_pct
        except Exception as e:
            print(f"🚨 [NQ=F 10분봉 추세 판별 방어] {e}", flush=True)

        c = cls._nq_10m_trend_cache
        return (
            str(c.get("trend_sym") or ""),
            float(c.get("open_10m", 0.0)),
            float(c.get("close_10m", 0.0)),
            float(c.get("body_pct", 0.0)),
        )


class AssassinLedger:
    """
    제1헌법, Case 02, 06, 09, 30, 54, 76, 82, 95, 96:
    SOXS 및 SOXL 듀얼 스나이퍼 로컬 원장 원자적(Atomic) I/O 통제소.
    """
    ALLOWED_SYMBOLS = ("SOXS", "SOXL")

    @classmethod
    def _validate_symbol(cls, symbol: str) -> str:
        sym = str(symbol or "").strip().upper()
        if sym not in cls.ALLOWED_SYMBOLS:
            raise ValueError(f"🚨 [Case 09 위반] 허용되지 않은 종목({sym}) 원장 접근이 차단되었습니다.")
        return sym

    @classmethod
    def _get_filepath(cls, symbol: str) -> str:
        sym = cls._validate_symbol(symbol)
        return os.path.abspath(f"assassin_ledger_{sym}.json")

    @staticmethod
    def _default_dict() -> dict:
        return {
            "price": 0.0,
            "budget": 100.0,
            "target_profit_rate": SINGLE_TARGET_PROFIT_RATE,
            "last_session_id": "",
            "is_session_done": False,
            "is_active": True,
            "target_sell_price": 0.0,
            "buy_order_id": "",
            "sell_order_id": "",
            "entry_session": "",
            "entry_trend": "",
            "entry_time": 0.0,
            "nq_entry_price": 0.0,
            "nq_entry_amp": 0.0,
            "is_compound": True,
            "whipsaw_target_price": 0.0,
        }

    @classmethod
    def _read_raw_sync(cls, filepath: str) -> dict:
        base = cls._default_dict()
        if not os.path.exists(filepath):
            return base
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                base.update(loaded)
        except Exception:
            pass
        base["target_profit_rate"] = normalize_target_rate(base.get("target_profit_rate", 1.0))
        return base

    @classmethod
    def _write_raw_sync(cls, filepath: str, data: dict):
        data["target_profit_rate"] = normalize_target_rate(data.get("target_profit_rate", 1.0))
        tmp_path = f"{filepath}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, filepath)

    @classmethod
    async def _load_dict(cls, symbol: str) -> dict:
        fp = cls._get_filepath(symbol)
        async with GlobalThrottle.get_file_lock(fp):
            return await asyncio.wait_for(asyncio.to_thread(cls._read_raw_sync, fp), timeout=5.0)

    @classmethod
    async def get_state(cls, symbol: str) -> tuple:
        d = await cls._load_dict(symbol)
        return (
            float(d.get("price") or 0.0),
            float(d.get("budget") or 100.0),
            normalize_target_rate(d.get("target_profit_rate", 1.0)),
            str(d.get("last_session_id") or ""),
            bool(d.get("is_session_done", False)),
            bool(d.get("is_active", True)),
            float(d.get("target_sell_price") or 0.0),
            str(d.get("sell_order_id") or ""),
            str(d.get("entry_session") or ""),
            float(d.get("entry_time") or 0.0),
            float(d.get("nq_entry_price") or 0.0),
            float(d.get("nq_entry_amp") or 0.0),
        )

    @classmethod
    async def save_state(cls, symbol: str, **kwargs):
        fp = cls._get_filepath(symbol)
        async with GlobalThrottle.get_file_lock(fp):
            cur = await asyncio.wait_for(asyncio.to_thread(cls._read_raw_sync, fp), timeout=5.0)
            for k, v in kwargs.items():
                if v is not None:
                    cur[k] = v
            cur["target_profit_rate"] = normalize_target_rate(cur.get("target_profit_rate", 1.0))
            await asyncio.wait_for(asyncio.to_thread(cls._write_raw_sync, fp, cur), timeout=5.0)

    @classmethod
    async def get_buy_order_id(cls, symbol: str) -> str:
        d = await cls._load_dict(symbol)
        return str(d.get("buy_order_id") or "")

    @classmethod
    async def get_is_compound(cls, symbol: str = "SOXS") -> bool:
        d = await cls._load_dict(symbol)
        return bool(d.get("is_compound", True))

    @classmethod
    async def get_entry_trend(cls, symbol: str) -> str:
        d = await cls._load_dict(symbol)
        return str(d.get("entry_trend") or "")

    @classmethod
    async def get_whipsaw_target_price(cls, symbol: str) -> float:
        d = await cls._load_dict(symbol)
        return float(d.get("whipsaw_target_price") or 0.0)

    @classmethod
    async def sync_global_settings(
        cls,
        budget: float = None,
        target_profit_rate: float = None,
        is_active: bool = None,
        is_compound: bool = None,
    ):
        """초과 Case 37 & 76: 통합 시드, 마스터 전원, 복리 모드를 SOXS와 SOXL 양쪽 원장에 동시 동기화."""
        updates = {}
        if budget is not None and math.isfinite(float(budget)) and float(budget) > 0.0:
            updates["budget"] = round(float(budget), 2)
        if target_profit_rate is not None:
            updates["target_profit_rate"] = normalize_target_rate(target_profit_rate)
        if is_active is not None:
            updates["is_active"] = bool(is_active)
        if is_compound is not None:
            updates["is_compound"] = bool(is_compound)

        if not updates:
            return
        for sym in cls.ALLOWED_SYMBOLS:
            await cls.save_state(sym, **updates)

    @classmethod
    async def lock_all_symbols_macro(cls, last_session_id: str = None):
        """
        초과 Case 82, 95, 96:
        1·2차 매크로 과열 또는 08:15 EST 이후 3차 손절 시 SOXS/SOXL 양쪽 원장을 CANCELED_MACRO로 동기화 잠금하되,
        수동 보유(MANUAL) 중인 종목과 03:58~04:06 EST 진행 중인 1주 휩소 작전(WHIPSAW_1SH)은 보존하며,
        04:07 EST 이후에는 미체결 휩소 매수 주문 ID(buy_order_id)를 즉시 소각합니다.
        """
        now_est = datetime.now(EST_TZ)
        est_hm = now_est.hour * 100 + now_est.minute
        for sym in cls.ALLOWED_SYMBOLS:
            d = await cls._load_dict(sym)
            cur_es = str(d.get("entry_session") or "")
            has_pos = float(d.get("price") or 0.0) > 0.0 or float(d.get("target_sell_price") or 0.0) > 0.0 or bool(d.get("sell_order_id"))
            has_buy_id = bool(d.get("buy_order_id"))

            if cur_es == "MANUAL" and has_pos:
                upd = {"is_session_done": True}
                if last_session_id is not None:
                    upd["last_session_id"] = last_session_id
                await cls.save_state(sym, **upd)
                continue

            if cur_es == "WHIPSAW_1SH":
                if has_pos or (358 <= est_hm <= 406 and has_buy_id):
                    upd = {"is_session_done": True}
                    if last_session_id is not None:
                        upd["last_session_id"] = last_session_id
                    await cls.save_state(sym, **upd)
                    continue

            upd = {
                "is_session_done": True,
                "entry_session": "CANCELED_MACRO",
                "buy_order_id": "",
                "whipsaw_target_price": 0.0,
            }
            if last_session_id is not None:
                upd["last_session_id"] = last_session_id
            await cls.save_state(sym, **upd)

    @classmethod
    async def unlock_all_symbols_for_reentry(cls):
        """
        초과 Case 52, 86, 95:
        프리장 골든타임(04:00 ~ 08:14 EST) 내 청산 완료 후 당일 NQ=F 진폭이 2.0% 미만인 경우에 한해
        양쪽 장부의 is_session_done 잠금을 해제하여 무한 재출동을 보장합니다.
        """
        _, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
        nq_amp = (((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0)
        is_overheated = nq_amp >= 2.0

        for sym in cls.ALLOWED_SYMBOLS:
            d = await cls._load_dict(sym)
            has_pos = (
                float(d.get("price") or 0.0) > 0.0
                or float(d.get("target_sell_price") or 0.0) > 0.0
                or bool(d.get("sell_order_id"))
                or bool(d.get("buy_order_id"))
            )
            if has_pos:
                continue

            if is_overheated:
                await cls.save_state(
                    sym,
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    whipsaw_target_price=0.0,
                )
            else:
                cur_es = str(d.get("entry_session") or "")
                new_es = cur_es if cur_es == "STAGE3_STOP_TRACK" else ""
                await cls.save_state(
                    sym,
                    is_session_done=False,
                    entry_session=new_es,
                    whipsaw_target_price=0.0,
                )

    @classmethod
    async def reset_all_ledgers(cls):
        """Case 02: 통합 시드 금액(budget)과 복리/단리(is_compound), 가동 상태(is_active)만 보존하고 양쪽 원장 0점 소각."""
        d_s = await cls._load_dict("SOXS")
        preserved_budget = float(d_s.get("budget") or 100.0)
        preserved_comp = bool(d_s.get("is_compound", True))
        preserved_act = bool(d_s.get("is_active", True))

        for sym in cls.ALLOWED_SYMBOLS:
            clean = cls._default_dict()
            clean["budget"] = preserved_budget
            clean["is_compound"] = preserved_comp
            clean["is_active"] = preserved_act
            fp = cls._get_filepath(sym)
            async with GlobalThrottle.get_file_lock(fp):
                await asyncio.wait_for(asyncio.to_thread(cls._write_raw_sync, fp, clean), timeout=5.0)
