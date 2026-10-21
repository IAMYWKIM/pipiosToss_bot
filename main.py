# -*- coding: utf-8 -*-
"""
# NEW: main.py
인피니트 스노우볼 듀얼 스나이퍼 메인 엔트리, AssassinLedger 원자적 로컬 장부,
6대 인메모리 캐시 컨텍스트, 0.4초/1.5초 메인 타격 루프, 5.0초 듀얼 캔들 레이더 루프,
17:00 EST 일일 GC 및 03:30 ~ 17:00 EST 장중 업데이트 락다운/컴파일 검증 코어 (Step 10 / 10)
- 제1헌법: 모든 로컬 장부 I/O의 asyncio.to_thread 격리 및 GlobalThrottle.get_file_lock 강제
- 제2헌법: 기동 시 ensure_account_seq() 1회 호출로 BROKERAGE accountSeq 메모리 락온
- 제3헌법: 10개 플러그인 모듈 단일 책임 오케스트레이션 및 데드코드 영구 소각
- 제4헌법: 모든 시계열/스케줄러 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: 고유 clientOrderId 멱등성 키 상태 보존
- 제6헌법: US 마켓 캘린더 Fail-Open 폴백 연동
- Case 01 ~ Case 102 전술 자산 100% 통합 완결판
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import json
import logging
import os
import py_compile
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from daymarket_sim_plugin import (
    create_default_day_sim_state,
    execute_daymarket_sim_plugin,
    is_daymarket_late_vwap_esc_window,
    is_daymarket_sim_active_window,
    settle_daymarket_sim_before_premarket,
    update_daymarket_trend_leader,
)
from defense_plugin import (
    enforce_0815_macro_cutoff_plugin,
    execute_1500_regular_vwap_escape,
    execute_1559_moc_force_liquidation,
    execute_stage1_2_nq_overheat_defense,
    execute_stage3_qqq_adverse_defense,
    relock_qqq_baseline_on_bot_resume,
    sync_manual_and_indexing_guard,
)
from quant_engine import (
    NQ_DYNAMIC_FLOOR,
    NQ_OVERHEAT_THRESHOLD,
    CandleRecorder,
    MacroDataCache,
    get_cme_futures_session_bounds,
)
from quant_indicators_plugin import (
    compute_session_vwap_metrics,
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    execute_premarket_late_vwap_escape,
    manage_mechanical_profit_trap_plugin,
    reconcile_zero_holdings_exit_plugin,
)
from sniper_plugin import (
    execute_post_stop_bottom_rebound_plugin,
    execute_premarket_vwap_sniper_plugin,
    sync_dynamic_vwap_buy_order_modify,
    update_premarket_trend_leader,
)
from tg_ui_plugin import TelegramBotRouter
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    GlobalThrottle,
    TossInvestAPI,
    safe_float,
    safe_int,
)
from whipsaw_plugin import (
    enforce_0407_whipsaw_cutoff_plugin,
    execute_0358_whipsaw_trap_plugin,
    manage_whipsaw_1sh_profit_trap,
)

# ---------------------------------------------------------------------------
# 로깅 포맷 설정
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("toss_sniper.main")

# ---------------------------------------------------------------------------
# 전역 상수 및 6대 인메모리 캐시 (Case 02, Case 11, Case 28, Case 69)
# ---------------------------------------------------------------------------
ALL_PLUGIN_FILES: Tuple[str, ...] = (
    "toss_api.py",
    "quant_indicators_plugin.py",
    "quant_engine.py",
    "sniper_plugin.py",
    "sniper_exit_plugin.py",
    "defense_plugin.py",
    "daymarket_sim_plugin.py",
    "whipsaw_plugin.py",
    "tg_ui_plugin.py",
    "main.py",
)

# 폴링 주기 상수 (Case 28: 평시 1.5초, 6대 조건 부스터 시 0.4초 고속 폴링)
NORMAL_LOOP_TIMEOUT_SEC: float = 1.5
FAST_SHIELD_LOOP_TIMEOUT_SEC: float = 0.4
# 캔들 레이더 주기 상수 (🎯 3: 데이장 및 03:55 ~ 08:15 EST 또는 보유/추적 중 5.0초 가속)
FAST_CANDLE_POLL_SEC: float = 5.0
IDLE_CANDLE_POLL_SEC: float = 20.0

# Case 02: 전역 6대 인메모리 런타임 캐시
_TRAIL_RUNTIME_CACHE: Dict[str, Dict[str, Any]] = {}
_EXIT_SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}
_BOTTOM_TRACK_CACHE: Dict[str, Any] = {"active": False}
_PRE_LEADER_CACHE: Dict[str, Any] = {
    "logical_date": "",
    "locked": False,
    "leader_sym": "",
    "vwap_0407": 0.0,
    "c_0407": 0.0,
    "disp_0407": 0.0,
    "notified": False,
    "hybrid_mode": False,
}
_DAY_LEADER_CACHE: Dict[str, Any] = {
    "logical_date": "",
    "locked": False,
    "leader_sym": "",
    "vwap_1907": 0.0,
    "c_1907": 0.0,
    "disp_1907": 0.0,
    "notified": False,
    "hybrid_mode": False,
}
_DAY_SIM_STATE: Dict[str, Any] = create_default_day_sim_state()


# ---------------------------------------------------------------------------
# Case 12 : 시그니처 적응형 플러그인 호출기 (_call_plugin_adaptive)
# ---------------------------------------------------------------------------
async def _call_plugin_adaptive(
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Case 12:
    플러그인 함수의 inspect.signature 를 분석하여 수용 가능한 위치/키워드 인자만
    안전하게 바인딩해 호출함으로써 모듈 수정 시 인자 불일치(TypeError)를 원천 방어한다.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    accepts_var_args = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)

    pos_params = [
        p
        for p in params
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    call_args = list(args) if accepts_var_args else list(args[: len(pos_params)])

    bound_param_names = {p.name for p in pos_params[: len(call_args)]}
    if accepts_var_kw:
        call_kwargs = {
            k: v for k, v in kwargs.items() if k not in bound_param_names
        }
    else:
        valid_kw_names = {
            p.name
            for p in params
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }
        call_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in valid_kw_names and k not in bound_param_names
        }

    res = func(*call_args, **call_kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# ---------------------------------------------------------------------------
# Case 06 & Case 09 : AssassinLedger 원자적 로컬 장부 코어
# ---------------------------------------------------------------------------
def _default_symbol_ledger_entry(
    symbol: str, logical_date: str = "", default_budget: float = 1000.0
) -> Dict[str, Any]:
    return {
        "symbol": str(symbol).upper().strip(),
        "logical_date": str(logical_date or get_logical_trading_date_str(rollover_hour=18)),
        "is_active": True,
        "is_compound": True,
        "budget": float(default_budget),
        "target_profit_rate": 1.0,
        "is_session_done": False,
        "entry_session": "",
        "entry_trend": "",
        "buy_order_id": "",
        "sell_order_id": "",
        "client_order_id": "",
        "ordered_qty": 0,
        "holdings_qty": 0,
        "buy_price": 0.0,
        "avg_price": 0.0,
        "target_sell_price": 0.0,
        "nq_entry_price": 0.0,
        "nq_entry_amp": 0.0,
        "last_modify_mono": 0.0,
        "entry_ts": 0.0,
        "realized_pnl_usd": 0.0,
        "trade_count": 0,
        "win_count": 0,
    }


class AssassinLedger:
    """
    제1헌법 & Case 02, 05, 06, 09, 36:
    - 오직 REAL_ALLOWED_SYMBOLS = ('SOXS', 'SOXL') 2종목만 격리 관리한다.
    - 모든 파일 읽기와 쓰기는 asyncio.to_thread 및 GlobalThrottle.get_file_lock 으로 보호하며,
      저장 시 임시 파일(.tmp) 작성 후 os.replace 로 원자적(Atomic) 덮어쓰기를 수행한다.
    """

    def __init__(
        self,
        filepath: str = "assassin_ledger.json",
        default_budget: float = 1000.0,
    ) -> None:
        self.filepath = os.path.abspath(str(filepath or "assassin_ledger.json"))
        self.default_budget = max(100.0, safe_float(default_budget, 1000.0))
        self._mem_lock = threading.RLock()
        logical_today = get_logical_trading_date_str(rollover_hour=18)
        self._data: Dict[str, Dict[str, Any]] = {
            "SOXS": _default_symbol_ledger_entry("SOXS", logical_today, self.default_budget),
            "SOXL": _default_symbol_ledger_entry("SOXL", logical_today, self.default_budget),
        }
        self._load_sync()

    def _validate_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        if sym not in REAL_ALLOWED_SYMBOLS:
            raise ValueError(f"AssassinLedger 허용 종목({REAL_ALLOWED_SYMBOLS}) 위반: {sym}")
        return sym

    def _load_sync(self) -> None:
        lock = GlobalThrottle.get_file_lock(self.filepath)
        with lock:
            if not os.path.isfile(self.filepath):
                self._save_sync_unlocked()
                return
            try:
                with open(self.filepath, "r", encoding="utf-8") as fp:
                    raw = json.load(fp)
                if isinstance(raw, dict):
                    logical_today = get_logical_trading_date_str(rollover_hour=18)
                    with self._mem_lock:
                        for sym in REAL_ALLOWED_SYMBOLS:
                            base = _default_symbol_ledger_entry(
                                sym, logical_today, self.default_budget
                            )
                            loaded_sym = raw.get(sym)
                            if isinstance(loaded_sym, dict):
                                for k in base.keys():
                                    if k in loaded_sym and loaded_sym[k] is not None:
                                        base[k] = loaded_sym[k]
                            self._data[sym] = base
            except Exception as exc:
                logger.warning("⚠️ 로컬 장부 로드 실패 (기본값 복원): %s", exc)

    def _save_sync_unlocked(self) -> None:
        tmp_path = f"{self.filepath}.tmp"
        parent_dir = os.path.dirname(self.filepath)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        with self._mem_lock:
            payload = json.dumps(self._data, ensure_ascii=False, indent=2)
        with open(tmp_path, "w", encoding="utf-8") as fp:
            fp.write(payload)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, self.filepath)

    def save_sync(self) -> None:
        lock = GlobalThrottle.get_file_lock(self.filepath)
        with lock:
            self._save_sync_unlocked()

    async def save_async(self) -> None:
        await asyncio.wait_for(asyncio.to_thread(self.save_sync), timeout=5.0)

    def get_state(self, symbol: str) -> Dict[str, Any]:
        sym = self._validate_symbol(symbol)
        with self._mem_lock:
            return dict(self._data[sym])

    async def update_state(self, symbol: str, **kwargs: Any) -> Dict[str, Any]:
        sym = self._validate_symbol(symbol)
        with self._mem_lock:
            cur = self._data[sym]
            for k, v in kwargs.items():
                cur[k] = v
            snapshot = dict(cur)
        await self.save_async()
        return snapshot

    async def check_and_rollover_logical_date(
        self, now_est: Optional[datetime] = None
    ) -> bool:
        """
        Case 01 & Case 36:
        18:00 EST 신세션 진입으로 논리 거래일(logical_date)이 변경되었을 때,
        미보유 & 미체결 상태인 경우 당일 세션 플래그(is_session_done, entry_session 등)를
        새 거래일 기준으로 깨끗이 초기화한다 (보유/미체결 주문이 있으면 날짜만 승계).
        """
        logical_today = get_logical_trading_date_str(now_est, rollover_hour=18)
        changed = False
        with self._mem_lock:
            for sym in REAL_ALLOWED_SYMBOLS:
                cur = self._data[sym]
                if cur.get("logical_date") != logical_today:
                    changed = True
                    cur["logical_date"] = logical_today
                    has_live_state = bool(
                        safe_int(cur.get("holdings_qty"), 0) > 0
                        or str(cur.get("buy_order_id") or "").strip()
                        or str(cur.get("sell_order_id") or "").strip()
                    )
                    if not has_live_state:
                        cur["is_session_done"] = False
                        cur["entry_session"] = ""
                        cur["entry_trend"] = ""
                        cur["ordered_qty"] = 0
                        cur["buy_price"] = 0.0
                        cur["target_sell_price"] = 0.0
                        cur["nq_entry_price"] = 0.0
                        cur["nq_entry_amp"] = 0.0
                        cur["entry_ts"] = 0.0
        if changed:
            await self.save_async()
            logger.info("📅 [논리 거래일 롤오버 완료] 신규 거래일=%s", logical_today)
        return changed

    async def reset_dual_ledgers_preserve_seed(self) -> None:
        """
        Case 02: /reset 명령 시 통합 시드(budget), 복리/단리(is_compound), 익절 점화율(target_profit_rate),
        누적 성적은 보존하고 모든 포지션/주문/세션 락을 원자적으로 0점 소각한다.
        """
        logical_today = get_logical_trading_date_str(rollover_hour=18)
        with self._mem_lock:
            for sym in REAL_ALLOWED_SYMBOLS:
                cur = self._data[sym]
                kept_budget = safe_float(cur.get("budget"), self.default_budget)
                kept_comp = bool(cur.get("is_compound", True))
                kept_rate = safe_float(cur.get("target_profit_rate"), 1.0)
                kept_active = bool(cur.get("is_active", True))
                kept_pnl = safe_float(cur.get("realized_pnl_usd"), 0.0)
                kept_trades = safe_int(cur.get("trade_count"), 0)
                kept_wins = safe_int(cur.get("win_count"), 0)

                fresh = _default_symbol_ledger_entry(sym, logical_today, kept_budget)
                fresh.update(
                    {
                        "is_active": kept_active,
                        "is_compound": kept_comp,
                        "target_profit_rate": kept_rate,
                        "realized_pnl_usd": kept_pnl,
                        "trade_count": kept_trades,
                        "win_count": kept_wins,
                    }
                )
                self._data[sym] = fresh
        await self.save_async()


# ---------------------------------------------------------------------------
# Case 07 & Case 11 : 장중 업데이트 락다운 및 10개 파일 프리플라이트 컴파일 검증기
# ---------------------------------------------------------------------------
def is_update_lockdown_window(now_est: Optional[datetime] = None) -> bool:
    """
    🚨 Case 07:
    평일(월 ~ 금) 03:30 ~ 17:00 EST 구간에는 장중 무인 자동 업데이트 및 재기동을 전면 차단한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 100 + dt.minute
    return bool(330 <= hm <= 1700)


def verify_all_modules_compile(base_dir: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    🚨 Case 11:
    분리된 서브 플러그인을 포함한 전체 10개 코어 모듈(ALL_PLUGIN_FILES)에 대해
    py_compile 프리플라이트 구문 검증을 수행한다.
    """
    root = os.path.abspath(base_dir or os.path.dirname(os.path.abspath(__file__)))
    errors: List[str] = []
    for fname in ALL_PLUGIN_FILES:
        fpath = os.path.join(root, fname)
        if not os.path.isfile(fpath):
            errors.append(f"MISSING:{fname}")
            continue
        try:
            py_compile.compile(fpath, doraise=True)
        except Exception as exc:
            errors.append(f"SYNTAX_ERR:{fname}:{exc}")
    return (len(errors) == 0), errors


async def execute_safe_hot_reload(context: Any) -> Dict[str, Any]:
    """
    Case 07 & Case 11:
    장중 락다운(03:30 ~ 17:00 EST)이 아니고 보유 포지션/미체결 주문이 없을 때만
    10개 파일 전체 py_compile 검증 통과 후 os._exit(0) 하드 킬 재기동을 수행한다.
    """
    if is_update_lockdown_window():
        return {"status": "BLOCKED_INTRADAY_LOCKDOWN"}

    ok, errs = await asyncio.to_thread(verify_all_modules_compile)
    if not ok:
        return {"status": "COMPILE_FAILED", "errors": errs}

    logger.info("♻️ 10개 플러그인 컴파일 검증 통과 -> os._exit(0) 하드 리로드 실행")
    os._exit(0)


# ---------------------------------------------------------------------------
# BotRuntimeContext : 전술 실행 컨텍스트 및 6대 캐시 바인딩 컨테이너
# ---------------------------------------------------------------------------
class BotRuntimeContext:
    """
    10개 플러그인이 공유하는 단일 런타임 컨텍스트 객체.
    """

    def __init__(self) -> None:
        default_seed = safe_float(os.getenv("SNIPER_INITIAL_BUDGET_USD"), 1000.0)
        self.api = TossInvestAPI()
        self.ledger = AssassinLedger(
            filepath=os.getenv("SNIPER_LEDGER_PATH", "assassin_ledger.json"),
            default_budget=default_seed,
        )
        self.macro_cache = MacroDataCache(self.api, nq_ttl_sec=15.0)
        self.candle_recorder = CandleRecorder(
            base_dir=os.getenv("SNIPER_CANDLE_DIR", "candle_archive"),
            retention_days=14,
        )

        # 비동기 이벤트 및 타격 뮤텍스 락 (Case 25, 27, 54, 63, 64)
        self.running: bool = True
        self.wakeup_event = asyncio.Event()
        self.in_memory_ordering_lock = asyncio.Lock()
        self.dual_strike_mutex = asyncio.Lock()

        # 6대 인메모리 캐시 및 보조 캐시 연결 (Case 02)
        self.trail_runtime_cache = _TRAIL_RUNTIME_CACHE
        self.exit_snapshot_cache = _EXIT_SNAPSHOT_CACHE
        self.bottom_track_cache = _BOTTOM_TRACK_CACHE
        self.pre_leader_cache = _PRE_LEADER_CACHE
        self.day_leader_cache = _DAY_LEADER_CACHE
        self.day_sim_state = _DAY_SIM_STATE
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}
        self.whipsaw_runtime_cache: Dict[str, Any] = {}
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}

        # 텔레그램 라우터 결속
        self.tg_router = TelegramBotRouter(self)

    async def send_slim_alert(self, title: str, body: str = "") -> Dict[str, Any]:
        return await self.tg_router.send_slim_alert(title=title, body=body)

    async def send_strike_hold_alert_once(
        self, symbol: str, reason_key: str, message: str
    ) -> None:
        await self.tg_router.send_strike_hold_alert_once(
            symbol=symbol, reason_key=reason_key, message=message
        )

    async def refresh_symbol_candles_once(
        self, symbols: Iterable[str] = ("SOXS", "SOXL")
    ) -> None:
        """
        지정된 심볼들의 다중 페이지(400분봉) 1분봉을 즉시 조회하여
        candle_metrics_cache 를 최신화하고 로컬 아카이브에 기록한다.
        """
        now_est = datetime.now(EST_TZ)
        prices = await self.api.get_current_prices_batch(("SOXS", "SOXL", "QQQ"))
        if safe_float(prices.get("QQQ"), 0.0) > 0.0:
            self.macro_cache.record_qqq_tick(prices["QQQ"])

        for sym in symbols:
            s_up = str(sym or "").upper().strip()
            if s_up not in REAL_ALLOWED_SYMBOLS:
                continue
            candles = await self.api.get_candles_multi_page(
                symbol=s_up, interval="1m", total_count=400
            )
            live_p = safe_float(prices.get(s_up), 0.0)
            metrics = await asyncio.to_thread(
                compute_session_vwap_metrics, candles, live_p, now_est
            )
            self.candle_metrics_cache[s_up] = metrics
            await self.candle_recorder.record_candles_async(
                s_up, candles, now_est=now_est
            )


# ---------------------------------------------------------------------------
# 🎯 3 & Case 28 : 6대 0.4초 고주파 부스터(fast_shield_mode) 판정기
# ---------------------------------------------------------------------------
def evaluate_fast_shield_mode(
    context: BotRuntimeContext,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: datetime,
) -> bool:
    """
    🎯 3 [6대 0.4초 고주파 부스터 (fast_shield_mode = True)] & Case 28:
    ① 프리장 또는 데이장 추세 주도주의 실시간 이격도(disp_curr)가 -0.20% 이하로 수축하여
      동적 VWAP -0.50% 매수 덫 체결권에 근접하거나 매수 덫(PRE_VWAP_M050 / DAY_VWAP_M050)이 대기 중일 때
    ② 실전 또는 데이장 [모의] 보유 포지션(수동 진입 포함)이 하이킨 아시 트레일링 점화(armed == True)
      또는 점화 근접(평단가 대비 +0.80% 이상) 상태일 때
    ③ 프리장 후반(07:15 ~ 08:14 EST) 또는 데이장 후반(02:45 ~ 03:57 EST) 미익절 포지션이
      동적 VWAP 선제 탈출(PRE_VWAP_ESC / DAY_VWAP_ESC)을 감시 중일 때
    ④ 실전 또는 데이장 [모의] 3차 안전망 손절 후 최저점 역트레일링 추적(_BOTTOM_TRACK_CACHE["active"] == True) 중일 때
    ⑤ 정규장 마감 1시간 전(15:00 ~ 15:58 EST) 미익절 보유 포지션이 당일 정규장 VWAP 탈출(VWAP_ESCAPE)
      감시 중이거나 15:59 ~ 16:01 EST MOC 윈도우일 때
    ⑥ 03:58 ~ 04:07 EST 1주 휩소 노출 작전 가동 중일 때
    """
    hm = now_est.hour * 100 + now_est.minute

    # 조건 ⑥: 03:58 ~ 04:07 EST 1주 휩소 노출 작전 윈도우 또는 15:59 ~ 16:01 EST MOC 윈도우
    if (358 <= hm <= 407) or (1559 <= hm <= 1601):
        return True

    # 조건 ④: 실전 또는 데이장 [모의] 3차 손절 후 최저점 반등 추적 중
    if bool(context.bottom_track_cache.get("active")) or bool(
        context.day_sim_state.get("bottom_track_active")
    ):
        return True

    # 데이장 [모의] 상태 점검 (조건 ①, ②, ③)
    sim_phase = str(context.day_sim_state.get("phase") or "IDLE")
    if sim_phase == "TRAP_WAITING":
        return True
    if sim_phase == "HOLDING":
        if bool(context.day_sim_state.get("day_sim_trail_armed")):
            return True
        sim_sym = str(context.day_sim_state.get("symbol") or "")
        sim_ent = safe_float(context.day_sim_state.get("entry_price"), 0.0)
        sim_live = safe_float((prices or {}).get(sim_sym), 0.0)
        if sim_ent > 0.0 and sim_live >= (sim_ent * 1.0080):
            return True
        if is_daymarket_late_vwap_esc_window(hm):
            return True

    # 추세 주도주 이격도 <= -0.20% 수축 점검 (조건 ①)
    if is_daymarket_sim_active_window(hm):
        d_leader = str(context.day_leader_cache.get("leader_sym") or "")
        if d_leader in REAL_ALLOWED_SYMBOLS:
            d_met = context.candle_metrics_cache.get(d_leader, {})
            if safe_float(d_met.get("day_disp_curr"), 0.0) <= -0.20:
                return True
    elif 407 <= hm <= 814:
        p_leader = str(context.pre_leader_cache.get("leader_sym") or "")
        if p_leader in REAL_ALLOWED_SYMBOLS:
            p_met = context.candle_metrics_cache.get(p_leader, {})
            if safe_float(p_met.get("pre_disp_curr"), 0.0) <= -0.20:
                return True

    # 실전 SOXS / SOXL 주문 및 포지션 점검 (조건 ①, ②, ③, ⑤)
    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if str(st.get("buy_order_id") or "").strip():
            return True

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        t_state = context.trail_runtime_cache.get(sym, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            return True

        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p >= (avg_p * 1.0080):
            return True

        if (715 <= hm <= 814) or (1500 <= hm <= 1558):
            return True

    return False


async def _wait_with_wakeup(wakeup_event: asyncio.Event, timeout_sec: float) -> None:
    """
    Case 25 & Case 64:
    지정된 timeout_sec 동안 대기하되, wakeup_event.set() 이 격발되면 0.0초 만에 즉시 깨어난다.
    """
    if wakeup_event.is_set():
        wakeup_event.clear()
        return
    try:
        await asyncio.wait_for(wakeup_event.wait(), timeout=max(0.05, float(timeout_sec)))
    except asyncio.TimeoutError:
        pass
    finally:
        wakeup_event.clear()


# ---------------------------------------------------------------------------
# [루프 1] 듀얼 5.0초 백그라운드 캔들 레이더 루프 (background_candle_radar_loop)
# ---------------------------------------------------------------------------
async def background_candle_radar_loop(context: BotRuntimeContext) -> None:
    """
    🎯 3 [듀얼 5.0초 캔들 기저 동기화]:
    - 데이장(19:00 ~ 03:57 EST) 및 프리장 골든타임(03:55 ~ 08:15 EST), 정규장 후반(14:55 ~ 16:00 EST),
      또는 포지션 보유 / 트레일링 점화 / 최저점 반등 추적 중일 때 SOXS 와 SOXL 양쪽 모두
      5.0초 주기로 다중 페이지(400분봉) 1분봉을 조회하여 VWAP_1907, VWAP_0407, 동적 VWAP_t,
      Wilder's RSI(14), 하이킨 아시 기저를 상시 동기화한다.
    - 주말 휴장 또는 16:02 ~ 18:55 EST 대기 구간에는 API Rate Limit 보호를 위해 20.0초 주기로 완화한다.
    """
    logger.info("📡 백그라운드 5.0초 듀얼 캔들 레이더 루프 가동 개시")
    while context.running:
        try:
            now_est = datetime.now(EST_TZ)
            bounds = get_cme_futures_session_bounds(now_est)
            if bounds["is_weekend_closed"]:
                await asyncio.sleep(15.0)
                continue

            hm = now_est.hour * 100 + now_est.minute
            in_active_session = bool(
                hm >= 1900
                or hm <= 815
                or (1455 <= hm <= 1601)
            )
            has_active_track = bool(
                context.bottom_track_cache.get("active")
                or context.day_sim_state.get("bottom_track_active")
                or str(context.day_sim_state.get("phase") or "") == "HOLDING"
                or any(
                    safe_int(context.ledger.get_state(s).get("holdings_qty"), 0) > 0
                    for s in REAL_ALLOWED_SYMBOLS
                )
            )

            # 0세션 대기 구간(16:05 ~ 18:55 EST)이고 미보유 시 캔들 폴링 휴식
            if (1605 <= hm <= 1855) and not has_active_track:
                await asyncio.sleep(15.0)
                continue

            await context.refresh_symbol_candles_once(("SOXS", "SOXL"))

            sleep_sec = (
                FAST_CANDLE_POLL_SEC
                if (in_active_session or has_active_track)
                else IDLE_CANDLE_POLL_SEC
            )
            await asyncio.sleep(sleep_sec)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("⚠️ 캔들 레이더 루프 예외: %s", exc)
            await asyncio.sleep(5.0)


# ---------------------------------------------------------------------------
# [루프 2] 0.4초 / 1.5초 메인 전술 타격 오케스트레이션 루프 (trading_strike_loop)
# ---------------------------------------------------------------------------
async def trading_strike_loop(context: BotRuntimeContext) -> None:
    """
    🎯 1 ~ 🎯 5 전체 시간대별 전술 플러그인을 오케스트레이션하는 메인 타격 루프.
    """
    logger.info("⚔️ 메인 전술 타격 루프(0.4s 부스터 / 1.5s 평시) 가동 개시")
    while context.running:
        try:
            now_est = datetime.now(EST_TZ)
            hm = now_est.hour * 100 + now_est.minute

            # 1) 18:00 EST 논리 거래일 롤오버 점검 및 주말 휴장 제어 (Case 01, 36)
            await context.ledger.check_and_rollover_logical_date(now_est)
            bounds = get_cme_futures_session_bounds(now_est)
            if bounds["is_weekend_closed"]:
                await _wait_with_wakeup(context.wakeup_event, 10.0)
                continue

            # 2) 실시간 배치 현재가(SOXS, SOXL, QQQ), 계좌 잔고, 매크로 스냅샷 동기화
            prices = await context.api.get_current_prices_batch(("SOXS", "SOXL", "QQQ"))
            holdings = await context.api.get_holdings_all()
            macro = await context.macro_cache.get_macro_snapshot(
                live_qqq_price=safe_float(prices.get("QQQ"), 0.0)
            )

            # 3) 매수 체결 직후 잔고 인덱싱 시차(0.1 ~ 0.4초) MANUAL 둔갑 방어 & 잔고 0주 복리 정산
            await _call_plugin_adaptive(
                sync_manual_and_indexing_guard,
                context,
                holdings=holdings,
                prices=prices,
                macro=macro,
            )
            await _call_plugin_adaptive(
                reconcile_zero_holdings_exit_plugin,
                context,
                holdings=holdings,
                prices=prices,
                macro=macro,
                now_est=now_est,
            )

            # 4) 시간대별 전술 플러그인 라우팅
            # [A] 19:00 ~ 03:57 EST: 데이장 100% [모의] 프리장 동형 스나이퍼 (실매수 물리적 차단!)
            if is_daymarket_sim_active_window(hm):
                await _call_plugin_adaptive(
                    execute_daymarket_sim_plugin,
                    context,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [B] 03:58 ~ 04:06 EST: 데이장 모의 포지션 묵음 클리어 & 과열독립 1주 휩소 노출 작전
            if 358 <= hm <= 406:
                await _call_plugin_adaptive(
                    settle_daymarket_sim_before_premarket,
                    context,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    execute_0358_whipsaw_trap_plugin,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    manage_whipsaw_1sh_profit_trap,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [C] 04:07 ~ 08:14 EST: 04:07 정각 1주 휩소 정리 후 프리장 본진 [실전] 동형 스나이퍼 가동
            if 407 <= hm <= 814:
                await _call_plugin_adaptive(
                    enforce_0407_whipsaw_cutoff_plugin,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    update_premarket_trend_leader,
                    context,
                    prices=prices,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    execute_post_stop_bottom_rebound_plugin,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    execute_premarket_vwap_sniper_plugin,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    sync_dynamic_vwap_buy_order_modify,
                    context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [D] 08:15 ~ 15:58 EST: 거시지표 발표 15분 전 선제 컷오프
            if 815 <= hm <= 1558:
                await _call_plugin_adaptive(
                    enforce_0815_macro_cutoff_plugin,
                    context,
                    holdings=holdings,
                    now_est=now_est,
                )

            # 5) 상시 가동: 보유 포지션 +5% 안전캡 & +1% 하드보존 HA 트레일링 & 후반부 VWAP 탈출 & 3단 방어망 & MOC
            await _call_plugin_adaptive(
                manage_mechanical_profit_trap_plugin,
                context,
                holdings=holdings,
                prices=prices,
                macro=macro,
                now_est=now_est,
            )
            await _call_plugin_adaptive(
                execute_premarket_late_vwap_escape,
                context,
                holdings=holdings,
                prices=prices,
                now_est=now_est,
            )
            await _call_plugin_adaptive(
                execute_stage1_2_nq_overheat_defense,
                context,
                holdings=holdings,
                prices=prices,
                macro=macro,
            )
            await _call_plugin_adaptive(
                execute_stage3_qqq_adverse_defense,
                context,
                holdings=holdings,
                prices=prices,
                macro=macro,
                now_est=now_est,
            )
            await _call_plugin_adaptive(
                execute_1500_regular_vwap_escape,
                context,
                holdings=holdings,
                prices=prices,
                now_est=now_est,
            )
            await _call_plugin_adaptive(
                execute_1559_moc_force_liquidation,
                context,
                holdings=holdings,
                prices=prices,
                now_est=now_est,
            )

            # 6) 6대 고주파 부스터 조건 평가 후 0.4초 또는 1.5초 대기
            fast_mode = evaluate_fast_shield_mode(context, holdings, prices, now_est)
            loop_timeout = (
                FAST_SHIELD_LOOP_TIMEOUT_SEC if fast_mode else NORMAL_LOOP_TIMEOUT_SEC
            )
            await _wait_with_wakeup(context.wakeup_event, loop_timeout)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("🚨 메인 타격 루프 예외: %s", exc, exc_info=True)
            await _wait_with_wakeup(context.wakeup_event, NORMAL_LOOP_TIMEOUT_SEC)


# ---------------------------------------------------------------------------
# [루프 3] 17:00 EST 일일 가비지 컬렉션(GC) 및 캐시 정비 루프 (Case 22)
# ---------------------------------------------------------------------------
async def daily_gc_maintenance_loop(context: BotRuntimeContext) -> None:
    """
    🚨 Case 22:
    매일 17:00 EST 정각 구간에 오래된 캔들 아카이브 및 .tmp 임시 파일을 청소하고
    파이썬 가비지 컬렉션(gc.collect)을 수행한다.
    """
    last_gc_date = ""
    while context.running:
        try:
            now_est = datetime.now(EST_TZ)
            date_str = now_est.strftime("%Y-%m-%d")
            if now_est.hour == 17 and now_est.minute == 0 and last_gc_date != date_str:
                last_gc_date = date_str
                removed_cnt = await context.candle_recorder.cleanup_old_files_async()
                collected_objs = await asyncio.to_thread(gc.collect)
                logger.info(
                    "🧹 [17:00 EST 일일 GC 완료] 아카이브 정리=%d개, gc.collect=%d개",
                    removed_cnt,
                    collected_objs,
                )
            await asyncio.sleep(20.0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("⚠️ 일일 GC 루프 예외: %s", exc)
            await asyncio.sleep(30.0)


# ---------------------------------------------------------------------------
# Case 11 : SIGTERM / SIGINT 0.1초 고속 하드 킬 핸들러 및 메인 엔트리포인트
# ---------------------------------------------------------------------------
def _install_fast_hard_kill_signals() -> None:
    """
    🚨 Case 11:
    systemd 재시작 또는 컨테이너 종료(SIGTERM / SIGINT) 수신 시 블로킹 스레드 대기 없이
    0.1초 내에 즉시 프로세스를 종료(os._exit(0))한다.
    """

    def _hard_kill_handler(signum: int, _frame: Any) -> None:
        logger.info("⚡ 시그널(%s) 수신 -> 0.1초 고속 하드 킬(os._exit(0)) 실행", signum)
        os._exit(0)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _hard_kill_handler)
        except Exception:
            pass


async def async_main() -> None:
    _install_fast_hard_kill_signals()

    # 1) 기동 시 10개 전체 플러그인 파일 구문(py_compile) 무결성 사전 검증 (Case 11)
    compile_ok, compile_errs = await asyncio.to_thread(verify_all_modules_compile)
    if not compile_ok:
        logger.error("🚨 플러그인 프리플라이트 컴파일 실패: %s", compile_errs)
    else:
        logger.info("✅ 전체 10개 플러그인 모듈 py_compile 무결성 검증 통과")

    context = BotRuntimeContext()

    # 2) 제2헌법: 종합매매(BROKERAGE) 계좌 AccountSeq 메모리 락온 및 초기 QQQ 기준가 확보
    try:
        await context.api.ensure_account_seq()
    except Exception as exc:
        logger.warning("⚠️ 기동 시 AccountSeq 락온 지연 (요청 시 자동 재시도): %s", exc)

    try:
        await context.refresh_symbol_candles_once(("SOXS", "SOXL"))
        await relock_qqq_baseline_on_bot_resume(context)
    except Exception as exc:
        logger.warning("⚠️ 기동 시 초기 캔들/QQQ 기준가 동기화 예외: %s", exc)

    # 3) Case 13: 서버 기동 완료 시 "✅ 서버가 정상 작동 중입니다." 단일 문구 타전
    await context.tg_router.send_startup_message()

    # 4) 4대 비동기 코어 태스크 병렬 가동
    tasks = [
        asyncio.create_task(trading_strike_loop(context), name="trading_strike_loop"),
        asyncio.create_task(
            background_candle_radar_loop(context), name="background_candle_radar_loop"
        ),
        asyncio.create_task(
            daily_gc_maintenance_loop(context), name="daily_gc_maintenance_loop"
        ),
        asyncio.create_task(
            context.tg_router.polling_loop(), name="telegram_polling_loop"
        ),
    ]
    await asyncio.gather(*tasks)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        os._exit(0)


if __name__ == "__main__":
    main()
