# -*- coding: utf-8 -*-
"""
# MODIFIED: main.py
인피니트 스노우볼 듀얼 스나이퍼 메인 오케스트레이터 및 비동기 스케줄러 (Step 04 / 04)
- MODIFIED: refresh_symbol_candles_once 에 가변 total_count(기본 400 / 딥스캔 시 600분봉) 파라미터를 추가하고,
  compute_session_vwap_metrics 호출 시 prev_metrics 를 주입하여 400분 경과 후에도
  19:07 / 04:07 초기 기준가 및 초기 VWAP 이 절대 휘발되지 않도록 불변 계승 결속
- NEW: async_main 기동 직후 _ensure_day_sim_caches 를 선행 호출하여 day_sim_state.json 디스크 파일에
  영속화된 당일 데이장/프리장 추세 주도주(day_leader_cache, pre_leader_cache) 및 데이장 [모의] 포지션을
  즉시 복원하고, 장 후반 미락온 기동 시 600분봉 딥스캔으로 초기 분봉을 확보하도록 완결
- 제1헌법 ~ 제6헌법 및 102대 엣지 케이스 방어망 완벽 준수
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Optional

from daymarket_sim_plugin import (
    LEADER_RECOVERY_DEEP_SCAN_COUNT,
    _ensure_day_sim_caches,
    create_default_day_sim_state,
    execute_daymarket_sim_plugin,
    is_daymarket_late_vwap_esc_window,
    is_daymarket_sim_active_window,
    persist_day_sim_state_async,
    settle_daymarket_sim_before_premarket,
)
from defense_plugin import (
    enforce_0815_macro_cutoff_plugin,
    execute_1500_vwap_escape_plugin,
    execute_1559_moc_liquidation_plugin,
    execute_macro_3stage_defense_plugin,
)
from plugin_updater import (
    is_update_lockdown_window,
    verify_all_modules_compile,
)
from quant_engine import NQ_OVERHEAT_THRESHOLD, MacroDataCache
from quant_indicators_plugin import (
    compute_session_vwap_metrics,
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    is_premarket_late_vwap_esc_window,
    manage_mechanical_profit_trap_plugin,
    reconcile_zero_holdings_exit_plugin,
)
from sniper_plugin import (
    execute_premarket_vwap_sniper_plugin,
    sync_dynamic_vwap_buy_order_modify,
    update_premarket_trend_leader,
)
from tg_ui_plugin import TelegramBotRouter
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    AssassinLedger,
    TossApiClient,
    _autoload_dotenv_files,
    safe_float,
    safe_int,
)
from whipsaw_plugin import (
    enforce_0407_whipsaw_cutoff_plugin,
    execute_whipsaw_1sh_plugin,
    is_whipsaw_window,
)

# 로깅 포맷 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("toss_sniper.main")

# 메인 루프 주기 상수 (평시 1.5초 / 5대 고주파 부스터 가동 시 0.4초)
NORMAL_LOOP_INTERVAL_SEC: float = 1.5
FAST_BOOSTER_INTERVAL_SEC: float = 0.4

# 백그라운드 1분봉 캔들 레이더 주기 상수 (집중 세션 5.0초 / 평시 15.0초)
ACTIVE_CANDLE_POLL_SEC: float = 5.0
IDLE_CANDLE_POLL_SEC: float = 15.0


# ---------------------------------------------------------------------------
# Case 11: SIGTERM / SIGINT 수신 시 0.1초 고속 하드 킬(os._exit(0)) 핸들러
# ---------------------------------------------------------------------------
def _install_fast_kill_signal_handlers() -> None:
    def _fast_kill_handler(signum: int, _frame: Any) -> None:
        logger.info(
            "🛑 시그널(%d) 수신 -> 0.1초 고속 하드 킬(os._exit(0)) 즉시 종료",
            signum,
        )
        os._exit(0)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _fast_kill_handler)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Case 12: 플러그인 함수 시그니처 적응형 호출기 (_call_plugin_adaptive)
# ---------------------------------------------------------------------------
async def _call_plugin_adaptive(func: Callable[..., Any], **kwargs: Any) -> Any:
    """
    Case 12: 플러그인 함수의 파라미터 시그니처를 동적으로 검사하여
    지원하는 인자만 안전하게 주입하고 동기/비동기 함수를 모두 실행한다.
    """
    sig = inspect.signature(func)
    accepts_var_kw = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )
    if accepts_var_kw:
        filtered_kwargs = kwargs
    else:
        filtered_kwargs = {
            k: v for k, v in kwargs.items() if k in sig.parameters
        }

    res = func(**filtered_kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# ---------------------------------------------------------------------------
# Case 01: 주말 휴장 구간 (금요일 17:00 EST ~ 일요일 17:59 EST) 판별기
# ---------------------------------------------------------------------------
def is_weekend_market_closed(now_est: Optional[datetime] = None) -> bool:
    """
    Case 01: 금요일 17:00 EST 부터 일요일 17:59 EST 까지를 주말 휴장 구간으로 판별한다.
    일요일 18:00 EST (CME 선물 신세션 오픈) 부터 정상 가동한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    hm = dt.hour * 100 + dt.minute
    if wd == 4 and hm >= 1700:
        return True
    if wd == 5:
        return True
    if wd == 6 and hm < 1800:
        return True
    return False


# ---------------------------------------------------------------------------
# 중앙 관제 컨텍스트 (SniperContext : 5대 인메모리 캐시 & 비동기 락 & 공유 상태)
# ---------------------------------------------------------------------------
class SniperContext:
    """
    제1헌법 ~ 제6헌법 및 Case 02:
    듀얼 스나이퍼의 API 클라이언트, 로컬 원장(AssassinLedger), 매크로 캐시(MacroDataCache),
    5대 핵심 인메모리 캐시(_TRAIL_RUNTIME_CACHE, _EXIT_SNAPSHOT_CACHE, _PRE_LEADER_CACHE,
    _DAY_LEADER_CACHE, _DAY_SIM_STATE) 및 비동기 타격 뮤텍스를 통합 관리하는 컨텍스트.
    """

    def __init__(self) -> None:
        _autoload_dotenv_files(override=False)
        self.running: bool = True
        self.api: TossApiClient = TossApiClient()
        self.ledger: AssassinLedger = AssassinLedger()
        self.macro_cache: MacroDataCache = MacroDataCache(toss_api=self.api)

        # Case 25 ~ 27 & Case 54: 소프트웨어 웨이크업 트리거 및 이중 타격 방지 비동기 뮤텍스
        self.wakeup_event: asyncio.Event = asyncio.Event()
        self.in_memory_ordering_lock: asyncio.Lock = asyncio.Lock()
        self.dual_strike_mutex: asyncio.Lock = asyncio.Lock()

        now_est = datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)

        # Case 02: 5대 핵심 인메모리 캐시
        self.trail_runtime_cache: Dict[str, Dict[str, Any]] = {}
        self.exit_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self.pre_leader_cache: Dict[str, Any] = {
            "logical_date": logical_date,
            "locked": False,
            "leader_sym": "",
            "vwap_0407": 0.0,
            "c_0407": 0.0,
            "disp_0407": 0.0,
            "notified": False,
            "hybrid_mode": False,
        }
        self.day_leader_cache: Dict[str, Any] = {
            "logical_date": logical_date,
            "locked": False,
            "leader_sym": "",
            "vwap_1907": 0.0,
            "c_1907": 0.0,
            "disp_1907": 0.0,
            "notified": False,
            "hybrid_mode": False,
        }
        self.day_sim_state: Dict[str, Any] = create_default_day_sim_state(
            logical_date
        )
        # 기동 직후 디스크(day_sim_state.json)로부터 주도주 및 모의 상태를 복원하도록 플래그 설정
        self.day_sim_state["_disk_loaded"] = False

        # 보조 런타임 캐시 (5.0초 1분봉 지표 캐시, 체결 인덱싱 그레이스 쉴드, 1주 휩소 캐시)
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}
        self.whipsaw_runtime_cache: Dict[str, Any] = {}

        self.fast_shield_mode: bool = False
        self.last_gc_date: str = ""
        self.last_rollover_date: str = logical_date

        # 텔레그램 라우터 결속
        self.tg_router: TelegramBotRouter = TelegramBotRouter(context=self)

    async def send_slim_alert(
        self, title: str, body: str = ""
    ) -> Dict[str, Any]:
        if hasattr(self, "tg_router") and self.tg_router is not None:
            return await self.tg_router.send_slim_alert(title=title, body=body)
        return {"ok": False}

    async def send_strike_hold_alert_once(
        self, symbol: str, reason_key: str, message: str
    ) -> None:
        if hasattr(self, "tg_router") and self.tg_router is not None:
            await self.tg_router.send_strike_hold_alert_once(
                symbol=symbol, reason_key=reason_key, message=message
            )

    async def refresh_symbol_candles_once(
        self,
        symbols: Iterable[str] = REAL_ALLOWED_SYMBOLS,
        total_count: int = 400,
    ) -> None:
        """
        # MODIFIED:
        다중 페이지(기본 400분봉 / 딥스캔 복원 시 600분봉) 조회로 19:00 EST 데이장 및
        04:00 EST 프리장 시계열을 확보하고, prev_metrics 를 주입하여 이미 확정된
        19:07 / 04:07 초기 기준가와 초기 VWAP 이 400분 경과 후에도 절대 휘발되지 않도록 보존한다.
        """
        now_est = datetime.now(EST_TZ)
        sym_tuple = tuple(str(s).upper().strip() for s in symbols if s)
        if not sym_tuple:
            return

        req_count = max(100, min(800, int(total_count)))
        prices = await self.api.get_current_prices_batch(sym_tuple)
        for sym in sym_tuple:
            try:
                candles = await self.api.get_candles_multi_page(
                    symbol=sym,
                    interval="1m",
                    total_count=req_count,
                    adjusted=True,
                )
                live_p = safe_float(prices.get(sym), 0.0)
                prev_met = (
                    self.candle_metrics_cache.get(sym)
                    if isinstance(self.candle_metrics_cache.get(sym), dict)
                    else None
                )
                metrics = compute_session_vwap_metrics(
                    candles=candles,
                    live_price=live_p,
                    now_est=now_est,
                    prev_metrics=prev_met,
                )
                self.candle_metrics_cache[sym] = metrics
            except Exception as exc:
                logger.warning("⚠️ [%s] 1분봉 캔들 지표 갱신 예외: %s", sym, exc)


# ---------------------------------------------------------------------------
# 🎯 3 & Case 28: 5대 조건 기반 0.4초 고주파 부스터 (fast_shield_mode) 판별기
# ---------------------------------------------------------------------------
def _evaluate_fast_shield_mode(
    context: SniperContext,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: datetime,
) -> bool:
    """
    🎯 3 [5대 0.4초 고주파 부스터 (fast_shield_mode = True)]:
    ① 프리장 또는 데이장 추세 주도주(vwap_leader_sym / day_vwap_leader_sym)의 실시간 이격도(disp_curr)가
       -0.20% 이하로 수축하여 동적 VWAP -0.50% 매수 덫 체결권에 근접하거나 매수 덫(PRE_VWAP_M050 / DAY_VWAP_M050)이 대기 중일 때
    ② 실전 또는 데이장 [모의] 보유 포지션(수동 진입 포함)이 하이킨 아시 트레일링 점화(armed == True)
       또는 점화 근접(평단가 대비 +0.80% 이상) 상태일 때
    ③ 프리장 후반(07:15 ~ 08:14 EST) 또는 데이장 후반(02:45 ~ 03:57 EST) 미익절 포지션이
       동적 VWAP 선제 탈출(PRE_VWAP_ESC / DAY_VWAP_ESC)을 감시 중일 때
    ④ 정규장 마감 1시간 전(15:00 ~ 15:58 EST) 미익절 보유 포지션이 당일 정규장 VWAP 탈출(VWAP_ESCAPE) 감시 중일 때
    ⑤ 03:58 ~ 04:07 EST 1주 휩소 노출 작전 가동 중일 때
    """
    hm = now_est.hour * 100 + now_est.minute

    # 조건 ⑤: 03:58 ~ 04:07 EST 1주 휩소 노출 작전 윈도우
    if is_whipsaw_window(hm) or hm == 407:
        return True

    # 조건 ①-A: 실전 미체결 매수 덫 대기 중 또는 데이장 [모의] 매수 덫 대기 중
    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if str(st.get("buy_order_id") or "").strip():
            return True

    day_sim = getattr(context, "day_sim_state", {})
    sim_phase = str(day_sim.get("phase") or "")
    if is_daymarket_sim_active_window(hm) and sim_phase in (
        "TRAP_WAITING",
        "HOLDING",
    ):
        if sim_phase == "TRAP_WAITING":
            return True
        if bool(day_sim.get("day_sim_trail_armed")):
            return True
        sim_ent_p = safe_float(day_sim.get("entry_price"), 0.0)
        sim_sym = str(day_sim.get("symbol") or "")
        sim_live_p = safe_float((prices or {}).get(sim_sym), 0.0)
        if sim_ent_p > 0.0 and sim_live_p >= (sim_ent_p * 1.0080):
            return True
        if is_daymarket_late_vwap_esc_window(hm):
            return True

    # 조건 ①-B: 프리장(04:07~07:55) 또는 데이장(19:07~03:35) 추세 주도주 이격도가 -0.20% 이하로 수축 시
    if 407 <= hm <= 755:
        pre_ldr = str(
            context.pre_leader_cache.get("leader_sym") or ""
        ).upper()
        if pre_ldr in REAL_ALLOWED_SYMBOLS:
            met = refresh_metrics_with_live_tick(
                context.candle_metrics_cache.get(pre_ldr),
                safe_float((prices or {}).get(pre_ldr), 0.0),
                now_est=now_est,
            )
            if safe_float(met.get("pre_disp_curr"), 0.0) <= -0.20:
                return True
    elif hm >= 1907 or hm <= 335:
        day_ldr = str(
            context.day_leader_cache.get("leader_sym") or ""
        ).upper()
        if day_ldr in REAL_ALLOWED_SYMBOLS:
            met = refresh_metrics_with_live_tick(
                context.candle_metrics_cache.get(day_ldr),
                safe_float((prices or {}).get(day_ldr), 0.0),
                now_est=now_est,
            )
            if safe_float(met.get("day_disp_curr"), 0.0) <= -0.20:
                return True

    # 조건 ②, ③, ④: 실전 보유 포지션 점검
    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        # 조건 ②: 트레일링 점화 완료 또는 평단 대비 +0.80% 이상 근접
        t_state = context.trail_runtime_cache.get(sym, {})
        if isinstance(t_state, dict) and bool(t_state.get("armed")):
            return True
        live_p = safe_float(
            (prices or {}).get(sym), safe_float(h_info.get("last_price"), avg_p)
        )
        if live_p >= (avg_p * 1.0080):
            return True

        # 조건 ③: 프리장 후반(07:15 ~ 08:14 EST) 미익절 동적 VWAP 선제 탈출 감시
        if is_premarket_late_vwap_esc_window(hm):
            return True

        # 조건 ④: 정규장 마감 1시간 전(15:00 ~ 15:58 EST) 정규장 VWAP 탈출 감시 또는 15:59 MOC
        if 1500 <= hm <= 1601:
            return True

    return False


# ---------------------------------------------------------------------------
# 백그라운드 루프 1 : 듀얼 5.0초 1분봉 캔들 기저 레이더 (candle_radar_loop)
# ---------------------------------------------------------------------------
async def candle_radar_loop(context: SniperContext) -> None:
    """
    🎯 3: 데이장(19:00 ~ 03:57 EST) 및 프리장(03:55 ~ 08:15 EST) 또는 포지션 보유 중일 때
    5.0초 주기로 SOXS 와 SOXL 의 다중 페이지(400분봉) 1분봉 시계열을 조회하여
    동적 VWAP_t, VWAP_1907, VWAP_0407, 하이킨 아시 기저를 상시 동기화한다.
    """
    while context.running:
        now_est = datetime.now(EST_TZ)
        if is_weekend_market_closed(now_est):
            await asyncio.sleep(30.0)
            continue

        hm = now_est.hour * 100 + now_est.minute
        in_focus_window = bool(
            hm >= 1900 or hm <= 815 or (1500 <= hm <= 1600)
        )
        has_active_trail = any(
            bool((context.trail_runtime_cache.get(s) or {}).get("armed"))
            for s in REAL_ALLOWED_SYMBOLS
        )
        poll_interval = (
            ACTIVE_CANDLE_POLL_SEC
            if (in_focus_window or has_active_trail or context.fast_shield_mode)
            else IDLE_CANDLE_POLL_SEC
        )

        try:
            await context.refresh_symbol_candles_once(
                REAL_ALLOWED_SYMBOLS, total_count=400
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("캔들 레이더 루프 일시 예외: %s", exc)

        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# 백그라운드 루프 2 : 17:00 EST 일일 GC 및 18:00 EST 신세션 롤오버 루프
# ---------------------------------------------------------------------------
async def daily_gc_and_rollover_loop(context: SniperContext) -> None:
    """
    Case 01 & Case 22 & Case 93:
    - 17:00 EST 정각 일일 가비지 컬렉션(GC) 파이프라인 실행
    - 18:00 EST CME 선물 신세션 오픈 시 논리적 거래일 롤오버, 당일 세션 플래그 초기화 및
      토스 QQQ 초기 시세 확보
    """
    while context.running:
        try:
            now_est = datetime.now(EST_TZ)
            today_cal_str = now_est.strftime("%Y-%m-%d")
            logical_date = get_logical_trading_date_str(
                now_est, rollover_hour=18
            )
            hm = now_est.hour * 100 + now_est.minute

            # 1) 17:00 EST 일일 가비지 컬렉션 (Case 22)
            if hm == 1700 and context.last_gc_date != today_cal_str:
                context.last_gc_date = today_cal_str
                collected = await asyncio.to_thread(gc.collect)
                logger.info(
                    "🧹 [17:00 EST 일일 가비지 컬렉션 완료] collected=%d objects",
                    collected,
                )

            # 2) 18:00 EST 신세션 논리적 거래일 롤오버 감지 (Case 01)
            if logical_date != context.last_rollover_date:
                prev_logical = context.last_rollover_date
                context.last_rollover_date = logical_date
                holdings = await context.api.get_holdings_all()

                for sym in REAL_ALLOWED_SYMBOLS:
                    h_qty = safe_int(
                        (holdings or {}).get(sym, {}).get("qty"), 0
                    )
                    if h_qty == 0:
                        await context.ledger.update_state(
                            sym,
                            buy_order_id="",
                            sell_order_id="",
                            ordered_qty=0,
                            buy_price=0.0,
                            target_sell_price=0.0,
                            entry_session="",
                            entry_trend="",
                            is_session_done=False,
                        )
                    else:
                        # 수동 오버나이트 보유 물량이 있으면 세션 플래그만 유지
                        await context.ledger.update_state(
                            sym,
                            is_session_done=True,
                        )

                hyb_d = bool(context.day_leader_cache.get("hybrid_mode", False))
                context.day_leader_cache.clear()
                context.day_leader_cache.update(
                    {
                        "logical_date": logical_date,
                        "locked": False,
                        "leader_sym": "",
                        "vwap_1907": 0.0,
                        "c_1907": 0.0,
                        "disp_1907": 0.0,
                        "notified": False,
                        "hybrid_mode": hyb_d,
                    }
                )
                hyb_p = bool(context.pre_leader_cache.get("hybrid_mode", False))
                context.pre_leader_cache.clear()
                context.pre_leader_cache.update(
                    {
                        "logical_date": logical_date,
                        "locked": False,
                        "leader_sym": "",
                        "vwap_0407": 0.0,
                        "c_0407": 0.0,
                        "disp_0407": 0.0,
                        "notified": False,
                        "hybrid_mode": hyb_p,
                    }
                )
                context.day_sim_state.clear()
                context.day_sim_state.update(
                    create_default_day_sim_state(logical_date)
                )
                await persist_day_sim_state_async(
                    context.day_sim_state, context=context
                )
                await context.macro_cache.ensure_qqq_live_price()
                logger.info(
                    "🌐 [18:00 EST 신세션 롤오버 완료] %s -> %s",
                    prev_logical,
                    logical_date,
                )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("GC/롤오버 루프 대기: %s", exc)

        await asyncio.sleep(10.0)


# ---------------------------------------------------------------------------
# 메인 전술 오케스트레이션 루프 (main_sniper_loop)
# ---------------------------------------------------------------------------
async def main_sniper_loop(context: SniperContext) -> None:
    """
    제1헌법 ~ 제6헌법 & 🎯 1 ~ 🎯 5:
    전 시간대(18:00 선물 신세션 -> 19:00 데이장 [모의] -> 03:58 1주 휩소 ->
    04:07 프리장 동적 VWAP -0.50% 본진 -> 08:15 선제 컷오프 -> 15:00 정규장 VWAP 탈출 ->
    15:59 MOC 강제 청산)를 총괄하는 메인 루프.
    """
    while context.running:
        loop_start_mono = time.monotonic()
        context.wakeup_event.clear()

        try:
            now_est = datetime.now(EST_TZ)

            # 주말 휴장 구간(금 17:00 ~ 일 17:59 EST)에는 관망 대기 (Case 01)
            if is_weekend_market_closed(now_est):
                context.fast_shield_mode = False
                await asyncio.sleep(5.0)
                continue

            hm = now_est.hour * 100 + now_est.minute

            # 1. 실시간 현재가 배치 조회 (SOXS, SOXL, QQQ 단일 호출) 및 실보유 잔고 조회
            prices = await context.api.get_current_prices_batch(
                ("SOXS", "SOXL", "QQQ")
            )
            holdings = await context.api.get_holdings_all()

            has_real_pos = any(
                safe_int((holdings or {}).get(s, {}).get("qty"), 0) > 0
                for s in REAL_ALLOWED_SYMBOLS
            )
            has_sim_pos = bool(
                is_daymarket_sim_active_window(hm)
                and str(context.day_sim_state.get("phase") or "") == "HOLDING"
            )

            # 2. 매크로 스냅샷 갱신 (보유 포지션 존재 시 QQQ 1.5초 프로빙 활성화)
            macro = await context.macro_cache.get_macro_snapshot(
                live_qqq_price=safe_float(prices.get("QQQ"), 0.0),
                has_active_position=(has_real_pos or has_sim_pos),
                now_est=now_est,
            )

            # 3. 실보유 잔고 0주 복귀 시 실발생 순수익금(세전 - 왕복수수료 0.14%) 복리/단리 정산
            #    및 07:45 EST 이전 프리장 다회전(Multi-Cycle) 자동 해금
            await _call_plugin_adaptive(
                reconcile_zero_holdings_exit_plugin,
                context=context,
                holdings=holdings,
                macro=macro,
                now_est=now_est,
            )

            # 4. 실보유 물량 존재 시: 상단 안전 캡(+5.00% LIMIT) 선장전,
            #    +1.00% 점화 하드보존 하이킨 아시 트레일링(HARD_TRAIL_100 × NQ_VEL_BYPASS),
            #    프리장 후반(07:15 ~ 08:14 EST) 미익절 동적 VWAP 선제 탈출(PRE_VWAP_ESC) 가동
            if has_real_pos:
                await _call_plugin_adaptive(
                    manage_mechanical_profit_trap_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # 5. 방향성 인지 매크로 3단 방어망 (1·2차 NQ=F 진폭 과열방어 & 3차 QQQ 실시간 최소 0.80% 역행방어)
            await _call_plugin_adaptive(
                execute_macro_3stage_defense_plugin,
                context=context,
                holdings=holdings,
                prices=prices,
                macro=macro,
                now_est=now_est,
            )

            # 6. EST 시간대별 전술 플러그인 라우팅
            # [A] 18:00 ~ 18:59 EST: 선물 신세션 오픈 및 QQQ 기준가 초기 확보
            if 1800 <= hm <= 1859:
                await context.macro_cache.ensure_qqq_live_price(
                    safe_float(prices.get("QQQ"), 0.0)
                )

            # [B] 19:00 ~ 03:57 EST: 데이장 100% [모의] 전용 프리장 동형 스나이퍼 (실매수 절대 차단!)
            elif is_daymarket_sim_active_window(hm):
                await _call_plugin_adaptive(
                    execute_daymarket_sim_plugin,
                    context=context,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [C] 03:58 ~ 04:06 EST: 데이장 [모의] 묵음 클리어 및 03:58 ~ 04:07 듀얼 1주 휩소 노출 작전
            elif is_whipsaw_window(hm):
                await _call_plugin_adaptive(
                    settle_daymarket_sim_before_premarket,
                    context=context,
                    now_est=now_est,
                )
                await _call_plugin_adaptive(
                    execute_whipsaw_1sh_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [D] 04:07 ~ 08:14 EST: 프리장 본진 골든타임 (👑 VWAP_TREND_M050 / 🪤 DYN_VWAP_M050)
            elif 407 <= hm <= 814:
                # 04:07:00 EST 정각 1주 휩소 타임쉴드 정리 및 본진 인계/과열차단 복원
                await _call_plugin_adaptive(
                    enforce_0407_whipsaw_cutoff_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                # 04:07 EST 추세 주도주(C_0407 > VWAP_0407) 선정 및 불변 복원
                await _call_plugin_adaptive(
                    update_premarket_trend_leader,
                    context=context,
                    prices=prices,
                    now_est=now_est,
                )
                # 04:07 ~ 07:55 EST 실시간 동적 프리장 VWAP -0.50% 매수 덫 투하 (PRE_VWAP_M050)
                await _call_plugin_adaptive(
                    execute_premarket_vwap_sniper_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )
                # 04:07 ~ 08:14 EST 미체결 매수 덫 체결 감시(그레이스 쉴드) 및 15초/$0.03 동적 추적 정정
                await _call_plugin_adaptive(
                    sync_dynamic_vwap_buy_order_modify,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # [E] 08:15 ~ 15:58 EST: 08:15 거시지표 선제 컷오프 & 15:00 ~ 15:58 정규장 VWAP 탈출
            elif 815 <= hm <= 1558:
                await _call_plugin_adaptive(
                    enforce_0815_macro_cutoff_plugin,
                    context=context,
                    holdings=holdings,
                    now_est=now_est,
                )
                if 1500 <= hm <= 1558:
                    await _call_plugin_adaptive(
                        execute_1500_vwap_escape_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        macro=macro,
                        now_est=now_est,
                    )

            # [F] 15:59 ~ 16:01 EST: 제로-오버나이트 MOC 2단계 방탄 강제 청산
            elif 1559 <= hm <= 1601:
                await _call_plugin_adaptive(
                    execute_1559_moc_liquidation_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    macro=macro,
                    now_est=now_est,
                )

            # 7. 5대 조건 기반 0.4초 고주파 부스터(fast_shield_mode) 판별
            context.fast_shield_mode = _evaluate_fast_shield_mode(
                context=context,
                holdings=holdings,
                prices=prices,
                now_est=now_est,
            )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("🚨 메인 스나이퍼 루프 예외: %s", exc, exc_info=True)

        target_sleep = (
            FAST_BOOSTER_INTERVAL_SEC
            if context.fast_shield_mode
            else NORMAL_LOOP_INTERVAL_SEC
        )
        elapsed = time.monotonic() - loop_start_mono
        remain_sleep = max(0.05, target_sleep - elapsed)
        try:
            await asyncio.wait_for(
                context.wakeup_event.wait(), timeout=remain_sleep
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# 엔트리포인트 : 프리플라이트 검증 ➔ AccountSeq 락온 ➔ 디스크 주도주 복원 ➔ 4대 태스크 가동
# ---------------------------------------------------------------------------
async def async_main() -> None:
    _install_fast_kill_signal_handlers()
    _autoload_dotenv_files(override=False)

    # Case 11: 기동 직전 전체 코어 모듈 py_compile 무결성 검증
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ok, compile_errs = await asyncio.to_thread(
        verify_all_modules_compile, base_dir, True
    )
    if not ok:
        logger.error(
            "🚨 프리플라이트 컴파일 에러 감지:\n%s", "\n".join(compile_errs)
        )
        sys.exit(1)

    context = SniperContext()

    # 제2헌법: 기동 시 /api/v1/accounts 1회 호출하여 종합매매(BROKERAGE) accountSeq 메모리 락온
    try:
        acc_seq = await context.api.ensure_account_seq()
        logger.info("🏛️ [제2헌법 AccountSeq 락온 완료] accountSeq=%s", acc_seq)
    except Exception as exc:
        logger.warning(
            "⚠️ 기동 시 AccountSeq 초기 락온 대기 (재시도 예정): %s", exc
        )

    # 기동 직후 디스크(day_sim_state.json)에 저장된 당일 주도주 및 모의 상태 선행 복원
    now_est = datetime.now(EST_TZ)
    logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)
    hm = now_est.hour * 100 + now_est.minute
    try:
        await _ensure_day_sim_caches(context, logical_date)
    except Exception as exc:
        logger.warning("⚠️ 기동 시 디스크 모의/주도주 캐시 복원 예외: %s", exc)

    # 데이장 후반(01:30 ~ 03:57 EST)에 주도주가 아직 미락온 상태이면 600분봉 딥스캔으로 초기 동기화
    need_deep_scan = bool(
        (130 <= hm <= 357) and not bool(context.day_leader_cache.get("locked"))
    )
    init_candle_count = (
        LEADER_RECOVERY_DEEP_SCAN_COUNT if need_deep_scan else 400
    )

    try:
        await context.refresh_symbol_candles_once(
            REAL_ALLOWED_SYMBOLS, total_count=init_candle_count
        )
        await context.macro_cache.ensure_qqq_live_price()
    except Exception as exc:
        logger.warning("⚠️ 초기 캔들/QQQ 시세 선행 동기화 예외: %s", exc)

    tasks = [
        asyncio.create_task(
            main_sniper_loop(context), name="main_sniper_loop"
        ),
        asyncio.create_task(
            candle_radar_loop(context), name="candle_radar_loop"
        ),
        asyncio.create_task(
            daily_gc_and_rollover_loop(context),
            name="daily_gc_and_rollover_loop",
        ),
        asyncio.create_task(
            context.tg_router.polling_loop(), name="tg_polling_loop"
        ),
    ]

    logger.info(
        "🚀 [인피니트 스노우볼 듀얼 스나이퍼 가동 완료] 전략=👑VWAP_TREND_M050 (추세 주도주 불변 각인 & 600분봉 복원 탑재)"
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        context.running = False
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        os._exit(0)
