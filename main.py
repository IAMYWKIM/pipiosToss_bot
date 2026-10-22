# -*- coding: utf-8 -*-
"""
# MODIFIED: main.py
순수 롱(SOXL) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼(전술 A 챔피언 : 🛡️ 05_AF_WIDE_10_01)
메인 오케스트레이터 및 비동기 스케줄러 (Step 05 / 05)
- MODIFIED (제3헌법): 데이장(daymarket_sim_plugin.py), 1주 휩소(whipsaw_plugin.py), 인버스 숏(SOXS),
  08:15 선제 컷오프, 15:59 강제 손절 MOC 청산 및 구버전 가상 래칫 부스터 잔재를 100% 영구 폐기
- MODIFIED (🎯 1 ~ 🎯 5 & Case 59, 60, 64): 오직 프리장 개시(04:07 EST)부터 정규장 마감(16:00 EST)까지만 연속 가동하며,
  '8% 황금비율 정액 기반 정량제(COMP_08PCT 고정) + 자투리/수익금 복리 산입 + 0주 즉시 실물 1층 진입 +
  2층 단독 보유 시 10분 게이트 눌림(-1.0%)/돌파(+1.0%) 독립 실물 1층 재장전 + 기억된 최저가 기준 10분 고정
  4단 와이드 시드보호 매수 게이트(-1.0% / -2.0% / -3.5% / -5.5%) + 1분 상시 실물 스나이퍼 매도(08_VREV_PROG 고정)' 단일 아키텍처로 구동
- NEW: 기동 직전 폐기 파일 자동 청소(purge_deprecated_legacy_files), 9대 코어 모듈 py_compile + AST 심볼 임포트 검증,
  TDD 수리 공식 검증(run_quant_engine_tdd) 및 오버나이트 지층-실잔고 화해 선행 실행
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 64 완벽 준수
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

from defense_plugin import (
    execute_0400_premarket_stabilization_plugin,
    execute_1600_daily_closing_settlement_plugin,
    reconcile_stratum_with_broker_holdings_plugin,
)
from plugin_updater import (
    purge_deprecated_legacy_files,
    verify_all_modules_compile,
)
from quant_engine import (
    run_quant_engine_tdd,
    summarize_stratum_portfolio,
)
from quant_indicators_plugin import (
    compute_buy_gate_bucket_info,
    compute_session_vwap_metrics,
    get_logical_trading_date_str,
    is_active_trading_window,
    is_daily_settlement_window,
    is_premarket_stabilization_window,
)
from sniper_exit_plugin import execute_1m_sniper_sell_and_reanchor_plugin
from sniper_plugin import execute_seed_protected_buy_gate_plugin
from tg_ui_plugin import TelegramBotRouter
from toss_api import (
    EST_TZ,
    LOCKED_BUY_INTERVAL_MIN,
    REAL_ALLOWED_SYMBOLS,
    TARGET_SYMBOL,
    AssassinLedger,
    TossApiClient,
    _autoload_dotenv_files,
    safe_float,
    safe_int,
)

# 로깅 포맷 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("toss_sniper.main")

# 메인 루프 주기 상수 (평시 1.5초 / 고주파 스나이퍼 부스터 가동 시 0.4초)
NORMAL_LOOP_INTERVAL_SEC: float = 1.5
FAST_BOOSTER_INTERVAL_SEC: float = 0.4

# 백그라운드 1분봉 캔들 레이더 주기 상수 (04:00 ~ 16:05 EST 운용 중 5.0초 / 장외 20.0초)
ACTIVE_CANDLE_POLL_SEC: float = 5.0
IDLE_CANDLE_POLL_SEC: float = 20.0


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
# 중앙 관제 컨텍스트 (SniperContext : SOXL 단일 롱 지층관리 원장 & 비동기 뮤텍스)
# ---------------------------------------------------------------------------
class SniperContext:
    """
    제1헌법 ~ 제6헌법 및 Case 01 ~ 64:
    순수 롱(SOXL) 지층관리 스나이퍼의 API 클라이언트, 로컬 원장(AssassinLedger),
    1분봉 지표 캐시, 체결 그레이스 쉴드 및 비동기 타격 뮤텍스를 통합 관리하는 컨텍스트.
    """

    def __init__(self) -> None:
        _autoload_dotenv_files(override=False)
        self.running: bool = True
        self.api: TossApiClient = TossApiClient()
        self.ledger: AssassinLedger = AssassinLedger()

        # 소프트웨어 웨이크업 트리거 및 이중 타격 방지 비동기 뮤텍스
        self.wakeup_event: asyncio.Event = asyncio.Event()
        self.in_memory_ordering_lock: asyncio.Lock = asyncio.Lock()
        self.dual_strike_mutex: asyncio.Lock = asyncio.Lock()

        now_est = datetime.now(EST_TZ)
        logical_date = get_logical_trading_date_str(now_est, rollover_hour=18)

        # 런타임 보조 캐시 (보유 스냅샷, 5.0초 1분봉 지표 캐시, 체결 인덱싱 그레이스 쉴드)
        self.exit_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}

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
        total_count: int = 300,
    ) -> None:
        """
        SOXL 의 1분봉 시계열을 조회하여 당일 프리장(04:00 ~ 09:29) 및 정규장(09:30 ~ 16:00)
        고/저가, 진폭, VWAP, 하이킨 아시 및 10분 매수 게이트 버킷 상태를 갱신한다.
        """
        now_est = datetime.now(EST_TZ)
        req_count = max(100, min(600, int(total_count)))
        prices = await self.api.get_current_prices_batch((TARGET_SYMBOL,))
        live_p = safe_float(prices.get(TARGET_SYMBOL), 0.0)

        try:
            candles = await self.api.get_candles_multi_page(
                symbol=TARGET_SYMBOL,
                interval="1m",
                total_count=req_count,
                adjusted=True,
            )
            prev_met = (
                self.candle_metrics_cache.get(TARGET_SYMBOL)
                if isinstance(
                    self.candle_metrics_cache.get(TARGET_SYMBOL), dict
                )
                else None
            )
            metrics = compute_session_vwap_metrics(
                candles=candles,
                live_price=live_p,
                now_est=now_est,
                prev_metrics=prev_met,
            )
            self.candle_metrics_cache[TARGET_SYMBOL] = metrics
        except Exception as exc:
            logger.warning("⚠️ [SOXL] 1분봉 캔들 지표 갱신 예외: %s", exc)


# ---------------------------------------------------------------------------
# 0.4초 고주파 스나이퍼 부스터 (_evaluate_fast_shield_mode) 판별기 (전술 A 동기화)
# ---------------------------------------------------------------------------
def _evaluate_fast_shield_mode(
    context: SniperContext,
    prices: Dict[str, float],
    now_est: datetime,
) -> bool:
    """
    MODIFIED (전술 A : 🛡️ 05_AF_WIDE_10_01 & Case 59, 60, 64):
    04:07 ~ 16:00 EST 운용 구간에서 아래 조건 중 하나라도 충족하면
    메인 루프 주기를 1.5초에서 0.4초 고주파 모드(fast_shield_mode = True)로 즉시 가속한다:
    ① 미체결 매수 주문(buy_order_id) 또는 미체결 매도 주문(sell_order_id)이 대기 중일 때
    ② 보유 지층이 0주(held_shares == 0)여서 즉시 현재가 실물 1층(REENTRY_1FL) 산입을 준비 중일 때
    ③ 10분 고정 시드보호 매수 게이트 정각 분봉(is_exact_boundary_min == True)이거나
       당일 현재 10분 버킷(active_bucket_id)이 아직 평가되지 않았을 때
    ④ 현재가(live_p)가 1층 실물 익절 목표가(+1.50%) 또는 2층 졸업 목표가(+1.00%) 대비
       0.30% 이내로 근접했을 때
    ⑤ 2층 단독 보유(is_l2_only_anti_bloat == True) 중 현재가가 직전 익절가 대비
       눌림목(-1.00%) 또는 상승돌파(+1.00%) 재장전 타점에 근접했을 때
    """
    hm = now_est.hour * 100 + now_est.minute
    if not is_active_trading_window(hm):
        return False

    st = context.ledger.get_state(TARGET_SYMBOL)
    if not bool(st.get("is_active", True)):
        return False

    if (
        str(st.get("buy_order_id") or "").strip()
        or str(st.get("sell_order_id") or "").strip()
    ):
        return True

    live_p = safe_float((prices or {}).get(TARGET_SYMBOL), 0.0)
    summary = summarize_stratum_portfolio(st, live_price=live_p)

    # 0주 상태일 때는 즉시 현재가 1층 진입을 위해 0.4초 부스터 가동
    if safe_int(summary.get("held_shares"), 0) == 0:
        return True

    gate_info = compute_buy_gate_bucket_info(
        now_est, buy_interval_min=LOCKED_BUY_INTERVAL_MIN
    )
    cur_bucket_id = safe_int(gate_info.get("active_bucket_id"), -1)
    last_eval_bucket_id = safe_int(
        st.get("last_evaluated_buy_bucket_id"), -1
    )
    last_eval_date = str(st.get("last_evaluated_date") or "").strip()
    today_str = str(gate_info.get("today_str") or "").strip()

    if bool(gate_info.get("is_exact_boundary_min")) or (
        cur_bucket_id >= 0
        and (today_str != last_eval_date or cur_bucket_id != last_eval_bucket_id)
    ):
        return True

    if live_p > 0.0:
        l1_target = safe_float(summary.get("l1_target_price"), 0.0)
        l2_target = safe_float(summary.get("l2_target_price"), 0.0)
        if l1_target > 0.0 and live_p >= (l1_target * 0.9970):
            return True
        if l2_target > 0.0 and live_p >= (l2_target * 0.9970):
            return True

        if bool(summary.get("is_l2_only_anti_bloat")):
            pull_p = safe_float(summary.get("reentry_pullback_price"), 0.0)
            brk_p = safe_float(summary.get("reentry_breakout_price"), 0.0)
            if pull_p > 0.0 and live_p <= (pull_p * 1.0030):
                return True
            if brk_p > 0.0 and live_p >= (brk_p * 0.9970):
                return True

    return False


# ---------------------------------------------------------------------------
# 백그라운드 루프 1 : SOXL 1분봉 캔들 기저 레이더 (candle_radar_loop)
# ---------------------------------------------------------------------------
async def candle_radar_loop(context: SniperContext) -> None:
    """
    04:00 ~ 16:05 EST 운용 시간대에는 5.0초 주기로, 장외 시간대에는 20.0초 주기로
    SOXL 1분봉 시계열을 갱신한다.
    """
    while context.running:
        now_est = datetime.now(EST_TZ)
        if is_weekend_market_closed(now_est):
            await asyncio.sleep(30.0)
            continue

        hm = now_est.hour * 100 + now_est.minute
        in_focus_window = bool(400 <= hm <= 1605)
        poll_interval = (
            ACTIVE_CANDLE_POLL_SEC
            if (in_focus_window or context.fast_shield_mode)
            else IDLE_CANDLE_POLL_SEC
        )

        try:
            await context.refresh_symbol_candles_once(
                REAL_ALLOWED_SYMBOLS, total_count=300
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("캔들 레이더 루프 일시 예외: %s", exc)

        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# 백그라운드 루프 2 : 17:00 EST 일일 GC 및 거래일 롤오버 루프
# ---------------------------------------------------------------------------
async def daily_gc_and_rollover_loop(context: SniperContext) -> None:
    """
    Case 01 & Case 22 & Case 64:
    - 17:00 EST 정각 일일 가비지 컬렉션(GC) 파이프라인 실행
    - 새 거래일 진입 시 오버나이트 지층(buckets)과 기억된 최저가(cycle_min_buy_price), 누적 시드는
      100% 그대로 보존하고 일일 결산 카운터(daily_*)와 게이트 버킷 인덱스만 새 거래일 기준으로 준비시킨다.
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

            # 2) 논리 거래일 롤오버 감지 시 일일 통계 카운터 초기화 (지층·기억저가·시드 100% 보존!)
            if logical_date != context.last_rollover_date:
                prev_logical = context.last_rollover_date
                context.last_rollover_date = logical_date
                await context.ledger.update_state(
                    TARGET_SYMBOL,
                    buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
                    portion_mode="COMP_08PCT",
                    threshold_mode="08_VREV_PROG",
                    is_compound=True,
                    last_evaluated_buy_bucket_id=-1,
                    daily_realized_net_usd=0.0,
                    daily_buy_events=0,
                    daily_sell_events=0,
                    daily_seed_protect_pass_cnt=0,
                    daily_leftover_saved_usd=0.0,
                    daily_reanchor_cnt=0,
                )
                logger.info(
                    "🌐 [신규 거래일 롤오버 완료] %s -> %s (SOXL 오버나이트 지층 · 기억저가 · 복리 시드 100%% 보존)",
                    prev_logical,
                    logical_date,
                )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("GC/롤오버 루프 대기: %s", exc)

        await asyncio.sleep(10.0)


# ---------------------------------------------------------------------------
# 메인 전술 오케스트레이션 루프 (main_sniper_loop : 04:00 ~ 16:59 EST 전용)
# ---------------------------------------------------------------------------
async def main_sniper_loop(context: SniperContext) -> None:
    """
    제1헌법 ~ 제6헌법 & 🎯 1 ~ 🎯 5 & Case 01 ~ 64:
    데이장(19:00 ~ 03:59 EST) 및 1주 휩소, 15:59 MOC, 가상 래칫 없이
    오직 프리장(04:00 호가안정화 / 04:07 개시)부터 정규장 마감(16:00 EST)까지만
    '8% 황금비율 정액 기반 정량제 × 자투리/수익금 복리 산입 × 0주 즉시 실물 1층 진입 ×
    2층 보유 시 10분 게이트 눌림(-1.0%)/돌파(+1.0%) 독립 실물 1층 재장전 ×
    기억된 최저가 기준 10분 고정 4단 와이드 시드보호 매수 게이트 × 1분 상시 실물 스나이퍼 매도'를 구동하는 메인 루프.
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

            # 장외 휴식 구간(17:00 ~ 03:59 EST · 데이장 완전 폐기)에는 매매 없이 대기
            if hm >= 1700 or hm < 400:
                context.fast_shield_mode = False
                await asyncio.sleep(3.0)
                continue

            # 1. 실시간 SOXL 현재가 및 토스 실보유 잔고 조회
            prices = await context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            holdings = await context.api.get_holdings_all()

            # 2. [04:00 ~ 04:06 EST] 프리장 개장 초기 호가 안정화 및 오버나이트 지층 동기화
            if is_premarket_stabilization_window(hm):
                await _call_plugin_adaptive(
                    execute_0400_premarket_stabilization_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
                context.fast_shield_mode = False

            # 3. [04:07 ~ 16:00 EST] 프리장 + 정규장 전 구간 연속 지층관리 스나이퍼 가동
            elif is_active_trading_window(hm):
                # 3-1) 로컬 지층 원장 ↔ 토스 실보유 잔고 무결성 화해 (Case 29, 58, 60, 61, 63)
                await _call_plugin_adaptive(
                    reconcile_stratum_with_broker_holdings_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

                # 3-2) ⚡ [1분 상시(실시간) 실물 스나이퍼 매도 게이트 & 실물 수익금 2층 리앵커링 선행 실행]
                sell_res = await _call_plugin_adaptive(
                    execute_1m_sniper_sell_and_reanchor_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
                did_sell_in_tick = bool(
                    isinstance(sell_res, dict)
                    and sell_res.get("did_sell_or_ratchet")
                )

                # 3-3) 🛡️ [0주 즉시 실물 1층 산입 · 2층 단독 시 눌림/돌파 독립 1층 재장전 · 4단 와이드 매수 게이트 실행]
                #      Case 44: 동일 틱에서 매도가 격발/체결된 경우 자전거래 방지를 위해 매수를 건너뜀!
                if not did_sell_in_tick:
                    await _call_plugin_adaptive(
                        execute_seed_protected_buy_gate_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

                # 3-4) 0.4초 고주파 스나이퍼 부스터 판별
                context.fast_shield_mode = _evaluate_fast_shield_mode(
                    context=context,
                    prices=prices,
                    now_est=now_est,
                )

            # 4. [16:01 ~ 16:59 EST] 정규장 마감 일일 결산 보고 및 보유 지층 오버나이트 보존 (MOC 없음!)
            elif is_daily_settlement_window(hm):
                await _call_plugin_adaptive(
                    execute_1600_daily_closing_settlement_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
                context.fast_shield_mode = False

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
# 엔트리포인트 : 폐기 파일 청소 ➔ 9대 모듈 검증 ➔ TDD 검증 ➔ AccountSeq 락온 ➔ 가동
# ---------------------------------------------------------------------------
async def async_main() -> None:
    _install_fast_kill_signal_handlers()
    _autoload_dotenv_files(override=False)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. 제3헌법: 영구 폐기된 데이장(daymarket_sim_plugin.py), 1주 휩소(whipsaw_plugin.py), SOXS 잔재 자동 삭제
    removed_files = await asyncio.to_thread(
        purge_deprecated_legacy_files, base_dir
    )
    if removed_files:
        logger.info(
            "🗑️ 기동 시 폐기 레거시 파일 자동 삭제 완료: %s",
            ", ".join(removed_files),
        )

    # 2. Case 11 & Case 62: 기동 직전 9대 필수 코어 모듈 py_compile 및 AST 심볼 임포트 무결성 검증
    ok, compile_errs = await asyncio.to_thread(
        verify_all_modules_compile, base_dir, True
    )
    if not ok:
        logger.error(
            "🚨 프리플라이트 컴파일/임포트 에러 감지:\n%s",
            "\n".join(compile_errs),
        )
        sys.exit(1)

    # 3. TDD 사전 무결성 검증 (8% 황금정량 산출 · 4단 와이드 활주로 · 눌림/돌파 독립 1층 재장전 · Case 59/60 검증)
    run_quant_engine_tdd()

    context = SniperContext()

    # 4. Case 64: 10분 시드보호 매수 게이트 하드 고정 및 8% 동적 복리 정량(COMP_08PCT · 08_VREV_PROG) 설정 락온
    await context.ledger.update_state(
        TARGET_SYMBOL,
        buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
        portion_mode="COMP_08PCT",
        threshold_mode="08_VREV_PROG",
        is_compound=True,
    )

    # 5. 제2헌법: 기동 시 /api/v1/accounts 1회 호출하여 종합매매(BROKERAGE) accountSeq 메모리 락온
    try:
        acc_seq = await context.api.ensure_account_seq()
        logger.info("🏛️ [제2헌법 AccountSeq 락온 완료] accountSeq=%s", acc_seq)
    except Exception as exc:
        logger.warning(
            "⚠️ 기동 시 AccountSeq 초기 락온 대기 (재시도 예정): %s", exc
        )

    # 6. 초기 SOXL 1분봉 시계열 및 실보유 잔고-지층 무결성 선행 동기화
    try:
        await context.refresh_symbol_candles_once(
            REAL_ALLOWED_SYMBOLS, total_count=300
        )
        holdings = await context.api.get_holdings_all()
        prices = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
        await reconcile_stratum_with_broker_holdings_plugin(
            context=context,
            holdings=holdings,
            prices=prices,
            now_est=datetime.now(EST_TZ),
        )
    except Exception as exc:
        logger.warning("⚠️ 초기 캔들/지층 잔고 선행 동기화 예외: %s", exc)

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
        "🚀 [SOXL 지층관리 스나이퍼 가동 완료] 04:07 ~ 16:00 EST · 전술 A (🛡️ 05_AF_WIDE_10_01) · 8%% 황금정량 · 10분 4단 와이드 매수게이트 · 1분 상시 매도"
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
