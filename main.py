# -*- coding: utf-8 -*-
"""
# MODIFIED: main.py
인피니트 스노우볼 (Infinite Snowball) — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
메인 오케스트레이터 및 비동기 이벤트 루프 컨트롤러 (SyntaxError 무결점 교정판)
- 전술 A 챔피언 (🛡️ 05_AF_WIDE_10_01) 100% 결속:
  1) 8%(12.5분할) 황금비율 정액 기반 정량제 & 자투리 절약금 · 세후 실현수익금 실시간 시드 풀 복리 산입 (COMP_08PCT 고정)
  2) 0주 상태 60초 타임쉴드 확인 후 실시간 현재가 조회 및 현재가 지정가(LIMIT) 실물 1층(REENTRY_1FL) 산입
  3) 2층(MERGED_UPPER) 단독 보유 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 실물 1층(REENTRY_1FL) 재장전
  4) 기억된 사이클 최저 매수가(cycle_min_buy_price) 잠금 기반 10분 고정 4단 와이드 활주로 시드보호 매수 게이트
     (WIDE_RUNWAY : 1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5% 신저가 도달 시에만 현재가 지정가 추매)
  5) 매수 후 60초 타임쉴드 & 1분 상시 순수 현재가 지정가 스나이퍼 매도(1층 +1.50% 익절 & 2층 리앵커링 / 2층 +1.00% 전량 졸업)
  6) 🚨 [Case 66 미결 매수 선행 정산] 04:07 ~ 16:00 EST 루프 진입 시 reconcile 호출 전에 sync_pending_buy_order_plugin(Step 3-0)을
     최우선 실행하여, 거래소 즉시 체결 시에도 텔레그램 체결 알림 · 자투리 절약금 누적 · 60초 타임쉴드 각인이 100% 실행되도록 보장!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 66 전수 락온
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from defense_plugin import (
    execute_0400_premarket_stabilization_plugin,
    execute_1600_daily_closing_settlement_plugin,
    execute_daily_1700_gc_and_backup_plugin,
    reconcile_stratum_with_broker_holdings_plugin,
)
from plugin_updater import (
    purge_deprecated_legacy_files,
    run_auto_update_loop,
    verify_all_modules_compile,
)
from quant_engine import (
    buckets_from_dicts,
    run_quant_engine_tdd,
    summarize_stratum_portfolio,
)
from quant_indicators_plugin import (
    compute_soxl_quant_metrics_1m,
    evaluate_buy_gate_bucket_status,
    is_active_trading_window,
    is_weekend_nyse_closed,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    execute_1m_sniper_sell_and_reanchor_plugin,
    sync_pending_sell_order_plugin,
    update_exit_snapshot_on_hold,
)
from sniper_plugin import (
    execute_seed_protected_buy_gate_plugin,
    sync_pending_buy_order_plugin,
)
from tg_ui_plugin import TelegramCommandController
from toss_api import (
    EST_TZ,
    KRW_PER_USD_DEFAULT,
    LOCKED_BUY_INTERVAL_MIN,
    SoxlStratumLedger,
    TARGET_SYMBOL,
    TossApiClient,
    safe_float,
    safe_int,
)

# ---------------------------------------------------------------------------
# 로깅 포맷 설정 (EST/KST 병기 지원 및 저소음 고신뢰 로그)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("toss_sniper.main")

# 메인 루프 주기 상수 (평시 1.5초 / 고주파 스나이퍼 부스터 0.4초)
NORMAL_LOOP_INTERVAL_SEC: float = 1.50
FAST_SHIELD_LOOP_INTERVAL_SEC: float = 0.40


# ---------------------------------------------------------------------------
# [핵심 1] Case 13 : 적응형 비동기 플러그인 호출기 (_call_plugin_adaptive)
# ---------------------------------------------------------------------------
async def _call_plugin_adaptive(
    func: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    """
    🚨 Case 13:
    플러그인 함수의 시그니처(인자 목록 및 **kwargs 수용 여부)를 동적으로 검사하여
    허용된 인자만 안전하게 주입하고 동기/비동기 함수를 모두 실행한다.
    """
    sig = inspect.signature(func)
    accepts_var_kw = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in sig.parameters.values()
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
# [핵심 2] 스나이퍼 애플리케이션 전역 컨텍스트 (SniperAppContext)
# ---------------------------------------------------------------------------
class SniperAppContext:
    """
    9대 코어 모듈이 공유하는 단일 런타임 상태 컨텍스트.
    - api: TossApiClient (제1헌법 GlobalThrottle & 제2헌법 AccountSeq 락온)
    - ledger: SoxlStratumLedger (SOXL 단일 롱 지층 원장 · buy_interval_min = 10 & COMP_08PCT 하드 고정)
    - tg: TelegramCommandController (텔레그램 관제 UI & 2대 옵션 전술 제어반)
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        self.base_dir: str = os.path.abspath(
            base_dir or os.path.dirname(os.path.abspath(__file__)) or "."
        )
        self.ledger_path: str = os.path.join(
            self.base_dir, "soxl_stratum_ledger.json"
        )

        self.api: TossApiClient = TossApiClient()
        self.ledger: SoxlStratumLedger = SoxlStratumLedger(
            filepath=self.ledger_path
        )

        # 비동기 동시성 제어 뮤텍스 및 웨이크업 이벤트
        self.in_memory_ordering_lock: asyncio.Lock = asyncio.Lock()
        self.dual_strike_mutex: asyncio.Lock = asyncio.Lock()
        self.wakeup_event: asyncio.Event = asyncio.Event()
        self.shutdown_event: asyncio.Event = asyncio.Event()

        # 실시간 시세 · 잔고 · 1분봉 지표 · 체결 스냅샷 · 인덱싱 그레이스 쉴드 & 60초 타임쉴드 캐시
        self.latest_prices: Dict[str, float] = {TARGET_SYMBOL: 0.0}
        self.latest_holdings: Dict[str, Dict[str, Any]] = {
            TARGET_SYMBOL: {
                "symbol": TARGET_SYMBOL,
                "qty": 0,
                "avg_price": 0.0,
                "last_price": 0.0,
                "purchase_amount": 0.0,
                "eval_amount": 0.0,
                "profit_loss": 0.0,
                "profit_rate": 0.0,
            }
        }
        self.holdings_fetch_ok: bool = False
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self.exit_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}

        # 중복 보류 알림 방지 스로틀 맵
        self._strike_hold_alert_ts: Dict[str, float] = {}
        self.last_0400_sync_date: str = ""
        self.fast_shield_mode: bool = False

        self.tg: TelegramCommandController = TelegramCommandController(
            context=self
        )

    async def send_slim_alert(self, title: str, body: str = "") -> bool:
        return await self.tg.send_slim_alert(title=title, body=body)

    async def send_strike_hold_alert_once(
        self,
        symbol: str,
        reason_key: str,
        message: str,
        cooldown_sec: float = 1800.0,
    ) -> None:
        key = f"{symbol}:{reason_key}"
        now_m = time.monotonic()
        last_m = self._strike_hold_alert_ts.get(key, 0.0)
        if (now_m - last_m) < cooldown_sec:
            return
        self._strike_hold_alert_ts[key] = now_m
        await self.send_slim_alert(
            title=f"⚠️ [{symbol} 지층 스나이퍼 안내]",
            body=message,
        )


# ---------------------------------------------------------------------------
# [핵심 3] 백그라운드 태스크 1 : 1분봉 시계열 및 10분 게이트 지표 레이더 (candle_radar_loop)
# ---------------------------------------------------------------------------
async def candle_radar_loop(context: SniperAppContext) -> None:
    """
    04:00 ~ 16:05 EST 운용 시간대에는 5.0초 주기로, 장외 시간대에는 20.0초 주기로
    SOXL 1분봉(300개) 및 최근 일봉(6개)을 조회하여 VWAP · 5MA 진폭 · 하이킨 아시 추세 ·
    10분 고정 매수 게이트 버킷 상태를 갱신한다.
    """
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        hm = now_est.hour * 100 + now_est.minute
        in_market = bool(
            (400 <= hm <= 1605) and not is_weekend_nyse_closed(now_est)
        )
        sleep_sec = 5.0 if in_market else 20.0

        try:
            candles_1m = await context.api.get_candles_1m(
                symbol=TARGET_SYMBOL, total_count=300
            )
            candles_1d = await context.api.get_candles_1d(
                symbol=TARGET_SYMBOL, count=10
            )
            live_p = safe_float(
                context.latest_prices.get(TARGET_SYMBOL),
                safe_float(
                    context.latest_holdings.get(TARGET_SYMBOL, {}).get(
                        "last_price"
                    ),
                    0.0,
                ),
            )
            metrics = compute_soxl_quant_metrics_1m(
                candles=candles_1m,
                live_price=live_p,
                now_est=now_est,
                daily_candles=candles_1d,
            )
            context.candle_metrics_cache[TARGET_SYMBOL] = metrics
            if live_p <= 0.0 and safe_float(metrics.get("live_price"), 0.0) > 0.0:
                context.latest_prices[TARGET_SYMBOL] = safe_float(
                    metrics.get("live_price"), 0.0
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("SOXL 캔들 레이더 루프 예외: %s", exc)

        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=sleep_sec
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 4] 백그라운드 태스크 2 : 04:07 ~ 16:00 EST 메인 지층 스나이퍼 루프 (main_sniper_loop)
# ---------------------------------------------------------------------------
def _evaluate_fast_shield_mode(
    context: SniperAppContext,
    now_est: datetime,
    live_p: float,
) -> bool:
    """
    평시 1.5초 폴링을 0.4초 고주파 스나이퍼 부스터로 전환할지 판정한다:
    1) 미체결 매수 주문(buy_order_id) 또는 미체결 매도 주문(sell_order_id) 진행 중일 때
    2) 보유 수량 0주(held_shares == 0)여서 0주 현재가 지정가 1층 진입을 대기 중일 때
    3) 04:07 EST 기점 10분 고정 매수 게이트 정각 분봉이거나 현재 버킷 미평가 상태일 때
    4) 1층 익절 목표가(+1.50%) 또는 2층 졸업 목표가(+1.00%)에 0.30% 이내로 근접했을 때
    5) 2층 단독 보유 중 눌림목(-1.00%) 또는 상승돌파(+1.00%) 재장전 타점에 근접했을 때
    """
    hm = now_est.hour * 100 + now_est.minute
    if not is_active_trading_window(hm):
        return False

    st = context.ledger.get_state(TARGET_SYMBOL)
    if not bool(st.get("is_active", True)):
        return False

    if str(st.get("buy_order_id") or "").strip() or str(
        st.get("sell_order_id") or ""
    ).strip():
        return True

    summary = summarize_stratum_portfolio(st, live_price=live_p)
    held_sh = safe_int(summary.get("held_shares"), 0)

    # 0주 상태이면 실물 1층 산입을 위해 0.4초 고주파 감시
    if held_sh <= 0:
        return True

    is_gate_open, _, _, b_info = evaluate_buy_gate_bucket_status(
        now_est=now_est,
        buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
        last_evaluated_bucket_id=safe_int(
            st.get("last_evaluated_buy_bucket_id"), -1
        ),
        last_evaluated_date=str(st.get("last_evaluated_date") or ""),
    )
    if is_gate_open or bool(b_info.get("is_exact_boundary_min")):
        return True

    l1_target = safe_float(summary.get("l1_target_price"), 0.0)
    l2_target = safe_float(summary.get("l2_target_price"), 0.0)
    if live_p > 0.0 and l1_target > 0.0 and live_p >= (l1_target * 0.997):
        return True
    if live_p > 0.0 and l2_target > 0.0 and live_p >= (l2_target * 0.997):
        return True

    if bool(summary.get("is_l2_only_anti_bloat")):
        pull_p = safe_float(summary.get("reentry_pullback_price"), 0.0)
        brk_p = safe_float(summary.get("reentry_breakout_price"), 0.0)
        if live_p > 0.0 and (
            (pull_p > 0.0 and live_p <= pull_p * 1.003)
            or (brk_p > 0.0 and live_p >= brk_p * 0.997)
        ):
            return True

    return False


async def main_sniper_loop(context: SniperAppContext) -> None:
    """
    04:00 ~ 16:59 EST 일일 스케줄러 및 메인 지층관리 스나이퍼 루프:
    - 04:00 ~ 04:06 EST: 프리장 개장 초기 호가 안정화 및 오버나이트 지층 동기화
    - 04:07 ~ 16:00 EST:
      Step 3-0) 🚨 [Case 66] 미결 매수 주문(buy_order_id) 최우선 체결 확인 및 커밋
      Step 3-1) 로컬 지층 원장 ↔ 토스 실잔고 화해 (미결 봇 주문 진행 중 가로채기 차단 가드 내장)
      Step 3-2) 매수 후 60초 타임쉴드 검문 & 1분 상시 순수 현재가 지정가 스나이퍼 매도 (체결 시 자전거래 방지 스킵)
      Step 3-3) 매도 후 60초 타임쉴드 검문 & 0주 진입 / 2층 단독 눌림·돌파 재장전 / 10분 4단 와이드 신저가 추매
    - 16:00 ~ 16:59 EST: 정규장 마감 일일 결산 보고 및 보유 지층 오버나이트 보존
    """
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        hm = now_est.hour * 100 + now_est.minute

        try:
            if is_weekend_nyse_closed(now_est):
                context.fast_shield_mode = False
                try:
                    await asyncio.wait_for(
                        context.shutdown_event.wait(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            # 1) SOXL 실시간 현재가 및 보유 잔고 조회 (MODIFIED: 함수명 오타 완전 교정)
            price_map = await context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            live_p = safe_float(price_map.get(TARGET_SYMBOL), 0.0)
            if live_p > 0.0:
                context.latest_prices[TARGET_SYMBOL] = live_p

            holdings_map, fetch_ok = (
                await context.api.get_holdings_map_with_status()
            )
            context.holdings_fetch_ok = fetch_ok
            if fetch_ok and isinstance(holdings_map, dict):
                context.latest_holdings = holdings_map
                if live_p <= 0.0:
                    live_p = safe_float(
                        holdings_map.get(TARGET_SYMBOL, {}).get("last_price"),
                        0.0,
                    )
                    if live_p > 0.0:
                        context.latest_prices[TARGET_SYMBOL] = live_p

            # 캔들 캐시에 실시간 틱 합성
            if (
                live_p > 0.0
                and TARGET_SYMBOL in context.candle_metrics_cache
                and isinstance(
                    context.candle_metrics_cache[TARGET_SYMBOL], dict
                )
            ):
                context.candle_metrics_cache[TARGET_SYMBOL] = (
                    refresh_metrics_with_live_tick(
                        context.candle_metrics_cache[TARGET_SYMBOL],
                        live_p,
                        now_est=now_est,
                    )
                )

            prices = dict(context.latest_prices)
            holdings = dict(context.latest_holdings)

            # 2) 04:00 ~ 04:06 EST 프리장 오프닝 안정화 및 전일 미체결 청소
            if 400 <= hm <= 406:
                await _call_plugin_adaptive(
                    execute_0400_premarket_stabilization_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

            # 3) 04:07 ~ 16:00 EST (프리장 + 정규장 전 구간 연속 가동)
            elif is_active_trading_window(hm):
                # 🚨 3-0) [Case 66 핵심 방어 : 미결 매수 주문 최우선 선행 정산]
                # 직전 루프에서 발주한 매수 주문(buy_order_id)이 존재하면 reconcile 보다 먼저
                # sync_pending_buy_order_plugin 을 실행하여 정식 매수 체결 알림, 자투리 절약금 누적,
                # 매수 후 60초 타임쉴드 각인이 가로채기 없이 100% 완수되도록 보장한다!
                st_pre = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_pre.get("buy_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_buy_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

                # 3-1) 로컬 지층 원장 ↔ 토스 실보유 잔고 무결성 화해 (Case 66 가드 내장)
                await _call_plugin_adaptive(
                    reconcile_stratum_with_broker_holdings_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

                # 3-2) 매수 후 60초 타임쉴드 검문 & 1분 상시 순수 현재가 지정가 스나이퍼 매도 (Case 44, 65)
                sell_res = await _call_plugin_adaptive(
                    execute_1m_sniper_sell_and_reanchor_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
                did_sell = bool(
                    isinstance(sell_res, dict)
                    and sell_res.get("did_sell_or_ratchet")
                )

                # 🚨 Case 44: 동일 틱에서 매도가 발생(또는 진행 중)한 경우 추가 매수를 즉시 스킵하여 자전거래 차단!
                if not did_sell:
                    # 3-3) 매도 후 60초 타임쉴드 검문 & 0주 진입 / 2층 단독 눌림·돌파 재장전 / 10분 4단 와이드 신저가 추매
                    await _call_plugin_adaptive(
                        execute_seed_protected_buy_gate_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

            # 4) 16:00 ~ 16:59 EST 정규장 마감 일일 결산 보고 (보유 지층 오버나이트 보존)
            elif 1600 <= hm <= 1659:
                await _call_plugin_adaptive(
                    execute_1600_daily_closing_settlement_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

            # 5) 장외 대기 시간에도 미체결 주문이 남아 있으면 상태 동기화 수행
            else:
                st_idle = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_idle.get("sell_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_sell_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )
                if str(st_idle.get("buy_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_buy_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

            context.fast_shield_mode = _evaluate_fast_shield_mode(
                context=context,
                now_est=now_est,
                live_p=safe_float(
                    context.latest_prices.get(TARGET_SYMBOL), 0.0
                ),
            )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("메인 지층 스나이퍼 루프 예외: %s", exc, exc_info=True)

        loop_wait = (
            FAST_SHIELD_LOOP_INTERVAL_SEC
            if context.fast_shield_mode
            else NORMAL_LOOP_INTERVAL_SEC
        )
        context.wakeup_event.clear()
        try:
            await asyncio.wait_for(
                context.wakeup_event.wait(), timeout=loop_wait
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 5] 백그라운드 태스크 3 : 17:00 EST 일일 GC 및 논리 거래일 롤오버 루프
# ---------------------------------------------------------------------------
async def daily_gc_and_rollover_loop(context: SniperAppContext) -> None:
    """
    매일 17:00 EST 에 가비지 컬렉션(GC)을 수행하고 새 거래일 진입 시 일일 카운터를 롤오버한다.
    """
    last_gc_date = ""
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        today_str = now_est.strftime("%Y-%m-%d")
        hm = now_est.hour * 100 + now_est.minute

        try:
            await context.ledger.rollover_daily_counters_if_needed(today_str)
            if hm >= 1700 and last_gc_date != today_str:
                last_gc_date = today_str
                await execute_daily_1700_gc_and_backup_plugin(
                    context=context, now_est=now_est
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("일일 GC/롤오버 루프 예외: %s", exc)

        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=30.0
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 6] 시스템 부트스트랩 · 프리플라이트 TDD · 시그널 핸들러 (Case 11, 16)
# ---------------------------------------------------------------------------
async def async_main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__)) or "."

    # 1) 구버전 폐기 파일(daymarket_sim_plugin.py, whipsaw_plugin.py, state_SOXS.json) 자동 청소
    purged = purge_deprecated_legacy_files(base_dir)
    if purged:
        logger.info("🧹 [구버전 폐기 파일 청소 완료] 삭제 목록: %s", purged)

    # 2) 9대 코어 모듈 py_compile + AST 심볼 임포트 정적 검증 & QuantEngine TDD 실행
    ok_compile, err_msg = verify_all_modules_compile(base_dir)
    if not ok_compile:
        logger.critical(
            "🚨 [프리플라이트 컴파일/심볼 검증 실패] 기동 중단: %s", err_msg
        )
        sys.exit(1)

    run_quant_engine_tdd()
    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 전술 A (8%% 황금정량 · 4단 와이드 활주로 · 눌림/돌파 독립 1층 재장전 · Case 59/60/65/66) 검증 완료"
    )

    context = SniperAppContext(base_dir=base_dir)

    # 3) SIGTERM / SIGINT 0.1초 고속 종료 시그널 결속 (Case 11)
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        logger.info("🛑 종료 시그널(SIGTERM/SIGINT) 수신 -> 고속 안전 종료 개시...")
        context.shutdown_event.set()
        context.wakeup_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    # 4) 제2헌법: 토스증권 AccountSeq 락온 및 초기 실잔고 동기화
    try:
        acc_seq = await context.api.ensure_account_seq()
        logger.info("🏛️ [제2헌법 AccountSeq 락온 완료] accountSeq=%d", acc_seq)
    except Exception as exc:
        logger.warning(
            "⚠️ 초기 AccountSeq 조회 지연 (루프 내 자동 재시도): %s", exc
        )

    try:
        price_map = await context.api.get_current_prices_batch(
            (TARGET_SYMBOL,)
        )
        if safe_float(price_map.get(TARGET_SYMBOL), 0.0) > 0.0:
            context.latest_prices[TARGET_SYMBOL] = safe_float(
                price_map.get(TARGET_SYMBOL), 0.0
            )
        holdings_map, fetch_ok = (
            await context.api.get_holdings_map_with_status()
        )
        context.holdings_fetch_ok = fetch_ok
        if fetch_ok:
            context.latest_holdings = holdings_map
            await reconcile_stratum_with_broker_holdings_plugin(
                context=context,
                holdings=holdings_map,
                prices=context.latest_prices,
                now_est=datetime.now(EST_TZ),
                force_sync=True,
            )
    except Exception as exc:
        logger.warning("초기 잔고/시세 동기화 폴백: %s", exc)

    # 5) Case 16: 기동 완료 후 텔레그램 생존 신호 타전 ("✅ 서버가 정상 작동 중입니다.")
    await context.tg.send_startup_alive_banner()

    # 6) 5대 백그라운드 비동기 워커 가동
    tasks = [
        asyncio.create_task(candle_radar_loop(context), name="candle_radar"),
        asyncio.create_task(main_sniper_loop(context), name="main_sniper"),
        asyncio.create_task(
            context.tg.run_polling_loop(), name="telegram_polling"
        ),
        asyncio.create_task(
            daily_gc_and_rollover_loop(context), name="daily_gc"
        ),
        asyncio.create_task(
            run_auto_update_loop(context, interval_sec=3600.0),
            name="auto_updater"
        ),
    ]

    logger.info(
        "🚀 [SOXL 지층관리 스나이퍼 가동 완료] 04:07 ~ 16:00 EST · 전술 A (🛡️ 05_AF_WIDE_10_01) · 8%% 황금정량 · 10분 4단 와이드 매수게이트 · 1분 상시 매도 · 양방향 60초 타임쉴드 & 선행 체결정산(Case 65/66)"
    )

    try:
        await context.shutdown_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await context.api.close()
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
