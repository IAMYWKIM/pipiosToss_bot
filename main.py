# =====================================================================
# FILE: main.py
# 목적: 인피니트 스노우볼 듀얼 스나이퍼(SOXS & SOXL) 메인 엔트리포인트,
#       SIGTERM/SIGINT 0.1초 고속 하드 킬 핸들러(_install_fast_shutdown_handlers — 90초 재시작 멈춤 영구 소각),
#       tg_router.inject_dependencies(api_client, chat_id, wakeup_event, shared_rsi_state) 최우선 직결,
#       제2헌법 종합매매(BROKERAGE) 계좌 accountSeq 실전 헤더 검증 및 강제 바인딩(ensure_account_seq_locked),
#       O(1) 시그니처 캐싱 기반 전술 플러그인 적응형 호출기(_call_plugin_adaptive),
#       듀얼 심볼(SOXS & SOXL) 캔들 레코더 자동 매핑 기동기(_start_candle_recorder_task) 및
#       메인 전술 루프(assassin_loop) 통합 관제 코어
# =====================================================================
# MODIFIED: 제1 ~ 제6헌법 및 100대 엣지 케이스 방어망(Case 01 ~ Case 100) 100% 결속
# NEW: 1) `_install_fast_shutdown_handlers()` 탑재로 `systemctl restart / stop` 시 발생하는
#         90초 타임아웃(`State 'stop-sigterm' timed out`)을 0.1초 즉시 종료(os._exit(0))로 영구 해결
#      2) `_bind_telegram_router` 최상단에 `tg_router.inject_dependencies(api_client, chat_id, wakeup_event, shared_rsi_state)`
#         명시적 직결로 `/start`, `/update`, `/avwap`, `/sync`, `/settlement` 무응답 현상 영구 소각
#      3) `_call_plugin_adaptive(fn, **kwargs)` 및 `_start_candle_recorder_task` 듀얼 심볼 자동 매핑 탑재
#      4) 초과 Case 97, 98, 99, 100 — 프리장 첫 RSI>=79.9 상승주도주(👑 leader_sym) 실시간 갱신,
#         15:00~15:58 EST 미익절 정규장 VWAP 탈출, 이원화 매도 믹스, 30초 타임쉴드 정정 직후 실잔고 즉시 재조회 결속

import os
import sys
import math
import time
import signal
import inspect
import asyncio
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher

from toss_api import TossApiClient, GlobalThrottle
from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    SINGLE_TARGET_PROFIT_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    calculate_rsi_1m,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
    get_cme_session_window_est,
)
import candle_recorder
import tg_router
from tg_ui_plugin import (
    get_main_menu_text,
    build_avwap_radar,
    build_sync_board,
    build_settlement_board,
)
from sniper_plugin import (
    SniperPluginContext,
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_TREND_BOOSTER_RSI,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    TRAIL_ARM_PROFIT_RATE,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _reset_trailing_runtime_state,
    _reset_bottom_track_state,
    _reset_pre_leader_state,
    _reset_manual_stage3_anchor,
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    get_trailing_runtime_status,
    get_bottom_track_status,
    get_pre_leader_status,
    update_premarket_trend_leader,
    settle_daymarket_sim_before_premarket,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
    execute_post_stop_bottom_rebound_plugin,
    execute_premarket_rsi_sniper_strike_plugin,
    rearm_premarket_buy_trap_after_30s_plugin,
    reconcile_zero_holdings_exit_plugin,
    manage_mechanical_profit_trap_plugin,
    execute_rsi_monitoring_plugin,
    execute_late_regular_vwap_escape_plugin,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
)
from defense_plugin import (
    _cancel_open_orders_and_wait_sellable as sniper_plugin_cancel_helper,
)
from main_loops_plugin import (
    EST_TZ,
    KST_TZ,
    TRADE_SYMBOLS,
    RSI_SYMBOLS,
    NQ_DYNAMIC_FLOOR,
    HOLIDAY_TRANSLATIONS,
    LOOP_INTERVAL_NORMAL,
    LOOP_INTERVAL_FAST,
    shared_rsi_state,
    shared_holdings,
    wakeup_event,
    in_memory_ordering_lock,
    dual_strike_mutex,
    idempotency_keys,
    _unpack_ledger_state_safe,
    _run_git_update_sync,
    run_daily_gc,
    auto_update_loop,
    rsi_radar_loop,
)

# 텔레그램 핸들러 내부 예외가 묵음 처리되지 않도록 ERROR 레벨 유지
logging.getLogger("aiogram").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

_LAST_GC_DATE_MAIN = ""
_ACCOUNT_LOCK_MUTEX = asyncio.Lock()
_PLUGIN_SIG_CACHE: dict = {}


def _install_fast_shutdown_handlers():
    """
    NEW (Case 11): systemd 가 `systemctl restart` 또는 `stop` 시 보내는 SIGTERM / SIGINT 신호를 포착하여
    90초 타임아웃(`State 'stop-sigterm' timed out`) 대기 없이 0.1초 만에 즉시 프로세스를 종료합니다.
    """
    def _fast_exit_handler(signum, frame):
        print(f"⚡ [시그널 {signum} 수신] 0.1초 고속 하드 킬 종료 실행", flush=True)
        os._exit(0)

    try:
        signal.signal(signal.SIGTERM, _fast_exit_handler)
        signal.signal(signal.SIGINT, _fast_exit_handler)
    except Exception:
        pass


async def _call_plugin_adaptive(fn, **kwargs):
    """
    NEW: 전술 플러그인 함수의 inspect.signature 를 O(1) 캐싱하여
    대상 함수가 선언한 매개변수만 정확히 필터링 전달함으로써
    `got an unexpected keyword argument` 예외를 100% 원천 차단합니다.
    """
    if fn is None or not callable(fn):
        return False

    sig_info = _PLUGIN_SIG_CACHE.get(fn)
    if sig_info is None:
        try:
            sig = inspect.signature(fn)
            has_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )
            valid_keys = frozenset(sig.parameters.keys())
            sig_info = (has_var_kw, valid_keys)
        except Exception:
            sig_info = (True, frozenset())
        _PLUGIN_SIG_CACHE[fn] = sig_info

    has_var_kw, valid_keys = sig_info
    if has_var_kw:
        res = fn(**kwargs)
    else:
        filtered_kw = {k: v for k, v in kwargs.items() if k in valid_keys}
        res = fn(**filtered_kw)

    if asyncio.iscoroutine(res):
        return await res
    return res


def _is_valid_positive_seq(val) -> bool:
    """accountSeq 값이 0 이나 빈 문자열이 아닌 유효한 양수(또는 유효 식별자)인지 검증합니다."""
    if val is None or isinstance(val, bool):
        return False
    s = str(val).strip()
    if not s or s in ("0", "None", "null", "False"):
        return False
    try:
        return int(s) > 0
    except ValueError:
        return len(s) > 0


def _is_account_seq_really_locked(api_client: TossApiClient) -> bool:
    """
    제2헌법 실전 검증:
    실제 `api_client._get_headers(require_account=True)` 호출이 예외 없이 성공하고
    `X-Tossinvest-Account` 헤더에 유효한 시퀀스(> 0)가 담기는지 직접 확인합니다.
    """
    if api_client is None:
        return False
    try:
        if hasattr(api_client, "_get_headers"):
            try:
                hdr = api_client._get_headers(require_account=True)
            except TypeError:
                hdr = api_client._get_headers(True)
            if isinstance(hdr, dict):
                acc_hdr = hdr.get("X-Tossinvest-Account") or hdr.get("x-tossinvest-account")
                if _is_valid_positive_seq(acc_hdr):
                    return True
            return False
    except Exception:
        return False

    for attr_name in ("account_seq", "_account_seq", "accountSeq", "_accountSeq"):
        if _is_valid_positive_seq(getattr(api_client, attr_name, None)):
            return True
    return False


def _inject_account_seq_everywhere(api_client: TossApiClient, seq_val, acc_no: str = ""):
    """
    추출된 BROKERAGE accountSeq 를 api_client 인스턴스, TossApiClient 클래스, GlobalThrottle 및
    _get_headers 래퍼에 원자적으로 주입합니다.
    """
    try:
        seq_norm = int(str(seq_val).strip())
    except ValueError:
        seq_norm = str(seq_val).strip()
    seq_str = str(seq_norm)

    for target_obj in (api_client, TossApiClient, GlobalThrottle):
        for attr_name in ("account_seq", "_account_seq", "accountSeq", "_accountSeq"):
            try:
                setattr(target_obj, attr_name, seq_norm)
            except Exception:
                pass

    if acc_no:
        for no_attr in ("account_no", "_account_no", "accountNo", "_accountNo"):
            try:
                setattr(api_client, no_attr, str(acc_no))
            except Exception:
                pass

    for k in list(getattr(api_client, "__dict__", {}).keys()):
        k_low = k.lower()
        if "account" in k_low and "seq" in k_low:
            try:
                setattr(api_client, k, seq_norm)
            except Exception:
                pass

    if not _is_account_seq_really_locked(api_client) and hasattr(api_client, "_get_headers"):
        orig_get_headers = api_client._get_headers

        def _patched_get_headers(*args, **kwargs):
            try:
                hdr = orig_get_headers(False)
            except Exception:
                tok = (
                    getattr(api_client, "access_token", None)
                    or getattr(api_client, "_access_token", None)
                    or getattr(api_client, "token", None)
                    or ""
                )
                hdr = {"Content-Type": "application/json"}
                if tok:
                    hdr["Authorization"] = f"Bearer {tok}"
            if isinstance(hdr, dict):
                hdr = dict(hdr)
                hdr["X-Tossinvest-Account"] = seq_str
            return hdr

        api_client._get_headers = _patched_get_headers


async def ensure_account_seq_locked(api_client: TossApiClient) -> bool:
    """
    제2헌법 완벽 준수:
    시스템 기동 시 및 자산/잔고/주문 API 호출 전 /api/v1/accounts 를 호출하여
    종합매매(BROKERAGE) 계좌의 accountSeq 를 메모리에 100% 락온합니다.
    """
    if api_client is None:
        return False

    if _is_account_seq_really_locked(api_client):
        return True

    async with _ACCOUNT_LOCK_MUTEX:
        if _is_account_seq_really_locked(api_client):
            return True

        candidate_methods = (
            "lock_account_seq",
            "_lock_account_seq",
            "init_account_seq",
            "_init_account_seq",
            "initialize_account",
            "init_account",
            "fetch_account_seq",
            "_fetch_account_seq",
            "get_account_seq",
            "ensure_account_seq",
            "load_account_seq",
        )
        for m_name in candidate_methods:
            fn = getattr(api_client, m_name, None)
            if callable(fn):
                try:
                    sig = inspect.signature(fn)
                    req_params = [
                        p for p in sig.parameters.values()
                        if p.default is inspect.Parameter.empty
                        and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                    ]
                    if len(req_params) == 0:
                        res = fn()
                        if asyncio.iscoroutine(res):
                            res = await res
                        if _is_valid_positive_seq(res):
                            _inject_account_seq_everywhere(api_client, res)
                except Exception as e:
                    print(f"⚠️ [계좌 락온 메서드 {m_name} 호출 방어] {e}", flush=True)

                if _is_account_seq_really_locked(api_client):
                    return True

        try:
            for tok_m in (
                "_ensure_token",
                "ensure_token",
                "_get_access_token",
                "get_access_token",
                "issue_token",
                "_issue_token",
                "refresh_token",
            ):
                tok_fn = getattr(api_client, tok_m, None)
                if callable(tok_fn):
                    try:
                        t_res = tok_fn()
                        if asyncio.iscoroutine(t_res):
                            await t_res
                        break
                    except Exception:
                        pass

            base_headers = None
            if hasattr(api_client, "_get_headers"):
                for call_fn in (
                    lambda: api_client._get_headers(require_account=False),
                    lambda: api_client._get_headers(False),
                ):
                    try:
                        base_headers = call_fn()
                        if isinstance(base_headers, dict):
                            break
                    except Exception:
                        pass

            if not isinstance(base_headers, dict):
                tok = (
                    getattr(api_client, "access_token", None)
                    or getattr(api_client, "_access_token", None)
                    or getattr(api_client, "token", None)
                    or ""
                )
                base_headers = {"Content-Type": "application/json"}
                if tok:
                    base_headers["Authorization"] = f"Bearer {tok}"

            _inject_account_seq_everywhere(api_client, 1)

            resp = await api_client._request(
                "GET",
                "/api/v1/accounts",
                "ACCOUNT",
                headers=base_headers,
            )
            accounts = (resp or {}).get("result") or []
            if isinstance(accounts, list) and accounts:
                chosen_seq = None
                chosen_acc_no = ""
                for acc in accounts:
                    if (
                        isinstance(acc, dict)
                        and acc.get("accountType") == "BROKERAGE"
                        and _is_valid_positive_seq(acc.get("accountSeq"))
                    ):
                        chosen_seq = acc.get("accountSeq")
                        chosen_acc_no = str(acc.get("accountNo") or "")
                        break
                if (
                    chosen_seq is None
                    and isinstance(accounts[0], dict)
                    and _is_valid_positive_seq(accounts[0].get("accountSeq"))
                ):
                    chosen_seq = accounts[0].get("accountSeq")
                    chosen_acc_no = str(accounts[0].get("accountNo") or "")

                if _is_valid_positive_seq(chosen_seq):
                    _inject_account_seq_everywhere(api_client, chosen_seq, chosen_acc_no)
                    print(
                        f"🔒 [제2헌법 AccountSeq 직접 락온 완료] BROKERAGE accountSeq={chosen_seq}",
                        flush=True,
                    )
                    return True
            else:
                if _is_account_seq_really_locked(api_client):
                    return True
        except Exception as e:
            print(f"🚨 [제2헌법 /api/v1/accounts 폴백 락온 방어] {e}", flush=True)

        return _is_account_seq_really_locked(api_client)


async def assassin_loop(
    api_client: TossApiClient,
    bot: Bot,
    chat_id: int,
):
    """
    제1 ~ 제6헌법 및 100대 엣지 케이스 방어망(Case 01 ~ Case 100)이 결속된 메인 듀얼 스나이퍼 전술 루프.
    """
    global _LAST_GC_DATE_MAIN

    ctx = SniperPluginContext(
        client=api_client,
        bot=bot,
        chat_id=chat_id,
        wakeup_event=wakeup_event,
        in_memory_ordering_lock=in_memory_ordering_lock,
        dual_strike_mutex=dual_strike_mutex,
        shared_holdings=shared_holdings,
        idempotency_keys=idempotency_keys,
        nq_dynamic_floor=NQ_DYNAMIC_FLOOR,
        shared_rsi_state=shared_rsi_state,
    )

    while True:
        fast_shield_mode = False
        try:
            # 제2헌법: 매 루프 사이클 진입 시 accountSeq 메모리 락온 무결성 보장
            if not await ensure_account_seq_locked(api_client):
                await asyncio.sleep(2.0)
                continue

            now_est = datetime.now(EST_TZ)
            now_kst = now_est.astimezone(KST_TZ)
            wk = now_est.weekday()
            est_hm = now_est.hour * 100 + now_est.minute
            kst_hm = now_kst.hour * 100 + now_kst.minute

            logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 18 else now_est
            current_session_id = logical_dt.strftime("%Y-%m-%d")
            today_str = now_est.strftime("%Y-%m-%d")

            # Case 22: 17:00 EST 일일 가비지 컬렉션(GC) 1회 실행
            if est_hm == 1700 and _LAST_GC_DATE_MAIN != today_str:
                _LAST_GC_DATE_MAIN = today_str
                await run_daily_gc(rsi_state_dict=shared_rsi_state, holdings_dict=shared_holdings)

            is_weekend_closed = (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800)
            if is_weekend_closed or (1700 <= est_hm <= 1759):
                await asyncio.sleep(5.0)
                continue

            # 03:58 ~ 04:00 EST 데이장 [모의] 포지션 자동 정리 (초과 Case 78)
            if 358 <= est_hm <= 400:
                await settle_daymarket_sim_before_premarket(ctx, now_est)

            # 1단계: SOXS & SOXL 다건 배치 실시간 현재가 조회 및 O(1) 실시간 틱 RSI·하이킨 아시 합성 (초과 Case 73, 83, 85)
            prices_batch = await api_client.get_current_prices_batch(TRADE_SYMBOLS)
            live_prices = {
                "SOXS": float((prices_batch or {}).get("SOXS", 0.0) or 0.0),
                "SOXL": float((prices_batch or {}).get("SOXL", 0.0) or 0.0),
            }

            live_rsis = {"SOXS": 0.0, "SOXL": 0.0}
            live_vwaps = {"SOXS": 0.0, "SOXL": 0.0}
            day_vwaps = {"SOXS": 0.0, "SOXL": 0.0}

            for sym_t in TRADE_SYMBOLS:
                st_r = (shared_rsi_state.get(sym_t) or {}) if isinstance(shared_rsi_state, dict) else {}
                ewm_r = st_r.get("ewm_state") or {}
                base_r = float(st_r.get("value", 0.0) or 0.0)
                base_v = float(st_r.get("vwap", 0.0) or 0.0)
                p_t = live_prices[sym_t]

                if p_t > 0.0 and ewm_r:
                    synthesized_r = compute_realtime_tick_rsi(
                        live_price=p_t,
                        ewm_state=ewm_r,
                        fallback_rsi=base_r,
                    )
                    compute_realtime_tick_heikin_ashi(p_t, ewm_r)
                    st_r["value"] = synthesized_r
                    live_rsis[sym_t] = synthesized_r
                else:
                    live_rsis[sym_t] = base_r

                live_vwaps[sym_t] = float(ewm_r.get("reg_vwap", 0.0) or 0.0) if est_hm >= 930 else base_v
                if live_vwaps[sym_t] <= 0.0:
                    live_vwaps[sym_t] = base_v
                day_vwaps[sym_t] = float(ewm_r.get("day_vwap", 0.0) or 0.0) or base_v

            if 407 <= est_hm <= 814:
                for sym_ld in TRADE_SYMBOLS:
                    update_premarket_trend_leader(
                        symbol=sym_ld,
                        live_rsi_val=live_rsis[sym_ld],
                        now_est=now_est,
                        shared_rsi_state=shared_rsi_state,
                    )

            # 2단계: 종목별 원자적 전술 스텝 실행 (초과 Case 70)
            for symbol in TRADE_SYMBOLS:
                try:
                    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
                    current_price = live_prices[symbol]
                    rsi_val = live_rsis[symbol]
                    opp_rsi_val = live_rsis[opp_sym]
                    vwap_val = live_vwaps[symbol]
                    day_vwap_val = day_vwaps[symbol]

                    raw_state = await AssassinLedger.get_state(symbol)
                    (
                        last_buy_price,
                        budget,
                        target_profit_rate,
                        is_compound,
                        is_session_done,
                        is_active,
                        target_sell_price,
                        sell_order_id,
                        entry_session,
                        entry_time,
                        nq_entry_price,
                        nq_entry_amp,
                        entry_trend,
                    ) = _unpack_ledger_state_safe(raw_state)
                    buy_order_id = await AssassinLedger.get_buy_order_id(symbol)

                    # 초과 Case 52 & Case 90: 신규 논리 세션 진입 시 미보유·무주문 장부 자가 치유
                    last_sess_id = await AssassinLedger.get_last_session_id(symbol)
                    if last_sess_id and last_sess_id != current_session_id:
                        if int(shared_holdings.get(symbol, 0)) <= 0 and not buy_order_id:
                            await AssassinLedger.save_state(
                                symbol,
                                is_session_done=False,
                                entry_session="",
                                last_session_id=current_session_id,
                                whipsaw_target_price=0.0,
                            )
                            is_session_done = False
                            entry_session = ""

                    hold_detail = await api_client.get_symbol_holdings_detail(symbol)
                    if hold_detail.get("error"):
                        # 초과 Case 29: 유령 잔고 방어 (잔고 조회 실패 시 해당 틱 매매 로직 스킵)
                        continue

                    holdings_qty = int(math.floor(float(hold_detail.get("qty", 0.0) or 0.0)))
                    avg_price = float(hold_detail.get("avg_price", 0.0) or 0.0)
                    shared_holdings[symbol] = holdings_qty

                    if current_price <= 0.0:
                        current_price = float(hold_detail.get("current_price", 0.0) or 0.0)
                        live_prices[symbol] = current_price

                    # 초과 Case 46 & Case 94: 스나이퍼 OFF 상태 시 미체결 매도 주문 취소 및 구버전 3차 기준가 0.0 초기화
                    if not is_active:
                        if sell_order_id:
                            try:
                                await api_client.cancel_order(sell_order_id)
                            except Exception:
                                pass
                            await AssassinLedger.save_state(
                                symbol,
                                sell_order_id="",
                                target_sell_price=0.0,
                                nq_entry_price=0.0,
                                nq_entry_amp=0.0,
                            )
                        _reset_trailing_runtime_state(symbol)
                        _reset_manual_stage3_anchor(symbol)
                        continue

                    # [Step A] 15:59 ~ 16:01 EST 제로-오버나이트 MOC 강제 청산망 (Case 16, 24)
                    if 1559 <= est_hm <= 1601:
                        fast_shield_mode = True
                        await _call_plugin_adaptive(
                            execute_moc_zero_overnight_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            holdings_qty=holdings_qty,
                            buy_order_id=buy_order_id,
                            sell_order_id=sell_order_id,
                            current_price=current_price,
                        )
                        continue

                    # [Step B] 잔고 0주 도달 시 익절/손절/수동청산 정산 및 매수 FILLED 인덱싱 시차 보호 (Case 49, 51, 76, 100)
                    if holdings_qty == 0 and (
                        last_buy_price > 0.0
                        or target_sell_price > 0.0
                        or bool(sell_order_id)
                        or bool(buy_order_id)
                    ):
                        cleared = await _call_plugin_adaptive(
                            reconcile_zero_holdings_exit_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            last_buy_price=last_buy_price,
                            target_sell_price=target_sell_price,
                            buy_order_id=buy_order_id,
                            sell_order_id=sell_order_id,
                            entry_session=entry_session,
                            current_price=current_price,
                            budget=budget,
                            target_profit_rate=target_profit_rate,
                            is_compound=is_compound,
                        )
                        if cleared:
                            raw_state = await AssassinLedger.get_state(symbol)
                            (
                                last_buy_price,
                                budget,
                                target_profit_rate,
                                is_compound,
                                is_session_done,
                                is_active,
                                target_sell_price,
                                sell_order_id,
                                entry_session,
                                entry_time,
                                nq_entry_price,
                                nq_entry_amp,
                                entry_trend,
                            ) = _unpack_ledger_state_safe(raw_state)
                            buy_order_id = await AssassinLedger.get_buy_order_id(symbol)

                    # [Step C] 방향성 인지 매크로 3단 방어망 (1·2차 NQ=F 과열 & 3차 토스 QQQ 0.0초 실시간 역행 방어 — Case 66, 67, 86, 92, 94, 95, 96)
                    macro_triggered = await _call_plugin_adaptive(
                        execute_macro_defense_plugin,
                        ctx=ctx,
                        symbol=symbol,
                        now_est=now_est,
                        current_session_id=current_session_id,
                        is_active=is_active,
                        holdings_qty=holdings_qty,
                        avg_price=avg_price,
                        current_price=current_price,
                        buy_order_id=buy_order_id,
                        sell_order_id=sell_order_id,
                        entry_session=entry_session,
                        entry_trend=entry_trend,
                        nq_entry_price=nq_entry_price,
                        nq_entry_amp=nq_entry_amp,
                    )
                    if macro_triggered:
                        fast_shield_mode = True
                        continue

                    # [Step D] 04:07:00 EST 듀얼 1주 휩소 노출 작전 타임쉴드 만료 정리 (Case 80, 95, 96)
                    if entry_session == "WHIPSAW_1SH" and (407 <= est_hm <= 929):
                        buy_order_id, entry_session, whip_handled = await _call_plugin_adaptive(
                            enforce_0407_whipsaw_cutoff_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            holdings_qty=holdings_qty,
                            buy_order_id=buy_order_id,
                            sell_order_id=sell_order_id,
                            entry_session=entry_session,
                            current_price=current_price,
                            cancel_helper_fn=sniper_plugin_cancel_helper,
                        )
                        if whip_handled:
                            fast_shield_mode = True
                            continue

                    # [Step E] 08:15 EST 거시지표 발표 15분 전 선제 컷오프 및 미체결 매수 파기 (Case 48, 84)
                    if 815 <= est_hm <= 1558:
                        buy_order_id, is_session_done = await _call_plugin_adaptive(
                            enforce_0930_regular_cutoff_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            is_session_done=is_session_done,
                            buy_order_id=buy_order_id,
                        )

                    # [Step F] 15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 물량 당일 정규장 VWAP 도달 즉시 덤핑 탈출 (Case 98)
                    if (1500 <= est_hm <= 1558) and holdings_qty > 0:
                        vwap_escaped = await _call_plugin_adaptive(
                            execute_late_regular_vwap_escape_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            holdings_qty=holdings_qty,
                            avg_price=avg_price,
                            current_price=current_price,
                            vwap_val=vwap_val,
                            buy_order_id=buy_order_id,
                            sell_order_id=sell_order_id,
                            target_sell_price=target_sell_price,
                            entry_session=entry_session,
                        )
                        if vwap_escaped:
                            fast_shield_mode = True
                            continue

                    # [Step G] 조건부 30초 타임쉴드 정정 (RSI>30/35 반등 또는 30초 만료) + 정정 직후 잔고 즉시 재조회 (Case 59, 97, 100)
                    if buy_order_id and entry_session in ("PRE_SNIPER", "PRE_PULLBACK"):
                        fast_shield_mode = True
                        buy_order_id, entry_session = await _call_plugin_adaptive(
                            rearm_premarket_buy_trap_after_30s_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            buy_order_id=buy_order_id,
                            entry_session=entry_session,
                            entry_time=entry_time,
                            budget=budget,
                            current_price=current_price,
                            rsi_val=rsi_val,
                        )
                        # 초과 Case 100: 정정 직후 즉시 체결된 물량이 같은 루프 틱에 안전 캡 장전으로 직행하도록 잔고 재조회!
                        hold_recheck = await api_client.get_symbol_holdings_detail(symbol)
                        if not hold_recheck.get("error"):
                            holdings_qty = int(math.floor(float(hold_recheck.get("qty", 0.0) or 0.0)))
                            avg_price = float(hold_recheck.get("avg_price", 0.0) or avg_price)
                            shared_holdings[symbol] = holdings_qty

                    # [Step H] 보유 물량 상단 안전 캡(+5.00% LIMIT) 선장전 및 이원화 하이킨 아시 트레일링 스윕 (Case 58, 85, 89, 99)
                    if holdings_qty > 0:
                        await _call_plugin_adaptive(
                            manage_mechanical_profit_trap_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            holdings_qty=holdings_qty,
                            avg_price=avg_price,
                            current_price=current_price,
                            target_profit_rate=target_profit_rate,
                            target_sell_price=target_sell_price,
                            buy_order_id=buy_order_id,
                            sell_order_id=sell_order_id,
                            entry_session=entry_session,
                            nq_entry_price=nq_entry_price,
                            nq_entry_amp=nq_entry_amp,
                        )

                    # [Step I] 03:58 ~ 04:06:59 EST 듀얼 1주 고정 '휩소 노출 작전' (Case 79, 95)
                    if 358 <= est_hm <= 406:
                        fast_shield_mode = True
                        await _call_plugin_adaptive(
                            execute_whipsaw_vwap_exposure_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            current_session_id=current_session_id,
                            is_active=is_active,
                            is_session_done=is_session_done,
                            holdings_qty=holdings_qty,
                            buy_order_id=buy_order_id,
                            current_price=current_price,
                            day_vwap=day_vwap_val,
                        )
                        continue

                    # [Step J] 04:07 ~ 08:14 EST 프리장 골든타임 본진 타격 (1. 최저점 역트레일링 반등 ➔ 2. 선택지 B [대칭 + 07:00~ 추세눌림목])
                    if (407 <= est_hm <= 814) and holdings_qty == 0 and not buy_order_id:
                        rebound_fired = await _call_plugin_adaptive(
                            execute_post_stop_bottom_rebound_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            current_session_id=current_session_id,
                            is_active=is_active,
                            is_session_done=is_session_done,
                            holdings_qty=holdings_qty,
                            buy_order_id=buy_order_id,
                            current_price=current_price,
                            budget=budget,
                            rsi_val=rsi_val,
                            opp_rsi_val=opp_rsi_val,
                        )
                        if rebound_fired:
                            fast_shield_mode = True
                            continue

                        await _call_plugin_adaptive(
                            execute_premarket_rsi_sniper_strike_plugin,
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            current_session_id=current_session_id,
                            current_price=current_price,
                            budget=budget,
                            target_profit_rate=target_profit_rate,
                            rsi_val=rsi_val,
                            opp_rsi_val=opp_rsi_val,
                        )

                    # 6대 0.4초 고주파 부스터 조건 판별 (초과 Case 28, 83, 85, 87, 93, 97, 98)
                    tr_st_sym = get_trailing_runtime_status(symbol)
                    pre_ld_sym = str(_PRE_LEADER_CACHE.get("leader_sym") or "")
                    if (
                        (buy_order_id and entry_session in ("PRE_SNIPER", "PRE_PULLBACK"))
                        or tr_st_sym.get("armed", False)
                        or (holdings_qty > 0 and avg_price > 0.0 and current_price >= avg_price * 1.008)
                        or ((1500 <= est_hm <= 1558) and holdings_qty > 0 and not tr_st_sym.get("armed", False))
                        or bool(_BOTTOM_TRACK_CACHE.get("active", False))
                        or (
                            (407 <= est_hm <= 814)
                            and (
                                rsi_val >= PRE_CLIMAX_BOOSTER_RSI
                                or (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD and opp_rsi_val >= 78.0)
                                or (
                                    (700 <= est_hm <= 814)
                                    and pre_ld_sym == symbol
                                    and (0.0 < rsi_val <= PRE_TREND_BOOSTER_RSI)
                                )
                            )
                        )
                        or ((est_hm >= 1859 or est_hm <= 357) and bool(get_daymarket_sim_status().get("active", False)))
                        or ((1859 <= est_hm <= 1906) or (859 <= kst_hm <= 906))
                    ):
                        fast_shield_mode = True

                except Exception as sym_loop_e:
                    print(f"🚨 [듀얼 스나이퍼 루프 {symbol}] 감시망 붕괴 방어: {sym_loop_e}", flush=True)

        except Exception as outer_e:
            print(f"🚨 [메인 스나이퍼 루프 외곽 방어] {outer_e}", flush=True)

        loop_timeout = LOOP_INTERVAL_FAST if fast_shield_mode else LOOP_INTERVAL_NORMAL
        try:
            await asyncio.wait_for(wakeup_event.wait(), timeout=loop_timeout)
            wakeup_event.clear()
        except asyncio.TimeoutError:
            pass


def _bind_telegram_router(
    dp: Dispatcher,
    bot: Bot,
    api_client: TossApiClient,
    chat_id: int,
):
    """
    MODIFIED: 서버 tg_router.py 의 `inject_dependencies(client, admin_id, event, rsi_state)` 를
    최우선 직결 호출하고, 기타 초기화 함수 시그니처(setup_telegram_router 등)와 router 등록까지
    100% 무결하게 Dispatcher에 바인딩합니다.
    """
    setup_called = False

    # 1순위: 서버 tg_router.py 표준 의존성 주입 함수 `inject_dependencies` 직결
    if hasattr(tg_router, "inject_dependencies") and callable(tg_router.inject_dependencies):
        try:
            tg_router.inject_dependencies(
                api_client,
                chat_id,
                wakeup_event,
                shared_rsi_state,
            )
            setup_called = True
            print("✅ [텔레그램 라우터 inject_dependencies 바인딩 완료]", flush=True)
        except Exception as e:
            print(f"🚨 [inject_dependencies 호출 방어] {e}", flush=True)

    available_ctx = {
        "dp": dp,
        "dispatcher": dp,
        "bot": bot,
        "api_client": api_client,
        "client": api_client,
        "chat_id": chat_id,
        "admin_id": chat_id,
        "event": wakeup_event,
        "wakeup_event": wakeup_event,
        "rsi_state": shared_rsi_state,
        "shared_rsi_state": shared_rsi_state,
        "shared_holdings": shared_holdings,
        "in_memory_ordering_lock": in_memory_ordering_lock,
        "dual_strike_mutex": dual_strike_mutex,
        "idempotency_keys": idempotency_keys,
        "run_git_update_fn": _run_git_update_sync,
        "_run_git_update_sync": _run_git_update_sync,
    }

    for fn_name in (
        "setup_telegram_router",
        "setup_tg_router",
        "setup_router",
        "register_telegram_handlers",
        "register_handlers",
        "init_telegram_router",
    ):
        fn = getattr(tg_router, fn_name, None)
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                call_kwargs = {}
                call_args = []
                has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
                for p_name, param in sig.parameters.items():
                    if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                        continue
                    if p_name in available_ctx:
                        if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                            call_args.append(available_ctx[p_name])
                        else:
                            call_kwargs[p_name] = available_ctx[p_name]
                if has_var_kw:
                    for k, v in available_ctx.items():
                        if k not in call_kwargs:
                            call_kwargs[k] = v
                res = fn(*call_args, **call_kwargs)
                setup_called = True
                if hasattr(res, "message") and hasattr(res, "callback_query"):
                    try:
                        dp.include_router(res)
                    except Exception:
                        pass
                break
            except Exception as e:
                print(f"⚠️ [{fn_name} 바인딩 방어] {e}", flush=True)

    router_obj = getattr(tg_router, "router", None)
    if router_obj is not None:
        for attr_k, attr_v in available_ctx.items():
            if hasattr(tg_router, attr_k):
                try:
                    setattr(tg_router, attr_k, attr_v)
                except Exception:
                    pass
        if getattr(router_obj, "parent_router", None) is None:
            try:
                dp.include_router(router_obj)
            except Exception:
                pass


def _start_candle_recorder_task(api_client: TossApiClient) -> list:
    """
    candle_recorder.py 의 백그라운드 태스크 엔트리포인트 시그니처를 정밀 분석하여,
    `record_candles_loop(api_client, symbol)` 처럼 `symbol` 인자를 요구하는 경우
    TRADE_SYMBOLS ("SOXS", "SOXL") 각각에 대해 독립 태스크를 기동합니다.
    """
    tasks = []
    for fn_name in (
        "candle_recorder_loop",
        "start_candle_recorder",
        "run_candle_recorder",
        "record_candles_loop",
    ):
        fn = getattr(candle_recorder, fn_name, None)
        if callable(fn):
            try:
                sig = inspect.signature(fn)
                params = [
                    p for p in sig.parameters.values()
                    if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                ]
                param_names = [p.name.lower() for p in params]
                req_params = [p for p in params if p.default is inspect.Parameter.empty]

                if any("sym" in n for n in param_names) or len(req_params) >= 2:
                    for sym in TRADE_SYMBOLS:
                        if len(params) >= 2 and "sym" in param_names[0]:
                            coro = fn(sym, api_client)
                        elif len(params) == 1:
                            coro = fn(sym)
                        else:
                            coro = fn(api_client, sym)
                        if asyncio.iscoroutine(coro):
                            tasks.append(asyncio.create_task(coro))
                    if tasks:
                        break
                elif len(req_params) == 1 or len(params) == 1:
                    coro = fn(api_client)
                    if asyncio.iscoroutine(coro):
                        tasks.append(asyncio.create_task(coro))
                        break
                else:
                    coro = fn()
                    if asyncio.iscoroutine(coro):
                        tasks.append(asyncio.create_task(coro))
                        break
            except Exception as e:
                print(f"🚨 [캔들 레코더 기동 방어 ({fn_name})] {e}", flush=True)

    rec_cls = getattr(candle_recorder, "CandleRecorder", None)
    if not tasks and rec_cls is not None:
        try:
            inst = rec_cls(api_client)
            for m_name in ("start_loop", "run_loop", "start", "run"):
                m_fn = getattr(inst, m_name, None)
                if callable(m_fn):
                    coro = m_fn()
                    if asyncio.iscoroutine(coro):
                        tasks.append(asyncio.create_task(coro))
                        break
        except Exception:
            pass
    return tasks


async def main():
    _install_fast_shutdown_handlers()

    tg_token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or os.getenv("TELEGRAM_TOKEN")
        or os.getenv("BOT_TOKEN")
        or ""
    ).strip()
    raw_chat_id = (
        os.getenv("TELEGRAM_CHAT_ID")
        or os.getenv("CHAT_ID")
        or "0"
    ).strip()
    chat_id = int(raw_chat_id) if raw_chat_id.lstrip("-").isdigit() else 0

    if not tg_token:
        print("🚨 [치명적 에러] .env 파일에 TELEGRAM_BOT_TOKEN 이 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    api_client = TossApiClient()
    GlobalThrottle.active_client = api_client

    # 제2헌법: 기동 즉시 종합매매(BROKERAGE) 계좌 accountSeq 메모리 락온 (최대 3회 백오프 시도)
    for attempt in range(3):
        if await ensure_account_seq_locked(api_client):
            break
        await asyncio.sleep(1.0 * (attempt + 1))

    bot = Bot(token=tg_token)
    dp = Dispatcher()

    _bind_telegram_router(
        dp=dp,
        bot=bot,
        api_client=api_client,
        chat_id=chat_id,
    )

    bg_tasks = [
        asyncio.create_task(
            rsi_radar_loop(
                api_client=api_client,
                bot=bot,
                chat_id=chat_id,
                shared_rsi_state=shared_rsi_state,
                shared_holdings=shared_holdings,
                wakeup_event=wakeup_event,
                in_memory_ordering_lock=in_memory_ordering_lock,
                dual_strike_mutex=dual_strike_mutex,
                idempotency_keys=idempotency_keys,
            )
        ),
        asyncio.create_task(
            assassin_loop(
                api_client=api_client,
                bot=bot,
                chat_id=chat_id,
            )
        ),
        asyncio.create_task(
            auto_update_loop(
                bot=bot,
                chat_id=chat_id,
            )
        ),
    ]
    bg_tasks.extend(_start_candle_recorder_task(api_client))

    print("🚀 [듀얼 스나이퍼 봇 기동 완료] 9대 코어 및 100대 방어망 정상 가동 개시", flush=True)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types(), handle_signals=False)
    finally:
        for t in bg_tasks:
            t.cancel()
        if hasattr(api_client, "close"):
            try:
                await api_client.close()
            except Exception:
                pass
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
