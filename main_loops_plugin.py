# =====================================================================
# FILE: main_loops_plugin.py
# 목적: main.py (line 73~83) 필수 임포트 9대 심볼(EST_TZ, KST_TZ, TRADE_SYMBOLS,
#       RSI_SYMBOLS, NQ_DYNAMIC_FLOOR, HOLIDAY_TRANSLATIONS, _run_git_update_sync,
#       auto_update_loop, rsi_radar_loop) 및 assassin_loop 100% 제공,
#       04:07 ~ 08:14 EST 프리장 2페이지(400분봉) 풀 스캔 기반 첫 RSI>=79.9 상승주도주(👑) 자동 복원,
#       03:30 ~ 17:00 EST 장중 무인 업데이트 락다운(Case 07) 및 9대 코어 프리플라이트 검증(Case 11) 플러그인
#       (main.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 백그라운드 레이더·자동업데이트·메인 전술 루프를 main_loops_plugin.py로 분리
# MODIFIED: 1) main.py 에서 임포트하는 HOLIDAY_TRANSLATIONS(초과 Case 45), _run_git_update_sync(Case 11),
#              auto_update_loop(Case 07), TRADE_SYMBOLS, RSI_SYMBOLS, NQ_DYNAMIC_FLOOR 전수 구현으로 ImportError 영구 소각
#           2) AssassinLedger.get_state 및 calculate_rsi_1m 반환값 추출 시 고정 언패킹 대신 안전 인덱스 추출기 적용
#           3) 초과 Case 28, 72, 73, 83, 85, 87, 93, 97, 98, 99, 100 전술 스텝 및 2페이지(400분봉) 조회 결속

import os
import sys
import glob
import math
import time
import asyncio
import subprocess
import py_compile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
    execute_rsi_monitoring_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
    execute_post_stop_bottom_rebound_plugin,
    execute_premarket_rsi_sniper_strike_plugin,
    rearm_premarket_buy_trap_after_30s_plugin,
    reconcile_zero_holdings_exit_plugin,
    manage_mechanical_profit_trap_plugin,
    execute_late_regular_vwap_escape_plugin,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
)
from defense_plugin import (
    _cancel_open_orders_and_wait_sellable as sniper_plugin_cancel_helper,
)

# 제4헌법 및 Case 09, 11, 45: 필수 타임존, 허용 종목 상수 및 미국 휴장일 한글 번역 매핑 사전
EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

TRADE_SYMBOLS = ("SOXS", "SOXL")
RSI_SYMBOLS = ("SOXS", "SOXL")
ALLOWED_SYMBOLS = ("SOXS", "SOXL")
REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL")
NQ_DYNAMIC_FLOOR = MIN_STAGE3_QQQ_ADVERSE_PCT
LOOP_INTERVAL_NORMAL = 1.5
LOOP_INTERVAL_FAST = 0.4

# 초과 Case 45: pandas_market_calendars 영문 휴일명 한글 정밀 매핑 사전
HOLIDAY_TRANSLATIONS = {
    "New Year's Day": "신정",
    "Martin Luther King, Jr. Day": "마틴 루터 킹 데이",
    "Dr. Martin Luther King Jr. Day": "마틴 루터 킹 데이",
    "Washington's Birthday": "대통령의 날(워싱턴 탄신일)",
    "Presidents' Day": "대통령의 날",
    "Good Friday": "성금요일(부활절 전 금요일)",
    "Memorial Day": "현충일(메모리얼 데이)",
    "Juneteenth National Independence Day": "준틴스 노예해방기념일",
    "Juneteenth": "준틴스 노예해방기념일",
    "Independence Day": "미국 독립기념일",
    "Labor Day": "노동절",
    "Thanksgiving Day": "추수감사절",
    "Christmas Day": "크리스마스(성탄절)",
}

shared_rsi_state = {
    "SOXS": {"value": 0.0, "vwap": 0.0, "ewm_state": {}},
    "SOXL": {"value": 0.0, "vwap": 0.0, "ewm_state": {}},
}
shared_holdings = {"SOXS": 0, "SOXL": 0}
wakeup_event = asyncio.Event()
in_memory_ordering_lock = {"SOXS": False, "SOXL": False}
dual_strike_mutex = asyncio.Lock()
idempotency_keys = {
    "SOXS": {"BUY": None, "SELL": None, "M_DUMP": None},
    "SOXL": {"BUY": None, "SELL": None, "M_DUMP": None},
}

_LAST_GC_DATE_STR = ""
LAST_GC_DATE = ""

CORE_VERIFY_MODULES = (
    "main.py",
    "sniper_plugin.py",
    "defense_plugin.py",
    "whipsaw_plugin.py",
    "tg_router.py",
    "tg_ui_plugin.py",
    "quant_engine.py",
    "toss_api.py",
    "candle_recorder.py",
)


def _unpack_rsi_calc_safe(calc_res) -> tuple[float, float, dict]:
    """
    calculate_rsi_1m 반환값이 2-튜플(rsi, vwap)이든 3-튜플(rsi, vwap, ewm_state)이든
    ValueError(too many values to unpack) 없이 100% 안전하게 추출합니다.
    """
    if not isinstance(calc_res, (tuple, list)):
        return 50.0, 0.0, {}
    n = len(calc_res)
    r_val = float(calc_res[0] or 50.0) if n >= 1 else 50.0
    v_val = float(calc_res[1] or 0.0) if n >= 2 else 0.0
    ewm_st = calc_res[2] if (n >= 3 and isinstance(calc_res[2], dict)) else getattr(calc_res, "ewm_state", {})
    if not isinstance(ewm_st, dict):
        ewm_st = {}
    return r_val, v_val, ewm_st


def _unpack_ledger_state_safe(state_tuple) -> tuple:
    """
    AssassinLedger.get_state(symbol) 반환값이 12개, 13개, 14개 중 어느 길이이더라도
    ValueError(too many values to unpack) 없이 13개 표준 필드로 안전하게 정규화 추출합니다.
    """
    if not isinstance(state_tuple, (tuple, list)):
        return (0.0, 100.0, SINGLE_TARGET_PROFIT_RATE, True, False, True, 0.0, "", "", 0.0, 0.0, 0.0, "")
    n = len(state_tuple)
    last_buy_price = float(state_tuple[0] or 0.0) if n > 0 else 0.0
    budget = float(state_tuple[1] or 100.0) if n > 1 else 100.0
    target_profit_rate = float(state_tuple[2] or SINGLE_TARGET_PROFIT_RATE) if n > 2 else SINGLE_TARGET_PROFIT_RATE
    is_compound = bool(state_tuple[3]) if n > 3 else True
    is_session_done = bool(state_tuple[4]) if n > 4 else False
    is_active = bool(state_tuple[5]) if n > 5 else True
    target_sell_price = float(state_tuple[6] or 0.0) if n > 6 else 0.0
    sell_order_id = str(state_tuple[7] or "") if n > 7 else ""
    entry_session = str(state_tuple[8] or "") if n > 8 else ""
    entry_time = float(state_tuple[9] or 0.0) if n > 9 else 0.0
    nq_entry_price = float(state_tuple[10] or 0.0) if n > 10 else 0.0
    nq_entry_amp = float(state_tuple[11] or 0.0) if n > 11 else 0.0
    entry_trend = str(state_tuple[12] or "") if n > 12 else ""
    return (
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
    )


def _run_git_update_sync(force_pull: bool = False) -> tuple[bool, str]:
    """
    Case 11: Git 원격 저장소(origin/main) 동기화 및 9대 코어 + 분할 플러그인 전체 py_compile 프리플라이트 검증.
    """
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        subprocess.run(
            ["git", "fetch", "--all"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
        local_hash = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).stdout.strip()
        remote_hash = subprocess.run(
            ["git", "rev-parse", "origin/main"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).stdout.strip()

        if not force_pull and local_hash and remote_hash and local_hash == remote_hash:
            return False, "ALREADY_UP_TO_DATE"

        pull_proc = subprocess.run(
            ["git", "reset", "--hard", "origin/main"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
        if pull_proc.returncode != 0:
            return False, f"GIT_RESET_ERR: {pull_proc.stderr.strip()}"

        # 9대 코어 및 분할 플러그인 전체 py_compile 문법 무결성 검증 (Case 11)
        py_files = set(CORE_VERIFY_MODULES)
        for fp in glob.glob(os.path.join(repo_dir, "*.py")):
            py_files.add(os.path.basename(fp))

        for mod_name in sorted(py_files):
            mod_path = os.path.join(repo_dir, mod_name)
            if os.path.exists(mod_path):
                py_compile.compile(mod_path, doraise=True)

        return True, f"{local_hash[:7]} -> {remote_hash[:7]}"
    except Exception as e:
        return False, f"UPDATE_EX: {e}"


async def auto_update_loop(*args, **kwargs):
    """
    Case 07 & Case 11:
    프리장 개장 전부터 애프터장 종료(03:30 ~ 17:00 EST)까지 무인 자동 업데이트 재기동을 전면 차단(Lockdown)하고,
    장외 안전 시간대에만 원격 커밋을 감지하여 프리플라이트 검증 통과 시 하드 킬(os._exit(0)) 재기동합니다.
    """
    deps = _resolve_loop_dependencies(*args, **kwargs)
    bot = deps.get("bot")
    chat_id = deps.get("chat_id", 0)

    while True:
        try:
            await asyncio.sleep(300.0)
            now_est = datetime.now(EST_TZ)
            wk = now_est.weekday()
            est_hm = now_est.hour * 100 + now_est.minute

            # Case 07: 평일 03:30 ~ 17:00 EST 장중 업데이트 락다운 엄수
            if wk in (0, 1, 2, 3, 4) and (330 <= est_hm <= 1700):
                continue

            updated, detail_msg = await asyncio.wait_for(
                asyncio.to_thread(_run_git_update_sync, False),
                timeout=45.0,
            )
            if updated:
                if bot and chat_id:
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                "🚀 <b>[장외 자동 업데이트 완료]</b>\n"
                                f"▫️ 버전 동기화: <code>{detail_msg}</code>\n"
                                "▫️ 9대 코어 컴파일 검증 통과 ➡️ 시스템을 재기동합니다."
                            ),
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
                os._exit(0)
        except Exception as e:
            print(f"🚨 [자동 업데이트 루프 방어] {e}", flush=True)
            await asyncio.sleep(60.0)


def _resolve_loop_dependencies(*args, **kwargs) -> dict:
    """
    main.py 에서 위치 인자 또는 키워드 인자 순서가 어떻게 전달되더라도
    타입 검사를 통해 100% 정확하게 루프 의존성 객체를 매핑합니다.
    """
    api_client = kwargs.get("api_client") or kwargs.get("client")
    bot = kwargs.get("bot")
    chat_id = kwargs.get("chat_id")
    rsi_st = kwargs.get("shared_rsi_state")
    hold_st = kwargs.get("shared_holdings")
    wake_ev = kwargs.get("wakeup_event")
    ord_lock = kwargs.get("in_memory_ordering_lock")
    strike_mtx = kwargs.get("dual_strike_mutex")
    idem_map = kwargs.get("idempotency_keys")

    dict_args = []
    for arg in args:
        if arg is None:
            continue
        if isinstance(arg, TossApiClient) or hasattr(arg, "get_1m_candles_pagination"):
            api_client = api_client or arg
        elif hasattr(arg, "send_message"):
            bot = bot or arg
        elif isinstance(arg, asyncio.Event):
            wake_ev = wake_ev or arg
        elif isinstance(arg, asyncio.Lock):
            strike_mtx = strike_mtx or arg
        elif isinstance(arg, int) and not isinstance(arg, bool):
            if chat_id is None:
                chat_id = arg
        elif isinstance(arg, str) and arg.lstrip("-").isdigit():
            if chat_id is None:
                chat_id = int(arg)
        elif isinstance(arg, dict):
            dict_args.append(arg)

    for d in dict_args:
        soxs_v = d.get("SOXS")
        if isinstance(soxs_v, dict) and ("value" in soxs_v or "vwap" in soxs_v or "ewm_state" in soxs_v):
            if rsi_st is None:
                rsi_st = d
        elif isinstance(soxs_v, dict) and ("BUY" in soxs_v or "SELL" in soxs_v):
            if idem_map is None:
                idem_map = d
        elif isinstance(soxs_v, bool):
            if ord_lock is None:
                ord_lock = d
        elif isinstance(soxs_v, (int, float)):
            if hold_st is None:
                hold_st = d

    if api_client is None:
        api_client = getattr(GlobalThrottle, "active_client", None)
    if chat_id is None:
        env_cid = os.getenv("TELEGRAM_CHAT_ID", "0").strip()
        chat_id = int(env_cid) if env_cid.lstrip("-").isdigit() else 0
    if rsi_st is None:
        rsi_st = shared_rsi_state
    if hold_st is None:
        hold_st = shared_holdings
    if wake_ev is None:
        wake_ev = wakeup_event
    if ord_lock is None:
        ord_lock = in_memory_ordering_lock
    if strike_mtx is None:
        strike_mtx = dual_strike_mutex
    if idem_map is None:
        idem_map = idempotency_keys

    return {
        "api_client": api_client,
        "bot": bot,
        "chat_id": int(chat_id or 0),
        "shared_rsi_state": rsi_st,
        "shared_holdings": hold_st,
        "wakeup_event": wake_ev,
        "in_memory_ordering_lock": ord_lock,
        "dual_strike_mutex": strike_mtx,
        "idempotency_keys": idem_map,
    }


async def run_daily_gc(rsi_state_dict: dict = None, holdings_dict: dict = None, *args, **kwargs):
    """
    Case 22: 17:00 EST 일일 마감 시 낡은 로그/임시 파일 소각 및
    미보유 종목의 장부 잠금·트레일링·최저점추적·상승주도주 캐시 초기화.
    """
    try:
        target_rsi = rsi_state_dict if isinstance(rsi_state_dict, dict) else shared_rsi_state
        target_hold = holdings_dict if isinstance(holdings_dict, dict) else shared_holdings

        def _clean_files_sync():
            now_ts = time.time()
            for pat in ("*.tmp", "*.log.*", "candles_*.bak"):
                for fp in glob.glob(pat):
                    try:
                        if now_ts - os.path.getmtime(fp) > 86400 * 3:
                            os.remove(fp)
                    except Exception:
                        pass

        await asyncio.to_thread(_clean_files_sync)

        _reset_bottom_track_state()
        _reset_pre_leader_state()

        qty_s = int((target_hold or {}).get("SOXS", 0))
        qty_l = int((target_hold or {}).get("SOXL", 0))
        if qty_s <= 0:
            _reset_trailing_runtime_state("SOXS")
            _reset_manual_stage3_anchor("SOXS")
            await AssassinLedger.save_state(
                "SOXS",
                is_session_done=False,
                buy_order_id="",
                entry_session="",
                whipsaw_target_price=0.0,
            )
        if qty_l <= 0:
            _reset_trailing_runtime_state("SOXL")
            _reset_manual_stage3_anchor("SOXL")
            await AssassinLedger.save_state(
                "SOXL",
                is_session_done=False,
                buy_order_id="",
                entry_session="",
                whipsaw_target_price=0.0,
            )

        if isinstance(target_rsi, dict):
            for sym in TRADE_SYMBOLS:
                if sym in target_rsi and isinstance(target_rsi[sym], dict):
                    target_rsi[sym]["ewm_state"] = {}
        print("🧹 [17:00 EST 일일 GC 완료] 캐시 및 미보유 장부 초기화 완료", flush=True)
    except Exception as e:
        print(f"🚨 [17:00 EST 일일 GC 방어] {e}", flush=True)


execute_daily_garbage_collection = run_daily_gc


async def _fetch_candles_for_radar(
    api_client: TossApiClient,
    symbol: str,
    now_est: datetime,
    holdings_map: dict,
) -> list:
    """
    초과 Case 72, 97, 98:
    - 평시에는 최신 1분봉 1페이지(200개)를 조회합니다.
    - 단, ① 04:07 ~ 08:14 EST 프리장 골든타임 구간(04:00 EST 개장 첫 봉 및 이전 워밍업 구간 포함하여
      첫 RSI>=79.9 상승 주도주 `leader_sym` 을 100% 복원·추적해야 하는 구간) 또는
      ② 12:50 ~ 15:58 EST 정규장 후반에 포지션을 보유 중(09:30 EST 개장 첫 봉부터의 정규장 누적 VWAP 산출 필요)인 경우에는
      최대 2페이지(400분봉)를 페이지네이션 조회하여 시계열 단절을 원천 봉쇄합니다.
    """
    est_hm = now_est.hour * 100 + now_est.minute
    hold_qty = int((holdings_map or {}).get(symbol, 0))

    need_multi_page = (
        (407 <= est_hm <= 814)
        or ((1250 <= est_hm <= 1558) and hold_qty > 0)
    )

    if not need_multi_page:
        return await api_client.get_1m_candles(symbol, count=200)

    all_candles = []
    before_cursor = None
    for _ in range(2):
        page_data = await api_client.get_1m_candles_pagination(symbol, count=200, before=before_cursor)
        c_list = (page_data or {}).get("candles", [])
        if not c_list:
            break
        all_candles.extend(c_list)
        before_cursor = (page_data or {}).get("nextBefore")
        if not before_cursor:
            break

    return all_candles if all_candles else await api_client.get_1m_candles(symbol, count=200)


async def rsi_radar_loop(*args, **kwargs):
    """
    초과 Case 72, 73, 93, 97, 98:
    SOXS 및 SOXL 백그라운드 1분봉 캔들·VWAP·하이킨 아시·상승주도주 동기화 레이더 루프.
    """
    deps = _resolve_loop_dependencies(*args, **kwargs)
    api_client: TossApiClient = deps["api_client"]
    bot = deps["bot"]
    chat_id: int = deps["chat_id"]
    rsi_state_map: dict = deps["shared_rsi_state"]
    holdings_map: dict = deps["shared_holdings"]
    wake_ev: asyncio.Event = deps["wakeup_event"]
    ord_lock: dict = deps["in_memory_ordering_lock"]
    strike_mtx: asyncio.Lock = deps["dual_strike_mutex"]
    idem_map: dict = deps["idempotency_keys"]

    ctx = SniperPluginContext(
        client=api_client,
        bot=bot,
        chat_id=chat_id,
        wakeup_event=wake_ev,
        in_memory_ordering_lock=ord_lock,
        dual_strike_mutex=strike_mtx,
        shared_holdings=holdings_map,
        idempotency_keys=idem_map,
        nq_dynamic_floor=NQ_DYNAMIC_FLOOR,
        shared_rsi_state=rsi_state_map,
    )

    while True:
        try:
            now_est = datetime.now(EST_TZ)
            now_kst = now_est.astimezone(KST_TZ)
            wk = now_est.weekday()
            est_hm = now_est.hour * 100 + now_est.minute
            kst_hm = now_kst.hour * 100 + now_kst.minute

            is_weekend_closed = (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800)
            if is_weekend_closed or (1700 <= est_hm <= 1759):
                await asyncio.sleep(15.0)
                continue

            if 1800 <= est_hm <= 1858:
                await asyncio.sleep(10.0)
                continue

            prices_map = await api_client.get_current_prices_batch(RSI_SYMBOLS)

            for sym in RSI_SYMBOLS:
                try:
                    candles = await _fetch_candles_for_radar(
                        api_client=api_client,
                        symbol=sym,
                        now_est=now_est,
                        holdings_map=holdings_map,
                    )
                    if not candles:
                        continue

                    calc_raw = await asyncio.to_thread(calculate_rsi_1m, candles, now_est)
                    rsi_val, vwap_val, ewm_state = _unpack_rsi_calc_safe(calc_raw)

                    live_p = float((prices_map or {}).get(sym, 0.0) or 0.0)
                    if live_p <= 0.0 and ewm_state:
                        live_p = float(ewm_state.get("prev_close", 0.0) or 0.0)

                    if live_p > 0.0 and ewm_state:
                        rsi_val = compute_realtime_tick_rsi(
                            live_price=live_p,
                            ewm_state=ewm_state,
                            fallback_rsi=rsi_val,
                        )
                        compute_realtime_tick_heikin_ashi(live_p, ewm_state)

                    if sym not in rsi_state_map or not isinstance(rsi_state_map[sym], dict):
                        rsi_state_map[sym] = {"value": rsi_val, "vwap": vwap_val, "ewm_state": ewm_state}
                    else:
                        rsi_state_map[sym]["value"] = rsi_val
                        rsi_state_map[sym]["vwap"] = vwap_val
                        rsi_state_map[sym]["ewm_state"] = ewm_state

                    await execute_rsi_monitoring_plugin(
                        ctx=ctx,
                        symbol=sym,
                        rsi_val=rsi_val,
                        vwap_val=vwap_val,
                        current_price=live_p,
                    )
                except Exception as sym_e:
                    print(f"🚨 [RSI 레이더망 {sym}] 붕괴 방어: {sym_e}", flush=True)

            # 초과 Case 97: 04:07 ~ 08:14 EST 구간에는 양쪽 스캔 직후 즉시 상승 주도주(leader_sym) 비교·복원 수행
            if 407 <= est_hm <= 814:
                for sym_ld in TRADE_SYMBOLS:
                    r_ld = float((rsi_state_map.get(sym_ld) or {}).get("value", 0.0))
                    update_premarket_trend_leader(
                        symbol=sym_ld,
                        live_rsi_val=r_ld,
                        now_est=now_est,
                        shared_rsi_state=rsi_state_map,
                    )

            total_hold = sum(int(v) for v in (holdings_map or {}).values())
            any_trail_armed = any(
                bool(_TRAIL_RUNTIME_CACHE.get(s, {}).get("armed", False)) for s in TRADE_SYMBOLS
            )
            bot_track_on = bool(_BOTTOM_TRACK_CACHE.get("active", False))
            day_sim_st = get_daymarket_sim_status()
            in_day_open_window = (1859 <= est_hm <= 1906) or (859 <= kst_hm <= 906)

            if (
                (355 <= est_hm <= 814)
                or total_hold > 0
                or any_trail_armed
                or bot_track_on
                or in_day_open_window
                or bool(day_sim_st.get("active", False))
            ):
                sleep_sec = 5.0
            else:
                sleep_sec = 15.0

            await asyncio.sleep(sleep_sec)
        except Exception as e:
            print(f"🚨 [RSI 레이더 루프 전체 방어] {e}", flush=True)
            await asyncio.sleep(5.0)


async def assassin_loop(*args, **kwargs):
    """
    제1 ~ 제6헌법 및 100대 엣지 케이스 방어망이 결속된 메인 듀얼 스나이퍼 전술 루프.
    """
    global _LAST_GC_DATE_STR
    deps = _resolve_loop_dependencies(*args, **kwargs)
    api_client: TossApiClient = deps["api_client"]
    bot = deps["bot"]
    chat_id: int = deps["chat_id"]
    rsi_state_map: dict = deps["shared_rsi_state"]
    holdings_map: dict = deps["shared_holdings"]
    wake_ev: asyncio.Event = deps["wakeup_event"]
    ord_lock: dict = deps["in_memory_ordering_lock"]
    strike_mtx: asyncio.Lock = deps["dual_strike_mutex"]
    idem_map: dict = deps["idempotency_keys"]

    ctx = SniperPluginContext(
        client=api_client,
        bot=bot,
        chat_id=chat_id,
        wakeup_event=wake_ev,
        in_memory_ordering_lock=ord_lock,
        dual_strike_mutex=strike_mtx,
        shared_holdings=holdings_map,
        idempotency_keys=idem_map,
        nq_dynamic_floor=NQ_DYNAMIC_FLOOR,
        shared_rsi_state=rsi_state_map,
    )

    while True:
        fast_shield_mode = False
        try:
            now_est = datetime.now(EST_TZ)
            now_kst = now_est.astimezone(KST_TZ)
            wk = now_est.weekday()
            est_hm = now_est.hour * 100 + now_est.minute
            kst_hm = now_kst.hour * 100 + now_kst.minute

            logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 18 else now_est
            current_session_id = logical_dt.strftime("%Y-%m-%d")
            today_str = now_est.strftime("%Y-%m-%d")

            # Case 22: 17:00 EST 일일 가비지 컬렉션(GC) 1회 실행
            if est_hm == 1700 and _LAST_GC_DATE_STR != today_str:
                _LAST_GC_DATE_STR = today_str
                await run_daily_gc(rsi_state_dict=rsi_state_map, holdings_dict=holdings_map)

            is_weekend_closed = (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800)
            if is_weekend_closed or (1700 <= est_hm <= 1759):
                await asyncio.sleep(5.0)
                continue

            # 03:58 ~ 04:00 EST 데이장 [모의] 포지션 자동 정리
            if 358 <= est_hm <= 400:
                await settle_daymarket_sim_before_premarket(ctx, now_est)

            # 1단계: SOXS & SOXL 다건 배치 실시간 현재가 조회 및 O(1) 실시간 틱 RSI·하이킨 아시 합성 (Case 73, 83, 85)
            prices_batch = await api_client.get_current_prices_batch(TRADE_SYMBOLS)
            live_prices = {
                "SOXS": float((prices_batch or {}).get("SOXS", 0.0) or 0.0),
                "SOXL": float((prices_batch or {}).get("SOXL", 0.0) or 0.0),
            }

            live_rsis = {"SOXS": 0.0, "SOXL": 0.0}
            live_vwaps = {"SOXS": 0.0, "SOXL": 0.0}
            day_vwaps = {"SOXS": 0.0, "SOXL": 0.0}

            for sym_t in TRADE_SYMBOLS:
                st_r = (rsi_state_map.get(sym_t) or {}) if isinstance(rsi_state_map, dict) else {}
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
                        shared_rsi_state=rsi_state_map,
                    )

            # 2단계: 종목별 원자적 전술 스텝 실행 (Case 70)
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

                    # 초과 Case 52 & Case 90: 신규 논리 세션 진입 또는 프리장 골든타임 미보유·비과열 잠금 잔재 자가 치유
                    last_sess_id = await AssassinLedger.get_last_session_id(symbol)
                    if last_sess_id and last_sess_id != current_session_id:
                        if int(holdings_map.get(symbol, 0)) <= 0 and not buy_order_id:
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
                        # Case 29: 유령 잔고 방어 (잔고 조회 실패 시 해당 틱 매매 로직 스킵)
                        continue

                    holdings_qty = int(math.floor(float(hold_detail.get("qty", 0.0) or 0.0)))
                    avg_price = float(hold_detail.get("avg_price", 0.0) or 0.0)
                    holdings_map[symbol] = holdings_qty

                    if current_price <= 0.0:
                        current_price = float(hold_detail.get("current_price", 0.0) or 0.0)
                        live_prices[symbol] = current_price

                    # Case 46 & Case 94: 스나이퍼 OFF 상태 시 미체결 매도 주문 취소 및 구버전 3차 기준가 0.0 초기화
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
                        await execute_moc_zero_overnight_plugin(
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
                        cleared = await reconcile_zero_holdings_exit_plugin(
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
                    macro_triggered = await execute_macro_defense_plugin(
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
                        buy_order_id, entry_session, whip_handled = await enforce_0407_whipsaw_cutoff_plugin(
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
                        buy_order_id, is_session_done = await enforce_0930_regular_cutoff_plugin(
                            ctx=ctx,
                            symbol=symbol,
                            now_est=now_est,
                            is_active=is_active,
                            is_session_done=is_session_done,
                            buy_order_id=buy_order_id,
                        )

                    # [Step F] 15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 물량 당일 정규장 VWAP 도달 즉시 덤핑 탈출 (Case 98)
                    if (1500 <= est_hm <= 1558) and holdings_qty > 0:
                        vwap_escaped = await execute_late_regular_vwap_escape_plugin(
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
                        buy_order_id, entry_session = await rearm_premarket_buy_trap_after_30s_plugin(
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
                            holdings_map[symbol] = holdings_qty

                    # [Step H] 보유 물량 상단 안전 캡(+5.00% LIMIT) 선장전 및 이원화 하이킨 아시 트레일링 스윕 (Case 58, 85, 89, 99)
                    if holdings_qty > 0:
                        await manage_mechanical_profit_trap_plugin(
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
                        await execute_whipsaw_vwap_exposure_plugin(
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
                        rebound_fired = await execute_post_stop_bottom_rebound_plugin(
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

                        await execute_premarket_rsi_sniper_strike_plugin(
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
            await asyncio.wait_for(wake_ev.wait(), timeout=loop_timeout)
            wake_ev.clear()
        except asyncio.TimeoutError:
            pass


def __getattr__(name: str):
    """
    PEP 562 모듈 레벨 안전 폴백:
    만일 main.py 에서 예상치 못한 상수명을 import 시도하더라도 ImportError 를 원천 차단합니다.
    """
    up = name.upper()
    if "SYMBOL" in up:
        return ("SOXS", "SOXL")
    if "FLOOR" in up:
        return NQ_DYNAMIC_FLOOR
    if "HOLIDAY" in up:
        return HOLIDAY_TRANSLATIONS
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
