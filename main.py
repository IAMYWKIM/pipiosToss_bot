# =====================================================================
# FILE: main.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=79.9[80.0 클라이맥스 인지])
#       무한 출동 듀얼 스나이퍼(SOXS/SOXL), 단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0)' 무제한 익절 추적,
#       3차 손절 후 최저점 역트레일링 +0.30% 반등 재진입(RE_BOT_TRAIL) 및 03:58 ~ 04:07 EST 듀얼 1주 휩소 매수 덫 오케스트레이터
# 구조: sniper_plugin.py / whipsaw_plugin.py 전술 플러그인과 SniperPluginContext로 결속된 아키텍처
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 데드코드 소각 및 whipsaw_plugin.py 분리 모듈 결속 (8대 코어 모듈)
# MODIFIED: 프리장 스나이퍼 보호를 위해 무인 업데이트 락다운 시간대를 03:30 ~ 17:00 EST로 전면 확장 (Case 07)
# MODIFIED: SOXS 및 SOXL 양방향 대칭 클라이맥스 듀얼 감시 루프(TRADE_SYMBOLS = ("SOXS", "SOXL")) 가동 (Case 09, 54, 71, 81)
# MODIFIED: 03:55 ~ 08:14 EST 구간 및 포지션 보유(수동 포함)/트레일링 점화/최저점 추적 중 5.0초 듀얼 캔들 스캔 가속으로 EWM·HA 기저 및 VWAP 실시간 동기화 (Case 73, 84, 85, 87)
# NEW: 4대 0.4초 고주파 부스터(1차 매수덫 대기 · RSI>=79.0 프리-클라이맥스 · HA트레일링 점화/근접 · 3차 손절 후 최저점 역트레일링 추적) 자동 가속 (Case 28, 83, 85, 87)
# NEW: 수동 진입(MANUAL) 첫 틱 감지 시 execute_macro_defense_plugin과 로컬 상태를 즉시 동기화하여 NQ>=2.0% 구간에서도 2차 방어 패스 후 3차 안전망 & HA트레일링 직결 (Case 50, 66, 67, 86)
# NEW: 04:07 ~ 08:14 EST 중 NQ<2.0% 내 3차 역행 손절 후 최저점 역트레일링 +0.30% 양봉 반등 재진입(execute_post_stop_bottom_rebound_plugin) 결속 (Case 87)
# MODIFIED: 프리장 본진 신규 진입·무한 재출동·자가 치유(Self-Healing) 허용 구간을 04:07 ~ 08:14 EST로 락온 및 08:15 EST 거시지표 선제 컷오프 결속 (Case 48, 52, 53, 71, 84)
# MODIFIED: 수동 매매·1주 휩소 작전·본진 하이킨 아시 트레일링 청산 시 실제 보유 수량(last_known_qty) 추적 및 통합 복리 정산 연동 (Case 76)
# MODIFIED: 시스템 기동 완료 텔레그램 메시지를 "정상작동 중입니다." 단일 라인으로 슬림화 및 미사용 지역 변수 소각 (Case 38)

import sys
import os
import math
import time
import asyncio
import html
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

from toss_api import TossApiClient
from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    calculate_rsi_1m,
    compute_realtime_tick_rsi,
)
from tg_router import router, inject_dependencies
from candle_recorder import record_candles_loop
from sniper_plugin import (
    SniperPluginContext,
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    _cancel_open_orders_and_wait_sellable,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    get_bottom_track_status,
    settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
    reconcile_zero_holdings_exit_plugin,
    execute_post_stop_bottom_rebound_plugin,
    execute_premarket_rsi_sniper_strike_plugin,
    rearm_premarket_buy_trap_after_30s_plugin,
    manage_mechanical_profit_trap_plugin,
    execute_rsi_monitoring_plugin,
)

logging.getLogger("aiogram").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)

EST_TZ = ZoneInfo("America/New_York")

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

TOSS_CLIENT_ID = os.getenv("TOSS_CLIENT_ID")
TOSS_CLIENT_SECRET = os.getenv("TOSS_CLIENT_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_telegram_chat_id_str = os.getenv("TELEGRAM_CHAT_ID")

if not all([TOSS_CLIENT_ID, TOSS_CLIENT_SECRET, TELEGRAM_BOT_TOKEN, _telegram_chat_id_str]):
    print("🚨 치명적 에러: 필수 환경변수 누락.", flush=True)
    sys.exit(1)

ADMIN_CHAT_ID = int(_telegram_chat_id_str)
wakeup_event = asyncio.Event()

TRADE_SYMBOLS = ("SOXS", "SOXL")
RSI_SYMBOLS = ("SOXS", "SOXL")
NQ_DYNAMIC_FLOOR = 0.8

HOLIDAY_TRANSLATIONS = {
    "New Year's Day": "신정 (New Year's Day)",
    "Martin Luther King Jr. Day": "마틴 루터 킹 주니어 탄생일 (MLK Day)",
    "Washington's Birthday": "대통령의 날 (Presidents' Day)",
    "Good Friday": "성금요일 (Good Friday)",
    "Memorial Day": "메모리얼 데이 (Memorial Day)",
    "Juneteenth National Independence Day": "노예해방기념일 (Juneteenth)",
    "Independence Day": "독립기념일 (Independence Day)",
    "Labor Day": "노동절 (Labor Day)",
    "Thanksgiving Day": "추수감사절 (Thanksgiving Day)",
    "Christmas": "크리스마스 (Christmas Day)",
    "Christmas Day": "크리스마스 (Christmas Day)",
    "주말 (Weekend)": "주말 (Weekend)",
}

in_memory_ordering_lock = {sym: False for sym in TRADE_SYMBOLS}
dual_strike_mutex = asyncio.Lock()
shared_holdings = {sym: 0 for sym in TRADE_SYMBOLS}

# RSI<=20/10 경보 상태 머신 폐기에 따라 불필요한 level 키 소각 및 ewm_state(Wilder's RSI + Heikin-Ashi 기저) 슬롯 유지
shared_rsi_state = {
    sym: {"value": 0.0, "vwap": 0.0, "ewm_state": {}} for sym in RSI_SYMBOLS
}

idempotency_keys = {
    sym: {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}
    for sym in TRADE_SYMBOLS
}

holiday_notify_lock = asyncio.Lock()
last_holiday_notified_date = ""


def _run_git_update_sync() -> tuple[bool, str]:
    import subprocess
    import py_compile

    def run_cmd(cmd):
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            out, err = proc.communicate(timeout=30)
            return proc.returncode, out.strip(), err.strip()
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            return -1, out.strip(), "Subprocess Timeout Expired (네트워크 교착 상태 방어)"

    code, current_hash, err = run_cmd("git rev-parse HEAD")
    if code != 0:
        return False, f"해시 백업 실패: {err}"

    code, fetch_out, fetch_err = run_cmd("git fetch origin main")
    if code != 0:
        return False, f"Fetch 실패:\n{fetch_err}"

    code, local_hash, _ = run_cmd("git rev-parse HEAD")
    code, remote_hash, _ = run_cmd("git rev-parse origin/main")

    if local_hash == remote_hash:
        return True, "Already up to date."

    code, reset_out, reset_err = run_cmd("git reset --hard origin/main")
    if code != 0:
        run_cmd(f"git reset --hard {current_hash}")
        return False, f"Hard Reset 붕괴 (원상 복구됨):\n{reset_err}"

    try:
        for mod_file in (
            "main.py",
            "sniper_plugin.py",
            "whipsaw_plugin.py",
            "tg_router.py",
            "tg_ui_plugin.py",
            "quant_engine.py",
            "toss_api.py",
            "candle_recorder.py",
        ):
            if os.path.exists(mod_file):
                py_compile.compile(mod_file, doraise=True)
    except Exception as e:
        run_cmd(f"git reset --hard {current_hash}")
        return False, f"문법 에러 감지. 롤백 완료:\n{str(e)}"

    return True, f"업데이트 성공:\n{reset_out}"


async def auto_update_loop(bot: Bot, chat_id: int):
    while True:
        try:
            now_est = datetime.now(EST_TZ)
            est_hm = now_est.hour * 100 + now_est.minute
            # Case 07 프리장 개장 전부터 애프터장 종료 (03:30 ~ 17:00 EST) 까지 무인 업데이트 전면 차단
            if 330 <= est_hm < 1700:
                await asyncio.sleep(3600.0)
                continue

            success, msg = await asyncio.to_thread(_run_git_update_sync)
            if success:
                if "Already up to date." not in msg:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "🚀 <b>[무인 자동 업데이트 성공]</b>\n"
                            "▫️ 최신 코드가 백그라운드에서 감지되어 구글 클라우드 서버에 안전하게 탑재되었습니다.\n"
                            "▫️ 파이썬 컴파일 검증 완료. 코어 재기동(os._exit) 격발.\n"
                            f"<pre>{html.escape(msg)}</pre>"
                        ),
                        parse_mode="HTML",
                    )
                    await asyncio.sleep(1.0)
                    os._exit(0)
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"🚨 <b>[무인 자동 업데이트 실패 및 롤백]</b>\n<pre>{html.escape(msg)}</pre>",
                    parse_mode="HTML",
                )
        except Exception as e:
            print(f"🚨 [무인 업데이트망 붕괴 방어] {e}", flush=True)

        await asyncio.sleep(3600.0)


async def rsi_radar_loop(ctx: SniperPluginContext, symbol: str):
    while True:
        try:
            now_est = datetime.now(EST_TZ)
            is_open, _, sess_name, _ = await ctx.client.is_market_open()

            if is_open and sess_name != "CLOSED":
                data = await ctx.client.get_1m_candles_pagination(symbol, count=200)
                candles = data.get("candles", [])

                if candles:
                    if now_est.hour >= 19:
                        session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
                    else:
                        session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

                    current_price = await ctx.client.get_current_price(symbol)

                    sym_state = ctx.shared_rsi_state.setdefault(
                        symbol, {"value": 0.0, "vwap": 0.0, "ewm_state": {}}
                    )
                    ewm_state_buf = sym_state.setdefault("ewm_state", {})

                    rsi_val, vwap_val = await asyncio.to_thread(
                        calculate_rsi_1m, candles, session_start_est, 14, current_price, ewm_state_buf
                    )

                    await execute_rsi_monitoring_plugin(
                        ctx=ctx, symbol=symbol, rsi_val=rsi_val, vwap_val=vwap_val, current_price=current_price
                    )

        except Exception as e:
            print(f"🚨 [RSI 레이더망 {symbol}] 붕괴 방어: {e}", flush=True)

        # Case 73 & Case 84 & Case 85 & Case 87 — 03:55 ~ 08:14 EST 구간이거나 포지션 보유(수동 포함)/트레일링 점화/최저점 추적 중에는
        # 5.0초 고속 스캔을 유지하여 Wilder's RSI 및 하이킨 아시(Heikin-Ashi) 1분봉 기저 상태를 오차 없이 동기화
        now_est_chk = datetime.now(EST_TZ)
        est_hm_chk = now_est_chk.hour * 100 + now_est_chk.minute
        is_holding_or_trailing = (
            ctx.shared_holdings.get(symbol, 0) > 0
            or bool(get_trailing_runtime_status(symbol).get("armed", False))
            or bool(get_bottom_track_status(symbol).get("active", False))
        )
        if symbol in RSI_SYMBOLS and ((355 <= est_hm_chk <= 814) or is_holding_or_trailing):
            sleep_sec = 5.0
        elif symbol in RSI_SYMBOLS and (est_hm_chk >= 1900 or est_hm_chk <= 354):
            sleep_sec = 15.0
        else:
            sleep_sec = 60.0
        await asyncio.sleep(sleep_sec)


async def assassin_loop(ctx: SniperPluginContext, symbol: str):
    global last_holiday_notified_date
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    runtime_state = {
        "last_moc_tick": 0.0,
        "moc_dump_active": False,
        "last_heartbeat_hour": -1,
        "last_logged_session": "",
        "prev_is_active": None,
        "last_gc_date": "",
        "last_error_msg": "",
        "last_trap_error_msg": "",
        "fast_shield_mode": False,
        "last_known_qty": 0,
    }

    while True:
        # Case 28 & Case 83 & Case 85 & Case 87: 평시 1.5초 대기,
        # ① 본진 1차 슬리피지 매수 덫 타임쉴드(PRE_SNIPER) 대기 중이거나
        # ② 미보유 감시 중 RSI >= 79.0 프리-클라이맥스 구간 진입 시 또는
        # ③ 보유 포지션(수동 포함)이 하이킨 아시 트레일링 점화(+0.80% 근접 포함) 상태이거나
        # ④ 3차 안전망 손절 후 최저점 역트레일링 추적(RE_BOT_TRAIL) 중일 때 0.4초 고주파 대기로 자동 가속!
        wait_sec = 0.4 if runtime_state.get("fast_shield_mode", False) else 1.5
        try:
            await asyncio.wait_for(ctx.wakeup_event.wait(), timeout=wait_sec)
            ctx.wakeup_event.clear()
        except asyncio.TimeoutError:
            pass

        try:
            now_est = datetime.now(EST_TZ)
            est_time_int = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

            # 초과 Case 29: 토스증권 유령 잔고 방어 (통신 실패 시 하위 매매 로직 전면 스킵)
            try:
                holdings_detail = await ctx.client.get_symbol_holdings_detail(symbol)
                holdings_qty = int(math.floor(holdings_detail["qty"]))
                ctx.shared_holdings[symbol] = holdings_qty
                if holdings_qty > 0:
                    runtime_state["last_known_qty"] = holdings_qty

                if runtime_state["last_error_msg"] != "":
                    await ctx.notify_tg(
                        f"✅ <b>[듀얼 스나이퍼 {symbol}] 토스증권 API 통신망 복구 완료</b>\n"
                        "▫️ 서버 응답 정상화. 감시를 재개합니다."
                    )
                    runtime_state["last_error_msg"] = ""
            except Exception as e:
                err_str = str(e)
                if err_str != runtime_state["last_error_msg"]:
                    await ctx.notify_tg(
                        f"🚨 <b>[듀얼 스나이퍼 {symbol}] 통신 붕괴 (유령 잔고 방어)</b>\n"
                        f"▫️ 사유: {html.escape(err_str)}"
                    )
                    runtime_state["last_error_msg"] = err_str
                await asyncio.sleep(5.0)
                continue

            ledger_tuple = await AssassinLedger.get_state(symbol)
            (
                last_buy_price,
                budget,
                target_profit_rate,
                last_session_id,
                is_session_done,
                is_active,
                target_sell_price,
                sell_order_id,
                entry_session,
                entry_time,
                nq_entry_price,
                nq_entry_amp,
            ) = ledger_tuple
            buy_order_id = await AssassinLedger.get_buy_order_id(symbol)

            sym_rsi_dict = ctx.shared_rsi_state.setdefault(
                symbol, {"value": 0.0, "vwap": 0.0, "ewm_state": {}}
            )
            opp_rsi_dict = ctx.shared_rsi_state.setdefault(
                opp_sym, {"value": 0.0, "vwap": 0.0, "ewm_state": {}}
            )
            cached_my_rsi = float(sym_rsi_dict.get("value", 0.0))
            cached_opp_rsi = float(opp_rsi_dict.get("value", 0.0))

            is_whipsaw_window = (358 <= est_time_int <= 406)
            # 초과 Case 53, 84 — 프리장 본진 스나이퍼 허용 구간을 04:07 ~ 08:14 EST 초중반 황금 구간으로 락온
            is_premarket_sniper_window = (407 <= est_time_int <= 814)
            is_daymarket_window = (est_time_int >= 1900 or est_time_int <= 359)
            is_daymarket_active_window = (est_time_int >= 1900 or est_time_int <= 357)

            trail_st_pre = get_trailing_runtime_status(symbol)
            is_trail_armed_now = bool(
                is_active
                and holdings_qty > 0
                and entry_session != "WHIPSAW_1SH"
                and trail_st_pre.get("armed", False)
            )
            bot_track_pre = get_bottom_track_status(symbol)
            is_bottom_track_now = bool(
                is_active
                and is_premarket_sniper_window
                and bot_track_pre.get("active", False)
            )

            # Case 28 & Case 83 & Case 87: 4대 고주파 부스터 조건 판정
            is_trap_waiting = bool(
                is_active
                and is_premarket_sniper_window
                and entry_session == "PRE_SNIPER"
                and bool(buy_order_id)
                and holdings_qty == 0
            )
            is_pre_climax_booster = bool(
                is_active
                and (is_premarket_sniper_window or is_daymarket_active_window)
                and not is_session_done
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and (
                    cached_my_rsi >= PRE_CLIMAX_BOOSTER_RSI
                    or cached_opp_rsi >= PRE_CLIMAX_BOOSTER_RSI
                    or (0.0 < cached_my_rsi <= PRE_SNIPER_RSI_THRESHOLD and cached_opp_rsi >= 78.0)
                    or (0.0 < cached_opp_rsi <= PRE_SNIPER_RSI_THRESHOLD and cached_my_rsi >= 78.0)
                )
            )
            runtime_state["fast_shield_mode"] = bool(
                is_trap_waiting or is_pre_climax_booster or is_trail_armed_now or is_bottom_track_now
            )

            if runtime_state["prev_is_active"] is None:
                runtime_state["prev_is_active"] = is_active
            just_turned_off = (runtime_state["prev_is_active"] is True and is_active is False)
            runtime_state["prev_is_active"] = is_active

            # Case 22 — 17:00 EST 가비지 컬렉션(GC) 실행 시 미보유 상태면 is_session_done=False, 트레일링/최저점 캐시 및 EWM 캐시 초기화
            if 17 <= now_est.hour < 19 and runtime_state["last_gc_date"] != today_str:
                runtime_state["last_gc_date"] = today_str
                ctx.in_memory_ordering_lock[symbol] = False
                ctx.idempotency_keys[symbol] = {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}

                if symbol in ctx.shared_rsi_state:
                    ctx.shared_rsi_state[symbol]["ewm_state"] = {}

                runtime_state["last_moc_tick"] = 0.0
                runtime_state["moc_dump_active"] = False
                runtime_state["fast_shield_mode"] = False
                runtime_state["last_known_qty"] = 0
                _reset_bottom_track_state(symbol)
                if holdings_qty == 0:
                    _reset_trailing_runtime_state(symbol)
                    await AssassinLedger.save_state(
                        symbol,
                        price=0.0,
                        target_sell_price=0.0,
                        is_session_done=False,
                        buy_order_id="",
                        sell_order_id="",
                        entry_session="",
                        entry_time=0.0,
                        nq_entry_price=0.0,
                        nq_entry_amp=0.0,
                        entry_trend="",
                        whipsaw_target_price=0.0,
                    )
                print(f"🧹 [GC {symbol}] 17:00 EST 윈도우 진입. 락 해제 및 자정 초기화 완료.", flush=True)
                await asyncio.sleep(60.0)
                continue

            try:
                is_open, _, sess_name, _ = await asyncio.wait_for(
                    ctx.client.is_market_open(), timeout=10.0
                )
            except Exception:
                is_open = True
                sess_name = "UNKNOWN"

            if not is_open and (
                sess_name and (sess_name.startswith("HOLIDAY") or sess_name in ["CLOSED", "UNKNOWN"])
            ):
                if sess_name.startswith("HOLIDAY") and symbol == "SOXS":
                    raw_reason = sess_name.split("|")[1] if "|" in sess_name else "미국 주식시장 정규 휴장"
                    reason = html.escape(HOLIDAY_TRANSLATIONS.get(raw_reason, raw_reason))
                    if last_holiday_notified_date != today_str and 400 <= est_time_int <= 405:
                        async with holiday_notify_lock:
                            if last_holiday_notified_date != today_str:
                                last_holiday_notified_date = today_str
                                await ctx.notify_tg(
                                    f"🛑 <b>[시스템 대기] 미국 주식시장 휴무 안내</b>\n"
                                    f"▫️ 사유: {reason}\n"
                                    "▫️ 조치: 금일 전술 가동 전면 차단 및 레이더망 휴식"
                                )
                await asyncio.sleep(60.0)
                continue

            # Case 01 & 초과 Case 36 — 19:00 EST 데이마켓 진입 시 논리적 거래일(익일 날짜) 기준으로 세션 ID 산출
            if 400 <= est_time_int < 930:
                hardcoded_session = "preMarket"
                base_h, base_m, base_date = 4, 0, now_est
            elif 930 <= est_time_int < 1600:
                hardcoded_session = "regularMarket"
                base_h, base_m, base_date = 9, 30, now_est
            elif 1600 <= est_time_int <= 1859:
                hardcoded_session = "afterMarket"
                base_h, base_m, base_date = 16, 0, now_est
            else:
                hardcoded_session = "dayMarket"
                base_h, base_m = 19, 0
                base_date = (now_est + timedelta(days=1)) if now_est.hour >= 19 else now_est

            session_baseline_est = base_date.replace(hour=base_h, minute=base_m, second=0, microsecond=0)
            current_session_id = f"{session_baseline_est.strftime('%Y%m%d_%H%M')}_{hardcoded_session}"

            if hardcoded_session != runtime_state["last_logged_session"]:
                if runtime_state["last_logged_session"]:
                    print(
                        f"🔄 [세션 전이 {symbol}] {runtime_state['last_logged_session']} ➡️ {hardcoded_session}",
                        flush=True,
                    )
                runtime_state["last_logged_session"] = hardcoded_session

            if now_est.hour != runtime_state["last_heartbeat_hour"]:
                print(
                    f"💓 [맥박 {symbol}] {now_est.strftime('%Y-%m-%d %H:%M:%S')} EST | "
                    f"세션: {hardcoded_session} | 활성: {is_active} | 잔고: {holdings_qty}주 | 모드: HA트레일링 & RE_BOT_TRAIL",
                    flush=True,
                )
                runtime_state["last_heartbeat_hour"] = now_est.hour

            # 목표 5 & Case 16, 24: 15:59 ~ 16:01 EST 제로-오버나이트 MOC 강제 청산
            moc_handled = await execute_moc_zero_overnight_plugin(
                ctx=ctx,
                symbol=symbol,
                now_est=now_est,
                is_active=is_active,
                sell_order_id=sell_order_id,
                runtime_state=runtime_state,
            )
            if moc_handled:
                continue

            # Case 83: 프리-클라이맥스/최저점추적 가속 구간이거나 매수 대상 종목이 과매도(<=30.0)일 때는
            # 단일 배치 API(get_current_prices_batch)로 SOXS와 SOXL 현재가를 동시 조회하여 시차 0.0ms 보장!
            need_batch_price_sync = bool(
                runtime_state["fast_shield_mode"]
                or (0.0 < cached_my_rsi <= PRE_SNIPER_RSI_THRESHOLD)
                or (cached_opp_rsi >= 78.0)
            )
            opp_current_price = 0.0
            if need_batch_price_sync and (is_premarket_sniper_window or is_daymarket_active_window):
                batch_prices = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
                current_price = float(batch_prices.get(symbol, 0.0))
                opp_current_price = float(batch_prices.get(opp_sym, 0.0))
                if current_price <= 0.0:
                    current_price = await ctx.client.get_current_price(symbol)
            else:
                current_price = await ctx.client.get_current_price(symbol)

            avg_price_dump = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price

            # 목표 5 & Case 50, 66, 67, 82, 86, 87 — 방향성 인지 NQ=F 매크로 3단 방어망 호출
            was_unregistered_manual = bool(
                holdings_qty > 0
                and not buy_order_id
                and entry_session not in ("PRE_SNIPER", "PRE_SNIPER_REARM", "WHIPSAW_1SH", "MANUAL")
            )
            macro_dumped = await execute_macro_defense_plugin(
                ctx=ctx,
                symbol=symbol,
                now_est=now_est,
                is_active=is_active,
                is_session_done=is_session_done,
                holdings_qty=holdings_qty,
                current_price=current_price,
                avg_price_dump=avg_price_dump,
                sell_order_id=sell_order_id,
                nq_entry_price=nq_entry_price,
                nq_entry_amp=nq_entry_amp,
                buy_order_id=buy_order_id,
                entry_session=entry_session,
                last_buy_price=last_buy_price,
            )
            if macro_dumped:
                continue

            if was_unregistered_manual:
                st_sync = await AssassinLedger.get_state(symbol)
                last_buy_price = st_sync[0]
                is_session_done = st_sync[4]
                entry_session = st_sync[8]
                nq_entry_price = st_sync[10]
                nq_entry_amp = st_sync[11]

            if not is_open or current_price <= 0.0:
                continue

            if current_session_id != last_session_id:
                opp_st_tr = await AssassinLedger.get_state(opp_sym)
                opp_buy_tr = await AssassinLedger.get_buy_order_id(opp_sym)
                is_same_day_transition = (
                    bool(last_session_id)
                    and len(last_session_id) >= 8
                    and last_session_id[:8] == current_session_id[:8]
                    and (
                        entry_session in ("CANCELED_MACRO", "WHIPSAW_1SH", "MANUAL", "PRE_SNIPER", "PRE_SNIPER_REARM", "STAGE3_STOP_TRACK")
                        or opp_st_tr[8] in ("CANCELED_MACRO", "WHIPSAW_1SH", "MANUAL", "PRE_SNIPER", "PRE_SNIPER_REARM", "STAGE3_STOP_TRACK")
                        or bool(buy_order_id)
                        or bool(opp_buy_tr)
                        or bool(sell_order_id)
                        or bool(opp_st_tr[7])
                        or target_sell_price > 0.0
                        or opp_st_tr[6] > 0.0
                        or holdings_qty > 0
                        or ctx.shared_holdings.get(opp_sym, 0) > 0
                    )
                )
                if is_same_day_transition:
                    await AssassinLedger.save_state(symbol, last_session_id=current_session_id)
                elif holdings_qty == 0:
                    can_reset = True
                    if buy_order_id:
                        try:
                            od = await ctx.client.get_order_detail(buy_order_id)
                            if od.get("status") in ["PENDING", "PARTIAL_FILLED"]:
                                can_reset = False
                        except Exception:
                            can_reset = False
                    if can_reset:
                        _reset_trailing_runtime_state(symbol)
                        _reset_bottom_track_state(symbol)
                        await AssassinLedger.save_state(
                            symbol,
                            price=0.0,
                            target_sell_price=0.0,
                            last_session_id=current_session_id,
                            is_session_done=False,
                            buy_order_id="",
                            sell_order_id="",
                            entry_session="",
                            entry_time=0.0,
                            nq_entry_price=0.0,
                            nq_entry_amp=0.0,
                            entry_trend="",
                            whipsaw_target_price=0.0,
                        )
                        is_session_done = False
                        target_sell_price = 0.0
                        buy_order_id = ""
                        sell_order_id = ""
                        entry_session = ""
                else:
                    await AssassinLedger.save_state(symbol, last_session_id=current_session_id)
                last_session_id = current_session_id

            # Case 78: 04:00 ~ 09:29 EST 프리장 진입 시 혹시 미결된 데이장 [모의] 포지션이 있다면 즉시 무음 클리어 (본진 보호)
            if 400 <= est_time_int <= 929:
                await settle_daymarket_sim_before_premarket(
                    ctx=ctx, symbol=symbol, now_est=now_est, current_price=current_price, silent=True
                )

            # Case 80: 04:07:00 EST 도달 시 듀얼 휩소 노출 작전(WHIPSAW_1SH) 타임쉴드 만료 정리
            if (407 <= est_time_int <= 929) and entry_session == "WHIPSAW_1SH":
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
                    cancel_helper_fn=_cancel_open_orders_and_wait_sellable,
                )
                if whip_handled:
                    ledger_after_cut = await AssassinLedger.get_state(symbol)
                    is_session_done = ledger_after_cut[4]
                    target_sell_price = ledger_after_cut[6]
                    sell_order_id = ledger_after_cut[7]
                    entry_session = ledger_after_cut[8]

            # 초과 Case 52, 71, 84 — 본진 프리장 초중반 골든타임 (04:07 ~ 08:14 EST) 교차 검증 자가 치유(Self-Healing) 가드
            if (
                407 <= est_time_int <= 814
                and is_session_done
                and holdings_qty == 0
                and not buy_order_id
                and not sell_order_id
                and target_sell_price <= 0.0
                and last_buy_price <= 0.0
                and entry_session != "CANCELED_MACRO"
            ):
                opp_state_chk = await AssassinLedger.get_state(opp_sym)
                opp_buy_chk = await AssassinLedger.get_buy_order_id(opp_sym)
                opp_busy = (
                    ctx.shared_holdings.get(opp_sym, 0) > 0
                    or bool(opp_buy_chk)
                    or bool(opp_state_chk[7])
                    or opp_state_chk[6] > 0.0
                    or opp_state_chk[0] > 0.0
                    or opp_state_chk[8] == "CANCELED_MACRO"
                )
                if not opp_busy:
                    await AssassinLedger.save_state(symbol, is_session_done=False, whipsaw_target_price=0.0)
                    is_session_done = False
                    print(
                        f"♻️ [프리장 자가 치유 {symbol}] 비정상 마감 락(is_session_done) 자동 해제 완료 ➡️ 04:07~08:14 양방향 대칭 감시 복귀",
                        flush=True,
                    )

            # 목표 3 & 초과 Case 48, 53, 84, 87 — 08:15 EST 거시지표 발표 15분 전 선제 컷오프 (08:15 ~ 15:59 EST)
            is_session_done, buy_order_id = await enforce_0930_regular_cutoff_plugin(
                ctx=ctx,
                symbol=symbol,
                est_time_int=est_time_int,
                is_session_done=is_session_done,
                buy_order_id=buy_order_id,
            )

            # Case 73 & Case 83: 실시간 틱 RSI(compute_realtime_tick_rsi) 동시 합성
            fallback_rsi = float(sym_rsi_dict.get("value", 0.0))
            if current_price > 0.0:
                live_rsi_val = compute_realtime_tick_rsi(
                    live_price=current_price,
                    ewm_state=sym_rsi_dict.get("ewm_state", {}),
                    fallback_rsi=fallback_rsi,
                )
                sym_rsi_dict["value"] = live_rsi_val
            else:
                live_rsi_val = fallback_rsi

            opp_fallback_rsi = float(opp_rsi_dict.get("value", 0.0))
            if opp_current_price > 0.0:
                opp_live_rsi_val = compute_realtime_tick_rsi(
                    live_price=opp_current_price,
                    ewm_state=opp_rsi_dict.get("ewm_state", {}),
                    fallback_rsi=opp_fallback_rsi,
                )
                opp_rsi_dict["value"] = opp_live_rsi_val
            else:
                opp_live_rsi_val = opp_fallback_rsi

            if (
                (is_premarket_sniper_window or is_daymarket_active_window)
                and is_active
                and not is_session_done
                and holdings_qty == 0
                and (0.0 < live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
                and opp_current_price <= 0.0
            ):
                try:
                    opp_p_ondemand = await ctx.client.get_current_price(opp_sym)
                    if opp_p_ondemand > 0.0:
                        opp_live_rsi_val = compute_realtime_tick_rsi(
                            live_price=opp_p_ondemand,
                            ewm_state=opp_rsi_dict.get("ewm_state", {}),
                            fallback_rsi=opp_fallback_rsi,
                        )
                        opp_rsi_dict["value"] = opp_live_rsi_val
                except Exception:
                    pass

            # 포지션 보유 중(수동 진입 포함) 하이킨 아시 트레일링 점화(armed) 또는 점화 근접(+0.80% 이상) 시 0.4초 고주파 부스터 가동
            is_trailing_booster = bool(
                is_active
                and holdings_qty > 0
                and entry_session != "WHIPSAW_1SH"
                and (
                    bool(get_trailing_runtime_status(symbol).get("armed", False))
                    or (avg_price_dump > 0.0 and current_price >= avg_price_dump * 1.008)
                )
            )
            is_bottom_track_booster = bool(
                is_active
                and is_premarket_sniper_window
                and get_bottom_track_status(symbol).get("active", False)
            )

            # 차기 루프 대기 시간(0.4초 부스터)을 최신 합성 RSI, 트레일링, 최저점 추적 상태 기준으로 즉시 업데이트 (Case 28, 83, 85, 87)
            runtime_state["fast_shield_mode"] = bool(
                is_trap_waiting
                or is_trailing_booster
                or is_bottom_track_booster
                or (
                    is_active
                    and (is_premarket_sniper_window or is_daymarket_active_window)
                    and not is_session_done
                    and holdings_qty == 0
                    and ctx.shared_holdings.get(opp_sym, 0) == 0
                    and not buy_order_id
                    and (
                        live_rsi_val >= PRE_CLIMAX_BOOSTER_RSI
                        or opp_live_rsi_val >= PRE_CLIMAX_BOOSTER_RSI
                        or (0.0 < live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD and opp_live_rsi_val >= 78.0)
                        or (0.0 < opp_live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD and live_rsi_val >= 78.0)
                    )
                )
            )

            # 본진 프리장 골든타임(04:07 ~ 08:14 EST) 1차 슬리피지 매수 덫 대기 중 조건부 30초 타임쉴드 감시
            if (
                is_active
                and is_premarket_sniper_window
                and holdings_qty == 0
                and buy_order_id
                and entry_session == "PRE_SNIPER"
            ):
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
                    rsi_val=live_rsi_val,
                )

            # 목표 3·4 & 초과 Case 46, 49, 51, 52, 76, 84, 87: 잔고 0주 복귀 시 통합 실수익 복리 정산 및 08:14 EST까지 재출동 릴리스
            if holdings_qty == 0 and (
                target_sell_price > 0.0 or buy_order_id or sell_order_id or last_buy_price > 0.0
            ):
                cleared = await reconcile_zero_holdings_exit_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    last_buy_price=last_buy_price,
                    buy_order_id=buy_order_id,
                    sell_order_id=sell_order_id,
                    is_active=is_active,
                    fallback_qty=float(runtime_state.get("last_known_qty", 0)),
                )
                if cleared:
                    runtime_state["last_known_qty"] = 0
                    continue

            # 데이장 (19:00 ~ 03:59 EST) 프리장 동일 조건 양방향 대칭 클라이맥스 2단계 모의 관측 알림 가동 (03:58~03:59 묵음 내장)
            if is_active and is_daymarket_window and current_price > 0.0:
                await execute_daymarket_rsi_alert_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_price=current_price,
                    rsi_val=live_rsi_val,
                    vwap_val=float(sym_rsi_dict.get("vwap", 0.0)),
                )

            # Case 79: 03:58 ~ 04:06:59 EST (9분간) 데이장 누적 VWAP 상단 종목 기준 1% 역산 듀얼 1주 무비용 매수 덫 발사
            if (
                is_active
                and is_whipsaw_window
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and current_price > 0.0
            ):
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
                    day_vwap=float(sym_rsi_dict.get("vwap", 0.0)),
                )
                buy_order_id = await AssassinLedger.get_buy_order_id(symbol)
                if buy_order_id:
                    ledger_after_w = await AssassinLedger.get_state(symbol)
                    is_session_done = ledger_after_w[4]
                    entry_session = ledger_after_w[8]
                    entry_time = ledger_after_w[9]

            # NEW: 초과 Case 87 — 본진 프리장 초중반 골든타임 (04:07 ~ 08:14 EST) 내
            #      3차 역행 안전망 손절 후 '손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 재진입 (🪤 RE_BOT_TRAIL)' 감시 및 타격!
            if (
                is_active
                and not is_session_done
                and is_premarket_sniper_window
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and get_bottom_track_status(symbol).get("active", False)
            ):
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
                    rsi_val=live_rsi_val,
                    opp_rsi_val=opp_live_rsi_val,
                )
                if rebound_fired:
                    buy_order_id = await AssassinLedger.get_buy_order_id(symbol)
                    ledger_after_rb = await AssassinLedger.get_state(symbol)
                    is_session_done = ledger_after_rb[4]
                    entry_session = ledger_after_rb[8]
                    entry_time = ledger_after_rb[9]

            # 목표 3 & Case 15, 25, 52, 53, 54, 59, 71, 81, 83, 84, 87 — 본진 프리장 초중반 골든타임 (04:07 ~ 08:14 EST)
            # 양방향 대칭 클라이맥스(매수종목 RSI <= 30.0 AND 반대종목 RSI >= 79.9[80.0 클라이맥스 인지]) 포착 시 1차 슬리피지 매수 덫 즉시 투하
            if (
                is_active
                and not is_session_done
                and is_premarket_sniper_window
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and not get_bottom_track_status(symbol).get("active", False)
                and (0.0 < live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
                and (opp_live_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD)
            ):
                await execute_premarket_rsi_sniper_strike_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_session_id=current_session_id,
                    current_price=current_price,
                    budget=budget,
                    target_profit_rate=target_profit_rate,
                    rsi_val=live_rsi_val,
                    opp_rsi_val=opp_live_rsi_val,
                )

            # 목표 4 & 초과 Case 50, 58, 64, 85: 매수 전량 체결(본진 및 수동 진입 공통) 확인 즉시 상단 안전 캡(+5.00%) 선장전 및
            # +1.00% 도달 시 하이킨 아시(Heikin-Ashi) + 고점 트레일링(HA_HYB_1.0) 0.1초 즉시 정정 스윕 가동
            await manage_mechanical_profit_trap_plugin(
                ctx=ctx,
                symbol=symbol,
                now_est=now_est,
                current_session_id=current_session_id,
                is_active=is_active,
                just_turned_off=just_turned_off,
                holdings_qty=holdings_qty,
                holdings_detail=holdings_detail,
                last_buy_price=last_buy_price,
                target_profit_rate=target_profit_rate,
                target_sell_price=target_sell_price,
                buy_order_id=buy_order_id,
                sell_order_id=sell_order_id,
                entry_session=entry_session,
                runtime_state=runtime_state,
                current_price=current_price,
            )

        except Exception as e:
            print(f"🚨 [듀얼 스나이퍼 루프 {symbol}] 감시망 붕괴 방어: {e}", flush=True)


async def main():
    session = AiohttpSession(timeout=10.0)
    bot = Bot(token=TELEGRAM_BOT_TOKEN, session=session)
    dp = Dispatcher()

    api_client = TossApiClient(client_id=TOSS_CLIENT_ID, client_secret=TOSS_CLIENT_SECRET)

    try:
        await api_client.fetch_account_seq()
    except Exception as e:
        print(f"🚨 [초기 기동 방어] 계좌 연동 지연 (자동 재시도 예정): {e}", flush=True)

    # 기동 시 SOXS 기준 통합 시드·단일 트레일링 설정·복리모드·전원을 SOXL 원장에도 동기화
    try:
        boot_s = await AssassinLedger.get_state("SOXS")
        boot_c = await AssassinLedger.get_is_compound("SOXS")
        await AssassinLedger.sync_global_settings(
            budget=float(boot_s[1] or 100.0),
            target_profit_rate=AssassinLedger.DEFAULT_TARGET_PROFIT_RATE,
            is_compound=boot_c,
            is_active=bool(boot_s[5]),
        )
    except Exception:
        pass

    inject_dependencies(api_client, ADMIN_CHAT_ID, wakeup_event, shared_rsi_state)
    dp.include_router(router)

    plugin_ctx = SniperPluginContext(
        client=api_client,
        bot=bot,
        chat_id=ADMIN_CHAT_ID,
        wakeup_event=wakeup_event,
        in_memory_ordering_lock=in_memory_ordering_lock,
        dual_strike_mutex=dual_strike_mutex,
        shared_holdings=shared_holdings,
        idempotency_keys=idempotency_keys,
        nq_dynamic_floor=NQ_DYNAMIC_FLOOR,
        shared_rsi_state=shared_rsi_state,
    )

    asyncio.create_task(api_client.token_renewal_loop())
    asyncio.create_task(auto_update_loop(bot, ADMIN_CHAT_ID))

    for sym in TRADE_SYMBOLS:
        asyncio.create_task(assassin_loop(plugin_ctx, sym))

    for sym in RSI_SYMBOLS:
        asyncio.create_task(record_candles_loop(api_client, sym))
        asyncio.create_task(rsi_radar_loop(plugin_ctx, sym))

    print("시스템 코어: 04:07~08:14 골든타임 & HA트레일링(HA_HYB_1.0) & 최저점 반등 재진입(RE_BOT_TRAIL) 결속 완료...", flush=True)

    # 기동 완료 시 단일 상태 메시지만 전송 (Case 38)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text="✅ <b>정상작동 중입니다.</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    while True:
        try:
            await dp.start_polling(bot)
        except Exception as e:
            print(f"🚨 [통신 붕괴 방어] 5초 후 치유 재가동: {e}", flush=True)
            await asyncio.sleep(5.0)
        else:
            break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
