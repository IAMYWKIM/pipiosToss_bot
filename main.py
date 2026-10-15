# =====================================================================
# FILE: main.py
# 목적: 프리장(04:00~09:29 EST) SOXS 단방향 RSI<=30 무한 출동 스나이퍼 코어 오케스트레이터
# 구조: sniper_plugin.py 전술 플러그인과 SniperPluginContext로 결속된 슬림 아키텍처
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 03:58/04:07 암살자 플러그인 및 FORCE_BUY 데드코드 영구 소각
# MODIFIED: 제3헌법 의거 RSI<=20/10 푸시 알림 폐기에 따른 shared_rsi_state 내 불필요한 level 상태 변수 영구 소각
# MODIFIED: 프리장 스나이퍼 보호를 위해 무인 업데이트 락다운 시간대를 03:30~17:00 EST로 전면 확장 (Case 07)
# NEW: 09:30 EST 정규장 컷오프 및 프리장 미체결 매수 즉시 파기 플러그인(enforce_0930_regular_cutoff_plugin) 메인 루프 결속 (Case 48, 53)
# NEW: 프리장(04:00~09:29 EST) SOXS RSI<=30.0 즉시 요격 플러그인(execute_premarket_rsi_sniper_strike_plugin) 원자적 편입 (Case 25, 52, 59, 70)
# NEW: 20초 폴링 사각지대(원인 D) 원천 소각 — 프리장 SOXS 5초 캔들 스캔 가속 및 1.5초 메인 루프 실시간 틱 RSI(compute_realtime_tick_rsi) 즉시 합성 결속
# MODIFIED: 17:00 EST GC 가동 시 is_session_done=False 초기화 및 19:00 EST 논리적 거래일 롤오버 적용으로 전일 마감 락 이월 원천 차단 (Case 01, 22, 36)
# NEW: 프리장(04:00~09:29 EST) 미보유·무주문·비매크로차단 상태에서 구버전 잔재 잠금(is_session_done=True) 감지 시 즉시 해제하는 자가 치유(Self-Healing) 가드 주입 (Case 52)
# NEW: 시스템 기동 시 장부 내 선택된 목표 익절률(+0.64% vs +1.00%) 및 복리/단리 시드 모드를 동적으로 읽어 부팅 타전에 반영

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
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
    reconcile_zero_holdings_exit_plugin,
    execute_premarket_rsi_sniper_strike_plugin,
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

TRADE_SYMBOL = "SOXS"
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

in_memory_ordering_lock = {TRADE_SYMBOL: False}
dual_strike_mutex = asyncio.Lock()
shared_holdings = {TRADE_SYMBOL: 0}

# MODIFIED: RSI<=20/10 경보 상태 머신 폐기에 따라 불필요한 level 키 소각 및 ewm_state 슬롯 유지
shared_rsi_state = {
    sym: {"value": 0.0, "vwap": 0.0, "ewm_state": {}} for sym in RSI_SYMBOLS
}

idempotency_keys = {
    TRADE_SYMBOL: {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None},
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
            # Case 07 프리장 개장 전부터 애프터장 종료(03:30~17:00 EST)까지 무인 업데이트 전면 차단
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

        # 프리장 골든타임(04:00~09:29 EST) 중 SOXS 캔들 스캔 주기를 5초로 가속하여 EWM 기저 상태 초고속 동기화
        now_est_chk = datetime.now(EST_TZ)
        est_hm_chk = now_est_chk.hour * 100 + now_est_chk.minute
        sleep_sec = 5.0 if (symbol == "SOXS" and 400 <= est_hm_chk <= 929) else 60.0
        await asyncio.sleep(sleep_sec)


async def assassin_loop(ctx: SniperPluginContext, symbol: str):
    global last_holiday_notified_date
    runtime_state = {
        "last_moc_tick": 0.0,
        "moc_dump_active": False,
        "last_heartbeat_hour": -1,
        "last_logged_session": "",
        "prev_is_active": None,
        "last_gc_date": "",
        "last_error_msg": "",
        "last_trap_error_msg": "",
    }

    while True:
        # 초과 Case 28: 평시 1.5초 비동기 대기망 구축 및 wakeup_event 격발 시 즉시 기상
        try:
            await asyncio.wait_for(ctx.wakeup_event.wait(), timeout=1.5)
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

                if runtime_state["last_error_msg"] != "":
                    await ctx.notify_tg(
                        f"✅ <b>[스나이퍼 {symbol}] 토스증권 API 통신망 복구 완료</b>\n"
                        "▫️ 서버 응답 정상화. 감시를 재개합니다."
                    )
                    runtime_state["last_error_msg"] = ""
            except Exception as e:
                err_str = str(e)
                if err_str != runtime_state["last_error_msg"]:
                    await ctx.notify_tg(
                        f"🚨 <b>[스나이퍼 {symbol}] 통신 붕괴 (유령 잔고 방어)</b>\n"
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

            if runtime_state["prev_is_active"] is None:
                runtime_state["prev_is_active"] = is_active
            just_turned_off = (runtime_state["prev_is_active"] is True and is_active is False)
            runtime_state["prev_is_active"] = is_active

            # Case 22 — 17:00 EST 가비지 컬렉션(GC) 실행 시 미보유 상태면 is_session_done=False 및 EWM 캐시 초기화
            if 17 <= now_est.hour < 19 and runtime_state["last_gc_date"] != today_str:
                runtime_state["last_gc_date"] = today_str
                ctx.in_memory_ordering_lock[symbol] = False
                ctx.idempotency_keys[symbol] = {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}

                for r_sym in ctx.shared_rsi_state:
                    ctx.shared_rsi_state[r_sym]["ewm_state"] = {}

                runtime_state["last_moc_tick"] = 0.0
                runtime_state["moc_dump_active"] = False
                if holdings_qty == 0:
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
                if sess_name.startswith("HOLIDAY"):
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
                    f"세션: {hardcoded_session} | 활성: {is_active} | 잔고: {holdings_qty}주 | 목표: +{target_profit_rate:.2f}%",
                    flush=True,
                )
                runtime_state["last_heartbeat_hour"] = now_est.hour

            # 목표 4 & Case 16, 24: 15:59~16:01 EST 제로-오버나이트 MOC 강제 청산
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

            current_price = await ctx.client.get_current_price(symbol)
            avg_price_dump = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price

            # 목표 3 & Case 66, 67: NQ=F 매크로 3단 방어망 (1차 진입차단/미체결파기, 2·3차 보유물량 손실권 덤핑)
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
            )
            if macro_dumped:
                continue

            if not is_open or current_price <= 0.0:
                continue

            if current_session_id != last_session_id:
                is_same_day_transition = (
                    bool(last_session_id)
                    and len(last_session_id) >= 8
                    and last_session_id[:8] == current_session_id[:8]
                    and (
                        entry_session == "CANCELED_MACRO"
                        or bool(buy_order_id)
                        or bool(sell_order_id)
                        or target_sell_price > 0.0
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
                        )
                        is_session_done = False
                        target_sell_price = 0.0
                        buy_order_id = ""
                        sell_order_id = ""
                        entry_session = ""
                else:
                    await AssassinLedger.save_state(symbol, last_session_id=current_session_id)
                last_session_id = current_session_id

            # 초과 Case 52 — 프리장(04:00~09:29 EST) 무한 출동 자가 치유(Self-Healing) 가드
            if (
                400 <= est_time_int <= 929
                and is_session_done
                and holdings_qty == 0
                and not buy_order_id
                and not sell_order_id
                and target_sell_price <= 0.0
                and last_buy_price <= 0.0
                and entry_session != "CANCELED_MACRO"
            ):
                await AssassinLedger.save_state(symbol, is_session_done=False)
                is_session_done = False
                print(
                    f"♻️ [프리장 자가 치유 {symbol}] 비정상 마감 락(is_session_done) 자동 해제 완료 ➡️ 프리장 RSI≤30 무한 감시 복귀",
                    flush=True,
                )

            # 목표 1 & 초과 Case 48, 53 — 09:30 EST 정규장 개장 정각 신규 매수 마감 및 미체결 매수 즉시 파기
            is_session_done, buy_order_id = await enforce_0930_regular_cutoff_plugin(
                ctx=ctx,
                symbol=symbol,
                est_time_int=est_time_int,
                is_session_done=is_session_done,
                buy_order_id=buy_order_id,
            )

            # 목표 1·2 & 초과 Case 46, 49, 51, 52: 잔고 0주 복귀 시 정산 및 프리장 내 무한 재출동 릴리스
            if holdings_qty == 0 and (
                target_sell_price > 0.0 or buy_order_id or sell_order_id or last_buy_price > 0.0
            ):
                await reconcile_zero_holdings_exit_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    last_buy_price=last_buy_price,
                    buy_order_id=buy_order_id,
                    sell_order_id=sell_order_id,
                    is_active=is_active,
                )
                continue

            # 1.5초 메인 루프 실시간 틱 RSI(compute_realtime_tick_rsi) 즉시 합성
            is_premarket_window = (400 <= est_time_int <= 929)
            sym_rsi_dict = ctx.shared_rsi_state.setdefault(
                symbol, {"value": 0.0, "vwap": 0.0, "ewm_state": {}}
            )
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

            # 목표 1 & Case 15, 25, 52, 53, 59 — 프리장(04:00~09:29 EST) SOXS RSI<=30.0 실거래 지정가 매수 발사
            if (
                is_active
                and not is_session_done
                and is_premarket_window
                and holdings_qty == 0
                and not buy_order_id
                and (0.0 < live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
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
                )

            # 목표 2 & 초과 Case 50, 58, 64: 매수 전량 체결 확인 즉시 선택된 목표 익절가(+0.64% / +1.00%) 지정가 매도 덫(TRAP) 하드 락온
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
            )

        except Exception as e:
            print(f"🚨 [스나이퍼 루프 {symbol}] 감시망 붕괴 방어: {e}", flush=True)


async def main():
    session = AiohttpSession(timeout=10.0)
    bot = Bot(token=TELEGRAM_BOT_TOKEN, session=session)
    dp = Dispatcher()

    api_client = TossApiClient(client_id=TOSS_CLIENT_ID, client_secret=TOSS_CLIENT_SECRET)

    try:
        await api_client.fetch_account_seq()
    except Exception as e:
        print(f"🚨 [초기 기동 방어] 계좌 연동 지연 (자동 재시도 예정): {e}", flush=True)

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

    asyncio.create_task(assassin_loop(plugin_ctx, TRADE_SYMBOL))

    for sym in RSI_SYMBOLS:
        asyncio.create_task(record_candles_loop(api_client, sym))
        asyncio.create_task(rsi_radar_loop(plugin_ctx, sym))

    print("시스템 코어: 프리장(04:00~09:29 EST) SOXS 단방향 RSI<=30 1.5초 틱 스나이퍼 결속 완료...", flush=True)

    try:
        boot_state = await AssassinLedger.get_state(TRADE_SYMBOL)
        boot_comp = await AssassinLedger.get_is_compound(TRADE_SYMBOL)
        boot_budget = float(boot_state[1] or 100.0)
        boot_rate = AssassinLedger.normalize_target_rate(boot_state[2])
        boot_net_rate = AssassinLedger.get_net_compound_rate(boot_rate)
        comp_desc = (
            f"📈 복리 모드 (익절 시 +{boot_net_rate:.2f}% 증액 · 손절 시 수수료포함 차감)"
            if boot_comp
            else "💵 단리 모드 (시드 고정)"
        )

        await bot.delete_webhook(drop_pending_updates=True)
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=(
                "✅ <b>[시스템 기동 완료 — 프리장 SOXS 단방향 RSI≤30 실거래 스나이퍼]</b>\n"
                "▫️ 타겟: <b>SOXS(숏)</b> 배타적 실물 자산 투입 (SOXL 매매 영구 차단)\n"
                "▫️ 관제: <b>5초 고해상도 캔들 동기화 + 1.5초 실시간 틱 RSI 합성</b> 상시 가동\n"
                "▫️ 타격: <b>프리장(04:00~09:29 EST)</b> NQ=F 진폭 2.0% 미만 & <b>SOXS 실시간 RSI≤30.0</b> 포착 시 현재가 지정가(LIMIT) 즉시 매수\n"
                f"▫️ 익절: 현재 설정 <b>+{boot_rate:.2f}% (순수익 +{boot_net_rate:.2f}%)</b> · 제어반(/settlement)에서 <b>+0.64% / +1.00%</b> 즉시 전환 가능\n"
                f"▫️ 시드: <b>${boot_budget:,.2f}</b> · {comp_desc}\n"
                "▫️ 마감: <b>09:30 EST</b> 정규장 개장 정각 신규 매수 마감 및 미체결 매수 주문 즉각 파기\n"
                "▫️ 방어: <b>NQ=F 3단 매크로 손절 덤핑</b> & <b>15:59 EST 제로-오버나이트 MOC(선취소+정정 스윕)</b> 상시 가동"
            ),
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
