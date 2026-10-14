# =====================================================================
# FILE: main.py
# 목적: 02:58 EST 데이장 마감 직전 숏(SOXS) 단일 방향 스나이퍼 코어 오케스트레이터
# 구조: sniper_plugin.py 전술 플러그인과 SniperPluginContext로 결속된 슬림 아키텍처
# =====================================================================

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
from quant_engine import AssassinLedger, AVWAPEngine, MacroDataCache, calculate_rsi_1m
from tg_router import router, inject_dependencies
from candle_recorder import record_candles_loop
from sniper_plugin import (
    SniperPluginContext,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_post_0258_cutoff_plugin,
    reconcile_zero_holdings_exit_plugin,
    execute_0258_sniper_strike_plugin,
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

SYMBOLS = ("SOXS",)
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

in_memory_ordering_lock = {"SOXS": False}
dual_strike_mutex = asyncio.Lock()
shared_holdings = {"SOXS": 0}
shared_rsi_state = {"SOXS": {"value": 0.0, "level": 0, "vwap": 0.0}}

idempotency_keys = {
    "SOXS": {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None},
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
            return -1, "", "Subprocess Timeout Expired (네트워크 교착 상태 방어)"

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
            if 4 <= now_est.hour < 17:
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
    """
    NEW: RSI + VWAP 24시간 읽기 전용 레이더망. 
    초과 Case 72 방어: 데이마켓(19:00 EST) 시작 시점 VWAP 0점 롤오버 하드 락온.
    """
    while True:
        try:
            now_est = datetime.now(EST_TZ)
            is_open, _, sess_name, _ = await ctx.client.is_market_open()
            
            if is_open and sess_name != "CLOSED":
                data = await ctx.client.get_1m_candles_pagination(symbol, count=200)
                candles = data.get("candles", [])
                
                if candles:
                    # MODIFIED: 19:00 EST 기점 24시간 논리적 거래일 롤오버 (Case 72 세션 오염 방어)
                    if now_est.hour >= 19:
                        session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
                    else:
                        session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)
                        
                    current_price = await ctx.client.get_current_price(symbol)
                    
                    rsi_val, vwap_val = await asyncio.to_thread(calculate_rsi_1m, candles, session_start_est)
                    
                    await execute_rsi_monitoring_plugin(
                        ctx=ctx, symbol=symbol, rsi_val=rsi_val, vwap_val=vwap_val, current_price=current_price
                    )
                    
        except Exception as e:
            print(f"🚨 [RSI 레이더망 {symbol}] 붕괴 방어: {e}", flush=True)
            
        await asyncio.sleep(60.0)

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
        now_est_pre = datetime.now(EST_TZ)
        est_hm_pre = now_est_pre.hour * 100 + now_est_pre.minute
        loop_timeout = 0.4 if est_hm_pre == 258 else 1.5

        try:
            await asyncio.wait_for(ctx.wakeup_event.wait(), timeout=loop_timeout)
            ctx.wakeup_event.clear()
        except asyncio.TimeoutError:
            pass

        try:
            now_est = datetime.now(EST_TZ)
            est_time_int = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

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

            if 17 <= now_est.hour < 19 and runtime_state["last_gc_date"] != today_str:
                runtime_state["last_gc_date"] = today_str
                ctx.in_memory_ordering_lock[symbol] = False
                ctx.idempotency_keys[symbol] = {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}
                
                # 17:00 EST 가비지 컬렉션(GC) 시점: RSI 경보망 강제 릴리스(초기화)
                ctx.shared_rsi_state[symbol]["level"] = 0
                
                runtime_state["last_moc_tick"] = 0.0
                runtime_state["moc_dump_active"] = False
                if holdings_qty == 0:
                    await AssassinLedger.save_state(
                        symbol,
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
                base_date = now_est if now_est.hour >= 19 else now_est - timedelta(days=1)

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
                    f"세션: {hardcoded_session} | 활성: {is_active} | 잔고: {holdings_qty}주",
                    flush=True,
                )
                runtime_state["last_heartbeat_hour"] = now_est.hour

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
                        is_session_done
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
                else:
                    await AssassinLedger.save_state(symbol, last_session_id=current_session_id)
                last_session_id = current_session_id

            is_session_done = await enforce_post_0258_cutoff_plugin(
                symbol=symbol,
                est_time_int=est_time_int,
                is_session_done=is_session_done,
            )

            if holdings_qty == 0 and (
                target_sell_price > 0.0 or buy_order_id or sell_order_id or last_buy_price > 0.0
            ):
                await reconcile_zero_holdings_exit_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    last_buy_price=last_buy_price,
                    buy_order_id=buy_order_id,
                    sell_order_id=sell_order_id,
                )
                continue

            is_0258_strike = (est_time_int == 258 and now_est.second >= 0)
            if (
                is_active
                and not is_session_done
                and is_0258_strike
                and holdings_qty == 0
                and not buy_order_id
            ):
                await execute_0258_sniper_strike_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_session_id=current_session_id,
                    current_price=current_price,
                    budget=budget,
                    target_profit_rate=target_profit_rate,
                )

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

    for sym in SYMBOLS:
        asyncio.create_task(assassin_loop(plugin_ctx, sym))
        asyncio.create_task(record_candles_loop(api_client, sym))
        asyncio.create_task(rsi_radar_loop(plugin_ctx, sym))

    print("시스템 코어 및 02:58 EST 숏(SOXS) 단방향 스나이퍼 플러그인 결속 완료...", flush=True)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=(
                "✅ <b>[시스템 기동 완료 — 02:58 EST 숏(SOXS) 스나이퍼]</b>\n"
                "▫️ 타겟: <b>SOXS(숏)</b> 배타적 1회 출동\n"
                "▫️ 전략: NQ=F 진폭 2.0% 미만 시 02:58 EST 실시간 현재가 지정가 매수\n"
                "▫️ 방어: 3단 매크로 폭격 덤핑 & 24시간 패닉 셀 레이더망(19:00 EST 롤오버) 상시 가동\n"
                "▫️ 익절: 시드 전량 체결 즉시 팩트 평단가 기준 +0.64% 매도 하드 락온"
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
