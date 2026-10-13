# =====================================================================
# FILE: main.py
# 목적: 09:30 EST 정규장 개장 NQ=F 10분봉 추세 양방향(SOXL/SOXS) 스나이퍼 코어 오케스트레이터
# 구조: sniper_plugin.py 전술 플러그인과 SniperPluginContext로 결속된 슬림 아키텍처
# =====================================================================
# MODIFIED: 제3헌법 & Case 43 방어 - 구 04:01 프리장 틱 다수결/휩소 바이패스 데드코드 전면 소각
# NEW: Case 09, Case 52, Case 54 방어 - SOXL/SOXS 양방향 병렬 루프 및 dual_strike_mutex 전역 타격 락 결속
# NEW: Case 69 방어 - 3차 방어망 동적 임계치 하한선(NQ_DYNAMIC_FLOOR = 0.8) 상단 상수 하드 락온
# MODIFIED: 플러그인 아키텍처 분리 - 전술 타격/방어/청산/덫 로직을 sniper_plugin.py로 모듈화 연결

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
from quant_engine import AssassinLedger, AVWAPEngine, MacroDataCache
from tg_router import router, inject_dependencies
from candle_recorder import record_candles_loop
from sniper_plugin import (
    SniperPluginContext,
    fetch_full_session_candles,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_post_0930_cutoff_plugin,
    reconcile_zero_holdings_exit_plugin,
    execute_0930_sniper_strike_plugin,
    manage_mechanical_profit_trap_plugin,
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

# NEW: Case 09 & Case 69 - 양방향 감시 타겟 및 3차 방어망 동적 임계치 하한선(0.8%) 상수 락온
SYMBOLS = ("SOXL", "SOXS")
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

# MODIFIED: Case 54 방어 - SOXL/SOXS 양방향 비동기 주문 락, 잔고 상태, 멱등성 키 레지스트리
in_memory_ordering_lock = {"SOXL": False, "SOXS": False}
dual_strike_mutex = asyncio.Lock()
shared_holdings = {"SOXL": 0, "SOXS": 0}

idempotency_keys = {
    "SOXL": {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None},
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
            out, err = proc.communicate(timeout=20)
            return proc.returncode, out.strip(), err.strip()
        except subprocess.TimeoutExpired:
            proc.kill()
            return -1, "", "Subprocess Timeout Expired"

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

    # MODIFIED: 분리된 sniper_plugin.py를 포함한 전체 모듈 프리플라이트 컴파일 검증
    try:
        for mod_file in (
            "main.py",
            "sniper_plugin.py",
            "tg_router.py",
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
            # Case 07: 장중 업데이트 락다운 (04:00 ~ 16:59 EST 전면 차단)
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


async def assassin_loop(ctx: SniperPluginContext, symbol: str):
    """
    MODIFIED: SOXL 및 SOXS 각각 독립 실행되는 09:30 오프닝 스나이퍼 메인 상태 루프.
    세부 전술 액션은 sniper_plugin.py의 플러그인 함수로 위임하여 슬림화 및 단일 책임을 달성한다.
    """
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
        try:
            await asyncio.wait_for(ctx.wakeup_event.wait(), timeout=1.5)
            ctx.wakeup_event.clear()
        except asyncio.TimeoutError:
            pass

        try:
            now_est = datetime.now(EST_TZ)
            est_time_int = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

            # 1. 실시간 잔고 프로빙 및 유령 잔고 통신 에러 방어 (Case 18, Case 29)
            try:
                holdings_detail = await ctx.client.get_symbol_holdings_detail(symbol)
                holdings_qty = int(math.floor(holdings_detail["qty"]))
                ctx.shared_holdings[symbol] = holdings_qty

                if runtime_state["last_error_msg"] != "":
                    await ctx.notify_tg(
                        f"✅ <b>[스나이퍼 {symbol}] 토스증권 API 통신망 복구 완료</b>\n"
                        "▫️ 서버 응답 정상화. 레이더 감시 및 전술 연산을 즉시 재개합니다."
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

            # 2. 로컬 장부 상태 로드
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

            # 3. 17:00~18:59 EST 자정 가비지 컬렉션(GC) 파이프라인 (Case 22)
            if 17 <= now_est.hour < 19 and runtime_state["last_gc_date"] != today_str:
                runtime_state["last_gc_date"] = today_str
                ctx.in_memory_ordering_lock[symbol] = False
                ctx.idempotency_keys[symbol] = {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}
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

            # 4. 시장 캘린더 및 휴장일 판별 (제6헌법, Case 10, Case 45)
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

            # 5. 세션 베이스라인 및 세션 ID 산출 (Case 36)
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

            # 6. 플러그인 ①: 15:59~16:01 EST 제로-오버나이트 MOC 강제 청산망 (Case 16, Case 24, Case 48)
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

            # 7. 플러그인 ②: NQ=F 매크로 2·3차 양방향 대칭 방어선 프로빙 및 손실 덤핑 (Case 66, Case 67, Case 69)
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

            # 8. 세션 ID 변경 시 미보유/미체결 상태 클린업 (09:30 정각 신규 스나이핑 준비)
            if current_session_id != last_session_id:
                if holdings_qty == 0:
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

            # 9. 플러그인 ③: 09:31~15:58 EST 골든타임 마감 후 장중 추격 매수 영구 차단 (Case 48, Case 53)
            is_session_done = await enforce_post_0930_cutoff_plugin(
                symbol=symbol,
                est_time_int=est_time_int,
                is_session_done=is_session_done,
            )

            # 10. 플러그인 ④: 잔고 0주 전환 시 익절/MOC/수동청산 판별 및 타전 (Case 46, Case 49, Case 51)
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

            # 11. 플러그인 ⑤: 09:30:00 EST 정규장 개장 NQ=F 10분봉 양방향 스나이퍼 타격 (Case 15, 43, 52, 53, 54, 65)
            if (
                is_active
                and not is_session_done
                and hardcoded_session == "regularMarket"
                and est_time_int == 930
                and holdings_qty == 0
                and not buy_order_id
            ):
                await execute_0930_sniper_strike_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_session_id=current_session_id,
                    current_price=current_price,
                    budget=budget,
                    target_profit_rate=target_profit_rate,
                )

            # 12. 플러그인 ⑥: +1.0% 기계적 익절 덫 장전, 수동 매수 감지 및 유령 덫 복구 (Case 50, 58, 64, 70, 71)
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

    inject_dependencies(api_client, ADMIN_CHAT_ID, wakeup_event)
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
    )

    asyncio.create_task(api_client.token_renewal_loop())
    asyncio.create_task(auto_update_loop(bot, ADMIN_CHAT_ID))

    # MODIFIED: Case 09 & Case 52 - SOXL(롱) 및 SOXS(숏) 양방향 스나이퍼 및 캔들 레코더 기동
    for sym in SYMBOLS:
        asyncio.create_task(assassin_loop(plugin_ctx, sym))
        asyncio.create_task(record_candles_loop(api_client, sym))

    print("시스템 코어 및 09:30 NQ 10분봉 양방향(SOXL/SOXS) 스나이퍼 플러그인 결속 완료...", flush=True)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=(
                "✅ <b>[시스템 기동 완료 — 09:30 양방향 스나이퍼]</b>\n"
                "▫️ 타겟: <b>SOXL(롱) / SOXS(숏)</b> 배타적 1회 출동\n"
                "▫️ 전략: 09:20~09:30 EST NQ=F 10분봉 추세 기반 09:30 정각 즉시 타격\n"
                "▫️ 익절 및 방어: 기본 +1.0% 지정가 익절 & NQ=F 양방향 3단 방어망 결속 완료"
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
