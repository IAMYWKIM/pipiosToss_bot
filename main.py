# =====================================================================
# FILE: main.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       선택지 B (👑 HYB_35_1ST) 듀얼 스나이퍼(SOXS/SOXL),
#       진입 성격별 '이원화 매도 믹스(기술적 반등 ⚡ OLD_IMM_CB × 진성 상승종목 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 × 🚀 NQ_VEL_BYPASS)',
#       3차 손절 후 최저점 역트레일링 +0.30% 반등 재진입(RE_BOT_TRAIL),
#       매수 체결 직후 잔고 인덱싱 시차 중 자동 진입 물량의 MANUAL 오분류 원천 차단(초과 Case 100),
#       15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 물량 당일 정규장 VWAP 도달 즉시 덤핑 탈출(🛟 VWAP_ESCAPE),
#       데이장(19:00 ~ 03:57 EST) 100% [모의] 스나이퍼 및
#       03:58 ~ 04:07 EST 듀얼 1주 휩소 매수 덫 메인 오케스트레이터
#       (main.py 2/2 분할본 · main_loops_plugin 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 무인 업데이트 루프 및 RSI 레이더 루프를 main_loops_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 초과 Case 28, 78, 83, 85, 87, 93, 97, 98 — 6대 0.4초 고주파 부스터에 데이장 개시 1분 전(08:59 KST / 18:59 EST) 10분봉 사전 락온,
#           개시 후 7분간(09:00~09:06:59 KST / 19:00~19:06:59 EST) RSI>=50.0 추세감시 및 데이장 [모의] 트레일링 추적을 연동!
# NEW: 수동 진입(MANUAL) 첫 틱 감지 시 execute_macro_defense_plugin과 로컬 상태를 즉시 동기화하여 NQ>=2.0% 구간에서도 2차 방어 패스 후 3차 안전망 & HA트레일링 직결 (Case 50, 66, 67, 86)
# NEW: 초과 Case 100 — rearm_premarket_buy_trap_after_30s_plugin 실행 직후 실잔고(holdings_qty)를 즉시 재조회 동기화하여,
#      재장전 정정 주문이 즉시 체결되었을 때 reconcile_zero_holdings_exit_plugin 오진입 없이 동일 틱 내 안전 캡 장전으로 직행하도록 방어
# MODIFIED: 시스템 기동 완료 텔레그램 메시지를 "정상작동 중입니다." 단일 라인으로 슬림화 유지 (Case 38, 88)

import sys
import os
import math
import time
import asyncio
import html
import logging
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

from toss_api import TossApiClient
from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    calculate_rsi_1m,
    compute_realtime_tick_rsi,
)
from tg_router import router, inject_dependencies
from candle_recorder import record_candles_loop
from sniper_plugin import (
    SniperPluginContext,
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_TREND_BOOSTER_RSI,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    get_daymarket_sim_status,
    _cancel_open_orders_and_wait_sellable,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    get_bottom_track_status,
    _reset_pre_leader_state,
    get_pre_leader_status,
    update_premarket_trend_leader,
    settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
    execute_late_regular_vwap_escape_plugin,
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
from main_loops_plugin import (
    EST_TZ,
    KST_TZ,
    TRADE_SYMBOLS,
    RSI_SYMBOLS,
    NQ_DYNAMIC_FLOOR,
    HOLIDAY_TRANSLATIONS,
    _run_git_update_sync,
    auto_update_loop,
    rsi_radar_loop,
)

logging.getLogger("aiogram").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)

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

in_memory_ordering_lock = {sym: False for sym in TRADE_SYMBOLS}
dual_strike_mutex = asyncio.Lock()
shared_holdings = {sym: 0 for sym in TRADE_SYMBOLS}

shared_rsi_state = {
    sym: {"value": 0.0, "vwap": 0.0, "ewm_state": {}} for sym in RSI_SYMBOLS
}

idempotency_keys = {
    sym: {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}
    for sym in TRADE_SYMBOLS
}

holiday_notify_lock = asyncio.Lock()
last_holiday_notified_date = ""


async def assassin_loop(ctx: SniperPluginContext, symbol: str):
    global last_holiday_notified_date
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    runtime_state = {
        "last_moc_tick": 0.0,
        "moc_dump_active": False,
        "last_vwap_escape_tick": 0.0,
        "vwap_escape_alert_sent": False,
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
        # Case 28, 78, 83, 85, 87, 93, 97 & Case 98: 평시 1.5초 대기, 6대 부스터 조건 충족 시 0.4초 고주파 대기로 자동 가속
        wait_sec = 0.4 if runtime_state.get("fast_shield_mode", False) else 1.5
        try:
            await asyncio.wait_for(ctx.wakeup_event.wait(), timeout=wait_sec)
            ctx.wakeup_event.clear()
        except asyncio.TimeoutError:
            pass

        try:
            now_est = datetime.now(EST_TZ)
            now_kst = now_est.astimezone(KST_TZ)
            est_time_int = now_est.hour * 100 + now_est.minute
            kst_time_int = now_kst.hour * 100 + now_kst.minute
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
            # 초과 Case 53, 78, 84, 93, 97, 98 — 프리장 본진(04:07 ~ 08:14 EST), 후반 추세눌림목(07:00 ~ 08:14 EST),
            # 정규장 마감 1시간 전 VWAP 탈출(15:00 ~ 15:58 EST), 데이장 개시 1분 전(18:59 EST / 08:59 KST) 및 개시 7분 윈도우
            is_premarket_sniper_window = (407 <= est_time_int <= 814)
            is_premarket_0700_window = (700 <= est_time_int <= 814)
            is_late_reg_vwap_window = (1500 <= est_time_int <= 1558)
            is_day_prelock_or_open7m = (859 <= kst_time_int <= 906) or (1859 <= est_time_int <= 1906)
            is_daymarket_window = (est_time_int >= 1859 or est_time_int <= 359)
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
            pre_leader_st = get_pre_leader_status()
            cur_leader_sym = str(pre_leader_st.get("leader_sym") or "")
            is_pb_rearm_ready = bool(pre_leader_st.get("pb_rearm_ready", True))

            day_sim_st = get_daymarket_sim_status()
            day_leader_sym = str(day_sim_st.get("leader_sym") or "")
            day_pb_ready = bool(day_sim_st.get("pb_rearm_ready", True))
            is_day_sim_booster = bool(
                is_active
                and (is_daymarket_active_window or is_day_prelock_or_open7m)
                and (
                    is_day_prelock_or_open7m
                    or bool(day_sim_st.get("active", False))
                    or (day_leader_sym == symbol and day_pb_ready and (0.0 < cached_my_rsi <= PRE_TREND_BOOSTER_RSI))
                )
            )

            # Case 28, 78, 83, 85, 87, 93, 97 & Case 98: 6대 고주파 부스터 조건 판정
            is_trap_waiting = bool(
                is_active
                and is_premarket_sniper_window
                and entry_session in ("PRE_SNIPER", "PRE_PULLBACK")
                and bool(buy_order_id)
                and holdings_qty == 0
            )
            is_trend_pullback_booster = bool(
                is_active
                and is_premarket_0700_window
                and not is_session_done
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and cur_leader_sym == symbol
                and is_pb_rearm_ready
                and (0.0 < cached_my_rsi <= PRE_TREND_BOOSTER_RSI)
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
            is_vwap_escape_booster = bool(
                is_active
                and is_late_reg_vwap_window
                and holdings_qty > 0
                and not is_trail_armed_now
            )
            runtime_state["fast_shield_mode"] = bool(
                is_trap_waiting
                or is_trend_pullback_booster
                or is_pre_climax_booster
                or is_trail_armed_now
                or is_bottom_track_now
                or is_vwap_escape_booster
                or is_day_sim_booster
            )

            if runtime_state["prev_is_active"] is None:
                runtime_state["prev_is_active"] = is_active
            just_turned_off = (runtime_state["prev_is_active"] is True and is_active is False)
            runtime_state["prev_is_active"] = is_active

            # Case 22 — 17:00 EST 가비지 컬렉션(GC) 실행 시 미보유 상태면 is_session_done=False, 트레일링/최저점/상승주도주 캐시 및 EWM 캐시 초기화
            # (단, 18:58 ~ 18:59 EST 데이장 개시 직전 2분은 10분봉 사전 락온을 위해 GC 슬립에서 제외!)
            if (1700 <= est_time_int <= 1857) and runtime_state["last_gc_date"] != today_str:
                runtime_state["last_gc_date"] = today_str
                ctx.in_memory_ordering_lock[symbol] = False
                ctx.idempotency_keys[symbol] = {"BUY": None, "TRAP": None, "MOC": None, "M_DUMP": None}

                if symbol in ctx.shared_rsi_state:
                    ctx.shared_rsi_state[symbol]["ewm_state"] = {}

                runtime_state["last_moc_tick"] = 0.0
                runtime_state["moc_dump_active"] = False
                runtime_state["last_vwap_escape_tick"] = 0.0
                runtime_state["vwap_escape_alert_sent"] = False
                runtime_state["fast_shield_mode"] = False
                runtime_state["last_known_qty"] = 0
                _reset_bottom_track_state(symbol)
                _reset_pre_leader_state()
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

            # 초과 Case 78 & Case 93 — 미국 데이마켓 개장 1분 전(18:59 EST, sess_name == "STANDBY")에도
            # 나스닥 100 선물지수 10분봉 1차 락온이 100% 작동하도록 선행 호출 보장!
            if is_active and est_time_int == 1859 and symbol == "SOXS":
                await execute_daymarket_rsi_alert_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_price=0.0,
                    rsi_val=cached_my_rsi,
                    vwap_val=float(sym_rsi_dict.get("vwap", 0.0)),
                )

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
                    f"세션: {hardcoded_session} | 활성: {is_active} | 잔고: {holdings_qty}주 | 모드: HYB_35_1ST & 이원화트레일링 & VWAP탈출",
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

            # Case 83 & Case 97: 프리-클라이맥스/추세눌림목/최저점추적/데이장 개시7분 가속 구간이거나 매수 대상 종목이 과매도(<=35.0)일 때는
            # 단일 배치 API(get_current_prices_batch)로 SOXS와 SOXL 현재가를 동시 조회하여 시차 0.0ms 보장!
            need_batch_price_sync = bool(
                runtime_state["fast_shield_mode"]
                or (0.0 < cached_my_rsi <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
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

            # 목표 5 & Case 50, 66, 67, 82, 86, 87, 97, 98, 99 — 방향성 인지 NQ=F 매크로 3단 방어망 호출
            was_unregistered_manual = bool(
                holdings_qty > 0
                and not buy_order_id
                and entry_session not in (
                    "PRE_SNIPER",
                    "PRE_PULLBACK",
                    "PRE_SNIPER_REARM",
                    "PRE_PULLBACK_REARM",
                    "WHIPSAW_1SH",
                    "VWAP_ESCAPE",
                    "MANUAL",
                )
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

            # 초과 Case 98 — 정규장 장마감 1시간 전(15:00 ~ 15:58 EST)까지 +1.00% 익절하지 못한
            # 미익절 표류 물량이 당일 정규장 누적 VWAP 가격에 도달하면 즉시 덤핑 탈출 스윕!
            if is_active and is_late_reg_vwap_window and holdings_qty > 0:
                vwap_escaped = await execute_late_regular_vwap_escape_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    is_active=is_active,
                    holdings_qty=holdings_qty,
                    holdings_detail=holdings_detail,
                    avg_price_dump=avg_price_dump,
                    last_buy_price=last_buy_price,
                    buy_order_id=buy_order_id,
                    sell_order_id=sell_order_id,
                    entry_session=entry_session,
                    runtime_state=runtime_state,
                    current_price=current_price,
                    vwap_val=float(sym_rsi_dict.get("vwap", 0.0)),
                )
                if vwap_escaped:
                    continue

            if not is_open or current_price <= 0.0:
                continue

            if current_session_id != last_session_id:
                opp_st_tr = await AssassinLedger.get_state(opp_sym)
                opp_buy_tr = await AssassinLedger.get_buy_order_id(opp_sym)
                active_session_tags = (
                    "CANCELED_MACRO",
                    "WHIPSAW_1SH",
                    "MANUAL",
                    "PRE_SNIPER",
                    "PRE_PULLBACK",
                    "PRE_SNIPER_REARM",
                    "PRE_PULLBACK_REARM",
                    "STAGE3_STOP_TRACK",
                    "VWAP_ESCAPE",
                )
                is_same_day_transition = (
                    bool(last_session_id)
                    and len(last_session_id) >= 8
                    and last_session_id[:8] == current_session_id[:8]
                    and (
                        entry_session in active_session_tags
                        or opp_st_tr[8] in active_session_tags
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
                            st_od = str(od.get("status") or "")
                            exec_od = od.get("execution") or {}
                            fq_od = float(exec_od.get("filledQuantity") or 0.0)
                            # 초과 Case 100 — 세션 전이 시에도 매수 주문이 PENDING/PARTIAL/REPLACED 상태이거나
                            # 방금 FILLED(또는 filledQuantity > 0)되어 아직 안전 캡 매도 주문(sell_order_id)이 장전되기 전이면 초기화 금지!
                            if st_od in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE", "REPLACED"]:
                                can_reset = False
                            elif (st_od == "FILLED" or fq_od > 0.0) and not sell_order_id and last_buy_price <= 0.0:
                                can_reset = False
                        except Exception:
                            can_reset = False
                    if can_reset:
                        _reset_trailing_runtime_state(symbol)
                        _reset_bottom_track_state(symbol)
                        runtime_state["vwap_escape_alert_sent"] = False
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
                        f"♻️ [프리장 자가 치유 {symbol}] 비정상 마감 락(is_session_done) 자동 해제 완료 ➡️ 04:07~08:14 HYB_35_1ST 감시 복귀",
                        flush=True,
                    )

            # 목표 3 & 초과 Case 48, 53, 84, 87, 97 — 08:15 EST 거시지표 발표 15분 전 선제 컷오프 (08:15 ~ 15:59 EST)
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

            # 초과 Case 97 — 04:07 ~ 08:14 EST 구간 동안 당일 첫 RSI >= 79.9 달성 상승 추세 종목(leader_sym) 실시간 각인 및 회복 감시
            if is_premarket_sniper_window:
                pre_leader_st = update_premarket_trend_leader(
                    symbol=symbol,
                    live_rsi_val=live_rsi_val,
                    now_est=now_est,
                    shared_rsi_state=ctx.shared_rsi_state,
                )
                if opp_live_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD:
                    pre_leader_st = update_premarket_trend_leader(
                        symbol=opp_sym,
                        live_rsi_val=opp_live_rsi_val,
                        now_est=now_est,
                        shared_rsi_state=ctx.shared_rsi_state,
                    )
            else:
                pre_leader_st = get_pre_leader_status()

            cur_leader_sym = str(pre_leader_st.get("leader_sym") or "")
            is_pb_rearm_ready = bool(pre_leader_st.get("pb_rearm_ready", True))

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
            is_pb_0700_booster_now = bool(
                is_active
                and is_premarket_0700_window
                and not is_session_done
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and cur_leader_sym == symbol
                and is_pb_rearm_ready
                and (0.0 < live_rsi_val <= PRE_TREND_BOOSTER_RSI)
            )
            is_vwap_escape_booster_now = bool(
                is_active
                and is_late_reg_vwap_window
                and holdings_qty > 0
                and not bool(get_trailing_runtime_status(symbol).get("armed", False))
            )
            day_sim_now = get_daymarket_sim_status()
            is_day_sim_booster_now = bool(
                is_active
                and (is_daymarket_active_window or is_day_prelock_or_open7m)
                and (
                    is_day_prelock_or_open7m
                    or bool(day_sim_now.get("active", False))
                    or (
                        str(day_sim_now.get("leader_sym") or "") == symbol
                        and bool(day_sim_now.get("pb_rearm_ready", True))
                        and (0.0 < live_rsi_val <= PRE_TREND_BOOSTER_RSI)
                    )
                )
            )

            # 차기 루프 대기 시간(0.4초 부스터)을 최신 합성 RSI, 추세눌림목, 트레일링, 최저점 추적, VWAP 탈출 감시, 데이장 모의 감시 기준으로 즉시 업데이트 (Case 28, 78, 83, 85, 87, 93, 97, 98)
            runtime_state["fast_shield_mode"] = bool(
                is_trap_waiting
                or is_pb_0700_booster_now
                or is_trailing_booster
                or is_bottom_track_booster
                or is_vwap_escape_booster_now
                or is_day_sim_booster_now
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

            # 본진 프리장 골든타임(04:07 ~ 08:14 EST) 1차 슬리피지 매수 덫(PRE_SNIPER / PRE_PULLBACK) 대기 중 조건부 30초 타임쉴드 감시
            if (
                is_active
                and is_premarket_sniper_window
                and holdings_qty == 0
                and buy_order_id
                and entry_session in ("PRE_SNIPER", "PRE_PULLBACK")
            ):
                prev_es_before_rearm = entry_session
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
                # 초과 Case 100 ③ — 30초 타임쉴드 해제 및 현재가 상향 정정(또는 부분체결 확정) 직후
                # 거래소에서 즉시 체결된 물량을 놓치지 않고 동기화하기 위해 실잔고(holdings_detail)를 즉시 재조회!
                if entry_session != prev_es_before_rearm or not buy_order_id:
                    try:
                        holdings_detail = await ctx.client.get_symbol_holdings_detail(symbol)
                        holdings_qty = int(math.floor(holdings_detail["qty"]))
                        ctx.shared_holdings[symbol] = holdings_qty
                        if holdings_qty > 0:
                            runtime_state["last_known_qty"] = holdings_qty
                            avg_price_dump = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price
                    except Exception:
                        pass

            # 목표 3·4 & 초과 Case 46, 49, 51, 52, 76, 84, 87, 97, 98, 100: 잔고 0주 복귀 시 통합 실수익 복리 정산 및 08:14 EST까지 재출동 릴리스
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
                    runtime_state["vwap_escape_alert_sent"] = False
                    continue

            # 데이장 (18:59 ~ 03:59 EST, 08:59 KST 10분봉 사전 락온 포함) 100% [모의] 스나이퍼 가동 (03:58~03:59 묵음 내장 · 실거래 매수 100% 차단)
            if is_active and is_daymarket_window and current_price > 0.0:
                await execute_daymarket_rsi_alert_plugin(
                    ctx=ctx,
                    symbol=symbol,
                    now_est=now_est,
                    current_price=current_price,
                    rsi_val=live_rsi_val,
                    vwap_val=float(sym_rsi_dict.get("vwap", 0.0)),
                )

            # Case 79 & Case 95: 03:58 ~ 04:06:59 EST (9분간) 데이장 누적 VWAP 상단 종목 기준 0.5% 역산 듀얼 1주 무비용 매수 덫 발사
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

            # 초과 Case 87 — 본진 프리장 초중반 골든타임 (04:07 ~ 08:14 EST) 내
            # 3차 역행 안전망 손절 후 '손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 재진입 (🪤 RE_BOT_TRAIL)' 감시 및 타격!
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

            # 목표 3 & Case 15, 25, 52, 53, 54, 59, 71, 81, 83, 84, 87, 97 — 본진 프리장 골든타임 (04:07 ~ 08:14 EST) 선택지 B (👑 HYB_35_1ST)
            # ① [04:07 ~ 08:14 상시] 양방향 동시 대칭(매수종목 RSI <= 30.0 AND 반대종목 RSI >= 79.9) OR
            # ② [07:00 ~ 08:14 전용] 04:07 이후 첫 RSI >= 79.9 달성 상승 추세 종목(leader_sym)이 RSI <= 35.0 도달 시 1차 슬리피지 매수 덫 즉시 투하!
            is_gate_a_climax = (0.0 < live_rsi_val <= PRE_SNIPER_RSI_THRESHOLD) and (
                opp_live_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD
            )
            is_gate_b_pullback = (
                is_premarket_0700_window
                and (cur_leader_sym == symbol)
                and is_pb_rearm_ready
                and (0.0 < live_rsi_val <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
            )
            if (
                is_active
                and not is_session_done
                and is_premarket_sniper_window
                and holdings_qty == 0
                and ctx.shared_holdings.get(opp_sym, 0) == 0
                and not buy_order_id
                and not get_bottom_track_status(symbol).get("active", False)
                and (is_gate_a_climax or is_gate_b_pullback)
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

            # 목표 4 & 초과 Case 50, 58, 64, 85, 97, 99, 100: 매수 전량 체결(본진 및 수동 진입 공통) 확인 즉시 상단 안전 캡(+5.00%) 선장전 및
            # +1.00% 도달 시 진입 성격별 이원화 하이킨 아시 트레일링(기술적 반등 ⚡ OLD_IMM_CB × 진성 상승종목 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020) 0.1초 즉시 정정 스윕 가동
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

    print("시스템 코어: 04:07~08:14 골든타임(👑HYB_35_1ST) & 데이장 모의 추세·반복출격 & 이원화트레일링 & 15:00 VWAP탈출 플러그인 모듈 결속 완료...", flush=True)

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
