# =====================================================================
# FILE: main_loops_plugin.py
# 목적: 무인 자동 업데이트 루프(03:30 ~ 17:00 EST 락다운 & 18대 모듈 프리플라이트 컴파일 검증) 및
#       듀얼 5.0초 고속 / 정규장 후반 2페이지(400분봉) 페이지네이션 VWAP 스캔 RSI 레이더 루프 플러그인
#       (main.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 main.py의 무인 업데이트 루프 및 백그라운드 RSI·VWAP 레이더 루프를 main_loops_plugin.py로 무손실 분리
# MODIFIED: 분리된 서브 플러그인을 포함한 18대 코어 모듈 전체 프리플라이트 컴파일 검증 결속 (Case 11, 88)
# MODIFIED: 프리장 스나이퍼 보호를 위해 무인 업데이트 락다운 시간대를 03:30 ~ 17:00 EST로 전면 확장 (Case 07)
# MODIFIED: 03:55 ~ 08:14 EST 구간, 데이장 개시 7분 윈도우(18:58 ~ 19:07 EST / 08:58 ~ 09:07 KST) 및
#           포지션 보유(수동 포함)/트레일링 점화/최저점 추적 중 5.0초 듀얼 캔들 스캔 가속 및
#           정규장 후반(12:50 ~ 15:59 EST) 보유 중 2페이지(최대 400분봉) 페이지네이션 스캔으로
#           09:30 EST 개장 첫 봉부터 누락 없는 정규장 누적 VWAP 산출 (Case 73, 78, 84, 85, 87, 93, 98)

import os
import html
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram import Bot

from quant_engine import calculate_rsi_1m
from sniper_plugin import (
    SniperPluginContext,
    get_daymarket_sim_status,
    get_trailing_runtime_status,
    get_bottom_track_status,
    execute_rsi_monitoring_plugin,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

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
        # MODIFIED: Case 11 & Case 88 — 1:2 분할된 서브 플러그인을 포함한 전체 코어 모듈 프리플라이트 컴파일 검증
        for mod_file in (
            "main.py",
            "main_loops_plugin.py",
            "sniper_plugin.py",
            "sniper_exit_plugin.py",
            "defense_plugin.py",
            "defense_state_plugin.py",
            "whipsaw_plugin.py",
            "daymarket_sim_plugin.py",
            "tg_router.py",
            "tg_admin_plugin.py",
            "tg_ui_plugin.py",
            "tg_ui_boards_plugin.py",
            "quant_engine.py",
            "quant_indicators_plugin.py",
            "toss_api.py",
            "toss_api_core.py",
            "candle_recorder.py",
            "plugin_updater.py",
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
            now_kst = now_est.astimezone(KST_TZ)
            est_hm = now_est.hour * 100 + now_est.minute
            kst_hm = now_kst.hour * 100 + now_kst.minute
            wk = now_est.weekday()
            is_weekend_closed = (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800)

            is_open, _, sess_name, _ = await ctx.client.is_market_open()
            # 초과 Case 78 & Case 93 — 데이장 개시 직전 2분(18:58~18:59 EST 또는 08:58~08:59 KST)에는
            # 개시 후 7분간 RSI>=50.0 즉시 판별을 위해 캔들 기저 워밍업 스캔을 선행 허용!
            is_day_open_warmup = (not is_weekend_closed) and (
                (1858 <= est_hm <= 1859) or (858 <= kst_hm <= 859)
            )

            if (is_open and sess_name != "CLOSED") or is_day_open_warmup:
                data = await ctx.client.get_1m_candles_pagination(symbol, count=200)
                candles = list(data.get("candles") or [])
                next_before = data.get("nextBefore")

                # 초과 Case 98 — 정규장 후반(12:50 ~ 15:58 EST, 정규장 총 390분)에 보유 포지션이 존재할 때는
                # 2페이지(최대 400분봉)까지 페이지네이션 조회하여 09:30 EST 개장 첫 봉부터 누락 없는 정규장 누적 VWAP을 산출!
                if (
                    candles
                    and next_before
                    and (1250 <= est_hm <= 1559)
                    and int(ctx.shared_holdings.get(symbol, 0)) > 0
                ):
                    try:
                        data_p2 = await ctx.client.get_1m_candles_pagination(
                            symbol, count=200, before=next_before
                        )
                        candles_p2 = data_p2.get("candles") or []
                        if candles_p2:
                            candles.extend(candles_p2)
                    except Exception:
                        pass

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

        # Case 73, 78, 84, 85, 87, 93, 98 — 03:55 ~ 08:14 EST 구간, 데이장 개시 7분 윈도우(18:58~19:07 EST / 08:58~09:07 KST),
        # 또는 포지션 보유(수동 및 데이장 모의 포함)/트레일링 점화/최저점 추적 중에는 5.0초 고속 스캔 유지
        now_est_chk = datetime.now(EST_TZ)
        now_kst_chk = now_est_chk.astimezone(KST_TZ)
        est_hm_chk = now_est_chk.hour * 100 + now_est_chk.minute
        kst_hm_chk = now_kst_chk.hour * 100 + now_kst_chk.minute
        day_sim_chk = get_daymarket_sim_status()
        is_day_open_fast = (1858 <= est_hm_chk <= 1907) or (858 <= kst_hm_chk <= 907)
        is_holding_or_trailing = (
            ctx.shared_holdings.get(symbol, 0) > 0
            or bool(get_trailing_runtime_status(symbol).get("armed", False))
            or bool(get_bottom_track_status(symbol).get("active", False))
            or bool(day_sim_chk.get("active", False))
        )
        if symbol in RSI_SYMBOLS and ((355 <= est_hm_chk <= 814) or is_day_open_fast or is_holding_or_trailing):
            sleep_sec = 5.0
        elif symbol in RSI_SYMBOLS and (est_hm_chk >= 1900 or est_hm_chk <= 354):
            sleep_sec = 15.0
        else:
            sleep_sec = 60.0
        await asyncio.sleep(sleep_sec)
