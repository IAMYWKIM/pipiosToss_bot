# -*- coding: utf-8 -*-
"""
# MODIFIED: tg_ui_plugin.py
순수 롱(SOXL) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼 전용 텔레그램 비동기 롱폴링 라우터,
/update 무한 재기동 원천 차단 4중 방어벽,
2대 핵심 제어 옵션(작동 ON/OFF 버튼 & 시드머니 설정 옵션) 전용 전술 제어반(/settlement),
대화형 시드 숫자 입력기, 메인 메뉴(/menu), 지층 지시서(/sync), 스나이퍼 레이더(/avwap) 및
/reset 원자적 지층 소각 플러그인 (Step 07 / 09)
- MODIFIED: 30분 옵션을 완전히 배제하고 10분 시드보호 매수 게이트(buy_interval_min = 10) 및
  8% 황금비율 정액 기반 정량 복리(COMP_08PCT · 08_VREV_PROG)로 100% 하드 고정!
- MODIFIED: 전술 제어반(/settlement)에는 오직 [작동 ON/OFF 버튼]과 [시드머니 설정 버튼] 2가지 옵션만 존재하도록 단순화!
- MODIFIED: 제3헌법에 의거하여 폐기된 데이장(daymarket_sim_plugin), 1주 휩소(whipsaw_plugin),
  인버스 숏(SOXS) 및 15:59 MOC 관련 코드/문구를 100% 영구 소각
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 58 완벽 준수
"""

from __future__ import annotations

import asyncio
import html
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from defense_plugin import relock_stratum_on_bot_resume
from plugin_updater import (
    has_active_orders_in_ledger,
    is_update_lockdown_window,
    run_command,
    verify_all_modules_compile,
)
from quant_engine import summarize_stratum_portfolio
from quant_indicators_plugin import (
    calc_session_expected_band,
    compute_buy_gate_bucket_info,
    compute_daily_5ma_and_expected_band,
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    EST_TZ,
    TARGET_SYMBOL,
    GlobalThrottle,
    _autoload_dotenv_files,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.tg_ui_plugin")

_autoload_dotenv_files(override=False)

# 전략 공식 코드명 (10분 시드보호 매수 게이트 · 1분 상시 스나이퍼 매도 · 8% 황금정량 복리 고정)
STRATEGY_CODE_LABEL: str = "👑CMP08_10M_1M"
DIVIDER_LINE: str = "➖➖➖➖➖➖➖➖➖➖➖➖➖➖"

# /update 명령 중복 실행 방지용 마지막 처리 update_id 기록 파일 경로
LAST_UPDATE_ID_FILE: str = "/tmp/toss_sniper_last_update_id.txt"

# USD -> KRW 환율 60초 인메모리 캐시 (API.txt GET /api/v1/exchange-rate 연동용)
_FX_CACHE_LOCK = threading.Lock()
_FX_USD_KRW_CACHE: Dict[str, float] = {
    "rate": 1380.0,
    "updated_mono": 0.0,
}


def sanitize_tg_text(text: Any) -> str:
    """
    Case 17:
    - 동적 문자열의 HTML 특수문자(<, >, &)를 안전하게 이스케이프하고,
    - 모바일 클립보드 수식 에러(\\vert{})를 유발하는 세로바 기호를 가운뎃점(·)으로 치환한다.
    """
    raw = str(text if text is not None else "")
    cleaned = raw.replace("|", "·")
    return html.escape(cleaned, quote=False)


# ---------------------------------------------------------------------------
# /update 무한 재기동 방지용 update_id 원자적 파일 기록/조회 헬퍼 (제1헌법 & Case 06)
# ---------------------------------------------------------------------------
def _read_last_processed_update_id_sync() -> int:
    lock = GlobalThrottle.get_file_lock(LAST_UPDATE_ID_FILE)
    with lock:
        if not os.path.isfile(LAST_UPDATE_ID_FILE):
            return 0
        try:
            with open(LAST_UPDATE_ID_FILE, "r", encoding="utf-8") as fp:
                return safe_int(fp.read().strip(), 0)
        except Exception:
            return 0


def _write_last_processed_update_id_sync(update_id: int) -> None:
    lock = GlobalThrottle.get_file_lock(LAST_UPDATE_ID_FILE)
    with lock:
        tmp_path = f"{LAST_UPDATE_ID_FILE}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fp:
                fp.write(str(int(update_id)))
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp_path, LAST_UPDATE_ID_FILE)
        except Exception as exc:
            logger.debug("last_update_id 기록 예외: %s", exc)


# ---------------------------------------------------------------------------
# 서머타임(EDT / EST) 판별 · 세션 배너 · 지층 상태 요약 포맷터
# ---------------------------------------------------------------------------
def get_dst_status_badges(now_est: Optional[datetime] = None) -> Dict[str, str]:
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    is_dst = bool(dt.dst() and dt.dst() != timedelta(0))
    if is_dst:
        return {
            "menu_header": "🌞서머타임 ON (EDT)",
            "sync_header": f"🌞 서머타임 ({dt.strftime('%H:%M')} EST)",
        }
    return {
        "menu_header": "❄️서머타임 OFF (EST)",
        "sync_header": f"❄️ 표준시 ({dt.strftime('%H:%M')} EST)",
    }


def get_market_session_banners(now_est: Optional[datetime] = None) -> Dict[str, str]:
    """
    현재 EST 시각에 따른 상단 세션 배너 및 출동 상태 문구를 반환한다 (데이장/휩소/MOC 완전 폐기 반영).
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()
    hm = dt.hour * 100 + dt.minute

    if (wd == 4 and hm >= 1700) or wd == 5 or (wd == 6 and hm < 1800):
        return {
            "sync_banner": "🌙 주말 휴장 (오버나이트 지층 보존)",
            "radar_banner": "🌙 주말 휴장 · 지층 안전 보존",
            "dispatch_status": "🌙 주말 휴장 (오버나이트 지층 보존)",
        }
    if 400 <= hm <= 406:
        return {
            "sync_banner": "🌅 04:00 프리장 호가 안정화 · 지층 동기화",
            "radar_banner": "🌅 04:00 ~ 04:06 · 프리장 안정화 대기",
            "dispatch_status": "⏳ 04:07 EST 첫 게이트 오픈 대기 중",
        }
    if 407 <= hm <= 929:
        return {
            "sync_banner": f"🌅 프리장 지층관리 가동 ({STRATEGY_CODE_LABEL})",
            "radar_banner": f"🌅 프리장 가동 · {STRATEGY_CODE_LABEL}",
            "dispatch_status": "🔫 프리장 (10분 매수게이트 · 1분 상시매도)",
        }
    if 930 <= hm <= 1600:
        return {
            "sync_banner": f"🔥 정규장 지층관리 가동 ({STRATEGY_CODE_LABEL})",
            "radar_banner": f"🔥 정규장 가동 · {STRATEGY_CODE_LABEL}",
            "dispatch_status": "🔥 정규장 (10분 매수게이트 · 1분 상시매도)",
        }
    if 1601 <= hm <= 1659:
        return {
            "sync_banner": "🌙 정규장 마감 · 일일 결산 & 지층 오버나이트 보존",
            "radar_banner": "🌙 16:00 마감 · 지층 오버나이트 이월",
            "dispatch_status": "🌙 정규장 마감 (지층 오버나이트 보존)",
        }
    return {
        "sync_banner": "🌙 장외 휴식 (04:07 EST 프리장 대기)",
        "radar_banner": "🌙 장외 대기 · 지층 오버나이트 보존",
        "dispatch_status": "🌙 장외 휴식 (04:07 EST 자동 재개)",
    }


async def _fetch_usd_krw_rate(context: Any) -> float:
    with _FX_CACHE_LOCK:
        cached_rate = safe_float(_FX_USD_KRW_CACHE.get("rate"), 1380.0)
        age = time.monotonic() - safe_float(
            _FX_USD_KRW_CACHE.get("updated_mono"), 0.0
        )
    if cached_rate > 0.0 and age < 60.0:
        return cached_rate

    try:
        res = await context.api._request_async(
            "GET",
            "/api/v1/exchange-rate",
            group="MARKET_INFO",
            params={"baseCurrency": "USD", "quoteCurrency": "KRW"},
            require_account=False,
            timeout=6.0,
        )
        if isinstance(res, dict):
            r_val = safe_float(res.get("rate") or res.get("midRate"), 0.0)
            if r_val > 500.0:
                with _FX_CACHE_LOCK:
                    _FX_USD_KRW_CACHE["rate"] = r_val
                    _FX_USD_KRW_CACHE["updated_mono"] = time.monotonic()
                return r_val
    except Exception as exc:
        logger.debug("USD/KRW 환율 조회 폴백 사용: %s", exc)

    return cached_rate if cached_rate > 0.0 else 1380.0


def _format_stratum_lines(summary: Dict[str, Any], live_p: float) -> List[str]:
    """
    1층(L1) 및 2층(MERGED_UPPER) 지층 상태를 직관적인 모바일 라인으로 포맷팅한다.
    """
    buckets = summary.get("buckets", [])
    if not buckets:
        return [
            "▫️ 보유 지층: 0주 (운용시간 진입 즉시 현재가 1층 자동 산입)"
        ]

    out_lines: List[str] = []
    is_l2_only = bool(summary.get("is_l2_only_anti_bloat"))

    if is_l2_only and len(buckets) == 1:
        b2 = buckets[0]
        t2_p = safe_float(summary.get("l2_target_price"), 0.0)
        ratchet_p = safe_float(
            summary.get("ratchet_anchor_price"), b2.book_price
        )
        ratchet_target = round(ratchet_p * 1.015, 2)
        chg_l2 = (
            ((live_p - b2.book_price) / b2.book_price) * 100.0
            if (b2.book_price > 0.0 and live_p > 0.0)
            else 0.0
        )
        out_lines.append(
            f"🛡️ <b>[1층: 무팽창 Always-L1 감시]</b> 래칫기준 ${ratchet_p:.2f} (계단목표 ${ratchet_target:.2f})"
        )
        out_lines.append(
            f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> "
            f"장부 ${b2.book_price:.2f}(실평단 ${b2.actual_avg_price:.2f}) · "
            f"졸업가 ${t2_p:.2f}(+1.0%) [{chg_l2:+.2f}%]"
        )
        return out_lines

    b1 = buckets[0]
    t1_p = safe_float(summary.get("l1_target_price"), 0.0)
    chg_l1 = (
        ((live_p - b1.book_price) / b1.book_price) * 100.0
        if (b1.book_price > 0.0 and live_p > 0.0)
        else 0.0
    )
    out_lines.append(
        f"⚡ <b>[1층: 공격수 {b1.qty}주]</b> 단가 ${b1.book_price:.2f} · "
        f"익절목표 ${t1_p:.2f}(+1.5%) [{chg_l1:+.2f}%]"
    )

    if len(buckets) >= 2:
        b2 = buckets[1]
        t2_p = safe_float(summary.get("l2_target_price"), 0.0)
        chg_l2 = (
            ((live_p - b2.book_price) / b2.book_price) * 100.0
            if (b2.book_price > 0.0 and live_p > 0.0)
            else 0.0
        )
        out_lines.append(
            f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> "
            f"장부 ${b2.book_price:.2f}(실평단 ${b2.actual_avg_price:.2f}) · "
            f"졸업가 ${t2_p:.2f}(+1.0%) [{chg_l2:+.2f}%]"
        )

    return out_lines


# ---------------------------------------------------------------------------
# [보드 1] 메인 메뉴 (/start, /menu, /help, 🔙 메인 메뉴 버튼)
# ---------------------------------------------------------------------------
async def render_main_menu_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    🐂 [ SOXL 지층관리 스나이퍼 ] 메인 메뉴 및 일일 스케줄러 보드 렌더링.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dst_info = get_dst_status_badges(dt_est)
    st = context.ledger.get_state(TARGET_SYMBOL)
    init_seed = safe_float(
        st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD
    )

    lines: List[str] = [
        f"🐂 <b>[ SOXL 지층관리 스나이퍼 ({dst_info['menu_header']}) ]</b>",
        f"▫️ 전략: {STRATEGY_CODE_LABEL} (8% 황금정량 · 자투리/수익금 복리 · 무팽창 Always-L1)",
        f"▫️ 매수: 🛡️ 10분 고정 시드보호 게이트 (1~6회 -1.0% / 7~11회 -1.5% / 12회+ -2.0%)",
        "▫️ 매도: ⚡ 1분 상시 스나이퍼 매도 (1층 +1.50% 익절&amp;리앵커링 / 2층 +1.00% 졸업)",
        f"▫️ 기준 시드머니: ${init_seed:,.2f} (1회 8% 정량 자동 산출)",
        DIVIDER_LINE,
        "🔹 04:00 ~ 04:06: 🌅 프리장 호가 안정화 &amp; 오버나이트 지층 동기화",
        "🔹 04:07 ~ 09:29: 🔫 프리장 지층관리 가동 (10분 매수게이트 · 1분 상시매도)",
        "🔹 09:30 ~ 16:00: 🔥 정규장 지층관리 가동 (10분 매수게이트 · 1분 상시매도)",
        "🔹 16:00 ~ 16:59: 🌙 일일 결산 보고 &amp; 지층 오버나이트 보존 (강제 MOC 없음)",
        "🔹 17:00: 🧹 일일 가비지 컬렉션(GC) 및 원장 백업",
        "",
        "🛠 <b>[ 핵심 명령어 ]</b>",
        "▶️ /avwap : 🔫 스나이퍼 레이더 관제탑",
        "▶️ /sync : 📜 SOXL 지층 지시서",
        "▶️ /settlement : ⚙️ 전술 제어반 (작동 ON/OFF · 시드머니 설정)",
        "",
        "⚠️ /reset : 🧹 지층 장부 0점 초기화 (복리 시드 보존)",
        "⚠️ /update : 🚀 시스템 자가 업데이트",
    ]

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "refresh_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "refresh_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "refresh_settlement"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 2] /settlement 전술 제어반 (오직 작동 ON/OFF 버튼 & 시드머니 설정 옵션 2가지만 존재!)
# ---------------------------------------------------------------------------
async def render_settlement_control_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    ⚙️ [ SOXL 지층관리 전술 제어반 ]
    - 요청사항 완벽 반영: 30분 옵션 없음(10분 고정), 사용자는 오로지
      ① 작동 ON/OFF 버튼(toggle_bot_active)과 ② 시드머니 설정 옵션(prompt_seed_input) 2가지만 조작!
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    banners = get_market_session_banners(dt_est)
    prices = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
    live_p = safe_float(prices.get(TARGET_SYMBOL), 0.0)

    st = context.ledger.get_state(TARGET_SYMBOL)
    summary = summarize_stratum_portfolio(st, live_price=live_p)

    is_active = bool(st.get("is_active", True))
    init_seed = safe_float(
        summary.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD
    )
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    tot_mark_eq = safe_float(summary.get("total_mark_equity_usd"), init_seed)
    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)
    next_budget = safe_float(summary.get("next_portion_budget"), init_seed * 0.08)
    next_qty = safe_int(summary.get("next_target_qty"), 1)

    if not is_active:
        dispatch_str = "⛔ 스나이퍼 가동 정지 (수동 관망)"
    elif held_sh > 0:
        dispatch_str = f"🎯 SOXL {held_sh}주({held_lots}회차) 지층 운용 중"
    else:
        dispatch_str = banners["dispatch_status"]

    power_str = "🟢 ON (가동 중)" if is_active else "🔴 OFF (정지됨)"

    lines: List[str] = [
        "⚙️ <b>[ SOXL 지층관리 전술 제어반 ]</b>",
        DIVIDER_LINE,
        f"▫️ 작동 상태: {power_str}",
        f"▫️ 운용 현황: {sanitize_tg_text(dispatch_str)}",
        "▫️ 매수 게이트: 🛡️ 10분 고정 시드보호 게이트 (-1.0% / -1.5% / -2.0%)",
        "▫️ 매도 게이트: ⚡ 1분 상시 스나이퍼 매도 (1층 +1.50% / 2층 +1.00%)",
        "▫️ 정량 엔진: 👑 8%(12.5분할) 정액기반 정량 + 자투리·수익금 복리 산입",
        DIVIDER_LINE,
        f"💵 <b>설정 시드머니: ${init_seed:,.2f}</b>",
        f"💰 가용 현금시드: ${free_cash:,.2f} · 총자산: ${tot_mark_eq:,.2f}",
        f"🎯 차기 1회(8%) 정량: ${next_budget:,.2f} ➔ <b>{next_qty}주</b> 확정 대기",
    ]

    toggle_power_btn = (
        {"text": "🔴 스나이퍼 작동 OFF", "callback_data": "toggle_bot_active"}
        if is_active
        else {
            "text": "🟢 스나이퍼 작동 ON",
            "callback_data": "toggle_bot_active",
        }
    )

    # 사용자는 오로지 [작동 ON/OFF] 와 [시드머니 설정] 2가지 제어 옵션만 사용
    reply_markup = {
        "inline_keyboard": [
            [
                toggle_power_btn,
                {"text": "💵 시드머니 설정", "callback_data": "prompt_seed_input"},
            ],
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "refresh_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "refresh_sync"},
                {"text": "🔙 메인 메뉴", "callback_data": "open_main_menu"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 3] /sync SOXL 지층 지시서 (1층/2층 지층 현황 · 가용현금 · 달러/원화 손익)
# ---------------------------------------------------------------------------
async def render_sync_session_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    📜 [ SOXL 지층관리 지시서 ] 렌더러.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dst_info = get_dst_status_badges(dt_est)
    banners = get_market_session_banners(dt_est)

    prices = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
    holdings = await context.api.get_holdings_all()
    raw_bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.0)
    fx_rate = await _fetch_usd_krw_rate(context)

    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = (
        raw_cache.get(TARGET_SYMBOL) if isinstance(raw_cache, dict) else None
    )
    live_p = safe_float(prices.get(TARGET_SYMBOL), 0.0)
    met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
    live_p = safe_float(met.get("live_price"), live_p)

    h_info = holdings.get(TARGET_SYMBOL, {})
    h_qty = safe_int(h_info.get("qty"), 0)
    avg_p = safe_float(h_info.get("avg_price"), 0.0)
    if live_p <= 0.0:
        live_p = safe_float(h_info.get("last_price"), avg_p)

    sess_h = safe_float(met.get("session_high"), live_p)
    sess_l = safe_float(met.get("session_low"), live_p)

    try:
        daily_candles, _ = await context.api.get_candles(
            symbol=TARGET_SYMBOL, interval="1d", count=6, adjusted=True
        )
    except Exception:
        daily_candles = []

    band = compute_daily_5ma_and_expected_band(
        daily_candles=daily_candles,
        session_high=sess_h,
        session_low=sess_l,
        live_price=live_p,
        now_est=dt_est,
    )
    h_chg_pct = safe_float(band.get("high_chg_pct"), 0.0)
    l_chg_pct = safe_float(band.get("low_chg_pct"), 0.0)

    st = context.ledger.get_state(TARGET_SYMBOL)
    summary = summarize_stratum_portfolio(st, live_price=live_p)
    stratum_lines = _format_stratum_lines(summary, live_p)

    init_seed = safe_float(
        summary.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD
    )
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    cum_realized = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
    unreal_usd = safe_float(summary.get("unrealized_pnl_usd"), 0.0)
    unreal_pct = safe_float(summary.get("unrealized_rate_pct"), 0.0)
    tot_net_usd = safe_float(summary.get("total_net_pnl_usd"), 0.0)
    tot_roi_pct = safe_float(summary.get("total_roi_pct"), 0.0)
    tot_net_krw = int(round(tot_net_usd * fx_rate))

    next_q = safe_int(summary.get("next_target_qty"), 1)
    next_b = safe_float(summary.get("next_portion_budget"), init_seed * 0.08)
    drop_p = safe_float(summary.get("drop_buy_pct"), -1.0)
    trig_buy_p = safe_float(summary.get("trigger_buy_price"), 0.0)

    u_sign = "+" if unreal_usd >= 0.0 else "-"
    r_sign = "+" if cum_realized >= 0.0 else "-"
    t_sign = "+" if tot_net_usd >= 0.0 else "-"
    k_sign = "+" if tot_net_krw >= 0 else "-"

    lines: List[str] = [
        f"📜 <b>[ SOXL 지층 지시서 ({banners['sync_banner']}) ]</b>",
        f"📅 {dst_info['sync_header']}",
        f"💵 설정시드: ${init_seed:,.2f} · 가용현금시드: ${free_cash:,.2f}",
        f"🏦 토스 계좌 주문가능금액: ${raw_bp_usd:,.2f}",
        DIVIDER_LINE,
        f"🐂 <b>[SOXL] 현재가 ${live_p:.2f} / 실평단 ${avg_p:.2f} (총 {summary['held_shares']}주 · {summary['held_lots']}회차)</b>",
        f"📈 금일 고가: ${sess_h:.2f} ({h_chg_pct:+.2f}%) · 저가: ${sess_l:.2f} ({l_chg_pct:+.2f}%)",
        *stratum_lines,
        f"🛡️ 차기 10분 매수게이트({drop_p:.1f}%): ${trig_buy_p:.2f} 이하 시 <b>{next_q}주</b>(예산 ${next_b:,.0f}) 정량매수",
        DIVIDER_LINE,
        f"💰 누적 세후실현익: {r_sign}${abs(cum_realized):,.2f}",
        f"📊 현재 평가손익: {u_sign}${abs(unreal_usd):,.2f} ({unreal_pct:+.2f}%)",
        f"🏆 총 순손익(실현+평가): {t_sign}${abs(tot_net_usd):,.2f} ({tot_roi_pct:+.2f}% · {k_sign}₩{abs(tot_net_krw):,})",
    ]

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔄 지시서 갱신", "callback_data": "refresh_sync"},
                {"text": "🔫 스나이퍼 레이더", "callback_data": "refresh_avwap"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "refresh_settlement"},
                {"text": "🔙 메인 메뉴", "callback_data": "open_main_menu"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 4] /avwap SOXL 지층관리 스나이퍼 레이더 관제탑
# ---------------------------------------------------------------------------
async def render_avwap_radar_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    📡 [SOXL 지층관리 스나이퍼 레이더] — 1층/2층 지층 상태, 10분 시드보호 매수 게이트,
    1분 상시 스나이퍼 매도 목표가, 리앵커링 및 자투리 절약 통계를 표출하는 메인 관제 보드.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    banners = get_market_session_banners(dt_est)
    gate_info = compute_buy_gate_bucket_info(dt_est, buy_interval_min=10)

    prices = await context.api.get_current_prices_batch((TARGET_SYMBOL,))
    holdings = await context.api.get_holdings_all()

    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = (
        raw_cache.get(TARGET_SYMBOL) if isinstance(raw_cache, dict) else None
    )
    live_p = safe_float(prices.get(TARGET_SYMBOL), 0.0)
    met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
    live_p = safe_float(met.get("live_price"), live_p)

    h_info = holdings.get(TARGET_SYMBOL, {})
    if live_p <= 0.0:
        live_p = safe_float(h_info.get("last_price"), 0.0)

    try:
        d_candles, _ = await context.api.get_candles(
            symbol=TARGET_SYMBOL, interval="1d", count=6, adjusted=True
        )
    except Exception:
        d_candles = []

    band = compute_daily_5ma_and_expected_band(
        daily_candles=d_candles,
        session_high=safe_float(met.get("session_high"), live_p),
        session_low=safe_float(met.get("session_low"), live_p),
        live_price=live_p,
        now_est=dt_est,
    )
    amp_5ma = safe_float(band.get("amp_5ma_pct"), 0.0)
    yest_amp = safe_float(band.get("yesterday_amp_pct"), 0.0)
    ha_badge = "🕯️HA양봉" if met.get("is_ha_bull") else "🌑HA음봉"

    pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
    ph = safe_float(met.get("pre_high"), 0.0)
    pl = safe_float(met.get("pre_low"), 0.0)
    p_amp = safe_float(met.get("pre_amp_pct"), 0.0)
    p_exp_l, p_exp_h = calc_session_expected_band(ph, pl, amp_5ma)

    reg_vwap = safe_float(met.get("reg_vwap"), 0.0)
    rh = safe_float(met.get("reg_high"), 0.0)
    rl = safe_float(met.get("reg_low"), 0.0)
    r_amp = safe_float(met.get("reg_amp_pct"), 0.0)
    r_exp_l, r_exp_h = calc_session_expected_band(rh, rl, amp_5ma)

    st = context.ledger.get_state(TARGET_SYMBOL)
    summary = summarize_stratum_portfolio(st, live_price=live_p)
    stratum_lines = _format_stratum_lines(summary, live_p)

    on_badge = "🟢ON" if st.get("is_active", True) else "🔴OFF"
    drop_p = safe_float(summary.get("drop_buy_pct"), -1.0)
    ref_anchor = safe_float(summary.get("active_buy_anchor"), live_p)
    trig_buy_p = safe_float(summary.get("trigger_buy_price"), 0.0)
    next_q = safe_int(summary.get("next_target_qty"), 1)
    next_b = safe_float(summary.get("next_portion_budget"), 800.0)
    cur_chg_from_anchor = (
        ((live_p - ref_anchor) / ref_anchor) * 100.0
        if (ref_anchor > 0.0 and live_p > 0.0)
        else 0.0
    )

    reanchor_cnt = safe_int(st.get("reanchor_cnt"), 0)
    reanchor_drop = safe_float(st.get("cumulative_reanchor_drop_usd"), 0.0)
    pass_cnt = safe_int(st.get("seed_protect_pass_cnt"), 0)
    saved_usd = safe_float(st.get("cumulative_leftover_saved_usd"), 0.0)
    extra_13_cnt = safe_int(st.get("extra_13plus_buy_events"), 0)

    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    tot_mark_eq = safe_float(summary.get("total_mark_equity_usd"), 0.0)
    cum_realized = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
    tot_roi = safe_float(summary.get("total_roi_pct"), 0.0)
    r_sign = "+" if cum_realized >= 0.0 else "-"

    lines: List[str] = [
        f"📡 <b>[SOXL 지층관리 레이더]</b> ( {banners['radar_banner']} )",
        DIVIDER_LINE,
        f"🐂 <b>SOXL [{on_badge}] 현재가 ${live_p:.2f}</b> · 5MA {amp_5ma:.1f}%(전일 {yest_amp:.1f}%) · {ha_badge}",
        f"🌅 프리장 VWAP ${pre_vwap:.2f} · 팩트 ${pl:.2f} ~ ${ph:.2f} ({p_amp:.1f}%) · 예상 ${p_exp_l:.2f} ~ ${p_exp_h:.2f}",
        f"🔥 정규장 VWAP ${reg_vwap:.2f} · 팩트 ${rl:.2f} ~ ${rh:.2f} ({r_amp:.1f}%) · 예상 ${r_exp_l:.2f} ~ ${r_exp_h:.2f}",
        DIVIDER_LINE,
        f"🏛️ <b>[2-Tier 지층 &amp; 무팽창 Always-L1 현황]</b> (총 {summary['held_shares']}주 · {summary['held_lots']}회차)",
        *stratum_lines,
        DIVIDER_LINE,
        f"🛡️ <b>[10분 고정 시드보호 매수 게이트]</b> (차기검문: {gate_info['next_gate_hm_str']})",
        f"▫️ 기준가 ${ref_anchor:.2f} 대비 현재 {cur_chg_from_anchor:+.2f}% (요구단차 {drop_p:.2f}% · 매수가 ${trig_buy_p:.2f} 이하)",
        f"▫️ 차기 8% 정량: 예산 ${next_b:,.2f} ➔ <b>{next_q}주</b> 고정 대기",
        DIVIDER_LINE,
        f"♻️ 평단 리앵커링: {reanchor_cnt}회 (누적 -${reanchor_drop:,.2f} 단가인하)",
        f"🛡️ 시드보호 패스: {pass_cnt:,}회 · 🔥13회차+ 확장매수: {extra_13_cnt}회",
        f"💰 자투리절약 누적: +${saved_usd:,.2f} · 세후실현익: {r_sign}${abs(cum_realized):,.2f}",
        f"💵 가용현금: ${free_cash:,.2f} · <b>총자산: ${tot_mark_eq:,.2f} ({tot_roi:+.2f}%)</b>",
        "",
        f"⏱️ 갱신: {dt_est.strftime('%m-%d %H:%M:%S')} EST",
    ]

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔄 레이더 갱신", "callback_data": "refresh_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "refresh_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "refresh_settlement"},
                {"text": "🔙 메인 메뉴", "callback_data": "open_main_menu"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [핵심 행정 1] /reset 명령어 원자적 지층 소각 핸들러 (Case 02)
# ---------------------------------------------------------------------------
async def handle_reset_command(
    context: Any,
    new_seed_usd: Optional[float] = None,
) -> str:
    """
    🚨 Case 02:
    텔레그램 /reset 명령어를 통해:
    1) SOXL 미체결 매수/매도 주문을 취소하고 지층 큐(buckets), 기준가를 원자적 0점 초기화한다.
    2) 누적 복리 시드 금액(initial_seed_usd / free_cash_usd)은 그대로 보존하며,
       /reset_seed [금액] 입력 시에는 해당 금액으로 시드를 재설정한다.
    """
    st = context.ledger.get_state(TARGET_SYMBOL)
    for oid_key in ("buy_order_id", "sell_order_id"):
        oid = str(st.get(oid_key) or "").strip()
        if oid:
            try:
                await context.api.cancel_order(oid)
            except Exception:
                pass

    updated_st = await context.ledger.reset_stratum_ledger_preserve_seed(
        new_seed_usd=new_seed_usd
    )
    # 10분 고정 및 8% 복리 정량 설정 유지 보장
    await context.ledger.update_state(
        TARGET_SYMBOL,
        buy_interval_min=10,
        portion_mode="COMP_08PCT",
        threshold_mode="08_VREV_PROG",
        is_compound=True,
    )

    if hasattr(context, "exit_snapshot_cache") and isinstance(
        context.exit_snapshot_cache, dict
    ):
        context.exit_snapshot_cache.clear()
    if hasattr(context, "indexing_grace_cache") and isinstance(
        context.indexing_grace_cache, dict
    ):
        context.indexing_grace_cache.clear()

    context.wakeup_event.set()
    init_seed = safe_float(
        updated_st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD
    )
    free_cash = safe_float(updated_st.get("free_cash_usd"), init_seed)

    return (
        "🧹 <b>[/reset SOXL 지층 원장 원자적 0점 소각 완료]</b>\n"
        f"🔸 설정 시드머니: <code>${init_seed:,.2f}</code> · 가용 현금시드: <code>${free_cash:,.2f}</code>"
    )


# ---------------------------------------------------------------------------
# [핵심 행정 2] /update 시스템 자가 업데이트 핸들러
# ---------------------------------------------------------------------------
def _run_git_update_and_compile_sync(force: bool = False) -> Tuple[bool, str]:
    root_dir = os.path.dirname(os.path.abspath(__file__))
    now_est = datetime.now(EST_TZ)

    if not force and is_update_lockdown_window(now_est):
        return (
            False,
            "🛡️ [Case 07 장중 락다운] 평일 04:00 ~ 16:05 EST 구간에는 무인 업데이트가 차단됩니다. (강제 실행: /update force)",
        )

    has_ord, ord_reason = has_active_orders_in_ledger(base_dir=root_dir)
    if not force and has_ord:
        return (
            False,
            f"🛡️ [원장 보호 가드] {ord_reason} -> 주문 보호를 위해 업데이트를 보류합니다.",
        )

    code, current_hash, err = run_command(
        "git rev-parse HEAD", timeout=30, cwd=root_dir
    )
    if code != 0 or not current_hash:
        return False, f"🚨 Git 해시 백업 실패: {err}"

    branch = str(os.getenv("SNIPER_GIT_BRANCH", "main") or "main").strip()
    f_code, _f_out, f_err = run_command(
        f"git fetch origin {branch}", timeout=30, cwd=root_dir
    )
    if f_code != 0:
        return False, f"🚨 Git Fetch 실패: {f_err}"

    r_code, r_out, r_err = run_command(
        f"git reset --hard origin/{branch}", timeout=30, cwd=root_dir
    )
    if r_code != 0:
        run_command(f"git reset --hard {current_hash}", timeout=30, cwd=root_dir)
        return False, f"🚨 Git Reset 동기화 실패: {r_err}"

    ok, compile_errs = verify_all_modules_compile(
        base_dir=root_dir, strict_required=True
    )
    if not ok:
        run_command(f"git reset --hard {current_hash}", timeout=30, cwd=root_dir)
        return (
            False,
            "🚨 프리플라이트 컴파일 에러 감지 (이전 정상 해시로 즉시 롤백 완료):\n"
            + "\n".join(compile_errs[:5]),
        )

    head_line = r_out.strip() if r_out else ""
    if not head_line:
        _c, log_out, _e = run_command(
            "git log -1 --oneline", timeout=10, cwd=root_dir
        )
        head_line = f"HEAD is now at {log_out}"
    return True, head_line


# ---------------------------------------------------------------------------
# TelegramBotRouter : 비동기 롱폴링 · 무한 재기동 4중 방어벽 · 2대 제어 옵션 라우터
# ---------------------------------------------------------------------------
class TelegramBotRouter:
    """
    제1헌법 & Case 02, 06, 07, 11, 13, 17, 18, 26:
    텔레그램 봇 폴링 라우터 및 4대 관제 보드 / 2대 제어 옵션(작동 ON/OFF, 시드머니 설정) 전담 클래스.
    """

    def __init__(
        self,
        context: Any,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> None:
        _autoload_dotenv_files(override=False)
        self.context = context
        self.bot_token = str(
            bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
        ).strip()
        self.chat_id = str(
            chat_id or os.getenv("TELEGRAM_CHAT_ID", "") or ""
        ).strip()
        self._session = requests.Session()
        self._offset: int = 0
        self._boot_epoch: float = time.time()
        self._startup_sent: bool = False
        self._stale_flushed: bool = False
        self._awaiting_seed_input: bool = False
        self._hold_alert_lock = threading.Lock()
        self._hold_alert_sent_keys: Dict[str, str] = {}

    def _refresh_credentials_if_needed(self) -> bool:
        if self.bot_token and self.chat_id:
            return True
        _autoload_dotenv_files(override=True)
        self.bot_token = str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
        self.chat_id = str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
        return bool(self.bot_token and self.chat_id)

    @property
    def is_configured(self) -> bool:
        return self._refresh_credentials_if_needed()

    def _tg_post_sync(
        self, method: str, payload: Dict[str, Any], timeout: float = 10.0
    ) -> Dict[str, Any]:
        if not self._refresh_credentials_if_needed():
            return {"ok": False, "description": "TELEGRAM_NOT_CONFIGURED"}
        GlobalThrottle.wait_api_sync("DEFAULT")
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        resp = self._session.post(url, json=payload, timeout=timeout)
        try:
            return resp.json()
        except Exception:
            return {"ok": False, "status_code": resp.status_code}

    async def _tg_post_async(
        self, method: str, payload: Dict[str, Any], timeout: float = 12.0
    ) -> Dict[str, Any]:
        if not self.is_configured:
            return {"ok": False}
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._tg_post_sync,
                    method,
                    payload,
                    max(timeout - 1.0, 4.0),
                ),
                timeout=timeout,
            )
        except Exception as exc:
            logger.debug("텔레그램 API 호출 예외 (%s): %s", method, exc)
            return {"ok": False, "error": str(exc)}

    async def _commit_offset_to_telegram_server(self) -> None:
        if self._offset <= 0:
            return
        try:
            await self._tg_post_async(
                "getUpdates",
                {
                    "offset": self._offset,
                    "limit": 1,
                    "timeout": 0,
                },
                timeout=5.0,
            )
        except Exception as exc:
            logger.debug("텔레그램 오프셋 커밋 예외: %s", exc)

    async def _flush_stale_updates_on_startup(self) -> None:
        if self._stale_flushed:
            return
        try:
            last_saved_uid = await asyncio.to_thread(
                _read_last_processed_update_id_sync
            )
            if last_saved_uid > 0 and (last_saved_uid + 1) > self._offset:
                self._offset = last_saved_uid + 1

            res = await self._tg_post_async(
                "getUpdates",
                {
                    "offset": -1,
                    "limit": 5,
                    "timeout": 0,
                },
                timeout=6.0,
            )
            updates = res.get("result") if isinstance(res, dict) else []
            if isinstance(updates, list) and updates:
                for upd in updates:
                    if not isinstance(upd, dict):
                        continue
                    uid = safe_int(upd.get("update_id"), 0)
                    if uid >= self._offset:
                        self._offset = uid + 1
                await self._commit_offset_to_telegram_server()
            self._stale_flushed = True
        except Exception as exc:
            logger.debug("기동 시 텔레그램 큐 플러시 예외: %s", exc)
            self._stale_flushed = True

    async def send_raw_html(
        self,
        html_text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        chat_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self.is_configured:
            return {"ok": False}
        target_chat = str(chat_id or self.chat_id).strip()
        if not target_chat:
            return {"ok": False}
        safe_html = str(html_text or "").replace("|", "·")
        payload: Dict[str, Any] = {
            "chat_id": target_chat,
            "text": safe_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return await self._tg_post_async("sendMessage", payload)

    async def edit_message_html(
        self,
        chat_id: str,
        message_id: int,
        html_text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        safe_html = str(html_text or "").replace("|", "·")
        payload: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "message_id": int(message_id),
            "text": safe_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            return await self._tg_post_async("editMessageText", payload)
        except Exception as exc:
            logger.debug("텔레그램 editMessageText 예외: %s", exc)
            return {"ok": False}

    async def send_startup_message(self) -> Dict[str, Any]:
        """
        🚨 Case 13: 서버 기동 완료 시 "✅ 서버가 정상 작동 중입니다." 단일 문구를 타전한다.
        """
        if not self.is_configured or self._startup_sent:
            return {"ok": False}
        self._startup_sent = True
        return await self.send_raw_html("✅ 서버가 정상 작동 중입니다.")

    async def send_slim_alert(self, title: str, body: str = "") -> Dict[str, Any]:
        safe_title = sanitize_tg_text(title)
        safe_body = sanitize_tg_text(body)
        if safe_body:
            msg = f"<b>{safe_title}</b>\n🔸 {safe_body}"
        else:
            msg = f"<b>{safe_title}</b>"
        return await self.send_raw_html(msg)

    async def send_strike_hold_alert_once(
        self, symbol: str, reason_key: str, message: str
    ) -> None:
        logical_date = get_logical_trading_date_str(rollover_hour=18)
        composite_key = f"{symbol}:{reason_key}"
        with self._hold_alert_lock:
            if self._hold_alert_sent_keys.get(composite_key) == logical_date:
                return
            self._hold_alert_sent_keys[composite_key] = logical_date

        await self.send_slim_alert(
            title=f"⚠️ [{symbol} 타격/시드 점검 알림]",
            body=message,
        )

    # -----------------------------------------------------------------------
    # 대화형 시드머니 숫자 입력 처리 및 명령어 디스패처
    # -----------------------------------------------------------------------
    async def _apply_new_seed_and_reply(
        self, chat_id: str, new_seed_val: float
    ) -> None:
        """
        운용자가 채팅창에 시드머니 금액(예: 10000)을 입력하면 즉시 SOXL 원장 시드머니를 갱신하고
        ⚙️ [ SOXL 지층관리 전술 제어반 ] 보드를 전송한다.
        """
        self._awaiting_seed_input = False
        clean_seed = round(max(100.0, float(new_seed_val)), 2)
        await self.context.ledger.set_initial_seed_usd(clean_seed)
        await self.context.ledger.update_state(
            TARGET_SYMBOL,
            buy_interval_min=10,
            portion_mode="COMP_08PCT",
            threshold_mode="08_VREV_PROG",
            is_compound=True,
        )
        self.context.wakeup_event.set()
        board_html, markup = await render_settlement_control_board(self.context)
        await self.send_raw_html(
            board_html, reply_markup=markup, chat_id=chat_id
        )

    async def _handle_incoming_message(
        self,
        chat_id: str,
        text: str,
        update_id: int = 0,
        msg_date: int = 0,
    ) -> None:
        raw_txt = str(text or "").strip()
        if not raw_txt:
            return

        # 1) 채팅창에 순수 숫자(예: "10000", "$10,000")를 입력한 경우 시드머니 설정으로 즉시 처리
        cleaned_num = raw_txt.replace("$", "").replace(",", "").strip()
        try:
            num_val = float(cleaned_num)
            if (
                not math.isnan(num_val)
                and not math.isinf(num_val)
                and num_val >= 100.0
            ):
                await self._apply_new_seed_and_reply(chat_id, num_val)
                return
        except ValueError:
            pass

        if not raw_txt.startswith("/"):
            return

        parts = raw_txt.split()
        cmd = parts[0].lower().split("@")[0]

        if cmd in ("/start", "/menu", "/help"):
            self._awaiting_seed_input = False
            board_html, markup = await render_main_menu_board(self.context)
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd in ("/avwap", "/status", "/radar"):
            self._awaiting_seed_input = False
            board_html, markup = await render_avwap_radar_board(self.context)
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd == "/sync":
            self._awaiting_seed_input = False
            board_html, markup = await render_sync_session_board(self.context)
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd in ("/settlement", "/seed", "/config"):
            self._awaiting_seed_input = False
            if len(parts) >= 2:
                seed_arg = safe_float(
                    parts[1].replace("$", "").replace(",", ""), 0.0
                )
                if seed_arg >= 100.0:
                    await self._apply_new_seed_and_reply(chat_id, seed_arg)
                    return
            board_html, markup = await render_settlement_control_board(
                self.context
            )
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd in ("/reset", "/reset_seed"):
            self._awaiting_seed_input = False
            if msg_date > 0 and float(msg_date) < (self._boot_epoch - 5.0):
                await self._commit_offset_to_telegram_server()
                return
            seed_override: Optional[float] = None
            if len(parts) >= 2:
                s_val = safe_float(
                    parts[1].replace("$", "").replace(",", ""), 0.0
                )
                if s_val >= 100.0:
                    seed_override = s_val
            msg = await handle_reset_command(
                self.context, new_seed_usd=seed_override
            )
            await self.send_raw_html(msg, chat_id=chat_id)
            return

        if cmd == "/update":
            self._awaiting_seed_input = False

            last_done_uid = await asyncio.to_thread(
                _read_last_processed_update_id_sync
            )
            if update_id > 0 and update_id <= last_done_uid:
                await self._commit_offset_to_telegram_server()
                return

            if msg_date > 0 and float(msg_date) < (self._boot_epoch - 3.0):
                if update_id > 0:
                    await asyncio.to_thread(
                        _write_last_processed_update_id_sync, update_id
                    )
                await self._commit_offset_to_telegram_server()
                return

            if update_id > 0:
                await asyncio.to_thread(
                    _write_last_processed_update_id_sync, update_id
                )
            await self._commit_offset_to_telegram_server()

            ok, summary = await asyncio.to_thread(
                _run_git_update_and_compile_sync, True
            )
            safe_sum = sanitize_tg_text(summary)
            if not ok:
                await self.send_raw_html(
                    f"⚠️ <b>[업데이트 중단]</b>\n<blockquote>{safe_sum}</blockquote>",
                    chat_id=chat_id,
                )
                return

            await self.send_raw_html(
                f"🚀 <b>탑재 성공. 재기동 격발.</b>\n<blockquote>업데이트 성공:\n{safe_sum}</blockquote>",
                chat_id=chat_id,
            )
            await self._commit_offset_to_telegram_server()
            await asyncio.sleep(0.5)
            os._exit(0)

    # -----------------------------------------------------------------------
    # 인라인 버튼 콜백 디스패처 (작동 ON/OFF & 시드머니 설정 & 보드 네비게이션)
    # -----------------------------------------------------------------------
    async def _handle_callback_query(self, cb: Dict[str, Any]) -> None:
        cb_id = str(cb.get("id") or "")
        data = str(cb.get("data") or "").strip()
        msg_obj = cb.get("message") if isinstance(cb.get("message"), dict) else {}
        chat_obj = (
            msg_obj.get("chat") if isinstance(msg_obj.get("chat"), dict) else {}
        )
        chat_id = str(chat_obj.get("id") or self.chat_id)
        message_id = safe_int(msg_obj.get("message_id"), 0)

        toast_text = "처리 완료"

        try:
            if data == "toggle_bot_active":
                st = self.context.ledger.get_state(TARGET_SYMBOL)
                next_active = not bool(st.get("is_active", True))
                await self.context.ledger.update_state(
                    TARGET_SYMBOL,
                    is_active=next_active,
                    buy_interval_min=10,
                    portion_mode="COMP_08PCT",
                    threshold_mode="08_VREV_PROG",
                    is_compound=True,
                )
                if next_active:
                    await relock_stratum_on_bot_resume(self.context)
                    toast_text = "🟢 SOXL 지층관리 스나이퍼 가동 ON"
                else:
                    toast_text = "🔴 SOXL 지층관리 스나이퍼 정지 OFF"
                self.context.wakeup_event.set()
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "prompt_seed_input":
                self._awaiting_seed_input = True
                toast_text = "💵 채팅창에 시드머니 금액(숫자)을 입력해주세요."
                prompt_msg = (
                    "💵 <b>[시드머니 설정]</b>\n"
                    "설정한 시드머니(및 누적 수익금/자투리 절약금 합산 총자산)의 <b>8% (12.5분할)</b>를 기준으로 "
                    "직전 기준가에서 1회 매수 정량 주수가 자동 확정됩니다.\n\n"
                    "적용할 시드머니 금액(USD 숫자, 예: <code>10000</code>)을 채팅창에 입력해주세요."
                )
                await self.send_raw_html(prompt_msg, chat_id=chat_id)

            elif data == "open_main_menu":
                self._awaiting_seed_input = False
                toast_text = "🕒 메인 메뉴"
                board_html, markup = await render_main_menu_board(self.context)
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_avwap":
                self._awaiting_seed_input = False
                toast_text = "🔫 스나이퍼 레이더 최신화"
                board_html, markup = await render_avwap_radar_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_settlement":
                self._awaiting_seed_input = False
                toast_text = "⚙️ 전술 제어반 최신화"
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_sync":
                self._awaiting_seed_input = False
                toast_text = "📜 지층 지시서 최신화"
                board_html, markup = await render_sync_session_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

        except Exception as exc:
            logger.warning("⚠️ 텔레그램 콜백 처리 예외 (%s): %s", data, exc)
            toast_text = "처리 중 일시 오류가 발생했습니다."

        if cb_id:
            await self._tg_post_async(
                "answerCallbackQuery",
                {"callback_query_id": cb_id, "text": toast_text[:180]},
            )

    async def polling_loop(self) -> None:
        logged_wait = False
        while getattr(self.context, "running", True):
            if not self._refresh_credentials_if_needed():
                if not logged_wait:
                    logger.info(
                        "ℹ️ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 미감지 (/home/pipiosToss/.env 대기 중)"
                    )
                    logged_wait = True
                await asyncio.sleep(5.0)
                continue

            if logged_wait:
                logger.info(
                    "🤖 /home/pipiosToss/.env 텔레그램 토큰 동적 로드 완료 -> 롱폴링 라우터 가동"
                )
                logged_wait = False

            if not self._stale_flushed:
                await self._flush_stale_updates_on_startup()

            if not self._startup_sent:
                await self.send_startup_message()

            try:
                res = await self._tg_post_async(
                    "getUpdates",
                    {
                        "offset": self._offset,
                        "timeout": 15,
                        "allowed_updates": ["message", "callback_query"],
                    },
                    timeout=22.0,
                )
                updates = res.get("result") if isinstance(res, dict) else []
                if isinstance(updates, list):
                    for upd in updates:
                        if not isinstance(upd, dict):
                            continue
                        uid = safe_int(upd.get("update_id"), 0)
                        if uid >= self._offset:
                            self._offset = uid + 1

                        if isinstance(upd.get("message"), dict):
                            msg = upd["message"]
                            chat_obj = (
                                msg.get("chat")
                                if isinstance(msg.get("chat"), dict)
                                else {}
                            )
                            cid = str(chat_obj.get("id") or "")
                            txt = str(msg.get("text") or "").strip()
                            msg_date = safe_int(msg.get("date"), 0)
                            if txt and (
                                not self.chat_id or cid == self.chat_id
                            ):
                                await self._handle_incoming_message(
                                    chat_id=cid,
                                    text=txt,
                                    update_id=uid,
                                    msg_date=msg_date,
                                )

                        elif isinstance(upd.get("callback_query"), dict):
                            await self._handle_callback_query(
                                upd["callback_query"]
                            )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("텔레그램 폴링 루프 대기: %s", exc)
                await asyncio.sleep(2.0)
