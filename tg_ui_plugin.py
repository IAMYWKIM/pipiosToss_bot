# -*- coding: utf-8 -*-
"""
# MODIFIED: tg_ui_plugin.py
텔레그램 비동기 롱폴링 라우터, /update 무한 재기동 원천 차단 4중 방어벽,
스크린샷(1000031235.jpg) 100% 동기화 전술 제어반(/settlement), 대화형 통합 시드 숫자 입력기,
메인 메뉴(/menu), 듀얼 지시서(/sync), 스나이퍼 레이더(/avwap) 및 /reset 원자적 소각 플러그인 (Step 01 / 04)
- MODIFIED: /update 실행 후 os._exit(0) 재기동 시 텔레그램 서버 큐에 미커밋된 /update 가 재수신되어
  무한 재기동 루프에 빠지는 현상을 막기 위해 4중 오프셋/타임스탬프/update_id 멱등성 방어벽 탑재
  1) 기동 직후 _flush_stale_updates_on_startup() 으로 과거 잔류 업데이트 큐 즉시 소각
  2) /update 수신 즉시 _commit_offset_to_telegram_server() 로 텔레그램 본사 서버에 offset=uid+1 강제 확정
  3) message.date < _boot_epoch 과거 타임스탬프 /update 명령 무시
  4) /tmp/toss_sniper_last_update_id.txt 에 처리된 update_id 를 원자적 기록하여 동일 ID 재실행 영구 차단
- 제1헌법: 모든 텔레그램/환율 HTTP 요청 및 파일/서브프로세스 I/O의 asyncio.to_thread 격리 및 GlobalThrottle 통제
- 제4헌법: 모든 논리 판단은 EST_TZ(America/New_York) 100% 락온
- Case 02, 06, 07, 11, 13, 17, 18, 26, 38, 39, 40, 42, 47, 50, 55, 56, 68, 93 완벽 준수
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

from daymarket_sim_plugin import create_default_day_sim_state
from defense_plugin import relock_qqq_baseline_on_bot_resume
from plugin_updater import (
    has_active_orders_in_ledger,
    is_update_lockdown_window,
    run_command,
    verify_all_modules_compile,
)
from quant_engine import NQ_OVERHEAT_THRESHOLD, calc_qqq_adverse_status
from quant_indicators_plugin import (
    calc_session_expected_band,
    compute_daily_5ma_and_expected_band,
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    GlobalThrottle,
    _autoload_dotenv_files,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.tg_ui_plugin")

# 모듈 임포트 시점 /home/pipiosToss/.env 단일 경로 자동 로드 확인
_autoload_dotenv_files(override=False)

# 전략 공식 코드명 및 구분선 상수
STRATEGY_CODE_LABEL: str = "👑VWAP_TREND_M050"
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
# NEW: /update 무한 재기동 방지용 update_id 원자적 파일 기록/조회 헬퍼 (제1헌법 & Case 06)
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
# 서머타임(EDT / EST) 판별 · 세션 헤더 · 출동 상태 라벨 포맷터
# ---------------------------------------------------------------------------
def get_dst_status_badges(now_est: Optional[datetime] = None) -> Dict[str, str]:
    """
    ZoneInfo('America/New_York') 의 dst() 오프셋을 확인하여
    서머타임 ON(EDT) / OFF(EST) 배지 문자열을 반환한다.
    """
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
    현재 EST 시각에 따른 /sync 및 /avwap 상단 세션 배너와 /settlement 출동 상태 문구를 반환한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()
    hm = dt.hour * 100 + dt.minute

    if (wd == 4 and hm >= 1700) or wd == 5 or (wd == 6 and hm < 1800):
        return {
            "sync_banner": "🌙 주말 휴장 (관망)",
            "radar_banner": "🌙 주말 휴장 · 시스템 대기",
            "dispatch_status": "🔒 주말 휴장 (관망)",
        }
    if 1800 <= hm <= 1859:
        return {
            "sync_banner": "🌐 선물 신세션 오픈 (데이장 대기)",
            "radar_banner": "🌐 선물 신세션 · QQQ 기준가 락온",
            "dispatch_status": "🌐 선물 신세션 감시 중",
        }
    if hm >= 1900 or hm <= 357:
        return {
            "sync_banner": "🌙 데이마켓 (🔔100% [모의] 추격)",
            "radar_banner": "🌙 데이마켓 · 🔔100% [모의] 스나이퍼",
            "dispatch_status": "🧪 데이장 [모의] 가동 중 (실매수 차단)",
        }
    if 358 <= hm <= 406:
        return {
            "sync_banner": "🌪️ 03:58 1주 휩소 노출 작전",
            "radar_banner": "🌪️ 03:58 ~ 04:07 · 1주 휩소 작전",
            "dispatch_status": "🌪️ 1주 휩소 노출 작전 가동 중",
        }
    if 407 <= hm <= 814:
        return {
            "sync_banner": f"🌅 프리장 골든타임 ({STRATEGY_CODE_LABEL})",
            "radar_banner": f"🌅 프리장 본진 · {STRATEGY_CODE_LABEL}",
            "dispatch_status": "🔫 프리장 본진 골든타임 출동 중",
        }
    if 815 <= hm <= 929:
        return {
            "sync_banner": "🔒 프리장 후반 (08:15 선제 컷오프)",
            "radar_banner": "🔒 프리장 후반 · 08:15 컷오프 완료",
            "dispatch_status": "🔒 08:15 선제 컷오프 (보유분 감시)",
        }
    if 930 <= hm <= 1459:
        return {
            "sync_banner": "🔥 정규장 본장 (트레일링 · 3차 방어)",
            "radar_banner": "🔥 정규장 본장 · 트레일링 감시",
            "dispatch_status": "🔒 정규장 본장 (신규매수 마감)",
        }
    if 1500 <= hm <= 1558:
        return {
            "sync_banner": "🛟 정규장 마감 1시간 전 (VWAP 탈출 감시)",
            "radar_banner": "🛟 15:00 ~ 15:58 · 정규장 VWAP 탈출",
            "dispatch_status": "🛟 미익절 정규장 VWAP 탈출 감시",
        }
    if 1559 <= hm <= 1601:
        return {
            "sync_banner": "🛑 15:59 제로-오버나이트 MOC 청산",
            "radar_banner": "🛑 15:59 ~ 16:01 · MOC 강제 청산",
            "dispatch_status": "🛑 제로-오버나이트 강제 청산 중",
        }
    return {
        "sync_banner": "🔒 정규장 마감 (관망)",
        "radar_banner": "🔒 정규장 마감 · 시스템 대기",
        "dispatch_status": "🔒 정규장 마감 (관망)",
    }


async def _fetch_usd_krw_rate(context: Any) -> float:
    """
    API.txt GET /api/v1/exchange-rate?baseCurrency=USD&quoteCurrency=KRW 를 호출하여
    60초 캐싱된 실시간 USD -> KRW 환율을 반환한다 (실패 시 1,380.0원 폴백).
    """
    with _FX_CACHE_LOCK:
        cached_rate = safe_float(_FX_USD_KRW_CACHE.get("rate"), 1380.0)
        age = time.monotonic() - safe_float(_FX_USD_KRW_CACHE.get("updated_mono"), 0.0)
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


def _format_symbol_directive_status(
    context: Any,
    symbol: str,
    holdings_qty: int,
    avg_price: float,
    other_holdings_qty: int,
) -> str:
    """
    /sync 및 /avwap 하단에 표출할 종목별 상태 문자열(예: '🎯 수동보유 (점화 $45.03)', '⏳ 대기') 생성.
    """
    st = context.ledger.get_state(symbol)
    if not bool(st.get("is_active", True)):
        return "⛔ 가동중지(OFF)"

    cfg_rate = safe_float(st.get("target_profit_rate"), 1.0)
    if cfg_rate <= 0.0:
        cfg_rate = 1.0

    if holdings_qty > 0 and avg_price > 0.0:
        arm_p = math.ceil((avg_price * (1.0 + cfg_rate / 100.0)) * 100.0) / 100.0
        entry_sess = str(st.get("entry_session") or "")
        t_state = getattr(context, "trail_runtime_cache", {}).get(symbol, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            return f"🔥 트레일링점화 (고점 ${safe_float(t_state.get('peak_price'), arm_p):.2f})"
        if entry_sess == "MANUAL":
            return f"🎯 수동보유 (점화 ${arm_p:.2f})"
        if entry_sess == "WHIPSAW_1SH":
            return f"🌪️ 1주휩소보유 (익절 ${arm_p:.2f})"
        return f"🎯 자동보유 (점화 ${arm_p:.2f})"

    buy_oid = str(st.get("buy_order_id") or "").strip()
    if buy_oid:
        buy_p = safe_float(st.get("buy_price"), 0.0)
        return f"🪤 매수덫대기 (${buy_p:.2f})"

    bottom_cache = getattr(context, "bottom_track_cache", {})
    if (
        isinstance(bottom_cache, dict)
        and bottom_cache.get("active")
        and str(bottom_cache.get("stopped_sym") or "") == symbol
    ):
        low_p = safe_float(bottom_cache.get("post_stop_low"), 0.0)
        return f"🪤 최저점반등감시 (저점 ${low_p:.2f})"

    if str(st.get("entry_session") or "") == "CANCELED_MACRO":
        return "🛑 과열차단 (NQ 2.0%초과)"

    if other_holdings_qty > 0:
        other_sym = "SOXS" if symbol == "SOXL" else "SOXL"
        return f"🔒 대기({other_sym} 운용중)"

    return "⏳ 대기"


# ---------------------------------------------------------------------------
# [보드 1] 메인 메뉴 (/start, /menu, /help, 🔙 메인 메뉴 버튼)
# ---------------------------------------------------------------------------
async def render_main_menu_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    🕒 [ 듀얼 스나이퍼 (🌞서머타임 ON (EDT)) ] 메인 메뉴 및 9단계 일일 스케줄러 보드 렌더링.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dst_info = get_dst_status_badges(dt_est)
    st_soxs = context.ledger.get_state("SOXS")
    is_comp = bool(st_soxs.get("is_compound", True))
    cfg_rate = safe_float(st_soxs.get("target_profit_rate"), 1.0)
    comp_badge = "통합 복리 ON" if is_comp else "고정 단리 ON"

    lines: List[str] = [
        f"🕒 <b>[ 듀얼 스나이퍼 ({dst_info['menu_header']}) ]</b>",
        f"▫️ 익절·복구: 🔥 +{cfg_rate:.2f}% 점화 하드보존 트레일링 ({comp_badge})",
        DIVIDER_LINE,
        "🔹 18:00 ~ 18:59: 🌐 선물 신세션 &amp; QQQ 기준가 락온",
        "🔹 19:00 ~ 03:57: 🌙 [모의] 데이장 (19:07 주도주·동적VWAP·다회전)",
        "🔹 03:58 ~ 03:59: 🔇 휩소 묵음",
        "🔹 03:58 ~ 04:07: 🌪️ '휩소 노출 작전'",
        f"🔹 04:07 ~ 08:14: 🔫 프리장 본진 ({STRATEGY_CODE_LABEL})",
        "🔹 08:15 ~ 14:59: 🔒 정규장 본장",
        "🔹 15:00 ~ 15:58: 🛟 미익절 VWAP 탈출",
        "🔹 15:59 ~ 16:01: 🛑 제로-오버나이트",
        "🔹 17:00: 🧹 가비지 컬렉션(GC)",
        "",
        "🛠 <b>[ 핵심 명령어 ]</b>",
        "▶️ /avwap : 🔫 스나이퍼 레이더 관제탑",
        "▶️ /sync : 📜 듀얼 지시서 동기화",
        "▶️ /settlement : ⚙️ 전술 제어반",
        "",
        "⚠️ /reset : 🧹 듀얼 장부 0점 소각",
        "",
        "⚠️ /update : 🚀 시스템 자가 업데이트",
        "",
        "🌙 수동 오버나이트를 원할 경우 [전술 제어반]에서 가동을 OFF 해주세요.",
    ]

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "refresh_avwap"},
                {"text": "📜 듀얼 지시서", "callback_data": "refresh_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "refresh_settlement"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 2] /settlement 전술 제어반 (스크린샷 1000031235.jpg 100% 동일 레이아웃)
# ---------------------------------------------------------------------------
async def render_settlement_control_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    ⚙️ [ 듀얼 스나이퍼 전술 제어반 ] — 스크린샷(1000031235.jpg) 100% 동기화 렌더러.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    banners = get_market_session_banners(dt_est)
    holdings = await context.api.get_holdings_all()

    st_soxs = context.ledger.get_state("SOXS")
    st_soxl = context.ledger.get_state("SOXL")

    is_active = bool(
        st_soxs.get("is_active", True) and st_soxl.get("is_active", True)
    )
    is_compound = bool(st_soxs.get("is_compound", True))
    budget = safe_float(st_soxs.get("budget"), 1000.0)
    target_rate = safe_float(st_soxs.get("target_profit_rate"), 1.0)

    soxs_qty = safe_int(holdings.get("SOXS", {}).get("qty"), 0)
    soxl_qty = safe_int(holdings.get("SOXL", {}).get("qty"), 0)
    if not is_active:
        dispatch_str = "⛔ 전술 가동 정지 (수동 오버나이트)"
    elif soxs_qty > 0:
        dispatch_str = f"🎯 SOXS {soxs_qty}주 교전 운용 중"
    elif soxl_qty > 0:
        dispatch_str = f"🎯 SOXL {soxl_qty}주 교전 운용 중"
    else:
        dispatch_str = banners["dispatch_status"]

    power_str = "🟢 ON" if is_active else "🔴 OFF"
    comp_str = "📈 통합 복리" if is_compound else "📌 고정 단리"

    lines: List[str] = [
        "⚙️ <b>[ 듀얼 스나이퍼 전술 제어반 ]</b>",
        DIVIDER_LINE,
        f"▫️ 마스터 전원: {power_str}",
        f"▫️ 출동 상태: {sanitize_tg_text(dispatch_str)}",
        f"▫️ 익절 엔진: 🔥 +{target_rate:.2f}% 점화 하드보존 트레일링 ({STRATEGY_CODE_LABEL})",
        f"▫️ 운용 방식: {comp_str}",
        f"▫️ 통합 시드: ${budget:,.2f}",
    ]

    toggle_power_btn = (
        {"text": "🔴 듀얼 스나이퍼 정지", "callback_data": "toggle_bot_active"}
        if is_active
        else {
            "text": "🟢 듀얼 스나이퍼 가동",
            "callback_data": "toggle_bot_active",
        }
    )
    toggle_comp_btn = (
        {
            "text": "💵 단리(시드고정) 전환",
            "callback_data": "toggle_compound_mode",
        }
        if is_compound
        else {
            "text": "📈 통합 복리 전환",
            "callback_data": "toggle_compound_mode",
        }
    )

    reply_markup = {
        "inline_keyboard": [
            [toggle_power_btn],
            [
                toggle_comp_btn,
                {"text": "💵 통합 시드 설정", "callback_data": "prompt_seed_input"},
            ],
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "refresh_avwap"},
                {"text": "🔙 메인 메뉴", "callback_data": "open_main_menu"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 3] /sync 듀얼 지시서 (주문가능금액 · 전일대비 고저 등락% · 달러/원화 손익)
# ---------------------------------------------------------------------------
async def render_sync_session_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    📜 [ SOXS & SOXL 듀얼 지시서 ] 렌더러.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dst_info = get_dst_status_badges(dt_est)
    banners = get_market_session_banners(dt_est)

    prices = await context.api.get_current_prices_batch(("SOXS", "SOXL"))
    holdings = await context.api.get_holdings_all()
    raw_bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.0)
    fx_rate = await _fetch_usd_krw_rate(context)

    lines: List[str] = [
        f"📜 <b>[ SOXS &amp; SOXL 듀얼 지시서 ({banners['sync_banner']}) ]</b>",
        f"📅 {dst_info['sync_header']}",
        f"💵 주문가능금액: ${raw_bp_usd:,.2f}",
        DIVIDER_LINE,
    ]

    raw_cache = getattr(context, "candle_metrics_cache", {})
    soxs_qty = safe_int(holdings.get("SOXS", {}).get("qty"), 0)
    soxl_qty = safe_int(holdings.get("SOXL", {}).get("qty"), 0)

    for sym in REAL_ALLOWED_SYMBOLS:
        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        live_p = safe_float(prices.get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
        live_p = safe_float(met.get("live_price"), 0.0)

        h_info = holdings.get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if live_p <= 0.0:
            live_p = safe_float(h_info.get("last_price"), avg_p)

        sess_h = safe_float(met.get("session_high"), live_p)
        sess_l = safe_float(met.get("session_low"), live_p)

        try:
            daily_candles, _ = await context.api.get_candles(
                symbol=sym, interval="1d", count=6, adjusted=True
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

        st = context.ledger.get_state(sym)
        budget = safe_float(st.get("budget"), 1000.0)
        comp_prefix = (
            "📈 통합복리시드" if st.get("is_compound", True) else "📌 고정단리시드"
        )
        other_qty = soxl_qty if sym == "SOXS" else soxs_qty
        status_txt = _format_symbol_directive_status(
            context, sym, h_qty, avg_p, other_qty
        )

        if h_qty > 0 and avg_p > 0.0 and live_p > 0.0:
            pnl_usd = (live_p - avg_p) * float(h_qty)
            pnl_pct = ((live_p - avg_p) / avg_p) * 100.0
            pnl_krw = int(round(pnl_usd * fx_rate))
        else:
            pnl_usd = 0.0
            pnl_pct = 0.0
            pnl_krw = 0

        usd_sign = "+" if pnl_usd >= 0.0 else "-"
        krw_sign = "+" if pnl_krw >= 0 else "-"
        pct_sign = "+" if pnl_pct >= 0.0 else ""

        icon = "🐻" if sym == "SOXS" else "🐂"
        lines.extend(
            [
                "",
                f"{icon} <b>[{sym}] 듀얼 스나이퍼 지시서 ({STRATEGY_CODE_LABEL})</b>",
                f"{comp_prefix}: ${budget:,.2f} · {sanitize_tg_text(status_txt)}",
                f"💰 현재 ${live_p:.2f} / 평단 ${avg_p:.2f} ({h_qty}주)",
                f"📈 금일 고가: ${sess_h:.2f} ({h_chg_pct:+.2f}%)",
                f"📉 금일 저가: ${sess_l:.2f} ({l_chg_pct:+.2f}%)",
                f"🔺 수익: {pct_sign}{pnl_pct:.2f}% ({usd_sign}${abs(pnl_usd):,.2f} · {krw_sign}₩{abs(pnl_krw):,})",
            ]
        )

    lines.extend(
        [
            "",
            DIVIDER_LINE,
            "▶️ /avwap : 🔫 듀얼 스나이퍼 레이더 관제탑",
        ]
    )

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
# [보드 4] /avwap 듀얼 스나이퍼 레이더 관제탑
# ---------------------------------------------------------------------------
async def render_avwap_radar_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    📡 [듀얼 스나이퍼 레이더] — 선물 진폭/3단 방어망, 5MA(어제 진폭) & HA(1m),
    프리장/정규장 VWAP 팩트·예상 밴드, 보유 상태 및 데이장 [모의] 전적을 표출하는 관제 보드 렌더러.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    banners = get_market_session_banners(dt_est)

    prices = await context.api.get_current_prices_batch(("SOXS", "SOXL", "QQQ"))
    holdings = await context.api.get_holdings_all()
    macro = await context.macro_cache.get_macro_snapshot(
        live_qqq_price=safe_float(prices.get("QQQ"), 0.0)
    )

    nq_p = safe_float(macro.get("nq_price"), 0.0)
    nq_h = safe_float(macro.get("nq_high"), 0.0)
    nq_l = safe_float(macro.get("nq_low"), 0.0)
    nq_amp = safe_float(macro.get("nq_amp"), 0.0)
    nq_low_base_amp = (
        ((nq_p - nq_l) / nq_l) * 100.0 if (nq_l > 0.0 and nq_p >= nq_l) else 0.0
    )
    qqq_p = safe_float(macro.get("qqq_price"), 0.0)
    dyn_thresh = safe_float(macro.get("dynamic_threshold_pct"), 0.80)

    soxs_qty = safe_int(holdings.get("SOXS", {}).get("qty"), 0)
    soxl_qty = safe_int(holdings.get("SOXL", {}).get("qty"), 0)
    active_sym = "SOXS" if soxs_qty > 0 else ("SOXL" if soxl_qty > 0 else "")

    if active_sym:
        st_act = context.ledger.get_state(active_sym)
        ent_sess = str(st_act.get("entry_session") or "MANUAL")
        if ent_sess == "MANUAL":
            stage12_str = "🟢 수동보유(2차 패스)"
        elif ent_sess == "WHIPSAW_1SH":
            stage12_str = "🟢 1주휩소(2차 패스)"
        elif nq_amp >= NQ_OVERHEAT_THRESHOLD:
            stage12_str = "🛑 과열경보(2.0%초과)"
        else:
            stage12_str = "🟢 정상가동"

        avg_act = safe_float(holdings.get(active_sym, {}).get("avg_price"), 0.0)
        live_act = safe_float(prices.get(active_sym), avg_act)
        adv = calc_qqq_adverse_status(
            symbol=active_sym,
            qqq_entry_price=safe_float(st_act.get("nq_entry_price"), qqq_p),
            qqq_curr_price=qqq_p,
            nq_entry_amp=safe_float(st_act.get("nq_entry_amp"), nq_amp),
            position_avg_price=avg_act,
            position_live_price=live_act,
        )
        act_icon = "🐻SOXS" if active_sym == "SOXS" else "🐂SOXL"
        ref_qqq = safe_float(st_act.get("nq_entry_price"), qqq_p)
        stage3_line = (
            f"▫️ 3차 방어({act_icon}): {adv['adverse_move_pct']:+.2f}% "
            f"(기준 ${ref_qqq:.2f} / 한도 +{adv['dynamic_threshold_pct']:.2f}%)"
        )
    else:
        stage12_str = (
            "🛑 과열차단(2.0%초과)"
            if nq_amp >= NQ_OVERHEAT_THRESHOLD
            else "🟢 정상 (진폭 2.0% 미만)"
        )
        stage3_line = (
            f"▫️ 3차 방어(QQQ): 대기 (현가 ${qqq_p:.2f} / 한도 +{dyn_thresh:.2f}%)"
        )

    raw_cache = getattr(context, "candle_metrics_cache", {})
    sym_data: Dict[str, Dict[str, Any]] = {}
    for sym in REAL_ALLOWED_SYMBOLS:
        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        live_p = safe_float(prices.get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
        live_p = safe_float(met.get("live_price"), 0.0)

        try:
            d_candles, _ = await context.api.get_candles(
                symbol=sym, interval="1d", count=6, adjusted=True
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
        sym_data[sym] = {"met": met, "band": band, "live_p": live_p}

    lines: List[str] = [
        f"📡 <b>[듀얼 스나이퍼 레이더]</b> ( {banners['radar_banner']} )",
        DIVIDER_LINE,
        "🌐 <b>나스닥 100 선물</b>",
        f"▫️ 고가: {nq_h:,.2f} · 저가: {nq_l:,.2f}",
        f"▫️ 현재: {nq_p:,.2f} · 총 진폭: {nq_amp:.2f}%",
        f"▫️ 저가기준 진폭: {nq_low_base_amp:.2f}%",
        f"▫️ 1·2차 방어: {stage12_str}",
        stage3_line,
        DIVIDER_LINE,
        "📊 <b>현재가 &amp; 5MA(어제 진폭) &amp; HA(1m)</b>",
    ]

    for sym in REAL_ALLOWED_SYMBOLS:
        icon = "🐻" if sym == "SOXS" else "🐂"
        sd = sym_data[sym]
        lp = sd["live_p"]
        amp_5ma = safe_float(sd["band"].get("amp_5ma_pct"), 0.0)
        yest_amp = safe_float(sd["band"].get("yesterday_amp_pct"), 0.0)
        ha_badge = "🕯️HA양봉" if sd["met"].get("is_ha_bull") else "🌑HA음봉"
        lines.append(
            f"{icon} {sym} ${lp:.2f} · {amp_5ma:.1f}%({yest_amp:.1f}%) · {ha_badge}"
        )

    pre_leader_sym = str(
        getattr(context, "pre_leader_cache", {}).get("leader_sym") or ""
    )
    pre_title = (
        f"🌅 <b>프리장 (👑주도주: {pre_leader_sym})</b>"
        if pre_leader_sym
        else "🌅 <b>프리장</b>"
    )
    lines.extend(["", pre_title])
    for sym in REAL_ALLOWED_SYMBOLS:
        icon = "🐻" if sym == "SOXS" else "🐂"
        met = sym_data[sym]["met"]
        amp_5ma = safe_float(sym_data[sym]["band"].get("amp_5ma_pct"), 0.0)
        pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
        ph = safe_float(met.get("pre_high"), 0.0)
        pl = safe_float(met.get("pre_low"), 0.0)
        p_amp = safe_float(met.get("pre_amp_pct"), 0.0)
        p_exp_l, p_exp_h = calc_session_expected_band(ph, pl, amp_5ma)
        trap_m050 = safe_float(met.get("pre_dyn_trap_m050"), 0.0)
        trap_note = f" (덫 ${trap_m050:.2f})" if pre_vwap > 0.0 else ""

        lines.append(f"{icon} {sym} [VWAP] ${pre_vwap:.2f}{trap_note}")
        lines.append(f"  ⤷ 팩트: ${pl:.2f} ~ ${ph:.2f} ({p_amp:.1f}%)")
        lines.append(f"      · 예상: ${p_exp_l:.2f} ~ ${p_exp_h:.2f}")

    lines.extend(["", "🔥 <b>정규장</b>"])
    for sym in REAL_ALLOWED_SYMBOLS:
        icon = "🐻" if sym == "SOXS" else "🐂"
        met = sym_data[sym]["met"]
        amp_5ma = safe_float(sym_data[sym]["band"].get("amp_5ma_pct"), 0.0)
        reg_vwap = safe_float(met.get("reg_vwap"), 0.0)
        rh = safe_float(met.get("reg_high"), 0.0)
        rl = safe_float(met.get("reg_low"), 0.0)
        r_amp = safe_float(met.get("reg_amp_pct"), 0.0)
        r_exp_l, r_exp_h = calc_session_expected_band(rh, rl, amp_5ma)

        lines.append(f"{icon} {sym} [VWAP] ${reg_vwap:.2f}")
        lines.append(f"  ⤷ 팩트: ${rl:.2f} ~ ${rh:.2f} ({r_amp:.1f}%)")
        lines.append(f"      · 예상: ${r_exp_l:.2f} ~ ${r_exp_h:.2f}")

    lines.append(DIVIDER_LINE)

    for sym in REAL_ALLOWED_SYMBOLS:
        icon = "🐻" if sym == "SOXS" else "🐂"
        st = context.ledger.get_state(sym)
        on_badge = "ON" if st.get("is_active", True) else "OFF"
        h_qty = safe_int(holdings.get(sym, {}).get("qty"), 0)
        avg_p = safe_float(holdings.get(sym, {}).get("avg_price"), 0.0)
        other_qty = soxl_qty if sym == "SOXS" else soxs_qty
        ha_badge = (
            "🕯️HA양봉" if sym_data[sym]["met"].get("is_ha_bull") else "🌑HA음봉"
        )

        if h_qty > 0 and avg_p > 0.0:
            cfg_rate = safe_float(st.get("target_profit_rate"), 1.0)
            arm_p = (
                math.ceil((avg_p * (1.0 + cfg_rate / 100.0)) * 100.0) / 100.0
            )
            ent_sess = str(st.get("entry_session") or "MANUAL")
            mode_kor = "수동보유" if ent_sess == "MANUAL" else "자동보유"
            lines.append(
                f"{icon} {sym} [{on_badge}] 🎯 {mode_kor}({h_qty}주 · 점화 ${arm_p:.2f} · {ha_badge})"
            )
        else:
            status_short = _format_symbol_directive_status(
                context, sym, h_qty, avg_p, other_qty
            )
            lines.append(
                f"{icon} {sym} [{on_badge}] {sanitize_tg_text(status_short)}"
            )

    day_sim = getattr(context, "day_sim_state", {})
    d_tot = safe_int(day_sim.get("sim_trade_count"), 0)
    d_win = safe_int(day_sim.get("sim_win_count"), 0)
    d_loss = safe_int(day_sim.get("sim_loss_count"), 0)
    d_wrate = (float(d_win) / float(d_tot)) * 100.0 if d_tot > 0 else 0.0
    d_cum_net = safe_float(day_sim.get("sim_cum_net_rate_pct"), 0.0)
    d_sign = "+" if d_cum_net >= 0.0 else ""

    st_soxs = context.ledger.get_state("SOXS")
    budget_val = safe_float(st_soxs.get("budget"), 1000.0)
    seed_label = (
        "통합복리 시드" if st_soxs.get("is_compound", True) else "고정단리 시드"
    )

    lines.extend(
        [
            f"📊 데이장 [모의] 전적: {d_tot}출격 {d_win}승 {d_loss}패 (승률 {d_wrate:.1f}% · 세후 {d_sign}{d_cum_net:.2f}%)",
            f"💰 {seed_label}: ${budget_val:,.2f}",
            "",
            f"⏱️ 갱신: {dt_est.strftime('%m-%d %H:%M:%S')} EST",
        ]
    )

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔄 레이더 갱신", "callback_data": "refresh_avwap"},
                {"text": "📜 듀얼 지시서", "callback_data": "refresh_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "refresh_settlement"},
                {"text": "🔙 메인 메뉴", "callback_data": "open_main_menu"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [핵심 행정 1] /reset 명령어 원자적 0점 소각 핸들러 (Case 02)
# ---------------------------------------------------------------------------
async def handle_reset_command(
    context: Any,
    now_est: Optional[datetime] = None,
) -> str:
    """
    🚨 Case 02:
    텔레그램 /reset 명령어를 통해:
    1) SOXS 및 SOXL 듀얼 로컬 장부(AssassinLedger)의 주문/보유/세션 상태 필드를 원자적 0점 소각하되,
       통합 시드 금액(budget), 복리/단리 모드(is_compound), 목표 수익률(target_profit_rate)은 보존한다.
    2) 6대 인메모리 캐시(_TRAIL_RUNTIME_CACHE, _EXIT_SNAPSHOT_CACHE, _BOTTOM_TRACK_CACHE,
       _PRE_LEADER_CACHE, _DAY_LEADER_CACHE, _DAY_SIM_STATE) 및 인덱싱 그레이스 캐시를
       즉시 0점 초기화한다.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_date = get_logical_trading_date_str(dt_est, rollover_hour=18)

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        for oid_key in ("buy_order_id", "sell_order_id"):
            oid = str(st.get(oid_key) or "").strip()
            if oid:
                try:
                    await context.api.cancel_order(oid)
                except Exception:
                    pass

    await context.ledger.reset_dual_ledgers_preserve_seed()

    if hasattr(context, "trail_runtime_cache") and isinstance(
        context.trail_runtime_cache, dict
    ):
        context.trail_runtime_cache.clear()
    if hasattr(context, "exit_snapshot_cache") and isinstance(
        context.exit_snapshot_cache, dict
    ):
        context.exit_snapshot_cache.clear()
    if hasattr(context, "bottom_track_cache") and isinstance(
        context.bottom_track_cache, dict
    ):
        context.bottom_track_cache.clear()
        context.bottom_track_cache["active"] = False
    if hasattr(context, "pre_leader_cache") and isinstance(
        context.pre_leader_cache, dict
    ):
        hyb = bool(context.pre_leader_cache.get("hybrid_mode", False))
        context.pre_leader_cache.clear()
        context.pre_leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": False,
                "leader_sym": "",
                "vwap_0407": 0.0,
                "c_0407": 0.0,
                "disp_0407": 0.0,
                "notified": False,
                "hybrid_mode": hyb,
            }
        )
    if hasattr(context, "day_leader_cache") and isinstance(
        context.day_leader_cache, dict
    ):
        hyb_d = bool(context.day_leader_cache.get("hybrid_mode", False))
        context.day_leader_cache.clear()
        context.day_leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": False,
                "leader_sym": "",
                "vwap_1907": 0.0,
                "c_1907": 0.0,
                "disp_1907": 0.0,
                "notified": False,
                "hybrid_mode": hyb_d,
            }
        )
    if hasattr(context, "day_sim_state") and isinstance(
        context.day_sim_state, dict
    ):
        context.day_sim_state.clear()
        context.day_sim_state.update(create_default_day_sim_state(logical_date))
    if hasattr(context, "indexing_grace_cache") and isinstance(
        context.indexing_grace_cache, dict
    ):
        context.indexing_grace_cache.clear()
    if hasattr(context, "whipsaw_runtime_cache") and isinstance(
        context.whipsaw_runtime_cache, dict
    ):
        context.whipsaw_runtime_cache.clear()
    if hasattr(context, "macro_cache") and hasattr(
        context.macro_cache, "reset_runtime_cache"
    ):
        context.macro_cache.reset_runtime_cache()

    context.wakeup_event.set()
    st_after = context.ledger.get_state("SOXS")
    budget_kept = safe_float(st_after.get("budget"), 1000.0)
    comp_kept = "통합 복리" if st_after.get("is_compound", True) else "고정 단리"

    return (
        "🧹 <b>[/reset 듀얼 장부 및 6대 캐시 원자적 0점 소각 완료]</b>\n"
        f"🔸 보존 시드: <code>${budget_kept:,.2f}</code> ({comp_kept}) · SOXS/SOXL 대기 복원"
    )


# ---------------------------------------------------------------------------
# [핵심 행정 2] /update 시스템 자가 업데이트 핸들러
# ---------------------------------------------------------------------------
def _run_git_update_and_compile_sync(force: bool = False) -> Tuple[bool, str]:
    """
    백그라운드 스레드에서 실행되는 Git Pull + HEAD 요약 추출 + 10대 코어 모듈 py_compile 검증기.
    """
    root_dir = os.path.dirname(os.path.abspath(__file__))
    now_est = datetime.now(EST_TZ)

    if not force and is_update_lockdown_window(now_est):
        return (
            False,
            "🛡️ [Case 07 장중 락다운] 평일 03:30 ~ 17:00 EST 구간에는 무인 업데이트가 차단됩니다. (강제 실행: /update force)",
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
# TelegramBotRouter : 비동기 롱폴링 · 무한 재기동 4중 방어벽 · 인플레이스 제어반
# ---------------------------------------------------------------------------
class TelegramBotRouter:
    """
    제1헌법 & Case 02, 06, 07, 11, 13, 17, 18, 26, 38:
    텔레그램 봇 폴링 라우터 및 4대 관제 보드 / 대화형 시드 입력 / 자가 업데이트 전담 클래스.
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
        """
        # NEW [2차 방어벽]: 현재 self._offset 을 텔레그램 본사 서버에 즉시 커밋하여
        처리 완료된 메시지(/update 등)가 재기동 후 다시 내려오는 현상을 원천 차단한다.
        """
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
        """
        # NEW [1차 방어벽]: 프로세스 기동 직후 롱폴링에 들어가기 전에
        텔레그램 큐의 마지막(offset=-1) 업데이트를 확인하여 부팅 이전에 쌓인
        구버전 메시지(특히 직전 /update)를 모두 스킵하고 서버 큐를 깨끗이 비운다.
        """
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
        """
        Case 26: 인라인 버튼 갱신 시 제자리 갱신(In-place Edit)을 수행한다.
        """
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
        """
        🚨 Case 13 & Case 17 & Case 38:
        모바일 2줄 이내 초슬림 HTML 안전 알림을 타전한다.
        """
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
        """
        🚨 Case 18:
        동일 논리 거래일(세션) 내 동일한 보류/에러 사유(reason_key)에 대해 단 1회만 텔레그램으로 타전한다.
        """
        logical_date = get_logical_trading_date_str(rollover_hour=18)
        composite_key = f"{symbol}:{reason_key}"
        with self._hold_alert_lock:
            if self._hold_alert_sent_keys.get(composite_key) == logical_date:
                return
            self._hold_alert_sent_keys[composite_key] = logical_date

        await self.send_slim_alert(
            title=f"⚠️ [{symbol} 타격/통신 점검 알림]",
            body=message,
        )

    # -----------------------------------------------------------------------
    # 대화형 통합 시드 숫자 입력 처리 및 명령어 디스패처
    # -----------------------------------------------------------------------
    async def _apply_new_seed_and_reply(
        self, chat_id: str, new_seed_val: float
    ) -> None:
        """
        스크린샷(1000031235.jpg)처럼 숫자(예: 200)가 입력되면 양쪽 시드를 즉시 갱신하고
        ⚙️ [ 듀얼 스나이퍼 전술 제어반 ] 보드를 전송한다.
        """
        self._awaiting_seed_input = False
        clean_seed = round(max(10.0, float(new_seed_val)), 2)
        for s in REAL_ALLOWED_SYMBOLS:
            await self.context.ledger.update_state(s, budget=clean_seed)
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

        # 1) 채팅창에 순수 숫자(예: "200", "$200", "200.5")를 입력한 경우 시드 설정으로 즉시 처리
        cleaned_num = raw_txt.replace("$", "").replace(",", "").strip()
        try:
            num_val = float(cleaned_num)
            if (
                not math.isnan(num_val)
                and not math.isinf(num_val)
                and num_val >= 10.0
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
                seed_arg = safe_float(parts[1].replace("$", ""), 0.0)
                if seed_arg >= 10.0:
                    await self._apply_new_seed_and_reply(chat_id, seed_arg)
                    return
            board_html, markup = await render_settlement_control_board(
                self.context
            )
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd == "/reset":
            self._awaiting_seed_input = False
            if msg_date > 0 and float(msg_date) < (self._boot_epoch - 5.0):
                logger.info("ℹ️ 부팅 이전 과거 /reset 메시지 무시 (msg_date=%s)", msg_date)
                await self._commit_offset_to_telegram_server()
                return
            msg = await handle_reset_command(self.context)
            await self.send_raw_html(msg, chat_id=chat_id)
            return

        if cmd == "/update":
            self._awaiting_seed_input = False

            # [3차 & 4차 방어벽] 부팅 이전 과거 메시지이거나 이미 처리된 update_id 이면 즉시 스킵!
            last_done_uid = await asyncio.to_thread(
                _read_last_processed_update_id_sync
            )
            if update_id > 0 and update_id <= last_done_uid:
                logger.info(
                    "🛡️ 이미 처리 완료된 /update 명령 재수신 차단 (update_id=%d <= %d)",
                    update_id,
                    last_done_uid,
                )
                await self._commit_offset_to_telegram_server()
                return

            if msg_date > 0 and float(msg_date) < (self._boot_epoch - 3.0):
                logger.info(
                    "🛡️ 부팅 이전 과거 /update 명령 재수신 차단 (msg_date=%d < boot=%.0f)",
                    msg_date,
                    self._boot_epoch,
                )
                if update_id > 0:
                    await asyncio.to_thread(
                        _write_last_processed_update_id_sync, update_id
                    )
                await self._commit_offset_to_telegram_server()
                return

            # [2차 & 4차 방어벽] 업데이트 실행 직전 update_id 파일 기록 및 텔레그램 서버 오프셋 즉시 커밋!
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
            # os._exit(0) 직전 텔레그램 서버에 오프셋 최종 재커밋
            await self._commit_offset_to_telegram_server()
            await asyncio.sleep(0.5)
            os._exit(0)

    # -----------------------------------------------------------------------
    # 인라인 버튼 콜백 디스패처
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
                st = self.context.ledger.get_state("SOXS")
                next_active = not bool(st.get("is_active", True))
                for s in REAL_ALLOWED_SYMBOLS:
                    await self.context.ledger.update_state(
                        s, is_active=next_active
                    )
                if next_active:
                    await relock_qqq_baseline_on_bot_resume(self.context)
                    toast_text = "🟢 듀얼 스나이퍼 가동 (QQQ 0.00% 재락온)"
                else:
                    toast_text = "🔴 듀얼 스나이퍼 정지 (수동 오버나이트 허용)"
                self.context.wakeup_event.set()
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "toggle_compound_mode":
                st = self.context.ledger.get_state("SOXS")
                next_comp = not bool(st.get("is_compound", True))
                for s in REAL_ALLOWED_SYMBOLS:
                    await self.context.ledger.update_state(
                        s, is_compound=next_comp
                    )
                toast_text = (
                    "📈 통합 복리 모드 전환"
                    if next_comp
                    else "💵 단리(시드고정) 모드 전환"
                )
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "prompt_seed_input":
                self._awaiting_seed_input = True
                toast_text = "💵 채팅창에 시드 금액(숫자)을 입력해주세요."
                prompt_msg = (
                    "💵 <b>[통합 시드 설정]</b>\n"
                    "프리장 본진 · 최저점 반등 재진입 · 수동 스나이퍼 · 1주 휩소 작전 등 모든 청산 시 "
                    "<b>실제 발생한 순수익금(세전손익 - 왕복수수료 0.14%)</b>이 양쪽 장부 시드에 동시 가감됩니다.\n\n"
                    "적용할 통합 시드 금액(USD 숫자, 예: <code>200</code>)을 채팅창에 입력해주세요."
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
                toast_text = "📜 듀얼 지시서 최신화"
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
        """
        백그라운드 비동기 텔레그램 롱폴링 루프.
        """
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

            # 기동 직후 잔류 구버전 업데이트 큐를 먼저 비운 뒤 기동 메시지 전송
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
