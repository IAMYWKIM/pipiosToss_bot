# -*- coding: utf-8 -*-
"""
# MODIFIED: tg_ui_plugin.py
텔레그램 비동기 롱폴링 라우터, /home/pipiosToss/.env 동적 자격증명 복구기,
3대 전술 관제 보드(/avwap, /settlement, /sync), /reset 원자적 0점 소각,
인플레이스 콜백 제어반 및 모바일 2줄 초슬림 타전망 통합 플러그인 (Step 02 / 02)
- MODIFIED: toss_api._autoload_dotenv_files 연동으로 /home/pipios4006 참조 없이
  오직 /home/pipiosToss/.env 단일 경로에서 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 자동 로드 및 동적 복구
- 제1헌법: 모든 텔레그램 HTTP 요청의 asyncio.to_thread 비동기 격리 및 GlobalThrottle 통제
- 제3헌법: 구버전 +0.64% 고정 익절 버튼, set_rate_SOXS_ 콜백, RSI 패닉셀 알림 데드코드 완전 소각
- 제4헌법: 모든 논리 판단은 EST_TZ(America/New_York) 100% 락온, KST_TZ는 UI 표출 전용 격리
- Case 02: /reset 명령 시 시드/복리모드 보존 및 SOXS/SOXL 듀얼 장부 + 6대 인메모리 캐시 원자적 0점 소각
- Case 13: 모바일 2줄 이내 초슬림 알림(send_slim_alert) 및 기동 시 "✅ 서버가 정상 작동 중입니다." 단일 타전
- Case 17: html.escape 전역 적용, 원시 부등호 금지, 세로바 기호의 가운뎃점(·) 치환 강제
- Case 18: 세션별 동일 사유 1회 타전망(send_strike_hold_alert_once)
- Case 26: 인라인 버튼 클릭 시 editMessageText 제자리 갱신(In-place Edit) 사수
- Case 39, 40, 45, 47, 55, 56: /sync 당일 세션(19:00 EST~) 고/저가 및 5MA(count=6) 예상 밴드/휴장일 한글 맵핑
- Case 42, 50, 68, 93: /settlement 전술 제어반(봇 ON/OFF 시 QQQ 기준가 0.00% 재락온, 1.0%↔0.5% 토글, 복리/단리)
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from daymarket_sim_plugin import create_default_day_sim_state
from defense_plugin import relock_qqq_baseline_on_bot_resume
from quant_engine import NQ_OVERHEAT_THRESHOLD, calc_qqq_adverse_status
from quant_indicators_plugin import (
    compute_daily_5ma_and_expected_band,
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from toss_api import (
    EST_TZ,
    KST_TZ,
    REAL_ALLOWED_SYMBOLS,
    GlobalThrottle,
    _autoload_dotenv_files,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.tg_ui_plugin")

# 모듈 임포트 시점 /home/pipiosToss/.env 단일 경로 자동 로드 확인
_autoload_dotenv_files(override=False)

# ---------------------------------------------------------------------------
# Case 45 : 미국 증시(NYSE/NASDAQ) 주요 휴장일 영문 -> 한글 정밀 맵핑 사전
# ---------------------------------------------------------------------------
US_HOLIDAY_KR_MAP: Dict[str, str] = {
    "New Year's Day": "신정 (New Year's Day)",
    "Martin Luther King, Jr. Day": "마틴 루터 킹 데이",
    "Washington's Birthday": "대통령의 날 (Presidents' Day)",
    "Presidents' Day": "대통령의 날 (Presidents' Day)",
    "Good Friday": "성금요일 (Good Friday)",
    "Memorial Day": "현충일 (Memorial Day)",
    "Juneteenth National Independence Day": "준틴스 독립기념일 (Juneteenth)",
    "Juneteenth": "준틴스 독립기념일 (Juneteenth)",
    "Independence Day": "미국 독립기념일 (Independence Day)",
    "Labor Day": "근로자의 날 (Labor Day)",
    "Thanksgiving Day": "추수감사절 (Thanksgiving Day)",
    "Christmas Day": "크리스마스 (Christmas Day)",
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


def _format_session_status_label(
    context: Any,
    symbol: str,
    holdings_qty: int,
    avg_price: float,
    live_price: float,
    now_est: datetime,
) -> str:
    """
    Case 13 & Case 57:
    현재 EST 시각과 종목별 장부/보유 상태에 맞춰 직관적인 전술 상태 라벨을 반환한다.
    """
    st = context.ledger.get_state(symbol)
    if not bool(st.get("is_active", True)):
        return "⛔ 전술OFF (수동오버나이트)"

    hm = now_est.hour * 100 + now_est.minute
    buy_oid = str(st.get("buy_order_id") or "").strip()
    entry_sess = str(st.get("entry_session") or "").strip()
    cfg_rate = safe_float(st.get("target_profit_rate"), 1.0)

    if holdings_qty > 0 and avg_price > 0.0:
        pnl_pct = (
            ((live_price - avg_price) / avg_price) * 100.0
            if live_price > 0.0
            else 0.0
        )
        sign = "+" if pnl_pct >= 0.0 else ""
        t_state = getattr(context, "trail_runtime_cache", {}).get(symbol, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            return f"🔥 트레일링점화({sign}{pnl_pct:.2f}%)"
        return f"🛡️ 보유(+{cfg_rate:.1f}%목표 · {sign}{pnl_pct:.2f}%)"

    if buy_oid:
        buy_p = safe_float(st.get("buy_price"), 0.0)
        return f"⏳ 지정가매수대기(${buy_p:.2f} · {entry_sess})"

    bottom_cache = getattr(context, "bottom_track_cache", {})
    if (
        isinstance(bottom_cache, dict)
        and bottom_cache.get("active")
        and str(bottom_cache.get("stopped_sym") or "") == symbol
    ):
        low_p = safe_float(bottom_cache.get("post_stop_low"), 0.0)
        return f"🪤 최저점반등추적(저점 ${low_p:.2f})"

    if entry_sess == "CANCELED_MACRO":
        return "🛑 과열차단(NQ 2.0%초과)"

    if hm >= 1900 or hm <= 357:
        return "🧪 데이장 [모의] 가동중 (실매수차단)"
    if 358 <= hm <= 406:
        return "⚡ 03:58 1주 휩소 작전 구간"
    if 400 <= hm <= 406:
        return "🔍 프리장 04:07 주도주 수집중"
    if 407 <= hm <= 814:
        leader_sym = str(
            getattr(context, "pre_leader_cache", {}).get("leader_sym") or ""
        )
        if leader_sym == symbol:
            return "👑 프리장 주도주 (VWAP -0.50% 조준)"
        return "🔍 프리장 정찰 대기"
    if 815 <= hm <= 1600:
        return "장외대기 (08:15 컷오프 완료)"
    return "🌙 시스템 대기"


# ---------------------------------------------------------------------------
# [보드 1] /avwap 실시간 듀얼 스나이퍼 레이더 보드 렌더러
# ---------------------------------------------------------------------------
async def render_avwap_radar_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    🎯 1 ~ 🎯 5 & Case 17, 23, 38:
    /avwap (또는 /status, /start) 명령 시 호출되어
    1) 매크로 3단 방어망 (NQ=F 진폭/고저가, QQQ 0.0초 실시간 시세 및 10m/3m 속도, 3차 동적 임계치)
    2) 데이장(19:00 ~ 03:57 EST) 100% [모의] 동형 스나이퍼 현황 및 누적 승률/세후수익률
    3) 프리장/정규장 [실전] SOXS · SOXL 동적 VWAP, 이격도, -0.50% 매수덫, RSI, 하이킨 아시, 트레일링 상태
    를 HTML 안전 포맷으로 렌더링한다.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dt_kst = dt_est.astimezone(KST_TZ)

    prices = await context.api.get_current_prices_batch(("SOXS", "SOXL", "QQQ"))
    holdings = await context.api.get_holdings_all()
    macro = await context.macro_cache.get_macro_snapshot(
        live_qqq_price=safe_float(prices.get("QQQ"), 0.0)
    )

    nq_p = safe_float(macro.get("nq_price"), 0.0)
    nq_h = safe_float(macro.get("nq_high"), 0.0)
    nq_l = safe_float(macro.get("nq_low"), 0.0)
    nq_amp = safe_float(macro.get("nq_amp"), 0.0)
    dyn_thresh = safe_float(macro.get("dynamic_threshold_pct"), 0.80)
    qqq_p = safe_float(macro.get("qqq_price"), 0.0)
    qqq_3m = safe_float(macro.get("qqq_3m_chg_pct"), 0.0)
    qqq_10m = safe_float(macro.get("qqq_10m_chg_pct"), 0.0)
    overheat_badge = (
        "🛑과열(2.0%초과)" if nq_amp >= NQ_OVERHEAT_THRESHOLD else "🟢정상"
    )

    # 데이장 [모의] 요약
    day_leader = getattr(context, "day_leader_cache", {})
    day_sim = getattr(context, "day_sim_state", {})
    d_sym = str(day_leader.get("leader_sym") or "대기")
    d_vwap_1907 = safe_float(day_leader.get("vwap_1907"), 0.0)
    d_disp_1907 = safe_float(day_leader.get("disp_1907"), 0.0)
    d_phase = str(day_sim.get("phase") or "IDLE")
    d_trap_p = safe_float(day_sim.get("sim_trap_buy_price"), 0.0)
    d_ent_p = safe_float(day_sim.get("entry_price"), 0.0)
    d_tot = safe_int(day_sim.get("sim_trade_count"), 0)
    d_win = safe_int(day_sim.get("sim_win_count"), 0)
    d_wrate = (float(d_win) / float(d_tot)) * 100.0 if d_tot > 0 else 0.0
    d_cum_net = safe_float(day_sim.get("sim_cum_net_rate_pct"), 0.0)
    d_cum_sign = "+" if d_cum_net >= 0.0 else ""

    # 프리장 [실전] 주도주 요약
    pre_leader = getattr(context, "pre_leader_cache", {})
    p_sym = str(pre_leader.get("leader_sym") or "대기")
    p_vwap_0407 = safe_float(pre_leader.get("vwap_0407"), 0.0)
    p_disp_0407 = safe_float(pre_leader.get("disp_0407"), 0.0)

    lines: List[str] = [
        "📡 <b>[토스 스나이퍼 aVWAP 실시간 관제탑]</b>",
        f"🕒 EST <code>{dt_est.strftime('%Y-%m-%d %H:%M:%S')}</code> · KST <code>{dt_kst.strftime('%H:%M:%S')}</code>",
        "",
        f"🛡️ <b>[매크로 3단 방어망 · {overheat_badge}]</b>",
        f"🔸 NQ=F <code>{nq_p:,.2f}</code> (H <code>{nq_h:,.2f}</code> · L <code>{nq_l:,.2f}</code> · 진폭 <b>{nq_amp:.2f}%</b>)",
        f"🔸 QQQ <code>${qqq_p:.2f}</code> (3m <code>{qqq_3m:+.2f}%</code> · 10m <code>{qqq_10m:+.2f}%</code> · 3차임계 <b>{dyn_thresh:.2f}%</b>)",
        "",
        "🧪 <b>[데이장 19:00 ~ 03:57 [모의] 동형 스나이퍼]</b>",
        f"🔸 주도주 <b>{sanitize_tg_text(d_sym)}</b> (VWAP_1907 <code>${d_vwap_1907:.2f}</code> · 초기이격 <code>{d_disp_1907:+.2f}%</code>)",
        f"🔸 상태 <code>{sanitize_tg_text(d_phase)}</code> · 덫단가 <code>${d_trap_p:.2f}</code> · 진입가 <code>${d_ent_p:.2f}</code>",
        f"🔸 당일성적 <b>{d_tot}전 {d_win}승 (승률 {d_wrate:.1f}%)</b> · 세후누적 <b>{d_cum_sign}{d_cum_net:.2f}%</b>",
        "",
        f"👑 <b>[프리장 04:07 ~ 08:15 [실전] 추세 주도주 : {sanitize_tg_text(p_sym)}]</b>",
        f"🔸 VWAP_0407 <code>${p_vwap_0407:.2f}</code> (04:07 초기이격 <code>{p_disp_0407:+.2f}%</code>)",
    ]

    raw_cache = getattr(context, "candle_metrics_cache", {})
    for sym in REAL_ALLOWED_SYMBOLS:
        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        live_p = safe_float(prices.get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
        live_p = safe_float(met.get("live_price"), 0.0)

        h_info = holdings.get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        st = context.ledger.get_state(sym)

        day_vwap = safe_float(met.get("day_vwap"), 0.0)
        pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
        reg_vwap = safe_float(met.get("reg_vwap"), 0.0)
        pre_disp = safe_float(met.get("pre_disp_curr"), 0.0)
        trap_m050 = safe_float(met.get("pre_dyn_trap_m050"), 0.0)
        live_rsi = safe_float(met.get("live_rsi"), 50.0)
        ha_badge = "🕯️HA양봉" if met.get("is_ha_bull") else "🌑HA음봉"
        status_label = _format_session_status_label(
            context, sym, h_qty, avg_p, live_p, dt_est
        )

        icon = "🐻" if sym == "SOXS" else "🐂"
        lines.append("")
        lines.append(
            f"{icon} <b>[{sym}]</b> 현가 <code>${live_p:.2f}</code> · {ha_badge} · RSI <code>{live_rsi:.1f}</code>"
        )
        lines.append(f"🔸 상태: <b>{sanitize_tg_text(status_label)}</b>")
        lines.append(
            f"🔸 프리VWAP <code>${pre_vwap:.2f}</code> (이격 <code>{pre_disp:+.2f}%</code>) · -0.50%덫 <code>${trap_m050:.2f}</code>"
        )
        lines.append(
            f"🔸 데이VWAP <code>${day_vwap:.2f}</code> · 정규VWAP <code>${reg_vwap:.2f}</code>"
        )

        if h_qty > 0 and avg_p > 0.0:
            adv = calc_qqq_adverse_status(
                symbol=sym,
                qqq_entry_price=safe_float(st.get("nq_entry_price"), qqq_p),
                qqq_curr_price=qqq_p,
                nq_entry_amp=safe_float(st.get("nq_entry_amp"), nq_amp),
                position_avg_price=avg_p,
                position_live_price=live_p,
            )
            lines.append(
                f"🔸 보유 <code>{h_qty}주</code> (평단 <code>${avg_p:.2f}</code>) · QQQ역행 <code>{adv['adverse_move_pct']:+.2f}%</code> / <code>{adv['dynamic_threshold_pct']:.2f}%</code>"
            )

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔄 레이더 갱신", "callback_data": "refresh_avwap"},
                {
                    "text": "⚙️ 전술 제어반 (/settlement)",
                    "callback_data": "refresh_settlement",
                },
            ],
            [
                {"text": "📊 세션 고저/5MA (/sync)", "callback_data": "refresh_sync"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 2] /settlement 복리 시드 · 익절률 · 전술 ON/OFF 제어반 렌더러
# ---------------------------------------------------------------------------
async def render_settlement_control_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    🎯 4 & Case 42, 68, 76:
    /settlement 명령 시 호출되어 통합 시드 풀(budget), 복리/단리 모드, 목표 익절 점화율(1.0% ↔ 0.5%),
    동적/하이브리드 매수 덫 모드, 스나이퍼 전술 가동 ON/OFF 상태 및 수동 오버나이트 안내를 표출한다.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    st_soxs = context.ledger.get_state("SOXS")
    st_soxl = context.ledger.get_state("SOXL")

    is_active = bool(
        st_soxs.get("is_active", True) and st_soxl.get("is_active", True)
    )
    is_compound = bool(st_soxs.get("is_compound", True))
    budget = safe_float(st_soxs.get("budget"), 1000.0)
    target_rate = safe_float(st_soxs.get("target_profit_rate"), 1.0)
    hybrid_mode = bool(
        getattr(context, "pre_leader_cache", {}).get("hybrid_mode", False)
    )

    tot_trades = safe_int(st_soxs.get("trade_count"), 0) + safe_int(
        st_soxl.get("trade_count"), 0
    )
    tot_wins = safe_int(st_soxs.get("win_count"), 0) + safe_int(
        st_soxl.get("win_count"), 0
    )
    tot_pnl = safe_float(st_soxs.get("realized_pnl_usd"), 0.0) + safe_float(
        st_soxl.get("realized_pnl_usd"), 0.0
    )
    win_rate = (
        (float(tot_wins) / float(tot_trades)) * 100.0 if tot_trades > 0 else 0.0
    )
    pnl_sign = "+" if tot_pnl >= 0.0 else ""

    bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.5)

    active_str = (
        "🟢 가동중 (ON · 15:59 제로-오버나이트 사수)"
        if is_active
        else "⛔ 정지 (OFF · 수동 오버나이트 허용)"
    )
    comp_str = "♻️ 복리 (실발생 순수익 가감)" if is_compound else "📌 단리 (고정 예산)"
    trap_mode_str = (
        "🛡️ 하이브리드 min(VWAP_0407, VWAP_t×0.995)"
        if hybrid_mode
        else "⚡ 동적 VWAP_t × 0.995 (-0.50%)"
    )

    lines: List[str] = [
        "⚙️ <b>[인피니트 스노우볼 복리 · 전술 제어반]</b>",
        f"🕒 기준시각 EST <code>{dt_est.strftime('%Y-%m-%d %H:%M:%S')}</code>",
        "",
        f"🔸 <b>전술 가동 상태</b> : <b>{active_str}</b>",
        f"🔸 <b>통합 시드 예산</b> : <code>${budget:,.2f}</code> (계좌 안전예수금 <code>${bp_usd:,.2f}</code>)",
        f"🔸 <b>정산 운용 방식</b> : <b>{comp_str}</b> (왕복수수료 <code>0.14%</code> 차감)",
        f"🔸 <b>트레일링 점화율</b> : <b>+{target_rate:.2f}%</b> (안전캡 <code>+5.00%</code> · 보존잠금 <code>+1.30%</code>)",
        f"🔸 <b>매수 덫 산출식</b> : <b>{trap_mode_str}</b>",
        "",
        "📊 <b>[실전 누적 정산 성적]</b>",
        f"🔸 총 <b>{tot_trades}전 {tot_wins}승 (승률 {win_rate:.1f}%)</b> · 실발생 순수익 <b>{pnl_sign}${tot_pnl:,.2f}</b>",
        "",
        "💡 <b>[수동 오버나이트 안내]</b>",
        "🔸 오버나이트 홀딩을 원하실 경우 아래 <b>[전술 가동 ON/OFF]</b> 버튼으로 봇을 <b>OFF</b> 하십시오. 봇이 OFF 되면 15:59 EST 강제 청산(MOC) 루프가 정지되며, 다시 <b>ON</b> 하면 현재 QQQ 기준가(0.00%)로 3차 방어선이 재락온됩니다.",
    ]

    toggle_active_label = (
        "⛔ 전술 가동 OFF 전환"
        if is_active
        else "🟢 전술 가동 ON 전환 (QQQ 재락온)"
    )
    toggle_comp_label = "📌 단리 모드로 전환" if is_compound else "♻️ 복리 모드로 전환"
    toggle_rate_label = (
        "🎯 점화율 +0.50%로 전환"
        if abs(target_rate - 1.0) < 0.05
        else "🎯 점화율 +1.00%로 전환"
    )
    toggle_hyb_label = (
        "⚡ 순수 동적 VWAP(-0.50%) 전환"
        if hybrid_mode
        else "🛡️ 하이브리드 매수덫 전환"
    )

    reply_markup = {
        "inline_keyboard": [
            [{"text": toggle_active_label, "callback_data": "toggle_bot_active"}],
            [
                {"text": toggle_comp_label, "callback_data": "toggle_compound_mode"},
                {"text": toggle_rate_label, "callback_data": "toggle_profit_rate"},
            ],
            [{"text": toggle_hyb_label, "callback_data": "toggle_hybrid_trap"}],
            [
                {"text": "➖ 시드 -$100", "callback_data": "seed_adj_-100"},
                {"text": "➕ 시드 +$100", "callback_data": "seed_adj_+100"},
                {"text": "➕ 시드 +$500", "callback_data": "seed_adj_+500"},
            ],
            [
                {"text": "🔄 제어반 갱신", "callback_data": "refresh_settlement"},
                {"text": "📡 레이더 (/avwap)", "callback_data": "refresh_avwap"},
            ],
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [보드 3] /sync 당일 세션(19:00 EST~) 고/저가 및 5MA 예상 밴드 동기화 보드
# ---------------------------------------------------------------------------
async def render_sync_session_board(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Case 39, 40, 45, 47, 55, 56:
    /sync 명령 시 호출되어:
    1) 당일 세션(19:00 EST ~) 1분봉 동적 고/저가/진폭(session_high, session_low, session_amp_pct)과
    2) 토스 Open API 1d 캔들 6개(count=6, 당일 미완성 캔들 배제) 기반 5MA 평균 진폭, 어제 진폭,
       장세(상승/하락/횡보장), 당일 예상 고/저가 밴드 및 미국 시장 캘린더를 표출한다.
    """
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    prices = await context.api.get_current_prices_batch(("SOXS", "SOXL"))
    cal_info = await context.api.get_us_market_calendar(
        dt_est.strftime("%Y-%m-%d")
    )

    is_open_day = bool(cal_info.get("is_open_day", True))
    cal_badge = "🟢 정상 영업일" if is_open_day else "🛑 미국 증시 휴장일"
    if cal_info.get("is_fallback"):
        cal_badge += " (제6헌법 Fail-Open 폴백)"

    lines: List[str] = [
        "📊 <b>[당일 세션 고/저가 및 5MA 예상 밴드 동기화 (/sync)]</b>",
        f"🕒 EST <code>{dt_est.strftime('%Y-%m-%d %H:%M:%S')}</code> · {cal_badge}",
    ]

    raw_cache = getattr(context, "candle_metrics_cache", {})
    for sym in REAL_ALLOWED_SYMBOLS:
        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        live_p = safe_float(prices.get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_est)
        live_p = safe_float(met.get("live_price"), 0.0)
        sess_h = safe_float(met.get("session_high"), live_p)
        sess_l = safe_float(met.get("session_low"), live_p)
        sess_amp = safe_float(met.get("session_amp_pct"), 0.0)

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

        icon = "🐻" if sym == "SOXS" else "🐂"
        lines.append("")
        lines.append(
            f"{icon} <b>[{sym} · {sanitize_tg_text(band['market_regime'])}]</b> 현가 <code>${live_p:.2f}</code>"
        )
        lines.append(
            f"🔸 팩트 세션고저(19:00~): 고 <code>${sess_h:.2f}</code> · 저 <code>${sess_l:.2f}</code> (진폭 <b>{sess_amp:.2f}%</b>)"
        )
        lines.append(
            f"🔸 5MA 진폭(미완성배제): <b>{band['amp_5ma_pct']:.2f}%</b> (전일진폭 <code>{band['yesterday_amp_pct']:.2f}%</code>)"
        )
        lines.append(
            f"🔸 5MA 예상밴드: 예상고가 <code>${band['expected_high']:.2f}</code> · 예상저가 <code>${band['expected_low']:.2f}</code>"
        )

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "🔄 /sync 재동기화", "callback_data": "refresh_sync"},
                {"text": "📡 레이더 (/avwap)", "callback_data": "refresh_avwap"},
            ]
        ]
    }
    return "\n".join(lines), reply_markup


# ---------------------------------------------------------------------------
# [핵심 행정] /reset 명령어 원자적 0점 소각 핸들러 (Case 02)
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

    # 거래소에 남아있는 미체결 OPEN 주문도 안전하게 취소 시도
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

    # 6대 인메모리 캐시 원자적 0점 소각
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
    comp_kept = "복리" if st_after.get("is_compound", True) else "단리"

    return (
        "🧹 <b>[/reset 양방향 장부 및 6대 캐시 원자적 0점 소각 완료]</b>\n"
        f"🔸 보존 시드 <code>${budget_kept:,.2f}</code> ({comp_kept}) · SOXS/SOXL 대기 상태 복원 완료"
    )


# ---------------------------------------------------------------------------
# TelegramBotRouter : 비동기 롱폴링 · 인플레이스 버튼 편집 · 2줄 초슬림 타전망
# ---------------------------------------------------------------------------
class TelegramBotRouter:
    """
    제1헌법 & Case 02, 13, 17, 18, 26, 38:
    텔레그램 봇 폴링 라우터 및 알림/보드 전송 전담 클래스.
    - # MODIFIED: /home/pipiosToss/.env 단일 경로 자동 로더(_autoload_dotenv_files)를 통해
      런타임 중에도 텔레그램 토큰/챗ID를 동적 복구한다.
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
        self._startup_sent: bool = False
        self._hold_alert_lock = threading.Lock()
        self._hold_alert_sent_keys: Dict[str, str] = {}

    def _refresh_credentials_if_needed(self) -> bool:
        """
        # NEW: bot_token 또는 chat_id 가 비어 있을 경우 /home/pipiosToss/.env 를 즉시 재스캔하여 복구한다.
        """
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
        # 세로바 기호 치환 강제 (Case 17)
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
        if not self.is_configured:
            return {"ok": False}
        res = await self.send_raw_html("✅ 서버가 정상 작동 중입니다.")
        if isinstance(res, dict) and res.get("ok"):
            self._startup_sent = True
        return res

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
    # 명령어 및 인라인 콜백 디스패처
    # -----------------------------------------------------------------------
    async def _handle_text_command(self, chat_id: str, text: str) -> None:
        raw_cmd = str(text or "").strip()
        parts = raw_cmd.split()
        cmd = parts[0].lower().split("@")[0]

        if cmd in ("/start", "/avwap", "/status", "/radar"):
            board_html, markup = await render_avwap_radar_board(self.context)
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd in ("/settlement", "/seed", "/config"):
            # /seed 2500 형태로 직접 금액을 입력한 경우 즉시 양쪽 시드 변경
            if len(parts) >= 2:
                new_b = safe_float(parts[1], 0.0)
                if new_b >= 100.0:
                    for s in REAL_ALLOWED_SYMBOLS:
                        await self.context.ledger.update_state(
                            s, budget=round(new_b, 2)
                        )
            board_html, markup = await render_settlement_control_board(
                self.context
            )
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd == "/sync":
            board_html, markup = await render_sync_session_board(self.context)
            await self.send_raw_html(
                board_html, reply_markup=markup, chat_id=chat_id
            )
            return

        if cmd == "/reset":
            msg = await handle_reset_command(self.context)
            await self.send_raw_html(msg, chat_id=chat_id)
            return

        if cmd == "/help":
            help_lines = [
                "🤖 <b>[토스 스나이퍼 명령어 안내]</b>",
                "🔸 <code>/avwap</code> : 실시간 동적 VWAP · 3단 방어 · 데이장[모의] 레이더",
                "🔸 <code>/settlement</code> : 통합 시드 · 복리/단리 · 점화율(1.0%↔0.5%) · 봇 ON/OFF",
                "🔸 <code>/sync</code> : 당일 세션(19:00~) 고/저가 및 5MA 예상 밴드 동기화",
                "🔸 <code>/reset</code> : 양방향 장부 및 6대 캐시 원자적 0점 소각 (시드 보존)",
                "🔸 <code>/seed 2000</code> : 통합 시드 예산을 $2,000.00 으로 즉시 설정",
            ]
            await self.send_raw_html("\n".join(help_lines), chat_id=chat_id)

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
                    # OFF ➔ ON 재가동 시 현재 QQQ 기준가 0.00% 재락온 (Case 50, 93)
                    await relock_qqq_baseline_on_bot_resume(self.context)
                    toast_text = "🟢 스나이퍼 ON (QQQ 0.00% 기준가 재락온)"
                else:
                    toast_text = "⛔ 스나이퍼 OFF (수동 오버나이트 허용)"
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
                    "♻️ 복리 모드 전환" if next_comp else "📌 단리 모드 전환"
                )
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "toggle_profit_rate":
                # Case 68: +1.0% ↔ +0.5% 단일 익절 점화 변수 토글
                st = self.context.ledger.get_state("SOXS")
                cur_rate = safe_float(st.get("target_profit_rate"), 1.0)
                next_rate = 0.5 if abs(cur_rate - 1.0) < 0.05 else 1.0
                for s in REAL_ALLOWED_SYMBOLS:
                    await self.context.ledger.update_state(
                        s, target_profit_rate=next_rate
                    )
                toast_text = f"🎯 트레일링 점화율 +{next_rate:.2f}% 설정"
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "toggle_hybrid_trap":
                pre_c = getattr(self.context, "pre_leader_cache", {})
                day_c = getattr(self.context, "day_leader_cache", {})
                next_hyb = not bool(pre_c.get("hybrid_mode", False))
                pre_c["hybrid_mode"] = next_hyb
                day_c["hybrid_mode"] = next_hyb
                toast_text = (
                    "🛡️ 하이브리드 매수덫 ON"
                    if next_hyb
                    else "⚡ 순수 동적 VWAP -0.50% ON"
                )
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data.startswith("seed_adj_"):
                delta = safe_float(data.replace("seed_adj_", ""), 0.0)
                st = self.context.ledger.get_state("SOXS")
                cur_b = safe_float(st.get("budget"), 1000.0)
                next_b = max(100.0, round(cur_b + delta, 2))
                for s in REAL_ALLOWED_SYMBOLS:
                    await self.context.ledger.update_state(s, budget=next_b)
                toast_text = f"💰 통합 시드 ${next_b:,.2f} 설정"
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_avwap":
                toast_text = "📡 레이더 최신화 완료"
                board_html, markup = await render_avwap_radar_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_settlement":
                toast_text = "⚙️ 제어반 최신화 완료"
                board_html, markup = await render_settlement_control_board(
                    self.context
                )
                await self.edit_message_html(
                    chat_id, message_id, board_html, markup
                )

            elif data == "refresh_sync":
                toast_text = "📊 /sync 고저가 최신화 완료"
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
        # MODIFIED: 기동 시점에 /home/pipiosToss/.env 가 없었더라도 루프 내에서 동적 감지하여
        즉시 폴링을 시작하고 기동 완료 단일 문구(Case 13)를 타전한다.
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
                            if txt.startswith("/") and (
                                not self.chat_id or cid == self.chat_id
                            ):
                                await self._handle_text_command(cid, txt)

                        elif isinstance(upd.get("callback_query"), dict):
                            await self._handle_callback_query(
                                upd["callback_query"]
                            )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("텔레그램 폴링 루프 대기: %s", exc)
                await asyncio.sleep(2.0)
