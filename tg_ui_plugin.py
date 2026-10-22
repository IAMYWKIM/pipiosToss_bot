# -*- coding: utf-8 -*-
"""
# MODIFIED: tg_ui_plugin.py
인피니트 스노우볼 (Infinite Snowball) v3.0 — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
텔레그램 관제탑 UI 렌더러 및 명령어·콜백 비동기 컨트롤러 코어 모듈 (Step 04 / 05)
- 제1헌법: 모든 텔레그램 HTTP API 호출을 asyncio.to_thread 로 비동기 격리 및 타임아웃 적용
- 제4헌법: 모든 논리 시계열은 미국 동부 시간(EST/EDT) 100% 락온
- Case 11 & Case 62:
  1) MODIFIED: toss_api 심볼 불일치(KRW_PER_USD_DEFAULT) 방지를 위해 모듈 내부 자체 상수로 직접 정의하여
     `python3 plugin_updater.py --verify-only` AST 심볼 교차 검증 100% 통과 보장!
  2) 기동 직후 _flush_stale_updates_on_startup() 에서 조건 없이 큐의 마지막 update_id 까지 일괄 소각(Ack)
  3) /update 실행 시 오프셋 선행 확정 후 신규 커밋(changed == True)이 있을 때만 재기동(os._exit(0))하고,
     이미 최신 코드(changed == False)일 때는 재기동 없이 무중단 가동 유지!
- Case 15 & Case 16: 모바일 가독성 최적화, html.escape 전역 적용, 세로바 대신 가운뎃점(·) 사용,
  기동 완료 시 "✅ 서버가 정상 작동 중입니다." 생존 신호 자동 타전
- Case 64: 전술 제어반(/settlement) 내 30분 옵션 완전 제거 -> 오직 [작동 ON/OFF 버튼]과 [시드머니 설정 버튼] 2가지 옵션만 유지
- Case 65 & Case 66: /avwap 레이더 내 양방향 60초 타임쉴드(매수 후 60초 / 매도 후 60초) 상태 및 4단 와이드 활주로 실시간 표출
- NEW (Case 67): /avwap 레이더 및 /settlement 전술 제어반에서 차기 8% 정량 표출 시
  min(1회분 예산 상한 주수 budget_cap_qty, 기준가 고정 정량 anchor_quota_qty) 캡핑 상태를 실시간 반영!
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from defense_plugin import (
    reconcile_stratum_with_broker_holdings_plugin,
    relock_stratum_on_bot_resume,
)
from plugin_updater import execute_git_pull_and_preflight
from quant_engine import summarize_stratum_portfolio
from quant_indicators_plugin import (
    evaluate_buy_gate_bucket_status,
    get_market_session_label,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    is_within_post_buy_time_shield,
    is_within_post_sell_time_shield,
)
from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    EST_TZ,
    LOCKED_BUY_INTERVAL_MIN,
    TARGET_SYMBOL,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.tg_ui_plugin")

# MODIFIED: toss_api.py 와의 AST 심볼 임포트 불일치를 원천 차단하기 위해 기본 환율 상수를 모듈 내부에 직접 정의
KRW_PER_USD_DEFAULT: float = 1380.0


# ---------------------------------------------------------------------------
# [유틸리티] HTML 이스케이프 및 DST(서머타임) 상태 배지 산출 (Case 15)
# ---------------------------------------------------------------------------
def escape_html(val: Any) -> str:
    return html.escape(str(val if val is not None else ""), quote=False)


def _get_dst_badge(now_est: Optional[datetime] = None) -> str:
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    tz_name = dt.tzname() or "EST"
    if "EDT" in tz_name.upper():
        return "☀️서머타임 ON (EDT)"
    return "❄️서머타임 OFF (EST)"


def _format_case67_quota_badge(summary: Dict[str, Any]) -> str:
    """
    NEW [Case 67]:
    기준가 고정 정량(anchor_quota_qty)과 현재가 기준 1회분 예산 상한 주수(budget_cap_qty)의
    min(budget_cap_qty, anchor_quota_qty) 캡핑 상태를 직관적인 문자열로 반환한다.
    """
    next_qty = safe_int(summary.get("next_target_qty"), 1)
    anc_qty = safe_int(summary.get("anchor_quota_qty"), next_qty)
    cap_qty = safe_int(summary.get("budget_cap_qty"), next_qty)
    if next_qty < anc_qty:
        return f"<b>{next_qty}주</b> (🛡️min(1회분 {cap_qty}주, 고정 {anc_qty}주) 예산초과 차단)"
    return f"<b>{next_qty}주</b> (min(1회분, 고정 {anc_qty}주) 대기)"


def _build_main_reply_keyboard() -> Dict[str, Any]:
    """
    텔레그램 하단 고정 퀵 메뉴 키보드
    """
    return {
        "keyboard": [
            [
                {"text": "🔫 스나이퍼 레이더"},
                {"text": "📜 지층 지시서"},
                {"text": "⬅️ 메인 메뉴"},
            ],
            [
                {"text": "⚙️ 전술 제어반"},
                {"text": "🔄 잔고 동기화"},
                {"text": "🚀 시스템 업데이트"},
            ],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def _build_radar_inline_keyboard() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "🔄 레이더 갱신", "callback_data": "cmd_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "cmd_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "cmd_settlement"},
                {"text": "⬅️ 메인 메뉴", "callback_data": "cmd_start"},
            ],
        ]
    }


def _build_settlement_inline_keyboard(is_active: bool) -> Dict[str, Any]:
    """
    🚨 Case 64:
    전술 제어반(/settlement)에는 30분 옵션 없이 오직:
    1) [작동 ON / OFF 전환 버튼]
    2) [시드머니 설정 버튼]
    2가지 핵심 조작 옵션만 표출한다!
    """
    power_label = (
        "🔴 스나이퍼 작동 OFF 로 전환"
        if is_active
        else "🟢 스나이퍼 작동 ON 으로 전환"
    )
    return {
        "inline_keyboard": [
            [
                {"text": power_label, "callback_data": "toggle_bot_active"},
            ],
            [
                {
                    "text": "💵 기준 시드머니($) 설정",
                    "callback_data": "prompt_seed_input",
                },
            ],
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "cmd_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "cmd_sync"},
            ],
        ]
    }


# ---------------------------------------------------------------------------
# [핵심 1] 4대 관제 보드 메시지 빌더 (/start · /avwap · /sync · /settlement)
# ---------------------------------------------------------------------------
def build_start_schedule_message(
    st: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> str:
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    dst_str = _get_dst_badge(dt_now)
    init_seed = max(
        100.0,
        safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD),
    )

    lines = [
        f"🐂 <b>[ SOXL 지층관리 스나이퍼 ({escape_html(dst_str)}) ]</b>",
        "▫️ 전략: 🛡️ 05_AF_WIDE_10_01 (8% 황금정량 · 눌림/돌파 독립 1층 재장전 · 기억저가 와이드 활주로)",
        "▫️ 정량: 📐 min(1회분 8% 예산 상한 주수, 기준가 고정 정량) 이중 캡핑으로 돌파 시 예산 초과 차단 (Case 67)",
        "▫️ 매수: 🛡️ 10분 고정 4단 와이드 게이트 (기억저가 대비 1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5%)",
        "▫️ 재장전: 🔥 0주 즉시 진입 · 2층 단독 시 10분 게이트 눌림(-1.0%) OR 돌파(+1.0%) 독립 1층 산입",
        "▫️ 매도: ⚡ 1분 상시 현재가 지정가 매도 (1층 +1.50% 익절&amp;리앵커링 / 2층 +1.00% 졸업)",
        "▫️ 방어: ⏱️ 양방향 60초 강제 타임쉴드 &amp; 순수 현재가 지정가(LIMIT) 락온 (Case 65/66/67)",
        f"▫️ 기준 시드머니: <b>${init_seed:,.2f}</b> (1회 8% 정량 자동 산출)",
        "──────────────",
        "🔹 04:00 ~ 04:06: 🌅 프리장 호가 안정화 &amp; 오버나이트 지층 동기화",
        "🔹 04:07 ~ 09:29: 🔫 프리장 지층관리 가동 (10분 와이드매수게이트 · 1분 지정가매도)",
        "🔹 09:30 ~ 16:00: 🔥 정규장 지층관리 가동 (10분 와이드매수게이트 · 1분 지정가매도)",
        "🔹 16:00 ~ 16:59: 🌙 정규장 마감 일일 결산 &amp; 보유 지층 오버나이트 보존",
        "🔹 17:00 ~ 03:59: 🧹 일일 GC 및 장외 대기 (강제 손절 MOC 없음)",
        "──────────────",
        "📡 /avwap : 실시간 지층 스나이퍼 레이더 관제탑",
        "📜 /sync : 토스 실잔고 동기화 &amp; 지층 지시서",
        "⚙️ /settlement : 초슬림 전술 제어반 (작동 ON/OFF · 시드머니 설정)",
        "🧹 /reset : 지층 원장 0점 초기화 (/reset_seed 금액)",
        "🚀 /update : 최신 깃허브 코드 검증 업데이트",
    ]
    return "\n".join(lines)


def build_avwap_radar_message(
    context: Any,
    st: Dict[str, Any],
    metrics: Dict[str, Any],
    live_price: float,
    now_est: Optional[datetime] = None,
) -> str:
    """
    📡 /avwap 스나이퍼 레이더 관제탑 메시지 생성 (Case 15, 59, 60, 64, 65, 66, 67)
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    met = refresh_metrics_with_live_tick(metrics, live_price, now_est=dt_now)
    lp = safe_float(
        live_price,
        safe_float(
            met.get("live_price"), safe_float(st.get("anchor_price"), 0.0)
        ),
    )

    summary = summarize_stratum_portfolio(st, live_price=lp)
    is_active = bool(st.get("is_active", True))
    active_badge = "🟢 ON" if is_active else "🔴 OFF"
    fast_badge = (
        "⚡0.4s 부스터"
        if getattr(context, "fast_shield_mode", False)
        else "⏱️1.5s 표준"
    )

    ma5_amp = safe_float(met.get("ma5_amp_pct"), 6.50)
    ha_badge = str(met.get("ha_badge") or "🕯️HA양봉")

    pre_vwap = safe_float(met.get("premarket_vwap"), 0.0)
    pre_l = safe_float(met.get("premarket_low"), 0.0)
    pre_h = safe_float(met.get("premarket_high"), 0.0)
    pre_amp = safe_float(met.get("premarket_amp_pct"), 0.0)
    pre_el = safe_float(met.get("premarket_est_low"), 0.0)
    pre_eh = safe_float(met.get("premarket_est_high"), 0.0)

    reg_vwap = safe_float(met.get("regular_vwap"), 0.0)
    reg_l = safe_float(met.get("regular_low"), 0.0)
    reg_h = safe_float(met.get("regular_high"), 0.0)
    reg_amp = safe_float(met.get("regular_amp_pct"), 0.0)
    reg_el = safe_float(met.get("regular_est_low"), 0.0)
    reg_eh = safe_float(met.get("regular_est_high"), 0.0)

    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)
    buckets = summary.get("buckets", [])

    # 🛡️ Case 65: 양방향 60초 타임쉴드 작동 상태 확인
    in_buy_shield, rem_buy_sec = is_within_post_buy_time_shield(
        context, st, TARGET_SYMBOL
    )
    in_sell_shield, rem_sell_sec = is_within_post_sell_time_shield(
        context, st, TARGET_SYMBOL
    )
    shield_line = ""
    if in_buy_shield:
        shield_line = f"🛡️ <b>[매수 후 60초 타임쉴드 가동 중]</b> 잔여 <b>{rem_buy_sec:.1f}초</b> 후 현재가 조회 &amp; 매도 감시 개시"
    elif in_sell_shield:
        shield_line = f"🛡️ <b>[매도 후 60초 타임쉴드 가동 중]</b> 잔여 <b>{rem_sell_sec:.1f}초</b> 후 현재가 조회 &amp; 매수 감시 개시"

    # 2-Tier 지층 상태 라인 구성
    stratum_lines: List[str] = []
    if held_sh <= 0 or not buckets:
        stratum_lines.append(
            "⚪ <b>[보유 지층 없음 · 0주]</b> 60초 타임쉴드 확인 후 현재가 지정가 1층(REENTRY_1FL) 즉시 산입 대기"
        )
    elif bool(summary.get("is_l2_only_anti_bloat")):
        b2 = buckets[0]
        l2_t = safe_float(summary.get("l2_target_price"), 0.0)
        l2_g = safe_float(summary.get("l2_gain_pct"), 0.0)
        g_sign = "+" if l2_g >= 0.0 else ""
        stratum_lines.append(
            "⚡ <b>[1층: 공석 · 10분 게이트 눌림(-1%)/돌파(+1%) 대기]</b>"
        )
        stratum_lines.append(
            f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> 장부 <b>${b2.book_price:.2f}</b>"
            f"(실평단 ${b2.actual_avg_price:.2f}) · 졸업가 <b>${l2_t:.2f}</b>(+1.0%) [{g_sign}{l2_g:.2f}%]"
        )
    else:
        b1 = buckets[0]
        l1_t = safe_float(summary.get("l1_target_price"), 0.0)
        l1_g = safe_float(summary.get("l1_gain_pct"), 0.0)
        g1_sign = "+" if l1_g >= 0.0 else ""
        stratum_lines.append(
            f"⚡ <b>[1층: 공격수 {b1.qty}주]</b> 단가 <b>${b1.book_price:.2f}</b> · "
            f"익절목표 <b>${l1_t:.2f}</b>(+1.5%) [{g1_sign}{l1_g:.2f}%]"
        )
        if len(buckets) >= 2:
            b2 = buckets[1]
            l2_t = safe_float(summary.get("l2_target_price"), 0.0)
            l2_g = safe_float(summary.get("l2_gain_pct"), 0.0)
            g2_sign = "+" if l2_g >= 0.0 else ""
            stratum_lines.append(
                f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> 장부 <b>${b2.book_price:.2f}</b>"
                f"(실평단 ${b2.actual_avg_price:.2f}) · 졸업가 <b>${l2_t:.2f}</b>(+1.0%) [{g2_sign}{l2_g:.2f}%]"
            )

    # 10분 고정 와이드 매수 게이트 상태 구성
    _, _, _, gate_info = evaluate_buy_gate_bucket_status(
        now_est=dt_now,
        buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
        last_evaluated_bucket_id=safe_int(
            st.get("last_evaluated_buy_bucket_id"), -1
        ),
        last_evaluated_date=str(st.get("last_evaluated_date") or ""),
    )
    next_gate_str = str(gate_info.get("next_gate_hm_str") or "04:07 EST")
    next_bud = safe_float(summary.get("next_portion_budget"), 800.0)
    quota_badge_str = _format_case67_quota_badge(summary)
    ref_anchor = safe_float(summary.get("ref_buy_anchor"), lp)
    chg_anchor = safe_float(summary.get("chg_from_buy_anchor_pct"), 0.0)
    c_sign = "+" if chg_anchor >= 0.0 else ""

    if bool(summary.get("is_l2_only_anti_bloat")):
        pull_p = safe_float(summary.get("reentry_pullback_price"), 0.0)
        brk_p = safe_float(summary.get("reentry_breakout_price"), 0.0)
        locked_min_p = safe_float(summary.get("cycle_min_buy_price"), 0.0)
        gate_cond_line = (
            f"▫️ 직전익절 ${ref_anchor:.2f} 대비 {c_sign}{chg_anchor:.2f}% "
            f"(눌림목 <b>${pull_p:.2f}</b> 이하 OR 돌파 <b>${brk_p:.2f}</b> 이상 · 기억저가 ${locked_min_p:.2f})"
        )
    else:
        drop_req = safe_float(summary.get("drop_buy_pct"), -1.0)
        trig_p = safe_float(summary.get("trigger_buy_price"), 0.0)
        gate_cond_line = (
            f"▫️ 기억저가 ${ref_anchor:.2f} 대비 현재 {c_sign}{chg_anchor:.2f}% "
            f"(와이드단차 {drop_req:.2f}% · 신저가 <b>${trig_p:.2f}</b> 이하)"
        )

    reanchor_cnt = safe_int(summary.get("reanchor_cnt"), 0)
    reanchor_drop = safe_float(
        summary.get("cumulative_reanchor_drop_usd"), 0.0
    )
    pass_cnt = safe_int(summary.get("seed_protect_pass_cnt"), 0)
    extra_13 = safe_int(summary.get("extra_13plus_buy_events"), 0)
    saved_u = safe_float(summary.get("cumulative_leftover_saved_usd"), 0.0)
    cum_net = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    mark_eq = safe_float(summary.get("mark_equity_usd"), 0.0)
    mark_roi = safe_float(summary.get("mark_roi_pct"), 0.0)
    n_sign = "+" if cum_net >= 0.0 else "-"
    m_sign = "+" if mark_roi >= 0.0 else ""

    lines = [
        f"📡 <b>[SOXL 지층 스나이퍼 레이더]</b> ({active_badge} · {fast_badge})",
        f"🐂 <b>SOXL</b> [{active_badge}] 현재가 <b>${lp:.2f}</b> · 5MA {ma5_amp:.2f}% · {escape_html(ha_badge)}",
        f"🌅 프리장 VWAP ${pre_vwap:.2f} · 밴드 ${pre_l:.2f} ~ ${pre_h:.2f} ({pre_amp:.1f}%) · 예상 ${pre_el:.2f} ~ ${pre_eh:.2f}",
        f"🔥 정규장 VWAP ${reg_vwap:.2f} · 밴드 ${reg_l:.2f} ~ ${reg_h:.2f} ({reg_amp:.1f}%) · 예상 ${reg_el:.2f} ~ ${reg_eh:.2f}",
    ]
    if shield_line:
        lines.append(shield_line)

    lines.extend(
        [
            "──────────────",
            f"🏛️ <b>[2-Tier 실물 지층 &amp; 와이드 활주로]</b> (총 {held_sh}주 · {held_lots}회차)",
            *stratum_lines,
            "──────────────",
            f"🛡️ <b>[10분 고정 와이드 시드보호 매수 게이트]</b> (차기검문: {escape_html(next_gate_str)})",
            gate_cond_line,
            f"▫️ 차기 8% 정량: 예산 ${next_bud:,.2f} ➔ {quota_badge_str}",
            "──────────────",
            f"♻️ 평단 리앵커링: {reanchor_cnt}회 (누적 -${reanchor_drop:.2f} 단가인하)",
            f"🛡️ 시드보호 패스: {pass_cnt:,}회 · 🔥 13회차+ 확장매수: {extra_13}회",
            f"💰 자투리절약 누적: +${saved_u:,.2f} · 세후실현익: {n_sign}${abs(cum_net):,.2f}",
            f"💵 가용현금: <b>${free_cash:,.2f}</b> · 총자산: <b>${mark_eq:,.2f}</b> ({m_sign}{mark_roi:.2f}%)",
            f"🕒 갱신: {dt_now.strftime('%m-%d %H:%M:%S EST')}",
        ]
    )
    return "\n".join(lines)


def build_sync_instruction_message(
    st: Dict[str, Any],
    holdings: Dict[str, Dict[str, Any]],
    live_price: float,
    broker_bp_usd: float,
    fx_rate: float,
    now_est: Optional[datetime] = None,
) -> str:
    """
    📜 /sync 지층 지시서 메시지 생성
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    h_info = (holdings or {}).get(TARGET_SYMBOL, {})
    lp = safe_float(
        live_price,
        safe_float(
            h_info.get("last_price"), safe_float(st.get("anchor_price"), 0.0)
        ),
    )
    summary = summarize_stratum_portfolio(st, live_price=lp)
    eff_fx = fx_rate if fx_rate > 100.0 else KRW_PER_USD_DEFAULT

    init_seed = safe_float(summary.get("initial_seed_usd"), 10000.0)
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    actual_inv = safe_float(summary.get("actual_invested_usd"), 0.0)
    cum_net = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
    cum_roi = safe_float(summary.get("realized_roi_pct"), 0.0)
    unreal_pnl = safe_float(summary.get("unrealized_pnl_usd"), 0.0)
    mark_eq = safe_float(summary.get("mark_equity_usd"), 0.0)
    mark_roi = safe_float(summary.get("mark_roi_pct"), 0.0)

    total_net_usd = cum_net + unreal_pnl
    total_net_krw = total_net_usd * eff_fx
    cum_net_krw = cum_net * eff_fx

    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)
    actual_avg = safe_float(summary.get("actual_avg_price"), 0.0)
    book_avg = safe_float(summary.get("book_avg_price"), 0.0)
    locked_min_p = safe_float(summary.get("cycle_min_buy_price"), 0.0)

    c_sign = "+" if cum_net >= 0.0 else "-"
    u_sign = "+" if unreal_pnl >= 0.0 else "-"
    t_sign = "+" if total_net_usd >= 0.0 else "-"

    lines = [
        "📜 <b>[SOXL 지층관리 스나이퍼 지시서 (실잔고 동기화)]</b>",
        f"💵 설정 기준시드: <b>${init_seed:,.2f}</b> (환율 {eff_fx:,.1f}원/$)",
        f"🏦 스나이퍼 가용현금: <b>${free_cash:,.2f}</b> (토스 실주문가능: ${broker_bp_usd:,.2f})",
        f"📦 보유 지층 합계: <b>{held_sh}주</b> ({held_lots}회차 · 실매수원금 ${actual_inv:,.2f})",
        f"📐 전체 실평단: <b>${actual_avg:.2f}</b> · 리앵커링 장부평단: <b>${book_avg:.2f}</b> (기억저가 ${locked_min_p:.2f})",
        "──────────────",
        f"💰 누적 세후실현익(보유원금 미차감): <b>{c_sign}${abs(cum_net):,.2f}</b> ({c_sign}{abs(cum_roi):.2f}% · {c_sign}{abs(cum_net_krw):,.0f}원)",
        f"📊 현재 지층 평가손익: <b>{u_sign}${abs(unreal_pnl):,.2f}</b> (현재가 ${lp:.2f})",
        f"👑 시가평가 총자산: <b>${mark_eq:,.2f}</b> ({t_sign}{abs(mark_roi):.2f}% · 총순익 {t_sign}{abs(total_net_krw):,.0f}원)",
        f"🕒 동기화 시각: {dt_now.strftime('%Y-%m-%d %H:%M:%S EST')}",
    ]
    return "\n".join(lines)


def build_settlement_control_message(
    st: Dict[str, Any],
    live_price: float = 0.0,
    now_est: Optional[datetime] = None,
) -> str:
    """
    ⚙️ /settlement 초단순 전술 제어반 메시지 생성 (Case 64: 오직 ON/OFF 와 시드설정 2가지만 표출 + Case 67 캡핑 표시)
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    lp = safe_float(live_price, safe_float(st.get("anchor_price"), 100.0))
    summary = summarize_stratum_portfolio(st, live_price=lp)

    is_active = bool(st.get("is_active", True))
    power_str = "🟢 가동 중 (ON)" if is_active else "🔴 일시정지 (OFF)"
    init_seed = safe_float(summary.get("initial_seed_usd"), 10000.0)
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    real_eq = safe_float(summary.get("realized_equity_usd"), init_seed)
    next_bud = safe_float(
        summary.get("next_portion_budget"), init_seed * 0.08
    )
    quota_badge_str = _format_case67_quota_badge(summary)
    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)

    lines = [
        "⚙️ <b>[SOXL 인피니트 스노우볼 v3.0 전술 제어반]</b>",
        f"1️⃣ 마스터 전원: <b>{power_str}</b>",
        f"2️⃣ 기준 시드머니: <b>${init_seed:,.2f}</b> (실현기준 총자산 ${real_eq:,.2f})",
        "3️⃣ 운용 엔진: 🛡️ <b>8% 동적복리 · min(1회분,고정정량) 캡핑 · 10분 4단 와이드 · 1분 지정가매도 · 60초 타임쉴드</b> (고정)",
        f"4️⃣ 차기 1회(8%) 정량: 예산 <b>${next_bud:,.2f}</b> ➔ {quota_badge_str} (가용현금 ${free_cash:,.2f})",
        f"5️⃣ 현재 보유 지층: <b>{held_sh}주</b> ({held_lots}회차)",
        "──────────────",
        "💡 시드머니를 변경하시려면 아래 <b>[💵 기준 시드머니($) 설정]</b> 버튼을 누르거나 채팅창에 숫자(예: <code>19000</code>)를 바로 입력하세요.",
        f"🕒 기준 시각: {dt_now.strftime('%m-%d %H:%M:%S EST')}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# [핵심 2] 텔레그램 명령어 및 콜백 비동기 컨트롤러 (TelegramCommandController)
# ---------------------------------------------------------------------------
class TelegramCommandController:
    """
    🚨 제1헌법 & Case 02, 15, 16, 62, 64, 65, 66, 67:
    main.py 의 SniperAppContext 와 정식 결속되는 텔레그램 봇 컨트롤러.
    - MODIFIED: 기동 직후 _flush_stale_updates_on_startup() 에서 조건 없이 큐의 마지막 update_id 를
      즉시 소비 확정(Ack)하고, /update 실행 시 변경 사항이 없으면(changed == False) 재기동하지 않고
      무중단 가동을 유지하여 9초 주기 무한 재기동 루프를 100% 영구 봉쇄!
    """

    def __init__(self, context: Any) -> None:
        self.context: Any = context
        self.bot_token: str = (
            os.environ.get("TELEGRAM_BOT_TOKEN")
            or os.environ.get("TG_BOT_TOKEN")
            or os.environ.get("BOT_TOKEN")
            or ""
        ).strip()
        self.chat_id: str = str(
            os.environ.get("TELEGRAM_CHAT_ID")
            or os.environ.get("TG_CHAT_ID")
            or os.environ.get("CHAT_ID")
            or ""
        ).strip()

        self._boot_epoch: float = time.time()
        self._last_update_id: int = 0
        try:
            st_init = self.context.ledger.get_state(TARGET_SYMBOL)
            self._last_update_id = max(
                0, safe_int(st_init.get("last_tg_update_id"), 0)
            )
        except Exception:
            self._last_update_id = 0

        self._awaiting_seed_input_chats: Dict[str, float] = {}

    def _api_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/{method}"

    def _post_tg_sync(
        self,
        method: str,
        payload: Dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> Dict[str, Any]:
        if not self.bot_token:
            return {"ok": False, "description": "missing-bot-token"}
        url = self._api_url(method)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw) if raw else {"ok": True}
        except urllib.error.HTTPError as he:
            try:
                err_raw = he.read().decode("utf-8", errors="replace")
                return json.loads(err_raw)
            except Exception:
                return {"ok": False, "error_code": he.code}
        except Exception as exc:
            return {"ok": False, "description": str(exc)}

    async def _call_tg(
        self,
        method: str,
        payload: Dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> Dict[str, Any]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._post_tg_sync, method, payload, timeout_sec
                ),
                timeout=timeout_sec + 3.0,
            )
        except Exception as exc:
            logger.debug("텔레그램 API(%s) 호출 예외: %s", method, exc)
            return {"ok": False, "description": str(exc)}

    async def _ack_telegram_offset(self, target_update_id: int) -> None:
        """
        NEW: 텔레그램 서버 큐에서 target_update_id 이하의 모든 메시지를 즉시 소비 확정(Ack)하고
        원장(last_tg_update_id)에 영속화하여 프로세스 재기동 시 동일 명령이 재실행되지 않게 한다.
        """
        uid = max(self._last_update_id, safe_int(target_update_id, 0))
        if uid <= 0:
            return
        self._last_update_id = uid
        try:
            await self.context.ledger.update_state(
                TARGET_SYMBOL, last_tg_update_id=uid
            )
        except Exception:
            pass
        try:
            await self._call_tg(
                "getUpdates",
                {"offset": uid + 1, "limit": 1, "timeout": 0},
                timeout_sec=5.0,
            )
        except Exception:
            pass

    async def _flush_stale_updates_on_startup(self) -> None:
        """
        MODIFIED: 데몬 기동 직후 텔레그램 큐의 가장 마지막 항목(offset=-1)을 조회하여,
        메시지 종류나 발송 시각과 무관하게 부팅 전에 쌓여 있던 모든 적체 이벤트를 조건 없이
        즉시 소각(Ack)한다.
        """
        try:
            res = await self._call_tg(
                "getUpdates",
                {"offset": -1, "timeout": 0},
                timeout_sec=6.0,
            )
            if not (isinstance(res, dict) and bool(res.get("ok"))):
                return
            updates = res.get("result")
            if not isinstance(updates, list) or not updates:
                return

            max_stale_uid = 0
            for upd in updates:
                if isinstance(upd, dict):
                    uid = safe_int(upd.get("update_id"), 0)
                    if uid > max_stale_uid:
                        max_stale_uid = uid

            if max_stale_uid > 0:
                await self._ack_telegram_offset(max_stale_uid)
                logger.info(
                    "🧹 [텔레그램 부팅 전 적체 큐 전량 소각 완료] last_update_id=%d",
                    max_stale_uid,
                )
        except Exception as exc:
            logger.debug("부팅 시 텔레그램 큐 플러시 스킵: %s", exc)

    async def send_message(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML",
    ) -> bool:
        target_chat = str(chat_id or self.chat_id or "").strip()
        if not self.bot_token or not target_chat:
            return False
        payload: Dict[str, Any] = {
            "chat_id": target_chat,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        res = await self._call_tg("sendMessage", payload, timeout_sec=10.0)
        if not bool(res.get("ok")):
            payload.pop("parse_mode", None)
            res = await self._call_tg("sendMessage", payload, timeout_sec=10.0)
        return bool(res.get("ok"))

    async def edit_or_send_message(
        self,
        chat_id: str,
        message_id: Optional[int],
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if message_id is not None and message_id > 0:
            payload: Dict[str, Any] = {
                "chat_id": str(chat_id),
                "message_id": int(message_id),
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            res = await self._call_tg(
                "editMessageText", payload, timeout_sec=10.0
            )
            if bool(res.get("ok")) or "message is not modified" in str(
                res.get("description") or ""
            ):
                return True
        return await self.send_message(
            text=text, chat_id=chat_id, reply_markup=reply_markup
        )

    async def send_slim_alert(
        self,
        title: str,
        body: str = "",
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        safe_title = escape_html(title)
        safe_body = escape_html(body)
        msg = (
            f"<b>{safe_title}</b>\n🔸 {safe_body}"
            if safe_body
            else f"<b>{safe_title}</b>"
        )
        return await self.send_message(text=msg, reply_markup=reply_markup)

    async def send_startup_alive_banner(self) -> bool:
        """
        🚨 Case 16 & Case 67:
        기동 완료 후 "✅ 서버가 정상 작동 중입니다." 생존 신호를 타전한다.
        """
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        init_seed = safe_float(st.get("initial_seed_usd"), 10000.0)
        free_cash = safe_float(st.get("free_cash_usd"), init_seed)
        text = (
            "<b>✅ 서버가 정상 작동 중입니다.</b>\n"
            "🔸 전략: 🛡️ 05_AF_WIDE_10_01 (8% 동적복리 · min(1회분,고정정량) 캡핑 · 10분 4단 와이드 · 1분 현재가 지정가 매도)\n"
            "🔸 방어: ⏱️ 양방향 60초 타임쉴드 &amp; 미결 매수 선행 정산 &amp; 예산초과 차단(Case 65/66/67) 락온 완료\n"
            f"🔸 기준시드: ${init_seed:,.2f} · 가용현금: ${free_cash:,.2f}"
        )
        return await self.send_message(
            text=text, reply_markup=_build_main_reply_keyboard()
        )

    # -----------------------------------------------------------------------
    # 실시간 시세/잔고 최신화 후 각 관제 보드 렌더링
    # -----------------------------------------------------------------------
    async def _refresh_live_price_and_metrics(
        self,
    ) -> Tuple[float, Dict[str, Any]]:
        now_est = datetime.now(EST_TZ)
        live_p = safe_float(
            self.context.latest_prices.get(TARGET_SYMBOL), 0.0
        )
        try:
            p_map = await self.context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            fetched_p = safe_float(p_map.get(TARGET_SYMBOL), 0.0)
            if fetched_p > 0.0:
                live_p = fetched_p
                self.context.latest_prices[TARGET_SYMBOL] = live_p
        except Exception:
            pass

        base_met = self.context.candle_metrics_cache.get(TARGET_SYMBOL, {})
        met = refresh_metrics_with_live_tick(
            base_met, live_p, now_est=now_est
        )
        self.context.candle_metrics_cache[TARGET_SYMBOL] = met
        if live_p <= 0.0:
            live_p = safe_float(met.get("live_price"), 0.0)
        return live_p, met

    async def _render_and_send_avwap(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        live_p, met = await self._refresh_live_price_and_metrics()
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        msg = build_avwap_radar_message(
            context=self.context,
            st=st,
            metrics=met,
            live_price=live_p,
            now_est=datetime.now(EST_TZ),
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_radar_inline_keyboard(),
        )

    async def _render_and_send_sync(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        now_est = datetime.now(EST_TZ)
        live_p, _ = await self._refresh_live_price_and_metrics()
        holdings_map, fetch_ok = (
            await self.context.api.get_holdings_map_with_status()
        )
        if fetch_ok and isinstance(holdings_map, dict):
            self.context.latest_holdings = holdings_map
            self.context.holdings_fetch_ok = True
            await reconcile_stratum_with_broker_holdings_plugin(
                context=self.context,
                holdings=holdings_map,
                prices={TARGET_SYMBOL: live_p},
                now_est=now_est,
                force_sync=False,
            )
        else:
            holdings_map = self.context.latest_holdings

        bp_usd = await self.context.api.get_buying_power_usd(
            safety_buffer_pct=0.0
        )
        fx_rate = await self.context.api.get_usd_krw_rate()
        st = self.context.ledger.get_state(TARGET_SYMBOL)

        msg = build_sync_instruction_message(
            st=st,
            holdings=holdings_map,
            live_price=live_p,
            broker_bp_usd=bp_usd,
            fx_rate=fx_rate,
            now_est=now_est,
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_radar_inline_keyboard(),
        )

    async def _render_and_send_settlement(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        live_p, _ = await self._refresh_live_price_and_metrics()
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        msg = build_settlement_control_message(
            st=st, live_price=live_p, now_est=datetime.now(EST_TZ)
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_settlement_inline_keyboard(
                bool(st.get("is_active", True))
            ),
        )

    async def _apply_new_seed_value(
        self, chat_id: str, raw_num_str: str
    ) -> bool:
        cleaned = (
            str(raw_num_str)
            .replace(",", "")
            .replace("$", "")
            .replace("불", "")
            .replace("달러", "")
            .strip()
        )
        try:
            val = float(cleaned)
        except Exception:
            return False
        if val < 100.0 or val > 10000000.0:
            await self.send_message(
                chat_id=chat_id,
                text="⚠️ 시드머니는 <b>$100 ~ $10,000,000</b> 사이의 숫자로 입력해 주세요. (예: <code>19000</code>)",
            )
            return True

        updated_st = await self.context.ledger.set_initial_seed_usd(val)
        self.context.wakeup_event.set()
        live_p, _ = await self._refresh_live_price_and_metrics()
        summary = summarize_stratum_portfolio(updated_st, live_price=live_p)
        quota_badge_str = _format_case67_quota_badge(summary)
        await self.send_message(
            chat_id=chat_id,
            text=(
                f"✅ <b>[기준 시드머니 설정 완료]</b>\n"
                f"🔸 새 기준 시드머니: <b>${val:,.2f}</b>\n"
                f"🔸 가용 현금시드: <b>${safe_float(summary.get('free_cash_usd'), 0.0):,.2f}</b>\n"
                f"🔸 차기 1회(8%) 예산: <b>${safe_float(summary.get('next_portion_budget'), 0.0):,.2f}</b> "
                f"➔ {quota_badge_str}"
            ),
            reply_markup=_build_settlement_inline_keyboard(
                bool(updated_st.get("is_active", True))
            ),
        )
        return True

    async def _execute_manual_update_command(self, chat_id: str) -> None:
        """
        🚨 Case 62 & 무한 재기동 방어:
        1) /update 명령 수신 즉시 _ack_telegram_offset() 으로 텔레그램 서버에 update_id 소비 완료를 확정!
        2) execute_git_pull_and_preflight() 수행 후 신규 커밋(changed == True)이 반영되었을 때만
           데몬을 재기동(os._exit(0))하고, 이미 최신 버전(changed == False)일 때는 재기동 없이 무중단 유지한다!
        """
        await self._ack_telegram_offset(self._last_update_id)
        await self.send_message(
            chat_id=chat_id,
            text="⏳ <b>[시스템 업데이트 개시]</b> GitHub 최신 코드 동기화 및 9대 코어 모듈 프리플라이트 검증 중...",
        )
        upd_res = await execute_git_pull_and_preflight(self.context.base_dir)
        changed = False
        if isinstance(upd_res, tuple):
            ok = bool(upd_res[0]) if len(upd_res) >= 1 else False
            if len(upd_res) >= 3 and isinstance(upd_res[2], bool):
                changed = bool(upd_res[2])
            str_parts = [
                str(x).strip()
                for x in upd_res[1:]
                if isinstance(x, str) and str(x).strip()
            ]
            msg = (
                " · ".join(str_parts)
                if str_parts
                else "✅ 9대 코어 모듈 검증 통과"
            )
        else:
            ok = bool(upd_res)
            msg = "✅ 9대 코어 모듈 검증 통과"

        await self._ack_telegram_offset(self._last_update_id)

        if not ok:
            await self.send_message(
                chat_id=chat_id,
                text=f"⚠️ <b>[업데이트 중단]</b>\n{escape_html(msg)}",
            )
            return

        if not changed:
            await self.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>[최신 버전 검증 완료 · 무중단 가동 유지]</b>\n"
                    f"🔸 {escape_html(msg)}\n"
                    "🔸 원격 변경사항이 없으므로 재기동 없이 현재 스나이퍼 엔진을 계속 가동합니다."
                ),
            )
            return

        await self.send_message(
            chat_id=chat_id,
            text=f"🚀 <b>[신규 업데이트 검증 통과]</b> {escape_html(msg)}\n잠시 후 스나이퍼 데몬을 재기동합니다...",
        )
        await asyncio.sleep(0.5)
        os._exit(0)

    # -----------------------------------------------------------------------
    # 메시지 및 콜백 디스패처
    # -----------------------------------------------------------------------
    async def _handle_command_or_text(
        self, chat_id: str, raw_text: str
    ) -> None:
        text = str(raw_text or "").strip()
        if not text:
            return

        clean_num_check = (
            text.replace(",", "").replace("$", "").replace(".", "").strip()
        )
        if clean_num_check.isdigit():
            self._awaiting_seed_input_chats.pop(chat_id, None)
            await self._apply_new_seed_value(chat_id, text)
            return

        low = text.lower()

        if (
            low.startswith("/start")
            or low.startswith("/menu")
            or low.startswith("/help")
            or text == "⬅️ 메인 메뉴"
        ):
            st = self.context.ledger.get_state(TARGET_SYMBOL)
            msg = build_start_schedule_message(
                st, now_est=datetime.now(EST_TZ)
            )
            await self.send_message(
                chat_id=chat_id,
                text=msg,
                reply_markup=_build_main_reply_keyboard(),
            )
            return

        if (
            low.startswith("/avwap")
            or low.startswith("/status")
            or low.startswith("/radar")
            or text == "🔫 스나이퍼 레이더"
        ):
            await self._render_and_send_avwap(chat_id=chat_id)
            return

        if low.startswith("/sync") or text in (
            "📜 지층 지시서",
            "🔄 잔고 동기화",
        ):
            await self._render_and_send_sync(chat_id=chat_id)
            return

        if (
            low.startswith("/settlement")
            or low.startswith("/config")
            or text == "⚙️ 전술 제어반"
        ):
            await self._render_and_send_settlement(chat_id=chat_id)
            return

        if low.startswith("/seed"):
            parts = text.split(maxsplit=1)
            if len(parts) >= 2:
                await self._apply_new_seed_value(chat_id, parts[1])
            else:
                await self._render_and_send_settlement(chat_id=chat_id)
            return

        if low.startswith("/reset_seed"):
            parts = text.split(maxsplit=1)
            new_seed: Optional[float] = None
            if len(parts) >= 2:
                s_val = safe_float(parts[1], 0.0)
                if s_val >= 100.0:
                    new_seed = s_val
            await self.context.api.cancel_all_open_orders(symbol=TARGET_SYMBOL)
            st_reset = await self.context.ledger.reset_stratum_only(
                symbol=TARGET_SYMBOL,
                new_seed_usd=new_seed,
                preserve_realized_profit=(new_seed is None),
            )
            self.context.indexing_grace_cache.pop(TARGET_SYMBOL, None)
            self.context.exit_snapshot_cache.pop(TARGET_SYMBOL, None)
            self.context.wakeup_event.set()
            await self.send_message(
                chat_id=chat_id,
                text=(
                    "🧹 <b>[SOXL 지층 및 시드 원자적 초기화 완료]</b>\n"
                    f"🔸 기준 시드머니: <b>${safe_float(st_reset.get('initial_seed_usd'), 10000.0):,.2f}</b>\n"
                    f"🔸 가용 현금시드: <b>${safe_float(st_reset.get('free_cash_usd'), 10000.0):,.2f}</b>\n"
                    "🔸 보유 지층 큐 및 기억된 최저가(cycle_min_buy_price) 0점 소각 완료"
                ),
            )
            return

        if low.startswith("/reset"):
            await self.context.api.cancel_all_open_orders(symbol=TARGET_SYMBOL)
            st_reset = await self.context.ledger.reset_stratum_only(
                symbol=TARGET_SYMBOL,
                new_seed_usd=None,
                preserve_realized_profit=True,
            )
            self.context.indexing_grace_cache.pop(TARGET_SYMBOL, None)
            self.context.exit_snapshot_cache.pop(TARGET_SYMBOL, None)
            self.context.wakeup_event.set()
            await self.send_message(
                chat_id=chat_id,
                text=(
                    "🧹 <b>[SOXL 지층 원장 0점 소각 완료]</b>\n"
                    "🔸 미체결 주문 취소 및 지층 큐·기억저가 초기화 완료\n"
                    f"🔸 보존된 가용 현금시드: <b>${safe_float(st_reset.get('free_cash_usd'), 10000.0):,.2f}</b>"
                ),
            )
            return

        if low.startswith("/update") or text == "🚀 시스템 업데이트":
            await self._execute_manual_update_command(chat_id=chat_id)
            return

    async def _handle_callback_query(self, cb: Dict[str, Any]) -> None:
        cb_id = str(cb.get("id") or "").strip()
        data = str(cb.get("data") or "").strip()
        msg_obj = (
            cb.get("message") if isinstance(cb.get("message"), dict) else {}
        )
        chat_obj = (
            msg_obj.get("chat") if isinstance(msg_obj.get("chat"), dict) else {}
        )
        chat_id = str(chat_obj.get("id") or self.chat_id or "").strip()
        msg_id = safe_int(msg_obj.get("message_id"), 0)

        if cb_id:
            await self._call_tg(
                "answerCallbackQuery",
                {"callback_query_id": cb_id},
                timeout_sec=5.0,
            )

        if not chat_id:
            return

        if data == "cmd_avwap":
            await self._render_and_send_avwap(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_sync":
            await self._render_and_send_sync(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_settlement":
            await self._render_and_send_settlement(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_start":
            st = self.context.ledger.get_state(TARGET_SYMBOL)
            msg = build_start_schedule_message(
                st, now_est=datetime.now(EST_TZ)
            )
            await self.edit_or_send_message(
                chat_id=chat_id,
                message_id=msg_id,
                text=msg,
                reply_markup=_build_radar_inline_keyboard(),
            )
        elif data == "toggle_bot_active":
            updated_st = await self.context.ledger.toggle_bot_active()
            now_active = bool(updated_st.get("is_active", True))
            if now_active:
                await relock_stratum_on_bot_resume(
                    context=self.context, now_est=datetime.now(EST_TZ)
                )
            self.context.wakeup_event.set()
            await self._render_and_send_settlement(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "prompt_seed_input":
            self._awaiting_seed_input_chats[chat_id] = time.monotonic()
            await self.send_message(
                chat_id=chat_id,
                text=(
                    "💵 <b>[기준 시드머니($) 입력 안내]</b>\n"
                    "운용할 기준 달러 시드머니 금액을 숫자만 채팅창에 입력해 주세요.\n"
                    "예시: <code>10000</code> 또는 <code>19000</code>"
                ),
            )

    async def run_polling_loop(self) -> None:
        """
        텔레그램 getUpdates 비동기 롱폴링 루프 (기동 시 적체 큐 무조건부 일괄 소각 탑재)
        """
        if not self.bot_token:
            logger.warning(
                "TELEGRAM_BOT_TOKEN 이 설정되지 않아 텔레그램 폴링 대기 모드로 전환합니다."
            )
            while not self.context.shutdown_event.is_set():
                await asyncio.sleep(10.0)
            return

        await self._flush_stale_updates_on_startup()
        logger.info("📡 [텔레그램 관제탑 폴링 루프 가동 완료]")

        while not self.context.shutdown_event.is_set():
            try:
                payload: Dict[str, Any] = {
                    "offset": self._last_update_id + 1,
                    "timeout": 15,
                    "allowed_updates": ["message", "callback_query"],
                }
                res = await self._call_tg(
                    "getUpdates", payload, timeout_sec=20.0
                )
                if isinstance(res, dict) and bool(res.get("ok")):
                    updates = res.get("result")
                    if isinstance(updates, list):
                        for upd in updates:
                            if not isinstance(upd, dict):
                                continue
                            uid = safe_int(upd.get("update_id"), 0)
                            if uid > self._last_update_id:
                                self._last_update_id = uid
                                try:
                                    await self.context.ledger.update_state(
                                        TARGET_SYMBOL, last_tg_update_id=uid
                                    )
                                except Exception:
                                    pass

                            if isinstance(upd.get("callback_query"), dict):
                                await self._handle_callback_query(
                                    upd["callback_query"]
                                )
                            elif isinstance(upd.get("message"), dict):
                                m_obj = upd["message"]
                                c_obj = (
                                    m_obj.get("chat")
                                    if isinstance(m_obj.get("chat"), dict)
                                    else {}
                                )
                                cid = str(
                                    c_obj.get("id") or self.chat_id or ""
                                ).strip()
                                if self.chat_id and cid != self.chat_id:
                                    continue
                                txt = str(m_obj.get("text") or "").strip()
                                msg_date = safe_float(m_obj.get("date"), 0.0)
                                if (
                                    msg_date > 0.0
                                    and msg_date < (self._boot_epoch - 1.0)
                                    and (
                                        txt.lower().startswith("/update")
                                        or txt == "🚀 시스템 업데이트"
                                    )
                                ):
                                    await self._ack_telegram_offset(uid)
                                    continue
                                if txt:
                                    await self._handle_command_or_text(
                                        chat_id=cid, raw_text=txt
                                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("텔레그램 폴링 루프 일시 예외: %s", exc)
                await asyncio.sleep(2.0)


# 하위 호환 별칭 (타 모듈 임포트 호환 100% 보장)
TelegramBotController = TelegramCommandController
TelegramUiController = TelegramCommandController
SniperTelegramController = TelegramCommandController
