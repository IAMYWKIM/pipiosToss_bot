# -*- coding: utf-8 -*-
"""
# NEW: defense_plugin.py
방향성 인지(Direction-Aware) 매크로 3단 방어망, 체결 시차 MANUAL 오분류 가드,
스나이퍼 OFF➔ON 재가동 시 QQQ 0.00% 기준가 재락온, 08:15 EST 거시지표 선제 컷오프,
15:00 ~ 15:58 EST 정규장 VWAP 탈출(VWAP_ESCAPE) 및 15:59 ~ 16:01 EST 제로-오버나이트 MOC 플러그인 (Step 06 / 10)
- 제3헌법: 3단 매크로 방어, 상태 동기화 가드, 08:15 컷오프, 15:00 VWAP 탈출, 15:59 MOC 전담 분리
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 5 & Case 16, 24, 48, 50, 63, 66, 67, 69, 88 ~ 96, 101, 102 완벽 구현
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import (
    NQ_DYNAMIC_FLOOR,
    NQ_OVERHEAT_THRESHOLD,
    calc_qqq_adverse_status,
    is_qqq_scale_valid,
)
from quant_indicators_plugin import refresh_metrics_with_live_tick
from sniper_plugin import activate_bottom_rebound_tracker, mark_auto_fill_grace_shield
from toss_api import EST_TZ, REAL_ALLOWED_SYMBOLS, safe_float, safe_int

logger = logging.getLogger("toss_sniper.defense_plugin")

# 자동 진입 세션 태그 화이트리스트 (MANUAL 오분류 방어 기준, Case 102)
AUTO_ENTRY_SESSIONS = (
    "PRE_VWAP_M050",
    "RE_BOT_TRAIL",
    "PRE_VWAP_ESC",
    "WHIPSAW_1SH",
)


# ---------------------------------------------------------------------------
# [보조 1] 1주 휩소 정리 또는 정산 후 양쪽 심볼 재진입 해금 / 과열차단 복원기
# ---------------------------------------------------------------------------
async def unlock_all_symbols_for_reentry(
    context: Any,
    nq_amp: float,
    reason: str = "REENTRY_UNLOCK",
) -> Dict[str, Any]:
    """
    🎯 2 & Case 95, 96:
    - 당일 NQ=F 진폭이 2.0% 미만이면 양쪽 장부의 is_session_done = False 및 entry_session = '' 로 해제하여
      프리장 본진(또는 다회전 재출동)에 매수 권한을 인계한다.
    - 당일 NQ=F 진폭이 2.0% 이상(과열일)이면 즉시 양쪽 장부를 entry_session = 'CANCELED_MACRO' 및
      is_session_done = True 로 복원하여 본진 자동 진입을 영구 차단한다.
    """
    amp = safe_float(nq_amp, 0.0)
    is_overheated = bool(amp >= NQ_OVERHEAT_THRESHOLD)

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if safe_int(st.get("holdings_qty"), 0) > 0:
            continue
        await context.ledger.update_state(
            sym,
            buy_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            is_session_done=is_overheated,
            entry_session="CANCELED_MACRO" if is_overheated else "",
        )

    return {
        "status": "RESTORED_CANCELED_MACRO" if is_overheated else "UNLOCKED_FOR_MAIN",
        "nq_amp": amp,
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# [보조 2] 스나이퍼 OFF ➔ ON 재가동 시 보유 포지션 QQQ 0.00% 기준가 재락온
# ---------------------------------------------------------------------------
async def relock_qqq_baseline_on_bot_resume(
    context: Any,
    symbol: Optional[str] = None,
    holdings: Optional[Dict[str, Dict[str, Any]]] = None,
    macro: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 50, 93:
    스나이퍼가 OFF 상태에서 ON 으로 재가동될 때 현재 보유 중인 포지션이 있다면,
    과거(또는 선물 스케일로 오염된) 기준가로 인해 켜자마자 3차 역행 손절이 오발사되지 않도록
    현재 실시간 QQQ 가격을 nq_entry_price(역행률 0.00% 출발)로 즉시 재락온한다.
    """
    target_syms = (
        (str(symbol).upper().strip(),)
        if symbol and str(symbol).upper().strip() in REAL_ALLOWED_SYMBOLS
        else REAL_ALLOWED_SYMBOLS
    )
    if holdings is None:
        holdings = await context.api.get_holdings_all()

    qqq_now = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0),
        force_refresh_sec=0.5,
    )
    nq_amp_now = safe_float((macro or {}).get("nq_amp"), 0.0)

    relocked = []
    for sym in target_syms:
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)
        avg_p = safe_float((holdings or {}).get(sym, {}).get("avg_price"), 0.0)
        st = context.ledger.get_state(sym)
        if h_qty <= 0 and safe_int(st.get("holdings_qty"), 0) <= 0:
            continue

        cur_entry_sess = str(st.get("entry_session") or "").strip()
        if not cur_entry_sess or cur_entry_sess in ("CANCELED_MACRO", "DONE_TODAY", "STAGE3_STOP_TRACK"):
            cur_entry_sess = "MANUAL"

        await context.ledger.update_state(
            sym,
            holdings_qty=h_qty if h_qty > 0 else safe_int(st.get("holdings_qty"), 0),
            avg_price=avg_p if avg_p > 0.0 else safe_float(st.get("avg_price"), 0.0),
            entry_session=cur_entry_sess,
            nq_entry_price=qqq_now,
            nq_entry_amp=nq_amp_now if nq_amp_now > 0.0 else safe_float(st.get("nq_entry_amp"), 0.0),
        )
        relocked.append(sym)
        logger.info(
            "🔒 [OFF➔ON QQQ 기준가 0.00%% 재락온] %s -> QQQ=$%.2f, NQ진폭=%.2f%% (session=%s)",
            sym,
            qqq_now,
            nq_amp_now,
            cur_entry_sess,
        )

    return {"status": "RELOCKED", "symbols": relocked, "qqq_price": qqq_now}


# ---------------------------------------------------------------------------
# [핵심 1] 체결 시차(0.1 ~ 0.4초) MANUAL 오분류 방어 & 수동 진입 감지 가드 (Case 50, 93, 102)
# ---------------------------------------------------------------------------
async def sync_manual_and_indexing_guard(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
) -> Dict[str, Any]:
    """
    🎯 5 & Case 50, 93, 102:
    1) [매수 FILLED 인덱싱 시차 보호] 장부에 buy_order_id 가 존재하고 아직 holdings_qty == 0 일 때
       주문 상태가 FILLED / PARTIAL_FILLED 이면 즉시 indexing_grace_cache(15초 쉴드)를 각인하여
       0주 정산 플러그인이 장부의 entry_session('PRE_VWAP_M050' 등)을 지우지 못하게 방어한다.
    2) [잔고 포착 시 자동 vs 수동 정밀 판별] holdings_qty > 0 이 감지되었을 때:
       - 장부 entry_session 이 이미 AUTO_ENTRY_SESSIONS 에 속하거나,
       - buy_order_id 가 존재하거나,
       - indexing_grace_cache 에 최근 자동 체결 기록이 있으면
         절대로 'MANUAL' 로 덮어쓰지 않고 기존 자동 진입 세션 자격을 100% 유지한다!
       - 위 자동 진입 흔적이 전혀 없을 때만 운용자의 수동 매수('MANUAL')로 판정하여
         실시간 QQQ 기준가(0.00%)와 NQ 진폭을 락온하고 양쪽 장부의 당일 신규 자동 매수를 잠근다.
    3) [Case 93 선물 스케일 오염 치유] 만약 보유 종목의 nq_entry_price 가 NQ=F 선물 스케일(예: 20,000pt)로
       오염되어 있으면 즉시 현재 실시간 QQQ 가격으로 자가 치유한다.
    """
    grace_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "indexing_grace_cache", {}
    )
    qqq_live = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        h_avg_p = safe_float(h_info.get("avg_price"), 0.0)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        entry_sess = str(st.get("entry_session") or "").strip()

        # [A] 잔고 0주이나 buy_order_id 가 존재할 때 체결 여부 프로빙 -> 그레이스 쉴드 선각인
        if h_qty <= 0 and buy_oid:
            detail = await context.api.get_order_detail(buy_oid)
            if isinstance(detail, dict):
                ord_st = str(detail.get("status") or "").upper().strip()
                exec_obj = (
                    detail.get("execution")
                    if isinstance(detail.get("execution"), dict)
                    else {}
                )
                f_qty = safe_int(exec_obj.get("filledQuantity"), 0)
                f_avg = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
                if ord_st in ("FILLED", "PARTIAL_FILLED") or f_qty > 0:
                    mark_auto_fill_grace_shield(
                        context,
                        symbol=sym,
                        entry_session=entry_sess if entry_sess in AUTO_ENTRY_SESSIONS else "PRE_VWAP_M050",
                        fill_price=f_avg,
                    )
            continue

        if h_qty <= 0:
            continue

        # [B] 실보유 잔고(h_qty > 0)가 존재하는 경우
        grace_info = grace_cache_map.get(sym)
        grace_sess = (
            str(grace_info.get("entry_session") or "")
            if isinstance(grace_info, dict)
            else ""
        )

        # 자동 진입 자격 복원/유지 판정 (Case 102 핵심 방어)
        if entry_sess in AUTO_ENTRY_SESSIONS:
            resolved_sess = entry_sess
        elif grace_sess in AUTO_ENTRY_SESSIONS:
            resolved_sess = grace_sess
        elif buy_oid:
            resolved_sess = "PRE_VWAP_M050"
        elif entry_sess == "MANUAL":
            resolved_sess = "MANUAL"
        else:
            # 최근 CLOSED 매수 주문 중 15초 내 자동 clientOrderId(PRE/REB/WHP) 체결건이 있는지 최종 확인
            resolved_sess = "MANUAL"
            closed_orders = await context.api.get_orders(
                status="CLOSED", symbol=sym, limit=5
            )
            for o in closed_orders:
                if not isinstance(o, dict):
                    continue
                if (
                    str(o.get("side") or "").upper() == "BUY"
                    and str(o.get("status") or "").upper() == "FILLED"
                ):
                    oid_closed = str(o.get("orderId") or "").strip()
                    if oid_closed and oid_closed == buy_oid:
                        resolved_sess = "PRE_VWAP_M050"
                        break

        # Case 93: nq_entry_price 가 0.0 이거나 선물 스케일(20,000pt 등)로 오염된 경우 QQQ 현재가로 즉시 치유
        cur_ref_p = safe_float(st.get("nq_entry_price"), 0.0)
        need_qqq_fix = not is_qqq_scale_valid(cur_ref_p, qqq_live)
        eff_ref_p = qqq_live if (need_qqq_fix and qqq_live > 0.0) else cur_ref_p
        eff_amp = (
            nq_amp
            if safe_float(st.get("nq_entry_amp"), 0.0) <= 0.0
            else safe_float(st.get("nq_entry_amp"), 0.0)
        )

        is_new_manual_detect = bool(
            resolved_sess == "MANUAL" and entry_sess != "MANUAL"
        )

        if (
            is_new_manual_detect
            or entry_sess != resolved_sess
            or safe_int(st.get("holdings_qty"), 0) != h_qty
            or abs(safe_float(st.get("avg_price"), 0.0) - h_avg_p) > 0.001
            or need_qqq_fix
        ):
            other_sym = "SOXL" if sym == "SOXS" else "SOXS"
            trend_tag = "DOWN" if sym == "SOXS" else "UP"
            await context.ledger.update_state(
                sym,
                holdings_qty=h_qty,
                avg_price=h_avg_p,
                entry_session=resolved_sess,
                entry_trend=trend_tag,
                nq_entry_price=eff_ref_p,
                nq_entry_amp=eff_amp,
                is_session_done=True,
                entry_ts=safe_float(st.get("entry_ts"), 0.0) or time.time(),
            )
            await context.ledger.update_state(other_sym, is_session_done=True)

            if is_new_manual_detect and hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🖐️ [수동 진입 감지 · MANUAL 방어 결속] {sym} {h_qty}주",
                    body=(
                        f"평단 ${h_avg_p:.2f} · 2차 과열방어 패스 & "
                        f"QQQ기준 ${eff_ref_p:.2f}(0.00%) 3차 역행방어 가동"
                    ),
                )

    return {"status": "SYNCED"}


# ---------------------------------------------------------------------------
# [핵심 2] 1·2차 매크로 과열 방어선 (NQ=F >= 2.0% — MANUAL 및 WHIPSAW_1SH 100% 패스)
# ---------------------------------------------------------------------------
async def execute_stage1_2_nq_overheat_defense(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
) -> Dict[str, Any]:
    """
    🎯 5 & Case 63, 66, 95, 96:
    - NQ=F 당일 진폭(nq_amp)이 2.0% 이상일 때 작동한다.
    - [1차 방어선] 본진 미체결 자동 매수 주문(PRE_VWAP_M050, RE_BOT_TRAIL 등)이 대기 중이면
      즉시 cancel_order 로 파기하고 장부의 buy_order_id='' 를 소각하며 CANCELED_MACRO 로 잠근다.
      (단, 1주 휩소 작전 'WHIPSAW_1SH' 의 미체결 1주 매수 주문은 상시 100% 예외 허용!)
    - [2차 방어선] 본진 자동 진입으로 보유 중인 물량이 손실 구간(0 < live_p < avg_price)에서
      NQ=F 진폭 2.0%를 돌파하면 즉시 매수 1호가 LIMIT 스윕으로 전량 손절 덤핑한 뒤 CANCELED_MACRO 로 마감한다.
      (단, 수동 진입 'MANUAL' 과 1주 휩소 'WHIPSAW_1SH' 보유 물량은 NQ>=2.0% 여도 2차 방어선을 100% 패스!)
    """
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    if nq_amp < NQ_OVERHEAT_THRESHOLD:
        return {"status": "NORMAL_BELOW_200", "nq_amp": nq_amp}

    # 최저점 반등 추적기가 켜져 있다면 즉시 중단
    bottom_cache = getattr(context, "bottom_track_cache", {})
    if isinstance(bottom_cache, dict) and bottom_cache.get("active"):
        bottom_cache["active"] = False

    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        entry_sess = str(st.get("entry_session") or "").strip()
        # 🚨 Case 95 & Case 96: 1주 휩소 작전(WHIPSAW_1SH)은 1·2차 과열 방어선을 100% 패스!
        if entry_sess == "WHIPSAW_1SH":
            continue

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        buy_oid = str(st.get("buy_order_id") or "").strip()

        # [1차 방어선] 본진 미체결 자동 매수 주문 즉시 취소 및 장부 소각
        if buy_oid and h_qty <= 0:
            async with context.in_memory_ordering_lock:
                try:
                    await context.api.cancel_order(buy_oid)
                except Exception as exc:
                    logger.warning("⚠️ [%s] 1차 과열방어 미체결 매수 취소 예외: %s", sym, exc)

                other_sym = "SOXL" if sym == "SOXS" else "SOXS"
                await context.ledger.update_state(
                    sym,
                    buy_order_id="",
                    ordered_qty=0,
                    buy_price=0.0,
                    entry_session="CANCELED_MACRO",
                    is_session_done=True,
                )
                await context.ledger.update_state(
                    other_sym,
                    entry_session="CANCELED_MACRO",
                    is_session_done=True,
                )
                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🛑 [1차 과열방어 · 미체결 매수덫 파기] {sym}",
                        body=f"NQ=F 당일 진폭 {nq_amp:.2f}% >= 2.00% 도달 · 본진 자동매수 영구 차단",
                    )
            continue

        # [2차 방어선] 본진 자동 진입 보유 물량 손실권 덤핑 (수동 진입 MANUAL 은 100% 패스!)
        if h_qty > 0 and avg_p > 0.0:
            if entry_sess == "MANUAL":
                continue

            live_p = safe_float((prices or {}).get(sym), 0.0)
            if live_p <= 0.0:
                live_p = safe_float(h_info.get("last_price"), avg_p)

            # 수익권(live_p >= avg_p)이면 기존 익절 트레일링/안전캡을 유지하고, 손실권(0 < live_p < avg_p)일 때만 덤핑
            if not (0.0 < live_p < avg_p):
                continue

            sell_oid = str(st.get("sell_order_id") or "").strip()
            async with context.in_memory_ordering_lock:
                try:
                    sweep_res = await context.api.sweep_sell_position_bulletproof(
                        symbol=sym,
                        holdings_qty=h_qty,
                        sell_order_id=sell_oid,
                        fallback_price=live_p,
                    )
                    sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)
                    new_oid = str(sweep_res.get("orderId") or sell_oid).strip()

                    if sym not in snap_cache_map or not isinstance(snap_cache_map.get(sym), dict):
                        snap_cache_map[sym] = {
                            "symbol": sym,
                            "last_holdings_qty": h_qty,
                            "last_avg_price": avg_p,
                        }
                    snap_cache_map[sym]["last_sweep_price"] = sweep_p
                    snap_cache_map[sym]["exit_tag"] = "STAGE2_OVERHEAT_DUMP"

                    other_sym = "SOXL" if sym == "SOXS" else "SOXS"
                    await context.ledger.update_state(
                        sym,
                        sell_order_id=new_oid,
                        target_sell_price=sweep_p,
                        entry_session="CANCELED_MACRO",
                        is_session_done=True,
                    )
                    await context.ledger.update_state(
                        other_sym,
                        entry_session="CANCELED_MACRO",
                        is_session_done=True,
                    )
                    context.wakeup_event.set()

                    if hasattr(context, "send_slim_alert"):
                        await context.send_slim_alert(
                            title=f"🚨 [2차 과열방어 손절 스윕] {sym} {h_qty}주",
                            body=(
                                f"NQ=F 진폭 {nq_amp:.2f}% >= 2.00% 돌파 · "
                                f"손실권(${live_p:.2f} < 평단 ${avg_p:.2f}) 매수1호가 ${sweep_p:.2f} 덤핑"
                            ),
                        )
                    return {
                        "status": "STAGE2_DUMP_FIRED",
                        "symbol": sym,
                        "sweep_price": sweep_p,
                        "nq_amp": nq_amp,
                    }
                except Exception as exc:
                    logger.error("🚨 [%s] 2차 과열방어 덤핑 실패: %s", sym, exc)

        # 미보유 & 미체결 없는 상태에서도 본진 장부가 아직 열려 있으면 CANCELED_MACRO 각인
        if h_qty <= 0 and not buy_oid and entry_sess != "CANCELED_MACRO":
            await context.ledger.update_state(
                sym,
                entry_session="CANCELED_MACRO",
                is_session_done=True,
            )

    return {"status": "OVERHEAT_GUARD_ENFORCED", "nq_amp": nq_amp}


# ---------------------------------------------------------------------------
# [핵심 3] 3차 방향성 인지 실시간 QQQ 최소 0.80% 역행 방어선 & RE_BOT_TRAIL 연동
# ---------------------------------------------------------------------------
async def execute_stage3_qqq_adverse_defense(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 67, 69, 88, 89, 93, 94:
    - 보유 포지션 자체 평가손실률이 -0.80%, -2.0% 이상이더라도 즉시 손절 절대 금지!
    - 오직 손실 구간(0 < live_p < avg_p) 전제 하에, 진입(또는 수동 ON 락온) 시점 실시간 QQQ 기준가
      (nq_entry_price) 대비 현재 실시간 QQQ 현재가(qqq_curr)가
      dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%) 이상 반대 방향으로 역행했을 때만
      매수 1호가 LIMIT 스윕으로 손절 덤핑한다.
    - 프리장 골든타임(04:07 ~ 08:14 EST) 중 NQ=F < 2.0% 상태에서 자동 진입 물량이 3차 손절된 경우,
      당일 퇴근 없이 activate_bottom_rebound_tracker 를 가동하여 최저점 +0.30% 양봉 반등 재진입(RE_BOT_TRAIL)을
      준비하고 양쪽 장부의 is_session_done = False 로 복원한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    nq_amp_curr = safe_float((macro or {}).get("nq_amp"), 0.0)
    qqq_curr = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    if qqq_curr <= 0.0:
        return {"status": "NO_QQQ_PRICE"}

    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        qqq_entry = safe_float(st.get("nq_entry_price"), 0.0)
        nq_entry_amp = safe_float(st.get("nq_entry_amp"), 0.0)
        if nq_entry_amp <= 0.0 and qqq_entry <= 0.0:
            # 구버전 0.0 장부 승계 시 즉시 현재 QQQ 로 락온 후 이번 틱 패스
            await context.ledger.update_state(
                sym, nq_entry_price=qqq_curr, nq_entry_amp=nq_amp_curr
            )
            continue

        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p <= 0.0:
            live_p = safe_float(h_info.get("last_price"), avg_p)

        adv = calc_qqq_adverse_status(
            symbol=sym,
            qqq_entry_price=qqq_entry,
            qqq_curr_price=qqq_curr,
            nq_entry_amp=nq_entry_amp,
            position_avg_price=avg_p,
            position_live_price=live_p,
        )

        # Case 93: 만약 스케일이 유효하지 않으면 현재 QQQ 가격으로 즉시 치유 후 스킵
        if not adv.get("scale_valid"):
            await context.ledger.update_state(sym, nq_entry_price=qqq_curr)
            continue

        if not adv.get("should_stop"):
            continue

        sell_oid = str(st.get("sell_order_id") or "").strip()
        entry_sess = str(st.get("entry_session") or "PRE_VWAP_M050")

        async with context.in_memory_ordering_lock:
            try:
                sweep_res = await context.api.sweep_sell_position_bulletproof(
                    symbol=sym,
                    holdings_qty=h_qty,
                    sell_order_id=sell_oid,
                    fallback_price=live_p,
                )
                sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)
                new_oid = str(sweep_res.get("orderId") or sell_oid).strip()

                if sym not in snap_cache_map or not isinstance(snap_cache_map.get(sym), dict):
                    snap_cache_map[sym] = {
                        "symbol": sym,
                        "last_holdings_qty": h_qty,
                        "last_avg_price": avg_p,
                    }
                snap_cache_map[sym]["last_sweep_price"] = sweep_p
                snap_cache_map[sym]["exit_tag"] = "STAGE3_QQQ_STOP"

                # 🎯 3 [Step 3]: 04:07 ~ 08:14 EST 프리장 골든타임 & NQ < 2.0% & 자동 진입이면
                # 최저점 역트레일링 +0.30% 반등 재진입(RE_BOT_TRAIL) 추적기 가동!
                can_track_bottom = bool(
                    407 <= hm <= 814
                    and nq_amp_curr < NQ_OVERHEAT_THRESHOLD
                    and entry_sess in ("PRE_VWAP_M050", "RE_BOT_TRAIL")
                )
                if can_track_bottom:
                    activate_bottom_rebound_tracker(
                        context=context,
                        symbol=sym,
                        stop_price=sweep_p,
                        nq_amp=nq_amp_curr,
                        now_est=dt_now,
                    )

                other_sym = "SOXL" if sym == "SOXS" else "SOXS"
                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    target_sell_price=sweep_p,
                    entry_session="STAGE3_STOP_TRACK" if can_track_bottom else entry_sess,
                    is_session_done=not can_track_bottom,
                )
                if can_track_bottom:
                    await context.ledger.update_state(other_sym, is_session_done=False)

                context.wakeup_event.set()

                rebound_msg = " · 🪤최저점 +0.30% 반등 감시 개시" if can_track_bottom else ""
                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🛡️ [3차 QQQ 역행 방어 손절 스윕] {sym} {h_qty}주",
                        body=(
                            f"QQQ 역행 +{adv['adverse_move_pct']:.2f}% >= 임계치 {adv['dynamic_threshold_pct']:.2f}% "
                            f"(기준 ${qqq_entry:.2f}➔현가 ${qqq_curr:.2f}) · 스윕가 ${sweep_p:.2f}{rebound_msg}"
                        ),
                    )

                return {
                    "status": "STAGE3_STOP_FIRED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "adverse_move_pct": adv["adverse_move_pct"],
                    "threshold_pct": adv["dynamic_threshold_pct"],
                    "bottom_track_activated": can_track_bottom,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 3차 QQQ 역행 손절 스윕 실패: %s", sym, exc)

    return {"status": "STAGE3_OK"}


# ---------------------------------------------------------------------------
# [핵심 4] 08:15 EST 거시지표 발표 15분 전 선제 컷오프 (enforce_0815_macro_cutoff_plugin)
# ---------------------------------------------------------------------------
async def enforce_0815_macro_cutoff_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 48, 53:
    08:15:00 ~ 15:58:59 EST 구간 진입 시:
    - 프리장 미체결 매수 주문(buy_order_id)이 남아있으면 즉시 선취소하고 장부에서 소각한다.
    - 3차 손절 후 최저점 반등 추적기(_BOTTOM_TRACK_CACHE["active"])를 종료하고,
      당일 신규 자동 매수 권한을 전면 마감(is_session_done = True)한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (815 <= hm <= 1558):
        return {"status": "OUTSIDE_WINDOW"}

    bottom_cache = getattr(context, "bottom_track_cache", {})
    if isinstance(bottom_cache, dict) and bottom_cache.get("active"):
        bottom_cache["active"] = False

    canceled_syms = []
    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        buy_oid = str(st.get("buy_order_id") or "").strip()
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)

        if buy_oid and h_qty <= 0:
            async with context.in_memory_ordering_lock:
                try:
                    await context.api.cancel_order(buy_oid)
                except Exception as exc:
                    logger.warning("⚠️ [%s] 08:15 선제 컷오프 매수 취소 예외: %s", sym, exc)

                await context.ledger.update_state(
                    sym,
                    buy_order_id="",
                    ordered_qty=0,
                    buy_price=0.0,
                    is_session_done=True,
                    entry_session="CUTOFF_0815",
                )
                canceled_syms.append(sym)
        elif not bool(st.get("is_session_done", False)):
            await context.ledger.update_state(sym, is_session_done=True)

    if canceled_syms and hasattr(context, "send_slim_alert"):
        await context.send_slim_alert(
            title=f"⏰ [08:15 EST 거시지표 선제 컷오프] {', '.join(canceled_syms)}",
            body="거시지표 발표 15분 전 미체결 매수덫 선취소 완료 · 당일 신규 자동매수 마감",
        )

    return {"status": "CUTOFF_0815_ENFORCED", "canceled": canceled_syms}


# ---------------------------------------------------------------------------
# [핵심 5] 15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 정규장 VWAP 탈출 (🛟 VWAP_ESCAPE)
# ---------------------------------------------------------------------------
async def execute_1500_regular_vwap_escape(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 101:
    15:00:00 ~ 15:58:59 EST (정규장 마감 1시간 전) 동안:
    - 아직 +1.00% 하드보존 트레일링(armed)이 점화되지 않은 미익절 보유 물량(수동 포함)이 존재할 때,
    - 해당 종목의 실시간 현재가(live_p)가 당일 정규장(09:30 EST ~) 누적 VWAP(reg_vwap) 이상에 도달하면
      즉시 매수 1호가 LIMIT 스윕으로 전량 탈출(VWAP_ESCAPE)하여 15:59 MOC 투매 손실을 방어한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (1500 <= hm <= 1558):
        return {"status": "OUTSIDE_WINDOW"}

    trail_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "trail_runtime_cache", {}
    )
    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        # 이미 +1.00% 트레일링이 점화된 상태라면 HARD_TRAIL_100 이 전담
        t_state = trail_cache_map.get(sym, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            continue

        raw_met_cache = getattr(context, "candle_metrics_cache", {})
        base_met = raw_met_cache.get(sym) if isinstance(raw_met_cache, dict) else None
        live_p = safe_float((prices or {}).get(sym), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=dt_now)

        reg_vwap = safe_float(met.get("reg_vwap"), 0.0)
        live_p = safe_float(met.get("live_price"), 0.0)
        if reg_vwap <= 0.0 or live_p <= 0.0:
            continue

        vwap_target_p = math.floor(reg_vwap * 100.0) / 100.0
        if live_p < vwap_target_p:
            continue

        sell_oid = str(st.get("sell_order_id") or "").strip()
        async with context.in_memory_ordering_lock:
            try:
                sweep_res = await context.api.sweep_sell_position_bulletproof(
                    symbol=sym,
                    holdings_qty=h_qty,
                    sell_order_id=sell_oid,
                    fallback_price=live_p,
                )
                sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)
                new_oid = str(sweep_res.get("orderId") or sell_oid).strip()

                if sym not in snap_cache_map or not isinstance(snap_cache_map.get(sym), dict):
                    snap_cache_map[sym] = {
                        "symbol": sym,
                        "last_holdings_qty": h_qty,
                        "last_avg_price": avg_p,
                    }
                snap_cache_map[sym]["last_sweep_price"] = sweep_p
                snap_cache_map[sym]["exit_tag"] = "VWAP_ESCAPE"

                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    entry_session="VWAP_ESCAPE",
                    target_sell_price=sweep_p,
                    is_session_done=True,
                )
                context.wakeup_event.set()

                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🛟 [15:00 정규장 VWAP 도달 탈출] {sym} {h_qty}주",
                        body=(
                            f"현가 ${live_p:.2f} >= 정규장VWAP ${reg_vwap:.2f} "
                            f"(평단 ${avg_p:.2f}) · 스윕가 ${sweep_p:.2f} 탈출"
                        ),
                    )
                return {
                    "status": "VWAP_ESCAPE_FIRED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "reg_vwap": reg_vwap,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 15:00 정규장 VWAP 탈출 스윕 실패: %s", sym, exc)

    return {"status": "NO_REG_VWAP_ESCAPE"}


# ---------------------------------------------------------------------------
# [핵심 6] 15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 강제 청산망 (Case 16, 24)
# ---------------------------------------------------------------------------
async def execute_1559_moc_force_liquidation(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 16, 24:
    15:59:00 ~ 16:01:59 EST (MOC 컷오프 3분 윈도우):
    - 스나이퍼가 가동(is_active == True) 중인 종목에 대해 오버나이트를 100% 영구 차단한다.
    - 미체결 매수 주문이 남아있으면 최우선 선취소하고,
      보유 물량(holdings_qty > 0)이 존재하면 1.5초 주기로 2단계 방탄 매도 스윕
      (modify_order 매수 1호가 정정 -> 실패 시 선취소 후 신규 LIMIT 발사)을 격발하여 전량 청산한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (1559 <= hm <= 1601):
        return {"status": "OUTSIDE_MOC_WINDOW"}

    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        # 수동 오버나이트 통제: 사용자가 봇을 OFF 한 경우 MOC 강제 청산 패스
        if not bool(st.get("is_active", True)):
            continue

        # 1) 잔존 미체결 매수 주문 최우선 취소
        buy_oid = str(st.get("buy_order_id") or "").strip()
        if buy_oid:
            try:
                await context.api.cancel_order(buy_oid)
            except Exception:
                pass
            await context.ledger.update_state(
                sym, buy_order_id="", ordered_qty=0, buy_price=0.0, is_session_done=True
            )

        # 2) 보유 물량 2단계 방탄 매도 스윕
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0:
            continue

        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p <= 0.0:
            live_p = safe_float(h_info.get("last_price"), avg_p)

        sell_oid = str(st.get("sell_order_id") or "").strip()
        async with context.in_memory_ordering_lock:
            try:
                sweep_res = await context.api.sweep_sell_position_bulletproof(
                    symbol=sym,
                    holdings_qty=h_qty,
                    sell_order_id=sell_oid,
                    fallback_price=live_p,
                )
                sweep_p = safe_float(sweep_res.get("sweep_price"), live_p)
                new_oid = str(sweep_res.get("orderId") or sell_oid).strip()

                if sym not in snap_cache_map or not isinstance(snap_cache_map.get(sym), dict):
                    snap_cache_map[sym] = {
                        "symbol": sym,
                        "last_holdings_qty": h_qty,
                        "last_avg_price": avg_p,
                    }
                snap_cache_map[sym]["last_sweep_price"] = sweep_p
                snap_cache_map[sym]["exit_tag"] = "MOC_FORCE_EXIT"

                await context.ledger.update_state(
                    sym,
                    sell_order_id=new_oid,
                    target_sell_price=sweep_p,
                    is_session_done=True,
                )
                context.wakeup_event.set()

                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🧹 [15:59 MOC 제로-오버나이트 강제청산] {sym} {h_qty}주",
                        body=f"매수1호가 ${sweep_p:.2f} 방탄 스윕 발사 (평단 ${avg_p:.2f})",
                    )
                return {
                    "status": "MOC_SWEEP_FIRED",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "qty": h_qty,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 15:59 MOC 강제 청산 스윕 실패: %s", sym, exc)

    return {"status": "MOC_CHECKED"}
