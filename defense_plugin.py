# -*- coding: utf-8 -*-
"""
# MODIFIED: defense_plugin.py
방향성 인지(Direction-Aware) 매크로 3단 방어망 (1·2차 NQ=F 진폭 과열방어 & 3차 토스 QQQ 0.0초 실시간 최소 0.80% 역행방어),
수동 진입(MANUAL) 자동 감지 및 체결 시차 오분류 차단(Case 102), 스나이퍼 OFF->ON 재가동 시 QQQ 0.00% 기준가 재락온,
08:15 EST 거시지표 선제 컷오프, 15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 정규장 VWAP 도달 즉시 탈출(VWAP_ESCAPE),
15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 강제 청산 전술 플러그인 (Step 02 / 05)
- MODIFIED: 제3헌법 및 최신 프롬프트.txt에 의거하여 3차 역행 손절 시 폐기된 activate_bottom_rebound_tracker(RE_BOT_TRAIL)
  임포트 및 호출을 100% 영구 소각하고, 3차 손절 즉시 당일 본진 세션 안전 마감(CANCELED_STAGE3)으로 직결
- 제3헌법: 방어/컷오프/정규장 탈출/MOC 상태 머신 전담 분리
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 🎯 5 & Case 16, 24, 48, 49, 66, 92, 95, 96, 101, 102 완벽 준수
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import (
    NQ_OVERHEAT_THRESHOLD,
    calc_qqq_adverse_status,
    is_qqq_scale_valid,
)
from quant_indicators_plugin import refresh_metrics_with_live_tick
from sniper_exit_plugin import (
    ensure_safety_cap_sell_order,
    execute_bulletproof_sell_sweep,
    reconcile_zero_holdings_exit_plugin,
    update_exit_snapshot_on_hold,
)
from toss_api import EST_TZ, REAL_ALLOWED_SYMBOLS, safe_float, safe_int

logger = logging.getLogger("toss_sniper.defense_plugin")

# 매수 체결 직후 잔고 인덱싱 시차(0.1 ~ 0.4초) 동안 MANUAL 오분류를 막는 그레이스 쉴드 유지 시간(15.0초)
AUTO_FILL_GRACE_SHIELD_SEC: float = 15.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 자동 체결 그레이스 쉴드 점검 및 실시간 보정 지표 추출
# ---------------------------------------------------------------------------
def is_within_auto_fill_grace_shield(context: Any, symbol: str) -> bool:
    """
    🎯 5 & Case 102:
    최근 15.0초 이내에 프리장 본진(PRE_VWAP_M050) 또는 1주 휩소(WHIPSAW_1SH)
    매수 주문이 체결된 이력이 있으면 True 를 반환하여,
    잔고 인덱싱 시차 중 자동 진입 물량이 수동(MANUAL)으로 오분류되는 경합을 원천 차단한다.
    """
    sym = str(symbol or "").upper().strip()
    guard_map = getattr(context, "indexing_grace_cache", None)
    if not isinstance(guard_map, dict):
        return False
    info = guard_map.get(sym)
    if not isinstance(info, dict):
        return False
    last_ts = safe_float(info.get("last_auto_fill_ts"), 0.0)
    if last_ts <= 0.0:
        return False
    return (time.monotonic() - last_ts) <= AUTO_FILL_GRACE_SHIELD_SEC


def _get_live_symbol_metrics(
    context: Any,
    symbol: str,
    prices: Dict[str, float],
    now_est: datetime,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
    live_p = safe_float((prices or {}).get(sym), 0.0)
    if live_p <= 0.0 and isinstance(base_met, dict):
        live_p = safe_float(base_met.get("live_price"), 0.0)
    return refresh_metrics_with_live_tick(base_met, live_p, now_est=now_est)


# ---------------------------------------------------------------------------
# [핵심 1] 스나이퍼 OFF ➔ ON 재가동 시 보유 포지션 실시간 QQQ 0.00% 기준가 재락온
# ---------------------------------------------------------------------------
async def relock_qqq_baseline_on_bot_resume(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 92:
    운용자가 전술 제어반에서 스나이퍼를 OFF 했다가 ON 으로 재가동할 때 호출되어,
    현재 보유 중인 포지션(수동 진입 포함)의 3차 방어 기준가(nq_entry_price)를
    재가동 시점의 실시간 QQQ 현재가(0.00% 역행 출발점)로 즉시 재락온한다.
    """
    holdings = await context.api.get_holdings_all()
    prices = await context.api.get_current_prices_batch(("SOXS", "SOXL", "QQQ"))
    qqq_live = await context.macro_cache.ensure_qqq_live_price(
        safe_float(prices.get("QQQ"), 0.0)
    )
    macro = await context.macro_cache.get_macro_snapshot(
        live_qqq_price=qqq_live
    )
    nq_amp = safe_float(macro.get("nq_amp"), 0.0)
    ref_price = (
        qqq_live if qqq_live > 0.0 else safe_float(macro.get("nq_price"), 0.0)
    )

    relocked_syms = []
    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        entry_sess = str(st.get("entry_session") or "").strip()
        if not entry_sess or entry_sess in (
            "CANCELED_MACRO",
            "CANCELED_STAGE3",
            "CANCELED_EXTERNAL",
        ):
            entry_sess = "MANUAL"

        await context.ledger.update_state(
            sym,
            entry_session=entry_sess,
            nq_entry_price=ref_price,
            nq_entry_amp=nq_amp,
            entry_ts=(
                safe_float(st.get("entry_ts"), 0.0) or time.time()
            ),
        )
        relocked_syms.append(sym)
        logger.info(
            "🔒 [OFF->ON 재가동 QQQ 0.00%% 기준가 재락온] %s (%d주, 세션=%s) -> QQQ기준=$%.2f, NQ진폭=%.2f%%",
            sym,
            h_qty,
            entry_sess,
            ref_price,
            nq_amp,
        )

    return {
        "status": "RELOCKED",
        "symbols": relocked_syms,
        "qqq_baseline": ref_price,
        "nq_amp": nq_amp,
    }


# ---------------------------------------------------------------------------
# [핵심 2] 수동 진입(MANUAL) 포지션 자동 감지 · 안전 캡 선장전 · 체결 시차 오분류 방어
# ---------------------------------------------------------------------------
async def sync_manual_holdings_defense_lock(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 4 & 🎯 5 & Case 49, 92, 102:
    - 토스 계좌에 실보유 잔고(holdings_qty > 0)가 감지되었을 때:
    1) 만약 장부에 자동 진입 세션(PRE_VWAP_M050, WHIPSAW_1SH)이 기록되어 있거나
       15.0초 자동 체결 그레이스 쉴드(is_within_auto_fill_grace_shield) 구간 내라면,
       절대로 MANUAL 로 덮어쓰지 않고 기존 자동 진입 세션을 100% 보존한다(Case 102).
    2) 자동 주문 없이 운용자가 직접 수동 매수한 물량이면 즉시 entry_session = "MANUAL" 로 각인하고,
       현재 실시간 QQQ 가격을 nq_entry_price(0.00% 출발점)로 락온하며,
       상단 안전 캡(+5.00% LIMIT) 매도 주문을 선장전한다(수동 물량은 2차 과열 방어 패스, 3차 QQQ 역행 방어 가동).
    3) 기존 장부의 nq_entry_price 가 선물 스케일(>= 2000)로 오염되어 있으면 즉시 실시간 QQQ 가격으로 자가 치유한다.
    """
    qqq_curr = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    ref_price = (
        qqq_curr
        if qqq_curr > 0.0
        else safe_float((macro or {}).get("nq_price"), 0.0)
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        live_p = safe_float(
            (prices or {}).get(sym), safe_float(h_info.get("last_price"), avg_p)
        )
        update_exit_snapshot_on_hold(context, sym, h_qty, avg_p, live_p)

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        entry_sess = str(st.get("entry_session") or "").strip()
        buy_oid = str(st.get("buy_order_id") or "").strip()

        # Case 102: 자동 체결 그레이스 쉴드 기간 내에는 자동 진입 세션 강제 복원/보존
        if is_within_auto_fill_grace_shield(context, sym):
            grace_info = getattr(context, "indexing_grace_cache", {}).get(
                sym, {}
            )
            auto_sess = str(
                grace_info.get("entry_session")
                or entry_sess
                or "PRE_VWAP_M050"
            )
            if auto_sess == "MANUAL":
                auto_sess = "PRE_VWAP_M050"
            if entry_sess != auto_sess:
                await context.ledger.update_state(sym, entry_session=auto_sess)
                entry_sess = auto_sess

        # 수동 진입(MANUAL) 최초 감지 및 락온
        elif not entry_sess or entry_sess in (
            "CANCELED_MACRO",
            "CANCELED_STAGE3",
            "CANCELED_EXTERNAL",
        ):
            if not buy_oid:
                entry_sess = "MANUAL"
                other_sym = "SOXL" if sym == "SOXS" else "SOXS"
                await context.ledger.update_state(
                    sym,
                    entry_session="MANUAL",
                    buy_price=avg_p,
                    ordered_qty=h_qty,
                    nq_entry_price=ref_price,
                    nq_entry_amp=nq_amp,
                    is_session_done=True,
                    entry_ts=time.time(),
                )
                await context.ledger.update_state(
                    other_sym, is_session_done=True
                )
                if hasattr(context, "send_slim_alert"):
                    arm_p = math.ceil((avg_p * 1.0100) * 100.0) / 100.0
                    await context.send_slim_alert(
                        title=f"🎯 [수동 스나이퍼 자동 결속] {sym} {h_qty}주",
                        body=(
                            f"평단 ${avg_p:.2f} · 점화가 ${arm_p:.2f}(+1.00%) · "
                            f"2차 과열방어 패스 & 3차 QQQ기준 ${ref_price:.2f} 락온"
                        ),
                    )

        # 3차 방어 기준가 스케일 오염(0.0 이하 또는 NQ=F 선물 스케일 >= 2000) 자가 치유
        cur_nq_ref = safe_float(st.get("nq_entry_price"), 0.0)
        if qqq_curr > 0.0 and not is_qqq_scale_valid(cur_nq_ref):
            await context.ledger.update_state(
                sym,
                nq_entry_price=qqq_curr,
                nq_entry_amp=(
                    safe_float(st.get("nq_entry_amp"), 0.0) or nq_amp
                ),
            )

        # 상단 안전 캡(+5.00% LIMIT, 1주 휩소는 +1.00% LIMIT) 매도 주문이 없으면 즉시 선장전
        if not str(st.get("sell_order_id") or "").strip():
            await ensure_safety_cap_sell_order(context, sym, h_qty, avg_p)

    return {"status": "SYNCED"}


# ---------------------------------------------------------------------------
# [핵심 3] 방향성 인지(Direction-Aware) 매크로 3단 방어망
# ---------------------------------------------------------------------------
async def execute_macro_3stage_defense_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 66, 92, 95, 96:
    - [1차 방어선] : NQ=F 당일 진폭 >= 2.0% 일 때 본진 자동 신규 매수 차단 및
      본진 미체결 매수 주문(PRE_VWAP_M050) 즉시 취소·장부 소각(CANCELED_MACRO).
      (단, 1주 휩소 노출 작전 WHIPSAW_1SH 은 1·2차 과열 방어를 100% 패스!)
    - [2차 방어선] : 본진 자동 진입 보유 물량(PRE_VWAP_M050)이 손실 구간(0 < live_p < avg_price)에서
      NQ=F 당일 진폭 >= 2.0% 돌파 시 즉시 매수 1호가(Bid 1) LIMIT 스윕 손절 덤핑 후 당일 마감.
      (단, 수동 진입 MANUAL 및 1주 휩소 WHIPSAW_1SH 은 2차 방어선을 100% 패스!)
    - [3차 방어선] : 포지션 자체 손실률 기반 즉시 손절 절대 금지!
      오직 손실 구간(0 < live_p < avg_price) 전제 하에 진입/수동락온 시점 QQQ 기준가(nq_entry_price) 대비
      실시간 QQQ 가격(qqq_curr)이 dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%) 이상
      반대 방향으로 역행했을 때만 매수 1호가 LIMIT 스윕으로 손절 덤핑하고
      즉시 당일 본진 세션을 안전 마감(CANCELED_STAGE3)한다 (폐기된 RE_BOT_TRAIL 호출 영구 제거 완료).
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)

    # 스동 진입 물량 결속 및 QQQ 기준가 스케일 점검 선행
    await sync_manual_holdings_defense_lock(
        context, holdings, prices, macro, dt_now
    )

    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    qqq_curr = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    is_overheated = bool(nq_amp >= NQ_OVERHEAT_THRESHOLD)

    # -----------------------------------------------------------------------
    # [1차 방어선] NQ=F 진폭 >= 2.0% 과열 시 본진 미체결 매수 주문 즉시 파기
    # -----------------------------------------------------------------------
    if is_overheated:
        for sym in REAL_ALLOWED_SYMBOLS:
            st = context.ledger.get_state(sym)
            if not bool(st.get("is_active", True)):
                continue
            entry_sess = str(st.get("entry_session") or "")
            # Case 95 & 96: 1주 휩소 작전(WHIPSAW_1SH)은 과열차단 독립 가동!
            if entry_sess == "WHIPSAW_1SH":
                continue

            buy_oid = str(st.get("buy_order_id") or "").strip()
            h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)

            if buy_oid and h_qty == 0:
                try:
                    await context.api.cancel_order(buy_oid)
                except Exception as exc:
                    logger.warning(
                        "⚠️ [%s] 1차 과열방어 미체결 매수 취소 예외: %s",
                        sym,
                        exc,
                    )

                for s in REAL_ALLOWED_SYMBOLS:
                    await context.ledger.update_state(
                        s,
                        buy_order_id="" if s == sym else context.ledger.get_state(s).get("buy_order_id", ""),
                        ordered_qty=0 if s == sym else context.ledger.get_state(s).get("ordered_qty", 0),
                        buy_price=0.0 if s == sym else context.ledger.get_state(s).get("buy_price", 0.0),
                        entry_session="CANCELED_MACRO",
                        is_session_done=True,
                    )
                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"🛑 [1차 과열방어 발동 · 미체결 매수 파기] {sym}",
                        body=f"NQ=F 당일 진폭 {nq_amp:.2f}% >= 2.00% 도달 · 본진 자동매수 영구 차단",
                    )
                return {"status": "STAGE1_CANCELED_BUY", "symbol": sym, "nq_amp": nq_amp}

    # -----------------------------------------------------------------------
    # [2차 & 3차 방어선] 보유 포지션(holdings_qty > 0) 정밀 방어 감시
    # -----------------------------------------------------------------------
    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        # 이미 트레일링 익절이 점화(armed)된 수익권 포지션은 sniper_exit_plugin 이 전담
        t_state = getattr(context, "trail_runtime_cache", {}).get(sym, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            continue

        live_p = safe_float(
            (prices or {}).get(sym), safe_float(h_info.get("last_price"), avg_p)
        )
        if live_p <= 0.0 or live_p >= avg_p:
            # 수익 또는 본전 구간(live_p >= avg_p)에서는 2·3차 손절을 절대 발동하지 않는다
            continue

        entry_sess = str(st.get("entry_session") or "MANUAL")
        if is_within_auto_fill_grace_shield(context, sym) and entry_sess == "MANUAL":
            entry_sess = "PRE_VWAP_M050"

        # -------------------------------------------------------------------
        # [2차 방어선] 본진 자동 진입 물량 손실 구간 + NQ=F >= 2.0% 과열 즉시 덤핑
        # (단, 수동 진입 MANUAL 및 1주 휩소 WHIPSAW_1SH 는 100% 패스!)
        # -------------------------------------------------------------------
        if is_overheated and entry_sess not in ("MANUAL", "WHIPSAW_1SH"):
            bid1_p = await context.api.get_best_bid_price(
                sym, fallback_price=live_p
            )
            sweep_p = math.floor(min(live_p, bid1_p) * 100.0) / 100.0
            await execute_bulletproof_sell_sweep(
                context=context,
                symbol=sym,
                holdings_qty=h_qty,
                sweep_price=sweep_p,
                reason_tag="STAGE2_OVERHEAT_DUMP",
            )
            for s in REAL_ALLOWED_SYMBOLS:
                await context.ledger.update_state(
                    s,
                    entry_session="CANCELED_MACRO",
                    is_session_done=True,
                )
            await reconcile_zero_holdings_exit_plugin(
                context=context,
                holdings={sym: {"qty": 0, "avg_price": 0.0}},
                macro=macro,
                now_est=dt_now,
                override_exit_price=sweep_p,
                override_exit_reason="STAGE2_OVERHEAT_DUMP",
            )
            return {
                "status": "STAGE2_OVERHEAT_DUMPED",
                "symbol": sym,
                "sweep_price": sweep_p,
                "nq_amp": nq_amp,
            }

        # -------------------------------------------------------------------
        # [3차 방어선] 실시간 QQQ 최소 0.80% 방향성 인지 역행 하드 스톱
        # (포지션 자체 손실률 기반 즉시 손절 절대 금지!)
        # -------------------------------------------------------------------
        if qqq_curr <= 0.0:
            continue

        qqq_entry_p = safe_float(st.get("nq_entry_price"), 0.0)
        nq_entry_amp = safe_float(st.get("nq_entry_amp"), nq_amp)
        adv = calc_qqq_adverse_status(
            symbol=sym,
            qqq_entry_price=qqq_entry_p,
            qqq_curr_price=qqq_curr,
            nq_entry_amp=nq_entry_amp,
            position_avg_price=avg_p,
            position_live_price=live_p,
        )

        # 만약 장부의 qqq_entry_price 가 선물 스케일로 오염되어 있었다면 현재 QQQ 로 즉시 교정 후 패스
        if not adv.get("scale_valid"):
            await context.ledger.update_state(sym, nq_entry_price=qqq_curr)
            continue

        if adv.get("should_stop"):
            bid1_p = await context.api.get_best_bid_price(
                sym, fallback_price=live_p
            )
            sweep_p = math.floor(min(live_p, bid1_p) * 100.0) / 100.0
            await execute_bulletproof_sell_sweep(
                context=context,
                symbol=sym,
                holdings_qty=h_qty,
                sweep_price=sweep_p,
                reason_tag="STAGE3_QQQ_STOP",
            )

            # MODIFIED: 폐기된 RE_BOT_TRAIL 호출을 제거하고 당일 본진 세션 안전 마감(CANCELED_STAGE3) 직결
            await reconcile_zero_holdings_exit_plugin(
                context=context,
                holdings={sym: {"qty": 0, "avg_price": 0.0}},
                macro=macro,
                now_est=dt_now,
                override_exit_price=sweep_p,
                override_exit_reason=(
                    f"STAGE3_QQQ_STOP(QQQ역행 +{adv['adverse_move_pct']:.2f}% >= +{adv['dynamic_threshold_pct']:.2f}%)"
                ),
            )

            if entry_sess != "WHIPSAW_1SH":
                for s in REAL_ALLOWED_SYMBOLS:
                    await context.ledger.update_state(
                        s,
                        entry_session="CANCELED_STAGE3",
                        is_session_done=True,
                    )

            return {
                "status": "STAGE3_QQQ_STOPPED",
                "symbol": sym,
                "sweep_price": sweep_p,
                "adverse_move_pct": adv["adverse_move_pct"],
                "threshold_pct": adv["dynamic_threshold_pct"],
            }

    return {"status": "DEFENSE_OK"}


# ---------------------------------------------------------------------------
# [핵심 4] 08:15 EST 거시지표 발표 15분 전 선제 컷오프 (Case 48)
# ---------------------------------------------------------------------------
async def enforce_0815_macro_cutoff_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 48:
    08:15:00 ~ 15:58:59 EST 구간에 도달하면 프리장 미체결 매수 주문(buy_order_id)을
    즉시 선취소하고 당일 신규 자동 매수를 전면 마감(is_session_done = True)한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (815 <= hm <= 1558):
        return {"status": "OUTSIDE_WINDOW"}

    canceled_syms = []
    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)

        if buy_oid and h_qty == 0:
            try:
                await context.api.cancel_order(buy_oid)
            except Exception as exc:
                logger.warning(
                    "⚠️ [%s] 08:15 컷오프 미체결 매수 취소 예외: %s", sym, exc
                )
            await context.ledger.update_state(
                sym,
                buy_order_id="",
                ordered_qty=0,
                buy_price=0.0,
                is_session_done=True,
            )
            canceled_syms.append(sym)
        elif not bool(st.get("is_session_done", False)):
            await context.ledger.update_state(sym, is_session_done=True)

    if canceled_syms and hasattr(context, "send_slim_alert"):
        await context.send_slim_alert(
            title=f"🔒 [08:15 EST 거시지표 선제 컷오프] {', '.join(canceled_syms)}",
            body="미체결 매수덫 취소 완료 · 당일 신규 자동매수 마감 (보유분 트레일링/3차방어 유지)",
        )

    return {"status": "CUTOFF_ENFORCED", "canceled": canceled_syms}


# ---------------------------------------------------------------------------
# [핵심 5] 15:00 ~ 15:58 EST 정규장 마감 1시간 전 미익절 정규장 VWAP 도달 탈출 (VWAP_ESCAPE)
# ---------------------------------------------------------------------------
async def execute_1500_vwap_escape_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 101:
    15:00:00 ~ 15:58:59 EST (정규장 마감 1시간 전)까지 아직 +1.00% 트레일링이 점화되지 않은
    미익절 보유 물량(수동 포함)이 존재할 때, 해당 종목의 실시간 현재가가
    당일 정규장(09:30 EST ~) 누적 VWAP(reg_vwap) 이상에 도달하면
    즉시 매수 1호가 LIMIT 스윕으로 전량 탈출(VWAP_ESCAPE)한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (1500 <= hm <= 1558):
        return {"status": "OUTSIDE_WINDOW"}

    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0 or avg_p <= 0.0:
            continue

        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        t_state = getattr(context, "trail_runtime_cache", {}).get(sym, {})
        if isinstance(t_state, dict) and t_state.get("armed"):
            continue

        met = _get_live_symbol_metrics(context, sym, prices, dt_now)
        live_p = safe_float(met.get("live_price"), 0.0)
        reg_vwap = safe_float(met.get("reg_vwap"), 0.0)
        if live_p <= 0.0 or reg_vwap <= 0.0:
            continue

        if live_p >= reg_vwap:
            bid1_p = await context.api.get_best_bid_price(
                sym, fallback_price=live_p
            )
            sweep_p = math.floor(min(live_p, bid1_p) * 100.0) / 100.0
            await execute_bulletproof_sell_sweep(
                context=context,
                symbol=sym,
                holdings_qty=h_qty,
                sweep_price=sweep_p,
                reason_tag="VWAP_ESCAPE",
            )
            await reconcile_zero_holdings_exit_plugin(
                context=context,
                holdings={sym: {"qty": 0, "avg_price": 0.0}},
                macro=macro,
                now_est=dt_now,
                override_exit_price=sweep_p,
                override_exit_reason=f"VWAP_ESCAPE(정규장VWAP ${reg_vwap:.2f} 도달 탈출)",
            )
            return {
                "status": "VWAP_ESCAPE_EXECUTED",
                "symbol": sym,
                "sweep_price": sweep_p,
                "reg_vwap": reg_vwap,
            }

    return {"status": "MONITORING_1500_VWAP"}


# ---------------------------------------------------------------------------
# [핵심 6] 15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 강제 청산 (Case 16, 24)
# ---------------------------------------------------------------------------
async def execute_1559_moc_liquidation_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 10, 16, 24:
    15:59:00 ~ 16:01:59 EST 구간에 도달하면(스나이퍼 가동 ON 상태 기준)
    계좌에 남아 있는 모든 SOXS / SOXL 보유 물량 및 미체결 주문을
    2단계 방탄 긴급 매도(modify_order 매수 1호가 정정 + 실패 시 선취소 후 신규 LIMIT 발사)로
    100% 강제 청산하고 실발생 순수익금을 정산하여 제로-오버나이트를 사수한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (1559 <= hm <= 1601):
        return {"status": "OUTSIDE_WINDOW"}

    liquidated_syms = []
    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if not bool(st.get("is_active", True)):
            continue

        buy_oid = str(st.get("buy_order_id") or "").strip()
        if buy_oid:
            try:
                await context.api.cancel_order(buy_oid)
            except Exception:
                pass
            await context.ledger.update_state(
                sym, buy_order_id="", ordered_qty=0
            )

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        if h_qty <= 0:
            continue

        live_p = safe_float(
            (prices or {}).get(sym), safe_float(h_info.get("last_price"), avg_p)
        )
        bid1_p = await context.api.get_best_bid_price(
            sym, fallback_price=live_p
        )
        # 체결 100% 보장을 위해 매수 1호가 대비 0.2% 공격적 하향 지정가(최소 $0.01) 투하
        moc_sweep_p = (
            math.floor((min(live_p, bid1_p) * 0.9980) * 100.0) / 100.0
            if bid1_p > 0.0
            else math.floor(live_p * 100.0) / 100.0
        )
        moc_sweep_p = max(0.01, moc_sweep_p)

        await execute_bulletproof_sell_sweep(
            context=context,
            symbol=sym,
            holdings_qty=h_qty,
            sweep_price=moc_sweep_p,
            reason_tag="MOC_ZERO_OVERNIGHT",
        )
        await context.ledger.update_state(sym, is_session_done=True)
        await reconcile_zero_holdings_exit_plugin(
            context=context,
            holdings={sym: {"qty": 0, "avg_price": 0.0}},
            macro=macro,
            now_est=dt_now,
            override_exit_price=moc_sweep_p,
            override_exit_reason="MOC_ZERO_OVERNIGHT(15:59 제로-오버나이트 강제청산)",
        )
        liquidated_syms.append(sym)

    return {"status": "MOC_CHECKED", "liquidated": liquidated_syms}
