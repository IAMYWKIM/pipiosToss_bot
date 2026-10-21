# -*- coding: utf-8 -*-
"""
# NEW: whipsaw_plugin.py
03:58 ~ 04:07 EST (9분간) 과열차단 독립형 듀얼 1주 고정 '휩소 노출 작전' 전술 플러그인 (Step 08 / 10)
- 제3헌법: 03:58 ~ 04:07 EST 1주 역산 매수 덫, +1.00% 익절 매도 덫, 04:07 정각 타임쉴드 정리 전담 분리
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: 1주 매수/매도 주문 생성 시 36자 이내 고유 clientOrderId 주입
- 🎯 2 & Case 24, 52, 54, 95, 96, 102:
  1) NQ=F >= 2.0% 과열차단(CANCELED_MACRO)일에도 본진과 별개로 독립 진입 허용
  2) 데이장 누적 VWAP 상단 종목(current_price > day_vwap, 양쪽 하회 시 이격도 1위 폴백) 기준
     하단 0.5% 역산 단가(floor((day_vwap / 1.005) * 100) / 100)에 1주 LIMIT 매수 덫 설치
  3) 1주 매수 체결 시 팩트 평단가 대비 +1.00%(ceil(avg_price * 1.0100 * 100) / 100) LIMIT 매도 덫 장전
  4) 04:07:00 EST 정각 도달 시 미체결 1주 매수 덫 무비용 취소 및 보유 1주 현재가 즉시 매도 후
     NQ < 2.0% 이면 프리장 본진 해금, NQ >= 2.0% 이면 CANCELED_MACRO 과열차단 복원
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from defense_plugin import unlock_all_symbols_for_reentry
from quant_engine import NQ_OVERHEAT_THRESHOLD
from quant_indicators_plugin import (
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from sniper_plugin import mark_auto_fill_grace_shield
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.whipsaw_plugin")


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 데이장 누적 VWAP 상단 종목 선정 및 0.5% 역산 단가(VWAP / 1.005) 산출
# ---------------------------------------------------------------------------
def _select_whipsaw_target_symbol(
    context: Any,
    prices: Dict[str, float],
    now_est: datetime,
) -> Dict[str, Any]:
    """
    🎯 2 [선취 매수 폐기 및 무비용 하단 0.5% 역산 매수 덫 전용화 원칙]:
    - SOXS 와 SOXL 중 데이장 누적 VWAP(day_vwap)보다 높은 가격(current_price > day_vwap)에
      위치한 종목을 우선 선정한다 (양쪽 모두 상회 시 day_disp_curr 상위 1위 선정).
    - 만약 양쪽 모두 day_vwap 하단에 위치할 경우 상대 괴리율(day_disp_curr) 1위 종목으로 폴백한다.
    - 선정된 종목의 데이장 VWAP 에서 0.5% 역산한 하단 가격(floor((day_vwap / 1.005) * 100) / 100,
      단 현재가 cur_p_fl 이하 캡 적용)을 반환한다.
    """
    raw_cache = getattr(context, "candle_metrics_cache", {})
    above_candidates: List[Tuple[str, float, float, float, float]] = []
    all_candidates: List[Tuple[str, float, float, float, float]] = []

    for sym in REAL_ALLOWED_SYMBOLS:
        base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p <= 0.0 and isinstance(base_met, dict):
            live_p = safe_float(base_met.get("live_price"), 0.0)
        met = refresh_metrics_with_live_tick(base_met, live_p, now_est=now_est)

        day_vwap = safe_float(met.get("day_vwap"), 0.0)
        cur_p = safe_float(met.get("live_price"), 0.0)
        if day_vwap <= 0.0 or cur_p <= 0.0:
            continue

        disp_pct = ((cur_p - day_vwap) / day_vwap) * 100.0
        raw_rev_p = safe_float(met.get("whipsaw_buy_p"), 0.0)
        if raw_rev_p <= 0.0:
            raw_rev_p = math.floor((day_vwap / 1.005) * 100.0) / 100.0

        cur_p_fl = math.floor(cur_p * 100.0) / 100.0
        trap_p = min(cur_p_fl, raw_rev_p)
        if trap_p <= 0.0:
            continue

        item = (sym, disp_pct, day_vwap, cur_p, trap_p)
        all_candidates.append(item)
        if cur_p > day_vwap:
            above_candidates.append(item)

    target_list = above_candidates if above_candidates else all_candidates
    if not target_list:
        return {
            "symbol": "",
            "disp_pct": 0.0,
            "day_vwap": 0.0,
            "live_price": 0.0,
            "trap_buy_price": 0.0,
        }

    target_list.sort(key=lambda x: x[1], reverse=True)
    win_sym, win_disp, win_vwap, win_cur, win_trap = target_list[0]
    return {
        "symbol": win_sym,
        "disp_pct": round(win_disp, 3),
        "day_vwap": round(win_vwap, 4),
        "live_price": round(win_cur, 4),
        "trap_buy_price": win_trap,
    }


# ---------------------------------------------------------------------------
# [핵심 1] 03:58 ~ 04:06 EST 과열차단 독립형 1주 역산 매수 덫 발사 (execute_0358_whipsaw_trap_plugin)
# ---------------------------------------------------------------------------
async def execute_0358_whipsaw_trap_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 & 초과 Case 95, 초과 Case 96:
    - 03:58:00 ~ 04:06:59 EST (9분간) 윈도우에서 당일 단 1회 1주(quantity = 1) 역산 매수 덫을 설치한다.
    - 당일 데이장에서 NQ=F 진폭이 2.0%를 초과하여 CANCELED_MACRO(is_session_done = True) 상태이더라도
      차단되지 않고 본진과 별개로 정상 진입하여 매수 체결가 대비 +1.00% 익절을 노린다!
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (358 <= hm <= 406):
        return {"status": "OUTSIDE_WHIPSAW_WINDOW"}

    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
    whipsaw_cache: Dict[str, Any] = getattr(context, "whipsaw_runtime_cache", {})
    if whipsaw_cache.get("logical_date") != logical_date:
        whipsaw_cache.clear()
        whipsaw_cache.update(
            {
                "logical_date": logical_date,
                "fired_today": False,
                "cutoff_done": False,
                "symbol": "",
                "trap_price": 0.0,
            }
        )

    # 당일 이미 1주 휩소 매수 덫을 1회 발사했다면 중복 발사 금지
    if whipsaw_cache.get("fired_today"):
        return {"status": "ALREADY_FIRED_TODAY"}

    # 현재 실보유 잔고나 미체결 주문이 존재하면 진입 보류 (수동 보유 포지션 등 보호)
    for s in REAL_ALLOWED_SYMBOLS:
        if safe_int((holdings or {}).get(s, {}).get("qty"), 0) > 0:
            return {"status": "EXISTING_HOLDINGS_PRESENT"}
        st_s = context.ledger.get_state(s)
        if str(st_s.get("buy_order_id") or "").strip() or str(st_s.get("sell_order_id") or "").strip():
            return {"status": "EXISTING_ORDER_PRESENT"}

    cand = _select_whipsaw_target_symbol(context, prices, dt_now)
    sym = str(cand.get("symbol") or "").upper().strip()
    trap_p = safe_float(cand.get("trap_buy_price"), 0.0)
    day_vwap = safe_float(cand.get("day_vwap"), 0.0)
    live_p = safe_float(cand.get("live_price"), 0.0)

    if sym not in REAL_ALLOWED_SYMBOLS or trap_p <= 0.0:
        return {"status": "TARGET_NOT_READY"}

    st_sym = context.ledger.get_state(sym)
    if not bool(st_sym.get("is_active", True)):
        return {"status": "BOT_INACTIVE"}

    # 1주 매수가 가능한 USD 매수가능금액 확인
    bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.5)
    if bp_usd > 0.0 and bp_usd < trap_p:
        return {"status": "INSUFFICIENT_USD_FOR_1SH", "bp_usd": bp_usd, "trap_p": trap_p}

    other_sym = "SOXL" if sym == "SOXS" else "SOXS"
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    qqq_ref = await context.macro_cache.ensure_qqq_live_price(
        safe_float((macro or {}).get("qqq_price"), 0.0)
    )
    if qqq_ref <= 0.0:
        qqq_ref = safe_float((macro or {}).get("nq_price"), 0.0)

    async with context.in_memory_ordering_lock:
        async with context.dual_strike_mutex:
            if whipsaw_cache.get("fired_today"):
                return {"status": "ALREADY_FIRED_IN_MUTEX"}

            cid = generate_client_order_id(prefix=f"WHP{sym}")
            try:
                # 03:58 EST 이후이므로 enforce_daymarket_block=True 여도 통과되며, 단 1주(quantity=1)만 발사
                ord_res = await context.api.create_order(
                    symbol=sym,
                    side="BUY",
                    quantity=1,
                    price=trap_p,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                    enforce_daymarket_block=True,
                )
            except Exception as exc:
                logger.error("🚨 [%s] 03:58 1주 휩소 역산 매수덫 발사 실패: %s", sym, exc)
                return {"status": "ORDER_ERROR", "error": str(exc)}

            oid = str(ord_res.get("orderId") or "").strip()
            trend_tag = "DOWN" if sym == "SOXS" else "UP"

            whipsaw_cache.update(
                {
                    "fired_today": True,
                    "symbol": sym,
                    "trap_price": trap_p,
                    "order_id": oid,
                }
            )

            # Case 95 & 96: entry_session = "WHIPSAW_1SH" 로 각인하여 1·2차 과열 방어선을 100% 패스시킨다!
            await context.ledger.update_state(
                sym,
                buy_order_id=oid,
                client_order_id=cid,
                ordered_qty=1,
                buy_price=trap_p,
                entry_session="WHIPSAW_1SH",
                entry_trend=trend_tag,
                nq_entry_price=qqq_ref,
                nq_entry_amp=nq_amp,
                is_session_done=True,
                entry_ts=time.time(),
            )
            await context.ledger.update_state(other_sym, is_session_done=True)
            context.wakeup_event.set()

            overheat_note = " · ⚡과열차단일 독립가동" if nq_amp >= NQ_OVERHEAT_THRESHOLD else ""
            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"⚡ [03:58 1주 휩소 역산 매수덫 장전] {sym} 1주",
                    body=(
                        f"역산가 ${trap_p:.2f} (데이장VWAP ${day_vwap:.2f} / 1.005 · 현가 ${live_p:.2f})"
                        f" · 04:07 정각 타임쉴드{overheat_note}"
                    ),
                )

            return {
                "status": "WHIPSAW_1SH_TRAP_PLACED",
                "symbol": sym,
                "orderId": oid,
                "price": trap_p,
                "day_vwap": day_vwap,
            }


# ---------------------------------------------------------------------------
# [핵심 2] 03:58 ~ 04:06 EST 1주 체결 감시 및 +1.00% 익절 매도 덫 장전 (manage_whipsaw_1sh_profit_trap)
# ---------------------------------------------------------------------------
async def manage_whipsaw_1sh_profit_trap(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 & Case 102:
    - 03:58 ~ 04:06 EST 동안 WHIPSAW_1SH 1주 매수 주문의 FILLED 여부를 프로빙하여
      체결 시 즉시 mark_auto_fill_grace_shield 를 각인(MANUAL 오분류 차단)하고,
    - 실보유 잔고(holdings_qty >= 1)가 확인되면 실제 매수 평단가(avg_price) 대비 +1.00% 익절가
      (ceil(avg_price * 1.0100 * 100) / 100)로 1주 지정가 매도 덫을 즉시 장전한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (358 <= hm <= 406):
        return {"status": "OUTSIDE_WINDOW"}

    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if str(st.get("entry_session") or "") != "WHIPSAW_1SH":
            continue

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        sell_oid = str(st.get("sell_order_id") or "").strip()

        # 1) 잔고 미반영 상태에서 매수 주문 FILLED 프로빙 -> 인덱싱 시차 그레이스 쉴드 각인 (Case 102)
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
                if ord_st == "FILLED" or f_qty > 0:
                    mark_auto_fill_grace_shield(
                        context,
                        symbol=sym,
                        entry_session="WHIPSAW_1SH",
                        fill_price=f_avg if f_avg > 0.0 else safe_float(st.get("buy_price"), 0.0),
                    )
                    context.wakeup_event.set()
            continue

        # 2) 1주 체결 완료 후 아직 +1.00% 익절 매도 주문이 장전되지 않은 경우 즉시 장전
        if h_qty > 0 and avg_p > 0.0 and not sell_oid:
            target_100_p = math.ceil((avg_p * 1.0100) * 100.0) / 100.0
            qqq_now = await context.macro_cache.ensure_qqq_live_price(
                safe_float((macro or {}).get("qqq_price"), 0.0)
            )
            if qqq_now <= 0.0:
                qqq_now = safe_float((macro or {}).get("nq_price"), 0.0)
            nq_amp_now = safe_float((macro or {}).get("nq_amp"), 0.0)

            async with context.in_memory_ordering_lock:
                cid = generate_client_order_id(prefix=f"WHPS{sym}")
                try:
                    ord_res = await context.api.create_order(
                        symbol=sym,
                        side="SELL",
                        quantity=h_qty,
                        price=target_100_p,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        client_order_id=cid,
                        enforce_daymarket_block=False,
                    )
                    new_sell_oid = str(ord_res.get("orderId") or "").strip()
                    await context.ledger.update_state(
                        sym,
                        buy_order_id="",
                        holdings_qty=h_qty,
                        avg_price=avg_p,
                        sell_order_id=new_sell_oid,
                        target_sell_price=target_100_p,
                        nq_entry_price=qqq_now,
                        nq_entry_amp=nq_amp_now,
                    )
                    snap_cache_map[sym] = {
                        "symbol": sym,
                        "last_holdings_qty": h_qty,
                        "last_avg_price": avg_p,
                        "peak_price": max(avg_p, safe_float((prices or {}).get(sym), avg_p)),
                        "entry_session": "WHIPSAW_1SH",
                        "safety_cap_price": target_100_p,
                        "arm_price": target_100_p,
                        "last_sweep_price": target_100_p,
                        "exit_tag": "WHIPSAW_TP_100",
                        "updated_ts": time.time(),
                    }
                    if hasattr(context, "send_slim_alert"):
                        await context.send_slim_alert(
                            title=f"⚡🎯 [1주 휩소 체결 · +1.00% 익절덫 장전] {sym} {h_qty}주",
                            body=f"매수평단 ${avg_p:.2f} ➔ 익절지정가 ${target_100_p:.2f}(+1.00%) · 04:07 정각 타임쉴드",
                        )
                    return {
                        "status": "WHIPSAW_SELL_TRAP_PLACED",
                        "symbol": sym,
                        "sell_order_id": new_sell_oid,
                        "target_price": target_100_p,
                    }
                except Exception as exc:
                    logger.error("🚨 [%s] 1주 휩소 +1.00%% 매도덫 장전 실패: %s", sym, exc)

    return {"status": "MONITORED"}


# ---------------------------------------------------------------------------
# [핵심 3] 04:07:00 EST 타임쉴드 만료 및 강제 정리 후 본진 인계 / 과열차단 복원
# ---------------------------------------------------------------------------
async def enforce_0407_whipsaw_cutoff_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 [04:07:00 EST 타임쉴드 만료 및 강제 정리 후 본진 인계/과열차단 복원] & Case 95, 96:
    - 04:07:00 EST 이상(hm >= 407) 도달 시 entry_session == 'WHIPSAW_1SH' 인 주문을 전수 점검한다.
    - ① [미체결 시 (비용 $0 취소)] : holdings_qty == 0 이고 buy_order_id 가 존재하면 즉시 cancel_order 로
         취소하고 장부의 buy_order_id='' 를 원자적으로 소각한 뒤 unlock_all_symbols_for_reentry() 를 호출한다.
         (NQ < 2.0% 이면 프리장 본진에 매수 권한 인계, NQ >= 2.0% 이면 즉시 CANCELED_MACRO 복원!)
    - ② [1주 보유 중일 시 (현재가 즉시 매도)] : holdings_qty > 0 이고 entry_session == 'WHIPSAW_1SH' 이면
         2단계 방탄 매도 스윕(sweep_sell_position_bulletproof)으로 실시간 매수 1호가/현재가에 즉시 매도 처분한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (407 <= hm <= 814):
        return {"status": "OUTSIDE_CUTOFF_WINDOW"}

    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    snap_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "exit_snapshot_cache", {}
    )
    grace_cache_map: Dict[str, Dict[str, Any]] = getattr(
        context, "indexing_grace_cache", {}
    )

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        if str(st.get("entry_session") or "") != "WHIPSAW_1SH":
            continue

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        sell_oid = str(st.get("sell_order_id") or "").strip()

        # ① 04:07:00 EST 미체결 상태 (비용 $0 취소 후 본진 인계 또는 과열차단 복원)
        if h_qty <= 0:
            # 단, 방금 04:06:59 에 체결되어 잔고 인덱싱 시차 중인지 주문 상세를 1회 확인
            if buy_oid:
                detail = await context.api.get_order_detail(buy_oid)
                if isinstance(detail, dict):
                    ord_st = str(detail.get("status") or "").upper().strip()
                    exec_obj = (
                        detail.get("execution")
                        if isinstance(detail.get("execution"), dict)
                        else {}
                    )
                    if ord_st == "FILLED" or safe_int(exec_obj.get("filledQuantity"), 0) > 0:
                        mark_auto_fill_grace_shield(
                            context,
                            symbol=sym,
                            entry_session="WHIPSAW_1SH",
                            fill_price=safe_float(exec_obj.get("averageFilledPrice"), 0.0),
                        )
                        continue

            async with context.in_memory_ordering_lock:
                if buy_oid:
                    try:
                        await context.api.cancel_order(buy_oid)
                    except Exception as exc:
                        logger.warning("⚠️ [%s] 04:07 1주 휩소 미체결 취소 예외: %s", sym, exc)

                grace_cache_map.pop(sym, None)
                unlock_res = await unlock_all_symbols_for_reentry(
                    context=context,
                    nq_amp=nq_amp,
                    reason="WHIPSAW_0407_UNFILLED_CANCEL",
                )
                context.wakeup_event.set()

                handover_msg = (
                    f"🛑 NQ진폭 {nq_amp:.2f}% >= 2.00% 과열차단(CANCELED_MACRO) 복원"
                    if nq_amp >= NQ_OVERHEAT_THRESHOLD
                    else "👑 04:07 프리장 본진 추세추종 스나이퍼 인계 완료"
                )
                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"⏰ [04:07 1주 휩소 타임쉴드 · 무비용($0) 취소] {sym}",
                        body=f"미체결 1주 매수덫 취소 완료 · {handover_msg}",
                    )
                return {
                    "status": "WHIPSAW_UNFILLED_CANCELED",
                    "symbol": sym,
                    "unlock_result": unlock_res,
                }

        # ② 04:07:00 EST 1주 보유 중인 상태 (현재가/매수 1호가 즉시 매도 스윕)
        live_p = safe_float((prices or {}).get(sym), 0.0)
        if live_p <= 0.0:
            live_p = safe_float(h_info.get("last_price"), avg_p)

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

                snap_cache_map[sym] = {
                    "symbol": sym,
                    "last_holdings_qty": h_qty,
                    "last_avg_price": avg_p if avg_p > 0.0 else sweep_p,
                    "peak_price": max(avg_p, live_p),
                    "entry_session": "WHIPSAW_1SH",
                    "last_sweep_price": sweep_p,
                    "exit_tag": "WHIPSAW_0407_CUTOFF",
                    "updated_ts": time.time(),
                }

                await context.ledger.update_state(
                    sym,
                    buy_order_id="",
                    sell_order_id=new_oid,
                    target_sell_price=sweep_p,
                )
                context.wakeup_event.set()

                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title=f"⏰🧹 [04:07 1주 휩소 타임쉴드 즉시 매도] {sym} {h_qty}주",
                        body=(
                            f"평단 ${avg_p:.2f} ➔ 현재가 스윕 ${sweep_p:.2f} 처분 발사 "
                            f"· 정산 직후 본진 인계/복원"
                        ),
                    )
                return {
                    "status": "WHIPSAW_HOLDING_SWEPT",
                    "symbol": sym,
                    "sweep_price": sweep_p,
                    "qty": h_qty,
                }
            except Exception as exc:
                logger.error("🚨 [%s] 04:07 1주 휩소 보유분 즉시 매도 실패: %s", sym, exc)

    return {"status": "NO_WHIPSAW_ACTION"}
