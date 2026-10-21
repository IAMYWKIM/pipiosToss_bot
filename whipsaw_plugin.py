# -*- coding: utf-8 -*-
"""
# MODIFIED: whipsaw_plugin.py
03:58 ~ 04:07 EST (9분간) 듀얼 1주 고정 '휩소 노출 작전' 전술 플러그인 (Step 03 / 03)
- 🎯 2 & Case 95, 96, 102 완벽 구현:
  1) [선취 매수 폐기 및 무비용 하단 0.5% 역산 매수 덫 전용화]
     데이장 누적 VWAP보다 높은 가격(current_price > day_vwap, 양쪽 하회 시 상대 괴리율 1위 폴백)에
     위치한 종목을 기준으로 데이장 VWAP에서 0.5% 역산한 하단 가격(VWAP / 1.005)에
     단 1주(quantity = 1) 지정가 매수 덫(entry_session = "WHIPSAW_1SH")만 설치하고,
     체결 즉시 실제 매수 평단가 대비 +1.00% 익절가(avg_price * 1.01)로 매도 주문을 장전한다.
  2) [NQ=F >= 2.0% 과열차단 독립(별개) 가동 원칙 (Case 95 & Case 96)]
     당일 데이장에서 NQ=F 진폭이 2.0%를 초과하여 CANCELED_MACRO 가 발동된 날에도
     차단되지 않고 별개로 정상 1주 진입하여 +1.00% 익절을 노린다.
  3) [04:07:00 EST 타임쉴드 만료 및 강제 정리 후 본진 인계 / 과열차단 복원]
     - 04:07:00 EST 미체결 시: 즉시 주문 취소(비용 $0) 후 NQ < 2.0% 이면 프리장 본진 해금,
       NQ >= 2.0% 이면 CANCELED_MACRO 복원
     - 04:07:00 EST 1주 보유 중일 시: 실시간 매수 1호가로 즉시 스윕 매도 및 복리 정산 후
       NQ < 2.0% 이면 프리장 본진 해금, NQ >= 2.0% 이면 CANCELED_MACRO 복원
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from quant_indicators_plugin import (
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    WHIPSAW_TARGET_PROFIT_PCT,
    ensure_safety_cap_sell_order,
    execute_bulletproof_sell_sweep,
    reconcile_zero_holdings_exit_plugin,
    unlock_all_symbols_for_reentry,
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

# 1주 휩소 작전 역산 비율 (VWAP / 1.005) 및 고정 수량 (1주)
WHIPSAW_DIVISOR: float = 1.005
WHIPSAW_FIXED_QTY: int = 1


def is_whipsaw_window(hm: int) -> bool:
    """
    🎯 2: 03:58:00 ~ 04:06:59 EST (9분간) 1주 휩소 노출 작전 활성 윈도우 여부 반환.
    (04:07:00 EST 정각은 enforce_0407_whipsaw_cutoff_plugin 이 타임쉴드 정리를 수행)
    """
    return bool(358 <= hm <= 406)


def _ensure_whipsaw_cache(context: Any, logical_date: str) -> Dict[str, Any]:
    cache: Dict[str, Any] = getattr(context, "whipsaw_runtime_cache", {})
    if cache.get("logical_date") != logical_date:
        cache.clear()
        cache.update(
            {
                "logical_date": logical_date,
                "fired_today": False,
                "target_symbol": "",
                "trap_price": 0.0,
                "order_id": "",
                "cutoff_0407_done": False,
            }
        )
    return cache


def _get_live_metrics_for_whipsaw(
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


def _select_whipsaw_symbol_and_vwap(
    soxs_met: Dict[str, Any],
    soxl_met: Dict[str, Any],
) -> Tuple[str, float, float, float]:
    """
    🎯 2:
    데이장 누적 VWAP(day_vwap, 부재 시 pre_vwap 또는 session_vwap 폴백)보다 높은 가격
    (current_price > day_vwap)에 위치한 종목을 선정한다.
    양쪽 모두 상회하거나 양쪽 모두 하회할 경우 VWAP 대비 상대 괴리율((live_p - vwap) / vwap)
    상위 1위 종목으로 폴백하여 (symbol, ref_vwap, live_price, disp_pct)를 반환한다.
    """
    candidates = []
    for sym, met in (("SOXS", soxs_met), ("SOXL", soxl_met)):
        if not isinstance(met, dict):
            continue
        live_p = safe_float(met.get("live_price"), 0.0)
        ref_vwap = safe_float(met.get("day_vwap"), 0.0)
        if ref_vwap <= 0.0:
            ref_vwap = safe_float(met.get("pre_vwap"), 0.0)
        if ref_vwap <= 0.0:
            ref_vwap = safe_float(met.get("session_vwap"), live_p)

        if live_p > 0.0 and ref_vwap > 0.0:
            disp_pct = ((live_p - ref_vwap) / ref_vwap) * 100.0
            candidates.append((disp_pct, sym, ref_vwap, live_p))

    if not candidates:
        return "", 0.0, 0.0, 0.0

    # 괴리율(disp_pct) 내림차순 정렬 -> current_price > day_vwap 인 종목이 자연스럽게 우선 선택됨
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_disp, best_sym, best_vwap, best_live = candidates[0]
    return best_sym, best_vwap, best_live, round(best_disp, 2)


# ---------------------------------------------------------------------------
# [핵심 1] 03:58 ~ 04:06 EST 과열차단 독립 1주 휩소 역산 매수 덫 발사 및 체결 감시
# ---------------------------------------------------------------------------
async def execute_whipsaw_1sh_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 2 & Case 95, 96, 102:
    - 03:58:00 ~ 04:06:59 EST 구간에서 작동한다.
    - 당일 NQ=F >= 2.0% 과열차단(CANCELED_MACRO)이 발동된 날에도 차단되지 않고 독립 가동된다!
    - 데이장 누적 VWAP 상단 종목을 기준으로 VWAP / 1.005 하단 가격에 1주 지정가 매수 덫을 설치하고,
      매수 체결 확인 시 실제 체결 평단가 대비 +1.00% 익절가(avg_price * 1.01)로 매도 주문을 장전한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not is_whipsaw_window(hm):
        return {"status": "OUTSIDE_WHIPSAW_WINDOW"}

    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
    w_cache = _ensure_whipsaw_cache(context, logical_date)

    # 1) 스나이퍼 마스터 전원(is_active) 확인
    st_soxs = context.ledger.get_state("SOXS")
    st_soxl = context.ledger.get_state("SOXL")
    if not (
        bool(st_soxs.get("is_active", True))
        and bool(st_soxl.get("is_active", True))
    ):
        return {"status": "BOT_INACTIVE"}

    # 2) 현재 보유 중인 포지션 또는 미체결 1주 휩소 매수 주문 점검
    for sym in REAL_ALLOWED_SYMBOLS:
        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        st = context.ledger.get_state(sym)
        entry_sess = str(st.get("entry_session") or "")
        buy_oid = str(st.get("buy_order_id") or "").strip()

        # 이미 1주 휩소 물량이 체결되어 보유 중인 경우 -> +1.00% 익절 매도 주문 선장전 보장
        if h_qty > 0 and avg_p > 0.0:
            if entry_sess == "WHIPSAW_1SH":
                await ensure_safety_cap_sell_order(context, sym, h_qty, avg_p)
                return {
                    "status": "WHIPSAW_HOLDING_MONITORED",
                    "symbol": sym,
                    "qty": h_qty,
                    "avg_price": avg_p,
                }
            # 수동 오버나이트 등 다른 포지션을 이미 보유 중이면 1주 휩소 진입 스킵
            return {"status": "EXISTING_HOLDINGS_SKIP", "symbol": sym}

        # 미체결 1주 휩소 매수 덫 대기 중인 경우 -> 체결 여부(get_order_detail) 즉시 프로빙
        if buy_oid and entry_sess == "WHIPSAW_1SH":
            detail = await context.api.get_order_detail(buy_oid)
            if isinstance(detail, dict):
                ord_status = str(detail.get("status") or "").upper().strip()
                exec_obj = (
                    detail.get("execution")
                    if isinstance(detail.get("execution"), dict)
                    else {}
                )
                filled_q = safe_int(exec_obj.get("filledQuantity"), 0)
                avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
                if ord_status in ("FILLED", "PARTIAL_FILLED") or filled_q > 0:
                    fill_p = (
                        avg_fill_p
                        if avg_fill_p > 0.0
                        else safe_float(st.get("buy_price"), 0.0)
                    )
                    # Case 102: 체결 직후 잔고 인덱싱 시차 중 MANUAL 오분류 차단 그레이스 쉴드 각인
                    mark_auto_fill_grace_shield(
                        context,
                        symbol=sym,
                        entry_session="WHIPSAW_1SH",
                        fill_price=fill_p,
                    )
                    context.wakeup_event.set()
                    return {
                        "status": "WHIPSAW_BUY_FILLED_DETECTED",
                        "symbol": sym,
                        "fill_price": fill_p,
                    }
            return {
                "status": "WHIPSAW_TRAP_WAITING",
                "symbol": sym,
                "orderId": buy_oid,
            }

    # 당일 이미 1주 휩소 매수 덫을 1회 발사했다면 중복 발사 금지
    if bool(w_cache.get("fired_today")):
        return {"status": "WHIPSAW_ALREADY_FIRED_TODAY"}

    # 3) 데이장 VWAP 상단 종목 선정 및 하단 0.5% 역산 단가(VWAP / 1.005) 산출
    soxs_met = _get_live_metrics_for_whipsaw(context, "SOXS", prices, dt_now)
    soxl_met = _get_live_metrics_for_whipsaw(context, "SOXL", prices, dt_now)
    win_sym, ref_vwap, live_p, disp_pct = _select_whipsaw_symbol_and_vwap(
        soxs_met, soxl_met
    )

    if win_sym not in REAL_ALLOWED_SYMBOLS or ref_vwap <= 0.0 or live_p <= 0.0:
        return {"status": "WHIPSAW_METRICS_NOT_READY"}

    raw_trap_p = math.floor(((ref_vwap / WHIPSAW_DIVISOR) + 1e-9) * 100.0) / 100.0
    cur_p_fl = math.floor((live_p + 1e-9) * 100.0) / 100.0
    trap_buy_p = min(cur_p_fl, raw_trap_p)
    if trap_buy_p <= 0.0:
        return {"status": "INVALID_WHIPSAW_TRAP_PRICE"}

    # 4) 1주 매수 예산 확인 후 지정가(LIMIT, DAY) 1주 매수 덫 투하 (과열일 CANCELED_MACRO 무관 독립 진입!)
    bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.2)
    if 0.0 < bp_usd < trap_buy_p:
        return {
            "status": "INSUFFICIENT_BUYING_POWER_FOR_1SH",
            "bp_usd": bp_usd,
            "trap_price": trap_buy_p,
        }

    async with context.in_memory_ordering_lock:
        async with context.dual_strike_mutex:
            if bool(w_cache.get("fired_today")):
                return {"status": "WHIPSAW_ALREADY_FIRED_IN_MUTEX"}

            nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
            qqq_ref = await context.macro_cache.ensure_qqq_live_price(
                safe_float((macro or {}).get("qqq_price"), 0.0)
            )
            if qqq_ref <= 0.0:
                qqq_ref = safe_float((macro or {}).get("nq_price"), 0.0)

            cid = generate_client_order_id(prefix=f"WHP{win_sym}")
            try:
                ord_res = await context.api.create_order(
                    symbol=win_sym,
                    side="BUY",
                    quantity=WHIPSAW_FIXED_QTY,
                    price=trap_buy_p,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                    enforce_daymarket_block=False,
                )
            except Exception as exc:
                logger.warning(
                    "⚠️ [%s] 03:58 1주 휩소 매수덫 발사 보류: %s",
                    win_sym,
                    exc,
                )
                return {"status": "WHIPSAW_ORDER_ERROR", "error": str(exc)}

            oid = str(ord_res.get("orderId") or "").strip()
            other_sym = "SOXL" if win_sym == "SOXS" else "SOXS"
            target_tp_p = (
                math.ceil(
                    (trap_buy_p * (1.0 + WHIPSAW_TARGET_PROFIT_PCT / 100.0))
                    * 100.0
                )
                / 100.0
            )

            w_cache.update(
                {
                    "fired_today": True,
                    "target_symbol": win_sym,
                    "trap_price": trap_buy_p,
                    "order_id": oid,
                    "cutoff_0407_done": False,
                }
            )

            await context.ledger.update_state(
                win_sym,
                buy_order_id=oid,
                client_order_id=cid,
                ordered_qty=WHIPSAW_FIXED_QTY,
                buy_price=trap_buy_p,
                target_sell_price=target_tp_p,
                entry_session="WHIPSAW_1SH",
                entry_trend=("DOWN" if win_sym == "SOXS" else "UP"),
                nq_entry_price=qqq_ref,
                nq_entry_amp=nq_amp,
                is_session_done=True,
                last_modify_mono=time.monotonic(),
                entry_ts=time.time(),
            )
            await context.ledger.update_state(
                other_sym,
                is_session_done=True,
            )

            context.wakeup_event.set()

            if hasattr(context, "send_slim_alert"):
                overheat_badge = (
                    " · 🔥과열일 독립가동" if nq_amp >= 2.0 else ""
                )
                await context.send_slim_alert(
                    title=f"🌪️ [03:58 1주 휩소 매수덫 장전] {win_sym} 1주",
                    body=(
                        f"역산지정가 ${trap_buy_p:.2f} (데이장VWAP ${ref_vwap:.2f} / 1.005 · 이격 {disp_pct:+.2f}%)"
                        f" · 체결시 목표 ${target_tp_p:.2f}(+1.00%){overheat_badge}"
                    ),
                )

            return {
                "status": "WHIPSAW_TRAP_PLACED",
                "symbol": win_sym,
                "orderId": oid,
                "trap_price": trap_buy_p,
                "ref_vwap": ref_vwap,
            }


# ---------------------------------------------------------------------------
# [핵심 2] 04:07:00 EST 타임쉴드 만료 정리 및 프리장 본진 인계 / 과열차단 복원
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
    - 04:07:00 EST 이상 도달 시:
    ① 미체결 시 (비용 $0 취소): 1주 매수 덫 주문이 미체결 상태(holdings_qty == 0, entry_session == 'WHIPSAW_1SH')이면
       즉시 cancel_order 로 취소하고 장부의 buy_order_id="" 소각 후 unlock_all_symbols_for_reentry() 호출!
    ② 1주 보유 중일 시 (현재가 즉시 매도): 체결되어 보유 중(holdings_qty > 0, entry_session == 'WHIPSAW_1SH')이나
       아직 +1.00% 목표가에 매도되지 않았다면, execute_bulletproof_sell_sweep 으로 실시간 매수 1호가에
       즉시 매도 처분 및 정산 완료 후 unlock_all_symbols_for_reentry() 호출!
    ③ 이미 04:07 이전에 +1.00% 익절 체결 완료된 경우에도 04:07 정각에 unlock_all_symbols_for_reentry() 를
       호출하여 당일 NQ < 2.0% 이면 프리장 본진에 인계하고 NQ >= 2.0% 이면 CANCELED_MACRO 로 복원한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if hm < 407:
        return {"status": "BEFORE_0407_CUTOFF"}

    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
    w_cache = _ensure_whipsaw_cache(context, logical_date)

    has_whipsaw_ledger = any(
        str(context.ledger.get_state(s).get("entry_session") or "")
        == "WHIPSAW_1SH"
        for s in REAL_ALLOWED_SYMBOLS
    )
    if bool(w_cache.get("cutoff_0407_done")) and not has_whipsaw_ledger:
        return {"status": "ALREADY_CUTOFF_DONE"}

    handled_any = False

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        entry_sess = str(st.get("entry_session") or "")
        if entry_sess != "WHIPSAW_1SH":
            continue

        h_info = (holdings or {}).get(sym, {})
        h_qty = safe_int(h_info.get("qty"), 0)
        avg_p = safe_float(h_info.get("avg_price"), 0.0)
        buy_oid = str(st.get("buy_order_id") or "").strip()

        # 케이스 ①: 04:07:00 EST 미체결 상태 -> 즉시 무비용 취소 후 본진 인계/과열차단 복원
        if h_qty == 0 and buy_oid:
            # 취소 직전 혹시 방금 체결되었는지 1회 최종 확인
            detail = await context.api.get_order_detail(buy_oid)
            exec_obj = (
                detail.get("execution")
                if (isinstance(detail, dict) and isinstance(detail.get("execution"), dict))
                else {}
            )
            filled_q = safe_int(exec_obj.get("filledQuantity"), 0)
            avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
            if filled_q > 0:
                h_qty = filled_q
                avg_p = (
                    avg_fill_p
                    if avg_fill_p > 0.0
                    else safe_float(st.get("buy_price"), 0.0)
                )
            else:
                try:
                    await context.api.cancel_order(buy_oid)
                except Exception as exc:
                    logger.warning(
                        "⚠️ [%s] 04:07 휩소 미체결 매수 취소 예외: %s",
                        sym,
                        exc,
                    )
                unlock_res = await unlock_all_symbols_for_reentry(
                    context, macro=macro, now_est=dt_now
                )
                w_cache["cutoff_0407_done"] = True
                handled_any = True
                if hasattr(context, "send_slim_alert"):
                    handover_txt = (
                        "🛑 과열차단 복원(NQ>=2.0%)"
                        if unlock_res.get("status") == "LOCKED_MACRO"
                        else "🔫 프리장 본진 인계 완료"
                    )
                    await context.send_slim_alert(
                        title=f"🌪️🔒 [04:07 휩소 타임쉴드 · 미체결 무비용 취소] {sym}",
                        body=f"1주 매수덫 취소 완료 (비용 $0) · {handover_txt}",
                    )
                continue

        # 케이스 ②: 04:07:00 EST 1주 보유 중 -> 실시간 매수 1호가로 즉시 매도 처분 후 본진 인계/과열 복원
        if h_qty > 0:
            live_p = safe_float(
                (prices or {}).get(sym),
                safe_float(h_info.get("last_price"), avg_p),
            )
            bid1_p = await context.api.get_best_bid_price(
                sym, fallback_price=live_p
            )
            sweep_p = max(
                0.01,
                math.floor(min(live_p, bid1_p) * 100.0) / 100.0
                if (live_p > 0.0 and bid1_p > 0.0)
                else math.floor(max(live_p, avg_p) * 100.0) / 100.0,
            )

            await execute_bulletproof_sell_sweep(
                context=context,
                symbol=sym,
                holdings_qty=h_qty,
                sweep_price=sweep_p,
                reason_tag="WHIPSAW_0407_CUTOFF",
            )
            await reconcile_zero_holdings_exit_plugin(
                context=context,
                holdings={sym: {"qty": 0, "avg_price": 0.0}},
                macro=macro,
                now_est=dt_now,
                override_exit_price=sweep_p,
                override_exit_reason="WHIPSAW_0407_CUTOFF(04:07 타임쉴드 즉시청산)",
            )
            await unlock_all_symbols_for_reentry(
                context, macro=macro, now_est=dt_now
            )
            w_cache["cutoff_0407_done"] = True
            handled_any = True
            continue

        # 잔고 0주이고 buy_order_id 도 없는 잔류 WHIPSAW_1SH 상태 정리
        await unlock_all_symbols_for_reentry(
            context, macro=macro, now_est=dt_now
        )
        w_cache["cutoff_0407_done"] = True
        handled_any = True

    # 케이스 ③: 04:07 이전에 이미 +1.00% 익절로 청산 완료되었던 경우에도 04:07 정각 1회 본진 인계/과열 복원 보장
    if (
        not handled_any
        and bool(w_cache.get("fired_today"))
        and not bool(w_cache.get("cutoff_0407_done"))
    ):
        any_holding = any(
            safe_int((holdings or {}).get(s, {}).get("qty"), 0) > 0
            for s in REAL_ALLOWED_SYMBOLS
        )
        if not any_holding:
            await unlock_all_symbols_for_reentry(
                context, macro=macro, now_est=dt_now
            )
        w_cache["cutoff_0407_done"] = True

    return {"status": "WHIPSAW_0407_CUTOFF_CHECKED", "handled": handled_any}
