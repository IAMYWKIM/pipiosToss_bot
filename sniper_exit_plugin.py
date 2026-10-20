# =====================================================================
# FILE: sniper_exit_plugin.py
# 목적: 잔고 0주 복귀 시 매수취소/매도청산 정밀 판별 및 실제 발생 순수익금(Realized Net PnL) 통합 복리 정산,
#       매수 FILLED 직후 잔고 인덱싱 시차(0.1~0.4초) 장부 보존 방어(초과 Case 100),
#       상단 안전 캡(+5.00% LIMIT, 1주 휩소는 +1.00% LIMIT) 선장전 및
#       진입 성격별 이원화 하이킨 아시 트레일링(기술적 반등 ⚡OLD_IMM_CB [-0.30%] vs
#       진성 상승종목·10분봉 대파동 꺾임목 🚀PEAK_LCK_130 / 📊BAR_BEAR_020 [+1.30%전 양봉홀딩 ➔ -0.20%])
#       0.1초 즉시 정정 스윕 코어 플러그인 (sniper_plugin.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 청산·복리정산·이원화 트레일링 스윕 코어를 sniper_exit_plugin.py로 분리
# MODIFIED: 초과 Case 49, 51, 76 — 실수익(세전손익 - 왕복수수료 0.14%) 기반 통합 시드 복리 정산 및 매수 취소 허위 익절 억제
# MODIFIED: 초과 Case 85, 89, 91, 99 — 상단 안전 캡(+5.00%) 선장전, 이원화 하이킨 아시 트레일링 스윕 및 토스 QQQ 실시간 급등락 HA음봉 바이패스
# MODIFIED: 초과 Case 94 & Case 95 — 스나이퍼 OFF 시 매도 주문 취소 및 QQQ 기준가 초기화, 1주 휩소(WHIPSAW_1SH) +1.00% 익절 장전 및 과열차단 복원
# MODIFIED: 초과 Case 100 — 매수 FILLED 직후 잔고 인덱싱 시차 동안 장부가 초기화되어 자동 물량이 MANUAL로 둔갑하는 경합 버그 원천 소각
# NEW: 초과 Case 101 — 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 돌파 꺾임목(PRE_PIVOT_10M / PRE_PIVOT_10M_REARM) 포지션을
#      진성 추세 매도 레짐(🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020)에 자동 결속

import math
import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    ROUND_TRIP_COMMISSION_RATE,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
    calc_compound_next_budget,
    compute_realtime_tick_heikin_ashi,
)
from whipsaw_plugin import (
    WHIPSAW_SELL_PROFIT_FACTOR,
)
from defense_plugin import (
    TRAIL_ARM_PROFIT_RATE,
    TRAIL_FLOOR_LOCK_RATE,
    TRAIL_DROP_CALLBACK_PCT,
    TRAIL_SAFETY_CAP_RATE,
    NQ_VELOCITY_SURGE_PCT,
    _AUTO_ENTRY_SESSION_TAGS,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    _reset_trailing_runtime_state,
    _cancel_open_orders_and_wait_sellable,
)

EST_TZ = ZoneInfo("America/New_York")

_TREND_REGIME_ENTRY_TAGS = (
    "PRE_PULLBACK",
    "PRE_PULLBACK_REARM",
    "PRE_PIVOT_10M",
    "PRE_PIVOT_10M_REARM",
)


def _resolve_exit_regime_for_symbol(symbol: str, entry_session: str) -> str:
    """
    초과 Case 99 & Case 101:
    진성 상승종목 눌림목(PRE_PULLBACK / PRE_PULLBACK_REARM),
    10분봉 대파동 돌파 꺾임목(PRE_PIVOT_10M / PRE_PIVOT_10M_REARM),
    또는 당일 상승 주도주(leader_sym)의 수동(MANUAL) 보유 포지션은
    초입(+1.30% 전) 양봉 홀딩 후 -0.20% 타이트 스탑을 수행하는 'PEAK_LCK_130' 레짐으로 배정합니다.
    반면 단기 기술적 반등(PRE_SNIPER / PRE_SNIPER_REARM)은 'OLD_IMM_CB' 레짐으로 배정합니다.
    """
    es = str(entry_session or "")
    if es in _TREND_REGIME_ENTRY_TAGS:
        return "PEAK_LCK_130"
    if es == "MANUAL" and str(_PRE_LEADER_CACHE.get("leader_sym") or "") == symbol:
        return "PEAK_LCK_130"
    return "OLD_IMM_CB"


# Case 19, 46, 49, 51, 52, 76, 87, 95, 100 — 잔고 0주 복귀 시 정밀 정산 및 골든타임 무한 재출동 플러그인
async def reconcile_zero_holdings_exit_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    last_buy_price: float,
    budget: float,
    target_profit_rate: float,
    target_sell_price: float,
    buy_order_id: str,
    sell_order_id: str,
    entry_session: str,
    runtime_state: dict = None,
    current_price: float = 0.0,
) -> bool:
    if symbol not in ("SOXS", "SOXL"):
        return False

    has_dirty_ledger = bool(
        last_buy_price > 0.0
        or target_sell_price > 0.0
        or bool(buy_order_id)
        or bool(sell_order_id)
        or (entry_session in _AUTO_ENTRY_SESSION_TAGS)
        or (entry_session == "MANUAL")
    )
    if not has_dirty_ledger:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    r_state = runtime_state if isinstance(runtime_state, dict) else {}

    # =================================================================
    # 1단계: buy_order_id 검증 및 초과 Case 100 (매수 FILLED 직후 잔고 인덱싱 시차 보호)
    # =================================================================
    is_buy_cancel_exit = False
    if buy_order_id:
        try:
            b_od = await ctx.client.get_order_detail(buy_order_id)
            b_status = str(b_od.get("status") or "").upper()
            b_exec = b_od.get("execution") or {}
            b_filled_q = int(math.floor(float(b_exec.get("filledQuantity") or 0.0)))

            # 대기 중이거나 정정/취소 처리 중이면 절대 장부를 지우지 않고 대기
            if b_status in ("PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"):
                return False

            # 초과 Case 100 핵심 방어:
            # 매수 주문이 방금 FILLED(또는 filledQuantity > 0) 되었으나 토스 잔고 API(/api/v1/holdings)에
            # 아직 0주로 찍히는 0.1~0.4초 인덱싱 시차 구간이고 아직 안전 캡(sell_order_id)이 장전되기 전이라면,
            # 절대 매수 취소로 오판하여 장부(entry_session 등)를 초기화하지 않고 즉시 False를 반환해 보존!
            if (b_status == "FILLED" or b_filled_q > 0) and last_buy_price <= 0.0 and not sell_order_id:
                await asyncio.sleep(0.25)
                hold_recheck = await ctx.client.get_symbol_holdings_detail(symbol)
                re_qty = int(math.floor(float(hold_recheck.get("qty", 0.0))))
                if re_qty > 0:
                    ctx.shared_holdings[symbol] = re_qty
                    ctx.wakeup_event.set()
                return False

            if b_status in ("CANCELED", "REJECTED", "REPLACED") and b_filled_q <= 0 and last_buy_price <= 0.0 and not sell_order_id:
                is_buy_cancel_exit = True
        except Exception:
            return False

    # =================================================================
    # 2단계: sell_order_id 검증 및 실제 매도 체결가·체결 수량 추출 (초과 Case 19, 49, 51)
    # =================================================================
    exit_price = 0.0
    filled_qty = int(r_state.get("last_known_qty", 0) or 0)

    if sell_order_id and not is_buy_cancel_exit:
        try:
            s_od = await ctx.client.get_order_detail(sell_order_id)
            s_status = str(s_od.get("status") or "").upper()
            s_exec = s_od.get("execution") or {}
            s_filled_q = int(math.floor(float(s_exec.get("filledQuantity") or 0.0)))
            s_avg_p = float(s_exec.get("averageFilledPrice") or s_od.get("price") or 0.0)

            if s_status in ("PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"):
                # 잔고는 0주인데 매도 주문이 아직 PENDING으로 보이면 0.3초 대기 후 잔고 재확인
                await asyncio.sleep(0.3)
                hold_chk = await ctx.client.get_symbol_holdings_detail(symbol)
                if int(math.floor(float(hold_chk.get("qty", 0.0)))) > 0:
                    return False

            if s_filled_q > 0:
                filled_qty = s_filled_q
            if s_avg_p > 0.0:
                exit_price = s_avg_p
        except Exception:
            pass

    # 매수 체결 이력(last_buy_price)이 전혀 없고 매도 주문도 없이 buy_order_id만 취소된 경우 (Case 51: 허위 익절 타전 억제)
    if is_buy_cancel_exit or (last_buy_price <= 0.0 and not sell_order_id):
        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)
        await AssassinLedger.save_state(
            symbol,
            price=0.0,
            target_sell_price=0.0,
            buy_order_id="",
            sell_order_id="",
            entry_session="" if entry_session != "CANCELED_MACRO" else "CANCELED_MACRO",
            entry_trend="",
            entry_time=0.0,
            nq_entry_price=0.0,
            nq_entry_amp=0.0,
            whipsaw_target_price=0.0,
        )
        if 400 <= est_hm <= 814 and entry_session != "CANCELED_MACRO":
            await AssassinLedger.unlock_all_symbols_for_reentry()
        r_state["last_known_qty"] = 0
        r_state["last_known_avg"] = 0.0
        return True

    # =================================================================
    # 3단계: 스나이퍼 가동 OFF 중 수동 청산 식별 (초과 Case 46: Silent Reset)
    # =================================================================
    if not is_active:
        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)
        await AssassinLedger.save_state(
            symbol,
            price=0.0,
            target_sell_price=0.0,
            buy_order_id="",
            sell_order_id="",
            entry_session="",
            entry_trend="",
            entry_time=0.0,
            nq_entry_price=0.0,
            nq_entry_amp=0.0,
            whipsaw_target_price=0.0,
        )
        r_state["last_known_qty"] = 0
        r_state["last_known_avg"] = 0.0
        return True

    # =================================================================
    # 4단계: 실제 매도 체결 완료 정산 (실발생 순수익금 복리 가감 및 골든타임 재출동 해금)
    # =================================================================
    entry_p = float(last_buy_price if last_buy_price > 0.0 else (r_state.get("last_known_avg", 0.0) or 0.0))
    if exit_price <= 0.0:
        exit_price = float(
            current_price
            if current_price > 0.0
            else (target_sell_price if target_sell_price > 0.0 else entry_p)
        )
    if filled_qty <= 0 and entry_p > 0.0:
        filled_qty = (
            1
            if entry_session == "WHIPSAW_1SH"
            else max(1, int(math.floor(budget / entry_p)))
        )

    trail_st = _TRAIL_RUNTIME_CACHE.get(symbol) or {}
    peak_p = max(float(trail_st.get("peak_price", 0.0) or 0.0), exit_price, entry_p)
    was_trail_armed = bool(trail_st.get("armed", False))
    exit_regime_used = str(trail_st.get("exit_regime") or _resolve_exit_regime_for_symbol(symbol, entry_session))

    is_comp = await AssassinLedger.get_is_compound(symbol)
    new_budget, gross_pnl_usd, comm_usd, net_delta_usd = calc_compound_next_budget(
        current_budget=budget,
        is_compound=is_comp,
        entry_price=entry_p,
        exit_price=exit_price,
        filled_qty=filled_qty,
    )

    if is_comp and new_budget > 0.0:
        await AssassinLedger.sync_global_settings(budget=new_budget)

    _reset_trailing_runtime_state(symbol)
    _reset_manual_stage3_anchor(symbol)
    r_state["last_known_qty"] = 0
    r_state["last_known_avg"] = 0.0

    _, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
    nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
    is_macro_overheated = bool(nq_amp_now >= 2.0)

    is_bottom_tracking = bool(
        entry_session == "STAGE3_STOP_TRACK"
        or (_BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol)
    )
    in_golden_window = bool(400 <= est_hm <= 814)

    if is_bottom_tracking and in_golden_window and not is_macro_overheated:
        next_es = "STAGE3_STOP_TRACK"
        next_done = False
    elif is_macro_overheated:
        next_es = "CANCELED_MACRO"
        next_done = True
    elif in_golden_window:
        next_es = ""
        next_done = False
    else:
        next_es = "VWAP_ESCAPE" if entry_session == "VWAP_ESCAPE" else ("CANCELED_MACRO" if entry_session == "CANCELED_MACRO" else "")
        next_done = True

    await AssassinLedger.save_state(
        symbol,
        price=0.0,
        budget=new_budget,
        target_sell_price=0.0,
        is_session_done=next_done,
        buy_order_id="",
        sell_order_id="",
        entry_session=next_es,
        entry_trend="",
        entry_time=0.0,
        nq_entry_price=0.0,
        nq_entry_amp=0.0,
        whipsaw_target_price=0.0,
    )

    if in_golden_window and not is_macro_overheated:
        await AssassinLedger.unlock_all_symbols_for_reentry()
    elif is_macro_overheated:
        await AssassinLedger.lock_all_symbols_macro()

    ctx.wakeup_event.set()

    if entry_p > 0.0 and exit_price > 0.0:
        gross_pct = (exit_price / entry_p - 1.0) * 100.0
        net_pct = gross_pct - (ROUND_TRIP_COMMISSION_RATE * 100.0)
        peak_pct = ((peak_p / entry_p - 1.0) * 100.0) if peak_p > 0.0 else gross_pct
        pnl_sign = "+" if net_delta_usd >= 0 else "-"
        gross_sign = "+" if gross_pnl_usd >= 0 else "-"
        mode_str = "📈 통합복리" if is_comp else "💵 단리고정"

        if entry_session == "WHIPSAW_1SH":
            exit_title = f"🌪️ [듀얼 휩소 1주 작전 청산 정산] {sym_icon}{symbol}"
        elif entry_session == "VWAP_ESCAPE":
            exit_title = f"🛟 [장마감 1시간 전 VWAP 탈출 정산] {sym_icon}{symbol}"
        elif is_bottom_tracking:
            exit_title = f"🛡️ [3차 역행 손절 정산 ➡️ 최저점 역트레일링 감시] {sym_icon}{symbol}"
        elif net_delta_usd >= 0.0 or was_trail_armed:
            reg_badge = "🚀진성·10분꺾임래칫" if exit_regime_used == "PEAK_LCK_130" else "⚡기술적반등스윕"
            exit_title = f"💰 [듀얼 스나이퍼 익절 정산 완료 · {reg_badge}] {sym_icon}{symbol}"
        else:
            exit_title = f"🛑 [듀얼 스나이퍼 손절 정산 완료] {sym_icon}{symbol}"

        if is_bottom_tracking and in_golden_window and not is_macro_overheated:
            rearm_footer = (
                f"🪤 최저점 역트레일링 가동 중: 손절가(<code>${exit_price:.2f}</code>) 하회 최저점 대비 "
                "<code>+0.30%</code> 양봉 반등 시 즉시 재진입!"
            )
        elif in_golden_window and not is_macro_overheated:
            rearm_footer = "🔫 04:07 ~ 08:14 EST 골든타임 무한 재출동 준비 완료 (👑HYB_35_1ST × 🚀08_HYB_PIVBRK 감시 복귀)"
        elif is_macro_overheated:
            rearm_footer = f"🛑 당일 NQ=F 진폭(<code>{nq_amp_now:.2f}%</code>) 과열로 본진 자동 진입 잠금(CANCELED_MACRO)"
        else:
            rearm_footer = "🔒 금일 신규 진입 마감 (대기 모드 전환)"

        await ctx.notify_tg(
            f"<b>{exit_title}</b>\n"
            f"▫️ 체결 궤적: 평단 <code>${entry_p:.2f}</code> ➡️ 경유고점 <code>${peak_p:.2f} ({peak_pct:+.2f}%)</code> ➡️ 청산 <code>${exit_price:.2f}</code> (<code>{filled_qty}주</code>)\n"
            f"▫️ 실현 손익: 세전 <code>{gross_pct:+.2f}% ({gross_sign}${abs(gross_pnl_usd):,.2f})</code> | 수수료 <code>-${comm_usd:.2f}</code> | <b>순수익 <code>{net_pct:+.2f}% ({pnl_sign}${abs(net_delta_usd):,.2f})</code></b>\n"
            f"▫️ 차기 시드({mode_str}): <code>${budget:,.2f}</code> ➡️ <b><code>${new_budget:,.2f}</code></b>\n"
            f"▫️ 후속 상태: {rearm_footer}"
        )
    return True


# 초과 Case 50, 58, 64, 85, 89, 91, 92, 94, 95, 99, 101 —
# 상단 안전 캡(+5.00% LIMIT / 1주 휩소는 +1.00% LIMIT) 선장전 및 이원화 하이킨 아시 트레일링 0.1초 즉시 정정 스윕 엔진
async def manage_mechanical_profit_trap_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int,
    avg_price: float,
    current_price: float,
    buy_order_id: str,
    sell_order_id: str,
    target_profit_rate: float,
    target_sell_price: float,
    entry_session: str,
) -> tuple[str, float]:
    if symbol not in ("SOXS", "SOXL") or holdings_qty <= 0 or avg_price <= 0.0:
        return sell_order_id, target_sell_price

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    est_hm = now_est.hour * 100 + now_est.minute

    # =================================================================
    # [A] 스나이퍼 가동 OFF 시: 기존 매도 주문 취소 및 QQQ 3차 기준가 0.00% 초기화 (초과 Case 42, 94)
    # =================================================================
    if not is_active:
        if sell_order_id and not ctx.in_memory_ordering_lock.get(symbol, False):
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                try:
                    await ctx.client.cancel_order(sell_order_id)
                except Exception:
                    pass
                _reset_trailing_runtime_state(symbol)
                _reset_manual_stage3_anchor(symbol)
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id="",
                    target_sell_price=0.0,
                    nq_entry_price=0.0,
                    nq_entry_amp=0.0,
                )
                return "", 0.0
            finally:
                ctx.in_memory_ordering_lock[symbol] = False
        return sell_order_id, target_sell_price

    # =================================================================
    # [B] 미체결 매수 주문(buy_order_id) 잔존 여부 확인 (초과 Case 64: 분할 체결 증발 방어)
    # =================================================================
    if buy_order_id:
        try:
            b_od = await ctx.client.get_order_detail(buy_order_id)
            b_st = str(b_od.get("status") or "").upper()
            if b_st in ("PENDING", "PARTIAL_FILLED", "PENDING_REPLACE"):
                return sell_order_id, target_sell_price
            await AssassinLedger.save_state(symbol, buy_order_id="")
            buy_order_id = ""
        except Exception:
            return sell_order_id, target_sell_price

    is_whipsaw_pos = bool(entry_session == "WHIPSAW_1SH")
    is_auto_pos = bool(entry_session in _AUTO_ENTRY_SESSION_TAGS)
    is_manual_pos = bool(not is_auto_pos)

    if is_whipsaw_pos:
        cap_limit_price = math.ceil(avg_price * WHIPSAW_SELL_PROFIT_FACTOR * 100.0) / 100.0
        arm_price = cap_limit_price
        assigned_regime = "WHIPSAW_1SH"
    else:
        cap_limit_price = math.ceil(avg_price * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0
        arm_price = math.ceil(avg_price * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        assigned_regime = _resolve_exit_regime_for_symbol(symbol, entry_session)

    # =================================================================
    # [C] 1단계: 상단 안전 캡(+5.00% LIMIT / 휩소 1주는 +1.00% LIMIT) 매도 주문 선장전
    # =================================================================
    if (not sell_order_id or target_sell_price <= 0.0) and not ctx.in_memory_ordering_lock.get(symbol, False):
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            existing_sell_id = ""
            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                for o in open_orders:
                    if str(o.get("side") or "").upper() == "SELL" and o.get("orderId"):
                        existing_sell_id = str(o["orderId"])
                        break
            except Exception:
                pass

            ref_p_now, ref_src_now = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
            _, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
            nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)

            st_cur = await AssassinLedger.get_state(symbol)
            cur_nq_p = float(st_cur[10] or 0.0)
            cur_nq_a = float(st_cur[11] or 0.0)

            # 수동 진입이거나 기준가가 비어 있으면 현재 실시간 QQQ 가격으로 즉시 0.00% 동기화 락온 (Case 92, 94)
            if is_manual_pos or cur_nq_p <= 0.0 or ((cur_nq_p > 3000.0) != (ref_p_now > 3000.0 and ref_p_now > 0.0)):
                cur_nq_p = ref_p_now
                cur_nq_a = nq_amp_now if nq_amp_now > 0.0 else 0.8
                if is_manual_pos:
                    mark_manual_stage3_anchored(symbol, ref_price=cur_nq_p, ref_amp=cur_nq_a)

            eff_entry_sess = "MANUAL" if is_manual_pos else entry_session
            trend_str = "SHORT" if symbol == "SOXS" else "LONG"

            if existing_sell_id:
                new_sell_id = existing_sell_id
            else:
                idem = ctx.idempotency_keys[symbol]["SELL"]
                if not idem or idem.get("qty") != holdings_qty or abs(float(idem.get("price", 0.0)) - cap_limit_price) >= 0.01:
                    idem = {
                        "key": f"TRAP_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
                        "qty": holdings_qty,
                        "price": cap_limit_price,
                    }
                    ctx.idempotency_keys[symbol]["SELL"] = idem

                res = await ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=holdings_qty,
                    price=f"{cap_limit_price:.2f}",
                    client_order_id=idem["key"],
                )
                new_sell_id = str((res or {}).get("result", {}).get("orderId") or "")
                ctx.idempotency_keys[symbol]["SELL"] = None

            if new_sell_id:
                _TRAIL_RUNTIME_CACHE[symbol]["exit_regime"] = assigned_regime
                _TRAIL_RUNTIME_CACHE[symbol]["floor_price"] = arm_price
                await AssassinLedger.save_state(
                    symbol,
                    price=avg_price,
                    target_sell_price=arm_price,
                    sell_order_id=new_sell_id,
                    buy_order_id="",
                    entry_session=eff_entry_sess,
                    entry_trend=trend_str,
                    nq_entry_price=cur_nq_p,
                    nq_entry_amp=cur_nq_a,
                )
                ctx.wakeup_event.set()
                ref_unit = "$" if ref_src_now == "QQQ실시간" else ""
                dyn_th_v = max(cur_nq_a * 0.5, 0.80)

                if is_whipsaw_pos:
                    await ctx.notify_tg(
                        f"🌪️ <b>[듀얼 휩소 1주 체결 완료 ➡️ +1.00% 목표 매도 덫 장전] {sym_icon}{symbol}</b>\n"
                        f"▫️ 체결 평단가: <code>${avg_price:.2f}</code> (<code>{holdings_qty}주</code>) ➡️ 목표 매도가(LIMIT): <b><code>${cap_limit_price:.2f}</code> (+1.00%)</b>\n"
                        f"▫️ 3차 방어 기준({ref_src_now}): <code>{ref_unit}{cur_nq_p:.2f}</code> (한도 <code>+{dyn_th_v:.2f}%</code>) | 04:07 EST 정각 타임쉴드 적용"
                    )
                else:
                    if eff_entry_sess in ("PRE_PIVOT_10M", "PRE_PIVOT_10M_REARM"):
                        pos_tag_str = "🎯 10분봉 대파동 돌파 꺾임목"
                    elif eff_entry_sess in ("PRE_PULLBACK", "PRE_PULLBACK_REARM"):
                        pos_tag_str = "🚀 07:00~ 진성 추세눌림목"
                    elif eff_entry_sess == "MANUAL":
                        pos_tag_str = "🖐️ 수동 스나이퍼 개입"
                    else:
                        pos_tag_str = "🪤 양방향 대칭 클라이맥스"

                    reg_desc = (
                        "🚀 진성·10분꺾임 매도 (+1.30%전 양봉홀딩·쿠션+0.75% ➔ 파란음봉/+1.30%후 +1.00%잠금 &amp; -0.20% 타이트스탑)"
                        if assigned_regime == "PEAK_LCK_130"
                        else "⚡ 기술적반등 매도 (+1.00% 보존 고정 &amp; 고점 대비 -0.30% 콜백/HA음봉 즉시 스윕)"
                    )
                    await ctx.notify_tg(
                        f"🎯 <b>[상단 안전 캡 선장전 &amp; 이원화 트레일링 결속] {sym_icon}{symbol} ({pos_tag_str})</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (<code>{holdings_qty}주</code>) | 상단 안전 캡(LIMIT): <code>${cap_limit_price:.2f} (+5.00%)</code>\n"
                        f"▫️ 트레일링 점화가(+1.00%): <b><code>${arm_price:.2f}</code></b> | 매도 레짐: <code>{reg_desc}</code>\n"
                        f"▫️ 3차 역행 방어({ref_src_now}): 기준가 <code>{ref_unit}{cur_nq_p:.2f}</code> (역행 한도 <code>+{dyn_th_v:.2f}%</code> · 수동/자동 보호 가동)"
                    )
                return new_sell_id, arm_price
        except Exception as e:
            print(f"🚨 [안전 캡 매도 주문 선장전 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 1주 휩소 작전 포지션은 이미 거래소에 +1.00% LIMIT 주문이 나가 있으므로 트레일링 스윕 생략
    if is_whipsaw_pos:
        return sell_order_id, target_sell_price

    # =================================================================
    # [D] 2단계 & 3단계: +1.00% 돌파 트레일링 점화 및 이원화(OLD_IMM_CB vs PEAK_LCK_130) 0.1초 정정 스윕
    # =================================================================
    live_p = float(current_price or 0.0)
    if live_p <= 0.0:
        return sell_order_id, target_sell_price

    trail_st = _TRAIL_RUNTIME_CACHE[symbol]
    trail_st["exit_regime"] = assigned_regime

    sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol) or {}
    ewm_st = sym_rsi_dict.get("ewm_state") or {}
    cur_bar_open = float(ewm_st.get("cur_bar_open", 0.0) or live_p)
    cur_bar_high = float(ewm_st.get("cur_bar_high", 0.0) or live_p)
    cur_bar_high_fl = math.floor(max(live_p, cur_bar_high) * 100.0) / 100.0

    # 1) +1.00% 도달 시 트레일링 점화
    just_armed = False
    if not bool(trail_st.get("armed", False)) and live_p >= arm_price:
        trail_st["armed"] = True
        trail_st["peak_price"] = max(arm_price, live_p, cur_bar_high_fl)
        trail_st["floor_price"] = arm_price
        trail_st["armed_time"] = time.time()
        trail_st["armed_hm"] = est_hm
        trail_st["bear_candle_seen"] = False
        trail_st["exit_regime"] = assigned_regime
        just_armed = True
        ctx.wakeup_event.set()

    if not bool(trail_st.get("armed", False)):
        return sell_order_id, target_sell_price

    # 2) 경유 최고가(peak_price) 갱신 및 실시간 하이킨 아시 / 단기 변화율 바이패스 계산
    prev_peak = float(trail_st.get("peak_price", arm_price))
    peak_price = max(prev_peak, live_p, cur_bar_high_fl)
    trail_st["peak_price"] = peak_price
    peak_pct = ((peak_price / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 1.00

    is_ha_bull, live_ha_open, live_ha_close = compute_realtime_tick_heikin_ashi(live_p, ewm_st)
    is_candle_bull = bool(live_p >= cur_bar_open)

    vel_10m, vel_3m = await MacroDataCache.get_nq_short_velocity()
    if symbol == "SOXS":
        nq_vel_bypass = bool(min(vel_10m, vel_3m) <= -NQ_VELOCITY_SURGE_PCT)
    else:
        nq_vel_bypass = bool(max(vel_10m, vel_3m) >= NQ_VELOCITY_SURGE_PCT)
    trail_st["nq_vel_bypass"] = nq_vel_bypass

    elapsed_arm = time.time() - float(trail_st.get("armed_time", 0.0))
    armed_hm = int(trail_st.get("armed_hm", -1))
    # 점화된 당해 1분봉 초입에서 호가 1~2센트 노이즈로 점화 즉시 팔리는 현상을 방지하는 8초/1.15% 가드
    callback_ready = bool((est_hm != armed_hm) or (elapsed_arm >= 8.0) or (peak_pct >= 1.15))

    # 3) 진입 성격별 이원화 매도 판정 (초과 Case 99 & Case 101)
    if assigned_regime == "PEAK_LCK_130":
        cushion_floor = math.ceil(avg_price * (1.0 + TREND_PB_CUSHION_FLOOR_RATE / 100.0) * 100.0) / 100.0
        if (not is_candle_bull or not is_ha_bull) and elapsed_arm >= 3.0 and not just_armed:
            trail_st["bear_candle_seen"] = True

        # 경유 최고가가 +1.30% 미만인 초입 구간에서 빨간 양봉(C>=O) 및 HA양봉이 유지되고 파란 음봉 미출현 시 밑꼬리 스탑 유예!
        if (
            peak_pct < TREND_PB_LAUNCH_RATCHET_PCT
            and is_candle_bull
            and is_ha_bull
            and not bool(trail_st.get("bear_candle_seen", False))
        ):
            floor_price = cushion_floor
            effective_stop_p = cushion_floor
            is_cb_hit = bool((not just_armed) and (live_p <= cushion_floor))
            is_ha_bear_reversal = False
        else:
            floor_price = arm_price
            raw_cb = math.floor(peak_price * (1.0 - TREND_PB_CALLBACK_PCT / 100.0) * 100.0) / 100.0
            effective_stop_p = max(floor_price, raw_cb)
            is_cb_hit = bool(callback_ready and (live_p <= effective_stop_p))
            is_ha_bear_reversal = bool(callback_ready and (not is_ha_bull) and (not nq_vel_bypass))
    else:
        floor_price = arm_price
        raw_cb = math.floor(peak_price * (1.0 - TRAIL_DROP_CALLBACK_PCT / 100.0) * 100.0) / 100.0
        effective_stop_p = max(floor_price, raw_cb)
        is_cb_hit = bool(callback_ready and (live_p <= effective_stop_p))
        is_ha_bear_reversal = bool(callback_ready and (not is_ha_bull) and (not nq_vel_bypass))

    trail_st["floor_price"] = floor_price
    trail_st["stop_price"] = effective_stop_p

    if just_armed:
        reg_title = (
            "🚀 [진성추세·10분봉꺾임 트레일링 점화!] +1.30%전 양봉홀딩 ➔ -0.20% 타이트스탑"
            if assigned_regime == "PEAK_LCK_130"
            else "⚡ [기술적반등 트레일링 점화!] +1.00% 보존 고정 &amp; -0.30% 콜백/HA음봉 스윕"
        )
        await ctx.notify_tg(
            f"🔥 <b>{reg_title} ({sym_icon}{symbol})</b>\n"
            f"▫️ 평단가: <code>${avg_price:.2f}</code> ➡️ 점화 현재가: <code>${live_p:.2f} (+{peak_pct:.2f}%)</code>\n"
            f"▫️ 현재 스탑선: <code>${effective_stop_p:.2f}</code> | 상단 안전 캡: <code>${cap_limit_price:.2f}</code>\n"
            "▫️ 조치: <code>0.4초 고주파 부스터 고정 및 최고가 무제한 추적 개시!</code>"
        )
        return sell_order_id, target_sell_price

    if not (is_cb_hit or is_ha_bear_reversal):
        return sell_order_id, target_sell_price

    now_ts = time.time()
    if (
        ctx.in_memory_ordering_lock.get(symbol, False)
        or (now_ts - float(trail_st.get("last_sweep_ts", 0.0))) < 1.2
    ):
        return sell_order_id, target_sell_price

    # =================================================================
    # [E] 콜백 스탑 또는 HA음봉 전환 감지 -> 0.1초 즉시 가격 정정(modify_order) 익절 스윕!
    # =================================================================
    ctx.in_memory_ordering_lock[symbol] = True
    try:
        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        sweep_price = math.floor(latest_p * 100.0) / 100.0

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0].get("price") or 0.0) > 0.0:
                bid_1 = float(bids[0]["price"])
                sweep_price = min(sweep_price, bid_1) if sweep_price > 0.0 else bid_1
        except Exception:
            pass

        # 보존선 단가 방어: 현재가가 +1.00% 점화가 부근(>= arm_price * 0.997)이면 스프레드로 인한 헐값 투매 방지
        if latest_p >= arm_price * 0.997:
            sweep_price = max(floor_price, sweep_price)
        else:
            sweep_price = max(0.01, sweep_price)

        sweep_done = False
        new_sell_id = sell_order_id

        # 1순위: 기존 상단 안전 캡 매도 주문(sell_order_id)을 0.1초 modify_order로 즉시 하향 정정 스윕
        if sell_order_id:
            try:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=sell_order_id,
                        price=f"{sweep_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=3.5,
                )
                new_sell_id = str((mod_res or {}).get("result", {}).get("orderId") or sell_order_id)
                sweep_done = True
            except Exception as mod_err:
                err_l = str(mod_err).lower()
                if "already-filled" in err_l:
                    return sell_order_id, sweep_price
                print(f"🚨 [트레일링 modify_order 폴백 전환 {symbol}] {mod_err}", flush=True)

        # 2순위 폴백: 정정 실패 시 기존 매도 선취소 + 수량 해방 대기 후 신규 LIMIT 스윕 발사
        if not sweep_done:
            sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
                ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=False
            )
            if sellable_q_int > 0:
                sw_qty = min(int(holdings_qty), sellable_q_int)
                idem_key = f"TSWP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
                res_s = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=sw_qty,
                        price=f"{sweep_price:.2f}",
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                if res_s and isinstance(res_s, dict) and res_s.get("result", {}).get("orderId"):
                    new_sell_id = str(res_s["result"]["orderId"])
                    sweep_done = True
            elif surviving_sell_ids:
                mod_res2 = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=surviving_sell_ids[0],
                        price=f"{sweep_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=3.5,
                )
                new_sell_id = str((mod_res2 or {}).get("result", {}).get("orderId") or surviving_sell_ids[0])
                sweep_done = True

        if sweep_done:
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=new_sell_id,
                target_sell_price=sweep_price,
            )
            ctx.wakeup_event.set()
            est_profit_pct = ((sweep_price / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 0.0
            trig_reason_str = (
                f"고점(<code>${peak_price:.2f}</code>) 대비 콜백 스탑(<code>${effective_stop_p:.2f}</code>) 도달"
                if is_cb_hit
                else f"하이킨 아시 1분봉 음봉 전환(HA_C <code>{live_ha_close:.3f}</code> ≤ HA_O <code>{live_ha_open:.3f}</code>)"
            )
            reg_lbl = "🚀진성·10분꺾임(-0.20%)" if assigned_regime == "PEAK_LCK_130" else "⚡기술적반등(-0.30%)"
            await ctx.notify_tg(
                f"⚡ <b>[이원화 트레일링 0.1초 익절 스윕 발사!] {sym_icon}{symbol} ({reg_lbl})</b>\n"
                f"▫️ 격발 사유: {trig_reason_str}\n"
                f"▫️ 스윕 단가(LIMIT): <code>${sweep_price:.2f}</code> (평단 <code>${avg_price:.2f}</code> 대비 <code>+{est_profit_pct:.2f}%</code> · 경유고점 <code>${peak_price:.2f} [+{peak_pct:.2f}%]</code>)"
            )
            return new_sell_id, sweep_price
    except Exception as e:
        print(f"🚨 [이원화 트레일링 익절 스윕 방어 {symbol}] {e}", flush=True)
    finally:
        trail_st["last_sweep_ts"] = time.time()
        ctx.in_memory_ordering_lock[symbol] = False

    return sell_order_id, target_sell_price
