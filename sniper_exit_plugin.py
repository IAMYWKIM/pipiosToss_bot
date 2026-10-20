# =====================================================================
# FILE: sniper_exit_plugin.py
# 목적: 듀얼 스나이퍼(SOXS/SOXL) 통합 실수익 복리 정산 및 진입 성격별 이원화 매도 믹스 코어
#       (sniper_plugin.py 1/2 분할본)
#       — 매수 주문 FILLED 직후 잔고 인덱싱 시차 중 자동 진입 물량의 MANUAL 오분류 원천 차단(초과 Case 100),
#         잔고 0주 복귀 시 통합 실수익 복리 정산 및 04:00 ~ 08:14 EST 무한 재출동 릴리스(reconcile_zero_holdings_exit_plugin),
#         매수 체결 즉시 상단 안전 캡(+5.00% LIMIT, 1주 휩소는 +1.00% LIMIT) 선장전 및 실시간 QQQ 3차 기준가 동기화,
#         진입 성격별 '이원화 매도 믹스(기술적 반등 ⚡ OLD_IMM_CB × 진성 상승종목 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 × 🚀 NQ_VEL_BYPASS)'
#         0.1초 즉시 정정 스윕(manage_mechanical_profit_trap_plugin)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 sniper_plugin.py의 청산·복리정산·이원화 트레일링 스윕 코어를 sniper_exit_plugin.py로 무손실 분리
# MODIFIED: 초과 Case 89 & Case 91 — 트레일링 점화(+1.00%) 후 고점 추적 중 토스 Open API QQQ 0.0초 실시간(폴백 NQ=F)
#           10분/3분 단기 변화율이 유리 방향으로 ±0.20% 이상 급등·급락 지속 시 실시간 1분봉 하이킨 아시 음봉 전환을 바이패스하는 🚀 NQ_VEL_BYPASS 탑재
# NEW: 초과 Case 94 — 스나이퍼 가동 OFF 전환 시 nq_entry_price=0.0, nq_entry_amp=0.0 원자적 초기화 및
#      수동 보유 물량 안전캡(+5.00% LIMIT) 결속 시 현재 실시간 QQQ 기준가(0.00% 역행 출발) 강제 동기화·마킹(mark_manual_stage3_anchored)
# MODIFIED: 초과 Case 58, 79, 95 & 96 — 03:58 ~ 04:07 EST 1주 휩소 작전(WHIPSAW_1SH)이 체결되면
#           실제 매수 체결 평단가(avg_price) 대비 +1.00% 익절가에 1주 지정가 매도 덫을 장전하며,
#           과열차단(NQ>=2.0%)일에 청산 완료 시 즉시 양쪽 장부를 CANCELED_MACRO(is_session_done=True)로 복원
# NEW: 초과 Case 98 — 정규장 마감 1시간 전(15:00 ~ 15:58 EST) 미익절 물량 VWAP 도달 탈출(VWAP_ESCAPE) 정산 연동
# NEW: 초과 Case 99 — 기술적 반등(PRE_SNIPER : ⚡ OLD_IMM_CB [-0.30% 콜백]) × 진성 상승종목 추세추종(PRE_PULLBACK : 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 [초입 +1.30% 전 양봉 홀딩 후 -0.20% 타이트 스탑]) 이원화 매도 믹스 탑재
# NEW: 초과 Case 100 — 매수 주문 FILLED(또는 filledQuantity > 0) 직후 토스 잔고 API 인덱싱 시차(0.1~0.4초) 동안
#      reconcile_zero_holdings_exit_plugin이 장부를 매수 취소로 오판해 초기화함으로써 자동 진입 물량이 MANUAL(수동 진입 · 2차 방어 패스)로 둔갑하는 경합 버그 원천 소각

import math
import time
import asyncio
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
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
    BOTTOM_REBOUND_PCT,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _AUTO_ENTRY_SESSION_TAGS,
    get_pre_leader_status,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    _activate_bottom_track_state,
    get_bottom_track_status,
    _cancel_open_orders_and_wait_sellable,
)

EST_TZ = ZoneInfo("America/New_York")


# 하이킨 아시 트레일링 익절(+1.00% ~ +5.00%+), 15:00~15:58 VWAP 탈출(VWAP_ESCAPE), 3차 손절 후 최저점 역트레일링 전환(STAGE3_STOP_TRACK) 및 모든 청산 시
# 실제 발생 순수익금((실제매도가 - 실제매수가) * 체결수량 - 왕복수수료 0.14%) 복리 정산 및
# 프리장 골든타임(04:00 ~ 08:14 EST) 내 조건 충족 시 양쪽 장부 동시 재출동 락 해제 (Case 52, 76, 80, 84, 87, 94, 95, 97, 98, 99, 100)
async def reconcile_zero_holdings_exit_plugin(
    ctx,
    symbol: str,
    last_buy_price: float,
    buy_order_id: str,
    sell_order_id: str,
    is_active: bool = True,
    fallback_qty: float = 0.0,
) -> bool:
    if ctx.in_memory_ordering_lock[symbol]:
        return False
    can_clear = True
    if buy_order_id:
        try:
            od = await ctx.client.get_order_detail(buy_order_id)
            st = str(od.get("status") or "")
            exec_info_b = od.get("execution") or {}
            filled_q_b = float(exec_info_b.get("filledQuantity") or 0.0)

            # MODIFIED: 초과 Case 100 — 매수 주문이 PENDING / PARTIAL_FILLED / REPLACED 상태일 뿐만 아니라,
            # 방금 전량 체결(status == "FILLED" 또는 filledQuantity > 0)되었는데 아직 토스 잔고 API 인덱싱 시차(0.1~0.4초)로 인해
            # holdings_qty == 0 으로 조회되고 안전 캡 매도 주문(sell_order_id) 및 평단가(last_buy_price)가 결속되기 전이라면,
            # 절대 장부를 매수 취소로 오판해 지우지 않고 보호(can_clear = False)하여 MANUAL(수동 진입) 둔갑 사고를 100% 원천 차단!
            if st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE", "REPLACED"]:
                can_clear = False
            elif (st == "FILLED" or filled_q_b > 0.0) and not sell_order_id and last_buy_price <= 0.0:
                can_clear = False
                ctx.wakeup_event.set()
        except Exception:
            can_clear = False

    if not can_clear:
        return False

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        current_state = await AssassinLedger.get_state(symbol)
        current_budget = float(current_state[1] or 100.0)
        active_target_rate = AssassinLedger.DEFAULT_TARGET_PROFIT_RATE
        prev_target_sell_price = float(current_state[6] or 0.0)
        prev_entry_session = str(current_state[8] or "")
        is_compound = await AssassinLedger.get_is_compound(symbol)
        whipsaw_tp_saved = await AssassinLedger.get_whipsaw_target_price(symbol)
        trail_snapshot = get_trailing_runtime_status(symbol)
        was_trail_armed = bool(trail_snapshot.get("armed", False))
        peak_reached_p = float(trail_snapshot.get("peak_price", 0.0))
        exit_regime_used = str(trail_snapshot.get("exit_regime", "OLD_IMM_CB"))
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"

        now_est_check = datetime.now(EST_TZ)
        est_hm_now = now_est_check.hour * 100 + now_est_check.minute
        is_premarket_now = (400 <= est_hm_now <= 814)
        is_moc_time = (now_est_check.hour == 15 and now_est_check.minute >= 59) or (
            now_est_check.hour == 16 and 0 <= now_est_check.minute <= 5
        )
        is_vwap_escape_exit = (prev_entry_session == "VWAP_ESCAPE")
        is_take_profit_exit, is_trap_survived = False, False
        exit_price, filled_qty = 0.0, 0.0

        # 1) sell_order_id 상세 원장 우선 프로빙
        if sell_order_id:
            try:
                od = await ctx.client.get_order_detail(sell_order_id)
                st = od.get("status", "")
                exec_info_trap = od.get("execution") or {}
                trap_avg_p = float(exec_info_trap.get("averageFilledPrice") or 0.0)
                trap_f_qty = float(exec_info_trap.get("filledQuantity") or 0.0)
                if trap_avg_p > 0.0 and trap_f_qty > 0.0:
                    exit_price, filled_qty = trap_avg_p, trap_f_qty

                if st == "FILLED" and not is_vwap_escape_exit:
                    is_take_profit_exit = True
                elif st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                    is_trap_survived = True
                    await ctx.client.cancel_order(sell_order_id)
                    await asyncio.sleep(0.4)
            except Exception:
                pass

        # 2) CLOSED 주문 목록에서 최신 SELL 체결 단가 및 수량 추출
        was_holding = last_buy_price > 0.0
        for attempt_closed in range(2):
            if exit_price > 0.0 and filled_qty > 0.0:
                break
            if attempt_closed == 1 and was_holding:
                await asyncio.sleep(0.4)
            try:
                orders = await ctx.client.get_orders(status="CLOSED", symbol=symbol)
                for o in orders:
                    if o.get("side") == "SELL" and o.get("status") in ["FILLED", "PARTIAL_FILLED"]:
                        exec_info = o.get("execution") or {}
                        avg_p = float(exec_info.get("averageFilledPrice") or 0.0)
                        f_qty = float(exec_info.get("filledQuantity") or 0.0)
                        if avg_p > 0.0 and f_qty > 0.0:
                            exit_price, filled_qty = avg_p, f_qty
                            if (
                                not is_take_profit_exit
                                and not is_vwap_escape_exit
                                and prev_target_sell_price > 0.0
                                and avg_p >= (prev_target_sell_price - 0.01)
                            ):
                                is_take_profit_exit = True
                            break
            except Exception:
                pass

        entry_price = last_buy_price

        if filled_qty <= 0.0 and was_holding:
            if prev_entry_session == "WHIPSAW_1SH":
                filled_qty = 1.0
            elif fallback_qty > 0.0:
                filled_qty = float(fallback_qty)

        if exit_price <= 0.0 and was_holding:
            if (is_take_profit_exit or is_vwap_escape_exit) and prev_target_sell_price > 0.0:
                exit_price = prev_target_sell_price
            else:
                try:
                    exit_price = await ctx.client.get_current_price(symbol)
                except Exception:
                    exit_price = 0.0

        is_whipsaw_0407_exit = (
            prev_entry_session == "WHIPSAW_1SH"
            and whipsaw_tp_saved <= 0.0
            and est_hm_now >= 407
        )
        is_stage3_stop_track_exit = (prev_entry_session == "STAGE3_STOP_TRACK")

        # 하이킨 아시 트레일링 점화(was_trail_armed) 후 익절 또는 수익권(exit_price > entry_price) 청산은 모두 정상 익절로 판정
        if (
            was_holding
            and entry_price > 0.0
            and exit_price > entry_price
            and not is_moc_time
            and not is_vwap_escape_exit
            and prev_entry_session not in ("CANCELED_MACRO", "STAGE3_STOP_TRACK")
        ):
            is_take_profit_exit = True
        elif (
            was_trail_armed
            and not is_moc_time
            and not is_vwap_escape_exit
            and prev_entry_session not in ("CANCELED_MACRO", "STAGE3_STOP_TRACK")
        ):
            is_take_profit_exit = True
        elif is_stage3_stop_track_exit or is_vwap_escape_exit:
            is_take_profit_exit = False

        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            pnl_amt = (exit_price * filled_qty) - (entry_price * filled_qty)
            pnl_rate = (exit_price / entry_price - 1.0) * 100.0
            sign = "+" if pnl_amt >= 0 else ""
            peak_info_str = ""
            if was_trail_armed and peak_reached_p > 0.0 and entry_price > 0.0:
                peak_r = (peak_reached_p / entry_price - 1.0) * 100.0
                peak_info_str = f" (경유 최고가 <code>${peak_reached_p:.2f} / +{peak_r:.2f}%</code>)"
            pnl_str = (
                f"\n▫️ 타점 ({int(filled_qty)}주): 매수 <code>${entry_price:.2f}</code> ➡️ 매도 <code>${exit_price:.2f}</code>{peak_info_str}"
                f"\n▫️ 세전 손익: <code>{sign}{pnl_rate:.2f}% ({sign}${pnl_amt:.2f})</code>"
            )
        else:
            pnl_str = "\n▫️ 타점: 체결 데이터 추출 지연 (장부 참조 요망)"

        is_manual_exit = (
            was_holding
            and not is_take_profit_exit
            and not is_moc_time
            and not is_vwap_escape_exit
            and not is_trap_survived
            and not is_whipsaw_0407_exit
            and not is_stage3_stop_track_exit
        )
        is_buy_cancel_exit = (not was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived)

        # Case 76: 실제 발생한 순수익금((매도가 - 매수가) * 체결수량 - 왕복수수료 0.14%) 기반 통합 복리 시드 자동 정산
        updated_budget = None
        compound_str = ""
        if was_holding and not is_buy_cancel_exit:
            if is_compound:
                new_b, net_delta, comm_val = AssassinLedger.calc_compound_next_budget(
                    current_budget=current_budget,
                    is_take_profit=is_take_profit_exit,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    filled_qty=filled_qty,
                    target_profit_rate=active_target_rate,
                )
                updated_budget = new_b
                d_sign = "+" if net_delta >= 0.0 else ""
                if net_delta >= 0.0:
                    compound_str = (
                        f"\n▫️ 📈 <b>통합 복리 시드 실수익 증액</b>: <code>${current_budget:,.2f}</code> ➡️ "
                        f"<code>${new_b:,.2f}</code> (실순수익 <code>{d_sign}${net_delta:,.2f}</code> · 수수료 ${comm_val:,.2f} 차감)"
                    )
                else:
                    compound_str = (
                        f"\n▫️ 📉 <b>통합 복리 시드 실손실 차감</b>: <code>${current_budget:,.2f}</code> ➡️ "
                        f"<code>${new_b:,.2f}</code> (실순손실 <code>{d_sign}${net_delta:,.2f}</code> · 수수료 ${comm_val:,.2f} 포함)"
                    )
            else:
                compound_str = f"\n▫️ 💵 <b>단리 시드 고정</b>: <code>${current_budget:,.2f}</code>"

        _, nq_h_chk, nq_l_chk = await MacroDataCache.get_cached_nq_data()
        nq_amp_chk = (((nq_h_chk - nq_l_chk) / nq_l_chk * 100.0) if nq_l_chk > 0.0 else 0.0)

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        opp_holding_now = int(ctx.shared_holdings.get(opp_sym, 0)) > 0

        # MODIFIED: 초과 Case 52, 80, 84, 87, 94, 95, 97 — 프리장 골든타임 (04:00 ~ 08:14 EST) 내 익절 완료, 매수 취소, 1주 휩소 작전 정리,
        # 또는 NQ=F < 2.0% 내 3차 안전망 손절(STAGE3_STOP_TRACK) 완료 시(단, 반대편 종목을 수동 보유 중이 아닐 때) 양쪽 장부 모두 is_session_done=False로 복원!
        if (
            is_premarket_now
            and nq_amp_chk < 2.0
            and not opp_holding_now
            and prev_entry_session != "CANCELED_MACRO"
            and (
                is_take_profit_exit
                or is_buy_cancel_exit
                or prev_entry_session == "WHIPSAW_1SH"
                or is_stage3_stop_track_exit
            )
        ):
            next_is_session_done = False
            next_entry_session = "STAGE3_STOP_TRACK" if is_stage3_stop_track_exit else ""
            if is_stage3_stop_track_exit and exit_price > 0.0:
                cur_low_saved = float(_BOTTOM_TRACK_CACHE.get("post_stop_low", 0.0))
                eff_low = min(cur_low_saved, exit_price) if cur_low_saved > 0.0 else exit_price
                _activate_bottom_track_state(
                    symbol=symbol,
                    stop_price=exit_price,
                    current_price=eff_low,
                    logical_date=now_est_check.strftime("%Y-%m-%d"),
                )
        else:
            next_is_session_done = True
            next_entry_session = (
                "CANCELED_MACRO"
                if (prev_entry_session == "CANCELED_MACRO" or nq_amp_chk >= 2.0)
                else ""
            )
            if is_stage3_stop_track_exit or prev_entry_session == "CANCELED_MACRO" or nq_amp_chk >= 2.0:
                _reset_bottom_track_state()

        _reset_trailing_runtime_state(symbol)

        await AssassinLedger.save_state(
            symbol,
            price=0.0,
            budget=updated_budget,
            target_sell_price=0.0,
            buy_order_id="",
            sell_order_id="",
            is_session_done=next_is_session_done,
            entry_session=next_entry_session,
            entry_time=0.0,
            nq_entry_price=0.0,
            nq_entry_amp=0.0,
            entry_trend="",
            whipsaw_target_price=0.0,
        )

        if updated_budget is not None:
            await AssassinLedger.sync_global_settings(budget=updated_budget)

        if not next_is_session_done and not opp_holding_now:
            await AssassinLedger.unlock_all_symbols_for_reentry()
        elif nq_amp_chk >= 2.0 and prev_entry_session == "WHIPSAW_1SH":
            await AssassinLedger.lock_all_symbols_macro(force_all=False)

        if not is_active:
            print(f"🧹 [Silent Reset {symbol}] 가동 OFF 상태 청산 확인. 타전 억제 및 장부 초기화 완료.", flush=True)
            return True

        if is_buy_cancel_exit:
            print(f"🧹 [매수 취소/실패 {symbol}] 허위 익절 타전 억제 및 장부 초기화 완료.", flush=True)
        elif is_vwap_escape_exit:
            await ctx.notify_tg(
                f"🛟 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 장마감 1시간 전 VWAP 도달 손실 축소 탈출 완료</b>\n"
                f"▫️ 잔고 0주 (15:59 MOC 대량 손절 선제 회피 성공){pnl_str}{compound_str}\n"
                "▫️ 당일 신규 진입 권한 영구 소각 완료"
            )
        elif is_moc_time:
            await ctx.notify_tg(
                f"🛑 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] MOC 강제 덤핑 청산 완료</b>\n"
                f"▫️ 잔고 0주 (제로-오버나이트 락온){pnl_str}{compound_str}"
            )
        elif prev_entry_session == "WHIPSAW_1SH":
            if nq_amp_chk >= 2.0:
                whip_next_note = (
                    f"당일 NQ=F 진폭(<code>{nq_amp_chk:.2f}%</code> ≥ 2.00%) 과열로 "
                    "<b>본진 자동 진입 차단(🛑 CANCELED_MACRO) 복원 완료</b>"
                )
            else:
                whip_next_note = "04:07 ~ 08:14 EST 본진(대칭 클라이맥스 &amp; 07:00~ 추세눌림목) 무한 출동 감시 개시"

            if is_whipsaw_0407_exit:
                await ctx.notify_tg(
                    f"⏱️ <b>[듀얼 휩소 작전 04:07 정산 완료 {sym_icon}{symbol}] 1주 현재가 매도 체결</b>\n"
                    f"▫️ 잔고 0주 (04:07 타임쉴드 해제 완료){pnl_str}{compound_str}\n"
                    f"▫️ 후속 조치: {whip_next_note}"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 휩소 작전 1주 익절 성공 {sym_icon}{symbol}] +1.00% 익절 타점 달성</b>\n"
                    f"▫️ 잔고 0주 (1주 지정가 매도 덫 체결 확인){pnl_str}{compound_str}\n"
                    f"▫️ 후속 조치: {whip_next_note}"
                )
        elif is_stage3_stop_track_exit and not next_is_session_done:
            st_bot = get_bottom_track_status(symbol)
            sp_val = float(st_bot.get("last_stop_price", exit_price))
            low_val = float(st_bot.get("post_stop_low", exit_price))
            trig_val = float(st_bot.get("rebound_trig_price", 0.0))
            await ctx.notify_tg(
                f"🪤 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 3차 실시간 안전망 손절 정산 완료 ➡️ 최저점 역트레일링 반등 감시 돌입</b>\n"
                f"▫️ 잔고 0주 (손절 체결 확인){pnl_str}{compound_str}\n"
                f"▫️ 추적 기준: 손절가 <code>${sp_val:.2f}</code> | 경유 최저점 <code>${low_val:.2f}</code> ➡️ 반등 타격가(<code>+{BOTTOM_REBOUND_PCT:.2f}%</code>) <code>${trig_val:.2f}</code>\n"
                "▫️ 후속 조치: <code>신저가 경신 중 매수 보류(칼날 회피) · 손절가 아래 최저점 대비 +0.30% 양봉 반등 시 08:14 EST까지 즉시 재출격</code>"
            )
        elif is_take_profit_exit:
            regime_badge = "🚀진성추세(-0.20%)" if exit_regime_used == "PEAK_LCK_130" else "⚡기술적반등(-0.30%)"
            if not next_is_session_done:
                ld_st = get_pre_leader_status()
                ld_sym = str(ld_st.get("leader_sym") or "")
                ld_tail = f" · 상승주도 {ld_sym} RSI≤35.0" if ld_sym else ""
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 이원화 트레일링({regime_badge}) 익절 성공 ➡️ 무한 재출동 복귀</b>\n"
                    f"▫️ 잔고 0주 (트레일링 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    f"▫️ 후속 조치: 08:14 EST까지 차기 대칭 클라이맥스(≤30.0 &amp; 반대≥79.9){ld_tail} 연속 감시 재개"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 이원화 트레일링({regime_badge}) 익절 성공 (08:15 컷오프/과열 마감)</b>\n"
                    f"▫️ 잔고 0주 (트레일링 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 당일 신규 진입 권한 영구 소각"
                )
        elif is_manual_exit or is_stage3_stop_track_exit:
            await ctx.notify_tg(
                f"🛑 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 수동 매도/매크로 청산 감지 (퇴근 락온 완료)</b>\n"
                f"▫️ 잔고 0주 (청산 완료 식별){pnl_str}{compound_str}\n"
                "▫️ 당일 신규 진입 권한 영구 소각 완료"
            )
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# MODIFIED: 초과 Case 58, 79, 85, 89, 91, 92, 93, 94, 95, 97, 99, 100 —
#           진입 성격별 '이원화 매도 믹스 (Dual-Regime Mixed Exit)' :
#           1) 기술적 반등(PRE_SNIPER / PRE_SNIPER_REARM) : ⚡ OLD_IMM_CB (+1.00% 보존 고정 & 고점 대비 -0.30% 콜백/HA음봉 스윕)
#           2) 진성 상승종목 추세추종(PRE_PULLBACK / PRE_PULLBACK_REARM / 상승주도주 MANUAL) :
#              🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020 (초입 +1.30% 전 빨간 양봉 C>=O & HA양봉 홀딩[후방 쿠션 +0.75%] ➔
#              파란 음봉 C<O 출현 또는 +1.30% 돌파 시 +1.00% 보존 잠금 & 고점 대비 -0.20% 타이트 스탑)
async def manage_mechanical_profit_trap_plugin(
    ctx, symbol: str, now_est: datetime, current_session_id: str,
    is_active: bool, just_turned_off: bool, holdings_qty: int, holdings_detail: dict,
    last_buy_price: float, target_profit_rate: float, target_sell_price: float,
    buy_order_id: str, sell_order_id: str, entry_session: str, runtime_state: dict,
    current_price: float = 0.0,
):
    if holdings_qty <= 0:
        _reset_trailing_runtime_state(symbol)
    else:
        runtime_state["last_known_qty"] = holdings_qty
        if _BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
            _reset_bottom_track_state(symbol)

    open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
    has_open_sell = any(o.get("side") == "SELL" for o in open_orders)
    has_open_buy = any(o.get("side") == "BUY" for o in open_orders)

    # 초과 Case 94 ② — 스나이퍼 가동을 OFF로 끌 때는 기존 매도 주문 취소와 함께
    # 구버전 nq_entry_price=0.0, nq_entry_amp=0.0 도 원자적으로 초기화하여,
    # 추후 스나이퍼를 다시 ON으로 켰을 때 현재 실시간 QQQ 가격(0.00% 역행)부터 새로 출발하도록 강제!
    if not is_active:
        _reset_trailing_runtime_state(symbol)
        if (sell_order_id or just_turned_off or target_sell_price > 0.0) and not ctx.in_memory_ordering_lock[symbol]:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                if sell_order_id:
                    try:
                        await ctx.client.cancel_order(sell_order_id)
                    except Exception:
                        pass
            finally:
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id="",
                    target_sell_price=0.0,
                    nq_entry_price=0.0,
                    nq_entry_amp=0.0,
                )
                await asyncio.sleep(0.5)
                ctx.in_memory_ordering_lock[symbol] = False
        return

    avg_price = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    leader_sym_now = str(_PRE_LEADER_CACHE.get("leader_sym") or "")

    # 초과 Case 99 — 진입 성격별 이원화 매도 레짐 판별
    is_trend_pb_regime = bool(
        entry_session in ("PRE_PULLBACK", "PRE_PULLBACK_REARM")
        or (leader_sym_now == symbol and entry_session == "MANUAL")
    )

    # =====================================================================
    # 1. 보유 포지션 이원화 하이킨 아시 트레일링(⚡ OLD_IMM_CB vs 🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020) + QQQ실시간 급등락 바이패스 추적 및 익절 스윕
    # =====================================================================
    if (
        holdings_qty > 0
        and is_active
        and avg_price > 0.0
        and entry_session not in ("WHIPSAW_1SH", "VWAP_ESCAPE")
    ):
        live_p = current_price
        if live_p <= 0.0:
            try:
                live_p = await ctx.client.get_current_price(symbol)
            except Exception:
                live_p = 0.0

        arm_p = math.ceil(avg_price * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        floor_p = math.ceil(avg_price * (1.0 + TRAIL_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
        cushion_floor_p = math.ceil(avg_price * (1.0 + TREND_PB_CUSHION_FLOOR_RATE / 100.0) * 100.0) / 100.0
        safety_cap_p = math.ceil(avg_price * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0

        sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol, {})
        ewm_st = sym_rsi_dict.get("ewm_state", {}) if isinstance(sym_rsi_dict, dict) else {}
        cur_bar_open = float(ewm_st.get("cur_bar_open", 0.0))
        is_candle_bull = bool(cur_bar_open <= 0.0 or live_p >= cur_bar_open)
        is_ha_bull, live_ha_o, live_ha_c = compute_realtime_tick_heikin_ashi(live_p, ewm_st)

        # 초과 Case 89 & Case 91 — 토스 Open API QQQ 0.0초 실시간(폴백 NQ=F) 10분/3분 변화율 조회 및 ±0.20% 급등락 바이패스 판정
        is_nq_favorable_surge, nq_vel_10m, nq_vel_3m = await MacroDataCache.is_nq_velocity_favorable_surge(
            symbol=symbol, surge_threshold=NQ_VELOCITY_SURGE_PCT
        )
        vel_src_label = MacroDataCache.get_cached_nq_velocity_source()

        t_state = _TRAIL_RUNTIME_CACHE.setdefault(symbol, {})
        t_state["exit_regime"] = "PEAK_LCK_130" if is_trend_pb_regime else "OLD_IMM_CB"
        t_state["candle_bull"] = is_candle_bull
        t_state["avg_price"] = avg_price
        t_state["arm_price"] = arm_p
        t_state["floor_price"] = floor_p
        t_state["safety_cap_price"] = safety_cap_p
        t_state["ha_bull"] = is_ha_bull
        t_state["ha_open"] = live_ha_o
        t_state["ha_close"] = live_ha_c
        t_state["nq_vel_bypass"] = bool(is_nq_favorable_surge)
        t_state["nq_vel_10m"] = nq_vel_10m
        t_state["nq_vel_3m"] = nq_vel_3m

        if live_p > 0.0:
            cur_hm = now_est.hour * 100 + now_est.minute
            if live_p >= arm_p:
                if not t_state.get("armed", False):
                    t_state["armed"] = True
                    t_state["armed_ts"] = time.time()
                    t_state["armed_hm"] = cur_hm
                    t_state["peak_price"] = max(live_p, arm_p)
                    t_state["bear_candle_seen"] = False
                    t_state["sweep_fired"] = False
                    runtime_state["fast_shield_mode"] = True
                    ctx.wakeup_event.set()

                    cur_gain_pct = (live_p / avg_price - 1.0) * 100.0
                    if is_ha_bull:
                        ha_tag = "🕯️양봉 유지"
                    elif is_nq_favorable_surge:
                        ha_tag = f"🚀음봉이나 {vel_src_label} 급변({nq_vel_10m:+.2f}%) 바이패스 홀딩"
                    else:
                        ha_tag = "🌑음봉 주의"

                    if is_trend_pb_regime:
                        regime_title = "🚀 [진성 상승종목 추세 트레일링 점화]"
                        regime_rule_str = (
                            f"<code>고점 +{TREND_PB_LAUNCH_RATCHET_PCT:.2f}% 전 빨간양봉(C≥O &amp; HA양봉) 상승 중 밑꼬리 스탑 유예(쿠션 ${cushion_floor_p:.2f}) ➡️ "
                            f"파란음봉(C&lt;O) 출현 또는 +{TREND_PB_LAUNCH_RATCHET_PCT:.2f}% 돌파 시 보존선(${floor_p:.2f}) 잠금 &amp; 고점 -{TREND_PB_CALLBACK_PCT:.2f}% 타이트 스탑</code>"
                        )
                    else:
                        regime_title = "⚡ [기술적 반등 트레일링 점화]"
                        regime_rule_str = (
                            f"<code>최소 보존선 ${floor_p:.2f}(+1.00%) 하드 락온 · HA양봉 지속({vel_src_label} ±{NQ_VELOCITY_SURGE_PCT:.2f}% 급변 바이패스) 시 홀딩 / "
                            f"고점 대비 -{TRAIL_DROP_CALLBACK_PCT:.2f}% 콜백 또는 HA음봉 시 즉시 익절 스윕</code>"
                        )

                    await ctx.notify_tg(
                        f"<b>{regime_title} {sym_icon}{symbol} +1.00% 돌파 ➡️ 이원화 수익 추적 가동</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> ({holdings_qty}주) ➡️ 현재가: <code>${live_p:.2f}</code> (<code>+{cur_gain_pct:.2f}%</code>)\n"
                        f"▫️ 기준 보존선: <code>${floor_p:.2f}</code> (+1.00%) | 실시간 HA(1m): <code>{ha_tag}</code>\n"
                        f"▫️ 이원화 규칙: {regime_rule_str}"
                    )
                else:
                    if live_p > float(t_state.get("peak_price", 0.0)):
                        t_state["peak_price"] = live_p
                        if is_candle_bull:
                            t_state["bear_candle_seen"] = False

            if t_state.get("armed", False):
                runtime_state["fast_shield_mode"] = True
                peak_p = max(float(t_state.get("peak_price", arm_p)), arm_p)
                peak_pct = ((peak_p / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 1.00
                elapsed_arm = time.time() - float(t_state.get("armed_ts", 0.0))
                armed_hm = int(t_state.get("armed_hm", cur_hm))

                if (not is_candle_bull) and (cur_hm != armed_hm or elapsed_arm >= 4.0):
                    t_state["bear_candle_seen"] = True

                # ---------------------------------------------------------
                # [분기 1] 기술적 반등 모드 (⚡ OLD_IMM_CB : +1.00% 보존 고정 & -0.30% 콜백/HA음봉 스윕)
                # ---------------------------------------------------------
                if not is_trend_pb_regime:
                    trail_stop_p = math.floor(peak_p * (1.0 - TRAIL_DROP_CALLBACK_PCT / 100.0) * 100.0) / 100.0
                    effective_stop_p = max(floor_p, trail_stop_p)
                    t_state["stop_price"] = effective_stop_p
                    t_state["active_callback_pct"] = TRAIL_DROP_CALLBACK_PCT

                    is_callback_stop = (
                        (cur_hm != armed_hm or peak_pct >= 1.15 or elapsed_arm >= 8.0)
                        and (live_p <= effective_stop_p)
                    )
                    is_ha_bear_reversal = (
                        not is_ha_bull
                        and not is_nq_favorable_surge
                        and (cur_hm != armed_hm or elapsed_arm >= 4.0 or peak_p >= arm_p * 1.002)
                    )
                # ---------------------------------------------------------
                # [분기 2] 진성 상승종목 추세추종 모드 (🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020)
                #          초입(+1.00% ~ +1.29%) 빨간 양봉(C>=O & HA양봉) 상승 중에는 밑꼬리 스탑 유예(후방 쿠션 +0.75%),
                #          파란 음봉(C<O) 출현 또는 고점 +1.30% 돌파 시 +1.00% 보존 잠금 & 고점 대비 -0.20% 타이트 스탑!
                # ---------------------------------------------------------
                else:
                    trail_stop_20 = math.floor(peak_p * (1.0 - TREND_PB_CALLBACK_PCT / 100.0) * 100.0) / 100.0
                    t_state["active_callback_pct"] = TREND_PB_CALLBACK_PCT
                    in_launchpad_bull_hold = (
                        (peak_pct < TREND_PB_LAUNCH_RATCHET_PCT)
                        and is_candle_bull
                        and is_ha_bull
                        and (not bool(t_state.get("bear_candle_seen", False)))
                    )

                    if in_launchpad_bull_hold:
                        effective_stop_p = cushion_floor_p
                        t_state["stop_price"] = effective_stop_p
                        is_callback_stop = (elapsed_arm >= 4.0) and (live_p <= cushion_floor_p)
                        is_ha_bear_reversal = False
                    else:
                        effective_stop_p = max(floor_p, trail_stop_20)
                        t_state["stop_price"] = effective_stop_p
                        is_callback_stop = (
                            (cur_hm != armed_hm or peak_pct >= TREND_PB_LAUNCH_RATCHET_PCT or elapsed_arm >= 4.0)
                            and (live_p <= effective_stop_p)
                        )
                        is_ha_bear_reversal = (
                            not is_ha_bull
                            and not is_nq_favorable_surge
                            and (cur_hm != armed_hm or elapsed_arm >= 4.0 or peak_p >= arm_p * 1.002)
                        )

                now_ts = time.time()
                if (
                    (is_callback_stop or is_ha_bear_reversal)
                    and not ctx.in_memory_ordering_lock[symbol]
                    and (now_ts - float(t_state.get("last_sweep_ts", 0.0)) >= 1.2)
                ):
                    ctx.in_memory_ordering_lock[symbol] = True
                    t_state["last_sweep_ts"] = now_ts
                    try:
                        latest_p = await ctx.client.get_current_price(symbol)
                        if latest_p <= 0.0:
                            latest_p = live_p
                        cur_fl = math.floor(latest_p * 100.0) / 100.0

                        bid_1 = cur_fl
                        try:
                            ob = await ctx.client.get_orderbook(symbol)
                            bids = ob.get("bids", [])
                            if bids and float(bids[0]["price"]) > 0.0:
                                bid_1 = float(bids[0]["price"])
                        except Exception:
                            pass

                        min_profit_floor = math.ceil((avg_price + 0.01) * 100.0) / 100.0
                        active_floor_p = max(min_profit_floor, min(floor_p, effective_stop_p))
                        if bid_1 >= active_floor_p:
                            exec_sell_price = bid_1
                        elif cur_fl >= active_floor_p or live_p >= arm_p * 0.997:
                            exec_sell_price = max(active_floor_p, cur_fl)
                        else:
                            exec_sell_price = max(min_profit_floor, bid_1 if bid_1 > 0.0 else cur_fl)

                        exec_pct = (exec_sell_price / avg_price - 1.0) * 100.0
                        cb_pct_used = float(t_state.get("active_callback_pct", TRAIL_DROP_CALLBACK_PCT))
                        regime_tag_s = "🚀진성추세(-0.20%)" if is_trend_pb_regime else "⚡기술적반등(-0.30%)"
                        if is_callback_stop:
                            exit_reason_str = (
                                f"[{regime_tag_s}] 최고가(<code>${peak_p:.2f} / +{peak_pct:.2f}%</code>) 대비 "
                                f"-{cb_pct_used:.2f}% 트레일링 스탑선(<code>${effective_stop_p:.2f}</code>) 도달"
                            )
                        else:
                            exit_reason_str = (
                                f"[{regime_tag_s}] 실시간 하이킨 아시 1분봉 음봉 전환 "
                                f"(HA시가 <code>${live_ha_o:.2f}</code> ≥ HA종가 <code>${live_ha_c:.2f}</code> · "
                                f"고점 <code>${peak_p:.2f}</code> · {vel_src_label} 10m <code>{nq_vel_10m:+.2f}%</code>)"
                            )

                        sweep_ok = False
                        if sell_order_id:
                            try:
                                mod_res = await asyncio.wait_for(
                                    ctx.client.modify_order(
                                        order_id=sell_order_id,
                                        price=f"{exec_sell_price:.2f}",
                                        order_type="LIMIT",
                                        market_country="US",
                                    ),
                                    timeout=4.0,
                                )
                                new_s_id = str((mod_res or {}).get("result", {}).get("orderId") or sell_order_id)
                                await AssassinLedger.save_state(
                                    symbol,
                                    sell_order_id=new_s_id,
                                    target_sell_price=exec_sell_price,
                                )
                                sell_order_id = new_s_id
                                sweep_ok = True
                            except Exception as mod_e:
                                if "already-filled" in str(mod_e):
                                    sweep_ok = True
                                else:
                                    print(f"🚨 [트레일링 정정 폴백 {symbol}] modify_order 실패 -> 선취소 스윕 전환: {mod_e}", flush=True)

                        if not sweep_ok:
                            sellable_q_int, surviving_s_ids = await _cancel_open_orders_and_wait_sellable(
                                ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=False
                            )
                            if sellable_q_int > 0:
                                s_qty = min(holdings_qty, sellable_q_int)
                                idem_k = f"TRSW_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
                                res_s = await asyncio.wait_for(
                                    ctx.client.create_order(
                                        symbol=symbol,
                                        side="SELL",
                                        order_type="LIMIT",
                                        quantity=s_qty,
                                        price=f"{exec_sell_price:.2f}",
                                        client_order_id=idem_k,
                                    ),
                                    timeout=4.0,
                                )
                                new_s_id = str((res_s or {}).get("result", {}).get("orderId") or "")
                                await AssassinLedger.save_state(
                                    symbol,
                                    sell_order_id=new_s_id,
                                    target_sell_price=exec_sell_price,
                                )
                                sell_order_id = new_s_id
                                sweep_ok = True
                            elif surviving_s_ids:
                                await asyncio.wait_for(
                                    ctx.client.modify_order(
                                        order_id=surviving_s_ids[0],
                                        price=f"{exec_sell_price:.2f}",
                                        order_type="LIMIT",
                                        market_country="US",
                                    ),
                                    timeout=4.0,
                                )
                                await AssassinLedger.save_state(
                                    symbol,
                                    sell_order_id=surviving_s_ids[0],
                                    target_sell_price=exec_sell_price,
                                )
                                sell_order_id = surviving_s_ids[0]
                                sweep_ok = True

                        if sweep_ok and not t_state.get("sweep_fired", False):
                            t_state["sweep_fired"] = True
                            ctx.wakeup_event.set()
                            est_net_usd = round(
                                (exec_sell_price - avg_price) * holdings_qty
                                - (exec_sell_price + avg_price) * holdings_qty * 0.0007,
                                2,
                            )
                            await ctx.notify_tg(
                                f"⚡ <b>[이원화 트레일링 익절 스윕] {sym_icon}{symbol} 0.1초 즉시 정정 타격</b>\n"
                                f"▫️ 격발 사유: {exit_reason_str}\n"
                                f"▫️ 평단가: <code>${avg_price:.2f}</code> ➡️ 익절 스윕 단가(LIMIT): <code>${exec_sell_price:.2f}</code> "
                                f"(세전 <code>+{exec_pct:.2f}%</code> · 예상 실순수익 <code>+${est_net_usd:,.2f}</code>)"
                            )
                    except Exception as sw_err:
                        print(f"🚨 [하이킨 아시 트레일링 스윕 방어 {symbol}] {sw_err}", flush=True)
                    finally:
                        ctx.in_memory_ordering_lock[symbol] = False
                    return

    # =====================================================================
    # 2. 기존 매도 덫 주문 상태 무결성 점검 (외부 취소/거절/세션만료 시 자동 복구 및 재결속 준비)
    # =====================================================================
    if holdings_qty > 0 and sell_order_id and is_active:
        try:
            od = await ctx.client.get_order_detail(sell_order_id)
            status_trap = od.get("status", "")
            if status_trap in ["CANCELED", "REJECTED"]:
                _reset_manual_stage3_anchor(symbol)
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                sell_order_id = ""
                target_sell_price = 0.0
        except Exception as e:
            if "404" in str(e).lower() or "not-found" in str(e).lower():
                _reset_manual_stage3_anchor(symbol)
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                sell_order_id = ""
                target_sell_price = 0.0

    # =====================================================================
    # 3. 최초 매수 전량 체결(또는 스나이퍼 재가동/세션 전환) 확인 즉시
    #    거래소 매도 주문(상단 안전 캡 +5.00% 또는 휩소 1주 +1.00% 익절가) 장전 및 실시간 QQQ 기준가 동기화
    # =====================================================================
    if (
        holdings_qty > 0
        and not has_open_sell
        and not has_open_buy
        and not sell_order_id
        and not ctx.in_memory_ordering_lock[symbol]
        and is_active
    ):
        is_rearm, trap_tag, skip_trap = True, "", False
        order_limit_price = 0.0
        ledger_target_price = 0.0

        if target_sell_price <= 0.0:
            if buy_order_id:
                try:
                    order_detail = await ctx.client.get_order_detail(buy_order_id)
                    status = order_detail.get("status", "")
                    ordered_qty = int(math.floor(float(order_detail.get("quantity") or 0.0)))
                    exec_info = order_detail.get("execution") or {}
                    filled_qty = int(math.floor(float(exec_info.get("filledQuantity") or 0.0)))

                    if status in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                        skip_trap = True
                    elif status == "FILLED":
                        if ordered_qty > 0 and (filled_qty < ordered_qty or holdings_qty < ordered_qty):
                            skip_trap = True
                        else:
                            is_rearm = False
                    elif status in ["CANCELED", "REJECTED", "REPLACED"]:
                        is_rearm = False
                    else:
                        skip_trap = True
                except Exception:
                    skip_trap = True
            else:
                is_rearm = False

            if skip_trap:
                return

        if avg_price > 0.0:
            if entry_session == "WHIPSAW_1SH":
                order_limit_price = math.ceil(avg_price * WHIPSAW_SELL_PROFIT_FACTOR * 100.0) / 100.0
                ledger_target_price = order_limit_price
                exp_w_rate = ((order_limit_price / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 1.00
                trap_tag = f"듀얼 휩소 작전 1주 체결(목표 +{exp_w_rate:.2f}% 익절)"
            else:
                arm_p = math.ceil(avg_price * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                safety_cap_p = math.ceil(avg_price * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0
                order_limit_price = safety_cap_p
                ledger_target_price = arm_p
                if entry_session == "PRE_SNIPER":
                    trap_tag = "프리장 대칭 매수덫 체결(⚡기술적반등 트레일링 결속)"
                elif entry_session == "PRE_PULLBACK":
                    trap_tag = "프리장 추세눌림목(RSI≤35) 매수덫 체결(🚀진성추세 트레일링 결속)"
                elif entry_session == "PRE_PULLBACK_REARM":
                    trap_tag = "프리장 추세눌림목 현재가 요격 체결(🚀진성추세 트레일링 결속)"
                elif entry_session == "PRE_SNIPER_REARM":
                    trap_tag = "프리장 현재가 요격 체결(⚡기술적반등 트레일링 결속)"
                else:
                    trap_tag = f"수동 스나이퍼 개입({holdings_qty}주 · 🔥이원화 트레일링 결속)"

        trap_qty = holdings_qty
        if order_limit_price > 0.0 and trap_qty > 0 and not skip_trap:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                sellable_q = await ctx.client.get_sellable_quantity(symbol)
                sellable_q_int = int(math.floor(sellable_q))
                if sellable_q_int < trap_qty:
                    return

                idem = ctx.idempotency_keys[symbol]["TRAP"]
                if not idem:
                    idem = {
                        "key": f"TRAP_{symbol}_{now_est.strftime('%Y%m%d_%H%M%S')}"[:36],
                        "qty": trap_qty,
                        "price": order_limit_price,
                    }
                    ctx.idempotency_keys[symbol]["TRAP"] = idem
                else:
                    trap_qty = idem["qty"]
                    order_limit_price = idem["price"]

                res = await ctx.client.create_order(
                    symbol=symbol, side="SELL", order_type="LIMIT",
                    quantity=trap_qty, price=f"{order_limit_price:.2f}", client_order_id=idem["key"],
                )

                new_sell_id = ""
                if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                    new_sell_id = str(res["result"]["orderId"])

                is_manual_trap = (
                    not buy_order_id
                    and entry_session not in _AUTO_ENTRY_SESSION_TAGS
                )

                if not is_rearm:
                    nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                    nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
                    ref_p_now, ref_src_now = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                    if ref_p_now <= 0.0:
                        ref_p_now = nq_c_now

                    trend_str = "SHORT" if symbol == "SOXS" else "LONG"
                    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"

                    st_cur = await AssassinLedger.get_state(symbol)
                    saved_nq_p = float(st_cur[10] or 0.0)
                    saved_nq_a = float(st_cur[11] or 0.0)

                    eff_nq_p = ref_p_now if ref_p_now > 0.0 else saved_nq_p
                    eff_nq_a = nq_amp_now if nq_amp_now > 0.0 else saved_nq_a

                    # 초과 Case 100 — 상단 안전 캡 매도 주문(new_sell_id) 및 평단가(price=avg_price) 결속 시
                    # entry_session(PRE_SNIPER / PRE_SNIPER_REARM / PRE_PULLBACK / PRE_PULLBACK_REARM 등)은 그대로 유지하고
                    # 체결 완료된 buy_order_id는 깔끔히 비워 정산 시 불필요한 매수 상세 조회를 방지
                    kwargs = {
                        "price": avg_price, "target_sell_price": ledger_target_price,
                        "buy_order_id": "", "sell_order_id": new_sell_id, "is_session_done": True,
                        "nq_entry_price": eff_nq_p, "nq_entry_amp": eff_nq_a,
                    }

                    if entry_session == "WHIPSAW_1SH":
                        kwargs["whipsaw_target_price"] = order_limit_price
                    elif is_manual_trap:
                        kwargs.update({
                            "entry_session": "MANUAL", "entry_trend": trend_str,
                        })
                        mark_manual_stage3_anchored(symbol, new_sell_id)

                    await AssassinLedger.save_state(symbol, **kwargs)
                    await AssassinLedger.save_state(opp_sym, is_session_done=True)

                    if entry_session == "WHIPSAW_1SH":
                        gross_tp_pct = ((order_limit_price / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 1.00
                        est_net_usd = round(
                            (order_limit_price - avg_price) * trap_qty
                            - (order_limit_price + avg_price) * trap_qty * 0.0007,
                            2,
                        )
                        await ctx.notify_tg(
                            f"🟢 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] {trap_tag} 지정가 매도 덫 장전 완료</b>\n"
                            f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 <code>{trap_qty}주</code>)\n"
                            f"▫️ 목표 익절가(+1.00%): <code>${order_limit_price:.2f}</code> (+{gross_tp_pct:.2f}% 지정가 · 예상 실순수익 <code>+${est_net_usd:,.2f}</code>)"
                        )
                    else:
                        min_net_usd = round(
                            (ledger_target_price - avg_price) * trap_qty
                            - (ledger_target_price + avg_price) * trap_qty * 0.0007,
                            2,
                        )
                        ref_unit = "$" if ref_src_now == "QQQ실시간" else ""
                        dyn_th = max(
                            eff_nq_a * 0.5,
                            float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                            MIN_STAGE3_QQQ_ADVERSE_PCT,
                        )
                        if is_manual_trap:
                            defense_desc = (
                                f"<code>수동 진입(MANUAL) 2차 과열방어 패스 · 3차 {ref_src_now} 역행방어"
                                f"(기준 {ref_unit}{eff_nq_p:.2f} / NQ진폭 {eff_nq_a:.2f}% 기반 한도 +{dyn_th:.2f}%) · 15:00~15:58 미익절 VWAP탈출 가동</code>"
                            )
                        else:
                            defense_desc = (
                                f"<code>2차 NQ=F 과열방어(≥2.0%) · 3차 {ref_src_now} 역행방어"
                                f"(기준 {ref_unit}{eff_nq_p:.2f} / NQ진폭 {eff_nq_a:.2f}% 기반 한도 +{dyn_th:.2f}%) · 최저점 반등(RE_BOT_TRAIL) · 15:00~15:58 VWAP탈출 가동</code>"
                            )

                        if is_trend_pb_regime:
                            exit_mode_desc = (
                                f"<code>🚀 진성추세 모드 : ${ledger_target_price:.2f}(+1.00%) 돌파 후 고점 +{TREND_PB_LAUNCH_RATCHET_PCT:.2f}% 전 빨간양봉(C≥O&amp;HA양봉) 홀딩 ➔ "
                                f"파란음봉(C&lt;O) 또는 +{TREND_PB_LAUNCH_RATCHET_PCT:.2f}% 돌파 시 +1.00% 잠금 &amp; -{TREND_PB_CALLBACK_PCT:.2f}% 타이트 스윕</code>"
                            )
                        else:
                            exit_mode_desc = (
                                f"<code>⚡ 기술적반등 모드 : ${ledger_target_price:.2f}(+1.00%) 돌파 즉시 보존선 고정 &amp; "
                                f"고점 대비 -{TRAIL_DROP_CALLBACK_PCT:.2f}% 콜백/HA음봉 시 0.1초 정정 익절 스윕</code>"
                            )

                        await ctx.notify_tg(
                            f"🟢 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] {trap_tag} 상단 안전 캡 &amp; 트레일링 결속 완료</b>\n"
                            f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 <code>{trap_qty}주</code>)\n"
                            f"▫️ 트레일링 점화가(+1.00%): <code>${ledger_target_price:.2f}</code> (보존선 기준 최소 실순수익 <code>+${min_net_usd:,.2f}</code> ~ 무제한 추적)\n"
                            f"▫️ 거래소 안전 캡(+5.00% LIMIT): <code>${order_limit_price:.2f}</code> 선장전 완료\n"
                            f"▫️ 매크로 방어: {defense_desc}\n"
                            f"▫️ 매도 레짐: {exit_mode_desc}"
                        )
                else:
                    if is_manual_trap:
                        mark_manual_stage3_anchored(symbol, new_sell_id)
                    await AssassinLedger.save_state(symbol, buy_order_id="", sell_order_id=new_sell_id)

                ctx.idempotency_keys[symbol]["TRAP"] = None
                runtime_state["last_trap_error_msg"] = ""
            except Exception as e:
                err_str = str(e)
                if any(code in err_str for code in ["400", "422", "404", "409", "401", "403"]):
                    ctx.idempotency_keys[symbol]["TRAP"] = None
                if err_str != runtime_state["last_trap_error_msg"]:
                    await ctx.notify_tg(f"🚨 <b>[TRAP 에러 {sym_icon}{symbol}]</b> {html.escape(err_str)}")
                    runtime_state["last_trap_error_msg"] = err_str
                await asyncio.sleep(5.0)
            finally:
                ctx.in_memory_ordering_lock[symbol] = False
