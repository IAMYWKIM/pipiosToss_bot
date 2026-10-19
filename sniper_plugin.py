# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=79.9[80.0 클라이맥스 인지] · 옵션 A : 🪤 DUAL80_TRAP 즉시 매수 덫)
#       무한 출동 듀얼 스나이퍼(SOXS/SOXL), 3차 손절 후 최저점 역트레일링 +0.30% 반등 재진입(🪤 RE_BOT_TRAIL),
#       단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0 × 🚀 NQ_VEL_BYPASS)' 무제한 익절 추적 및 통합 실수익 복리 정산 코어
# =====================================================================
# MODIFIED: 제3헌법 및 초과 Case 88 의거 매크로 3단 방어·08:15 선제 컷오프·15:59 MOC 청산 모듈을 defense_plugin.py로 분리 및 리익스포트 결속
# MODIFIED: 프리장 본진 타격 및 무한 재출동 허용 윈도우를 04:07 ~ 08:14 EST 초중반 황금 구간으로 하드 락온 (Case 52, 53, 84)
# MODIFIED: 단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0)' 무제한 익절 추적 및 0.1초 즉시 정정 스윕 가동 (Case 58, 85)
# MODIFIED: 초과 Case 89 & Case 91 — 트레일링 점화(+1.00%) 후 고점 추적 중 토스 Open API QQQ 0.0초 실시간(폴백 NQ=F)
#           10분/3분 단기 변화율이 유리 방향으로 ±0.20% 이상 급등·급락 지속 시 실시간 1분봉 하이킨 아시 음봉 전환을 바이패스하는 🚀 NQ_VEL_BYPASS 탑재
# MODIFIED: 초과 Case 92 — 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)은 나스닥 100 선물지수(NQ=F) 진폭(nq_entry_amp)을 기준으로 산출하고,
#           해당 임계치를 적용할 진입 기준가(nq_entry_price)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온 및 체결 확증 순간 재동기화
# MODIFIED: 제3헌법 및 초과 Case 93 — 프리장 본진(04:07 ~ 08:14 EST)에서 선행 역산 버퍼(_AMP_BUFFER_CACHE)를 100% 영구 소각하고,
#           대칭 클라이맥스 포착 즉시 1차 슬리피지(-0.18%) 매수 덫 + 조건부 30초 타임쉴드를 발사하는 옵션 A(🪤 DUAL80_TRAP) 원형으로 100% 복원!
# NEW: 초과 Case 94 — 스나이퍼 가동 OFF 전환 시 nq_entry_price=0.0, nq_entry_amp=0.0 원자적 초기화 및
#      수동 보유 물량 안전캡(+5.00% LIMIT) 결속 시 현재 실시간 QQQ 기준가(0.00% 역행 출발) 강제 동기화·마킹(mark_manual_stage3_anchored)으로
#      포지션 자체 손실률(-0.80% 초과 등)이나 과거 기준가로 인한 즉시 손절을 100% 원천 차단
# MODIFIED: 초과 Case 58, 79, 95 & 96 — 03:58 ~ 04:07 EST 1주 휩소 작전(WHIPSAW_1SH)이 데이장 VWAP 0.5% 역산 가격에 체결되면
#           즉시 실제 매수 체결 평단가(avg_price) 대비 +1.00% 익절가(math.ceil(avg_price * 1.01 * 100) / 100.0)에 1주 지정가 매도 덫을 장전하며,
#           과열차단(NQ>=2.0%)일에 청산 완료 시 즉시 양쪽 장부를 CANCELED_MACRO(is_session_done=True)로 복원하여 본진 자동 진입을 100% 차단!
# MODIFIED: 04:07 ~ 08:14 EST 중 NQ=F < 2.0% 내 3차 역행 손절 후 최저점 역트레일링 +0.30% 양봉 반등 재진입(RE_BOT_TRAIL) 가동 (Case 87)
# MODIFIED: 통합 시드 풀(Unified Seed Pool) 기반 실제 발생 순수익금((실제매도가-실제매수가)*체결수량 - 수수료 0.14%) 복리 정산 동기화 (Case 76)

import math
import time
import asyncio
import html
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from aiogram import Bot

from toss_api import TossApiClient
from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
)
from whipsaw_plugin import (
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    BUY_TRAP_SLIPPAGE_PCT,
    BUY_TRAP_MIN_DISC_USD,
    BUY_TRAP_REARM_WAIT_SEC,
    WHIPSAW_BUY_REVERSE_FACTOR,
    WHIPSAW_SELL_PROFIT_FACTOR,
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
)
from defense_plugin import (
    TRAIL_ARM_PROFIT_RATE,
    TRAIL_FLOOR_LOCK_RATE,
    TRAIL_DROP_CALLBACK_PCT,
    TRAIL_SAFETY_CAP_RATE,
    NQ_VELOCITY_SURGE_PCT,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    BOTTOM_REBOUND_PCT,
    BOTTOM_MAX_RSI_CAP,
    _STRIKE_ALERT_CACHE,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    _activate_bottom_track_state,
    get_bottom_track_status,
    _cancel_open_orders_and_wait_sellable,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
    enforce_0815_pre_macro_cutoff_plugin,
)

EST_TZ = ZoneInfo("America/New_York")


@dataclass
class SniperPluginContext:
    client: TossApiClient
    bot: Bot
    chat_id: int
    wakeup_event: asyncio.Event
    in_memory_ordering_lock: dict
    dual_strike_mutex: asyncio.Lock
    shared_holdings: dict
    idempotency_keys: dict
    nq_dynamic_floor: float = 0.8
    shared_rsi_state: dict = None

    async def notify_tg(self, text: str):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML")
        except Exception as e:
            print(f"🚨 [텔레그램 전송 붕괴 방어] {e}", flush=True)


async def execute_rsi_monitoring_plugin(
    ctx: SniperPluginContext, symbol: str, rsi_val: float, vwap_val: float, current_price: float
):
    if ctx.shared_rsi_state is None:
        return

    state = ctx.shared_rsi_state.get(symbol)
    if not state:
        return

    state["value"] = rsi_val
    state["vwap"] = vwap_val

    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute

    # 트레일링 점화 상태이거나 3차 손절 후 최저점 역트레일링 추적 중일 때도 캔들 갱신 즉시 메인 루프 기상 (Case 25, 85, 87)
    if (
        _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False)
        or (_BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol)
    ):
        ctx.wakeup_event.set()

    if symbol in ("SOXS", "SOXL") and (400 <= est_hm <= 814):
        if (
            0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD
            or rsi_val >= PRE_CLIMAX_BOOSTER_RSI
            or rsi_val > PRE_SNIPER_RSI_THRESHOLD
        ):
            ctx.wakeup_event.set()
    elif symbol in ("SOXS", "SOXL") and (est_hm >= 1900 or est_hm <= 359):
        if rsi_val >= PRE_CLIMAX_BOOSTER_RSI or (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD):
            ctx.wakeup_event.set()
        await execute_daymarket_rsi_alert_plugin(
            ctx=ctx,
            symbol=symbol,
            now_est=now_est,
            current_price=current_price,
            rsi_val=rsi_val,
            vwap_val=vwap_val,
        )


# 하이킨 아시 트레일링 익절(+1.00% ~ +5.00%+), 3차 손절 후 최저점 역트레일링 전환(STAGE3_STOP_TRACK) 및 모든 청산 시
# 실제 발생 순수익금((실제매도가 - 실제매수가) * 체결수량 - 왕복수수료 0.14%) 복리 정산 및
# 프리장 골든타임(04:00 ~ 08:14 EST) 내 조건 충족 시 양쪽 장부 동시 재출동 락 해제 (Case 52, 76, 80, 84, 87, 94, 95)
async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext,
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
            st = od.get("status", "")
            if st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                can_clear = False
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
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"

        now_est_check = datetime.now(EST_TZ)
        est_hm_now = now_est_check.hour * 100 + now_est_check.minute
        is_premarket_now = (400 <= est_hm_now <= 814)
        is_moc_time = (now_est_check.hour == 15 and now_est_check.minute >= 59) or (
            now_est_check.hour == 16 and 0 <= now_est_check.minute <= 5
        )
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

                if st == "FILLED":
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
            if is_take_profit_exit and prev_target_sell_price > 0.0:
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
            and prev_entry_session not in ("CANCELED_MACRO", "STAGE3_STOP_TRACK")
        ):
            is_take_profit_exit = True
        elif was_trail_armed and not is_moc_time and prev_entry_session not in ("CANCELED_MACRO", "STAGE3_STOP_TRACK"):
            is_take_profit_exit = True
        elif is_stage3_stop_track_exit:
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

        # MODIFIED: 초과 Case 52, 80, 84, 87, 94, 95 — 프리장 골든타임 (04:00 ~ 08:14 EST) 내 익절 완료, 매수 취소, 1주 휩소 작전 정리,
        # 또는 NQ=F < 2.0% 내 3차 안전망 손절(STAGE3_STOP_TRACK) 완료 시(단, 반대편 종목을 수동 보유 중이 아닐 때) 양쪽 장부 모두 is_session_done=False로 복원!
        # 반면 당일 NQ=F >= 2.0% 과열일 상태에서 1주 휩소 작전(WHIPSAW_1SH)이 청산된 경우에는 즉시 CANCELED_MACRO(과열차단)로 복원하여 본진 자동 진입을 차단!
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
                whip_next_note = "04:07 ~ 08:14 EST 본진 양방향 대칭 클라이맥스 듀얼 스나이퍼 무한 출동 감시 개시"

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
            if not next_is_session_done:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 하이킨 아시 트레일링 익절 성공 ➡️ 무한 재출동 복귀</b>\n"
                    f"▫️ 잔고 0주 (트레일링 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 후속 조치: 08:14 EST까지 차기 양방향 대칭 클라이맥스(≤30.0 &amp; 반대≥79.9) 연속 감시 재개"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 하이킨 아시 트레일링 익절 성공 (08:15 컷오프/과열 마감)</b>\n"
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


# 초과 Case 87, Case 92, Case 93, Case 94 — 04:07 ~ 08:14 EST 골든타임 내
# 순수 3차 안전망 손절 후 '손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 재진입 (🪤 RE_BOT_TRAIL)' 전용 엔진
# (선행 역산 버퍼 분기 소각 · NQ=F 진폭 기준 3차 임계치[최소 0.80%] 산출 + 토스 QQQ 0.0초 실시간 현재가 기준점 락온)
async def execute_post_stop_bottom_rebound_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    buy_order_id: str,
    current_price: float,
    budget: float,
    rsi_val: float,
    opp_rsi_val: float = 0.0,
) -> bool:
    if not (_BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol):
        return False

    if symbol not in ("SOXS", "SOXL"):
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if (
        not is_active
        or not (407 <= est_hm <= 814)
        or _BOTTOM_TRACK_CACHE.get("date") != today_str
        or holdings_qty > 0
        or bool(buy_order_id)
        or ctx.in_memory_ordering_lock.get(symbol, False)
    ):
        if not (407 <= est_hm <= 814) or _BOTTOM_TRACK_CACHE.get("date") != today_str:
            _reset_bottom_track_state()
        return False

    if current_price <= 0.0:
        return False

    cur_p_fl = math.floor(current_price * 100.0) / 100.0
    if cur_p_fl <= 0.0:
        return False

    last_stop_p = float(_BOTTOM_TRACK_CACHE.get("last_stop_price", 0.0))
    if last_stop_p <= 0.0:
        _reset_bottom_track_state()
        return False

    # 주가가 반등 체결 없이 손절가 대비 +1.5% 이상 이미 날아가 버렸거나 RSI가 50.0 이상으로 중립화되면 최저점 추적 자동 해제
    if cur_p_fl > last_stop_p * 1.015 or rsi_val >= 50.0:
        _reset_bottom_track_state()
        st_cur = await AssassinLedger.get_state(symbol)
        if st_cur[8] == "STAGE3_STOP_TRACK":
            await AssassinLedger.save_state(symbol, entry_session="")
        return False

    sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol, {})
    ewm_st = sym_rsi_dict.get("ewm_state", {}) if isinstance(sym_rsi_dict, dict) else {}
    cur_bar_open = float(ewm_st.get("cur_bar_open", 0.0))
    cur_bar_low = float(ewm_st.get("cur_bar_low", 0.0))
    cand_low = min(cur_p_fl, math.floor(cur_bar_low * 100.0) / 100.0) if cur_bar_low > 0.0 else cur_p_fl

    prev_low = float(_BOTTOM_TRACK_CACHE.get("post_stop_low", last_stop_p))
    if (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD) and (opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD):
        _BOTTOM_TRACK_CACHE["climax_seen"] = True

    # 1단계 (칼날 회피): 신저가를 경신 중이면 절대 매수하지 않고 최저점(post_stop_low)과 반등 목표가(+0.30%)만 갱신!
    made_new_low_tick = False
    if cand_low < prev_low:
        _BOTTOM_TRACK_CACHE["post_stop_low"] = cand_low
        _BOTTOM_TRACK_CACHE["rebound_trig_price"] = (
            math.ceil(cand_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
        )
        _BOTTOM_TRACK_CACHE["last_new_low_ts"] = time.time()
        if cand_low < last_stop_p:
            _BOTTOM_TRACK_CACHE["dipped_below_stop"] = True
        if cur_p_fl <= cand_low:
            made_new_low_tick = True

    post_low = float(_BOTTOM_TRACK_CACHE.get("post_stop_low", prev_low))
    if post_low < last_stop_p:
        _BOTTOM_TRACK_CACHE["dipped_below_stop"] = True

    dipped_below_stop = bool(_BOTTOM_TRACK_CACHE.get("dipped_below_stop", False))
    rebound_trig_p = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
    if rebound_trig_p <= 0.0 and post_low > 0.0:
        rebound_trig_p = math.ceil(post_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
        _BOTTOM_TRACK_CACHE["rebound_trig_price"] = rebound_trig_p

    if made_new_low_tick or not dipped_below_stop or is_session_done:
        return False

    is_ha_bull, _, _ = compute_realtime_tick_heikin_ashi(cur_p_fl, ewm_st)
    is_bull_rebound = bool(is_ha_bull or (cur_bar_open > 0.0 and cur_p_fl > cur_bar_open))

    # 2단계: 손절가 아래 최저점(post_low < last_stop_p)을 찍고 신저가 멈춤 + 최저점 대비 +0.30% 이상 양봉 반등 + RSI<=38.0 확인!
    if not (
        bool(_BOTTOM_TRACK_CACHE.get("climax_seen", True))
        and cur_p_fl >= rebound_trig_p
        and cur_p_fl <= last_stop_p
        and is_bull_rebound
        and (0.0 < rsi_val <= BOTTOM_MAX_RSI_CAP)
    ):
        return False

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False) or ctx.in_memory_ordering_lock.get(opp_sym, False):
            return False
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            opp_state = await AssassinLedger.get_state(opp_sym)
            if (
                self_state[4]
                or opp_state[4]
                or not bool(self_state[5])
                or self_state[8] == "CANCELED_MACRO"
                or opp_state[8] == "CANCELED_MACRO"
                or await AssassinLedger.get_buy_order_id(symbol) != ""
                or await AssassinLedger.get_buy_order_id(opp_sym) != ""
                or bool(self_state[7])
                or bool(opp_state[7])
                or ctx.shared_holdings.get(symbol, 0) > 0
                or ctx.shared_holdings.get(opp_sym, 0) > 0
            ):
                return False

            # 1차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준은 나스닥 100 선물지수(NQ=F) 진폭을 사용 (Case 66, 67, 92, 94)
            nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                _reset_bottom_track_state()
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 최저점 역트레일링 감시 중 대세장 과열 감지 (자동 진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 최저점 추적 캐시 파기 및 금일 자동 매수 권한 영구 소각"
                )
                return False

            # 초과 Case 92 & Case 94 — NQ=F 진폭 기반 임계치 max(nq_entry_amp * 0.5, 0.80%)를 실시간 적용할
            # 3차 역행 안전망 진입 기준가(ref_entry_p)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온!
            ref_entry_p, ref_src = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True, realtime_probing=True
            )
            if ref_entry_p <= 0.0:
                ref_entry_p = nq_c_global

            latest_p = await ctx.client.get_current_price(symbol)
            if latest_p <= 0.0:
                latest_p = cur_p_fl
            exec_buy_p = math.floor(latest_p * 100.0) / 100.0
            if exec_buy_p < rebound_trig_p or exec_buy_p > last_stop_p:
                return False

            for chk_sym in ("SOXS", "SOXL"):
                hold_chk = await ctx.client.get_symbol_holdings_detail(chk_sym)
                if int(math.floor(hold_chk.get("qty", 0.0))) > 0:
                    return False
                open_orders_chk = await ctx.client.get_orders(status="OPEN", symbol=chk_sym)
                if any(o.get("side") in ("BUY", "SELL") for o in open_orders_chk):
                    return False

            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.995)
            except Exception:
                actual_budget = budget

            target_qty = int(math.floor(actual_budget / exec_buy_p))
            if target_qty <= 0:
                return False

            trend_str = "SHORT" if symbol == "SOXS" else "LONG"
            idem_key = f"RBOT_{symbol}_{now_est.strftime('%H%M%S')}"[:36]
            res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                order_type="LIMIT",
                quantity=target_qty,
                price=f"{exec_buy_p:.2f}",
                client_order_id=idem_key,
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                _reset_trailing_runtime_state(symbol)
                _reset_bottom_track_state()
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="PRE_SNIPER_REARM",
                    entry_trend=trend_str,
                    entry_time=time.time(),
                    nq_entry_price=ref_entry_p,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                ctx.wakeup_event.set()
                rebound_actual_pct = ((exec_buy_p / post_low - 1.0) * 100.0) if post_low > 0.0 else BOTTOM_REBOUND_PCT
                disc_from_stop = last_stop_p - exec_buy_p
                ref_unit = "$" if ref_src == "QQQ실시간" else ""
                dyn_th_init = max(
                    nq_amp_global * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                await ctx.notify_tg(
                    f"🪤 <b>[3차 손절 후 최저점 역트레일링 재출격] {sym_icon}{symbol} 바닥 확인 반등 요격 발사!</b>\n"
                    f"▫️ 바닥 궤적: 직전 손절가 <code>${last_stop_p:.2f}</code> ➡️ 경유 최저점 <code>${post_low:.2f}</code> 찍고 <code>+{rebound_actual_pct:.2f}%</code> 양봉 반등 돌파!\n"
                    f"▫️ 요격 단가(LIMIT): <code>${exec_buy_p:.2f}</code> (손절가 대비 <code>-${disc_from_stop:.2f}</code> 저가 재진입 · <code>{target_qty}주</code> · 현재 RSI <code>{rsi_val:.1f}</code>)\n"
                    f"▫️ 3차 방어 기준({ref_src}): <code>{ref_unit}{ref_entry_p:.2f}</code> (NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> 기반 3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
                    "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 하이킨 아시 트레일링(+1.00% 점화 · QQQ실시간 급변 바이패스) 자동 결속</code>"
                )
                return True
        except Exception as e:
            print(f"🚨 [최저점 역트레일링 반등 진입 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
    return False


# 프리장 초중반 골든타임 (04:07 ~ 08:14 EST) 양방향 대칭 클라이맥스 단일화 타격 (옵션 A 원형 : 🪤 DUAL80_TRAP)
# — (SOXS RSI <= 30.0 AND SOXL RSI >= 79.9) 또는 (SOXL RSI <= 30.0 AND SOXS >= 79.9) 포착 시
#   선행 역산 버퍼 보류 없이 즉시 1차 슬리피지(-0.18%) 지정가 매수 덫 선(先) 투하 (Case 54, 59, 71, 81, 83, 84, 87, 92, 93, 94)
async def execute_premarket_rsi_sniper_strike_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    current_price: float,
    budget: float,
    target_profit_rate: float,
    rsi_val: float,
    opp_rsi_val: float = 0.0,
):
    if symbol not in ("SOXS", "SOXL") or ctx.in_memory_ordering_lock.get(symbol, False):
        return

    # Case 87: 해당 종목이 3차 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 추적 중이면 중복 포착 차단!
    if _BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
        return

    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return

    if not (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD and opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD):
        return

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    opp_icon = "🐂" if opp_sym == "SOXL" else "🐻"

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False) or ctx.in_memory_ordering_lock.get(opp_sym, False):
            return
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            opp_state = await AssassinLedger.get_state(opp_sym)

            if (
                self_state[4]
                or opp_state[4]
                or not bool(self_state[5])
                or self_state[8] == "CANCELED_MACRO"
                or opp_state[8] == "CANCELED_MACRO"
                or await AssassinLedger.get_buy_order_id(symbol) != ""
                or await AssassinLedger.get_buy_order_id(opp_sym) != ""
                or bool(self_state[7])
                or bool(opp_state[7])
                or ctx.shared_holdings.get(symbol, 0) > 0
                or ctx.shared_holdings.get(opp_sym, 0) > 0
            ):
                return

            # 1차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준은 나스닥 100 선물지수(NQ=F) 진폭을 사용 (Case 66, 67, 92, 94)
            nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                _reset_bottom_track_state()
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                _STRIKE_ALERT_CACHE["last_macro1_date"] = now_est.strftime("%Y-%m-%d")
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 대세장 과열 감지 (자동 진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 금일 SOXS 및 SOXL 자동 매수 권한 영구 소각 및 관망 전환"
                )
                return

            # 초과 Case 92 & Case 94 — NQ=F 진폭 기반 임계치 max(nq_entry_amp * 0.5, 0.80%)를 실시간 적용할
            # 3차 역행 안전망 진입 기준가(ref_entry_p)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온!
            ref_entry_p, ref_src = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True, realtime_probing=True
            )
            if ref_entry_p <= 0.0:
                ref_entry_p = nq_c_global

            prices_batch = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
            latest_price = float(prices_batch.get(symbol, 0.0)) or current_price
            opp_latest_p = float(prices_batch.get(opp_sym, 0.0))

            if latest_price <= 0.0:
                return

            if opp_latest_p > 0.0 and ctx.shared_rsi_state and opp_sym in ctx.shared_rsi_state:
                opp_dict = ctx.shared_rsi_state[opp_sym]
                opp_rsi_recheck = compute_realtime_tick_rsi(
                    live_price=opp_latest_p,
                    ewm_state=opp_dict.get("ewm_state", {}),
                    fallback_rsi=opp_rsi_val,
                )
                opp_dict["value"] = opp_rsi_recheck
            else:
                opp_rsi_recheck = opp_rsi_val

            if opp_rsi_recheck < OPP_CLIMAX_RSI_THRESHOLD:
                return

            for chk_sym in ("SOXS", "SOXL"):
                hold_chk = await ctx.client.get_symbol_holdings_detail(chk_sym)
                if int(math.floor(hold_chk.get("qty", 0.0))) > 0:
                    return
                open_orders_chk = await ctx.client.get_orders(status="OPEN", symbol=chk_sym)
                if any(o.get("side") in ("BUY", "SELL") for o in open_orders_chk):
                    return

            cur_p_floored = math.floor(latest_price * 100.0) / 100.0
            if cur_p_floored <= 0.0:
                return

            # 옵션 A 원형 복원 (Case 59, 93): 프리장 본진에서는 선행 역산 버퍼 보류 없이
            # 즉시 1차 슬리피지(-0.18%, 최소 -$0.02 할인) 지정가 매수 덫을 선투하!
            slip_disc = max(
                BUY_TRAP_MIN_DISC_USD,
                round(cur_p_floored * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2),
            )
            trap_buy_price = math.floor((cur_p_floored - slip_disc) * 100.0) / 100.0
            if trap_buy_price <= 0.0:
                trap_buy_price = cur_p_floored

            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.990)
            except Exception:
                actual_budget = budget

            target_qty = int(math.floor(actual_budget / cur_p_floored))

            warn_key = f"{current_session_id}_{symbol}"
            if target_qty <= 0:
                if _STRIKE_ALERT_CACHE.get("last_warn_session") != warn_key:
                    _STRIKE_ALERT_CACHE["last_warn_session"] = warn_key
                    bp_str = f"${current_bp:,.2f}" if current_bp >= 0.0 else "조회실패"
                    await ctx.notify_tg(
                        f"⚠️ <b>[프리장 듀얼 스나이퍼 요격 보류] {sym_icon}{symbol} 주문 수량 0주 산출</b>\n"
                        f"▫️ 대칭 포착: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> &amp; {opp_icon}{opp_sym} RSI <code>{opp_rsi_recheck:.1f}</code> | 현재가: <code>${cur_p_floored:.2f}</code>\n"
                        f"▫️ 설정 시드: <code>${budget:,.2f}</code> | USD 가능금액: <code>{bp_str}</code>\n"
                        f"▫️ 사유: 유효 투입금(<code>${actual_budget:,.2f}</code>)이 1주 가격(<code>${cur_p_floored:.2f}</code>) 미만입니다."
                    )
                return

            is_compound = await AssassinLedger.get_is_compound(symbol)
            mode_tag = "통합복리" if is_compound else "단리"
            comp_tail = "🔥 +1.00% 돌파 HA·트레일링 추적 · 실수익 복리 증액" if is_compound else "🔥 +1.00% 돌파 HA·트레일링 추적 · 단리 고정"
            trend_str = "SHORT" if symbol == "SOXS" else "LONG"

            idem = ctx.idempotency_keys[symbol]["BUY"]
            if not idem:
                idem = {
                    "key": f"PSNP_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
                    "qty": target_qty,
                    "price": trap_buy_price,
                }
                ctx.idempotency_keys[symbol]["BUY"] = idem
            else:
                target_qty = idem["qty"]
                trap_buy_price = idem["price"]

            res = await ctx.client.create_order(
                symbol=symbol, side="BUY", order_type="LIMIT",
                quantity=target_qty, price=f"{trap_buy_price:.2f}", client_order_id=idem["key"],
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                _reset_trailing_runtime_state(symbol)
                _reset_bottom_track_state()
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="PRE_SNIPER",
                    entry_trend=trend_str,
                    entry_time=time.time(),
                    nq_entry_price=ref_entry_p,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                _STRIKE_ALERT_CACHE["last_err_msg"] = ""
                ctx.wakeup_event.set()
                disc_actual = cur_p_floored - trap_buy_price
                ref_unit = "$" if ref_src == "QQQ실시간" else ""
                dyn_th_init = max(
                    nq_amp_global * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                await ctx.notify_tg(
                    f"🪤 <b>[프리장 듀얼 스나이퍼] {sym_icon}{symbol} 대칭 클라이맥스 포착 ➡️ 1차 슬리피지 매수 덫 투하</b>\n"
                    f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≤30.0) &amp; {opp_icon}{opp_sym} RSI <code>{opp_rsi_recheck:.1f}</code> (≥79.9[80인지])\n"
                    f"▫️ 포착 현재가: <code>${cur_p_floored:.2f}</code> ➡️ 1차 매수 덫(LIMIT): <code>${trap_buy_price:.2f}</code> (슬리피지 <code>-${disc_actual:.2f}</code> 할인 · {mode_tag} <code>${budget:,.2f}</code> ➔ <code>{target_qty}주</code>)\n"
                    f"▫️ 매크로 기준: 3차 방어({ref_src}) <code>{ref_unit}{ref_entry_p:.2f}</code> | NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> (3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
                    f"▫️ 조건부 30초 타임쉴드: <code>RSI≤30 유지 시 30초간 저가 덫 대기 / 30초 내 RSI 30 초과 반등 시 즉각 현재가 매수 ({comp_tail})</code>"
                )
            ctx.idempotency_keys[symbol]["BUY"] = None
        except Exception as e:
            err_str = str(e)
            print(f"🚨 [프리장 듀얼 스나이퍼 타격 방어 {symbol}] {err_str}", flush=True)
            if any(code in err_str for code in ["400", "422", "404", "409", "401", "403", "429"]):
                ctx.idempotency_keys[symbol]["BUY"] = None
            if _STRIKE_ALERT_CACHE.get("last_err_msg") != err_str:
                _STRIKE_ALERT_CACHE["last_err_msg"] = err_str
                await ctx.notify_tg(
                    f"🚨 <b>[프리장 듀얼 스나이퍼 주문 에러 {sym_icon}{symbol}]</b>\n"
                    f"▫️ 포착 RSI: <code>{rsi_val:.1f}</code> (반대 {opp_sym} RSI: <code>{opp_rsi_val:.1f}</code>)\n"
                    f"▫️ 사유: <code>{html.escape(err_str)}</code>"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False


# 조건부 30초 타임쉴드 및 즉각 매수 전환 플러그인 (SOXS / SOXL 본진 PRE_SNIPER 공통 — 04:07 ~ 08:14 EST)
async def rearm_premarket_buy_trap_after_30s_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    buy_order_id: str,
    entry_session: str,
    entry_time: float,
    budget: float,
    current_price: float = 0.0,
    rsi_val: float = 0.0,
) -> tuple[str, str]:
    if (
        symbol not in ("SOXS", "SOXL")
        or not is_active
        or not buy_order_id
        or entry_session != "PRE_SNIPER"
        or entry_time <= 0.0
        or ctx.in_memory_ordering_lock.get(symbol, False)
    ):
        return buy_order_id, entry_session

    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return buy_order_id, entry_session

    elapsed = time.time() - entry_time
    is_rsi_rebound_breakout = (rsi_val > PRE_SNIPER_RSI_THRESHOLD)
    is_shield_time_expired = (elapsed >= BUY_TRAP_REARM_WAIT_SEC)

    if not is_rsi_rebound_breakout and not is_shield_time_expired:
        return buy_order_id, entry_session

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    ctx.in_memory_ordering_lock[symbol] = True
    try:
        od = await ctx.client.get_order_detail(buy_order_id)
        status = str(od.get("status") or "")

        if status in ["FILLED", "CANCELED", "REJECTED", "REPLACED"]:
            return buy_order_id, entry_session

        if status not in ["PENDING", "PARTIAL_FILLED"]:
            return buy_order_id, entry_session

        old_trap_price = float(od.get("price") or 0.0)
        ordered_qty = int(math.floor(float(od.get("quantity") or 0.0)))
        exec_info = od.get("execution") or {}
        filled_qty = int(math.floor(float(exec_info.get("filledQuantity") or 0.0)))

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = current_price
        rearm_price = math.floor(latest_p * 100.0) / 100.0
        if rearm_price <= 0.0:
            return buy_order_id, entry_session

        if is_rsi_rebound_breakout:
            trigger_title = f"⚡ [프리장 스나이퍼 RSI 30 초과 반등 즉각 매수] {sym_icon}{symbol}"
            trigger_desc = f"타임쉴드 중 실시간 RSI <code>{rsi_val:.1f}</code> (30.0 초과 반등 감지 · 경과 <code>{elapsed:.1f}초</code>)"
        else:
            trigger_title = f"🔄 [프리장 스나이퍼 30초 타임쉴드 만료 재장전] {sym_icon}{symbol}"
            trigger_desc = f"RSI≤30 유지 30초 타임쉴드 만료 (현재 RSI <code>{rsi_val:.1f}</code> · 경과 <code>{elapsed:.1f}초</code>)"

        if not is_rsi_rebound_breakout and old_trap_price > 0.0 and rearm_price <= old_trap_price:
            await AssassinLedger.save_state(
                symbol, entry_session="PRE_SNIPER_REARM", entry_time=time.time()
            )
            return buy_order_id, "PRE_SNIPER_REARM"

        if status == "PENDING" and abs(rearm_price - old_trap_price) >= 0.01:
            try:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=buy_order_id,
                        price=f"{rearm_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=4.0,
                )
                new_buy_id = str((mod_res or {}).get("result", {}).get("orderId") or buy_order_id)
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="PRE_SNIPER_REARM",
                    entry_time=time.time(),
                )
                ctx.wakeup_event.set()
                await ctx.notify_tg(
                    f"<b>{trigger_title} 현재가 즉시 상향 요격</b>\n"
                    f"▫️ 사유: {trigger_desc}\n"
                    f"▫️ 단가 정정(LIMIT): 기존 덫 <code>${old_trap_price:.2f}</code> ➡️ 현재가 <code>${rearm_price:.2f}</code> (<code>{ordered_qty}주</code>)\n"
                    "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 하이킨 아시 트레일링(+1.00% 점화 · QQQ실시간 급변 바이패스) 자동 결속</code>"
                )
                return new_buy_id, "PRE_SNIPER_REARM"
            except Exception as mod_err:
                print(f"🚨 [재장전 정정 폴백 {symbol}] modify_order 실패 -> 선취소 후 재발사 전환: {mod_err}", flush=True)

        try:
            await ctx.client.cancel_order(buy_order_id)
        except Exception as cancel_err:
            if "already-filled" in str(cancel_err):
                return buy_order_id, entry_session
        await asyncio.sleep(0.35)

        try:
            od_after = await ctx.client.get_order_detail(buy_order_id)
            exec_after = od_after.get("execution") or {}
            filled_after = int(math.floor(float(exec_after.get("filledQuantity") or 0.0)))
        except Exception:
            filled_after = filled_qty

        if filled_after > 0:
            await AssassinLedger.save_state(
                symbol, buy_order_id="", entry_session="PRE_SNIPER", entry_time=time.time()
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🟢 <b>[프리장 스나이퍼 부분체결 확정] {sym_icon}{symbol} 1차 매수 덫 ({filled_after}주) 체결 완료</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 잔여 미체결분 취소 완료 ➡️ 확보된 저가 물량(<code>{filled_after}주</code>) 즉시 하이킨 아시 트레일링 결속 직행"
            )
            return "", "PRE_SNIPER"

        current_bp = -1.0
        try:
            current_bp = await ctx.client.get_usd_buying_power()
            actual_budget = min(budget, current_bp * 0.995)
        except Exception:
            actual_budget = budget

        rearm_qty = int(math.floor(actual_budget / rearm_price))
        if rearm_qty <= 0:
            await AssassinLedger.save_state(
                symbol, buy_order_id="", is_session_done=False, entry_session="", entry_time=0.0
            )
            await AssassinLedger.unlock_all_symbols_for_reentry()
            return "", ""

        idem_key = f"PRRM_{symbol}_{now_est.strftime('%H%M%S')}"[:36]
        res_new = await ctx.client.create_order(
            symbol=symbol,
            side="BUY",
            order_type="LIMIT",
            quantity=rearm_qty,
            price=f"{rearm_price:.2f}",
            client_order_id=idem_key,
        )
        if res_new and isinstance(res_new, dict) and res_new.get("result", {}).get("orderId"):
            new_buy_id = str(res_new["result"]["orderId"])
            await AssassinLedger.save_state(
                symbol,
                buy_order_id=new_buy_id,
                entry_session="PRE_SNIPER_REARM",
                entry_time=time.time(),
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"<b>{trigger_title} 1차 매수 덫 취소 ➡️ 현재가 신규 요격 발사</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 재장전 지정가(LIMIT): <code>${old_trap_price:.2f}</code> ➡️ <code>${rearm_price:.2f}</code> (<code>{rearm_qty}주</code>)\n"
                "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 하이킨 아시 트레일링(+1.00% 점화 · QQQ실시간 급변 바이패스) 자동 결속</code>"
            )
            return new_buy_id, "PRE_SNIPER_REARM"

        await AssassinLedger.save_state(
            symbol, buy_order_id="", is_session_done=False, entry_session="", entry_time=0.0
        )
        await AssassinLedger.unlock_all_symbols_for_reentry()
        return "", ""
    except Exception as e:
        print(f"🚨 [조건부 30초 타임쉴드 재장전 방어 {symbol}] {e}", flush=True)
        return buy_order_id, entry_session
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# 단일 '하이킨 아시 + 고점 트레일링 (🔥 HA_HYB_1.0 × 🚀 NQ_VEL_BYPASS)' 전용 매도 안전 캡 장전,
# 1주 휩소 작전(WHIPSAW_1SH) 체결 시 실체결 평단가 기준 +1.00% 매도 덫 장전,
# 스나이퍼 OFF 전환 시 구버전 QQQ 기준가 소각 및 체결/수동재결속 시점 실시간 QQQ 3차 방어 기준가 동기화 (Case 58, 79, 85, 89, 91, 92, 93, 94, 95)
async def manage_mechanical_profit_trap_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, current_session_id: str,
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

    # =====================================================================
    # 1. 보유 포지션 실시간 하이킨 아시(Heikin-Ashi) + 고점 트레일링(HA_HYB_1.0) + QQQ실시간 급등락 HA음봉 바이패스(🚀 NQ_VEL_BYPASS) 추적 및 즉시 익절 스윕
    # =====================================================================
    if holdings_qty > 0 and is_active and avg_price > 0.0 and entry_session != "WHIPSAW_1SH":
        live_p = current_price
        if live_p <= 0.0:
            try:
                live_p = await ctx.client.get_current_price(symbol)
            except Exception:
                live_p = 0.0

        arm_p = math.ceil(avg_price * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        floor_p = math.ceil(avg_price * (1.0 + TRAIL_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
        safety_cap_p = math.ceil(avg_price * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0

        sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol, {})
        ewm_st = sym_rsi_dict.get("ewm_state", {}) if isinstance(sym_rsi_dict, dict) else {}
        is_ha_bull, live_ha_o, live_ha_c = compute_realtime_tick_heikin_ashi(live_p, ewm_st)

        # 초과 Case 89 & Case 91 — 토스 Open API QQQ 0.0초 실시간(폴백 NQ=F) 10분/3분 변화율 조회 및 ±0.20% 급등락 바이패스 판정
        is_nq_favorable_surge, nq_vel_10m, nq_vel_3m = await MacroDataCache.is_nq_velocity_favorable_surge(
            symbol=symbol, surge_threshold=NQ_VELOCITY_SURGE_PCT
        )
        vel_src_label = MacroDataCache.get_cached_nq_velocity_source()

        t_state = _TRAIL_RUNTIME_CACHE.setdefault(symbol, {})
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
                    await ctx.notify_tg(
                        f"🔥 <b>[하이킨 아시 트레일링 점화] {sym_icon}{symbol} +1.00% 돌파 ➡️ 무제한 수익 추적 가동</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> ({holdings_qty}주) ➡️ 현재가: <code>${live_p:.2f}</code> (<code>+{cur_gain_pct:.2f}%</code>)\n"
                        f"▫️ 최소 익절 보존선: <code>${floor_p:.2f}</code> (+1.00% 하드 락온) | 실시간 HA(1m): <code>{ha_tag}</code>\n"
                        f"▫️ 추적 규칙: <code>0.4초 고주파 감시 · HA 양봉 지속(또는 {vel_src_label} ±{NQ_VELOCITY_SURGE_PCT:.2f}% 급변 바이패스) 시 무제한 홀딩 / 고점 대비 -{TRAIL_DROP_CALLBACK_PCT:.2f}% 콜백 또는 HA 음봉 전환 시 즉시 익절 스윕</code>"
                    )
                else:
                    if live_p > float(t_state.get("peak_price", 0.0)):
                        t_state["peak_price"] = live_p

            if t_state.get("armed", False):
                runtime_state["fast_shield_mode"] = True
                peak_p = max(float(t_state.get("peak_price", arm_p)), arm_p)
                trail_stop_p = math.floor(peak_p * (1.0 - TRAIL_DROP_CALLBACK_PCT / 100.0) * 100.0) / 100.0
                effective_stop_p = max(floor_p, trail_stop_p)
                t_state["stop_price"] = effective_stop_p

                elapsed_arm = time.time() - float(t_state.get("armed_ts", 0.0))
                armed_hm = int(t_state.get("armed_hm", cur_hm))

                # 1) 고점 대비 -0.30% 콜백 스탑은 바이패스 여부와 무관하게 상시 가동하여 확보된 고점 수익을 보호
                is_callback_stop = (
                    (peak_p > arm_p or elapsed_arm >= 2.5)
                    and live_p <= effective_stop_p
                )
                # 2) 초과 Case 89 & Case 91 — 하이킨 아시 1분봉 음봉 전환(not is_ha_bull)이더라도
                #    토스 QQQ 0.0초 실시간 변화율(10m/3m)이 포지션 유리 방향으로 ±0.20% 이상 급등·급락 진행 중(is_nq_favorable_surge)이면
                #    음봉 매도를 100% 바이패스(False)하고 폭등 추세를 끝까지 홀딩!
                is_ha_bear_reversal = (
                    not is_ha_bull
                    and not is_nq_favorable_surge
                    and (elapsed_arm >= 4.0 or cur_hm != armed_hm or peak_p >= arm_p * 1.002)
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
                        if bid_1 >= floor_p:
                            exec_sell_price = bid_1
                        elif cur_fl >= floor_p:
                            exec_sell_price = max(floor_p, cur_fl)
                        else:
                            exec_sell_price = max(min_profit_floor, bid_1 if bid_1 > 0.0 else cur_fl)

                        peak_pct = (peak_p / avg_price - 1.0) * 100.0
                        exec_pct = (exec_sell_price / avg_price - 1.0) * 100.0
                        if is_callback_stop:
                            exit_reason_str = (
                                f"최고가(<code>${peak_p:.2f} / +{peak_pct:.2f}%</code>) 대비 "
                                f"트레일링 스탑선(<code>${effective_stop_p:.2f}</code>) 도달"
                            )
                        else:
                            exit_reason_str = (
                                f"실시간 하이킨 아시 1분봉 음봉 전환 "
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
                                f"⚡ <b>[하이킨 아시 트레일링 익절 스윕] {sym_icon}{symbol} 0.1초 즉시 정정 타격</b>\n"
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
                    elif status in ["CANCELED", "REJECTED"]:
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
                # MODIFIED: 초과 Case 58, 79 & Case 95 — 03:58 ~ 04:07 EST 1주 휩소 작전 체결 시
                # 실제 매수 체결 평단가(avg_price) 대비 +1.00% 익절가(math.ceil(avg_price * 1.01 * 100) / 100.0)로 매도 주문 장전!
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
                    trap_tag = "프리장 1차 슬리피지 매수덫 체결(🔥HA·트레일링 결속)"
                elif entry_session == "PRE_SNIPER_REARM":
                    trap_tag = "프리장 현재가 요격 체결(🔥HA·트레일링 결속)"
                else:
                    trap_tag = f"수동 스나이퍼 개입({holdings_qty}주 · 🔥HA·트레일링 결속)"

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
                    not buy_order_id and entry_session not in ("PRE_SNIPER", "PRE_SNIPER_REARM", "WHIPSAW_1SH")
                )

                if not is_rearm:
                    # 초과 Case 92 & Case 94 —
                    # 1) 3차 동적 임계치 산출용 진폭(nq_amp_now)은 나스닥 100 선물(NQ=F)에서 조회!
                    # 2) 해당 임계치를 적용할 3차 진입 기준가(ref_p_now)는 토스 Open API QQQ 0.0초 실시간 현재가로 동기화!
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

                    # 초과 Case 94 ② — 수동 보유 물량이 안전캡을 새로 장전할 때는
                    # 항상 결속 시점의 실시간 QQQ 현재가(ref_p_now)로 동기화하여 역행률 0.00% 출발 보장!
                    eff_nq_p = ref_p_now if ref_p_now > 0.0 else saved_nq_p
                    eff_nq_a = nq_amp_now if nq_amp_now > 0.0 else saved_nq_a

                    kwargs = {
                        "price": avg_price, "target_sell_price": ledger_target_price,
                        "sell_order_id": new_sell_id, "is_session_done": True,
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
                                f"(기준 {ref_unit}{eff_nq_p:.2f} / NQ진폭 {eff_nq_a:.2f}% 기반 한도 +{dyn_th:.2f}%) 전용 가동</code>"
                            )
                        else:
                            defense_desc = (
                                f"<code>2차 NQ=F 과열방어(≥2.0%) · 3차 {ref_src_now} 역행방어"
                                f"(기준 {ref_unit}{eff_nq_p:.2f} / NQ진폭 {eff_nq_a:.2f}% 기반 한도 +{dyn_th:.2f}%) &amp; 최저점 반등(RE_BOT_TRAIL) 가동 중</code>"
                            )
                        await ctx.notify_tg(
                            f"🟢 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] {trap_tag} 상단 안전 캡 &amp; 트레일링 결속 완료</b>\n"
                            f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 <code>{trap_qty}주</code>)\n"
                            f"▫️ 트레일링 점화가(+1.00%): <code>${ledger_target_price:.2f}</code> (보존선 기준 최소 실순수익 <code>+${min_net_usd:,.2f}</code> ~ 무제한 추적)\n"
                            f"▫️ 거래소 안전 캡(+5.00% LIMIT): <code>${order_limit_price:.2f}</code> 선장전 완료\n"
                            f"▫️ 매크로 방어: {defense_desc}\n"
                            f"▫️ 작동 방식: <code>${ledger_target_price:.2f}(+1.00%) 돌파 즉시 HA 1분봉 양봉 홀딩(QQQ실시간 ±{NQ_VELOCITY_SURGE_PCT:.2f}% 급변 시 HA음봉 바이패스) &amp; 고점 대비 -{TRAIL_DROP_CALLBACK_PCT:.2f}% 콜백/HA음봉 시 0.1초 정정 익절 스윕</code>"
                        )
                else:
                    if is_manual_trap:
                        mark_manual_stage3_anchored(symbol, new_sell_id)
                    await AssassinLedger.save_state(symbol, sell_order_id=new_sell_id)

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
