# =====================================================================
# FILE: sniper_plugin.py
# 목적: 03:58 EST 숏 단방향 스나이퍼 및 24시간 RSI 20-10-30 패닉 셀 레이더 상태 머신
# =====================================================================
# MODIFIED: 스나이핑 골든타임을 데이장 마감 2분 전(03:58 EST)으로 영구 하드 락온

import math
import time
import asyncio
import html
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from aiogram import Bot

from toss_api import TossApiClient
from quant_engine import AssassinLedger, MacroDataCache

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
    current_level = state["level"]

    if rsi_val >= 30.0:
        if current_level != 0:
            print(f"♻️ [RSI 레이더 {symbol}] {rsi_val:.1f} 회복. 경보 상태 머신 릴리스 (Level 0).", flush=True)
            state["level"] = 0
        return
        
    is_under_vwap = current_price < vwap_val
    
    if is_under_vwap:
        if rsi_val <= 10.0 and current_level == 1:
            state["level"] = 2
            await ctx.notify_tg(
                f"🚨 <b>[패닉 셀 심각 {symbol}] 2차 폭포수 과매도 포착</b>\n"
                f"▫️ 실시간 RSI(1m): <code>{rsi_val:.1f}</code> (10 붕괴)\n"
                f"▫️ VWAP 하방 이탈: <code>${current_price:.2f} &lt; ${vwap_val:.2f}</code>\n"
                "▫️ 극단적 휩소 우려 (스팸 타전 락온)"
            )
        elif rsi_val <= 20.0 and current_level == 0:
            state["level"] = 1
            await ctx.notify_tg(
                f"⚠️ <b>[패닉 셀 경고 {symbol}] 1차 투매 구간 진입 포착</b>\n"
                f"▫️ 실시간 RSI(1m): <code>{rsi_val:.1f}</code> (20 붕괴)\n"
                f"▫️ VWAP 하방 이탈: <code>${current_price:.2f} &lt; ${vwap_val:.2f}</code>\n"
                "▫️ (스팸 타전 락온. RSI 10 붕괴 또는 30 회복 전까지 타전 금지)"
            )

async def execute_moc_zero_overnight_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, is_active: bool,
    sell_order_id: str, runtime_state: dict,
) -> bool:
    is_reg_moc = (now_est.hour == 15 and now_est.minute == 59) or (
        now_est.hour == 16 and 0 <= now_est.minute <= 1
    )
    if not (is_reg_moc and is_active):
        runtime_state["moc_dump_active"] = False
        return False

    if not ctx.in_memory_ordering_lock[symbol]:
        current_time = time.time()
        if current_time - runtime_state["last_moc_tick"] >= 1.5:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                if sell_order_id:
                    try:
                        await ctx.client.cancel_order(sell_order_id)
                    except Exception as e:
                        print(f"🚨 [MOC 매도 주문 취소 방어 {symbol}] {e}", flush=True)
                    finally:
                        await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)

                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                cancel_issued = False
                for order in open_orders:
                    try:
                        await ctx.client.cancel_order(order["orderId"])
                        cancel_issued = True
                    except Exception as e:
                        print(f"🚨 [MOC 미체결 취소 방어 {symbol}] {e}", flush=True)

                if cancel_issued:
                    await asyncio.sleep(0.5)
                    ctx.idempotency_keys[symbol]["MOC"] = None

                holdings_detail_moc = await ctx.client.get_symbol_holdings_detail(symbol)
                dump_qty = int(math.floor(holdings_detail_moc["qty"]))

                if dump_qty > 0:
                    sellable_q = await ctx.client.get_sellable_quantity(symbol)
                    sellable_q_int = int(math.floor(sellable_q))
                    if sellable_q_int > 0:
                        dump_qty = min(dump_qty, sellable_q_int)
                        current_price = await ctx.client.get_current_price(symbol)
                        orderbook = await ctx.client.get_orderbook(symbol)
                        bids = orderbook.get("bids", [])
                        bid_1_price = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else current_price

                        if bid_1_price > 0.0:
                            idem = ctx.idempotency_keys[symbol]["MOC"]
                            if not idem:
                                idem = {
                                    "key": f"MOC_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
                                    "qty": dump_qty,
                                    "price": bid_1_price,
                                }
                                ctx.idempotency_keys[symbol]["MOC"] = idem
                            else:
                                dump_qty = idem["qty"]
                                bid_1_price = idem["price"]

                            try:
                                await asyncio.wait_for(
                                    ctx.client.create_order(
                                        symbol=symbol, side="SELL", order_type="LIMIT",
                                        quantity=dump_qty, price=f"{bid_1_price:.2f}",
                                        client_order_id=idem["key"],
                                    ), timeout=2.0,
                                )
                                print(f"🔴 [MOC 덤핑 스윕 {symbol}] 수량: {dump_qty}주 | 단가: ${bid_1_price:.2f}", flush=True)
                                await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
                                if not runtime_state["moc_dump_active"]:
                                    runtime_state["moc_dump_active"] = True
                                    await ctx.notify_tg(
                                        f"🔴 <b>[스나이퍼 {symbol}] 15:59~16:01 제로-오버나이트 덤핑망 결속</b>\n"
                                        "▫️ 미체결 주문 선취소 완료 및 1.5초 주기 매수 1호가 스윕 폭격 개시"
                                    )
                                ctx.idempotency_keys[symbol]["MOC"] = None
                            except asyncio.TimeoutError:
                                print(f"🚨 [MOC 타임아웃 방어 {symbol}] 2초 초과.", flush=True)
                else:
                    await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
            except Exception as e:
                err_str = str(e)
                print(f"🚨 [MOC 방어 {symbol}] {err_str}", flush=True)
                if any(code in err_str for code in ["400", "422", "404", "409"]):
                    ctx.idempotency_keys[symbol]["MOC"] = None
            finally:
                runtime_state["last_moc_tick"] = time.time()
                ctx.in_memory_ordering_lock[symbol] = False
    return True

async def execute_macro_defense_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, is_active: bool,
    is_session_done: bool, holdings_qty: int, current_price: float, avg_price_dump: float,
    sell_order_id: str, nq_entry_price: float, nq_entry_amp: float,
) -> bool:
    if not is_active or ctx.in_memory_ordering_lock[symbol]:
        return False

    nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
    nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

    dynamic_threshold = max(nq_entry_amp * 0.5, ctx.nq_dynamic_floor) if nq_entry_amp > 0.0 else 0.0
    nq_diff_pct = (((nq_c_global - nq_entry_price) / nq_entry_price * 100.0) if nq_entry_price > 0.0 else 0.0)

    is_macro_breach = nq_amp_global >= 2.0

    is_dynamic_breach = (
        nq_entry_price > 0.0
        and dynamic_threshold > 0.0
        and nq_c_global > 0.0
        and nq_diff_pct >= dynamic_threshold
    )

    if is_macro_breach and not is_session_done:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            open_orders_macro = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for order in open_orders_macro:
                if order.get("side") == "BUY":
                    try:
                        await ctx.client.cancel_order(order["orderId"])
                    except Exception as e:
                        pass
            await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
            print(f"🛑 [매크로 진입 차단 {symbol}] NQ=F 진폭 {nq_amp_global:.2f}% 도달. 출동 취소.", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    if (is_macro_breach or is_dynamic_breach) and holdings_qty > 0 and avg_price_dump > 0.0 and 0.0 < current_price < avg_price_dump:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            if sell_order_id:
                try:
                    await ctx.client.cancel_order(sell_order_id)
                except Exception:
                    pass
                finally:
                    await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                    await asyncio.sleep(0.5)

            open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            cancel_issued = False
            for order in open_orders:
                if order.get("side") == "SELL":
                    try:
                        await ctx.client.cancel_order(order["orderId"])
                        cancel_issued = True
                    except Exception:
                        pass
            if cancel_issued:
                await asyncio.sleep(0.5)
                ctx.idempotency_keys[symbol]["M_DUMP"] = None

            holdings_detail_mdump = await ctx.client.get_symbol_holdings_detail(symbol)
            dump_qty = int(math.floor(holdings_detail_mdump["qty"]))

            if dump_qty > 0:
                sellable_q = await ctx.client.get_sellable_quantity(symbol)
                sellable_q_int = int(math.floor(sellable_q))
                if sellable_q_int > 0:
                    dump_qty = min(dump_qty, sellable_q_int)
                    orderbook = await ctx.client.get_orderbook(symbol)
                    bids = orderbook.get("bids", [])
                    bid_1_price = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else current_price

                    if bid_1_price > 0.0:
                        idem = ctx.idempotency_keys[symbol]["M_DUMP"]
                        is_new_dump = idem is None
                        if not idem:
                            idem = {
                                "key": f"MDUMP_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
                                "qty": dump_qty,
                                "price": bid_1_price,
                            }
                            ctx.idempotency_keys[symbol]["M_DUMP"] = idem
                        else:
                            dump_qty = idem["qty"]
                            bid_1_price = idem["price"]

                        await asyncio.wait_for(
                            ctx.client.create_order(
                                symbol=symbol, side="SELL", order_type="LIMIT",
                                quantity=dump_qty, price=f"{bid_1_price:.2f}",
                                client_order_id=idem["key"],
                            ), timeout=2.0,
                        )
                        await AssassinLedger.save_state(symbol, is_session_done=True)

                        if is_new_dump:
                            if is_macro_breach:
                                reason_msg = f"NQ=F 당일 전체 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 초과 확증)"
                            else:
                                bind_str = " · 하한선 바인딩" if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor) else ""
                                reason_msg = f"체결 기준 지수({nq_entry_price:.2f}) 대비 <code>{nq_diff_pct:+.2f}%</code> 상승(숏 역행) (동적 임계치 {dynamic_threshold:.2f}%{bind_str} 이탈)"
                            await ctx.notify_tg(
                                f"🚨 <b>[스나이퍼 {symbol}] 대세장 이탈 손실 덤핑 격발</b>\n"
                                f"▫️ 사유: {reason_msg}\n"
                                "▫️ 손익 상태: 마이너스 (계좌 보호 발동)\n"
                                "▫️ 조치: 지정가 매도 덫 파기 및 전량 매수 1호가(LIMIT) 덤핑 퇴근"
                            )
                        ctx.idempotency_keys[symbol]["M_DUMP"] = None
        except Exception as e:
            err_str = str(e)
            if any(code in err_str for code in ["400", "422", "404", "409"]):
                ctx.idempotency_keys[symbol]["M_DUMP"] = None
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return True
    return False

# MODIFIED: 03:59 컷오프
async def enforce_post_0358_cutoff_plugin(symbol: str, est_time_int: int, is_session_done: bool) -> bool:
    if 359 <= est_time_int < 1600 and not is_session_done:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        print(f"🔒 [03:59 골든타임 마감 {symbol}] 당일 추가 신규 매수 영구 차단.", flush=True)
        return True
    return is_session_done

async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext, symbol: str, last_buy_price: float, buy_order_id: str, sell_order_id: str,
):
    if ctx.in_memory_ordering_lock[symbol]: return
    can_clear = True
    if buy_order_id:
        try:
            od = await ctx.client.get_order_detail(buy_order_id)
            st = od.get("status", "")
            if st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                can_clear = False
        except Exception:
            can_clear = False

    if not can_clear: return

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        exit_price, filled_qty = 0.0, 0.0
        try:
            orders = await ctx.client.get_orders(status="CLOSED", symbol=symbol)
            for o in orders:
                if o.get("side") == "SELL" and o.get("status") in ["FILLED", "PARTIAL_FILLED"]:
                    exec_info = o.get("execution", {})
                    avg_p = float(exec_info.get("averageFilledPrice") or 0.0)
                    f_qty = float(exec_info.get("filledQuantity") or 0.0)
                    if avg_p > 0.0 and f_qty > 0.0:
                        exit_price, filled_qty = avg_p, f_qty
                        break
        except Exception:
            pass

        entry_price = last_buy_price
        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            pnl_amt = (exit_price * filled_qty) - (entry_price * filled_qty)
            pnl_rate = (exit_price / entry_price - 1.0) * 100.0
            sign = "+" if pnl_amt > 0 else ""
            pnl_str = f"\n▫️ 타점: 매수 ${entry_price:.2f} ➡️ 매도 ${exit_price:.2f}\n▫️ 손익: {sign}{pnl_rate:.2f}% ({sign}${pnl_amt:.2f})"
        else:
            pnl_str = "\n▫️ 타점: 체결 데이터 추출 지연 (장부 참조 요망)"

        now_est_check = datetime.now(EST_TZ)
        is_moc_time = (now_est_check.hour == 15 and now_est_check.minute >= 59) or (now_est_check.hour == 16 and 0 <= now_est_check.minute <= 5)
        is_take_profit_exit, is_trap_survived = False, False

        if sell_order_id:
            try:
                od = await ctx.client.get_order_detail(sell_order_id)
                st = od.get("status", "")
                if st == "FILLED": is_take_profit_exit = True
                elif st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]: is_trap_survived = True
                await ctx.client.cancel_order(sell_order_id)
                await asyncio.sleep(0.5)
            except Exception: pass

        was_holding = last_buy_price > 0.0
        is_manual_exit = (was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived)
        is_buy_cancel_exit = (not was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived)

        await AssassinLedger.save_state(
            symbol, price=0.0, target_sell_price=0.0, buy_order_id="", sell_order_id="",
            is_session_done=True, entry_session="", entry_time=0.0,
            nq_entry_price=0.0, nq_entry_amp=0.0, entry_trend="",
        )

        if is_buy_cancel_exit:
            print(f"🧹 [매수 취소/실패 {symbol}] 허위 익절 타전 억제 및 장부 초기화 완료.", flush=True)
        elif is_moc_time:
            await ctx.notify_tg(f"🛑 <b>[스나이퍼 {symbol}] MOC 강제 덤핑 청산 완료</b>\n▫️ 잔고 0주 (제로-오버나이트 락온){pnl_str}")
        elif is_take_profit_exit:
            await ctx.notify_tg(f"🎉 <b>[스나이퍼 {symbol}] 익절 타격 성공 (당일 퇴근 완료)</b>\n▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}\n▫️ 당일 신규 진입 권한 영구 소각")
        elif is_manual_exit:
            await ctx.notify_tg(f"🛑 <b>[스나이퍼 {symbol}] 수동 매도 청산 감지 (퇴근 락온 완료)</b>\n▫️ 잔고 0주 (수동 청산 식별){pnl_str}\n▫️ 당일 신규 진입 권한 영구 소각 완료")
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

# MODIFIED: 03:58 스나이퍼
async def execute_0358_sniper_strike_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, current_session_id: str,
    current_price: float, budget: float, target_profit_rate: float,
):
    if ctx.in_memory_ordering_lock[symbol]: return

    async with ctx.dual_strike_mutex:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            if self_state[4] or await AssassinLedger.get_buy_order_id(symbol) != "" or ctx.shared_holdings.get(symbol, 0) > 0:
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            _, _, _, nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_nq_sniper_signal(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                await AssassinLedger.save_state(symbol, is_session_done=True, last_session_id=current_session_id)
                await ctx.notify_tg(
                    f"🛑 <b>[03:58 스나이퍼] 대세장 이탈 감지 (진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 당일 매수 권한 영구 소각 및 관망 퇴근"
                )
                return

            my_hold_check = await ctx.client.get_symbol_holdings_detail(symbol)
            if int(math.floor(my_hold_check.get("qty", 0.0))) > 0:
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.995)
            except Exception:
                actual_budget = budget

            target_price = current_price
            target_qty = int(math.floor(actual_budget / target_price))

            if target_qty <= 0:
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            idem = ctx.idempotency_keys[symbol]["BUY"]
            if not idem:
                idem = {
                    "key": f"BUY_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
                    "qty": target_qty,
                    "price": target_price,
                }
                ctx.idempotency_keys[symbol]["BUY"] = idem
            else:
                target_qty = idem["qty"]
                target_price = idem["price"]

            res = await ctx.client.create_order(
                symbol=symbol, side="BUY", order_type="LIMIT",
                quantity=target_qty, price=f"{target_price:.2f}", client_order_id=idem["key"],
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                # MODIFIED: 진입 세션 플래그 반영 (0358)
                await AssassinLedger.save_state(
                    symbol, buy_order_id=new_buy_id, entry_session="DAY_0358", entry_trend="SHORT",
                    entry_time=time.time(), nq_entry_price=nq_c_global, nq_entry_amp=nq_amp_global,
                    is_session_done=True, last_session_id=current_session_id,
                )
                ctx.wakeup_event.set()
                await ctx.notify_tg(
                    f"🎯 <b>[03:58 데이장 마감 스나이퍼] {symbol} 단방향 매수 발사 완료</b>\n"
                    f"▫️ 타격 지정가(현재가): <code>${target_price:.2f}</code> (시드기준 <code>{target_qty}주</code>)\n"
                    f"▫️ 후속 프로토콜: <code>전량 체결 확인 즉시 +{target_profit_rate}% 매도 덫 장전</code>"
                )
            ctx.idempotency_keys[symbol]["BUY"] = None
        except Exception as e:
            err_str = str(e)
            print(f"🚨 [03:58 스나이퍼 타격 방어 {symbol}] {err_str}", flush=True)
            if any(code in err_str for code in ["400", "422", "404", "409", "401", "403", "429"]):
                ctx.idempotency_keys[symbol]["BUY"] = None
                if "400" in err_str or "422" in err_str:
                    await AssassinLedger.save_state(symbol, is_session_done=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

async def manage_mechanical_profit_trap_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, current_session_id: str,
    is_active: bool, just_turned_off: bool, holdings_qty: int, holdings_detail: dict,
    last_buy_price: float, target_profit_rate: float, target_sell_price: float,
    buy_order_id: str, sell_order_id: str, entry_session: str, runtime_state: dict,
):
    open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
    has_open_sell = any(o.get("side") == "SELL" for o in open_orders)
    has_open_buy = any(o.get("side") == "BUY" for o in open_orders)

    if not is_active:
        if sell_order_id and not ctx.in_memory_ordering_lock[symbol]:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                await ctx.client.cancel_order(sell_order_id)
            except Exception: pass
            finally:
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                await asyncio.sleep(0.5)
                ctx.in_memory_ordering_lock[symbol] = False
        return

    if holdings_qty > 0 and sell_order_id and is_active:
        try:
            od = await ctx.client.get_order_detail(sell_order_id)
            status_trap = od.get("status", "")
            if status_trap in ["CANCELED", "REJECTED"]:
                await AssassinLedger.save_state(symbol, sell_order_id="")
                sell_order_id = ""
        except Exception as e:
            if "404" in str(e).lower() or "not-found" in str(e).lower():
                await AssassinLedger.save_state(symbol, sell_order_id="")
                sell_order_id = ""

    if (holdings_qty > 0 and not has_open_sell and not has_open_buy and not sell_order_id and not ctx.in_memory_ordering_lock[symbol] and is_active):
        calculated_target = target_sell_price
        avg_price = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price
        is_rearm, trap_tag, skip_trap = True, "", False

        if calculated_target <= 0.0:
            if buy_order_id:
                try:
                    order_detail = await ctx.client.get_order_detail(buy_order_id)
                    status = order_detail.get("status", "")
                    ordered_qty = int(math.floor(float(order_detail.get("quantity") or 0.0)))
                    exec_info = order_detail.get("execution") or {}
                    filled_qty = int(math.floor(float(exec_info.get("filledQuantity") or 0.0)))

                    if status in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]: skip_trap = True
                    elif status == "FILLED":
                        if ordered_qty > 0 and (filled_qty < ordered_qty or holdings_qty < ordered_qty): skip_trap = True
                        else: is_rearm = False
                    elif status in ["CANCELED", "REJECTED"]: is_rearm = False
                    else: skip_trap = True
                except Exception:
                    skip_trap = True
            else:
                is_rearm = False

            if skip_trap: return

            if avg_price > 0.0:
                calculated_target = math.ceil(avg_price * (1.0 + target_profit_rate / 100.0) * 100) / 100.0
                trap_tag = f"03:58 스나이퍼(+{target_profit_rate}%)" if (entry_session == "DAY_0358" and buy_order_id) else f"수동개입(+{target_profit_rate}%)"

        trap_qty = holdings_qty
        if calculated_target > 0.0 and trap_qty > 0 and not skip_trap:
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
                        "price": calculated_target,
                    }
                    ctx.idempotency_keys[symbol]["TRAP"] = idem
                else:
                    trap_qty = idem["qty"]
                    calculated_target = idem["price"]

                res = await ctx.client.create_order(
                    symbol=symbol, side="SELL", order_type="LIMIT",
                    quantity=trap_qty, price=f"{calculated_target:.2f}", client_order_id=idem["key"],
                )

                new_sell_id = ""
                if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                    new_sell_id = str(res["result"]["orderId"])

                if not is_rearm:
                    nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                    nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
                    
                    kwargs = {
                        "price": avg_price, "target_sell_price": calculated_target,
                        "sell_order_id": new_sell_id, "is_session_done": True,
                    }
                    
                    if not buy_order_id:
                        kwargs.update({
                            "entry_session": "MANUAL", "entry_trend": "SHORT",
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now
                        })
                    else:
                        kwargs.update({
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now
                        })
                        
                    await AssassinLedger.save_state(symbol, **kwargs)

                    await ctx.notify_tg(
                        f"🟢 <b>[스나이퍼 {symbol}] {trap_tag} 지정가 매도 덫 장전 완료</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 {trap_qty}주)\n"
                        f"▫️ 목표 익절가: <code>${calculated_target:.2f}</code> (+{target_profit_rate}% 지정가)\n"
                        f"▫️ 상태: <code>안전 익절 대기 락온</code>"
                    )
                else:
                    await AssassinLedger.save_state(symbol, sell_order_id=new_sell_id)

                ctx.idempotency_keys[symbol]["TRAP"] = None
                runtime_state["last_trap_error_msg"] = ""
            except Exception as e:
                err_str = str(e)
                if any(code in err_str for code in ["400", "422", "404", "409", "401", "403"]):
                    ctx.idempotency_keys[symbol]["TRAP"] = None
                if err_str != runtime_state["last_trap_error_msg"]:
                    await ctx.notify_tg(f"🚨 <b>[TRAP 에러 {symbol}]</b> {html.escape(err_str)}")
                    runtime_state["last_trap_error_msg"] = err_str
                await asyncio.sleep(5.0)
            finally:
                ctx.in_memory_ordering_lock[symbol] = False
