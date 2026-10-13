# =====================================================================
# FILE: sniper_plugin.py
# 목적: main.py와 플러그인 방식으로 연결되는 09:30 양방향(SOXL/SOXS) 스나이퍼 전술 실행 모듈
# 포함 기능:
#   1) 09:30:00 EST 정규장 개장 NQ=F 10분봉(09:20~09:30) 추세 기반 양방향 배타적 1회 타격
#   2) 09:31~15:58 EST 골든타임 컷오프 (장중 뒤늦은 추격 매수 영구 차단 & 09:30 미체결 주문 유지)
#   3) NQ=F 매크로 1·2·3차 양방향 대칭 방어선 (2.0% 이탈 및 0.8% 하한 동적 덤핑)
#   4) 팩트 평단가(averagePurchasePrice) 기준 +1.0% 지정가 익절 덫 장전 및 체결 시점 NQ 기준점 동기화
#   5) 15:59~16:01 EST 제로-오버나이트 MOC 매수 1호가 1.5초 주기 고주파 강제 청산
# =====================================================================
# MODIFIED: 제3헌법 준수 - 미사용 데드코드 함수(fetch_full_session_candles) 및 불필요 임포트 영구 소각

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
    """main.py 코어와 sniper_plugin.py 전술 모듈을 연결하는 플러그인 런타임 컨텍스트"""

    client: TossApiClient
    bot: Bot
    chat_id: int
    wakeup_event: asyncio.Event
    in_memory_ordering_lock: dict
    dual_strike_mutex: asyncio.Lock
    shared_holdings: dict
    idempotency_keys: dict
    nq_dynamic_floor: float = 0.8

    async def notify_tg(self, text: str):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML")
        except Exception as e:
            print(f"🚨 [텔레그램 전송 붕괴 방어] {e}", flush=True)


# =====================================================================
# 1. [플러그인] 15:59~16:01 EST 제로-오버나이트 MOC 강제 청산망 (Case 16, 24, 48, 71)
# =====================================================================
async def execute_moc_zero_overnight_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    sell_order_id: str,
    runtime_state: dict,
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
                        await AssassinLedger.save_state(
                            symbol, sell_order_id="", target_sell_price=0.0
                        )

                # Case 48: 09:30 미체결 매수 주문 및 미체결 매도 주문 전량 선취소
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
                    # Case 71: 매도 가능 수량 사전 프로빙
                    sellable_q = await ctx.client.get_sellable_quantity(symbol)
                    sellable_q_int = int(math.floor(sellable_q))
                    if sellable_q_int > 0:
                        dump_qty = min(dump_qty, sellable_q_int)
                        current_price = await ctx.client.get_current_price(symbol)
                        orderbook = await ctx.client.get_orderbook(symbol)
                        bids = orderbook.get("bids", [])
                        bid_1_price = (
                            float(bids[0]["price"])
                            if bids and float(bids[0]["price"]) > 0.0
                            else current_price
                        )

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
                                        symbol=symbol,
                                        side="SELL",
                                        order_type="LIMIT",
                                        quantity=dump_qty,
                                        price=f"{bid_1_price:.2f}",
                                        client_order_id=idem["key"],
                                    ),
                                    timeout=2.0,
                                )
                                print(
                                    f"🔴 [MOC 덤핑 스윕 {symbol}] 수량: {dump_qty}주 | 단가: ${bid_1_price:.2f}",
                                    flush=True,
                                )
                                await AssassinLedger.save_state(
                                    symbol, buy_order_id="", is_session_done=True
                                )
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


# =====================================================================
# 2. [플러그인] NQ=F 매크로 2·3차 양방향 대칭 방어선 덤핑 (Case 66, 67, 69, 71)
# =====================================================================
async def execute_macro_defense_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    current_price: float,
    avg_price_dump: float,
    sell_order_id: str,
    nq_entry_price: float,
    nq_entry_amp: float,
) -> bool:
    if not is_active or ctx.in_memory_ordering_lock[symbol]:
        return False

    nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
    nq_amp_global = (
        ((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0
    )

    dynamic_threshold = (
        max(nq_entry_amp * 0.5, ctx.nq_dynamic_floor) if nq_entry_amp > 0.0 else 0.0
    )
    nq_diff_pct = (
        ((nq_c_global - nq_entry_price) / nq_entry_price * 100.0)
        if nq_entry_price > 0.0
        else 0.0
    )

    is_macro_breach = nq_amp_global >= 2.0

    # NEW: Case 67 양방향 대칭 3차 방어선 (SOXS 숏은 NQ 상승 시 덤핑 / SOXL 롱은 NQ 하락 시 덤핑)
    if symbol == "SOXS":
        is_dynamic_breach = (
            nq_entry_price > 0.0
            and dynamic_threshold > 0.0
            and nq_c_global > 0.0
            and nq_diff_pct >= dynamic_threshold
        )
    else:
        is_dynamic_breach = (
            nq_entry_price > 0.0
            and dynamic_threshold > 0.0
            and nq_c_global > 0.0
            and nq_diff_pct <= -dynamic_threshold
        )

    # 2차 방어선: 당일 NQ 진폭 2.0% 이상 시 미체결 매수 주문 취소 및 신규 진입 차단
    if is_macro_breach and not is_session_done:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            open_orders_macro = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for order in open_orders_macro:
                if order.get("side") == "BUY":
                    try:
                        await ctx.client.cancel_order(order["orderId"])
                    except Exception as e:
                        print(f"🚨 [매크로 매수 취소 방어 {symbol}] {e}", flush=True)
            await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
            print(
                f"🛑 [매크로 진입 차단 {symbol}] NQ=F 진폭 {nq_amp_global:.2f}% 도달. 미체결 매수 파기.",
                flush=True,
            )
        except Exception as e:
            print(f"🚨 [매크로 매수 파기 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 2·3차 방어선: 보유 중 손실권(current_price < avg_price_dump)일 때 즉시 매수 1호가 덤핑
    if (
        (is_macro_breach or is_dynamic_breach)
        and holdings_qty > 0
        and avg_price_dump > 0.0
        and 0.0 < current_price < avg_price_dump
    ):
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            if sell_order_id:
                try:
                    await ctx.client.cancel_order(sell_order_id)
                except Exception as e:
                    print(f"🚨 [매크로 매도 덫 취소 방어 {symbol}] {e}", flush=True)
                finally:
                    await AssassinLedger.save_state(
                        symbol, sell_order_id="", target_sell_price=0.0
                    )
                    await asyncio.sleep(0.5)

            open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            cancel_issued = False
            for order in open_orders:
                if order.get("side") == "SELL":
                    try:
                        await ctx.client.cancel_order(order["orderId"])
                        cancel_issued = True
                    except Exception as e:
                        print(f"🚨 [매크로 추가 매도 취소 방어 {symbol}] {e}", flush=True)

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
                    bid_1_price = (
                        float(bids[0]["price"])
                        if bids and float(bids[0]["price"]) > 0.0
                        else current_price
                    )

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
                                symbol=symbol,
                                side="SELL",
                                order_type="LIMIT",
                                quantity=dump_qty,
                                price=f"{bid_1_price:.2f}",
                                client_order_id=idem["key"],
                            ),
                            timeout=2.0,
                        )
                        await AssassinLedger.save_state(symbol, is_session_done=True)

                        if is_new_dump:
                            if is_macro_breach:
                                reason_msg = f"NQ=F 당일 전체 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 초과 확증)"
                            else:
                                bind_str = (
                                    " · 하한선 바인딩"
                                    if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor)
                                    else ""
                                )
                                dir_word = "상승(숏 역행)" if symbol == "SOXS" else "하락(롱 역행)"
                                reason_msg = (
                                    f"체결 기준 지수({nq_entry_price:.2f}) 대비 <code>{nq_diff_pct:+.2f}%</code> {dir_word} "
                                    f"(동적 임계치 {dynamic_threshold:.2f}%{bind_str} 이탈)"
                                )
                            await ctx.notify_tg(
                                f"🚨 <b>[스나이퍼 {symbol}] 대세장 이탈 손실 덤핑 격발</b>\n"
                                f"▫️ 사유: {reason_msg}\n"
                                "▫️ 손익 상태: 마이너스 (계좌 보호 발동)\n"
                                "▫️ 조치: 지정가 매도 덫 파기 및 전량 매수 1호가(LIMIT) 덤핑 퇴근"
                            )
                        ctx.idempotency_keys[symbol]["M_DUMP"] = None
        except Exception as e:
            err_str = str(e)
            print(f"🚨 [매크로 덤핑 방어 {symbol}] {err_str}", flush=True)
            if any(code in err_str for code in ["400", "422", "404", "409"]):
                ctx.idempotency_keys[symbol]["M_DUMP"] = None
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return True

    return False


# =====================================================================
# 3. [플러그인] 09:31~15:58 EST 골든타임 컷오프 강제 (Case 48, Case 53)
# =====================================================================
async def enforce_post_0930_cutoff_plugin(
    symbol: str, est_time_int: int, is_session_done: bool
) -> bool:
    """
    09:30 EST 오프닝 스나이핑 윈도우(09:30:00~09:30:59 EST) 경과 즉시(09:31 EST~)
    신규 매수 권한(is_session_done = True)을 영구 소각하여 장중 추격 매수를 원천 차단한다.
    단, 09:30에 정상 발사된 미체결 매수 주문(buy_order_id)은 취소하지 않고 15:58까지 유지한다 (Case 48).
    """
    if 931 <= est_time_int < 1600 and not is_session_done:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        print(
            f"🔒 [09:31 골든타임 마감 {symbol}] 09:30 스나이핑 윈도우 종료. 당일 추가 신규 매수 영구 차단.",
            flush=True,
        )
        return True
    return is_session_done


# =====================================================================
# 4. [플러그인] 잔고 0주 전환 시 익절/MOC/수동청산 판별 및 타전 (Case 46, 49, 51)
# =====================================================================
async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    last_buy_price: float,
    buy_order_id: str,
    sell_order_id: str,
):
    if ctx.in_memory_ordering_lock[symbol]:
        return

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
        return

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
        except Exception as e:
            print(f"🚨 [매도 정보 추출 방어 {symbol}] {e}", flush=True)

        entry_price = last_buy_price
        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            principal = entry_price * filled_qty
            gross = exit_price * filled_qty
            pnl_amt = gross - principal
            pnl_rate = (exit_price / entry_price - 1.0) * 100.0
            sign = "+" if pnl_amt > 0 else ""
            pnl_str = (
                f"\n▫️ 타점: 매수 ${entry_price:.2f} ➡️ 매도 ${exit_price:.2f}"
                f"\n▫️ 손익: {sign}{pnl_rate:.2f}% ({sign}${pnl_amt:.2f})"
            )
        else:
            pnl_str = "\n▫️ 타점: 체결 데이터 추출 지연 (장부 참조 요망)"

        now_est_check = datetime.now(EST_TZ)
        is_moc_time = (now_est_check.hour == 15 and now_est_check.minute >= 59) or (
            now_est_check.hour == 16 and 0 <= now_est_check.minute <= 5
        )

        is_take_profit_exit = False
        is_trap_survived = False

        if sell_order_id:
            try:
                od = await ctx.client.get_order_detail(sell_order_id)
                st = od.get("status", "")
                if st == "FILLED":
                    is_take_profit_exit = True
                elif st in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                    is_trap_survived = True
                await ctx.client.cancel_order(sell_order_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"🚨 [고아 덫 파기 방어 {symbol}] {e}", flush=True)

        was_holding = last_buy_price > 0.0
        is_manual_exit = (
            was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived
        )
        is_buy_cancel_exit = (
            not was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived
        )

        await AssassinLedger.save_state(
            symbol,
            price=0.0,
            target_sell_price=0.0,
            buy_order_id="",
            sell_order_id="",
            is_session_done=True,
            entry_session="",
            entry_time=0.0,
            nq_entry_price=0.0,
            nq_entry_amp=0.0,
            entry_trend="",
        )

        if is_buy_cancel_exit:
            print(f"🧹 [매수 취소/실패 {symbol}] 허위 익절 타전 억제 및 장부 초기화 완료.", flush=True)
        elif is_moc_time:
            await ctx.notify_tg(
                f"🛑 <b>[스나이퍼 {symbol}] MOC 강제 덤핑 청산 완료</b>\n"
                f"▫️ 잔고 0주 (제로-오버나이트 락온){pnl_str}"
            )
        elif is_take_profit_exit:
            await ctx.notify_tg(
                f"🎉 <b>[스나이퍼 {symbol}] 익절 타격 성공 (당일 퇴근 완료)</b>\n"
                f"▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}\n"
                "▫️ 당일 SOXL/SOXS 신규 진입 권한 영구 소각"
            )
        elif is_manual_exit:
            await ctx.notify_tg(
                f"🛑 <b>[스나이퍼 {symbol}] 수동 매도 청산 감지 (퇴근 락온 완료)</b>\n"
                f"▫️ 잔고 0주 (수동 청산 식별){pnl_str}\n"
                "▫️ 당일 SOXL/SOXS 신규 진입 권한 영구 소각 완료"
            )
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 5. [플러그인] 09:30:00 EST 정규장 개장 NQ=F 10분봉 양방향 스나이퍼 타격 (Case 15, 43, 52, 53, 54, 65)
# =====================================================================
async def execute_0930_sniper_strike_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    current_price: float,
    budget: float,
    target_profit_rate: float,
):
    if ctx.in_memory_ordering_lock[symbol]:
        return

    # Case 52 & Case 54: 양방향 동시 타격 경쟁(Race Condition) 원천 봉쇄를 위한 전역 뮤텍스 획득
    async with ctx.dual_strike_mutex:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            opposite_symbol = "SOXS" if symbol == "SOXL" else "SOXL"

            # 본인 및 반대 종목 장부/잔고 교차 검증 (일일 최대 1회 배타적 진입 강제)
            self_state = await AssassinLedger.get_state(symbol)
            opp_state = await AssassinLedger.get_state(opposite_symbol)
            self_buy_id = await AssassinLedger.get_buy_order_id(symbol)
            opp_buy_id = await AssassinLedger.get_buy_order_id(opposite_symbol)

            if (
                self_state[4]  # self is_session_done
                or opp_state[4]  # opposite is_session_done (단, 반대 종목이 이미 타격/보유한 경우)
                and (opp_buy_id != "" or opp_state[7] != "" or ctx.shared_holdings.get(opposite_symbol, 0) > 0)
            ):
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            if (
                self_buy_id != ""
                or opp_buy_id != ""
                or ctx.shared_holdings.get(symbol, 0) > 0
                or ctx.shared_holdings.get(opposite_symbol, 0) > 0
            ):
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            # Case 43 & Case 61: 09:20~09:30 EST NQ=F 10분봉 확정 시그널 즉시 조회
            (
                nq_10m_trend,
                nq_10m_chg_pct,
                nq_10m_bar_str,
                nq_c_global,
                nq_h_global,
                nq_l_global,
            ) = await MacroDataCache.get_nq_10m_sniper_signal(force_refresh=True)

            nq_amp_global = (
                ((nq_h_global - nq_l_global) / nq_l_global * 100.0)
                if nq_l_global > 0.0
                else 0.0
            )

            # 1차 방어선 (Case 66): NQ=F 당일 진폭 2.0% 이상 시 SOXL/SOXS 양방향 모두 진입 차단 후 퇴근
            if nq_amp_global >= 2.0:
                await AssassinLedger.save_state("SOXL", is_session_done=True, last_session_id=current_session_id)
                await AssassinLedger.save_state("SOXS", is_session_done=True, last_session_id=current_session_id)
                await ctx.notify_tg(
                    f"🛑 <b>[09:30 스나이퍼] 대세장 이탈 감지 (양방향 매수 차단)</b>\n"
                    f"▫️ 사유: NQ=F 실시간 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: SOXL/SOXS 당일 매수 권한 영구 소각 및 관망 퇴근"
                )
                return

            # 방향 매칭 검증: DOWN -> SOXS(숏) / UP -> SOXL(롱)
            expected_trend = "DOWN" if symbol == "SOXS" else "UP"
            if nq_10m_trend != expected_trend:
                if nq_10m_trend in ("DOWN", "UP"):
                    # 반대 방향 시그널로 확정된 경우 본인 종목은 즉시 당일 대기 종료
                    await AssassinLedger.save_state(
                        symbol, is_session_done=True, last_session_id=current_session_id
                    )
                return

            # 토스 API 실시간 잔고 이중 확인
            my_hold_check = await ctx.client.get_symbol_holdings_detail(symbol)
            opp_hold_check = await ctx.client.get_symbol_holdings_detail(opposite_symbol)
            if (
                int(math.floor(my_hold_check.get("qty", 0.0))) > 0
                or int(math.floor(opp_hold_check.get("qty", 0.0))) > 0
            ):
                await AssassinLedger.save_state(symbol, is_session_done=True)
                return

            # Case 65: USD Buying Power 프로빙 및 0.5% 안전 버퍼 적용
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                safe_bp = current_bp * 0.995
                actual_budget = min(budget, safe_bp)
            except Exception:
                actual_budget = budget

            target_price = current_price
            target_qty = int(math.floor(actual_budget / target_price))

            if target_qty <= 0:
                await AssassinLedger.save_state(symbol, is_session_done=True)
                await ctx.notify_tg(
                    f"🚨 <b>[09:30 스나이퍼 {symbol}] 주문 가능 수량 0주 (예산 부족)</b>\n"
                    f"▫️ 실질 예산: ${actual_budget:.2f} | 현재가: ${target_price:.2f}"
                )
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
                symbol=symbol,
                side="BUY",
                order_type="LIMIT",
                quantity=target_qty,
                price=f"{target_price:.2f}",
                client_order_id=idem["key"],
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                # Case 41 & Case 52: 본인 장부 기록 및 반대 종목(opposite_symbol) 당일 매수 권한 영구 소각
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="REG_0930",
                    entry_trend=nq_10m_trend,
                    entry_time=time.time(),
                    nq_entry_price=nq_c_global,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                await AssassinLedger.lock_opposite_symbol(symbol, current_session_id)

                dir_kor = "하락(DOWN) ➡️ 숏(SOXS)" if symbol == "SOXS" else "상승(UP) ➡️ 롱(SOXL)"
                await ctx.notify_tg(
                    f"🎯 <b>[09:30 오프닝 스나이퍼 {symbol}] 정규장 개장 동시 타격 장전</b>\n"
                    f"▫️ NQ 10분봉({nq_10m_bar_str}): <code>{dir_kor} ({nq_10m_chg_pct:+.3f}%)</code>\n"
                    f"▫️ 타격 지정가(현재가): <code>${target_price:.2f}</code> ({target_qty}주)\n"
                    f"▫️ 익절 목표: <code>+{target_profit_rate}%</code> (체결 즉시 팩트 평단가 기준 장전)\n"
                    f"▫️ 배타 락온: 반대 종목(<b>{opposite_symbol}</b>) 금일 진입 권한 영구 소각"
                )
            ctx.idempotency_keys[symbol]["BUY"] = None

        except Exception as e:
            err_str = str(e)
            print(f"🚨 [09:30 스나이퍼 타격 방어 {symbol}] {err_str}", flush=True)
            if any(code in err_str for code in ["400", "422", "404", "409", "401", "403", "429"]):
                ctx.idempotency_keys[symbol]["BUY"] = None
                if "400" in err_str or "422" in err_str:
                    await AssassinLedger.save_state(symbol, is_session_done=True)
                    await ctx.notify_tg(
                        f"🛑 <b>[09:30 스나이퍼 {symbol}] 매수 요격 영구 셧다운</b>\n"
                        f"▫️ 사유: 400/422 주문 거부 감지 ({html.escape(err_str)})\n"
                        "▫️ 조치: 금일 매수 권한 100% 영구 소각"
                    )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 6. [플러그인] +1.0% 기계적 익절 덫 장전, 수동 개입 락온 및 유령 덫 복구 (Case 50, 58, 64, 70, 71)
# =====================================================================
async def manage_mechanical_profit_trap_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    is_active: bool,
    just_turned_off: bool,
    holdings_qty: int,
    holdings_detail: dict,
    last_buy_price: float,
    target_profit_rate: float,
    target_sell_price: float,
    buy_order_id: str,
    sell_order_id: str,
    entry_session: str,
    runtime_state: dict,
):
    open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
    has_open_sell = any(o.get("side") == "SELL" for o in open_orders)
    has_open_buy = any(o.get("side") == "BUY" for o in open_orders)

    # 수동 오버나이트(Bot OFF) 전환 시 기존 매도 덫 안전 파기 (Case 42)
    if not is_active:
        if sell_order_id and not ctx.in_memory_ordering_lock[symbol]:
            ctx.in_memory_ordering_lock[symbol] = True
            notify_msg = ""
            try:
                await ctx.client.cancel_order(sell_order_id)
                notify_msg = (
                    f"🛑 <b>[스나이퍼 {symbol}] 수동 오버나이트(가동 OFF) 전환</b>\n"
                    "▫️ 조치: 기장전된 지정가 매도 주문 안전 파기 완료"
                )
            except Exception as e:
                notify_msg = (
                    f"🛑 <b>[스나이퍼 {symbol}] 수동 오버나이트(가동 OFF) 전환</b>\n"
                    "▫️ 조치: 로컬 덫 장부 초기화 완료"
                )
                print(f"🚨 [수동 OFF 덫 파기 방어 {symbol}] {e}", flush=True)
            finally:
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                if notify_msg:
                    await ctx.notify_tg(notify_msg)
                await asyncio.sleep(0.5)
                ctx.in_memory_ordering_lock[symbol] = False
        elif just_turned_off:
            await ctx.notify_tg(
                f"🛑 <b>[스나이퍼 {symbol}] 수동 오버나이트(가동 OFF) 전환</b>\n"
                "▫️ 조치: 시스템 대기 모드로 안전 전환 완료"
            )
        return

    # 보유 중 유령 지정가 덫 증발(CANCELED/REJECTED/404) 감지 및 자동 복구
    if holdings_qty > 0 and sell_order_id and is_active:
        try:
            od = await ctx.client.get_order_detail(sell_order_id)
            status_trap = od.get("status", "")
            if status_trap in ["CANCELED", "REJECTED"]:
                await ctx.notify_tg(
                    f"🚨 <b>[스나이퍼 {symbol}] 유령 지정가 덫 증발 감지</b>\n"
                    f"▫️ 사유: 상태 변이({status_trap}) ➡️ 즉시 자동 재장전 가동"
                )
                await AssassinLedger.save_state(symbol, sell_order_id="")
                sell_order_id = ""
        except Exception as e:
            err_str = str(e).lower()
            if "404" in err_str or "not-found" in err_str:
                await ctx.notify_tg(
                    f"🚨 <b>[스나이퍼 {symbol}] 유령 지정가 덫 증발 감지</b>\n"
                    "▫️ 사유: 서버 404 (수동 취소 추정) ➡️ 즉시 자동 재장전 가동"
                )
                await AssassinLedger.save_state(symbol, sell_order_id="")
                sell_order_id = ""

    # 잔고 확증 시 100% 보유 수량에 대한 +target_profit_rate%(기본 +1.0%) 지정가 매도 덫 장전 (Case 58, Case 64)
    if (
        holdings_qty > 0
        and not has_open_sell
        and not has_open_buy
        and not sell_order_id
        and not ctx.in_memory_ordering_lock[symbol]
        and is_active
    ):
        calculated_target = target_sell_price
        avg_price = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price

        is_rearm = True
        trap_tag = ""
        skip_trap = False

        if calculated_target <= 0.0:
            if buy_order_id:
                try:
                    order_detail = await ctx.client.get_order_detail(buy_order_id)
                    status = order_detail.get("status", "")
                    if status in ["PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"]:
                        skip_trap = True
                    elif status in ["FILLED", "CANCELED", "REJECTED"]:
                        is_rearm = False
                    else:
                        skip_trap = True
                except Exception as e:
                    print(f"🚨 [덫 대기망 방어 {symbol}] 주문 상태 프로빙 실패: {e}", flush=True)
                    skip_trap = True
            else:
                is_rearm = False

            if skip_trap:
                return

            if avg_price > 0.0:
                calculated_target = (
                    math.ceil(avg_price * (1.0 + target_profit_rate / 100.0) * 100) / 100.0
                )
                trap_tag = (
                    f"09:30스나이퍼(+{target_profit_rate}%)"
                    if entry_session == "REG_0930" and buy_order_id
                    else f"수동개입(+{target_profit_rate}%)"
                )

        trap_qty = holdings_qty
        if calculated_target > 0.0 and trap_qty > 0 and not skip_trap:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                # Case 71: 매도 가능 수량 프로빙
                sellable_q = await ctx.client.get_sellable_quantity(symbol)
                sellable_q_int = int(math.floor(sellable_q))
                if sellable_q_int < trap_qty:
                    print(
                        f"🚨 [매도 가능 수량 부족 {symbol}] 잔고:{trap_qty}주 | 가능:{sellable_q_int}주. 덫 보류.",
                        flush=True,
                    )
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
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=trap_qty,
                    price=f"{calculated_target:.2f}",
                    client_order_id=idem["key"],
                )

                new_sell_id = ""
                if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                    new_sell_id = str(res["result"]["orderId"])

                # Case 50 & Case 52: 잔고 보유 확정 즉시 반대 종목 당일 진입 권한 영구 소각
                await AssassinLedger.lock_opposite_symbol(symbol, current_session_id)

                # Case 70: 최초 덫 장전 순간(if not is_rearm:)의 실시간 NQ=F 지수/진폭으로 기준점 동기화
                if not is_rearm:
                    nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                    nq_amp_now = (
                        ((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0
                    )
                    inferred_trend = "DOWN" if symbol == "SOXS" else "UP"
                    if not buy_order_id:
                        await AssassinLedger.save_state(
                            symbol,
                            price=avg_price,
                            target_sell_price=calculated_target,
                            sell_order_id=new_sell_id,
                            is_session_done=True,
                            entry_session="MANUAL",
                            entry_trend=inferred_trend,
                            nq_entry_price=nq_c_now,
                            nq_entry_amp=nq_amp_now,
                        )
                    else:
                        await AssassinLedger.save_state(
                            symbol,
                            price=avg_price,
                            target_sell_price=calculated_target,
                            sell_order_id=new_sell_id,
                            is_session_done=True,
                            nq_entry_price=nq_c_now,
                            nq_entry_amp=nq_amp_now,
                        )
                    await ctx.notify_tg(
                        f"🟢 <b>[스나이퍼 {symbol}] {trap_tag} 지정가 매도 덫 장전 완료</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code>\n"
                        f"▫️ 목표 익절가: <code>${calculated_target:.2f}</code> (+{target_profit_rate}%)\n"
                        f"▫️ 장전 수량: <code>{trap_qty}주</code> (DAY 당일 유효)"
                    )
                else:
                    await AssassinLedger.save_state(symbol, sell_order_id=new_sell_id)
                    await ctx.notify_tg(
                        f"🟢 <b>[스나이퍼 {symbol}] 포지션 지정가 매도 덫 재장전</b>\n"
                        f"▫️ 유지 평단가: <code>${avg_price:.2f}</code> | 익절 덫: <code>${calculated_target:.2f}</code> ({trap_qty}주)"
                    )

                ctx.idempotency_keys[symbol]["TRAP"] = None
                runtime_state["last_trap_error_msg"] = ""
            except Exception as e:
                err_str = str(e)
                if any(code in err_str for code in ["400", "422", "404", "409", "401", "403"]):
                    ctx.idempotency_keys[symbol]["TRAP"] = None
                if err_str != runtime_state["last_trap_error_msg"]:
                    await ctx.notify_tg(
                        f"🚨 <b>[TRAP 에러 {symbol}]</b> {html.escape(err_str)}\n"
                        "▫️ 조치: 멱등성 데드락 해제 및 재장전 대기 (5초 쿨다운)"
                    )
                    runtime_state["last_trap_error_msg"] = err_str
                await asyncio.sleep(5.0)
            finally:
                ctx.in_memory_ordering_lock[symbol] = False
