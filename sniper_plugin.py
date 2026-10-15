# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장(04:00~09:29 EST) SOXS 단방향 RSI<=30 무한 출동 스나이퍼 및 매크로 3단 방어 상태 머신
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 03:58/04:07 암살자 타격 함수 영구 소각
# MODIFIED: 제3헌법 의거 롱(SOXL)/숏(SOXS) RSI<=20 및 RSI<=10 패닉 셀 텔레그램 푸시 알림 기능 영구 폐기
# NEW: 프리장 골든타임(04:00~09:29 EST) SOXS 1분봉 RSI<=30.0 포착 즉시 wakeup_event 격발 및 지정가(LIMIT) 실거래 요격 (Case 15, 25, 52, 53, 59)
# NEW: 프리장 타격 시 시드/USD 예수금 부족(target_qty <= 0) 및 주문 API 거절 예외 발생 시 무음 스킵 방지 텔레그램 타전망 결속 (Case 17, 18, 65)
# NEW: 09:30 EST 정규장 개장 정각 신규 매수 원천 봉쇄 및 프리장 미체결 매수 주문(buy_order_id) 즉각 선취소 (Case 48, 53)
# MODIFIED: 프리장(04:00~09:29 EST) 내 익절 완료 시 is_session_done=False 복원으로 무한 재출동 루프 결속 (Case 52)
# MODIFIED: 매크로 1·2·3차 방어선 작동 시 entry_session="CANCELED_MACRO" 및 당일 재진입 영구 차단 락온 (Case 66, 67)
# MODIFIED: 스나이퍼 OFF 상태 중 수동 청산 식별 시 타전 억제(Silent Reset) 분기 결속 (Case 46)
# NEW: MOC 및 매크로 덤핑 시 미체결 매도 선취소 후 수량 해방(sellableQuantity > 0) 2.0초 폴링 대기 및 거래소 취소 병목 시 기존 매도 주문 가격 즉시 정정(modify_order) 우회 타격망 결속 (Case 16, 24, 69)
# NEW: 전술 제어반 목표 익절률(+0.64% vs +1.00%) 동적 연동, 보유 중 익절률 변경 시 매도 덫 자동 재장전 및 복리 모드(+0.50% / +0.86% 순수익 가산, 손절 시 수수료 0.14% 포함 실손실 차감) 결속

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
PRE_SNIPER_RSI_THRESHOLD = 30.0

# 무음 불발(Silent Drop) 방어 및 스팸 억제용 인메모리 경보 캐시 (Case 18)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_err_msg": "",
    "last_macro1_date": "",
}


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


async def _cancel_open_orders_and_wait_sellable(
    ctx: SniperPluginContext,
    symbol: str,
    sell_order_id: str,
    cancel_all_sides: bool = False,
) -> tuple[int, list[str]]:
    """
    NEW: MOC 및 매크로 긴급 덤핑 전 기존 미체결 주문(익절 TRAP 등)을 중복 없이 선취소하고,
    미국 거래소에서 수량 잠금이 해제되어 sellableQuantity > 0이 될 때까지 최대 2.0초(0.25초 * 8회) 폴링 대기합니다.
    만약 장마감 주문 폭주로 취소가 지연/거부되어 여전히 잠겨 있는 SELL 주문이 있다면 해당 주문 ID 리스트를 반환하여
    즉시 가격 정정(modify_order) 우회 타격이 가능하도록 지원합니다.
    """
    canceled_ids = set()
    surviving_sell_ids = []
    need_wait_unlock = False

    if sell_order_id:
        try:
            await ctx.client.cancel_order(sell_order_id)
            canceled_ids.add(sell_order_id)
            need_wait_unlock = True
        except Exception as e:
            err_s = str(e)
            print(f"🚨 [선취소 방어 {symbol}] ({sell_order_id[:10]}): {err_s}", flush=True)
            if "already-filled" not in err_s and "already-canceled" not in err_s and "order-not-found" not in err_s:
                surviving_sell_ids.append(sell_order_id)
        finally:
            await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)

    try:
        open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
    except Exception:
        open_orders = []

    for order in open_orders:
        oid = str(order.get("orderId") or "")
        if not oid:
            continue
        side = str(order.get("side") or "")
        st = str(order.get("status") or "")

        if not cancel_all_sides and side != "SELL":
            continue

        if side == "SELL" and oid not in surviving_sell_ids:
            surviving_sell_ids.append(oid)

        # 이미 취소 요청했거나 거래소에서 취소 진행 중(PENDING_CANCEL)인 주문은 중복 취소(409) 스킵 후 대기만 활성화
        if oid in canceled_ids or st == "PENDING_CANCEL":
            need_wait_unlock = True
            continue

        try:
            await ctx.client.cancel_order(oid)
            canceled_ids.add(oid)
            need_wait_unlock = True
        except Exception as e:
            print(f"🚨 [미체결 취소 방어 {symbol}] ({oid[:10]}): {e}", flush=True)

    sellable_q_int = 0
    max_polls = 8 if need_wait_unlock else 1
    for poll_idx in range(max_polls):
        if poll_idx > 0 or need_wait_unlock:
            await asyncio.sleep(0.25)
        try:
            sq = await ctx.client.get_sellable_quantity(symbol)
            sellable_q_int = int(math.floor(sq))
            if sellable_q_int > 0:
                break
        except Exception:
            pass

    return sellable_q_int, surviving_sell_ids


async def execute_rsi_monitoring_plugin(
    ctx: SniperPluginContext, symbol: str, rsi_val: float, vwap_val: float, current_price: float
):
    if ctx.shared_rsi_state is None:
        return

    state = ctx.shared_rsi_state.get(symbol)
    if not state:
        return

    # 듀얼 관제탑(/avwap) 표출용 실시간 RSI 및 VWAP 상태만 갱신하고, RSI<=20/10 텔레그램 푸시 알림은 영구 폐기
    state["value"] = rsi_val
    state["vwap"] = vwap_val

    # Case 25 & Case 59: 프리장(04:00~09:29 EST) 내 SOXS RSI <= 30.0 포착 즉시 메인 루프 기상 트리거 격발
    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    if symbol == "SOXS" and (400 <= est_hm <= 929) and (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD):
        ctx.wakeup_event.set()


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
                holdings_detail_moc = await ctx.client.get_symbol_holdings_detail(symbol)
                dump_qty = int(math.floor(holdings_detail_moc["qty"]))

                if dump_qty <= 0:
                    # 잔고 0주 확인 시 장부에 미정산 포지션(last_buy_price > 0)이 남아 있다면
                    # False를 반환하여 메인 루프의 reconcile_zero_holdings_exit_plugin이 즉시 복리 정산 및 타전을 수행하도록 연동
                    st_chk = await AssassinLedger.get_state(symbol)
                    has_unsettled_ledger = (st_chk[0] > 0.0 or st_chk[6] > 0.0 or bool(st_chk[7]))
                    if has_unsettled_ledger:
                        return False
                    await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
                    return True

                # 1단계 — 미체결 주문 선취소 및 매도 가능 수량 해방(sellableQuantity > 0) 폴링 대기
                sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
                    ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
                )
                ctx.idempotency_keys[symbol]["MOC"] = None

                current_price = await ctx.client.get_current_price(symbol)
                orderbook = await ctx.client.get_orderbook(symbol)
                bids = orderbook.get("bids", [])
                bid_1_price = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else current_price

                if bid_1_price <= 0.0:
                    return True

                if sellable_q_int > 0:
                    exec_qty = min(dump_qty, sellable_q_int)
                    idem_key = f"MOC_{symbol}_{now_est.strftime('%H%M%S')}_{int(time.time() * 10) % 100}"[:36]
                    ctx.idempotency_keys[symbol]["MOC"] = {
                        "key": idem_key,
                        "qty": exec_qty,
                        "price": bid_1_price,
                    }

                    try:
                        await asyncio.wait_for(
                            ctx.client.create_order(
                                symbol=symbol, side="SELL", order_type="LIMIT",
                                quantity=exec_qty, price=f"{bid_1_price:.2f}",
                                client_order_id=idem_key,
                            ), timeout=4.0,
                        )
                        print(f"🔴 [MOC 덤핑 스윕 {symbol}] 수량: {exec_qty}주 | 단가: ${bid_1_price:.2f}", flush=True)
                        await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
                        if not runtime_state["moc_dump_active"]:
                            runtime_state["moc_dump_active"] = True
                            await ctx.notify_tg(
                                f"🔴 <b>[스나이퍼 {symbol}] 15:59~16:01 제로-오버나이트 덤핑망 결속</b>\n"
                                f"▫️ 미체결 주문 선취소 완료 및 매수 1호가(<code>${bid_1_price:.2f}</code>, {exec_qty}주) 스윕 폭격 개시"
                            )
                    finally:
                        ctx.idempotency_keys[symbol]["MOC"] = None

                elif surviving_sell_ids:
                    # 2단계 우회 타격 — 장마감 1분 전 거래소 취소 병목 시 살아있는 기존 매도 주문 가격을 매수 1호가로 즉시 정정(modify_order)
                    target_mod_id = surviving_sell_ids[0]
                    await asyncio.wait_for(
                        ctx.client.modify_order(
                            order_id=target_mod_id,
                            price=f"{bid_1_price:.2f}",
                            order_type="LIMIT",
                            market_country="US",
                        ),
                        timeout=4.0,
                    )
                    print(
                        f"🔴 [MOC 우회 정정 스윕 {symbol}] 기존 주문({target_mod_id[:10]}) 단가 -> ${bid_1_price:.2f} 즉시 하향 정정 완료",
                        flush=True,
                    )
                    await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
                    if not runtime_state["moc_dump_active"]:
                        runtime_state["moc_dump_active"] = True
                        await ctx.notify_tg(
                            f"🔴 <b>[스나이퍼 {symbol}] 15:59~16:01 제로-오버나이트 정정 스윕 결속</b>\n"
                            f"▫️ 장마감 취소 병목 우회: 기존 매도 주문을 매수 1호가(<code>${bid_1_price:.2f}</code>)로 즉시 하향 정정 완료"
                        )
            except asyncio.TimeoutError:
                ctx.idempotency_keys[symbol]["MOC"] = None
                print(f"🚨 [MOC 타임아웃 방어 {symbol}] 4초 초과 (차기 틱 즉시 재타격).", flush=True)
            except Exception as e:
                ctx.idempotency_keys[symbol]["MOC"] = None
                err_str = str(e)
                print(f"🚨 [MOC 방어 {symbol}] {err_str}", flush=True)
            finally:
                runtime_state["last_moc_tick"] = time.time()
                ctx.in_memory_ordering_lock[symbol] = False
    return True


async def execute_macro_defense_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, is_active: bool,
    is_session_done: bool, holdings_qty: int, current_price: float, avg_price_dump: float,
    sell_order_id: str, nq_entry_price: float, nq_entry_amp: float, buy_order_id: str = "",
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

    # 1차 방어선: NQ=F 진폭 2.0% 이상 시 프리장 신규 진입 차단 및 미체결 매수 주문 즉각 파기 (Case 66)
    if is_macro_breach and (not is_session_done or bool(buy_order_id)):
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            canceled_any_buy = False
            canceled_buy_ids = set()
            if buy_order_id:
                try:
                    await ctx.client.cancel_order(buy_order_id)
                    canceled_buy_ids.add(buy_order_id)
                    canceled_any_buy = True
                except Exception:
                    pass

            open_orders_macro = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for order in open_orders_macro:
                oid = str(order.get("orderId") or "")
                if order.get("side") == "BUY" and oid and oid not in canceled_buy_ids:
                    try:
                        await ctx.client.cancel_order(oid)
                        canceled_buy_ids.add(oid)
                        canceled_any_buy = True
                    except Exception:
                        pass

            save_kwargs = {"buy_order_id": "", "is_session_done": True, "entry_session": "CANCELED_MACRO"}
            await AssassinLedger.save_state(symbol, **save_kwargs)
            print(f"🛑 [매크로 1차 진입 차단 {symbol}] NQ=F 진폭 {nq_amp_global:.2f}% 도달. 출동 취소.", flush=True)

            est_hm = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

            if canceled_any_buy:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    f"🛑 <b>[스나이퍼 {symbol}] 대세장 과열 감지 (미체결 매수 긴급 파기)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 프리장 미체결 매수 주문 원자적 파기 및 당일 매수 권한 영구 소각"
                )
            elif (400 <= est_hm <= 929) and _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    f"🛑 <b>[프리장 스나이퍼 {symbol}] 매크로 1차 방어선 작동 (출동 마감)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상 과열)\n"
                    "▫️ 조치: 금일 프리장 신규 매수 진입 전면 차단 및 관망 전환"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 2차 & 3차 방어선: 보유 중 손실권(0 < 현재가 < 평단가)에서 NQ 2.0% 돌파 또는 동적 임계치 상승 역행 시 덤핑 (Case 66, 67)
    if (is_macro_breach or is_dynamic_breach) and holdings_qty > 0 and avg_price_dump > 0.0 and 0.0 < current_price < avg_price_dump:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            holdings_detail_mdump = await ctx.client.get_symbol_holdings_detail(symbol)
            dump_qty = int(math.floor(holdings_detail_mdump["qty"]))

            if dump_qty > 0:
                sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
                    ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=False
                )
                ctx.idempotency_keys[symbol]["M_DUMP"] = None

                orderbook = await ctx.client.get_orderbook(symbol)
                bids = orderbook.get("bids", [])
                bid_1_price = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else current_price

                if bid_1_price > 0.0:
                    dumped_ok = False
                    if sellable_q_int > 0:
                        exec_qty = min(dump_qty, sellable_q_int)
                        idem_key = f"MDUMP_{symbol}_{now_est.strftime('%H%M%S')}_{int(time.time() * 10) % 100}"[:36]
                        ctx.idempotency_keys[symbol]["M_DUMP"] = {
                            "key": idem_key,
                            "qty": exec_qty,
                            "price": bid_1_price,
                        }
                        await asyncio.wait_for(
                            ctx.client.create_order(
                                symbol=symbol, side="SELL", order_type="LIMIT",
                                quantity=exec_qty, price=f"{bid_1_price:.2f}",
                                client_order_id=idem_key,
                            ), timeout=4.0,
                        )
                        dumped_ok = True
                    elif surviving_sell_ids:
                        await asyncio.wait_for(
                            ctx.client.modify_order(
                                order_id=surviving_sell_ids[0],
                                price=f"{bid_1_price:.2f}",
                                order_type="LIMIT",
                                market_country="US",
                            ),
                            timeout=4.0,
                        )
                        dumped_ok = True

                    if dumped_ok:
                        await AssassinLedger.save_state(
                            symbol, is_session_done=True, entry_session="CANCELED_MACRO"
                        )
                        if is_macro_breach:
                            reason_msg = f"NQ=F 당일 전체 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 초과 확증)"
                        else:
                            bind_str = " · 하한선 바인딩" if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor) else ""
                            reason_msg = f"체결 기준 지수({nq_entry_price:.2f}) 대비 <code>{nq_diff_pct:+.2f}%</code> 상승(숏 역행) (동적 임계치 {dynamic_threshold:.2f}%{bind_str} 이탈)"
                        await ctx.notify_tg(
                            f"🚨 <b>[스나이퍼 {symbol}] 매크로 방어선 붕괴 손실 덤핑 격발</b>\n"
                            f"▫️ 사유: {reason_msg}\n"
                            "▫️ 손익 상태: 마이너스 (계좌 보호 하드 스톱 발동)\n"
                            f"▫️ 조치: 지정가 매도 덫 파기 및 전량 매수 1호가(<code>${bid_1_price:.2f}</code>) 스윕 퇴근"
                        )
        except Exception as e:
            print(f"🚨 [매크로 덤핑 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.idempotency_keys[symbol]["M_DUMP"] = None
            ctx.in_memory_ordering_lock[symbol] = False
        return True
    return False


# 목표 1 & 초과 Case 48, 53 — 09:30 EST 정규장 개장 시 프리장 미체결 매수 즉시 취소 및 신규 진입 영구 차단
async def enforce_0930_regular_cutoff_plugin(
    ctx: SniperPluginContext, symbol: str, est_time_int: int, is_session_done: bool, buy_order_id: str
) -> tuple[bool, str]:
    if not (930 <= est_time_int < 1600):
        return is_session_done, buy_order_id

    if buy_order_id and not ctx.in_memory_ordering_lock[symbol]:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            canceled_pre_buy = False
            canceled_ids = set()
            try:
                await ctx.client.cancel_order(buy_order_id)
                canceled_ids.add(buy_order_id)
                canceled_pre_buy = True
            except Exception:
                pass

            open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for o in open_orders:
                oid = str(o.get("orderId") or "")
                if o.get("side") == "BUY" and oid and oid not in canceled_ids:
                    try:
                        await ctx.client.cancel_order(oid)
                        canceled_ids.add(oid)
                        canceled_pre_buy = True
                    except Exception:
                        pass

            await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
            buy_order_id = ""
            is_session_done = True

            if canceled_pre_buy:
                await ctx.notify_tg(
                    f"🔒 <b>[09:30 정규장 컷오프 {symbol}] 프리장 미체결 매수 주문 선취소 완료</b>\n"
                    "▫️ 사유: 09:30 EST 정규장 개장 (정규장 지연 체결 원천 봉쇄)\n"
                    "▫️ 조치: 미체결 매수 파기 및 당일 신규 매수 영구 마감"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return is_session_done, buy_order_id

    if not is_session_done:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        print(f"🔒 [09:30 프리장 골든타임 마감 {symbol}] 정규장 신규 매수 영구 차단.", flush=True)
        return True, buy_order_id

    return is_session_done, buy_order_id


async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext, symbol: str, last_buy_price: float,
    buy_order_id: str, sell_order_id: str, is_active: bool = True,
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
        current_state = await AssassinLedger.get_state(symbol)
        current_budget = float(current_state[1] or 100.0)
        active_target_rate = AssassinLedger.normalize_target_rate(current_state[2])
        net_comp_rate = AssassinLedger.get_net_compound_rate(active_target_rate)
        prev_target_sell_price = float(current_state[6] or 0.0)
        prev_entry_session = str(current_state[8] or "")
        is_compound = await AssassinLedger.get_is_compound(symbol)

        now_est_check = datetime.now(EST_TZ)
        est_hm_now = now_est_check.hour * 100 + now_est_check.minute
        is_premarket_now = (400 <= est_hm_now <= 929)
        is_moc_time = (now_est_check.hour == 15 and now_est_check.minute >= 59) or (
            now_est_check.hour == 16 and 0 <= now_est_check.minute <= 5
        )
        is_take_profit_exit, is_trap_survived = False, False
        exit_price, filled_qty = 0.0, 0.0

        # 1) sell_order_id 상세 원장 우선 프로빙 (익절 체결 여부 및 체결 단가/수량 1차 확인)
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

        # 2) CLOSED 주문 목록에서 최신 SELL 체결 단가 및 수량 추출 (인덱싱 지연 시 0.4초 대기 후 1회 재시도)
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
        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            pnl_amt = (exit_price * filled_qty) - (entry_price * filled_qty)
            pnl_rate = (exit_price / entry_price - 1.0) * 100.0
            sign = "+" if pnl_amt > 0 else ""
            pnl_str = f"\n▫️ 타점: 매수 ${entry_price:.2f} ➡️ 매도 ${exit_price:.2f}\n▫️ 세전 손익: {sign}{pnl_rate:.2f}% ({sign}${pnl_amt:.2f})"
        else:
            pnl_str = "\n▫️ 타점: 체결 데이터 추출 지연 (장부 참조 요망)"

        is_manual_exit = (was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived)
        is_buy_cancel_exit = (not was_holding and not is_take_profit_exit and not is_moc_time and not is_trap_survived)

        # MODIFIED: 선택된 목표 익절률(+0.64% -> 순수익 +0.50%, +1.00% -> 순수익 +0.86%) 연동 복리 시드 자동 갱신
        updated_budget = None
        compound_str = ""
        if was_holding and not is_buy_cancel_exit:
            if is_compound:
                calc_exit_p = exit_price
                if calc_exit_p <= 0.0 and not is_take_profit_exit:
                    try:
                        calc_exit_p = await ctx.client.get_current_price(symbol)
                    except Exception:
                        calc_exit_p = 0.0

                new_b, net_delta, comm_val = AssassinLedger.calc_compound_next_budget(
                    current_budget=current_budget,
                    is_take_profit=is_take_profit_exit,
                    entry_price=entry_price,
                    exit_price=calc_exit_p,
                    filled_qty=filled_qty,
                    target_profit_rate=active_target_rate,
                )
                updated_budget = new_b
                d_sign = "+" if net_delta >= 0.0 else ""
                if is_take_profit_exit:
                    compound_str = (
                        f"\n▫️ 📈 <b>복리 시드 증액(+{net_comp_rate:.2f}%)</b>: <code>${current_budget:,.2f}</code> ➡️ "
                        f"<code>${new_b:,.2f}</code> (<code>{d_sign}${net_delta:,.2f}</code> · 수수료 0.14% 제함)"
                    )
                else:
                    compound_str = (
                        f"\n▫️ 📉 <b>복리 시드 차감(수수료포함)</b>: <code>${current_budget:,.2f}</code> ➡️ "
                        f"<code>${new_b:,.2f}</code> (<code>{d_sign}${net_delta:,.2f}</code> · 수수료 ${comm_val:,.2f} 포함)"
                    )
            else:
                compound_str = f"\n▫️ 💵 <b>단리 시드 고정</b>: <code>${current_budget:,.2f}</code>"

        # 초과 Case 52 — 프리장(04:00~09:29 EST) 내 익절 완료(또는 매크로 아닌 단순 매수취소) 시 is_session_done=False로 무한 재출동 활성화
        if (
            is_premarket_now
            and prev_entry_session != "CANCELED_MACRO"
            and (is_take_profit_exit or is_buy_cancel_exit)
        ):
            next_is_session_done = False
            next_entry_session = ""
        else:
            next_is_session_done = True
            next_entry_session = "CANCELED_MACRO" if prev_entry_session == "CANCELED_MACRO" else ""

        await AssassinLedger.save_state(
            symbol, price=0.0, budget=updated_budget, target_sell_price=0.0, buy_order_id="", sell_order_id="",
            is_session_done=next_is_session_done, entry_session=next_entry_session, entry_time=0.0,
            nq_entry_price=0.0, nq_entry_amp=0.0, entry_trend="",
        )

        # Case 46: 스나이퍼 OFF 상태 중 수동 청산 식별 시 Silent Reset (알람 타전 억제)
        if not is_active:
            print(f"🧹 [Silent Reset {symbol}] 가동 OFF 상태 청산 확인. 타전 억제 및 장부 초기화 완료.", flush=True)
            return

        if is_buy_cancel_exit:
            print(f"🧹 [매수 취소/실패 {symbol}] 허위 익절 타전 억제 및 장부 초기화 완료.", flush=True)
        elif is_moc_time:
            await ctx.notify_tg(
                f"🛑 <b>[스나이퍼 {symbol}] MOC 강제 덤핑 청산 완료</b>\n"
                f"▫️ 잔고 0주 (제로-오버나이트 락온){pnl_str}{compound_str}"
            )
        elif is_take_profit_exit:
            if not next_is_session_done:
                await ctx.notify_tg(
                    f"🎉 <b>[스나이퍼 {symbol}] 프리장 +{active_target_rate:.2f}% 익절 성공 ➡️ 무한 재출동 대기 복귀</b>\n"
                    f"▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 후속 조치: 09:29 EST까지 차기 SOXS RSI≤30 타점 연속 감시 재개"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[스나이퍼 {symbol}] 정규장 +{active_target_rate:.2f}% 익절 타격 성공 (당일 퇴근 완료)</b>\n"
                    f"▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 당일 신규 진입 권한 영구 소각"
                )
        elif is_manual_exit:
            await ctx.notify_tg(
                f"🛑 <b>[스나이퍼 {symbol}] 수동 매도/매크로 청산 감지 (퇴근 락온 완료)</b>\n"
                f"▫️ 잔고 0주 (청산 완료 식별){pnl_str}{compound_str}\n"
                "▫️ 당일 신규 진입 권한 영구 소각 완료"
            )
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# 목표 1 & Case 15, 17, 18, 25, 52, 53, 59, 65, 71 — 프리장(04:00~09:29 EST) SOXS RSI<=30.0 실거래 지정가 매수 요격 플러그인
async def execute_premarket_rsi_sniper_strike_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, current_session_id: str,
    current_price: float, budget: float, target_profit_rate: float, rsi_val: float,
):
    if symbol != "SOXS" or ctx.in_memory_ordering_lock[symbol]:
        return

    est_hm = now_est.hour * 100 + now_est.minute
    if not (400 <= est_hm <= 929):
        return

    if not (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD):
        return

    async with ctx.dual_strike_mutex:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            if (
                self_state[4]
                or await AssassinLedger.get_buy_order_id(symbol) != ""
                or ctx.shared_holdings.get(symbol, 0) > 0
            ):
                return

            active_rate = AssassinLedger.normalize_target_rate(target_profit_rate)
            net_comp_rate = AssassinLedger.get_net_compound_rate(active_rate)

            # Case 61 & Case 66: 타격 직전 NQ=F 실시간 진폭 강제 갱신 및 1차 방어선(2.0% 미만) 검증
            _, _, _, nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_nq_sniper_signal(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                await AssassinLedger.save_state(
                    symbol, is_session_done=True, last_session_id=current_session_id, entry_session="CANCELED_MACRO"
                )
                _STRIKE_ALERT_CACHE["last_macro1_date"] = now_est.strftime("%Y-%m-%d")
                await ctx.notify_tg(
                    f"🛑 <b>[프리장 스나이퍼] 대세장 과열 감지 (진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 당일 매수 권한 영구 소각 및 관망 퇴근"
                )
                return

            # Case 71: 실제 잔고 및 미체결 매수 주문 이중 프로빙 (중복 진입 원천 차단)
            my_hold_check = await ctx.client.get_symbol_holdings_detail(symbol)
            if int(math.floor(my_hold_check.get("qty", 0.0))) > 0:
                return

            open_orders_check = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            if any(o.get("side") == "BUY" for o in open_orders_check):
                return

            latest_price = await ctx.client.get_current_price(symbol)
            if latest_price <= 0.0:
                latest_price = current_price
            if latest_price <= 0.0:
                return

            # Case 65: USD Buying Power 프로빙 및 0.5% 안전 버퍼(0.995) 차감
            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.995)
            except Exception:
                actual_budget = budget

            target_price = math.floor(latest_price * 100.0) / 100.0
            if target_price <= 0.0:
                return

            target_qty = int(math.floor(actual_budget / target_price))

            # Case 18 & Case 65 — target_qty <= 0 발생 시 무음 스킵 방지 및 세션당 1회 텔레그램 경고 타전
            if target_qty <= 0:
                if _STRIKE_ALERT_CACHE.get("last_warn_session") != current_session_id:
                    _STRIKE_ALERT_CACHE["last_warn_session"] = current_session_id
                    bp_str = f"${current_bp:,.2f}" if current_bp >= 0.0 else "조회실패"
                    await ctx.notify_tg(
                        f"⚠️ <b>[프리장 스나이퍼 요격 보류] 주문 수량 0주 산출</b>\n"
                        f"▫️ 포착 RSI: <code>{rsi_val:.1f}</code> | 현재가: <code>${target_price:.2f}</code>\n"
                        f"▫️ 설정 시드: <code>${budget:,.2f}</code> | USD 가능금액: <code>{bp_str}</code>\n"
                        f"▫️ 사유: 유효 투입금(<code>${actual_budget:,.2f}</code>)이 1주 가격(<code>${target_price:.2f}</code>) 미만입니다."
                    )
                return

            is_compound = await AssassinLedger.get_is_compound(symbol)
            mode_tag = "복리" if is_compound else "단리"
            comp_tail = f"익절 시 +{net_comp_rate:.2f}% 복리 증액" if is_compound else "단리 시드 고정"

            # 제5헌법 & Case 34: 토스증권 멱등성 키(clientOrderId) 생성 및 주입
            idem = ctx.idempotency_keys[symbol]["BUY"]
            if not idem:
                idem = {
                    "key": f"PSNP_{symbol}_{now_est.strftime('%H%M%S')}"[:36],
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
                await AssassinLedger.save_state(
                    symbol, buy_order_id=new_buy_id, entry_session="PRE_SNIPER", entry_trend="SHORT",
                    entry_time=time.time(), nq_entry_price=nq_c_global, nq_entry_amp=nq_amp_global,
                    is_session_done=True, last_session_id=current_session_id,
                )
                _STRIKE_ALERT_CACHE["last_err_msg"] = ""
                ctx.wakeup_event.set()
                await ctx.notify_tg(
                    f"🔫 <b>[프리장 스나이퍼] {symbol} RSI≤30 과매도 포착 ➡️ 실거래 지정가 요격 발사</b>\n"
                    f"▫️ 실시간 1분봉 RSI: <code>{rsi_val:.1f}</code> (기준 ≤ 30.0)\n"
                    f"▫️ 요격 지정가(LIMIT): <code>${target_price:.2f}</code> ({mode_tag}시드 <code>${budget:,.2f}</code> ➔ <code>{target_qty}주</code>)\n"
                    f"▫️ NQ=F 기준점: <code>{nq_c_global:.2f}</code> (당일 진폭 <code>{nq_amp_global:.2f}%</code>)\n"
                    f"▫️ 후속 프로토콜: <code>전량 체결 즉시 +{active_rate:.2f}% 익절 덫 장전 ({comp_tail})</code>"
                )
            ctx.idempotency_keys[symbol]["BUY"] = None
        except Exception as e:
            err_str = str(e)
            print(f"🚨 [프리장 스나이퍼 타격 방어 {symbol}] {err_str}", flush=True)
            if any(code in err_str for code in ["400", "422", "404", "409", "401", "403", "429"]):
                ctx.idempotency_keys[symbol]["BUY"] = None
            if _STRIKE_ALERT_CACHE.get("last_err_msg") != err_str:
                _STRIKE_ALERT_CACHE["last_err_msg"] = err_str
                await ctx.notify_tg(
                    f"🚨 <b>[프리장 스나이퍼 주문 에러 {symbol}]</b>\n"
                    f"▫️ 포착 RSI: <code>{rsi_val:.1f}</code>\n"
                    f"▫️ 사유: <code>{html.escape(err_str)}</code>"
                )
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
            except Exception:
                pass
            finally:
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                await asyncio.sleep(0.5)
                ctx.in_memory_ordering_lock[symbol] = False
        return

    active_rate = AssassinLedger.normalize_target_rate(target_profit_rate)
    avg_price = float(holdings_detail.get("avg_price", 0.0)) or last_buy_price

    # NEW: 포지션 보유 및 매도 덫 대기 중 사용자가 제어반(/settlement)에서 목표 익절률(+0.64% <-> +1.00%)을 변경한 경우
    # 기존 매도 덫을 선취소하고 차기 루프(즉시 기상)에서 새로운 목표 익절가로 자동 재장전!
    if (
        holdings_qty > 0
        and sell_order_id
        and is_active
        and avg_price > 0.0
        and target_sell_price > 0.0
        and not ctx.in_memory_ordering_lock[symbol]
    ):
        expected_target = math.ceil(avg_price * (1.0 + active_rate / 100.0) * 100) / 100.0
        if abs(expected_target - target_sell_price) >= 0.01:
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                await _cancel_open_orders_and_wait_sellable(
                    ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=False
                )
                await AssassinLedger.save_state(symbol, sell_order_id="", target_sell_price=0.0)
                ctx.idempotency_keys[symbol]["TRAP"] = None
                ctx.wakeup_event.set()
                print(
                    f"🔄 [익절 타점 변경 재장전 {symbol}] 기존 ${target_sell_price:.2f} 취소 완료 ➡️ 신규 +{active_rate:.2f}% (${expected_target:.2f}) 재장전 직행",
                    flush=True,
                )
            except Exception as e:
                print(f"🚨 [익절 타점 변경 취소 방어 {symbol}] {e}", flush=True)
            finally:
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

    if (
        holdings_qty > 0
        and not has_open_sell
        and not has_open_buy
        and not sell_order_id
        and not ctx.in_memory_ordering_lock[symbol]
        and is_active
    ):
        calculated_target = target_sell_price
        is_rearm, trap_tag, skip_trap = True, "", False

        if calculated_target <= 0.0:
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
                calculated_target = math.ceil(avg_price * (1.0 + active_rate / 100.0) * 100) / 100.0
                if entry_session == "PRE_SNIPER" and buy_order_id:
                    trap_tag = f"프리장 RSI≤30 스나이퍼(+{active_rate:.2f}%)"
                else:
                    trap_tag = f"수동개입(+{active_rate:.2f}%)"

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
                    net_comp_rate = AssassinLedger.get_net_compound_rate(active_rate)

                    kwargs = {
                        "price": avg_price, "target_sell_price": calculated_target,
                        "sell_order_id": new_sell_id, "is_session_done": True,
                    }

                    if not buy_order_id:
                        kwargs.update({
                            "entry_session": "MANUAL", "entry_trend": "SHORT",
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now,
                        })
                    else:
                        kwargs.update({
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now,
                        })

                    await AssassinLedger.save_state(symbol, **kwargs)

                    await ctx.notify_tg(
                        f"🟢 <b>[스나이퍼 {symbol}] {trap_tag} 지정가 매도 덫 장전 완료</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 {trap_qty}주)\n"
                        f"▫️ 목표 익절가: <code>${calculated_target:.2f}</code> (+{active_rate:.2f}% 지정가 · 순수익 +{net_comp_rate:.2f}%)\n"
                        f"▫️ 상태: <code>안전 익절 대기 락온 (매크로 2·3차 방어망 가동 중)</code>"
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
