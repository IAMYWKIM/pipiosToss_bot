# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장 (04:07 ~ 09:29 EST) 양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=80.0)
#       무한 출동 듀얼 스나이퍼(SOXS/SOXL), 방향성 인지 매크로 3단 방어, 15:59 MOC 및 통합 실수익 복리 정산 코어
# =====================================================================
# MODIFIED: 제3헌법 의거 1,000줄 초과 방지를 위해 데이장 [모의] 및 03:58~04:07 듀얼 1주 휩소 작전을 whipsaw_plugin.py로 모듈 분리
# MODIFIED: 프리장 본진(04:07 ~ 09:29 EST) 타격 조건을 양방향 대칭 클라이맥스(매수종목 RSI<=30.0 AND 반대종목 RSI>=80.0)로 단일화 (Case 54, 81)
# NEW: 이종 종목 간 동시 타격 및 이중 보유를 원천 봉쇄하는 단일 포지션 배타적 뮤텍스(Cross-Symbol Single-Position Mutex) 탑재 (Case 71)
# MODIFIED: 방향성 인지형(Direction-Aware) NQ=F 매크로 3차 방어선(SOXS는 NQ 상승 역행 / SOXL은 NQ 하락 역행) 및 글로벌 매크로 락아웃 결속 (Case 67, 82)
# MODIFIED: 통합 시드 풀(Unified Seed Pool) 기반 실제 발생 순수익금((매도가-매수가)*체결수량 - 수수료 0.14%) 복리 정산 및 양쪽 장부 동시 동기화 (Case 76, 77)
# MODIFIED: MOC 및 매크로 덤핑 시 미체결 매도 선취소 후 수량 해방 2.0초 폴링 대기 및 취소 병목 시 modify_order 가격 정정 우회 타격망 유지 (Case 16, 24, 69, 75)

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
    compute_realtime_tick_rsi,
)
from whipsaw_plugin import (
    PRE_SNIPER_RSI_THRESHOLD,
    OPP_CLIMAX_RSI_THRESHOLD,
    BUY_TRAP_SLIPPAGE_PCT,
    BUY_TRAP_MIN_DISC_USD,
    BUY_TRAP_REARM_WAIT_SEC,
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
)

EST_TZ = ZoneInfo("America/New_York")

# 무음 불발(Silent Drop) 방어 인메모리 캐시 (Case 18, 74)
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
    MOC, 매크로 긴급 덤핑 및 04:07 휩소 1주 정리 전 기존 미체결 주문을 중복 없이 선취소하고,
    미국 거래소에서 수량 잠금이 해제되어 sellableQuantity > 0이 될 때까지 최대 2.0초(0.25초 * 8회) 폴링 대기합니다.
    만약 주문 폭주로 취소가 지연되어 여전히 잠겨 있는 SELL 주문이 있다면 해당 주문 ID 리스트를 반환하여
    즉시 가격 정정(modify_order) 우회 타격이 가능하도록 지원합니다 (Case 24, 69, 75).
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

    # 듀얼 관제탑(/avwap) 및 03:58 듀얼 휩소 노출 작전용 실시간 RSI 및 당일 누적 VWAP 상태 갱신
    state["value"] = rsi_val
    state["vwap"] = vwap_val

    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute

    # Case 25 & Case 59 & Case 81: 프리장 (04:00 ~ 09:29 EST) 내 양방향 대칭 클라이맥스 포착 또는
    # 1차 매수 덫 대기 중 RSI > 30.0 반등 돌파 시 메인 루프 즉시 기상
    if symbol in ("SOXS", "SOXL") and (400 <= est_hm <= 929):
        if (
            0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD
            or rsi_val >= OPP_CLIMAX_RSI_THRESHOLD
            or rsi_val > PRE_SNIPER_RSI_THRESHOLD
        ):
            ctx.wakeup_event.set()
    # 데이장 (19:00 ~ 03:59 EST) 내 양방향 대칭 클라이맥스 [모의] 2단계 추격 알림 플러그인 실행 (03:58~03:59 묵음 내장)
    elif symbol in ("SOXS", "SOXL") and (est_hm >= 1900 or est_hm <= 359):
        await execute_daymarket_rsi_alert_plugin(
            ctx=ctx,
            symbol=symbol,
            now_est=now_est,
            current_price=current_price,
            rsi_val=rsi_val,
            vwap_val=vwap_val,
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
                holdings_detail_moc = await ctx.client.get_symbol_holdings_detail(symbol)
                dump_qty = int(math.floor(holdings_detail_moc["qty"]))

                if dump_qty <= 0:
                    st_chk = await AssassinLedger.get_state(symbol)
                    has_unsettled_ledger = (st_chk[0] > 0.0 or st_chk[6] > 0.0 or bool(st_chk[7]))
                    if has_unsettled_ledger:
                        return False
                    await AssassinLedger.save_state(
                        symbol, buy_order_id="", is_session_done=True, whipsaw_target_price=0.0
                    )
                    return True

                runtime_state["last_known_qty"] = dump_qty
                sym_icon = "🐻" if symbol == "SOXS" else "🐂"

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
                        await AssassinLedger.save_state(
                            symbol, buy_order_id="", is_session_done=True, whipsaw_target_price=0.0
                        )
                        if not runtime_state["moc_dump_active"]:
                            runtime_state["moc_dump_active"] = True
                            await ctx.notify_tg(
                                f"🔴 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 15:59 ~ 16:01 제로-오버나이트 덤핑망 결속</b>\n"
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
                    await AssassinLedger.save_state(
                        symbol, buy_order_id="", is_session_done=True, whipsaw_target_price=0.0
                    )
                    if not runtime_state["moc_dump_active"]:
                        runtime_state["moc_dump_active"] = True
                        await ctx.notify_tg(
                            f"🔴 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 15:59 ~ 16:01 제로-오버나이트 정정 스윕 결속</b>\n"
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


# MODIFIED: 방향성 인지형(Direction-Aware) NQ=F 매크로 3단 방어망 및 글로벌 매크로 락아웃 (Case 66, 67, 82)
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

    # Case 67: 보유 종목 방향성(SOXS 숏 vs SOXL 롱)에 따른 NQ=F 역행 폭 산출
    if nq_entry_price > 0.0 and nq_c_global > 0.0:
        if symbol == "SOXS":
            # SOXS(숏) 보유 시: NQ=F 상승이 역행
            nq_adverse_pct = (nq_c_global - nq_entry_price) / nq_entry_price * 100.0
            adverse_label = "상승(숏 역행)"
        else:
            # SOXL(롱) 보유 시: NQ=F 하락이 역행
            nq_adverse_pct = (nq_entry_price - nq_c_global) / nq_entry_price * 100.0
            adverse_label = "하락(롱 역행)"
    else:
        nq_adverse_pct = 0.0
        adverse_label = "역행"

    is_macro_breach = nq_amp_global >= 2.0

    is_dynamic_breach = (
        nq_entry_price > 0.0
        and dynamic_threshold > 0.0
        and nq_c_global > 0.0
        and nq_adverse_pct >= dynamic_threshold
    )

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1차 방어선: NQ=F 진폭 2.0% 이상 시 프리장 신규 진입 차단 및 양쪽 미체결 매수 주문 즉각 파기 (Case 66, 82)
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

            for chk_sym in ("SOXS", "SOXL"):
                try:
                    open_orders_macro = await ctx.client.get_orders(status="OPEN", symbol=chk_sym)
                    for order in open_orders_macro:
                        oid = str(order.get("orderId") or "")
                        if order.get("side") == "BUY" and oid and oid not in canceled_buy_ids:
                            try:
                                await ctx.client.cancel_order(oid)
                                canceled_buy_ids.add(oid)
                                canceled_any_buy = True
                            except Exception:
                                pass
                except Exception:
                    pass

            await AssassinLedger.lock_all_symbols_macro()
            print(f"🛑 [매크로 1차 진입 차단 {symbol}] NQ=F 진폭 {nq_amp_global:.2f}% 도달. 양쪽 출동 취소.", flush=True)

            est_hm = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

            if canceled_any_buy:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    f"🛑 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 대세장 과열 감지 (미체결 매수 긴급 파기)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 프리장 미체결 매수 주문 원자적 파기 및 금일 SOXS/SOXL 매수 권한 영구 소각"
                )
            elif (400 <= est_hm <= 929) and _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 매크로 1차 방어선 작동 (출동 마감)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상 과열)\n"
                    "▫️ 조치: 금일 프리장 SOXS 및 SOXL 신규 매수 진입 전면 차단 및 관망 전환"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 2차 & 3차 방어선: 보유 중 손실권(0 < 현재가 < 평단가)에서 NQ 2.0% 돌파 또는 방향성 인지 동적 임계치 역행 시 덤핑 (Case 66, 67, 82)
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
                        await AssassinLedger.lock_all_symbols_macro()
                        if is_macro_breach:
                            reason_msg = f"NQ=F 당일 전체 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 초과 확증)"
                        else:
                            bind_str = " · 하한선 바인딩" if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor) else ""
                            reason_msg = (
                                f"체결 기준 지수({nq_entry_price:.2f}) 대비 <code>+{nq_adverse_pct:.2f}%</code> {adverse_label} "
                                f"(동적 임계치 {dynamic_threshold:.2f}%{bind_str} 이탈)"
                            )
                        await ctx.notify_tg(
                            f"🚨 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 매크로 방어선 붕괴 손실 덤핑 격발</b>\n"
                            f"▫️ 사유: {reason_msg}\n"
                            "▫️ 손익 상태: 마이너스 (계좌 보호 하드 스톱 발동)\n"
                            f"▫️ 조치: 지정가 매도 덫 파기 및 전량 매수 1호가(<code>${bid_1_price:.2f}</code>) 스윕 퇴근 (금일 양방향 매매 종료)"
                        )
        except Exception as e:
            print(f"🚨 [매크로 덤핑 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.idempotency_keys[symbol]["M_DUMP"] = None
            ctx.in_memory_ordering_lock[symbol] = False
        return True
    return False


# Case 48, 53 — 09:30 EST 정규장 개장 시 프리장 미체결 매수 즉시 취소 및 신규 진입 영구 차단
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

            await AssassinLedger.save_state(
                symbol, buy_order_id="", is_session_done=True, whipsaw_target_price=0.0
            )
            buy_order_id = ""
            is_session_done = True
            sym_icon = "🐻" if symbol == "SOXS" else "🐂"

            if canceled_pre_buy:
                await ctx.notify_tg(
                    f"🔒 <b>[09:30 정규장 컷오프 {sym_icon}{symbol}] 프리장 미체결 매수 주문 선취소 완료</b>\n"
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


# MODIFIED: 통합 시드 풀(Unified Seed Pool) 기반 실제 발생 수익금((매도가 - 매수가) * 체결수량 - 왕복수수료) 복리 정산 및
#           프리장 내 청산 완료 시 SOXS/SOXL 양쪽 장부 동시 무한 재출동 락 해제 (Case 52, 76, 80)
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
        active_target_rate = AssassinLedger.normalize_target_rate(current_state[2])
        prev_target_sell_price = float(current_state[6] or 0.0)
        prev_entry_session = str(current_state[8] or "")
        is_compound = await AssassinLedger.get_is_compound(symbol)
        whipsaw_tp_saved = await AssassinLedger.get_whipsaw_target_price(symbol)
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"

        now_est_check = datetime.now(EST_TZ)
        est_hm_now = now_est_check.hour * 100 + now_est_check.minute
        is_premarket_now = (400 <= est_hm_now <= 929)
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

        # Case 19 & Case 76: API 인덱싱 지연 시 수동 매매(1주 등) 및 1주 휩소 작전의 실제 수량 폴백 보정
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
        if is_whipsaw_0407_exit and entry_price > 0.0 and exit_price > 0.0:
            is_take_profit_exit = (exit_price > entry_price)

        if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0.0:
            pnl_amt = (exit_price * filled_qty) - (entry_price * filled_qty)
            pnl_rate = (exit_price / entry_price - 1.0) * 100.0
            sign = "+" if pnl_amt >= 0 else ""
            pnl_str = (
                f"\n▫️ 타점 ({int(filled_qty)}주): 매수 <code>${entry_price:.2f}</code> ➡️ 매도 <code>${exit_price:.2f}</code>"
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

        # 초과 Case 52 & Case 80 — 프리장 (04:00 ~ 09:29 EST) 내 익절 완료, 매수 취소, 또는 1주 휩소 작전(WHIPSAW_1SH) 정리 시
        # SOXS/SOXL 양쪽 장부 모두 is_session_done=False로 복원하여 04:07 ~ 09:29 EST 본진 양방향 대칭 스나이퍼 무한 출동 활성화!
        if (
            is_premarket_now
            and prev_entry_session != "CANCELED_MACRO"
            and (is_take_profit_exit or is_buy_cancel_exit or prev_entry_session == "WHIPSAW_1SH")
        ):
            next_is_session_done = False
            next_entry_session = ""
        else:
            next_is_session_done = True
            next_entry_session = "CANCELED_MACRO" if prev_entry_session == "CANCELED_MACRO" else ""

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

        # 통합 복리 시드를 반대편 종목 원장에도 즉시 동기화 (Case 76)
        if updated_budget is not None:
            await AssassinLedger.sync_global_settings(budget=updated_budget)

        # 프리장 내 재출동 조건 충족 시 반대편 종목 잠금도 함께 해제 (Case 52, 80)
        if not next_is_session_done:
            await AssassinLedger.unlock_all_symbols_for_reentry()

        # Case 46: 스나이퍼 OFF 상태 중 수동 청산 식별 시 Silent Reset (알람 타전 억제)
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
            if is_whipsaw_0407_exit:
                await ctx.notify_tg(
                    f"⏱️ <b>[듀얼 휩소 작전 04:07 정산 완료 {sym_icon}{symbol}] 1주 현재가 매도 체결 ➡️ 본진 듀얼 스나이퍼 인계</b>\n"
                    f"▫️ 잔고 0주 (04:07 타임쉴드 해제 완료){pnl_str}{compound_str}\n"
                    "▫️ 후속 조치: 04:07 ~ 09:29 EST 본진 양방향 대칭 클라이맥스 듀얼 스나이퍼 무한 출동 감시 개시"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 휩소 작전 1주 익절 성공 {sym_icon}{symbol}] 데이장 VWAP 타점 달성 ➡️ 본진 대기</b>\n"
                    f"▫️ 잔고 0주 (1주 지정가 매도 덫 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 후속 조치: 04:07 ~ 09:29 EST 본진 양방향 대칭 클라이맥스 듀얼 스나이퍼 무한 출동 감시 대기"
                )
        elif is_take_profit_exit:
            if not next_is_session_done:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 프리장 익절 성공 ➡️ 양방향 대칭 무한 재출동 복귀</b>\n"
                    f"▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 후속 조치: 09:29 EST까지 차기 양방향 대칭 클라이맥스(≤30 &amp; 반대≥80) 연속 감시 재개"
                )
            else:
                await ctx.notify_tg(
                    f"🎉 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 정규장 익절 타격 성공 (당일 퇴근 완료)</b>\n"
                    f"▫️ 잔고 0주 (지정가 익절 매도 체결 확인){pnl_str}{compound_str}\n"
                    "▫️ 당일 신규 진입 권한 영구 소각"
                )
        elif is_manual_exit:
            await ctx.notify_tg(
                f"🛑 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 수동 매도/매크로 청산 감지 (퇴근 락온 완료)</b>\n"
                f"▫️ 잔고 0주 (청산 완료 식별){pnl_str}{compound_str}\n"
                "▫️ 당일 신규 진입 권한 영구 소각 완료"
            )
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# MODIFIED: 프리장 골든타임 (04:07 ~ 09:29 EST · 04:07 타임쉴드 적용) 양방향 대칭 클라이맥스 단일화 타격
#           — (SOXS RSI <= 30.0 AND SOXL RSI >= 80.0) 또는 (SOXL RSI <= 30.0 AND SOXS RSI >= 80.0) 포착 시
#             단일 포지션 배타적 뮤텍스 검증 후 1차 슬리피지(-0.18%) 지정가 매수 덫 선(先) 투하 (Case 54, 59, 71, 81)
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

    est_hm = now_est.hour * 100 + now_est.minute
    # Case 53 & Case 80: 03:58 ~ 04:06:59 EST 동안은 듀얼 1주 휩소 노출 작전 시간대이므로 본진 스나이퍼는 04:07 ~ 09:29 EST에만 작동!
    if not (407 <= est_hm <= 929):
        return

    # Case 54 & Case 81: 양방향 대칭 클라이맥스 단일화 게이트 (매수 대상 RSI <= 30.0 AND 반대 종목 RSI >= 80.0 필수)
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

            # Case 71: 양쪽 장부 활성 여부, 당일 마감 잠금, 매크로 차단, 인메모리 보유 수량, 미체결 매수/매도 교차 검증
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

            active_rate = AssassinLedger.normalize_target_rate(target_profit_rate)

            # Case 61 & Case 66: 타격 직전 NQ=F 실시간 진폭 강제 갱신 및 1차 방어선(2.0% 미만) 검증
            nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                _STRIKE_ALERT_CACHE["last_macro1_date"] = now_est.strftime("%Y-%m-%d")
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 대세장 과열 감지 (진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 금일 SOXS 및 SOXL 매수 권한 영구 소각 및 관망 퇴근"
                )
                return

            # Case 54 & Case 81: 뮤텍스 내부에서 양쪽 최신 현재가를 일괄 조회하여 대칭 클라이맥스 조건(<=30.0 & 반대>=80.0) 이중 검증
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

            # Case 71: SOXS 및 SOXL 양쪽 모두 토스 API 실제 잔고 및 OPEN 주문 이중 프로빙 (중복/동시 진입 원천 차단)
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

            # 30초간 발생하는 하락 언더슈팅 슬리피지(-0.18%, 최소 -$0.02)를 이용한 1차 매수 덫 단가 산출
            slip_disc = max(
                BUY_TRAP_MIN_DISC_USD,
                round(cur_p_floored * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2),
            )
            trap_buy_price = math.floor((cur_p_floored - slip_disc) * 100.0) / 100.0
            if trap_buy_price <= 0.0:
                trap_buy_price = cur_p_floored

            # Case 65: 30초 내/후 현재가 상향 정정(modify_order) 시 예수금 부족(422)이 발생하지 않도록
            # 현재가(cur_p_floored) 및 1.0% 안전 버퍼(0.990) 기준으로 주문 수량을 산출
            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.990)
            except Exception:
                actual_budget = budget

            target_qty = int(math.floor(actual_budget / cur_p_floored))

            # Case 18 & Case 65 & Case 74 — target_qty <= 0 발생 시 무음 스킵 방지 및 세션당 1회 텔레그램 경고 타전
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
            comp_tail = f"목표 +{active_rate:.2f}% · 실수익 복리 증액" if is_compound else "단리 시드 고정"
            trend_str = "SHORT" if symbol == "SOXS" else "LONG"

            # 제5헌법 & Case 34: 토스증권 멱등성 키(clientOrderId) 생성 및 주입
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
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="PRE_SNIPER",
                    entry_trend=trend_str,
                    entry_time=time.time(),
                    nq_entry_price=nq_c_global,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                # 반대편 종목도 동시 진입 방지를 위해 잠금 동기화 (단일 포지션 배타적 뮤텍스)
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                _STRIKE_ALERT_CACHE["last_err_msg"] = ""
                ctx.wakeup_event.set()
                disc_actual = cur_p_floored - trap_buy_price
                await ctx.notify_tg(
                    f"🪤 <b>[프리장 듀얼 스나이퍼] {sym_icon}{symbol} 대칭 클라이맥스 포착 ➡️ 1차 슬리피지 매수 덫 투하</b>\n"
                    f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≤30.0) &amp; {opp_icon}{opp_sym} RSI <code>{opp_rsi_recheck:.1f}</code> (≥80.0)\n"
                    f"▫️ 포착 현재가: <code>${cur_p_floored:.2f}</code> ➡️ 1차 매수 덫(LIMIT): <code>${trap_buy_price:.2f}</code> (슬리피지 <code>-${disc_actual:.2f}</code> 할인 · {mode_tag} <code>${budget:,.2f}</code> ➔ <code>{target_qty}주</code>)\n"
                    f"▫️ NQ=F 기준점: <code>{nq_c_global:.2f}</code> (당일 진폭 <code>{nq_amp_global:.2f}%</code>)\n"
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


# 조건부 30초 타임쉴드 및 즉각 매수 전환 플러그인 (SOXS / SOXL 본진 PRE_SNIPER 공통)
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
    if not (407 <= est_hm <= 929):
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

        # 1순위 경로: 전량 미체결(PENDING) 상태일 때 토스 주문 정정 API(modify_order)로 현재가 상향 정정 (Case 69)
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
                    "▫️ 후속 조치: <code>전량 체결 즉시 목표 익절 매도 덫 자동 장전</code>"
                )
                return new_buy_id, "PRE_SNIPER_REARM"
            except Exception as mod_err:
                print(f"🚨 [재장전 정정 폴백 {symbol}] modify_order 실패 -> 선취소 후 재발사 전환: {mod_err}", flush=True)

        # 2순위 폴백 경로: 부분 체결(PARTIAL_FILLED) 상태이거나 급등으로 정정이 거절된 경우 선취소 후 처리
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
                f"▫️ 잔여 미체결분 취소 완료 ➡️ 확보된 저가 물량(<code>{filled_after}주</code>) 즉시 익절 매도 덫 장전 직행"
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
                "▫️ 후속 조치: <code>전량 체결 즉시 목표 익절 매도 덫 자동 장전</code>"
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


async def manage_mechanical_profit_trap_plugin(
    ctx: SniperPluginContext, symbol: str, now_est: datetime, current_session_id: str,
    is_active: bool, just_turned_off: bool, holdings_qty: int, holdings_detail: dict,
    last_buy_price: float, target_profit_rate: float, target_sell_price: float,
    buy_order_id: str, sell_order_id: str, entry_session: str, runtime_state: dict,
):
    if holdings_qty > 0:
        runtime_state["last_known_qty"] = holdings_qty

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
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # Case 77: 포지션 보유 및 매도 덫 대기 중 제어반(/settlement)에서 목표 익절률(+0.64% <-> +1.00%) 변경 시 자동 선취소 재장전
    if (
        holdings_qty > 0
        and sell_order_id
        and is_active
        and avg_price > 0.0
        and target_sell_price > 0.0
        and entry_session != "WHIPSAW_1SH"
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
                if entry_session == "WHIPSAW_1SH":
                    whipsaw_tp = await AssassinLedger.get_whipsaw_target_price(symbol)
                    if whipsaw_tp > 0.0:
                        calculated_target = math.ceil(whipsaw_tp * 100.0) / 100.0
                    else:
                        calculated_target = math.ceil(avg_price * 1.01 * 100.0) / 100.0
                    exp_w_rate = ((calculated_target / avg_price - 1.0) * 100.0) if avg_price > 0.0 else 1.00
                    trap_tag = f"듀얼 휩소 작전 1주 체결(목표 VWAP +{exp_w_rate:.2f}%)"
                else:
                    calculated_target = math.ceil(avg_price * (1.0 + active_rate / 100.0) * 100) / 100.0
                    if entry_session == "PRE_SNIPER":
                        trap_tag = f"프리장 1차 슬리피지 매수덫 체결(+{active_rate:.2f}%)"
                    elif entry_session == "PRE_SNIPER_REARM":
                        trap_tag = f"프리장 타임쉴드 현재가 재장전 체결(+{active_rate:.2f}%)"
                    else:
                        trap_tag = f"수동 스나이퍼 개입({holdings_qty}주 · +{active_rate:.2f}%)"

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
                    trend_str = "SHORT" if symbol == "SOXS" else "LONG"
                    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"

                    kwargs = {
                        "price": avg_price, "target_sell_price": calculated_target,
                        "sell_order_id": new_sell_id, "is_session_done": True,
                    }

                    if not buy_order_id and entry_session not in ("PRE_SNIPER", "PRE_SNIPER_REARM", "WHIPSAW_1SH"):
                        kwargs.update({
                            "entry_session": "MANUAL", "entry_trend": trend_str,
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now,
                        })
                    else:
                        kwargs.update({
                            "nq_entry_price": nq_c_now, "nq_entry_amp": nq_amp_now,
                        })

                    await AssassinLedger.save_state(symbol, **kwargs)
                    # 수동 매수 포함 포지션 보유 중 반대 종목 신규 진입 잠금 동기화 (Case 71)
                    await AssassinLedger.save_state(opp_sym, is_session_done=True)

                    gross_tp_pct = ((calculated_target / avg_price - 1.0) * 100.0) if avg_price > 0.0 else active_rate
                    est_net_usd = round(
                        (calculated_target - avg_price) * trap_qty
                        - (calculated_target + avg_price) * trap_qty * 0.0007,
                        2,
                    )
                    await ctx.notify_tg(
                        f"🟢 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] {trap_tag} 지정가 매도 덫 장전 완료</b>\n"
                        f"▫️ 팩트 평단가: <code>${avg_price:.2f}</code> (전량 <code>{trap_qty}주</code>)\n"
                        f"▫️ 목표 익절가: <code>${calculated_target:.2f}</code> (+{gross_tp_pct:.2f}% 지정가 · 예상 실순수익 <code>+${est_net_usd:,.2f}</code>)\n"
                        f"▫️ 상태: <code>안전 익절 대기 락온 (방향성 인지 매크로 2·3차 방어망 가동 중)</code>"
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
                    await ctx.notify_tg(f"🚨 <b>[TRAP 에러 {sym_icon}{symbol}]</b> {html.escape(err_str)}")
                    runtime_state["last_trap_error_msg"] = err_str
                await asyncio.sleep(5.0)
            finally:
                ctx.in_memory_ordering_lock[symbol] = False
