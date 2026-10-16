# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장 (04:00 ~ 09:29 EST) SOXS 단방향 RSI<=30 무한 출동 스나이퍼 및 매크로 3단 방어 상태 머신
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 03:58 / 04:07 암살자 타격 함수 영구 소각
# MODIFIED: 제3헌법 의거 롱(SOXL)/숏(SOXS) RSI<=20 및 RSI<=10 패닉 셀 텔레그램 푸시 알림 기능 영구 폐기
# NEW: 프리장 골든타임 (04:00 ~ 09:29 EST) SOXS RSI<=30.0 포착 즉시 1차 슬리피지(-0.18%) 지정가 매수 덫 투하 및 조건부 30초 타임쉴드(RSI>30 돌파 시 즉각 현재가 매수 / RSI<=30 유지 시 30초 후 현재가 매수) 엔진 탑재 (Case 15, 25, 52, 53, 59, 69)
# NEW: 프리장 타격 시 시드/USD 예수금 부족(target_qty <= 0) 및 주문 API 거절 예외 발생 시 무음 스킵 방지 텔레그램 타전망 결속 (Case 17, 18, 65, 74)
# NEW: 09:30 EST 정규장 개장 정각 신규 매수 원천 봉쇄 및 프리장 미체결 매수 주문(buy_order_id) 즉각 선취소 (Case 48, 53)
# MODIFIED: 프리장 (04:00 ~ 09:29 EST) 내 익절 완료 시 is_session_done=False 복원으로 무한 재출동 루프 결속 (Case 52)
# MODIFIED: 매크로 1·2·3차 방어선 작동 시 entry_session="CANCELED_MACRO" 및 당일 재진입 영구 차단 락온 (Case 66, 67)
# MODIFIED: 스나이퍼 OFF 상태 중 수동 청산 식별 시 타전 억제(Silent Reset) 분기 결속 (Case 46)
# NEW: MOC 및 매크로 덤핑 시 미체결 매도 선취소 후 수량 해방(sellableQuantity > 0) 2.0초 폴링 대기 및 거래소 취소 병목 시 기존 매도 주문 가격 즉시 정정(modify_order) 우회 타격망 결속 (Case 16, 24, 69, 75)
# NEW: 전술 제어반 목표 익절률(+0.64% vs +1.00%) 동적 연동, 보유 중 익절률 변경 시 매도 덫 자동 재장전 및 복리 모드(+0.50% / +0.86% 순수익 가산, 손절 시 수수료 0.14% 포함 실손실 차감) 결속 (Case 76, 77)
# MODIFIED: 데이장 (19:00 ~ 03:59 EST) SOXS RSI<=30.0 [모의] 알림 추격 시스템 — 1회 모의 진입 알림 후 현재가가 +1.00% 모의 익절가에 도달하기 전까지 절대 알림을 리셋하지 않고 다중 알림을 원천 차단하는 타점 추적기 탑재 (Case 01, 36, 53, 78)

import math
import time
import asyncio
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram import Bot

from toss_api import TossApiClient
from quant_engine import AssassinLedger, MacroDataCache

EST_TZ = ZoneInfo("America/New_York")
PRE_SNIPER_RSI_THRESHOLD = 30.0

# 데이장 (19:00 ~ 03:59 EST) [모의] 알림 리셋 기준 목표 수익률 (+1.00% 익절가 도달 시에만 리셋)
DAY_SIM_RESET_PROFIT_RATE = 1.00

# NEW: 프리장 (04:00 ~ 09:29 EST) 1차 슬리피지 지정가 매수 덫 & 조건부 30초 타임쉴드 상수
BUY_TRAP_SLIPPAGE_PCT = 0.18          # 1차 매수 덫 슬리피지 할인율 (-0.18%, 예: $44.91 -> $44.83)
BUY_TRAP_MIN_DISC_USD = 0.02          # 1차 매수 덫 최소 할인 폭 (-$0.02 보장)
BUY_TRAP_REARM_WAIT_SEC = 30.0        # RSI <= 30.0 유지 시 1차 매수 덫 최대 타임쉴드 대기 시간 (30.0초)

# 무음 불발(Silent Drop) 방어 및 데이장 [모의] +1.00% 익절 추격 래치 인메모리 캐시 (Case 18, 74, 78)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_err_msg": "",
    "last_macro1_date": "",
    "day_logical_date": "",
    "day_sim_active": False,
    "day_sim_entry_price": 0.0,
    "day_sim_target_price": 0.0,
    "day_sim_min_price": 0.0,
    "day_sim_entry_time_str": "",
}


def get_daymarket_sim_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 데이장 [모의] +1.00% 익절 추적 상태를 조회하기 위한 헬퍼."""
    return {
        "active": bool(_STRIKE_ALERT_CACHE.get("day_sim_active", False)),
        "entry_price": float(_STRIKE_ALERT_CACHE.get("day_sim_entry_price", 0.0)),
        "target_price": float(_STRIKE_ALERT_CACHE.get("day_sim_target_price", 0.0)),
        "min_price": float(_STRIKE_ALERT_CACHE.get("day_sim_min_price", 0.0)),
        "entry_time": str(_STRIKE_ALERT_CACHE.get("day_sim_entry_time_str", "")),
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
    MOC 및 매크로 긴급 덤핑 전 기존 미체결 주문(익절 TRAP 등)을 중복 없이 선취소하고,
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


# MODIFIED: 데이장 (19:00 ~ 03:59 EST) SOXS RSI <= 30.0 [모의] 타점 추적 알림 플러그인
#           — 1회 모의 포착 알림 발송 후 +1.00% 모의 익절가 도달 전까지 알림 리셋을 영구 금지하여 다중 알림 스팸 100% 차단 (Case 78)
async def execute_daymarket_rsi_alert_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_price: float,
    rsi_val: float,
    vwap_val: float = 0.0,
):
    if symbol != "SOXS":
        return

    est_hm = now_est.hour * 100 + now_est.minute
    is_day_market = (est_hm >= 1900 or est_hm <= 359)
    if not is_day_market:
        return

    # Case 01 & 초과 Case 36: 19:00 EST 기준 논리적 거래일 산출 및 새 데이장 개장 시 모의 추적 상태 초기화
    logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 19 else now_est
    logical_date_str = logical_dt.strftime("%Y-%m-%d")

    if _STRIKE_ALERT_CACHE.get("day_logical_date") != logical_date_str:
        _STRIKE_ALERT_CACHE["day_logical_date"] = logical_date_str
        _STRIKE_ALERT_CACHE["day_sim_active"] = False
        _STRIKE_ALERT_CACHE["day_sim_entry_price"] = 0.0
        _STRIKE_ALERT_CACHE["day_sim_target_price"] = 0.0
        _STRIKE_ALERT_CACHE["day_sim_min_price"] = 0.0
        _STRIKE_ALERT_CACHE["day_sim_entry_time_str"] = ""

    state_s = await AssassinLedger.get_state(symbol)
    is_active = bool(state_s[5])
    if not is_active:
        return

    if current_price <= 0.0:
        try:
            current_price = await ctx.client.get_current_price(symbol)
        except Exception:
            current_price = 0.0
    if current_price <= 0.0:
        return

    cur_p_fl = math.floor(current_price * 100.0) / 100.0
    if cur_p_fl <= 0.0:
        return

    # [상태 1] 이미 데이장 [모의] 포지션이 잡혀서 +1.00% 익절가 도달을 추격 중인 상태
    # -> +1.00% 익절가(day_sim_target_price)에 도달하기 전까지는 RSI가 아무리 등락해도 절대 리셋하거나 재알림을 보내지 않음!
    if _STRIKE_ALERT_CACHE.get("day_sim_active", False):
        prev_min_p = float(_STRIKE_ALERT_CACHE.get("day_sim_min_price", 0.0))
        if prev_min_p <= 0.0 or cur_p_fl < prev_min_p:
            _STRIKE_ALERT_CACHE["day_sim_min_price"] = cur_p_fl

        target_p = float(_STRIKE_ALERT_CACHE.get("day_sim_target_price", 0.0))
        entry_p = float(_STRIKE_ALERT_CACHE.get("day_sim_entry_price", 0.0))
        min_p = float(_STRIKE_ALERT_CACHE.get("day_sim_min_price", cur_p_fl))
        entry_t_str = str(_STRIKE_ALERT_CACHE.get("day_sim_entry_time_str", "--:--:--"))

        if target_p > 0.0 and cur_p_fl >= target_p:
            # +1.00% 모의 익절가 도달 확증! -> 모의 익절 완료 타전 후 차기 RSI<=30 감시 리셋
            _STRIKE_ALERT_CACHE["day_sim_active"] = False
            _STRIKE_ALERT_CACHE["day_sim_entry_price"] = 0.0
            _STRIKE_ALERT_CACHE["day_sim_target_price"] = 0.0
            _STRIKE_ALERT_CACHE["day_sim_min_price"] = 0.0
            _STRIKE_ALERT_CACHE["day_sim_entry_time_str"] = ""

            sim_gross_pct = ((cur_p_fl / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.00
            sim_net_pct = sim_gross_pct - 0.14
            max_dd_pct = ((min_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0

            await ctx.notify_tg(
                f"🎯 <b>[데이장 모의 익절 달성] {symbol} +1.00% 목표가 도달 ➡️ 알림 락 리셋 완료</b>\n"
                f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code>) ➡️ 도달 <code>${cur_p_fl:.2f}</code> (목표 <code>${target_p:.2f}</code>)\n"
                f"▫️ 경유 최저가: <code>${min_p:.2f}</code> (최대 역행 <code>{max_dd_pct:+.2f}%</code>)\n"
                f"▫️ 모의 수익률: 세전 <code>+{sim_gross_pct:.2f}%</code> / 세후 순수익 <code>+{sim_net_pct:.2f}%</code> (수수료 0.14% 차감)\n"
                "▫️ 후속 조치: <code>+1.00% 익절 도달이 확인되어 데이장 차기 SOXS RSI≤30 모의 감시가 재장전되었습니다.</code>"
            )
        return

    # [상태 2] 미진입 대기 상태(day_sim_active == False)에서 SOXS RSI <= 30.0 최초 포착 시
    if 0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD:
        sim_tp_100 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0
        sim_tp_064 = math.ceil(cur_p_fl * 1.0064 * 100.0) / 100.0
        slip_disc = max(BUY_TRAP_MIN_DISC_USD, round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2))
        sim_trap_buy = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)

        _STRIKE_ALERT_CACHE["day_sim_active"] = True
        _STRIKE_ALERT_CACHE["day_sim_entry_price"] = cur_p_fl
        _STRIKE_ALERT_CACHE["day_sim_target_price"] = sim_tp_100
        _STRIKE_ALERT_CACHE["day_sim_min_price"] = cur_p_fl
        _STRIKE_ALERT_CACHE["day_sim_entry_time_str"] = now_est.strftime("%H:%M:%S")

        if vwap_val <= 0.0 and ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
            vwap_val = float(ctx.shared_rsi_state[symbol].get("vwap", 0.0))

        nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
        nq_amp_global = (
            ((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0
        )
        vwap_str = f"${vwap_val:.2f}" if vwap_val > 0.0 else "집계중"

        await ctx.notify_tg(
            f"🔔 <b>[데이장 모의 관측] {symbol} RSI≤30 포착 ➡️ +1.00% 도달 추격 개시 (매수차단)</b>\n"
            f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> (0세션 데이마켓)\n"
            f"▫️ 실시간 1분봉 RSI: <code>{rsi_val:.1f}</code> (기준 ≤ 30.0)\n"
            f"▫️ 모의 진입가: <code>${cur_p_fl:.2f}</code> (슬리피지 덫 참고가: <code>${sim_trap_buy:.2f}</code>) | VWAP: <code>{vwap_str}</code>\n"
            f"▫️ NQ=F 당일 진폭: <code>{nq_amp_global:.2f}%</code> (현재지수 <code>{nq_c_global:.2f}</code>)\n"
            f"▫️ 리셋 목표가(+1.00%): <code>${sim_tp_100:.2f}</code> (참고 +0.64%: <code>${sim_tp_064:.2f}</code>)\n"
            f"▫️ 다중알림 잠금: <code>현재가가 +1.00% (${sim_tp_100:.2f}) 에 도달하기 전까지 추가 알림이 100% 차단됩니다.</code>"
        )


async def execute_rsi_monitoring_plugin(
    ctx: SniperPluginContext, symbol: str, rsi_val: float, vwap_val: float, current_price: float
):
    if ctx.shared_rsi_state is None:
        return

    state = ctx.shared_rsi_state.get(symbol)
    if not state:
        return

    # 듀얼 관제탑(/avwap) 표출용 실시간 RSI 및 VWAP 상태 갱신
    state["value"] = rsi_val
    state["vwap"] = vwap_val

    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute

    # Case 25 & Case 59: 프리장 (04:00 ~ 09:29 EST) 내 SOXS RSI <= 30.0 포착 또는 1차 매수 덫 중 RSI > 30.0 반등 시 메인 루프 즉시 기상
    if symbol == "SOXS" and (400 <= est_hm <= 929):
        if 0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD or rsi_val > PRE_SNIPER_RSI_THRESHOLD:
            ctx.wakeup_event.set()
    # 데이장 (19:00 ~ 03:59 EST) 내 SOXS [모의] +1.00% 추격 알림 플러그인 실행
    elif symbol == "SOXS" and (est_hm >= 1900 or est_hm <= 359):
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
                                f"🔴 <b>[스나이퍼 {symbol}] 15:59 ~ 16:01 제로-오버나이트 덤핑망 결속</b>\n"
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
                            f"🔴 <b>[스나이퍼 {symbol}] 15:59 ~ 16:01 제로-오버나이트 정정 스윕 결속</b>\n"
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


# MODIFIED: 실제로 장부 정산/초기화가 수행된 경우 True, 미체결 매수 대기 중이라 보류된 경우 False를 반환하도록 개선
async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext, symbol: str, last_buy_price: float,
    buy_order_id: str, sell_order_id: str, is_active: bool = True,
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

        # 선택된 목표 익절률(+0.64% -> 순수익 +0.50%, +1.00% -> 순수익 +0.86%) 연동 복리 시드 자동 갱신 (Case 76)
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

        # 초과 Case 52 — 프리장 (04:00 ~ 09:29 EST) 내 익절 완료(또는 매크로 아닌 단순 매수취소) 시 is_session_done=False로 무한 재출동 활성화
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
            return True

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
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# 프리장 (04:00 ~ 09:29 EST) SOXS RSI<=30.0 포착 즉시(t=0s) 30초 슬리피지(-0.18%)를 반영한 1차 지정가 매수 덫 선(先) 투하
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
                or not bool(self_state[5])
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
            if target_qty <= 0:
                if _STRIKE_ALERT_CACHE.get("last_warn_session") != current_session_id:
                    _STRIKE_ALERT_CACHE["last_warn_session"] = current_session_id
                    bp_str = f"${current_bp:,.2f}" if current_bp >= 0.0 else "조회실패"
                    await ctx.notify_tg(
                        f"⚠️ <b>[프리장 스나이퍼 요격 보류] 주문 수량 0주 산출</b>\n"
                        f"▫️ 포착 RSI: <code>{rsi_val:.1f}</code> | 현재가: <code>${cur_p_floored:.2f}</code>\n"
                        f"▫️ 설정 시드: <code>${budget:,.2f}</code> | USD 가능금액: <code>{bp_str}</code>\n"
                        f"▫️ 사유: 유효 투입금(<code>${actual_budget:,.2f}</code>)이 1주 가격(<code>${cur_p_floored:.2f}</code>) 미만입니다."
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
                    symbol, buy_order_id=new_buy_id, entry_session="PRE_SNIPER", entry_trend="SHORT",
                    entry_time=time.time(), nq_entry_price=nq_c_global, nq_entry_amp=nq_amp_global,
                    is_session_done=True, last_session_id=current_session_id,
                )
                _STRIKE_ALERT_CACHE["last_err_msg"] = ""
                ctx.wakeup_event.set()
                disc_actual = cur_p_floored - trap_buy_price
                await ctx.notify_tg(
                    f"🪤 <b>[프리장 스나이퍼] {symbol} RSI≤30 포착 ➡️ 1차 슬리피지 지정가 매수 덫 투하</b>\n"
                    f"▫️ 실시간 1분봉 RSI: <code>{rsi_val:.1f}</code> | 포착 현재가: <code>${cur_p_floored:.2f}</code>\n"
                    f"▫️ 1차 매수 덫(LIMIT): <code>${trap_buy_price:.2f}</code> (슬리피지 <code>-${disc_actual:.2f}</code> 할인 · {mode_tag}시드 <code>${budget:,.2f}</code> ➔ <code>{target_qty}주</code>)\n"
                    f"▫️ NQ=F 기준점: <code>{nq_c_global:.2f}</code> (당일 진폭 <code>{nq_amp_global:.2f}%</code>)\n"
                    f"▫️ 조건부 30초 타임쉴드: <code>RSI≤30 유지 시 30초간 저가 덫 대기 / 30초 내 RSI 30 초과 반등 시 즉각 현재가 매수 ({comp_tail})</code>"
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


# MODIFIED: 조건부 30초 타임쉴드 및 즉각 매수 전환 플러그인
#           — RSI <= 30.0 이하인 동안에는 최대 30.0초 동안 1차 매수 덫을 유지하며 대기
#           — 30초 이내라도 RSI > 30.0 초과 반등 시 타임쉴드를 즉시 해제하고 현재가로 즉각 정정 매수!
#           — RSI <= 30.0 상태로 30.0초가 경과하면 현재가로 즉시 재장전 매수!
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
        symbol != "SOXS"
        or not is_active
        or not buy_order_id
        or entry_session != "PRE_SNIPER"
        or entry_time <= 0.0
        or ctx.in_memory_ordering_lock[symbol]
    ):
        return buy_order_id, entry_session

    est_hm = now_est.hour * 100 + now_est.minute
    if not (400 <= est_hm <= 929):
        return buy_order_id, entry_session

    elapsed = time.time() - entry_time
    is_rsi_rebound_breakout = (rsi_val > PRE_SNIPER_RSI_THRESHOLD)
    is_shield_time_expired = (elapsed >= BUY_TRAP_REARM_WAIT_SEC)

    # 아직 30.0초 미만이고 실시간 RSI도 여전히 30.0 이하라면 1차 슬리피지 매수 덫을 유지하며 대기!
    if not is_rsi_rebound_breakout and not is_shield_time_expired:
        return buy_order_id, entry_session

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        od = await ctx.client.get_order_detail(buy_order_id)
        status = str(od.get("status") or "")

        # 이미 1차 슬리피지 매수 덫에 걸려 전량 체결되었거나 종료된 경우
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
            trigger_title = "⚡ [프리장 스나이퍼 RSI 30 초과 반등 즉각 매수]"
            trigger_desc = f"타임쉴드 중 실시간 RSI <code>{rsi_val:.1f}</code> (30.0 초과 반등 감지 · 경과 <code>{elapsed:.1f}초</code>)"
        else:
            trigger_title = "🔄 [프리장 스나이퍼 30초 타임쉴드 만료 재장전]"
            trigger_desc = f"RSI≤30 유지 30초 타임쉴드 만료 (현재 RSI <code>{rsi_val:.1f}</code> · 경과 <code>{elapsed:.1f}초</code>)"

        # 현재가가 이미 1차 매수 덫 가격 이하인 상태에서 30초가 만료된 경우 기존 주문 유지 및 재장전 완료 마킹
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
                    f"<b>{trigger_title} {symbol} 현재가 즉시 상향 요격</b>\n"
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

        # 만약 1차 매수 덫에서 이미 일부라도 체결된 물량이 있다면 추가 추격 매수 없이 체결된 저가 물량만 즉시 익절 덫 장전
        if filled_after > 0:
            await AssassinLedger.save_state(
                symbol, buy_order_id="", entry_session="PRE_SNIPER", entry_time=time.time()
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🟢 <b>[프리장 스나이퍼 부분체결 확정] {symbol} 1차 매수 덫 ({filled_after}주) 체결 완료</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 잔여 미체결분 취소 완료 ➡️ 확보된 저가 물량(<code>{filled_after}주</code>) 즉시 익절 매도 덫 장전 직행"
            )
            return "", "PRE_SNIPER"

        # 체결 수량이 0주인 경우 현재 오르는 가격(rearm_price) 기준으로 신규 지정가 매수 재발사
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
                f"<b>{trigger_title} {symbol} 1차 매수 덫 취소 ➡️ 현재가 신규 요격 발사</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 재장전 지정가(LIMIT): <code>${old_trap_price:.2f}</code> ➡️ <code>${rearm_price:.2f}</code> (<code>{rearm_qty}주</code>)\n"
                "▫️ 후속 조치: <code>전량 체결 즉시 목표 익절 매도 덫 자동 장전</code>"
            )
            return new_buy_id, "PRE_SNIPER_REARM"

        await AssassinLedger.save_state(
            symbol, buy_order_id="", is_session_done=False, entry_session="", entry_time=0.0
        )
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

    # 포지션 보유 및 매도 덫 대기 중 사용자가 제어반(/settlement)에서 목표 익절률(+0.64% <-> +1.00%)을 변경한 경우 (Case 77)
    # 기존 매도 덫을 선취소하고 차기 루프(즉시 기상)에서 새로운 목표 익절가로 자동 재장전
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
                if entry_session == "PRE_SNIPER":
                    trap_tag = f"프리장 1차 슬리피지 매수덫 체결(+{active_rate:.2f}%)"
                elif entry_session == "PRE_SNIPER_REARM":
                    trap_tag = f"프리장 타임쉴드 현재가 재장전 체결(+{active_rate:.2f}%)"
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

                    if not buy_order_id and entry_session not in ("PRE_SNIPER", "PRE_SNIPER_REARM"):
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
