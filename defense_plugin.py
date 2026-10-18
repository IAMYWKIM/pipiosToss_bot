# =====================================================================
# FILE: defense_plugin.py
# 목적: 방향성 인지 NQ=F 매크로 3단 방어망(수동 진입 시 2차 과열 방어 패스 &
#       04:07~08:14 EST 내 NQ<2.0% 3차 역행 손절 시 최저점 역트레일링[RE_BOT_TRAIL] 점화),
#       08:15 EST 거시지표 발표 15분 전 선제 컷오프, 15:59~16:01 EST MOC 2단계 방탄 청산
#       및 트레일링(🚀 NQ_VEL_BYPASS 포함)·최저점 추적 런타임 상태 캐시 전담 플러그인 (초과 Case 88, 89)
# =====================================================================
# MODIFIED: 제3헌법 및 초과 Case 88 의거 sniper_plugin.py에서 방어·컷오프·MOC 모듈 분리
# NEW: 초과 Case 89 — NQ_VELOCITY_SURGE_PCT(0.20%) 상수 동기화 및 _TRAIL_RUNTIME_CACHE 내 nq_vel_bypass·nq_vel_10m·nq_vel_3m 슬롯 확장
# MODIFIED: 수동 진입(MANUAL) 포지션 첫 틱 선행 식별 및 NQ=F>=2.0% 구간 2차 과열 방어선 100% 패스·3차 역행 방어 전용 가동 (Case 50, 66, 67, 86)
# MODIFIED: 04:07 ~ 08:14 EST 중 NQ=F<2.0% 내 3차 역행 안전망 손절 시 당일 퇴근 대신 최저점 역트레일링(STAGE3_STOP_TRACK) 점화 (Case 87)
# MODIFIED: 08:15:00 EST 거시지표 선제 컷오프 시 최저점 추적 캐시 파기 및 미체결 매수 주문 즉각 선취소 결속 (Case 48, 53, 84, 87)

import math
import time
import asyncio
import html
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import AssassinLedger, MacroDataCache
from whipsaw_plugin import (
    PRE_SNIPER_RSI_THRESHOLD,
    OPP_CLIMAX_RSI_THRESHOLD,
)

EST_TZ = ZoneInfo("America/New_York")

# 단일 '하이킨 아시 + 고점 트레일링 (HA_HYB_1.0 × 🚀 NQ_VEL_BYPASS)' 핵심 상수 (AssassinLedger 동기화)
TRAIL_ARM_PROFIT_RATE = AssassinLedger.TRAIL_ARM_PROFIT_RATE          # +1.00% 도달 시 트레일링 점화(Arming)
TRAIL_FLOOR_LOCK_RATE = AssassinLedger.TRAIL_FLOOR_LOCK_RATE          # 점화 즉시 최소 +1.00% 익절 보존선 락온
TRAIL_DROP_CALLBACK_PCT = AssassinLedger.TRAIL_DROP_CALLBACK_PCT      # 최고가(Peak) 대비 -0.30% 콜백 스탑
TRAIL_SAFETY_CAP_RATE = AssassinLedger.TRAIL_SAFETY_CAP_RATE          # 거래소 선장전 상단 안전 캡 (+5.00% LIMIT)
NQ_VELOCITY_SURGE_PCT = AssassinLedger.NQ_VELOCITY_SURGE_PCT          # NEW: NQ=F 10분/3분 변화율 ±0.20% 급등락 시 HA음봉 바이패스 임계치 (Case 89)

# 3차 안전망 손절 후 '손절가 하회 최저점 역트레일링 반등 재진입 (🪤 RE_BOT_TRAIL)' 상수 (Case 87)
BOTTOM_REBOUND_PCT = 0.30                                             # 경유 최저점(post_stop_low) 대비 반등 돌파 폭 (+0.30%)
BOTTOM_MAX_RSI_CAP = 38.0                                             # 최저점 반등 재진입 허용 최대 RSI 상한 (38.0 이하)

# 무음 불발(Silent Drop) 방어 인메모리 캐시 (Case 18, 74)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_err_msg": "",
    "last_macro1_date": "",
}

# MODIFIED: 종목별 실시간 하이킨 아시 + 고점 트레일링(HA_HYB_1.0 × 🚀 NQ_VEL_BYPASS) 런타임 추적 캐시 (Case 85, 89)
_TRAIL_RUNTIME_CACHE = {
    sym: {
        "armed": False,
        "armed_ts": 0.0,
        "armed_hm": 0,
        "avg_price": 0.0,
        "arm_price": 0.0,
        "floor_price": 0.0,
        "safety_cap_price": 0.0,
        "peak_price": 0.0,
        "stop_price": 0.0,
        "ha_bull": True,
        "ha_open": 0.0,
        "ha_close": 0.0,
        "nq_vel_bypass": False,
        "nq_vel_10m": 0.0,
        "nq_vel_3m": 0.0,
        "sweep_fired": False,
        "last_sweep_ts": 0.0,
    }
    for sym in ("SOXS", "SOXL")
}

# 3차 역행 안전망 손절 후 '손절가 하회 최저점 역트레일링 추적 (RE_BOT_TRAIL)' 런타임 캐시 (Case 87)
_BOTTOM_TRACK_CACHE = {
    "active": False,
    "date": "",
    "symbol": "",
    "last_stop_price": 0.0,
    "post_stop_low": 0.0,
    "rebound_trig_price": 0.0,
    "dipped_below_stop": False,
    "climax_seen": True,
    "last_new_low_ts": 0.0,
}


def _reset_trailing_runtime_state(symbol: str):
    """보유 포지션 청산 또는 초기화 시 해당 종목의 인메모리 트레일링 상태를 원자적으로 0점 리셋합니다."""
    if symbol in _TRAIL_RUNTIME_CACHE:
        _TRAIL_RUNTIME_CACHE[symbol].update({
            "armed": False,
            "armed_ts": 0.0,
            "armed_hm": 0,
            "avg_price": 0.0,
            "arm_price": 0.0,
            "floor_price": 0.0,
            "safety_cap_price": 0.0,
            "peak_price": 0.0,
            "stop_price": 0.0,
            "ha_bull": True,
            "ha_open": 0.0,
            "ha_close": 0.0,
            "nq_vel_bypass": False,
            "nq_vel_10m": 0.0,
            "nq_vel_3m": 0.0,
            "sweep_fired": False,
            "last_sweep_ts": 0.0,
        })


def get_trailing_runtime_status(symbol: str) -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 메인 루프에서 하이킨 아시 트레일링 상태를 조회하는 헬퍼."""
    st = _TRAIL_RUNTIME_CACHE.get(symbol, {})
    return dict(st) if isinstance(st, dict) else {"armed": False}


def _reset_bottom_track_state(symbol: str = None):
    """3차 안전망 손절 후 최저점 역트레일링 추적 캐시를 원자적으로 0점 리셋합니다 (Case 02, 87)."""
    if symbol is None or _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
        _BOTTOM_TRACK_CACHE.update({
            "active": False,
            "date": "",
            "symbol": "",
            "last_stop_price": 0.0,
            "post_stop_low": 0.0,
            "rebound_trig_price": 0.0,
            "dipped_below_stop": False,
            "climax_seen": True,
            "last_new_low_ts": 0.0,
        })


def _activate_bottom_track_state(symbol: str, stop_price: float, current_price: float, logical_date: str):
    """04:07 ~ 08:14 EST 골든타임 내 NQ=F < 2.0% 상태에서 3차 역행 손절 시 최저점 역트레일링 감시를 점화합니다."""
    sp = math.floor(float(stop_price) * 100.0) / 100.0 if stop_price > 0.0 else 0.0
    cp = math.floor(float(current_price) * 100.0) / 100.0 if current_price > 0.0 else sp
    low_init = min(sp, cp) if (sp > 0.0 and cp > 0.0) else max(sp, cp)
    trig_p = math.ceil(low_init * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0 if low_init > 0.0 else 0.0
    _BOTTOM_TRACK_CACHE.update({
        "active": bool(sp > 0.0),
        "date": str(logical_date or ""),
        "symbol": str(symbol),
        "last_stop_price": sp,
        "post_stop_low": low_init,
        "rebound_trig_price": trig_p,
        "dipped_below_stop": bool(low_init < sp),
        "climax_seen": True,
        "last_new_low_ts": time.time(),
    })


def get_bottom_track_status(symbol: str = None) -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 메인 루프에서 3차 손절 후 최저점 역트레일링 상태를 조회하는 헬퍼."""
    if symbol is not None and _BOTTOM_TRACK_CACHE.get("symbol") != symbol:
        return {"active": False, "symbol": symbol}
    return dict(_BOTTOM_TRACK_CACHE)


async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    sell_order_id: str,
    cancel_all_sides: bool = False,
) -> tuple[int, list[str]]:
    """
    MOC, 매크로 긴급 덤핑, 트레일링 폴백 스윕 및 04:07 휩소 1주 정리 전 기존 미체결 주문을 중복 없이 선취소하고,
    미국 거래소에서 수량 잠금이 해제되어 sellableQuantity > 0이 될 때까지 최대 2.0초(0.25초 * 8회) 폴링 대기합니다 (Case 24, 69, 75).
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


async def execute_moc_zero_overnight_plugin(
    ctx, symbol: str, now_est: datetime, is_active: bool,
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


async def execute_macro_defense_plugin(
    ctx, symbol: str, now_est: datetime, is_active: bool,
    is_session_done: bool, holdings_qty: int, current_price: float, avg_price_dump: float,
    sell_order_id: str, nq_entry_price: float, nq_entry_amp: float, buy_order_id: str = "",
    entry_session: str = "", last_buy_price: float = 0.0,
) -> bool:
    if not is_active or ctx.in_memory_ordering_lock[symbol]:
        return False

    if holdings_qty > 0 and not entry_session:
        st_fallback = await AssassinLedger.get_state(symbol)
        last_buy_price = float(st_fallback[0] or 0.0)
        entry_session = str(st_fallback[8] or "")
        if nq_entry_price <= 0.0:
            nq_entry_price = float(st_fallback[10] or 0.0)
        if nq_entry_amp <= 0.0:
            nq_entry_amp = float(st_fallback[11] or 0.0)

    nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
    nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

    # 수동 진입(MANUAL) 포지션 원자적 식별 및 첫 틱 선행 보호 (Case 86)
    is_manual_pos = bool(
        holdings_qty > 0
        and not buy_order_id
        and entry_session not in ("PRE_SNIPER", "PRE_SNIPER_REARM", "WHIPSAW_1SH")
    )

    if is_manual_pos and (entry_session != "MANUAL" or nq_entry_price <= 0.0):
        if nq_c_global > 0.0:
            nq_entry_price = nq_c_global
            nq_entry_amp = nq_amp_global
        entry_session = "MANUAL"
        trend_str = "SHORT" if symbol == "SOXS" else "LONG"
        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        await AssassinLedger.save_state(
            symbol,
            price=avg_price_dump if avg_price_dump > 0.0 else last_buy_price,
            entry_session="MANUAL",
            entry_trend=trend_str,
            nq_entry_price=nq_entry_price,
            nq_entry_amp=nq_entry_amp,
            is_session_done=True,
        )
        await AssassinLedger.save_state(opp_sym, is_session_done=True)
        is_session_done = True

    dynamic_threshold = max(nq_entry_amp * 0.5, ctx.nq_dynamic_floor) if nq_entry_amp > 0.0 else ctx.nq_dynamic_floor

    if nq_entry_price > 0.0 and nq_c_global > 0.0:
        if symbol == "SOXS":
            nq_adverse_pct = (nq_c_global - nq_entry_price) / nq_entry_price * 100.0
            adverse_label = "상승(숏 역행)"
        else:
            nq_adverse_pct = (nq_entry_price - nq_c_global) / nq_entry_price * 100.0
            adverse_label = "하락(롱 역행)"
    else:
        nq_adverse_pct = 0.0
        adverse_label = "역행"

    is_macro_breach = nq_amp_global >= 2.0
    effective_stage2_breach = False if (is_manual_pos or nq_entry_amp >= 2.0) else is_macro_breach

    is_dynamic_breach = (
        nq_entry_price > 0.0
        and dynamic_threshold > 0.0
        and nq_c_global > 0.0
        and nq_adverse_pct >= dynamic_threshold
    )

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1차 방어선: NQ=F 진폭 2.0% 이상 시 프리장 자동 신규 진입 및 최저점 추적 차단, 미체결 자동 매수 즉각 파기 (Case 66, 82, 84, 87)
    if is_macro_breach and (not is_session_done or bool(buy_order_id) or _BOTTOM_TRACK_CACHE.get("active", False)):
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            _reset_bottom_track_state()
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
                if ctx.shared_holdings.get(chk_sym, 0) > 0:
                    continue
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
            if is_manual_pos:
                await AssassinLedger.save_state(symbol, entry_session="MANUAL", is_session_done=True)
            print(f"🛑 [매크로 1차 진입 차단 {symbol}] NQ=F 진폭 {nq_amp_global:.2f}% 도달. 자동 출동 마감.", flush=True)

            est_hm = now_est.hour * 100 + now_est.minute
            today_str = now_est.strftime("%Y-%m-%d")

            if canceled_any_buy:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    f"🛑 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 대세장 과열 감지 (미체결 매수 긴급 파기)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 프리장 미체결 매수 주문 원자적 파기 및 금일 SOXS/SOXL 자동 매수 권한 영구 소각"
                )
            elif (400 <= est_hm <= 814) and _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 매크로 1차 방어선 작동 (자동 출동 마감)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상 과열)\n"
                    "▫️ 조치: 금일 프리장 SOXS 및 SOXL 자동 신규 진입 전면 차단 (수동 진입 시에는 2차 방어 패스 후 3차 안전망 &amp; HA트레일링 작동)"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 2차(자동 진입 전용) & 3차(자동 + 수동 공통, 방향성 인지 동적 임계치 역행) 방어선: 보유 중 손실권(0 < 현재가 < 평단가) 덤핑
    if (
        (effective_stage2_breach or is_dynamic_breach)
        and holdings_qty > 0
        and avg_price_dump > 0.0
        and 0.0 < current_price < avg_price_dump
    ):
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
                        _reset_trailing_runtime_state(symbol)
                        est_hm_dump = now_est.hour * 100 + now_est.minute
                        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"

                        # 초과 Case 87 — 04:07 ~ 08:14 EST 프리장 골든타임 내이고 당일 NQ=F 진폭이 2.0% 미만인 상태에서
                        # 3차 역행 안전망(is_dynamic_breach)만으로 손절된 경우, 당일 퇴근 대신 최저점 역트레일링 감시(STAGE3_STOP_TRACK) 점화!
                        can_arm_bottom_track = bool(
                            is_dynamic_breach
                            and not effective_stage2_breach
                            and not is_macro_breach
                            and not is_manual_pos
                            and (407 <= est_hm_dump <= 814)
                        )

                        if can_arm_bottom_track:
                            _activate_bottom_track_state(
                                symbol=symbol,
                                stop_price=bid_1_price,
                                current_price=current_price,
                                logical_date=now_est.strftime("%Y-%m-%d"),
                            )
                            await AssassinLedger.save_state(
                                symbol,
                                buy_order_id="",
                                is_session_done=True,
                                entry_session="STAGE3_STOP_TRACK",
                                whipsaw_target_price=0.0,
                            )
                            await AssassinLedger.save_state(
                                opp_sym,
                                buy_order_id="",
                                is_session_done=True,
                                whipsaw_target_price=0.0,
                            )
                            bind_str = " · 하한선 바인딩" if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor) else ""
                            rebound_p_init = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
                            await ctx.notify_tg(
                                f"🚨 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 3차 역행 방어선 칼손절 스윕 ➡️ 🪤 최저점 역트레일링 반등 감시 가동</b>\n"
                                f"▫️ 사유: 진입 기준 지수(<code>{nq_entry_price:.2f}</code>) 대비 <code>+{nq_adverse_pct:.2f}%</code> {adverse_label} "
                                f"(동적 임계치 <code>+{dynamic_threshold:.2f}%</code>{bind_str} 이탈 · NQ 당일진폭 <code>{nq_amp_global:.2f}%</code>)\n"
                                f"▫️ 조치: 상단 안전 캡 파기 및 전량 매수 1호가(<code>${bid_1_price:.2f}</code>) 즉시 손절 발사\n"
                                f"▫️ 🪤 <b>최저점 추적(RE_BOT_TRAIL)</b>: 손절가(<code>${bid_1_price:.2f}</code>) 아래 신저가 경신 중 매수 보류(칼날 회피) ➔ "
                                f"최저점 대비 <code>+{BOTTOM_REBOUND_PCT:.2f}%</code> 양봉 반등(초기 기준 <code>${rebound_p_init:.2f}</code>) 시 08:14 EST까지 즉시 재진입!"
                            )
                        else:
                            _reset_bottom_track_state()
                            await AssassinLedger.lock_all_symbols_macro(force_all=True)
                            if effective_stage2_breach:
                                reason_msg = f"2차 방어선: NQ=F 당일 전체 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 초과 확증)"
                            else:
                                bind_str = " · 하한선 바인딩" if (nq_entry_amp * 0.5 < ctx.nq_dynamic_floor) else ""
                                pos_tag = "수동진입 3차 방어선" if is_manual_pos else "3차 역행 방어선"
                                reason_msg = (
                                    f"{pos_tag}: 진입 기준 지수(<code>{nq_entry_price:.2f}</code>) 대비 "
                                    f"<code>+{nq_adverse_pct:.2f}%</code> {adverse_label} "
                                    f"(동적 임계치 <code>+{dynamic_threshold:.2f}%</code>{bind_str} 이탈)"
                                )
                            await ctx.notify_tg(
                                f"🚨 <b>[듀얼 스나이퍼 {sym_icon}{symbol}] 매크로 방어선 붕괴 손실 덤핑 격발</b>\n"
                                f"▫️ 사유: {reason_msg}\n"
                                "▫️ 손익 상태: 마이너스 (계좌 보호 하드 스톱 발동)\n"
                                f"▫️ 조치: 상단 안전 캡 파기 및 전량 매수 1호가(<code>${bid_1_price:.2f}</code>) 스윕 퇴근 (금일 자동 매매 종료)"
                            )
                        ctx.wakeup_event.set()
        except Exception as e:
            print(f"🚨 [매크로 덤핑 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.idempotency_keys[symbol]["M_DUMP"] = None
            ctx.in_memory_ordering_lock[symbol] = False
        return True
    return False


async def enforce_0930_regular_cutoff_plugin(
    ctx, symbol: str, est_time_int: int, is_session_done: bool, buy_order_id: str
) -> tuple[bool, str]:
    """08:15 EST 거시지표 발표 15분 전 선제 컷오프 (08:15 ~ 15:59 EST 전 구간 감시) (Case 48, 53, 84, 87)."""
    if not (815 <= est_time_int < 1600):
        return is_session_done, buy_order_id

    if _BOTTOM_TRACK_CACHE.get("active", False):
        _reset_bottom_track_state()

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
                    f"🔒 <b>[08:15 거시지표 선제 컷오프 {sym_icon}{symbol}] 미체결 매수 주문 선취소 완료</b>\n"
                    "▫️ 사유: 08:15 EST 거시지표 방어 컷오프 도달 (08:30 지표 쇼크 및 정규장 지연 체결 원천 봉쇄)\n"
                    "▫️ 조치: 미체결 매수 파기 및 당일 신규 매수 영구 마감"
                )
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return is_session_done, buy_order_id

    if not is_session_done:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        print(f"🔒 [08:15 프리장 골든타임 마감 {symbol}] 거시지표 방어 및 정규장 신규 매수 영구 차단.", flush=True)
        return True, buy_order_id

    return is_session_done, buy_order_id


enforce_0815_pre_macro_cutoff_plugin = enforce_0930_regular_cutoff_plugin
