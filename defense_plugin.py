# =====================================================================
# FILE: defense_plugin.py
# 목적: 방향성 인지(Direction-Aware) 매크로 3단 방어망, 08:15 선제 컷오프,
#       15:00 ~ 15:58 EST 미익절 물량 당일 정규장 VWAP 도달 탈출(🛟 VWAP_ESCAPE) 및
#       15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산 코어
#       (defense_plugin.py 2/2 분할본 · defense_state_plugin 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 5대 인메모리 상태 캐시 및 2단계 취소 헬퍼를 defense_state_plugin.py로 분리하고 100% 리익스포트 결속
# NEW: 초과 Case 86 — 수동 진입(MANUAL) 시 첫 틱 기준점 선행 락온 및 2차 과열 방어선(NQ>=2.0%) 100% 패스
# NEW: 초과 Case 92 & Case 94 — 포지션 자체 손실률 기반 즉시 손절 절대 금지, 최소 0.80% 실시간 QQQ 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT) 강제,
#      스나이퍼 OFF->ON 재가동 또는 지난주 만료 주문(CANCELED) 감지 시 현재 실시간 QQQ 가격으로 0.00% 영점 재락온 및 양방향 수동 보유 보호
# MODIFIED: 초과 Case 95 & Case 96 — 1주 휩소 노출 작전(WHIPSAW_1SH)의 미체결 1주 매수덫 및 1주 보유 물량은 시간대와 무관하게
#           1·2차 과열 방어선을 100% 바이패스하여 04:07 정각 정리를 enforce_0407_whipsaw_cutoff_plugin에 전담 위임하고,
#           본진 미체결 매수 1차 파기 시 buy_order_id="" 즉시 소각 및 중복 알림 폭탄을 원천 차단
# NEW: 초과 Case 98 — 정규장 장마감 1시간 전(15:00 ~ 15:58 EST)까지 +1.00% 익절하지 못한 미익절 보유 물량이
#      당일 정규장 누적 VWAP 가격에 도달하면 즉시 2단계 방탄 스윕으로 덤핑 탈출하는 execute_late_regular_vwap_escape_plugin 탑재

import math
import time
import asyncio
import html
from datetime import datetime

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
)
from defense_state_plugin import (
    EST_TZ,
    TRAIL_ARM_PROFIT_RATE,
    TRAIL_FLOOR_LOCK_RATE,
    TRAIL_DROP_CALLBACK_PCT,
    TRAIL_SAFETY_CAP_RATE,
    NQ_VELOCITY_SURGE_PCT,
    BOTTOM_REBOUND_PCT,
    BOTTOM_MAX_RSI_CAP,
    _STRIKE_ALERT_CACHE,
    _AUTO_ENTRY_SESSION_TAGS,
    _make_empty_trail_slot,
    _TRAIL_RUNTIME_CACHE,
    _MANUAL_STAGE3_ANCHOR_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _reset_pre_leader_state,
    mark_pre_pullback_dispatched,
    get_pre_leader_status,
    update_premarket_trend_leader,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    _activate_bottom_track_state,
    get_bottom_track_status,
    _cancel_open_orders_and_wait_sellable,
)


# NEW: 초과 Case 98 — 정규장 장마감 1시간 전(15:00:00 ~ 15:58:59 EST)까지 +1.00% 익절하지 못한
#      미익절 표류 물량이 당일 정규장 누적 VWAP 가격에 도달하면 즉시 덤핑 탈출하는 2단계 방탄 플러그인 (🛟 VWAP_ESCAPE)
async def execute_late_regular_vwap_escape_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    holdings_detail: dict = None,
    avg_price_dump: float = 0.0,
    last_buy_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    runtime_state: dict = None,
    current_price: float = 0.0,
    vwap_val: float = 0.0,
    **kwargs,
) -> bool:
    if runtime_state is None:
        runtime_state = {}

    est_hm = now_est.hour * 100 + now_est.minute
    if symbol not in ("SOXS", "SOXL") or not is_active or not (1500 <= est_hm <= 1558):
        return False

    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))
    if holdings_qty <= 0 or ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    # 이미 +1.00% 돌파로 하이킨 아시 트레일링이 점화(armed == True)된 수익 포지션은 트레일링 엔진에 100% 위임!
    if _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False):
        return False

    avg_p = (
        float(avg_price_dump or 0.0)
        or (float(holdings_detail.get("avg_price", 0.0)) if isinstance(holdings_detail, dict) else 0.0)
        or float(last_buy_price or 0.0)
    )

    live_p = current_price
    if live_p <= 0.0:
        try:
            live_p = await ctx.client.get_current_price(symbol)
        except Exception:
            live_p = 0.0
    if live_p <= 0.0:
        return False

    # 현재가가 이미 +1.00% 트레일링 점화가(arm_p) 이상에 도달한 경우 즉시 하단 트레일링 엔진이 점화하도록 바이패스
    if avg_p > 0.0:
        arm_p = math.ceil(avg_p * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        if live_p >= arm_p:
            return False

    # 당일 정규장 누적 VWAP 조회 (reg_vwap 최우선, 폴백 vwap_val)
    eff_vwap = float(vwap_val or 0.0)
    if ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
        sym_st = ctx.shared_rsi_state.get(symbol) or {}
        ewm_st = sym_st.get("ewm_state") or {}
        reg_vwap = float(ewm_st.get("reg_vwap", 0.0))
        if reg_vwap > 0.0:
            eff_vwap = reg_vwap
        elif eff_vwap <= 0.0:
            eff_vwap = float(sym_st.get("vwap", 0.0))

    vwap_target_p = math.floor(eff_vwap * 100.0) / 100.0 if eff_vwap > 0.0 else 0.0
    if vwap_target_p <= 0.0:
        return False

    # [핵심 조건] 실시간 현재가(live_p)가 당일 정규장 VWAP 가격(vwap_target_p)에 도달했는지 판별!
    if live_p < vwap_target_p:
        return False

    now_ts = time.time()
    last_vwap_ts = float(runtime_state.get("last_vwap_escape_tick", 0.0))
    if now_ts - last_vwap_ts < 1.5:
        return False

    ctx.in_memory_ordering_lock[symbol] = True
    runtime_state["last_vwap_escape_tick"] = now_ts
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else vwap_target_p

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = vwap_target_p

        dispatched = False
        new_s_id = ""
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"VWAP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_s_id = str((res or {}).get("result", {}).get("orderId") or "")
            dispatched = True
        elif surviving_sell_ids:
            new_s_id = surviving_sell_ids[0]
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=new_s_id,
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            dispatched = True

        if dispatched:
            _reset_trailing_runtime_state(symbol)
            _reset_bottom_track_state()
            _reset_pre_leader_state()
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=new_s_id,
                target_sell_price=bid_price,
                is_session_done=True,
                entry_session="VWAP_ESCAPE",
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            if not runtime_state.get("vwap_escape_alert_sent", False):
                runtime_state["vwap_escape_alert_sent"] = True
                pnl_pct_est = ((bid_price / avg_p - 1.0) * 100.0) if avg_p > 0.0 else 0.0
                avg_str = f"${avg_p:.2f}" if avg_p > 0.0 else "집계중"
                await ctx.notify_tg(
                    f"🛟 <b>[장마감 1시간 전 VWAP 탈출 스윕] {sym_icon}{symbol} 미익절 물량 VWAP 도달 즉시 덤핑</b>\n"
                    f"▫️ 발동 사유: 정규장 마감 1시간 전(<code>15:00 ~ 15:58 EST</code>) 미익절 표류 중 당일 정규장 VWAP(<code>${vwap_target_p:.2f}</code>) 도달!\n"
                    f"▫️ 탈출 단가(Bid 1 LIMIT): 평단 <code>{avg_str}</code> ➡️ 현재 <code>${live_p:.2f}</code> / 처분가 <code>${bid_price:.2f}</code> "
                    f"(<code>{holdings_qty}주</code> · 예상 <code>{pnl_pct_est:+.2f}%</code> · 15:59 MOC 대량 손절 선제 회피)"
                )
            return True
    except Exception as e:
        print(f"🚨 [장마감 1시간 전 VWAP 탈출 스윕 방어 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return False


# Case 16 & Case 24: 15:59 ~ 16:01 EST 절대 제로-오버나이트 MOC 2단계 방탄 강제 청산 플러그인 (main.py 호출 시그니처 100% 호환)
async def execute_moc_zero_overnight_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int = None,
    buy_order_id: str = None,
    sell_order_id: str = "",
    runtime_state: dict = None,
    current_price: float = 0.0,
    **kwargs,
) -> bool:
    if runtime_state is None:
        runtime_state = {}

    is_moc_window = (
        (now_est.hour == 15 and now_est.minute >= 59)
        or (now_est.hour == 16 and 0 <= now_est.minute <= 1)
    )
    if not is_moc_window or not is_active:
        return False

    _reset_bottom_track_state()
    _reset_pre_leader_state()
    _reset_trailing_runtime_state(symbol)

    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))
    if buy_order_id is None:
        buy_order_id = await AssassinLedger.get_buy_order_id(symbol)

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1) 미체결 매수 주문 잔존 시 최우선 선취소
    if buy_order_id:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            await ctx.client.cancel_order(buy_order_id)
        except Exception:
            pass
        finally:
            await AssassinLedger.save_state(
                symbol, buy_order_id="", is_session_done=True
            )
            ctx.in_memory_ordering_lock[symbol] = False

    if holdings_qty <= 0:
        return True

    now_ts = time.time()
    last_moc_ts = float(runtime_state.get("last_moc_dump_tick", runtime_state.get("last_moc_tick", 0.0)))
    if now_ts - last_moc_ts < 1.5:
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    runtime_state["last_moc_dump_tick"] = now_ts
    runtime_state["last_moc_tick"] = now_ts
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = current_price
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else 0.01

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = 0.01

        dispatched = False
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"MOC_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_s_id = str((res or {}).get("result", {}).get("orderId") or "")
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=new_s_id,
                target_sell_price=bid_price,
                is_session_done=True,
            )
            dispatched = True
        elif surviving_sell_ids:
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=surviving_sell_ids[0],
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            await AssassinLedger.save_state(
                symbol,
                sell_order_id=surviving_sell_ids[0],
                target_sell_price=bid_price,
                is_session_done=True,
            )
            dispatched = True

        if dispatched and not runtime_state.get("moc_alert_sent", False):
            runtime_state["moc_alert_sent"] = True
            runtime_state["moc_dump_active"] = True
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🛑 <b>[제로-오버나이트 MOC 발동] {sym_icon}{symbol} 장마감 강제 청산 스윕</b>\n"
                f"▫️ 보유 수량: <code>{holdings_qty}주</code> ➡️ 매수 1호가(LIMIT): <code>${bid_price:.2f}</code> 즉시 투하"
            )
    except Exception as e:
        ctx.idempotency_keys[symbol]["MOC"] = None
        print(f"🚨 [MOC 강제 청산 방어 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return True


# MODIFIED: 초과 Case 66, 67, 82, 86, 87, 92, 93, 94, 95, 96, 97, 98, 99 —
#           1) 포지션 자체의 현재 평가손실률(예: 지난주 보유분 -3% ~ -10% 손실 등)을 3차 임계치에 바로 적용하여 즉시 손절하는 것을 100% 금지!
#           2) 1·2차 대세 과열 방어(>=2.0%) 및 3차 동적 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)의 기준은 나스닥 100 선물(NQ=F)이되,
#              최소 0.80% 실시간 QQQ 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80)을 하드 락온!
#           3) 스나이퍼 OFF->ON 재가동 시 또는 수동 물량 미결속(not sell_order_id 또는 만료된 구버전 sell_order_id) 감지 첫 틱에
#              과거 nq_entry_price를 폐기하고 현재 실시간 QQQ 가격(ref_curr_man)으로 0.00% 영점 재조준!
#           4) 1주 휩소 노출 작전(WHIPSAW_1SH)은 시간대와 무관하게 1·2차 과열 방어선(주문 취소 및 손실권 즉시 덤핑)을 100% 바이패스하여
#              04:07 정각 정리를 enforce_0407_whipsaw_cutoff_plugin에 전담 위임하고,
#              본진 미체결 매수 1차 파기 시 buy_order_id="" 즉시 소각 및 중복 알림 폭탄을 원천 차단!
async def execute_macro_defense_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str = None,
    is_active: bool = True,
    is_session_done: bool = False,
    holdings_qty: int = 0,
    holdings_detail: dict = None,
    avg_price_dump: float = 0.0,
    last_buy_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    nq_entry_price: float = 0.0,
    nq_entry_amp: float = 0.0,
    runtime_state: dict = None,
    current_price: float = 0.0,
    **kwargs,
) -> bool:
    if symbol not in ("SOXS", "SOXL"):
        return False

    if holdings_qty <= 0 or not is_active:
        _reset_manual_stage3_anchor(symbol)
        if not is_active:
            return False

    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1. 1·2차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준인 나스닥 100 선물지수(NQ=F) 조회
    nq_curr, nq_high, nq_low = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if nq_low > 0.0 else 0.0

    # MODIFIED: 초과 Case 95 & Case 96 — 1주 휩소 노출 작전(WHIPSAW_1SH)은 시간대(03:58~04:06 및 04:07+ 정리 시점 포함)와 무관하게
    # execute_macro_defense_plugin의 1·2차 과열 방어선을 100% 패스하고 04:07 정각 정리는 enforce_0407_whipsaw_cutoff_plugin이 전담!
    is_whipsaw_1sh_active = (entry_session == "WHIPSAW_1SH")

    # =====================================================================
    # [선행 처리] 수동 진입(MANUAL) 포지션 첫 감지 틱 및 재가동(OFF->ON) 시
    #            실시간 QQQ 기준가 0.00% 영점 재락온 및 NQ=F 진폭 동기화 (초과 Case 50, 86, 92, 94, 97, 98, 99)
    # =====================================================================
    is_untagged_manual_hold = (
        holdings_qty > 0
        and not buy_order_id
        and entry_session not in _AUTO_ENTRY_SESSION_TAGS
    )
    if is_untagged_manual_hold:
        ref_curr_man, ref_src_man = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
        anchor_slot = _MANUAL_STAGE3_ANCHOR_CACHE.setdefault(
            symbol, {"anchored": False, "verified_sell_id": ""}
        )

        # 초과 Case 94 — 장부에 지난주/전세션의 만료된 sell_order_id가 남아있는 경우 실제 유효 여부를 1회 검증하여
        # CANCELED / REJECTED / 404 이면 즉시 폐기하고 현재 실시간 QQQ 가격으로 0.00% 영점 재조준!
        effective_sell_id = str(sell_order_id or "")
        if effective_sell_id and anchor_slot.get("verified_sell_id") != effective_sell_id:
            try:
                od_chk = await ctx.client.get_order_detail(effective_sell_id)
                st_chk = str(od_chk.get("status") or "")
                if st_chk in ("CANCELED", "REJECTED"):
                    effective_sell_id = ""
                else:
                    anchor_slot["verified_sell_id"] = effective_sell_id
                    anchor_slot["anchored"] = True
            except Exception as e_chk:
                if "404" in str(e_chk).lower() or "not-found" in str(e_chk).lower():
                    effective_sell_id = ""

        # 안전 캡 매도 주문(effective_sell_id)이 아직 장전되지 않았거나(신규 수동 매수 / 스나이퍼 OFF->ON 직후 / 지난주 주문 만료),
        # 기준가가 없거나 스케일이 불일치하면 무조건 현재 실시간 QQQ 가격(ref_curr_man)으로 0.00% 영점 재락온!
        need_fresh_reanchor = bool(
            not effective_sell_id
            or not anchor_slot.get("anchored", False)
            or nq_entry_price <= 0.0
            or (
                nq_entry_price > 0.0
                and ref_curr_man > 0.0
                and ((nq_entry_price > 3000.0) != (ref_curr_man > 3000.0))
            )
        )

        need_man_save = False
        save_kwargs = {}

        if effective_sell_id != sell_order_id:
            sell_order_id = ""
            save_kwargs["sell_order_id"] = ""
            save_kwargs["target_sell_price"] = 0.0
            need_man_save = True

        if entry_session != "MANUAL":
            entry_session = "MANUAL"
            save_kwargs["entry_session"] = "MANUAL"
            save_kwargs["entry_trend"] = "SHORT" if symbol == "SOXS" else "LONG"
            save_kwargs["is_session_done"] = True
            need_man_save = True

        if need_fresh_reanchor and ref_curr_man > 0.0:
            nq_entry_price = ref_curr_man
            save_kwargs["nq_entry_price"] = nq_entry_price
            need_man_save = True
            if nq_amp > 0.0:
                nq_entry_amp = nq_amp
                save_kwargs["nq_entry_amp"] = nq_entry_amp
            anchor_slot["anchored"] = True
            anchor_slot["verified_sell_id"] = effective_sell_id
            print(
                f"🛡️ [수동 보유 3차 안전망 0.00% 영점 락온 {symbol}] 기준({ref_src_man}): {nq_entry_price:.2f} "
                f"| NQ진폭: {nq_entry_amp:.2f}% | 최소 역행 한도: +{max(nq_entry_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT):.2f}%",
                flush=True,
            )
        elif nq_entry_amp <= 0.0 and nq_amp > 0.0:
            nq_entry_amp = nq_amp
            save_kwargs["nq_entry_amp"] = nq_entry_amp
            need_man_save = True

        if need_man_save:
            await AssassinLedger.save_state(symbol, **save_kwargs)

    # =====================================================================
    # [1차 방어선] NQ=F 당일 진폭 >= 2.0% 대세 과열 시 본진 미체결 매수 즉시 파기 및 본진 신규 진입 잠금
    #             (단, 초과 Case 95 & 96 의거 1주 휩소 작전 WHIPSAW_1SH 매수덫은 절대 파기하지 않고 04:07 컷오프에 위임하며,
    #              본진 미체결 주문 파기 시에는 buy_order_id=""를 원자적으로 즉시 소각하고 중복 알림을 차단!)
    # =====================================================================
    if nq_amp >= 2.0:
        if _BOTTOM_TRACK_CACHE.get("active", False):
            _reset_bottom_track_state()
        if _PRE_LEADER_CACHE.get("leader_sym"):
            _reset_pre_leader_state()

        if buy_order_id and not is_whipsaw_1sh_active and not ctx.in_memory_ordering_lock.get(symbol, False):
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                already_canceled = False
                try:
                    await ctx.client.cancel_order(buy_order_id)
                except Exception as cancel_e:
                    err_low = str(cancel_e).lower()
                    if "already-filled" in err_low:
                        return False
                    if "already-canceled" in err_low or "404" in err_low or "order-not-found" in err_low:
                        already_canceled = True

                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id="",
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                ctx.wakeup_event.set()

                if not already_canceled and _STRIKE_ALERT_CACHE.get("last_macro1_cancel_id") != buy_order_id:
                    _STRIKE_ALERT_CACHE["last_macro1_cancel_id"] = buy_order_id
                    await ctx.notify_tg(
                        f"🛑 <b>[매크로 1차 방어선 발동] {sym_icon}{symbol} 미체결 매수 주문 즉시 파기</b>\n"
                        f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp:.2f}%</code> (2.0% 대세 과열 돌파)\n"
                        "▫️ 조치: 미체결 매수 취소 완료 및 금일 양 종목 본진 자동 매수 권한 영구 소각"
                    )
                return True
            finally:
                ctx.in_memory_ordering_lock[symbol] = False

        if holdings_qty <= 0 and not buy_order_id:
            if entry_session not in ("CANCELED_MACRO", "WHIPSAW_1SH"):
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
            if (407 <= est_hm <= 814) and _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[매크로 1차 방어선 작동] 나스닥 100 선물(NQ=F) 대세 과열 감지</b>\n"
                    f"▫️ 현재 NQ=F 진폭: <code>{nq_amp:.2f}%</code> (기준 ≥ 2.00%)\n"
                    "▫️ 조치: 금일 SOXS·SOXL 본진 자동 신규 진입 잠금 (수동 진입 시 2차 과열방어 패스 · 3차 QQQ 실시간 최소 0.80% 역행방어 가동)"
                )
            return False

    # =====================================================================
    # [2차 & 3차 방어선] 보유 포지션(holdings_qty > 0) 손실 구간 방어 프로빙 (초과 Case 66, 67, 86, 87, 92, 94, 95, 96, 97, 98)
    # =====================================================================
    if holdings_qty <= 0 or ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    avg_p = (
        float(avg_price_dump or 0.0)
        or (float(holdings_detail.get("avg_price", 0.0)) if isinstance(holdings_detail, dict) else 0.0)
        or float(last_buy_price or 0.0)
    )
    if avg_p <= 0.0:
        return False

    live_p = current_price
    if live_p <= 0.0:
        try:
            live_p = await ctx.client.get_current_price(symbol)
        except Exception:
            live_p = 0.0

    # 수익권(live_p >= avg_p)이거나 이미 하이킨 아시 트레일링이 점화(armed)된 상태에서는 익절 트레일링 엔진이 전담
    # 주의(초과 Case 94): live_p < avg_p 는 단지 손실 구간 전제 조건일 뿐, 포지션 자체 손실률((avg_p - live_p)/avg_p)로 손절하지 않는다!
    if live_p <= 0.0 or live_p >= avg_p:
        return False
    if _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False):
        return False

    # 초과 Case 92 & Case 94 —
    # 1) 동적 임계치 max(eff_entry_amp * 0.5, 0.80%)는 나스닥 100 선물(NQ=F) 진폭을 기준으로 산출하되 최소 0.80% 하한 강제!
    # 2) 산출된 임계치를 적용할 진입 기준가(nq_entry_price) 및 실시간 비교가(ref_curr)는
    #    토스 Open API QQQ 0.0초 실시간 현재가(1.5초 주기 실시간 프로빙)로 비교!
    ref_curr, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)

    # 스케일 불일치 자가 치유 가드 (기존 NQ=F 지수 스케일 > 3000.0 <-> QQQ 스케일 < 3000.0 전환 시 자동 재동기화)
    if nq_entry_price <= 0.0 and ref_curr > 0.0:
        nq_entry_price = ref_curr
        await AssassinLedger.save_state(symbol, nq_entry_price=nq_entry_price)
    elif nq_entry_price > 0.0 and ref_curr > 0.0 and ((nq_entry_price > 3000.0) != (ref_curr > 3000.0)):
        print(
            f"🔄 [3차 방어 기준점 스케일 자가 치유 {symbol}] 기존 {nq_entry_price:.2f} -> 신규({ref_src}) {ref_curr:.2f}",
            flush=True,
        )
        nq_entry_price = ref_curr
        await AssassinLedger.save_state(symbol, nq_entry_price=nq_entry_price)

    eff_entry_amp = nq_entry_amp if nq_entry_amp > 0.0 else nq_amp
    dynamic_threshold = max(
        eff_entry_amp * 0.5,
        float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
        MIN_STAGE3_QQQ_ADVERSE_PCT,
    )

    if nq_entry_price > 0.0 and ref_curr > 0.0:
        if symbol == "SOXS":
            nq_adverse_pct = (ref_curr - nq_entry_price) / nq_entry_price * 100.0
            adverse_dir_str = "상승(숏 역행)"
        else:
            nq_adverse_pct = (nq_entry_price - ref_curr) / nq_entry_price * 100.0
            adverse_dir_str = "하락(롱 역행)"
    else:
        nq_adverse_pct = 0.0
        adverse_dir_str = "역행"

    # 초과 Case 86, 94, 95, 96, 97, 98, 99: 수동 진입(MANUAL) 포지션과 1주 휩소 작전(WHIPSAW_1SH) 포지션은
    # 2차 과열 방어선(NQ=F >= 2.0%)을 100% 패스(Bypass)하며,
    # 오직 진입/락온 시점 QQQ 기준가 대비 실시간 QQQ가 최소 0.80%(dynamic_threshold) 이상 역행했을 때만 3차 손절 격발!
    is_manual_pos = (
        entry_session == "MANUAL"
        or is_untagged_manual_hold
        or (
            nq_entry_amp >= 2.0
            and entry_session not in _AUTO_ENTRY_SESSION_TAGS
        )
    )
    effective_stage2_breach = (
        (nq_amp >= 2.0)
        and (not is_manual_pos)
        and (entry_session not in ("WHIPSAW_1SH", "VWAP_ESCAPE"))
    )
    is_stage3_breach = (
        nq_entry_price > 0.0
        and ref_curr > 0.0
        and nq_adverse_pct >= dynamic_threshold
        and nq_adverse_pct >= MIN_STAGE3_QQQ_ADVERSE_PCT
    )

    if not effective_stage2_breach and not is_stage3_breach:
        return False

    now_ts = time.time()
    if runtime_state is not None:
        if now_ts - float(runtime_state.get("last_macro_dump_tick", 0.0)) < 1.5:
            return False
        runtime_state["last_macro_dump_tick"] = now_ts

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        bid_price = math.floor(latest_p * 100.0) / 100.0 if latest_p > 0.0 else 0.01

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0]["price"]) > 0.0:
                bid_price = float(bids[0]["price"])
        except Exception:
            pass

        if bid_price <= 0.0:
            bid_price = 0.01

        dump_dispatched = False
        new_sell_id = ""
        if sellable_q_int > 0:
            dump_qty = min(holdings_qty, sellable_q_int)
            idem_key = f"MDMP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            res = await asyncio.wait_for(
                ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    quantity=dump_qty,
                    price=f"{bid_price:.2f}",
                    client_order_id=idem_key,
                ),
                timeout=4.0,
            )
            new_sell_id = str((res or {}).get("result", {}).get("orderId") or "")
            dump_dispatched = True
        elif surviving_sell_ids:
            new_sell_id = surviving_sell_ids[0]
            await asyncio.wait_for(
                ctx.client.modify_order(
                    order_id=new_sell_id,
                    price=f"{bid_price:.2f}",
                    order_type="LIMIT",
                    market_country="US",
                ),
                timeout=4.0,
            )
            dump_dispatched = True

        if dump_dispatched:
            _reset_trailing_runtime_state(symbol)
            can_bottom_track = (
                not effective_stage2_breach
                and is_stage3_breach
                and (407 <= est_hm <= 814)
                and (nq_amp < 2.0)
            )
            ref_unit = "$" if ref_src == "QQQ실시간" else ""

            if can_bottom_track:
                # 초과 Case 87: 04:07 ~ 08:14 EST 골든타임 내 NQ=F < 2.0% 상태에서 3차 역행 손절 시
                #               당일 퇴근하지 않고 최저점 역트레일링 반등 감시(STAGE3_STOP_TRACK) 점화!
                _activate_bottom_track_state(
                    symbol=symbol,
                    stop_price=bid_price,
                    current_price=min(live_p, bid_price),
                    logical_date=today_str,
                )
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_sell_id,
                    target_sell_price=bid_price,
                    is_session_done=True,
                    entry_session="STAGE3_STOP_TRACK",
                    whipsaw_target_price=0.0,
                )
                ctx.wakeup_event.set()
                trig_rebound_p = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
                await ctx.notify_tg(
                    f"🚨 <b>[매크로 3차 실시간 역행 손절 ➡️ 🪤최저점 반등 대기] {sym_icon}{symbol} 칼손절 스윕 발사</b>\n"
                    f"▫️ 역행 사유({ref_src}): 진입 기준 <code>{ref_unit}{nq_entry_price:.2f}</code> ➡️ 현재 <code>{ref_unit}{ref_curr:.2f}</code> "
                    f"(<code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_str} · NQ진폭 <code>{eff_entry_amp:.2f}%</code> 기반 한도 <code>+{dynamic_threshold:.2f}%</code> 돌파)\n"
                    f"▫️ 손절 단가(Bid 1 LIMIT): 평단 <code>${avg_p:.2f}</code> ➡️ <code>${bid_price:.2f}</code> (<code>{holdings_qty}주</code>)\n"
                    f"▫️ 🪤 최저점 역트레일링 점화: <code>손절 체결 즉시 최저점 추적 개시 (초기 반등목표 +{BOTTOM_REBOUND_PCT:.2f}% = ${trig_rebound_p:.2f} · 08:14 EST까지 재출격 대기)</code>"
                )
            else:
                _reset_bottom_track_state()
                _reset_pre_leader_state()
                # 초과 Case 94 ③ — force_all=False를 전달하여 반대편 종목이 수동 보유(MANUAL) 중일 때
                # 반대편 종목의 MANUAL 표식과 QQQ 기준가가 소각되지 않도록 100% 보호!
                await AssassinLedger.lock_all_symbols_macro(
                    last_session_id=current_session_id, force_all=False
                )
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_sell_id,
                    target_sell_price=bid_price,
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    whipsaw_target_price=0.0,
                )
                ctx.wakeup_event.set()
                if effective_stage2_breach:
                    reason_msg = f"2차 대세장 과열 방어 (NQ=F 당일 진폭 <code>{nq_amp:.2f}%</code> ≥ 2.00%)"
                else:
                    mode_tag_s = "수동 3차 역행 방어" if is_manual_pos else "3차 역행 방어"
                    reason_msg = (
                        f"{mode_tag_s} ({ref_src} 기준 <code>{ref_unit}{nq_entry_price:.2f}</code> ➡️ 현재 <code>{ref_unit}{ref_curr:.2f}</code> · "
                        f"<code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_str} / NQ진폭 기반 한도 <code>+{dynamic_threshold:.2f}%</code>)"
                    )
                await ctx.notify_tg(
                    f"🛑 <b>[매크로 방어선 손절 덤핑 &amp; 당일 마감] {sym_icon}{symbol} 매수 1호가 즉시 스윕</b>\n"
                    f"▫️ 발동 사유: {reason_msg}\n"
                    f"▫️ 처분 단가(Bid 1 LIMIT): 평단 <code>${avg_p:.2f}</code> ➡️ <code>${bid_price:.2f}</code> (<code>{holdings_qty}주</code>)\n"
                    "▫️ 후속 조치: 금일 자동 신규 진입 권한 영구 소각 완료"
                )
            return True
    except Exception as e:
        ctx.idempotency_keys[symbol]["M_DUMP"] = None
        print(f"🚨 [매크로 방어 덤핑 에러 {symbol}] {e}", flush=True)
    finally:
        ctx.in_memory_ordering_lock[symbol] = False

    return False


# 초과 Case 48, 84 & 97: 08:15:00 EST 거시지표 발표 15분 전 선제 컷오프
# (08:15 ~ 15:58 EST 전 구간 신규 매수 봉쇄, 최저점 추적 캐시 및 상승 주도주 캐시 파기, 미체결 매수 즉시 선취소)
async def enforce_0930_regular_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime = None,
    est_time_int: int = None,
    is_active: bool = True,
    is_session_done: bool = False,
    holdings_qty: int = None,
    buy_order_id: str = "",
    **kwargs,
) -> tuple[bool, str]:
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = int(est_time_int) if est_time_int is not None else (now_est.hour * 100 + now_est.minute)
    if holdings_qty is None:
        holdings_qty = int(ctx.shared_holdings.get(symbol, 0))

    if not is_active or not (815 <= est_hm <= 1559):
        return is_session_done, buy_order_id

    # 08:15 EST 진입 즉시 최저점 역트레일링 추적 캐시 및 프리장 상승 주도주 캐시 파기 (Case 48, 84, 97)
    if _BOTTOM_TRACK_CACHE.get("active", False):
        _reset_bottom_track_state()
    if _PRE_LEADER_CACHE.get("leader_sym"):
        _reset_pre_leader_state()

    if buy_order_id and not ctx.in_memory_ordering_lock.get(symbol, False):
        ctx.in_memory_ordering_lock[symbol] = True
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"
        try:
            canceled_ok = False
            try:
                await ctx.client.cancel_order(buy_order_id)
                canceled_ok = True
            except Exception as ce:
                if "already-filled" in str(ce):
                    return is_session_done, buy_order_id

            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                for o in open_orders:
                    if o.get("side") == "BUY" and o.get("orderId"):
                        try:
                            await ctx.client.cancel_order(str(o["orderId"]))
                            canceled_ok = True
                        except Exception:
                            pass
            except Exception:
                pass

            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                is_session_done=True,
                entry_session="",
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            if canceled_ok:
                await ctx.notify_tg(
                    f"🔒 <b>[08:15 거시지표 선제 컷오프] {sym_icon}{symbol} 미체결 매수 주문 즉시 파기</b>\n"
                    "▫️ 사유: 08:15 EST 거시경제 지표 발표 15분 전 리스크 방어 컷오프 도달\n"
                    "▫️ 조치: 미체결 매수 주문 선취소 및 금일 신규 자동 진입 마감"
                )
            return True, ""
        except Exception as e:
            print(f"🚨 [08:15 컷오프 매수 취소 방어 {symbol}] {html.escape(str(e))}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
        return is_session_done, buy_order_id

    if not is_session_done and holdings_qty <= 0:
        await AssassinLedger.save_state(symbol, is_session_done=True)
        return True, buy_order_id

    return is_session_done, buy_order_id


# 하위 호환 별칭 유지 (Case 84, 88)
enforce_0815_pre_macro_cutoff_plugin = enforce_0930_regular_cutoff_plugin
