# =====================================================================
# FILE: defense_plugin.py
# 목적: 방향성 인지(Direction-Aware) 매크로 3단 방어망(1·2차 NQ=F 2.0% 과열방어 [수동·1주휩소 패스] &
#       3차 NQ=F 진폭 기반 임계치[최소 0.80%] × 토스 Open API QQQ 0.0초 실시간 역행방어 및 최저점 역트레일링 점화),
#       08:15 EST 거시지표 선제 컷오프, 15:00 ~ 15:58 EST 장마감 1시간 전 미익절 물량 VWAP 도달 즉시 탈출(🛟 VWAP_ESCAPE),
#       15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산 코어 플러그인
#       (defense_plugin.py 2/2 분할본 · defense_state_plugin 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제3헌법 및 초과 Case 88 의거 상태 캐시 코어를 defense_state_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 초과 Case 66, 67, 86, 87, 92, 94, 95, 96 — 수동(MANUAL) 및 1주 휩소(WHIPSAW_1SH) 1·2차 과열 패스,
#           포지션 자체 손실률 즉시 손절 금지 및 토스 QQQ 0.0초 실시간 최소 0.80% 역행 시에만 3차 손절 덤핑,
#           04:07~08:14 EST NQ<2.0% 내 3차 손절 시 최저점 역트레일링(RE_BOT_TRAIL) 자동 점화
# MODIFIED: 초과 Case 84 & Case 98 — 08:15 EST 선제 컷오프 및 15:00~15:58 EST 미익절 보유 물량 정규장 VWAP 도달 즉시 탈출
# NEW: 초과 Case 101 — 옵션 A (🚀 08_HYB_PIVBRK) 10분봉 대파동 돌파 꺾임목(PRE_PIVOT_10M / PRE_PIVOT_10M_REARM) 방어망 통합 및 리익스포트 결속

import math
import time
import asyncio
from datetime import datetime

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
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
    _AUTO_ENTRY_SESSION_TAGS,
    _STRIKE_ALERT_CACHE,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _SWING_10M_CACHE,
    _MANUAL_STAGE3_ANCHOR_CACHE,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    is_manual_stage3_anchored,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    _activate_bottom_track_state,
    get_bottom_track_status,
    _reset_pre_leader_state,
    mark_pre_pullback_dispatched,
    get_pre_leader_status,
    update_premarket_trend_leader,
    _reset_swing_10m_state,
    mark_pre_pivot_10m_dispatched,
    get_swing_10m_status,
    update_premarket_swing_10m_wave,
    evaluate_premarket_10m_pivot_breakout,
    _cancel_open_orders_and_wait_sellable,
)

_LAST_DEFENSE_DUMP_TS = {"SOXS": 0.0, "SOXL": 0.0}


# 초과 Case 48, 53, 84, 101 — 08:15:00 EST 거시지표 발표 15분 전 선제 컷오프 및 미체결 매수 주문 즉시 파기
async def enforce_0930_regular_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    buy_order_id: str,
) -> tuple[bool, str]:
    est_hm = now_est.hour * 100 + now_est.minute
    if symbol not in ("SOXS", "SOXL") or not (815 <= est_hm <= 1558):
        return is_session_done, buy_order_id

    if _BOTTOM_TRACK_CACHE.get("active", False):
        _reset_bottom_track_state()
    if _PRE_LEADER_CACHE.get("leader_sym"):
        _reset_pre_leader_state()
    if _SWING_10M_CACHE.get("date"):
        _reset_swing_10m_state()

    if not is_active:
        return is_session_done, buy_order_id

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # 1) 08:15 EST 이후 미체결 매수 주문(buy_order_id) 잔존 시 즉시 선취소 및 장부 소각
    if buy_order_id and not ctx.in_memory_ordering_lock.get(symbol, False):
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            canceled_ok = False
            try:
                await ctx.client.cancel_order(buy_order_id)
                canceled_ok = True
            except Exception as ce:
                err_l = str(ce).lower()
                if "already-filled" in err_l:
                    await AssassinLedger.save_state(symbol, buy_order_id="", is_session_done=True)
                    return True, ""
                if "already-canceled" in err_l or "404" in err_l or "order-not-found" in err_l:
                    canceled_ok = True

            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                for o in open_orders:
                    if str(o.get("side") or "").upper() == "BUY" and o.get("orderId"):
                        try:
                            await ctx.client.cancel_order(str(o["orderId"]))
                            canceled_ok = True
                        except Exception:
                            pass
            except Exception:
                pass

            st_cur = await AssassinLedger.get_state(symbol)
            cur_es = str(st_cur[8] or "")
            new_es = cur_es if (holdings_qty > 0 and cur_es) else ""
            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                is_session_done=True,
                entry_session=new_es,
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            if canceled_ok:
                cutoff_tag = "08:15 거시지표 선제 컷오프" if est_hm < 930 else "정규장 매수 컷오프"
                await ctx.notify_tg(
                    f"🔒 <b>[{cutoff_tag} {sym_icon}{symbol}] 미체결 매수 주문 즉시 파기 완료</b>\n"
                    f"▫️ 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code>\n"
                    "▫️ 조치: 08:30 EST 거시지표 발표 및 정규장 변동성 방어를 위해 미체결 매수 덫을 취소하고 금일 신규 진입을 마감합니다."
                )
            return True, ""
        except Exception as e:
            print(f"🚨 [08:15 선제 컷오프 매수취소 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # 2) 미보유·무주문 상태에서 아직 is_session_done이 False이면 즉시 True로 잠금
    if not is_session_done:
        st_cur = await AssassinLedger.get_state(symbol)
        cur_es = str(st_cur[8] or "")
        new_es = "" if cur_es == "STAGE3_STOP_TRACK" else cur_es
        await AssassinLedger.save_state(symbol, is_session_done=True, entry_session=new_es)
        return True, buy_order_id

    return is_session_done, buy_order_id


async def enforce_0815_pre_macro_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    buy_order_id: str,
) -> tuple[bool, str]:
    return await enforce_0930_regular_cutoff_plugin(
        ctx=ctx,
        symbol=symbol,
        now_est=now_est,
        is_active=is_active,
        is_session_done=is_session_done,
        holdings_qty=holdings_qty,
        buy_order_id=buy_order_id,
    )


# 초과 Case 66, 67, 82, 86, 87, 92, 94, 95, 96, 101 — 방향성 인지(Direction-Aware) 매크로 3단 방어망
async def execute_macro_defense_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    avg_price: float,
    current_price: float,
    buy_order_id: str,
    sell_order_id: str,
    target_sell_price: float,
    entry_session: str,
    nq_entry_price: float,
    nq_entry_amp: float,
) -> tuple[bool, str, str, bool]:
    """
    반환값: (is_session_done, buy_order_id, entry_session, defense_triggered)
    """
    if symbol not in ("SOXS", "SOXL") or not is_active:
        return is_session_done, buy_order_id, entry_session, False

    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"

    # 초과 Case 95 & Case 96: 1주 휩소 노출 작전(WHIPSAW_1SH) 미체결 매수 주문은 시간대와 무관하게
    # 1·2차 과열 방어선에서 100% 패스시켜 enforce_0407_whipsaw_cutoff_plugin이 전담 정리하도록 격리!
    if entry_session == "WHIPSAW_1SH" and holdings_qty <= 0:
        return is_session_done, buy_order_id, entry_session, False

    nq_c, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
    nq_amp = (((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0)
    is_stage12_overheated = bool(nq_amp >= 2.0)

    # =================================================================
    # [1차 방어선] 미보유 상태(holdings_qty == 0)에서 NQ=F 진폭 >= 2.0% 과열 감지 시
    #             본진 미체결 매수 주문 즉시 취소 및 당일 본진 자동 진입 권한 영구 소각
    # =================================================================
    if holdings_qty <= 0:
        if _MANUAL_STAGE3_ANCHOR_CACHE.get(symbol, {}).get("anchored", False):
            _reset_manual_stage3_anchor(symbol)

        if is_stage12_overheated and (400 <= est_hm <= 1558):
            if _BOTTOM_TRACK_CACHE.get("active", False):
                _reset_bottom_track_state()
            if _PRE_LEADER_CACHE.get("leader_sym"):
                _reset_pre_leader_state()
            if _SWING_10M_CACHE.get("date"):
                _reset_swing_10m_state()

            if buy_order_id and entry_session != "WHIPSAW_1SH":
                if not ctx.in_memory_ordering_lock.get(symbol, False):
                    ctx.in_memory_ordering_lock[symbol] = True
                    try:
                        canceled_new = False
                        try:
                            await ctx.client.cancel_order(buy_order_id)
                            canceled_new = True
                        except Exception as ce:
                            err_l = str(ce).lower()
                            if "already-filled" in err_l:
                                return is_session_done, buy_order_id, entry_session, False
                            if "already-canceled" in err_l or "404" in err_l or "order-not-found" in err_l:
                                canceled_new = False

                        await AssassinLedger.save_state(
                            symbol,
                            buy_order_id="",
                            is_session_done=True,
                            entry_session="CANCELED_MACRO",
                            whipsaw_target_price=0.0,
                        )
                        await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                        ctx.wakeup_event.set()

                        if (
                            canceled_new
                            and _STRIKE_ALERT_CACHE.get("last_macro1_cancel_id") != buy_order_id
                        ):
                            _STRIKE_ALERT_CACHE["last_macro1_cancel_id"] = buy_order_id
                            _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                            await ctx.notify_tg(
                                f"🛑 <b>[1차 매크로 과열 방어 {sym_icon}{symbol}] 미체결 본진 매수 덫 즉시 취소 완료</b>\n"
                                f"▫️ 사유: 나스닥 100 선물(NQ=F) 당일 진폭 <code>{nq_amp:.2f}%</code> (2.0% 이상 과열 돌파)\n"
                                "▫️ 조치: 미체결 매수 주문 취소 및 금일 본진 자동 매수 권한 영구 소각(CANCELED_MACRO)"
                            )
                        return True, "", "CANCELED_MACRO", True
                    finally:
                        ctx.in_memory_ordering_lock[symbol] = False

            elif entry_session != "CANCELED_MACRO" or not is_session_done:
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                if _STRIKE_ALERT_CACHE.get("last_macro1_date") != today_str:
                    _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                    await ctx.notify_tg(
                        "🛑 <b>[1차 매크로 과열 방어선 가동] 나스닥 100 선물 대세장 과열 감지</b>\n"
                        f"▫️ 당일 NQ=F 진폭: <code>{nq_amp:.2f}%</code> (기준 2.00% 이상)\n"
                        "▫️ 조치: 금일 프리장 본진(SOXS/SOXL) 자동 신규 진입을 차단하고 관망 모드로 전환합니다."
                    )
                return True, "", "CANCELED_MACRO", True

        return is_session_done, buy_order_id, entry_session, False

    # =================================================================
    # [보유 중 (holdings_qty > 0)] 수동(MANUAL) vs 자동 진입 판별 및
    # 실시간 QQQ 3차 역행 기준가(nq_entry_price) 동기화·자가 치유 (초과 Case 86, 92, 94)
    # =================================================================
    is_auto_pos = bool(entry_session in _AUTO_ENTRY_SESSION_TAGS)
    is_whipsaw_pos = bool(entry_session == "WHIPSAW_1SH")
    is_manual_pos = bool(not is_auto_pos and not buy_order_id)

    ref_curr, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
    if ref_curr <= 0.0:
        ref_curr = nq_c

    # 초과 Case 86 & Case 94:
    # 1) 수동 보유 물량이 처음 감지되었거나 스나이퍼 OFF -> ON 재가동 직후(미결속 상태)이거나
    # 2) 구버전 NQ=F 지수 스케일(>3000)과 QQQ 스케일(<3000) 불일치가 감지된 경우
    # 즉시 현재 실시간 QQQ 가격(ref_curr)으로 3차 방어 기준가(nq_entry_price)를 0.00% 재락온!
    scale_mismatch = bool(
        nq_entry_price > 0.0
        and ref_curr > 0.0
        and ((nq_entry_price > 3000.0) != (ref_curr > 3000.0))
    )
    unanchored_manual = bool(
        is_manual_pos
        and (
            not is_manual_stage3_anchored(symbol)
            or nq_entry_price <= 0.0
            or (not sell_order_id and target_sell_price <= 0.0)
        )
    )

    if unanchored_manual or scale_mismatch or nq_entry_price <= 0.0:
        if ref_curr > 0.0:
            nq_entry_price = ref_curr
            nq_entry_amp = nq_amp if nq_amp > 0.0 else (nq_entry_amp or 0.8)
            new_es_tag = "MANUAL" if is_manual_pos else entry_session
            trend_tag = "SHORT" if symbol == "SOXS" else "LONG"
            await AssassinLedger.save_state(
                symbol,
                entry_session=new_es_tag,
                entry_trend=trend_tag,
                nq_entry_price=nq_entry_price,
                nq_entry_amp=nq_entry_amp,
            )
            entry_session = new_es_tag
            if is_manual_pos:
                mark_manual_stage3_anchored(symbol, ref_price=nq_entry_price, ref_amp=nq_entry_amp)
            if scale_mismatch or unanchored_manual:
                return is_session_done, buy_order_id, entry_session, False

    live_p = float(current_price or 0.0)
    if live_p <= 0.0:
        live_p = await ctx.client.get_current_price(symbol)
    if live_p <= 0.0 or avg_price <= 0.0:
        return is_session_done, buy_order_id, entry_session, False

    # 트레일링 점화 상태이거나 수익/보합 구간(live_p >= avg_price)이면 2·3차 손절 대상이 아님!
    trail_st = _TRAIL_RUNTIME_CACHE.get(symbol) or {}
    if bool(trail_st.get("armed", False)) or live_p >= avg_price:
        return is_session_done, buy_order_id, entry_session, False

    # =================================================================
    # [손실 구간 (0 < live_p < avg_price)] 2차 과열 방어 및 3차 실시간 QQQ 역행 방어 판정
    # =================================================================
    # 초과 Case 86 & Case 95: 수동(MANUAL) 보유 물량 및 1주 휩소(WHIPSAW_1SH) 보유 물량은 2차 과열 방어선 100% 패스!
    effective_stage2_breach = bool(
        is_stage12_overheated
        and is_auto_pos
        and not is_whipsaw_pos
        and (nq_entry_amp < 2.0)
    )

    # 초과 Case 67, 92, 94:
    # 포지션 자체 손실률 기반 즉시 손절 절대 금지!
    # 오직 NQ=F 진입 진폭 기반 dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%) 이상
    # 실시간 QQQ(ref_curr)가 진입 기준가(nq_entry_price) 대비 반대 방향으로 역행했을 때만 3차 손절 격발!
    base_amp_for_th = float(nq_entry_amp if nq_entry_amp > 0.0 else nq_amp)
    dynamic_threshold = max(
        base_amp_for_th * 0.5,
        float(getattr(ctx, "nq_dynamic_floor", MIN_STAGE3_QQQ_ADVERSE_PCT) or MIN_STAGE3_QQQ_ADVERSE_PCT),
        MIN_STAGE3_QQQ_ADVERSE_PCT,
    )

    qqq_adverse_pct = 0.0
    if ref_curr > 0.0 and nq_entry_price > 0.0:
        if symbol == "SOXS":
            # 숏(SOXS) 보유 시 나스닥(QQQ)이 상승하면 역행
            qqq_adverse_pct = (ref_curr - nq_entry_price) / nq_entry_price * 100.0
        else:
            # 롱(SOXL) 보유 시 나스닥(QQQ)이 하락하면 역행
            qqq_adverse_pct = (nq_entry_price - ref_curr) / nq_entry_price * 100.0

    effective_stage3_breach = bool(qqq_adverse_pct >= dynamic_threshold)

    if not (effective_stage2_breach or effective_stage3_breach):
        return is_session_done, buy_order_id, entry_session, False

    now_ts = time.time()
    if (
        ctx.in_memory_ordering_lock.get(symbol, False)
        or (now_ts - float(_LAST_DEFENSE_DUMP_TS.get(symbol, 0.0))) < 1.5
    ):
        return is_session_done, buy_order_id, entry_session, False

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        exec_sell_price = math.floor(latest_p * 100.0) / 100.0

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0].get("price") or 0.0) > 0.0:
                bid_1 = float(bids[0]["price"])
                exec_sell_price = min(exec_sell_price, bid_1) if exec_sell_price > 0.0 else bid_1
        except Exception:
            pass

        if exec_sell_price <= 0.0:
            return is_session_done, buy_order_id, entry_session, False

        dump_dispatched = False
        new_sell_id = sell_order_id

        if sellable_q_int > 0:
            dump_qty = min(int(holdings_qty), sellable_q_int)
            idem_key = f"MDMP_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            try:
                res_sell = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=dump_qty,
                        price=f"{exec_sell_price:.2f}",
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                if res_sell and isinstance(res_sell, dict) and res_sell.get("result", {}).get("orderId"):
                    new_sell_id = str(res_sell["result"]["orderId"])
                    dump_dispatched = True
            except Exception as ce:
                print(f"🚨 [매크로 덤핑 신규 매도 실패 {symbol}] {ce}", flush=True)
                ctx.idempotency_keys[symbol]["SELL"] = None

        if not dump_dispatched and surviving_sell_ids:
            target_mod_id = surviving_sell_ids[0]
            try:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=target_mod_id,
                        price=f"{exec_sell_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=4.0,
                )
                new_sell_id = str((mod_res or {}).get("result", {}).get("orderId") or target_mod_id)
                dump_dispatched = True
            except Exception as me:
                print(f"🚨 [매크로 덤핑 정정 스윕 실패 {symbol}] {me}", flush=True)

        if not dump_dispatched:
            return is_session_done, buy_order_id, entry_session, False

        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)

        # 초과 Case 87: 04:07 ~ 08:14 EST 골든타임 내 NQ<2.0% 상태에서 순수 3차 역행 손절 시
        # 당일 퇴근하지 않고 손절가 하회 최저점 역트레일링 +0.30% 반등 재진입(RE_BOT_TRAIL) 점화!
        can_rearm_bottom = bool(
            effective_stage3_breach
            and not effective_stage2_breach
            and not is_stage12_overheated
            and not is_whipsaw_pos
            and (407 <= est_hm <= 814)
        )

        ref_unit = "$" if ref_src == "QQQ실시간" else ""
        est_loss_pct = ((exec_sell_price - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0

        if can_rearm_bottom:
            _activate_bottom_track_state(symbol=symbol, stop_price=exec_sell_price, now_est=now_est)
            next_es = "STAGE3_STOP_TRACK"
            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                sell_order_id=new_sell_id,
                target_sell_price=exec_sell_price,
                is_session_done=False,
                entry_session=next_es,
                whipsaw_target_price=0.0,
            )
            await AssassinLedger.save_state(opp_sym, is_session_done=False)
            trig_reb_p = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🛡️ <b>[3차 역행 안전망 칼손절 ➡️ 최저점 역트레일링 점화] {sym_icon}{symbol}</b>\n"
                f"▫️ 사유: {ref_src} 기준가(<code>{ref_unit}{nq_entry_price:.2f}</code>) 대비 현재(<code>{ref_unit}{ref_curr:.2f}</code>) "
                f"역행 <code>+{qqq_adverse_pct:.2f}%</code> (3차 한도 <code>+{dynamic_threshold:.2f}%</code> 도달)\n"
                f"▫️ 스윕 단가(Bid 1 LIMIT): <code>${exec_sell_price:.2f}</code> (평단 <code>${avg_price:.2f}</code> 대비 <code>{est_loss_pct:+.2f}%</code> · <code>{holdings_qty}주</code>)\n"
                f"▫️ 🪤 재출격 준비: 손절가(<code>${exec_sell_price:.2f}</code>) 하회 최저점 확인 후 <code>+{BOTTOM_REBOUND_PCT:.2f}%</code> 양봉 반등(초기 목표 <code>${trig_reb_p:.2f}</code>) 시 즉시 재진입합니다!"
            )
            return False, "", next_es, True
        else:
            next_es = "CANCELED_MACRO"
            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                sell_order_id=new_sell_id,
                target_sell_price=exec_sell_price,
                is_session_done=True,
                entry_session=next_es,
                whipsaw_target_price=0.0,
            )
            await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
            _reset_bottom_track_state()
            ctx.wakeup_event.set()

            if effective_stage2_breach:
                stage_title = f"🛑 [2차 대세장 과열 안전망 덤핑] {sym_icon}{symbol} 전량 손절 스윕"
                stage_reason = f"손실 구간 중 NQ=F 당일 진폭 <code>{nq_amp:.2f}%</code> (2.00% 과열 돌파)"
            else:
                pos_lbl = "수동보유" if is_manual_pos else "자동보유"
                stage_title = f"🛑 [3차 실시간 {ref_src} 역행 안전망 덤핑] {sym_icon}{symbol} ({pos_lbl})"
                stage_reason = (
                    f"{ref_src} 기준가(<code>{ref_unit}{nq_entry_price:.2f}</code>) 대비 현재(<code>{ref_unit}{ref_curr:.2f}</code>) "
                    f"역행 <code>+{qqq_adverse_pct:.2f}%</code> (3차 한도 <code>+{dynamic_threshold:.2f}%</code> 돌파)"
                )

            await ctx.notify_tg(
                f"<b>{stage_title}</b>\n"
                f"▫️ 사유: {stage_reason}\n"
                f"▫️ 스윕 단가(Bid 1 LIMIT): <code>${exec_sell_price:.2f}</code> (평단 <code>${avg_price:.2f}</code> 대비 <code>{est_loss_pct:+.2f}%</code> · <code>{holdings_qty}주</code>)\n"
                "▫️ 후속 조치: 체결 즉시 실수익 정산 및 금일 자동 진입 마감(CANCELED_MACRO)"
            )
            return True, "", next_es, True
    finally:
        _LAST_DEFENSE_DUMP_TS[symbol] = time.time()
        ctx.in_memory_ordering_lock[symbol] = False


# 초과 Case 98 — 정규장 장마감 1시간 전(15:00:00 ~ 15:58:59 EST) 미익절 보유 물량 당일 정규장 VWAP 도달 즉시 탈출
async def execute_late_regular_vwap_escape_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int,
    avg_price: float,
    current_price: float,
    sell_order_id: str,
    target_sell_price: float,
    vwap_val: float = 0.0,
) -> bool:
    if symbol not in ("SOXS", "SOXL") or not is_active or holdings_qty <= 0:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (1500 <= est_hm <= 1558):
        return False

    trail_st = _TRAIL_RUNTIME_CACHE.get(symbol) or {}
    if bool(trail_st.get("armed", False)):
        return False

    arm_p = (
        target_sell_price
        if target_sell_price > 0.0
        else (math.ceil(avg_price * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0 if avg_price > 0.0 else 0.0)
    )
    live_p = float(current_price or 0.0)
    if live_p <= 0.0:
        live_p = await ctx.client.get_current_price(symbol)
    if live_p <= 0.0:
        return False

    # 이미 +1.00% 트레일링 점화가 이상에 도달한 수익 포지션은 하이킨 아시 트레일링이 전담하도록 보호
    if arm_p > 0.0 and live_p >= arm_p:
        return False

    sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol) or {}
    ewm_st = sym_rsi_dict.get("ewm_state") or {}
    reg_vwap = float(ewm_st.get("reg_vwap", 0.0)) or float(vwap_val or 0.0) or float(sym_rsi_dict.get("vwap", 0.0))
    if reg_vwap <= 0.0:
        return False

    vwap_floor_p = math.floor(reg_vwap * 100.0) / 100.0
    if vwap_floor_p <= 0.0 or live_p < vwap_floor_p:
        return False

    now_ts = time.time()
    if (
        ctx.in_memory_ordering_lock.get(symbol, False)
        or (now_ts - float(_LAST_DEFENSE_DUMP_TS.get(symbol, 0.0))) < 1.5
    ):
        return False

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    ctx.in_memory_ordering_lock[symbol] = True
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = live_p
        exec_sell_price = math.floor(latest_p * 100.0) / 100.0

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0].get("price") or 0.0) > 0.0:
                bid_1 = float(bids[0]["price"])
                exec_sell_price = min(exec_sell_price, bid_1) if exec_sell_price > 0.0 else bid_1
        except Exception:
            pass

        if exec_sell_price <= 0.0:
            return False

        esc_dispatched = False
        new_sell_id = sell_order_id

        if sellable_q_int > 0:
            esc_qty = min(int(holdings_qty), sellable_q_int)
            idem_key = f"VESC_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            try:
                res_sell = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=esc_qty,
                        price=f"{exec_sell_price:.2f}",
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                if res_sell and isinstance(res_sell, dict) and res_sell.get("result", {}).get("orderId"):
                    new_sell_id = str(res_sell["result"]["orderId"])
                    esc_dispatched = True
            except Exception as ce:
                print(f"🚨 [15:00 VWAP 탈출 신규 매도 실패 {symbol}] {ce}", flush=True)

        if not esc_dispatched and surviving_sell_ids:
            target_mod_id = surviving_sell_ids[0]
            try:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=target_mod_id,
                        price=f"{exec_sell_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=4.0,
                )
                new_sell_id = str((mod_res or {}).get("result", {}).get("orderId") or target_mod_id)
                esc_dispatched = True
            except Exception as me:
                print(f"🚨 [15:00 VWAP 탈출 정정 스윕 실패 {symbol}] {me}", flush=True)

        if not esc_dispatched:
            return False

        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)
        await AssassinLedger.save_state(
            symbol,
            buy_order_id="",
            sell_order_id=new_sell_id,
            target_sell_price=exec_sell_price,
            is_session_done=True,
            entry_session="VWAP_ESCAPE",
            whipsaw_target_price=0.0,
        )
        ctx.wakeup_event.set()
        est_pnl_pct = ((exec_sell_price - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0
        await ctx.notify_tg(
            f"🛟 <b>[장마감 1시간 전 미익절 VWAP 탈출] {sym_icon}{symbol} 정규장 VWAP 도달 전량 스윕!</b>\n"
            f"▫️ 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> (15:00 ~ 15:58 EST 보호 구간)\n"
            f"▫️ 탈출 조건: 현재가 <code>${live_p:.2f}</code> ≥ 당일 정규장 누적 VWAP <code>${vwap_floor_p:.2f}</code> 도달\n"
            f"▫️ 처분 단가(LIMIT): <code>${exec_sell_price:.2f}</code> (평단 <code>${avg_price:.2f}</code> 대비 <code>{est_pnl_pct:+.2f}%</code> · <code>{holdings_qty}주</code>)\n"
            "▫️ 조치: 15:59 EST MOC 장마감 투매 리스크를 회피하고 실수익 정산 후 금일 마감합니다."
        )
        return True
    finally:
        _LAST_DEFENSE_DUMP_TS[symbol] = time.time()
        ctx.in_memory_ordering_lock[symbol] = False


# Case 16 & Case 24 — 15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산망
async def execute_moc_zero_overnight_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int,
    sell_order_id: str,
    current_price: float,
) -> bool:
    if symbol not in ("SOXS", "SOXL") or not is_active or holdings_qty <= 0:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (1559 <= est_hm <= 1601):
        return False

    now_ts = time.time()
    if (
        ctx.in_memory_ordering_lock.get(symbol, False)
        or (now_ts - float(_LAST_DEFENSE_DUMP_TS.get(symbol, 0.0))) < 1.5
    ):
        return False

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    ctx.in_memory_ordering_lock[symbol] = True
    try:
        sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
            ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
        )

        latest_p = await ctx.client.get_current_price(symbol)
        if latest_p <= 0.0:
            latest_p = current_price
        exec_sell_price = math.floor(latest_p * 100.0) / 100.0

        try:
            ob = await ctx.client.get_orderbook(symbol)
            bids = ob.get("bids", [])
            if bids and float(bids[0].get("price") or 0.0) > 0.0:
                bid_1 = float(bids[0]["price"])
                exec_sell_price = min(exec_sell_price, bid_1) if exec_sell_price > 0.0 else bid_1
        except Exception:
            pass

        if exec_sell_price <= 0.0:
            return False

        moc_dispatched = False
        new_sell_id = sell_order_id

        if sellable_q_int > 0:
            moc_qty = min(int(holdings_qty), sellable_q_int)
            idem_key = f"MOC_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
            try:
                res_sell = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=moc_qty,
                        price=f"{exec_sell_price:.2f}",
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                if res_sell and isinstance(res_sell, dict) and res_sell.get("result", {}).get("orderId"):
                    new_sell_id = str(res_sell["result"]["orderId"])
                    moc_dispatched = True
            except Exception as ce:
                print(f"🚨 [15:59 MOC 신규 매도 실패 {symbol}] {ce}", flush=True)
                ctx.idempotency_keys[symbol]["SELL"] = None

        if not moc_dispatched and surviving_sell_ids:
            target_mod_id = surviving_sell_ids[0]
            try:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=target_mod_id,
                        price=f"{exec_sell_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=4.0,
                )
                new_sell_id = str((mod_res or {}).get("result", {}).get("orderId") or target_mod_id)
                moc_dispatched = True
            except Exception as me:
                print(f"🚨 [15:59 MOC 정정 스윕 실패 {symbol}] {me}", flush=True)

        if moc_dispatched:
            _reset_trailing_runtime_state(symbol)
            _reset_manual_stage3_anchor(symbol)
            await AssassinLedger.save_state(
                symbol,
                buy_order_id="",
                sell_order_id=new_sell_id,
                target_sell_price=exec_sell_price,
                is_session_done=True,
                whipsaw_target_price=0.0,
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🛑 <b>[15:59 제로-오버나이트 MOC 강제 청산] {sym_icon}{symbol} 전량 스윕 발사</b>\n"
                f"▫️ 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> | 수량: <code>{holdings_qty}주</code>\n"
                f"▫️ 처분 단가(Bid 1 LIMIT): <code>${exec_sell_price:.2f}</code>\n"
                "▫️ 조치: 오버나이트 리스크 0% 원칙에 의거 전량 당일 청산 후 복리 정산을 수행합니다."
            )
            return True
    finally:
        _LAST_DEFENSE_DUMP_TS[symbol] = time.time()
        ctx.in_memory_ordering_lock[symbol] = False

    return False
