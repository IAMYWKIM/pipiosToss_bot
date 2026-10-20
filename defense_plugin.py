# =====================================================================
# FILE: defense_plugin.py
# 목적: 방향성 인지(Direction-Aware) 매크로 3단 방어망(execute_macro_defense_plugin),
#       08:15 EST 거시지표 발표 15분 전 선제 컷오프(enforce_0930_regular_cutoff_plugin),
#       15:00 ~ 15:58 EST 장마감 1시간 전 미익절 물량 정규장 VWAP 도달 즉시 탈출(execute_late_regular_vwap_escape_plugin),
#       15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산(execute_moc_zero_overnight_plugin) 및
#       미체결 주문 선취소·매도가능수량 해방 폴링 헬퍼(_cancel_open_orders_and_wait_sellable) 독립 플러그인
# =====================================================================
# MODIFIED: 제1 ~ 제6헌법 및 초과 Case 48, 63, 66, 67, 82, 84, 86, 87, 88, 92, 94, 95, 96, 98 완벽 준수
# NEW: 1) 모든 방어 플러그인 함수에 `entry_trend: str = ""` 및 `**kwargs` 명시 수용으로
#         `execute_macro_defense_plugin() got an unexpected keyword argument 'entry_trend'` 원천 소각
#      2) 1주 휩소 작전(`WHIPSAW_1SH`) 및 수동 진입(`MANUAL`) 포지션의 1·2차 과열 방어선 100% 바이패스 격리
#      3) 포지션 자체 손실률 즉시 손절 절대 금지 & 토스 Open API QQQ 0.0초 실시간 최소 0.80% 역행 시에만
#         3차 손절 스윕 및 04:07 ~ 08:14 EST 골든타임 내 최저점 역트레일링(`STAGE3_STOP_TRACK`) 점화

import math
import time
import uuid
import html
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    SINGLE_TARGET_PROFIT_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 초과 Case 94: 수동 보유 포지션 스나이퍼 ON 결속 시 실시간 QQQ 기준가 인메모리 앵커 캐시
_MANUAL_STAGE3_ANCHOR_CACHE: dict[str, dict] = {
    "SOXS": {"ref_price": 0.0, "ref_amp": 0.0, "locked_ts": 0.0, "qty": 0},
    "SOXL": {"ref_price": 0.0, "ref_amp": 0.0, "locked_ts": 0.0, "qty": 0},
}

# 초과 Case 96: 1차 매크로 과열 미체결 취소 중복 알림 방지 캐시
_LAST_MACRO1_CANCEL_ID: dict[str, str] = {"SOXS": "", "SOXL": ""}
_LAST_CUTOFF_0815_DATE: dict[str, str] = {"SOXS": "", "SOXL": ""}


def _reset_manual_stage3_anchor(symbol: str = None):
    """스나이퍼 OFF 또는 포지션 전량 청산 시 수동 3차 기준가 인메모리 캐시를 초기화합니다."""
    if symbol in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
            "ref_price": 0.0,
            "ref_amp": 0.0,
            "locked_ts": 0.0,
            "qty": 0,
        }
    else:
        for sym in ("SOXS", "SOXL"):
            _MANUAL_STAGE3_ANCHOR_CACHE[sym] = {
                "ref_price": 0.0,
                "ref_amp": 0.0,
                "locked_ts": 0.0,
                "qty": 0,
            }


async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    holdings_qty: int,
    sell_order_id: str = "",
    buy_order_id: str = "",
    max_polls: int = 8,
    poll_interval: float = 0.25,
    **kwargs,
) -> tuple[int, list[str]]:
    """
    Case 24: 미체결 매도(및 매수) 주문을 선취소하고 최대 2.0초(0.25초 * 8회) 동안
    매도 가능 수량(sellableQuantity) 해방을 폴링 대기합니다.
    취소 지연으로 인해 여전히 살아있는 매도 주문 ID 목록(surviving_sell_ids)을 함께 반환하여
    즉시 modify_order 가격 하향 정정 스윕 폴백을 지원합니다.
    """
    client = ctx.client
    target_cancel_ids = set()
    if sell_order_id:
        target_cancel_ids.add(str(sell_order_id).strip())
    if buy_order_id:
        target_cancel_ids.add(str(buy_order_id).strip())

    try:
        open_orders = await client.get_orders(status="OPEN", symbol=symbol)
        if isinstance(open_orders, list):
            for od in open_orders:
                if isinstance(od, dict) and od.get("orderId"):
                    target_cancel_ids.add(str(od["orderId"]).strip())
    except Exception:
        pass

    for oid in list(target_cancel_ids):
        if not oid:
            continue
        try:
            await client.cancel_order(oid)
        except Exception:
            pass

    sellable_q_int = 0
    for _ in range(max(1, int(max_polls))):
        try:
            sq = await client.get_sellable_quantity(symbol)
            sellable_q_int = int(math.floor(float(sq or 0.0)))
            if sellable_q_int >= int(holdings_qty) and sellable_q_int > 0:
                return sellable_q_int, []
        except Exception:
            pass
        await asyncio.sleep(poll_interval)

    surviving_sell_ids = []
    try:
        open_after = await client.get_orders(status="OPEN", symbol=symbol)
        if isinstance(open_after, list):
            for od in open_after:
                if (
                    isinstance(od, dict)
                    and str(od.get("side") or "").upper() == "SELL"
                    and od.get("orderId")
                ):
                    surviving_sell_ids.append(str(od["orderId"]).strip())
    except Exception:
        pass

    if not surviving_sell_ids and sell_order_id:
        surviving_sell_ids.append(str(sell_order_id).strip())

    return sellable_q_int, surviving_sell_ids


async def _execute_two_stage_emergency_sell(
    ctx,
    symbol: str,
    holdings_qty: int,
    sell_order_id: str,
    buy_order_id: str,
    current_price: float,
    tag_prefix: str = "mdump",
) -> tuple[bool, float, str]:
    """
    Case 24: 1단계 기존 매도 주문(sell_order_id) 가격 즉시 정정(modify_order) 스윕 시도 후,
    미장전 또는 정정 불가 시 2단계 선취소 + 매도가능수량 폴링 대기 + 매수 1호가 LIMIT 신규 매도를 발사합니다.
    """
    client = ctx.client
    best_bid = await client.get_best_bid_price(symbol, fallback_price=current_price)
    sweep_price = best_bid if best_bid > 0.0 else (math.floor(float(current_price or 0.0) * 100.0) / 100.0)
    if sweep_price <= 0.0:
        return False, 0.0, ""

    # 1단계: 이미 거래소에 접수된 매도 주문이 있으면 0.1초 만에 가격만 매수 1호가로 하향 정정 스윕
    if sell_order_id:
        try:
            mod_res = await client.modify_order(
                order_id=sell_order_id,
                price=sweep_price,
                order_type="LIMIT",
            )
            new_oid = str((mod_res or {}).get("orderId") or sell_order_id)
            return True, sweep_price, new_oid
        except Exception as mod_e:
            err_s = str(mod_e).lower()
            if "already-filled" in err_s:
                return True, sweep_price, sell_order_id

    # 2단계: 미체결 주문 선취소 및 매도 가능 수량 해방 대기 후 신규 LIMIT 매도 발사
    sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
        ctx=ctx,
        symbol=symbol,
        holdings_qty=holdings_qty,
        sell_order_id=sell_order_id,
        buy_order_id=buy_order_id,
    )

    if sellable_q_int <= 0 and surviving_sell_ids:
        for sv_oid in surviving_sell_ids:
            try:
                mod_res = await client.modify_order(
                    order_id=sv_oid,
                    price=sweep_price,
                    order_type="LIMIT",
                )
                new_oid = str((mod_res or {}).get("orderId") or sv_oid)
                return True, sweep_price, new_oid
            except Exception:
                pass
        sellable_q_int = int(holdings_qty)

    target_sell_qty = sellable_q_int if sellable_q_int > 0 else int(holdings_qty)
    if target_sell_qty <= 0:
        return False, sweep_price, ""

    cid = f"{tag_prefix}-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
    if isinstance(ctx.idempotency_keys, dict) and symbol in ctx.idempotency_keys:
        ctx.idempotency_keys[symbol]["M_DUMP"] = cid

    try:
        ord_res = await client.create_order(
            symbol=symbol,
            side="SELL",
            quantity=target_sell_qty,
            price=sweep_price,
            order_type="LIMIT",
            time_in_force="DAY",
            client_order_id=cid,
        )
        new_oid = str((ord_res or {}).get("orderId") or "")
        return True, sweep_price, new_oid
    except Exception as sell_e:
        if isinstance(ctx.idempotency_keys, dict) and symbol in ctx.idempotency_keys:
            ctx.idempotency_keys[symbol]["M_DUMP"] = None
        print(f"🚨 [{tag_prefix.upper()} 긴급 매도 방어 {symbol}] {sell_e}", flush=True)
        return False, sweep_price, ""


async def execute_macro_defense_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str = "",
    is_active: bool = True,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    current_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    entry_trend: str = "",
    nq_entry_price: float = 0.0,
    nq_entry_amp: float = 0.0,
    **kwargs,
) -> bool:
    """
    초과 Case 66, 67, 82, 86, 87, 92, 94, 95, 96:
    방향성 인지 매크로 3단 방어망
    - 1차 방어선: NQ=F 당일 진폭 >= 2.0% 시 본진 자동 진입 차단 및 본진 미체결 매수 주문 즉시 취소·장부 소각
                 (단, 1주 휩소 `WHIPSAW_1SH` 및 수동 보유 `MANUAL` 은 100% 예외 보호)
    - 2차 방어선: NQ=F 당일 진폭 >= 2.0% 시 본진 자동 진입 손실권 보유 물량 즉시 매수 1호가 덤핑
                 (단, 수동 진입 `MANUAL` 및 1주 휩소 `WHIPSAW_1SH` 는 2차 방어선 100% 패스!)
    - 3차 방어선: 포지션 자체 손실률 즉시 손절 절대 금지!
                 손실 구간(0 < current_price < avg_price) 전제 하에 진입/수동 락온 시점의
                 토스 실시간 QQQ 기준가(nq_entry_price) 대비 현재 실시간 QQQ 가격(ref_curr)이
                 `dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%)` (최소 0.80%) 이상 역행했을 때만 손절 스윕!
    """
    if not is_active:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    _, nq_high, nq_low = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if (nq_high > 0.0 and nq_low > 0.0) else 0.0
    is_stage12_overheated = bool(nq_amp >= 2.0)

    es_clean = str(entry_session or "").strip()

    # =================================================================
    # [A] 미보유 상태(holdings_qty == 0): 1차 매크로 과열 방어선 점검
    # =================================================================
    if int(holdings_qty) <= 0:
        _reset_manual_stage3_anchor(symbol)

        if not is_stage12_overheated:
            return False

        # 초과 Case 95 & Case 96: 1주 휩소 노출 작전(WHIPSAW_1SH)은 NQ>=2.0% 과열차단과 별개 독립 가동하며
        # 04:07:00 EST 정리는 enforce_0407_whipsaw_cutoff_plugin 이 전담하므로 여기서 절대 취소하지 않음!
        if es_clean == "WHIPSAW_1SH":
            return False

        # 본진 미체결 매수 주문이 떠 있는 상태에서 NQ=F 진폭 2.0% 돌파 시 즉시 취소 및 장부 소각
        if buy_order_id:
            if ctx.in_memory_ordering_lock.get(symbol, False):
                return True
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                already_gone = False
                try:
                    await ctx.client.cancel_order(buy_order_id)
                except Exception as cancel_e:
                    err_str = str(cancel_e).lower()
                    if "already-canceled" in err_str or "404" in err_str or "not-found" in err_str:
                        already_gone = True
                    elif "already-filled" in err_str:
                        return False

                # 초과 Case 96: 로컬 장부의 buy_order_id 를 즉시 "" 로 원자적 소각하여 무한 루프 차단
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id="",
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)

                from sniper_plugin import _reset_bottom_track_state, _reset_pre_leader_state
                _reset_bottom_track_state()
                _reset_pre_leader_state()

                if not already_gone and _LAST_MACRO1_CANCEL_ID.get(symbol) != buy_order_id:
                    _LAST_MACRO1_CANCEL_ID[symbol] = buy_order_id
                    if ctx.bot and ctx.chat_id:
                        try:
                            await ctx.bot.send_message(
                                chat_id=ctx.chat_id,
                                text=(
                                    f"🛑 <b>[1차 매크로 과열 방어 — 미체결 매수 파기]</b>\n"
                                    f"▫️ 종목: <b>{symbol}</b> | NQ 진폭: <b>{nq_amp:.2f}% (≥2.0%)</b>\n"
                                    f"▫️ 조치: 미체결 매수 주문 취소 및 당일 본진 진입 잠금"
                                ),
                                parse_mode="HTML",
                            )
                        except Exception:
                            pass
                return True
            finally:
                ctx.in_memory_ordering_lock[symbol] = False

        # 미보유·무주문 상태에서 아직 CANCELED_MACRO 잠금이 안 걸려 있다면 조용히 잠금 동기화
        if es_clean != "CANCELED_MACRO" and not (358 <= est_hm <= 406):
            await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
            from sniper_plugin import _reset_bottom_track_state
            _reset_bottom_track_state()
        return False

    # =================================================================
    # [B] 보유 상태(holdings_qty > 0): 수동 진입(MANUAL) 선행 식별 및 QQQ 0.0초 기준점 락온
    # =================================================================
    ref_curr, ref_label = await MacroDataCache.get_stage3_reference_price(
        force_refresh=False,
        realtime_probing=True,
    )
    anchor_amp = float(nq_entry_amp or 0.0) if float(nq_entry_amp or 0.0) > 0.0 else nq_amp

    auto_sessions = (
        "PRE_SNIPER",
        "PRE_PULLBACK",
        "PRE_SNIPER_REARM",
        "PRE_PULLBACK_REARM",
        "WHIPSAW_1SH",
    )
    is_manual_pos = bool(es_clean == "MANUAL" or (es_clean not in auto_sessions and not buy_order_id))

    if is_manual_pos:
        mem_anchor = _MANUAL_STAGE3_ANCHOR_CACHE.get(symbol) or {}
        need_reanchor = bool(
            es_clean != "MANUAL"
            or float(nq_entry_price or 0.0) <= 0.0
            or (not sell_order_id and float(kwargs.get("target_sell_price", 0.0) or 0.0) <= 0.0 and mem_anchor.get("qty", 0) != int(holdings_qty))
            or ((float(nq_entry_price or 0.0) > 3000.0) != (ref_curr > 3000.0) and ref_curr > 0.0)
        )
        if need_reanchor and ref_curr > 0.0:
            nq_entry_price = ref_curr
            nq_entry_amp = anchor_amp
            es_clean = "MANUAL"
            _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
                "ref_price": ref_curr,
                "ref_amp": anchor_amp,
                "locked_ts": time.time(),
                "qty": int(holdings_qty),
            }
            await AssassinLedger.save_state(
                symbol,
                entry_session="MANUAL",
                nq_entry_price=ref_curr,
                nq_entry_amp=anchor_amp,
                entry_trend=("SHORT" if symbol == "SOXS" else "LONG"),
            )
            return False
    else:
        # 초과 Case 92: 자동 진입 포지션 스케일 불일치(구 NQ=F > 3000 vs QQQ < 3000) 또는 미설정 시 자가 치유 재동기화
        if ref_curr > 0.0 and (
            float(nq_entry_price or 0.0) <= 0.0
            or ((float(nq_entry_price or 0.0) > 3000.0) != (ref_curr > 3000.0))
        ):
            nq_entry_price = ref_curr
            nq_entry_amp = anchor_amp
            await AssassinLedger.save_state(
                symbol,
                nq_entry_price=ref_curr,
                nq_entry_amp=anchor_amp,
            )
            return False

    live_p = float(current_price or 0.0)
    if live_p <= 0.0 or avg_price <= 0.0:
        return False

    # 수익 또는 보합 구간(live_p >= avg_price)에서는 2·3차 손절을 절대 발동하지 않고 익절 트레일링 유지
    if live_p >= avg_price:
        return False

    # =================================================================
    # [C] 손실 구간(0 < live_p < avg_price) 2차 & 3차 매크로 방어선 판정
    # =================================================================
    # 2차 방어선: 오직 본진 자동 진입 물량만 NQ>=2.0% 과열 시 손실권 덤핑 (MANUAL 및 WHIPSAW_1SH 는 100% 패스!)
    effective_stage2_breach = bool(
        is_stage12_overheated
        and not is_manual_pos
        and es_clean != "WHIPSAW_1SH"
        and float(nq_entry_amp or 0.0) < 2.0
    )

    # 3차 방어선: 포지션 자체 손실률 즉시 손절 절대 금지!
    # 오직 진입/락온 시점 QQQ 기준가(nq_entry_price) 대비 실시간 QQQ 현재가(ref_curr)가
    # dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%) 이상 역행했을 때만 격발!
    eff_amp = float(nq_entry_amp or 0.0) if float(nq_entry_amp or 0.0) > 0.0 else nq_amp
    dynamic_threshold = max(eff_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT)

    adverse_move_pct = 0.0
    effective_stage3_breach = False
    if ref_curr > 0.0 and float(nq_entry_price or 0.0) > 0.0:
        if symbol == "SOXS":
            # 숏(SOXS) 보유 시 나스닥(QQQ)이 기준가 대비 +dynamic_threshold 이상 상승하면 역행
            adverse_move_pct = (ref_curr - float(nq_entry_price)) / float(nq_entry_price) * 100.0
        else:
            # 롱(SOXL) 보유 시 나스닥(QQQ)이 기준가 대비 -dynamic_threshold 이하 하락하면 역행
            adverse_move_pct = (float(nq_entry_price) - ref_curr) / float(nq_entry_price) * 100.0

        if adverse_move_pct >= dynamic_threshold:
            effective_stage3_breach = True

    if not effective_stage2_breach and not effective_stage3_breach:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=live_p,
            tag_prefix="mdump",
        )
        if not swept_ok:
            return False

        from sniper_plugin import (
            _reset_trailing_runtime_state,
            activate_post_stop_bottom_tracker,
        )
        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)

        pnl_pct = ((sweep_p - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0

        # 초과 Case 87: 04:07 ~ 08:14 EST 프리장 골든타임 내 & NQ<2.0% 상태에서 3차 역행 손절된 경우
        # 당일 퇴근 대신 손절가 하회 최저점 역트레일링(+0.30% 양봉 반등 재진입) 점화!
        can_bottom_rebound = bool(
            effective_stage3_breach
            and not effective_stage2_breach
            and not is_stage12_overheated
            and (407 <= est_hm <= 814)
            and es_clean != "WHIPSAW_1SH"
        )

        if can_bottom_rebound:
            activate_post_stop_bottom_tracker(
                symbol=symbol,
                stop_price=sweep_p,
                now_est=now_est,
            )
            await AssassinLedger.save_state(
                symbol,
                is_session_done=False,
                entry_session="STAGE3_STOP_TRACK",
                buy_order_id="",
                sell_order_id="",
                whipsaw_target_price=0.0,
            )
            defense_title = "🛡️ [3차 실시간 QQQ 역행 방어 ➔ 🪤 최저점 반등 추적 점화]"
            follow_up_note = (
                f"🪤 손절가(${sweep_p:.2f}) 하회 최저점 역트레일링 감시 돌입 (+0.30% 양봉 반등 시 재진입)"
            )
        else:
            await AssassinLedger.save_state(
                symbol,
                is_session_done=True,
                entry_session="CANCELED_MACRO",
                buy_order_id="",
                sell_order_id="",
                last_session_id=current_session_id,
                whipsaw_target_price=0.0,
            )
            # 초과 Case 94: 반대편 종목에 수동 보유 잔고가 있으면 반대편 MANUAL 원장은 보존
            opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
            opp_qty = int((ctx.shared_holdings or {}).get(opp_sym, 0))
            opp_raw = await AssassinLedger.get_raw_dict(opp_sym)
            if opp_qty <= 0 and str(opp_raw.get("entry_session") or "") != "MANUAL":
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )

            if effective_stage2_breach:
                defense_title = "🛑 [2차 매크로 대세장 과열(≥2.0%) 손절 스윕]"
                follow_up_note = f"🛑 NQ 당일 진폭 {nq_amp:.2f}% 돌파 ➡️ 당일 본진 매매 마감"
            else:
                defense_title = f"🛡️ [3차 실시간 {ref_label} 역행 방어 손절 스윕]"
                follow_up_note = (
                    f"▫️ {ref_label} 역행: {adverse_move_pct:+.2f}% (임계치 -{dynamic_threshold:.2f}%)"
                )

        if ctx.bot and ctx.chat_id:
            try:
                await ctx.bot.send_message(
                    chat_id=ctx.chat_id,
                    text=(
                        f"<b>{html.escape(defense_title)}</b>\n"
                        f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                        f"▫️ 평단: ${avg_price:.2f} ➡️ 스윕가: <b>${sweep_p:.2f} ({pnl_pct:+.2f}%)</b>\n"
                        f"▫️ 기준 {ref_label}: {float(nq_entry_price):.2f} ➡️ 현재: {ref_curr:.2f} "
                        f"(역행 {adverse_move_pct:+.2f}% / 한도 {dynamic_threshold:.2f}%)\n"
                        f"{html.escape(follow_up_note)}"
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass

        if ctx.wakeup_event:
            ctx.wakeup_event.set()
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


async def enforce_0930_regular_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    is_session_done: bool = False,
    buy_order_id: str = "",
    **kwargs,
) -> tuple[str, bool]:
    """
    초과 Case 48 & Case 84:
    08:15:00 EST 거시경제 지표 발표 15분 전 선제 컷오프 (08:15 ~ 15:58 EST 전 구간 감시).
    - 미체결 매수 주문(buy_order_id) 잔존 시 즉시 선취소 및 장부 소각
    - 최저점 역트레일링 캐시 및 프리장 상승 주도주 캐시 파기, 당일 신규 매수 잠금(is_session_done = True)
    """
    from sniper_plugin import _reset_bottom_track_state, _reset_pre_leader_state
    _reset_bottom_track_state()
    _reset_pre_leader_state()

    today_str = now_est.strftime("%Y-%m-%d")
    cleaned_buy_id = str(buy_order_id or "").strip()

    if cleaned_buy_id:
        try:
            await ctx.client.cancel_order(cleaned_buy_id)
        except Exception:
            pass
        cleaned_buy_id = ""
        await AssassinLedger.save_state(
            symbol,
            buy_order_id="",
            is_session_done=True,
            whipsaw_target_price=0.0,
        )
        is_session_done = True

        if _LAST_CUTOFF_0815_DATE.get(symbol) != today_str:
            _LAST_CUTOFF_0815_DATE[symbol] = today_str
            if ctx.bot and ctx.chat_id:
                try:
                    await ctx.bot.send_message(
                        chat_id=ctx.chat_id,
                        text=(
                            f"🔒 <b>[08:15 EST 거시지표 선제 컷오프]</b>\n"
                            f"▫️ 종목: <b>{symbol}</b> 미체결 매수 주문 즉시 파기 완료\n"
                            f"▫️ 08:15 EST 이후 신규 매수 진입을 전면 잠금합니다."
                        ),
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
    elif not is_session_done:
        await AssassinLedger.save_state(
            symbol,
            is_session_done=True,
        )
        is_session_done = True

    return cleaned_buy_id, is_session_done


async def execute_late_regular_vwap_escape_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    current_price: float = 0.0,
    vwap_val: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    target_sell_price: float = 0.0,
    entry_session: str = "",
    **kwargs,
) -> bool:
    """
    초과 Case 98:
    정규장 장마감 1시간 전(15:00:00 ~ 15:58:59 EST) 미익절 표류 물량 당일 정규장 누적 VWAP 도달 즉시 덤핑 탈출(🛟 VWAP_ESCAPE).
    - 이미 +1.00% 하이킨 아시 트레일링이 점화(armed == True)되었거나 현재가가 +1.00% 점화가 이상인 수익 포지션은
      기존 하이킨 아시 트레일링이 전담하도록 100% 보호합니다.
    """
    if not is_active or int(holdings_qty) <= 0:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (1500 <= est_hm <= 1558):
        return False

    live_p = float(current_price or 0.0)
    reg_vwap = float(vwap_val or 0.0)
    if live_p <= 0.0 or reg_vwap <= 0.0 or avg_price <= 0.0:
        return False

    from sniper_plugin import get_trailing_runtime_status
    tr_st = get_trailing_runtime_status(symbol)
    trail_armed = bool(tr_st.get("armed", False))
    arm_price = (
        float(target_sell_price)
        if float(target_sell_price or 0.0) > 0.0
        else (math.ceil(avg_price * (1.0 + SINGLE_TARGET_PROFIT_RATE / 100.0) * 100.0) / 100.0)
    )

    if trail_armed or live_p >= arm_price:
        return False

    vwap_trigger_p = math.floor(reg_vwap * 100.0) / 100.0
    if live_p < vwap_trigger_p:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=live_p,
            tag_prefix="vwapesc",
        )
        if not swept_ok:
            return False

        from sniper_plugin import _reset_trailing_runtime_state
        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)

        await AssassinLedger.save_state(
            symbol,
            is_session_done=True,
            entry_session="VWAP_ESCAPE",
            buy_order_id="",
            sell_order_id="",
            whipsaw_target_price=0.0,
        )

        pnl_pct = ((sweep_p - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0
        if ctx.bot and ctx.chat_id:
            try:
                await ctx.bot.send_message(
                    chat_id=ctx.chat_id,
                    text=(
                        f"🛟 <b>[15:00~15:58 미익절 정규장 VWAP 도달 탈출 스윕]</b>\n"
                        f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                        f"▫️ 평단: ${avg_price:.2f} \vert{} 정규장 VWAP: <b>${reg_vwap:.2f}</b>\n"
                        f"▫️ 탈출 스윕가: <b>${sweep_p:.2f} ({pnl_pct:+.2f}%)</b>"
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass

        if ctx.wakeup_event:
            ctx.wakeup_event.set()
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


async def execute_moc_zero_overnight_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    current_price: float = 0.0,
    **kwargs,
) -> bool:
    """
    Case 16 & Case 24:
    15:59 ~ 16:01 EST 절대 제로-오버나이트 MOC 2단계 방탄 강제 청산망.
    - 미체결 매수 주문 즉시 파기
    - 보유 물량 존재 시 1단계 modify_order 매수 1호가 즉시 정정 스윕, 실패 시 2단계 선취소 + 2.0초 폴링 후 매수 1호가 LIMIT 스윕
    """
    if not is_active:
        return False

    if buy_order_id:
        try:
            await ctx.client.cancel_order(buy_order_id)
        except Exception:
            pass
        await AssassinLedger.save_state(
            symbol,
            buy_order_id="",
            is_session_done=True,
            whipsaw_target_price=0.0,
        )

    if int(holdings_qty) <= 0:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=current_price,
            tag_prefix="moc",
        )
        if swept_ok:
            from sniper_plugin import _reset_trailing_runtime_state
            _reset_trailing_runtime_state(symbol)
            _reset_manual_stage3_anchor(symbol)
            await AssassinLedger.save_state(
                symbol,
                is_session_done=True,
                buy_order_id="",
                sell_order_id="",
                whipsaw_target_price=0.0,
            )
            if ctx.bot and ctx.chat_id:
                try:
                    await ctx.bot.send_message(
                        chat_id=ctx.chat_id,
                        text=(
                            f"🛑 <b>[15:59 EST 제로-오버나이트 MOC 강제 청산]</b>\n"
                            f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                            f"▫️ 스윕 지정가: <b>${sweep_p:.2f}</b> (매수 1호가 즉시 스윕)"
                        ),
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
        return swept_ok
    finally:
        ctx.in_memory_ordering_lock[symbol] = False
