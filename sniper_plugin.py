# -*- coding: utf-8 -*-
"""
# NEW: sniper_plugin.py
프리장 본진 골든타임 (04:07 ~ 08:15 EST) 동적 VWAP -0.50% 언더슈팅 매수 덫 투하,
15초/$0.03 동적 추적 정정, 07:45 EST 이전 다회전(Multi-Cycle) 재출동 및
3차 역행 손절 후 최저점 +0.30% 양봉 반등 재진입(RE_BOT_TRAIL) 전술 플러그인 (Step 04 / 10)
- 제3헌법: 프리장 본진 진입/정정/재진입 상태 머신 전담 분리
- 제4헌법: 모든 타임라인 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: 지정가 매수 주문 발사 시 고유 clientOrderId 주입
- 🎯 3 & Case 52, 54, 64, 65, 88, 89, 97 ~ 102 완벽 구현
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import NQ_OVERHEAT_THRESHOLD
from quant_indicators_plugin import (
    get_logical_trading_date_str,
    refresh_metrics_with_live_tick,
    select_vwap_trend_leader,
)
from toss_api import (
    EST_TZ,
    REAL_ALLOWED_SYMBOLS,
    _NonRetryableApiError,
    generate_client_order_id,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.sniper_plugin")

# 동적 VWAP 매수 덫 자동 정정 최소 가격 괴리($0.03 및 0.05%) 및 최소 쿨다운(15.0초) 상수 (🎯 3)
TRAP_MODIFY_MIN_DIFF_USD: float = 0.03
TRAP_MODIFY_MIN_DIFF_PCT: float = 0.05
TRAP_MODIFY_COOLDOWN_SEC: float = 15.0


# ---------------------------------------------------------------------------
# 내부 헬퍼 : 심볼별 최신 실시간 보정 지표 조회 및 배타적 포지션/주문 상태 점검
# ---------------------------------------------------------------------------
def _get_live_symbol_metrics(
    context: Any,
    symbol: str,
    prices: Dict[str, float],
    now_est: datetime,
) -> Dict[str, Any]:
    """
    context.candle_metrics_cache 에 저장된 5.0초 캔들 기저 지표에
    현재 틱 가격(prices[symbol])을 결합하여 0.0ms 실시간 보정 지표를 반환한다.
    """
    sym = str(symbol or "").upper().strip()
    raw_cache = getattr(context, "candle_metrics_cache", {})
    base_met = raw_cache.get(sym) if isinstance(raw_cache, dict) else None
    live_p = safe_float((prices or {}).get(sym), 0.0)
    if live_p <= 0.0 and isinstance(base_met, dict):
        live_p = safe_float(base_met.get("live_price"), 0.0)
    return refresh_metrics_with_live_tick(base_met, live_p, now_est=now_est)


def _has_any_active_position_or_order(
    context: Any, holdings: Dict[str, Dict[str, Any]]
) -> bool:
    """
    Case 52 & Case 54:
    SOXS 와 SOXL 중 어느 한 종목이라도 실보유 잔고(qty > 0)가 있거나
    장부상 미체결 매수/매도 주문 ID 가 존재하면 True 를 반환하여 양방향 동시 진입을 원천 차단한다.
    """
    for sym in REAL_ALLOWED_SYMBOLS:
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)
        if h_qty > 0:
            return True
        st = context.ledger.get_state(sym)
        if str(st.get("buy_order_id") or "").strip():
            return True
        if str(st.get("sell_order_id") or "").strip():
            return True
    return False


async def _calculate_safe_order_qty(
    context: Any, symbol: str, order_price: float
) -> Tuple[int, float, str]:
    """
    Case 65:
    장부에 설정된 종목 시드 예산(budget)과 토스 계좌 실시간 USD 매수가능금액(0.5% 안전 버퍼 차감)을
    비교하여 안전한 주문 수량(정수 주)을 산출한다.
    """
    op = safe_float(order_price, 0.0)
    if op <= 0.0:
        return 0, 0.0, "INVALID_ORDER_PRICE"

    st = context.ledger.get_state(symbol)
    ledger_budget = max(0.0, safe_float(st.get("budget"), 1000.0))
    bp_usd = await context.api.get_buying_power_usd(safety_buffer_pct=0.5)

    # 계좌 예수금이 조회되면 min(장부예산, 안전예수금)을 적용하고, 일시 조회 실패(0.0) 시 장부예산 사용
    eff_budget = min(ledger_budget, bp_usd) if bp_usd > 0.0 else ledger_budget
    qty = int(math.floor((eff_budget + 1e-9) / op))
    if qty <= 0:
        return 0, eff_budget, f"INSUFFICIENT_BUDGET(eff=${eff_budget:.2f}, price=${op:.2f})"
    return qty, eff_budget, "OK"


def mark_auto_fill_grace_shield(
    context: Any, symbol: str, entry_session: str, fill_price: float = 0.0
) -> None:
    """
    🎯 5 & Case 102:
    매수 주문이 거래소에서 FILLED 된 직후 토스 잔고 API(/api/v1/holdings) 인덱싱 시차(0.1 ~ 0.4초) 동안
    reconcile_zero_holdings_exit_plugin 이 장부를 초기화하거나 defense_plugin 이 MANUAL 로 오분류하는
    경합을 원천 차단하기 위해 15.0초 그레이스 쉴드 타임스탬프를 각인한다.
    """
    sym = str(symbol or "").upper().strip()
    guard_map = getattr(context, "indexing_grace_cache", None)
    if isinstance(guard_map, dict):
        guard_map[sym] = {
            "last_auto_fill_ts": time.monotonic(),
            "entry_session": str(entry_session or "PRE_VWAP_M050"),
            "fill_price": safe_float(fill_price, 0.0),
        }


# ---------------------------------------------------------------------------
# [Step 1] 04:07 EST 프리장 추세추종 주도주(👑 vwap_leader_sym) 선정 및 재기동 복원
# ---------------------------------------------------------------------------
async def update_premarket_trend_leader(
    context: Any,
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 1] & Case 97:
    - 04:07:00 ~ 08:14:59 EST 구간에 진입하면 SOXL 과 SOXS 의 04:00 ~ 04:07 EST 누적 VWAP_0407 대비
      04:07 EST 기준 가격(C_0407)을 비교하여 C_0407 > VWAP_0407 (disp_0407 > 0.0%) 조건을 만족하는
      종목을 당일 프리장 추세추종 주도주(_PRE_LEADER_CACHE["leader_sym"])로 불변 각인한다.
    - 양쪽 모두 disp_0407 > 0.0% 이면 초기 이격도 disp_0407 상위 1위 종목을 선정한다.
    - 장중 프로세스 재기동 시에도 1분봉 시계열로부터 leader_sym, vwap_0407, disp_0407 을 100% 자동 복원한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)

    leader_cache: Dict[str, Any] = getattr(context, "pre_leader_cache", {})
    if leader_cache.get("logical_date") != logical_date:
        leader_cache.clear()
        leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": False,
                "leader_sym": "",
                "vwap_0407": 0.0,
                "c_0407": 0.0,
                "disp_0407": 0.0,
                "notified": False,
                "hybrid_mode": bool(leader_cache.get("hybrid_mode", False)),
            }
        )

    # 04:07 EST 이전에는 아직 초기 7분 VWAP 수집 구간이므로 대기
    if not (407 <= hm <= 814):
        return leader_cache

    if leader_cache.get("locked") and str(leader_cache.get("leader_sym") or "") in REAL_ALLOWED_SYMBOLS:
        return leader_cache

    soxs_met = _get_live_symbol_metrics(context, "SOXS", prices, dt_now)
    soxl_met = _get_live_symbol_metrics(context, "SOXL", prices, dt_now)

    # 만약 기동 직후라 캔들 캐시에 vwap_0407 이 아직 계산되지 않았다면 즉시 1분봉 조회 후 산출
    if (
        safe_float(soxs_met.get("vwap_0407"), 0.0) <= 0.0
        and safe_float(soxl_met.get("vwap_0407"), 0.0) <= 0.0
        and hasattr(context, "refresh_symbol_candles_once")
    ):
        try:
            await context.refresh_symbol_candles_once(("SOXS", "SOXL"))
            soxs_met = _get_live_symbol_metrics(context, "SOXS", prices, dt_now)
            soxl_met = _get_live_symbol_metrics(context, "SOXL", prices, dt_now)
        except Exception as exc:
            logger.warning("⚠️ 04:07 주도주 복원용 캔들 즉시 갱신 실패: %s", exc)

    res = select_vwap_trend_leader(soxs_met, soxl_met, session_mode="PRE")
    win_sym = str(res.get("leader_sym") or "").upper().strip()

    if win_sym in REAL_ALLOWED_SYMBOLS:
        leader_cache.update(
            {
                "logical_date": logical_date,
                "locked": True,
                "leader_sym": win_sym,
                "vwap_0407": safe_float(res.get("vwap_init"), 0.0),
                "c_0407": safe_float(res.get("c_init"), 0.0),
                "disp_0407": safe_float(res.get("disp_init"), 0.0),
            }
        )
        if not leader_cache.get("notified") and hasattr(context, "send_slim_alert"):
            leader_cache["notified"] = True
            await context.send_slim_alert(
                title=f"👑 [프리장 추세 주도주 락온] {win_sym}",
                body=(
                    f"04:07가 ${leader_cache['c_0407']:.2f} > VWAP_0407 ${leader_cache['vwap_0407']:.2f} "
                    f"(이격 +{leader_cache['disp_0407']:.2f}%) · 동적 VWAP -0.50% 매수덫 가동"
                ),
            )
    return leader_cache


# ---------------------------------------------------------------------------
# [Step 2] 실시간 동적 프리장 VWAP -0.50% 언더슈팅 매수 덫 투하 (PRE_VWAP_M050)
# ---------------------------------------------------------------------------
async def execute_premarket_vwap_sniper_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 2-1] & Case 52, 54, 64, 65, 97 ~ 100:
    04:07:00 ~ 07:55:59 EST 동안:
    1) 스나이퍼 가동 ON, 양쪽 잔고 0주 및 미체결 주문 부재, NQ=F 당일 진폭 < 2.0% 검증
    2) 3차 손절 후 최저점 반등 추적(_BOTTOM_TRACK_CACHE["active"]) 중이 아닐 때,
       당일 프리장 추세 주도주(leader_sym)에 대해 실시간 동적 프리장 VWAP_t 대비 -0.50% 하단가
       (trap_buy_price = min(cur_p_fl, floor(pre_vwap * 0.9950 * 100) / 100))에
       일반 지정가(LIMIT, DAY) 매수 덫 주문을 투하한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute

    # 신규 매수 덫 발사 허용 윈도우: 04:07 ~ 07:55 EST (07:56 ~ 08:14 는 기존 덫 체결/청산만 감시)
    if not (407 <= hm <= 755):
        return {"status": "OUTSIDE_WINDOW"}

    # 3차 역행 손절 후 최저점 반등 추적 중일 때는 RE_BOT_TRAIL 엔진이 우선권을 가진다
    bottom_cache = getattr(context, "bottom_track_cache", {})
    if isinstance(bottom_cache, dict) and bottom_cache.get("active"):
        return {"status": "BOTTOM_TRACK_ACTIVE"}

    # 1차 매크로 과열 방어선: NQ=F 당일 진폭 >= 2.0% 이면 진입 원천 차단
    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    if nq_amp >= NQ_OVERHEAT_THRESHOLD:
        return {"status": "BLOCKED_MACRO_OVERHEAT", "nq_amp": nq_amp}

    leader_cache = await update_premarket_trend_leader(context, prices, dt_now)
    leader_sym = str(leader_cache.get("leader_sym") or "").upper().strip()
    if leader_sym not in REAL_ALLOWED_SYMBOLS:
        return {"status": "NO_LEADER_LOCKED"}

    other_sym = "SOXL" if leader_sym == "SOXS" else "SOXS"
    st_leader = context.ledger.get_state(leader_sym)
    st_other = context.ledger.get_state(other_sym)

    # 스나이퍼 가동 ON 여부 및 당일 세션 마감 여부 확인
    if not bool(st_leader.get("is_active", True)):
        return {"status": "BOT_INACTIVE"}
    if bool(st_leader.get("is_session_done", False)):
        return {"status": "SESSION_DONE"}
    if str(st_leader.get("entry_session") or "") == "CANCELED_MACRO":
        return {"status": "CANCELED_MACRO"}

    # 이미 보유 포지션이나 미체결 주문이 존재하면 중복 발사 금지
    if _has_any_active_position_or_order(context, holdings):
        return {"status": "ACTIVE_POSITION_OR_ORDER_EXISTS"}

    met = _get_live_symbol_metrics(context, leader_sym, prices, dt_now)
    pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
    live_p = safe_float(met.get("live_price"), 0.0)
    if pre_vwap <= 0.0 or live_p <= 0.0:
        return {"status": "METRICS_NOT_READY"}

    cur_p_fl = math.floor(live_p * 100.0) / 100.0
    use_hybrid = bool(leader_cache.get("hybrid_mode", False))
    raw_trap_p = (
        safe_float(met.get("pre_hyb_trap_m050"), 0.0)
        if use_hybrid
        else safe_float(met.get("pre_dyn_trap_m050"), 0.0)
    )
    if raw_trap_p <= 0.0:
        raw_trap_p = math.floor((pre_vwap * 0.9950) * 100.0) / 100.0

    # 현재가보다 높게 지정가가 나가지 않도록 cur_p_fl 캡 적용 (🎯 3)
    trap_buy_price = min(cur_p_fl, raw_trap_p)
    if trap_buy_price <= 0.0:
        return {"status": "INVALID_TRAP_PRICE"}

    # Case 54: 비동기 타격 뮤텍스 락온
    async with context.in_memory_ordering_lock:
        async with context.dual_strike_mutex:
            # 락 내부에서 재검증 (Double-Checked Locking)
            st_leader_now = context.ledger.get_state(leader_sym)
            if (
                bool(st_leader_now.get("is_session_done", False))
                or str(st_leader_now.get("buy_order_id") or "").strip()
            ):
                return {"status": "ALREADY_LOCKED_IN_MUTEX"}

            qty, eff_budget, qty_reason = await _calculate_safe_order_qty(
                context, leader_sym, trap_buy_price
            )
            if qty <= 0:
                if hasattr(context, "send_strike_hold_alert_once"):
                    await context.send_strike_hold_alert_once(
                        symbol=leader_sym,
                        reason_key=f"BUDGET_{leader_sym}",
                        message=f"⚠️ [{leader_sym} 매수덫 보류] {qty_reason}",
                    )
                return {"status": "HOLD_BUDGET", "reason": qty_reason}

            qqq_ref = await context.macro_cache.ensure_qqq_live_price(
                safe_float((macro or {}).get("qqq_price"), 0.0)
            )
            if qqq_ref <= 0.0:
                qqq_ref = safe_float((macro or {}).get("nq_price"), 0.0)

            cid = generate_client_order_id(prefix=f"PRE{leader_sym}")
            try:
                ord_res = await context.api.create_order(
                    symbol=leader_sym,
                    side="BUY",
                    quantity=qty,
                    price=trap_buy_price,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                    enforce_daymarket_block=True,
                )
            except Exception as exc:
                logger.error("🚨 [%s] 프리장 동적 VWAP -0.50%% 매수덫 발사 실패: %s", leader_sym, exc)
                if hasattr(context, "send_strike_hold_alert_once"):
                    await context.send_strike_hold_alert_once(
                        symbol=leader_sym,
                        reason_key=f"API_ERR_{leader_sym}",
                        message=f"🚨 [{leader_sym} 매수덫 주문 실패] {exc}",
                    )
                return {"status": "ORDER_ERROR", "error": str(exc)}

            oid = str(ord_res.get("orderId") or "").strip()
            trend_tag = "DOWN" if leader_sym == "SOXS" else "UP"
            now_mono = time.monotonic()

            # Case 52: 당일 배타적 단일 포지션 뮤텍스 — 양쪽 장부 동시 잠금 및 주문 정보 기록
            await context.ledger.update_state(
                leader_sym,
                buy_order_id=oid,
                client_order_id=cid,
                ordered_qty=qty,
                buy_price=trap_buy_price,
                entry_session="PRE_VWAP_M050",
                entry_trend=trend_tag,
                nq_entry_price=qqq_ref,
                nq_entry_amp=nq_amp,
                is_session_done=True,
                last_modify_mono=now_mono,
                entry_ts=time.time(),
            )
            await context.ledger.update_state(
                other_sym,
                is_session_done=True,
            )

            # Case 64: 매수 발사 직후 wakeup_event 즉시 격발
            context.wakeup_event.set()

            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🪤 [프리장 VWAP -0.50% 매수덫 장전] {leader_sym} {qty}주",
                    body=(
                        f"지정가 ${trap_buy_price:.2f} (동적VWAP ${pre_vwap:.2f} · 현가 ${live_p:.2f}) "
                        f"· QQQ기준 ${qqq_ref:.2f} · NQ진폭 {nq_amp:.2f}%"
                    ),
                )

            return {
                "status": "TRAP_PLACED",
                "symbol": leader_sym,
                "orderId": oid,
                "price": trap_buy_price,
                "qty": qty,
            }


# ---------------------------------------------------------------------------
# [Step 2-2] 시간 흐름에 따른 동적 VWAP_t 상승 반영 매수 덫 자동 추적 정정 (modify_order)
# ---------------------------------------------------------------------------
async def sync_dynamic_vwap_buy_order_modify(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 2-2] & Case 24, 58, 74, 98, 102:
    - 04:07 ~ 07:55 EST 동안 미체결 매수 덫(buy_order_id 존재, holdings_qty == 0,
      entry_session == 'PRE_VWAP_M050') 대기 중:
    1) 주문 상세(get_order_detail)를 조회하여 FILLED 감지 시 즉시 last_auto_fill_ts 그레이스 쉴드를
       각인하고 wakeup_event 를 깨워 0.0초 만에 +5.00% 안전 캡 매도 덫 장전 루프로 직결한다.
    2) PENDING 상태이고 실시간 동적 프리장 VWAP_t 가 변화하여 신규 역산 타점(VWAP_t * 0.9950)이
       기존 주문 단가와 $0.03 이상(>= 0.05%) 벌어지고 최소 15초가 경과하면 modify_order 로
       매수 덫 단가를 최신 동적 VWAP_t * 0.9950 으로 자동 정정 추적한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (407 <= hm <= 814):
        return {"status": "OUTSIDE_WINDOW"}

    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    leader_cache = getattr(context, "pre_leader_cache", {})
    use_hybrid = bool(leader_cache.get("hybrid_mode", False))

    for sym in REAL_ALLOWED_SYMBOLS:
        st = context.ledger.get_state(sym)
        buy_oid = str(st.get("buy_order_id") or "").strip()
        if not buy_oid:
            continue

        entry_sess = str(st.get("entry_session") or "")
        h_qty = safe_int((holdings or {}).get(sym, {}).get("qty"), 0)

        # 이미 잔고가 반영된 경우 buy_order_id 정리는 sniper_exit_plugin 에서 처리
        if h_qty > 0:
            continue

        detail = await context.api.get_order_detail(buy_oid)
        if isinstance(detail, dict):
            ord_status = str(detail.get("status") or "").upper().strip()
            exec_obj = (
                detail.get("execution")
                if isinstance(detail.get("execution"), dict)
                else {}
            )
            filled_qty = safe_int(exec_obj.get("filledQuantity"), 0)
            avg_fill_p = safe_float(exec_obj.get("averageFilledPrice"), 0.0)

            # Case 102: FILLED 직후 잔고 API 인덱싱 시차(0.1 ~ 0.4초) 보호용 그레이스 쉴드 즉시 각인!
            if ord_status == "FILLED" or (filled_qty > 0 and ord_status == "FILLED"):
                mark_auto_fill_grace_shield(
                    context,
                    symbol=sym,
                    entry_session=entry_sess or "PRE_VWAP_M050",
                    fill_price=avg_fill_p if avg_fill_p > 0.0 else safe_float(st.get("buy_price"), 0.0),
                )
                context.wakeup_event.set()
                return {
                    "status": "FILLED_DETECTED_GRACE_LOCKED",
                    "symbol": sym,
                    "orderId": buy_oid,
                }

            # Case 58 & 63: 부분 체결(PARTIAL_FILLED) 진행 중에는 수량 증발 방지를 위해 정정 보류
            if ord_status == "PARTIAL_FILLED" or filled_qty > 0:
                mark_auto_fill_grace_shield(
                    context,
                    symbol=sym,
                    entry_session=entry_sess or "PRE_VWAP_M050",
                    fill_price=avg_fill_p,
                )
                return {"status": "PARTIAL_FILLED_WAIT", "symbol": sym}

            # 거래소 외부 취소/거부 감지 시 장부 클리어 및 재출동 해금
            if ord_status in ("CANCELED", "REJECTED"):
                other_sym = "SOXL" if sym == "SOXS" else "SOXS"
                can_reenter = bool(hm <= 745 and nq_amp < NQ_OVERHEAT_THRESHOLD)
                await context.ledger.update_state(
                    sym,
                    buy_order_id="",
                    ordered_qty=0,
                    buy_price=0.0,
                    entry_session="" if can_reenter else "CANCELED_EXTERNAL",
                    is_session_done=not can_reenter,
                )
                if can_reenter:
                    await context.ledger.update_state(other_sym, is_session_done=False)
                return {"status": f"ORDER_{ord_status}_CLEARED", "symbol": sym}

        # 동적 VWAP 추적 정정은 PRE_VWAP_M050 세션이고 04:08 ~ 07:55 EST 사이일 때만 수행
        if entry_sess != "PRE_VWAP_M050" or not (408 <= hm <= 755):
            continue

        last_mod_mono = safe_float(st.get("last_modify_mono"), 0.0)
        now_mono = time.monotonic()
        if (now_mono - last_mod_mono) < TRAP_MODIFY_COOLDOWN_SEC:
            continue

        met = _get_live_symbol_metrics(context, sym, prices, dt_now)
        pre_vwap = safe_float(met.get("pre_vwap"), 0.0)
        live_p = safe_float(met.get("live_price"), 0.0)
        if pre_vwap <= 0.0 or live_p <= 0.0:
            continue

        cur_p_fl = math.floor(live_p * 100.0) / 100.0
        raw_new_trap = (
            safe_float(met.get("pre_hyb_trap_m050"), 0.0)
            if use_hybrid
            else safe_float(met.get("pre_dyn_trap_m050"), 0.0)
        )
        if raw_new_trap <= 0.0:
            raw_new_trap = math.floor((pre_vwap * 0.9950) * 100.0) / 100.0

        new_trap_p = min(cur_p_fl, raw_new_trap)
        old_buy_p = safe_float(st.get("buy_price"), 0.0)
        if new_trap_p <= 0.0 or old_buy_p <= 0.0:
            continue

        diff_usd = abs(new_trap_p - old_buy_p)
        diff_pct = (diff_usd / old_buy_p) * 100.0
        if diff_usd < TRAP_MODIFY_MIN_DIFF_USD or diff_pct < TRAP_MODIFY_MIN_DIFF_PCT:
            continue

        # 예산 초과 방어: 상향 정정 시 현재 주문 수량(ordered_qty) * new_trap_p 가 예산을 초과하는지 점검
        ordered_qty = max(1, safe_int(st.get("ordered_qty"), 1))
        ledger_budget = max(0.0, safe_float(st.get("budget"), 1000.0))
        if (ordered_qty * new_trap_p) > (ledger_budget * 1.005):
            continue

        async with context.in_memory_ordering_lock:
            try:
                mod_res = await context.api.modify_order(
                    order_id=buy_oid,
                    new_price=new_trap_p,
                    order_type="LIMIT",
                    symbol=sym,
                    side="BUY",
                )
                new_oid = str(mod_res.get("orderId") or buy_oid).strip()
                await context.ledger.update_state(
                    sym,
                    buy_order_id=new_oid,
                    buy_price=new_trap_p,
                    last_modify_mono=time.monotonic(),
                )
                logger.info(
                    "🔄 [%s] 동적 VWAP 매수덫 추적 정정 완료: $%.2f -> $%.2f (VWAP=$%.2f, orderId=%s)",
                    sym,
                    old_buy_p,
                    new_trap_p,
                    pre_vwap,
                    new_oid,
                )
                return {
                    "status": "TRAP_MODIFIED",
                    "symbol": sym,
                    "old_price": old_buy_p,
                    "new_price": new_trap_p,
                    "orderId": new_oid,
                }
            except _NonRetryableApiError as exc:
                if exc.error_code == "already-filled":
                    mark_auto_fill_grace_shield(
                        context,
                        symbol=sym,
                        entry_session=entry_sess,
                        fill_price=old_buy_p,
                    )
                    context.wakeup_event.set()
                    return {"status": "ALREADY_FILLED_ON_MODIFY", "symbol": sym}
                logger.warning("⚠️ [%s] 매수덫 정정 거절(%s): %s", sym, exc.error_code, exc.error_message)
            except Exception as exc:
                logger.warning("⚠️ [%s] 매수덫 정정 예외: %s", sym, exc)

    return {"status": "NO_MODIFY_NEEDED"}


# ---------------------------------------------------------------------------
# [Step 3] 3차 역행 손절 후 최저점 역트레일링 +0.30% 양봉 반등 재진입 (RE_BOT_TRAIL)
# ---------------------------------------------------------------------------
def activate_bottom_rebound_tracker(
    context: Any,
    symbol: str,
    stop_price: float,
    nq_amp: float,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 3] & Case 88, 89:
    04:07 ~ 08:14 EST 프리장 골든타임 중 NQ=F < 2.0% 상태에서 3차 실시간 QQQ 역행 안전망으로
    손절된 직후 defense_plugin 에서 호출되어, 당일 퇴근 없이 손절가 하회 최저점(Bottom) 역트레일링
    추적 캐시(_BOTTOM_TRACK_CACHE)를 활성화한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)
    sym = str(symbol or "").upper().strip()
    sp = safe_float(stop_price, 0.0)

    bottom_cache: Dict[str, Any] = getattr(context, "bottom_track_cache", {})
    if not (407 <= hm <= 814) or safe_float(nq_amp, 0.0) >= NQ_OVERHEAT_THRESHOLD or sp <= 0.0:
        bottom_cache.clear()
        bottom_cache["active"] = False
        return bottom_cache

    bottom_cache.clear()
    bottom_cache.update(
        {
            "active": True,
            "logical_date": logical_date,
            "stopped_sym": sym,
            "last_stop_price": sp,
            "post_stop_low": sp,
            "dipped_below_stop": False,
            "activated_ts": time.time(),
        }
    )
    logger.info(
        "🪤 [RE_BOT_TRAIL 가동] %s 3차 손절($%.2f) 후 최저점 역트레일링 +0.30%% 반등 감시 개시",
        sym,
        sp,
    )
    return bottom_cache


async def execute_post_stop_bottom_rebound_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    macro: Dict[str, Any],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🎯 3 [Step 3] & Case 88, 89:
    - 04:07 ~ 08:14 EST 중 _BOTTOM_TRACK_CACHE["active"] == True 상태일 때:
    1) NQ=F 당일 진폭이 2.0% 이상 돌파하면 즉시 추적을 중단하고 과열차단으로 전환한다.
    2) 손절된 종목(stopped_sym)이 신저가를 경신(cur_p < post_stop_low)하는 동안에는
       매수를 100% 보류(칼날 회피)하고 post_stop_low 만 낮춰 갱신한다.
    3) 손절가 아래 최저점(post_stop_low < last_stop_price)을 찍고 신저가 갱신이 멈춘 뒤,
       경유 최저점 대비 +0.30% 이상 반등(cur_p >= ceil(post_stop_low * 1.0030 * 100) / 100) &
       실시간 양봉 전환(is_raw_bull 또는 is_ha_bull) & 과매도 반등권(live_rsi <= 38.0 및 cur_p <= last_stop_price)
       충족 시 즉시 실시간 현재가(cur_p_fl) LIMIT 주문으로 재진입(RE_BOT_TRAIL)한다!
    """
    bottom_cache: Dict[str, Any] = getattr(context, "bottom_track_cache", {})
    if not isinstance(bottom_cache, dict) or not bottom_cache.get("active"):
        return {"status": "INACTIVE"}

    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    logical_date = get_logical_trading_date_str(dt_now, rollover_hour=18)

    # 날짜가 바뀌었거나 08:15 EST 거시지표 컷오프 도달 시 추적 종료
    if bottom_cache.get("logical_date") != logical_date or not (407 <= hm <= 814):
        bottom_cache["active"] = False
        return {"status": "EXPIRED_WINDOW"}

    nq_amp = safe_float((macro or {}).get("nq_amp"), 0.0)
    if nq_amp >= NQ_OVERHEAT_THRESHOLD:
        bottom_cache["active"] = False
        for s in REAL_ALLOWED_SYMBOLS:
            await context.ledger.update_state(
                s, is_session_done=True, entry_session="CANCELED_MACRO"
            )
        return {"status": "CANCELED_BY_OVERHEAT"}

    sym = str(bottom_cache.get("stopped_sym") or "").upper().strip()
    if sym not in REAL_ALLOWED_SYMBOLS:
        bottom_cache["active"] = False
        return {"status": "INVALID_SYMBOL"}

    st_sym = context.ledger.get_state(sym)
    if not bool(st_sym.get("is_active", True)):
        return {"status": "BOT_INACTIVE"}

    if _has_any_active_position_or_order(context, holdings):
        return {"status": "POSITION_OR_ORDER_ALREADY_EXISTS"}

    met = _get_live_symbol_metrics(context, sym, prices, dt_now)
    cur_p = safe_float(met.get("live_price"), 0.0)
    if cur_p <= 0.0:
        return {"status": "NO_LIVE_PRICE"}

    last_stop_p = safe_float(bottom_cache.get("last_stop_price"), 0.0)
    post_low = safe_float(bottom_cache.get("post_stop_low"), last_stop_p)

    # 신저가 경신 중에는 칼날 회피 (매수 100% 보류 및 최저점 갱신)
    if cur_p < post_low:
        bottom_cache["post_stop_low"] = cur_p
        if cur_p < last_stop_p:
            bottom_cache["dipped_below_stop"] = True
        return {
            "status": "NEW_LOW_TRACKING",
            "symbol": sym,
            "post_stop_low": cur_p,
        }

    if post_low < last_stop_p:
        bottom_cache["dipped_below_stop"] = True

    if not bottom_cache.get("dipped_below_stop"):
        return {"status": "WAITING_DIP_BELOW_STOP", "symbol": sym}

    rebound_trigger_p = math.ceil((post_low * 1.0030) * 100.0) / 100.0
    is_bull_candle = bool(met.get("is_raw_bull") or met.get("is_ha_bull"))
    live_rsi = safe_float(met.get("live_rsi"), 50.0)

    # 재진입 4대 조건 검증: 최저점 +0.30% 반등 & 손절가 이하 & 실시간 양봉 & RSI <= 38.0
    if not (
        cur_p >= rebound_trigger_p
        and cur_p <= last_stop_p
        and is_bull_candle
        and live_rsi <= 38.0
    ):
        return {
            "status": "MONITORING_REBOUND",
            "symbol": sym,
            "cur_p": cur_p,
            "rebound_trigger_p": rebound_trigger_p,
            "live_rsi": live_rsi,
        }

    cur_p_fl = math.floor(cur_p * 100.0) / 100.0
    other_sym = "SOXL" if sym == "SOXS" else "SOXS"

    async with context.in_memory_ordering_lock:
        async with context.dual_strike_mutex:
            if not bottom_cache.get("active"):
                return {"status": "ALREADY_CONSUMED"}

            qty, _, qty_reason = await _calculate_safe_order_qty(context, sym, cur_p_fl)
            if qty <= 0:
                return {"status": "HOLD_BUDGET", "reason": qty_reason}

            qqq_ref = await context.macro_cache.ensure_qqq_live_price(
                safe_float((macro or {}).get("qqq_price"), 0.0)
            )
            if qqq_ref <= 0.0:
                qqq_ref = safe_float((macro or {}).get("nq_price"), 0.0)

            cid = generate_client_order_id(prefix=f"REB{sym}")
            try:
                ord_res = await context.api.create_order(
                    symbol=sym,
                    side="BUY",
                    quantity=qty,
                    price=cur_p_fl,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                    enforce_daymarket_block=True,
                )
            except Exception as exc:
                logger.error("🚨 [%s] RE_BOT_TRAIL 반등 재진입 주문 실패: %s", sym, exc)
                return {"status": "ORDER_ERROR", "error": str(exc)}

            oid = str(ord_res.get("orderId") or "").strip()
            bottom_cache["active"] = False
            trend_tag = "DOWN" if sym == "SOXS" else "UP"

            await context.ledger.update_state(
                sym,
                buy_order_id=oid,
                client_order_id=cid,
                ordered_qty=qty,
                buy_price=cur_p_fl,
                entry_session="RE_BOT_TRAIL",
                entry_trend=trend_tag,
                nq_entry_price=qqq_ref,
                nq_entry_amp=nq_amp,
                is_session_done=True,
                last_modify_mono=time.monotonic(),
                entry_ts=time.time(),
            )
            await context.ledger.update_state(other_sym, is_session_done=True)
            context.wakeup_event.set()

            if hasattr(context, "send_slim_alert"):
                await context.send_slim_alert(
                    title=f"🪤 [최저점 +0.30% 반등 재진입] {sym} {qty}주",
                    body=(
                        f"재진입가 ${cur_p_fl:.2f} (최저점 ${post_low:.2f} · 직전손절 ${last_stop_p:.2f}) "
                        f"· RSI {live_rsi:.1f} · 신규QQQ기준 ${qqq_ref:.2f}"
                    ),
                )

            return {
                "status": "RE_BOT_TRAIL_ORDERED",
                "symbol": sym,
                "orderId": oid,
                "price": cur_p_fl,
                "qty": qty,
            }
