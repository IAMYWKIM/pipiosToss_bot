# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장 본진 골든타임(04:07 ~ 08:14 EST) '선택지 B (👑 HYB_35_1ST)':
#       [1] 04:07 ~ 08:14 EST 양방향 대칭 클라이맥스(🪤 DUAL80_TRAP: 매수종목 RSI<=30.0 & 반대>=79.9) 타격
#       [2] 04:07 EST 이후 첫 RSI>=79.9 달성 상승 추세 주도주(FIRST_799 · leader_sym) 각인 후
#           07:00 ~ 08:14 EST 해당 상승종목 RSI<=35.0 추세추종 눌림목 매수(🚀 PB_0700_35),
#       3차 실시간 QQQ 역행 손절 후 손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 재진입(🪤 RE_BOT_TRAIL),
#       1차 슬리피지(-0.18%) 매수 덫 + 조건부 30초 타임쉴드(RSI>30.0 / RSI>35.0 반등 또는 30초 만료 정정),
#       진입 성격별 이원화 하이킨 아시 트레일링(기술적 반등[PRE_SNIPER] ⚡ OLD_IMM_CB [-0.30%] vs
#       진성 상승종목[PRE_PULLBACK 및 모든 수동 매수 MANUAL] 🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020 [초입 양봉 홀딩 후 -0.20%]
#       × 🚀 NQ_VEL_BYPASS), 409 already-modified 자동 추적 정정 및 REPLACED 안전 캡($47.98)/수량(1주➔4주) 왜곡 차단 코어
# =====================================================================
# MODIFIED: 제1 ~ 제6헌법 및 100대 엣지 케이스 방어망(Case 01 ~ Case 100) 완벽 준수
# NEW: 1) `whipsaw_plugin` 동적 안전 바인딩(`import whipsaw_plugin as _whipsaw_mod` + 다중 별칭 `getattr`):
#         `_reset_daymarket_sim_state` / `_reset_day_sim_state` 및 `execute_rsi_monitoring_plugin` 등
#         어떠한 함수명 차이에서도 `ImportError` 가 절대 발생하지 않도록 원천 봉쇄
#      2) 트레일링 스윕 정정 시 `[409 already-modified]` 에러가 발생하면 즉시 거래소 `OPEN` 매도 주문을 조회하여
#         새로 발급된 최신 `orderId` 로 정정을 완수하고, 2단계 취소 폴백 시 `holdings_qty` 를 명시 전달
#      3) 수동 매수(`MANUAL`) 포지션을 `leader_sym` 여부나 시간대와 무관하게 100% `🚀 진성상승 추세추종(TREND_PULLBACK)` 고정
#      4) `_EXIT_SNAPSHOT_CACHE` 를 통해 `REPLACED` 원주문 안전 캡($47.98) 오인 및 `1주 ➔ 4주` 수량 왜곡 영구 소각

import math
import time
import uuid
import html
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    SINGLE_TARGET_PROFIT_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
    calc_compound_next_budget,
)
from defense_plugin import (
    SniperPluginContext,
    TRAIL_ARM_PROFIT_RATE,
    TRAIL_FLOOR_LOCK_RATE,
    TRAIL_DROP_CALLBACK_PCT,
    TRAIL_CALLBACK_PCT,
    TRAIL_SAFETY_CAP_RATE,
    SAFETY_CAP_PROFIT_RATE,
    NQ_VELOCITY_SURGE_PCT,
    NQ_DYNAMIC_FLOOR,
    BOTTOM_REBOUND_PCT,
    POST_STOP_REBOUND_PCT,
    BOTTOM_MAX_RSI_CAP,
    POST_STOP_MAX_RSI,
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_TREND_BOOSTER_RSI,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    FIRST_TRAP_SLIPPAGE_PCT,
    FIRST_TRAP_MIN_DISC_USD,
    TRAP_SHIELD_WAIT_SEC,
    _AUTO_ENTRY_SESSION_TAGS,
    _STRIKE_ALERT_CACHE,
    _RSI_MONITOR_CACHE,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _MANUAL_STAGE3_ANCHOR_CACHE,
    _send_tg_safe,
    _send_strike_hold_alert_once,
    _reset_trailing_runtime_state,
    get_trailing_runtime_status,
    _reset_bottom_track_state,
    _activate_bottom_track_state,
    activate_post_stop_bottom_tracker,
    get_bottom_track_status,
    _reset_pre_leader_state,
    mark_pre_pullback_dispatched,
    get_pre_leader_status,
    update_premarket_trend_leader,
    _reset_manual_stage3_anchor,
    mark_manual_stage3_anchored,
    _reset_swing_10m_state,
    get_swing_10m_status,
    _cancel_open_orders_and_wait_sellable,
    sniper_plugin_cancel_helper,
    execute_late_regular_vwap_escape_plugin,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
    enforce_0815_pre_macro_cutoff_plugin,
)
import whipsaw_plugin as _whipsaw_mod

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")


# =====================================================================
# NEW: whipsaw_plugin 다중 별칭 동적 안전 바인딩 (ImportError 100% 영구 소각)
# =====================================================================
def _ resolve_whipsaw_attr(*names, default_fn=None):
    for nm in names:
        fn = getattr(_whipsaw_mod, nm, None)
        if fn is not None:
            return fn
    return default_fn


def _noop_sync(*args, **kwargs):
    return {"active": False}


async def _noop_async(*args, **kwargs):
    return False


_reset_day_sim_state = _resolve_whipsaw_attr(
    "_reset_day_sim_state",
    "_reset_daymarket_sim_state",
    "reset_daymarket_sim_state",
    default_fn=_noop_sync,
)
_reset_daymarket_sim_state = _reset_day_sim_state

get_daymarket_sim_status = _resolve_whipsaw_attr(
    "get_daymarket_sim_status",
    "get_day_sim_status",
    default_fn=_noop_sync,
)
get_day_sim_status = get_daymarket_sim_status

get_whipsaw_runtime_status = _resolve_whipsaw_attr(
    "get_whipsaw_runtime_status",
    "get_whipsaw_status",
    default_fn=_noop_sync,
)

settle_daymarket_sim_before_premarket = _resolve_whipsaw_attr(
    "settle_daymarket_sim_before_premarket",
    "settle_day_sim_before_premarket",
    default_fn=_noop_async,
)

execute_whipsaw_vwap_exposure_plugin = _resolve_whipsaw_attr(
    "execute_whipsaw_vwap_exposure_plugin",
    "execute_whipsaw_exposure_plugin",
    default_fn=_noop_async,
)

enforce_0407_whipsaw_cutoff_plugin = _resolve_whipsaw_attr(
    "enforce_0407_whipsaw_cutoff_plugin",
    default_fn=None,
)
if enforce_0407_whipsaw_cutoff_plugin is None:
    async def enforce_0407_whipsaw_cutoff_plugin(
        ctx, symbol, now_est, is_active=True, holdings_qty=0,
        buy_order_id="", sell_order_id="", entry_session="", current_price=0.0, **kwargs
    ):
        return buy_order_id, entry_session, False

execute_rsi_monitoring_plugin = _resolve_whipsaw_attr(
    "execute_rsi_monitoring_plugin",
    "execute_daymarket_sim_plugin",
    "execute_day_sim_plugin",
    "run_daymarket_sim_plugin",
    default_fn=_noop_async,
)
execute_daymarket_sim_plugin = execute_rsi_monitoring_plugin


# =====================================================================
# NEW: 정산 왜곡($47.98 안전 캡 오인 및 1주➔4주 뻥튀기) 원천 차단용 불변 스냅샷 캐시
# =====================================================================
def _default_exit_snapshot() -> dict:
    return {
        "last_holdings_qty": 0,
        "avg_price": 0.0,
        "peak_price": 0.0,
        "last_sweep_price": 0.0,
        "safety_cap_price": 0.0,
        "sell_order_id": "",
        "orig_cap_order_id": "",
        "sweep_mode_label": "",
        "sweep_reason": "",
        "updated_at": 0.0,
    }


_EXIT_SNAPSHOT_CACHE: dict[str, dict] = {
    "SOXS": _default_exit_snapshot(),
    "SOXL": _default_exit_snapshot(),
}


def _reset_exit_snapshot(symbol: str = None):
    targets = (str(symbol).upper(),) if str(symbol or "").upper() in ("SOXS", "SOXL") else ("SOXS", "SOXL")
    for sym in targets:
        _EXIT_SNAPSHOT_CACHE[sym] = _default_exit_snapshot()


def _extract_order_exec_facts(od: dict, safety_cap_price: float = 0.0) -> tuple[float, int, str]:
    """
    NEW (API.txt 명세 & Case 19, 49, 76 완벽 준수):
    단일 주문 객체(Order)에서 실제 체결 단가(filled_price), 실제 체결 수량(filled_qty), 상태(status)를 추출합니다.
    - `status` 가 REPLACED, CANCELED, REJECTED, PENDING 등 미체결/대체 상태일 때
      원주문의 단순 지정가(`price`, 예: +5.00% 상단 안전 캡 $47.98)를 체결가로 읽는 행위를 100% 금지합니다!
    - 오직 `execution.averageFilledPrice` (실제 평균 체결가)가 0보다 클 때만 체결가로 인정합니다.
    """
    if not isinstance(od, dict):
        return 0.0, 0, ""

    status = str(od.get("status") or "").upper()
    exec_obj = od.get("execution") if isinstance(od.get("execution"), dict) else {}

    raw_fq = exec_obj.get("filledQuantity")
    if raw_fq is None:
        raw_fq = od.get("filledQuantity", 0)
    try:
        fq = int(math.floor(float(raw_fq or 0.0)))
    except (ValueError, TypeError):
        fq = 0

    raw_fp = exec_obj.get("averageFilledPrice")
    if raw_fp is None:
        raw_fp = od.get("averageFilledPrice") or od.get("filledPrice")
    try:
        fp = float(raw_fp or 0.0)
    except (ValueError, TypeError):
        fp = 0.0

    # 만약 status == "FILLED" 인데도 averageFilledPrice 가 잠시 비어있는 경우에만
    # 주문 price 를 확인하되, 상단 안전 캡 가격(safety_cap_price)과 동일한 REPLACED 원주문 가격은 제외!
    if fp <= 0.0 and status == "FILLED":
        try:
            cand_p = float(od.get("price") or 0.0)
            if cand_p > 0.0 and (safety_cap_price <= 0.0 or abs(cand_p - safety_cap_price) >= 0.015):
                fp = cand_p
        except (ValueError, TypeError):
            pass

    if status == "FILLED" and fq <= 0:
        try:
            fq = int(math.floor(float(od.get("quantity") or 0.0)))
        except (ValueError, TypeError):
            fq = 0

    return fp, fq, status


# =====================================================================
# 1. 04:07 ~ 08:14 EST 3차 손절 후 최저점 역트레일링 +0.30% 양봉 반등 재진입 (🪤 RE_BOT_TRAIL)
# =====================================================================
async def execute_post_stop_bottom_rebound_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    buy_order_id: str,
    current_price: float,
    budget: float,
    rsi_val: float,
    opp_rsi_val: float = 0.0,
    **kwargs,
) -> bool:
    """
    초과 Case 87:
    04:07 ~ 08:14 EST 프리장 골든타임 중 NQ<2.0% 내 3차 실시간 QQQ 역행 손절 직후,
    손절가(last_stop_price) 아래 최저점(post_stop_low)을 역트레일링 추적하다가
    신저가 경신이 멈추고 최저점 대비 +0.30% 이상 양봉 반등(RSI <= 38.0) 시 즉시 현재가 LIMIT 으로 재진입합니다.
    """
    if not is_active or int(holdings_qty) > 0 or bool(buy_order_id):
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        if _BOTTOM_TRACK_CACHE.get("active"):
            _reset_bottom_track_state()
        return False

    bt_st = get_bottom_track_status(symbol)
    if not bt_st.get("active", False):
        return False

    stopped_sym = str(bt_st.get("stopped_sym") or _BOTTOM_TRACK_CACHE.get("stopped_sym") or "").upper()
    if stopped_sym != symbol:
        return False

    live_p = float(current_price or 0.0)
    if live_p <= 0.0:
        return False

    _, nq_high, nq_low = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if (nq_high > 0.0 and nq_low > 0.0) else 0.0
    if nq_amp >= 2.0:
        _reset_bottom_track_state()
        await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
        return False

    last_stop_p = float(bt_st.get("last_stop_price") or bt_st.get("stop_price") or 0.0)
    post_low_p = float(bt_st.get("post_stop_low") or bt_st.get("lowest_price") or last_stop_p)
    if last_stop_p <= 0.0:
        _reset_bottom_track_state()
        return False

    # 1) 칼날 회피: 신저가를 경신하는 동안에는 매수를 100% 보류하고 최저점만 계속 낮춤
    if live_p < post_low_p:
        new_low = round(live_p, 4)
        rebound_p = math.ceil(new_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
        _BOTTOM_TRACK_CACHE["post_stop_low"] = new_low
        _BOTTOM_TRACK_CACHE["lowest_price"] = new_low
        _BOTTOM_TRACK_CACHE["low_price"] = new_low
        _BOTTOM_TRACK_CACHE["rebound_price"] = rebound_p
        _BOTTOM_TRACK_CACHE["trigger_price"] = rebound_p
        if isinstance(_BOTTOM_TRACK_CACHE.get(symbol), dict):
            _BOTTOM_TRACK_CACHE[symbol].update({
                "post_stop_low": new_low,
                "lowest_price": new_low,
                "low_price": new_low,
                "rebound_price": rebound_p,
                "trigger_price": rebound_p,
            })
        return False

    # 반드시 손절가보다 낮은 최저점을 형성한 뒤 반등해야 함
    if post_low_p >= last_stop_p:
        return False

    rebound_trig_p = math.ceil(post_low_p * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    if live_p < rebound_trig_p or live_p > last_stop_p:
        return False

    if not (0.0 < float(rsi_val or 0.0) <= BOTTOM_MAX_RSI_CAP):
        return False

    # 실시간 양봉 전환 확인 (일반 1분봉 양봉 또는 하이킨 아시 양봉)
    ewm_st = ((ctx.shared_rsi_state or {}).get(symbol) or {}).get("ewm_state") or {}
    cur_o = float(ewm_st.get("cur_bar_open", 0.0) or 0.0)
    is_ha_bull = bool(ewm_st.get("live_ha_bull", ewm_st.get("ha_bull", False)))
    is_bar_bull = bool(cur_o > 0.0 and live_p > cur_o)
    if not (is_bar_bull or is_ha_bull):
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False):
            return False

        # 초과 Case 71: 양쪽 실잔고 및 미체결 주문 이중 교차 검증
        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        if int((ctx.shared_holdings or {}).get("SOXS", 0)) > 0 or int((ctx.shared_holdings or {}).get("SOXL", 0)) > 0:
            return False
        if await AssassinLedger.get_buy_order_id(opp_sym):
            return False

        ctx.in_memory_ordering_lock[symbol] = True
        try:
            order_price = math.floor(live_p * 100.0) / 100.0
            if order_price <= 0.0:
                return False

            usd_bp = await ctx.client.get_usd_buying_power()
            eff_budget = min(float(budget or 0.0), usd_bp * 0.995) if usd_bp > 0.0 else float(budget or 0.0)
            target_qty = int(math.floor(eff_budget / order_price))
            if target_qty <= 0:
                await _send_strike_hold_alert_once(
                    ctx,
                    symbol,
                    f"{current_session_id}_rebot_zero",
                    (
                        f"⚠️ <b>[최저점 반등 재진입 보류 — 수량 0주]</b>\n"
                        f"▫️ 종목: <b>{symbol}</b> · 현재가: ${order_price:.2f} · 가용예산: ${eff_budget:.2f}"
                    ),
                )
                return False

            ref_qqq, ref_label = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True,
                realtime_probing=True,
            )
            _, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data(force_refresh=False)
            nq_amp_now = ((nq_h_now - nq_l_now) / nq_l_now * 100.0) if (nq_h_now > 0.0 and nq_l_now > 0.0) else nq_amp
            if nq_amp_now >= 2.0:
                _reset_bottom_track_state()
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                return False

            cid = f"rebot-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
            ctx.idempotency_keys[symbol]["BUY"] = cid

            ord_res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                quantity=target_qty,
                price=order_price,
                order_type="LIMIT",
                time_in_force="DAY",
                client_order_id=cid,
            )
            new_buy_oid = str((ord_res or {}).get("orderId") or "")
            if not new_buy_oid:
                return False

            _reset_bottom_track_state()
            now_str = now_est.strftime("%Y-%m-%d %H:%M:%S")
            await AssassinLedger.save_state(
                symbol,
                buy_order_id=new_buy_oid,
                is_session_done=False,
                entry_session="PRE_SNIPER_REARM",
                entry_time=now_str,
                entry_trend=("SHORT" if symbol == "SOXS" else "LONG"),
                nq_entry_price=float(ref_qqq or 0.0),
                nq_entry_amp=float(nq_amp_now or 0.0),
                last_session_id=current_session_id,
                whipsaw_target_price=0.0,
            )

            rebound_gain_pct = ((live_p - post_low_p) / post_low_p * 100.0) if post_low_p > 0.0 else 0.0
            await _send_tg_safe(
                ctx,
                (
                    f"🪤 <b>[최저점 역트레일링 +0.30% 양봉 반등 재진입 발사!]</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> ({target_qty}주 · LIMIT <b>${order_price:.2f}</b>)\n"
                    f"▫️ 직전 손절가: ${last_stop_p:.2f} ➡️ 경유 최저점: <b>${post_low_p:.2f}</b> "
                    f"➡️ 반등: <b>${live_p:.2f} (+{rebound_gain_pct:.2f}%)</b>\n"
                    f"▫️ RSI(1m): <b>{rsi_val:.1f}</b> · 3차 기준({ref_label}): <b>{ref_qqq:.2f}</b>"
                ),
            )
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
            return True
        except Exception as e:
            ctx.idempotency_keys[symbol]["BUY"] = None
            print(f"🚨 [최저점 반등 재진입 방어 {symbol}] {e}", flush=True)
            return False
        finally:
            ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 2. 04:07 ~ 08:14 EST 프리장 골든타임 본진 타격 (선택지 B: 대칭 클라이맥스 + 07:00~ 추세눌림목)
# =====================================================================
async def execute_premarket_rsi_sniper_strike_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    current_price: float,
    budget: float,
    target_profit_rate: float,
    rsi_val: float,
    opp_rsi_val: float,
    **kwargs,
) -> bool:
    """
    초과 Case 54, 59, 71, 81, 83, 84, 97:
    프리장 초중반 골든타임(04:07 ~ 08:14 EST) 본진 스나이퍼 타격:
    - [게이트 A : 04:07 ~ 08:14 EST 상시] 양방향 대칭 클라이맥스 (매수종목 RSI <= 30.0 AND 반대종목 RSI >= 79.9)
      ➔ `entry_session = "PRE_SNIPER"`
    - [게이트 B : 07:00 ~ 08:14 EST 전용] 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 주도주(`leader_sym`)가
      조정을 받아 `0.0 < RSI <= 35.0` 에 도달 (`pb_rearm_ready == True`)
      ➔ `entry_session = "PRE_PULLBACK"`
    """
    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return False

    live_p = float(current_price or 0.0)
    live_r = float(rsi_val or 0.0)
    opp_r = float(opp_rsi_val or 0.0)
    if live_p <= 0.0 or live_r <= 0.0:
        return False

    # 당일 04:07 EST 이후 첫 RSI>=79.9 상승 주도주 상태 동기화
    leader_info = update_premarket_trend_leader(
        symbol=symbol,
        live_rsi_val=live_r,
        now_est=now_est,
        shared_rsi_state=ctx.shared_rsi_state,
    )
    leader_sym = str(leader_info.get("leader_sym") or "").upper()
    pb_rearm_ready = bool(leader_info.get("pb_rearm_ready", True))

    # [게이트 A] 04:07 ~ 08:14 EST 상시 양방향 대칭 클라이맥스 (RSI <= 30.0 & 반대 RSI >= 79.9)
    is_gate_a_dual = bool(
        (0.0 < live_r <= PRE_SNIPER_RSI_THRESHOLD)
        and (opp_r >= OPP_CLIMAX_RSI_THRESHOLD)
    )

    # [게이트 B] 07:00 ~ 08:14 EST 후반 황금 구간 전용 선행 상승 주도주(leader_sym) RSI <= 35.0 추세눌림목
    is_gate_b_pullback = bool(
        (700 <= est_hm <= 814)
        and (leader_sym == symbol)
        and pb_rearm_ready
        and (0.0 < live_r <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
    )

    if not (is_gate_a_dual or is_gate_b_pullback):
        return False

    raw_st = await AssassinLedger.get_raw_dict(symbol)
    if not bool(raw_st.get("is_active", True)):
        return False

    # 초과 Case 52: 04:07 ~ 08:14 EST 골든타임 내 미보유·무주문·비매크로차단 시 잠금 자가 치유
    es_cur = str(raw_st.get("entry_session") or "")
    if bool(raw_st.get("is_session_done", False)):
        if es_cur == "CANCELED_MACRO":
            return False
        _, nq_h_chk, nq_l_chk = await MacroDataCache.get_cached_nq_data()
        nq_amp_chk = ((nq_h_chk - nq_l_chk) / nq_l_chk * 100.0) if (nq_h_chk > 0.0 and nq_l_chk > 0.0) else 0.0
        if nq_amp_chk >= 2.0:
            return False
        await AssassinLedger.save_state(symbol, is_session_done=False, entry_session="")

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False):
            return False

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        # 초과 Case 71: 양쪽 종목 실잔고 및 미체결 주문 배타적 검증
        if int((ctx.shared_holdings or {}).get("SOXS", 0)) > 0 or int((ctx.shared_holdings or {}).get("SOXL", 0)) > 0:
            return False
        if await AssassinLedger.get_buy_order_id("SOXS") or await AssassinLedger.get_buy_order_id("SOXL"):
            return False

        # 타격 직전 NQ=F 진폭 강제 갱신 및 1차 과열 방어선(>= 2.0%) 검증
        _, nq_high, nq_low = await MacroDataCache.get_cached_nq_data(force_refresh=True)
        nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if (nq_high > 0.0 and nq_low > 0.0) else 0.0
        if nq_amp >= 2.0:
            await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
            _reset_bottom_track_state()
            _reset_pre_leader_state()
            await _send_strike_hold_alert_once(
                ctx,
                symbol,
                f"{current_session_id}_macro_lock",
                (
                    f"🛑 <b>[1차 매크로 과열 방어 — 프리장 본진 진입 차단]</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> · NQ 당일 진폭: <b>{nq_amp:.2f}% (≥2.0%)</b>"
                ),
            )
            return False

        # 거래소 OPEN 주문 실물 대조 (이중 타격 원천 차단)
        try:
            for chk_sym in ("SOXS", "SOXL"):
                open_ords = await ctx.client.get_orders(status="OPEN", symbol=chk_sym)
                if isinstance(open_ords, list) and len(open_ords) > 0:
                    return False
        except Exception:
            pass

        ctx.in_memory_ordering_lock[symbol] = True
        try:
            cur_p_floored = math.floor(live_p * 100.0) / 100.0
            slip_disc = max(cur_p_floored * FIRST_TRAP_SLIPPAGE_PCT, FIRST_TRAP_MIN_DISC_USD)
            trap_buy_price = math.floor((cur_p_floored - slip_disc) * 100.0) / 100.0
            if trap_buy_price <= 0.0:
                trap_buy_price = cur_p_floored

            # 초과 Case 65: 1차 슬리피지 덫 발사 시 향후 현재가 재장전 대비 0.990 안전 버퍼 적용
            usd_bp = await ctx.client.get_usd_buying_power()
            eff_budget = min(float(budget or 0.0), usd_bp * 0.990) if usd_bp > 0.0 else float(budget or 0.0)
            denom_p = max(cur_p_floored, trap_buy_price)
            target_qty = int(math.floor(eff_budget / denom_p)) if denom_p > 0.0 else 0

            if target_qty <= 0:
                await _send_strike_hold_alert_once(
                    ctx,
                    symbol,
                    f"{current_session_id}_zero_qty",
                    (
                        f"⚠️ <b>[프리장 스나이퍼 타격 보류 — 주문 수량 0주]</b>\n"
                        f"▫️ 종목: <b>{symbol}</b> · 현재가: ${cur_p_floored:.2f} · 가용예산: ${eff_budget:.2f}"
                    ),
                )
                return False

            ref_qqq, ref_label = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True,
                realtime_probing=True,
            )

            chosen_session = "PRE_PULLBACK" if (is_gate_b_pullback and not is_gate_a_dual) else "PRE_SNIPER"
            cid = f"pre1-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
            ctx.idempotency_keys[symbol]["BUY"] = cid

            ord_res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                quantity=target_qty,
                price=trap_buy_price,
                order_type="LIMIT",
                time_in_force="DAY",
                client_order_id=cid,
            )
            buy_oid = str((ord_res or {}).get("orderId") or "")
            if not buy_oid:
                return False

            if chosen_session == "PRE_PULLBACK":
                mark_pre_pullback_dispatched(symbol)

            now_str = now_est.strftime("%Y-%m-%d %H:%M:%S")
            await AssassinLedger.save_state(
                symbol,
                buy_order_id=buy_oid,
                is_session_done=False,
                entry_session=chosen_session,
                entry_time=now_str,
                entry_trend=("SHORT" if symbol == "SOXS" else "LONG"),
                nq_entry_price=float(ref_qqq or 0.0),
                nq_entry_amp=float(nq_amp or 0.0),
                last_session_id=current_session_id,
                whipsaw_target_price=0.0,
            )

            if chosen_session == "PRE_PULLBACK":
                strike_badge = "👑 [07:00~08:14 선행 상승주도주 추세눌림목(RSI≤35) 1차 매수 덫 투하!]"
                gate_desc = f"선행 상승주도주(👑{leader_sym}) RSI {live_r:.1f} ≤ 35.0 · 30초 타임쉴드(RSI>35 반등 시 즉시 정정)"
            else:
                strike_badge = "🪤 [04:07~08:14 양방향 대칭 클라이맥스 1차 매수 덫 투하!]"
                gate_desc = f"{symbol} RSI {live_r:.1f} ≤ 30.0 & {opp_sym} RSI {opp_r:.1f} ≥ 79.9 · 30초 타임쉴드(RSI>30 반등 시 즉시 정정)"

            await _send_tg_safe(
                ctx,
                (
                    f"<b>{html.escape(strike_badge)}</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> ({target_qty}주) · 현재가: ${cur_p_floored:.2f} ➡️ 1차 덫: <b>${trap_buy_price:.2f} (-0.18%)</b>\n"
                    f"▫️ 타점: {html.escape(gate_desc)}\n"
                    f"▫️ 3차 기준({ref_label}): <b>{ref_qqq:.2f}</b> · NQ 진폭: <b>{nq_amp:.2f}%</b>"
                ),
            )
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
            return True
        except Exception as e:
            ctx.idempotency_keys[symbol]["BUY"] = None
            print(f"🚨 [프리장 1차 매수 덫 발사 방어 {symbol}] {e}", flush=True)
            return False
        finally:
            ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 3. 조건부 30초 타임쉴드 현재가 상향 정정 요격 (rearm_premarket_buy_trap_after_30s_plugin)
# =====================================================================
async def rearm_premarket_buy_trap_after_30s_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    buy_order_id: str,
    entry_session: str,
    entry_time: str,
    budget: float,
    current_price: float,
    rsi_val: float = 0.0,
    **kwargs,
) -> tuple[str, str]:
    """
    초과 Case 15, 59, 60, 69, 97, 100:
    1차 슬리피지(-0.18%) 매수 덫 투하 후:
    - `PRE_SNIPER` 는 `RSI > 30.0` 반등 돌파 또는 30.0초 만료 시,
    - `PRE_PULLBACK` 은 `RSI > 35.0` 반등 돌파 또는 30.0초 만료 시
    타임쉴드를 즉시 해제하고 실시간 현재가(rearm_price)로 상향 정정(modify_order, 실패 시 취소 후 재발사)합니다.
    """
    if not is_active or not buy_order_id or entry_session not in ("PRE_SNIPER", "PRE_PULLBACK"):
        return buy_order_id, entry_session

    elapsed_sec = 0.0
    if entry_time:
        try:
            dt_naive = datetime.strptime(str(entry_time).strip(), "%Y-%m-%d %H:%M:%S")
            dt_est = dt_naive.replace(tzinfo=EST_TZ)
            elapsed_sec = max(0.0, (now_est - dt_est).total_seconds())
        except Exception:
            elapsed_sec = TRAP_SHIELD_WAIT_SEC + 1.0
    else:
        elapsed_sec = TRAP_SHIELD_WAIT_SEC + 1.0

    rebound_rsi_limit = (
        PRE_TREND_PULLBACK_RSI_THRESHOLD if entry_session == "PRE_PULLBACK" else PRE_SNIPER_RSI_THRESHOLD
    )
    is_rsi_rebounded = bool(float(rsi_val or 0.0) > rebound_rsi_limit)
    is_time_expired = bool(elapsed_sec >= TRAP_SHIELD_WAIT_SEC)

    if not (is_rsi_rebounded or is_time_expired):
        return buy_order_id, entry_session

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return buy_order_id, entry_session

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        # 1) 현재 1차 매수 덫 주문 상태 먼저 확인 (이미 전량 체결되었으면 정정 불필요)
        od_info = await ctx.client.get_order(buy_order_id)
        if isinstance(od_info, dict) and not od_info.get("error"):
            st_u = str(od_info.get("status") or "").upper()
            exec_o = od_info.get("execution") if isinstance(od_info.get("execution"), dict) else {}
            fq_int = int(math.floor(float(exec_o.get("filledQuantity") or 0.0)))
            if st_u == "FILLED":
                rearm_sess = "PRE_PULLBACK_REARM" if entry_session == "PRE_PULLBACK" else "PRE_SNIPER_REARM"
                await AssassinLedger.save_state(symbol, entry_session=rearm_sess)
                return buy_order_id, rearm_sess
        else:
            fq_int = 0

        rearm_price = math.floor(float(current_price or 0.0) * 100.0) / 100.0
        if rearm_price <= 0.0:
            return buy_order_id, entry_session

        rearm_sess = "PRE_PULLBACK_REARM" if entry_session == "PRE_PULLBACK" else "PRE_SNIPER_REARM"
        new_buy_oid = ""

        # 2) 부분 체결이 없는 순수 PENDING 상태면 토스 주문 정정 API(modify_order)로 0.1초 상향 정정
        if fq_int <= 0:
            try:
                mod_res = await ctx.client.modify_order(
                    order_id=buy_order_id,
                    price=rearm_price,
                    order_type="LIMIT",
                )
                new_buy_oid = str((mod_res or {}).get("orderId") or buy_order_id)
            except Exception as mod_e:
                err_s = str(mod_e).lower()
                if "already-filled" in err_s:
                    await AssassinLedger.save_state(symbol, entry_session=rearm_sess)
                    return buy_order_id, rearm_sess

        # 3) 정정 실패 또는 부분 체결 시 기존 미체결분 선취소 후 가용 예수금 기준 신규 현재가 LIMIT 발사
        if not new_buy_oid:
            try:
                await ctx.client.cancel_order(buy_order_id)
            except Exception as can_e:
                if "already-filled" in str(can_e).lower():
                    await AssassinLedger.save_state(symbol, entry_session=rearm_sess)
                    return buy_order_id, rearm_sess

            await asyncio.sleep(0.25)
            usd_bp = await ctx.client.get_usd_buying_power()
            eff_budget = min(float(budget or 0.0), usd_bp * 0.995) if usd_bp > 0.0 else float(budget or 0.0)
            rearm_qty = int(math.floor(eff_budget / rearm_price))
            if rearm_qty <= 0:
                await AssassinLedger.save_state(symbol, buy_order_id="", entry_session=rearm_sess)
                return "", rearm_sess

            cid = f"rearm-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
            ctx.idempotency_keys[symbol]["REARM"] = cid
            ord_res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                quantity=rearm_qty,
                price=rearm_price,
                order_type="LIMIT",
                time_in_force="DAY",
                client_order_id=cid,
            )
            new_buy_oid = str((ord_res or {}).get("orderId") or "")

        if new_buy_oid:
            now_str = now_est.strftime("%Y-%m-%d %H:%M:%S")
            await AssassinLedger.save_state(
                symbol,
                buy_order_id=new_buy_oid,
                entry_session=rearm_sess,
                entry_time=now_str,
            )
            trig_why = (
                f"RSI 반등 돌파({rsi_val:.1f} > {rebound_rsi_limit:.1f})"
                if is_rsi_rebounded
                else f"30초 타임쉴드 만료({elapsed_sec:.1f}s)"
            )
            await _send_tg_safe(
                ctx,
                (
                    f"⚡ <b>[조건부 타임쉴드 해제 — 현재가 상향 정정 요격!]</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> · 모드: <b>{rearm_sess}</b>\n"
                    f"▫️ 사유: {html.escape(trig_why)} ➡️ 정정 단가: <b>${rearm_price:.2f}</b>"
                ),
            )
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
            return new_buy_oid, rearm_sess

        return buy_order_id, entry_session
    except Exception as e:
        print(f"🚨 [30초 타임쉴드 정정 방어 {symbol}] {e}", flush=True)
        return buy_order_id, entry_session
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 4. 상단 안전 캡(+5.00% LIMIT) 선장전 & 이원화 하이킨 아시 트레일링 스윕 엔진
#    (기술적 반등 ⚡ OLD_IMM_CB [-0.30%] vs 진성 상승종목/수동매수 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 [-0.20%])
# =====================================================================
async def manage_mechanical_profit_trap_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int,
    avg_price: float,
    current_price: float,
    target_profit_rate: float,
    target_sell_price: float,
    buy_order_id: str,
    sell_order_id: str,
    entry_session: str,
    nq_entry_price: float = 0.0,
    nq_entry_amp: float = 0.0,
    **kwargs,
) -> bool:
    """
    초과 Case 50, 58, 64, 69, 85, 89, 94, 99 & 수동 매수(MANUAL) 100% 진성상승 추세추종 레짐 고정:
    1) 보유 중 매 틱마다 `_EXIT_SNAPSHOT_CACHE[symbol]` 에 실보유 수량(holdings_qty), 평단가(avg_price),
       경유 최고가(peak_price)를 상시 기록하여 청산 직후 `1주 ➔ 4주` 수량 왜곡을 원천 봉쇄합니다.
    2) 진입 성격 판별(`is_trend_pb_regime`):
       - `PRE_PULLBACK`, `PRE_PULLBACK_REARM` 및 **모든 수동 매수(`MANUAL`)** 포지션은 `leader_sym` 여부나
         시간대와 무관하게 **100% 무조건 `🚀 진성상승 추세추종 매도(TREND_PULLBACK)`** 로 고정합니다!
       - 오직 양방향 대칭 클라이맥스 자동 진입(`PRE_SNIPER`, `PRE_SNIPER_REARM`)만 `⚡ 기술적반등 매도(TECH_BOUNCE)` 로 작동합니다.
    3) 스윕 정정(`modify_order`) 시 `[409 already-modified]` 에러가 반환되면 즉시 거래소의 살아있는 `OPEN` 매도 주문을
       스캔하여 최신 `orderId` 로 정정을 완수하고, 2단계 취소 폴백 시 `holdings_qty` 를 명시 전달합니다.
    """
    if not is_active or int(holdings_qty) <= 0 or float(avg_price or 0.0) <= 0.0:
        return False

    live_p = float(current_price or 0.0)
    if live_p <= 0.0:
        live_p = float(avg_price)

    es_clean = str(entry_session or "").strip()
    is_whipsaw_1sh = bool(es_clean == "WHIPSAW_1SH")
    eff_sess = es_clean if es_clean in _AUTO_ENTRY_SESSION_TAGS else "MANUAL"

    # MODIFIED: 수동 매수(MANUAL) 및 추세눌림목(PRE_PULLBACK / PRE_PULLBACK_REARM)은
    #           leader_sym 여부나 시간대와 무관하게 100% 무조건 진성 상승종목 추세추종 레짐(TREND_PULLBACK)으로 고정!
    is_trend_pb_regime = bool(eff_sess in ("PRE_PULLBACK", "PRE_PULLBACK_REARM", "MANUAL"))
    regime_code = "TREND_PULLBACK" if is_trend_pb_regime else "TECH_BOUNCE"

    # 1) 보유 중 매 틱 불변 정산 스냅샷(_EXIT_SNAPSHOT_CACHE) 및 트레일링 런타임 상태 갱신
    snap = _EXIT_SNAPSHOT_CACHE.get(symbol)
    if not isinstance(snap, dict):
        snap = _default_exit_snapshot()
        _EXIT_SNAPSHOT_CACHE[symbol] = snap

    snap["last_holdings_qty"] = int(holdings_qty)
    snap["avg_price"] = float(avg_price)
    if live_p > float(snap.get("peak_price", 0.0) or 0.0):
        snap["peak_price"] = live_p
    if float(snap.get("peak_price", 0.0) or 0.0) < float(avg_price):
        snap["peak_price"] = max(live_p, float(avg_price))
    snap["updated_at"] = time.time()

    tr_state = _TRAIL_RUNTIME_CACHE.get(symbol)
    if not isinstance(tr_state, dict):
        tr_state = {}
        _TRAIL_RUNTIME_CACHE[symbol] = tr_state
    tr_state["last_known_qty"] = int(holdings_qty)
    tr_state["regime"] = regime_code
    tr_state["callback_pct"] = (TREND_PB_CALLBACK_PCT if is_trend_pb_regime else TRAIL_DROP_CALLBACK_PCT)

    # 초과 Case 64: 아직 미체결 매수 주문(분할 체결 중)이 살아 있는지 점검
    if buy_order_id:
        try:
            b_info = await ctx.client.get_order(buy_order_id)
            if isinstance(b_info, dict) and not b_info.get("error"):
                b_st = str(b_info.get("status") or "").upper()
                if b_st in ("PENDING", "PARTIAL_FILLED", "PENDING_REPLACE"):
                    return False
                await AssassinLedger.save_state(symbol, buy_order_id="")
                buy_order_id = ""
        except Exception:
            pass

    arm_price = math.ceil(float(avg_price) * (1.0 + TRAIL_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
    safety_cap_price = (
        arm_price
        if is_whipsaw_1sh
        else (math.ceil(float(avg_price) * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0)
    )
    snap["safety_cap_price"] = safety_cap_price

    # =================================================================
    # [단계 1] 거래소 상단 안전 캡(+5.00% LIMIT, 1주 휩소는 +1.00% LIMIT) 매도 주문 선장전
    # =================================================================
    if not sell_order_id:
        if ctx.in_memory_ordering_lock.get(symbol, False):
            return False

        ctx.in_memory_ordering_lock[symbol] = True
        try:
            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                if isinstance(open_orders, list):
                    for od in open_orders:
                        if isinstance(od, dict) and str(od.get("side") or "").upper() == "SELL" and od.get("orderId"):
                            existing_oid = str(od["orderId"]).strip()
                            existing_q = int(math.floor(float(od.get("quantity") or 0.0)))
                            if existing_q == int(holdings_qty):
                                sell_order_id = existing_oid
                                break
                            else:
                                await ctx.client.cancel_order(existing_oid)
            except Exception:
                pass

            if not sell_order_id:
                sellable_q = await ctx.client.get_sellable_quantity(symbol)
                sellable_int = int(math.floor(float(sellable_q or 0.0)))
                if sellable_int <= 0:
                    return False
                order_qty = min(int(holdings_qty), sellable_int)

                cid = f"cap-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
                ctx.idempotency_keys[symbol]["SELL"] = cid
                ord_res = await ctx.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    quantity=order_qty,
                    price=safety_cap_price,
                    order_type="LIMIT",
                    time_in_force="DAY",
                    client_order_id=cid,
                )
                sell_order_id = str((ord_res or {}).get("orderId") or "")

            if sell_order_id:
                ref_qqq, ref_label = await MacroDataCache.get_stage3_reference_price(
                    force_refresh=False,
                    realtime_probing=True,
                )
                _, nq_h, nq_l = await MacroDataCache.get_cached_nq_data(force_refresh=False)
                cur_nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if (nq_h > 0.0 and nq_l > 0.0) else float(nq_entry_amp or 0.0)

                eff_nq_p = float(ref_qqq) if float(ref_qqq or 0.0) > 0.0 else float(nq_entry_price or 0.0)
                eff_nq_amp = float(nq_entry_amp) if float(nq_entry_amp or 0.0) > 0.0 else cur_nq_amp

                if eff_sess == "MANUAL":
                    mark_manual_stage3_anchored(
                        symbol=symbol,
                        ref_price=eff_nq_p,
                        ref_amp=eff_nq_amp,
                        qty=int(holdings_qty),
                    )

                tr_state["active_sell_oid"] = sell_order_id
                snap["sell_order_id"] = sell_order_id
                snap["orig_cap_order_id"] = sell_order_id
                snap["last_sweep_price"] = 0.0

                await AssassinLedger.save_state(
                    symbol,
                    last_buy_price=float(avg_price),
                    target_sell_price=arm_price,
                    sell_order_id=sell_order_id,
                    buy_order_id="",
                    entry_session=eff_sess,
                    entry_trend=("SHORT" if symbol == "SOXS" else "LONG"),
                    nq_entry_price=eff_nq_p,
                    nq_entry_amp=eff_nq_amp,
                )

                if is_whipsaw_1sh:
                    await _send_tg_safe(
                        ctx,
                        (
                            f"🎯 <b>[1주 휩소 작전 체결 ➡️ +1.00% 익절 매도 덫 장전!]</b>\n"
                            f"▫️ 종목: <b>{symbol}</b> (1주) · 체결 평단: <b>${avg_price:.2f}</b>\n"
                            f"▫️ 목표 매도가(LIMIT): <b>${safety_cap_price:.2f} (+1.00%)</b>"
                        ),
                    )
                else:
                    mode_tag = "🖐️ 수동 스나이퍼 개입" if eff_sess == "MANUAL" else f"🔫 본진 체결({eff_sess})"
                    if is_trend_pb_regime:
                        regime_desc = (
                            "🚀 진성상승 추세추종 (+1.30% 전 양봉홀딩[쿠션+0.75%] · "
                            "음봉/+1.30% 시 +1.00% 잠금 & 고점 대비 -0.20% 타이트 스윕)"
                        )
                    else:
                        regime_desc = (
                            "⚡ 기술적반등 매도 (+1.00% 보존 고정 & 고점 대비 -0.30% 콜백/HA음봉 즉시 스윕)"
                        )
                    dyn_th = max(eff_nq_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT)
                    await _send_tg_safe(
                        ctx,
                        (
                            f"🎯 <b>[상단 안전 캡 선장전 & 이원화 트레일링 결속]</b> {symbol} ({html.escape(mode_tag)})\n"
                            f"▫️ 팩트 평단가: <b>${avg_price:.2f}</b> ({holdings_qty}주) · 상단 안전 캡(LIMIT): <b>${safety_cap_price:.2f} (+5.00%)</b>\n"
                            f"▫️ 트레일링 점화가(+1.00%): <b>${arm_price:.2f}</b> · 매도 레짐: <b>{html.escape(regime_desc)}</b>\n"
                            f"▫️ 3차 역행 방어({ref_label}): 기준가 <b>${eff_nq_p:.2f}</b> (역행 한도 +{dyn_th:.2f}% · 수동/자동 보호 가동)"
                        ),
                    )
                return True
        except Exception as cap_e:
            ctx.idempotency_keys[symbol]["SELL"] = None
            print(f"🚨 [상단 안전 캡 선장전 방어 {symbol}] {cap_e}", flush=True)
            return False
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    if is_whipsaw_1sh:
        return False

    # =================================================================
    # [단계 2] +1.00% 트레일링 점화 판정 및 진입 성격별 레짐(Regime) 작동 (초과 Case 85, 99)
    # =================================================================
    armed = bool(tr_state.get("armed", False))
    cur_hm = now_est.hour * 100 + now_est.minute

    if not armed:
        if live_p < arm_price:
            return False

        armed = True
        peak_price = max(live_p, arm_price)
        cushion_floor = math.ceil(float(avg_price) * (1.0 + TREND_PB_CUSHION_FLOOR_RATE / 100.0) * 100.0) / 100.0
        init_floor = cushion_floor if is_trend_pb_regime else arm_price

        tr_state.update({
            "armed": True,
            "is_armed": True,
            "peak_price": peak_price,
            "max_price": peak_price,
            "arm_price": arm_price,
            "floor_price": init_floor,
            "stop_price": init_floor,
            "callback_pct": (TREND_PB_CALLBACK_PCT if is_trend_pb_regime else TRAIL_DROP_CALLBACK_PCT),
            "armed_at": time.time(),
            "armed_hm": cur_hm,
            "last_mod_price": 0.0,
            "last_known_qty": int(holdings_qty),
            "bear_candle_seen": False,
            "regime": regime_code,
            "active_sell_oid": str(tr_state.get("active_sell_oid") or snap.get("sell_order_id") or sell_order_id),
            "sweep_dispatched_ts": 0.0,
        })
        snap["peak_price"] = max(float(snap.get("peak_price", 0.0) or 0.0), peak_price)

        gain_pct_now = (live_p - avg_price) / avg_price * 100.0
        if is_trend_pb_regime:
            fire_title = f"🔥 🚀 [진성상승 추세추종 트레일링 점화!] 초입 양봉 홀딩 & -0.20% 타이트 스윕 ({symbol})"
        else:
            fire_title = f"🔥 ⚡ [기술적반등 트레일링 점화!] +1.00% 보존 고정 & -0.30% 콜백/HA음봉 스윕 ({symbol})"

        await _send_tg_safe(
            ctx,
            (
                f"<b>{html.escape(fire_title)}</b>\n"
                f"▫️ 평단가: ${avg_price:.2f} ➡️ 점화 현재가: <b>${live_p:.2f} (+{gain_pct_now:.2f}%)</b>\n"
                f"▫️ 현재 스탑선: <b>${init_floor:.2f}</b> · 상단 안전 캡: <b>${safety_cap_price:.2f}</b>\n"
                f"▫️ 조치: 0.4초 고주파 부스터 고정 및 최고가 무제한 추적 개시!"
            ),
        )
        if ctx.wakeup_event:
            ctx.wakeup_event.set()

    # =================================================================
    # [단계 3] 최고가 갱신 및 이원화 매도 믹스(⚡ OLD_IMM_CB vs 🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020) 판정
    # =================================================================
    peak_price = max(float(tr_state.get("peak_price", 0.0) or 0.0), live_p, arm_price)
    tr_state["peak_price"] = peak_price
    tr_state["max_price"] = peak_price
    snap["peak_price"] = max(float(snap.get("peak_price", 0.0) or 0.0), peak_price)

    peak_pct = ((peak_price - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0
    elapsed_arm = max(0.0, time.time() - float(tr_state.get("armed_at", 0.0) or 0.0))
    armed_hm = int(tr_state.get("armed_hm", -1))

    ewm_st = ((ctx.shared_rsi_state or {}).get(symbol) or {}).get("ewm_state") or {}
    cur_bar_open = float(ewm_st.get("cur_bar_open", 0.0) or 0.0)
    live_ha_open = float(ewm_st.get("live_ha_open", ewm_st.get("prev_ha_open", 0.0)) or 0.0)
    live_ha_close = float(ewm_st.get("live_ha_close", ewm_st.get("prev_ha_close", 0.0)) or 0.0)
    is_ha_bull = bool(ewm_st.get("live_ha_bull", ewm_st.get("ha_bull", True)))
    is_ha_bear = bool((live_ha_open > 0.0 and live_ha_close <= live_ha_open) or not is_ha_bull)
    is_bar_bear = bool(cur_bar_open > 0.0 and live_p < cur_bar_open)

    # 초과 Case 89: 토스 Open API QQQ 실시간 10m/3m 변화율 ±0.20% 급등락 진행 중에는 HA음봉 바이패스(🚀 NQ_VEL_BYPASS)
    nq_vel_10m, nq_vel_3m = await MacroDataCache.get_nq_velocity_metrics()
    if symbol == "SOXS":
        nq_vel_bypass = bool(min(nq_vel_10m, nq_vel_3m) <= -NQ_VELOCITY_SURGE_PCT)
    else:
        nq_vel_bypass = bool(max(nq_vel_10m, nq_vel_3m) >= NQ_VELOCITY_SURGE_PCT)

    effective_ha_bear = bool(is_ha_bear and not nq_vel_bypass)

    trigger_sweep = False
    sweep_reason = ""
    mode_badge = ""

    if is_trend_pb_regime:
        # [3단계-B : 진성 상승종목 / 수동 매수 추세추종 매도 모드 (🚀 PEAK_LCK_130 × 📊 BAR_BEAR_020)]
        mode_badge = "🚀진성추세추종(-0.20%)"
        if is_bar_bear or effective_ha_bear:
            tr_state["bear_candle_seen"] = True

        bear_seen = bool(tr_state.get("bear_candle_seen", False))
        cushion_floor = math.ceil(float(avg_price) * (1.0 + TREND_PB_CUSHION_FLOOR_RATE / 100.0) * 100.0) / 100.0

        if peak_pct < TREND_PB_LAUNCH_RATCHET_PCT and not bear_seen and not is_bar_bear and is_ha_bull:
            effective_stop_p = cushion_floor
            tr_state["floor_price"] = cushion_floor
            tr_state["stop_price"] = effective_stop_p
            if live_p <= effective_stop_p:
                trigger_sweep = True
                sweep_reason = f"초입 비상 쿠션(${effective_stop_p:.2f}) 도달"
        else:
            floor_price = arm_price
            raw_cb_stop = math.floor(peak_price * (1.0 - TREND_PB_CALLBACK_PCT / 100.0) * 100.0) / 100.0
            effective_stop_p = max(floor_price, raw_cb_stop)
            tr_state["floor_price"] = floor_price
            tr_state["stop_price"] = effective_stop_p

            if live_p <= effective_stop_p:
                trigger_sweep = True
                sweep_reason = f"고점(${peak_price:.2f}) 대비 -0.20% 타이트 스탑(${effective_stop_p:.2f}) 도달"
            elif effective_ha_bear and (cur_hm != armed_hm or elapsed_arm >= 8.0 or peak_pct >= 1.30):
                trigger_sweep = True
                sweep_reason = f"하이킨 아시 1분봉 음봉 전환(🌑HA음봉 · 경유고점 ${peak_price:.2f})"
    else:
        # [3단계-A : 기술적 반등 매도 모드 (⚡ OLD_IMM_CB)]
        mode_badge = "⚡기술적반등(-0.30%)"
        floor_price = arm_price
        raw_cb_stop = math.floor(peak_price * (1.0 - TRAIL_DROP_CALLBACK_PCT / 100.0) * 100.0) / 100.0
        effective_stop_p = max(floor_price, raw_cb_stop)
        tr_state["floor_price"] = floor_price
        tr_state["stop_price"] = effective_stop_p

        can_check_cb = bool(cur_hm != armed_hm or elapsed_arm >= 8.0 or peak_pct >= 1.15 or live_p < floor_price)
        if can_check_cb and live_p <= effective_stop_p:
            trigger_sweep = True
            sweep_reason = f"고점(${peak_price:.2f}) 대비 콜백 스탑(${effective_stop_p:.2f}) 도달"
        elif effective_ha_bear and (cur_hm != armed_hm or elapsed_arm >= 8.0 or peak_pct >= 1.15):
            trigger_sweep = True
            sweep_reason = f"하이킨 아시 1분봉 음봉 전환(🌑HA음봉 · 경유고점 ${peak_price:.2f})"

    if not trigger_sweep:
        return False

    # 직전 스윕 발사 후 체결 대기 중(3.0초 이내 동일 가격 이상)이면 중복 정정 스킵
    last_disp_ts = float(tr_state.get("sweep_dispatched_ts", 0.0) or 0.0)
    last_mod_p = float(tr_state.get("last_mod_price", 0.0) or 0.0)
    if last_disp_ts > 0.0 and (time.time() - last_disp_ts) < 3.0 and last_mod_p > 0.0 and live_p >= last_mod_p - 0.01:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        best_bid = await ctx.client.get_best_bid_price(symbol, fallback_price=live_p)
        raw_sweep_p = best_bid if best_bid > 0.0 else (math.floor(live_p * 100.0) / 100.0)

        floor_lock_p = float(tr_state.get("floor_price", arm_price) or arm_price)
        if live_p >= arm_price * 0.997:
            sweep_price = max(floor_lock_p, raw_sweep_p)
        else:
            sweep_price = raw_sweep_p
        sweep_price = math.floor(sweep_price * 100.0) / 100.0
        if sweep_price <= 0.0:
            return False

        swept_ok = False
        new_sell_oid = ""
        active_sell_oid = str(
            tr_state.get("active_sell_oid")
            or snap.get("sell_order_id")
            or sell_order_id
            or ""
        ).strip()

        # 1단계: 현재 유효한 active_sell_oid 가격을 sweep_price 로 0.1초 즉시 하향 정정!
        if active_sell_oid:
            try:
                mod_res = await ctx.client.modify_order(
                    order_id=active_sell_oid,
                    price=sweep_price,
                    order_type="LIMIT",
                )
                new_sell_oid = str((mod_res or {}).get("orderId") or active_sell_oid)
                tr_state["active_sell_oid"] = new_sell_oid
                swept_ok = True
            except Exception as mod_e:
                err_s = str(mod_e).lower()
                if "already-filled" in err_s:
                    return True
                # NEW (API.txt 명세): 409 already-modified 발생 시 거래소의 살아있는 OPEN 매도 주문 ID를 즉시 찾아 정정!
                if "already-modified" in err_s or "409" in err_s or "not-found" in err_s or "404" in err_s:
                    try:
                        open_ords = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                        if isinstance(open_ords, list):
                            for od in open_ords:
                                if (
                                    isinstance(od, dict)
                                    and str(od.get("side") or "").upper() == "SELL"
                                    and od.get("orderId")
                                ):
                                    live_oid = str(od["orderId"]).strip()
                                    tr_state["active_sell_oid"] = live_oid
                                    snap["sell_order_id"] = live_oid
                                    try:
                                        od_p = float(od.get("price") or 0.0)
                                    except (ValueError, TypeError):
                                        od_p = 0.0
                                    if od_p > 0.0 and abs(od_p - sweep_price) < 0.005:
                                        new_sell_oid = live_oid
                                        swept_ok = True
                                        break
                                    mod_res2 = await ctx.client.modify_order(
                                        order_id=live_oid,
                                        price=sweep_price,
                                        order_type="LIMIT",
                                    )
                                    new_sell_oid = str((mod_res2 or {}).get("orderId") or live_oid)
                                    tr_state["active_sell_oid"] = new_sell_oid
                                    swept_ok = True
                                    break
                    except Exception as mod_e2:
                        if "already-filled" in str(mod_e2).lower():
                            return True

        # 2단계 폴백: 정정 불가 시 미체결 주문 선취소 + 매도가능수량 폴링 대기 후 신규 LIMIT 발사
        if not swept_ok:
            sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
                ctx=ctx,
                symbol=symbol,
                holdings_qty=max(1, int(holdings_qty or 1)),
                sell_order_id=active_sell_oid,
                buy_order_id=buy_order_id,
            )
            if sellable_q_int <= 0 and surviving_sell_ids:
                for sv_oid in surviving_sell_ids:
                    try:
                        mod_res = await ctx.client.modify_order(
                            order_id=sv_oid,
                            price=sweep_price,
                            order_type="LIMIT",
                        )
                        new_sell_oid = str((mod_res or {}).get("orderId") or sv_oid)
                        tr_state["active_sell_oid"] = new_sell_oid
                        swept_ok = True
                        break
                    except Exception as sv_e:
                        if "already-filled" in str(sv_e).lower():
                            return True

            if not swept_ok:
                target_q = sellable_q_int if sellable_q_int > 0 else max(1, int(holdings_qty or 1))
                if target_q > 0:
                    cid = f"tswp-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
                    ctx.idempotency_keys[symbol]["SELL"] = cid
                    ord_res = await ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        quantity=target_q,
                        price=sweep_price,
                        order_type="LIMIT",
                        time_in_force="DAY",
                        client_order_id=cid,
                    )
                    new_sell_oid = str((ord_res or {}).get("orderId") or "")
                    tr_state["active_sell_oid"] = new_sell_oid
                    swept_ok = bool(new_sell_oid)

        if swept_ok:
            final_oid = str(new_sell_oid or active_sell_oid or sell_order_id)
            tr_state["active_sell_oid"] = final_oid
            tr_state["last_mod_price"] = sweep_price
            tr_state["sweep_dispatched_ts"] = time.time()
            tr_state["last_known_qty"] = int(holdings_qty)

            snap.update({
                "last_holdings_qty": int(holdings_qty),
                "avg_price": float(avg_price),
                "peak_price": float(peak_price),
                "last_sweep_price": float(sweep_price),
                "sell_order_id": final_oid,
                "sweep_mode_label": mode_badge,
                "sweep_reason": sweep_reason,
                "updated_at": time.time(),
            })

            await AssassinLedger.save_state(
                symbol,
                sell_order_id=final_oid,
                target_sell_price=float(sweep_price),
            )

            est_pnl_pct = ((sweep_price - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0
            await _send_tg_safe(
                ctx,
                (
                    f"⚡ <b>[이원화 트레일링 0.1초 익절 스윕 발사!]</b> {symbol} ({html.escape(mode_badge)})\n"
                    f"▫️ 격발 사유: {html.escape(sweep_reason)}\n"
                    f"▫️ 스윕 단가(LIMIT): <b>${sweep_price:.2f}</b> "
                    f"(평단 ${avg_price:.2f} 대비 <b>{est_pnl_pct:+.2f}%</b> · 경유고점 <b>${peak_price:.2f} [+{peak_pct:.2f}%]</b>)"
                ),
            )
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
            return True
        return False
    except Exception as sw_e:
        print(f"🚨 [이원화 트레일링 익절 스윕 방어 {symbol}] {sw_e}", flush=True)
        return False
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


# =====================================================================
# 5. 잔고 0주 복귀 시 무결점 정산 및 복리 시드 동기화 (reconcile_zero_holdings_exit_plugin)
# =====================================================================
async def reconcile_zero_holdings_exit_plugin(
    ctx: SniperPluginContext,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    last_buy_price: float,
    target_sell_price: float,
    buy_order_id: str,
    sell_order_id: str,
    entry_session: str,
    current_price: float,
    budget: float,
    target_profit_rate: float,
    is_compound: bool,
    **kwargs,
) -> bool:
    """
    NEW & MODIFIED (초과 Case 49, 51, 52, 76, 86, 100 완벽 준수):
    1) 초과 Case 100: 매수 주문(`buy_order_id`)이 PENDING / PARTIAL_FILLED 상태이거나,
       방금 `FILLED` (또는 `filledQuantity > 0`) 되었는데 아직 잔고 API 인덱싱 시차(0.1~0.4초)로 인해
       상단 안전 캡 매도 주문(`sell_order_id`)이 장전되기 전(`last_buy_price <= 0.0` 및 `not sell_order_id`)인 경우
       절대로 장부를 초기화하지 않고 `False` 를 반환하여 자동 진입 물량이 `MANUAL` 로 둔갑하는 것을 원천 차단합니다.
    2) 매도 청산 정산 시 `REPLACED` / `CANCELED` 상태인 원주문의 미체결 지정가(`price = $47.98` 상단 안전 캡)를
       체결가로 읽는 오류를 100% 차단하고, 오직 `FILLED` 주문의 `execution.averageFilledPrice` 또는
       `_EXIT_SNAPSHOT_CACHE[symbol]["last_sweep_price"]` ($46.31) 만 사용합니다.
    3) 체결 수량(`filled_qty`) 산출 시 `_EXIT_SNAPSHOT_CACHE[symbol]["last_holdings_qty"]` (`1주`) 및
       `tr_state["last_known_qty"]` 를 최우선 적용하여 `int(budget // entry_price)` (`4주`) 뻥튀기를 영구 소각합니다.
    """
    est_hm = now_est.hour * 100 + now_est.minute
    es_clean = str(entry_session or "").strip()

    # [A] 미체결 매수 주문(buy_order_id) 상태 검증 및 체결 인덱싱 시차 보호 (초과 Case 51, 100)
    if buy_order_id and not sell_order_id and float(last_buy_price or 0.0) <= 0.0:
        try:
            b_od = await ctx.client.get_order(buy_order_id)
            if isinstance(b_od, dict) and not b_od.get("error"):
                _, b_fq, b_st = _extract_order_exec_facts(b_od)
                if b_st in ("PENDING", "PARTIAL_FILLED", "PENDING_REPLACE", "PENDING_CANCEL", "FILLED") or b_fq > 0:
                    return False
                if b_st in ("CANCELED", "REJECTED"):
                    await AssassinLedger.save_state(
                        symbol,
                        buy_order_id="",
                        entry_session=("CANCELED_MACRO" if es_clean == "CANCELED_MACRO" else ""),
                        whipsaw_target_price=0.0,
                    )
                    return True
            else:
                return False
        except Exception:
            return False

    if not sell_order_id and float(last_buy_price or 0.0) <= 0.0 and float(target_sell_price or 0.0) <= 0.0:
        return False

    # [B] 매도 청산 팩트(체결 단가 exit_price · 체결 수량 filled_qty · 경유 고점 peak_price) 정밀 추출
    snap = _EXIT_SNAPSHOT_CACHE.get(symbol) or _default_exit_snapshot()
    tr_state = get_trailing_runtime_status(symbol)

    entry_price = float(last_buy_price or 0.0) if float(last_buy_price or 0.0) > 0.0 else float(snap.get("avg_price", 0.0) or 0.0)
    safety_cap_p = float(snap.get("safety_cap_price", 0.0) or 0.0)
    if safety_cap_p <= 0.0 and entry_price > 0.0 and es_clean != "WHIPSAW_1SH":
        safety_cap_p = math.ceil(entry_price * (1.0 + TRAIL_SAFETY_CAP_RATE / 100.0) * 100.0) / 100.0

    target_sell_oid = str(
        tr_state.get("active_sell_oid")
        or snap.get("sell_order_id")
        or sell_order_id
        or ""
    ).strip()

    exit_price = 0.0
    filled_qty = 0
    order_spec_qty = 0

    for attempt in range(2):
        for cand_oid in (target_sell_oid, sell_order_id):
            if not cand_oid:
                continue
            try:
                s_od = await ctx.client.get_order(cand_oid)
                if isinstance(s_od, dict) and not s_od.get("error"):
                    fp, fq, s_st = _extract_order_exec_facts(s_od, safety_cap_price=safety_cap_p)
                    if s_st in ("PENDING", "PARTIAL_FILLED", "PENDING_REPLACE"):
                        return False
                    try:
                        q_spec = int(math.floor(float(s_od.get("quantity") or 0.0)))
                        if q_spec > 0:
                            order_spec_qty = q_spec
                    except Exception:
                        pass
                    if fp > 0.0:
                        exit_price = fp
                    if fq > 0:
                        filled_qty = fq
                    if exit_price > 0.0 and filled_qty > 0:
                        break
            except Exception:
                pass

        if exit_price > 0.0 and filled_qty > 0:
            break

        try:
            closed_orders = await ctx.client.get_orders(status="CLOSED", symbol=symbol, limit=15)
            if isinstance(closed_orders, list):
                for cod in closed_orders:
                    if not isinstance(cod, dict):
                        continue
                    if str(cod.get("side") or "").upper() != "SELL":
                        continue
                    fp, fq, c_st = _extract_order_exec_facts(cod, safety_cap_price=safety_cap_p)
                    if c_st == "FILLED" and fp > 0.0 and fq > 0:
                        exit_price = fp
                        filled_qty = fq
                        break
        except Exception:
            pass

        if exit_price > 0.0 and filled_qty > 0:
            break
        if attempt == 0:
            await asyncio.sleep(0.35)

    snap_sweep_p = float(snap.get("last_sweep_price", 0.0) or 0.0)
    tr_mod_p = float(tr_state.get("last_mod_price", 0.0) or 0.0)
    if exit_price <= 0.0:
        if snap_sweep_p > 0.0:
            exit_price = snap_sweep_p
        elif tr_mod_p > 0.0:
            exit_price = tr_mod_p
        elif float(current_price or 0.0) > 0.0:
            exit_price = float(current_price)
        elif float(target_sell_price or 0.0) > 0.0 and abs(float(target_sell_price) - safety_cap_p) >= 0.015:
            exit_price = float(target_sell_price)
        else:
            exit_price = entry_price

    if filled_qty <= 0:
        if int(snap.get("last_holdings_qty", 0) or 0) > 0:
            filled_qty = int(snap["last_holdings_qty"])
        elif int(tr_state.get("last_known_qty", 0) or 0) > 0:
            filled_qty = int(tr_state["last_known_qty"])
        elif order_spec_qty > 0:
            filled_qty = order_spec_qty
        elif es_clean == "WHIPSAW_1SH":
            filled_qty = 1
        else:
            filled_qty = 1

    recorded_peak = max(
        float(snap.get("peak_price", 0.0) or 0.0),
        float(tr_state.get("peak_price", 0.0) or 0.0),
    )
    peak_price = max(recorded_peak, entry_price, exit_price)

    # [C] 실제 발생 순수익금(Realized Net PnL USD · 왕복 수수료 0.14% 차감) 및 통합 복리 정산
    if entry_price > 0.0 and exit_price > 0.0 and filled_qty > 0:
        new_budget, net_delta_usd, net_pnl_pct = calc_compound_next_budget(
            current_budget=float(budget or 200.0),
            entry_price=entry_price,
            exit_price=exit_price,
            filled_qty=filled_qty,
            is_compound=bool(is_compound),
        )
        gross_pnl_usd = round((exit_price - entry_price) * filled_qty, 2)
        gross_pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0
        fee_usd = round((entry_price + exit_price) * filled_qty * 0.0007, 2)
        peak_pnl_pct = ((peak_price - entry_price) / entry_price) * 100.0
    else:
        new_budget = float(budget or 200.0)
        net_delta_usd = 0.0
        net_pnl_pct = 0.0
        gross_pnl_usd = 0.0
        gross_pnl_pct = 0.0
        fee_usd = 0.0
        peak_pnl_pct = 0.0

    # [D] 프리장 골든타임(04:00 ~ 08:14 EST) 무한 재출동 vs 08:15 이후/과열장 마감 분기
    _, nq_h, nq_l = await MacroDataCache.get_cached_nq_data(force_refresh=False)
    nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if (nq_h > 0.0 and nq_l > 0.0) else 0.0
    is_overheated = bool(nq_amp >= 2.0)

    can_reenter_golden = bool((400 <= est_hm <= 814) and not is_overheated)
    next_session_done = not can_reenter_golden
    next_entry_session = "CANCELED_MACRO" if is_overheated else ""

    await AssassinLedger.save_state(
        symbol,
        last_buy_price=0.0,
        budget=new_budget,
        target_sell_price=0.0,
        buy_order_id="",
        sell_order_id="",
        is_session_done=next_session_done,
        entry_session=next_entry_session,
        entry_time="",
        nq_entry_price=0.0,
        nq_entry_amp=0.0,
        whipsaw_target_price=0.0,
    )

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    opp_qty = int((ctx.shared_holdings or {}).get(opp_sym, 0))
    opp_raw = await AssassinLedger.get_raw_dict(opp_sym)
    opp_es = str(opp_raw.get("entry_session") or "")
    if opp_qty <= 0 and opp_es != "MANUAL":
        await AssassinLedger.save_state(
            opp_sym,
            budget=new_budget,
            is_session_done=next_session_done,
            entry_session=(next_entry_session if is_overheated else opp_es),
        )
    else:
        await AssassinLedger.save_state(opp_sym, budget=new_budget)

    _reset_trailing_runtime_state(symbol)
    _reset_manual_stage3_anchor(symbol)
    _reset_exit_snapshot(symbol)

    # [E] 텔레그램 정산 완료 보고 (Case 13, 17, 49, 76)
    if is_active and entry_price > 0.0:
        if es_clean in ("PRE_PULLBACK", "PRE_PULLBACK_REARM"):
            exit_tag = "🚀진성추세스윕"
        elif es_clean == "WHIPSAW_1SH":
            exit_tag = "🌪️1주휩소작전"
        elif es_clean == "VWAP_ESCAPE":
            exit_tag = "🛟VWAP탈출"
        elif es_clean == "MANUAL":
            exit_tag = "🎯수동진성추세스윕"
        else:
            exit_tag = "⚡기술적반등스윕"

        title_icon = "💰" if net_delta_usd >= 0.0 else "🛡️"
        title_word = "익절 정산 완료" if net_delta_usd >= 0.0 else "청산 정산 완료"
        mode_str = "📈 통합복리" if is_compound else "💵 고정단리"
        follow_str = (
            "🟢 프리장 골든타임 무한 재출동 감시 복귀"
            if can_reenter_golden
            else ("🛑 NQ 과열(≥2.0%) 당일 매매 마감" if is_overheated else "🔒 금일 신규 진입 마감 (대기 모드 전환)")
        )

        await _send_tg_safe(
            ctx,
            (
                f"{title_icon} <b>[듀얼 스나이퍼 {title_word} · {exit_tag}]</b> {symbol}\n"
                f"▫️ 체결 궤적: 평단 ${entry_price:.2f} ➡️ 경유고점 <b>${peak_price:.2f} ({peak_pnl_pct:+.2f}%)</b> "
                f"➡️ 청산 <b>${exit_price:.2f} ({filled_qty}주)</b>\n"
                f"▫️ 실현 손익: 세전 {gross_pnl_pct:+.2f}% ({gross_pnl_usd:+.2f}$) · "
                f"수수료 -{fee_usd:.2f}$ · 순수익 <b>{net_pnl_pct:+.2f}% ({net_delta_usd:+.2f}$)</b>\n"
                f"▫️ 차기 시드({mode_str}): ${float(budget or 200.0):,.2f} ➡️ <b>${new_budget:,.2f}</b>\n"
                f"▫️ 후속 상태: {follow_str}"
            ),
        )

    if ctx.wakeup_event:
        ctx.wakeup_event.set()
    return True


def __getattr__(name: str):
    """
    PEP 562 모듈 레벨 안전 폴백:
    외부 모듈(tg_router, tg_ui_plugin, main_loops_plugin 등)이 sniper_plugin 에서
    whipsaw_plugin 또는 defense_plugin 의 심볼을 교차 임포트할 때 ImportError 가 절대 발생하지 않도록 방어합니다.
    """
    if hasattr(_whipsaw_mod, name):
        return getattr(_whipsaw_mod, name)
    import defense_plugin as _def_mod
    if hasattr(_def_mod, name):
        return getattr(_def_mod, name)

    def _safe_fallback(*args, **kwargs):
        return {"active": False}

    return _safe_fallback
