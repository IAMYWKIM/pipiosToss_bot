# =====================================================================
# FILE: sniper_plugin.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       선택지 B (👑 HYB_35_1ST) —
#       [1] 04:07 ~ 08:14 EST 양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=79.9[80.0 클라이맥스 인지] · 🪤 DUAL80_TRAP) 상시 타격 +
#       [2] 04:07 EST 이후 첫 RSI>=79.9 달성 상승 추세 종목(FIRST_799 · leader_sym) 자동 각인 후
#           07:00 ~ 08:14 EST 후반 황금 구간에 해당 상승 종목이 RSI<=35.0 도달 시 추세추종 저가 매수(🚀 PB_0700_35)
#       무한 출동 듀얼 스나이퍼(SOXS/SOXL), 3차 손절 후 최저점 역트레일링 +0.30% 반등 재진입(🪤 RE_BOT_TRAIL),
#       조건부 30초 타임쉴드(RSI>30/35) 및 sniper_exit_plugin / defense_plugin / whipsaw_plugin 통합 리익스포트 코어
#       (sniper_plugin.py 2/2 분할본)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 청산·복리정산·이원화 트레일링 스윕 코어를 sniper_exit_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 프리장 본진 타격 및 무한 재출동 허용 윈도우를 04:07 ~ 08:14 EST 초중반 황금 구간으로 하드 락온 (Case 52, 53, 84)
# MODIFIED: 초과 Case 92 & Case 94 — 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)은 나스닥 100 선물지수(NQ=F) 진폭을 기준으로 산출하고,
#           해당 임계치를 적용할 진입 기준가(nq_entry_price)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온
# MODIFIED: 초과 Case 97 — 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 종목(leader_sym) 불변 각인 및
#           07:00 ~ 08:14 EST 해당 상승 종목 RSI <= 35.0 도달 시 추세추종 저가 매수(PRE_PULLBACK) + 조건부 30초 타임쉴드(RSI > 35.0 반등 정정) 결합

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
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
)
from whipsaw_plugin import (
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_TREND_BOOSTER_RSI,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    BUY_TRAP_SLIPPAGE_PCT,
    BUY_TRAP_MIN_DISC_USD,
    BUY_TRAP_REARM_WAIT_SEC,
    WHIPSAW_BUY_REVERSE_FACTOR,
    WHIPSAW_SELL_PROFIT_FACTOR,
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin,
    execute_whipsaw_vwap_exposure_plugin,
    enforce_0407_whipsaw_cutoff_plugin,
)
from defense_plugin import (
    TRAIL_ARM_PROFIT_RATE,
    TRAIL_FLOOR_LOCK_RATE,
    TRAIL_DROP_CALLBACK_PCT,
    TRAIL_SAFETY_CAP_RATE,
    NQ_VELOCITY_SURGE_PCT,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    BOTTOM_REBOUND_PCT,
    BOTTOM_MAX_RSI_CAP,
    _STRIKE_ALERT_CACHE,
    _TRAIL_RUNTIME_CACHE,
    _BOTTOM_TRACK_CACHE,
    _PRE_LEADER_CACHE,
    _AUTO_ENTRY_SESSION_TAGS,
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
    execute_late_regular_vwap_escape_plugin,
    execute_moc_zero_overnight_plugin,
    execute_macro_defense_plugin,
    enforce_0930_regular_cutoff_plugin,
    enforce_0815_pre_macro_cutoff_plugin,
)
from sniper_exit_plugin import (
    reconcile_zero_holdings_exit_plugin,
    manage_mechanical_profit_trap_plugin,
)

# 하위 모듈 리익스포트 호환 가드 (ImportError 원천 차단)
try:
    from defense_plugin import (
        _reset_swing_10m_state,
        get_swing_10m_status,
    )
except ImportError:
    def _reset_swing_10m_state():
        pass

    def get_swing_10m_status(symbol: str = None) -> dict:
        return {"pivot_ready": False, "swing_drop_pct": 0.0}

EST_TZ = ZoneInfo("America/New_York")


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


async def execute_rsi_monitoring_plugin(
    ctx: SniperPluginContext, symbol: str, rsi_val: float, vwap_val: float, current_price: float
):
    if ctx.shared_rsi_state is None:
        return

    state = ctx.shared_rsi_state.get(symbol)
    if not state:
        return

    state["value"] = rsi_val
    state["vwap"] = vwap_val

    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute

    # 트레일링 점화 상태이거나 3차 손절 후 최저점 역트레일링 추적 중이거나
    # 15:00 ~ 15:58 EST 보유 물량이 VWAP 가격에 도달했을 때 즉시 메인 루프 기상 (Case 25, 85, 87, 98)
    if (
        _TRAIL_RUNTIME_CACHE.get(symbol, {}).get("armed", False)
        or (_BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol)
        or (
            (1500 <= est_hm <= 1558)
            and int(ctx.shared_holdings.get(symbol, 0)) > 0
            and vwap_val > 0.0
            and current_price >= (math.floor(vwap_val * 100.0) / 100.0)
        )
    ):
        ctx.wakeup_event.set()

    if symbol in ("SOXS", "SOXL") and (400 <= est_hm <= 814):
        if 407 <= est_hm <= 814:
            update_premarket_trend_leader(
                symbol=symbol,
                live_rsi_val=rsi_val,
                now_est=now_est,
                shared_rsi_state=ctx.shared_rsi_state,
            )
        if (
            0.0 < rsi_val <= PRE_TREND_BOOSTER_RSI
            or rsi_val >= PRE_CLIMAX_BOOSTER_RSI
            or rsi_val > PRE_SNIPER_RSI_THRESHOLD
        ):
            ctx.wakeup_event.set()
    elif symbol in ("SOXS", "SOXL") and (est_hm >= 1859 or est_hm <= 359):
        if rsi_val >= PRE_CLIMAX_BOOSTER_RSI or (0.0 < rsi_val <= PRE_TREND_BOOSTER_RSI):
            ctx.wakeup_event.set()
        await execute_daymarket_rsi_alert_plugin(
            ctx=ctx,
            symbol=symbol,
            now_est=now_est,
            current_price=current_price,
            rsi_val=rsi_val,
            vwap_val=vwap_val,
        )


# 초과 Case 87, Case 92, Case 93, Case 94 — 04:07 ~ 08:14 EST 골든타임 내
# 순수 3차 안전망 손절 후 '손절가 하회 최저점 역트레일링 +0.30% 양봉 반등 재진입 (🪤 RE_BOT_TRAIL)' 전용 엔진
# (NQ=F 진폭 기준 3차 임계치[최소 0.80%] 산출 + 토스 QQQ 0.0초 실시간 현재가 기준점 락온)
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
) -> bool:
    if not (_BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol):
        return False

    if symbol not in ("SOXS", "SOXL"):
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if (
        not is_active
        or not (407 <= est_hm <= 814)
        or _BOTTOM_TRACK_CACHE.get("date") != today_str
        or holdings_qty > 0
        or bool(buy_order_id)
        or ctx.in_memory_ordering_lock.get(symbol, False)
    ):
        if not (407 <= est_hm <= 814) or _BOTTOM_TRACK_CACHE.get("date") != today_str:
            _reset_bottom_track_state()
        return False

    if current_price <= 0.0:
        return False

    cur_p_fl = math.floor(current_price * 100.0) / 100.0
    if cur_p_fl <= 0.0:
        return False

    last_stop_p = float(_BOTTOM_TRACK_CACHE.get("last_stop_price", 0.0))
    if last_stop_p <= 0.0:
        _reset_bottom_track_state()
        return False

    # 주가가 반등 체결 없이 손절가 대비 +1.5% 이상 이미 날아가 버렸거나 RSI가 50.0 이상으로 중립화되면 최저점 추적 자동 해제
    if cur_p_fl > last_stop_p * 1.015 or rsi_val >= 50.0:
        _reset_bottom_track_state()
        st_cur = await AssassinLedger.get_state(symbol)
        if st_cur[8] == "STAGE3_STOP_TRACK":
            await AssassinLedger.save_state(symbol, entry_session="")
        return False

    sym_rsi_dict = (ctx.shared_rsi_state or {}).get(symbol, {})
    ewm_st = sym_rsi_dict.get("ewm_state", {}) if isinstance(sym_rsi_dict, dict) else {}
    cur_bar_open = float(ewm_st.get("cur_bar_open", 0.0))
    cur_bar_low = float(ewm_st.get("cur_bar_low", 0.0))
    cand_low = min(cur_p_fl, math.floor(cur_bar_low * 100.0) / 100.0) if cur_bar_low > 0.0 else cur_p_fl

    prev_low = float(_BOTTOM_TRACK_CACHE.get("post_stop_low", last_stop_p))
    if (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD) and (opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD):
        _BOTTOM_TRACK_CACHE["climax_seen"] = True

    # 1단계 (칼날 회피): 신저가를 경신 중이면 절대 매수하지 않고 최저점(post_stop_low)과 반등 목표가(+0.30%)만 갱신!
    made_new_low_tick = False
    if cand_low < prev_low:
        _BOTTOM_TRACK_CACHE["post_stop_low"] = cand_low
        _BOTTOM_TRACK_CACHE["rebound_trig_price"] = (
            math.ceil(cand_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
        )
        _BOTTOM_TRACK_CACHE["last_new_low_ts"] = time.time()
        if cand_low < last_stop_p:
            _BOTTOM_TRACK_CACHE["dipped_below_stop"] = True
        if cur_p_fl <= cand_low:
            made_new_low_tick = True

    post_low = float(_BOTTOM_TRACK_CACHE.get("post_stop_low", prev_low))
    if post_low < last_stop_p:
        _BOTTOM_TRACK_CACHE["dipped_below_stop"] = True

    dipped_below_stop = bool(_BOTTOM_TRACK_CACHE.get("dipped_below_stop", False))
    rebound_trig_p = float(_BOTTOM_TRACK_CACHE.get("rebound_trig_price", 0.0))
    if rebound_trig_p <= 0.0 and post_low > 0.0:
        rebound_trig_p = math.ceil(post_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
        _BOTTOM_TRACK_CACHE["rebound_trig_price"] = rebound_trig_p

    if made_new_low_tick or not dipped_below_stop or is_session_done:
        return False

    is_ha_bull, _, _ = compute_realtime_tick_heikin_ashi(cur_p_fl, ewm_st)
    is_bull_rebound = bool(is_ha_bull or (cur_bar_open > 0.0 and cur_p_fl > cur_bar_open))

    # 2단계: 손절가 아래 최저점(post_low < last_stop_p)을 찍고 신저가 멈춤 + 최저점 대비 +0.30% 이상 양봉 반등 + RSI<=38.0 확인!
    if not (
        bool(_BOTTOM_TRACK_CACHE.get("climax_seen", True))
        and cur_p_fl >= rebound_trig_p
        and cur_p_fl <= last_stop_p
        and is_bull_rebound
        and (0.0 < rsi_val <= BOTTOM_MAX_RSI_CAP)
    ):
        return False

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False) or ctx.in_memory_ordering_lock.get(opp_sym, False):
            return False
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            opp_state = await AssassinLedger.get_state(opp_sym)
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
                return False

            # 1차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준은 나스닥 100 선물지수(NQ=F) 진폭을 사용 (Case 66, 67, 92, 94)
            nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                _reset_bottom_track_state()
                _reset_pre_leader_state()
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                _STRIKE_ALERT_CACHE["last_macro1_date"] = today_str
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 최저점 역트레일링 감시 중 대세장 과열 감지 (자동 진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 최저점 추적 캐시 파기 및 금일 자동 매수 권한 영구 소각"
                )
                return False

            # 초과 Case 92 & Case 94 — NQ=F 진폭 기반 임계치 max(nq_entry_amp * 0.5, 0.80%)를 실시간 적용할
            # 3차 역행 안전망 진입 기준가(ref_entry_p)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온!
            ref_entry_p, ref_src = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True, realtime_probing=True
            )
            if ref_entry_p <= 0.0:
                ref_entry_p = nq_c_global

            latest_p = await ctx.client.get_current_price(symbol)
            if latest_p <= 0.0:
                latest_p = cur_p_fl
            exec_buy_p = math.floor(latest_p * 100.0) / 100.0
            if exec_buy_p < rebound_trig_p or exec_buy_p > last_stop_p:
                return False

            for chk_sym in ("SOXS", "SOXL"):
                hold_chk = await ctx.client.get_symbol_holdings_detail(chk_sym)
                if int(math.floor(hold_chk.get("qty", 0.0))) > 0:
                    return False
                open_orders_chk = await ctx.client.get_orders(status="OPEN", symbol=chk_sym)
                if any(o.get("side") in ("BUY", "SELL") for o in open_orders_chk):
                    return False

            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.995)
            except Exception:
                actual_budget = budget

            target_qty = int(math.floor(actual_budget / exec_buy_p))
            if target_qty <= 0:
                return False

            trend_str = "SHORT" if symbol == "SOXS" else "LONG"
            rebound_sess_tag = (
                "PRE_PULLBACK_REARM"
                if str(_PRE_LEADER_CACHE.get("leader_sym") or "") == symbol
                else "PRE_SNIPER_REARM"
            )
            idem_key = f"RBOT_{symbol}_{now_est.strftime('%H%M%S')}"[:36]
            res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                order_type="LIMIT",
                quantity=target_qty,
                price=f"{exec_buy_p:.2f}",
                client_order_id=idem_key,
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                _reset_trailing_runtime_state(symbol)
                _reset_bottom_track_state()
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session=rebound_sess_tag,
                    entry_trend=trend_str,
                    entry_time=time.time(),
                    nq_entry_price=ref_entry_p,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                ctx.wakeup_event.set()
                rebound_actual_pct = ((exec_buy_p / post_low - 1.0) * 100.0) if post_low > 0.0 else BOTTOM_REBOUND_PCT
                disc_from_stop = last_stop_p - exec_buy_p
                ref_unit = "$" if ref_src == "QQQ실시간" else ""
                dyn_th_init = max(
                    nq_amp_global * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                await ctx.notify_tg(
                    f"🪤 <b>[3차 손절 후 최저점 역트레일링 재출격] {sym_icon}{symbol} 바닥 확인 반등 요격 발사!</b>\n"
                    f"▫️ 바닥 궤적: 직전 손절가 <code>${last_stop_p:.2f}</code> ➡️ 경유 최저점 <code>${post_low:.2f}</code> 찍고 <code>+{rebound_actual_pct:.2f}%</code> 양봉 반등 돌파!\n"
                    f"▫️ 요격 단가(LIMIT): <code>${exec_buy_p:.2f}</code> (손절가 대비 <code>-${disc_from_stop:.2f}</code> 저가 재진입 · <code>{target_qty}주</code> · 현재 RSI <code>{rsi_val:.1f}</code>)\n"
                    f"▫️ 3차 방어 기준({ref_src}): <code>{ref_unit}{ref_entry_p:.2f}</code> (NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> 기반 3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
                    "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 이원화 하이킨 아시 트레일링 자동 결속</code>"
                )
                return True
        except Exception as e:
            print(f"🚨 [최저점 역트레일링 반등 진입 방어 {symbol}] {e}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False
    return False


# MODIFIED: 프리장 골든타임 (04:07 ~ 08:14 EST) 선택지 B (👑 HYB_35_1ST) 이중 타격 게이트 —
#           [게이트 A : 04:07 ~ 08:14 EST 상시] 양방향 동시 대칭 클라이맥스(매수종목 RSI <= 30.0 AND 반대종목 RSI >= 79.9)
#           [게이트 B : 07:00 ~ 08:14 EST 전용] 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 종목(leader_sym)이 RSI <= 35.0 도달 시 추세추종 눌림목 요격
#           (포착 즉시 1차 슬리피지 -0.18% 지정가 매수 덫 선투하 · Case 54, 59, 71, 81, 83, 84, 87, 92, 93, 94, 97, 99)
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

    # Case 87: 해당 종목이 3차 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 추적 중이면 중복 포착 차단!
    if _BOTTOM_TRACK_CACHE.get("active", False) and _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
        return

    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return

    leader_st = update_premarket_trend_leader(
        symbol=symbol,
        live_rsi_val=rsi_val,
        now_est=now_est,
        shared_rsi_state=ctx.shared_rsi_state,
    )
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    if opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD:
        leader_st = update_premarket_trend_leader(
            symbol=opp_sym,
            live_rsi_val=opp_rsi_val,
            now_est=now_est,
            shared_rsi_state=ctx.shared_rsi_state,
        )

    is_opta_cand = (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD) and (opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD)
    is_pb35_cand = (
        (700 <= est_hm <= 814)
        and (str(leader_st.get("leader_sym") or "") == symbol)
        and bool(leader_st.get("pb_rearm_ready", True))
        and (0.0 < rsi_val <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
    )

    if not (is_opta_cand or is_pb35_cand):
        return

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    opp_icon = "🐂" if opp_sym == "SOXL" else "🐻"

    async with ctx.dual_strike_mutex:
        if ctx.in_memory_ordering_lock.get(symbol, False) or ctx.in_memory_ordering_lock.get(opp_sym, False):
            return
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            self_state = await AssassinLedger.get_state(symbol)
            opp_state = await AssassinLedger.get_state(opp_sym)

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

            # 1차 과열 방어선(>=2.0%) 및 3차 동적 임계치 산출 기준은 나스닥 100 선물지수(NQ=F) 진폭을 사용 (Case 66, 67, 92, 94)
            nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data(force_refresh=True)
            nq_amp_global = (((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0)

            if nq_amp_global >= 2.0:
                _reset_bottom_track_state()
                _reset_pre_leader_state()
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
                _STRIKE_ALERT_CACHE["last_macro1_date"] = now_est.strftime("%Y-%m-%d")
                await ctx.notify_tg(
                    "🛑 <b>[프리장 듀얼 스나이퍼] 대세장 과열 감지 (자동 진입 차단)</b>\n"
                    f"▫️ 사유: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (2.0% 이상)\n"
                    "▫️ 조치: 금일 SOXS 및 SOXL 자동 매수 권한 영구 소각 및 관망 전환"
                )
                return

            # 초과 Case 92 & Case 94 — NQ=F 진폭 기반 임계치 max(nq_entry_amp * 0.5, 0.80%)를 실시간 적용할
            # 3차 역행 안전망 진입 기준가(ref_entry_p)는 토스 Open API QQQ 0.0초 실시간 현재가로 락온!
            ref_entry_p, ref_src = await MacroDataCache.get_stage3_reference_price(
                force_refresh=True, realtime_probing=True
            )
            if ref_entry_p <= 0.0:
                ref_entry_p = nq_c_global

            prices_batch = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
            latest_price = float(prices_batch.get(symbol, 0.0)) or current_price
            opp_latest_p = float(prices_batch.get(opp_sym, 0.0))

            if latest_price <= 0.0:
                return

            if ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
                my_dict = ctx.shared_rsi_state[symbol]
                my_rsi_recheck = compute_realtime_tick_rsi(
                    live_price=latest_price,
                    ewm_state=my_dict.get("ewm_state", {}),
                    fallback_rsi=rsi_val,
                )
                my_dict["value"] = my_rsi_recheck
            else:
                my_rsi_recheck = rsi_val

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

            update_premarket_trend_leader(
                symbol=symbol,
                live_rsi_val=my_rsi_recheck,
                now_est=now_est,
                shared_rsi_state=ctx.shared_rsi_state,
            )
            leader_st_now = update_premarket_trend_leader(
                symbol=opp_sym,
                live_rsi_val=opp_rsi_recheck,
                now_est=now_est,
                shared_rsi_state=ctx.shared_rsi_state,
            )

            is_opta_ok = (0.0 < my_rsi_recheck <= PRE_SNIPER_RSI_THRESHOLD) and (
                opp_rsi_recheck >= OPP_CLIMAX_RSI_THRESHOLD
            )
            is_pb35_ok = (
                (700 <= est_hm <= 814)
                and (str(leader_st_now.get("leader_sym") or "") == symbol)
                and bool(leader_st_now.get("pb_rearm_ready", True))
                and (0.0 < my_rsi_recheck <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
            )

            if not (is_opta_ok or is_pb35_ok):
                return

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

            # 포착 즉시 1차 슬리피지(-0.18%, 최소 -$0.02 할인) 지정가 매수 덫을 선투하! (Case 59, 93, 97)
            slip_disc = max(
                BUY_TRAP_MIN_DISC_USD,
                round(cur_p_floored * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2),
            )
            trap_buy_price = math.floor((cur_p_floored - slip_disc) * 100.0) / 100.0
            if trap_buy_price <= 0.0:
                trap_buy_price = cur_p_floored

            current_bp = -1.0
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                actual_budget = min(budget, current_bp * 0.990)
            except Exception:
                actual_budget = budget

            target_qty = int(math.floor(actual_budget / cur_p_floored))

            warn_key = f"{current_session_id}_{symbol}"
            if target_qty <= 0:
                if _STRIKE_ALERT_CACHE.get("last_warn_session") != warn_key:
                    _STRIKE_ALERT_CACHE["last_warn_session"] = warn_key
                    bp_str = f"${current_bp:,.2f}" if current_bp >= 0.0 else "조회실패"
                    await ctx.notify_tg(
                        f"⚠️ <b>[프리장 듀얼 스나이퍼 요격 보류] {sym_icon}{symbol} 주문 수량 0주 산출</b>\n"
                        f"▫️ 포착 지표: {sym_icon}{symbol} RSI <code>{my_rsi_recheck:.1f}</code> &amp; {opp_icon}{opp_sym} RSI <code>{opp_rsi_recheck:.1f}</code> | 현재가: <code>${cur_p_floored:.2f}</code>\n"
                        f"▫️ 설정 시드: <code>${budget:,.2f}</code> | USD 가능금액: <code>{bp_str}</code>\n"
                        f"▫️ 사유: 유효 투입금(<code>${actual_budget:,.2f}</code>)이 1주 가격(<code>${cur_p_floored:.2f}</code>) 미만입니다. (/settlement 에서 시드를 상향해 주세요)"
                    )
                return

            is_compound = await AssassinLedger.get_is_compound(symbol)
            mode_tag = "통합복리" if is_compound else "단리"
            trend_str = "SHORT" if symbol == "SOXS" else "LONG"
            entry_sess_tag = "PRE_SNIPER" if is_opta_ok else "PRE_PULLBACK"

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
                _reset_trailing_runtime_state(symbol)
                _reset_bottom_track_state()
                if entry_sess_tag == "PRE_PULLBACK":
                    mark_pre_pullback_dispatched(symbol)

                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session=entry_sess_tag,
                    entry_trend=trend_str,
                    entry_time=time.time(),
                    nq_entry_price=ref_entry_p,
                    nq_entry_amp=nq_amp_global,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    last_session_id=current_session_id,
                )
                _STRIKE_ALERT_CACHE["last_err_msg"] = ""
                ctx.wakeup_event.set()
                disc_actual = cur_p_floored - trap_buy_price
                ref_unit = "$" if ref_src == "QQQ실시간" else ""
                dyn_th_init = max(
                    nq_amp_global * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )

                if entry_sess_tag == "PRE_PULLBACK":
                    ld_time_s = str(leader_st_now.get("locked_time_str") or "")
                    ld_peak_r = float(leader_st_now.get("leader_rsi") or 79.9)
                    comp_tail = "🚀 +1.30%전 양봉홀딩 ➔ -0.20% 타이트스탑"
                    strike_title = f"🚀 <b>[프리장 07:00~ 추세눌림목 요격] {sym_icon}{symbol} 진성 상승종목 RSI≤35.0 포착 ➡️ 1차 매수 덫 투하</b>"
                    strike_cond_line = (
                        f"▫️ 추세추종 눌림목: 선행 첫 상승종목 {sym_icon}{symbol} (<code>{ld_time_s} EST</code> RSI <code>{ld_peak_r:.1f}</code> 각인) ➡️ "
                        f"현재 RSI <code>{my_rsi_recheck:.1f}</code> (≤35.0 저가 눌림목 · 반대 {opp_sym} <code>{opp_rsi_recheck:.1f}</code>)"
                    )
                    shield_desc_line = (
                        f"<code>RSI≤35 유지 시 30초간 저가 덫 대기 / 30초 내 RSI 35 초과 반등 시 즉각 현재가 매수 ({comp_tail})</code>"
                    )
                else:
                    comp_tail = "⚡ +1.00% 보존 고정 &amp; -0.30% 콜백/HA음봉 스윕"
                    strike_title = f"🪤 <b>[프리장 듀얼 스나이퍼] {sym_icon}{symbol} 대칭 클라이맥스 포착 ➡️ 1차 슬리피지 매수 덫 투하</b>"
                    strike_cond_line = (
                        f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{my_rsi_recheck:.1f}</code> (≤30.0) &amp; "
                        f"{opp_icon}{opp_sym} RSI <code>{opp_rsi_recheck:.1f}</code> (≥79.9[80인지])"
                    )
                    shield_desc_line = (
                        f"<code>RSI≤30 유지 시 30초간 저가 덫 대기 / 30초 내 RSI 30 초과 반등 시 즉각 현재가 매수 ({comp_tail})</code>"
                    )

                await ctx.notify_tg(
                    f"{strike_title}\n"
                    f"{strike_cond_line}\n"
                    f"▫️ 포착 현재가: <code>${cur_p_floored:.2f}</code> ➡️ 1차 매수 덫(LIMIT): <code>${trap_buy_price:.2f}</code> (슬리피지 <code>-${disc_actual:.2f}</code> 할인 · {mode_tag} <code>${budget:,.2f}</code> ➔ <code>{target_qty}주</code>)\n"
                    f"▫️ 매크로 기준: 3차 방어({ref_src}) <code>{ref_unit}{ref_entry_p:.2f}</code> | NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> (3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
                    f"▫️ 조건부 30초 타임쉴드: {shield_desc_line}"
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


# MODIFIED: 조건부 30초 타임쉴드 및 즉각 매수 전환 플러그인 (SOXS / SOXL 본진 PRE_SNIPER [RSI>30.0 ➔ PRE_SNIPER_REARM] & PRE_PULLBACK [RSI>35.0 ➔ PRE_PULLBACK_REARM] 분기 — 04:07 ~ 08:14 EST)
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
        or entry_session not in ("PRE_SNIPER", "PRE_PULLBACK")
        or entry_time <= 0.0
        or ctx.in_memory_ordering_lock.get(symbol, False)
    ):
        return buy_order_id, entry_session

    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814):
        return buy_order_id, entry_session

    rebound_rsi_threshold = (
        PRE_TREND_PULLBACK_RSI_THRESHOLD
        if entry_session == "PRE_PULLBACK"
        else PRE_SNIPER_RSI_THRESHOLD
    )
    rearm_sess_tag = (
        "PRE_PULLBACK_REARM"
        if entry_session == "PRE_PULLBACK"
        else "PRE_SNIPER_REARM"
    )
    elapsed = time.time() - entry_time
    is_rsi_rebound_breakout = (rsi_val > rebound_rsi_threshold)
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

        th_int = int(rebound_rsi_threshold)
        if is_rsi_rebound_breakout:
            trigger_title = f"⚡ [프리장 스나이퍼 RSI {th_int} 초과 반등 즉각 매수] {sym_icon}{symbol}"
            trigger_desc = (
                f"타임쉴드 중 실시간 RSI <code>{rsi_val:.1f}</code> "
                f"({rebound_rsi_threshold:.1f} 초과 반등 감지 · 경과 <code>{elapsed:.1f}초</code>)"
            )
        else:
            trigger_title = f"🔄 [프리장 스나이퍼 30초 타임쉴드 만료 재장전] {sym_icon}{symbol}"
            trigger_desc = (
                f"RSI≤{th_int} 유지 30초 타임쉴드 만료 "
                f"(현재 RSI <code>{rsi_val:.1f}</code> · 경과 <code>{elapsed:.1f}초</code>)"
            )

        if not is_rsi_rebound_breakout and old_trap_price > 0.0 and rearm_price <= old_trap_price:
            await AssassinLedger.save_state(
                symbol, entry_session=rearm_sess_tag, entry_time=time.time()
            )
            return buy_order_id, rearm_sess_tag

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
                    entry_session=rearm_sess_tag,
                    entry_time=time.time(),
                )
                ctx.wakeup_event.set()
                await ctx.notify_tg(
                    f"<b>{trigger_title} 현재가 즉시 상향 요격</b>\n"
                    f"▫️ 사유: {trigger_desc}\n"
                    f"▫️ 단가 정정(LIMIT): 기존 덫 <code>${old_trap_price:.2f}</code> ➡️ 현재가 <code>${rearm_price:.2f}</code> (<code>{ordered_qty}주</code>)\n"
                    "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 이원화 하이킨 아시 트레일링 자동 결속</code>"
                )
                return new_buy_id, rearm_sess_tag
            except Exception as mod_err:
                print(f"🚨 [재장전 정정 폴백 {symbol}] modify_order 실패 -> 선취소 후 재발사 전환: {mod_err}", flush=True)

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
                symbol, buy_order_id="", entry_session=entry_session, entry_time=time.time()
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"🟢 <b>[프리장 스나이퍼 부분체결 확정] {sym_icon}{symbol} 1차 매수 덫 ({filled_after}주) 체결 완료</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 잔여 미체결분 취소 완료 ➡️ 확보된 저가 물량(<code>{filled_after}주</code>) 즉시 이원화 트레일링 결속 직행"
            )
            return "", entry_session

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
                entry_session=rearm_sess_tag,
                entry_time=time.time(),
            )
            ctx.wakeup_event.set()
            await ctx.notify_tg(
                f"<b>{trigger_title} 1차 매수 덫 취소 ➡️ 현재가 신규 요격 발사</b>\n"
                f"▫️ 사유: {trigger_desc}\n"
                f"▫️ 재장전 지정가(LIMIT): <code>${old_trap_price:.2f}</code> ➡️ <code>${rearm_price:.2f}</code> (<code>{rearm_qty}주</code>)\n"
                "▫️ 후속 조치: <code>전량 체결 즉시 상단 안전 캡(+5.00%) &amp; 이원화 하이킨 아시 트레일링 자동 결속</code>"
            )
            return new_buy_id, rearm_sess_tag

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
