# =====================================================================
# FILE: whipsaw_plugin.py
# 목적: 03:58 ~ 04:07 EST (9분간) 실전 듀얼 1주 고정 '휩소 노출 작전' 및 04:07 정각 타임쉴드 정리 코어
#       (whipsaw_plugin.py 2/2 분할본 · daymarket_sim_plugin 100% 리익스포트 결속)
#       — NQ>=2.0% 과열차단일에도 본진과 별개로 독립 가동되는 데이장 VWAP 0.5% 역산 매수 ➔
#         체결 시 +1.00% 익절 매도 듀얼 1주 휩소 매수 덫 발사 및
#         04:07:00 EST 정각 미체결 무비용 취소 / 보유 1주 현재가 즉시 매도 후 본진 인계·과열차단 복원
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 데이장(19:00~03:57 EST) 100% [모의] 스나이퍼를 daymarket_sim_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 초과 Case 79, 80, 95, 96 & 98 — 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전(WHIPSAW_1SH)은 당일 NQ=F >= 2.0% 과열차단(CANCELED_MACRO)일에도
#           차단되지 않고 본진과 별개로 데이장 VWAP(ewm_state["day_vwap"] 우선) 0.5% 역산 가격(VWAP / 1.005)에 1주 진입하여 매수 체결가 대비 +1.00%(avg_price * 1.01) 익절을 추구하며,
#           04:07:00 EST 정리 직후 buy_order_id="" 원자적 소각 및 과열차단(CANCELED_MACRO)을 복원하여 본진 시드를 보호
# NEW: 1) 장중 재기동/04:00 전환 시 ewm_state["day_vwap"] 결측(0.0)이면 즉시 1분봉 200개를 조회해 온디맨드(On-Demand) 복원 (asyncio.to_thread 비동기 격리)
#      2) 3파동 왕복장 등에서 SOXS·SOXL 양쪽 모두 현재가 <= 데이장 VWAP 인 날에도 포기하지 않고 괴리율 상위 종목을 100% 선정하되
#         매수 덫 가격(calc_buy_p)이 항상 현재가 아래 하단(-0.5% 역산)에 걸리도록 보장
#      3) 03:58~03:59 EST 토스 장전환 공백(422 order-hours-closed) 시 텔레그램 사전 락온 알림 발송 및 고유 clientOrderId로 04:00:00 EST 즉시 발사 보장
#      4) execute_rsi_monitoring_plugin / _reset_daymarket_sim_state 다중 별칭 100% 리익스포트 및
#         ctx.notify_tg / cancel_helper_fn NoneType 호출 크래시('NoneType' object is not callable) 원천 차단 래퍼 결속

import math
import time
import asyncio
import inspect
import html
from datetime import datetime

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    calculate_rsi_1m,
)
import daymarket_sim_plugin as _daysim_mod
from daymarket_sim_plugin import (
    EST_TZ,
    KST_TZ,
    PRE_SNIPER_RSI_THRESHOLD,
    PRE_TREND_BOOSTER_RSI,
    PRE_CLIMAX_BOOSTER_RSI,
    OPP_CLIMAX_RSI_THRESHOLD,
    DAY_SIM_ARM_PROFIT_RATE,
    DAY_SIM_FLOOR_LOCK_RATE,
    DAY_SIM_TRAIL_DROP_PCT,
    DAY_SIM_NQ_VEL_SURGE_PCT,
    _DAY_SIM_LOCK,
    _WHIPSAW_LOCK,
    BUY_TRAP_SLIPPAGE_PCT,
    BUY_TRAP_MIN_DISC_USD,
    BUY_TRAP_REARM_WAIT_SEC,
    WHIPSAW_FIXED_QTY,
    WHIPSAW_BUY_REVERSE_FACTOR,
    WHIPSAW_SELL_PROFIT_FACTOR,
    WHIPSAW_PROFIT_FACTOR,
    _WHIPSAW_SIM_CACHE,
    _reset_day_sim_position_only,
    _reset_day_sim_state,
    _record_day_sim_result,
    _build_day_sim_scoreboard_line,
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    _get_day_opening_window_state,
    _sync_daymarket_trend_leader_and_reset,
    settle_daymarket_sim_before_premarket as _raw_settle_daymarket_sim_before_premarket,
    execute_daymarket_rsi_alert_plugin as _raw_execute_daymarket_rsi_alert_plugin,
)


# =====================================================================
# NEW: 'NoneType' object is not callable 원천 차단용 텔레그램 방탄 발송기 & 컨텍스트 힐러
# =====================================================================
async def _notify_tg_safe(ctx, text: str):
    if not text or ctx is None:
        return
    try:
        from defense_plugin import _send_tg_safe as _def_send_tg
        await _def_send_tg(ctx, text)
        return
    except Exception:
        pass

    bot = getattr(ctx, "bot", None)
    chat_id = getattr(ctx, "admin_chat_id", None) or getattr(ctx, "chat_id", None)
    if bot is not None and chat_id:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except Exception as e:
            print(f"🚨 [텔레그램 알림 방어] {e}", flush=True)


def _ensure_ctx_notify_tg(ctx):
    """
    SniperPluginContext 의 notify_tg 또는 send_tg 콜백이 None 일 때
    daymarket_sim_plugin / whipsaw_plugin 내부에서 `await ctx.notify_tg(...)` 호출 시
    발생하는 `TypeError: 'NoneType' object is not callable` 크래시를 100% 원천 차단합니다.
    """
    if ctx is None:
        return
    if not callable(getattr(ctx, "notify_tg", None)):
        try:
            setattr(ctx, "notify_tg", lambda msg: _notify_tg_safe(ctx, msg))
        except Exception:
            pass
    if not callable(getattr(ctx, "send_tg", None)):
        try:
            setattr(ctx, "send_tg", lambda msg: _notify_tg_safe(ctx, msg))
        except Exception:
            pass


async def settle_daymarket_sim_before_premarket(ctx, now_est: datetime, *args, **kwargs):
    _ensure_ctx_notify_tg(ctx)
    return await _raw_settle_daymarket_sim_before_premarket(ctx, now_est, *args, **kwargs)


async def execute_daymarket_rsi_alert_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    current_price: float = 0.0,
    rsi_val: float = 0.0,
    opp_rsi_val: float = 0.0,
    *args,
    **kwargs,
):
    _ensure_ctx_notify_tg(ctx)
    return await _raw_execute_daymarket_rsi_alert_plugin(
        ctx,
        symbol,
        now_est,
        is_active,
        current_price,
        rsi_val,
        opp_rsi_val,
        *args,
        **kwargs,
    )


# NEW: sniper_plugin, main_loops_plugin, tg_router 등 모든 외부 모듈 호환용 다중 별칭 100% 바인딩
_reset_daymarket_sim_state = _reset_day_sim_state
reset_daymarket_sim_state = _reset_day_sim_state
get_day_sim_status = get_daymarket_sim_status
get_whipsaw_status = get_whipsaw_runtime_status
settle_day_sim_before_premarket = settle_daymarket_sim_before_premarket
execute_rsi_monitoring_plugin = execute_daymarket_rsi_alert_plugin
execute_daymarket_sim_plugin = execute_daymarket_rsi_alert_plugin
execute_day_sim_plugin = execute_daymarket_rsi_alert_plugin
run_daymarket_sim_plugin = execute_daymarket_rsi_alert_plugin


def _extract_order_id_safe(res: dict) -> str:
    """
    NEW: toss_api.py 의 create_order 가 이미 언래핑된 {"orderId": "..."} 를 반환하든
    구버전 래퍼가 {"result": {"orderId": "..."}} 를 반환하든 100% 안전하게 orderId 를 추출합니다.
    """
    if not isinstance(res, dict):
        return ""
    oid = res.get("orderId")
    if not oid and isinstance(res.get("result"), dict):
        oid = res["result"].get("orderId")
    return str(oid or "").strip()


# =====================================================================
# MODIFIED: 초과 Case 79, 92, 95, 96 & 98 — 03:58 ~ 04:06:59 EST (9분간) 듀얼(SOXS/SOXL) 데이장 누적 VWAP 비교 및
#           VWAP 상단 종목(양쪽 모두 VWAP 하회 시 상대 괴리율 1위 종목 폴백) 기준 하단 0.5% 역산(VWAP / 1.005) 1주 지정가 매수 덫
#           ➔ 체결 시 매수가 대비 +1.00%(avg_price * 1.01) 익절 매도 덫 전용 플러그인
# =====================================================================
async def execute_whipsaw_vwap_exposure_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str,
    is_active: bool,
    is_session_done: bool,
    holdings_qty: int,
    buy_order_id: str,
    current_price: float,
    day_vwap: float = 0.0,
    **kwargs,
):
    _ensure_ctx_notify_tg(ctx)
    if symbol not in ("SOXS", "SOXL") or not is_active or ctx.in_memory_ordering_lock.get(symbol, False):
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (358 <= est_hm <= 406):
        return False

    today_str = now_est.strftime("%Y-%m-%d")

    async with _WHIPSAW_LOCK:
        if _WHIPSAW_SIM_CACHE.get("whipsaw_date") != today_str:
            _WHIPSAW_SIM_CACHE["whipsaw_date"] = today_str
            _WHIPSAW_SIM_CACHE["whipsaw_target_sym"] = ""
            _WHIPSAW_SIM_CACHE["whipsaw_spot_price"] = 0.0
            _WHIPSAW_SIM_CACHE["whipsaw_locked_vwap"] = 0.0
            _WHIPSAW_SIM_CACHE["whipsaw_buy_price"] = 0.0
            _WHIPSAW_SIM_CACHE["whipsaw_sell_target"] = 0.0
            _WHIPSAW_SIM_CACHE["whipsaw_mode_desc"] = ""
            _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = False
            _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = False
            _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = 0.0
            _WHIPSAW_SIM_CACHE["whipsaw_prelock_notified"] = False
            _WHIPSAW_SIM_CACHE["whipsaw_skip_notified"] = False
            _WHIPSAW_SIM_CACHE["whipsaw_last_err"] = ""

        if _WHIPSAW_SIM_CACHE.get("whipsaw_order_fired", False):
            return False

        total_shared_hold = sum(int(v) for v in (ctx.shared_holdings or {}).values())
        if total_shared_hold > 0 or int(holdings_qty or 0) > 0 or bool(buy_order_id):
            if not _WHIPSAW_SIM_CACHE.get("whipsaw_skip_notified", False):
                _WHIPSAW_SIM_CACHE["whipsaw_skip_notified"] = True
                await _notify_tg_safe(
                    ctx,
                    (
                        "⚠️ <b>[듀얼 휩소 노출 작전 보류] 계좌 내 기존 보유 물량 또는 미체결 주문 감지</b>\n"
                        f"▫️ 현재 보유 수량 합계: <code>{max(total_shared_hold, int(holdings_qty or 0))}주</code>\n"
                        "▫️ 조치: 기존 포지션 보호를 위해 금일 03:58 ~ 04:07 EST 1주 휩소 신규 진입을 패스합니다."
                    ),
                )
            _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
            return False

        # 1단계: SOXS와 SOXL의 실시간 현재가 및 데이장 누적 VWAP(결측 시 온디맨드 200분봉 즉시 산출) 비교 스냅샷 선정
        if not _WHIPSAW_SIM_CACHE.get("whipsaw_target_sym"):
            prices_map = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
            if current_price > 0.0 and prices_map.get(symbol, 0.0) <= 0.0:
                prices_map[symbol] = current_price

            above_vwap_candidates = []
            all_candidates = []

            for cand_sym in ("SOXS", "SOXL"):
                c_p = float(prices_map.get(cand_sym, 0.0) or 0.0)
                if c_p <= 0.0:
                    c_p = await ctx.client.get_current_price(cand_sym)

                cand_st = ((ctx.shared_rsi_state or {}).get(cand_sym)) or {}
                cand_ewm = cand_st.get("ewm_state") or {}
                c_vwap = float(cand_ewm.get("day_vwap", 0.0) or 0.0)

                # MODIFIED: 제1헌법 준수 — 동기 함수 calculate_rsi_1m 를 asyncio.to_thread 로 비동기 스레드 격리하여 호출!
                if c_vwap <= 0.0:
                    try:
                        candles_1m = await ctx.client.get_1m_candles(cand_sym, count=200)
                        if candles_1m:
                            calc_res = await asyncio.to_thread(calculate_rsi_1m, candles_1m, now_est)
                            if inspect.isawaitable(calc_res):
                                calc_res = await calc_res
                            r_calc, v_calc, ewm_calc = calc_res
                            if isinstance(ctx.shared_rsi_state, dict) and cand_sym in ctx.shared_rsi_state:
                                ctx.shared_rsi_state[cand_sym]["value"] = r_calc
                                ctx.shared_rsi_state[cand_sym]["vwap"] = v_calc
                                ctx.shared_rsi_state[cand_sym]["ewm_state"] = ewm_calc
                            c_vwap = float((ewm_calc or {}).get("day_vwap", 0.0) or 0.0) or float(v_calc or 0.0)
                    except Exception as ve:
                        print(f"🚨 [휩소 온디맨드 VWAP 복원 방어 {cand_sym}] {ve}", flush=True)

                if c_vwap <= 0.0:
                    c_vwap = float(cand_st.get("vwap", 0.0) or 0.0)
                if cand_sym == symbol and c_vwap <= 0.0 and float(day_vwap or 0.0) > 0.0:
                    c_vwap = float(day_vwap)
                if c_vwap <= 0.0 and c_p > 0.0:
                    c_vwap = c_p

                c_p_fl = math.floor(c_p * 100.0) / 100.0
                c_vwap_fl = round(c_vwap, 2)
                if c_p_fl > 0.0 and c_vwap_fl > 0.0:
                    premium_ratio = (c_p_fl - c_vwap_fl) / c_vwap_fl * 100.0
                    item_tuple = (premium_ratio, cand_sym, c_p_fl, c_vwap_fl)
                    all_candidates.append(item_tuple)
                    if c_p_fl > c_vwap_fl:
                        above_vwap_candidates.append(item_tuple)

            if not all_candidates:
                return False

            # 1순위: 현재가 > 데이장 VWAP 종목 중 괴리율 1위 선정
            # 2순위(W자 왕복장 폴백): 양쪽 모두 현재가 <= 데이장 VWAP일 경우 상대 괴리율 1위(VWAP 최근접) 종목 선정
            if above_vwap_candidates:
                above_vwap_candidates.sort(key=lambda x: x[0], reverse=True)
                best_prem, chosen_sym, chosen_spot, chosen_vwap = above_vwap_candidates[0]
                is_fallback_mode = False
            else:
                all_candidates.sort(key=lambda x: x[0], reverse=True)
                best_prem, chosen_sym, chosen_spot, chosen_vwap = all_candidates[0]
                is_fallback_mode = True

            vwap_rev_p = max(0.01, math.floor((chosen_vwap / WHIPSAW_BUY_REVERSE_FACTOR) * 100.0) / 100.0)
            spot_rev_p = max(0.01, math.floor((chosen_spot / WHIPSAW_BUY_REVERSE_FACTOR) * 100.0) / 100.0)

            # 무비용 하단 매수 덫 원칙 사수: 역산 매수가가 현재가 이상이면 현재가 대비 -0.5% 역산가로 상한 캡 적용
            if vwap_rev_p < chosen_spot:
                calc_buy_p = vwap_rev_p
                rev_basis_str = f"VWAP 0.5% 역산 하단 매수덫(${calc_buy_p:.2f})"
            else:
                calc_buy_p = spot_rev_p
                rev_basis_str = f"양종목 VWAP하회 폴백 ➡️ 현재가 0.5% 역산 하단 매수덫(${calc_buy_p:.2f})"

            calc_sell_tp = math.ceil(calc_buy_p * WHIPSAW_SELL_PROFIT_FACTOR * 100.0) / 100.0
            pos_rel_str = "상단 위치" if not is_fallback_mode else "최근접 상대강세"
            mode_desc = (
                f"{chosen_sym} 현재가(${chosen_spot:.2f}) 데이장VWAP(${chosen_vwap:.2f}, {best_prem:+.2f}%) {pos_rel_str} "
                f"➡️ {rev_basis_str} · 체결 시 +1.00% 익절 매도덫(${calc_sell_tp:.2f})"
            )

            _WHIPSAW_SIM_CACHE["whipsaw_target_sym"] = chosen_sym
            _WHIPSAW_SIM_CACHE["whipsaw_spot_price"] = chosen_spot
            _WHIPSAW_SIM_CACHE["whipsaw_locked_vwap"] = chosen_vwap
            _WHIPSAW_SIM_CACHE["whipsaw_buy_price"] = calc_buy_p
            _WHIPSAW_SIM_CACHE["whipsaw_sell_target"] = calc_sell_tp
            _WHIPSAW_SIM_CACHE["whipsaw_mode_desc"] = mode_desc
            print(
                f"🌪️ [듀얼 휩소 매수덫 타점 락온 {chosen_sym}] {now_est.strftime('%H:%M:%S')} EST | {mode_desc}",
                flush=True,
            )

        target_sym = str(_WHIPSAW_SIM_CACHE.get("whipsaw_target_sym", ""))
        if not target_sym or symbol != target_sym:
            return False

        target_buy_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_buy_price", 0.0) or 0.0)
        target_sell_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_sell_target", 0.0) or 0.0)
        locked_vwap = float(_WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0) or 0.0)
        mode_desc_str = str(_WHIPSAW_SIM_CACHE.get("whipsaw_mode_desc", ""))

        if target_buy_p <= 0.0 or target_sell_p <= 0.0:
            return False

        async with ctx.dual_strike_mutex:
            if ctx.in_memory_ordering_lock.get(target_sym, False):
                return False
            ctx.in_memory_ordering_lock[target_sym] = True
            try:
                state_chk = await AssassinLedger.get_state(target_sym)
                if not bool(state_chk[5]):
                    return False

                nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)

                ref_p_now, _ = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                if ref_p_now <= 0.0:
                    ref_p_now = nq_c_now

                for chk_s in ("SOXS", "SOXL"):
                    if await AssassinLedger.get_buy_order_id(chk_s) != "":
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return False
                    h_det = await ctx.client.get_symbol_holdings_detail(chk_s)
                    if int(math.floor(float(h_det.get("qty", 0.0) or 0.0))) > 0:
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return False
                    open_o = await ctx.client.get_orders(status="OPEN", symbol=chk_s)
                    if isinstance(open_o, list) and any(
                        isinstance(o, dict) and o.get("side") in ("BUY", "SELL") for o in open_o
                    ):
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return False

                try:
                    current_bp = await ctx.client.get_usd_buying_power()
                    if 0.0 < current_bp < target_buy_p:
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        await _notify_tg_safe(
                            ctx,
                            (
                                f"⚠️ <b>[듀얼 휩소 노출 작전 보류] {target_sym} 1주 매수덫 예수금 부족</b>\n"
                                f"▫️ 필요 금액: <code>${target_buy_p:.2f}</code> (1주) \vert{} USD 가능금액: <code>${current_bp:,.2f}</code>"
                            ),
                        )
                        return False
                except Exception:
                    pass

                # 토스 서버의 clientOrderId 10분 결과 캐싱으로 인해 03:58~04:00 장전환 경계의 422 거절이
                # 캐싱되는 현상을 원천 차단하기 위해 매 시도마다 고유 시퀀스를 부여합니다.
                seq_tag = int(time.time() * 10) % 100000
                idem_key = f"WHP_{target_sym}_{today_str.replace('-', '')}_{seq_tag}"[:36]
                res = await ctx.client.create_order(
                    symbol=target_sym,
                    side="BUY",
                    order_type="LIMIT",
                    quantity=WHIPSAW_FIXED_QTY,
                    price=target_buy_p,
                    client_order_id=idem_key,
                )

                new_buy_id = _extract_order_id_safe(res)
                if new_buy_id:
                    _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                    trend_tag = "SHORT" if target_sym == "SOXS" else "LONG"
                    opp_sym = "SOXL" if target_sym == "SOXS" else "SOXS"
                    now_str = now_est.strftime("%Y-%m-%d %H:%M:%S")

                    await AssassinLedger.save_state(
                        target_sym,
                        buy_order_id=new_buy_id,
                        entry_session="WHIPSAW_1SH",
                        entry_trend=trend_tag,
                        entry_time=now_str,
                        nq_entry_price=float(ref_p_now or 0.0),
                        nq_entry_amp=float(nq_amp_now or 0.0),
                        is_session_done=True,
                        last_session_id=current_session_id,
                        whipsaw_target_price=float(target_sell_p),
                    )
                    await AssassinLedger.save_state(
                        opp_sym,
                        is_session_done=True,
                        last_session_id=current_session_id,
                    )
                    if getattr(ctx, "wakeup_event", None):
                        ctx.wakeup_event.set()
                    exp_gross_pct = ((target_sell_p / target_buy_p - 1.0) * 100.0) if target_buy_p > 0.0 else 1.00
                    sym_icon = "🐻" if target_sym == "SOXS" else "🐂"
                    macro_indep_note = (
                        f" (NQ진폭 {nq_amp_now:.2f}% 과열차단 별개 1주 독립 가동 · 04:07 정리 후 본진 차단 복원)"
                        if nq_amp_now >= 2.0
                        else ""
                    )
                    await _notify_tg_safe(
                        ctx,
                        (
                            f"🌪️ <b>[듀얼 휩소 노출 작전 개시] {sym_icon}{target_sym} 0.5% 역산 1주 매수 덫 투하 완료!</b>\n"
                            f"▫️ 작전 시간: <code>03:58 ~ 04:07 EST (9분간 유지{macro_indep_note})</code>\n"
                            f"▫️ 선정 종목: <b>{sym_icon}{target_sym}</b> | 데이장 누적 VWAP: <code>${locked_vwap:.2f}</code>\n"
                            f"▫️ 타점 판정: <code>{html.escape(mode_desc_str)}</code>\n"
                            f"▫️ 주문 내역: <b>1주 고정(LIMIT)</b> 하단 매수덫 <code>${target_buy_p:.2f}</code> ➡️ 체결 시 +1.00\% 목표 매도덫 <code>${target_sell_p:.2f}</code> (기대 <code>+{exp_gross_pct:.2f}%</code>)\n"
                            "▫️ 무비용 방어: <code>04:07 EST까지 덫 미체결 시 비용 $0 자동 취소 / 체결 후 미매도 시 현재가 즉시 매도</code>"
                        ),
                    )
                    return True
            except Exception as e:
                err_str = str(e)
                sym_icon = "🐻" if target_sym == "SOXS" else "🐂"
                if ("order-hours-closed" in err_str or "422" in err_str) and (358 <= est_hm <= 400):
                    print(
                        f"⏳ [듀얼 휩소 매수덫 대기 {target_sym}] 장전환 공백 감지 -> 04:00:00 EST 프리장 개장 즉시 락온 타점(${target_buy_p:.2f})으로 자동 발사합니다.",
                        flush=True,
                    )
                    if not _WHIPSAW_SIM_CACHE.get("whipsaw_prelock_notified", False):
                        _WHIPSAW_SIM_CACHE["whipsaw_prelock_notified"] = True
                        await _notify_tg_safe(
                            ctx,
                            (
                                f"⏳ <b>[03:58 듀얼 휩소 타점 사전 락온 완료] {sym_icon}{target_sym} (04:00:00 EST 개장 발사 대기)</b>\n"
                                f"▫️ 락온 타점: <b>{sym_icon}{target_sym} 1주 매수덫 <code>${target_buy_p:.2f}</code></b> ➡️ 목표 매도덫 <code>${target_sell_p:.2f} (+1.00%)</code>\n"
                                f"▫️ 판정 근거: <code>{html.escape(mode_desc_str)}</code>\n"
                                "▫️ 상태: 토스 데이장 마감(16:50 KST) ~ 프리장 개장(17:00 KST / 04:00 EST) 전환 대기 중이며, <b>04:00:00 EST 정각 첫 틱에 거래소로 즉시 발사됩니다!</b>"
                            ),
                        )
                else:
                    print(f"🚨 [듀얼 휩소 노출 작전 에러 {target_sym}] {err_str}", flush=True)
                    if _WHIPSAW_SIM_CACHE.get("whipsaw_last_err") != err_str:
                        _WHIPSAW_SIM_CACHE["whipsaw_last_err"] = err_str
                        await _notify_tg_safe(
                            ctx,
                            (
                                f"🚨 <b>[듀얼 휩소 노출 작전 주문 에러 {sym_icon}{target_sym}]</b>\n"
                                f"▫️ 목표 매수덫: <code>${target_buy_p:.2f}</code> (1주)\n"
                                f"▫️ 사유: <code>{html.escape(err_str)}</code>"
                            ),
                        )
            finally:
                ctx.in_memory_ordering_lock[target_sym] = False
    return False


execute_whipsaw_exposure_plugin = execute_whipsaw_vwap_exposure_plugin


# =====================================================================
# Case 80, 초과 Case 95 & Case 96 — 04:07:00 EST 정각 듀얼 휩소 노출 작전 9분 타임쉴드 만료 정리 플러그인
# =====================================================================
async def enforce_0407_whipsaw_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    current_price: float = 0.0,
    cancel_helper_fn=None,
    **kwargs,
) -> tuple[str, str, bool]:
    _ensure_ctx_notify_tg(ctx)
    est_hm = now_est.hour * 100 + now_est.minute
    if symbol not in ("SOXS", "SOXL") or not is_active or not (407 <= est_hm <= 929):
        return buy_order_id, entry_session, False

    if entry_session != "WHIPSAW_1SH":
        return buy_order_id, entry_session, False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return buy_order_id, entry_session, False

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # [분기 1] 04:07 EST 도달 시점에 1주 매수 덫이 미체결(holdings_qty == 0)인 경우 -> 비용 $0 즉시 취소 후 본진 인계/과열차단 복원
    if int(holdings_qty or 0) == 0:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            canceled_any = False
            canceled_ids = set()
            if buy_order_id:
                try:
                    await ctx.client.cancel_order(buy_order_id)
                    canceled_ids.add(buy_order_id)
                    canceled_any = True
                except Exception as ce:
                    err_low = str(ce).lower()
                    if "already-filled" in err_low:
                        return buy_order_id, entry_session, True
                    if "already-canceled" in err_low or "404" in err_low or "order-not-found" in err_low:
                        canceled_ids.add(buy_order_id)

            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                if isinstance(open_orders, list):
                    for o in open_orders:
                        if not isinstance(o, dict):
                            continue
                        oid = str(o.get("orderId") or "")
                        if o.get("side") == "BUY" and oid and oid not in canceled_ids:
                            try:
                                await ctx.client.cancel_order(oid)
                                canceled_ids.add(oid)
                                canceled_any = True
                            except Exception:
                                pass
            except Exception:
                pass

            _, nq_h_cut, nq_l_cut = await MacroDataCache.get_cached_nq_data()
            nq_amp_cut = (((nq_h_cut - nq_l_cut) / nq_l_cut * 100.0) if nq_l_cut > 0.0 else 0.0)
            is_macro_overheated = (nq_amp_cut >= 2.0)

            await AssassinLedger.save_state(
                symbol,
                last_buy_price=0.0,
                price=0.0,
                target_sell_price=0.0,
                is_session_done=True if is_macro_overheated else False,
                buy_order_id="",
                sell_order_id="",
                entry_session="CANCELED_MACRO" if is_macro_overheated else "",
                entry_time="",
                nq_entry_price=0.0,
                nq_entry_amp=0.0,
                entry_trend="",
                whipsaw_target_price=0.0,
            )
            await AssassinLedger.unlock_all_symbols_for_reentry()
            should_notify = not _WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False)
            _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
            if getattr(ctx, "wakeup_event", None):
                ctx.wakeup_event.set()

            if (canceled_any or bool(buy_order_id)) and should_notify:
                if is_macro_overheated:
                    next_action_desc = (
                        f"미체결 1주 매수 덫 무비용 파기 ➡️ 당일 NQ=F 진폭(<code>{nq_amp_cut:.2f}%</code>) 과열로 "
                        "<b>본진 자동 진입 차단(🛑 CANCELED_MACRO) 복원 완료</b>"
                    )
                else:
                    next_action_desc = (
                        "미체결 1주 매수 덫 무비용 파기 ➡️ "
                        "<b>04:07 ~ 08:14 EST 본진(👑HYB_35_1ST : 대칭 클라이맥스 &amp; 07:00~ 추세눌림목) 정식 가동 개시</b>"
                    )
                await _notify_tg_safe(
                    ctx,
                    (
                        f"⏱️ <b>[04:07 타임쉴드 해제 {sym_icon}{symbol}] 휩소 1주 매수 덫 미체결 취소 완료 (비용 $0)</b>\n"
                        "▫️ 사유: 03:58 ~ 04:07 EST (9분간) 듀얼 휩소 노출 작전 시간 만료\n"
                        f"▫️ 조치: {next_action_desc}"
                    ),
                )
            return "", ("CANCELED_MACRO" if is_macro_overheated else ""), True
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # [분기 2] 04:07 EST 도달 시점에 휩소/데이장 1주가 체결되어 보유 중(holdings_qty > 0)인 경우 -> 현재가 즉시 매도!
    if int(holdings_qty or 0) > 0:
        now_ts = time.time()
        if now_ts - float(_WHIPSAW_SIM_CACHE.get("whipsaw_last_dump_tick", 0.0) or 0.0) < 1.5:
            return buy_order_id, entry_session, True

        ctx.in_memory_ordering_lock[symbol] = True
        try:
            if buy_order_id:
                try:
                    await ctx.client.cancel_order(buy_order_id)
                except Exception:
                    pass
                await AssassinLedger.save_state(symbol, buy_order_id="")
                buy_order_id = ""

            # NEW: cancel_helper_fn 이 None 으로 전달될 때 defense_plugin 의 헬퍼로 자동 안전 폴백
            if not callable(cancel_helper_fn):
                from defense_plugin import _cancel_open_orders_and_wait_sellable
                sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
                    ctx=ctx,
                    symbol=symbol,
                    holdings_qty=max(1, int(holdings_qty or 1)),
                    sell_order_id=sell_order_id,
                    buy_order_id=buy_order_id,
                )
            else:
                try:
                    sellable_q_int, surviving_sell_ids = await cancel_helper_fn(
                        ctx=ctx,
                        symbol=symbol,
                        holdings_qty=max(1, int(holdings_qty or 1)),
                        sell_order_id=sell_order_id,
                        cancel_all_sides=True,
                    )
                except TypeError:
                    sellable_q_int, surviving_sell_ids = await cancel_helper_fn(
                        ctx=ctx,
                        symbol=symbol,
                        sell_order_id=sell_order_id,
                        cancel_all_sides=True,
                    )

            latest_p = await ctx.client.get_current_price(symbol)
            if latest_p <= 0.0:
                latest_p = current_price
            exec_sell_price = math.floor(float(latest_p or 0.0) * 100.0) / 100.0

            try:
                ob = await ctx.client.get_orderbook(symbol)
                bids = (ob or {}).get("bids", [])
                if isinstance(bids, list) and bids and float(bids[0].get("price") or 0.0) > 0.0:
                    bid_1 = float(bids[0]["price"])
                    exec_sell_price = min(exec_sell_price, bid_1) if exec_sell_price > 0.0 else bid_1
            except Exception:
                pass

            if exec_sell_price <= 0.0:
                return buy_order_id, entry_session, True

            sold_dispatched = False
            if sellable_q_int > 0:
                dump_qty = min(int(holdings_qty), int(sellable_q_int))
                idem_key = f"WCUT_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
                res_sell = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=dump_qty,
                        price=exec_sell_price,
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                new_s_id = _extract_order_id_safe(res_sell)
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_s_id,
                    target_sell_price=exec_sell_price,
                    whipsaw_target_price=0.0,
                )
                sold_dispatched = True
            elif surviving_sell_ids:
                mod_res = await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=surviving_sell_ids[0],
                        price=exec_sell_price,
                        order_type="LIMIT",
                    ),
                    timeout=4.0,
                )
                new_mod_id = _extract_order_id_safe(mod_res) or str(surviving_sell_ids[0])
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_mod_id,
                    target_sell_price=exec_sell_price,
                    whipsaw_target_price=0.0,
                )
                sold_dispatched = True

            if sold_dispatched and not _WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False):
                _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
                await _notify_tg_safe(
                    ctx,
                    (
                        f"⏱️ <b>[04:07 타임쉴드 정리 {sym_icon}{symbol}] 휩소 노출 작전 1주 현재가 즉시 매도 집행</b>\n"
                        f"▫️ 사유: 04:07 EST 휩소 노출 작전 종료 시각 도달 (보유 <code>{holdings_qty}주</code> 정리)\n"
                        f"▫️ 처분 단가(LIMIT): <code>${exec_sell_price:.2f}</code> (현재가/매수1호가 즉시 체결 유도)\n"
                        "▫️ 후속 조치: <code>1주 체결 완료 즉시 실수익 복리 정산 후 본진(과열 시 과열차단 복원) 상태로 전환됩니다.</code>"
                    ),
                )
            return buy_order_id, entry_session, True
        except Exception as e:
            print(f"🚨 [04:07 휩소 1주 현재가 매도 방어 {symbol}] {e}", flush=True)
            return buy_order_id, entry_session, True
        finally:
            _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = time.time()
            ctx.in_memory_ordering_lock[symbol] = False

    return buy_order_id, entry_session, False


def __getattr__(name: str):
    """
    NEW: PEP 562 모듈 레벨 안전 폴백 — daymarket_sim_plugin 의 모든 속성·함수를 누락 없이 위임합니다.
    """
    if hasattr(_daysim_mod, name):
        return getattr(_daysim_mod, name)
    raise AttributeError(f"module 'whipsaw_plugin' has no attribute '{name}'")
