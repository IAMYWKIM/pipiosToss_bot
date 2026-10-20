# =====================================================================
# FILE: tg_ui_boards_plugin.py
# 목적: 듀얼 통합 지시서(/sync) 및 전술 제어반(/settlement) 초슬림 텔레그램 UI 렌더링 플러그인
#       (tg_ui_plugin.py 1/2 분할본)
#       — SOXS & SOXL 당일 세션(19:00 EST~) 1m 분봉 동적 고가/저가 집계(초과 Case 40)·환율·보유수익 지시서(build_sync_board) 및
#         마스터 전원·양방향 보유·이원화 트레일링(⚡반등 / 🚀진성)·15:00~15:58 VWAP탈출·
#         03:58~04:07 휩소 1주 타점 락온/접수 상태·프리장 첫 RSI>=79.9 상승주도주(👑) 자가 복원·
#         데이장 100% [모의] 출격 승률(%) 전술 제어반(build_settlement_board)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 tg_ui_plugin.py의 /sync 및 /settlement 보드 빌더를 tg_ui_boards_plugin.py로 무손실 분리
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 +0.64% / +1.00% 익절률 선택 버튼 및 데드코드 영구 소각
# MODIFIED: 초과 Case 13, 38, 40, 77 — 듀얼 지시서(/sync)에서 당일 세션(19:00 EST~) 1m 분봉 전체와 실시간 현재가를 동적 병합하여
#           전일 일봉 고/저가 오표출을 원천 차단하고, 모바일 2줄 이내 초슬림 레이아웃 유지
# MODIFIED: 초과 Case 78, 79, 80, 93, 94, 95, 97, 98, 99 — 양방향 동시 수동 보유 표출, 데이장 모의 전적(승률 %),
#           03:58~04:07 휩소 1주 사전 락온 타점 및 주문 접수 상태 구분 표출, 프리장 상승주도주(👑) 온디맨드 소급 복원 반영

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from toss_api import GlobalThrottle
from quant_engine import (
    AssassinLedger,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    calculate_rsi_1m,
    compute_realtime_tick_rsi,
)
from sniper_plugin import (
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    get_trailing_runtime_status,
    get_bottom_track_status,
    get_pre_leader_status,
    update_premarket_trend_leader,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")
NQ_DYNAMIC_FLOOR_UI = MIN_STAGE3_QQQ_ADVERSE_PCT

_AUTO_ENTRY_TAGS_UI = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_PIVOT_10M",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "PRE_PIVOT_10M_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)


async def _heal_pre_leader_from_api_if_needed(api_client, now_est: datetime):
    """
    초과 Case 97:
    04:07 ~ 08:14 EST 구간에 프로세스 재기동 직후 아직 _PRE_LEADER_CACHE["leader_sym"] 이 비어 있을 때,
    전일 19:00 EST 이후 다중 페이지 1분봉을 스캔하여 당일 04:07 EST 이후 첫 RSI >= 79.9 상승 주도주를 즉시 복원합니다.
    """
    est_hm = now_est.hour * 100 + now_est.minute
    if not (407 <= est_hm <= 814) or api_client is None:
        return
    if str(get_pre_leader_status().get("leader_sym") or ""):
        return

    if now_est.hour >= 19:
        session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
    else:
        session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

    temp_rsi_state = {"SOXS": {}, "SOXL": {}}
    for sym in ("SOXS", "SOXL"):
        all_c = []
        before = None
        for _ in range(4):
            try:
                page = await api_client.get_1m_candles_pagination(sym, count=200, before=before)
                c_list = (page or {}).get("candles", [])
                if not c_list:
                    break
                all_c.extend(c_list)
                oldest_dt = pd.to_datetime(c_list[-1]["timestamp"], utc=True).tz_convert(EST_TZ)
                if oldest_dt <= session_start_est:
                    break
                before = (page or {}).get("nextBefore")
                if not before:
                    break
            except Exception:
                break
        if all_c:
            try:
                r_v, v_v, ewm_v = await asyncio.to_thread(calculate_rsi_1m, all_c, now_est)
                temp_rsi_state[sym] = {"value": r_v, "vwap": v_v, "ewm_state": ewm_v}
            except Exception:
                pass

    for sym in ("SOXS", "SOXL"):
        r_now = float((temp_rsi_state.get(sym) or {}).get("value", 0.0))
        update_premarket_trend_leader(
            symbol=sym,
            live_rsi_val=r_now,
            now_est=now_est,
            shared_rsi_state=temp_rsi_state,
        )


# MODIFIED: 듀얼 지시서(/sync) 당일 1m 분봉 동적 고가/저가 집계(초과 Case 40) 및 상승주도주·휩소 타점·이원화 트레일링·VWAP 탈출 표시 (Case 38, 40, 78, 79, 93, 97, 98, 99)
async def build_sync_board(api_client) -> str:
    now_est = datetime.now(EST_TZ)
    wk = now_est.weekday()
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_str = "🌞 서머타임" if is_dst else "❄️ 서머타임 OFF"
    t = now_est.hour * 100 + now_est.minute
    is_weekend_closed = (wk == 4 and t >= 1700) or (wk == 5) or (wk == 6 and t < 1800)

    if is_weekend_closed:
        market_state = "🌙 주말/휴장일"
    elif 1800 <= t <= 1859:
        market_state = "🌐 선물 신세션 개장"
    elif 358 <= t <= 406:
        market_state = "🌪️ 03:58 ~ 04:07 휩소 작전"
    elif 407 <= t <= 814:
        market_state = "🔫 프리장 본진 가동 (👑HYB_35_1ST)"
    elif 815 <= t <= 929:
        market_state = "🔒 프리장 마감"
    elif 1500 <= t <= 1558:
        market_state = "🛟 정규장 마감 1시간전 (VWAP탈출)"
    elif 930 <= t <= 1559:
        market_state = "🔥 정규장 (신규매수 마감)"
    elif 1600 <= t <= 1759:
        market_state = "🛑 애프터마켓"
    else:
        market_state = "🌙 데이마켓 (🔔100% [모의] 추격)"

    bp = await api_client.get_usd_buying_power()
    exchange_rate = 1400.0
    try:
        ex_data = await api_client._request(
            "GET",
            "/api/v1/exchange-rate?baseCurrency=USD&quoteCurrency=KRW",
            "MARKET_INFO",
            headers=api_client._get_headers(),
        )
        exchange_rate = float((ex_data or {}).get("result", {}).get("rate", 1400.0))
    except Exception:
        pass

    if now_est.hour >= 19:
        session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
        logical_today = (now_est + timedelta(days=1)).date()
    else:
        session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)
        logical_today = now_est.date()

    sync_rsi_holder = {"SOXS": {}, "SOXL": {}}

    async def get_symbol_sync_data(symbol: str):
        state = await AssassinLedger.get_state(symbol)
        buy_id = await AssassinLedger.get_buy_order_id(symbol)
        is_comp = await AssassinLedger.get_is_compound(symbol)
        budget = state[1]
        target_sell_p = float(state[6] or 0.0)
        entry_session = state[8]
        hold = await api_client.get_symbol_holdings_detail(symbol)
        qty, avg_price = hold.get("qty", 0.0), hold.get("avg_price", 0.0)
        profit_usd, profit_rate = hold.get("profit_usd", 0.0), hold.get("profit_rate", 0.0) * 100.0
        profit_krw = profit_usd * exchange_rate
        curr = await api_client.get_current_price(symbol)
        trail_st = get_trailing_runtime_status(symbol)
        bot_st = get_bottom_track_status(symbol)

        prev_close = curr
        high, low = curr, curr

        # 1. 직전 영업일 정규장 마감 종가(prev_close) 추출
        try:
            data_1d = await api_client._request(
                "GET",
                f"/api/v1/candles?symbol={symbol}&interval=1d&count=5",
                "MARKET_DATA_CHART",
                headers=api_client._get_headers(),
            )
            candles_1d = (data_1d or {}).get("result", {}).get("candles", [])
            for c in candles_1d:
                c_dt = pd.to_datetime(c["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ).date()
                if c_dt < logical_today:
                    pc_val = float(c.get("closePrice") or 0.0)
                    if pc_val > 0.0:
                        prev_close = pc_val
                        break
        except Exception:
            pass

        # 2. 초과 Case 40: 당일 세션(19:00 EST~) 1m 분봉 페이지네이션 동적 고가/저가 Aggregation
        session_candles_1m = []
        before_cursor = None
        for _ in range(6):
            try:
                page_1m = await api_client.get_1m_candles_pagination(symbol, count=200, before=before_cursor)
                c_list = (page_1m or {}).get("candles", [])
                if not c_list:
                    break
                session_candles_1m.extend(c_list)
                oldest_dt = pd.to_datetime(c_list[-1]["timestamp"], utc=True).tz_convert(EST_TZ)
                if oldest_dt <= session_start_est:
                    break
                before_cursor = (page_1m or {}).get("nextBefore")
                if not before_cursor:
                    break
            except Exception:
                break

        if session_candles_1m:
            try:
                r_c, v_c, ewm_c = await asyncio.to_thread(calculate_rsi_1m, session_candles_1m, now_est)
                if curr > 0.0 and ewm_c:
                    r_c = compute_realtime_tick_rsi(curr, ewm_c, fallback_rsi=r_c)
                sync_rsi_holder[symbol] = {"value": r_c, "vwap": v_c, "ewm_state": ewm_c}
            except Exception:
                pass

            session_highs = [curr] if curr > 0.0 else []
            session_lows = [curr] if curr > 0.0 else []
            for c_item in session_candles_1m:
                try:
                    c_ts = pd.to_datetime(c_item["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ)
                    if c_ts >= session_start_est:
                        h_v = float(c_item.get("highPrice") or 0.0)
                        l_v = float(c_item.get("lowPrice") or 0.0)
                        if h_v > 0.0:
                            session_highs.append(h_v)
                        if l_v > 0.0:
                            session_lows.append(l_v)
                except Exception:
                    pass
            if session_highs:
                high = max(session_highs)
            if session_lows:
                low = min(session_lows)

        high_rate = ((high - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
        low_rate = ((low - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

        return {
            "symbol": symbol, "budget": budget, "is_compound": is_comp,
            "target_sell_p": target_sell_p, "buy_id": buy_id, "trail_st": trail_st, "bot_st": bot_st,
            "entry_session": entry_session, "curr": curr, "avg_price": avg_price, "qty": qty,
            "high": high, "high_rate": high_rate, "low": low, "low_rate": low_rate,
            "profit_rate": profit_rate, "profit_usd": profit_usd, "profit_krw": profit_krw,
        }

    soxs_data, soxl_data = await asyncio.gather(
        get_symbol_sync_data("SOXS"),
        get_symbol_sync_data("SOXL"),
    )

    if 407 <= t <= 814:
        for sym_k in ("SOXS", "SOXL"):
            r_k = float((sync_rsi_holder.get(sym_k) or {}).get("value", 0.0))
            update_premarket_trend_leader(
                symbol=sym_k,
                live_rsi_val=r_k,
                now_est=now_est,
                shared_rsi_state=sync_rsi_holder,
            )

    pre_ld = get_pre_leader_status()
    pre_leader_sym = str(pre_ld.get("leader_sym") or "")
    day_sim = get_daymarket_sim_status()
    day_leader_sym = str(day_sim.get("leader_sym") or "")
    whip_st = get_whipsaw_runtime_status()

    def format_symbol(d: dict) -> str:
        sym = d["symbol"]
        icon = "🐻" if sym == "SOXS" else "🐂"
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        comp_label = "📈 통합복리시드" if d["is_compound"] else "💵 단리시드"
        tr_st = d["trail_st"]
        bt_st = d["bot_st"]
        es_str = str(d["entry_session"] or "")
        bid_str = str(d.get("buy_id") or "")
        is_man = es_str not in _AUTO_ENTRY_TAGS_UI
        if d["qty"] > 0:
            if es_str == "WHIPSAW_1SH":
                flag_str = "🌪️ 휩소 1주 보유"
            elif tr_st.get("armed", False):
                pk_p = float(tr_st.get("peak_price", 0.0))
                st_p = float(tr_st.get("stop_price", d["target_sell_p"]))
                reg_tag = "🚀진성트레일링" if tr_st.get("exit_regime") == "PEAK_LCK_130" else "⚡반등트레일링"
                flag_str = f"{reg_tag} (고점 ${pk_p:.2f} / 스탑 ${st_p:.2f})"
            elif 1500 <= t <= 1558:
                flag_str = f"🛟 VWAP탈출감시 (점화 ${d['target_sell_p']:.2f})"
            elif is_man:
                flag_str = f"🎯 수동보유 (점화 ${d['target_sell_p']:.2f})"
            else:
                flag_str = f"🎯 보유 (점화 ${d['target_sell_p']:.2f})"
        elif bid_str and es_str == "WHIPSAW_1SH":
            flag_str = f"🌪️ 휩소1주 매수덫 접수 (${whip_st['buy_price']:.2f})"
        elif (358 <= t <= 406) and whip_st.get("target_sym") == sym and float(whip_st.get("buy_price", 0.0)) > 0.0:
            flag_str = f"🌪️ 휩소타점락온 (${whip_st['buy_price']:.2f} ➔ ${whip_st['sell_target']:.2f})"
        elif (407 <= t <= 814) and bt_st.get("active", False):
            lp_b = float(bt_st.get("post_stop_low", 0.0))
            rp_b = float(bt_st.get("rebound_trig_price", 0.0))
            flag_str = f"🪤 최저점추적 (${lp_b:.2f} ➔ ${rp_b:.2f})"
        elif (407 <= t <= 814) and pre_leader_sym == sym:
            flag_str = "👑 상승주도(RSI≤35)" if t >= 700 else "👑 상승각인(07시 대기)"
        elif (t >= 1900 or t <= 357) and day_leader_sym == sym:
            flag_str = "👑 [모의] 상승장락온(RSI≤35)"
        elif is_weekend_closed:
            flag_str = "🌙 주말 휴장"
        elif 1800 <= t <= 1859:
            flag_str = "⏳ 데이장 대기"
        elif 815 <= t <= 1759:
            flag_str = "🔒 신규진입 마감"
        else:
            flag_str = "⏳ 대기"
        return (
            f"{icon} <b>[{sym}] 듀얼 스나이퍼 지시서 (👑HYB_35_1ST)</b>\n"
            f"{comp_label}: <code>${d['budget']:,.2f}</code> | {flag_str}\n"
            f"💰 현재 ${d['curr']:.2f} / 평단 ${d['avg_price']:.2f} ({int(d['qty'])}주)\n"
            f"📈 금일 고가: ${d['high']:.2f} ({d['high_rate']:+.2f}%)\n"
            f"📉 금일 저가: ${d['low']:.2f} ({d['low_rate']:+.2f}%)\n"
            f"🔺 수익: {d['profit_rate']:+.2f}% ({profit_sign}${abs(d['profit_usd']):,.2f} | {profit_sign}₩{int(abs(d['profit_krw'])):,})"
        )

    text = (
        f"📜 <b>[ SOXS &amp; SOXL 듀얼 지시서 ({market_state}) ]</b>\n"
        f"📅 {dst_str} ({now_est.strftime('%H:%M')} EST)\n"
        f"💵 주문가능금액: ${bp:,.2f}\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        f"{format_symbol(soxs_data)}\n\n"
        f"{format_symbol(soxl_data)}\n\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "▶️ /avwap : 🔫 듀얼 스나이퍼 레이더 관제탑"
    )
    return text


# MODIFIED: 전술 제어반(/settlement) 슬림화, 프리장 상승주도주(👑) 자가 복원, 03:58~04:07 휩소 타점 사전 락온/주문 접수 구분 표출, 양방향 동시 보유 표출, 데이장 모의 전적(승률 %), 이원화 트레일링 및 15:00 VWAP 탈출 표시 (Case 37, 77, 78, 79, 93, 94, 97, 98, 99)
async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    now_kst = now_est.astimezone(KST_TZ)
    wk = now_est.weekday()
    t = now_est.hour * 100 + now_est.minute
    kst_t = now_kst.hour * 100 + now_kst.minute
    is_weekend_closed = (wk == 4 and t >= 1700) or (wk == 5) or (wk == 6 and t < 1800)

    state_s, state_l = await asyncio.gather(
        AssassinLedger.get_state("SOXS"),
        AssassinLedger.get_state("SOXL"),
    )
    buy_id_s, buy_id_l = await asyncio.gather(
        AssassinLedger.get_buy_order_id("SOXS"),
        AssassinLedger.get_buy_order_id("SOXL"),
    )
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")

    if (407 <= t <= 814) and not str(get_pre_leader_status().get("leader_sym") or ""):
        await _heal_pre_leader_from_api_if_needed(getattr(GlobalThrottle, "active_client", None), now_est)

    day_sim = get_daymarket_sim_status()
    whip_st = get_whipsaw_runtime_status()
    bot_all = get_bottom_track_status()
    pre_ld = get_pre_leader_status()
    ld_sym = str(pre_ld.get("leader_sym") or "")

    budget_s = float(state_s[1] or 100.0)
    act_s = bool(state_s[5])

    active_pos_syms = []
    for sym_chk, st_chk, bid_chk in (("SOXS", state_s, buy_id_s), ("SOXL", state_l, buy_id_l)):
        if float(st_chk[0] or 0.0) > 0.0 or float(st_chk[6] or 0.0) > 0.0 or bool(st_chk[7]) or bool(bid_chk):
            active_pos_syms.append((sym_chk, st_chk, bid_chk))

    act_s_str = "🟢 ON" if act_s else "🔴 OFF"
    comp_s_str = "📈 통합 복리" if is_comp_s else "💵 단리 고정"
    rate_mode_desc = "🔥 +1.00% 점화 이원화 트레일링 (👑HYB_35_1ST)"

    def _format_single_pos_status(sym_a: str, st_a: tuple, bid_a: str) -> str:
        tp_a = float(st_a[6] or 0.0)
        es_a = str(st_a[8] or "")
        tr_a = get_trailing_runtime_status(sym_a)
        is_man_a = (not bid_a and es_a not in _AUTO_ENTRY_TAGS_UI)
        if bid_a:
            if es_a == "WHIPSAW_1SH":
                return f"🌪️ [{sym_a}] 휩소 1주 매수덫 접수 (${whip_st['buy_price']:.2f} ➔ ${whip_st['sell_target']:.2f})"
            elif es_a in ("PRE_PIVOT_10M", "PRE_PIVOT_10M_REARM"):
                return f"🎯 [{sym_a}] 10분꺾임 매수덫 대기중"
            elif es_a in ("PRE_PULLBACK", "PRE_PULLBACK_REARM"):
                return f"🚀 [{sym_a}] 추세눌림 매수덫 대기중"
            elif es_a == "PRE_SNIPER":
                return f"🪤 [{sym_a}] 1차 매수덫 대기중"
            return f"⚡ [{sym_a}] 재장전 체결 대기중"
        if es_a == "WHIPSAW_1SH":
            return f"🌪️ [{sym_a}] 휩소 1주 보유 (목표 ${tp_a:.2f})"
        if tr_a.get("armed", False):
            pk_a = float(tr_a.get("peak_price", 0.0))
            sp_a = float(tr_a.get("stop_price", tp_a))
            reg_lbl = "진성추세" if tr_a.get("exit_regime") == "PEAK_LCK_130" else "기술적반등"
            m_lbl = f"수동·{sym_a}" if is_man_a else f"{sym_a}·{reg_lbl}"
            return f"🔥 [{m_lbl}] 트레일링 (고점 ${pk_a:.2f} / 스탑 ${sp_a:.2f})"
        if 1500 <= t <= 1558:
            m_lbl = f"수동·{sym_a}" if is_man_a else sym_a
            return f"🛟 [{m_lbl}] 장마감 1시간전 VWAP탈출 감시중"
        if tp_a > 0.0:
            m_lbl = f"수동·{sym_a}" if is_man_a else sym_a
            return f"🎯 [{m_lbl}] 보유 (점화 ${tp_a:.2f})"
        m_lbl = f"수동·{sym_a}" if is_man_a else sym_a
        return f"🎯 [{m_lbl}] 보유중 (안전캡 장전중)"

    if not act_s:
        strike_status_str = "⏸️ 가동 OFF (수동 대기)"
    elif 1559 <= t <= 1601 and not is_weekend_closed:
        strike_status_str = "🛑 제로-오버나이트 청산중"
    elif active_pos_syms:
        strike_status_str = " / ".join(
            _format_single_pos_status(sym_a, st_a, bid_a)
            for sym_a, st_a, bid_a in active_pos_syms
        )
    elif is_weekend_closed:
        strike_status_str = "🌙 주말 휴장"
    elif 1800 <= t <= 1859:
        pre_s = str(day_sim.get("prelock_sym") or "")
        if pre_s:
            strike_status_str = f"🔍 NQ 10분봉 1차락온 ({pre_s} · 개시7분 RSI≥50 대기)"
        else:
            strike_status_str = "🌐 선물 신세션 개장 (19:00 데이장 대기)"
    elif 358 <= t <= 406:
        w_sym = str(whip_st.get("target_sym") or "")
        w_bp = float(whip_st.get("buy_price", 0.0))
        w_tp = float(whip_st.get("sell_target", 0.0))
        if w_sym and w_bp > 0.0:
            if t < 400:
                strike_status_str = f"⏳ [{w_sym}] 휩소타점 락온 (${w_bp:.2f} ➔ 04:00 개장발사 대기)"
            else:
                strike_status_str = f"🌪️ [{w_sym}] 휩소 매수덫 발사중 (${w_bp:.2f} ➔ ${w_tp:.2f})"
        else:
            strike_status_str = "🌪️ 휩소 노출 작전중 (03:58 ~ 04:07 · VWAP 스캔중)"
    elif (state_s[8] == "CANCELED_MACRO" or state_l[8] == "CANCELED_MACRO") and (407 <= t <= 1759):
        strike_status_str = "🛑 매크로 과열방어 (자동마감)"
    elif (state_s[8] == "VWAP_ESCAPE" or state_l[8] == "VWAP_ESCAPE") and (1500 <= t <= 1759):
        strike_status_str = "🛟 장마감 1시간전 VWAP 탈출 완료"
    elif 407 <= t <= 814:
        if bot_all.get("active", False):
            b_sym = str(bot_all.get("symbol", "SOXS"))
            lp_b = float(bot_all.get("post_stop_low", 0.0))
            rp_b = float(bot_all.get("rebound_trig_price", 0.0))
            strike_status_str = f"🪤 [{b_sym}] 최저점추적 (${lp_b:.2f} ➔ ${rp_b:.2f})"
        elif not bool(state_s[4]) and not bool(state_l[4]):
            if ld_sym:
                strike_status_str = f"🔫 프리장 감시중 (👑상승주도: {ld_sym})"
            else:
                strike_status_str = "🔫 프리장 본진 감시중 (04:07 ~ 08:14)"
        else:
            strike_status_str = "🔒 프리장 타격 잠금"
    elif 815 <= t <= 1558:
        if t < 930:
            strike_status_str = "🔒 08:15 마감 (관망)"
        else:
            strike_status_str = "🔒 정규장 마감 (관망)"
    elif t >= 1900 or t <= 357:
        if day_sim.get("active", False):
            sim_sym = day_sim.get("symbol", "SOXS")
            if day_sim.get("phase") == "TRAP_WAIT":
                strike_status_str = f"🪤 [모의·{sim_sym}] 1차덫 대기 (${day_sim['trap_price']:.2f})"
            elif day_sim.get("trail_armed", False):
                strike_status_str = (
                    f"🔥 [모의·{sim_sym}] 1.3%락온 (고점 ${day_sim['max_price']:.2f} / 스탑 ${day_sim['target_price']:.2f})"
                )
            else:
                strike_status_str = (
                    f"🚀 [모의·{sim_sym}] 추격중 (진입 ${day_sim['entry_price']:.2f} ➔ 락온 ${day_sim['arm_price']:.2f})"
                )
        elif (900 <= kst_t <= 906) or (1900 <= t <= 1906):
            pre_s = str(day_sim.get("prelock_sym") or "대기")
            strike_status_str = f"🚀 [모의] 개시7분 ({pre_s} RSI≥50 즉시진입 감시)"
        else:
            d_tot = int(day_sim.get("stat_total", 0))
            d_win = int(day_sim.get("stat_wins", 0))
            d_wr = float(day_sim.get("stat_win_rate", 0.0))
            d_ld = str(day_sim.get("leader_sym") or "")
            ld_tag = f" · 👑상승장:{d_ld}" if d_ld else ""
            if d_tot > 0:
                strike_status_str = f"🌙 [모의] 감시중 ({d_tot}출격 {d_win}승 {d_wr:.0f}%{ld_tag})"
            else:
                strike_status_str = f"🌙 [모의] 데이장 감시중{ld_tag}"
    else:
        strike_status_str = "🛑 애프터마켓 (마감)"

    text = (
        "⚙️ <b>[ 듀얼 스나이퍼 전술 제어반 ]</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        f"▫️ 마스터 전원: <code>{act_s_str}</code>\n"
        f"▫️ 출동 상태: <code>{strike_status_str}</code>\n"
        f"▫️ 익절 엔진: <code>{rate_mode_desc}</code>\n"
        f"▫️ 운용 방식: <code>{comp_s_str}</code>\n"
        f"▫️ 통합 시드: <code>${budget_s:,.2f}</code>"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔴 듀얼 스나이퍼 정지" if act_s else "🟢 듀얼 스나이퍼 가동",
                    callback_data="toggle_set_act_SOXS",
                )
            ],
            [
                InlineKeyboardButton(
                    text="💵 단리(시드고정) 전환" if is_comp_s else "📈 통합복리(실수익가감) 전환",
                    callback_data="toggle_compound_SOXS",
                ),
                InlineKeyboardButton(text="💵 통합 시드 설정", callback_data="set_budget_SOXS"),
            ],
            [
                InlineKeyboardButton(text="🔫 스나이퍼 레이더", callback_data="open_avwap"),
                InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main"),
            ],
        ]
    )
    return text, keyboard
