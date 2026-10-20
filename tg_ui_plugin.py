# =====================================================================
# FILE: tg_ui_plugin.py
# 목적: 메인 메뉴(/start), 세션/5MA 파서, 2줄 고정 초슬림 상태 빌더 및
#       스나이퍼 레이더 관제탑(/avwap) 텔레그램 UI 렌더링 코어
#       (tg_ui_plugin.py 2/2 분할본 · tg_ui_boards_plugin 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 /sync 및 /settlement 보드 빌더를 tg_ui_boards_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 초과 Case 13, 38, 77 — 메인 메뉴(/start) 및 관제탑(/avwap)에서 모든 항목이 모바일 화면 2줄을 넘어가지 않도록 초슬림화
# MODIFIED: 초과 Case 90 & Case 91 — 17:00 ~ 17:59 EST(06:00 ~ 06:59 KST) 선물 일일 마감 휴장 및 주말 휴장 구간에서
#           직전 마감된 선물 세션 기준일과 '(마감 시세 · 18:00 신세션 대기)' 라벨 및 마감가를 명확히 표출
# MODIFIED: 초과 Case 78, 92, 93, 94, 97, 98, 99 — 데이장 개시 7분 추세·1.3% 락온·40~70 리셋·당일 모의 출격 승률,
#           SOXS·SOXL 동시 수동 보유 시 3차 방어 라인 독립 표출, 선택지 B(👑HYB_35_1ST), 이원화 매도 및 15:00 VWAP 탈출 상태 표출
# NEW: 초과 Case 101 — 옵션 A (🚀 08_HYB_PIVBRK & 데이장 🎯 PIV10) 10분봉 대파동 스윙·꺾임목(PRE_PIVOT_10M / PIV10) 상태 표출 결합

import math
import asyncio
from datetime import datetime, timedelta
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    get_cme_session_window_est,
)
from sniper_plugin import (
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    get_trailing_runtime_status,
    get_bottom_track_status,
    get_pre_leader_status,
    get_swing_10m_status,
)
from tg_ui_boards_plugin import (
    EST_TZ,
    KST_TZ,
    NQ_DYNAMIC_FLOOR_UI,
    _AUTO_ENTRY_TAGS_UI,
    build_sync_board,
    build_settlement_board,
)


# MODIFIED: 메인 메뉴(/start 및 메인 복귀) 화면 불필요 설명 삭제 및 1줄 고정 슬림화 (Case 38, 77, 78, 93, 97, 98, 99, 101)
async def get_main_menu_text() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_status_text = "🌞서머타임 ON (EDT)" if is_dst else "❄️서머타임 OFF (EST)"

    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    comp_mode_str = "통합 복리 ON" if is_comp_s else "단리 ON"

    return (
        f"🕒 <b>[ 듀얼 스나이퍼 ({dst_status_text}) ]</b>\n"
        f"▫️ 익절·복구: <b>🔥 +1.00% 점화 이원화 트레일링 ({comp_mode_str})</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 18:00 ~ 18:59: 🌐 <b>선물 신세션 &amp; 10분봉 락온</b>\n"
        "🔹 19:00 ~ 03:57: 🌙 <b>[모의] 데이장 (개시7분·반복·🎯PIV10)</b>\n"
        "🔹 03:58 ~ 03:59: 🔇 <b>휩소 묵음</b>\n"
        "🔹 03:58 ~ 04:07: 🌪️ <b>'휩소 노출 작전'</b>\n"
        "🔹 04:07 ~ 08:14: 🔫 <b>프리장 본진 (👑HYB_35_1ST·🚀PIV10)</b>\n"
        "🔹 08:15 ~ 14:59: 🔒 <b>정규장 본장</b>\n"
        "🔹 15:00 ~ 15:58: 🛟 <b>미익절 VWAP 탈출</b>\n"
        "🔹 15:59 ~ 16:01: 🛑 <b>제로-오버나이트</b>\n"
        "🔹 17:00: 🧹 <b>가비지 컬렉션(GC)</b>\n\n"
        "🛠 <b>[ 핵심 명령어 ]</b>\n"
        "▶️ /avwap : 🔫 스나이퍼 레이더 관제탑\n"
        "▶️ /sync : 📜 듀얼 지시서 동기화\n"
        "▶️ /settlement : ⚙️ 전술 제어반\n\n"
        "⚠️ /reset : 🧹 듀얼 장부 0점 소각\n\n"
        "⚠️ /update : 🚀 시스템 자가 업데이트\n\n"
        "🌙 <b>수동 오버나이트를 원할 경우 [전술 제어반]에서 가동을 OFF 해주세요.</b>"
    )


def parse_session_data(all_candles: list, session_start_est: datetime) -> dict:
    res = {
        "pre_h": 0.0, "pre_l": 0.0, "pre_amp": 0.0, "pre_vwap": 0.0, "pre_body": 0.0,
        "reg_h": 0.0, "reg_l": 0.0, "reg_amp": 0.0, "reg_vwap": 0.0, "reg_body": 0.0,
    }
    if not all_candles:
        return res
    df = pd.DataFrame(all_candles)
    if df.empty:
        return res
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df.sort_index(ascending=True, inplace=True)

    for col in ["openPrice", "highPrice", "lowPrice", "closePrice", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[df.index >= session_start_est]
    if df.empty:
        return res

    pre_df = df.between_time("04:00", "09:29")
    reg_df = df.between_time("09:30", "16:00")

    def calc_metrics(sub_df: pd.DataFrame):
        if sub_df.empty:
            return 0.0, 0.0, 0.0, 0.0, 0.0
        h = float(sub_df["highPrice"].max())
        l = float(sub_df["lowPrice"].min())
        amp = ((h - l) / l * 100.0) if l > 0.0 else 0.0
        o = float(sub_df["openPrice"].iloc[0])
        c = float(sub_df["closePrice"].iloc[-1])
        body = ((c - o) / o * 100.0) if o > 0.0 else 0.0
        tp = (sub_df["highPrice"] + sub_df["lowPrice"] + sub_df["closePrice"]) / 3.0
        pv = tp * sub_df["volume"]
        vol = float(sub_df["volume"].sum())
        vwap = float(pv.sum() / vol) if vol > 0.0 else 0.0
        return h, l, float(amp), vwap, float(body)

    pre_h, pre_l, pre_amp, pre_vwap, pre_body = calc_metrics(pre_df)
    reg_h, reg_l, reg_amp, reg_vwap, reg_body = calc_metrics(reg_df)
    res.update({
        "pre_h": pre_h, "pre_l": pre_l, "pre_amp": pre_amp, "pre_vwap": pre_vwap, "pre_body": pre_body,
        "reg_h": reg_h, "reg_l": reg_l, "reg_amp": reg_amp, "reg_vwap": reg_vwap, "reg_body": reg_body,
    })
    return res


async def _fetch_5ma_amp(api_client, symbol: str) -> tuple[float, float, float]:
    avg_amp, yesterday_amp, yesterday_return = 0.0, 0.0, 0.0
    try:
        endpoint = f"/api/v1/candles?symbol={symbol}&interval=1d&count=6"
        data = await api_client._request("GET", endpoint, "MARKET_DATA_CHART", headers=api_client._get_headers())
        candles = (data or {}).get("result", {}).get("candles", [])
        if not candles:
            return 0.0, 0.0, 0.0

        c0_dt = pd.to_datetime(candles[0]["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ).date()
        now_est_check = datetime.now(EST_TZ)
        today_dt = now_est_check.date()

        if c0_dt > today_dt:
            valid_candles = candles[1:6]
        elif c0_dt == today_dt:
            if 16 <= now_est_check.hour < 19:
                valid_candles = candles[0:5]
            else:
                valid_candles = candles[1:6]
        else:
            valid_candles = candles[0:5]

        amps = []
        for i, c in enumerate(valid_candles):
            h = float(c.get("highPrice") or 0.0)
            l = float(c.get("lowPrice") or 0.0)
            if l > 0.0:
                amp = (h - l) / l * 100.0
                amps.append(amp)
                if i == 0:
                    yesterday_amp = amp

        if len(valid_candles) >= 2:
            yest_cls = float(valid_candles[0].get("closePrice") or 0.0)
            prev_cls = float(valid_candles[1].get("closePrice") or 0.0)
            if prev_cls > 0.0:
                yesterday_return = ((yest_cls - prev_cls) / prev_cls) * 100.0

        if amps:
            avg_amp = sum(amps) / len(amps)
        return avg_amp, yesterday_amp, yesterday_return
    except Exception:
        return 0.0, 0.0, 0.0


async def _fetch_session_stats(api_client, symbol: str, now_est: datetime) -> dict:
    all_candles = []
    before = None
    if now_est.hour >= 19:
        session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
    else:
        session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

    for _ in range(8):
        try:
            data = await api_client.get_1m_candles_pagination(symbol, count=200, before=before)
            candles = data.get("candles", [])
            all_candles.extend(candles)
            if not candles:
                break
            oldest_time = pd.to_datetime(candles[-1]["timestamp"], utc=True).tz_convert(EST_TZ)
            if oldest_time <= session_start_est:
                break
            before = data.get("nextBefore")
            if not before:
                break
        except Exception:
            break
    return await asyncio.to_thread(parse_session_data, all_candles, session_start_est)


# MODIFIED: Case 13 & 초과 Case 38, 50, 57, 78, 83, 84, 85, 86, 87, 89, 90, 91, 92, 93, 94, 97, 98, 99, 101 —
#           하단 종목 상태가 2줄을 넘어가지 않도록 간결화 및 데이장 개시 7분 추세·1.3% 락온·40~70 리셋·10분봉 꺾임목·이원화 트레일링·15:00 VWAP 탈출 표시 반영
def build_compact_status(
    symbol: str,
    state_tuple: tuple,
    buy_order_id: str,
    current_session: str,
    est_time_int: int,
    qty: float,
    opp_qty: float = 0.0,
    opp_buy_id: str = "",
    is_compound: bool = True,
    ha_bull: bool = True,
    reg_vwap: float = 0.0,
    kst_time_int: int = -1,
) -> str:
    is_done = state_tuple[4]
    is_active = state_tuple[5]
    target_sell_price = state_tuple[6]
    entry_session = state_tuple[8]
    state_flag = "ON" if is_active else "OFF"
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    day_sim = get_daymarket_sim_status()
    whip_st = get_whipsaw_runtime_status()
    trail_st = get_trailing_runtime_status(symbol)
    bot_st = get_bottom_track_status(symbol)
    pre_ld = get_pre_leader_status()
    sw_st = get_swing_10m_status(symbol)
    nq_bypass_on = bool(trail_st.get("nq_vel_bypass", False))
    exit_reg_str = str(trail_st.get("exit_regime", "OLD_IMM_CB"))

    if kst_time_int < 0:
        kst_time_int = datetime.now(KST_TZ).hour * 100 + datetime.now(KST_TZ).minute
    in_day_open_7m = (900 <= kst_time_int <= 906) or (1900 <= est_time_int <= 1906)

    if ha_bull:
        ha_badge = "🕯️HA양봉"
    elif nq_bypass_on:
        ha_badge = "🚀HA바이패스"
    else:
        ha_badge = "🌑HA음봉"

    is_manual_hold = (
        qty > 0
        and not buy_order_id
        and entry_session not in _AUTO_ENTRY_TAGS_UI
    )

    if not is_active:
        state_text = "⏸️ 가동OFF(수동대기)"
    elif qty > 0:
        if entry_session == "WHIPSAW_1SH":
            if target_sell_price > 0.0:
                state_text = f"🌪️ 휩소보유(1주 · 목표 ${target_sell_price:.2f})"
            else:
                state_text = "🌪️ 휩소보유(1주 · 장전중)"
        elif trail_st.get("armed", False):
            pk_p = float(trail_st.get("peak_price", 0.0))
            st_p = float(tr_st_stop := trail_st.get("stop_price", target_sell_price))
            reg_lbl = "🚀진성·10분트레일링" if exit_reg_str == "PEAK_LCK_130" else "⚡반등트레일링"
            prefix_tag = f"🔥 수동·{reg_lbl}" if is_manual_hold else reg_lbl
            state_text = f"{prefix_tag}({int(qty)}주 · 고점 ${pk_p:.2f} / 스탑 ${st_p:.2f})"
        elif (1500 <= est_time_int <= 1558) and reg_vwap > 0.0:
            vwap_fl = math.floor(reg_vwap * 100.0) / 100.0
            state_text = f"🛟 VWAP탈출감시({int(qty)}주 · VWAP ${vwap_fl:.2f})"
        elif target_sell_price > 0.0:
            if is_manual_hold:
                prefix_tag = "🎯 수동보유"
            elif entry_session in ("PRE_PIVOT_10M", "PRE_PIVOT_10M_REARM"):
                prefix_tag = "🎯 10분꺾임보유"
            elif entry_session in ("PRE_PULLBACK", "PRE_PULLBACK_REARM"):
                prefix_tag = "🚀 추세보유"
            else:
                prefix_tag = "🎯 대칭보유"
            state_text = f"{prefix_tag}({int(qty)}주 · 점화 ${target_sell_price:.2f} · {ha_badge})"
        else:
            prefix_tag = "🎯 수동보유" if is_manual_hold else "🎯 보유"
            state_text = f"{prefix_tag}({int(qty)}주 · 안전캡 장전중)"
    elif buy_order_id:
        if entry_session == "WHIPSAW_1SH":
            w_bp = whip_st.get("buy_price", 0.0)
            state_text = f"🌪️ 휩소1주 매수덫(${w_bp:.2f})"
        elif entry_session == "PRE_PIVOT_10M":
            state_text = "🎯 10분꺾임 매수덫(30초 쉴드)"
        elif entry_session == "PRE_PIVOT_10M_REARM":
            state_text = "🎯 10분꺾임 재장전 대기"
        elif entry_session == "PRE_PULLBACK":
            state_text = "🚀 추세눌림 매수덫(30초 쉴드)"
        elif entry_session == "PRE_PULLBACK_REARM":
            state_text = "🚀 추세눌림 재장전 대기"
        elif entry_session == "PRE_SNIPER":
            state_text = "🪤 1차 매수덫(30초 쉴드)"
        else:
            state_text = "⚡ 재장전 매수 대기"
    elif opp_qty > 0 or bool(opp_buy_id):
        state_text = f"🔒 대기({opp_sym} 운용중)"
    elif current_session == "HOLIDAY":
        state_text = "🌙 휴장(관망)"
    elif current_session == "cmeOpenWait" or (1800 <= est_time_int <= 1859 and current_session != "HOLIDAY"):
        if day_sim.get("prelock_sym") == symbol:
            state_text = "🔍 10분봉 1차락온(개시7분 RSI≥50 대기)"
        else:
            state_text = "⏳ 데이장 대기(19:00 개장)"
    elif entry_session == "CANCELED_MACRO" and (400 <= est_time_int <= 1759):
        state_text = "🛑 매크로방어(자동마감)"
    elif entry_session == "VWAP_ESCAPE" and (1500 <= est_time_int <= 1759):
        state_text = "🛟 VWAP탈출 완료(마감)"
    elif 358 <= est_time_int <= 406:
        if whip_st.get("target_sym") == symbol and whip_st.get("locked_vwap", 0.0) > 0.0:
            state_text = f"🌪️ 휩소타겟(${whip_st['buy_price']:.2f} ➡️ ${whip_st['sell_target']:.2f})"
        else:
            state_text = "🌪️ 휩소구간(04:07 쉴드)"
    elif current_session == "dayMarket" and (est_time_int >= 1900 or est_time_int <= 357):
        if day_sim.get("active", False) and day_sim.get("symbol") == symbol:
            sim_mode = str(day_sim.get("trig_mode") or "OPTA")
            if day_sim.get("phase") == "TRAP_WAIT":
                state_text = f"🪤 [모의·{sim_mode}] 1차덫(${day_sim['trap_price']:.2f})"
            elif day_sim.get("trail_armed", False):
                pk_d = float(day_sim.get("max_price", 0.0))
                st_d = float(day_sim.get("target_price", 0.0))
                state_text = f"🔥 [모의] 1.3%락온(고점 ${pk_d:.2f} / 스탑 ${st_d:.2f})"
            else:
                ep_d = float(day_sim.get("entry_price", 0.0))
                lk_d = float(day_sim.get("arm_price", 0.0))
                state_text = f"🚀 [모의·{sim_mode}] 추격(진입 ${ep_d:.2f} ➔ 락온 ${lk_d:.2f})"
        elif in_day_open_7m:
            if day_sim.get("prelock_sym") == symbol:
                state_text = "🚀 [모의] 개시7분(10분봉락온 · RSI≥50 즉시진입)"
            else:
                state_text = "🛡️ [모의] 개시7분 쉴드(역추세 진입차단)"
        elif day_sim.get("leader_sym") == symbol:
            if day_sim.get("pb_rearm_ready", True):
                state_text = "👑 [모의] 상승장락온(≤35·대칭·🎯PIV10)"
            else:
                state_text = "🔄 [모의] 상승장 리셋대기(40≤RSI≤70·🎯PIV10)"
        elif not day_sim.get("opta_rearm_ready", True):
            state_text = "🔄 [모의] 대칭 리셋대기(40≤RSI≤70·🎯PIV10)"
        else:
            state_text = f"🔔 [모의] 감시(≤30&amp;{opp_sym}≥79.9 · 🎯PIV10)"
    elif 407 <= est_time_int <= 814:
        if bot_st.get("active", False):
            lp_v = float(bot_st.get("post_stop_low", 0.0))
            rp_v = float(bot_st.get("rebound_trig_price", 0.0))
            state_text = f"🪤 최저점추적(최저 ${lp_v:.2f} ➔ 반등 ${rp_v:.2f})"
        elif not is_done:
            if bool(sw_st.get("pivot_ready", False)):
                drop_v = float(sw_st.get("swing_drop_pct", 0.0))
                state_text = f"🎯 10분꺾임대기(진폭 -{drop_v:.1f}% · 돌파감시)"
            elif pre_ld.get("leader_sym") == symbol:
                if (700 <= est_time_int <= 814) and bool(pre_ld.get("pb_rearm_ready", True)):
                    state_text = "👑 상승주도(RSI≤35 추세·대칭·🎯PIV10)"
                else:
                    state_text = "👑 상승각인(07:00 눌림대기·대칭·🎯PIV10)"
            else:
                state_text = f"🔫 감시(≤30&amp;{opp_sym}≥79.9 · 🎯PIV10)"
        else:
            state_text = "🔒 프리장 타격잠금"
    elif 815 <= est_time_int < 930:
        state_text = "🔒 08:15 마감(관망)"
    elif 930 <= est_time_int < 1600:
        state_text = "🔒 마감(관망)"
    elif 1600 <= est_time_int <= 1759:
        state_text = "🛑 마감(관망)"
    else:
        state_text = "🛑 장외대기"

    return f"{sym_icon} <b>{symbol}</b> <code>[{state_flag}]</code> {state_text}"


# MODIFIED: 초과 Case 38, 78, 90, 91, 92, 93, 94, 97, 98, 99, 101 —
#           관제탑(/avwap)에서 17:00~17:59 EST(06:00~06:59 KST) 선물 마감 휴장 시 직전 세션 마감 시세·기준일을 명확히 표시하고
#           데이장 개시 7분 추세·1.3% 락온·40~70 리셋·10분봉 꺾임목·당일 모의 출격 승률 및 옵션 A(🚀08_HYB_PIVBRK)·이원화 매도·15:00 VWAP 탈출 상태 표출
async def build_avwap_radar(api_client, shared_rsi_state=None) -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    now_kst = now_est.astimezone(KST_TZ)
    wk = now_est.weekday()  # Mon=0 ... Fri=4, Sat=5, Sun=6
    t = now_est.hour * 100 + now_est.minute
    kst_t = now_kst.hour * 100 + now_kst.minute

    is_weekend_closed = (wk == 4 and t >= 1700) or (wk == 5) or (wk == 6 and t < 1800)
    is_cme_open_wait = (not is_weekend_closed) and (1800 <= t <= 1859)
    is_cme_daily_break = (not is_weekend_closed) and (1700 <= t <= 1759)
    is_day_time = (not is_weekend_closed) and (t >= 1900 or t <= 359)
    is_day_open_7m = is_day_time and ((900 <= kst_t <= 906) or (1900 <= t <= 1906))
    is_whipsaw_time = (not is_weekend_closed) and (358 <= t <= 406)
    is_pre_sniper_time = (not is_weekend_closed) and (407 <= t <= 814)
    is_pre_cutoff_time = (not is_weekend_closed) and (815 <= t <= 929)
    is_late_reg_vwap_time = (not is_weekend_closed) and (1500 <= t <= 1558)

    if is_weekend_closed:
        session_name_ui = "HOLIDAY"
        market_header = "( <b>🌙 주말/휴장일</b> )"
    elif is_cme_open_wait:
        session_name_ui = "cmeOpenWait"
        market_header = "( <b>🌐 선물 신세션 개장 · 19:00 데이장 대기</b> )"
    elif is_cme_daily_break:
        session_name_ui = "afterMarket"
        market_header = "( <b>🧹 17:00 정산 · 선물 마감휴장</b> )"
    elif is_whipsaw_time:
        session_name_ui = "preMarket" if t >= 400 else "dayMarket"
        market_header = "( <b>🌪️ 03:58 ~ 04:07 휩소작전</b> )"
    elif is_pre_sniper_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 프리장 골든타임 · 🚀08_HYB_PIVBRK</b> )"
    elif is_pre_cutoff_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🔒 프리장 마감</b> )"
    elif is_late_reg_vwap_time:
        session_name_ui = "regularMarket"
        market_header = "( <b>🛟 정규장 마감 1시간전 · 미익절 VWAP탈출</b> )"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 정규장 · 신규진입 마감</b> )"
    elif 1600 <= t <= 1659:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 애프터마켓</b> )"
    elif is_day_open_7m:
        session_name_ui = "dayMarket"
        market_header = "( <b>🚀 데이마켓 개시 7분 · NQ10분봉 추세판별</b> )"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>🌙 데이마켓 · 🔔100% [모의] 스나이퍼</b> )"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(api_client.is_market_open(), timeout=3.0)
        if not is_open:
            if sess_name and "HOLIDAY" in sess_name:
                session_name_ui = "HOLIDAY"
                market_header = "( <b>🌙 주말/휴장일</b> )"
            elif sess_name and "STANDBY" in sess_name:
                if is_cme_open_wait:
                    session_name_ui = "cmeOpenWait"
                    market_header = "( <b>🌐 선물 신세션 개장 · 19:00 데이장 대기</b> )"
                elif is_cme_daily_break:
                    market_header = "( <b>🧹 17:00 정산 · 선물 마감휴장</b> )"
    except Exception:
        pass

    # 초과 Case 90 & Case 91 — CME 선물 활성 여부 및 세션 기준일(예: 10-19) 확인
    is_cme_open_nq, nq_sess_start = get_cme_session_window_est(now_est)
    nq_sess_date_str = (nq_sess_start + timedelta(hours=6)).strftime("%m-%d")
    if is_cme_open_nq:
        nq_header_suffix = ""
        nq_curr_label = "현재"
    else:
        nq_header_suffix = f" <code>({nq_sess_date_str} 마감 시세 · 18:00 신세션 대기)</code>"
        nq_curr_label = "마감"

    nq_c, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0
    nq_curr_amp = (max(0.0, ((nq_c - nq_l) / nq_l * 100.0)) if (nq_l > 0.0 and nq_c > 0.0) else 0.0)

    prices_map = await api_client.get_current_prices_batch(("SOXS", "SOXL"))
    price_s = float(prices_map.get("SOXS", 0.0))
    price_l = float(prices_map.get("SOXL", 0.0))

    hold_s, hold_l = await asyncio.gather(
        api_client.get_symbol_holdings_detail("SOXS"),
        api_client.get_symbol_holdings_detail("SOXL"),
    )
    qty_s = float(hold_s.get("qty", 0.0))
    qty_l = float(hold_l.get("qty", 0.0))

    (amp_s, yest_amp_s, _), (amp_l, yest_amp_l, _) = await asyncio.gather(
        _fetch_5ma_amp(api_client, "SOXS"),
        _fetch_5ma_amp(api_client, "SOXL"),
    )

    empty_sess = {
        "pre_h": 0.0, "pre_l": 0.0, "pre_amp": 0.0, "pre_vwap": 0.0, "pre_body": 0.0,
        "reg_h": 0.0, "reg_l": 0.0, "reg_amp": 0.0, "reg_vwap": 0.0, "reg_body": 0.0,
    }
    if session_name_ui in ("dayMarket", "cmeOpenWait", "HOLIDAY"):
        sess_s = dict(empty_sess)
        sess_l = dict(empty_sess)
    else:
        sess_s, sess_l = await asyncio.gather(
            _fetch_session_stats(api_client, "SOXS", now_est),
            _fetch_session_stats(api_client, "SOXL", now_est),
        )

    pre_exp_l_s = sess_s["pre_h"] * (1.0 - amp_s / 100.0) if sess_s["pre_h"] > 0 else 0.0
    pre_exp_h_s = sess_s["pre_l"] * (1.0 + amp_s / 100.0) if sess_s["pre_l"] > 0 else 0.0
    reg_exp_l_s = sess_s["reg_h"] * (1.0 - amp_s / 100.0) if sess_s["reg_h"] > 0 else 0.0
    reg_exp_h_s = sess_s["reg_l"] * (1.0 + amp_s / 100.0) if sess_s["reg_l"] > 0 else 0.0

    pre_exp_l_l = sess_l["pre_h"] * (1.0 - amp_l / 100.0) if sess_l["pre_h"] > 0 else 0.0
    pre_exp_h_l = sess_l["pre_l"] * (1.0 + amp_l / 100.0) if sess_l["pre_l"] > 0 else 0.0
    reg_exp_l_l = sess_l["reg_h"] * (1.0 - amp_l / 100.0) if sess_l["reg_h"] > 0 else 0.0
    reg_exp_h_l = sess_l["reg_l"] * (1.0 + amp_l / 100.0) if sess_l["reg_l"] > 0 else 0.0

    state_s, state_l = await asyncio.gather(
        AssassinLedger.get_state("SOXS"),
        AssassinLedger.get_state("SOXL"),
    )
    buy_id_s, buy_id_l = await asyncio.gather(
        AssassinLedger.get_buy_order_id("SOXS"),
        AssassinLedger.get_buy_order_id("SOXL"),
    )

    # 초과 Case 90 — 18:00 EST 선물 신세션 또는 데이장(18:00~03:57 EST) 중 미보유·무주문·비과열(nq_amp < 2.0%) 상태에서
    # 전일/금요일에 남아 있던 CANCELED_MACRO 잠금 잔재 감지 시 즉시 자가 치유(Self-Healing) 클리어
    if (
        (is_cme_open_wait or is_day_time)
        and nq_amp < 2.0
        and qty_s <= 0.0
        and qty_l <= 0.0
        and not buy_id_s
        and not buy_id_l
        and (state_s[8] == "CANCELED_MACRO" or state_l[8] == "CANCELED_MACRO")
    ):
        for heal_sym in ("SOXS", "SOXL"):
            await AssassinLedger.save_state(
                heal_sym,
                is_session_done=False,
                entry_session="",
                whipsaw_target_price=0.0,
            )
        state_s, state_l = await asyncio.gather(
            AssassinLedger.get_state("SOXS"),
            AssassinLedger.get_state("SOXL"),
        )

    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    budget_s = float(state_s[1] or 100.0)
    comp_label_str = "통합복리" if is_comp_s else "단리"

    ewm_s = ((shared_rsi_state or {}).get("SOXS") or {}).get("ewm_state") or {}
    ewm_l = ((shared_rsi_state or {}).get("SOXL") or {}).get("ewm_state") or {}
    ha_bull_s = bool(ewm_s.get("ha_bull", True))
    ha_bull_l = bool(ewm_l.get("ha_bull", True))
    reg_vwap_s = float(sess_s.get("reg_vwap", 0.0)) or float(ewm_s.get("reg_vwap", 0.0))
    reg_vwap_l = float(sess_l.get("reg_vwap", 0.0)) or float(ewm_l.get("reg_vwap", 0.0))

    status_s = build_compact_status(
        "SOXS", state_s, buy_id_s, session_name_ui, t, qty_s,
        opp_qty=qty_l, opp_buy_id=buy_id_l, is_compound=is_comp_s, ha_bull=ha_bull_s,
        reg_vwap=reg_vwap_s, kst_time_int=kst_t,
    )
    status_l = build_compact_status(
        "SOXL", state_l, buy_id_l, session_name_ui, t, qty_l,
        opp_qty=qty_s, opp_buy_id=buy_id_s, is_compound=is_comp_s, ha_bull=ha_bull_l,
        reg_vwap=reg_vwap_l, kst_time_int=kst_t,
    )

    is_manual_s = (
        qty_s > 0.0
        and not buy_id_s
        and str(state_s[8] or "") not in _AUTO_ENTRY_TAGS_UI
    )
    is_manual_l = (
        qty_l > 0.0
        and not buy_id_l
        and str(state_l[8] or "") not in _AUTO_ENTRY_TAGS_UI
    )

    if is_manual_s or is_manual_l:
        macro_1st_str = "🟢 수동보유(2차 패스)"
    else:
        if nq_amp >= 2.0:
            macro_1st_str = "🛑 과열차단"
        else:
            macro_1st_str = "🟢 진입허용"

    # 초과 Case 92 & Case 94 — 포지션 보유 시 3차 실시간 QQQ 역행 방어 상태 간결 표출 (양방향 동시 보유 지원)
    macro_3rd_lines = []
    if qty_s > 0.0 or qty_l > 0.0:
        ref_curr, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
        ref_unit = "$" if ref_src == "QQQ실시간" else ""

        if qty_s > 0.0:
            nq_entry_p_s = float(state_s[10] or 0.0) or ref_curr
            nq_entry_a_s = float(state_s[11] or 0.0) or nq_amp
            if nq_entry_p_s > 0.0 and ref_curr > 0.0 and ((nq_entry_p_s > 3000.0) != (ref_curr > 3000.0)):
                nq_entry_p_s = ref_curr
                await AssassinLedger.save_state("SOXS", nq_entry_price=nq_entry_p_s)
            dyn_thresh_s = max(nq_entry_a_s * 0.5, NQ_DYNAMIC_FLOOR_UI, MIN_STAGE3_QQQ_ADVERSE_PCT)
            nq_adv_s = ((ref_curr - nq_entry_p_s) / nq_entry_p_s * 100.0) if (ref_curr > 0.0 and nq_entry_p_s > 0.0) else 0.0
            macro_3rd_lines.append(
                f"\n▫️ 3차 방어(🐻SOXS): <code>{nq_adv_s:+.2f}%</code> "
                f"(기준 <code>{ref_unit}{nq_entry_p_s:.2f}</code> / 한도 <code>+{dyn_thresh_s:.2f}%</code>)"
            )

        if qty_l > 0.0:
            nq_entry_p_l = float(state_l[10] or 0.0) or ref_curr
            nq_entry_a_l = float(state_l[11] or 0.0) or nq_amp
            if nq_entry_p_l > 0.0 and ref_curr > 0.0 and ((nq_entry_p_l > 3000.0) != (ref_curr > 3000.0)):
                nq_entry_p_l = ref_curr
                await AssassinLedger.save_state("SOXL", nq_entry_price=nq_entry_p_l)
            dyn_thresh_l = max(nq_entry_a_l * 0.5, NQ_DYNAMIC_FLOOR_UI, MIN_STAGE3_QQQ_ADVERSE_PCT)
            nq_adv_l = ((nq_entry_p_l - ref_curr) / nq_entry_p_l * 100.0) if (ref_curr > 0.0 and nq_entry_p_l > 0.0) else 0.0
            macro_3rd_lines.append(
                f"\n▫️ 3차 방어(🐂SOXL): <code>{nq_adv_l:+.2f}%</code> "
                f"(기준 <code>{ref_unit}{nq_entry_p_l:.2f}</code> / 한도 <code>+{dyn_thresh_l:.2f}%</code>)"
            )

    macro_3rd_line = "".join(macro_3rd_lines)

    day_sim = get_daymarket_sim_status()
    pre_ld = get_pre_leader_status()
    pre_leader_sym = str(pre_ld.get("leader_sym") or "")
    pre_pb_ready = bool(pre_ld.get("pb_rearm_ready", True))

    # Case 17, 38, 44, 57, 78, 81, 83, 84, 85, 87, 89, 93, 97, 98, 101:
    # 실시간 RSI + 하이킨 아시 + 10분봉 스윙·꺾임목 + 데이장 개시7분/반복출격 + VWAP 탈출 통합 렌더링
    rsi_ui_lines = []
    if shared_rsi_state:
        r_s = float((shared_rsi_state.get("SOXS") or {}).get("value", 0.0))
        r_l = float((shared_rsi_state.get("SOXL") or {}).get("value", 0.0))

        for r_sym in ("SOXS", "SOXL"):
            r_val = r_s if r_sym == "SOXS" else r_l
            opp_val = r_l if r_sym == "SOXS" else r_s
            opp_name = "SOXL" if r_sym == "SOXS" else "SOXS"
            is_ha_g = ha_bull_s if r_sym == "SOXS" else ha_bull_l
            r_qty = qty_s if r_sym == "SOXS" else qty_l
            r_reg_vwap = reg_vwap_s if r_sym == "SOXS" else reg_vwap_l
            tr_r = get_trailing_runtime_status(r_sym)
            sw_r = get_swing_10m_status(r_sym)
            day_sw_drop = float(day_sim.get(f"swing_drop_{r_sym}", 0.0))

            if is_ha_g:
                ha_short = "🕯️HA양봉"
            elif (tr_r.get("armed", False) and tr_r.get("nq_vel_bypass", False)) or (
                day_sim.get("active", False)
                and day_sim.get("symbol") == r_sym
                and day_sim.get("nq_vel_bypass", False)
            ):
                ha_short = "🚀HA바이패스"
            else:
                ha_short = "🌑HA음봉"
            bot_r = get_bottom_track_status(r_sym)

            if 358 <= t <= 359:
                warn_str = "🔇 마감휩소 묵음"
            elif 400 <= t <= 406:
                warn_str = "⏱️ 04:07 쉴드대기"
            elif is_late_reg_vwap_time and r_qty > 0.0 and not tr_r.get("armed", False) and r_reg_vwap > 0.0:
                vwap_fl_r = math.floor(r_reg_vwap * 100.0) / 100.0
                warn_str = f"🛟 VWAP탈출감시(${vwap_fl_r:.2f})"
            elif is_pre_sniper_time and bot_r.get("active", False):
                lp_r = float(bot_r.get("post_stop_low", 0.0))
                rp_r = float(bot_r.get("rebound_trig_price", 0.0))
                warn_str = f"🪤 최저점추적(${lp_r:.2f} ➡️ ${rp_r:.2f})"
            elif is_day_time and day_sim.get("active", False) and day_sim.get("symbol") == r_sym:
                sim_m = str(day_sim.get("trig_mode") or "OPTA")
                if day_sim.get("phase") == "TRAP_WAIT":
                    warn_str = f"🪤 모의1차덫·{sim_m}(${day_sim['trap_price']:.2f})"
                elif day_sim.get("trail_armed", False):
                    warn_str = f"🔥 모의1.3%락온(스탑 ${day_sim['target_price']:.2f} · {ha_short})"
                else:
                    warn_str = f"🚀 모의추격·{sim_m}(락온 ${day_sim['arm_price']:.2f} · {ha_short})"
            elif is_day_open_7m:
                if day_sim.get("prelock_sym") == r_sym:
                    warn_str = f"🚀 개시7분 10분봉락온(≥50 즉시진입 · {ha_short})"
                else:
                    warn_str = f"🛡️ 개시7분 역추세차단({ha_short})"
            elif is_pre_sniper_time and bool(sw_r.get("pivot_ready", False)):
                drop_v = float(sw_r.get("swing_drop_pct", 0.0))
                warn_str = f"🎯 10분꺾임대기(진폭 -{drop_v:.1f}% · {ha_short})"
            elif is_day_time and day_sw_drop >= 1.80 and (0.0 < r_val <= 45.0):
                warn_str = f"🎯 모의10분꺾임감시(진폭 -{day_sw_drop:.1f}% · {ha_short})"
            elif (
                is_pre_sniper_time
                and (700 <= t <= 814)
                and pre_leader_sym == r_sym
                and pre_pb_ready
                and (0.0 < r_val <= 35.0)
            ):
                warn_str = f"🚀 추세눌림 타격권(≤35.0 · {ha_short})"
            elif (
                is_pre_sniper_time
                and (700 <= t <= 814)
                and pre_leader_sym == r_sym
                and pre_pb_ready
                and (35.0 < r_val <= 38.0)
            ):
                warn_str = f"⚡ 추세눌림 부스터(≤38.0 · {ha_short})"
            elif (
                is_day_time
                and day_sim.get("leader_sym") == r_sym
                and (0.0 < r_val <= 35.0)
            ):
                if day_sim.get("pb_rearm_ready", True):
                    warn_str = f"👑 모의상승장 35눌림(≤35.0 · {ha_short})"
                else:
                    warn_str = f"🔄 리셋대기(40≤RSI≤70 · {ha_short})"
            elif 0.0 < r_val <= 30.0:
                if opp_val >= 79.9:
                    if is_pre_sniper_time:
                        warn_str = f"🔫 대칭 타격권({opp_name} {opp_val:.1f})"
                    elif is_day_time:
                        if day_sim.get("opta_rearm_ready", True):
                            warn_str = f"🔔 모의 타격권({opp_name} {opp_val:.1f})"
                        else:
                            warn_str = f"🔄 대칭리셋대기(40~70 · {ha_short})"
                    else:
                        warn_str = f"📉 과매도({ha_short})"
                elif opp_val >= 79.0 and (is_pre_sniper_time or is_day_time):
                    warn_str = f"⚡ 0.4초 부스터({opp_name} {opp_val:.1f})"
                else:
                    warn_str = f"⏳ 과매도({ha_short})"
            elif r_val >= 79.9:
                warn_str = f"🔥 클라이맥스({ha_short})"
            elif r_val >= 79.0 and (is_pre_sniper_time or is_day_time):
                warn_str = f"⚡ 0.4초 부스터({ha_short})"
            else:
                if is_pre_sniper_time and pre_leader_sym == r_sym:
                    warn_str = f"👑 상승주도({ha_short})"
                elif is_day_time and day_sim.get("leader_sym") == r_sym:
                    warn_str = f"👑 모의상승장({ha_short})"
                else:
                    warn_str = f"🟢 중립({ha_short})"

            if r_val > 0.0:
                icon = "🐻" if r_sym == "SOXS" else "🐂"
                rsi_ui_lines.append(f"▫️ {icon} {r_sym} RSI(1m): <code>{r_val:.1f}</code> ➡️ {warn_str}")

    rsi_ui_str = "\n".join(rsi_ui_lines) + "\n" if rsi_ui_lines else ""

    # 초과 Case 78, 93, 101 — 데이장 진행 중이거나 당일 데이장 [모의] 출격 기록이 있을 때 적중 승률 1줄 요약 표출
    day_stat_line = ""
    if is_day_time or int(day_sim.get("stat_total", 0)) > 0:
        d_tot = int(day_sim.get("stat_total", 0))
        d_win = int(day_sim.get("stat_wins", 0))
        d_loss = int(day_sim.get("stat_losses", 0))
        d_wr = float(day_sim.get("stat_win_rate", 0.0))
        d_net = float(day_sim.get("stat_cum_net_pct", 0.0))
        d_pv_c = int(day_sim.get("stat_piv10_cnt", 0))
        d_pv_w = int(day_sim.get("stat_piv10_wins", 0))
        pv_tag = f" · 🎯PIV10 {d_pv_w}/{d_pv_c}" if d_pv_c > 0 else ""
        day_stat_line = (
            f"\n📊 <b>데이장 [모의] 전적:</b> <code>{d_tot}출격 {d_win}승 {d_loss}패 "
            f"(승률 {d_wr:.1f}% · 세후 {d_net:+.2f}%{pv_tag})</code>"
        )

    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[듀얼 스나이퍼 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물</b>{nq_header_suffix}
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ {nq_curr_label}: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code>
▫️ 저가기준 진폭: <code>{nq_curr_amp:.2f}%</code>
▫️ 1·2차 방어: <code>{macro_1st_str}</code>{macro_3rd_line}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
📊 <b>현재가 &amp; 5MA(어제 진폭) &amp; RSI·HA(1m)</b>
🐻 <b>SOXS</b> <code>${price_s:.2f}</code> | <code>{amp_s:.1f}%({yest_amp_s:.1f}%)</code>
🐂 <b>SOXL</b> <code>${price_l:.2f}</code> | <code>{amp_l:.1f}%({yest_amp_l:.1f}%)</code>
{rsi_ui_str}
🌅 <b>프리장</b>
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['pre_l']:.2f} ~ ${sess_s['pre_h']:.2f} ({sess_s['pre_amp']:.1f}%)</code>
      | 예상: <code>${pre_exp_l_s:.2f} ~ ${pre_exp_h_s:.2f}</code>
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['pre_l']:.2f} ~ ${sess_l['pre_h']:.2f} ({sess_l['pre_amp']:.1f}%)</code>
      | 예상: <code>${pre_exp_l_l:.2f} ~ ${pre_exp_h_l:.2f}</code>

🔥 <b>정규장</b>
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['reg_l']:.2f} ~ ${sess_s['reg_h']:.2f} ({sess_s['reg_amp']:.1f}%)</code>
      | 예상: <code>${reg_exp_l_s:.2f} ~ ${reg_exp_h_s:.2f}</code>
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['reg_l']:.2f} ~ ${sess_l['reg_h']:.2f} ({sess_l['reg_amp']:.1f}%)</code>
      | 예상: <code>${reg_exp_l_l:.2f} ~ ${reg_exp_h_l:.2f}</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
{status_s}
{status_l}{day_stat_line}
💰 <b>{comp_label_str} 시드:</b> <code>${budget_s:,.2f}</code>

⏱️ 갱신: <code>{scan_time} EST</code>"""

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 레이더 갱신", callback_data="open_avwap")],
            [InlineKeyboardButton(text="⚙️ 전술 제어반", callback_data="open_settlement")],
            [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
        ]
    )
    return text, keyboard
