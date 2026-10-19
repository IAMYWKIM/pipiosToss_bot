# =====================================================================
# FILE: tg_ui_plugin.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       선택지 B (👑 HYB_35_1ST : 양방향 대칭 클라이맥스 + 04:07~ 첫 RSI>=79.9 상승종목의 07:00~08:14 RSI<=35.0 추세눌림목) 듀얼 스나이퍼(SOXS/SOXL),
#       진입 성격별 '이원화 매도 믹스(기술적 반등 ⚡ OLD_IMM_CB × 진성 상승종목 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 × 🚀 NQ_VEL_BYPASS)',
#       3차 실시간 QQQ 최소 0.80% 역행 방어 및 최저점 반등 재진입(🪤 RE_BOT_TRAIL),
#       15:00 ~ 15:58 EST 정규장 장마감 1시간 전 미익절 물량 당일 정규장 VWAP 도달 탈출(🛟 VWAP_ESCAPE),
#       19:00 ~ 03:57 EST 데이장 100% [모의] 관측(개시 1분 09:00 KST 한정 역산버퍼 포함),
#       03:58 ~ 04:07 EST 듀얼 1주 무비용 휩소 매수 덫 및 초슬림 텔레그램 UI 렌더링 모듈
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 +0.64% / +1.00% 익절률 선택 버튼 및 데드코드 영구 소각
# MODIFIED: 초과 Case 13, 38, 77 — 메인 메뉴(/start), 듀얼 지시서(/sync), 관제탑(/avwap), 제어반(/settlement)에서
#           불필요한 부연 설명을 전면 삭제하여 모든 항목이 모바일 화면에서 2줄을 넘어가지 않도록 초슬림화
# MODIFIED: 초과 Case 93 — 프리장 본진 UI에서 선행 역산버퍼(AMP_BUFFER) 표시를 전면 제거
# MODIFIED: 초과 Case 92 & Case 94 — 최소 0.80% 실시간 QQQ 역행 하한선(MIN_STAGE3_QQQ_ADVERSE_PCT = 0.80) 반영 및
#           SOXS·SOXL 동시 수동 보유 시 /avwap 3차 방어 라인과 /settlement 출동 상태에서 양쪽 종목을 모두 독립 표출
# NEW: 초과 Case 97 — 04:07 EST 이후 각인된 당일 첫 상승 추세 종목(👑상승주도: SOXL/SOXS) 및
#      07:00 ~ 08:14 EST RSI <= 35.0 추세눌림목 타격/부스터 상태(PRE_PULLBACK / PRE_PULLBACK_REARM) 초슬림 렌더링 탑재
# NEW: 초과 Case 98 — 15:00 ~ 15:58 EST 정규장 장마감 1시간 전 미익절 보유 물량의 당일 정규장 VWAP 도달 탈출 감시(🛟 VWAP탈출감시) 상태 표출
# NEW: 초과 Case 99 — 기술적 반등(⚡반등트레일링) vs 진성 상승종목(🚀진성트레일링) 이원화 매도 레짐 실시간 표출
# MODIFIED: 텔레그램 HTML 파서 크래시(Unsupported start tag) 방어를 위해 코드 블록 내 원시 부등호(<, >) 전면 소각 (Case 17)

import math
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
)
from sniper_plugin import (
    get_daymarket_sim_status,
    get_whipsaw_runtime_status,
    get_trailing_runtime_status,
    get_bottom_track_status,
    get_pre_leader_status,
)

EST_TZ = ZoneInfo("America/New_York")
NQ_DYNAMIC_FLOOR_UI = MIN_STAGE3_QQQ_ADVERSE_PCT

_AUTO_ENTRY_TAGS_UI = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)


# MODIFIED: 메인 메뉴(/start 및 메인 복귀) 화면 불필요 설명 삭제 및 1줄 고정 슬림화 (Case 38, 77, 97, 98, 99)
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
        "🔹 18:00 ~ 18:59: 🌐 <b>선물 신세션 개장</b>\n"
        "🔹 19:00 ~ 03:57: 🌙 <b>[모의] 데이장</b>\n"
        "🔹 03:58 ~ 03:59: 🔇 <b>휩소 묵음</b>\n"
        "🔹 03:58 ~ 04:07: 🌪️ <b>'휩소 노출 작전'</b>\n"
        "🔹 04:07 ~ 08:14: 🔫 <b>프리장 본진 (👑HYB_35_1ST)</b>\n"
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


# MODIFIED: Case 13 & 초과 Case 38, 50, 57, 83, 84, 85, 86, 87, 89, 90, 91, 92, 93, 94, 97, 98, 99 —
#           하단 종목 상태가 2줄을 넘어가지 않도록 간결화 및 이원화 트레일링·15:00 VWAP 탈출 감시 표시 반영
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
    nq_bypass_on = bool(trail_st.get("nq_vel_bypass", False))
    exit_reg_str = str(trail_st.get("exit_regime", "OLD_IMM_CB"))

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
            st_p = float(trail_st.get("stop_price", target_sell_price))
            reg_lbl = "🚀진성트레일링" if exit_reg_str == "PEAK_LCK_130" else "⚡반등트레일링"
            prefix_tag = f"🔥 수동·{reg_lbl}" if is_manual_hold else reg_lbl
            state_text = f"{prefix_tag}({int(qty)}주 · 고점 ${pk_p:.2f} / 스탑 ${st_p:.2f})"
        elif (1500 <= est_time_int <= 1558) and reg_vwap > 0.0:
            vwap_fl = math.floor(reg_vwap * 100.0) / 100.0
            state_text = f"🛟 VWAP탈출감시({int(qty)}주 · VWAP ${vwap_fl:.2f})"
        elif target_sell_price > 0.0:
            prefix_tag = "🎯 수동보유" if is_manual_hold else (
                "🚀 추세보유" if entry_session in ("PRE_PULLBACK", "PRE_PULLBACK_REARM") else "🎯 대칭보유"
            )
            state_text = f"{prefix_tag}({int(qty)}주 · 점화 ${target_sell_price:.2f} · {ha_badge})"
        else:
            prefix_tag = "🎯 수동보유" if is_manual_hold else "🎯 보유"
            state_text = f"{prefix_tag}({int(qty)}주 · 안전캡 장전중)"
    elif buy_order_id:
        if entry_session == "WHIPSAW_1SH":
            w_bp = whip_st.get("buy_price", 0.0)
            state_text = f"🌪️ 휩소1주 매수덫(${w_bp:.2f})"
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
            if day_sim.get("phase") == "BUFFER_WAIT":
                td_d = float(day_sim.get("target_drop_p", 0.0))
                rp_d = float(day_sim.get("rebound_trig_p", 0.0))
                state_text = f"🛡️ [모의] 개시버퍼(${td_d:.2f} ➔ 반등 ${rp_d:.2f})"
            elif day_sim.get("phase") == "TRAP_WAIT":
                state_text = f"🪤 [모의] 1차덫(${day_sim['trap_price']:.2f})"
            else:
                state_text = f"🔔 [모의] 추격(목표 ${day_sim['target_price']:.2f})"
        elif day_sim.get("leader_sym") == symbol:
            state_text = "👑 [모의] 상승주도(≤35 추세·대칭감시)"
        else:
            state_text = f"🔔 [모의] 대칭감시(≤30 &amp; {opp_sym}≥79.9)"
    elif 407 <= est_time_int <= 814:
        if bot_st.get("active", False):
            lp_v = float(bot_st.get("post_stop_low", 0.0))
            rp_v = float(bot_st.get("rebound_trig_price", 0.0))
            state_text = f"🪤 최저점추적(최저 ${lp_v:.2f} ➔ 반등 ${rp_v:.2f})"
        elif not is_done:
            if pre_ld.get("leader_sym") == symbol:
                if (700 <= est_time_int <= 814) and bool(pre_ld.get("pb_rearm_ready", True)):
                    state_text = "👑 상승주도(RSI≤35 추세·대칭감시)"
                else:
                    state_text = "👑 상승각인(07:00 눌림대기·대칭감시)"
            else:
                state_text = f"🔫 대칭감시(≤30 &amp; {opp_sym}≥79.9)"
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


# MODIFIED: 초과 Case 38, 90, 91, 92, 93, 94, 97, 98, 99 —
#           관제탑(/avwap)에서 불필요한 설명 줄을 모두 제거하여 2줄 초과 줄바꿈을 원천 차단하고 선택지 B·이원화 매도·15:00 VWAP 탈출 상태 표출
async def build_avwap_radar(api_client, shared_rsi_state=None) -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    wk = now_est.weekday()  # Mon=0 ... Fri=4, Sat=5, Sun=6
    t = now_est.hour * 100 + now_est.minute

    is_weekend_closed = (wk == 4 and t >= 1700) or (wk == 5) or (wk == 6 and t < 1800)
    is_cme_open_wait = (not is_weekend_closed) and (1800 <= t <= 1859)
    is_day_time = (not is_weekend_closed) and (t >= 1900 or t <= 359)
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
    elif is_whipsaw_time:
        session_name_ui = "preMarket" if t >= 400 else "dayMarket"
        market_header = "( <b>🌪️ 03:58 ~ 04:07 휩소작전</b> )"
    elif is_pre_sniper_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 프리장 골든타임</b> )"
    elif is_pre_cutoff_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🔒 프리장 마감</b> )"
    elif is_late_reg_vwap_time:
        session_name_ui = "regularMarket"
        market_header = "( <b>🛟 정규장 마감 1시간전 · 미익절 VWAP탈출</b> )"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 정규장 · 신규진입 마감</b> )"
    elif 1600 <= t <= 1759:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 애프터마켓</b> )"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>🌙 데이마켓 · 🔔100% [모의] 감시</b> )"

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
                elif 1700 <= t <= 1759:
                    market_header = "( <b>🧹 17:00 정산 대기</b> )"
    except Exception:
        pass

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
        reg_vwap=reg_vwap_s,
    )
    status_l = build_compact_status(
        "SOXL", state_l, buy_id_l, session_name_ui, t, qty_l,
        opp_qty=qty_s, opp_buy_id=buy_id_s, is_compound=is_comp_s, ha_bull=ha_bull_l,
        reg_vwap=reg_vwap_l,
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

    # Case 17, 38, 44, 57, 78, 81, 83, 84, 85, 87, 89, 93, 97, 98: 실시간 RSI + 하이킨 아시 + 추세눌림목 + VWAP 탈출 통합 렌더링
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
            if is_ha_g:
                ha_short = "🕯️HA양봉"
            elif tr_r.get("armed", False) and tr_r.get("nq_vel_bypass", False):
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
                if day_sim.get("phase") == "BUFFER_WAIT":
                    td_d = float(day_sim.get("target_drop_p", 0.0))
                    rp_d = float(day_sim.get("rebound_trig_p", 0.0))
                    warn_str = f"🛡️ 모의개시버퍼(${td_d:.2f} ➡️ ${rp_d:.2f})"
                elif day_sim.get("phase") == "TRAP_WAIT":
                    warn_str = f"🪤 모의1차덫(${day_sim['trap_price']:.2f})"
                else:
                    warn_str = f"🔒 모의추격(${day_sim['target_price']:.2f})"
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
                and day_sim.get("pb_rearm_ready", True)
                and (0.0 < r_val <= 35.0)
            ):
                warn_str = f"🔔 모의추세눌림(≤35.0 · {ha_short})"
            elif 0.0 < r_val <= 30.0:
                if opp_val >= 79.9:
                    if is_pre_sniper_time:
                        warn_str = f"🔫 대칭 타격권({opp_name} {opp_val:.1f})"
                    elif is_day_time:
                        warn_str = f"🔔 모의 타격권({opp_name} {opp_val:.1f})"
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
                else:
                    warn_str = f"🟢 중립({ha_short})"

            if r_val > 0.0:
                icon = "🐻" if r_sym == "SOXS" else "🐂"
                rsi_ui_lines.append(f"▫️ {icon} {r_sym} RSI(1m): <code>{r_val:.1f}</code> ➡️ {warn_str}")

    rsi_ui_str = "\n".join(rsi_ui_lines) + "\n" if rsi_ui_lines else ""
    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[듀얼 스나이퍼 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물</b>
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ 현재: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code>
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
{status_l}
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


# MODIFIED: 듀얼 지시서(/sync)에서 종목 대기 상태의 불필요한 부연 설명을 삭제하여 1줄 고정 및 이원화 트레일링·VWAP 탈출 표시 (Case 38, 97, 98, 99)
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
        market_state = "🌪️ 03:58~04:07 휩소 작전"
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

    async def get_symbol_sync_data(symbol: str):
        state = await AssassinLedger.get_state(symbol)
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

        if now_est.hour >= 19:
            session_start_est = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
        else:
            session_start_est = (now_est - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

        try:
            data_1d = await api_client._request(
                "GET",
                f"/api/v1/candles?symbol={symbol}&interval=1d&count=5",
                "MARKET_DATA_CHART",
                headers=api_client._get_headers(),
            )
            candles_1d = (data_1d or {}).get("result", {}).get("candles", [])
            target_date = session_start_est.date()

            for c in candles_1d:
                c_dt = pd.to_datetime(c["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ).date()
                if c_dt == target_date:
                    h_val = float(c.get("highPrice") or 0.0)
                    l_val = float(c.get("lowPrice") or 0.0)
                    if h_val > 0:
                        high = h_val
                    if l_val > 0:
                        low = l_val
                elif c_dt < target_date:
                    prev_close = float(c.get("closePrice") or 0.0)
                    break
        except Exception:
            pass

        high_rate = ((high - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
        low_rate = ((low - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

        return {
            "symbol": symbol, "budget": budget, "is_compound": is_comp,
            "target_sell_p": target_sell_p, "trail_st": trail_st, "bot_st": bot_st,
            "entry_session": entry_session, "curr": curr, "avg_price": avg_price, "qty": qty,
            "high": high, "high_rate": high_rate, "low": low, "low_rate": low_rate,
            "profit_rate": profit_rate, "profit_usd": profit_usd, "profit_krw": profit_krw,
        }

    soxs_data, soxl_data = await asyncio.gather(
        get_symbol_sync_data("SOXS"),
        get_symbol_sync_data("SOXL"),
    )
    pre_ld = get_pre_leader_status()
    pre_leader_sym = str(pre_ld.get("leader_sym") or "")

    def format_symbol(d: dict) -> str:
        sym = d["symbol"]
        icon = "🐻" if sym == "SOXS" else "🐂"
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        comp_label = "📈 통합복리시드" if d["is_compound"] else "💵 단리시드"
        tr_st = d["trail_st"]
        bt_st = d["bot_st"]
        es_str = str(d["entry_session"] or "")
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
        elif (407 <= t <= 814) and bt_st.get("active", False):
            lp_b = float(bt_st.get("post_stop_low", 0.0))
            rp_b = float(bt_st.get("rebound_trig_price", 0.0))
            flag_str = f"🪤 최저점추적 (${lp_b:.2f} ➔ ${rp_b:.2f})"
        elif (407 <= t <= 814) and pre_leader_sym == sym:
            flag_str = "👑 상승주도(RSI≤35)" if t >= 700 else "👑 상승각인(07시 대기)"
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


# MODIFIED: 전술 제어반(/settlement) 슬림화, 양방향 동시 보유 표출, 이원화 트레일링 및 15:00 VWAP 탈출 표시 (Case 37, 77, 94, 97, 98, 99)
async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    wk = now_est.weekday()
    t = now_est.hour * 100 + now_est.minute
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
                return f"🌪️ [{sym_a}] 휩소 1주 매수덫 (${whip_st['buy_price']:.2f})"
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
        strike_status_str = "🌐 선물 신세션 개장 (19:00 데이장 대기)"
    elif (state_s[8] == "CANCELED_MACRO" or state_l[8] == "CANCELED_MACRO") and (400 <= t <= 1759):
        strike_status_str = "🛑 매크로 과열방어 (자동마감)"
    elif (state_s[8] == "VWAP_ESCAPE" or state_l[8] == "VWAP_ESCAPE") and (1500 <= t <= 1759):
        strike_status_str = "🛟 장마감 1시간전 VWAP 탈출 완료"
    elif 358 <= t <= 406:
        strike_status_str = "🌪️ 휩소 노출 작전중 (03:58 ~ 04:07)"
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
            if day_sim.get("phase") == "BUFFER_WAIT":
                td_d = float(day_sim.get("target_drop_p", 0.0))
                strike_status_str = f"🛡️ [모의·{sim_sym}] 개시버퍼 대기 (${td_d:.2f})"
            elif day_sim.get("phase") == "TRAP_WAIT":
                strike_status_str = f"🪤 [모의·{sim_sym}] 1차덫 대기 (${day_sim['trap_price']:.2f})"
            else:
                strike_status_str = f"🔔 [모의·{sim_sym}] 추격중 (목표 ${day_sim['target_price']:.2f})"
        else:
            strike_status_str = "🌙 [모의] 데이장 감시중"
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
