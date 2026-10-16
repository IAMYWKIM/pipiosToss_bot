# =====================================================================
# FILE: tg_ui_plugin.py
# 목적: 프리장 (04:07 ~ 09:29 EST) 양방향 대칭 클라이맥스 듀얼 스나이퍼(SOXS/SOXL) 및
#       03:58 ~ 04:07 EST 듀얼 1주 무비용 휩소 매수 덫 텔레그램 UI 렌더링 모듈
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 NQ 10분봉 추세 UI 잔재 및 미사용 상수(NQ_UI_BETA) 소각
# NEW: RSI >= 79.0 프리-클라이맥스 0.4초 고주파 부스터 및 RSI >= 79.9 (80.0 클라이맥스 인지) 상태 표출 반영 (Case 28, 81, 83)
# MODIFIED: 데이장(19:00 ~ 03:57 EST) 프리장 동일 대칭 조건 [모의] 듀얼 2단계 추격 및 03:58 ~ 03:59 EST 묵음 바이패스 상태 표출 (Case 78)
# MODIFIED: 03:58 ~ 04:07 EST 듀얼 1주 무비용 휩소 매수 덫(현재가 > 데이장VWAP 종목 1% 역산) 및 04:07 EST 타임쉴드 렌더링 (Case 79, 80)
# MODIFIED: 관제탑(/avwap) 시안 A(평시) <-> 시안 B(부스터/보유) 조건부 동적 전환 및 프리·정규장 3단 계단식 줄바꿈 레이아웃 적용 (Case 13, 38, 47, 67)
# MODIFIED: 전술 제어반(/settlement) 상단 장문 설명을 소각하고 핵심 5대 요건(전원·출동상태·익절·운용방식·통합시드) 초슬림 UI로 개편 (Case 37, 38)
# MODIFIED: 텔레그램 HTML 파서 크래시(Unsupported start tag) 방어를 위해 코드 블록 내 원시 부등호(<, >) 전면 소각 및 한글 치환 (Case 17)
# MODIFIED: 메인 메뉴(get_main_menu_text) 내 과도한 내부 수식 설명을 소각하고 핵심 타임라인 및 명령어 중심의 심플 UI로 전면 개편 (Case 38)
# MODIFIED: 16:00 ~ 18:59 EST 전 구간을 애프터마켓(afterMarket)으로 하드 락온하여 17:00 ~ 18:59 EST 데이마켓 오판 버그 원천 소각 (Case 01, 57)

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from quant_engine import AssassinLedger, MacroDataCache
from sniper_plugin import get_daymarket_sim_status, get_whipsaw_runtime_status

EST_TZ = ZoneInfo("America/New_York")
NQ_DYNAMIC_FLOOR_UI = 0.8


# MODIFIED: 메인 메뉴(/start 및 메인 복귀) 화면에서 복잡한 내부 수식 설명을 제거하고 핵심 스케줄과 명령어만 깔끔하게 표출 (Case 38)
async def get_main_menu_text() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_status_text = "🌞서머타임 ON (EDT)" if is_dst else "❄️서머타임 OFF (EST)"

    state_s = await AssassinLedger.get_state("SOXS")
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    active_rate = AssassinLedger.normalize_target_rate(state_s[2])
    comp_mode_str = "통합 복리 ON (실제 발생 순수익금 가감)" if is_comp_s else "단리 ON (시드 고정)"

    return (
        f"🕒 <b>[ 듀얼 스나이퍼 &amp; 무비용 휩소 매수덫 스케줄 ({dst_status_text}) ]</b>\n"
        f"▫️ 현재 설정: <b>목표 익절 +{active_rate:.2f}% · {comp_mode_str}</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 19:00 ~ 03:57: 🌙 <b>[모의] 데이장</b>\n"
        "🔹 03:58 ~ 03:59: 🔇 <b>휩소 묵음</b>\n"
        "🔹 03:58 ~ 04:07: 🌪️ <b>'휩소 노출 작전'</b>\n"
        "🔹 04:07 ~ 09:29: 🔫 <b>프리장 본진</b>\n"
        "🔹 09:30 ~ 15:58: 🔥 <b>정규장 본장</b>\n"
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


# MODIFIED: Case 13 & 초과 Case 38, 57, 83 — 시안 A(평시/마감) ⟷ 시안 B(보유/주문/뮤텍스 대기) 조건부 동적 상태 표출
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
) -> str:
    active_rate = AssassinLedger.normalize_target_rate(state_tuple[2])
    is_done = state_tuple[4]
    is_active = state_tuple[5]
    target_sell_price = state_tuple[6]
    entry_session = state_tuple[8]
    state_flag = "ON" if is_active else "OFF"
    sym_icon = "🐻" if symbol == "SOXS" else "🐂"
    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    day_sim = get_daymarket_sim_status()
    whip_st = get_whipsaw_runtime_status()

    if not is_active:
        state_text = "⏸️ 가동OFF(수동대기)"
    elif qty > 0:
        if entry_session == "WHIPSAW_1SH":
            if target_sell_price > 0.0:
                state_text = f"🌪️ 휩소보유(1주 · 목표 ${target_sell_price:.2f})"
            else:
                state_text = "🌪️ 휩소보유(1주 · VWAP덫 장전중)"
        elif target_sell_price > 0.0:
            state_text = f"🎯 보유({int(qty)}주 · 목표 ${target_sell_price:.2f})"
        else:
            state_text = f"🎯 보유({int(qty)}주 · +{active_rate:.2f}% 덫 장전중)"
    elif buy_order_id:
        if entry_session == "WHIPSAW_1SH":
            w_bp = whip_st.get("buy_price", 0.0)
            state_text = f"🌪️ 휩소1주 매수덫(${w_bp:.2f})"
        elif entry_session == "PRE_SNIPER":
            state_text = "🪤 1차 매수덫(30초 타임쉴드)"
        else:
            state_text = "⚡ 재장전 매수 체결대기"
    elif opp_qty > 0 or bool(opp_buy_id):
        state_text = f"🔒 대기({opp_sym} 운용중)"
    elif entry_session == "CANCELED_MACRO":
        state_text = "🛑 매크로방어(금일마감)"
    elif 358 <= est_time_int <= 406:
        if whip_st.get("target_sym") == symbol and whip_st.get("locked_vwap", 0.0) > 0.0:
            state_text = f"🌪️ 휩소타겟(덫 ${whip_st['buy_price']:.2f} ➡️ ${whip_st['sell_target']:.2f})"
        else:
            state_text = "🌪️ 휩소구간(04:07 타임쉴드)"
    elif current_session == "dayMarket" and (est_time_int >= 1900 or est_time_int <= 357):
        if day_sim.get("active", False) and day_sim.get("symbol") == symbol:
            if day_sim.get("phase") == "TRAP_WAIT":
                state_text = f"🪤 데이장[모의] 1차덫(${day_sim['trap_price']:.2f})"
            else:
                state_text = f"🔔 데이장[모의] 추격(목표 ${day_sim['target_price']:.2f})"
        else:
            state_text = f"🔔 [모의] 대칭감시(≤30 &amp; {opp_sym}≥79.9)"
    elif 407 <= est_time_int <= 929:
        if not is_done:
            state_text = f"🔫 대칭감시(≤30 &amp; {opp_sym}≥79.9)"
        else:
            state_text = "🔒 프리장 타격잠금"
    elif 930 <= est_time_int < 1600:
        state_text = "🔒 마감(관망)"
    elif 1600 <= est_time_int <= 1859:
        state_text = "🛑 마감(관망)"
    else:
        state_text = "🛑 장외대기"

    return f"{sym_icon} <b>{symbol}</b> <code>[{state_flag}]</code> {state_text}"


# MODIFIED: 필수 관제 정보 보존 + 3단 계단식 줄바꿈 + 시안 A / 시안 B 조건부 동적 렌더링 (Case 13, 38, 47, 67, 83)
async def build_avwap_radar(api_client, shared_rsi_state=None) -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute
    is_day_time = (t >= 1900 or t <= 359)
    is_whipsaw_time = (358 <= t <= 406)
    is_pre_sniper_time = (407 <= t <= 929)

    if is_whipsaw_time:
        session_name_ui = "preMarket" if t >= 400 else "dayMarket"
        market_header = "( <b>🌪️ 03:58 ~ 04:07 휩소작전</b> )"
        pre_window_label = "(03:58 ~ 04:07 휩소1주 · 04:07 본진)"
    elif is_pre_sniper_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 프리장 골든타임</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 대칭 출동중)"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 정규장 · 신규진입 마감</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 출동 마감)"
    # Case 01, 57: 16:00 ~ 18:59 EST 전 구간을 afterMarket으로 락온
    elif 1600 <= t <= 1859:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 애프터마켓</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 출동 마감)"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>🌙 데이마켓 · 🔔[모의] 감시</b> )"
        pre_window_label = "(03:58 휩소1주 · 04:07 본진 대기)"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(api_client.is_market_open(), timeout=3.0)
        if not is_open:
            if sess_name and "HOLIDAY" in sess_name:
                market_header = "( <b>🌙 주말/휴장일</b> )"
            elif not (1600 <= t <= 1859):
                market_header = "( <b>🌙 시스템 대기</b> )"
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
    if session_name_ui == "dayMarket":
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
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    budget_s = float(state_s[1] or 100.0)
    active_rate_s = AssassinLedger.normalize_target_rate(state_s[2])
    comp_label_str = "통합복리" if is_comp_s else "단리"

    status_s = build_compact_status(
        "SOXS", state_s, buy_id_s, session_name_ui, t, qty_s, opp_qty=qty_l, opp_buy_id=buy_id_l, is_compound=is_comp_s
    )
    status_l = build_compact_status(
        "SOXL", state_l, buy_id_l, session_name_ui, t, qty_l, opp_qty=qty_s, opp_buy_id=buy_id_s, is_compound=is_comp_s
    )

    # Case 17 & Case 67: 평시(시안 A)에는 1·2차 방어선만 표출하고, 포지션 보유 시(시안 B) 3차 역행방어선을 동적 추가
    macro_1st_str = "🛑 과열차단(≥2.0%)" if nq_amp >= 2.0 else "🟢 진입허용(2.0%미만)"
    if qty_s > 0.0 and float(state_s[10] or 0.0) > 0.0:
        nq_entry_p = float(state_s[10])
        nq_entry_a = float(state_s[11] or 0.0)
        dyn_thresh = max(nq_entry_a * 0.5, NQ_DYNAMIC_FLOOR_UI)
        nq_adv = ((nq_c - nq_entry_p) / nq_entry_p * 100.0) if nq_c > 0.0 else 0.0
        macro_3rd_line = (
            f"\n▫️ 3차 역행방어(🐻SOXS): <code>{nq_adv:+.2f}%</code> 상승역행 (한도 <code>+{dyn_thresh:.2f}%</code>)"
        )
    elif qty_l > 0.0 and float(state_l[10] or 0.0) > 0.0:
        nq_entry_p = float(state_l[10])
        nq_entry_a = float(state_l[11] or 0.0)
        dyn_thresh = max(nq_entry_a * 0.5, NQ_DYNAMIC_FLOOR_UI)
        nq_adv = ((nq_entry_p - nq_c) / nq_entry_p * 100.0) if nq_c > 0.0 else 0.0
        macro_3rd_line = (
            f"\n▫️ 3차 역행방어(🐂SOXL): <code>{nq_adv:+.2f}%</code> 하락역행 (한도 <code>+{dyn_thresh:.2f}%</code>)"
        )
    else:
        macro_3rd_line = ""

    day_sim = get_daymarket_sim_status()

    # Case 17, 38, 44, 57, 78, 81, 83: 평시(시안 A: "🟢 중립") <-> 부스터/타격권(시안 B) 조건부 단축 RSI 렌더링
    rsi_ui_lines = []
    if shared_rsi_state:
        r_s = float((shared_rsi_state.get("SOXS") or {}).get("value", 0.0))
        r_l = float((shared_rsi_state.get("SOXL") or {}).get("value", 0.0))

        for r_sym in ("SOXS", "SOXL"):
            r_val = r_s if r_sym == "SOXS" else r_l
            opp_val = r_l if r_sym == "SOXS" else r_s
            opp_name = "SOXL" if r_sym == "SOXS" else "SOXS"

            if 358 <= t <= 359:
                warn_str = "🔇 마감휩소 묵음"
            elif 400 <= t <= 406:
                warn_str = "⏱️ 04:07 쉴드대기"
            elif is_day_time and day_sim.get("active", False) and day_sim.get("symbol") == r_sym:
                if day_sim.get("phase") == "TRAP_WAIT":
                    warn_str = f"🪤 모의1차덫(${day_sim['trap_price']:.2f})"
                else:
                    warn_str = f"🔒 모의추격(${day_sim['target_price']:.2f})"
            elif 0.0 < r_val <= 30.0:
                if opp_val >= 79.9:
                    if is_pre_sniper_time:
                        warn_str = f"🔫 대칭 타격권({opp_name} {opp_val:.1f})"
                    elif is_day_time:
                        warn_str = f"🔔 모의 타격권({opp_name} {opp_val:.1f})"
                    else:
                        warn_str = "📉 과매도(진입마감)"
                elif opp_val >= 79.0 and (is_pre_sniper_time or is_day_time):
                    warn_str = f"⚡ 0.4초 부스터({opp_name} {opp_val:.1f})"
                else:
                    warn_str = f"⏳ 과매도({opp_name} 79.9미만)"
            elif r_val >= 79.9:
                warn_str = "🔥 클라이맥스(≥79.9)"
            elif r_val >= 79.0 and (is_pre_sniper_time or is_day_time):
                warn_str = "⚡ 0.4초 부스터(≥79.0)"
            else:
                warn_str = "🟢 중립"

            if r_val > 0.0:
                icon = "🐻" if r_sym == "SOXS" else "🐂"
                rsi_ui_lines.append(f"▫️ {icon} {r_sym} RSI(1m): <code>{r_val:.1f}</code> ➡️ {warn_str}")

    rsi_ui_str = "\n".join(rsi_ui_lines) + "\n" if rsi_ui_lines else ""
    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[듀얼 스나이퍼 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물 (3단 매크로 방어)</b>
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ 현재: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code>
▫️ 저가기준 진폭: <code>{nq_curr_amp:.2f}%</code>
▫️ 1·2차 방어: <code>{macro_1st_str}</code>{macro_3rd_line}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
📊 <b>현재가 &amp; 5MA(어제 진폭) &amp; RSI(1m)</b>
🐻 <b>SOXS</b> <code>${price_s:.2f}</code> | <code>{amp_s:.1f}%({yest_amp_s:.1f}%)</code>
🐂 <b>SOXL</b> <code>${price_l:.2f}</code> | <code>{amp_l:.1f}%({yest_amp_l:.1f}%)</code>
{rsi_ui_str}
🌅 <b>프리장</b> {pre_window_label}
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['pre_l']:.2f} ~ ${sess_s['pre_h']:.2f} ({sess_s['pre_amp']:.1f}%)</code>
      | 예상: <code>${pre_exp_l_s:.2f} ~ ${pre_exp_h_s:.2f}</code>
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['pre_l']:.2f} ~ ${sess_l['pre_h']:.2f} ({sess_l['pre_amp']:.1f}%)</code>
      | 예상: <code>${pre_exp_l_l:.2f} ~ ${pre_exp_h_l:.2f}</code>

🔥 <b>정규장</b> (09:30 ~ 16:00 · 봉쇄)
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['reg_l']:.2f} ~ ${sess_s['reg_h']:.2f} ({sess_s['reg_amp']:.1f}%)</code>
      | 예상: <code>${reg_exp_l_s:.2f} ~ ${reg_exp_h_s:.2f}</code>
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['reg_l']:.2f} ~ ${sess_l['reg_h']:.2f} ({sess_l['reg_amp']:.1f}%)</code>
      | 예상: <code>${reg_exp_l_l:.2f} ~ ${reg_exp_h_l:.2f}</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
{status_s}
{status_l}
💰 <b>{comp_label_str} 시드:</b> <code>${budget_s:,.2f}</code> (목표 <code>+{active_rate_s:.2f}%</code>)

⏱️ 갱신: <code>{scan_time} EST</code>"""

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 레이더 갱신", callback_data="open_avwap")],
            [InlineKeyboardButton(text="⚙️ 전술 제어반", callback_data="open_settlement")],
            [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
        ]
    )
    return text, keyboard


async def build_sync_board(api_client) -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_str = "🌞 서머타임" if is_dst else "❄️ 서머타임 OFF"
    t = now_est.hour * 100 + now_est.minute

    if 358 <= t <= 406:
        market_state = "🌪️ 03:58~04:07 듀얼 휩소 1주 작전"
    elif 407 <= t <= 929:
        market_state = "🔫 프리장 양방향 대칭 스나이퍼 가동"
    elif 930 <= t <= 1559:
        market_state = "🔥 정규장 (신규매수 마감)"
    # MODIFIED: 16:00 ~ 18:59 EST 전 구간을 애프터마켓으로 락온하여 17:00~18:59 데이마켓 오판 해결
    elif 1600 <= t <= 1859:
        market_state = "🛑 애프터마켓 (신규매수 마감)"
    else:
        market_state = "🌙 데이마켓 (🔔[모의] 대칭 2단계 추격)"

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
        active_rate = AssassinLedger.normalize_target_rate(state[2])
        net_rate = AssassinLedger.get_net_compound_rate(active_rate)
        entry_session = state[8]
        hold = await api_client.get_symbol_holdings_detail(symbol)
        qty, avg_price = hold.get("qty", 0.0), hold.get("avg_price", 0.0)
        profit_usd, profit_rate = hold.get("profit_usd", 0.0), hold.get("profit_rate", 0.0) * 100.0
        profit_krw = profit_usd * exchange_rate
        curr = await api_client.get_current_price(symbol)

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
            "target_profit_rate": active_rate, "net_profit_rate": net_rate,
            "entry_session": entry_session, "curr": curr, "avg_price": avg_price, "qty": qty,
            "high": high, "high_rate": high_rate, "low": low, "low_rate": low_rate,
            "profit_rate": profit_rate, "profit_usd": profit_usd, "profit_krw": profit_krw,
        }

    soxs_data, soxl_data = await asyncio.gather(
        get_symbol_sync_data("SOXS"),
        get_symbol_sync_data("SOXL"),
    )

    def format_symbol(d: dict) -> str:
        sym = d["symbol"]
        icon = "🐻" if sym == "SOXS" else "🐂"
        opp_sym = "SOXL" if sym == "SOXS" else "SOXS"
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        comp_label = "📈 통합복리시드" if d["is_compound"] else "💵 단리시드"
        if d["qty"] > 0:
            if d["entry_session"] == "WHIPSAW_1SH":
                flag_str = "🌪️ [WHIPSAW_1SH: 1주 VWAP 타점 · 04:07 현재가정리]"
            else:
                flag_str = f"⚡ [{d['entry_session'] or 'HOLD'}: +{d['target_profit_rate']:.2f}% (순+{d['net_profit_rate']:.2f}%)]"
        elif 930 <= t <= 1859:
            flag_str = f"🔒 금일 신규진입 마감 (관망 · 목표 +{d['target_profit_rate']:.2f}%)"
        else:
            flag_str = f"⏳ 대기 ({sym}≤30 &amp; {opp_sym}≥79.9 · 목표 +{d['target_profit_rate']:.2f}%)"
        return (
            f"{icon} <b>[{sym}] 양방향 대칭 클라이맥스 지시서</b>\n"
            f"{comp_label}: <code>${d['budget']:,.2f}</code> | 🎯 {flag_str}\n"
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


# MODIFIED: 전술 제어반(/settlement) 상단 장문 설명을 전면 소각하고 핵심 5대 요건만 초슬림 5줄로 렌더링 (Case 37, 38, 57, 76, 77)
async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute

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

    budget_s = float(state_s[1] or 100.0)
    rate_s = AssassinLedger.normalize_target_rate(state_s[2])
    net_rate_s = AssassinLedger.get_net_compound_rate(rate_s)
    act_s = bool(state_s[5])

    active_pos_sym = ""
    for sym_chk, st_chk, bid_chk in (("SOXS", state_s, buy_id_s), ("SOXL", state_l, buy_id_l)):
        if float(st_chk[0] or 0.0) > 0.0 or float(st_chk[6] or 0.0) > 0.0 or bool(st_chk[7]) or bool(bid_chk):
            active_pos_sym = sym_chk
            break

    act_s_str = "🟢 ON" if act_s else "🔴 OFF"
    comp_s_str = "📈 통합 복리" if is_comp_s else "💵 단리 고정"
    rate_mode_desc = (
        f"🎯 +{rate_s:.2f}% (순수익 +{net_rate_s:.2f}%)"
        if abs(rate_s - 0.64) < 1e-4
        else f"🚀 +{rate_s:.2f}% (순수익 +{net_rate_s:.2f}%)"
    )

    if not act_s:
        strike_status_str = "⏸️ 가동 OFF (수동 대기)"
    elif state_s[8] == "CANCELED_MACRO" or state_l[8] == "CANCELED_MACRO":
        strike_status_str = "🛑 매크로 방어 (금일 마감)"
    elif 358 <= t <= 406:
        w_sym = whip_st.get("target_sym", "") or active_pos_sym
        if (buy_id_s or buy_id_l) and (state_s[8] == "WHIPSAW_1SH" or state_l[8] == "WHIPSAW_1SH"):
            strike_status_str = f"🌪️ [{w_sym}] 휩소 1주 매수덫 (${whip_st['buy_price']:.2f})"
        elif active_pos_sym and (state_s[8] == "WHIPSAW_1SH" or state_l[8] == "WHIPSAW_1SH"):
            tp_w = state_s[6] if active_pos_sym == "SOXS" else state_l[6]
            strike_status_str = f"🌪️ [{active_pos_sym}] 휩소 1주 보유 (목표 ${tp_w:.2f})"
        else:
            strike_status_str = "🌪️ 휩소 노출 작전중 (03:58 ~ 04:07)"
    elif 407 <= t <= 929:
        if active_pos_sym:
            st_a = state_s if active_pos_sym == "SOXS" else state_l
            bid_a = buy_id_s if active_pos_sym == "SOXS" else buy_id_l
            tp_a = float(st_a[6] or 0.0)
            es_a = str(st_a[8] or "")
            if bid_a:
                if es_a == "PRE_SNIPER":
                    strike_status_str = f"🪤 [{active_pos_sym}] 1차 매수덫 대기중"
                else:
                    strike_status_str = f"⚡ [{active_pos_sym}] 재장전 체결 대기중"
            elif tp_a > 0.0:
                strike_status_str = f"🎯 [{active_pos_sym}] 보유중 (목표 ${tp_a:.2f})"
            else:
                strike_status_str = f"🎯 [{active_pos_sym}] 보유중 (매도덫 장전중)"
        elif not bool(state_s[4]) and not bool(state_l[4]):
            strike_status_str = "🔫 프리장 본진 감시중"
        else:
            strike_status_str = "🔒 프리장 타격 잠금"
    elif 930 <= t <= 1558:
        if active_pos_sym:
            st_a = state_s if active_pos_sym == "SOXS" else state_l
            tp_a = float(st_a[6] or 0.0)
            strike_status_str = f"🔒 [{active_pos_sym}] 보유분 감시 (목표 ${tp_a:.2f})"
        else:
            strike_status_str = "🔒 정규장 마감 (관망)"
    elif 1559 <= t <= 1601:
        strike_status_str = "🛑 제로-오버나이트 청산중"
    elif t >= 1900 or t <= 357:
        if day_sim.get("active", False):
            sim_sym = day_sim.get("symbol", "SOXS")
            if day_sim.get("phase") == "TRAP_WAIT":
                strike_status_str = f"🪤 [모의·{sim_sym}] 1차덫 대기 (${day_sim['trap_price']:.2f})"
            else:
                strike_status_str = f"🔔 [모의·{sim_sym}] 추격중 (목표 ${day_sim['target_price']:.2f})"
        else:
            strike_status_str = "🌙 [모의] 데이장 감시중"
    else:
        strike_status_str = "🛑 애프터마켓 (마감)"

    btn_064_label = "✅ 🎯 +0.64% (순+0.50%)" if abs(rate_s - 0.64) < 1e-4 else "🎯 +0.64% (순+0.50%)"
    btn_100_label = "✅ 🚀 +1.00% (순+0.86%)" if abs(rate_s - 1.00) < 1e-4 else "🚀 +1.00% (순+0.86%)"

    text = (
        "⚙️ <b>[ 듀얼 스나이퍼 전술 제어반 ]</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        f"▫️ 마스터 전원: <code>{act_s_str}</code>\n"
        f"▫️ 출동 상태: <code>{strike_status_str}</code>\n"
        f"▫️ 목표 익절: <code>{rate_mode_desc}</code>\n"
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
                InlineKeyboardButton(text=btn_064_label, callback_data="set_rate_SOXS_0.64"),
                InlineKeyboardButton(text=btn_100_label, callback_data="set_rate_SOXS_1.00"),
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
