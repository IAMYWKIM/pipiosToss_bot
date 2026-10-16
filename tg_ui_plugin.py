# =====================================================================
# FILE: tg_ui_plugin.py
# 목적: 프리장 (04:07 ~ 09:29 EST) SOXS 단방향 본진 스나이퍼 및
#       03:58 ~ 04:07 EST 1주 휩소 노출 작전 텔레그램 UI 렌더링 모듈
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 데드코드 소각 및 whipsaw_plugin 상태 헬퍼 결속
# MODIFIED: 데이장(19:00 ~ 03:57 EST) [모의] 2단계 체결(TRAP_WAIT / HOLDING) 및 03:58 ~ 03:59 EST 마감 묵음 바이패스 상태 표출
# NEW: 03:58 ~ 04:07 EST (9분간) 데이장 VWAP 기반 1주 고정 '휩소 노출 작전'(WHIPSAW_1SH) 및 04:07 EST 본진 타임쉴드 상태 렌더링
# MODIFIED: 복리 모드(is_compound) 안내를 실제 발생 순수익금((매도가-매수가)*체결수량 - 수수료 0.14%) 기준으로 동기화 (Case 76)
# MODIFIED: 텔레그램 HTML 파서 크래시(Unsupported start tag) 방어를 위해 코드 블록 내 원시 부등호(<, >) 전면 소각 및 한글 치환 (Case 17)

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from quant_engine import AssassinLedger, MacroDataCache
from sniper_plugin import get_daymarket_sim_status, get_whipsaw_runtime_status

EST_TZ = ZoneInfo("America/New_York")
NQ_UI_BETA = 4.2
NQ_DYNAMIC_FLOOR_UI = 0.8


async def get_main_menu_text() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_status_text = "🌞서머타임 ON (EDT)" if is_dst else "❄️서머타임 OFF (EST)"

    state_s = await AssassinLedger.get_state("SOXS")
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    active_rate = AssassinLedger.normalize_target_rate(state_s[2])
    net_rate = AssassinLedger.get_net_compound_rate(active_rate)
    comp_mode_str = "복리 ON (실제 발생 순수익금 가감)" if is_comp_s else "단리 ON (시드 고정)"

    return (
        f"🕒 <b>[ 프리장 SOXS 단방향 스나이퍼 &amp; 휩소 노출 작전 스케줄 ({dst_status_text}) ]</b>\n"
        f"▫️ 현재 설정: <b>목표 익절 +{active_rate:.2f}% (순수익 +{net_rate:.2f}%) · {comp_mode_str}</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 19:00 ~ 03:57: 🌙 <b>0세션 데이마켓 ([모의] 2단계 추격 · 매수차단)</b>\n"
        "      (실거래 매수 100% 차단 · 15초 듀얼 캔들 스캔 및 실시간 틱 RSI 관제)\n"
        "      (📈 NQ=F 10분봉 상승추세[UP] 시: SOXS RSI ≤ 30.0 &amp; SOXL RSI ≥ 80.0 악조건 동시 충족 시 포착)\n"
        "      (📉 NQ=F 10분봉 하락추세[DOWN] 시: 악조건 면제 · SOXS RSI ≤ 30.0 단독 조건 즉시 포착)\n"
        "      (🪤 포착 시 1차 슬리피지[-0.18%] 매수 덫 + 조건부 30초 타임쉴드 가상 체결 시뮬레이션)\n"
        "      (🔒 모의 익절[+1.00%] 또는 매크로 2·3차 모의 손절 도달 전까지 중복 알림 잠금)\n"
        "🔹 03:58 ~ 03:59: 🔇 <b>데이장 마감 휩소 묵음 바이패스</b>\n"
        "      (장 전환 휩소 알림 100% 음소거 및 미결 모의 상태 자동 초기화로 본진 스나이퍼 보호)\n"
        "🔹 03:58 ~ 04:07: 🌪️ <b>실전 1주 고정 '휩소 노출 작전' (9분간 유지)</b>\n"
        "      (데이장 누적 VWAP 스냅샷 기준 시드 무관 무조건 <b>1주[quantity=1]</b> 실전 테스트)\n"
        "      (현재가 ≥ VWAP 시: VWAP ÷ 1.01 하단 1주 매수 덫 ➔ 체결 시 목표 매도 덫 = VWAP)\n"
        "      (현재가 미만 VWAP 시: 현재가 1주 즉시 매수 ➔ 체결 시 목표 매도 덫 = VWAP × 1.01)\n"
        "      (⏱️ <b>04:07 EST 타임쉴드 해제</b>: 미체결 1주 매수 즉시 취소 / 보유 중인 1주 현재가 즉시 매도)\n"
        "🔹 04:07 ~ 09:29: 🔫 <b>프리장 본진 스나이퍼 골든타임 (04:07 타임쉴드 해제 후 출동)</b>\n"
        "      (NQ=F 당일 진폭 2.0% 미만 &amp; SOXS 실시간 RSI ≤ 30.0 감시)\n"
        "      (🪤 포착 즉시 슬리피지[-0.18%] 할인 1차 지정가 매수 덫 선[先] 투하)\n"
        "      (⚡ 30초 내 RSI 30.0 초과 반등 시 타임쉴드 즉시 해제 &amp; 현재가 즉각 매수)\n"
        "      (🔄 RSI ≤ 30.0 유지 시 30초간 저가 덫 대기 후 미체결 시 현재가 재장전)\n"
        f"      (전량 체결 확인 즉시 팩트 평단가 +{active_rate:.2f}% 매도 덫 하드 락온 · 익절 시 09:29까지 무한 재출동)\n"
        "🔹 09:30 ~ 15:58: 🔥 <b>정규장 본장 (09:30 신규 매수 마감)</b>\n"
        "      (09:30 정각 프리장 미체결 매수 주문 즉각 선취소)\n"
        f"      (보유 물량 +{active_rate:.2f}% 익절 홀딩 &amp; NQ=F 2·3차 매크로 손절 감시)\n"
        "🔹 15:59 ~ 16:01: 🛑 제로-오버나이트 MOC 매수 1호가 스윕 강제 청산\n"
        "🔹 17:00: 🧹 정산 스캔 및 가비지 컬렉션(GC)\n\n"
        "🛠 <b>[ 핵심 명령어 ]</b>\n"
        "▶️ /avwap : 🔫 프리장 스나이퍼 레이더 관제탑\n"
        "▶️ /sync : 📜 SOXS 스나이퍼 지시서 동기화\n"
        "▶️ /settlement : ⚙️ 전술 제어반 (+0.64% / +1.00% · 실수익 복리/단리 · 시드)\n\n"
        "⚠️ /reset : 🧹 SOXS 장부 0점 소각\n"
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


# MODIFIED: Case 13 & 초과 Case 57 — 03:58~04:07 1주 휩소 노출 작전, 04:07~09:29 본진 스나이퍼,
#           데이장 2단계(TRAP_WAIT / HOLDING) [모의] 추격 및 03:58~03:59 묵음 바이패스 상태 표출
def build_compact_status(
    symbol: str,
    state_tuple: tuple,
    buy_order_id: str,
    current_session: str,
    est_time_int: int,
    qty: float,
    is_compound: bool = True,
    nq_trend: str = "DOWN",
) -> str:
    budget = state_tuple[1]
    active_rate = AssassinLedger.normalize_target_rate(state_tuple[2])
    is_done = state_tuple[4]
    is_active = state_tuple[5]
    target_sell_price = state_tuple[6]
    entry_session = state_tuple[8]
    state_flag = "ON" if is_active else "OFF"
    comp_tag = "실수익복리" if is_compound else "단리"
    day_sim = get_daymarket_sim_status()
    whip_st = get_whipsaw_runtime_status()

    if not is_active:
        state_text = "가동OFF(수동대기)"
    elif qty > 0:
        if entry_session == "WHIPSAW_1SH":
            if target_sell_price > 0.0:
                state_text = f"🌪️ 휩소1주 보유(목표 ${target_sell_price:.2f} · 04:07 현재가정리)"
            else:
                state_text = "🌪️ 휩소1주 보유(VWAP 매도덫 장전중)"
        elif target_sell_price > 0.0:
            state_text = f"보유({int(qty)}주 · 목표 ${target_sell_price:.2f} [+{active_rate:.2f}%])"
        else:
            state_text = f"보유({int(qty)}주 · +{active_rate:.2f}% 덫 장전중)"
    elif buy_order_id:
        if entry_session == "WHIPSAW_1SH":
            w_bp = whip_st.get("buy_price", 0.0)
            state_text = f"🌪️ 휩소1주 매수대기(${w_bp:.2f} · 04:07 미체결취소)"
        elif entry_session == "PRE_SNIPER":
            state_text = "🪤 1차 매수덫(RSI≤30 30초쉴드·30초과시 즉각매수)"
        else:
            state_text = f"⚡ 재장전 매수 체결대기(목표+{active_rate:.2f}%)"
    elif entry_session == "CANCELED_MACRO":
        state_text = "🛑 매크로방어 발동(금일 마감)"
    elif 358 <= est_time_int <= 406:
        if whip_st.get("locked_vwap", 0.0) > 0.0:
            state_text = (
                f"🌪️ 휩소1주 작전중(VWAP ${whip_st['locked_vwap']:.2f} · 본진 04:07 타임쉴드)"
            )
        else:
            state_text = "🌪️ 03:58~04:07 휩소작전 구간(데이장모의 묵음 · 본진 04:07 대기)"
    elif current_session == "dayMarket":
        if day_sim.get("active", False):
            if day_sim.get("phase") == "TRAP_WAIT":
                state_text = (
                    f"🪤 데이장[모의] 1차덫(${day_sim['trap_price']:.2f}) 30초쉴드 대기"
                )
            else:
                state_text = (
                    f"🔔 데이장[모의] 추격(${day_sim['entry_price']:.2f}➡️${day_sim['target_price']:.2f} 도달전 잠금)"
                )
        elif nq_trend == "UP":
            state_text = "🔔 데이장[모의] 감시(NQ상승: SOXS≤30 & SOXL≥80)"
        else:
            state_text = "🔔 데이장[모의] 감시(NQ하락: SOXS≤30 단독)"
    elif 407 <= est_time_int <= 929:
        if not is_done:
            state_text = f"🔫 프리장 본진 RSI≤30 감시중(목표+{active_rate:.2f}%)"
        else:
            state_text = "🔒 금일 프리장 타격마감"
    elif 930 <= est_time_int < 1600:
        state_text = "🔒 09:30 정규장 진입마감(관망)"
    else:
        state_text = "장외대기"

    return f"🐻 <b>{symbol}</b> <code>[{state_flag}·{comp_tag}]</code> {state_text} | <code>${budget:,.2f}</code>"


async def build_avwap_radar(api_client, shared_rsi_state=None) -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute
    is_day_time = (t >= 1900 or t <= 359)
    is_whipsaw_time = (358 <= t <= 406)
    is_pre_sniper_time = (407 <= t <= 929)

    if is_whipsaw_time:
        session_name_ui = "preMarket" if t >= 400 else "dayMarket"
        market_header = "( <b>🌪️ 03:58~04:07 1주 휩소작전 · 본진 04:07 타임쉴드</b> )"
        pre_window_label = "(03:58~04:07 휩소1주 / 04:07~09:29 본진 출동)"
    elif is_pre_sniper_time:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 PRE_MARKET · 본진 스나이퍼 골든타임</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 무한 출동 가동중)"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 REG_MARKET · 신규진입 마감</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 출동 마감)"
    elif 1600 <= t <= 1659:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 AFT_MARKET</b> )"
        pre_window_label = "(04:07 ~ 09:29 · 출동 마감)"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>🌙 데이마켓 · 🔔[모의] 2단계 추세연동 감시</b> )"
        pre_window_label = "(03:58 휩소1주 · 04:07 본진 개장 대기)"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(api_client.is_market_open(), timeout=3.0)
        if not is_open:
            session_name_ui = "dayMarket"
            market_header = (
                "( <b>🌙 주말/휴장일</b> )" if sess_name and "HOLIDAY" in sess_name else "( <b>🌙 시스템 대기</b> )"
            )
    except Exception:
        pass

    nq_trend, nq_ema_diff, nq_ema6_str, nq_c, nq_h, nq_l = await MacroDataCache.get_nq_sniper_signal()
    nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0
    nq_curr_amp = (max(0.0, ((nq_c - nq_l) / nq_l * 100.0)) if (nq_l > 0.0 and nq_c > 0.0) else 0.0)
    etf_expected_amp_ui = nq_amp * NQ_UI_BETA

    if nq_trend == "UP":
        nq_trend_ui_str = f"📈 상승추세[UP] (EMA6 {nq_ema6_str} 대비 {nq_ema_diff:+.2f}% · 데이장 SOXL≥80 악조건)"
    else:
        nq_trend_ui_str = f"📉 하락추세[DOWN] (EMA6 {nq_ema6_str} 대비 {nq_ema_diff:+.2f}% · 데이장 SOXS≤30 단독)"

    price_s = await api_client.get_current_price("SOXS")
    hold_s = await api_client.get_symbol_holdings_detail("SOXS")
    qty_s = float(hold_s.get("qty", 0.0))
    amp_s, yest_amp_s, _ = await _fetch_5ma_amp(api_client, "SOXS")

    empty_sess = {
        "pre_h": 0.0, "pre_l": 0.0, "pre_amp": 0.0, "pre_vwap": 0.0, "pre_body": 0.0,
        "reg_h": 0.0, "reg_l": 0.0, "reg_amp": 0.0, "reg_vwap": 0.0, "reg_body": 0.0,
    }
    if session_name_ui == "dayMarket":
        sess_s = dict(empty_sess)
    else:
        sess_s = await _fetch_session_stats(api_client, "SOXS", now_est)

    pre_exp_l_s = sess_s["pre_h"] * (1.0 - amp_s / 100.0) if sess_s["pre_h"] > 0 else 0.0
    pre_exp_h_s = sess_s["pre_l"] * (1.0 + amp_s / 100.0) if sess_s["pre_l"] > 0 else 0.0
    reg_exp_l_s = sess_s["reg_h"] * (1.0 - amp_s / 100.0) if sess_s["reg_h"] > 0 else 0.0
    reg_exp_h_s = sess_s["reg_l"] * (1.0 + amp_s / 100.0) if sess_s["reg_l"] > 0 else 0.0

    state_s = await AssassinLedger.get_state("SOXS")
    buy_id_s = await AssassinLedger.get_buy_order_id("SOXS")
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    status_s = build_compact_status(
        "SOXS", state_s, buy_id_s, session_name_ui, t, qty_s, is_comp_s, nq_trend=nq_trend
    )

    # Case 17 텔레그램 HTML 파서 충돌 방지를 위해 원시 부등호('<', '>') 제거 및 한글 치환
    nq_entry_p = float(state_s[10] or 0.0)
    nq_entry_a = float(state_s[11] or 0.0)
    macro_1st_str = "🛑 과열차단(≥2.0%)" if nq_amp >= 2.0 else "🟢 진입허용(2.0%미만)"
    if qty_s > 0.0 and nq_entry_p > 0.0:
        dyn_thresh = max(nq_entry_a * 0.5, NQ_DYNAMIC_FLOOR_UI)
        nq_diff = ((nq_c - nq_entry_p) / nq_entry_p * 100.0) if nq_c > 0.0 else 0.0
        macro_3rd_line = (
            f"\n▫️ 3차 역행방어: 기준 <code>{nq_entry_p:.2f}</code> 대비 "
            f"<code>{nq_diff:+.2f}%</code> (한도 <code>+{dyn_thresh:.2f}%</code>)"
        )
    else:
        macro_3rd_line = ""

    day_sim = get_daymarket_sim_status()

    # 세션별(04:07~09:29 프리장 본진 / 03:58~04:07 휩소1주 / 데이장 2단계 모의추격) 듀얼 RSI 관제 상태 렌더링 (Case 17, 44, 53, 78)
    rsi_ui_lines = []
    day_vwap_ui = 0.0
    if shared_rsi_state:
        soxl_r_val = float((shared_rsi_state.get("SOXL") or {}).get("value", 0.0))
        day_vwap_ui = float((shared_rsi_state.get("SOXS") or {}).get("vwap", 0.0))
        for r_sym in ("SOXS", "SOXL"):
            if r_sym in shared_rsi_state:
                rsi_data = shared_rsi_state[r_sym]
                r_val = float(rsi_data.get("value", 0.0))

                if r_sym == "SOXS":
                    if 358 <= t <= 359:
                        warn_str = "🔇 마감휩소 묵음구간(03:58~03:59)"
                    elif 400 <= t <= 406:
                        warn_str = "⏱️ 본진 04:07 타임쉴드 대기중"
                    elif is_day_time and day_sim.get("active", False):
                        if day_sim.get("phase") == "TRAP_WAIT":
                            warn_str = f"🪤 모의 1차덫(${day_sim['trap_price']:.2f}) 30초쉴드"
                        else:
                            warn_str = f"🔒 모의 +1% 추격중(목표 ${day_sim['target_price']:.2f})"
                    elif 0.0 < r_val <= 30.0:
                        if is_pre_sniper_time:
                            warn_str = "🔫 본진 스나이퍼 타격권(≤30)"
                        elif is_day_time:
                            if nq_trend == "UP" and soxl_r_val < 80.0:
                                warn_str = "⏳ 대기(≤30 · NQ상승 SOXL 80미만 차단)"
                            else:
                                warn_str = "🔔 데이장 모의포착(≤30)"
                        else:
                            warn_str = "📉 과매도(≤30 · 진입마감)"
                    else:
                        warn_str = "🟢 안정(30초과)"
                else:
                    if r_val >= 80.0:
                        if is_day_time:
                            warn_str = "🔥 과열극단(≥80 · 데이장 악조건충족)"
                        else:
                            warn_str = "🔥 과매수(≥80 · 매매차단)"
                    elif 0.0 < r_val <= 30.0:
                        warn_str = "📉 과매도(≤30 · 매매차단)"
                    else:
                        warn_str = "🟢 중립(80미만 · 매매차단)"

                if r_val > 0.0:
                    icon = "🐻" if r_sym == "SOXS" else "🐂"
                    rsi_ui_lines.append(f"▫️ {icon} {r_sym} RSI(1m): <code>{r_val:.1f}</code> ➡️ {warn_str}")

    rsi_ui_str = "\n".join(rsi_ui_lines) + "\n" if rsi_ui_lines else ""
    day_vwap_line = f"▫️ 🌙 당일 누적(데이장) VWAP: <code>${day_vwap_ui:.2f}</code>\n" if day_vwap_ui > 0.0 else ""

    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[프리장 SOXS 스나이퍼 &amp; 03:58 휩소 작전 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물 (NQ=F · 3단 매크로 방어 &amp; 10분봉 추세)</b>
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ 현재: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code> (레버 <code>{etf_expected_amp_ui:.1f}%</code>)
▫️ 10분봉 추세: <code>{nq_trend_ui_str}</code>
▫️ 저가기준 진폭: <code>{nq_curr_amp:.2f}%</code> | 1·2차 방어: <code>{macro_1st_str}</code>{macro_3rd_line}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
📊 <b>현재가 &amp; 5MA(어제 진폭) &amp; 듀얼 RSI(1m)</b>
🐻 <b>SOXS</b> <code>${price_s:.2f}</code> | <code>{amp_s:.1f}%({yest_amp_s:.1f}%)</code>
{day_vwap_line}{rsi_ui_str}
🌅 <b>프리장</b> {pre_window_label}
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['pre_l']:.2f} ~ ${sess_s['pre_h']:.2f} ({sess_s['pre_amp']:.1f}%)</code> | 예상: <code>${pre_exp_l_s:.2f} ~ ${pre_exp_h_s:.2f}</code>

🔥 <b>정규장</b> (09:30 ~ 16:00 · 신규진입 봉쇄)
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['reg_l']:.2f} ~ ${sess_s['reg_h']:.2f} ({sess_s['reg_amp']:.1f}%)</code> | 예상: <code>${reg_exp_l_s:.2f} ~ ${reg_exp_h_s:.2f}</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
{status_s}

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
        market_state = "🌪️ 03:58~04:07 휩소 1주 작전"
    elif 407 <= t <= 929:
        market_state = "🔫 프리장 본진 스나이퍼 가동"
    elif 930 <= t <= 1559:
        market_state = "🔥 정규장 (신규매수 마감)"
    elif 1600 <= t <= 1659:
        market_state = "⛔ 장마감"
    else:
        market_state = "🌙 데이마켓 (🔔[모의] 2단계 추격)"

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

    soxs_data = await get_symbol_sync_data("SOXS")

    def format_symbol(d: dict) -> str:
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        comp_label = "📈 실수익복리시드" if d["is_compound"] else "💵 단리시드"
        if d["qty"] > 0:
            if d["entry_session"] == "WHIPSAW_1SH":
                flag_str = "🌪️ [WHIPSAW_1SH: 1주 VWAP 타점 · 04:07 현재가정리]"
            else:
                flag_str = f"⚡ [{d['entry_session'] or 'HOLD'}: +{d['target_profit_rate']:.2f}% (순+{d['net_profit_rate']:.2f}%)]"
        else:
            flag_str = f"⏳ 대기 (목표 +{d['target_profit_rate']:.2f}% · 순수익 +{d['net_profit_rate']:.2f}%)"
        return (
            f"🐻 <b>[SOXS] 프리장 RSI≤30 스나이퍼 지시서</b>\n"
            f"{comp_label}: <code>${d['budget']:,.2f}</code> | 🎯 {flag_str}\n"
            f"💰 현재 ${d['curr']:.2f} / 평단 ${d['avg_price']:.2f} ({int(d['qty'])}주)\n"
            f"📈 금일 고가: ${d['high']:.2f} ({d['high_rate']:+.2f}%)\n"
            f"📉 금일 저가: ${d['low']:.2f} ({d['low_rate']:+.2f}%)\n"
            f"🔺 수익: {d['profit_rate']:+.2f}% ({profit_sign}${abs(d['profit_usd']):,.2f} | {profit_sign}₩{int(abs(d['profit_krw'])):,})"
        )

    text = (
        f"📜 <b>[ SOXS 단독 지시서 ({market_state}) ]</b>\n"
        f"📅 {dst_str} ({now_est.strftime('%H:%M')} EST)\n"
        f"💵 주문가능금액: ${bp:,.2f}\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        f"{format_symbol(soxs_data)}\n\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "▶️ /avwap : 🔫 스나이퍼 레이더 관제탑"
    )
    return text


# MODIFIED: 전술 제어반(/settlement) 내 03:58~04:07 휩소 1주 작전, 04:07 타임쉴드, 데이장 2단계 [모의] 추격 및 실수익 복리 상태 렌더링
async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute

    state_s = await AssassinLedger.get_state("SOXS")
    buy_id_s = await AssassinLedger.get_buy_order_id("SOXS")
    is_comp_s = await AssassinLedger.get_is_compound("SOXS")
    day_sim = get_daymarket_sim_status()
    whip_st = get_whipsaw_runtime_status()
    nq_trend, _, _, _, _, _ = await MacroDataCache.get_nq_sniper_signal()

    last_buy_p = float(state_s[0] or 0.0)
    budget_s = float(state_s[1] or 100.0)
    rate_s = AssassinLedger.normalize_target_rate(state_s[2])
    net_rate_s = AssassinLedger.get_net_compound_rate(rate_s)
    is_done = bool(state_s[4])
    act_s = bool(state_s[5])
    target_sell_p = float(state_s[6] or 0.0)
    sell_id_s = str(state_s[7] or "")
    entry_sess = str(state_s[8] or "")

    act_s_str = "🟢 ON (시스템 활성)" if act_s else "🔴 OFF (수동 통제)"
    comp_s_str = (
        "📈 복리 모드 (실제 발생 순수익금[세전손익 - 왕복수수료 0.14%] 정밀 가감)"
        if is_comp_s
        else "💵 단리 모드 (설정 시드 고정 출동)"
    )
    rate_mode_desc = (
        "🎯 고승률 회전형 (+0.64% 익절 · 5개년 승률 90.6%)"
        if abs(rate_s - 0.64) < 1e-4
        else "🚀 고수익 복리형 (+1.00% 익절 · 5개년 복리 +720.54%)"
    )
    has_position_in_ledger = (target_sell_p > 0.0 or bool(sell_id_s) or last_buy_p > 0.0)

    if not act_s:
        strike_status_str = "⏸️ 가동OFF (자동 매수·방어·MOC·알림 전면 중지)"
    elif entry_sess == "CANCELED_MACRO":
        strike_status_str = "🛑 매크로 방어 발동 (금일 신규진입 영구 소각)"
    elif 358 <= t <= 406:
        if buy_id_s and entry_sess == "WHIPSAW_1SH":
            strike_status_str = (
                f"🌪️ 휩소 1주 매수대기 (${whip_st['buy_price']:.2f} ➡️ 목표 ${whip_st['sell_target']:.2f} · 04:07 미체결 시 취소)"
            )
        elif has_position_in_ledger and entry_sess == "WHIPSAW_1SH":
            strike_status_str = (
                f"🌪️ 휩소 1주 보유중 (목표 ${target_sell_p:.2f} · 04:07 미매도 시 현재가 즉시처분)"
            )
        else:
            strike_status_str = "🌪️ 03:58 ~ 04:07 휩소 1주 작전 구간 (데이장모의 묵음 · 본진 04:07 타임쉴드)"
    elif 407 <= t <= 929:
        if buy_id_s:
            if entry_sess == "PRE_SNIPER":
                strike_status_str = "🪤 1차 슬리피지 매수덫 대기 (RSI≤30 30초쉴드 · 30초과 시 즉각매수)"
            else:
                strike_status_str = "⚡ 타임쉴드 현재가 재장전 체결 대기중"
        elif has_position_in_ledger:
            if target_sell_p > 0.0:
                strike_status_str = f"🎯 보유중 (목표 ${target_sell_p:.2f} [+{rate_s:.2f}%] 익절 시 무한 재출동)"
            else:
                strike_status_str = f"🎯 보유중 (+{rate_s:.2f}% 익절 덫 장전중)"
        elif not is_done:
            strike_status_str = "🔫 04:07~09:29 본진 RSI≤30 무한 출동 감시중 (신규진입 가능)"
        else:
            strike_status_str = "🔒 금일 프리장 타격 잠금"
    elif 930 <= t <= 1558:
        if has_position_in_ledger:
            if target_sell_p > 0.0:
                strike_status_str = f"🔒 09:30 신규진입 마감 (보유분 ${target_sell_p:.2f} [+{rate_s:.2f}%] 감시)"
            else:
                strike_status_str = f"🔒 09:30 신규진입 마감 (보유분 +{rate_s:.2f}% 감시)"
        else:
            strike_status_str = "🔒 09:30 정규장 신규진입 봉쇄 (관망 · 매수불가)"
    elif 1559 <= t <= 1601:
        strike_status_str = "🛑 15:59 ~ 16:01 제로-오버나이트 MOC 강제 청산 윈도우"
    elif t >= 1900 or t <= 357:
        if day_sim.get("active", False):
            if day_sim.get("phase") == "TRAP_WAIT":
                strike_status_str = (
                    f"🪤 데이장 [모의] 1차 슬리피지덫(${day_sim['trap_price']:.2f}) 30초 타임쉴드 대기중"
                )
            else:
                strike_status_str = (
                    f"🔔 데이장 [모의] +1.00% 추격중 (체결 ${day_sim['entry_price']:.2f} ➡️ 목표 ${day_sim['target_price']:.2f} 도달 전 잠금)"
                )
        elif nq_trend == "UP":
            strike_status_str = "🌙 데이마켓 대기 (NQ 10분봉 상승[UP] ➔ SOXS≤30 & SOXL≥80 악조건 감시중)"
        else:
            strike_status_str = "🌙 데이마켓 대기 (NQ 10분봉 하락[DOWN] ➔ SOXS≤30 단독 조건 감시중)"
    else:
        strike_status_str = "⛔ 장외/애프터장 대기 (신규진입 차단)"

    btn_064_label = "✅ 🎯 +0.64% (순+0.50%)" if abs(rate_s - 0.64) < 1e-4 else "🎯 +0.64% (순+0.50%)"
    btn_100_label = "✅ 🚀 +1.00% (순+0.86%)" if abs(rate_s - 1.00) < 1e-4 else "🚀 +1.00% (순+0.86%)"

    text = (
        "⚙️ <b>[단방향 전술 코어 제어반 (프리장 SOXS 스나이퍼)]</b>\n"
        "▫️ <b>본진 모드:</b> <code>🔫 프리장 (04:07 ~ 09:29 EST) 1차 슬리피지 매수덫 + 조건부 30초 타임쉴드</code>\n"
        "▫️ <b>휩소 작전:</b> <code>🌪️ 03:58 ~ 04:07 EST (9분간) 데이장 VWAP 역산 1주 실전 테스트 (04:07 정리)</code>\n"
        "▫️ <b>데이장 모드:</b> <code>🔔 19:00 ~ 03:57 EST [모의] 2단계 추격 · 03:58 ~ 03:59 EST 마감휩소 묵음</code>\n"
        "▫️ <b>방어 체계:</b> <code>NQ=F 3단 매크로 손절 덤핑 &amp; 15:59 ~ 16:01 제로-오버나이트 MOC</code>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🐻 <b>SOXS (SHORT · 단방향 스나이퍼)</b>\n"
        f"▫️ 마스터 전원: <code>{act_s_str}</code>\n"
        f"▫️ 현재 출동상태: <code>{strike_status_str}</code>\n"
        f"▫️ 목표 익절타점: <code>{rate_mode_desc}</code>\n"
        f"▫️ 시드 운용방식: <code>{comp_s_str}</code>\n"
        f"▫️ 현재 출동시드: <code>${budget_s:,.2f}</code> | 목표 익절: <code>+{rate_s:.2f}% (순수익 +{net_rate_s:.2f}%)</code>"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔴 SOXS 스나이퍼 정지" if act_s else "🟢 SOXS 스나이퍼 가동",
                    callback_data="toggle_set_act_SOXS",
                )
            ],
            [
                InlineKeyboardButton(text=btn_064_label, callback_data="set_rate_SOXS_0.64"),
                InlineKeyboardButton(text=btn_100_label, callback_data="set_rate_SOXS_1.00"),
            ],
            [
                InlineKeyboardButton(
                    text="💵 단리(시드고정) 전환" if is_comp_s else "📈 복리(실수익가감) 전환",
                    callback_data="toggle_compound_SOXS",
                ),
                InlineKeyboardButton(text="💵 SOXS 시드 설정", callback_data="set_budget_SOXS"),
            ],
            [
                InlineKeyboardButton(text="🔫 스나이퍼 레이더", callback_data="open_avwap"),
                InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main"),
            ],
        ]
    )
    return text, keyboard
