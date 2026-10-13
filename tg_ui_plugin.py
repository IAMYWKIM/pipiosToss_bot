# =====================================================================
# FILE: tg_ui_plugin.py
# 목적: 02:58 EST 단방향(SOXS) 전용 텔레그램 데이터 연산 및 UI 렌더링 전용 모듈
# =====================================================================

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from quant_engine import AssassinLedger, MacroDataCache

EST_TZ = ZoneInfo("America/New_York")
NQ_UI_BETA = 4.2

def get_main_menu_text() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_status_text = "🌞서머타임 ON (EDT)" if is_dst else "❄️서머타임 OFF (EST)"

    return (
        f"🕒 <b>[ 02:58 숏(SOXS) 전용 스나이퍼 스케줄 ({dst_status_text}) ]</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 19:00~02:57: 🌙 0세션 시스템 대기 (매수 차단)\n"
        "🔹 02:58:00: ⚡ 데이장 마감 직전 숏(SOXS) 선제 지정가 타격\n"
        "      (NQ=F 진폭 2.0% 미만 시 즉각 진입)\n"
        "      (전량 체결 확인 즉시 +0.64% 매도 덫 하드 락온)\n"
        "🔹 02:59~15:58: 🎯 +0.64% 익절 감시 & NQ 매크로 방어\n"
        "      (장중 NQ 상승(손실방향) 임계치 이탈 시 덤핑)\n"
        "🔹 15:59~16:01: 🛑 제로-오버나이트 MOC 강제 덤핑\n"
        "🔹 17:00: 🧹 정산 스캔 및 가비지 컬렉션(GC)\n\n"
        "🛠 <b>[ 핵심 명령어 ]</b>\n"
        "▶️ /avwap : 🔫 스나이퍼 레이더 관제탑\n"
        "▶️ /sync : 📜 SOXS 전용 지시서 동기화\n"
        "▶️ /settlement : ⚙️ 단일 전술 제어반 (시드/가동)\n\n"
        "⚠️ /reset : 🧹 SOXS 장부 0점 소각\n"
        "⚠️ /update : 🚀 시스템 자가 업데이트\n\n"
        "🌙 <b>수동 오버나이트를 원할 경우 [전술 제어반]에서 가동을 OFF 해주세요.</b>"
    )

def parse_session_data(all_candles: list, session_start_est: datetime) -> dict:
    res = {
        "pre_h": 0.0, "pre_l": 0.0, "pre_amp": 0.0, "pre_vwap": 0.0, "pre_body": 0.0,
        "reg_h": 0.0, "reg_l": 0.0, "reg_amp": 0.0, "reg_vwap": 0.0, "reg_body": 0.0,
    }
    if not all_candles: return res
    df = pd.DataFrame(all_candles)
    if df.empty: return res
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).dt.tz_convert(EST_TZ)
    df.set_index("timestamp", inplace=True)
    df.sort_index(ascending=True, inplace=True)

    for col in ["openPrice", "highPrice", "lowPrice", "closePrice", "volume"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[df.index >= session_start_est]
    if df.empty: return res

    pre_df = df.between_time("04:00", "09:29")
    reg_df = df.between_time("09:30", "16:00")

    def calc_metrics(sub_df: pd.DataFrame):
        if sub_df.empty: return 0.0, 0.0, 0.0, 0.0, 0.0
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
    res.update({"pre_h": pre_h, "pre_l": pre_l, "pre_amp": pre_amp, "pre_vwap": pre_vwap, "pre_body": pre_body,
                "reg_h": reg_h, "reg_l": reg_l, "reg_amp": reg_amp, "reg_vwap": reg_vwap, "reg_body": reg_body})
    return res

async def _fetch_5ma_amp(api_client, symbol: str) -> tuple[float, float, float]:
    avg_amp, yesterday_amp, yesterday_return = 0.0, 0.0, 0.0
    try:
        endpoint = f"/api/v1/candles?symbol={symbol}&interval=1d&count=6"
        data = await api_client._request("GET", endpoint, "MARKET_DATA_CHART", headers=api_client._get_headers())
        candles = (data or {}).get("result", {}).get("candles", [])
        if not candles: return 0.0, 0.0, 0.0

        c0_dt = pd.to_datetime(candles[0]["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ).date()
        now_est_check = datetime.now(EST_TZ)
        today_dt = now_est_check.date()

        if c0_dt > today_dt: valid_candles = candles[1:6]
        elif c0_dt == today_dt:
            if 16 <= now_est_check.hour < 19: valid_candles = candles[0:5]
            else: valid_candles = candles[1:6]
        else: valid_candles = candles[0:5]

        amps = []
        for i, c in enumerate(valid_candles):
            h = float(c.get("highPrice") or 0.0)
            l = float(c.get("lowPrice") or 0.0)
            if l > 0.0:
                amp = (h - l) / l * 100.0
                amps.append(amp)
                if i == 0: yesterday_amp = amp

        if len(valid_candles) >= 2:
            yest_cls = float(valid_candles[0].get("closePrice") or 0.0)
            prev_cls = float(valid_candles[1].get("closePrice") or 0.0)
            if prev_cls > 0.0: yesterday_return = ((yest_cls - prev_cls) / prev_cls) * 100.0

        if amps: avg_amp = sum(amps) / len(amps)
        return avg_amp, yesterday_amp, yesterday_return
    except Exception:
        return 0.0, 0.0, 0.0

async def _fetch_session_stats(api_client, symbol: str, now_est: datetime) -> dict:
    all_candles = []
    before = None
    if now_est.hour >= 4: session_start_est = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
    else: session_start_est = (now_est - timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)

    for _ in range(8):
        try:
            data = await api_client.get_1m_candles_pagination(symbol, count=200, before=before)
            candles = data.get("candles", [])
            all_candles.extend(candles)
            if not candles: break
            oldest_time = pd.to_datetime(candles[-1]["timestamp"], utc=True).tz_convert(EST_TZ)
            if oldest_time <= session_start_est: break
            before = data.get("nextBefore")
            if not before: break
        except Exception: break
    return await asyncio.to_thread(parse_session_data, all_candles, session_start_est)

def build_compact_status(symbol: str, state_tuple: tuple, buy_order_id: str, current_session: str, est_time_int: int, qty: float) -> str:
    budget = state_tuple[1]
    target_profit_rate = state_tuple[2]
    is_done = state_tuple[4]
    is_active = state_tuple[5]
    state_flag = "ON" if is_active else "OFF"

    if not is_active: state_text = "가동OFF(수동대기)"
    elif qty > 0: state_text = f"보유(+{target_profit_rate}%)"
    elif current_session == "dayMarket": state_text = "🌙 시스템 대기(02:58 타격 대기)"
    elif buy_order_id: state_text = "⏳ 지정가 매수 전량체결 대기"
    elif est_time_int == 258 and not is_done: state_text = "⚡ 02:58 단방향 스나이핑 타격 중"
    elif is_done: state_text = "🔒 금일 타격완료(대기)"
    else: state_text = "장외대기"

    return f"🐻 <b>{symbol}</b> <code>[{state_flag}]</code> {state_text} | <code>${budget:.0f}</code>"

async def build_avwap_radar(api_client) -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute

    if 400 <= t <= 929:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 PRE_MARKET</b> )"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 REG_MARKET</b> )"
    elif 1600 <= t <= 1659:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 AFT_MARKET</b> )"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>⚡ 02:58 데이장 마감 타격 대기</b> )" if t == 258 else "( <b>🌙 시스템 대기</b> )"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(api_client.is_market_open(), timeout=3.0)
        if not is_open:
            session_name_ui = "dayMarket"
            market_header = "( <b>🌙 주말/휴장일</b> )" if sess_name and "HOLIDAY" in sess_name else "( <b>🌙 시스템 대기</b> )"
    except Exception: pass

    _, _, _, nq_c, nq_h, nq_l = await MacroDataCache.get_nq_sniper_signal()
    nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0
    nq_curr_amp = (max(0.0, ((nq_c - nq_l) / nq_l * 100.0)) if (nq_l > 0.0 and nq_c > 0.0) else 0.0)
    etf_expected_amp_ui = nq_amp * NQ_UI_BETA

    price_s = await api_client.get_current_price("SOXS")
    hold_s = await api_client.get_symbol_holdings_detail("SOXS")
    amp_s, yest_amp_s, yest_return_s = await _fetch_5ma_amp(api_client, "SOXS")

    empty_sess = {"pre_h": 0.0, "pre_l": 0.0, "pre_amp": 0.0, "pre_vwap": 0.0, "pre_body": 0.0, "reg_h": 0.0, "reg_l": 0.0, "reg_amp": 0.0, "reg_vwap": 0.0, "reg_body": 0.0}
    if session_name_ui == "dayMarket": sess_s = dict(empty_sess)
    else: sess_s = await _fetch_session_stats(api_client, "SOXS", now_est)

    pre_exp_l_s = sess_s["pre_h"] * (1.0 - amp_s / 100.0) if sess_s["pre_h"] > 0 else 0.0
    pre_exp_h_s = sess_s["pre_l"] * (1.0 + amp_s / 100.0) if sess_s["pre_l"] > 0 else 0.0
    reg_exp_l_s = sess_s["reg_h"] * (1.0 - amp_s / 100.0) if sess_s["reg_h"] > 0 else 0.0
    reg_exp_h_s = sess_s["reg_l"] * (1.0 + amp_s / 100.0) if sess_s["reg_l"] > 0 else 0.0

    state_s = await AssassinLedger.get_state("SOXS")
    buy_id_s = await AssassinLedger.get_buy_order_id("SOXS")
    status_s = build_compact_status("SOXS", state_s, buy_id_s, session_name_ui, t, hold_s.get("qty", 0.0))

    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[02:58 숏 전용 스나이퍼 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물 (NQ=F)</b>
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ 현재: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code> (레버 <code>{etf_expected_amp_ui:.1f}%</code>)
▫️ 저가기준 현재지수 진폭: <code>{nq_curr_amp:.2f}%</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
📊 <b>현재가 & 5MA(어제 진폭)</b>
🐻 <b>SOXS</b> <code>${price_s:.2f}</code> | <code>{amp_s:.1f}%({yest_amp_s:.1f}%)</code>

🌅 <b>프리장</b> (04:00~09:29)
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['pre_l']:.2f}~${sess_s['pre_h']:.2f} ({sess_s['pre_amp']:.1f}%)</code> | 예상: <code>${pre_exp_l_s:.2f}~${pre_exp_h_s:.2f}</code>

🔥 <b>정규장</b> (09:30~16:00)
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['reg_l']:.2f}~${sess_s['reg_h']:.2f} ({sess_s['reg_amp']:.1f}%)</code> | 예상: <code>${reg_exp_l_s:.2f}~${reg_exp_h_s:.2f}</code>
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
    
    if 400 <= t <= 929: market_state = "🌅 프리장 가동"
    elif 930 <= t <= 1559: market_state = "🔥 정규장 가동"
    elif 1600 <= t <= 1659: market_state = "⛔ 장마감"
    else: market_state = "⚡ 02:58 선제 타격" if t == 258 else "🌙 시스템 대기"

    bp = await api_client.get_usd_buying_power()
    exchange_rate = 1400.0
    try:
        ex_data = await api_client._request("GET", "/api/v1/exchange-rate?baseCurrency=USD&quoteCurrency=KRW", "MARKET_INFO", headers=api_client._get_headers())
        exchange_rate = float((ex_data or {}).get("result", {}).get("rate", 1400.0))
    except Exception: pass

    async def get_symbol_sync_data(symbol: str):
        state = await AssassinLedger.get_state(symbol)
        budget, target_profit_rate, entry_session = state[1], state[2], state[8]
        hold = await api_client.get_symbol_holdings_detail(symbol)
        qty, avg_price = hold.get("qty", 0.0), hold.get("avg_price", 0.0)
        profit_usd, profit_rate = hold.get("profit_usd", 0.0), hold.get("profit_rate", 0.0) * 100.0
        profit_krw = profit_usd * exchange_rate
        curr = await api_client.get_current_price(symbol)
        prev_close = curr

        if now_est.hour >= 4: session_start_est = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
        else: session_start_est = (now_est - timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)

        try:
            data_1d = await api_client._request("GET", f"/api/v1/candles?symbol={symbol}&interval=1d&count=5", "MARKET_DATA_CHART", headers=api_client._get_headers())
            candles_1d = (data_1d or {}).get("result", {}).get("candles", [])
            target_date = session_start_est.date()
            for c in candles_1d:
                c_dt = pd.to_datetime(c["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ).date()
                if c_dt < target_date:
                    prev_close = float(c.get("closePrice") or 0.0)
                    break
        except Exception: pass

        high, low = curr, curr
        try:
            c_data = await api_client.get_1m_candles_pagination(symbol, count=200)
            c_list = c_data.get("candles", [])
            if c_list:
                df = pd.DataFrame(c_list)
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True).tz_convert(EST_TZ)
                df.set_index("timestamp", inplace=True)
                df = df[df.index >= session_start_est]
                if not df.empty:
                    h_max = float(pd.to_numeric(df["highPrice"], errors="coerce").max())
                    l_min = float(pd.to_numeric(df["lowPrice"], errors="coerce").min())
                    if h_max > 0: high = h_max
                    if l_min > 0: low = l_min
        except Exception: pass

        high_rate = ((high - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
        low_rate = ((low - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

        return {
            "symbol": symbol, "budget": budget, "target_profit_rate": target_profit_rate,
            "entry_session": entry_session, "curr": curr, "avg_price": avg_price, "qty": qty,
            "high": high, "high_rate": high_rate, "low": low, "low_rate": low_rate,
            "profit_rate": profit_rate, "profit_usd": profit_usd, "profit_krw": profit_krw,
        }

    soxs_data = await get_symbol_sync_data("SOXS")

    def format_symbol(d: dict) -> str:
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        if d["qty"] > 0: flag_str = f"⚡ [{d['entry_session']}: +{d['target_profit_rate']}%]"
        else: flag_str = f"⏳ 대기 (목표 +{d['target_profit_rate']}%)"
        return (
            f"🐻 <b>[SOXS] 단일 스나이퍼 지시서</b>\n"
            f"💵 시드: ${d['budget']:,.0f} | 🎯 {flag_str}\n"
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

async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    state_s = await AssassinLedger.get_state("SOXS")
    budget_s, rate_s, act_s = state_s[1], state_s[2], state_s[5]
    act_s_str = "🟢 ON" if act_s else "🔴 OFF"

    text = (
        "⚙️ <b>[단방향 전술 코어 제어반 (SOXS 락온)]</b>\n"
        "▫️ <b>전술 모드:</b> <code>⚡ 02:58 데이장 마감 스나이핑 & +0.64% 익절 노출</code>\n"
        "▫️ <b>타격 원칙:</b> <code>일일 1회 단독 출동 (진폭 2.0% 미만 시)</code>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🐻 <b>SOXS (SHORT · 02:58 EST 무조건 선제 타격)</b>\n"
        f"▫️ 상태: <code>{act_s_str}</code> | 예산: <code>${budget_s:,.2f}</code> | 익절: <code>+{rate_s}% (고정)</code>"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔴 SOXS 정지" if act_s else "🟢 SOXS 가동", callback_data="toggle_set_act_SOXS")],
            [InlineKeyboardButton(text="💵 SOXS 시드 설정", callback_data="set_budget_SOXS")],
            [InlineKeyboardButton(text="🔫 스나이퍼 레이더", callback_data="open_avwap"), InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
        ]
    )
    return text, keyboard
