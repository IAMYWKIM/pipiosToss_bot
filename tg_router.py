# =====================================================================
# FILE: tg_router.py
# 목적: 09:30 EST NQ=F 10분봉 양방향(SOXL/SOXS) 스나이퍼 관제탑 UI, 지시서 및 전술 제어반 라우팅
# =====================================================================
# MODIFIED: Case 02 & Case 09 방어 - SOXL/SOXS 양방향 듀얼 관제 UI 및 양방향 장부 원자적 0점 초기화(/reset) 결속
# MODIFIED: Case 13 & Case 57 방어 - 19:00~03:59 '🌙 시스템 대기', 04:00~09:29 '🔍 프리장 NQ 10분봉 정찰(09:30 타격 대기)',
#           잔고 보유 시 '보유(+수익률%)', 09:31 이후 미보유/미체결 시 '장외대기' 팩트 표출 강제 (미래 참조 반등 위치 영구 소각)
# MODIFIED: Case 37 & Case 68 방어 - 세션 토글/profit_mode 데드코드 소각 및 ☀️ 09:30 오프닝 스나이퍼 +1.0%/+0.5% 토글 결속
# MODIFIED: Case 39, 47, 55, 56 방어 - SOXL/SOXS 양방향 5MA(count=6) 동적 시프트 및 예상 밴드 렌더링

import os
import html
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from quant_engine import AssassinLedger, MacroDataCache

EST_TZ = ZoneInfo("America/New_York")

router = Router()
api_client = None
ADMIN_CHAT_ID = None
wakeup_event = None


def inject_dependencies(client, admin_id, event):
    global api_client, ADMIN_CHAT_ID, wakeup_event
    api_client = client
    ADMIN_CHAT_ID = admin_id
    wakeup_event = event


class BudgetState(StatesGroup):
    waiting_for_budget = State()


def get_main_menu_text() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_status_text = "🌞서머타임 ON (EDT)" if is_dst else "❄️서머타임 OFF (EST)"

    return (
        f"🕒 <b>[ 09:30 양방향 스나이퍼 스케줄 ({dst_status_text}) ]</b>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 19:00~03:59: 🌙 0세션 시스템 대기 (폴링 휴식)\n"
        "🔹 04:00~09:29: 🔍 1세션 프리장 NQ 10분봉 정찰\n"
        "      (신규 매수 금지 · 09:20~09:30 추세 수집)\n"
        "🔹 09:30:00: ⚡ 2세션 정규장 오프닝 스나이핑\n"
        "      (NQ 10분봉 하락=<b>SOXS</b> / 상승=<b>SOXL</b> 1회 타격)\n"
        "🔹 09:31~15:58: 🎯 +1.0% 익절 감시 & NQ 3단 방어\n"
        "      (장중 뒤늦은 추격 매수 100% 영구 차단)\n"
        "🔹 15:59~16:01: 🛑 제로-오버나이트 MOC 덤핑\n"
        "🔹 17:00: 🧹 정산 스캔 및 가비지 컬렉션(GC)\n\n"
        "🛠 <b>[ 핵심 명령어 ]</b>\n"
        "▶️ /avwap : 🔫 양방향 스나이퍼 레이더 관제탑\n"
        "▶️ /sync : 📜 SOXL/SOXS 통합 지시서 동기화\n"
        "▶️ /settlement : ⚙️ 통합 전술 제어반 (시드/익절률)\n\n"
        "⚠️ /reset : 🧹 양방향 장부(SOXL/SOXS) 0점 소각\n"
        "⚠️ /update : 🚀 시스템 자가 업데이트\n\n"
        "🌙 <b>오버나이트를 원할 경우 [통합 전술 제어반]에서 가동을 OFF 해주세요.</b>"
    )


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /start 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        await message.answer(get_main_menu_text(), parse_mode="HTML")
    except Exception as e:
        print(f"🚨 [/start 응답 붕괴 방어] {e}", flush=True)


def parse_session_data(all_candles: list, session_start_est: datetime) -> dict:
    res = {
        "pre_h": 0.0,
        "pre_l": 0.0,
        "pre_amp": 0.0,
        "pre_vwap": 0.0,
        "pre_body": 0.0,
        "reg_h": 0.0,
        "reg_l": 0.0,
        "reg_amp": 0.0,
        "reg_vwap": 0.0,
        "reg_body": 0.0,
    }
    if not all_candles:
        return res

    df = pd.DataFrame(all_candles)
    if df.empty:
        return res

    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="ISO8601", utc=True
    ).dt.tz_convert(EST_TZ)
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

    def calc_metrics(sub_df):
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
        vol = sub_df["volume"].sum()
        vwap = float(pv.sum() / vol) if vol > 0.0 else 0.0
        return h, l, float(amp), vwap, float(body)

    pre_h, pre_l, pre_amp, pre_vwap, pre_body = calc_metrics(pre_df)
    reg_h, reg_l, reg_amp, reg_vwap, reg_body = calc_metrics(reg_df)

    res.update(
        {
            "pre_h": pre_h,
            "pre_l": pre_l,
            "pre_amp": pre_amp,
            "pre_vwap": pre_vwap,
            "pre_body": pre_body,
            "reg_h": reg_h,
            "reg_l": reg_l,
            "reg_amp": reg_amp,
            "reg_vwap": reg_vwap,
            "reg_body": reg_body,
        }
    )
    return res


async def build_avwap_radar() -> tuple[str, InlineKeyboardMarkup]:
    now_est = datetime.now(EST_TZ)
    t = now_est.hour * 100 + now_est.minute

    if 400 <= t <= 929:
        session_name_ui = "preMarket"
        market_header = "( <b>🌅 PRE_MARKET 정찰</b> )"
    elif 930 <= t <= 1559:
        session_name_ui = "regularMarket"
        market_header = "( <b>🔥 REG_MARKET 가동</b> )"
    elif 1600 <= t <= 1659:
        session_name_ui = "afterMarket"
        market_header = "( <b>🛑 AFT_MARKET 마감</b> )"
    else:
        session_name_ui = "dayMarket"
        market_header = "( <b>🌙 시스템 대기</b> )"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(
            api_client.is_market_open(), timeout=3.0
        )
        if not is_open:
            session_name_ui = "dayMarket"
            if sess_name and "HOLIDAY" in sess_name:
                market_header = "( <b>🌙 주말/휴장일</b> )"
            else:
                market_header = "( <b>🌙 시스템 대기</b> )"
    except Exception:
        pass

    # NEW: Case 13 & Case 43 - NQ=F 실시간 지표 및 09:20~09:30 EST 10분봉 스나이핑 시그널 로드 (반등 위치 영구 소각)
    (
        nq_10m_trend,
        nq_10m_chg_pct,
        nq_10m_bar_str,
        nq_c,
        nq_h,
        nq_l,
    ) = await MacroDataCache.get_nq_10m_sniper_signal()

    nq_amp = ((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0
    lev_expected_amp = nq_amp * 4.2

    if nq_10m_trend == "DOWN":
        sniper_sig_str = f"🐻 하락(DOWN {nq_10m_chg_pct:+.2f}%) ➡️ 09:30 SOXS 타격"
    elif nq_10m_trend == "UP":
        sniper_sig_str = f"🐂 상승(UP {nq_10m_chg_pct:+.2f}%) ➡️ 09:30 SOXL 타격"
    else:
        sniper_sig_str = "⏳ 10분봉 데이터 수집 대기"

    price_l, price_s = await asyncio.gather(
        api_client.get_current_price("SOXL"),
        api_client.get_current_price("SOXS"),
    )
    hold_l, hold_s = await asyncio.gather(
        api_client.get_symbol_holdings_detail("SOXL"),
        api_client.get_symbol_holdings_detail("SOXS"),
    )

    async def fetch_5ma_amp(symbol: str):
        avg_amp = 0.0
        yesterday_amp = 0.0
        yesterday_return = 0.0
        try:
            endpoint = f"/api/v1/candles?symbol={symbol}&interval=1d&count=6"
            data = await api_client._request(
                "GET", endpoint, "MARKET_DATA_CHART", headers=api_client._get_headers()
            )
            candles = (data or {}).get("result", {}).get("candles", [])
            if not candles:
                return 0.0, 0.0, 0.0

            c0_dt = (
                pd.to_datetime(candles[0]["timestamp"], format="ISO8601", utc=True)
                .tz_convert(EST_TZ)
                .date()
            )
            now_est_check = datetime.now(EST_TZ)
            today_dt = now_est_check.date()

            # Case 39 & Case 55: 당일 미완성 일봉 배제 동적 시프트
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

    (amp_l, yest_amp_l, yest_return_l), (amp_s, yest_amp_s, yest_return_s) = (
        await asyncio.gather(fetch_5ma_amp("SOXL"), fetch_5ma_amp("SOXS"))
    )

    # Case 56: 어제 진폭 격차 기반 상승/하락/횡보장 판별
    diff_return = abs(yest_return_l - yest_return_s)
    if diff_return <= 0.3:
        trend_msg = "▫️ ⚔️ <b>횡보/휩소장 (방향성 상실)</b> : 전일 등락률 격차 미미"
    elif yest_return_l > yest_return_s:
        trend_msg = "▫️ 🐂 <b>상승장 (SOXL 우위)</b> : 전일 롱(SOXL) 등락률 우위"
    else:
        trend_msg = "▫️ 🐻 <b>하락장 (SOXS 우위)</b> : 전일 숏(SOXS) 등락률 우위"

    async def fetch_session_stats(symbol: str):
        all_candles = []
        before = None
        if now_est.hour >= 4:
            session_start_est = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
        else:
            session_start_est = (now_est - timedelta(days=1)).replace(
                hour=4, minute=0, second=0, microsecond=0
            )

        for _ in range(8):
            try:
                data = await api_client.get_1m_candles_pagination(
                    symbol, count=200, before=before
                )
                candles = data.get("candles", [])
                all_candles.extend(candles)
                if not candles:
                    break
                oldest_time = pd.to_datetime(candles[-1]["timestamp"], utc=True).tz_convert(
                    EST_TZ
                )
                if oldest_time <= session_start_est:
                    break
                before = data.get("nextBefore")
                if not before:
                    break
            except Exception:
                break
        return await asyncio.to_thread(parse_session_data, all_candles, session_start_est)

    empty_sess = {
        "pre_h": 0.0,
        "pre_l": 0.0,
        "pre_amp": 0.0,
        "pre_vwap": 0.0,
        "pre_body": 0.0,
        "reg_h": 0.0,
        "reg_l": 0.0,
        "reg_amp": 0.0,
        "reg_vwap": 0.0,
        "reg_body": 0.0,
    }
    # 0세션(19:00~03:59 EST) 캔들 조회 및 VWAP 연산 100% 영구 차단
    if session_name_ui == "dayMarket":
        sess_l, sess_s = dict(empty_sess), dict(empty_sess)
    else:
        sess_l, sess_s = await asyncio.gather(
            fetch_session_stats("SOXL"), fetch_session_stats("SOXS")
        )

    # Case 47: 5MA 기반 동적 예상 저가/고가 밴드 산출
    pre_exp_l_l = sess_l["pre_h"] * (1.0 - amp_l / 100.0) if sess_l["pre_h"] > 0 else 0.0
    pre_exp_h_l = sess_l["pre_l"] * (1.0 + amp_l / 100.0) if sess_l["pre_l"] > 0 else 0.0
    reg_exp_l_l = sess_l["reg_h"] * (1.0 - amp_l / 100.0) if sess_l["reg_h"] > 0 else 0.0
    reg_exp_h_l = sess_l["reg_l"] * (1.0 + amp_l / 100.0) if sess_l["reg_l"] > 0 else 0.0

    pre_exp_l_s = sess_s["pre_h"] * (1.0 - amp_s / 100.0) if sess_s["pre_h"] > 0 else 0.0
    pre_exp_h_s = sess_s["pre_l"] * (1.0 + amp_s / 100.0) if sess_s["pre_l"] > 0 else 0.0
    reg_exp_l_s = sess_s["reg_h"] * (1.0 - amp_s / 100.0) if sess_s["reg_h"] > 0 else 0.0
    reg_exp_h_s = sess_s["reg_l"] * (1.0 + amp_s / 100.0) if sess_s["reg_l"] > 0 else 0.0

    state_l, state_s = await asyncio.gather(
        AssassinLedger.get_state("SOXL"), AssassinLedger.get_state("SOXS")
    )
    buy_id_l, buy_id_s = await asyncio.gather(
        AssassinLedger.get_buy_order_id("SOXL"), AssassinLedger.get_buy_order_id("SOXS")
    )

    # NEW: Case 13 & Case 57 - 09:30 오프닝 스나이퍼 UI 상태 팩트 표출 함수
    def build_compact_status(
        symbol: str,
        state_tuple: tuple,
        buy_order_id: str,
        current_session: str,
        est_time_int: int,
        qty: float,
    ) -> str:
        budget = state_tuple[1]
        target_profit_rate = state_tuple[2]
        is_done = state_tuple[4]
        is_active = state_tuple[5]
        state_flag = "ON" if is_active else "OFF"
        emoji = "🐂" if symbol == "SOXL" else "🐻"

        if not is_active:
            state_text = "가동OFF(수동대기)"
        elif qty > 0:
            # Case 13: 잔고 보유 시 오직 '보유(+사용자설정수익률%)' 팩트로 표출
            state_text = f"보유(+{target_profit_rate}%)"
        elif current_session == "dayMarket":
            # Case 13: 19:00~03:59 EST '🌙 시스템 대기'
            state_text = "🌙 시스템 대기"
        elif current_session == "preMarket":
            # Case 13: 04:00~09:29 EST '🔍 프리장 NQ 10분봉 정찰(09:30 타격 대기)'
            state_text = "🔍 프리장 NQ 10분봉 정찰(09:30 타격 대기)"
        elif buy_order_id:
            # Case 48: 09:30 발사된 지정가 매수 주문 체결 대기 중
            state_text = "⏳ 09:30 지정가 매수 대기"
        elif current_session == "regularMarket" and est_time_int == 930 and not is_done:
            state_text = "⚡ 09:30 스나이핑 타격 중"
        else:
            # Case 57: 09:31 EST 이후 미보유(0주) 및 미체결 주문 부재 시 '장외대기' 강제 표출
            state_text = "장외대기"

        return f"{emoji} <b>{symbol}</b> <code>[{state_flag}]</code> {state_text} | <code>${budget:.0f}</code>"

    status_l = build_compact_status(
        "SOXL", state_l, buy_id_l, session_name_ui, t, hold_l.get("qty", 0.0)
    )
    status_s = build_compact_status(
        "SOXS", state_s, buy_id_s, session_name_ui, t, hold_s.get("qty", 0.0)
    )

    scan_time = now_est.strftime("%m-%d %H:%M:%S")

    text = f"""📡 <b>[09:30 스나이퍼 레이더]</b> {market_header}
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
🌐 <b>나스닥 100 선물 (NQ=F)</b>
▫️ 고가: <code>{nq_h:.2f}</code> | 저가: <code>{nq_l:.2f}</code>
▫️ 현재: <code>{nq_c:.2f}</code> | 총 진폭: <code>{nq_amp:.2f}%</code>
▫️ 레버리지 기대 진폭 (x4.2): <code>{lev_expected_amp:.2f}%</code>
▫️ 10분봉({nq_10m_bar_str}): <code>{sniper_sig_str}</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
📊 <b>현재가 & 5MA(어제 진폭)</b>
🐂 <b>SOXL</b> <code>${price_l:.2f}</code> | <code>{amp_l:.1f}%({yest_amp_l:.1f}%)</code>
🐻 <b>SOXS</b> <code>${price_s:.2f}</code> | <code>{amp_s:.1f}%({yest_amp_s:.1f}%)</code>
{trend_msg}

🌅 <b>프리장</b> (04:00~09:29 정찰)
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['pre_l']:.2f}~${sess_l['pre_h']:.2f} ({sess_l['pre_amp']:.1f}%)</code> | 예상: <code>${pre_exp_l_l:.2f}~${pre_exp_h_l:.2f}</code>
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['pre_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['pre_l']:.2f}~${sess_s['pre_h']:.2f} ({sess_s['pre_amp']:.1f}%)</code> | 예상: <code>${pre_exp_l_s:.2f}~${pre_exp_h_s:.2f}</code>

🔥 <b>정규장</b> (09:30~16:00 타격)
🐂 <b>SOXL</b> <code>[VWAP] ${sess_l['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_l['reg_l']:.2f}~${sess_l['reg_h']:.2f} ({sess_l['reg_amp']:.1f}%)</code> | 예상: <code>${reg_exp_l_l:.2f}~${reg_exp_h_l:.2f}</code>
🐻 <b>SOXS</b> <code>[VWAP] ${sess_s['reg_vwap']:.2f}</code>
  ⤷ 팩트: <code>${sess_s['reg_l']:.2f}~${sess_s['reg_h']:.2f} ({sess_s['reg_amp']:.1f}%)</code> | 예상: <code>${reg_exp_l_s:.2f}~${reg_exp_h_s:.2f}</code>
➖➖➖➖➖➖➖➖➖➖➖➖➖➖
{status_l}
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


async def build_sync_board() -> str:
    now_est = datetime.now(EST_TZ)
    is_dst = now_est.dst() is not None and now_est.dst().total_seconds() != 0
    dst_str = "🌞 서머타임" if is_dst else "❄️ 서머타임 OFF"

    t = now_est.hour * 100 + now_est.minute
    if 400 <= t <= 929:
        market_state = "🌅 프리장 정찰"
    elif 930 <= t <= 1559:
        market_state = "🔥 정규장 가동"
    elif 1600 <= t <= 1659:
        market_state = "⛔ 장마감"
    else:
        market_state = "🌙 시스템 대기"

    try:
        is_open, _, sess_name, _ = await asyncio.wait_for(
            api_client.is_market_open(), timeout=3.0
        )
        if not is_open:
            if sess_name and "HOLIDAY" in sess_name:
                market_state = "🌙 주말/휴장일"
            else:
                market_state = "🌙 장외 대기"
    except Exception:
        pass

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
        budget = state[1]
        target_profit_rate = state[2]
        entry_session = state[8]
        entry_trend = await AssassinLedger.get_entry_trend(symbol)

        hold = await api_client.get_symbol_holdings_detail(symbol)
        qty = hold.get("qty", 0.0)
        avg_price = hold.get("avg_price", 0.0)
        profit_usd = hold.get("profit_usd", 0.0)
        profit_rate = hold.get("profit_rate", 0.0) * 100.0
        profit_krw = profit_usd * exchange_rate

        curr = await api_client.get_current_price(symbol)
        prev_close = curr

        if now_est.hour >= 4:
            session_start_est = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
        else:
            session_start_est = (now_est - timedelta(days=1)).replace(
                hour=4, minute=0, second=0, microsecond=0
            )

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
                c_dt = (
                    pd.to_datetime(c["timestamp"], format="ISO8601", utc=True)
                    .tz_convert(EST_TZ)
                    .date()
                )
                if c_dt < target_date:
                    prev_close = float(c.get("closePrice") or 0.0)
                    break
        except Exception:
            pass

        if market_state in ["🌙 시스템 대기", "🌙 주말/휴장일", "🌙 장외 대기"]:
            high, low, high_rate, low_rate = 0.0, 0.0, 0.0, 0.0
        else:
            all_candles = []
            before = None
            for _ in range(8):
                try:
                    c_data = await api_client.get_1m_candles_pagination(
                        symbol, count=200, before=before
                    )
                    c_list = c_data.get("candles", [])
                    all_candles.extend(c_list)
                    if not c_list:
                        break
                    oldest_time = pd.to_datetime(c_list[-1]["timestamp"], utc=True).tz_convert(
                        EST_TZ
                    )
                    if oldest_time <= session_start_est:
                        break
                    before = c_data.get("nextBefore")
                    if not before:
                        break
                except Exception:
                    break

            high, low = 0.0, 0.0
            if all_candles:
                df = pd.DataFrame(all_candles)
                if not df.empty:
                    df["timestamp"] = pd.to_datetime(
                        df["timestamp"], format="ISO8601", utc=True
                    ).tz_convert(EST_TZ)
                    df.set_index("timestamp", inplace=True)
                    df = df[df.index >= session_start_est]
                    if not df.empty:
                        df["highPrice"] = pd.to_numeric(
                            df["highPrice"], errors="coerce"
                        ).fillna(0.0)
                        df["lowPrice"] = pd.to_numeric(
                            df["lowPrice"], errors="coerce"
                        ).fillna(0.0)
                        h_max = float(df["highPrice"].max())
                        l_min = float(df["lowPrice"].min())
                        if h_max > 0:
                            high = h_max
                        if l_min > 0:
                            low = l_min

            if high == 0.0:
                high = curr
            if low == 0.0:
                low = curr

            high_rate = ((high - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
            low_rate = ((low - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

        return {
            "symbol": symbol,
            "budget": budget,
            "target_profit_rate": target_profit_rate,
            "entry_session": entry_session,
            "entry_trend": entry_trend,
            "curr": curr,
            "avg_price": avg_price,
            "qty": qty,
            "high": high,
            "high_rate": high_rate,
            "low": low,
            "low_rate": low_rate,
            "profit_rate": profit_rate,
            "profit_usd": profit_usd,
            "profit_krw": profit_krw,
        }

    soxl_data, soxs_data = await asyncio.gather(
        get_symbol_sync_data("SOXL"), get_symbol_sync_data("SOXS")
    )

    def format_symbol(d: dict) -> str:
        profit_sign = "+" if d["profit_usd"] >= 0 else "-"
        icon = "🐂" if d["symbol"] == "SOXL" else "🐻"
        if d["qty"] > 0:
            sess_lbl = d["entry_session"] or "REG_0930"
            tr_lbl = f"·{d['entry_trend']}" if d["entry_trend"] else ""
            flag_str = f"⚡ [{sess_lbl}{tr_lbl}: +{d['target_profit_rate']}%]"
        else:
            flag_str = f"⏳ 대기 (목표 +{d['target_profit_rate']}%)"

        return (
            f"{icon} <b>[{d['symbol']}] 09:30 스나이퍼 지시서</b>\n"
            f"💵 시드: ${d['budget']:,.0f} | 🎯 {flag_str}\n"
            f"💰 현재 ${d['curr']:.2f} / 평단 ${d['avg_price']:.2f} ({int(d['qty'])}주)\n"
            f"📈 금일 고가: ${d['high']:.2f} ({d['high_rate']:+.2f}%)\n"
            f"📉 금일 저가: ${d['low']:.2f} ({d['low_rate']:+.2f}%)\n"
            f"🔺 수익: {d['profit_rate']:+.2f}% ({profit_sign}${abs(d['profit_usd']):,.2f} | {profit_sign}₩{int(abs(d['profit_krw'])):,})"
        )

    text = (
        f"📜 <b>[ 양방향 통합 지시서 ({market_state}) ]</b>\n"
        f"📅 {dst_str} ({now_est.strftime('%H:%M')} EST)\n"
        f"💵 주문가능금액: ${bp:,.2f}\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        f"{format_symbol(soxl_data)}\n\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        f"{format_symbol(soxs_data)}\n\n"
        "▶️ /avwap : 🔫 양방향 스나이퍼 레이더 관제탑"
    )
    return text


async def build_settlement_board() -> tuple[str, InlineKeyboardMarkup]:
    state_l, state_s = await asyncio.gather(
        AssassinLedger.get_state("SOXL"), AssassinLedger.get_state("SOXS")
    )
    budget_l, rate_l, act_l = state_l[1], state_l[2], state_l[5]
    budget_s, rate_s, act_s = state_s[1], state_s[2], state_s[5]

    act_l_str = "🟢 ON" if act_l else "🔴 OFF"
    act_s_str = "🟢 ON" if act_s else "🔴 OFF"

    next_rate_l = 0.5 if rate_l == 1.0 else 1.0
    next_rate_s = 0.5 if rate_s == 1.0 else 1.0

    # Case 37 & Case 68: ☀️ 09:30 정규장 오프닝 스나이퍼 모드 고정 및 +1.0%/+0.5% 토글 제어반
    text = (
        "⚙️ <b>[통합 전술 코어 제어반 (양방향 락온)]</b>\n"
        "▫️ <b>전술 모드:</b> <code>☀️ 09:30 정규장 오프닝 스나이퍼 (REG_OPEN_SNIPER)</code>\n"
        "▫️ <b>타격 원칙:</b> <code>일일 1회 배타적 출동 (하락=SOXS / 상승=SOXL)</code>\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
        "🐂 <b>SOXL (LONG · 10분봉 상승 시 타격)</b>\n"
        f"▫️ 상태: <code>{act_l_str}</code> | 예산: <code>${budget_l:,.2f}</code> | 익절: <code>+{rate_l}%</code>\n\n"
        "🐻 <b>SOXS (SHORT · 10분봉 하락 시 타격)</b>\n"
        f"▫️ 상태: <code>{act_s_str}</code> | 예산: <code>${budget_s:,.2f}</code> | 익절: <code>+{rate_s}%</code>"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔴 SOXL 정지" if act_l else "🟢 SOXL 가동",
                    callback_data="toggle_set_act_SOXL",
                ),
                InlineKeyboardButton(
                    text="🔴 SOXS 정지" if act_s else "🟢 SOXS 가동",
                    callback_data="toggle_set_act_SOXS",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💵 SOXL 시드 설정", callback_data="set_budget_SOXL"
                ),
                InlineKeyboardButton(
                    text="💵 SOXS 시드 설정", callback_data="set_budget_SOXS"
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"🎯 SOXL 익절({rate_l}%➡️{next_rate_l}%)",
                    callback_data="toggle_profit_rate_SOXL",
                ),
                InlineKeyboardButton(
                    text=f"🎯 SOXS 익절({rate_s}%➡️{next_rate_s}%)",
                    callback_data="toggle_profit_rate_SOXS",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔫 스나이퍼 레이더", callback_data="open_avwap"
                ),
                InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main"),
            ],
        ]
    )
    return text, keyboard


@router.message(Command("avwap"))
async def cmd_avwap(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /avwap 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        msg = await message.answer(
            "📡 <b>양방향 스나이퍼 레이더 스캔 중...</b>", parse_mode="HTML"
        )
        text, keyboard = await build_avwap_radar()
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(
            f"🚨 <b>관제탑 렌더링 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "open_avwap")
async def process_open_avwap(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_avwap (User: {user.id})", flush=True)
    await state.clear()
    try:
        text, keyboard = await build_avwap_radar()
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback_query.message.answer(
                f"🚨 <b>관제탑 갱신 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
            )
    finally:
        try:
            await callback_query.answer("레이더 갱신 완료")
        except Exception:
            pass


@router.message(Command("sync"))
async def cmd_sync(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /sync 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        msg = await message.answer(
            "📡 <b>양방향 통합 지시서 스캔 중...</b>", parse_mode="HTML"
        )
        text = await build_sync_board()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(
            f"🚨 <b>통합 지시서 동기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "open_sync")
async def process_open_sync(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_sync (User: {user.id})", flush=True)
    await state.clear()
    try:
        text = await build_sync_board()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback_query.message.answer(
                f"🚨 <b>갱신 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
            )
    finally:
        try:
            await callback_query.answer("동기화 완료")
        except Exception:
            pass


@router.message(Command("settlement"))
async def cmd_settlement(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /settlement 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    text, keyboard = await build_settlement_board()
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data == "open_settlement")
async def process_open_settlement(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_settlement (User: {user.id})", flush=True)
    await state.clear()
    try:
        text, keyboard = await build_settlement_board()
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("toggle_set_act_"))
async def process_toggle_set_act(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[3].upper()
    print(f"💬 [TG 콜백 수신] toggle_set_act_{symbol} (User: {user.id})", flush=True)
    state_data = await AssassinLedger.get_state(symbol)
    await AssassinLedger.save_state(symbol, is_active=not state_data[5])
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("toggle_profit_rate_"))
async def process_toggle_profit_rate(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[3].upper()
    print(f"💬 [TG 콜백 수신] toggle_profit_rate_{symbol} (User: {user.id})", flush=True)
    state_data = await AssassinLedger.get_state(symbol)
    current_rate = state_data[2]
    new_rate = 0.5 if current_rate == 1.0 else 1.0
    await AssassinLedger.save_state(symbol, target_profit_rate=new_rate)
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
        await callback_query.answer(
            f"✅ {symbol} 익절 목표가 +{new_rate}%로 변경되었습니다.", show_alert=True
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("set_budget_"))
async def process_set_budget(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[2].upper()
    print(f"💬 [TG 콜백 수신] set_budget_{symbol} (User: {user.id})", flush=True)
    await state.set_state(BudgetState.waiting_for_budget)
    await state.update_data(symbol=symbol)
    text = (
        f"⌨️ <b>{html.escape(symbol)} 고정 투입 예산 입력 (USD)</b>\n\n"
        "▫️ 09:30 스나이핑 출동 시 투입할 달러 예산을 숫자로 전송하십시오."
    )
    try:
        await callback_query.message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


@router.message(BudgetState.waiting_for_budget)
async def process_budget_input(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return

    user_data = await state.get_data()
    symbol = user_data.get("symbol")
    if not symbol:
        return

    print(
        f"💬 [TG 상태 수신] {symbol} 예산 입력 접수: {message.text.strip()} (User: {user.id})",
        flush=True,
    )
    try:
        budget = float(message.text.strip())
        await AssassinLedger.save_state(symbol, budget=budget)
        if wakeup_event:
            wakeup_event.set()
        await state.clear()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 제어반으로", callback_data="open_settlement")]
            ]
        )
        await message.answer(
            f"✅ <b>{html.escape(symbol)} 예산 ${budget:,.2f} 락온 완료.</b>",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception:
        await message.answer("🚨 유효한 숫자를 입력하세요.", parse_mode="HTML")


@router.message(Command("reset"))
async def cmd_reset(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /reset 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ 양방향(SOXL/SOXS) 장부 초기화",
                    callback_data="execute_dual_reset",
                )
            ],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[SOXL / SOXS 양방향 장부 초기화]</b>\n\n"
        "경고: SOXL 및 SOXS 양방향 로컬 장부(평단가, 목표가, 주문 ID, NQ 기록, 당일 진입 락)를 "
        "100% 영구 소각하고 0점으로 원자적 초기화를 수행합니다.\n"
        "진행하시겠습니까?"
    )
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.in_({"execute_dual_reset", "execute_single_reset"}))
async def process_execute_dual_reset(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] execute_dual_reset (User: {user.id})", flush=True)
    await state.clear()
    try:
        # Case 02: SOXL 및 SOXS 양방향 장부 원자적 0점 소각
        await AssassinLedger.reset_all_ledgers()
        if wakeup_event:
            wakeup_event.set()

        await callback_query.answer("✅ SOXL/SOXS 양방향 장부 0점 소각 완료", show_alert=True)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")]
            ]
        )
        await callback_query.message.edit_text(
            "✅ <b>[SOXL / SOXS 양방향 장부 영구 소각 완료]</b>\n"
            "▫️ 롱(SOXL) 및 숏(SOXS) 로컬 장부가 모두 0점으로 원자적 초기화되었습니다.",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            try:
                await callback_query.message.answer(
                    f"🚨 <b>초기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
                )
            except Exception:
                pass


@router.message(Command("update"))
async def cmd_update(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /update 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 구글 클라우드 서버 탑재", callback_data="execute_update"
                )
            ],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[시스템 자가 업데이트]</b>\n\n"
        "경고: 깃허브(main) 최신 코드를 다운로드하여 프리플라이트 컴파일 검증 후 시스템을 재기동(os._exit)합니다.\n"
        "진행하시겠습니까?"
    )
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data == "execute_update")
async def process_execute_update(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] execute_update (User: {user.id})", flush=True)
    await state.clear()
    try:
        await callback_query.message.edit_text(
            "🔄 <b>GitHub 최신 코드 다운로드 및 프리플라이트 컴파일 검증 중...</b>",
            parse_mode="HTML",
        )

        def _run_git_update():
            import subprocess
            import py_compile

            def run_cmd(cmd):
                proc = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    out, err = proc.communicate(timeout=20)
                    return proc.returncode, out.strip(), err.strip()
                except subprocess.TimeoutExpired:
                    proc.kill()
                    return -1, "", "Subprocess Timeout Expired"

            code, current_hash, err = run_cmd("git rev-parse HEAD")
            if code != 0:
                return False, f"해시 백업 실패: {err}"

            code, fetch_out, fetch_err = run_cmd("git fetch origin main")
            if code != 0:
                return False, f"Fetch 실패:\n{fetch_err}"

            code, local_hash, _ = run_cmd("git rev-parse HEAD")
            code, remote_hash, _ = run_cmd("git rev-parse origin/main")

            if local_hash == remote_hash:
                return True, "Already up to date."

            code, reset_out, reset_err = run_cmd("git reset --hard origin/main")
            if code != 0:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"Hard Reset 붕괴 (원상 복구됨):\n{reset_err}"

            try:
                for mod_file in (
                    "main.py",
                    "sniper_plugin.py",
                    "tg_router.py",
                    "quant_engine.py",
                    "toss_api.py",
                    "candle_recorder.py",
                ):
                    if os.path.exists(mod_file):
                        py_compile.compile(mod_file, doraise=True)
            except Exception as e:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"문법 에러 감지. 롤백 완료:\n{str(e)}"

            return True, f"업데이트 성공:\n{reset_out}"

        success, msg = await asyncio.to_thread(_run_git_update)

        if success:
            if "Already up to date." in msg:
                await callback_query.message.edit_text(
                    "✅ <b>서버 탑재 완료</b>\n▫️ 구글 클라우드 서버가 이미 최신 버전입니다.",
                    parse_mode="HTML",
                )
            else:
                await callback_query.message.edit_text(
                    f"🚀 <b>탑재 성공. 코어 재기동(os._exit) 격발.</b>\n<pre>{html.escape(msg)}</pre>",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1.0)
                os._exit(0)
        else:
            await callback_query.message.edit_text(
                f"🚨 <b>업데이트 실패 (롤백됨)</b>\n<pre>{html.escape(msg)}</pre>",
                parse_mode="HTML",
            )

    except Exception as e:
        await callback_query.message.edit_text(
            f"🚨 <b>서버 탑재 붕괴 방어:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "back_to_main")
async def process_back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] back_to_main (User: {user.id})", flush=True)
    await state.clear()
    try:
        await callback_query.answer()
        await callback_query.message.edit_text(
            get_main_menu_text(), reply_markup=None, parse_mode="HTML"
        )
    except Exception:
        pass
