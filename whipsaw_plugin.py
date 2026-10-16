# =====================================================================
# FILE: whipsaw_plugin.py
# 목적: 데이장(19:00~03:57 EST) 프리장 동일 조건(양방향 대칭 클라이맥스) [모의] 듀얼 2단계 추격,
#       03:58~03:59 EST 마감 휩소 묵음 바이패스 및
#       03:58~04:07 EST (9분간) 데이장 VWAP 상단 종목 1% 역산 듀얼 1주 휩소 매수 덫 전용 플러그인
# =====================================================================
# MODIFIED: 제3헌법 의거 sniper_plugin.py 1,000줄 초과 방지를 위한 모듈 분리 및 NQ 10분봉 추세 데드코드 영구 소각 (Case 43)
# MODIFIED: 데이장 [모의] 스나이퍼를 프리장 본진과 100% 동일한 양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=79.9)로 단일화 (Case 78, 81, 83)
# NEW: PRE_CLIMAX_BOOSTER_RSI = 79.0 (1.5초 -> 0.4초 고주파 부스터 가속) 및 OPP_CLIMAX_RSI_THRESHOLD = 79.9 (79.9를 80.0 클라이맥스로 즉시 인지) 탑재 (Case 28, 83)
# NEW: 데이장 [모의] 포착 시 get_current_prices_batch(("SOXS", "SOXL")) 단일 배치 호출로 양쪽 현재가 동시 조회 및 0.0ms 시차 틱 RSI 합성 (Case 83)
# NEW: 데이장 [모의] 2단계 가상 체결(TRAP_WAIT -> HOLDING) 및 방향성 인지(SOXS: NQ 상승역행 / SOXL: NQ 하락역행) 매크로 모의 손절 탑재 (Case 67, 78)
# NEW: 03:58 ~ 03:59 EST 데이장 마감 직전 휩소 알림 100% 묵음 바이패스(Silent Bypass) 및 04:00 EST 프리장 전환 자동 클리어 탑재 (Case 78)
# MODIFIED: 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 — 선취 매수 폐기 및 현재가 > 데이장 VWAP 종목 기준 1% 역산 지정가 매수 덫 전용화 (Case 79, 80)

import math
import time
import asyncio
import html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    compute_realtime_tick_rsi,
)

EST_TZ = ZoneInfo("America/New_York")

# 매수 대상 종목 과매도 임계치 (30.0 이하 엄격 고정)
PRE_SNIPER_RSI_THRESHOLD = 30.0
# NEW: 반대편 대세 종목이 79.0 이상에 진입하면 1.5초 루프를 0.4초 고주파로 당기는 프리-클라이맥스 부스터 임계치 (Case 28, 83)
PRE_CLIMAX_BOOSTER_RSI = 79.0
# MODIFIED: 순간 스파이크 누락 방지를 위해 반대편 대세 종목 RSI 79.9 이상을 80.0 클라이맥스로 즉시 인지 (Case 81, 83)
OPP_CLIMAX_RSI_THRESHOLD = 79.9

# 데이장 (19:00 ~ 03:57 EST) [모의] 알림 리셋 기준 목표 수익률 (+1.00% 익절가 도달 또는 매크로 손절 시 리셋)
DAY_SIM_RESET_PROFIT_RATE = 1.00
_DAY_SIM_LOCK = asyncio.Lock()
_WHIPSAW_LOCK = asyncio.Lock()

# 1차 슬리피지 지정가 매수 덫 & 조건부 30초 타임쉴드 상수 (프리장 실전 및 데이장 모의 공통)
BUY_TRAP_SLIPPAGE_PCT = 0.18          # 1차 매수 덫 슬리피지 할인율 (-0.18%, 예: $43.00 -> $42.92)
BUY_TRAP_MIN_DISC_USD = 0.02          # 1차 매수 덫 최소 할인 폭 (-$0.02 보장)
BUY_TRAP_REARM_WAIT_SEC = 30.0        # RSI <= 30.0 유지 시 1차 매수 덫 최대 타임쉴드 대기 시간 (30.0초)

# 03:58 ~ 04:07 EST (9분간) 실전 듀얼 1주 고정 '휩소 노출 작전' 상수
WHIPSAW_FIXED_QTY = 1                 # 시드 무관 무조건 1주 고정 테스트
WHIPSAW_PROFIT_FACTOR = 1.01          # 데이장 VWAP = 1.01 * 매수덫가격 (1% 역산 하단 매수 덫)

# 데이장 [모의] 듀얼 2단계 추격 및 03:58~04:07 듀얼 1주 휩소 매수 덫 인메모리 상태 캐시
_WHIPSAW_SIM_CACHE = {
    "day_logical_date": "",
    "day_sim_active": False,
    "day_sim_symbol": "",             # 모의 포착된 종목 ("SOXS" 또는 "SOXL")
    "day_sim_phase": "",              # "TRAP_WAIT" (1차 슬리피지 덫 대기) | "HOLDING" (체결 후 +1.00% 추격)
    "day_sim_spot_price": 0.0,        # 최초 포착 당시 현재가
    "day_sim_trap_price": 0.0,        # 1차 슬리피지(-0.18%) 매수 덫 가격
    "day_sim_entry_price": 0.0,       # 확정된 모의 진입(체결) 평단가
    "day_sim_fill_type": "",          # 체결 유형 설명 문자열
    "day_sim_target_price": 0.0,      # +1.00% 리셋 목표가
    "day_sim_target_064": 0.0,        # +0.64% 회전형 참고 목표가
    "day_sim_hit_064": False,         # 추격 중 +0.64% 경유 여부
    "day_sim_min_price": 0.0,         # 경유 최저가 (MAE)
    "day_sim_max_price": 0.0,         # 경유 최고가 (MFE)
    "day_sim_start_ts": 0.0,          # 포착 시각 타임스탬프 (30초 타임쉴드 측정용)
    "day_sim_entry_time_str": "",
    "day_sim_buy_rsi": 0.0,           # 포착 당시 매수종목 RSI (<= 30.0)
    "day_sim_opp_rsi": 0.0,           # 포착 당시 반대종목 RSI (>= 79.9)
    "day_sim_nq_entry_price": 0.0,    # 모의 진입 시점 NQ=F 지수 (방향성 인지 매크로 3차 방어용)
    "day_sim_nq_entry_amp": 0.0,      # 모의 진입 시점 NQ=F 당일 진폭
    # 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 런타임 스냅샷
    "whipsaw_date": "",               # 당일 휩소 작전 수행 논리적 날짜 (1일 1회 제한)
    "whipsaw_target_sym": "",         # 선정된 VWAP 상단 종목 ("SOXS" 또는 "SOXL")
    "whipsaw_spot_price": 0.0,        # 선정 당시 해당 종목 현재가 (> day_vwap)
    "whipsaw_locked_vwap": 0.0,       # 선정된 종목의 데이장 누적 VWAP
    "whipsaw_buy_price": 0.0,         # 산출된 하단 1% 역산 1주 매수 덫 가격 (VWAP / 1.01)
    "whipsaw_sell_target": 0.0,       # 산출된 1주 목표 매도 덫 가격 (VWAP)
    "whipsaw_mode_desc": "",          # 타점 선정 설명
    "whipsaw_order_fired": False,     # 실제 토스 주문 접수 완료 여부
    "whipsaw_cutoff_done": False,     # 04:07 EST 컷오프 정리 완료 여부
    "whipsaw_last_dump_tick": 0.0,    # 04:07 EST 보유 1주 현재가 매도 스윕 틱 간격 제어
}


def _reset_day_sim_state():
    """데이장 [모의] 추격 상태를 원자적으로 0점 초기화하는 내부 헬퍼."""
    _WHIPSAW_SIM_CACHE["day_sim_active"] = False
    _WHIPSAW_SIM_CACHE["day_sim_symbol"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_phase"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_target_064"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_hit_064"] = False
    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = 0.0


def get_daymarket_sim_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 데이장 [모의] 듀얼 2단계 추격 상태를 조회하기 위한 헬퍼."""
    return {
        "active": bool(_WHIPSAW_SIM_CACHE.get("day_sim_active", False)),
        "symbol": str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "")),
        "phase": str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", "")),
        "spot_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", 0.0)),
        "trap_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0)),
        "entry_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0)),
        "fill_type": str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "")),
        "target_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0)),
        "target_064": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_064", 0.0)),
        "hit_064": bool(_WHIPSAW_SIM_CACHE.get("day_sim_hit_064", False)),
        "min_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0)),
        "max_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0)),
        "entry_time": str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "")),
        "buy_rsi": float(_WHIPSAW_SIM_CACHE.get("day_sim_buy_rsi", 0.0)),
        "opp_rsi": float(_WHIPSAW_SIM_CACHE.get("day_sim_opp_rsi", 0.0)),
    }


def get_whipsaw_runtime_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 상태를 조회하기 위한 헬퍼."""
    return {
        "date": str(_WHIPSAW_SIM_CACHE.get("whipsaw_date", "")),
        "target_sym": str(_WHIPSAW_SIM_CACHE.get("whipsaw_target_sym", "")),
        "spot_price": float(_WHIPSAW_SIM_CACHE.get("whipsaw_spot_price", 0.0)),
        "locked_vwap": float(_WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0)),
        "buy_price": float(_WHIPSAW_SIM_CACHE.get("whipsaw_buy_price", 0.0)),
        "sell_target": float(_WHIPSAW_SIM_CACHE.get("whipsaw_sell_target", 0.0)),
        "mode_desc": str(_WHIPSAW_SIM_CACHE.get("whipsaw_mode_desc", "")),
        "order_fired": bool(_WHIPSAW_SIM_CACHE.get("whipsaw_order_fired", False)),
        "cutoff_done": bool(_WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False)),
    }


# Case 78: 03:58 EST 진입 또는 04:00 EST 프리장 전환 시 미결된 데이장 [모의] 포지션 자동 클리어
async def settle_daymarket_sim_before_premarket(
    ctx, symbol: str, now_est: datetime, current_price: float, silent: bool = True
):
    if not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
        return

    async with _DAY_SIM_LOCK:
        if not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            return

        sim_sym = str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "SOXS"))
        phase = str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", ""))
        entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
        target_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0))
        min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
        max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
        hit_064 = bool(_WHIPSAW_SIM_CACHE.get("day_sim_hit_064", False))
        entry_t = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))

        _reset_day_sim_state()

        cur_p_fl = math.floor(current_price * 100.0) / 100.0 if (symbol == sim_sym and current_price > 0.0) else entry_p
        print(
            f"🧹 [데이장 모의 자동마감 {sim_sym}] 03:58/04:00 전환 클리어 완료 (단계:{phase}, 진입:${entry_p:.2f}, 마감:${cur_p_fl:.2f})",
            flush=True,
        )

        if not silent and phase == "HOLDING" and entry_p > 0.0 and cur_p_fl > 0.0:
            gross_pct = (cur_p_fl / entry_p - 1.0) * 100.0
            net_pct = gross_pct - 0.14
            hit_064_str = "달성(✅)" if hit_064 else "미달(❌)"
            await ctx.notify_tg(
                f"🏁 <b>[데이장 모의 마감 정산] {sim_sym} 프리장 전환 인계 완료</b>\n"
                f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t} EST</code>) ➡️ 마감 <code>${cur_p_fl:.2f}</code> (목표 <code>${target_p:.2f}</code>)\n"
                f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code> (+0.64% 경유: {hit_064_str})\n"
                f"▫️ 마감 손익률: 세전 <code>{gross_pct:+.2f}%</code> / 세후 <code>{net_pct:+.2f}%</code> (수수료 0.14% 차감)\n"
                "▫️ 후속 조치: <code>데이장 모의 캐시를 초기화하고 본진 프리장 듀얼 스나이퍼로 제어권을 이관합니다.</code>"
            )


# MODIFIED: 데이장 (19:00 ~ 03:57 EST) 프리장 본진과 100% 동일한 양방향 대칭 클라이맥스(매수종목<=30.0 & 반대종목>=79.9)
#           [모의] 듀얼 2단계 체결 시뮬레이션 & 방향성 인지 매크로 모의 손절 & 03:58~03:59 묵음 바이패스 (Case 78, 81, 83)
async def execute_daymarket_rsi_alert_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_price: float,
    rsi_val: float,
    vwap_val: float = 0.0,
):
    if symbol not in ("SOXS", "SOXL"):
        return

    est_hm = now_est.hour * 100 + now_est.minute
    is_day_market = (est_hm >= 1900 or est_hm <= 359)
    if not is_day_market:
        return

    # Case 78: 03:58 ~ 03:59 EST 데이장 마감 2분 전 구간은 세션 전환 휩소 구간이므로
    #          03:59:59 EST 알림 폭주를 막고 본진 프리장 및 듀얼 1주 휩소 작전을 보호하기 위해 100% 묵음 바이패스!
    if 358 <= est_hm <= 359:
        if _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            await settle_daymarket_sim_before_premarket(
                ctx=ctx, symbol=symbol, now_est=now_est, current_price=current_price, silent=True
            )
        elif 0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD:
            print(
                f"🔇 [데이장 마감 묵음 바이패스 {symbol}] {now_est.strftime('%H:%M:%S')} EST | "
                f"RSI {rsi_val:.1f} 포착되었으나 본진 프리장 보호를 위해 텔레그램 알림을 생략합니다.",
                flush=True,
            )
        return

    async with _DAY_SIM_LOCK:
        # Case 01 & 초과 Case 36: 19:00 EST 기준 논리적 거래일 산출 및 새 데이장 개장 시 모의 추적 상태 초기화
        logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 19 else now_est
        logical_date_str = logical_dt.strftime("%Y-%m-%d")

        if _WHIPSAW_SIM_CACHE.get("day_logical_date") != logical_date_str:
            _WHIPSAW_SIM_CACHE["day_logical_date"] = logical_date_str
            _reset_day_sim_state()

        state_s = await AssassinLedger.get_state("SOXS")
        is_active = bool(state_s[5])
        if not is_active:
            return

        # =====================================================================
        # [상태 1] 데이장 [모의] 포지션 활성 상태 (TRAP_WAIT 또는 HOLDING)
        # =====================================================================
        if _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            active_sim_sym = str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "SOXS"))
            if symbol != active_sim_sym:
                return

            if current_price <= 0.0:
                try:
                    current_price = await ctx.client.get_current_price(active_sim_sym)
                except Exception:
                    current_price = 0.0
            if current_price <= 0.0:
                return

            cur_p_fl = math.floor(current_price * 100.0) / 100.0
            if cur_p_fl <= 0.0:
                return

            phase = str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", "HOLDING"))
            nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
            nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)

            # -----------------------------------------------------------------
            # [단계 1-A] TRAP_WAIT : 1차 슬리피지(-0.18%) 매수 덫 & 조건부 30초 타임쉴드 시뮬레이션
            # -----------------------------------------------------------------
            if phase == "TRAP_WAIT":
                trap_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0))
                spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", cur_p_fl))
                start_ts = float(_WHIPSAW_SIM_CACHE.get("day_sim_start_ts", 0.0))
                elapsed = max(0.0, time.time() - start_ts) if start_ts > 0.0 else 0.0

                # 매크로 1차 방어선: 1차 매수 덫 대기 중 NQ=F 당일 진폭 2.0% 이상 돌파 시 모의 덫 즉시 파기
                if nq_amp_now >= 2.0:
                    _reset_day_sim_state()
                    await ctx.notify_tg(
                        f"🛑 <b>[데이장 모의 1차 매수덫 취소] {active_sim_sym} NQ=F 과열 감지</b>\n"
                        f"▫️ 사유: 1차 매수 덫(<code>${trap_p:.2f}</code>) 대기 중 NQ=F 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 돌파)\n"
                        "▫️ 조치: 모의 매수 덫 즉시 파기 및 알림 락 리셋 완료"
                    )
                    return

                filled_now = False
                confirmed_entry = 0.0
                fill_type_str = ""

                # 분기 ①: 30초 이내에 현재가가 1차 슬리피지 매수 덫(trap_p) 이하로 도달 -> 저가 덫 체결 확정!
                if trap_p > 0.0 and cur_p_fl <= trap_p:
                    filled_now = True
                    confirmed_entry = trap_p
                    fill_type_str = f"🪤 1차 슬리피지 덫 저가체결(포착 ${spot_p:.2f} ➔ 체결 ${trap_p:.2f})"
                # 분기 ②: 30초 이내에 RSI > 30.0 초과 반등 돌파 -> 타임쉴드 해제 및 반등 현재가 즉각 체결!
                elif rsi_val > PRE_SNIPER_RSI_THRESHOLD:
                    filled_now = True
                    confirmed_entry = cur_p_fl
                    fill_type_str = f"⚡ RSI 30초과 반등 즉각체결(RSI {rsi_val:.1f} · 경과 {elapsed:.1f}초)"
                # 분기 ③: RSI <= 30.0 유지 상태로 30.0초 만료 -> 만료 시점 현재가로 재장전 체결!
                elif elapsed >= BUY_TRAP_REARM_WAIT_SEC:
                    filled_now = True
                    confirmed_entry = cur_p_fl
                    fill_type_str = f"🔄 30초 타임쉴드 만료 현재가체결(RSI {rsi_val:.1f} · 30.0초 경과)"

                if filled_now and confirmed_entry > 0.0:
                    new_tp_100 = math.ceil(confirmed_entry * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0
                    new_tp_064 = math.ceil(confirmed_entry * 1.0064 * 100.0) / 100.0
                    _WHIPSAW_SIM_CACHE["day_sim_phase"] = "HOLDING"
                    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = confirmed_entry
                    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = fill_type_str
                    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = new_tp_100
                    _WHIPSAW_SIM_CACHE["day_sim_target_064"] = new_tp_064
                    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = min(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = max(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = nq_c_now
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_now
                return

            # -----------------------------------------------------------------
            # [단계 1-B] HOLDING : 모의 체결 완료 후 +1.00% 익절 추격 & 방향성 인지 매크로 2·3차 모의 손절 감시
            # -----------------------------------------------------------------
            prev_min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
            if prev_min_p <= 0.0 or cur_p_fl < prev_min_p:
                _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl

            prev_max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
            if prev_max_p <= 0.0 or cur_p_fl > prev_max_p:
                _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl

            target_064 = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_064", 0.0))
            if target_064 > 0.0 and cur_p_fl >= target_064:
                _WHIPSAW_SIM_CACHE["day_sim_hit_064"] = True

            target_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0))
            entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
            spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", entry_p))
            fill_type_s = str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "모의체결"))
            min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", cur_p_fl))
            max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", cur_p_fl))
            hit_064 = bool(_WHIPSAW_SIM_CACHE.get("day_sim_hit_064", False))
            entry_t_str = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))
            buy_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_buy_rsi", 0.0))
            opp_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_opp_rsi", 0.0))
            nq_entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_price", 0.0))
            nq_entry_a = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0))
            opp_sym = "SOXL" if active_sim_sym == "SOXS" else "SOXS"
            sym_icon = "🐻" if active_sim_sym == "SOXS" else "🐂"

            # 1) +1.00% 모의 익절 목표가 도달 확증 시 -> 익절 브리핑 타전 및 락 리셋
            if target_p > 0.0 and cur_p_fl >= target_p:
                _reset_day_sim_state()

                sim_gross_pct = ((cur_p_fl / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.00
                sim_net_pct = sim_gross_pct - 0.14
                spot_gross_pct = ((cur_p_fl / spot_p - 1.0) * 100.0) if spot_p > 0.0 else sim_gross_pct
                max_dd_pct = ((min_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0

                await ctx.notify_tg(
                    f"🎯 <b>[데이장 모의 익절 달성] {sym_icon}{active_sim_sym} +1.00% 목표가 도달 ➡️ 알림 락 리셋 완료</b>\n"
                    f"▫️ 체결 방식: <code>{fill_type_s}</code>\n"
                    f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (포착 <code>${spot_p:.2f}</code> · <code>{entry_t_str} EST</code> · 대칭 {active_sim_sym} RSI <code>{buy_rsi_s:.1f}</code> &amp; {opp_sym} RSI <code>{opp_rsi_s:.1f}</code>) ➡️ 도달 <code>${cur_p_fl:.2f}</code> (목표 <code>${target_p:.2f}</code>)\n"
                    f"▫️ 경유 최저가: <code>${min_p:.2f}</code> (체결가 대비 최대역행 <code>{max_dd_pct:+.2f}%</code>)\n"
                    f"▫️ 모의 수익률: 세전 <code>+{sim_gross_pct:.2f}%</code> / 세후 순수익 <code>+{sim_net_pct:.2f}%</code> (포착가 대비 <code>+{spot_gross_pct:.2f}%</code>)\n"
                    "▫️ 후속 조치: <code>+1.00% 익절 도달이 확인되어 데이장 차기 양방향 대칭 클라이맥스 모의 감시가 재장전되었습니다.</code>"
                )
                return

            # 2) 보유 중 손실권(0 < cur_p_fl < entry_p)에서 방향성 인지 NQ=F 매크로 2·3차 방어선 이탈 시 -> 모의 손절 정산 및 락 리셋 (Case 67)
            if entry_p > 0.0 and 0.0 < cur_p_fl < entry_p:
                dyn_thresh = max(nq_entry_a * 0.5, ctx.nq_dynamic_floor) if nq_entry_a > 0.0 else ctx.nq_dynamic_floor
                if nq_entry_p > 0.0 and nq_c_now > 0.0:
                    if active_sim_sym == "SOXS":
                        nq_adverse_pct = (nq_c_now - nq_entry_p) / nq_entry_p * 100.0
                        adverse_dir_label = "상승(숏 역행)"
                    else:
                        nq_adverse_pct = (nq_entry_p - nq_c_now) / nq_entry_p * 100.0
                        adverse_dir_label = "하락(롱 역행)"
                else:
                    nq_adverse_pct = 0.0
                    adverse_dir_label = "역행"

                is_macro_2nd = (nq_amp_now >= 2.0)
                is_macro_3rd = (nq_entry_p > 0.0 and nq_c_now > 0.0 and nq_adverse_pct >= dyn_thresh)

                if is_macro_2nd or is_macro_3rd:
                    _reset_day_sim_state()
                    try:
                        ob = await ctx.client.get_orderbook(active_sim_sym)
                        bids = ob.get("bids", [])
                        bid_1_p = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else cur_p_fl
                    except Exception:
                        bid_1_p = cur_p_fl

                    loss_gross_pct = ((bid_1_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                    loss_net_pct = loss_gross_pct - 0.14
                    hit_064_str = "달성 후 역행(✅)" if hit_064 else "미달(❌)"
                    if is_macro_2nd:
                        macro_reason = f"NQ=F 당일 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 대세 과열 돌파)"
                    else:
                        macro_reason = (
                            f"진입 지수(<code>{nq_entry_p:.2f}</code>) 대비 <code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_label} "
                            f"(3차 동적 한도 <code>+{dyn_thresh:.2f}%</code> 이탈)"
                        )

                    await ctx.notify_tg(
                        f"🚨 <b>[데이장 모의 매크로 손절] {sym_icon}{active_sim_sym} 방어선 작동 ➡️ 알림 락 리셋 완료</b>\n"
                        f"▫️ 사유: {macro_reason}\n"
                        f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code>) ➡️ 매수1호가 손절 <code>${bid_1_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code> (+0.64% 경유: {hit_064_str})\n"
                        f"▫️ 모의 손익률: 세전 <code>{loss_gross_pct:+.2f}%</code> / 세후 <code>{loss_net_pct:+.2f}%</code> (수수료 0.14% 포함)\n"
                        "▫️ 후속 조치: <code>모의 손절 정산 완료 및 차기 양방향 대칭 클라이맥스 감시 락이 리셋되었습니다.</code>"
                    )
            return

        # =====================================================================
        # [상태 2] 미진입 대기 상태(day_sim_active == False)에서 양방향 대칭 클라이맥스 최초 포착 시
        #          — (SOXS RSI <= 30.0 AND SOXL RSI >= 79.9) 또는 (SOXL RSI <= 30.0 AND SOXS RSI >= 79.9)
        # =====================================================================
        if not (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD):
            return

        nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
        nq_amp_global = (
            ((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0
        )

        # 매크로 1차 방어선: 당일 NQ=F 진폭이 이미 2.0% 이상이면 과열 장세이므로 모의 진입 차단
        if nq_amp_global >= 2.0:
            return

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        opp_state = (ctx.shared_rsi_state or {}).get(opp_sym, {})
        opp_fallback_rsi = float(opp_state.get("value", 0.0)) if isinstance(opp_state, dict) else 0.0
        opp_ewm = opp_state.get("ewm_state", {}) if isinstance(opp_state, dict) else {}

        # Case 83: 단일 배치 API(get_current_prices_batch)로 두 종목의 실시간 현재가를 동시 조회하여 시차 0.0ms로 합성
        try:
            batch_prices = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
            current_price = float(batch_prices.get(symbol, 0.0)) or current_price
            opp_price = float(batch_prices.get(opp_sym, 0.0))
        except Exception:
            opp_price = 0.0

        if opp_price > 0.0:
            opp_live_rsi = compute_realtime_tick_rsi(
                live_price=opp_price,
                ewm_state=opp_ewm,
                fallback_rsi=opp_fallback_rsi,
            )
            if isinstance(opp_state, dict):
                opp_state["value"] = opp_live_rsi
        else:
            opp_live_rsi = opp_fallback_rsi

        # Case 78 & Case 81 & Case 83: 프리장 본진과 100% 동일한 양방향 대칭 클라이맥스 게이트!
        # 매수 대상 종목 RSI <= 30.0 이면서 동시에 반대편 대세 종목 RSI >= 79.9 (80.0 클라이맥스 인지) 이어야만 포착!
        if opp_live_rsi < OPP_CLIMAX_RSI_THRESHOLD:
            return

        cur_p_fl = math.floor(current_price * 100.0) / 100.0
        if cur_p_fl <= 0.0:
            return

        slip_disc = max(BUY_TRAP_MIN_DISC_USD, round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2))
        sim_trap_buy = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)
        sim_tp_100 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0
        sim_tp_064 = math.ceil(cur_p_fl * 1.0064 * 100.0) / 100.0
        trap_tp_100 = math.ceil(sim_trap_buy * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0

        _WHIPSAW_SIM_CACHE["day_sim_active"] = True
        _WHIPSAW_SIM_CACHE["day_sim_symbol"] = symbol
        _WHIPSAW_SIM_CACHE["day_sim_phase"] = "TRAP_WAIT"
        _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = sim_trap_buy
        _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = "1차 슬리피지 매수덫 대기중"
        _WHIPSAW_SIM_CACHE["day_sim_target_price"] = sim_tp_100
        _WHIPSAW_SIM_CACHE["day_sim_target_064"] = sim_tp_064
        _WHIPSAW_SIM_CACHE["day_sim_hit_064"] = False
        _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = time.time()
        _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = now_est.strftime("%H:%M:%S")
        _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = rsi_val
        _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = opp_live_rsi
        _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = nq_c_global
        _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_global

        if vwap_val <= 0.0 and ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
            vwap_val = float(ctx.shared_rsi_state[symbol].get("vwap", 0.0))

        vwap_str = f"${vwap_val:.2f}" if vwap_val > 0.0 else "집계중"
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"
        opp_icon = "🐂" if opp_sym == "SOXL" else "🐻"

        await ctx.notify_tg(
            f"🔔 <b>[데이장 모의 관측] {sym_icon}{symbol} 양방향 대칭 클라이맥스 포착 ➡️ 2단계 타임쉴드 &amp; +1.00% 추격 개시 (매수차단)</b>\n"
            f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> (0세션 데이마켓 · 프리장 본진 동일 조건)\n"
            f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (기준 ≤ 30.0) &amp; {opp_icon}{opp_sym} RSI <code>{opp_live_rsi:.1f}</code> (기준 ≥ 79.9[80인지])\n"
            f"▫️ 포착 현재가: <code>${cur_p_fl:.2f}</code> | 🪤 1차 슬리피지 덫: <code>${sim_trap_buy:.2f}</code> (-0.18%) | VWAP: <code>{vwap_str}</code>\n"
            f"▫️ 30초 타임쉴드: <code>30초 내 ${sim_trap_buy:.2f} 도달 시 저가체결(목표 ${trap_tp_100:.2f}) / RSI 30초과 반등 또는 30초 만료 시 현재가체결</code>\n"
            f"▫️ NQ=F 당일 진폭: <code>{nq_amp_global:.2f}%</code> (현재지수 <code>{nq_c_global:.2f}</code>)\n"
            f"▫️ 리셋 목표가(+1.00%): <code>${sim_tp_100:.2f}</code> (참고 +0.64%: <code>${sim_tp_064:.2f}</code>)\n"
            f"▫️ 다중알림 잠금: <code>모의 익절(+1.00%) 또는 매크로 손절 도달 전까지 추가 알림이 100% 차단됩니다.</code>"
        )


# MODIFIED: 03:58 ~ 04:06:59 EST (9분간) 듀얼(SOXS/SOXL) 데이장 누적 VWAP 비교 및
#           현재가 > 데이장 VWAP 종목 기준 하단 1% 역산 1주 지정가 매수 덫 전용 플러그인 (Case 79)
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
    day_vwap: float,
):
    """
    [듀얼 1주 휩소 노출 작전 (03:58 ~ 04:07 EST · 9분간 유지 · 미체결 시 무비용)]
    - 선취 매수 폐기: 현재가로 미리 매수하여 덫 미체결 시 발생하는 비용(수수료/슬리피지)을 0원으로 원천 차단합니다.
    - 03:58 EST에 SOXS와 SOXL 두 종목의 현재가와 데이장 누적 VWAP을 동시 비교하여
      현재가가 자신의 데이장 누적 VWAP보다 높은 종목(current_price > day_vwap, 둘 다 높으면 괴리율 상위 종목)을 선정합니다.
    - 선정된 종목에 대해 [데이장 VWAP = 1.01 * 매수덫가격] 공식으로 하단 1% 역산 가격
      trap_buy_price = floor((day_vwap / 1.01) * 100) / 100 에 1주(quantity=1) LIMIT 매수 덫만 설치합니다.
    - 체결 시 목표 매도 덫은 데이장 VWAP(ceil(day_vwap * 100) / 100, +1.00%)으로 자동 장전됩니다.
    - 03:58 ~ 03:59 EST 장 전환 공백(422 order-hours-closed) 발생 시 스냅샷을 보존하여 04:00:00 EST 첫 틱에 자동 발사합니다.
    """
    if symbol not in ("SOXS", "SOXL") or not is_active or ctx.in_memory_ordering_lock.get(symbol, False):
        return

    est_hm = now_est.hour * 100 + now_est.minute
    if not (358 <= est_hm <= 406):
        return

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

        if _WHIPSAW_SIM_CACHE.get("whipsaw_order_fired", False):
            return

        total_shared_hold = sum(int(v) for v in (ctx.shared_holdings or {}).values())
        if total_shared_hold > 0 or holdings_qty > 0 or bool(buy_order_id):
            _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
            return

        # 1단계: SOXS와 SOXL 중 현재가가 자신의 데이장 누적 VWAP보다 높은 종목(cur_p > day_vwap) 스냅샷 선정
        if not _WHIPSAW_SIM_CACHE.get("whipsaw_target_sym"):
            prices_map = await ctx.client.get_current_prices_batch(("SOXS", "SOXL"))
            if current_price > 0.0:
                prices_map[symbol] = current_price

            candidates = []
            for cand_sym in ("SOXS", "SOXL"):
                c_p = float(prices_map.get(cand_sym, 0.0))
                c_vwap = float(((ctx.shared_rsi_state or {}).get(cand_sym) or {}).get("vwap", 0.0))
                if cand_sym == symbol and day_vwap > 0.0:
                    c_vwap = float(day_vwap)

                c_p_fl = math.floor(c_p * 100.0) / 100.0
                c_vwap_fl = round(c_vwap, 2)
                if c_p_fl > 0.0 and c_vwap_fl > 0.0 and c_p_fl > c_vwap_fl:
                    premium_ratio = (c_p_fl - c_vwap_fl) / c_vwap_fl * 100.0
                    candidates.append((premium_ratio, cand_sym, c_p_fl, c_vwap_fl))

            if not candidates:
                return

            candidates.sort(key=lambda x: x[0], reverse=True)
            best_prem, chosen_sym, chosen_spot, chosen_vwap = candidates[0]

            # 공식: 데이장 VWAP = 1.01 * 매수덫가격 -> 매수덫가격 = floor((VWAP / 1.01) * 100) / 100
            calc_buy_p = max(0.01, math.floor((chosen_vwap / WHIPSAW_PROFIT_FACTOR) * 100.0) / 100.0)
            calc_sell_tp = math.ceil(chosen_vwap * 100.0) / 100.0
            mode_desc = (
                f"{chosen_sym} 현재가(${chosen_spot:.2f})가 데이장VWAP(${chosen_vwap:.2f}, +{best_prem:.2f}%) 상단 위치 "
                f"➡️ 1% 역산 하단 매수덫(${calc_buy_p:.2f}) · 체결 시 목표 VWAP(${calc_sell_tp:.2f})"
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
            return

        target_buy_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_buy_price", 0.0))
        target_sell_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_sell_target", 0.0))
        locked_vwap = float(_WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0))
        mode_desc_str = str(_WHIPSAW_SIM_CACHE.get("whipsaw_mode_desc", ""))

        if target_buy_p <= 0.0 or target_sell_p <= 0.0:
            return

        # 2단계: 매크로 1차 과열 방어선(NQ=F 진폭 < 2.0%) 및 양쪽 종목 무포지션 교차 검증 후 1주 매수 덫 발사
        async with ctx.dual_strike_mutex:
            if ctx.in_memory_ordering_lock.get(target_sym, False):
                return
            ctx.in_memory_ordering_lock[target_sym] = True
            try:
                state_chk = await AssassinLedger.get_state(target_sym)
                if not bool(state_chk[5]) or state_chk[8] == "CANCELED_MACRO":
                    return

                nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
                if nq_amp_now >= 2.0:
                    _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                    return

                # Case 71: SOXS 및 SOXL 양쪽 모두 실잔고 및 미체결 주문 이중 프로빙
                for chk_s in ("SOXS", "SOXL"):
                    if await AssassinLedger.get_buy_order_id(chk_s) != "":
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return
                    h_det = await ctx.client.get_symbol_holdings_detail(chk_s)
                    if int(math.floor(h_det.get("qty", 0.0))) > 0:
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return
                    open_o = await ctx.client.get_orders(status="OPEN", symbol=chk_s)
                    if any(o.get("side") in ("BUY", "SELL") for o in open_o):
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        return

                # Case 65: 1주 매수 가능 USD 예수금 확인
                try:
                    current_bp = await ctx.client.get_usd_buying_power()
                    if current_bp < target_buy_p:
                        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                        await ctx.notify_tg(
                            f"⚠️ <b>[듀얼 휩소 노출 작전 보류] {target_sym} 1주 매수덫 예수금 부족</b>\n"
                            f"▫️ 필요 금액: <code>${target_buy_p:.2f}</code> (1주) | USD 가능금액: <code>${current_bp:,.2f}</code>"
                        )
                        return
                except Exception:
                    pass

                idem_key = f"WHIP_{target_sym}_{today_str.replace('-', '')}_{est_hm}"[:36]
                res = await ctx.client.create_order(
                    symbol=target_sym,
                    side="BUY",
                    order_type="LIMIT",
                    quantity=WHIPSAW_FIXED_QTY,
                    price=f"{target_buy_p:.2f}",
                    client_order_id=idem_key,
                )

                if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                    new_buy_id = str(res["result"]["orderId"])
                    _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                    trend_tag = "SHORT" if target_sym == "SOXS" else "LONG"
                    opp_sym = "SOXL" if target_sym == "SOXS" else "SOXS"

                    await AssassinLedger.save_state(
                        target_sym,
                        buy_order_id=new_buy_id,
                        entry_session="WHIPSAW_1SH",
                        entry_trend=trend_tag,
                        entry_time=time.time(),
                        nq_entry_price=nq_c_now,
                        nq_entry_amp=nq_amp_now,
                        is_session_done=True,
                        last_session_id=current_session_id,
                        whipsaw_target_price=target_sell_p,
                    )
                    await AssassinLedger.save_state(
                        opp_sym,
                        is_session_done=True,
                        last_session_id=current_session_id,
                    )
                    ctx.wakeup_event.set()
                    exp_gross_pct = ((target_sell_p / target_buy_p - 1.0) * 100.0) if target_buy_p > 0.0 else 1.00
                    sym_icon = "🐻" if target_sym == "SOXS" else "🐂"
                    await ctx.notify_tg(
                        f"🌪️ <b>[듀얼 휩소 노출 작전 개시] {sym_icon}{target_sym} VWAP 1% 역산 1주 매수 덫 투하</b>\n"
                        f"▫️ 작전 시간: <code>03:58 ~ 04:07 EST (9분간 유지 · 본진 듀얼 스나이퍼 04:07 타임쉴드)</code>\n"
                        f"▫️ 선정 종목: <b>{sym_icon}{target_sym}</b> | 데이장 누적 VWAP: <code>${locked_vwap:.2f}</code>\n"
                        f"▫️ 타점 판정: <code>{mode_desc_str}</code>\n"
                        f"▫️ 주문 내역: <b>1주 고정(LIMIT)</b> 하단 매수덫 <code>${target_buy_p:.2f}</code> ➡️ 체결 시 목표 매도덫 <code>${target_sell_p:.2f}</code> (기대 <code>+{exp_gross_pct:.2f}%</code>)\n"
                        "▫️ 무비용 방어: <code>04:07 EST까지 덫 미체결 시 비용 $0 자동 취소 / 체결 후 미매도 시 현재가 즉시 매도 후 본진 인계</code>"
                    )
            except Exception as e:
                err_str = str(e)
                # Case 79: 03:58 ~ 03:59 EST 토스 장전환 공백(order-hours-closed) 발생 시 04:00:00 EST 첫 틱에 자동 재발사
                if (358 <= est_hm <= 359) and ("order-hours-closed" in err_str or "422" in err_str):
                    print(
                        f"⏳ [듀얼 휩소 매수덫 대기 {target_sym}] 03:58~03:59 장전환 공백 감지 -> 04:00:00 EST 정각에 락온 타점(${target_buy_p:.2f})으로 자동 발사합니다.",
                        flush=True,
                    )
                else:
                    print(f"🚨 [듀얼 휩소 노출 작전 에러 {target_sym}] {err_str}", flush=True)
            finally:
                ctx.in_memory_ordering_lock[target_sym] = False


# Case 80: 04:07:00 EST 정각 듀얼 휩소 노출 작전 9분 타임쉴드 만료 정리 플러그인
async def enforce_0407_whipsaw_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool,
    holdings_qty: int,
    buy_order_id: str,
    sell_order_id: str,
    entry_session: str,
    current_price: float,
    cancel_helper_fn,
) -> tuple[str, str, bool]:
    est_hm = now_est.hour * 100 + now_est.minute
    if symbol not in ("SOXS", "SOXL") or not is_active or not (407 <= est_hm <= 929):
        return buy_order_id, entry_session, False

    if entry_session != "WHIPSAW_1SH":
        return buy_order_id, entry_session, False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return buy_order_id, entry_session, False

    sym_icon = "🐻" if symbol == "SOXS" else "🐂"

    # [분기 1] 04:07 EST 도달 시점에 1주 매수 덫이 미체결(holdings_qty == 0)인 경우 -> 비용 $0 즉시 취소 후 본진 인계
    if holdings_qty == 0 and buy_order_id:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            canceled_any = False
            canceled_ids = set()
            try:
                await ctx.client.cancel_order(buy_order_id)
                canceled_ids.add(buy_order_id)
                canceled_any = True
            except Exception as ce:
                if "already-filled" in str(ce):
                    return buy_order_id, entry_session, True

            try:
                open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
                for o in open_orders:
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

            await AssassinLedger.save_state(
                symbol,
                price=0.0,
                target_sell_price=0.0,
                is_session_done=False,
                buy_order_id="",
                sell_order_id="",
                entry_session="",
                entry_time=0.0,
                nq_entry_price=0.0,
                nq_entry_amp=0.0,
                entry_trend="",
                whipsaw_target_price=0.0,
            )
            await AssassinLedger.unlock_all_symbols_for_reentry()
            _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
            ctx.wakeup_event.set()

            if canceled_any:
                await ctx.notify_tg(
                    f"⏱️ <b>[04:07 타임쉴드 해제 {sym_icon}{symbol}] 휩소 1주 매수 덫 미체결 취소 완료 (비용 $0)</b>\n"
                    "▫️ 사유: 03:58 ~ 04:07 EST (9분간) 듀얼 휩소 노출 작전 시간 만료\n"
                    "▫️ 조치: 미체결 1주 매수 덫 무비용 파기 ➡️ <b>04:07 EST 본진 양방향 대칭 듀얼 스나이퍼 정식 가동 개시</b>"
                )
            return "", "", True
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # [분기 2] 04:07 EST 도달 시점에 휩소/데이장 1주가 체결되어 보유 중(holdings_qty > 0)인 경우 -> 현재가 즉시 매도!
    if holdings_qty > 0:
        now_ts = time.time()
        if now_ts - float(_WHIPSAW_SIM_CACHE.get("whipsaw_last_dump_tick", 0.0)) < 1.5:
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

            sellable_q_int, surviving_sell_ids = await cancel_helper_fn(
                ctx=ctx, symbol=symbol, sell_order_id=sell_order_id, cancel_all_sides=True
            )

            latest_p = await ctx.client.get_current_price(symbol)
            if latest_p <= 0.0:
                latest_p = current_price
            exec_sell_price = math.floor(latest_p * 100.0) / 100.0

            try:
                ob = await ctx.client.get_orderbook(symbol)
                bids = ob.get("bids", [])
                if bids and float(bids[0]["price"]) > 0.0:
                    bid_1 = float(bids[0]["price"])
                    exec_sell_price = min(exec_sell_price, bid_1) if exec_sell_price > 0.0 else bid_1
            except Exception:
                pass

            if exec_sell_price <= 0.0:
                return buy_order_id, entry_session, True

            sold_dispatched = False
            if sellable_q_int > 0:
                dump_qty = min(holdings_qty, sellable_q_int)
                idem_key = f"WCUT_{symbol}_{now_est.strftime('%H%M%S')}_{int(now_ts * 10) % 100}"[:36]
                res_sell = await asyncio.wait_for(
                    ctx.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        quantity=dump_qty,
                        price=f"{exec_sell_price:.2f}",
                        client_order_id=idem_key,
                    ),
                    timeout=4.0,
                )
                new_s_id = str((res_sell or {}).get("result", {}).get("orderId") or "")
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=new_s_id,
                    target_sell_price=exec_sell_price,
                    whipsaw_target_price=0.0,
                )
                sold_dispatched = True
            elif surviving_sell_ids:
                await asyncio.wait_for(
                    ctx.client.modify_order(
                        order_id=surviving_sell_ids[0],
                        price=f"{exec_sell_price:.2f}",
                        order_type="LIMIT",
                        market_country="US",
                    ),
                    timeout=4.0,
                )
                await AssassinLedger.save_state(
                    symbol,
                    sell_order_id=surviving_sell_ids[0],
                    target_sell_price=exec_sell_price,
                    whipsaw_target_price=0.0,
                )
                sold_dispatched = True

            if sold_dispatched and not _WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False):
                _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
                await ctx.notify_tg(
                    f"⏱️ <b>[04:07 타임쉴드 정리 {sym_icon}{symbol}] 휩소 노출 작전 1주 현재가 즉시 매도 집행</b>\n"
                    f"▫️ 사유: 04:07 EST 휩소 노출 작전 종료 시각 도달 (보유 <code>{holdings_qty}주</code> 정리)\n"
                    f"▫️ 처분 단가(LIMIT): <code>${exec_sell_price:.2f}</code> (현재가/매수1호가 즉시 체결 유도)\n"
                    "▫️ 후속 조치: <code>1주 체결 완료 즉시 실수익 복리 정산 후 본진 양방향 대칭 듀얼 스나이퍼 감시로 전환됩니다.</code>"
                )
            return buy_order_id, entry_session, True
        except Exception as e:
            print(f"🚨 [04:07 휩소 1주 현재가 매도 방어 {symbol}] {e}", flush=True)
            return buy_order_id, entry_session, True
        finally:
            _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = time.time()
            ctx.in_memory_ordering_lock[symbol] = False

    return buy_order_id, entry_session, False
