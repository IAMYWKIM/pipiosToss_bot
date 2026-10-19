# =====================================================================
# FILE: whipsaw_plugin.py
# 목적: 데이장(19:00~03:57 EST) 100% [모의] 전용(실거래 매수 절대 차단!) —
#       유일하게 데이장 개시 시각(한국시간 09:00:00~09:00:59 KST / 19:00:00~19:00:59 EST) 1분간만
#       선물 진폭 NQ>=0.80% 역산 하락가 버퍼 역트레일링[🛡️ BUFFER_WAIT]을 적용하고,
#       그 외 데이장 전 구간은 프리장 본진(옵션 A)과 동일하게 즉시 1차 슬리피지(-0.18%) 매수덫 + 30초 타임쉴드 +
#       하이킨 아시 트레일링(🚀 NQ_VEL_BYPASS) + QQQ 실시간 최소 0.80% 3차 역행 방어를 [모의] 추격하며,
#       03:58~03:59 EST 마감 휩소 묵음 바이패스 및
#       03:58~04:07 EST (9분간) NQ>=2.0% 과열차단일에도 별개 독립 가동되는 데이장 VWAP 상단 종목 0.5% 역산 매수 ➔ +1.00% 익절 매도 듀얼 1주 휩소 매수 덫 플러그인
# =====================================================================
# MODIFIED: 제3헌법 의거 모듈 분리 및 구버전 +0.64% 참고 타점 데드코드 영구 소각
# MODIFIED: 데이장(19:00~03:57 EST)은 100% [모의] 전용이며 어떠한 경우에도 실거래 매수 주문(create_order)을 발사하지 않도록 물리적 차단 (Case 53, 78, 93)
# MODIFIED: 데이장 [모의] 스나이퍼를 프리장 본진(옵션 A)과 100% 동일한 양방향 대칭 클라이맥스(매수종목 RSI<=30.0 & 반대종목 RSI>=79.9)로 단일화 (Case 78, 81, 83)
# MODIFIED: 초과 Case 93 — 나스닥 100 선물 시장이 열리고 나서 발생한 진폭을 데이장 개시 시각에 모두 받지 않도록,
#           선물 진폭(NQ=F >= 0.80%) 3배 역산 하락가 버퍼(BUFFER_WAIT)를 오직 데이장 개시 첫 1분(한국시간 09:00:00 ~ 09:00:59 KST / 19:00:00 ~ 19:00:59 EST)에만 유일하게 적용!
# MODIFIED: 데이장 [모의] 2단계 추격(HOLDING)에 본진과 동일하게 '+1.00% 점화 후 하이킨 아시 양봉 홀딩 & QQQ실시간 ±0.20% 급등락 시 HA음봉 바이패스(🚀 NQ_VEL_BYPASS) & 고점 -0.30% 콜백/HA음봉 모의 익절' 동기화 (Case 78, 89, 91)
# MODIFIED: 초과 Case 92 & Case 94 — 데이장 [모의] 및 03:58 휩소 1주 작전에서 3차 역행 안전망의 진입 임계치 공식 max(nq_entry_amp * 0.5, 0.80%)은
#           나스닥 100 선물지수(NQ=F) 진폭(nq_entry_amp)을 기준으로 산출하되 최소 0.80% QQQ 역행 하한을 강제하고,
#           해당 임계치를 적용하는 진입 기준가 및 실시간 역행 비교가는 토스 Open API QQQ 0.0초 실시간 현재가(get_stage3_reference_price)로 전면 대체
# MODIFIED: 초과 Case 79, 80, 95 & 96 — 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전(WHIPSAW_1SH)은 당일 NQ=F >= 2.0% 과열차단(CANCELED_MACRO)일에도
#           차단되지 않고 본진과 별개로 데이장 VWAP 0.5% 역산 가격(VWAP / 1.005)에 1주 진입하여 매수 체결가 대비 +1.00%(avg_price * 1.01) 익절을 추구하며,
#           04:07:00 EST 정리 직후 buy_order_id="" 원자적 소각 및 과열차단(CANCELED_MACRO)을 복원하여 본진 시드를 보호

import math
import time
import asyncio
import html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    AMP_BUFFER_ACTIVATE_NQ_PCT,
    AMP_BUFFER_LEVERAGE_MULT,
    AMP_BUFFER_REBOUND_PCT,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    compute_amp_buffer_target_price,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 매수 대상 종목 과매도 임계치 (30.0 이하 엄격 고정)
PRE_SNIPER_RSI_THRESHOLD = 30.0
# 반대편 대세 종목이 79.0 이상에 진입하면 1.5초 루프를 0.4초 고주파로 당기는 프리-클라이맥스 부스터 임계치 (Case 28, 83)
PRE_CLIMAX_BOOSTER_RSI = 79.0
# 순간 스파이크 누락 방지를 위해 반대편 대세 종목 RSI 79.9 이상을 80.0 클라이맥스로 즉시 인지 (Case 81, 83)
OPP_CLIMAX_RSI_THRESHOLD = 79.9

# 데이장 (19:00 ~ 03:57 EST) [모의] 하이킨 아시 트레일링 점화 기준 수익률 (+1.00%), 콜백 폭 (-0.30%) 및 급등락 바이패스 임계치 (±0.20%)
DAY_SIM_ARM_PROFIT_RATE = AssassinLedger.TRAIL_ARM_PROFIT_RATE
DAY_SIM_TRAIL_DROP_PCT = AssassinLedger.TRAIL_DROP_CALLBACK_PCT
DAY_SIM_NQ_VEL_SURGE_PCT = AssassinLedger.NQ_VELOCITY_SURGE_PCT
_DAY_SIM_LOCK = asyncio.Lock()
_WHIPSAW_LOCK = asyncio.Lock()

# 1차 슬리피지 지정가 매수 덫 & 조건부 30초 타임쉴드 상수 (프리장 실전 옵션 A 및 데이장 모의 공통)
BUY_TRAP_SLIPPAGE_PCT = 0.18          # 1차 매수 덫 슬리피지 할인율 (-0.18%, 예: $43.00 -> $42.92)
BUY_TRAP_MIN_DISC_USD = 0.02          # 1차 매수 덫 최소 할인 폭 (-$0.02 보장)
BUY_TRAP_REARM_WAIT_SEC = 30.0        # RSI <= 30.0 유지 시 1차 매수 덫 최대 타임쉴드 대기 시간 (30.0초)

# MODIFIED: 초과 Case 79 & Case 95 — 03:58 ~ 04:07 EST (9분간) 실전 듀얼 1주 고정 '휩소 노출 작전' 상수
#           (데이장 VWAP 0.5% 역산 매수 덫 투하 ➔ 매수 체결 후 +1.00% 익절 매도 덫 장전)
WHIPSAW_FIXED_QTY = 1                      # 시드 무관 무조건 1주 고정 테스트
WHIPSAW_BUY_REVERSE_FACTOR = 1.005         # 데이장 VWAP = 1.005 * 매수덫가격 (VWAP 대비 0.5% 역산 하단 매수 덫)
WHIPSAW_SELL_PROFIT_FACTOR = 1.01          # 목표 매도덫가격 = ceil(매수체결가 * 1.01) (+1.00% 익절 매도 덫)
WHIPSAW_PROFIT_FACTOR = WHIPSAW_SELL_PROFIT_FACTOR

# 데이장 100% [모의] (개시 1분 한정 BUFFER_WAIT / 평시 TRAP_WAIT / HOLDING) 및 03:58 휩소 런타임 상태 캐시
_WHIPSAW_SIM_CACHE = {
    "day_logical_date": "",
    "day_sim_active": False,
    "day_sim_symbol": "",             # 모의 포착된 종목 ("SOXS" 또는 "SOXL")
    "day_sim_phase": "",              # "BUFFER_WAIT" (09:00 KST 개시 1분 한정 역산버퍼) | "TRAP_WAIT" (1차 슬리피지 덫 대기) | "HOLDING" (체결 후 HA·트레일링 추격)
    "day_sim_spot_price": 0.0,        # 최초 포착 당시 현재가
    "day_sim_anchor_high": 0.0,       # 충격 전 기준 고가 (예: $49.03) (Case 93)
    "day_sim_target_drop_p": 0.0,     # NQ진폭×3.0배 역산 예상 하락가 버퍼 (예: $46.95) (Case 93)
    "day_sim_partial_drop_p": 0.0,    # 최소 유효 흡수 하한선 (예: $47.57) (Case 93)
    "day_sim_exp_drop_pct": 0.0,      # 역산 기대 하락률 (예: 4.23%) (Case 93)
    "day_sim_rebound_trig_p": 0.0,    # 역산버퍼 도달 후 최저점 대비 +0.30% 반등 목표가 (예: $46.82) (Case 93)
    "day_sim_buf_reached": False,     # 역산 예상 하락가 버퍼 도달 여부 (Case 93)
    "day_sim_trap_price": 0.0,        # 1차 슬리피지(-0.18%) 매수 덫 가격
    "day_sim_entry_price": 0.0,       # 확정된 모의 진입(체결) 평단가
    "day_sim_fill_type": "",          # 체결 유형 설명 문자열
    "day_sim_arm_price": 0.0,         # +1.00% 트레일링 점화 기준가 (보존선 겸용)
    "day_sim_target_price": 0.0,      # UI 표출용 목표가 (점화 전에는 arm_price, 점화 후에는 유효 스탑가)
    "day_sim_trail_armed": False,     # +1.00% 돌파 트레일링 점화 여부
    "day_sim_nq_vel_bypass": False,   # 트레일링 중 QQQ/NQ ±0.20% 급등락 HA음봉 바이패스 작동 여부 (Case 89, 91)
    "day_sim_armed_ts": 0.0,          # 트레일링 점화 시각 타임스탬프
    "day_sim_armed_hm": 0,            # 트레일링 점화 분봉 시각 (HHMM)
    "day_sim_min_price": 0.0,         # 경유 최저가 (MAE / post_shock_low)
    "day_sim_max_price": 0.0,         # 경유 최고가 (MFE / Peak)
    "day_sim_start_ts": 0.0,          # 포착 시각 타임스탬프 (30초 타임쉴드 측정용)
    "day_sim_entry_time_str": "",
    "day_sim_buy_rsi": 0.0,           # 포착 당시 매수종목 RSI (<= 30.0)
    "day_sim_opp_rsi": 0.0,           # 포착 당시 반대종목 RSI (>= 79.9)
    "day_sim_nq_entry_price": 0.0,    # 모의 진입 시점 3차 방어 기준가 (토스 QQQ 0.0초 실시간 현재가, 폴백 NQ=F) (Case 92, 94)
    "day_sim_nq_entry_amp": 0.0,      # 모의 진입 시점 NQ=F 당일 진폭 (max(진폭*0.5, 0.80%) 임계치 산출 기준!) (Case 92, 94)
    "day_sim_ref_src": "QQQ실시간",   # 3차 방어 기준가 소스 라벨 ("QQQ실시간" 또는 "NQ선물")
    # 03:58 ~ 04:07 EST 듀얼 1주 휩소 노출 작전 런타임 스냅샷
    "whipsaw_date": "",               # 당일 휩소 작전 수행 논리적 날짜 (1일 1회 제한)
    "whipsaw_target_sym": "",         # 선정된 VWAP 상단 종목 ("SOXS" 또는 "SOXL")
    "whipsaw_spot_price": 0.0,        # 선정 당시 해당 종목 현재가 (> day_vwap)
    "whipsaw_locked_vwap": 0.0,       # 선정된 종목의 데이장 누적 VWAP
    "whipsaw_buy_price": 0.0,         # 산출된 하단 0.5% 역산 1주 매수 덫 가격 (VWAP / 1.005)
    "whipsaw_sell_target": 0.0,       # 산출된 1주 목표 매도 덫 가격 (매수가 * 1.01, +1.00% 익절가)
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
    _WHIPSAW_SIM_CACHE["day_sim_anchor_high"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_target_drop_p"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_partial_drop_p"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_exp_drop_pct"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_rebound_trig_p"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_buf_reached"] = False
    _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
    _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
    _WHIPSAW_SIM_CACHE["day_sim_armed_ts"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_armed_hm"] = 0
    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = "QQQ실시간"


def get_daymarket_sim_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 데이장 100% [모의] 추격 상태를 조회하기 위한 헬퍼 (초과 Case 78, 93)."""
    return {
        "active": bool(_WHIPSAW_SIM_CACHE.get("day_sim_active", False)),
        "symbol": str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "")),
        "phase": str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", "")),
        "spot_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", 0.0)),
        "anchor_high": float(_WHIPSAW_SIM_CACHE.get("day_sim_anchor_high", 0.0)),
        "target_drop_p": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_drop_p", 0.0)),
        "partial_drop_p": float(_WHIPSAW_SIM_CACHE.get("day_sim_partial_drop_p", 0.0)),
        "exp_drop_pct": float(_WHIPSAW_SIM_CACHE.get("day_sim_exp_drop_pct", 0.0)),
        "rebound_trig_p": float(_WHIPSAW_SIM_CACHE.get("day_sim_rebound_trig_p", 0.0)),
        "buf_reached": bool(_WHIPSAW_SIM_CACHE.get("day_sim_buf_reached", False)),
        "trap_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0)),
        "entry_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0)),
        "fill_type": str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "")),
        "target_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0)),
        "trail_armed": bool(_WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False)),
        "nq_vel_bypass": bool(_WHIPSAW_SIM_CACHE.get("day_sim_nq_vel_bypass", False)),
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
            await ctx.notify_tg(
                f"🏁 <b>[데이장 모의 마감 정산] {sim_sym} 프리장 전환 인계 완료</b>\n"
                f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t} EST</code>) ➡️ 마감 <code>${cur_p_fl:.2f}</code> (목표/스탑 <code>${target_p:.2f}</code>)\n"
                f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code>\n"
                f"▫️ 마감 손익률: 세전 <code>{gross_pct:+.2f}%</code> / 세후 <code>{net_pct:+.2f}%</code> (수수료 0.14% 차감)\n"
                "▫️ 후속 조치: <code>데이장 모의 캐시를 초기화하고 본진 프리장 듀얼 스나이퍼로 제어권을 이관합니다.</code>"
            )


# 데이장 (19:00 ~ 03:57 EST) 100% [모의] 전용 (실거래 매수 절대 차단!)
# — 유일하게 데이장 개시 시각(한국시간 09:00:00 ~ 09:00:59 KST / 19:00:00 ~ 19:00:59 EST) 1분간만
#   선물 진폭 NQ=F >= 0.80% 선행 폭등·폭락 시 0단계 BUFFER_WAIT(역산 하락가 버퍼 대기 후 최저점 +0.30% 양봉 반등 가상 체결) 적용!
# — 그 외 모든 데이장 시간대는 즉시 1단계 TRAP_WAIT(30초 타임쉴드) ➔ 2단계 HOLDING(하이킨 아시 트레일링 & QQQ실시간 바이패스 & QQQ실시간 최소 0.80% 3차 역행 손절) (Case 78, 81, 83, 89, 91, 92, 93, 94)
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
        # [상태 1] 데이장 100% [모의] 포지션 활성 상태 (BUFFER_WAIT, TRAP_WAIT 또는 HOLDING)
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
            sym_icon = "🐻" if active_sim_sym == "SOXS" else "🐂"

            # -----------------------------------------------------------------
            # [단계 1-0] BUFFER_WAIT : 데이장 개시 첫 1분(09:00:00 ~ 09:00:59 KST)에 선물 진폭(NQ=F >= 0.80%)으로
            #            장전된 역산 하락가 버퍼 대기 & 최저점 역트레일링 +0.30% 양봉 반등 가상 체결 (초과 Case 93 · 100% 모의, 실매수 절대 차단!)
            # -----------------------------------------------------------------
            if phase == "BUFFER_WAIT":
                spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", cur_p_fl))
                anchor_h = float(_WHIPSAW_SIM_CACHE.get("day_sim_anchor_high", spot_p))
                target_drop_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_drop_p", 0.0))
                partial_drop_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_partial_drop_p", target_drop_p))

                # 매크로 1차 방어선: 역산 버퍼 대기 중 NQ=F 당일 진폭 2.0% 이상 돌파 시 모의 추적 즉시 파기
                if nq_amp_now >= 2.0:
                    _reset_day_sim_state()
                    await ctx.notify_tg(
                        f"🛑 <b>[데이장 모의 역산버퍼 취소] {sym_icon}{active_sim_sym} NQ=F 2.0% 과열 감지 (매수차단)</b>\n"
                        f"▫️ 사유: 역산 하락가 버퍼(<code>${target_drop_p:.2f}</code>) 역트레일링 중 NQ=F 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 돌파)\n"
                        "▫️ 조치: 데이장 모의 역산버퍼 추적 즉시 파기 및 알림 락 리셋 완료"
                    )
                    return

                # 주가가 반등 체결 없이 포착가 대비 +1.5% 이상 이미 날아가 버렸거나 RSI >= 50.0 중립화 시 모의 버퍼 리셋
                if cur_p_fl > spot_p * 1.015 or rsi_val >= 50.0:
                    _reset_day_sim_state()
                    return

                # 선물 진폭(nq_amp_now)이 추가로 더 확대된 경우 역산 하락가 버퍼를 실시간으로 더 낮춰서 2차 하락 방어!
                if nq_amp_now > float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0)):
                    new_td_p, new_pd_p, new_exp_pct, _ = compute_amp_buffer_target_price(
                        anchor_h, spot_p, nq_amp_now, AMP_BUFFER_LEVERAGE_MULT
                    )
                    if new_td_p > 0.0 and new_td_p < target_drop_p:
                        target_drop_p = new_td_p
                        partial_drop_p = min(partial_drop_p, new_pd_p)
                        _WHIPSAW_SIM_CACHE["day_sim_target_drop_p"] = target_drop_p
                        _WHIPSAW_SIM_CACHE["day_sim_partial_drop_p"] = partial_drop_p
                        _WHIPSAW_SIM_CACHE["day_sim_exp_drop_pct"] = new_exp_pct
                        _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_now

                sym_state_b = (ctx.shared_rsi_state or {}).get(active_sim_sym, {})
                ewm_b = sym_state_b.get("ewm_state", {}) if isinstance(sym_state_b, dict) else {}
                cur_bar_open = float(ewm_b.get("cur_bar_open", 0.0))
                cur_bar_low = float(ewm_b.get("cur_bar_low", 0.0))

                prev_low = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", spot_p))
                cand_low = min(cur_p_fl, math.floor(cur_bar_low * 100.0) / 100.0) if cur_bar_low > 0.0 else cur_p_fl

                # 칼날 회피: 신저가를 경신 중이면 절대 모의 체결하지 않고 최저점(day_sim_min_price)과 반등 목표가(+0.30%)만 갱신!
                made_new_low = False
                if cand_low < prev_low:
                    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cand_low
                    _WHIPSAW_SIM_CACHE["day_sim_rebound_trig_p"] = (
                        math.ceil(cand_low * (1.0 + AMP_BUFFER_REBOUND_PCT / 100.0) * 100.0) / 100.0
                    )
                    if cur_p_fl <= cand_low:
                        made_new_low = True

                post_low = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", prev_low))
                rebound_trig_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_rebound_trig_p", 0.0))
                if rebound_trig_p <= 0.0 and post_low > 0.0:
                    rebound_trig_p = math.ceil(post_low * (1.0 + AMP_BUFFER_REBOUND_PCT / 100.0) * 100.0) / 100.0
                    _WHIPSAW_SIM_CACHE["day_sim_rebound_trig_p"] = rebound_trig_p

                is_full_buf_reached = (target_drop_p > 0.0 and post_low <= target_drop_p)
                is_partial_buf_reached = (partial_drop_p > 0.0 and post_low <= partial_drop_p)
                if is_full_buf_reached or is_partial_buf_reached:
                    _WHIPSAW_SIM_CACHE["day_sim_buf_reached"] = True

                if made_new_low or not _WHIPSAW_SIM_CACHE.get("day_sim_buf_reached", False):
                    return

                is_ha_bull_b, _, _ = compute_realtime_tick_heikin_ashi(cur_p_fl, ewm_b)
                is_bull_rebound = bool(is_ha_bull_b or (cur_bar_open > 0.0 and cur_p_fl > cur_bar_open))

                # 역산 하락가 버퍼 도달 후 최저점(post_low) 대비 +0.30% 이상 양봉 반등 확인 시 [모의] 가상 체결 확정 (HOLDING 전이)!
                if (
                    cur_p_fl >= rebound_trig_p
                    and cur_p_fl <= spot_p
                    and is_bull_rebound
                    and (0.0 < rsi_val <= 38.0)
                ):
                    ref_p_fill, ref_src_fill = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                    if ref_p_fill <= 0.0:
                        ref_p_fill = nq_c_now
                        ref_src_fill = "NQ선물"

                    confirmed_entry = cur_p_fl
                    new_arm_100 = math.ceil(confirmed_entry * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                    saved_disc = spot_p - confirmed_entry
                    fill_type_str = (
                        f"🛡️ 데이장 개시 진폭역산버퍼 최저점 역트레일링 모의체결"
                        f"(포착 ${spot_p:.2f} ➔ 역산버퍼 ${target_drop_p:.2f} ➔ 최저 ${post_low:.2f} 찍고 +0.30% 반등 ${confirmed_entry:.2f} · -${saved_disc:.2f} 절감)"
                    )

                    _WHIPSAW_SIM_CACHE["day_sim_phase"] = "HOLDING"
                    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = confirmed_entry
                    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = fill_type_str
                    _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = new_arm_100
                    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = new_arm_100
                    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = post_low
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = confirmed_entry
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_p_fill
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_now
                    _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_fill

                    ref_unit = "$" if ref_src_fill == "QQQ실시간" else ""
                    dyn_th_f = max(
                        nq_amp_now * 0.5,
                        float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                        MIN_STAGE3_QQQ_ADVERSE_PCT,
                    )
                    await ctx.notify_tg(
                        f"🪤 <b>[데이장 모의 개시 역산버퍼 바닥 포착 성공] {sym_icon}{active_sim_sym} 최저점 반등 가상 체결 확정 (매수차단)</b>\n"
                        f"▫️ 휩소 방어: 최초 포착가 <code>${spot_p:.2f}</code> 진입 보류 ➡️ 역산 하락가 <code>${target_drop_p:.2f}</code> 하회 최저점 <code>${post_low:.2f}</code> 확인!\n"
                        f"▫️ 모의 체결가: <code>${confirmed_entry:.2f}</code> (최저점 대비 <code>+0.30%</code> 양봉 반등 · 고가 대비 <code>-${saved_disc:.2f}</code> 저가 모의체결)\n"
                        f"▫️ 트레일링 점화가(+1.00%): <code>${new_arm_100:.2f}</code> | 3차 방어({ref_src_fill}): <code>{ref_unit}{ref_p_fill:.2f}</code> (NQ진폭 <code>{nq_amp_now:.2f}%</code> 한도 <code>+{dyn_th_f:.2f}%</code>)"
                    )
                return

            # -----------------------------------------------------------------
            # [단계 1-A] TRAP_WAIT : 1차 슬리피지(-0.18%) 매수 덫 & 조건부 30초 타임쉴드 모의 시뮬레이션
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
                    # 초과 Case 92 & Case 94 — 모의 체결 확정 순간의 NQ=F 진폭(nq_amp_now)은 3차 임계치 산출 기준으로 저장하고,
                    # 3차 역행 비교 기준가(day_sim_nq_entry_price)는 토스 Open API QQQ 0.0초 실시간 현재가로 동기화!
                    ref_p_fill, ref_src_fill = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                    if ref_p_fill <= 0.0:
                        ref_p_fill = nq_c_now
                        ref_src_fill = "NQ선물"

                    new_arm_100 = math.ceil(confirmed_entry * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                    _WHIPSAW_SIM_CACHE["day_sim_phase"] = "HOLDING"
                    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = confirmed_entry
                    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = fill_type_str
                    _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = new_arm_100
                    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = new_arm_100
                    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = min(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = max(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_p_fill
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_now
                    _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_fill
                return

            # -----------------------------------------------------------------
            # [단계 1-B] HOLDING : 모의 체결 완료 후 +1.00% 점화 하이킨 아시 트레일링(🚀 NQ_VEL_BYPASS 포함) & 매크로 2·3차 모의 손절 감시
            # -----------------------------------------------------------------
            prev_min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
            if prev_min_p <= 0.0 or cur_p_fl < prev_min_p:
                _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl

            prev_max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
            if prev_max_p <= 0.0 or cur_p_fl > prev_max_p:
                _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl

            arm_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_arm_price", 0.0))
            entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
            spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", entry_p))
            fill_type_s = str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "모의체결"))
            min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", cur_p_fl))
            max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", cur_p_fl))
            entry_t_str = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))
            buy_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_buy_rsi", 0.0))
            opp_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_opp_rsi", 0.0))
            nq_entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_price", 0.0))
            nq_entry_a = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0))
            opp_sym = "SOXL" if active_sim_sym == "SOXS" else "SOXS"

            sym_state_d = (ctx.shared_rsi_state or {}).get(active_sim_sym, {})
            ewm_d = sym_state_d.get("ewm_state", {}) if isinstance(sym_state_d, dict) else {}
            is_ha_bull, live_ha_o, live_ha_c = compute_realtime_tick_heikin_ashi(cur_p_fl, ewm_d)

            # 초과 Case 89 & Case 91 — 데이장 모의 추격 중에도 토스 QQQ 0.0초 실시간 변화율(10m/3m) ±0.20% 유리 급변 시 HA음봉 바이패스 판정
            is_nq_favorable_surge, nq_vel_10m, _ = await MacroDataCache.is_nq_velocity_favorable_surge(
                symbol=active_sim_sym, surge_threshold=DAY_SIM_NQ_VEL_SURGE_PCT
            )
            vel_src_lbl = MacroDataCache.get_cached_nq_velocity_source()
            _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = bool(is_nq_favorable_surge)

            # 1) +1.00% 점화선 도달 확인 및 하이킨 아시 트레일링 추적
            if arm_p > 0.0 and cur_p_fl >= arm_p:
                if not _WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False):
                    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = True
                    _WHIPSAW_SIM_CACHE["day_sim_armed_ts"] = time.time()
                    _WHIPSAW_SIM_CACHE["day_sim_armed_hm"] = est_hm
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = max(max_p, cur_p_fl, arm_p)
                    return

            if _WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False):
                peak_p = max(float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", arm_p)), arm_p)
                trail_stop_p = math.floor(peak_p * (1.0 - DAY_SIM_TRAIL_DROP_PCT / 100.0) * 100.0) / 100.0
                effective_stop_p = max(arm_p, trail_stop_p)
                _WHIPSAW_SIM_CACHE["day_sim_target_price"] = effective_stop_p

                elapsed_arm = time.time() - float(_WHIPSAW_SIM_CACHE.get("day_sim_armed_ts", 0.0))
                armed_hm = int(_WHIPSAW_SIM_CACHE.get("day_sim_armed_hm", est_hm))

                is_cb_exit = (peak_p > arm_p or elapsed_arm >= 2.5) and (cur_p_fl <= effective_stop_p)
                # 초과 Case 89 & Case 91 — QQQ실시간(폴백 NQ)이 유리 방향으로 ±0.20% 이상 급등/급락 중이면 HA음봉 모의 익절을 바이패스!
                is_ha_exit = (
                    not is_ha_bull
                    and not is_nq_favorable_surge
                    and (elapsed_arm >= 4.0 or est_hm != armed_hm or peak_p >= arm_p * 1.002)
                )

                if is_cb_exit or is_ha_exit:
                    sim_exit_p = max(arm_p, cur_p_fl) if is_ha_exit else effective_stop_p
                    _reset_day_sim_state()

                    sim_gross_pct = ((sim_exit_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.00
                    sim_net_pct = sim_gross_pct - 0.14
                    peak_pct = ((peak_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else sim_gross_pct
                    max_dd_pct = ((min_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                    exit_trig_str = (
                        f"고점(${peak_p:.2f} / +{peak_pct:.2f}%) 대비 -0.30% 콜백 스탑(${effective_stop_p:.2f})"
                        if is_cb_exit
                        else f"하이킨 아시 1분봉 음봉 전환(HA시가 ${live_ha_o:.2f} ≥ HA종가 ${live_ha_c:.2f} · 고점 ${peak_p:.2f} · {vel_src_lbl} 10m {nq_vel_10m:+.2f}%)"
                    )

                    await ctx.notify_tg(
                        f"🎯 <b>[데이장 모의 HA·트레일링 익절 달성] {sym_icon}{active_sim_sym} +{sim_gross_pct:.2f}% 수확 ➡️ 알림 락 리셋 완료</b>\n"
                        f"▫️ 체결 방식: <code>{fill_type_s}</code>\n"
                        f"▫️ 트레일링 격발: <code>{exit_trig_str}</code>\n"
                        f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (포착 <code>${spot_p:.2f}</code> · <code>{entry_t_str} EST</code> · 대칭 {active_sim_sym} RSI <code>{buy_rsi_s:.1f}</code> &amp; {opp_sym} RSI <code>{opp_rsi_s:.1f}</code>) ➡️ 트레일링 익절 <code>${sim_exit_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> (<code>{max_dd_pct:+.2f}%</code>) ~ 최고 <code>${peak_p:.2f}</code> (<code>+{peak_pct:.2f}%</code>)\n"
                        f"▫️ 모의 수익률: 세전 <code>+{sim_gross_pct:.2f}%</code> / 세후 순수익 <code>+{sim_net_pct:.2f}%</code>\n"
                        "▫️ 후속 조치: <code>트레일링 익절 완료로 데이장 차기 양방향 대칭 클라이맥스 모의 감시가 재장전되었습니다.</code>"
                    )
                return

            # 2) 트레일링 미점화 상태 + 손실권(0 < cur_p_fl < entry_p)에서
            #    2차 과열 방어선(NQ=F 진폭 >= 2.0%) 또는
            #    3차 역행 방어선(NQ=F 진폭 기반 dyn_thresh = max(진폭*0.5, 0.80%) × 토스 QQQ 0.0초 실시간 최소 0.80% 역행) 이탈 시 모의 손절 정산 (Case 67, 92, 94)
            if entry_p > 0.0 and 0.0 < cur_p_fl < entry_p:
                dyn_thresh = max(
                    nq_entry_a * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                ref_c_now, ref_src_now = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)

                # 스케일 불일치 자가 치유 (NQ=F > 3000.0 vs QQQ < 3000.0)
                if nq_entry_p <= 0.0 and ref_c_now > 0.0:
                    nq_entry_p = ref_c_now
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = nq_entry_p
                elif nq_entry_p > 0.0 and ref_c_now > 0.0 and ((nq_entry_p > 3000.0) != (ref_c_now > 3000.0)):
                    nq_entry_p = ref_c_now
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = nq_entry_p

                if nq_entry_p > 0.0 and ref_c_now > 0.0:
                    if active_sim_sym == "SOXS":
                        nq_adverse_pct = (ref_c_now - nq_entry_p) / nq_entry_p * 100.0
                        adverse_dir_label = "상승(숏 역행)"
                    else:
                        nq_adverse_pct = (nq_entry_p - ref_c_now) / nq_entry_p * 100.0
                        adverse_dir_label = "하락(롱 역행)"
                else:
                    nq_adverse_pct = 0.0
                    adverse_dir_label = "역행"

                is_macro_2nd = (nq_amp_now >= 2.0)
                is_macro_3rd = (
                    nq_entry_p > 0.0
                    and ref_c_now > 0.0
                    and nq_adverse_pct >= dyn_thresh
                    and nq_adverse_pct >= MIN_STAGE3_QQQ_ADVERSE_PCT
                )

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
                    ref_unit = "$" if ref_src_now == "QQQ실시간" else ""
                    if is_macro_2nd:
                        macro_reason = f"NQ=F 당일 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 대세 과열 돌파)"
                    else:
                        macro_reason = (
                            f"3차 {ref_src_now} 기준(<code>{ref_unit}{nq_entry_p:.2f}</code> ➡️ <code>{ref_unit}{ref_c_now:.2f}</code>) "
                            f"대비 <code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_label} "
                            f"(NQ=F 진폭 <code>{nq_entry_a:.2f}%</code> 기반 3차 한도 <code>+{dyn_thresh:.2f}%</code> 이탈)"
                        )

                    await ctx.notify_tg(
                        f"🚨 <b>[데이장 모의 매크로 손절] {sym_icon}{active_sim_sym} 방어선 작동 ➡️ 알림 락 리셋 완료</b>\n"
                        f"▫️ 사유: {macro_reason}\n"
                        f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code>) ➡️ 매수1호가 손절 <code>${bid_1_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code>\n"
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

        # 매크로 1차 방어선: 당일 NQ=F 진폭이 이미 2.0% 이상이면 과열 장세이므로 데이장 모의 진입 차단
        if nq_amp_global >= 2.0:
            return

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        my_state = (ctx.shared_rsi_state or {}).get(symbol, {})
        my_ewm = my_state.get("ewm_state", {}) if isinstance(my_state, dict) else {}
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
        if opp_live_rsi < OPP_CLIMAX_RSI_THRESHOLD:
            return

        cur_p_fl = math.floor(current_price * 100.0) / 100.0
        if cur_p_fl <= 0.0:
            return

        # 초과 Case 92 & Case 94 — 3차 역행 안전망 진입 기준가는 토스 Open API QQQ 0.0초 실시간 현재가로 락온하고,
        # 3차 동적 임계치 산출용 진폭은 나스닥 100 선물(NQ=F) 당일 진폭(nq_amp_global)으로 락온!
        ref_entry_p, ref_src_init = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
        if ref_entry_p <= 0.0:
            ref_entry_p = nq_c_global
            ref_src_init = "NQ선물"

        if vwap_val <= 0.0 and isinstance(my_state, dict):
            vwap_val = float(my_state.get("vwap", 0.0))
        vwap_str = f"${vwap_val:.2f}" if vwap_val > 0.0 else "집계중"
        sym_icon = "🐻" if symbol == "SOXS" else "🐂"
        opp_icon = "🐂" if opp_sym == "SOXL" else "🐻"
        ref_unit = "$" if ref_src_init == "QQQ실시간" else ""
        dyn_th_init = max(
            nq_amp_global * 0.5,
            float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
            MIN_STAGE3_QQQ_ADVERSE_PCT,
        )

        # 초과 Case 93 — 선물 시장(18:00 EST 개장)이 먼저 열리고 나서 발생한 진폭을
        # 데이장 개시 시각에 고스란히 고점으로 받지 않도록 하기 위해,
        # 역산 하락가 버퍼(BUFFER_WAIT)는 유일하게 데이장이 개시되는 첫 1분
        # (한국시간 09:00:00 ~ 09:00:59 KST / 미국시간 19:00:00 ~ 19:00:59 EST)에만 작동!
        now_kst = now_est.astimezone(KST_TZ)
        kst_hm = now_kst.hour * 100 + now_kst.minute
        is_daymarket_opening_minute = (kst_hm == 900 or est_hm == 1900)

        anchor_high = max(
            float(my_ewm.get("session_anchor_high", 0.0)),
            float(my_ewm.get("prev_close", 0.0)),
            cur_p_fl,
        )
        target_drop_p, partial_drop_p, exp_drop_pct, eff_anchor = compute_amp_buffer_target_price(
            anchor_high, cur_p_fl, nq_amp_global, AMP_BUFFER_LEVERAGE_MULT
        )

        if (
            is_daymarket_opening_minute
            and nq_amp_global >= AMP_BUFFER_ACTIVATE_NQ_PCT
            and target_drop_p > 0.0
            and cur_p_fl > target_drop_p
        ):
            init_rebound_p = math.ceil(cur_p_fl * (1.0 + AMP_BUFFER_REBOUND_PCT / 100.0) * 100.0) / 100.0
            est_buf_arm_100 = math.ceil(target_drop_p * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0

            _WHIPSAW_SIM_CACHE["day_sim_active"] = True
            _WHIPSAW_SIM_CACHE["day_sim_symbol"] = symbol
            _WHIPSAW_SIM_CACHE["day_sim_phase"] = "BUFFER_WAIT"
            _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = cur_p_fl
            _WHIPSAW_SIM_CACHE["day_sim_anchor_high"] = eff_anchor
            _WHIPSAW_SIM_CACHE["day_sim_target_drop_p"] = target_drop_p
            _WHIPSAW_SIM_CACHE["day_sim_partial_drop_p"] = partial_drop_p
            _WHIPSAW_SIM_CACHE["day_sim_exp_drop_pct"] = exp_drop_pct
            _WHIPSAW_SIM_CACHE["day_sim_rebound_trig_p"] = init_rebound_p
            _WHIPSAW_SIM_CACHE["day_sim_buf_reached"] = False
            _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = target_drop_p
            _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = cur_p_fl
            _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = "데이장 개시(09:00 KST) 진폭역산버퍼 역트레일링 대기중"
            _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = est_buf_arm_100
            _WHIPSAW_SIM_CACHE["day_sim_target_price"] = est_buf_arm_100
            _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
            _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
            _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl
            _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl
            _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = time.time()
            _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = now_est.strftime("%H:%M:%S")
            _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = rsi_val
            _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = opp_live_rsi
            _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_entry_p
            _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_global
            _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_init

            await ctx.notify_tg(
                f"🛡️ <b>[데이장 개시(09:00 KST) 모의 · 진폭 역산 버퍼 가동] {sym_icon}{symbol} 고가 진입 보류 ➡️ 최저점 역트레일링 개시 (매수차단)</b>\n"
                f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST ({now_kst.strftime('%H:%M:%S')} KST)</code> (데이장 개시 1분 전용 · 100% [모의])\n"
                f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≤30.0) &amp; {opp_icon}{opp_sym} RSI <code>{opp_live_rsi:.1f}</code> (≥79.9) | VWAP: <code>{vwap_str}</code>\n"
                f"▫️ 선물 선행 충격: NQ=F 당일 진폭 <code>{nq_amp_global:.2f}%</code> (≥0.80%) ➡️ 3배 역산 기대하락폭 <code>-{exp_drop_pct:.2f}%</code>\n"
                f"▫️ 역산 하락가 버퍼: 기준고가 <code>${eff_anchor:.2f}</code> ➡️ 예상 하락가 <code>${target_drop_p:.2f}</code> (유효흡수 <code>${partial_drop_p:.2f}</code> · 현재 포착가 <code>${cur_p_fl:.2f}</code> 진입 보류!)\n"
                f"▫️ 역트레일링 규칙: <code>신저가 경신 중 모의체결 보류(칼날 회피) · 역산 하락가 버퍼 도달 후 최저점 대비 +{AMP_BUFFER_REBOUND_PCT:.2f}% 양봉 반등 시 최저가로 모의 체결 확정</code>"
            )
            return

        # 데이장 일반 시간대 전체(09:01:00 KST ~) 및 평시(옵션 A 원형): 즉시 1차 슬리피지(-0.18%) 매수 덫 + 30초 타임쉴드 가동
        slip_disc = max(BUY_TRAP_MIN_DISC_USD, round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2))
        sim_trap_buy = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)
        sim_arm_100 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        trap_arm_100 = math.ceil(sim_trap_buy * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0

        _WHIPSAW_SIM_CACHE["day_sim_active"] = True
        _WHIPSAW_SIM_CACHE["day_sim_symbol"] = symbol
        _WHIPSAW_SIM_CACHE["day_sim_phase"] = "TRAP_WAIT"
        _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_anchor_high"] = eff_anchor
        _WHIPSAW_SIM_CACHE["day_sim_target_drop_p"] = target_drop_p
        _WHIPSAW_SIM_CACHE["day_sim_partial_drop_p"] = partial_drop_p
        _WHIPSAW_SIM_CACHE["day_sim_exp_drop_pct"] = exp_drop_pct
        _WHIPSAW_SIM_CACHE["day_sim_rebound_trig_p"] = 0.0
        _WHIPSAW_SIM_CACHE["day_sim_buf_reached"] = True
        _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = sim_trap_buy
        _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = "1차 슬리피지 매수덫 대기중"
        _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = sim_arm_100
        _WHIPSAW_SIM_CACHE["day_sim_target_price"] = sim_arm_100
        _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
        _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
        _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = time.time()
        _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = now_est.strftime("%H:%M:%S")
        _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = rsi_val
        _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = opp_live_rsi
        _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_entry_p
        _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_global
        _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_init

        await ctx.notify_tg(
            f"🔔 <b>[데이장 모의 관측] {sym_icon}{symbol} 양방향 대칭 클라이맥스 포착 ➡️ 2단계 타임쉴드 &amp; 🔥HA·트레일링 추격 개시 (매수차단)</b>\n"
            f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> (0세션 데이마켓 · 프리장 본진 옵션 A 동일 조건)\n"
            f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (기준 ≤ 30.0) &amp; {opp_icon}{opp_sym} RSI <code>{opp_live_rsi:.1f}</code> (기준 ≥ 79.9[80인지])\n"
            f"▫️ 포착 현재가: <code>${cur_p_fl:.2f}</code> | 🪤 1차 슬리피지 덫: <code>${sim_trap_buy:.2f}</code> (-0.18%) | VWAP: <code>{vwap_str}</code>\n"
            f"▫️ 30초 타임쉴드: <code>30초 내 ${sim_trap_buy:.2f} 도달 시 저가체결(점화가 ${trap_arm_100:.2f}) / RSI 30초과 반등 또는 30초 만료 시 현재가체결</code>\n"
            f"▫️ 매크로 기준: 3차 방어({ref_src_init}) <code>{ref_unit}{ref_entry_p:.2f}</code> | NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> (3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
            f"▫️ 트레일링 점화가(+1.00%): <code>${sim_arm_100:.2f}</code> (점화 후 HA 1분봉 양봉 홀딩 · QQQ실시간 ±{DAY_SIM_NQ_VEL_SURGE_PCT:.2f}% 급변 시 HA음봉 바이패스 · 고점 -0.30% 콜백/HA음봉 시 모의 익절)\n"
            f"▫️ 다중알림 잠금: <code>모의 트레일링 익절 또는 매크로 손절 도달 전까지 추가 알림이 100% 차단됩니다.</code>"
        )


# MODIFIED: 초과 Case 79, 92, 95 & 96 — 03:58 ~ 04:06:59 EST (9분간) 듀얼(SOXS/SOXL) 데이장 누적 VWAP 비교 및
#           현재가 > 데이장 VWAP 종목 기준 하단 0.5% 역산(VWAP / 1.005) 1주 지정가 매수 덫 ➔ 체결 시 매수가 대비 +1.00%(avg_price * 1.01) 익절 매도 덫 전용 플러그인
#           (당일 NQ=F >= 2.0% 과열차단[CANCELED_MACRO] 발동일에도 차단하지 않고 본진과 별개로 1주 진입하여 +1.00% 익절 추구!)
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

            # MODIFIED: 초과 Case 79 & Case 95 —
            # 1) 1주 매수 덫: 데이장 VWAP에서 0.5% 역산 (floor((VWAP / 1.005) * 100) / 100)
            # 2) 1주 목표 매도 덫: 매수 체결가 대비 +1.00% 익절가 (ceil(매수가 * 1.01 * 100) / 100)
            calc_buy_p = max(0.01, math.floor((chosen_vwap / WHIPSAW_BUY_REVERSE_FACTOR) * 100.0) / 100.0)
            calc_sell_tp = math.ceil(calc_buy_p * WHIPSAW_SELL_PROFIT_FACTOR * 100.0) / 100.0
            mode_desc = (
                f"{chosen_sym} 현재가(${chosen_spot:.2f})가 데이장VWAP(${chosen_vwap:.2f}, +{best_prem:.2f}%) 상단 위치 "
                f"➡️ VWAP 0.5% 역산 하단 매수덫(${calc_buy_p:.2f}) · 체결 시 +1.00% 익절 매도덫(${calc_sell_tp:.2f})"
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

        # 2단계: 초과 Case 95 — 마스터 전원(is_active) 및 양쪽 종목 무포지션 교차 검증 후
        #        당일 NQ=F >= 2.0% 과열차단(CANCELED_MACRO) 여부와 무관하게 별개로 1주 매수 덫 발사!
        async with ctx.dual_strike_mutex:
            if ctx.in_memory_ordering_lock.get(target_sym, False):
                return
            ctx.in_memory_ordering_lock[target_sym] = True
            try:
                state_chk = await AssassinLedger.get_state(target_sym)
                if not bool(state_chk[5]):
                    return

                nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
                nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)

                # 초과 Case 92 — 휩소 1주 작전에서도 3차 방어 임계치 산출용 진폭은 NQ=F(nq_amp_now)를 기록하고,
                # 3차 방어 기준가는 토스 QQQ 0.0초 실시간 현재가(ref_p_now)로 락온
                ref_p_now, _ = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                if ref_p_now <= 0.0:
                    ref_p_now = nq_c_now

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
                        nq_entry_price=ref_p_now,
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
                    macro_indep_note = (
                        f" (NQ진폭 {nq_amp_now:.2f}% 과열차단 별개 1주 독립 가동 · 04:07 정리 후 본진 차단 복원)"
                        if nq_amp_now >= 2.0
                        else ""
                    )
                    await ctx.notify_tg(
                        f"🌪️ <b>[듀얼 휩소 노출 작전 개시] {sym_icon}{target_sym} VWAP 0.5% 역산 1주 매수 덫 투하</b>\n"
                        f"▫️ 작전 시간: <code>03:58 ~ 04:07 EST (9분간 유지{macro_indep_note})</code>\n"
                        f"▫️ 선정 종목: <b>{sym_icon}{target_sym}</b> | 데이장 누적 VWAP: <code>${locked_vwap:.2f}</code>\n"
                        f"▫️ 타점 판정: <code>{mode_desc_str}</code>\n"
                        f"▫️ 주문 내역: <b>1주 고정(LIMIT)</b> 하단 매수덫 <code>${target_buy_p:.2f}</code> ➡️ 체결 시 +1.00% 목표 매도덫 <code>${target_sell_p:.2f}</code> (기대 <code>+{exp_gross_pct:.2f}%</code>)\n"
                        "▫️ 무비용 방어: <code>04:07 EST까지 덫 미체결 시 비용 $0 자동 취소 / 체결 후 미매도 시 현재가 즉시 매도</code>"
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


# MODIFIED: Case 80, 초과 Case 95 & Case 96 — 04:07:00 EST 정각 듀얼 휩소 노출 작전 9분 타임쉴드 만료 정리 플러그인
#           (미체결 취소 시 already-canceled/404 여부와 무관하게 buy_order_id="" 원자적 즉시 소각 및
#            당일 NQ >= 2.0% 과열일이면 즉시 CANCELED_MACRO 잠금을 복원하여 본진 자동 진입을 차단!)
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

    # [분기 1] 04:07 EST 도달 시점에 1주 매수 덫이 미체결(holdings_qty == 0)인 경우 -> 비용 $0 즉시 취소 후 본진 인계/과열차단 복원
    if holdings_qty == 0:
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

            _, nq_h_cut, nq_l_cut = await MacroDataCache.get_cached_nq_data()
            nq_amp_cut = (((nq_h_cut - nq_l_cut) / nq_l_cut * 100.0) if nq_l_cut > 0.0 else 0.0)
            is_macro_overheated = (nq_amp_cut >= 2.0)

            await AssassinLedger.save_state(
                symbol,
                price=0.0,
                target_sell_price=0.0,
                is_session_done=True if is_macro_overheated else False,
                buy_order_id="",
                sell_order_id="",
                entry_session="CANCELED_MACRO" if is_macro_overheated else "",
                entry_time=0.0,
                nq_entry_price=0.0,
                nq_entry_amp=0.0,
                entry_trend="",
                whipsaw_target_price=0.0,
            )
            await AssassinLedger.unlock_all_symbols_for_reentry()
            should_notify = not _WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False)
            _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
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
                        "<b>04:07 ~ 08:14 EST 본진 양방향 대칭 듀얼 스나이퍼 정식 가동 개시</b>"
                    )
                await ctx.notify_tg(
                    f"⏱️ <b>[04:07 타임쉴드 해제 {sym_icon}{symbol}] 휩소 1주 매수 덫 미체결 취소 완료 (비용 $0)</b>\n"
                    "▫️ 사유: 03:58 ~ 04:07 EST (9분간) 듀얼 휩소 노출 작전 시간 만료\n"
                    f"▫️ 조치: {next_action_desc}"
                )
            return "", ("CANCELED_MACRO" if is_macro_overheated else ""), True
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
                    "▫️ 후속 조치: <code>1주 체결 완료 즉시 실수익 복리 정산 후 본진(과열 시 과열차단 복원) 상태로 전환됩니다.</code>"
                )
            return buy_order_id, entry_session, True
        except Exception as e:
            print(f"🚨 [04:07 휩소 1주 현재가 매도 방어 {symbol}] {e}", flush=True)
            return buy_order_id, entry_session, True
        finally:
            _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = time.time()
            ctx.in_memory_ordering_lock[symbol] = False

    return buy_order_id, entry_session, False
