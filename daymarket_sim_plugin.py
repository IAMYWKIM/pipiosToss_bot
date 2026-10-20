# =====================================================================
# FILE: daymarket_sim_plugin.py
# 목적: 데이장(19:00 ~ 03:57 EST) 100% [모의] 전용 스나이퍼 코어 플러그인
#       (whipsaw_plugin.py 1/2 분할본)
#       — [1막] 개시 1분 전(08:59 KST / 18:59 EST) NQ=F 10분봉 롱/숏 1차 락온 ➔
#               개시 후 7분간(09:00~09:06:59 KST / 19:00~19:06:59 EST) RSI>=50.0 현재가 즉시 [모의] 진입(OPEN7M_TREND)
#       — [2막] 개시 7분 이후 ① 양방향 대칭(<=30.0 & 반대>=79.9, OPTA) 및
#               ② 데이장 첫 RSI>=79.9 상승장 종목(day_leader_sym)의 RSI<=35.0 눌림목(PB35)
#               (1회 달성 후 40.0 <= RSI <= 70.0 중립 밴드 복귀 시 리셋하여 무제한 반복 [모의] 출격)
#       — [익절] +1.30% 상승가 락온 × 최고가 -0.30% 하락 & 🌑HA음봉 시 최소 +1.00% 익절 보장 트레일링 & 승률 집계
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 데이장 100% [모의] 스나이퍼 및 휩소 공유 캐시를 daymarket_sim_plugin.py로 분리
# MODIFIED: 초과 Case 78 & Case 93 — 데이장 실거래 매수 100% 물리적 차단, 개시 7분 추세 진입, 40~70 중립 리셋 및 +1.30% 락온 트레일링

import math
import time
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    MacroDataCache,
    ROUND_TRIP_COMMISSION_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    compute_realtime_tick_heikin_ashi,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 프리장 및 데이장 공통 RSI / 덫 / 휩소 상수 (초과 Case 78, 79, 81, 83, 93, 95)
PRE_SNIPER_RSI_THRESHOLD = 30.0
PRE_TREND_BOOSTER_RSI = 38.0
PRE_CLIMAX_BOOSTER_RSI = 79.0
OPP_CLIMAX_RSI_THRESHOLD = 79.9

DAY_SIM_ARM_PROFIT_RATE = 1.30          # 데이장 [모의] +1.30% 상승가 트레일링 락온
DAY_SIM_FLOOR_LOCK_RATE = 1.00          # 데이장 [모의] 최소 +1.00% 익절 보장 하한선
DAY_SIM_TRAIL_DROP_PCT = 0.30           # 데이장 [모의] 최고가 대비 -0.30% 콜백 스탑폭
DAY_SIM_NQ_VEL_SURGE_PCT = 0.20         # 데이장 [모의] 토스 QQQ 10m/3m ±0.20% 급등락 시 HA음봉 바이패스

BUY_TRAP_SLIPPAGE_PCT = 0.18
BUY_TRAP_MIN_DISC_USD = 0.02
BUY_TRAP_REARM_WAIT_SEC = 30.0

WHIPSAW_FIXED_QTY = 1
WHIPSAW_BUY_REVERSE_FACTOR = 1.005      # 데이장 VWAP 0.5% 역산 매수 덫 (VWAP / 1.005)
WHIPSAW_SELL_PROFIT_FACTOR = 1.010      # 매수 체결 평단가 대비 +1.00% 익절 매도 덫 (avg_price * 1.01)
WHIPSAW_PROFIT_FACTOR = WHIPSAW_SELL_PROFIT_FACTOR

_DAY_SIM_LOCK = asyncio.Lock()
_WHIPSAW_LOCK = asyncio.Lock()

_WHIPSAW_SIM_CACHE = {
    # 데이장 [모의] 스나이퍼 런타임 상태
    "day_sim_session_id": "",
    "day_sim_active": False,
    "day_sim_phase": "",                # "TRAP_WAIT" | "HOLDING"
    "day_sim_symbol": "",
    "day_sim_trig_mode": "",            # "OPEN7M_TREND" | "OPTA" | "PB35"
    "day_sim_spot_price": 0.0,
    "day_sim_trap_price": 0.0,
    "day_sim_entry_price": 0.0,
    "day_sim_arm_price": 0.0,
    "day_sim_floor_price": 0.0,
    "day_sim_target_price": 0.0,
    "day_sim_max_price": 0.0,
    "day_sim_trail_armed": False,
    "day_sim_nq_vel_bypass": False,
    "day_sim_trap_ts": 0.0,
    "day_sim_entry_ts": 0.0,
    "day_sim_entry_time_str": "",
    "day_sim_ref_entry_price": 0.0,
    "day_sim_ref_src": "QQQ실시간",
    "day_sim_nq_entry_amp": 0.0,
    # 데이장 개시 1분 전 NQ=F 10분봉 1차 락온 & 개시 7분 상태
    "day_prelock_sym": "",
    "day_prelock_open10": 0.0,
    "day_prelock_close10": 0.0,
    "day_prelock_body_pct": 0.0,
    "day_prelock_notified": False,
    "day_open7m_done": False,
    # 데이장 첫 RSI >= 79.9 상승장 주도주 및 40~70 중립 리셋 상태
    "day_leader_sym": "",
    "day_leader_rsi": 0.0,
    "day_leader_time_str": "",
    "day_opta_rearm_ready": True,
    "day_pb_rearm_ready": True,
    # 데이장 당일 [모의] 출격 전적 및 승률 스코어보드
    "stat_total": 0,
    "stat_wins": 0,
    "stat_losses": 0,
    "stat_cum_net_pct": 0.0,
    "stat_open7m_cnt": 0,
    "stat_open7m_wins": 0,
    "stat_opta_cnt": 0,
    "stat_opta_wins": 0,
    "stat_pb35_cnt": 0,
    "stat_pb35_wins": 0,
    # 03:58 ~ 04:07 EST 실전 1주 휩소 노출 작전 상태
    "whipsaw_date": "",
    "whipsaw_target_sym": "",
    "whipsaw_spot_price": 0.0,
    "whipsaw_locked_vwap": 0.0,
    "whipsaw_buy_price": 0.0,
    "whipsaw_sell_target": 0.0,
    "whipsaw_mode_desc": "",
    "whipsaw_order_fired": False,
    "whipsaw_cutoff_done": False,
    "whipsaw_last_dump_tick": 0.0,
    "whipsaw_prelock_notified": False,
    "whipsaw_skip_notified": False,
    "whipsaw_last_err": "",
}


def _get_logical_day_session_id(now_est: datetime = None) -> str:
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 18 else now_est
    return logical_dt.strftime("%Y-%m-%d")


def _reset_day_sim_position_only():
    _WHIPSAW_SIM_CACHE.update({
        "day_sim_active": False,
        "day_sim_phase": "",
        "day_sim_symbol": "",
        "day_sim_trig_mode": "",
        "day_sim_spot_price": 0.0,
        "day_sim_trap_price": 0.0,
        "day_sim_entry_price": 0.0,
        "day_sim_arm_price": 0.0,
        "day_sim_floor_price": 0.0,
        "day_sim_target_price": 0.0,
        "day_sim_max_price": 0.0,
        "day_sim_trail_armed": False,
        "day_sim_nq_vel_bypass": False,
        "day_sim_trap_ts": 0.0,
        "day_sim_entry_ts": 0.0,
        "day_sim_entry_time_str": "",
        "day_sim_ref_entry_price": 0.0,
        "day_sim_ref_src": "QQQ실시간",
        "day_sim_nq_entry_amp": 0.0,
    })


def _reset_day_sim_state(new_session_id: str = ""):
    _reset_day_sim_position_only()
    _WHIPSAW_SIM_CACHE.update({
        "day_sim_session_id": new_session_id,
        "day_prelock_sym": "",
        "day_prelock_open10": 0.0,
        "day_prelock_close10": 0.0,
        "day_prelock_body_pct": 0.0,
        "day_prelock_notified": False,
        "day_open7m_done": False,
        "day_leader_sym": "",
        "day_leader_rsi": 0.0,
        "day_leader_time_str": "",
        "day_opta_rearm_ready": True,
        "day_pb_rearm_ready": True,
        "stat_total": 0,
        "stat_wins": 0,
        "stat_losses": 0,
        "stat_cum_net_pct": 0.0,
        "stat_open7m_cnt": 0,
        "stat_open7m_wins": 0,
        "stat_opta_cnt": 0,
        "stat_opta_wins": 0,
        "stat_pb35_cnt": 0,
        "stat_pb35_wins": 0,
    })


def _record_day_sim_result(trig_mode: str, is_win: bool, net_pct: float):
    tot = int(_WHIPSAW_SIM_CACHE.get("stat_total", 0)) + 1
    wins = int(_WHIPSAW_SIM_CACHE.get("stat_wins", 0)) + (1 if is_win else 0)
    losses = int(_WHIPSAW_SIM_CACHE.get("stat_losses", 0)) + (0 if is_win else 1)
    cum_net = round(float(_WHIPSAW_SIM_CACHE.get("stat_cum_net_pct", 0.0)) + float(net_pct), 2)

    _WHIPSAW_SIM_CACHE["stat_total"] = tot
    _WHIPSAW_SIM_CACHE["stat_wins"] = wins
    _WHIPSAW_SIM_CACHE["stat_losses"] = losses
    _WHIPSAW_SIM_CACHE["stat_cum_net_pct"] = cum_net

    if trig_mode == "OPEN7M_TREND":
        _WHIPSAW_SIM_CACHE["stat_open7m_cnt"] = int(_WHIPSAW_SIM_CACHE.get("stat_open7m_cnt", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["stat_open7m_wins"] = int(_WHIPSAW_SIM_CACHE.get("stat_open7m_wins", 0)) + 1
    elif trig_mode == "PB35":
        _WHIPSAW_SIM_CACHE["stat_pb35_cnt"] = int(_WHIPSAW_SIM_CACHE.get("stat_pb35_cnt", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["stat_pb35_wins"] = int(_WHIPSAW_SIM_CACHE.get("stat_pb35_wins", 0)) + 1
    else:
        _WHIPSAW_SIM_CACHE["stat_opta_cnt"] = int(_WHIPSAW_SIM_CACHE.get("stat_opta_cnt", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["stat_opta_wins"] = int(_WHIPSAW_SIM_CACHE.get("stat_opta_wins", 0)) + 1


def _build_day_sim_scoreboard_line() -> str:
    tot = int(_WHIPSAW_SIM_CACHE.get("stat_total", 0))
    wins = int(_WHIPSAW_SIM_CACHE.get("stat_wins", 0))
    losses = int(_WHIPSAW_SIM_CACHE.get("stat_losses", 0))
    wr = (wins / tot * 100.0) if tot > 0 else 0.0
    cum_net = float(_WHIPSAW_SIM_CACHE.get("stat_cum_net_pct", 0.0))

    o7_c = int(_WHIPSAW_SIM_CACHE.get("stat_open7m_cnt", 0))
    o7_w = int(_WHIPSAW_SIM_CACHE.get("stat_open7m_wins", 0))
    op_c = int(_WHIPSAW_SIM_CACHE.get("stat_opta_cnt", 0))
    op_w = int(_WHIPSAW_SIM_CACHE.get("stat_opta_wins", 0))
    pb_c = int(_WHIPSAW_SIM_CACHE.get("stat_pb35_cnt", 0))
    pb_w = int(_WHIPSAW_SIM_CACHE.get("stat_pb35_wins", 0))

    return (
        f"📊 <b>당일 데이장 [모의] 누적 전적:</b> <code>{tot}출격 {wins}승 {losses}패 "
        f"(적중확률 {wr:.1f}% · 세후누적 {cum_net:+.2f}%)</code>\n"
        f"▫️ 부문별: <code>🚀개시7분추세 {o7_w}/{o7_c} | 🪤대칭클라이맥스 {op_w}/{op_c} | 👑상승장35눌림 {pb_w}/{pb_c}</code>"
    )


def get_daymarket_sim_status() -> dict:
    tot = int(_WHIPSAW_SIM_CACHE.get("stat_total", 0))
    wins = int(_WHIPSAW_SIM_CACHE.get("stat_wins", 0))
    losses = int(_WHIPSAW_SIM_CACHE.get("stat_losses", 0))
    wr = (wins / tot * 100.0) if tot > 0 else 0.0

    return {
        "active": bool(_WHIPSAW_SIM_CACHE.get("day_sim_active", False)),
        "phase": str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", "")),
        "symbol": str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "")),
        "trig_mode": str(_WHIPSAW_SIM_CACHE.get("day_sim_trig_mode", "")),
        "spot_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", 0.0)),
        "trap_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0)),
        "entry_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0)),
        "arm_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_arm_price", 0.0)),
        "floor_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_floor_price", 0.0)),
        "target_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0)),
        "max_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0)),
        "trail_armed": bool(_WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False)),
        "nq_vel_bypass": bool(_WHIPSAW_SIM_CACHE.get("day_sim_nq_vel_bypass", False)),
        "prelock_sym": str(_WHIPSAW_SIM_CACHE.get("day_prelock_sym", "")),
        "leader_sym": str(_WHIPSAW_SIM_CACHE.get("day_leader_sym", "")),
        "leader_rsi": float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 0.0)),
        "opta_rearm_ready": bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True)),
        "pb_rearm_ready": bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True)),
        "stat_total": tot,
        "stat_wins": wins,
        "stat_losses": losses,
        "stat_win_rate": round(wr, 1),
        "stat_cum_net_pct": float(_WHIPSAW_SIM_CACHE.get("stat_cum_net_pct", 0.0)),
    }


def get_whipsaw_runtime_status() -> dict:
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


def _get_day_opening_window_state(now_est: datetime = None) -> tuple[bool, bool]:
    """
    초과 Case 93:
    데이장 개시 1분 전(08:59 KST / 18:59 EST) 사전 락온 윈도우(is_prelock_1m) 및
    개시 직후 7분간(09:00:00~09:06:59 KST / 19:00:00~19:06:59 EST) 추세 판별 윈도우(is_open_7m)를 반환합니다.
    """
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    now_kst = now_est.astimezone(KST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    kst_hm = now_kst.hour * 100 + now_kst.minute

    is_prelock_1m = bool((est_hm == 1859) or (kst_hm == 859))
    is_open_7m = bool((1900 <= est_hm <= 1906) or (900 <= kst_hm <= 906))
    return is_prelock_1m, is_open_7m


def _sync_daymarket_trend_leader_and_reset(
    symbol: str,
    live_rsi_val: float,
    opp_rsi_val: float,
    shared_rsi_state: dict = None,
    now_est: datetime = None,
):
    """
    초과 Case 93:
    1) 데이장(19:00 ~ 03:57 EST) 중 가장 먼저 RSI >= 79.9를 달성한 종목을 day_leader_sym으로 불변 각인합니다.
    2) 1회 출격 후 40.0 <= RSI <= 70.0 중립 밴드 복귀 시 day_opta_rearm_ready 및 day_pb_rearm_ready를 리셋합니다.
    """
    if now_est is None:
        now_est = datetime.now(EST_TZ)

    if not _WHIPSAW_SIM_CACHE.get("day_leader_sym") and isinstance(shared_rsi_state, dict):
        ewm_s = (shared_rsi_state.get("SOXS") or {}).get("ewm_state") or {}
        ewm_l = (shared_rsi_state.get("SOXL") or {}).get("ewm_state") or {}
        ts_s = float(ewm_s.get("first_day_799_ts", 0.0) or 0.0)
        ts_l = float(ewm_l.get("first_day_799_ts", 0.0) or 0.0)
        if ts_s > 0.0 and ts_l > 0.0:
            win_s = "SOXS" if ts_s <= ts_l else "SOXL"
            win_e = ewm_s if win_s == "SOXS" else ewm_l
        elif ts_s > 0.0:
            win_s = "SOXS"
            win_e = ewm_s
        elif ts_l > 0.0:
            win_s = "SOXL"
            win_e = ewm_l
        else:
            win_s = ""
            win_e = {}

        if win_s:
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = win_s
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(
                win_e.get("first_day_799_rsi", PRE_TREND_LEADER_SELECT_RSI) or PRE_TREND_LEADER_SELECT_RSI
            )
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = str(
                win_e.get("first_day_799_time_str") or now_est.strftime("%H:%M:%S")
            )

    if symbol in ("SOXS", "SOXL") and live_rsi_val >= PRE_TREND_LEADER_SELECT_RSI:
        if not _WHIPSAW_SIM_CACHE.get("day_leader_sym"):
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = symbol
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = round(float(live_rsi_val), 1)
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = now_est.strftime("%H:%M:%S")
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
        elif _WHIPSAW_SIM_CACHE.get("day_leader_sym") == symbol:
            if live_rsi_val > float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 0.0)):
                _WHIPSAW_SIM_CACHE["day_leader_rsi"] = round(float(live_rsi_val), 1)

    opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
    if opp_rsi_val >= PRE_TREND_LEADER_SELECT_RSI and not _WHIPSAW_SIM_CACHE.get("day_leader_sym"):
        _WHIPSAW_SIM_CACHE["day_leader_sym"] = opp_sym
        _WHIPSAW_SIM_CACHE["day_leader_rsi"] = round(float(opp_rsi_val), 1)
        _WHIPSAW_SIM_CACHE["day_leader_time_str"] = now_est.strftime("%H:%M:%S")
        _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True

    # 40.0 <= RSI <= 70.0 중립 밴드 복귀 시 재출동 리셋
    if not bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True)):
        if (40.0 <= live_rsi_val <= 70.0) and (opp_rsi_val <= 0.0 or (40.0 <= opp_rsi_val <= 70.0)):
            _WHIPSAW_SIM_CACHE["day_opta_rearm_ready"] = True

    if (
        _WHIPSAW_SIM_CACHE.get("day_leader_sym") == symbol
        and not bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True))
        and (40.0 <= live_rsi_val <= 70.0)
    ):
        _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True


async def settle_daymarket_sim_before_premarket(ctx, now_est: datetime = None):
    """
    초과 Case 78:
    03:58 ~ 04:00 EST 데이장 마감 직전 미결 [모의] 포지션을 자동 정리하고 초기화하여
    본진 프리장 스나이퍼와의 간섭을 원천 차단합니다.
    """
    async with _DAY_SIM_LOCK:
        if not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            return
        _reset_day_sim_position_only()


async def execute_daymarket_rsi_alert_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_price: float,
    rsi_val: float,
    vwap_val: float = 0.0,
):
    """
    초과 Case 78 & Case 93:
    데이장(19:00 ~ 03:57 EST) 100% [모의] 스나이퍼 전용 엔진 (실거래 매수 주문 create_order 절대 차단!).
    """
    if symbol not in ("SOXS", "SOXL"):
        return

    est_hm = now_est.hour * 100 + now_est.minute
    # 03:58 ~ 03:59 EST 데이장 마감 휩소 묵음 바이패스 및 자동 초기화
    if 358 <= est_hm <= 359:
        await settle_daymarket_sim_before_premarket(ctx, now_est)
        return

    if not (est_hm >= 1859 or est_hm <= 357):
        return

    logical_sess_id = _get_logical_day_session_id(now_est)
    is_prelock_1m, is_open_7m = _get_day_opening_window_state(now_est)

    async with _DAY_SIM_LOCK:
        if _WHIPSAW_SIM_CACHE.get("day_sim_session_id") != logical_sess_id:
            _reset_day_sim_state(new_session_id=logical_sess_id)

        # 1) 개시 1분 전(08:59 KST / 18:59 EST) 또는 개시 7분 내 미락온 시 NQ=F 10분봉 추세 1차 락온
        if (is_prelock_1m or is_open_7m) and not _WHIPSAW_SIM_CACHE.get("day_prelock_sym"):
            t_sym, o_10, c_10, b_pct = await MacroDataCache.get_nq_10m_candle_trend(force_refresh=True)
            if t_sym in ("SOXL", "SOXS"):
                _WHIPSAW_SIM_CACHE["day_prelock_sym"] = t_sym
                _WHIPSAW_SIM_CACHE["day_prelock_open10"] = o_10
                _WHIPSAW_SIM_CACHE["day_prelock_close10"] = c_10
                _WHIPSAW_SIM_CACHE["day_prelock_body_pct"] = b_pct
                if not _WHIPSAW_SIM_CACHE.get("day_prelock_notified", False):
                    _WHIPSAW_SIM_CACHE["day_prelock_notified"] = True
                    t_icon = "🐂" if t_sym == "SOXL" else "🐻"
                    candle_lbl = "양봉(상승)" if t_sym == "SOXL" else "음봉(하락)"
                    await ctx.notify_tg(
                        f"🔍 <b>[데이장 개시 1분 전 NQ=F 10분봉 1차 락온] {t_icon}{t_sym} 선정</b>\n"
                        f"▫️ NQ=F 직전 10분봉: 시가 <code>{o_10:.2f}</code> ➡️ 종가 <code>{c_10:.2f}</code> (<code>{b_pct:+.2f}%</code> {candle_lbl})\n"
                        f"▫️ 개시 7분 작전: <code>09:00 ~ 09:06:59 KST (19:00 ~ 19:06:59 EST) 동안 {t_sym} RSI(1m) ≥ 50.0 달성 시 현재가 즉시 [모의] 진입!</code>"
                    )

        if est_hm == 1859 and not is_open_7m:
            return

        if current_price <= 0.0:
            return
        cur_p_fl = math.floor(float(current_price) * 100.0) / 100.0
        if cur_p_fl <= 0.0:
            return

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        opp_dict = ((ctx.shared_rsi_state or {}).get(opp_sym)) or {}
        opp_rsi_val = float(opp_dict.get("value", 0.0))

        _sync_daymarket_trend_leader_and_reset(
            symbol=symbol,
            live_rsi_val=rsi_val,
            opp_rsi_val=opp_rsi_val,
            shared_rsi_state=ctx.shared_rsi_state,
            now_est=now_est,
        )

        my_dict = ((ctx.shared_rsi_state or {}).get(symbol)) or {}
        my_ewm = my_dict.get("ewm_state") or {}

        # =============================================================
        # [A] 데이장 [모의] 포지션 진행 중 (TRAP_WAIT 또는 HOLDING)
        # =============================================================
        if _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            if _WHIPSAW_SIM_CACHE.get("day_sim_symbol") != symbol:
                return

            sym_icon = "🐻" if symbol == "SOXS" else "🐂"
            phase = str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", ""))
            trig_mode = str(_WHIPSAW_SIM_CACHE.get("day_sim_trig_mode", "OPTA"))

            # 1) 1차 매수 덫 대기(TRAP_WAIT) -> 저가 체결 또는 조건부 30초 타임쉴드 현재가 체결
            if phase == "TRAP_WAIT":
                trap_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0))
                trap_ts = float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_ts", 0.0))
                elapsed = time.time() - trap_ts

                rebound_th = (
                    PRE_TREND_PULLBACK_RSI_THRESHOLD
                    if trig_mode == "PB35"
                    else PRE_SNIPER_RSI_THRESHOLD
                )
                is_trap_filled = (cur_p_fl <= trap_p)
                is_rebound_break = (rsi_val > rebound_th)
                is_time_expired = (elapsed >= BUY_TRAP_REARM_WAIT_SEC)

                if not (is_trap_filled or is_rebound_break or is_time_expired):
                    return

                exec_entry_p = trap_p if is_trap_filled else cur_p_fl
                arm_130_p = math.ceil(exec_entry_p * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                floor_100_p = math.ceil(exec_entry_p * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0

                ref_p, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                _, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
                nq_amp = (((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0)

                _WHIPSAW_SIM_CACHE.update({
                    "day_sim_phase": "HOLDING",
                    "day_sim_entry_price": exec_entry_p,
                    "day_sim_arm_price": arm_130_p,
                    "day_sim_floor_price": floor_100_p,
                    "day_sim_target_price": floor_100_p,
                    "day_sim_max_price": max(exec_entry_p, cur_p_fl),
                    "day_sim_trail_armed": False,
                    "day_sim_entry_ts": time.time(),
                    "day_sim_entry_time_str": now_est.strftime("%H:%M:%S"),
                    "day_sim_ref_entry_price": ref_p,
                    "day_sim_ref_src": ref_src,
                    "day_sim_nq_entry_amp": nq_amp,
                })
                ctx.wakeup_event.set()

                fill_reason = (
                    f"1차 저가 매수덫(<code>${trap_p:.2f}</code>) 직격 체결"
                    if is_trap_filled
                    else (
                        f"RSI <code>{rsi_val:.1f}</code> 반등 돌파(>{rebound_th:.0f}) 현재가 즉시 체결"
                        if is_rebound_break
                        else f"30초 타임쉴드 만료 현재가 체결(<code>{elapsed:.1f}초</code>)"
                    )
                )
                await ctx.notify_tg(
                    f"🟢 <b>[데이장 모의 체결 완료] {sym_icon}{symbol} 추격 개시 ({trig_mode})</b>\n"
                    f"▫️ 체결 사유: {fill_reason}\n"
                    f"▫️ [모의] 진입 단가: <code>${exec_entry_p:.2f}</code> ➡️ <b>+1.30% 락온 목표가: <code>${arm_130_p:.2f}</code></b> (최소 보존 <code>${floor_100_p:.2f}</code>)\n"
                    "▫️ 익절 규칙: <code>+1.30% 도달 시 락온 ➡️ HA음봉 출현 &amp; 최고가 대비 -0.30% 하락 시 최소 +1.00% 익절 보장!</code>"
                )
                return

            # 2) [모의] 보유 중(HOLDING) -> +1.30% 락온 고가 추적 트레일링 & 2·3차 모의 손절 감시
            entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
            arm_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_arm_price", 0.0))
            floor_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_floor_price", 0.0))
            max_p = max(float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", entry_p)), cur_p_fl)
            _WHIPSAW_SIM_CACHE["day_sim_max_price"] = max_p

            trail_armed = bool(_WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False))
            just_armed = False
            if not trail_armed and arm_p > 0.0 and cur_p_fl >= arm_p:
                trail_armed = True
                just_armed = True
                _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = True
                ctx.wakeup_event.set()

            is_ha_bull, _, _ = compute_realtime_tick_heikin_ashi(cur_p_fl, my_ewm)
            v10, v3 = await MacroDataCache.get_nq_short_velocity()
            if symbol == "SOXS":
                nq_vel_bypass = bool(min(v10, v3) <= -DAY_SIM_NQ_VEL_SURGE_PCT)
            else:
                nq_vel_bypass = bool(max(v10, v3) >= DAY_SIM_NQ_VEL_SURGE_PCT)
            _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = nq_vel_bypass

            if trail_armed:
                eff_stop_p = max(
                    floor_p,
                    math.floor(max_p * (1.0 - DAY_SIM_TRAIL_DROP_PCT / 100.0) * 100.0) / 100.0,
                )
                _WHIPSAW_SIM_CACHE["day_sim_target_price"] = eff_stop_p

                if just_armed:
                    peak_pct_now = ((max_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.30
                    await ctx.notify_tg(
                        f"🔥 <b>[데이장 모의 +1.30% 트레일링 락온!] {sym_icon}{symbol} 최소 +1.00% 익절 보장 결속</b>\n"
                        f"▫️ 진입가: <code>${entry_p:.2f}</code> ➡️ 현재 고점: <code>${max_p:.2f}</code> (<code>+{peak_pct_now:.2f}%</code>)\n"
                        f"▫️ 동적 스탑선(-0.30%): <code>${eff_stop_p:.2f}</code> (하한 보존선 <code>${floor_p:.2f}</code>)\n"
                        "▫️ 상태: <code>🕯️HA양봉 유지 중 최고가 무한 추적 가동!</code>"
                    )
                    return

                is_cb_hit = bool(cur_p_fl <= eff_stop_p)
                is_ha_bear_stop = bool((not is_ha_bull) and (not nq_vel_bypass) and (cur_p_fl <= eff_stop_p))

                if is_cb_hit or is_ha_bear_stop:
                    exit_p = max(floor_p, cur_p_fl)
                    gross_pct = ((exit_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                    net_pct = gross_pct - (ROUND_TRIP_COMMISSION_RATE * 100.0)
                    peak_pct = ((max_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0

                    _record_day_sim_result(trig_mode=trig_mode, is_win=True, net_pct=net_pct)
                    scoreboard_str = _build_day_sim_scoreboard_line()
                    _reset_day_sim_position_only()

                    await ctx.notify_tg(
                        f"💰 <b>[데이장 모의 트레일링 익절 확정!] {sym_icon}{symbol} ({trig_mode})</b>\n"
                        f"▫️ 궤적: 진입 <code>${entry_p:.2f}</code> ➡️ 경유 최고점 <code>${max_p:.2f} (+{peak_pct:.2f}%)</code> ➡️ 익절 <code>${exit_p:.2f}</code>\n"
                        f"▫️ 수익률: 세전 <code>+{gross_pct:.2f}%</code> / <b>세후 <code>+{net_pct:.2f}%</code></b> (최소 +1.00% 보장 완료)\n"
                        f"{scoreboard_str}"
                    )
                return

            # 3) +1.30% 락온 전 손실 구간(cur_p_fl < entry_p) -> 2·3차 매크로 [모의] 손절 감시
            if cur_p_fl < entry_p:
                _, nq_h_c, nq_l_c = await MacroDataCache.get_cached_nq_data()
                nq_amp_c = (((nq_h_c - nq_l_c) / nq_l_c * 100.0) if nq_l_c > 0.0 else 0.0)
                ref_curr, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                ref_entry = float(_WHIPSAW_SIM_CACHE.get("day_sim_ref_entry_price", 0.0)) or ref_curr
                entry_amp = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0)) or nq_amp_c

                dyn_th = max(entry_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT)
                adv_pct = 0.0
                if ref_curr > 0.0 and ref_entry > 0.0:
                    if symbol == "SOXS":
                        adv_pct = (ref_curr - ref_entry) / ref_entry * 100.0
                    else:
                        adv_pct = (ref_entry - ref_curr) / ref_entry * 100.0

                is_s2 = bool(nq_amp_c >= 2.0 and entry_amp < 2.0)
                is_s3 = bool(adv_pct >= dyn_th)

                if is_s2 or is_s3:
                    gross_pct = ((cur_p_fl / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                    net_pct = gross_pct - (ROUND_TRIP_COMMISSION_RATE * 100.0)
                    _record_day_sim_result(trig_mode=trig_mode, is_win=False, net_pct=net_pct)
                    scoreboard_str = _build_day_sim_scoreboard_line()
                    _reset_day_sim_position_only()

                    stop_reason = (
                        f"2차 과열(NQ진폭 <code>{nq_amp_c:.2f}% ≥ 2.0%</code>)"
                        if is_s2
                        else f"3차 {ref_src} 역행(<code>+{adv_pct:.2f}% ≥ +{dyn_th:.2f}%</code>)"
                    )
                    await ctx.notify_tg(
                        f"🛑 <b>[데이장 모의 안전망 손절] {sym_icon}{symbol} ({trig_mode})</b>\n"
                        f"▫️ 사유: {stop_reason}\n"
                        f"▫️ 내역: 진입 <code>${entry_p:.2f}</code> ➡️ 손절 <code>${cur_p_fl:.2f}</code> (세후 <code>{net_pct:+.2f}%</code>)\n"
                        f"{scoreboard_str}"
                    )
            return

        # =============================================================
        # [B] 데이장 [모의] 미보유 감시 상태 -> 1막(개시 7분) & 2막(OPTA / PB35) 타점 포착
        # =============================================================
        nq_c, nq_h, nq_l = await MacroDataCache.get_cached_nq_data()
        nq_amp = (((nq_h - nq_l) / nq_l * 100.0) if nq_l > 0.0 else 0.0)
        if nq_amp >= 2.0:
            return

        sym_icon = "🐻" if symbol == "SOXS" else "🐂"
        opp_icon = "🐂" if opp_sym == "SOXL" else "🐻"

        # -------------------------------------------------------------
        # [제1막] 개시 직후 7분간 (09:00~09:06:59 KST / 19:00~19:06:59 EST)
        #         1차 락온 종목 RSI >= 50.0 달성 시 현재가 즉시 [모의] 진입 (OPEN7M_TREND)
        # -------------------------------------------------------------
        if is_open_7m:
            prelock_sym = str(_WHIPSAW_SIM_CACHE.get("day_prelock_sym") or "")
            if (
                not _WHIPSAW_SIM_CACHE.get("day_open7m_done", False)
                and prelock_sym == symbol
                and rsi_val >= 50.0
            ):
                arm_130_p = math.ceil(cur_p_fl * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                floor_100_p = math.ceil(cur_p_fl * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
                ref_p, ref_src = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)

                _WHIPSAW_SIM_CACHE.update({
                    "day_open7m_done": True,
                    "day_sim_active": True,
                    "day_sim_phase": "HOLDING",
                    "day_sim_symbol": symbol,
                    "day_sim_trig_mode": "OPEN7M_TREND",
                    "day_sim_spot_price": cur_p_fl,
                    "day_sim_trap_price": cur_p_fl,
                    "day_sim_entry_price": cur_p_fl,
                    "day_sim_arm_price": arm_130_p,
                    "day_sim_floor_price": floor_100_p,
                    "day_sim_target_price": floor_100_p,
                    "day_sim_max_price": cur_p_fl,
                    "day_sim_trail_armed": False,
                    "day_sim_entry_ts": time.time(),
                    "day_sim_entry_time_str": now_est.strftime("%H:%M:%S"),
                    "day_sim_ref_entry_price": ref_p,
                    "day_sim_ref_src": ref_src,
                    "day_sim_nq_entry_amp": nq_amp,
                })
                ctx.wakeup_event.set()
                b_pct_pre = float(_WHIPSAW_SIM_CACHE.get("day_prelock_body_pct", 0.0))
                await ctx.notify_tg(
                    f"🚀 <b>[데이장 개시 7분 추세판별 즉시 모의진입!] {sym_icon}{symbol} (OPEN7M_TREND)</b>\n"
                    f"▫️ 판별 근거: 개시 1분 전 NQ=F 10분봉(<code>{b_pct_pre:+.2f}%</code>) 1차 락온 ➡️ 개시 7분 내 {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≥50.0 상승 확증)\n"
                    f"▫️ [모의] 현재가 진입: <code>${cur_p_fl:.2f}</code> ➡️ <b>+1.30% 락온 목표가: <code>${arm_130_p:.2f}</code></b> (최소 보존 <code>${floor_100_p:.2f}</code>)"
                )
            return

        # -------------------------------------------------------------
        # [제2막] 개시 7분 이후 (09:07 KST~ / 19:07 ~ 03:57 EST)
        #         2대 [모의] 타격 게이트 : ① OPTA (대칭 <=30.0 & >=79.9) | ② PB35 (상승장 락온 종목 RSI<=35.0)
        # -------------------------------------------------------------
        is_opta_ok = bool(
            _WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True)
            and (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
            and (opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD)
        )
        is_pb35_ok = bool(
            _WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True)
            and (_WHIPSAW_SIM_CACHE.get("day_leader_sym") == symbol)
            and (0.0 < rsi_val <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
        )

        if not (is_opta_ok or is_pb35_ok):
            return

        if is_opta_ok:
            trig_mode_new = "OPTA"
            _WHIPSAW_SIM_CACHE["day_opta_rearm_ready"] = False
        else:
            trig_mode_new = "PB35"
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = False

        slip_disc = max(
            BUY_TRAP_MIN_DISC_USD,
            round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2),
        )
        trap_p = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)
        arm_130_p = math.ceil(trap_p * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0

        _WHIPSAW_SIM_CACHE.update({
            "day_sim_active": True,
            "day_sim_phase": "TRAP_WAIT",
            "day_sim_symbol": symbol,
            "day_sim_trig_mode": trig_mode_new,
            "day_sim_spot_price": cur_p_fl,
            "day_sim_trap_price": trap_p,
            "day_sim_arm_price": arm_130_p,
            "day_sim_trap_ts": time.time(),
        })
        ctx.wakeup_event.set()

        if trig_mode_new == "PB35":
            ld_t = str(_WHIPSAW_SIM_CACHE.get("day_leader_time_str") or "")
            ld_r = float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi") or 79.9)
            title_line = f"👑 <b>[데이장 모의 상승장 35눌림 포착] {sym_icon}{symbol} 1차 매수 덫 투하 (PB35)</b>"
            cond_line = (
                f"▫️ 조건: 데이장 첫 상승종목 {sym_icon}{symbol} (<code>{ld_t} EST</code> RSI <code>{ld_r:.1f}</code> 락온) "
                f"➡️ 현재 RSI <code>{rsi_val:.1f}</code> (≤35.0 눌림목 도달)"
            )
        else:
            title_line = f"🔔 <b>[데이장 모의 대칭 클라이맥스 포착] {sym_icon}{symbol} 1차 매수 덫 투하 (OPTA)</b>"
            cond_line = (
                f"▫️ 대칭 조건: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≤30.0) &amp; "
                f"{opp_icon}{opp_sym} RSI <code>{opp_rsi_val:.1f}</code> (≥79.9)"
            )

        await ctx.notify_tg(
            f"{title_line}\n"
            f"{cond_line}\n"
            f"▫️ 포착 현재가: <code>${cur_p_fl:.2f}</code> ➡️ [모의] 1차 매수 덫: <code>${trap_p:.2f}</code> (30초 타임쉴드 대기)"
        )
