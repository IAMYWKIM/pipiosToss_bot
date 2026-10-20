# =====================================================================
# FILE: daymarket_sim_plugin.py
# 목적: 데이장(19:00~03:57 EST) 100% [모의] 전용 스나이퍼 플러그인 (whipsaw_plugin.py 1/2 분할본)
#       (실거래 매수 절대 차단!) —
#       [1] 데이장 개시 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물지수(NQ=F) 10분봉 양/음봉 판별로 롱(SOXL)·숏(SOXS) 1차 락온 ➔
#           개시 직후 7분간(09:00:00~09:06:59 KST / 19:00:00~19:06:59 EST)에만 1차 락온 종목이 RSI(1m)>=50.0 달성 시
#           최종 상승종목으로 판별하여 바로 실시간 현재가로 즉시 [모의] 진입(🚀 OPEN7M_TREND · 개시 7분간 반대편 과매도 역추세 진입 전면 차단),
#       [2] 개시 7분 이후(09:07 KST ~ / 19:07 ~ 03:57 EST)에는 데이장 첫 RSI>=79.9(80.0) 달성 상승장 종목(day_leader_sym)이
#           RSI<=35.0으로 떨어졌을 때(👑 PB35) 및 양방향 동시 대칭(<=30.0 & 반대>=79.9, 🪤 OPTA)이 발생할 때마다 [모의] 스나이퍼 출격
#           (단, 1회 조건 달성 후 '40.0 <= RSI <= 70.0' [70 이하 40 이상] 중립 구간 복귀 시 리셋하여 재달성 시마다 무제한 반복 출동!),
#       [3] 모든 데이장 [모의] 포지션은 진입가 대비 +1.30% 상승가에서 락온되어 하이킨 아시 1분봉 음봉(🌑HA음봉) 출현 및
#           최고가 대비 -0.30% 하락 시 최소 +1.00% 익절을 보장받는 고가 추적 트레일링 가동 & 당일 출격 빈도·적중 확률(승률 %) 실시간 집계,
#       [4] 03:58~03:59 EST 마감 휩소 묵음 바이패스 및 프리장 전환 자동 클리어
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 whipsaw_plugin.py의 데이장 100% [모의] 스나이퍼 코어를 daymarket_sim_plugin.py로 무손실 분리
# MODIFIED: 데이장(19:00~03:57 EST)은 100% [모의] 전용이며 어떠한 경우에도 실거래 매수 주문(create_order)을 발사하지 않도록 물리적 차단 (Case 53, 78, 93)
# NEW: 초과 Case 78 & Case 93 — 개시 1분 전 NQ 10분봉 1차 락온 ➔ 개시 7분간 RSI>=50.0 최종 상승종목 즉시 진입(OPEN7M_TREND),
#      개시 7분 이후 상승장 종목 35눌림(PB35) & 양방향 대칭(OPTA) 40.0<=RSI<=70.0 리셋 무한 반복 출격,
#      +1.30% 상승가 락온 × 최고가 -0.30% 하락 & 🌑HA음봉 시 최소 +1.00% 익절 보장 트레일링 및 실시간 승률 트래커 탑재

import math
import time
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    DAY_OPEN_7M_RSI_THRESHOLD,
    DAY_SIM_LOCK_PROFIT_RATE,
    DAY_SIM_FLOOR_PROFIT_RATE,
    DAY_SIM_CALLBACK_PCT,
    DAY_REARM_RESET_RSI_MIN,
    DAY_REARM_RESET_RSI_MAX,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    compute_realtime_tick_rsi,
    compute_realtime_tick_heikin_ashi,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 양방향 동시 대칭 클라이맥스 매수 대상 종목 과매도 임계치 (30.0 이하 엄격 고정)
PRE_SNIPER_RSI_THRESHOLD = 30.0
# 07:00 ~ 08:14 EST / 데이장 선행 첫 상승 추세 종목(leader_sym)의 추세눌림목 0.4초 부스터 근접 임계치 (38.0 이하)
PRE_TREND_BOOSTER_RSI = 38.0
# 반대편 대세 종목이 79.0 이상에 진입하면 1.5초 루프를 0.4초 고주파로 당기는 프리-클라이맥스 부스터 임계치 (Case 28, 83)
PRE_CLIMAX_BOOSTER_RSI = 79.0
# 순간 스파이크 누락 방지를 위해 반대편 대세 종목 및 상승 주도주 RSI 79.9 이상을 80.0 클라이맥스로 즉시 인지 (Case 81, 83, 97)
OPP_CLIMAX_RSI_THRESHOLD = PRE_TREND_LEADER_SELECT_RSI

# 데이장 (19:00 ~ 03:57 EST) [모의] +1.30% 상승가 락온 × 최고가 -0.30% 하락(최소 +1.00% 익절 보장) 상수 (초과 Case 78, 93)
DAY_SIM_ARM_PROFIT_RATE = DAY_SIM_LOCK_PROFIT_RATE       # +1.30% 상승가에서 트레일링 락온!
DAY_SIM_FLOOR_LOCK_RATE = DAY_SIM_FLOOR_PROFIT_RATE      # +1.00% 최소 보장 익절선!
DAY_SIM_TRAIL_DROP_PCT = DAY_SIM_CALLBACK_PCT            # 최고가 대비 -0.30% 하락 시 익절!
DAY_SIM_NQ_VEL_SURGE_PCT = AssassinLedger.NQ_VELOCITY_SURGE_PCT
_DAY_SIM_LOCK = asyncio.Lock()
_WHIPSAW_LOCK = asyncio.Lock()

# 1차 슬리피지 지정가 매수 덫 & 조건부 30초 타임쉴드 상수 (프리장 실전 및 데이장 개시 7분 이후 모의 공통)
BUY_TRAP_SLIPPAGE_PCT = 0.18          # 1차 매수 덫 슬리피지 할인율 (-0.18%, 예: $43.00 -> $42.92)
BUY_TRAP_MIN_DISC_USD = 0.02          # 1차 매수 덫 최소 할인 폭 (-$0.02 보장)
BUY_TRAP_REARM_WAIT_SEC = 30.0        # RSI <= 30.0 (추세눌림목은 <= 35.0) 유지 시 1차 매수 덫 최대 타임쉴드 대기 시간 (30.0초)

# 초과 Case 79 & Case 95 — 03:58 ~ 04:07 EST (9분간) 실전 듀얼 1주 고정 '휩소 노출 작전' 상수
WHIPSAW_FIXED_QTY = 1                      # 시드 무관 무조건 1주 고정 테스트
WHIPSAW_BUY_REVERSE_FACTOR = 1.005         # 데이장 VWAP = 1.005 * 매수덫가격 (VWAP 대비 0.5% 역산 하단 매수 덫)
WHIPSAW_SELL_PROFIT_FACTOR = 1.01          # 목표 매도덫가격 = ceil(매수체결가 * 1.01) (+1.00% 익절 매도 덫)
WHIPSAW_PROFIT_FACTOR = WHIPSAW_SELL_PROFIT_FACTOR

# 데이장 100% [모의] (개시 1분전 NQ 10분봉 1차 락온 ➔ 개시 7분간 RSI>=50 최종판별 즉시진입 / 개시 7분후 대칭·상승장35눌림 40~70 리셋 무한출격 / +1.30% 락온 트레일링 / 확률 집계) 및 03:58 휩소 런타임 캐시
_WHIPSAW_SIM_CACHE = {
    "day_logical_date": "",
    # [제0막 & 제1막] 데이장 개시 1분 전 NQ=F 10분봉 1차 락온 & 개시 후 7분간 RSI >= 50.0 최종 상승종목 판별 슬롯
    "day_prelock_sym": "",            # 개시 1분 전 NQ 10분봉으로 1차 락온된 종목 ("SOXL" 또는 "SOXS")
    "day_prelock_dir": "",            # "LONG" (10분봉 상승) | "SHORT" (10분봉 하락)
    "day_prelock_vel_10m": 0.0,       # 1차 락온 당시 NQ 10분봉 등락률 (%)
    "day_prelock_open_10m": 0.0,      # 1차 락온 당시 NQ 10분봉 시가
    "day_prelock_close_10m": 0.0,     # 1차 락온 당시 NQ 10분봉 종가
    "day_prelock_window_tag": "",     # "EST_1900" 또는 "KST_0900" (개시 윈도우 태그)
    "day_prelock_notified_tag": "",   # 개시 1분 전 1차 락온 알림 발송 완료 윈도우 태그
    "day_open7m_confirmed_sym": "",   # 개시 7분간 RSI >= 50.0 달성으로 최종 확정된 상승종목
    "day_open7m_fired_tag": "",       # 개시 7분간 최종 판별 모의 진입 발사 완료 윈도우 태그
    # [제2막] 데이장 첫 RSI >= 79.9(80.0) 달성 상승장 종목 각인 & 40.0 <= RSI <= 70.0 리셋 플래그
    "day_leader_sym": "",             # 데이장 개장(19:00 EST) 이후 첫 RSI >= 79.9 달성 상승장 종목 (Case 78, 93, 97)
    "day_leader_rsi": 0.0,            # 데이장 상승장 종목 최고 RSI
    "day_leader_time_str": "",        # 데이장 상승장 종목 최초 달성 시각
    "day_pb_rearm_ready": True,       # 상승장 종목 RSI <= 35.0 출격 가능 여부 (1회 달성 후 40 <= RSI <= 70 복귀 시 True 리셋)
    "day_opta_rearm_ready": True,     # 양방향 대칭(<=30 & >=79.9) 출격 가능 여부 (1회 달성 후 40 <= RSI <= 70 복귀 시 True 리셋)
    "day_opta_last_sym": "",          # 직전 양방향 대칭으로 출격한 종목
    # [진행 중 모의 포지션 슬롯]
    "day_sim_active": False,
    "day_sim_symbol": "",             # 현재 모의 포착/보유 종목 ("SOXS" 또는 "SOXL")
    "day_sim_trig_mode": "OPTA",      # "OPEN7M_TREND" (개시 7분 추세) | "OPTA" (동시 대칭) | "PB35" (상승장 종목 RSI<=35 눌림)
    "day_sim_exit_regime": "LOCK_130_CB_030",
    "day_sim_ha_bear_seen": False,    # +1.30% 락온 이후 하이킨 아시 1분봉 음봉(🌑HA음봉) 출현 여부
    "day_sim_phase": "",              # "TRAP_WAIT" (1차 슬리피지 덫 대기) | "HOLDING" (체결 후 +1.30% 락온 고가 추적 트레일링)
    "day_sim_spot_price": 0.0,        # 최초 포착 당시 현재가
    "day_sim_trap_price": 0.0,        # 1차 슬리피지(-0.18%) 매수 덫 가격
    "day_sim_entry_price": 0.0,       # 확정된 모의 진입(체결) 평단가
    "day_sim_fill_type": "",          # 체결 유형 설명 문자열
    "day_sim_floor_price": 0.0,       # +1.00% 최소 익절 보장 보존선 (ceil(entry_price * 1.0100))
    "day_sim_arm_price": 0.0,         # +1.30% 트레일링 락온 기준가 (ceil(entry_price * 1.0130))
    "day_sim_target_price": 0.0,      # UI 표출용 목표가 (락온 전에는 +1.30% arm_price, 락온 후에는 유효 스탑가)
    "day_sim_trail_armed": False,     # +1.30% 상승가 도달 트레일링 락온 여부
    "day_sim_nq_vel_bypass": False,   # 트레일링 중 QQQ/NQ ±0.20% 급등락 HA음봉 바이패스 작동 여부
    "day_sim_armed_ts": 0.0,          # +1.30% 트레일링 락온 시각 타임스탬프
    "day_sim_armed_hm": 0,            # +1.30% 트레일링 락온 분봉 시각 (HHMM)
    "day_sim_min_price": 0.0,         # 경유 최저가 (MAE)
    "day_sim_max_price": 0.0,         # 경유 최고가 (Peak)
    "day_sim_start_ts": 0.0,          # 포착 시각 타임스탬프 (30초 타임쉴드 측정용)
    "day_sim_entry_time_str": "",
    "day_sim_buy_rsi": 0.0,           # 포착 당시 진입 종목 RSI
    "day_sim_opp_rsi": 0.0,           # 포착 당시 반대 종목 RSI
    "day_sim_nq_entry_price": 0.0,    # 모의 진입 시점 3차 방어 기준가 (토스 QQQ 0.0초 실시간 현재가, 폴백 NQ=F)
    "day_sim_nq_entry_amp": 0.0,      # 모의 진입 시점 NQ=F 당일 진폭 (max(진폭*0.5, 0.80%) 임계치 산출 기준!)
    "day_sim_ref_src": "QQQ실시간",   # 3차 방어 기준가 소스 라벨 ("QQQ실시간" 또는 "NQ선물")
    # [당일 데이장 모의 스나이퍼 출격 빈도 & 적중 확률(승률) 실시간 집계 슬롯]
    "day_stat_total": 0,              # 당일 데이장 모의 총 정산 완료 횟수
    "day_stat_wins": 0,               # 당일 데이장 모의 익절 승리 횟수
    "day_stat_losses": 0,             # 당일 데이장 모의 손절 패배 횟수
    "day_stat_cum_net_pct": 0.0,      # 당일 데이장 모의 누적 세후 수익률 (%)
    "day_stat_open7m_runs": 0,        # 개시 7분 추세추종(OPEN7M_TREND) 출격 횟수
    "day_stat_open7m_wins": 0,        # 개시 7분 추세추종 익절 승리 횟수
    "day_stat_opta_runs": 0,          # 양방향 대칭(OPTA) 출격 횟수
    "day_stat_opta_wins": 0,          # 양방향 대칭 익절 승리 횟수
    "day_stat_pb35_runs": 0,          # 상승장 락온 종목 RSI<=35.0 눌림목(PB35) 출격 횟수
    "day_stat_pb35_wins": 0,          # 상승장 락온 종목 RSI<=35.0 눌림목 익절 승리 횟수
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


def _reset_day_sim_position_only():
    """데이장 [모의] 단일 포지션 슬롯만 초기화하고, 당일 상승장 종목·리셋 상태·출격 전적 통계는 보존합니다."""
    _WHIPSAW_SIM_CACHE["day_sim_active"] = False
    _WHIPSAW_SIM_CACHE["day_sim_symbol"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_trig_mode"] = "OPTA"
    _WHIPSAW_SIM_CACHE["day_sim_exit_regime"] = "LOCK_130_CB_030"
    _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = False
    _WHIPSAW_SIM_CACHE["day_sim_phase"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_floor_price"] = 0.0
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


def _reset_day_sim_state(reset_session_all: bool = False):
    """데이장 [모의] 추격 상태를 초기화하며, 새 데이장 세션 개장(reset_session_all=True) 시 전적과 락온 슬롯도 함께 초기화합니다."""
    _reset_day_sim_position_only()
    if reset_session_all:
        _WHIPSAW_SIM_CACHE["day_prelock_sym"] = ""
        _WHIPSAW_SIM_CACHE["day_prelock_dir"] = ""
        _WHIPSAW_SIM_CACHE["day_prelock_vel_10m"] = 0.0
        _WHIPSAW_SIM_CACHE["day_prelock_open_10m"] = 0.0
        _WHIPSAW_SIM_CACHE["day_prelock_close_10m"] = 0.0
        _WHIPSAW_SIM_CACHE["day_prelock_window_tag"] = ""
        _WHIPSAW_SIM_CACHE["day_prelock_notified_tag"] = ""
        _WHIPSAW_SIM_CACHE["day_open7m_confirmed_sym"] = ""
        _WHIPSAW_SIM_CACHE["day_open7m_fired_tag"] = ""
        _WHIPSAW_SIM_CACHE["day_leader_sym"] = ""
        _WHIPSAW_SIM_CACHE["day_leader_rsi"] = 0.0
        _WHIPSAW_SIM_CACHE["day_leader_time_str"] = ""
        _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
        _WHIPSAW_SIM_CACHE["day_opta_rearm_ready"] = True
        _WHIPSAW_SIM_CACHE["day_opta_last_sym"] = ""
        _WHIPSAW_SIM_CACHE["day_stat_total"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_wins"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_losses"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_cum_net_pct"] = 0.0
        _WHIPSAW_SIM_CACHE["day_stat_open7m_runs"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_open7m_wins"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_opta_runs"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_opta_wins"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_pb35_runs"] = 0
        _WHIPSAW_SIM_CACHE["day_stat_pb35_wins"] = 0


def _record_day_sim_result(trig_mode: str, is_win: bool, net_pct: float):
    """데이장 [모의] 스나이퍼 익절/손절 완료 시 출격 빈도 및 적중 확률(승률 %) 통계를 갱신합니다 (초과 Case 78, 93)."""
    _WHIPSAW_SIM_CACHE["day_stat_total"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_total", 0)) + 1
    if is_win:
        _WHIPSAW_SIM_CACHE["day_stat_wins"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_wins", 0)) + 1
    else:
        _WHIPSAW_SIM_CACHE["day_stat_losses"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_losses", 0)) + 1
    _WHIPSAW_SIM_CACHE["day_stat_cum_net_pct"] = round(
        float(_WHIPSAW_SIM_CACHE.get("day_stat_cum_net_pct", 0.0)) + float(net_pct), 2
    )

    if trig_mode == "OPEN7M_TREND":
        _WHIPSAW_SIM_CACHE["day_stat_open7m_runs"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_runs", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["day_stat_open7m_wins"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_wins", 0)) + 1
    elif trig_mode == "PB35":
        _WHIPSAW_SIM_CACHE["day_stat_pb35_runs"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_runs", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["day_stat_pb35_wins"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_wins", 0)) + 1
    else:
        _WHIPSAW_SIM_CACHE["day_stat_opta_runs"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_runs", 0)) + 1
        if is_win:
            _WHIPSAW_SIM_CACHE["day_stat_opta_wins"] = int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_wins", 0)) + 1


def _build_day_sim_scoreboard_line() -> str:
    """텔레그램 정산 알림에 첨부할 당일 데이장 [모의] 누적 출격 빈도 및 적중 승률(%) 요약 라인을 생성합니다."""
    tot = int(_WHIPSAW_SIM_CACHE.get("day_stat_total", 0))
    wins = int(_WHIPSAW_SIM_CACHE.get("day_stat_wins", 0))
    losses = int(_WHIPSAW_SIM_CACHE.get("day_stat_losses", 0))
    cum_net = float(_WHIPSAW_SIM_CACHE.get("day_stat_cum_net_pct", 0.0))
    win_rate = (wins / tot * 100.0) if tot > 0 else 0.0

    r_open = int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_runs", 0))
    w_open = int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_wins", 0))
    r_opta = int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_runs", 0))
    w_opta = int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_wins", 0))
    r_pb35 = int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_runs", 0))
    w_pb35 = int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_wins", 0))

    return (
        f"▫️ 📊 <b>당일 데이장 [모의] 적중 확률</b>: 총 <code>{tot}회</code> 출격 | "
        f"<code>{wins}승 {losses}패</code> (승률 <code>{win_rate:.1f}%</code> · 누적 세후 <code>{cum_net:+.2f}%</code>)\n"
        f"  ⤷ 부문별: 🚀개시7분추세 <code>{w_open}/{r_open}</code> · 🪤대칭 <code>{w_opta}/{r_opta}</code> · 👑상승장35눌림 <code>{w_pb35}/{r_pb35}</code>"
    )


def get_daymarket_sim_status() -> dict:
    """UI 관제탑(/avwap, /settlement) 및 main.py 부스터에서 데이장 100% [모의] 추격 상태와 승률 통계를 조회하기 위한 헬퍼 (초과 Case 78, 93)."""
    tot = int(_WHIPSAW_SIM_CACHE.get("day_stat_total", 0))
    wins = int(_WHIPSAW_SIM_CACHE.get("day_stat_wins", 0))
    losses = int(_WHIPSAW_SIM_CACHE.get("day_stat_losses", 0))
    win_rate = (wins / tot * 100.0) if tot > 0 else 0.0
    return {
        "active": bool(_WHIPSAW_SIM_CACHE.get("day_sim_active", False)),
        "symbol": str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "")),
        "trig_mode": str(_WHIPSAW_SIM_CACHE.get("day_sim_trig_mode", "OPTA")),
        "exit_regime": str(_WHIPSAW_SIM_CACHE.get("day_sim_exit_regime", "LOCK_130_CB_030")),
        "prelock_sym": str(_WHIPSAW_SIM_CACHE.get("day_prelock_sym", "")),
        "prelock_dir": str(_WHIPSAW_SIM_CACHE.get("day_prelock_dir", "")),
        "prelock_vel_10m": float(_WHIPSAW_SIM_CACHE.get("day_prelock_vel_10m", 0.0)),
        "open7m_confirmed_sym": str(_WHIPSAW_SIM_CACHE.get("day_open7m_confirmed_sym", "")),
        "leader_sym": str(_WHIPSAW_SIM_CACHE.get("day_leader_sym", "")),
        "leader_rsi": float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 0.0)),
        "leader_time": str(_WHIPSAW_SIM_CACHE.get("day_leader_time_str", "")),
        "pb_rearm_ready": bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True)),
        "opta_rearm_ready": bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True)),
        "phase": str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", "")),
        "spot_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", 0.0)),
        "trap_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0)),
        "entry_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0)),
        "fill_type": str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "")),
        "floor_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_floor_price", 0.0)),
        "arm_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_arm_price", 0.0)),
        "target_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0)),
        "trail_armed": bool(_WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False)),
        "ha_bear_seen": bool(_WHIPSAW_SIM_CACHE.get("day_sim_ha_bear_seen", False)),
        "nq_vel_bypass": bool(_WHIPSAW_SIM_CACHE.get("day_sim_nq_vel_bypass", False)),
        "min_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0)),
        "max_price": float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0)),
        "entry_time": str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "")),
        "buy_rsi": float(_WHIPSAW_SIM_CACHE.get("day_sim_buy_rsi", 0.0)),
        "opp_rsi": float(_WHIPSAW_SIM_CACHE.get("day_sim_opp_rsi", 0.0)),
        "stat_total": tot,
        "stat_wins": wins,
        "stat_losses": losses,
        "stat_win_rate": round(win_rate, 1),
        "stat_cum_net_pct": float(_WHIPSAW_SIM_CACHE.get("day_stat_cum_net_pct", 0.0)),
        "stat_open7m_runs": int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_runs", 0)),
        "stat_open7m_wins": int(_WHIPSAW_SIM_CACHE.get("day_stat_open7m_wins", 0)),
        "stat_opta_runs": int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_runs", 0)),
        "stat_opta_wins": int(_WHIPSAW_SIM_CACHE.get("day_stat_opta_wins", 0)),
        "stat_pb35_runs": int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_runs", 0)),
        "stat_pb35_wins": int(_WHIPSAW_SIM_CACHE.get("day_stat_pb35_wins", 0)),
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


def _get_day_opening_window_state(now_est: datetime) -> tuple[bool, bool, str, str]:
    """
    현재 시각이 ① 데이장 개시 1분 전(08:59 KST 또는 18:59 EST)인지,
    ② 데이장 개시 직후 7분간(09:00~09:06:59 KST 또는 19:00~19:06:59 EST)인지 판별합니다 (초과 Case 78, 93).
    반환값: (is_prelock_1m, is_open_7m, window_tag, window_label)
    """
    now_kst = now_est.astimezone(KST_TZ)
    kst_hm = now_kst.hour * 100 + now_kst.minute
    est_hm = now_est.hour * 100 + now_est.minute

    if kst_hm == 859:
        return True, False, "KST_0900", "한국 09:00 KST 데이장 개시"
    if 900 <= kst_hm <= 906:
        return False, True, "KST_0900", "한국 09:00~09:07 KST 개시 7분"
    if est_hm == 1859:
        return True, False, "EST_1900", "미국 19:00 EST 데이장 개시"
    if 1900 <= est_hm <= 1906:
        return False, True, "EST_1900", "미국 19:00~19:07 EST 개시 7분"
    return False, False, "", ""


def _sync_daymarket_trend_leader_and_reset(
    ctx, symbol: str, rsi_val: float, opp_sym: str, opp_rsi_val: float, now_est: datetime
):
    """
    초과 Case 78, 93 & 97 —
    1) 데이장(19:00 ~ 03:57 EST) 중 가장 먼저 RSI >= 79.9(80.0 클라이맥스)를 달성한 첫 상승장 종목(day_leader_sym)을 불변 각인하며,
       장중 재기동 시에도 ewm_state의 first_day_799_ts 벡터 스캔 결과로 즉시 복원합니다.
    2) 1회 조건 달성 후 '40.0 <= RSI <= 70.0' (70 이하 40 이상) 중립 밴드 복귀 시
       day_pb_rearm_ready 및 day_opta_rearm_ready를 True로 즉시 리셋합니다!
    """
    cur_leader = str(_WHIPSAW_SIM_CACHE.get("day_leader_sym", ""))
    if not cur_leader and ctx.shared_rsi_state:
        ewm_s = ((ctx.shared_rsi_state.get("SOXS") or {}).get("ewm_state")) or {}
        ewm_l = ((ctx.shared_rsi_state.get("SOXL") or {}).get("ewm_state")) or {}
        ts_s = float(ewm_s.get("first_day_799_ts", 0.0))
        ts_l = float(ewm_l.get("first_day_799_ts", 0.0))
        if ts_s > 0.0 and (ts_l <= 0.0 or ts_s <= ts_l):
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = "SOXS"
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(ewm_s.get("first_day_799_rsi", 79.9))
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = str(ewm_s.get("first_day_799_time_str", ""))
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
            cur_leader = "SOXS"
        elif ts_l > 0.0 and (ts_s <= 0.0 or ts_l < ts_s):
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = "SOXL"
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(ewm_l.get("first_day_799_rsi", 79.9))
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = str(ewm_l.get("first_day_799_time_str", ""))
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
            cur_leader = "SOXL"

    if not cur_leader:
        if rsi_val >= OPP_CLIMAX_RSI_THRESHOLD:
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = symbol
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(rsi_val)
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = now_est.strftime("%H:%M:%S")
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
            cur_leader = symbol
        elif opp_rsi_val >= OPP_CLIMAX_RSI_THRESHOLD:
            _WHIPSAW_SIM_CACHE["day_leader_sym"] = opp_sym
            _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(opp_rsi_val)
            _WHIPSAW_SIM_CACHE["day_leader_time_str"] = now_est.strftime("%H:%M:%S")
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True
            cur_leader = opp_sym

    if cur_leader == symbol and rsi_val > float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 0.0)):
        _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(rsi_val)
    elif cur_leader == opp_sym and opp_rsi_val > float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 0.0)):
        _WHIPSAW_SIM_CACHE["day_leader_rsi"] = float(opp_rsi_val)

    # [핵심 리셋 규칙 1] 상승장 락온 종목(day_leader_sym)의 RSI <= 35.0 눌림목 출격 후
    # 해당 종목 RSI가 '40.0 이상 70.0 이하' (DAY_REARM_RESET_RSI_MIN <= RSI <= DAY_REARM_RESET_RSI_MAX)에 진입하면 리셋!
    if not bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True)):
        leader_now_rsi = rsi_val if cur_leader == symbol else (opp_rsi_val if cur_leader == opp_sym else 0.0)
        if DAY_REARM_RESET_RSI_MIN <= leader_now_rsi <= DAY_REARM_RESET_RSI_MAX:
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = True

    # [핵심 리셋 규칙 2] 양방향 동시 대칭(<= 30.0 & 반대 >= 79.9) 출격 후
    # 직전 출격 종목(및 반대 종목)의 RSI가 '40.0 이상 70.0 이하' 중립 구간으로 복귀하면 리셋!
    if not bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True)):
        last_opta_s = str(_WHIPSAW_SIM_CACHE.get("day_opta_last_sym", ""))
        my_in_band = (DAY_REARM_RESET_RSI_MIN <= rsi_val <= DAY_REARM_RESET_RSI_MAX)
        opp_in_band = (opp_rsi_val <= 0.0) or (DAY_REARM_RESET_RSI_MIN <= opp_rsi_val <= DAY_REARM_RESET_RSI_MAX)
        if (not last_opta_s or symbol == last_opta_s) and my_in_band and opp_in_band:
            _WHIPSAW_SIM_CACHE["day_opta_rearm_ready"] = True


# Case 78: 03:58 EST 진입 또는 04:00 EST 프리장 전환 시 미결된 데이장 [모의] 포지션 자동 클리어
async def settle_daymarket_sim_before_premarket(
    ctx, symbol: str, now_est: datetime, current_price: float, silent: bool = True
):
    if (
        not _WHIPSAW_SIM_CACHE.get("day_sim_active", False)
        and not _WHIPSAW_SIM_CACHE.get("day_leader_sym")
        and not _WHIPSAW_SIM_CACHE.get("day_prelock_sym")
    ):
        return

    async with _DAY_SIM_LOCK:
        if not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            _reset_day_sim_state(reset_session_all=False)
            return

        sim_sym = str(_WHIPSAW_SIM_CACHE.get("day_sim_symbol", "SOXS"))
        phase = str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", ""))
        entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
        target_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0))
        min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
        max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
        entry_t = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))

        _reset_day_sim_position_only()

        cur_p_fl = math.floor(current_price * 100.0) / 100.0 if (symbol == sim_sym and current_price > 0.0) else entry_p
        print(
            f"🧹 [데이장 모의 자동마감 {sim_sym}] 03:58/04:00 전환 클리어 완료 (단계:{phase}, 진입:${entry_p:.2f}, 마감:${cur_p_fl:.2f})",
            flush=True,
        )

        if not silent and phase == "HOLDING" and entry_p > 0.0 and cur_p_fl > 0.0:
            gross_pct = (cur_p_fl / entry_p - 1.0) * 100.0
            net_pct = gross_pct - 0.14
            score_line = _build_day_sim_scoreboard_line()
            await ctx.notify_tg(
                f"🏁 <b>[데이장 모의 마감 정산] {sim_sym} 프리장 전환 인계 완료</b>\n"
                f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t} EST</code>) ➡️ 마감 <code>${cur_p_fl:.2f}</code> (목표/스탑 <code>${target_p:.2f}</code>)\n"
                f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code>\n"
                f"▫️ 마감 손익률: 세전 <code>{gross_pct:+.2f}%</code> / 세후 <code>{net_pct:+.2f}%</code> (수수료 0.14% 차감)\n"
                f"{score_line}\n"
                "▫️ 후속 조치: <code>데이장 모의 캐시를 정리하고 본진 프리장 듀얼 스나이퍼로 제어권을 이관합니다.</code>"
            )


# MODIFIED: 초과 Case 78 & Case 93 — 데이장 (19:00 ~ 03:57 EST, 개시 1분 전 18:59 EST 포함) 100% [모의] 전용 (실거래 매수 절대 차단!)
# 1) 데이장 시작 1분 전(08:59 KST / 18:59 EST) 나스닥 100 선물지수 10분봉 판별 ➔ 롱(🐂SOXL)/숏(🐻SOXS) 1차 락온
# 2) 데이장 시작 후 7분간(09:00~09:06:59 KST / 19:00~19:06:59 EST)에만 1차 락온 종목이 RSI(1m) >= 50.0 달성 시
#    상승하는 종목으로 최종 판별하여 바로 실시간 현재가로 [모의] 진입 (OPEN7M_TREND)
# 3) 개시 7분 이후에는 데이장 첫 RSI >= 79.9(80.0) 달성 상승장 종목(day_leader_sym)이 RSI <= 35.0으로 떨어졌을 때(PB35) 및
#    양방향 동시 대칭(<= 30.0 & 반대 >= 79.9, OPTA) 달성 시마다 출격하되, '40.0 <= RSI <= 70.0' 리셋 후 무한 반복 출격
# 4) +1.30% 상승가에서 락온되어 하이킨 아시 1분봉 음봉(🌑HA음봉) 및 최고가 -0.30% 하락 시 최소 +1.00% 익절 보장 트레일링 & 확률 집계
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
    is_day_or_prelock = (est_hm >= 1859 or est_hm <= 359)
    if not is_day_or_prelock:
        return

    # Case 78: 03:58 ~ 03:59 EST 데이장 마감 2분 전 구간은 세션 전환 휩소 구간이므로 100% 묵음 바이패스!
    if 358 <= est_hm <= 359:
        if _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            await settle_daymarket_sim_before_premarket(
                ctx=ctx, symbol=symbol, now_est=now_est, current_price=current_price, silent=True
            )
        return

    async with _DAY_SIM_LOCK:
        # Case 01 & 초과 Case 36: 18:59 / 19:00 EST 기준 논리적 거래일 산출 및 새 데이장 개장 시 모의 추적 상태 초기화
        logical_dt = (now_est + timedelta(days=1)) if now_est.hour >= 18 else now_est
        logical_date_str = logical_dt.strftime("%Y-%m-%d")

        if _WHIPSAW_SIM_CACHE.get("day_logical_date") != logical_date_str:
            _WHIPSAW_SIM_CACHE["day_logical_date"] = logical_date_str
            _reset_day_sim_state(reset_session_all=True)

        state_s = await AssassinLedger.get_state("SOXS")
        is_active = bool(state_s[5])
        if not is_active:
            return

        opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
        my_state = (ctx.shared_rsi_state or {}).get(symbol, {})
        opp_state = (ctx.shared_rsi_state or {}).get(opp_sym, {})
        opp_rsi_cached = float(opp_state.get("value", 0.0)) if isinstance(opp_state, dict) else 0.0

        # 데이장 개장(19:00 EST) 이후 첫 RSI >= 79.9(80.0) 상승장 종목 각인 및 '40.0 <= RSI <= 70.0' 리셋 동기화
        if est_hm >= 1900 or est_hm <= 357:
            _sync_daymarket_trend_leader_and_reset(
                ctx=ctx,
                symbol=symbol,
                rsi_val=rsi_val,
                opp_sym=opp_sym,
                opp_rsi_val=opp_rsi_cached,
                now_est=now_est,
            )

        is_prelock_1m, is_open_7m, win_tag, win_label = _get_day_opening_window_state(now_est)
        now_kst = now_est.astimezone(KST_TZ)

        # =====================================================================
        # [Step 0] 데이장 시작 1분 전 (08:59 KST 또는 18:59 EST) 나스닥 100 선물지수 10분봉 판별 & 롱/숏 1차 락온
        # =====================================================================
        if is_prelock_1m:
            if _WHIPSAW_SIM_CACHE.get("day_prelock_notified_tag") != win_tag:
                trend_dir, target_sym, vel_10m, open_10m, close_10m = await MacroDataCache.get_nq_10m_candle_trend(
                    force_refresh=True
                )
                _, nq_h_pre, nq_l_pre = await MacroDataCache.get_cached_nq_data()
                nq_amp_pre = (((nq_h_pre - nq_l_pre) / nq_l_pre * 100.0) if nq_l_pre > 0.0 else 0.0)

                _WHIPSAW_SIM_CACHE["day_prelock_sym"] = target_sym
                _WHIPSAW_SIM_CACHE["day_prelock_dir"] = trend_dir
                _WHIPSAW_SIM_CACHE["day_prelock_vel_10m"] = vel_10m
                _WHIPSAW_SIM_CACHE["day_prelock_open_10m"] = open_10m
                _WHIPSAW_SIM_CACHE["day_prelock_close_10m"] = close_10m
                _WHIPSAW_SIM_CACHE["day_prelock_window_tag"] = win_tag
                _WHIPSAW_SIM_CACHE["day_prelock_notified_tag"] = win_tag

                t_icon = "🐂" if target_sym == "SOXL" else "🐻"
                dir_kor = "상승(양봉) ➡️ 롱(🐂SOXL)" if trend_dir == "LONG" else "하락(음봉) ➡️ 숏(🐻SOXS)"
                await ctx.notify_tg(
                    f"🔍 <b>[데이장 개시 1분 전 · NQ 10분봉 1차 락온] {t_icon}{target_sym} 진입 후보 조준 완료 (매수차단)</b>\n"
                    f"▫️ 기준 시각: <code>{now_est.strftime('%H:%M:%S')} EST ({now_kst.strftime('%H:%M:%S')} KST · {win_label} 1분 전)</code>\n"
                    f"▫️ NQ=F 10분봉: 시가 <code>{open_10m:.2f}</code> ➡️ 종가 <code>{close_10m:.2f}</code> (<code>{vel_10m:+.2f}%</code>) | 판정: <b>{dir_kor}</b>\n"
                    f"▫️ 당일 NQ 진폭: <code>{nq_amp_pre:.2f}%</code>\n"
                    f"▫️ 최종 판별 조건: <code>데이장 개시 직후 7분간({win_label}) {t_icon}{target_sym} RSI(1m) ≥ 50.0 달성 시 상승종목으로 최종 확정하여 현재가 즉시 [모의] 진입!</code>"
                )
            return

        # =====================================================================
        # [상태 1] 데이장 100% [모의] 포지션 활성 상태 (TRAP_WAIT 또는 HOLDING)
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
            trig_mode_saved = str(_WHIPSAW_SIM_CACHE.get("day_sim_trig_mode", "OPTA"))
            nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_cached_nq_data()
            nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
            sym_icon = "🐻" if active_sim_sym == "SOXS" else "🐂"

            # -----------------------------------------------------------------
            # [단계 1-A] TRAP_WAIT : 개시 7분 이후 대칭(OPTA) 또는 상승장 종목 35눌림(PB35) 1차 매수 덫 & 30초 타임쉴드
            # -----------------------------------------------------------------
            if phase == "TRAP_WAIT":
                trap_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_trap_price", 0.0))
                spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", cur_p_fl))
                start_ts = float(_WHIPSAW_SIM_CACHE.get("day_sim_start_ts", 0.0))
                elapsed = max(0.0, time.time() - start_ts) if start_ts > 0.0 else 0.0
                shield_rsi_cap = (
                    PRE_TREND_PULLBACK_RSI_THRESHOLD
                    if trig_mode_saved == "PB35"
                    else PRE_SNIPER_RSI_THRESHOLD
                )

                # 매크로 1차 방어선: 1차 매수 덫 대기 중 NQ=F 당일 진폭 2.0% 이상 돌파 시 모의 덫 즉시 파기
                if nq_amp_now >= 2.0:
                    _reset_day_sim_position_only()
                    await ctx.notify_tg(
                        f"🛑 <b>[데이장 모의 1차 매수덫 취소] {sym_icon}{active_sim_sym} NQ=F 과열 감지</b>\n"
                        f"▫️ 사유: 1차 매수 덫(<code>${trap_p:.2f}</code>) 대기 중 NQ=F 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 돌파)\n"
                        "▫️ 조치: 모의 매수 덫 즉시 파기 및 알림 락 리셋 완료"
                    )
                    return

                filled_now = False
                confirmed_entry = 0.0
                fill_type_str = ""

                if trap_p > 0.0 and cur_p_fl <= trap_p:
                    filled_now = True
                    confirmed_entry = trap_p
                    fill_type_str = f"🪤 1차 슬리피지 덫 저가체결(포착 ${spot_p:.2f} ➔ 체결 ${trap_p:.2f})"
                elif rsi_val > shield_rsi_cap:
                    filled_now = True
                    confirmed_entry = cur_p_fl
                    fill_type_str = f"⚡ RSI {shield_rsi_cap:.0f}초과 반등 즉각체결(RSI {rsi_val:.1f} · 경과 {elapsed:.1f}초)"
                elif elapsed >= BUY_TRAP_REARM_WAIT_SEC:
                    filled_now = True
                    confirmed_entry = cur_p_fl
                    fill_type_str = f"🔄 30초 타임쉴드 만료 현재가체결(RSI {rsi_val:.1f} · 30.0초 경과)"

                if filled_now and confirmed_entry > 0.0:
                    ref_p_fill, ref_src_fill = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                    if ref_p_fill <= 0.0:
                        ref_p_fill = nq_c_now
                        ref_src_fill = "NQ선물"

                    new_floor_100 = math.ceil(confirmed_entry * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
                    new_lock_130 = math.ceil(confirmed_entry * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                    _WHIPSAW_SIM_CACHE["day_sim_phase"] = "HOLDING"
                    _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = confirmed_entry
                    _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = fill_type_str
                    _WHIPSAW_SIM_CACHE["day_sim_floor_price"] = new_floor_100
                    _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = new_lock_130
                    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = new_lock_130
                    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
                    _WHIPSAW_SIM_CACHE["day_sim_min_price"] = min(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = max(confirmed_entry, cur_p_fl)
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_p_fill
                    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_now
                    _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_fill
                return

            # -----------------------------------------------------------------
            # [단계 1-B] HOLDING : +1.30% 상승가 락온 × 최고가 -0.30% 하락 & 🌑HA음봉 시 최소 +1.00% 익절 보장 고가 추적 트레일링!
            # -----------------------------------------------------------------
            sym_state_d = (ctx.shared_rsi_state or {}).get(active_sim_sym, {})
            ewm_d = sym_state_d.get("ewm_state", {}) if isinstance(sym_state_d, dict) else {}
            is_ha_bull, live_ha_o, live_ha_c = compute_realtime_tick_heikin_ashi(cur_p_fl, ewm_d)

            prev_min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
            if prev_min_p <= 0.0 or cur_p_fl < prev_min_p:
                _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl

            prev_max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
            if prev_max_p <= 0.0 or cur_p_fl > prev_max_p:
                _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl

            entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
            floor_100_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_floor_price", 0.0)) or (
                math.ceil(entry_p * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
            )
            lock_130_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_arm_price", 0.0)) or (
                math.ceil(entry_p * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
            )
            spot_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_spot_price", entry_p))
            fill_type_s = str(_WHIPSAW_SIM_CACHE.get("day_sim_fill_type", "모의체결"))
            min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", cur_p_fl))
            max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", cur_p_fl))
            entry_t_str = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))
            buy_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_buy_rsi", 0.0))
            opp_rsi_s = float(_WHIPSAW_SIM_CACHE.get("day_sim_opp_rsi", 0.0))
            nq_entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_price", 0.0))
            nq_entry_a = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0))

            is_nq_favorable_surge, nq_vel_10m, _ = await MacroDataCache.is_nq_velocity_favorable_surge(
                symbol=active_sim_sym, surge_threshold=DAY_SIM_NQ_VEL_SURGE_PCT
            )
            vel_src_lbl = MacroDataCache.get_cached_nq_velocity_source()
            _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = bool(is_nq_favorable_surge)

            # 1) +1.30% 상승가(lock_130_p) 도달 시 트레일링 락온! (최고가 -0.30% 하락 시 최소 +1.00% 익절 보장!)
            if lock_130_p > 0.0 and cur_p_fl >= lock_130_p:
                if not _WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False):
                    _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = True
                    _WHIPSAW_SIM_CACHE["day_sim_armed_ts"] = time.time()
                    _WHIPSAW_SIM_CACHE["day_sim_armed_hm"] = est_hm
                    _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = bool(not is_ha_bull and not is_nq_favorable_surge)
                    peak_init = max(max_p, cur_p_fl, lock_130_p)
                    _WHIPSAW_SIM_CACHE["day_sim_max_price"] = peak_init
                    init_stop_p = max(
                        floor_100_p,
                        math.floor(peak_init * (1.0 - DAY_SIM_TRAIL_DROP_PCT / 100.0) * 100.0) / 100.0,
                    )
                    _WHIPSAW_SIM_CACHE["day_sim_target_price"] = init_stop_p
                    cur_gain_pct = ((cur_p_fl / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.30
                    ha_lbl = "🕯️HA양봉 지속" if is_ha_bull else "🌑HA음봉 감지"
                    await ctx.notify_tg(
                        f"🔥 <b>[데이장 모의 +1.30% 상승가 락온] {sym_icon}{active_sim_sym} 최소 +1.00% 익절 보장 &amp; 고가 추적 개시 (매수차단)</b>\n"
                        f"▫️ 모의 체결가: <code>${entry_p:.2f}</code> ➡️ 현재가: <code>${cur_p_fl:.2f}</code> (<code>+{cur_gain_pct:.2f}%</code> 돌파!)\n"
                        f"▫️ 최소 보장 익절선(+1.00%): <code>${floor_100_p:.2f}</code> | 현재 스탑선(고점 -0.30%): <code>${init_stop_p:.2f}</code>\n"
                        f"▫️ 실시간 HA(1m): <code>{ha_lbl}</code> | 추적 규칙: <code>HA 1분봉 양봉 따라 최고가 갱신 ➡️ HA음봉 출현 및 최고가 대비 -0.30% 하락 시 최소 +1.00% 이상 모의 익절 확정</code>"
                    )
                    return

            if _WHIPSAW_SIM_CACHE.get("day_sim_trail_armed", False):
                peak_p = max(float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", lock_130_p)), lock_130_p)
                peak_pct = ((peak_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.30
                elapsed_arm = time.time() - float(_WHIPSAW_SIM_CACHE.get("day_sim_armed_ts", 0.0))
                armed_hm = int(_WHIPSAW_SIM_CACHE.get("day_sim_armed_hm", est_hm))

                if (not is_ha_bull) and (not is_nq_favorable_surge):
                    _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = True

                trail_stop_p = math.floor(peak_p * (1.0 - DAY_SIM_TRAIL_DROP_PCT / 100.0) * 100.0) / 100.0
                effective_stop_p = max(floor_100_p, trail_stop_p)
                _WHIPSAW_SIM_CACHE["day_sim_target_price"] = effective_stop_p

                ha_bear_seen = bool(_WHIPSAW_SIM_CACHE.get("day_sim_ha_bear_seen", False))
                # ① 하이킨 아시 1분봉 음봉(🌑HA음봉) 출현 후 최고가 대비 -0.30% 하락(effective_stop_p 도달) 또는
                # ② 락온 이후 최고가 대비 -0.30% 스탑선(effective_stop_p) 도달 시 최소 +1.00%(floor_100_p) 보장 익절!
                is_ha_and_cb_exit = ha_bear_seen and (cur_p_fl <= effective_stop_p)
                is_direct_cb_exit = (
                    (est_hm != armed_hm or elapsed_arm >= 4.0 or peak_pct >= 1.45)
                    and (cur_p_fl <= effective_stop_p)
                )

                if is_ha_and_cb_exit or is_direct_cb_exit:
                    sim_exit_p = max(floor_100_p, effective_stop_p)
                    sim_gross_pct = ((sim_exit_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.00
                    sim_net_pct = sim_gross_pct - 0.14
                    max_dd_pct = ((min_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0

                    _record_day_sim_result(trig_mode=trig_mode_saved, is_win=True, net_pct=sim_net_pct)
                    _reset_day_sim_position_only()

                    if trig_mode_saved == "OPEN7M_TREND":
                        trig_badge = "🚀개시7분 추세추종(NQ10분봉+RSI≥50)"
                    elif trig_mode_saved == "PB35":
                        trig_badge = "👑상승장종목 추세눌림(RSI≤35.0)"
                    else:
                        trig_badge = "🪤양방향 대칭 클라이맥스(≤30&≥79.9)"

                    ha_exit_note = (
                        f"🌑HA음봉 확인(HA시가 ${live_ha_o:.2f} ≥ HA종가 ${live_ha_c:.2f}) &amp; "
                        if ha_bear_seen
                        else ""
                    )
                    exit_trig_str = (
                        f"{ha_exit_note}최고가(${peak_p:.2f} / +{peak_pct:.2f}%) 대비 "
                        f"-{DAY_SIM_TRAIL_DROP_PCT:.2f}% 스탑선(${effective_stop_p:.2f} · 최소 +1.00% 보장) 도달"
                    )
                    score_line = _build_day_sim_scoreboard_line()

                    await ctx.notify_tg(
                        f"🎯 <b>[데이장 모의 고가 추적 트레일링 익절] {sym_icon}{active_sim_sym} +{sim_gross_pct:.2f}% 수확 (매수차단)</b>\n"
                        f"▫️ 출격 유형: <code>{trig_badge} · {fill_type_s}</code>\n"
                        f"▫️ 익절 격발: <code>{exit_trig_str}</code>\n"
                        f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code> · RSI <code>{buy_rsi_s:.1f}</code>) ➡️ 익절 <code>${sim_exit_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> (<code>{max_dd_pct:+.2f}%</code>) ~ 최고 <code>${peak_p:.2f}</code> (<code>+{peak_pct:.2f}%</code>)\n"
                        f"▫️ 모의 수익률: 세전 <code>+{sim_gross_pct:.2f}%</code> / 세후 순수익 <code>+{sim_net_pct:.2f}%</code>\n"
                        f"{score_line}\n"
                        f"▫️ 리셋 안내: <code>RSI 40.0 ~ 70.0 중립 복귀 시 차기 조건 달성마다 무제한 반복 출격합니다.</code>"
                    )
                return

            # 2) +1.30% 락온 전 손실권(0 < cur_p_fl < entry_p)에서 2차 과열(NQ >= 2.0%) 또는 3차 실시간 QQQ 역행 이탈 시 모의 손절 정산
            if entry_p > 0.0 and 0.0 < cur_p_fl < entry_p:
                dyn_thresh = max(
                    nq_entry_a * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                ref_c_now, ref_src_now = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)

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
                    try:
                        ob = await ctx.client.get_orderbook(active_sim_sym)
                        bids = ob.get("bids", [])
                        bid_1_p = float(bids[0]["price"]) if bids and float(bids[0]["price"]) > 0.0 else cur_p_fl
                    except Exception:
                        bid_1_p = cur_p_fl

                    loss_gross_pct = ((bid_1_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                    loss_net_pct = loss_gross_pct - 0.14

                    _record_day_sim_result(trig_mode=trig_mode_saved, is_win=False, net_pct=loss_net_pct)
                    _reset_day_sim_position_only()

                    ref_unit = "$" if ref_src_now == "QQQ실시간" else ""
                    if is_macro_2nd:
                        macro_reason = f"NQ=F 당일 진폭 <code>{nq_amp_now:.2f}%</code> (2.0% 이상 대세 과열 돌파)"
                    else:
                        macro_reason = (
                            f"3차 {ref_src_now} 기준(<code>{ref_unit}{nq_entry_p:.2f}</code> ➡️ <code>{ref_unit}{ref_c_now:.2f}</code>) "
                            f"대비 <code>+{nq_adverse_pct:.2f}%</code> {adverse_dir_label} "
                            f"(NQ=F 진폭 <code>{nq_entry_a:.2f}%</code> 기반 3차 한도 <code>+{dyn_thresh:.2f}%</code> 이탈)"
                        )
                    score_line = _build_day_sim_scoreboard_line()

                    await ctx.notify_tg(
                        f"🚨 <b>[데이장 모의 매크로 손절] {sym_icon}{active_sim_sym} 방어선 작동 ➡️ 차기 감시 복귀 (매수차단)</b>\n"
                        f"▫️ 사유: {macro_reason}\n"
                        f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code>) ➡️ 매수1호가 손절 <code>${bid_1_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code>\n"
                        f"▫️ 모의 손익률: 세전 <code>{loss_gross_pct:+.2f}%</code> / 세후 <code>{loss_net_pct:+.2f}%</code> (수수료 0.14% 포함)\n"
                        f"{score_line}\n"
                        "▫️ 후속 조치: <code>RSI 40.0 ~ 70.0 중립 복귀 후 조건 재달성 시 차기 [모의] 스나이퍼가 출동합니다.</code>"
                    )
            return

        # =====================================================================
        # [상태 2] 미진입 대기 상태(day_sim_active == False)
        # =====================================================================
        if not (est_hm >= 1900 or est_hm <= 357):
            return

        nq_c_global, nq_h_global, nq_l_global = await MacroDataCache.get_cached_nq_data()
        nq_amp_global = (
            ((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0
        )
        if nq_amp_global >= 2.0:
            return

        # -----------------------------------------------------------------
        # [제1막 : 데이장 개시 직후 7분간 (09:00:00 ~ 09:06:59 KST / 19:00:00 ~ 19:06:59 EST)]
        # 개시 1분 전 NQ=F 10분봉으로 1차 락온된 종목이 'RSI(1m) >= 50.0' 일 때만
        # 상승하는 종목으로 최종 판별하여 바로 실시간 현재가로 [모의] 진입! (반대편 역추세 진입은 100% 차단)
        # -----------------------------------------------------------------
        if is_open_7m:
            # 만약 봇이 개시 직후 재기동되어 1분 전 1차 락온이 비어 있다면 즉시 NQ 10분봉으로 자동 복원
            if _WHIPSAW_SIM_CACHE.get("day_prelock_window_tag") != win_tag or not _WHIPSAW_SIM_CACHE.get("day_prelock_sym"):
                trend_dir, target_sym, vel_10m, open_10m, close_10m = await MacroDataCache.get_nq_10m_candle_trend(
                    force_refresh=True
                )
                _WHIPSAW_SIM_CACHE["day_prelock_sym"] = target_sym
                _WHIPSAW_SIM_CACHE["day_prelock_dir"] = trend_dir
                _WHIPSAW_SIM_CACHE["day_prelock_vel_10m"] = vel_10m
                _WHIPSAW_SIM_CACHE["day_prelock_open_10m"] = open_10m
                _WHIPSAW_SIM_CACHE["day_prelock_close_10m"] = close_10m
                _WHIPSAW_SIM_CACHE["day_prelock_window_tag"] = win_tag

            prelock_sym = str(_WHIPSAW_SIM_CACHE.get("day_prelock_sym", ""))
            already_fired_7m = (_WHIPSAW_SIM_CACHE.get("day_open7m_fired_tag") == win_tag)

            if (
                not already_fired_7m
                and symbol == prelock_sym
                and rsi_val >= DAY_OPEN_7M_RSI_THRESHOLD
            ):
                if current_price <= 0.0:
                    try:
                        current_price = await ctx.client.get_current_price(symbol)
                    except Exception:
                        current_price = 0.0
                cur_p_fl = math.floor(current_price * 100.0) / 100.0
                if cur_p_fl <= 0.0:
                    return

                ref_entry_p, ref_src_init = await MacroDataCache.get_stage3_reference_price(realtime_probing=True)
                if ref_entry_p <= 0.0:
                    ref_entry_p = nq_c_global
                    ref_src_init = "NQ선물"

                floor_100_p = math.ceil(cur_p_fl * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
                lock_130_p = math.ceil(cur_p_fl * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
                pre_vel_10m = float(_WHIPSAW_SIM_CACHE.get("day_prelock_vel_10m", 0.0))
                pre_dir = str(_WHIPSAW_SIM_CACHE.get("day_prelock_dir", ""))

                _WHIPSAW_SIM_CACHE["day_open7m_confirmed_sym"] = symbol
                _WHIPSAW_SIM_CACHE["day_open7m_fired_tag"] = win_tag
                _WHIPSAW_SIM_CACHE["day_sim_active"] = True
                _WHIPSAW_SIM_CACHE["day_sim_symbol"] = symbol
                _WHIPSAW_SIM_CACHE["day_sim_trig_mode"] = "OPEN7M_TREND"
                _WHIPSAW_SIM_CACHE["day_sim_exit_regime"] = "LOCK_130_CB_030"
                _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = False
                _WHIPSAW_SIM_CACHE["day_sim_phase"] = "HOLDING"
                _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = cur_p_fl
                _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = cur_p_fl
                _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = cur_p_fl
                _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = f"🚀 개시7분 추세추종 현재가 즉시진입(NQ10분봉 {pre_dir} & RSI {rsi_val:.1f}≥50)"
                _WHIPSAW_SIM_CACHE["day_sim_floor_price"] = floor_100_p
                _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = lock_130_p
                _WHIPSAW_SIM_CACHE["day_sim_target_price"] = lock_130_p
                _WHIPSAW_SIM_CACHE["day_sim_trail_armed"] = False
                _WHIPSAW_SIM_CACHE["day_sim_nq_vel_bypass"] = False
                _WHIPSAW_SIM_CACHE["day_sim_min_price"] = cur_p_fl
                _WHIPSAW_SIM_CACHE["day_sim_max_price"] = cur_p_fl
                _WHIPSAW_SIM_CACHE["day_sim_start_ts"] = time.time()
                _WHIPSAW_SIM_CACHE["day_sim_entry_time_str"] = now_est.strftime("%H:%M:%S")
                _WHIPSAW_SIM_CACHE["day_sim_buy_rsi"] = rsi_val
                _WHIPSAW_SIM_CACHE["day_sim_opp_rsi"] = opp_rsi_cached
                _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = ref_entry_p
                _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_global
                _WHIPSAW_SIM_CACHE["day_sim_ref_src"] = ref_src_init

                sym_icon = "🐻" if symbol == "SOXS" else "🐂"
                ref_unit = "$" if ref_src_init == "QQQ실시간" else ""
                dyn_th_init = max(
                    nq_amp_global * 0.5,
                    float(ctx.nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT),
                    MIN_STAGE3_QQQ_ADVERSE_PCT,
                )
                await ctx.notify_tg(
                    f"🚀 <b>[데이장 개시 7분 추세추종 모의 진입] {sym_icon}{symbol} 상승종목 최종 판별 ➡️ 현재가 즉시 진입 (매수차단)</b>\n"
                    f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST ({now_kst.strftime('%H:%M:%S')} KST · {win_label})</code>\n"
                    f"▫️ 최종 판별: 개시 1분 전 NQ 10분봉 <code>{pre_dir}({pre_vel_10m:+.2f}%)</code> 1차 락온 + 개시 7분 내 {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (≥50.0 상승 확증!)\n"
                    f"▫️ 모의 진입가(현재가): <code>${cur_p_fl:.2f}</code> | +1.30% 락온 기준가: <code>${lock_130_p:.2f}</code> (최소 +1.00% 보장선 <code>${floor_100_p:.2f}</code>)\n"
                    f"▫️ 매크로 기준: 3차 방어({ref_src_init}) <code>{ref_unit}{ref_entry_p:.2f}</code> | NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> (3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
                    "▫️ 고가 추적 규칙: <code>+1.30% 상승가에서 락온되어 HA 1분봉 음봉 출현 및 최고가 -0.30% 하락 시 최소 +1.00% 익절 보장</code>"
                )
            # 개시 후 7분 동안은 폭락 칼날 피격을 막기 위해 반대편 과매도 역추세 진입을 100% 차단!
            return

        # -----------------------------------------------------------------
        # [제2막 : 개시 7분 이후 (09:07:00 KST ~ / 19:07:00 ~ 03:57:59 EST)]
        # ① 양방향 동시 대칭(RSI <= 30.0 & 반대 >= 79.9) 또는
        # ② 데이장 첫 RSI >= 79.9(80.0) 달성 상승장 종목(day_leader_sym)의 RSI <= 35.0 눌림목
        # 발생할 때마다 출격 (단, 1회 달성 후 '40.0 <= RSI <= 70.0' 리셋 완료 상태일 때!)
        # -----------------------------------------------------------------
        day_leader_sym = str(_WHIPSAW_SIM_CACHE.get("day_leader_sym", ""))
        day_pb_ready = bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True))
        day_opta_ready = bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True))

        is_cand_pb35 = (
            day_leader_sym == symbol
            and day_pb_ready
            and (0.0 < rsi_val <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
        )
        is_cand_opta = (
            day_opta_ready
            and (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
        )

        if not (is_cand_opta or is_cand_pb35):
            return

        opp_ewm = opp_state.get("ewm_state", {}) if isinstance(opp_state, dict) else {}
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
                fallback_rsi=opp_rsi_cached,
            )
            if isinstance(opp_state, dict):
                opp_state["value"] = opp_live_rsi
            _sync_daymarket_trend_leader_and_reset(
                ctx=ctx,
                symbol=symbol,
                rsi_val=rsi_val,
                opp_sym=opp_sym,
                opp_rsi_val=opp_live_rsi,
                now_est=now_est,
            )
        else:
            opp_live_rsi = opp_rsi_cached

        is_opta_valid = (
            bool(_WHIPSAW_SIM_CACHE.get("day_opta_rearm_ready", True))
            and (0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD)
            and (opp_live_rsi >= OPP_CLIMAX_RSI_THRESHOLD)
        )
        is_pb35_valid = (
            str(_WHIPSAW_SIM_CACHE.get("day_leader_sym", "")) == symbol
            and bool(_WHIPSAW_SIM_CACHE.get("day_pb_rearm_ready", True))
            and (0.0 < rsi_val <= PRE_TREND_PULLBACK_RSI_THRESHOLD)
        )

        if not (is_opta_valid or is_pb35_valid):
            return

        cur_p_fl = math.floor(current_price * 100.0) / 100.0
        if cur_p_fl <= 0.0:
            return

        trig_mode_now = "OPTA" if is_opta_valid else "PB35"
        if trig_mode_now == "OPTA":
            _WHIPSAW_SIM_CACHE["day_opta_rearm_ready"] = False
            _WHIPSAW_SIM_CACHE["day_opta_last_sym"] = symbol
        else:
            _WHIPSAW_SIM_CACHE["day_pb_rearm_ready"] = False

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

        slip_disc = max(BUY_TRAP_MIN_DISC_USD, round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2))
        sim_trap_buy = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)
        sim_floor_100 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_FLOOR_LOCK_RATE / 100.0) * 100.0) / 100.0
        sim_lock_130 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0
        trap_lock_130 = math.ceil(sim_trap_buy * (1.0 + DAY_SIM_ARM_PROFIT_RATE / 100.0) * 100.0) / 100.0

        _WHIPSAW_SIM_CACHE["day_sim_active"] = True
        _WHIPSAW_SIM_CACHE["day_sim_symbol"] = symbol
        _WHIPSAW_SIM_CACHE["day_sim_trig_mode"] = trig_mode_now
        _WHIPSAW_SIM_CACHE["day_sim_exit_regime"] = "LOCK_130_CB_030"
        _WHIPSAW_SIM_CACHE["day_sim_ha_bear_seen"] = False
        _WHIPSAW_SIM_CACHE["day_sim_phase"] = "TRAP_WAIT"
        _WHIPSAW_SIM_CACHE["day_sim_spot_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_trap_price"] = sim_trap_buy
        _WHIPSAW_SIM_CACHE["day_sim_entry_price"] = cur_p_fl
        _WHIPSAW_SIM_CACHE["day_sim_fill_type"] = "1차 슬리피지 매수덫 대기중"
        _WHIPSAW_SIM_CACHE["day_sim_floor_price"] = sim_floor_100
        _WHIPSAW_SIM_CACHE["day_sim_arm_price"] = sim_lock_130
        _WHIPSAW_SIM_CACHE["day_sim_target_price"] = sim_lock_130
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

        if trig_mode_now == "PB35":
            ld_pk_rsi = float(_WHIPSAW_SIM_CACHE.get("day_leader_rsi", 79.9))
            ld_t_str = str(_WHIPSAW_SIM_CACHE.get("day_leader_time_str", ""))
            trig_header_str = f"상승장 락온 종목({sym_icon}{symbol}) RSI≤35.0 눌림목 포착"
            trig_detail_str = (
                f"▫️ 상승장 눌림목: 데이장 첫 RSI≥80 달성 {sym_icon}{symbol} (<code>{ld_t_str} EST</code> 피크 <code>{ld_pk_rsi:.1f}</code>) ➡️ "
                f"현재 RSI <code>{rsi_val:.1f}</code> (≤35.0 눌림목 · 반대 {opp_sym} <code>{opp_live_rsi:.1f}</code>)\n"
            )
            shield_cap_num = 35
        else:
            trig_header_str = "양방향 대칭 클라이맥스 포착"
            trig_detail_str = (
                f"▫️ 대칭 클라이맥스: {sym_icon}{symbol} RSI <code>{rsi_val:.1f}</code> (기준 ≤ 30.0) &amp; "
                f"{opp_icon}{opp_sym} RSI <code>{opp_live_rsi:.1f}</code> (기준 ≥ 79.9[80인지])\n"
            )
            shield_cap_num = 30

        score_line = _build_day_sim_scoreboard_line()
        await ctx.notify_tg(
            f"🔔 <b>[데이장 모의 스나이퍼 출격] {sym_icon}{symbol} {trig_header_str} ➡️ +1.30% 락온 고가 추적 개시 (매수차단)</b>\n"
            f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST ({now_kst.strftime('%H:%M:%S')} KST)</code>\n"
            f"{trig_detail_str}"
            f"▫️ 포착 현재가: <code>${cur_p_fl:.2f}</code> | 🪤 1차 슬리피지 덫: <code>${sim_trap_buy:.2f}</code> (-0.18%) | VWAP: <code>{vwap_str}</code>\n"
            f"▫️ 30초 타임쉴드: <code>30초 내 ${sim_trap_buy:.2f} 도달 시 저가체결(락온가 ${trap_lock_130:.2f}) / RSI {shield_cap_num}초과 반등 또는 30초 만료 시 현재가체결</code>\n"
            f"▫️ 매크로 기준: 3차 방어({ref_src_init}) <code>{ref_unit}{ref_entry_p:.2f}</code> | NQ=F 진폭 <code>{nq_amp_global:.2f}%</code> (3차 한도 <code>+{dyn_th_init:.2f}%</code>)\n"
            f"▫️ 고가 추적 규칙: <code>+1.30% 상승가(${sim_lock_130:.2f}) 락온 ➔ HA 1분봉 음봉 &amp; 최고가 -0.30% 하락 시 최소 +1.00%(${sim_floor_100:.2f}) 익절 보장 · 40≤RSI≤70 복귀 시 리셋</code>\n"
            f"{score_line}"
        )
