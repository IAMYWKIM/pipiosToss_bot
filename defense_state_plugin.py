# =====================================================================
# FILE: defense_state_plugin.py
# 목적: 매크로 방어·이원화 트레일링·최저점 반등·프리장 상승 주도주 인메모리 상태 캐시 및
#       2단계 미체결 주문 선취소/수량 해방 폴링 헬퍼 플러그인 (defense_plugin.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 defense_plugin.py의 5대 인메모리 캐시·상승주도주 각인·2단계 취소 헬퍼를 무손실 분리
# NEW: 초과 Case 85 & Case 89 & Case 99 — 이원화 하이킨 아시 트레일링 런타임 캐시(_TRAIL_RUNTIME_CACHE) 및 NQ_VELOCITY_SURGE_PCT(0.20%) 슬롯 관리
# NEW: 초과 Case 87 — 04:07 ~ 08:14 EST 중 NQ<2.0% 내 3차 역행 손절 시 당일 퇴근 대신 최저점 역트레일링(_BOTTOM_TRACK_CACHE) 점화
# NEW: 초과 Case 94 — 스나이퍼 OFF->ON 재가동 또는 지난주 만료 주문(CANCELED) 감지 시 현재 실시간 QQQ 가격으로 0.00% 영점 재락온(_MANUAL_STAGE3_ANCHOR_CACHE)
# NEW: 초과 Case 97 — 04:07 EST 이후 첫 RSI >= 79.9 달성 상승 추세 종목(_PRE_LEADER_CACHE · FIRST_799) 불변 각인·복원

import math
import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
)

EST_TZ = ZoneInfo("America/New_York")

# 이원화 하이킨 아시 + 고점 트레일링(⚡ OLD_IMM_CB × 🚀 PEAK_LCK_130 / 📊 BAR_BEAR_020 × 🚀 NQ_VEL_BYPASS) 상수 동기화 (Case 85, 89, 91, 99)
TRAIL_ARM_PROFIT_RATE = AssassinLedger.TRAIL_ARM_PROFIT_RATE          # +1.00% 도달 시 트레일링 점화
TRAIL_FLOOR_LOCK_RATE = AssassinLedger.TRAIL_FLOOR_LOCK_RATE          # 기본 최소 +1.00% 익절 보존선 하드 락온
TRAIL_DROP_CALLBACK_PCT = AssassinLedger.TRAIL_DROP_CALLBACK_PCT      # 기술적 반등(PRE_SNIPER) 최고가 대비 -0.30% 콜백 스탑
TRAIL_SAFETY_CAP_RATE = AssassinLedger.TRAIL_SAFETY_CAP_RATE          # 체결 직후 거래소 선장전 안전 캡 (+5.00% LIMIT)
NQ_VELOCITY_SURGE_PCT = AssassinLedger.NQ_VELOCITY_SURGE_PCT          # QQQ/NQ 10분/3분 변화율 ±0.20% HA음봉 바이패스 임계치

# 초과 Case 87 — 3차 안전망 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 상수
BOTTOM_REBOUND_PCT = 0.30          # 최저점(post_stop_low) 대비 +0.30% 이상 양봉 반등 시 재진입 요격
BOTTOM_MAX_RSI_CAP = 38.0          # 최저점 반등 진입 허용 최대 RSI 상한선 (과매도 반등권 확인)

# 스나이퍼 무음 불발 및 매크로 1차 과열 차단 알림 쿨다운 캐시 (Case 74, 96)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_macro1_date": "",
    "last_macro1_cancel_id": "",
    "last_err_msg": "",
}

# 자동 진입 세션 태그 화이트리스트 (수동 MANUAL 오분류 방어용)
_AUTO_ENTRY_SESSION_TAGS = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)


def _make_empty_trail_slot() -> dict:
    return {
        "armed": False,
        "armed_ts": 0.0,
        "armed_hm": 0,
        "exit_regime": "OLD_IMM_CB",
        "bear_candle_seen": False,
        "candle_bull": True,
        "active_callback_pct": TRAIL_DROP_CALLBACK_PCT,
        "avg_price": 0.0,
        "arm_price": 0.0,
        "floor_price": 0.0,
        "safety_cap_price": 0.0,
        "peak_price": 0.0,
        "stop_price": 0.0,
        "ha_bull": True,
        "ha_open": 0.0,
        "ha_close": 0.0,
        "nq_vel_bypass": False,
        "nq_vel_10m": 0.0,
        "nq_vel_3m": 0.0,
        "sweep_fired": False,
        "last_sweep_ts": 0.0,
    }


# 인메모리 하이킨 아시 트레일링 런타임 상태 캐시 (Case 02, 85, 89, 99)
_TRAIL_RUNTIME_CACHE = {
    "SOXS": _make_empty_trail_slot(),
    "SOXL": _make_empty_trail_slot(),
}

# 초과 Case 94 — 수동 보유(MANUAL) 물량의 스나이퍼 OFF->ON 재가동 및 지난주 만료 주문 감지 시
# 현재 실시간 QQQ 가격으로 0.00% 영점 재락온을 보장하기 위한 인메모리 앵커 검증 캐시
_MANUAL_STAGE3_ANCHOR_CACHE = {
    "SOXS": {"anchored": False, "verified_sell_id": ""},
    "SOXL": {"anchored": False, "verified_sell_id": ""},
}

# 인메모리 3차 손절 후 최저점 역트레일링(RE_BOT_TRAIL) 추적 캐시 (Case 02, 87)
_BOTTOM_TRACK_CACHE = {
    "active": False,
    "mode": "STAGE3_STOP",
    "symbol": "",
    "date": "",
    "last_stop_price": 0.0,
    "post_stop_low": 0.0,
    "rebound_trig_price": 0.0,
    "dipped_below_stop": False,
    "climax_seen": True,
    "activated_ts": 0.0,
    "last_new_low_ts": 0.0,
}

# 초과 Case 97 — 선택지 B (👑 HYB_35_1ST) : 04:07 EST 이후 가장 먼저 RSI >= 79.9 를 달성한
# 당일 첫 상승 추세 종목(FIRST_799 · leader_sym) 인메모리 추세 원장 캐시
_PRE_LEADER_CACHE = {
    "date": "",
    "leader_sym": "",
    "leader_rsi": 0.0,
    "locked_ts": 0.0,
    "locked_time_str": "",
    "pb_rearm_ready": True,
}


def _reset_pre_leader_state(logical_date: str = ""):
    """프리장 상승 추세 주도주 캐시(_PRE_LEADER_CACHE)를 원자적으로 0점 초기화합니다 (Case 02, 22, 48, 97)."""
    _PRE_LEADER_CACHE["date"] = str(logical_date or "")
    _PRE_LEADER_CACHE["leader_sym"] = ""
    _PRE_LEADER_CACHE["leader_rsi"] = 0.0
    _PRE_LEADER_CACHE["locked_ts"] = 0.0
    _PRE_LEADER_CACHE["locked_time_str"] = ""
    _PRE_LEADER_CACHE["pb_rearm_ready"] = True


def mark_pre_pullback_dispatched(symbol: str):
    """07:00 ~ 08:14 EST 선행 상승종목 추세눌림목(RSI <= 35.0) 매수 덫 발사 시 동일 눌림목 중복 진입을 잠급니다 (초과 Case 97)."""
    if _PRE_LEADER_CACHE.get("leader_sym") == symbol:
        _PRE_LEADER_CACHE["pb_rearm_ready"] = False


def get_pre_leader_status() -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 메인 루프에서 당일 프리장 첫 상승 추세 종목 상태를 조회합니다 (초과 Case 97)."""
    return dict(_PRE_LEADER_CACHE)


def update_premarket_trend_leader(
    symbol: str,
    live_rsi_val: float,
    now_est: datetime = None,
    shared_rsi_state: dict = None,
) -> dict:
    """
    초과 Case 97 — 04:07 ~ 08:14 EST 구간에서 당일 가장 먼저 RSI >= 79.9 를 달성한 첫 번째 종목을
    당일 프리장의 상승 추세 주도주(_PRE_LEADER_CACHE["leader_sym"])로 불변 각인하며,
    장중 프로세스 재기동 시에도 shared_rsi_state(200봉 벡터 스캔 결과 first_0407_799_ts)를 대조하여 100% 자동 복원합니다.
    """
    if symbol not in ("SOXS", "SOXL"):
        return dict(_PRE_LEADER_CACHE)

    if now_est is None:
        now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if 400 <= est_hm <= 814 and _PRE_LEADER_CACHE.get("date") != today_str:
        _reset_pre_leader_state(logical_date=today_str)

    if not (407 <= est_hm <= 814):
        return dict(_PRE_LEADER_CACHE)

    cur_leader = str(_PRE_LEADER_CACHE.get("leader_sym", ""))

    # 1) 장중 재기동 시 200봉 1분봉 벡터 스캔 타임스탬프(first_0407_799_ts) 비교로 첫 상승 추세 종목 자동 복원
    if not cur_leader and isinstance(shared_rsi_state, dict):
        ewm_s = ((shared_rsi_state.get("SOXS") or {}).get("ewm_state")) or {}
        ewm_l = ((shared_rsi_state.get("SOXL") or {}).get("ewm_state")) or {}
        ts_s = float(ewm_s.get("first_0407_799_ts", 0.0)) if ewm_s.get("logical_date") == today_str else 0.0
        ts_l = float(ewm_l.get("first_0407_799_ts", 0.0)) if ewm_l.get("logical_date") == today_str else 0.0

        if ts_s > 0.0 and (ts_l <= 0.0 or ts_s <= ts_l):
            _PRE_LEADER_CACHE["leader_sym"] = "SOXS"
            _PRE_LEADER_CACHE["leader_rsi"] = float(ewm_s.get("first_0407_799_rsi", PRE_TREND_LEADER_SELECT_RSI))
            _PRE_LEADER_CACHE["locked_ts"] = ts_s
            _PRE_LEADER_CACHE["locked_time_str"] = str(ewm_s.get("first_0407_799_time_str", now_est.strftime("%H:%M:%S")))
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
            cur_leader = "SOXS"
            print(
                f"👑 [프리장 첫 상승추세 종목 벡터 복원 SOXS] 시각: {_PRE_LEADER_CACHE['locked_time_str']} EST | "
                f"피크 RSI: {_PRE_LEADER_CACHE['leader_rsi']:.1f} ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 감시 대기",
                flush=True,
            )
        elif ts_l > 0.0 and (ts_s <= 0.0 or ts_l < ts_s):
            _PRE_LEADER_CACHE["leader_sym"] = "SOXL"
            _PRE_LEADER_CACHE["leader_rsi"] = float(ewm_l.get("first_0407_799_rsi", PRE_TREND_LEADER_SELECT_RSI))
            _PRE_LEADER_CACHE["locked_ts"] = ts_l
            _PRE_LEADER_CACHE["locked_time_str"] = str(ewm_l.get("first_0407_799_time_str", now_est.strftime("%H:%M:%S")))
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
            cur_leader = "SOXL"
            print(
                f"👑 [프리장 첫 상승추세 종목 벡터 복원 SOXL] 시각: {_PRE_LEADER_CACHE['locked_time_str']} EST | "
                f"피크 RSI: {_PRE_LEADER_CACHE['leader_rsi']:.1f} ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 감시 대기",
                flush=True,
            )

    # 2) 실시간 틱 RSI >= 79.9 최초 도달 종목 불변 각인 (FIRST_799)
    if not cur_leader and live_rsi_val >= PRE_TREND_LEADER_SELECT_RSI:
        _PRE_LEADER_CACHE["leader_sym"] = symbol
        _PRE_LEADER_CACHE["leader_rsi"] = float(live_rsi_val)
        _PRE_LEADER_CACHE["locked_ts"] = time.time()
        _PRE_LEADER_CACHE["locked_time_str"] = now_est.strftime("%H:%M:%S")
        _PRE_LEADER_CACHE["pb_rearm_ready"] = True
        print(
            f"👑 [프리장 첫 상승추세 종목 각인 {symbol}] {now_est.strftime('%H:%M:%S')} EST | "
            f"RSI {live_rsi_val:.1f} (≥{PRE_TREND_LEADER_SELECT_RSI:.1f}) ➡️ 07:00~08:14 RSI≤{PRE_TREND_PULLBACK_RSI_THRESHOLD:.1f} 추세눌림목 타겟 락온",
            flush=True,
        )
    elif cur_leader == symbol:
        if live_rsi_val > float(_PRE_LEADER_CACHE.get("leader_rsi", 0.0)):
            _PRE_LEADER_CACHE["leader_rsi"] = float(live_rsi_val)
        if not bool(_PRE_LEADER_CACHE.get("pb_rearm_ready", True)) and live_rsi_val > PRE_TREND_REARM_RESET_RSI:
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True

    return dict(_PRE_LEADER_CACHE)


def _reset_manual_stage3_anchor(symbol: str = None):
    """수동 포지션 3차 방어 QQQ 기준가 영점 앵커 캐시를 초기화합니다 (초과 Case 94)."""
    if symbol and symbol in _MANUAL_STAGE3_ANCHOR_CACHE:
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {"anchored": False, "verified_sell_id": ""}
    else:
        for sym in ("SOXS", "SOXL"):
            _MANUAL_STAGE3_ANCHOR_CACHE[sym] = {"anchored": False, "verified_sell_id": ""}


def mark_manual_stage3_anchored(symbol: str, sell_order_id: str = ""):
    """수동 포지션의 상단 안전 캡(+5.00% LIMIT) 장전 및 실시간 QQQ 기준가 락온 완료를 기록합니다 (초과 Case 94)."""
    if symbol in _MANUAL_STAGE3_ANCHOR_CACHE:
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = {
            "anchored": True,
            "verified_sell_id": str(sell_order_id or ""),
        }


def _reset_trailing_runtime_state(symbol: str = None):
    """하이킨 아시 트레일링 인메모리 캐시 및 수동 3차 방어 앵커 캐시를 원자적으로 초기화합니다 (Case 02, 22, 85, 94)."""
    _reset_manual_stage3_anchor(symbol)
    if symbol and symbol in _TRAIL_RUNTIME_CACHE:
        _TRAIL_RUNTIME_CACHE[symbol] = _make_empty_trail_slot()
    else:
        for sym in ("SOXS", "SOXL"):
            _TRAIL_RUNTIME_CACHE[sym] = _make_empty_trail_slot()


def get_trailing_runtime_status(symbol: str) -> dict:
    """UI 관제탑(/avwap, /sync, /settlement) 및 정산 모듈에서 트레일링 상태를 조회합니다."""
    slot = _TRAIL_RUNTIME_CACHE.get(str(symbol or "").upper()) or _make_empty_trail_slot()
    return dict(slot)


def _reset_bottom_track_state(symbol: str = None):
    """
    3차 손절 후 최저점 역트레일링 캐시(_BOTTOM_TRACK_CACHE)를 원자적으로 초기화합니다 (Case 02, 22, 87).
    """
    if symbol and _BOTTOM_TRACK_CACHE.get("symbol") not in ("", symbol):
        return
    _BOTTOM_TRACK_CACHE["active"] = False
    _BOTTOM_TRACK_CACHE["mode"] = "STAGE3_STOP"
    _BOTTOM_TRACK_CACHE["symbol"] = ""
    _BOTTOM_TRACK_CACHE["date"] = ""
    _BOTTOM_TRACK_CACHE["last_stop_price"] = 0.0
    _BOTTOM_TRACK_CACHE["post_stop_low"] = 0.0
    _BOTTOM_TRACK_CACHE["rebound_trig_price"] = 0.0
    _BOTTOM_TRACK_CACHE["dipped_below_stop"] = False
    _BOTTOM_TRACK_CACHE["climax_seen"] = True
    _BOTTOM_TRACK_CACHE["activated_ts"] = 0.0
    _BOTTOM_TRACK_CACHE["last_new_low_ts"] = 0.0


def _activate_bottom_track_state(
    symbol: str,
    stop_price: float,
    current_price: float = 0.0,
    logical_date: str = "",
):
    """
    04:07 ~ 08:14 EST 골든타임 내 NQ=F < 2.0% 상태에서 3차 역행 손절 발생 시
    최저점 역트레일링 반등 감시(_BOTTOM_TRACK_CACHE)를 점화합니다 (초과 Case 87).
    """
    if symbol not in ("SOXS", "SOXL") or stop_price <= 0.0:
        return
    sp_fl = math.floor(float(stop_price) * 100.0) / 100.0
    cp_fl = math.floor(float(current_price) * 100.0) / 100.0 if current_price > 0.0 else sp_fl
    init_low = min(sp_fl, cp_fl) if cp_fl > 0.0 else sp_fl
    trig_p = math.ceil(init_low * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    today_s = logical_date or datetime.now(EST_TZ).strftime("%Y-%m-%d")

    _BOTTOM_TRACK_CACHE["active"] = True
    _BOTTOM_TRACK_CACHE["mode"] = "STAGE3_STOP"
    _BOTTOM_TRACK_CACHE["symbol"] = symbol
    _BOTTOM_TRACK_CACHE["date"] = today_s
    _BOTTOM_TRACK_CACHE["last_stop_price"] = sp_fl
    _BOTTOM_TRACK_CACHE["post_stop_low"] = init_low
    _BOTTOM_TRACK_CACHE["rebound_trig_price"] = trig_p
    _BOTTOM_TRACK_CACHE["dipped_below_stop"] = bool(init_low < sp_fl)
    _BOTTOM_TRACK_CACHE["climax_seen"] = True
    _BOTTOM_TRACK_CACHE["activated_ts"] = time.time()
    _BOTTOM_TRACK_CACHE["last_new_low_ts"] = time.time()


def get_bottom_track_status(symbol: str = None) -> dict:
    """
    UI 관제탑(/avwap, /sync, /settlement) 및 main.py 0.4초 부스터 루프에서
    3차 손절 후 최저점 역트레일링 추적(STAGE3_STOP) 상태를 조회합니다 (초과 Case 87, 93).
    """
    if _BOTTOM_TRACK_CACHE.get("active", False):
        if not symbol or _BOTTOM_TRACK_CACHE.get("symbol") == symbol:
            res = dict(_BOTTOM_TRACK_CACHE)
            res["mode"] = "STAGE3_STOP"
            return res

    return {
        "active": False,
        "mode": "NONE",
        "symbol": symbol or "",
        "date": "",
        "last_stop_price": 0.0,
        "post_stop_low": 0.0,
        "rebound_trig_price": 0.0,
        "dipped_below_stop": False,
    }


# Case 24: MOC, 매크로 덤핑, 15:00 VWAP 탈출 스윕, 트레일링 스윕 및 04:07 휩소 정리 시 미체결 주문을 선취소하고
#          최대 2.0초(0.25초 × 8회) 동안 매도 가능 수량(sellableQuantity) 해방을 폴링 대기하는 방탄 헬퍼
async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    sell_order_id: str = "",
    cancel_all_sides: bool = True,
) -> tuple[int, list[str]]:
    canceled_ids = set()
    if sell_order_id:
        try:
            await ctx.client.cancel_order(sell_order_id)
            canceled_ids.add(sell_order_id)
        except Exception:
            pass

    try:
        open_orders = await ctx.client.get_orders(status="OPEN", symbol=symbol)
        for o in open_orders:
            oid = str(o.get("orderId") or "")
            side = str(o.get("side") or "")
            if not oid or oid in canceled_ids:
                continue
            if cancel_all_sides or side == "SELL":
                try:
                    await ctx.client.cancel_order(oid)
                    canceled_ids.add(oid)
                except Exception:
                    pass
    except Exception:
        pass

    sellable_q_int = 0
    for _ in range(8):
        try:
            sq = await ctx.client.get_sellable_quantity(symbol)
            sellable_q_int = int(math.floor(float(sq or 0.0)))
            if sellable_q_int > 0:
                break
        except Exception:
            pass
        await asyncio.sleep(0.25)

    surviving_sell_ids: list[str] = []
    if sellable_q_int <= 0:
        try:
            open_after = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for o in open_after:
                if o.get("side") == "SELL" and o.get("orderId"):
                    surviving_sell_ids.append(str(o["orderId"]))
        except Exception:
            pass
        if not surviving_sell_ids and sell_order_id:
            surviving_sell_ids.append(sell_order_id)

    return sellable_q_int, surviving_sell_ids
