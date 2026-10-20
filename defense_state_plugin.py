# =====================================================================
# FILE: defense_state_plugin.py
# 목적: 프리장 04:07 EST 이후 첫 RSI>=79.9 진성 상승 추세 주도주(_PRE_LEADER_CACHE) 불변 각인·복원,
#       이원화 하이킨 아시 트레일링(_TRAIL_RUNTIME_CACHE),
#       3차 안전망 손절 후 최저점 역트레일링 +0.30% 반등 재진입(_BOTTOM_TRACK_CACHE),
#       수동 진입(MANUAL) 첫 틱 실시간 QQQ 0.00% 영점 재락온(_MANUAL_STAGE3_ANCHOR_CACHE) 및
#       미체결 주문 선취소 + 매도 가능 수량(sellableQuantity) 2.0초 폴링 해방 헬퍼 코어 플러그인
#       (defense_plugin.py 1/2 분할본)
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 방어·트레일링·상승주도주·최저점추적 인메모리 상태 머신을 defense_state_plugin.py로 분리
# MODIFIED: 초과 Case 24 — 미체결 매도 선취소 후 최대 2.0초(0.25초 * 8회) sellableQuantity 해방 폴링 및 생존 주문 ID 반환
# MODIFIED: 초과 Case 85, 89, 99 — 이원화 하이킨 아시 트레일링(OLD_IMM_CB vs PEAK_LCK_130) 런타임 캐시 관리
# MODIFIED: 초과 Case 87 — 04:07 ~ 08:14 EST 내 NQ<2.0% 3차 역행 손절 후 최저점 역트레일링(_BOTTOM_TRACK_CACHE) 상태 관리
# MODIFIED: 초과 Case 94 — 스나이퍼 OFF➔ON 재가동 시 수동 보유 물량 실시간 QQQ 기준가 0.00% 영점 재락온 캐시 관리
# MODIFIED: 초과 Case 97 — update_premarket_trend_leader 에서 first_0407_799_ts 및 first_799_ts 양방향 키 스캔과
#           최선행 달성 시각(FIRST_799) 비교를 통해 장중 재기동 시에도 100% 누락 없이 상승 주도주(leader_sym) 소급 복원

import math
import time
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_engine import (
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    PRE_PIVOT_10M_MIN_SWING_AMP_PCT,
    PRE_PIVOT_10M_MAX_TROUGH_RSI,
    PRE_PIVOT_10M_REBOUND_MIN_PCT,
    PRE_PIVOT_10M_REBOUND_MAX_PCT,
    PRE_PIVOT_10M_MIN_BODY_PCT,
    compute_realtime_tick_heikin_ashi,
    compute_realtime_tick_10m_state,
)

EST_TZ = ZoneInfo("America/New_York")

# 하이킨 아시 트레일링 및 3단 방어망 핵심 상수 (초과 Case 85, 87, 89, 92, 94, 97, 99)
TRAIL_ARM_PROFIT_RATE = 1.00            # +1.00% 도달 시 하이킨 아시 트레일링 점화
TRAIL_FLOOR_LOCK_RATE = 1.00            # 최소 익절 보존 하한선 (+1.00%)
TRAIL_DROP_CALLBACK_PCT = 0.30          # 기술적 반등(OLD_IMM_CB) 최고가 대비 콜백 스탑폭 (-0.30%)
TRAIL_SAFETY_CAP_RATE = 5.00            # 매수 체결 즉시 거래소에 선장전하는 상단 안전 캡 (+5.00% LIMIT)
NQ_VELOCITY_SURGE_PCT = 0.20            # QQQ 실시간 10m/3m 변화율 ±0.20% 급등락 시 HA음봉 바이패스

BOTTOM_REBOUND_PCT = 0.30               # 3차 손절 후 최저점 대비 반등 재진입 기준폭 (+0.30%)
BOTTOM_MAX_RSI_CAP = 38.0               # 최저점 반등 재진입 허용 최대 RSI 상한 (<= 38.0)

_AUTO_ENTRY_SESSION_TAGS = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_PIVOT_10M",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "PRE_PIVOT_10M_REARM",
    "WHIPSAW_1SH",
    "VWAP_ESCAPE",
)

# 중복 경보 차단 및 상태 추적 인메모리 캐시 (초과 Case 74, 85, 87, 94, 96, 97, 98)
_STRIKE_ALERT_CACHE = {
    "last_warn_session": "",
    "last_err_msg": "",
    "last_macro1_date": "",
    "last_macro1_cancel_id": "",
    "last_cutoff_0815_date": "",
    "last_vwap_escape_date": "",
}


def _default_trail_symbol_state() -> dict:
    return {
        "armed": False,
        "peak_price": 0.0,
        "stop_price": 0.0,
        "arm_price": 0.0,
        "armed_ts": 0.0,
        "armed_hm": -1,
        "bear_candle_seen": False,
        "nq_vel_bypass": False,
        "exit_regime": "OLD_IMM_CB",    # "OLD_IMM_CB" | "PEAK_LCK_130"
        "sweep_fired": False,
        "last_known_qty": 0,
    }


_TRAIL_RUNTIME_CACHE = {
    "SOXS": _default_trail_symbol_state(),
    "SOXL": _default_trail_symbol_state(),
}

_BOTTOM_TRACK_CACHE = {
    "active": False,
    "date": "",
    "symbol": "",
    "last_stop_price": 0.0,
    "post_stop_low": 0.0,
    "rebound_trig_price": 0.0,
    "dipped_below_stop": False,
    "climax_seen": True,
    "last_new_low_ts": 0.0,
}

_PRE_LEADER_CACHE = {
    "date": "",
    "leader_sym": "",
    "leader_rsi": 0.0,
    "locked_ts": 0.0,
    "locked_time_str": "",
    "pb_rearm_ready": True,
    "pb_dispatched_count": 0,
}

# 하위/상위 모듈 호환용 10분봉 스윙 캐시 유지
_SWING_10M_CACHE = {
    "date": "",
    "SOXS": {
        "swing_peak": 0.0,
        "swing_trough": 0.0,
        "swing_drop_pct": 0.0,
        "trough_min_rsi": 100.0,
        "pivot_ready": False,
        "pivot_dispatched": False,
    },
    "SOXL": {
        "swing_peak": 0.0,
        "swing_trough": 0.0,
        "swing_drop_pct": 0.0,
        "trough_min_rsi": 100.0,
        "pivot_ready": False,
        "pivot_dispatched": False,
    },
}

_MANUAL_STAGE3_ANCHOR_CACHE = {
    "SOXS": False,
    "SOXL": False,
}


def _reset_pre_leader_state():
    _PRE_LEADER_CACHE.update({
        "date": "",
        "leader_sym": "",
        "leader_rsi": 0.0,
        "locked_ts": 0.0,
        "locked_time_str": "",
        "pb_rearm_ready": True,
        "pb_dispatched_count": 0,
    })


def mark_pre_pullback_dispatched(symbol: str):
    if _PRE_LEADER_CACHE.get("leader_sym") == symbol:
        _PRE_LEADER_CACHE["pb_rearm_ready"] = False
        _PRE_LEADER_CACHE["pb_dispatched_count"] = int(_PRE_LEADER_CACHE.get("pb_dispatched_count", 0)) + 1


def get_pre_leader_status() -> dict:
    now_est = datetime.now(EST_TZ)
    today_str = now_est.strftime("%Y-%m-%d")
    if _PRE_LEADER_CACHE.get("date") and _PRE_LEADER_CACHE.get("date") != today_str:
        _reset_pre_leader_state()
    return dict(_PRE_LEADER_CACHE)


def update_premarket_trend_leader(
    symbol: str,
    live_rsi_val: float,
    now_est: datetime = None,
    shared_rsi_state: dict = None,
) -> dict:
    """
    초과 Case 97:
    1) 당일 04:07 ~ 08:14 EST 구간에서 가장 먼저 RSI(1m) >= 79.9(80.0 클라이맥스)를 달성한 종목을
       당일 프리장 진성 상승 추세 주도주(_PRE_LEADER_CACHE["leader_sym"])로 불변 각인합니다.
    2) 장중 프로세스 재기동 또는 캔들 갱신 시 shared_rsi_state 내부 ewm_state의
       `first_0407_799_ts` 및 `first_799_ts` 를 비교하여 가장 먼저 달성한 종목(FIRST_799)을 100% 자동 복원합니다.
    3) 추세눌림목(PRE_PULLBACK) 진입 후 해당 leader_sym의 RSI가 40.0(PRE_TREND_REARM_RESET_RSI)을 초과 회복하면
       차기 추세눌림목 재출동 플래그(pb_rearm_ready = True)를 재장전합니다.
    """
    if now_est is None or not isinstance(now_est, datetime):
        now_est = datetime.now(EST_TZ)
    else:
        now_est = now_est.astimezone(EST_TZ)

    today_str = now_est.strftime("%Y-%m-%d")
    est_hm = now_est.hour * 100 + now_est.minute

    if _PRE_LEADER_CACHE.get("date") != today_str:
        _reset_pre_leader_state()
        _PRE_LEADER_CACHE["date"] = today_str

    if not (407 <= est_hm <= 814):
        return dict(_PRE_LEADER_CACHE)

    # 1단계: shared_rsi_state의 캔들 벡터 스캔 결과(first_0407_799_ts / first_799_ts)로부터 최선행 종목(FIRST_799) 복원
    if isinstance(shared_rsi_state, dict):
        ewm_s = (shared_rsi_state.get("SOXS") or {}).get("ewm_state") or {}
        ewm_l = (shared_rsi_state.get("SOXL") or {}).get("ewm_state") or {}

        ts_s = float(ewm_s.get("first_0407_799_ts") or ewm_s.get("first_799_ts") or 0.0)
        ts_l = float(ewm_l.get("first_0407_799_ts") or ewm_l.get("first_799_ts") or 0.0)

        cand_sym = ""
        cand_ts = 0.0
        cand_ewm = {}
        if ts_s > 0.0 and ts_l > 0.0:
            if ts_s <= ts_l:
                cand_sym, cand_ts, cand_ewm = "SOXS", ts_s, ewm_s
            else:
                cand_sym, cand_ts, cand_ewm = "SOXL", ts_l, ewm_l
        elif ts_s > 0.0:
            cand_sym, cand_ts, cand_ewm = "SOXS", ts_s, ewm_s
        elif ts_l > 0.0:
            cand_sym, cand_ts, cand_ewm = "SOXL", ts_l, ewm_l

        if cand_sym and cand_ts > 0.0:
            cur_leader = str(_PRE_LEADER_CACHE.get("leader_sym") or "")
            cur_locked_ts = float(_PRE_LEADER_CACHE.get("locked_ts", 0.0) or 0.0)
            # 아직 각인 전이거나, 캔들 히스토리상 더 앞선 시각(cand_ts < cur_locked_ts)의 첫 달성 종목이 확인된 경우 즉시 복원!
            if (not cur_leader) or (cur_locked_ts <= 0.0) or (cand_ts + 1.0 < cur_locked_ts):
                cand_rsi = float(
                    cand_ewm.get("first_0407_799_rsi")
                    or cand_ewm.get("first_799_rsi")
                    or PRE_TREND_LEADER_SELECT_RSI
                )
                cand_t_str = str(
                    cand_ewm.get("first_0407_799_time_str")
                    or cand_ewm.get("first_799_time_str")
                    or datetime.fromtimestamp(cand_ts, tz=EST_TZ).strftime("%H:%M:%S")
                )
                _PRE_LEADER_CACHE["leader_sym"] = cand_sym
                _PRE_LEADER_CACHE["leader_rsi"] = round(max(cand_rsi, PRE_TREND_LEADER_SELECT_RSI), 1)
                _PRE_LEADER_CACHE["locked_ts"] = cand_ts
                _PRE_LEADER_CACHE["locked_time_str"] = cand_t_str
                if int(_PRE_LEADER_CACHE.get("pb_dispatched_count", 0)) == 0:
                    _PRE_LEADER_CACHE["pb_rearm_ready"] = True

    # 2단계: 실시간 틱 RSI가 79.9 이상 도달 시 즉시 각인 (FIRST_799 불변 원칙)
    if symbol in ("SOXS", "SOXL") and live_rsi_val >= PRE_TREND_LEADER_SELECT_RSI:
        if not _PRE_LEADER_CACHE.get("leader_sym"):
            _PRE_LEADER_CACHE["leader_sym"] = symbol
            _PRE_LEADER_CACHE["leader_rsi"] = round(float(live_rsi_val), 1)
            _PRE_LEADER_CACHE["locked_ts"] = float(now_est.timestamp())
            _PRE_LEADER_CACHE["locked_time_str"] = now_est.strftime("%H:%M:%S")
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
        elif _PRE_LEADER_CACHE.get("leader_sym") == symbol:
            if live_rsi_val > float(_PRE_LEADER_CACHE.get("leader_rsi", 0.0)):
                _PRE_LEADER_CACHE["leader_rsi"] = round(float(live_rsi_val), 1)

    # 3단계: 추세눌림목 출격 후 RSI > 40.0 회복 시 재장전
    if (
        _PRE_LEADER_CACHE.get("leader_sym") == symbol
        and not bool(_PRE_LEADER_CACHE.get("pb_rearm_ready", True))
        and live_rsi_val > PRE_TREND_REARM_RESET_RSI
    ):
        _PRE_LEADER_CACHE["pb_rearm_ready"] = True

    return dict(_PRE_LEADER_CACHE)


# 하위/상위 모듈 호환용 10분봉 스윙 함수군 유지
def _reset_swing_10m_state():
    _SWING_10M_CACHE["date"] = ""
    for s in ("SOXS", "SOXL"):
        _SWING_10M_CACHE[s] = {
            "swing_peak": 0.0,
            "swing_trough": 0.0,
            "swing_drop_pct": 0.0,
            "trough_min_rsi": 100.0,
            "pivot_ready": False,
            "pivot_dispatched": False,
        }


def mark_pre_pivot_10m_dispatched(symbol: str):
    if symbol in ("SOXS", "SOXL") and symbol in _SWING_10M_CACHE:
        _SWING_10M_CACHE[symbol]["pivot_dispatched"] = True
        _SWING_10M_CACHE[symbol]["pivot_ready"] = False


def get_swing_10m_status(symbol: str = None) -> dict:
    if symbol in ("SOXS", "SOXL"):
        return dict(_SWING_10M_CACHE.get(symbol, {}))
    return {
        "SOXS": dict(_SWING_10M_CACHE.get("SOXS", {})),
        "SOXL": dict(_SWING_10M_CACHE.get("SOXL", {})),
    }


def update_premarket_swing_10m_wave(
    symbol: str,
    live_price: float,
    live_rsi: float,
    ewm_state: dict = None,
    now_est: datetime = None,
) -> dict:
    if symbol not in ("SOXS", "SOXL"):
        return {}
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    today_str = now_est.strftime("%Y-%m-%d")
    if _SWING_10M_CACHE.get("date") != today_str:
        _reset_swing_10m_state()
        _SWING_10M_CACHE["date"] = today_str
    return dict(_SWING_10M_CACHE[symbol])


def evaluate_premarket_10m_pivot_breakout(
    symbol: str,
    live_price: float,
    live_rsi: float,
    opp_live_price: float,
    opp_live_rsi: float,
    shared_rsi_state: dict = None,
    now_est: datetime = None,
) -> tuple[bool, dict]:
    return False, {}


def _reset_manual_stage3_anchor(symbol: str = None):
    if symbol in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = False
    else:
        _MANUAL_STAGE3_ANCHOR_CACHE["SOXS"] = False
        _MANUAL_STAGE3_ANCHOR_CACHE["SOXL"] = False


def mark_manual_stage3_anchored(symbol: str):
    if symbol in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[symbol] = True


def is_manual_stage3_anchored(symbol: str) -> bool:
    return bool(_MANUAL_STAGE3_ANCHOR_CACHE.get(symbol, False))


def _reset_trailing_runtime_state(symbol: str = None):
    if symbol in ("SOXS", "SOXL"):
        _TRAIL_RUNTIME_CACHE[symbol] = _default_trail_symbol_state()
    else:
        _TRAIL_RUNTIME_CACHE["SOXS"] = _default_trail_symbol_state()
        _TRAIL_RUNTIME_CACHE["SOXL"] = _default_trail_symbol_state()


def get_trailing_runtime_status(symbol: str) -> dict:
    if symbol in ("SOXS", "SOXL"):
        return dict(_TRAIL_RUNTIME_CACHE.get(symbol, _default_trail_symbol_state()))
    return _default_trail_symbol_state()


def _reset_bottom_track_state():
    _BOTTOM_TRACK_CACHE.update({
        "active": False,
        "date": "",
        "symbol": "",
        "last_stop_price": 0.0,
        "post_stop_low": 0.0,
        "rebound_trig_price": 0.0,
        "dipped_below_stop": False,
        "climax_seen": True,
        "last_new_low_ts": 0.0,
    })


def _activate_bottom_track_state(symbol: str, stop_price: float, now_est: datetime = None):
    if symbol not in ("SOXS", "SOXL") or stop_price <= 0.0:
        return
    if now_est is None:
        now_est = datetime.now(EST_TZ)
    stop_fl = math.floor(float(stop_price) * 100.0) / 100.0
    trig_p = math.ceil(stop_fl * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    _BOTTOM_TRACK_CACHE.update({
        "active": True,
        "date": now_est.strftime("%Y-%m-%d"),
        "symbol": symbol,
        "last_stop_price": stop_fl,
        "post_stop_low": stop_fl,
        "rebound_trig_price": trig_p,
        "dipped_below_stop": False,
        "climax_seen": True,
        "last_new_low_ts": time.time(),
    })


def get_bottom_track_status(symbol: str = None) -> dict:
    now_est = datetime.now(EST_TZ)
    est_hm = now_est.hour * 100 + now_est.minute
    today_str = now_est.strftime("%Y-%m-%d")

    if _BOTTOM_TRACK_CACHE.get("active", False):
        if _BOTTOM_TRACK_CACHE.get("date") != today_str or not (407 <= est_hm <= 814):
            _reset_bottom_track_state()

    st = dict(_BOTTOM_TRACK_CACHE)
    if symbol in ("SOXS", "SOXL"):
        if not (st.get("active", False) and st.get("symbol") == symbol):
            return {
                "active": False,
                "date": today_str,
                "symbol": symbol,
                "last_stop_price": 0.0,
                "post_stop_low": 0.0,
                "rebound_trig_price": 0.0,
                "dipped_below_stop": False,
                "climax_seen": False,
            }
    return st


async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    sell_order_id: str = "",
    cancel_all_sides: bool = False,
) -> tuple[int, list[str]]:
    """
    초과 Case 24:
    미체결 매도(또는 전체) 주문을 선취소하고 최대 2.0초(0.25초 * 8회) 동안
    판매 가능 수량(sellableQuantity > 0) 해방을 폴링 대기합니다.
    취소 병목 시 modify_order 우회 정정에 사용할 수 있도록 살아있는 매도 주문 ID 리스트(surviving_sell_ids)를 함께 반환합니다.
    """
    canceled_ids = set()
    surviving_sell_ids = []

    if sell_order_id:
        try:
            await ctx.client.cancel_order(sell_order_id)
            canceled_ids.add(sell_order_id)
        except Exception as e:
            err_s = str(e).lower()
            if "already-canceled" not in err_s and "404" not in err_s and "order-not-found" not in err_s:
                surviving_sell_ids.append(sell_order_id)

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
                except Exception as ce:
                    err_l = str(ce).lower()
                    if side == "SELL" and "already-canceled" not in err_l and "404" not in err_l:
                        if oid not in surviving_sell_ids:
                            surviving_sell_ids.append(oid)
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

    if sellable_q_int <= 0 and not surviving_sell_ids:
        try:
            open_after = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            for oa in open_after:
                if oa.get("side") == "SELL" and oa.get("orderId"):
                    surviving_sell_ids.append(str(oa["orderId"]))
        except Exception:
            pass

    return sellable_q_int, surviving_sell_ids
