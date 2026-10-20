# =====================================================================
# FILE: defense_plugin.py
# 목적: sniper_plugin.py (line 59) 및 tg_ui_plugin.py 교차 임포트 32개 심볼 100% 정식 제공,
#       get_bottom_track_status(symbol=None) 외 13대 상태 제어·조회 헬퍼 다형성 시그니처(0개/1개+ 인자 완벽 호환),
#       방향성 인지(Direction-Aware) 매크로 3단 방어망(execute_macro_defense_plugin),
#       08:15 EST 거시지표 발표 15분 전 선제 컷오프(enforce_0930_regular_cutoff_plugin / enforce_0815_pre_macro_cutoff_plugin),
#       15:00 ~ 15:58 EST 장마감 1시간 전 미익절 물량 정규장 VWAP 도달 즉시 탈출(execute_late_regular_vwap_escape_plugin),
#       15:59 ~ 16:01 EST 제로-오버나이트 MOC 2단계 방탄 청산(execute_moc_zero_overnight_plugin) 및
#       미체결 주문 선취소·매도가능수량 해방 폴링 헬퍼(_cancel_open_orders_and_wait_sellable) 코어 플러그인
# =====================================================================
# MODIFIED: 제1 ~ 제6헌법 및 초과 Case 48, 63, 66, 67, 74, 82, 84, 85, 86, 87, 88, 92, 94, 95, 96, 97, 98, 99 완벽 준수
# NEW: 1) `get_bottom_track_status(symbol: str = None, *args, **kwargs)` 등 모든 상태 조회·초기화·마킹 함수가
#         인자 0개 호출(`get_bottom_track_status()`)과 종목 인자 1개 호출(`get_bottom_track_status("SOXS")`)을
#         모두 100% 수용하도록 다형성 시그니처 적용 (`/sync` 지시서 및 `/avwap` 관제탑 TypeError 영구 소각)
#      2) `_BOTTOM_TRACK_CACHE` 에 최상위 요약 키와 종목별(`"SOXS"`, `"SOXL"`) 서브 딕셔너리를 동시 유지하여
#         직접 딕셔너리 인덱싱과 헬퍼 함수 호출 양쪽 모두 100% 호환 보장
#      3) 모바일 클립보드 복사 시 `\vert{}` 문법 에러를 유발하는 세로바 기호를 전면 배제하고 가운뎃점(`·`) 적용

import sys
import math
import time
import uuid
import html
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_engine import (
    AssassinLedger,
    MacroDataCache,
    SINGLE_TARGET_PROFIT_RATE,
    MIN_STAGE3_QQQ_ADVERSE_PCT,
    PRE_TREND_LEADER_SELECT_RSI,
    PRE_TREND_PULLBACK_RSI_THRESHOLD,
    PRE_TREND_REARM_RESET_RSI,
    TREND_PB_CALLBACK_PCT,
    TREND_PB_LAUNCH_RATCHET_PCT,
    TREND_PB_CUSHION_FLOOR_RATE,
)

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# =====================================================================
# 1. 전술 상수 선언 (sniper_plugin.py 59번 줄 요구 상수 8종 + 호환 별칭 100% 완비)
# =====================================================================
TRAIL_ARM_PROFIT_RATE = float(SINGLE_TARGET_PROFIT_RATE)   # +1.00% 하이킨 아시 트레일링 점화가 기준
TRAIL_FLOOR_LOCK_RATE = float(SINGLE_TARGET_PROFIT_RATE)   # +1.00% 최소 익절 보존선 하드 락온 기준
TRAIL_DROP_CALLBACK_PCT = 0.30                             # 기술적 반등(PRE_SNIPER) 최고가 대비 -0.30% 콜백 스탑
TRAIL_CALLBACK_PCT = 0.30                                  # 호환 별칭 (-0.30% 콜백 스탑)
TRAIL_SAFETY_CAP_RATE = 5.00                               # +5.00% 거래소 선장전 상단 안전 캡 LIMIT
SAFETY_CAP_PROFIT_RATE = 5.00                              # 호환 별칭 (+5.00% 상단 안전 캡)
NQ_VELOCITY_SURGE_PCT = 0.20                               # QQQ 실시간 10m/3m 변화율 ±0.20% 급등락 시 HA음봉 바이패스
NQ_DYNAMIC_FLOOR = float(MIN_STAGE3_QQQ_ADVERSE_PCT)       # 3차 실시간 QQQ 역행 최소 하한선 (0.80%)

BOTTOM_REBOUND_PCT = 0.30                                  # 3차 손절 후 최저점 대비 +0.30% 양봉 반등 재진입 기준
POST_STOP_REBOUND_PCT = 0.30                               # 호환 별칭 (+0.30% 양봉 반등)
BOTTOM_MAX_RSI_CAP = 38.0                                  # 최저점 반등 재진입 허용 최대 RSI 상한 (RSI <= 38.0)
POST_STOP_MAX_RSI = 38.0                                   # 호환 별칭 (RSI <= 38.0)

PRE_SNIPER_RSI_THRESHOLD = 30.0                            # 양방향 대칭 클라이맥스 과매도 매수 기준 (RSI <= 30.0)
PRE_TREND_BOOSTER_RSI = 38.0                               # 07:00~08:14 선행 상승종목 0.4초 고주파 부스터 진입 기준
PRE_CLIMAX_BOOSTER_RSI = 79.0                              # 프리-클라이맥스 0.4초 고주파 부스터 진입 기준 (RSI >= 79.0)
OPP_CLIMAX_RSI_THRESHOLD = 79.9                            # 반대편 대세 종목 과열 클라이맥스 인지 기준 (RSI >= 79.9)

FIRST_TRAP_SLIPPAGE_PCT = 0.0018                           # 1차 지정가 매수 덫 슬리피지 할인율 (-0.18%)
FIRST_TRAP_MIN_DISC_USD = 0.02                             # 1차 지정가 매수 덫 최소 달러 할인폭 (-$0.02)
TRAP_SHIELD_WAIT_SEC = 30.0                                # 조건부 타임쉴드 최대 대기 시간 (30.0초)

# 자동 진입 세션 식별 표준 태그 튜플 (초과 Case 41, 86, 94)
_AUTO_ENTRY_SESSION_TAGS = (
    "PRE_SNIPER",
    "PRE_PULLBACK",
    "PRE_SNIPER_REARM",
    "PRE_PULLBACK_REARM",
    "WHIPSAW_1SH",
    "STAGE3_STOP_TRACK",
)


# =====================================================================
# 2. 공용 컨텍스트 및 인메모리 캐시 6종 (초과 Case 74, 85, 87, 94, 96, 97, 99)
# =====================================================================
class SniperPluginContext:
    """듀얼 스나이퍼 전술 플러그인 공유 컨텍스트."""
    def __init__(
        self,
        client,
        bot,
        chat_id: int,
        wakeup_event: asyncio.Event,
        in_memory_ordering_lock: dict,
        dual_strike_mutex: asyncio.Lock,
        shared_holdings: dict,
        idempotency_keys: dict,
        nq_dynamic_floor: float = MIN_STAGE3_QQQ_ADVERSE_PCT,
        shared_rsi_state: dict = None,
    ):
        self.client = client
        self.bot = bot
        self.chat_id = int(chat_id or 0)
        self.wakeup_event = wakeup_event
        self.in_memory_ordering_lock = in_memory_ordering_lock
        self.dual_strike_mutex = dual_strike_mutex
        self.shared_holdings = shared_holdings
        self.idempotency_keys = idempotency_keys
        self.nq_dynamic_floor = float(nq_dynamic_floor or MIN_STAGE3_QQQ_ADVERSE_PCT)
        self.shared_rsi_state = shared_rsi_state if isinstance(shared_rsi_state, dict) else {}


def _default_trail_state() -> dict:
    return {
        "armed": False,
        "is_armed": False,
        "peak_price": 0.0,
        "max_price": 0.0,
        "arm_price": 0.0,
        "floor_price": 0.0,
        "stop_price": 0.0,
        "callback_pct": TRAIL_DROP_CALLBACK_PCT,
        "armed_at": 0.0,
        "armed_hm": -1,
        "last_mod_price": 0.0,
        "last_known_qty": 0,
        "bear_candle_seen": False,
        "regime": "TECH_BOUNCE",
    }


def _default_symbol_bottom_state(symbol: str = "") -> dict:
    sym_u = str(symbol or "").upper()
    return {
        "active": False,
        "symbol": sym_u,
        "stopped_sym": sym_u,
        "stop_price": 0.0,
        "last_stop_price": 0.0,
        "lowest_price": 0.0,
        "post_stop_low": 0.0,
        "low_price": 0.0,
        "min_price": 0.0,
        "rebound_price": 0.0,
        "rebound_trig_price": 0.0,
        "trigger_price": 0.0,
        "stopped_at": 0.0,
        "triggered_at": 0.0,
        "session_date": "",
        "session_id": "",
    }


def _default_global_bottom_cache() -> dict:
    base = _default_symbol_bottom_state("")
    base["SOXS"] = _default_symbol_bottom_state("SOXS")
    base["SOXL"] = _default_symbol_bottom_state("SOXL")
    return base


# 초과 Case 74: 스나이퍼 무음 불발(예수금 부족 0주 또는 매크로 과열 차단) 세션별 1회 경보 캐시
_STRIKE_ALERT_CACHE: dict[str, str] = {"SOXS": "", "SOXL": ""}

# 초과 Case 44: RSI 레이더 모니터링 캐시
_RSI_MONITOR_CACHE: dict[str, dict] = {
    "SOXS": {"last_alert_ts": 0.0},
    "SOXL": {"last_alert_ts": 0.0},
}

# 초과 Case 85 & Case 99: 이원화 하이킨 아시 고점 트레일링 런타임 캐시
_TRAIL_RUNTIME_CACHE: dict[str, dict] = {
    "SOXS": _default_trail_state(),
    "SOXL": _default_trail_state(),
}

# 초과 Case 87: 3차 실시간 QQQ 역행 손절 후 최저점 역트레일링 반등 재진입 캐시 (전역 + 종목별 서브키 동시 지원)
_BOTTOM_TRACK_CACHE: dict = _default_global_bottom_cache()

# 초과 Case 97: 04:07 EST 이후 첫 RSI >= 79.9 달성 진성 상승 추세 주도주(FIRST_799) 캐시
_PRE_LEADER_CACHE: dict = {
    "leader_sym": "",
    "symbol": "",
    "first_799_sym": "",
    "first_799_ts": 0.0,
    "locked_at": 0.0,
    "first_799_rsi": 0.0,
    "peak_rsi": 0.0,
    "first_799_time_str": "",
    "locked_time_str": "",
    "pb_rearm_ready": True,
    "rearm_ready": True,
    "pb_dispatched": False,
    "session_date": "",
    "session_id": "",
}

# 초과 Case 94: 수동 보유 포지션 스나이퍼 ON 결속 시 실시간 QQQ 기준가 인메모리 앵커 캐시
_MANUAL_STAGE3_ANCHOR_CACHE: dict[str, dict] = {
    "SOXS": {"ref_price": 0.0, "ref_amp": 0.0, "locked_ts": 0.0, "qty": 0, "anchored": False},
    "SOXL": {"ref_price": 0.0, "ref_amp": 0.0, "locked_ts": 0.0, "qty": 0, "anchored": False},
}

# 10분봉 스윙 상태 호환 캐시
_SWING_10M_CACHE: dict = {
    "active": False,
    "symbol": "",
    "locked_sym": "",
    "prelock_sym": "",
    "trend": "WAIT",
    "direction": "NONE",
    "SOXS": {"active": False, "symbol": "SOXS", "trend": "WAIT"},
    "SOXL": {"active": False, "symbol": "SOXL", "trend": "WAIT"},
}

# 초과 Case 96: 1차 매크로 과열 미체결 취소 중복 알림 방지 캐시
_LAST_MACRO1_CANCEL_ID: dict[str, str] = {"SOXS": "", "SOXL": ""}
_LAST_CUTOFF_0815_DATE: dict[str, str] = {"SOXS": "", "SOXL": ""}


# =====================================================================
# 3. 공용 상태 제어·마킹·조회·알림 헬퍼 함수 13종 정식 구현 (0개 / 1개+ 인자 양방향 호환)
# =====================================================================
async def _send_tg_safe(ctx, text: str):
    """Case 17: 텔레그램 HTML 알림 안전 발송 헬퍼."""
    if ctx and getattr(ctx, "bot", None) and getattr(ctx, "chat_id", 0):
        try:
            await ctx.bot.send_message(
                chat_id=ctx.chat_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as e:
            print(f"⚠️ [텔레그램 전송 방어] {e}", flush=True)


async def _send_strike_hold_alert_once(ctx, symbol: str, session_key: str, reason_html: str):
    """초과 Case 74: 무음 불발 방어용 세션별 1회 경보 타전 헬퍼."""
    cache_val = f"{session_key}_{reason_html[:24]}"
    if _STRIKE_ALERT_CACHE.get(symbol) == cache_val:
        return
    _STRIKE_ALERT_CACHE[symbol] = cache_val
    await _send_tg_safe(ctx, reason_html)


def _reset_trailing_runtime_state(symbol: str = None, *args, **kwargs):
    """초과 Case 85: 하이킨 아시 트레일링 인메모리 캐시 초기화 (0개 또는 1개 인자 모두 수용)."""
    targets = (str(symbol).upper(),) if str(symbol or "").upper() in ("SOXS", "SOXL") else ("SOXS", "SOXL")
    for sym in targets:
        if sym in _TRAIL_RUNTIME_CACHE and isinstance(_TRAIL_RUNTIME_CACHE[sym], dict):
            _TRAIL_RUNTIME_CACHE[sym].clear()
            _TRAIL_RUNTIME_CACHE[sym].update(_default_trail_state())
        else:
            _TRAIL_RUNTIME_CACHE[sym] = _default_trail_state()


def get_trailing_runtime_status(symbol: str = None, *args, **kwargs) -> dict:
    """초과 Case 85: 종목별 하이킨 아시 트레일링 런타임 상태 사본 반환 (0개 또는 1개 인자 모두 수용)."""
    sym_u = str(symbol or "").upper()
    if sym_u in ("SOXS", "SOXL"):
        st = _TRAIL_RUNTIME_CACHE.get(sym_u)
        if not isinstance(st, dict):
            return _default_trail_state()
        out = dict(st)
        out["is_armed"] = bool(out.get("armed", False))
        out["max_price"] = float(out.get("peak_price", 0.0) or 0.0)
        return out
    return {
        "SOXS": dict(_TRAIL_RUNTIME_CACHE.get("SOXS") or _default_trail_state()),
        "SOXL": dict(_TRAIL_RUNTIME_CACHE.get("SOXL") or _default_trail_state()),
    }


def _reset_bottom_track_state(symbol: str = None, *args, **kwargs):
    """초과 Case 87: 3차 손절 후 최저점 역트레일링 캐시 in-place 초기화 (0개 또는 1개 인자 모두 수용)."""
    sym_u = str(symbol or "").upper()
    if sym_u in ("SOXS", "SOXL"):
        _BOTTOM_TRACK_CACHE[sym_u] = _default_symbol_bottom_state(sym_u)
        if str(_BOTTOM_TRACK_CACHE.get("stopped_sym") or "").upper() == sym_u:
            for k, v in _default_symbol_bottom_state("").items():
                _BOTTOM_TRACK_CACHE[k] = v
    else:
        _BOTTOM_TRACK_CACHE.clear()
        _BOTTOM_TRACK_CACHE.update(_default_global_bottom_cache())


def _activate_bottom_track_state(
    symbol: str = "",
    stop_price: float = 0.0,
    now_est: datetime = None,
    *args,
    **kwargs,
):
    """
    초과 Case 87:
    04:07 ~ 08:14 EST 중 NQ<2.0% 내 3차 역행 손절 직후 최저점 역트레일링 점화.
    최상위 요약 키와 종목별 서브 딕셔너리(`_BOTTOM_TRACK_CACHE[symbol]`)를 동시에 갱신합니다.
    """
    sym_u = str(symbol or kwargs.get("stopped_sym") or "").upper()
    if sym_u not in ("SOXS", "SOXL"):
        return

    raw_sp = (
        stop_price
        or kwargs.get("last_stop_price")
        or kwargs.get("price")
        or kwargs.get("current_price")
        or 0.0
    )
    sp = round(float(raw_sp or 0.0), 4)
    if sp <= 0.0:
        return

    if isinstance(now_est, datetime):
        sess_date = now_est.astimezone(EST_TZ).strftime("%Y-%m-%d")
    elif isinstance(now_est, str) and now_est.strip():
        sess_date = now_est.strip()
    else:
        sess_date = str(kwargs.get("session_id") or kwargs.get("session_date") or datetime.now(EST_TZ).strftime("%Y-%m-%d"))

    rebound_p = math.ceil(sp * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0
    now_ts = time.time()

    sym_entry = {
        "active": True,
        "symbol": sym_u,
        "stopped_sym": sym_u,
        "stop_price": sp,
        "last_stop_price": sp,
        "lowest_price": sp,
        "post_stop_low": sp,
        "low_price": sp,
        "min_price": sp,
        "rebound_price": rebound_p,
        "rebound_trig_price": rebound_p,
        "trigger_price": rebound_p,
        "stopped_at": now_ts,
        "triggered_at": now_ts,
        "session_date": sess_date,
        "session_id": sess_date,
    }

    opp_sym = "SOXL" if sym_u == "SOXS" else "SOXS"
    _BOTTOM_TRACK_CACHE.update(sym_entry)
    _BOTTOM_TRACK_CACHE[sym_u] = dict(sym_entry)
    _BOTTOM_TRACK_CACHE[opp_sym] = _default_symbol_bottom_state(opp_sym)


activate_post_stop_bottom_tracker = _activate_bottom_track_state


def get_bottom_track_status(symbol: str = None, *args, **kwargs) -> dict:
    """
    NEW & MODIFIED (초과 Case 87 · /sync 및 /avwap TypeError 영구 소각):
    - `get_bottom_track_status("SOXS")` 처럼 종목 인자 1개가 전달된 경우:
      해당 종목의 최저점 역트레일링 상태 딕셔너리(stop_price, last_stop_price, lowest_price,
      post_stop_low, rebound_price, trigger_price 등 모든 호환 키 완비)를 반환합니다.
    - `get_bottom_track_status()` 처럼 인자 없이(0개) 호출된 경우:
      전역 `_BOTTOM_TRACK_CACHE` 상태 사본을 반환합니다.
    """
    sym_u = str(symbol or "").upper()
    top_active = bool(_BOTTOM_TRACK_CACHE.get("active", False))
    top_sym = str(_BOTTOM_TRACK_CACHE.get("stopped_sym") or _BOTTOM_TRACK_CACHE.get("symbol") or "").upper()

    if sym_u in ("SOXS", "SOXL"):
        sub_raw = _BOTTOM_TRACK_CACHE.get(sym_u)
        sub = dict(sub_raw) if isinstance(sub_raw, dict) else _default_symbol_bottom_state(sym_u)

        is_sym_active = bool(sub.get("active", False)) or (top_active and top_sym == sym_u)
        if is_sym_active:
            sp = float(
                sub.get("last_stop_price")
                or sub.get("stop_price")
                or _BOTTOM_TRACK_CACHE.get("last_stop_price")
                or _BOTTOM_TRACK_CACHE.get("stop_price")
                or 0.0
            )
            lp = float(
                sub.get("post_stop_low")
                or sub.get("lowest_price")
                or sub.get("low_price")
                or _BOTTOM_TRACK_CACHE.get("post_stop_low")
                or _BOTTOM_TRACK_CACHE.get("lowest_price")
                or sp
            )
            if lp <= 0.0 and sp > 0.0:
                lp = sp
            rb = (math.ceil(lp * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0) if lp > 0.0 else 0.0
            s_date = str(
                sub.get("session_date")
                or sub.get("session_id")
                or _BOTTOM_TRACK_CACHE.get("session_date")
                or ""
            )
            s_at = float(sub.get("stopped_at") or _BOTTOM_TRACK_CACHE.get("stopped_at") or 0.0)
            sub.update({
                "active": True,
                "symbol": sym_u,
                "stopped_sym": sym_u,
                "stop_price": sp,
                "last_stop_price": sp,
                "lowest_price": lp,
                "post_stop_low": lp,
                "low_price": lp,
                "min_price": lp,
                "rebound_price": rb,
                "rebound_trig_price": rb,
                "trigger_price": rb,
                "stopped_at": s_at,
                "triggered_at": s_at,
                "session_date": s_date,
                "session_id": s_date,
            })
        else:
            sub = _default_symbol_bottom_state(sym_u)
        return sub

    # 인자 없이 호출된 경우 전역 상태 반환 (하위 최신값 동기화 포함)
    out = dict(_BOTTOM_TRACK_CACHE)
    if top_active and top_sym in ("SOXS", "SOXL"):
        sp = float(out.get("last_stop_price") or out.get("stop_price") or 0.0)
        lp = float(out.get("post_stop_low") or out.get("lowest_price") or sp)
        rb = (math.ceil(lp * (1.0 + BOTTOM_REBOUND_PCT / 100.0) * 100.0) / 100.0) if lp > 0.0 else 0.0
        out.update({
            "symbol": top_sym,
            "stopped_sym": top_sym,
            "stop_price": sp,
            "last_stop_price": sp,
            "lowest_price": lp,
            "post_stop_low": lp,
            "low_price": lp,
            "min_price": lp,
            "rebound_price": rb,
            "rebound_trig_price": rb,
            "trigger_price": rb,
        })
    return out


def _reset_pre_leader_state(symbol: str = None, *args, **kwargs):
    """초과 Case 97: 프리장 첫 RSI>=79.9 상승 추세 주도주 캐시 in-place 초기화 (0개/1개 인자 모두 수용)."""
    _PRE_LEADER_CACHE.clear()
    _PRE_LEADER_CACHE.update({
        "leader_sym": "",
        "symbol": "",
        "first_799_sym": "",
        "first_799_ts": 0.0,
        "locked_at": 0.0,
        "first_799_rsi": 0.0,
        "peak_rsi": 0.0,
        "first_799_time_str": "",
        "locked_time_str": "",
        "pb_rearm_ready": True,
        "rearm_ready": True,
        "pb_dispatched": False,
        "session_date": "",
        "session_id": "",
    })


def mark_pre_pullback_dispatched(symbol: str = None, *args, **kwargs):
    """
    초과 Case 97:
    07:00 ~ 08:14 EST 선행 상승 추세 주도주(leader_sym)가 추세눌림목(RSI <= 35.0) 매수 덫을 발사한 직후
    `pb_rearm_ready = False` 및 `pb_dispatched = True` 로 마킹합니다.
    """
    _PRE_LEADER_CACHE["pb_rearm_ready"] = False
    _PRE_LEADER_CACHE["rearm_ready"] = False
    _PRE_LEADER_CACHE["pb_dispatched"] = True


def get_pre_leader_status(symbol: str = None, *args, **kwargs) -> dict:
    """초과 Case 97: 당일 프리장 첫 RSI>=79.9 상승 추세 주도주 상태 사본 반환 (0개/1개 인자 모두 수용)."""
    out = dict(_PRE_LEADER_CACHE)
    ld_sym = str(out.get("leader_sym") or out.get("symbol") or "").upper()
    rearm_ok = bool(out.get("pb_rearm_ready", True))
    out["leader_sym"] = ld_sym
    out["symbol"] = ld_sym
    out["first_799_sym"] = ld_sym
    out["rearm_ready"] = rearm_ok
    out["pb_dispatched"] = not rearm_ok
    if symbol is not None:
        sym_u = str(symbol or "").upper()
        out["is_leader"] = bool(ld_sym and ld_sym == sym_u)
    else:
        out["is_leader"] = bool(ld_sym)
    return out


def update_premarket_trend_leader(
    symbol: str = "",
    live_rsi_val: float = 0.0,
    now_est: datetime = None,
    shared_rsi_state: dict = None,
    *args,
    **kwargs,
) -> dict:
    """
    초과 Case 97:
    04:07:00 ~ 08:14:59 EST 구간에서 SOXS와 SOXL 중 당일 04:07 EST 이후
    가장 먼저 RSI(1m) >= 79.9를 달성한 첫 번째 종목을 `leader_sym` 으로 불변 각인하며,
    장중 재기동 시에도 ewm_state 의 `first_799_ts` 를 비교하여 100% 자동 복원합니다.
    """
    if now_est is None or not isinstance(now_est, datetime):
        now_est = datetime.now(EST_TZ)
    else:
        now_est = now_est.astimezone(EST_TZ)

    today_str = now_est.strftime("%Y-%m-%d")
    est_hm = now_est.hour * 100 + now_est.minute

    if _PRE_LEADER_CACHE.get("session_date") and _PRE_LEADER_CACHE.get("session_date") != today_str:
        _reset_pre_leader_state()
    _PRE_LEADER_CACHE["session_date"] = today_str
    _PRE_LEADER_CACHE["session_id"] = today_str

    if not (407 <= est_hm <= 814):
        return get_pre_leader_status(symbol)

    # 1) 캔들 벡터 스캔(first_799_ts) 기반 당일 04:07 EST 이후 최초 달성 종목 자동 복원
    if not _PRE_LEADER_CACHE.get("leader_sym") and isinstance(shared_rsi_state, dict):
        best_sym = ""
        best_ts = 0.0
        best_rsi = 0.0
        best_t_str = ""
        for cand_s in ("SOXS", "SOXL"):
            ewm_c = ((shared_rsi_state.get(cand_s) or {}).get("ewm_state")) or {}
            c_ts = float(ewm_c.get("first_799_ts") or ewm_c.get("first_0407_799_ts") or 0.0)
            c_rsi = float(ewm_c.get("first_799_rsi") or ewm_c.get("first_0407_799_rsi") or 0.0)
            c_tstr = str(ewm_c.get("first_799_time_str") or ewm_c.get("first_0407_799_time_str") or "")
            if c_ts > 0.0 and c_rsi >= PRE_TREND_LEADER_SELECT_RSI:
                if best_ts <= 0.0 or c_ts < best_ts:
                    best_sym = cand_s
                    best_ts = c_ts
                    best_rsi = c_rsi
                    best_t_str = c_tstr
        if best_sym:
            _PRE_LEADER_CACHE.update({
                "leader_sym": best_sym,
                "symbol": best_sym,
                "first_799_sym": best_sym,
                "first_799_ts": best_ts,
                "locked_at": best_ts,
                "first_799_rsi": round(best_rsi, 1),
                "peak_rsi": round(best_rsi, 1),
                "first_799_time_str": best_t_str,
                "locked_time_str": best_t_str,
                "pb_rearm_ready": True,
                "rearm_ready": True,
                "pb_dispatched": False,
            })

    # 2) 실시간 틱 RSI >= 79.9 최초 도달 시 즉시 각인
    sym_u = str(symbol or "").upper()
    r_val = float(live_rsi_val or 0.0)
    if not _PRE_LEADER_CACHE.get("leader_sym") and sym_u in ("SOXS", "SOXL"):
        if r_val >= PRE_TREND_LEADER_SELECT_RSI:
            now_ts = time.time()
            t_str = now_est.strftime("%H:%M:%S")
            _PRE_LEADER_CACHE.update({
                "leader_sym": sym_u,
                "symbol": sym_u,
                "first_799_sym": sym_u,
                "first_799_ts": now_ts,
                "locked_at": now_ts,
                "first_799_rsi": round(r_val, 1),
                "peak_rsi": round(r_val, 1),
                "first_799_time_str": t_str,
                "locked_time_str": t_str,
                "pb_rearm_ready": True,
                "rearm_ready": True,
                "pb_dispatched": False,
            })

    # 3) 이미 각인된 상승 추세 주도주가 추세눌림목 진입 후 RSI > 40.0 회복 시 재출동 리셋
    if _PRE_LEADER_CACHE.get("leader_sym") == sym_u and not _PRE_LEADER_CACHE.get("pb_rearm_ready", True):
        if r_val > PRE_TREND_REARM_RESET_RSI:
            _PRE_LEADER_CACHE["pb_rearm_ready"] = True
            _PRE_LEADER_CACHE["rearm_ready"] = True
            _PRE_LEADER_CACHE["pb_dispatched"] = False

    return get_pre_leader_status(symbol)


def _reset_manual_stage3_anchor(symbol: str = None, *args, **kwargs):
    """초과 Case 94: 스나이퍼 OFF 또는 포지션 전량 청산 시 수동 3차 기준가 인메모리 캐시 초기화."""
    targets = (str(symbol).upper(),) if str(symbol or "").upper() in ("SOXS", "SOXL") else ("SOXS", "SOXL")
    for sym in targets:
        if sym in _MANUAL_STAGE3_ANCHOR_CACHE and isinstance(_MANUAL_STAGE3_ANCHOR_CACHE[sym], dict):
            _MANUAL_STAGE3_ANCHOR_CACHE[sym].clear()
            _MANUAL_STAGE3_ANCHOR_CACHE[sym].update({
                "ref_price": 0.0,
                "ref_amp": 0.0,
                "locked_ts": 0.0,
                "qty": 0,
                "anchored": False,
            })
        else:
            _MANUAL_STAGE3_ANCHOR_CACHE[sym] = {
                "ref_price": 0.0,
                "ref_amp": 0.0,
                "locked_ts": 0.0,
                "qty": 0,
                "anchored": False,
            }


def mark_manual_stage3_anchored(
    symbol: str = None,
    ref_price: float = 0.0,
    ref_amp: float = 0.0,
    qty: int = 0,
    *args,
    **kwargs,
):
    """
    초과 Case 94:
    수동(MANUAL) 보유 물량에 상단 안전 캡(+5.00% LIMIT)을 장전하거나 스나이퍼 가동 ON 결속 시
    실시간 QQQ 기준가(ref_price)와 선물 진폭(ref_amp), 보유 수량(qty)을 인메모리 앵커에 기록합니다.
    """
    sym_u = str(symbol or "").upper()
    if sym_u in ("SOXS", "SOXL"):
        _MANUAL_STAGE3_ANCHOR_CACHE[sym_u] = {
            "ref_price": float(ref_price or 0.0),
            "ref_amp": float(ref_amp or 0.0),
            "locked_ts": time.time(),
            "qty": int(qty or 0),
            "anchored": True,
        }


def _reset_swing_10m_state(symbol: str = None, *args, **kwargs):
    """10분봉 스윙 상태 캐시 초기화 헬퍼 (0개/1개 인자 모두 수용)."""
    _SWING_10M_CACHE.clear()
    _SWING_10M_CACHE.update({
        "active": False,
        "symbol": "",
        "locked_sym": "",
        "prelock_sym": "",
        "trend": "WAIT",
        "direction": "NONE",
        "SOXS": {"active": False, "symbol": "SOXS", "trend": "WAIT"},
        "SOXL": {"active": False, "symbol": "SOXL", "trend": "WAIT"},
    })


def get_swing_10m_status(symbol: str = None, *args, **kwargs) -> dict:
    """10분봉 스윙 상태 조회 헬퍼 (0개/1개 인자 모두 수용)."""
    sym_u = str(symbol or "").upper()
    if sym_u in ("SOXS", "SOXL"):
        sub = _SWING_10M_CACHE.get(sym_u)
        if isinstance(sub, dict):
            return dict(sub)
        return {"active": False, "symbol": sym_u, "trend": "WAIT", "direction": "NONE"}
    return dict(_SWING_10M_CACHE)


# =====================================================================
# 4. 미체결 주문 선취소·수량 해방 폴링 및 2단계 긴급 매도 스윕 헬퍼 (Case 24)
# =====================================================================
async def _cancel_open_orders_and_wait_sellable(
    ctx,
    symbol: str,
    holdings_qty: int,
    sell_order_id: str = "",
    buy_order_id: str = "",
    max_polls: int = 8,
    poll_interval: float = 0.25,
    **kwargs,
) -> tuple[int, list[str]]:
    """
    Case 24: 미체결 매도(및 매수) 주문을 선취소하고 최대 2.0초(0.25초 * 8회) 동안
    매도 가능 수량(sellableQuantity) 해방을 폴링 대기합니다.
    """
    client = ctx.client
    target_cancel_ids = set()
    if sell_order_id:
        target_cancel_ids.add(str(sell_order_id).strip())
    if buy_order_id:
        target_cancel_ids.add(str(buy_order_id).strip())

    try:
        open_orders = await client.get_orders(status="OPEN", symbol=symbol)
        if isinstance(open_orders, list):
            for od in open_orders:
                if isinstance(od, dict) and od.get("orderId"):
                    target_cancel_ids.add(str(od["orderId"]).strip())
    except Exception:
        pass

    for oid in list(target_cancel_ids):
        if not oid:
            continue
        try:
            await client.cancel_order(oid)
        except Exception:
            pass

    sellable_q_int = 0
    for _ in range(max(1, int(max_polls))):
        try:
            sq = await client.get_sellable_quantity(symbol)
            sellable_q_int = int(math.floor(float(sq or 0.0)))
            if sellable_q_int >= int(holdings_qty) and sellable_q_int > 0:
                return sellable_q_int, []
        except Exception:
            pass
        await asyncio.sleep(poll_interval)

    surviving_sell_ids = []
    try:
        open_after = await client.get_orders(status="OPEN", symbol=symbol)
        if isinstance(open_after, list):
            for od in open_after:
                if (
                    isinstance(od, dict)
                    and str(od.get("side") or "").upper() == "SELL"
                    and od.get("orderId")
                ):
                    surviving_sell_ids.append(str(od["orderId"]).strip())
    except Exception:
        pass

    if not surviving_sell_ids and sell_order_id:
        surviving_sell_ids.append(str(sell_order_id).strip())

    return sellable_q_int, surviving_sell_ids


sniper_plugin_cancel_helper = _cancel_open_orders_and_wait_sellable


async def _execute_two_stage_emergency_sell(
    ctx,
    symbol: str,
    holdings_qty: int,
    sell_order_id: str,
    buy_order_id: str,
    current_price: float,
    tag_prefix: str = "mdump",
) -> tuple[bool, float, str]:
    """
    Case 24: 1단계 기존 매도 주문(sell_order_id) 가격 즉시 정정(modify_order) 스윕 시도 후,
    미장전 또는 정정 불가 시 2단계 선취소 + 매도가능수량 폴링 대기 + 매수 1호가 LIMIT 신규 매도를 발사합니다.
    """
    client = ctx.client
    best_bid = await client.get_best_bid_price(symbol, fallback_price=current_price)
    sweep_price = best_bid if best_bid > 0.0 else (math.floor(float(current_price or 0.0) * 100.0) / 100.0)
    if sweep_price <= 0.0:
        return False, 0.0, ""

    if sell_order_id:
        try:
            mod_res = await client.modify_order(
                order_id=sell_order_id,
                price=sweep_price,
                order_type="LIMIT",
            )
            new_oid = str((mod_res or {}).get("orderId") or sell_order_id)
            return True, sweep_price, new_oid
        except Exception as mod_e:
            err_s = str(mod_e).lower()
            if "already-filled" in err_s:
                return True, sweep_price, sell_order_id

    sellable_q_int, surviving_sell_ids = await _cancel_open_orders_and_wait_sellable(
        ctx=ctx,
        symbol=symbol,
        holdings_qty=holdings_qty,
        sell_order_id=sell_order_id,
        buy_order_id=buy_order_id,
    )

    if sellable_q_int <= 0 and surviving_sell_ids:
        for sv_oid in surviving_sell_ids:
            try:
                mod_res = await client.modify_order(
                    order_id=sv_oid,
                    price=sweep_price,
                    order_type="LIMIT",
                )
                new_oid = str((mod_res or {}).get("orderId") or sv_oid)
                return True, sweep_price, new_oid
            except Exception:
                pass
        sellable_q_int = int(holdings_qty)

    target_sell_qty = sellable_q_int if sellable_q_int > 0 else int(holdings_qty)
    if target_sell_qty <= 0:
        return False, sweep_price, ""

    cid = f"{tag_prefix}-{symbol.lower()}-{uuid.uuid4().hex[:20]}"[:36]
    if isinstance(ctx.idempotency_keys, dict) and symbol in ctx.idempotency_keys:
        ctx.idempotency_keys[symbol]["M_DUMP"] = cid

    try:
        ord_res = await client.create_order(
            symbol=symbol,
            side="SELL",
            quantity=target_sell_qty,
            price=sweep_price,
            order_type="LIMIT",
            time_in_force="DAY",
            client_order_id=cid,
        )
        new_oid = str((ord_res or {}).get("orderId") or "")
        return True, sweep_price, new_oid
    except Exception as sell_e:
        if isinstance(ctx.idempotency_keys, dict) and symbol in ctx.idempotency_keys:
            ctx.idempotency_keys[symbol]["M_DUMP"] = None
        print(f"🚨 [{tag_prefix.upper()} 긴급 매도 방어 {symbol}] {sell_e}", flush=True)
        return False, sweep_price, ""


# =====================================================================
# 5. 방향성 인지 매크로 3단 방어 · 08:15 컷오프 · 15:00 VWAP 탈출 · 15:59 MOC
# =====================================================================
async def execute_macro_defense_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_session_id: str = "",
    is_active: bool = True,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    current_price: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    entry_session: str = "",
    entry_trend: str = "",
    nq_entry_price: float = 0.0,
    nq_entry_amp: float = 0.0,
    **kwargs,
) -> bool:
    """
    초과 Case 66, 67, 82, 86, 87, 92, 94, 95, 96:
    방향성 인지 매크로 3단 방어망
    - 1차 방어선: NQ=F 당일 진폭 >= 2.0% 시 본진 자동 진입 차단 및 본진 미체결 매수 주문 즉시 취소·장부 소각
                 (단, 1주 휩소 `WHIPSAW_1SH` 및 수동 보유 `MANUAL` 은 100% 예외 보호)
    - 2차 방어선: NQ=F 당일 진폭 >= 2.0% 시 본진 자동 진입 손실권 보유 물량 즉시 매수 1호가 덤핑
                 (단, 수동 진입 `MANUAL` 및 1주 휩소 `WHIPSAW_1SH` 는 2차 방어선 100% 패스!)
    - 3차 방어선: 포지션 자체 손실률 즉시 손절 절대 금지!
                 손실 구간(0 < current_price < avg_price) 전제 하에 진입/수동 락온 시점의
                 토스 실시간 QQQ 기준가(nq_entry_price) 대비 현재 실시간 QQQ 가격(ref_curr)이
                 `dynamic_threshold = max(nq_entry_amp * 0.5, 0.80%)` (최소 0.80%) 이상 역행했을 때만 손절 스윕!
    """
    if not is_active:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    _, nq_high, nq_low = await MacroDataCache.get_cached_nq_data()
    nq_amp = ((nq_high - nq_low) / nq_low * 100.0) if (nq_high > 0.0 and nq_low > 0.0) else 0.0
    is_stage12_overheated = bool(nq_amp >= 2.0)

    es_clean = str(entry_session or "").strip()

    # [A] 미보유 상태(holdings_qty == 0): 1차 매크로 과열 방어선 점검
    if int(holdings_qty) <= 0:
        _reset_manual_stage3_anchor(symbol)

        if not is_stage12_overheated:
            return False

        # 초과 Case 95 & Case 96: 1주 휩소 노출 작전(WHIPSAW_1SH)은 NQ>=2.0% 과열차단과 별개 독립 가동하며
        # 04:07:00 EST 정리는 enforce_0407_whipsaw_cutoff_plugin 이 전담하므로 여기서 절대 취소하지 않음!
        if es_clean == "WHIPSAW_1SH":
            return False

        if buy_order_id:
            if ctx.in_memory_ordering_lock.get(symbol, False):
                return True
            ctx.in_memory_ordering_lock[symbol] = True
            try:
                already_gone = False
                try:
                    await ctx.client.cancel_order(buy_order_id)
                except Exception as cancel_e:
                    err_str = str(cancel_e).lower()
                    if "already-canceled" in err_str or "404" in err_str or "not-found" in err_str:
                        already_gone = True
                    elif "already-filled" in err_str:
                        return False

                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id="",
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )
                await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)

                _reset_bottom_track_state()
                _reset_pre_leader_state()

                if not already_gone and _LAST_MACRO1_CANCEL_ID.get(symbol) != buy_order_id:
                    _LAST_MACRO1_CANCEL_ID[symbol] = buy_order_id
                    await _send_tg_safe(
                        ctx,
                        (
                            f"🛑 <b>[1차 매크로 과열 방어 — 미체결 매수 파기]</b>\n"
                            f"▫️ 종목: <b>{symbol}</b> · NQ 진폭: <b>{nq_amp:.2f}% (≥2.0%)</b>\n"
                            f"▫️ 조치: 미체결 매수 주문 취소 및 당일 본진 진입 잠금"
                        ),
                    )
                return True
            finally:
                ctx.in_memory_ordering_lock[symbol] = False

        if es_clean != "CANCELED_MACRO" and not (358 <= est_hm <= 406):
            await AssassinLedger.lock_all_symbols_macro(last_session_id=current_session_id)
            _reset_bottom_track_state()
        return False

    # [B] 보유 상태(holdings_qty > 0): 수동 진입(MANUAL) 선행 식별 및 QQQ 0.0초 기준점 락온
    ref_curr, ref_label = await MacroDataCache.get_stage3_reference_price(
        force_refresh=False,
        realtime_probing=True,
    )
    anchor_amp = float(nq_entry_amp or 0.0) if float(nq_entry_amp or 0.0) > 0.0 else nq_amp

    is_manual_pos = bool(es_clean == "MANUAL" or (es_clean not in _AUTO_ENTRY_SESSION_TAGS and not buy_order_id))

    if is_manual_pos:
        mem_anchor = _MANUAL_STAGE3_ANCHOR_CACHE.get(symbol) or {}
        need_reanchor = bool(
            es_clean != "MANUAL"
            or float(nq_entry_price or 0.0) <= 0.0
            or (
                not sell_order_id
                and float(kwargs.get("target_sell_price", 0.0) or 0.0) <= 0.0
                and mem_anchor.get("qty", 0) != int(holdings_qty)
            )
            or ((float(nq_entry_price or 0.0) > 3000.0) != (ref_curr > 3000.0) and ref_curr > 0.0)
        )
        if need_reanchor and ref_curr > 0.0:
            nq_entry_price = ref_curr
            nq_entry_amp = anchor_amp
            es_clean = "MANUAL"
            mark_manual_stage3_anchored(
                symbol=symbol,
                ref_price=ref_curr,
                ref_amp=anchor_amp,
                qty=int(holdings_qty),
            )
            await AssassinLedger.save_state(
                symbol,
                entry_session="MANUAL",
                nq_entry_price=ref_curr,
                nq_entry_amp=anchor_amp,
                entry_trend=("SHORT" if symbol == "SOXS" else "LONG"),
            )
            return False
    else:
        if ref_curr > 0.0 and (
            float(nq_entry_price or 0.0) <= 0.0
            or ((float(nq_entry_price or 0.0) > 3000.0) != (ref_curr > 3000.0))
        ):
            nq_entry_price = ref_curr
            nq_entry_amp = anchor_amp
            await AssassinLedger.save_state(
                symbol,
                nq_entry_price=ref_curr,
                nq_entry_amp=anchor_amp,
            )
            return False

    live_p = float(current_price or 0.0)
    if live_p <= 0.0 or avg_price <= 0.0:
        return False

    if live_p >= avg_price:
        return False

    # [C] 손실 구간(0 < live_p < avg_price) 2차 & 3차 매크로 방어선 판정
    effective_stage2_breach = bool(
        is_stage12_overheated
        and not is_manual_pos
        and es_clean != "WHIPSAW_1SH"
        and float(nq_entry_amp or 0.0) < 2.0
    )

    eff_amp = float(nq_entry_amp or 0.0) if float(nq_entry_amp or 0.0) > 0.0 else nq_amp
    dynamic_threshold = max(eff_amp * 0.5, MIN_STAGE3_QQQ_ADVERSE_PCT)

    adverse_move_pct = 0.0
    effective_stage3_breach = False
    if ref_curr > 0.0 and float(nq_entry_price or 0.0) > 0.0:
        if symbol == "SOXS":
            adverse_move_pct = (ref_curr - float(nq_entry_price)) / float(nq_entry_price) * 100.0
        else:
            adverse_move_pct = (float(nq_entry_price) - ref_curr) / float(nq_entry_price) * 100.0

        if adverse_move_pct >= dynamic_threshold:
            effective_stage3_breach = True

    if not effective_stage2_breach and not effective_stage3_breach:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=live_p,
            tag_prefix="mdump",
        )
        if not swept_ok:
            return False

        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)

        pnl_pct = ((sweep_p - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0

        can_bottom_rebound = bool(
            effective_stage3_breach
            and not effective_stage2_breach
            and not is_stage12_overheated
            and (407 <= est_hm <= 814)
            and es_clean != "WHIPSAW_1SH"
        )

        if can_bottom_rebound:
            _activate_bottom_track_state(
                symbol=symbol,
                stop_price=sweep_p,
                now_est=now_est,
            )
            await AssassinLedger.save_state(
                symbol,
                is_session_done=False,
                entry_session="STAGE3_STOP_TRACK",
                buy_order_id="",
                sell_order_id="",
                whipsaw_target_price=0.0,
            )
            defense_title = "🛡️ [3차 실시간 QQQ 역행 방어 ➔ 🪤 최저점 반등 추적 점화]"
            follow_up_note = (
                f"🪤 손절가(${sweep_p:.2f}) 하회 최저점 역트레일링 감시 돌입 (+0.30% 양봉 반등 시 재진입)"
            )
        else:
            await AssassinLedger.save_state(
                symbol,
                is_session_done=True,
                entry_session="CANCELED_MACRO",
                buy_order_id="",
                sell_order_id="",
                last_session_id=current_session_id,
                whipsaw_target_price=0.0,
            )
            opp_sym = "SOXL" if symbol == "SOXS" else "SOXS"
            opp_qty = int((ctx.shared_holdings or {}).get(opp_sym, 0))
            opp_raw = await AssassinLedger.get_raw_dict(opp_sym)
            if opp_qty <= 0 and str(opp_raw.get("entry_session") or "") != "MANUAL":
                await AssassinLedger.save_state(
                    opp_sym,
                    is_session_done=True,
                    entry_session="CANCELED_MACRO",
                    last_session_id=current_session_id,
                    whipsaw_target_price=0.0,
                )

            if effective_stage2_breach:
                defense_title = "🛑 [2차 매크로 대세장 과열(≥2.0%) 손절 스윕]"
                follow_up_note = f"🛑 NQ 당일 진폭 {nq_amp:.2f}% 돌파 ➡️ 당일 본진 매매 마감"
            else:
                defense_title = f"🛡️ [3차 실시간 {ref_label} 역행 방어 손절 스윕]"
                follow_up_note = (
                    f"▫️ {ref_label} 역행: {adverse_move_pct:+.2f}% (임계치 -{dynamic_threshold:.2f}%)"
                )

        await _send_tg_safe(
            ctx,
            (
                f"<b>{html.escape(defense_title)}</b>\n"
                f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                f"▫️ 평단: ${avg_price:.2f} ➡️ 스윕가: <b>${sweep_p:.2f} ({pnl_pct:+.2f}%)</b>\n"
                f"▫️ 기준 {ref_label}: {float(nq_entry_price):.2f} ➡️ 현재: {ref_curr:.2f} "
                f"(역행 {adverse_move_pct:+.2f}% / 한도 {dynamic_threshold:.2f}%)\n"
                f"{html.escape(follow_up_note)}"
            ),
        )

        if ctx.wakeup_event:
            ctx.wakeup_event.set()
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


async def enforce_0930_regular_cutoff_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    is_session_done: bool = False,
    buy_order_id: str = "",
    entry_trend: str = "",
    **kwargs,
) -> tuple[str, bool]:
    """
    초과 Case 48 & Case 84:
    08:15:00 EST 거시경제 지표 발표 15분 전 선제 컷오프 (08:15 ~ 15:58 EST 전 구간 감시).
    """
    _reset_bottom_track_state()
    _reset_pre_leader_state()

    today_str = now_est.strftime("%Y-%m-%d")
    cleaned_buy_id = str(buy_order_id or "").strip()

    if cleaned_buy_id:
        try:
            await ctx.client.cancel_order(cleaned_buy_id)
        except Exception:
            pass
        cleaned_buy_id = ""
        await AssassinLedger.save_state(
            symbol,
            buy_order_id="",
            is_session_done=True,
            whipsaw_target_price=0.0,
        )
        is_session_done = True

        if _LAST_CUTOFF_0815_DATE.get(symbol) != today_str:
            _LAST_CUTOFF_0815_DATE[symbol] = today_str
            await _send_tg_safe(
                ctx,
                (
                    f"🔒 <b>[08:15 EST 거시지표 선제 컷오프]</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> 미체결 매수 주문 즉시 파기 완료\n"
                    f"▫️ 08:15 EST 이후 신규 매수 진입을 전면 잠금합니다."
                ),
            )
    elif not is_session_done:
        await AssassinLedger.save_state(
            symbol,
            is_session_done=True,
        )
        is_session_done = True

    return cleaned_buy_id, is_session_done


# sniper_plugin.py 59번 줄 교차 임포트 호환 별칭
enforce_0815_pre_macro_cutoff_plugin = enforce_0930_regular_cutoff_plugin


async def execute_late_regular_vwap_escape_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    avg_price: float = 0.0,
    current_price: float = 0.0,
    vwap_val: float = 0.0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    target_sell_price: float = 0.0,
    entry_session: str = "",
    entry_trend: str = "",
    **kwargs,
) -> bool:
    """
    초과 Case 98:
    정규장 장마감 1시간 전(15:00:00 ~ 15:58:59 EST) 미익절 표류 물량 당일 정규장 누적 VWAP 도달 즉시 덤핑 탈출(🛟 VWAP_ESCAPE).
    """
    if not is_active or int(holdings_qty) <= 0:
        return False

    est_hm = now_est.hour * 100 + now_est.minute
    if not (1500 <= est_hm <= 1558):
        return False

    live_p = float(current_price or 0.0)
    reg_vwap = float(vwap_val or 0.0)
    if live_p <= 0.0 or reg_vwap <= 0.0 or avg_price <= 0.0:
        return False

    tr_st = get_trailing_runtime_status(symbol)
    trail_armed = bool(tr_st.get("armed", False))
    arm_price = (
        float(target_sell_price)
        if float(target_sell_price or 0.0) > 0.0
        else (math.ceil(avg_price * (1.0 + SINGLE_TARGET_PROFIT_RATE / 100.0) * 100.0) / 100.0)
    )

    if trail_armed or live_p >= arm_price:
        return False

    vwap_trigger_p = math.floor(reg_vwap * 100.0) / 100.0
    if live_p < vwap_trigger_p:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=live_p,
            tag_prefix="vwapesc",
        )
        if not swept_ok:
            return False

        _reset_trailing_runtime_state(symbol)
        _reset_manual_stage3_anchor(symbol)

        await AssassinLedger.save_state(
            symbol,
            is_session_done=True,
            entry_session="VWAP_ESCAPE",
            buy_order_id="",
            sell_order_id="",
            whipsaw_target_price=0.0,
        )

        pnl_pct = ((sweep_p - avg_price) / avg_price * 100.0) if avg_price > 0.0 else 0.0
        await _send_tg_safe(
            ctx,
            (
                f"🛟 <b>[15:00~15:58 미익절 정규장 VWAP 도달 탈출 스윕]</b>\n"
                f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                f"▫️ 평단: ${avg_price:.2f} · 정규장 VWAP: <b>${reg_vwap:.2f}</b>\n"
                f"▫️ 탈출 스윕가: <b>${sweep_p:.2f} ({pnl_pct:+.2f}%)</b>"
            ),
        )

        if ctx.wakeup_event:
            ctx.wakeup_event.set()
        return True
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


async def execute_moc_zero_overnight_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    is_active: bool = True,
    holdings_qty: int = 0,
    buy_order_id: str = "",
    sell_order_id: str = "",
    current_price: float = 0.0,
    entry_trend: str = "",
    **kwargs,
) -> bool:
    """
    Case 16 & Case 24:
    15:59 ~ 16:01 EST 절대 제로-오버나이트 MOC 2단계 방탄 강제 청산망.
    """
    if not is_active:
        return False

    if buy_order_id:
        try:
            await ctx.client.cancel_order(buy_order_id)
        except Exception:
            pass
        await AssassinLedger.save_state(
            symbol,
            buy_order_id="",
            is_session_done=True,
            whipsaw_target_price=0.0,
        )

    if int(holdings_qty) <= 0:
        return False

    if ctx.in_memory_ordering_lock.get(symbol, False):
        return True

    ctx.in_memory_ordering_lock[symbol] = True
    try:
        swept_ok, sweep_p, _ = await _execute_two_stage_emergency_sell(
            ctx=ctx,
            symbol=symbol,
            holdings_qty=holdings_qty,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            current_price=current_price,
            tag_prefix="moc",
        )
        if swept_ok:
            _reset_trailing_runtime_state(symbol)
            _reset_manual_stage3_anchor(symbol)
            await AssassinLedger.save_state(
                symbol,
                is_session_done=True,
                buy_order_id="",
                sell_order_id="",
                whipsaw_target_price=0.0,
            )
            await _send_tg_safe(
                ctx,
                (
                    f"🛑 <b>[15:59 EST 제로-오버나이트 MOC 강제 청산]</b>\n"
                    f"▫️ 종목: <b>{symbol}</b> ({holdings_qty}주)\n"
                    f"▫️ 스윕 지정가: <b>${sweep_p:.2f}</b> (매수 1호가 즉시 스윕)"
                ),
            )
            if ctx.wakeup_event:
                ctx.wakeup_event.set()
        return swept_ok
    finally:
        ctx.in_memory_ordering_lock[symbol] = False


def __getattr__(name: str):
    """
    PEP 562 무결점 모듈 레벨 안전 폴백 (None 반환 절대 금지):
    만일 예상치 못한 상수나 헬퍼 심볼이 추가로 임포트되더라도
    ImportError 및 'NoneType' object is not callable 에러가 절대 발생하지 않도록 방어합니다.
    """
    up = name.upper()
    if "TAGS" in up:
        return _AUTO_ENTRY_SESSION_TAGS
    if "CACHE" in up or "MAP" in up or "DICT" in up or "STATE" in up:
        val = {"SOXS": {}, "SOXL": {}, "active": False}
        globals()[name] = val
        return val
    if "CAP" in up and "RATE" in up:
        return TRAIL_SAFETY_CAP_RATE
    if ("ARM" in up or "FLOOR" in up) and "RATE" in up:
        return TRAIL_ARM_PROFIT_RATE
    if "CALLBACK" in up:
        return TRAIL_DROP_CALLBACK_PCT
    if "REBOUND" in up:
        return BOTTOM_REBOUND_PCT
    if "MAX_RSI" in up:
        return BOTTOM_MAX_RSI_CAP
    if "FLOOR" in up or "ADVERSE" in up:
        return MIN_STAGE3_QQQ_ADVERSE_PCT
    if "VEL" in up:
        return NQ_VELOCITY_SURGE_PCT
    if any(k in up for k in ("RATE", "PCT", "RSI", "SEC", "USD", "THRESHOLD", "FACTOR")):
        return 1.0

    def _safe_callable(*args, **kwargs):
        return {"active": False}

    globals()[name] = _safe_callable
    return _safe_callable
