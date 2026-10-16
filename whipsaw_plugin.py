# =====================================================================
# FILE: whipsaw_plugin.py
# 목적: 데이장(19:00~03:57 EST) [모의] 2단계 스나이퍼(묵음 바이패스 포함) 및
#       03:58~04:07 EST (9분간) 데이장 VWAP 기반 1주 고정 '휩소 노출 작전' 전용 플러그인
# =====================================================================
# NEW: sniper_plugin.py 1,000줄 초과 방지를 위한 모듈화 분리 플러그인 (제3헌법 단일 책임 원칙 준수)
# NEW: 데이장 [모의] 스나이퍼 2단계 체결 시뮬레이션(1차 슬리피지 -0.18% 매수덫 TRAP_WAIT -> 30초 타임쉴드 HOLDING) 및 매크로 1·2·3차 모의 손절 탑재
# NEW: 03:58 ~ 03:59 EST 데이장 마감 직전 휩소 알림 100% 묵음 바이패스(Silent Bypass) 및 04:00 EST 프리장 전환 자동 정산 탑재
# NEW: 03:58 ~ 04:07 EST (9분간) 데이장 VWAP 역산 1주(quantity=1) 실전 '휩소 노출 작전' 및 04:07 EST 미체결 취소 / 보유 1주 현재가 즉시 매도 플러그인 탑재

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
PRE_SNIPER_RSI_THRESHOLD = 30.0

# 데이장 (19:00 ~ 03:57 EST) [모의] 알림 리셋 기준 목표 수익률 (+1.00% 익절가 도달 시에만 리셋)
DAY_SIM_RESET_PROFIT_RATE = 1.00
# 데이장 NQ=F 10분봉 상승추세("UP") 시 추가로 강제할 SOXL(롱) 최소 RSI 악조건 임계치 (80.0 이상)
DAY_SIM_SOXL_RSI_THRESHOLD = 80.0
_DAY_SIM_LOCK = asyncio.Lock()

# 1차 슬리피지 지정가 매수 덫 & 조건부 30초 타임쉴드 상수 (프리장 실전 및 데이장 모의 공통)
BUY_TRAP_SLIPPAGE_PCT = 0.18          # 1차 매수 덫 슬리피지 할인율 (-0.18%, 예: $43.00 -> $42.92)
BUY_TRAP_MIN_DISC_USD = 0.02          # 1차 매수 덫 최소 할인 폭 (-$0.02 보장)
BUY_TRAP_REARM_WAIT_SEC = 30.0        # RSI <= 30.0 유지 시 1차 매수 덫 최대 타임쉴드 대기 시간 (30.0초)

# 03:58 ~ 04:07 EST (9분간) 실전 1주 고정 '휩소 노출 작전' 상수
WHIPSAW_FIXED_QTY = 1                 # 시드 무관 무조건 1주 고정 테스트
WHIPSAW_PROFIT_FACTOR = 1.01          # VWAP 기준 1.00% 역산/상승 팩터 (vwap = 1.01 * trap_price)

# 데이장 [모의] 2단계 추격 및 03:58~04:07 1주 휩소 노출 작전 인메모리 상태 캐시
_WHIPSAW_SIM_CACHE = {
    "day_logical_date": "",
    "day_sim_active": False,
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
    "day_sim_nq_trend": "",
    "day_sim_soxl_rsi": 0.0,
    "day_sim_nq_entry_price": 0.0,    # 모의 진입 시점 NQ=F 지수 (매크로 3차 방어용)
    "day_sim_nq_entry_amp": 0.0,      # 모의 진입 시점 NQ=F 당일 진폭
    # 03:58 ~ 04:07 EST 1주 휩소 노출 작전 런타임 스냅샷
    "whipsaw_date": "",               # 당일 휩소 작전 수행 논리적 날짜 (1일 1회 제한)
    "whipsaw_locked_vwap": 0.0,       # 03:58 EST 스냅샷 데이장 VWAP
    "whipsaw_buy_price": 0.0,         # 산출된 1주 매수가
    "whipsaw_sell_target": 0.0,       # 산출된 1주 목표 매도가
    "whipsaw_mode_desc": "",          # 케이스 설명 (하단 덫 vs 즉시 매수)
    "whipsaw_order_fired": False,     # 실제 토스 주문 접수 완료 여부
    "whipsaw_cutoff_done": False,     # 04:07 EST 컷오프 정리 완료 여부
    "whipsaw_last_ dump_tick": 0.0,   # 04:07 EST 보유 1주 현재가 매도 스윕 틱 간격 제어
}


def _reset_day_sim_state():
    """데이장 [모의] 추격 상태를 원자적으로 0점 초기화하는 내부 헬퍼."""
    _WHIPSAW_SIM_CACHE["day_sim_active"] = False
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
    _WHIPSAW_SIM_CACHE["day_sim_nq_trend"] = ""
    _WHIPSAW_SIM_CACHE["day_sim_soxl_rsi"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = 0.0
    _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = 0.0


def get_daymarket_sim_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 데이장 [모의] 2단계 추격 상태를 조회하기 위한 헬퍼."""
    return {
        "active": bool(_WHIPSAW_SIM_CACHE.get("day_sim_active", False)),
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
        "nq_trend": str(_WHIPSAW_SIM_CACHE.get("day_sim_nq_trend", "")),
        "soxl_rsi": float(_WHIPSAW_SIM_CACHE.get("day_sim_soxl_rsi", 0.0)),
    }


def get_whipsaw_runtime_status() -> dict:
    """UI 관제탑(/avwap, /settlement)에서 03:58 ~ 04:07 EST 1주 휩소 노출 작전 상태를 조회하기 위한 헬퍼."""
    return {
        "date": str(_WHIPSAW_SIM_CACHE.get("whipsaw_date", "")),
        "locked_vwap": float(_WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0)),
        "buy_price": float(_WHIPSAW_SIM_CACHE.get("whipsaw_buy_price", 0.0)),
        "sell_target": float(_WHIPSAW_SIM_CACHE.get("whipsaw_sell_target", 0.0)),
        "mode_desc": str(_WHIPSAW_SIM_CACHE.get("whipsaw_mode_desc", "")),
        "order_fired": bool(_WHIPSAW_SIM_CACHE.get("whipsaw_order_fired", False)),
        "cutoff_done": bool(_WHIPSAW_SIM_CACHE.get("whipsaw_cutoff_done", False)),
    }


# NEW: 03:58 EST 진입 또는 04:00 EST 프리장 전환 시 미결된 데이장 [모의] 포지션을 정리하는 함수
async def settle_daymarket_sim_before_premarket(
    ctx, symbol: str, now_est: datetime, current_price: float, silent: bool = True
):
    """
    03:58 EST (휩소 노출 작전 개시 시각) 또는 04:00 EST (프리장 개장 시각) 도달 시
    아직 종료되지 않은 데이장 [모의] 포지션이 남아 있다면 본진 스나이퍼 보호를 위해 즉시 초기화합니다.
    """
    if symbol != "SOXS" or not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
        return

    async with _DAY_SIM_LOCK:
        if not _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
            return

        phase = str(_WHIPSAW_SIM_CACHE.get("day_sim_phase", ""))
        entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_entry_price", 0.0))
        target_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_target_price", 0.0))
        min_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_min_price", 0.0))
        max_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_max_price", 0.0))
        hit_064 = bool(_WHIPSAW_SIM_CACHE.get("day_sim_hit_064", False))
        entry_t = str(_WHIPSAW_SIM_CACHE.get("day_sim_entry_time_str", "--:--:--"))

        _reset_day_sim_state()

        cur_p_fl = math.floor(current_price * 100.0) / 100.0 if current_price > 0.0 else entry_p
        print(
            f"🧹 [데이장 모의 자동마감 {symbol}] 03:58/04:00 전환 클리어 완료 (단계:{phase}, 진입:${entry_p:.2f}, 마감:${cur_p_fl:.2f})",
            flush=True,
        )

        if not silent and phase == "HOLDING" and entry_p > 0.0 and cur_p_fl > 0.0:
            gross_pct = (cur_p_fl / entry_p - 1.0) * 100.0
            net_pct = gross_pct - 0.14
            hit_064_str = "달성(✅)" if hit_064 else "미달(❌)"
            await ctx.notify_tg(
                f"🏁 <b>[데이장 모의 마감 정산] {symbol} 프리장 전환 인계 완료</b>\n"
                f"▫️ 모의 타점: 진입 <code>${entry_p:.2f}</code> (<code>{entry_t} EST</code>) ➡️ 마감 <code>${cur_p_fl:.2f}</code> (목표 <code>${target_p:.2f}</code>)\n"
                f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code> (+0.64% 경유: {hit_064_str})\n"
                f"▫️ 마감 손익률: 세전 <code>{gross_pct:+.2f}%</code> / 세후 <code>{net_pct:+.2f}%</code> (수수료 0.14% 차감)\n"
                "▫️ 후속 조치: <code>데이장 모의 캐시를 초기화하고 본진 프리장 스나이퍼로 제어권을 이관합니다.</code>"
            )


# MODIFIED: 데이장 (19:00 ~ 03:57 EST) SOXS RSI <= 30.0 [모의] 2단계 체결 시뮬레이션 & 매크로 방어 & 03:58~03:59 묵음 바이패스
async def execute_daymarket_rsi_alert_plugin(
    ctx,
    symbol: str,
    now_est: datetime,
    current_price: float,
    rsi_val: float,
    vwap_val: float = 0.0,
):
    if symbol != "SOXS":
        return

    est_hm = now_est.hour * 100 + now_est.minute
    is_day_market = (est_hm >= 1900 or est_hm <= 359)
    if not is_day_market:
        return

    # NEW: 03:58 ~ 03:59 EST 데이장 마감 2분 전 구간은 세션 전환 휩소 구간이므로
    #      03:59:59 EST 알림 폭주를 막고 본진 프리장 및 1주 휩소 작전을 보호하기 위해 100% 묵음 바이패스!
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

        state_s = await AssassinLedger.get_state(symbol)
        is_active = bool(state_s[5])
        if not is_active:
            return

        if current_price <= 0.0:
            try:
                current_price = await ctx.client.get_current_price(symbol)
            except Exception:
                current_price = 0.0
        if current_price <= 0.0:
            return

        cur_p_fl = math.floor(current_price * 100.0) / 100.0
        if cur_p_fl <= 0.0:
            return

        # =====================================================================
        # [상태 1] 데이장 [모의] 포지션 활성 상태 (TRAP_WAIT 또는 HOLDING)
        # =====================================================================
        if _WHIPSAW_SIM_CACHE.get("day_sim_active", False):
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
                        f"🛑 <b>[데이장 모의 1차 매수덫 취소] {symbol} NQ=F 과열 감지</b>\n"
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
            # [단계 1-B] HOLDING : 모의 체결 완료 후 +1.00% 익절 추격 & 매크로 2·3차 모의 손절 감시
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
            entry_nq_tr = str(_WHIPSAW_SIM_CACHE.get("day_sim_nq_trend", ""))
            entry_soxl_r = float(_WHIPSAW_SIM_CACHE.get("day_sim_soxl_rsi", 0.0))
            nq_entry_p = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_price", 0.0))
            nq_entry_a = float(_WHIPSAW_SIM_CACHE.get("day_sim_nq_entry_amp", 0.0))

            # 1) +1.00% 모의 익절 목표가 도달 확증 시 -> 익절 브리핑 타전 및 락 리셋
            if target_p > 0.0 and cur_p_fl >= target_p:
                _reset_day_sim_state()

                sim_gross_pct = ((cur_p_fl / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 1.00
                sim_net_pct = sim_gross_pct - 0.14
                spot_gross_pct = ((cur_p_fl / spot_p - 1.0) * 100.0) if spot_p > 0.0 else sim_gross_pct
                max_dd_pct = ((min_p / entry_p - 1.0) * 100.0) if entry_p > 0.0 else 0.0
                tr_badge = "상승추세[UP] 악조건통과" if entry_nq_tr == "UP" else "하락추세[DOWN] 단독포착"

                await ctx.notify_tg(
                    f"🎯 <b>[데이장 모의 익절 달성] {symbol} +1.00% 목표가 도달 ➡️ 알림 락 리셋 완료</b>\n"
                    f"▫️ 체결 방식: <code>{fill_type_s}</code>\n"
                    f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (포착 <code>${spot_p:.2f}</code> · <code>{entry_t_str} EST</code> · {tr_badge} · SOXL RSI <code>{entry_soxl_r:.1f}</code>) ➡️ 도달 <code>${cur_p_fl:.2f}</code> (목표 <code>${target_p:.2f}</code>)\n"
                    f"▫️ 경유 최저가: <code>${min_p:.2f}</code> (체결가 대비 최대역행 <code>{max_dd_pct:+.2f}%</code>)\n"
                    f"▫️ 모의 수익률: 세전 <code>+{sim_gross_pct:.2f}%</code> / 세후 순수익 <code>+{sim_net_pct:.2f}%</code> (포착가 대비 <code>+{spot_gross_pct:.2f}%</code>)\n"
                    "▫️ 후속 조치: <code>+1.00% 익절 도달이 확인되어 데이장 차기 SOXS RSI≤30 모의 감시가 재장전되었습니다.</code>"
                )
                return

            # 2) 보유 중 손실권(0 < cur_p_fl < entry_p)에서 NQ=F 매크로 2·3차 방어선 이탈 시 -> 모의 손절 정산 및 락 리셋
            if entry_p > 0.0 and 0.0 < cur_p_fl < entry_p:
                dyn_thresh = max(nq_entry_a * 0.5, ctx.nq_dynamic_floor) if nq_entry_a > 0.0 else ctx.nq_dynamic_floor
                nq_diff_pct = (((nq_c_now - nq_entry_p) / nq_entry_p * 100.0) if nq_entry_p > 0.0 else 0.0)
                is_macro_2nd = (nq_amp_now >= 2.0)
                is_macro_3rd = (nq_entry_p > 0.0 and nq_c_now > 0.0 and nq_diff_pct >= dyn_thresh)

                if is_macro_2nd or is_macro_3rd:
                    _reset_day_sim_state()
                    try:
                        ob = await ctx.client.get_orderbook(symbol)
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
                            f"진입 지수(<code>{nq_entry_p:.2f}</code>) 대비 <code>{nq_diff_pct:+.2f}%</code> 상승 역행 "
                            f"(3차 동적 한도 <code>+{dyn_thresh:.2f}%</code> 이탈)"
                        )

                    await ctx.notify_tg(
                        f"🚨 <b>[데이장 모의 매크로 손절] {symbol} 방어선 작동 ➡️ 알림 락 리셋 완료</b>\n"
                        f"▫️ 사유: {macro_reason}\n"
                        f"▫️ 모의 타점: 체결 <code>${entry_p:.2f}</code> (<code>{entry_t_str} EST</code>) ➡️ 매수1호가 손절 <code>${bid_1_p:.2f}</code>\n"
                        f"▫️ 경유 범위: 최저 <code>${min_p:.2f}</code> ~ 최고 <code>${max_p:.2f}</code> (+0.64% 경유: {hit_064_str})\n"
                        f"▫️ 모의 손익률: 세전 <code>{loss_gross_pct:+.2f}%</code> / 세후 <code>{loss_net_pct:+.2f}%</code> (수수료 0.14% 포함)\n"
                        "▫️ 후속 조치: <code>모의 손절 정산 완료 및 차기 타점 감시 락이 리셋되었습니다.</code>"
                    )
            return

        # =====================================================================
        # [상태 2] 미진입 대기 상태(day_sim_active == False)에서 SOXS RSI <= 30.0 최초 포착 시
        # =====================================================================
        if 0.0 < rsi_val <= PRE_SNIPER_RSI_THRESHOLD:
            nq_trend, nq_ema_diff, nq_ema6_str, nq_c_global, nq_h_global, nq_l_global = (
                await MacroDataCache.get_nq_sniper_signal()
            )
            nq_amp_global = (
                ((nq_h_global - nq_l_global) / nq_l_global * 100.0) if nq_l_global > 0.0 else 0.0
            )

            # 매크로 1차 방어선: 당일 NQ=F 진폭이 이미 2.0% 이상이면 과열 장세이므로 모의 진입 차단
            if nq_amp_global >= 2.0:
                return

            # 온디맨드(On-Demand)로 SOXL 실시간 현재가를 조회하여 O(1) 실시간 틱 RSI 합성
            soxl_state = (ctx.shared_rsi_state or {}).get("SOXL", {})
            soxl_fallback_rsi = float(soxl_state.get("value", 0.0)) if isinstance(soxl_state, dict) else 0.0
            soxl_ewm = soxl_state.get("ewm_state", {}) if isinstance(soxl_state, dict) else {}

            try:
                soxl_price = await ctx.client.get_current_price("SOXL")
            except Exception:
                soxl_price = 0.0

            if soxl_price > 0.0:
                soxl_live_rsi = compute_realtime_tick_rsi(
                    live_price=soxl_price,
                    ewm_state=soxl_ewm,
                    fallback_rsi=soxl_fallback_rsi,
                )
                if isinstance(soxl_state, dict):
                    soxl_state["value"] = soxl_live_rsi
            else:
                soxl_live_rsi = soxl_fallback_rsi

            # 핵심 추세 조건부 필터:
            # NQ=F 10분봉 상승추세("UP")일 때는 SOXL RSI >= 80.0 악조건 강제 / 하락추세("DOWN")일 때는 SOXS RSI <= 30.0 단독 통과
            if nq_trend == "UP" and soxl_live_rsi < DAY_SIM_SOXL_RSI_THRESHOLD:
                return

            slip_disc = max(BUY_TRAP_MIN_DISC_USD, round(cur_p_fl * (BUY_TRAP_SLIPPAGE_PCT / 100.0), 2))
            sim_trap_buy = max(0.01, math.floor((cur_p_fl - slip_disc) * 100.0) / 100.0)
            sim_tp_100 = math.ceil(cur_p_fl * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0
            sim_tp_064 = math.ceil(cur_p_fl * 1.0064 * 100.0) / 100.0
            trap_tp_100 = math.ceil(sim_trap_buy * (1.0 + DAY_SIM_RESET_PROFIT_RATE / 100.0) * 100.0) / 100.0

            _WHIPSAW_SIM_CACHE["day_sim_active"] = True
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
            _WHIPSAW_SIM_CACHE["day_sim_nq_trend"] = nq_trend
            _WHIPSAW_SIM_CACHE["day_sim_soxl_rsi"] = soxl_live_rsi
            _WHIPSAW_SIM_CACHE["day_sim_nq_entry_price"] = nq_c_global
            _WHIPSAW_SIM_CACHE["day_sim_nq_entry_amp"] = nq_amp_global

            if vwap_val <= 0.0 and ctx.shared_rsi_state and symbol in ctx.shared_rsi_state:
                vwap_val = float(ctx.shared_rsi_state[symbol].get("vwap", 0.0))

            vwap_str = f"${vwap_val:.2f}" if vwap_val > 0.0 else "집계중"

            if nq_trend == "UP":
                trend_filter_desc = (
                    f"📈 NQ 10분봉 상승추세(기준 {nq_ema6_str} 대비 {nq_ema_diff:+.2f}%) ➡️ 악조건(SOXL RSI≥80) 통과"
                )
            else:
                trend_filter_desc = (
                    f"📉 NQ 10분봉 하락추세(기준 {nq_ema6_str} 대비 {nq_ema_diff:+.2f}%) ➡️ 악조건 면제(SOXS≤30 단독)"
                )

            await ctx.notify_tg(
                f"🔔 <b>[데이장 모의 관측] {symbol} 진성 타점 포착 ➡️ 2단계 타임쉴드 &amp; +1.00% 추격 개시 (매수차단)</b>\n"
                f"▫️ 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code> (0세션 데이마켓)\n"
                f"▫️ 듀얼 1분봉 RSI: 🐻SOXS <code>{rsi_val:.1f}</code> (기준 ≤ 30.0) | 🐂SOXL <code>{soxl_live_rsi:.1f}</code>\n"
                f"▫️ 선물 추세 필터: <code>{trend_filter_desc}</code>\n"
                f"▫️ 포착 현재가: <code>${cur_p_fl:.2f}</code> | 🪤 1차 슬리피지 덫: <code>${sim_trap_buy:.2f}</code> (-0.18%) | VWAP: <code>{vwap_str}</code>\n"
                f"▫️ 30초 타임쉴드: <code>30초 내 ${sim_trap_buy:.2f} 도달 시 저가체결(목표 ${trap_tp_100:.2f}) / RSI 30초과 반등 또는 30초 만료 시 현재가체결</code>\n"
                f"▫️ NQ=F 당일 진폭: <code>{nq_amp_global:.2f}%</code> (현재지수 <code>{nq_c_global:.2f}</code>)\n"
                f"▫️ 리셋 목표가(+1.00%): <code>${sim_tp_100:.2f}</code> (참고 +0.64%: <code>${sim_tp_064:.2f}</code>)\n"
                f"▫️ 다중알림 잠금: <code>모의 익절(+1.00%) 또는 매크로 손절 도달 전까지 추가 알림이 100% 차단됩니다.</code>"
            )


# NEW: 03:58 ~ 04:06:59 EST (9분간) 데이장 누적 VWAP 기반 1주 고정 실전 '휩소 노출 작전' 플러그인
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
    [휩소 노출 작전 (03:58 ~ 04:07 EST · 9분간 유지)]
    - 데이장 마감 2분 전(03:58 EST)에 데이장 동안 형성된 VWAP 가격(day_vwap)을 스냅샷 락온합니다.
    - 케이스 1 (현재가 >= VWAP): VWAP = 1.01 * 매수덫가격 -> 매수덫 = floor((VWAP / 1.01) * 100) / 100 에 1주 매수 덫,
                               체결 시 목표 매도 덫 = ceil(VWAP * 100) / 100
    - 케이스 2 (현재가 <  VWAP): 현재가로 1주 즉시 매수,
                               체결 시 목표 매도 덫 = ceil((VWAP * 1.01) * 100) / 100
    - 시드(budget)와 무관하게 오직 1주(quantity=1)만 실전 주문합니다.
    - 만약 03:58 ~ 03:59 EST 장 전환 공백으로 토스 서버가 422(order-hours-closed)를 반환하면,
      03:58 EST에 락온한 VWAP 타점을 그대로 유지하여 04:00:00 EST 첫 틱에 즉시 발사합니다.
    """
    if symbol != "SOXS" or not is_active or ctx.in_memory_ordering_lock[symbol]:
        return

    est_hm = now_est.hour * 100 + now_est.minute
    if not (358 <= est_hm <= 406):
        return

    today_str = now_est.strftime("%Y-%m-%d")

    # 새로운 날짜 진입 시 휩소 노출 작전 일일 캐시 초기화
    if _WHIPSAW_SIM_CACHE.get("whipsaw_date") != today_str:
        _WHIPSAW_SIM_CACHE["whipsaw_date"] = today_str
        _WHIPSAW_SIM_CACHE["whipsaw_locked_vwap"] = 0.0
        _WHIPSAW_SIM_CACHE["whipsaw_buy_price"] = 0.0
        _WHIPSAW_SIM_CACHE["whipsaw_sell_target"] = 0.0
        _WHIPSAW_SIM_CACHE["whipsaw_mode_desc"] = ""
        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = False
        _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = False
        _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = 0.0

    # 당일 이미 휩소 1주 주문을 발사했거나, 기존 보유 물량/미체결 주문이 있는 경우 중복 발사 차단
    if _WHIPSAW_SIM_CACHE.get("whipsaw_order_fired", False):
        return
    if holdings_qty > 0 or bool(buy_order_id):
        _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
        return

    # 1단계: 03:58 EST (또는 첫 유효 틱) 에 데이장 누적 VWAP 및 주문 타점 스냅샷 락온
    if _WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0) <= 0.0:
        if day_vwap <= 0.0 or current_price <= 0.0:
            return

        cur_p_fl = math.floor(current_price * 100.0) / 100.0
        vwap_fl = round(float(day_vwap), 2)
        if cur_p_fl <= 0.0 or vwap_fl <= 0.0:
            return

        if cur_p_fl >= vwap_fl:
            # 케이스 1: 현재가 >= 데이장 VWAP -> VWAP = 1.01 * 덫가격 역산 하단 매수 덫, 매도 목표 = VWAP
            calc_buy_p = max(0.01, math.floor((vwap_fl / WHIPSAW_PROFIT_FACTOR) * 100.0) / 100.0)
            calc_sell_tp = math.ceil(vwap_fl * 100.0) / 100.0
            mode_desc = f"현재가(${cur_p_fl:.2f}) ≥ 데이장VWAP(${vwap_fl:.2f}) ➡️ 하단 1% 역산 매수덫(${calc_buy_p:.2f}) · 목표 VWAP(${calc_sell_tp:.2f})"
        else:
            # 케이스 2: 현재가 < 데이장 VWAP -> 현재가 1주 즉시 매수, 매도 목표 = VWAP * 1.01
            calc_buy_p = cur_p_fl
            calc_sell_tp = math.ceil((vwap_fl * WHIPSAW_PROFIT_FACTOR) * 100.0) / 100.0
            mode_desc = f"현재가(${cur_p_fl:.2f}) 미만 데이장VWAP(${vwap_fl:.2f}) ➡️ 현재가(${calc_buy_p:.2f}) 즉시매수 · 목표 VWAP+1%(${calc_sell_tp:.2f})"

        _WHIPSAW_SIM_CACHE["whipsaw_locked_vwap"] = vwap_fl
        _WHIPSAW_SIM_CACHE["whipsaw_buy_price"] = calc_buy_p
        _WHIPSAW_SIM_CACHE["whipsaw_sell_target"] = calc_sell_tp
        _WHIPSAW_SIM_CACHE["whipsaw_mode_desc"] = mode_desc
        print(
            f"🌪️ [휩소 노출 작전 타점 락온 {symbol}] {now_est.strftime('%H:%M:%S')} EST | {mode_desc}",
            flush=True,
        )

    target_buy_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_buy_price", 0.0))
    target_sell_p = float(_WHIPSAW_SIM_CACHE.get("whipsaw_sell_target", 0.0))
    locked_vwap = float(_WHIPSAW_SIM_CACHE.get("whipsaw_locked_vwap", 0.0))
    mode_desc_str = str(_WHIPSAW_SIM_CACHE.get("whipsaw_mode_desc", ""))

    if target_buy_p <= 0.0 or target_sell_p <= 0.0:
        return

    # 2단계: 매크로 1차 과열 방어선(NQ=F 진폭 < 2.0%) 확인 및 1주 실전 주문 발사
    async with ctx.dual_strike_mutex:
        ctx.in_memory_ordering_lock[symbol] = True
        try:
            state_chk = await AssassinLedger.get_state(symbol)
            if not bool(state_chk[5]) or state_chk[8] == "CANCELED_MACRO":
                return

            _, _, _, nq_c_now, nq_h_now, nq_l_now = await MacroDataCache.get_nq_sniper_signal()
            nq_amp_now = (((nq_h_now - nq_l_now) / nq_l_now * 100.0) if nq_l_now > 0.0 else 0.0)
            if nq_amp_now >= 2.0:
                _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                return

            hold_chk = await ctx.client.get_symbol_holdings_detail(symbol)
            if int(math.floor(hold_chk.get("qty", 0.0))) > 0:
                _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                return

            open_orders_chk = await ctx.client.get_orders(status="OPEN", symbol=symbol)
            if any(o.get("side") == "BUY" for o in open_orders_chk):
                _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                return

            # Case 65: 1주 매수 가능 USD 예수금 확인
            try:
                current_bp = await ctx.client.get_usd_buying_power()
                if current_bp < target_buy_p:
                    _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                    await ctx.notify_tg(
                        f"⚠️ <b>[휩소 노출 작전 보류] 1주 매수 예수금 부족</b>\n"
                        f"▫️ 필요 금액: <code>${target_buy_p:.2f}</code> (1주) | USD 가능금액: <code>${current_bp:,.2f}</code>"
                    )
                    return
            except Exception:
                pass

            idem_key = f"WHIP_{symbol}_{today_str.replace('-', '')}_{est_hm}"[:36]
            res = await ctx.client.create_order(
                symbol=symbol,
                side="BUY",
                order_type="LIMIT",
                quantity=WHIPSAW_FIXED_QTY,
                price=f"{target_buy_p:.2f}",
                client_order_id=idem_key,
            )

            if res and isinstance(res, dict) and res.get("result", {}).get("orderId"):
                new_buy_id = str(res["result"]["orderId"])
                _WHIPSAW_SIM_CACHE["whipsaw_order_fired"] = True
                await AssassinLedger.save_state(
                    symbol,
                    buy_order_id=new_buy_id,
                    entry_session="WHIPSAW_1SH",
                    entry_trend="SHORT",
                    entry_time=time.time(),
                    nq_entry_price=nq_c_now,
                    nq_entry_amp=nq_amp_now,
                    is_session_done=True,
                    last_session_id=current_session_id,
                    whipsaw_target_price=target_sell_p,
                )
                ctx.wakeup_event.set()
                exp_gross_pct = ((target_sell_p / target_buy_p - 1.0) * 100.0) if target_buy_p > 0.0 else 1.00
                await ctx.notify_tg(
                    f"🌪️ <b>[휩소 노출 작전 개시] {symbol} 데이장 VWAP 기반 1주 실전 테스트 발사</b>\n"
                    f"▫️ 작전 시간: <code>03:58 ~ 04:07 EST (9분간 유지 · 본진 스나이퍼 04:07 타임쉴드)</code>\n"
                    f"▫️ 데이장 VWAP: <code>${locked_vwap:.2f}</code> | 포착 시각: <code>{now_est.strftime('%H:%M:%S')} EST</code>\n"
                    f"▫️ 타점 판정: <code>{mode_desc_str}</code>\n"
                    f"▫️ 주문 내역: <b>1주 고정(LIMIT)</b> 매수가 <code>${target_buy_p:.2f}</code> ➡️ 체결 시 목표 매도덫 <code>${target_sell_p:.2f}</code> (기대 <code>+{exp_gross_pct:.2f}%</code>)\n"
                    "▫️ 04:07 타임쉴드: <code>04:07 EST 미체결 시 주문 취소 / 1주 보유 중일 시 현재가 즉시 매도 후 본진 스나이퍼 인계</code>"
                )
        except Exception as e:
            err_str = str(e)
            # 03:58 ~ 03:59 EST 토스 장전환 공백(order-hours-closed) 발생 시 에러 타전 없이 04:00:00 EST 첫 틱에 자동 재발사
            if (358 <= est_hm <= 359) and ("order-hours-closed" in err_str or "422" in err_str):
                print(
                    f"⏳ [휩소 노출 작전 대기 {symbol}] 03:58~03:59 장전환 공백 감지 -> 04:00:00 EST 정각에 락온 타점(${target_buy_p:.2f})으로 자동 발사합니다.",
                    flush=True,
                )
            else:
                print(f"🚨 [휩소 노출 작전 에러 {symbol}] {err_str}", flush=True)
        finally:
            ctx.in_memory_ordering_lock[symbol] = False


# NEW: 04:07:00 EST 정각 휩소 노출 작전 9분 타임쉴드 만료 정리 플러그인
#      (미체결 1주 매수 취소 & 데이장/휩소로 매수해 보유 중인 1주 현재가 즉시 매도 처리)
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
    """
    04:07:00 EST (est_hm >= 407) 도달 시:
    1) 휩소 노출 작전(entry_session == "WHIPSAW_1SH")의 1주 매수 주문이 미체결 상태(holdings_qty == 0)이면
       즉시 주문을 취소하고 is_session_done=False로 초기화하여 04:07 EST부터 본진 스나이퍼를 출동시킵니다.
    2) 휩소 노출 작전 또는 데이장에 매수한 1주가 아직 매도되지 않고 보유 중(holdings_qty > 0 and entry_session == "WHIPSAW_1SH")이면
       기존 1주 매도 덫을 선취소(또는 정정)하고 실시간 현재가(current_price)로 즉시 매도 주문을 집행합니다.
    """
    est_hm = now_est.hour * 100 + now_est.minute
    if symbol != "SOXS" or not is_active or not (407 <= est_hm <= 929):
        return buy_order_id, entry_session, False

    if entry_session != "WHIPSAW_1SH":
        return buy_order_id, entry_session, False

    if ctx.in_memory_ordering_lock[symbol]:
        return buy_order_id, entry_session, False

    # -----------------------------------------------------------------
    # [분기 1] 04:07 EST 도달 시점에 1주 매수 주문이 미체결(holdings_qty == 0)인 경우 -> 즉시 주문 취소 후 본진 인계
    # -----------------------------------------------------------------
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
                    # 취소 직전 간발의 차이로 체결된 경우 다음 틱에 보유 1주 현재가 매도 분기로 처리
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
            _WHIPSAW_SIM_CACHE["whipsaw_cutoff_done"] = True
            ctx.wakeup_event.set()

            if canceled_any:
                await ctx.notify_tg(
                    f"⏱️ <b>[04:07 타임쉴드 해제 {symbol}] 휩소 노출 작전 1주 미체결 매수 취소 완료</b>\n"
                    "▫️ 사유: 03:58 ~ 04:07 EST (9분간) 휩소 노출 작전 시간 만료\n"
                    "▫️ 조치: 미체결 1주 매수 주문 즉시 파기 ➡️ <b>04:07 EST 본진 프리장 스나이퍼(RSI≤30) 정식 가동 개시</b>"
                )
            return "", "", True
        finally:
            ctx.in_memory_ordering_lock[symbol] = False

    # -----------------------------------------------------------------
    # [분기 2] 04:07 EST 도달 시점에 휩소/데이장 1주가 체결되어 보유 중(holdings_qty > 0)인 경우 -> 현재가 즉시 매도!
    # -----------------------------------------------------------------
    if holdings_qty > 0:
        now_ts = time.time()
        if now_ts - float(_WHIPSAW_SIM_CACHE.get("whipsaw_last_dump_tick", 0.0)) < 1.5:
            return buy_order_id, entry_session, True

        ctx.in_memory_ordering_lock[symbol] = True
        try:
            # 혹시 잔여 미체결 매수 주문이 있다면 함께 선취소
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

            # 즉각 체결 보장을 위해 호가창 매수 1호가(Bid 1)가 존재하고 현재가보다 낮다면 매수 1호가로, 아니면 현재가로 설정
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
                    f"⏱️ <b>[04:07 타임쉴드 정리 {symbol}] 휩소 노출 작전 1주 현재가 즉시 매도 집행</b>\n"
                    f"▫️ 사유: 04:07 EST 휩소 노출 작전 종료 시각 도달 (보유 <code>{holdings_qty}주</code> 정리)\n"
                    f"▫️ 처분 단가(LIMIT): <code>${exec_sell_price:.2f}</code> (현재가/매수1호가 즉시 체결 유도)\n"
                    "▫️ 후속 조치: <code>1주 체결 완료 즉시 실수익 복리 정산 후 본진 프리장 스나이퍼(RSI≤30) 감시로 전환됩니다.</code>"
                )
            return buy_order_id, entry_session, True
        except Exception as e:
            print(f"🚨 [04:07 휩소 1주 현재가 매도 방어 {symbol}] {e}", flush=True)
            return buy_order_id, entry_session, True
        finally:
            _WHIPSAW_SIM_CACHE["whipsaw_last_dump_tick"] = time.time()
            ctx.in_memory_ordering_lock[symbol] = False

    return buy_order_id, entry_session, False
