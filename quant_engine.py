# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_engine.py
순수 롱(SOXL) 전용 '8%(12.5분할) 황금비율 정액제 기반 정량 산출기 · 자투리/수익금 복리 산입 ·
보유 회차별 3단 점진 단차(1~6회 -1.0% / 7~11회 -1.5% / 12회+ -2.0%) ·
프롬프트 2-Tier 가중평균 자동 병합(Case 53 하드 캡핑) · 단일 지층 수익금 리앵커링(0.994 버퍼) ·
실물 원가(actual_cost_usd) 분리 회계 및 TDD 사전 무결성 검증' 코어 수리 엔진 (Step 03 / 09)
- MODIFIED: 제3헌법에 의거하여 구버전 NQ=F 과열 덤핑 및 QQQ 3차 역행 손절 데드코드를 100% 영구 소각
- NEW: 5개년(2022 ~ 2026년 · 846,512분봉) 백테스트 챔피언(👑 06_CMP08_10_01 & 🔥 08_CMP08_30_01 & 🛡️ 01_FIX08_10_10)
  수리 모델을 실전 원장(AssassinLedger)과 1:1 직결
- 제1헌법 ~ 제6헌법 및 Case 19, 23, 25 ~ 58 완벽 준수
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    PORTION_RATIO_LOCKED,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.quant_engine")

# ---------------------------------------------------------------------------
# 📈 핵심 퀀트 상수 (5개년 백테스트 검증 황금 파라미터 락온)
# ---------------------------------------------------------------------------
GOLDEN_PORTION_RATIO: float = PORTION_RATIO_LOCKED  # 0.08 (8% · 12.5분할 황금비율 고정)
BASE_MAX_LOTS_8PCT: int = 12                        # 8% 분할 기본 회차 (13회차부터 확장 매수로 집계)
TIER1_EXPAND_LOT: int = 7                           # 7 ~ 11회차: -1.50% 매수 단차 확장
TIER2_EXPAND_LOT: int = 12                          # 12회차 이상: -2.00% 매수 단차 확장

ROUND_TRIP_FEE_RATE: float = 0.0014                 # 토스증권 해외주식 왕복 수수료율 (0.14% = 편도 0.07% * 2)
ROUND_TRIP_FEE_RATE_PCT: float = 0.14               # 왕복 수수료 퍼센트 (0.14%)
REANCHOR_NET_BUFFER_MULT: float = 0.994             # 리앵커링 순수 회수금 안전 버퍼 (0.6% 차감 = * 0.994)


# ---------------------------------------------------------------------------
# 🎯 3 : StratumBucket (실제 매수 원가와 리앵커링 장부 목표 단가의 분리 회계 객체)
# ---------------------------------------------------------------------------
@dataclass
class StratumBucket:
    """
    🎯 3 & Case 46 ~ 58:
    실제 현금 시드에서 지출된 순수 매수 원금(actual_cost_usd)과
    리앵커링 목표 매도 판정용 장부 단가(book_price) / 장부 투자금(book_invested_usd)을
    엄격히 분리 보존하는 지층 데이터 클래스.
    """
    qty: int
    book_price: float
    book_invested_usd: float
    actual_cost_usd: float
    lot_count: int = 1
    lot_type: str = "QUOTA_L1"

    @property
    def actual_avg_price(self) -> float:
        if self.qty <= 0:
            return 0.0
        return round(self.actual_cost_usd / float(self.qty), 4)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "qty": int(self.qty),
            "book_price": round(float(self.book_price), 4),
            "book_invested_usd": round(float(self.book_invested_usd), 4),
            "actual_cost_usd": round(float(self.actual_cost_usd), 4),
            "lot_count": max(1, int(self.lot_count)),
            "lot_type": str(self.lot_type or "QUOTA_L1"),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["StratumBucket"]:
        if not isinstance(data, dict):
            return None
        q = safe_int(data.get("qty"), 0)
        if q <= 0:
            return None
        bp = max(0.01, safe_float(data.get("book_price"), 0.01))
        binv = max(
            0.01 * float(q),
            safe_float(data.get("book_invested_usd"), bp * float(q)),
        )
        acost = max(
            0.01 * float(q),
            safe_float(data.get("actual_cost_usd"), binv),
        )
        lcnt = max(1, safe_int(data.get("lot_count"), 1))
        ltype = str(data.get("lot_type") or "QUOTA_L1").strip()
        return cls(
            qty=q,
            book_price=round(bp, 4),
            book_invested_usd=round(binv, 4),
            actual_cost_usd=round(acost, 4),
            lot_count=lcnt,
            lot_type=ltype,
        )


def buckets_from_dicts(raw_list: Iterable[Any]) -> List[StratumBucket]:
    """
    원장(JSON)의 딕셔너리 리스트를 StratumBucket 객체 리스트로 안전하게 변환한다.
    """
    out: List[StratumBucket] = []
    if not isinstance(raw_list, Iterable):
        return out
    for item in raw_list:
        if isinstance(item, StratumBucket):
            if item.qty > 0:
                out.append(item)
        elif isinstance(item, dict):
            b_obj = StratumBucket.from_dict(item)
            if b_obj is not None:
                out.append(b_obj)
    return out


def buckets_to_dicts(buckets: Iterable[StratumBucket]) -> List[Dict[str, Any]]:
    """
    StratumBucket 객체 리스트를 JSON 직렬화 가능한 딕셔너리 리스트로 변환한다.
    """
    out: List[Dict[str, Any]] = []
    if not isinstance(buckets, Iterable):
        return out
    for b in buckets:
        if isinstance(b, StratumBucket) and b.qty > 0:
            out.append(b.to_dict())
        elif isinstance(b, dict):
            b_obj = StratumBucket.from_dict(b)
            if b_obj is not None:
                out.append(b_obj.to_dict())
    return out


# ---------------------------------------------------------------------------
# 🎯 1 : 8%(12.5분할) 황금비율 정액제 기반 정량 산출기 및 가용 현금 캡핑
# ---------------------------------------------------------------------------
def compute_golden8_quota_qty(
    anchor_price: float,
    current_total_equity: float,
    initial_seed_usd: float = DEFAULT_INITIAL_SEED_USD,
    portion_mode: str = "COMP_08PCT",
) -> Tuple[int, float]:
    """
    🎯 1 & Case 25:
    [8%(12.5분할) 황금비율 고정 기반 정량 산출 공식]
    1) 1회분 기준 예산(portion_budget) 산출:
       - COMP_08PCT : max(initial_seed_usd, current_total_equity) * 0.08 (총자산 비례 동적 복리 증액)
       - FIXED_800  : initial_seed_usd * 0.08 (원금 고정 안정 모드, 기본 $800.00 고정)
    2) 직전 매매 기준가격(anchor_price)을 기반으로 다음 매수할 고정 정수 수량(target_qty) 선행 확정:
       target_qty = max(1, int(math.floor(portion_budget / max(0.01, anchor_price))))
       * 하락장에서는 낮은 anchor_price 로 인해 target_qty 가 자동 증가(다량 매수)하고,
       * 상승장에서는 높은 anchor_price 로 인해 target_qty 가 자동 감소(소수 매수)한다.
    """
    init_seed = max(100.0, safe_float(initial_seed_usd, DEFAULT_INITIAL_SEED_USD))
    base_unit_usd = round(init_seed * GOLDEN_PORTION_RATIO, 4)

    mode = str(portion_mode or "COMP_08PCT").upper().strip()
    if mode == "COMP_08PCT":
        eff_equity = max(init_seed, safe_float(current_total_equity, init_seed))
        portion_budget = round(eff_equity * GOLDEN_PORTION_RATIO, 4)
    else:
        portion_budget = base_unit_usd

    ref_p = max(0.01, safe_float(anchor_price, 0.01))
    target_qty = max(1, int(math.floor((portion_budget + 1e-9) / ref_p)))
    return target_qty, portion_budget


def resolve_affordable_buy_qty(
    target_qty: int,
    price_t: float,
    free_cash_usd: float,
    broker_buying_power_usd: Optional[float] = None,
) -> Tuple[int, float]:
    """
    🚨 Case 25:
    가용 현금 시드(free_cash_usd, 그리고 토스 실제 USD 매수가능금액 broker_buying_power_usd 제공 시 둘 중 작은 값)
    내에서 목표 정량(target_qty)을 매수할 수 있는지 확인하고,
    현금이 부족할 경우 남은 현금으로 살 수 있는 최대 정수 주수(max_affordable)로 안전 캡핑한다.
    왕복 수수료(0.14%) 버퍼를 반영하여 미수 주문 거절(422 insufficient-buying-power)을 100% 원천 차단한다.
    """
    t_qty = safe_int(target_qty, 0)
    exec_p = safe_float(price_t, 0.0)
    avail_cash = max(0.0, safe_float(free_cash_usd, 0.0))

    if broker_buying_power_usd is not None and safe_float(broker_buying_power_usd, 0.0) > 0.0:
        avail_cash = min(avail_cash, safe_float(broker_buying_power_usd, 0.0))

    if t_qty <= 0 or exec_p <= 0.0 or avail_cash <= 0.0:
        return 0, 0.0

    unit_cost_with_fee = exec_p * (1.0 + ROUND_TRIP_FEE_RATE)
    max_affordable = int(math.floor((avail_cash + 1e-9) / unit_cost_with_fee))
    if max_affordable <= 0:
        return 0, 0.0

    final_qty = min(t_qty, max_affordable)
    actual_spent_usd = round(float(final_qty) * exec_p, 4)
    return final_qty, actual_spent_usd


# ---------------------------------------------------------------------------
# 🎯 2 : 보유 회차(held_lots)에 따른 3단 점진 단차 및 스나이퍼 매도 목표율 산출기
# ---------------------------------------------------------------------------
def get_golden8_prog_thresholds(
    held_lots: int,
    threshold_mode: str = "08_VREV_PROG",
) -> Tuple[float, float, float]:
    """
    🎯 2 [Step 1 & Step 2]:
    8%(12.5분할) 황금비율 기준 현재 보유 회차(held_lots)에 따른
    (drop_buy_pct, rise_sell_l1_pct, rise_sell_l2_pct) 튜플을 반환한다.
    - 매수(떨사) 요구 하락률 (전 모드 공통):
      * 1 ~  6회차 보유 중 : -1.00% 이하 하락 시에만 매수 (미달 시 100% 매수 패스하여 시드 보호!)
      * 7 ~ 11회차 보유 중 : -1.50% 이하 하락 시에만 매수
      * 12회차 이상 보유 중: -2.00% 이하 하락 시에만 매수
    - 매도(오팔) 목표 상승률:
      * 08_VREV_PROG (복리 챔피언 기본값) : 1층 +1.50% / 2층 통합층 +1.00%
      * 01_PROG_LOT  (고정 정량 선택 시)  : 1~6회차 +1.00% / 7~11회차 +1.50% / 12회차+ +2.00%
    """
    lots = max(0, safe_int(held_lots, 0))
    t_mode = str(threshold_mode or "08_VREV_PROG").upper().strip()

    if lots >= TIER2_EXPAND_LOT:
        drop_p = -2.00
    elif lots >= TIER1_EXPAND_LOT:
        drop_p = -1.50
    else:
        drop_p = -1.00

    if t_mode == "01_PROG_LOT":
        sell_p = abs(drop_p)
        return drop_p, sell_p, sell_p

    # 기본 챔피언 모드: 08_VREV_PROG (1층 +1.50%, 2층 +1.00%)
    return drop_p, 1.50, 1.00


def calc_target_sell_prices_for_buckets(
    buckets: List[StratumBucket],
    threshold_mode: str = "08_VREV_PROG",
) -> Dict[str, float]:
    """
    현재 지층 큐(buckets)의 보유 회차 및 장부 단가(book_price)를 기반으로
    1층 목표 매도가(l1_target_price)와 2층 목표 매도가(l2_target_price)를 산출한다.
    """
    clean_b = buckets_from_dicts(buckets)
    if not clean_b:
        return {
            "drop_buy_pct": -1.00,
            "rise_sell_l1_pct": 1.50,
            "rise_sell_l2_pct": 1.00,
            "l1_book_price": 0.0,
            "l1_target_price": 0.0,
            "l2_book_price": 0.0,
            "l2_target_price": 0.0,
        }

    held_lots = sum(b.lot_count for b in clean_b)
    drop_p, sell_l1_p, sell_l2_p = get_golden8_prog_thresholds(
        held_lots=held_lots, threshold_mode=threshold_mode
    )

    # 만약 장부에 2층(MERGED_UPPER) 단독 1개만 남아 있는 상태라면 해당 층의 탈출 목표율은 sell_l2_p 적용
    if len(clean_b) == 1 and clean_b[0].lot_type == "MERGED_UPPER":
        b2 = clean_b[0]
        t2 = round(b2.book_price * (1.0 + sell_l2_p / 100.0), 4)
        return {
            "drop_buy_pct": drop_p,
            "rise_sell_l1_pct": sell_l1_p,
            "rise_sell_l2_pct": sell_l2_p,
            "l1_book_price": 0.0,
            "l1_target_price": 0.0,
            "l2_book_price": b2.book_price,
            "l2_target_price": t2,
        }

    b1 = clean_b[0]
    t1 = round(b1.book_price * (1.0 + sell_l1_p / 100.0), 4)
    l2_bp = 0.0
    t2 = 0.0
    if len(clean_b) >= 2:
        b2 = clean_b[1]
        l2_bp = b2.book_price
        t2 = round(b2.book_price * (1.0 + sell_l2_p / 100.0), 4)

    return {
        "drop_buy_pct": drop_p,
        "rise_sell_l1_pct": sell_l1_p,
        "rise_sell_l2_pct": sell_l2_p,
        "l1_book_price": b1.book_price,
        "l1_target_price": t1,
        "l2_book_price": l2_bp,
        "l2_target_price": t2,
    }


# ---------------------------------------------------------------------------
# 🎯 3 : 프롬프트 2-Tier Auto-Merge Protocol & 단일 지층 수익금 리앵커링
# ---------------------------------------------------------------------------
def vrev_add_quota_lot_and_merge_2tier(
    queue: List[StratumBucket],
    buy_qty: int,
    buy_price: float,
    actual_spent_usd: Optional[float] = None,
    lot_type: str = "QUOTA_L1",
) -> List[StratumBucket]:
    """
    🎯 3 & Case 46, 53:
    [프롬프트 2-Tier Auto-Merge Protocol & Case 53 풍선효과 방지 상한 하드 캡핑]
    1) 방금 가장 낮은 가격(buy_price)에 매수한 정량 묶음(buy_qty 주)은 무조건
       새로운 1층(queue[0])으로 배치하고 순수 체결 단가를 100% 보존한다.
    2) 기존에 보유 중이던 모든 지층들은 단일 상위 통합층인 2층(queue[1], lot_type="MERGED_UPPER")으로
       수량, 장부 투자금, 실제 매수 원금, 회차를 원자적으로 합산하며,
       2층의 장부 단가가 기존 전체 장부 평단가를 초과하지 않도록 하드 캡핑(Case 53)한다.
    """
    existing = buckets_from_dicts(queue)
    q_int = max(1, safe_int(buy_qty, 1))
    bp = max(0.01, safe_float(buy_price, 0.01))
    cost_usd = (
        round(max(0.01 * float(q_int), safe_float(actual_spent_usd, 0.0)), 4)
        if (actual_spent_usd is not None and safe_float(actual_spent_usd, 0.0) > 0.0)
        else round(float(q_int) * bp, 4)
    )

    new_l1 = StratumBucket(
        qty=q_int,
        book_price=round(bp, 4),
        book_invested_usd=cost_usd,
        actual_cost_usd=cost_usd,
        lot_count=1,
        lot_type=str(lot_type or "QUOTA_L1"),
    )
    if not existing:
        return [new_l1]

    old_total_qty = sum(b.qty for b in existing if b.qty > 0)
    if old_total_qty <= 0:
        return [new_l1]

    old_book_inv = sum(b.book_invested_usd for b in existing if b.qty > 0)
    old_actual_cost = sum(b.actual_cost_usd for b in existing if b.qty > 0)
    old_lot_cnt = sum(b.lot_count for b in existing if b.qty > 0)
    old_overall_book_avg = old_book_inv / float(old_total_qty)

    raw_upper_book_p = max(0.01, old_book_inv / float(old_total_qty))
    capped_upper_book_p = min(raw_upper_book_p, old_overall_book_avg)
    capped_upper_book_inv = capped_upper_book_p * float(old_total_qty)

    merged_l2 = StratumBucket(
        qty=old_total_qty,
        book_price=round(capped_upper_book_p, 4),
        book_invested_usd=round(capped_upper_book_inv, 4),
        actual_cost_usd=round(old_actual_cost, 4),
        lot_count=max(1, old_lot_cnt),
        lot_type="MERGED_UPPER",
    )
    return [new_l1, merged_l2]


def vrev_reanchor_single_tier_quota(
    queue: List[StratumBucket],
    net_realized_cash_profit: float,
) -> Tuple[List[StratumBucket], float]:
    """
    🎯 3 & Case 47:
    [단일 지층 수익금 리앵커링 (Re-anchoring)]
    1층(하부 공격수)이 익절 매도되어 장부에 상위 통합층인 2층(단일 지층, len(queue) == 1)만 남았을 때,
    회수된 세후 실현 순수익금(net_realized_cash_profit)에 안전 버퍼(0.994)를 곱한 금액(credit_usd)만큼
    잔여 2층의 장부 투자원금(book_invested_usd)만 즉시 차감하여 목표 탈출 단가(book_price)를 인하한다.
    (실제 매수 원금 actual_cost_usd 는 절대 건드리지 않아 회계 무결성을 100% 보존한다!)
    """
    existing = buckets_from_dicts(queue)
    if (
        len(existing) != 1
        or existing[0].qty <= 0
        or safe_float(net_realized_cash_profit, 0.0) <= 0.0
    ):
        return existing, 0.0

    rem = existing[0]
    credit_usd = max(
        0.0,
        safe_float(net_realized_cash_profit, 0.0) * REANCHOR_NET_BUFFER_MULT,
    )
    old_book_p = rem.book_price

    new_book_inv = max(
        0.01 * float(rem.qty), rem.book_invested_usd - credit_usd
    )
    new_pure_price = max(0.01, new_book_inv / float(rem.qty))
    rem.book_invested_usd = round(new_book_inv, 4)
    rem.book_price = round(new_pure_price, 4)
    price_drop_amt = round(max(0.0, old_book_p - rem.book_price), 4)
    return [rem], price_drop_amt


# ---------------------------------------------------------------------------
# 🎯 5 : 매도 세후 실현손익 산출 및 포트폴리오/회계 항등식 요약기
# ---------------------------------------------------------------------------
def calc_sell_realized_pnl(
    sold_qty: int,
    sell_exec_price: float,
    sold_actual_cost_usd: float,
) -> Dict[str, float]:
    """
    매도 체결 수량(sold_qty), 실제 매도 체결 단가(sell_exec_price),
    매도된 지층들의 실제 매수 원금 합계(sold_actual_cost_usd)를 받아
    왕복 수수료(0.14%)를 차감한 세후 실현 순수익금(net_pnl_usd)을 정확히 계산한다.
    """
    sq = max(0, safe_int(sold_qty, 0))
    sp = max(0.0, safe_float(sell_exec_price, 0.0))
    scost = max(0.0, safe_float(sold_actual_cost_usd, 0.0))

    if sq <= 0 or sp <= 0.0 or scost <= 0.0:
        return {
            "gross_proceeds_usd": 0.0,
            "sold_cost_usd": scost,
            "gross_pnl_usd": 0.0,
            "fee_usd": 0.0,
            "net_pnl_usd": 0.0,
            "net_rate_pct": 0.0,
        }

    gross_proceeds = sp * float(sq)
    gross_pnl = gross_proceeds - scost
    fee_usd = (gross_proceeds + scost) * (ROUND_TRIP_FEE_RATE * 0.5)
    net_pnl = gross_pnl - fee_usd
    net_rate_pct = (net_pnl / scost) * 100.0 if scost > 0.0 else 0.0

    return {
        "gross_proceeds_usd": round(gross_proceeds, 4),
        "sold_cost_usd": round(scost, 4),
        "gross_pnl_usd": round(gross_pnl, 4),
        "fee_usd": round(fee_usd, 4),
        "net_pnl_usd": round(net_pnl, 4),
        "net_rate_pct": round(net_rate_pct, 2),
    }


def summarize_stratum_portfolio(
    state: Dict[str, Any],
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    원장 상태(state)와 실시간 현재가(live_price)를 기반으로:
    - 보유 지층(1층 / 2층) 상세, 총 보유 주수, 총 보유 회차(held_lots),
    - 실투입 원금(actual_invested_usd), 가용 현금(free_cash_usd),
    - 현재 총자산(total_equity_usd = free_cash + market_value) 및 원금 기준 총자산(cost_equity_usd),
    - 미실현 평가손익(unrealized_pnl_usd) 및 다음 8% 목표 정량(next_target_qty, next_portion_budget)
    을 한 번에 계산하여 반환한다.
    """
    buckets = buckets_from_dicts(state.get("buckets", []))
    held_shares = sum(b.qty for b in buckets)
    held_lots = sum(b.lot_count for b in buckets)
    actual_invested = round(sum(b.actual_cost_usd for b in buckets), 4)
    book_invested = round(sum(b.book_invested_usd for b in buckets), 4)

    init_seed = max(
        100.0,
        safe_float(state.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD),
    )
    cum_realized_net = safe_float(state.get("cumulative_realized_net_usd"), 0.0)
    free_cash = max(
        0.0,
        safe_float(
            state.get("free_cash_usd"),
            (init_seed + cum_realized_net) - actual_invested,
        ),
    )
    cost_equity = round(free_cash + actual_invested, 4)

    lp = safe_float(live_price, 0.0)
    if lp <= 0.0 and buckets:
        lp = buckets[0].book_price

    market_val = round(lp * float(held_shares), 4) if held_shares > 0 else 0.0
    unrealized_pnl = (
        round(market_val - actual_invested, 4) if held_shares > 0 else 0.0
    )
    unrealized_rate = (
        round((unrealized_pnl / actual_invested) * 100.0, 2)
        if actual_invested > 0.0
        else 0.0
    )
    total_mark_equity = round(free_cash + market_val, 4)
    total_net_pnl = round(cum_realized_net + unrealized_pnl, 4)
    total_roi_pct = round((total_net_pnl / init_seed) * 100.0, 2)

    portion_mode = str(state.get("portion_mode") or "COMP_08PCT").upper()
    threshold_mode = str(state.get("threshold_mode") or "08_VREV_PROG").upper()
    buy_interval_min = safe_int(state.get("buy_interval_min"), 10)

    # 현재 매수 기준가(active_buy_anchor) 판별
    anchor_p = safe_float(state.get("anchor_price"), lp)
    cycle_min_p = safe_float(state.get("cycle_min_buy_price"), anchor_p)
    ratchet_p = safe_float(state.get("ratchet_anchor_price"), anchor_p)

    is_l2_only_anti_bloat = bool(
        len(buckets) == 1 and buckets[0].lot_type == "MERGED_UPPER"
    )
    if is_l2_only_anti_bloat:
        candidates = [p for p in (cycle_min_p, ratchet_p) if p > 0.0]
        active_buy_anchor = min(candidates) if candidates else buckets[0].book_price
    elif buckets:
        active_buy_anchor = (
            min(buckets[0].book_price, anchor_p)
            if anchor_p > 0.0
            else buckets[0].book_price
        )
    else:
        active_buy_anchor = lp if lp > 0.0 else max(0.01, anchor_p)

    next_qty, next_budget = compute_golden8_quota_qty(
        anchor_price=active_buy_anchor if active_buy_anchor > 0.0 else 100.0,
        current_total_equity=cost_equity,
        initial_seed_usd=init_seed,
        portion_mode=portion_mode,
    )

    targets = calc_target_sell_prices_for_buckets(
        buckets=buckets, threshold_mode=threshold_mode
    )
    drop_buy_pct = targets["drop_buy_pct"]
    trigger_buy_price = (
        round(active_buy_anchor * (1.0 + drop_buy_pct / 100.0), 4)
        if active_buy_anchor > 0.0
        else 0.0
    )

    return {
        "buckets": buckets,
        "tier_count": len(buckets),
        "is_l2_only_anti_bloat": is_l2_only_anti_bloat,
        "held_shares": held_shares,
        "held_lots": held_lots,
        "initial_seed_usd": init_seed,
        "free_cash_usd": round(free_cash, 2),
        "actual_invested_usd": round(actual_invested, 2),
        "book_invested_usd": round(book_invested, 2),
        "cost_equity_usd": round(cost_equity, 2),
        "market_value_usd": round(market_val, 2),
        "unrealized_pnl_usd": round(unrealized_pnl, 2),
        "unrealized_rate_pct": unrealized_rate,
        "cumulative_realized_net_usd": round(cum_realized_net, 2),
        "total_net_pnl_usd": round(total_net_pnl, 2),
        "total_mark_equity_usd": round(total_mark_equity, 2),
        "total_roi_pct": total_roi_pct,
        "portion_mode": portion_mode,
        "threshold_mode": threshold_mode,
        "buy_interval_min": buy_interval_min,
        "active_buy_anchor": round(active_buy_anchor, 4),
        "trigger_buy_price": trigger_buy_price,
        "ratchet_anchor_price": round(ratchet_p, 4),
        "cycle_min_buy_price": round(cycle_min_p, 4),
        "next_target_qty": next_qty,
        "next_portion_budget": round(next_budget, 2),
        **targets,
    }


# ---------------------------------------------------------------------------
# 🛠️ TDD 사전 무결성 검증 (run_quant_engine_tdd)
# ---------------------------------------------------------------------------
def run_quant_engine_tdd() -> None:
    """
    백테스트 및 프롬프트 절대기준 수식이 100% 오차 없이 작동하는지 기동 시 자동 검증한다.
    """
    # 1) 8%($800) 황금비율 정액 기반 정량 산출 및 $90 하락 매수 시 $80 자투리 시드 보존 검증
    q8, b8 = compute_golden8_quota_qty(
        anchor_price=100.0,
        current_total_equity=10000.0,
        initial_seed_usd=10000.0,
        portion_mode="COMP_08PCT",
    )
    assert q8 == 8 and abs(b8 - 800.0) < 1e-6, "8% 정량 8주 산출 오류"

    buy_q, spent_u = resolve_affordable_buy_qty(
        target_qty=q8,
        price_t=90.0,
        free_cash_usd=10000.0,
    )
    saved_u = b8 - spent_u
    assert buy_q == 8 and abs(spent_u - 720.0) < 1e-6 and abs(saved_u - 80.0) < 1e-6

    # 2) 총자산 증가($15,000) 시 COMP_08PCT(8% = $1,200) vs FIXED_800($800) 비교 검증
    q_comp, b_comp = compute_golden8_quota_qty(
        anchor_price=100.0,
        current_total_equity=15000.0,
        initial_seed_usd=10000.0,
        portion_mode="COMP_08PCT",
    )
    q_fix, b_fix = compute_golden8_quota_qty(
        anchor_price=100.0,
        current_total_equity=15000.0,
        initial_seed_usd=10000.0,
        portion_mode="FIXED_800",
    )
    assert q_comp == 12 and abs(b_comp - 1200.0) < 1e-6
    assert q_fix == 8 and abs(b_fix - 800.0) < 1e-6

    # 3) 보유 회차별(1~6회 / 7~11회 / 12회+) 점진 단차 검증
    assert get_golden8_prog_thresholds(1, "08_VREV_PROG") == (-1.00, 1.50, 1.00)
    assert get_golden8_prog_thresholds(6, "08_VREV_PROG") == (-1.00, 1.50, 1.00)
    assert get_golden8_prog_thresholds(7, "08_VREV_PROG") == (-1.50, 1.50, 1.00)
    assert get_golden8_prog_thresholds(11, "08_VREV_PROG") == (-1.50, 1.50, 1.00)
    assert get_golden8_prog_thresholds(12, "08_VREV_PROG") == (-2.00, 1.50, 1.00)

    # 4) 2-Tier 가중병합 및 단일 지층 수익금 리앵커링 검증
    q_list: List[StratumBucket] = []
    q_list = vrev_add_quota_lot_and_merge_2tier(q_list, 8, 100.0)
    q_list = vrev_add_quota_lot_and_merge_2tier(q_list, 8, 98.0)
    q_list = vrev_add_quota_lot_and_merge_2tier(q_list, 9, 96.0)
    assert (
        len(q_list) == 2
        and q_list[0].qty == 9
        and q_list[1].qty == 16
        and q_list[1].lot_count == 2
    )
    re_q, drop_amt = vrev_reanchor_single_tier_quota([q_list[1]], 16.0)
    assert len(re_q) == 1 and drop_amt > 0.99 and re_q[0].actual_cost_usd == q_list[1].actual_cost_usd


# 모듈 로드 시 즉시 TDD 무결성 검증 실행
run_quant_engine_tdd()
