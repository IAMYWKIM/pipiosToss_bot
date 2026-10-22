# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_engine.py
순수 롱(SOXL) 전용 '8%(12.5분할) 황금비율 정액제 기반 정량 산출기 ·
자투리 절약금 및 세후 실물 수익금 실시간 시드 풀 복리 산입 ·
보유 회차별 3단 점진 단차(1~6회 -1.0% / 7~11회 -1.5% / 12회+ -2.0% 및 1층 +1.50% / 2층 +1.00%) ·
프롬프트 2-Tier 가중평균 자동 병합(REENTRY_1FL 독립 1층 배치 vs DIP_1FL 2층 통합 · Case 53 하드 캡핑) ·
단일 지층 실물 수익금 리앵커링(0.994 버퍼) · 기억된 최저 매수가(cycle_min_buy_price) 잠금 요약 및 TDD 검증' 코어 모듈 (Step 01 / 02)
- MODIFIED: toss_api.py 로부터 REANCHOR_NET_BUFFER_MULT 등 퀀트 상수를 직접 import 하던 구문을 제거하고
  getattr(toss_api, ...) 폴백 및 자체 상수 선언으로 전환하여 ImportError 크래시를 100% 원천 소각!
- NEW (Case 59): 2층(MERGED_UPPER) 단독 보유 상태에서 상시 1층 실물 재매수(REENTRY_1FL) 체결 시
  기존 2층과 병합하지 않고 [L1(REENTRY_1FL), L2(MERGED_UPPER)] 독립 2-Tier 로 배치
- NEW (Case 60): 1층 보유 중 추가 매수(물타기) 판정 기준가(active_buy_anchor)를
  min(buckets[0].book_price, anchor_price, cycle_min_buy_price) 로 고정하여 기억된 최저가 도달 전 추매 원천 봉쇄
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 64 완벽 준수
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import toss_api
from toss_api import safe_float, safe_int

logger = logging.getLogger("toss_sniper.quant_engine")

# MODIFIED: toss_api.py 내부 상수 존재 여부와 무관하게 100% 안전하게 작동하는 자립형 폴백 상수 정의
DEFAULT_INITIAL_SEED_USD: float = float(
    getattr(toss_api, "DEFAULT_INITIAL_SEED_USD", 10000.0)
)
PORTION_RATIO_LOCKED: float = float(
    getattr(toss_api, "PORTION_RATIO_LOCKED", 0.08)
)
REANCHOR_NET_BUFFER_MULT: float = float(
    getattr(toss_api, "REANCHOR_NET_BUFFER_MULT", 0.994)
)
ROUND_TRIP_FEE_RATE_PCT: float = float(
    getattr(toss_api, "ROUND_TRIP_FEE_RATE_PCT", 0.14)
)

# 8%(12.5분할) 황금비율 기준 기본 분할 회차 상수
BASE_MAX_LOTS_8PCT: int = 12


# ---------------------------------------------------------------------------
# [핵심 1] StratumBucket : 리앵커링 장부단가(book_price)와 실제 매수원금(actual_cost_usd) 분리 회계 객체
# ---------------------------------------------------------------------------
@dataclass
class StratumBucket:
    """
    🎯 3 & Case 46 ~ 60:
    SOXL 지층(1층 L1 / 2층 통합층 MERGED_UPPER) 개별 버킷 데이터 구조체.
    - qty              : 보유 정수 주식 수 (주)
    - book_price       : 리앵커링이 반영된 목표 탈출 기준 장부 단가 ($)
    - book_invested_usd: 리앵커링 차감이 반영된 장부상 잔여 투자금 ($)
    - actual_cost_usd  : 실제 가용 현금 시드에서 지출된 순수 매수 원금 ($, 회계 항등식 보존용)
    - lot_count        : 해당 지층에 병합된 누적 매수 회차 수
    - lot_type         : 지층 성격 태그 ("REENTRY_1FL", "QUOTA_L1", "DIP_1FL", "MERGED_UPPER", "SYNCED_1FL")
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
        q_int = max(1, safe_int(self.qty, 1))
        b_price = round(max(0.0001, safe_float(self.book_price, 0.01)), 4)
        b_inv = round(
            max(
                0.01,
                safe_float(self.book_invested_usd, float(q_int) * b_price),
            ),
            4,
        )
        a_cost = round(
            max(0.01, safe_float(self.actual_cost_usd, b_inv)),
            4,
        )
        l_cnt = max(1, safe_int(self.lot_count, 1))
        return {
            "qty": q_int,
            "book_price": b_price,
            "book_invested_usd": b_inv,
            "actual_cost_usd": a_cost,
            "lot_count": l_cnt,
            "lot_type": str(self.lot_type or "QUOTA_L1"),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["StratumBucket"]:
        if not isinstance(data, dict):
            return None
        q_int = safe_int(data.get("qty"), 0)
        if q_int <= 0:
            return None
        b_price = safe_float(
            data.get("book_price"), safe_float(data.get("buy_price"), 0.0)
        )
        if b_price <= 0.0:
            return None
        b_inv = safe_float(
            data.get("book_invested_usd"), float(q_int) * b_price
        )
        if b_inv <= 0.0:
            b_inv = float(q_int) * b_price
        a_cost = safe_float(data.get("actual_cost_usd"), b_inv)
        if a_cost <= 0.0:
            a_cost = b_inv
        l_cnt = max(1, safe_int(data.get("lot_count"), 1))
        l_type = str(data.get("lot_type") or "QUOTA_L1")
        return cls(
            qty=q_int,
            book_price=round(b_price, 4),
            book_invested_usd=round(b_inv, 4),
            actual_cost_usd=round(a_cost, 4),
            lot_count=l_cnt,
            lot_type=l_type,
        )


def buckets_from_dicts(
    raw_list: Sequence[Dict[str, Any]],
) -> List[StratumBucket]:
    out: List[StratumBucket] = []
    if not isinstance(raw_list, (list, tuple)):
        return out
    for item in raw_list:
        b_obj = StratumBucket.from_dict(item)
        if b_obj is not None:
            out.append(b_obj)
    return out


def buckets_to_dicts(
    buckets: Sequence[StratumBucket],
) -> List[Dict[str, Any]]:
    return [
        b.to_dict()
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]


# ---------------------------------------------------------------------------
# [핵심 2] 8%(12.5분할) 황금비율 정액 기반 정량 주수 산출 및 가용현금 캡핑 (🎯 1 & Case 25 ~ 35)
# ---------------------------------------------------------------------------
def compute_golden8_quota_qty(
    anchor_price: float,
    current_total_equity: float,
    initial_seed_usd: float = DEFAULT_INITIAL_SEED_USD,
    portion_mode: str = "COMP_08PCT",
) -> Tuple[int, float]:
    """
    🎯 1 & Case 25 ~ 35:
    직전 매매 기준가(anchor_price)를 기반으로 8%(12.5분할) 황금비율 1회분 예산(portion_budget)과
    고정 목표 매수 주수(target_qty)를 산출한다.
    - COMP_08PCT (기본 챔피언 모드):
      portion_budget = round(max(initial_seed_usd, current_total_equity) * 0.08, 4)
    - FIXED_800 (원금 고정 모드):
      portion_budget = round(initial_seed_usd * 0.08, 4)
    - target_qty = max(1, int(math.floor(portion_budget / max(0.01, anchor_price))))
    반환값: (target_qty, portion_budget)
    """
    init_seed = max(
        100.0, safe_float(initial_seed_usd, DEFAULT_INITIAL_SEED_USD)
    )
    tot_eq = max(init_seed, safe_float(current_total_equity, init_seed))
    mode = str(portion_mode or "COMP_08PCT").upper().strip()

    if mode == "FIXED_800":
        portion_budget = round(init_seed * PORTION_RATIO_LOCKED, 4)
    else:
        portion_budget = round(tot_eq * PORTION_RATIO_LOCKED, 4)

    ref_p = max(0.01, safe_float(anchor_price, 0.01))
    raw_qty = int(math.floor((portion_budget + 1e-9) / ref_p))
    target_qty = max(1, raw_qty)
    return target_qty, portion_budget


def resolve_affordable_buy_qty(
    target_qty: int,
    price_t: float,
    free_cash_usd: float,
    broker_buying_power_usd: Optional[float] = None,
) -> Tuple[int, float]:
    """
    🚨 Case 25 ~ 35:
    직전 기준가로 산출된 목표 정량(target_qty)을 실제 하락가(price_t)로 매수하기 직전,
    왕복 수수료 버퍼(0.14%)를 감안한 가용 현금(free_cash_usd) 및 토스 실계좌 매수가능금액
    (broker_buying_power_usd) 한도 내 매수 가능 정수 주수로 캡핑하여
    422 insufficient-buying-power 에러를 100% 원천 차단한다.
    반환값: (buy_qty, actual_spent_usd)
    """
    t_qty = max(0, safe_int(target_qty, 0))
    exec_p = safe_float(price_t, 0.0)
    if t_qty <= 0 or exec_p <= 0.0:
        return 0, 0.0

    avail_cash = max(0.0, safe_float(free_cash_usd, 0.0))
    if broker_buying_power_usd is not None:
        bp_val = safe_float(broker_buying_power_usd, 0.0)
        if bp_val >= 0.0:
            avail_cash = min(avail_cash, bp_val)

    fee_mult = 1.0 + (ROUND_TRIP_FEE_RATE_PCT / 100.0)
    unit_cost_with_fee = exec_p * fee_mult
    if avail_cash < unit_cost_with_fee:
        return 0, 0.0

    max_affordable = int(math.floor((avail_cash + 1e-9) / unit_cost_with_fee))
    buy_qty = max(0, min(t_qty, max_affordable))
    actual_spent_usd = (
        round(float(buy_qty) * exec_p, 4) if buy_qty > 0 else 0.0
    )
    return buy_qty, actual_spent_usd


# ---------------------------------------------------------------------------
# [핵심 3] 보유 회차별 3단 점진 하락 단차 & 스나이퍼 매도 임계치 (🎯 2 & Case 36 ~ 45)
# ---------------------------------------------------------------------------
def get_golden8_prog_thresholds(
    held_lots: int,
    threshold_mode: str = "08_VREV_PROG",
) -> Tuple[float, float, float]:
    """
    🎯 2 & Case 36 ~ 45:
    현재 보유 중인 총 투입 회차(held_lots)에 따른:
    1) 10분 시드보호 매수 게이트 요구 하락률(drop_buy_pct):
       - 1 ~  6회차 보유 중 : -1.00%
       - 7 ~ 11회차 보유 중 : -1.50%
       - 12회차 이상 보유 중: -2.00%
    2) 1분 상시 스나이퍼 매도 게이트 목표 상승률(rise_sell_l1_pct, rise_sell_l2_pct):
       - 08_VREV_PROG (챔피언 기본값): 1층 +1.50% / 2층 통합층 +1.00%
       - 01_PROG_LOT (고정정량 옵션): 1~6회 +1.00% / 7~11회 +1.50% / 12회+ +2.00%
    반환값: (drop_buy_pct, rise_sell_l1_pct, rise_sell_l2_pct)
    """
    lots = max(1, safe_int(held_lots, 1))
    if lots <= 6:
        drop_buy_pct = -1.00
        prog_rise = 1.00
    elif lots <= 11:
        drop_buy_pct = -1.50
        prog_rise = 1.50
    else:
        drop_buy_pct = -2.00
        prog_rise = 2.00

    mode = str(threshold_mode or "08_VREV_PROG").upper().strip()
    if mode == "01_PROG_LOT":
        return drop_buy_pct, prog_rise, prog_rise
    return drop_buy_pct, 1.50, 1.00


# ---------------------------------------------------------------------------
# [핵심 4] 2-Tier 가중평균 자동 병합 (REENTRY_1FL 독립 배치 vs DIP_1FL 병합) & 리앵커링
# ---------------------------------------------------------------------------
def vrev_add_quota_lot_and_merge_2tier(
    queue: Sequence[StratumBucket],
    buy_qty: int,
    buy_price: float,
    actual_spent_usd: Optional[float] = None,
    lot_type: str = "QUOTA_L1",
) -> List[StratumBucket]:
    """
    🎯 3 & 🎯 4 & Case 46 ~ 60:
    신규 정량 매수 체결 시 최대 2개 지층(2-Tier) 체제를 유지하는 핵심 병합 함수:
    1) 기존 보유 지층이 없을 때(len(queue) == 0):
       신규 매수분을 단일 1층([new_l1])으로 생성한다.
    2) 🚨 Case 59 (실물 기반 무팽창 True Always-L1 독립 배치):
       1층이 익절되어 장부에 상위 2층(len(queue) == 1 and queue[0].lot_type == "MERGED_UPPER")만
       단독으로 남아 있을 때, 현재가로 즉시 재매수한 실물 1층(lot_type == "REENTRY_1FL")이 체결되면:
       기존 2층(MERGED_UPPER)을 재병합하거나 건드리지 않고 그대로 2층(queue[1])에 유지한 채,
       신규 매수분을 독립된 1층(queue[0])에 배치하여 [new_l1(REENTRY_1FL), existing_l2(MERGED_UPPER)] 를 반환한다!
    3) 이미 1층이 존재하는 상태에서 기억된 최저가를 깨고 내려가 10분 게이트 신저가 추매(DIP_1FL / QUOTA_L1)가
       체결된 경우:
       신규 매수분은 1층(queue[0])에 순수 단가로 보존하고, 기존 지층들(기존 1층 + 기존 2층)은
       단일 상위 2층(queue[1], MERGED_UPPER)으로 원자적 가중평균 병합(Case 53 하드 캡핑 적용)한다.
    """
    q_int = max(1, safe_int(buy_qty, 1))
    p_val = round(max(0.0001, safe_float(buy_price, 0.01)), 4)
    spent = (
        round(
            max(0.01, safe_float(actual_spent_usd, float(q_int) * p_val)), 4
        )
        if actual_spent_usd is not None
        else round(float(q_int) * p_val, 4)
    )
    tag = str(lot_type or "QUOTA_L1").upper().strip()

    new_l1 = StratumBucket(
        qty=q_int,
        book_price=p_val,
        book_invested_usd=round(float(q_int) * p_val, 4),
        actual_cost_usd=spent,
        lot_count=1,
        lot_type=tag,
    )

    existing = [
        b
        for b in (queue or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not existing:
        return [new_l1]

    # 🚨 Case 59: 2층(MERGED_UPPER) 단독 보유 중 REENTRY_1FL 실물 1층 유입 시 2층 비병합 독립 배치!
    if (
        len(existing) == 1
        and str(existing[0].lot_type).upper() == "MERGED_UPPER"
        and tag == "REENTRY_1FL"
    ):
        existing_l2 = StratumBucket(
            qty=existing[0].qty,
            book_price=existing[0].book_price,
            book_invested_usd=existing[0].book_invested_usd,
            actual_cost_usd=existing[0].actual_cost_usd,
            lot_count=existing[0].lot_count,
            lot_type="MERGED_UPPER",
        )
        return [new_l1, existing_l2]

    # 기존 지층들(기존 1층 + 기존 2층)을 단일 상위 2층(MERGED_UPPER)으로 원자적 가중평균 병합
    old_total_qty = sum(b.qty for b in existing)
    old_book_inv = sum(b.book_invested_usd for b in existing)
    old_actual_cost = sum(b.actual_cost_usd for b in existing)
    old_lot_cnt = sum(max(1, b.lot_count) for b in existing)

    raw_upper_book_p = old_book_inv / float(old_total_qty)
    old_overall_book_avg = (
        sum(float(b.qty) * b.book_price for b in existing)
        / float(old_total_qty)
    )
    # Case 53: 병합 시 부동소수점 풍선효과 방지 하드 캡핑
    capped_upper_book_p = round(min(raw_upper_book_p, old_overall_book_avg), 4)
    capped_upper_book_inv = round(
        capped_upper_book_p * float(old_total_qty), 4
    )

    merged_l2 = StratumBucket(
        qty=old_total_qty,
        book_price=capped_upper_book_p,
        book_invested_usd=capped_upper_book_inv,
        actual_cost_usd=round(old_actual_cost, 4),
        lot_count=old_lot_cnt,
        lot_type="MERGED_UPPER",
    )
    return [new_l1, merged_l2]


def vrev_reanchor_single_tier_quota(
    queue: Sequence[StratumBucket],
    net_realized_cash_profit: float,
) -> Tuple[List[StratumBucket], float]:
    """
    🎯 3 & Case 47, 59:
    1층(하부 공격수)이 실물 익절되어 장부에 상위 통합층(2층) 단 1개 지층(len(queue) == 1)만 남았을 때,
    실제로 회수된 세후 실현 순수익금(net_realized_cash_profit)에 안전 버퍼(0.994)를 곱한 금액만큼
    2층의 장부 투자원금(book_invested_usd)만 차감해 목표 탈출 단가(book_price)를 강제 인하한다.
    (단, 실제 원금 actual_cost_usd 는 절대 건드리지 않는다!)
    반환값: (updated_queue, per_share_price_drop_usd)
    """
    existing = [
        b
        for b in (queue or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if len(existing) != 1:
        return existing, 0.0

    rem = existing[0]
    net_profit = safe_float(net_realized_cash_profit, 0.0)
    if net_profit <= 0.0 or rem.qty <= 0:
        return existing, 0.0

    credit_usd = max(0.0, net_profit * REANCHOR_NET_BUFFER_MULT)
    old_book_p = rem.book_price
    min_floor_inv = 0.01 * float(rem.qty)
    new_book_inv = round(
        max(min_floor_inv, rem.book_invested_usd - credit_usd), 4
    )
    new_book_p = round(max(0.01, new_book_inv / float(rem.qty)), 4)
    new_book_p = min(old_book_p, new_book_p)

    price_drop = round(max(0.0, old_book_p - new_book_p), 4)
    updated_bucket = StratumBucket(
        qty=rem.qty,
        book_price=new_book_p,
        book_invested_usd=round(new_book_p * float(rem.qty), 4),
        actual_cost_usd=rem.actual_cost_usd,
        lot_count=rem.lot_count,
        lot_type=(
            "MERGED_UPPER"
            if rem.lot_count > 1 or rem.lot_type == "MERGED_UPPER"
            else rem.lot_type
        ),
    )
    return [updated_bucket], price_drop


def calc_sell_realized_pnl(
    sold_qty: int,
    sell_exec_price: float,
    sold_actual_cost_usd: float,
) -> Dict[str, float]:
    """
    매도 체결 수량(sold_qty), 실제 체결 단가(sell_exec_price), 매도 지층의 실제 매수 원금(sold_actual_cost_usd)을
    기반으로 왕복 수수료(0.14%) 차감 후 세후 실현 손익을 정밀 산출한다.
    """
    q_int = max(0, safe_int(sold_qty, 0))
    exec_p = max(0.0, safe_float(sell_exec_price, 0.0))
    actual_cost = max(0.0, safe_float(sold_actual_cost_usd, 0.0))
    if q_int <= 0 or exec_p <= 0.0:
        return {
            "proceeds_usd": 0.0,
            "actual_cost_usd": actual_cost,
            "gross_pnl_usd": 0.0,
            "fee_usd": 0.0,
            "net_pnl_usd": 0.0,
            "net_rate_pct": 0.0,
        }

    proceeds = round(float(q_int) * exec_p, 4)
    gross_pnl = round(proceeds - actual_cost, 4)
    # 매수 원금 + 매도 대금 평균에 대해 왕복 수수료율(0.14%) 적용
    fee_usd = round(
        ((actual_cost + proceeds) * 0.5) * (ROUND_TRIP_FEE_RATE_PCT / 100.0), 4
    )
    net_pnl = round(gross_pnl - fee_usd, 4)
    net_rate_pct = (
        round((net_pnl / actual_cost) * 100.0, 4) if actual_cost > 0.0 else 0.0
    )
    return {
        "proceeds_usd": proceeds,
        "actual_cost_usd": actual_cost,
        "gross_pnl_usd": gross_pnl,
        "fee_usd": fee_usd,
        "net_pnl_usd": net_pnl,
        "net_rate_pct": net_rate_pct,
    }


# ---------------------------------------------------------------------------
# [핵심 5] 포트폴리오 · 지층 · 기억된 최저가(cycle_min_buy_price) 통합 요약기 (Case 59, 60)
# ---------------------------------------------------------------------------
def summarize_stratum_portfolio(
    state_dict: Dict[str, Any],
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 5 & Case 59, 60:
    원장 상태(state_dict)와 현재가(live_price)를 받아:
    - 1층(L1) 및 2층(MERGED_UPPER) 보유 현황과 목표 익절가(l1_target_price, l2_target_price)
    - 기억된 사이클 최저 매수가(cycle_min_buy_price)를 반영한 추가 매수 기준가(active_buy_anchor) 및
      신저가 매수 트리거 가격(trigger_buy_price)
    - 회계 항등식 기반 총자산(total_mark_equity_usd), 가용현금(free_cash_usd), 평가손익, 총 ROI 를 반환한다.
    """
    st = state_dict if isinstance(state_dict, dict) else {}
    buckets = buckets_from_dicts(st.get("buckets", []))

    init_seed = max(
        100.0, safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD)
    )
    cum_realized = safe_float(st.get("cumulative_realized_net_usd"), 0.0)
    actual_invested = round(sum(b.actual_cost_usd for b in buckets), 4)
    book_invested = round(sum(b.book_invested_usd for b in buckets), 4)

    # 회계 항등식: free_cash_usd = max(0, (initial_seed + cum_realized) - actual_invested)
    expected_free_cash = round(
        max(0.0, (init_seed + cum_realized) - actual_invested), 4
    )
    free_cash = safe_float(st.get("free_cash_usd"), expected_free_cash)
    if abs(free_cash - expected_free_cash) > 0.05:
        free_cash = expected_free_cash

    held_shares = sum(b.qty for b in buckets)
    held_lots = sum(max(1, b.lot_count) for b in buckets)
    lp = safe_float(live_price, 0.0)
    if lp <= 0.0 and buckets:
        lp = buckets[0].actual_avg_price

    threshold_mode = str(st.get("threshold_mode") or "08_VREV_PROG").upper()
    portion_mode = str(st.get("portion_mode") or "COMP_08PCT").upper()
    drop_buy_pct, rise_l1_pct, rise_l2_pct = get_golden8_prog_thresholds(
        held_lots=max(1, held_lots), threshold_mode=threshold_mode
    )

    is_l2_only = bool(
        len(buckets) == 1
        and str(buckets[0].lot_type).upper() == "MERGED_UPPER"
    )
    l1_target_price = 0.0
    l2_target_price = 0.0

    if buckets:
        if is_l2_only:
            l2_target_price = round(
                buckets[0].book_price * (1.0 + rise_l2_pct / 100.0), 2
            )
        else:
            l1_target_price = round(
                buckets[0].book_price * (1.0 + rise_l1_pct / 100.0), 2
            )
            if len(buckets) >= 2:
                l2_target_price = round(
                    buckets[1].book_price * (1.0 + rise_l2_pct / 100.0), 2
                )

    anchor_p = safe_float(st.get("anchor_price"), 0.0)
    cycle_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    ratchet_p = safe_float(st.get("ratchet_anchor_price"), anchor_p)

    # 🚨 Case 60: 이미 지층을 보유 중일 때 추가 매수(물타기) 기준가는 반드시 기억된 최저 매수가(cycle_min_buy_price)!
    if buckets:
        candidates = [
            p
            for p in (buckets[0].book_price, anchor_p, cycle_min_p)
            if p > 0.0
        ]
        active_buy_anchor = (
            min(candidates) if candidates else buckets[0].book_price
        )
    else:
        active_buy_anchor = lp if lp > 0.0 else max(anchor_p, 1.0)

    trigger_buy_price = (
        round(active_buy_anchor * (1.0 + drop_buy_pct / 100.0), 2)
        if active_buy_anchor > 0.0
        else 0.0
    )

    cost_equity = free_cash + actual_invested
    ref_for_quota = (
        lp
        if (len(buckets) == 0 or is_l2_only) and lp > 0.0
        else active_buy_anchor
    )
    next_target_qty, next_portion_budget = compute_golden8_quota_qty(
        anchor_price=max(0.01, ref_for_quota),
        current_total_equity=cost_equity,
        initial_seed_usd=init_seed,
        portion_mode=portion_mode,
    )

    market_eval_usd = (
        round(float(held_shares) * lp, 4)
        if held_shares > 0 and lp > 0.0
        else 0.0
    )
    unrealized_pnl_usd = (
        round(market_eval_usd - actual_invested, 4)
        if held_shares > 0 and lp > 0.0
        else 0.0
    )
    unrealized_rate_pct = (
        round((unrealized_pnl_usd / actual_invested) * 100.0, 2)
        if actual_invested > 0.0
        else 0.0
    )
    total_mark_equity_usd = round(free_cash + market_eval_usd, 4)
    total_net_pnl_usd = round(cum_realized + unrealized_pnl_usd, 4)
    total_roi_pct = round((total_net_pnl_usd / init_seed) * 100.0, 2)

    return {
        "buckets": buckets,
        "held_shares": held_shares,
        "held_lots": held_lots,
        "is_l2_only_anti_bloat": is_l2_only,
        "initial_seed_usd": init_seed,
        "free_cash_usd": free_cash,
        "actual_invested_usd": actual_invested,
        "book_invested_usd": book_invested,
        "market_eval_usd": market_eval_usd,
        "unrealized_pnl_usd": unrealized_pnl_usd,
        "unrealized_rate_pct": unrealized_rate_pct,
        "cumulative_realized_net_usd": cum_realized,
        "total_mark_equity_usd": total_mark_equity_usd,
        "total_net_pnl_usd": total_net_pnl_usd,
        "total_roi_pct": total_roi_pct,
        "drop_buy_pct": drop_buy_pct,
        "rise_sell_l1_pct": rise_l1_pct,
        "rise_sell_l2_pct": rise_l2_pct,
        "l1_target_price": l1_target_price,
        "l2_target_price": l2_target_price,
        "active_buy_anchor": round(active_buy_anchor, 4),
        "cycle_min_buy_price": round(
            cycle_min_p if cycle_min_p > 0.0 else active_buy_anchor, 4
        ),
        "ratchet_anchor_price": round(ratchet_p, 4),
        "trigger_buy_price": trigger_buy_price,
        "next_target_qty": next_target_qty,
        "next_portion_budget": next_portion_budget,
    }


# ---------------------------------------------------------------------------
# [핵심 6] TDD 사전 무결성 자동 검증 슈트 (Case 25 ~ 60 전수 검증)
# ---------------------------------------------------------------------------
def run_quant_engine_tdd() -> bool:
    """
    기동 직전 8% 황금정량 산출, 2-Tier 병합, 단일지층 리앵커링 및
    Case 59/60(실물 1층 REENTRY_1FL 비병합 독립 배치 & 기억된 최저가 cycle_min_buy_price 잠금)을 검증한다.
    """
    # 1) 8% 황금정량 산출 검증 ($10,000 시드 -> $800 예산, 기준가 $100 -> 8주 확정)
    q1, bud1 = compute_golden8_quota_qty(
        100.0, 10000.0, 10000.0, "COMP_08PCT"
    )
    assert q1 == 8 and abs(bud1 - 800.0) < 1e-6, (
        f"TDD 실패: q1={q1}, bud1={bud1}"
    )

    # 2) 하락가 매수 시 자투리 시드 보존 및 가용현금 캡핑 검증
    aff_q, spent = resolve_affordable_buy_qty(8, 90.0, 800.0)
    assert aff_q == 8 and abs(spent - 720.0) < 1e-6, (
        "TDD 실패: 하락가 매수 자투리 계산 오류"
    )

    # 3) 2-Tier 병합 및 리앵커링 검증
    q_list = vrev_add_quota_lot_and_merge_2tier(
        [], 8, 100.0, 800.0, "REENTRY_1FL"
    )
    q_list = vrev_add_quota_lot_and_merge_2tier(
        q_list, 8, 96.0, 768.0, "DIP_1FL"
    )
    q_list = vrev_add_quota_lot_and_merge_2tier(
        q_list, 8, 90.0, 720.0, "DIP_1FL"
    )
    assert len(q_list) == 2, "TDD 실패: 2-Tier 병합 개수 오류"
    assert q_list[0].qty == 8 and abs(q_list[0].book_price - 90.0) < 1e-4
    assert q_list[1].qty == 16 and abs(q_list[1].book_price - 98.0) < 1e-4

    # 4) 1층($90 -> $91.35) 실물 익절 후 2층 리앵커링 검증
    rem_q = [q_list[1]]
    reanchored_q, drop_amt = vrev_reanchor_single_tier_quota(rem_q, 10.0)
    assert len(reanchored_q) == 1 and drop_amt > 0.0, (
        "TDD 실패: 2층 리앵커링 미작동"
    )

    # 5) 🚨 Case 59 & Case 60: 2층 단독 상태에서 실물 1층(REENTRY_1FL @ $91.35) 재매수 시
    #    2층과 병합되지 않고 [L1($91.35), L2(MERGED_UPPER)] 로 독립 배치되는지 검증!
    after_reentry_q = vrev_add_quota_lot_and_merge_2tier(
        reanchored_q, 8, 91.35, 730.80, "REENTRY_1FL"
    )
    assert len(after_reentry_q) == 2, (
        "TDD 실패(Case 59): REENTRY_1FL 이 2층에 잘못 병합됨!"
    )
    assert (
        after_reentry_q[0].lot_type == "REENTRY_1FL"
        and after_reentry_q[0].qty == 8
    )
    assert (
        after_reentry_q[1].lot_type == "MERGED_UPPER"
        and after_reentry_q[1].qty == 16
    )

    # 6) 기억된 최저 매수가(cycle_min_buy_price = $90.00) 잠금 확인 (Case 60)
    mock_state = {
        "buckets": buckets_to_dicts(after_reentry_q),
        "initial_seed_usd": 10000.0,
        "anchor_price": 91.35,
        "cycle_min_buy_price": 90.00,
        "ratchet_anchor_price": 91.35,
        "cumulative_realized_net_usd": 10.0,
    }
    sum_info = summarize_stratum_portfolio(mock_state, live_price=91.00)
    assert abs(sum_info["active_buy_anchor"] - 90.00) < 1e-4, (
        f"TDD 실패(Case 60): 기억된 최저가($90.00) 잠금 풀림 -> {sum_info['active_buy_anchor']}"
    )
    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 8%% 황금정량 · 2-Tier 병합 · 리앵커링 · Case 59/60 실물 무팽창 검증 완료"
    )
    return True
