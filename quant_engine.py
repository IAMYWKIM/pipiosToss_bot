# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_engine.py
순수 롱(SOXL) 전용 '8%(12.5분할) 황금비율 정액제 기반 정량 산출기(min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑 Case 67 탑재) ·
자투리 절약금 및 세후 실물 수익금 실시간 시드 풀 복리 산입 ·
전술 A 챔피언(🛡️ 05_AF_WIDE_10_01) 보유 회차별 4단 와이드 점진 활주로(1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5% 및 1층 +1.50% / 2층 +1.00%) ·
2층 단독 보유 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 1층(REENTRY_1FL) 재장전 판정기 ·
프롬프트 2-Tier 가중평균 자동 병합(REENTRY_1FL 독립 1층 배치 vs DIP_1FL 2층 통합 · Case 53 하드 캡핑) ·
단일 지층 실물 수익금 리앵커링(0.994 버퍼) · 리앵커링 이중 차감 방지 진성 손익분기점 하한선(True Break-Even Floor · Case 69) ·
기억된 최저 매수가(cycle_min_buy_price) 잠금 요약 및 TDD 검증' 코어 모듈 (Step 04 / 05)
- MODIFIED (🎯 1 & Case 67): 정액제를 기반으로 한 정량제에서 상승 돌파 매수(exec_price > anchor_price) 시
  기존 고정 주수(anchor_quota_qty)로 인해 1회분(8%) 시드 예산(portion_budget)을 초과하는 현상을 원천 차단하기 위해
  target_qty = min(budget_cap_qty, anchor_quota_qty) 이중 캡핑 공식을 compute_golden8_quota_qty 및
  resolve_affordable_buy_qty, summarize_stratum_portfolio, run_quant_engine_tdd 에 100% 탑재!
- MODIFIED (🎯 2 & Case 36 ~ 45, 60): 보유 회차별 요구 하락 단차를 4단 와이드 활주로(1~4회 -1.00% / 5~8회 -2.00% /
  9~10회 -3.50% / 11회+ -5.50%)로 유지하여 대폭락장 시드 조기 고갈 및 무리셋 장기 동결을 원천 차단!
- MODIFIED (🎯 4 & Case 59): 2층(MERGED_UPPER) 단독 보유 시 직전 익절가(anchor_price) 대비 -1.00% 이하 눌림목(PULLBACK_RELOAD)
  또는 +1.00% 이상 상승 돌파(BREAKOUT_RELOAD) 시에만 독립 실물 1층(REENTRY_1FL) 재장전을 승인하는
  evaluate_l2_only_hybrid_reentry_signal() 함수 및 관제탑(main.py / tg_ui_plugin.py / defense_plugin.py) 전 필드 100% 매핑!
- NEW (🎯 3 & Case 69):
  1) enforce_true_breakeven_floor_on_buckets() 및 vrev_reanchor_single_tier_quota() 내 max_allowed_total_discount_usd 상한 캡핑으로
     보유 지층의 리앵커링 할인 총액(actual_cost_usd - book_invested_usd)이 현재 사이클 내 실수확된 리앵커링 크레딧 한도를
     절대 초과할 수 없도록 진성 손익분기점 하한선(True Break-Even Floor)을 물리적으로 강제!
  2) run_quant_engine_tdd() 에 Case 69 (이동평균 희석 이중 차감 차단 및 진성 흑자 졸업 보장) 검증 테스트 추가!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 69 완벽 준수
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import toss_api
from toss_api import safe_float, safe_int

logger = logging.getLogger("toss_sniper.quant_engine")

# toss_api.py 내부 상수 존재 여부와 무관하게 100% 안전하게 작동하는 자립형 폴백 상수 정의
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

# 🎯 4 & Case 59: 2층 단독 보유 시 10분 게이트 독립 1층(REENTRY_1FL) 눌림목/상승돌파 임계치(%)
REENTRY_PULLBACK_DROP_PCT: float = -1.00
REENTRY_BREAKOUT_RISE_PCT: float = 1.00


# ---------------------------------------------------------------------------
# [핵심 1] StratumBucket : 리앵커링 장부단가(book_price)와 실제 매수원금(actual_cost_usd) 분리 회계 객체
# ---------------------------------------------------------------------------
@dataclass
class StratumBucket:
    """
    🎯 3 & Case 46 ~ 69:
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
# [핵심 1-B · NEW Case 69] 진성 손익분기점 하한선(True Break-Even Floor) 강제 정규화기
# ---------------------------------------------------------------------------
def enforce_true_breakeven_floor_on_buckets(
    buckets: Sequence[StratumBucket],
    max_allowed_discount_usd: Optional[float] = None,
) -> List[StratumBucket]:
    """
    🚨 NEW [Case 69 진성 손익분기점 하한선(True Break-Even Floor) 강제]:
    보유 지층(특히 2층 MERGED_UPPER)의 리앵커링 할인 총액(actual_cost_usd - book_invested_usd)이
    현재 사이클에서 실제 1층 익절로 수확된 리앵커링 크레딧 상한(max_allowed_discount_usd)을
    절대 초과하지 못하도록 book_invested_usd 와 book_price 의 하한선을 물리적으로 강제한다.
    (토스 이동평균 희석 단가 덮어쓰기 및 리앵커링 이중 차감으로 인한 조기 손절 졸업 영구 봉쇄!)
    """
    valid = [
        b
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not valid or max_allowed_discount_usd is None:
        return valid

    rem_allowable_disc = max(0.0, safe_float(max_allowed_discount_usd, 0.0))
    adjusted: List[StratumBucket] = []

    # 2층(MERGED_UPPER)이 존재하는 경우 2층에 우선적으로 허용 크레딧을 배정하고 1층은 순수 단가 유지
    for idx, b in enumerate(valid):
        cur_disc = max(0.0, b.actual_cost_usd - b.book_invested_usd)
        if len(valid) >= 2 and idx == 0 and str(b.lot_type).upper() != "MERGED_UPPER":
            capped_disc = 0.0
        else:
            capped_disc = min(cur_disc, rem_allowable_disc)
            rem_allowable_disc = max(0.0, rem_allowable_disc - capped_disc)

        min_floor_inv = max(
            0.01 * float(b.qty),
            round(b.actual_cost_usd - capped_disc, 4),
        )
        eff_book_inv = max(b.book_invested_usd, min_floor_inv)
        eff_book_p = round(
            min(b.actual_avg_price, max(0.01, eff_book_inv / float(b.qty))),
            4,
        )
        adjusted.append(
            StratumBucket(
                qty=b.qty,
                book_price=eff_book_p,
                book_invested_usd=round(eff_book_p * float(b.qty), 4),
                actual_cost_usd=round(b.actual_cost_usd, 4),
                lot_count=b.lot_count,
                lot_type=b.lot_type,
            )
        )
    return adjusted


# ---------------------------------------------------------------------------
# [핵심 2] 8%(12.5분할) 황금비율 정액 기반 정량 주수 산출 및 min(1회분, 고정정량) 캡핑 (🎯 1 & Case 25 ~ 35, 67)
# ---------------------------------------------------------------------------
def compute_golden8_quota_qty(
    anchor_price: float,
    current_total_equity: float,
    initial_seed_usd: float = DEFAULT_INITIAL_SEED_USD,
    portion_mode: str = "COMP_08PCT",
    exec_price: Optional[float] = None,
) -> Tuple[int, float]:
    """
    🎯 1 & Case 25 ~ 35 & Case 67:
    직전 매매 기준가(anchor_price)를 기반으로 8%(12.5분할) 황금비율 1회분 예산(portion_budget)과
    고정 목표 매수 주수(anchor_quota_qty)를 산출하되, 실제 매수 지정가(exec_price)가 제공된 경우
    상승 돌파 등으로 인해 1회분(8%) 예산을 초과하지 않도록 min(budget_cap_qty, anchor_quota_qty)로 캡핑한다.
    - COMP_08PCT (챔피언 고정 모드):
      portion_budget = round(max(initial_seed_usd, current_total_equity) * 0.08, 4)
    - anchor_quota_qty = max(1, int(math.floor((portion_budget + 1e-9) / max(0.01, anchor_price))))
    - NEW [Case 67]: exec_price > 0 일 때
      budget_cap_qty = max(1, int(math.floor((portion_budget + 1e-9) / max(0.01, exec_price))))
      target_qty = min(budget_cap_qty, anchor_quota_qty)
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
    anchor_quota_qty = max(1, int(math.floor((portion_budget + 1e-9) / ref_p)))

    # 🚨 Case 67: 실제 매수가(exec_price)가 기준가보다 높을 때 1회분(8%) 예산 초과 원천 차단!
    if exec_price is not None and safe_float(exec_price, 0.0) > 0.0:
        eff_exec_p = max(0.01, safe_float(exec_price, ref_p))
        budget_cap_qty = max(
            1, int(math.floor((portion_budget + 1e-9) / eff_exec_p))
        )
        target_qty = min(budget_cap_qty, anchor_quota_qty)
    else:
        target_qty = anchor_quota_qty

    return target_qty, portion_budget


def resolve_affordable_buy_qty(
    target_qty: int,
    price_t: float,
    free_cash_usd: float,
    broker_buying_power_usd: Optional[float] = None,
    max_portion_budget_usd: Optional[float] = None,
) -> Tuple[int, float]:
    """
    🚨 Case 25 ~ 35 & Case 67:
    직전 기준가로 산출된 목표 정량(target_qty)을 실제 주문가(price_t)로 매수하기 직전:
    1) NEW [Case 67]: max_portion_budget_usd(1회분 8% 예산)가 지정된 경우,
       budget_cap_qty = max(1, int(math.floor((max_portion_budget_usd + 1e-9) / price_t))) 와
       target_qty 중 작은 값인 min(budget_cap_qty, target_qty) 로 1회분 예산 초과를 한 번 더 하드 캡핑한다.
    2) 왕복 수수료 버퍼(0.14%)를 감안한 가용 현금(free_cash_usd) 및 토스 실계좌 매수가능금액
       (broker_buying_power_usd) 한도 내 매수 가능 정수 주수로 캡핑하여
       422 insufficient-buying-power 에러를 100% 원천 차단한다.
    반환값: (buy_qty, actual_spent_usd)
    """
    t_qty = max(0, safe_int(target_qty, 0))
    exec_p = safe_float(price_t, 0.0)
    if t_qty <= 0 or exec_p <= 0.0:
        return 0, 0.0

    # 🚨 Case 67: 1회분(8%) 예산 상한 주수(budget_cap_qty) 하드 캡핑
    if (
        max_portion_budget_usd is not None
        and safe_float(max_portion_budget_usd, 0.0) > 0.0
    ):
        p_bud = safe_float(max_portion_budget_usd, 0.0)
        budget_cap_qty = max(1, int(math.floor((p_bud + 1e-9) / exec_p)))
        t_qty = min(budget_cap_qty, t_qty)

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
# [핵심 3] 전술 A 보유 회차별 4단 와이드 점진 단차 & 하이브리드 재장전 판정기 (🎯 2, 🎯 4 & Case 36 ~ 60, 67)
# ---------------------------------------------------------------------------
def get_golden8_prog_thresholds(
    held_lots: int,
    threshold_mode: str = "08_VREV_PROG",
) -> Tuple[float, float, float]:
    """
    MODIFIED [🎯 2 & Case 36 ~ 45, 60 : 전술 A (🛡️ 05_AF_WIDE_10_01) 4단 와이드 점진 활주로]:
    현재 보유 중인 총 투입 회차(held_lots)에 따른:
    1) 10분 시드보호 매수 게이트 요구 하락률(drop_buy_pct):
       -  1 ~  4회차 보유 중 : -1.00%
       -  5 ~  8회차 보유 중 : -2.00%
       -  9 ~ 10회차 보유 중 : -3.50%
       - 11회차 이상 보유 중 : -5.50%
    2) 1분 상시 스나이퍼 매도 게이트 목표 상승률(rise_sell_l1_pct, rise_sell_l2_pct):
       - 1층(공격수) +1.50% / 2층(통합층) +1.00%
    반환값: (drop_buy_pct, rise_sell_l1_pct, rise_sell_l2_pct)
    """
    lots = max(1, safe_int(held_lots, 1))
    if lots <= 4:
        drop_buy_pct = -1.00
    elif lots <= 8:
        drop_buy_pct = -2.00
    elif lots <= 10:
        drop_buy_pct = -3.50
    else:
        drop_buy_pct = -5.50

    mode = str(threshold_mode or "08_VREV_PROG").upper().strip()
    if mode == "01_PROG_LOT":
        prog_rise = 1.00 if lots <= 6 else (1.50 if lots <= 11 else 2.00)
        return drop_buy_pct, prog_rise, prog_rise
    return drop_buy_pct, 1.50, 1.00


def evaluate_l2_only_hybrid_reentry_signal(
    live_price: float,
    anchor_price: float,
    l2_book_price: float = 0.0,
) -> Dict[str, Any]:
    """
    MODIFIED [🎯 4 & Case 59, Case 67 : 2층(MERGED_UPPER) 단독 보유 시 눌림목(-1.00%) OR 상승돌파(+1.00%) 독립 1층 재장전 판정기]:
    1층이 실물 익절되어 장부에 상위 2층만 단독으로 남아 있을 때,
    반등 고점에서의 이중 현금 소진을 막기 위해 회수 현금을 보유하다가 10분 매수 게이트에서:
    - 조건 A (눌림목 재장전 : chg_from_exit <= -1.00%):
      직전 익절가(ref_anchor) 기준으로 8% 고정 정량을 확정하고 min(1회분 예산 상한 주수, 기준가 정량)을 적용하여
      더 싸진 현재가에 REENTRY_1FL 매수 승인!
    - 조건 B (상승돌파 재장전 : chg_from_exit >= +1.00%):
      눌림 없이 위로 치고 나가는 상승 추세 돌파로 판정하되, Case 67 에 따라 직전 익절가(ref_anchor) 기준 고정 정량과
      현재가(live_price) 기준 1회분 예산 상한 주수 중 작은 값(min(budget_cap_qty, anchor_quota_qty))을 적용하여
      1회분(8%) 시드 예산 초과 없이 REENTRY_1FL 매수 승인!
    - 불발 시 (-1.00% < chg_from_exit < +1.00%):
      고점 부근 횡보 구간으로 판정하여 매수를 패스(HOLD_WAIT_GATE)하고 현금 시드를 보호한다.
    """
    lp = safe_float(live_price, 0.0)
    anc = safe_float(anchor_price, 0.0)
    b2_p = safe_float(l2_book_price, 0.0)
    ref_anchor = anc if anc > 0.0 else (b2_p if b2_p > 0.0 else lp)

    pullback_target_p = (
        round(ref_anchor * (1.0 + REENTRY_PULLBACK_DROP_PCT / 100.0), 2)
        if ref_anchor > 0.0
        else 0.0
    )
    breakout_target_p = (
        round(ref_anchor * (1.0 + REENTRY_BREAKOUT_RISE_PCT / 100.0), 2)
        if ref_anchor > 0.0
        else 0.0
    )

    if lp <= 0.0 or ref_anchor <= 0.0:
        return {
            "should_buy": False,
            "signal_type": "NO_PRICE",
            "ref_anchor": round(ref_anchor, 4),
            "quota_ref_price": round(ref_anchor, 4),
            "chg_from_exit_pct": 0.0,
            "pullback_target_price": pullback_target_p,
            "breakout_target_price": breakout_target_p,
        }

    chg_from_exit_pct = ((lp - ref_anchor) / ref_anchor) * 100.0

    if chg_from_exit_pct <= REENTRY_PULLBACK_DROP_PCT:
        return {
            "should_buy": True,
            "signal_type": "PULLBACK_RELOAD",
            "ref_anchor": round(ref_anchor, 4),
            "quota_ref_price": round(ref_anchor, 4),
            "chg_from_exit_pct": round(chg_from_exit_pct, 2),
            "pullback_target_price": pullback_target_p,
            "breakout_target_price": breakout_target_p,
        }

    if chg_from_exit_pct >= REENTRY_BREAKOUT_RISE_PCT:
        return {
            "should_buy": True,
            "signal_type": "BREAKOUT_RELOAD",
            "ref_anchor": round(ref_anchor, 4),
            "quota_ref_price": round(ref_anchor, 4),
            "chg_from_exit_pct": round(chg_from_exit_pct, 2),
            "pullback_target_price": pullback_target_p,
            "breakout_target_price": breakout_target_p,
        }

    return {
        "should_buy": False,
        "signal_type": "HOLD_WAIT_GATE",
        "ref_anchor": round(ref_anchor, 4),
        "quota_ref_price": round(ref_anchor, 4),
        "chg_from_exit_pct": round(chg_from_exit_pct, 2),
        "pullback_target_price": pullback_target_p,
        "breakout_target_price": breakout_target_p,
    }


# ---------------------------------------------------------------------------
# [핵심 4] 2-Tier 가중평균 자동 병합 (REENTRY_1FL 독립 배치 vs DIP_1FL 병합) & 리앵커링 (Case 69 탑재)
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
       단독으로 남아 있을 때, 10분 게이트 눌림/돌파로 재장전한 실물 1층(lot_type == "REENTRY_1FL")이 체결되면:
       기존 2층(MERGED_UPPER)을 재병합하거나 건드리지 않고 그대로 2층(queue[1])에 유지한 채,
       신규 매수분을 독립된 1층(queue[0])에 배치하여 [new_l1(REENTRY_1FL), existing_l2(MERGED_UPPER)] 를 반환한다!
    3) 이미 1층이 존재하는 상태에서 기억된 최저가를 깨고 내려가 10분 게이트 4단 와이드 신저가 추매(DIP_1FL / QUOTA_L1)가
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
    max_allowed_total_discount_usd: Optional[float] = None,
) -> Tuple[List[StratumBucket], float]:
    """
    🎯 3 & Case 47, 59, 69:
    1층(하부 공격수)이 실물 익절되어 장부에 상위 통합층(2층) 단 1개 지층(len(queue) == 1)만 남았을 때,
    실제로 회수된 세후 실현 순수익금(net_realized_cash_profit)에 안전 버퍼(0.994)를 곱한 금액만큼
    2층의 장부 투자원금(book_invested_usd)만 차감해 목표 탈출 단가(book_price)를 강제 인하한다.
    - 단, 실제 원금 actual_cost_usd 는 절대 건드리지 않는다!
    - 🚨 NEW [Case 69]: max_allowed_total_discount_usd 가 지정된 경우,
      누적 할인 총액(actual_cost_usd - new_book_inv)이 max_allowed_total_discount_usd 를 초과하지 못하도록
      진성 손익분기점 하한선(True Break-Even Floor)을 강제한다!
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

    if (
        max_allowed_total_discount_usd is not None
        and safe_float(max_allowed_total_discount_usd, -1.0) >= 0.0
    ):
        allowed_disc = max(0.0, safe_float(max_allowed_total_discount_usd, 0.0))
        min_floor_inv = max(
            min_floor_inv, round(rem.actual_cost_usd - allowed_disc, 4)
        )

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
# [핵심 5] 포트폴리오 · 지층 · 기억된 최저가 · 하이브리드 1층 재장전 · Case 67/69 통합 요약기
# ---------------------------------------------------------------------------
def summarize_stratum_portfolio(
    state_dict: Dict[str, Any],
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 1, 🎯 2, 🎯 4, 🎯 5 & Case 59, 60, 67, 69:
    원장 상태(state_dict)와 현재가(live_price)를 받아:
    - 1층(L1) 및 2층(MERGED_UPPER) 보유 현황, 목표 익절가(l1_target_price, l2_target_price) 및 실시간 수익률(l1_gain_pct, l2_gain_pct)
    - NEW [Case 69]: cycle_reanchor_credit_usd 가 존재할 때 enforce_true_breakeven_floor_on_buckets() 를 적용하여
      2층 장부단가(book_price)와 졸업 목표가(l2_target_price)가 진성 손익분기점 밑으로 왜곡되지 않도록 보장!
    - 2층 단독 보유(is_l2_only_anti_bloat) 시 직전 익절가(reentry_ref_anchor) 대비
      눌림목 재장전 타점(reentry_pullback_price, -1.00%) 및 상승돌파 재장전 타점(reentry_breakout_price, +1.00%)
    - 1층 보유 중 기억된 사이클 최저 매수가(cycle_min_buy_price)를 반영한 4단 와이드 활주로
      추가 매수 기준가(active_buy_anchor / ref_buy_anchor) 및 신저가 매수 트리거 가격(trigger_buy_price)
    - Case 67: 기준가 기반 고정 정량(anchor_quota_qty), 현재가 기준 1회분 예산 상한 주수(budget_cap_qty),
      그리고 min(budget_cap_qty, anchor_quota_qty) 가 적용된 최종 목표 매수 주수(next_target_qty)
    - 회계 항등식 기반 총자산(mark_equity_usd / total_mark_equity_usd), 가용현금(free_cash_usd),
      실현기준 총자산(realized_equity_usd), 평가손익, 실현/총 ROI 및 누적 통계 필드를 100% 반환한다.
    """
    st = state_dict if isinstance(state_dict, dict) else {}
    raw_buckets = buckets_from_dicts(st.get("buckets", []))

    init_seed = max(
        100.0, safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD)
    )
    cum_realized = round(
        safe_float(st.get("cumulative_realized_net_usd"), 0.0), 4
    )

    # 🚨 Case 69: cycle_reanchor_credit_usd 기반 진성 손익분기점 하한선 적용
    raw_cycle_credit = safe_float(st.get("cycle_reanchor_credit_usd"), -1.0)
    if raw_cycle_credit >= 0.0:
        buckets = enforce_true_breakeven_floor_on_buckets(
            raw_buckets, max_allowed_discount_usd=raw_cycle_credit
        )
        eff_cycle_credit = round(raw_cycle_credit, 4)
    else:
        buckets = raw_buckets
        eff_cycle_credit = round(
            max(
                0.0,
                sum(b.actual_cost_usd - b.book_invested_usd for b in buckets),
            ),
            4,
        )

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

    actual_avg_p = (
        round(actual_invested / float(held_shares), 4)
        if held_shares > 0
        else 0.0
    )
    book_avg_p = (
        round(book_invested / float(held_shares), 4)
        if held_shares > 0
        else 0.0
    )

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
    l1_gain_pct = 0.0
    l2_gain_pct = 0.0

    if buckets:
        if is_l2_only:
            b2 = buckets[0]
            l2_target_price = round(
                b2.book_price * (1.0 + rise_l2_pct / 100.0), 2
            )
            if lp > 0.0 and b2.book_price > 0.0:
                l2_gain_pct = round(
                    ((lp - b2.book_price) / b2.book_price) * 100.0, 2
                )
        else:
            b1 = buckets[0]
            l1_target_price = round(
                b1.book_price * (1.0 + rise_l1_pct / 100.0), 2
            )
            if lp > 0.0 and b1.book_price > 0.0:
                l1_gain_pct = round(
                    ((lp - b1.book_price) / b1.book_price) * 100.0, 2
                )
            if len(buckets) >= 2:
                b2 = buckets[1]
                l2_target_price = round(
                    b2.book_price * (1.0 + rise_l2_pct / 100.0), 2
                )
                if lp > 0.0 and b2.book_price > 0.0:
                    l2_gain_pct = round(
                        ((lp - b2.book_price) / b2.book_price) * 100.0, 2
                    )

    anchor_p = safe_float(st.get("anchor_price"), 0.0)
    cycle_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    ratchet_p = safe_float(st.get("ratchet_anchor_price"), anchor_p)

    # 2층 단독 보유 시 눌림목(-1.00%) / 상승돌파(+1.00%) 독립 1층 재장전 타점 산출
    l2_book_p = buckets[0].book_price if is_l2_only and buckets else 0.0
    reentry_sig = evaluate_l2_only_hybrid_reentry_signal(
        live_price=lp,
        anchor_price=anchor_p,
        l2_book_price=l2_book_p,
    )
    reentry_ref_anchor = safe_float(reentry_sig.get("ref_anchor"), anchor_p)
    reentry_pullback_price = safe_float(
        reentry_sig.get("pullback_target_price"), 0.0
    )
    reentry_breakout_price = safe_float(
        reentry_sig.get("breakout_target_price"), 0.0
    )
    chg_from_reentry_anchor_pct = safe_float(
        reentry_sig.get("chg_from_exit_pct"), 0.0
    )

    # 🚨 Case 60: 이미 지층을 보유 중일 때 2층 병합 추매(DIP_1FL) 기준가는 반드시 기억된 최저 매수가(cycle_min_buy_price)!
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

    # 관제 보드(/avwap)용 ref_buy_anchor 및 chg_from_buy_anchor_pct 정밀 산출
    if is_l2_only:
        ref_buy_anchor = (
            reentry_ref_anchor
            if reentry_ref_anchor > 0.0
            else active_buy_anchor
        )
        chg_from_buy_anchor_pct = chg_from_reentry_anchor_pct
    else:
        ref_buy_anchor = active_buy_anchor
        chg_from_buy_anchor_pct = (
            round(((lp - active_buy_anchor) / active_buy_anchor) * 100.0, 2)
            if (lp > 0.0 and active_buy_anchor > 0.0)
            else 0.0
        )

    cost_equity = free_cash + actual_invested
    if len(buckets) == 0:
        ref_for_quota = lp if lp > 0.0 else max(anchor_p, 1.0)
    elif is_l2_only:
        ref_for_quota = (
            reentry_ref_anchor
            if reentry_ref_anchor > 0.0
            else (lp if lp > 0.0 else active_buy_anchor)
        )
    else:
        ref_for_quota = active_buy_anchor

    # Case 67: 기준가 기반 고정 정량(anchor_quota_qty)과 현재가(lp) 기준 1회분 예산 상한 주수(budget_cap_qty)의 min() 산출
    anchor_quota_qty, next_portion_budget = compute_golden8_quota_qty(
        anchor_price=max(0.01, ref_for_quota),
        current_total_equity=cost_equity,
        initial_seed_usd=init_seed,
        portion_mode=portion_mode,
        exec_price=None,
    )
    eff_exec_for_cap = lp if lp > 0.0 else max(0.01, ref_for_quota)
    budget_cap_qty = max(
        1, int(math.floor((next_portion_budget + 1e-9) / eff_exec_for_cap))
    )
    next_target_qty = min(budget_cap_qty, anchor_quota_qty)

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

    realized_equity_usd = round(max(0.0, init_seed + cum_realized), 4)
    realized_roi_pct = round((cum_realized / init_seed) * 100.0, 2)
    total_mark_equity_usd = round(free_cash + market_eval_usd, 4)
    total_net_pnl_usd = round(cum_realized + unrealized_pnl_usd, 4)
    total_roi_pct = round((total_net_pnl_usd / init_seed) * 100.0, 2)

    reanchor_cnt = safe_int(st.get("reanchor_cnt"), 0)
    cum_reanchor_drop = round(
        safe_float(st.get("cumulative_reanchor_drop_usd"), 0.0), 4
    )
    seed_protect_pass_cnt = safe_int(st.get("seed_protect_pass_cnt"), 0)
    extra_13plus_buy_events = safe_int(st.get("extra_13plus_buy_events"), 0)
    cum_leftover_saved = round(
        safe_float(st.get("cumulative_leftover_saved_usd"), 0.0), 4
    )

    return {
        "buckets": buckets,
        "held_shares": held_shares,
        "held_lots": held_lots,
        "is_l2_only_anti_bloat": is_l2_only,
        "initial_seed_usd": init_seed,
        "free_cash_usd": free_cash,
        "actual_invested_usd": actual_invested,
        "book_invested_usd": book_invested,
        "actual_avg_price": actual_avg_p,
        "book_avg_price": book_avg_p,
        "cycle_reanchor_credit_usd": eff_cycle_credit,
        "market_eval_usd": market_eval_usd,
        "unrealized_pnl_usd": unrealized_pnl_usd,
        "unrealized_rate_pct": unrealized_rate_pct,
        "cumulative_realized_net_usd": cum_realized,
        "realized_equity_usd": realized_equity_usd,
        "realized_roi_pct": realized_roi_pct,
        "total_mark_equity_usd": total_mark_equity_usd,
        "mark_equity_usd": total_mark_equity_usd,
        "total_net_pnl_usd": total_net_pnl_usd,
        "total_roi_pct": total_roi_pct,
        "mark_roi_pct": total_roi_pct,
        "drop_buy_pct": drop_buy_pct,
        "rise_sell_l1_pct": rise_l1_pct,
        "rise_sell_l2_pct": rise_l2_pct,
        "l1_target_price": l1_target_price,
        "l2_target_price": l2_target_price,
        "l1_gain_pct": l1_gain_pct,
        "l2_gain_pct": l2_gain_pct,
        "active_buy_anchor": round(active_buy_anchor, 4),
        "ref_buy_anchor": round(ref_buy_anchor, 4),
        "chg_from_buy_anchor_pct": chg_from_buy_anchor_pct,
        "cycle_min_buy_price": round(
            cycle_min_p if cycle_min_p > 0.0 else active_buy_anchor, 4
        ),
        "ratchet_anchor_price": round(ratchet_p, 4),
        "trigger_buy_price": trigger_buy_price,
        "reentry_ref_anchor": round(reentry_ref_anchor, 4),
        "reentry_pullback_price": reentry_pullback_price,
        "reentry_breakout_price": reentry_breakout_price,
        "chg_from_reentry_anchor_pct": chg_from_reentry_anchor_pct,
        "next_target_qty": next_target_qty,
        "anchor_quota_qty": anchor_quota_qty,
        "budget_cap_qty": budget_cap_qty,
        "next_portion_budget": next_portion_budget,
        "reanchor_cnt": reanchor_cnt,
        "cumulative_reanchor_drop_usd": cum_reanchor_drop,
        "seed_protect_pass_cnt": seed_protect_pass_cnt,
        "extra_13plus_buy_events": extra_13plus_buy_events,
        "cumulative_leftover_saved_usd": cum_leftover_saved,
    }


# ---------------------------------------------------------------------------
# [핵심 6] TDD 사전 무결성 자동 검증 슈트 (Case 25 ~ 69 & 전술 A 4단 와이드 활주로 전수 검증)
# ---------------------------------------------------------------------------
def run_quant_engine_tdd() -> bool:
    """
    기동 직전 8% 황금정량 산출, Case 67 min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑,
    4단 와이드 활주로(-1.0% / -2.0% / -3.5% / -5.5%),
    2층 단독 시 눌림/돌파 하이브리드 독립 1층 재장전(Case 59), 기억된 최저가 잠금(Case 60),
    Case 69 (이동평균 희석 이중 차감 방지 진성 손익분기점 하한선) 및 관제 UI 필수 요약 필드를 전수 검증한다.
    """
    # 1) 8% 황금정량 산출 검증 ($10,000 시드 -> $800 예산, 기준가 $100 -> 8주 확정)
    q1, bud1 = compute_golden8_quota_qty(
        100.0, 10000.0, 10000.0, "COMP_08PCT"
    )
    assert q1 == 8 and abs(bud1 - 800.0) < 1e-6, (
        f"TDD 실패: q1={q1}, bud1={bud1}"
    )

    # 1-B) 🚨 Case 67: min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑 검증
    q_dip, bud_c67 = compute_golden8_quota_qty(
        82.39, 19155.74, 19000.0, "COMP_08PCT", exec_price=79.52
    )
    q_brk, _ = compute_golden8_quota_qty(
        82.39, 19155.74, 19000.0, "COMP_08PCT", exec_price=86.00
    )
    assert q_dip == 18, f"TDD 실패(Case 67 하락 고정정량): q_dip={q_dip}"
    assert q_brk == 17 and (q_brk * 86.00) <= bud_c67, (
        f"TDD 실패(Case 67 상승돌파 예산초과 차단): q_brk={q_brk}, bud={bud_c67}"
    )

    aff_brk_q, spent_brk = resolve_affordable_buy_qty(
        target_qty=18,
        price_t=86.00,
        free_cash_usd=8909.61,
        max_portion_budget_usd=bud_c67,
    )
    assert aff_brk_q == 17 and abs(spent_brk - 1462.0) < 1e-4, (
        f"TDD 실패(Case 67 resolve_affordable_buy_qty 캡핑): aff_brk_q={aff_brk_q}"
    )

    # 2) 하락가 매수 시 자투리 시드 보존 및 가용현금 캡핑 검증
    aff_q, spent = resolve_affordable_buy_qty(8, 90.0, 800.0)
    assert aff_q == 8 and abs(spent - 720.0) < 1e-6, (
        "TDD 실패: 하락가 매수 자투리 계산 오류"
    )

    # 3) 전술 A 4단 와이드 활주로 단차 검증 (1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5%)
    d_l1, s1, s2 = get_golden8_prog_thresholds(1, "08_VREV_PROG")
    d_l4, _, _ = get_golden8_prog_thresholds(4, "08_VREV_PROG")
    d_l5, _, _ = get_golden8_prog_thresholds(5, "08_VREV_PROG")
    d_l8, _, _ = get_golden8_prog_thresholds(8, "08_VREV_PROG")
    d_l9, _, _ = get_golden8_prog_thresholds(9, "08_VREV_PROG")
    d_l10, _, _ = get_golden8_prog_thresholds(10, "08_VREV_PROG")
    d_l11, _, _ = get_golden8_prog_thresholds(11, "08_VREV_PROG")
    assert (
        d_l1 == -1.00
        and d_l4 == -1.00
        and d_l5 == -2.00
        and d_l8 == -2.00
        and d_l9 == -3.50
        and d_l10 == -3.50
        and d_l11 == -5.50
        and s1 == 1.50
        and s2 == 1.00
    ), "TDD 실패: 4단 와이드 활주로 단차 불일치"

    # 4) 2-Tier 병합 및 리앵커링 검증
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

    # 5) 1층($90 -> $91.35) 실물 익절 후 2층 리앵커링 및 눌림/돌파 하이브리드 재장전 신호 검증
    rem_q = [q_list[1]]
    reanchored_q, drop_amt = vrev_reanchor_single_tier_quota(rem_q, 10.0)
    assert len(reanchored_q) == 1 and drop_amt > 0.0, (
        "TDD 실패: 2층 리앵커링 미작동"
    )
    assert abs(reanchored_q[0].actual_cost_usd - 1568.0) < 1e-4, (
        "TDD 실패(Case 69): 리앵커링 시 actual_cost_usd 변조 발생!"
    )

    sig_hold = evaluate_l2_only_hybrid_reentry_signal(91.00, 91.35, 97.38)
    sig_pull = evaluate_l2_only_hybrid_reentry_signal(90.40, 91.35, 97.38)
    sig_brk = evaluate_l2_only_hybrid_reentry_signal(92.30, 91.35, 97.38)
    assert not sig_hold["should_buy"], "TDD 실패: 횡보 구간 오작동"
    assert (
        sig_pull["should_buy"]
        and sig_pull["signal_type"] == "PULLBACK_RELOAD"
        and abs(sig_pull["quota_ref_price"] - 91.35) < 1e-4
    ), "TDD 실패: 눌림목 독립 1층 재장전 신호 오류"
    assert (
        sig_brk["should_buy"]
        and sig_brk["signal_type"] == "BREAKOUT_RELOAD"
        and abs(sig_brk["quota_ref_price"] - 91.35) < 1e-4
    ), "TDD 실패: 상승돌파 독립 1층 재장전 신호 오류"

    # 6) 🚨 Case 59 & Case 60: 2층 단독 상태에서 실물 1층(REENTRY_1FL @ $90.40) 재장전 시
    #    2층과 병합되지 않고 [L1($90.40), L2(MERGED_UPPER)] 로 독립 배치되며 기억저가($90.00)가 잠기는지 검증!
    after_reentry_q = vrev_add_quota_lot_and_merge_2tier(
        reanchored_q, 8, 90.40, 723.20, "REENTRY_1FL"
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

    mock_state = {
        "buckets": buckets_to_dicts(after_reentry_q),
        "initial_seed_usd": 10000.0,
        "anchor_price": 90.40,
        "cycle_min_buy_price": 90.00,
        "ratchet_anchor_price": 90.40,
        "cumulative_realized_net_usd": 10.0,
        "cycle_reanchor_credit_usd": 9.94,
    }
    sum_info = summarize_stratum_portfolio(mock_state, live_price=90.20)
    assert abs(sum_info["active_buy_anchor"] - 90.00) < 1e-4, (
        f"TDD 실패(Case 60): 기억된 최저가($90.00) 잠금 풀림 -> {sum_info['active_buy_anchor']}"
    )

    # 7) 🚨 Case 69: 이중 차감(Double-Discount) 오염 방어 하한선 검증
    # 실제 원금이 $11,783.56(133주 @ $88.60)이고 사이클 내 리앵커링 크레딧이 $154.81 일 때,
    # 누군가 book_invested_usd 를 $11,348.89($85.33)로 과도하게 깎아놓더라도
    # enforce_true_breakeven_floor_on_buckets 가 $11,628.75($87.4342)로 즉시 복구하여 적자 졸업을 막는지 검증!
    polluted_b2 = StratumBucket(
        qty=133,
        book_price=85.33,
        book_invested_usd=11348.89,
        actual_cost_usd=11783.56,
        lot_count=7,
        lot_type="MERGED_UPPER",
    )
    healed_list = enforce_true_breakeven_floor_on_buckets(
        [polluted_b2], max_allowed_discount_usd=154.81
    )
    assert len(healed_list) == 1
    assert healed_list[0].book_price >= 87.43, (
        f"TDD 실패(Case 69): 이중 차감 복구 실패 -> book_price={healed_list[0].book_price}"
    )

    assert (
        "mark_equity_usd" in sum_info
        and "ref_buy_anchor" in sum_info
        and "anchor_quota_qty" in sum_info
        and "budget_cap_qty" in sum_info
        and "cycle_reanchor_credit_usd" in sum_info
    ), "TDD 실패: 관제 보드 필수 요약 키 누락"
    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 전술 A (8%% 황금정량 · Case 67 min(1회분, 고정정량) 캡핑 · 4단 와이드 활주로 · 눌림/돌파 독립 1층 재장전 · Case 59/60/69) 검증 완료"
    )
    return True
