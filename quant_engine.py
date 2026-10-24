# -*- coding: utf-8 -*-
"""
# MODIFIED: quant_engine.py
순수 롱(SOXL) 전용 '8%(12.5분할) 황금비율 정액제 기반 정량 산출기(min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑 Case 67 탑재) ·
자투리 절약금 및 세후 실물 수익금 실시간 시드 풀 복리 산입 ·
최종 확정 챔피언(🔥 03_7G_PURE_10_01 / 01_7L_STATIC_CYCLE_MIN) 보유 회차별 4단 와이드 점진 활주로
(1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5% 및 1층 +1.50% / 2~7층 +1.00%) ·
상단 그리드(2층 ~ 7층) 보유 중 1층 공석 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 1층(REENTRY_1FL) 재장전 판정기 ·
1층 ~ 7층 순수 원가 골디락스 멀티 그리드 관리(MAX_GRID_TIERS_7 = 7 : 1층 공격수 + 2~6층 개별 완충층[5개] + 통합 7층 본대[GRID_L7] ·
REENTRY_1FL 독립 1층 배치 vs DIP_1FL 2~6층 개별 승격 및 8회차 이상 최상단 6층+7층 통합 7층 병합 · Case 53 하드 캡핑 · Case 71, Case 72) ·
2층 ~ 7층 통합 이동평균(wma_2_to_7) 산출 및 3중 다중 익절 판정(① 1층 +1.50% / ② 2~6층 개별 및 통합 7층 +1.00% / ③ 2~7층 통합 이동평균 +1.00% · Case 71, Case 72) ·
순수 실물 원가 무결성(book_price == actual_avg_price · 리앵커링 원가 차감 영구 폐기 · Case 69, Case 71) ·
정적 기억 최저 매수가(cycle_min_buy_price) 잠금 요약 및 TDD 검증' 코어 모듈 (Step 03 / 06)
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 72 완벽 준수
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

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

# 8%(12.5분할) 황금비율 기준 기본 분할 회차 및 1~7층 골디락스 멀티 그리드 최대 층수 상수 (Case 72)
BASE_MAX_LOTS_8PCT: int = 12
MAX_GRID_TIERS_7: int = int(getattr(toss_api, "MAX_GRID_TIERS_7", 7))
MAX_GRID_TIERS: int = MAX_GRID_TIERS_7
# 타 모듈 AST 심볼 임포트 호환을 위해 MAX_GRID_TIERS_12 별칭을 7 로 동시 유지
MAX_GRID_TIERS_12: int = MAX_GRID_TIERS_7

# 🎯 4 & Case 59, Case 71, Case 72: 상단 그리드(2~7층) 보유 중 1층 공석 시 10분 게이트 눌림목/상승돌파 임계치(%)
REENTRY_PULLBACK_DROP_PCT: float = -1.00
REENTRY_BREAKOUT_RISE_PCT: float = 1.00


# ---------------------------------------------------------------------------
# [핵심 1] StratumBucket : 순수 실물 매수 원가(book_price == actual_avg_price) 그리드 구조체
# ---------------------------------------------------------------------------
@dataclass
class StratumBucket:
    """
    🎯 3 & Case 46 ~ 72:
    SOXL 1층 ~ 7층 골디락스 멀티 그리드(1층 공격수 L1 / 2층 ~ 6층 개별 완충층 GRID_L2 ~ GRID_L6 / 통합 7층 본대 GRID_L7) 데이터 구조체.
    - qty              : 보유 정수 주식 수 (주)
    - book_price       : 목표 탈출 기준 단가 ($, 순수 원가 모드에서는 actual_avg_price 와 100% 동일)
    - book_invested_usd: 장부상 투자금 ($, 순수 원가 모드에서는 actual_cost_usd 와 100% 동일)
    - actual_cost_usd  : 실제 가용 현금 시드에서 지출된 순수 매수 원금 ($, 회계 항등식 보존용)
    - lot_count        : 해당 그리드 층에 포함된 누적 매수 회차 수 (1~6층은 개별 회차, 7층은 7회차 이상 통합 회차 수)
    - lot_type         : 지층 성격 태그 ("REENTRY_1FL", "DIP_1FL", "QUOTA_L1", "MANUAL_SYNC_L1", "GRID_L2" ~ "GRID_L7")
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
        a_cost = round(
            max(
                0.01 * float(q_int),
                safe_float(
                    self.actual_cost_usd,
                    safe_float(
                        self.book_invested_usd,
                        float(q_int) * safe_float(self.book_price, 0.01),
                    ),
                ),
            ),
            4,
        )
        pure_p = round(max(0.0001, a_cost / float(q_int)), 4)
        l_cnt = max(1, safe_int(self.lot_count, 1))
        return {
            "qty": q_int,
            "book_price": pure_p,
            "book_invested_usd": a_cost,
            "actual_cost_usd": a_cost,
            "lot_count": l_cnt,
            "lot_type": str(self.lot_type or "QUOTA_L1").upper().strip(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["StratumBucket"]:
        if not isinstance(data, dict):
            return None
        q_int = safe_int(data.get("qty"), 0)
        if q_int <= 0:
            return None
        raw_p = safe_float(
            data.get("book_price"), safe_float(data.get("buy_price"), 0.0)
        )
        raw_bi = safe_float(
            data.get("book_invested_usd"), float(q_int) * max(0.0, raw_p)
        )
        a_cost = safe_float(data.get("actual_cost_usd"), raw_bi)
        if a_cost <= 0.0 and raw_p > 0.0:
            a_cost = float(q_int) * raw_p
        if a_cost <= 0.0:
            return None
        # Case 69, Case 71 & Case 72: 순수 매수 원가(a_cost) 기준으로 단가와 장부원금을 100% 일치시켜 원가 훼손 원천 차단
        clean_cost = round(a_cost, 4)
        pure_p = round(clean_cost / float(q_int), 4)
        l_cnt = max(1, safe_int(data.get("lot_count"), 1))
        l_type = str(data.get("lot_type") or "QUOTA_L1").upper().strip()
        return cls(
            qty=q_int,
            book_price=pure_p,
            book_invested_usd=clean_cost,
            actual_cost_usd=clean_cost,
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
# [핵심 1-B · Case 69, Case 71 & Case 72] 그리드 층 판별기 & 진성 손익분기점(순수 원가) 하한선 정규화기
# ---------------------------------------------------------------------------
def is_upper_grid_tier(bucket_or_lot_type: Any) -> bool:
    """
    🚨 [Case 71 & Case 72 상단 그리드(2층 ~ 7층) 여부 결정론적 판별기]:
    - lot_type 이 "MERGED_UPPER" 이거나 "GRID_L2" ~ "GRID_L7" (즉, "GRID_L" 로 시작하되 "GRID_L1" 이 아닌 경우)이면
      상단 그리드(2층 ~ 7층) 버킷이므로 True 를 반환한다.
    - "REENTRY_1FL", "DIP_1FL", "QUOTA_L1", "MANUAL_SYNC_L1", "SYNCED_1FL", "GRID_L1" 등은
      최하단 활성 공격수 1층이므로 False 를 반환한다.
    """
    if isinstance(bucket_or_lot_type, StratumBucket):
        tag = str(bucket_or_lot_type.lot_type or "").upper().strip()
    elif isinstance(bucket_or_lot_type, dict):
        tag = str(bucket_or_lot_type.get("lot_type") or "").upper().strip()
    else:
        tag = str(bucket_or_lot_type or "").upper().strip()

    if tag == "MERGED_UPPER":
        return True
    if tag.startswith("GRID_L") and tag != "GRID_L1":
        return True
    return False


def has_active_l1_bucket(buckets: Sequence[StratumBucket]) -> bool:
    """
    🚨 [Case 71 & Case 72 활성 1층 공격수 보유 여부 판별기]:
    - buckets 가 비어 있지 않고 맨 앞(buckets[0])이 상단 그리드(GRID_L2 ~ GRID_L7 / MERGED_UPPER)가 아니면
      현재 활성 1층 공격수가 존재하는 상태(True)이다.
    - 만약 1층이 익절되어 빠져나가고 buckets[0] 이 GRID_L2(또는 MERGED_UPPER)라면
      현재 1층이 공석이고 상단 2층 ~ 7층 그리드만 보유 중인 상태(False)이다!
    """
    valid = [
        b
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not valid:
        return False
    return not is_upper_grid_tier(valid[0])


def relabel_upper_grid_tiers(
    upper_buckets: Sequence[StratumBucket],
    max_tiers: int = MAX_GRID_TIERS_7,
) -> List[StratumBucket]:
    """
    🚨 MODIFIED [Case 71 & Case 72 상단 그리드(2층 ~ 6층 개별 완충층 + 통합 7층 본대) 번호 정규화 및 합병기]:
    1층(공격수)을 제외한 상단 그리드 버킷 리스트를 받아:
    - max_upper_cnt = max_tiers - 1 (기본 6개 : GRID_L2 ~ GRID_L7) 기준으로
    - 2층 ~ 6층(GRID_L2 ~ GRID_L6, 5개 완충층)은 개별 순수 원가를 100% 분리 보존하고,
    - 7층을 초과하는 물량(valid[5:])은 최상단 '통합 7층 본대(GRID_L7)'로 가중평균 합병(Case 53 하드 캡핑 적용)하여 반환한다.
    """
    valid = [
        b
        for b in (upper_buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not valid:
        return []

    max_upper_cnt = max(1, safe_int(max_tiers, MAX_GRID_TIERS_7) - 1)
    if len(valid) > max_upper_cnt:
        kept = valid[: max_upper_cnt - 1]
        overflow = valid[max_upper_cnt - 1 :]
        tot_q = sum(b.qty for b in overflow)
        tot_cost = round(sum(b.actual_cost_usd for b in overflow), 4)
        tot_lots = sum(max(1, b.lot_count) for b in overflow)
        avg_p = round(max(0.01, tot_cost / float(tot_q)), 4)
        merged_top = StratumBucket(
            qty=tot_q,
            book_price=avg_p,
            book_invested_usd=tot_cost,
            actual_cost_usd=tot_cost,
            lot_count=tot_lots,
            lot_type=f"GRID_L{max_upper_cnt + 1}",
        )
        valid = kept + [merged_top]

    out: List[StratumBucket] = []
    for idx, b in enumerate(valid):
        tier_no = min(max_upper_cnt + 1, idx + 2)
        pure_cost = round(max(0.01 * float(b.qty), b.actual_cost_usd), 4)
        pure_p = round(max(0.01, pure_cost / float(b.qty)), 4)
        out.append(
            StratumBucket(
                qty=b.qty,
                book_price=pure_p,
                book_invested_usd=pure_cost,
                actual_cost_usd=pure_cost,
                lot_count=max(1, b.lot_count),
                lot_type=f"GRID_L{tier_no}",
            )
        )
    return out


def enforce_true_breakeven_floor_on_buckets(
    buckets: Sequence[StratumBucket],
    max_allowed_discount_usd: Optional[float] = None,
) -> List[StratumBucket]:
    """
    🚨 [Case 69, Case 71 & Case 72 진성 손익분기점(순수 매수 원가) 하한선 강제]:
    챔피언(🔥 03_7G_PURE_10_01)은 인위적 리앵커링 차감을 수행하지 않으므로,
    모든 보유 지층(1층 ~ 7층)의 book_invested_usd 와 book_price 가 실제 매수 원금(actual_cost_usd / actual_avg_price)
    밑으로 절대 내려가지 못하도록 100% 순수 원가 하한선을 강제한다.
    """
    valid = [
        b
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not valid:
        return []

    adjusted: List[StratumBucket] = []
    for b in valid:
        pure_cost = round(max(0.01 * float(b.qty), b.actual_cost_usd), 4)
        pure_p = round(max(0.01, pure_cost / float(b.qty)), 4)
        adjusted.append(
            StratumBucket(
                qty=b.qty,
                book_price=pure_p,
                book_invested_usd=pure_cost,
                actual_cost_usd=pure_cost,
                lot_count=max(1, b.lot_count),
                lot_type=str(b.lot_type or "QUOTA_L1").upper().strip(),
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
    1) max_portion_budget_usd(1회분 8% 예산) 상한 주수(budget_cap_qty)와 target_qty 중 작은 값으로 캡핑한다.
    2) 왕복 수수료 버퍼(0.14%)를 감안한 가용 현금(free_cash_usd) 및 토스 실계좌 매수가능금액 한도 내로 캡핑한다.
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
# [핵심 3] 보유 회차별 4단 와이드 점진 단차 & 하이브리드 1층 재장전 판정기 (🎯 2, 🎯 4 & Case 36 ~ 72)
# ---------------------------------------------------------------------------
def get_golden8_prog_thresholds(
    held_lots: int,
    threshold_mode: str = "WIDE_RUNWAY",
) -> Tuple[float, float, float]:
    """
    🎯 2 & Case 36 ~ 45, 60, 71, 72 : 챔피언 (🔥 03_7G_PURE_10_01) 4단 와이드 점진 활주로:
    현재 보유 중인 총 투입 회차(held_lots)에 따른:
    1) 10분 시드보호 매수 게이트 요구 하락률(drop_buy_pct):
       -  1 ~  4회차 보유 중 : -1.00%
       -  5 ~  8회차 보유 중 : -2.00%
       -  9 ~ 10회차 보유 중 : -3.50%
       - 11회차 이상 보유 중 : -5.50%
    2) 60초 타임쉴드 주기 스나이퍼 매도 모니터 목표 상승률(rise_sell_l1_pct, rise_sell_upper_pct):
       - 1층(공격수) +1.50% / 2층 ~ 7층(개별 완충층·통합 7층 및 상단 통합 이동평균) +1.00%
    반환값: (drop_buy_pct, rise_sell_l1_pct, rise_sell_upper_pct)
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

    mode = str(threshold_mode or "WIDE_RUNWAY").upper().strip()
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
    🎯 4 & Case 59, Case 67, Case 71, Case 72:
    1층이 실물 익절되어 장부에 상단 그리드(2층 ~ 7층)만 남아 1층이 공석일 때,
    반등 고점에서의 이중 현금 소진을 막기 위해 회수 현금을 보유하다가 10분 매수 게이트에서:
    - 조건 A (눌림목 재장전 : chg_from_exit <= -1.00%)
    - 조건 B (상승돌파 재장전 : chg_from_exit >= +1.00%)
    충족 시에만 상단 2층 ~ 7층 비병합 독립 실물 1층(REENTRY_1FL) 재장전을 승인한다.
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
# [핵심 4 · MODIFIED Case 71 & Case 72] 1층 ~ 7층 골디락스 순수 멀티 그리드 배치 & 2~7층 통합 이동평균 · 3중 다중 익절 엔진
# ---------------------------------------------------------------------------
def vrev_add_quota_lot_and_merge_2tier(
    queue: Sequence[StratumBucket],
    buy_qty: int,
    buy_price: float,
    actual_spent_usd: Optional[float] = None,
    lot_type: str = "QUOTA_L1",
    max_tiers: int = MAX_GRID_TIERS_7,
) -> List[StratumBucket]:
    """
    🎯 3 & 🎯 4 & Case 46 ~ 72 (함수명은 타 모듈 AST 호환을 위해 유지하되 내부는 1~7층 골디락스 멀티 그리드로 작동):
    신규 정량 매수 체결 시 최대 7개 지층(1층 공격수 + 2~6층 개별 완충층[5개] + 통합 7층 본대[GRID_L7])을 관리하는 핵심 함수:
    1) 기존 보유 지층이 없을 때(len(existing) == 0):
       신규 매수분을 단일 1층([new_l1])으로 생성한다.
    2) 🚨 Case 59, Case 71 & Case 72 (상단 2층 ~ 7층 보유 중 1층 공석 상태에서 REENTRY_1FL 독립 1층 재장전):
       1층이 익절되어 장부에 상단 그리드(not has_active_l1_bucket(existing))만 남아 있을 때,
       10분 게이트 눌림(-1.0%)/돌파(+1.0%)로 재장전한 실물 1층(lot_type == "REENTRY_1FL")이 체결되면:
       기존 상단 2층 ~ 7층(GRID_L2 ~ GRID_L7)을 전혀 병합하거나 이동시키지 않고 그대로 유지한 채,
       맨 앞(index 0)에 신규 1층(new_l1)을 독립 배치하여 [new_l1(REENTRY_1FL), GRID_L2, ..., GRID_Lk] 를 반환한다!
    3) 🚨 Case 71 & Case 72 (활성 1층 보유 상태에서 4단 와이드 신저가 추매 DIP_1FL / QUOTA_L1 체결 시 2~6층 개별 승격 & 7층 통합 합병):
       - 신규 매수분은 1층(index 0, DIP_1FL)에 순수 단가로 배치한다.
       - 기존 지층들(기존 1층 + 기존 2층 ~ k층)은 2층(GRID_L2) ~ 6층(GRID_L6)까지는 병합 없이 한 층씩 위로 개별 승격시킨다!
       - 기존 지층 수가 이미 7개 이상이어서 총 층수가 7개(max_tiers = 7)를 초과할 때는
         새 2층 ~ 6층(5개 개별 완충층)은 그대로 보존하고 최상단 6층+7층을 '통합 7층 본대(GRID_L7)'로 가중평균 병합(Case 53 하드 캡핑 적용)한다!
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
    pure_p = round(max(0.0001, spent / float(q_int)), 4)
    tag = str(lot_type or "QUOTA_L1").upper().strip()
    if is_upper_grid_tier(tag):
        tag = "QUOTA_L1"

    new_l1 = StratumBucket(
        qty=q_int,
        book_price=pure_p,
        book_invested_usd=spent,
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

    eff_max_tiers = max(2, safe_int(max_tiers, MAX_GRID_TIERS_7))

    # 🚨 Case 59, Case 71 & Case 72: 상단 그리드(2층 ~ 7층)만 보유 중(1층 공석) REENTRY_1FL 유입 시 상단 비병합 독립 1층 배치!
    if not has_active_l1_bucket(existing) and tag == "REENTRY_1FL":
        preserved_uppers = relabel_upper_grid_tiers(
            existing, max_tiers=eff_max_tiers
        )
        return [new_l1] + preserved_uppers

    # 활성 1층 보유 중 신저가 추매(DIP_1FL 등) 유입 시: 기존 1층 ~ k층을 2층 ~ 6층 개별 승격 & 7층 통합 합병
    if eff_max_tiers <= 2:
        old_total_qty = sum(b.qty for b in existing)
        old_actual_cost = round(sum(b.actual_cost_usd for b in existing), 4)
        old_lot_cnt = sum(max(1, b.lot_count) for b in existing)
        old_overall_avg = (
            sum(float(b.qty) * b.book_price for b in existing)
            / float(old_total_qty)
        )
        raw_upper_p = old_actual_cost / float(old_total_qty)
        capped_upper_p = round(min(raw_upper_p, old_overall_avg), 4)
        merged_l2 = StratumBucket(
            qty=old_total_qty,
            book_price=capped_upper_p,
            book_invested_usd=old_actual_cost,
            actual_cost_usd=old_actual_cost,
            lot_count=old_lot_cnt,
            lot_type="GRID_L2",
        )
        return [new_l1, merged_l2]

    shifted_uppers = relabel_upper_grid_tiers(
        existing, max_tiers=eff_max_tiers
    )
    return [new_l1] + shifted_uppers


def compute_upper_2_to_12_moving_avg(
    buckets: Sequence[StratumBucket],
    rise_upper_pct: float = 1.00,
) -> Dict[str, Any]:
    """
    🚨 MODIFIED [Case 71 & Case 72 상단 2층 ~ 7층 통합 이동평균법(수량 가중평균) 단가 및 목표가 산출기]:
    (타 모듈 AST 임포트 호환을 위해 함수명 compute_upper_2_to_12_moving_avg 를 유지하며 compute_upper_2_to_7_moving_avg 별칭 동시 제공)
    - 활성 1층(has_active_l1_bucket == True)이 존재하면 buckets[1:] (2층 ~ 7층)을 상단 그리드로 집계하고,
    - 1층이 익절되어 공석(has_active_l1_bucket == False)이면 buckets[0:] 전체를 상단 그리드(2층 ~ 7층)로 집계한다.
    - 상단 2층 ~ 7층의 통합 이동평균 단가(upper_wma_price = sum(actual_cost_usd) / sum(qty))와
      +1.00% 일괄 익절 목표가(upper_wma_target_price), 그리고 상단 개별 층 중 가장 낮은 단가 층의
      +1.00% 개별 익절 목표가(min_indiv_upper_target_price)를 함께 산출하여 반환한다.
    """
    valid = [
        b
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    active_l1 = has_active_l1_bucket(valid)
    upper_list = valid[1:] if active_l1 else valid[:]

    if not upper_list:
        return {
            "has_active_l1": active_l1,
            "upper_tier_count": 0,
            "upper_qty": 0,
            "upper_lots": 0,
            "upper_actual_cost_usd": 0.0,
            "upper_wma_price": 0.0,
            "upper_wma_target_price": 0.0,
            "min_indiv_upper_price": 0.0,
            "min_indiv_upper_target_price": 0.0,
            "upper_buckets": [],
        }

    u_qty = sum(b.qty for b in upper_list)
    u_lots = sum(max(1, b.lot_count) for b in upper_list)
    u_cost = round(sum(b.actual_cost_usd for b in upper_list), 4)
    wma_p = round(u_cost / float(u_qty), 4) if u_qty > 0 else 0.0
    wma_target_p = (
        round(
            math.ceil(
                (wma_p * (1.0 + rise_upper_pct / 100.0) - 1e-9) * 100.0
            )
            / 100.0,
            2,
        )
        if wma_p > 0.0
        else 0.0
    )

    min_indiv_p = min(b.book_price for b in upper_list)
    min_indiv_target_p = round(
        math.ceil(
            (min_indiv_p * (1.0 + rise_upper_pct / 100.0) - 1e-9) * 100.0
        )
        / 100.0,
        2,
    )

    return {
        "has_active_l1": active_l1,
        "upper_tier_count": len(upper_list),
        "upper_qty": u_qty,
        "upper_lots": u_lots,
        "upper_actual_cost_usd": u_cost,
        "upper_wma_price": wma_p,
        "upper_wma_target_price": wma_target_p,
        "min_indiv_upper_price": round(min_indiv_p, 4),
        "min_indiv_upper_target_price": min_indiv_target_p,
        "upper_buckets": upper_list,
    }


compute_upper_2_to_7_moving_avg = compute_upper_2_to_12_moving_avg


def evaluate_multi_grid_sell_targets(
    buckets: Sequence[StratumBucket],
    live_price: float,
    rise_l1_pct: float = 1.50,
    rise_upper_pct: float = 1.00,
) -> Dict[str, Any]:
    """
    🚨 [Case 71 & Case 72 : 60초 타임쉴드 주기 3중 다중 익절 스나이퍼 매도 판정기]:
    매수 후 60.0초 및 보유 중 매 60.0초 타임쉴드가 해제된 시점에 실시간 현재가(live_price)로
    아래 3가지 매도 조건을 동시에 스캔하여 조건이 충족된 모든 지층 인덱스를 반환한다:
    1) [조건 ① : 활성 1층(buckets[0]) 개별 +1.50% 이상 도래]
       has_active_l1 == True 이고 live_price >= ceil(buckets[0].book_price * 1.015 * 100) / 100 이면
       인덱스 0 (1층)을 매도 목록에 추가!
    2) [조건 ② : 상단 2층 ~ 7층 통합 이동평균(wma_2_to_7) +1.00% 이상 도래]
       상단 그리드(2층 ~ 7층)가 존재하고 live_price >= ceil(wma_2_to_7 * 1.010 * 100) / 100 이면
       상단 2층 ~ 7층 전 지층 인덱스를 일괄 매도 목록에 추가!
       (이때 활성 1층도 본전 이상 live_price >= round(buckets[0].book_price, 2) 이면 인덱스 0 도 포함하여 전량 동시 졸업!)
    3) [조건 ③ : 상단 2층 ~ 6층 개별 완충층 및 통합 7층 본대 +1.00% 이상 도래]
       상단 2층 ~ 7층 통합 이동평균이 아직 +1.00% 에 도달하지 않았더라도,
       상단 개별 층 buckets[k] 중 live_price >= ceil(buckets[k].book_price * 1.010 * 100) / 100 에 도달한
       모든 개별 층 인덱스 k 를 즉시 매도 목록에 추가!
    """
    valid = [
        b
        for b in (buckets or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    lp = safe_float(live_price, 0.0)
    if not valid or lp <= 0.0:
        return {
            "should_sell": False,
            "sold_indices": [],
            "sell_qty": 0,
            "min_floor_price": 0.0,
            "reason_tag": "NONE",
            "hit_l1": False,
            "hit_indiv_grid": False,
            "hit_wma_grad": False,
        }

    active_l1 = has_active_l1_bucket(valid)
    sold_idx_set: Set[int] = set()
    floor_candidates: List[float] = []
    hit_l1 = False
    hit_indiv_grid = False
    hit_wma_grad = False

    # ① 활성 1층(index 0) +1.50% 개별 익절 판정
    if active_l1:
        b1 = valid[0]
        min_l1_target_p = (
            math.ceil(
                (b1.book_price * (1.0 + rise_l1_pct / 100.0) - 1e-9) * 100.0
            )
            / 100.0
        )
        if lp >= min_l1_target_p:
            sold_idx_set.add(0)
            floor_candidates.append(min_l1_target_p)
            hit_l1 = True
        upper_start_idx = 1
    else:
        upper_start_idx = 0

    # ② & ③ 상단 2층 ~ 7층 통합 이동평균(+1.00%) 및 개별 그리드(+1.00%) 판정
    if len(valid) > upper_start_idx:
        wma_info = compute_upper_2_to_12_moving_avg(
            valid, rise_upper_pct=rise_upper_pct
        )
        wma_target_p = safe_float(wma_info.get("upper_wma_target_price"), 0.0)

        if wma_target_p > 0.0 and lp >= wma_target_p:
            for k in range(upper_start_idx, len(valid)):
                sold_idx_set.add(k)
            floor_candidates.append(wma_target_p)
            hit_wma_grad = True
            if active_l1 and lp >= round(valid[0].book_price, 2):
                sold_idx_set.add(0)
                floor_candidates.append(round(valid[0].book_price, 2))
        else:
            for k in range(upper_start_idx, len(valid)):
                bk = valid[k]
                bk_target_p = (
                    math.ceil(
                        (bk.book_price * (1.0 + rise_upper_pct / 100.0) - 1e-9)
                        * 100.0
                    )
                    / 100.0
                )
                if lp >= bk_target_p:
                    sold_idx_set.add(k)
                    floor_candidates.append(bk_target_p)
                    hit_indiv_grid = True

    if not sold_idx_set:
        return {
            "should_sell": False,
            "sold_indices": [],
            "sell_qty": 0,
            "min_floor_price": 0.0,
            "reason_tag": "NONE",
            "hit_l1": False,
            "hit_indiv_grid": False,
            "hit_wma_grad": False,
        }

    sorted_indices = sorted(sold_idx_set)
    sell_qty = sum(valid[idx].qty for idx in sorted_indices)
    min_floor_p = max(floor_candidates) if floor_candidates else round(lp, 2)

    if len(sorted_indices) == len(valid):
        reason_tag = (
            "SELL_ALL_GRAD"
            if hit_wma_grad or len(valid) > 1
            else "SELL_L1_ONLY"
        )
    elif hit_wma_grad:
        reason_tag = "SELL_WMA_UPPER"
    elif hit_l1 and hit_indiv_grid:
        reason_tag = "SELL_L1_AND_GRID"
    elif hit_indiv_grid:
        reason_tag = "SELL_GRID_INDIV"
    else:
        reason_tag = "SELL_L1_ONLY"

    return {
        "should_sell": True,
        "sold_indices": sorted_indices,
        "sell_qty": sell_qty,
        "min_floor_price": round(min_floor_p, 2),
        "reason_tag": reason_tag,
        "hit_l1": hit_l1,
        "hit_indiv_grid": hit_indiv_grid,
        "hit_wma_grad": hit_wma_grad,
    }


def vrev_reanchor_single_tier_quota(
    queue: Sequence[StratumBucket],
    net_realized_cash_profit: float,
    max_allowed_total_discount_usd: Optional[float] = None,
    enable_reanchor: bool = False,
) -> Tuple[List[StratumBucket], float]:
    """
    하위 호환 심볼 유지용 함수:
    챔피언(🔥 03_7G_PURE_10_01 · Case 71, Case 72)에서는 리앵커링 원가 차감을 수행하지 않으므로
    기본값(enable_reanchor=False) 호출 시 원가를 100% 그대로 보존하고 (existing, 0.0)을 반환한다.
    """
    existing = [
        b
        for b in (queue or [])
        if isinstance(b, StratumBucket) and b.qty > 0
    ]
    if not enable_reanchor or len(existing) != 1:
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
    new_book_p = round(
        min(old_book_p, max(0.01, new_book_inv / float(rem.qty))), 4
    )
    price_drop = round(max(0.0, old_book_p - new_book_p), 4)
    updated_bucket = StratumBucket(
        qty=rem.qty,
        book_price=new_book_p,
        book_invested_usd=round(new_book_p * float(rem.qty), 4),
        actual_cost_usd=rem.actual_cost_usd,
        lot_count=rem.lot_count,
        lot_type=rem.lot_type,
    )
    return [updated_bucket], price_drop


def calc_sell_realized_pnl(
    sold_qty: int,
    sell_exec_price: float,
    sold_actual_cost_usd: float,
) -> Dict[str, float]:
    """
    매도 체결 수량(sold_qty), 실제 체결 단가(sell_exec_price), 매도 지층들의 실제 매수 원금 합계(sold_actual_cost_usd)를
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
# [핵심 5] 포트폴리오 · 1~7층 골디락스 멀티 그리드 · 2~7층 통합 이동평균 · 정적 기억저가 통합 요약기
# ---------------------------------------------------------------------------
def summarize_stratum_portfolio(
    state_dict: Dict[str, Any],
    live_price: float = 0.0,
) -> Dict[str, Any]:
    """
    🎯 1 ~ 🎯 5 & Case 59, 60, 67, 69, 71, 72:
    원장 상태(state_dict)와 현재가(live_price)를 받아:
    - 1층(활성 공격수) 및 상단 2층 ~ 6층 개별 완충층 · 통합 7층 본대(GRID_L2 ~ GRID_L7) 현황
    - 상단 2층 ~ 7층 통합 이동평균 단가(upper_wma_price) 및 +1.00% 일괄 익절 목표가(l2_target_price / upper_wma_target_price)
    - 상단 2층 ~ 7층 중 최저 단가 개별 층(min_indiv_upper_price) 및 +1.00% 개별 익절 목표가(min_indiv_upper_target_price)
    - 상단 그리드 단독 보유(1층 공석, is_l2_only_anti_bloat) 시 직전 익절가 대비 눌림(-1.0%)/돌파(+1.0%) 타점
    - 활성 1층 보유 중 정적 기억 사이클 최저 매수가(cycle_min_buy_price) 기준 4단 와이드 신저가 매수 트리거 가격
    - Case 67 min(budget_cap_qty, anchor_quota_qty) 캡핑 정량 및 회계 항등식 기반 자산 요약을 반환한다.
    """
    st = state_dict if isinstance(state_dict, dict) else {}
    raw_buckets = buckets_from_dicts(st.get("buckets", []))
    buckets = enforce_true_breakeven_floor_on_buckets(raw_buckets)

    init_seed = max(
        100.0, safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD)
    )
    cum_realized = round(
        safe_float(st.get("cumulative_realized_net_usd"), 0.0), 4
    )

    actual_invested = round(sum(b.actual_cost_usd for b in buckets), 4)
    book_invested = round(sum(b.book_invested_usd for b in buckets), 4)

    expected_free_cash = round(
        max(0.0, (init_seed + cum_realized) - actual_invested), 4
    )
    free_cash = safe_float(st.get("free_cash_usd"), expected_free_cash)
    if abs(free_cash - expected_free_cash) > 0.05:
        free_cash = expected_free_cash

    held_shares = sum(b.qty for b in buckets)
    held_lots = sum(max(1, b.lot_count) for b in buckets)
    held_tiers = len(buckets)
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

    threshold_mode = str(st.get("threshold_mode") or "WIDE_RUNWAY").upper()
    portion_mode = str(st.get("portion_mode") or "COMP_08PCT").upper()
    drop_buy_pct, rise_l1_pct, rise_l2_pct = get_golden8_prog_thresholds(
        held_lots=max(1, held_lots), threshold_mode=threshold_mode
    )

    active_l1 = has_active_l1_bucket(buckets)
    # 1층이 익절되어 공석이고 상단 2층 ~ 7층 그리드만 보유 중인 상태
    is_l2_only = bool(len(buckets) > 0 and not active_l1)

    wma_info = compute_upper_2_to_12_moving_avg(
        buckets, rise_upper_pct=rise_l2_pct
    )
    upper_tier_count = safe_int(wma_info.get("upper_tier_count"), 0)
    upper_qty = safe_int(wma_info.get("upper_qty"), 0)
    upper_lots = safe_int(wma_info.get("upper_lots"), 0)
    upper_wma_price = safe_float(wma_info.get("upper_wma_price"), 0.0)
    upper_wma_target_price = safe_float(
        wma_info.get("upper_wma_target_price"), 0.0
    )
    min_indiv_upper_price = safe_float(
        wma_info.get("min_indiv_upper_price"), 0.0
    )
    min_indiv_upper_target_price = safe_float(
        wma_info.get("min_indiv_upper_target_price"), 0.0
    )

    l1_target_price = 0.0
    l2_target_price = upper_wma_target_price
    l1_gain_pct = 0.0
    l2_gain_pct = 0.0
    min_indiv_upper_gain_pct = 0.0

    if active_l1 and buckets:
        b1 = buckets[0]
        l1_target_price = round(
            math.ceil(
                (b1.book_price * (1.0 + rise_l1_pct / 100.0) - 1e-9) * 100.0
            )
            / 100.0,
            2,
        )
        if lp > 0.0 and b1.book_price > 0.0:
            l1_gain_pct = round(
                ((lp - b1.book_price) / b1.book_price) * 100.0, 2
            )

    if upper_wma_price > 0.0 and lp > 0.0:
        l2_gain_pct = round(
            ((lp - upper_wma_price) / upper_wma_price) * 100.0, 2
        )
    if min_indiv_upper_price > 0.0 and lp > 0.0:
        min_indiv_upper_gain_pct = round(
            ((lp - min_indiv_upper_price) / min_indiv_upper_price) * 100.0, 2
        )

    anchor_p = safe_float(st.get("anchor_price"), 0.0)
    cycle_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
    ratchet_p = safe_float(st.get("ratchet_anchor_price"), anchor_p)

    reentry_sig = evaluate_l2_only_hybrid_reentry_signal(
        live_price=lp,
        anchor_price=anchor_p,
        l2_book_price=upper_wma_price,
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

    # 🚨 Case 60 & Case 72: 활성 1층 보유 중 추가 매수(DIP_1FL) 기준가는 반드시 정적 기억 최저 매수가(cycle_min_buy_price)!
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
    indiv_grid_sell_cnt = safe_int(st.get("indiv_grid_sell_cnt"), 0)
    wma_grad_sell_cnt = safe_int(st.get("wma_grad_sell_cnt"), 0)
    seed_protect_pass_cnt = safe_int(st.get("seed_protect_pass_cnt"), 0)
    extra_13plus_buy_events = safe_int(st.get("extra_13plus_buy_events"), 0)
    cum_leftover_saved = round(
        safe_float(st.get("cumulative_leftover_saved_usd"), 0.0), 4
    )

    return {
        "buckets": buckets,
        "has_active_l1": active_l1,
        "held_shares": held_shares,
        "held_lots": held_lots,
        "held_tiers": held_tiers,
        "upper_tier_count": upper_tier_count,
        "upper_qty": upper_qty,
        "upper_lots": upper_lots,
        "upper_wma_price": upper_wma_price,
        "upper_wma_target_price": upper_wma_target_price,
        "min_indiv_upper_price": min_indiv_upper_price,
        "min_indiv_upper_target_price": min_indiv_upper_target_price,
        "min_indiv_upper_gain_pct": min_indiv_upper_gain_pct,
        "is_l2_only_anti_bloat": is_l2_only,
        "initial_seed_usd": init_seed,
        "free_cash_usd": free_cash,
        "actual_invested_usd": actual_invested,
        "book_invested_usd": book_invested,
        "actual_avg_price": actual_avg_p,
        "book_avg_price": book_avg_p,
        "cycle_reanchor_credit_usd": 0.0,
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
        "indiv_grid_sell_cnt": indiv_grid_sell_cnt,
        "wma_grad_sell_cnt": wma_grad_sell_cnt,
        "reanchor_cnt": reanchor_cnt,
        "cumulative_reanchor_drop_usd": cum_reanchor_drop,
        "seed_protect_pass_cnt": seed_protect_pass_cnt,
        "extra_13plus_buy_events": extra_13plus_buy_events,
        "cumulative_leftover_saved_usd": cum_leftover_saved,
    }


# ---------------------------------------------------------------------------
# [핵심 6] TDD 사전 무결성 자동 검증 슈트 (Case 25 ~ 72 & 1~7층 순수 멀티 그리드 전수 검증)
# ---------------------------------------------------------------------------
def run_quant_engine_tdd() -> bool:
    """
    기동 직전 8% 황금정량 산출, Case 67 min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑,
    4단 와이드 활주로(-1.0% / -2.0% / -3.5% / -5.5%),
    1층 ~ 6층 개별 완충층 보존 및 8회차 최상단(6층+7층 ➔ 통합 7층 GRID_L7) 병합(Case 72),
    1층(+1.50%) / 2~7층 개별(+1.00%) / 2~7층 통합 이동평균(+1.00%) 3중 다중 익절 판정(Case 71, 72),
    상단 그리드 단독 보유(1층 공석) 시 눌림/돌파 독립 1층 재장전(Case 59) 및 정적 기억저가 잠금(Case 60, 72)을 전수 검증한다.
    """
    # 1) 8% 황금정량 및 Case 67 min(budget_cap_qty, anchor_quota_qty) 캡핑 검증
    q_dip, bud_c67 = compute_golden8_quota_qty(
        82.39, 19155.74, 19000.0, "COMP_08PCT", exec_price=79.52
    )
    q_brk, _ = compute_golden8_quota_qty(
        82.39, 19155.74, 19000.0, "COMP_08PCT", exec_price=86.00
    )
    assert q_dip == 18 and q_brk == 17 and (q_brk * 86.00) <= bud_c67, (
        f"TDD 실패(Case 67): q_dip={q_dip}, q_brk={q_brk}"
    )

    # 2) 4단 와이드 활주로 단차 검증
    d1, s1, s2 = get_golden8_prog_thresholds(1, "WIDE_RUNWAY")
    d5, _, _ = get_golden8_prog_thresholds(5, "WIDE_RUNWAY")
    d9, _, _ = get_golden8_prog_thresholds(9, "WIDE_RUNWAY")
    d11, _, _ = get_golden8_prog_thresholds(11, "WIDE_RUNWAY")
    assert (
        d1 == -1.00
        and d5 == -2.00
        and d9 == -3.50
        and d11 == -5.50
        and s1 == 1.50
        and s2 == 1.00
    ), "TDD 실패: 4단 와이드 단차 불일치"

    # 3) 🚨 Case 72: 1층 ~ 6층 개별 완충층 적재 및 8회차 유입 시 최상단(6층+7층 ➔ 통합 7층 GRID_L7) 병합 검증
    grid_q: List[StratumBucket] = []
    for step in range(8):
        p_step = round(100.0 - float(step), 2)
        tag_step = "REENTRY_1FL" if step == 0 else "DIP_1FL"
        grid_q = vrev_add_quota_lot_and_merge_2tier(
            grid_q, 10, p_step, round(10.0 * p_step, 4), tag_step
        )
        assert len(grid_q) == min(MAX_GRID_TIERS_7, step + 1), (
            f"TDD 실패(Case 72 그리드 층수): step={step}, len={len(grid_q)}"
        )

    assert (
        len(grid_q) == 7
        and has_active_l1_bucket(grid_q)
        and grid_q[0].book_price == 93.0
        and grid_q[-1].lot_type == "GRID_L7"
        and grid_q[-1].lot_count == 2
        and abs(grid_q[-1].book_price - 99.50) < 1e-4
    ), f"TDD 실패(Case 72): 8회차 시 최상단 통합 7층 병합 불일치 {grid_q[-1]}"

    # 4) 🚨 Case 71 & Case 72: 3중 다중 익절 판정(① 1층 +1.5% / ② 2~7층 개별 +1.0% / ③ 2~7층 이동평균 +1.0%) 검증
    test_grid = [
        StratumBucket(10, 80.0, 800.0, 800.0, 1, "DIP_1FL"),
        StratumBucket(10, 81.0, 810.0, 810.0, 1, "GRID_L2"),
        StratumBucket(10, 85.0, 850.0, 850.0, 1, "GRID_L3"),
        StratumBucket(10, 90.0, 900.0, 900.0, 1, "GRID_L4"),
    ]
    res_part = evaluate_multi_grid_sell_targets(test_grid, live_price=81.90)
    assert (
        res_part["should_sell"]
        and res_part["sold_indices"] == [0, 1]
        and res_part["sell_qty"] == 20
        and res_part["hit_l1"]
        and res_part["hit_indiv_grid"]
    ), f"TDD 실패(Case 71/72 부분 다중익절): {res_part}"

    # 1층과 2층이 팔린 후 남은 [GRID_L3($85), GRID_L4($90)] -> relabel_upper_grid_tiers 호출 시 [GRID_L2($85), GRID_L3($90)] (1층 공석!)
    rem_uppers = relabel_upper_grid_tiers(test_grid[2:])
    assert len(rem_uppers) == 2 and not has_active_l1_bucket(rem_uppers), (
        "TDD 실패(Case 71/72): 1층 익절 후 1층 공석(상단 그리드만 보유) 상태 판별 오류"
    )

    # 1층 공석 상태에서 눌림목($80.50)으로 REENTRY_1FL 재장전 시 상단 [GRID_L2, GRID_L3] 비병합 독립 1층 배치!
    reloaded_grid = vrev_add_quota_lot_and_merge_2tier(
        rem_uppers, 10, 80.50, 805.0, "REENTRY_1FL"
    )
    assert (
        len(reloaded_grid) == 3
        and has_active_l1_bucket(reloaded_grid)
        and reloaded_grid[0].book_price == 80.50
        and reloaded_grid[1].book_price == 85.00
        and reloaded_grid[2].book_price == 90.00
    ), "TDD 실패(Case 59/72): 1층 공석 재장전 시 상단 그리드 비병합 배치 오류"

    # 상단 2~3층 이동평균 = ($850 + $900) / 20주 = $87.50 -> +1.0% 목표가 = $88.38
    # 현재가 $88.50 도달 시 1층($80.50) + 2~3층($87.50) 전량 30주 동시 졸업!
    res_grad = evaluate_multi_grid_sell_targets(reloaded_grid, live_price=88.50)
    assert (
        res_grad["should_sell"]
        and res_grad["sold_indices"] == [0, 1, 2]
        and res_grad["sell_qty"] == 30
        and res_grad["hit_wma_grad"]
    ), f"TDD 실패(Case 71/72 2~7층 이동평균 일괄졸업): {res_grad}"

    # 5) Case 60 & Case 72 정적 기억 최저가($80.00) 잠금 및 포트폴리오 요약 검증
    mock_st = {
        "buckets": buckets_to_dicts(reloaded_grid),
        "initial_seed_usd": 10000.0,
        "anchor_price": 80.50,
        "cycle_min_buy_price": 80.00,
        "cumulative_realized_net_usd": 25.0,
    }
    sum_info = summarize_stratum_portfolio(mock_st, live_price=81.00)
    assert abs(sum_info["active_buy_anchor"] - 80.00) < 1e-4, (
        "TDD 실패(Case 60/72): 정적 기억 최저가 잠금 풀림"
    )
    assert (
        sum_info["held_tiers"] == 3
        and sum_info["upper_tier_count"] == 2
        and abs(sum_info["upper_wma_price"] - 87.50) < 1e-4
    ), "TDD 실패(Case 72): 상단 2~7층 이동평균 요약 오류"

    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 챔피언 🔥 03_7G_PURE_10_01 (1~6층 개별 완충층 + 통합 7층 본대 · 2~7층 통합이평 3중 익절 · 정적 저가 잠금 Case 59~72) 검증 완료"
    )
    return True
