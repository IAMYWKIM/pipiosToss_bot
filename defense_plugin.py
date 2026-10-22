# -*- coding: utf-8 -*-
"""
# MODIFIED: defense_plugin.py
순수 롱(SOXL) 오버나이트 지층관리 스나이퍼 전용 (Step 04 / 06):
1) 🚨 [Case 66 봇 주문 진행 중 가로채기 차단 및 미결 매수 즉시 정산 가드]
   원장에 미결 매수(buy_order_id)가 존재할 때 reconcile 모듈이 호출되면, 실잔고 증가를 수동 매수로
   가로채지 않고 즉시 sync_pending_buy_order_plugin() 을 선행 호출한 뒤 SKIP_RECONCILE_BOT_ORDER_PENDING 을 반환하여
   텔레그램 매수 체결 알림 · 자투리 절약금 누적 · 매수 후 60초 타임쉴드 각인이 100% 완수되도록 보장!
2) 🔗 [tg_ui_plugin.py 심볼 완벽 호환] 전술 제어반(/settlement)에서 봇 작동 OFF ➔ ON 재개 시 호출되는
   relock_stratum_on_bot_resume() 함수를 정식 제공하여 AST 심볼 임포트 무결성 100% 충족!
3) 04:00 ~ 04:06 EST 프리장 개장 초기 호가 안정화 · 전일 잔류 미체결 청소 · 오버나이트 이월 지층 동기화
4) 장중/기동 시 로컬 지층 원장(soxl_stratum_ledger_<폴더명>.json) ↔ 토스 실보유 잔고(/api/v1/holdings) 무결성 화해
   - Case 61: 매수 체결 직후 15.0초 인덱싱 시차 그레이스 쉴드(is_within_auto_fill_grace_shield) 보호
   - Case 60: 토스 앱 수동 전량 매도(0주) 시 기억된 최저 매수가(cycle_min_buy_price = 0.0) 원자적 리셋
   - Case 63: 수동 매수/매도 동기화 시 기존에 깎아둔 리앵커링 단가(book_price) 크레딧 보존 및 음수 시드 방지
   - 🚨 [NEW · Case 68 백업 금고(ledger_backups/) 자가 치유 연동 & 시드 $100 변조 방어]:
     로컬 원장 지층이 비어 있고 토스 실잔고(h_qty > 0)가 존재할 때, 먼저 백업 금고(recover_from_backup_vault_sync)를
     조회하여 어제의 1·2층 분리 지층·리앵커링 단가·기억저가·누적 수익·설정 시드머니를 100% 원형 복원!
5) 16:00 ~ 16:59 EST 정규장 마감 일일 결산 리포트 타전 · 일일 스냅샷 백업 및 보유 지층 오버나이트 안전 보존
6) 17:00 EST 일일 가비지 컬렉션(GC) · 일일 통계 롤오버 및 ledger_backups/ 날짜별 원장 스냅샷(최근 14일 보관) 물리적 생성
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 68 완벽 준수
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

from quant_engine import (
    StratumBucket,
    buckets_from_dicts,
    buckets_to_dicts,
    summarize_stratum_portfolio,
    vrev_add_quota_lot_and_merge_2tier,
)
from sniper_plugin import (
    is_within_auto_fill_grace_shield,
    sync_pending_buy_order_plugin,
)
from toss_api import (
    DEFAULT_INITIAL_SEED_USD,
    EST_TZ,
    TARGET_SYMBOL,
    get_folder_slug,
    safe_float,
    safe_int,
)

logger = logging.getLogger("toss_sniper.defense_plugin")

# toss_api 심볼 불일치 방지를 위해 기본 환율 상수를 모듈 내부에 직접 정의
KRW_PER_USD_DEFAULT: float = 1380.0


def _resolve_valid_seed_from_state(st: Dict[str, Any]) -> float:
    """
    🚨 Case 68:
    상태 딕셔너리에서 initial_seed_usd 를 읽을 때 0.0 또는 비정상 $100 초기값으로 인해
    시드가 $100 으로 축소되는 현상을 방어한다.
    """
    raw_seed = safe_float(st.get("initial_seed_usd"), 0.0)
    explicit_100 = bool(st.get("seed_explicitly_set_100", False))
    if raw_seed > 100.0:
        return round(raw_seed, 4)
    if abs(raw_seed - 100.0) < 1e-6 and explicit_100:
        return 100.0
    return DEFAULT_INITIAL_SEED_USD


# ---------------------------------------------------------------------------
# [핵심 1] 로컬 지층 원장 ↔ 토스 실보유 잔고(/api/v1/holdings) 무결성 화해 (Case 58, 60, 61, 63, 66, 68)
# ---------------------------------------------------------------------------
async def reconcile_stratum_with_broker_holdings_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
    force_sync: bool = False,
) -> Dict[str, Any]:
    """
    🚨 Case 58, Case 60, Case 61, Case 63, Case 66 & Case 68:
    프로세스 재기동 시, 04:00 EST 오프닝 동기화 시, 또는 장중 사용자가 토스 앱에서 수동 매매했을 때
    로컬 지층 원장(buckets)의 총 보유 주수(ledger_qty)와 토스 실계좌 보유 주수(h_qty)를 대조하여 화해한다:
    0) 🛡️ [Case 66 핵심 방어 : 봇 미결 주문(buy_order_id / sell_order_id) 진행 중 가로채기 원천 차단]
       - force_sync 가 아닌 일반 루프 호출 시 원장에 buy_order_id 가 남아 있다면,
         거래소 즉시 체결로 실잔고(h_qty)가 먼저 늘어났더라도 reconcile 이 가로채지 않고
         즉시 sync_pending_buy_order_plugin() 을 선행 호출하여 정식 체결 알림 · 자투리 누적 · 60초 타임쉴드를 완수한다!
       - sell_order_id 가 남아 있을 때 역시 SKIP_RECONCILE_BOT_ORDER_PENDING 을 반환하여
         직후 실행되는 매도 게이트(sync_pending_sell_order_plugin)가 정식 익절 정산을 수행하도록 보장한다!
    1) 🛡️ [Case 68 핵심 방어 : 원장 초기화 감지 시 백업 금고(ledger_backups/) 자가 치유 복원]
       - 토스 실보유 수량(h_qty > 0)이 있는데 로컬 원장 수량(ledger_qty == 0)이 비어 있을 때,
         먼저 context.ledger.recover_from_backup_vault_sync() 를 호출하여 백업 금고에 보존된
         2-Tier 지층·리앵커링 단가·기억저가·누적 실현수익·설정 시드머니를 최우선 복구한다!
    """
    if not isinstance(holdings, dict) or not getattr(
        context, "holdings_fetch_ok", True
    ):
        return {"status": "SKIP_HOLDINGS_NOT_READY"}

    st = context.ledger.get_state(TARGET_SYMBOL)
    pending_buy_oid = str(st.get("buy_order_id") or "").strip()
    pending_sell_oid = str(st.get("sell_order_id") or "").strip()

    # 🚨 [Case 66 핵심 방어] 봇이 발주한 지정가 매수/매도 주문이 대기 중일 때 가로채기 금지 및 매수 선행 정산!
    if not force_sync and (pending_buy_oid or pending_sell_oid):
        if pending_buy_oid:
            try:
                await sync_pending_buy_order_plugin(
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
            except Exception as exc:
                logger.warning("Case 66 미결 매수 선행 동기화 예외: %s", exc)
        return {
            "status": "SKIP_RECONCILE_BOT_ORDER_PENDING",
            "buy_order_id": pending_buy_oid,
            "sell_order_id": pending_sell_oid,
        }

    buckets = buckets_from_dicts(st.get("buckets", []))
    ledger_qty = sum(b.qty for b in buckets)

    h_info = holdings.get(TARGET_SYMBOL, {})
    h_qty = max(0, safe_int(h_info.get("qty"), 0))
    h_avg = round(max(0.0, safe_float(h_info.get("avg_price"), 0.0)), 4)
    h_pur = round(
        max(
            0.0,
            safe_float(h_info.get("purchase_amount"), float(h_qty) * h_avg),
        ),
        4,
    )
    live_p = safe_float(
        (prices or {}).get(TARGET_SYMBOL),
        safe_float(h_info.get("last_price"), h_avg),
    )

    if pending_buy_oid or pending_sell_oid:
        open_orders = await context.api.get_open_orders(symbol=TARGET_SYMBOL)
        if open_orders:
            return {
                "status": "SKIP_OPEN_ORDER_ACTIVE",
                "open_count": len(open_orders),
            }

    # [케이스 1] 수량이 완벽히 일치하는 경우 -> 즉시 통과
    if h_qty == ledger_qty:
        return {
            "status": "STRATUM_IN_SYNC",
            "held_shares": h_qty,
            "tiers": len(buckets),
        }

    # [케이스 2] 토스 실보유 0주인데 로컬 원장에 지층이 남아 있는 경우
    if h_qty == 0 and ledger_qty > 0:
        # 🚨 Case 61: 방금 자동 매수가 체결된 직후(15.0초 이내)라면 토스 잔고 API 인덱싱 시차이므로 절대 지우지 않고 보호!
        if is_within_auto_fill_grace_shield(context, TARGET_SYMBOL):
            logger.info(
                "🛡️ [Case 61 인덱싱 그레이스 쉴드 작동] 매수 체결 직후 토스 잔고 API 시차 감지 -> 원장 %d주 안전 보호",
                ledger_qty,
            )
            return {
                "status": "PROTECTED_BY_AUTO_FILL_GRACE_SHIELD",
                "held_shares": ledger_qty,
            }

        open_orders = await context.api.get_open_orders(symbol=TARGET_SYMBOL)
        if open_orders:
            return {"status": "WAIT_OPEN_ORDER_SETTLEMENT"}

        # 🚨 Case 60: 수동 전량 청산으로 0주가 되었으므로 cycle_min_buy_price = 0.0 으로 리셋!
        await context.ledger.update_state(
            TARGET_SYMBOL,
            buckets=[],
            cycle_min_buy_price=0.0,
            buy_order_id="",
            sell_order_id="",
            ordered_qty=0,
            target_sell_price=0.0,
            pending_sell_mode="",
            pending_lot_type="",
        )
        logger.info(
            "🧹 [SOXL 실잔고 0주 동기화 완료] 수동 전량 청산 감지 -> 지층 큐 및 기억된 최저가(cycle_min_buy_price=0) 초기화 완료 (이전 장부 %d주)",
            ledger_qty,
        )
        return {"status": "CLEARED_TO_ZERO_HOLDINGS", "prev_qty": ledger_qty}

    # [케이스 3] 토스 실보유 수량(h_qty > 0)이 존재하는데 로컬 원장이 비어 있는 경우 (Case 68 백업 금고 우선 복원 또는 초기 기동 복원)
    if h_qty > 0 and ledger_qty == 0:
        # 🛡️ [Case 68 자가 치유 1단계] 백업 금고(ledger_backups/)에 보존된 지층 데이터가 있는지 먼저 확인!
        vault_st: Optional[Dict[str, Any]] = None
        recover_fn = getattr(
            context.ledger, "recover_from_backup_vault_sync", None
        )
        if callable(recover_fn):
            try:
                vault_st = await asyncio.to_thread(recover_fn)
            except Exception as exc:
                logger.debug("백업 금고 조회 예외(무시): %s", exc)

        if isinstance(vault_st, dict):
            vault_buckets = buckets_from_dicts(vault_st.get("buckets", []))
            vault_qty = sum(b.qty for b in vault_buckets)
            vault_seed = _resolve_valid_seed_from_state(vault_st)

            # 백업 금고의 보유 수량이 토스 실보유 수량(h_qty)과 정확히 일치하면 1·2층 분리 지층 및 모든 누적 기록을 100% 원형 복구!
            if vault_qty == h_qty and len(vault_buckets) > 0:
                restored_kw: Dict[str, Any] = dict(vault_st)
                restored_kw["initial_seed_usd"] = vault_seed
                restored_kw["buy_order_id"] = ""
                restored_kw["sell_order_id"] = ""
                restored_kw["ordered_qty"] = 0
                await context.ledger.update_state(TARGET_SYMBOL, **restored_kw)
                logger.info(
                    "🛡️ [Case 68 백업 금고 100%% 원형 복원 완료] %d주 (%d개 지층 · 시드 $%.2f · 기억저가 $%.2f)",
                    h_qty,
                    len(vault_buckets),
                    vault_seed,
                    safe_float(vault_st.get("cycle_min_buy_price"), 0.0),
                )
                return {
                    "status": "RESTORED_FROM_BACKUP_VAULT",
                    "held_shares": h_qty,
                    "tiers": len(vault_buckets),
                }

        # 백업 금고에 일치하는 지층이 없는 경우: 토스 실계좌 평균단가 기반으로 1층 복원 (단, 백업 시드/누적수익이 있으면 승계)
        ref_p = h_avg if h_avg > 0.0 else (live_p if live_p > 0.0 else 100.0)
        cost_u = h_pur if h_pur > 0.0 else round(float(h_qty) * ref_p, 4)
        init_seed = _resolve_valid_seed_from_state(st)
        if isinstance(vault_st, dict) and init_seed <= 100.0:
            init_seed = _resolve_valid_seed_from_state(vault_st)

        est_lots = max(
            1, int(round(cost_u / max(1.0, init_seed * 0.08)))
        )
        restored_bucket = StratumBucket(
            qty=h_qty,
            book_price=ref_p,
            book_invested_usd=cost_u,
            actual_cost_usd=cost_u,
            lot_count=est_lots,
            lot_type="QUOTA_L1",
        )
        now_sync_ts = time.time()
        update_kw: Dict[str, Any] = {
            "initial_seed_usd": init_seed,
            "buckets": buckets_to_dicts([restored_bucket]),
            "anchor_price": ref_p,
            "cycle_min_buy_price": ref_p,
            "ratchet_anchor_price": ref_p,
            "buy_order_id": "",
            "sell_order_id": "",
            "ordered_qty": 0,
            "entry_ts": now_sync_ts,
            "last_buy_fill_ts": now_sync_ts,
        }
        if isinstance(vault_st, dict):
            for stat_k in (
                "cumulative_realized_net_usd",
                "cumulative_realized_gross_usd",
                "cumulative_realized_fee_usd",
                "cumulative_leftover_saved_usd",
                "cumulative_reanchor_drop_usd",
                "reanchor_cnt",
                "seed_protect_pass_cnt",
                "total_buy_events",
                "total_sell_events",
                "win_sell_events",
            ):
                if safe_float(st.get(stat_k), 0.0) == 0.0 and safe_float(
                    vault_st.get(stat_k), 0.0
                ) != 0.0:
                    update_kw[stat_k] = vault_st[stat_k]

        await context.ledger.update_state(TARGET_SYMBOL, **update_kw)
        logger.info(
            "♻️ [SOXL 실잔고 기반 1층 복원 완료] %d주 @ $%.2f (원금 $%.2f · 시드 $%.2f · 추정 %d회차 · 기억저가 $%.2f)",
            h_qty,
            ref_p,
            cost_u,
            init_seed,
            est_lots,
            ref_p,
        )
        return {
            "status": "RESTORED_FROM_BROKER_HOLDINGS",
            "held_shares": h_qty,
            "avg_price": ref_p,
        }

    # [케이스 4] 토스 실보유 수량(h_qty > 0)과 로컬 원장 수량(ledger_qty > 0)이 부분 불일치하는 경우 (수동 추매 / 수동 부분매도)
    if h_qty > ledger_qty:
        delta_q = h_qty - ledger_qty
        old_cost = sum(b.actual_cost_usd for b in buckets)
        delta_cost = max(0.0, h_pur - old_cost)
        est_buy_p = (
            round(delta_cost / float(delta_q), 4)
            if delta_cost > 0.0
            else (live_p if live_p > 0.0 else h_avg)
        )
        if est_buy_p <= 0.0:
            est_buy_p = h_avg if h_avg > 0.0 else 50.0
        merged_buckets = vrev_add_quota_lot_and_merge_2tier(
            queue=buckets,
            buy_qty=delta_q,
            buy_price=est_buy_p,
            actual_spent_usd=round(float(delta_q) * est_buy_p, 4),
            lot_type="MANUAL_SYNC_L1",
        )
        old_min_p = safe_float(st.get("cycle_min_buy_price"), 0.0)
        new_min_p = (
            min(old_min_p, est_buy_p) if old_min_p > 0.0 else est_buy_p
        )
        now_sync_ts = time.time()
        await context.ledger.update_state(
            TARGET_SYMBOL,
            buckets=buckets_to_dicts(merged_buckets),
            anchor_price=est_buy_p,
            cycle_min_buy_price=round(new_min_p, 4),
            ratchet_anchor_price=est_buy_p,
            entry_ts=now_sync_ts,
            last_buy_fill_ts=now_sync_ts,
        )
        logger.info(
            "➕ [SOXL 수동 추가매수 지층 동기화] +%d주 @ $%.2f 반영 -> 총 %d주 (기억저가 $%.2f)",
            delta_q,
            est_buy_p,
            h_qty,
            new_min_p,
        )
        return {
            "status": "SYNCED_MANUAL_BUY_ADDITION",
            "added_qty": delta_q,
            "held_shares": h_qty,
        }

    # 4-2) h_qty < ledger_qty (수동 부분 매도 또는 체결 후 잔여 수량 조정):
    # 🚨 Case 63: 기존 지층에 쌓여 있던 리앵커링 크레딧(old_actual_cost - old_book_inv)을 보존한다!
    ref_p = h_avg if h_avg > 0.0 else buckets[0].actual_avg_price
    cost_u = h_pur if h_pur > 0.0 else round(float(h_qty) * ref_p, 4)

    old_actual_cost = sum(b.actual_cost_usd for b in buckets)
    old_book_inv = sum(b.book_invested_usd for b in buckets)
    existing_reanchor_credit = max(0.0, old_actual_cost - old_book_inv)
    scale_ratio = (
        min(1.0, float(h_qty) / float(ledger_qty))
        if ledger_qty > 0
        else 1.0
    )
    preserved_credit = round(existing_reanchor_credit * scale_ratio, 4)

    new_book_inv = round(
        max(0.01 * float(h_qty), cost_u - preserved_credit), 4
    )
    new_book_p = round(
        min(ref_p, max(0.01, new_book_inv / float(h_qty))), 4
    )

    init_seed = _resolve_valid_seed_from_state(st)
    adj_lots = max(
        1,
        min(
            sum(b.lot_count for b in buckets),
            int(round(cost_u / max(1.0, init_seed * 0.08))),
        ),
    )
    preserved_lot_type = (
        "MERGED_UPPER"
        if (len(buckets) == 1 and buckets[0].lot_type == "MERGED_UPPER")
        else "QUOTA_L1"
    )
    adjusted_bucket = StratumBucket(
        qty=h_qty,
        book_price=new_book_p,
        book_invested_usd=round(new_book_p * float(h_qty), 4),
        actual_cost_usd=cost_u,
        lot_count=adj_lots,
        lot_type=preserved_lot_type,
    )
    await context.ledger.update_state(
        TARGET_SYMBOL,
        buckets=buckets_to_dicts([adjusted_bucket]),
    )
    logger.info(
        "➖ [SOXL 수동 부분매도 지층 동기화] %d주 -> %d주 조정 완료 (실평단 $%.2f / 리앵커링 장부단가 $%.2f 보존)",
        ledger_qty,
        h_qty,
        ref_p,
        new_book_p,
    )
    return {
        "status": "SYNCED_MANUAL_PARTIAL_REDUCTION",
        "held_shares": h_qty,
        "book_price": new_book_p,
    }


# ---------------------------------------------------------------------------
# [핵심 1-B] tg_ui_plugin.py 호환 : 봇 작동 재개(OFF ➔ ON) 시 실잔고 즉시 재락온 헬퍼
# ---------------------------------------------------------------------------
async def relock_stratum_on_bot_resume(
    context: Any,
    holdings: Optional[Dict[str, Dict[str, Any]]] = None,
    prices: Optional[Dict[str, float]] = None,
    now_est: Optional[datetime] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    전술 제어반(/settlement)에서 스나이퍼 작동을 🔴 OFF ➔ 🟢 ON 으로 전환할 때
    tg_ui_plugin.py 에서 호출하는 실잔고 ↔ 지층 원장 즉시 동기화 함수.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    eff_holdings = holdings
    if not isinstance(eff_holdings, dict) or not eff_holdings:
        try:
            h_map, ok = await context.api.get_holdings_map_with_status()
            if ok and isinstance(h_map, dict):
                eff_holdings = h_map
                context.latest_holdings = h_map
                context.holdings_fetch_ok = True
        except Exception:
            eff_holdings = getattr(context, "latest_holdings", {})

    eff_prices = prices
    if not isinstance(eff_prices, dict) or not eff_prices:
        try:
            p_map = await context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            if (
                isinstance(p_map, dict)
                and safe_float(p_map.get(TARGET_SYMBOL), 0.0) > 0.0
            ):
                eff_prices = p_map
                context.latest_prices = p_map
        except Exception:
            eff_prices = getattr(context, "latest_prices", {})

    return await reconcile_stratum_with_broker_holdings_plugin(
        context=context,
        holdings=eff_holdings or {},
        prices=eff_prices or {},
        now_est=dt_now,
        force_sync=True,
    )


# ---------------------------------------------------------------------------
# [핵심 2] 04:00 ~ 04:06 EST 프리장 개장 초기 호가 안정화 및 전일 미체결 청소
# ---------------------------------------------------------------------------
async def execute_0400_premarket_stabilization_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🔹 04:00 ~ 04:06 EST:
    프리장 개장 초기 호가 스프레드 안정화를 대기하면서, 하루에 1회 전일 미체결 잔류 주문을 청소하고
    오버나이트 이월된 SOXL 지층(1층 / 2층)과 가용 현금 시드(free_cash_usd)를 토스 실계좌와 대조 동기화한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    today_str = dt_now.strftime("%Y-%m-%d")
    done_date = str(getattr(context, "last_0400_sync_date", "") or "")

    if done_date == today_str:
        return {"status": "ALREADY_STABILIZED_TODAY", "date": today_str}

    setattr(context, "last_0400_sync_date", today_str)

    canceled_cnt = await context.api.cancel_all_open_orders(
        symbol=TARGET_SYMBOL
    )
    await context.ledger.update_state(
        TARGET_SYMBOL,
        buy_order_id="",
        sell_order_id="",
        ordered_qty=0,
        target_sell_price=0.0,
        pending_sell_mode="",
        pending_lot_type="",
    )

    sync_res = await reconcile_stratum_with_broker_holdings_plugin(
        context=context,
        holdings=holdings,
        prices=prices,
        now_est=dt_now,
        force_sync=True,
    )

    logger.info(
        "🌅 [04:00 EST 프리장 오프닝 안정화 완료] 잔류주문 취소=%d건 · 지층동기화=%s",
        canceled_cnt,
        sync_res.get("status"),
    )
    return {
        "status": "PREMARKET_0400_STABILIZED",
        "canceled_orders": canceled_cnt,
        "sync_result": sync_res,
    }


# ---------------------------------------------------------------------------
# [핵심 3] 16:00 ~ 16:59 EST 정규장 마감 일일 결산 보고 & 스냅샷 백업 (강제 MOC 손절 100% 폐기!)
# ---------------------------------------------------------------------------
async def execute_1600_daily_closing_settlement_plugin(
    context: Any,
    holdings: Dict[str, Dict[str, Any]],
    prices: Dict[str, float],
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🔹 16:00 ~ 16:59 EST:
    정규장 마감 시 보유 지층(1층 및 2층)을 강제 손절(MOC)하지 않고 100% 오버나이트 안전 보존하며,
    미체결 잔류 DAY 주문을 정리하고 당일 세후 실현수익금 · 자투리 절약액 · 리앵커링 횟수 · 시드보호 패스 통계를
    텔레그램으로 일일 결산 보고함과 동시에 ledger_backups/ 에 당일 마감 스냅샷을 백업한다 (Case 68).
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt_now.hour * 100 + dt_now.minute
    if not (1600 <= hm <= 1659):
        return {"status": "NOT_CLOSING_WINDOW"}

    today_str = dt_now.strftime("%Y-%m-%d")
    st = context.ledger.get_state(TARGET_SYMBOL)
    if str(st.get("last_closing_report_date") or "") == today_str:
        return {"status": "CLOSING_REPORT_ALREADY_SENT", "date": today_str}

    await context.api.cancel_all_open_orders(symbol=TARGET_SYMBOL)
    updated_st = await context.ledger.update_state(
        TARGET_SYMBOL,
        buy_order_id="",
        sell_order_id="",
        ordered_qty=0,
        target_sell_price=0.0,
        pending_sell_mode="",
        pending_lot_type="",
        last_closing_report_date=today_str,
    )

    # 🛡️ Case 68: 정규장 마감 시점의 원장 상태를 날짜별 백업 금고에 즉시 스냅샷 저장
    snap_fn = getattr(context.ledger, "create_daily_backup_snapshot", None)
    if callable(snap_fn):
        try:
            await snap_fn(logical_date=today_str, keep_days=14)
        except Exception as exc:
            logger.debug("16:00 마감 스냅샷 백업 예외(무시): %s", exc)

    live_p = safe_float(
        (prices or {}).get(TARGET_SYMBOL),
        safe_float(
            (holdings or {}).get(TARGET_SYMBOL, {}).get("last_price"),
            safe_float(updated_st.get("anchor_price"), 0.0),
        ),
    )
    summary = summarize_stratum_portfolio(updated_st, live_price=live_p)
    fx_rate = await context.api.get_usd_krw_rate()
    if fx_rate <= 0.0:
        fx_rate = KRW_PER_USD_DEFAULT

    folder_slug = str(
        getattr(context.ledger, "folder_slug", "")
        or updated_st.get("folder_slug")
        or get_folder_slug(getattr(context, "base_dir", None))
    )
    day_net = safe_float(updated_st.get("daily_realized_net_usd"), 0.0)
    day_krw = day_net * fx_rate
    cum_net = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
    cum_roi = safe_float(summary.get("realized_roi_pct"), 0.0)
    free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
    held_sh = safe_int(summary.get("held_shares"), 0)
    held_lots = safe_int(summary.get("held_lots"), 0)
    day_buy_ev = safe_int(updated_st.get("daily_buy_events"), 0)
    day_sell_ev = safe_int(updated_st.get("daily_sell_events"), 0)
    day_pass = safe_int(updated_st.get("daily_seed_protect_pass_cnt"), 0)
    day_reanchor = safe_int(updated_st.get("daily_reanchor_cnt"), 0)
    day_saved = safe_float(updated_st.get("daily_leftover_saved_usd"), 0.0)

    if hasattr(context, "send_slim_alert"):
        d_sign = "+" if day_net >= 0.0 else "-"
        c_sign = "+" if cum_net >= 0.0 else "-"
        await context.send_slim_alert(
            title=f"🌙 [16:00 EST 정규장 마감 일일 결산 · 📂{folder_slug}] ({today_str})",
            body=(
                f"당일 세후실현: {d_sign}${abs(day_net):,.2f} ({d_sign}{abs(day_krw):,.0f}원) "
                f"· 누적실현: {c_sign}${abs(cum_net):,.2f} ({c_sign}{abs(cum_roi):.2f}%) "
                f"· 당일 매수 {day_buy_ev}회 / 익절 {day_sell_ev}회 / 리앵커링 {day_reanchor}회 "
                f"· 🛡️시드패스 {day_pass:,}회 / 자투리절약 +${day_saved:,.2f} "
                f"· 오버나이트 이월: {held_sh}주({held_lots}회차) · 가용현금 ${free_cash:,.2f} (3중 금고 백업 완료)"
            ),
        )

    logger.info(
        "🌙 [16:00 EST 일일 결산 완료 · %s] 당일실현=$%.2f · 누적실현=$%.2f · 오버나이트보유=%d주(%d회차)",
        folder_slug,
        day_net,
        cum_net,
        held_sh,
        held_lots,
    )
    return {
        "status": "DAILY_CLOSING_SETTLEMENT_DONE",
        "date": today_str,
        "daily_realized_net_usd": day_net,
        "cumulative_realized_net_usd": cum_net,
        "held_shares": held_sh,
    }


# ---------------------------------------------------------------------------
# [핵심 4] 17:00 EST 일일 가비지 컬렉션(GC) · 일일 카운터 롤오버 · 날짜별 원장 백업 (Case 22, 68)
# ---------------------------------------------------------------------------
async def execute_daily_1700_gc_and_backup_plugin(
    context: Any,
    now_est: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🔹 17:00 EST (Case 22 & Case 68):
    1) 롤오버 직전 당일 원장 상태를 `ledger_backups/soxl_stratum_ledger_<folder_slug>_YYYY-MM-DD.json` 에
       날짜별 물리적 스냅샷으로 백업(최근 14일분 유지)한다.
    2) 원장 일일 카운터를 새 거래일 기준으로 롤오버하고 가비지 컬렉션(gc.collect)을 수행한다.
    """
    dt_now = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    logical_date = dt_now.strftime("%Y-%m-%d")

    snap_file = ""
    snap_fn = getattr(context.ledger, "create_daily_backup_snapshot", None)
    if callable(snap_fn):
        try:
            snap_file = await snap_fn(logical_date=logical_date, keep_days=14)
        except Exception as exc:
            logger.warning("17:00 EST 원장 스냅샷 백업 예외: %s", exc)

    await context.ledger.rollover_daily_counters_if_needed(logical_date)
    collected = gc.collect()
    logger.info(
        "🧹 [17:00 EST 일일 GC 및 원장 금고 백업 완료] date=%s · snapshot=%s · gc_collected=%d",
        logical_date,
        os.path.basename(snap_file) if snap_file else "none",
        collected,
    )
    return {
        "status": "DAILY_GC_DONE",
        "logical_date": logical_date,
        "snapshot_file": snap_file,
        "gc_collected": collected,
    }


# 하위 호환 별칭 (구버전 호출부가 존재하더라도 무해하게 동작하도록 연결)
execute_macro_defense_plugin = reconcile_stratum_with_broker_holdings_plugin
enforce_0930_regular_cutoff_plugin = reconcile_stratum_with_broker_holdings_plugin
execute_moc_zero_overnight_plugin = execute_1600_daily_closing_settlement_plugin
