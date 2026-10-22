# -*- coding: utf-8 -*-
"""
# MODIFIED: main.py
인피니트 스노우볼 (Infinite Snowball) — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
메인 오케스트레이터 및 비동기 이벤트 루프 컨트롤러 (TossApiClient 폴리필 & AST 심볼 100% 호환 완비판)
- 전술 A 챔피언 (🛡️ 05_AF_WIDE_10_01) 100% 결속:
  1) 8%(12.5분할) 황금비율 정액 기반 정량제 & 자투리 절약금 · 세후 실현수익금 실시간 시드 풀 복리 산입 (COMP_08PCT 고정)
  2) 0주 상태 60초 타임쉴드 확인 후 실시간 현재가 조회 및 현재가 지정가(LIMIT) 실물 1층(REENTRY_1FL) 산입
  3) 2층(MERGED_UPPER) 단독 보유 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 실물 1층(REENTRY_1FL) 재장전
  4) 기억된 사이클 최저 매수가(cycle_min_buy_price) 잠금 기반 10분 고정 4단 와이드 활주로 시드보호 매수 게이트
     (WIDE_RUNWAY : 1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5% 신저가 도달 시에만 현재가 지정가 추매)
  5) 매수 후 60초 타임쉴드 & 1분 상시 순수 현재가 지정가 스나이퍼 매도(1층 +1.50% 익절 & 2층 리앵커링 / 2층 +1.00% 전량 졸업)
  6) 🚨 [Case 66 미결 매수 선행 정산] 04:07 ~ 16:00 EST 루프 진입 시 reconcile 호출 전에 sync_pending_buy_order_plugin(Step 3-0)을
     최우선 실행하여, 거래소 즉시 체결 시에도 텔레그램 체결 알림 · 자투리 절약금 누적 · 60초 타임쉴드 각인이 100% 실행되도록 보장!
  7) 🔗 [TossApiClient 런타임 폴리필] get_holdings_map_with_status, get_candles_1m, get_candles_1d 등 메서드 명칭 차이를
     자동 흡수하는 어댑터를 장착하여 AttributeError 를 원천 차단!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 66 전수 락온
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import plugin_updater
import quant_engine
import quant_indicators_plugin
import tg_ui_plugin
import toss_api
from defense_plugin import (
    execute_0400_premarket_stabilization_plugin,
    execute_1600_daily_closing_settlement_plugin,
    execute_daily_1700_gc_and_backup_plugin,
    reconcile_stratum_with_broker_holdings_plugin,
    relock_stratum_on_bot_resume,
)
from plugin_updater import (
    purge_deprecated_legacy_files,
    verify_all_modules_compile,
)
from quant_engine import (
    buckets_from_dicts,
    run_quant_engine_tdd,
    summarize_stratum_portfolio,
)
from quant_indicators_plugin import (
    evaluate_buy_gate_bucket_status,
    is_active_trading_window,
    refresh_metrics_with_live_tick,
)
from sniper_exit_plugin import (
    execute_1m_sniper_sell_and_reanchor_plugin,
    sync_pending_sell_order_plugin,
    update_exit_snapshot_on_hold,
)
from sniper_plugin import (
    execute_seed_protected_buy_gate_plugin,
    sync_pending_buy_order_plugin,
)
from toss_api import (
    EST_TZ,
    LOCKED_BUY_INTERVAL_MIN,
    TARGET_SYMBOL,
    TossApiClient,
    safe_float,
    safe_int,
)

# ---------------------------------------------------------------------------
# 로깅 포맷 설정 (EST/KST 병기 지원 및 저소음 고신뢰 로그)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("toss_sniper.main")

# 메인 루프 주기 상수 (평시 1.5초 / 고주파 스나이퍼 부스터 0.4초)
NORMAL_LOOP_INTERVAL_SEC: float = 1.50
FAST_SHIELD_LOOP_INTERVAL_SEC: float = 0.40
KRW_PER_USD_DEFAULT: float = 1380.0


# ---------------------------------------------------------------------------
# [핵심 1] Case 13 & Case 62 : 적응형 비동기 플러그인 호출기 및 동적 심볼 리졸버
# ---------------------------------------------------------------------------
async def _call_plugin_adaptive(
    func: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    """
    🚨 Case 13:
    플러그인 함수의 시그니처(인자 목록 및 **kwargs 수용 여부)를 동적으로 검사하여
    허용된 인자만 안전하게 주입하고 동기/비동기 함수를 모두 실행한다.
    """
    sig = inspect.signature(func)
    accepts_var_kw = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in sig.parameters.values()
    )
    if accepts_var_kw:
        filtered_kwargs = kwargs
    else:
        filtered_kwargs = {
            k: v for k, v in kwargs.items() if k in sig.parameters
        }

    res = func(**filtered_kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


def _resolve_module_attr(
    modules: Iterable[Any],
    candidate_names: Tuple[str, ...],
    predicate: Optional[Callable[[Any], bool]] = None,
) -> Any:
    """
    주어진 모듈 목록에서 후보 심볼명을 순차 탐색하거나 조건(predicate)에 부합하는 객체를 반환한다.
    """
    for mod in modules:
        for name in candidate_names:
            obj = getattr(mod, name, None)
            if obj is not None:
                return obj
    if predicate is not None:
        for mod in modules:
            for attr_name in dir(mod):
                if attr_name.startswith("_"):
                    continue
                obj = getattr(mod, attr_name, None)
                try:
                    if predicate(obj):
                        return obj
                except Exception:
                    pass
    return None


# ---------------------------------------------------------------------------
# [핵심 1-B] TossApiClient 런타임 메서드 호환성 폴리필 어댑터 (AttributeError 원천 소각)
# ---------------------------------------------------------------------------
def _empty_soxl_holding_entry() -> Dict[str, Any]:
    return {
        "symbol": TARGET_SYMBOL,
        "qty": 0,
        "quantity": 0,
        "avg_price": 0.0,
        "averagePurchasePrice": 0.0,
        "last_price": 0.0,
        "lastPrice": 0.0,
        "purchase_amount": 0.0,
        "eval_amount": 0.0,
        "profit_loss": 0.0,
        "profit_rate": 0.0,
    }


def _normalize_single_holding_item(
    item: Dict[str, Any], symbol: str = TARGET_SYMBOL
) -> Dict[str, Any]:
    sym = str(item.get("symbol") or symbol or TARGET_SYMBOL).upper().strip()
    qty = max(
        0,
        safe_int(
            item.get("qty")
            if item.get("qty") is not None
            else item.get("quantity"),
            0,
        ),
    )
    avg_p = round(
        max(
            0.0,
            safe_float(
                item.get("avg_price")
                if item.get("avg_price") is not None
                else item.get("averagePurchasePrice"),
                0.0,
            ),
        ),
        4,
    )
    last_p = round(
        max(
            0.0,
            safe_float(
                item.get("last_price")
                if item.get("last_price") is not None
                else item.get("lastPrice"),
                avg_p,
            ),
        ),
        4,
    )

    mv_obj = (
        item.get("marketValue")
        if isinstance(item.get("marketValue"), dict)
        else {}
    )
    pl_obj = (
        item.get("profitLoss")
        if isinstance(item.get("profitLoss"), dict)
        else {}
    )

    pur_amt = safe_float(
        item.get("purchase_amount")
        if item.get("purchase_amount") is not None
        else mv_obj.get("purchaseAmount"),
        round(float(qty) * avg_p, 4),
    )
    eval_amt = safe_float(
        item.get("eval_amount")
        if item.get("eval_amount") is not None
        else mv_obj.get("amount"),
        round(float(qty) * last_p, 4),
    )
    pnl_amt = safe_float(
        item.get("profit_loss")
        if item.get("profit_loss") is not None
        else (
            pl_obj.get("amountAfterCost")
            if pl_obj.get("amountAfterCost") is not None
            else pl_obj.get("amount")
        ),
        round(eval_amt - pur_amt, 4),
    )
    raw_rate = safe_float(
        item.get("profit_rate")
        if item.get("profit_rate") is not None
        else (
            pl_obj.get("rateAfterCost")
            if pl_obj.get("rateAfterCost") is not None
            else pl_obj.get("rate")
        ),
        0.0,
    )
    # 토스 API의 rate는 소수비율(예: 0.1333 = 13.33%)로 내려오므로 절대값이 5.0 이하이고 실제 손익률과 차이가 나면 보정
    if pur_amt > 0.0 and abs(raw_rate) < 2.0 and abs(pnl_amt / pur_amt) > 0.005:
        profit_rate = round((pnl_amt / pur_amt) * 100.0, 2)
    else:
        profit_rate = round(raw_rate, 2)

    return {
        "symbol": sym,
        "qty": qty,
        "quantity": qty,
        "avg_price": avg_p,
        "averagePurchasePrice": avg_p,
        "last_price": last_p,
        "lastPrice": last_p,
        "purchase_amount": round(pur_amt, 4),
        "eval_amount": round(eval_amt, 4),
        "profit_loss": round(pnl_amt, 4),
        "profit_rate": profit_rate,
    }


def _normalize_holdings_payload(
    raw_res: Any,
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """
    TossApiClient 의 기존 잔고 조회 함수 반환값(튜플, 맵 딕셔너리, 원본 BFF JSON, 리스트)을
    표준 (holdings_map, fetch_ok) 튜플로 변환한다.
    """
    if raw_res is None:
        return {}, False

    if isinstance(raw_res, tuple) and len(raw_res) == 2:
        first, second = raw_res
        if isinstance(first, bool) and isinstance(second, dict):
            norm_map, _ = _normalize_holdings_payload(second)
            return norm_map, first
        if isinstance(first, dict):
            norm_map, _ = _normalize_holdings_payload(first)
            return norm_map, bool(second)

    out_map: Dict[str, Dict[str, Any]] = {
        TARGET_SYMBOL: _empty_soxl_holding_entry()
    }

    if isinstance(raw_res, dict):
        # 1) 이미 {"SOXL": {...}} 형태인 경우
        if TARGET_SYMBOL in raw_res and isinstance(
            raw_res[TARGET_SYMBOL], dict
        ):
            out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                raw_res[TARGET_SYMBOL], TARGET_SYMBOL
            )
            return out_map, True

        # 2) 단일 종목 딕셔너리 형태인 경우
        if (
            str(raw_res.get("symbol") or "").upper().strip() == TARGET_SYMBOL
            and ("quantity" in raw_res or "qty" in raw_res)
        ):
            out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                raw_res, TARGET_SYMBOL
            )
            return out_map, True

        # 3) 토스 Open API 원본 BFF 응답 {"result": {"items": [...]}} 또는 {"items": [...]}
        res_obj = (
            raw_res.get("result")
            if isinstance(raw_res.get("result"), dict)
            else raw_res
        )
        items = res_obj.get("items") if isinstance(res_obj, dict) else None
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    sym = str(it.get("symbol") or "").upper().strip()
                    if sym == TARGET_SYMBOL:
                        out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                            it, TARGET_SYMBOL
                        )
            return out_map, True

        # 4) 빈 딕셔너리({})인 경우 (미보유 0주 정상 응답)
        if len(raw_res) == 0:
            return out_map, True

    if isinstance(raw_res, list):
        for it in raw_res:
            if isinstance(it, dict):
                sym = str(it.get("symbol") or "").upper().strip()
                if sym == TARGET_SYMBOL:
                    out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                        it, TARGET_SYMBOL
                    )
        return out_map, True

    return out_map, True


async def _invoke_raw_toss_api(
    api: Any,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    require_account: bool = False,
) -> Any:
    """
    TossApiClient 내부의 HTTP 호출 메서드(_request / request / _api_request / _get 등)를
    시그니처 적응형으로 호출한다.
    """
    for req_name in (
        "_request",
        "request",
        "_api_request",
        "_call_api",
        "_ fetch_json",
    ):
        req_fn = getattr(api, req_name.replace(" ", ""), None)
        if callable(req_fn):
            sig = inspect.signature(req_fn)
            kw: Dict[str, Any] = {}
            accepts_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in sig.parameters.values()
            )
            for p_name in sig.parameters:
                if p_name in ("method", "http_method"):
                    kw[p_name] = method
                elif p_name in ("path", "endpoint", "url", "uri"):
                    kw[p_name] = path
                elif p_name in ("params", "query", "query_params"):
                    kw[p_name] = params
                elif p_name in ("json_data", "json", "body", "data", "payload"):
                    kw[p_name] = json_data
                elif p_name in (
                    "require_account",
                    "use_account",
                    "account_required",
                    "with_account",
                    "need_account",
                ):
                    kw[p_name] = require_account
                elif p_name in ("rate_group", "group"):
                    kw[p_name] = (
                        "MARKET_DATA_CHART"
                        if "candles" in path
                        else ("ASSET" if "holdings" in path else "MARKET_DATA")
                    )
            if accepts_var_kw:
                if "params" not in kw and params is not None:
                    kw["params"] = params
                if "json_data" not in kw and json_data is not None:
                    kw["json_data"] = json_data
            try:
                res = req_fn(**kw)
                if inspect.isawaitable(res):
                    return await res
                return res
            except TypeError:
                res = req_fn(method, path, params=params)
                if inspect.isawaitable(res):
                    return await res
                return res
    return None


def _extract_candles_list(raw_res: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if isinstance(raw_res, list):
        return [c for c in raw_res if isinstance(c, dict)], None
    if isinstance(raw_res, dict):
        res_obj = (
            raw_res.get("result")
            if isinstance(raw_res.get("result"), dict)
            else raw_res
        )
        if isinstance(res_obj, dict):
            c_list = res_obj.get("candles")
            next_b = res_obj.get("nextBefore")
            if isinstance(c_list, list):
                return (
                    [c for c in c_list if isinstance(c, dict)],
                    str(next_b) if next_b else None,
                )
    return [], None


def _install_toss_api_compatibility_patches() -> None:
    """
    NEW: 서버의 TossApiClient 에 get_holdings_map_with_status, get_candles_1m, get_candles_1d 등의
    별칭 메서드가 없을 경우 기존 메서드를 감싸거나 토스 Open API v1.2.14 규격으로 호출하는 폴리필을 자동 장착한다.
    """
    # 1) get_holdings_map_with_status 폴리필
    if not hasattr(TossApiClient, "get_holdings_map_with_status"):

        async def _poly_get_holdings_map_with_status(
            self: Any, symbol: str = TARGET_SYMBOL
        ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
            for cand_name in (
                "get_holdings_map",
                "get_holdings",
                "fetch_holdings_map",
                "fetch_holdings",
                "get_account_holdings",
                "get_broker_holdings",
                "get_positions",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(fn, symbol=symbol)
                        return _normalize_holdings_payload(res)
                    except Exception as exc:
                        logger.warning(
                            "TossApiClient.%s 호출 예외: %s", cand_name, exc
                        )
                        return {}, False

            try:
                raw = await _invoke_raw_toss_api(
                    self,
                    method="GET",
                    path="/api/v1/holdings",
                    params={"symbol": symbol},
                    require_account=True,
                )
                return _normalize_holdings_payload(raw)
            except Exception as exc:
                logger.warning("GET /api/v1/holdings 폴백 호출 예외: %s", exc)
                return {}, False

        setattr(
            TossApiClient,
            "get_holdings_map_with_status",
            _poly_get_holdings_map_with_status,
        )

    # 2) get_candles_1m 폴리필
    if not hasattr(TossApiClient, "get_candles_1m"):

        async def _poly_get_candles_1m(
            self: Any,
            symbol: str = TARGET_SYMBOL,
            total_count: int = 300,
            count: Optional[int] = None,
            **kwargs: Any,
        ) -> List[Dict[str, Any]]:
            req_total = max(1, safe_int(count or total_count, 200))
            for cand_name in (
                "get_candles_paginated",
                "fetch_candles_1m",
                "get_minute_candles",
                "get_1m_candles",
                "get_candles",
                "fetch_candles",
                "get_chart_candles",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(
                            fn,
                            symbol=symbol,
                            interval="1m",
                            count=min(200, req_total),
                            total_count=req_total,
                            adjusted=True,
                        )
                        c_list, _ = _extract_candles_list(res)
                        if c_list:
                            return c_list
                    except Exception as exc:
                        logger.debug(
                            "TossApiClient.%s(1m) 폴백 전환: %s", cand_name, exc
                        )

            # 직접 GET /api/v1/candles 호출 (최대 2페이지 페이지네이션 지원)
            collected: List[Dict[str, Any]] = []
            before_cursor: Optional[str] = None
            pages = 2 if req_total > 200 else 1
            for _ in range(pages):
                rem = max(1, min(200, req_total - len(collected)))
                q_params: Dict[str, Any] = {
                    "symbol": symbol,
                    "interval": "1m",
                    "count": rem,
                    "adjusted": "true",
                }
                if before_cursor:
                    q_params["before"] = before_cursor
                raw = await _invoke_raw_toss_api(
                    self,
                    method="GET",
                    path="/api/v1/candles",
                    params=q_params,
                    require_account=False,
                )
                page_candles, next_b = _extract_candles_list(raw)
                if not page_candles:
                    break
                collected.extend(page_candles)
                if not next_b or len(collected) >= req_total:
                    break
                before_cursor = next_b
            return collected

        setattr(TossApiClient, "get_candles_1m", _poly_get_candles_1m)

    # 3) get_candles_1d 폴리필
    if not hasattr(TossApiClient, "get_candles_1d"):

        async def _poly_get_candles_1d(
            self: Any,
            symbol: str = TARGET_SYMBOL,
            count: int = 10,
            **kwargs: Any,
        ) -> List[Dict[str, Any]]:
            req_cnt = max(1, min(200, safe_int(count, 10)))
            for cand_name in (
                "fetch_candles_1d",
                "get_daily_candles",
                "get_1d_candles",
                "get_candles",
                "fetch_candles",
                "get_chart_candles",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(
                            fn,
                            symbol=symbol,
                            interval="1d",
                            count=req_cnt,
                            total_count=req_cnt,
                            adjusted=True,
                        )
                        c_list, _ = _extract_candles_list(res)
                        if c_list:
                            return c_list
                    except Exception as exc:
                        logger.debug(
                            "TossApiClient.%s(1d) 폴백 전환: %s", cand_name, exc
                        )

            raw = await _invoke_raw_toss_api(
                self,
                method="GET",
                path="/api/v1/candles",
                params={
                    "symbol": symbol,
                    "interval": "1d",
                    "count": req_cnt,
                    "adjusted": "true",
                },
                require_account=False,
            )
            c_list, _ = _extract_candles_list(raw)
            return c_list

        setattr(TossApiClient, "get_candles_1d", _poly_get_candles_1d)

    # 4) get_open_orders 폴리필
    if not hasattr(TossApiClient, "get_open_orders"):

        async def _poly_get_open_orders(
            self: Any, symbol: str = TARGET_SYMBOL
        ) -> List[Dict[str, Any]]:
            for cand_name in (
                "fetch_open_orders",
                "get_orders",
                "list_open_orders",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(
                            fn, status="OPEN", symbol=symbol
                        )
                        if isinstance(res, list):
                            return res
                        if isinstance(res, dict):
                            r_obj = (
                                res.get("result")
                                if isinstance(res.get("result"), dict)
                                else res
                            )
                            if isinstance(r_obj, dict) and isinstance(
                                r_obj.get("orders"), list
                            ):
                                return r_obj["orders"]
                    except Exception:
                        pass
            raw = await _invoke_raw_toss_api(
                self,
                method="GET",
                path="/api/v1/orders",
                params={"status": "OPEN", "symbol": symbol},
                require_account=True,
            )
            if isinstance(raw, dict):
                r_obj = (
                    raw.get("result")
                    if isinstance(raw.get("result"), dict)
                    else raw
                )
                if isinstance(r_obj, dict) and isinstance(
                    r_obj.get("orders"), list
                ):
                    return r_obj["orders"]
            return []

        setattr(TossApiClient, "get_open_orders", _poly_get_open_orders)

    # 5) cancel_all_open_orders 폴리필
    if not hasattr(TossApiClient, "cancel_all_open_orders"):

        async def _poly_cancel_all_open_orders(
            self: Any, symbol: str = TARGET_SYMBOL
        ) -> int:
            orders = await self.get_open_orders(symbol=symbol)
            cnt = 0
            for o in orders:
                if isinstance(o, dict):
                    oid = str(o.get("orderId") or "").strip()
                    if oid and hasattr(self, "cancel_order"):
                        try:
                            await self.cancel_order(oid)
                            cnt += 1
                        except Exception:
                            pass
            return cnt

        setattr(
            TossApiClient,
            "cancel_all_open_orders",
            _poly_cancel_all_open_orders,
        )

    # 6) find_latest_open_order_id 폴리필
    if not hasattr(TossApiClient, "find_latest_open_order_id"):

        async def _poly_find_latest_open_order_id(
            self: Any, symbol: str = TARGET_SYMBOL, side: Optional[str] = None
        ) -> str:
            orders = await self.get_open_orders(symbol=symbol)
            target_side = str(side or "").upper().strip()
            for o in orders:
                if isinstance(o, dict):
                    o_side = str(o.get("side") or "").upper().strip()
                    if not target_side or o_side == target_side:
                        oid = str(o.get("orderId") or "").strip()
                        if oid:
                            return oid
            return ""

        setattr(
            TossApiClient,
            "find_latest_open_order_id",
            _poly_find_latest_open_order_id,
        )

    # 7) get_usd_krw_rate 폴리필
    if not hasattr(TossApiClient, "get_usd_krw_rate"):

        async def _poly_get_usd_krw_rate(self: Any) -> float:
            for cand_name in (
                "get_exchange_rate",
                "fetch_usd_krw_rate",
                "fetch_exchange_rate",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(
                            fn, baseCurrency="USD", quoteCurrency="KRW"
                        )
                        if isinstance(res, (int, float)) and float(res) > 0.0:
                            return float(res)
                        if isinstance(res, dict):
                            r_obj = (
                                res.get("result")
                                if isinstance(res.get("result"), dict)
                                else res
                            )
                            rate_v = safe_float(r_obj.get("rate"), 0.0)
                            if rate_v > 0.0:
                                return rate_v
                    except Exception:
                        pass
            try:
                raw = await _invoke_raw_toss_api(
                    self,
                    method="GET",
                    path="/api/v1/exchange-rate",
                    params={"baseCurrency": "USD", "quoteCurrency": "KRW"},
                    require_account=False,
                )
                if isinstance(raw, dict):
                    r_obj = (
                        raw.get("result")
                        if isinstance(raw.get("result"), dict)
                        else raw
                    )
                    rate_v = safe_float(r_obj.get("rate"), 0.0)
                    if rate_v > 0.0:
                        return rate_v
            except Exception:
                pass
            return KRW_PER_USD_DEFAULT

        setattr(TossApiClient, "get_usd_krw_rate", _poly_get_usd_krw_rate)


# 모듈 임포트 즉시 TossApiClient 호환성 폴리필 장착
_install_toss_api_compatibility_patches()


def _instantiate_stratum_ledger(filepath: str) -> Any:
    """
    toss_api 또는 quant_engine 내에 정의된 원장 클래스를 동적으로 찾아 인스턴스화한다.
    """
    ledger_cls = _resolve_module_attr(
        modules=(toss_api, quant_engine),
        candidate_names=(
            "SoxlStratumLedger",
            "StratumLedger",
            "AssassinLedger",
            "TossStratumLedger",
            "SoxlLedger",
        ),
        predicate=lambda c: (
            inspect.isclass(c)
            and hasattr(c, "get_state")
            and hasattr(c, "update_state")
        ),
    )
    if ledger_cls is None:
        raise RuntimeError(
            "원장 클래스(get_state/update_state 보유)를 찾을 수 없습니다."
        )

    try:
        sig = inspect.signature(ledger_cls.__init__)
        if "filepath" in sig.parameters:
            return ledger_cls(filepath=filepath)
        if len(sig.parameters) >= 2:
            return ledger_cls(filepath)
        return ledger_cls()
    except Exception:
        return ledger_cls(filepath)


def _check_weekend_nyse_closed_dynamic(now_est: datetime) -> bool:
    """
    quant_indicators_plugin 내 주말 휴장 판별 함수를 동적 호출하거나 표준 EST 주말 규칙(금 17:00 ~ 일 17:59 EST)을 적용한다.
    """
    fn = _resolve_module_attr(
        modules=(quant_indicators_plugin, toss_api),
        candidate_names=(
            "is_weekend_nyse_closed",
            "is_weekend_closed",
            "is_nyse_weekend_closed",
        ),
    )
    if callable(fn):
        try:
            return bool(fn(now_est))
        except Exception:
            pass

    dt = now_est.astimezone(EST_TZ)
    wd = dt.weekday()
    hm = dt.hour * 100 + dt.minute
    if wd == 4 and hm >= 1700:
        return True
    if wd == 5:
        return True
    if wd == 6 and hm < 1800:
        return True
    return False


async def _compute_candle_metrics_dynamic(
    context: Any,
    candles_1m: List[Dict[str, Any]],
    live_price: float,
    now_est: datetime,
    candles_1d: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    quant_indicators_plugin 내 1분봉 지표 산출 함수를 시그니처 적응형으로 호출한다.
    """
    excluded = {
        "evaluate_buy_gate_bucket_status",
        "is_active_trading_window",
        "refresh_metrics_with_live_tick",
        "is_weekend_nyse_closed",
        "is_weekend_closed",
    }
    fn = _resolve_module_attr(
        modules=(quant_indicators_plugin, quant_engine),
        candidate_names=(
            "compute_soxl_quant_metrics_1m",
            "compute_quant_metrics_1m",
            "calculate_soxl_quant_metrics_1m",
            "compute_candle_metrics_1m",
            "build_soxl_candle_metrics",
            "calculate_vwap_metrics_1m",
            "compute_soxl_indicators",
            "calculate_intraday_vwap_metrics",
        ),
        predicate=lambda f: (
            inspect.isfunction(f)
            and getattr(f, "__module__", "")
            == quant_indicators_plugin.__name__
            and f.__name__ not in excluded
            and not f.__name__.startswith("_")
        ),
    )
    if callable(fn):
        try:
            res = await _call_plugin_adaptive(
                fn,
                context=context,
                api=context.api,
                symbol=TARGET_SYMBOL,
                candles=candles_1m,
                candles_1m=candles_1m,
                minute_candles=candles_1m,
                daily_candles=candles_1d,
                candles_1d=candles_1d,
                day_candles=candles_1d,
                live_price=live_price,
                current_price=live_price,
                price=live_price,
                now_est=now_est,
                dt_now=now_est,
            )
            if isinstance(res, dict) and res:
                return refresh_metrics_with_live_tick(
                    res, live_price, now_est=now_est
                )
        except Exception as exc:
            logger.debug("캔들 지표 함수 동적 호출 폴백: %s", exc)

    return refresh_metrics_with_live_tick(
        {"symbol": TARGET_SYMBOL, "live_price": live_price},
        live_price,
        now_est=now_est,
    )


class _AdaptiveTelegramBridge:
    """
    tg_ui_plugin 내의 컨트롤러 클래스 또는 모듈 레벨 텔레그램 함수들을 동적으로 감싸서
    SniperAppContext 와 100% 호환시키는 어댑터.
    """

    def __init__(self, context: Any) -> None:
        self.context = context
        self._impl: Any = None

        ctrl_cls = _resolve_module_attr(
            modules=(tg_ui_plugin,),
            candidate_names=(
                "TelegramCommandController",
                "TelegramBotController",
                "TelegramUiController",
                "SniperTelegramController",
                "TelegramController",
                "TelegramRouter",
            ),
            predicate=lambda c: (
                inspect.isclass(c)
                and (
                    hasattr(c, "run_polling_loop")
                    or hasattr(c, "poll_updates_loop")
                    or hasattr(c, "send_slim_alert")
                )
            ),
        )
        if ctrl_cls is not None:
            try:
                sig = inspect.signature(ctrl_cls.__init__)
                if "context" in sig.parameters:
                    self._impl = ctrl_cls(context=context)
                elif len(sig.parameters) >= 2:
                    self._impl = ctrl_cls(context)
                else:
                    self._impl = ctrl_cls()
            except Exception as exc:
                logger.warning("텔레그램 컨트롤러 클래스 초기화 폴백: %s", exc)

    async def send_slim_alert(self, title: str, body: str = "") -> bool:
        if self._impl is not None and hasattr(self._impl, "send_slim_alert"):
            res = await _call_plugin_adaptive(
                self._impl.send_slim_alert,
                context=self.context,
                title=title,
                body=body,
                message=f"{title}\n{body}" if body else title,
            )
            return bool(res)

        fn = _resolve_module_attr(
            modules=(tg_ui_plugin,),
            candidate_names=(
                "send_slim_alert",
                "send_telegram_slim_alert",
                "send_telegram_alert",
                "send_telegram_message",
            ),
        )
        if callable(fn):
            res = await _call_plugin_adaptive(
                fn,
                context=self.context,
                title=title,
                body=body,
                text=f"{title}\n{body}" if body else title,
                message=f"{title}\n{body}" if body else title,
            )
            return bool(res)
        return False

    async def send_startup_alive_banner(self) -> None:
        if self._impl is not None and hasattr(
            self._impl, "send_startup_alive_banner"
        ):
            await _call_plugin_adaptive(
                self._impl.send_startup_alive_banner, context=self.context
            )
            return

        fn = _resolve_module_attr(
            modules=(tg_ui_plugin,),
            candidate_names=(
                "send_startup_alive_banner",
                "send_startup_banner",
                "notify_server_startup",
            ),
        )
        if callable(fn):
            await _call_plugin_adaptive(fn, context=self.context)
            return

        await self.send_slim_alert(
            title="✅ 서버가 정상 작동 중입니다.",
            body="🐂 [인피니트 스노우볼 v3.0 · 🛡️ 05_AF_WIDE_10_01] 양방향 60초 타임쉴드 & 선행 체결정산(Case 65/66) 가동 완료",
        )

    async def run_polling_loop(self) -> None:
        if self._impl is not None:
            for m_name in (
                "run_polling_loop",
                "poll_updates_loop",
                "telegram_polling_loop",
                "start_polling",
                "run",
            ):
                m_fn = getattr(self._impl, m_name, None)
                if callable(m_fn):
                    await _call_plugin_adaptive(m_fn, context=self.context)
                    return

        fn = _resolve_module_attr(
            modules=(tg_ui_plugin,),
            candidate_names=(
                "run_telegram_polling_loop",
                "telegram_polling_loop",
                "run_polling_loop",
                "poll_telegram_updates_loop",
                "start_telegram_polling_loop",
            ),
            predicate=lambda f: (
                inspect.iscoroutinefunction(f) and "poll" in f.__name__.lower()
            ),
        )
        if callable(fn):
            await _call_plugin_adaptive(fn, context=self.context)
            return

        while not self.context.shutdown_event.is_set():
            await asyncio.sleep(5.0)


async def _run_auto_update_loop_dynamic(
    context: Any, interval_sec: float = 3600.0
) -> None:
    """
    plugin_updater 모듈 내의 자동 업데이트 루프 함수를 동적으로 찾아 실행하거나 안전한 주기 점검 루프를 가동한다.
    """
    fn = _resolve_module_attr(
        modules=(plugin_updater,),
        candidate_names=(
            "run_auto_update_loop",
            "auto_update_loop",
            "start_auto_update_loop",
            "background_auto_update_loop",
        ),
        predicate=lambda f: (
            inspect.iscoroutinefunction(f) and "loop" in f.__name__.lower()
        ),
    )
    if callable(fn):
        await _call_plugin_adaptive(
            fn, context=context, interval_sec=interval_sec
        )
        return

    while not context.shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=interval_sec
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 2] 스나이퍼 애플리케이션 전역 컨텍스트 (SniperAppContext)
# ---------------------------------------------------------------------------
class SniperAppContext:
    """
    9대 코어 모듈이 공유하는 단일 런타임 상태 컨텍스트.
    - api: TossApiClient (제1헌법 GlobalThrottle & 제2헌법 AccountSeq 락온 & 런타임 폴리필 완료)
    - ledger: SOXL 단일 롱 지층 원장 (buy_interval_min = 10 & COMP_08PCT 하드 고정)
    - tg: _AdaptiveTelegramBridge (텔레그램 관제 UI & 2대 옵션 전술 제어반)
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        _install_toss_api_compatibility_patches()
        self.base_dir: str = os.path.abspath(
            base_dir or os.path.dirname(os.path.abspath(__file__)) or "."
        )
        self.ledger_path: str = os.path.join(
            self.base_dir, "soxl_stratum_ledger.json"
        )

        self.api: TossApiClient = TossApiClient()
        self.ledger: Any = _instantiate_stratum_ledger(
            filepath=self.ledger_path
        )

        # 비동기 동시성 제어 뮤텍스 및 웨이크업 이벤트
        self.in_memory_ordering_lock: asyncio.Lock = asyncio.Lock()
        self.dual_strike_mutex: asyncio.Lock = asyncio.Lock()
        self.wakeup_event: asyncio.Event = asyncio.Event()
        self.shutdown_event: asyncio.Event = asyncio.Event()

        # 실시간 시세 · 잔고 · 1분봉 지표 · 체결 스냅샷 · 인덱싱 그레이스 쉴드 & 60초 타임쉴드 캐시
        self.latest_prices: Dict[str, float] = {TARGET_SYMBOL: 0.0}
        self.latest_holdings: Dict[str, Dict[str, Any]] = {
            TARGET_SYMBOL: _empty_soxl_holding_entry()
        }
        self.holdings_fetch_ok: bool = False
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self.exit_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}

        # 중복 보류 알림 방지 스로틀 맵
        self._strike_hold_alert_ts: Dict[str, float] = {}
        self.last_0400_sync_date: str = ""
        self.fast_shield_mode: bool = False

        self.tg: _AdaptiveTelegramBridge = _AdaptiveTelegramBridge(
            context=self
        )

    async def send_slim_alert(self, title: str, body: str = "") -> bool:
        return await self.tg.send_slim_alert(title=title, body=body)

    async def send_strike_hold_alert_once(
        self,
        symbol: str,
        reason_key: str,
        message: str,
        cooldown_sec: float = 1800.0,
    ) -> None:
        key = f"{symbol}:{reason_key}"
        now_m = time.monotonic()
        last_m = self._strike_hold_alert_ts.get(key, 0.0)
        if (now_m - last_m) < cooldown_sec:
            return
        self._strike_hold_alert_ts[key] = now_m
        await self.send_slim_alert(
            title=f"⚠️ [{symbol} 지층 스나이퍼 안내]",
            body=message,
        )


# ---------------------------------------------------------------------------
# [핵심 3] 백그라운드 태스크 1 : 1분봉 시계열 및 10분 게이트 지표 레이더 (candle_radar_loop)
# ---------------------------------------------------------------------------
async def candle_radar_loop(context: SniperAppContext) -> None:
    """
    04:00 ~ 16:05 EST 운용 시간대에는 5.0초 주기로, 장외 시간대에는 20.0초 주기로
    SOXL 1분봉(300개) 및 최근 일봉(10개)을 조회하여 VWAP · 5MA 진폭 · 하이킨 아시 추세 ·
    10분 고정 매수 게이트 버킷 상태를 갱신한다.
    """
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        hm = now_est.hour * 100 + now_est.minute
        in_market = bool(
            (400 <= hm <= 1605)
            and not _check_weekend_nyse_closed_dynamic(now_est)
        )
        sleep_sec = 5.0 if in_market else 20.0

        try:
            candles_1m = await context.api.get_candles_1m(
                symbol=TARGET_SYMBOL, total_count=300
            )
            candles_1d = await context.api.get_candles_1d(
                symbol=TARGET_SYMBOL, count=10
            )
            live_p = safe_float(
                context.latest_prices.get(TARGET_SYMBOL),
                safe_float(
                    context.latest_holdings.get(TARGET_SYMBOL, {}).get(
                        "last_price"
                    ),
                    0.0,
                ),
            )
            metrics = await _compute_candle_metrics_dynamic(
                context=context,
                candles_1m=candles_1m,
                live_price=live_p,
                now_est=now_est,
                candles_1d=candles_1d,
            )
            context.candle_metrics_cache[TARGET_SYMBOL] = metrics
            if (
                live_p <= 0.0
                and safe_float(metrics.get("live_price"), 0.0) > 0.0
            ):
                context.latest_prices[TARGET_SYMBOL] = safe_float(
                    metrics.get("live_price"), 0.0
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("SOXL 캔들 레이더 루프 예외: %s", exc)

        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=sleep_sec
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 4] 백그라운드 태스크 2 : 04:07 ~ 16:00 EST 메인 지층 스나이퍼 루프 (main_sniper_loop)
# ---------------------------------------------------------------------------
def _evaluate_fast_shield_mode(
    context: SniperAppContext,
    now_est: datetime,
    live_p: float,
) -> bool:
    """
    평시 1.5초 폴링을 0.4초 고주파 스나이퍼 부스터로 전환할지 판정한다:
    1) 미체결 매수 주문(buy_order_id) 또는 미체결 매도 주문(sell_order_id) 진행 중일 때
    2) 보유 수량 0주(held_shares == 0)여서 0주 현재가 지정가 1층 진입을 대기 중일 때
    3) 04:07 EST 기점 10분 고정 매수 게이트 정각 분봉이거나 현재 버킷 미평가 상태일 때
    4) 1층 익절 목표가(+1.50%) 또는 2층 졸업 목표가(+1.00%)에 0.30% 이내로 근접했을 때
    5) 2층 단독 보유 중 눌림목(-1.00%) 또는 상승돌파(+1.00%) 재장전 타점에 근접했을 때
    """
    hm = now_est.hour * 100 + now_est.minute
    if not is_active_trading_window(hm):
        return False

    st = context.ledger.get_state(TARGET_SYMBOL)
    if not bool(st.get("is_active", True)):
        return False

    if str(st.get("buy_order_id") or "").strip() or str(
        st.get("sell_order_id") or ""
    ).strip():
        return True

    summary = summarize_stratum_portfolio(st, live_price=live_p)
    held_sh = safe_int(summary.get("held_shares"), 0)

    if held_sh <= 0:
        return True

    is_gate_open, _, _, b_info = evaluate_buy_gate_bucket_status(
        now_est=now_est,
        buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
        last_evaluated_bucket_id=safe_int(
            st.get("last_evaluated_buy_bucket_id"), -1
        ),
        last_evaluated_date=str(st.get("last_evaluated_date") or ""),
    )
    if is_gate_open or bool(b_info.get("is_exact_boundary_min")):
        return True

    l1_target = safe_float(summary.get("l1_target_price"), 0.0)
    l2_target = safe_float(summary.get("l2_target_price"), 0.0)
    if live_p > 0.0 and l1_target > 0.0 and live_p >= (l1_target * 0.997):
        return True
    if live_p > 0.0 and l2_target > 0.0 and live_p >= (l2_target * 0.997):
        return True

    if bool(summary.get("is_l2_only_anti_bloat")):
        pull_p = safe_float(summary.get("reentry_pullback_price"), 0.0)
        brk_p = safe_float(summary.get("reentry_breakout_price"), 0.0)
        if live_p > 0.0 and (
            (pull_p > 0.0 and live_p <= pull_p * 1.003)
            or (brk_p > 0.0 and live_p >= brk_p * 0.997)
        ):
            return True

    return False


async def main_sniper_loop(context: SniperAppContext) -> None:
    """
    04:00 ~ 16:59 EST 일일 스케줄러 및 메인 지층관리 스나이퍼 루프:
    - 04:00 ~ 04:06 EST: 프리장 개장 초기 호가 안정화 및 오버나이트 지층 동기화
    - 04:07 ~ 16:00 EST:
      Step 3-0) 🚨 [Case 66] 미결 매수 주문(buy_order_id) 최우선 체결 확인 및 커밋
      Step 3-1) 로컬 지층 원장 ↔ 토스 실잔고 화해 (미결 봇 주문 진행 중 가로채기 차단 가드 내장)
      Step 3-2) 매수 후 60초 타임쉴드 검문 & 1분 상시 순수 현재가 지정가 스나이퍼 매도 (체결 시 자전거래 방지 스킵)
      Step 3-3) 매도 후 60초 타임쉴드 검문 & 0주 진입 / 2층 단독 눌림·돌파 재장전 / 10분 4단 와이드 신저가 추매
    - 16:00 ~ 16:59 EST: 정규장 마감 일일 결산 보고 및 보유 지층 오버나이트 보존
    """
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        hm = now_est.hour * 100 + now_est.minute

        try:
            if _check_weekend_nyse_closed_dynamic(now_est):
                context.fast_shield_mode = False
                try:
                    await asyncio.wait_for(
                        context.shutdown_event.wait(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            # 1) SOXL 실시간 현재가 및 보유 잔고 조회 (폴리필 장착으로 AttributeError 0건 보장)
            price_map = await context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            live_p = safe_float(price_map.get(TARGET_SYMBOL), 0.0)
            if live_p > 0.0:
                context.latest_prices[TARGET_SYMBOL] = live_p

            holdings_map, fetch_ok = (
                await context.api.get_holdings_map_with_status()
            )
            context.holdings_fetch_ok = fetch_ok
            if fetch_ok and isinstance(holdings_map, dict):
                context.latest_holdings = holdings_map
                if live_p <= 0.0:
                    live_p = safe_float(
                        holdings_map.get(TARGET_SYMBOL, {}).get("last_price"),
                        0.0,
                    )
                    if live_p > 0.0:
                        context.latest_prices[TARGET_SYMBOL] = live_p

            # 캔들 캐시에 실시간 틱 합성
            if (
                live_p > 0.0
                and TARGET_SYMBOL in context.candle_metrics_cache
                and isinstance(
                    context.candle_metrics_cache[TARGET_SYMBOL], dict
                )
            ):
                context.candle_metrics_cache[TARGET_SYMBOL] = (
                    refresh_metrics_with_live_tick(
                        context.candle_metrics_cache[TARGET_SYMBOL],
                        live_p,
                        now_est=now_est,
                    )
                )

            prices = dict(context.latest_prices)
            holdings = dict(context.latest_holdings)

            # 2) 04:00 ~ 04:06 EST 프리장 오프닝 안정화 및 전일 미체결 청소
            if 400 <= hm <= 406:
                await _call_plugin_adaptive(
                    execute_0400_premarket_stabilization_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

            # 3) 04:07 ~ 16:00 EST (프리장 + 정규장 전 구간 연속 가동)
            elif is_active_trading_window(hm):
                # 🚨 3-0) [Case 66 핵심 방어 : 미결 매수 주문 최우선 선행 정산]
                # 직전 루프에서 발주한 매수 주문(buy_order_id)이 존재하면 reconcile 보다 먼저
                # sync_pending_buy_order_plugin 을 실행하여 정식 매수 체결 알림, 자투리 절약금 누적,
                # 매수 후 60초 타임쉴드 각인이 가로채기 없이 100% 완수되도록 보장한다!
                st_pre = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_pre.get("buy_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_buy_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

                # 3-1) 로컬 지층 원장 ↔ 토스 실보유 잔고 무결성 화해 (Case 66 가드 내장)
                await _call_plugin_adaptive(
                    reconcile_stratum_with_broker_holdings_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

                # 3-2) 매수 후 60초 타임쉴드 검문 & 1분 상시 순수 현재가 지정가 스나이퍼 매도 (Case 44, 65)
                sell_res = await _call_plugin_adaptive(
                    execute_1m_sniper_sell_and_reanchor_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )
                did_sell = bool(
                    isinstance(sell_res, dict)
                    and sell_res.get("did_sell_or_ratchet")
                )

                # 🚨 Case 44: 동일 틱에서 매도가 발생(또는 진행 중)한 경우 추가 매수를 즉시 스킵하여 자전거래 차단!
                if not did_sell:
                    # 3-3) 매도 후 60초 타임쉴드 검문 & 0주 진입 / 2층 단독 눌림·돌파 재장전 / 10분 4단 와이드 신저가 추매
                    await _call_plugin_adaptive(
                        execute_seed_protected_buy_gate_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

            # 4) 16:00 ~ 16:59 EST 정규장 마감 일일 결산 보고 (보유 지층 오버나이트 보존)
            elif 1600 <= hm <= 1659:
                await _call_plugin_adaptive(
                    execute_1600_daily_closing_settlement_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

            # 5) 장외 대기 시간에도 미체결 주문이 남아 있으면 상태 동기화 수행
            else:
                st_idle = context.ledger.get_state(TARGET_SYMBOL)
                if str(st_idle.get("sell_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_sell_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )
                if str(st_idle.get("buy_order_id") or "").strip():
                    await _call_plugin_adaptive(
                        sync_pending_buy_order_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

            context.fast_shield_mode = _evaluate_fast_shield_mode(
                context=context,
                now_est=now_est,
                live_p=safe_float(
                    context.latest_prices.get(TARGET_SYMBOL), 0.0
                ),
            )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("메인 지층 스나이퍼 루프 예외: %s", exc, exc_info=True)

        loop_wait = (
            FAST_SHIELD_LOOP_INTERVAL_SEC
            if context.fast_shield_mode
            else NORMAL_LOOP_INTERVAL_SEC
        )
        context.wakeup_event.clear()
        try:
            await asyncio.wait_for(
                context.wakeup_event.wait(), timeout=loop_wait
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 5] 백그라운드 태스크 3 : 17:00 EST 일일 GC 및 논리 거래일 롤오버 루프
# ---------------------------------------------------------------------------
async def daily_gc_and_rollover_loop(context: SniperAppContext) -> None:
    """
    매일 17:00 EST 에 가비지 컬렉션(GC)을 수행하고 새 거래일 진입 시 일일 카운터를 롤오버한다.
    """
    last_gc_date = ""
    while not context.shutdown_event.is_set():
        now_est = datetime.now(EST_TZ)
        today_str = now_est.strftime("%Y-%m-%d")
        hm = now_est.hour * 100 + now_est.minute

        try:
            if hasattr(context.ledger, "rollover_daily_counters_if_needed"):
                await _call_plugin_adaptive(
                    context.ledger.rollover_daily_counters_if_needed,
                    logical_date=today_str,
                    today_str=today_str,
                )
            if hm >= 1700 and last_gc_date != today_str:
                last_gc_date = today_str
                await execute_daily_1700_gc_and_backup_plugin(
                    context=context, now_est=now_est
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("일일 GC/롤오버 루프 예외: %s", exc)

        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=30.0
            )
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# [핵심 6] 시스템 부트스트랩 · 프리플라이트 TDD · 시그널 핸들러 (Case 11, 16)
# ---------------------------------------------------------------------------
async def async_main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__)) or "."

    # 1) 구버전 폐기 파일(daymarket_sim_plugin.py, whipsaw_plugin.py, state_SOXS.json) 자동 청소
    purged = purge_deprecated_legacy_files(base_dir)
    if purged:
        logger.info("🧹 [구버전 폐기 파일 청소 완료] 삭제 목록: %s", purged)

    # 2) 9대 코어 모듈 py_compile + AST 심볼 임포트 정적 검증 & QuantEngine TDD 실행
    ok_compile, err_msg = verify_all_modules_compile(base_dir)
    if not ok_compile:
        logger.critical(
            "🚨 [프리플라이트 컴파일/심볼 검증 실패] 기동 중단: %s", err_msg
        )
        sys.exit(1)

    run_quant_engine_tdd()
    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 전술 A (8%% 황금정량 · 4단 와이드 활주로 · 눌림/돌파 독립 1층 재장전 · Case 59/60/65/66) 검증 완료"
    )

    context = SniperAppContext(base_dir=base_dir)

    # 3) SIGTERM / SIGINT 0.1초 고속 종료 시그널 결속 (Case 11)
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        logger.info("🛑 종료 시그널(SIGTERM/SIGINT) 수신 -> 고속 안전 종료 개시...")
        context.shutdown_event.set()
        context.wakeup_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    # 4) 제2헌법: 토스증권 AccountSeq 락온 및 초기 실잔고 동기화
    try:
        acc_seq = await context.api.ensure_account_seq()
        logger.info("🏛️ [제2헌법 AccountSeq 락온 완료] accountSeq=%d", acc_seq)
    except Exception as exc:
        logger.warning(
            "⚠️ 초기 AccountSeq 조회 지연 (루프 내 자동 재시도): %s", exc
        )

    try:
        price_map = await context.api.get_current_prices_batch(
            (TARGET_SYMBOL,)
        )
        if safe_float(price_map.get(TARGET_SYMBOL), 0.0) > 0.0:
            context.latest_prices[TARGET_SYMBOL] = safe_float(
                price_map.get(TARGET_SYMBOL), 0.0
            )
        holdings_map, fetch_ok = (
            await context.api.get_holdings_map_with_status()
        )
        context.holdings_fetch_ok = fetch_ok
        if fetch_ok:
            context.latest_holdings = holdings_map
            await reconcile_stratum_with_broker_holdings_plugin(
                context=context,
                holdings=holdings_map,
                prices=context.latest_prices,
                now_est=datetime.now(EST_TZ),
                force_sync=True,
            )
    except Exception as exc:
        logger.warning("초기 잔고/시세 동기화 폴백: %s", exc)

    # 5) Case 16: 기동 완료 후 텔레그램 생존 신호 타전 ("✅ 서버가 정상 작동 중입니다.")
    await context.tg.send_startup_alive_banner()

    # 6) 5대 백그라운드 비동기 워커 가동
    tasks = [
        asyncio.create_task(candle_radar_loop(context), name="candle_radar"),
        asyncio.create_task(main_sniper_loop(context), name="main_sniper"),
        asyncio.create_task(
            context.tg.run_polling_loop(), name="telegram_polling"
        ),
        asyncio.create_task(
            daily_gc_and_rollover_loop(context), name="daily_gc"
        ),
        asyncio.create_task(
            _run_auto_update_loop_dynamic(context, interval_sec=3600.0),
            name="auto_updater",
        ),
    ]

    logger.info(
        "🚀 [SOXL 지층관리 스나이퍼 가동 완료] 04:07 ~ 16:00 EST · 전술 A (🛡️ 05_AF_WIDE_10_01) · 8%% 황금정량 · 10분 4단 와이드 매수게이트 · 1분 상시 매도 · 양방향 60초 타임쉴드 & 선행 체결정산(Case 65/66)"
    )

    try:
        await context.shutdown_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await context.api.close()
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
