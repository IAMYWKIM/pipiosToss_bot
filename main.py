# -*- coding: utf-8 -*-
"""
# MODIFIED: main.py
인피니트 스노우볼 (Infinite Snowball) v3.0 — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
메인 오케스트레이터 · TossApiClient 폴리필(close 포함) · 내장형 텔레그램 롱폴링/관제탑 컨트롤러 통합 코어 모듈 (Step 05 / 05)
- 전술 A 챔피언 (🛡️ 05_AF_WIDE_10_01) 100% 결속:
  1) 8%(12.5분할) 황금비율 정액 기반 정량제 & min(1회분 예산 상한 주수, 기준가 고정 정량) 이중 캡핑(Case 67) &
     자투리 절약금 · 세후 실현수익금 실시간 시드 풀 복리 산입 (COMP_08PCT 고정)
  2) 0주 상태 60초 타임쉴드 확인 후 실시간 현재가 조회 및 현재가 지정가(LIMIT) 실물 1층(REENTRY_1FL) 산입
  3) 2층(MERGED_UPPER) 단독 보유 시 10분 게이트 눌림목(-1.00%) OR 상승돌파(+1.00%) 하이브리드 독립 실물 1층(REENTRY_1FL) 재장전
     (상승돌파 시 Case 67 이중 캡핑으로 1회분 8% 시드 예산 초과 원천 차단!)
  4) 기억된 사이클 최저 매수가(cycle_min_buy_price) 잠금 기반 10분 고정 4단 와이드 활주로 시드보호 매수 게이트
     (WIDE_RUNWAY : 1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5% 신저가 도달 시에만 현재가 지정가 추매)
  5) 매수 후 60초 타임쉴드 & 1분 상시 순수 현재가 지정가 스나이퍼 매도(1층 +1.50% 익절 & 2층 리앵커링 / 2층 +1.00% 전량 졸업)
  6) 🚨 [Case 66 미결 매수 선행 정산] 04:07 ~ 16:00 EST 루프 진입 시 reconcile 호출 전에 sync_pending_buy_order_plugin(Step 3-0)을
     최우선 실행하여, 거래소 즉시 체결 시에도 텔레그램 체결 알림 · 자투리 절약금 누적 · 60초 타임쉴드 각인이 100% 실행되도록 보장!
  7) 📡 [Case 11 & Case 62 부팅 중 SIGTERM 0.1초 고속 종료 가드 & 텔레그램 적체 큐 무조건부 일괄 소각]
     부팅 초기화 도중 SIGTERM 수신 시 0.1초 내에 즉시 탈출하여 systemd stop-sigterm 타임아웃을 원천 차단하고,
     기동 시 적체 큐 일괄 소각 및 무변경(changed == False) 시 무중단 가동 유지를 통해 9초 무한 재기동 루프를 영구 종결!
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 67 전수 락온
"""

from __future__ import annotations

import asyncio
import html
import inspect
import json
import logging
import math
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import defense_plugin
import plugin_updater
import quant_engine
import quant_indicators_plugin
import sniper_exit_plugin
import sniper_plugin
import tg_ui_plugin
import toss_api
from defense_plugin import (
    execute_0400_premarket_stabilization_plugin,
    execute_1600_daily_closing_settlement_plugin,
    execute_daily_1700_gc_and_backup_plugin,
    reconcile_stratum_with_broker_holdings_plugin,
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
    DEFAULT_INITIAL_SEED_USD,
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

# URL("https://...") 오매칭을 100% 차단하는 엄격한 텔레그램 봇 토큰 정규식
_TG_TOKEN_STRICT_RE = re.compile(r"\b(\d{8,12}:[A-Za-z0-9_-]{25,60})\b")


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
# [핵심 1-B] TossApiClient 런타임 메서드 호환성 폴리필 어댑터 (close 포함 AttributeError 원천 소각)
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
        if TARGET_SYMBOL in raw_res and isinstance(
            raw_res[TARGET_SYMBOL], dict
        ):
            out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                raw_res[TARGET_SYMBOL], TARGET_SYMBOL
            )
            return out_map, True

        if (
            str(raw_res.get("symbol") or "").upper().strip() == TARGET_SYMBOL
            and ("quantity" in raw_res or "qty" in raw_res)
        ):
            out_map[TARGET_SYMBOL] = _normalize_single_holding_item(
                raw_res, TARGET_SYMBOL
            )
            return out_map, True

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
    for req_name in (
        "_request",
        "request",
        "_api_request",
        "_call_api",
        "_fetch_json",
    ):
        req_fn = getattr(api, req_name, None)
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


def _extract_candles_list(
    raw_res: Any,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
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
    서버의 TossApiClient 에 get_holdings_map_with_status, get_candles_1m, get_candles_1d, close 등의
    메서드가 없을 경우 자동으로 폴리필을 장착하여 런타임 및 종료 시점의 AttributeError 를 0건으로 차단한다.
    """
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
                    except Exception:
                        pass

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
                    except Exception:
                        pass

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

    if not hasattr(TossApiClient, "get_buying_power_usd"):

        async def _poly_get_buying_power_usd(
            self: Any, safety_buffer_pct: float = 0.2
        ) -> float:
            for cand_name in (
                "get_buying_power",
                "fetch_buying_power_usd",
                "fetch_buying_power",
            ):
                fn = getattr(self, cand_name, None)
                if callable(fn):
                    try:
                        res = await _call_plugin_adaptive(
                            fn,
                            currency="USD",
                            safety_buffer_pct=safety_buffer_pct,
                        )
                        if isinstance(res, (int, float)):
                            return max(0.0, float(res))
                        if isinstance(res, dict):
                            r_obj = (
                                res.get("result")
                                if isinstance(res.get("result"), dict)
                                else res
                            )
                            return max(
                                0.0,
                                safe_float(r_obj.get("cashBuyingPower"), 0.0),
                            )
                    except Exception:
                        pass
            try:
                raw = await _invoke_raw_toss_api(
                    self,
                    method="GET",
                    path="/api/v1/buying-power",
                    params={"currency": "USD"},
                    require_account=True,
                )
                if isinstance(raw, dict):
                    r_obj = (
                        raw.get("result")
                        if isinstance(raw.get("result"), dict)
                        else raw
                    )
                    raw_bp = max(
                        0.0, safe_float(r_obj.get("cashBuyingPower"), 0.0)
                    )
                    mult = max(
                        0.90,
                        min(
                            1.0,
                            1.0 - safe_float(safety_buffer_pct, 0.0) / 100.0,
                        ),
                    )
                    return round(raw_bp * mult, 4)
            except Exception:
                pass
            return 0.0

        setattr(
            TossApiClient, "get_buying_power_usd", _poly_get_buying_power_usd
        )

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

    if not hasattr(TossApiClient, "close"):

        async def _poly_close(self: Any) -> None:
            for sess_attr in ("_session", "session", "_client", "client"):
                s_obj = getattr(self, sess_attr, None)
                if s_obj is not None and hasattr(s_obj, "close"):
                    try:
                        c_res = s_obj.close()
                        if inspect.isawaitable(c_res):
                            await c_res
                    except Exception:
                        pass
            return None

        setattr(TossApiClient, "close", _poly_close)


_install_toss_api_compatibility_patches()


def _instantiate_stratum_ledger(filepath: str) -> Any:
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


# ---------------------------------------------------------------------------
# [핵심 1-C] 자체 벡터화 1분봉/일봉 지표 산출 폴백 (VWAP · 5MA · HA양봉/음봉)
# ---------------------------------------------------------------------------
def _compute_vectorized_metrics_fallback(
    candles_1m: List[Dict[str, Any]],
    live_price: float,
    now_est: datetime,
    candles_1d: List[Dict[str, Any]],
) -> Dict[str, Any]:
    dt_now = now_est.astimezone(EST_TZ)
    today_str = dt_now.strftime("%Y-%m-%d")
    lp = safe_float(live_price, 0.0)

    out: Dict[str, Any] = {
        "symbol": TARGET_SYMBOL,
        "live_price": round(lp, 4),
        "ma5_amp_pct": 6.50,
        "rsi_14": 50.0,
        "ha_bull": True,
        "ha_badge": "🕯️HA양봉",
        "premarket_vwap": 0.0,
        "premarket_high": 0.0,
        "premarket_low": 0.0,
        "premarket_amp_pct": 0.0,
        "premarket_est_low": 0.0,
        "premarket_est_high": 0.0,
        "regular_vwap": 0.0,
        "regular_high": 0.0,
        "regular_low": 0.0,
        "regular_amp_pct": 0.0,
        "regular_est_low": 0.0,
        "regular_est_high": 0.0,
    }

    if isinstance(candles_1d, list) and len(candles_1d) >= 2:
        try:
            df_d = pd.DataFrame(candles_1d)
            if "highPrice" in df_d.columns and "lowPrice" in df_d.columns:
                hs = pd.to_numeric(df_d["highPrice"], errors="coerce")
                ls = pd.to_numeric(df_d["lowPrice"], errors="coerce")
                valid = (hs > 0) & (ls > 0) & (hs >= ls)
                hv = (
                    hs[valid].to_numpy(dtype=float)[-6:-1]
                    if valid.sum() > 5
                    else hs[valid].to_numpy(dtype=float)[-5:]
                )
                lv = (
                    ls[valid].to_numpy(dtype=float)[-6:-1]
                    if valid.sum() > 5
                    else ls[valid].to_numpy(dtype=float)[-5:]
                )
                if len(lv) >= 1:
                    out["ma5_amp_pct"] = round(
                        float(
                            np.mean(((hv - lv) / np.maximum(0.01, lv)) * 100.0)
                        ),
                        2,
                    )
        except Exception:
            pass

    if not isinstance(candles_1m, list) or not candles_1m:
        return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)

    try:
        df = pd.DataFrame(candles_1m)
        if df.empty or "timestamp" not in df.columns:
            return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)

        ts_utc = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.loc[ts_utc.notna()].copy()
        df["dt_est"] = ts_utc[ts_utc.notna()].dt.tz_convert(EST_TZ)
        df.sort_values("dt_est", ascending=True, inplace=True)
        df.drop_duplicates(subset=["dt_est"], keep="last", inplace=True)

        df["O"] = pd.to_numeric(
            df.get("openPrice", df.get("open")), errors="coerce"
        )
        df["H"] = pd.to_numeric(
            df.get("highPrice", df.get("high")), errors="coerce"
        )
        df["L"] = pd.to_numeric(
            df.get("lowPrice", df.get("low")), errors="coerce"
        )
        df["C"] = pd.to_numeric(
            df.get("closePrice", df.get("close")), errors="coerce"
        )
        df["V"] = pd.to_numeric(
            df.get("volume", 0.0), errors="coerce"
        ).fillna(0.0)
        df.dropna(subset=["O", "H", "L", "C"], inplace=True)
        if df.empty:
            return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)

        if lp <= 0.0:
            lp = float(df["C"].iloc[-1])
            out["live_price"] = round(lp, 4)

        opens = df["O"].to_numpy(dtype=float)
        highs = df["H"].to_numpy(dtype=float)
        lows = df["L"].to_numpy(dtype=float)
        closes = df["C"].to_numpy(dtype=float)
        if lp > 0.0 and len(closes) > 0:
            closes[-1] = lp
            highs[-1] = max(highs[-1], lp)
            lows[-1] = min(lows[-1], lp)

        ha_c = (opens + highs + lows + closes) * 0.25
        shifted = np.empty(len(ha_c), dtype=float)
        shifted[0] = (opens[0] + closes[0]) * 0.5
        if len(ha_c) > 1:
            shifted[1:] = ha_c[:-1]
        ha_o = (
            pd.Series(shifted)
            .ewm(alpha=0.5, adjust=False)
            .mean()
            .to_numpy(dtype=float)
        )
        is_bull = bool(ha_c[-1] >= ha_o[-1])
        out["ha_bull"] = is_bull
        out["ha_badge"] = "🕯️HA양봉" if is_bull else "🌑HA음봉"
        out["prev_ha_open"] = round(
            float(ha_o[-2] if len(ha_o) >= 2 else ha_o[-1]), 4
        )
        out["prev_ha_close"] = round(
            float(ha_c[-2] if len(ha_c) >= 2 else ha_c[-1]), 4
        )
        out["cur_bar_open"] = round(float(opens[-1]), 4)
        out["cur_bar_high"] = round(float(highs[-1]), 4)
        out["cur_bar_low"] = round(float(lows[-1]), 4)

        df["Date"] = df["dt_est"].dt.strftime("%Y-%m-%d")
        df["hm"] = df["dt_est"].dt.hour * 100 + df["dt_est"].dt.minute
        t_date = (
            today_str
            if (df["Date"] == today_str).any()
            else str(df["Date"].iloc[-1])
        )
        df_t = df[df["Date"] == t_date]

        amp_ratio = max(0.01, float(out["ma5_amp_pct"]) / 100.0)
        for prefix, sub in (
            ("premarket", df_t[(df_t["hm"] >= 400) & (df_t["hm"] <= 929)]),
            ("regular", df_t[(df_t["hm"] >= 930) & (df_t["hm"] <= 1600)]),
        ):
            if not sub.empty:
                h_a = sub["H"].to_numpy(dtype=float)
                l_a = sub["L"].to_numpy(dtype=float)
                c_a = sub["C"].to_numpy(dtype=float)
                v_a = np.maximum(0.0, sub["V"].to_numpy(dtype=float))
                tp = (h_a + l_a + c_a) / 3.0
                tv = float(np.sum(v_a))
                vwap_v = (
                    float(np.sum(tp * v_a) / tv)
                    if tv > 0
                    else float(np.mean(tp))
                )
                sh = float(np.max(h_a))
                sl = float(np.min(l_a))
                out[f"{prefix}_vwap"] = round(vwap_v, 4)
                out[f"{prefix}_high"] = round(sh, 4)
                out[f"{prefix}_low"] = round(sl, 4)
                out[f"{prefix}_amp_pct"] = round(
                    ((sh - sl) / sl) * 100.0 if sl > 0 else 0.0, 2
                )
                out[f"{prefix}_est_high"] = round(
                    max(sh, sl * (1.0 + amp_ratio)), 2
                )
                out[f"{prefix}_est_low"] = round(
                    min(sl, sh / (1.0 + amp_ratio)), 2
                )
    except Exception:
        pass

    return refresh_metrics_with_live_tick(out, lp, now_est=dt_now)


async def _compute_candle_metrics_dynamic(
    context: Any,
    candles_1m: List[Dict[str, Any]],
    live_price: float,
    now_est: datetime,
    candles_1d: List[Dict[str, Any]],
) -> Dict[str, Any]:
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
            if isinstance(res, dict) and (
                safe_float(res.get("premarket_vwap"), 0.0) > 0.0
                or safe_float(res.get("regular_vwap"), 0.0) > 0.0
            ):
                return refresh_metrics_with_live_tick(
                    res, live_price, now_est=now_est
                )
        except Exception:
            pass

    return _compute_vectorized_metrics_fallback(
        candles_1m=candles_1m,
        live_price=live_price,
        now_est=now_est,
        candles_1d=candles_1d,
    )


# ---------------------------------------------------------------------------
# [핵심 1-D] 정밀 텔레그램 토큰·채팅방 ID 자동 탐색기 (URL 오매칭 100% 차단)
# ---------------------------------------------------------------------------
def _extract_valid_tg_token(text: str) -> str:
    if not text or "://" in text:
        return ""
    m = _TG_TOKEN_STRICT_RE.search(str(text).strip())
    return m.group(1) if m else ""


def _discover_telegram_credentials(
    base_dir: str, api_obj: Any = None
) -> Tuple[str, str]:
    """
    1) TOKEN_URL("https://...") 등 URL 상수가 봇 토큰으로 오매칭되는 버그를 100% 차단하고
       오직 _TG_TOKEN_STRICT_RE(\\d{8,12}:[A-Za-z0-9_-]{25,60}) 에 일치하는 토큰만 추출한다.
    2) .env, os.environ, api_obj 속성, tg_ui_plugin / toss_api 모듈 변수 및 로컬 파일에서
       (bot_token, chat_id) 를 탐색한다.
    """
    candidate_dirs = [base_dir, "/home/pipiosToss", os.getcwd()]
    for d in candidate_dirs:
        if not d or not os.path.isdir(d):
            continue
        env_fp = os.path.join(d, ".env")
        if os.path.isfile(env_fp):
            try:
                with open(env_fp, "r", encoding="utf-8") as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        if line.startswith("export "):
                            line = line[len("export ") :].strip()
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'").strip('"')
                        if k and v and k not in os.environ:
                            os.environ[k] = v
            except Exception:
                pass

    token_keys = (
        "TELEGRAM_BOT_TOKEN",
        "TG_BOT_TOKEN",
        "BOT_TOKEN",
        "TELEGRAM_TOKEN",
        "TG_TOKEN",
    )
    chat_keys = (
        "TELEGRAM_CHAT_ID",
        "TG_CHAT_ID",
        "CHAT_ID",
        "TELEGRAM_ADMIN_ID",
        "ADMIN_CHAT_ID",
        "ALLOWED_CHAT_ID",
    )

    bot_token = ""
    chat_id = ""

    # 1) 환경변수 우선 탐색
    for k in token_keys:
        cand = _extract_valid_tg_token(str(os.environ.get(k) or ""))
        if cand:
            bot_token = cand
            break

    if not bot_token:
        for _, v in os.environ.items():
            cand = _extract_valid_tg_token(str(v or ""))
            if cand:
                bot_token = cand
                break

    for k in chat_keys:
        v = str(os.environ.get(k) or "").strip()
        if v.lstrip("-").isdigit() and len(v.lstrip("-")) >= 5:
            chat_id = v
            break

    # 2) api_obj 인스턴스 및 tg_ui_plugin / toss_api 모듈 전역 변수 탐색
    scan_targets: List[Any] = [tg_ui_plugin, toss_api]
    if api_obj is not None:
        scan_targets.insert(0, api_obj)

    for target in scan_targets:
        try:
            obj_dict = vars(target)
        except Exception:
            continue
        for attr_name, attr_val in obj_dict.items():
            if isinstance(attr_val, (str, int)):
                s_val = str(attr_val).strip()
                u_name = attr_name.upper()
                if not bot_token:
                    cand = _extract_valid_tg_token(s_val)
                    if cand:
                        bot_token = cand
                if not chat_id and (
                    "CHAT_ID" in u_name or "ADMIN_ID" in u_name
                ):
                    if (
                        s_val.lstrip("-").isdigit()
                        and len(s_val.lstrip("-")) >= 5
                    ):
                        chat_id = s_val

    # 3) 모듈 소스 파일(.py / .json / .env) 텍스트 정규식 스캔 폴백
    if not bot_token or not chat_id:
        chat_re = re.compile(
            r"(?:CHAT_ID|chat_id)\s*[:=]\s*['\"]?(-?\d{6,15})['\"]?"
        )
        for d in candidate_dirs:
            if not d or not os.path.isdir(d):
                continue
            for fn in (
                ".env",
                "config.json",
                "secrets.json",
                "toss_api.py",
                "tg_ui_plugin.py",
            ):
                fp = os.path.join(d, fn)
                if not os.path.isfile(fp):
                    continue
                try:
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    if not bot_token:
                        m_tok = _TG_TOKEN_STRICT_RE.search(content)
                        if m_tok:
                            bot_token = m_tok.group(1)
                    if not chat_id:
                        m_cid = chat_re.search(content)
                        if m_cid:
                            chat_id = m_cid.group(1)
                except Exception:
                    pass

    return bot_token, chat_id


def _escape_html(val: Any) -> str:
    return html.escape(str(val if val is not None else ""), quote=False)


def _get_dst_badge(now_est: Optional[datetime] = None) -> str:
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    tz_name = dt.tzname() or "EST"
    if "EDT" in tz_name.upper():
        return "☀️서머타임 ON (EDT)"
    return "❄️서머타임 OFF (EST)"


def _format_case67_quota_badge(summary: Dict[str, Any]) -> str:
    """
    NEW [Case 67]:
    기준가 고정 정량(anchor_quota_qty)과 현재가 기준 1회분 예산 상한 주수(budget_cap_qty)의
    min(budget_cap_qty, anchor_quota_qty) 캡핑 상태를 직관적인 문자열로 반환한다.
    """
    next_qty = safe_int(summary.get("next_target_qty"), 1)
    anc_qty = safe_int(summary.get("anchor_quota_qty"), next_qty)
    cap_qty = safe_int(summary.get("budget_cap_qty"), next_qty)
    if next_qty < anc_qty:
        return f"<b>{next_qty}주</b> (🛡️min(1회분 {cap_qty}주, 고정 {anc_qty}주) 예산초과 차단)"
    return f"<b>{next_qty}주</b> (min(1회분, 고정 {anc_qty}주) 대기)"


def _build_main_reply_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [
                {"text": "🔫 스나이퍼 레이더"},
                {"text": "📜 지층 지시서"},
                {"text": "⬅️ 메인 메뉴"},
            ],
            [
                {"text": "⚙️ 전술 제어반"},
                {"text": "🔄 잔고 동기화"},
                {"text": "🚀 시스템 업데이트"},
            ],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def _build_radar_inline_keyboard() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "🔄 레이더 갱신", "callback_data": "cmd_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "cmd_sync"},
            ],
            [
                {"text": "⚙️ 전술 제어반", "callback_data": "cmd_settlement"},
                {"text": "⬅️ 메인 메뉴", "callback_data": "cmd_start"},
            ],
        ]
    }


def _build_settlement_inline_keyboard(is_active: bool) -> Dict[str, Any]:
    """
    🚨 Case 64:
    전술 제어반(/settlement)에는 오직 [작동 ON/OFF] 와 [시드머니 설정] 2가지 핵심 조작 버튼만 둔다.
    """
    power_label = (
        "🔴 스나이퍼 작동 OFF 로 전환"
        if is_active
        else "🟢 스나이퍼 작동 ON 으로 전환"
    )
    return {
        "inline_keyboard": [
            [
                {"text": power_label, "callback_data": "toggle_bot_active"},
            ],
            [
                {
                    "text": "💵 기준 시드머니($) 설정",
                    "callback_data": "prompt_seed_input",
                },
            ],
            [
                {"text": "🔫 스나이퍼 레이더", "callback_data": "cmd_avwap"},
                {"text": "📜 지층 지시서", "callback_data": "cmd_sync"},
            ],
        ]
    }


class _AdaptiveTelegramBridge:
    """
    외부 tg_ui_plugin 의 폴링 루프 구조에 구애받지 않고
    자체 내장 getUpdates 롱폴링 + /start, /avwap, /sync, /settlement, /reset, /update +
    인라인 버튼 콜백 + 시드 숫자 입력을 100% 직접 수행하는 완결형 텔레그램 컨트롤러.
    - MODIFIED:
      1) 기동 직후 _flush_stale_updates_on_startup() 에서 조건 없이 큐의 마지막 update_id 를
         즉시 소비 확정(Ack)하여 과거 적체된 /start, /update 를 100% 소멸시킨다.
      2) /update 실행 시 오프셋 선행 확정 후, 신규 커밋(changed == True)이 있을 때만 재기동(os._exit(0))하고
         변경 사항이 없을 때(changed == False)는 무중단 가동을 유지하여 9초 주기 무한 재기동 루프를 영구 차단한다.
      3) NEW [Case 67]: /avwap 레이더 및 /settlement 제어반에서 min(1회분 예산 상한 주수, 기준가 고정 정량)
         캡핑 상태를 실시간 표출한다.
    """

    def __init__(self, context: Any) -> None:
        self.context: Any = context
        self.bot_token, self.chat_id = _discover_telegram_credentials(
            context.base_dir, getattr(context, "api", None)
        )
        self._boot_epoch: float = time.time()
        self._last_update_id: int = 0
        try:
            st_init = self.context.ledger.get_state(TARGET_SYMBOL)
            self._last_update_id = max(
                0, safe_int(st_init.get("last_tg_update_id"), 0)
            )
        except Exception:
            self._last_update_id = 0
        self._awaiting_seed_input_chats: Dict[str, float] = {}

    def _api_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/{method}"

    def _post_tg_sync(
        self,
        method: str,
        payload: Dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> Dict[str, Any]:
        if not self.bot_token:
            return {"ok": False, "description": "missing-bot-token"}
        url = self._api_url(method)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw) if raw else {"ok": True}
        except urllib.error.HTTPError as he:
            try:
                err_raw = he.read().decode("utf-8", errors="replace")
                return json.loads(err_raw)
            except Exception:
                return {"ok": False, "error_code": he.code}
        except Exception as exc:
            return {"ok": False, "description": str(exc)}

    async def _call_tg(
        self,
        method: str,
        payload: Dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> Dict[str, Any]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._post_tg_sync, method, payload, timeout_sec
                ),
                timeout=timeout_sec + 3.0,
            )
        except Exception as exc:
            logger.debug("텔레그램 API(%s) 호출 예외: %s", method, exc)
            return {"ok": False, "description": str(exc)}

    async def _ack_telegram_offset(self, target_update_id: int) -> None:
        """
        NEW: 텔레그램 본사 서버 큐에서 target_update_id 이하의 모든 메시지를 즉시 소비 확정(Ack)하고
        로컬 원장(last_tg_update_id)에 영속화하여 프로세스 재기동 후 동일 명령이 재실행되지 않게 한다.
        """
        uid = max(self._last_update_id, safe_int(target_update_id, 0))
        if uid <= 0:
            return
        self._last_update_id = uid
        try:
            await self.context.ledger.update_state(
                TARGET_SYMBOL, last_tg_update_id=uid
            )
        except Exception:
            pass
        try:
            await self._call_tg(
                "getUpdates",
                {"offset": uid + 1, "limit": 1, "timeout": 0},
                timeout_sec=5.0,
            )
        except Exception:
            pass

    async def _flush_stale_updates_on_startup(self) -> None:
        """
        MODIFIED: 기동 직후 텔레그램 큐의 가장 마지막 항목(offset=-1)을 조회하여,
        메시지 종류나 발송 시각과 무관하게 부팅 전에 쌓여 있던 모든 적체 이벤트를 조건 없이
        즉시 소각(Ack)한다.
        """
        try:
            res = await self._call_tg(
                "getUpdates",
                {"offset": -1, "timeout": 0},
                timeout_sec=6.0,
            )
            if not (isinstance(res, dict) and bool(res.get("ok"))):
                return
            updates = res.get("result")
            if not isinstance(updates, list) or not updates:
                return

            max_stale_uid = 0
            for upd in updates:
                if isinstance(upd, dict):
                    uid = safe_int(upd.get("update_id"), 0)
                    if uid > max_stale_uid:
                        max_stale_uid = uid

            if max_stale_uid > 0:
                await self._ack_telegram_offset(max_stale_uid)
                logger.info(
                    "🧹 [텔레그램 부팅 전 적체 큐 전량 소각 완료] last_update_id=%d",
                    max_stale_uid,
                )
        except Exception as exc:
            logger.debug("부팅 시 텔레그램 큐 플러시 스킵: %s", exc)

    async def send_message(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML",
    ) -> bool:
        target_chat = str(chat_id or self.chat_id or "").strip()
        if self.bot_token and target_chat:
            payload: Dict[str, Any] = {
                "chat_id": target_chat,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            res = await self._call_tg("sendMessage", payload, timeout_sec=10.0)
            if not bool(res.get("ok")):
                payload.pop("parse_mode", None)
                res = await self._call_tg(
                    "sendMessage", payload, timeout_sec=10.0
                )
            if bool(res.get("ok")):
                return True

        fn = _resolve_module_attr(
            modules=(tg_ui_plugin,),
            candidate_names=(
                "send_telegram_message",
                "send_slim_alert",
                "send_telegram_alert",
            ),
        )
        if callable(fn):
            res_f = await _call_plugin_adaptive(
                fn,
                context=self.context,
                title=text,
                body="",
                text=text,
                message=text,
                reply_markup=reply_markup,
            )
            return bool(res_f)
        return False

    async def edit_or_send_message(
        self,
        chat_id: str,
        message_id: Optional[int],
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if self.bot_token and chat_id and message_id and message_id > 0:
            payload: Dict[str, Any] = {
                "chat_id": str(chat_id),
                "message_id": int(message_id),
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            res = await self._call_tg(
                "editMessageText", payload, timeout_sec=10.0
            )
            if bool(res.get("ok")) or "message is not modified" in str(
                res.get("description") or ""
            ):
                return True
        return await self.send_message(
            text=text, chat_id=chat_id, reply_markup=reply_markup
        )

    async def send_slim_alert(
        self,
        title: str,
        body: str = "",
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        safe_title = _escape_html(title)
        safe_body = _escape_html(body)
        msg = (
            f"<b>{safe_title}</b>\n🔸 {safe_body}"
            if safe_body
            else f"<b>{safe_title}</b>"
        )
        if self.bot_token and self.chat_id:
            return await self.send_message(
                text=msg, chat_id=self.chat_id, reply_markup=reply_markup
            )

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
                text=msg,
                message=msg,
            )
            return bool(res)
        return False

    async def send_startup_alive_banner(self) -> None:
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        init_seed = safe_float(
            st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD
        )
        free_cash = safe_float(st.get("free_cash_usd"), init_seed)
        text = (
            "<b>✅ 서버가 정상 작동 중입니다.</b>\n"
            "🔸 🐂 [인피니트 스노우볼 v3.0 · 🛡️ 05_AF_WIDE_10_01] 양방향 60초 타임쉴드 &amp; 선행 체결정산 &amp; min(1회분,고정정량) 캡핑(Case 65/66/67) 가동 완료\n"
            f"🔸 기준시드: <b>${init_seed:,.2f}</b> · 가용현금: <b>${free_cash:,.2f}</b> (📡 텔레그램 명령어 폴링 ON)"
        )
        sent = await self.send_message(
            text=text, reply_markup=_build_main_reply_keyboard()
        )
        if not sent:
            await self.send_slim_alert(
                title="✅ 서버가 정상 작동 중입니다.",
                body="🐂 [인피니트 스노우볼 v3.0 · 🛡️ 05_AF_WIDE_10_01] 양방향 60초 타임쉴드 & 선행 체결정산 & min(1회분,고정정량) 캡핑(Case 65/66/67) 가동 완료",
            )

    # -----------------------------------------------------------------------
    # 4대 관제 보드 메시지 빌더 (/start · /avwap · /sync · /settlement)
    # -----------------------------------------------------------------------
    def _build_start_schedule_message(
        self, st: Dict[str, Any], now_est: datetime
    ) -> str:
        dst_str = _get_dst_badge(now_est)
        init_seed = max(
            100.0,
            safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD),
        )
        lines = [
            f"🐂 <b>[ SOXL 지층관리 스나이퍼 ({_escape_html(dst_str)}) ]</b>",
            "▫️ 전략: 🛡️ 05_AF_WIDE_10_01 (8% 황금정량 · 눌림/돌파 독립 1층 재장전 · 기억저가 와이드 활주로)",
            "▫️ 정량: 📐 min(1회분 8% 예산 상한 주수, 기준가 고정 정량) 이중 캡핑으로 돌파 시 예산 초과 차단 (Case 67)",
            "▫️ 매수: 🛡️ 10분 고정 4단 와이드 게이트 (기억저가 대비 1~4회 -1.0% / 5~8회 -2.0% / 9~10회 -3.5% / 11회+ -5.5%)",
            "▫️ 재장전: 🔥 0주 즉시 진입 · 2층 단독 시 10분 게이트 눌림(-1.0%) OR 돌파(+1.0%) 독립 1층 산입",
            "▫️ 매도: ⚡ 1분 상시 실물 스나이퍼 매도 (1층 +1.50% 익절&amp;리앵커링 / 2층 +1.00% 졸업)",
            "▫️ 방어: ⏱️ 양방향 60초 타임쉴드 &amp; 순수 현재가 지정가(LIMIT) 락온 (Case 65/66/67)",
            f"▫️ 기준 시드머니: <b>${init_seed:,.2f}</b> (1회 8% 정량 자동 산출)",
            "──────────────",
            "🔹 04:00 ~ 04:06: 🌅 프리장 호가 안정화 &amp; 오버나이트 지층 동기화",
            "🔹 04:07 ~ 09:29: 🔫 프리장 지층관리 가동 (10분 와이드매수게이트 · 1분 상시매도)",
            "🔹 09:30 ~ 16:00: 🔥 정규장 지층관리 가동 (10분 와이드매수게이트 · 1분 상시매도)",
            "🔹 16:00 ~ 16:59: 🌙 정규장 마감 일일 결산 &amp; 보유 지층 오버나이트 보존",
            "🔹 17:00 ~ 03:59: 🧹 일일 GC 및 장외 대기 (강제 손절 MOC 없음)",
            "──────────────",
            "📡 /avwap : 실시간 지층 스나이퍼 레이더 관제탑",
            "📜 /sync : 토스 실잔고 동기화 &amp; 지층 지시서",
            "⚙️ /settlement : 초슬림 전술 제어반 (작동 ON/OFF · 시드머니 설정)",
            "🧹 /reset : 지층 원장 0점 초기화 (/reset_seed 금액)",
            "🚀 /update : 최신 깃허브 코드 검증 업데이트",
        ]
        return "\n".join(lines)

    def _build_avwap_radar_message(
        self,
        st: Dict[str, Any],
        metrics: Dict[str, Any],
        live_price: float,
        now_est: datetime,
    ) -> str:
        met = refresh_metrics_with_live_tick(
            metrics, live_price, now_est=now_est
        )
        lp = safe_float(
            live_price,
            safe_float(
                met.get("live_price"), safe_float(st.get("anchor_price"), 0.0)
            ),
        )
        summary = summarize_stratum_portfolio(st, live_price=lp)
        is_active = bool(st.get("is_active", True))
        active_badge = "🟢 ON" if is_active else "🔴 OFF"
        fast_badge = (
            "⚡0.4s 부스터"
            if getattr(self.context, "fast_shield_mode", False)
            else "⏱️1.5s 표준"
        )

        ma5_amp = safe_float(met.get("ma5_amp_pct"), 6.50)
        ha_badge = str(met.get("ha_badge") or "🕯️HA양봉")

        pre_vwap = safe_float(met.get("premarket_vwap"), 0.0)
        pre_l = safe_float(met.get("premarket_low"), 0.0)
        pre_h = safe_float(met.get("premarket_high"), 0.0)
        pre_amp = safe_float(met.get("premarket_amp_pct"), 0.0)
        pre_el = safe_float(met.get("premarket_est_low"), 0.0)
        pre_eh = safe_float(met.get("premarket_est_high"), 0.0)

        reg_vwap = safe_float(met.get("regular_vwap"), 0.0)
        reg_l = safe_float(met.get("regular_low"), 0.0)
        reg_h = safe_float(met.get("regular_high"), 0.0)
        reg_amp = safe_float(met.get("regular_amp_pct"), 0.0)
        reg_el = safe_float(met.get("regular_est_low"), 0.0)
        reg_eh = safe_float(met.get("regular_est_high"), 0.0)

        held_sh = safe_int(summary.get("held_shares"), 0)
        held_lots = safe_int(summary.get("held_lots"), 0)
        buckets = summary.get("buckets", [])

        shield_line = ""
        fn_buy_sh = getattr(
            sniper_exit_plugin, "is_within_post_buy_time_shield", None
        )
        fn_sell_sh = getattr(
            sniper_exit_plugin, "is_within_post_sell_time_shield", None
        )
        if callable(fn_buy_sh):
            in_b, rem_b = fn_buy_sh(self.context, st, TARGET_SYMBOL)
            if in_b:
                shield_line = f"🛡️ <b>[매수 후 60초 타임쉴드 가동 중]</b> 잔여 <b>{rem_b:.1f}초</b> 후 현재가 조회 &amp; 매도 감시 개시"
        if not shield_line and callable(fn_sell_sh):
            in_s, rem_s = fn_sell_sh(self.context, st, TARGET_SYMBOL)
            if in_s:
                shield_line = f"🛡️ <b>[매도 후 60초 타임쉴드 가동 중]</b> 잔여 <b>{rem_s:.1f}초</b> 후 현재가 조회 &amp; 매수 감시 개시"

        stratum_lines: List[str] = []
        if held_sh <= 0 or not buckets:
            stratum_lines.append(
                "⚪ <b>[보유 지층 없음 · 0주]</b> 60초 타임쉴드 확인 후 현재가 지정가 1층(REENTRY_1FL) 즉시 산입 대기"
            )
        elif bool(summary.get("is_l2_only_anti_bloat")):
            b2 = buckets[0]
            l2_t = safe_float(summary.get("l2_target_price"), 0.0)
            l2_g = safe_float(summary.get("l2_gain_pct"), 0.0)
            g_sign = "+" if l2_g >= 0.0 else ""
            stratum_lines.append(
                "⚡ <b>[1층: 공석 · 10분 게이트 눌림(-1%)/돌파(+1%) 대기]</b>"
            )
            stratum_lines.append(
                f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> 장부 <b>${b2.book_price:.2f}</b>"
                f"(실평단 ${b2.actual_avg_price:.2f}) · 졸업가 <b>${l2_t:.2f}</b>(+1.0%) [{g_sign}{l2_g:.2f}%]"
            )
        else:
            b1 = buckets[0]
            l1_t = safe_float(summary.get("l1_target_price"), 0.0)
            l1_g = safe_float(summary.get("l1_gain_pct"), 0.0)
            g1_sign = "+" if l1_g >= 0.0 else ""
            stratum_lines.append(
                f"⚡ <b>[1층: 공격수 {b1.qty}주]</b> 단가 <b>${b1.book_price:.2f}</b> · "
                f"익절목표 <b>${l1_t:.2f}</b>(+1.5%) [{g1_sign}{l1_g:.2f}%]"
            )
            if len(buckets) >= 2:
                b2 = buckets[1]
                l2_t = safe_float(summary.get("l2_target_price"), 0.0)
                l2_g = safe_float(summary.get("l2_gain_pct"), 0.0)
                g2_sign = "+" if l2_g >= 0.0 else ""
                stratum_lines.append(
                    f"📦 <b>[2층: 통합층 {b2.qty}주({b2.lot_count}회차)]</b> 장부 <b>${b2.book_price:.2f}</b>"
                    f"(실평단 ${b2.actual_avg_price:.2f}) · 졸업가 <b>${l2_t:.2f}</b>(+1.0%) [{g2_sign}{l2_g:.2f}%]"
                )

        _, _, _, gate_info = evaluate_buy_gate_bucket_status(
            now_est=now_est,
            buy_interval_min=LOCKED_BUY_INTERVAL_MIN,
            last_evaluated_bucket_id=safe_int(
                st.get("last_evaluated_buy_bucket_id"), -1
            ),
            last_evaluated_date=str(st.get("last_evaluated_date") or ""),
        )
        next_gate_str = str(gate_info.get("next_gate_hm_str") or "04:07 EST")
        next_bud = safe_float(summary.get("next_portion_budget"), 800.0)
        quota_badge_str = _format_case67_quota_badge(summary)
        ref_anchor = safe_float(summary.get("ref_buy_anchor"), lp)
        chg_anchor = safe_float(summary.get("chg_from_buy_anchor_pct"), 0.0)
        c_sign = "+" if chg_anchor >= 0.0 else ""

        if bool(summary.get("is_l2_only_anti_bloat")):
            pull_p = safe_float(summary.get("reentry_pullback_price"), 0.0)
            brk_p = safe_float(summary.get("reentry_breakout_price"), 0.0)
            locked_min_p = safe_float(summary.get("cycle_min_buy_price"), 0.0)
            gate_cond_line = (
                f"▫️ 직전익절 ${ref_anchor:.2f} 대비 {c_sign}{chg_anchor:.2f}% "
                f"(눌림목 <b>${pull_p:.2f}</b> 이하 OR 돌파 <b>${brk_p:.2f}</b> 이상 · 기억저가 ${locked_min_p:.2f})"
            )
        else:
            drop_req = safe_float(summary.get("drop_buy_pct"), -1.0)
            trig_p = safe_float(summary.get("trigger_buy_price"), 0.0)
            gate_cond_line = (
                f"▫️ 기억저가 ${ref_anchor:.2f} 대비 현재 {c_sign}{chg_anchor:.2f}% "
                f"(와이드단차 {drop_req:.2f}% · 신저가 <b>${trig_p:.2f}</b> 이하)"
            )

        reanchor_cnt = safe_int(summary.get("reanchor_cnt"), 0)
        reanchor_drop = safe_float(
            summary.get("cumulative_reanchor_drop_usd"), 0.0
        )
        pass_cnt = safe_int(summary.get("seed_protect_pass_cnt"), 0)
        extra_13 = safe_int(summary.get("extra_13plus_buy_events"), 0)
        saved_u = safe_float(summary.get("cumulative_leftover_saved_usd"), 0.0)
        cum_net = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
        free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
        mark_eq = safe_float(summary.get("mark_equity_usd"), 0.0)
        mark_roi = safe_float(summary.get("mark_roi_pct"), 0.0)
        n_sign = "+" if cum_net >= 0.0 else "-"
        m_sign = "+" if mark_roi >= 0.0 else ""

        lines = [
            f"📡 <b>[SOXL 지층 스나이퍼 레이더]</b> ({active_badge} · {fast_badge})",
            f"🐂 <b>SOXL</b> [{active_badge}] 현재가 <b>${lp:.2f}</b> · 5MA {ma5_amp:.2f}% · {_escape_html(ha_badge)}",
            f"🌅 프리장 VWAP ${pre_vwap:.2f} · 밴드 ${pre_l:.2f} ~ ${pre_h:.2f} ({pre_amp:.1f}%) · 예상 ${pre_el:.2f} ~ ${pre_eh:.2f}",
            f"🔥 정규장 VWAP ${reg_vwap:.2f} · 밴드 ${reg_l:.2f} ~ ${reg_h:.2f} ({reg_amp:.1f}%) · 예상 ${reg_el:.2f} ~ ${reg_eh:.2f}",
        ]
        if shield_line:
            lines.append(shield_line)

        lines.extend(
            [
                "──────────────",
                f"🏛️ <b>[2-Tier 실물 지층 &amp; 와이드 활주로]</b> (총 {held_sh}주 · {held_lots}회차)",
                *stratum_lines,
                "──────────────",
                f"🛡️ <b>[10분 고정 와이드 시드보호 매수 게이트]</b> (차기검문: {_escape_html(next_gate_str)})",
                gate_cond_line,
                f"▫️ 차기 8% 정량: 예산 ${next_bud:,.2f} ➔ {quota_badge_str}",
                "──────────────",
                f"♻️ 평단 리앵커링: {reanchor_cnt}회 (누적 -${reanchor_drop:.2f} 단가인하)",
                f"🛡️ 시드보호 패스: {pass_cnt:,}회 · 🔥 13회차+ 확장매수: {extra_13}회",
                f"💰 자투리절약 누적: +${saved_u:,.2f} · 세후실현익: {n_sign}${abs(cum_net):,.2f}",
                f"💵 가용현금: <b>${free_cash:,.2f}</b> · 총자산: <b>${mark_eq:,.2f}</b> ({m_sign}{mark_roi:.2f}%)",
                f"🕒 갱신: {now_est.strftime('%m-%d %H:%M:%S EST')}",
            ]
        )
        return "\n".join(lines)

    def _build_sync_instruction_message(
        self,
        st: Dict[str, Any],
        holdings: Dict[str, Dict[str, Any]],
        live_price: float,
        broker_bp_usd: float,
        fx_rate: float,
        now_est: datetime,
    ) -> str:
        h_info = (holdings or {}).get(TARGET_SYMBOL, {})
        lp = safe_float(
            live_price,
            safe_float(
                h_info.get("last_price"),
                safe_float(st.get("anchor_price"), 0.0),
            ),
        )
        summary = summarize_stratum_portfolio(st, live_price=lp)
        eff_fx = fx_rate if fx_rate > 100.0 else KRW_PER_USD_DEFAULT

        init_seed = safe_float(summary.get("initial_seed_usd"), 10000.0)
        free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
        actual_inv = safe_float(summary.get("actual_invested_usd"), 0.0)
        cum_net = safe_float(summary.get("cumulative_realized_net_usd"), 0.0)
        cum_roi = safe_float(summary.get("realized_roi_pct"), 0.0)
        unreal_pnl = safe_float(summary.get("unrealized_pnl_usd"), 0.0)
        mark_eq = safe_float(summary.get("mark_equity_usd"), 0.0)
        mark_roi = safe_float(summary.get("mark_roi_pct"), 0.0)

        total_net_usd = cum_net + unreal_pnl
        total_net_krw = total_net_usd * eff_fx
        cum_net_krw = cum_net * eff_fx

        held_sh = safe_int(summary.get("held_shares"), 0)
        held_lots = safe_int(summary.get("held_lots"), 0)
        actual_avg = safe_float(summary.get("actual_avg_price"), 0.0)
        book_avg = safe_float(summary.get("book_avg_price"), 0.0)
        locked_min_p = safe_float(summary.get("cycle_min_buy_price"), 0.0)

        c_sign = "+" if cum_net >= 0.0 else "-"
        u_sign = "+" if unreal_pnl >= 0.0 else "-"
        t_sign = "+" if total_net_usd >= 0.0 else "-"

        lines = [
            "📜 <b>[SOXL 지층관리 스나이퍼 지시서 (실잔고 동기화)]</b>",
            f"💵 설정 기준시드: <b>${init_seed:,.2f}</b> (환율 {eff_fx:,.1f}원/$)",
            f"🏦 스나이퍼 가용현금: <b>${free_cash:,.2f}</b> (토스 실주문가능: ${broker_bp_usd:,.2f})",
            f"📦 보유 지층 합계: <b>{held_sh}주</b> ({held_lots}회차 · 실매수원금 ${actual_inv:,.2f})",
            f"📐 전체 실평단: <b>${actual_avg:.2f}</b> · 리앵커링 장부평단: <b>${book_avg:.2f}</b> (기억저가 ${locked_min_p:.2f})",
            "──────────────",
            f"💰 누적 세후실현익(보유원금 미차감): <b>{c_sign}${abs(cum_net):,.2f}</b> ({c_sign}{abs(cum_roi):.2f}% · {c_sign}{abs(cum_net_krw):,.0f}원)",
            f"📊 현재 지층 평가손익: <b>{u_sign}${abs(unreal_pnl):,.2f}</b> (현재가 ${lp:.2f})",
            f"👑 시가평가 총자산: <b>${mark_eq:,.2f}</b> ({t_sign}{abs(mark_roi):.2f}% · 총순익 {t_sign}{abs(total_net_krw):,.0f}원)",
            f"🕒 동기화 시각: {now_est.strftime('%Y-%m-%d %H:%M:%S EST')}",
        ]
        return "\n".join(lines)

    def _build_settlement_control_message(
        self, st: Dict[str, Any], live_price: float, now_est: datetime
    ) -> str:
        lp = safe_float(live_price, safe_float(st.get("anchor_price"), 100.0))
        summary = summarize_stratum_portfolio(st, live_price=lp)

        is_active = bool(st.get("is_active", True))
        power_str = "🟢 가동 중 (ON)" if is_active else "🔴 일시정지 (OFF)"
        init_seed = safe_float(summary.get("initial_seed_usd"), 10000.0)
        free_cash = safe_float(summary.get("free_cash_usd"), 0.0)
        real_eq = safe_float(summary.get("realized_equity_usd"), init_seed)
        next_bud = safe_float(
            summary.get("next_portion_budget"), init_seed * 0.08
        )
        quota_badge_str = _format_case67_quota_badge(summary)
        held_sh = safe_int(summary.get("held_shares"), 0)
        held_lots = safe_int(summary.get("held_lots"), 0)

        lines = [
            "⚙️ <b>[SOXL 인피니트 스노우볼 v3.0 전술 제어반]</b>",
            f"1️⃣ 마스터 전원: <b>{power_str}</b>",
            f"2️⃣ 기준 시드머니: <b>${init_seed:,.2f}</b> (실현기준 총자산 ${real_eq:,.2f})",
            "3️⃣ 운용 엔진: 🛡️ <b>8% 동적복리 · min(1회분,고정정량) 캡핑 · 10분 4단 와이드 · 1분 지정가매도 · 60초 타임쉴드</b> (고정)",
            f"4️⃣ 차기 1회(8%) 정량: 예산 <b>${next_bud:,.2f}</b> ➔ {quota_badge_str} (가용현금 ${free_cash:,.2f})",
            f"5️⃣ 현재 보유 지층: <b>{held_sh}주</b> ({held_lots}회차)",
            "──────────────",
            "💡 시드머니를 변경하시려면 아래 <b>[💵 기준 시드머니($) 설정]</b> 버튼을 누르거나 채팅창에 숫자(예: <code>19000</code>)를 바로 입력하세요.",
            f"🕒 기준 시각: {now_est.strftime('%m-%d %H:%M:%S EST')}",
        ]
        return "\n".join(lines)

    # -----------------------------------------------------------------------
    # 실시간 시세/잔고 갱신 및 명령어/콜백 핸들러
    # -----------------------------------------------------------------------
    async def _refresh_live_price_and_metrics(
        self,
    ) -> Tuple[float, Dict[str, Any]]:
        now_est = datetime.now(EST_TZ)
        live_p = safe_float(
            self.context.latest_prices.get(TARGET_SYMBOL), 0.0
        )
        try:
            p_map = await self.context.api.get_current_prices_batch(
                (TARGET_SYMBOL,)
            )
            fetched_p = safe_float(p_map.get(TARGET_SYMBOL), 0.0)
            if fetched_p > 0.0:
                live_p = fetched_p
                self.context.latest_prices[TARGET_SYMBOL] = live_p
        except Exception:
            pass

        base_met = self.context.candle_metrics_cache.get(TARGET_SYMBOL, {})
        met = refresh_metrics_with_live_tick(
            base_met, live_p, now_est=now_est
        )
        self.context.candle_metrics_cache[TARGET_SYMBOL] = met
        if live_p <= 0.0:
            live_p = safe_float(met.get("live_price"), 0.0)
        return live_p, met

    async def _render_and_send_avwap(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        live_p, met = await self._refresh_live_price_and_metrics()
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        msg = self._build_avwap_radar_message(
            st=st,
            metrics=met,
            live_price=live_p,
            now_est=datetime.now(EST_TZ),
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_radar_inline_keyboard(),
        )

    async def _render_and_send_sync(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        now_est = datetime.now(EST_TZ)
        live_p, _ = await self._refresh_live_price_and_metrics()
        holdings_map, fetch_ok = (
            await self.context.api.get_holdings_map_with_status()
        )
        if fetch_ok and isinstance(holdings_map, dict):
            self.context.latest_holdings = holdings_map
            self.context.holdings_fetch_ok = True
            await _call_plugin_adaptive(
                reconcile_stratum_with_broker_holdings_plugin,
                context=self.context,
                holdings=holdings_map,
                prices={TARGET_SYMBOL: live_p},
                now_est=now_est,
                force_sync=False,
            )
        else:
            holdings_map = self.context.latest_holdings

        bp_usd = await self.context.api.get_buying_power_usd(
            safety_buffer_pct=0.0
        )
        fx_rate = await self.context.api.get_usd_krw_rate()
        st = self.context.ledger.get_state(TARGET_SYMBOL)

        msg = self._build_sync_instruction_message(
            st=st,
            holdings=holdings_map,
            live_price=live_p,
            broker_bp_usd=bp_usd,
            fx_rate=fx_rate,
            now_est=now_est,
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_radar_inline_keyboard(),
        )

    async def _render_and_send_settlement(
        self, chat_id: str, edit_msg_id: Optional[int] = None
    ) -> None:
        live_p, _ = await self._refresh_live_price_and_metrics()
        st = self.context.ledger.get_state(TARGET_SYMBOL)
        msg = self._build_settlement_control_message(
            st=st, live_price=live_p, now_est=datetime.now(EST_TZ)
        )
        await self.edit_or_send_message(
            chat_id=chat_id,
            message_id=edit_msg_id,
            text=msg,
            reply_markup=_build_settlement_inline_keyboard(
                bool(st.get("is_active", True))
            ),
        )

    async def _apply_new_seed_value(
        self, chat_id: str, raw_num_str: str
    ) -> None:
        cleaned = (
            str(raw_num_str)
            .replace(",", "")
            .replace("$", "")
            .replace("불", "")
            .replace("달러", "")
            .strip()
        )
        try:
            val = float(cleaned)
        except Exception:
            return
        if val < 100.0 or val > 10000000.0:
            await self.send_message(
                chat_id=chat_id,
                text="⚠️ 시드머니는 <b>$100 ~ $10,000,000</b> 사이의 숫자로 입력해 주세요. (예: <code>19000</code>)",
            )
            return

        if hasattr(self.context.ledger, "set_initial_seed_usd"):
            updated_st = await _call_plugin_adaptive(
                self.context.ledger.set_initial_seed_usd,
                new_seed_usd=val,
            )
        else:
            updated_st = await self.context.ledger.update_state(
                TARGET_SYMBOL, initial_seed_usd=round(val, 4)
            )
        self.context.wakeup_event.set()
        live_p, _ = await self._refresh_live_price_and_metrics()
        summary = summarize_stratum_portfolio(updated_st, live_price=live_p)
        quota_badge_str = _format_case67_quota_badge(summary)
        await self.send_message(
            chat_id=chat_id,
            text=(
                f"✅ <b>[기준 시드머니 설정 완료]</b>\n"
                f"🔸 새 기준 시드머니: <b>${val:,.2f}</b>\n"
                f"🔸 가용 현금시드: <b>${safe_float(summary.get('free_cash_usd'), 0.0):,.2f}</b>\n"
                f"🔸 차기 1회(8%) 예산: <b>${safe_float(summary.get('next_portion_budget'), 0.0):,.2f}</b> "
                f"➔ {quota_badge_str}"
            ),
            reply_markup=_build_settlement_inline_keyboard(
                bool(updated_st.get("is_active", True))
            ),
        )

    async def _execute_manual_update_command(self, chat_id: str) -> None:
        """
        MODIFIED [무한 재기동 루프 및 `[]` 출력 버그 원천 종결]:
        1) /update 명령 진입 즉시 _ack_telegram_offset() 을 호출하여 텔레그램 본사 서버 큐에서
           해당 update_id 를 즉시 소비 확정(Ack)하고 원장(last_tg_update_id)에 영속화한다.
        2) execute_git_pull_and_preflight 반환 튜플에서 비어 있지 않은 문자열(msg)과 변경 여부(changed)를 추출한다.
        3) 원격 저장소에 새로운 커밋이 없어 변경 사항이 없을 때(changed == False)는 os._exit(0) 를 호출하지 않고
           현재 프로세스를 무중단 유지한다!
        """
        await self._ack_telegram_offset(self._last_update_id)
        await self.send_message(
            chat_id=chat_id,
            text="⏳ <b>[시스템 업데이트 개시]</b> GitHub 최신 코드 동기화 및 9대 코어 모듈 프리플라이트 검증 중...",
        )
        fn_upd = getattr(
            plugin_updater, "execute_git_pull_and_preflight", None
        )
        changed = False
        if callable(fn_upd):
            upd_res = await _call_plugin_adaptive(
                fn_upd, base_dir=self.context.base_dir
            )
            if isinstance(upd_res, tuple):
                ok = bool(upd_res[0]) if len(upd_res) >= 1 else False
                if len(upd_res) >= 3 and isinstance(upd_res[2], bool):
                    changed = bool(upd_res[2])
                str_parts = [
                    str(x).strip()
                    for x in upd_res[1:]
                    if isinstance(x, str) and str(x).strip()
                ]
                msg = (
                    " · ".join(str_parts)
                    if str_parts
                    else "✅ 9대 코어 모듈 py_compile 및 AST 심볼 정합성 100% 통과"
                )
            else:
                ok = bool(upd_res)
                msg = "✅ 9대 코어 모듈 py_compile 및 AST 심볼 정합성 100% 통과"
        else:
            ok, msg = verify_all_modules_compile(self.context.base_dir)

        await self._ack_telegram_offset(self._last_update_id)

        if not ok:
            await self.send_message(
                chat_id=chat_id,
                text=f"⚠️ <b>[업데이트 중단]</b>\n{_escape_html(msg)}",
            )
            return

        if not changed:
            await self.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>[최신 버전 검증 완료 · 무중단 가동 유지]</b>\n"
                    f"🔸 {_escape_html(msg)}\n"
                    "🔸 원격 변경사항이 없으므로 재기동 없이 현재 스나이퍼 엔진을 계속 가동합니다."
                ),
            )
            return

        await self.send_message(
            chat_id=chat_id,
            text=f"🚀 <b>[신규 업데이트 검증 통과]</b> {_escape_html(msg)}\n잠시 후 스나이퍼 데몬을 재기동합니다...",
        )
        await asyncio.sleep(0.5)
        os._exit(0)

    async def _reset_ledger_state(
        self, chat_id: str, new_seed_usd: Optional[float] = None
    ) -> None:
        await self.context.api.cancel_all_open_orders(symbol=TARGET_SYMBOL)
        if hasattr(self.context.ledger, "reset_stratum_only"):
            st_reset = await _call_plugin_adaptive(
                self.context.ledger.reset_stratum_only,
                symbol=TARGET_SYMBOL,
                new_seed_usd=new_seed_usd,
                preserve_realized_profit=(new_seed_usd is None),
            )
        else:
            kw: Dict[str, Any] = {
                "buckets": [],
                "anchor_price": 0.0,
                "cycle_min_buy_price": 0.0,
                "ratchet_anchor_price": 0.0,
                "buy_order_id": "",
                "sell_order_id": "",
                "ordered_qty": 0,
                "target_sell_price": 0.0,
                "pending_sell_mode": "",
                "pending_lot_type": "",
            }
            if new_seed_usd is not None and new_seed_usd >= 100.0:
                kw["initial_seed_usd"] = round(new_seed_usd, 4)
                kw["cumulative_realized_net_usd"] = 0.0
            st_reset = await self.context.ledger.update_state(
                TARGET_SYMBOL, **kw
            )

        self.context.indexing_grace_cache.pop(TARGET_SYMBOL, None)
        self.context.exit_snapshot_cache.pop(TARGET_SYMBOL, None)
        self.context.wakeup_event.set()
        await self.send_message(
            chat_id=chat_id,
            text=(
                "🧹 <b>[SOXL 지층 원장 0점 소각 완료]</b>\n"
                f"🔸 기준 시드머니: <b>${safe_float(st_reset.get('initial_seed_usd'), 10000.0):,.2f}</b>\n"
                f"🔸 가용 현금시드: <b>${safe_float(st_reset.get('free_cash_usd'), 10000.0):,.2f}</b>\n"
                "🔸 미체결 주문 취소 및 지층 큐·기억저가(cycle_min_buy_price) 초기화 완료"
            ),
        )

    async def _handle_command_or_text(
        self, chat_id: str, raw_text: str
    ) -> None:
        text = str(raw_text or "").strip()
        if not text:
            return

        if not self.chat_id and chat_id:
            self.chat_id = str(chat_id)

        clean_num_check = (
            text.replace(",", "").replace("$", "").replace(".", "").strip()
        )
        if clean_num_check.isdigit():
            self._awaiting_seed_input_chats.pop(chat_id, None)
            await self._apply_new_seed_value(chat_id, text)
            return

        first_token = text.split()[0].lower()
        if "@" in first_token:
            first_token = first_token.split("@", 1)[0]

        if first_token in ("/start", "/menu", "/help") or text == "⬅️ 메인 메뉴":
            st = self.context.ledger.get_state(TARGET_SYMBOL)
            msg = self._build_start_schedule_message(
                st, now_est=datetime.now(EST_TZ)
            )
            await self.send_message(
                chat_id=chat_id,
                text=msg,
                reply_markup=_build_main_reply_keyboard(),
            )
            return

        if (
            first_token in ("/avwap", "/status", "/radar")
            or text == "🔫 스나이퍼 레이더"
        ):
            await self._render_and_send_avwap(chat_id=chat_id)
            return

        if first_token == "/sync" or text in (
            "📜 지층 지시서",
            "🔄 잔고 동기화",
        ):
            await self._render_and_send_sync(chat_id=chat_id)
            return

        if (
            first_token in ("/settlement", "/config")
            or text == "⚙️ 전술 제어반"
        ):
            await self._render_and_send_settlement(chat_id=chat_id)
            return

        if first_token == "/seed":
            parts = text.split(maxsplit=1)
            if len(parts) >= 2:
                await self._apply_new_seed_value(chat_id, parts[1])
            else:
                await self._render_and_send_settlement(chat_id=chat_id)
            return

        if first_token == "/reset_seed":
            parts = text.split(maxsplit=1)
            new_seed: Optional[float] = None
            if len(parts) >= 2:
                sv = safe_float(parts[1], 0.0)
                if sv >= 100.0:
                    new_seed = sv
            await self._reset_ledger_state(
                chat_id=chat_id, new_seed_usd=new_seed
            )
            return

        if first_token == "/reset":
            await self._reset_ledger_state(chat_id=chat_id, new_seed_usd=None)
            return

        if first_token == "/update" or text == "🚀 시스템 업데이트":
            await self._execute_manual_update_command(chat_id=chat_id)
            return

    async def _handle_callback_query(self, cb: Dict[str, Any]) -> None:
        cb_id = str(cb.get("id") or "").strip()
        data = str(cb.get("data") or "").strip()
        msg_obj = (
            cb.get("message") if isinstance(cb.get("message"), dict) else {}
        )
        chat_obj = (
            msg_obj.get("chat") if isinstance(msg_obj.get("chat"), dict) else {}
        )
        chat_id = str(chat_obj.get("id") or self.chat_id or "").strip()
        msg_id = safe_int(msg_obj.get("message_id"), 0)

        if cb_id:
            await self._call_tg(
                "answerCallbackQuery",
                {"callback_query_id": cb_id},
                timeout_sec=5.0,
            )

        if not chat_id:
            return

        if data == "cmd_avwap":
            await self._render_and_send_avwap(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_sync":
            await self._render_and_send_sync(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_settlement":
            await self._render_and_send_settlement(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "cmd_start":
            st = self.context.ledger.get_state(TARGET_SYMBOL)
            msg = self._build_start_schedule_message(
                st, now_est=datetime.now(EST_TZ)
            )
            await self.edit_or_send_message(
                chat_id=chat_id,
                message_id=msg_id,
                text=msg,
                reply_markup=_build_radar_inline_keyboard(),
            )
        elif data == "toggle_bot_active":
            st_cur = self.context.ledger.get_state(TARGET_SYMBOL)
            next_act = not bool(st_cur.get("is_active", True))
            await self.context.ledger.update_state(
                TARGET_SYMBOL, is_active=next_act
            )
            if next_act:
                fn_relock = getattr(
                    defense_plugin, "relock_stratum_on_bot_resume", None
                )
                if callable(fn_relock):
                    await _call_plugin_adaptive(
                        fn_relock,
                        context=self.context,
                        now_est=datetime.now(EST_TZ),
                    )
                else:
                    await _call_plugin_adaptive(
                        reconcile_stratum_with_broker_holdings_plugin,
                        context=self.context,
                        holdings=self.context.latest_holdings,
                        prices=self.context.latest_prices,
                        now_est=datetime.now(EST_TZ),
                        force_sync=True,
                    )
            self.context.wakeup_event.set()
            await self._render_and_send_settlement(
                chat_id=chat_id, edit_msg_id=msg_id
            )
        elif data == "prompt_seed_input":
            self._awaiting_seed_input_chats[chat_id] = time.monotonic()
            await self.send_message(
                chat_id=chat_id,
                text=(
                    "💵 <b>[기준 시드머니($) 입력 안내]</b>\n"
                    "운용할 기준 달러 시드머니 금액을 숫자만 채팅창에 입력해 주세요.\n"
                    "예시: <code>10000</code> 또는 <code>19000</code>"
                ),
            )

    async def run_polling_loop(self) -> None:
        """
        내장형 텔레그램 getUpdates 비동기 롱폴링 루프
        (엄격한 토큰 검증 + deleteWebhook 초기화 + 기동 시 적체 큐 무조건부 일괄 소각 탑재)
        """
        if not _extract_valid_tg_token(self.bot_token):
            self.bot_token, discovered_cid = _discover_telegram_credentials(
                self.context.base_dir, getattr(self.context, "api", None)
            )
            if not self.chat_id and discovered_cid:
                self.chat_id = discovered_cid

        if not _extract_valid_tg_token(self.bot_token):
            logger.error(
                "🚨 유효한 텔레그램 봇 토큰(숫자:영숫자)을 찾을 수 없습니다! .env 의 TELEGRAM_BOT_TOKEN 을 확인하세요."
            )
            while not self.context.shutdown_event.is_set():
                await asyncio.sleep(10.0)
            return

        await self._call_tg(
            "deleteWebhook", {"drop_pending_updates": False}, timeout_sec=8.0
        )
        await self._flush_stale_updates_on_startup()
        logger.info(
            "📡 [텔레그램 관제탑 폴링 루프 가동 완료] (chat_id=%s)",
            self.chat_id or "auto-detect",
        )

        while not self.context.shutdown_event.is_set():
            try:
                payload: Dict[str, Any] = {
                    "offset": self._last_update_id + 1,
                    "timeout": 15,
                    "allowed_updates": ["message", "callback_query"],
                }
                res = await self._call_tg(
                    "getUpdates", payload, timeout_sec=20.0
                )
                if isinstance(res, dict) and bool(res.get("ok")):
                    updates = res.get("result")
                    if isinstance(updates, list):
                        for upd in updates:
                            if not isinstance(upd, dict):
                                continue
                            uid = safe_int(upd.get("update_id"), 0)
                            if uid > self._last_update_id:
                                self._last_update_id = uid
                                try:
                                    await self.context.ledger.update_state(
                                        TARGET_SYMBOL, last_tg_update_id=uid
                                    )
                                except Exception:
                                    pass

                            if isinstance(upd.get("callback_query"), dict):
                                await self._handle_callback_query(
                                    upd["callback_query"]
                                )
                            elif isinstance(upd.get("message"), dict):
                                m_obj = upd["message"]
                                c_obj = (
                                    m_obj.get("chat")
                                    if isinstance(m_obj.get("chat"), dict)
                                    else {}
                                )
                                cid = str(
                                    c_obj.get("id") or self.chat_id or ""
                                ).strip()
                                if self.chat_id and cid and cid != self.chat_id:
                                    continue
                                txt = str(m_obj.get("text") or "").strip()
                                msg_date = safe_float(m_obj.get("date"), 0.0)
                                if (
                                    msg_date > 0.0
                                    and msg_date < (self._boot_epoch - 1.0)
                                    and (
                                        txt.lower().startswith("/update")
                                        or txt == "🚀 시스템 업데이트"
                                    )
                                ):
                                    await self._ack_telegram_offset(uid)
                                    continue
                                if txt:
                                    await self._handle_command_or_text(
                                        chat_id=cid, raw_text=txt
                                    )
                else:
                    await asyncio.sleep(1.5)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("텔레그램 폴링 루프 일시 예외: %s", exc)
                await asyncio.sleep(2.0)


async def _run_auto_update_loop_dynamic(
    context: Any, interval_sec: float = 3600.0
) -> None:
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

        self.in_memory_ordering_lock: asyncio.Lock = asyncio.Lock()
        self.dual_strike_mutex: asyncio.Lock = asyncio.Lock()
        self.wakeup_event: asyncio.Event = asyncio.Event()
        self.shutdown_event: asyncio.Event = asyncio.Event()

        self.latest_prices: Dict[str, float] = {TARGET_SYMBOL: 0.0}
        self.latest_holdings: Dict[str, Dict[str, Any]] = {
            TARGET_SYMBOL: _empty_soxl_holding_entry()
        }
        self.holdings_fetch_ok: bool = False
        self.candle_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self.exit_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self.indexing_grace_cache: Dict[str, Dict[str, Any]] = {}

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

            if 400 <= hm <= 406:
                await _call_plugin_adaptive(
                    execute_0400_premarket_stabilization_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

            elif is_active_trading_window(hm):
                # 🚨 3-0) [Case 66 핵심 방어 : 미결 매수 주문 최우선 선행 정산]
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
                    await _call_plugin_adaptive(
                        execute_seed_protected_buy_gate_plugin,
                        context=context,
                        holdings=holdings,
                        prices=prices,
                        now_est=now_est,
                    )

            elif 1600 <= hm <= 1659:
                await _call_plugin_adaptive(
                    execute_1600_daily_closing_settlement_plugin,
                    context=context,
                    holdings=holdings,
                    prices=prices,
                    now_est=now_est,
                )

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
# [핵심 6] 시스템 부트스트랩 · 프리플라이트 TDD · Case 11 시그널 0.1초 고속 종료 가드
# ---------------------------------------------------------------------------
async def _run_init_step_with_cancel_guard(
    context: SniperAppContext,
    coro: Awaitable[Any],
    timeout_sec: float = 8.0,
) -> Any:
    """
    NEW [Case 11 부팅 중 SIGTERM 0.1초 고속 종료 가드]:
    부팅 초기화 네트워크 I/O 도중 SIGTERM / SIGINT 시그널이 유입되면
    블로킹 대기 없이 0.1초 이내에 즉시 코루틴을 취소하고 탈출하여
    systemd 의 'State stop-sigterm timed out. Killing.' 현상을 100% 원천 차단한다.
    """
    if context.shutdown_event.is_set():
        try:
            coro.close()  # type: ignore[attr-defined]
        except Exception:
            pass
        return None

    work_task = asyncio.create_task(coro)
    stop_task = asyncio.create_task(context.shutdown_event.wait())
    try:
        done, _ = await asyncio.wait(
            {work_task, stop_task},
            timeout=timeout_sec,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done or context.shutdown_event.is_set():
            work_task.cancel()
            return None
        if work_task in done:
            return work_task.result()
        work_task.cancel()
        return None
    finally:
        if not stop_task.done():
            stop_task.cancel()
        if not work_task.done():
            work_task.cancel()


async def async_main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__)) or "."

    purged = purge_deprecated_legacy_files(base_dir)
    if purged:
        logger.info("🧹 [구버전 폐기 파일 청소 완료] 삭제 목록: %s", purged)

    ok_compile, err_msg = verify_all_modules_compile(base_dir)
    if not ok_compile:
        logger.critical(
            "🚨 [프리플라이트 컴파일/심볼 검증 실패] 기동 중단: %s", err_msg
        )
        sys.exit(1)

    run_quant_engine_tdd()
    logger.info(
        "✅ [QuantEngine TDD 사전 무결성 검증 통과] 전술 A (8%% 황금정량 · Case 67 min(1회분, 고정정량) 캡핑 · 4단 와이드 활주로 · 눌림/돌파 독립 1층 재장전 · Case 59/60/65/66/67) 검증 완료"
    )

    context = SniperAppContext(base_dir=base_dir)

    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        logger.info(
            "🛑 종료 시그널(SIGTERM/SIGINT) 수신 -> 고속 안전 종료 개시..."
        )
        context.shutdown_event.set()
        context.wakeup_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    if context.shutdown_event.is_set():
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")
        return

    try:
        acc_seq = await _run_init_step_with_cancel_guard(
            context, context.api.ensure_account_seq(), timeout_sec=8.0
        )
        if acc_seq is not None:
            logger.info(
                "🏛️ [제2헌법 AccountSeq 락온 완료] accountSeq=%d", acc_seq
            )
    except Exception as exc:
        logger.warning(
            "⚠️ 초기 AccountSeq 조회 지연 (루프 내 자동 재시도): %s", exc
        )

    if context.shutdown_event.is_set():
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")
        return

    try:
        price_map = await _run_init_step_with_cancel_guard(
            context,
            context.api.get_current_prices_batch((TARGET_SYMBOL,)),
            timeout_sec=6.0,
        )
        if (
            isinstance(price_map, dict)
            and safe_float(price_map.get(TARGET_SYMBOL), 0.0) > 0.0
        ):
            context.latest_prices[TARGET_SYMBOL] = safe_float(
                price_map.get(TARGET_SYMBOL), 0.0
            )
        h_res = await _run_init_step_with_cancel_guard(
            context,
            context.api.get_holdings_map_with_status(),
            timeout_sec=6.0,
        )
        if isinstance(h_res, tuple) and len(h_res) == 2:
            holdings_map, fetch_ok = h_res
            context.holdings_fetch_ok = bool(fetch_ok)
            if fetch_ok and isinstance(holdings_map, dict):
                context.latest_holdings = holdings_map
                await _run_init_step_with_cancel_guard(
                    context,
                    reconcile_stratum_with_broker_holdings_plugin(
                        context=context,
                        holdings=holdings_map,
                        prices=context.latest_prices,
                        now_est=datetime.now(EST_TZ),
                        force_sync=True,
                    ),
                    timeout_sec=6.0,
                )
    except Exception as exc:
        logger.warning("초기 잔고/시세 동기화 폴백: %s", exc)

    if context.shutdown_event.is_set():
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")
        return

    # 기동 배너 발송 전에 텔레그램 적체 큐를 먼저 소각하여 과거 /update 재발동을 100% 차단
    await _run_init_step_with_cancel_guard(
        context, context.tg._flush_stale_updates_on_startup(), timeout_sec=6.0
    )
    if not context.shutdown_event.is_set():
        await _run_init_step_with_cancel_guard(
            context, context.tg.send_startup_alive_banner(), timeout_sec=6.0
        )

    if context.shutdown_event.is_set():
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")
        return

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
        "🚀 [SOXL 지층관리 스나이퍼 가동 완료] 04:07 ~ 16:00 EST · 전술 A (🛡️ 05_AF_WIDE_10_01) · 8%% 황금정량 · Case 67 min(1회분,고정정량) 캡핑 · 10분 4단 와이드 매수게이트 · 1분 상시 매도 · 양방향 60초 타임쉴드 & 선행 체결정산(Case 65/66/67)"
    )

    try:
        await context.shutdown_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        close_fn = getattr(context.api, "close", None)
        if callable(close_fn):
            try:
                c_res = close_fn()
                if inspect.isawaitable(c_res):
                    await c_res
            except Exception:
                pass
        logger.info("✅ [SOXL 지층관리 스나이퍼 안전 종료 완료]")


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
