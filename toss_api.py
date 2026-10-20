# =====================================================================
# FILE: toss_api.py
# 목적: 토스증권 Open API 비동기 통신 통제소 (toss_api.py 2/2 분할본 · toss_api_core 리익스포트 결속)
#       — 다건 배치 현재가 조회, 호가/1분봉 페이지네이션, QQQ 0.0초 실시간 스냅샷 브릿지,
#         제3종목 주문 원천 차단, 미국 주식 주문 정정(quantity 적출), HTTP 415 빈 JSON 취소 방어,
#         금요일 17:00 ~ 일요일 17:59 EST 주말 휴장 확정 및 일요일 18:00 EST 논리적 거래일 롤오버
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 인증·스로틀·AccountSeq 베이스를 toss_api_core.py로 분리하고 100% 리익스포트 결속
# MODIFIED: Case 09 & 초과 Case 54 — REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL") 외 제3종목 주문 물리적 차단
# MODIFIED: 초과 Case 69 — 미국 주식 주문 정정(modify_order) 시 quantity 파라미터 원천 적출 (400 에러 방어)
# MODIFIED: 초과 Case 75 — cancel_order 시 빈 JSON(json_data={}) 자동 주입으로 HTTP 415 소각
# MODIFIED: 초과 Case 83 — get_current_prices_batch 다건 시세 동시 조회로 SOXS·SOXL 틱 합성 시차 0.0ms 달성
# MODIFIED: Case 01, 초과 Case 36, 57, 90 — 금요일 17:00 EST ~ 일요일 17:59 EST 주말 휴장 확정 및
#           일~목 18:00 EST 이후 익일(월~금) 논리적 거래일 롤오버로 월요일 아침 주말 오판 버그 영구 소각
# NEW: 초과 Case 91 — 읽기 전용 get_qqq_realtime_snapshot 탑재로 나스닥 100(QQQ) 0.0초 실시간 단기 변화율 산출 지원

import time
import math
import asyncio
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from toss_api_core import (
    EST_TZ,
    KST_TZ,
    _GLOBAL_CALENDAR_CACHE,
    _HOLIDAY_DATE_CACHE,
    get_nyse_holiday_name,
    GlobalThrottle,
    _NonRetryableApiError,
    TossApiClientCore,
)


class TossApiClient(TossApiClientCore):
    async def get_current_prices_batch(self, symbols: tuple | list = ("SOXS", "SOXL")) -> dict[str, float]:
        """초과 Case 83: 다건 현재가 배치 조회 API(/api/v1/prices?symbols=SOXS,SOXL)로 시차 0.0ms 동기화."""
        sym_list = [str(s).strip().upper() for s in symbols if str(s).strip()]
        if not sym_list:
            return {}
        joined = ",".join(sym_list)
        out = {s: 0.0 for s in sym_list}
        try:
            data = await self._request(
                "GET",
                f"/api/v1/prices?symbols={joined}",
                "MARKET_DATA",
                headers=self._get_headers(include_account=False),
            )
            items = (data or {}).get("result") or []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        s_code = str(item.get("symbol") or "").strip().upper()
                        lp = float(item.get("lastPrice") or 0.0)
                        if s_code in out and lp > 0.0:
                            out[s_code] = lp
        except Exception as e:
            print(f"🚨 [배치 현재가 조회 방어 ({joined})] {e}", flush=True)
        return out

    async def get_current_price(self, symbol: str) -> float:
        sym = str(symbol).strip().upper()
        batch = await self.get_current_prices_batch((sym,))
        return float(batch.get(sym, 0.0))

    async def get_orderbook(self, symbol: str) -> dict:
        sym = str(symbol).strip().upper()
        data = await self._request(
            "GET",
            f"/api/v1/orderbook?symbol={sym}",
            "MARKET_DATA",
            headers=self._get_headers(include_account=False),
        )
        res = (data or {}).get("result")
        return res if isinstance(res, dict) else {"asks": [], "bids": []}

    async def get_1m_candles_pagination(
        self, symbol: str, count: int = 200, before: str = None
    ) -> dict:
        sym = str(symbol).strip().upper()
        cnt = max(1, min(200, int(count or 200)))
        endpoint = f"/api/v1/candles?symbol={sym}&interval=1m&count={cnt}&adjusted=true"
        if before:
            encoded_before = urllib.parse.quote(str(before), safe="")
            endpoint += f"&before={encoded_before}"
        data = await self._request(
            "GET",
            endpoint,
            "MARKET_DATA_CHART",
            headers=self._get_headers(include_account=False),
        )
        res = (data or {}).get("result")
        if not isinstance(res, dict):
            return {"candles": [], "nextBefore": None}
        candles = res.get("candles")
        return {
            "candles": candles if isinstance(candles, list) else [],
            "nextBefore": res.get("nextBefore"),
        }

    async def get_1m_candles(self, symbol: str, count: int = 200) -> list:
        res = await self.get_1m_candles_pagination(symbol=symbol, count=count, before=None)
        return res.get("candles", [])

    async def get_qqq_realtime_snapshot(self, count: int = 25) -> tuple[float, list]:
        """
        NEW: 초과 Case 91 [해법 2 방법 A] — SOXL/SOXS 롱·숏 자체 노이즈에 절대 영향받지 않고
        야후 파이낸스 선물 피드의 10분 지연을 0.0초로 제거하기 위해,
        토스 Open API 조회 전용 엔드포인트로 나스닥 100 현물 ETF(QQQ)의 실시간 현재가와 1분봉을 조회합니다.
        """
        try:
            q_price, q_candles = await asyncio.gather(
                self.get_current_price("QQQ"),
                self.get_1m_candles("QQQ", count=max(15, min(60, int(count or 25)))),
            )
            return float(q_price or 0.0), (q_candles if isinstance(q_candles, list) else [])
        except Exception as e:
            print(f"🚨 [QQQ 실시간 스냅샷 조회 방어] {e}", flush=True)
            return 0.0, []

    async def get_symbol_holdings_detail(self, symbol: str) -> dict:
        """Case 03 & 초과 Case 29: 보유 주식 상세 조회 및 null 방어 0.0 형변환."""
        sym = str(symbol).strip().upper()
        data = await self._request(
            "GET",
            f"/api/v1/holdings?symbol={sym}",
            "ASSET",
            headers=self._get_headers(include_account=True),
        )
        res = (data or {}).get("result")
        if not isinstance(res, dict):
            return {"qty": 0.0, "avg_price": 0.0, "profit_usd": 0.0, "profit_rate": 0.0}

        items = res.get("items") or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and str(item.get("symbol") or "").upper() == sym:
                    qty = float(item.get("quantity") or 0.0)
                    avg_p = float(item.get("averagePurchasePrice") or 0.0)
                    pl_obj = item.get("profitLoss") if isinstance(item.get("profitLoss"), dict) else {}
                    profit_usd = float(pl_obj.get("amount") or 0.0)
                    profit_rate = float(pl_obj.get("rate") or 0.0)
                    return {
                        "qty": qty,
                        "avg_price": avg_p,
                        "profit_usd": profit_usd,
                        "profit_rate": profit_rate,
                    }
        return {"qty": 0.0, "avg_price": 0.0, "profit_usd": 0.0, "profit_rate": 0.0}

    async def get_usd_buying_power(self) -> float:
        """초과 Case 65: USD 현금 주문가능금액(cashBuyingPower) 조회."""
        data = await self._request(
            "GET",
            "/api/v1/buying-power?currency=USD",
            "ORDER_INFO",
            headers=self._get_headers(include_account=True),
        )
        res = (data or {}).get("result")
        if isinstance(res, dict) and res.get("cashBuyingPower") is not None:
            return float(res.get("cashBuyingPower") or 0.0)
        return 0.0

    async def get_sellable_quantity(self, symbol: str) -> float:
        sym = str(symbol).strip().upper()
        data = await self._request(
            "GET",
            f"/api/v1/sellable-quantity?symbol={sym}",
            "ORDER_INFO",
            headers=self._get_headers(include_account=True),
        )
        res = (data or {}).get("result")
        if isinstance(res, dict) and res.get("sellableQuantity") is not None:
            return float(res.get("sellableQuantity") or 0.0)
        return 0.0

    async def get_orders(self, status: str = "OPEN", symbol: str = None) -> list:
        st = str(status or "OPEN").strip().upper()
        endpoint = f"/api/v1/orders?status={st}"
        if symbol:
            endpoint += f"&symbol={str(symbol).strip().upper()}"
        data = await self._request(
            "GET",
            endpoint,
            "ORDER_HISTORY",
            headers=self._get_headers(include_account=True),
        )
        res = (data or {}).get("result")
        if isinstance(res, dict) and isinstance(res.get("orders"), list):
            return res["orders"]
        return []

    async def get_order_detail(self, order_id: str) -> dict:
        oid = urllib.parse.quote(str(order_id).strip(), safe="")
        data = await self._request(
            "GET",
            f"/api/v1/orders/{oid}",
            "ORDER_HISTORY",
            headers=self._get_headers(include_account=True),
        )
        res = (data or {}).get("result")
        return res if isinstance(res, dict) else {}

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: int | float,
        price: str = None,
        client_order_id: str = None,
        time_in_force: str = "DAY",
    ) -> dict:
        """제5헌법, Case 09, 14, 34, 54: 허용 종목(SOXS/SOXL) 검증 및 멱등성 키(clientOrderId) 강제 주입."""
        sym = self._validate_order_symbol(symbol)
        side_up = str(side).strip().upper()
        otype_up = str(order_type).strip().upper()
        tif_up = str(time_in_force or "DAY").strip().upper()

        idem_key = str(client_order_id or f"SNP_{sym}_{int(time.time() * 1000)}")[:36]
        qty_int = int(math.floor(float(quantity)))
        if qty_int <= 0:
            raise ValueError(f"주문 수량은 1주 이상이어야 합니다: {quantity}")

        payload = {
            "clientOrderId": idem_key,
            "symbol": sym,
            "side": side_up,
            "orderType": otype_up,
            "timeInForce": tif_up,
            "quantity": str(qty_int),
        }
        if otype_up == "LIMIT":
            if price is None:
                raise ValueError("지정가(LIMIT) 주문에는 price가 필수입니다.")
            payload["price"] = f"{float(price):.2f}"

        return await self._request(
            "POST",
            "/api/v1/orders",
            "ORDER",
            headers=self._get_headers(include_account=True),
            json_data=payload,
        )

    async def modify_order(
        self,
        order_id: str,
        price: str,
        order_type: str = "LIMIT",
        quantity: int = None,
        market_country: str = "US",
    ) -> dict:
        """초과 Case 69: 미국 주식(US) 주문 정정 시 quantity 파라미터를 원천 적출하여 400 에러를 방어합니다."""
        oid = urllib.parse.quote(str(order_id).strip(), safe="")
        otype_up = str(order_type or "LIMIT").strip().upper()
        payload = {
            "orderType": otype_up,
            "price": f"{float(price):.2f}",
        }
        if str(market_country or "US").strip().upper() != "US" and quantity is not None:
            payload["quantity"] = str(int(quantity))

        return await self._request(
            "POST",
            f"/api/v1/orders/{oid}/modify",
            "ORDER",
            headers=self._get_headers(include_account=True),
            json_data=payload,
        )

    async def cancel_order(self, order_id: str) -> dict:
        """초과 Case 75: 주문 취소 호출 시 빈 JSON 객체(json_data={})를 명시적으로 전달하여 HTTP 415를 원천 소각합니다."""
        oid = urllib.parse.quote(str(order_id).strip(), safe="")
        return await self._request(
            "POST",
            f"/api/v1/orders/{oid}/cancel",
            "ORDER",
            headers=self._get_headers(include_account=True),
            json_data={},
        )

    async def is_market_open(self) -> tuple[bool, str, str, str]:
        """
        제6헌법, Case 01, 10, 33, 36, 45, 57, 90:
        1) 금요일 17:00 EST ~ 일요일 17:59 EST 를 완전한 주말 휴장 구간으로 확정합니다.
        2) 일~목요일 18:00 EST 이후에는 논리적 거래일을 익일(월~금요일)로 롤오버하여
           월요일 아침(일요일 18:00 ~ 23:59 EST)이 주말 휴장으로 오판되는 버그를 원천 소각합니다.
        3) 일~목요일 18:00 ~ 18:59 EST 구간은 'STANDBY|선물 신세션 개장 · 19:00 데이장 대기'로 명확히 분리합니다.
        """
        now_est = datetime.now(EST_TZ)
        wk = now_est.weekday()  # Mon=0 ... Fri=4, Sat=5, Sun=6
        est_hm = now_est.hour * 100 + now_est.minute

        # 1) 완전한 주말 휴장 구간: 금요일 17:00 EST ~ 일요일 17:59 EST (초과 Case 90)
        if (wk == 4 and est_hm >= 1700) or (wk == 5) or (wk == 6 and est_hm < 1800):
            return False, "04:00", "HOLIDAY|주말 (Weekend)", "09:30"

        # 2) 논리적 거래일(Logical Trading Date) 산출: 일~목 18:00 EST 이후에는 익일(월~금) 영업일 적용
        logical_dt = (now_est + timedelta(days=1)) if est_hm >= 1800 else now_est
        target_date = logical_dt.date()
        target_date_str = target_date.strftime("%Y-%m-%d")

        # 3) 시간대 버킷 키 산출 (세션 전환 정각에 캐시 지연 0.0초 보장)
        if 1700 <= est_hm < 1800:
            time_bucket = "GC_1700"
        elif 1800 <= est_hm < 1900:
            time_bucket = "CME_OPEN_1800"
        elif est_hm >= 1900 or est_hm < 400:
            time_bucket = "DAY_1900"
        elif 400 <= est_hm < 930:
            time_bucket = "PRE_0400"
        elif 930 <= est_hm < 1600:
            time_bucket = "REG_0930"
        else:
            time_bucket = "AFT_1600"

        bucket_key = f"{target_date_str}_{time_bucket}"
        now_ts = time.time()
        if (
            _GLOBAL_CALENDAR_CACHE.get("bucket_key") == bucket_key
            and (now_ts - float(_GLOBAL_CALENDAR_CACHE.get("timestamp", 0.0))) < 30.0
        ):
            return _GLOBAL_CALENDAR_CACHE["result"]

        # 4) NYSE 정규 휴장일 판정 (비동기 스레드 격리)
        holiday_kr = await asyncio.to_thread(get_nyse_holiday_name, target_date)
        if holiday_kr:
            res_tuple = (False, "04:00", f"HOLIDAY|{holiday_kr}", "09:30")
            _GLOBAL_CALENDAR_CACHE.update({
                "timestamp": now_ts,
                "bucket_key": bucket_key,
                "result": res_tuple,
            })
            return res_tuple

        # 5) 토스 시장 캘린더 API(/api/v1/market-calendar/US?date=...) 교차 확인 및 제6헌법 Fail-Open 폴백
        try:
            cal_data = await asyncio.wait_for(
                self._request(
                    "GET",
                    f"/api/v1/market-calendar/US?date={target_date_str}",
                    "MARKET_INFO",
                    headers=self._get_headers(include_account=False),
                ),
                timeout=5.0,
            )
            today_info = ((cal_data or {}).get("result") or {}).get("today")
            if isinstance(today_info, dict) and today_info.get("date") == target_date_str:
                if (
                    today_info.get("dayMarket") is None
                    and today_info.get("preMarket") is None
                    and today_info.get("regularMarket") is None
                    and today_info.get("afterMarket") is None
                ):
                    res_tuple = (False, "04:00", "HOLIDAY|미국 증시 휴장일 (US Holiday)", "09:30")
                    _GLOBAL_CALENDAR_CACHE.update({
                        "timestamp": now_ts,
                        "bucket_key": bucket_key,
                        "result": res_tuple,
                    })
                    return res_tuple
        except Exception:
            pass

        # 6) 정상 영업일 시간대별 세션 확정 (EST 단일 타임존 100% 락온)
        if 1700 <= est_hm < 1800:
            res_tuple = (False, "04:00", "STANDBY|17:00 정산 및 시스템 대기", "09:30")
        elif 1800 <= est_hm < 1900:
            res_tuple = (False, "04:00", "STANDBY|선물 신세션 개장 · 19:00 데이장 대기", "09:30")
        elif est_hm >= 1900 or est_hm < 400:
            res_tuple = (True, "04:00", "dayMarket", "09:30")
        elif 400 <= est_hm < 930:
            res_tuple = (True, "04:00", "preMarket", "09:30")
        elif 930 <= est_hm < 1600:
            res_tuple = (True, "04:00", "regularMarket", "09:30")
        else:
            res_tuple = (True, "04:00", "afterMarket", "09:30")

        _GLOBAL_CALENDAR_CACHE.update({
            "timestamp": now_ts,
            "bucket_key": bucket_key,
            "result": res_tuple,
        })
        return res_tuple
