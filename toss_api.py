# =====================================================================
# FILE: toss_api.py
# 목적: 토스증권 Open API 비동기 통신 통제소 (GlobalThrottle, AccountSeq 락온,
#       다건 배치 현재가 조회, 제3종목 원천 차단, HTTP 415/422 방어 및
#       일요일 18:00 EST 논리적 거래일 롤오버 기반 미국 시장 캘린더 판정)
# =====================================================================
# MODIFIED: 제1헌법 및 Case 20, 30 의거 GlobalThrottle API(0.15초) 및 파일 비동기 Lock 중앙 통제
# MODIFIED: 제2헌법 의거 /api/v1/accounts 조회 후 BROKERAGE accountSeq 인메모리 락온 및 헤더 원자적 주입
# MODIFIED: Case 09 & 초과 Case 54 — REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL") 외 제3종목 주문 물리적 차단
# MODIFIED: 초과 Case 69 — 미국 주식 주문 정정(modify_order) 시 quantity 파라미터 원천 적출 (400 에러 방어)
# MODIFIED: 초과 Case 75 — POST/PUT/PATCH 호출 및 cancel_order 시 빈 JSON(json_data={}) 자동 주입으로 HTTP 415 소각
# MODIFIED: 초과 Case 83 — get_current_prices_batch 다건 시세 동시 조회로 SOXS·SOXL 틱 합성 시차 0.0ms 달성
# NEW: Case 01, 초과 Case 36, 57, 90 — 금요일 17:00 EST ~ 일요일 17:59 EST 주말 휴장 확정 및
#      일~목 18:00 EST 이후 익일(월~금) 논리적 거래일 롤오버로 월요일 아침(일요일 18:00~23:59 EST) 주말 오판 버그 영구 소각

import os
import time
import math
import asyncio
import urllib.parse
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import aiohttp
import pandas as pd

try:
    import pandas_market_calendars as mcal
    _NYSE_CALENDAR = mcal.get_calendar("NYSE")
except Exception:
    mcal = None
    _NYSE_CALENDAR = None

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 인메모리 캘린더 캐시 (초과 Case 33, 90: 30초 TTL + 세션 구간 버킷 전환 시 즉시 갱신)
_GLOBAL_CALENDAR_CACHE = {
    "timestamp": 0.0,
    "bucket_key": "",
    "result": (True, "04:00", "regularMarket", "09:30"),
}

_HOLIDAY_DATE_CACHE: dict[str, str] = {}


def get_nyse_holiday_name(target_date: date) -> str:
    """
    pandas_market_calendars(NYSE)를 사용하여 지정된 미국 현지 영업일(target_date)이
    정규 휴장일인지 판별하고 한글 휴장일명을 반환합니다 (초과 Case 45, 90).
    평일 정상 개장일이면 빈 문자열("")을 반환합니다.
    """
    if target_date is None:
        return ""
    if target_date.weekday() in (5, 6):
        return "주말 (Weekend)"

    d_str = target_date.strftime("%Y-%m-%d")
    if d_str in _HOLIDAY_DATE_CACHE:
        return _HOLIDAY_DATE_CACHE[d_str]

    # 미국 주요 정규 휴장일 월/일 기반 한글 정밀 맵핑 (Case 45)
    fixed_kr_map = {
        (1, 1): "신정 (New Year's Day)",
        (1, 2): "신정 대체휴일 (New Year's Day Observed)",
        (6, 19): "준틴스 독립기념일 (Juneteenth)",
        (6, 18): "준틴스 대체휴일 (Juneteenth Observed)",
        (6, 20): "준틴스 대체휴일 (Juneteenth Observed)",
        (7, 3): "독립기념일 대체휴일 (Independence Day Observed)",
        (7, 4): "미국 독립기념일 (Independence Day)",
        (7, 5): "독립기념일 대체휴일 (Independence Day Observed)",
        (12, 24): "크리스마스 대체휴일 (Christmas Observed)",
        (12, 25): "크리스마스 (Christmas Day)",
        (12, 26): "크리스마스 대체휴일 (Christmas Observed)",
    }

    def _resolve_floating_kr_name(dt: date) -> str:
        m, d, wk = dt.month, dt.day, dt.weekday()
        if (m, d) in fixed_kr_map:
            return fixed_kr_map[(m, d)]
        if m == 1 and wk == 0 and 15 <= d <= 21:
            return "마틴 루터 킹 데이 (Martin Luther King Jr. Day)"
        if m == 2 and wk == 0 and 15 <= d <= 21:
            return "대통령의 날 (Presidents' Day)"
        if m in (3, 4) and wk == 4:
            return "성금요일 (Good Friday)"
        if m == 5 and wk == 0 and d >= 25:
            return "메모리얼 데이 (Memorial Day)"
        if m == 9 and wk == 0 and d <= 7:
            return "노동절 (Labor Day)"
        if m == 11 and wk == 3 and 22 <= d <= 28:
            return "추수감사절 (Thanksgiving Day)"
        return "미국 증시 휴장일 (US Market Holiday)"

    try:
        if _NYSE_CALENDAR is not None:
            sched = _NYSE_CALENDAR.schedule(start_date=d_str, end_date=d_str)
            if sched is not None and sched.empty:
                kr_name = _resolve_floating_kr_name(target_date)
                _HOLIDAY_DATE_CACHE[d_str] = kr_name
                return kr_name
            _HOLIDAY_DATE_CACHE[d_str] = ""
            return ""
    except Exception:
        pass

    return ""


class GlobalThrottle:
    """제1헌법 및 Case 20, 30: API Rate Limit 방어(0.15초 간격) 및 파일별 비동기 뮤텍스 중앙 통제소."""
    _api_locks: dict[str, asyncio.Lock] = {}
    _last_call_times: dict[str, float] = {}
    _file_locks: dict[str, asyncio.Lock] = {}

    @classmethod
    async def wait_api_sync(cls, group: str = "DEFAULT", min_interval: float = 0.15):
        if group not in cls._api_locks:
            cls._api_locks[group] = asyncio.Lock()
        async with cls._api_locks[group]:
            now = time.monotonic()
            last_ts = cls._last_call_times.get(group, 0.0)
            elapsed = now - last_ts
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            cls._last_call_times[group] = time.monotonic()

    @classmethod
    def get_file_lock(cls, filepath: str) -> asyncio.Lock:
        norm_path = os.path.abspath(str(filepath))
        if norm_path not in cls._file_locks:
            cls._file_locks[norm_path] = asyncio.Lock()
        return cls._file_locks[norm_path]


class _NonRetryableApiError(Exception):
    """Case 21: 4xx 클라이언트 비즈니스 에러 발생 시 지수 백오프 없이 즉시 탈출하기 위한 전용 예외."""
    pass


class TossApiClient:
    # Case 09 & 초과 Case 54: 듀얼 스나이퍼 실거래 주문 허용 종목 화이트리스트 하드 락온
    REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL")

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        base_url: str = "https://openapi.tossinvest.com",
        **kwargs,
    ):
        self.client_id = (
            client_id
            or kwargs.get("app_key")
            or os.getenv("TOSS_CLIENT_ID")
            or os.getenv("TOSS_APP_KEY")
            or ""
        ).strip()
        self.client_secret = (
            client_secret
            or kwargs.get("app_secret")
            or os.getenv("TOSS_CLIENT_SECRET")
            or os.getenv("TOSS_APP_SECRET")
            or ""
        ).strip()
        self.base_url = base_url.rstrip("/")
        self.access_token: str = ""
        self.token_expires_at: float = 0.0
        self.account_seq: int = 0
        self._auth_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10.0, connect=4.0)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    def _validate_order_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").strip().upper()
        if sym not in self.REAL_ALLOWED_SYMBOLS:
            raise ValueError(
                f"🚨 [초과 Case 54 위반] 허용되지 않은 제3종목({sym}) 주문 API 호출이 물리적으로 차단되었습니다."
            )
        return sym

    def _get_headers(self, include_account: bool = False) -> dict:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if include_account:
            if not self.account_seq:
                raise RuntimeError("🚨 [제2헌법 위반] accountSeq가 아직 락온되지 않았습니다.")
            headers["X-Tossinvest-Account"] = str(self.account_seq)
        return headers

    async def _do_authenticate(self) -> str:
        """OAuth 2.0 Client Credentials 토큰 발급 (/oauth2/token, Case 62)."""
        await GlobalThrottle.wait_api_sync("AUTH", min_interval=0.25)
        session = await self._get_session()
        url = f"{self.base_url}/oauth2/token"
        form_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        async with session.post(url, data=form_data, headers=headers, timeout=10.0) as resp:
            status = resp.status
            try:
                data = await resp.json(content_type=None)
            except Exception:
                text = await resp.text()
                raise RuntimeError(f"토스 토큰 발급 JSON 파싱 실패 (HTTP {status}): {text[:200]}")

            if status != 200 or not isinstance(data, dict) or not data.get("access_token"):
                err_desc = (
                    data.get("error_description")
                    or data.get("error")
                    or str(data)
                    if isinstance(data, dict)
                    else str(data)
                )
                raise RuntimeError(f"토스 OAuth2 토큰 발급 실패 (HTTP {status}): {err_desc}")

            self.access_token = str(data["access_token"])
            expires_in = float(data.get("expires_in") or 86400.0)
            self.token_expires_at = time.time() + max(600.0, expires_in)
            return self.access_token

    async def get_token(self) -> str:
        if self.access_token and time.time() < (self.token_expires_at - 300.0):
            return self.access_token
        async with self._auth_lock:
            if self.access_token and time.time() < (self.token_expires_at - 300.0):
                return self.access_token
            return await self._do_authenticate()

    async def token_renewal_loop(self):
        """백그라운드 토큰 자동 갱신 파이프라인."""
        while True:
            try:
                await self.get_token()
                remain = max(60.0, (self.token_expires_at - time.time()) - 300.0)
                await asyncio.sleep(min(remain, 3600.0))
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"🚨 [토큰 자동 갱신 루프 방어] {e}", flush=True)
                await asyncio.sleep(30.0)

    async def _request(
        self,
        method: str,
        endpoint: str,
        group: str = "DEFAULT",
        headers: dict = None,
        json_data: dict = None,
        data: dict = None,
    ) -> dict:
        """
        제1헌법, Case 20, 21, 75: GlobalThrottle 속도 제한, 3단 지수 백오프,
        4xx 즉시 탈출(_NonRetryableApiError) 및 POST 빈 JSON 자동 주입.
        """
        await self.get_token()
        method_up = method.upper()
        url = f"{self.base_url}{endpoint}" if endpoint.startswith("/") else f"{self.base_url}/{endpoint}"

        # 초과 Case 75: POST / PUT / PATCH 호출 시 json_data가 None이면 빈 JSON 객체({})를 자동 주입하여 HTTP 415 원천 차단
        if method_up in ("POST", "PUT", "PATCH") and json_data is None and data is None:
            json_data = {}

        last_exc = None
        for attempt in range(3):
            await GlobalThrottle.wait_api_sync(group)
            req_headers = dict(headers) if headers is not None else self._get_headers()
            req_headers["Authorization"] = f"Bearer {self.access_token}"

            try:
                session = await self._get_session()
                async with session.request(
                    method_up,
                    url,
                    headers=req_headers,
                    json=json_data,
                    data=data,
                    timeout=10.0,
                ) as resp:
                    status = resp.status
                    if status == 204:
                        return {"result": {}}

                    try:
                        resp_json = await resp.json(content_type=None)
                    except Exception:
                        resp_text = await resp.text()
                        resp_json = {"raw_text": resp_text}

                    if 200 <= status < 300:
                        return resp_json if isinstance(resp_json, dict) else {"result": resp_json}

                    err_obj = (resp_json.get("error") if isinstance(resp_json, dict) else None) or {}
                    err_code = str(err_obj.get("code") or "") if isinstance(err_obj, dict) else ""
                    err_msg = str(err_obj.get("message") or resp_json) if isinstance(err_obj, dict) else str(resp_json)

                    if status == 401:
                        async with self._auth_lock:
                            await self._do_authenticate()
                        continue

                    # Case 21: 4xx 클라이언트 비즈니스 에러는 지수 백오프 대기 없이 즉시 예외 탈출
                    if status in (400, 403, 404, 409, 415, 422):
                        raise _NonRetryableApiError(f"HTTP {status} [{err_code}]: {err_msg}")

                    raise RuntimeError(f"HTTP {status} [{err_code}]: {err_msg}")

            except _NonRetryableApiError as nr_err:
                raise RuntimeError(str(nr_err)) from nr_err
            except Exception as e:
                last_exc = e
                if attempt < 2:
                    await asyncio.sleep(0.3 * (2 ** attempt))
                else:
                    raise last_exc

        raise last_exc if last_exc else RuntimeError("API 요청 알 수 없는 실패")

    async def fetch_account_seq(self) -> int:
        """제2헌법: 시스템 기동 시 /api/v1/accounts를 호출하여 종합매매(BROKERAGE) 계좌의 accountSeq를 락온합니다."""
        data = await self._request("GET", "/api/v1/accounts", "ACCOUNT", headers=self._get_headers(include_account=False))
        accounts = (data or {}).get("result") or []
        if not isinstance(accounts, list) or not accounts:
            raise RuntimeError("🚨 [제2헌법] 조회 가능한 토스증권 종합매매(BROKERAGE) 계좌가 없습니다.")

        for acc in accounts:
            if isinstance(acc, dict) and acc.get("accountType") == "BROKERAGE" and acc.get("accountSeq") is not None:
                self.account_seq = int(acc["accountSeq"])
                return self.account_seq

        first_acc = accounts[0]
        if isinstance(first_acc, dict) and first_acc.get("accountSeq") is not None:
            self.account_seq = int(first_acc["accountSeq"])
            return self.account_seq

        raise RuntimeError("🚨 [제2헌법] 유효한 accountSeq를 찾을 수 없습니다.")

    async def init_account(self) -> int:
        return await self.fetch_account_seq()

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
