# =====================================================================
# FILE: toss_api_core.py
# 목적: 토스증권 Open API 비동기 통신 코어 플러그인 (toss_api.py 1/2 분할본)
#       — GlobalThrottle 중앙 통제소, NYSE 정규 휴장일 한글 맵핑,
#         OAuth2 토큰 발급/자동 갱신 루프, 3단 지수 백오프 및 4xx 즉시 탈출(_request),
#         HTTP 415 빈 JSON 자동 주입 방어 및 BROKERAGE AccountSeq 인메모리 락온 베이스
# =====================================================================
# NEW: 제3헌법 및 플러그인 분할 아키텍처 의거 toss_api.py의 인증·스로틀·세션·계좌 락온 코어를 toss_api_core.py로 무손실 분리
# MODIFIED: 제1헌법 및 Case 20, 30 의거 GlobalThrottle API(0.15초) 및 파일 비동기 Lock 중앙 통제
# MODIFIED: 제2헌법 의거 /api/v1/accounts 조회 후 BROKERAGE accountSeq 인메모리 락온 및 헤더 원자적 주입
# MODIFIED: Case 09 & 초과 Case 54 — REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL") 외 제3종목 주문 물리적 차단
# MODIFIED: 초과 Case 75 — POST/PUT/PATCH 호출 시 빈 JSON(json_data={}) 자동 주입으로 HTTP 415 소각
# NEW: 초과 Case 91 — GlobalThrottle.active_client 자동 바인딩 지원

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
    """
    제1헌법 및 Case 20, 30, 91: API Rate Limit 방어(0.15초 간격), 파일별 비동기 뮤텍스 중앙 통제소
    및 MacroDataCache 전용 토스 클라이언트 브릿지(active_client).
    """
    _api_locks: dict[str, asyncio.Lock] = {}
    _last_call_times: dict[str, float] = {}
    _file_locks: dict[str, asyncio.Lock] = {}
    active_client = None

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


class TossApiClientCore:
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
        # NEW: 초과 Case 91 — MacroDataCache에서 토스 Open API QQQ 실시간 시세/1분봉을 조회할 수 있도록 자동 바인딩
        GlobalThrottle.active_client = self

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
