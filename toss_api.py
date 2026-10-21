# -*- coding: utf-8 -*-
"""
# NEW: toss_api.py
토스증권 Open API 방탄 통신 코어 및 중앙 스로틀링/파일락 통제 모듈 (Step 01 / 10)
- 제1헌법: 모든 동기 HTTP/파일 I/O의 asyncio.to_thread 비동기 격리 및 GlobalThrottle 통제
- 제2헌법: /api/v1/accounts 기반 BROKERAGE accountSeq 메모리 락온 및 X-Tossinvest-Account 주입
- 제4헌법: 모든 시간 판단 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: 주문 생성 시 36자 이내 고유 clientOrderId 주입 및 동일 키 재시도 멱등성 보장
- 제6헌법: /api/v1/market-calendar/US 30초 인메모리 캐싱 및 평일 강제 개장 Fail-Open 폴백
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger("toss_sniper.toss_api")

# ---------------------------------------------------------------------------
# 전역 타임존 및 화이트리스트 상수 (제4헌법 & Case 08, Case 09)
# ---------------------------------------------------------------------------
EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 스나이퍼 실거래 주문 허용 화이트리스트 (오직 SOXS, SOXL 2종목만 허용)
REAL_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXS", "SOXL")
# 읽기 전용 시세/캔들 조회 허용 화이트리스트 (매크로 3차 방어용 QQQ 포함)
READONLY_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXS", "SOXL", "QQQ")


# ---------------------------------------------------------------------------
# 4xx 클라이언트 에러 즉시 탈출 예외 클래스 (Case 21)
# ---------------------------------------------------------------------------
class _NonRetryableApiError(RuntimeError):
    """
    400, 403, 404, 409, 415, 422 등 재시도해도 결과가 동일한 4xx 에러 발생 시
    지수 백오프를 거치지 않고 즉시 탈출하기 위한 전용 예외 클래스.
    """

    def __init__(
        self,
        status_code: int,
        error_code: str,
        error_message: str,
        response_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = int(status_code)
        self.error_code = str(error_code or "unknown-error")
        self.error_message = str(error_message or "")
        self.response_data = response_data if isinstance(response_data, dict) else {}
        super().__init__(
            f"[HTTP {self.status_code}] {self.error_code}: {self.error_message}"
        )


# ---------------------------------------------------------------------------
# 동기(with) 및 비동기(async with) 양방향 호환 하이브리드 파일 락 (Case 06, 30)
# ---------------------------------------------------------------------------
class _HybridFileLock:
    """
    동기 컨텍스트(with lock:)와 비동기 컨텍스트(async with lock:) 양쪽에서
    동일하게 사용할 수 있는 스레드 안전 재진입(RLock) 기반 하이브리드 락.
    """

    def __init__(self, filepath: str) -> None:
        self.filepath = str(filepath)
        self._rlock = threading.RLock()

    def acquire_sync(self) -> None:
        self._rlock.acquire()

    def release_sync(self) -> None:
        try:
            self._rlock.release()
        except RuntimeError:
            pass

    def __enter__(self) -> "_HybridFileLock":
        self._rlock.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release_sync()

    async def __aenter__(self) -> "_HybridFileLock":
        await asyncio.to_thread(self._rlock.acquire)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release_sync()


# ---------------------------------------------------------------------------
# 제1헌법 : 중앙 API 스로틀러 및 전역 파일 락 매니저 (GlobalThrottle)
# ---------------------------------------------------------------------------
class GlobalThrottle:
    """
    토스증권 Open API Rate Limits 그룹별 최소 호출 간격 통제 및
    파일 경로별 _HybridFileLock 싱글톤 레지스트리.
    """

    _api_lock = threading.Lock()
    _last_call_ts: Dict[str, float] = {}

    # API.txt Rate Limits Group 별 안전 호출 간격(초)
    _GROUP_INTERVALS: Dict[str, float] = {
        "AUTH": 1.00,
        "ACCOUNT": 0.25,
        "ASSET": 0.18,
        "MARKET_DATA": 0.12,
        "MARKET_DATA_CHART": 0.22,
        "MARKET_INFO": 0.30,
        "ORDER": 0.12,
        "ORDER_HISTORY": 0.18,
        "ORDER_INFO": 0.18,
        "DEFAULT": 0.15,
    }

    _file_locks_guard = threading.Lock()
    _file_locks: Dict[str, _HybridFileLock] = {}

    @classmethod
    def wait_api_sync(cls, group: str = "DEFAULT") -> None:
        """
        동기 스레드 내부에서 API 호출 직전 그룹별 최소 간격을 보장한다.
        """
        grp = str(group or "DEFAULT").upper()
        min_interval = cls._GROUP_INTERVALS.get(grp, cls._GROUP_INTERVALS["DEFAULT"])
        sleep_needed = 0.0
        with cls._api_lock:
            now = time.monotonic()
            last_ts = cls._last_call_ts.get(grp, 0.0)
            elapsed = now - last_ts
            if elapsed < min_interval:
                sleep_needed = min_interval - elapsed
                cls._last_call_ts[grp] = now + sleep_needed
            else:
                cls._last_call_ts[grp] = now
        if sleep_needed > 0.0:
            time.sleep(sleep_needed)

    @classmethod
    async def wait_api_async(cls, group: str = "DEFAULT") -> None:
        """
        비동기 루프에서 GlobalThrottle 대기를 수행할 때 사용한다.
        """
        await asyncio.to_thread(cls.wait_api_sync, group)

    @classmethod
    def get_file_lock(cls, filepath: str) -> _HybridFileLock:
        """
        파일 절대경로 기준 단일 _HybridFileLock 인스턴스를 반환한다.
        """
        norm_path = os.path.abspath(str(filepath or "default.lock"))
        with cls._file_locks_guard:
            lock_obj = cls._file_locks.get(norm_path)
            if lock_obj is None:
                lock_obj = _HybridFileLock(norm_path)
                cls._file_locks[norm_path] = lock_obj
            return lock_obj


# ---------------------------------------------------------------------------
# 유틸리티 : 멱등성 키 생성 (제5헌법) 및 안전 형변환 / 가격 포맷팅
# ---------------------------------------------------------------------------
def generate_client_order_id(prefix: str = "SNP") -> str:
    """
    제5헌법 & API.txt OrderCreateRequest.clientOrderId 규격:
    최대 36자, 영숫자 및 '-', '_' 만 허용.
    """
    clean_prefix = "".join(
        ch for ch in str(prefix or "SNP") if ch.isalnum() or ch in ("-", "_")
    )[:8]
    if not clean_prefix:
        clean_prefix = "SNP"
    ts_part = datetime.now(EST_TZ).strftime("%m%d%H%M%S")
    rand_part = uuid.uuid4().hex[:12]
    cid = f"{clean_prefix}-{ts_part}-{rand_part}"
    return cid[:36]


def safe_float(val: Any, default: float = 0.0) -> float:
    """
    Case 03 & Case 04: None, 빈 문자열, NaN, inf 방어용 안전 float 변환기.
    """
    if val is None or isinstance(val, bool):
        return default
    try:
        res = float(str(val).replace(",", "").strip())
        if math.isnan(res) or math.isinf(res):
            return default
        return res
    except (ValueError, TypeError):
        return default


def safe_int(val: Any, default: int = 0) -> int:
    """
    안전 int 변환기 (소수점 문자열 입력 시 버림 정수 변환).
    """
    f_val = safe_float(val, float(default))
    try:
        return int(math.floor(f_val + 1e-9))
    except (ValueError, OverflowError):
        return default


def format_us_order_price(price: float) -> str:
    """
    API.txt 주문 가격 규격:
    - US $1 이상: 소수점 둘째 자리까지 (절삭/표준화)
    - US $1 미만: 소수점 넷째 자리까지
    """
    p = safe_float(price, 0.0)
    if p <= 0.0:
        raise ValueError(f"유효하지 않은 미국 주식 주문 단가: {price}")
    if p >= 1.0:
        rounded = round(p + 1e-9, 2)
        return f"{rounded:.2f}"
    rounded = round(p + 1e-9, 4)
    return f"{rounded:.4f}"


def is_daymarket_buy_blocked_window(now_est: Optional[datetime] = None) -> bool:
    """
    🎯 1 & Case 60, Case 77:
    데이장(19:00:00 ~ 03:57:59 EST)은 100% [모의] 전용 구간이므로
    실계좌 신규 매수(BUY) 주문 발사를 물리적으로 100% 차단한다.
    (03:58:00 EST부터는 1주 휩소 노출 작전 실전 매수가 허용됨)
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    hm = dt.hour * 100 + dt.minute
    if hm >= 1900 or hm <= 357:
        return True
    return False


# ---------------------------------------------------------------------------
# 제2헌법 ~ 제6헌법 : 토스증권 Open API 방탄 클라이언트 (TossInvestAPI)
# ---------------------------------------------------------------------------
class TossInvestAPI:
    """
    토스증권 Open API (v1.2.14) 비동기 방탄 클라이언트.
    - 모든 네트워크 I/O는 asyncio.to_thread + asyncio.wait_for 로 격리
    - OAuth2 토큰 자동 발급/갱신 및 X-Tossinvest-Account 헤더 원자적 주입
    - 3단 지수 백오프 및 4xx 즉시 예외 탈출(_NonRetryableApiError)
    """

    BASE_URL = "https://openapi.tossinvest.com"

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        request_timeout: float = 8.0,
    ) -> None:
        self.client_id = str(
            client_id or os.getenv("TOSS_CLIENT_ID", "") or ""
        ).strip()
        self.client_secret = str(
            client_secret or os.getenv("TOSS_CLIENT_SECRET", "") or ""
        ).strip()
        self.base_url = str(base_url or os.getenv("TOSS_API_BASE_URL", self.BASE_URL)).rstrip("/")
        self.request_timeout = float(request_timeout)

        self._session = requests.Session()
        self._token_lock = threading.RLock()
        self._access_token: str = ""
        self._token_expires_at: float = 0.0

        # 제2헌법: 종합매매(BROKERAGE) 계좌 accountSeq 메모리 락온
        self._account_seq: Optional[int] = None
        self._account_no: str = ""

        # Case 33: 미국 시장 캘린더 30초 인메모리 캐시
        self._calendar_cache_lock = threading.Lock()
        self._calendar_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

        # 에러 알림 중복 방지용 최근 에러 기록
        self.last_api_error_msg: str = ""
        self.last_api_error_ts: float = 0.0

    # -----------------------------------------------------------------------
    # 내부 동기 HTTP 호출 및 OAuth2 토큰 / AccountSeq 관리
    # -----------------------------------------------------------------------
    def _issue_token_sync(self, force: bool = False) -> str:
        """
        POST /oauth2/token (application/x-www-form-urlencoded)
        OAuth2 Client Credentials Grant 토큰 발급.
        """
        with self._token_lock:
            now_mono = time.monotonic()
            if (
                not force
                and self._access_token
                and now_mono < (self._token_expires_at - 60.0)
            ):
                return self._access_token

            if not self.client_id or not self.client_secret:
                raise _NonRetryableApiError(
                    401,
                    "missing-credentials",
                    "TOSS_CLIENT_ID 또는 TOSS_CLIENT_SECRET 환경변수가 설정되지 않았습니다.",
                )

            GlobalThrottle.wait_api_sync("AUTH")
            url = f"{self.base_url}/oauth2/token"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            resp = self._session.post(
                url,
                data=payload,
                headers=headers,
                timeout=self.request_timeout,
            )
            if resp.status_code != 200:
                try:
                    err_json = resp.json()
                except Exception:
                    err_json = {}
                err_code = str(
                    err_json.get("error") or f"http-{resp.status_code}"
                )
                err_desc = str(
                    err_json.get("error_description") or resp.text[:200]
                )
                raise _NonRetryableApiError(
                    resp.status_code, err_code, err_desc, err_json
                )

            data = resp.json()
            token = str(data.get("access_token") or "").strip()
            expires_in = safe_float(data.get("expires_in"), 86400.0)
            if not token:
                raise RuntimeError("토스 OAuth2 응답에 access_token 이 존재하지 않습니다.")

            self._access_token = token
            self._token_expires_at = time.monotonic() + max(expires_in, 300.0)
            return self._access_token

    def _parse_error_response(
        self, resp: requests.Response
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        API.txt ErrorResponse 스키마(error.code, error.message, error.data) 파싱.
        """
        try:
            body = resp.json()
        except Exception:
            body = {}
        if isinstance(body, dict):
            err_obj = body.get("error")
            if isinstance(err_obj, dict):
                code = str(err_obj.get("code") or f"http-{resp.status_code}")
                msg = str(err_obj.get("message") or "")
                data = (
                    err_obj.get("data")
                    if isinstance(err_obj.get("data"), dict)
                    else {}
                )
                return code, msg, data
            if isinstance(err_obj, str):
                return err_obj, str(body.get("error_description") or ""), body
        return f"http-{resp.status_code}", resp.text[:200], {}

    def _request_sync(
        self,
        method: str,
        path: str,
        *,
        group: str = "DEFAULT",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_account: bool = False,
        timeout: Optional[float] = None,
    ) -> Any:
        """
        동기 HTTP 요청 실행 코어:
        - GlobalThrottle.wait_api_sync(group) 강제
        - require_account=True 시 X-Tossinvest-Account 헤더 원자적 주입
        - 401 발생 시 토큰 1회 강제 재발급 후 재시도
        - 400/403/404/409/415/422 발생 시 백오프 없이 즉시 _NonRetryableApiError 발생 (Case 21)
        - 429/5xx/네트워크 타임아웃 시 3단 지수 백오프(0.4s -> 0.8s -> 1.6s) 재시도
        """
        http_method = str(method or "GET").upper()
        url = f"{self.base_url}{path}"
        eff_timeout = float(timeout or self.request_timeout)
        backoffs = (0.4, 0.8, 1.6)
        last_exc: Optional[Exception] = None

        for attempt in range(len(backoffs) + 1):
            try:
                token = self._issue_token_sync(force=False)
                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
                if require_account:
                    if self._account_seq is None:
                        self._ensure_account_seq_sync()
                    headers["X-Tossinvest-Account"] = str(self._account_seq)

                if json_body is not None:
                    headers["Content-Type"] = "application/json"

                GlobalThrottle.wait_api_sync(group)

                resp = self._session.request(
                    method=http_method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=eff_timeout,
                )

                if resp.status_code == 204:
                    return {}

                if resp.status_code == 200:
                    payload = resp.json()
                    if isinstance(payload, dict) and "result" in payload:
                        return payload["result"]
                    return payload

                # 401 Unauthorized -> 토큰 재발급 후 1회 즉시 재시도
                if resp.status_code == 401 and attempt == 0:
                    self._issue_token_sync(force=True)
                    continue

                err_code, err_msg, err_data = self._parse_error_response(resp)
                self.last_api_error_msg = f"[{resp.status_code}] {err_code}: {err_msg}"
                self.last_api_error_ts = time.time()

                # Case 21: 4xx 클라이언트 에러는 지수 백오프 없이 즉시 탈출
                if resp.status_code in (400, 403, 404, 409, 415, 422):
                    raise _NonRetryableApiError(
                        resp.status_code, err_code, err_msg, err_data
                    )

                # 429 RateLimitExceeded 또는 5xx 서버 오류는 백오프 재시도
                if resp.status_code == 429:
                    retry_after = safe_float(
                        resp.headers.get("Retry-After"), backoffs[min(attempt, 2)]
                    )
                    if attempt < len(backoffs):
                        time.sleep(max(retry_after, 0.4))
                        continue

                raise RuntimeError(
                    f"API 호출 실패 ({http_method} {path}) -> [{resp.status_code}] {err_code}: {err_msg}"
                )

            except _NonRetryableApiError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < len(backoffs):
                    time.sleep(backoffs[attempt])
                else:
                    break

        raise RuntimeError(
            f"API 3단 재시도 최종 실패 ({http_method} {path}): {last_exc}"
        )

    async def _request_async(
        self,
        method: str,
        path: str,
        *,
        group: str = "DEFAULT",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_account: bool = False,
        timeout: float = 12.0,
    ) -> Any:
        """
        제1헌법: 모든 외부 통신을 asyncio.to_thread 로 격리하고 wait_for 타임아웃 족쇄를 채운다.
        """
        return await asyncio.wait_for(
            asyncio.to_thread(
                self._request_sync,
                method,
                path,
                group=group,
                params=params,
                json_body=json_body,
                require_account=require_account,
                timeout=min(self.request_timeout, max(timeout - 1.0, 3.0)),
            ),
            timeout=timeout,
        )

    # -----------------------------------------------------------------------
    # 제2헌법 : 종합매매(BROKERAGE) 계좌 AccountSeq 락온
    # -----------------------------------------------------------------------
    def _ensure_account_seq_sync(self) -> int:
        if self._account_seq is not None:
            return self._account_seq

        result = self._request_sync(
            "GET", "/api/v1/accounts", group="ACCOUNT", require_account=False
        )
        accounts = result if isinstance(result, list) else []
        selected: Optional[Dict[str, Any]] = None
        for acc in accounts:
            if isinstance(acc, dict) and str(acc.get("accountType")) == "BROKERAGE":
                selected = acc
                break
        if selected is None and accounts and isinstance(accounts[0], dict):
            selected = accounts[0]

        if not selected or selected.get("accountSeq") is None:
            raise _NonRetryableApiError(
                404,
                "account-not-found",
                "토스증권 종합매매(BROKERAGE) 계좌(accountSeq)를 찾을 수 없습니다.",
            )

        self._account_seq = int(selected["accountSeq"])
        self._account_no = str(selected.get("accountNo") or "")
        logger.info(
            "🔒 [제2헌법] 토스증권 AccountSeq 락온 완료: accountSeq=%s (accountNo=%s)",
            self._account_seq,
            self._account_no,
        )
        return self._account_seq

    async def ensure_account_seq(self) -> int:
        """
        시스템 기동 시 1회 호출하여 종합매매 계좌의 accountSeq 를 메모리에 락온한다.
        """
        if self._account_seq is not None:
            return self._account_seq
        return await asyncio.wait_for(
            asyncio.to_thread(self._ensure_account_seq_sync), timeout=10.0
        )

    @property
    def account_seq(self) -> Optional[int]:
        return self._account_seq

    # -----------------------------------------------------------------------
    # 시세 / 호가 / 1분봉·일봉 차트 조회 (Market Data & Chart)
    # -----------------------------------------------------------------------
    async def get_current_prices_batch(
        self, symbols: Iterable[str] = ("SOXS", "SOXL", "QQQ")
    ) -> Dict[str, float]:
        """
        GET /api/v1/prices?symbols=SOXS,SOXL,QQQ (Rate Limits Group: MARKET_DATA)
        단일 배치 호출로 SOXS, SOXL, QQQ 실시간 현재가를 0.0ms 시차로 동시 조회한다.
        """
        valid_syms = [
            str(s).upper().strip()
            for s in symbols
            if str(s).upper().strip() in READONLY_ALLOWED_SYMBOLS
        ]
        if not valid_syms:
            valid_syms = ["SOXS", "SOXL", "QQQ"]

        out: Dict[str, float] = {sym: 0.0 for sym in valid_syms}
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/prices",
                group="MARKET_DATA",
                params={"symbols": ",".join(valid_syms)},
                require_account=False,
                timeout=8.0,
            )
            items = res if isinstance(res, list) else []
            for item in items:
                if isinstance(item, dict):
                    sym = str(item.get("symbol") or "").upper().strip()
                    lp = safe_float(item.get("lastPrice"), 0.0)
                    if sym in out and lp > 0.0:
                        out[sym] = lp
        except Exception as exc:
            logger.warning("⚠️ 배치 현재가 조회 실패 (%s): %s", valid_syms, exc)
        return out

    async def get_current_price(self, symbol: str) -> float:
        sym = str(symbol or "").upper().strip()
        if sym not in READONLY_ALLOWED_SYMBOLS:
            raise ValueError(f"허용되지 않은 시세 조회 심볼: {sym}")
        batch = await self.get_current_prices_batch((sym,))
        return safe_float(batch.get(sym), 0.0)

    async def get_orderbook(self, symbol: str) -> Dict[str, Any]:
        """
        GET /api/v1/orderbook?symbol=... (Rate Limits Group: MARKET_DATA)
        매수 1호가(bid1) 스윕 및 호가 잔량 조회를 수행한다.
        """
        sym = str(symbol or "").upper().strip()
        if sym not in READONLY_ALLOWED_SYMBOLS:
            raise ValueError(f"허용되지 않은 호가 조회 심볼: {sym}")

        info: Dict[str, Any] = {
            "symbol": sym,
            "bid1": 0.0,
            "ask1": 0.0,
            "bids": [],
            "asks": [],
        }
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/orderbook",
                group="MARKET_DATA",
                params={"symbol": sym},
                require_account=False,
                timeout=8.0,
            )
            if isinstance(res, dict):
                bids = res.get("bids") if isinstance(res.get("bids"), list) else []
                asks = res.get("asks") if isinstance(res.get("asks"), list) else []
                info["bids"] = bids
                info["asks"] = asks
                if bids and isinstance(bids[0], dict):
                    info["bid1"] = safe_float(bids[0].get("price"), 0.0)
                if asks and isinstance(asks[0], dict):
                    info["ask1"] = safe_float(asks[0].get("price"), 0.0)
        except Exception as exc:
            logger.warning("⚠️ 호가창 조회 실패 (%s): %s", sym, exc)
        return info

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1m",
        count: int = 200,
        before: Optional[str] = None,
        adjusted: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        GET /api/v1/candles (Rate Limits Group: MARKET_DATA_CHART)
        단일 페이지(최대 200봉) 캔들 목록과 nextBefore 커서를 반환한다.
        """
        sym = str(symbol or "").upper().strip()
        if sym not in READONLY_ALLOWED_SYMBOLS:
            raise ValueError(f"허용되지 않은 캔들 조회 심볼: {sym}")

        params: Dict[str, Any] = {
            "symbol": sym,
            "interval": str(interval or "1m"),
            "count": max(1, min(int(count), 200)),
            "adjusted": "true" if adjusted else "false",
        }
        if before:
            params["before"] = str(before)

        res = await self._request_async(
            "GET",
            "/api/v1/candles",
            group="MARKET_DATA_CHART",
            params=params,
            require_account=False,
            timeout=10.0,
        )
        if not isinstance(res, dict):
            return [], None
        candles = res.get("candles") if isinstance(res.get("candles"), list) else []
        next_before = (
            str(res.get("nextBefore"))
            if isinstance(res.get("nextBefore"), str) and res.get("nextBefore")
            else None
        )
        return candles, next_before

    async def get_candles_multi_page(
        self,
        symbol: str,
        interval: str = "1m",
        total_count: int = 400,
    ) -> List[Dict[str, Any]]:
        """
        🎯 3: 다중 페이지(최대 400분봉) 조회로 19:00 EST 데이장 및 04:00 EST 프리장
        개장 첫 봉부터 누락 없는 완전한 시계열을 확보한다.
        """
        target = max(1, min(int(total_count), 600))
        collected: List[Dict[str, Any]] = []
        seen_ts = set()
        cursor_before: Optional[str] = None
        max_pages = math.ceil(target / 200.0)

        for _ in range(max_pages):
            req_cnt = min(200, target - len(collected) + 5)
            try:
                page_candles, next_before = await self.get_candles(
                    symbol=symbol,
                    interval=interval,
                    count=req_cnt,
                    before=cursor_before,
                    adjusted=True,
                )
            except Exception as exc:
                logger.warning("⚠️ 다중 페이지 캔들 조회 중단 (%s): %s", symbol, exc)
                break

            if not page_candles:
                break

            for c in page_candles:
                if isinstance(c, dict):
                    ts_str = str(c.get("timestamp") or "")
                    if ts_str and ts_str not in seen_ts:
                        seen_ts.add(ts_str)
                        collected.append(c)

            if not next_before or len(collected) >= target:
                break
            cursor_before = next_before

        return collected[:target]

    # -----------------------------------------------------------------------
    # 계좌 잔고 / 매수 가능 금액 / 매도 가능 수량 조회 (Asset & Order Info)
    # -----------------------------------------------------------------------
    async def get_holdings_all(self) -> Dict[str, Dict[str, Any]]:
        """
        GET /api/v1/holdings (Rate Limits Group: ASSET)
        Case 03: krw / usd 분리 객체의 null 체크 및 0.0 강제 형변환을 수행하고,
        SOXS 와 SOXL 의 실보유 수량(qty) 및 팩트 평단가(avg_price)를 반환한다.
        """
        default_item = lambda s: {
            "symbol": s,
            "qty": 0,
            "avg_price": 0.0,
            "last_price": 0.0,
            "purchase_amount": 0.0,
            "eval_amount": 0.0,
            "pnl_amount": 0.0,
            "pnl_rate_pct": 0.0,
        }
        out: Dict[str, Dict[str, Any]] = {
            "SOXS": default_item("SOXS"),
            "SOXL": default_item("SOXL"),
        }
        res = await self._request_async(
            "GET",
            "/api/v1/holdings",
            group="ASSET",
            require_account=True,
            timeout=10.0,
        )
        if not isinstance(res, dict):
            return out

        items = res.get("items") if isinstance(res.get("items"), list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or "").upper().strip()
            if sym not in REAL_ALLOWED_SYMBOLS:
                continue
            qty = safe_int(item.get("quantity"), 0)
            avg_p = safe_float(item.get("averagePurchasePrice"), 0.0)
            last_p = safe_float(item.get("lastPrice"), 0.0)
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
            out[sym] = {
                "symbol": sym,
                "qty": max(0, qty),
                "avg_price": max(0.0, avg_p),
                "last_price": max(0.0, last_p),
                "purchase_amount": safe_float(mv_obj.get("purchaseAmount"), 0.0),
                "eval_amount": safe_float(mv_obj.get("amount"), 0.0),
                "pnl_amount": safe_float(pl_obj.get("amount"), 0.0),
                "pnl_rate_pct": safe_float(pl_obj.get("rate"), 0.0) * 100.0,
            }
        return out

    async def get_buying_power_usd(self, safety_buffer_pct: float = 0.5) -> float:
        """
        GET /api/v1/buying-power?currency=USD (Rate Limits Group: ORDER_INFO)
        Case 65: USD 현금 매수 가능 금액 조회 후 0.5% 안전 버퍼를 차감하여 반환한다.
        """
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/buying-power",
                group="ORDER_INFO",
                params={"currency": "USD"},
                require_account=True,
                timeout=8.0,
            )
            if isinstance(res, dict):
                raw_bp = safe_float(res.get("cashBuyingPower"), 0.0)
                buffer_mult = max(0.0, 1.0 - (safe_float(safety_buffer_pct, 0.5) / 100.0))
                return math.floor((raw_bp * buffer_mult) * 100.0) / 100.0
        except Exception as exc:
            logger.warning("⚠️ USD 매수가능금액 조회 실패: %s", exc)
        return 0.0

    async def get_sellable_quantity(self, symbol: str) -> int:
        """
        GET /api/v1/sellable-quantity?symbol=... (Rate Limits Group: ORDER_INFO)
        """
        sym = str(symbol or "").upper().strip()
        if sym not in REAL_ALLOWED_SYMBOLS:
            return 0
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/sellable-quantity",
                group="ORDER_INFO",
                params={"symbol": sym},
                require_account=True,
                timeout=8.0,
            )
            if isinstance(res, dict):
                return max(0, safe_int(res.get("sellableQuantity"), 0))
        except Exception as exc:
            logger.warning("⚠️ 매도가능수량 조회 실패 (%s): %s", sym, exc)
        return 0

    # -----------------------------------------------------------------------
    # 주문 내역 조회 / 지정가 주문 생성 / US 정정 / 빈 JSON 취소 (Order & History)
    # -----------------------------------------------------------------------
    async def get_orders(
        self,
        status: str = "OPEN",
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        GET /api/v1/orders (Rate Limits Group: ORDER_HISTORY)
        status: 'OPEN' 또는 'CLOSED'
        """
        st = str(status or "OPEN").upper().strip()
        if st not in ("OPEN", "CLOSED"):
            st = "OPEN"
        params: Dict[str, Any] = {"status": st}
        if symbol:
            params["symbol"] = str(symbol).upper().strip()
        if st == "CLOSED":
            params["limit"] = max(1, min(int(limit), 100))

        try:
            res = await self._request_async(
                "GET",
                "/api/v1/orders",
                group="ORDER_HISTORY",
                params=params,
                require_account=True,
                timeout=10.0,
            )
            if isinstance(res, dict) and isinstance(res.get("orders"), list):
                return res["orders"]
        except Exception as exc:
            logger.warning("⚠️ 주문 목록 조회 실패 (status=%s, symbol=%s): %s", st, symbol, exc)
        return []

    async def get_order_detail(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        GET /api/v1/orders/{orderId} (Rate Limits Group: ORDER_HISTORY)
        """
        oid = str(order_id or "").strip()
        if not oid:
            return None
        try:
            encoded_oid = urllib.parse.quote(oid, safe="")
            res = await self._request_async(
                "GET",
                f"/api/v1/orders/{encoded_oid}",
                group="ORDER_HISTORY",
                require_account=True,
                timeout=8.0,
            )
            return res if isinstance(res, dict) else None
        except _NonRetryableApiError as exc:
            if exc.status_code == 404:
                return None
            raise
        except Exception as exc:
            logger.warning("⚠️ 주문 상세 조회 실패 (orderId=%s): %s", oid, exc)
            return None

    async def create_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        order_type: str = "LIMIT",
        time_in_force: str = "DAY",
        client_order_id: Optional[str] = None,
        enforce_daymarket_block: bool = True,
    ) -> Dict[str, Any]:
        """
        POST /api/v1/orders (Rate Limits Group: ORDER)
        - Case 09: 오직 SOXS, SOXL 만 허용
        - Case 14: 지정가(LIMIT) 전용 통제망
        - Case 60 & 77: 데이장(19:00 ~ 03:57 EST) 실거래 매수(BUY) 100% 물리적 차단
        - 제5헌법: 36자 이내 고유 clientOrderId 주입
        """
        sym = str(symbol or "").upper().strip()
        if sym not in REAL_ALLOWED_SYMBOLS:
            raise _NonRetryableApiError(
                400,
                "symbol-not-allowed",
                f"스나이퍼 허용 종목({REAL_ALLOWED_SYMBOLS})이 아닙니다: {sym}",
            )

        ord_side = str(side or "").upper().strip()
        if ord_side not in ("BUY", "SELL"):
            raise ValueError(f"유효하지 않은 주문 방향: {ord_side}")

        # 🎯 1: 데이장(19:00 ~ 03:57 EST) 실거래 매수 100% 물리적 차단
        if (
            ord_side == "BUY"
            and enforce_daymarket_block
            and is_daymarket_buy_blocked_window()
        ):
            raise _NonRetryableApiError(
                422,
                "daymarket-buy-physically-blocked",
                "데이장(19:00 ~ 03:57 EST)은 100% [모의] 전용 구간으로 실계좌 매수가 물리적으로 차단됩니다.",
            )

        qty_int = safe_int(quantity, 0)
        if qty_int <= 0:
            raise ValueError(f"주문 수량은 1주 이상의 정수여야 합니다: {quantity}")

        ord_type = str(order_type or "LIMIT").upper().strip()
        if ord_type != "LIMIT":
            ord_type = "LIMIT"

        tif = str(time_in_force or "DAY").upper().strip()
        if tif not in ("DAY", "CLS"):
            tif = "DAY"

        formatted_price = format_us_order_price(price)
        cid = (
            str(client_order_id).strip()[:36]
            if client_order_id
            else generate_client_order_id(prefix=f"{sym}{ord_side[:1]}")
        )

        body: Dict[str, Any] = {
            "clientOrderId": cid,
            "symbol": sym,
            "side": ord_side,
            "orderType": ord_type,
            "timeInForce": tif,
            "quantity": str(qty_int),
            "price": formatted_price,
        }

        res = await self._request_async(
            "POST",
            "/api/v1/orders",
            group="ORDER",
            json_body=body,
            require_account=True,
            timeout=10.0,
        )
        if not isinstance(res, dict):
            return {"orderId": "", "clientOrderId": cid}
        return {
            "orderId": str(res.get("orderId") or ""),
            "clientOrderId": str(res.get("clientOrderId") or cid),
            "price": safe_float(formatted_price, price),
            "quantity": qty_int,
        }

    async def modify_order(
        self,
        order_id: str,
        new_price: float,
        order_type: str = "LIMIT",
        symbol: Optional[str] = None,
        side: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST /api/v1/orders/{orderId}/modify (Rate Limits Group: ORDER)
        - Case 74: 미국 주식 주문 정정 시 quantity 파라미터를 100% 배제 (가격만 정정)
        - Case 24: 409 already-modified 발생 시 최신 OPEN 주문 ID를 자동 추적하여 정정 완수
        """
        oid = str(order_id or "").strip()
        if not oid:
            raise ValueError("정정할 order_id 가 비어 있습니다.")

        formatted_price = format_us_order_price(new_price)
        body: Dict[str, Any] = {
            "orderType": str(order_type or "LIMIT").upper().strip(),
            "price": formatted_price,
        }

        try:
            encoded_oid = urllib.parse.quote(oid, safe="")
            res = await self._request_async(
                "POST",
                f"/api/v1/orders/{encoded_oid}/modify",
                group="ORDER",
                json_body=body,
                require_account=True,
                timeout=8.0,
            )
            new_oid = (
                str(res.get("orderId") or oid) if isinstance(res, dict) else oid
            )
            return {
                "orderId": new_oid,
                "price": safe_float(formatted_price, new_price),
                " tracked_from_409": False,
            }
        except _NonRetryableApiError as exc:
            # 409 already-modified 발생 시 해당 종목의 현재 OPEN 주문을 추적하여 재정정
            if exc.status_code == 409 and exc.error_code == "already-modified" and symbol:
                open_orders = await self.get_orders(status="OPEN", symbol=symbol)
                target_side = str(side or "").upper().strip()
                for o in open_orders:
                    if not isinstance(o, dict):
                        continue
                    cand_id = str(o.get("orderId") or "").strip()
                    cand_side = str(o.get("side") or "").upper().strip()
                    if cand_id and cand_id != oid and (not target_side or cand_side == target_side):
                        encoded_cand = urllib.parse.quote(cand_id, safe="")
                        res2 = await self._request_async(
                            "POST",
                            f"/api/v1/orders/{encoded_cand}/modify",
                            group="ORDER",
                            json_body=body,
                            require_account=True,
                            timeout=8.0,
                        )
                        final_oid = (
                            str(res2.get("orderId") or cand_id)
                            if isinstance(res2, dict)
                            else cand_id
                        )
                        return {
                            "orderId": final_oid,
                            "price": safe_float(formatted_price, new_price),
                            "tracked_from_409": True,
                        }
            raise

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        POST /api/v1/orders/{orderId}/cancel (Rate Limits Group: ORDER)
        Case 75: HTTP 415 방지를 위해 반드시 빈 JSON 객체({})를 json_body 로 주입한다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return {"orderId": "", "status": "EMPTY_ID"}

        try:
            encoded_oid = urllib.parse.quote(oid, safe="")
            res = await self._request_async(
                "POST",
                f"/api/v1/orders/{encoded_oid}/cancel",
                group="ORDER",
                json_body={},
                require_account=True,
                timeout=8.0,
            )
            canceled_oid = (
                str(res.get("orderId") or oid) if isinstance(res, dict) else oid
            )
            return {"orderId": canceled_oid, "status": "CANCELED"}
        except _NonRetryableApiError as exc:
            if exc.status_code in (404, 409):
                return {
                    "orderId": oid,
                    "status": exc.error_code.upper(),
                    "detail": exc.error_message,
                }
            raise

    # -----------------------------------------------------------------------
    # Case 24 : 2단계 방탄 긴급/익절/탈출/MOC 매도 스윕 (modify_order -> 선취소 후 재발사)
    # -----------------------------------------------------------------------
    async def sweep_sell_position_bulletproof(
        self,
        symbol: str,
        holdings_qty: int = 1,
        sell_order_id: str = "",
        fallback_price: float = 0.0,
        min_floor_price: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Case 24 방탄 청산망:
        1) 실시간 호가창(/api/v1/orderbook)에서 매수 1호가(bid1)를 조회하고,
           min_floor_price 가 지정된 경우 max(bid1, min_floor_price)를 스윕 단가로 산출한다.
        2) [1단계] 기존 sell_order_id 가 존재하면 modify_order 로 0.1초 즉시 가격 정정을 시도한다.
        3) [2단계] 정정 실패 또는 sell_order_id 부재 시 해당 종목의 모든 OPEN 매도 주문을 선취소하고,
           sellableQuantity 를 최대 2.0초간 폴링 확인한 뒤 즉시 신규 LIMIT 매도 주문을 발사한다.
        """
        sym = str(symbol or "").upper().strip()
        ob = await self.get_orderbook(sym)
        bid1 = safe_float(ob.get("bid1"), 0.0)
        base_p = bid1 if bid1 > 0.0 else safe_float(fallback_price, 0.0)
        if base_p <= 0.0:
            base_p = await self.get_current_price(sym)
        if base_p <= 0.0:
            raise RuntimeError(f"[{sym}] 매도 스윕 단가 산출 불가 (현재가/호가 0.0)")

        sweep_price = math.floor(base_p * 100.0) / 100.0
        if min_floor_price > 0.0:
            sweep_price = max(sweep_price, math.ceil(min_floor_price * 100.0) / 100.0)

        # [1단계] 기장전된 매도 주문이 있으면 modify_order 즉시 정정 시도
        if sell_order_id:
            try:
                mod_res = await self.modify_order(
                    order_id=sell_order_id,
                    new_price=sweep_price,
                    order_type="LIMIT",
                    symbol=sym,
                    side="SELL",
                )
                return {
                    "method": "MODIFY",
                    "orderId": str(mod_res.get("orderId") or sell_order_id),
                    "sweep_price": sweep_price,
                    "qty": max(1, safe_int(holdings_qty, 1)),
                }
            except _NonRetryableApiError as exc:
                if exc.error_code == "already-filled":
                    return {
                        "method": "ALREADY_FILLED",
                        "orderId": sell_order_id,
                        "sweep_price": sweep_price,
                        "qty": max(1, safe_int(holdings_qty, 1)),
                    }
                logger.info(
                    "ℹ️ [%s] 1단계 modify_order 실패(%s) -> 2단계 선취소 후 신규 발사 전환",
                    sym,
                    exc.error_code,
                )
            except Exception as exc:
                logger.info(
                    "ℹ️ [%s] 1단계 modify_order 예외(%s) -> 2단계 선취소 후 신규 발사 전환",
                    sym,
                    exc,
                )

        # [2단계] 모든 OPEN 매도 주문 선취소
        open_orders = await self.get_orders(status="OPEN", symbol=sym)
        for o in open_orders:
            if isinstance(o, dict) and str(o.get("side") or "").upper() == "SELL":
                oid = str(o.get("orderId") or "").strip()
                if oid:
                    try:
                        await self.cancel_order(oid)
                    except Exception:
                        pass

        # sellableQuantity 최대 2.0초(0.25초 * 8회) 폴링 대기
        sellable_qty = 0
        poll_deadline = time.monotonic() + 2.0
        while time.monotonic() < poll_deadline:
            sellable_qty = await self.get_sellable_quantity(sym)
            if sellable_qty > 0:
                break
            await asyncio.sleep(0.25)

        if sellable_qty <= 0:
            holdings_map = await self.get_holdings_all()
            rem_qty = safe_int(holdings_map.get(sym, {}).get("qty"), 0)
            if rem_qty <= 0:
                return {
                    "method": "ALREADY_FILLED",
                    "orderId": sell_order_id,
                    "sweep_price": sweep_price,
                    "qty": max(1, safe_int(holdings_qty, 1)),
                }
            sellable_qty = rem_qty

        ord_res = await self.create_order(
            symbol=sym,
            side="SELL",
            quantity=sellable_qty,
            price=sweep_price,
            order_type="LIMIT",
            time_in_force="DAY",
            enforce_daymarket_block=False,
        )
        return {
            "method": "CANCEL_AND_CREATE",
            "orderId": str(ord_res.get("orderId") or ""),
            "sweep_price": sweep_price,
            "qty": sellable_qty,
        }

    # -----------------------------------------------------------------------
    # 제6헌법 & Case 10, Case 33 : 미국 시장 캘린더 30초 캐싱 및 평일 Fail-Open 폴백
    # -----------------------------------------------------------------------
    async def get_us_market_calendar(
        self, target_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        GET /api/v1/market-calendar/US (Rate Limits Group: MARKET_INFO)
        - 30초 인메모리 캐시 적용으로 동시 기상 병목 차단 (Case 33)
        - 10초 타임아웃 및 API 장애 시 평일 강제 개장 Fail-Open 폴백 반환 (제6헌법 & Case 10)
        """
        now_est = datetime.now(EST_TZ)
        date_str = str(target_date or now_est.strftime("%Y-%m-%d")).strip()

        with self._calendar_cache_lock:
            cached = self._calendar_cache.get(date_str)
            if cached is not None and (time.monotonic() - cached[0]) < 30.0:
                return cached[1]

        is_weekday = now_est.weekday() < 5
        fallback_today = {
            "date": date_str,
            "is_open_day": is_weekday,
            "is_fallback": True,
            "dayMarket": {"startTime": "19:00", "endTime": "03:50"} if is_weekday else None,
            "preMarket": {"startTime": "04:00", "endTime": "09:30"} if is_weekday else None,
            "regularMarket": {"startTime": "09:30", "endTime": "16:00"} if is_weekday else None,
            "afterMarket": {"startTime": "16:00", "endTime": "18:00"} if is_weekday else None,
        }

        try:
            res = await self._request_async(
                "GET",
                "/api/v1/market-calendar/US",
                group="MARKET_INFO",
                params={"date": date_str},
                require_account=False,
                timeout=10.0,
            )
            if isinstance(res, dict) and isinstance(res.get("today"), dict):
                today_obj = dict(res["today"])
                today_obj["is_open_day"] = any(
                    today_obj.get(k) is not None
                    for k in ("dayMarket", "preMarket", "regularMarket", "afterMarket")
                )
                today_obj["is_fallback"] = False
                with self._calendar_cache_lock:
                    self._calendar_cache[date_str] = (time.monotonic(), today_obj)
                return today_obj
        except Exception as exc:
            logger.warning(
                "⚠️ US 마켓 캘린더 조회 지연/실패 -> 제6헌법 평일 Fail-Open 폴백 가동: %s",
                exc,
            )

        with self._calendar_cache_lock:
            self._calendar_cache[date_str] = (time.monotonic(), fallback_today)
        return fallback_today
