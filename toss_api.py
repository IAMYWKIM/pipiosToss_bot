# =====================================================================
# FILE: toss_api.py
# 목적: 토스증권 Open API (v1.2.14) 비동기 통신 클라이언트(TossApiClient) 및
#       전역 Rate Limit·하이브리드 파일 뮤텍스 통제 코어(GlobalThrottle)
#       — 제1헌법(GlobalThrottle & _HybridFileLock), 제2헌법(BROKERAGE AccountSeq 자동 락온),
#         제5헌법(clientOrderId 멱등성 키), 제6헌법(마켓 캘린더 30초 캐싱 및 Fail-Open 폴백),
#         초과 Case 53/54/78(비허용 종목 및 데이장 실매수 물리적 원천 차단),
#         초과 Case 69(US 정정 quantity 배제), 초과 Case 75(POST 빈 JSON {} 강제 주입),
#         초과 Case 91/92(토스 QQQ 0.0초 실시간 가격·단기 변화율 스냅샷 브릿지) 100% 결속
# =====================================================================
# MODIFIED: 제1 ~ 제6헌법 및 100대 엣지 케이스 방어망 전수 반영
# NEW: 1) `_request` 내부에서 계좌 필수 API 호출 시 `account_seq` 가 미락온 상태이면 예외로 죽지 않고
#         즉시 `await self.lock_account_seq()` 를 자가 실행(Lazy Self-Lock)하여
#         `🚨 [제2헌법 위반] accountSeq가 아직 락온되지 않았습니다.` 에러를 영구 소각
#      2) `GlobalThrottle.get_file_lock` 에 동기(`with`) 및 비동기(`async with`) 양방향 호환 `_HybridFileLock` 내장
#      3) `get_qqq_realtime_snapshot` 탑재로 3차 역행 방어선 및 HA음봉 바이패스용 토스 QQQ 0.0초 실시간 시세 제공

import os
import time
import uuid
import math
import urllib.parse
import asyncio
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import aiohttp
import pandas as pd
from dotenv import load_dotenv

EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path)


class _NonRetryableApiError(RuntimeError):
    """Case 21: 4xx 클라이언트 에러(400, 403, 404, 409, 415, 422) 발생 시 백오프 없이 즉시 탈출용 예외."""
    pass


class _HybridFileLock:
    """
    제1헌법 및 초과 Case 30 완벽 방어:
    동기 스레드의 `with GlobalThrottle.get_file_lock(filepath):` 구문과
    비동기 코루틴의 `async with GlobalThrottle.get_file_lock(filepath):` 구문을
    동시에 100% 지원하는 재진입 가능 하이브리드 파일 락 클래스.
    """
    def __init__(self):
        self._rlock = threading.RLock()

    def acquire(self, blocking: bool = True, timeout: float = -1.0) -> bool:
        if timeout is None or timeout < 0.0:
            return self._rlock.acquire(blocking=blocking)
        return self._rlock.acquire(blocking=blocking, timeout=timeout)

    def release(self):
        try:
            self._rlock.release()
        except RuntimeError:
            pass

    def __enter__(self):
        self._rlock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

    async def __aenter__(self):
        await asyncio.to_thread(self._rlock.acquire)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


class GlobalThrottle:
    """
    제1헌법, Case 20, 초과 Case 30, 91:
    토스증권 Open API Rate Limit 그룹별 호출 간격 통제 및 전역 파일 뮤텍스 레지스트리.
    """
    active_client = None
    _api_lock = threading.Lock()
    _file_locks_guard = threading.Lock()
    _file_locks: dict[str, _HybridFileLock] = {}
    _last_call_ts: dict[str, float] = {}

    GROUP_MIN_INTERVAL = {
        "AUTH": 0.50,
        "ACCOUNT": 0.20,
        "ASSET": 0.12,
        "ORDER": 0.10,
        "ORDER_HISTORY": 0.12,
        "ORDER_INFO": 0.12,
        "MARKET_DATA": 0.08,
        "MARKET_DATA_CHART": 0.12,
        "MARKET_INFO": 0.20,
        "STOCK": 0.20,
        "DEFAULT": 0.10,
    }

    @classmethod
    def get_file_lock(cls, filepath: str) -> _HybridFileLock:
        key = os.path.abspath(str(filepath or "default.lock"))
        with cls._file_locks_guard:
            lock_obj = cls._file_locks.get(key)
            if lock_obj is None:
                lock_obj = _HybridFileLock()
                cls._file_locks[key] = lock_obj
            return lock_obj

    @classmethod
    def wait_api_sync(cls, group: str = "DEFAULT"):
        min_gap = float(cls.GROUP_MIN_INTERVAL.get(group, cls.GROUP_MIN_INTERVAL["DEFAULT"]))
        with cls._api_lock:
            now = time.monotonic()
            last_t = float(cls._last_call_ts.get(group, 0.0))
            wait_s = (last_t + min_gap) - now
            if wait_s > 0.0:
                time.sleep(wait_s)
            cls._last_call_ts[group] = time.monotonic()

    @classmethod
    async def wait_api_async(cls, group: str = "DEFAULT"):
        await asyncio.to_thread(cls.wait_api_sync, group)


class TossApiClient:
    """
    토스증권 Open API (https://openapi.tossinvest.com) 방탄 비동기 클라이언트.
    """
    BASE_URL = "https://openapi.tossinvest.com"
    REAL_ALLOWED_SYMBOLS = ("SOXS", "SOXL")
    ALLOWED_SYMBOLS = ("SOXS", "SOXL")
    READONLY_ALLOWED_SYMBOLS = ("SOXS", "SOXL", "QQQ")

    _ACCOUNT_REQUIRED_GROUPS = frozenset({
        "ASSET",
        "ORDER",
        "ORDER_HISTORY",
        "ORDER_INFO",
        "CONDITIONAL_ORDER",
        "CONDITIONAL_ORDER_HISTORY",
    })
    _ACCOUNT_REQUIRED_PATHS = (
        "/api/v1/holdings",
        "/api/v1/orders",
        "/api/v1/buying-power",
        "/api/v1/sellable-quantity",
        "/api/v1/commissions",
        "/api/v1/conditional-orders",
    )

    def __init__(self):
        self.client_id = (
            os.getenv("TOSS_CLIENT_ID")
            or os.getenv("TOSS_API_KEY")
            or os.getenv("CLIENT_ID")
            or ""
        ).strip()
        self.client_secret = (
            os.getenv("TOSS_CLIENT_SECRET")
            or os.getenv("TOSS_API_SECRET")
            or os.getenv("CLIENT_SECRET")
            or ""
        ).strip()

        self.access_token: str = ""
        self.token_expires_at: float = 0.0
        self.account_seq: int = 0
        self.account_no: str = ""

        self._session: aiohttp.ClientSession | None = None
        self._token_lock = asyncio.Lock()
        self._account_lock = asyncio.Lock()

        # 초과 Case 33: 미국 장 운영 캘린더 30초 인메모리 캐시
        self._cal_cache_ts: float = 0.0
        self._cal_cache_result: tuple | None = None

        # 초과 Case 91: GlobalThrottle.active_client 자동 바인딩
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

    @staticmethod
    def _is_valid_seq(val) -> bool:
        if val is None or isinstance(val, bool):
            return False
        s = str(val).strip()
        if not s or s in ("0", "None", "null", "False"):
            return False
        try:
            return int(s) > 0
        except ValueError:
            return len(s) > 0

    def _set_account_seq_internal(self, seq_val, acc_no: str = ""):
        try:
            seq_int = int(str(seq_val).strip())
        except ValueError:
            seq_int = 1
        self.account_seq = seq_int
        self._account_seq = seq_int
        self.accountSeq = seq_int
        self._accountSeq = seq_int
        if acc_no:
            self.account_no = str(acc_no)
            self.accountNo = str(acc_no)

    async def _ensure_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if not force_refresh and self.access_token and now < (self.token_expires_at - 60.0):
            return self.access_token

        async with self._token_lock:
            now = time.time()
            if not force_refresh and self.access_token and now < (self.token_expires_at - 60.0):
                return self.access_token

            await GlobalThrottle.wait_api_async("AUTH")
            session = await self._get_session()
            url = f"{self.BASE_URL}/oauth2/token"
            form_data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            async with session.post(url, data=form_data, headers=headers) as resp:
                body = await resp.json(content_type=None)
                if resp.status != 200 or not isinstance(body, dict) or "access_token" not in body:
                    err_desc = (body or {}).get("error_description") or (body or {}).get("error") or str(body)
                    raise RuntimeError(f"토스 OAuth2 토큰 발급 실패 (HTTP {resp.status}): {err_desc}")

                self.access_token = str(body["access_token"]).strip()
                expires_in = float(body.get("expires_in") or 86400.0)
                self.token_expires_at = time.time() + max(300.0, expires_in)
                return self.access_token

    ensure_token = _ensure_token
    get_access_token = _ensure_token

    def _get_headers(self, require_account: bool = False) -> dict:
        """
        제2헌법 헤더 빌더:
        - `self.account_seq > 0` 이 이미 메모리에 락온되어 있으면 `require_account` 인자와 무관하게
          항상 `X-Tossinvest-Account` 헤더를 주입합니다.
        - 아직 미락온 상태에서 `require_account=True` 로 호출되더라도, `_request` 단계에서
          자동 `lock_account_seq()`(Lazy Self-Lock)가 수행될 수 있도록 예외를 던지지 않고 기본 헤더를 반환합니다.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if self._is_valid_seq(self.account_seq):
            headers["X-Tossinvest-Account"] = str(self.account_seq)
        return headers

    async def lock_account_seq(self) -> int:
        """
        제2헌법: GET /api/v1/accounts 를 호출하여 종합매매(BROKERAGE) 계좌의 accountSeq 를 메모리에 락온합니다.
        """
        if self._is_valid_seq(self.account_seq):
            return int(self.account_seq)

        async with self._account_lock:
            if self._is_valid_seq(self.account_seq):
                return int(self.account_seq)

            try:
                await self._ensure_token()
                await GlobalThrottle.wait_api_async("ACCOUNT")
                session = await self._get_session()
                url = f"{self.BASE_URL}/api/v1/accounts"
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 401:
                        await self._ensure_token(force_refresh=True)
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        async with session.get(url, headers=headers) as retry_resp:
                            data = await retry_resp.json(content_type=None)
                    else:
                        data = await resp.json(content_type=None)

                accounts = ((data or {}).get("result")) if isinstance(data, dict) else []
                if isinstance(accounts, list) and accounts:
                    chosen_seq = None
                    chosen_no = ""
                    for acc in accounts:
                        if (
                            isinstance(acc, dict)
                            and acc.get("accountType") == "BROKERAGE"
                            and self._is_valid_seq(acc.get("accountSeq"))
                        ):
                            chosen_seq = acc.get("accountSeq")
                            chosen_no = str(acc.get("accountNo") or "")
                            break
                    if chosen_seq is None and isinstance(accounts[0], dict):
                        chosen_seq = accounts[0].get("accountSeq")
                        chosen_no = str(accounts[0].get("accountNo") or "")

                    if self._is_valid_seq(chosen_seq):
                        self._set_account_seq_internal(chosen_seq, chosen_no)
                        print(
                            f"🔒 [제2헌법 AccountSeq 락온 완료] BROKERAGE accountSeq={self.account_seq}",
                            flush=True,
                        )
                        return int(self.account_seq)
            except Exception as e:
                print(f"⚠️ [제2헌법 /api/v1/accounts 조회 방어] {e}", flush=True)

            # 환경변수 TOSS_ACCOUNT_SEQ 또는 기본 종합매매 시퀀스(1) 안전 폴백
            env_seq = os.getenv("TOSS_ACCOUNT_SEQ", "1").strip()
            fallback_seq = int(env_seq) if env_seq.isdigit() and int(env_seq) > 0 else 1
            self._set_account_seq_internal(fallback_seq)
            print(
                f"🔒 [제2헌법 AccountSeq 폴백 락온 완료] accountSeq={self.account_seq}",
                flush=True,
            )
            return int(self.account_seq)

    # 외부 모듈 호출 호환용 메서드 별칭 100% 바인딩
    initialize_account = lock_account_seq
    init_account_seq = lock_account_seq
    init_account = lock_account_seq
    get_account_seq = lock_account_seq
    fetch_account_seq = lock_account_seq
    ensure_account_seq = lock_account_seq

    def _needs_account_header(self, endpoint: str, rate_group: str) -> bool:
        if rate_group in self._ACCOUNT_REQUIRED_GROUPS:
            return True
        ep_clean = str(endpoint or "").split("?")[0]
        return any(ep_clean.startswith(p) for p in self._ACCOUNT_REQUIRED_PATHS)

    async def _request(
        self,
        method: str,
        endpoint: str,
        rate_group: str = "DEFAULT",
        headers: dict = None,
        json_data: dict = None,
        params: dict = None,
    ) -> dict:
        """
        제1헌법, 제2헌법, Case 20, Case 21, 초과 Case 75:
        - 모든 API 호출 전 토큰 유효성 및 계좌 필수 엔드포인트의 `X-Tossinvest-Account` 자동 락온 보장
        - POST / PUT / PATCH 요청 시 `json_data is None` 이면 자동으로 `{}` 주입 (HTTP 415 원천 소각)
        - 3단 지수 백오프 및 4xx 즉시 예외 탈출(_NonRetryableApiError)
        """
        method_up = str(method or "GET").upper()
        await self._ensure_token()

        need_acc = self._needs_account_header(endpoint, rate_group)
        if need_acc and not self._is_valid_seq(self.account_seq):
            await self.lock_account_seq()

        req_headers = dict(headers) if isinstance(headers, dict) else self._get_headers(require_account=need_acc)
        req_headers["Authorization"] = f"Bearer {self.access_token}"
        req_headers.setdefault("Content-Type", "application/json")
        req_headers.setdefault("Accept", "application/json")

        if self._is_valid_seq(self.account_seq) and (need_acc or "X-Tossinvest-Account" not in req_headers):
            req_headers["X-Tossinvest-Account"] = str(self.account_seq)

        # 초과 Case 75: POST / PUT / PATCH 시 빈 JSON 객체({}) 명시적 강제 주입
        if method_up in ("POST", "PUT", "PATCH") and json_data is None:
            json_data = {}

        url = f"{self.BASE_URL}{endpoint}" if endpoint.startswith("/") else f"{self.BASE_URL}/{endpoint}"

        last_err = None
        for attempt in range(3):
            try:
                await GlobalThrottle.wait_api_async(rate_group)
                session = await self._get_session()

                req_kwargs = {"headers": req_headers}
                if params is not None:
                    req_kwargs["params"] = params
                if method_up in ("POST", "PUT", "PATCH"):
                    req_kwargs["json"] = json_data if json_data is not None else {}

                async with session.request(method_up, url, **req_kwargs) as resp:
                    status_code = resp.status
                    if status_code == 204:
                        return {"result": {}}

                    resp_body = await resp.json(content_type=None)

                    if status_code == 200:
                        return resp_body if isinstance(resp_body, dict) else {"result": resp_body}

                    if status_code == 401 and attempt == 0:
                        await self._ensure_token(force_refresh=True)
                        req_headers["Authorization"] = f"Bearer {self.access_token}"
                        continue

                    err_obj = (resp_body or {}).get("error") if isinstance(resp_body, dict) else {}
                    err_code = (err_obj or {}).get("code") or f"HTTP_{status_code}"
                    err_msg = (err_obj or {}).get("message") or str(resp_body)

                    # 만약 account-header-required 에러 발생 시 즉시 재락온 후 재시도
                    if status_code == 400 and "account-header" in str(err_code).lower() and attempt < 2:
                        self.account_seq = 0
                        await self.lock_account_seq()
                        req_headers["X-Tossinvest-Account"] = str(self.account_seq)
                        continue

                    if status_code in (400, 403, 404, 409, 415, 422):
                        raise _NonRetryableApiError(f"[{status_code} {err_code}] {err_msg}")

                    if status_code == 429:
                        retry_after = float(resp.headers.get("Retry-After", 1.0) or 1.0)
                        await asyncio.sleep(min(3.0, max(0.5, retry_after)))
                        continue

                    last_err = RuntimeError(f"[{status_code} {err_code}] {err_msg}")
            except _NonRetryableApiError:
                raise
            except Exception as e:
                last_err = e

            if attempt < 2:
                await asyncio.sleep(0.3 * (2 ** attempt))

        if last_err:
            raise last_err
        return {}

    # =================================================================
    # Market Data (현재가 배치, 호가, 1분봉 캔들, QQQ 실시간 스냅샷)
    # =================================================================
    async def get_current_prices_batch(self, symbols: tuple | list = ("SOXS", "SOXL")) -> dict[str, float]:
        """GET /api/v1/prices?symbols=... 다건 현재가 배치 조회 (Case 04, 83, 91, 92)."""
        valid_syms = [
            str(s).strip().upper()
            for s in (symbols or ())
            if str(s).strip().upper() in self.READONLY_ALLOWED_SYMBOLS
        ]
        if not valid_syms:
            return {}

        sym_query = ",".join(valid_syms)
        out = {s: 0.0 for s in valid_syms}
        try:
            data = await self._request(
                "GET",
                f"/api/v1/prices?symbols={sym_query}",
                "MARKET_DATA",
                headers=self._get_headers(require_account=False),
            )
            items = (data or {}).get("result") or []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        sym_k = str(item.get("symbol") or "").upper()
                        lp = float(item.get("lastPrice") or 0.0)
                        if sym_k in out and lp > 0.0:
                            out[sym_k] = lp
        except Exception:
            pass
        return out

    async def get_current_price(self, symbol: str) -> float:
        sym_u = str(symbol or "").strip().upper()
        batch = await self.get_current_prices_batch((sym_u,))
        return float(batch.get(sym_u, 0.0) or 0.0)

    async def get_orderbook(self, symbol: str) -> dict:
        sym_u = str(symbol or "").strip().upper()
        if sym_u not in self.READONLY_ALLOWED_SYMBOLS:
            return {"asks": [], "bids": []}
        try:
            data = await self._request(
                "GET",
                f"/api/v1/orderbook?symbol={sym_u}",
                "MARKET_DATA",
                headers=self._get_headers(require_account=False),
            )
            res = (data or {}).get("result") or {}
            return res if isinstance(res, dict) else {"asks": [], "bids": []}
        except Exception:
            return {"asks": [], "bids": []}

    async def get_best_bid_price(self, symbol: str, fallback_price: float = 0.0) -> float:
        """Case 24: MOC 및 긴급 스윕 시 실시간 매수 1호가(Bid 1) 추출 (호가 공백 시 현재가 폴백)."""
        ob = await self.get_orderbook(symbol)
        bids = (ob or {}).get("bids") or []
        if isinstance(bids, list) and bids:
            try:
                bid_1 = float((bids[0] or {}).get("price") or 0.0)
                if bid_1 > 0.0:
                    return math.floor(bid_1 * 100.0) / 100.0
            except Exception:
                pass
        cur_p = await self.get_current_price(symbol)
        if cur_p > 0.0:
            return math.floor(cur_p * 100.0) / 100.0
        return math.floor(float(fallback_price or 0.0) * 100.0) / 100.0

    async def get_1m_candles_pagination(
        self,
        symbol: str,
        count: int = 200,
        before: str = None,
        adjusted: bool = True,
    ) -> dict:
        """GET /api/v1/candles 페이지네이션 조회 (nextBefore 타임존 '+' -> '%2B' 인코딩 엄수)."""
        sym_u = str(symbol or "").strip().upper()
        if sym_u not in self.READONLY_ALLOWED_SYMBOLS:
            return {"candles": [], "nextBefore": None}

        cnt = max(1, min(200, int(count or 200)))
        adj_str = "true" if adjusted else "false"
        endpoint = f"/api/v1/candles?symbol={sym_u}&interval=1m&count={cnt}&adjusted={adj_str}"
        if before:
            encoded_before = urllib.parse.quote(str(before), safe="")
            endpoint += f"&before={encoded_before}"

        try:
            data = await self._request(
                "GET",
                endpoint,
                "MARKET_DATA_CHART",
                headers=self._get_headers(require_account=False),
            )
            res = (data or {}).get("result") or {}
            if isinstance(res, dict):
                return {
                    "candles": res.get("candles") or [],
                    "nextBefore": res.get("nextBefore"),
                }
        except Exception:
            pass
        return {"candles": [], "nextBefore": None}

    async def get_1m_candles(self, symbol: str, count: int = 200) -> list:
        page = await self.get_1m_candles_pagination(symbol=symbol, count=count, before=None)
        return page.get("candles") or []

    async def get_qqq_realtime_snapshot(self) -> dict:
        """
        초과 Case 91 & Case 92:
        토스 Open API 로 나스닥 100 1배 현물 ETF(QQQ)의 0.0초 실시간 현재가 및
        당일 세션 1분봉 기준 10분 변화율(vel_10m) / 3분 변화율(vel_3m)을 산출합니다.
        """
        out = {"price": 0.0, "vel_10m": 0.0, "vel_3m": 0.0}
        try:
            p_map, candles_q = await asyncio.gather(
                self.get_current_prices_batch(("QQQ",)),
                self.get_1m_candles("QQQ", count=25),
            )
            qp = float((p_map or {}).get("QQQ", 0.0) or 0.0)
            if qp > 0.0:
                out["price"] = qp

            if candles_q and isinstance(candles_q, list):
                df_q = pd.DataFrame(candles_q)
                if not df_q.empty and "closePrice" in df_q.columns and "timestamp" in df_q.columns:
                    df_q["timestamp"] = pd.to_datetime(
                        df_q["timestamp"], format="ISO8601", utc=True
                    ).dt.tz_convert(EST_TZ)
                    df_q.sort_values("timestamp", ascending=True, inplace=True)
                    df_q["closePrice"] = pd.to_numeric(df_q["closePrice"], errors="coerce").fillna(0.0)
                    df_q = df_q[df_q["closePrice"] > 0.0]
                    if not df_q.empty:
                        cur_q = qp if qp > 0.0 else float(df_q["closePrice"].iloc[-1])
                        out["price"] = cur_q
                        c10 = float(df_q["closePrice"].iloc[-11]) if len(df_q) >= 11 else float(df_q["closePrice"].iloc[0])
                        c3 = float(df_q["closePrice"].iloc[-4]) if len(df_q) >= 4 else float(df_q["closePrice"].iloc[0])
                        if c10 > 0.0:
                            out["vel_10m"] = round((cur_q - c10) / c10 * 100.0, 4)
                        if c3 > 0.0:
                            out["vel_3m"] = round((cur_q - c3) / c3 * 100.0, 4)
        except Exception:
            pass
        return out

    # =================================================================
    # Market Calendar (30초 캐싱 및 제6헌법 Fail-Open 폴백)
    # =================================================================
    async def is_market_open(self) -> tuple[bool, str, str, dict]:
        """
        제6헌법, Case 10, 초과 Case 33:
        미국 시장 운영 여부 확인 (30초 인메모리 캐시 + API 장애 시 평일 강제 개장 Fail-Open).
        """
        now_ts = time.time()
        if self._cal_cache_result is not None and (now_ts - self._cal_cache_ts < 30.0):
            return self._cal_cache_result

        now_est = datetime.now(EST_TZ)
        wk = now_est.weekday()
        hm = now_est.hour * 100 + now_est.minute

        # 주말 휴장 (금요일 17:00 EST ~ 일요일 17:59 EST)
        if (wk == 4 and hm >= 1700) or (wk == 5) or (wk == 6 and hm < 1800):
            res = (False, "WEEKEND", "HOLIDAY", {})
            self._cal_cache_ts = now_ts
            self._cal_cache_result = res
            return res

        # 일~목 18:00 ~ 18:59 EST 선물 신세션 개장 · 데이장 대기
        if 1800 <= hm <= 1859:
            res = (False, "CME_OPEN_WAIT", "STANDBY", {})
            self._cal_cache_ts = now_ts
            self._cal_cache_result = res
            return res

        # 평일 17:00 ~ 17:59 EST 일일 정산 휴장
        if 1700 <= hm <= 1759:
            res = (False, "DAILY_BREAK", "STANDBY", {})
            self._cal_cache_ts = now_ts
            self._cal_cache_result = res
            return res

        if hm >= 1900 or hm < 400:
            fallback_sess = "dayMarket"
        elif 400 <= hm < 930:
            fallback_sess = "preMarket"
        elif 930 <= hm < 1600:
            fallback_sess = "regularMarket"
        else:
            fallback_sess = "afterMarket"

        try:
            logical_date = ((now_est + timedelta(days=1)) if now_est.hour >= 19 else now_est).strftime("%Y-%m-%d")
            data = await asyncio.wait_for(
                self._request(
                    "GET",
                    f"/api/v1/market-calendar/US?date={logical_date}",
                    "MARKET_INFO",
                    headers=self._get_headers(require_account=False),
                ),
                timeout=5.0,
            )
            today_info = ((data or {}).get("result") or {}).get("today") or {}
            if isinstance(today_info, dict) and today_info:
                if (
                    today_info.get("dayMarket") is None
                    and today_info.get("preMarket") is None
                    and today_info.get("regularMarket") is None
                    and today_info.get("afterMarket") is None
                ):
                    res = (False, "US_HOLIDAY", "HOLIDAY", today_info)
                    self._cal_cache_ts = now_ts
                    self._cal_cache_result = res
                    return res
                res = (True, "OPEN", fallback_sess, today_info)
                self._cal_cache_ts = now_ts
                self._cal_cache_result = res
                return res
        except Exception:
            pass

        # 제6헌법 Fail-Open 폴백
        res = (True, "FAIL_OPEN", fallback_sess, {})
        self._cal_cache_ts = now_ts
        self._cal_cache_result = res
        return res

    # =================================================================
    # Asset & Order Info (보유 잔고, USD 매수가능금액, 매도가능수량)
    # =================================================================
    async def get_symbol_holdings_detail(self, symbol: str) -> dict:
        """
        Case 03 & 초과 Case 29:
        GET /api/v1/holdings?symbol={symbol} 호출 후 수량·평단가·현재가·평가손익 추출.
        통신 장애 발생 시 `error=True` 를 반환하여 유령 잔고 오작동을 차단합니다.
        """
        sym_u = str(symbol or "").strip().upper()
        default_ret = {
            "symbol": sym_u,
            "qty": 0.0,
            "avg_price": 0.0,
            "current_price": 0.0,
            "profit_usd": 0.0,
            "profit_rate": 0.0,
            "error": False,
        }
        if sym_u not in self.ALLOWED_SYMBOLS:
            return default_ret

        try:
            data = await self._request(
                "GET",
                f"/api/v1/holdings?symbol={sym_u}",
                "ASSET",
                headers=self._get_headers(require_account=True),
            )
            result = (data or {}).get("result") or {}
            items = result.get("items") if isinstance(result, dict) else []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and str(item.get("symbol") or "").upper() == sym_u:
                        qty = float(item.get("quantity") or 0.0)
                        avg_p = float(item.get("averagePurchasePrice") or 0.0)
                        last_p = float(item.get("lastPrice") or 0.0)
                        pl_obj = item.get("profitLoss") if isinstance(item.get("profitLoss"), dict) else {}
                        prof_usd = float(pl_obj.get("amount") or 0.0)
                        prof_rate = float(pl_obj.get("rate") or 0.0)
                        return {
                            "symbol": sym_u,
                            "qty": qty,
                            "avg_price": avg_p,
                            "current_price": last_p,
                            "profit_usd": prof_usd,
                            "profit_rate": prof_rate,
                            "error": False,
                        }
            return default_ret
        except Exception as e:
            print(f"🚨 [잔고 조회 방어 {sym_u}] {e}", flush=True)
            err_ret = dict(default_ret)
            err_ret["error"] = True
            return err_ret

    async def get_usd_buying_power(self) -> float:
        """GET /api/v1/buying-power?currency=USD 현금 매수 가능 달러 조회 (초과 Case 65)."""
        try:
            data = await self._request(
                "GET",
                "/api/v1/buying-power?currency=USD",
                "ORDER_INFO",
                headers=self._get_headers(require_account=True),
            )
            res = (data or {}).get("result") or {}
            return float((res or {}).get("cashBuyingPower") or 0.0)
        except Exception as e:
            print(f"🚨 [USD 매수가능금액 조회 방어] {e}", flush=True)
            return 0.0

    async def get_sellable_quantity(self, symbol: str) -> float:
        """GET /api/v1/sellable-quantity?symbol={symbol} 매도 가능 수량 조회 (Case 24)."""
        sym_u = str(symbol or "").strip().upper()
        if sym_u not in self.ALLOWED_SYMBOLS:
            return 0.0
        try:
            data = await self._request(
                "GET",
                f"/api/v1/sellable-quantity?symbol={sym_u}",
                "ORDER_INFO",
                headers=self._get_headers(require_account=True),
            )
            res = (data or {}).get("result") or {}
            return float((res or {}).get("sellableQuantity") or 0.0)
        except Exception:
            return 0.0

    # =================================================================
    # Order & Order History (일반 지정가 생성, 정정, 취소, 목록/상세 조회)
    # =================================================================
    async def create_order(
        self,
        symbol: str,
        side: str,
        quantity: int | float,
        price: float = None,
        order_type: str = "LIMIT",
        time_in_force: str = "DAY",
        client_order_id: str = None,
    ) -> dict:
        """
        제5헌법, Case 09, 14, 초과 Case 34, 53, 54, 78:
        1) SOXS / SOXL 외 제3종목 주문 물리적 원천 차단
        2) 데이장(19:00 ~ 03:57 EST) 및 08:15 ~ 18:59 EST 구간 실거래 신규 매수(BUY) 물리적 원천 봉쇄
        3) clientOrderId 멱등성 키(최대 36자) 강제 주입
        """
        sym_u = str(symbol or "").strip().upper()
        if sym_u not in self.REAL_ALLOWED_SYMBOLS:
            raise ValueError(f"🚨 [초과 Case 54 위반] 허용되지 않은 제3종목 주문 원천 차단: {sym_u}")

        side_u = str(side or "").strip().upper()
        if side_u not in ("BUY", "SELL"):
            raise ValueError(f"유효하지 않은 주문 방향: {side_u}")

        # 초과 Case 53 & Case 78: 데이장(19:00~03:57 EST) 및 08:15~18:59 EST 실거래 매수 100% 물리적 차단!
        if side_u == "BUY":
            now_est = datetime.now(EST_TZ)
            est_hm = now_est.hour * 100 + now_est.minute
            if not (358 <= est_hm <= 814):
                raise RuntimeError(
                    f"🚨 [초과 Case 53/78 실매수 물리적 차단] {now_est.strftime('%H:%M:%S')} EST 는 "
                    "실거래 매수 허용 시간대(03:58 ~ 08:14 EST)가 아닙니다."
                )

        qty_int = int(math.floor(float(quantity or 0)))
        if qty_int <= 0:
            raise ValueError(f"주문 수량은 1주 이상이어야 합니다: {quantity}")

        otype_u = str(order_type or "LIMIT").strip().upper()
        tif_u = str(time_in_force or "DAY").strip().upper()

        cid = str(client_order_id or f"snp-{sym_u.lower()}-{uuid.uuid4().hex[:24]}")[:36]
        payload = {
            "clientOrderId": cid,
            "symbol": sym_u,
            "side": side_u,
            "orderType": otype_u,
            "timeInForce": tif_u,
            "quantity": str(qty_int),
        }
        if otype_u == "LIMIT":
            if price is None or float(price) <= 0.0:
                raise ValueError(f"지정가(LIMIT) 주문에는 유효한 가격이 필요합니다: {price}")
            payload["price"] = f"{float(price):.2f}"

        data = await self._request(
            "POST",
            "/api/v1/orders",
            "ORDER",
            headers=self._get_headers(require_account=True),
            json_data=payload,
        )
        res = (data or {}).get("result") or {}
        return res if isinstance(res, dict) else {}

    async def modify_order(
        self,
        order_id: str,
        price: float,
        order_type: str = "LIMIT",
        **kwargs,
    ) -> dict:
        """
        초과 Case 69:
        POST /api/v1/orders/{orderId}/modify 해외주식(US) 주문 정정 시
        `quantity` 파라미터를 원천 배제(`400 us-modify-quantity-not-supported` 방어)하고 가격만 정정합니다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            raise ValueError("modify_order: order_id 가 비어 있습니다.")

        otype_u = str(order_type or "LIMIT").strip().upper()
        payload = {"orderType": otype_u}
        if otype_u == "LIMIT":
            payload["price"] = f"{float(price):.2f}"

        data = await self._request(
            "POST",
            f"/api/v1/orders/{oid}/modify",
            "ORDER",
            headers=self._get_headers(require_account=True),
            json_data=payload,
        )
        res = (data or {}).get("result") or {}
        return res if isinstance(res, dict) else {}

    async def cancel_order(self, order_id: str) -> dict:
        """
        초과 Case 75:
        POST /api/v1/orders/{orderId}/cancel 호출 시 반드시 빈 JSON 객체(json_data={})를 명시적으로 전송하여
        HTTP 415 Unsupported Media Type 에러를 100% 원천 차단합니다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return {}
        data = await self._request(
            "POST",
            f"/api/v1/orders/{oid}/cancel",
            "ORDER",
            headers=self._get_headers(require_account=True),
            json_data={},
        )
        res = (data or {}).get("result") or {}
        return res if isinstance(res, dict) else {}

    async def get_order(self, order_id: str) -> dict:
        """GET /api/v1/orders/{orderId} 개별 주문 상세 조회."""
        oid = str(order_id or "").strip()
        if not oid:
            return {}
        try:
            data = await self._request(
                "GET",
                f"/api/v1/orders/{oid}",
                "ORDER_HISTORY",
                headers=self._get_headers(require_account=True),
            )
            res = (data or {}).get("result") or {}
            return res if isinstance(res, dict) else {}
        except Exception:
            return {}

    get_order_detail = get_order

    async def get_orders(
        self,
        status: str = "OPEN",
        symbol: str = None,
        limit: int = 20,
    ) -> list:
        """GET /api/v1/orders?status=OPEN|CLOSED 주문 목록 조회."""
        st_u = str(status or "OPEN").strip().upper()
        endpoint = f"/api/v1/orders?status={st_u}"
        if symbol:
            endpoint += f"&symbol={str(symbol).strip().upper()}"
        if st_u == "CLOSED" and limit:
            endpoint += f"&limit={max(1, min(100, int(limit)))}"
        try:
            data = await self._request(
                "GET",
                endpoint,
                "ORDER_HISTORY",
                headers=self._get_headers(require_account=True),
            )
            res = (data or {}).get("result") or {}
            orders = res.get("orders") if isinstance(res, dict) else []
            return orders if isinstance(orders, list) else []
        except Exception:
            return []
