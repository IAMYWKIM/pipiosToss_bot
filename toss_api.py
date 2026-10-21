# -*- coding: utf-8 -*-
"""
# MODIFIED: toss_api.py
토스증권(Toss) Open API v1.2.14 방탄 클라이언트, GlobalThrottle 중앙 통제기,
_HybridFileLock 하이브리드 파일 락 및 AssassinLedger 듀얼 스나이퍼 원자적 원장 코어 (Step 01 / 03)
- NEW: main.py 및 전술 플러그인과 100% 호환되는 AssassinLedger 클래스 탑재 (ImportError 완벽 소각)
- 제1헌법: 모든 동기 I/O(파일, HTTP)의 asyncio.to_thread 비동기 격리 및 GlobalThrottle / _HybridFileLock 통제
- 제2헌법: GET /api/v1/accounts 호출을 통한 종합매매(BROKERAGE) accountSeq 메모리 락온 및 X-Tossinvest-Account 주입
- 제4헌법: 모든 논리 시계열 ZoneInfo('America/New_York') 단일 소스 락온
- 제5헌법: POST /api/v1/orders 호출 시 고유 clientOrderId(최대 36자 영숫자) 자동 생성 및 멱등성 보장
- 제6헌법: GET /api/v1/market-calendar/US 지연/장애 시 평일 강제 개장 Fail-Open 폴백
- Case 01 ~ 10, 14, 20, 21, 24, 30, 31, 60, 65, 74, 75 완벽 준수
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger("toss_sniper.toss_api")

# 🚨 제4헌법: 타임존 단일 소스 락온 (EST 100%)
EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

# 🚨 Case 09: 듀얼 스나이퍼 허용 종목 화이트리스트 하드 락온
REAL_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXS", "SOXL")
READONLY_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXS", "SOXL", "QQQ")

TOSS_API_BASE_URL: str = "https://openapi.tossinvest.com"

# 4xx 즉시 탈출 대상 상태 코드 (Case 21: 백오프 재시도 금지)
NON_RETRYABLE_STATUS_CODES: Tuple[int, ...] = (400, 403, 404, 409, 415, 422)


# ---------------------------------------------------------------------------
# 안전한 형변환 및 /home/pipiosToss/.env 단일 경로 자동 로더
# ---------------------------------------------------------------------------
def safe_float(val: Any, default: float = 0.0) -> float:
    """
    Case 03 & Case 23: None, 빈 문자열, NaN, Inf 를 방어하는 안전한 float 변환기.
    """
    if val is None or isinstance(val, bool):
        return float(default)
    try:
         cleaned = str(val).replace(",", "").replace("$", "").strip()
        if not cleaned:
            return float(default)
        res = float(cleaned)
        if math.isnan(res) or math.isinf(res):
            return float(default)
        return res
    except (ValueError, TypeError):
        return float(default)


def safe_int(val: Any, default: int = 0) -> int:
    """
    소수점 문자열("10.0" 등)이나 None 도 안전하게 정수(int)로 변환한다.
    """
    if val is None or isinstance(val, bool):
        return int(default)
    try:
        return int(math.floor(safe_float(val, float(default))))
    except (ValueError, TypeError):
        return int(default)


def _autoload_dotenv_files(override: bool = False) -> None:
    """
    /home/pipiosToss/.env 단일 운영 경로(및 현재 모듈 폴더 폴백)에서
    환경변수를 파싱하여 os.environ 에 주입한다.
    """
    candidate_paths = [
        "/home/pipiosToss/.env",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
    ]
    for env_path in candidate_paths:
        if not os.path.isfile(env_path):
            continue
        try:
            with open(env_path, "r", encoding="utf-8") as fp:
                for raw_line in fp:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    if line.startswith("export "):
                        line = line[len("export ") :].strip()
                    k, v = line.split("=", 1)
                    key = k.strip()
                    val = v.strip()
                    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                        val = val[1:-1]
                    if key and (override or key not in os.environ or not os.environ.get(key)):
                        os.environ[key] = val
        except Exception as exc:
            logger.debug(".env 로드 예외(%s): %s", env_path, exc)


_autoload_dotenv_files(override=False)


def generate_client_order_id(prefix: str = "SNP") -> str:
    """
    🚨 제5헌법:
    토스증권 주문 생성 시 최대 36자 영숫자 및 '-', '_' 규칙(^[a-zA-Z0-9\\-_]+$)을 만족하는
    고유 멱등성 키(clientOrderId)를 생성한다.
    """
    clean_prefix = re.sub(r"[^a-zA-Z0-9]", "", str(prefix or "SNP"))[:8] or "SNP"
    ts_ms = int(time.time() * 1000) % 1000000000
    rand_hex = uuid.uuid4().hex[:12]
    cid = f"{clean_prefix}-{ts_ms}-{rand_hex}"
    return cid[:36]


def format_us_order_price(price: float) -> str:
    """
    API.txt OrderCreateRequest / OrderModifyRequest 가격 포맷 규격:
    - US 주식 $1 이상: 소수점 둘째 자리까지 (불필요한 후행 0 제거 또는 표준 소수 문자열)
    - US 주식 $1 미만: 소수점 넷째 자리까지
    """
    p = max(0.0001, safe_float(price, 0.0))
    if p >= 1.0:
         floored = math.floor((p + 1e-9) * 100.0) / 100.0
        return f"{floored:.2f}"
    floored_sub = math.floor((p + 1e-9) * 10000.0) / 10000.0
    return f"{floored_sub:.4f}"


# ---------------------------------------------------------------------------
# Case 21 : 4xx 비즈니스/클라이언트 에러 즉시 탈출용 예외 클래스
# ---------------------------------------------------------------------------
class _NonRetryableApiError( RuntimeError):
    """
    Case 21: 400, 403, 404, 409, 415, 422 등 재시도해도 결과가 같거나
    즉각적인 분기 처리가 필요한 4xx 에러 발생 시 백오프 없이 즉시 탈출하기 위한 예외.
    """

    def __init__(
        self,
        status_code: int,
        error_code: str = "",
        error_message: str = "",
        error_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = int(status_code)
        self.error_code = str(error_code or f"http-{status_code}")
        self.error_message = str(error_message or "")
        self.error_data = error_data if isinstance(error_data, dict) else {}
        super().__init__(
            f"[{self.status_code} {self.error_code}] {self.error_message}"
        )


# ---------------------------------------------------------------------------
# Case 06 & Case 30 : 동기(with) / 비동기(async with) 양방향 호환 _HybridFileLock
# ---------------------------------------------------------------------------
class _HybridFileLock:
    """
    🚨 제1헌법 & Case 06 & Case 30:
    단일 파일 경로에 대해 스레드 간 동기 컨텍스트(with lock:)와
    코루틴 간 비동기 컨텍스트(async with lock:) 모두에서 안전하게 동작하는 하이브리드 락.
    """

    def __init__(self, filepath: str) -> None:
        self.filepath = os.path.abspath(str(filepath or ""))
        self._rlock = threading.RLock()

    def acquire(self) -> None:
        self._rlock.acquire()

    def release(self) -> None:
        try:
            self._rlock.release()
        except RuntimeError:
            pass

    def __enter__(self) -> "_HybridFileLock":
        self._rlock.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()

    async def __aenter__(self) -> "_HybridFileLock":
        await asyncio.to_thread(self._rlock.acquire)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# 제1헌법 & Case 20 : GlobalThrottle (API Rate Limit 그룹별 중앙 통제 및 파일 락 레지스트리)
# ---------------------------------------------------------------------------
class GlobalThrottle:
    """
    🚨 제1헌법 & Case 20:
    토스증권 Open API Rate Limits 그룹별 최소 호출 간격을 중앙에서 통제하고,
    파일 경로별 단일 _HybridFileLock 인스턴스를 관리한다.
    """

    _group_locks_guard = threading.Lock()
    _group_locks: Dict[str, threading.Lock] = {}
    _group_last_call_mono: Dict[str, float] = {}

    _file_locks_guard = threading.Lock()
    _file_locks: Dict[str, _HybridFileLock] = {}

    # 토스증권 API Rate Limit 그룹별 안전 최소 간격(초)
    GROUP_MIN_INTERVALS: Dict[str, float] = {
        "AUTH": 1.00,
        "ACCOUNT": 0.25,
        "ASSET": 0.20,
        "ORDER": 0.12,
        "ORDER_HISTORY": 0.20,
        "ORDER_INFO": 0.20,
        "MARKET_DATA": 0.10,
        "MARKET_DATA_CHART": 0.22,
        "MARKET_INFO": 0.30,
        "DEFAULT": 0.15,
    }

    @classmethod
    def wait_api_sync(cls, group: str = "DEFAULT") -> None:
        grp = str(group or "DEFAULT").upper().strip()
        min_interval = cls.GROUP_MIN_INTERVALS.get(
            grp, cls.GROUP_MIN_INTERVALS["DEFAULT"]
        )
        with cls._group_locks_guard:
            grp_lock = cls._group_locks.get(grp)
            if grp_lock is None:
                grp_lock = threading.Lock()
                cls._group_locks[grp] = grp_lock

        with grp_lock:
            now_mono = time.monotonic()
            last_mono = cls._group_last_call_mono.get(grp, 0.0)
            elapsed = now_mono - last_mono
            if elapsed < min_interval:
                time.sleep(max(0.0, min_interval - elapsed))
            cls._group_last_call_mono[grp] = time.monotonic()

    @classmethod
    async def wait_api_async(cls, group: str = "DEFAULT") -> None:
        await asyncio.to_thread(cls.wait_api_sync, group)

    @classmethod
    def get_file_lock(cls, filepath: str) -> _HybridFileLock:
        abs_path = os.path.abspath(str(filepath or "default.lock"))
        with cls._file_locks_guard:
            lock_obj = cls._file_locks.get(abs_path)
            if lock_obj is None:
                lock_obj = _HybridFileLock(abs_path)
                cls._file_locks[abs_path] = lock_obj
            return lock_obj


# ---------------------------------------------------------------------------
# NEW : AssassinLedger (SOXS & SOXL 듀얼 스나이퍼 원자적 JSON 로컬 원장 관리기)
# ---------------------------------------------------------------------------
def _default_symbol_ledger_state(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "SOXS").upper().strip()
    return {
        "symbol": sym,
        "is_active": True,
        "is_compound": True,
        "budget": 1000.0,
        "target_profit_rate": 1.0,
        "buy_order_id": "",
        "sell_order_id": "",
        "client_order_id": "",
        "ordered_qty": 0,
        "buy_price": 0.0,
        "target_sell_price": 0.0,
        "entry_session": "",
        "entry_trend": "",
        "nq_entry_price": 0.0,
        "nq_entry_amp": 0.0,
        "is_session_done": False,
        "last_modify_mono": 0.0,
        "entry_ts": 0.0,
        "updated_at": "",
    }


class AssassinLedger:
    """
    🚨 제1헌법 & Case 02, 05, 06, 09:
    SOXS 및 SOXL 듀얼 원장(state_SOXS.json, state_SOXL.json)을 인메모리 캐시와
    원자적 파일 덮어쓰기(.tmp 기록 + fsync + os.replace)로 동기화 관리하는 클래스.
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        if base_dir and os.path.isdir(base_dir):
            self.base_dir = os.path.abspath(base_dir)
        elif os.path.isdir("/home/pipiosToss"):
            self.base_dir = "/home/pipiosToss"
        else:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self._mem_lock = threading.RLock()
        self._states: Dict[str, Dict[str, Any]] = {}

        for sym in REAL_ALLOWED_SYMBOLS:
            self._states[sym] = self._load_symbol_state_sync(sym)

        # 기동 시 양쪽 원장의 시드(budget) 및 복리 모드(is_compound) 일치 동기화
        self._unify_seed_pool_on_init_sync()

    def _validate_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        if sym not in REAL_ALLOWED_SYMBOLS:
            raise ValueError(
                f"[Case 09 위반] 스나이퍼 원장 비허용 종목 접근 차단: {sym}"
            )
        return sym

    def _get_filepath(self, symbol: str) -> str:
        sym = self._validate_symbol(symbol)
        return os.path.join(self.base_dir, f"state_{sym}.json")

    def _load_symbol_state_sync(self, symbol: str) -> Dict[str, Any]:
        sym = self._validate_symbol(symbol)
        fpath = self._get_filepath(sym)
        base = _default_symbol_ledger_state(sym)

        with GlobalThrottle.get_file_lock(fpath):
            if not os.path.isfile(fpath):
                self._write_symbol_state_sync(sym, base)
                return base
            try:
                with open(fpath, "r", encoding="utf-8") as fp:
                    loaded = json.load(fp)
                if isinstance(loaded, dict):
                    for k, default_v in base.items():
                        if k in loaded and loaded[k] is not None:
                            if isinstance(default_v, bool):
                                base[k] = bool(loaded[k])
                            elif isinstance(default_v, int):
                                base[k] = safe_int(loaded[k], default_v)
                            elif isinstance(default_v, float):
                                base[k] = safe_float(loaded[k], default_v)
                            else:
                                base[k] = str(loaded[k])
                    base["symbol"] = sym
            except Exception as exc:
                logger.warning("⚠️ [%s] 원장 파일 로드 폴백 사용: %s", sym, exc)
        return base

    def _write_symbol_state_sync(
        self, symbol: str, state_data: Dict[str, Any]
    ) -> None:
        sym = self._validate_symbol(symbol)
        fpath = self._get_filepath(sym)
        tmp_path = f"{fpath}.tmp"

        with GlobalThrottle.get_file_lock(fpath):
            try:
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                payload = dict(state_data)
                payload["symbol"] = sym
                payload["updated_at"] = datetime.now(EST_TZ).strftime(
                    "%Y-%m-%d %H:%M:%S EST"
                )
                with open(tmp_path, "w", encoding="utf-8") as fp:
                    json.dump(payload, fp, ensure_ascii=False, indent=2)
                    fp.flush()
                    os.fsync(fp.fileno())
                os.replace(tmp_path, fpath)
            except Exception as exc:
                logger.error("🚨 [%s] 원장 파일 원자적 기록 실패: %s", sym, exc)

    def _unify_seed_pool_on_init_sync(self) -> None:
        with self._mem_lock:
            st_s = self._states.get("SOXS", {})
            st_l = self._states.get("SOXL", {})
            b_s = safe_float(st_s.get("budget"), 1000.0)
            b_l = safe_float(st_l.get("budget"), 1000.0)
            if b_s != b_l:
                unified_b = b_s if b_s > 0.0 else b_l
                st_s["budget"] = unified_b
                st_l["budget"] = unified_b
                self._write_symbol_state_sync("SOXS", st_s)
                self._write_symbol_state_sync("SOXL", st_l)

    def get_state(self, symbol: str) -> Dict[str, Any]:
        sym = self._validate_symbol(symbol)
        with self._mem_lock:
            cur = self._states.get(sym)
            if not isinstance(cur, dict):
                cur = _default_symbol_ledger_state(sym)
                self._states[sym] = cur
            return dict(cur)

    async def update_state(self, symbol: str, **kwargs: Any) -> Dict[str, Any]:
        """
        제1헌법 & Case 05, 06:
        인메모리 원장 상태를 즉시 갱신하고 백그라운드 스레드(asyncio.to_thread)를 통해
        디스크 JSON 원장에 원자적으로 덮어쓴다.
        """
        sym = self._validate_symbol(symbol)
        with self._mem_lock:
            cur = self._states.get(sym)
            if not isinstance(cur, dict):
                cur = _default_symbol_ledger_state(sym)
            for k, v in kwargs.items():
                cur[k] = v
            cur["symbol"] = sym
            self._states[sym] = cur
            snapshot = dict(cur)

        await asyncio.wait_for(
            asyncio.to_thread(self._write_symbol_state_sync, sym, snapshot),
            timeout=5.0,
        )
        return snapshot

    async def reset_dual_ledgers_preserve_seed(self) -> None:
        """
        🚨 Case 02:
        /reset 명령 시 SOXS 및 SOXL 양쪽 장부의 주문/보유/세션 플래그는 0점으로 소각하되,
        통합 시드(budget), 복리/단리 모드(is_compound), 목표 수익률(target_profit_rate),
        스나이퍼 가동 여부(is_active)는 그대로 보존한다.
        """
        for sym in REAL_ALLOWED_SYMBOLS:
            st = self.get_state(sym)
            await self.update_state(
                sym,
                is_active=bool(st.get("is_active", True)),
                is_compound=bool(st.get("is_compound", True)),
                budget=max(10.0, safe_float(st.get("budget"), 1000.0)),
                target_profit_rate=safe_float(
                    st.get("target_profit_rate"), 1.0
                ),
                buy_order_id="",
                sell_order_id="",
                client_order_id="",
                ordered_qty=0,
                buy_price=0.0,
                target_sell_price=0.0,
                entry_session="",
                entry_trend="",
                nq_entry_price=0.0,
                nq_entry_amp=0.0,
                is_session_done=False,
                last_modify_mono=0.0,
                entry_ts=0.0,
            )


# ---------------------------------------------------------------------------
# TossApiClient : 토스증권 Open API v1.2.14 방탄 비동기 클라이언트
# ---------------------------------------------------------------------------
class TossApiClient:
    """
    제1헌법 ~ 제6헌법 및 API.txt v1.2.14 전 엔드포인트 완벽 구현 클라이언트.
    """

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: str = TOSS_API_BASE_URL,
    ) -> None:
        _autoload_dotenv_files(override=False)
        self.base_url = str(base_url or TOSS_API_BASE_URL).rstrip("/")
        self.client_id = str(
            client_id
            or os.getenv("TOSS_CLIENT_ID")
            or os.getenv("CLIENT_ID")
            or ""
        ).strip()
        self.client_secret = str(
            client_secret
            or os.getenv("TOSS_CLIENT_SECRET")
            or os.getenv("CLIENT_SECRET")
            or ""
        ).strip()

        self._session = requests.Session()
        self._auth_lock = threading.RLock()
        self._access_token: str = ""
        self._token_expires_mono: float = 0.0
        self._account_seq: Optional[int] = None

        # US 장 운영 캘린더 30초 캐시 (Case 31 & 제6헌법)
        self._cal_lock = threading.Lock()
        self._cal_cache: Dict[str, Any] = {
            "date": "",
            "updated_mono": 0.0,
            "data": {},
        }

    def _refresh_env_credentials(self) -> None:
        if self.client_id and self.client_secret:
            return
        _autoload_dotenv_files(override=True)
        self.client_id = str(
            os.getenv("TOSS_CLIENT_ID") or os.getenv("CLIENT_ID") or ""
        ).strip()
        self.client_secret = str(
            os.getenv("TOSS_CLIENT_SECRET") or os.getenv("CLIENT_SECRET") or ""
        ).strip()

    # -----------------------------------------------------------------------
    # [Auth] POST /oauth2/token (application/x-www-form-urlencoded)
    # -----------------------------------------------------------------------
    def _ensure_token_sync(self, force_refresh: bool = False) -> str:
        with self._auth_lock:
            now_mono = time.monotonic()
            if (
                not force_refresh
                and self._access_token
                and now_mono < (self._token_expires_mono - 60.0)
            ):
                return self._access_token

            self._refresh_env_credentials()
            if not self.client_id or not self.client_secret:
                raise RuntimeError(
                    "TOSS_CLIENT_ID / TOSS_CLIENT_SECRET 환경변수가 설정되지 않았습니다."
                )

            GlobalThrottle.wait_api_sync("AUTH")
            url = f"{self.base_url}/oauth2/token"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            resp = self._session.post(url, headers=headers, data=data, timeout=10.0)
            if resp.status_code != 200:
                try:
                    err_j = resp.json()
                except Exception:
                    err_j = {"error": resp.text}
                raise RuntimeError(
                    f"OAuth2 토큰 발급 실패 (HTTP {resp.status_code}): {err_j}"
                )

            body = resp.json()
            token = str(body.get("access_token") or "").strip()
            expires_in = max(300, safe_int(body.get("expires_in"), 86400))
            if not token:
                raise RuntimeError("OAuth2 응답에 access_token 이 없습니다.")

            self._access_token = token
            self._token_expires_mono = time.monotonic() + float(expires_in)
            return self._access_token

    # -----------------------------------------------------------------------
    # [제2헌법] GET /api/v1/accounts -> 종합매매(BROKERAGE) accountSeq 메모리 락온
    # -----------------------------------------------------------------------
    def _ensure_account_seq_sync(self, force_refresh: bool = False) -> int:
        with self._auth_lock:
            if not force_refresh and self._account_seq is not None and self._account_seq > 0:
                return self._account_seq

            env_seq = safe_int(os.getenv("TOSS_ACCOUNT_SEQ"), 0)
            token = self._ensure_token_sync(force_refresh=False)
            GlobalThrottle.wait_api_sync("ACCOUNT")

            url = f"{self.base_url}/api/v1/accounts"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            resp = self._session.get(url, headers=headers, timeout=10.0)
            if resp.status_code == 401:
                token = self._ensure_token_sync(force_refresh=True)
                headers["Authorization"] = f"Bearer {token}"
                GlobalThrottle.wait_api_sync("ACCOUNT")
                resp = self._session.get(url, headers=headers, timeout=10.0)

            if resp.status_code == 200:
                payload = resp.json()
                acc_list = (
                    payload.get("result")
                    if isinstance(payload, dict)
                    else []
                )
                if isinstance(acc_list, list) and acc_list:
                    for acc in acc_list:
                        if not isinstance(acc, dict):
                            continue
                        if str(acc.get("accountType") or "") == "BROKERAGE":
                            seq = safe_int(acc.get("accountSeq"), 0)
                            if seq > 0:
                                self._account_seq = seq
                                return seq
                    first_seq = safe_int(acc_list[0].get("accountSeq"), 0)
                    if first_seq > 0:
                        self._account_seq = first_seq
                        return first_seq

            if env_seq > 0:
                self._account_seq = env_seq
                return env_seq

            raise RuntimeError(
                f"종합매매(BROKERAGE) 계좌 accountSeq 조회 실패 (HTTP {resp.status_code})"
            )

    async def ensure_account_seq(self) -> int:
        return await asyncio.wait_for(
            asyncio.to_thread(self._ensure_account_seq_sync, False),
            timeout=12.0,
        )

    # -----------------------------------------------------------------------
    # 공통 HTTP 요청 엔진 (Case 20 스로틀링 + Case 21 3단 지수 백오프 & 4xx 즉시 탈출)
    # -----------------------------------------------------------------------
    def _request_sync(
        self,
        method: str,
        path: str,
        group: str = "DEFAULT",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_account: bool = True,
        timeout: float = 8.0,
    ) -> Any:
        url = (
            path
            if path.startswith("http")
            else f"{self.base_url}/{path.lstrip('/')}"
        )
        m_upper = str(method or "GET").upper().strip()

        backoff_delays = (0.3, 0.7, 1.4)
        last_exc: Optional[Exception] = None

        for attempt in range(len(backoff_delays) + 1):
            try:
                token = self._ensure_token_sync(force_refresh=(attempt > 0 and False))
                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
                if require_account:
                    acc_seq = self._ensure_account_seq_sync(force_refresh=False)
                    headers["X-Tossinvest-Account"] = str(acc_seq)

                if json_body is not None:
                    headers["Content-Type"] = "application/json"

                GlobalThrottle.wait_api_sync(group)
                resp = self._session.request(
                    method=m_upper,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=timeout,
                )

                if resp.status_code in (200, 201):
                    if not resp.text:
                        return {}
                    data = resp.json()
                    if isinstance(data, dict) and "result" in data:
                        return data["result"]
                    return data

                if resp.status_code == 204:
                    return {}

                # 401 토큰 만료 시 즉시 재발급 후 재시도
                if resp.status_code == 401 and attempt < len(backoff_delays):
                    self._ensure_token_sync(force_refresh=True)
                    continue

                # 에러 응답 파싱
                err_code = f"http-{resp.status_code}"
                err_msg = resp.text[:300] if resp.text else ""
                err_data: Dict[str, Any] = {}
                try:
                    err_json = resp.json()
                    if isinstance(err_json, dict):
                        err_obj = err_json.get("error")
                        if isinstance(err_obj, dict):
                            err_code = str(err_obj.get("code") or err_code)
                            err_msg = str(err_obj.get("message") or err_msg)
                            if isinstance(err_obj.get("data"), dict):
                                err_data = err_obj["data"]
                except Exception:
                    pass

                # Case 21: 4xx 클라이언트/비즈니스 에러는 백오프 없이 즉시 탈출!
                if resp.status_code in NON_RETRYABLE_STATUS_CODES:
                    raise _NonRetryableApiError(
                        status_code=resp.status_code,
                        error_code=err_code,
                        error_message=err_msg,
                        error_data=err_data,
                    )

                # 429 Rate Limit 또는 5xx 서버 오류 시 지수 백오프
                if resp.status_code == 429:
                    retry_after = safe_float(
                        resp.headers.get("Retry-After"), backoff_delays[min(attempt, 2)]
                    )
                    time.sleep(max(0.2, min(retry_after, 3.0)))
                    continue

                last_exc = RuntimeError(
                    f"HTTP {resp.status_code} ({err_code}): {err_msg}"
                )
            except _NonRetryableApiError:
                raise
            except Exception as exc:
                last_exc = exc

            if attempt < len(backoff_delays):
                time.sleep(backoff_delays[attempt])

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"API 요청 실패: {m_upper} {path}")

    async def _request_async(
        self,
        method: str,
        path: str,
        group: str = "DEFAULT",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_account: bool = True,
        timeout: float = 10.0,
    ) -> Any:
        return await asyncio.wait_for(
            asyncio.to_thread(
                self._request_sync,
                method,
                path,
                group,
                params,
                json_body,
                require_account,
                max(4.0, timeout - 1.0),
            ),
            timeout=timeout,
        )

    # -----------------------------------------------------------------------
    # [Market Data] 현재가 다건 조회 · 호가 조회 · 캔들 차트(단일/다중 페이지) 조회
    # -----------------------------------------------------------------------
    async def get_current_prices_batch(
        self, symbols: Iterable[str] = READONLY_ALLOWED_SYMBOLS
    ) -> Dict[str, float]:
        """
        API.txt GET /api/v1/prices?symbols=SOXS,SOXL,QQQ
        """
        valid_syms = [
            str(s).upper().strip()
            for s in symbols
            if str(s).upper().strip() in READONLY_ALLOWED_SYMBOLS
        ]
        if not valid_syms:
            return {}

        out: Dict[str, float] = {s: 0.0 for s in valid_syms}
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/prices",
                group="MARKET_DATA",
                params={"symbols": ",".join(valid_syms)},
                require_account=False,
                timeout=8.0,
            )
            if isinstance(res, list):
                for item in res:
                    if not isinstance(item, dict):
                        continue
                    sym = str(item.get("symbol") or "").upper().strip()
                    lp = safe_float(item.get("lastPrice"), 0.0)
                    if sym in out and lp > 0.0:
                        out[sym] = lp
        except Exception as exc:
            logger.debug("현재가 배치 조회 예외: %s", exc)
        return out

    async def get_orderbook(self, symbol: str) -> Dict[str, Any]:
        """
        API.txt GET /api/v1/orderbook?symbol=SOXS
        """
        sym = str(symbol or "").upper().strip()
        if sym not in READONLY_ALLOWED_SYMBOLS:
            return {"asks": [], "bids": []}
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/orderbook",
                group="MARKET_DATA",
                params={"symbol": sym},
                require_account=False,
                timeout=6.0,
            )
            return res if isinstance(res, dict) else {"asks": [], "bids": []}
        except Exception:
            return {"asks": [], "bids": []}

    async def get_best_bid_price(
        self, symbol: str, fallback_price: float = 0.0
    ) -> float:
        ob = await self.get_orderbook(symbol)
        bids = ob.get("bids") if isinstance(ob, dict) else []
        if isinstance(bids, list) and bids:
            top_bid = bids[0]
            if isinstance(top_bid, dict):
                bp = safe_float(top_bid.get("price"), 0.0)
                if bp > 0.0:
                    return bp
        return safe_float(fallback_price, 0.0)

    async def get_best_ask_price(
        self, symbol: str, fallback_price: float = 0.0
    ) -> float:
        ob = await self.get_orderbook(symbol)
        asks = ob.get("asks") if isinstance(ob, dict) else []
        if isinstance(asks, list) and asks:
            # asks 는 낮은 가격순 정렬이나 안전을 위해 양수 최소값 추출
            valid_asks = [
                safe_float(a.get("price"), 0.0)
                for a in asks
                if isinstance(a, dict) and safe_float(a.get("price"), 0.0) > 0.0
            ]
            if valid_asks:
                return min(valid_asks)
        return safe_float(fallback_price, 0.0)

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1m",
        count: int = 100,
        before: Optional[str] = None,
        adjusted: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        API.txt GET /api/v1/candles (Rate Limits Group: MARKET_DATA_CHART)
        """
        sym = str(symbol or "").upper().strip()
        if sym not in READONLY_ALLOWED_SYMBOLS:
            return [], None

        params: Dict[str, Any] = {
            "symbol": sym,
            "interval": str(interval or "1m"),
            "count": max(1, min(200, int(count))),
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
            timeout=9.0,
        )
        if isinstance(res, dict):
            c_list = (
                res.get("candles") if isinstance(res.get("candles"), list) else []
            )
            next_b = res.get("nextBefore")
            return c_list, (str(next_b) if next_b else None)
        return [], None

    async def get_candles_multi_page(
        self,
        symbol: str,
        interval: str = "1m",
        total_count: int = 400,
        adjusted: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        🎯 3: 다중 페이지(최대 400분봉) 조회로 19:00 EST 데이장 및 04:00 EST 프리장
        개장 첫 봉부터 누락 없는 완전한 시계열을 확보한다.
        """
        collected: List[Dict[str, Any]] = []
        seen_ts = set()
        next_before: Optional[str] = None
        target = max(1, min(800, int(total_count)))
        max_pages = max(1, math.ceil(target / 195.0))

        for _ in range(max_pages):
            req_cnt = min(200, target - len(collected) + 5)
            if req_cnt <= 0:
                break
            c_list, next_b = await self.get_candles(
                symbol=symbol,
                interval=interval,
                count=req_cnt,
                before=next_before,
                adjusted=adjusted,
            )
            if not c_list:
                break
            for item in c_list:
                if not isinstance(item, dict):
                    continue
                ts_str = str(item.get("timestamp") or "")
                if ts_str and ts_str not in seen_ts:
                    seen_ts.add(ts_str)
                    collected.append(item)
            if not next_b or next_b == next_before or len(collected) >= target:
                break
            next_before = next_b

        return collected

    # -----------------------------------------------------------------------
    # [Asset & Order Info] 보유 주식 조회 · 매수가능금액 · 매도가능수량 조회
    # -----------------------------------------------------------------------
    async def get_holdings_all(self) -> Dict[str, Dict[str, Any]]:
        """
        API.txt GET /api/v1/holdings & Case 03:
        krw / usd null 결측치를 방어하며 SOXS 와 SOXL 의 실보유 수량 및 평균 매입가를 반환한다.
        """
        out: Dict[str, Dict[str, Any]] = {
            s: {
                "symbol": s,
                "qty": 0,
                "avg_price": 0.0,
                "last_price": 0.0,
                "purchase_amount": 0.0,
                "market_value": 0.0,
                "profit_loss_usd": 0.0,
                "profit_loss_rate": 0.0,
            }
            for s in REAL_ALLOWED_SYMBOLS
        }
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/holdings",
                group="ASSET",
                require_account=True,
                timeout=8.0,
            )
            items = res.get("items") if isinstance(res, dict) else []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sym = str(item.get("symbol") or "").upper().strip()
                    if sym not in out:
                        continue
                    qty = safe_int(item.get("quantity"), 0)
                    avg_p = safe_float(item.get("averagePurchasePrice"), 0.0)
                    last_p = safe_float(item.get("lastPrice"), avg_p)
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
                        "purchase_amount": safe_float(
                            mv_obj.get("purchaseAmount"), 0.0
                        ),
                        "market_value": safe_float(mv_obj.get("amount"), 0.0),
                        "profit_loss_usd": safe_float(pl_obj.get("amount"), 0.0),
                        "profit_loss_rate": safe_float(pl_obj.get("rate"), 0.0)
                        * 100.0,
                    }
        except Exception as exc:
            logger.warning("⚠️ 보유 주식(holdings) 조회 예외: %s", exc)
        return out

    async def get_buying_power_usd(
        self, safety_buffer_pct: float = 0.5
    ) -> float:
        """
        API.txt GET /api/v1/buying-power?currency=USD & Case 65:
        현금 기반 USD 매수가능금액을 조회하고 안전 버퍼(기본 0.5%)를 차감한 금액을 반환한다.
        """
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/buying-power",
                group="ORDER_INFO",
                params={"currency": "USD"},
                require_account=True,
                timeout=7.0,
            )
            if isinstance(res, dict):
                raw_bp = safe_float(res.get("cashBuyingPower"), 0.0)
                buf_mult = max(
                    0.0, 1.0 - (safe_float(safety_buffer_pct, 0.0) / 100.0)
                )
                return round(max(0.0, raw_bp * buf_mult), 2)
        except Exception as exc:
            logger.debug("USD 매수가능금액 조회 예외: %s", exc)
        return 0.0

    async def get_sellable_quantity(self, symbol: str) -> int:
        """
        API.txt GET /api/v1/sellable-quantity?symbol=SOXS
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
                timeout=6.0,
            )
            if isinstance(res, dict):
                return max(0, safe_int(res.get("sellableQuantity"), 0))
        except Exception as exc:
            logger.debug("판매가능수량 조회 예외(%s): %s", sym, exc)
        return 0

    # -----------------------------------------------------------------------
    # [Order History] 주문 목록 조회 · 주문 상세 조회 · 최신 OPEN 주문 ID 탐색
    # -----------------------------------------------------------------------
    async def get_orders(
        self,
        status: str = "OPEN",
        symbol: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        API.txt GET /api/v1/orders?status=OPEN|CLOSED
        """
        st_code = str(status or "OPEN").upper().strip()
        params: Dict[str, Any] = {"status": st_code}
        if symbol:
            params["symbol"] = str(symbol).upper().strip()
        if st_code == "CLOSED":
            params["limit"] = max(1, min(100, int(limit)))

        try:
            res = await self._request_async(
                "GET",
                "/api/v1/orders",
                group="ORDER_HISTORY",
                params=params,
                require_account=True,
                timeout=8.0,
            )
            if isinstance(res, dict) and isinstance(res.get("orders"), list):
                return res["orders"]
        except Exception as exc:
            logger.debug("주문 목록 조회 예외(%s): %s", st_code, exc)
        return []

    async def get_order_detail(
        self, order_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        API.txt GET /api/v1/orders/{orderId}
        """
        oid = str(order_id or "").strip()
        if not oid:
            return None
        try:
            res = await self._request_async(
                "GET",
                f"/api/v1/orders/{oid}",
                group="ORDER_HISTORY",
                require_account=True,
                timeout=7.0,
            )
            return res if isinstance(res, dict) else None
        except Exception as exc:
            logger.debug("주문 상세 조회 예외(%s): %s", oid, exc)
            return None

    async def find_latest_open_order_id(
        self, symbol: str, side: Optional[str] = None
    ) -> str:
        """
        Case 24: 거래소에 현재 살아있는(OPEN) 해당 종목(및 방향)의 최신 orderId 를 반환한다.
        """
        sym = str(symbol or "").upper().strip()
        side_up = str(side or "").upper().strip() if side else ""
        open_orders = await self.get_orders(status="OPEN", symbol=sym)
        for ord_item in open_orders:
            if not isinstance(ord_item, dict):
                continue
            if str(ord_item.get("symbol") or "").upper() != sym:
                continue
            if side_up and str(ord_item.get("side") or "").upper() != side_up:
                continue
            oid = str(ord_item.get("orderId") or "").strip()
            if oid:
                return oid
        return ""

    # -----------------------------------------------------------------------
    # [Order] 주문 생성(POST /orders) · 정정(POST /modify) · 취소(POST /cancel)
    # -----------------------------------------------------------------------
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
        API.txt POST /api/v1/orders & 제5헌법 & Case 09, 14, 60:
        - 오직 REAL_ALLOWED_SYMBOLS ("SOXS", "SOXL") 및 지정가(LIMIT)만 허용
        - 데이장(19:00 ~ 03:57 EST) 구간에는 실거래 신규 매수(BUY)를 100% 물리적으로 차단!
        - 고유 clientOrderId(최대 36자)를 페이로드에 자동 주입하여 멱등성 보장
        """
        sym = str(symbol or "").upper().strip()
        if sym not in REAL_ALLOWED_SYMBOLS:
            raise ValueError(f"[Case 09 위반] 비허용 종목 주문 차단: {sym}")

        side_up = str(side or "").upper().strip()
        if side_up not in ("BUY", "SELL"):
            raise ValueError(f"유효하지 않은 주문 방향: {side_up}")

        # 🚨 Case 60 & 🎯 1: 데이장(19:00 ~ 03:57 EST) 실거래 매수 100% 물리적 차단!
        if side_up == "BUY" and enforce_daymarket_block:
            now_est = datetime.now(EST_TZ)
            hm = now_est.hour * 100 + now_est.minute
            if hm >= 1900 or hm <= 357:
                raise RuntimeError(
                    f"[Case 60 데이장 실매수 물리적 차단] 19:00~03:57 EST 구간({hm:04d} EST) 실거래 BUY 금지!"
                )

        qty_int = max(1, safe_int(quantity, 1))
        otype = str(order_type or "LIMIT").upper().strip()
        tif = str(time_in_force or "DAY").upper().strip()
        cid = (
            str(client_order_id).strip()[:36]
            if client_order_id
            else generate_client_order_id(prefix=f"{side_up[:1]}{sym}")
        )

        body: Dict[str, Any] = {
            "clientOrderId": cid,
            "symbol": sym,
            "side": side_up,
            "orderType": otype,
            "timeInForce": tif,
            "quantity": str(qty_int),
        }
        if otype == "LIMIT":
            body["price"] = format_us_order_price(price)

        res = await self._request_async(
            "POST",
            "/api/v1/orders",
            group="ORDER",
            json_body=body,
            require_account=True,
            timeout=10.0,
        )
        return res if isinstance(res, dict) else {}

    async def modify_order(
        self,
        order_id: str,
        new_price: float,
        order_type: str = "LIMIT",
        symbol: Optional[str] = None,
        side: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        API.txt POST /api/v1/orders/{orderId}/modify & Case 24, 74:
        - 미국 주식(US) 주문 정정 시 quantity 파라미터를 완전 배제(400 us-modify-quantity-not-supported 방어)
        - 409 already-modified 발생 시 거래소의 최신 OPEN 주문 ID 를 자동 추적하여 정정 완수!
        """
        oid = str(order_id or "").strip()
        if not oid:
            raise ValueError("modify_order: order_id 가 비어 있습니다.")

        otype = str(order_type or "LIMIT").upper().strip()
        body: Dict[str, Any] = {
            "orderType": otype,
            "price": format_us_order_price(new_price),
        }

        try:
            res = await self._request_async(
                "POST",
                f"/api/v1/orders/{oid}/modify",
                group="ORDER",
                json_body=body,
                require_account=True,
                timeout=9.0,
            )
            return res if isinstance(res, dict) else {"orderId": oid}
        except _NonRetryableApiError as exc:
            # Case 24: 409 already-modified 시 최신 OPEN 주문 ID 추적 후 즉시 재정정
            if exc.error_code == "already-modified" and symbol:
                latest_oid = await self.find_latest_open_order_id(
                    symbol=symbol, side=side
                )
                if latest_oid and latest_oid != oid:
                    res2 = await self._request_async(
                        "POST",
                        f"/api/v1/orders/{latest_oid}/modify",
                        group="ORDER",
                        json_body=body,
                        require_account=True,
                        timeout=9.0,
                    )
                    return (
                        res2
                        if isinstance(res2, dict)
                        else {"orderId": latest_oid}
                    )
            raise

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        API.txt POST /api/v1/orders/{orderId}/cancel & Case 75:
        반드시 빈 JSON 객체({})를 본문에 주입하여 HTTP 415 Unsupported Media Type 에러를 원천 소각한다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return {}
        try:
            res = await self._request_async(
                "POST",
                f"/api/v1/orders/{oid}/cancel",
                group="ORDER",
                json_body={},
                require_account=True,
                timeout=8.0,
            )
            return res if isinstance(res, dict) else {"orderId": oid}
        except _NonRetryableApiError as exc:
            if exc.error_code in ("already-canceled", "already-filled", "order-not-found"):
                return {"orderId": oid, "status": exc.error_code}
            raise

    # -----------------------------------------------------------------------
    # [제6헌법 & Case 10, 31] GET /api/v1/market-calendar/US (30초 캐싱 & 평일 강제 개장 폴백)
    # -----------------------------------------------------------------------
    async def get_us_market_calendar(
        self, date_str: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        API.txt GET /api/v1/market-calendar/US & 제6헌법 & Case 10, 31:
        미국 장 운영 정보를 30초간 캐싱 조회하며, API 지연이나 에러 시
        평일(월~금) 강제 개장(is_open_day = True)으로 Fail-Open 폴백한다.
        """
        now_est = datetime.now(EST_TZ)
        target_date = str(date_str or now_est.strftime("%Y-%m-%d")).strip()

        with self._cal_lock:
            if (
                self._cal_cache.get("date") == target_date
                and (time.monotonic() - safe_float(self._cal_cache.get("updated_mono"), 0.0)) < 30.0
                and self._cal_cache.get("data")
            ):
                return dict(self._cal_cache["data"])

        is_weekday = now_est.weekday() in (0, 1, 2, 3, 4)
        fallback_cal = {
            "date": target_date,
            "is_open_day": is_weekday,
            "fallback": True,
        }

        try:
            res = await self._request_async(
                "GET",
                "/api/v1/market-calendar/US",
                group="MARKET_INFO",
                params={"date": target_date},
                require_account=False,
                timeout=5.0,
            )
            if isinstance(res, dict) and isinstance(res.get("today"), dict):
                today_obj = res["today"]
                has_reg = today_obj.get("regularMarket") is not None
                out = {
                    "date": str(today_obj.get("date") or target_date),
                    "is_open_day": bool(has_reg),
                    "dayMarket": today_obj.get("dayMarket"),
                    "preMarket": today_obj.get("preMarket"),
                    "regularMarket": today_obj.get("regularMarket"),
                    "afterMarket": today_obj.get("afterMarket"),
                    "fallback": False,
                }
                with self._cal_lock:
                    self._cal_cache = {
                        "date": target_date,
                        "updated_mono": time.monotonic(),
                        "data": out,
                    }
                return out
        except Exception as exc:
            logger.debug("US 마켓 캘린더 Fail-Open 폴백 적용: %s", exc)

        return fallback_cal
