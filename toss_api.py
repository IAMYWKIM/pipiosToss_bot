# -*- coding: utf-8 -*-
"""
# MODIFIED: toss_api.py
토스증권(Toss) Open API v1.2.14 방탄 클라이언트, GlobalThrottle 중앙 통제기,
_HybridFileLock 하이브리드 파일 락 및 순수 롱(SOXL) 2-Tier 지층관리 원자적 원장(AssassinLedger) 코어 (Step 01 / 09)
- NEW: 인버스 숏(SOXS) 및 데이장/1주 휩소 영구 폐기에 따라 REAL_ALLOWED_SYMBOLS = ("SOXL",) 단일 롱 하드 락온
- NEW: 04:07 ~ 16:00 EST 운용 시간대 외 자동 매수 주문 물리적 차단 가드 장착
- MODIFIED: AssassinLedger 를 '8% 황금비율 정액 기반 정량제 + 자투리/수익금 복리 산입 + 10분/30분 시드보호 게이트 +
  1분 상시 스나이퍼 매도 + 무팽창 True Always-L1 리앵커링' 전용 2-Tier 지층 원장(state_SOXL.json / soxl_stratum_ledger.json)으로 전면 개편
- 제1헌법 ~ 제6헌법 및 Case 01 ~ 58 완벽 준수
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

# 🚨 Case 09: 순수 롱(SOXL 단독) 허용 종목 화이트리스트 하드 락온 (숏 SOXS 영구 폐기)
TARGET_SYMBOL: str = "SOXL"
REAL_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)
READONLY_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)

TOSS_API_BASE_URL: str = "https://openapi.tossinvest.com"

# 4xx 즉시 탈출 대상 상태 코드 (Case 21: 백오프 재시도 금지)
NON_RETRYABLE_STATUS_CODES: Tuple[int, ...] = (400, 403, 404, 409, 415, 422)

# 기본 초기 시드머니 ($10,000.00) 및 8%(12.5분할) 황금비율 상수
DEFAULT_INITIAL_SEED_USD: float = 10000.0
PORTION_RATIO_LOCKED: float = 0.08


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
                    if (
                        len(val) >= 2
                        and val[0] == val[-1]
                        and val[0] in ("'", '"')
                    ):
                        val = val[1:-1]
                    if key and (
                        override
                        or key not in os.environ
                        or not os.environ.get(key)
                    ):
                        os.environ[key] = val
        except Exception as exc:
            logger.debug(".env 로드 예외(%s): %s", env_path, exc)


_autoload_dotenv_files(override=False)


def generate_client_order_id(prefix: str = "SOXL") -> str:
    """
    🚨 제5헌법:
    토스증권 주문 생성 시 최대 36자 영숫자 및 '-', '_' 규칙(^[a-zA-Z0-9\\-_]+$)을 만족하는
    고유 멱등성 키(clientOrderId)를 생성한다.
    """
    clean_prefix = (
        re.sub(r"[^a-zA-Z0-9]", "", str(prefix or "SOXL"))[:8] or "SOXL"
    )
    ts_ms = int(time.time() * 1000) % 1000000000
    rand_hex = uuid.uuid4().hex[:12]
    cid = f"{clean_prefix}-{ts_ms}-{rand_hex}"
    return cid[:36]


def format_us_order_price(price: float) -> str:
    """
    API.txt OrderCreateRequest / OrderModifyRequest 가격 포맷 규격:
    - US 주식 $1 이상: 소수점 둘째 자리까지
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
class _NonRetryableApiError(RuntimeError):
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
# NEW : AssassinLedger (순수 롱 SOXL 2-Tier 지층 · 8% 황금정량 · 복리 시드 원장 관리기)
# ---------------------------------------------------------------------------
def _normalize_bucket_dict(raw_b: Any) -> Optional[Dict[str, Any]]:
    """
    JSON 에서 로드된 지층 딕셔너리의 수치형 필드를 정규화하고 유효성(qty > 0)을 검증한다.
    """
    if not isinstance(raw_b, dict):
        return None
    qty = safe_int(raw_b.get("qty"), 0)
    if qty <= 0:
        return None
    book_p = max(0.01, safe_float(raw_b.get("book_price"), 0.01))
    book_inv = max(
        0.01 * float(qty),
        safe_float(raw_b.get("book_invested_usd"), book_p * float(qty)),
    )
    actual_cost = max(
        0.01 * float(qty),
        safe_float(raw_b.get("actual_cost_usd"), book_inv),
    )
    lot_cnt = max(1, safe_int(raw_b.get("lot_count"), 1))
    lot_type = str(raw_b.get("lot_type") or "QUOTA_L1").strip()
    return {
        "qty": qty,
        "book_price": round(book_p, 4),
        "book_invested_usd": round(book_inv, 4),
        "actual_cost_usd": round(actual_cost, 4),
        "lot_count": lot_cnt,
        "lot_type": lot_type,
    }


def _default_soxl_stratum_state() -> Dict[str, Any]:
    """
    순수 롱(SOXL) 8% 황금비율 정액 기반 정량제 × 2-Tier 리앵커링 × 무팽창 Always-L1 기본 원장 스키마.
    """
    return {
        "symbol": TARGET_SYMBOL,
        "is_active": True,
        "is_compound": True,
        "portion_mode": "COMP_08PCT",         # "COMP_08PCT" (총자산 8% 복리) | "FIXED_800" ($800 고정 정량)
        "threshold_mode": "08_VREV_PROG",     # "08_VREV_PROG" (1층+1.5%/2층+1.0%) | "01_PROG_LOT" (+1%/+1.5%/+2%)
        "buy_interval_min": 10,               # 10 (10분 시드보호 매수 게이트) | 30 (30분 시드보호 매수 게이트)
        "initial_seed_usd": DEFAULT_INITIAL_SEED_USD,
        "budget": DEFAULT_INITIAL_SEED_USD,   # 하위 호환 별칭 (initial_seed_usd 와 상시 동기화)
        "free_cash_usd": DEFAULT_INITIAL_SEED_USD,
        "buckets": [],                        # List[Dict]: 최대 2개 지층 ([0]=1층 L1, [1]=2층 MERGED_UPPER)
        "anchor_price": 0.0,                  # 직전 매매 기준가 (8% 정량 주수 산출 기준)
        "cycle_min_buy_price": 0.0,           # 현재 사이클 역대 최저 매수가 (무팽창 ANTI_BLOAT 신저가 판정용)
        "ratchet_anchor_price": 0.0,          # 2층 단독 보유 시 무팽창 상승 래칫 기준가
        "last_evaluated_buy_bucket_id": -1,   # 동일 10m/30m 버킷 내 중복 매수 평가 방지 락
        "last_evaluated_date": "",
        # 미체결 주문 추적 필드
        "buy_order_id": "",
        "sell_order_id": "",
        "client_order_id": "",
        "ordered_qty": 0,
        "buy_price": 0.0,
        "pending_portion_budget": 0.0,
        "pending_lot_type": "",
        "pending_sell_mode": "",              # "SELL_L1_ONLY" | "SELL_ALL_GRAD" | "SELL_L2_GRAD"
        "target_sell_price": 0.0,
        # 5개년 모델 누적 통계 필드
        "total_buy_events": 0,
        "total_buy_shares": 0,
        "total_sell_events": 0,
        "total_sold_shares": 0,
        "win_sell_events": 0,
        "reanchor_cnt": 0,
        "cumulative_reanchor_drop_usd": 0.0,
        "extra_13plus_buy_events": 0,
        "seed_protect_pass_cnt": 0,
        "cumulative_leftover_saved_usd": 0.0,
        "cumulative_realized_gross_usd": 0.0,
        "cumulative_realized_fee_usd": 0.0,
        "cumulative_realized_net_usd": 0.0,
        # 일일 결산 통계 필드 (16:00 EST 정규장 마감 리포트용)
        "daily_realized_net_usd": 0.0,
        "daily_buy_events": 0,
        "daily_sell_events": 0,
        "daily_seed_protect_pass_cnt": 0,
        "daily_leftover_saved_usd": 0.0,
        "daily_reanchor_cnt": 0,
        "last_daily_report_date": "",
        "last_modify_mono": 0.0,
        "entry_ts": 0.0,
        "updated_at": "",
    }


class AssassinLedger:
    """
    🚨 제1헌법 & Case 02, 04 ~ 06, 09, 25 ~ 58:
    순수 롱(SOXL) 단일 종목 2-Tier 지층관리 원장(state_SOXL.json 및 soxl_stratum_ledger.json)을
    인메모리 캐시와 원자적 파일 덮어쓰기(.tmp 기록 + fsync + os.replace)로 동기화 관리하는 클래스.
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        if base_dir and os.path.isdir(base_dir):
            self.base_dir = os.path.abspath(base_dir)
        elif os.path.isdir("/home/pipiosToss"):
            self.base_dir = "/home/pipiosToss"
        else:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self._mem_lock = threading.RLock()
        self._state: Dict[str, Any] = self._load_soxl_state_sync()

    def _validate_symbol(self, symbol: str = TARGET_SYMBOL) -> str:
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
        if sym != TARGET_SYMBOL:
            raise ValueError(
                f"[Case 09 위반] 순수 롱(SOXL) 전용 원장에 비허용 종목 접근 차단: {sym}"
            )
        return TARGET_SYMBOL

    def _get_primary_filepath(self) -> str:
        return os.path.join(self.base_dir, f"state_{TARGET_SYMBOL}.json")

    def _get_stratum_mirror_filepath(self) -> str:
        return os.path.join(self.base_dir, "soxl_stratum_ledger.json")

    def _reconcile_accounting_identity_in_place(
        self, st: Dict[str, Any]
    ) -> None:
        """
        🎯 5 [회계 항등식(Accounting Identity) 무결성 강제]:
        가용 현금(free_cash_usd) + 보유 지층 실투입 원금 합계(sum(actual_cost_usd))
        == 초기 시드(initial_seed_usd) + 누적 세후 실현순익(cumulative_realized_net_usd)
        항등식을 상시 동기화한다.
        """
        raw_buckets = st.get("buckets")
        clean_buckets: List[Dict[str, Any]] = []
        if isinstance(raw_buckets, list):
            for b in raw_buckets:
                nb = _normalize_bucket_dict(b)
                if nb is not None:
                    clean_buckets.append(nb)
        st["buckets"] = clean_buckets

        init_seed = max(
            100.0,
            safe_float(
                st.get("initial_seed_usd"),
                safe_float(st.get("budget"), DEFAULT_INITIAL_SEED_USD),
            ),
        )
        st["initial_seed_usd"] = round(init_seed, 4)
        st["budget"] = round(init_seed, 2)

        cum_net = safe_float(st.get("cumulative_realized_net_usd"), 0.0)
        invested_actual = sum(
            safe_float(b.get("actual_cost_usd"), 0.0) for b in clean_buckets
        )
        expected_free_cash = max(0.0, (init_seed + cum_net) - invested_actual)
        st["free_cash_usd"] = round(expected_free_cash, 4)

        p_mode = str(st.get("portion_mode") or "COMP_08PCT").upper().strip()
        if p_mode not in ("COMP_08PCT", "FIXED_800"):
            p_mode = "COMP_08PCT" if bool(st.get("is_compound", True)) else "FIXED_800"
        st["portion_mode"] = p_mode
        st["is_compound"] = bool(p_mode == "COMP_08PCT")

        t_mode = str(st.get("threshold_mode") or "").upper().strip()
        if t_mode not in ("08_VREV_PROG", "01_PROG_LOT"):
            t_mode = "08_VREV_PROG" if p_mode == "COMP_08PCT" else "01_PROG_LOT"
        st["threshold_mode"] = t_mode

        b_int = safe_int(st.get("buy_interval_min"), 10)
        st["buy_interval_min"] = 30 if b_int == 30 else 10

    def _load_soxl_state_sync(self) -> Dict[str, Any]:
        primary_fp = self._get_primary_filepath()
        mirror_fp = self._get_stratum_mirror_filepath()
        base = _default_soxl_stratum_state()

        target_fp = (
            mirror_fp
            if os.path.isfile(mirror_fp)
            else (primary_fp if os.path.isfile(primary_fp) else primary_fp)
        )

        with GlobalThrottle.get_file_lock(primary_fp):
            if not os.path.isfile(target_fp):
                self._reconcile_accounting_identity_in_place(base)
                self._write_soxl_state_sync(base)
                return base
            try:
                with open(target_fp, "r", encoding="utf-8") as fp:
                    loaded = json.load(fp)
                if isinstance(loaded, dict):
                    # 구버전 budget 값만 존재하고 initial_seed_usd 가 없을 경우 자연스럽게 승계
                    if "initial_seed_usd" not in loaded and "budget" in loaded:
                        b_old = safe_float(loaded.get("budget"), DEFAULT_INITIAL_SEED_USD)
                        if b_old >= 100.0:
                            base["initial_seed_usd"] = b_old
                            base["budget"] = b_old
                    for k, default_v in base.items():
                        if k == "buckets":
                            if isinstance(loaded.get("buckets"), list):
                                base["buckets"] = loaded["buckets"]
                            continue
                        if k in loaded and loaded[k] is not None:
                            if isinstance(default_v, bool):
                                base[k] = bool(loaded[k])
                            elif isinstance(default_v, int):
                                base[k] = safe_int(loaded[k], default_v)
                            elif isinstance(default_v, float):
                                base[k] = safe_float(loaded[k], default_v)
                            else:
                                base[k] = str(loaded[k])
                    base["symbol"] = TARGET_SYMBOL
            except Exception as exc:
                logger.warning("⚠️ [SOXL] 지층 원장 파일 로드 폴백 사용: %s", exc)

            self._reconcile_accounting_identity_in_place(base)
            self._write_soxl_state_sync(base)
        return base

    def _write_soxl_state_sync(self, state_data: Dict[str, Any]) -> None:
        primary_fp = self._get_primary_filepath()
        mirror_fp = self._get_stratum_mirror_filepath()

        with GlobalThrottle.get_file_lock(primary_fp):
            try:
                os.makedirs(self.base_dir, exist_ok=True)
                payload = dict(state_data)
                payload["symbol"] = TARGET_SYMBOL
                payload["updated_at"] = datetime.now(EST_TZ).strftime(
                    "%Y-%m-%d %H:%M:%S EST"
                )
                for dst_fp in (primary_fp, mirror_fp):
                    tmp_path = f"{dst_fp}.tmp"
                    with open(tmp_path, "w", encoding="utf-8") as fp:
                        json.dump(payload, fp, ensure_ascii=False, indent=2)
                        fp.flush()
                        os.fsync(fp.fileno())
                    os.replace(tmp_path, dst_fp)
            except Exception as exc:
                logger.error("🚨 [SOXL] 지층 원장 원자적 기록 실패: %s", exc)

    def get_state(self, symbol: str = TARGET_SYMBOL) -> Dict[str, Any]:
        self._validate_symbol(symbol)
        with self._mem_lock:
            snap = dict(self._state)
            snap["buckets"] = [
                dict(b) for b in self._state.get("buckets", []) if isinstance(b, dict)
            ]
            return snap

    async def update_state(
        self, symbol: str = TARGET_SYMBOL, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        제1헌법 & Case 04 ~ 06:
        인메모리 지층 원장 상태를 즉시 갱신(회계 항등식 자동 보정 포함)하고,
        백그라운드 스레드(asyncio.to_thread)를 통해 state_SOXL.json 및 soxl_stratum_ledger.json 에 원자적으로 덮어쓴다.
        """
        self._validate_symbol(symbol)
        with self._mem_lock:
            cur = self._state
            if "budget" in kwargs and "initial_seed_usd" not in kwargs:
                kwargs["initial_seed_usd"] = max(
                    100.0, safe_float(kwargs["budget"], DEFAULT_INITIAL_SEED_USD)
                )
            if "is_compound" in kwargs and "portion_mode" not in kwargs:
                is_c = bool(kwargs["is_compound"])
                kwargs["portion_mode"] = "COMP_08PCT" if is_c else "FIXED_800"
                kwargs["threshold_mode"] = (
                    "08_VREV_PROG" if is_c else "01_PROG_LOT"
                )

            for k, v in kwargs.items():
                cur[k] = v
            cur["symbol"] = TARGET_SYMBOL
            self._reconcile_accounting_identity_in_place(cur)
            snapshot = dict(cur)
            snapshot["buckets"] = [
                dict(b) for b in cur.get("buckets", []) if isinstance(b, dict)
            ]

        await asyncio.wait_for(
            asyncio.to_thread(self._write_soxl_state_sync, snapshot),
            timeout=5.0,
        )
        return snapshot

    async def set_initial_seed_usd(self, new_seed_usd: float) -> Dict[str, Any]:
        """
        초기 기준 시드머니(initial_seed_usd)를 변경하고 회계 항등식에 맞춰 가용 현금(free_cash_usd)을 재동기화한다.
        """
        clean_seed = round(max(100.0, safe_float(new_seed_usd, DEFAULT_INITIAL_SEED_USD)), 2)
        return await self.update_state(
            TARGET_SYMBOL,
            initial_seed_usd=clean_seed,
            budget=clean_seed,
        )

    async def reset_stratum_ledger_preserve_seed(
        self, new_seed_usd: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        🚨 Case 02:
        /reset 명령 시 SOXL 지층 큐(buckets), 기준가(anchor_price, cycle_min_buy_price, ratchet_anchor_price),
        미체결 주문 ID를 원자적으로 0점 초기화한다.
        - new_seed_usd 미지정 시: 기존 총 실현 자산(initial_seed_usd + cumulative_realized_net_usd)과 운용 모드를 보존한다.
        - new_seed_usd 지정 시(/reset_seed): 해당 금액으로 초기 시드를 재설정하고 누적 손익 통계를 0으로 리셋한다.
        """
        st = self.get_state(TARGET_SYMBOL)
        if new_seed_usd is not None and safe_float(new_seed_usd, 0.0) >= 100.0:
            preserved_seed = round(safe_float(new_seed_usd, DEFAULT_INITIAL_SEED_USD), 2)
            cum_net = 0.0
            cum_gross = 0.0
            cum_fee = 0.0
            cum_saved = 0.0
            cum_reanchor_drop = 0.0
            reanchor_c = 0
            pass_c = 0
            buy_ev = 0
            buy_sh = 0
            sell_ev = 0
            sell_sh = 0
            win_ev = 0
            extra_13 = 0
        else:
            preserved_seed = max(
                100.0, safe_float(st.get("initial_seed_usd"), DEFAULT_INITIAL_SEED_USD)
            )
            cum_net = safe_float(st.get("cumulative_realized_net_usd"), 0.0)
            cum_gross = safe_float(st.get("cumulative_realized_gross_usd"), 0.0)
            cum_fee = safe_float(st.get("cumulative_realized_fee_usd"), 0.0)
            cum_saved = safe_float(st.get("cumulative_leftover_saved_usd"), 0.0)
            cum_reanchor_drop = safe_float(
                st.get("cumulative_reanchor_drop_usd"), 0.0
            )
            reanchor_c = safe_int(st.get("reanchor_cnt"), 0)
            pass_c = safe_int(st.get("seed_protect_pass_cnt"), 0)
            buy_ev = safe_int(st.get("total_buy_events"), 0)
            buy_sh = safe_int(st.get("total_buy_shares"), 0)
            sell_ev = safe_int(st.get("total_sell_events"), 0)
            sell_sh = safe_int(st.get("total_sold_shares"), 0)
            win_ev = safe_int(st.get("win_sell_events"), 0)
            extra_13 = safe_int(st.get("extra_13plus_buy_events"), 0)

        return await self.update_state(
            TARGET_SYMBOL,
            is_active=bool(st.get("is_active", True)),
            portion_mode=str(st.get("portion_mode") or "COMP_08PCT"),
            threshold_mode=str(st.get("threshold_mode") or "08_VREV_PROG"),
            buy_interval_min=safe_int(st.get("buy_interval_min"), 10),
            initial_seed_usd=preserved_seed,
            budget=preserved_seed,
            buckets=[],
            anchor_price=0.0,
            cycle_min_buy_price=0.0,
            ratchet_anchor_price=0.0,
            last_evaluated_buy_bucket_id=-1,
            last_evaluated_date="",
            buy_order_id="",
            sell_order_id="",
            client_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            pending_portion_budget=0.0,
            pending_lot_type="",
            pending_sell_mode="",
            target_sell_price=0.0,
            total_buy_events=buy_ev,
            total_buy_shares=buy_sh,
            total_sell_events=sell_ev,
            total_sold_shares=sell_sh,
            win_sell_events=win_ev,
            reanchor_cnt=reanchor_c,
            cumulative_reanchor_drop_usd=cum_reanchor_drop,
            extra_13plus_buy_events=extra_13,
            seed_protect_pass_cnt=pass_c,
            cumulative_leftover_saved_usd=cum_saved,
            cumulative_realized_gross_usd=cum_gross,
            cumulative_realized_fee_usd=cum_fee,
            cumulative_realized_net_usd=cum_net,
            daily_realized_net_usd=0.0,
            daily_buy_events=0,
            daily_sell_events=0,
            daily_seed_protect_pass_cnt=0,
            daily_leftover_saved_usd=0.0,
            daily_reanchor_cnt=0,
            last_modify_mono=0.0,
            entry_ts=0.0,
        )

    async def reset_dual_ledgers_preserve_seed(self) -> Dict[str, Any]:
        """
        구버전 호환 별칭: reset_stratum_ledger_preserve_seed 호출로 직결한다.
        """
        return await self.reset_stratum_ledger_preserve_seed()


# ---------------------------------------------------------------------------
# TossApiClient : 토스증권 Open API v1.2.14 방탄 비동기 클라이언트
# ---------------------------------------------------------------------------
class TossApiClient:
    """
    제1헌법 ~ 제6헌법 및 API.txt v1.2.14 전 엔드포인트 완벽 구현 클라이언트 (순수 롱 SOXL 전용).
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

        # US 장 운영 캘린더 30초 캐시 (Case 10 & 제6헌법)
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
            resp = self._session.post(
                url, headers=headers, data=data, timeout=10.0
            )
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
            if (
                not force_refresh
                and self._account_seq is not None
                and self._account_seq > 0
            ):
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
                token = self._ensure_token_sync(force_refresh=False)
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

                if resp.status_code == 401 and attempt < len(backoff_delays):
                    self._ensure_token_sync(force_refresh=True)
                    continue

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

                if resp.status_code in NON_RETRYABLE_STATUS_CODES:
                    raise _NonRetryableApiError(
                        status_code=resp.status_code,
                        error_code=err_code,
                        error_message=err_msg,
                        error_data=err_data,
                    )

                if resp.status_code == 429:
                    retry_after = safe_float(
                        resp.headers.get("Retry-After"),
                        backoff_delays[min(attempt, 2)],
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
    # [Market Data] 현재가 조회 · 호가 조회 · 캔들 차트(단일/다중 페이지) 조회
    # -----------------------------------------------------------------------
    async def get_current_prices_batch(
        self, symbols: Iterable[str] = READONLY_ALLOWED_SYMBOLS
    ) -> Dict[str, float]:
        """
        API.txt GET /api/v1/prices?symbols=SOXL
        """
        valid_syms = [
            str(s).upper().strip()
            for s in symbols
            if str(s).upper().strip() in READONLY_ALLOWED_SYMBOLS
        ]
        if not valid_syms:
            valid_syms = [TARGET_SYMBOL]

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

    async def get_orderbook(self, symbol: str = TARGET_SYMBOL) -> Dict[str, Any]:
        """
        API.txt GET /api/v1/orderbook?symbol=SOXL
        """
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
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
        self, symbol: str = TARGET_SYMBOL, fallback_price: float = 0.0
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
        self, symbol: str = TARGET_SYMBOL, fallback_price: float = 0.0
    ) -> float:
        ob = await self.get_orderbook(symbol)
        asks = ob.get("asks") if isinstance(ob, dict) else []
        if isinstance(asks, list) and asks:
            valid_asks = [
                safe_float(a.get("price"), 0.0)
                for a in asks
                if isinstance(a, dict)
                and safe_float(a.get("price"), 0.0) > 0.0
            ]
            if valid_asks:
                return min(valid_asks)
        return safe_float(fallback_price, 0.0)

    async def get_candles(
        self,
        symbol: str = TARGET_SYMBOL,
        interval: str = "1m",
        count: int = 100,
        before: Optional[str] = None,
        adjusted: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        API.txt GET /api/v1/candles (Rate Limits Group: MARKET_DATA_CHART)
        """
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
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
                res.get("candles")
                if isinstance(res.get("candles"), list)
                else []
            )
            next_b = res.get("nextBefore")
            return c_list, (str(next_b) if next_b else None)
        return [], None

    async def get_candles_multi_page(
        self,
        symbol: str = TARGET_SYMBOL,
        interval: str = "1m",
        total_count: int = 400,
        adjusted: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        다중 페이지 조회로 04:07 ~ 16:00 EST 시계열 분봉을 누락 없이 확보한다.
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
        krw / usd null 결측치를 방어하며 순수 롱 SOXL 의 실보유 수량 및 평균 매입가를 반환한다.
        """
        out: Dict[str, Dict[str, Any]] = {
            TARGET_SYMBOL: {
                "symbol": TARGET_SYMBOL,
                "qty": 0,
                "avg_price": 0.0,
                "last_price": 0.0,
                "purchase_amount": 0.0,
                "market_value": 0.0,
                "profit_loss_usd": 0.0,
                "profit_loss_rate": 0.0,
            }
        }
        try:
            res = await self._request_async(
                "GET",
                "/api/v1/holdings",
                group="ASSET",
                params={"symbol": TARGET_SYMBOL},
                require_account=True,
                timeout=8.0,
            )
            items = res.get("items") if isinstance(res, dict) else []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sym = str(item.get("symbol") or "").upper().strip()
                    if sym != TARGET_SYMBOL:
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
                    out[TARGET_SYMBOL] = {
                        "symbol": TARGET_SYMBOL,
                        "qty": max(0, qty),
                        "avg_price": max(0.0, avg_p),
                        "last_price": max(0.0, last_p),
                        "purchase_amount": safe_float(
                            mv_obj.get("purchaseAmount"), 0.0
                        ),
                        "market_value": safe_float(mv_obj.get("amount"), 0.0),
                        "profit_loss_usd": safe_float(
                            pl_obj.get("amount"), 0.0
                        ),
                        "profit_loss_rate": safe_float(
                            pl_obj.get("rate"), 0.0
                        )
                        * 100.0,
                    }
        except Exception as exc:
            logger.warning("⚠️ 보유 주식(holdings) 조회 예외: %s", exc)
        return out

    async def get_buying_power_usd(
        self, safety_buffer_pct: float = 0.2
    ) -> float:
        """
        API.txt GET /api/v1/buying-power?currency=USD & Case 03, 25:
        현금 기반 USD 매수가능금액을 조회하고 안전 버퍼를 차감한 금액을 반환한다.
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

    async def get_sellable_quantity(self, symbol: str = TARGET_SYMBOL) -> int:
        """
        API.txt GET /api/v1/sellable-quantity?symbol=SOXL
        """
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
        if sym != TARGET_SYMBOL:
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
        symbol: Optional[str] = TARGET_SYMBOL,
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
        self, symbol: str = TARGET_SYMBOL, side: Optional[str] = None
    ) -> str:
        """
        Case 24: 거래소에 현재 살아있는(OPEN) SOXL (및 방향)의 최신 orderId 를 반환한다.
        """
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
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
        API.txt POST /api/v1/orders & 제5헌법 & Case 09, 14:
        - 오직 순수 롱 SOXL 단일 종목 및 지정가(LIMIT)만 허용 (SOXS 물리적 원천 차단!)
        - 04:07 ~ 16:00 EST 외 시간대(데이장 19:00 ~ 03:59 EST 및 프리장 초입 04:00 ~ 04:06 EST)에는
          실거래 신규 매수(BUY)를 100% 물리적으로 차단!
        - 고유 clientOrderId(최대 36자)를 페이로드에 자동 주입하여 멱등성 보장
        """
        sym = str(symbol or "").upper().strip()
        if sym != TARGET_SYMBOL:
            raise ValueError(
                f"[Case 09 위반] 순수 롱(SOXL) 외 비허용 종목 주문 물리적 차단: {sym}"
            )

        side_up = str(side or "").upper().strip()
        if side_up not in ("BUY", "SELL"):
            raise ValueError(f"유효하지 않은 주문 방향: {side_up}")

        # 🎯 2: 오직 프리장(04:07 EST)부터 정규장 마감(16:00 EST)까지만 운영 (데이장/장외 매수 물리적 차단)
        if side_up == "BUY" and enforce_daymarket_block:
            now_est = datetime.now(EST_TZ)
            hm = now_est.hour * 100 + now_est.minute
            if not (407 <= hm <= 1600):
                raise RuntimeError(
                    f"[운용시간 외 매수 차단] 04:07 ~ 16:00 EST 외 구간({hm:04d} EST) 실거래 BUY 금지!"
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
        symbol: Optional[str] = TARGET_SYMBOL,
        side: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        API.txt POST /api/v1/orders/{orderId}/modify & Case 24:
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
        API.txt POST /api/v1/orders/{orderId}/cancel:
        반드시 빈 JSON 객체({})를 본문에 주입하여 HTTP 415 에러를 원천 소각한다.
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
            if exc.error_code in (
                "already-canceled",
                "already-filled",
                "order-not-found",
            ):
                return {"orderId": oid, "status": exc.error_code}
            raise

    # -----------------------------------------------------------------------
    # [제6헌법 & Case 10] GET /api/v1/market-calendar/US (30초 캐싱 & 평일 강제 개장 폴백)
    # -----------------------------------------------------------------------
    async def get_us_market_calendar(
        self, date_str: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        API.txt GET /api/v1/market-calendar/US & 제6헌법 & Case 10:
        미국 장 운영 정보를 30초간 캐싱 조회하며, API 지연이나 에러 시
        평일(월 ~ 금) 강제 개장(is_open_day = True)으로 Fail-Open 폴백한다.
        """
        now_est = datetime.now(EST_TZ)
        target_date = str(date_str or now_est.strftime("%Y-%m-%d")).strip()

        with self._cal_lock:
            if (
                self._cal_cache.get("date") == target_date
                and (
                    time.monotonic()
                    - safe_float(self._cal_cache.get("updated_mono"), 0.0)
                )
                < 30.0
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
