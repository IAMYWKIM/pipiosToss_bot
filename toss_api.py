# -*- coding: utf-8 -*-
"""
# MODIFIED: toss_api.py
인피니트 스노우볼 (Infinite Snowball) v3.0 — 토스증권 Open API (v1.2.14) 방탄 통신 통제소 &
SOXL 단일 롱 7-Tier 순수 멀티 그리드(1층 공격수 + 2 ~ 6층 개별 완충층 + 통합 7층 본대) 폴더별 3중 지층 원장(SoxlStratumLedger) 코어 모듈 (Step 01 / 05)
- 제1헌법: 모든 파일 I/O 및 네트워크 통신의 asyncio.to_thread 비동기 격리 + GlobalThrottle 중앙 통제
- 제2헌법: 기동 시 GET /api/v1/accounts 1회 호출로 BROKERAGE accountSeq 메모리 락온 및 X-Tossinvest-Account 자동 주입
- 제3헌법 ~ 제6헌법 및 Case 01 ~ 72 전수 락온:
  1) REAL_ALLOWED_SYMBOLS = ("SOXL",) 단일 롱 화이트리스트 물리적 강제 (인버스 숏 SOXS 원천 차단)
  2) get_holdings_map_with_status, get_candles_1m, get_candles_1d, get_current_prices_batch,
     get_orderbook, get_buying_power_usd, get_sellable_quantity, create_order, modify_order,
     cancel_order, get_order_detail, get_open_orders, get_closed_orders, probe_latest_manual_fill_price,
     get_usd_krw_rate 등 전 메서드 100% 정식 구현!
  3) 미국 주식 주문 정정(/modify) 시 quantity 필드 원천 배제(400 us-modify-quantity-not-supported 차단) 및
     주문 취소(/cancel) 시 빈 JSON 객체 {} 전송 강제(HTTP 415 차단)
  4) 🚨 [Case 68 폴더 위치(base_dir) 기반 멀티 봇 자동 식별 격리 & 3중 원장 금고 & 시드 $100 변조 방어]
     - "/home/pipiosToss" 고정 경로 하드코딩 100% 영구 소각 -> 오직 실행 파일이 위치한 폴더(base_dir)의 .env 만 로드
     - 폴더명(folder_slug) 기반 ① 고유 원장(soxl_stratum_ledger_<폴더명>.json), ② 호환 원장(soxl_stratum_ledger.json),
       ③ 실시간 백업 금고(ledger_backups/soxl_stratum_ledger_<폴더명>.bak.json) 3중 원자적 동기화 및 기존 데이터 무손실 자동 이관
  5) 🚨 [Case 65, Case 69 & Case 70 양방향·주기 60초 타임쉴드 시각 영속화 및 순수 원가 무결성 강제]
     - last_buy_fill_ts, last_sell_fill_ts, last_cycle_end_ts, last_sell_gate_eval_ts 원장 영속화
  6) 🚨 [NEW · Case 63, Case 71 & Case 72 : 최종 확정 챔피언 🔥 03_7G_PURE_10_01 7-Layer 순수 원가 보존 및
     토스 CLOSED 주문 원장 역추적(probe_latest_manual_fill_price) & 기정산 주문 링버퍼(last_settled_order_ids) 탑재]
"""

from __future__ import annotations

import asyncio
import glob
import json
import logging
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("toss_sniper.toss_api")

# ---------------------------------------------------------------------------
# [핵심 상수] 제4헌법 EST 단일 타임존 & 최종 확정 챔피언 (🔥 03_7G_PURE_10_01 · Case 72) 전역 상수
# ---------------------------------------------------------------------------
EST_TZ = ZoneInfo("America/New_York")
KST_TZ = ZoneInfo("Asia/Seoul")

TARGET_SYMBOL: str = "SOXL"
REAL_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)
ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)

DEFAULT_INITIAL_SEED_USD: float = 10000.0
PORTION_RATIO_LOCKED: float = 0.08
BASE_MAX_LOTS_8PCT: int = 12

# NEW [Case 72]: 골디락스 7-Layer (1층 공격수 + 2 ~ 6층 개별 완충층 + 통합 7층 본대) 상수 락온
# (타 모듈 AST 임포트 무결성 100% 호환을 위해 MAX_GRID_TIERS_12 도 7 로 동시 매핑)
MAX_GRID_TIERS_7: int = 7
MAX_GRID_TIERS: int = 7
MAX_GRID_TIERS_12: int = 7

LOCKED_BUY_INTERVAL_MIN: int = 10
LOCKED_SELL_INTERVAL_MIN: int = 1
ROUND_TRIP_FEE_RATE: float = 0.0014
ROUND_TRIP_FEE_RATE_PCT: float = 0.14
REANCHOR_NET_BUFFER_MULT: float = 0.994
KRW_PER_USD_DEFAULT: float = 1380.0

TOSS_OPENAPI_BASE_URL: str = "https://openapi.tossinvest.com"


# ---------------------------------------------------------------------------
# [유틸리티] 안전한 수치 형변환 (ZeroDivision / NaN / inf / null 방어 — Case 03, 23)
# ---------------------------------------------------------------------------
def safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or isinstance(val, bool):
        return float(default)
    try:
        if isinstance(val, str):
            cleaned = (
                val.replace(",", "").replace("$", "").replace("%", "").strip()
            )
            if not cleaned:
                return float(default)
            f_val = float(cleaned)
        else:
            f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            return float(default)
        return f_val
    except Exception:
        return float(default)


def safe_int(val: Any, default: int = 0) -> int:
    if val is None or isinstance(val, bool):
        return int(default)
    try:
        f_val = safe_float(val, float(default))
        return (
            int(round(f_val))
            if abs(f_val - round(f_val)) < 1e-6
            else int(math.floor(f_val))
        )
    except Exception:
        return int(default)


def generate_client_order_id(prefix: str = "SNIPER") -> str:
    """
    🚨 제5헌법:
    토스증권 주문 생성 시 최대 36자 영숫자 및 '-', '_' 로 구성된 고유 멱등성 키(clientOrderId)를 생성한다.
    """
    clean_prefix = "".join(
        ch for ch in str(prefix or "ORD").upper() if ch.isalnum()
    )[:8]
    ts_part = int(time.time() * 1000) % 100000000
    rand_part = uuid.uuid4().hex[:12]
    cid = f"{clean_prefix}-{ts_part}-{rand_part}"
    return cid[:36]


class _NonRetryableApiError(Exception):
    """
    🚨 Case 21:
    4xx 클라이언트 비즈니스 에러(400, 401, 403, 404, 409, 415, 422) 발생 시
    불필요한 재시도 없이 즉시 탈출하기 위한 전용 예외 클래스.
    """

    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        raw_body: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = int(status_code)
        self.error_code = str(error_code or "unknown-error")
        self.message = str(message or "")
        self.raw_body = raw_body or {}
        super().__init__(
            f"[HTTP {self.status_code} · {self.error_code}] {self.message}"
        )


# ---------------------------------------------------------------------------
# [Case 68] 폴더 위치(base_dir) 기반 자동 식별기 및 격리 환경변수 로더
# ("/home/pipiosToss" 고정 경로 하드코딩 100% 영구 소각)
# ---------------------------------------------------------------------------
def get_bot_base_dir(explicit_dir: Optional[str] = None) -> str:
    """
    🚨 Case 68:
    고정 경로("/home/pipiosToss")를 절대 참조하지 않고, 오직 현재 봇 스크립트가 위치한
    실제 물리적 폴더(base_dir)의 절대 경로를 반환한다.
    """
    if explicit_dir and str(explicit_dir).strip():
        cand = os.path.abspath(str(explicit_dir).strip())
        if os.path.isfile(cand):
            return os.path.dirname(cand)
        return cand
    return os.path.abspath(os.path.dirname(os.path.abspath(__file__)) or ".")


def get_folder_slug(base_dir: Optional[str] = None) -> str:
    """
    🚨 Case 68:
    현재 봇이 실행 중인 폴더명(예: /home/pipiosToss_family1 -> "pipiosToss_family1")을
    안전한 파일명 식별자(slug)로 정규화하여 반환한다.
    """
    target_dir = get_bot_base_dir(base_dir)
    raw_name = os.path.basename(os.path.normpath(target_dir)).strip()
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_name).strip("._-")
    return slug if slug else "default_bot"


def resolve_folder_isolated_ledger_paths(
    base_dir: Optional[str] = None,
    explicit_filepath: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    """
    🚨 Case 68 [폴더별 3중 안전 원장 경로 결정기]:
    단일 서버 내 여러 폴더에서 다중 봇을 운영할 때 각 폴더 위치를 기준으로:
    1) primary_path : <base_dir>/soxl_stratum_ledger_<folder_slug>.json (Git 추적 면역 폴더 고유 원장)
    2) legacy_path  : <base_dir>/soxl_stratum_ledger.json (하위 호환 미러 원장)
    3) backup_path  : <base_dir>/ledger_backups/soxl_stratum_ledger_<folder_slug>.bak.json (실시간 백업 금고)
    4) folder_slug  : 현재 폴더 식별자 문자열
    을 산출하여 반환한다.
    """
    if explicit_filepath and str(explicit_filepath).strip():
        raw_fp = str(explicit_filepath).strip()
        if os.path.isabs(raw_fp):
            target_dir = os.path.dirname(raw_fp) or get_bot_base_dir(base_dir)
        else:
            target_dir = get_bot_base_dir(base_dir)
    else:
        target_dir = get_bot_base_dir(base_dir)

    folder_slug = get_folder_slug(target_dir)
    primary_path = os.path.join(
        target_dir, f"soxl_stratum_ledger_{folder_slug}.json"
    )
    legacy_path = os.path.join(target_dir, "soxl_stratum_ledger.json")
    backup_dir = os.path.join(target_dir, "ledger_backups")
    backup_path = os.path.join(
        backup_dir, f"soxl_stratum_ledger_{folder_slug}.bak.json"
    )
    return primary_path, legacy_path, backup_path, folder_slug


def _load_env_files_into_environ(
    base_dir: Optional[str] = None,
    override_existing: bool = False,
) -> str:
    """
    🚨 Case 68:
    "/home/pipiosToss" 고정 경로 폴백을 100% 폐기하고, 오직 해당 봇이 위치한 폴더(target_dir)의
    .env 및 설정 JSON 파일만 로드하여 폴더 간 환경변수 교차 오염을 원천 차단한다.
    """
    target_dir = get_bot_base_dir(base_dir)
    if not os.path.isdir(target_dir):
        return target_dir

    env_fp = os.path.join(target_dir, ".env")
    if os.path.isfile(env_fp):
        try:
            with open(env_fp, "r", encoding="utf-8", errors="ignore") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    if line.startswith("export "):
                        line = line[len("export ") :].strip()
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'").strip('"')
                    if k and (override_existing or k not in os.environ):
                        os.environ[k] = v
        except Exception:
            pass

    for json_name in ("config.json", "secrets.json", "toss_config.json"):
        j_fp = os.path.join(target_dir, json_name)
        if os.path.isfile(j_fp):
            try:
                with open(j_fp, "r", encoding="utf-8", errors="ignore") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        if isinstance(v, (str, int, float)):
                            k_str = str(k).strip()
                            if k_str and (
                                override_existing or k_str not in os.environ
                            ):
                                os.environ[k_str] = str(v)
            except Exception:
                pass

    return target_dir


_load_env_files_into_environ()


# ---------------------------------------------------------------------------
# [제1헌법] _HybridFileLock 및 GlobalThrottle (동기/비동기 양방향 호환 중앙 통제)
# ---------------------------------------------------------------------------
class _HybridFileLock:
    """
    🚨 제1헌법 & Case 06:
    일반 동기 `with GlobalThrottle.get_file_lock(path):` 와
    비동기 `async with GlobalThrottle.get_file_lock(path):` 를 모두 지원하는 하이브리드 락.
    """

    def __init__(self) -> None:
        self._thread_lock = threading.RLock()

    def __enter__(self) -> "_HybridFileLock":
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._thread_lock.release()

    async def __aenter__(self) -> "_HybridFileLock":
        await asyncio.to_thread(self._thread_lock.acquire)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._thread_lock.release()


class GlobalThrottle:
    """
    🚨 제1헌법 & Case 20:
    모든 API 호출 전 그룹별 최소 호출 간격(Rate Limit)을 강제하고,
    모든 로컬 파일 I/O 시 파일 경로별 상호 배제 Lock 을 제공한다.
    """

    _api_lock = threading.Lock()
    _last_call_mono: Dict[str, float] = {}
    _file_locks_guard = threading.Lock()
    _file_locks: Dict[str, _HybridFileLock] = {}

    GROUP_INTERVALS: Dict[str, float] = {
        "AUTH": 0.50,
        "ORDER": 0.18,
        "ORDER_HISTORY": 0.15,
        "ORDER_INFO": 0.15,
        "ASSET": 0.18,
        "MARKET_DATA": 0.12,
        "MARKET_DATA_CHART": 0.22,
        "MARKET_INFO": 0.25,
        "DEFAULT": 0.15,
    }

    @classmethod
    def wait_api_sync(cls, group: str = "DEFAULT") -> None:
        grp = str(group or "DEFAULT").upper().strip()
        min_gap = cls.GROUP_INTERVALS.get(
            grp, cls.GROUP_INTERVALS["DEFAULT"]
        )
        with cls._api_lock:
            now_m = time.monotonic()
            last_m = cls._last_call_mono.get(grp, 0.0)
            wait_s = min_gap - (now_m - last_m)
            if wait_s > 0.0:
                time.sleep(wait_s)
            cls._last_call_mono[grp] = time.monotonic()

    @classmethod
    async def wait_api(cls, group: str = "DEFAULT") -> None:
        await asyncio.to_thread(cls.wait_api_sync, group)

    @classmethod
    def get_file_lock(cls, filepath: str) -> _HybridFileLock:
        abs_p = os.path.abspath(str(filepath or "default.lock"))
        with cls._file_locks_guard:
            lock_obj = cls._file_locks.get(abs_p)
            if lock_obj is None:
                lock_obj = _HybridFileLock()
                cls._file_locks[abs_p] = lock_obj
            return lock_obj


# ---------------------------------------------------------------------------
# [핵심 2] SOXL 단일 롱 7-Tier 순수 멀티 그리드 폴더별 3중 안전 지층 원장 (SoxlStratumLedger — Case 68 ~ 72)
# ---------------------------------------------------------------------------
class SoxlStratumLedger:
    """
    🎯 1 ~ 🎯 5 & Case 02, 05, 06, 58 ~ 72:
    현재 봇이 위치한 폴더(`base_dir`)를 기준으로:
    ① 폴더 고유 원장(`soxl_stratum_ledger_<folder_slug>.json` — Git 덮어쓰기 100% 면역)
    ② 표준 호환 원장(`soxl_stratum_ledger.json` — 기존 코드 및 외부 도구 호환 미러)
    ③ 실시간 백업 금고(`ledger_backups/soxl_stratum_ledger_<folder_slug>.bak.json`)
    3곳에 SOXL 1 ~ 7층 순수 멀티 그리드 지층(1층 공격수 + 2 ~ 6층 개별 완충층 + 통합 7층 본대),
    가용 현금 시드(free_cash_usd), 정적 잠금 사이클 최저 매수가(cycle_min_buy_price),
    양방향·주기 60초 타임쉴드 시각(last_buy_fill_ts, last_sell_fill_ts, last_cycle_end_ts, last_sell_gate_eval_ts),
    다중 그리드 매도 대기 인덱스(pending_sell_indices), 기정산 주문 ID 링버퍼(last_settled_order_ids),
    누적 세후 실현수익금 및 자투리 절약금을 원자적(.tmp -> os.replace)으로 동시 영속화한다.
    """

    ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)

    def __init__(
        self,
        filepath: Optional[str] = None,
        base_dir: Optional[str] = None,
    ) -> None:
        self.base_dir: str = get_bot_base_dir(
            base_dir
            or (
                os.path.dirname(os.path.abspath(filepath))
                if filepath and os.path.isabs(str(filepath))
                else None
            )
        )
        (
            self.primary_filepath,
            self.legacy_filepath,
            self.backup_filepath,
            self.folder_slug,
        ) = resolve_folder_isolated_ledger_paths(
            base_dir=self.base_dir,
            explicit_filepath=filepath,
        )
        self.filepath: str = self.legacy_filepath
        self.backup_dir: str = os.path.dirname(self.backup_filepath)

        self._mem_lock = threading.RLock()
        self._last_known_valid_seed: float = self._read_env_initial_seed()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._load_sync()

    @staticmethod
    def _read_env_initial_seed() -> float:
        for env_k in (
            "INITIAL_SEED_USD",
            "TOSS_INITIAL_SEED_USD",
            "SEED_USD",
            "DEFAULT_INITIAL_SEED_USD",
        ):
            val = safe_float(os.environ.get(env_k), 0.0)
            if val > 100.0:
                return round(val, 4)
        return DEFAULT_INITIAL_SEED_USD

    @classmethod
    def _resolve_safe_initial_seed(
        cls,
        raw_st: Dict[str, Any],
        fallback_seed: float = DEFAULT_INITIAL_SEED_USD,
    ) -> float:
        """
        🚨 Case 68 [시드머니 $100 강제 변조 버그 영구 소각]:
        1) raw_seed > 100.0 이면 사용자 설정 시드로 정상 인정한다.
        2) raw_seed == 100.0 인 경우, 명시적 플래그(`seed_explicitly_set_100 == True`)가 없으면
           초기화 오류로 발생한 $100 변조로 판정하여 직전 정상 시드(fallback_seed) 또는 환경변수/기본값($10,000)으로 복구한다!
        3) raw_seed < 100.0 (0.0 또는 음수/결측)이면 즉시 fallback_seed (또는 $10,000)로 복원한다.
        """
        raw_val = (
            raw_st.get("initial_seed_usd")
            if isinstance(raw_st, dict)
            else None
        )
        raw_seed = safe_float(raw_val, 0.0)
        explicit_100 = bool(
            isinstance(raw_st, dict)
            and raw_st.get("seed_explicitly_set_100", False)
        )

        safe_fb = (
            fallback_seed
            if fallback_seed > 100.0
            else cls._read_env_initial_seed()
        )
        if safe_fb <= 100.0:
            safe_fb = DEFAULT_INITIAL_SEED_USD

        if raw_seed > 100.0:
            return round(raw_seed, 4)
        if abs(raw_seed - 100.0) < 1e-6 and explicit_100:
            return 100.0
        return round(safe_fb, 4)

    @classmethod
    def _default_symbol_state(
        cls,
        folder_slug: str = "default_bot",
        initial_seed: float = DEFAULT_INITIAL_SEED_USD,
    ) -> Dict[str, Any]:
        today_est = datetime.now(EST_TZ).strftime("%Y-%m-%d")
        clean_seed = (
            round(initial_seed, 4)
            if initial_seed >= 100.0
            else DEFAULT_INITIAL_SEED_USD
        )
        return {
            "symbol": TARGET_SYMBOL,
            "folder_slug": str(folder_slug or "default_bot"),
            "is_active": True,
            "initial_seed_usd": clean_seed,
            "seed_explicitly_set_100": False,
            "free_cash_usd": clean_seed,
            "portion_mode": "COMP_08PCT",
            "threshold_mode": "WIDE_RUNWAY",
            "reentry_mode": "HYBRID_PULL_OR_BRK",
            "buy_interval_min": LOCKED_BUY_INTERVAL_MIN,
            "sell_interval_min": LOCKED_SELL_INTERVAL_MIN,
            # MODIFIED [Case 72]: 7-Layer (1층 공격수 + 2 ~ 6층 개별 완충층 + 통합 7층 본대) 기본값 락온
            "max_grid_tiers": MAX_GRID_TIERS_7,
            "buckets": [],
            "anchor_price": 0.0,
            "cycle_min_buy_price": 0.0,
            "ratchet_anchor_price": 0.0,
            "cycle_reanchor_credit_usd": 0.0,
            "buy_order_id": "",
            "sell_order_id": "",
            "client_order_id": "",
            "ordered_qty": 0,
            "buy_price": 0.0,
            "target_sell_price": 0.0,
            "pending_portion_budget": 0.0,
            "pending_lot_type": "",
            "pending_sell_mode": "",
            "pending_sell_indices": [],
            # NEW: 봇 자동 정산 완료된 orderId 링버퍼(최근 50개) — 수동 체결 역추적 시 중복 정산 방지
            "last_settled_order_ids": [],
            "last_modify_mono": 0.0,
            "entry_ts": 0.0,
            "last_buy_fill_ts": 0.0,
            "last_sell_fill_ts": 0.0,
            "last_cycle_end_ts": 0.0,
            "last_sell_gate_eval_ts": 0.0,
            "last_evaluated_buy_bucket_id": -1,
            "last_evaluated_date": today_est,
            "logical_date": today_est,
            "last_closing_report_date": "",
            "last_tg_update_id": 0,
            "total_buy_events": 0,
            "total_buy_shares": 0,
            "daily_buy_events": 0,
            "extra_13plus_buy_events": 0,
            "seed_protect_pass_cnt": 0,
            "daily_seed_protect_pass_cnt": 0,
            "cumulative_leftover_saved_usd": 0.0,
            "daily_leftover_saved_usd": 0.0,
            "total_sell_events": 0,
            "total_sold_shares": 0,
            "win_sell_events": 0,
            "daily_sell_events": 0,
            "indiv_grid_sell_cnt": 0,
            "daily_indiv_grid_sell_cnt": 0,
            "wma_grad_sell_cnt": 0,
            "daily_wma_grad_sell_cnt": 0,
            "reanchor_cnt": 0,
            "daily_reanchor_cnt": 0,
            "cumulative_reanchor_drop_usd": 0.0,
            "cumulative_realized_gross_usd": 0.0,
            "cumulative_realized_fee_usd": 0.0,
            "cumulative_realized_net_usd": 0.0,
            "daily_realized_net_usd": 0.0,
            "updated_at": datetime.now(EST_TZ).isoformat(),
        }

    @staticmethod
    def _is_upper_grid_tag(lot_type_str: str) -> bool:
        tag = str(lot_type_str or "").upper().strip()
        if tag == "MERGED_UPPER":
            return True
        if tag.startswith("GRID_L") and tag != "GRID_L1":
            return True
        return False

    @classmethod
    def _enforce_7tier_cap_on_dict_buckets(
        cls, clean_buckets: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        # NEW [Case 72]:
        구 12-Layer 등에서 이관된 원장에 7개 층 초과 버킷이 존재할 경우,
        활성 1층과 2 ~ 6층(5개 개별 완충층)은 그대로 보존하고 7층 이상 초과분만
        '통합 7층 본대(GRID_L7)'로 무손실 가중평균 병합하여 7-Layer 규격을 100% 보장한다.
        """
        if not clean_buckets:
            return []

        has_active_l1 = not cls._is_upper_grid_tag(
            str(clean_buckets[0].get("lot_type") or "")
        )
        if has_active_l1:
            l1_list = [clean_buckets[0]]
            uppers = list(clean_buckets[1:])
        else:
            l1_list = []
            uppers = list(clean_buckets[:])

        max_upper_cnt = MAX_GRID_TIERS_7 - 1  # 6개 (GRID_L2 ~ GRID_L7)
        if len(uppers) > max_upper_cnt:
            kept = uppers[: max_upper_cnt - 1]
            overflow = uppers[max_upper_cnt - 1 :]
            tot_q = sum(safe_int(b.get("qty"), 0) for b in overflow)
            tot_ac = round(
                sum(safe_float(b.get("actual_cost_usd"), 0.0) for b in overflow),
                4,
            )
            tot_lc = sum(max(1, safe_int(b.get("lot_count"), 1)) for b in overflow)
            pure_p = round(max(0.01, tot_ac / float(max(1, tot_q))), 4)
            merged_top = {
                "qty": max(1, tot_q),
                "book_price": pure_p,
                "book_invested_usd": tot_ac,
                "actual_cost_usd": tot_ac,
                "lot_count": max(1, tot_lc),
                "lot_type": f"GRID_L{MAX_GRID_TIERS_7}",
            }
            uppers = kept + [merged_top]

        relabeled_uppers: List[Dict[str, Any]] = []
        for idx, b in enumerate(uppers):
            tier_no = min(MAX_GRID_TIERS_7, idx + 2)
            b_copy = dict(b)
            b_copy["lot_type"] = f"GRID_L{tier_no}"
            relabeled_uppers.append(b_copy)

        return l1_list + relabeled_uppers

    def _sanitize_and_enforce_identity(
        self, raw_st: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        🚨 Case 63, Case 64, Case 68, Case 69, Case 70, Case 71 & Case 72:
        1) buy_interval_min = 10, sell_interval_min = 1, max_grid_tiers = 7 (MAX_GRID_TIERS_7),
           portion_mode = "COMP_08PCT", threshold_mode = "WIDE_RUNWAY" 를 강제 동기화한다.
        2) 시드머니 결측 또는 0.0 유입 시 $100.0 으로 변조되지 않도록 _resolve_safe_initial_seed() 를 적용한다.
        3) 🛡️ [Case 69, Case 71 & Case 72 순수 실물 원가 100% 보존 강제 (리앵커링 원가 차감 영구 폐기)]:
           - 모든 보유 지층(1층 ~ 7층)의 장부 단가(book_price)와 장부 투자금(book_invested_usd)을
             순수 실물 매수 원가(actual_avg_price = ac / q, actual_cost_usd = ac)와 100% 일치시켜,
             리앵커링 차감이나 이동평균 희석으로 인한 조기 손절 착시를 물리적으로 원천 봉쇄한다!
           - cycle_reanchor_credit_usd = 0.0 으로 항상 고정한다.
        4) 회계 항등식(free_cash_usd = max(0.0, initial_seed_usd + cumulative_realized_net_usd - actual_invested))을
           항상 정밀 유지한다.
        """
        st = self._default_symbol_state(
            folder_slug=self.folder_slug,
            initial_seed=self._last_known_valid_seed,
        )
        if isinstance(raw_st, dict):
            st.update(raw_st)

        st["symbol"] = TARGET_SYMBOL
        st["folder_slug"] = self.folder_slug
        st["buy_interval_min"] = LOCKED_BUY_INTERVAL_MIN
        st["sell_interval_min"] = LOCKED_SELL_INTERVAL_MIN
        st["max_grid_tiers"] = MAX_GRID_TIERS_7
        st["portion_mode"] = "COMP_08PCT"
        st["threshold_mode"] = "WIDE_RUNWAY"
        st["reentry_mode"] = "HYBRID_PULL_OR_BRK"

        init_seed = self._resolve_safe_initial_seed(
            st, fallback_seed=self._last_known_valid_seed
        )
        st["initial_seed_usd"] = round(init_seed, 4)
        if init_seed > 100.0:
            self._last_known_valid_seed = round(init_seed, 4)

        cum_net = round(
            safe_float(st.get("cumulative_realized_net_usd"), 0.0), 4
        )
        st["cumulative_realized_net_usd"] = cum_net

        raw_buckets = st.get("buckets")
        clean_buckets: List[Dict[str, Any]] = []
        if isinstance(raw_buckets, list):
            for b in raw_buckets:
                if isinstance(b, dict) and safe_int(b.get("qty"), 0) > 0:
                    q = max(1, safe_int(b.get("qty"), 1))
                    bp_raw = round(
                        max(0.01, safe_float(b.get("book_price"), 0.01)), 4
                    )
                    bi_raw = round(
                        max(
                            0.01 * q,
                            safe_float(b.get("book_invested_usd"), q * bp_raw),
                        ),
                        4,
                    )
                    ac = round(
                        max(
                            0.01 * q,
                            safe_float(b.get("actual_cost_usd"), bi_raw),
                        ),
                        4,
                    )
                    lc = max(1, safe_int(b.get("lot_count"), 1))
                    lt = str(b.get("lot_type") or "QUOTA_L1").upper().strip()
                    pure_avg_p = round(max(0.01, ac / float(q)), 4)
                    clean_buckets.append(
                        {
                            "qty": q,
                            "book_price": pure_avg_p,
                            "book_invested_usd": round(ac, 4),
                            "actual_cost_usd": round(ac, 4),
                            "lot_count": lc,
                            "lot_type": lt,
                        }
                    )

        clean_buckets = self._enforce_7tier_cap_on_dict_buckets(clean_buckets)
        st["buckets"] = clean_buckets
        st["cycle_reanchor_credit_usd"] = 0.0

        # NEW: 기정산 주문 ID 링버퍼(최근 50개) 정규화
        raw_settled_ids = st.get("last_settled_order_ids")
        if isinstance(raw_settled_ids, list):
            clean_oids: List[str] = []
            for x in raw_settled_ids:
                sx = str(x or "").strip()
                if sx and sx not in clean_oids:
                    clean_oids.append(sx)
            st["last_settled_order_ids"] = clean_oids[-50:]
        else:
            st["last_settled_order_ids"] = []

        if not clean_buckets:
            st["last_sell_gate_eval_ts"] = 0.0
            st["pending_sell_indices"] = []
        else:
            st["last_sell_gate_eval_ts"] = round(
                max(0.0, safe_float(st.get("last_sell_gate_eval_ts"), 0.0)), 4
            )
            raw_p_idx = st.get("pending_sell_indices")
            if isinstance(raw_p_idx, list):
                valid_idx = sorted(
                    {
                        safe_int(x, -1)
                        for x in raw_p_idx
                        if 0 <= safe_int(x, -1) < len(clean_buckets)
                    }
                )
                st["pending_sell_indices"] = valid_idx
            else:
                st["pending_sell_indices"] = []

        actual_inv = round(
            sum(safe_float(b["actual_cost_usd"], 0.0) for b in clean_buckets),
            4,
        )
        realized_equity = max(0.0, init_seed + cum_net)
        st["free_cash_usd"] = round(max(0.0, realized_equity - actual_inv), 4)
        st["updated_at"] = datetime.now(EST_TZ).isoformat()
        return st

    @staticmethod
    def _extract_raw_soxl_dict(loaded: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(loaded, dict):
            return None
        if TARGET_SYMBOL in loaded and isinstance(loaded[TARGET_SYMBOL], dict):
            return dict(loaded[TARGET_SYMBOL])
        if "buckets" in loaded or "initial_seed_usd" in loaded:
            return dict(loaded)
        return None

    @staticmethod
    def _score_ledger_state(
        st: Dict[str, Any],
    ) -> Tuple[int, int, int, float, str]:
        """
        🚨 Case 68 [다중 원장 상태 비교 자가 치유 스코어링]:
        폴더 내 3개 원장 후보(고유 원장 · 실시간 백업 금고 · 호환 원장) 중
        실수로 초기화된 빈 파일 대신 실제 매매 데이터를 보유한 최적 원장을 선택하기 위한 우선순위 튜플.
        """
        buckets = (
            st.get("buckets") if isinstance(st.get("buckets"), list) else []
        )
        total_qty = sum(
            safe_int(b.get("qty"), 0) for b in buckets if isinstance(b, dict)
        )
        has_pending_ord = bool(
            str(st.get("buy_order_id") or "").strip()
            or str(st.get("sell_order_id") or "").strip()
        )
        has_active = 1 if (total_qty > 0 or has_pending_ord) else 0

        trade_cnt = (
            safe_int(st.get("total_buy_events"), 0)
            + safe_int(st.get("total_sell_events"), 0)
            + safe_int(st.get("reanchor_cnt"), 0)
        )
        seed_val = safe_float(st.get("initial_seed_usd"), 0.0)
        has_valid_seed = 1 if seed_val > 100.0 else 0

        realized_metric = round(
            abs(safe_float(st.get("cumulative_realized_net_usd"), 0.0))
            + safe_float(st.get("cumulative_leftover_saved_usd"), 0.0),
            4,
        )
        updated_str = str(st.get("updated_at") or "")
        return (
            has_active,
            trade_cnt,
            has_valid_seed,
            realized_metric,
            updated_str,
        )

    def _load_candidate_file_sync(
        self, fpath: str
    ) -> Optional[Dict[str, Any]]:
        if not fpath or not os.path.isfile(fpath):
            return None
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            return self._extract_raw_soxl_dict(loaded)
        except Exception as exc:
            logger.warning("원장 후보 파일 로드 실패(%s): %s", fpath, exc)
            return None

    def _load_sync(self) -> None:
        """
        🚨 Case 68 [기존 soxl_stratum_ledger.json 100% 무손실 자동 이관 & 3중 금고 자가 치유]:
        1) primary_filepath (`soxl_stratum_ledger_<폴더명>.json`)
        2) backup_filepath  (`ledger_backups/soxl_stratum_ledger_<폴더명>.bak.json`)
        3) legacy_filepath  (`soxl_stratum_ledger.json`)
        3개 파일을 모두 검사하여 가장 풍부하고 유효한 상태를 가진 원장을 채택한 뒤,
        즉시 3개 경로 모두에 원자적으로 동기화 저장한다.
        """
        with GlobalThrottle.get_file_lock(self.primary_filepath):
            with GlobalThrottle.get_file_lock(self.legacy_filepath):
                with self._mem_lock:
                    candidates: List[Tuple[str, Dict[str, Any]]] = []
                    for label, fp in (
                        ("PRIMARY", self.primary_filepath),
                        ("BACKUP_VAULT", self.backup_filepath),
                        ("LEGACY_MIRROR", self.legacy_filepath),
                    ):
                        raw_d = self._load_candidate_file_sync(fp)
                        if isinstance(raw_d, dict):
                            candidates.append((label, raw_d))

                    for _, cand_d in candidates:
                        c_seed = safe_float(cand_d.get("initial_seed_usd"), 0.0)
                        if c_seed > 100.0:
                            self._last_known_valid_seed = round(c_seed, 4)
                            break

                    best_raw: Dict[str, Any] = {}
                    best_label: str = "DEFAULT_INIT"
                    if candidates:
                        priority_map = {
                            "PRIMARY": 3,
                            "BACKUP_VAULT": 2,
                            "LEGACY_MIRROR": 1,
                        }
                        ranked = sorted(
                            candidates,
                            key=lambda item: (
                                self._score_ledger_state(item[1]),
                                priority_map.get(item[0], 0),
                            ),
                            reverse=True,
                        )
                        best_label, best_raw = ranked[0]

                    sanitized = self._sanitize_and_enforce_identity(best_raw)
                    self._data[TARGET_SYMBOL] = sanitized
                    self._save_sync_unlocked()

                    held_q = sum(
                        safe_int(b.get("qty"), 0)
                        for b in sanitized.get("buckets", [])
                    )
                    logger.info(
                        "📂 [Case 68~72 폴더별 3중 7층 골디락스 그리드 원장 락온 완료] 폴더=[%s] · 채택소스=%s · 시드=$%.2f · 보유=%d주(%d개층) · 고유원장=%s",
                        self.folder_slug,
                        best_label,
                        safe_float(sanitized.get("initial_seed_usd"), 10000.0),
                        held_q,
                        len(sanitized.get("buckets", [])),
                        os.path.basename(self.primary_filepath),
                    )

    @staticmethod
    def _atomic_write_json_sync(
        target_path: str, payload: Dict[str, Any]
    ) -> None:
        parent_dir = os.path.dirname(target_path)
        if parent_dir and not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        tmp_path = (
            f"{target_path}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target_path)
        except Exception as exc:
            logger.error("원장 원자적 저장 예외(%s): %s", target_path, exc)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def _save_sync_unlocked(self) -> None:
        """
        🚨 Case 05 & Case 68:
        1) 폴더 고유 원장 (`soxl_stratum_ledger_<folder_slug>.json`)
        2) 호환 미러 원장 (`soxl_stratum_ledger.json`)
        3) 실시간 백업 금고 (`ledger_backups/soxl_stratum_ledger_<folder_slug>.bak.json`)
        3곳 모두 원자적(.tmp -> os.replace)으로 동시 기록한다.
        """
        payload = {TARGET_SYMBOL: self._data[TARGET_SYMBOL]}
        self._atomic_write_json_sync(self.primary_filepath, payload)
        if os.path.abspath(self.legacy_filepath) != os.path.abspath(
            self.primary_filepath
        ):
            self._atomic_write_json_sync(self.legacy_filepath, payload)
        self._atomic_write_json_sync(self.backup_filepath, payload)

    def create_daily_backup_snapshot_sync(
        self, logical_date: Optional[str] = None, keep_days: int = 14
    ) -> str:
        """
        Case 68 [일일 날짜별 스냅샷 금고 보관]:
        17:00 EST 일일 GC 또는 장 마감 결산 시 `ledger_backups/soxl_stratum_ledger_<folder_slug>_YYYY-MM-DD.json`
        파일을 생성하고 오래된 스냅샷(최근 14개 초과분)을 자동 정리한다.
        """
        dt_str = str(
            logical_date or datetime.now(EST_TZ).strftime("%Y-%m-%d")
        ).strip()
        snap_path = os.path.join(
            self.backup_dir,
            f"soxl_stratum_ledger_{self.folder_slug}_{dt_str}.json",
        )
        with GlobalThrottle.get_file_lock(self.primary_filepath):
            with self._mem_lock:
                payload = {TARGET_SYMBOL: self._data[TARGET_SYMBOL]}
                self._atomic_write_json_sync(snap_path, payload)

        try:
            pattern = os.path.join(
                self.backup_dir,
                f"soxl_stratum_ledger_{self.folder_slug}_20*.json",
            )
            files = sorted(glob.glob(pattern))
            if len(files) > max(3, keep_days):
                for old_fp in files[: len(files) - max(3, keep_days)]:
                    try:
                        os.remove(old_fp)
                    except Exception:
                        pass
        except Exception:
            pass
        return snap_path

    async def create_daily_backup_snapshot(
        self, logical_date: Optional[str] = None, keep_days: int = 14
    ) -> str:
        return await asyncio.to_thread(
            self.create_daily_backup_snapshot_sync, logical_date, keep_days
        )

    def recover_from_backup_vault_sync(self) -> Optional[Dict[str, Any]]:
        """
        Case 68:
        백업 금고(`backup_filepath` 또는 최신 일일 스냅샷)에서 유효한 지층(`buckets`)이나
        시드/수익 데이터가 존재하면 읽어와 반환한다.
        """
        cand_paths = [self.backup_filepath, self.primary_filepath]
        try:
            pattern = os.path.join(
                self.backup_dir,
                f"soxl_stratum_ledger_{self.folder_slug}_20*.json",
            )
            cand_paths.extend(sorted(glob.glob(pattern), reverse=True)[:3])
        except Exception:
            pass

        for fp in cand_paths:
            raw_d = self._load_candidate_file_sync(fp)
            if isinstance(raw_d, dict):
                b_list = raw_d.get("buckets")
                if isinstance(b_list, list) and sum(
                    safe_int(b.get("qty"), 0)
                    for b in b_list
                    if isinstance(b, dict)
                ) > 0:
                    return self._sanitize_and_enforce_identity(raw_d)
        return None

    def _assert_allowed_symbol(self, symbol: str) -> str:
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
        if sym not in self.ALLOWED_SYMBOLS:
            raise ValueError(
                f"🚨 [Case 09 위반] 허용되지 않은 종목 접근 차단: {sym} (허용: {self.ALLOWED_SYMBOLS})"
            )
        return sym

    def get_state(self, symbol: str = TARGET_SYMBOL) -> Dict[str, Any]:
        sym = self._assert_allowed_symbol(symbol)
        with self._mem_lock:
            st = self._data.get(sym)
            if not isinstance(st, dict):
                st = self._sanitize_and_enforce_identity({})
                self._data[sym] = st
            return json.loads(json.dumps(st))

    def update_state_sync(
        self, symbol: str = TARGET_SYMBOL, **kwargs: Any
    ) -> Dict[str, Any]:
        sym = self._assert_allowed_symbol(symbol)
        with GlobalThrottle.get_file_lock(self.primary_filepath):
            with GlobalThrottle.get_file_lock(self.legacy_filepath):
                with self._mem_lock:
                    cur = dict(
                        self._data.get(sym)
                        or self._default_symbol_state(
                            folder_slug=self.folder_slug,
                            initial_seed=self._last_known_valid_seed,
                        )
                    )
                    if "initial_seed_usd" in kwargs:
                        req_seed = safe_float(
                            kwargs.get("initial_seed_usd"), 0.0
                        )
                        cur["seed_explicitly_set_100"] = bool(
                            abs(req_seed - 100.0) < 1e-6
                        )
                    cur.update(kwargs)
                    sanitized = self._sanitize_and_enforce_identity(cur)
                    self._data[sym] = sanitized
                    self._save_sync_unlocked()
                    return json.loads(json.dumps(sanitized))

    async def update_state(
        self, symbol: str = TARGET_SYMBOL, **kwargs: Any
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self.update_state_sync, symbol, **kwargs
        )

    async def reset_stratum_only(
        self,
        symbol: str = TARGET_SYMBOL,
        new_seed_usd: Optional[float] = None,
        preserve_realized_profit: bool = True,
    ) -> Dict[str, Any]:
        """
        🚨 Case 02, Case 65, Case 69, Case 70, Case 71 & Case 72:
        /reset 또는 /reset_seed 명령 시 지층 큐(buckets), 기준가, 기억된 최저가, 리앵커링 크레딧,
        다중 그리드 매도 대기 인덱스(pending_sell_indices), 매도 모니터 검문 시각(last_sell_gate_eval_ts),
        미체결 주문 ID를 원자적으로 0점 초기화하고, 0주 싸이클 종료 타임쉴드(last_sell_fill_ts, last_cycle_end_ts)를 각인한다.
        """
        sym = self._assert_allowed_symbol(symbol)
        st = self.get_state(sym)
        if new_seed_usd is not None and safe_float(new_seed_usd, 0.0) >= 100.0:
            init_seed = safe_float(new_seed_usd, DEFAULT_INITIAL_SEED_USD)
        else:
            init_seed = self._resolve_safe_initial_seed(
                st, fallback_seed=self._last_known_valid_seed
            )
        cum_net = (
            safe_float(st.get("cumulative_realized_net_usd"), 0.0)
            if preserve_realized_profit
            else 0.0
        )
        now_reset_ts = time.time()
        return await self.update_state(
            sym,
            initial_seed_usd=round(init_seed, 4),
            cumulative_realized_net_usd=round(cum_net, 4),
            buckets=[],
            anchor_price=0.0,
            cycle_min_buy_price=0.0,
            ratchet_anchor_price=0.0,
            cycle_reanchor_credit_usd=0.0,
            last_sell_fill_ts=now_reset_ts,
            last_cycle_end_ts=now_reset_ts,
            last_sell_gate_eval_ts=0.0,
            buy_order_id="",
            sell_order_id="",
            client_order_id="",
            ordered_qty=0,
            buy_price=0.0,
            target_sell_price=0.0,
            pending_portion_budget=0.0,
            pending_lot_type="",
            pending_sell_mode="",
            pending_sell_indices=[],
            last_evaluated_buy_bucket_id=-1,
        )

    async def set_initial_seed_usd(
        self, new_seed_usd: float, reset_realized_profit: bool = False
    ) -> Dict[str, Any]:
        clean_seed = round(max(100.0, safe_float(new_seed_usd, 10000.0)), 4)
        kw: Dict[str, Any] = {
            "initial_seed_usd": clean_seed,
            "seed_explicitly_set_100": bool(abs(clean_seed - 100.0) < 1e-6),
        }
        if reset_realized_profit:
            kw["cumulative_realized_net_usd"] = 0.0
            kw["cumulative_realized_gross_usd"] = 0.0
            kw["cumulative_realized_fee_usd"] = 0.0
        return await self.update_state(TARGET_SYMBOL, **kw)

    async def toggle_bot_active(
        self, active: Optional[bool] = None
    ) -> Dict[str, Any]:
        st = self.get_state(TARGET_SYMBOL)
        next_active = (
            bool(active)
            if active is not None
            else (not bool(st.get("is_active", True)))
        )
        return await self.update_state(TARGET_SYMBOL, is_active=next_active)

    async def rollover_daily_counters_if_needed(
        self, logical_date: str
    ) -> Dict[str, Any]:
        st = self.get_state(TARGET_SYMBOL)
        cur_date = str(st.get("logical_date") or "")
        if cur_date == str(logical_date):
            return st
        return await self.update_state(
            TARGET_SYMBOL,
            logical_date=str(logical_date),
            daily_buy_events=0,
            daily_sell_events=0,
            daily_indiv_grid_sell_cnt=0,
            daily_wma_grad_sell_cnt=0,
            daily_seed_protect_pass_cnt=0,
            daily_reanchor_cnt=0,
            daily_leftover_saved_usd=0.0,
            daily_realized_net_usd=0.0,
            last_evaluated_buy_bucket_id=-1,
            last_evaluated_date=str(logical_date),
        )


# 하위 호환 별칭 (타 모듈 임포트 호환 100% 보장)
StratumLedger = SoxlStratumLedger
AssassinLedger = SoxlStratumLedger


# ---------------------------------------------------------------------------
# [핵심 3] 토스증권 Open API v1.2.14 방탄 클라이언트 (TossApiClient)
# ---------------------------------------------------------------------------
class TossApiClient:
    """
    🏛️ 제1헌법 ~ 제6헌법 & API.txt (v1.2.14) 100% 완벽 구현:
    - OAuth2 토큰 자동 발급 및 만료 전 선제 갱신 (`POST /oauth2/token`)
    - 종합매매 계좌(`BROKERAGE`) `accountSeq` 자동 락온 및 `X-Tossinvest-Account` 헤더 원자적 주입 (`GET /api/v1/accounts`)
    - `REAL_ALLOWED_SYMBOLS = ("SOXL",)` 외 제3종목(SOXS 등) 주문 물리적 차단 (`Case 09`)
    - 외부 패키지(`httpx`/`aiohttp`) 유무에 영향받지 않는 표준 `urllib.request` + `asyncio.to_thread` 방탄 비동기 엔진
    """

    REAL_ALLOWED_SYMBOLS: Tuple[str, ...] = ("SOXL",)

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        base_dir: Optional[str] = None,
    ) -> None:
        self.base_dir: str = _load_env_files_into_environ(base_dir=base_dir)
        self.base_url: str = (
            base_url
            or os.environ.get("TOSS_BASE_URL")
            or os.environ.get("TOSS_API_BASE_URL")
            or TOSS_OPENAPI_BASE_URL
        ).rstrip("/")

        self.client_id: str = (
            client_id
            or os.environ.get("TOSS_CLIENT_ID")
            or os.environ.get("TOSS_API_KEY")
            or os.environ.get("TOSS_APP_KEY")
            or os.environ.get("CLIENT_ID")
            or os.environ.get("APP_KEY")
            or ""
        ).strip()

        self.client_secret: str = (
            client_secret
            or os.environ.get("TOSS_CLIENT_SECRET")
            or os.environ.get("TOSS_API_SECRET")
            or os.environ.get("TOSS_APP_SECRET")
            or os.environ.get("CLIENT_SECRET")
            or os.environ.get("APP_SECRET")
            or ""
        ).strip()

        self._access_token: str = ""
        self._token_expires_mono: float = 0.0
        self._token_lock: asyncio.Lock = asyncio.Lock()

        self.account_seq: Optional[int] = None
        self._account_lock: asyncio.Lock = asyncio.Lock()

        self._fx_cache_rate: float = KRW_PER_USD_DEFAULT
        self._fx_cache_mono: float = 0.0

    def _assert_real_order_symbol(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        if sym not in self.REAL_ALLOWED_SYMBOLS:
            raise ValueError(
                f"🚨 [Case 09 물리적 주문 차단] 허용되지 않은 종목 주문 시도: {sym} (오직 SOXL 만 허용)"
            )
        return sym

    # -----------------------------------------------------------------------
    # 내부 저수준 HTTP 실행기 (제1헌법: GlobalThrottle.wait_api_sync + asyncio.to_thread)
    # -----------------------------------------------------------------------
    def _execute_http_sync(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body_bytes: Optional[bytes],
        rate_group: str,
        timeout_sec: float,
    ) -> Tuple[int, Dict[str, Any]]:
        GlobalThrottle.wait_api_sync(rate_group)
        req = urllib.request.Request(
            url=url,
            data=body_bytes,
            headers=headers,
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                status_code = int(resp.getcode() or 200)
                raw_bytes = resp.read()
                if not raw_bytes:
                    return status_code, {}
                text = raw_bytes.decode("utf-8", errors="replace").strip()
                if not text:
                    return status_code, {}
                return status_code, json.loads(text)
        except urllib.error.HTTPError as http_err:
            status_code = int(http_err.code or 500)
            err_payload: Dict[str, Any] = {}
            try:
                err_bytes = http_err.read()
                if err_bytes:
                    err_payload = json.loads(
                        err_bytes.decode("utf-8", errors="replace")
                    )
            except Exception:
                err_payload = {}
            return status_code, err_payload

    async def _ensure_access_token(self, force_refresh: bool = False) -> str:
        if (
            not force_refresh
            and self._access_token
            and time.monotonic() < (self._token_expires_mono - 120.0)
        ):
            return self._access_token

        async with self._token_lock:
            if (
                not force_refresh
                and self._access_token
                and time.monotonic() < (self._token_expires_mono - 120.0)
            ):
                return self._access_token

            if not self.client_id or not self.client_secret:
                _load_env_files_into_environ(base_dir=self.base_dir)
                self.client_id = (
                    os.environ.get("TOSS_CLIENT_ID")
                    or os.environ.get("TOSS_API_KEY")
                    or os.environ.get("CLIENT_ID")
                    or self.client_id
                ).strip()
                self.client_secret = (
                    os.environ.get("TOSS_CLIENT_SECRET")
                    or os.environ.get("TOSS_API_SECRET")
                    or os.environ.get("CLIENT_SECRET")
                    or self.client_secret
                ).strip()

            url = f"{self.base_url}/oauth2/token"
            form_data = urllib.parse.urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
            ).encode("utf-8")
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }

            status_code, payload = await asyncio.wait_for(
                asyncio.to_thread(
                    self._execute_http_sync,
                    "POST",
                    url,
                    headers,
                    form_data,
                    "AUTH",
                    10.0,
                ),
                timeout=12.0,
            )
            if status_code != 200 or not isinstance(payload, dict):
                err_code = str(payload.get("error") or f"http-{status_code}")
                err_desc = str(
                    payload.get("error_description") or "OAuth2 토큰 발급 실패"
                )
                raise _NonRetryableApiError(
                    status_code, err_code, err_desc, payload
                )

            token = str(payload.get("access_token") or "").strip()
            expires_in = max(300, safe_int(payload.get("expires_in"), 86400))
            if not token:
                raise _NonRetryableApiError(
                    500, "empty-token", "access_token 이 비어 있습니다.", payload
                )

            self._access_token = token
            self._token_expires_mono = time.monotonic() + float(expires_in)
            return self._access_token

    async def ensure_account_seq(self) -> int:
        """
        🚨 제2헌법:
        GET /api/v1/accounts 를 호출하여 BROKERAGE(종합매매) 계좌의 accountSeq 를 메모리에 락온한다.
        """
        if self.account_seq is not None and self.account_seq > 0:
            return self.account_seq

        async with self._account_lock:
            if self.account_seq is not None and self.account_seq > 0:
                return self.account_seq

            env_seq = safe_int(os.environ.get("TOSS_ACCOUNT_SEQ"), 0)
            res = await self._request(
                method="GET",
                path="/api/v1/accounts",
                require_account=False,
                rate_group="ACCOUNT",
            )
            items = res.get("result") if isinstance(res, dict) else []
            if isinstance(items, list) and items:
                chosen_seq = 0
                for acc in items:
                    if isinstance(acc, dict):
                        acc_type = (
                            str(acc.get("accountType") or "").upper().strip()
                        )
                        seq_val = safe_int(acc.get("accountSeq"), 0)
                        if acc_type == "BROKERAGE" and seq_val > 0:
                            chosen_seq = seq_val
                            break
                if chosen_seq <= 0 and isinstance(items[0], dict):
                    chosen_seq = safe_int(items[0].get("accountSeq"), 0)
                if chosen_seq > 0:
                    self.account_seq = chosen_seq
                    return self.account_seq

            if env_seq > 0:
                self.account_seq = env_seq
                return self.account_seq

            self.account_seq = 1
            return self.account_seq

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        require_account: bool = False,
        rate_group: str = "DEFAULT",
        timeout_sec: float = 10.0,
        max_retries: int = 2,
    ) -> Dict[str, Any]:
        """
        🚨 제1헌법, 제2헌법 & Case 20, 21:
        모든 토스 Open API 요청을 처리하는 중앙 비동기 디스패처.
        """
        clean_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{clean_path}"
        if params:
            filtered_params = {
                k: (
                    "true"
                    if v is True
                    else ("false" if v is False else str(v))
                )
                for k, v in params.items()
                if v is not None
            }
            if filtered_params:
                url = f"{url}?{urllib.parse.urlencode(filtered_params)}"

        body_bytes: Optional[bytes] = None
        if json_data is not None:
            body_bytes = json.dumps(json_data, ensure_ascii=False).encode(
                "utf-8"
            )

        last_exc: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                token = await self._ensure_access_token(
                    force_refresh=(attempt > 0 and last_exc is None)
                )
                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
                if body_bytes is not None:
                    headers["Content-Type"] = "application/json"

                if require_account:
                    acc_seq = await self.ensure_account_seq()
                    headers["X-Tossinvest-Account"] = str(acc_seq)

                status_code, payload = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._execute_http_sync,
                        method,
                        url,
                        headers,
                        body_bytes,
                        rate_group,
                        timeout_sec,
                    ),
                    timeout=timeout_sec + 3.0,
                )

                if 200 <= status_code < 300:
                    return (
                        payload
                        if isinstance(payload, dict)
                        else {"result": payload}
                    )

                err_obj = (
                    payload.get("error")
                    if isinstance(payload, dict)
                    and isinstance(payload.get("error"), dict)
                    else {}
                )
                err_code = str(
                    err_obj.get("code")
                    or (
                        payload.get("error")
                        if isinstance(payload, dict)
                        else ""
                    )
                    or f"http-{status_code}"
                )
                err_msg = str(
                    err_obj.get("message")
                    or (
                        payload.get("error_description")
                        if isinstance(payload, dict)
                        else ""
                    )
                    or f"HTTP {status_code}"
                )

                if status_code == 401 and attempt < max_retries:
                    self._access_token = ""
                    last_exc = None
                    continue

                if (
                    status_code in (429, 500, 502, 503, 504)
                    and attempt < max_retries
                ):
                    await asyncio.sleep(0.35 * (attempt + 1))
                    continue

                raise _NonRetryableApiError(
                    status_code, err_code, err_msg, payload
                )

            except _NonRetryableApiError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries:
                    await asyncio.sleep(0.30 * (attempt + 1))
                else:
                    raise

        if last_exc:
            raise last_exc
        return {}

    # -----------------------------------------------------------------------
    # [Market Data 그룹] 현재가 · 호가 · 1분봉/일봉 차트 (API.txt v1.2.14)
    # -----------------------------------------------------------------------
    async def get_current_prices_batch(
        self, symbols: Iterable[str] = (TARGET_SYMBOL,)
    ) -> Dict[str, float]:
        """
        GET /api/v1/prices?symbols=SOXL
        """
        sym_list = [
            str(s).upper().strip() for s in symbols if str(s).strip()
        ]
        if not sym_list:
            sym_list = [TARGET_SYMBOL]
        out: Dict[str, float] = {s: 0.0 for s in sym_list}
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/prices",
                params={"symbols": ",".join(sym_list)},
                require_account=False,
                rate_group="MARKET_DATA",
            )
            items = res.get("result") if isinstance(res, dict) else []
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        s = str(it.get("symbol") or "").upper().strip()
                        lp = safe_float(it.get("lastPrice"), 0.0)
                        if s and lp > 0.0:
                            out[s] = lp
        except Exception as exc:
            logger.debug("get_current_prices_batch 예외: %s", exc)
        return out

    async def get_current_price(self, symbol: str = TARGET_SYMBOL) -> float:
        p_map = await self.get_current_prices_batch((symbol,))
        return safe_float(p_map.get(str(symbol).upper().strip()), 0.0)

    async def get_orderbook(
        self, symbol: str = TARGET_SYMBOL
    ) -> Dict[str, Any]:
        """
        GET /api/v1/orderbook?symbol=SOXL
        """
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/orderbook",
                params={
                    "symbol": str(symbol or TARGET_SYMBOL).upper().strip()
                },
                require_account=False,
                rate_group="MARKET_DATA",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            return r_obj if isinstance(r_obj, dict) else {}
        except Exception as exc:
            logger.debug("get_orderbook 예외: %s", exc)
            return {}

    async def get_best_bid_price(
        self, symbol: str = TARGET_SYMBOL, fallback_price: float = 0.0
    ) -> float:
        ob = await self.get_orderbook(symbol=symbol)
        bids = ob.get("bids") if isinstance(ob, dict) else None
        if isinstance(bids, list) and bids:
            prices = [
                safe_float(b.get("price"), 0.0)
                for b in bids
                if isinstance(b, dict)
                and safe_float(b.get("price"), 0.0) > 0.0
            ]
            if prices:
                return max(prices)
        return safe_float(fallback_price, 0.0)

    async def get_best_ask_price(
        self, symbol: str = TARGET_SYMBOL, fallback_price: float = 0.0
    ) -> float:
        ob = await self.get_orderbook(symbol=symbol)
        asks = ob.get("asks") if isinstance(ob, dict) else None
        if isinstance(asks, list) and asks:
            prices = [
                safe_float(a.get("price"), 0.0)
                for a in asks
                if isinstance(a, dict)
                and safe_float(a.get("price"), 0.0) > 0.0
            ]
            if prices:
                return min(prices)
        return safe_float(fallback_price, 0.0)

    async def get_candles(
        self,
        symbol: str = TARGET_SYMBOL,
        interval: str = "1m",
        count: int = 200,
        before: Optional[str] = None,
        adjusted: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        GET /api/v1/candles (interval ∈ {"1m", "1d"}, count ≤ 200)
        """
        q_cnt = max(1, min(200, safe_int(count, 100)))
        params: Dict[str, Any] = {
            "symbol": str(symbol or TARGET_SYMBOL).upper().strip(),
            "interval": str(interval or "1m"),
            "count": q_cnt,
            "adjusted": bool(adjusted),
        }
        if before:
            params["before"] = str(before)

        res = await self._request(
            method="GET",
            path="/api/v1/candles",
            params=params,
            require_account=False,
            rate_group="MARKET_DATA_CHART",
        )
        r_obj = res.get("result") if isinstance(res, dict) else {}
        if not isinstance(r_obj, dict):
            return [], None
        candles = r_obj.get("candles")
        next_before = r_obj.get("nextBefore")
        c_list = (
            [c for c in candles if isinstance(c, dict)]
            if isinstance(candles, list)
            else []
        )
        return c_list, (str(next_before) if next_before else None)

    async def get_candles_1m(
        self,
        symbol: str = TARGET_SYMBOL,
        total_count: int = 300,
        adjusted: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        GET /api/v1/candles (interval="1m") 를 nextBefore 페이지네이션으로 호출하여
        최대 total_count(기본 300)개의 1분봉 리스트를 반환한다.
        """
        target_cnt = max(1, min(600, safe_int(total_count, 300)))
        collected: List[Dict[str, Any]] = []
        seen_ts = set()
        before_cursor: Optional[str] = None
        max_pages = max(1, int(math.ceil(target_cnt / 200.0)) + 1)

        try:
            for _ in range(max_pages):
                rem = max(1, min(200, target_cnt - len(collected) + 1))
                batch, next_b = await self.get_candles(
                    symbol=symbol,
                    interval="1m",
                    count=rem,
                    before=before_cursor,
                    adjusted=adjusted,
                )
                if not batch:
                    break
                for c in batch:
                    ts_str = str(c.get("timestamp") or "")
                    if ts_str and ts_str not in seen_ts:
                        seen_ts.add(ts_str)
                        collected.append(c)
                if not next_b or len(collected) >= target_cnt:
                    break
                before_cursor = next_b
        except Exception as exc:
            logger.debug("get_candles_1m 조회 예외: %s", exc)

        collected.sort(key=lambda x: str(x.get("timestamp") or ""))
        if len(collected) > target_cnt:
            collected = collected[-target_cnt:]
        return collected

    async def get_candles_1d(
        self,
        symbol: str = TARGET_SYMBOL,
        count: int = 10,
        adjusted: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        GET /api/v1/candles (interval="1d") 일봉 데이터를 조회한다.
        """
        try:
            batch, _ = await self.get_candles(
                symbol=symbol,
                interval="1d",
                count=max(1, min(200, safe_int(count, 10))),
                adjusted=adjusted,
            )
            batch.sort(key=lambda x: str(x.get("timestamp") or ""))
            return batch
        except Exception as exc:
            logger.debug("get_candles_1d 조회 예외: %s", exc)
            return []

    # -----------------------------------------------------------------------
    # [Market Info 그룹] USD/KRW 환율 및 미국 장 캘린더 (API.txt v1.2.14)
    # -----------------------------------------------------------------------
    async def get_usd_krw_rate(self) -> float:
        """
        GET /api/v1/exchange-rate?baseCurrency=USD&quoteCurrency=KRW (60초 캐시)
        """
        now_m = time.monotonic()
        if (
            self._fx_cache_rate > 0.0
            and (now_m - self._fx_cache_mono) < 60.0
        ):
            return self._fx_cache_rate

        try:
            res = await self._request(
                method="GET",
                path="/api/v1/exchange-rate",
                params={"baseCurrency": "USD", "quoteCurrency": "KRW"},
                require_account=False,
                rate_group="MARKET_INFO",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            if isinstance(r_obj, dict):
                rate_val = safe_float(
                    r_obj.get("rate") or r_obj.get("midRate"), 0.0
                )
                if rate_val > 100.0:
                    self._fx_cache_rate = rate_val
                    self._fx_cache_mono = now_m
                    return rate_val
        except Exception as exc:
            logger.debug("환율 조회 폴백 사용: %s", exc)

        return (
            self._fx_cache_rate
            if self._fx_cache_rate > 0.0
            else KRW_PER_USD_DEFAULT
        )

    async def get_us_market_calendar(
        self, date_str: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        GET /api/v1/market-calendar/US
        """
        params: Dict[str, Any] = {}
        if date_str:
            params["date"] = str(date_str)
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/market-calendar/US",
                params=params,
                require_account=False,
                rate_group="MARKET_INFO",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            return r_obj if isinstance(r_obj, dict) else {}
        except Exception as exc:
            logger.debug("미국 장 캘린더 조회 폴백: %s", exc)
            return {}

    # -----------------------------------------------------------------------
    # [Asset & Order Info 그룹] 보유 잔고 · 매수가능금액 · 매도가능수량 (Case 03)
    # -----------------------------------------------------------------------
    @staticmethod
    def _empty_holding_dict(symbol: str = TARGET_SYMBOL) -> Dict[str, Any]:
        sym = str(symbol or TARGET_SYMBOL).upper().strip()
        return {
            "symbol": sym,
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

    async def get_holdings_map_with_status(
        self, symbol: Optional[str] = None
    ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
        """
        🚨 Case 03:
        GET /api/v1/holdings 를 호출하여 (holdings_map, fetch_ok) 튜플을 반환한다.
        """
        out_map: Dict[str, Dict[str, Any]] = {
            TARGET_SYMBOL: self._empty_holding_dict(TARGET_SYMBOL)
        }
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = str(symbol).upper().strip()

        try:
            res = await self._request(
                method="GET",
                path="/api/v1/holdings",
                params=params if params else None,
                require_account=True,
                rate_group="ASSET",
            )
            r_obj = res.get("result") if isinstance(res, dict) else None
            if not isinstance(r_obj, dict):
                return out_map, False

            items = r_obj.get("items")
            if isinstance(items, list):
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    sym = str(it.get("symbol") or "").upper().strip()
                    if not sym:
                        continue
                    qty = max(0, safe_int(it.get("quantity"), 0))
                    avg_p = round(
                        max(
                            0.0,
                            safe_float(it.get("averagePurchasePrice"), 0.0),
                        ),
                        4,
                    )
                    last_p = round(
                        max(0.0, safe_float(it.get("lastPrice"), avg_p)), 4
                    )

                    mv_obj = (
                        it.get("marketValue")
                        if isinstance(it.get("marketValue"), dict)
                        else {}
                    )
                    pl_obj = (
                        it.get("profitLoss")
                        if isinstance(it.get("profitLoss"), dict)
                        else {}
                    )

                    pur_amt = round(
                        max(
                            0.0,
                            safe_float(
                                mv_obj.get("purchaseAmount"),
                                float(qty) * avg_p,
                            ),
                        ),
                        4,
                    )
                    eval_amt = round(
                        max(
                            0.0,
                            safe_float(
                                mv_obj.get("amount"), float(qty) * last_p
                            ),
                        ),
                        4,
                    )
                    pnl_amt = round(
                        safe_float(
                            pl_obj.get("amountAfterCost")
                            if pl_obj.get("amountAfterCost") is not None
                            else pl_obj.get("amount"),
                            eval_amt - pur_amt,
                        ),
                        4,
                    )
                    raw_rate = safe_float(
                        pl_obj.get("rateAfterCost")
                        if pl_obj.get("rateAfterCost") is not None
                        else pl_obj.get("rate"),
                        0.0,
                    )
                    profit_rate_pct = round(raw_rate * 100.0, 2)

                    out_map[sym] = {
                        "symbol": sym,
                        "qty": qty,
                        "quantity": qty,
                        "avg_price": avg_p,
                        "averagePurchasePrice": avg_p,
                        "last_price": last_p,
                        "lastPrice": last_p,
                        "purchase_amount": pur_amt,
                        "eval_amount": eval_amt,
                        "profit_loss": pnl_amt,
                        "profit_rate": profit_rate_pct,
                    }
            return out_map, True
        except Exception as exc:
            logger.warning("보유 잔고 조회(/api/v1/holdings) 예외: %s", exc)
            return out_map, False

    async def get_holdings_map(
        self, symbol: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        h_map, _ = await self.get_holdings_map_with_status(symbol=symbol)
        return h_map

    async def get_holdings(
        self, symbol: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        h_map, _ = await self.get_holdings_map_with_status(symbol=symbol)
        return h_map

    async def get_buying_power_usd(
        self, safety_buffer_pct: float = 0.2
    ) -> float:
        """
        GET /api/v1/buying-power?currency=USD
        """
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/buying-power",
                params={"currency": "USD"},
                require_account=True,
                rate_group="ORDER_INFO",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            if isinstance(r_obj, dict):
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
        except Exception as exc:
            logger.debug("매수가능금액 조회 예외: %s", exc)
        return 0.0

    async def get_sellable_quantity(
        self, symbol: str = TARGET_SYMBOL
    ) -> int:
        """
        GET /api/v1/sellable-quantity?symbol=SOXL
        """
        sym = self._assert_real_order_symbol(symbol)
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/sellable-quantity",
                params={"symbol": sym},
                require_account=True,
                rate_group="ORDER_INFO",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            if isinstance(r_obj, dict):
                return max(
                    0,
                    int(
                        math.floor(
                            safe_float(r_obj.get("sellableQuantity"), 0.0)
                            + 1e-9
                        )
                    ),
                )
        except Exception as exc:
            logger.debug("매도가능수량 조회 예외: %s", exc)
        return 0

    # -----------------------------------------------------------------------
    # [Order & Order History 그룹] 주문 생성 · 정정 · 취소 · 상세/목록 조회 · 수동체결 역추적
    # -----------------------------------------------------------------------
    @staticmethod
    def _format_us_limit_price(price: float) -> str:
        p = max(0.0001, safe_float(price, 0.01))
        if p >= 1.0:
            p_trunc = math.floor((p + 1e-9) * 100.0) / 100.0
            return f"{p_trunc:.2f}"
        p_trunc = math.floor((p + 1e-9) * 10000.0) / 10000.0
        return f"{p_trunc:.4f}"

    async def create_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        order_type: str = "LIMIT",
        time_in_force: str = "DAY",
        client_order_id: Optional[str] = None,
        enforce_daymarket_block: bool = False,
    ) -> Dict[str, Any]:
        """
        🎯 5 & 제5헌법 & Case 09, 18:
        POST /api/v1/orders (오직 SOXL LIMIT 지정가 주문만 허용)
        """
        sym = self._assert_real_order_symbol(symbol)
        clean_side = str(side or "BUY").upper().strip()
        if clean_side not in ("BUY", "SELL"):
            raise ValueError(f"유효하지 않은 주문 방향: {clean_side}")

        if enforce_daymarket_block and clean_side == "BUY":
            now_est = datetime.now(EST_TZ)
            hm = now_est.hour * 100 + now_est.minute
            if not (407 <= hm <= 1600):
                raise _NonRetryableApiError(
                    422,
                    "outside-active-window",
                    f"04:07 ~ 16:00 EST 외 매수 차단 (현재 {hm:04d} EST)",
                )

        qty_int = max(1, safe_int(quantity, 1))
        cid = str(
            client_order_id or generate_client_order_id(prefix=clean_side)
        ).strip()[:36]
        price_str = self._format_us_limit_price(price)

        payload: Dict[str, Any] = {
            "clientOrderId": cid,
            "symbol": sym,
            "side": clean_side,
            "orderType": "LIMIT",
            "timeInForce": str(time_in_force or "DAY").upper().strip(),
            "quantity": str(qty_int),
            "price": price_str,
        }

        res = await self._request(
            method="POST",
            path="/api/v1/orders",
            json_data=payload,
            require_account=True,
            rate_group="ORDER",
        )
        r_obj = res.get("result") if isinstance(res, dict) else {}
        return r_obj if isinstance(r_obj, dict) else {}

    async def modify_order(
        self,
        order_id: str,
        new_price: float,
        order_type: str = "LIMIT",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        🎯 5 (토스 US 주식 정정 규격 엄수):
        POST /api/v1/orders/{orderId}/modify
        미국 주식 주문 정정 시 `quantity` 를 전달하면 `400 us-modify-quantity-not-supported` 에러가 발생하므로
        오직 `orderType` 과 `price` 만 전송한다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            raise ValueError("modify_order: order_id 가 비어 있습니다.")

        payload: Dict[str, Any] = {
            "orderType": "LIMIT",
            "price": self._format_us_limit_price(new_price),
        }
        res = await self._request(
            method="POST",
            path=f"/api/v1/orders/{urllib.parse.quote(oid, safe='')}/modify",
            json_data=payload,
            require_account=True,
            rate_group="ORDER",
        )
        r_obj = res.get("result") if isinstance(res, dict) else {}
        return r_obj if isinstance(r_obj, dict) else {}

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        🎯 5 (HTTP 415 원천 방어):
        POST /api/v1/orders/{orderId}/cancel 호출 시 반드시 빈 JSON 객체 `{}` 를 전송한다.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return {}
        res = await self._request(
            method="POST",
            path=f"/api/v1/orders/{urllib.parse.quote(oid, safe='')}/cancel",
            json_data={},
            require_account=True,
            rate_group="ORDER",
        )
        r_obj = res.get("result") if isinstance(res, dict) else {}
        return r_obj if isinstance(r_obj, dict) else {}

    async def get_order_detail(
        self, order_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        GET /api/v1/orders/{orderId}
        """
        oid = str(order_id or "").strip()
        if not oid:
            return None
        try:
            res = await self._request(
                method="GET",
                path=f"/api/v1/orders/{urllib.parse.quote(oid, safe='')}",
                require_account=True,
                rate_group="ORDER_HISTORY",
            )
            r_obj = res.get("result") if isinstance(res, dict) else None
            return r_obj if isinstance(r_obj, dict) else None
        except Exception as exc:
            logger.debug("주문 상세 조회(%s) 예외: %s", oid, exc)
            return None

    async def get_open_orders(
        self, symbol: str = TARGET_SYMBOL
    ) -> List[Dict[str, Any]]:
        """
        GET /api/v1/orders?status=OPEN&symbol=SOXL
        """
        params: Dict[str, Any] = {"status": "OPEN"}
        if symbol:
            params["symbol"] = str(symbol).upper().strip()
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/orders",
                params=params,
                require_account=True,
                rate_group="ORDER_HISTORY",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            if isinstance(r_obj, dict) and isinstance(
                r_obj.get("orders"), list
            ):
                return [
                    o for o in r_obj["orders"] if isinstance(o, dict)
                ]
        except Exception as exc:
            logger.debug("미체결 주문 목록 조회 예외: %s", exc)
        return []

    async def get_closed_orders(
        self, symbol: str = TARGET_SYMBOL, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        # NEW (API.txt v1.2.14 준수):
        GET /api/v1/orders?status=CLOSED&symbol=SOXL&limit=20
        종료된 주문(FILLED, CANCELED, REJECTED, REPLACED 등) 목록을 최신순으로 조회한다.
        """
        params: Dict[str, Any] = {
            "status": "CLOSED",
            "limit": max(1, min(100, safe_int(limit, 20))),
        }
        if symbol:
            params["symbol"] = str(symbol).upper().strip()
        try:
            res = await self._request(
                method="GET",
                path="/api/v1/orders",
                params=params,
                require_account=True,
                rate_group="ORDER_HISTORY",
            )
            r_obj = res.get("result") if isinstance(res, dict) else {}
            if isinstance(r_obj, dict) and isinstance(
                r_obj.get("orders"), list
            ):
                return [
                    o for o in r_obj["orders"] if isinstance(o, dict)
                ]
        except Exception as exc:
            logger.debug("종료 주문 목록 조회(/api/v1/orders?status=CLOSED) 예외: %s", exc)
        return []

    async def probe_latest_manual_fill_price(
        self,
        side: str,
        target_qty: int,
        settled_order_ids: Iterable[str] = (),
        fallback_price: float = 0.0,
        symbol: str = TARGET_SYMBOL,
    ) -> Tuple[float, List[str]]:
        """
        # NEW [Case 19 & Case 63 수동 매수·매도 실체결가 역추적기]:
        GET /api/v1/orders?status=CLOSED&symbol=SOXL 응답에서 봇이 이미 정산한 settled_order_ids 를 제외하고,
        요청된 방향(side="BUY" 또는 "SELL")의 최신 체결 완료 건(execution.filledQuantity > 0 & averageFilledPrice > 0)들을
        target_qty 수량만큼 역추적하여 수량 가중평균 체결단가와 매칭된 orderId 리스트를 반환한다.
        """
        target_side = str(side or "SELL").upper().strip()
        need_q = max(1, safe_int(target_qty, 1))
        settled_set: Set[str] = {
            str(x).strip() for x in (settled_order_ids or ()) if str(x).strip()
        }
        fb_p = round(max(0.0, safe_float(fallback_price, 0.0)), 4)

        closed_list = await self.get_closed_orders(symbol=symbol, limit=30)
        if not closed_list:
            return fb_p, []

        acc_qty = 0
        acc_val = 0.0
        matched_ids: List[str] = []

        for ord_item in closed_list:
            if not isinstance(ord_item, dict):
                continue
            oid = str(ord_item.get("orderId") or "").strip()
            if not oid or oid in settled_set:
                continue
            o_side = str(ord_item.get("side") or "").upper().strip()
            if o_side != target_side:
                continue

            # Case 19: 오직 실제 체결 데이터(execution.filledQuantity & averageFilledPrice)만 인정!
            exec_obj = (
                ord_item.get("execution")
                if isinstance(ord_item.get("execution"), dict)
                else {}
            )
            f_qty = safe_int(exec_obj.get("filledQuantity"), 0)
            f_avg = safe_float(exec_obj.get("averageFilledPrice"), 0.0)
            if f_qty <= 0 or f_avg <= 0.0:
                continue

            take_q = min(need_q - acc_qty, f_qty)
            if take_q <= 0:
                break
            acc_qty += take_q
            acc_val += float(take_q) * f_avg
            matched_ids.append(oid)
            if acc_qty >= need_q:
                break

        if acc_qty > 0 and acc_val > 0.0:
            return round(acc_val / float(acc_qty), 4), matched_ids
        return fb_p, []

    async def find_latest_open_order_id(
        self, symbol: str = TARGET_SYMBOL, side: Optional[str] = None
    ) -> str:
        orders = await self.get_open_orders(symbol=symbol)
        target_side = str(side or "").upper().strip()
        for o in orders:
            o_side = str(o.get("side") or "").upper().strip()
            if not target_side or o_side == target_side:
                oid = str(o.get("orderId") or "").strip()
                if oid:
                    return oid
        return ""

    async def cancel_all_open_orders(
        self, symbol: str = TARGET_SYMBOL
    ) -> int:
        orders = await self.get_open_orders(symbol=symbol)
        canceled = 0
        for o in orders:
            oid = str(o.get("orderId") or "").strip()
            if oid:
                try:
                    await self.cancel_order(oid)
                    canceled += 1
                except Exception:
                    pass
        return canceled

    async def close(self) -> None:
        return None
