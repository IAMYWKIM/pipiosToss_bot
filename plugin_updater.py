# -*- coding: utf-8 -*-
# =====================================================================
# FILE: plugin_updater.py
# 목적: 토스 스나이퍼 깃허브 코드 다운로드 검증, 장중 락다운(Case 07),
#       미체결 원장 보호 및 전체 코어 플러그인 프리플라이트 컴파일/롤백 (Case 11)
# =====================================================================
# MODIFIED: 제1헌법 ~ 제4헌법 및 Case 07(평일 03:30 ~ 17:00 EST 장중 업데이트 락다운),
#           Case 08(ZoneInfo('America/New_York') 단일 타임존), Case 11(전체 플러그인 py_compile 검증) 결속
# MODIFIED: 프리장/데이장 동형 아키텍처 코어 모듈(sniper_exit_plugin.py, daymarket_sim_plugin.py,
#           quant_indicators_plugin.py 등) 누락 방지 및 디렉터리 내 전체 .py 파일 구문 전수 검사 추가
# NEW: 로컬 장부(assassin_ledger.json) 내 미체결 주문 존재 시 무인 업데이트 보호 가드 및
#      main.py 연동용 비동기 핫 리로드 인터페이스(execute_safe_hot_reload) 통합

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import py_compile
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("toss_sniper.plugin_updater")

# ---------------------------------------------------------------------------
# 제4헌법 & Case 08 : 미국 동부 시간(EST_TZ) 단일 타임존 락온
# ---------------------------------------------------------------------------
EST_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Case 11 : 토스 스나이퍼 필수 코어 모듈 화이트리스트 (누락 시 즉시 롤백 대상)
# ---------------------------------------------------------------------------
REQUIRED_CORE_MODULES: Tuple[str, ...] = (
    "toss_api.py",
    "quant_indicators_plugin.py",
    "quant_engine.py",
    "sniper_plugin.py",
    "sniper_exit_plugin.py",
    "defense_plugin.py",
    "daymarket_sim_plugin.py",
    "whipsaw_plugin.py",
    "tg_ui_plugin.py",
    "main.py",
)

# 분리형 서브 플러그인 파일이 존재할 경우 함께 검증할 확장 모듈 목록
OPTIONAL_SUB_MODULES: Tuple[str, ...] = (
    "plugin_updater.py",
    "toss_api_core.py",
    "defense_state_plugin.py",
    "tg_router.py",
    "tg_admin_plugin.py",
    "tg_ui_boards_plugin.py",
    "candle_recorder.py",
    "main_loops_plugin.py",
)


# ---------------------------------------------------------------------------
# 서브프로세스 30초 타임아웃 및 강제 Kill 실행기 (네트워크 교착 방어)
# ---------------------------------------------------------------------------
def run_command(
    command: str,
    timeout: int = 30,
    cwd: Optional[str] = None,
) -> Tuple[int, str, str]:
    """
    외부 쉘 명령(git 등)을 실행하고 timeout(기본 30초) 초과 시 프로세스를 즉시 강제 종료(kill)한다.
    """
    work_dir = os.path.abspath(cwd or os.path.dirname(os.path.abspath(__file__)))
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=work_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=max(5, int(timeout)))
        return process.returncode, (stdout or "").strip(), (stderr or "").strip()
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        return (
            -1,
            (stdout or "").strip(),
            "Subprocess Timeout Expired (네트워크 교착 상태 차단)",
        )
    except Exception as exc:
        try:
            process.kill()
        except Exception:
            pass
        return -1, "", f"Subprocess Execution Error: {exc}"


# ---------------------------------------------------------------------------
# Case 07 : 평일 03:30 ~ 17:00 EST 장중 업데이트 락다운 판정기
# ---------------------------------------------------------------------------
def is_update_lockdown_window(now_est: Optional[datetime] = None) -> bool:
    """
    🚨 Case 07 장중 업데이트 락다운:
    평일(월 ~ 금) 03:30 ~ 17:00 EST 구간(03:58 1주 휩소 작전, 04:07 ~ 08:15 프리장 본진,
    08:15 ~ 16:00 정규장, 15:59 ~ 16:01 MOC 강제청산 포함)에는
    무인 자동 업데이트 및 재기동을 전면 차단한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    # 토요일(5), 일요일(6)은 장중 락다운 해제
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 100 + dt.minute
    return bool(330 <= hm <= 1700)


# ---------------------------------------------------------------------------
# 로컬 원장(assassin_ledger.json) 미체결 주문/보유 포지션 생존 보호 가드
# ---------------------------------------------------------------------------
def has_active_orders_in_ledger(
    ledger_path: Optional[str] = None,
    base_dir: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    로컬 장부(assassin_ledger.json)를 읽어 현재 미체결 매수 주문(buy_order_id)이나
    미체결 매도 주문(sell_order_id)이 존재하면 무인 업데이트로 인한 추적 유실을 막기 위해
    True 와 상세 사유를 반환한다.
    """
    root = os.path.abspath(base_dir or os.path.dirname(os.path.abspath(__file__)))
    fpath = os.path.abspath(
        ledger_path or os.getenv("SNIPER_LEDGER_PATH", os.path.join(root, "assassin_ledger.json"))
    )
    if not os.path.isfile(fpath):
        return False, "LEDGER_NOT_FOUND"

    try:
        with open(fpath, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if not isinstance(data, dict):
            return False, "INVALID_LEDGER_FORMAT"

        for sym in ("SOXS", "SOXL"):
            st = data.get(sym)
            if not isinstance(st, dict):
                continue
            buy_oid = str(st.get("buy_order_id") or "").strip()
            sell_oid = str(st.get("sell_order_id") or "").strip()
            if buy_oid:
                return True, f"{sym} 미체결 매수 주문 대기 중 (orderId={buy_oid})"
            if sell_oid:
                return True, f"{sym} 매도 안전캡/익절 주문 대기 중 (orderId={sell_oid})"
    except Exception as exc:
        logger.debug("장부 미체결 점검 스킵: %s", exc)

    return False, "CLEAR"


# ---------------------------------------------------------------------------
# Case 11 & Case 88 : 전체 코어 플러그인 및 프로젝트 .py 파일 프리플라이트 컴파일 검증
# ---------------------------------------------------------------------------
def verify_all_modules_compile(
    base_dir: Optional[str] = None,
    strict_required: bool = True,
) -> Tuple[bool, List[str]]:
    """
    🚨 Case 11 & Case 88:
    1) REQUIRED_CORE_MODULES (10개 핵심 파일)의 존재 여부 및 py_compile 구문 무결성을 검증한다.
    2) OPTIONAL_SUB_MODULES 및 프로젝트 루트 내 모든 .py 파일에 대해서도
       py_compile.compile(..., doraise=True) 검증을 수행하여 단 1개의 문법 오류도 허용하지 않는다.
    """
    root = os.path.abspath(base_dir or os.path.dirname(os.path.abspath(__file__)))
    errors: List[str] = []
    checked_files = set()

    # 1단계: 필수 코어 모듈 검증
    for mod_file in REQUIRED_CORE_MODULES:
        fpath = os.path.join(root, mod_file)
        checked_files.add(os.path.abspath(fpath))
        if not os.path.isfile(fpath):
            if strict_required:
                errors.append(f"MISSING_REQUIRED_MODULE: {mod_file}")
            continue
        try:
            py_compile.compile(fpath, doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"PY_COMPILE_ERROR [{mod_file}]: {exc}")
        except Exception as exc:
            errors.append(f"UNEXPECTED_COMPILE_ERROR [{mod_file}]: {exc}")

    # 2단계: 확장 서브 모듈 및 루트 디렉터리 내 모든 .py 파일 전수 컴파일 검증
    extra_candidates: List[str] = list(OPTIONAL_SUB_MODULES)
    try:
        for entry in os.listdir(root):
            if entry.endswith(".py") and entry not in extra_candidates:
                extra_candidates.append(entry)
    except Exception:
        pass

    for mod_file in extra_candidates:
        fpath = os.path.abspath(os.path.join(root, mod_file))
        if fpath in checked_files or not os.path.isfile(fpath):
            continue
        checked_files.add(fpath)
        try:
            py_compile.compile(fpath, doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"PY_COMPILE_ERROR [{mod_file}]: {exc}")
        except Exception as exc:
            errors.append(f"UNEXPECTED_COMPILE_ERROR [{mod_file}]: {exc}")

    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# 제1헌법 & Case 07, Case 11 : 봇 런타임 내부 호출용 비동기 안전 핫 리로드
# ---------------------------------------------------------------------------
async def execute_safe_hot_reload(
    context: Optional[Any] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    main.py 또는 텔레그램 관리자 명령에서 비동기로 안전 업데이트 및 하드 리로드(os._exit(0))를
    수행할 때 호출하는 함수.
    """
    now_est = datetime.now(EST_TZ)
    if not force and is_update_lockdown_window(now_est):
        return {
            "status": "BLOCKED_INTRADAY_LOCKDOWN",
            "message": "평일 03:30 ~ 17:00 EST 장중 업데이트 락다운 구간입니다.",
        }

    has_ord, ord_reason = await asyncio.to_thread(has_active_orders_in_ledger)
    if not force and has_ord:
        return {
            "status": "BLOCKED_ACTIVE_ORDER",
            "message": ord_reason,
        }

    exit_code = await asyncio.to_thread(
        run_auto_update_sync,
        force_update=force,
        check_only=False,
    )
    if exit_code != 0:
        return {
            "status": "UPDATE_OR_COMPILE_FAILED",
            "exit_code": exit_code,
        }

    logger.info("♻️ [plugin_updater] 업데이트 및 컴파일 검증 통과 -> os._exit(0) 하드 킬 재기동")
    os._exit(0)


# ---------------------------------------------------------------------------
# 동기 업데이트 메인 파이프라인 (CLI 및 systemd ExecStartPre 겸용)
# ---------------------------------------------------------------------------
def run_auto_update_sync(
    force_update: bool = False,
    check_only: bool = False,
    branch: Optional[str] = None,
) -> int:
    """
    깃허브 최신 코드 Pull, 코어 모듈 프리플라이트 컴파일 검증 및 실패 시 자동 하드 롤백을 수행한다.
    반환값: 정상 완료(또는 스킵) 시 0, 치명적 오류(및 롤백 발동) 시 1.
    """
    root_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root_dir)

    now_est = datetime.now(EST_TZ)
    est_str = now_est.strftime("%Y-%m-%d %H:%M:%S EST")

    # 1. --check-only 모드: Git 통신 없이 로컬 코어 모듈 컴파일만 즉시 검증
    if check_only:
        ok, errs = verify_all_modules_compile(base_dir=root_dir, strict_required=True)
        if ok:
            print(f"✅ [{est_str}] 토스 스나이퍼 전체 코어 플러그인 컴파일 무결성 검증 통과.")
            return 0
        print(
            f"🚨 [{est_str}] 코어 플러그인 컴파일 검증 실패:\n" + "\n".join(errs),
            file=sys.stderr,
        )
        return 1

    # 2. Case 07: 평일 03:30 ~ 17:00 EST 장중 업데이트 락다운 방어
    if not force_update and is_update_lockdown_window(now_est):
        print(
            f"🛡️ [{est_str}] [Case 07 장중 업데이트 락다운] 평일 03:30 ~ 17:00 EST 구간이므로 "
            f"무인 업데이트를 스킵하고 기존 프로세스를 보호합니다."
        )
        return 0

    # 3. 로컬 장부(assassin_ledger.json) 미체결 주문 존재 시 보호
    has_ord, ord_reason = has_active_orders_in_ledger(base_dir=root_dir)
    if not force_update and has_ord:
        print(
            f"🛡️ [{est_str}] [원장 보호 가드] {ord_reason} -> 무인 업데이트를 스킵합니다."
        )
        return 0

    # 4. 현재 정상 Git Commit Hash 백업
    code, current_hash, err = run_command("git rev-parse HEAD", timeout=30, cwd=root_dir)
    if code != 0 or not current_hash:
        print(
            f"🚨 [{est_str}] 해시 백업 실패({err}). 업데이트를 중단합니다.",
            file=sys.stderr,
        )
        return 1

    print(f"✅ [{est_str}] 현재 정상 해시 백업 완료: {current_hash}")

    # 5. 대상 브랜치 결정 및 Git Pull 실행 (30초 타임아웃 적용)
    target_branch = str(
        branch or os.getenv("SNIPER_GIT_BRANCH", "main") or "main"
    ).strip()
    code, pull_out, pull_err = run_command(
        f"git pull origin {target_branch}", timeout=30, cwd=root_dir
    )
    if pull_out:
        print(pull_out)

    if code != 0:
        print(f"🚨 [{est_str}] Pull 네트워크/충돌 붕괴: {pull_err}", file=sys.stderr)
        run_command(f"git reset --hard {current_hash}", timeout=30, cwd=root_dir)
        return 1

    if "Already up to date." in pull_out or "이미 업데이트된 상태입니다." in pull_out:
        ok, errs = verify_all_modules_compile(base_dir=root_dir, strict_required=False)
        if not ok:
            print(
                f"🚨 [{est_str}] 로컬 파일 문법 손상 감지:\n" + "\n".join(errs),
                file=sys.stderr,
            )
            return 1
        return 0

    # 6. Case 11 & Case 88: 업데이트 직후 전체 코어 플러그인 프리플라이트 컴파일 검증 및 자동 롤백
    ok, compile_errors = verify_all_modules_compile(
        base_dir=root_dir, strict_required=True
    )
    if ok:
        print(
            f"✅ [{est_str}] 토스 스나이퍼 10대 코어 플러그인 프리플라이트 파이썬 컴파일 검증 통과."
        )
        return 0

    err_joined = "\n".join(compile_errors)
    print(
        f"🚨 [{est_str}] 치명적 문법 에러 또는 필수 모듈 누락 감지. 레스큐 롤백 즉시 가동:\n{err_joined}",
        file=sys.stderr,
    )
    reset_code, _reset_out, reset_err = run_command(
        f"git reset --hard {current_hash}", timeout=30, cwd=root_dir
    )
    if reset_code == 0:
        print(
            f"✅ [{est_str}] 롤백 성공. 이전 정상 해시({current_hash})로 파일 시스템 복구 완료.",
            file=sys.stderr,
        )
    else:
        print(
            f"🚨 [{est_str}] 롤백 실패. 시스템 권한 붕괴: {reset_err}",
            file=sys.stderr,
        )
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="토스 스나이퍼 방탄 자동 업데이트 및 프리플라이트 컴파일 검증기"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="장중 락다운(03:30 ~ 17:00 EST) 및 미체결 가드를 무시하고 강제 업데이트 수행",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Git Pull 없이 로컬 코어 플러그인 전체 py_compile 구문 검증만 수행",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default=os.getenv("SNIPER_GIT_BRANCH", "main"),
        help="Pull 대상 Git 브랜치명 (기본값: main)",
    )
    args, _unknown = parser.parse_known_args()

    force_flag = bool(
        args.force or str(os.getenv("SNIPER_FORCE_UPDATE", "0")).strip() == "1"
    )
    exit_code = run_auto_update_sync(
        force_update=force_flag,
        check_only=bool(args.check_only),
        branch=args.branch,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
