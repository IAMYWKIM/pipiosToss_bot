# -*- coding: utf-8 -*-
"""
# MODIFIED: plugin_updater.py
순수 롱(SOXL) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼 전용 무인 Git 원격 동기화,
폐기된 데이장/휩소/SOXS 파일 자동 청소기(purge_deprecated_legacy_files),
평일 04:00 ~ 16:05 EST 장중 업데이트 락다운(Case 07), 스마트 원장 보호 가드 및
9대 필수 코어 모듈 프리플라이트 컴파일 검증(Case 11) 플러그인 (Step 08 / 09)
- NEW: 데이장(daymarket_sim_plugin.py), 1주 휩소(whipsaw_plugin.py), 데이장 모의 상태(day_sim_state.json),
  숏 원장(state_SOXS.json) 등 영구 폐기된 구버전 파일을 디스크에서 자동 삭제하는 청소기 탑재
- MODIFIED: REQUIRED_CORE_MODULES 를 9대 필수 코어 모듈 체제로 락온하고,
  평일 04:00 ~ 16:05 EST 운용 시간대 무인 재기동 차단(Case 07) 적용
- 제1헌법 & 제3헌법 & 제4헌법 & Case 06, 07, 11 완벽 준수
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import py_compile
import shlex
import subprocess
import sys
from datetime import datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("toss_sniper.plugin_updater")

# 🚨 제4헌법: 타임존 단일 소스 락온 (America/New_York)
EST_TZ = ZoneInfo("America/New_York")

# 순수 롱(SOXL) 지층관리 스나이퍼 9대 필수 검증 코어 모듈 목록 (Case 11)
REQUIRED_CORE_MODULES: Tuple[str, ...] = (
    "toss_api.py",
    "quant_indicators_plugin.py",
    "quant_engine.py",
    "sniper_plugin.py",
    "sniper_exit_plugin.py",
    "defense_plugin.py",
    "tg_ui_plugin.py",
    "plugin_updater.py",
    "main.py",
)

# 🚨 제3헌법: 영구 폐기되어 디스크에서 즉시 삭제해야 할 구버전 데이장·휩소·SOXS 파일 목록
DEPRECATED_LEGACY_FILES: Tuple[str, ...] = (
    "daymarket_sim_plugin.py",
    "whipsaw_plugin.py",
    "day_sim_state.json",
    "day_sim_state.json.tmp",
    "state_SOXS.json",
    "state_SOXS.json.tmp",
)


def _resolve_base_dir(base_dir: Optional[str] = None) -> str:
    if base_dir and os.path.isdir(base_dir):
        return os.path.abspath(base_dir)
    prod_dir = "/home/pipiosToss"
    if os.path.isdir(prod_dir):
        return prod_dir
    return os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# 제3헌법 : 폐기된 데이장 / 1주 휩소 / SOXS 잔재 파일 디스크 자동 청소기
# ---------------------------------------------------------------------------
def purge_deprecated_legacy_files(base_dir: Optional[str] = None) -> List[str]:
    """
    NEW [제3헌법 구버전 데드코드 및 폐기 파일 영구 소각]:
    디스크에 남아 있을 수 있는 daymarket_sim_plugin.py, whipsaw_plugin.py,
    day_sim_state.json, state_SOXS.json 파일을 감지하여 안전하게 삭제한다.
    """
    root = _resolve_base_dir(base_dir)
    removed: List[str] = []
    for fname in DEPRECATED_LEGACY_FILES:
        fpath = os.path.join(root, fname)
        if os.path.isfile(fpath):
            try:
                os.remove(fpath)
                removed.append(fname)
                logger.info("🗑️ [폐기 파일 영구 소각 완료] %s", fpath)
            except Exception as exc:
                logger.debug("폐기 파일 삭제 예외(%s): %s", fpath, exc)
    return removed


# ---------------------------------------------------------------------------
# Case 07 : 평일 04:00 ~ 16:05 EST 장중 업데이트 락다운 판별기
# ---------------------------------------------------------------------------
def is_update_lockdown_window(now_est: Optional[datetime] = None) -> bool:
    """
    🚨 Case 07 (장중 업데이트 락다운):
    평일(월 ~ 금) 04:00 ~ 16:05 EST 구간에는 무인 자동 업데이트 및 재기동을 전면 차단한다.
    (04:00 프리장 호가 안정화, 04:07 ~ 16:00 지층관리 본진, 16:00 일일 결산 보호)
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    wd = dt.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    if wd in (5, 6):
        return False
    hm = dt.hour * 100 + dt.minute
    return bool(400 <= hm <= 1605)


# ---------------------------------------------------------------------------
# 스마트 원장 보호 가드 : SOXL 미체결 매수/매도 주문 보호
# ---------------------------------------------------------------------------
def has_active_orders_in_ledger(
    base_dir: Optional[str] = None,
    now_est: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    로컬 원장(state_SOXL.json 및 soxl_stratum_ledger.json)을 점검하여:
    1) 미체결 매수 주문(buy_order_id)이 대기 중이면 시간대를 불문하고 원장 보호 가드를 발동한다.
    2) 미체결 매도 주문(sell_order_id)이 대기 중이고 실전 운용 시간대(04:00 ~ 16:05 EST)이면
       주문 체결 추적을 보호하기 위해 무인 업데이트를 보류한다.
    """
    root = _resolve_base_dir(base_dir)
    dt_est = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    in_live_session = is_update_lockdown_window(dt_est)

    for ledger_name in ("soxl_stratum_ledger.json", "state_SOXL.json"):
        ledger_path = os.path.join(root, ledger_name)
        if not os.path.isfile(ledger_path):
            continue
        try:
            with open(ledger_path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            if not isinstance(data, dict):
                continue

            buy_oid = str(data.get("buy_order_id") or "").strip()
            if buy_oid:
                return (
                    True,
                    f"SOXL 미체결 정량 매수 주문 대기 중 (orderId={buy_oid})",
                )

            sell_oid = str(data.get("sell_order_id") or "").strip()
            if sell_oid and in_live_session:
                return (
                    True,
                    f"SOXL 장중 스나이퍼 매도 주문 대기 중 (orderId={sell_oid})",
                )
        except Exception:
            continue

    return False, "NO_ACTIVE_CRITICAL_ORDERS"


# ---------------------------------------------------------------------------
# 외부 쉘 명령 안전 실행기 (타임아웃 강제)
# ---------------------------------------------------------------------------
def run_command(
    cmd: str,
    timeout: int = 30,
    cwd: Optional[str] = None,
) -> Tuple[int, str, str]:
    """
    지정된 디렉토리(cwd)에서 서브프로세스 명령을 타임아웃과 함께 안전하게 실행하고
    (returncode, stdout, stderr) 튜플을 반환한다.
    """
    work_dir = _resolve_base_dir(cwd)
    try:
        args = shlex.split(cmd)
        proc = subprocess.run(
            args,
            cwd=work_dir,
            capture_output=True,
            text=True,
            timeout=max(5, int(timeout)),
            check=False,
        )
        return (
            int(proc.returncode),
            (proc.stdout or "").strip(),
            (proc.stderr or "").strip(),
        )
    except subprocess.TimeoutExpired:
        return -1, "", f"COMMAND_TIMEOUT({timeout}s): {cmd}"
    except Exception as exc:
        return -1, "", f"COMMAND_ERROR: {exc}"


# ---------------------------------------------------------------------------
# Case 11 : 9대 코어 모듈 py_compile 프리플라이트 컴파일 검증기
# ---------------------------------------------------------------------------
def verify_all_modules_compile(
    base_dir: Optional[str] = None,
    strict_required: bool = True,
) -> Tuple[bool, List[str]]:
    """
    🚨 Case 11:
    1) 폐기된 데이장/휩소 파일(daymarket_sim_plugin.py, whipsaw_plugin.py 등)을 먼저 자동 청소하고,
    2) REQUIRED_CORE_MODULES (9개 핵심 파일) 누락 여부를 확인하며,
    3) 디렉토리 내 모든 운영 .py 파일(임시 백테스트 파일 backtest_*.py 제외)에 대해
       py_compile.compile(doraise=True) 프리플라이트 문법 검증을 수행한다.
    """
    root = _resolve_base_dir(base_dir)
    purge_deprecated_legacy_files(root)

    errors: List[str] = []

    if strict_required:
        for req_file in REQUIRED_CORE_MODULES:
            req_path = os.path.join(root, req_file)
            if not os.path.isfile(req_path):
                errors.append(f"필수 코어 모듈 누락: {req_file}")

    try:
        py_files = sorted(
            f
            for f in os.listdir(root)
            if f.endswith(".py")
            and not f.startswith("backtest_")
            and f not in DEPRECATED_LEGACY_FILES
        )
    except Exception as exc:
        return False, [f"디렉토리 스캔 실패({root}): {exc}"]

    for fname in py_files:
        fpath = os.path.join(root, fname)
        if not os.path.isfile(fpath):
            continue
        try:
            py_compile.compile(fpath, doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"컴파일 오류 [{fname}]: {exc.msg}")
        except Exception as exc:
            errors.append(f"컴파일 예외 [{fname}]: {exc}")

    return (len(errors) == 0), errors


# ---------------------------------------------------------------------------
# 무인 핫-리로드 실행기 (Git Fetch/Reset + 폐기 파일 청소 + py_compile 검증 + 롤백)
# ---------------------------------------------------------------------------
def execute_safe_hot_reload(
    force: bool = False,
    restart_service: bool = False,
    base_dir: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    systemd ExecStartPre 또는 CLI(--force)에서 호출되어:
    1) Case 07 장중 락다운(평일 04:00 ~ 16:05 EST) 및 스마트 원장 보호 가드를 점검한다.
    2) 현재 Git HEAD 해시를 백업한 뒤 원격 브랜치(origin/main)와 동기화한다.
    3) 폐기된 데이장/휩소 파일을 자동 삭제하고 9개 코어 모듈 전체를 py_compile 검증하여
       단 1개의 문법 오류라도 발견되면 즉시 백업 해시로 롤백(Self-Rollback)한다.
    """
    root = _resolve_base_dir(base_dir)
    now_est = datetime.now(EST_TZ)
    ts_str = now_est.strftime("%Y-%m-%d %H:%M:%S EST")

    if not force:
        if is_update_lockdown_window(now_est):
            msg = (
                f"🛡️ [{ts_str}] [Case 07 장중 락다운] 평일 04:00 ~ 16:05 EST 구간 -> "
                "무인 업데이트를 스킵하고 기존 검증 코드로 즉시 기동합니다."
            )
            print(msg, flush=True)
            return True, msg

        has_ord, ord_reason = has_active_orders_in_ledger(
            base_dir=root, now_est=now_est
        )
        if has_ord:
            msg = (
                f"🛡️ [{ts_str}] [원장 보호 가드] {ord_reason} -> "
                "무인 업데이트를 스킵합니다."
            )
            print(msg, flush=True)
            return True, msg

    purge_deprecated_legacy_files(root)

    git_dir = os.path.join(root, ".git")
    if not os.path.isdir(git_dir):
        ok, errs = verify_all_modules_compile(
            base_dir=root, strict_required=True
        )
        if not ok:
            err_msg = f"🚨 [{ts_str}] 로컬 코드 컴파일 검증 실패:\n" + "\n".join(
                errs
            )
            print(err_msg, flush=True)
            return False, err_msg
        return (
            True,
            f"✅ [{ts_str}] 로컬 9대 코어 모듈 컴파일 검증 완료 (.git 미사용)",
        )

    code, backup_hash, err = run_command(
        "git rev-parse HEAD", timeout=20, cwd=root
    )
    if code != 0 or not backup_hash:
        ok, errs = verify_all_modules_compile(
            base_dir=root, strict_required=True
        )
        return ok, f"⚠️ Git HEAD 조회 실패({err}) · 로컬 컴파일 검증={ok}"

    branch = str(os.getenv("SNIPER_GIT_BRANCH", "main") or "main").strip()
    f_code, _f_out, f_err = run_command(
        f"git fetch origin {branch}", timeout=30, cwd=root
    )
    if f_code != 0:
        msg = f"⚠️ [{ts_str}] Git Fetch 보류({f_err}) -> 기존 버전({backup_hash[:7]}) 유지"
        print(msg, flush=True)
        return True, msg

    r_code, r_out, r_err = run_command(
        f"git reset --hard origin/{branch}", timeout=30, cwd=root
    )
    if r_code != 0:
        run_command(f"git reset --hard {backup_hash}", timeout=20, cwd=root)
        msg = f"🚨 [{ts_str}] Git Reset 실패({r_err}) -> 기존 버전({backup_hash[:7]}) 복원"
        print(msg, flush=True)
        return False, msg

    purge_deprecated_legacy_files(root)

    ok, compile_errs = verify_all_modules_compile(
        base_dir=root, strict_required=True
    )
    if not ok:
        run_command(f"git reset --hard {backup_hash}", timeout=20, cwd=root)
        purge_deprecated_legacy_files(root)
        err_msg = (
            f"🚨 [{ts_str}] 신규 코드 컴파일 에러 감지 -> 이전 정상 커밋({backup_hash[:7]})으로 즉시 롤백 완료:\n"
            + "\n".join(compile_errs[:5])
        )
        print(err_msg, flush=True)
        return False, err_msg

    summary_line = (
        r_out.strip() if r_out else f"HEAD is now at {backup_hash[:7]}"
    )
    success_msg = (
        f"🚀 [{ts_str}] 무인 업데이트 및 9대 코어 모듈 검증 완료: {summary_line}"
    )
    print(success_msg, flush=True)

    if restart_service:
        run_command(
            "sudo systemctl restart pipiosToss.service", timeout=30, cwd=root
        )

    return True, success_msg


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Toss SOXL Stratum Sniper Safe Hot-Reload & Preflight Updater"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="장중 락다운 및 원장 보호 가드를 무시하고 강제로 Git 동기화 및 컴파일 검증을 수행합니다.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="업데이트 성공 후 pipiosToss.service 를 재시작합니다.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Git 동기화 없이 폐기 파일 청소 및 9대 코어 모듈의 py_compile 무결성만 즉시 검증합니다.",
    )
    args = parser.parse_args()

    if args.verify_only:
        ok, errs = verify_all_modules_compile(strict_required=True)
        if not ok:
            print("🚨 컴파일 검증 실패:\n" + "\n".join(errs), flush=True)
            sys.exit(1)
        print(
            "✅ 폐기 파일 청소 및 전체 9개 코어 모듈 py_compile 검증 통과!",
            flush=True,
        )
        sys.exit(0)

    ok, _msg = execute_safe_hot_reload(
        force=bool(args.force),
        restart_service=bool(args.restart),
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
