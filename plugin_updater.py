# -*- coding: utf-8 -*-
"""
# MODIFIED: plugin_updater.py
인피니트 스노우볼 (Infinite Snowball) v3.0 — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
9대 코어 모듈 프리플라이트 컴파일 · AST 심볼 교차 검증 · 홀수 들여쓰기 자동 치유 및 무인 Git 업데이트 코어 모듈 (Step 03 / 05)
- 제1헌법: 모든 파일 I/O 및 git/python 서브프로세스 실행을 asyncio.to_thread 로 비동기 격리 및 타임아웃 적용
- 제3헌법: 구버전 폐기 파일(daymarket_sim_plugin.py, whipsaw_plugin.py, state_SOXS.json 등) 디스크 영구 소각
- Case 07: 평일 04:00 ~ 16:05 EST 장중 백그라운드 무인 업데이트 재기동 전면 차단 및 systemd ExecStartPre 장중 락다운 지원
- Case 11 & Case 62:
  1) 9대 필수 코어 모듈(REQUIRED_CORE_MODULES) py_compile 문법 검증 + AST ImportFrom 심볼 테이블 정적 교차 검증
  2) 모바일 복사·붙여넣기 과정에서 발생한 홀수 공백(1칸/9칸 등) IndentationError 자동 치유기(_auto_heal_indentation_glitches) 내장
  3) /update 실행 시 구버전 RAM 캐시(sys.modules) 간섭을 막기 위해 `python3 plugin_updater.py --verify-only` 를
     독립 서브프로세스로 호출하고 실패 시 직전 정상 커밋 해시로 즉시 원자적 롤백 수행
  4) MODIFIED: 반환 튜플(ok, msg, changed)의 msg 문자열 정규화로 `[]` 빈 리스트 표출 버그 100% 소각
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import logging
import os
import py_compile
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from toss_api import EST_TZ, GlobalThrottle

logger = logging.getLogger("toss_sniper.plugin_updater")

# ---------------------------------------------------------------------------
# [핵심 상수] 9대 필수 코어 모듈 및 영구 폐기 레거시 파일 목록 (제3헌법)
# ---------------------------------------------------------------------------
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

DEPRECATED_LEGACY_FILES: Tuple[str, ...] = (
    "daymarket_sim_plugin.py",
    "whipsaw_plugin.py",
    "candle_recorder.py",
    "tg_router.py",
    "state_SOXS.json",
    "soxs_stratum_ledger.json",
)


# ---------------------------------------------------------------------------
# [핵심 1] 구버전 폐기 파일 자동 청소기 (purge_deprecated_legacy_files)
# ---------------------------------------------------------------------------
def purge_deprecated_legacy_files(
    base_dir: Optional[str] = None,
) -> List[str]:
    """
    🚨 제3헌법:
    구버전 데드코드 파일(daymarket_sim_plugin.py, whipsaw_plugin.py, state_SOXS.json 등)이
    디스크에 남아 있을 경우 즉시 삭제하고 삭제된 파일명 리스트를 반환한다.
    """
    target_dir = os.path.abspath(
        base_dir or os.path.dirname(os.path.abspath(__file__)) or "."
    )
    removed: List[str] = []
    for fname in DEPRECATED_LEGACY_FILES:
        fpath = os.path.join(target_dir, fname)
        if os.path.isfile(fpath):
            try:
                with GlobalThrottle.get_file_lock(fpath):
                    os.remove(fpath)
                removed.append(fname)
            except Exception as exc:
                logger.warning("폐기 파일 삭제 실패 (%s): %s", fname, exc)
    return removed


# ---------------------------------------------------------------------------
# [핵심 2] 모바일 복사·붙여넣기 공백 오타 자동 치유기 (_auto_heal_indentation_glitches)
# ---------------------------------------------------------------------------
def _auto_heal_indentation_glitches(
    filepath: str, max_passes: int = 10
) -> bool:
    """
    스마트폰/모바일 환경에서 코드를 복사해 붙여넣을 때 특정 줄 앞에 공백 1칸이 추가되거나 밀려서
    발생하는 IndentationError(예: 0칸이어야 할 최상위 상수가 1칸 들여쓰기 된 경우,
    8칸이어야 할 줄이 9칸 들여쓰기 된 경우)를 AST 파서 기반으로 자동 감지하여
    표준 4배수 들여쓰기(또는 직전/직후 라인 들여쓰기)로 즉시 교정 저장한다.
    """
    if not os.path.isfile(filepath):
        return False

    try:
        with GlobalThrottle.get_file_lock(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                src = f.read()

            healed_any = False
            for _ in range(max_passes):
                try:
                    ast.parse(src, filename=filepath)
                    break
                except IndentationError as ind_err:
                    lineno = getattr(ind_err, "lineno", None)
                    if not isinstance(lineno, int) or lineno < 1:
                        break
                    lines = src.splitlines(keepends=True)
                    if lineno > len(lines):
                        break

                    idx = lineno - 1
                    raw_line = lines[idx]
                    stripped = raw_line.lstrip(" \t")
                    if not stripped:
                        break
                    lead_len = len(raw_line) - len(stripped)

                    # 인접한 이전 유효 코드 라인의 들여쓰기 탐색
                    prev_indent = 0
                    for p_i in range(idx - 1, -1, -1):
                        p_str = lines[p_i].strip()
                        if p_str and not p_str.startswith("#"):
                            prev_indent = len(lines[p_i]) - len(
                                lines[p_i].lstrip(" ")
                            )
                            if p_str.endswith(":"):
                                prev_indent += 4
                            break

                    if lead_len % 4 != 0:
                        nearest_4 = int(round(lead_len / 4.0)) * 4
                        new_indent = (
                            prev_indent
                            if abs(lead_len - prev_indent) <= 2
                            else nearest_4
                        )
                    else:
                        new_indent = prev_indent

                    if new_indent == lead_len:
                        break

                    lines[idx] = (" " * max(0, new_indent)) + stripped
                    src = "".join(lines)
                    healed_any = True
                except SyntaxError:
                    break

            if healed_any:
                tmp_fp = f"{filepath}.heal.tmp"
                with open(tmp_fp, "w", encoding="utf-8") as wf:
                    wf.write(src)
                    wf.flush()
                    os.fsync(wf.fileno())
                os.replace(tmp_fp, filepath)
                logger.info(
                    "🔧 [들여쓰기 공백 오타 자동 치유 완료] %s",
                    os.path.basename(filepath),
                )
                return True
    except Exception as exc:
        logger.debug("들여쓰기 자동 치유 스킵 (%s): %s", filepath, exc)
    return False


# ---------------------------------------------------------------------------
# [핵심 3] AST 최상위 익스포트 심볼 추출기 및 9대 코어 모듈 정적 교차 검증기
# ---------------------------------------------------------------------------
def _extract_module_exported_symbols(tree: ast.AST) -> Set[str]:
    """
    AST 트리에서 모듈 최상위 레벨(또는 조건문 내부)에 정의된 모든 함수, 클래스, 변수, 임포트 별칭을 추출한다.
    """
    exported: Set[str] = set()

    for node in ast.walk(tree):
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            exported.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    exported.add(target.id)
                elif isinstance(target, (ast.Tuple, ast.List)):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            exported.add(elt.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                exported.add(node.target.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                exported.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    exported.add(alias.asname or alias.name)

    return exported


def verify_all_modules_compile(
    base_dir: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    🚨 Case 11 & Case 62:
    1) 9대 필수 코어 모듈(REQUIRED_CORE_MODULES)이 디스크에 모두 존재하는지 확인한다.
    2) 홀수 공백 들여쓰기 오타 자동 치유(_auto_heal_indentation_glitches) 후
       py_compile.compile(doraise=True) 및 ast.parse() 로 문법 무결성을 검증한다.
    3) 9대 코어 모듈 간 `from <local_module> import <symbol>` 구문을 전수 대조하여,
       대상 모듈에 존재하지 않는 심볼을 임포트하는 경우 즉시 에러 원인을 반환한다.
    """
    target_dir = os.path.abspath(
        base_dir or os.path.dirname(os.path.abspath(__file__)) or "."
    )
    errors: List[str] = []
    ast_trees: Dict[str, ast.AST] = {}
    exported_map: Dict[str, Set[str]] = {}

    # 1단계: 파일 존재 확인 · 들여쓰기 자동 치유 · py_compile & AST 파싱
    for fname in REQUIRED_CORE_MODULES:
        fpath = os.path.join(target_dir, fname)
        mod_name = fname[:-3]
        if not os.path.isfile(fpath):
            errors.append(f"필수 코어 모듈 누락: {fname}")
            continue

        _auto_heal_indentation_glitches(fpath)

        try:
            py_compile.compile(fpath, doraise=True)
        except py_compile.PyCompileError as comp_err:
            errors.append(f"컴파일 오류 [{fname}]: {comp_err.msg}")
            continue
        except Exception as exc:
            errors.append(f"컴파일 예외 [{fname}]: {exc}")
            continue

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                src = f.read()
            tree = ast.parse(src, filename=fpath)
            ast_trees[mod_name] = tree
            exported_map[mod_name] = _extract_module_exported_symbols(tree)
        except Exception as exc:
            errors.append(f"AST 파싱 오류 [{fname}]: {exc}")

    if errors:
        return False, "🚨 컴파일/문법 검증 실패:\n" + "\n".join(errors)

    # 2단계: 9대 로컬 모듈 간 from <mod> import <symbol> 심볼 존재 여부 교차 검증
    local_mod_names = {f[:-3] for f in REQUIRED_CORE_MODULES}
    for mod_name, tree in ast_trees.items():
        fname = f"{mod_name}.py"
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module in local_mod_names
            ):
                target_mod = node.module
                target_exports = exported_map.get(target_mod, set())
                for alias in node.names:
                    sym = alias.name
                    if sym == "*":
                        continue
                    if sym not in target_exports:
                        errors.append(
                            f"심볼 임포트 불일치 [{fname}]: from {target_mod} import {sym} ('{target_mod}.py' 내 '{sym}' 미정의)"
                        )

    if errors:
        return False, "🚨 컴파일/AST 임포트 검증 실패:\n" + "\n".join(errors)

    return (
        True,
        f"✅ 9대 코어 모듈 py_compile 및 AST 심볼 정합성 100% 통과 ({len(REQUIRED_CORE_MODULES)}개 파일)",
    )


# ---------------------------------------------------------------------------
# [핵심 4] Git 동기화 및 Case 62 독립 서브프로세스 검증·롤백 엔진
# ---------------------------------------------------------------------------
def _run_cmd_sync(
    cmd: List[str], cwd: str, timeout_sec: float = 30.0
) -> Tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    return (
        int(proc.returncode),
        (proc.stdout or "").strip(),
        (proc.stderr or "").strip(),
    )


def _execute_git_pull_and_preflight_sync(
    base_dir: Optional[str] = None,
) -> Tuple[bool, str, bool]:
    """
    🚨 Case 62:
    1) 현재 HEAD 해시(prev_hash)를 기록한다.
    2) git fetch --all 및 git reset --hard origin/<branch> 로 최신 코드를 반영한다.
    3) 구버전 RAM 캐시(sys.modules) 간섭을 차단하기 위해
       반드시 `[sys.executable, "plugin_updater.py", "--verify-only"]` 를 독립 서브프로세스로 실행한다!
    4) 서브프로세스 검증 실패 시 즉시 `git reset --hard <prev_hash>` 로 자동 롤백한다.
    5) MODIFIED: 항상 비어 있지 않은 명확한 결과 문자열(summary_msg)을 반환하여 `[]` 출력 버그를 차단한다.
    """
    target_dir = os.path.abspath(
        base_dir or os.path.dirname(os.path.abspath(__file__)) or "."
    )

    try:
        rc_h, prev_hash, _ = _run_cmd_sync(
            ["git", "rev-parse", "HEAD"], cwd=target_dir, timeout_sec=10.0
        )
        if rc_h != 0 or not prev_hash:
            prev_hash = ""

        rc_b, branch_name, _ = _run_cmd_sync(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=target_dir,
            timeout_sec=10.0,
        )
        if rc_b != 0 or not branch_name or branch_name == "HEAD":
            branch_name = "main"

        rc_f, _, err_f = _run_cmd_sync(
            ["git", "fetch", "--all", "--prune"],
            cwd=target_dir,
            timeout_sec=25.0,
        )
        if rc_f != 0:
            return False, f"git fetch 실패: {err_f}", False

        # origin/<branch> 또는 origin/main 결정
        target_ref = f"origin/{branch_name}"
        rc_ref, _, _ = _run_cmd_sync(
            ["git", "rev-parse", "--verify", target_ref],
            cwd=target_dir,
            timeout_sec=8.0,
        )
        if rc_ref != 0:
            target_ref = "origin/main"

        rc_r, _, err_r = _run_cmd_sync(
            ["git", "reset", "--hard", target_ref],
            cwd=target_dir,
            timeout_sec=15.0,
        )
        if rc_r != 0:
            return False, f"git reset 실패: {err_r}", False

        _, new_hash, _ = _run_cmd_sync(
            ["git", "rev-parse", "HEAD"], cwd=target_dir, timeout_sec=10.0
        )
        changed = bool(prev_hash and new_hash and prev_hash != new_hash)

        # 구버전 폐기 파일 청소
        purged_list = purge_deprecated_legacy_files(target_dir)

        # 🚨 Case 62: 디스크의 최신 plugin_updater.py 를 독립 서브프로세스로 호출하여 검증!
        updater_script = os.path.join(target_dir, "plugin_updater.py")
        rc_v, out_v, err_v = _run_cmd_sync(
            [sys.executable, updater_script, "--verify-only"],
            cwd=target_dir,
            timeout_sec=25.0,
        )
        if rc_v != 0:
            detail_err = out_v or err_v or "알 수 없는 컴파일/심볼 검증 오류"
            if prev_hash:
                _run_cmd_sync(
                    ["git", "reset", "--hard", prev_hash],
                    cwd=target_dir,
                    timeout_sec=15.0,
                )
            return (
                False,
                f"🚨 프리플라이트 컴파일 에러 감지 (이전 정상 해시로 즉시 롤백 완료):\n{detail_err}",
                False,
            )

        short_hash = new_hash[:7] if new_hash else "latest"
        status_note = (
            f"신규 커밋({short_hash}) 적용"
            if changed
            else f"최신 커밋({short_hash}) 유지"
        )
        verify_note = (
            out_v
            if out_v
            else f"✅ 9대 코어 모듈 검증 통과 ({len(REQUIRED_CORE_MODULES)}개 파일)"
        )
        purge_note = (
            f" · 폐기파일 {len(purged_list)}개 소각" if purged_list else ""
        )
        summary_msg = f"{status_note} · {verify_note}{purge_note}"
        return True, summary_msg, changed

    except Exception as exc:
        return False, f"업데이트 실행 예외: {exc}", False


async def execute_git_pull_and_preflight(
    base_dir: Optional[str] = None,
) -> Tuple[bool, str, bool]:
    """
    제1헌법: 동기 Git 및 서브프로세스 검증을 백그라운드 스레드로 격리 실행한다.
    """
    return await asyncio.wait_for(
        asyncio.to_thread(_execute_git_pull_and_preflight_sync, base_dir),
        timeout=60.0,
    )


# ---------------------------------------------------------------------------
# [핵심 5] 백그라운드 무인 자동 업데이트 루프 (Case 07: 장중 재기동 전면 차단)
# ---------------------------------------------------------------------------
def _is_weekday_market_busy_est(now_est: Optional[datetime] = None) -> bool:
    """
    🚨 Case 07:
    평일(월~금) 04:00 ~ 16:05 EST 장중에는 무인 자동 업데이트로 인한 프로세스 재기동을 원천 차단한다.
    """
    dt = now_est.astimezone(EST_TZ) if now_est else datetime.now(EST_TZ)
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 100 + dt.minute
    return 400 <= hm <= 1605


async def run_auto_update_loop(
    context: Any,
    interval_sec: float = 3600.0,
) -> None:
    """
    백그라운드 무인 자동 업데이트 워커 (main.py 에서 정식 임포트):
    - 평일 04:00 ~ 16:05 EST 장중에는 절대 재기동하지 않고 대기한다 (Case 07).
    - 장외 시간대에 원격 저장소에 새로운 커밋이 감지되고 프리플라이트 검증을 통과했을 때만
      안전하게 프로세스를 재기동한다.
    """
    wait_s = max(300.0, float(interval_sec or 3600.0))
    base_dir = getattr(
        context,
        "base_dir",
        os.path.dirname(os.path.abspath(__file__)) or ".",
    )

    while not context.shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                context.shutdown_event.wait(), timeout=wait_s
            )
            break
        except asyncio.TimeoutError:
            pass

        now_est = datetime.now(EST_TZ)
        if _is_weekday_market_busy_est(now_est):
            continue

        # 미체결 주문이 진행 중이면 무인 업데이트 스킵
        try:
            st = context.ledger.get_state("SOXL")
            if str(st.get("buy_order_id") or "").strip() or str(
                st.get("sell_order_id") or ""
            ).strip():
                continue
        except Exception:
            pass

        try:
            ok, msg, changed = await execute_git_pull_and_preflight(base_dir)
            if ok and changed:
                logger.info(
                    "🚀 [장외 무인 자동 업데이트 검증 통과] %s -> 재기동 수행",
                    msg,
                )
                if hasattr(context, "send_slim_alert"):
                    await context.send_slim_alert(
                        title="🚀 [장외 자동 업데이트 완료]",
                        body=f"{msg} · 데몬을 자동 재기동합니다.",
                    )
                await asyncio.sleep(0.5)
                os._exit(0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("무인 업데이트 점검 루프 예외: %s", exc)


# ---------------------------------------------------------------------------
# CLI 엔트리포인트 (`--verify-only` 독립 검증 및 `systemd ExecStartPre` Case 07 호환)
# ---------------------------------------------------------------------------
def _cli_main() -> int:
    parser = argparse.ArgumentParser(
        description="Infinite Snowball v3.0 Preflight Compiler & AST Verifier"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="디스크의 9대 코어 모듈 py_compile 및 AST 심볼 정합성만 검증하고 종료합니다.",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=os.path.dirname(os.path.abspath(__file__)) or ".",
        help="검증 대상 디렉토리 경로",
    )
    args, _ = parser.parse_known_args()

    purge_deprecated_legacy_files(args.base_dir)

    # 1) --verify-only 플래그가 지정된 경우: 순수 컴파일 및 AST 심볼 정합성 검증만 즉시 수행
    if args.verify_only:
        ok, msg = verify_all_modules_compile(args.base_dir)
        if ok:
            print(msg)
            return 0
        print(msg, file=sys.stderr)
        return 1

    # 2) 인자 없이 호출된 경우 (systemd ExecStartPre 등): Case 07 장중 락다운 판별
    now_est = datetime.now(EST_TZ)
    if _is_weekday_market_busy_est(now_est):
        print(
            f"🛡️ [{now_est.strftime('%Y-%m-%d %H:%M:%S EST')}] [Case 07 장중 락다운] "
            "평일 04:00 ~ 16:05 EST 구간 -> 무인 업데이트를 스킵하고 기존 검증 코드로 즉시 기동합니다."
        )
        ok, msg = verify_all_modules_compile(args.base_dir)
        if not ok:
            print(msg, file=sys.stderr)
            return 1
        return 0

    ok_git, msg_git, _ = _execute_git_pull_and_preflight_sync(args.base_dir)
    if ok_git:
        print(msg_git)
        return 0
    print(msg_git, file=sys.stderr)
    ok_local, msg_local = verify_all_modules_compile(args.base_dir)
    return 0 if ok_local else 1


if __name__ == "__main__":
    sys.exit(_cli_main())
