# -*- coding: utf-8 -*-
"""
# MODIFIED: plugin_updater.py
인피니트 스노우볼 (Infinite Snowball) v3.0 — 순수 롱(🐂 SOXL 단독) 04:07 ~ 16:00 EST 오버나이트 지층관리 스나이퍼
9대 코어 모듈 프리플라이트 컴파일 · AST 심볼 교차 검증 · 4배수 선제 스냅 치유기 · 텔레그램 잔류 큐 소각 ·
폴더별 원장/환경변수 스냅샷 철벽 보존 및 무인 Git 업데이트 코어 모듈 (Step 03 / 06)
- 제1헌법: 모든 파일 I/O 및 git/python 서브프로세스 실행을 asyncio.to_thread 로 비동기 격리 및 타임아웃 적용
- 제3헌법: 구버전 폐기 파일(daymarket_sim_plugin.py, whipsaw_plugin.py, state_SOXS.json 등) 디스크 영구 소각
- Case 07: 평일 04:00 ~ 16:05 EST 장중 백그라운드 무인 업데이트 재기동 전면 차단 및 systemd ExecStartPre 장중 락다운 지원
- Case 11 & Case 62:
  1) 9대 필수 코어 모듈(REQUIRED_CORE_MODULES) py_compile 문법 검증 + AST ImportFrom 심볼 테이블 정적 교차 검증
  2) 모바일 복사·붙여넣기 과정에서 발생한 마크다운 펜스 및 홀수 공백(1칸 -> 0칸, 9칸 -> 8칸)을
     AST 파싱 전에 4배수로 선제 스냅 교정하여 다음 줄이 오염되는 현상을 100% 원천 차단(_auto_heal_indentation_glitches)
  3) /update 실행 시 구버전 RAM 캐시(sys.modules) 간섭을 막기 위해 `python3 plugin_updater.py --verify-only` 를
     독립 서브프로세스로 호출하고 실패 시 직전 정상 커밋 해시로 즉시 원자적 롤백 수행
  4) 프리플라이트 및 업데이트 시점에 텔레그램 서버의 미확인 적체 큐(getUpdates offset = max_uid + 1)를
     즉시 강제 소각(_flush_telegram_pending_updates_sync)하고, 원격 저장소에 신규 커밋이 없을 때 로컬 수정본을 안전 보존!
- 🚨 [NEW · Case 68 폴더 위치(base_dir) 기반 격리 & git reset --hard 원장/환경변수 증발 100% 차단 쉴드]:
  1) "/home/pipiosToss" 고정 경로 하드코딩 100% 영구 제거 -> 오직 현재 실행 폴더(base_dir)의 .env 와 원장만 참조
  2) git reset --hard 실행 직전 해당 폴더 내 모든 원장(*ledger*.json, ledger_backups/*.json)과 환경설정(.env*, *config*.json)을
     메모리 및 백업 금고에 스냅샷 보존하고, git reset --hard 직후 0.001초 내에 즉시 원자적 원상 복원하여
     장외 자동 업데이트로 인한 어제 기록 초기화 및 타 폴더 원장 덮어쓰기 사고를 물리적으로 영구 봉쇄!
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import glob
import json
import logging
import os
import py_compile
import re
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from toss_api import (
    EST_TZ,
    GlobalThrottle,
    SoxlStratumLedger,
    get_bot_base_dir,
    resolve_folder_isolated_ledger_paths,
    safe_int,
)

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

_TG_TOKEN_STRICT_RE = re.compile(r"\b(\d{8,12}:[A-Za-z0-9_-]{25,60})\b")


# ---------------------------------------------------------------------------
# [핵심 0] 폴더 위치(base_dir) 전용 텔레그램 적체 큐 선제 강제 소각기 (Case 68)
# ---------------------------------------------------------------------------
def _discover_bot_token_sync(base_dir: Optional[str] = None) -> str:
    """
    🚨 Case 68:
    "/home/pipiosToss" 고정 경로 폴백을 100% 폐기하고, 오직 현재 봇이 위치한 폴더(target_dir)의
    .env 및 환경변수에서만 텔레그램 봇 토큰을 추출한다.
    """
    target_dir = get_bot_base_dir(base_dir)
    if os.path.isdir(target_dir):
        env_fp = os.path.join(target_dir, ".env")
        if os.path.isfile(env_fp):
            try:
                with open(env_fp, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                m = _TG_TOKEN_STRICT_RE.search(content)
                if m:
                    return m.group(1)
            except Exception:
                pass

    for k in (
        "TELEGRAM_BOT_TOKEN",
        "TG_BOT_TOKEN",
        "BOT_TOKEN",
        "TELEGRAM_TOKEN",
        "TG_TOKEN",
    ):
        val = str(os.environ.get(k) or "").strip()
        if "://" not in val:
            m = _TG_TOKEN_STRICT_RE.search(val)
            if m:
                return m.group(1)
    return ""


def _update_ledger_tg_offset_sync(ledger_fp: str, max_uid: int) -> None:
    if not ledger_fp or not os.path.isfile(ledger_fp) or max_uid <= 0:
        return
    try:
        with GlobalThrottle.get_file_lock(ledger_fp):
            with open(ledger_fp, "r", encoding="utf-8") as rf:
                ldata = json.load(rf)
            if isinstance(ldata, dict) and "SOXL" in ldata and isinstance(
                ldata["SOXL"], dict
            ):
                cur_saved = safe_int(ldata["SOXL"].get("last_tg_update_id"), 0)
                if max_uid > cur_saved:
                    ldata["SOXL"]["last_tg_update_id"] = max_uid
                    tmp_p = (
                        f"{ledger_fp}.ack.tmp.{os.getpid()}."
                        f"{threading.get_ident()}"
                    )
                    with open(tmp_p, "w", encoding="utf-8") as wf:
                        json.dump(ldata, wf, ensure_ascii=False, indent=2)
                        wf.flush()
                        os.fsync(wf.fileno())
                    os.replace(tmp_p, ledger_fp)
    except Exception:
        pass


def _flush_telegram_pending_updates_sync(
    base_dir: Optional[str] = None,
) -> int:
    """
    데몬 기동 전(ExecStartPre / --verify-only) 또는 /update 실행 직후에
    텔레그램 서버 큐에 남아 있는 모든 미확인 메시지(/start, /update, callback_query 등)의
    최대 update_id 를 조회하여 즉시 `offset = max_update_id + 1` 로 소비 확정(Ack)하고
    해당 폴더의 3중 원장(`primary_path`, `legacy_path`, `backup_path`)의 `last_tg_update_id` 에 기록한다.
    """
    target_dir = get_bot_base_dir(base_dir)
    bot_token = _discover_bot_token_sync(target_dir)
    if not bot_token:
        return 0

    def _call_tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://api.telegram.org/bot{bot_token}/{method}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}

    max_uid = 0
    try:
        _call_tg_api("deleteWebhook", {"drop_pending_updates": False})
    except Exception:
        pass

    try:
        res = _call_tg_api("getUpdates", {"offset": -1, "timeout": 0})
        if isinstance(res, dict) and bool(res.get("ok")):
            updates = res.get("result")
            if isinstance(updates, list) and updates:
                for u in updates:
                    if isinstance(u, dict):
                        uid = safe_int(u.get("update_id"), 0)
                        if uid > max_uid:
                            max_uid = uid
                if max_uid > 0:
                    _call_tg_api(
                        "getUpdates",
                        {"offset": max_uid + 1, "limit": 1, "timeout": 0},
                    )
    except Exception as exc:
        logger.debug("텔레그램 큐 선제 소각 예외(무시): %s", exc)

    if max_uid > 0:
        primary_fp, legacy_fp, backup_fp, _ = (
            resolve_folder_isolated_ledger_paths(base_dir=target_dir)
        )
        for l_fp in (primary_fp, legacy_fp, backup_fp):
            _update_ledger_tg_offset_sync(l_fp, max_uid)

    return max_uid


# ---------------------------------------------------------------------------
# [핵심 0-B · NEW Case 68] git reset --hard 실행 전후 폴더별 원장 & .env 철벽 보호기
# ---------------------------------------------------------------------------
def _snapshot_protected_local_files_sync(target_dir: str) -> Dict[str, bytes]:
    """
    🚨 Case 68:
    git reset --hard 가 실행되기 직전에 현재 폴더(target_dir) 내의:
    1) 3중 안전 원장(SoxlStratumLedger) 선행 동기화 실행 (가장 유효한 원장 데이터를 백업 금고에 먼저 확보)
    2) 모든 `*ledger*.json` 파일, `ledger_backups/*.json` 파일, `.env*` 및 `*config*.json`, `secrets.json`
       파일의 원본 바이트(bytes)를 메모리에 스냅샷으로 보존한다.
    """
    abs_dir = get_bot_base_dir(target_dir)
    try:
        # 업데이트 직전 현재 폴더의 가장 유효한 원장 상태를 3중 경로(고유·호환·백업금고)에 먼저 동기화
        SoxlStratumLedger(base_dir=abs_dir)
    except Exception as exc:
        logger.debug("업데이트 전 원장 선행 동기화 예외(무시): %s", exc)

    snapshots: Dict[str, bytes] = {}
    patterns = (
        os.path.join(abs_dir, "*ledger*.json"),
        os.path.join(abs_dir, ".env*"),
        os.path.join(abs_dir, "*config*.json"),
        os.path.join(abs_dir, "secrets.json"),
        os.path.join(abs_dir, "ledger_backups", "*.json"),
    )
    for pat in patterns:
        for fp in glob.glob(pat):
            if not os.path.isfile(fp):
                continue
            bname = os.path.basename(fp)
            if bname in DEPRECATED_LEGACY_FILES or ".tmp." in bname:
                continue
            try:
                with GlobalThrottle.get_file_lock(fp):
                    with open(fp, "rb") as rf:
                        raw_bytes = rf.read()
                if raw_bytes:
                    snapshots[os.path.abspath(fp)] = raw_bytes
            except Exception as exc:
                logger.warning("보호 파일 스냅샷 실패(%s): %s", fp, exc)

    return snapshots


def _restore_protected_local_files_sync(
    target_dir: str, snapshots: Dict[str, bytes]
) -> int:
    """
    🚨 Case 68:
    git reset --hard 실행 직후, 원격 저장소의 초기 파일(예: 초기화된 soxl_stratum_ledger.json)이
    로컬 디스크를 덮어썼더라도 메모리에 보존해 둔 스냅샷 바이트를 즉시 원자적으로 복원하고,
    SoxlStratumLedger 자가 치유 로더를 구동하여 어제의 기록과 시드머니를 100% 무손실 보존한다.
    """
    abs_dir = get_bot_base_dir(target_dir)
    restored_cnt = 0
    for fp, raw_bytes in (snapshots or {}).items():
        if not fp or not raw_bytes:
            continue
        try:
            parent_d = os.path.dirname(fp)
            if parent_d and not os.path.isdir(parent_d):
                os.makedirs(parent_d, exist_ok=True)
            with GlobalThrottle.get_file_lock(fp):
                tmp_fp = (
                    f"{fp}.restore.tmp.{os.getpid()}.{threading.get_ident()}"
                )
                with open(tmp_fp, "wb") as wf:
                    wf.write(raw_bytes)
                    wf.flush()
                    os.fsync(wf.fileno())
                os.replace(tmp_fp, fp)
            restored_cnt += 1
        except Exception as exc:
            logger.error("보호 파일 복원 예외(%s): %s", fp, exc)

    try:
        SoxlStratumLedger(base_dir=abs_dir)
    except Exception as exc:
        logger.debug("복원 후 원장 재검증 예외(무시): %s", exc)

    return restored_cnt


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
    target_dir = get_bot_base_dir(base_dir)
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
# [핵심 2] 4배수 선제 스냅 및 마크다운 펜스 자동 치유기 (_auto_heal_indentation_glitches)
# ---------------------------------------------------------------------------
def _auto_heal_indentation_glitches(
    filepath: str, max_passes: int = 6
) -> bool:
    """
    MODIFIED [역방향 들여쓰기 오염 원천 차단]:
    1) 파일 상단에 복사 시 섞여 들어간 텍스트나 마크다운 코드펜스(```)를 먼저 제거한다.
    2) AST 파싱 결과 SyntaxError / IndentationError 가 감지될 경우,
       먼저 코드 라인 중 4의 배수가 아닌 비정상 홀수 공백(예: 1칸 -> 0칸, 9칸 -> 8칸)을
       가장 가까운 4의 배수로 일괄 선제 스냅 교정한다!
       (블록 첫 줄이 9칸일 때 파이썬 파서가 다음 줄 8칸에서 에러를 발생시켜 정상 라인들이 9칸으로
        도미노 오염되던 현상을 100% 원천 방지!)
    """
    if not os.path.isfile(filepath):
        return False

    fence_prefix = chr(96) * 3
    coding_marker = "# -*- coding: utf-8 -*-"

    try:
        with GlobalThrottle.get_file_lock(filepath):
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                orig_src = f.read()

            src = orig_src

            # 0단계: 이미 문법적으로 완벽하면 즉시 통과
            try:
                ast.parse(src, filename=filepath)
                return False
            except (IndentationError, SyntaxError):
                pass

            # 1단계: 상단 불필요 프리픽스 및 마크다운 코드펜스 제거 + 홀수 공백(1칸, 9칸 등) 4배수 선제 스냅
            if coding_marker in src and not src.lstrip().startswith(
                coding_marker
            ):
                src = src[src.index(coding_marker) :]

            pre_lines = src.splitlines()
            snapped_lines: List[str] = []
            for line in pre_lines:
                if line.strip().startswith(fence_prefix):
                    continue
                stripped = line.lstrip(" ")
                lead = len(line) - len(stripped)
                if (
                    stripped
                    and not stripped.startswith(
                        ("#", '"', "'", ")", "]", "}")
                    )
                    and (lead % 4 != 0)
                ):
                    lead = int(round(lead / 4.0)) * 4
                snapped_lines.append((" " * lead) + stripped)

            src = "\n".join(snapped_lines) + "\n"

            # 2단계: 선제 스냅 후에도 남은 IndentationError 미세 교정
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

                    if stripped.startswith(
                        ("else:", "elif ", "except", "finally:")
                    ):
                        new_indent = max(0, prev_indent - 4)
                    elif lead_len % 4 != 0:
                        new_indent = int(round(lead_len / 4.0)) * 4
                    else:
                        new_indent = prev_indent

                    if new_indent == lead_len:
                        break

                    lines[idx] = (" " * max(0, new_indent)) + stripped
                    src = "".join(lines)
                except SyntaxError:
                    break

            if src != orig_src:
                try:
                    ast.parse(src, filename=filepath)
                except SyntaxError:
                    return False

                tmp_fp = f"{filepath}.heal.tmp"
                with open(tmp_fp, "w", encoding="utf-8") as wf:
                    wf.write(src)
                    wf.flush()
                    os.fsync(wf.fileno())
                os.replace(tmp_fp, filepath)
                logger.info(
                    "🔧 [들여쓰기/코드펜스 자동 정규화 완료] %s",
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
    target_dir = get_bot_base_dir(base_dir)
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
# [핵심 4] 스마트 Git 동기화 · Case 68 원장 철벽 보존 쉴드 및 Case 62 독립 서브프로세스 검증
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
    🚨 Case 62 & Case 68 [장외 업데이트 시 로컬 원장·환경변수 증발 100% 원천 차단]:
    1) 실행 즉시 `_flush_telegram_pending_updates_sync()` 로 텔레그램 서버의 미확인 `/update` 큐를 선제 소각한다.
    2) `_snapshot_protected_local_files_sync(target_dir)` 를 호출하여 현재 폴더의 모든 `*ledger*.json`,
       `ledger_backups/*.json`, `.env*`, `*config*.json` 파일을 메모리에 스냅샷 보존한다.
    3) `git fetch --all --prune` 후 원격 저장소에 신규 커밋이 실제로 존재할 때(`remote_ahead > 0`)만
       `git reset --hard origin/<branch>` 를 수행하고, 직후 0.001초 내에 `_restore_protected_local_files_sync()` 로
       로컬 원장과 `.env` 파일을 100% 원상 복원한다!
       (GitHub 저장소에 올라가 있던 초기 `soxl_stratum_ledger.json` 이 로컬 원장을 덮어쓰는 현상을 영구 봉쇄!)
    4) 구버전 RAM 캐시(sys.modules) 간섭을 차단하기 위해
       반드시 `[sys.executable, "plugin_updater.py", "--verify-only", "--base-dir", target_dir]` 를 독립 서브프로세스로 실행한다!
    5) 서브프로세스 검증 실패 시 즉시 이전 정상 커밋 해시로 자동 롤백하고 로컬 원장을 재복원한다.
    """
    target_dir = get_bot_base_dir(base_dir)

    _flush_telegram_pending_updates_sync(target_dir)

    # 🛡️ [Case 68 핵심 방어] git 명령 실행 전 현재 폴더의 모든 원장 및 .env 파일 스냅샷 보존!
    protected_snapshots = _snapshot_protected_local_files_sync(target_dir)

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

        rc_f, _, _ = _run_cmd_sync(
            ["git", "fetch", "--all", "--prune"],
            cwd=target_dir,
            timeout_sec=25.0,
        )
        if rc_f != 0:
            _restore_protected_local_files_sync(
                target_dir, protected_snapshots
            )
            ok_local, msg_local = verify_all_modules_compile(target_dir)
            return ok_local, f"로컬 검증 완료 (git fetch 스킵): {msg_local}", False

        target_ref = f"origin/{branch_name}"
        rc_ref, _, _ = _run_cmd_sync(
            ["git", "rev-parse", "--verify", target_ref],
            cwd=target_dir,
            timeout_sec=8.0,
        )
        if rc_ref != 0:
            target_ref = "origin/main"

        rc_cnt, cnt_str, _ = _run_cmd_sync(
            ["git", "rev-list", "--count", f"HEAD..{target_ref}"],
            cwd=target_dir,
            timeout_sec=8.0,
        )
        remote_ahead = safe_int(cnt_str, 0) if rc_cnt == 0 else 0

        changed = False
        new_hash = prev_hash
        if remote_ahead > 0:
            rc_r, _, err_r = _run_cmd_sync(
                ["git", "reset", "--hard", target_ref],
                cwd=target_dir,
                timeout_sec=15.0,
            )
            # 🛡️ git reset --hard 직후 0.001초 내에 보존된 로컬 원장(*ledger*.json) 및 .env 즉시 복원!
            _restore_protected_local_files_sync(
                target_dir, protected_snapshots
            )
            # Git 인덱스가 로컬 원장 변경을 추적하지 않도록 assume-unchanged 설정 (실패해도 무해)
            try:
                _run_cmd_sync(
                    [
                        "git",
                        "update-index",
                        "--assume-unchanged",
                        "soxl_stratum_ledger.json",
                    ],
                    cwd=target_dir,
                    timeout_sec=5.0,
                )
            except Exception:
                pass

            if rc_r != 0:
                return False, f"git reset 실패: {err_r}", False
            _, new_hash, _ = _run_cmd_sync(
                ["git", "rev-parse", "HEAD"], cwd=target_dir, timeout_sec=10.0
            )
            changed = bool(prev_hash and new_hash and prev_hash != new_hash)
        else:
            _restore_protected_local_files_sync(
                target_dir, protected_snapshots
            )

        purged_list = purge_deprecated_legacy_files(target_dir)

        # 🚨 Case 62: 디스크의 최신 plugin_updater.py 를 독립 서브프로세스로 호출하여 검증!
        updater_script = os.path.join(target_dir, "plugin_updater.py")
        rc_v, out_v, err_v = _run_cmd_sync(
            [
                sys.executable,
                updater_script,
                "--verify-only",
                "--base-dir",
                target_dir,
            ],
            cwd=target_dir,
            timeout_sec=25.0,
        )
        if rc_v != 0:
            detail_err = out_v or err_v or "알 수 없는 컴파일/심볼 검증 오류"
            if changed and prev_hash:
                _run_cmd_sync(
                    ["git", "reset", "--hard", prev_hash],
                    cwd=target_dir,
                    timeout_sec=15.0,
                )
            _restore_protected_local_files_sync(
                target_dir, protected_snapshots
            )
            return (
                False,
                f"🚨 프리플라이트 컴파일 에러 감지:\n{detail_err}",
                False,
            )

        short_hash = new_hash[:7] if new_hash else "local"
        status_note = (
            f"신규 커밋({short_hash}) 적용 · 원장 100% 보존"
            if changed
            else f"현재 버전({short_hash}) 유지 · 원장 100% 보존"
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
        _restore_protected_local_files_sync(target_dir, protected_snapshots)
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
      해당 폴더의 3중 원장 및 .env 를 100% 보존한 상태로 안전하게 프로세스를 재기동한다 (Case 68).
    """
    wait_s = max(300.0, float(interval_sec or 3600.0))
    base_dir = get_bot_base_dir(getattr(context, "base_dir", None))

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
# CLI 엔트리포인트 (`--verify-only` 독립 검증 및 `systemd ExecStartPre` Case 07/68 호환)
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
        default=get_bot_base_dir(),
        help="검증 대상 디렉토리 경로",
    )
    args, _ = parser.parse_known_args()
    target_dir = get_bot_base_dir(args.base_dir)

    purge_deprecated_legacy_files(target_dir)

    if args.verify_only:
        _flush_telegram_pending_updates_sync(target_dir)
        ok, msg = verify_all_modules_compile(target_dir)
        if ok:
            print(msg)
            return 0
        print(msg, file=sys.stderr)
        return 1

    _flush_telegram_pending_updates_sync(target_dir)

    now_est = datetime.now(EST_TZ)
    if _is_weekday_market_busy_est(now_est):
        print(
            f"🛡️ [{now_est.strftime('%Y-%m-%d %H:%M:%S EST')}] [Case 07 장중 락다운] "
            "평일 04:00 ~ 16:05 EST 구간 -> 무인 업데이트를 스킵하고 기존 검증 코드로 즉시 기동합니다."
        )
        ok, msg = verify_all_modules_compile(target_dir)
        if not ok:
            print(msg, file=sys.stderr)
            return 1
        return 0

    ok_git, msg_git, _ = _execute_git_pull_and_preflight_sync(target_dir)
    if ok_git:
        print(msg_git)
        return 0
    print(msg_git, file=sys.stderr)
    ok_local, _ = verify_all_modules_compile(target_dir)
    return 0 if ok_local else 1


if __name__ == "__main__":
    sys.exit(_cli_main())
