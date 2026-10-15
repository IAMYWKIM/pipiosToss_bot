# =====================================================================
# FILE: tg_router.py
# 목적: 03:58 EST 숏 단일 스나이퍼 텔레그램 명령 라우팅 컨트롤러
# =====================================================================
# MODIFIED: 초과 Case 71 의거 구버전 02:58 시계열 잔재를 03:58 EST로 100% 영구 동기화
# MODIFIED: 예산 입력 시 음수/0/NaN/Inf 유입으로 인한 장부 오염 방어 로직 주입

import os
import math
import html
import asyncio
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from quant_engine import AssassinLedger
from tg_ui_plugin import (
    get_main_menu_text,
    build_avwap_radar,
    build_sync_board,
    build_settlement_board,
)

router = Router()
api_client = None
ADMIN_CHAT_ID = None
wakeup_event = None
shared_rsi_state = None

def inject_dependencies(client, admin_id, event, rsi_state):
    global api_client, ADMIN_CHAT_ID, wakeup_event, shared_rsi_state
    api_client = client
    ADMIN_CHAT_ID = admin_id
    wakeup_event = event
    shared_rsi_state = rsi_state

class BudgetState(StatesGroup):
    waiting_for_budget = State()

@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try: await message.answer(get_main_menu_text(), parse_mode="HTML")
    except Exception: pass

@router.message(Command("avwap"))
async def cmd_avwap(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        msg = await message.answer("📡 <b>스나이퍼 레이더 스캔 중...</b>", parse_mode="HTML")
        text, keyboard = await build_avwap_radar(api_client, shared_rsi_state)
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"🚨 <b>관제탑 렌더링 실패:</b> {html.escape(str(e))}", parse_mode="HTML")

@router.callback_query(F.data == "open_avwap")
async def process_open_avwap(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        text, keyboard = await build_avwap_radar(api_client, shared_rsi_state)
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass
    finally:
        try: await callback_query.answer("레이더 갱신 완료")
        except Exception: pass

@router.message(Command("sync"))
async def cmd_sync(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        msg = await message.answer("📡 <b>SOXS 지시서 스캔 중...</b>", parse_mode="HTML")
        text = await build_sync_board(api_client)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"🚨 <b>지시서 동기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML")

@router.callback_query(F.data == "open_sync")
async def process_open_sync(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        text = await build_sync_board(api_client)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass
    finally:
        try: await callback_query.answer("동기화 완료")
        except Exception: pass

@router.message(Command("settlement"))
async def cmd_settlement(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    text, keyboard = await build_settlement_board()
    try: await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception: pass

@router.callback_query(F.data == "open_settlement")
async def process_open_settlement(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        text, keyboard = await build_settlement_board()
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass

@router.callback_query(F.data.startswith("toggle_set_act_SOXS"))
async def process_toggle_set_act(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    state_data = await AssassinLedger.get_state("SOXS")
    await AssassinLedger.save_state("SOXS", is_active=not state_data[5])
    if wakeup_event: wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try: await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass

@router.callback_query(F.data.startswith("set_budget_SOXS"))
async def process_set_budget(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.set_state(BudgetState.waiting_for_budget)
    # MODIFIED: 초과 Case 71 의거 03:58 EST 시계열 텍스트 동기화
    text = (
        "⌨️ <b>SOXS 고정 투입 예산 입력 (USD)</b>\n\n"
        "▫️ 03:58 EST 데이장 마감 스나이핑 출동 시 투입할 달러 예산을 숫자로 전송하십시오."
    )
    try: await callback_query.message.edit_text(text, parse_mode="HTML")
    except Exception as e:
        pass

@router.message(BudgetState.waiting_for_budget)
async def process_budget_input(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    try:
        budget = float(message.text.strip())
        # MODIFIED: 0 이하 또는 비정상 부동소수점(NaN/Inf) 입력 원천 차단
        if budget <= 0.0 or not math.isfinite(budget):
            raise ValueError("Budget must be a positive finite number.")
        await AssassinLedger.save_state("SOXS", budget=budget)
        if wakeup_event: wakeup_event.set()
        await state.clear()
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 제어반으로", callback_data="open_settlement")]])
        await message.answer(f"✅ <b>SOXS 예산 ${budget:,.2f} 락온 완료.</b>", reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        await message.answer("🚨 0보다 큰 유효한 숫자를 입력하세요.", parse_mode="HTML")

@router.message(Command("reset"))
async def cmd_reset(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ SOXS 장부 초기화", callback_data="execute_single_reset")],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[SOXS 장부 초기화]</b>\n\n"
        "경고: SOXS 로컬 장부(평단가, 목표가, 주문 ID, 당일 진입 락)를 "
        "100% 영구 소각하고 0점으로 원자적 초기화를 수행합니다.\n진행하시겠습니까?"
    )
    try: await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception: pass

@router.callback_query(F.data == "execute_single_reset")
async def process_execute_reset(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        await AssassinLedger.reset_all_ledgers()
        if wakeup_event: wakeup_event.set()
        await callback_query.answer("✅ SOXS 장부 0점 소각 완료", show_alert=True)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")]])
        await callback_query.message.edit_text(
            "✅ <b>[SOXS 장부 영구 소각 완료]</b>\n▫️ 로컬 장부가 0점으로 원자적 초기화되었습니다.",
            reply_markup=keyboard, parse_mode="HTML",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            try: await callback_query.message.answer(f"🚨 <b>초기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML")
            except Exception: pass

@router.message(Command("update"))
async def cmd_update(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 구글 클라우드 서버 탑재", callback_data="execute_update")],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = "⚠️ <b>[시스템 자가 업데이트]</b>\n\n최신 코드를 다운로드하고 재기동합니다. 진행하시겠습니까?"
    try: await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception: pass

@router.callback_query(F.data == "execute_update")
async def process_execute_update(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        await callback_query.message.edit_text("🔄 <b>GitHub 최신 코드 다운로드 중...</b>", parse_mode="HTML")
        def _run_git_update():
            import subprocess, py_compile
            def run_cmd(cmd):
                proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    out, err = proc.communicate(timeout=30)
                    return proc.returncode, out.strip(), err.strip()
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate()
                    return -1, out.strip(), "Subprocess Timeout Expired (네트워크 교착 상태 차단)"
            code, current_hash, err = run_cmd("git rev-parse HEAD")
            if code != 0: return False, f"해시 백업 실패: {err}"
            code, fetch_out, fetch_err = run_cmd("git fetch origin main")
            if code != 0: return False, f"Fetch 실패:\n{fetch_err}"
            code, local_hash, _ = run_cmd("git rev-parse HEAD")
            code, remote_hash, _ = run_cmd("git rev-parse origin/main")
            if local_hash == remote_hash: return True, "Already up to date."
            code, reset_out, reset_err = run_cmd("git reset --hard origin/main")
            if code != 0:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"Hard Reset 붕괴:\n{reset_err}"
            try:
                for mod_file in ("main.py", "sniper_plugin.py", "tg_router.py", "tg_ui_plugin.py", "quant_engine.py", "toss_api.py", "candle_recorder.py"):
                    if os.path.exists(mod_file): py_compile.compile(mod_file, doraise=True)
            except Exception as e:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"문법 에러 감지. 롤백 완료:\n{str(e)}"
            return True, f"업데이트 성공:\n{reset_out}"

        success, msg = await asyncio.to_thread(_run_git_update)
        if success:
            if "Already up to date." in msg:
                await callback_query.message.edit_text("✅ <b>서버 탑재 완료</b>\n▫️ 최신 버전입니다.", parse_mode="HTML")
            else:
                await callback_query.message.edit_text(f"🚀 <b>탑재 성공. 재기동 격발.</b>\n<pre>{html.escape(msg)}</pre>", parse_mode="HTML")
                await asyncio.sleep(1.0)
                os._exit(0)
        else:
            await callback_query.message.edit_text(f"🚨 <b>업데이트 실패 (롤백)</b>\n<pre>{html.escape(msg)}</pre>", parse_mode="HTML")
    except Exception as e:
        await callback_query.message.edit_text(f"🚨 <b>서버 탑재 에러:</b> {html.escape(str(e))}", parse_mode="HTML")

@router.callback_query(F.data == "back_to_main")
async def process_back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID: return
    await state.clear()
    try:
        await callback_query.answer()
        await callback_query.message.edit_text(get_main_menu_text(), reply_markup=None, parse_mode="HTML")
    except Exception as e:
        pass
