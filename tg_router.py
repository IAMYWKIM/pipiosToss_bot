# =====================================================================
# FILE: tg_router.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       선택지 B (👑 HYB_35_1ST) 듀얼 스나이퍼(SOXS/SOXL) 텔레그램 라우팅 컨트롤러 코어
#       (tg_router.py 2/2 분할본 · tg_admin_plugin 서브 라우터 및 100% 리익스포트 결속)
# =====================================================================
# MODIFIED: 제3헌법 및 플러그인 분할 아키텍처 의거 시드 입력 FSM·초기화(/reset)·자가 업데이트(/update) 핸들러를 tg_admin_plugin.py로 분리하고 100% 리익스포트 결속
# MODIFIED: 전술 제어반(/settlement) 설정 변경(전원, 복리모드, 통합시드) 시 SOXS/SOXL 양쪽 원장 동시 동기화(sync_global_settings) (Case 37, 76)
# MODIFIED: 모든 인라인 콜백 제자리 갱신(edit_text) 및 except Exception as e 문법 무결성 사수 (Case 17, 26)

import html
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

from quant_engine import AssassinLedger
from sniper_plugin import (
    _reset_trailing_runtime_state,
    _reset_bottom_track_state,
    _reset_pre_leader_state,
)
from tg_ui_plugin import (
    get_main_menu_text,
    build_avwap_radar,
    build_sync_board,
    build_settlement_board,
)
from tg_admin_plugin import (
    admin_router,
    inject_admin_dependencies,
    BudgetState,
    process_set_budget,
    process_budget_input,
    cmd_reset,
    process_execute_reset,
    cmd_update,
    process_execute_update,
    process_back_to_main,
)

router = Router()
router.include_router(admin_router)

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
    inject_admin_dependencies(admin_id, event)


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        text = await get_main_menu_text()
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        pass


@router.message(Command("avwap"))
async def cmd_avwap(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        msg = await message.answer("📡 <b>듀얼 스나이퍼 레이더(👑HYB_35_1ST) 스캔 중...</b>", parse_mode="HTML")
        text, keyboard = await build_avwap_radar(api_client, shared_rsi_state)
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"🚨 <b>관제탑 렌더링 실패:</b> {html.escape(str(e))}", parse_mode="HTML")


@router.callback_query(F.data == "open_avwap")
async def process_open_avwap(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        text, keyboard = await build_avwap_radar(api_client, shared_rsi_state)
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass
    finally:
        try:
            await callback_query.answer("레이더 갱신 완료")
        except Exception as e:
            pass


@router.message(Command("sync"))
async def cmd_sync(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        msg = await message.answer("📡 <b>SOXS &amp; SOXL 듀얼 지시서 스캔 중...</b>", parse_mode="HTML")
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
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
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
        try:
            await callback_query.answer("동기화 완료")
        except Exception as e:
            pass


@router.message(Command("settlement"))
async def cmd_settlement(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    text, keyboard = await build_settlement_board()
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass


@router.callback_query(F.data == "open_settlement")
async def process_open_settlement(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        text, keyboard = await build_settlement_board()
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass


@router.callback_query(F.data.startswith("toggle_set_act_SOXS"))
async def process_toggle_set_act(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    state_data = await AssassinLedger.get_state("SOXS")
    new_act = not state_data[5]
    await AssassinLedger.sync_global_settings(is_active=new_act)
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass
    finally:
        try:
            await callback_query.answer(f"듀얼 스나이퍼(SOXS/SOXL) {'ON' if new_act else 'OFF'}")
        except Exception as e:
            pass


# 전술 제어반(/settlement) 통합 복리 모드(is_compound) vs 단리 모드 원클릭 토글 핸들러
@router.callback_query(F.data.startswith("toggle_compound_SOXS"))
async def process_toggle_compound(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    cur_comp = await AssassinLedger.get_is_compound("SOXS")
    new_comp = not cur_comp
    await AssassinLedger.sync_global_settings(is_compound=new_comp)
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass
    finally:
        try:
            mode_label = (
                "📈 통합 복리 모드 ON (실제 발생 순수익금 정밀 가감)"
                if new_comp
                else "💵 단리(시드 고정) 모드 ON"
            )
            await callback_query.answer(mode_label)
        except Exception as e:
            pass
