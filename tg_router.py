# =====================================================================
# FILE: tg_router.py
# 목적: 09:29 선제 매수 & 09:30 오프닝 노출 양방향(SOXL/SOXS) 스나이퍼 텔레그램 명령/콜백 라우팅 컨트롤러
# 구조: tg_ui_plugin.py 빌더 플러그인과 결속된 슬림 아키텍처 (main.py 임포트 규격 100% 호환)
# =====================================================================
# MODIFIED: 단일 책임 원칙(제3헌법) 준수 - UI 연산/렌더링 로직을 tg_ui_plugin.py로 분리하여 파일 경량화 달성
# MODIFIED: Case 02 & Case 09 방어 - /reset 양방향(SOXL/SOXS) 장부 원자적 0점 소각 결속
# MODIFIED: Case 11 & 프리플라이트 검증 - /update 시 분리된 tg_ui_plugin.py 포함 7대 코어 모듈 문법 검증 강제

import os
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


def inject_dependencies(client, admin_id, event):
    global api_client, ADMIN_CHAT_ID, wakeup_event
    api_client = client
    ADMIN_CHAT_ID = admin_id
    wakeup_event = event


class BudgetState(StatesGroup):
    waiting_for_budget = State()


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /start 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        await message.answer(get_main_menu_text(), parse_mode="HTML")
    except Exception as e:
        print(f"🚨 [/start 응답 붕괴 방어] {e}", flush=True)


@router.message(Command("avwap"))
async def cmd_avwap(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /avwap 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        msg = await message.answer(
            "📡 <b>양방향 스나이퍼 레이더 스캔 중...</b>", parse_mode="HTML"
        )
        text, keyboard = await build_avwap_radar(api_client)
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(
            f"🚨 <b>관제탑 렌더링 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "open_avwap")
async def process_open_avwap(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_avwap (User: {user.id})", flush=True)
    await state.clear()
    try:
        text, keyboard = await build_avwap_radar(api_client)
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback_query.message.answer(
                f"🚨 <b>관제탑 갱신 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
            )
    finally:
        try:
            await callback_query.answer("레이더 갱신 완료")
        except Exception:
            pass


@router.message(Command("sync"))
async def cmd_sync(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /sync 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    try:
        msg = await message.answer(
            "📡 <b>양방향 통합 지시서 스캔 중...</b>", parse_mode="HTML"
        )
        text = await build_sync_board(api_client)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await msg.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(
            f"🚨 <b>통합 지시서 동기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "open_sync")
async def process_open_sync(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_sync (User: {user.id})", flush=True)
    await state.clear()
    try:
        text = await build_sync_board(api_client)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 새로고침", callback_data="open_sync")],
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")],
            ]
        )
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await callback_query.message.answer(
                f"🚨 <b>갱신 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
            )
    finally:
        try:
            await callback_query.answer("동기화 완료")
        except Exception:
            pass


@router.message(Command("settlement"))
async def cmd_settlement(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /settlement 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    text, keyboard = await build_settlement_board()
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data == "open_settlement")
async def process_open_settlement(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] open_settlement (User: {user.id})", flush=True)
    await state.clear()
    try:
        text, keyboard = await build_settlement_board()
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("toggle_set_act_"))
async def process_toggle_set_act(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[3].upper()
    print(f"💬 [TG 콜백 수신] toggle_set_act_{symbol} (User: {user.id})", flush=True)
    state_data = await AssassinLedger.get_state(symbol)
    await AssassinLedger.save_state(symbol, is_active=not state_data[5])
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("toggle_profit_rate_"))
async def process_toggle_profit_rate(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[3].upper()
    print(f"💬 [TG 콜백 수신] toggle_profit_rate_{symbol} (User: {user.id})", flush=True)
    state_data = await AssassinLedger.get_state(symbol)
    current_rate = state_data[2]
    new_rate = 0.5 if current_rate == 1.0 else 1.0
    await AssassinLedger.save_state(symbol, target_profit_rate=new_rate)
    if wakeup_event:
        wakeup_event.set()
    text, keyboard = await build_settlement_board()
    try:
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
        await callback_query.answer(
            f"✅ {symbol} 익절 목표가 +{new_rate}%로 변경되었습니다.", show_alert=True
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("set_budget_"))
async def process_set_budget(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    symbol = callback_query.data.split("_")[2].upper()
    print(f"💬 [TG 콜백 수신] set_budget_{symbol} (User: {user.id})", flush=True)
    await state.set_state(BudgetState.waiting_for_budget)
    await state.update_data(symbol=symbol)
    text = (
        f"⌨️ <b>{html.escape(symbol)} 고정 투입 예산 입력 (USD)</b>\n\n"
        "▫️ 09:29 프리장 선제 스나이핑 출동 시 투입할 달러 예산을 숫자로 전송하십시오."
    )
    try:
        await callback_query.message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


@router.message(BudgetState.waiting_for_budget)
async def process_budget_input(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return

    user_data = await state.get_data()
    symbol = user_data.get("symbol")
    if not symbol:
        return

    print(
        f"💬 [TG 상태 수신] {symbol} 예산 입력 접수: {message.text.strip()} (User: {user.id})",
        flush=True,
    )
    try:
        budget = float(message.text.strip())
        await AssassinLedger.save_state(symbol, budget=budget)
        if wakeup_event:
            wakeup_event.set()
        await state.clear()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 제어반으로", callback_data="open_settlement")]
            ]
        )
        await message.answer(
            f"✅ <b>{html.escape(symbol)} 예산 ${budget:,.2f} 락온 완료.</b>",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception:
        await message.answer("🚨 유효한 숫자를 입력하세요.", parse_mode="HTML")


@router.message(Command("reset"))
async def cmd_reset(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /reset 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ 양방향(SOXL/SOXS) 장부 초기화",
                    callback_data="execute_dual_reset",
                )
            ],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[SOXL / SOXS 양방향 장부 초기화]</b>\n\n"
        "경고: SOXL 및 SOXS 양방향 로컬 장부(평단가, 목표가, 주문 ID, NQ 기록, 당일 진입 락)를 "
        "100% 영구 소각하고 0점으로 원자적 초기화를 수행합니다.\n"
        "진행하시겠습니까?"
    )
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.in_({"execute_dual_reset", "execute_single_reset"}))
async def process_execute_dual_reset(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] execute_dual_reset (User: {user.id})", flush=True)
    await state.clear()
    try:
        # Case 02: SOXL 및 SOXS 양방향 장부 원자적 0점 소각
        await AssassinLedger.reset_all_ledgers()
        if wakeup_event:
            wakeup_event.set()

        await callback_query.answer("✅ SOXL/SOXS 양방향 장부 0점 소각 완료", show_alert=True)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")]
            ]
        )
        await callback_query.message.edit_text(
            "✅ <b>[SOXL / SOXS 양방향 장부 영구 소각 완료]</b>\n"
            "▫️ 롱(SOXL) 및 숏(SOXS) 로컬 장부가 모두 0점으로 원자적 초기화되었습니다.",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            try:
                await callback_query.message.answer(
                    f"🚨 <b>초기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
                )
            except Exception:
                pass


@router.message(Command("update"))
async def cmd_update(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 수신] /update 명령 하달 (User: {user.id})", flush=True)
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 구글 클라우드 서버 탑재", callback_data="execute_update"
                )
            ],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[시스템 자가 업데이트]</b>\n\n"
        "경고: 깃허브(main) 최신 코드를 다운로드하여 프리플라이트 컴파일 검증 후 시스템을 재기동(os._exit)합니다.\n"
        "진행하시겠습니까?"
    )
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data == "execute_update")
async def process_execute_update(
    callback_query: types.CallbackQuery, state: FSMContext
):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] execute_update (User: {user.id})", flush=True)
    await state.clear()
    try:
        await callback_query.message.edit_text(
            "🔄 <b>GitHub 최신 코드 다운로드 및 프리플라이트 컴파일 검증 중...</b>",
            parse_mode="HTML",
        )

        def _run_git_update():
            import subprocess
            import py_compile

            def run_cmd(cmd):
                proc = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    out, err = proc.communicate(timeout=20)
                    return proc.returncode, out.strip(), err.strip()
                except subprocess.TimeoutExpired:
                    proc.kill()
                    return -1, "", "Subprocess Timeout Expired"

            code, current_hash, err = run_cmd("git rev-parse HEAD")
            if code != 0:
                return False, f"해시 백업 실패: {err}"

            code, fetch_out, fetch_err = run_cmd("git fetch origin main")
            if code != 0:
                return False, f"Fetch 실패:\n{fetch_err}"

            code, local_hash, _ = run_cmd("git rev-parse HEAD")
            code, remote_hash, _ = run_cmd("git rev-parse origin/main")

            if local_hash == remote_hash:
                return True, "Already up to date."

            code, reset_out, reset_err = run_cmd("git reset --hard origin/main")
            if code != 0:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"Hard Reset 붕괴 (원상 복구됨):\n{reset_err}"

            # NEW: 분리된 tg_ui_plugin.py를 포함한 7대 코어 파일 프리플라이트 컴파일 검증
            try:
                for mod_file in (
                    "main.py",
                    "sniper_plugin.py",
                    "tg_router.py",
                    "tg_ui_plugin.py",
                    "quant_engine.py",
                    "toss_api.py",
                    "candle_recorder.py",
                ):
                    if os.path.exists(mod_file):
                        py_compile.compile(mod_file, doraise=True)
            except Exception as e:
                run_cmd(f"git reset --hard {current_hash}")
                return False, f"문법 에러 감지. 롤백 완료:\n{str(e)}"

            return True, f"업데이트 성공:\n{reset_out}"

        success, msg = await asyncio.to_thread(_run_git_update)

        if success:
            if "Already up to date." in msg:
                await callback_query.message.edit_text(
                    "✅ <b>서버 탑재 완료</b>\n▫️ 구글 클라우드 서버가 이미 최신 버전입니다.",
                    parse_mode="HTML",
                )
            else:
                await callback_query.message.edit_text(
                    f"🚀 <b>탑재 성공. 코어 재기동(os._exit) 격발.</b>\n<pre>{html.escape(msg)}</pre>",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1.0)
                os._exit(0)
        else:
            await callback_query.message.edit_text(
                f"🚨 <b>업데이트 실패 (롤백됨)</b>\n<pre>{html.escape(msg)}</pre>",
                parse_mode="HTML",
            )

    except Exception as e:
        await callback_query.message.edit_text(
            f"🚨 <b>서버 탑재 붕괴 방어:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "back_to_main")
async def process_back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    print(f"💬 [TG 콜백 수신] back_to_main (User: {user.id})", flush=True)
    await state.clear()
    try:
        await callback_query.answer()
        await callback_query.message.edit_text(
            get_main_menu_text(), reply_markup=None, parse_mode="HTML"
        )
    except Exception:
        pass
