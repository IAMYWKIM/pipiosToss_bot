# =====================================================================
# FILE: tg_router.py
# 목적: 프리장 초중반 골든타임 (04:07 ~ 08:14 EST · 08:15 EST 거시지표 선제 컷오프)
#       양방향 대칭 클라이맥스 듀얼 스나이퍼(SOXS/SOXL), 단일 '하이킨 아시 + 고점 트레일링(HA_HYB_1.0)'
#       및 03:58 ~ 04:07 EST 듀얼 1주 무비용 휩소 매수덫 텔레그램 라우팅 컨트롤러
# =====================================================================
# MODIFIED: 제3헌법 및 Case 43 의거 구버전 익절률 선택 콜백(set_rate_SOXS_) 데드코드 영구 소각 및 8대 코어 모듈 프리플라이트 컴파일 검증 결속
# MODIFIED: 전술 제어반(/settlement) 설정 변경(전원, 복리모드, 통합시드) 시 SOXS/SOXL 양쪽 원장 동시 동기화(sync_global_settings) (Case 37, 76)
# MODIFIED: 통합 시드 설정 및 장부 초기화 안내 문구에 단일 '🔥 +1.00% 점화 하이킨 아시 트레일링' 및 04:07 ~ 08:14 EST 골든타임 기준 반영 (Case 53, 84)
# MODIFIED: 예산 입력 시 음수/0/NaN/Inf 유입으로 인한 장부 오염 방어 로직 유지 (Case 37)
# MODIFIED: /reset 명령을 통한 SOXS 및 SOXL 듀얼 원장 + 인메모리 트레일링 캐시 원자적 0점 소각 및 wakeup_event 즉각 동기화 (Case 02)
# MODIFIED: 모든 인라인 콜백 제자리 갱신(edit_text) 및 except Exception as e 문법 무결성 사수 (Case 17, 26)

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
from sniper_plugin import _reset_trailing_runtime_state
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
        msg = await message.answer("📡 <b>양방향 대칭 듀얼 스나이퍼 레이더 스캔 중...</b>", parse_mode="HTML")
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


# MODIFIED: 초과 Case 37, 84 — 단일 하이킨 아시 트레일링(HA_HYB_1.0) 및 04:07 ~ 08:14 EST 골든타임 기준 통합 시드 설정 안내문
@router.callback_query(F.data.startswith("set_budget_SOXS"))
async def process_set_budget(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.set_state(BudgetState.waiting_for_budget)
    is_comp = await AssassinLedger.get_is_compound("SOXS")

    mode_desc = (
        "📈 현재 <b>[통합 복리 모드 · 🔥 +1.00% 점화 HA·트레일링]</b>이 활성화되어 있습니다. 입력하신 통합 시드를 시작점으로 SOXS/SOXL 본진 트레일링 매매·수동 스나이퍼·1주 휩소 작전 등 모든 청산 시 <b>실제 발생한 순수익금(세전손익 - 왕복수수료 0.14%)</b>이 양쪽 장부 시드에 동시 가감됩니다."
        if is_comp
        else "💵 현재 <b>[단리(고정) 모드 · 🔥 +1.00% 점화 HA·트레일링]</b>이 활성화되어 있습니다. 본진 듀얼 스나이퍼 출동 시 입력하신 고정 달러 예산만 투입됩니다."
    )
    text = (
        "⌨️ <b>듀얼 스나이퍼(SOXS/SOXL) 통합 투입 시드 설정 (USD)</b>\n\n"
        "▫️ 04:07 ~ 08:14 EST 프리장 초중반 골든타임 양방향 대칭 클라이맥스 본진 스나이퍼 출동 시 투입할 통합 기준 달러 예산을 숫자로 전송하십시오.\n"
        "▫️ (참고: 03:58 ~ 04:07 EST 듀얼 휩소 노출 작전은 시드와 무관하게 무조건 1주만 역산 매수 덫을 설치합니다.)\n"
        f"▫️ {mode_desc}"
    )
    try:
        await callback_query.message.edit_text(text, parse_mode="HTML")
    except Exception as e:
        pass


@router.message(BudgetState.waiting_for_budget)
async def process_budget_input(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    try:
        budget = round(float(message.text.strip()), 2)
        # 0 이하 또는 비정상 부동소수점(NaN/Inf) 입력 원천 차단 (Case 37)
        if budget <= 0.0 or not math.isfinite(budget):
            raise ValueError("Budget must be a positive finite number.")
        await AssassinLedger.sync_global_settings(budget=budget)
        is_comp = await AssassinLedger.get_is_compound("SOXS")
        mode_tag = "📈 통합 복리 시작 시드" if is_comp else "💵 단리 고정 시드"
        if wakeup_event:
            wakeup_event.set()
        await state.clear()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 제어반으로", callback_data="open_settlement")]]
        )
        await message.answer(
            f"✅ <b>듀얼 스나이퍼(SOXS/SOXL) {mode_tag} <code>${budget:,.2f}</code> 동기화 락온 완료.</b>",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer("🚨 0보다 큰 유효한 숫자를 입력하세요.", parse_mode="HTML")


@router.message(Command("reset"))
async def cmd_reset(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ 듀얼 장부(SOXS/SOXL) 초기화", callback_data="execute_single_reset")],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = (
        "⚠️ <b>[듀얼 스나이퍼(SOXS/SOXL) 장부 초기화]</b>\n\n"
        "경고: SOXS 및 SOXL 로컬 장부(평단가, 점화가, 휩소 타점, 주문 ID, 당일 진입 잠금)와 "
        "인메모리 하이킨 아시 트레일링 캐시를 100% 영구 소각하고 0점으로 원자적 초기화를 수행합니다.\n"
        "(설정된 통합 시드 금액 및 복리/단리 모드는 그대로 보존됩니다.)\n진행하시겠습니까?"
    )
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass


@router.callback_query(F.data == "execute_single_reset")
async def process_execute_reset(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        await AssassinLedger.reset_all_ledgers()
        for sym in ("SOXS", "SOXL"):
            _reset_trailing_runtime_state(sym)
        if wakeup_event:
            wakeup_event.set()
        await callback_query.answer("✅ 듀얼 장부 0점 소각 완료", show_alert=True)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 메인 메뉴", callback_data="back_to_main")]]
        )
        await callback_query.message.edit_text(
            "✅ <b>[SOXS &amp; SOXL 듀얼 장부 영구 소각 완료]</b>\n"
            "▫️ 양쪽 로컬 장부 및 트레일링 캐시가 0점으로 원자적 초기화되었습니다 (04:07 ~ 08:14 EST 프리장 골든타임 내 양방향 대칭 클라이맥스 감시 즉시 복귀).",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            try:
                await callback_query.message.answer(
                    f"🚨 <b>초기화 실패:</b> {html.escape(str(e))}", parse_mode="HTML"
                )
            except Exception as ex:
                pass


@router.message(Command("update"))
async def cmd_update(message: types.Message, state: FSMContext):
    user = getattr(message, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 구글 클라우드 서버 탑재", callback_data="execute_update")],
            [InlineKeyboardButton(text="🔙 취소", callback_data="back_to_main")],
        ]
    )
    text = "⚠️ <b>[시스템 자가 업데이트]</b>\n\n최신 코드를 다운로드하고 재기동합니다. 진행하시겠습니까?"
    try:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        pass


@router.callback_query(F.data == "execute_update")
async def process_execute_update(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        await callback_query.message.edit_text("🔄 <b>GitHub 최신 코드 다운로드 중...</b>", parse_mode="HTML")

        def _run_git_update():
            import subprocess
            import py_compile

            def run_cmd(cmd):
                proc = subprocess.Popen(
                    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                try:
                    out, err = proc.communicate(timeout=30)
                    return proc.returncode, out.strip(), err.strip()
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate()
                    return -1, out.strip(), "Subprocess Timeout Expired (네트워크 교착 상태 차단)"

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
                return False, f"Hard Reset 붕괴:\n{reset_err}"
            try:
                for mod_file in (
                    "main.py",
                    "sniper_plugin.py",
                    "whipsaw_plugin.py",
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
                    "✅ <b>서버 탑재 완료</b>\n▫️ 최신 버전입니다.", parse_mode="HTML"
                )
            else:
                await callback_query.message.edit_text(
                    f"🚀 <b>탑재 성공. 재기동 격발.</b>\n<pre>{html.escape(msg)}</pre>",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1.0)
                os._exit(0)
        else:
            await callback_query.message.edit_text(
                f"🚨 <b>업데이트 실패 (롤백)</b>\n<pre>{html.escape(msg)}</pre>", parse_mode="HTML"
            )
    except Exception as e:
        await callback_query.message.edit_text(
            f"🚨 <b>서버 탑재 에러:</b> {html.escape(str(e))}", parse_mode="HTML"
        )


@router.callback_query(F.data == "back_to_main")
async def process_back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    user = getattr(callback_query, "from_user", None)
    if not user or getattr(user, "id", None) != ADMIN_CHAT_ID:
        return
    await state.clear()
    try:
        await callback_query.answer()
        text = await get_main_menu_text()
        await callback_query.message.edit_text(
            text, reply_markup=None, parse_mode="HTML"
        )
    except Exception as e:
        pass
