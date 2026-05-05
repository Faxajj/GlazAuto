"""Логика передачи смены: итоги в Обмены + хендовер в Офис."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from bot.config import (
    CHAT_EXCHANGE,
    CHAT_OFFICE,
    OP_DAY,
    OP_NIGHT,
)
from bot.state import (
    clear_all_pending_cvus,
    close_shift,
    get_all_pending_cvus,
    get_or_create_current_shift,
    get_shift_total,
)

logger = logging.getLogger(__name__)
MSK = timezone(timedelta(hours=3))


def _fmt_ars(value: float) -> str:
    """1234567.89 → '1,234,567.89' (англо-формат для пресетов)."""
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _fmt_ars_int(value: float) -> str:
    """1234567.89 → '1,234,568'"""
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "0"


def format_shift_summary(total_ars: float, total_txns: int,
                         bybit_rate: Optional[float],
                         shift_label: str) -> str:
    """Формат итогов смены для чата «Обмены»."""
    lines = [f"📊 Итог смены ({shift_label} МСК)", ""]
    if bybit_rate and bybit_rate > 0:
        denom = bybit_rate + 8
        usdt = total_ars / denom if denom > 0 else 0
        lines.append(f"{_fmt_ars_int(total_ars)}/({bybit_rate:.2f}+8)={usdt:,.2f}")
        lines.append("")
        lines.append(f"💰 Выведено: {_fmt_ars_int(total_ars)} ARS")
        lines.append(f"📈 Курс Bybit P2P: {bybit_rate:.2f} ARS/USDT")
        lines.append(f"💱 Итого: {usdt:,.2f} USDT")
    else:
        lines.append(f"💰 Выведено: {_fmt_ars_int(total_ars)} ARS")
        lines.append("📈 Курс Bybit P2P: недоступен")
    lines.append(f"🔄 Переводов: {total_txns}")
    return "\n".join(lines)


def format_shift_handover(pending_cvus: List[dict], next_operator: str) -> str:
    """Формат сообщения о передаче смены для чата «Офис»."""
    lines = [f"🔄 Передача смены → {next_operator}", ""]
    if not pending_cvus:
        lines.append("Все реквизиты закрыты ✅")
        return "\n".join(lines)
    for p in pending_cvus:
        name = (p.get("name") or "").strip()
        cvu  = (p.get("cvu") or "").strip()
        rem  = float(p.get("remaining") or 0)
        if name:
            lines.append(name)
        lines.append(cvu)
        lines.append(f"{_fmt_ars_int(rem)} осталось")
        lines.append("")
    return "\n".join(lines).rstrip()


def _next_operator_for_shift(now_msk: datetime) -> str:
    """Кто принимает следующую смену по МСК-времени окончания."""
    h = now_msk.hour
    # Сейчас закрывается дневная (07:30) → передаём ночному (для ночи)
    # Сейчас закрывается ночная (19:30) → передаём дневному
    if 7 <= h < 19:
        return OP_DAY      # сейчас день, к 19:30 передаём дневному (≈ team-lead принимает)
    return OP_NIGHT


async def end_shift(bot, shift_label: str, site_client) -> None:
    """Закрывает текущую смену:
       1. Подсчёт total_ars и total_txns
       2. Курс Bybit с сайта (если доступен)
       3. Сообщение в Обмены — итоги
       4. Сообщение в Офис — handover с тегом следующего оператора
       5. Закрываем смену в БД, чистим pending_cvus, открываем новую
    """
    try:
        shift_id = get_or_create_current_shift()
        total_ars, total_txns = get_shift_total(shift_id)
        bybit_rate = None
        try:
            bybit_rate = await site_client.get_bybit_rate()
        except Exception:
            pass

        # 1. В Обмены — итоги
        try:
            await bot.send_message(
                chat_id=CHAT_EXCHANGE,
                text=format_shift_summary(total_ars, total_txns, bybit_rate, shift_label),
            )
        except Exception as e:
            logger.warning("end_shift: failed to send summary: %s", e)

        # 2. В Офис — handover
        try:
            now_msk = datetime.now(MSK)
            next_op = _next_operator_for_shift(now_msk)
            pending = get_all_pending_cvus()
            await bot.send_message(
                chat_id=CHAT_OFFICE,
                text=format_shift_handover(pending, next_op),
            )
        except Exception as e:
            logger.warning("end_shift: failed to send handover: %s", e)

        # 3. Закрываем смену + чистим pending для новой
        close_shift(shift_id)
        clear_all_pending_cvus()
        # Следующая смена создастся лениво при первом get_or_create_current_shift()
        logger.info("end_shift: смена %d закрыта (total=%.0f ARS / %d txns)",
                    shift_id, total_ars, total_txns)
    except Exception as e:
        logger.exception("end_shift failed: %s", e)
