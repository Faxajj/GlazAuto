"""Точка входа Anton-бота: handlers, scheduler, авторизация на сайте."""
from __future__ import annotations

import asyncio
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Dict

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot.config import (
    ALLOWED_SENDERS,
    AUTO_CONFIRM_AFTER_SEC,
    BOT_TOKEN,
    CHAT_EXCHANGE,
    CHAT_OFFICE,
    LOG_DIR,
    TELEGRAM_PROXY,
)
from bot.executor import (
    pending_limit_responses,
    process_items,
    try_resolve_pending_limit,
)
from bot.parser import ParseResult, parse_message
from bot.shift import end_shift
from bot.site_client import SiteClient
from bot.state import get_or_create_current_shift, init_state_db

logger = logging.getLogger("anton")

# Глобальный SiteClient — переиспользуется между запросами
site_client: SiteClient = SiteClient()

# Pending: message_id → ParseResult (предложение выводов с кнопками ✅/❌)
pending_confirmations: Dict[int, ParseResult] = {}

# Auto-confirm timers по message_id
_auto_confirm_tasks: Dict[int, asyncio.Task] = {}

# Callback dedup: при медленном answerCallbackQuery Telegram повторяет update.
# Игнорируем повторные клики на тот же message_id.
_callback_in_progress: set = set()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    handler = TimedRotatingFileHandler(
        os.path.join(LOG_DIR, "anton.log"),
        when="midnight", interval=1, backupCount=30, encoding="utf-8",
    )
    handler.suffix = "%Y-%m-%d"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(fmt)
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Удаляем старые хендлеры (на случай горячей перезагрузки)
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.addHandler(stream)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_ars_int(v: float) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "0"


def _build_preview_text(parsed: ParseResult, sender_name: str) -> str:
    lines = [f"📋 Новые реквизиты (от {sender_name})", ""]
    if parsed.ambiguous:
        lines.insert(1, "⚠️ Не уверен в суммах, проверь:")
        lines.append("")
    total_ars = 0.0
    for i, it in enumerate(parsed.items, 1):
        rem = " 🟡(остаток)" if it.is_remainder else ""
        name_part = f"{it.name} " if it.name else ""
        lines.append(f"{i}. {name_part}({it.cvu}) → {_fmt_ars_int(it.amount)} ARS{rem}")
        total_ars += it.amount
    lines.append("")
    lines.append(f"💰 Итого: {_fmt_ars_int(total_ars)} ARS")
    if total_ars <= 1_000_000:
        lines.append("🔄 1 перевод")
    else:
        chunks = max(1, int((total_ars + 999_999) // 1_000_000))
        lines.append(f"🔄 ~{chunks} переводов (по ≤1,000,000)")
    return "\n".join(lines)


def _confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Выполнить", callback_data="confirm:ok"),
        InlineKeyboardButton("❌ Отменить",  callback_data="confirm:cancel"),
    ]])


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────

async def on_exchange_message(update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сообщение в чате «Обмены» от разрешённого автора."""
    msg = update.effective_message
    if msg is None or not msg.text:
        return
    user = msg.from_user
    if not user or user.id not in ALLOWED_SENDERS:
        return

    parsed = parse_message(msg.text)
    if parsed is None or not parsed.items:
        # Молча игнорируем — это не реквизиты
        return

    sender = f"@{user.username}" if user.username else user.full_name
    text = _build_preview_text(parsed, sender)

    try:
        preview = await context.bot.send_message(
            chat_id=CHAT_OFFICE,
            text=text,
            reply_markup=_confirm_keyboard(),
        )
    except Exception as e:
        logger.exception("on_exchange_message: send preview failed: %s", e)
        return

    pending_confirmations[preview.message_id] = parsed

    # Auto-confirm если не ambiguous
    if not parsed.ambiguous:
        async def _auto():
            try:
                await asyncio.sleep(AUTO_CONFIRM_AFTER_SEC)
                if preview.message_id in pending_confirmations:
                    parsed_now = pending_confirmations.pop(preview.message_id)
                    try:
                        await context.bot.edit_message_text(
                            chat_id=CHAT_OFFICE,
                            message_id=preview.message_id,
                            text=text + "\n\n⏱ Auto-confirm (2 мин)",
                        )
                    except Exception:
                        pass
                    shift_id = get_or_create_current_shift()
                    await process_items(parsed_now.items, context.bot, shift_id, site_client)
            except Exception as e:
                logger.exception("auto-confirm failed: %s", e)

        task = asyncio.create_task(_auto())
        _auto_confirm_tasks[preview.message_id] = task


async def on_office_message(update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сообщение в чате «Офис»:
    1) reply на бот-запрос CVU (try_resolve_pending_limit);
    2) reply на preview-сообщение с реквизитами → переразбираем и обновляем pending.
    """
    msg = update.effective_message
    if msg is None or not msg.text:
        return
    if not msg.reply_to_message:
        return
    reply_to_id = msg.reply_to_message.message_id

    # 1) Попытка резолвнуть pending CVU (вывод остатка после "лимит карты")
    if try_resolve_pending_limit(reply_to_id, msg.text):
        try:
            await msg.reply_text("✅ CVU принят, продолжаю вывод остатка...")
        except Exception:
            pass
        return

    # 2) Это reply на preview-сообщение с реквизитами? → редактирование
    if reply_to_id in pending_confirmations:
        new_parsed = parse_message(msg.text)
        if new_parsed is None or not new_parsed.items:
            try:
                await msg.reply_text(
                    "⚠ Не удалось распарсить новые реквизиты. "
                    "Пришли формат как обычно (CVU + сумма)."
                )
            except Exception:
                pass
            return

        # Отменяем auto-confirm — после правки оператор должен явно подтвердить
        t = _auto_confirm_tasks.pop(reply_to_id, None)
        if t and not t.done():
            t.cancel()

        pending_confirmations[reply_to_id] = new_parsed
        sender = "оператор (правка)"
        new_text = _build_preview_text(new_parsed, sender)
        try:
            await context.bot.edit_message_text(
                chat_id=CHAT_OFFICE,
                message_id=reply_to_id,
                text=new_text + "\n\n✏️ Отредактировано — нажмите ✅ для запуска.",
                reply_markup=_confirm_keyboard(),
            )
        except Exception as e:
            logger.warning("edit preview failed: %s", e)
            try:
                await msg.reply_text("⚠ Не смог отредактировать preview, попробуй ещё раз.")
            except Exception:
                pass
            return
        try:
            await msg.reply_text("✅ Реквизиты обновлены, нажми ✅ для запуска.")
        except Exception:
            pass


async def on_callback_query(update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки ✅ / ❌ под preview-сообщением."""
    cq = update.callback_query
    if cq is None or not cq.data:
        return
    msg = cq.message
    if msg is None:
        return
    # ── Дедуп ───────────────────────────────────────────────────────────────
    # Telegram повторяет callback update если answerCallbackQuery приходит
    # с задержкой > 15 сек (медленный SOCKS-прокси). Без дедупа handler
    # выполнится дважды и второй раз увидит pending=None → "сессия истекла".
    if msg.message_id in _callback_in_progress:
        try:
            await cq.answer("Уже обрабатываю...")
        except Exception:
            pass
        return
    _callback_in_progress.add(msg.message_id)
    # Подстраховка — даже если handler упал/ушёл в timeout, через 60 сек
    # message_id освободится в dedup-сете (повторный клик возможен снова).
    asyncio.get_event_loop().call_later(
        60, lambda mid=msg.message_id: _callback_in_progress.discard(mid),
    )

    # cq.answer() имеет таймаут 15 сек на стороне Telegram.
    # При медленном SOCKS-прокси ack может прийти позже — Telegram возвращает
    # BadRequest "Query is too old". Это НЕ должно блокировать обработку.
    try:
        await cq.answer()
    except Exception as e:
        logger.warning("callback_query.answer() failed (продолжаем): %s", e)
    data = cq.data
    parsed = pending_confirmations.pop(msg.message_id, None)
    # Отменяем auto-confirm task если был
    t = _auto_confirm_tasks.pop(msg.message_id, None)
    if t and not t.done():
        t.cancel()

    if data == "confirm:cancel":
        try:
            await context.bot.edit_message_text(
                chat_id=msg.chat_id, message_id=msg.message_id,
                text=msg.text_markdown_v2_urled if False else (msg.text or "") + "\n\n❌ Отменено оператором",
            )
        except Exception:
            try:
                await context.bot.edit_message_reply_markup(
                    chat_id=msg.chat_id, message_id=msg.message_id, reply_markup=None,
                )
            except Exception:
                pass
        return

    if data == "confirm:ok":
        if parsed is None:
            try:
                await cq.message.reply_text("⚠ Сессия истекла, пришли реквизиты заново.")
            except Exception:
                pass
            return
        try:
            await context.bot.edit_message_text(
                chat_id=msg.chat_id, message_id=msg.message_id,
                text=(msg.text or "") + "\n\n✅ Принято в работу",
            )
        except Exception:
            try:
                await context.bot.edit_message_reply_markup(
                    chat_id=msg.chat_id, message_id=msg.message_id, reply_markup=None,
                )
            except Exception:
                pass
        shift_id = get_or_create_current_shift()
        # Запускаем выводы в фоне — не блокируем callback
        asyncio.create_task(process_items(parsed.items, context.bot, shift_id, site_client))


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler — конец смены 07:30 / 19:30 МСК
# ─────────────────────────────────────────────────────────────────────────────

def _setup_scheduler(bot) -> AsyncIOScheduler:
    msk = pytz.timezone("Europe/Moscow")
    scheduler = AsyncIOScheduler(timezone=msk)

    def _job_morning():
        asyncio.create_task(end_shift(bot, "07:30", site_client))

    def _job_evening():
        asyncio.create_task(end_shift(bot, "19:30", site_client))

    scheduler.add_job(_job_morning, "cron", hour=7,  minute=30)
    scheduler.add_job(_job_evening, "cron", hour=19, minute=30)
    return scheduler


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _on_startup(app) -> None:
    init_state_db()
    try:
        await site_client.ensure_logged_in()
        logger.info("startup: site login OK")
    except Exception as e:
        logger.error("startup: site login failed: %s — продолжаем, попробуем позже", e)
    scheduler = _setup_scheduler(app.bot)
    scheduler.start()
    app.bot_data["scheduler"] = scheduler


async def _on_shutdown(app) -> None:
    scheduler = app.bot_data.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)
    await site_client.close()


def main() -> None:
    setup_logging()
    logger.info("anton-bot: запуск")

    builder = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(_on_startup)
        .post_shutdown(_on_shutdown)
    )

    # Прокси для Telegram API (если api.telegram.org заблокирован у хостера).
    if TELEGRAM_PROXY:
        # httpx 0.25 (необходимая для python-telegram-bot 20.7) не поддерживает
        # схему socks5h:// (с DNS-resolution через прокси). Нормализуем в socks5://
        # — для нашего use-case (api.telegram.org с фиксированным IP) разницы нет.
        proxy_url = TELEGRAM_PROXY
        if proxy_url.startswith("socks5h://"):
            proxy_url = "socks5://" + proxy_url[len("socks5h://"):]

        masked = proxy_url
        try:
            from urllib.parse import urlparse, urlunparse
            u = urlparse(proxy_url)
            if u.password:
                masked = urlunparse(u._replace(
                    netloc=f"{u.username}:***@{u.hostname}:{u.port}"
                ))
        except Exception:
            pass
        logger.info("anton-bot: используем прокси для Telegram API: %s", masked)

        from telegram.request import HTTPXRequest
        # python-telegram-bot 20.7 — параметр называется `proxy` (новое имя),
        # `proxy_url` deprecated. Внутри передаётся как proxies= в httpx 0.25.
        # Один HTTPXRequest для обычных вызовов, второй для long-polling getUpdates.
        try:
            req         = HTTPXRequest(connection_pool_size=8, proxy=proxy_url)
            updates_req = HTTPXRequest(connection_pool_size=8, proxy=proxy_url,
                                       read_timeout=40, connect_timeout=20)
        except TypeError:
            # Fallback для 20.x где proxy ещё не существует
            req         = HTTPXRequest(connection_pool_size=8, proxy_url=proxy_url)
            updates_req = HTTPXRequest(connection_pool_size=8, proxy_url=proxy_url,
                                       read_timeout=40, connect_timeout=20)
        builder = builder.request(req).get_updates_request(updates_req)

    app = builder.build()

    app.add_handler(MessageHandler(
        filters.TEXT & filters.Chat(CHAT_EXCHANGE), on_exchange_message,
    ))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Chat(CHAT_OFFICE), on_office_message,
    ))
    app.add_handler(CallbackQueryHandler(on_callback_query))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
