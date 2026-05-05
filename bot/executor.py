"""Логика выполнения выводов: дробление, выбор карт, написание в Офис, чеки."""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from bot.config import (
    AP_INTERNAL_PREFIX,
    CHAT_EXCHANGE,
    CHAT_OFFICE,
    DAILY_WITHDRAW_LIMIT,
    EXCHANGE_OPERATORS_TAGS,
    LIMIT_RESPONSE_TIMEOUT_SEC,
    PP_INTERNAL_PREFIX,
    WITHDRAW_CHUNK,
    WITHDRAW_PAUSE,
)
from bot.parser import CVU_PATTERN, ParsedItem
from bot.receipts import capture_receipt
from bot.site_client import SiteClient
from bot.state import add_pending_cvu, record_withdrawal, remove_pending_cvu

logger = logging.getLogger(__name__)


# Per-account locks — одна карта обрабатывает один вывод за раз
_account_locks: Dict[int, asyncio.Lock] = {}
_account_locks_meta = asyncio.Lock()


async def _get_account_lock(account_id: int) -> asyncio.Lock:
    async with _account_locks_meta:
        lock = _account_locks.get(account_id)
        if lock is None:
            lock = asyncio.Lock()
            _account_locks[account_id] = lock
        return lock


# In-flight: сколько ARS уже зарезервировано/в процессе для каждой карты.
# Защита от ситуации "8 параллельных CVU выбрали одну карту по stale-балансу".
_inflight_per_account: Dict[int, float] = {}
_inflight_lock = asyncio.Lock()


async def _reserve(account_id: int, amount: float) -> None:
    async with _inflight_lock:
        _inflight_per_account[account_id] = (
            _inflight_per_account.get(account_id, 0.0) + amount
        )


async def _release(account_id: int, amount: float) -> None:
    async with _inflight_lock:
        cur = _inflight_per_account.get(account_id, 0.0) - amount
        if cur <= 0:
            _inflight_per_account.pop(account_id, None)
        else:
            _inflight_per_account[account_id] = cur


async def _release_after_delay(account_id: int, amount: float,
                               delay_sec: float = 45) -> None:
    """Освобождает in-flight через delay_sec — за это время кеш баланса
    на сайте обновится и реальное списание уже отразится. Без этого
    in-flight копится бесконечно и блокирует последующие выводы."""
    try:
        await asyncio.sleep(delay_sec)
    except asyncio.CancelledError:
        pass
    await _release(account_id, amount)


def _get_inflight(account_id: int) -> float:
    return _inflight_per_account.get(account_id, 0.0)


# Pending-CVU operator response: message_id → asyncio.Future
pending_limit_responses: Dict[int, asyncio.Future] = {}


def _is_internal_pp_dest(cvu: str) -> bool:
    """PP/AP-internal CVU — на сайте exempt от 15-лимита."""
    s = (cvu or "").strip()
    return s.startswith(PP_INTERNAL_PREFIX) or s.startswith(AP_INTERNAL_PREFIX)


def _fmt_ars_int(v: float) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "0"


# ─────────────────────────────────────────────────────────────────────────────
# Card selection
# ─────────────────────────────────────────────────────────────────────────────

async def pick_best_card(client: SiteClient, amount: float,
                         destination: str = "",
                         exclude_ids: Optional[set] = None) -> Optional[dict]:
    """Берёт PP-карту с максимальным балансом >= amount и доступным лимитом."""
    exclude_ids = exclude_ids or set()
    try:
        accs = await client.get_pp_accounts()
    except Exception as e:
        logger.warning("pick_best_card: failed to list pp accounts: %s", e)
        return None

    is_internal = _is_internal_pp_dest(destination)
    candidates = []
    for a in accs:
        if a["id"] in exclude_ids:
            continue
        # Учитываем уже зарезервированные суммы in-flight на этой карте.
        # Без этого 8 параллельных задач выбрали бы одну карту по stale-кешу.
        effective = float(a["balance"]) - _get_inflight(a["id"])
        if effective < amount:
            continue
        # Лимит per-card применяется только к НЕ-internal направлениям
        if not is_internal:
            if a["account_withdraw_count"] >= DAILY_WITHDRAW_LIMIT:
                continue
        candidates.append((effective, a))

    if not candidates:
        return None
    # Сортировка по эффективному балансу (после вычета in-flight)
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# ─────────────────────────────────────────────────────────────────────────────
# Single-CVU executor
# ─────────────────────────────────────────────────────────────────────────────

async def execute_cvu(item: ParsedItem, bot, shift_id: int,
                      client: SiteClient,
                      auto_chunk: float = WITHDRAW_CHUNK) -> None:
    """Выполняет вывод для одного CVU: дробит на чанки, выбирает карты,
    шлёт чеки в Офис, обрабатывает лимиты."""
    remaining = float(item.amount)
    name      = item.name or "?"
    cvu       = item.cvu

    # Регистрируем как pending — на случай рестарта или передачи смены
    add_pending_cvu(cvu, name, remaining)

    chunks_done = 0
    used_card_ids: set = set()    # для исключения той же карты при limit-fallback
    no_card_msg_id: Optional[int] = None    # антиспам — одно "ожидающее" сообщение

    while remaining > 0:
        chunk_amount = min(auto_chunk, remaining)
        # Если оставшийся остаток меньше chunk — выводим его одним переводом
        if remaining < auto_chunk:
            chunk_amount = remaining

        card = await pick_best_card(client, chunk_amount, destination=cvu,
                                    exclude_ids=used_card_ids)

        # ── Нет подходящей карты ─────────────────────────────────────────────
        if card is None:
            import time as _t
            wait_text = (
                f"⏳ Жду пополнения карт\n"
                f"Реквизит: {name} ({cvu})\n"
                f"Остаток: {_fmt_ars_int(remaining)} ARS (нужно ≥ {_fmt_ars_int(chunk_amount)})\n"
                f"⏱ обновлено: {_t.strftime('%H:%M:%S')}"
            )
            # Первый раз — отправляем; дальше — РЕДАКТИРУЕМ то же сообщение
            if no_card_msg_id is None:
                try:
                    m = await bot.send_message(chat_id=CHAT_OFFICE, text=wait_text)
                    no_card_msg_id = m.message_id
                except Exception:
                    pass
            else:
                try:
                    await bot.edit_message_text(
                        chat_id=CHAT_OFFICE,
                        message_id=no_card_msg_id,
                        text=wait_text,
                    )
                except Exception:
                    pass    # если Telegram ругается на "message not modified" — игнор
            await asyncio.sleep(60)    # ждём минуту, пробуем снова
            used_card_ids.clear()       # на следующем круге включаем все обратно
            continue

        # Карта появилась — стираем "ожидающее" сообщение
        if no_card_msg_id is not None:
            try:
                await bot.delete_message(chat_id=CHAT_OFFICE, message_id=no_card_msg_id)
            except Exception:
                pass
            no_card_msg_id = None

        # ── Резервируем сумму на этой карте ──────────────────────────────────
        await _reserve(card["id"], chunk_amount)

        # ── Выполняем вывод ─────────────────────────────────────────────────
        lock = await _get_account_lock(card["id"])
        async with lock:
            res = await client.withdraw(card["id"], cvu, chunk_amount)

        if res.get("ok"):
            tid = res.get("tid")

            # Если сайт не вернул tid в Location — пытаемся найти его через
            # /api/account/{id}/recent-attempts (свежий SUCCESS с тем же amount).
            if not tid:
                try:
                    tid = await client.find_recent_tid(
                        account_id=card["id"],
                        amount=chunk_amount,
                        destination=cvu,
                        max_age_sec=120,
                    )
                    if tid:
                        logger.info("recovered tid=%s via recent-attempts", tid)
                except Exception as e:
                    logger.warning("find_recent_tid failed: %s", e)

            # ── Polling реального статуса транзакции ────────────────────────
            # `ok=True` означает только "транзакция создана сайтом".
            # Банк может асинхронно вернуть rejected (rechazada) — нужен poll.
            final_status = "unknown"
            if tid:
                # 6 попыток × 5 сек = 30 сек ожидания финального статуса
                for _ in range(6):
                    await asyncio.sleep(5)
                    try:
                        st = await client.get_transaction_status(card["id"], tid)
                    except Exception:
                        st = "unknown"
                    if st in ("rejected", "approved"):
                        final_status = st
                        break
                    # pending / unknown — продолжаем ждать
                else:
                    # Не дождались — оставляем pending, на сайте reconciliation worker доразберётся
                    final_status = "pending"
            else:
                # Нет tid — нет способа проверить, считаем approved оптимистично
                final_status = "approved"

            # ── REJECTED ────────────────────────────────────────────────────
            if final_status == "rejected":
                await _release(card["id"], chunk_amount)
                # Записываем в state как rejected (не списывает remaining)
                record_withdrawal(
                    shift_id=shift_id, cvu=cvu, name=name,
                    amount=chunk_amount, account_id=card["id"],
                    account_label=card.get("label") or "",
                    transaction_id=tid, status="rejected",
                )
                used_card_ids.add(card["id"])     # больше не выбираем эту карту
                try:
                    await bot.send_message(
                        chat_id=CHAT_OFFICE,
                        text=(f"❌ Банк отклонил перевод\n"
                              f"💸 {name} ({cvu})\n"
                              f"💵 Сумма: {_fmt_ars_int(chunk_amount)} ARS\n"
                              f"📱 Карта: {card.get('label') or '?'}\n"
                              f"🔁 Пробую другую карту..."),
                    )
                except Exception:
                    pass
                # remaining НЕ уменьшаем — переходим на следующий круг while
                continue

            # ── APPROVED (или PENDING — оптимистично считаем успехом) ───────
            chunks_done += 1
            remaining -= chunk_amount
            # In-flight освобождаем через 45 сек — за это время кеш баланса
            # на сайте обновится и реальное списание учтётся. Если освободить
            # сразу — другая задача увидит stale-баланс с непросевшей суммой
            # и попытается списать ещё раз. Если не освобождать вообще —
            # in-flight накапливается и блокирует все последующие выводы.
            asyncio.create_task(_release_after_delay(card["id"], chunk_amount, 45))

            # Запись в state
            record_withdrawal(
                shift_id=shift_id, cvu=cvu, name=name,
                amount=chunk_amount, account_id=card["id"],
                account_label=card.get("label") or "",
                transaction_id=tid,
                status="success" if final_status == "approved" else "pending",
            )

            # Pending CVU обновляем (или удаляем если закрыт)
            if remaining > 0:
                add_pending_cvu(cvu, name, remaining)
            else:
                remove_pending_cvu(cvu)

            # Сообщение об успехе
            status_emoji = "✅" if final_status == "approved" else "⏳"
            status_text  = "Отправлено" if final_status == "approved" else "В обработке"
            try:
                await bot.send_message(
                    chat_id=CHAT_OFFICE,
                    text=(f"💸 {name} ({cvu})\n"
                          f"{status_emoji} {status_text}: {_fmt_ars_int(chunk_amount)} ARS\n"
                          f"📱 Карта: {card.get('label') or '?'}\n"
                          f"💰 Осталось: {_fmt_ars_int(remaining)} ARS"),
                )
            except Exception as e:
                logger.warning("send chunk message failed: %s", e)

            # Скриншот чека
            if tid:
                png = None
                try:
                    png = await capture_receipt(card["id"], tid,
                                                client.session_token,
                                                client.csrf_token)
                except Exception as e:
                    logger.warning("capture_receipt threw: %s", e)

                if png:
                    try:
                        from io import BytesIO
                        await bot.send_photo(
                            chat_id=CHAT_OFFICE,
                            photo=BytesIO(png),
                            caption=f"📄 Чек: {name} → {_fmt_ars_int(chunk_amount)} ARS",
                        )
                    except Exception as e:
                        logger.warning("send receipt photo failed: %s", e)
                else:
                    # Fallback: playwright не работает / chromium не установлен —
                    # шлём текстовую ссылку на чек, оператор откроет на сайте.
                    receipt_url = client.get_receipt_url(card["id"], tid)
                    logger.info("no receipt screenshot for tid=%s — sending link", tid)
                    try:
                        await bot.send_message(
                            chat_id=CHAT_OFFICE,
                            text=(f"📄 Чек: {name} → {_fmt_ars_int(chunk_amount)} ARS\n"
                                  f"🔗 {receipt_url}\n"
                                  f"(скриншот недоступен — playwright/chromium не установлен)"),
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.warning("send receipt link failed: %s", e)

            # Закрыт полностью?
            if remaining <= 0:
                try:
                    await bot.send_message(
                        chat_id=CHAT_OFFICE,
                        text=(f"✅ Закрыт: {name} ({cvu})\n"
                              f"Итого: {_fmt_ars_int(item.amount)} ARS "
                              f"({chunks_done} переводов)"),
                    )
                except Exception:
                    pass
                break

            # Пауза между чанками — защита от 60-сек idempotency window
            await asyncio.sleep(WITHDRAW_PAUSE)
            continue

        # ── Ошибка вывода ───────────────────────────────────────────────────
        # Сайт не создал транзакцию — освобождаем резервирование сразу.
        await _release(card["id"], chunk_amount)
        err = res.get("error") or "unknown"
        err_low = err.lower()

        # Лимит карты — спрашиваем у оператора другой CVU
        if "account_limit" in err_low or "limit_reached" in err_low:
            used_card_ids.add(card["id"])
            try:
                msg = await bot.send_message(
                    chat_id=CHAT_OFFICE,
                    text=(f"⚠️ Карта {card.get('label') or '?'} достигла лимита "
                          f"({DAILY_WITHDRAW_LIMIT} выводов)\n"
                          f"Реквизит: {name} ({cvu})\n"
                          f"Остаток для вывода: {_fmt_ars_int(remaining)} ARS\n"
                          f"Дайте CVU куда перевести остаток ↓ "
                          f"(reply на это сообщение)"),
                )
            except Exception:
                msg = None

            # Ждём ответ оператора
            if msg:
                fut: asyncio.Future = asyncio.get_event_loop().create_future()
                pending_limit_responses[msg.message_id] = fut
                try:
                    new_cvu = await asyncio.wait_for(fut, timeout=LIMIT_RESPONSE_TIMEOUT_SEC)
                    cvu = new_cvu      # переключаемся на новый CVU
                    used_card_ids.clear()
                    continue
                except asyncio.TimeoutError:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_OFFICE,
                            text=(f"⏱ Не получил CVU от оператора за 30 минут.\n"
                                  f"Остаток {name}: {_fmt_ars_int(remaining)} ARS — "
                                  f"оставлен в pending. Передам со сменой."),
                        )
                    except Exception:
                        pass
                    break
                finally:
                    pending_limit_responses.pop(msg.message_id, None)
            else:
                break

        # Idempotency-окно: ждём минуту, повторяем
        if "retry_after_minute" in err_low or "withdraw_in_progress" in err_low:
            await asyncio.sleep(70)
            continue

        # Bank-rejected — логируем + переходим к следующему чанку с другой картой
        if "rejected" in err_low or "rechazad" in err_low:
            try:
                await bot.send_message(
                    chat_id=CHAT_OFFICE,
                    text=(f"❌ Банк отклонил вывод {_fmt_ars_int(chunk_amount)} ARS\n"
                          f"Реквизит: {name} ({cvu})\n"
                          f"Причина: {err}\n"
                          f"Пропускаю этот чанк."),
                )
            except Exception:
                pass
            remaining -= chunk_amount
            if remaining <= 0:
                remove_pending_cvu(cvu)
                break
            continue

        # Любая другая ошибка — логируем, ждём, ретраим с другой картой
        used_card_ids.add(card["id"])
        try:
            await bot.send_message(
                chat_id=CHAT_OFFICE,
                text=(f"⚠ Ошибка вывода: {err[:200]}\n"
                      f"{name} ({cvu}) — пробую другую карту..."),
            )
        except Exception:
            pass
        await asyncio.sleep(15)


# ─────────────────────────────────────────────────────────────────────────────
# Multi-CVU orchestration
# ─────────────────────────────────────────────────────────────────────────────

async def process_items(items: List[ParsedItem], bot, shift_id: int,
                        client: SiteClient) -> None:
    """Запускает execute_cvu параллельно для каждого item.
    Разные CVU обрабатываются параллельно; на одной карте — last-write-wins
    через _account_locks. После завершения всех CVU тегает операторов
    в "Обмены" с просьбой следующих реквизитов."""
    if not items:
        return
    tasks = [execute_cvu(item, bot, shift_id, client) for item in items]
    await asyncio.gather(*tasks, return_exceptions=True)

    # После закрытия батча — просим следующие реквизиты в "Обмены"
    try:
        tags = " ".join(EXCHANGE_OPERATORS_TAGS)
        total_ars = sum(float(it.amount) for it in items)
        n = len(items)
        await bot.send_message(
            chat_id=CHAT_EXCHANGE,
            text=(f"✅ Закрыто {n} реквизит(ов) на {_fmt_ars_int(total_ars)} ARS.\n"
                  f"{tags} — давайте следующие реквизиты 🙏"),
        )
    except Exception as e:
        logger.warning("process_items: failed to ping for next requisites: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Helper для main.py: оператор прислал CVU в ответ на limit-сообщение
# ─────────────────────────────────────────────────────────────────────────────

def try_resolve_pending_limit(reply_to_msg_id: int, text: str) -> bool:
    """Если text содержит CVU и в pending_limit_responses есть future
    с этим message_id — резолвим future. Returns True если сматчили."""
    fut = pending_limit_responses.get(reply_to_msg_id)
    if fut is None or fut.done():
        return False
    m = CVU_PATTERN.search(text or "")
    if not m:
        return False
    fut.set_result(m.group(1))
    return True
