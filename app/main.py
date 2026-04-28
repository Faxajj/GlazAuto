"""
Banks Dashboard — единый дашборд для нескольких банковских аккаунтов.
"""
import asyncio
import base64
import json
import logging
import secrets
import statistics
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from urllib.parse import quote

import httpx

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from app.database import (
    DAILY_WITHDRAW_LIMIT,
    get_window_list,
    normalize_window_slug,
    accounts_by_window,
    add_account as db_add_account,
    add_auto_withdraw_rule,
    cleanup_sessions,
    create_session,
    create_user,
    delete_account as db_delete_account,
    delete_auto_withdraw_rule,
    delete_session,
    get_account,
    get_auto_withdraw_rule,
    get_session,
    get_user_by_username,
    get_account_withdraw_count,
    get_withdraw_count,
    increment_account_withdraw_count,
    increment_withdraw_count,
    init_db,
    is_account_withdraw_limit_reached,
    is_withdraw_limit_reached,
    list_accounts,
    list_auto_withdraw_rules,
    update_account as db_update_account,
    update_auto_withdraw_progress,
    verify_password,
    add_window as db_add_window,
    update_window as db_update_window,
    delete_window as db_delete_window,
    window_exists,
    save_rate_point,
    get_rate_history,
    cleanup_rate_history,
)
from app.drivers import (
    BANK_TYPES,
    create_withdraw as driver_withdraw,
    discover_beneficiary,
    get_balance as driver_balance,
)
from app.drivers.personalpay import (
    get_activities_list as pp_activities_list,
    get_transference_details as pp_transference_details,
    consume_refreshed_token as pp_consume_refreshed_token,
    _DEFAULT_PIN_HASH as PP_DEFAULT_PIN_HASH,
)

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

SESSION_COOKIE = "session_token"
SESSION_TTL    = 60 * 60 * 8   # 8 часов

CONCEPTS_UC = [
    ("VARIOS",    "VARIOS (разное)"),
    ("ALQUILER",  "ALQUILER (аренда)"),
    ("HONORARIOS","HONORARIOS (гонорары)"),
    ("COMPRA",    "COMPRA (покупка)"),
    ("VENTA",     "VENTA (продажа)"),
]

# Человекочитаемые сообщения об ошибках
ERROR_MESSAGES: dict[str, str] = {
    "rejected_by_bank": "❌ Перевод отклонён банком. Проверьте данные получателя или попробуйте позже.",
    "invalid_amount": "❌ Неверная сумма. Введите число больше нуля.",
    "no_destination": "❌ Укажите CVU или псевдоним получателя.",
    "document_required": "❌ Документ получателя (CUIT/CUIL) обязателен для UniversalCoins.",
    "invalid_auto_rule": "❌ Неверные параметры автоправила. Проверьте суммы.",
    "invalid_amount_rule": "❌ Неверная сумма в правиле автовывода.",
    "limit_reached": f"❌ Достигнут дневной лимит ({DAILY_WITHDRAW_LIMIT} выводов). Лимит обновляется ежедневно с 09:30 до 10:00 МСК.",
    "account_limit_reached": f"❌ Карта достигла лимита выводов ({DAILY_WITHDRAW_LIMIT} в день). Попробуйте после обновления лимитов с 09:30 до 10:00 МСК.",
    "token_expired": "❌ Сессия банка истекла. Обновите токен в настройках карты.",
    "bank_unavailable": "❌ Банк временно недоступен. Попробуйте повторить через 1–2 минуты.",
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("banks")

# ---------------------------------------------------------------------------
# FastAPI + шаблоны
# ---------------------------------------------------------------------------

app = FastAPI(title="Banks Dashboard")

init_db()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["ars"]    = lambda value, decimals=2: _format_ars(value, decimals)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Расширяемый список CVU-префиксов, освобождённых от лимита выводов.
# Логика: переводы между картами одного банка (PP→PP, AP→AP) — внутренние.
# Чтобы добавить новый банк — просто впишите его CVU-префикс в этот frozenset.
# ---------------------------------------------------------------------------
_EXEMPT_CVU_PREFIXES: frozenset = frozenset({
    "00000765",  # Personal Pay  (внутренние карты PP)
    "00001775",  # AstroPay      (внутренние карты AP)
})


def _is_limit_exempt(destination: str) -> bool:
    """Возвращает True если CVU получателя освобождён от лимита 15 выводов.
    Охватывает переводы между картами одного банка (PP→PP, AP→AP)."""
    dest = (destination or "").strip()
    return any(dest.startswith(prefix) for prefix in _EXEMPT_CVU_PREFIXES)


# Обратная совместимость — старое имя функции оставлено как псевдоним.
def _is_pp_internal_cvu(destination: str) -> bool:
    return _is_limit_exempt(destination)


def _format_ars(value, decimals: int = 2) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "0"
    if decimals == 0:
        return f"{num:,.0f}".replace(",", ".")
    return f"{num:,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    session = get_session(token)
    if not session:
        return None
    if session["exp"] < int(time.time()):
        delete_session(token)
        return None
    return session


def _window_name(slug: str) -> str:
    return next((name for s, name in get_window_list() if s == slug), slug)


def _parse_amount(raw: str) -> Optional[float]:
    """'1.234.567,89' или '1234567.89' -> float. None если не парсится."""
    s = (raw or "").strip().replace(" ", "")
    # Если точек несколько — они разделители тысяч
    if s.count(".") > 1:
        s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _jwt_expiry(credentials: dict) -> Tuple[bool, Optional[float]]:
    token = (credentials.get("auth_token") or "").strip()
    if token.upper().startswith("BEARER "):
        token = token[7:].strip()
    if not token or not token.startswith("eyJ"):
        return False, None
    parts = token.split(".")
    if len(parts) < 2:
        return False, None
    try:
        pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
    except Exception:
        return False, None
    exp = payload.get("exp")
    if not exp:
        return False, None
    now = time.time()
    return (now >= exp), max(0.0, (exp - now) / 3600.0)


def _normalize_balance(balance_info: dict, bank_type: str) -> dict:
    """Унифицирует ответ driver_balance в {balance, cvu_number, cvu_alias}."""
    if bank_type == "personalpay":
        raw_accounts = balance_info.get("raw_accounts")
    else:
        raw_accounts = None
    result = {
        "balance":     balance_info.get("balance") or 0,
        "cvu_number":  balance_info.get("cvu_number") or "",
        "cvu_alias":   balance_info.get("cvu_alias") or "",
        "raw_accounts": raw_accounts,
    }
    try:
        result["balance"] = float(result["balance"])
    except (TypeError, ValueError):
        result["balance"] = 0.0
    return result


def _error_text(code: str) -> str:
    return ERROR_MESSAGES.get(code, f"❌ Ошибка: {code}")


def _error_payload(code: str, details: str = "", suggestion: str = "") -> dict:
    return {
        "code": code,
        "message": _error_text(code),
        "details": details,
        "suggestion": suggestion,
    }


# ---------------------------------------------------------------------------
# Парсинг операций PersonalPay
# ---------------------------------------------------------------------------

def _extract_activities_raw(data) -> list:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("data", "activities", "items", "content", "results", "list", "records"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("activities", "items", "list", "content", "results"):
            val = inner.get(key)
            if isinstance(val, list):
                return val
        if isinstance(inner.get("data"), list):
            return inner["data"]
    if isinstance(data.get("included"), list):
        return data["included"]
    return []


def _find_any_in_dict(obj, keys: tuple) -> Optional[object]:
    def search(o, depth):
        if depth <= 0:
            return None
        if isinstance(o, dict):
            for k in keys:
                v = o.get(k)
                if v is not None and v != "":
                    return v
            for v in o.values():
                r = search(v, depth - 1)
                if r is not None:
                    return r
        elif isinstance(o, list):
            for item in o:
                r = search(item, depth - 1)
                if r is not None:
                    return r
        return None
    return search(obj, 5)


def _find_in_dict(obj, *keys: str, want_number: bool = False):
    def search(o, depth):
        if depth <= 0:
            return None
        if isinstance(o, dict):
            for k in keys:
                v = o.get(k)
                if v is None:
                    continue
                if want_number:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
                elif isinstance(v, str) and v.strip():
                    return v.strip()
            for v in o.values():
                r = search(v, depth - 1)
                if r is not None:
                    return r
        elif isinstance(o, list):
            for item in o:
                r = search(item, depth - 1)
                if r is not None:
                    return r
        return None
    return search(obj, 5)


def _find_in_details(act: dict, *labels: str) -> Optional[str]:
    if not isinstance(act, dict):
        return None
    details = act.get("details") or act.get("items") or act.get("attributes", {}).get("details") or []
    if not isinstance(details, list):
        return None
    labels_lower = [l.lower() for l in labels]
    for d in details:
        if not isinstance(d, dict):
            continue
        label = (d.get("label") or d.get("key") or "").strip().lower()
        if label in labels_lower:
            val = d.get("value") or d.get("displayValue") or d.get("name")
            if val and str(val).strip():
                return str(val).strip()
    return None


def _get_nested(obj: dict, *path: str) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    for key in path:
        obj = obj.get(key) if isinstance(obj, dict) else None
        if obj is None:
            return None
    s = str(obj).strip() if obj is not None else ""
    return s if s else None


def _join_name(*parts) -> Optional[str]:
    """Объединяет части имени (имя, фамилия), убирает пустые."""
    result = " ".join(p.strip() for p in parts if p and str(p).strip())
    return result if result else None


def _find_32char_hex_id(obj) -> Optional[str]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and "-" not in v and len(v) == 32 and all(
                c in "0123456789ABCDEFabcdef" for c in v
            ):
                return v
            found = _find_32char_hex_id(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_32char_hex_id(item)
            if found:
                return found
    return None


def _normalize_astropay_activity(act: dict) -> dict:
    """Нормализует запись активности в формате AstroPay v3/activities."""
    aid   = act.get("activity_id") or act.get("reference_id") or act.get("id")
    title = (act.get("title") or act.get("description") or str(aid or "Операция")).strip()

    # Сумма и направление
    main_amount = act.get("main_amount") or {}
    amount_raw  = main_amount.get("amount")
    amount      = None
    is_outgoing = False
    if amount_raw is not None:
        try:
            fval        = float(amount_raw)
            amount      = abs(fval)
            is_outgoing = fval < 0
        except (TypeError, ValueError):
            pass
    else:
        # Резервный вариант по иконке ресурса
        icon        = (act.get("resource") or {}).get("value") or ""
        is_outgoing = "up" in icon.lower()

    # Дата (миллисекунды)
    date_raw = act.get("date")
    date_str = None
    if date_raw:
        try:
            ts       = int(date_raw)
            date_str = datetime.utcfromtimestamp(
                ts / 1000 if ts > 1e12 else ts
            ).strftime("%d.%m.%Y %H:%M")
        except Exception:
            date_str = str(date_raw)

    # receipt_id — только из VIEW_RECEIPT action (настоящий reference_id для /invoice/...)
    # Fallback на activity_id убран: он вызывает 401 на инвойс-эндпоинте
    receipt_id = None
    for comp in (act.get("components") or []):
        meta = comp.get("metadata") or {}
        for action in (meta.get("actions") or []):
            if action.get("action_type") == "VIEW_RECEIPT":
                rid = (action.get("action_metadata") or {}).get("reference_id")
                if rid:
                    receipt_id = rid
                break
        if receipt_id:
            break

    # Контрагент: title — это полное имя (APELLIDO NOMBRE)
    sender    = title if not is_outgoing else None
    recipient = title if is_outgoing     else None

    return {
        "id":                aid,
        "title":             title,
        "receipt_id":        receipt_id,
        "amount":            amount,
        "date_str":          date_str,
        "is_outgoing":       is_outgoing,
        "sender":            sender,
        "sender_lastname":   None,
        "recipient":         recipient,
        "recipient_lastname":None,
        "_raw":              act,
    }


def _normalize_activity(act: dict) -> dict:
    if not isinstance(act, dict):
        return {}
    # AstroPay-формат: есть activity_id или main_amount
    if "activity_id" in act or "main_amount" in act:
        return _normalize_astropay_activity(act)
    attrs = act.get("attributes") or act
    aid = act.get("id") or act.get("transactionId") or attrs.get("id") or attrs.get("transactionId")
    title = (
        attrs.get("title") or attrs.get("description")
        or act.get("title") or act.get("description")
        or str(aid or "Операция")
    )
    # receipt_id: для PP — transactionId (36-символьный hex); для остальных — ищем 32-символьный hex
    receipt_id = (
        act.get("transactionId") or attrs.get("transactionId")
        or _find_32char_hex_id(act)
    )

    # PP: amount может быть объектом {"value": 5000, "status": "positive", "currencySymbol": "+"}
    _raw_amount = attrs.get("amount") or attrs.get("monto") or act.get("amount") or act.get("monto")
    _amount_obj: dict = _raw_amount if isinstance(_raw_amount, dict) else {}
    amount = _amount_obj.get("value") if _amount_obj else _raw_amount
    if amount is not None:
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            amount = None
    if amount is None:
        amount = _find_in_dict(act, "amount", "monto", "value", "total", "ars", want_number=True)

    date_raw = (
        attrs.get("date") or attrs.get("fecha") or attrs.get("createdAt") or attrs.get("created_at")
        or act.get("date") or act.get("fecha") or act.get("createdAt") or act.get("timestamp")
    )
    if date_raw is None:
        date_raw = _find_any_in_dict(act, ("createdAt", "date", "fecha", "timestamp", "created_at"))
    date_str = None
    if date_raw:
        if isinstance(date_raw, (int, float)):
            try:
                date_str = datetime.utcfromtimestamp(
                    date_raw / 1000 if date_raw > 1e12 else date_raw
                ).strftime("%d.%m.%Y %H:%M")
            except Exception:
                date_str = str(date_raw)
        else:
            date_str = str(date_raw)[:19]

    # ── Определение направления транзакции ──────────────────────────────────
    # Поддерживаем форматы PP и AstroPay (TRANSFER_SENT / TRANSFER_RECEIVED)
    tx_type_raw = (
        attrs.get("transactionType") or attrs.get("type")
        or act.get("transactionType") or act.get("type") or ""
    )
    # Нормализуем тип: убираем подчёркивания и дефисы для надёжного сравнения
    # bank_output_transfer → bankoutputtransfer, COMMIT_OUTER → commitouter
    tx_type = tx_type_raw.lower().replace("_", "").replace("-", "")
    title_lower = (title or "").lower()

    # Явные флаги "входящий" и "исходящий" из разных API
    _OUTGOING_TYPES = {
        "transfer_sent", "send", "output", "outgoing", "withdrawal",
        "debit", "salida", "envio", "pago",
        # PersonalPay outgoing types
        "bankoutputtransfer", "outertransfer", "externaloutput",
        "transferout", "cashout", "commitmain", "commitouter",
    }
    _INCOMING_TYPES = {
        "transfer_received", "receive", "input", "incoming", "deposit",
        "credit", "entrada", "cobro",
        # PersonalPay incoming types
        "bankinputtransfer", "externalinput", "transferin", "cashin",
    }

    is_outgoing: bool
    if any(t in tx_type for t in _OUTGOING_TYPES):
        is_outgoing = True
    elif any(t in tx_type for t in _INCOMING_TYPES):
        is_outgoing = False
    else:
        # PP: currencySymbol = "-" → вывод, "+" → приход
        # ВАЖНО: amount.status всегда "positive" даже для выводов — не использовать для направления!
        _amt_symbol = (_amount_obj.get("currencySymbol") or "").strip()
        _amt_status = (_amount_obj.get("status") or "").lower()
        if _amt_symbol == "-":
            is_outgoing = True
        elif _amt_symbol == "+":
            is_outgoing = False
        elif _amt_status == "negative":
            is_outgoing = True
        elif _amt_status == "positive":
            is_outgoing = False
        else:
            # Fallback по заголовку/описанию
            is_outgoing = (
                "enviaste" in title_lower
                or "envío" in title_lower
                or "transferencia enviada" in title_lower
                or "out" in tx_type
            )

    # PP: description = "de NOMBRE" (входящий) или "a NOMBRE" / "para NOMBRE" (исходящий)
    _desc_raw = (attrs.get("description") or act.get("description") or "").strip()
    _desc_lower = _desc_raw.lower()
    _desc_name: Optional[str] = None
    if _desc_lower.startswith("de "):
        _desc_name = _desc_raw[3:].strip()
    elif _desc_lower.startswith("para "):
        _desc_name = _desc_raw[5:].strip()
    elif _desc_lower.startswith("a ") and len(_desc_raw) > 2:
        _desc_name = _desc_raw[2:].strip()

    inner = act.get("transference") if isinstance(act.get("transference"), dict) else act

    # ── Имя и фамилия отправителя ────────────────────────────────────────────
    # AstroPay вкладывает данные в origin / destination объекты транзакции
    origin_obj = act.get("origin") or act.get("sender_info") or {}
    dest_obj   = act.get("destination") or act.get("recipient_info") or {}

    sender = (
        attrs.get("remitente") or attrs.get("sender") or attrs.get("originName") or attrs.get("senderName")
        or act.get("remitente") or act.get("sender") or act.get("from")
        # PP: "de NOMBRE" в description → отправитель при входящей
        or (_desc_name if not is_outgoing else None)
        # AstroPay: origin.name / origin.first_name
        or (_join_name(origin_obj.get("first_name"), None) if origin_obj else None)
        or _find_in_details(act, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _find_in_details(inner, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _get_nested(inner, "transactionData", "origin", "holder")
        or _find_in_dict(act, "remitente", "sender", "originName", "holder")
        or _find_in_dict(inner, "remitente", "sender", "originName", "holder")
    )
    sender_lastname = (
        origin_obj.get("last_name") or origin_obj.get("lastName") or origin_obj.get("apellido")
        or attrs.get("senderLastName") or attrs.get("sender_last_name")
        or act.get("senderLastName") or act.get("sender_last_name")
        or _find_in_dict(act, "senderLastName", "sender_last_name", want_number=False)
    )

    recipient = (
        attrs.get("destinatario") or attrs.get("recipient") or attrs.get("recipientName")
        or act.get("destinatario") or act.get("recipient") or act.get("to")
        # PP: "a NOMBRE" / "para NOMBRE" в description → получатель при исходящей
        or (_desc_name if is_outgoing else None)
        # AstroPay: destination.name / destination.first_name
        or (_join_name(dest_obj.get("first_name"), None) if dest_obj else None)
        or _find_in_details(act, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _find_in_details(inner, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _get_nested(inner, "transactionData", "destination", "holder")
        or _get_nested(inner, "transactionData", "destination", "label")
        or _find_in_dict(act, "destinatario", "recipient", "beneficiary", "label", "holder")
        or _find_in_dict(inner, "destinatario", "recipient", "beneficiary", "label", "holder")
    )
    recipient_lastname = (
        dest_obj.get("last_name") or dest_obj.get("lastName") or dest_obj.get("apellido")
        or attrs.get("recipientLastName") or attrs.get("recipient_last_name")
        or act.get("recipientLastName") or act.get("recipient_last_name")
        or _find_in_dict(act, "recipientLastName", "recipient_last_name", want_number=False)
    )

    sender           = sender.strip()           if isinstance(sender, str)           else None
    sender_lastname  = sender_lastname.strip()  if isinstance(sender_lastname, str)  else None
    recipient        = recipient.strip()        if isinstance(recipient, str)        else None
    recipient_lastname = recipient_lastname.strip() if isinstance(recipient_lastname, str) else None

    return {
        "id": aid, "title": title, "receipt_id": receipt_id,
        "amount": amount, "date_str": date_str,
        "is_outgoing": is_outgoing,
        "sender": sender,
        "sender_lastname": sender_lastname,
        "recipient": recipient,
        "recipient_lastname": recipient_lastname,
        "_raw": act,
    }


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------

def _proxy_to_url(raw: str, scheme: str = "socks5h") -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    if low.startswith(("socks5://", "socks5h://", "http://", "https://")):
        return raw
    if "@" in raw:
        return f"{scheme}://{raw}"
    parts = [p.strip() for p in raw.split(":")]
    if len(parts) >= 4:
        host, port, user = parts[0], parts[1], parts[2]
        password = ":".join(parts[3:])
        if host and port and user and password:
            return f"{scheme}://{user}:{password}@{host}:{port}"
    if len(parts) == 2 and all(parts):
        return f"{scheme}://{parts[0]}:{parts[1]}"
    return f"{scheme}://{raw}"


def _proxy_from_parts(proxy_host, proxy_port, proxy_user, proxy_password, proxy_type, proxy_raw) -> str:
    scheme = (proxy_type or "socks5h").strip().lower()
    if scheme not in ("socks5", "socks5h", "http", "https"):
        scheme = "socks5h"
    host, port = (proxy_host or "").strip(), (proxy_port or "").strip()
    user, password = (proxy_user or "").strip(), (proxy_password or "").strip()
    if host and port:
        auth = f"{user}:{password}@" if user and password else ""
        return f"{scheme}://{auth}{host}:{port}"
    return _proxy_to_url(proxy_raw, scheme=scheme) if (proxy_raw or "").strip() else ""


def _apply_proxy_to_credentials(proxy_url: str, credentials: dict) -> dict:
    proxy_url = (proxy_url or "").strip()
    if not proxy_url:
        return credentials
    c = dict(credentials or {})
    c["proxy"] = c["http_proxy"] = c["https_proxy"] = proxy_url
    return c


def _proxy_parts_from_credentials(credentials: dict) -> dict:
    c = credentials or {}
    raw = (c.get("https_proxy") or c.get("proxy") or c.get("http_proxy") or "").strip()
    result = {
        "proxy_type": "socks5h", "proxy_host": "", "proxy_port": "",
        "proxy_user": "", "proxy_password": "", "proxy_raw": raw,
    }
    if not raw:
        return result
    if "://" in raw:
        scheme, rest = raw.split("://", 1)
        if scheme.lower() in ("socks5", "socks5h", "http", "https"):
            result["proxy_type"] = scheme.lower()
        raw = rest
    auth, hostport = (raw.split("@", 1) + [""])[:2] if "@" in raw else ("", raw)
    if auth and ":" in auth:
        u, p = auth.split(":", 1)
        result["proxy_user"], result["proxy_password"] = u, p
    if hostport and ":" in hostport:
        h, pt = hostport.rsplit(":", 1)
        result["proxy_host"], result["proxy_port"] = h, pt
    else:
        result["proxy_host"] = hostport
    return result


def _parse_credentials(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    token = raw.strip()
    for prefix in ("Authorization:", "Authorization：", "authorization:"):
        if token.upper().startswith(prefix.upper()):
            token = token[len(prefix):].strip()
            break
    if not token:
        return {}
    if token.upper().startswith("BEARER "):
        return {"auth_token": token}
    if token.startswith("eyJ"):
        return {"auth_token": "Bearer " + token}
    return {}


# ---------------------------------------------------------------------------
# Auth Middleware
# ---------------------------------------------------------------------------

class AuthMiddleware(BaseHTTPMiddleware):
    PUBLIC = {"/login", "/static", "/health"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if any(path == p or path.startswith(p + "/") for p in self.PUBLIC):
            return await call_next(request)
        if not _current_user(request):
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)


app.add_middleware(AuthMiddleware)


# ---------------------------------------------------------------------------
# Фоновый сбор курса USDT/ARS каждые 10 минут
# ---------------------------------------------------------------------------

async def _rate_collector_loop():
    """Фоновая задача — собирает курс Bybit P2P каждые 10 минут в БД.
    Работает независимо от браузера и открытых страниц.
    Ежедневно в 07:30 МСК удаляет данные вне текущего 24-часового окна."""
    # Первый запрос через 5 сек после старта
    await asyncio.sleep(5)
    _msk = timezone(timedelta(hours=3))
    _last_cleanup_date: Optional[str] = None

    while True:
        try:
            # ── Ежедневная очистка истории курса в 07:30 МСК ──────────────
            now_msk = datetime.now(_msk)
            today_key = now_msk.date().isoformat()
            window_start = now_msk.replace(hour=7, minute=30, second=0, microsecond=0)
            if now_msk >= window_start and _last_cleanup_date != today_key:
                deleted = cleanup_rate_history()
                _last_cleanup_date = today_key
                logger.info("rate history cleanup at 07:30 MSK: removed %d rows", deleted)

            # ── Сбор курса ────────────────────────────────────────────────
            data = await _fetch_bybit_p2p()
            if data and not data.get("error"):
                save_rate_point(
                    buy_avg  = data.get("buy_avg")  or 0,
                    sell_avg = data.get("sell_avg") or 0,
                    ts       = data.get("ts") or int(time.time()),
                )
                global _rate_cache
                _rate_cache = {"ts": time.time(), "data": data}
                logger.info("rate collected: buy=%.2f sell=%.2f",
                            data.get("buy_avg", 0), data.get("sell_avg", 0))
        except Exception as e:
            logger.warning("rate collector error: %s", e)
        await asyncio.sleep(600)  # 10 минут


# ---------------------------------------------------------------------------
# Фоновый PIN keep-alive для PersonalPay — каждые 8 часов
# ---------------------------------------------------------------------------

_pp_last_keepalive: dict = {}   # account_id → timestamp последнего успешного PIN-refresh

async def _pp_keepalive_loop():
    """Фоновая задача: каждые 30 минут проверяет все PP аккаунты с pin_hash.
    Если прошло > 8 часов с последнего PIN-refresh — делает его автоматически.
    Это не даёт токену протухнуть: сессия продлевается до истечения JWT."""
    await asyncio.sleep(60)   # первый запуск через 1 мин после старта
    PP_INTERVAL = 8 * 3600    # 8 часов между keep-alive
    CHECK_INTERVAL = 1800     # проверяем раз в 30 минут

    while True:
        try:
            from app.drivers.personalpay import refresh_session_with_pin, _pp_jwt_exp
            all_accs = list_accounts()
            pp_accs = [a for a in all_accs if a.get("bank_type") == "personalpay"
                       and (a.get("credentials") or {}).get("pin_hash")]
            now = time.time()
            for acc in pp_accs:
                acc_id = acc["id"]
                last = _pp_last_keepalive.get(acc_id, 0)
                if now - last < PP_INTERVAL:
                    continue   # ещё не пора

                creds = acc.get("credentials") or {}
                token = creds.get("auth_token", "")
                # Если токен уже мёртв > 2ч — не пробуем (всё равно не поможет)
                exp = _pp_jwt_exp(token) if token else None
                if exp and exp < now - 7200:
                    logger.info("pp keepalive skip acc=%d: token expired >2h ago", acc_id)
                    continue

                try:
                    new_token = await asyncio.to_thread(refresh_session_with_pin, creds)
                    if new_token:
                        _pp_last_keepalive[acc_id] = now
                        # Сохраняем обновлённый токен (или тот же — но с продлённой серверной сессией)
                        new_creds = dict(creds)
                        new_creds["auth_token"] = new_token
                        db_update_account(acc_id, credentials=new_creds)
                        logger.info("pp keepalive ok acc=%d", acc_id)
                    else:
                        logger.warning("pp keepalive failed acc=%d: no token returned", acc_id)
                except Exception as e:
                    logger.warning("pp keepalive error acc=%d: %s", acc_id, e)
        except Exception as e:
            logger.warning("pp keepalive loop error: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


async def _pp_migrate_default_pin():
    """Одноразовая миграция: проставляем pin_hash=464646 всем PP-аккаунтам без пин-кода."""
    try:
        all_accs = list_accounts()
        for acc in all_accs:
            if acc.get("bank_type") != "personalpay":
                continue
            creds = acc.get("credentials") or {}
            if creds.get("pin_hash"):
                continue  # уже есть
            new_creds = dict(creds)
            new_creds["pin_hash"] = PP_DEFAULT_PIN_HASH
            db_update_account(acc["id"], credentials=new_creds)
            logger.info("pp migrate: добавлен default pin_hash для аккаунта id=%s (%s)", acc["id"], acc.get("label", ""))
    except Exception as e:
        logger.warning("pp migrate default pin error: %s", e)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_rate_collector_loop())
    asyncio.create_task(_pp_keepalive_loop())
    asyncio.create_task(_pp_migrate_default_pin())


# ---------------------------------------------------------------------------
# Автовывод
# ---------------------------------------------------------------------------

def _run_auto_withdraw_rule(acc: dict, rule: dict) -> Tuple[bool, str]:
    remaining = max(0.0, float(rule.get("total_limit", 0)) - float(rule.get("paid_amount", 0)))
    if remaining <= 0:
        update_auto_withdraw_progress(rule["id"], is_active=False)
        return False, "Лимит достигнут"

    chunk = min(float(rule.get("chunk_amount", 0)), remaining)
    if chunk <= 0:
        update_auto_withdraw_progress(rule["id"], last_error="Некорректная сумма", is_active=False)
        return False, "Некорректная сумма"

    # Лимиты не применяются для внутренних переводов (PP→PP, AP→AP)
    cvu = rule.get("cvu", "")
    if not _is_limit_exempt(cvu):
        if is_account_withdraw_limit_reached(acc["id"]):
            msg = f"Карта достигла лимита выводов ({DAILY_WITHDRAW_LIMIT})"
            update_auto_withdraw_progress(rule["id"], last_error=msg)
            return False, msg

        # Проверка дневного лимита по CVU
        if is_withdraw_limit_reached(cvu, acc["id"]):
            msg = f"Дневной лимит ({DAILY_WITHDRAW_LIMIT}) достигнут для CVU {cvu}"
            update_auto_withdraw_progress(rule["id"], last_error=msg)
            return False, msg

    # Проверка минимального баланса
    min_balance = float(rule.get("min_balance") or 0)
    if min_balance > 0:
        try:
            balance_info = driver_balance(acc["bank_type"], acc["credentials"])
            current_balance = float(balance_info.get("balance") or 0)
        except Exception as e:
            err = f"Не удалось получить баланс: {e}"
            update_auto_withdraw_progress(rule["id"], last_error=err)
            return False, err
        if current_balance < min_balance:
            return False, f"Баланс {current_balance:,.0f} < порога {min_balance:,.0f} — ожидание"

    try:
        if acc["bank_type"] == "universalcoins":
            raise ValueError("Автовывод не поддерживается для UniversalCoins")
        result = driver_withdraw(
            acc["bank_type"], acc["credentials"],
            destination=cvu, amount=chunk, comments="Auto withdraw",
        )
        increment_withdraw_count(cvu, acc["id"])
        increment_account_withdraw_count(acc["id"])
        update_auto_withdraw_progress(rule["id"], paid_delta=chunk, last_error="")
    except Exception as e:
        update_auto_withdraw_progress(rule["id"], last_error=str(e))
        return False, str(e)

    tid = _find_32char_hex_id(result) if isinstance(result, dict) else None
    return True, (tid or "")


# ---------------------------------------------------------------------------
# Маршруты: аутентификация
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _current_user(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, username: str = Form(""), password: str = Form("")):
    user = get_user_by_username(username)
    if not user or not user.get("is_active") or not verify_password(password, user.get("password_hash", "")):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Неверный логин или пароль."},
            status_code=400,
        )
    token = secrets.token_urlsafe(32)
    create_session(token, user["id"], user["username"], int(time.time() + SESSION_TTL))
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key=SESSION_COOKIE, value=token,
        httponly=True, samesite="lax", max_age=SESSION_TTL,
    )
    return response


@app.post("/logout", response_class=RedirectResponse)
async def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        delete_session(token)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


# ---------------------------------------------------------------------------
# Маршруты: главная / дашборд
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, account_id: Optional[int] = None, window: Optional[str] = None):
    try:
        return await _index_impl(request, account_id, window)
    except Exception:
        logger.error("index error: %s", traceback.format_exc())
        return HTMLResponse(
            content=_error_page("Внутренняя ошибка сервера. Попробуйте обновить страницу."),
            status_code=500,
        )


def _error_page(msg: str) -> str:
    return (
        f'<html><head><meta charset="UTF-8"><title>Ошибка</title>'
        f'<style>body{{font-family:sans-serif;padding:2rem;background:#0f172a;color:#e2e8f0}}'
        f'.box{{background:#1e293b;padding:1.5rem;border-radius:12px;border-left:4px solid #ef4444}}</style></head>'
        f'<body><div class="box"><h2>⚠️ Что-то пошло не так</h2><p>{msg}</p>'
        f'<a href="/" style="color:#38bdf8">← На главную</a></div></body></html>'
    )


async def _index_impl(request: Request, account_id: Optional[int], window: Optional[str]):
    # ── Только быстрые операции: чтение из БД (SQLite) ─────────────────────
    # Никаких HTTP-запросов к банку. Баланс и история грузятся через JS.
    groups = accounts_by_window()
    accounts = [acc for accs in groups.values() for acc in accs]

    selected = None
    window_accounts: list = []
    window_slug = window_name = None

    if window:
        wslug = normalize_window_slug(window)
        if wslug in groups:
            window_slug = wslug
            window_name = _window_name(wslug)
            window_accounts = groups.get(wslug, [])

    if account_id:
        selected = get_account(account_id)  # только SQLite — мгновенно

    # JWT-статус — вычисляется локально без HTTP
    token_expired = False
    token_expires_in_hours = None
    if selected and selected.get("credentials", {}).get("auth_token"):
        try:
            token_expired, token_expires_in_hours = _jwt_expiry(selected["credentials"])
        except Exception:
            pass

    # Правила автовывода и счётчики лимитов — только БД
    auto_rules: list = []
    withdraw_counts: dict = {}
    account_withdraw_count = 0
    account_limit_reached = False
    account_limit_near = False
    if selected:
        auto_rules = list_auto_withdraw_rules(selected["id"])
        account_withdraw_count = get_account_withdraw_count(selected["id"])
        account_limit_reached = account_withdraw_count >= DAILY_WITHDRAW_LIMIT
        account_limit_near = account_withdraw_count >= (DAILY_WITHDRAW_LIMIT - 1)
        for rule in auto_rules:
            cvu = rule.get("cvu", "")
            if cvu and cvu not in withdraw_counts:
                withdraw_counts[cvu] = get_withdraw_count(cvu, selected["id"])

    error_code = request.query_params.get("error", "")
    error_display = _error_text(error_code) if error_code else ""

    return templates.TemplateResponse("index.html", {
        "request":               request,
        "accounts":              accounts,
        "groups":                groups,
        "window_list":           get_window_list(),
        "selected":              selected,
        "account_id":            account_id,
        "bank_types":            BANK_TYPES,
        "concepts_uc":           CONCEPTS_UC,
        "error_display":         error_display,
        "token_expired":         token_expired,
        "token_expires_in_hours":token_expires_in_hours,
        "window_slug":           window_slug,
        "window_name":           window_name,
        "window_accounts":       window_accounts,
        "current_user":          _current_user(request),
        "auto_rules":            auto_rules,
        "withdraw_counts":       withdraw_counts,
        "daily_limit":           DAILY_WITHDRAW_LIMIT,
        "account_withdraw_count": account_withdraw_count,
        "account_limit_reached":  account_limit_reached,
        "account_limit_near":     account_limit_near,
        "prefill": {
            "cvu": "", "destination": "", "amount": "",
            "concept": "VARIOS", "comments": "Varios (VAR)",
            "alias": "", "document": "", "name": "", "bank": "",
        },
    })


# ---------------------------------------------------------------------------
# Dashboard — обзор всех аккаунтов
# ---------------------------------------------------------------------------

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    groups = accounts_by_window()
    accounts = [acc for accs in groups.values() for acc in accs]
    return templates.TemplateResponse("dashboard.html", {
        "request":      request,
        "accounts":     accounts,
        "groups":       groups,
        "window_list":  get_window_list(),
        "current_user": _current_user(request),
        "account_id":   None,
        "selected":     None,
        "window_slug":  None,
    })


# ---------------------------------------------------------------------------
# API: баланс + история (JSON, для lazy-loading)
# ---------------------------------------------------------------------------

@app.get("/account/{account_id}/balance")
async def api_balance(account_id: int):
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)
    try:
        raw = await asyncio.to_thread(driver_balance, acc["bank_type"], acc["credentials"])
        info = _normalize_balance(raw, acc["bank_type"])
        info.pop("raw_accounts", None)
        info["account_withdraw_count"] = get_account_withdraw_count(account_id)
        info["account_withdraw_limit"] = DAILY_WITHDRAW_LIMIT

        # ── Имя владельца (PersonalPay) ──────────────────────────────────────
        if acc["bank_type"] == "personalpay":
            from app.drivers.personalpay import get_owner_name_from_jwt, get_cvu_info
            owner = get_owner_name_from_jwt(acc["credentials"].get("auth_token", ""))
            if not owner:
                # Пробуем CVU endpoint — он часто возвращает полное имя владельца
                try:
                    cvu_data = await asyncio.to_thread(get_cvu_info, acc["credentials"])
                    owner = (
                        cvu_data.get("holder") or cvu_data.get("holderName")
                        or cvu_data.get("name") or cvu_data.get("fullName")
                        or cvu_data.get("owner") or ""
                    )
                    # Если CVU endpoint вернул CVU - обновим если текущий пустой
                    if not info.get("cvu_number"):
                        info["cvu_number"] = cvu_data.get("cvu") or cvu_data.get("number") or ""
                    if not info.get("cvu_alias"):
                        info["cvu_alias"] = cvu_data.get("alias") or cvu_data.get("name") or ""
                except Exception:
                    pass
            info["owner_name"] = str(owner).strip() if owner else ""

            # ── Авто-сохранение обновлённого токена (PIN авто-refresh) ─────────
            device_id = acc["credentials"].get("device_id", "")
            new_tok = pp_consume_refreshed_token(device_id)
            if new_tok:
                new_creds = dict(acc["credentials"])
                new_creds["auth_token"] = new_tok
                try:
                    db_update_account(account_id, credentials=new_creds)
                    info["token_auto_refreshed"] = True
                except Exception:
                    pass

        return info
    except Exception as e:
        err = str(e)
        low = err.lower()
        code = "bank_unavailable"
        if any(x in low for x in ("proxyerror", "proxy", "tunnel connection failed", "cannot connect to proxy")):
            code = "bank_unavailable"
        elif "401" in low or "unauthorized" in low:
            code = "token_expired"
        elif "403" in low or "forbidden" in low:
            # 403 с живым токеном — чаще всего неверный device_id или x-fraud-paygilant-session-id
            code = "token_expired"
            err = (
                "403 Forbidden — токен отклонён банком. "
                "Возможные причины: (1) Вставьте свежий auth_token из HTTP Toolkit. "
                "(2) Добавьте в credentials поле x_fraud_paygilant_session_id "
                "(скопируйте заголовок x-fraud-paygilant-session-id из любого запроса в приложении). "
                f"Оригинал ошибки: {err}"
            )
        elif any(x in low for x in ("token", "jwt", "истёк", "expired")):
            code = "token_expired"
        return JSONResponse({
            "error": err,
            "error_meta": _error_payload(
                code,
                details=err,
                suggestion=(
                    "Откройте настройки карты и обновите auth_token. "
                    "Если токен свежий — также скопируйте x-fraud-paygilant-session-id из HTTP Toolkit."
                    if code == "token_expired"
                    else "Проверьте настройки прокси/сети и повторите через 1–2 минуты."
                ),
            ),
        }, status_code=500)


@app.post("/account/{account_id}/pin-refresh")
async def api_pin_refresh(account_id: int):
    """Пробует продлить сессию PersonalPay через PIN-валидацию.
    Требует поля pin_hash в credentials (SHA-256 от PIN-кода).
    Если получен новый JWT — сохраняет его в БД и возвращает ok:true."""
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay":
        return JSONResponse({"error": "PP account not found"}, status_code=404)
    creds = acc.get("credentials") or {}
    if not creds.get("pin_hash"):
        return JSONResponse({
            "ok": False,
            "message": "pin_hash не задан. Добавьте SHA-256 от PIN-кода в credentials."
        })
    try:
        from app.drivers.personalpay import refresh_session_with_pin
        new_token = await asyncio.to_thread(refresh_session_with_pin, creds)
        if new_token:
            new_creds = dict(creds)
            new_creds["auth_token"] = new_token
            db_update_account(account_id, credentials=new_creds)
            return JSONResponse({"ok": True, "message": "Сессия продлена через PIN ✓"})
        return JSONResponse({
            "ok": False,
            "message": "PIN не принят — ответ от сервера пустой. Обновите auth_token вручную."
        })
    except RuntimeError as e:
        return JSONResponse({"ok": False, "message": str(e)})
    except Exception as e:
        logger.exception("api_pin_refresh error acc=%s", account_id)
        return JSONResponse({"ok": False, "message": f"Ошибка: {e}"}, status_code=500)


@app.get("/account/{account_id}/status")
async def api_account_status(account_id: int):
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)

    count = get_account_withdraw_count(account_id)
    return JSONResponse({
        "account_id": account_id,
        "withdraw_count": count,
        "withdraw_limit": DAILY_WITHDRAW_LIMIT,
        "is_limit_reached": count >= DAILY_WITHDRAW_LIMIT,
        "is_near_limit": count >= (DAILY_WITHDRAW_LIMIT - 1),
    })


@app.get("/account/{account_id}/receipt")
async def api_receipt(account_id: int, ref: str = ""):
    """Возвращает информацию о чеке по ID транзакции.
    PP  → {"type":"details", "confirmation_id":..., "amount":..., ...}
    AP  → {"type":"pdf", "url":"https://files.astropay.com/..."}
    """
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)
    if not ref:
        return JSONResponse({"error": "ref required"}, status_code=400)
    bank = acc["bank_type"]
    try:
        if bank == "astropay":
            from app.drivers.astropay import get_receipt as ap_get_receipt
            data = await asyncio.to_thread(ap_get_receipt, acc["credentials"], ref)
            pdf_url = data.get("file_resource") or data.get("file_path") or ""
            return JSONResponse({"type": "pdf", "url": pdf_url})

        elif bank == "personalpay":
            from app.drivers.personalpay import get_transference_details
            data = await asyncio.to_thread(get_transference_details, acc["credentials"], ref)
            t  = data.get("transference") or data
            sd = t.get("stateDetail") or {}

            # ── Читаем массив details (label/value пары от PP API) ──────────
            # Реальные лейблы из PP API (x-body-version: 2)
            label_ru = {
                "fecha":             "Fecha",
                "hora":              "Hora",
                "envía":             "Envía",
                "envia":             "Envía",
                "recibe":            "Recibe",
                "desde":             "Desde",
                "cuil/cuit":         "CUIL/CUIT",
                "banco/billetera":   "Banco/Billetera",
                "cbu/cvu":           "CBU/CVU",
                "nº de la operación":"Nº operación",
                "coelsaid":          "CoelsaID",
                "monto":             "Monto",
                "amount":            "Monto",
                "estado":            "Estado",
                "state":             "Estado",
                "date":              "Fecha",
                "remitente":         "Envía",
                "destinatario":      "Recibe",
                "id":                "ID operación",
                "confirmacion":      "Confirmación",
            }
            skip_labels = {"utr"}
            rows = []
            seen = set()
            for d in (t.get("details") or []):
                if not isinstance(d, dict):
                    continue
                lbl_raw = (d.get("label") or d.get("key") or "").strip()
                val = str(d.get("value") or d.get("displayValue") or "").strip()
                if not val or lbl_raw.lower() in skip_labels:
                    continue
                lbl_show = label_ru.get(lbl_raw.lower()) or lbl_raw
                key = (lbl_show + "|" + val)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"label": lbl_show, "value": val})

            # ── Добавляем поля верхнего уровня если details пустой ──────────
            td   = t.get("transactionData") or {}
            orig = td.get("origin") or {}
            dest = td.get("destination") or {}

            def _add(lbl, val):
                if val and (lbl + "|" + str(val)) not in seen:
                    rows.append({"label": lbl, "value": str(val)})

            amount_raw = t.get("amount")
            currency   = (t.get("currency") or {}).get("iso4217") or "ARS"
            if amount_raw is not None:
                try:
                    amt_fmt = f"{float(amount_raw):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
                    _add("Monto", f"{amt_fmt} {currency}")
                except Exception:
                    _add("Monto", f"{amount_raw} {currency}")

            _add("Estado",        sd.get("message") or t.get("state") or "")
            _add("Fecha",         t.get("confirmationDate") or t.get("created") or t.get("date") or "")
            _add("Confirmación",  t.get("confirmationId") or "")
            _add("Remitente",     orig.get("holder") or "")
            _add("Cuenta origen", orig.get("cbu") or "")
            _add("Destinatario",  dest.get("label") or dest.get("holder") or "")
            _add("CVU destino",   dest.get("cbu") or "")
            _add("ID",            t.get("id") or ref)

            # Если всё равно пусто — показываем сырые данные из ответа PP
            if not rows:
                for k, v in t.items():
                    if k.startswith("_") or isinstance(v, (dict, list)):
                        continue
                    if v is not None and str(v).strip():
                        rows.append({"label": k, "value": str(v)})
                if not rows:
                    rows.append({"label": "ID транзакции", "value": ref})

            return JSONResponse({
                "type":  "rows",
                "rows":  rows,
                "title": sd.get("message") or t.get("state") or "Чек",
            })

        else:
            return JSONResponse({"error": "not supported"}, status_code=400)

    except Exception as e:
        logger.exception("api_receipt error account=%d ref=%s", account_id, ref)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/account/{account_id}/activities")
async def api_activities(
    account_id: int,
    type:   str = "all",   # all | incoming | outgoing
    page:   int = 1,       # страница (1-based)
    limit:  int = 20,      # записей на страницу (макс 100)
    search: str = "",      # поиск по имени/фамилии
):
    """Lazy-load: история операций. Поддерживает PP и AstroPay.
    Параметры: ?type=all|incoming|outgoing&page=1&limit=20&search=Иванов"""
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)

    bank = acc["bank_type"]
    _SUPPORTED = ("personalpay", "astropay")
    if bank not in _SUPPORTED:
        return JSONResponse({"activities": [], "total": 0, "page": 1, "pages": 1})

    # Ограничиваем limit во избежание злоупотреблений
    limit = max(1, min(limit, 100))
    page  = max(1, page)

    try:
        # ── Загрузка сырых активностей ──────────────────────────────────────
        if bank == "personalpay":
            data = await asyncio.to_thread(pp_activities_list, acc["credentials"], 0, 50)
            raw  = _extract_activities_raw(data)
            # Если во время запроса произошёл авто-refresh токена — сохраняем в БД
            _new_tok = pp_consume_refreshed_token(acc["credentials"].get("device_id", ""))
            if _new_tok:
                try:
                    _nc = dict(acc["credentials"]); _nc["auth_token"] = _new_tok
                    db_update_account(account_id, credentials=_nc)
                except Exception:
                    pass
        else:  # astropay
            from app.drivers.astropay import get_activities as ap_get_activities
            data = await asyncio.to_thread(ap_get_activities, acc["credentials"], 1, 50)
            raw  = data.get("data") or []

        # ── Нормализация ─────────────────────────────────────────────────────
        activities = []
        for act in (raw if isinstance(raw, list) else []):
            n = _normalize_activity(act)
            if not n:
                continue
            is_outgoing = bool(n.get("is_outgoing"))

            s_name  = n.get("sender") or ""
            s_last  = n.get("sender_lastname") or ""
            r_name  = n.get("recipient") or ""
            r_last  = n.get("recipient_lastname") or ""

            # Полное имя контрагента (кто получил при выводе / кто прислал при поступлении)
            if is_outgoing:
                full_parts = [p for p in [r_name, r_last] if p]
            else:
                full_parts = [p for p in [s_name, s_last] if p]
            full_name = " ".join(full_parts) if full_parts else "Не указано"

            activities.append({
                "id":                n.get("id"),
                "title":             n.get("title"),
                "receipt_id":        n.get("receipt_id"),
                "amount":            n.get("amount"),
                "date_str":          n.get("date_str"),
                "is_outgoing":       is_outgoing,
                "type":              "outgoing" if is_outgoing else "incoming",
                "sender":            s_name,
                "sender_lastname":   s_last  or "Не указана",
                "recipient":         r_name,
                "recipient_lastname":r_last  or "Не указана",
                "full_name":         full_name,
            })

        # ── Фильтр по типу ───────────────────────────────────────────────────
        if type == "incoming":
            activities = [a for a in activities if not a["is_outgoing"]]
        elif type == "outgoing":
            activities = [a for a in activities if a["is_outgoing"]]

        # ── Поиск по имени / фамилии ─────────────────────────────────────────
        q = search.strip().lower()
        if q:
            def _match(a):
                haystack = " ".join(filter(None, [
                    a.get("sender"), a.get("sender_lastname"),
                    a.get("recipient"), a.get("recipient_lastname"),
                    a.get("full_name"),
                ])).lower()
                return q in haystack
            activities = [a for a in activities if _match(a)]

        # ── Пагинация ────────────────────────────────────────────────────────
        total  = len(activities)
        pages  = max(1, (total + limit - 1) // limit)
        page   = min(page, pages)
        offset = (page - 1) * limit
        paged  = activities[offset : offset + limit]

        return JSONResponse({
            "activities": paged,
            "total":  total,
            "page":   page,
            "pages":  pages,
            "limit":  limit,
        })
    except Exception as e:
        logger.exception("api_activities error account=%d", account_id)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/account/{account_id}/auto-withdraw/{rule_id}/trigger")
async def api_trigger_auto_withdraw(account_id: int, rule_id: int):
    """Запуск автовывода через JS (без перезагрузки страницы)."""
    acc = get_account(account_id)
    rule = get_auto_withdraw_rule(rule_id)
    if not acc or not rule or int(rule.get("account_id", 0)) != account_id:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    ok, message = await asyncio.to_thread(_run_auto_withdraw_rule, acc, rule)
    updated = get_auto_withdraw_rule(rule_id)
    return JSONResponse({
        "ok": ok,
        "message": message,
        "rule": updated,
    })


# ---------------------------------------------------------------------------
# Multi-withdraw API
# ---------------------------------------------------------------------------

@app.post("/multi-withdraw")
async def multi_withdraw(request: Request):
    """Вывод с нескольких счетов одновременно.
    Тело: { account_ids: [1,2,3], destination: "CVU", amount: "1000",
            concept: "VARIOS", comments: "..." }
    Возвращает: { results: [{account_id, label, ok, error, tid}] }
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    account_ids = body.get("account_ids") or []
    destination = (body.get("destination") or body.get("cvu") or "").strip()
    amount_raw  = str(body.get("amount") or "")
    concept     = body.get("concept") or "VARIOS"
    comments    = body.get("comments") or "Varios (VAR)"

    if not account_ids or not destination:
        return JSONResponse({"error": "account_ids and destination required"}, status_code=400)

    amt = _parse_amount(amount_raw)
    if not amt:
        return JSONResponse({"error": "invalid amount"}, status_code=400)

    async def _do_one(acc_id):
        acc = get_account(int(acc_id))
        if not acc:
            return {"account_id": acc_id, "label": "?", "ok": False, "error": "Счёт не найден", "tid": None}
        # Лимиты не применяются для внутренних переводов (PP→PP, AP→AP)
        if not _is_limit_exempt(destination):
            if is_account_withdraw_limit_reached(acc["id"]):
                return {
                    "account_id": acc_id, "label": acc["label"], "ok": False,
                    "error": "Карта достигла лимита выводов", "tid": None,
                }
            if is_withdraw_limit_reached(destination, acc["id"]):
                return {
                    "account_id": acc_id, "label": acc["label"], "ok": False,
                    "error": f"Дневной лимит ({DAILY_WITHDRAW_LIMIT}) достигнут", "tid": None,
                }
        try:
            if acc["bank_type"] == "universalcoins":
                result = await asyncio.to_thread(
                    driver_withdraw, acc["bank_type"], acc["credentials"],
                    cvu_recipient=destination, amount=amt, concept=concept,
                )
            else:
                result = await asyncio.to_thread(
                    driver_withdraw, acc["bank_type"], acc["credentials"],
                    destination=destination, amount=amt, comments=comments,
                )
            increment_withdraw_count(destination, acc["id"])
            increment_account_withdraw_count(acc["id"])
            tid = _find_32char_hex_id(result) if isinstance(result, dict) else None
            return {"account_id": acc_id, "label": acc["label"], "ok": True, "error": None, "tid": tid}
        except Exception as e:
            return {"account_id": acc_id, "label": acc["label"], "ok": False, "error": str(e), "tid": None}

    # Выполняем все выводы параллельно
    results = list(await asyncio.gather(*[_do_one(acc_id) for acc_id in account_ids]))
    return JSONResponse({"results": results})


# ---------------------------------------------------------------------------
# Вывод средств
# ---------------------------------------------------------------------------

@app.post("/account/{account_id}/withdraw", response_class=HTMLResponse)
async def withdraw(
    request: Request,
    account_id: int,
    cvu:         str = Form(""),
    destination: str = Form(""),
    amount:      str = Form(...),
    concept:     str = Form("VARIOS"),
    comments:    str = Form("Varios (VAR)"),
    alias:       str = Form(""),
    document:    str = Form(""),
    name:        str = Form(""),
    bank:        str = Form(""),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)

    amt = _parse_amount(amount)
    if amt is None:
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_amount", status_code=302)

    dest = (destination or cvu).strip()
    if not dest:
        return RedirectResponse(url=f"/?account_id={account_id}&error=no_destination", status_code=302)

    # Лимиты не применяются для внутренних переводов (PP→PP, AP→AP)
    if not _is_limit_exempt(dest):
        if is_account_withdraw_limit_reached(account_id):
            return RedirectResponse(url=f"/?account_id={account_id}&error=account_limit_reached", status_code=302)

        # Проверка дневного лимита
        if is_withdraw_limit_reached(dest, account_id):
            return RedirectResponse(url=f"/?account_id={account_id}&error=limit_reached", status_code=302)

    try:
        if acc["bank_type"] == "universalcoins":
            doc_clean = (document or "").strip().replace("-", "").replace(" ", "")
            if not doc_clean or len(doc_clean) < 10:
                return RedirectResponse(url=f"/?account_id={account_id}&error=document_required", status_code=302)
            result = await asyncio.to_thread(
                driver_withdraw,
                acc["bank_type"], acc["credentials"],
                cvu_recipient=dest, amount=amt, concept=concept,
                alias_recipient=alias.strip() or None,
                document_recipient=doc_clean,
                name_recipient=name.strip() or None,
                bank_recipient=bank.strip() or None,
            )
        else:
            result = await asyncio.to_thread(
                driver_withdraw,
                acc["bank_type"], acc["credentials"],
                destination=dest, amount=amt, comments=comments,
            )
        increment_withdraw_count(dest, account_id)
        increment_account_withdraw_count(account_id)
    except Exception as e:
        err_msg = str(e)
        if any(x in err_msg.lower() for x in ("rechazad", "rejected", "rechazo", "denied", "denegad")):
            error_param = "rejected_by_bank"
        else:
            error_param = quote(err_msg[:200], safe="")
        return RedirectResponse(url=f"/?account_id={account_id}&error={error_param}", status_code=302)

    tid = None
    if acc["bank_type"] in ("personalpay", "astropay") and isinstance(result, dict):
        tid = _find_32char_hex_id(result)
        if not tid:
            raw = (
                result.get("transactionId") or result.get("id")
                or result.get("transfer_id") or result.get("request_id")
                or (result.get("transference") or {}).get("id")
                or (result.get("data") or {}).get("transactionId")
            )
            if raw:
                raw = str(raw).strip()
                if "-" not in raw and len(raw) == 32 and all(c in "0123456789ABCDEFabcdef" for c in raw):
                    tid = raw

    if tid:
        return RedirectResponse(url=f"/account/{account_id}/receipt?transaction_id={tid}", status_code=302)
    return RedirectResponse(url=f"/?account_id={account_id}&success=1", status_code=302)


# ---------------------------------------------------------------------------
# Автовывод — правила
# ---------------------------------------------------------------------------

@app.post("/account/{account_id}/auto-withdraw", response_class=RedirectResponse)
async def create_auto_withdraw(
    account_id:  int,
    cvu:         str = Form(""),
    total_limit: str = Form(""),
    chunk_amount:str = Form(""),
    min_balance: str = Form("0"),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    total = _parse_amount(total_limit)
    chunk = _parse_amount(chunk_amount)
    min_bal = _parse_amount(min_balance) or 0.0
    if not total or not chunk or chunk > total or not cvu.strip():
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_auto_rule", status_code=302)
    add_auto_withdraw_rule(account_id, cvu.strip(), total, chunk, min_bal)
    return RedirectResponse(url=f"/?account_id={account_id}&success=auto_rule_created", status_code=302)


@app.post("/account/{account_id}/auto-withdraw/{rule_id}/run", response_class=RedirectResponse)
async def run_auto_withdraw(account_id: int, rule_id: int):
    acc = get_account(account_id)
    rule = get_auto_withdraw_rule(rule_id)
    if not acc or not rule or int(rule.get("account_id", 0)) != account_id:
        return RedirectResponse(url=f"/?account_id={account_id}", status_code=302)
    ok, message = _run_auto_withdraw_rule(acc, rule)
    if ok:
        return RedirectResponse(url=f"/?account_id={account_id}&success=auto_withdraw_done", status_code=302)
    return RedirectResponse(url=f"/?account_id={account_id}&error={quote(message[:200], safe='')}", status_code=302)


@app.post("/account/{account_id}/auto-withdraw/{rule_id}/delete", response_class=RedirectResponse)
async def delete_auto_withdraw(account_id: int, rule_id: int):
    delete_auto_withdraw_rule(rule_id, account_id)
    return RedirectResponse(url=f"/?account_id={account_id}", status_code=302)


# ---------------------------------------------------------------------------
# Аккаунты — CRUD
# ---------------------------------------------------------------------------

@app.get("/add", response_class=HTMLResponse)
async def add_account_page(request: Request, window: str = ""):
    groups = accounts_by_window()
    return templates.TemplateResponse("add_account.html", {
        "request":         request,
        "bank_types":      BANK_TYPES,
        "window_list":     get_window_list(),
        "preselect_window":normalize_window_slug(window or "glazars"),
        "accounts":        list_accounts(),
        "groups":          groups,
        "account_id":      None,
        "selected":        None,
        "window_slug":     None,
        "current_user":    _current_user(request),
    })


@app.post("/add", response_class=RedirectResponse)
async def add_account_post(
    bank_type:        str = Form(...),
    label:            str = Form(...),
    credentials_json: str = Form("{}"),
    proxy_type:       str = Form("socks5h"),
    proxy_host:       str = Form(""),
    proxy_port:       str = Form(""),
    proxy_user:       str = Form(""),
    proxy_password:   str = Form(""),
    proxy_raw:        str = Form(""),
    window:           str = Form("glazars"),
    window_custom:    str = Form(""),
):
    if bank_type not in BANK_TYPES:
        return RedirectResponse(url="/add?error=invalid_bank", status_code=302)
    window = normalize_window_slug(window_custom or window)
    credentials = _parse_credentials(credentials_json)
    if not credentials and credentials_json.strip():
        return RedirectResponse(url="/add?error=invalid_json", status_code=302)
    if bank_type in ("personalpay", "astropay"):
        proxy_url = _proxy_from_parts(proxy_host, proxy_port, proxy_user, proxy_password, proxy_type, proxy_raw)
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
    if bank_type == "personalpay" and not credentials.get("pin_hash"):
        credentials["pin_hash"] = PP_DEFAULT_PIN_HASH
    if not label.strip():
        label = f"{BANK_TYPES[bank_type]['name']} — {bank_type}"
    new_id = db_add_account(bank_type, label.strip(), credentials, window=window)
    return RedirectResponse(url=f"/?account_id={new_id}", status_code=302)


@app.get("/account/{account_id}/edit", response_class=HTMLResponse)
async def edit_account_page(request: Request, account_id: int):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    groups = accounts_by_window()
    return templates.TemplateResponse("edit_account.html", {
        "request":      request,
        "account":      acc,
        "window_list":  get_window_list(),
        "accounts":     list_accounts(),
        "groups":       groups,
        "account_id":   None,
        "selected":     acc,
        "window_slug":  None,
        "current_user": _current_user(request),
        **_proxy_parts_from_credentials(acc.get("credentials") or {}),
    })


@app.post("/account/{account_id}/edit", response_class=RedirectResponse)
async def edit_account_post(
    account_id:       int,
    label:            str = Form(...),
    credentials_json: str = Form("{}"),
    proxy_type:       str = Form("socks5h"),
    proxy_host:       str = Form(""),
    proxy_port:       str = Form(""),
    proxy_user:       str = Form(""),
    proxy_password:   str = Form(""),
    proxy_raw:        str = Form(""),
    window:           str = Form(""),
    window_custom:    str = Form(""),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    credentials = _parse_credentials(credentials_json)
    if not credentials and credentials_json.strip():
        return RedirectResponse(url=f"/account/{account_id}/edit?error=invalid_json", status_code=302)
    if acc["bank_type"] in ("personalpay", "astropay"):
        proxy_url = _proxy_from_parts(proxy_host, proxy_port, proxy_user, proxy_password, proxy_type, proxy_raw)
        if not proxy_url:
            # Если в форме прокси не указан — сохраняем старый из credentials
            # (защита от случайной потери прокси при обновлении токена)
            old_creds = acc.get("credentials") or {}
            proxy_url = (
                old_creds.get("https_proxy")
                or old_creds.get("http_proxy")
                or old_creds.get("proxy")
                or ""
            )
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
    if acc["bank_type"] == "personalpay" and credentials and not credentials.get("pin_hash"):
        credentials["pin_hash"] = PP_DEFAULT_PIN_HASH
    if label.strip():
        db_update_account(account_id, label=label.strip())
    if credentials:
        db_update_account(account_id, credentials=credentials)
    db_update_account(account_id, window=normalize_window_slug(window_custom or window or acc.get("window") or "glazars"))
    return RedirectResponse(url=f"/?account_id={account_id}&success=updated", status_code=302)


@app.post("/account/{account_id}/delete", response_class=RedirectResponse)
async def delete_account(account_id: int, redirect_window: Optional[str] = Form(None)):
    db_delete_account(account_id)
    if redirect_window:
        return RedirectResponse(url=f"/?window={normalize_window_slug(redirect_window)}", status_code=302)
    return RedirectResponse(url="/", status_code=302)


# ---------------------------------------------------------------------------
# Чек и discover
# ---------------------------------------------------------------------------

@app.get("/account/{account_id}/receipt", response_class=HTMLResponse)
async def receipt(request: Request, account_id: int, transaction_id: str = ""):
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay" or not transaction_id.strip():
        return RedirectResponse(url=f"/?account_id={account_id}", status_code=302)

    groups = accounts_by_window()
    base_ctx = {
        "request": request, "account": acc,
        "transaction_id": transaction_id,
        "groups": groups, "window_list": get_window_list(), "window_slug": None,
        "current_user": _current_user(request),
    }

    try:
        data = pp_transference_details(acc["credentials"], transaction_id)
    except Exception as e:
        return templates.TemplateResponse("receipt.html", {**base_ctx, "error": str(e), "transference": None})

    transference = (data.get("transference") or data) if isinstance(data, dict) else None
    receipt_lines = []
    label_ru = {
        "fecha": "Дата", "date": "Дата", "monto": "Сумма", "amount": "Сумма",
        "estado": "Статус", "status": "Статус",
        "remitente": "Отправитель", "titular": "Отправитель", "origen": "Отправитель",
        "sender": "Отправитель", "cuenta origen": "Отправитель",
        "nombre": "Имя получателя", "name": "Имя получателя",
        "destinatario": "Получатель", "recipient": "Получатель", "beneficiario": "Получатель",
        "cuit": "CUIT получателя", "cvu": "CVU получателя",
        "banco": "Банк получателя", "bank": "Банк получателя",
        "banco origen": "Банк отправителя", "banco remitente": "Банк отправителя",
        "id": "ID", "balance": "Баланс",
    }
    skip_labels = {"utr"}
    seen_recipient = None

    if transference and isinstance(transference, dict):
        for d in (transference.get("details") or []):
            if not isinstance(d, dict):
                continue
            label = (d.get("label") or d.get("key") or "").strip().lower()
            value = d.get("value") or d.get("displayValue") or ""
            if label in skip_labels:
                continue
            label_show = label_ru.get(label) or (d.get("label") or d.get("key") or "")
            if not (label_show or value):
                continue
            if label_show in ("Получатель", "Имя получателя") and str(value).strip() == str(seen_recipient or "").strip():
                continue
            if label_show in ("Получатель", "Имя получателя"):
                seen_recipient = value
            receipt_lines.append({"label": label_show or label, "value": value})

        tid = transference.get("id") or transaction_id
        if tid and not any(str(r.get("value") or "").strip() == str(tid).strip() for r in receipt_lines):
            receipt_lines.append({"label": "ID", "value": tid})

    tx_type = (transference or {}).get("transactionType") or ""
    is_outgoing = "output" in tx_type.lower() or "out" in tx_type.lower() or \
                  (transference or {}).get("title", "").lower().startswith("enviaste")
    amount_val = (transference or {}).get("amount")
    try:
        amount_num = float(amount_val) if amount_val is not None else None
    except (TypeError, ValueError):
        amount_num = None
    amount_display = (
        f"-{amount_val}" if (is_outgoing and amount_num is not None and amount_num > 0)
        else (amount_val if amount_val is not None else "")
    )

    return templates.TemplateResponse("receipt.html", {
        **base_ctx,
        "error":         None,
        "transference":  transference,
        "receipt_lines": receipt_lines,
        "receipt_title": "Исходящая транзакция" if is_outgoing else (transference or {}).get("title") or "Чек перевода",
        "amount_display":amount_display,
        "is_outgoing":   is_outgoing,
    })


@app.get("/account/{account_id}/discover")
async def discover(account_id: int, destination: str = ""):
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay" or not destination.strip():
        return JSONResponse({})
    try:
        data = discover_beneficiary(acc["bank_type"], acc["credentials"], destination)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)})


# ---------------------------------------------------------------------------
# Управление кабинетами (Windows) через интерфейс
# ---------------------------------------------------------------------------

@app.get("/windows", response_class=HTMLResponse)
async def windows_page(request: Request):
    """Страница управления кабинетами."""
    groups = accounts_by_window()
    return templates.TemplateResponse("windows.html", {
        "request":      request,
        "window_list":  get_window_list(),
        "groups":       groups,
        "accounts":     list_accounts(),
        "account_id":   None,
        "selected":     None,
        "window_slug":  None,
        "current_user": _current_user(request),
        "error":        request.query_params.get("error", ""),
        "success":      request.query_params.get("success", ""),
    })


@app.post("/windows/add", response_class=RedirectResponse)
async def windows_add(
    slug:  str = Form(""),
    title: str = Form(""),
):
    slug = normalize_window_slug(slug)
    title = title.strip()
    if not slug or not title:
        return RedirectResponse(url="/windows?error=empty_fields", status_code=302)
    if window_exists(slug):
        return RedirectResponse(url="/windows?error=already_exists", status_code=302)
    db_add_window(slug, title)
    return RedirectResponse(url=f"/windows?success=created&slug={slug}", status_code=302)


@app.post("/windows/{slug}/rename", response_class=RedirectResponse)
async def windows_rename(slug: str, title: str = Form("")):
    title = title.strip()
    if not title:
        return RedirectResponse(url="/windows?error=empty_title", status_code=302)
    db_update_window(slug, title)
    return RedirectResponse(url="/windows?success=renamed", status_code=302)


@app.post("/windows/{slug}/delete", response_class=RedirectResponse)
async def windows_delete(slug: str):
    groups = accounts_by_window()
    if groups.get(normalize_window_slug(slug)):
        return RedirectResponse(url="/windows?error=has_accounts", status_code=302)
    db_delete_window(slug)
    return RedirectResponse(url="/windows?success=deleted", status_code=302)


# ---------------------------------------------------------------------------
# Виджет курса USDT/ARS — Bybit P2P
# ---------------------------------------------------------------------------

# Кэш чтобы не спамить внешний API
_rate_cache: dict = {"ts": 0, "data": None}
_RATE_CACHE_TTL = 540  # 9 минут — обновление каждые 10 мин

async def _fetch_bybit_p2p() -> dict:
    """Получает топ-10 объявлений Bybit P2P USDT/ARS (buy + sell),
    считает среднюю цену по каждой стороне.

    Bybit P2P API:
      side=0 → объявления на ПОКУПКУ USDT (вы покупаете, мерчант продаёт) → BUY rate
      side=1 → объявления на ПРОДАЖУ USDT (вы продаёте, мерчант покупает) → SELL rate
    Обычно BUY > SELL (нормальный спред)."""

    _BYBIT_HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Origin":  "https://www.bybit.com",
        "Referer": "https://www.bybit.com/fiat/trade/otc/",
        "lang": "en",
    }

    async def fetch_side(side_code: str) -> list:
        """Возвращает список цен (float) для указанной стороны (0=buy, 1=sell)."""
        url = "https://api2.bybit.com/fiat/otc/item/online"
        payload = {
            "tokenId":    "USDT",
            "currencyId": "ARS",
            "payment":    [],
            "side":       side_code,   # строка "0" или "1"
            "size":       "10",
            "page":       "1",
            "amount":     "",
            "authMaker":  False,
            "canTrade":   False,
        }
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.post(url, json=payload, headers=_BYBIT_HEADERS)
            r.raise_for_status()
            data = r.json()

        # Проверяем ret_code (0 = успех у Bybit)
        ret_code = data.get("ret_code") or data.get("retCode") or 0
        if ret_code != 0:
            logger.warning("Bybit P2P side=%s ret_code=%s msg=%s",
                           side_code, ret_code, data.get("ret_msg") or data.get("retMsg"))
            return []

        items = (data.get("result") or {}).get("items") or []
        prices = []
        for item in items[:10]:
            try:
                # price бывает строкой или числом
                p = float(str(item.get("price") or item.get("Price") or 0).replace(",", "."))
                if p > 0:
                    prices.append(p)
            except Exception:
                pass
        logger.debug("Bybit P2P side=%s items=%d prices=%s", side_code, len(items), prices[:3])
        return prices

    # Запрашиваем обе стороны параллельно.
    # Bybit P2P: side="1" → мерчант ПРОДАЁТ USDT вам (вы покупаете, платите ARS) → BUY rate
    #            side="0" → мерчант ПОКУПАЕТ USDT у вас (вы продаёте, получаете ARS) → SELL rate
    # Норма: BUY > SELL (вы всегда платите больше, чем получаете).
    buy_prices, sell_prices = await asyncio.gather(
        fetch_side("1"),   # BUY  — вы покупаете USDT (мерчант продаёт)
        fetch_side("0"),   # SELL — вы продаёте USDT (мерчант покупает)
    )

    buy_avg  = round(statistics.mean(buy_prices),  2) if buy_prices  else None
    sell_avg = round(statistics.mean(sell_prices), 2) if sell_prices else None
    # best: самая дешёвая покупка / самая дорогая продажа
    buy_best  = min(buy_prices)  if buy_prices  else None
    sell_best = max(sell_prices) if sell_prices else None

    return {
        "buy_avg":    buy_avg,
        "sell_avg":   sell_avg,
        "buy_best":   buy_best,
        "sell_best":  sell_best,
        "buy_count":  len(buy_prices),
        "sell_count": len(sell_prices),
        "ts": int(time.time()),
    }


@app.get("/api/debug-pp-headers/{account_id}")
async def api_debug_pp_headers(account_id: int):
    """Диагностика: показывает точные заголовки которые сервер отправляет в PersonalPay."""
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay":
        return JSONResponse({"error": "PP account not found"}, status_code=404)
    try:
        from app.drivers.personalpay import _norm_creds, _base_headers, _paygilant_id, _get_token, _session
        c = _norm_creds(acc["credentials"])
        s = _session(c)
        token, paygilant = _get_token(s, c)
        headers = _base_headers(c)
        headers["Authorization"] = token[:20] + "…[TRUNCATED]"
        headers["x-fraud-paygilant-session-id"] = paygilant
        return JSONResponse({
            "headers_sent": headers,
            "device_id": c.get("device_id") or "(EMPTY — нет device_id в credentials!)",
            "app_version": c.get("app_version"),
            "user_agent": c.get("user_agent"),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/bybit-debug")
async def api_bybit_debug():
    """Диагностика: сырой ответ Bybit P2P для обеих сторон (buy/sell).
    Используй для проверки поля price и структуры ответа."""
    results = {}
    async with httpx.AsyncClient(timeout=12) as client:
        for side_code, label in [("0", "buy"), ("1", "sell")]:
            try:
                payload = {
                    "tokenId": "USDT", "currencyId": "ARS", "payment": [],
                    "side": side_code, "size": "5", "page": "1",
                    "amount": "", "authMaker": False, "canTrade": False,
                }
                headers = {
                    "Content-Type": "application/json", "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://www.bybit.com",
                    "Referer": "https://www.bybit.com/fiat/trade/otc/",
                    "lang": "en",
                }
                r = await client.post("https://api2.bybit.com/fiat/otc/item/online",
                                      json=payload, headers=headers)
                data = r.json()
                items = (data.get("result") or {}).get("items") or []
                results[label] = {
                    "status": r.status_code,
                    "ret_code": data.get("ret_code") or data.get("retCode"),
                    "item_count": len(items),
                    "first_3_prices": [
                        {"price": it.get("price"), "nickName": it.get("nickName")}
                        for it in items[:3]
                    ],
                }
            except Exception as e:
                results[label] = {"error": str(e)}
    return JSONResponse(results)


@app.get("/api/bybit-rate")
async def api_bybit_rate():
    """Возвращает текущий курс USDT/ARS с Bybit P2P.
    Кэшируется на 60 секунд."""
    global _rate_cache
    now = time.time()
    if _rate_cache["data"] and (now - _rate_cache["ts"]) < _RATE_CACHE_TTL:
        return JSONResponse({**_rate_cache["data"], "cached": True})
    try:
        data = await _fetch_bybit_p2p()
        _rate_cache = {"ts": now, "data": data}
        return JSONResponse({**data, "cached": False})
    except Exception as e:
        # Возвращаем кэш если есть, иначе ошибку
        if _rate_cache["data"]:
            return JSONResponse({**_rate_cache["data"], "cached": True, "stale": True})
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/rate-history")
async def api_rate_history(hours: int = 24):
    """История курса из БД за последние N часов (макс 48)."""
    hours = min(max(hours, 1), 48)
    hist = get_rate_history(hours)
    return JSONResponse({"history": hist, "count": len(hist)})


@app.get("/rates", response_class=HTMLResponse)
async def rates_page(request: Request):
    """Страница с полноценным графиком курса USDT/ARS."""
    groups = accounts_by_window()
    return templates.TemplateResponse("rates.html", {
        "request":      request,
        "window_list":  get_window_list(),
        "groups":       groups,
        "accounts":     list_accounts(),
        "account_id":   None,
        "selected":     None,
        "window_slug":  None,
        "current_user": _current_user(request),
    })


@app.get("/health")
async def health():
    return {"status": "ok"}
