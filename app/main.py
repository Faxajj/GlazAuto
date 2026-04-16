"""
Banks Dashboard — единый дашборд для нескольких банковских аккаунтов.
"""
import asyncio
import base64
import json
import logging
import secrets
import time
import traceback
from datetime import datetime
from typing import Optional, Tuple
from urllib.parse import quote

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

def _is_pp_internal_cvu(destination: str) -> bool:
    """Возвращает True если получатель — внутренняя карта Personal Pay (префикс 00000765).
    Для таких переводов лимиты выводов не применяются."""
    return (destination or "").strip().startswith("00000765")


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


def _normalize_activity(act: dict) -> dict:
    if not isinstance(act, dict):
        return {}
    attrs = act.get("attributes") or act
    aid = act.get("id") or act.get("transactionId") or attrs.get("id") or attrs.get("transactionId")
    title = (
        attrs.get("title") or attrs.get("description")
        or act.get("title") or act.get("description")
        or str(aid or "Операция")
    )
    receipt_id = _find_32char_hex_id(act)

    amount = attrs.get("amount") or attrs.get("monto") or act.get("amount") or act.get("monto")
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

    tx_type = (
        attrs.get("transactionType") or attrs.get("type")
        or act.get("transactionType") or act.get("type") or ""
    ).lower()
    title_lower = (title or "").lower()
    is_outgoing = (
        "output" in tx_type or "out" in tx_type or "outgoing" in tx_type
        or "enviaste" in title_lower or "envío" in title_lower
        or "transferencia enviada" in title_lower
    )

    inner = act.get("transference") if isinstance(act.get("transference"), dict) else act

    sender = (
        attrs.get("remitente") or attrs.get("sender") or attrs.get("originName") or attrs.get("senderName")
        or act.get("remitente") or act.get("sender") or act.get("from")
        or _find_in_details(act, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _find_in_details(inner, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _get_nested(inner, "transactionData", "origin", "holder")
        or _find_in_dict(act, "remitente", "sender", "originName", "holder")
        or _find_in_dict(inner, "remitente", "sender", "originName", "holder")
    )
    recipient = (
        attrs.get("destinatario") or attrs.get("recipient") or attrs.get("recipientName")
        or act.get("destinatario") or act.get("recipient") or act.get("to")
        or _find_in_details(act, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _find_in_details(inner, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _get_nested(inner, "transactionData", "destination", "holder")
        or _get_nested(inner, "transactionData", "destination", "label")
        or _find_in_dict(act, "destinatario", "recipient", "beneficiary", "label", "holder")
        or _find_in_dict(inner, "destinatario", "recipient", "beneficiary", "label", "holder")
    )
    sender    = sender.strip()    if isinstance(sender, str)    else None
    recipient = recipient.strip() if isinstance(recipient, str) else None

    return {
        "id": aid, "title": title, "receipt_id": receipt_id,
        "amount": amount, "date_str": date_str,
        "is_outgoing": is_outgoing, "sender": sender, "recipient": recipient,
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

    # Лимиты не применяются для внутренних карт PP (префикс 00000765)
    cvu = rule.get("cvu", "")
    if not _is_pp_internal_cvu(cvu):
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
            raise ValueError("Автовывод поддерживается только для Personal Pay")
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
        return info
    except Exception as e:
        err = str(e)
        low = err.lower()
        code = "bank_unavailable"
        if any(x in low for x in ("proxyerror", "proxy", "tunnel connection failed", "cannot connect to proxy")):
            code = "bank_unavailable"
        elif any(x in low for x in ("401", "403", "token", "unauthorized", "forbidden", "jwt")):
            code = "token_expired"
        return JSONResponse({
            "error": err,
            "error_meta": _error_payload(
                code,
                details=err,
                suggestion=(
                    "Откройте настройки карты и обновите токен, затем повторите обновление баланса."
                    if code == "token_expired"
                    else "Проверьте настройки прокси/сети и повторите через 1–2 минуты."
                ),
            ),
        }, status_code=500)


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


@app.get("/account/{account_id}/activities")
async def api_activities(account_id: int):
    """Lazy-load: история операций. Вызывается из JS после рендера страницы."""
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)
    if acc["bank_type"] != "personalpay":
        return JSONResponse({"activities": []})
    try:
        data = await asyncio.to_thread(pp_activities_list, acc["credentials"], 0, 30)
        raw = _extract_activities_raw(data)
        activities = []
        for act in (raw if isinstance(raw, list) else []):
            n = _normalize_activity(act)
            if n:
                is_outgoing = bool(n.get("is_outgoing"))
                full_name = (n.get("recipient") if is_outgoing else n.get("sender")) or "Не указано"
                activities.append({
                    "id": n.get("id"),
                    "title": n.get("title"),
                    "receipt_id": n.get("receipt_id"),
                    "amount": n.get("amount"),
                    "date_str": n.get("date_str"),
                    "is_outgoing": is_outgoing,
                    "type": "outgoing" if is_outgoing else "incoming",
                    "sender": n.get("sender"),
                    "recipient": n.get("recipient"),
                    "full_name": full_name,
                })
        return JSONResponse({"activities": activities})
    except Exception as e:
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
        # Лимиты не применяются для внутренних карт PP (префикс 00000765)
        if not _is_pp_internal_cvu(destination):
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

    # Лимиты не применяются для внутренних карт PP (префикс 00000765)
    if not _is_pp_internal_cvu(dest):
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
    if acc["bank_type"] == "personalpay" and isinstance(result, dict):
        tid = _find_32char_hex_id(result)
        if not tid:
            raw = (
                result.get("transactionId") or result.get("id")
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
    try:
        min_bal = float((min_balance or "0").replace(" ", "").replace(".", "").replace(",", "."))
    except ValueError:
        min_bal = 0.0
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
    if bank_type == "personalpay":
        proxy_url = _proxy_from_parts(proxy_host, proxy_port, proxy_user, proxy_password, proxy_type, proxy_raw)
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
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
    if acc["bank_type"] == "personalpay":
        proxy_url = _proxy_from_parts(proxy_host, proxy_port, proxy_user, proxy_password, proxy_type, proxy_raw)
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
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
    amount_display = (
        f"-{amount_val}" if (is_outgoing and amount_val is not None and float(amount_val) > 0)
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


@app.get("/account/{account_id}/discover", response_class=HTMLResponse)
async def discover(request: Request, account_id: int, destination: str = ""):
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay" or not destination.strip():
        return HTMLResponse(content="{}", media_type="application/json")
    try:
        data = discover_beneficiary(acc["bank_type"], acc["credentials"], destination)
        return HTMLResponse(content=json.dumps(data, ensure_ascii=False), media_type="application/json")
    except Exception as e:
        return HTMLResponse(content=json.dumps({"error": str(e)}), media_type="application/json")


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
_RATE_CACHE_TTL = 60  # секунд

async def _fetch_bybit_p2p() -> dict:
    """Получает топ-10 объявлений Bybit P2P USDT/ARS (buy + sell),
    считает среднюю цену по каждой стороне."""
    import httpx, statistics

    async def fetch_side(side: str) -> list:
        url = "https://api2.bybit.com/fiat/otc/item/online"
        payload = {
            "tokenId": "USDT",
            "currencyId": "ARS",
            "payment": [],
            "side": "0" if side == "buy" else "1",  # 0=buy, 1=sell
            "size": "10",
            "page": "1",
        }
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
        items = (data.get("result") or {}).get("items") or []
        prices = []
        for item in items[:10]:
            try:
                prices.append(float(item["price"]))
            except Exception:
                pass
        return prices

    buy_prices, sell_prices = await asyncio.gather(
        fetch_side("buy"),
        fetch_side("sell"),
    )

    buy_avg  = round(statistics.mean(buy_prices),  2) if buy_prices  else None
    sell_avg = round(statistics.mean(sell_prices), 2) if sell_prices else None
    buy_best  = min(buy_prices)  if buy_prices  else None
    sell_best = max(sell_prices) if sell_prices else None

    return {
        "buy_avg":   buy_avg,
        "sell_avg":  sell_avg,
        "buy_best":  buy_best,
        "sell_best": sell_best,
        "buy_count":  len(buy_prices),
        "sell_count": len(sell_prices),
        "ts": int(time.time()),
    }


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


@app.get("/health")
async def health():
    return {"status": "ok"}
