"""
Banks Dashboard — единый дашборд для нескольких банковских аккаунтов.
"""
import asyncio
import base64
import collections
import hashlib
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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

from app.database import (
    DAILY_WITHDRAW_LIMIT,
    GROUP_A,
    GROUP_B,
    get_window_list,
    normalize_window_slug,
    accounts_by_window,
    add_account as db_add_account,
    add_auto_withdraw_rule,
    cleanup_sessions,
    cleanup_withdraw_attempts,
    get_account_state,
    list_uncertain_withdraw_attempts,
    update_account_state,
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
    get_withdraw_attempt,
    get_withdraw_count,
    increment_account_withdraw_count,
    increment_withdraw_count,
    init_db,
    is_account_withdraw_limit_reached,
    is_withdraw_limit_reached,
    list_accounts,
    list_auto_withdraw_rules,
    release_account_withdraw_count,
    release_withdraw_count,
    try_create_withdraw_attempt,
    try_reserve_account_withdraw_count,
    try_reserve_withdraw_count,
    update_account as db_update_account,
    update_auto_withdraw_progress,
    update_withdraw_attempt_status,
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
from app.audit import audit
from app.database import (
    cleanup_audit_log,
    list_proxies as db_list_proxies,
    get_proxy as db_get_proxy,
    add_proxy as db_add_proxy,
    update_proxy as db_update_proxy,
    delete_proxy as db_delete_proxy,
    mark_proxy_status as db_mark_proxy_status,
    set_balance_cache as db_set_balance_cache,
    get_balance_cache as db_get_balance_cache,
    get_balance_cache_batch as db_get_balance_cache_batch,
    delete_balance_cache as db_delete_balance_cache,
)
from app.proxies import (
    proxy_url as build_proxy_url,
    health_check_one as proxy_health_check_one,
    HEALTH_CHECK_INTERVAL as PROXY_HEALTH_INTERVAL,
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
    "limit_reached": f"❌ Достигнут дневной лимит ({DAILY_WITHDRAW_LIMIT} выводов на CVU). Лимит обновляется ежедневно в 07:30 МСК.",
    "account_limit_reached": f"❌ Карта достигла лимита выводов ({DAILY_WITHDRAW_LIMIT} в день на группу). Лимит обновляется ежедневно в 07:30 МСК.",
    "group_a_limit_reached": f"❌ Лимит для PP-внутренних переводов ({DAILY_WITHDRAW_LIMIT}/день) достигнут. Сброс в 07:30 МСК.",
    "group_b_limit_reached": f"❌ Лимит для внешних переводов ({DAILY_WITHDRAW_LIMIT}/день) достигнут. Сброс в 07:30 МСК.",
    "duplicate_withdraw":   "❌ Этот вывод уже был отправлен. Проверьте историю операций.",
    "withdraw_in_progress": "⏳ Этот вывод сейчас обрабатывается. Подождите.",
    "retry_after_minute":   "⏳ Предыдущая попытка ждёт ответа банка. Подождите 1 минуту и повторите — система автоматически разрулит.",
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


def _timeago(unix_ts) -> str:
    """Человекочитаемое «N сек/мин/ч/дн назад» для unix-таймштампа."""
    try:
        ts = int(unix_ts)
    except (TypeError, ValueError):
        return "—"
    if ts <= 0:
        return "—"
    delta = int(time.time()) - ts
    if delta < 0:
        return "в будущем"
    if delta < 60:
        return f"{delta} сек назад"
    if delta < 3600:
        return f"{delta // 60} мин назад"
    if delta < 86400:
        return f"{delta // 3600} ч назад"
    return f"{delta // 86400} дн назад"


templates.env.filters["timeago"] = _timeago


def _cached_balance_for_template(account_id) -> Optional[float]:
    """Jinja-helper: возвращает закешированный баланс для server-render.
    Читает из shared SQLite-кэша → виден всем gunicorn-воркерам."""
    try:
        acc_id = int(account_id)
        cached = db_get_balance_cache(acc_id)
        if cached and not cached.get("is_error"):
            data = cached.get("data") or {}
            bal = data.get("balance")
            return float(bal) if bal is not None else None
    except (TypeError, ValueError):
        pass
    return None


templates.env.globals["cached_balance"] = _cached_balance_for_template


# ---------------------------------------------------------------------------
# Security headers — добавляются ко ВСЕМ ответам
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers.update({
        "X-Frame-Options":        "DENY",
        "X-Content-Type-Options": "nosniff",
        "X-XSS-Protection":       "1; mode=block",
        "Referrer-Policy":        "strict-origin-when-cross-origin",
        "Permissions-Policy":     "camera=(), microphone=(), geolocation=()",
        # Inline scripts/styles в шаблонах требуют unsafe-inline.
        # blob: нужен для печати чека (_ppPrintReceipt).
        "Content-Security-Policy": (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: blob:; "
            "connect-src 'self' https://api2.bybit.com; "
            "worker-src blob:; "
            "frame-ancestors 'none'; "
            "object-src 'none'; "
            "base-uri 'self';"
        ),
    })
    return response


# ---------------------------------------------------------------------------
# CSRF protection — double-submit cookie pattern
# ---------------------------------------------------------------------------
# Защита от Cross-Site Request Forgery: на любой unsafe-метод (POST/PUT/PATCH/DELETE)
# проверяется совпадение значения в cookie 'csrf_token' и в одном из:
#   - заголовок X-CSRF-Token  (для fetch/JSON-запросов)
#   - поле формы csrf_token   (для HTML-форм)
#
# Cookie выставляется на любой первый GET. JS читает cookie (httponly=False)
# и передаёт в header. HTML-формы получают hidden input через Jinja-helper csrf_input(request).
#
# Exempt endpoints:
#   - /login, /logout (auth boundary)
#   - /static/* (статические файлы, GET)
#   - /events/* (SSE — только GET)

CSRF_COOKIE     = "csrf_token"
CSRF_HEADER     = "x-csrf-token"
CSRF_FORM_FIELD = "csrf_token"
CSRF_COOKIE_TTL = 60 * 60 * 24 * 30   # 30 дней
_CSRF_EXEMPT_PREFIXES = ("/static/", "/events/")
_CSRF_EXEMPT_PATHS    = {"/login"}   # /logout — НЕ exempt (защита от force-logout CSRF)
_CSRF_UNSAFE_METHODS  = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_CSRF_MAX_BODY_BYTES  = 5 * 1024 * 1024   # 5 MB — defence в DoS на парсинге


def _csrf_is_exempt(path: str) -> bool:
    if path in _CSRF_EXEMPT_PATHS:
        return True
    return any(path.startswith(p) for p in _CSRF_EXEMPT_PREFIXES)


async def _csrf_extract_submitted(request: Request, body: bytes) -> str:
    """Достаёт CSRF-токен из заголовка ИЛИ тела (urlencoded form / JSON)."""
    # 1) Header (предпочтительно — для JS fetch и AJAX)
    submitted = request.headers.get(CSRF_HEADER, "").strip()
    if submitted:
        return submitted
    # 2) Body — парсим в зависимости от Content-Type
    ct = request.headers.get("content-type", "").lower()
    if not body:
        return ""
    if "application/json" in ct:
        try:
            data = json.loads(body.decode("utf-8", errors="ignore"))
            if isinstance(data, dict):
                v = data.get(CSRF_FORM_FIELD) or data.get("csrfToken") or ""
                return str(v).strip()
        except Exception:
            return ""
    if "application/x-www-form-urlencoded" in ct:
        try:
            from urllib.parse import parse_qs
            decoded = body.decode("utf-8", errors="ignore")
            parts = parse_qs(decoded, keep_blank_values=False)
            return (parts.get(CSRF_FORM_FIELD, [""])[0] or "").strip()
        except Exception:
            return ""
    if "multipart/form-data" in ct:
        # Простой grep по boundary'у — без полного парсинга
        try:
            text = body.decode("utf-8", errors="ignore")
            # Ищем `name="csrf_token"\r\n\r\nVALUE\r\n--`
            import re
            m = re.search(r'name="' + CSRF_FORM_FIELD + r'"\s*\r?\n\r?\n([^\r\n]+)', text)
            if m:
                return m.group(1).strip()
        except Exception:
            return ""
    return ""


@app.middleware("http")
async def _csrf_middleware(request: Request, call_next):
    path = request.url.path
    method = request.method.upper()

    # Token: либо из cookie (если уже есть), либо генерируем новый
    token_from_cookie = request.cookies.get(CSRF_COOKIE, "")
    token = token_from_cookie or secrets.token_hex(32)
    request.state.csrf_token = token   # для Jinja-helper

    # Validate на unsafe методах
    if method in _CSRF_UNSAFE_METHODS and not _csrf_is_exempt(path):
        # Защита от DoS: не читаем тело больше лимита
        cl = request.headers.get("content-length", "")
        try:
            cl_int = int(cl) if cl else 0
        except ValueError:
            cl_int = 0
        if cl_int > _CSRF_MAX_BODY_BYTES:
            return JSONResponse({"error": "request body too large"}, status_code=413)

        # Читаем тело и кешируем — иначе downstream Form() не сможет прочесть
        body = await request.body()

        # Reinjection: подменяем receive чтобы FastAPI Form()/json() читали из кэша
        async def _replay_receive():
            return {"type": "http.request", "body": body, "more_body": False}
        request._receive = _replay_receive   # type: ignore[attr-defined]

        submitted = await _csrf_extract_submitted(request, body)
        if not token_from_cookie or not submitted or submitted != token_from_cookie:
            logger.warning(
                "csrf reject: path=%s ip=%s has_cookie=%s has_submitted=%s",
                path, _get_client_ip(request) if "_get_client_ip" in globals() else "?",
                bool(token_from_cookie), bool(submitted),
            )
            # Для AJAX запросов — JSON, для form-submit — HTML с инструкцией
            if "application/json" in request.headers.get("accept", "") \
               or "x-requested-with" in {k.lower() for k in request.headers.keys()}:
                return JSONResponse({"error": "csrf token invalid"}, status_code=403)
            html = ("<!DOCTYPE html><html><body style='font-family:sans-serif;padding:2rem'>"
                    "<h2>403 — Сессия устарела</h2>"
                    "<p>Защита от CSRF: токен не совпал. Это часто происходит если страница "
                    "была открыта до перезапуска сервера или истёк cookie.</p>"
                    "<p><a href='javascript:history.back()'>← Назад</a> | "
                    "<a href='/'>На главную</a></p></body></html>")
            return HTMLResponse(html, status_code=403)

    response = await call_next(request)

    # Всегда обновляем cookie (rolling expiration). Для безопасности:
    #   httponly=False — нужно чтобы JS читал и отправлял в header
    #   samesite=lax — соответствует session_token
    #   path=/ — на весь сайт
    response.set_cookie(
        CSRF_COOKIE, token,
        httponly=False,
        samesite="lax",
        path="/",
        max_age=CSRF_COOKIE_TTL,
    )
    return response


# Jinja helper: вставляет hidden input в любую форму через {{ csrf_input(request) }}
def _csrf_input(request: Request) -> str:
    token = getattr(request.state, "csrf_token", "") or request.cookies.get(CSRF_COOKIE, "")
    return f'<input type="hidden" name="{CSRF_FORM_FIELD}" value="{token}">'


def _csrf_meta(request: Request) -> str:
    """Meta-tag для JS fetch: <meta name='csrf-token' content='...'>"""
    token = getattr(request.state, "csrf_token", "") or request.cookies.get(CSRF_COOKIE, "")
    return f'<meta name="csrf-token" content="{token}">'


# Регистрируем Jinja-globals (нужен MarkupSafe чтобы HTML не эскейпился)
from markupsafe import Markup as _Markup
templates.env.globals["csrf_input"] = lambda request: _Markup(_csrf_input(request))
templates.env.globals["csrf_meta"]  = lambda request: _Markup(_csrf_meta(request))


# ---------------------------------------------------------------------------
# Login rate limiting — защита от брутфорса (5 попыток / 15 мин на IP)
# ---------------------------------------------------------------------------

_login_attempts: dict = collections.defaultdict(list)
_LOGIN_MAX_ATTEMPTS     = 5
_LOGIN_LOCKOUT_SECONDS  = 900   # 15 минут


def _get_client_ip(request: Request) -> str:
    """Возвращает IP клиента с учётом reverse-proxy заголовков."""
    for header in ("X-Forwarded-For", "X-Real-IP", "CF-Connecting-IP"):
        val = request.headers.get(header, "").strip()
        if val:
            return val.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _is_login_rate_limited(ip: str) -> bool:
    now = time.time()
    cutoff = now - _LOGIN_LOCKOUT_SECONDS
    _login_attempts[ip] = [t for t in _login_attempts[ip] if t > cutoff]
    return len(_login_attempts[ip]) >= _LOGIN_MAX_ATTEMPTS


def _record_failed_login(ip: str) -> int:
    """Записывает неудачную попытку, возвращает текущее число попыток."""
    _login_attempts[ip].append(time.time())
    return len(_login_attempts[ip])


def _clear_login_attempts(ip: str) -> None:
    _login_attempts.pop(ip, None)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CVU-группировка (per-card лимиты) — LIMIT BYPASS RULES
# ---------------------------------------------------------------------------
# EXEMPT (None) — destinations НЕ ограничены лимитом 15/день, можно выводить
#                 неограниченно даже после исчерпания дневного счётчика:
#   00000765* — PP-internal (PP→PP)
#   00001775* — AP-internal (AP→AP)
#
# Group B (b)   — все остальные направления (внешние переводы), 15/день.
#
# Group A (a)   — оставлен в БД для обратной совместимости (count_a колонка),
#                 но новый код в группу A никого не помещает. Старые данные
#                 (если были) продолжают читаться через get_account_withdraw_count.
#
# Поведение: после 15 внешних выводов карта продолжает работать на 00000765/00001775
# адреса без блокировки. Это "limit bypass" функционал.
# ---------------------------------------------------------------------------

PP_INTERNAL_PREFIX = "00000765"   # PP→PP — exempt
AP_INTERNAL_PREFIX = "00001775"   # AP→AP — exempt


def _group_key_for(bank_type: str, destination: str) -> Optional[str]:
    """Определяет группу лимита для пары (банк-источник, направление).
    Returns:
        None  — exempt (нет лимита). Для PP-internal и AP-internal направлений.
        'b'   — Group B (стандартный 15-лимит/день/карта).
    """
    dest = (destination or "").strip()
    # Bypass: PP-internal и AP-internal — без лимитов
    if dest.startswith(PP_INTERNAL_PREFIX) or dest.startswith(AP_INTERNAL_PREFIX):
        return None
    # Все остальные — стандартный лимит Group B
    return GROUP_B


def _make_idempotency_key(account_id: int, destination: str, amount: float,
                          salt: str = "") -> str:
    """Детерминированный ключ идемпотентности для (карта, цель, сумма, бизнес-день).
    Один и тот же набор параметров в течение бизнес-дня → один ключ →
    повторный submit не создаёт второй вывод.

    salt позволяет искусственно различать legitimate-повторы (например, чанки
    auto-withdraw используют seq как salt).
    """
    from app.database import _msk_date_str as _bd
    business_date = _bd()
    raw = f"{account_id}|{(destination or '').strip()}|{float(amount):.2f}|{business_date}|{salt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Per-account asyncio locks — сериализуют операции над одной картой
# ---------------------------------------------------------------------------
# Защищают от: одновременного withdraw + balance refresh + keepalive на одной карте.
# Lock ключуется по account_id (НЕ device_id) — две карты на одном телефоне
# работают независимо, но одна и та же карта не может быть в двух операциях сразу.

_account_locks: dict = {}
_account_locks_meta = asyncio.Lock()


async def _get_account_lock(account_id: int) -> asyncio.Lock:
    async with _account_locks_meta:
        lock = _account_locks.get(account_id)
        if lock is None:
            lock = asyncio.Lock()
            _account_locks[account_id] = lock
        return lock


def _pick_least_loaded_healthy_proxy() -> Optional[dict]:
    """Auto-assignment: возвращает healthy proxy с минимальной текущей нагрузкой.
    Считаем «нагрузку» = количество аккаунтов где этот proxy URL уже прописан в creds.
    Возвращает dict-строку proxies или None если нет healthy."""
    try:
        pool = db_list_proxies(only_enabled=True, only_healthy=True)
        if not pool:
            return None
        # Считаем сколько аккаунтов используют каждый прокси
        all_accs = list_accounts()
        usage: dict = {}   # proxy_url → count
        for acc in all_accs:
            cr = acc.get("credentials") or {}
            url = (cr.get("proxy") or cr.get("https_proxy") or cr.get("http_proxy") or "").strip()
            if url:
                usage[url] = usage.get(url, 0) + 1
        # Сортируем pool по: latency ASC, usage ASC, fail_count ASC
        def _score(p):
            url = build_proxy_url(p)
            return (
                usage.get(url, 0),                       # меньше нагрузка = выше
                int(p.get("last_latency_ms") or 99999),  # быстрее = выше
                int(p.get("fail_count") or 0),
            )
        pool.sort(key=_score)
        return pool[0]
    except Exception as e:
        logger.debug("_pick_least_loaded_healthy_proxy error: %s", e)
        return None



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


def _jwt_expiry(credentials: dict, account_id: Optional[int] = None) -> Tuple[bool, Optional[float]]:
    """Возвращает (token_expired, hours_left).

    Учитывает ДВА источника:
    1) JWT payload exp — embedded в токене
    2) accounts_state.last_keepalive_at — успешный PIN refresh продлевает
       серверную сессию на ~12 часов даже без выдачи нового JWT.

    Если PIN refresh был недавно (< 12h), считаем сессию активной независимо
    от JWT exp — потому что HAR показал, что PP не выдаёт новый JWT в response.
    """
    token = (credentials.get("auth_token") or "").strip()
    if token.upper().startswith("BEARER "):
        token = token[7:].strip()

    # JWT exp
    jwt_hours_left: Optional[float] = None
    if token and token.startswith("eyJ"):
        parts = token.split(".")
        if len(parts) >= 2:
            try:
                pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
                payload = json.loads(base64.urlsafe_b64decode(pad))
                exp = payload.get("exp")
                if exp:
                    jwt_hours_left = max(0.0, (float(exp) - time.time()) / 3600.0)
            except Exception:
                pass

    # accounts_state — серверная сессия после PIN refresh жива ~12 часов
    server_session_hours_left: Optional[float] = None
    if account_id is not None:
        try:
            state = get_account_state(account_id)
            last_ka = int(state.get("last_keepalive_at") or 0)
            if last_ka > 0:
                age_hours = (time.time() - last_ka) / 3600.0
                # Серверная сессия после успешного PIN refresh держится ~12 часов
                server_session_hours_left = max(0.0, 12.0 - age_hours)
        except Exception:
            pass

    # Эффективное оставшееся время = max из двух источников
    candidates = [v for v in (jwt_hours_left, server_session_hours_left) if v is not None]
    if not candidates:
        return False, None
    effective_hours_left = max(candidates)
    expired = effective_hours_left <= 0
    return expired, effective_hours_left


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
# GZip-сжатие для всех ответов >500 байт — экономит трафик на JSON и HTML
# в 4-7 раз. CPU-overhead минимален (несколько мс на typical response).
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=5)


# ---------------------------------------------------------------------------
# In-memory кэш баланса / активностей (stale-while-revalidate)
# ---------------------------------------------------------------------------
# Логика: при запросе отдаём кэш мгновенно, а обновление делаем в фоне.
#   • age < TTL   → отдать кэш, ничего не делать (данные свежие)
#   • TTL ≤ age < STALE → отдать кэш + запустить фоновое обновление
#   • age ≥ STALE → кэш слишком старый, ждём свежих данных синхронно
# Результат: первый запрос после старта сервера — синхронный (одноразовый «холодный» старт).
# Все последующие запросы — мгновенные из кэша, а кэш незаметно обновляется в фоне.

# SQLite-backed shared cache между gunicorn-воркерами. API совместим с dict
# (поддерживает .get/.pop/__setitem__/__getitem__/__contains__) — не нужно
# переписывать все 18 мест использования _balance_cache.X в main.py.
class _SharedBalanceCache:
    def get(self, key, default=None):
        v = db_get_balance_cache(int(key))
        return v if v is not None else default
    def __getitem__(self, key):
        v = db_get_balance_cache(int(key))
        if v is None:
            raise KeyError(key)
        return v
    def __setitem__(self, key, value):
        if not isinstance(value, dict):
            return
        db_set_balance_cache(
            int(key),
            value.get("data") or {},
            is_error=bool(value.get("is_error")),
            last_refresh_error=value.get("last_refresh_error"),
        )
    def __contains__(self, key):
        return db_get_balance_cache(int(key)) is not None
    def pop(self, key, default=None):
        v = db_get_balance_cache(int(key))
        db_delete_balance_cache(int(key))
        return v if v is not None else default
    def get_batch(self, keys):
        return db_get_balance_cache_batch([int(k) for k in keys])

_balance_cache = _SharedBalanceCache()
_balance_cache_legacy:    dict = {}   # legacy slot, не используется
_activities_cache: dict = {}   # {account_id: {"acts": list, "ts": float}}
_bg_refreshing_bal: set = set()  # id аккаунтов с активным фоновым обновлением баланса
_bg_refreshing_act: set = set()  # id аккаунтов с активным фоновым обновлением активностей

_BALANCE_TTL    = 25      # сек — кэш «свежий», не обновлять
_BALANCE_STALE  = 1800    # сек (30 мин) — кэш «устаревший», но ещё годный для отдачи
                          # Большое окно нужно потому что при падающем банке нет
                          # альтернативы — лучше показать 5-минутный кэш чем «Ошибка»
_ACTIVITIES_TTL   = 60
_ACTIVITIES_STALE = 1800  # 30 мин — та же логика для истории


async def _fetch_and_cache_balance(account_id: int, acc: dict) -> dict:
    """Делает реальный API-запрос за балансом и сохраняет результат в кэш.
    Возвращает dict (без account_withdraw_count/limit — они добавляются при отдаче)."""
    raw  = await asyncio.to_thread(driver_balance, acc["bank_type"], acc["credentials"])
    info = _normalize_balance(raw, acc["bank_type"])
    info.pop("raw_accounts", None)

    if acc["bank_type"] == "personalpay":
        from app.drivers.personalpay import get_owner_name_from_jwt, get_cvu_info
        creds = acc["credentials"]

        cached_cvu   = creds.get("_cvu_number") or ""
        cached_alias = creds.get("_cvu_alias")  or ""
        cached_owner = creds.get("_owner_name") or ""

        if cached_cvu:
            info["cvu_number"] = cached_cvu
        if cached_alias:
            info["cvu_alias"] = cached_alias

        owner = get_owner_name_from_jwt(creds.get("auth_token", "")) or cached_owner

        if not cached_cvu:
            try:
                cvu_data   = await asyncio.to_thread(get_cvu_info, creds)
                real_cvu   = (cvu_data.get("cvu") or cvu_data.get("number")
                              or cvu_data.get("cbu") or cvu_data.get("accountNumber") or "")
                real_alias = cvu_data.get("alias") or cvu_data.get("aliasId") or ""
                cvu_holder = (cvu_data.get("holder") or cvu_data.get("holderName")
                              or cvu_data.get("ownerName") or cvu_data.get("fullName")
                              or cvu_data.get("name") or "")
                if real_cvu:
                    info["cvu_number"] = real_cvu
                if real_alias:
                    info["cvu_alias"] = real_alias
                if not owner and cvu_holder:
                    owner = cvu_holder
                new_creds = dict(creds)
                if real_cvu:
                    new_creds["_cvu_number"] = real_cvu
                if real_alias:
                    new_creds["_cvu_alias"] = real_alias
                if owner:
                    new_creds["_owner_name"] = owner
                db_update_account(account_id, credentials=new_creds)
            except Exception as e:
                logger.debug("get_cvu_info failed (non-critical): %s", e)

        info["owner_name"] = str(owner).strip() if owner else ""

        # Авто-сохранение обновлённого токена (PIN авто-refresh)
        device_id = creds.get("device_id", "")
        new_tok = pp_consume_refreshed_token(device_id)
        if new_tok:
            new_creds = dict(creds)
            new_creds["auth_token"] = new_tok
            try:
                db_update_account(account_id, credentials=new_creds)
                info["token_auto_refreshed"] = True
            except Exception:
                pass

    # Sticky proxy-failover persistence: если драйвер переключился на pool-proxy
    # (credentials._failover_to_proxy выставлен), сохраняем новый прокси в БД
    # чтобы следующие вызовы не тратили время на мёртвый прокси
    creds_after = acc.get("credentials") or {}
    failover_to = creds_after.pop("_failover_to_proxy", None)
    if failover_to:
        fresh_acc = get_account(account_id)
        if fresh_acc:
            new_creds = dict(fresh_acc.get("credentials") or {})
            new_creds["proxy"] = failover_to
            new_creds["http_proxy"] = failover_to
            new_creds["https_proxy"] = failover_to
            db_update_account(account_id, credentials=new_creds)
            logger.info("proxy sticky failover for acc=%d → switched to pool proxy", account_id)

    _balance_cache[account_id] = {"data": info, "ts": time.time()}
    return info


async def _bg_refresh_balance(account_id: int) -> None:
    """Фоновое обновление кэша баланса (не блокирует HTTP-ответ).
    При ошибке записывает error-маркер в кэш — чтобы UI показал ⚠ вместо
    пустой плашки. Раньше эта ошибка молча терялась."""
    try:
        fresh_acc = get_account(account_id)
        if fresh_acc:
            await _fetch_and_cache_balance(account_id, fresh_acc)
            _sse_emit(f"account:{account_id}", {
                "type": "balance.updated", "account_id": account_id,
            })
            logger.debug("bg_refresh_balance ok acc=%d", account_id)
    except Exception as e:
        # Записываем ошибку в shared SQLite-кэш — все воркеры увидят is_error
        err_str = str(e)[:300]
        existing = db_get_balance_cache(account_id)
        if existing and not existing.get("is_error"):
            # Был успешный кэш — оставляем data, помечаем что последний refresh упал
            db_set_balance_cache(
                account_id,
                existing["data"],
                is_error=False,
                last_refresh_error=err_str,
            )
        else:
            db_set_balance_cache(
                account_id,
                {"error": err_str, "balance": None, "cvu_number": "", "cvu_alias": ""},
                is_error=True,
                last_refresh_error=err_str,
            )
        logger.debug("bg_refresh_balance error acc=%d: %s", account_id, e)
    finally:
        _bg_refreshing_bal.discard(account_id)


async def _fetch_and_cache_activities(account_id: int, acc: dict) -> list:
    """Делает реальный API-запрос за активностями и сохраняет в кэш.
    Возвращает нормализованный список dict (без фильтрации и пагинации)."""
    bank = acc["bank_type"]
    raw  = []
    if bank == "personalpay":
        data = await asyncio.to_thread(pp_activities_list, acc["credentials"], 0, 50)
        raw  = _extract_activities_raw(data)
        _new_tok = pp_consume_refreshed_token(acc["credentials"].get("device_id", ""))
        if _new_tok:
            try:
                _nc = dict(acc["credentials"])
                _nc["auth_token"] = _new_tok
                db_update_account(account_id, credentials=_nc)
            except Exception:
                pass
    elif bank == "astropay":
        from app.drivers.astropay import get_activities as ap_get_activities
        data = await asyncio.to_thread(ap_get_activities, acc["credentials"], 1, 50)
        raw  = data.get("data") or []

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

    _activities_cache[account_id] = {"acts": activities, "ts": time.time()}
    return activities


async def _bg_refresh_activities(account_id: int) -> None:
    """Фоновое обновление кэша активностей (не блокирует HTTP-ответ)."""
    try:
        fresh_acc = get_account(account_id)
        if fresh_acc:
            await _fetch_and_cache_activities(account_id, fresh_acc)
            logger.debug("bg_refresh_activities ok acc=%d", account_id)
    except Exception as e:
        logger.debug("bg_refresh_activities error acc=%d: %s", account_id, e)
    finally:
        _bg_refreshing_act.discard(account_id)


async def _proactive_refresh_loop() -> None:
    """Каждые 2 минуты прогревает кэш баланса для всех PP/AP аккаунтов.
    Благодаря этому кэш почти всегда свежий — пользователь получает данные мгновенно."""
    await asyncio.sleep(90)   # первый прогрев через 90 сек после старта
    while True:
        try:
            all_accs = list_accounts()
            for acc in all_accs:
                if acc.get("bank_type") not in ("personalpay", "astropay"):
                    continue
                acc_id = acc["id"]
                cached = _balance_cache.get(acc_id)
                age    = (time.time() - cached["ts"]) if cached else 9999
                if age > _BALANCE_TTL and acc_id not in _bg_refreshing_bal:
                    _bg_refreshing_bal.add(acc_id)
                    asyncio.create_task(_bg_refresh_balance(acc_id))
                    await asyncio.sleep(0.3)   # небольшая пауза между аккаунтами
        except Exception as e:
            logger.debug("proactive_refresh_loop error: %s", e)
        await asyncio.sleep(120)   # каждые 2 минуты


# ---------------------------------------------------------------------------
# Event bus + SSE — лёгкий real-time push без внешних зависимостей
# ---------------------------------------------------------------------------
# Topics: "account:{id}" — события одного аккаунта
# Events:
#   {"type": "balance.updated",     "ts": <unix>, "account_id": <int>}
#   {"type": "withdraw.completed",  "ts": <unix>, "account_id": <int>, "tid": "..."}
#   {"type": "token.refreshed",     "ts": <unix>, "account_id": <int>}
#
# Доставка at-most-once в рамках процесса (без Redis). Клиенты переподключаются
# по EventSource auto-reconnect при разрыве. Polling баланса остаётся как fallback.

_sse_subscribers: dict = collections.defaultdict(set)   # topic → set[asyncio.Queue]
_sse_lock = asyncio.Lock()
_SSE_QUEUE_MAXSIZE = 32        # больше — drop oldest


async def _sse_subscribe(topic: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=_SSE_QUEUE_MAXSIZE)
    async with _sse_lock:
        _sse_subscribers[topic].add(q)
    return q


async def _sse_unsubscribe(topic: str, q: asyncio.Queue) -> None:
    async with _sse_lock:
        _sse_subscribers[topic].discard(q)
        if not _sse_subscribers[topic]:
            _sse_subscribers.pop(topic, None)


def _sse_emit(topic: str, event: dict) -> None:
    """Не-блокирующая публикация события всем подписчикам топика.
    Если подписчик не успевает читать — drop oldest message в его очереди."""
    if not _sse_subscribers.get(topic):
        return
    payload = dict(event)
    payload.setdefault("ts", int(time.time()))
    for q in list(_sse_subscribers.get(topic, ())):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            # Подписчик завис — освобождаем место и дропаем самое старое
            try:
                q.get_nowait()
                q.put_nowait(payload)
            except Exception:
                pass


@app.get("/events/account/{account_id}")
async def sse_account_events(account_id: int, request: Request):
    """Server-Sent Events для одного аккаунта.
    Клиент подписывается через EventSource('/events/account/42')."""
    if not _current_user(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not get_account(account_id):
        return JSONResponse({"error": "account not found"}, status_code=404)

    topic = f"account:{account_id}"
    queue = await _sse_subscribe(topic)

    async def _gen():
        try:
            # Initial hello — клиент знает что соединение установлено
            yield f"event: hello\ndata: {json.dumps({'account_id': account_id, 'ts': int(time.time())})}\n\n"
            # Heartbeat каждые 25 сек чтобы прокси/nginx не закрыли idle-connection
            last_heartbeat = time.monotonic()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=5.0)
                    yield f"event: message\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    # Timeout — проверяем нужен ли heartbeat
                    if time.monotonic() - last_heartbeat >= 25.0:
                        yield f": heartbeat {int(time.time())}\n\n"
                        last_heartbeat = time.monotonic()
        finally:
            await _sse_unsubscribe(topic, queue)

    headers = {
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",   # отключает буферизацию nginx
        "Connection": "keep-alive",
    }
    return StreamingResponse(_gen(), media_type="text/event-stream", headers=headers)


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

async def _pp_keepalive_loop():
    """Фоновая задача: каждые 20 минут проверяет все PP аккаунты с pin_hash.
    Если прошло > 5 часов с последнего PIN-refresh — делает его автоматически.
    Интервал 5ч при 12-часовом JWT даёт ~2 refresh'а в окне жизни токена с запасом.

    Состояние last_keepalive_at теперь персистентно (таблица accounts_state) —
    переживает рестарты, не дудолбит банк после каждого перезапуска uvicorn.
    """
    await asyncio.sleep(60)   # первый запуск через 1 мин после старта
    PP_INTERVAL    = 5 * 3600  # 5 часов между keep-alive (было 8h — слишком мало запаса)
    CHECK_INTERVAL = 1200      # проверяем раз в 20 минут (было 30 мин)

    while True:
        try:
            from app.drivers.personalpay import refresh_session_with_pin, _pp_jwt_exp
            all_accs = list_accounts()
            pp_accs = [a for a in all_accs if a.get("bank_type") == "personalpay"
                       and (a.get("credentials") or {}).get("pin_hash")]
            now = time.time()
            for acc in pp_accs:
                acc_id = acc["id"]
                # Читаем состояние из БД (переживает рестарты)
                state = get_account_state(acc_id)
                last  = int(state.get("last_keepalive_at") or 0)

                creds = acc.get("credentials") or {}
                token = creds.get("auth_token", "")
                exp   = _pp_jwt_exp(token) if token else None

                # Токен истёк — пробуем обновить независимо от PP_INTERVAL
                # (PP сервер может принять PIN даже на недавно истёкший токен)
                token_needs_refresh = exp is not None and exp <= now + 300  # истёк или < 5 мин
                interval_passed = now - last >= PP_INTERVAL

                if not token_needs_refresh and not interval_passed:
                    continue   # рано, токен ещё свежий

                # Пропускаем только если токен мёртв давно (>8ч) И интервал ещё не прошёл
                if exp and exp < now - 8 * 3600 and not interval_passed:
                    logger.debug("pp keepalive skip acc=%d: token expired >8h ago, interval not passed", acc_id)
                    continue

                try:
                    new_token = await asyncio.to_thread(refresh_session_with_pin, creds)
                    # HAR показал: PP в pin/validate ответе НЕ выдаёт новый JWT.
                    # Успех = серверная сессия продлена, существующий JWT валиден.
                    # Записываем last_keepalive_at — UI использует его чтобы показать
                    # «Сессия активна» вместо «0 минут» (даже если JWT exp в прошлом).
                    if new_token:
                        update_account_state(
                            acc_id,
                            last_keepalive_at=int(now),
                            last_token_state="ALIVE",
                            fail_count_reset=True,
                        )
                        _balance_cache.pop(acc_id, None)
                        # Если PP внезапно выдал ДРУГОЙ JWT — сохраним с CAS
                        if new_token != token:
                            fresh_acc = get_account(acc_id)
                            if fresh_acc:
                                stored = (fresh_acc.get("credentials") or {}).get("auth_token", "")
                                if stored == token:
                                    fresh_creds = dict(fresh_acc.get("credentials") or creds)
                                    fresh_creds["auth_token"] = new_token
                                    db_update_account(acc_id, credentials=fresh_creds)
                        audit("token.keepalive_refreshed",
                              target_type="account", target_id=acc_id,
                              details={"source": "background_keepalive",
                                       "new_jwt": new_token != token})
                        _sse_emit(f"account:{acc_id}", {
                            "type": "token.refreshed", "account_id": acc_id,
                        })
                        logger.info("pp keepalive ok acc=%d: серверная сессия продлена", acc_id)
                    else:
                        update_account_state(acc_id, fail_count_inc=True,
                                             last_error="no_token_returned")
                        logger.warning("pp keepalive failed acc=%d: no token returned", acc_id)
                except Exception as e:
                    update_account_state(acc_id, fail_count_inc=True,
                                         last_error=str(e)[:300])
                    logger.warning("pp keepalive error acc=%d: %s", acc_id, e)
        except Exception as e:
            logger.warning("pp keepalive loop error: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


async def _session_cleanup_loop():
    """Удаляет протухшие сессии из БД раз в час.
    Без этого таблица sessions растёт без ограничений."""
    await asyncio.sleep(300)   # первый запуск через 5 мин после старта
    while True:
        try:
            cleanup_sessions()
            logger.debug("session cleanup: expired sessions removed")
        except Exception as e:
            logger.warning("session cleanup error: %s", e)
        await asyncio.sleep(3600)   # каждый час


# ---------------------------------------------------------------------------
# Cleanup фоновые задачи: idempotent withdraw attempts + audit log
# ---------------------------------------------------------------------------
async def _withdraw_attempts_cleanup_loop():
    """Раз в 6 часов удаляет завершённые попытки старше 48 часов."""
    await asyncio.sleep(600)   # первый запуск через 10 мин после старта
    while True:
        try:
            removed = cleanup_withdraw_attempts(retention_hours=48)
            if removed:
                logger.info("withdraw_attempts cleanup: removed %d old rows", removed)
        except Exception as e:
            logger.warning("withdraw_attempts cleanup error: %s", e)
        await asyncio.sleep(6 * 3600)


async def _audit_log_cleanup_loop():
    """Раз в 24 часа удаляет audit-записи старше 90 дней."""
    await asyncio.sleep(900)   # первый запуск через 15 мин после старта
    while True:
        try:
            removed = cleanup_audit_log(retention_days=90)
            if removed:
                logger.info("audit_log cleanup: removed %d old rows", removed)
        except Exception as e:
            logger.warning("audit_log cleanup error: %s", e)
        await asyncio.sleep(24 * 3600)


# ---------------------------------------------------------------------------
# Proxy health checker — каждые 90 сек тестирует все enabled прокси
# ---------------------------------------------------------------------------
# Real-connection check: HTTP HEAD к https://mapi.astropaycard.com через прокси.
# Любой response code < 600 = прокси жив. Timeout/ProxyError = прокси мёртв.
# После 3 fail'ов подряд → auto-disable (enabled=0). Можно re-enable вручную.

async def _proxy_health_check_loop():
    """Background: проверяет живость всех enabled прокси.
    При auto-disable прокси (3 fail подряд) → реактивно переназначает
    все аккаунты на этом мёртвом прокси на healthy из пула.
    """
    await asyncio.sleep(180)   # warm-up: даём системе подняться
    while True:
        try:
            proxies = db_list_proxies(only_enabled=True)
            if proxies:
                logger.info("proxy health-check: testing %d proxies", len(proxies))
                async def _check_and_mark(p):
                    status, latency, err = await asyncio.to_thread(
                        proxy_health_check_one, p,
                    )
                    db_mark_proxy_status(p["id"], status, latency, err)
                    if status == "fail":
                        # auto-disable если fail_count после инкремента >= 3
                        fresh = db_get_proxy(p["id"])
                        if fresh and fresh.get("fail_count", 0) >= 3:
                            db_update_proxy(p["id"], enabled=False)
                            logger.warning("proxy %d (%s:%d) auto-disabled after 3 fails",
                                           p["id"], p["host"], p["port"])
                            # Реактивно переназначаем все аккаунты на этом мёртвом прокси
                            dead_url = build_proxy_url(p)
                            res = _reassign_dead_proxy_accounts(only_proxy_url=dead_url)
                            if res["reassigned"]:
                                logger.warning(
                                    "proxy auto-disable acc-reassign: %d accounts moved off %s:%d",
                                    res["reassigned"], p["host"], p["port"],
                                )
                semaphore = asyncio.Semaphore(5)
                async def _with_sem(p):
                    async with semaphore:
                        await _check_and_mark(p)
                await asyncio.gather(*[_with_sem(p) for p in proxies], return_exceptions=True)
        except Exception as e:
            logger.warning("proxy health-check loop error: %s", e)
        await asyncio.sleep(PROXY_HEALTH_INTERVAL)


async def _proxy_auto_reassign_loop():
    """Раз в 10 минут (плюс одноразово через 5 мин после старта) сканирует все
    PP/AP аккаунты и переназначает с мёртвых прокси на healthy.
    Safety net поверх реактивного auto-disable — ловит крайние случаи:
      - аккаунты с прокси, которая никогда не была в пуле (legacy)
      - удалённые из пула прокси
      - прокси которые умерли но fail_count ещё не достиг 3
    """
    await asyncio.sleep(300)   # первый прогон через 5 мин — даём health-check'у поработать
    while True:
        try:
            res = _reassign_dead_proxy_accounts()
            if res.get("no_healthy"):
                logger.info("auto-reassign loop: пул пуст — пропуск")
            elif res["reassigned"]:
                logger.info(
                    "auto-reassign loop: переназначено %d аккаунтов на healthy proxies",
                    res["reassigned"],
                )
        except Exception as e:
            logger.warning("auto-reassign loop error: %s", e)
        await asyncio.sleep(600)   # каждые 10 минут


# ---------------------------------------------------------------------------
# Auto-withdraw scheduler — фоновое выполнение правил БЕЗ ручного триггера
# ---------------------------------------------------------------------------
# Каждые 60 сек обходит все is_active правила и выполняет ОДИН chunk если:
#   - правило ещё активно (paid < total_limit)
#   - кэшированный баланс достаточен (>= chunk + min_balance)
#   - per-CVU лимит не достигнут
#   - per-card лимит не достигнут (если group_key применим)
#
# Защита от штормов: между чанками одного правила минимум 30 сек
# (даже если бы scheduler запустился раньше). Отказы записываются в
# rule.last_error, повторные попытки — на следующей итерации.
#
# Survives restart: вся state в БД (auto_withdraw_rules, withdraw_attempts).
# На рестарт scheduler стартует с нуля и продолжает с paid_amount из БД.

_AUTO_SCHEDULER_INTERVAL    = 60     # сек между прохождениями всего списка
_AUTO_MIN_INTERVAL_PER_RULE = 30     # сек между чанками одного правила


async def _auto_withdraw_scheduler_loop():
    """Каждые 60 сек: для каждого активного правила — проверка условий +
    выполнение одного chunk если все условия выполнены."""
    # Прогрев: даём 2 минуты на инициализацию balance cache
    await asyncio.sleep(120)
    last_chunk_at: dict = {}     # {rule_id: unix_ts последнего chunk}

    while True:
        try:
            rules = list_auto_withdraw_rules()
            now_ts = time.time()
            for rule in rules:
                if not rule.get("is_active"):
                    continue
                rule_id = int(rule["id"])

                # Throttle на rule
                last = last_chunk_at.get(rule_id, 0)
                if now_ts - last < _AUTO_MIN_INTERVAL_PER_RULE:
                    continue

                acc_id = int(rule["account_id"])
                acc = get_account(acc_id)
                if not acc:
                    continue

                # Завершено? — сбросить is_active и пропустить
                paid  = float(rule.get("paid_amount", 0))
                total = float(rule.get("total_limit", 0))
                if total > 0 and paid >= total:
                    update_auto_withdraw_progress(rule_id, is_active=False)
                    continue

                chunk = float(rule.get("chunk_amount", 0))
                if chunk <= 0:
                    continue

                # Проверка баланса — берём из кэша (быстро, без HTTP)
                cached = _balance_cache.get(acc_id)
                if not cached:
                    # Кэш ещё не прогрет — попросим прогрев и пропустим итерацию
                    if acc_id not in _bg_refreshing_bal:
                        _bg_refreshing_bal.add(acc_id)
                        asyncio.create_task(_bg_refresh_balance(acc_id))
                    continue
                balance = float(cached["data"].get("balance") or 0)
                min_balance = float(rule.get("min_balance") or 0)
                if balance < (chunk + min_balance):
                    continue   # ждём пополнения

                # Pre-check лимитов (быстро, без блокировок)
                cvu = rule.get("cvu", "")
                if is_withdraw_limit_reached(cvu, acc_id):
                    continue
                group_key = _group_key_for(acc["bank_type"], cvu)
                if group_key is not None and is_account_withdraw_limit_reached(acc_id, group_key):
                    continue   # карта в лимите внешних → ждём 07:30 МСК

                # Поехали — забираем lock и выполняем chunk
                last_chunk_at[rule_id] = now_ts
                lock = await _get_account_lock(acc_id)
                async with lock:
                    try:
                        ok, msg = await asyncio.to_thread(_run_auto_withdraw_rule, acc, rule)
                        if ok:
                            logger.info("auto-scheduler: rule=%d acc=%d chunk=%.2f → OK",
                                        rule_id, acc_id, chunk)
                            _sse_emit(f"account:{acc_id}", {
                                "type": "auto_withdraw.executed",
                                "account_id": acc_id, "rule_id": rule_id,
                                "amount": chunk,
                            })
                        else:
                            logger.info("auto-scheduler: rule=%d acc=%d → skip: %s",
                                        rule_id, acc_id, msg)
                    except Exception as e:
                        logger.warning("auto-scheduler: rule=%d exception: %s", rule_id, e)
        except Exception as e:
            logger.warning("auto-scheduler loop error: %s", e)

        await asyncio.sleep(_AUTO_SCHEDULER_INTERVAL)


# ---------------------------------------------------------------------------
# Reconciliation worker — разбирает UNCERTAIN-попытки выводов
# ---------------------------------------------------------------------------
# Сценарий: вывод стартовал, банк ответил timeout — мы не знаем, прошёл ли он.
# Резервы лимитов УЖЕ откачены (release_*) на момент исключения, статус=UNCERTAIN.
# Worker раз в 30 сек:
#   1. Берёт UNCERTAIN attempts старше 30 сек (банку дано время дописать activity).
#   2. Загружает свежие activities аккаунта и ищет совпадение по
#      (amount + destination + ts близко к created_at).
#   3. Match found → SUCCESS + bank_tx_id (и пере-резервируем лимит).
#   4. Match not found, attempt старше 5 мин → STUCK (требует ручного разбора).
#
# Принцип: «лучше STUCK чем ложный SUCCESS». Match строгий — иначе не трогаем.

_RECONCILE_INTERVAL = 30          # проверка каждые 30 сек
_RECONCILE_STUCK_AFTER = 300      # 5 мин — после этого срока без match → STUCK
_RECONCILE_AMOUNT_TOLERANCE = 0.51  # центы могут округлиться, ±50 копеек терпим


def _activity_matches_attempt(activity: dict, attempt: dict) -> bool:
    """Проверяет, является ли activity тем самым выводом, что запустила attempt.
    Match по: outgoing + amount (точно) + destination prefix (12+ цифр) + ts close."""
    if not isinstance(activity, dict):
        return False
    norm = _normalize_activity(activity)
    if not norm or not norm.get("is_outgoing"):
        return False
    # Сумма: считаем модуль (PP активити возвращает отрицательную для outgoing)
    a_amount_raw = norm.get("amount")
    try:
        a_amt = abs(float(a_amount_raw))
    except (TypeError, ValueError):
        return False
    target_amt = abs(float(attempt.get("amount") or 0))
    if abs(a_amt - target_amt) > _RECONCILE_AMOUNT_TOLERANCE:
        return False

    # Время — activity должна быть не раньше attempt.created_at - 2 мин
    # и не позже + 5 мин (PP может задержать запись в activity-list)
    a_ts = _activity_unix_ts(activity, norm)
    if a_ts is None:
        return False
    created_at = int(attempt.get("created_at") or 0)
    if a_ts < created_at - 120 or a_ts > created_at + 600:
        return False

    # Destination — сверяем префикс (PP не всегда возвращает полный CVU в activity)
    dest = (attempt.get("destination") or "").strip()
    if dest:
        # Ищем destination в любых полях activity по подстроке (CVU/alias/account)
        haystack = " ".join(filter(None, [
            str(norm.get("recipient") or ""),
            str(norm.get("recipient_lastname") or ""),
            str(activity.get("destinationAccount") or ""),
            str(activity.get("targetAccount") or ""),
            json.dumps(activity, ensure_ascii=False)[:1000],
        ]))
        # Берём 8+ цифр из dest и проверяем что они есть в haystack
        digits = "".join(c for c in dest if c.isdigit())
        if len(digits) >= 8 and digits not in haystack:
            return False

    return True


def _activity_unix_ts(activity: dict, normalized: dict) -> Optional[int]:
    """Извлекает unix timestamp активности (несколько форматов)."""
    # 1) Из normalized.date_str (ISO)
    ds = (normalized or {}).get("date_str") or ""
    if ds:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return int(datetime.strptime(ds[:len(fmt) + 6], fmt).timestamp())
            except (ValueError, TypeError):
                pass
    # 2) Из самого activity (возможны разные ключи)
    for key in ("createdAt", "timestamp", "transactionDate", "date", "ts"):
        v = activity.get(key)
        if not v:
            continue
        if isinstance(v, (int, float)):
            v_int = int(v)
            return v_int // 1000 if v_int > 10**12 else v_int
        if isinstance(v, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return int(datetime.strptime(v[:26], fmt).timestamp())
                except (ValueError, TypeError):
                    pass
    return None


def _extract_bank_tx_id_from_activity(activity: dict) -> Optional[str]:
    """Пытается достать bank_tx_id из activity (32-hex)."""
    if not isinstance(activity, dict):
        return None
    # Прямой поиск 32-hex
    candidate = _find_32char_hex_id(activity)
    if candidate:
        return candidate
    # Fallback — стандартные ключи
    for key in ("transactionId", "id", "transferId", "bankTransactionId", "operationId"):
        v = activity.get(key)
        if v and isinstance(v, str):
            v = v.strip()
            if len(v) == 32 and all(c in "0123456789abcdefABCDEF" for c in v):
                return v
    return None


async def _reconcile_uncertain_loop():
    """Фоновая reconciliation: разбирает зависшие UNCERTAIN выводы."""
    await asyncio.sleep(45)   # первый запуск чуть позже старта
    while True:
        try:
            attempts = list_uncertain_withdraw_attempts(max_age_seconds=_RECONCILE_STUCK_AFTER + 60)
            if attempts:
                logger.info("reconcile: scanning %d UNCERTAIN attempts", len(attempts))
                # Группируем по account_id чтобы не дёргать банк лишний раз
                by_acc: dict = collections.defaultdict(list)
                for a in attempts:
                    by_acc[int(a["account_id"])].append(a)

                now = int(time.time())
                for acc_id, acc_attempts in by_acc.items():
                    acc = get_account(acc_id)
                    if not acc:
                        continue
                    bank = acc.get("bank_type")
                    if bank not in ("personalpay", "astropay"):
                        # UC не поддерживаем reconciliation — сразу STUCK после timeout
                        for att in acc_attempts:
                            if now - int(att["created_at"]) >= _RECONCILE_STUCK_AFTER:
                                update_withdraw_attempt_status(
                                    att["idempotency_key"], "STUCK",
                                    error_message="reconcile: bank not supported",
                                )
                        continue

                    # Загружаем свежий список активностей (через thread — это блокирующий I/O)
                    try:
                        if bank == "personalpay":
                            data = await asyncio.to_thread(pp_activities_list, acc["credentials"], 0, 50)
                            raw_acts = _extract_activities_raw(data)
                        else:
                            from app.drivers.astropay import get_activities as ap_get_activities
                            data = await asyncio.to_thread(ap_get_activities, acc["credentials"], 1, 50)
                            raw_acts = data.get("data") or []
                    except Exception as e:
                        logger.warning("reconcile: cannot fetch activities for acc=%d: %s", acc_id, e)
                        continue

                    if not isinstance(raw_acts, list):
                        continue

                    for att in acc_attempts:
                        matched = None
                        for activity in raw_acts:
                            if _activity_matches_attempt(activity, att):
                                matched = activity
                                break
                        if matched is not None:
                            tid = _extract_bank_tx_id_from_activity(matched)
                            update_withdraw_attempt_status(
                                att["idempotency_key"], "SUCCESS", bank_tx_id=tid,
                            )
                            # Восстанавливаем лимит — он же реально был использован
                            cvu = att.get("destination") or ""
                            try_reserve_withdraw_count(cvu, acc_id)
                            grp = att.get("group_key")
                            if grp in (GROUP_A, GROUP_B):
                                try_reserve_account_withdraw_count(acc_id, grp)
                            # Инвалидируем кэш для свежего отображения
                            _balance_cache.pop(acc_id, None)
                            _activities_cache.pop(acc_id, None)
                            _sse_emit(f"account:{acc_id}", {
                                "type": "withdraw.completed", "account_id": acc_id,
                                "tid": tid, "via": "reconcile",
                            })
                            logger.info("reconcile: matched attempt %s → SUCCESS tid=%s",
                                        att["idempotency_key"][:12], tid)
                        elif now - int(att["created_at"]) >= _RECONCILE_STUCK_AFTER:
                            update_withdraw_attempt_status(
                                att["idempotency_key"], "STUCK",
                                error_message="reconcile: no matching activity within 5min window",
                            )
                            logger.warning("reconcile: attempt %s → STUCK (no match)",
                                           att["idempotency_key"][:12])
        except Exception as e:
            logger.warning("reconcile loop error: %s", e)

        await asyncio.sleep(_RECONCILE_INTERVAL)


@app.on_event("startup")
async def startup_event():
    # ВНИМАНИЕ: _pp_migrate_default_pin() удалён сознательно — он перезаписывал
    # реальные PIN-хэши пользователей дефолтом 464646, ломая keepalive.
    # Дефолтный PIN остаётся как fallback в _norm_creds (drivers/personalpay.py),
    # но НЕ затирает данные в БД.
    asyncio.create_task(_rate_collector_loop())
    asyncio.create_task(_pp_keepalive_loop())
    asyncio.create_task(_session_cleanup_loop())
    asyncio.create_task(_proactive_refresh_loop())
    asyncio.create_task(_withdraw_attempts_cleanup_loop())
    asyncio.create_task(_audit_log_cleanup_loop())
    asyncio.create_task(_reconcile_uncertain_loop())
    asyncio.create_task(_auto_withdraw_scheduler_loop())
    asyncio.create_task(_proxy_health_check_loop())
    asyncio.create_task(_proxy_auto_reassign_loop())


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

    cvu       = rule.get("cvu", "")
    bank_type = acc["bank_type"]
    group_key = _group_key_for(bank_type, cvu)   # 'a' / 'b' / None

    # Проверка минимального баланса
    min_balance = float(rule.get("min_balance") or 0)
    if min_balance > 0:
        try:
            balance_info = driver_balance(bank_type, acc["credentials"])
            current_balance = float(balance_info.get("balance") or 0)
        except Exception as e:
            err = f"Не удалось получить баланс: {e}"
            update_auto_withdraw_progress(rule["id"], last_error=err)
            return False, err
        if current_balance < min_balance:
            return False, f"Баланс {current_balance:,.0f} < порога {min_balance:,.0f} — ожидание"

    if bank_type == "universalcoins":
        update_auto_withdraw_progress(rule["id"],
                                      last_error="Автовывод не поддерживается для UniversalCoins",
                                      is_active=False)
        return False, "Автовывод не поддерживается для UniversalCoins"

    # ── Idempotency для авто-чанков: salt включает paid_amount, чтобы каждый
    #     новый чанк имел уникальный ключ, но повтор того же чанка — нет ──
    paid_so_far = float(rule.get("paid_amount", 0))
    idem_key = _make_idempotency_key(acc["id"], cvu, chunk,
                                     salt=f"auto:{rule['id']}:{paid_so_far:.2f}")
    created, prior = try_create_withdraw_attempt(
        idem_key, acc["id"], cvu, chunk, group_key=group_key,
    )
    if not created and prior:
        st = prior.get("status")
        if st == "SUCCESS":
            # Чанк уже прошёл — не делаем повторно, но прогресс зафиксируем
            update_auto_withdraw_progress(rule["id"], paid_delta=chunk, last_error="")
            return True, prior.get("bank_tx_id") or ""
        if st in ("PENDING", "EXECUTING"):
            return False, "Чанк уже выполняется (idempotency-lock)"
        if st == "REJECTED":
            update_auto_withdraw_progress(rule["id"],
                                          last_error=prior.get("error_message") or "rejected")
            return False, prior.get("error_message") or "rejected"
        # UNCERTAIN — пропускаем, нужна reconciliation
        return False, "Предыдущий чанк в неопределённом состоянии"

    # ── Атомарные резервы лимитов ──
    if try_reserve_withdraw_count(cvu, acc["id"]) is None:
        msg = f"Дневной лимит ({DAILY_WITHDRAW_LIMIT}) достигнут для CVU {cvu}"
        update_withdraw_attempt_status(idem_key, "REJECTED", error_message=msg)
        update_auto_withdraw_progress(rule["id"], last_error=msg)
        return False, msg

    if group_key is not None:
        if try_reserve_account_withdraw_count(acc["id"], group_key) is None:
            release_withdraw_count(cvu, acc["id"])
            grp_label = "внешних"
            msg = f"Лимит карты ({DAILY_WITHDRAW_LIMIT} {grp_label}) достигнут"
            update_withdraw_attempt_status(idem_key, "REJECTED", error_message=msg)
            update_auto_withdraw_progress(rule["id"], last_error=msg)
            return False, msg

    update_withdraw_attempt_status(idem_key, "EXECUTING")
    try:
        result = driver_withdraw(
            bank_type, acc["credentials"],
            destination=cvu, amount=chunk, comments="Auto withdraw",
        )
        # Кэш сбрасываем чтобы UI увидел свежий баланс
        _balance_cache.pop(acc["id"], None)
        _activities_cache.pop(acc["id"], None)
        update_auto_withdraw_progress(rule["id"], paid_delta=chunk, last_error="")
    except Exception as e:
        # Откат резервов: вывод не прошёл
        err_msg = str(e)
        release_withdraw_count(cvu, acc["id"])
        if group_key is not None:
            release_account_withdraw_count(acc["id"], group_key)
        low = err_msg.lower()
        if any(x in low for x in ("rechazad", "rejected", "rechazo", "denied", "denegad")):
            update_withdraw_attempt_status(idem_key, "REJECTED", error_message=err_msg[:400])
        else:
            update_withdraw_attempt_status(idem_key, "UNCERTAIN", error_message=err_msg[:400])
        update_auto_withdraw_progress(rule["id"], last_error=err_msg)
        return False, err_msg

    tid = _find_32char_hex_id(result) if isinstance(result, dict) else None
    update_withdraw_attempt_status(idem_key, "SUCCESS", bank_tx_id=tid)
    audit("withdraw.auto.success",
          target_type="account", target_id=acc["id"],
          details={"rule_id": rule["id"], "cvu": cvu, "amount": chunk,
                   "tid": tid, "group": group_key})
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
    ip = _get_client_ip(request)

    # Блокировка после 5 неудачных попыток за 15 минут
    if _is_login_rate_limited(ip):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Слишком много попыток входа. Попробуйте через 15 минут."},
            status_code=429,
        )

    user = get_user_by_username(username)
    if not user or not user.get("is_active") or not verify_password(password, user.get("password_hash", "")):
        attempts = _record_failed_login(ip)
        left = max(0, _LOGIN_MAX_ATTEMPTS - attempts)
        warn = f" Осталось попыток: {left}." if left <= 2 else ""
        audit("login.failed", request=request,
              details={"username": username[:60], "attempts": attempts})
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": f"Неверный логин или пароль.{warn}"},
            status_code=400,
        )

    _clear_login_attempts(ip)
    token = secrets.token_urlsafe(32)
    create_session(token, user["id"], user["username"], int(time.time() + SESSION_TTL))
    audit("login.success", request=request,
          user={"user_id": user["id"], "username": user["username"]})
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key=SESSION_COOKIE, value=token,
        httponly=True, samesite="lax", max_age=SESSION_TTL,
    )
    return response


@app.post("/logout", response_class=RedirectResponse)
async def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    user = _current_user(request)
    if token:
        delete_session(token)
    audit("logout", request=request, user=user)
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
            token_expired, token_expires_in_hours = _jwt_expiry(
                selected["credentials"], account_id=selected.get("id"),
            )
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

@app.get("/api/accounts")
async def api_accounts_list():
    """JSON-список всех аккаунтов: id, label, bank_type, window.
    Используется ботом и автоматизацией — без чувствительных credentials."""
    accs = list_accounts()
    return JSONResponse({
        "accounts": [
            {
                "id":         int(a["id"]),
                "label":      a.get("label") or "",
                "bank_type":  a.get("bank_type") or "",
                "window":     a.get("window") or "glazars",
                "created_at": a.get("created_at") or "",
            }
            for a in accs
        ],
        "ts": int(time.time()),
    })


@app.get("/api/balances")
async def api_balances_batch(ids: str = ""):
    """Batched-эндпоинт: возвращает балансы N аккаунтов за один HTTP-запрос.
    Используется sidebar'ом — экономит N-1 round-trip'ов.

    Семантика: возвращает ТОЛЬКО кэшированные значения, без блокирующих запросов
    к банку. Если кэша нет — null. Это интенционально: sidebar обновляется часто
    и не должен ждать 28с холодного fetch'а на каждом аккаунте.

    Параметры: ids=1,2,3 (csv).
    Ответ: {"balances": {"1": {balance, cvu_number, ...}, "2": {...}, "3": null}, "ts": <unix>}
    """
    if not ids:
        return JSONResponse({"balances": {}, "ts": int(time.time())})
    try:
        id_list = [int(x.strip()) for x in ids.split(",") if x.strip().isdigit()]
    except Exception:
        return JSONResponse({"error": "invalid ids"}, status_code=400)
    if len(id_list) > 200:
        return JSONResponse({"error": "too many ids"}, status_code=400)

    result: dict = {}
    now = time.time()
    # Один SQL-запрос на все ID → намного быстрее чем N отдельных
    cached_batch = db_get_balance_cache_batch(id_list)
    for acc_id in id_list:
        cached = cached_batch.get(acc_id)
        if cached:
            age = now - cached["ts"]
            data = dict(cached["data"])
            data["account_withdraw_count"] = get_account_withdraw_count(acc_id)
            data["account_withdraw_limit"] = DAILY_WITHDRAW_LIMIT
            data["_cache_age_sec"] = int(age)
            data["_is_error"] = bool(cached.get("is_error"))
            data["_is_stale"] = age >= _BALANCE_STALE
            if cached.get("last_refresh_error"):
                data["_last_refresh_error"] = cached["last_refresh_error"][:200]
            # Если кэш устарел или это error-state → запускаем bg refresh
            # (но всё равно отдаём данные — пусть клиент сам решает)
            if (age >= _BALANCE_STALE or data["_is_error"]) and acc_id not in _bg_refreshing_bal:
                acc = get_account(acc_id)
                if acc and acc.get("bank_type"):
                    _bg_refreshing_bal.add(acc_id)
                    asyncio.create_task(_bg_refresh_balance(acc_id))
            result[str(acc_id)] = data
        else:
            # Кэша вообще нет → запускаем bg refresh, возвращаем null
            if acc_id not in _bg_refreshing_bal:
                acc = get_account(acc_id)
                if acc and acc.get("bank_type"):
                    _bg_refreshing_bal.add(acc_id)
                    asyncio.create_task(_bg_refresh_balance(acc_id))
            result[str(acc_id)] = None
    return JSONResponse({"balances": result, "ts": int(now)})


@app.get("/account/{account_id}/balance")
async def api_balance(account_id: int):
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"error": "account not found"}, status_code=404)

    now    = time.time()
    cached = _balance_cache.get(account_id)

    # ── Stale-while-revalidate ──────────────────────────────────────────────
    if cached:
        age  = now - cached["ts"]
        if age < _BALANCE_STALE:
            # Кэш актуален (или немного устарел) — отдаём мгновенно
            if age >= _BALANCE_TTL and account_id not in _bg_refreshing_bal:
                # Немного устарел — запускаем фоновое обновление
                _bg_refreshing_bal.add(account_id)
                asyncio.create_task(_bg_refresh_balance(account_id))
            data = dict(cached["data"])
            data["account_withdraw_count"] = get_account_withdraw_count(account_id)
            data["account_withdraw_limit"] = DAILY_WITHDRAW_LIMIT
            return JSONResponse(data)
        # Кэш слишком старый (> _BALANCE_STALE) — продолжаем к синхронному запросу

    # ── Нет кэша или слишком старый — синхронный запрос ────────────────────
    try:
        info   = await _fetch_and_cache_balance(account_id, acc)
        result = dict(info)
        result["account_withdraw_count"] = get_account_withdraw_count(account_id)
        result["account_withdraw_limit"] = DAILY_WITHDRAW_LIMIT
        return JSONResponse(result)
    except Exception as e:
        # Если упал, но есть хоть какой-то кэш — отдадим его (лучше устаревшие данные, чем ошибка)
        if cached:
            data = dict(cached["data"])
            data["account_withdraw_count"] = get_account_withdraw_count(account_id)
            data["account_withdraw_limit"] = DAILY_WITHDRAW_LIMIT
            return JSONResponse(data)
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
async def api_pin_refresh(request: Request, account_id: int):
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
        from app.drivers.personalpay import refresh_session_with_pin, _pp_jwt_exp
        old_token = creds.get("auth_token", "")
        new_token = await asyncio.to_thread(refresh_session_with_pin, creds)
        if new_token:
            # Согласно HAR: PP не выдаёт новый JWT — pin/validate просто продлевает
            # серверную сессию. Поэтому success = "сессия активна на ближайшие ~12ч".
            if new_token != old_token:
                new_creds = dict(creds)
                new_creds["auth_token"] = new_token
                db_update_account(account_id, credentials=new_creds)
            _balance_cache.pop(account_id, None)
            _activities_cache.pop(account_id, None)
            # Записываем last_keepalive_at чтобы UI показывал «Сессия активна»
            update_account_state(account_id, last_keepalive_at=int(time.time()),
                                 last_token_state="ALIVE", fail_count_reset=True)
            if account_id not in _bg_refreshing_bal:
                _bg_refreshing_bal.add(account_id)
                asyncio.create_task(_bg_refresh_balance(account_id))
            audit("token.pin_refresh", request=request, user=_current_user(request),
                  target_type="account", target_id=account_id,
                  details={"got_new_jwt": new_token != old_token})
            _sse_emit(f"account:{account_id}", {
                "type": "token.refreshed", "account_id": account_id,
            })
            msg = "Сессия продлена через PIN ✓ Серверная сессия активна на ~12 часов."
            return JSONResponse({"ok": True, "message": msg})
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

    limit = max(1, min(limit, 100))
    page  = max(1, page)

    now    = time.time()
    cached = _activities_cache.get(account_id)

    activities = None

    # ── Stale-while-revalidate ──────────────────────────────────────────────
    if cached:
        age = now - cached["ts"]
        if age < _ACTIVITIES_STALE:
            activities = cached["acts"]
            if age >= _ACTIVITIES_TTL and account_id not in _bg_refreshing_act:
                _bg_refreshing_act.add(account_id)
                asyncio.create_task(_bg_refresh_activities(account_id))

    if activities is None:
        try:
            activities = await _fetch_and_cache_activities(account_id, acc)
        except Exception as e:
            logger.exception("api_activities error account=%d", account_id)
            return JSONResponse({"error": str(e)}, status_code=500)

    # ── Фильтр по типу ───────────────────────────────────────────────────────
    filtered = activities
    if type == "incoming":
        filtered = [a for a in filtered if not a["is_outgoing"]]
    elif type == "outgoing":
        filtered = [a for a in filtered if a["is_outgoing"]]

    # ── Поиск по имени / фамилии ─────────────────────────────────────────────
    q = search.strip().lower()
    if q:
        def _match(a):
            haystack = " ".join(filter(None, [
                a.get("sender"), a.get("sender_lastname"),
                a.get("recipient"), a.get("recipient_lastname"),
                a.get("full_name"),
            ])).lower()
            return q in haystack
        filtered = [a for a in filtered if _match(a)]

    # ── Пагинация ─────────────────────────────────────────────────────────────
    total  = len(filtered)
    pages  = max(1, (total + limit - 1) // limit)
    page   = min(page, pages)
    offset = (page - 1) * limit
    paged  = filtered[offset : offset + limit]

    return JSONResponse({
        "activities": paged,
        "total":  total,
        "page":   page,
        "pages":  pages,
        "limit":  limit,
    })


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
            return {"account_id": acc_id, "label": "?", "ok": False,
                    "error": "Счёт не найден", "tid": None}

        bank_type = acc["bank_type"]
        group_key = _group_key_for(bank_type, destination)

        # Per-account lock: на одну карту — одна операция за раз.
        # Параллелизм сохраняется между разными account_id.
        lock = await _get_account_lock(acc["id"])
        async with lock:
            # Idempotency с 60-секундным окном — защита от double-click,
            # но через минуту тот же multi-withdraw можно повторить.
            minute_window = int(time.time() // 60)
            idem_key = _make_idempotency_key(acc["id"], destination, amt or 0,
                                             salt=f"multi:{minute_window}")
            created, prior = try_create_withdraw_attempt(
                idem_key, acc["id"], destination, amt or 0, group_key=group_key,
            )
            if not created and prior:
                status = prior.get("status")
                if status == "SUCCESS":
                    return {"account_id": acc_id, "label": acc["label"], "ok": True,
                            "error": None, "tid": prior.get("bank_tx_id")}
                if status in ("PENDING", "EXECUTING"):
                    return {"account_id": acc_id, "label": acc["label"], "ok": False,
                            "error": "Этот вывод сейчас обрабатывается", "tid": None}
                if status == "REJECTED":
                    return {"account_id": acc_id, "label": acc["label"], "ok": False,
                            "error": prior.get("error_message") or "rejected_by_bank",
                            "tid": None}
                # UNCERTAIN/STUCK в текущем 60-сек окне:
                # Не пропускаем дальше — risk double-charge если bank на самом деле
                # принял прошлый. Через минуту minute_window сменится → ключ
                # станет другим → новая попытка пройдёт автоматически.
                return {"account_id": acc_id, "label": acc["label"], "ok": False,
                        "error": "Предыдущая попытка ждёт ответа банка. Повторите через 1 минуту.",
                        "tid": None}

            # Атомарный резерв per-CVU
            if try_reserve_withdraw_count(destination, acc["id"]) is None:
                update_withdraw_attempt_status(idem_key, "REJECTED",
                                               error_message="cvu_limit_reached")
                return {"account_id": acc_id, "label": acc["label"], "ok": False,
                        "error": f"Лимит {DAILY_WITHDRAW_LIMIT} на CVU достигнут",
                        "tid": None}

            # Атомарный резерв per-card-group
            if group_key is not None:
                if try_reserve_account_withdraw_count(acc["id"], group_key) is None:
                    release_withdraw_count(destination, acc["id"])
                    grp_label = "внешних"
                    update_withdraw_attempt_status(idem_key, "REJECTED",
                                                   error_message=f"group_{group_key}_limit_reached")
                    return {"account_id": acc_id, "label": acc["label"], "ok": False,
                            "error": f"Лимит {DAILY_WITHDRAW_LIMIT} {grp_label} достигнут",
                            "tid": None}

            update_withdraw_attempt_status(idem_key, "EXECUTING")
            try:
                if bank_type == "universalcoins":
                    result = await asyncio.to_thread(
                        driver_withdraw, bank_type, acc["credentials"],
                        cvu_recipient=destination, amount=amt, concept=concept,
                    )
                else:
                    result = await asyncio.to_thread(
                        driver_withdraw, bank_type, acc["credentials"],
                        destination=destination, amount=amt, comments=comments,
                    )
                _balance_cache.pop(acc["id"], None)
                _activities_cache.pop(acc["id"], None)
                tid = _find_32char_hex_id(result) if isinstance(result, dict) else None
                update_withdraw_attempt_status(idem_key, "SUCCESS", bank_tx_id=tid)
                audit("withdraw.multi.success", request=request, user=_current_user(request),
                      target_type="account", target_id=acc["id"],
                      details={"destination": destination, "amount": amt,
                               "tid": tid, "group": group_key})
                _sse_emit(f"account:{acc['id']}", {
                    "type": "withdraw.completed", "account_id": acc["id"], "tid": tid,
                })
                return {"account_id": acc_id, "label": acc["label"], "ok": True,
                        "error": None, "tid": tid}
            except Exception as e:
                err_msg = str(e)
                # Откат резервов
                release_withdraw_count(destination, acc["id"])
                if group_key is not None:
                    release_account_withdraw_count(acc["id"], group_key)
                low = err_msg.lower()
                if any(x in low for x in ("rechazad", "rejected", "rechazo", "denied", "denegad")):
                    update_withdraw_attempt_status(idem_key, "REJECTED",
                                                   error_message=err_msg[:400])
                else:
                    update_withdraw_attempt_status(idem_key, "UNCERTAIN",
                                                   error_message=err_msg[:400])
                return {"account_id": acc_id, "label": acc["label"], "ok": False,
                        "error": err_msg, "tid": None}

    # Выполняем все выводы параллельно (lock внутри сериализует одинаковые account_id)
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

    bank_type = acc["bank_type"]
    group_key = _group_key_for(bank_type, dest)   # 'a' / 'b' / None (None=AP→AP exempt)

    # ── Сериализуем все операции на одной карте через per-account lock ─────
    account_lock = await _get_account_lock(account_id)
    async with account_lock:

        # ── Идемпотентность с 60-секундным окном:
        #   Защита от double-click и refresh-replay в первые 60 сек после submit.
        #   После 60 сек — minute_window меняется → новый ключ → можно повторно
        #   отправить тот же вывод (например, та же сумма на тот же CVU).
        minute_window = int(time.time() // 60)
        idem_key = _make_idempotency_key(account_id, dest, amt or 0,
                                         salt=f"manual:{minute_window}")
        created, prior = try_create_withdraw_attempt(
            idem_key, account_id, dest, amt or 0, group_key=group_key,
        )
        if not created and prior:
            status = prior.get("status")
            if status == "SUCCESS":
                # Тот же submit в окне 60 сек — silently показываем прошлый чек
                tid = prior.get("bank_tx_id")
                if tid:
                    return RedirectResponse(
                        url=f"/account/{account_id}/receipt?transaction_id={tid}",
                        status_code=302,
                    )
                return RedirectResponse(
                    url=f"/?account_id={account_id}&success=1", status_code=302,
                )
            if status in ("PENDING", "EXECUTING"):
                # Реальный in-flight — единственный случай где блокируем явно
                return RedirectResponse(
                    url=f"/?account_id={account_id}&error=withdraw_in_progress",
                    status_code=302,
                )
            if status == "REJECTED":
                err_param = quote((prior.get("error_message") or "rejected_by_bank")[:200], safe="")
                return RedirectResponse(
                    url=f"/?account_id={account_id}&error={err_param}", status_code=302,
                )
            # UNCERTAIN/STUCK в текущем 60-сек окне:
            # Не пропускаем — есть risk double-charge если банк на самом деле
            # принял прошлый запрос (мы не получили ответ из-за сети). Reconciliation
            # worker разрешит это сам в течение 5 минут. Через 1 минуту minute_window
            # сменится → ключ станет другим → новая попытка пройдёт автоматически.
            return RedirectResponse(
                url=f"/?account_id={account_id}&error=retry_after_minute",
                status_code=302,
            )

        # ── Per-CVU лимит: атомарный резерв (15 на CVU за день) ─────────────
        cvu_count = try_reserve_withdraw_count(dest, account_id)
        if cvu_count is None:
            update_withdraw_attempt_status(idem_key, "REJECTED",
                                           error_message="cvu_limit_reached")
            return RedirectResponse(url=f"/?account_id={account_id}&error=limit_reached",
                                    status_code=302)

        # ── Per-card лимит по группе (если группа применима) ────────────────
        if group_key is not None:
            grp_count = try_reserve_account_withdraw_count(account_id, group_key)
            if grp_count is None:
                # Откатываем CVU-резерв, чтобы не сжигать слот
                release_withdraw_count(dest, account_id)
                err = "group_a_limit_reached" if group_key == GROUP_A else "group_b_limit_reached"
                update_withdraw_attempt_status(idem_key, "REJECTED", error_message=err)
                return RedirectResponse(url=f"/?account_id={account_id}&error={err}",
                                        status_code=302)

        # ── Вызов банка ─────────────────────────────────────────────────────
        update_withdraw_attempt_status(idem_key, "EXECUTING")
        try:
            if bank_type == "universalcoins":
                doc_clean = (document or "").strip().replace("-", "").replace(" ", "")
                if not doc_clean or len(doc_clean) < 10:
                    # Откат обоих резервов
                    release_withdraw_count(dest, account_id)
                    if group_key is not None:
                        release_account_withdraw_count(account_id, group_key)
                    update_withdraw_attempt_status(idem_key, "REJECTED",
                                                   error_message="document_required")
                    return RedirectResponse(
                        url=f"/?account_id={account_id}&error=document_required",
                        status_code=302,
                    )
                result = await asyncio.to_thread(
                    driver_withdraw,
                    bank_type, acc["credentials"],
                    cvu_recipient=dest, amount=amt, concept=concept,
                    alias_recipient=alias.strip() or None,
                    document_recipient=doc_clean,
                    name_recipient=name.strip() or None,
                    bank_recipient=bank.strip() or None,
                )
            else:
                result = await asyncio.to_thread(
                    driver_withdraw,
                    bank_type, acc["credentials"],
                    destination=dest, amount=amt, comments=comments,
                )
            # Успех — кэш баланса/истории сбрасываем
            _balance_cache.pop(account_id, None)
            _activities_cache.pop(account_id, None)
        except Exception as e:
            # ── Откат резервов: вывод не прошёл, лимит не должен быть истрачен ──
            err_msg = str(e)
            release_withdraw_count(dest, account_id)
            if group_key is not None:
                release_account_withdraw_count(account_id, group_key)
            if any(x in err_msg.lower() for x in ("rechazad", "rejected", "rechazo", "denied", "denegad")):
                error_param = "rejected_by_bank"
                update_withdraw_attempt_status(idem_key, "REJECTED",
                                               error_message="rejected_by_bank")
            else:
                error_param = quote(err_msg[:200], safe="")
                # Сетевые ошибки → UNCERTAIN (не знаем прошёл ли вывод на стороне банка)
                update_withdraw_attempt_status(idem_key, "UNCERTAIN",
                                               error_message=err_msg[:400])
            return RedirectResponse(url=f"/?account_id={account_id}&error={error_param}",
                                    status_code=302)

        # ── Успех: извлекаем bank_tx_id и фиксируем SUCCESS в idempotency-таблице ──
        tid = None
        if bank_type in ("personalpay", "astropay") and isinstance(result, dict):
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

        update_withdraw_attempt_status(idem_key, "SUCCESS", bank_tx_id=tid)
        audit("withdraw.success", request=request, user=_current_user(request),
              target_type="account", target_id=account_id,
              details={"destination": dest, "amount": amt, "tid": tid,
                       "group": group_key, "concept": concept})
        _sse_emit(f"account:{account_id}", {
            "type": "withdraw.completed", "account_id": account_id, "tid": tid,
        })

        if tid:
            return RedirectResponse(url=f"/account/{account_id}/receipt?transaction_id={tid}",
                                    status_code=302)
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
    request: Request,
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
        # AUTO-ASSIGNMENT: если в форме прокси не указан — берём healthy из пула
        if not proxy_url:
            try:
                proxy_url = build_proxy_url(_pick_least_loaded_healthy_proxy() or {}) or ""
            except Exception:
                proxy_url = ""
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
    if bank_type == "personalpay" and not credentials.get("pin_hash"):
        credentials["pin_hash"] = PP_DEFAULT_PIN_HASH
    if not label.strip():
        label = f"{BANK_TYPES[bank_type]['name']} — {bank_type}"
    new_id = db_add_account(bank_type, label.strip(), credentials, window=window)
    audit("account.created", request=request, user=_current_user(request),
          target_type="account", target_id=new_id,
          details={"bank_type": bank_type, "label": label.strip(), "window": window,
                   "proxy_assigned": bool(proxy_url) if bank_type in ("personalpay","astropay") else None})
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
    request: Request,
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

    # Сбрасываем кэш — старые данные (ошибка 403 и т.д.) больше не актуальны
    _balance_cache.pop(account_id, None)
    _activities_cache.pop(account_id, None)
    # Немедленно запускаем фоновый прогрев с новыми credentials
    if account_id not in _bg_refreshing_bal:
        _bg_refreshing_bal.add(account_id)
        asyncio.create_task(_bg_refresh_balance(account_id))

    # В audit пишем только что было обновлено — БЕЗ значений credentials
    audit("account.updated", request=request, user=_current_user(request),
          target_type="account", target_id=account_id,
          details={
              "label_changed": bool(label.strip()),
              "credentials_changed": bool(credentials),
              "window_changed": bool(window_custom or window),
          })

    return RedirectResponse(url=f"/?account_id={account_id}&success=updated", status_code=302)


@app.post("/account/{account_id}/delete", response_class=RedirectResponse)
async def delete_account(request: Request, account_id: int,
                         redirect_window: Optional[str] = Form(None)):
    acc = get_account(account_id)
    db_delete_account(account_id)
    audit("account.deleted", request=request, user=_current_user(request),
          target_type="account", target_id=account_id,
          details={"label": (acc or {}).get("label"),
                   "bank_type": (acc or {}).get("bank_type")})
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
    # Сохраняем испанские лейблы как в оригинальном PP (1:1 совпадение с чеком PP)
    label_ru = {
        "fecha":              "Fecha",
        "date":               "Fecha",
        "hora":               "Hora",
        "envía":              "Envía",
        "envia":              "Envía",
        "recibe":             "Recibe",
        "desde":              "Desde",
        "cuil/cuit":          "CUIL/CUIT",
        "cuit":               "CUIL/CUIT",
        "banco/billetera":    "Banco/Billetera",
        "banco":              "Banco/Billetera",
        "bank":               "Banco/Billetera",
        "cbu/cvu":            "CBU/CVU",
        "cvu":                "CBU/CVU",
        "cbu":                "CBU/CVU",
        "nº de la operación": "Nº operación",
        "nº operación":       "Nº operación",
        "coelsaid":           "CoelsaID",
        "estado":             "Estado",
        "status":             "Estado",
        "monto":              "Monto",
        "amount":             "Monto",
        "id":                 "ID",
        "remitente":          "Envía",
        "titular":            "Envía",
        "sender":             "Envía",
        "destinatario":       "Recibe",
        "recipient":          "Recibe",
        "beneficiario":       "Recibe",
    }
    skip_labels = {"utr"}
    seen_keys: set = set()

    if transference and isinstance(transference, dict):
        for d in (transference.get("details") or []):
            if not isinstance(d, dict):
                continue
            label = (d.get("label") or d.get("key") or "").strip()
            value = str(d.get("value") or d.get("displayValue") or "").strip()
            if label.lower() in skip_labels or not value:
                continue
            label_show = label_ru.get(label.lower()) or label
            # Пропускаем дубли (один и тот же лейбл с одним значением)
            dedup_key = label_show.lower() + "|" + value.lower()
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            receipt_lines.append({"label": label_show, "value": value})

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
    # На чеке сумма всегда положительная — направление указывает заголовок (Enviaste / Recibiste)
    amount_display = str(abs(amount_num)) if amount_num is not None else str(amount_val or "")

    return templates.TemplateResponse("receipt.html", {
        **base_ctx,
        "error":         None,
        "transference":  transference,
        "receipt_lines": receipt_lines,
        "receipt_title": (transference or {}).get("title") or ("Enviaste dinero" if is_outgoing else "Recibiste dinero"),
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

# ---------------------------------------------------------------------------
# Admin: Migration — массовое перемещение аккаунтов между кабинетами
# ---------------------------------------------------------------------------

@app.get("/admin/migrate", response_class=HTMLResponse)
async def migrate_page(request: Request):
    """Страница массового переноса аккаунтов между кабинетами."""
    return templates.TemplateResponse("migrate.html", {
        "request":      request,
        "accounts":     list_accounts(),
        "groups":       accounts_by_window(),
        "window_list":  get_window_list(),
        "current_user": _current_user(request),
        "selected":     None,
        "account_id":   None,
        "window_slug":  None,
    })


@app.post("/admin/migrate/move", response_class=RedirectResponse)
async def migrate_move(
    request: Request,
    target_window: str = Form(...),
    account_ids:   list[str] = Form(default=[]),
):
    """Перенести выбранные account_ids в target_window."""
    target = normalize_window_slug(target_window)
    if not target:
        return RedirectResponse(url="/admin/migrate?error=invalid_window", status_code=302)
    moved = 0
    for raw in account_ids:
        try:
            acc_id = int(raw)
        except (ValueError, TypeError):
            continue
        acc = get_account(acc_id)
        if not acc:
            continue
        old_window = acc.get("window") or "glazars"
        if old_window == target:
            continue
        db_update_account(acc_id, window=target)
        audit("account.moved", request=request, user=_current_user(request),
              target_type="account", target_id=acc_id,
              details={"from_window": old_window, "to_window": target})
        moved += 1
    return RedirectResponse(url=f"/admin/migrate?moved={moved}", status_code=302)


# ---------------------------------------------------------------------------
# Admin: Proxy management
# ---------------------------------------------------------------------------

@app.get("/admin/proxies", response_class=HTMLResponse)
async def proxies_page(request: Request):
    """Список прокси с health-статусом + форма добавления."""
    proxies = db_list_proxies()
    return templates.TemplateResponse("proxies.html", {
        "request":      request,
        "proxies":      proxies,
        "current_user": _current_user(request),
        "groups":       accounts_by_window(),
        "window_list":  get_window_list(),
        "accounts":     list_accounts(),
        "selected":     None,
        "account_id":   None,
        "window_slug":  None,
    })


@app.post("/admin/proxies/add", response_class=RedirectResponse)
async def proxies_add(
    request:  Request,
    type:     str = Form("socks5h"),
    host:     str = Form(...),
    port:     str = Form(...),
    username: str = Form(""),
    password: str = Form(""),
    label:    str = Form(""),
    region:   str = Form(""),
):
    try:
        port_i = int(port)
        if port_i < 1 or port_i > 65535 or not host.strip():
            raise ValueError
    except (ValueError, TypeError):
        return RedirectResponse(url="/admin/proxies?error=invalid", status_code=302)
    new_id = db_add_proxy(
        type=type.strip() or "socks5h",
        host=host.strip(),
        port=port_i,
        username=username.strip() or None,
        password=password.strip() or None,
        label=label.strip() or None,
        region=(region.strip().upper() or None),
    )
    if not new_id:
        return RedirectResponse(url="/admin/proxies?error=duplicate", status_code=302)
    audit("proxy.created", request=request, user=_current_user(request),
          target_type="proxy", target_id=new_id,
          details={"host": host.strip(), "port": port_i, "type": type})
    return RedirectResponse(url="/admin/proxies?success=added", status_code=302)


@app.post("/admin/proxies/add-bulk", response_class=RedirectResponse)
async def proxies_add_bulk(
    request:        Request,
    bulk_list:      str = Form(...),
    default_type:   str = Form("socks5h"),
    region:         str = Form(""),
    label_prefix:   str = Form(""),
):
    """Парсит textarea со списком прокси и добавляет все валидные.
    Поддерживает форматы:
      host:port:user:pass
      host:port
      socks5h://user:pass@host:port
      http://host:port
    """
    added = skipped = invalid = 0
    region_norm = (region or "").strip().upper() or None
    prefix = (label_prefix or "").strip()
    lines = [ln.strip() for ln in (bulk_list or "").splitlines()]
    counter = 1

    for raw_line in lines:
        # Пропускаем пустые и комментарии
        if not raw_line or raw_line.startswith("#"):
            continue
        parsed = _parse_proxy_line(raw_line, default_type=default_type)
        if not parsed:
            invalid += 1
            continue

        # Авто-label: prefix + counter, или None
        auto_label: Optional[str] = None
        if prefix:
            auto_label = f"{prefix}{counter}"

        new_id = db_add_proxy(
            type=parsed["type"],
            host=parsed["host"],
            port=parsed["port"],
            username=parsed["user"],
            password=parsed["pass"],
            label=auto_label,
            region=region_norm,
        )
        if new_id:
            added += 1
            counter += 1
            audit("proxy.created", request=request, user=_current_user(request),
                  target_type="proxy", target_id=new_id,
                  details={"host": parsed["host"], "port": parsed["port"],
                           "type": parsed["type"], "via": "bulk_import"})
        else:
            skipped += 1     # дубликат (UNIQUE constraint на host+port+username)

    return RedirectResponse(
        url=f"/admin/proxies?success=bulk&added={added}&skipped={skipped}&invalid={invalid}",
        status_code=302,
    )


def _parse_proxy_line(line: str, default_type: str = "socks5h") -> Optional[dict]:
    """Парсит одну строку прокси в dict {type, host, port, user, pass}.
    Возвращает None если строка нераспознана.
    """
    line = (line or "").strip()
    if not line:
        return None
    # Формат 1: scheme://[user:pass@]host:port
    if "://" in line:
        try:
            from urllib.parse import urlparse
            u = urlparse(line)
            if not u.hostname or not u.port:
                return None
            scheme = (u.scheme or default_type).lower()
            if scheme not in ("socks5h", "socks5", "http", "https"):
                scheme = default_type
            return {
                "type":     scheme,
                "host":     u.hostname,
                "port":     int(u.port),
                "user":     u.username or None,
                "pass":     u.password or None,
            }
        except Exception:
            return None
    # Формат 2: host:port:user:pass или host:port
    parts = line.split(":")
    if len(parts) == 2:
        host, port = parts
        user = pwd = None
    elif len(parts) == 4:
        host, port, user, pwd = parts
    else:
        return None
    try:
        port_i = int(port.strip())
        if port_i < 1 or port_i > 65535 or not host.strip():
            return None
    except (ValueError, TypeError):
        return None
    return {
        "type": default_type,
        "host": host.strip(),
        "port": port_i,
        "user": user.strip() if user else None,
        "pass": pwd.strip() if pwd else None,
    }


def _reassign_dead_proxy_accounts(
    only_proxy_url: Optional[str] = None,
    audit_request: Optional[Request] = None,
    audit_user: Optional[dict] = None,
) -> dict:
    """Сканирует все PP/AP аккаунты и переназначает с DEAD прокси на healthy.

    Args:
        only_proxy_url: если задан — переназначает ТОЛЬКО аккаунты на этом конкретном
                        прокси (для реактивного вызова после auto-disable).
                        Если None — все аккаунты с прокси НЕ из healthy-pool.

    Returns:
        {"reassigned": int, "skipped_healthy": int, "skipped_uc": int, "no_healthy": bool}
    """
    healthy = db_list_proxies(only_enabled=True, only_healthy=True)
    if not healthy:
        return {"reassigned": 0, "skipped_healthy": 0, "skipped_uc": 0, "no_healthy": True}

    healthy_urls      = set(build_proxy_url(p) for p in healthy)
    healthy_url_to_id = {build_proxy_url(p): p["id"] for p in healthy}

    # Текущая нагрузка по healthy-прокси (для load-balancing)
    usage: dict = {url: 0 for url in healthy_urls}
    accounts = list_accounts()
    for acc in accounts:
        cr = acc.get("credentials") or {}
        cur = (cr.get("proxy") or cr.get("https_proxy") or cr.get("http_proxy") or "").strip()
        if cur in healthy_urls:
            usage[cur] += 1

    reassigned = skipped_healthy = skipped_uc = 0
    for acc in accounts:
        bank_type = acc.get("bank_type")
        if bank_type not in ("personalpay", "astropay"):
            skipped_uc += 1
            continue
        cr = acc.get("credentials") or {}
        cur = (cr.get("proxy") or cr.get("https_proxy") or cr.get("http_proxy") or "").strip()
        if cur in healthy_urls:
            skipped_healthy += 1
            continue
        if only_proxy_url is not None and cur != only_proxy_url:
            continue   # реактивный режим — трогаем только заданный прокси
        if not cur and only_proxy_url is None:
            # Нет прокси вообще — не трогаем (возможно намеренно)
            # Хотя можно бы и присвоить из пула. Делаем это:
            pass
        # Pick least-loaded healthy
        best_url = min(usage, key=lambda u: usage[u])
        new_creds = dict(cr)
        new_creds["proxy"]       = best_url
        new_creds["http_proxy"]  = best_url
        new_creds["https_proxy"] = best_url
        db_update_account(acc["id"], credentials=new_creds)
        usage[best_url] += 1
        reassigned += 1
        # Кэш сбрасываем — следующий запрос пойдёт через новый прокси
        _balance_cache.pop(acc["id"], None)
        try:
            audit("proxy.reassigned",
                  request=audit_request, user=audit_user,
                  target_type="account", target_id=acc["id"],
                  details={"from_url_prefix": cur[:30] if cur else "(none)",
                           "to_proxy_id": healthy_url_to_id[best_url],
                           "auto": audit_request is None})
        except Exception:
            pass
    return {"reassigned": reassigned, "skipped_healthy": skipped_healthy,
            "skipped_uc": skipped_uc, "no_healthy": False}


@app.post("/admin/proxies/reassign-all", response_class=RedirectResponse)
async def proxies_reassign_all(request: Request):
    """Manual мягкий режим — переназначает только аккаунты с прокси НЕ из healthy-пула.
    Аккаунты на уже-healthy прокси не трогаются."""
    res = _reassign_dead_proxy_accounts(audit_request=request, audit_user=_current_user(request))
    if res.get("no_healthy"):
        return RedirectResponse(url="/admin/proxies?error=no_healthy", status_code=302)
    return RedirectResponse(
        url=f"/admin/proxies?reassigned={res['reassigned']}"
            f"&skipped_healthy={res['skipped_healthy']}"
            f"&skipped_uc={res['skipped_uc']}",
        status_code=302,
    )


def _force_reassign_all_accounts(
    audit_request: Optional[Request] = None,
    audit_user: Optional[dict] = None,
) -> dict:
    """ПРИНУДИТЕЛЬНО переназначает прокси из healthy-пула на ВСЕ PP/AP аккаунты,
    независимо от текущего состояния. Распределяет нагрузку round-robin: первый
    аккаунт → наименее загруженный healthy, второй → следующий по нагрузке, и т.д.

    Это самый агрессивный режим — даже карты с уже healthy прокси получат новый
    (возможно тот же, но всё равно через пул). Используется для:
      - первичной настройки после добавления нового пула
      - принудительного rebalance после удаления нескольких прокси
      - инициации проверки всех карт через свежий пул

    Returns: {"reassigned": int, "skipped_uc": int, "no_healthy": bool}
    """
    healthy = db_list_proxies(only_enabled=True, only_healthy=True)
    if not healthy:
        return {"reassigned": 0, "skipped_uc": 0, "no_healthy": True}

    healthy_urls      = [build_proxy_url(p) for p in healthy]
    healthy_url_to_id = {build_proxy_url(p): p["id"] for p in healthy}
    # Стартуем с равной нагрузки (force = чистый redistribution)
    usage: dict = {url: 0 for url in healthy_urls}

    reassigned = skipped_uc = 0
    accounts = list_accounts()
    for acc in accounts:
        bank_type = acc.get("bank_type")
        if bank_type not in ("personalpay", "astropay"):
            skipped_uc += 1
            continue
        # Pick least-loaded (round-robin при равной нагрузке)
        best_url = min(usage, key=lambda u: usage[u])
        cr = acc.get("credentials") or {}
        cur = (cr.get("proxy") or cr.get("https_proxy") or cr.get("http_proxy") or "").strip()
        new_creds = dict(cr)
        new_creds["proxy"]       = best_url
        new_creds["http_proxy"]  = best_url
        new_creds["https_proxy"] = best_url
        db_update_account(acc["id"], credentials=new_creds)
        usage[best_url] += 1
        reassigned += 1
        _balance_cache.pop(acc["id"], None)
        try:
            audit("proxy.force_reassigned",
                  request=audit_request, user=audit_user,
                  target_type="account", target_id=acc["id"],
                  details={"from_url_prefix": cur[:30] if cur else "(none)",
                           "to_proxy_id": healthy_url_to_id[best_url],
                           "force": True})
        except Exception:
            pass

    return {"reassigned": reassigned, "skipped_uc": skipped_uc, "no_healthy": False}


@app.post("/admin/proxies/force-reassign-all", response_class=RedirectResponse)
async def proxies_force_reassign_all(request: Request):
    """ПРИНУДИТЕЛЬНОЕ переназначение прокси на ВСЕХ PP/AP аккаунтах.
    Заменяет даже на тех картах где сейчас вроде healthy прокси.
    Используется для гарантированной чистой переналадки на свежий пул."""
    res = _force_reassign_all_accounts(audit_request=request, audit_user=_current_user(request))
    if res.get("no_healthy"):
        return RedirectResponse(url="/admin/proxies?error=no_healthy", status_code=302)
    return RedirectResponse(
        url=f"/admin/proxies?force_reassigned={res['reassigned']}"
            f"&skipped_uc={res['skipped_uc']}",
        status_code=302,
    )


@app.post("/admin/proxies/{proxy_id}/check", response_class=RedirectResponse)
async def proxies_check(request: Request, proxy_id: int):
    """Manual recheck — для быстрой проверки без ожидания background loop."""
    p = db_get_proxy(proxy_id)
    if not p:
        return RedirectResponse(url="/admin/proxies", status_code=302)
    status, latency, err = await asyncio.to_thread(proxy_health_check_one, p)
    db_mark_proxy_status(proxy_id, status, latency, err)
    return RedirectResponse(url="/admin/proxies?success=checked", status_code=302)


@app.post("/admin/proxies/{proxy_id}/toggle", response_class=RedirectResponse)
async def proxies_toggle(request: Request, proxy_id: int):
    """Включить/выключить прокси (enabled flag)."""
    p = db_get_proxy(proxy_id)
    if p:
        db_update_proxy(proxy_id, enabled=not bool(p.get("enabled")))
        audit("proxy.toggled", request=request, user=_current_user(request),
              target_type="proxy", target_id=proxy_id,
              details={"new_enabled": not bool(p.get("enabled"))})
    return RedirectResponse(url="/admin/proxies", status_code=302)


@app.post("/admin/proxies/{proxy_id}/delete", response_class=RedirectResponse)
async def proxies_delete(request: Request, proxy_id: int):
    p = db_get_proxy(proxy_id)
    db_delete_proxy(proxy_id)
    audit("proxy.deleted", request=request, user=_current_user(request),
          target_type="proxy", target_id=proxy_id,
          details={"host": (p or {}).get("host"), "port": (p or {}).get("port")})
    return RedirectResponse(url="/admin/proxies?success=deleted", status_code=302)


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
        for side_code, label in [("1", "buy"), ("0", "sell")]:
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
