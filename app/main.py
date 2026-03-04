"""
Единый дашборд банков: несколько аккаунтов, переключение между ними.
"""
import base64
import html
import json
import secrets
import time
import traceback
from typing import Optional, Tuple
from urllib.parse import quote

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import (
    add_account as db_add_account,
    accounts_by_window,
    delete_account as db_delete_account,
    get_account,
    init_db,
    list_accounts,
    update_account as db_update_account,
    WINDOWS,
    add_auto_withdraw_rule,
    create_user,
    get_auto_withdraw_rule,
    get_user_by_username,
    list_auto_withdraw_rules,
    update_auto_withdraw_progress,
    verify_password,
create_session,
get_session,
delete_session,
cleanup_sessions,
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

SESSION_COOKIE = "session_token"
SESSION_TTL = 60 * 60 * 8  # 8 часов

app = FastAPI(title="Banks Dashboard — несколько аккаунтов")

init_db()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


def _tojson(obj, indent=2):
    return json.dumps(obj, indent=indent, ensure_ascii=False)


templates.env.filters["tojson"] = _tojson

CONCEPTS_UC = [
    ("VARIOS", "VARIOS (разное)"),
    ("ALQUILER", "ALQUILER (аренда)"),
    ("HONORARIOS", "HONORARIOS (гонорары)"),
    ("COMPRA", "COMPRA (покупка)"),
    ("VENTA", "VENTA (продажа)"),
]


def _current_user(request: Request):
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


def _format_ars(value, decimals: int = 2) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "0"
    if decimals == 0:
        return f"{num:,.0f}".replace(",", ".")
    return f"{num:,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        public_paths = {"/login", "/static"}
        if any(path == p or path.startswith(f"{p}/") for p in public_paths):
            return await call_next(request)
        if path == "/health":
            return await call_next(request)
        if not _current_user(request):
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)


app.add_middleware(AuthMiddleware)


def _ensure_admin_user() -> None:
    username = "admin"
    password = "admin123"

    user = get_user_by_username(username)
    if user:
        return

    try:
        create_user(username, password, is_active=True)
    except Exception:
        pass


_ensure_admin_user()


templates.env.filters["ars"] = _format_ars

def _extract_activities_raw(data) -> list:
    """Из ответа API activities-list извлекает список операций (разные форматы из логов)."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    # Плоский список в ключе
    for key in ("data", "activities", "items", "content", "results", "list", "records"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    # Вложенный: data -> { activities | items | list }
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("activities", "items", "list", "content", "results"):
            val = inner.get(key)
            if isinstance(val, list):
                return val
        if isinstance(inner.get("data"), list):
            return inner["data"]
    # JSON:API included
    if isinstance(data.get("included"), list):
        return data["included"]
    return []


def _find_any_in_dict(obj, keys: tuple) -> Optional[object]:
    """Рекурсивно ищет в dict/list первое непустое значение по ключам keys (глубина до 5)."""
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
    """Рекурсивно ищет в dict/list первое значение по ключам keys (глубина до 5)."""
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
    """Ищет в details/items по label/key одно из labels и возвращает value."""
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
    """Достаёт значение по цепочке ключей, например _get_nested(act, 'transactionData', 'origin', 'holder')."""
    if not isinstance(obj, dict):
        return None
    for key in path:
        obj = obj.get(key) if isinstance(obj, dict) else None
        if obj is None:
            return None
    s = str(obj).strip() if obj is not None else ""
    return s if s else None


def _normalize_activity(act: dict) -> dict:
    """Из сырой операции извлекает: title, amount, date, is_outgoing (приход/вывод), receipt_id."""
    if not isinstance(act, dict):
        return {}
    attrs = act.get("attributes") or act
    aid = act.get("id") or act.get("transactionId") or attrs.get("id") or attrs.get("transactionId")
    title = (
        attrs.get("title")
        or attrs.get("description")
        or act.get("title")
        or act.get("description")
        or str(aid or "Операция")
    )
    receipt_id = _find_32char_hex_id(act)

    # Сумма: amount, monto, value (в т.ч. рекурсивно по всему объекту из логов API)
    amount = attrs.get("amount") or attrs.get("monto") or act.get("amount") or act.get("monto")
    if amount is not None:
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            amount = None
    if amount is None:
        amount = _find_in_dict(act, "amount", "monto", "value", "total", "ars", want_number=True)

    # Дата: date, fecha, createdAt, created_at, timestamp (в т.ч. из вложенных объектов)
    date_raw = (
        attrs.get("date")
        or attrs.get("fecha")
        or attrs.get("createdAt")
        or attrs.get("created_at")
        or act.get("date")
        or act.get("fecha")
        or act.get("createdAt")
        or act.get("timestamp")
    )
    if date_raw is None:
        date_raw = _find_any_in_dict(act, ("createdAt", "date", "fecha", "timestamp", "created_at"))
    date_str = None
    if date_raw:
        if isinstance(date_raw, (int, float)):
            from datetime import datetime
            try:
                date_str = datetime.utcfromtimestamp(date_raw / 1000 if date_raw > 1e12 else date_raw).strftime("%d.%m.%Y %H:%M")
            except Exception:
                date_str = str(date_raw)
        else:
            date_str = str(date_raw)[:19] if date_raw else None

    # Направление: приход или вывод
    tx_type = (attrs.get("transactionType") or attrs.get("type") or act.get("transactionType") or act.get("type") or "").lower()
    title_lower = (title or "").lower()
    is_outgoing = (
        "output" in tx_type
        or "out" in tx_type
        or "outgoing" in tx_type
        or "enviaste" in title_lower
        or "envío" in title_lower
        or "transferencia enviada" in title_lower
    )

    # Объект для парсинга: список может возвращать { "transference": { details, transactionData } }
    inner = act.get("transference") if isinstance(act.get("transference"), dict) else act

    # От кого / Кому: из details (Envía/Recibe), transactionData.origin/destination, верхний уровень
    sender = (
        attrs.get("remitente")
        or attrs.get("sender")
        or attrs.get("originName")
        or attrs.get("senderName")
        or act.get("remitente")
        or act.get("sender")
        or act.get("from")
        or _find_in_details(act, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _find_in_details(inner, "remitente", "titular", "origen", "sender", "nombre", "envía", "envia")
        or _get_nested(inner, "transactionData", "origin", "holder")
        or _find_in_dict(act, "remitente", "sender", "originName", "holder", want_number=False)
        or _find_in_dict(inner, "remitente", "sender", "originName", "holder", want_number=False)
    )
    recipient = (
        attrs.get("destinatario")
        or attrs.get("recipient")
        or attrs.get("recipientName")
        or act.get("destinatario")
        or act.get("recipient")
        or act.get("to")
        or _find_in_details(act, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _find_in_details(inner, "destinatario", "beneficiario", "recipient", "nombre", "recibe")
        or _get_nested(inner, "transactionData", "destination", "holder")
        or _get_nested(inner, "transactionData", "destination", "label")
        or _find_in_dict(act, "destinatario", "recipient", "beneficiary", "label", "holder", want_number=False)
        or _find_in_dict(inner, "destinatario", "recipient", "beneficiary", "label", "holder", want_number=False)
    )
    if isinstance(sender, str):
        sender = sender.strip() or None
    if isinstance(recipient, str):
        recipient = recipient.strip() or None

    return {
        "id": aid,
        "title": title,
        "receipt_id": receipt_id,
        "amount": amount,
        "date_str": date_str,
        "is_outgoing": is_outgoing,
        "sender": sender,
        "recipient": recipient,
        "_raw": act,
    }


def _find_32char_hex_id(obj) -> Optional[str]:
    """Рекурсивно ищет в dict/list строку — 32 символа, hex без дефисов (ID операции банка для чека)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and "-" not in v and len(v) == 32 and all(c in "0123456789ABCDEFabcdef" for c in v):
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


def _jwt_expiry(credentials: dict) -> Tuple[bool, Optional[float]]:
    """Если в credentials есть auth_token (JWT), возвращает (истёк_ли, часов_до_истечения или None)."""
    token = (credentials.get("auth_token") or "").strip()
    if token.upper().startswith("BEARER "):
        token = token[7:].strip()
    if not token or not token.startswith("eyJ"):
        return False, None
    parts = token.split(".")
    if len(parts) < 2:
        return False, None
    try:
        payload_b64 = parts[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return False, None
    exp = payload.get("exp")
    if not exp:
        return False, None
    now = time.time()
    if now >= exp:
        return True, 0.0
    return False, (exp - now) / 3600.0


def _window_name(slug: str) -> str:
    for s, name in WINDOWS:
        if s == slug:
            return name
    return slug


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    if _current_user(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": error},
    )


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

    create_session(
        token,
        user["id"],
        user["username"],
        int(time.time() + SESSION_TTL),
    )

    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=SESSION_TTL,
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



@app.get("/", response_class=HTMLResponse)
async def index(request: Request, account_id: Optional[int] = None, window: Optional[str] = None):
    try:
        return await _index_impl(request, account_id, window)
    except Exception as e:
        tb = html.escape(traceback.format_exc())
        return HTMLResponse(
            content=f'<html><body style="font-family:sans-serif;padding:2rem;background:#1e293b;color:#e2e8f0;">'
            f'<h1>Ошибка</h1><pre style="white-space:pre-wrap;background:#0f172a;padding:1rem;border-radius:8px;">{tb}</pre>'
            f'<p><a href="/" style="color:#94a3b8;">На главную</a></p></body></html>',
            status_code=500,
        )


async def _index_impl(request: Request, account_id: Optional[int], window: Optional[str]):
    accounts = list_accounts()
    groups = accounts_by_window()
    selected = None
    balance_info = None
    accounts_display = None
    error = None
    window_accounts = []
    window_slug = None
    window_name = None
    if window and window in [w[0] for w in WINDOWS]:
        window_slug = window
        window_name = _window_name(window)
        window_accounts = groups.get(window, [])
    if account_id:
        selected = get_account(account_id)
        if selected:
            try:
                balance_info = driver_balance(selected["bank_type"], selected["credentials"])
                if selected["bank_type"] == "personalpay":
                    accounts_display = balance_info.get("raw_accounts")
                    balance_info = {
                        "balance": balance_info.get("balance", 0),
                        "cvu_number": balance_info.get("cvu_number", ""),
                        "cvu_alias": balance_info.get("cvu_alias", ""),
                    }
                else:
                    balance_info = {
                        "balance": balance_info.get("balance", 0),
                        "cvu_number": balance_info.get("cvu_number", ""),
                        "cvu_alias": balance_info.get("cvu_alias", ""),
                    }
                if balance_info.get("balance") is None:
                    balance_info["balance"] = 0
                else:
                    try:
                        balance_info["balance"] = float(balance_info["balance"])
                    except (TypeError, ValueError):
                        balance_info["balance"] = 0
            except Exception as e:
                balance_info = {"error": str(e)}
                error = str(e)
    token_expired = False
    token_expires_in_hours = None
    if selected and selected.get("credentials") and selected["credentials"].get("auth_token"):
        try:
            token_expired, token_expires_in_hours = _jwt_expiry(selected["credentials"])
        except Exception:
            token_expired, token_expires_in_hours = False, None
    activities_list = []
    auto_rules = list_auto_withdraw_rules(selected["id"]) if selected else []
    auto_run_result = None
    if selected and auto_rules:
        for rule in auto_rules:
            if int(rule.get("is_active", 0)) != 1:
                continue
            ok, msg = _run_auto_withdraw_rule(selected, rule)
            auto_run_result = "Автовывод выполнен." if ok else f"Автовывод: {msg}"
            break
        auto_rules = list_auto_withdraw_rules(selected["id"])
    if selected and selected["bank_type"] == "personalpay":
        try:
            data = pp_activities_list(selected["credentials"], offset=0, limit=15)
            raw = _extract_activities_raw(data)
            for act in raw if isinstance(raw, list) else []:
                normalized = _normalize_activity(act)
                if normalized:
                    activities_list.append(normalized)
        except Exception:
            pass
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "accounts": accounts,
            "groups": groups,
            "window_list": WINDOWS,
            "selected": selected,
            "account_id": account_id,
            "balance_info": balance_info,
            "accounts_display": accounts_display,
            "bank_types": BANK_TYPES,
            "concepts_uc": CONCEPTS_UC,
            "error": error,
            "token_expired": token_expired,
            "token_expires_in_hours": token_expires_in_hours,
            "activities_list": activities_list or [],
            "window_slug": window_slug,
            "window_name": window_name,
            "window_accounts": window_accounts,
            "current_user": _current_user(request),
            "auto_rules": auto_rules,
            "auto_run_result": auto_run_result,
            "prefill": {
                "cvu": "",
                "destination": "",
                "amount": "",
                "concept": "VARIOS",
                "comments": "Varios (VAR)",
                "alias": "",
                "document": "",
                "name": "",
                "bank": "",
            },
        },
    )


@app.get("/account/{account_id}/balance")
async def api_balance(account_id: int):
    """JSON: актуальный баланс для аккаунта (для кнопки и автообновления)."""
    acc = get_account(account_id)
    if not acc:
        return {"error": "account not found"}
    try:
        balance_info = driver_balance(acc["bank_type"], acc["credentials"])
        if acc["bank_type"] == "personalpay":
            return {
                "balance": balance_info.get("balance", 0),
                "cvu_number": balance_info.get("cvu_number", ""),
                "cvu_alias": balance_info.get("cvu_alias", ""),
            }
        return {
            "balance": balance_info.get("balance", 0),
            "cvu_number": balance_info.get("cvu_number", ""),
            "cvu_alias": balance_info.get("cvu_alias", ""),
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/account/{account_id}/withdraw", response_class=HTMLResponse)
async def withdraw(
    request: Request,
    account_id: int,
    cvu: str = Form(""),
    destination: str = Form(""),
    amount: str = Form(...),
    concept: str = Form("VARIOS"),
    comments: str = Form("Varios (VAR)"),
    alias: str = Form(""),
    document: str = Form(""),
    name: str = Form(""),
    bank: str = Form(""),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    try:
        amt = float(amount.replace(",", "."))
    except ValueError:
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_amount", status_code=302)
    if amt <= 0:
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_amount", status_code=302)
    dest = (destination or cvu).strip()
    if not dest:
        return RedirectResponse(url=f"/?account_id={account_id}&error=no_destination", status_code=302)
    try:
        if acc["bank_type"] == "universalcoins":
            doc_clean = (document or "").strip().replace("-", "").replace(" ", "")
            if not doc_clean or len(doc_clean) < 10:
                return RedirectResponse(
                    url=f"/?account_id={account_id}&error=document_required",
                    status_code=302,
                )
            result = driver_withdraw(
                acc["bank_type"],
                acc["credentials"],
                cvu_recipient=dest,
                amount=amt,
                concept=concept,
                alias_recipient=alias.strip() or None,
                document_recipient=doc_clean,
                name_recipient=name.strip() or None,
                bank_recipient=bank.strip() or None,
            )
        else:
            result = driver_withdraw(
                acc["bank_type"],
                acc["credentials"],
                destination=dest,
                amount=amt,
                comments=comments,
            )
    except Exception as e:
        err_msg = str(e)
        if any(x in err_msg.lower() for x in ("rechazad", "rejected", "rechazo", "denied", "denegad")):
            error_param = "rejected_by_bank"
        else:
            error_param = quote(err_msg[:200], safe="")
        return RedirectResponse(
            url=f"/?account_id={account_id}&error={error_param}",
            status_code=302,
        )
    tid = None
    if acc["bank_type"] == "personalpay" and isinstance(result, dict):
        tid = _find_32char_hex_id(result)
        if not tid:
            raw = (
                result.get("transactionId")
                or result.get("id")
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


def _run_auto_withdraw_rule(acc: dict, rule: dict) -> tuple[bool, str]:
    remaining = max(0.0, float(rule.get("total_limit", 0)) - float(rule.get("paid_amount", 0)))
    if remaining <= 0:
        update_auto_withdraw_progress(rule["id"], is_active=False)
        return False, "Лимит достигнут"
    chunk = min(float(rule.get("chunk_amount", 0)), remaining)
    if chunk <= 0:
        update_auto_withdraw_progress(rule["id"], last_error="Некорректная сумма", is_active=False)
        return False, "Некорректная сумма"

    # Проверяем баланс — если задан минимальный порог, не выводим пока баланс ниже него
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
            # Молча пропускаем — не спамим ошибками, просто ждём
            return False, f"Баланс {current_balance:,.0f} < порога {min_balance:,.0f} — ожидание пополнения"

    try:
        if acc["bank_type"] == "universalcoins":
            raise ValueError("Автовывод поддерживается только для Personal Pay")
        result = driver_withdraw(
            acc["bank_type"],
            acc["credentials"],
            destination=rule["cvu"],
            amount=chunk,
            comments="Auto withdraw",
        )
        update_auto_withdraw_progress(rule["id"], paid_delta=chunk, last_error="")
    except Exception as e:
        update_auto_withdraw_progress(rule["id"], last_error=str(e))
        return False, str(e)

    tid = _find_32char_hex_id(result) if isinstance(result, dict) else None
    return True, (tid or "")


@app.post("/account/{account_id}/auto-withdraw", response_class=RedirectResponse)
async def create_auto_withdraw(
    account_id: int,
    cvu: str = Form(""),
    total_limit: str = Form(""),
    chunk_amount: str = Form(""),
    min_balance: str = Form("0"),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    try:
        total = float((total_limit or "").replace(" ", "").replace(".", "").replace(",", "."))
        chunk = float((chunk_amount or "").replace(" ", "").replace(".", "").replace(",", "."))
        min_bal = float((min_balance or "0").replace(" ", "").replace(".", "").replace(",", "."))
    except ValueError:
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_amount", status_code=302)
    if total <= 0 or chunk <= 0 or chunk > total or not cvu.strip():
        return RedirectResponse(url=f"/?account_id={account_id}&error=invalid_auto_rule", status_code=302)
    add_auto_withdraw_rule(account_id, cvu.strip(), total, chunk, min_bal)
    return RedirectResponse(url=f"/?account_id={account_id}&success=auto_rule_created", status_code=302)


@app.post("/account/{account_id}/auto-withdraw/{rule_id}/delete", response_class=RedirectResponse)
async def delete_auto_withdraw(account_id: int, rule_id: int):
    from app.database import _get_conn
    with _get_conn() as conn:
        conn.execute(
            "DELETE FROM auto_withdraw_rules WHERE id = ? AND account_id = ?",
            (rule_id, account_id),
        )
        conn.commit()
    return RedirectResponse(url=f"/?account_id={account_id}", status_code=302)


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



@app.get("/add", response_class=HTMLResponse)
async def add_account_page(request: Request, window: str = ""):
    return templates.TemplateResponse(
        "add_account.html",
        {
            "request": request,
            "bank_types": BANK_TYPES,
            "window_list": WINDOWS,
            "preselect_window": window or "glazars",
            "accounts": list_accounts(),
            "groups": accounts_by_window(),
            "account_id": None,
            "selected": None,
            "window_slug": None,
        },
    )




def _proxy_to_url(raw: str, scheme: str = "socks5h") -> str:
    """Нормализует прокси из UI.

    Поддержка:
    - host:port:user:pass -> <scheme>://user:pass@host:port
    - user:pass@host:port -> <scheme>://user:pass@host:port
    - host:port -> <scheme>://host:port
    - готовый URL (socks5://, socks5h://, http://, https://) -> как есть
    """
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


def _proxy_from_parts(
    proxy_host: str,
    proxy_port: str,
    proxy_user: str,
    proxy_password: str,
    proxy_type: str,
    proxy_raw: str,
) -> str:
    """Собирает proxy URL из отдельных полей. Если поля пустые — fallback к raw строке."""
    scheme = (proxy_type or "socks5h").strip().lower()
    if scheme not in ("socks5", "socks5h", "http", "https"):
        scheme = "socks5h"

    host = (proxy_host or "").strip()
    port = (proxy_port or "").strip()
    user = (proxy_user or "").strip()
    password = (proxy_password or "").strip()

    if host and port:
        auth = f"{user}:{password}@" if user and password else ""
        return f"{scheme}://{auth}{host}:{port}"

    return _proxy_to_url(proxy_raw, scheme=scheme) if (proxy_raw or "").strip() else ""


def _apply_proxy_to_credentials(proxy_url: str, credentials: dict) -> dict:
    """Добавляет/обновляет proxy-поля в credentials."""
    proxy_url = (proxy_url or "").strip()
    if not proxy_url:
        return credentials
    c = dict(credentials or {})
    c["proxy"] = proxy_url
    c["http_proxy"] = proxy_url
    c["https_proxy"] = proxy_url
    return c


def _proxy_parts_from_credentials(credentials: dict) -> dict:
    """Достаёт proxy fields для формы редактирования из сохранённого URL."""
    c = credentials or {}
    raw = (c.get("https_proxy") or c.get("proxy") or c.get("http_proxy") or "").strip()
    result = {
        "proxy_type": "socks5h",
        "proxy_host": "",
        "proxy_port": "",
        "proxy_user": "",
        "proxy_password": "",
        "proxy_raw": raw,
    }
    if not raw:
        return result

    low = raw.lower()
    if "://" in raw:
        scheme, rest = raw.split("://", 1)
        if scheme.lower() in ("socks5", "socks5h", "http", "https"):
            result["proxy_type"] = scheme.lower()
        raw = rest

    auth, hostport = (raw.split("@", 1) + [""])[:2] if "@" in raw else ("", raw)
    if auth and ":" in auth:
        u, p = auth.split(":", 1)
        result["proxy_user"] = u
        result["proxy_password"] = p

    if hostport and ":" in hostport:
        h, pt = hostport.rsplit(":", 1)
        result["proxy_host"] = h
        result["proxy_port"] = pt
    else:
        result["proxy_host"] = hostport
    return result


def _parse_credentials(raw: str) -> dict:
    """Парсит credentials: либо JSON, либо токен (в т.ч. строка вида Authorization: eyJ...)."""
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Убираем префикс заголовка, если вставили целиком "Authorization: eyJ..." или "Authorization:\neyJ..."
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


@app.post("/add", response_class=RedirectResponse)
async def add_account_post(
    bank_type: str = Form(...),
    label: str = Form(...),
    credentials_json: str = Form("{}"),
    proxy_type: str = Form("socks5h"),
    proxy_host: str = Form(""),
    proxy_port: str = Form(""),
    proxy_user: str = Form(""),
    proxy_password: str = Form(""),
    proxy_raw: str = Form(""),
    window: str = Form("glazars"),
):
    if bank_type not in BANK_TYPES:
        return RedirectResponse(url="/add?error=invalid_bank", status_code=302)
    if window not in (w[0] for w in WINDOWS):
        window = "glazars"
    credentials = _parse_credentials(credentials_json)
    if not credentials and credentials_json.strip():
        return RedirectResponse(url="/add?error=invalid_json", status_code=302)
    if bank_type == "personalpay":
        proxy_url = _proxy_from_parts(
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_user=proxy_user,
            proxy_password=proxy_password,
            proxy_type=proxy_type,
            proxy_raw=proxy_raw,
        )
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
    proxy_parts = _proxy_parts_from_credentials(acc.get("credentials") or {})
    return templates.TemplateResponse(
        "edit_account.html",
        {
            "request": request,
            "account": acc,
            "window_list": WINDOWS,
            "accounts": list_accounts(),
            "groups": accounts_by_window(),
            "account_id": None,
            "selected": acc,
            "window_slug": None,
            **proxy_parts,
        },
    )


@app.post("/account/{account_id}/edit", response_class=RedirectResponse)
async def edit_account_post(
    account_id: int,
    label: str = Form(...),
    credentials_json: str = Form("{}"),
    proxy_type: str = Form("socks5h"),
    proxy_host: str = Form(""),
    proxy_port: str = Form(""),
    proxy_user: str = Form(""),
    proxy_password: str = Form(""),
    proxy_raw: str = Form(""),
    window: str = Form(""),
):
    acc = get_account(account_id)
    if not acc:
        return RedirectResponse(url="/", status_code=302)
    credentials = _parse_credentials(credentials_json)
    if not credentials and credentials_json.strip():
        return RedirectResponse(url=f"/account/{account_id}/edit?error=invalid_json", status_code=302)
    if acc["bank_type"] == "personalpay":
        proxy_url = _proxy_from_parts(
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_user=proxy_user,
            proxy_password=proxy_password,
            proxy_type=proxy_type,
            proxy_raw=proxy_raw,
        )
        credentials = _apply_proxy_to_credentials(proxy_url, credentials)
    if label.strip():
        db_update_account(account_id, label=label.strip())
    if credentials:
        db_update_account(account_id, credentials=credentials)
    if window and window in (w[0] for w in WINDOWS):
        db_update_account(account_id, window=window)
    return RedirectResponse(url=f"/?account_id={account_id}&success=updated", status_code=302)


@app.get("/account/{account_id}/receipt", response_class=HTMLResponse)
async def receipt(request: Request, account_id: int, transaction_id: str = ""):
    """Детали перевода (чек) Personal Pay по transactionId."""
    acc = get_account(account_id)
    if not acc or acc["bank_type"] != "personalpay" or not transaction_id.strip():
        return RedirectResponse(url=f"/?account_id={account_id}", status_code=302)
    try:
        data = pp_transference_details(acc["credentials"], transaction_id)
    except Exception as e:
        return templates.TemplateResponse(
            "receipt.html",
            {
                "request": request,
                "account": acc,
                "transaction_id": transaction_id,
                "error": str(e),
                "transference": None,
                "groups": accounts_by_window(),
                "window_list": WINDOWS,
                "window_slug": None,
            },
        )
    transference = (data.get("transference") or data) if isinstance(data, dict) else None
    receipt_lines = []
    if transference and isinstance(transference, dict):
        details = transference.get("details") or []
        label_ru = {
            "fecha": "Дата",
            "date": "Дата",
            "monto": "Сумма",
            "amount": "Сумма",
            "estado": "Статус",
            "status": "Статус",
            "remitente": "Отправитель",
            "titular": "Отправитель",
            "origen": "Отправитель",
            "sender": "Отправитель",
            "cuenta origen": "Отправитель",
            "nombre": "Имя получателя",
            "name": "Имя получателя",
            "destinatario": "Получатель",
            "recipient": "Получатель",
            "beneficiario": "Получатель",
            "cuit": "Получатель CUIT",
            "cvu": "Получатель CVU",
            "banco": "Банк получателя",
            "bank": "Банк получателя",
            "banco origen": "Банк отправителя",
            "banco remitente": "Банк отправителя",
            "origen banco": "Банк отправителя",
            "sender bank": "Банк отправителя",
            "banco emisor": "Банк отправителя",
            "id": "ID",
            "balance": "Баланс",
        }
        skip_labels = {"utr"}
        seen_recipient_value = None
        for d in details:
            if not isinstance(d, dict):
                continue
            label = (d.get("label") or d.get("key") or "").strip().lower()
            value = d.get("value") or d.get("displayValue") or ""
            if label in skip_labels:
                continue
            label_show = label_ru.get(label) or (d.get("label") or d.get("key") or "")
            if label_show and label_show.strip().lower() in skip_labels:
                continue
            if not (label_show or value):
                continue
            if label_show == "Получатель" and str(value).strip() == str(seen_recipient_value or "").strip():
                continue
            if label_show == "Имя получателя" and str(value).strip() == str(seen_recipient_value or "").strip():
                continue
            if label_show in ("Получатель", "Имя получателя"):
                seen_recipient_value = value
            receipt_lines.append({"label": label_show or label, "value": value})
        tid = transference.get("id") or transaction_id
        if tid and not any((str(r.get("value") or "").strip() == str(tid).strip() for r in receipt_lines)):
            receipt_lines.append({"label": "ID", "value": tid})
    tx_type = (transference or {}).get("transactionType") or ""
    is_outgoing = "output" in tx_type.lower() or "out" in tx_type.lower() or (transference or {}).get("title", "").lower().startswith("enviaste")
    amount_val = (transference or {}).get("amount")
    amount_display = f"-{amount_val}" if (is_outgoing and amount_val is not None and float(amount_val) > 0) else (amount_val if amount_val is not None else "")
    return templates.TemplateResponse(
        "receipt.html",
        {
            "request": request,
            "account": acc,
            "transaction_id": transaction_id,
            "error": None,
            "transference": transference,
            "receipt_lines": receipt_lines,
            "receipt_title": "Исходящая транзакция" if is_outgoing else (transference or {}).get("title") or "Чек перевода",
            "amount_display": amount_display,
            "is_outgoing": is_outgoing,
            "groups": accounts_by_window(),
            "window_list": WINDOWS,
            "window_slug": None,
        },
    )


@app.post("/account/{account_id}/delete", response_class=RedirectResponse)
async def delete_account(account_id: int, redirect_window: Optional[str] = Form(None)):
    db_delete_account(account_id)
    if redirect_window and redirect_window in [w[0] for w in WINDOWS]:
        return RedirectResponse(url=f"/?window={redirect_window}", status_code=302)
    return RedirectResponse(url="/", status_code=302)


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
