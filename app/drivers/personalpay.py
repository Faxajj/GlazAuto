"""
Personal Pay API — работа по переданным credentials (dict).
Прод-версия с безопасными таймаутами.
"""
import time
import uuid
from typing import Optional

import requests
from requests import exceptions as req_exc


HTTP_TIMEOUT = (5, 12)  # (connect, read) — даём API время ответить, без лишнего ожидания


def _norm_creds(creds: dict) -> dict:
    base = (creds.get("base_url") or "https://mobile.prod.personalpay.dev").strip().rstrip("/")
    raw_device = (creds.get("device_id") or "").strip()
    paygilant_raw = (
        creds.get("x_fraud_paygilant_session_id")
        or creds.get("paygilant_session_id")
        or creds.get("x-fraud-paygilant-session-id")
        or ""
    ).strip()
    # Часто в credentials вставляют полный x-fraud-paygilant-session-id вида <device_id>_<timestamp>.
    # Для device_id берём часть до первого подчёркивания.
    paygilant_device = paygilant_raw.split("_", 1)[0].strip() if paygilant_raw else ""
    device_id = paygilant_device or raw_device
    # Если вставили не только device_id, а целый session-id с timestamp — отрежем хвост.
    if "_" in device_id:
        device_id = device_id.split("_", 1)[0].strip()

    return {
        "base_url": base,
        "username": (creds.get("username") or "").strip(),
        "password": (creds.get("password") or "").strip().strip('"').strip("'"),
        "device_id": device_id,
        "push_device_token": (creds.get("push_device_token") or "").strip(),
        "auth_token": (creds.get("auth_token") or "").strip(),
        "pin_hash": (creds.get("pin_hash") or "").strip(),
        "app_version": (creds.get("app_version") or "2.0.1074").strip(),
        "app_os": (creds.get("app_os") or creds.get("x_app_os") or "android").strip(),
        "os_version": (creds.get("os_version") or "18.6.2").strip(),
        "useragent_device": (creds.get("useragent_device") or "Apple iPhone 15 Pro Max, iOS/18.6.2").strip(),
        "user_agent": (creds.get("user_agent") or "Personal%20Pay/2.0.1074 CFNetwork/3826.600.41 Darwin/24.6.0").strip(),
        "proxy": (creds.get("proxy") or "").strip(),
        "http_proxy": (creds.get("http_proxy") or "").strip(),
        "https_proxy": (creds.get("https_proxy") or "").strip(),
    }


def _base_headers(c: dict) -> dict:
    # User-Agent как в приложении — без замены %20 на пробел
    ua = (c["user_agent"] or "Personal%20Pay/2.0.1074 CFNetwork/3826.600.41 Darwin/24.6.0").strip()
    return {
        "Accept":           "application/json, text/plain, */*",
        "Accept-Language":  "es-AR",
        "Content-Type":     "application/json",
        # Новый формат заголовков (актуальный, с x-)
        "x-app-version":    c["app_version"],
        "x-app-os":         c.get("app_os") or "android",
        # Старый формат (оставляем для совместимости)
        "appversion":       c["app_version"],
        "osversion":        c["os_version"],
        "useragent":        c["useragent_device"],
        "User-Agent":       ua,
    }


def _paygilant_id(device_id: str) -> str:
    return f"{device_id}_{int(time.time() * 1000)}"


def _session(c: dict) -> requests.Session:
    s = requests.Session()
    # На сервере (Ubuntu/VPS) частая проблема — egress IP датацентра блочится anti-fraud API.
    # Даём возможность явно прокинуть прокси через credentials.
    proxy = c.get("proxy") or ""
    http_proxy = c.get("http_proxy") or proxy
    https_proxy = c.get("https_proxy") or proxy
    if http_proxy or https_proxy:
        s.proxies.update({
            "http": http_proxy or https_proxy,
            "https": https_proxy or http_proxy,
        })
    return s




def _request_with_proxy_fallback(method: str, c: dict, url: str, **kwargs):
    """Выполняет запрос через прокси (если задан), и при ProxyError ретраит без прокси."""
    session = _session(c)
    try:
        return session.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
    except req_exc.ProxyError:
        # Если прокси сломан/заблокирован, делаем один безопасный retry напрямую.
        if c.get("proxy") or c.get("http_proxy") or c.get("https_proxy"):
            direct = dict(c)
            direct["proxy"] = direct["http_proxy"] = direct["https_proxy"] = ""
            session2 = _session(direct)
            return session2.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
        raise

def _get_token(session: requests.Session, c: dict) -> tuple:
    """Возвращает (значение для заголовка Authorization, paygilant_session_id).
    Personal Pay в перехвате шлёт Authorization без 'Bearer ' — только голый JWT. Так и отдаём."""
    if c.get("auth_token"):
        token = c["auth_token"].strip()
        # Убираем Bearer, если пользователь вставил — приложение шлёт только eyJ...
        if token.upper().startswith("BEARER "):
            token = token[7:].strip()
        return token, _paygilant_id(c["device_id"] or "no_device")
    if not all([c.get("device_id"), c.get("username"), c.get("password"), c.get("push_device_token")]):
        raise ValueError("Заполни device_id, username, password, push_device_token или задай auth_token")
    paygilant = _paygilant_id(c["device_id"])
    headers = _base_headers(c) | {"x-fraud-paygilant-session-id": paygilant}
    payload = {
        "deviceId": c["device_id"],
        "username": c["username"],
        "password": c["password"],
        "useCase": "signin",
        "pushNotifications": {"deviceToken": c["push_device_token"]},
    }
    r = _request_with_proxy_fallback("POST", c, f"{c['base_url']}/authority/v4/login", headers=headers, json=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Login failed: {r.status_code} {r.text[:500]}")
    body = r.json()
    tokens = body.get("tokens") or {}
    token = (
        tokens.get("idToken")
        or tokens.get("id_token")
        or tokens.get("accessToken")
        or tokens.get("access_token")
        or body.get("token")
    )
    if not token:
        h = {k.lower(): v for k, v in (r.headers or {}).items()}
        token = (h.get("authorization") or "").replace("Bearer ", "").strip()
    if not token:
        raise RuntimeError("Токен не найден в ответе логина. Задай PP_AUTH_TOKEN из перехвата.")
    return token, paygilant


def get_accounts(credentials: dict) -> dict:
    """GET financial-accounts — для отображения счетов."""
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
    }
    r = _request_with_proxy_fallback(
        "GET", c, f"{c['base_url']}/payments/accounts-service/v1/financial-accounts",
        headers=headers,
    )
    r.raise_for_status()
    return r.json()


def _first_account_dict(accounts):
    """Из списка счетов вернуть первый элемент-словарь (если элемент — список, взять его первый dict)."""
    if not accounts:
        return {}
    first = accounts[0]
    if isinstance(first, dict):
        return first
    if isinstance(first, list) and first:
        return first[0] if isinstance(first[0], dict) else {}
    return {}


def _first_nonempty(*values):
    """Первое непустое значение из переданных (приводится к str)."""
    for v in values:
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def get_balance(credentials: dict) -> dict:
    """Баланс по financial-accounts. Personal Pay отдаёт balanceAmount/availableCredit.
    CVU/счёт и алиас берём из реального ответа API (account.id, name и т.д.)."""
    data = get_accounts(credentials)
    if isinstance(data, list):
        accounts = data
    else:
        inner = data.get("data") if isinstance(data.get("data"), (list, dict)) else None
        if isinstance(inner, list):
            accounts = inner
        elif isinstance(inner, dict):
            accounts = inner.get("accounts") or inner.get("availableAccounts") or []
        else:
            accounts = (
                data.get("availableAccounts")
                or data.get("accounts")
                or data.get("financialAccounts")
                or []
            )
    if not accounts:
        return {"balance": 0, "cvu_number": "", "cvu_alias": "", "raw_accounts": data}
    first = _first_account_dict(accounts)
    balance = float(
        first.get("balanceAmount")
        or first.get("availableCredit")
        or first.get("balance")
        or first.get("availableBalance")
        or 0
    )
    acc_obj = first.get("account") if isinstance(first.get("account"), dict) else {}
    # Реальный номер счёта/CVU из API: сначала из вложенного account, потом из верхнего уровня
    number = _first_nonempty(
        acc_obj.get("id"),
        acc_obj.get("cvu"),
        acc_obj.get("number"),
        acc_obj.get("accountNumber"),
        acc_obj.get("accountId"),
        first.get("id"),
        first.get("number"),
        first.get("accountNumber"),
        first.get("cvu"),
    )
    # Реальный алиас/название счёта из API (например "Disponible")
    alias = _first_nonempty(
        first.get("name"),
        first.get("alias"),
        first.get("description"),
        acc_obj.get("name"),
        acc_obj.get("alias"),
    )
    return {
        "balance": balance,
        "cvu_number": number,
        "cvu_alias": alias,
        "raw_accounts": data,
    }


def beneficiary_discovery(credentials: dict, destination: str) -> dict:
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
    }
    dest = destination.strip()
    r = _request_with_proxy_fallback(
        "GET", c, f"{c['base_url']}/payments/cashout/b2c-bff-service/transfers/beneficiary-discovery/{dest}",
        headers=headers,
    )
    r.raise_for_status()
    return r.json()


def create_withdraw(
    credentials: dict,
    destination: str,
    amount: float,
    comments: str = "Varios (VAR)",
) -> dict:
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
    }
    tx_id = str(uuid.uuid1())
    payload = {
        "amount": float(amount),
        "transactionId": tx_id,
        "comments": comments,
        "destination": destination.strip(),
        "additionalInfo": {"sessionId": paygilant, "deviceId": c.get("device_id") or "no_device_id"},
    }
    r = _request_with_proxy_fallback(
        "POST", c, f"{c['base_url']}/payments/cashout/b2c-bff-service/transferences/commit-outer",
        headers=headers,
        json=payload,
    )
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        raise RuntimeError(f"{r.status_code} {body}")
    return r.json()


def get_activities_list(credentials: dict, offset: int = 0, limit: int = 15) -> dict:
    """История операций: GET mobile.prod.../platform/transactional-activity/v1/activities-list."""
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
    }
    params = {"page[offset]": offset, "page[limit]": limit}
    r = _request_with_proxy_fallback(
        "GET", c, f"{c['base_url']}/platform/transactional-activity/v1/activities-list",
        headers=headers,
        params=params,
    )
    r.raise_for_status()
    return r.json()


def get_transference_details(credentials: dict, transaction_id: str) -> dict:
    """Детали перевода (чек). Пробуем mobile.prod (как в приложении), затем prod."""
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    tid = transaction_id.strip()
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
        "x-body-version": "2",
    }
    payload = {"transactionId": tid}
    errors = []
    for base, path, name in [
        (c["base_url"], "/payments/core-transactions/transference", "mobile.prod"),
        ("https://prod.personalpay.dev", "/core-transactions-service/transference", "prod"),
    ]:
        url = base.rstrip("/") + path
        try:
            r = _request_with_proxy_fallback("POST", c, url, headers=headers, json=payload)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            resp = getattr(e, "response", None)
            err = f"{resp.status_code} {getattr(resp, 'reason', '')}" if resp is not None else str(e)
            errors.append(f"{name}: {err}")
    raise RuntimeError("Не удалось получить чек: " + "; ".join(errors))
