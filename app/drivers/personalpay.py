"""
Personal Pay API — работа по переданным credentials (dict).
Прод-версия с безопасными таймаутами.
"""
import base64
import hashlib
import json
import time
import uuid
from typing import Optional

import requests
from requests import exceptions as req_exc


HTTP_TIMEOUT = (5, 12)  # (connect, read) — даём API время ответить, без лишнего ожидания

# ---------------------------------------------------------------------------
# Кэш токенов (AstroPay-style авто-обновление через PIN)
# ---------------------------------------------------------------------------
_pp_refreshed_tokens: dict = {}     # device_id → new_token, ожидает сохранения в БД

# SHA-256 от дефолтного PIN-кода (используется если pin_hash не задан в credentials)
_DEFAULT_PIN_HASH = "3bd9fa371342f9d53b917a59430208dca20f2f508e1337dbb4969543508d2c0f"  # PIN: 464646


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
        "pin_hash": (creds.get("pin_hash") or _DEFAULT_PIN_HASH).strip(),
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
    """Заголовки точно как в оригинальном приложении PersonalPay (OkHttp 4.12, Android).
    Взяты из HAR-перехвата. Лишние заголовки убраны — PP их не шлёт."""
    return {
        "accept":           "application/json, text/plain, */*",
        "x-app-version":    c.get("app_version") or "2.0.1074",
        "x-app-os":         "android",
        "User-Agent":       "okhttp/4.12.0",
        "Accept-Encoding":  "gzip",
        "Connection":       "Keep-Alive",
    }


def _post_headers(c: dict) -> dict:
    """Заголовки для POST запросов (добавляется Content-Type)."""
    h = _base_headers(c)
    h["Content-Type"] = "application/json"
    return h


def _paygilant_id(device_id: str) -> str:
    return f"{device_id}_{int(time.time() * 1000)}"


def _pp_jwt_exp(token: str) -> Optional[float]:
    """Извлекает поле `exp` из JWT-пейлоада (без верификации подписи)."""
    try:
        parts = token.split(".")
        pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
        exp = payload.get("exp")
        return float(exp) if exp else None
    except Exception:
        return None


def consume_refreshed_token(device_id: str) -> Optional[str]:
    """Извлекает и возвращает свежий токен, полученный при авто-обновлении.
    Вызывается из main.py чтобы сохранить новый JWT в БД."""
    return _pp_refreshed_tokens.pop(device_id, None) or _pp_refreshed_tokens.pop((device_id or "")[:40], None)


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

def _get_token_direct(c: dict) -> tuple:
    """Возвращает (token, paygilant) напрямую из credentials — без авто-обновления.
    Используется внутри PIN-refresh чтобы избежать рекурсии."""
    if c.get("auth_token"):
        token = c["auth_token"].strip()
        if token.upper().startswith("BEARER "):
            token = token[7:].strip()
        return token, _paygilant_id(c.get("device_id") or "no_device")
    if not all([c.get("device_id"), c.get("username"), c.get("password"), c.get("push_device_token")]):
        raise ValueError("Заполни device_id, username, password, push_device_token или задай auth_token")
    paygilant = _paygilant_id(c["device_id"])
    headers = _post_headers(c) | {"x-fraud-paygilant-session-id": paygilant}
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


def _do_pin_refresh(c: dict) -> Optional[str]:
    """Внутренняя функция: PIN-refresh без авто-обновления (без рекурсии через _get_token).
    Возвращает новый JWT или None."""
    pin_hash = c.get("pin_hash", "").strip()
    if not pin_hash:
        return None
    try:
        token, paygilant = _get_token_direct(c)
        base_hdrs = _base_headers(c) | {
            "Authorization": token,
            "x-fraud-paygilant-session-id": paygilant,
        }
        post_hdrs = _post_headers(c) | {
            "Authorization": token,
            "x-fraud-paygilant-session-id": paygilant,
        }
        # Шаг 1 — проверяем статус PIN (как делает приложение перед вводом)
        try:
            _request_with_proxy_fallback(
                "GET", c, f"{c['base_url']}/identity/auth/pin/status",
                headers=base_hdrs,
            )
        except Exception:
            pass

        # Шаг 2 — имитируем возврат в приложение (точное тело из HAR)
        try:
            _request_with_proxy_fallback(
                "POST", c, f"{c['base_url']}/identity/auth/session/focus/changed",
                headers=post_hdrs,
                json={
                    "deviceId": c.get("device_id") or "no_device_id",
                    "sessionId": _paygilant_id(c.get("device_id") or "no_device_id"),
                    "type": "focusIn",
                },
            )
        except Exception:
            pass

        # Шаг 3 — валидируем PIN (SHA-256 hex)
        r_pin = _request_with_proxy_fallback(
            "POST", c, f"{c['base_url']}/identity/auth/pin/validate",
            headers=post_hdrs, json={"pin": pin_hash},
        )
        if r_pin.status_code not in (200, 201):
            return None
        # Шаг 3 — статус сессии
        # PP продлевает серверную сессию через PIN — новый JWT может не выдаваться.
        # Если находим JWT в ответе — возвращаем его.
        # Если нет — возвращаем существующий токен: сервер уже принял сессию как активную.
        existing_token = token  # токен с которым мы вошли (может быть "просроченным" по exp)
        try:
            r_status = _request_with_proxy_fallback(
                "GET", c, f"{c['base_url']}/identity/auth/v3/session/status",
                headers=base_hdrs,
            )
            if r_status.status_code == 200:
                try:
                    body = r_status.json()
                except Exception:
                    body = {}
                # Ищем новый JWT во всех возможных полях (включая вложенные)
                data = body.get("data") or body
                new_token = (
                    data.get("idToken") or data.get("id_token")
                    or data.get("accessToken") or data.get("access_token")
                    or data.get("token")
                    or body.get("idToken") or body.get("id_token")
                    or body.get("accessToken") or body.get("access_token")
                    or body.get("token") or ""
                )
                if new_token and str(new_token).startswith("eyJ"):
                    return str(new_token)
        except Exception:
            pass

        # PIN принят → сессия продлена на сервере.
        # Возвращаем существующий токен — он снова работает (серверный exp сброшен).
        return existing_token
    except Exception:
        return None


def _get_token(session: requests.Session, c: dict) -> tuple:
    """Возвращает (token, paygilant_session_id) для PP-запросов.
    Если pin_hash задан и JWT истёк или истекает < 5 мин — прозрачно обновляет через PIN.
    Намеренно БЕЗ кросс-запросного кэша: разные аккаунты могут иметь одинаковый device_id
    (один телефон, несколько аккаунтов) — кэш по device_id даст чужой токен."""
    raw_token = (c.get("auth_token") or "").strip()
    if raw_token.upper().startswith("BEARER "):
        raw_token = raw_token[7:].strip()

    pin_hash = c.get("pin_hash", "").strip()

    # Если есть токен и PIN — проверяем не истёк ли JWT
    if raw_token and pin_hash:
        exp = _pp_jwt_exp(raw_token)
        now = time.time()
        if not exp or exp <= now + 300:          # истёк или меньше 5 мин
            new_token = _do_pin_refresh(c)
            if new_token:
                c["auth_token"] = new_token      # обновляем c in-place
                device_id = (c.get("device_id") or "")[:40]
                _pp_refreshed_tokens[device_id] = new_token   # сигнал main.py → сохранить в БД
                return new_token, _paygilant_id(c.get("device_id") or "no_device")

    return _get_token_direct(c)


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
    headers = _post_headers(c) | {
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
    headers = _post_headers(c) | {
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


# ---------------------------------------------------------------------------
# CVU / владелец аккаунта
# ---------------------------------------------------------------------------

def get_cvu_info(credentials: dict) -> dict:
    """GET /payments/cashin/b2c-bff-service/cvu — CVU-номер и алиас счёта."""
    c = _norm_creds(credentials)
    session = _session(c)
    token, paygilant = _get_token(session, c)
    headers = _base_headers(c) | {
        "Authorization": token,
        "x-fraud-paygilant-session-id": paygilant,
    }
    r = _request_with_proxy_fallback(
        "GET", c, f"{c['base_url']}/payments/cashin/b2c-bff-service/cvu",
        headers=headers,
    )
    if r.status_code == 304:
        return {}
    r.raise_for_status()
    return r.json()


def get_owner_name_from_jwt(auth_token: str) -> Optional[str]:
    """Пытается извлечь имя владельца из JWT-токена PP (поля given_name / name / family_name)."""
    token = (auth_token or "").strip()
    if token.upper().startswith("BEARER "):
        token = token[7:].strip()
    if not token.startswith("eyJ") or "." not in token:
        return None
    try:
        parts = token.split(".")
        pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
        # PP кладёт в name UUID или email — не нужно
        # Реальное ФИО бывает в given_name + family_name
        given  = (payload.get("given_name") or "").strip()
        family = (payload.get("family_name") or "").strip()
        full   = (payload.get("full_name") or payload.get("name") or "").strip()

        # Если family_name выглядит как email — не используем
        if given and "@" not in given:
            if family and "@" not in family:
                return f"{given} {family}"
            return given
        if full and "@" not in full and len(full) > 2 and not full.startswith("9ff"):
            return full
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# PIN-refresh — обновление сессии через PIN-код
# ---------------------------------------------------------------------------

def pin_hash_from_pin(pin_code: str) -> str:
    """SHA-256 PIN-кода в нижнем регистре hex — именно так PP отправляет PIN."""
    return hashlib.sha256(pin_code.encode()).hexdigest()


def refresh_session_with_pin(credentials: dict) -> Optional[str]:
    """Публичный API: продлевает сессию PP через PIN-валидацию.
    Используется кнопкой «Обновить через PIN» в UI (через /account/{id}/pin-refresh)
    и фоновым keepalive-циклом.

    Возвращает токен (тот же или новый) если PIN принят, иначе None.
    credentials должны содержать "pin_hash" (SHA-256 от PIN-кода).
    """
    c = _norm_creds(credentials)
    if not c.get("pin_hash"):
        return None
    return _do_pin_refresh(c)
