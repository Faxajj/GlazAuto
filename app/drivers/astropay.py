"""
Драйвер AstroPay (mapi.astropaycard.com).

Авторизация: Bearer токен + автообновление через refresh_token.
access_token живёт ~10 минут, refresh_token ~1 год.

credentials = {
    "auth_token":    "Bearer XpN4LSU...",   # access_token (перехватывается из HAR)
    "refresh_token": "1ZMuRaq...",          # refresh_token (перехватывается из HAR)
    "amp_device_id": "5e0e7e61-...",        # постоянный ID устройства
    "amp_session_id": "177662...",          # ID сессии
    "aifa":          "82ddb8a7-...",        # рекламный ID
    "asid":          "4b9a05c2-...",        # ID установки
    "state_token":   "2bf42e10...",         # токен состояния
    "discover_token":"AAAAAAAA..."          # токен обнаружения
}
"""
import json
import time
import threading
import requests
from typing import Optional

BASE_URL    = "https://mapi.astropaycard.com"
HTTP_TIMEOUT = (8, 20)

# Кэш токенов в памяти: {account_key: {access_token, refresh_token, expires_at}}
_token_cache: dict = {}
_token_lock = threading.Lock()


def _account_key(credentials: dict) -> str:
    """Уникальный ключ аккаунта для кэша токенов."""
    return (credentials.get("amp_device_id") or
            credentials.get("refresh_token") or
            credentials.get("auth_token") or "default")[:40]


def _base_headers(credentials: dict) -> dict:
    """Постоянные заголовки устройства — не меняются между запросами."""
    return {
        "Content-Type":    "application/json; charset=utf-8",
        "Accept-Language": "es-AR",
        "AppVersion":      credentials.get("app_version") or "5.77.0-prod-release",
        "Device-Info":     credentials.get("device_info") or "Google|Pixel 5|12|5.77.0-prod-release",
        "Country":         "AR",
        "AppName":         "apc",
        "Platform":        "android",
        "TimeZone":        "America/Argentina/Buenos_Aires",
        "User-Agent":      "okhttp/4.12.0",
        "Accept-Encoding": "gzip",
        "AMP-Device-ID":   credentials.get("amp_device_id") or "",
        "AMP-Session-ID":  credentials.get("amp_session_id") or "",
        "Aifa":            credentials.get("aifa") or "",
        "Asid":            credentials.get("asid") or "",
        "State-Token":     credentials.get("state_token") or "",
        "Discover-Token":  credentials.get("discover_token") or "",
    }


def _get_valid_token(credentials: dict) -> str:
    """
    Возвращает действующий access_token.
    Если истёк — автоматически обновляет через refresh_token.
    """
    key = _account_key(credentials)

    with _token_lock:
        cached = _token_cache.get(key)
        now = time.time()

        # Если кэш есть и токен ещё действует (с запасом 60 сек)
        if cached and cached["expires_at"] > now + 60:
            return cached["access_token"]

        # Пробуем взять токен из credentials
        token = (credentials.get("auth_token") or "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()

        refresh_token = (credentials.get("refresh_token") or "").strip()

        # Если есть refresh_token — обновляем
        if refresh_token:
            try:
                new_token, new_refresh, expires_in = _do_refresh(credentials, token, refresh_token)
                _token_cache[key] = {
                    "access_token":  new_token,
                    "refresh_token": new_refresh,
                    "expires_at":    now + expires_in,
                }
                # Обновляем credentials в памяти чтобы следующий вызов тоже работал
                credentials["auth_token"]    = "Bearer " + new_token
                credentials["refresh_token"] = new_refresh
                return new_token
            except Exception as e:
                # Если refresh не сработал — используем текущий токен
                pass

        # Используем токен как есть
        if not cached:
            _token_cache[key] = {
                "access_token":  token,
                "refresh_token": refresh_token,
                "expires_at":    now + 500,  # предполагаем ~8 мин
            }
        return token


def _do_refresh(credentials: dict, access_token: str, refresh_token: str):
    """
    POST /v4/auth/refresh
    Возвращает (new_access_token, new_refresh_token, expires_in_seconds)
    """
    headers = _base_headers(credentials)
    headers["Authorization"] = "Basic"  # именно так в HAR

    payload = {"refresh_token": refresh_token}
    if access_token:
        payload["access_token"] = access_token

    proxy = (
        credentials.get("proxy") or
        credentials.get("https_proxy") or
        credentials.get("http_proxy") or ""
    ).strip()
    proxies = {"http": proxy, "https": proxy} if proxy else {}

    resp = requests.post(
        f"{BASE_URL}/v4/auth/refresh",
        json=payload,
        headers=headers,
        proxies=proxies,
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    session = data.get("session") or {}
    new_access  = session.get("access_token") or access_token
    new_refresh = session.get("refresh_token") or refresh_token
    expires_in  = int(session.get("access_expires_in") or 599)

    return new_access, new_refresh, expires_in


def _session(credentials: dict) -> requests.Session:
    """Создаёт requests.Session с актуальным токеном."""
    s = requests.Session()
    token = _get_valid_token(credentials)
    headers = _base_headers(credentials)
    headers["Authorization"] = "Bearer " + token
    s.headers.update(headers)

    proxy = (
        credentials.get("proxy") or
        credentials.get("https_proxy") or
        credentials.get("http_proxy") or ""
    ).strip()
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}

    return s


def _handle_401(credentials: dict) -> Optional[str]:
    """При 401 принудительно сбрасываем кэш и пробуем refresh."""
    key = _account_key(credentials)
    with _token_lock:
        _token_cache.pop(key, None)
    try:
        refresh_token = (credentials.get("refresh_token") or "").strip()
        if not refresh_token:
            return None
        new_token, new_refresh, expires_in = _do_refresh(credentials, "", refresh_token)
        with _token_lock:
            _token_cache[key] = {
                "access_token":  new_token,
                "refresh_token": new_refresh,
                "expires_at":    time.time() + expires_in,
            }
        credentials["auth_token"]    = "Bearer " + new_token
        credentials["refresh_token"] = new_refresh
        return new_token
    except Exception:
        return None


def get_balance(credentials: dict) -> dict:
    """
    Баланс ARS кошелька и CVU.
    Ответ: {"balance": float, "cvu_number": str, "cvu_alias": str}
    """
    s = _session(credentials)
    resp = s.get(
        f"{BASE_URL}/v2/wallet",
        params={"order_by": "BALANCE", "currency_type": "ALL"},
        timeout=HTTP_TIMEOUT,
    )

    # При 401 пробуем обновить токен и повторяем
    if resp.status_code == 401:
        new_token = _handle_401(credentials)
        if new_token:
            s = _session(credentials)
            resp = s.get(
                f"{BASE_URL}/v2/wallet",
                params={"order_by": "BALANCE", "currency_type": "ALL"},
                timeout=HTTP_TIMEOUT,
            )
        else:
            raise ValueError("401 Unauthorized — токен истёк, обнови refresh_token в настройках карты")

    if resp.status_code == 403:
        raise ValueError("403 Forbidden — проверь заголовки устройства в credentials")
    resp.raise_for_status()

    data = resp.json()
    wallets = data.get("wallets") or []
    ars = next((w for w in wallets if w.get("currency") == "ARS"), None)
    if not ars:
        raise ValueError("ARS кошелёк не найден")

    balance = float(ars.get("balance") or 0)
    cvu_number = ""
    account_details = ars.get("account_details") or {}
    if account_details.get("scheme") == "CVU":
        cvu_number = (account_details.get("account_info") or {}).get("value") or ""

    return {
        "balance":    balance,
        "cvu_number": cvu_number,
        "cvu_alias":  "",
    }


def get_activities(credentials: dict, page: int = 1, size: int = 50) -> dict:
    """История операций."""
    s = _session(credentials)
    resp = s.get(
        f"{BASE_URL}/v3/activities",
        params={"page": page, "size": size},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 401:
        new_token = _handle_401(credentials)
        if new_token:
            s = _session(credentials)
            resp = s.get(
                f"{BASE_URL}/v3/activities",
                params={"page": page, "size": size},
                timeout=HTTP_TIMEOUT,
            )
    resp.raise_for_status()
    return resp.json()


def get_receipt(credentials: dict, reference_id: str) -> dict:
    """
    Получить PDF-чек по reference_id из активности.
    Возвращает {"file_resource": "https://...", "file_name": "..."}
    """
    s = _session(credentials)
    resp = s.get(
        f"{BASE_URL}/v1/wallet/schemeTransfer/invoice/{reference_id}",
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 401:
        new_token = _handle_401(credentials)
        if new_token:
            s = _session(credentials)
            resp = s.get(
                f"{BASE_URL}/v1/wallet/schemeTransfer/invoice/{reference_id}",
                timeout=HTTP_TIMEOUT,
            )
    resp.raise_for_status()
    return resp.json()


def create_withdraw(
    credentials: dict,
    destination: str,
    amount: float,
    comments: str = "",
) -> dict:
    """
    Вывод средств на CVU / alias.
    Шаг 1: GET summary — резолвит получателя и возвращает target_user.
    Шаг 2: POST schemeTransfer/cvu — выполняет перевод.
    """
    s = _session(credentials)

    # ── Шаг 1: резолвим получателя через summary ────────────────────────────
    def _do_summary(session):
        return session.get(
            f"{BASE_URL}/v1/wallet/schemeTransfer/cvu/summary",
            params={
                "origin_currency": "ARS",
                "origin_amount":   float(amount),
                "target_currency": "ARS",
                "target_account":  destination,
            },
            timeout=HTTP_TIMEOUT,
        )

    summary_resp = _do_summary(s)
    if summary_resp.status_code == 401:
        new_token = _handle_401(credentials)
        if new_token:
            s = _session(credentials)
            summary_resp = _do_summary(s)

    if summary_resp.status_code in (400, 404, 422):
        try:
            err = summary_resp.json()
            msg = err.get("message") or err.get("error") or str(err)
        except Exception:
            msg = summary_resp.text or str(summary_resp.status_code)
        raise ValueError(f"Получатель не найден: {msg}")
    summary_resp.raise_for_status()

    summary    = summary_resp.json().get("summary") or {}
    target_user = summary.get("target_user")
    if not target_user:
        raise ValueError("Не удалось получить данные получателя (пустой summary)")

    # ── Шаг 2: выполняем перевод ─────────────────────────────────────────────
    payload = {
        "comments":        comments,
        "origin_amount":   float(amount),
        "origin_currency": "ARS",
        "target_amount":   float(summary.get("target_amount") or amount),
        "target_currency": "ARS",
        "target_user":     target_user,
    }

    def _do_transfer(session):
        return session.post(
            f"{BASE_URL}/v1/wallet/schemeTransfer/cvu",
            json=payload,
            timeout=HTTP_TIMEOUT,
        )

    transfer_resp = _do_transfer(s)
    if transfer_resp.status_code == 401:
        new_token = _handle_401(credentials)
        if new_token:
            s = _session(credentials)
            transfer_resp = _do_transfer(s)

    if transfer_resp.status_code in (400, 422):
        try:
            err = transfer_resp.json()
            msg = err.get("message") or err.get("error") or str(err)
        except Exception:
            msg = transfer_resp.text or str(transfer_resp.status_code)
        raise ValueError(f"Ошибка перевода: {msg}")
    transfer_resp.raise_for_status()
    return transfer_resp.json()
