"""
Драйвер AstroPay (mapi.astropaycard.com).

Аутентификация: Bearer токен из credentials["auth_token"].
Токен получается через PIN-логин в приложении и перехватывается через HAR.

credentials = {
    "auth_token": "Bearer klWHhngkG_FtDR3L5DWFZ-..."
}
"""
import time
import requests
from typing import Optional

BASE_URL = "https://mapi.astropaycard.com"
HTTP_TIMEOUT = (8, 20)  # (connect, read)


def _session(credentials: dict) -> requests.Session:
    s = requests.Session()
    token = (credentials.get("auth_token") or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = "Bearer " + token
    s.headers.update({
        "Authorization":  token,
        "Content-Type":   "application/json; charset=utf-8",
        "User-Agent":     "okhttp/4.12.0",
        "x-app-version":  "5.77.0",
        "x-platform":     "android",
        "Accept":         "application/json",
    })
    # Прокси если есть
    proxy = (
        credentials.get("proxy")
        or credentials.get("https_proxy")
        or credentials.get("http_proxy")
        or ""
    ).strip()
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def get_balance(credentials: dict) -> dict:
    """
    Возвращает баланс ARS кошелька и CVU.
    Ответ: {"balance": float, "cvu_number": str, "cvu_alias": str}
    """
    s = _session(credentials)
    resp = s.get(
        f"{BASE_URL}/v2/wallet",
        params={"order_by": "BALANCE", "currency_type": "ALL"},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 401:
        raise ValueError("401 Unauthorized — токен истёк, обнови его в настройках карты")
    if resp.status_code == 403:
        raise ValueError("403 Forbidden — доступ запрещён, обнови токен")
    resp.raise_for_status()

    data = resp.json()
    wallets = data.get("wallets") or []

    # Ищем ARS кошелёк
    ars = next((w for w in wallets if w.get("currency") == "ARS"), None)
    if not ars:
        raise ValueError("ARS кошелёк не найден в ответе API")

    balance = float(ars.get("balance") or 0)

    # CVU из account_details
    cvu_number = ""
    account_details = ars.get("account_details") or {}
    account_info = account_details.get("account_info") or {}
    if account_details.get("scheme") == "CVU":
        cvu_number = account_info.get("value") or ""

    return {
        "balance":    balance,
        "cvu_number": cvu_number,
        "cvu_alias":  "",  # AstroPay не возвращает alias в wallet
    }


def get_activities(credentials: dict, page: int = 1, size: int = 30) -> dict:
    """
    История операций.
    Ответ: {"data": [...], "page": {...}}
    """
    s = _session(credentials)
    resp = s.get(
        f"{BASE_URL}/v3/activities",
        params={"page": page, "size": size, "currency": "ARS"},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 401:
        raise ValueError("401 Unauthorized — обнови токен")
    resp.raise_for_status()
    return resp.json()


def create_withdraw(
    credentials: dict,
    destination: str,
    amount: float,
    comments: str = "Varios",
) -> dict:
    """
    Вывод средств через /v1/transfers/send.

    destination — CVU или alias получателя
    amount      — сумма в ARS
    comments    — описание перевода
    """
    s = _session(credentials)

    # Шаг 1 — резолвим получателя
    resolve_resp = s.post(
        f"{BASE_URL}/v1/transfers/send/resolve",
        json={
            "destination": destination,
            "currency":    "ARS",
            "amount":      str(amount),
        },
        timeout=HTTP_TIMEOUT,
    )
    if resolve_resp.status_code == 401:
        raise ValueError("401 Unauthorized — обнови токен")
    if resolve_resp.status_code == 422:
        err = resolve_resp.json()
        raise ValueError(f"Ошибка резолвинга получателя: {err}")
    resolve_resp.raise_for_status()
    resolve_data = resolve_resp.json()

    transfer_id = (
        resolve_data.get("transfer_id")
        or resolve_data.get("id")
        or resolve_data.get("request_id")
        or ""
    )

    # Шаг 2 — подтверждаем перевод
    confirm_payload = {
        "destination": destination,
        "currency":    "ARS",
        "amount":      str(amount),
        "description": comments,
    }
    if transfer_id:
        confirm_payload["transfer_id"] = transfer_id

    confirm_resp = s.post(
        f"{BASE_URL}/v1/transfers/send/confirm",
        json=confirm_payload,
        timeout=HTTP_TIMEOUT,
    )
    if confirm_resp.status_code == 401:
        raise ValueError("401 Unauthorized — обнови токен")
    if confirm_resp.status_code in (422, 400):
        err = confirm_resp.json()
        msg = (
            err.get("message")
            or err.get("error")
            or err.get("description")
            or str(err)
        )
        raise ValueError(f"Ошибка перевода: {msg}")
    confirm_resp.raise_for_status()

    result = confirm_resp.json()
    return result
