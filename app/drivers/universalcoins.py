"""
UniversalCoins API — работа по переданным credentials (dict).
Прод-версия с безопасными таймаутами.
"""
from typing import Any, Optional

import requests


HTTP_TIMEOUT = (5, 12)  # (connect, read) — даём API время ответить


def _norm_creds(creds: dict) -> dict:
    base = (creds.get("base_url") or "https://api.universalcoins.net/api").strip().rstrip("/")
    return {
        "base_url": base,
        "username": (creds.get("username") or "").strip(),
        "password": (creds.get("password") or "").strip().strip('"').strip("'"),
        "user_id": (creds.get("user_id") or "").strip(),
        "pin": (creds.get("pin") or "").strip(),
        "fcm_token": (creds.get("fcm_token") or "").strip(),
    }


def _session_for(c: dict):
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru",
        "Content-Type": "application/json",
        "Origin": "capacitor://localhost",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    })
    return s


def _login(session: requests.Session, c: dict) -> str:
    if not all([c["username"], c["password"], c["pin"]]):
        raise ValueError("Нужны username, password, pin в credentials")
    payload = {
        "requiereHeader": False,
        "username": c["username"],
        "password": c["password"],
        "tokenFCM": c["fcm_token"] or "",
    }
    resp = session.post(f"{c['base_url']}/login/", json=payload, timeout=HTTP_TIMEOUT)
    if resp.status_code != 200:
        try:
            err = resp.json()
            msg = err.get("non_field_errors", [err.get("detail", resp.text)])
            if isinstance(msg, list):
                msg = msg[0] if msg else "Неверный логин или пароль"
        except Exception:
            msg = resp.text or f"HTTP {resp.status_code}"
        raise RuntimeError(f"Логин: {msg}")
    data = resp.json()
    access = data["access"]
    session.headers["Authorization"] = f"Bearer {access}"
    if not c["user_id"] and "user" in data:
        u = data["user"]
        uid = u.get("id") or u.get("uuid") or (u.get("information") or {}).get("id") or (u.get("information") or {}).get("uuid")
        if uid:
            c["user_id"] = str(uid)
    if not c["user_id"]:
        raise ValueError("Не задан user_id. Укажи в credentials или он подставится из ответа логина.")
    return access


def _check_pin(session: requests.Session, c: dict) -> None:
    resp = session.post(
        f"{c['base_url']}/pin-management/check-pin/",
        json={"user_code": c["pin"]},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 401:
        _login(session, c)
        resp = session.post(
            f"{c['base_url']}/pin-management/check-pin/",
            json={"user_code": c["pin"]},
            timeout=HTTP_TIMEOUT,
        )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data.get("message", "PIN error"))


def get_balance(credentials: dict) -> dict:
    """Баланс и CVU для UniversalCoins."""
    c = _norm_creds(credentials)
    session = _session_for(c)
    _login(session, c)
    _check_pin(session, c)
    user_id = c["user_id"]
    resp = session.get(f"{c['base_url']}/cvu/get-cvu/{user_id}/", timeout=HTTP_TIMEOUT)
    if resp.status_code == 401:
        _login(session, c)
        resp = session.get(f"{c['base_url']}/cvu/get-cvu/{user_id}/", timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return {
        "balance": float(data.get("cvu_balance", 0)),
        "cvu_number": data.get("cvu_number", ""),
        "cvu_alias": data.get("cvu_alias", ""),
    }


def create_withdraw(
    credentials: dict,
    cvu_recipient: str,
    amount: float,
    concept: str = "VARIOS",
    alias_recipient: Optional[str] = None,
    document_recipient: Optional[str] = None,
    name_recipient: Optional[str] = None,
    bank_recipient: Optional[str] = None,
) -> dict:
    c = _norm_creds(credentials)
    session = _session_for(c)
    _login(session, c)
    _check_pin(session, c)
    user_id = c["user_id"]
    alias_clean = (alias_recipient or "").strip() or None
    doc_clean = (document_recipient or "").strip().replace("-", "").replace(" ", "") or None
    name_clean = (name_recipient or "").strip() or None
    bank_clean = (bank_recipient or "").strip() or None
    payload = {
        "user_id": user_id,
        "ars": str(int(round(amount, 0))),
        "cvu_recipient": cvu_recipient,
        "alias_recipient": alias_clean if alias_clean is not None else "",
        "document_recipient": doc_clean if doc_clean is not None else "",
        "withdrawal_reason": concept,
        "full_name_recipient": name_clean if name_clean is not None else "",
        "bank_recipient": bank_clean if bank_clean is not None else "",
        "has_code": False,
        "description": "",
        "concept": "",
    }
    url1 = f"{c['base_url']}/cvu/withdraw-cvu/fiat-withdraw/"
    resp1 = session.post(url1, json=payload, timeout=HTTP_TIMEOUT)
    if resp1.status_code >= 400:
        try:
            err_body = resp1.json()
            msg = err_body.get("message") or err_body.get("detail") or err_body.get("data")
            if isinstance(msg, dict):
                msg = str(msg)
            if isinstance(msg, list):
                msg = "; ".join(str(m) for m in msg)
            if not msg:
                msg = resp1.text or f"HTTP {resp1.status_code}"
        except Exception:
            msg = resp1.text or f"HTTP {resp1.status_code}"
        raise RuntimeError(f"Запрос вывода отклонён: {msg}")
    resp1.raise_for_status()
    data1 = resp1.json()
    if data1.get("error"):
        raise RuntimeError(data1.get("message", "Ошибка создания вывода"))
    inner = data1.get("data") or {}
    tx_id = inner.get("pesos_transaction_id")
    if not tx_id:
        raise RuntimeError("API не вернул pesos_transaction_id")
    url2 = f"{c['base_url']}/cvu/withdraw-cvu/{tx_id}/confirm-fiat-withdraw/"
    resp2 = session.post(url2, json={"pin": c["pin"]}, timeout=HTTP_TIMEOUT)
    resp2.raise_for_status()
    data2 = resp2.json()
    if data2.get("error"):
        raise RuntimeError(data2.get("message", "Ошибка подтверждения вывода"))
    return {
        "error": False,
        "message": data2.get("message", "Withdraw was generated correctly"),
        "pesos_transaction_id": tx_id,
        "data": inner,
    }
