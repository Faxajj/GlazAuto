"""HTTP-клиент к сайту GlazAuto. Учитывает CSRF middleware и session-cookie."""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional
from urllib.parse import urlencode

import aiohttp

from bot.config import (
    HTTP_TIMEOUT_SEC,
    LOGIN_TIMEOUT_SEC,
    SITE_PASS,
    SITE_URL,
    SITE_USER,
)

logger = logging.getLogger(__name__)


class SiteClient:
    """Async aiohttp-клиент с CSRF и автоматическим re-login при 401/redirect."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        # Для Playwright — отдаём наружу
        self.csrf_token: str = ""
        self.session_token: str = ""

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                cookie_jar=aiohttp.CookieJar(unsafe=True),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── Cookies helpers ─────────────────────────────────────────────────────

    def _cookie(self, name: str) -> str:
        if not self._session:
            return ""
        for c in self._session.cookie_jar:
            if c.key == name:
                return c.value or ""
        return ""

    def _refresh_local_cookies(self) -> None:
        self.csrf_token    = self._cookie("csrf_token")
        self.session_token = self._cookie("session_token")

    # ── Auth ────────────────────────────────────────────────────────────────

    async def ensure_logged_in(self) -> None:
        """Логинится если ещё нет валидной сессии. Под локом — один вход за раз."""
        async with self._lock:
            session = await self._ensure_session()
            self._refresh_local_cookies()

            if self.session_token:
                # Проверяем что сессия живая — лёгкий GET на защищённый endpoint
                try:
                    async with session.get(
                        f"{SITE_URL}/dashboard",
                        timeout=aiohttp.ClientTimeout(total=LOGIN_TIMEOUT_SEC),
                        allow_redirects=False,
                    ) as r:
                        # 200 — ок; 302 на /login — сессия умерла
                        if r.status == 200:
                            self._refresh_local_cookies()
                            return
                except Exception:
                    pass

            # Логин с нуля
            await self._do_login()

    async def _do_login(self) -> None:
        """Логин с retry: при 400 (CSRF flake) пересоздаём сессию + повторяем."""
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                await self._do_login_once()
                return
            except RuntimeError as e:
                last_err = e
                msg = str(e)
                logger.warning("site_client: login attempt %d failed: %s",
                               attempt + 1, msg[:200])
                # Полный сброс session + cookies — для нового CSRF
                if self._session and not self._session.closed:
                    try:
                        await self._session.close()
                    except Exception:
                        pass
                self._session = None
                self.csrf_token = ""
                self.session_token = ""
                if attempt < 2:
                    await asyncio.sleep(2)
        # все попытки провалились
        raise last_err or RuntimeError("site login: all retries failed")

    async def _do_login_once(self) -> None:
        session = await self._ensure_session()
        # Шаг 1: GET /login → получаем csrf_token cookie
        async with session.get(
            f"{SITE_URL}/login",
            timeout=aiohttp.ClientTimeout(total=LOGIN_TIMEOUT_SEC),
        ) as r:
            await r.read()
        self._refresh_local_cookies()
        if not self.csrf_token:
            raise RuntimeError("Site login: csrf_token cookie not received from GET /login")

        # Шаг 2: POST /login (form-data) с csrf_token полем
        form = aiohttp.FormData()
        form.add_field("username", SITE_USER)
        form.add_field("password", SITE_PASS)
        form.add_field("csrf_token", self.csrf_token)
        async with session.post(
            f"{SITE_URL}/login",
            data=form,
            timeout=aiohttp.ClientTimeout(total=LOGIN_TIMEOUT_SEC),
            allow_redirects=False,
        ) as r:
            body = (await r.text())[:300]
            if r.status not in (302, 303):
                raise RuntimeError(f"Site login failed: {r.status} {body}")
        self._refresh_local_cookies()
        if not self.session_token:
            raise RuntimeError("Site login: session_token cookie not received after POST /login")
        logger.info("site_client: login успешен (session_token=%s...)", self.session_token[:10])

    # ── HTTP wrappers ───────────────────────────────────────────────────────

    async def _get(self, path: str, **kwargs) -> aiohttp.ClientResponse:
        """GET с авто-re-login если получили redirect на /login."""
        await self.ensure_logged_in()
        session = await self._ensure_session()
        url = path if path.startswith("http") else f"{SITE_URL}{path}"
        for attempt in range(2):
            r = await session.get(url, allow_redirects=False, **kwargs)
            if r.status in (301, 302, 303) and "/login" in r.headers.get("Location", ""):
                if attempt == 0:
                    await r.read()
                    self.session_token = ""
                    await self.ensure_logged_in()
                    continue
            return r
        return r  # type: ignore[return-value]

    async def _post_form(self, path: str, data: dict) -> aiohttp.ClientResponse:
        """POST form-urlencoded с CSRF-токеном в форме И в заголовке."""
        await self.ensure_logged_in()
        session = await self._ensure_session()
        url = f"{SITE_URL}{path}"
        # CSRF: токен передаём И в форме И в header — middleware принимает любой источник
        payload = dict(data)
        payload["csrf_token"] = self.csrf_token
        headers = {
            "X-CSRF-Token": self.csrf_token,
            "Accept": "text/html, */*",
        }
        for attempt in range(2):
            r = await session.post(url, data=payload, headers=headers,
                                   allow_redirects=False)
            if r.status in (301, 302, 303) and "/login" in r.headers.get("Location", ""):
                if attempt == 0:
                    await r.read()
                    self.session_token = ""
                    await self.ensure_logged_in()
                    payload["csrf_token"] = self.csrf_token
                    headers["X-CSRF-Token"] = self.csrf_token
                    continue
            return r
        return r  # type: ignore[return-value]

    async def _post_json(self, path: str, data: dict) -> aiohttp.ClientResponse:
        await self.ensure_logged_in()
        session = await self._ensure_session()
        url = f"{SITE_URL}{path}"
        headers = {
            "X-CSRF-Token": self.csrf_token,
            "Content-Type": "application/json",
        }
        for attempt in range(2):
            r = await session.post(url, json=data, headers=headers,
                                   allow_redirects=False)
            if r.status in (301, 302, 303) and "/login" in r.headers.get("Location", ""):
                if attempt == 0:
                    await r.read()
                    self.session_token = ""
                    await self.ensure_logged_in()
                    headers["X-CSRF-Token"] = self.csrf_token
                    continue
            return r
        return r  # type: ignore[return-value]

    # ── High-level API ──────────────────────────────────────────────────────

    async def get_all_accounts(self) -> List[dict]:
        """Список всех аккаунтов: id, label, bank_type, window."""
        r = await self._get("/api/accounts")
        try:
            data = await r.json(content_type=None)
        except Exception:
            return []
        return list(data.get("accounts") or [])

    async def get_pp_accounts(self) -> List[dict]:
        """PP-карты с балансом > 0, отсортированы по балансу desc.
        Если первый запрос вернул пустой список (кеш балансов на сайте ещё
        не прогрет после рестарта) — ждём 15 сек и повторяем."""
        result = await self._get_pp_accounts_once()
        if not result:
            logger.info(
                "get_pp_accounts: пусто на первой попытке, "
                "ждём прогрева кеша балансов 15 сек..."
            )
            await asyncio.sleep(15)
            result = await self._get_pp_accounts_once()
        return result

    async def _get_pp_accounts_once(self) -> List[dict]:
        """PP-карты с балансом > 0, отсортированы по балансу desc.
        Возвращает: [{id, label, balance, account_withdraw_count, bank_type}, ...]
        Не блокирует на банк-вызовах — читает только из shared cache.
        """
        accounts = await self.get_all_accounts()
        pp_ids = [a["id"] for a in accounts if a.get("bank_type") == "personalpay"]
        if not pp_ids:
            return []
        # Map id → label для финального ответа
        labels = {a["id"]: a.get("label") or "" for a in accounts}
        ids_csv = ",".join(str(i) for i in pp_ids)
        r = await self._get(f"/api/balances?{urlencode({'ids': ids_csv})}")
        try:
            data = await r.json(content_type=None)
        except Exception:
            return []
        balances = data.get("balances") or {}
        result: List[dict] = []
        skipped_null = 0
        skipped_error = 0
        skipped_zero = 0
        accepted_stale = 0
        for acc_id in pp_ids:
            entry = balances.get(str(acc_id))
            if not entry:
                skipped_null += 1
                continue
            # Принимаем stale-данные (cached_age > N сек) — лучше чем ничего.
            # Отказываемся только при явной ошибке банка (token expired, proxy dead).
            if entry.get("_is_error"):
                skipped_error += 1
                continue
            try:
                bal = float(entry.get("balance") or 0)
            except (TypeError, ValueError):
                bal = 0.0
            if bal <= 0:
                skipped_zero += 1
                continue
            if entry.get("_is_stale"):
                accepted_stale += 1
            result.append({
                "id":                     int(acc_id),
                "label":                  labels.get(acc_id, ""),
                "balance":                bal,
                "account_withdraw_count": int(entry.get("account_withdraw_count") or 0),
                "account_withdraw_limit": int(entry.get("account_withdraw_limit") or 15),
                "bank_type":              "personalpay",
                "cvu_number":             entry.get("cvu_number") or "",
                "_is_stale":              bool(entry.get("_is_stale")),
                "_cache_age_sec":         int(entry.get("_cache_age_sec") or 0),
            })
        result.sort(key=lambda a: a["balance"], reverse=True)
        if not result:
            logger.warning(
                "get_pp_accounts: пусто (всего PP=%d, null=%d, error=%d, zero=%d)",
                len(pp_ids), skipped_null, skipped_error, skipped_zero,
            )
        elif accepted_stale:
            logger.info(
                "get_pp_accounts: %d карт (из них %d со stale-балансом)",
                len(result), accepted_stale,
            )
        return result

    async def withdraw(self, account_id: int, destination: str, amount: float) -> dict:
        """POST /account/{id}/withdraw — возвращает разобранный результат.
        НЕ следует за redirect, читает Location.
        """
        r = await self._post_form(
            f"/account/{account_id}/withdraw",
            {
                "destination": destination,
                "amount":      str(amount),
                "comments":    "Varios (VAR)",
                "concept":     "VARIOS",
            },
        )
        location = r.headers.get("Location", "") or ""
        await r.read()

        if not location and r.status not in (302, 303):
            return {"ok": False, "error": f"unexpected http {r.status}", "tid": None}

        if "receipt?transaction_id=" in location:
            try:
                tid = location.split("transaction_id=")[1].split("&")[0]
            except Exception:
                tid = None
            return {"ok": True, "tid": tid, "error": None}
        if "success=1" in location or "success=updated" in location:
            return {"ok": True, "tid": None, "error": None}
        if "error=" in location:
            try:
                err = location.split("error=")[1].split("&")[0]
            except Exception:
                err = "unknown"
            return {"ok": False, "tid": None, "error": err}
        # Неожиданный redirect
        return {"ok": False, "tid": None, "error": f"unexpected redirect: {location[:120]}"}

    def get_receipt_url(self, account_id: int, transaction_id: str) -> str:
        return f"{SITE_URL}/account/{account_id}/receipt?transaction_id={transaction_id}"

    async def render_receipt_image(self, account_id: int,
                                   transaction_id: str) -> Optional[bytes]:
        """Просит сайт отрендерить чек в PNG через серверный playwright.
        Это надёжнее bot-side playwright (cookies/CSRF не нужны)."""
        if not transaction_id:
            return None
        try:
            r = await self._get(
                f"/api/receipt-image?account_id={account_id}"
                f"&transaction_id={transaction_id}"
            )
            if r.status != 200:
                logger.warning("render_receipt_image: http %d", r.status)
                return None
            ct = (r.headers.get("Content-Type") or "").lower()
            if "image/png" not in ct:
                # сервер вернул JSON с ошибкой
                try:
                    err = (await r.json(content_type=None)).get("error", "unknown")
                except Exception:
                    err = "unknown"
                logger.warning("render_receipt_image: server error: %s", err)
                return None
            return await r.read()
        except Exception as e:
            logger.warning("render_receipt_image: %s", e)
            return None

    async def find_tid_from_activities(self, account_id: int, amount: float,
                                        max_age_sec: int = 90) -> Optional[str]:
        """Ищет receipt_id в последних исходящих транзакциях карты.
        Используется когда withdraw вернул ok=True но tid=None.

        Сайт: GET /account/{id}/activities?type=outgoing&limit=5
        Ответ: {"activities": [{"id", "receipt_id", "amount", "is_outgoing", ...}]}

        receipt_id — это transactionId от PP, именно он нужен для чека.
        """
        await asyncio.sleep(4)   # PP пишет транзакцию асинхронно — ждём
        try:
            r = await self._get(
                f"/account/{account_id}/activities?type=outgoing&limit=5"
            )
            data = await r.json(content_type=None)
        except Exception as e:
            logger.warning("find_tid_from_activities: fetch failed: %s", e)
            return None

        activities = data.get("activities") or []
        amt_round = round(float(amount), 2)
        for act in activities:
            if not isinstance(act, dict):
                continue
            if not act.get("is_outgoing"):
                continue
            try:
                act_amt = abs(round(float(act.get("amount") or 0), 2))
            except (TypeError, ValueError):
                continue
            # Допуск 1 ARS — PP может округлить копейки
            if abs(act_amt - amt_round) > 1.0:
                continue
            # receipt_id — это правильный PP transactionId для чека (приоритет)
            tid = act.get("receipt_id") or act.get("id") or ""
            if tid:
                tid = str(tid).strip()
                if tid:
                    return tid
        return None

    async def find_recent_tid(self, account_id: int, amount: float,
                              destination: str = "",
                              max_age_sec: int = 120) -> Optional[str]:
        """Ищет недавний bank_tx_id (tid) для уже выполненного withdraw.
        Используется когда withdraw вернул ok=True но без tid в Location.
        Матчит по amount + destination + свежесть (<= max_age_sec)."""
        try:
            r = await self._get(f"/api/account/{account_id}/recent-attempts?limit=10")
            data = await r.json(content_type=None)
        except Exception as e:
            logger.warning("find_recent_tid: fetch failed: %s", e)
            return None
        attempts = data.get("attempts") or []
        import time as _t
        now = int(_t.time())
        amt_round = round(float(amount), 2)
        for a in attempts:
            try:
                if abs(round(float(a.get("amount") or 0), 2) - amt_round) > 0.01:
                    continue
            except (TypeError, ValueError):
                continue
            if destination and (a.get("destination") or "").strip() != destination.strip():
                continue
            ts = int(a.get("created_at") or 0)
            if ts and now - ts > max_age_sec:
                continue
            tid = (a.get("bank_tx_id") or "").strip()
            if tid:
                return tid
        return None

    async def get_transaction_status(self, account_id: int,
                                     transaction_id: str) -> str:
        """Возвращает реальный статус: 'approved' / 'rejected' / 'pending' / 'unknown'.
        Парсит receipt-страницу: ищет маркеры "rechazada/rejected" vs "approved/aprobada".
        Это критично — `tid` от withdraw означает только "транзакция создана",
        окончательный статус приходит асинхронно от банка.
        """
        if not transaction_id:
            return "unknown"
        try:
            r = await self._get(
                f"/account/{account_id}/receipt?transaction_id={transaction_id}"
            )
            html = (await r.text()) or ""
        except Exception as e:
            logger.warning("get_transaction_status: fetch error: %s", e)
            return "unknown"
        low = html.lower()
        # Сначала проверяем rejected (более специфично)
        if ("rechazad" in low or "rejected" in low or
                "denegad" in low or "failed" in low):
            return "rejected"
        if ("approved" in low or "aprobad" in low or
                "completad" in low or "exitos" in low or "success" in low):
            return "approved"
        if ("pending" in low or "pendiente" in low or
                "procesando" in low or "in_progress" in low):
            return "pending"
        return "unknown"

    async def get_bybit_rate(self) -> Optional[float]:
        """sell_avg в ARS/USDT (курс продавцов на P2P)."""
        try:
            r = await self._get("/api/bybit-rate")
            data = await r.json(content_type=None)
            v = data.get("sell_avg")
            return float(v) if v is not None else None
        except Exception as e:
            logger.warning("get_bybit_rate failed: %s", e)
            return None
