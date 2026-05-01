"""
Proxy pool management:
  - URL builder (db row → SOCKS5/HTTP url)
  - Health checker (HTTP HEAD к известным endpoint'ам через прокси)
  - Helpers для drivers (выбрать healthy proxy, mark failure)
"""
from __future__ import annotations

import logging
import time
from typing import Optional, Tuple
from urllib.parse import quote

import requests
from requests import exceptions as req_exc

from app.database import (
    list_proxies as _list_proxies,
    mark_proxy_status as _mark_proxy_status,
)

logger = logging.getLogger(__name__)

HEALTH_CHECK_URL = "https://mapi.astropaycard.com/v1/ping"     # любой HEAD-friendly
HEALTH_CHECK_TIMEOUT = (8, 12)
HEALTH_CHECK_INTERVAL = 90       # секунд между общими прохождениями
HEALTH_CHECK_PER_PROXY_MIN = 60  # мин секунд между проверками одного и того же
DEAD_AFTER_FAILS = 3             # после N подряд fail'ов — auto-disable


def proxy_url(p: dict) -> str:
    """Собирает URL из строки таблицы proxies. Возвращает '' если нет host/port."""
    host = (p.get("host") or "").strip()
    port = p.get("port")
    if not host or not port:
        return ""
    typ  = (p.get("type") or "socks5h").lower().strip()
    user = (p.get("username") or "").strip()
    pwd  = (p.get("password") or "").strip()
    auth = f"{quote(user, safe='')}:{quote(pwd, safe='')}@" if user else ""
    return f"{typ}://{auth}{host}:{int(port)}"


def health_check_one(p: dict, target_url: str = HEALTH_CHECK_URL) -> Tuple[str, Optional[int], Optional[str]]:
    """Синхронная проверка одного прокси.
    Returns: ('ok', latency_ms, None) | ('fail', None, error_message)
    """
    url = proxy_url(p)
    if not url:
        return "fail", None, "invalid proxy fields"
    proxies = {"http": url, "https": url}
    started = time.time()
    try:
        # HEAD дешевле чем GET. Если 4xx/5xx — для нас это всё ещё означает что прокси-канал работает.
        r = requests.head(target_url, proxies=proxies, timeout=HEALTH_CHECK_TIMEOUT,
                          allow_redirects=False)
        latency = int((time.time() - started) * 1000)
        # Любой ответ от целевого сервера через прокси = прокси жив
        if r.status_code < 600:
            return "ok", latency, None
        return "fail", latency, f"unexpected http {r.status_code}"
    except req_exc.ProxyError as e:
        return "fail", None, f"ProxyError: {str(e)[:160]}"
    except req_exc.ConnectTimeout:
        return "fail", None, "ConnectTimeout"
    except req_exc.ReadTimeout:
        return "fail", None, "ReadTimeout"
    except Exception as e:
        return "fail", None, f"{type(e).__name__}: {str(e)[:160]}"


def get_healthy_proxy_url(prefer_region: Optional[str] = None) -> Optional[str]:
    """Возвращает URL живого прокси из пула (для failover в драйверах).
    None если в пуле нет ни одного healthy.
    """
    pool = _list_proxies(only_enabled=True, only_healthy=True)
    if not pool:
        return None
    if prefer_region:
        regional = [p for p in pool if (p.get("region") or "").upper() == prefer_region.upper()]
        if regional:
            pool = regional
    # Sort by latency (fastest first), tie-break by fail_count
    pool.sort(key=lambda p: ((p.get("last_latency_ms") or 99999), p.get("fail_count") or 0))
    return proxy_url(pool[0])


def mark_status(proxy_id: int, ok: bool, latency_ms: Optional[int] = None,
                error: Optional[str] = None) -> None:
    """Wrapper над БД с auto-disable после 3 подряд fail'ов."""
    _mark_proxy_status(proxy_id, "ok" if ok else "fail", latency_ms, error)
