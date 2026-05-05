"""Скриншоты чеков через Playwright headless Chromium."""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlparse

from bot.config import SITE_URL

logger = logging.getLogger(__name__)


async def capture_receipt(
    account_id:     int,
    transaction_id: str,
    session_token:  str,
    csrf_token:     str,
) -> Optional[bytes]:
    """Открывает /account/{id}/receipt?transaction_id=... в headless-браузере
    с залогиненными cookies, делает скриншот.

    Returns: PNG bytes или None при ошибке.
    """
    if not transaction_id:
        return None
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error(
            "playwright НЕ установлен. На сервере выполни:\n"
            "  /var/www/app/venv/bin/pip install playwright\n"
            "  /var/www/app/venv/bin/playwright install --with-deps chromium"
        )
        return None

    parsed = urlparse(SITE_URL)
    domain = parsed.hostname or "localhost"
    url = f"{SITE_URL}/account/{account_id}/receipt?transaction_id={transaction_id}"

    try:
        async with async_playwright() as pw:
            try:
                browser = await pw.chromium.launch(headless=True)
            except Exception as e:
                logger.error(
                    "playwright chromium не запускается (%s). На сервере выполни:\n"
                    "  /var/www/app/venv/bin/playwright install --with-deps chromium",
                    e,
                )
                return None
            context = await browser.new_context(
                viewport={"width": 560, "height": 900},   # фиксированная ширина для чёткости чека
                device_scale_factor=2,                    # retina-качество
            )
            await context.add_cookies([
                {"name": "session_token", "value": session_token,
                 "domain": domain, "path": "/", "httpOnly": True, "secure": False},
                {"name": "csrf_token", "value": csrf_token,
                 "domain": domain, "path": "/", "httpOnly": False, "secure": False},
            ])
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
            except Exception as e:
                logger.warning("receipt page goto error: %s", e)
                # Пробуем «load» вместо networkidle
                try:
                    await page.goto(url, wait_until="load", timeout=30000)
                except Exception:
                    await context.close(); await browser.close()
                    return None

            # Ждём элемент чека — pp-receipt из shared CSS
            try:
                receipt_el = await page.wait_for_selector(".pp-receipt", timeout=10000)
            except Exception:
                # Fallback на другие возможные элементы
                try:
                    receipt_el = await page.wait_for_selector(".receipt-card, table", timeout=5000)
                except Exception:
                    receipt_el = None

            if receipt_el:
                # Скриншот именно карточки чека — без шапки сайта
                try:
                    png = await receipt_el.screenshot(type="png", omit_background=False)
                except Exception:
                    png = await page.screenshot(type="png", full_page=False)
            else:
                png = await page.screenshot(type="png", full_page=False)

            await context.close()
            await browser.close()
            return png
    except Exception as e:
        logger.exception("capture_receipt failed: %s", e)
        return None
