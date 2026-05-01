"""
Шифрование credentials at-rest (Fernet/AES-128-CBC + HMAC-SHA256).

Логика:
  - MASTER_KEY читается из env BANKS_MASTER_KEY (urlsafe-base64, 32 байта).
  - Если ключ не задан → encryption disabled. Чтение/запись plain JSON, как раньше.
    Поведение не меняется → безопасно для существующих инсталляций.
  - Если ключ задан → новые записи шифруются с маркером "enc:v1:" в начале.
  - На чтение: маркер обнаружен → расшифровываем; нет маркера → trate as plain JSON.
    Это обеспечивает плавную миграцию: старые plaintext-строки продолжают работать.

Опциональная зависимость `cryptography`. Если не установлена — encryption тоже off
(graceful fallback) с одним warning в лог при старте.

Команда генерации MASTER_KEY (одноразово):
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Хранить в /etc/systemd/system/banks.service (Environment=BANKS_MASTER_KEY=...) или в
.env, читаемом systemd. НИКОГДА не коммитить в git.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

ENC_MARKER = "enc:v1:"      # префикс зашифрованных значений
ENC_ENABLED = False         # выставляется в _init() при удачной загрузке ключа

_fernet: Optional[Any] = None    # экземпляр Fernet (если ключ загружен)


def _init() -> None:
    """Инициализирует Fernet один раз на процесс. Idempotent."""
    global _fernet, ENC_ENABLED
    if _fernet is not None:
        return

    key = (os.getenv("BANKS_MASTER_KEY") or "").strip()
    if not key:
        logger.info("crypto: BANKS_MASTER_KEY not set — encryption disabled (plaintext mode)")
        ENC_ENABLED = False
        return

    try:
        from cryptography.fernet import Fernet, InvalidToken  # noqa: F401
    except ImportError:
        logger.warning(
            "crypto: BANKS_MASTER_KEY set, but `cryptography` package not installed. "
            "Run: pip install cryptography. Encryption DISABLED."
        )
        ENC_ENABLED = False
        return

    try:
        from cryptography.fernet import Fernet
        _fernet = Fernet(key.encode("ascii"))
        ENC_ENABLED = True
        logger.info("crypto: encryption ENABLED (BANKS_MASTER_KEY loaded successfully)")
    except Exception as e:
        logger.error("crypto: invalid BANKS_MASTER_KEY (%s) — encryption DISABLED", e)
        _fernet = None
        ENC_ENABLED = False


def is_encrypted(value: str) -> bool:
    """True если строка имеет маркер зашифрованной credentials."""
    return isinstance(value, str) and value.startswith(ENC_MARKER)


def encrypt_credentials(creds: dict) -> str:
    """Сериализует dict → JSON → шифрует Fernet'ом → возвращает строку с маркером.
    Если encryption disabled — возвращает plain JSON (без маркера).
    """
    _init()
    raw = json.dumps(creds, ensure_ascii=False, separators=(",", ":"))
    if not ENC_ENABLED or _fernet is None:
        return raw
    token_bytes = _fernet.encrypt(raw.encode("utf-8"))
    return ENC_MARKER + token_bytes.decode("ascii")


def decrypt_credentials(stored: str) -> dict:
    """Парсит значение из БД:
      - с маркером enc:v1: → расшифровывает Fernet'ом → json.loads
      - без маркера       → старый формат → json.loads напрямую
    Возвращает dict или {} при ошибке.
    """
    _init()
    if not stored:
        return {}
    if is_encrypted(stored):
        if _fernet is None:
            # Маркер есть, но ключ не загружен — критическая ошибка конфигурации
            logger.error("crypto: encrypted row found but no MASTER_KEY — "
                         "credentials unreadable. Set BANKS_MASTER_KEY and restart.")
            return {}
        try:
            token = stored[len(ENC_MARKER):].encode("ascii")
            raw = _fernet.decrypt(token).decode("utf-8")
            return json.loads(raw)
        except Exception as e:
            logger.error("crypto: decrypt failed: %s", e)
            return {}
    # Plaintext (старый формат или encryption-off)
    try:
        return json.loads(stored)
    except json.JSONDecodeError as e:
        logger.error("crypto: malformed credentials JSON: %s", e)
        return {}


def status() -> dict:
    """Диагностика для /health и админ-эндпоинтов."""
    _init()
    return {
        "encryption_enabled": ENC_ENABLED,
        "marker": ENC_MARKER,
        "key_source": "BANKS_MASTER_KEY env" if ENC_ENABLED else "none (plaintext)",
    }
