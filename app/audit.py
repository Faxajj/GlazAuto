"""
Audit logging wrapper — принимает Request + контекст, выдёргивает user/ip,
делегирует database.write_audit_entry. Никогда не бросает исключение.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import Request

from app.database import write_audit_entry

logger = logging.getLogger(__name__)


def _client_ip(request: Optional[Request]) -> str:
    if request is None:
        return ""
    for h in ("x-forwarded-for", "x-real-ip", "cf-connecting-ip"):
        v = (request.headers.get(h) or "").strip()
        if v:
            return v.split(",")[0].strip()
    return request.client.host if request.client else ""


def audit(
    action: str,
    request: Optional[Request] = None,
    user: Optional[dict] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    details: Optional[dict] = None,
) -> None:
    """Запись в audit_log.

    Args:
        action: иерархическая строка (e.g. "withdraw.success", "account.deleted")
        request: FastAPI Request — для извлечения IP
        user: текущая сессия (dict с user_id, username) или None
        target_type: тип целевого объекта ("account", "auto_rule", "session", ...)
        target_id: ID объекта
        details: произвольный dict с дополнительной инфой (амаунт, причина, etc.)
    """
    user_id = (user or {}).get("user_id") if isinstance(user, dict) else None
    username = (user or {}).get("username") if isinstance(user, dict) else None
    ip = _client_ip(request)
    try:
        write_audit_entry(
            action=action,
            user_id=user_id,
            username=username,
            ip=ip,
            target_type=target_type,
            target_id=target_id,
            details=details,
        )
    except Exception as e:
        logger.warning("audit() unexpected failure: %s", e)
