"""SQLite состояние бота: смены, выводы, незакрытые CVU.

Отдельно от основной accounts.db — у бота своя жизнь, свой rollback,
никаких пересечений с банковскими данными.
"""
from __future__ import annotations

import os
import sqlite3
import time
from typing import List, Optional, Tuple

from bot.config import STATE_DB


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(STATE_DB) or ".", exist_ok=True)
    conn = sqlite3.connect(STATE_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_state_db() -> None:
    """Создаёт таблицы при первом запуске. Idempotent."""
    with _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS bot_shifts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_start  INTEGER NOT NULL,
            shift_end    INTEGER,
            total_ars    REAL    DEFAULT 0,
            total_txns   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bot_withdrawals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            shift_id        INTEGER NOT NULL,
            cvu             TEXT    NOT NULL,
            recipient_name  TEXT,
            amount          REAL    NOT NULL,
            account_id      INTEGER,
            account_label   TEXT,
            transaction_id  TEXT,
            status          TEXT    DEFAULT 'pending',
            created_at      INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_withdrawals_shift ON bot_withdrawals(shift_id, created_at);
        CREATE TABLE IF NOT EXISTS bot_pending_cvus (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            cvu         TEXT NOT NULL,
            name        TEXT,
            remaining   REAL NOT NULL,
            created_at  INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_pending_cvu ON bot_pending_cvus(cvu);
        """)
        c.commit()


# ── Смены ───────────────────────────────────────────────────────────────────

def get_or_create_current_shift() -> int:
    """Возвращает id текущей открытой смены. Если нет — открывает новую."""
    now = int(time.time())
    with _conn() as c:
        row = c.execute(
            "SELECT id FROM bot_shifts WHERE shift_end IS NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return int(row["id"])
        cur = c.execute(
            "INSERT INTO bot_shifts (shift_start) VALUES (?)", (now,)
        )
        c.commit()
        return int(cur.lastrowid)


def close_shift(shift_id: int) -> None:
    """Закрывает смену (выставляет shift_end + total_ars/total_txns)."""
    now = int(time.time())
    with _conn() as c:
        agg = c.execute(
            "SELECT COALESCE(SUM(amount),0) AS total_ars, "
            "       COUNT(*)               AS total_txns "
            "FROM bot_withdrawals "
            "WHERE shift_id=? AND status='success'",
            (shift_id,),
        ).fetchone()
        c.execute(
            "UPDATE bot_shifts SET shift_end=?, total_ars=?, total_txns=? WHERE id=?",
            (now, float(agg["total_ars"] or 0), int(agg["total_txns"] or 0), shift_id),
        )
        c.commit()


def get_shift_total(shift_id: int) -> Tuple[float, int]:
    """Возвращает (total_ars, total_txns) по успешным выводам смены."""
    with _conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(amount),0) AS total_ars, "
            "       COUNT(*)               AS total_txns "
            "FROM bot_withdrawals "
            "WHERE shift_id=? AND status='success'",
            (shift_id,),
        ).fetchone()
    return float(row["total_ars"] or 0), int(row["total_txns"] or 0)


# ── Выводы ──────────────────────────────────────────────────────────────────

def record_withdrawal(
    shift_id:       int,
    cvu:            str,
    name:           str,
    amount:         float,
    account_id:     int,
    account_label:  str,
    transaction_id: Optional[str],
    status:         str = "success",
) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO bot_withdrawals "
            "(shift_id, cvu, recipient_name, amount, account_id, "
            " account_label, transaction_id, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (shift_id, cvu, name, float(amount), int(account_id),
             account_label, transaction_id, status),
        )
        c.commit()
        return int(cur.lastrowid)


# ── Pending CVU (остатки для передачи смены) ───────────────────────────────

def add_pending_cvu(cvu: str, name: str, remaining: float) -> None:
    """UPSERT остатка по CVU. Если уже есть — обновляем remaining/name."""
    with _conn() as c:
        c.execute(
            "INSERT INTO bot_pending_cvus (cvu, name, remaining) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(cvu) DO UPDATE SET "
            "  name = excluded.name, remaining = excluded.remaining",
            (cvu.strip(), (name or "").strip(), float(remaining)),
        )
        c.commit()


def remove_pending_cvu(cvu: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM bot_pending_cvus WHERE cvu=?", (cvu.strip(),))
        c.commit()


def get_all_pending_cvus() -> List[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT cvu, name, remaining, created_at FROM bot_pending_cvus "
            "ORDER BY created_at"
        ).fetchall()
    return [dict(r) for r in rows]


def clear_all_pending_cvus() -> None:
    """Используется после успешного отчёта о передаче смены."""
    with _conn() as c:
        c.execute("DELETE FROM bot_pending_cvus")
        c.commit()
