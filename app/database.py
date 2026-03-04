"""
SQLite хранилище аккаунтов банков.
Аккаунты: id, bank_type, label, credentials (JSON), window (GLAZARS | GLAZ3 | GLAZ6).
"""
import json
import sqlite3
from pathlib import Path
from typing import Any, List, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "accounts.db"

# Окна (дерево): slug -> отображаемое имя
WINDOWS = [
    ("glazars", "GLAZARS"),
    ("glaz3", "GLAZ3"),
    ("glaz6", "GLAZ6"),
]


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_type TEXT NOT NULL,
                label TEXT NOT NULL,
                credentials TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        # Миграция: добавить колонку window если её нет
        try:
            conn.execute("ALTER TABLE accounts ADD COLUMN window TEXT DEFAULT 'glazars'")
            conn.execute("UPDATE accounts SET window = 'glazars' WHERE window IS NULL")
            conn.commit()
        except sqlite3.OperationalError:
            pass


def list_accounts() -> List[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, bank_type, label, credentials, created_at, COALESCE(window, 'glazars') AS window FROM accounts ORDER BY window, created_at DESC"
        ).fetchall()
    return [
        {
            "id": r["id"],
            "bank_type": r["bank_type"],
            "label": r["label"],
            "credentials": json.loads(r["credentials"]),
            "created_at": r["created_at"],
            "window": r["window"] if "window" in r.keys() else "glazars",
        }
        for r in rows
    ]


def accounts_by_window() -> dict:
    """Аккаунты сгруппированные по окну: { 'glazars': [...], 'glaz3': [...], 'glaz6': [...] }."""
    accounts = list_accounts()
    groups = {slug: [] for slug, _ in WINDOWS}
    for acc in accounts:
        w = acc.get("window") or "glazars"
        if w not in groups:
            groups[w] = []
        groups[w].append(acc)
    return groups


def get_account(account_id: int) -> Optional[dict]:
    with _get_conn() as conn:
        r = conn.execute(
            "SELECT id, bank_type, label, credentials, created_at, COALESCE(window, 'glazars') AS window FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
    if not r:
        return None
    return {
        "id": r["id"],
        "bank_type": r["bank_type"],
        "label": r["label"],
        "credentials": json.loads(r["credentials"]),
        "created_at": r["created_at"],
        "window": r["window"] if "window" in r.keys() else "glazars",
    }


def add_account(bank_type: str, label: str, credentials: dict, window: str = "glazars") -> int:
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO accounts (bank_type, label, credentials, window) VALUES (?, ?, ?, ?)",
            (bank_type, label, json.dumps(credentials), window),
        )
        conn.commit()
        return cur.lastrowid


def update_account(
    account_id: int,
    label: Optional[str] = None,
    credentials: Optional[dict] = None,
    window: Optional[str] = None,
) -> bool:
    with _get_conn() as conn:
        cur = conn.cursor()
        if label is not None:
            cur.execute("UPDATE accounts SET label = ? WHERE id = ?", (label, account_id))
        if credentials is not None:
            cur.execute("UPDATE accounts SET credentials = ? WHERE id = ?", (json.dumps(credentials), account_id))
        if window is not None:
            cur.execute("UPDATE accounts SET window = ? WHERE id = ?", (window, account_id))
        conn.commit()
        return cur.rowcount > 0


def delete_account(account_id: int) -> bool:
    with _get_conn() as conn:
        cur = conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        conn.commit()
        return cur.rowcount > 0
