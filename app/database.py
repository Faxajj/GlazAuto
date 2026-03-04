"""
SQLite хранилище аккаунтов банков.
Аккаунты: id, bank_type, label, credentials (JSON), window (GLAZARS | GLAZ3 | GLAZ6).
"""
import hashlib
import json
import os
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
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_accounts_window_created ON accounts(window, created_at DESC)"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auto_withdraw_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                cvu TEXT NOT NULL,
                total_limit REAL NOT NULL,
                chunk_amount REAL NOT NULL,
                paid_amount REAL NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_error TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_auto_withdraw_active ON auto_withdraw_rules(is_active, account_id)"
        )
        conn.commit()


def _hash_password(password: str, salt: Optional[str] = None) -> str:
    raw_salt = salt or os.urandom(16).hex()
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), raw_salt.encode("utf-8"), 150000)
    return f"pbkdf2_sha256${raw_salt}${derived.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, salt, digest = stored_hash.split("$", 2)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    check = _hash_password(password, salt=salt)
    return check == stored_hash


def create_user(username: str, password: str, is_active: bool = True) -> int:
    username = username.strip().lower()
    if not username or not password:
        raise ValueError("username and password are required")
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO users (username, password_hash, is_active) VALUES (?, ?, ?)",
            (username, _hash_password(password), 1 if is_active else 0),
        )
        conn.commit()
        return cur.lastrowid


def get_user_by_username(username: str) -> Optional[dict]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, is_active FROM users WHERE username = ?",
            (username.strip().lower(),),
        ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "username": row["username"],
        "password_hash": row["password_hash"],
        "is_active": bool(row["is_active"]),
    }


def list_auto_withdraw_rules(account_id: Optional[int] = None) -> List[dict]:
    where = ""
    params: tuple = ()
    if account_id is not None:
        where = "WHERE account_id = ?"
        params = (account_id,)
    with _get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT id, account_id, cvu, total_limit, chunk_amount, paid_amount, is_active, COALESCE(last_error, '') AS last_error
            FROM auto_withdraw_rules
            {where}
            ORDER BY created_at DESC
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def get_auto_withdraw_rule(rule_id: int) -> Optional[dict]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, account_id, cvu, total_limit, chunk_amount, paid_amount, is_active, COALESCE(last_error, '') AS last_error FROM auto_withdraw_rules WHERE id = ?",
            (rule_id,),
        ).fetchone()
    return dict(row) if row else None


def add_auto_withdraw_rule(account_id: int, cvu: str, total_limit: float, chunk_amount: float) -> int:
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO auto_withdraw_rules (account_id, cvu, total_limit, chunk_amount) VALUES (?, ?, ?, ?)",
            (account_id, cvu.strip(), float(total_limit), float(chunk_amount)),
        )
        conn.commit()
        return cur.lastrowid


def update_auto_withdraw_progress(rule_id: int, paid_delta: float = 0, last_error: str = "", is_active: Optional[bool] = None) -> None:
    with _get_conn() as conn:
        rule = conn.execute("SELECT paid_amount, total_limit FROM auto_withdraw_rules WHERE id = ?", (rule_id,)).fetchone()
        if not rule:
            return
        paid = float(rule["paid_amount"] or 0) + float(paid_delta or 0)
        total = float(rule["total_limit"] or 0)
        active = int(paid < total)
        if is_active is not None:
            active = 1 if is_active else 0
        conn.execute(
            """
            UPDATE auto_withdraw_rules
            SET paid_amount = ?, is_active = ?, last_error = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (paid, active, (last_error or "")[:400], rule_id),
        )
        conn.commit()


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

import sqlite3
import time

DB_PATH = "accounts.db"


def create_session(token, user_id, username, exp):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (token, user_id, username, exp) VALUES (?, ?, ?, ?)",
        (token, user_id, username, exp),
    )
    conn.commit()
    conn.close()


def get_session(token):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, exp FROM sessions WHERE token=?",
        (token,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "user_id": row[0],
        "username": row[1],
        "exp": row[2],
    }


def delete_session(token):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE token=?", (token,))
    conn.commit()
    conn.close()


def cleanup_sessions():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM sessions WHERE exp <= ?",
        (int(time.time()),),
    )
    conn.commit()
    conn.close()
