"""
SQLite хранилище: аккаунты, сессии, пользователи, правила автовывода, лимиты выводов.
"""
import hashlib
import json
import os
import sqlite3
import time
from typing import List, Optional

DB_PATH = "/var/www/app/accounts.db"

WINDOWS = [
    ("glazars", "GLAZARS"),
    ("glaz3",   "GLAZ3"),
    ("glaz6",   "GLAZ6"),
]

DAILY_WITHDRAW_LIMIT = 15


# ---------------------------------------------------------------------------
# Соединение
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# Инициализация и миграции
# ---------------------------------------------------------------------------

def init_db() -> None:
    with _get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_type   TEXT NOT NULL,
            label       TEXT NOT NULL,
            credentials TEXT NOT NULL,
            window      TEXT NOT NULL DEFAULT 'glazars',
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token    TEXT PRIMARY KEY,
            user_id  INTEGER,
            username TEXT,
            exp      INTEGER
        );
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_active     INTEGER NOT NULL DEFAULT 1,
            created_at    TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS auto_withdraw_rules (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id   INTEGER NOT NULL,
            cvu          TEXT NOT NULL,
            total_limit  REAL NOT NULL,
            chunk_amount REAL NOT NULL,
            min_balance  REAL NOT NULL DEFAULT 0,
            paid_amount  REAL NOT NULL DEFAULT 0,
            is_active    INTEGER NOT NULL DEFAULT 1,
            last_error   TEXT,
            created_at   TEXT DEFAULT (datetime('now')),
            updated_at   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS withdraw_limits (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cvu           TEXT NOT NULL,
            account_id    INTEGER NOT NULL,
            withdraw_date TEXT NOT NULL,
            count         INTEGER NOT NULL DEFAULT 0,
            UNIQUE(cvu, account_id, withdraw_date)
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_window_created
            ON accounts(window, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_auto_withdraw_active
            ON auto_withdraw_rules(is_active, account_id);
        CREATE INDEX IF NOT EXISTS idx_withdraw_limits_lookup
            ON withdraw_limits(cvu, account_id, withdraw_date);
        CREATE INDEX IF NOT EXISTS idx_sessions_exp
            ON sessions(exp);
        """)
        conn.commit()
        _run_migrations(conn)


def _run_migrations(conn: sqlite3.Connection) -> None:
    migrations = [
        ("accounts",            "window",      "TEXT NOT NULL DEFAULT 'glazars'"),
        ("auto_withdraw_rules", "min_balance",  "REAL NOT NULL DEFAULT 0"),
    ]
    for table, col, definition in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")
            conn.commit()
        except sqlite3.OperationalError:
            pass


# ---------------------------------------------------------------------------
# Пользователи
# ---------------------------------------------------------------------------

def _hash_password(password: str, salt: Optional[str] = None) -> str:
    raw_salt = salt or os.urandom(16).hex()
    derived = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), raw_salt.encode("utf-8"), 150_000
    )
    return f"pbkdf2_sha256${raw_salt}${derived.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, salt, _ = stored_hash.split("$", 2)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    return _hash_password(password, salt=salt) == stored_hash


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
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Сессии
# ---------------------------------------------------------------------------

def create_session(token: str, user_id: int, username: str, exp: int) -> None:
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sessions (token, user_id, username, exp) VALUES (?, ?, ?, ?)",
            (token, user_id, username, exp),
        )
        conn.commit()


def get_session(token: str) -> Optional[dict]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id, username, exp FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
    return dict(row) if row else None


def delete_session(token: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()


def cleanup_sessions() -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE exp <= ?", (int(time.time()),))
        conn.commit()


# ---------------------------------------------------------------------------
# Аккаунты
# ---------------------------------------------------------------------------

def _row_to_account(r: sqlite3.Row) -> dict:
    return {
        "id":          r["id"],
        "bank_type":   r["bank_type"],
        "label":       r["label"],
        "credentials": json.loads(r["credentials"]),
        "created_at":  r["created_at"],
        "window":      r["window"] if "window" in r.keys() else "glazars",
    }


def list_accounts() -> List[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, bank_type, label, credentials, created_at, "
            "COALESCE(window, 'glazars') AS window "
            "FROM accounts ORDER BY window, created_at DESC"
        ).fetchall()
    return [_row_to_account(r) for r in rows]


def accounts_by_window() -> dict:
    accounts = list_accounts()
    groups: dict = {slug: [] for slug, _ in WINDOWS}
    for acc in accounts:
        w = acc.get("window") or "glazars"
        groups.setdefault(w, []).append(acc)
    return groups


def get_account(account_id: int) -> Optional[dict]:
    with _get_conn() as conn:
        r = conn.execute(
            "SELECT id, bank_type, label, credentials, created_at, "
            "COALESCE(window, 'glazars') AS window FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
    return _row_to_account(r) if r else None


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
            cur.execute(
                "UPDATE accounts SET credentials = ? WHERE id = ?",
                (json.dumps(credentials), account_id),
            )
        if window is not None:
            cur.execute("UPDATE accounts SET window = ? WHERE id = ?", (window, account_id))
        conn.commit()
        return cur.rowcount > 0


def delete_account(account_id: int) -> bool:
    with _get_conn() as conn:
        cur = conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        conn.commit()
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Правила автовывода
# ---------------------------------------------------------------------------

_AUTO_COLS = (
    "id, account_id, cvu, total_limit, chunk_amount, paid_amount, is_active, "
    "COALESCE(last_error, '') AS last_error, COALESCE(min_balance, 0) AS min_balance"
)


def list_auto_withdraw_rules(account_id: Optional[int] = None) -> List[dict]:
    where = "WHERE account_id = ?" if account_id is not None else ""
    params: tuple = (account_id,) if account_id is not None else ()
    with _get_conn() as conn:
        rows = conn.execute(
            f"SELECT {_AUTO_COLS} FROM auto_withdraw_rules {where} ORDER BY created_at DESC",
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def get_auto_withdraw_rule(rule_id: int) -> Optional[dict]:
    with _get_conn() as conn:
        row = conn.execute(
            f"SELECT {_AUTO_COLS} FROM auto_withdraw_rules WHERE id = ?",
            (rule_id,),
        ).fetchone()
    return dict(row) if row else None


def add_auto_withdraw_rule(
    account_id: int,
    cvu: str,
    total_limit: float,
    chunk_amount: float,
    min_balance: float = 0,
) -> int:
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO auto_withdraw_rules "
            "(account_id, cvu, total_limit, chunk_amount, min_balance) VALUES (?, ?, ?, ?, ?)",
            (account_id, cvu.strip(), float(total_limit), float(chunk_amount), float(min_balance)),
        )
        conn.commit()
        return cur.lastrowid


def delete_auto_withdraw_rule(rule_id: int, account_id: int) -> bool:
    with _get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM auto_withdraw_rules WHERE id = ? AND account_id = ?",
            (rule_id, account_id),
        )
        conn.commit()
        return cur.rowcount > 0


def update_auto_withdraw_progress(
    rule_id: int,
    paid_delta: float = 0,
    last_error: str = "",
    is_active: Optional[bool] = None,
) -> None:
    with _get_conn() as conn:
        rule = conn.execute(
            "SELECT paid_amount, total_limit FROM auto_withdraw_rules WHERE id = ?",
            (rule_id,),
        ).fetchone()
        if not rule:
            return
        paid = float(rule["paid_amount"] or 0) + float(paid_delta or 0)
        total = float(rule["total_limit"] or 0)
        active = int(paid < total)
        if is_active is not None:
            active = 1 if is_active else 0
        conn.execute(
            "UPDATE auto_withdraw_rules "
            "SET paid_amount = ?, is_active = ?, last_error = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (paid, active, (last_error or "")[:400], rule_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Лимиты выводов (15 в день на CVU, сброс в 09:30 МСК)
# ---------------------------------------------------------------------------

def _msk_date_str() -> str:
    """Текущая дата по МСК (UTC+3)."""
    msk_ts = time.time() + 3 * 3600
    t = time.gmtime(msk_ts)
    return f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d}"


def get_withdraw_count(cvu: str, account_id: int) -> int:
    today = _msk_date_str()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT count FROM withdraw_limits WHERE cvu=? AND account_id=? AND withdraw_date=?",
            (cvu, account_id, today),
        ).fetchone()
    return int(row["count"]) if row else 0


def increment_withdraw_count(cvu: str, account_id: int) -> int:
    today = _msk_date_str()
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO withdraw_limits (cvu, account_id, withdraw_date, count) VALUES (?,?,?,1) "
            "ON CONFLICT(cvu, account_id, withdraw_date) DO UPDATE SET count = count + 1",
            (cvu, account_id, today),
        )
        conn.commit()
        row = conn.execute(
            "SELECT count FROM withdraw_limits WHERE cvu=? AND account_id=? AND withdraw_date=?",
            (cvu, account_id, today),
        ).fetchone()
    return int(row["count"]) if row else 1


def is_withdraw_limit_reached(cvu: str, account_id: int) -> bool:
    return get_withdraw_count(cvu, account_id) >= DAILY_WITHDRAW_LIMIT
