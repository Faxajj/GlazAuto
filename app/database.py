"""
SQLite хранилище: аккаунты, сессии, пользователи, правила автовывода, лимиты выводов.

Credentials (поле accounts.credentials) шифруется at-rest через app.crypto если
переменная окружения BANKS_MASTER_KEY задана. Поведение по умолчанию — plaintext
(обратно совместимо с существующими инсталляциями). Подробнее — app/crypto.py.
"""
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from app.crypto import encrypt_credentials, decrypt_credentials

DB_PATH = os.getenv("DB_PATH", "/var/www/app/accounts.db")

def _load_windows() -> List[Tuple[str, str]]:
    """Список кабинетов можно расширять через env WINDOWS_CONFIG.

    Формат: slug:Название,slug2:Название 2
    Пример: glazars:GLAZ ARS,glaz3:Glaz3,glaz6:Glaz6
    """
    raw = (os.getenv("WINDOWS_CONFIG") or "").strip()
    if not raw:
        return [
            ("glazars", "GLaz ars"),
            ("glaz3", "Glaz3"),
            ("glaz6", "Glaz6"),
        ]

    parsed: List[Tuple[str, str]] = []
    for chunk in raw.split(","):
        piece = chunk.strip()
        if not piece:
            continue
        if ":" in piece:
            slug, title = piece.split(":", 1)
        else:
            slug, title = piece, piece
        slug = slug.strip().lower()
        title = title.strip()
        if slug and title:
            parsed.append((slug, title))

    return parsed or [
        ("glazars", "GLaz ars"),
        ("glaz3", "Glaz3"),
        ("glaz6", "Glaz6"),
    ]


WINDOWS = _load_windows()


def normalize_window_slug(value: str) -> str:
    raw = (value or "").strip().lower().replace(" ", "")
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_-"
    slug = "".join(ch for ch in raw if ch in allowed)
    return slug or "glazars"


def _window_title_from_slug(slug: str) -> str:
    s = normalize_window_slug(slug)
    if s == "glazars":
        return "GLaz ars"
    if s.startswith("glaz") and s[4:].isdigit():
        return "Glaz" + s[4:]
    return s.upper() if len(s) <= 5 else s.capitalize()


def get_window_list() -> List[Tuple[str, str]]:
    """Возвращает список (slug, title) — приоритет: таблица windows, затем WINDOWS env, затем аккаунты."""
    by_slug: dict = {}
    sort_order: dict = {}

    # 1. Из таблицы windows (созданные через интерфейс)
    try:
        with _get_conn() as conn:
            rows = conn.execute("SELECT slug, title, sort_order FROM windows ORDER BY sort_order, id").fetchall()
        for r in rows:
            slug = normalize_window_slug(r["slug"])
            by_slug[slug] = r["title"]
            sort_order[slug] = r["sort_order"]
    except Exception:
        pass

    # 2. Из WINDOWS env / дефолтов (если не было в таблице)
    for slug_raw, name in WINDOWS:
        slug = normalize_window_slug(slug_raw)
        if slug not in by_slug:
            by_slug[slug] = name
            sort_order[slug] = 999

    # 3. Кабинеты из аккаунтов, которые не в списке выше
    try:
        with _get_conn() as conn:
            rows = conn.execute("SELECT DISTINCT COALESCE(window, 'glazars') AS window FROM accounts").fetchall()
        for row in rows:
            slug = normalize_window_slug(row["window"])
            if slug not in by_slug:
                by_slug[slug] = _window_title_from_slug(slug)
                sort_order[slug] = 9999
    except Exception:
        pass

    return sorted(by_slug.items(), key=lambda x: (sort_order.get(x[0], 9999), x[1].lower()))


def add_window(slug: str, title: str) -> bool:
    """Создаёт кабинет. Возвращает True если создан, False если уже существует."""
    slug = normalize_window_slug(slug)
    title = (title or "").strip()
    if not slug or not title:
        return False
    try:
        with _get_conn() as conn:
            max_order = conn.execute("SELECT COALESCE(MAX(sort_order), 0) FROM windows").fetchone()[0]
            conn.execute(
                "INSERT INTO windows (slug, title, sort_order) VALUES (?, ?, ?)",
                (slug, title, int(max_order) + 10),
            )
            conn.commit()
        return True
    except Exception:
        return False


def update_window(slug: str, title: str) -> bool:
    """Переименовывает кабинет."""
    slug = normalize_window_slug(slug)
    title = (title or "").strip()
    if not slug or not title:
        return False
    with _get_conn() as conn:
        cur = conn.execute("UPDATE windows SET title = ? WHERE slug = ?", (title, slug))
        conn.commit()
        return cur.rowcount > 0


def delete_window(slug: str) -> bool:
    """Удаляет кабинет (аккаунты остаются, просто переходят в разряд неизвестных)."""
    slug = normalize_window_slug(slug)
    with _get_conn() as conn:
        cur = conn.execute("DELETE FROM windows WHERE slug = ?", (slug,))
        conn.commit()
        return cur.rowcount > 0


def window_exists(slug: str) -> bool:
    slug = normalize_window_slug(slug)
    with _get_conn() as conn:
        row = conn.execute("SELECT 1 FROM windows WHERE slug = ?", (slug,)).fetchone()
    return row is not None


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
        CREATE TABLE IF NOT EXISTS account_withdraw_limits (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id    INTEGER NOT NULL,
            withdraw_date TEXT NOT NULL,
            count         INTEGER NOT NULL DEFAULT 0,
            count_a       INTEGER NOT NULL DEFAULT 0,
            count_b       INTEGER NOT NULL DEFAULT 0,
            UNIQUE(account_id, withdraw_date)
        );
        CREATE TABLE IF NOT EXISTS withdraw_attempts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key TEXT UNIQUE NOT NULL,
            account_id      INTEGER NOT NULL,
            destination     TEXT NOT NULL,
            amount          REAL NOT NULL,
            group_key       TEXT,
            status          TEXT NOT NULL DEFAULT 'PENDING',
            bank_tx_id      TEXT,
            error_message   TEXT,
            business_date   TEXT NOT NULL,
            created_at      INTEGER NOT NULL,
            completed_at    INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_withdraw_attempts_status
            ON withdraw_attempts(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_withdraw_attempts_account
            ON withdraw_attempts(account_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_accounts_window_created
            ON accounts(window, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_auto_withdraw_active
            ON auto_withdraw_rules(is_active, account_id);
        CREATE INDEX IF NOT EXISTS idx_withdraw_limits_lookup
            ON withdraw_limits(cvu, account_id, withdraw_date);
        CREATE INDEX IF NOT EXISTS idx_account_withdraw_limits_lookup
            ON account_withdraw_limits(account_id, withdraw_date);
        CREATE TABLE IF NOT EXISTS windows (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            slug       TEXT UNIQUE NOT NULL,
            title      TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_exp
            ON sessions(exp);
        CREATE TABLE IF NOT EXISTS rate_history (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        INTEGER NOT NULL,
            buy_avg   REAL,
            sell_avg  REAL
        );
        CREATE INDEX IF NOT EXISTS idx_rate_history_ts ON rate_history(ts DESC);
        CREATE TABLE IF NOT EXISTS accounts_state (
            account_id        INTEGER PRIMARY KEY,
            last_keepalive_at INTEGER NOT NULL DEFAULT 0,
            last_token_state  TEXT,
            fail_count        INTEGER NOT NULL DEFAULT 0,
            last_error        TEXT,
            updated_at        INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          INTEGER NOT NULL,
            user_id     INTEGER,
            username    TEXT,
            ip          TEXT,
            action      TEXT NOT NULL,
            target_type TEXT,
            target_id   INTEGER,
            details     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_action_ts ON audit_log(action, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_target ON audit_log(target_type, target_id, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id, ts DESC);
        CREATE TABLE IF NOT EXISTS proxies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            label           TEXT,
            type            TEXT NOT NULL DEFAULT 'socks5h',
            host            TEXT NOT NULL,
            port            INTEGER NOT NULL,
            username        TEXT,
            password        TEXT,
            region          TEXT,
            status          TEXT NOT NULL DEFAULT 'unknown',
            last_check_ts   INTEGER NOT NULL DEFAULT 0,
            last_latency_ms INTEGER,
            last_error      TEXT,
            fail_count      INTEGER NOT NULL DEFAULT 0,
            enabled         INTEGER NOT NULL DEFAULT 1,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL,
            UNIQUE(host, port, username)
        );
        CREATE INDEX IF NOT EXISTS idx_proxies_status_enabled
            ON proxies(status, enabled);
        CREATE TABLE IF NOT EXISTS balance_cache (
            account_id          INTEGER PRIMARY KEY,
            data_json           TEXT NOT NULL,
            ts                  INTEGER NOT NULL,
            is_error            INTEGER NOT NULL DEFAULT 0,
            last_refresh_error  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_balance_cache_ts ON balance_cache(ts DESC);
        """)
        conn.commit()
        _run_migrations(conn)


def _run_migrations(conn: sqlite3.Connection) -> None:
    migrations = [
        ("accounts",                "window",       "TEXT NOT NULL DEFAULT 'glazars'"),
        ("auto_withdraw_rules",     "min_balance",  "REAL NOT NULL DEFAULT 0"),
        # Group A/B per-card counters — независимые лимиты для PP-internal vs остальных
        ("account_withdraw_limits", "count_a",      "INTEGER NOT NULL DEFAULT 0"),
        ("account_withdraw_limits", "count_b",      "INTEGER NOT NULL DEFAULT 0"),
    ]
    for table, col, definition in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # Backfill: старые строки с count > 0 но count_a=count_b=0 — переносим в group B
    # (на момент миграции считаем что вся история была "внешними" выводами).
    try:
        conn.execute(
            "UPDATE account_withdraw_limits "
            "SET count_b = count "
            "WHERE count > 0 AND count_a = 0 AND count_b = 0"
        )
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
    # decrypt_credentials автоматически распознаёт маркер enc:v1: и расшифровывает,
    # либо парсит старый plaintext JSON (обратная совместимость).
    return {
        "id":          r["id"],
        "bank_type":   r["bank_type"],
        "label":       r["label"],
        "credentials": decrypt_credentials(r["credentials"] or ""),
        "created_at":  r["created_at"],
        "window":      r["window"] if "window" in r.keys() else "glazars",
    }


def list_accounts() -> List[dict]:
    """Список всех аккаунтов. Порядок: window ASC, created_at ASC (старые
    сверху, новые снизу — UI требование «новые добавляются в конец списка»)."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, bank_type, label, credentials, created_at, "
            "COALESCE(window, 'glazars') AS window "
            "FROM accounts ORDER BY window, created_at ASC, id ASC"
        ).fetchall()
    return [_row_to_account(r) for r in rows]


def accounts_by_window() -> dict:
    accounts = list_accounts()
    groups: dict = {slug: [] for slug, _ in get_window_list()}
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
    """Создаёт аккаунт. credentials автоматически шифруется если BANKS_MASTER_KEY задан."""
    window = normalize_window_slug(window)
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO accounts (bank_type, label, credentials, window) VALUES (?, ?, ?, ?)",
            (bank_type, label, encrypt_credentials(credentials), window),
        )
        conn.commit()
        return cur.lastrowid


def update_account(
    account_id: int,
    label: Optional[str] = None,
    credentials: Optional[dict] = None,
    window: Optional[str] = None,
) -> bool:
    """Обновляет поля аккаунта. credentials всегда шифруется при записи (если ключ задан)."""
    with _get_conn() as conn:
        cur = conn.cursor()
        if label is not None:
            cur.execute("UPDATE accounts SET label = ? WHERE id = ?", (label, account_id))
        if credentials is not None:
            cur.execute(
                "UPDATE accounts SET credentials = ? WHERE id = ?",
                (encrypt_credentials(credentials), account_id),
            )
        if window is not None:
            cur.execute("UPDATE accounts SET window = ? WHERE id = ?", (normalize_window_slug(window), account_id))
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
# Лимиты выводов (15 в день на CVU + 15 в день на карту по группам, сброс в 07:30 МСК)
# ---------------------------------------------------------------------------

# Группы лимита (per-card, независимые счётчики):
#   "a" — destination starts with 00000765 (PP internal)
#   "b" — все остальные направления
# AstroPay-внутренние (00001775*) обрабатываются на уровне main._group_key_for —
# они остаются exempt (передаётся group_key=None и счётчик не увеличивается).

GROUP_A = "a"
GROUP_B = "b"
GROUP_KEYS = (GROUP_A, GROUP_B)


def _msk_date_str() -> str:
    """Операционный день по МСК.

    Сброс лимитов и историй — в 07:30 МСК (UTC+3).
    До 07:30 считаем, что действует предыдущий операционный день.
    Часовой пояс закодирован жёстко (UTC+3) — не зависит от системного TZ сервера.
    """
    msk = timezone(timedelta(hours=3))
    now = datetime.now(msk)
    reset_point = now.replace(hour=7, minute=30, second=0, microsecond=0)
    business_day = now.date() if now >= reset_point else (now - timedelta(days=1)).date()
    return business_day.isoformat()


def get_withdraw_count(cvu: str, account_id: int) -> int:
    today = _msk_date_str()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT count FROM withdraw_limits WHERE cvu=? AND account_id=? AND withdraw_date=?",
            (cvu, account_id, today),
        ).fetchone()
    return int(row["count"]) if row else 0


def try_reserve_withdraw_count(cvu: str, account_id: int) -> Optional[int]:
    """Атомарно резервирует слот в per-CVU лимите (15/день).
    Возвращает новое значение count если зарезервировано, None если лимит достигнут.
    Использует ON CONFLICT...DO UPDATE WHERE...RETURNING — одной транзакцией.
    """
    today = _msk_date_str()
    with _get_conn() as conn:
        row = conn.execute(
            "INSERT INTO withdraw_limits (cvu, account_id, withdraw_date, count) "
            "VALUES (?,?,?,1) "
            "ON CONFLICT(cvu, account_id, withdraw_date) "
            "DO UPDATE SET count = count + 1 "
            "WHERE count < ? "
            "RETURNING count",
            (cvu, account_id, today, DAILY_WITHDRAW_LIMIT),
        ).fetchone()
        conn.commit()
    return int(row["count"]) if row else None


def release_withdraw_count(cvu: str, account_id: int) -> None:
    """Возвращает 1 слот в per-CVU лимит (откат после неудачного вывода)."""
    today = _msk_date_str()
    with _get_conn() as conn:
        conn.execute(
            "UPDATE withdraw_limits SET count = count - 1 "
            "WHERE cvu=? AND account_id=? AND withdraw_date=? AND count > 0",
            (cvu, account_id, today),
        )
        conn.commit()


def increment_withdraw_count(cvu: str, account_id: int) -> int:
    """Backward-compat: legacy инкремент без атомарной проверки лимита.
    Используется только в местах где лимит проверяется ДО (auto-withdraw).
    Новый код должен использовать try_reserve_withdraw_count + release_withdraw_count.
    """
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


def get_account_withdraw_count(account_id: int, group_key: Optional[str] = None) -> int:
    """Возвращает счётчик выводов карты за текущий бизнес-день.
    group_key=None  → суммарный (count_a + count_b), для UI display
    group_key='a'   → только PP-internal
    group_key='b'   → только остальные
    """
    today = _msk_date_str()
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(count_a,0) AS ca, COALESCE(count_b,0) AS cb "
                "FROM account_withdraw_limits WHERE account_id=? AND withdraw_date=?",
                (account_id, today),
            ).fetchone()
        if not row:
            return 0
        if group_key == GROUP_A:
            return int(row["ca"])
        if group_key == GROUP_B:
            return int(row["cb"])
        return int(row["ca"]) + int(row["cb"])
    except sqlite3.OperationalError:
        return 0


def try_reserve_account_withdraw_count(account_id: int, group_key: str) -> Optional[int]:
    """Атомарно резервирует слот в per-card лимите для указанной группы.
    Возвращает новое значение счётчика группы если зарезервировано, иначе None.
    """
    if group_key not in GROUP_KEYS:
        raise ValueError(f"invalid group_key: {group_key}")
    today = _msk_date_str()
    col = "count_a" if group_key == GROUP_A else "count_b"
    init_a = 1 if group_key == GROUP_A else 0
    init_b = 1 if group_key == GROUP_B else 0
    try:
        with _get_conn() as conn:
            # Один атомарный INSERT...ON CONFLICT с условием WHERE на нужной колонке.
            row = conn.execute(
                f"INSERT INTO account_withdraw_limits "
                f"(account_id, withdraw_date, count, count_a, count_b) "
                f"VALUES (?, ?, 1, ?, ?) "
                f"ON CONFLICT(account_id, withdraw_date) "
                f"DO UPDATE SET {col} = {col} + 1, count = count + 1 "
                f"WHERE {col} < ? "
                f"RETURNING {col}",
                (account_id, today, init_a, init_b, DAILY_WITHDRAW_LIMIT),
            ).fetchone()
            conn.commit()
        return int(row[0]) if row else None
    except sqlite3.OperationalError:
        # Старая БД без count_a/count_b — fallback на legacy счётчик
        return _legacy_increment_account(account_id)


def get_recent_withdraw_attempts(account_id: int, limit: int = 10) -> list:
    """Последние N попыток вывода для бота — найти tid после withdraw,
    если сайт не вернул его в Location-redirect."""
    try:
        with _get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT amount, bank_tx_id, status, destination, created_at "
                "FROM withdraw_attempts WHERE account_id=? "
                "ORDER BY created_at DESC LIMIT ?",
                (int(account_id), max(1, min(int(limit), 50))),
            ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def release_account_withdraw_count(account_id: int, group_key: str) -> None:
    """Откатывает резерв слота в per-card лимите."""
    if group_key not in GROUP_KEYS:
        return
    today = _msk_date_str()
    col = "count_a" if group_key == GROUP_A else "count_b"
    try:
        with _get_conn() as conn:
            conn.execute(
                f"UPDATE account_withdraw_limits "
                f"SET {col} = {col} - 1, count = MAX(0, count - 1) "
                f"WHERE account_id=? AND withdraw_date=? AND {col} > 0",
                (account_id, today),
            )
            conn.commit()
    except sqlite3.OperationalError:
        pass


def _legacy_increment_account(account_id: int) -> int:
    """Fallback для очень старых БД без count_a/count_b — увеличивает только count."""
    today = _msk_date_str()
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO account_withdraw_limits (account_id, withdraw_date, count) VALUES (?,?,1) "
                "ON CONFLICT(account_id, withdraw_date) DO UPDATE SET count = count + 1",
                (account_id, today),
            )
            conn.commit()
            row = conn.execute(
                "SELECT count FROM account_withdraw_limits WHERE account_id=? AND withdraw_date=?",
                (account_id, today),
            ).fetchone()
        return int(row["count"]) if row else 1
    except sqlite3.OperationalError:
        return 0


def increment_account_withdraw_count(account_id: int, group_key: Optional[str] = None) -> int:
    """Backward-compat обёртка. Если group_key передан — атомарный резерв,
    иначе legacy инкремент общего счётчика count.
    Возвращает новое значение счётчика группы (или legacy total).
    """
    if group_key in GROUP_KEYS:
        result = try_reserve_account_withdraw_count(account_id, group_key)
        if result is not None:
            return result
        # Лимит достигнут — но legacy-вызовы не ожидают None, возвращаем текущий счётчик
        return get_account_withdraw_count(account_id, group_key)
    # Без группы — старое поведение (для миграции/legacy кода)
    return _legacy_increment_account(account_id)


def is_account_withdraw_limit_reached(account_id: int, group_key: Optional[str] = None) -> bool:
    """Проверка лимита.
    group_key=None  → True только если ОБЕ группы достигли лимита
                     (т.е. карта вообще не может ничего вывести)
    group_key='a'   → True если group A достиг 15
    group_key='b'   → True если group B достиг 15
    """
    if group_key in GROUP_KEYS:
        return get_account_withdraw_count(account_id, group_key) >= DAILY_WITHDRAW_LIMIT
    a_reached = get_account_withdraw_count(account_id, GROUP_A) >= DAILY_WITHDRAW_LIMIT
    b_reached = get_account_withdraw_count(account_id, GROUP_B) >= DAILY_WITHDRAW_LIMIT
    return a_reached and b_reached


# ---------------------------------------------------------------------------
# Idempotency для withdraw — защита от двойного списания
# ---------------------------------------------------------------------------

def try_create_withdraw_attempt(
    idempotency_key: str,
    account_id: int,
    destination: str,
    amount: float,
    group_key: Optional[str] = None,
) -> Tuple[bool, Optional[dict]]:
    """Атомарно создаёт запись попытки вывода.
    Returns (created, existing):
      created=True, existing=None   — новая попытка зарегистрирована
      created=False, existing=dict  — попытка с этим ключом уже была, вернуть прошлый результат
    """
    today = _msk_date_str()
    now = int(time.time())
    try:
        with _get_conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO withdraw_attempts "
                    "(idempotency_key, account_id, destination, amount, group_key, "
                    " status, business_date, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)",
                    (idempotency_key, account_id, destination, float(amount),
                     group_key, today, now),
                )
                conn.commit()
                return True, None
            except sqlite3.IntegrityError:
                # UNIQUE violation — попытка с этим ключом уже существует
                row = conn.execute(
                    "SELECT id, idempotency_key, account_id, destination, amount, "
                    "       group_key, status, bank_tx_id, error_message, "
                    "       business_date, created_at, completed_at "
                    "FROM withdraw_attempts WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                return False, dict(row) if row else None
    except sqlite3.OperationalError:
        # Старая БД без таблицы — fallback: всегда новая попытка (без idempotency)
        return True, None


def update_withdraw_attempt_status(
    idempotency_key: str,
    status: str,
    bank_tx_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Обновляет статус попытки вывода.
    status ∈ {'PENDING','EXECUTING','SUCCESS','REJECTED','UNCERTAIN','STUCK'}
    """
    completed_at = int(time.time()) if status in ("SUCCESS", "REJECTED", "STUCK") else None
    try:
        with _get_conn() as conn:
            conn.execute(
                "UPDATE withdraw_attempts SET status=?, "
                "bank_tx_id=COALESCE(?, bank_tx_id), "
                "error_message=COALESCE(?, error_message), "
                "completed_at=COALESCE(?, completed_at) "
                "WHERE idempotency_key=?",
                (status, bank_tx_id, (error_message or "")[:400] if error_message else None,
                 completed_at, idempotency_key),
            )
            conn.commit()
    except sqlite3.OperationalError:
        pass


def get_withdraw_attempt(idempotency_key: str) -> Optional[dict]:
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT id, idempotency_key, account_id, destination, amount, "
                "       group_key, status, bank_tx_id, error_message, "
                "       business_date, created_at, completed_at "
                "FROM withdraw_attempts WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None


# ---------------------------------------------------------------------------
# audit_log — журнал критичных действий (для расследований/комплаенса)
# ---------------------------------------------------------------------------
# Что попадает: login/logout/fail, account create/edit/delete, withdraw success/fail,
# auto-withdraw create/delete/execute, pin_refresh, token_refresh, admin actions.
# НЕ попадает: чтение баланса, истории, диагностика — это шум.
# Retention: 90 дней (cleanup_audit_log) — после этого записи удаляются.

_AUDIT_MAX_DETAILS_BYTES = 4000   # обрезаем длинные details чтобы не раздувать БД


def write_audit_entry(
    action: str,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    ip: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    details: Optional[dict] = None,
) -> None:
    """Записывает событие в audit_log. НИКОГДА не должно бросать исключение —
    audit failure не должна ломать основной flow."""
    try:
        details_json = None
        if details:
            try:
                details_json = json.dumps(details, ensure_ascii=False, default=str)
                if len(details_json) > _AUDIT_MAX_DETAILS_BYTES:
                    details_json = details_json[:_AUDIT_MAX_DETAILS_BYTES] + "...[truncated]"
            except Exception:
                details_json = str(details)[:_AUDIT_MAX_DETAILS_BYTES]
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO audit_log (ts, user_id, username, ip, action, "
                "target_type, target_id, details) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (int(time.time()), user_id, username, ip, action,
                 target_type, target_id, details_json),
            )
            conn.commit()
    except Exception:
        # Логируем через стандартный logger чтобы не получить рекурсию
        import logging
        logging.getLogger(__name__).warning("audit write failed: %s", action)


def list_audit_log(
    limit: int = 100,
    offset: int = 0,
    action_prefix: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    user_id: Optional[int] = None,
    since_ts: Optional[int] = None,
) -> List[dict]:
    """Чтение журнала с фильтрами. Используется будущим админ-эндпоинтом."""
    where = []
    params: list = []
    if action_prefix:
        where.append("action LIKE ?")
        params.append(action_prefix + "%")
    if target_type:
        where.append("target_type = ?")
        params.append(target_type)
    if target_id is not None:
        where.append("target_id = ?")
        params.append(target_id)
    if user_id is not None:
        where.append("user_id = ?")
        params.append(user_id)
    if since_ts is not None:
        where.append("ts >= ?")
        params.append(since_ts)
    sql = "SELECT id, ts, user_id, username, ip, action, target_type, target_id, details FROM audit_log"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC LIMIT ? OFFSET ?"
    params.extend([max(1, min(limit, 1000)), max(0, offset)])
    try:
        with _get_conn() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def cleanup_audit_log(retention_days: int = 90) -> int:
    """Удаляет записи старше retention_days. Возвращает кол-во удалённых строк."""
    cutoff = int(time.time()) - retention_days * 24 * 3600
    try:
        with _get_conn() as conn:
            cur = conn.execute("DELETE FROM audit_log WHERE ts < ?", (cutoff,))
            conn.commit()
        return cur.rowcount
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# accounts_state — переживающее рестарты состояние per-account
# ---------------------------------------------------------------------------
# Хранит:
#   last_keepalive_at  — unix ts последнего успешного PIN refresh
#   last_token_state   — кэшированный JWT state (FRESH/AGING/STALE/EXPIRED/DEAD)
#   fail_count         — сколько подряд keepalive не получил новый JWT
#   last_error         — последняя ошибка (для UI/диагностики)
# Без этой таблицы все эти данные жили в RAM и пропадали при рестарте, что
# приводило к параллельному дудолбежу keepalive-loop'ом ВСЕХ аккаунтов разом.

def get_account_state(account_id: int) -> dict:
    """Возвращает состояние аккаунта (или дефолтное если ещё не сохранялось)."""
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT last_keepalive_at, last_token_state, fail_count, "
                "       last_error, updated_at "
                "FROM accounts_state WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        if row:
            return {
                "account_id":        account_id,
                "last_keepalive_at": int(row["last_keepalive_at"] or 0),
                "last_token_state":  row["last_token_state"] or "",
                "fail_count":        int(row["fail_count"] or 0),
                "last_error":        row["last_error"] or "",
                "updated_at":        int(row["updated_at"] or 0),
            }
    except sqlite3.OperationalError:
        pass
    return {
        "account_id": account_id, "last_keepalive_at": 0, "last_token_state": "",
        "fail_count": 0, "last_error": "", "updated_at": 0,
    }


def update_account_state(
    account_id: int,
    last_keepalive_at: Optional[int] = None,
    last_token_state: Optional[str] = None,
    fail_count_inc: bool = False,
    fail_count_reset: bool = False,
    last_error: Optional[str] = None,
) -> None:
    """Обновляет состояние аккаунта. UPSERT — создаёт строку если нет.
    fail_count_inc / fail_count_reset — атомарные инкремент/сброс счётчика ошибок.
    """
    now = int(time.time())
    try:
        with _get_conn() as conn:
            # Используем UPSERT: одна строка per account_id
            conn.execute(
                "INSERT INTO accounts_state "
                "(account_id, last_keepalive_at, last_token_state, fail_count, "
                " last_error, updated_at) "
                "VALUES (?, ?, ?, 0, ?, ?) "
                "ON CONFLICT(account_id) DO UPDATE SET "
                "  last_keepalive_at = COALESCE(?, last_keepalive_at), "
                "  last_token_state  = COALESCE(?, last_token_state), "
                "  fail_count        = CASE "
                "    WHEN ? THEN fail_count + 1 "
                "    WHEN ? THEN 0 "
                "    ELSE fail_count END, "
                "  last_error        = COALESCE(?, last_error), "
                "  updated_at        = ?",
                (
                    account_id,
                    last_keepalive_at or 0,
                    last_token_state or None,
                    last_error or None,
                    now,
                    last_keepalive_at,
                    last_token_state,
                    1 if fail_count_inc else 0,
                    1 if fail_count_reset else 0,
                    last_error,
                    now,
                ),
            )
            conn.commit()
    except sqlite3.OperationalError:
        # Миграция ещё не применена — игнорируем (in-memory fallback в main.py)
        pass


def list_uncertain_withdraw_attempts(max_age_seconds: int = 600) -> List[dict]:
    """Возвращает попытки в статусе UNCERTAIN, созданные не позже max_age_seconds назад.
    Используется reconciliation worker'ом — слишком новые (<30 сек) ещё могут получить
    ответ от банка штатно, слишком старые (>10 мин) уже неинформативны.
    """
    cutoff_min = int(time.time()) - max_age_seconds
    cutoff_max = int(time.time()) - 30   # не трогаем «свежие» — могут ещё дозавершиться
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT id, idempotency_key, account_id, destination, amount, "
                "       group_key, status, bank_tx_id, error_message, "
                "       business_date, created_at, completed_at "
                "FROM withdraw_attempts "
                "WHERE status = 'UNCERTAIN' "
                "  AND created_at >= ? AND created_at <= ? "
                "ORDER BY created_at ASC",
                (cutoff_min, cutoff_max),
            ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def cleanup_withdraw_attempts(retention_hours: int = 48) -> int:
    """Удаляет завершённые попытки старше retention_hours."""
    cutoff = int(time.time()) - retention_hours * 3600
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "DELETE FROM withdraw_attempts "
                "WHERE created_at < ? AND status IN ('SUCCESS','REJECTED','STUCK')",
                (cutoff,),
            )
            conn.commit()
        return cur.rowcount
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# История курса USDT/ARS
# ---------------------------------------------------------------------------

def _rate_window_start_ts() -> int:
    """Возвращает unix-timestamp начала текущего 24-часового окна курса.

    Окно идёт с 07:30 МСК сегодня до 07:30 МСК завтра.
    До 07:30 МСК считаем, что окно началось вчера в 07:30 МСК.
    """
    msk = timezone(timedelta(hours=3))
    now = datetime.now(msk)
    cutoff = now.replace(hour=7, minute=30, second=0, microsecond=0)
    if now < cutoff:
        cutoff -= timedelta(days=1)
    return int(cutoff.timestamp())


def cleanup_rate_history() -> int:
    """Удаляет точки курса старше 7 дней.
    Ранее удаляло за текущие сутки — теперь храним неделю для графика."""
    cutoff_ts = int(time.time()) - 7 * 24 * 3600
    with _get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM rate_history WHERE ts < ?", (cutoff_ts,)
        )
        conn.commit()
    return cur.rowcount


def save_rate_point(buy_avg: float, sell_avg: float, ts: int) -> None:
    """Сохраняет точку курса. Не дублирует если прошло менее 55 секунд.
    Чистка: удаляет данные старше 7 дней (для недельного графика)."""
    with _get_conn() as conn:
        last = conn.execute(
            "SELECT ts FROM rate_history ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if last and (ts - int(last["ts"])) < 55:
            return
        conn.execute(
            "INSERT INTO rate_history (ts, buy_avg, sell_avg) VALUES (?, ?, ?)",
            (ts, buy_avg, sell_avg),
        )
        # Чистим всё что старше 7 дней
        cutoff_ts = int(time.time()) - 7 * 24 * 3600
        conn.execute(
            "DELETE FROM rate_history WHERE ts < ?",
            (cutoff_ts,),
        )
        conn.commit()


def get_rate_history(hours: int = 24) -> list:
    """Возвращает историю курса за последние N часов (макс 168 = 7 дней)."""
    hours = min(hours, 168)
    since = int(time.time()) - hours * 3600
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT ts, buy_avg, sell_avg FROM rate_history WHERE ts >= ? ORDER BY ts ASC",
            (since,),
        ).fetchall()
    return [{"ts": r["ts"], "b": r["buy_avg"], "s": r["sell_avg"]} for r in rows]


# ---------------------------------------------------------------------------
# Proxy management — каталог, health-check, статус
# ---------------------------------------------------------------------------

def list_proxies(only_enabled: bool = False, only_healthy: bool = False) -> List[dict]:
    sql = "SELECT id, label, type, host, port, username, password, region, status, " \
          "last_check_ts, last_latency_ms, last_error, fail_count, enabled, " \
          "created_at, updated_at FROM proxies"
    where = []
    if only_enabled:
        where.append("enabled = 1")
    if only_healthy:
        where.append("status = 'ok'")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY status DESC, label, id"
    try:
        with _get_conn() as conn:
            rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def get_proxy(proxy_id: int) -> Optional[dict]:
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT id, label, type, host, port, username, password, region, status, "
                "last_check_ts, last_latency_ms, last_error, fail_count, enabled, "
                "created_at, updated_at FROM proxies WHERE id = ?",
                (proxy_id,),
            ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None


def add_proxy(
    type: str, host: str, port: int,
    username: Optional[str] = None, password: Optional[str] = None,
    label: Optional[str] = None, region: Optional[str] = None,
    enabled: bool = True,
) -> Optional[int]:
    now = int(time.time())
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO proxies (label, type, host, port, username, password, "
                "region, status, enabled, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'unknown', ?, ?, ?)",
                (label, type, host.strip(), int(port),
                 username or None, password or None,
                 region, 1 if enabled else 0, now, now),
            )
            conn.commit()
            return cur.lastrowid
    except sqlite3.IntegrityError:
        return None
    except sqlite3.OperationalError:
        return None


def update_proxy(
    proxy_id: int,
    type: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    label: Optional[str] = None,
    region: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> bool:
    fields, params = [], []
    if type is not None:     fields.append("type = ?");     params.append(type)
    if host is not None:     fields.append("host = ?");     params.append(host.strip())
    if port is not None:     fields.append("port = ?");     params.append(int(port))
    if username is not None: fields.append("username = ?"); params.append(username or None)
    if password is not None: fields.append("password = ?"); params.append(password or None)
    if label is not None:    fields.append("label = ?");    params.append(label)
    if region is not None:   fields.append("region = ?");   params.append(region)
    if enabled is not None:  fields.append("enabled = ?");  params.append(1 if enabled else 0)
    if not fields:
        return False
    fields.append("updated_at = ?")
    params.append(int(time.time()))
    params.append(proxy_id)
    try:
        with _get_conn() as conn:
            cur = conn.execute(f"UPDATE proxies SET {', '.join(fields)} WHERE id = ?", tuple(params))
            conn.commit()
        return cur.rowcount > 0
    except sqlite3.OperationalError:
        return False


def delete_proxy(proxy_id: int) -> bool:
    try:
        with _get_conn() as conn:
            cur = conn.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
            conn.commit()
        return cur.rowcount > 0
    except sqlite3.OperationalError:
        return False


# ---------------------------------------------------------------------------
# Balance cache (shared across gunicorn workers)
# ---------------------------------------------------------------------------
# Раньше хранился в _balance_cache: dict в памяти КАЖДОГО воркера.
# В multi-worker setup (gunicorn -w 4) это значит что каждый воркер видит
# только СВОЮ копию — proactive refresh worker'а 1 не виден worker'у 2.
# SQLite-backed cache решает проблему: одна общая таблица для всех воркеров.

def set_balance_cache(
    account_id: int,
    data: dict,
    is_error: bool = False,
    last_refresh_error: Optional[str] = None,
) -> None:
    """UPSERT записи кэша баланса. Все воркеры увидят результат сразу."""
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO balance_cache "
                "(account_id, data_json, ts, is_error, last_refresh_error) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(account_id) DO UPDATE SET "
                "  data_json = excluded.data_json, "
                "  ts = excluded.ts, "
                "  is_error = excluded.is_error, "
                "  last_refresh_error = excluded.last_refresh_error",
                (account_id, json.dumps(data, ensure_ascii=False),
                 int(time.time()), 1 if is_error else 0, last_refresh_error),
            )
            conn.commit()
    except sqlite3.OperationalError:
        pass


def get_balance_cache(account_id: int) -> Optional[dict]:
    """Возвращает {"data", "ts", "is_error", "last_refresh_error"} или None."""
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT data_json, ts, is_error, last_refresh_error "
                "FROM balance_cache WHERE account_id=?",
                (account_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "data": json.loads(row["data_json"]),
            "ts": int(row["ts"]),
            "is_error": bool(row["is_error"]),
            "last_refresh_error": row["last_refresh_error"],
        }
    except (sqlite3.OperationalError, json.JSONDecodeError, TypeError):
        return None


def get_balance_cache_batch(account_ids: List[int]) -> dict:
    """Один SELECT для всех ID-ов. Возвращает {account_id: cache_dict}."""
    if not account_ids:
        return {}
    placeholders = ",".join(["?"] * len(account_ids))
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                f"SELECT account_id, data_json, ts, is_error, last_refresh_error "
                f"FROM balance_cache WHERE account_id IN ({placeholders})",
                tuple(account_ids),
            ).fetchall()
        out = {}
        for r in rows:
            try:
                out[int(r["account_id"])] = {
                    "data": json.loads(r["data_json"]),
                    "ts": int(r["ts"]),
                    "is_error": bool(r["is_error"]),
                    "last_refresh_error": r["last_refresh_error"],
                }
            except (json.JSONDecodeError, TypeError):
                continue
        return out
    except sqlite3.OperationalError:
        return {}


def delete_balance_cache(account_id: int) -> None:
    """Инвалидация — удаляем запись. Следующий read вернёт None → запустится bg refresh."""
    try:
        with _get_conn() as conn:
            conn.execute("DELETE FROM balance_cache WHERE account_id=?", (account_id,))
            conn.commit()
    except sqlite3.OperationalError:
        pass


def mark_proxy_status(
    proxy_id: int,
    status: str,                   # 'ok' / 'fail'
    latency_ms: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """Обновляет результат health-check. На 'fail' инкрементит fail_count, на 'ok' сбрасывает."""
    now = int(time.time())
    try:
        with _get_conn() as conn:
            if status == "ok":
                conn.execute(
                    "UPDATE proxies SET status='ok', last_check_ts=?, "
                    "last_latency_ms=?, last_error=NULL, fail_count=0, updated_at=? "
                    "WHERE id=?",
                    (now, latency_ms, now, proxy_id),
                )
            else:
                conn.execute(
                    "UPDATE proxies SET status=?, last_check_ts=?, "
                    "last_latency_ms=?, last_error=?, fail_count=fail_count+1, "
                    "updated_at=? WHERE id=?",
                    (status, now, latency_ms, (error or "")[:300], now, proxy_id),
                )
            conn.commit()
    except sqlite3.OperationalError:
        pass
