"""
Миграция: шифрует все существующие plaintext-credentials в БД.

Использование (на сервере):
    1. Сгенерировать ключ:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    2. Сохранить ключ в systemd unit (Environment=BANKS_MASTER_KEY=...)
       или в .env, читаемый systemd.
    3. ОБЯЗАТЕЛЬНО сделать бэкап БД:
        cp /var/www/app/accounts.db /var/www/app/accounts.db.bak.before_encrypt
    4. Запустить миграцию (с тем же env что у production):
        cd /var/www/app
        BANKS_MASTER_KEY="<твой-ключ>" python -m scripts.migrate_encrypt
    5. Перезапустить сервис: systemctl restart banks
    6. Проверить что аккаунты читаются: открыть UI, нажать «Обновить» на балансе.

Скрипт идемпотентен: уже зашифрованные строки пропускаются. Можно запускать повторно.
"""
import os
import sys

# Чтобы можно было запускать как `python -m scripts.migrate_encrypt` из /var/www/app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.crypto import ENC_MARKER, _init, encrypt_credentials, is_encrypted, status
from app.database import _get_conn, _row_to_account


def main() -> int:
    _init()
    st = status()
    print(f"Encryption status: {st}")
    if not st["encryption_enabled"]:
        print("ERROR: BANKS_MASTER_KEY not set or invalid. Set it before running migration.")
        return 1

    encrypted = 0
    skipped = 0
    errors = 0

    with _get_conn() as conn:
        rows = conn.execute("SELECT id, label, credentials FROM accounts").fetchall()
        print(f"Found {len(rows)} account(s) total.")
        for r in rows:
            acc_id = r["id"]
            label  = r["label"]
            stored = r["credentials"] or ""

            if is_encrypted(stored):
                print(f"  [{acc_id}] {label}: already encrypted — skip")
                skipped += 1
                continue

            # Пробуем расшифровать (вернёт plain JSON-парс)
            try:
                acc = _row_to_account(r)
                creds = acc["credentials"]
                if not creds:
                    print(f"  [{acc_id}] {label}: empty credentials — skip")
                    skipped += 1
                    continue
                # Re-save with encryption
                encrypted_blob = encrypt_credentials(creds)
                conn.execute(
                    "UPDATE accounts SET credentials = ? WHERE id = ?",
                    (encrypted_blob, acc_id),
                )
                conn.commit()
                print(f"  [{acc_id}] {label}: encrypted ✓")
                encrypted += 1
            except Exception as e:
                print(f"  [{acc_id}] {label}: ERROR — {e}")
                errors += 1

    print()
    print(f"Done. encrypted={encrypted}, skipped(already-enc)={skipped}, errors={errors}")
    if errors:
        print("⚠️  Errors encountered — investigate before continuing.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
