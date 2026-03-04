"""
CLI для админа: создать пользователя для входа в дашборд.
Регистрация через веб-интерфейс отсутствует.
"""
import argparse
import getpass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import create_user, get_user_by_username, init_db


def main() -> int:
    parser = argparse.ArgumentParser(description="Создать пользователя для авторизации в дашборде")
    parser.add_argument("--username", required=True, help="Логин пользователя")
    parser.add_argument("--password", help="Пароль (если не указан, будет запрошен скрыто)")
    parser.add_argument("--inactive", action="store_true", help="Создать неактивного пользователя")
    args = parser.parse_args()

    username = args.username.strip().lower()
    if not username:
        print("Ошибка: пустой username")
        return 1

    password = args.password if args.password is not None else getpass.getpass("Пароль: ")
    if not password:
        print("Ошибка: пустой пароль")
        return 1

    init_db()
    if get_user_by_username(username):
        print(f"Ошибка: пользователь '{username}' уже существует")
        return 1

    uid = create_user(username, password, is_active=not args.inactive)
    print(f"Готово. Пользователь создан: id={uid}, username={username}, active={not args.inactive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
