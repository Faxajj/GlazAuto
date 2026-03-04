# Banks Dashboard — несколько аккаунтов

Один дашборд для переключения между аккаунтами **UniversalCoins** и **Personal Pay**. Все данные входа хранятся локально в SQLite.

## Запуск

```powershell
cd C:\Users\Glaz01\banks-dashboard
py -m pip install -r requirements.txt
py -m uvicorn app.main:app --host 127.0.0.1 --port 8015
```

Открой в браузере: **http://127.0.0.1:8015**

## Как пользоваться

1. **Добавить аккаунт** — кнопка «+ Добавить аккаунт». Выбери банк (UniversalCoins или Personal Pay), введи название и **Credentials (JSON)**.
2. **Credentials** — скопируй из своего `.env` или из перехвата (HTTP Toolkit). Формат:
   - **UniversalCoins:** `username`, `password`, `pin`, опционально `user_id`, `fcm_token`, `base_url`
   - **Personal Pay:** `username`, `password`, `device_id`, `push_device_token`; или один `auth_token` (JWT), если вход по ссылке из почты
3. В боковой панели выбираешь аккаунт — справа отображаются баланс и форма вывода для этого банка.
4. Переключаться между аккаунтами можно в любой момент.

Данные хранятся в файле `accounts.db` в папке проекта. Не передавай этот файл и бэкапы третьим лицам.

- **Баланс:** на странице аккаунта есть кнопка «↻ Обновить» и автообновление раз в 30 сек.
- Если Personal Pay отдаёт 403 — см. **docs/PERSONALPAY_403_FIX.md**.


## Авторизация (без регистрации)

На сайте **нет регистрации**.

Логин/пароль выдаёт администратор заранее.

Как администратору создать пользователя:

```bash
python scripts/create_user.py --username operator_1
# пароль будет запрошен скрыто
```

Или сразу с паролем:

```bash
python scripts/create_user.py --username operator_1 --password "StrongPass123"
```

После этого передайте пользователю логин/пароль по безопасному каналу.

