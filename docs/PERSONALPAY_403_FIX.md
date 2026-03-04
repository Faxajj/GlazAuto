# Personal Pay: исправление 403 Forbidden

Если при запросах к `mobile.prod.personalpay.dev` (financial-accounts, commit-outer и т.д.) приходит **403 Forbidden**, проверь следующее.

## 1. Заголовок Authorization — без "Bearer "

Приложение Personal Pay в перехвате отправляет **только JWT**, без слова `Bearer`:

- **Неправильно:** `Authorization: Bearer eyJ0eXAiOiJKV1QiLC...`
- **Правильно:** `Authorization: eyJ0eXAiOiJKV1QiLC...`

В коде (например `app/drivers/personalpay.py`) при использовании `auth_token` не добавлять префикс `"Bearer "` — отдавать токен как есть в заголовок `Authorization`.

## 2. User-Agent как в приложении

Не заменять `%20` на пробел в User-Agent. Отправлять строку как в перехвате:

- `User-Agent: Personal%20Pay/2.0.1070 CFNetwork/3826.600.41 Darwin/24.6.0`

## 3. device_id и x-fraud-paygilant-session-id

При использовании только `auth_token` в credentials нужно также передавать **device_id** (из тела запроса логина или из заголовка `x-fraud-paygilant-session-id` — часть до подчёркивания). Иначе сервер может отвечать 403.

Формат заголовка: `x-fraud-paygilant-session-id: <device_id>_<timestamp_ms>`.

## 4. Где смотреть в коде

- `app/drivers/personalpay.py`: функция `_get_token()` — не добавлять "Bearer " к auth_token; `_base_headers()` — не делать replace("%20", " ") для User-Agent.
- Credentials для аккаунта: минимум `auth_token` + `device_id`.

## 5. Сервер Ubuntu/VPS и блок по IP

Если на Windows/домашнем интернете работает, а на Ubuntu-сервере стабильно 403, часто это антифрод по IP датацентра.

Добавь прокси в credentials аккаунта:

```json
{
  "auth_token": "eyJ...",
  "device_id": "178DC6C7-C43C-4DCC-B3BC-DBFAAA0F8FD2",
  "https_proxy": "http://user:pass@host:port"
}
```

Поддерживаются поля: `proxy` (для http+https сразу), `http_proxy`, `https_proxy`.
В UI (добавление/редактирование аккаунта) прокси можно задать отдельными полями: тип, host, port, login, password.


## 6. Важно для SOCKS5

Для прокси `socks5://`/`socks5h://` в Python `requests` нужны SOCKS-зависимости (`requests[socks]` / `PySocks`).
Если их нет, будет ошибка `Missing dependencies for SOCKS support`.

