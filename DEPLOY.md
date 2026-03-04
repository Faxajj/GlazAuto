# Как выложить Banks Dashboard на сервер (glazauto.pro / Reg.ru)

Сервер: **server207.hosting.reg.ru** (панель: https://server207.hosting.reg.ru:1500)  
Сайт: **https://glazauto.pro**

---

## 1. Какие файлы загружать

Загружать нужно **всю папку проекта** с сохранением структуры. Список файлов — в **FILES_FOR_DEPLOY.txt**.

**Минимальный набор (обязательно):**

```
banks-dashboard/
├── requirements.txt
├── run_server.sh
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── database.py
│   ├── static/
│   │   └── style.css
│   ├── templates/
│   │   ├── base.html
│   │   ├── index.html
│   │   ├── add_account.html
│   │   ├── edit_account.html
│   │   └── receipt.html
│   └── drivers/
│       ├── __init__.py
│       ├── personalpay.py
│       └── universalcoins.py
```

Файл **accounts.db** на сервер не заливать (или залить свой бэкап, если хочешь перенести аккаунты). При первом запуске он создастся сам, если его нет.

---

## 2. Как залить на сервер

### Создать архив на компе (один раз)

В папке `banks-dashboard` выполни в PowerShell:
```powershell
.\pack_for_deploy.ps1
```
Появится файл **deploy-banks-dashboard.zip** — его и заливай на сервер.

### Вариант A: Через панель ISPmanager (Файловый менеджер)

1. Зайди в панель: **https://server207.hosting.reg.ru:1500**
2. Войди под своим логином/паролем хостинга.
3. Открой **«Файлы»** или **«Файловый менеджер»**.
4. Перейди в каталог, где должен лежать проект (например `~/banks-dashboard` или `~/www/glazauto.pro` — смотри, куда у тебя настроен сайт).
5. Создай папку **banks-dashboard** (если её ещё нет). Зайди в неё.
6. Загрузи файл **deploy-banks-dashboard.zip** (кнопка «Загрузить» / «Upload»).
7. В панели распакуй архив (ПКМ по архиву → «Распаковать» / Extract). После распаковки структура будет: `app/`, `docs/`, `scripts/`, файлы в корне.
8. При необходимости переименуй папку или перемести файлы из распакованной папки в нужное место.

### Вариант B: По SSH (если есть доступ)

1. Подключись по SSH к серверу (логин и хост тебе даёт Reg.ru в разделе «SSH»).
2. Перейди в нужную директорию, например:
   ```bash
   cd ~/www
   # или куда у тебя корень сайта
   ```
3. Создай папку и залей файлы через **SCP** с твоего ПК (в PowerShell или втором терминале):
   ```powershell
   scp -r C:\Users\Glaz01\banks-dashboard\* user@server207.hosting.reg.ru:~/banks-dashboard/
   ```
   (замени `user` на своего SSH-пользователя и путь при необходимости.)

Или упакуй в ZIP на компе, залей через панель в `~/banks-dashboard/`, потом по SSH:
```bash
cd ~/banks-dashboard
unzip archive.zip
```

---

## 3. Настройка на сервере

### 3.1 Python

На сервере должен быть **Python 3.8+**. Проверка по SSH:
```bash
python3 --version
```

Если Python нет — в панели Reg.ru/ISPmanager посмотри раздел «Программное обеспечение» или «Модули»: часто можно включить нужную версию Python.

### 3.2 Зависимости

По SSH зайди в папку проекта и установи зависимости:

```bash
cd ~/banks-dashboard   # или путь, куда залил
python3 -m venv venv
source venv/bin/activate   # Linux/Mac
pip install -r requirements.txt
```

Если виртуального окружения нет — просто:
```bash
pip3 install -r requirements.txt
```

### 3.3 Права на скрипт запуска

```bash
chmod +x run_server.sh
```

### 3.4 Запуск

**Один раз вручную (проверка):**
```bash
./run_server.sh
```
Или с портом (например 80):
```bash
PORT=80 ./run_server.sh
```

В браузере открой: **http://IP_СЕРВЕРА:8015** (или тот порт, что указал). Если открывается дашборд — всё ок.

Остановка: `Ctrl+C`.

**Постоянный запуск (чтобы работало после отключения):**

- В панели Reg.ru/ISPmanager часто есть «Планировщик» или «Менеджер процессов» — там можно добавить задачу/процесс на запуск `run_server.sh` или команды:
  ```bash
  cd /путь/к/banks-dashboard && ./run_server.sh
  ```
- Либо настроить **systemd** (если есть root) или **screen/tmux** и держать процесс в фоне.

---

## 4. Подключение домена glazauto.pro

Чтобы дашборд открывался по **https://glazauto.pro** (или поддомену, например **https://dashboard.glazauto.pro**):

1. В панели хостинга (ISPmanager) открой настройки сайта **glazauto.pro**.
2. Настрой **прокси** или **перенаправление** на `127.0.0.1:8015` (порт, на котором крутится дашборд).  
   Обычно это делается через:
   - Nginx: в конфиг сайта добавить `proxy_pass http://127.0.0.1:8015;`
   - или Apache: `ProxyPass / http://127.0.0.1:8015/`
3. Либо открой порт 8015 в файрволе и заходи по **https://glazauto.pro:8015** (часто нужен SSL на этот порт в панели).

Точные пункты меню зависят от версии ISPmanager — ищи «Настройка веб-сервера», «Прокси», «Домены».

---

## 5. Краткий чеклист

| Шаг | Действие |
|-----|----------|
| 1 | Залить все файлы из `banks-dashboard` на сервер (ZIP или SCP). |
| 2 | Установить зависимости: `pip3 install -r requirements.txt`. |
| 3 | `chmod +x run_server.sh` |
| 4 | Запустить: `./run_server.sh` (или с `PORT=80`). |
| 5 | Проверить в браузере по IP:порт. |
| 6 | Настроить в панели постоянный запуск и привязку домена glazauto.pro. |

---

## 6. Если что-то не работает

- **«ModuleNotFoundError»** — не установлены зависимости: снова выполни `pip install -r requirements.txt` из папки проекта.
- **«Permission denied»** на `run_server.sh` — выполни `chmod +x run_server.sh`.
- **Не открывается снаружи** — проверь, что запускаешь с `--host 0.0.0.0` (в `run_server.sh` уже так) и что порт не закрыт файрволом в панели Reg.ru.
- **Personal Pay 403** — см. **docs/PERSONALPAY_403_FIX.md** (обновить токен в аккаунте).

Удачи.

## 7. Ручной деплой через bash (рекомендовано для последнего фикса SOCKS)

Выполни на сервере в папке проекта:

```bash
cd ~/banks-dashboard
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -c "import socks,requests; print('socks ok; requests', requests.__version__)"
chmod +x run_server.sh
./run_server.sh
```

Если запускаешь через systemd/supervisor/cron — после `pip install -r requirements.txt` обязательно перезапусти процесс дашборда, иначе старый процесс останется без `PySocks`.
