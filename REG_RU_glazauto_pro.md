# glazauto.pro → дашборд на server207 (без нового VPS)

Чтобы по ссылке **https://glazauto.pro** открывался Banks Dashboard на том же хостинге (server207).

---

## Что уже есть

- Сайт **glazauto.pro** в панели Reg.ru (Сайты), корень: **/www/glazauto.pro**
- SSL включён (зелёная галочка)
- Дашборд запускается в Shell: uvicorn на порту **8015**

---

## Вариант 1: Прокси через .htaccess (без порта в адресе)

Если хостинг разрешает прокси в .htaccess, запросы с glazauto.pro пойдут на дашборд и в адресе останется **https://glazauto.pro**.

1. В панели Reg.ru открой **Менеджер файлов** → зайди в **www** → **glazauto.pro** (корень сайта).
2. Загрузи в корень сайта файл **`.htaccess`** из папки проекта (тот что в репозитории).
3. Убедись, что дашборд запущен (Shell: `./run_server.sh` или `./venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8015`).
4. Открой в браузере **https://glazauto.pro**.

Если откроется дашборд — всё ок. Если появится ошибка **500** или «Internal Server Error», хостинг не разрешает прокси в .htaccess → используй **Вариант 2**.

---

## Вариант 2: Редирект с главной страницы (одна ссылка)

Чтобы по **https://glazauto.pro** сразу перекидывало на дашборд (по адресу с портом).

1. В **Менеджере файлов** открой корень сайта **/www/glazauto.pro**.
2. Переименуй или загрузи файл **index.html**: его содержимое должно быть как в **index_redirect.html** из проекта (редирект на `http://server207.hosting.reg.ru:8015/`).
3. Сохрани. При заходе на **https://glazauto.pro** браузер автоматически откроет дашборд.

Пример содержимого **index.html** в корне сайта:

```html
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="0; url=http://server207.hosting.reg.ru:8015/">
  <title>Banks Dashboard</title>
</head>
<body>
  <p>Переход на дашборд… <a href="http://server207.hosting.reg.ru:8015/">Открыть Banks Dashboard</a></p>
</body>
</html>
```

---

## Важно: дашборд должен быть запущен

Пока в Shell на server207 выполнен `./run_server.sh` (или uvicorn на 8015), дашборд доступен. После закрытия сессии процесс может завершиться.

Чтобы он работал постоянно, в панели используй **Планировщик CRON** или **Менеджер процессов** (если есть): добавь задачу/процесс на запуск из папки проекта команды:

```bash
/var/www/u3436121/data/www/glazauto.pro/venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8015
```

(путь замени на свой, если папка проекта в другом месте).
