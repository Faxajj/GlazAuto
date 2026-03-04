# Настройка glazauto.pro на VPS (194.58.95.139)

Чтобы по адресу **https://glazauto.pro** открывался Banks Dashboard.

---

## Шаг 1. DNS — привязать домен к VPS

В панели, где управляешь доменом **glazauto.pro** (Reg.ru или другой регистратор):

1. Открой настройки DNS (зона домена).
2. Добавь или измени записи:

| Тип | Имя | Значение        | TTL (если есть) |
|-----|-----|-----------------|------------------|
| A   | @   | 194.58.95.139   | 300 или по умолч. |
| A   | www | 194.58.95.139   | 300 или по умолч. |

3. Сохрани. Подожди 5–30 минут (иногда до пары часов).

Проверка: в терминале на своём ПК выполни `ping glazauto.pro` — должен отвечать 194.58.95.139.

---

## Шаг 2. Подключение к VPS по SSH

На своём компьютере (PowerShell или cmd):

```bash
ssh root@194.58.95.139
```

(или `ssh ubuntu@194.58.95.139` — смотри, какой пользователь указан в письме от Reg.ru). Введи пароль от VPS.

---

## Шаг 3. Установка Nginx и подготовка проекта на VPS

Выполняй команды по порядку на VPS (после входа по SSH).

### 3.1 Обновление и установка Nginx

```bash
apt update
apt install -y nginx
systemctl enable nginx
systemctl start nginx
```

### 3.2 Папка для сайта и файлы дашборда

**Как залить файлы с ПК (Windows) на VPS:**

- **PowerShell:** `cd C:\Users\Glaz01\banks-dashboard` затем  
  `scp -r app requirements.txt run_server.sh root@194.58.95.139:/tmp/glazauto-upload/`  
  На VPS: `mkdir -p /var/www/glazauto.pro && mv /tmp/glazauto-upload/* /var/www/glazauto.pro/`
- **WinSCP / FileZilla:** подключись по SFTP к 194.58.95.139 (root + пароль), создай `/var/www/glazauto.pro`, перетащи туда папку `app`, файлы `requirements.txt`, `run_server.sh`.
- **Через ZIP:** упакуй проект в ZIP, залей на VPS (scp или панель), на VPS: `unzip archive.zip -d /var/www/glazauto.pro` и при необходимости перемести файлы из подпапки в корень.

Структура на VPS должна быть такой:

```
/var/www/glazauto.pro/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── database.py
│   ├── static/
│   │   └── style.css
│   ├── templates/
│   │   └── ...
│   └── drivers/
│       ├── __init__.py
│       ├── personalpay.py
│       └── universalcoins.py
├── requirements.txt
└── run_server.sh
```

Если файлы уже лежат в другой папке (например в домашней), перемести или скопируй их:

```bash
mkdir -p /var/www/glazauto.pro
# если файлы в домашней папке, например в ~/glazauto.pro:
# cp -r ~/glazauto.pro/* /var/www/glazauto.pro/
```

### 3.3 Python, venv и зависимости

```bash
cd /var/www/glazauto.pro
apt install -y python3 python3-pip python3-venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3.4 Исправить переводы строк в run_server.sh (если нужно)

```bash
sed -i 's/\r$//' run_server.sh
chmod +x run_server.sh
```

### 3.5 Запуск дашборда как сервиса (чтобы работал всегда)

Создай сервис systemd:

```bash
cat > /etc/systemd/system/glazauto-dashboard.service << 'EOF'
[Unit]
Description=Banks Dashboard glazauto.pro
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/var/www/glazauto.pro
ExecStart=/var/www/glazauto.pro/venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8015
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable glazauto-dashboard
systemctl start glazauto-dashboard
systemctl status glazauto-dashboard
```

Если в выводе `status` видно `active (running)` — приложение слушает порт 8015.

---

## Шаг 4. Конфиг Nginx — прокси на дашборд

Создай конфиг для домена:

```bash
cat > /etc/nginx/sites-available/glazauto.pro << 'EOF'
server {
    listen 80;
    server_name glazauto.pro www.glazauto.pro;
    location / {
        proxy_pass http://127.0.0.1:8015;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

ln -sf /etc/nginx/sites-available/glazauto.pro /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx
```

После этого **http://glazauto.pro** (и http://www.glazauto.pro) должны открывать дашборд.

---

## Шаг 5. HTTPS (SSL) для https://glazauto.pro

```bash
apt install -y certbot python3-certbot-nginx
certbot --nginx -d glazauto.pro -d www.glazauto.pro
```

Следуй подсказкам (email, согласие с условиями). Certbot сам настроит Nginx для HTTPS. После этого дашборд будет открываться по **https://glazauto.pro**.

Проверка продления сертификата:

```bash
certbot renew --dry-run
```

---

## Краткий чеклист

| Шаг | Действие |
|-----|----------|
| 1 | DNS: A-записи для glazauto.pro и www → 194.58.95.139 |
| 2 | SSH на VPS: `ssh root@194.58.95.139` |
| 3 | Установить Nginx, положить файлы в `/var/www/glazauto.pro`, venv, pip install |
| 4 | Сервис systemd для uvicorn на порту 8015 |
| 5 | Конфиг Nginx с proxy_pass на 127.0.0.1:8015 |
| 6 | certbot для HTTPS |

После этого дашборд открывается по **https://glazauto.pro** без порта и без привязки к старому серверу или твоему ПК.
