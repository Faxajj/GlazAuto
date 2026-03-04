#!/bin/bash
# Запуск дашборда в фоне, если ещё не запущен.
# Запускать по CRON каждую минуту: * * * * * /var/www/u3436121/data/www/glazauto.pro/START_DASHBOARD_ALWAYS.sh

DIR="/var/www/u3436121/data/www/glazauto.pro"
cd "$DIR" || exit 1

if pgrep -f "uvicorn app.main:app" > /dev/null; then
    exit 0
fi

mkdir -p "$DIR/logs"
nohup ./venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8015 >> "$DIR/logs/dashboard.log" 2>&1 &
echo "Started at $(date)" >> "$DIR/logs/dashboard.log"
