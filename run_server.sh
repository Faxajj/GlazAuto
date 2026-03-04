#!/bin/bash
# Запуск Banks Dashboard на сервере (доступ снаружи).
# Использование: ./run_server.sh   или   PORT=80 ./run_server.sh

cd "$(dirname "$0")"
export PORT="${PORT:-8015}"

echo "Banks Dashboard: http://0.0.0.0:$PORT"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
