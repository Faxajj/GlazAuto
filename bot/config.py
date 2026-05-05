"""Все константы Anton-бота. Не редактировать в продакшене без бэкапа."""
import os

# ── Telegram ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("ANTON_BOT_TOKEN") or "8652533942:AAG90MjNupPXjS-w_LirSjRHBf_qqrn-lOY"

# ── Сайт ────────────────────────────────────────────────────────────────────
SITE_URL  = os.getenv("SITE_URL")  or "http://localhost:8000"
SITE_USER = os.getenv("SITE_USER") or "OperatorGlazArs"
SITE_PASS = os.getenv("SITE_PASS") or "Nikolapubger@2"

# ── Telegram чаты ───────────────────────────────────────────────────────────
CHAT_EXCHANGE = -4791978155   # «Обмены» — откуда читаем реквизиты
CHAT_OFFICE   = -5218708173   # «Офис»   — куда пишем чеки и отчёты

# Только эти user_id парсятся как авторы реквизитов в "Обмены"
ALLOWED_SENDERS = {
    1022029395,    # @xxxyi822
    5084017909,    # @beliy_t
    6819350095,    # @luckydush
}

# Операторы для тегов при передаче смены
OP_DAY   = "@glazteam_teamlead"   # принимает смену в 07:30 МСК (день)
OP_NIGHT = "@glazteam_4ever"      # принимает смену в 19:30 МСК (ночь)

# ── Бизнес-параметры ────────────────────────────────────────────────────────
WITHDRAW_CHUNK = 1_000_000          # ARS — макс сумма одного перевода
WITHDRAW_PAUSE = 30                 # сек — пауза между чанками
DAILY_WITHDRAW_LIMIT = 15           # лимит выводов на карту/день
SHIFT_TIMES_MSK = [(7, 30), (19, 30)]   # время смен (МСК)

# Auto-confirmation timeout — если ambiguous=False, выводы стартуют автоматически
AUTO_CONFIRM_AFTER_SEC = 120         # 2 минуты

# Таймаут ожидания ответа оператора с CVU после "лимит карты"
LIMIT_RESPONSE_TIMEOUT_SEC = 30 * 60   # 30 минут

# ── Локальные пути ──────────────────────────────────────────────────────────
STATE_DB = os.getenv("BOT_STATE_DB") or "bot/bot_state.db"
LOG_DIR  = os.getenv("BOT_LOG_DIR")  or "bot/logs"

# ── HTTP таймауты ───────────────────────────────────────────────────────────
HTTP_TIMEOUT_SEC = 60   # запросы к сайту (включая медленные withdraw через прокси)
LOGIN_TIMEOUT_SEC = 15

# ── PP-internal префиксы (без лимита по бизнес-правилам сайта) ─────────────
PP_INTERNAL_PREFIX = "00000765"
AP_INTERNAL_PREFIX = "00001775"
