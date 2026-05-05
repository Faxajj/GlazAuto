"""Парсинг сообщений с реквизитами из чата «Обмены»."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

# CVU — строго 22 цифры подряд
CVU_PATTERN = re.compile(r'\b(\d{22})\b')

# CUIL/CUIT — НЕ путать с CVU
CUIL_DASH_PATTERN  = re.compile(r'\b\d{2}-\d{7,8}-\d\b')
CUIL_DOTS_PATTERN  = re.compile(r'\b\d{2}\.\d{3}\.\d{3}\b')

# Подсказки на остаток ("осталось", "ostalos", "ОСТАЛОСЬ")
REMAINDER_HINT = re.compile(r'\b(осталось|ostalos|ОСТАЛОСЬ|остаток)\b', re.IGNORECASE)

# Глобальные суммы — "по 600" / "каждый по X" / "все по X"
GLOBAL_AMOUNT_PATTERN = re.compile(
    r'(?:каждый\s+рек\s+по|каждый\s+по|все\s+по|по)\s+([\d.,\s]+(?:\s*[мМmМk]+к?|\s*[Лл]ям[ыов]*)?)',
    re.IGNORECASE,
)

# Суффиксы умножения
SUFFIX_MILLION = re.compile(r'^[мМmM]+$|^[Лл]ям[ыов]*$|^kk$|^кк$', re.IGNORECASE)

# Названия банков и сервисов — игнорируем
BANK_NAMES = re.compile(
    r'^(banco\s+de\s+galicia|personal\s+pay|mercado\s+pago|astropay|'
    r'uala|ualá|naranja\s*x?|resimple|prex|brubank|payway|provincia|'
    r'santander|bbva|galicia|macro|nación|nacion|titular|alias)\.?$',
    re.IGNORECASE,
)

# Заголовки блоков реквизитов
BLOCK_HEADERS = re.compile(
    r'^(buep+\s*#?\d*|total\s+\d+[мМmМ]?|cbu\s*[-–]?\s*(actualizado)?|'
    r'nombre\s+de\s+la\s+entidad|número\s+de\s+cvu|titular\s+de\s+la\s+cuenta)\.?$',
    re.IGNORECASE,
)


@dataclass
class ParsedItem:
    cvu:          str
    name:         str
    amount:       float
    is_remainder: bool


@dataclass
class ParseResult:
    items:     List[ParsedItem]
    ambiguous: bool
    raw:       str


# ─────────────────────────────────────────────────────────────────────────────
# Number normalization
# ─────────────────────────────────────────────────────────────────────────────

def parse_amount(text: str) -> Optional[float]:
    """
    Принимает строку, возвращает число в ARS или None если не парсится.

    "4,000,000" → 4000000          "1.800.000" → 1800000
    "1,8 м"     → 1800000          "4кк"       → 4000000
    "4 м"       → 4000000          "10 лямов"  → 10000000
    "2,5кк"     → 2500000          "1kk"       → 1000000
    "600"       → 600000           "по 600"    → 600000
    "600.000"   → 600000           "304 осталось" → 304000

    Не парсит: "3 / 6", "по +13", CUIL "20-44986385-3"
    """
    if not text:
        return None
    s = text.strip()
    if not s:
        return None

    # Отсекаем явные не-суммы
    if "/" in s and re.search(r"\d\s*/\s*\d", s):    # "3 / 6"
        return None
    if re.search(r"\bпо\s*\+", s, re.IGNORECASE):    # "по +13"
        return None
    if CUIL_DASH_PATTERN.search(s) or CUIL_DOTS_PATTERN.search(s):
        return None

    # Вычленяем число + опциональный суффикс (м/кк/лямов)
    m = re.match(
        r'^\s*([\d][\d\s.,]*?)\s*([мМmMkкKК]+|[Лл]ям[ыовaА]*)?\s*(?:осталось|ostalos|ОСТАЛОСЬ|остаток)?\s*$',
        s,
    )
    if not m:
        # Пытаемся вытащить просто число даже если строка с мусором
        m2 = re.search(r'(\d[\d\s.,]*\d|\d)\s*([мМmMkкKК]+|[Лл]ям[ыовaА]*)?', s)
        if not m2:
            return None
        num_str, suffix = m2.group(1), m2.group(2)
    else:
        num_str, suffix = m.group(1), m.group(2)

    # Чистим разделители: пробелы, точки и запятые. Дробная часть ОБЫЧНО отсутствует
    # в реквизитах (целые миллионы), но возможна с "1,8 м".
    has_decimal = False
    decimal_value = 0.0

    # Удаляем пробелы
    cleaned = num_str.replace(" ", "")

    # Отдельно обработаем "1,8" / "2,5" — десятичная часть с запятой
    if cleaned.count(",") == 1 and cleaned.count(".") == 0:
        whole, frac = cleaned.split(",")
        if frac.isdigit() and len(frac) <= 2:
            has_decimal = True
            try:
                decimal_value = float(f"{whole}.{frac}")
            except ValueError:
                return None
    elif cleaned.count(".") == 1 and cleaned.count(",") == 0:
        whole, frac = cleaned.split(".")
        # ".8" с одной цифрой / ".5" — десятичная часть
        if frac.isdigit() and len(frac) <= 2 and len(whole) <= 3:
            # Например "1.8" → 1.8 (с суффиксом м)
            has_decimal = True
            try:
                decimal_value = float(f"{whole}.{frac}")
            except ValueError:
                return None

    if has_decimal:
        base = decimal_value
    else:
        # Удаляем все разделители — это разделители тысяч
        digits = re.sub(r"[,.\s]", "", cleaned)
        if not digits.isdigit():
            return None
        base = float(digits)

    # Применяем суффикс
    if suffix:
        sl = suffix.lower()
        if sl in ("м", "m") or sl in ("кк", "kk") or "лям" in sl:
            base *= 1_000_000

    # Если число < 1000 без суффикса — это тысячи в сленге (например "по 600" = 600000)
    if not suffix and base < 1000 and not has_decimal:
        base *= 1000

    if base <= 0:
        return None

    return base


# ─────────────────────────────────────────────────────────────────────────────
# Line classification
# ─────────────────────────────────────────────────────────────────────────────

def _is_ignorable_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if BANK_NAMES.match(s):
        return True
    if BLOCK_HEADERS.match(s):
        return True
    if s.startswith("@") and re.match(r"^@\w+$", s):    # тег оператора отдельно
        return True
    if re.match(r"^\d+\.\s*$", s):    # одинокий номер "1."
        return True
    return False


def _line_amount(line: str) -> Optional[float]:
    """Извлекает сумму из строки если строка ВЫГЛЯДИТ как сумма.

    Разрешённые форматы строки целиком:
      "4,000,000" / "1.800.000" / "600"        — чистое число
      "4м" / "1.8 м" / "10 лямов" / "1kk"      — число + миллион-суффикс
      "2233000 осталось" / "5 560 000"         — число + опциональный остаток-маркер
      "ОСТАЛОСЬ - 5 560 000"                   — остаток-маркер + число

    Отвергает:
      "1. Alejandro Martin"  — ordinal + имя
      "CLAVE.ALIAS.228"      — alias-токен
      CUIL форматы
      Чистый CVU
      Имя без чисел
    """
    s = line.strip()
    if not s:
        return None
    if CUIL_DASH_PATTERN.search(s) or CUIL_DOTS_PATTERN.search(s):
        return None
    if CVU_PATTERN.fullmatch(s):
        return None
    if not re.search(r'\d', s):
        return None
    if re.search(r'\bпо\s*\+', s, re.IGNORECASE):
        return None

    # Ordinal-pattern: "1. Alejandro" / "2) Salvador" — не сумма
    if re.match(r'^\s*\d+[.\)]\s+[А-Яа-яA-Za-zёЁ]', s):
        return None

    # Allowed-pattern check: строка должна СОСТОЯТЬ из числа + (опц.) suffix +
    # (опц.) remainder-маркера. Никаких других слов.
    # Убираем remainder-маркеры:
    cleaned = re.sub(r'\b(осталось|ostalos|ОСТАЛОСЬ|остаток|остаток[аыов]*)\b',
                     '', s, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'^[-–—\s]+|[-–—\s]+$', '', cleaned).strip()
    # Должно остаться только число + опциональный множитель м/кк/лямов
    allowed = re.fullmatch(
        r'\s*\d[\d\s.,]*\s*([мМmMkкKК]+|[Лл]ям[ыовaА]*)?\s*',
        cleaned,
    )
    if not allowed:
        return None

    return parse_amount(s)


def _trailing_amount(line: str) -> Optional[float]:
    """Извлекает сумму из конца строки 'Имя 4,400,000' / 'Марсела 4 м' /
    'Jorge Bordenave 4.000.000'. Возвращает None если в хвосте строки нет числа.
    """
    s = (line or "").strip()
    if not s or not re.search(r'\d', s):
        return None
    if CUIL_DASH_PATTERN.search(s) or CUIL_DOTS_PATTERN.search(s):
        return None
    if CVU_PATTERN.search(s):
        return None
    # Берём последовательно последние N токенов и пробуем парсить как сумму
    tokens = s.split()
    if not tokens:
        return None
    for n in (1, 2, 3):
        if n > len(tokens):
            break
        candidate = " ".join(tokens[-n:])
        # Должно содержать цифры
        if not re.search(r'\d', candidate):
            continue
        # Не должно быть чистым ordinal "1." / alias "CLAVE.ALIAS.228"
        if re.match(r'^\d+[.\)]\s*$', candidate):
            continue
        # Allowed-pattern для хвоста
        cleaned = re.sub(r'\b(осталось|ostalos|ОСТАЛОСЬ|остаток)\b', '',
                         candidate, flags=re.IGNORECASE).strip()
        if not re.fullmatch(r'\s*\d[\d\s.,]*\s*([мМmMkкKК]+|[Лл]ям[ыовaА]*)?\s*', cleaned):
            continue
        v = parse_amount(candidate)
        if v is not None and v >= 1000:
            return v
    return None


def _extract_name(line: str, cvu: str) -> str:
    """Из строки '0070... = марсела / 4.000.000' вытащить 'марсела'."""
    s = line.replace(cvu, " ").strip()
    # Убираем числа и разделители "= - / |"
    s = re.sub(r'[=\-–—/|]+', ' ', s)
    # Убираем числа и сум-суффиксы
    s = re.sub(r'\d[\d.,\s]*', ' ', s)
    s = re.sub(r'\s*[мМmMkкKК]+\s*', ' ', s)
    s = re.sub(r'\b(осталось|ostalos|ОСТАЛОСЬ|остаток)\b', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'\s+', ' ', s).strip()
    if BANK_NAMES.match(s) or BLOCK_HEADERS.match(s):
        return ""
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Main parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_message(text: str) -> Optional[ParseResult]:
    """
    Возвращает ParseResult с найденными реквизитами или None если не нашли ни одного CVU.

    ambiguous=True означает «оператор должен проверить» (странный формат /
    не определена сумма / коллизия имён).
    """
    if not text or not text.strip():
        return None

    raw_lines = text.split("\n")
    # Все CVU в порядке появления (с номером строки)
    cvu_positions: List[tuple] = []  # (line_idx, cvu)
    for idx, line in enumerate(raw_lines):
        for m in CVU_PATTERN.finditer(line):
            cvu_positions.append((idx, m.group(1)))

    if not cvu_positions:
        return None

    # Глобальная сумма "По X" / "каждый по X"
    global_amount: Optional[float] = None
    global_is_remainder = False
    for line in raw_lines:
        m = GLOBAL_AMOUNT_PATTERN.search(line)
        if m:
            v = parse_amount(m.group(1))
            if v is not None:
                global_amount = v
                if REMAINDER_HINT.search(line):
                    global_is_remainder = True
                break

    # Top-of-message global: одинокая сумма на одной из первых строк ДО первого CVU
    # (например "4,000,000" сверху, потом пары имя/CVU).
    if global_amount is None and cvu_positions:
        first_cvu_line = cvu_positions[0][0]
        for idx in range(min(first_cvu_line, 5)):
            line = raw_lines[idx].strip()
            if not line or _is_ignorable_line(line):
                continue
            v = _line_amount(line)
            if v is not None and v >= 1000:    # минимальный порог чтобы не путать с номерами
                global_amount = v
                if REMAINDER_HINT.search(line):
                    global_is_remainder = True
                break

    items: List[ParsedItem] = []
    ambiguous = False
    seen_cvus = set()

    for cvu_idx, (line_idx, cvu) in enumerate(cvu_positions):
        if cvu in seen_cvus:
            continue
        seen_cvus.add(cvu)

        line = raw_lines[line_idx]

        # ── Имя: на той же строке что CVU, или предыдущая non-ignorable строка
        name = _extract_name(line, cvu)
        if not name:
            for back in range(line_idx - 1, max(line_idx - 3, -1), -1):
                if back < 0:
                    break
                bl = raw_lines[back].strip()
                if not bl or _is_ignorable_line(bl) or CVU_PATTERN.search(bl):
                    continue
                if _line_amount(bl) is not None:    # это сумма, не имя
                    continue
                # Возможно это имя
                cleaned = re.sub(r'^\d+\.\s*', '', bl)        # "1. Alejandro" → "Alejandro"
                cleaned = re.sub(r'[=\-–—/|]+\s*$', '', cleaned).strip()
                if cleaned and not BANK_NAMES.match(cleaned):
                    name = cleaned
                    break

        # ── Сумма: ищем в окне ±2 строки от CVU
        amount: Optional[float] = None
        is_remainder = False

        # Сначала на той же строке
        same_line_amount = None
        same_line_clean = re.sub(CVU_PATTERN, " ", line)
        if "осталось" in same_line_clean.lower() or "ostalos" in same_line_clean.lower():
            is_remainder = True
        # Пробуем сумму справа от CVU
        rest = line[line.find(cvu) + len(cvu):].strip()
        if rest:
            # Чистим имя из rest чтобы не мешало
            v = parse_amount(rest)
            if v is None:
                # Может быть формат "0070... = марсела / 4.000.000"
                parts = re.split(r'[/=]+', rest)
                for p in parts:
                    v = parse_amount(p)
                    if v is not None:
                        break
            if v is not None:
                same_line_amount = v
        if same_line_amount is not None:
            amount = same_line_amount

        # Иначе — следующая строка с числом
        if amount is None:
            for fwd in range(line_idx + 1, min(line_idx + 4, len(raw_lines))):
                fl = raw_lines[fwd].strip()
                if not fl:
                    continue
                if CVU_PATTERN.search(fl):
                    break    # дошли до следующего CVU без нахождения суммы
                if REMAINDER_HINT.search(fl):
                    is_remainder = True
                if _is_ignorable_line(fl):
                    continue
                v = _line_amount(fl)
                if v is None:
                    v = _trailing_amount(fl)
                if v is not None:
                    amount = v
                    if not name:
                        # Из 'Камила 4,400,000' извлекаем имя — всё до числа
                        prefix = re.split(r'\s*\d', fl, 1)[0].strip()
                        if prefix and not BANK_NAMES.match(prefix):
                            name = prefix
                    break

        # Иначе — предыдущая строка с числом (для формата «4,000,000\nИмя\nCVU»)
        if amount is None:
            for back in range(line_idx - 1, max(line_idx - 4, -1), -1):
                if back < 0:
                    break
                bl = raw_lines[back].strip()
                if not bl:
                    continue
                if CVU_PATTERN.search(bl):
                    break    # дошли до предыдущего CVU
                if REMAINDER_HINT.search(bl):
                    is_remainder = True
                if _is_ignorable_line(bl):
                    continue
                v = _line_amount(bl)
                if v is None:
                    v = _trailing_amount(bl)
                if v is not None:
                    amount = v
                    break

        # Глобальная сумма как fallback
        if amount is None and global_amount is not None:
            amount = global_amount
            if global_is_remainder:
                is_remainder = True

        if amount is None:
            ambiguous = True
            continue    # CVU без суммы — не добавляем, но помечаем для оператора

        items.append(ParsedItem(
            cvu=cvu,
            name=name or "",
            amount=amount,
            is_remainder=is_remainder,
        ))

    # Если вообще ничего не сложилось
    if not items:
        return ParseResult(items=[], ambiguous=True, raw=text)

    # Если CVU больше чем items — какие-то не сматчили сумму
    if len(items) < len(seen_cvus):
        ambiguous = True

    return ParseResult(items=items, ambiguous=ambiguous, raw=text)
