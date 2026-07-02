"""
ScraperELA · config.py
======================
Модуль конфигурации проекта. Читает настройки из переменных окружения (.env).
Предоставляет строгую типизацию для всех конфигурационных констант.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH, override=False)


# --- Вспомогательные парсеры типов ---


def _secret(key: str, default: str = "") -> str:
    """Возвращает очищенную от пробелов строку из окружения."""
    return os.environ.get(key, default).strip()


def _bool(key: str, default: bool) -> bool:
    """Приводит строковое значение окружения к логическому типу."""
    val = os.environ.get(key)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "on")


def _int(key: str, default: int) -> int:
    """Безопасно приводит строковое значение к типу int."""
    val = os.environ.get(key)
    if not val:
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default


def _float(key: str, default: float) -> float:
    """Безопасно приводит строковое значение к типу float."""
    val = os.environ.get(key)
    if not val:
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default


def _list(key: str, default: list[str]) -> list[str]:
    """Разбирает comma-separated строку в список строк."""
    val = os.environ.get(key)
    if not val:
        return default
    return [s.strip() for s in val.split(",") if s.strip()]


def _parse_sort_by(key: str, default: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Разбирает строку сортировки вида 'name:ASC,scrape_date:DESC'
    в список кортежей (поле, направление).
    """
    val = os.environ.get(key)
    if not val:
        return default
    pairs: list[tuple[str, str]] = []
    for item in val.split(","):
        if not item.strip():
            continue
        if ":" in item:
            field, direction = item.split(":", 1)
            pairs.append((field.strip(), direction.strip().upper()))
        else:
            pairs.append((item.strip(), "ASC"))
    return pairs


# =====================================================================
# 1. РЕЖИМЫ ЗАПУСКА
# =====================================================================

RUN_CATALOG_SCAN: bool = _bool("RUN_CATALOG_SCAN", False)
RUN_WEBSITE_CONTACT_SCAN: bool = _bool("RUN_WEBSITE_CONTACT_SCAN", False)
RUN_DETAIL_PARSER: bool = _bool("RUN_DETAIL_PARSER", False)
RUN_ENRICHMENT: bool = _bool("RUN_ENRICHMENT", True)
RUN_EXPORTER: bool = _bool("RUN_EXPORTER", True)

ACTIVE_SITE: str = _secret("ACTIVE_SITE", "chop_moscow")
ACTIVE_SITES: list[str] = _list("ACTIVE_SITE", ["chop_moscow"])

MAX_CONCURRENT_SITES: int = _int("MAX_CONCURRENT_SITES", 4)


# =====================================================================
# 2. БАЗА ДАННЫХ
# =====================================================================

COMMON_DB_NAME: str = _secret("COMMON_DB_NAME", "scraperela.db")
EXPORT_NAME: str = _secret("EXPORT_NAME", "scraperela_export")


# =====================================================================
# 3. РЕЕСТР ПОДДЕРЖИВАЕМЫХ САЙТОВ (Статический реестр)
# =====================================================================

SITES: dict[str, dict[str, Any]] = {
    "chop_moscow": {
        "name": "Реестр ЧОП Москвы (chop.moscow)",
        "base_url": "https://chop.moscow/catalog/",
        "parser_key": "chop_moscow",
        "max_pages": 0,
        "catalog_concurrency": 3,
        "detail_concurrency": 6,
    },
    "prochop_ru": {
        "name": "Каталог ЧОПов России (prochop.ru)",
        "base_url": "https://prochop.ru/chops/",
        "parser_key": "prochop_ru",
        "max_pages": 0,
        "catalog_concurrency": 3,
        "detail_concurrency": 6,
    },
    "vsechopy_mo": {
        "name": "ВсеЧопы.рф — Московская область (всечопы.рф)",
        "base_url": "https://xn--b1ag1aeh1b4a.xn--p1ai/region/moskovskaya-oblast/",
        "parser_key": "vsechopy_ru",
        "max_pages": 0,
        "catalog_concurrency": 3,
        "detail_concurrency": 6,
        "page_pattern": "{base_url}?page={page}",
    },
    "vsechopy_urfo": {
        "name": "ВсеЧопы.рф — Свердловская область (всечопы.рф)",
        "base_url": "https://xn--b1ag1aeh1b4a.xn--p1ai/region/sverdlovskaya-oblast/",
        "parser_key": "vsechopy_ru",
        "max_pages": 0,
        "catalog_concurrency": 3,
        "detail_concurrency": 6,
        "page_pattern": "{base_url}?page={page}",
    },
    "russkii_souz": {
        "name": "Русский союз — Охранные предприятия (русскийсоюз.рф)",
        "base_url": "https://xn--g1abdcwihado2k.xn--p1ai/russia/services/okhrannye_predpriiatiia__detektivy/",
        "parser_key": "russkii_souz",
        "max_pages": 0,
        "catalog_concurrency": 2,
        "detail_concurrency": 4,
        "page_pattern": "{base_url}?page={page}",
    },
}


# =====================================================================
# 4. КЭШИРОВАНИЕ HTML
# =====================================================================

USE_HTML_CACHE: bool = _bool("USE_HTML_CACHE", True)
CACHE_TTL_DAYS: int = _int("CACHE_TTL_DAYS", 14)
KEEP_CACHE_HISTORY: bool = _bool("KEEP_CACHE_HISTORY", True)


# =====================================================================
# 5. АДАПТИВНЫЙ THROTTLING И СЕТЬ
# =====================================================================

ADAPTIVE_MODE: bool = _bool("ADAPTIVE_MODE", True)
ADAPTIVE_MAX_DELAY: float = _float("ADAPTIVE_MAX_DELAY", 10.0)

REQUEST_DELAY: float = _float("REQUEST_DELAY", 0.5)
NETWORK_RETRIES: int = _int("NETWORK_RETRIES", 5)
BACKOFF_FACTOR: float = _float("BACKOFF_FACTOR", 1.0)


# =====================================================================
# 6. ЭКСПОРТ (ФОРМАТЫ)
# =====================================================================

EXPORT_TO_XLSX: bool = _bool("EXPORT_TO_XLSX", True)
EXPORT_TO_CSV: bool = _bool("EXPORT_TO_CSV", False)


# =====================================================================
# 7. ОБОГАЩЕНИЕ ДАННЫХ (ENRICHMENT)
# =====================================================================

ENRICHMENT_BATCH_SIZE: int = _int("ENRICHMENT_BATCH_SIZE", 500)
ENRICHMENT_RECHECK_DAYS: int = _int("ENRICHMENT_RECHECK_DAYS", 30)
ENRICHMENT_USE_CACHE: bool = _bool("ENRICHMENT_USE_CACHE", True)
ENRICHMENT_CACHE_TTL_DAYS: int = _int("ENRICHMENT_CACHE_TTL_DAYS", 30)

DADATA_API_KEY: str = _secret("DADATA_API_KEY")
DADATA_SECRET_KEY: str = _secret("DADATA_SECRET_KEY")
DADATA_DAILY_LIMIT: int = _int("DADATA_DAILY_LIMIT", 10_000)

FNS_REQUEST_DELAY: float = _float("FNS_REQUEST_DELAY", 2.0)


# =====================================================================
# 8. ИЗОЛИРОВАННЫЕ ПРАВИЛА ФИЛЬТРАЦИИ И СОРТИРОВКИ
# =====================================================================

EXPORT_CONFIG: dict[str, Any] = {
    "filter_city": _secret("EXPORT_FILTER_CITY") or None,
    "filter_site_key": _secret("EXPORT_FILTER_SITE_KEY") or None,
    "filter_status_official": _secret("EXPORT_FILTER_STATUS_OFFICIAL") or None,
    "filter_has_inn": _bool("EXPORT_FILTER_HAS_INN", False),
    "filter_has_phone": _bool("EXPORT_FILTER_HAS_PHONE", False),
    "sort_by": _parse_sort_by("EXPORT_SORT_BY", [("name", "ASC")]),
    "limit": _int("EXPORT_LIMIT", 0),
}

ENRICHMENT_CONFIG: dict[str, Any] = {
    "filter_city": _secret("ENRICHMENT_FILTER_CITY") or None,
    "filter_site_key": _secret("ENRICHMENT_FILTER_SITE_KEY") or None,
}

# Конфигурация фазы сканирования сайтов компаний.
# filter_only_without_email: True  — сканировать только компании без email
#                                    (экономит время при повторных запусках).
#                            False — сканировать все компании с website
#                                    (например, после расширения логики парсинга).
# filter_status_official:    Ограничить сканирование по статусу из ЕГРЮЛ.
#                            Пример: ACTIVE — только действующие.
#                            Пусто  — без ограничений.
WEBSITE_SCAN_CONFIG: dict[str, Any] = {
    "filter_city": _secret("WEBSITE_SCAN_FILTER_CITY") or None,
    "filter_site_key": _secret("WEBSITE_SCAN_FILTER_SITE_KEY") or None,
    "filter_status_official": _secret("WEBSITE_SCAN_FILTER_STATUS_OFFICIAL") or None,
    "filter_only_without_email": _bool("WEBSITE_SCAN_FILTER_ONLY_WITHOUT_EMAIL", True),
}


# =====================================================================
# 9. ДИАГНОСТИКА
# =====================================================================

LOG_LEVEL: str = _secret("LOG_LEVEL", "INFO")


def check_secrets() -> None:
    """Печатает статус загрузки секретов для верификации."""
    secrets = {
        "DADATA_API_KEY": DADATA_API_KEY,
        "DADATA_SECRET_KEY": DADATA_SECRET_KEY,
    }
    print(f"\n{'=' * 45}")
    print("  ScraperELA · статус секретов")
    print(f"  .env: {_ENV_PATH} ({'найден' if _ENV_PATH.exists() else 'НЕ НАЙДЕН'})")
    print(f"{'=' * 45}")
    for name, value in secrets.items():
        print(f"  {name:<22} {'✓ задан' if value else '✗ не задан'}")
    print(f"{'=' * 45}\n")


# =====================================================================
# 10. СКАНИРОВАНИЕ САЙТОВ КОМПАНИЙ
# =====================================================================

WEBSITE_SCAN_CONCURRENCY: int = _int("WEBSITE_SCAN_CONCURRENCY", 5)
WEBSITE_SCAN_DELAY: float = _float("WEBSITE_SCAN_DELAY", 1.0)
WEBSITE_SCAN_TIMEOUT: float = _float("WEBSITE_SCAN_TIMEOUT", 15.0)
WEBSITE_SCAN_MAX_PAGES: int = _int("WEBSITE_SCAN_MAX_PAGES", 4)
WEBSITE_SCAN_BATCH_SIZE: int = _int("WEBSITE_SCAN_BATCH_SIZE", 0)
