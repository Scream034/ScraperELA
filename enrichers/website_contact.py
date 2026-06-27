"""
ScraperELA · enrichers/website_contact.py
==========================================
Сканер контактов с сайтов компаний (email + телефоны).

Не является BaseEnricher (другой интерфейс: работает по website, а не по ИНН).
Вызывается как отдельная фаза конвейера между дедупликацией и обогащением.

Алгоритм для каждой компании:
  1. Проверка файлового кэша (SHA-256 URL → .html, TTL по mtime).
  2. GET главная страница (HTTPS → HTTP fallback).
  3. Поиск ссылок на страницу контактов (эвристика по тексту/href).
  4. GET 1-3 дополнительных страниц (контакты, о компании, реквизиты).
  5. Извлечение email (4 уровня) и телефонов из всех загруженных страниц.
  6. Валидация, фильтрация, дедупликация.
  7. Сохранение в company_contacts через DatabaseManager.

Уровни извлечения email:
  L1: ``<a href="mailto:...">``       — самое надёжное.
  L2: Regex по тексту страницы        — ловит email в plain text.
  L3: CloudFlare email protection     — декодирование ``data-cfemail``.
  L4: JSON-LD / Schema.org            — ``application/ld+json``.

Кэш:
  Каждая загруженная страница сохраняется в ``cache_dir/{sha256(url)}.html``.
  При следующем запуске кэш используется если файл моложе ``cache_ttl_days``.
  Весь файловый I/O неблокирующий через ``asyncio.to_thread``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

import config
from database import DatabaseManager

logger = logging.getLogger("WebsiteContactScanner")

# ---------------------------------------------------------------------------
# Константы и паттерны
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

_PHONE_RE = re.compile(
    r"(?:\+\s*7|8)"
    r"\s*[\-–—.(]*"
    r"\d{3}"
    r"[\s\-–—.)]*"
    r"\d{3}"
    r"[\s\-–—]*"
    r"\d{2}"
    r"[\s\-–—]*"
    r"\d{2}",
)

_CONTACT_LINK_RE = re.compile(
    r"контакт|связ[аь]|обратн|реквизит|contact|about"
    r"|о\s*компании|о\s*нас|наши\s*контакт",
    re.IGNORECASE,
)

_FALLBACK_PATHS: tuple[str, ...] = (
    "/contacts",
    "/kontakty",
    "/contact",
    "/about",
    "/about-us",
    "/o-kompanii",
    "/o-nas",
)

_JUNK_LOCAL_PARTS: frozenset[str] = frozenset(
    {
        "noreply",
        "no-reply",
        "no.reply",
        "mailer-daemon",
        "postmaster",
        "hostmaster",
        "root",
        "nobody",
        "test",
        "example",
        "email",
        "your",
        "name",
        "user",
    }
)

_JUNK_DOMAINS: frozenset[str] = frozenset(
    {
        # Placeholder-домены
        "example.com",
        "example.org",
        "test.com",
        "domain.com",
        "email.com",
        "site.com",
        "yoursite.com",
        "company.com",
        # Технические сервисы
        "sentry.io",
        "wixpress.com",
        "w3.org",
        "schema.org",
        "googleapis.com",
        "gravatar.com",
        "cloudflare.com",
        # Соцсети
        "google.com",
        "facebook.com",
        "twitter.com",
        "instagram.com",
        "vk.com",
        "ok.ru",
        "youtube.com",
        "t.me",
        "telegram.org",
        # Хостинги и регистраторы (парковка доменов)
        "timeweb.ru",
        "timeweb.com",
        "reg.ru",
        "nic.ru",
        "beget.com",
        "beget.tech",
        "jino.ru",
        "spaceweb.ru",
        "mchost.ru",
        "hostinger.com",
        "godaddy.com",
        "namecheap.com",
        # Конструкторы (email оттуда — мусор)
        "wix.com",
        "tilda.cc",
        "tildacdn.com",
        "taplink.cc",
        "taplink.at",
        "ucoz.ru",
        "ucoz.com",
        "narod.ru",
        "orgs.biz",
        "vsite.biz",
        # CMS-платформы
        "wordpress.com",
        "blogspot.com",
        "livejournal.com",
    }
)

_PARKING_DOMAINS: frozenset[str] = frozenset(
    {
        "timeweb.ru",
        "timeweb.com",
        "reg.ru",
        "nic.ru",
        "beget.com",
        "beget.tech",
        "jino.ru",
        "spaceweb.ru",
        "mchost.ru",
        "hostinger.com",
        "parking.reg.ru",
        "rf.ru",
    }
)

_FILE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".css",
        ".js",
        ".map",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".zip",
        ".rar",
        ".mp3",
        ".mp4",
        ".avi",
        ".mov",
    }
)

_INTRA_PAGE_DELAY: float = 0.5


# ---------------------------------------------------------------------------
# CloudFlare email protection decoder
# ---------------------------------------------------------------------------


def _decode_cf_email(encoded: str) -> str | None:
    """Декодирует email, защищённый CloudFlare email protection.

    CloudFlare заменяет email на ``<span data-cfemail="hex">`` в HTML.
    Первый байт — XOR-ключ, остальные — зашифрованные символы.

    Args:
        encoded: Hex-строка из атрибута ``data-cfemail``.

    Returns:
        Декодированный email или ``None`` при ошибке.
    """
    try:
        key = int(encoded[:2], 16)
        return "".join(
            chr(int(encoded[i : i + 2], 16) ^ key) for i in range(2, len(encoded), 2)
        )
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# WebsiteContactScanner
# ---------------------------------------------------------------------------


class WebsiteContactScanner:
    """Сканирует сайты компаний для извлечения email и телефонов.

    Предназначен для пакетной обработки: получает список компаний из БД,
    сканирует их сайты с ограничением конкурентности, сохраняет найденные
    контакты обратно в БД.

    Не является подклассом ``BaseEnricher`` — другой интерфейс и жизненный
    цикл. Создаётся и запускается непосредственно из ``main.py``.

    Example::

        scanner = WebsiteContactScanner(db=db, concurrency=5, delay=1.0)
        stats = await scanner.run(limit=0)
        await scanner.close()
    """

    def __init__(
        self,
        db: DatabaseManager,
        concurrency: int = 5,
        delay: float = 1.0,
        timeout: float = 15.0,
        max_pages: int = 4,
        cache_dir: Path | None = None,
        cache_ttl_days: int | None = None,
    ) -> None:
        """
        Args:
            db:             Подключённый менеджер БД.
            concurrency:    Максимальное число одновременно сканируемых сайтов.
            delay:          Задержка (сек) между сайтами в одном слоте.
            timeout:        Таймаут HTTP-запросов (сек).
            max_pages:      Максимум страниц на один сайт.
            cache_dir:      Директория файлового кэша HTML. None — кэш отключён.
            cache_ttl_days: TTL кэша в днях. None → ``config.CACHE_TTL_DAYS``.
        """
        self._db = db
        self._delay = delay
        self._max_pages = max_pages
        self._semaphore = asyncio.Semaphore(concurrency)

        self._cache_dir: Path | None = (
            cache_dir if (cache_dir is not None and config.USE_HTML_CACHE) else None
        )
        self._cache_ttl_days: int = (
            cache_ttl_days if cache_ttl_days is not None else config.CACHE_TTL_DAYS
        )

        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/149.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;" "q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
            },
            timeout=httpx.Timeout(timeout, connect=10.0),
            follow_redirects=True,
            verify=False,
        )

    # -----------------------------------------------------------------------
    # Публичный API
    # -----------------------------------------------------------------------

    async def run(
        self,
        limit: int = 0,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Запускает пакетное сканирование сайтов компаний.

        Args:
            limit:   Максимум компаний (0 — без лимита).
            filters: Словарь ``config.WEBSITE_SCAN_CONFIG`` с фильтрами.
                     Передаётся напрямую в ``db.get_companies_for_website_scan``.

        Returns:
            Словарь статистики::

                {"scanned": int, "failed": int,
                 "emails_found": int, "phones_found": int}
        """
        companies = await self._db.get_companies_for_website_scan(limit, filters)
        total = len(companies)

        if total == 0:
            logger.info("Нет компаний с сайтом для сканирования.")
            return {"scanned": 0, "failed": 0, "emails_found": 0, "phones_found": 0}

        logger.info(
            f"Сканирование сайтов: {total} компаний, "
            f"конкурентность {self._semaphore._value}."
        )

        stats: dict[str, int] = {
            "scanned": 0,
            "failed": 0,
            "emails_found": 0,
            "phones_found": 0,
        }
        stats_lock = asyncio.Lock()
        counter: dict[str, int] = {"idx": 0}

        async def _process(company: dict[str, Any]) -> None:
            async with self._semaphore:
                async with stats_lock:
                    counter["idx"] += 1
                    idx = counter["idx"]

                company_id = int(company["company_id"])
                website: str = company["website"]
                pct = idx / total * 100

                try:
                    emails, phones = await self._scan_one(website)
                    new_e = new_p = 0

                    if emails or phones:
                        new_e, new_p = await self._db.save_website_contacts(
                            company_id,
                            list(emails),
                            list(phones),
                        )

                    parts: list[str] = []
                    if new_e:
                        parts.append(f"email:+{new_e}")
                    if new_p:
                        parts.append(f"тел:+{new_p}")
                    status = f"[{' '.join(parts)}]" if parts else "[ничего нового]"

                    logger.info(f"[{idx}/{total}] ({pct:.1f}%) {website} → {status}")

                    async with stats_lock:
                        stats["scanned"] += 1
                        stats["emails_found"] += new_e
                        stats["phones_found"] += new_p

                except Exception as exc:
                    logger.debug(
                        f"[{idx}/{total}] ({pct:.1f}%) {website} → ошибка: {exc}"
                    )
                    async with stats_lock:
                        stats["failed"] += 1

                await asyncio.sleep(self._delay)

        try:
            async with asyncio.TaskGroup() as tg:
                for company in companies:
                    tg.create_task(_process(company))
        except* Exception as eg:
            for exc in eg.exceptions:
                logger.error(f"Сбой в сканере: {exc}", exc_info=exc)

        logger.info(
            f"Сканирование завершено. "
            f"Просканировано: {stats['scanned']}/{total}, "
            f"ошибок: {stats['failed']}, "
            f"новых email: {stats['emails_found']}, "
            f"новых телефонов: {stats['phones_found']}."
        )
        return stats

    async def close(self) -> None:
        """Закрывает HTTP-клиент."""
        await self._client.aclose()

    # -----------------------------------------------------------------------
    # Файловый кэш HTML
    # -----------------------------------------------------------------------

    def _get_cache_path(self, url: str) -> Path:
        """Возвращает путь к файлу кэша для данного URL.

        Ключ — SHA-256 хэш URL, гарантирует уникальность и безопасность FS.

        Args:
            url: Полный URL страницы.

        Returns:
            Path вида ``{cache_dir}/{sha256}.html``.
        """
        assert self._cache_dir is not None
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self._cache_dir / f"{key}.html"

    async def _load_from_cache(self, url: str) -> str | None:
        """Загружает HTML из файлового кэша, если он не устарел.

        Args:
            url: Полный URL страницы.

        Returns:
            HTML-текст или ``None`` при промахе / устаревании кэша.
        """
        if self._cache_dir is None:
            return None

        path = self._get_cache_path(url)

        try:
            exists: bool = await asyncio.to_thread(path.exists)
            if not exists:
                return None

            if self._cache_ttl_days > 0:
                mtime: float = await asyncio.to_thread(lambda: path.stat().st_mtime)
                age_days = (datetime.now().timestamp() - mtime) / 86_400
                if age_days > self._cache_ttl_days:
                    logger.debug(f"Кэш устарел ({age_days:.1f} дн.): {url}")
                    return None

            html: str = await asyncio.to_thread(path.read_text, "utf-8")
            logger.debug(f"Кэш-хит: {url}")
            return html

        except Exception as exc:
            logger.debug(f"Ошибка чтения кэша для {url}: {exc}")
            return None

    async def _save_to_cache(self, url: str, html: str) -> None:
        """Сохраняет HTML-страницу в файловый кэш.

        Args:
            url:  Полный URL страницы (используется как ключ).
            html: HTML-текст для сохранения.
        """
        if self._cache_dir is None:
            return

        path = self._get_cache_path(url)
        try:
            await asyncio.to_thread(self._cache_dir.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(path.write_text, html, "utf-8")
        except Exception as exc:
            logger.debug(f"Ошибка записи кэша для {url}: {exc}")

    # -----------------------------------------------------------------------
    # Сканирование одного сайта
    # -----------------------------------------------------------------------

    async def _scan_one(self, website: str) -> tuple[set[str], set[str]]:
        """Сканирует один сайт: главная + контактные страницы.

        Пробует HTTPS → HTTP fallback. Загружает главную, находит ссылки
        на контакты, загружает до ``max_pages - 1`` дополнительных страниц.

        Args:
            website: Нормализованный домен (``"akbnovikov.com"``).

        Returns:
            Кортеж ``(emails, phones)`` — множества валидных контактов.
        """
        all_emails: set[str] = set()
        all_phones: set[str] = set()

        base_url = f"https://{website}"
        main_html = await self._fetch(base_url)
        if main_html is None:
            base_url = f"http://{website}"
            main_html = await self._fetch(base_url)
        if main_html is None:
            return all_emails, all_phones

        emails, phones = self._extract_contacts(main_html)
        all_emails.update(emails)
        all_phones.update(phones)

        contact_urls = self._discover_contact_urls(base_url, main_html)

        pages_fetched = 1
        for url in contact_urls:
            if pages_fetched >= self._max_pages:
                break

            await asyncio.sleep(_INTRA_PAGE_DELAY)
            html = await self._fetch(url)
            if html is None:
                continue

            pages_fetched += 1
            emails, phones = self._extract_contacts(html)
            all_emails.update(emails)
            all_phones.update(phones)

        return all_emails, all_phones

    # -----------------------------------------------------------------------
    # HTTP + кэш
    # -----------------------------------------------------------------------

    async def _fetch(self, url: str) -> str | None:
        """GET-запрос с кэш-слоем и детекцией припаркованных доменов.

        Алгоритм:
          1. Проверить файловый кэш.
          2. При промахе — HTTP-запрос.
          3. Проверить: не увёл ли redirect на чужой домен (parking detection).
          4. Сохранить успешный ответ в кэш.

        Args:
            url: Полный URL с протоколом.

        Returns:
            HTML-текст или ``None`` при ошибке / нетекстовом ответе / парковке.
        """
        cached = await self._load_from_cache(url)
        if cached is not None:
            return cached

        try:
            resp = await self._client.get(url)
            resp.raise_for_status()

            # Детекция припаркованных доменов:
            # если redirect увёл на другой домен — сайт мёртв / припаркован.
            original_domain = urlparse(url).netloc.lower()
            final_domain = urlparse(str(resp.url)).netloc.lower()

            if final_domain != original_domain:
                # Проверяем: это парковка хостера?
                for parking in _PARKING_DOMAINS:
                    if parking in final_domain:
                        logger.debug(f"Припаркованный домен: {url} → {resp.url}")
                        return None
                # Redirect на поддомен или www — нормально.
                orig_base = original_domain.removeprefix("www.")
                final_base = final_domain.removeprefix("www.")
                if orig_base != final_base and not final_domain.endswith(
                    f".{orig_base}"
                ):
                    logger.debug(f"Redirect на чужой домен: {url} → {resp.url}")
                    return None

            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type and "text/plain" not in content_type:
                return None
            html = resp.text
        except Exception:
            return None

        await self._save_to_cache(url, html)
        return html

    # -----------------------------------------------------------------------
    # Поиск страницы контактов
    # -----------------------------------------------------------------------

    def _discover_contact_urls(self, base_url: str, html: str) -> list[str]:
        """Находит URL страниц с контактной информацией.

        Алгоритм:
          1. Парсит все ``<a>`` из HTML.
          2. Фильтрует по ключевым словам в ``href`` и тексте ссылки.
          3. Отсекает внешние домены.
          4. При пустом результате добавляет fallback-пути.

        Args:
            base_url: URL главной страницы (для resolve относительных ссылок).
            html:     HTML главной страницы.

        Returns:
            Дедуплицированный список URL (до 5 штук).
        """
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(base_url).netloc.lower()
        found: dict[str, None] = {}

        for tag in soup.find_all("a"):
            if not isinstance(tag, Tag):
                continue

            raw_href = tag.get("href")
            if not raw_href or not isinstance(raw_href, str):
                continue

            if raw_href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue

            text = tag.get_text(strip=True)
            resolved = urljoin(base_url, raw_href)
            resolved_domain = urlparse(resolved).netloc.lower()

            if resolved_domain and resolved_domain != base_domain:
                continue

            if _CONTACT_LINK_RE.search(raw_href) or _CONTACT_LINK_RE.search(text):
                found[resolved] = None

        if not found:
            for path in _FALLBACK_PATHS:
                found[urljoin(base_url, path)] = None

        return list(found)[:5]

    # -----------------------------------------------------------------------
    # Извлечение контактов (4 уровня email + телефоны)
    # -----------------------------------------------------------------------

    def _extract_contacts(self, html: str) -> tuple[set[str], set[str]]:
        """Извлекает email и телефоны из HTML через все 4 уровня.

        Args:
            html: HTML-код страницы.

        Returns:
            Кортеж ``(emails, phones)`` — множества валидных строк.
        """
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")

        raw_emails: set[str] = set()
        raw_emails.update(self._extract_mailto(soup))
        raw_emails.update(self._extract_email_regex(text))
        raw_emails.update(self._decode_cf_emails(soup))
        raw_emails.update(self._extract_jsonld_emails(soup))

        valid_emails = {e for e in raw_emails if self._is_valid_email(e)}

        phones: set[str] = set()
        for raw in _PHONE_RE.findall(text):
            normalized = self._normalize_phone(raw)
            if normalized:
                phones.add(normalized)

        return valid_emails, phones

    # --- L1: mailto ---------------------------------------------------------

    @staticmethod
    def _extract_mailto(soup: BeautifulSoup) -> set[str]:
        """Извлекает email из ``<a href="mailto:...">`` ссылок.

        Args:
            soup: Распарсенное дерево BeautifulSoup.

        Returns:
            Множество email-адресов в нижнем регистре.
        """
        result: set[str] = set()
        for tag in soup.find_all("a"):
            if not isinstance(tag, Tag):
                continue
            raw = tag.get("href")
            href = str(raw) if raw is not None else ""
            if not href.lower().startswith("mailto:"):
                continue
            addr = href.split(":", 1)[1].split("?", 1)[0].strip().lower()
            if "@" in addr:
                result.add(addr)
        return result

    # --- L2: regex ----------------------------------------------------------

    @staticmethod
    def _extract_email_regex(text: str) -> set[str]:
        """Извлекает email регулярным выражением из текста страницы.

        Args:
            text: Очищенный текст страницы (``soup.get_text()``).

        Returns:
            Множество email-адресов в нижнем регистре.
        """
        return {m.lower() for m in _EMAIL_RE.findall(text)}

    # --- L3: CloudFlare email protection ------------------------------------

    @staticmethod
    def _decode_cf_emails(soup: BeautifulSoup) -> set[str]:
        """Декодирует email, защищённые CloudFlare email protection.

        Использует CSS-селектор ``[data-cfemail]`` вместо ``find_all(attrs=...)``,
        чтобы избежать конфликта типов в bs4-стабсах Pyright.

        Args:
            soup: Распарсенное дерево BeautifulSoup.

        Returns:
            Множество декодированных email-адресов.
        """
        result: set[str] = set()
        for tag in soup.select("[data-cfemail]"):
            if not isinstance(tag, Tag):
                continue
            raw = tag.get("data-cfemail")
            encoded = str(raw) if raw is not None else ""
            if not encoded:
                continue
            decoded = _decode_cf_email(encoded)
            if decoded and "@" in decoded:
                result.add(decoded.lower())
        return result

    # --- L4: JSON-LD / Schema.org -------------------------------------------

    @staticmethod
    def _extract_jsonld_emails(soup: BeautifulSoup) -> set[str]:
        """Извлекает email из JSON-LD блоков (Schema.org structured data).

        Рекурсивно обходит все вложенные объекты и массивы,
        извлекая любые поля с ключом ``email``.

        Args:
            soup: Распарсенное дерево BeautifulSoup.

        Returns:
            Множество email-адресов из структурированных данных.
        """
        result: set[str] = set()

        def _collect(obj: Any) -> None:
            if isinstance(obj, dict):
                for key, val in obj.items():
                    if key.lower() == "email" and isinstance(val, str) and "@" in val:
                        result.add(val.strip().lower())
                    else:
                        _collect(val)
            elif isinstance(obj, list):
                for item in obj:
                    _collect(item)

        for script in soup.find_all("script", type="application/ld+json"):
            if not isinstance(script, Tag):
                continue
            raw = script.string
            if not raw:
                continue
            try:
                data = json.loads(raw)
                _collect(data)
            except (json.JSONDecodeError, TypeError):
                continue

        return result

    # -----------------------------------------------------------------------
    # Валидация и нормализация
    # -----------------------------------------------------------------------

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Проверяет email на валидность и отсекает мусор.

        Фильтры:
          • Формат (ровно один ``@``, домен с точкой).
          • Файловые расширения (``.png``, ``.js`` и др.).
          • Мусорные local-part (``noreply``, ``test``).
          • Мусорные домены (``example.com``, соцсети).

        Args:
            email: Email-адрес в нижнем регистре.

        Returns:
            ``True`` если email пригоден для использования.
        """
        email = email.strip().lower()
        if email.count("@") != 1:
            return False

        local, domain = email.split("@", 1)
        if not local or not domain or "." not in domain:
            return False

        if len(local) > 64 or len(domain) > 255:
            return False

        for ext in _FILE_EXTENSIONS:
            if email.endswith(ext):
                return False

        if local in _JUNK_LOCAL_PARTS:
            return False

        for junk_domain in _JUNK_DOMAINS:
            if domain == junk_domain or domain.endswith(f".{junk_domain}"):
                return False

        return True

    @staticmethod
    def _normalize_phone(raw: str) -> str | None:
        """Нормализует телефон в формат ``+7 (XXX) XXX-XX-XX``.

        Args:
            raw: Сырая строка телефона из regex-матча.

        Returns:
            Нормализованный телефон или ``None`` при невалидной длине.
        """
        digits = "".join(filter(str.isdigit, raw))
        if len(digits) == 11 and digits[0] in ("7", "8"):
            d = "7" + digits[1:]
            return f"+7 ({d[1:4]}) {d[4:7]}-{d[7:9]}-{d[9:11]}"
        if len(digits) == 10:
            return f"+7 ({digits[0:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:10]}"
        return None
