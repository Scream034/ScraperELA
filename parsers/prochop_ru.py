"""
ScraperELA · parsers/prochop_ru.py
====================================
Парсер сайта https://prochop.ru.

Структура HTML (ключевые селекторы):
  Каталог:
    Ссылки на карточки: <a class="title-arhive-chops" href="/chops/SLUG/">
    Пагинация:          /chops/page/N/

  Детальная карточка:
    Название:       h1.entry-title
    Осн. телефон:   span.phone-org
    Доп. телефоны:  span.dop-phone
    Email:          span.company-email
    Сайт:           a.web-site-org (href="/link/?url=DOMAIN")
    Адрес:          span.address-text
    Реквизиты:      div.optionally-info → span.contakt → strong + siblings
                    Юр. название: strong пустой, текст идёт сразу за ним
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString

from models import AggregatorStatus, CompanySchema
from parsers.base import BaseParser


class ProchopRuParser(BaseParser):
    """Стратегия парсинга ресурса https://prochop.ru."""

    BASE_URL = "https://prochop.ru"

    # Слаги, которые не являются карточками организаций
    _SKIP_SLUGS: frozenset[str] = frozenset(
        {"page", "add", "search", "category", "filter", "tag"}
    )

    # -----------------------------------------------------------------------
    # Листинг (каталог)
    # -----------------------------------------------------------------------

    def parse_listing(self, html: str) -> list[str]:
        """
        Извлекает URL карточек со страницы каталога.

        Ищет ссылки двумя способами:
          1. <a class="title-arhive-chops"> — основной селектор.
          2. Любые <a href="/chops/SLUG/"> — фоллбэк.
        """
        soup = BeautifulSoup(html, "lxml")
        urls: set[str] = set()

        # Основной способ — по классу
        for a_tag in soup.find_all("a", class_="title-arhive-chops", href=True):
            href = a_tag.get("href")
            if isinstance(href, str):
                url = self._normalize_chops_url(href)
                if url:
                    urls.add(url)

        # Фоллбэк — по паттерну URL
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href")
            if not isinstance(href, str):
                continue
            url = self._normalize_chops_url(href)
            if url:
                urls.add(url)

        return list(urls)

    def _normalize_chops_url(self, href: str) -> str | None:
        """
        Проверяет что href ведёт на карточку /chops/SLUG/
        и возвращает полный URL или None.
        """
        path = urlparse(href).path
        match = re.fullmatch(r"/chops/([^/]+)/?", path)
        if not match:
            return None

        slug = match.group(1)
        if slug in self._SKIP_SLUGS:
            return None

        return urljoin(self.BASE_URL, f"/chops/{slug}/")

    # -----------------------------------------------------------------------
    # Детальная карточка
    # -----------------------------------------------------------------------

    def parse_detail(self, html: str, url: str) -> CompanySchema | None:
        soup = BeautifulSoup(html, "lxml")

        # 1. Название ---------------------------------------------------------
        name = self._extract_name(soup)
        if not name:
            return None

        # 2. Статус (на prochop.ru нет явных плашек «закрыт») -----------------
        status_aggregator = AggregatorStatus.ACTIVE

        # 3. Реквизиты из DOM (div.optionally-info → span.contakt) ------------
        requisites = self._extract_all_requisites(soup)

        inn = requisites.get("инн")
        ogrn = requisites.get("огрн")
        kpp_raw = requisites.get("кпп")
        director = requisites.get("руководитель")
        # Юр. название: на prochop.ru strong пустой → ключ ""
        legal_name = requisites.get("") or requisites.get("юридическое название")

        kpp_list: list[str] = re.findall(r"\d{9}", kpp_raw) if kpp_raw else []

        # 4. Адрес ------------------------------------------------------------
        address = self._extract_address(soup)

        # 5. Телефоны ---------------------------------------------------------
        phones = self._extract_phones(soup)

        # 6. Email ------------------------------------------------------------
        emails = self._extract_emails(soup)

        # 7. Сайт -------------------------------------------------------------
        website = self._extract_website(soup)

        return CompanySchema(
            source_url=url,
            name=name,
            legal_name=legal_name,
            inn=inn,
            ogrn=ogrn,
            kpp_list=kpp_list,
            director=director,
            address=address,
            phones=phones,
            emails=emails,
            website=website,
            status_aggregator=status_aggregator,
        )

    # -----------------------------------------------------------------------
    # Извлечение реквизитов из DOM
    # -----------------------------------------------------------------------

    @staticmethod
    def _extract_all_requisites(soup: BeautifulSoup) -> dict[str, str]:
        """
        Собирает все реквизиты из блока div.optionally-info.

        Структура:
          <span class="contakt"><strong>ОГРН </strong> 1032128005425</span>
          <span class="contakt"><strong></strong> ООО "ЧОО "Цербер"</span>
                                 ↑ пустой strong = юридическое название

        Возвращает dict с нормализованными ключами (lowercase, strip).
        Пустой ключ "" → юридическое название.
        """
        result: dict[str, str] = {}

        # Ограничиваем поиск только блоком реквизитов
        info_block = soup.find("div", class_="optionally-info")
        search_scope = info_block if info_block else soup

        for span in search_scope.find_all("span", class_="contakt"):
            if not isinstance(span, Tag):
                continue

            strong = span.find("strong")
            if strong is None:
                # Нет <strong> — весь текст = значение, ключ пустой
                text = span.get_text(strip=True)
                if text:
                    result[""] = text
                continue

            if not isinstance(strong, Tag):
                continue

            # Ключ — текст внутри <strong>
            key = strong.get_text(strip=True).lower().rstrip(":")

            # Значение — всё что идёт ПОСЛЕ <strong> и <br>
            value_parts: list[str] = []
            for sibling in strong.next_siblings:
                if isinstance(sibling, NavigableString):
                    text = sibling.strip()
                    if text:
                        value_parts.append(text)
                elif isinstance(sibling, Tag):
                    if sibling.name == "br":
                        continue
                    text = sibling.get_text(strip=True)
                    if text:
                        value_parts.append(text)

            value = " ".join(value_parts).strip()
            if value:
                result[key] = value

        return result

    # -----------------------------------------------------------------------
    # Вспомогательные методы
    # -----------------------------------------------------------------------

    @staticmethod
    def _extract_name(soup: BeautifulSoup) -> str | None:
        h1 = soup.find("h1", class_="entry-title")
        if not h1 or not isinstance(h1, Tag):
            # Фоллбэк — любой h1
            h1 = soup.find("h1")
        if not h1 or not isinstance(h1, Tag):
            return None
        return h1.get_text(strip=True) or None

    @staticmethod
    def _extract_address(soup: BeautifulSoup) -> str | None:
        """
        Адрес лежит в <span class="address-text">.
        Содержит вложенные <span itemprop="..."> — собираем get_text().
        Убираем завершающую точку.
        """
        tag = soup.find("span", class_="address-text")
        if not tag or not isinstance(tag, Tag):
            return None
        text = tag.get_text(strip=True).rstrip(".")
        # Убираем ссылки на регион внутри адреса (текст уже в get_text)
        return text or None

    @staticmethod
    def _extract_phones(soup: BeautifulSoup) -> list[str]:
        """
        Основной телефон: span.phone-org
        Дополнительные:   span.dop-phone (может быть несколько)
        """
        phones: list[str] = []
        seen: set[str] = set()

        def _add(value: str) -> None:
            v = value.strip()
            if v and v not in seen:
                phones.append(v)
                seen.add(v)

        # Основной
        main = soup.find("span", class_="phone-org")
        if main and isinstance(main, Tag):
            _add(main.get_text(strip=True))

        # Дополнительные — каждый в отдельном <span class="dop-phone">
        for span in soup.find_all("span", class_="dop-phone"):
            if isinstance(span, Tag):
                _add(span.get_text(strip=True))

        return phones

    @staticmethod
    def _extract_emails(soup: BeautifulSoup) -> list[str]:
        """
        Email-ы лежат в <span class="company-email"> (может быть несколько).
        Также собираем из mailto-ссылок.
        """
        emails: list[str] = []
        seen: set[str] = set()

        def _add(value: str) -> None:
            v = value.strip().lower()
            if v and v not in seen:
                emails.append(v)
                seen.add(v)

        for span in soup.find_all("span", class_="company-email"):
            if isinstance(span, Tag):
                _add(span.get_text(strip=True))

        # Фоллбэк — mailto-ссылки
        for a in soup.find_all("a", href=re.compile(r"^mailto:")):
            if isinstance(a, Tag):
                href = a.get("href", "")
                if isinstance(href, str):
                    _add(href.replace("mailto:", ""))

        return emails

    @staticmethod
    def _extract_website(soup: BeautifulSoup) -> str | None:
        """
        Сайт: <a class="web-site-org" href="/link/?url=cerber21.com">
        Редирект через /link/?url=DOMAIN
        """
        site_a = soup.find("a", class_="web-site-org")
        if not site_a or not isinstance(site_a, Tag):
            return None

        href = site_a.get("href", "")
        if not isinstance(href, str) or not href:
            return None

        # /link/?url=DOMAIN
        if "url=" in href:
            match = re.search(r"url=([^&]+)", href)
            return match.group(1) if match else site_a.get_text(strip=True) or None

        if href.startswith("http"):
            return href

        return site_a.get_text(strip=True) or None
