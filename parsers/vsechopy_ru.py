"""
ScraperELA · parsers/vsechopy_ru.py
====================================
Парсер сайта https://всечопы.рф (xn--b1ag1aeh1b4a.xn--p1ai).
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from models import AggregatorStatus, CompanySchema
from parsers.base import BaseParser


class VseChopyRuParser(BaseParser):
    """Стратегия парсинга ресурса https://всечопы.рф."""

    BASE_URL = "https://xn--b1ag1aeh1b4a.xn--p1ai"

    _SKIP_SLUGS: frozenset[str] = frozenset(
        {"page", "add", "search", "category", "filter", "city"}
    )

    def parse_listing(self, html: str) -> list[str]:
        """Извлекает уникальные URL карточек со страницы каталога.

        Args:
            html: HTML разметка страницы списка.

        Returns:
            Список найденных URL детальных страниц.
        """
        soup = BeautifulSoup(html, "lxml")
        urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            # На всечопы.рф карточки имеют паттерн /chop/slug/
            match = re.fullmatch(r"/chop/([^/]+)/?", path)
            if not match:
                continue

            slug = match.group(1)
            if slug in self._SKIP_SLUGS:
                continue

            urls.add(urljoin(self.BASE_URL, f"/chop/{slug}/"))

        return list(urls)

    def parse_detail(self, html: str, url: str) -> CompanySchema | None:
        """Извлекает полную информацию об организации с детальной страницы.

        Args:
            html: HTML разметка детальной страницы.
            url: Исходный URL страницы.

        Returns:
            Объект CompanySchema или None при критической ошибке парсинга.
        """
        soup = BeautifulSoup(html, "lxml")

        # 1. Название организации
        name = self._extract_name(soup)
        if not name:
            return None

        # 2. Статус работы (ACTIVE по умолчанию)
        status_aggregator = AggregatorStatus.ACTIVE

        # 3. Извлечение реквизитов из блока "Реквизиты"
        requisites = self._extract_all_requisites(soup)

        inn = requisites.get("инн")
        ogrn = requisites.get("огрн")
        kpp_raw = requisites.get("кпп")
        director = requisites.get("руководитель")
        legal_name = requisites.get("полное название") or requisites.get(
            "полное наименование"
        )

        kpp_list: list[str] = []
        if kpp_raw:
            kpp_list = re.findall(r"\d{9}", kpp_raw)

        # 4. Адрес
        address = self._extract_address(soup)

        # 5. Телефоны
        phones = self._extract_phones(soup)

        # 6. Email
        emails = self._extract_emails(soup)

        # 7. Веб-сайт
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

    # --- Section: Вспомогательные методы извлечения ---

    @staticmethod
    def _extract_name(soup: BeautifulSoup) -> str | None:
        """Берёт текст из h1.about__title, с откатом до общего h1."""
        h1 = soup.find("h1", class_="about__title")
        if not h1 or not isinstance(h1, Tag):
            h1 = soup.find("h1")
        if not h1 or not isinstance(h1, Tag):
            return None
        return h1.get_text(strip=True) or None

    @staticmethod
    def _extract_address(soup: BeautifulSoup) -> str | None:
        """Извлекает текстовое представление адреса."""
        tag = soup.find("span", class_="about__address")
        if not tag or not isinstance(tag, Tag):
            return None
        return tag.get_text(strip=True) or None

    @staticmethod
    def _extract_phones(soup: BeautifulSoup) -> list[str]:
        """Собирает телефоны только из блока контактов текущей организации."""
        phones: list[str] = []
        seen: set[str] = set()

        def _add(val: str) -> None:
            v = val.strip()
            if v and v not in seen:
                phones.append(v)
                seen.add(v)

        # Ограничиваем область поиска только блоком контактов самой компании
        contacts_block = soup.find(class_="about__contacts") or soup

        for a in contacts_block.find_all("a", class_="about__phone"):
            _add(a.get_text(strip=True))

        for a in contacts_block.find_all("a", href=re.compile(r"^tel:")):
            href = a.get("href", "")
            if isinstance(href, str):
                val = a.get_text(strip=True) or href.replace("tel:", "")
                _add(val)

        return phones

    @staticmethod
    def _extract_emails(soup: BeautifulSoup) -> list[str]:
        """Собирает email-адреса только из блока контактов текущей организации."""
        emails: list[str] = []
        seen: set[str] = set()

        def _add(val: str) -> None:
            v = val.strip().lower()
            if v and v not in seen:
                emails.append(v)
                seen.add(v)

        # Ограничиваем область поиска только блоком контактов самой компании
        contacts_block = soup.find(class_="about__contacts") or soup

        for a in contacts_block.find_all("a", class_="about__email"):
            _add(a.get_text(strip=True))

        for a in contacts_block.find_all("a", href=re.compile(r"^mailto:")):
            href = a.get("href", "")
            if isinstance(href, str):
                val = a.get_text(strip=True) or href.replace("mailto:", "")
                _add(val)

        return emails

    @staticmethod
    def _extract_website(soup: BeautifulSoup) -> str | None:
        """Извлекает сайт только из блока контактов текущей организации, вырезая метки UTM."""
        contacts_block = soup.find(class_="about__contacts") or soup

        site_a = None
        for li in contacts_block.find_all("li"):
            if li.find("i", class_="fa-globe"):
                site_a = li.find("a")
                break

        if not site_a:
            site_a = contacts_block.find("a", href=re.compile(r"utm_source="))

        if not site_a or not isinstance(site_a, Tag):
            return None

        href = site_a.get("href", "")
        if not isinstance(href, str) or not href:
            return None

        if href.startswith("http"):
            if "?" in href:
                href = href.split("?")[0]
            return href

        return site_a.get_text(strip=True) or None

    @staticmethod
    def _extract_all_requisites(soup: BeautifulSoup) -> dict[str, str]:
        """Парсит блок реквизитов во внутренний словарь."""
        requisites: dict[str, str] = {}
        for block in soup.find_all(class_="about__block"):
            span = block.find("span", class_="about__small")
            if span and "реквизиты" in span.get_text(strip=True).lower():
                for div in block.find_all("div"):
                    text = div.get_text(strip=True)
                    if ":" in text:
                        key, val = text.split(":", maxsplit=1)
                        requisites[key.strip().lower()] = val.strip()
        return requisites
