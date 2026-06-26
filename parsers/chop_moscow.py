"""
ScraperELA · parsers/chop_moscow.py
====================================
Парсер сайта https://chop.moscow.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString

from models import AggregatorStatus, CompanySchema
from parsers.base import BaseParser


class ChopMoscowParser(BaseParser):
    """Стратегия парсинга ресурса https://chop.moscow."""

    BASE_URL = "https://chop.moscow"

    _SKIP_SLUGS: frozenset[str] = frozenset(
        {"page", "add", "search", "category", "filter"}
    )

    # -----------------------------------------------------------------------
    # Листинг (каталог)
    # -----------------------------------------------------------------------

    def parse_listing(self, html: str) -> list[str]:
        """Извлекает уникальные URL карточек со страницы каталога."""
        soup = BeautifulSoup(html, "lxml")
        urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            match = re.fullmatch(r"/catalog/([^/]+)/?", path)
            if not match:
                continue

            slug = match.group(1)
            if slug in self._SKIP_SLUGS:
                continue

            urls.add(urljoin(self.BASE_URL, f"/catalog/{slug}/"))

        return list(urls)

    # -----------------------------------------------------------------------
    # Детальная карточка
    # -----------------------------------------------------------------------

    def parse_detail(self, html: str, url: str) -> CompanySchema | None:
        soup = BeautifulSoup(html, "lxml")

        # 1. Название ---------------------------------------------------------
        name = self._extract_name(soup)
        if not name:
            return None

        # 2. Статус работы ----------------------------------------------------
        status_aggregator = self._extract_status(soup)

        # 3. Реквизиты из DOM (span.contakt → strong) -------------------------
        requisites = self._extract_all_requisites(soup)

        inn = requisites.get("инн")
        ogrn = requisites.get("огрн")
        kpp_raw = requisites.get("кпп")
        reg_date = requisites.get("дата регистрации")
        director = requisites.get("руководитель")
        legal_name = requisites.get("юридическое название")

        # КПП — может быть несколько (если на странице несколько span.contakt с КПП)
        kpp_list: list[str] = []
        if kpp_raw:
            # Извлекаем все 9-значные числа из значения
            kpp_list = re.findall(r"\d{9}", kpp_raw)

        # 4. Адрес ------------------------------------------------------------
        address = self._extract_tag_text(soup, class_="adresch2")

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
            registration_date=reg_date,
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
        Собирает все реквизиты из блоков <span class="contakt">.

        Каждый блок имеет структуру:
          <span class="contakt"><strong>Ключ</strong> Значение</span>
          или
          <span class="contakt"><strong>Ключ</strong><br>Значение</span>

        Возвращает dict с нормализованными ключами (lowercase, без двоеточия):
          {"руководитель": "Иванов Иван Иванович", "инн": "7721278319", ...}
        """
        result: dict[str, str] = {}

        for span in soup.find_all("span", class_="contakt"):
            if not isinstance(span, Tag):
                continue

            strong = span.find("strong")
            if not strong or not isinstance(strong, Tag):
                continue

            # Ключ — текст внутри <strong>, нормализованный
            key = strong.get_text(strip=True).lower().rstrip(":")

            # Значение — всё что идёт ПОСЛЕ <strong> и <br> в этом же <span>
            # Удаляем <strong> из копии, чтобы get_text() вернул только значение
            # Но безопаснее — собрать текст вручную из siblings
            value_parts: list[str] = []
            for sibling in strong.next_siblings:
                if isinstance(sibling, NavigableString):
                    text = sibling.strip()
                    if text:
                        value_parts.append(text)
                elif isinstance(sibling, Tag):
                    if sibling.name == "br":
                        continue  # Пропускаем <br>
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
        """Берёт текст первого <h1>, отрезает всё после двоеточия."""
        h1 = soup.find("h1")
        if not h1 or not isinstance(h1, Tag):
            return None
        raw = h1.get_text(strip=True)
        return re.split(r":\s+", raw, maxsplit=1)[0].strip() or None

    @staticmethod
    def _extract_status(soup: BeautifulSoup) -> str:
        """Определяет статус организации по CSS-классам блоков-плашек."""
        if soup.find(class_="cloze"):
            return AggregatorStatus.CLOSED
        if soup.find(class_="cloze-perhaps"):
            return AggregatorStatus.MAYBE
        return AggregatorStatus.ACTIVE

    @staticmethod
    def _extract_tag_text(soup: BeautifulSoup, class_: str) -> str | None:
        tag = soup.find(class_=class_)
        if not tag or not isinstance(tag, Tag):
            return None
        return tag.get_text(strip=True) or None

    @staticmethod
    def _extract_phones(soup: BeautifulSoup) -> list[str]:
        """
        Собирает основной телефон (.telch) и дополнительные (.dop-info-p).
        Возвращает дедуплицированный список строк.
        """
        phones: list[str] = []
        seen: set[str] = set()

        def _add(value: str) -> None:
            v = value.strip()
            if v and v not in seen:
                phones.append(v)
                seen.add(v)

        # Основной телефон
        main = soup.find(class_="telch")
        if main and isinstance(main, Tag):
            _add(main.get_text(strip=True))

        # Дополнительные телефоны
        for p in soup.find_all("p", class_="dop-info-p"):
            if not isinstance(p, Tag):
                continue
            p_text = p.get_text(strip=True)
            if "Доп. телефоны:" not in p_text:
                continue
            dop = p_text.replace("Доп. телефоны:", "").strip()
            for part in re.split(r"[,;]", dop):
                _add(part)

        return phones

    @staticmethod
    def _extract_emails(soup: BeautifulSoup) -> list[str]:
        """
        Собирает email из тега .emailchop и mailto-ссылок.
        """
        emails: list[str] = []
        seen: set[str] = set()

        def _add(value: str) -> None:
            v = value.strip().lower()
            if v and v not in seen:
                emails.append(v)
                seen.add(v)

        email_tag = soup.find(class_="emailchop")
        if email_tag and isinstance(email_tag, Tag):
            _add(email_tag.get_text(strip=True))

        for a in soup.find_all("a", href=re.compile(r"^mailto:")):
            if not isinstance(a, Tag):
                continue
            href = a.get("href", "")
            if isinstance(href, str):
                _add(href.replace("mailto:", ""))

        return emails

    @staticmethod
    def _extract_website(soup: BeautifulSoup) -> str | None:
        """Извлекает сайт из ссылки .sitech, обрабатывая редиректы."""
        site_a = soup.find("a", class_="sitech")
        if not site_a or not isinstance(site_a, Tag):
            return None

        href = site_a.get("href", "")
        if not isinstance(href, str) or not href:
            return None

        if "url=" in href:
            match = re.search(r"url=([^&]+)", href)
            return match.group(1) if match else site_a.get_text(strip=True) or None

        if href.startswith("http"):
            return href

        return site_a.get_text(strip=True) or None
