# --- Section: parsers/russkii_souz.py ---
"""
ScraperELA · parsers/russkii_souz.py
======================================
Стратегия парсинга каталога «Русский союз» (xn--g1abdcwihado2k.xn--p1ai).
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from models import AggregatorStatus, CompanySchema
from parsers.base import BaseParser


class RusskiiSouzParser(BaseParser):
    """Стратегия парсинга ресурса https://русскийсоюз.рф."""

    BASE_URL: str = "https://xn--g1abdcwihado2k.xn--p1ai"

    def parse_listing(self, html: str) -> list[str]:
        """Извлекает уникальные URL карточек со страницы каталога.

        Args:
            html: HTML разметка страницы списка.

        Returns:
            Список найденных URL детальных страниц.
        """
        soup = BeautifulSoup(html, "lxml")
        urls: set[str] = set()

        for a_tag in soup.find_all("a", class_="title", href=True):
            href = a_tag.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            if "/services/" in path:
                urls.add(urljoin(self.BASE_URL, path))

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
        name_tag = soup.find("h1")
        if not name_tag:
            return None
        name = name_tag.get_text(strip=True)

        # 2. Статус работы (по умолчанию ACTIVE)
        status_aggregator = AggregatorStatus.ACTIVE

        # 3. Адрес и Руководитель/Владелец
        address: str | None = None
        director: str | None = None

        fields_div = soup.find(class_="fields")
        if fields_div:
            # Парсинг адреса
            addr_b = fields_div.find("b", string=re.compile(r"Адрес", re.IGNORECASE))
            if addr_b and addr_b.parent:
                raw_text = addr_b.parent.get_text(strip=True)
                address = re.sub(
                    r"^адрес\s*:\s*", "", raw_text, flags=re.IGNORECASE
                ).strip()

            # Парсинг руководителя или владельца
            dir_b = fields_div.find(
                "b", string=re.compile(r"Руководитель", re.IGNORECASE)
            )
            if dir_b and dir_b.parent:
                raw_text = dir_b.parent.get_text(strip=True)
                director = re.sub(
                    r"^руководитель\s*:\s*", "", raw_text, flags=re.IGNORECASE
                ).strip()
            else:
                owner_b = fields_div.find(
                    "b", string=re.compile(r"Владелец", re.IGNORECASE)
                )
                if owner_b and owner_b.parent:
                    raw_text = owner_b.parent.get_text(strip=True)
                    director = re.sub(
                        r"^владелец\s*:\s*", "", raw_text, flags=re.IGNORECASE
                    ).strip()

        # 4. Телефоны, Email, Сайт из параметров компании
        phones: list[str] = []
        emails: list[str] = []
        websites: list[str] = []

        for param in soup.find_all(class_="company__param"):
            label_tag = param.find(class_="company__label")
            if not label_tag:
                continue
            label_text = label_tag.get_text(strip=True).lower()
            val_tag = param.find(class_="company__value")
            if not val_tag:
                continue

            if "телефон" in label_text:
                for div in val_tag.find_all("div"):
                    phones.append(div.get_text(strip=True))
                if not phones:
                    phones.append(val_tag.get_text(strip=True))

            elif "email" in label_text or "почта" in label_text:
                for div in val_tag.find_all("div"):
                    emails.append(div.get_text(strip=True))
                if not emails:
                    emails.append(val_tag.get_text(strip=True))

            elif "сайт" in label_text:
                a_tags = val_tag.find_all("a", href=True)
                for a in a_tags:
                    href = a.get("href")
                    if isinstance(href, str) and not href.startswith("javascript"):
                        websites.append(href.strip())
                if not websites:
                    for div in val_tag.find_all("div"):
                        websites.append(div.get_text(strip=True))
                    if not websites:
                        websites.append(val_tag.get_text(strip=True))

        # 5. Экстракция реквизитов (ИНН, ОГРН, КПП, Официальное имя)
        inn: str | None = None
        ogrn: str | None = None
        kpp_list: list[str] = []
        legal_name: str | None = None

        search_texts: list[str] = []
        desc_div = soup.find(class_="text")
        if desc_div:
            search_texts.append(desc_div.get_text(" ", strip=True))

        tabs_div = soup.find(class_="entity-tabs")
        if tabs_div:
            search_texts.append(tabs_div.get_text(" ", strip=True))

        combined_text = " ".join(search_texts)

        # Регулярные выражения для реквизитов с использованием flags=re.IGNORECASE
        inn_match = re.search(
            r"\бинн\s*(?:/\s*кпп)?\s*[:\s,\.-]*(\d{10,12})\b",
            combined_text,
            flags=re.IGNORECASE,
        )
        if inn_match:
            inn = inn_match.group(1)

        ogrn_match = re.search(
            r"\bогрн(?:ип)?\b\s*[:\s,\.-]*(\d{13,15})\b",
            combined_text,
            flags=re.IGNORECASE,
        )
        if ogrn_match:
            ogrn = ogrn_match.group(1)

        kpp_match = re.search(
            r"\bкпп\b\s*[:\s,\.-]*(\d{9})\b", combined_text, flags=re.IGNORECASE
        )
        if kpp_match:
            kpp_list.append(kpp_match.group(1))

        # Разбор ИНН/КПП вида 5025032116/502501001
        slash_match = re.search(r"\b\d{10}/(\d{9})\b", combined_text)
        if slash_match:
            kpp_val = slash_match.group(1)
            if kpp_val not in kpp_list:
                kpp_list.append(kpp_val)

        # Извлечение юридического лица
        legal_match = re.search(
            r"\b(?:ООО|ИП|АО|ЗАО|ОАО|АНО|ЧОП|ЧОО)\s+([А-Яа-яЁё\w\s«\"\'„“”\-\.\u201c\u201d]{3,80})",
            combined_text,
            flags=re.IGNORECASE,
        )
        if legal_match:
            legal_name = legal_match.group(0).strip()

        website = websites[0] if websites else None

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
