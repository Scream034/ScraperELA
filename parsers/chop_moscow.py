"""
Парсер для сайта chop.moscow с извлечением статуса работы организации.
"""

import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from parsers.base import BaseParser
from models import CompanySchema


class ChopMoscowParser(BaseParser):
    """Стратегия парсинга ресурса https://chop.moscow."""

    BASE_URL = "https://chop.moscow"

    def parse_listing(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            match = re.search(r"^/catalog/([^/]+)/?$", path)
            if match:
                slug = match.group(1)
                if slug not in {"page", "add", "search", "category", "filter"}:
                    full_url = urljoin(self.BASE_URL, f"/catalog/{slug}/")
                    urls.append(full_url)

        return list(set(urls))

    def parse_detail(self, html: str, url: str) -> CompanySchema | None:
        soup = BeautifulSoup(html, "lxml")
        text_content = soup.get_text()

        # 1. Извлечение названия
        h1_tag = soup.find("h1")
        name = h1_tag.get_text(strip=True) if h1_tag else None
        if name:
            name = re.split(r":\s*", name, flags=re.IGNORECASE)[0].strip()

        # 2. Определение статуса работы организации
        status = "Работает"
        if soup.find(class_="cloze"):
            status = "Компания больше не работает"
        elif soup.find(class_="cloze-perhaps"):
            status = "Компания, возможно, не работает"

        # 3. Реквизиты
        inn_match = re.search(r"ИНН\s*(\d{10,12})", text_content)
        ogrn_match = re.search(r"ОГРН\s*(\d{13,15})", text_content)
        kpp_match = re.search(r"КПП\s*(\d{9})", text_content)
        reg_date_match = re.search(
            r"Дата регистрации\s*(\d{2}\.\d{2}\.\d{4})", text_content
        )

        # 4. Юридическое название
        legal_name_match = re.search(r"Юридическое название:\s*([^.\n]+)", text_content)
        legal_name = (
            legal_name_match.group(1).replace('"', "").strip()
            if legal_name_match
            else None
        )

        # 5. ФИО руководителя
        director_match = re.search(r"Руководитель\s*([^.\n]+)", text_content)
        director = director_match.group(1).strip() if director_match else None

        # 6. Адрес
        address_span = soup.find(class_="adresch2")
        address = address_span.get_text(strip=True) if address_span else None

        # 7. Телефоны (Основной + Дополнительные)
        phones_list: list[str] = []

        tel_span = soup.find(class_="telch")
        if tel_span:
            phones_list.append(tel_span.get_text(strip=True))

        for p_tag in soup.find_all("p", class_="dop-info-p"):
            p_text = p_tag.get_text(strip=True)
            if "Доп. телефоны:" in p_text:
                dop_part = p_text.replace("Доп. телефоны:", "").strip()
                if dop_part:
                    phones_list.append(dop_part)
                break

        phones_str = "; ".join(phones_list) if phones_list else None

        # 8. Email
        email_span = soup.find(class_="emailchop")
        email = email_span.get_text(strip=True) if email_span else None

        # 9. Сайт
        website = None
        site_a = soup.find("a", class_="sitech")
        if site_a:
            href = site_a.get("href", "")
            if isinstance(href, str):
                if "url=" in href:
                    match = re.search(r"url=([^&/]+)", href)
                    website = match.group(1) if match else site_a.get_text(strip=True)
                else:
                    website = (
                        href if href.startswith("http") else site_a.get_text(strip=True)
                    )

        return CompanySchema(
            source_url=url,
            name=name,
            legal_name=legal_name,
            status=status,  # Передаем статус
            inn=inn_match.group(1) if inn_match else None,
            ogrn=ogrn_match.group(1) if ogrn_match else None,
            kpp=kpp_match.group(1) if kpp_match else None,
            registration_date=reg_date_match.group(1) if reg_date_match else None,
            director=director,
            address=address,
            phones=phones_str,
            email=email,
            website=website,
        )
