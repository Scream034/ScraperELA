"""
ScraperELA · models.py
======================
Схемы валидации данных (Pydantic v2).

Архитектура полей CompanySchema:
  • Иммутабельные    — inn, ogrn, registration_date
  • Свежие мутабельные — name, legal_name, director, address, website
  • Аккумуляторы     — phones, emails, kpp_list (отдельные таблицы БД)
  • Всегда новые     — status_aggregator, scrape_date, last_seen_at
  • Только enricher  — status_official, inn_verified, legal_name_official,
                       official_verified_at, provider_name

Совместимость с Pyright/Pylance (без pydantic-плагина):
  Все необязательные поля используют паттерн
      field: Annotated[T, Field(description=...)] = default
  вместо
      field: T = Field(default, description=...)
  Это единственный способ дать Pyright понять что поле имеет дефолт
  и не является обязательным аргументом конструктора.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Константы статусов
# ---------------------------------------------------------------------------


class AggregatorStatus:
    """Статусы с сайта-источника (агрегатора)."""

    ACTIVE = "Работает"
    CLOSED = "Компания больше не работает"
    MAYBE = "Компания, возможно, не работает"


class OfficialStatus:
    """Официальные статусы из ЕГРЮЛ / ФНС."""

    ACTIVE = "ACTIVE"
    LIQUIDATED = "LIQUIDATED"
    LIQUIDATING = "LIQUIDATING"
    BANKRUPT = "BANKRUPT"
    REORGANIZING = "REORGANIZING"
    UNKNOWN = "UNKNOWN"


# ---------------------------------------------------------------------------
# CompanySchema
# ---------------------------------------------------------------------------


class CompanySchema(BaseModel):
    """
    Единая схема организации, проходящая через весь конвейер:
    парсер → БД → обогатитель → экспортёр.

    Список-поля (phones, emails, kpp_list) при сохранении в БД
    распределяются по таблицам company_contacts / company_kpp
    и существуют здесь только как Python-списки.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    # --- Идентификатор источника -------------------------------------------
    source_url: str = Field(..., description="URL детальной страницы")

    # --- Иммутабельные реквизиты --------------------------------------------
    # Annotated + правосторонний дефолт — единственный Pyright-совместимый
    # способ сочетать Field(description=) с необязательным аргументом.
    inn: Annotated[str | None, Field(description="ИНН (10 или 12 цифр)")] = None
    ogrn: Annotated[str | None, Field(description="ОГРН (13 или 15 цифр)")] = None
    registration_date: Annotated[
        str | None, Field(description="Дата регистрации ДД.ММ.ГГГГ")
    ] = None

    # --- Мутабельные «свежие» поля ------------------------------------------
    name: Annotated[str | None, Field(description="Краткое название")] = None
    legal_name: Annotated[
        str | None, Field(description="Полное юридическое название")
    ] = None
    director: Annotated[str | None, Field(description="ФИО руководителя")] = None
    address: Annotated[str | None, Field(description="Адрес организации")] = None
    website: Annotated[str | None, Field(description="Сайт организации")] = None

    # --- Аккумуляторы (несколько значений) ----------------------------------
    phones: Annotated[
        list[str],
        Field(description="Список телефонов; дедупликация на уровне БД"),
    ] = Field(default_factory=list)

    emails: Annotated[
        list[str],
        Field(description="Список email-адресов; дедупликация на уровне БД"),
    ] = Field(default_factory=list)

    kpp_list: Annotated[
        list[str],
        Field(description="Список КПП (головной + обособленные подразделения)"),
    ] = Field(default_factory=list)

    # --- Статус с сайта-источника -------------------------------------------
    status_aggregator: Annotated[
        str,
        Field(description="Статус по данным агрегатора"),
    ] = AggregatorStatus.ACTIVE

    # --- Временны́е метки -----------------------------------------------------
    scrape_date: Annotated[
        str,
        Field(description="Дата парсинга (YYYY-MM-DD)"),
    ] = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    first_seen_at: Annotated[
        str | None,
        Field(description="Дата первого обнаружения; устанавливается БД при INSERT"),
    ] = None

    last_seen_at: Annotated[
        str | None,
        Field(description="Дата последнего успешного парсинга"),
    ] = None

    # --- Поля обогатителя (заполняет только EnrichmentChain) ---------------
    status_official: Annotated[
        str | None,
        Field(description="Статус из ЕГРЮЛ (ACTIVE / LIQUIDATED / …)"),
    ] = None

    inn_verified: Annotated[
        str | None,
        Field(description="ИНН, подтверждённый официальным API"),
    ] = None

    legal_name_official: Annotated[
        str | None,
        Field(description="Официальное наименование из ЕГРЮЛ"),
    ] = None

    official_verified_at: Annotated[
        str | None,
        Field(description="ISO-таймстамп последней проверки по реестру"),
    ] = None

    provider_name: Annotated[
        str | None,
        Field(description="Провайдер верификации (DaData / nalog.ru / …)"),
    ] = None

    # -----------------------------------------------------------------------
    # Валидаторы
    # -----------------------------------------------------------------------

    @field_validator("inn", "ogrn", mode="before")
    @classmethod
    def _clean_numeric(cls, value: Any) -> str | None:
        """Оставляет только цифры; возвращает None для пустых строк."""
        if value is None:
            return None
        cleaned = "".join(filter(str.isdigit, str(value)))
        return cleaned or None

    @field_validator("inn", mode="after")
    @classmethod
    def _validate_inn_length(cls, value: str | None) -> str | None:
        """ИНН: 10 цифр (юрлицо) или 12 (ИП). Невалидный — сбрасываем молча."""
        if value is not None and len(value) not in (10, 12):
            return None
        return value

    @field_validator("ogrn", mode="after")
    @classmethod
    def _validate_ogrn_length(cls, value: str | None) -> str | None:
        """ОГРН: 13 цифр (юрлицо) или 15 (ИП)."""
        if value is not None and len(value) not in (13, 15):
            return None
        return value

    @field_validator("phones", "emails", "kpp_list", mode="before")
    @classmethod
    def _ensure_list(cls, value: Any) -> list[Any]:
        """Допускает передачу одиночной строки вместо списка."""
        if isinstance(value, str):
            return [value] if value.strip() else []
        if value is None:
            return []
        return list(value)

    @field_validator("phones", mode="after")
    @classmethod
    def _deduplicate_phones(cls, value: list[str]) -> list[str]:
        seen: dict[str, None] = {}
        for v in value:
            stripped = v.strip()
            if stripped:
                seen[stripped] = None
        return list(seen.keys())

    @field_validator("emails", mode="after")
    @classmethod
    def _deduplicate_emails(cls, value: list[str]) -> list[str]:
        seen: dict[str, None] = {}
        for v in value:
            normalized = v.strip().lower()
            if normalized:
                seen[normalized] = None
        return list(seen.keys())

    @field_validator("kpp_list", mode="before")
    @classmethod
    def _clean_kpp_list(cls, value: Any) -> list[str]:
        """Очищает каждый КПП до 9 цифр с дедупликацией."""
        raw: list[Any] = (
            value if isinstance(value, list) else ([value] if value else [])
        )
        result: list[str] = []
        seen: set[str] = set()
        for item in raw:
            cleaned = "".join(filter(str.isdigit, str(item)))
            if len(cleaned) == 9 and cleaned not in seen:
                result.append(cleaned)
                seen.add(cleaned)
        return result

    @model_validator(mode="after")
    def _set_last_seen_at(self) -> "CompanySchema":
        """Автоматически проставляет last_seen_at = scrape_date если не задан."""
        if self.last_seen_at is None:
            self.last_seen_at = self.scrape_date
        return self


# ---------------------------------------------------------------------------
# EnrichmentResult
# ---------------------------------------------------------------------------


class EnrichmentResult(BaseModel):
    """Результат верификации юрлица через внешний API-провайдер."""

    model_config = ConfigDict(str_strip_whitespace=True)

    inn: str = Field(..., description="ИНН, по которому был запрос")
    status_official: str = Field(
        ..., description="Нормализованный статус (OfficialStatus.*)"
    )
    inn_verified: Annotated[str | None, Field(description="ИНН из ответа реестра")] = (
        None
    )
    legal_name_official: Annotated[
        str | None, Field(description="Официальное наименование из реестра")
    ] = None
    verified_at: Annotated[
        str,
        Field(description="ISO-таймстамп момента запроса к провайдеру"),
    ] = Field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    provider_name: str = Field(..., description="Идентификатор провайдера")

    @field_validator("inn_verified", mode="before")
    @classmethod
    def _clean_inn(cls, value: Any) -> str | None:
        if value is None:
            return None
        cleaned = "".join(filter(str.isdigit, str(value)))
        return cleaned or None
