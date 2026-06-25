"""
Схемы данных проекта с полем статуса активности.
"""

from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field, ConfigDict, field_validator


class CompanySchema(BaseModel):
    """Схема валидации данных организации (ЧОП)."""

    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    source_url: str = Field(..., description="URL детальной страницы организации")
    name: str | None = Field(None, description="Краткое название организации")
    legal_name: str | None = Field(None, description="Полное юридическое название")
    status: str = Field(
        "Работает", description="Статус активности (Работает/Закрыт/Возможно закрыт)"
    )
    inn: str | None = Field(None, description="ИНН организации")
    ogrn: str | None = Field(None, description="ОГРН организации")
    kpp: str | None = Field(None, description="КПП организации")
    registration_date: str | None = Field(None, description="Дата регистрации")
    director: str | None = Field(None, description="ФИО руководителя")
    address: str | None = Field(None, description="Физический/юридический адрес")
    phones: str | None = Field(None, description="Контактные телефоны")
    email: str | None = Field(None, description="Электронная почта")
    website: str | None = Field(None, description="Сайт организации")
    scrape_date: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d"),
        description="Дата сбора информации",
    )

    @field_validator("inn", "ogrn", "kpp", mode="before")
    @classmethod
    def clean_numeric_fields(cls, value: Any) -> str | None:
        if value is None:
            return None
        cleaned = "".join(filter(str.isdigit, str(value)))
        return cleaned if cleaned else None
