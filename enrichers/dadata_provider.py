"""
ScraperELA · enrichers/dadata_provider.py
==========================================
Провайдер обогащения через API DaData (https://dadata.ru).

Эндпоинт: POST https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party
Документация: https://dadata.ru/api/find-party/

Лимиты бесплатного тарифа (актуально на 2026):
  • 10 000 запросов в сутки (сбрасывается в 00:00 МСК).

Особенности реализации:
  • Дневной счётчик запросов с автосбросом при смене календарной даты.
  • is_available() возвращает False при исчерпании лимита или отсутствии ключа.
  • Маппинг статусов DaData → OfficialStatus (модели).
  • При ответе сервера 402 / 429 — лимит помечается исчерпанным немедленно.
"""

from __future__ import annotations

import logging
from datetime import date

import httpx

import config
from enrichers.base import BaseEnricher
from models import EnrichmentResult, OfficialStatus

logger = logging.getLogger("DadataEnricher")


# Маппинг статусов DaData → внутренние константы OfficialStatus
_DADATA_STATUS_MAP: dict[str, str] = {
    "ACTIVE": OfficialStatus.ACTIVE,
    "LIQUIDATING": OfficialStatus.LIQUIDATING,
    "LIQUIDATED": OfficialStatus.LIQUIDATED,
    "BANKRUPT": OfficialStatus.BANKRUPT,
    "REORGANIZING": OfficialStatus.REORGANIZING,
}

_SUGGESTIONS_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"


class DadataEnricher(BaseEnricher):
    """
    Первичный провайдер обогащения.
    Использует DaData Suggestions API для поиска юрлиц по ИНН в базе ЕГРЮЛ.
    """

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        daily_limit: int = 10_000,
    ) -> None:
        """
        Args:
            api_key:     Token для заголовка Authorization.
            secret_key:  Секретный ключ (X-Secret) — нужен для Clean API,
                         передаётся для полноты конфигурации.
            daily_limit: Максимальное число запросов в сутки.
        """
        self._api_key = api_key.strip()
        self._secret_key = secret_key.strip()
        self._daily_limit = daily_limit

        # Дневной счётчик с маркером текущей даты
        self._daily_count: int = 0
        self._day_marker: date = date.today()
        self._limit_exhausted = False  # флаг 402/429 от сервера

        self._client = httpx.AsyncClient(
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Token {self._api_key}",
                "X-Secret": self._secret_key,
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
        )

    # -----------------------------------------------------------------------
    # BaseEnricher interface
    # -----------------------------------------------------------------------

    @property
    def provider_name(self) -> str:
        return "DaData ЕГРЮЛ"

    async def is_available(self) -> bool:
        """
        Проверяет:
          1. Задан ли API-ключ.
          2. Не исчерпан ли дневной лимит (с учётом сброса при смене дня).
          3. Не вернул ли сервер 402/429 в текущей сессии.
        """
        if not self._api_key:
            logger.debug("DaData: API-ключ не задан в config.py.")
            return False

        # Сброс счётчика при смене календарного дня
        today = date.today()
        if today != self._day_marker:
            logger.info(
                f"DaData: Новый день ({today}). "
                f"Сброс счётчика запросов ({self._daily_count} → 0)."
            )
            self._daily_count = 0
            self._day_marker = today
            self._limit_exhausted = False

        if self._limit_exhausted:
            logger.debug(
                f"DaData: Лимит исчерпан (сервер вернул 402/429). "
                "Провайдер недоступен до конца дня."
            )
            return False

        if self._daily_count >= self._daily_limit:
            logger.info(
                f"DaData: Достигнут локальный счётчик ({self._daily_count}/{self._daily_limit}). "
                "Провайдер недоступен до следующего дня."
            )
            return False

        return True

    async def enrich_by_inn(self, inn: str) -> EnrichmentResult | None:
        """
        Запрашивает карточку юрлица по ИНН через DaData Suggestions API.

        Returns:
            EnrichmentResult при успехе, None если ИНН не найден в ЕГРЮЛ.

        Raises:
            httpx.HTTPError — при сетевых сбоях (перехватывается EnrichmentChain).
        """
        try:
            response = await self._client.post(
                _SUGGESTIONS_URL,
                json={"query": inn, "count": 1},
            )
        except httpx.HTTPError as exc:
            logger.warning(f"DaData: Сетевой сбой при запросе ИНН={inn}: {exc}")
            raise

        # Обрабатываем статусы лимитов
        if response.status_code in (402, 429):
            self._limit_exhausted = True
            logger.warning(
                f"DaData: Сервер вернул {response.status_code}. "
                "Лимит запросов исчерпан. Провайдер отключается до следующего дня."
            )
            raise httpx.HTTPStatusError(
                f"DaData limit reached: {response.status_code}",
                request=response.request,
                response=response,
            )

        response.raise_for_status()
        self._daily_count += 1

        data = response.json()
        suggestions: list[dict] = data.get("suggestions", [])

        if not suggestions:
            return None

        suggestion = suggestions[0]
        party_data = suggestion.get("data", {})
        state = party_data.get("state", {})
        name_block = party_data.get("name", {})

        raw_status = state.get("status", "")
        status_official = _DADATA_STATUS_MAP.get(raw_status, OfficialStatus.UNKNOWN)

        inn_verified = party_data.get("inn")
        legal_name = name_block.get("full_with_opf") or name_block.get("short_with_opf")

        return EnrichmentResult(
            inn=inn,
            status_official=status_official,
            inn_verified=inn_verified,
            legal_name_official=legal_name,
            provider_name=self.provider_name,
        )

    async def close(self) -> None:
        await self._client.aclose()
        logger.debug(
            f"DaData: Сессия закрыта. " f"Запросов за сессию: {self._daily_count}."
        )
