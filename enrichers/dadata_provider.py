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
from datetime import date, datetime
from pathlib import Path

import httpx

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
    """Первичный провайдер обогащения через DaData Suggestions API."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        daily_limit: int = 10_000,
        cache_dir: Path | None = None,
        cache_ttl_days: int | None = None,
    ) -> None:
        """
        Args:
            api_key:        Token для заголовка Authorization.
            secret_key:     Секретный ключ (X-Secret).
            daily_limit:    Максимальное число запросов в сутки.
            cache_dir:      Директория файлового кэша. None — кэш отключён.
            cache_ttl_days: TTL кэша в днях. None → берётся из конфига.
        """
        import config as _config

        self._api_key = api_key.strip()
        self._secret_key = secret_key.strip()
        self._daily_limit = daily_limit
        self._daily_count: int = 0
        self._day_marker: date = date.today()
        self._limit_exhausted = False

        self._client = httpx.AsyncClient(
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Token {self._api_key}",
                "X-Secret": self._secret_key,
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
        )

        # Инициализация кэша (из BaseEnricher)
        self._init_cache(
            cache_dir=cache_dir,
            ttl_days=(
                cache_ttl_days
                if cache_ttl_days is not None
                else _config.ENRICHMENT_CACHE_TTL_DAYS
            ),
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
        """Запрашивает расширенную карточку юрлица по ИНН через DaData."""
        try:
            response = await self._client.post(
                _SUGGESTIONS_URL,
                json={"query": inn, "count": 1},
            )
        except httpx.HTTPError as exc:
            logger.warning(f"DaData: Сетевой сбой при запросе ИНН={inn}: {exc}")
            raise

        if response.status_code in (402, 429):
            self._limit_exhausted = True
            logger.warning(f"DaData: Превышен лимит запросов ({response.status_code}).")
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
        party_data = suggestion.get("data", {}) or {}
        state = party_data.get("state", {}) or {}
        name_block = party_data.get("name", {}) or {}
        management = party_data.get("management", {}) or {}
        address_block = party_data.get("address", {}) or {}

        # Получаем численность персонала и официального руководителя
        director_official = management.get("name")
        address_official = address_block.get("value")

        # Парсим дату ликвидации (DaData отдает Unix-таймстамп в миллисекундах)
        liq_timestamp_ms = state.get("liquidation_date")
        liquidation_date_str = None
        if liq_timestamp_ms:
            try:
                liq_dt = datetime.fromtimestamp(liq_timestamp_ms / 1000.0)
                liquidation_date_str = liq_dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        raw_status = state.get("status", "")
        status_official = _DADATA_STATUS_MAP.get(raw_status, OfficialStatus.UNKNOWN)

        inn_verified = party_data.get("inn")
        legal_name = name_block.get("full_with_opf") or name_block.get("short_with_opf")

        return EnrichmentResult(
            inn=inn,
            status_official=status_official,
            inn_verified=inn_verified,
            legal_name_official=legal_name,
            liquidation_date=liquidation_date_str,
            director_official=director_official,
            address_official=address_official,
            provider_name=self.provider_name,
        )

    async def close(self) -> None:
        await self._client.aclose()
        logger.debug(
            f"DaData: Сессия закрыта. " f"Запросов за сессию: {self._daily_count}."
        )
