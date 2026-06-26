"""
ScraperELA · enrichers/fns_provider.py
========================================
Резервный провайдер обогащения через открытый API ФНС ЕГРЮЛ.

Эндпоинт: https://egrul.nalog.ru/
Ключ API не требуется. Является публичным сервисом.

Ограничения:
  • Отсутствие официального SLA — возможна нестабильность.
  • Rate limiting на стороне ФНС: рекомендуется REQUEST_DELAY >= 2 сек.
  • Ответ содержит меньше полей, чем DaData (нет детального state.status).
    Статус определяется косвенно по полю «СтатусЗапись» / признаку ликвидации.

Место в цепочке: резервный (после DadataEnricher).
Активируется автоматически при исчерпании лимита DaData.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

import config
from enrichers.base import BaseEnricher
from models import EnrichmentResult, OfficialStatus

logger = logging.getLogger("FnsEgrulEnricher")

# Базовый URL публичного сервиса ФНС
_FNS_SEARCH_URL = "https://egrul.nalog.ru/search-by-requisites.json"
_FNS_TOKEN_URL = "https://egrul.nalog.ru/"

# Маппинг текстовых статусов ФНС → внутренние константы
_FNS_STATUS_MAP: dict[str, str] = {
    "действующее": OfficialStatus.ACTIVE,
    "в стадии ликвидации": OfficialStatus.LIQUIDATING,
    "ликвидировано": OfficialStatus.LIQUIDATED,
    "в стадии реорганизации": OfficialStatus.REORGANIZING,
    "признано банкротом": OfficialStatus.BANKRUPT,
}


class FnsEgrulEnricher(BaseEnricher):
    """
    Резервный провайдер — публичный поиск по ИНН на портале ФНС ЕГРЮЛ.

    Алгоритм:
      1. POST на /search-by-requisites.json с телом {"query": INN}.
      2. Разбираем JSON-ответ: ищем поле статуса организации.
      3. Маппим текстовый статус ФНС → OfficialStatus.
      4. Соблюдаем паузу config.FNS_REQUEST_DELAY между запросами.
    """

    def __init__(
        self,
        request_delay: float | None = None,
    ) -> None:
        """
        Args:
            request_delay: Пауза между запросами в секундах.
                           По умолчанию берётся из config.FNS_REQUEST_DELAY.
        """
        self._request_delay = (
            request_delay if request_delay is not None else config.FNS_REQUEST_DELAY
        )
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": "https://egrul.nalog.ru/",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
            follow_redirects=True,
        )
        self._session_token: str | None = None

    # -----------------------------------------------------------------------
    # BaseEnricher interface
    # -----------------------------------------------------------------------

    @property
    def provider_name(self) -> str:
        return "ФНС ЕГРЮЛ (egrul.nalog.ru)"

    async def is_available(self) -> bool:
        """
        Провайдер не требует ключа и не имеет жёсткого дневного лимита.
        Всегда готов к работе — доступность ограничена только сетью.
        """
        return True

    async def enrich_by_inn(self, inn: str) -> EnrichmentResult | None:
        """
        Выполняет поиск юрлица на egrul.nalog.ru по ИНН.

        Соблюдает задержку между запросами во избежание бана.

        Returns:
            EnrichmentResult при успехе, None если ИНН не найден.

        Raises:
            httpx.HTTPError — при сетевых сбоях.
        """
        await asyncio.sleep(self._request_delay)

        # Шаг 1: Получаем session-токен (cookie-based сессия ФНС)
        if self._session_token is None:
            await self._acquire_session_token()

        # Шаг 2: Поисковый запрос
        try:
            response = await self._client.post(
                _FNS_SEARCH_URL,
                data={
                    "vyp3CaptchaToken": "",
                    "page": "",
                    "query": inn,
                    "region": "",
                    "praOrNko": "",
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(f"ФНС ЕГРЮЛ: Сетевой сбой при запросе ИНН={inn}: {exc}")
            raise

        data = response.json()

        # Ответ ФНС: {"rows": [...], "total": N}
        rows: list[dict] = data.get("rows", [])
        if not rows:
            return None

        # Берём первое совпадение (ИНН уникален)
        record = rows[0]
        return self._parse_fns_record(inn, record)

    # -----------------------------------------------------------------------
    # Вспомогательные методы
    # -----------------------------------------------------------------------

    async def _acquire_session_token(self) -> None:
        """
        Выполняет GET на главную страницу ЕГРЮЛ для инициализации cookie-сессии.
        Без этого шага POST-запросы могут возвращать 403.
        """
        try:
            response = await self._client.get(_FNS_TOKEN_URL)
            response.raise_for_status()
            self._session_token = "acquired"
            logger.debug("ФНС ЕГРЮЛ: Сессия инициализирована.")
        except httpx.HTTPError as exc:
            logger.warning(
                f"ФНС ЕГРЮЛ: Не удалось инициализировать сессию: {exc}. "
                "Попытка запроса без сессии."
            )
            self._session_token = "failed"

    def _parse_fns_record(self, inn: str, record: dict) -> EnrichmentResult | None:
        """
        Разбирает одну запись из ответа ФНС ЕГРЮЛ.

        Поля ответа ФНС (примерная структура публичного API):
          "n"    — полное наименование
          "inn"  — ИНН
          "ogrn" — ОГРН
          "r"    — текстовый статус на русском языке

        Returns:
            EnrichmentResult или None если разобрать запись не удалось.
        """
        raw_status: str = record.get("r", "").strip().lower()

        # Нормализуем статус через маппинг
        status_official = OfficialStatus.UNKNOWN
        for key, value in _FNS_STATUS_MAP.items():
            if key in raw_status:
                status_official = value
                break

        if status_official == OfficialStatus.UNKNOWN:
            logger.debug(
                f"ФНС ЕГРЮЛ: Неизвестный статус «{raw_status}» "
                f"для ИНН={inn}. Записываем как UNKNOWN."
            )

        return EnrichmentResult(
            inn=inn,
            status_official=status_official,
            inn_verified=record.get("inn"),
            legal_name_official=record.get("n"),
            provider_name=self.provider_name,
        )

    async def close(self) -> None:
        await self._client.aclose()
        logger.debug("ФНС ЕГРЮЛ: HTTP-клиент закрыт.")
