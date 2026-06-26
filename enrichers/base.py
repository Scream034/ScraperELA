"""
ScraperELA · enrichers/base.py
================================
Абстрактный контракт для провайдеров обогащения данных с встроенной
файловой кэш-инфраструктурой.

Кэш-слой (опционально):
  • Активируется вызовом _init_cache() в __init__ конкретного провайдера.
  • Хранит EnrichmentResult как JSON-файлы: {cache_dir}/{inn}.json
  • TTL проверяется по mtime файла.
  • Весь файловый I/O неблокирующий (asyncio.to_thread).

Публичный поток вызова:
  chain.py вызывает provider.enrich(inn)
    → _load_from_cache(inn)   (кэш-хит → возврат)
    → enrich_by_inn(inn)      (промах → запрос к API)
    → _save_to_cache(inn, result)

Чтобы добавить новый провайдер:
  1. Наследовать BaseEnricher.
  2. В __init__ вызвать self._init_cache(cache_dir, ttl_days).
  3. Реализовать три абстрактных метода.
  4. Добавить экземпляр в список при сборке EnrichmentChain в main.py.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from models import EnrichmentResult

logger = logging.getLogger("BaseEnricher")


class BaseEnricher(ABC):
    """
    Интерфейс провайдера обогащения юридических лиц по ИНН
    со встроенной файловой кэш-инфраструктурой.

    Жизненный цикл:
        enricher = ConcreteEnricher(cache_dir=Path("cache/fns"), ...)
        if await enricher.is_available():
            result = await enricher.enrich("7707083893")   # ← через кэш
        await enricher.close()
    """

    # -----------------------------------------------------------------------
    # Кэш-инфраструктура
    # -----------------------------------------------------------------------

    def _init_cache(self, cache_dir: Path | None, ttl_days: int) -> None:
        """Инициализирует файловый кэш для провайдера.

        Вызывается из ``__init__`` конкретного провайдера.
        Если ``cache_dir`` равен ``None`` или кэш отключён в конфиге,
        кэш-методы работают как no-op.

        Args:
            cache_dir: Директория для хранения JSON-файлов кэша.
                Рекомендуется использовать провайдеро-специфичный путь,
                например ``data_dir / "cache_enrichers" / "fns"``.
            ttl_days: Срок жизни кэша в днях (0 — бессрочно).
        """
        import config  # локальный import для избежания циклов

        self._cache_dir: Path | None = (
            cache_dir
            if (cache_dir is not None and config.ENRICHMENT_USE_CACHE)
            else None
        )
        self._cache_ttl_days: int = ttl_days

    def _get_cache_path(self, inn: str) -> Path:
        """Возвращает путь к JSON-файлу кэша для данного ИНН.

        Args:
            inn: ИНН организации (используется как имя файла напрямую —
                содержит только цифры, безопасен для файловой системы).

        Returns:
            Путь вида ``{_cache_dir}/{inn}.json``.
        """
        assert self._cache_dir is not None
        return self._cache_dir / f"{inn}.json"

    async def _load_from_cache(self, inn: str) -> EnrichmentResult | None:
        """Загружает результат из файлового кэша, если он не устарел.

        Args:
            inn: ИНН организации.

        Returns:
            ``EnrichmentResult`` из кэша или ``None`` при промахе / устаревании.
        """
        if self._cache_dir is None:
            return None

        path = self._get_cache_path(inn)

        try:
            exists = await asyncio.to_thread(path.exists)
            if not exists:
                return None

            if self._cache_ttl_days > 0:
                mtime: float = await asyncio.to_thread(lambda: path.stat().st_mtime)
                age_days = (datetime.now().timestamp() - mtime) / 86_400
                if age_days > self._cache_ttl_days:
                    logger.debug(
                        f"[{self.provider_name}] Кэш ИНН={inn} устарел "
                        f"({age_days:.1f} / {self._cache_ttl_days} дн.)."
                    )
                    return None

            raw = await asyncio.to_thread(path.read_text, "utf-8")
            result = EnrichmentResult.model_validate_json(raw)
            logger.debug(
                f"[{self.provider_name}] Кэш-хит ИНН={inn} "
                f"(статус: {result.status_official})."
            )
            return result

        except Exception as exc:
            logger.warning(
                f"[{self.provider_name}] Ошибка чтения кэша ИНН={inn}: {exc}. "
                "Выполняем запрос к API."
            )
            return None

    async def _save_to_cache(self, inn: str, result: EnrichmentResult) -> None:
        """Сохраняет результат в файловый кэш.

        Args:
            inn: ИНН организации.
            result: Объект для сериализации в JSON.
        """
        if self._cache_dir is None:
            return

        path = self._get_cache_path(inn)
        try:
            await asyncio.to_thread(self._cache_dir.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(
                path.write_text, result.model_dump_json(indent=2), "utf-8"
            )
            logger.debug(f"[{self.provider_name}] Кэш сохранён: ИНН={inn}.")
        except Exception as exc:
            logger.warning(
                f"[{self.provider_name}] Ошибка записи кэша ИНН={inn}: {exc}."
            )

    # -----------------------------------------------------------------------
    # Публичный кэш-aware метод (вызывается из EnrichmentChain)
    # -----------------------------------------------------------------------

    async def enrich(self, inn: str) -> EnrichmentResult | None:
        """Обогащает юрлицо по ИНН с проверкой кэша.

        Алгоритм:
            1. Проверить кэш → вернуть при хите.
            2. Вызвать ``enrich_by_inn`` → запрос к внешнему API.
            3. Сохранить результат в кэш (если не ``None``).

        Args:
            inn: ИНН организации (10 или 12 цифр).

        Returns:
            ``EnrichmentResult`` при успехе, ``None`` если не найдено.

        Raises:
            Любые сетевые исключения из ``enrich_by_inn`` — перехватываются
            в ``EnrichmentChain`` при итерации по провайдерам.
        """
        cached = await self._load_from_cache(inn)
        if cached is not None:
            return cached

        result = await self.enrich_by_inn(inn)

        if result is not None:
            await self._save_to_cache(inn, result)

        return result

    # -----------------------------------------------------------------------
    # Обязательный интерфейс
    # -----------------------------------------------------------------------

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Уникальное читаемое имя провайдера для логов и БД."""

    @abstractmethod
    async def is_available(self) -> bool:
        """Проверяет готовность провайдера к работе прямо сейчас.

        Должен проверять:
          - Задан ли API-ключ (если требуется).
          - Не исчерпан ли дневной / месячный лимит запросов.

        Метод НЕ должен бросать исключений — только возвращать ``bool``.
        """

    @abstractmethod
    async def enrich_by_inn(self, inn: str) -> EnrichmentResult | None:
        """Выполняет запрос к внешнему реестру и возвращает результат.

        Вызывается только из ``enrich()`` — не напрямую из ``EnrichmentChain``.

        Args:
            inn: ИНН организации (10 или 12 цифр).

        Returns:
            ``EnrichmentResult`` — если данные найдены.
            ``None``             — если ИНН не найден в реестре.

        Raises:
            Любые сетевые исключения — ``EnrichmentChain`` перехватывает их.
        """

    # -----------------------------------------------------------------------
    # Опциональный интерфейс
    # -----------------------------------------------------------------------

    async def close(self) -> None:
        """Освобождает ресурсы (HTTP-клиент и т.д.). Переопределяйте при необходимости."""
