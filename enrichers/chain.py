"""
ScraperELA · enrichers/chain.py
=================================
Цепочка ответственности (Chain of Responsibility) для провайдеров обогащения.

Алгоритм обхода цепочки для каждого ИНН:
  1. Берём провайдера в порядке приоритета (порядок списка при инициализации).
  2. Проверяем is_available().
     → False: логируем INFO, переходим к следующему.
  3. Вызываем enrich_by_inn(inn).
     → EnrichmentResult: возвращаем результат, цепочка останавливается.
     → None: данные не найдены, переходим к следующему.
     → Exception: логируем WARNING с трейсом, переходим к следующему.
  4. Если все провайдеры исчерпаны → логируем WARNING, возвращаем None.

Система НИКОГДА не падает с исключением наружу — только логирует.
"""

from __future__ import annotations

import logging

from enrichers.base import BaseEnricher
from models import EnrichmentResult

logger = logging.getLogger("EnrichmentChain")


class EnrichmentChain:
    """
    Управляет цепочкой провайдеров обогащения.

    Пример использования:
        chain = EnrichmentChain([
            DadataEnricher(api_key=..., secret_key=...),
            FnsEgrulEnricher(),
        ])
        result = await chain.enrich("7707083893")
        await chain.close()
    """

    def __init__(self, providers: list[BaseEnricher]) -> None:
        if not providers:
            raise ValueError(
                "EnrichmentChain требует хотя бы одного провайдера. "
                "Передан пустой список."
            )
        self._providers = providers
        logger.info(
            f"Цепочка обогащения инициализирована. "
            f"Провайдеры ({len(providers)}): "
            f"{' → '.join(p.provider_name for p in providers)}"
        )

    # -----------------------------------------------------------------------
    # Публичный API
    # -----------------------------------------------------------------------

    async def enrich(self, inn: str) -> EnrichmentResult | None:
        """
        Запрашивает статус юрлица по ИНН, обходя провайдеров по приоритету.

        Args:
            inn: ИНН организации (10 или 12 цифр).

        Returns:
            EnrichmentResult от первого успешного провайдера или None.
        """
        for provider in self._providers:
            provider_tag = f"[{provider.provider_name}]"

            # --- Шаг 1: проверяем доступность ------------------------------------
            try:
                available = await provider.is_available()
            except Exception as exc:
                logger.warning(
                    f"{provider_tag} Ошибка при проверке доступности, "
                    f"провайдер пропущен. Причина: {exc}"
                )
                continue

            if not available:
                logger.info(
                    f"{provider_tag} Провайдер недоступен "
                    f"(лимит исчерпан или ключ не задан). Переход к следующему."
                )
                continue

            # --- Шаг 2: запрашиваем данные --------------------------------------
            try:
                result = await provider.enrich_by_inn(inn)
            except Exception as exc:
                logger.warning(
                    f"{provider_tag} Ошибка при обогащении ИНН={inn}. "
                    f"Переход к следующему провайдеру. Причина: {exc}",
                    exc_info=True,
                )
                continue

            if result is not None:
                logger.info(
                    f"{provider_tag} ИНН={inn} → {result.status_official} "
                    f"({result.legal_name_official or 'название не получено'})"
                )
                return result

            logger.info(
                f"{provider_tag} ИНН={inn} не найден в реестре. "
                "Переход к следующему провайдеру."
            )

        # --- Все провайдеры исчерпаны -------------------------------------------
        logger.warning(
            f"Все провайдеры исчерпаны для ИНН={inn}. " "Обогащение не выполнено."
        )
        return None

    async def enrich_batch(
        self,
        records: list[dict],
        inn_key: str = "inn",
        url_key: str = "source_url",
    ) -> dict[str, EnrichmentResult]:
        """
        Обогащает пакет записей последовательно.

        Args:
            records:  Список dict с ключами inn_key и url_key.
            inn_key:  Имя ключа ИНН в каждом dict.
            url_key:  Имя ключа source_url для логирования.

        Returns:
            Словарь {source_url: EnrichmentResult} для успешно обогащённых.
        """
        results: dict[str, EnrichmentResult] = {}
        total = len(records)

        for idx, record in enumerate(records, 1):
            inn: str | None = record.get(inn_key)
            url: str = record.get(url_key, "unknown")

            if not inn:
                logger.debug(f"[{idx}/{total}] Нет ИНН для {url}, пропускаем.")
                continue

            logger.debug(f"[{idx}/{total}] Обогащение: {url} (ИНН={inn})")
            result = await self.enrich(inn)

            if result is not None:
                results[url] = result

        logger.info(
            f"Пакетное обогащение завершено. " f"Успешно: {len(results)}/{total}."
        )
        return results

    async def close(self) -> None:
        """Освобождает ресурсы всех провайдеров в цепочке."""
        for provider in self._providers:
            try:
                await provider.close()
                logger.debug(f"[{provider.provider_name}] Ресурсы освобождены.")
            except Exception as exc:
                logger.warning(f"[{provider.provider_name}] Ошибка при закрытии: {exc}")
