"""
ScraperELA · main.py
=====================
Асинхронный конвейерный оркестратор параллельного сбора данных.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import config
from crawler import AsyncCrawler
from database import DatabaseManager
from enrichers import DadataEnricher, EnrichmentChain, FnsEgrulEnricher
from exporter import export_sqlite_to_xlsx
from fetchers import AsyncHttpxFetcher
from parsers.chop_moscow import ChopMoscowParser
from parsers.prochop_ru import ProchopRuParser
from parsers.vsechopy_ru import VseChopyRuParser

PARSERS_MAP = {
    "chop_moscow": ChopMoscowParser,
    "prochop_ru": ProchopRuParser,
    "vsechopy_ru": VseChopyRuParser,
}


# --- Section: Логирование ---


def setup_logging(log_file_path: Path) -> None:
    """Инициализирует глобальную систему логирования."""
    root = logging.getLogger()
    level_str = getattr(config, "LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    root.setLevel(level)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in (
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path, mode="w", encoding="utf-8"),
    ):
        handler.setLevel(level)
        handler.setFormatter(fmt)
        root.addHandler(handler)


# --- Section: Конвейер отдельного сайта (Site Pipeline) ---


async def process_site_pipeline(
    site_info: dict[str, Any],
    site_sem: asyncio.Semaphore,
) -> None:
    """Полный жизненный цикл сбора данных одного сайта в режиме скользящего окна.

    Устраняет барьер ожидания: как только сайт закончил сканировать каталог,
    он немедленно переходит к парсингу карточек.

    Args:
        site_info: Словарь с компонентами краулера (БД, парсер, фетчер).
        site_sem: Семафор ограничения максимального числа параллельных сайтов.
    """
    site_key: str = site_info["site_key"]
    crawler: AsyncCrawler = site_info["crawler"]
    cfg: dict[str, Any] = site_info["site_config"]
    fetcher: AsyncHttpxFetcher = site_info["fetcher"]
    site_logger = logging.getLogger(f"Pipeline.{site_key}")

    async with site_sem:
        site_logger.info(f"▶ [Окно сайтов] Захват слота. Старт сбора для '{site_key}'")
        try:
            # Шаг 1: Сканирование каталога
            if config.RUN_CATALOG_SCAN:
                site_logger.info(f"[{site_key}] Фаза 1/2: Сканирование каталога")
                await crawler.scan_catalog(
                    catalog_base_url=cfg["base_url"],
                    max_pages=cfg["max_pages"],
                    catalog_concurrency=cfg["catalog_concurrency"],
                )

            # Шаг 2: Парсинг карточек (стартует мгновенно после шага 1)
            if config.RUN_DETAIL_PARSER:
                site_logger.info(f"[{site_key}] Фаза 2/2: Парсинг карточек")
                await crawler.process_queue()

            site_logger.info(
                f"✓ Конвейер '{site_key}' успешно завершен. Слот освобождается."
            )

        except Exception as exc:
            site_logger.error(f"✗ Сбой в конвейере '{site_key}': {exc}", exc_info=True)

        finally:
            # Мгновенно освобождаем сокеты целевого хоста
            await fetcher.close()


# --- Section: Фаза Обогащения ---


async def run_enrichment_phase(db: DatabaseManager, data_dir: Path) -> None:
    """Выполняет пакетное обогащение собранных юрлиц по ИНН.

    Args:
        db:       Подключённый менеджер БД.
        data_dir: Корневая директория данных (для путей кэша).
    """
    logger = logging.getLogger("Enrichment")
    providers = []

    enricher_cache_base = data_dir / "cache_enrichers"

    if config.DADATA_API_KEY:
        providers.append(
            DadataEnricher(
                api_key=config.DADATA_API_KEY,
                secret_key=config.DADATA_SECRET_KEY,
                daily_limit=config.DADATA_DAILY_LIMIT,
                cache_dir=enricher_cache_base / "dadata",
            )
        )
        logger.info("DaData добавлен в цепочку верификации.")
    else:
        logger.info("DADATA_API_KEY не задан — DaData пропущен.")

    providers.append(FnsEgrulEnricher(cache_dir=enricher_cache_base / "fns"))
    chain = EnrichmentChain(providers)

    try:
        records = await db.get_companies_for_enrichment(
            limit=config.ENRICHMENT_BATCH_SIZE,  # 0 = без лимита
            older_than_days=config.ENRICHMENT_RECHECK_DAYS,
            filters=config.ENRICHMENT_CONFIG,
        )
        if not records:
            logger.info("Нет юрлиц, требующих обогащения по заданным фильтрам.")
            return

        limit_info = (
            f"лимит {config.ENRICHMENT_BATCH_SIZE}"
            if config.ENRICHMENT_BATCH_SIZE > 0
            else "без лимита"
        )
        logger.info(
            f"Обогащение: {len(records)} компаний ({limit_info}), "
            f"фильтр: {config.ENRICHMENT_CONFIG}."
        )

        inn_to_cid = {r["inn"]: int(r["company_id"]) for r in records if r.get("inn")}
        batch = [{"inn": inn, "source_url": inn} for inn in inn_to_cid]
        results = await chain.enrich_batch(batch, inn_key="inn", url_key="source_url")

        saved = 0
        for inn, result in results.items():
            if cid := inn_to_cid.get(inn):
                await db.update_official_status(cid, result)
                saved += 1

        logger.info(f"Обогащение завершено. Обновлено в БД: {saved}/{len(records)}.")
    finally:
        await chain.close()


# --- Section: Главный процесс ---


async def main() -> None:
    """Точка входа оркестратора."""
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    db_file = data_dir / config.COMMON_DB_NAME
    log_file = data_dir / "scraperela.log"

    setup_logging(log_file)
    logger = logging.getLogger("ScraperELA")

    logger.info("=" * 60)
    logger.info("  ScraperELA · Оркестратор параллельного сбора")
    logger.info(
        f"  Активные источники ({len(config.ACTIVE_SITES)}): {', '.join(config.ACTIVE_SITES)}"
    )
    logger.info(f"  Лимит одновременных сайтов (окно): {config.MAX_CONCURRENT_SITES}")
    logger.info(f"  Единая БД: {db_file.name}")
    logger.info("=" * 60)

    db = DatabaseManager(db_file)

    # Инициализация изолированных инстансов
    crawlers_info = []
    for site_key in config.ACTIVE_SITES:
        site_config = config.SITES.get(site_key)
        if not site_config:
            logger.error(f"Сайт '{site_key}' не найден в config.SITES.")
            continue

        parser_class = PARSERS_MAP.get(site_config["parser_key"])
        if not parser_class:
            logger.error(f"Парсер '{site_config['parser_key']}' не зарегистрирован.")
            continue

        fetcher = AsyncHttpxFetcher(
            concurrency_limit=site_config["detail_concurrency"],
            retries=config.NETWORK_RETRIES,
            backoff_factor=config.BACKOFF_FACTOR,
            cache_dir=data_dir / f"cache_{site_key}",
        )

        crawler = AsyncCrawler(
            db=db,
            fetcher=fetcher,
            parser=parser_class(),
            site_key=site_key,
            concurrency=site_config["detail_concurrency"],
            request_delay=config.REQUEST_DELAY,
            stats_file_path=data_dir / f"{site_key}_stats.json",
            page_pattern=site_config.get("page_pattern"),
        )

        crawlers_info.append(
            {
                "site_key": site_key,
                "site_config": site_config,
                "fetcher": fetcher,
                "crawler": crawler,
            }
        )

    if not crawlers_info:
        logger.critical("Нет корректных источников для запуска. Выход.")
        return

    # Семафор скользящего окна сайтов
    site_sem = asyncio.Semaphore(config.MAX_CONCURRENT_SITES)

    try:
        await db.connect()
        logger.info(f"Подключение к SQLite установлено: {db_file.name}")

        # Запуск асинхронных конвейеров сайтов
        if config.RUN_CATALOG_SCAN or config.RUN_DETAIL_PARSER:
            logger.info("▶ Запуск асинхронных конвейеров сайтов...")
            try:
                async with asyncio.TaskGroup() as tg:
                    for item in crawlers_info:
                        tg.create_task(process_site_pipeline(item, site_sem))
            except* Exception as eg:
                for exc in eg.exceptions:
                    logger.error(f"Сбой внутри группы конвейеров: {exc}", exc_info=exc)

            # Дедупликация запускается ПОСЛЕ краулеров — всегда, не за флагом.
            # Причина: краулеры могут создавать дубли (level-4 не срабатывает
            # если у существующей записи ещё нет телефонов в момент INSERT).
            logger.info("▶ Фаза 2.5: Дедупликация после парсинга")
            dedup_stats = await db.run_dedup_pass()
            logger.info(
                f"✓ Дедупликация: website={dedup_stats['website']}, "
                f"имя+телефон={dedup_stats['name_phone']}."
            )
        else:
            logger.info("⏭ Парсинг пропущен (обе фазы отключены в конфиге)")

        if config.RUN_ENRICHMENT:
            logger.info("▶ Фаза 3: Обогащение по ИНН")
            await run_enrichment_phase(db, data_dir)

        if config.RUN_EXPORTER:
            logger.info("▶ Фаза 4: Генерация Excel-отчета")
            if config.EXPORT_TO_XLSX:
                timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
                xlsx_file = data_dir / f"{config.EXPORT_NAME}_{timestamp}.xlsx"

                export_sqlite_to_xlsx(db_path=db_file, xlsx_path=xlsx_file)
                logger.info(f"✓ Отчет успешно сформирован: {xlsx_file.name}")

    except KeyboardInterrupt:
        logger.warning(
            "Сессия прервана пользователем. Состояние очередей сохранено в БД."
        )

    except Exception as exc:
        logger.critical(f"Критический сбой оркестратора: {exc}", exc_info=True)

    finally:
        for item in crawlers_info:
            if not item["fetcher"].client.is_closed:
                await item["fetcher"].close()
        await db.close()
        logger.info("Работа ScraperELA завершена.")


if __name__ == "__main__":
    asyncio.run(main())
