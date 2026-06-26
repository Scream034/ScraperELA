"""
ScraperELA · main.py
=====================
Главная точка входа.

v4 — Единая БД:
  • COMMON_DB_NAME — одна БД на все источники.
  • site_key передаётся в AsyncCrawler → в БД → в company_sources.
  • Компании с разных сайтов объединяются по ИНН автоматически.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import config
from database import DatabaseManager
from enrichers import DadataEnricher, EnrichmentChain, FnsEgrulEnricher
from exporter import export_sqlite_to_xlsx
from fetchers import AsyncHttpxFetcher
from crawler import AsyncCrawler

from parsers.chop_moscow import ChopMoscowParser
from parsers.prochop_ru import ProchopRuParser

PARSERS_MAP = {
    "chop_moscow": ChopMoscowParser,
    "prochop_ru": ProchopRuParser,
}


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------


def setup_logging(log_file_path: Path) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in (
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path, mode="w", encoding="utf-8"),
    ):
        handler.setFormatter(fmt)
        root.addHandler(handler)


# ---------------------------------------------------------------------------
# Фаза обогащения
# ---------------------------------------------------------------------------


async def run_enrichment_phase(db: DatabaseManager) -> None:
    logger = logging.getLogger("Enrichment")

    providers = []
    if config.DADATA_API_KEY:
        providers.append(
            DadataEnricher(
                api_key=config.DADATA_API_KEY,
                secret_key=config.DADATA_SECRET_KEY,
                daily_limit=config.DADATA_DAILY_LIMIT,
            )
        )
        logger.info("DaData добавлен в цепочку.")
    else:
        logger.info("DADATA_API_KEY не задан — DaData пропущен.")

    providers.append(FnsEgrulEnricher())
    chain = EnrichmentChain(providers)

    try:
        records = await db.get_companies_for_enrichment(
            limit=config.ENRICHMENT_BATCH_SIZE,
            older_than_days=config.ENRICHMENT_RECHECK_DAYS,
        )

        if not records:
            logger.info("Нет компаний для обогащения.")
            return

        logger.info(f"Обогащение: {len(records)} компаний.")

        # enrich_batch ожидает list[dict] с ключами 'inn' и 'source_url'
        # У нас теперь company_id — адаптируем
        inn_to_cid: dict[str, int] = {
            r["inn"]: int(r["company_id"]) for r in records if r.get("inn")
        }

        # Строим псевдо-records для batch (используем inn как url-филлер)
        batch = [{"inn": inn, "source_url": inn} for inn in inn_to_cid]
        results = await chain.enrich_batch(batch, inn_key="inn", url_key="source_url")

        saved = 0
        for inn, result in results.items():
            cid = inn_to_cid.get(inn)
            if cid:
                await db.update_official_status(cid, result)
                saved += 1

        logger.info(f"Обогащение завершено. Обновлено: {saved}/{len(records)}.")

    finally:
        await chain.close()


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------


async def main() -> None:
    site_key = config.ACTIVE_SITE
    site_config = config.SITES.get(site_key)

    if not site_config:
        print(f"Ошибка: сайт '{site_key}' не зарегистрирован в config.SITES.")
        sys.exit(1)

    # --- Пути ---------------------------------------------------------------
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Единая БД для всех источников
    db_file    = data_dir / config.COMMON_DB_NAME
    stats_file = data_dir / f"{config.EXPORT_NAME}_stats.json"   # stats тоже обновляем
    cache_dir  = data_dir / f"cache_{site_key}"
    log_file   = data_dir / f"{site_key}.log"

    setup_logging(log_file)
    logger = logging.getLogger("ScraperELA")

    logger.info("=" * 60)
    logger.info(f"  ScraperELA · {site_config['name']}")
    logger.info(f"  БД: {db_file.name}  (единая для всех источников)")
    logger.info("=" * 60)

    # --- Парсер -------------------------------------------------------------
    parser_class = PARSERS_MAP.get(site_config["parser_key"])
    if not parser_class:
        logger.critical(
            f"Парсер '{site_config['parser_key']}' не зарегистрирован в PARSERS_MAP."
        )
        return

    # --- Компоненты ---------------------------------------------------------
    db = DatabaseManager(db_file)

    fetcher = AsyncHttpxFetcher(
        concurrency_limit=site_config["detail_concurrency"],
        retries=config.NETWORK_RETRIES,
        backoff_factor=config.BACKOFF_FACTOR,
        cache_dir=cache_dir,
    )

    parser = parser_class()

    crawler = AsyncCrawler(
        db=db,
        fetcher=fetcher,
        parser=parser,
        site_key=site_key,
        concurrency=site_config["detail_concurrency"],
        request_delay=config.REQUEST_DELAY,
        stats_file_path=stats_file,
    )

    # --- Основной цикл ------------------------------------------------------
    try:
        await db.connect()
        logger.info(f"SQLite подключён: {db_file.name}")

        if config.RUN_CATALOG_SCAN:
            logger.info("▶  Фаза 1: Сканирование каталога")
            await crawler.scan_catalog(
                catalog_base_url=site_config["base_url"],
                max_pages=site_config["max_pages"],
                catalog_concurrency=site_config["catalog_concurrency"],
            )
        else:
            logger.info("⏭  Фаза 1: Пропущена")

        if config.RUN_DETAIL_PARSER:
            logger.info("▶  Фаза 2: Парсинг карточек")
            await crawler.process_queue()
        else:
            logger.info("⏭  Фаза 2: Пропущена")

        if config.RUN_ENRICHMENT:
            logger.info("▶  Фаза 3: Обогащение по ИНН")
            await run_enrichment_phase(db)
        else:
            logger.info("⏭  Фаза 3: Пропущена (RUN_ENRICHMENT = False)")

        if config.RUN_EXPORTER:
            logger.info("▶  Фаза 4: Экспорт")
            if config.EXPORT_TO_XLSX:
                # Имя файла с меткой времени генерируется в момент экспорта
                from datetime import datetime as _dt

                timestamp = _dt.now().strftime("%Y-%m-%d_%H-%M-%S")
                xlsx_file = data_dir / f"{config.EXPORT_NAME}_{timestamp}.xlsx"

                export_sqlite_to_xlsx(db_path=db_file, xlsx_path=xlsx_file)
                logger.info(f"   XLSX: {xlsx_file.name}")
        else:
            logger.info("⏭  Фаза 4: Пропущена")

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем. Очередь сохранена в БД.")

    except Exception as exc:
        logger.critical(f"Критическая ошибка: {exc}", exc_info=True)

    finally:
        await fetcher.close()
        await db.close()
        logger.info("ScraperELA завершил работу.")


if __name__ == "__main__":
    asyncio.run(main())
