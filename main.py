"""
Главный запускаемый файл ScraperELA с интеграцией подсистемы кэширования.
"""

import asyncio
import logging
import sys
from pathlib import Path

import config
from database import DatabaseManager
from fetchers import AsyncHttpxFetcher
from parsers.chop_moscow import ChopMoscowParser
from crawler import AsyncCrawler
from exporter import export_sqlite_to_csv, export_sqlite_to_xlsx

PARSERS_MAP = {
    "chop_moscow": ChopMoscowParser,
}


def setup_logging(log_file_path: Path) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


async def main() -> None:
    site_key = config.ACTIVE_SITE
    site_config = config.SITES.get(site_key)

    if not site_config:
        print(
            f"Критическая ошибка: Сайт '{site_key}' не зарегистрирован в config.py в SITES!"
        )
        sys.exit(1)

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Инициализация путей для конкретного сайта
    db_file = data_dir / site_config["db_name"]
    csv_file = data_dir / f"{site_config['export_name']}.csv"
    xlsx_file = data_dir / f"{site_config['export_name']}.xlsx"
    stats_file = data_dir / f"{site_config['export_name']}_stats.json"

    # Индивидуальная директория кэша для сайта
    cache_dir = data_dir / f"cache_{site_key}"
    log_file = data_dir / "scraper.log"

    setup_logging(log_file)
    logger = logging.getLogger("ScraperELA")

    logger.info(f"=== Запуск ScraperELA для сайта: {site_config['name']} ===")

    parser_class = PARSERS_MAP.get(site_config["parser_key"])
    if not parser_class:
        logger.critical(
            f"Класс парсера для ключа '{site_config['parser_key']}' не зарегистрирован в main.py!"
        )
        return

    db = DatabaseManager(db_file)

    # Сетевой клиент инициализируется с указанием директории кэша
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
        concurrency=site_config["detail_concurrency"],
        request_delay=config.REQUEST_DELAY,
        stats_file_path=stats_file,
    )

    try:
        await db.connect()
        logger.info("Успешное подключение к SQLite.")

        # Сканирование каталога
        if config.RUN_CATALOG_SCAN:
            await crawler.scan_catalog(
                catalog_base_url=site_config["base_url"],
                max_pages=site_config["max_pages"],
                catalog_concurrency=site_config["catalog_concurrency"],
            )
        else:
            logger.info(
                "Шаг сканирования каталога пропущен согласно конфигурации (RUN_CATALOG_SCAN = False)."
            )

        # Парсинг детальных карточек
        if config.RUN_DETAIL_PARSER:
            await crawler.process_queue()
        else:
            logger.info(
                "Шаг парсинга карточек пропущен согласно конфигурации (RUN_DETAIL_PARSER = False)."
            )

        # Экспорт результатов
        if config.RUN_EXPORTER:
            logger.info("Формирование финальных выгрузок...")
            if config.EXPORT_TO_CSV:
                export_sqlite_to_csv(db_path=db_file, csv_path=csv_file)
                logger.info(f"CSV файл успешно создан: {csv_file}")

            if config.EXPORT_TO_XLSX:
                export_sqlite_to_xlsx(db_path=db_file, xlsx_path=xlsx_file)
                logger.info(f"XLSX файл успешно создан: {xlsx_file}")
        else:
            logger.info(
                "Шаг экспорта данных пропущен согласно конфигурации (RUN_EXPORTER = False)."
            )

    except KeyboardInterrupt:
        logger.warning(
            "Программа принудительно остановлена пользователем. Очередь сохранена."
        )
    except Exception as e:
        logger.critical(
            f"Критическая ошибка во время выполнения программы: {e}", exc_info=True
        )
    finally:
        await fetcher.close()
        await db.close()
        logger.info("Сессии закрыты. ScraperELA работу завершил.")


if __name__ == "__main__":
    asyncio.run(main())
