"""
Модуль асинхронного краулера со скользящим окном сканирования каталога,
детальной статистикой производительности и резервным сохранением метрик на диск.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
import httpx

import config
from database import DatabaseManager
from fetchers import BaseFetcher
from parsers.base import BaseParser

logger = logging.getLogger("Crawler")


class AsyncCrawler:
    """Оркестратор параллельного сбора данных с умным батчингом и метриками."""

    def __init__(
        self,
        db: DatabaseManager,
        fetcher: BaseFetcher,
        parser: BaseParser,
        concurrency: int = 5,
        request_delay: float = 0.5,
        stats_file_path: Path | None = None,
    ) -> None:
        self.db = db
        self.fetcher = fetcher
        self.parser = parser
        self.concurrency = concurrency
        self.delay = request_delay
        self.stats_file_path = stats_file_path

        # Инструменты прогресса
        self.total_tasks = 0
        self.processed_tasks = 0
        self._lock = asyncio.Lock()

        # Асинхронное скользящее окно для каталога
        self.catalog_page_counter = 1
        self.catalog_lock = asyncio.Lock()
        self.should_stop_catalog = False

        # Метрики производительности (Статистика)
        self.stats_start_time = 0.0
        self.stats_success_count = 0
        self.stats_failed_count = 0
        self.stats_total_response_time = 0.0
        self.stats_network_requests = 0

    async def _catalog_worker(self, base_url: str, max_pages: int) -> None:
        """Воркер скользящего окна каталога. Загружает страницы непрерывно."""
        while not self.should_stop_catalog:
            async with self.catalog_lock:
                page = self.catalog_page_counter
                self.catalog_page_counter += 1

            if max_pages > 0 and page > max_pages:
                break

            url = base_url if page == 1 else f"{base_url}page/{page}/"

            try:
                t0 = time.perf_counter()
                html = await self.fetcher.fetch(url)

                async with self._lock:
                    self.stats_network_requests += 1
                    self.stats_total_response_time += time.perf_counter() - t0

                urls = self.parser.parse_listing(html)

                if not urls:
                    logger.info(
                        f"Страница каталога {page} пуста. Завершаем сбор каталога."
                    )
                    self.should_stop_catalog = True
                    break

                added = await self.db.add_to_queue(urls)
                logger.info(
                    f"Страница каталога {page} обработана. Ссылок: {len(urls)} (новых: {added})"
                )

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info(
                        f"Страница каталога {page} вернула 404 Not Found. Сбор каталога завершен."
                    )
                else:
                    logger.error(f"Ошибка статуса на странице каталога {page}: {e}")
                self.should_stop_catalog = True
                break
            except Exception as e:
                logger.error(f"Сбой при обработке страницы каталога {page}: {e}")
                self.should_stop_catalog = True
                break

    async def scan_catalog(
        self, catalog_base_url: str, max_pages: int, catalog_concurrency: int
    ) -> None:
        """Запускает скользящее окно воркеров для параллельного сбора ссылок."""
        logger.info("Запуск скользящего окна воркеров каталога...")
        self.catalog_page_counter = 1
        self.should_stop_catalog = False

        workers = [
            asyncio.create_task(self._catalog_worker(catalog_base_url, max_pages))
            for _ in range(catalog_concurrency)
        ]
        await asyncio.gather(*workers)
        logger.info("Сканирование каталога полностью завершено.")

    async def _detail_worker(self) -> None:
        """Воркер обработки карточек с фиксацией метрик времени ответа."""
        while True:
            url = await self.db.get_next_task()
            if not url:
                break

            try:
                t0 = time.perf_counter()
                html = await self.fetcher.fetch(url)
                resp_time = time.perf_counter() - t0

                company_data = self.parser.parse_detail(html, url)

                async with self._lock:
                    self.processed_tasks += 1
                    self.stats_network_requests += 1
                    self.stats_total_response_time += resp_time
                    current = self.processed_tasks
                    total = self.total_tasks

                if company_data:
                    await self.db.save_company_and_complete_task(company_data)
                    async with self._lock:
                        self.stats_success_count += 1
                    logger.info(
                        f"[{current}/{total}] Успешно обработан: {company_data.name or url}"
                    )
                else:
                    await self.db.mark_task_failed(url)
                    async with self._lock:
                        self.stats_failed_count += 1
                    logger.warning(
                        f"[{current}/{total}] Пустой результат парсинга для: {url}"
                    )

                # Мгновенно сохраняем прогресс на диск на случай краша ПК
                await self._save_stats_to_disk()

            except Exception as e:
                await self.db.mark_task_failed(url)
                async with self._lock:
                    self.processed_tasks += 1
                    self.stats_failed_count += 1
                    current = self.processed_tasks
                    total = self.total_tasks
                logger.error(f"[{current}/{total}] Сбой при обработке {url}: {e}")
                await self._save_stats_to_disk()

    async def process_queue(self) -> None:
        """Запускает детальный парсинг очереди карточек с замером времени работы."""
        await self.db.reset_processing_tasks()

        self.total_tasks = await self.db.get_pending_tasks_count()
        self.processed_tasks = 0

        if self.total_tasks == 0:
            logger.info("Очередь пуста. Шаг детального парсинга пропускается.")
            return

        logger.info(
            f"Запуск пула воркеров. Очередь на обработку: {self.total_tasks} карточек."
        )

        # Фиксируем время старта фазы парсинга
        self.stats_start_time = time.perf_counter()
        self.stats_success_count = 0
        self.stats_failed_count = 0
        self.stats_total_response_time = 0.0
        self.stats_network_requests = 0

        # Сразу создаем файл пустой статистики
        await self._save_stats_to_disk()

        workers = [
            asyncio.create_task(self._detail_worker()) for _ in range(self.concurrency)
        ]
        await asyncio.gather(*workers)

        # Выводим подробный аналитический отчет в лог
        self._print_performance_statistics()

    async def _save_stats_to_disk(self) -> None:
        """Потокобезопасно сохраняет промежуточную статистику на диск в формате JSON."""
        if not self.stats_file_path:
            return

        total_time = (
            time.perf_counter() - self.stats_start_time
            if self.stats_start_time > 0
            else 0
        )
        total_processed = self.stats_success_count + self.stats_failed_count
        cpm = (total_processed / total_time) * 60 if total_time > 0 else 0

        avg_response = (
            self.stats_total_response_time / self.stats_network_requests
            if self.stats_network_requests > 0
            else 0
        )
        success_rate = (
            (self.stats_success_count / total_processed * 100)
            if total_processed > 0
            else 0
        )

        stats_data = {
            "site_key": config.ACTIVE_SITE,
            "queue_total": self.total_tasks,
            "processed_total": self.processed_tasks,
            "success_count": self.stats_success_count,
            "failed_count": self.stats_failed_count,
            "success_rate_percent": round(success_rate, 1),
            "cards_per_minute": round(cpm, 1),
            "avg_response_time_sec": round(avg_response, 3),
            "elapsed_time_sec": round(total_time, 1),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        try:
            # Запись маленького JSON происходит практически мгновенно
            with open(self.stats_file_path, mode="w", encoding="utf-8") as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Не удалось сохранить статистику на диск: {e}")

    def _print_performance_statistics(self) -> None:
        """Выводит структурированный лог со статистикой производительности парсера."""
        total_time = time.perf_counter() - self.stats_start_time
        minutes = int(total_time // 60)
        seconds = total_time % 60

        total_processed = self.stats_success_count + self.stats_failed_count
        cpm = (total_processed / total_time) * 60 if total_time > 0 else 0

        avg_response = (
            self.stats_total_response_time / self.stats_network_requests
            if self.stats_network_requests > 0
            else 0
        )
        success_rate = (
            (self.stats_success_count / total_processed * 100)
            if total_processed > 0
            else 0
        )

        stats_report = (
            "\n"
            "============================================================\n"
            "         АНАЛИТИКА ПРОИЗВОДИТЕЛЬНОСТИ ScraperELA\n"
            "============================================================\n"
            f"- Общее время парсинга очереди: {minutes} мин {seconds:.1f} сек\n"
            f"- Всего обработано карточек:    {total_processed}\n"
            f"  * Успешно записано в БД:      {self.stats_success_count}\n"
            f"  * Ошибок / Пропусков сети:     {self.stats_failed_count}\n"
            f"  * Процент успешных записей:   {success_rate:.1f}%\n"
            f"- Средняя скорость работы:      {cpm:.1f} карт/мин\n"
            f"- Всего сетевых GET-запросов:   {self.stats_network_requests}\n"
            f"- Среднее время ответа сервера: {avg_response:.3f} сек\n"
            "============================================================"
        )
        logger.info(stats_report)
