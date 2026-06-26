"""
ScraperELA · crawler.py
========================
Оркестратор параллельного сбора данных.

v4 — Изменения:
  • site_key передаётся при инициализации — записывается в company_sources и queue.
  • get_next_task() возвращает (url, site_key) — воркер знает источник задачи.
  • AdaptiveConcurrencyController и _AdaptiveGate без изменений.
"""

from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Динамический семафор
# ---------------------------------------------------------------------------


class _AdaptiveGate:
    """
    Семафор с изменяемым лимитом одновременных владельцев.

    Увеличение лимита → ожидающие воркеры немедленно получают разрешение.
    Уменьшение лимита → текущие доработают, новые подождут.
    """

    def __init__(self, initial_limit: int) -> None:
        self._limit = initial_limit
        self._active = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self._active >= self._limit:
                await self._cond.wait()
            self._active += 1

    async def release(self) -> None:
        async with self._cond:
            self._active -= 1
            self._cond.notify_all()

    async def set_limit(self, new_limit: int) -> None:
        async with self._cond:
            self._limit = new_limit
            self._cond.notify_all()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active


# ---------------------------------------------------------------------------
# Адаптивный контроллер конкурентности
# ---------------------------------------------------------------------------


class AdaptiveConcurrencyController:
    """
    Динамически регулирует число активных воркеров.

    +1 воркер: каждые N подряд успехов (N растёт с уровнем).
    -1 воркер: первая / единичная ошибка.
    -2 воркера: 3+ ошибок подряд (деградация сети).
    """

    _BASE_STREAK = 10  # Базовый порог успехов для +1

    def __init__(
        self,
        gate: _AdaptiveGate,
        min_concurrency: int,
        max_concurrency: int,
    ) -> None:
        self._gate = gate
        self._min = min_concurrency
        self._max = max_concurrency
        self._lock = asyncio.Lock()
        self._success_streak = 0
        self._error_streak = 0

    @property
    def current(self) -> int:
        return self._gate.limit

    def _streak_threshold(self) -> int:
        """Порог для +1 растёт с уровнем — осторожнее на высоких скоростях."""
        return self._BASE_STREAK + (self.current - self._min) * 2

    async def on_success(self) -> None:
        async with self._lock:
            self._error_streak = 0
            self._success_streak += 1

            if self._success_streak >= self._streak_threshold():
                self._success_streak = 0
                new = min(self.current + 1, self._max)
                if new != self.current:
                    await self._gate.set_limit(new)
                    logger.info(f"[Adaptive] ▲ {new} воркеров (макс: {self._max})")

    async def on_error(self) -> None:
        async with self._lock:
            self._success_streak = 0
            self._error_streak += 1

            step = 2 if self._error_streak >= 3 else 1
            new = max(self.current - step, self._min)

            if new != self.current:
                await self._gate.set_limit(new)
                logger.warning(
                    f"[Adaptive] ▼ {new} воркеров "
                    f"(ошибок подряд: {self._error_streak}, шаг: -{step})"
                )


# ---------------------------------------------------------------------------
# AsyncCrawler
# ---------------------------------------------------------------------------


class AsyncCrawler:
    """
    Оркестратор с адаптивным пулом воркеров и Sliding Window каталога.

    site_key — идентификатор источника (chop_moscow / prochop_ru / …).
    Передаётся в БД при записи очереди и при сохранении компании.
    """

    def __init__(
        self,
        db: DatabaseManager,
        fetcher: BaseFetcher,
        parser: BaseParser,
        site_key: str,
        concurrency: int = 6,
        request_delay: float = 0.5,
        stats_file_path: Path | None = None,
        min_concurrency: int | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        self.db = db
        self.fetcher = fetcher
        self.parser = parser
        self.site_key = site_key
        self.delay = request_delay
        self.stats_file_path = stats_file_path

        self._min_c = min_concurrency or max(1, concurrency // 2)
        self._max_c = max_concurrency or concurrency * 2

        self._gate = _AdaptiveGate(concurrency)
        self._adaptive = AdaptiveConcurrencyController(
            gate=self._gate,
            min_concurrency=self._min_c,
            max_concurrency=self._max_c,
        )

        # Прогресс
        self.total_tasks = 0
        self.processed_tasks = 0
        self._lock = asyncio.Lock()

        # Каталог
        self.catalog_page_counter = 1
        self.catalog_lock = asyncio.Lock()
        self.should_stop_catalog = False

        # Метрики
        self.stats_start_time = 0.0
        self.stats_success_count = 0
        self.stats_failed_count = 0
        self.stats_total_response_time = 0.0
        self.stats_network_requests = 0

    # -----------------------------------------------------------------------
    # Каталог — Sliding Window
    # -----------------------------------------------------------------------

    async def _catalog_worker(self, base_url: str, max_pages: int) -> None:
        """Воркер скользящего окна — непрерывно берёт следующую страницу."""
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
                    logger.info(f"[{self.site_key}] Каталог стр.{page} пуста. Конец.")
                    self.should_stop_catalog = True
                    break

                added = await self.db.add_to_queue(urls, self.site_key)
                logger.info(
                    f"[{self.site_key}] Каталог стр.{page}: "
                    f"{len(urls)} ссылок (новых: {added})."
                )

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.info(f"[{self.site_key}] Каталог стр.{page} → 404. Конец.")
                else:
                    logger.error(
                        f"[{self.site_key}] HTTP {exc.response.status_code} "
                        f"на стр.{page}: {exc}"
                    )
                self.should_stop_catalog = True
                break

            except Exception as exc:
                logger.error(f"[{self.site_key}] Сбой стр.{page}: {exc}")
                self.should_stop_catalog = True
                break

    async def scan_catalog(
        self,
        catalog_base_url: str,
        max_pages: int,
        catalog_concurrency: int,
    ) -> None:
        """Запускает пул воркеров скользящего окна."""
        logger.info(
            f"[{self.site_key}] Скользящее окно: {catalog_concurrency} воркеров, "
            f"max_pages={max_pages or '∞'}."
        )
        self.catalog_page_counter = 1
        self.should_stop_catalog = False

        await asyncio.gather(
            *[
                asyncio.create_task(self._catalog_worker(catalog_base_url, max_pages))
                for _ in range(catalog_concurrency)
            ]
        )
        logger.info(f"[{self.site_key}] Сканирование каталога завершено.")

    # -----------------------------------------------------------------------
    # Детальный парсинг — адаптивный пул
    # -----------------------------------------------------------------------

    async def _detail_worker(self) -> None:
        """
        Воркер детального парсинга.
        Захватывает _AdaptiveGate перед каждой задачей.
        При уменьшении лимита — ждёт разрешения, не завершается досрочно.
        """
        while True:
            await self._gate.acquire()
            try:
                task = await self.db.get_next_task()
                if not task:
                    break

                url, task_site_key = task

                try:
                    t0 = time.perf_counter()
                    html = await self.fetcher.fetch(url)
                    elapsed = time.perf_counter() - t0

                    company = self.parser.parse_detail(html, url)

                    async with self._lock:
                        self.processed_tasks += 1
                        self.stats_network_requests += 1
                        self.stats_total_response_time += elapsed
                        current = self.processed_tasks
                        total = self.total_tasks

                    if company is not None:
                        await self.db.save_company_and_complete_task(
                            company, task_site_key
                        )
                        async with self._lock:
                            self.stats_success_count += 1
                        await self._adaptive.on_success()

                        logger.info(
                            f"[{current}/{total}] ✓ {company.name or url} "
                            f"{_contact_summary(company.phones, company.emails, company.kpp_list)} "
                            f"[{self._gate.active}/{self._gate.limit}w]"
                        )
                    else:
                        await self.db.mark_task_failed(url)
                        async with self._lock:
                            self.stats_failed_count += 1
                        await self._adaptive.on_error()
                        logger.warning(f"[{current}/{total}] ✗ Пустой парсинг: {url}")

                except Exception as exc:
                    await self.db.mark_task_failed(url)
                    async with self._lock:
                        self.processed_tasks += 1
                        self.stats_failed_count += 1
                        current = self.processed_tasks
                        total = self.total_tasks
                    await self._adaptive.on_error()
                    logger.error(f"[{current}/{total}] ✗ {url}: {exc}")

                await self._save_stats()

            finally:
                await self._gate.release()

    async def process_queue(self) -> None:
        """
        Запускает детальный парсинг всей очереди.
        Стартует MAX воркеров — избыточные ждут на gate.acquire().
        """
        await self.db.reset_processing_tasks()

        self.total_tasks = await self.db.get_pending_tasks_count()
        self.processed_tasks = 0

        if self.total_tasks == 0:
            logger.info(f"[{self.site_key}] Очередь пуста.")
            return

        logger.info(
            f"[{self.site_key}] Пул воркеров: {self.total_tasks} карточек, "
            f"конкурентность {self._gate.limit} [{self._min_c}..{self._max_c}]."
        )

        self.stats_start_time = time.perf_counter()
        self.stats_success_count = 0
        self.stats_failed_count = 0
        self.stats_total_response_time = 0.0
        self.stats_network_requests = 0

        await self._save_stats()

        await asyncio.gather(
            *[asyncio.create_task(self._detail_worker()) for _ in range(self._max_c)]
        )

        self._print_report()

    # -----------------------------------------------------------------------
    # Метрики
    # -----------------------------------------------------------------------

    async def _save_stats(self) -> None:
        if not self.stats_file_path:
            return

        elapsed = time.perf_counter() - self.stats_start_time
        processed = self.stats_success_count + self.stats_failed_count
        cpm = (processed / elapsed * 60) if elapsed > 0 else 0.0
        avg_resp = (
            self.stats_total_response_time / self.stats_network_requests
            if self.stats_network_requests > 0
            else 0.0
        )

        payload = {
            "site_key": self.site_key,
            "queue_total": self.total_tasks,
            "processed_total": self.processed_tasks,
            "success_count": self.stats_success_count,
            "failed_count": self.stats_failed_count,
            "success_rate_percent": round(
                self.stats_success_count / processed * 100 if processed else 0, 1
            ),
            "cards_per_minute": round(cpm, 1),
            "avg_response_time_sec": round(avg_resp, 3),
            "elapsed_time_sec": round(elapsed, 1),
            "concurrency_current": self._gate.limit,
            "concurrency_active": self._gate.active,
            "concurrency_range": f"{self._min_c}..{self._max_c}",
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        try:
            self.stats_file_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error(f"Не удалось сохранить статистику: {exc}")

    def _print_report(self) -> None:
        elapsed = time.perf_counter() - self.stats_start_time
        minutes = int(elapsed // 60)
        seconds = elapsed % 60
        processed = self.stats_success_count + self.stats_failed_count
        cpm = (processed / elapsed * 60) if elapsed > 0 else 0.0
        avg_resp = (
            self.stats_total_response_time / self.stats_network_requests
            if self.stats_network_requests > 0
            else 0.0
        )
        success_pct = self.stats_success_count / processed * 100 if processed else 0.0

        report = (
            "\n"
            "╔══════════════════════════════════════════════════════════╗\n"
            f"║  ScraperELA · {self.site_key:<44}║\n"
            "╠══════════════════════════════════════════════════════════╣\n"
            f"║  Время:          {minutes} мин {seconds:04.1f} сек{'':<26}║\n"
            f"║  Обработано:     {processed:<40}║\n"
            f"║    Успешно:      {self.stats_success_count:<40}║\n"
            f"║    Ошибок:       {self.stats_failed_count:<40}║\n"
            f"║    Успех %:      {success_pct:<39.1f}║\n"
            f"║  Скорость:       {cpm:<39.1f}║\n"
            f"║  Конкурентность: {self._gate.limit} [{self._min_c}..{self._max_c}]{'':<28}║\n"
            f"║  Ср. ответ:      {avg_resp:<39.3f}║\n"
            "╚══════════════════════════════════════════════════════════╝"
        )
        logger.info(report)


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------


def _contact_summary(phones: list[str], emails: list[str], kpps: list[str]) -> str:
    parts: list[str] = []
    if phones:
        parts.append(f"тел:{len(phones)}")
    if emails:
        parts.append(f"email:{len(emails)}")
    if kpps:
        parts.append(f"кпп:{len(kpps)}")
    return f"[{' '.join(parts)}]" if parts else ""
