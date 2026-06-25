"""
Сетевой слой с поддержкой адаптивного Throttling, терминальных 404
и неблокирующего файлового хэш-кэша SHA-256.
"""

import asyncio
from datetime import datetime
import hashlib
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import httpx

import config

logger = logging.getLogger("Fetcher")


class BaseFetcher(ABC):
    """Абстрактный интерфейс для загрузчиков страниц."""

    @abstractmethod
    async def fetch(self, url: str) -> str:
        pass


class AsyncHttpxFetcher(BaseFetcher):
    """Адаптивный асинхронный загрузчик с поддержкой локального хэш-кэша."""

    def __init__(
        self,
        concurrency_limit: int = 5,
        retries: int = 3,
        backoff_factor: float = 1.5,
        cache_dir: Path | None = None,
    ) -> None:
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.retries = retries
        self.backoff_factor = backoff_factor

        # Настройки кэша
        self.cache_dir = cache_dir
        self.use_cache = config.USE_HTML_CACHE and (cache_dir is not None)

        # Адаптивные задержки
        self.baseline_delay = config.REQUEST_DELAY
        self.current_delay = config.REQUEST_DELAY
        self.adaptive_mode = config.ADAPTIVE_MODE
        self.max_delay = config.ADAPTIVE_MAX_DELAY

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=30),
            follow_redirects=True,
            verify=False,
        )

    def _get_cache_path(self, url: str) -> Path:
        """Генерирует уникальный путь к файлу на основе SHA-256 хэша URL."""
        assert self.cache_dir is not None
        hashed = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{hashed}.html"

    async def _read_from_cache(self, url: str) -> str | None:
        """
        Пытается прочитать HTML из локального кэша в пуле потоков.
        Автоматически проверяет возраст файла на диске согласно параметру CACHE_TTL_DAYS.
        """
        path = self._get_cache_path(url)

        if path.exists():
            if config.CACHE_TTL_DAYS > 0:
                try:
                    mtime = path.stat().st_mtime
                    file_age_seconds = datetime.now().timestamp() - mtime
                    file_age_days = file_age_seconds / (24 * 3600)

                    if file_age_days > config.CACHE_TTL_DAYS:
                        logger.debug(
                            f"Кэш для {url} устарел (возраст: {file_age_days:.1f} дн.). "
                            f"Будет выполнено обновление..."
                        )
                        return None  # Сигнал fetcher-у скачать свежую страницу
                except Exception as e:
                    logger.error(
                        f"Не удалось проверить возраст файла кэша {path.name}: {e}"
                    )
                    return None

            return await asyncio.to_thread(path.read_text, encoding="utf-8")

        return None

    async def _archive_old_cache(self, path: Path) -> None:
        """
        Переносит существующий файл кэша в подпапку истории,
        добавляя к имени дату его последней модификации.
        """
        assert self.cache_dir is not None
        if not path.exists():
            return

        try:
            # Получаем дату изменения старого файла для суффикса
            mtime = path.stat().st_mtime
            date_suffix = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")

            history_dir = self.cache_dir / "history"
            history_dir.mkdir(parents=True, exist_ok=True)

            # Новое имя файла: [hash]_[дата_изменения].html
            history_path = history_dir / f"{path.stem}_{date_suffix}.html"

            # Атомарное переименование (перенос) на уровне ОС
            await asyncio.to_thread(path.rename, history_path)
            logger.info(
                f"[АРХИВ] Старая версия страницы сохранена в историю: history/{history_path.name}"
            )
        except Exception as e:
            logger.error(f"Не удалось архивировать старый кэш {path.name}: {e}")

    async def _write_to_cache(self, url: str, html: str) -> None:
        """Сохраняет скачанный HTML в локальный кэш (с возможностью архивации старого)."""
        assert self.cache_dir is not None
        path = self._get_cache_path(url)

        # Если файл уже существовал и включено сохранение истории — архивируем его перед перезаписью
        if path.exists() and config.KEEP_CACHE_HISTORY:
            await self._archive_old_cache(path)

        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_text, html, encoding="utf-8")

    async def fetch(self, url: str) -> str:
        # Проверяем наличие страницы в локальном кэше (если кэширование включено)
        if self.use_cache:
            cached_html = await self._read_from_cache(url)
            if cached_html is not None:
                # Логируем загрузку из кэша без шума в консоли (на уровне DEBUG)
                logger.debug(f"Загружено из локального кэша: {url}")
                return cached_html

        async with self.semaphore:
            for attempt in range(self.retries):
                await asyncio.sleep(self.current_delay)

                try:
                    response = await self.client.get(url)

                    if response.status_code == 404:
                        raise httpx.HTTPStatusError(
                            "404 Not Found", request=response.request, response=response
                        )

                    response.raise_for_status()

                    # Плавное снижение задержки при успехах
                    if self.adaptive_mode and self.current_delay > self.baseline_delay:
                        self.current_delay = max(
                            self.current_delay - 0.05, self.baseline_delay
                        )

                    # Сохраняем успешный ответ в кэш
                    if self.use_cache:
                        await self._write_to_cache(url, response.text)

                    return response.text

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        raise e
                    await self._handle_failure(url, attempt, str(e))
                    if attempt == self.retries - 1:
                        raise e

                except (httpx.HTTPError, httpx.NetworkError) as e:
                    await self._handle_failure(url, attempt, str(e))
                    if attempt == self.retries - 1:
                        raise e

            raise httpx.HTTPError("Непредвиденная ошибка сети.")

    async def _handle_failure(self, url: str, attempt: int, error_msg: str) -> None:
        if self.adaptive_mode:
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
            logger.warning(
                f"[АДАПТИВНОСТЬ] Ошибка запроса к {url} ({error_msg}). "
                f"Увеличиваем паузу до {self.current_delay:.2f} сек."
            )

        sleep_time = self.backoff_factor * (2**attempt)
        logger.warning(f"Повторная попытка скачивания через {sleep_time:.2f} сек...")
        await asyncio.sleep(sleep_time)

    async def close(self) -> None:
        await self.client.aclose()
