"""
ScraperELA · enrichers/fns_provider.py
========================================
Резервный провайдер обогащения через публичный портал ФНС ЕГРЮЛ.

Эндпоинт: https://egrul.nalog.ru/
Ключ API не требуется. Является публичным сервисом.

Протокол (двухшаговый):
  1. POST на «/» с телом query=ИНН → JSON с поисковым токеном «t».
  2. GET  на «/search-result/{t}»  → JSON с массивом «rows».
  Шаг 2 может потребовать polling: сервер отдаёт результат не мгновенно.

Определение статуса (косвенное — прямого поля нет):
  • Есть поле «e» (дата прекращения)          → LIQUIDATED
  • Поле «g» содержит «ЛИКВИДАТОР»            → LIQUIDATING
  • Поле «g» содержит «КОНКУРСНЫЙ УПРАВЛЯЮЩИЙ» → BANKRUPT
  • Иначе                                      → ACTIVE

Маппинг полей ответа:
  c  — краткое название         n  — полное юридическое название
  i  — ИНН                     o  — ОГРН
  p  — КПП                     r  — дата регистрации (ДД.ММ.ГГГГ)
  g  — должность + ФИО рук-ля  e  — дата прекращения деятельности
  rn — регион                   k  — тип (ul / ip)
  t  — токен для скачивания PDF-выписки

Ограничения:
  • Нет официального SLA — возможна нестабильность.
  • При частых запросах сервер включает captcha.
  • Рекомендуемая задержка ≥ 3 сек между поисковыми запросами.

Место в цепочке: резервный (после DadataEnricher).
Активируется автоматически при исчерпании лимита DaData.
"""

from __future__ import annotations
from pathlib import Path

import asyncio
import logging
import time

import httpx

import config
from enrichers.base import BaseEnricher
from models import EnrichmentResult, OfficialStatus

logger = logging.getLogger("FnsEgrulEnricher")

# --- Section: Константы протокола ---

_BASE_URL = "https://egrul.nalog.ru/"
_SEARCH_RESULT_URL = "https://egrul.nalog.ru/search-result/"

_POLL_INTERVAL: float = 1.5
_POLL_MAX_ATTEMPTS: int = 8


# --- Section: Исключение капчи ---


class CaptchaRequiredError(Exception):
    """Сервер ФНС ЕГРЮЛ потребовал прохождение капчи.

    Выбрасывается при ``captchaRequired: true`` в ответе на POST.
    EnrichmentChain перехватывает исключение и переходит к следующему
    провайдеру, логируя причину сбоя.
    """


# --- Section: Провайдер ---


class FnsEgrulEnricher(BaseEnricher):
    """Резервный провайдер — публичный портал ФНС ЕГРЮЛ (egrul.nalog.ru).

    Двухшаговый протокол:
      1. POST ``/`` с ``query=ИНН`` → поисковый токен.
      2. GET  ``/search-result/{token}`` → JSON с данными юрлица.

    Между поисковыми запросами соблюдается задержка ``request_delay``
    (по умолчанию из ``config.FNS_REQUEST_DELAY``), рассчитанная
    по ``time.monotonic`` — первый запрос уходит без ожидания.

    Example:
        enricher = FnsEgrulEnricher(request_delay=3.0)
        result = await enricher.enrich_by_inn("7707653906")
        await enricher.close()
    """

    def __init__(
        self,
        request_delay: float | None = None,
        cache_dir: Path | None = None,
        cache_ttl_days: int | None = None,
    ) -> None:
        """
        Args:
            request_delay:  Минимальная пауза между поисковыми POST-запросами (сек).
            cache_dir:      Директория файлового кэша. None — кэш отключён.
            cache_ttl_days: TTL кэша в днях. None → берётся из конфига.
        """
        import config as _config

        self._request_delay: float = (
            request_delay if request_delay is not None else _config.FNS_REQUEST_DELAY
        )
        self._client: httpx.AsyncClient = httpx.AsyncClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
                ),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "ru,en;q=0.9",
                "Referer": "https://egrul.nalog.ru/index.html",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://egrul.nalog.ru",
                "DNT": "1",
            },
            timeout=httpx.Timeout(20.0, connect=10.0),
            follow_redirects=True,
        )
        self._session_initialized: bool = False
        self._last_request_at: float = 0.0

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
        """Уникальное читаемое имя провайдера для логов и БД."""
        return "ФНС ЕГРЮЛ (egrul.nalog.ru)"

    async def is_available(self) -> bool:
        """Провайдер публичный, всегда готов к работе.

        Returns:
            Всегда ``True``. Реальная доступность определяется сетью.
        """
        return True

    async def enrich_by_inn(self, inn: str) -> EnrichmentResult | None:
        """Выполняет двухшаговый поиск юрлица на egrul.nalog.ru по ИНН.

        Шаг 1: POST → получение поискового токена.
        Шаг 2: GET  → polling результата по токену.

        Args:
            inn: ИНН организации (10 или 12 цифр).

        Returns:
            ``EnrichmentResult`` при успехе, ``None`` если ИНН не найден.

        Raises:
            CaptchaRequiredError: Сервер потребовал капчу.
            httpx.HTTPError: Сетевой сбой.
        """
        await self._throttle()

        if not self._session_initialized:
            await self._init_session()

        # Шаг 1: POST → поисковый токен
        token = await self._request_search_token(inn)

        # Шаг 2: GET → polling результата
        record = await self._poll_search_result(token, inn)
        if record is None:
            return None

        return self._parse_record(inn, record)

    async def close(self) -> None:
        """Закрывает HTTP-клиент и освобождает сокеты."""
        await self._client.aclose()
        logger.debug("ФНС ЕГРЮЛ: HTTP-клиент закрыт.")

    # -----------------------------------------------------------------------
    # Throttle
    # -----------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Гарантирует минимальную паузу между поисковыми запросами.

        Первый вызов проходит без ожидания. Последующие ждут ровно
        столько, сколько не хватает до ``_request_delay``.
        """
        if self._last_request_at > 0.0:
            elapsed = time.monotonic() - self._last_request_at
            remaining = self._request_delay - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_request_at = time.monotonic()

    # -----------------------------------------------------------------------
    # Шаг 0: Инициализация cookie-сессии
    # -----------------------------------------------------------------------

    async def _init_session(self) -> None:
        """GET на главную страницу ЕГРЮЛ для получения ``JSESSIONID`` cookie.

        httpx.AsyncClient автоматически сохраняет cookies из ``Set-Cookie``.
        Без этого шага POST-запросы могут возвращать 403.
        """
        try:
            resp = await self._client.get(_BASE_URL)
            resp.raise_for_status()
            logger.debug("ФНС ЕГРЮЛ: Сессия инициализирована (cookies получены).")
        except httpx.HTTPError as exc:
            logger.warning(
                f"ФНС ЕГРЮЛ: Не удалось инициализировать сессию: {exc}. "
                "Попытка продолжить без cookies."
            )
        finally:
            self._session_initialized = True

    # -----------------------------------------------------------------------
    # Шаг 1: POST → поисковый токен
    # -----------------------------------------------------------------------

    async def _request_search_token(self, inn: str) -> str:
        """Отправляет поисковый запрос и извлекает токен результата.

        Args:
            inn: ИНН для поиска.

        Returns:
            Строка-токен для запроса результата.

        Raises:
            CaptchaRequiredError: ``captchaRequired: true`` в ответе.
            httpx.HTTPError: Сетевой сбой.
            ValueError: Пустой токен в ответе.
        """
        resp = await self._client.post(
            _BASE_URL,
            data={
                "vyp3CaptchaToken": "",
                "page": "",
                "query": inn,
                "region": "",
                "PreventChromeAutocomplete": "",
            },
        )
        resp.raise_for_status()

        data = resp.json()

        if data.get("captchaRequired"):
            raise CaptchaRequiredError(
                f"Сервер ФНС запросил капчу для ИНН={inn}. "
                f"Увеличьте FNS_REQUEST_DELAY (текущий: {self._request_delay} сек)."
            )

        token: str | None = data.get("t")
        if not token:
            raise ValueError(
                f"ФНС ЕГРЮЛ: Пустой токен в ответе для ИНН={inn}. " f"Ответ: {data}"
            )

        logger.debug(f"ФНС ЕГРЮЛ: Токен получен для ИНН={inn} ({len(token)} символов).")
        return token

    # -----------------------------------------------------------------------
    # Шаг 2: GET → polling результата
    # -----------------------------------------------------------------------

    async def _poll_search_result(
        self,
        token: str,
        inn: str,
    ) -> dict | None:
        """Запрашивает результат поиска с polling до появления данных.

        Сервер может не отдать результат мгновенно — первые 1-2 запроса
        могут вернуть пустой ответ или статус ожидания.

        Args:
            token: Поисковый токен из шага 1.
            inn: ИНН (для логирования).

        Returns:
            Первая запись из ``rows`` или ``None`` если данные не найдены.

        Raises:
            httpx.HTTPError: Сетевой сбой на последней попытке.
        """
        url = f"{_SEARCH_RESULT_URL}{token}"

        for attempt in range(1, _POLL_MAX_ATTEMPTS + 1):
            await asyncio.sleep(_POLL_INTERVAL)

            ts = int(time.time() * 1000)
            try:
                resp = await self._client.get(url, params={"r": ts, "_": ts})
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning(
                    f"ФНС ЕГРЮЛ: Ошибка polling для ИНН={inn} "
                    f"(попытка {attempt}/{_POLL_MAX_ATTEMPTS}): {exc}"
                )
                if attempt == _POLL_MAX_ATTEMPTS:
                    raise
                continue

            data = resp.json()
            rows: list[dict] = data.get("rows", [])

            if rows:
                logger.debug(
                    f"ФНС ЕГРЮЛ: Результат для ИНН={inn} получен "
                    f"(попытка {attempt}, записей: {len(rows)})."
                )
                return rows[0]

            # Сервер ещё обрабатывает — ждём
            if attempt < _POLL_MAX_ATTEMPTS:
                logger.debug(
                    f"ФНС ЕГРЮЛ: Ожидание результата для ИНН={inn} "
                    f"(попытка {attempt}/{_POLL_MAX_ATTEMPTS})..."
                )

        logger.info(
            f"ФНС ЕГРЮЛ: ИНН={inn} не найден после "
            f"{_POLL_MAX_ATTEMPTS} попыток polling."
        )
        return None

    # -----------------------------------------------------------------------
    # Парсинг записи
    # -----------------------------------------------------------------------

    def _parse_record(self, inn: str, record: dict) -> EnrichmentResult:
        """Преобразует запись из ответа ЕГРЮЛ в ``EnrichmentResult``.

        Args:
            inn: Исходный ИНН запроса.
            record: Первый элемент массива ``rows`` из ответа сервера.

        Returns:
            Заполненный ``EnrichmentResult`` с определённым статусом.
        """
        raw_g: str = record.get("g") or ""
        cessation_date: str | None = record.get("e")

        return EnrichmentResult(
            inn=inn,
            status_official=self._infer_status(cessation_date, raw_g),
            inn_verified=record.get("i"),
            legal_name_official=record.get("n"),
            director_official=self._extract_director_name(raw_g),
            liquidation_date=(
                self._normalize_date(cessation_date) if cessation_date else None
            ),
            provider_name=self.provider_name,
        )

    # -----------------------------------------------------------------------
    # Вспомогательные чистые методы
    # -----------------------------------------------------------------------

    @staticmethod
    def _infer_status(cessation_date: str | None, raw_g: str) -> str:
        """Определяет официальный статус юрлица косвенно по полям ответа.

        Логика приоритетов:
          1. Есть дата прекращения (``e``)     → LIQUIDATED
          2. Руководитель = ЛИКВИДАТОР          → LIQUIDATING
          3. Руководитель = КОНКУРСНЫЙ/АРБИТРАЖНЫЙ УПРАВЛЯЮЩИЙ → BANKRUPT
          4. Иначе                              → ACTIVE

        Args:
            cessation_date: Значение поля ``e`` (дата прекращения) или None.
            raw_g: Значение поля ``g`` (должность + ФИО руководителя).

        Returns:
            Константа из ``OfficialStatus``.
        """
        if cessation_date:
            return OfficialStatus.LIQUIDATED

        g_upper = raw_g.upper()
        if "ЛИКВИДАТОР" in g_upper:
            return OfficialStatus.LIQUIDATING
        if "КОНКУРСНЫЙ УПРАВЛЯЮЩИЙ" in g_upper:
            return OfficialStatus.BANKRUPT
        if "АРБИТРАЖНЫЙ УПРАВЛЯЮЩИЙ" in g_upper:
            return OfficialStatus.BANKRUPT

        return OfficialStatus.ACTIVE

    @staticmethod
    def _extract_director_name(raw_g: str) -> str | None:
        """Извлекает ФИО руководителя, отбрасывая префикс должности.

        Примеры::

            "ГЕНЕРАЛЬНЫЙ ДИРЕКТОР: Иванов Иван"  → "Иванов Иван"
            "ЛИКВИДАТОР: Петров Пётр"             → "Петров Пётр"
            ""                                     → None

        Args:
            raw_g: Значение поля ``g`` из ответа ЕГРЮЛ.

        Returns:
            ФИО без должности или ``None`` при отсутствии данных.
        """
        if not raw_g:
            return None
        if ":" in raw_g:
            name = raw_g.split(":", 1)[1].strip()
            return name or None
        return raw_g.strip() or None

    @staticmethod
    def _normalize_date(date_str: str) -> str | None:
        """Конвертирует дату из формата ДД.ММ.ГГГГ в ISO ГГГГ-ММ-ДД.

        Args:
            date_str: Дата в формате ``"17.06.2021"``.

        Returns:
            Дата в формате ``"2021-06-17"`` или исходная строка при ошибке.
        """
        parts = date_str.strip().split(".")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            day, month, year = parts
            return f"{year}-{month}-{day}"
        return date_str
