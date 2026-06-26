"""
ScraperELA · database.py
========================
Асинхронный менеджер SQLite (aiosqlite) — единая БД для всех источников.

Схема v4:
  companies        — основная запись юрлица (PK: company_id INTEGER)
  company_sources  — источники откуда взята компания (source_url UNIQUE)
  company_contacts — телефоны и email (аккумулятор)
  company_kpp      — КПП (аккумулятор)
  queue            — очередь задач краулера (+ site_key)

Дедупликация компаний:
  Уровень 1 (надёжный): по ИНН.
    → Одна компания с двух сайтов = одна запись в companies.
    → Оба источника в company_sources.
  Уровень 2 (фоллбэк): по source_url в company_sources.
    → Повторный парсинг того же URL → обновление данных.
  Без ИНН: → отдельная запись, объединение невозможно без верифицированных данных.

Потокобезопасность:
  Все операции сериализованы через asyncio.Lock().
  aiosqlite = один фоновый поток → конкурентные commit() вызывают краш.
  Lock занят < 1% времени (узкое место — сеть, не БД).
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite

from models import CompanySchema, EnrichmentResult

logger = logging.getLogger("Database")

Row = dict[str, Any]


class DatabaseManager:
    """
    Менеджер единой SQLite-базы для всех источников ScraperELA.

    Публичный API:
      connect() / close()
      add_to_queue(urls, site_key) → int
      get_next_task() → tuple[str, str] | None  (url, site_key)
      mark_task_failed(url)
      reset_processing_tasks()
      get_pending_tasks_count() → int
      save_company_and_complete_task(company, site_key)
      update_official_status(company_id, result)
      get_companies_for_enrichment(limit, older_than_days) → list[Row]
      get_all_companies_with_contacts() → list[Row]
    """

    SCHEMA_VERSION: int = 4

    _REQUIRED_TABLES: frozenset[str] = frozenset(
        {
            "companies",
            "company_sources",
            "company_contacts",
            "company_kpp",
            "queue",
        }
    )
    _REQUIRED_COLUMNS: frozenset[str] = frozenset(
        {
            "company_id",
            "inn",
            "ogrn",
            "registration_date",
            "name",
            "legal_name",
            "director",
            "address",
            "website",
            "status_aggregator",
            "status_official",
            "inn_verified",
            "legal_name_official",
            "official_verified_at",
            "provider_name",
            "first_seen_at",
            "last_seen_at",
            "scrape_date",
        }
    )
    # Колонки старых схем — их наличие = триггер пересоздания
    _FORBIDDEN_COLUMNS: frozenset[str] = frozenset(
        {
            "source_url",
            "phones",
            "email",
            "kpp",
            "status",
        }
    )

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None
        self._db_lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Жизненный цикл
    # -----------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Подключается к БД, валидирует схему.
        При несовместимости — бэкап + пересоздание v4.
        """
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row

        if await self._needs_recreate():
            await self._backup_and_recreate()

        await self._apply_pragmas()
        await self._init_schema()
        await self._conn.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION};")
        await self._conn.commit()

        logger.info(
            f"БД подключена: {self.db_path.name} "
            f"(schema v{self.SCHEMA_VERSION}, WAL)"
        )

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # -----------------------------------------------------------------------
    # Валидация схемы
    # -----------------------------------------------------------------------

    async def _needs_recreate(self) -> bool:
        assert self._conn is not None

        async with self._conn.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
            version: int = row[0] if row else 0

        if version == 0:
            return False  # Свежая БД — создаём с нуля

        if version < self.SCHEMA_VERSION:
            logger.warning(
                f"Схема БД v{version} устарела (требуется v{self.SCHEMA_VERSION})."
            )

        # Проверяем таблицы
        async with self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) as cur:
            existing = {r[0] for r in await cur.fetchall()}

        if missing := self._REQUIRED_TABLES - existing:
            logger.warning(f"Отсутствуют таблицы: {missing}")
            return True

        # Проверяем колонки companies
        async with self._conn.execute("PRAGMA table_info(companies)") as cur:
            cols = {r[1] for r in await cur.fetchall()}

        if missing_cols := self._REQUIRED_COLUMNS - cols:
            logger.warning(f"Отсутствуют колонки: {missing_cols}")
            return True

        if forbidden := self._FORBIDDEN_COLUMNS & cols:
            logger.warning(f"Устаревшие колонки (старая схема): {forbidden}")
            return True

        return False

    async def _backup_and_recreate(self) -> None:
        assert self._conn is not None
        await self._conn.close()
        self._conn = None

        bak = self.db_path.with_suffix(f".db.bak_{int(time.time())}")
        try:
            self.db_path.rename(bak)
            logger.warning(f"Бэкап несовместимой БД: {bak.name}. Создаём v4...")
        except OSError as exc:
            logger.critical(f"Не удалось создать бэкап: {exc}")
            raise

        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row

    # -----------------------------------------------------------------------
    # Инициализация схемы
    # -----------------------------------------------------------------------

    async def _apply_pragmas(self) -> None:
        assert self._conn is not None
        for pragma in (
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous  = NORMAL;",
            "PRAGMA foreign_keys = ON;",
            "PRAGMA cache_size   = -16384;",  # 16 МБ page cache
            "PRAGMA temp_store   = MEMORY;",
            "PRAGMA mmap_size    = 268435456;",  # 256 МБ mmap
        ):
            await self._conn.execute(pragma)
        await self._conn.commit()

    async def _init_schema(self) -> None:
        assert self._conn is not None

        # Очередь задач
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                url        TEXT    PRIMARY KEY,
                site_key   TEXT    NOT NULL,
                status     TEXT    NOT NULL DEFAULT 'pending',
                attempts   INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_status
                ON queue(status) WHERE status = 'pending'
        """)

        # Основная таблица юрлиц (PK = автоинкремент)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                -- Иммутабельные реквизиты
                inn                  TEXT,
                ogrn                 TEXT,
                registration_date    TEXT,
                -- Мутабельные «свежие» поля
                name                 TEXT,
                legal_name           TEXT,
                director             TEXT,
                address              TEXT,
                website              TEXT,
                -- Статус с сайта-источника
                status_aggregator    TEXT NOT NULL DEFAULT 'Работает',
                -- Поля обогатителя (только EnrichmentChain)
                status_official      TEXT,
                inn_verified         TEXT,
                legal_name_official  TEXT,
                official_verified_at TEXT,
                provider_name        TEXT,
                -- Временны́е метки
                first_seen_at        TEXT NOT NULL,
                last_seen_at         TEXT NOT NULL,
                scrape_date          TEXT NOT NULL
            )
        """)
        # Частичный уникальный индекс на ИНН — ключ дедупликации
        await self._conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_inn
                ON companies(inn) WHERE inn IS NOT NULL
        """)

        # Источники (многие-к-одному: много URL → одна компания)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS company_sources (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id    INTEGER NOT NULL
                                  REFERENCES companies(company_id) ON DELETE CASCADE,
                source_url    TEXT    NOT NULL UNIQUE,
                site_key      TEXT    NOT NULL,
                first_seen_at TEXT    NOT NULL,
                last_seen_at  TEXT    NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sources_company
                ON company_sources(company_id)
        """)

        # Контакты (телефоны + email)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS company_contacts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id    INTEGER NOT NULL
                                  REFERENCES companies(company_id) ON DELETE CASCADE,
                contact_type  TEXT    NOT NULL CHECK(contact_type IN ('phone','email')),
                value         TEXT    NOT NULL,
                first_seen_at TEXT    NOT NULL,
                last_seen_at  TEXT    NOT NULL,
                UNIQUE(company_id, contact_type, value)
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_contacts_company
                ON company_contacts(company_id)
        """)

        # КПП
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS company_kpp (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id    INTEGER NOT NULL
                                  REFERENCES companies(company_id) ON DELETE CASCADE,
                kpp           TEXT    NOT NULL,
                first_seen_at TEXT    NOT NULL,
                UNIQUE(company_id, kpp)
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_kpp_company
                ON company_kpp(company_id)
        """)

        await self._conn.commit()

    # -----------------------------------------------------------------------
    # Контекстный менеджер с блокировкой
    # -----------------------------------------------------------------------

    @property
    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("DatabaseManager не подключён. Вызовите connect().")
        return self._conn

    @asynccontextmanager
    async def _locked(self) -> AsyncIterator[aiosqlite.Connection]:
        """
        Захватывает мьютекс и отдаёт соединение.
        Гарантирует сериализацию всех DB-операций — требование aiosqlite.
        """
        async with self._db_lock:
            yield self._db

    # -----------------------------------------------------------------------
    # Вспомогательные методы чтения (вызываются внутри _locked)
    # -----------------------------------------------------------------------

    @staticmethod
    async def _fetchone(
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> Row | None:
        async with conn.execute(sql, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row is not None else None

    @staticmethod
    async def _fetchall(
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> list[Row]:
        async with conn.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

    @staticmethod
    async def _lastrowid(
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> int:
        async with conn.execute(sql, params) as cur:
            return cur.lastrowid or 0

    # -----------------------------------------------------------------------
    # Логика поиска/создания компании
    # -----------------------------------------------------------------------

    async def _resolve_company_id(
        self,
        conn: aiosqlite.Connection,
        inn: str | None,
        source_url: str,
        now: str,
    ) -> tuple[int, bool]:
        """
        Находит или создаёт запись компании. Возвращает (company_id, is_new).

        Приоритет поиска:
          1. По ИНН — надёжное объединение источников.
          2. По source_url в company_sources — повторный парсинг.
          3. INSERT — первое появление компании.
        """
        # Шаг 1: Поиск по ИНН (если есть)
        if inn:
            row = await self._fetchone(
                conn,
                "SELECT company_id FROM companies WHERE inn = ?",
                (inn,),
            )
            if row:
                return int(row["company_id"]), False

        # Шаг 2: Поиск по source_url (повторный парсинг / ИНН не был известен)
        row = await self._fetchone(
            conn,
            "SELECT company_id FROM company_sources WHERE source_url = ?",
            (source_url,),
        )
        if row:
            return int(row["company_id"]), False

        # Шаг 3: Новая компания — INSERT с минимальными данными
        company_id = await self._lastrowid(
            conn,
            """
            INSERT INTO companies (
                inn, first_seen_at, last_seen_at, scrape_date,
                status_aggregator
            ) VALUES (?, ?, ?, ?, 'Работает')
            """,
            (inn, now, now, now),
        )
        return company_id, True

    # -----------------------------------------------------------------------
    # UPSERT — стратегии слияния
    # -----------------------------------------------------------------------

    @staticmethod
    def _compute_merged(existing: Row, new: CompanySchema) -> dict[str, Any]:
        """
        Трёхуровневая стратегия слияния полей.

        IMMUTABLE     : inn, ogrn, registration_date, first_seen_at
                        → берём из БД если NOT NULL, иначе из парсера
        MUTABLE_FRESH : name, legal_name, director, address, website
                        → берём из парсера если NOT NULL и свежее
        ALWAYS        : status_aggregator, last_seen_at, scrape_date
                        → всегда из парсера
        ENRICHMENT    : status_official, inn_verified, legal_name_official,
                        official_verified_at, provider_name
                        → всегда из БД (только EnrichmentChain трогает)
        """

        def coalesce(db_val: Any, new_val: Any) -> Any:
            return db_val if db_val is not None else new_val

        new_is_fresher = (new.scrape_date or "") >= (existing.get("scrape_date") or "")

        def fresh(db_val: Any, new_val: Any) -> Any:
            return new_val if (new_val is not None and new_is_fresher) else db_val

        return {
            # IMMUTABLE
            "inn": coalesce(existing.get("inn"), new.inn),
            "ogrn": coalesce(existing.get("ogrn"), new.ogrn),
            "registration_date": coalesce(
                existing.get("registration_date"), new.registration_date
            ),
            "first_seen_at": coalesce(existing.get("first_seen_at"), new.scrape_date),
            # MUTABLE_FRESH
            "name": fresh(existing.get("name"), new.name),
            "legal_name": fresh(existing.get("legal_name"), new.legal_name),
            "director": fresh(existing.get("director"), new.director),
            "address": fresh(existing.get("address"), new.address),
            "website": fresh(existing.get("website"), new.website),
            # ALWAYS
            "status_aggregator": new.status_aggregator,
            "last_seen_at": new.last_seen_at or new.scrape_date,
            "scrape_date": new.scrape_date,
            # ENRICHMENT — не трогаем
            "status_official": existing.get("status_official"),
            "inn_verified": existing.get("inn_verified"),
            "legal_name_official": existing.get("legal_name_official"),
            "official_verified_at": existing.get("official_verified_at"),
            "provider_name": existing.get("provider_name"),
        }

    # -----------------------------------------------------------------------
    # Публичные методы сохранения
    # -----------------------------------------------------------------------

    async def save_company_and_complete_task(
        self,
        company: CompanySchema,
        site_key: str,
    ) -> None:
        """
        Полный атомарный UPSERT компании из одного источника.

        Алгоритм:
          1. Resolve company_id (по ИНН → по source_url → INSERT).
          2. Merge полей в companies (UPDATE).
          3. UPSERT в company_sources.
          4. INSERT OR IGNORE контакты и КПП.
          5. Завершить задачу в queue.

        Весь блок под _db_lock — одна транзакция.
        """
        now = company.scrape_date

        async with self._locked() as conn:
            # 1. Resolve
            company_id, is_new = await self._resolve_company_id(
                conn, company.inn, company.source_url, now
            )

            if not is_new:
                # 2. Merge: читаем существующую запись и обновляем
                existing = await self._fetchone(
                    conn,
                    "SELECT * FROM companies WHERE company_id = ?",
                    (company_id,),
                )
                if existing:
                    merged = self._compute_merged(existing, company)
                    await conn.execute(
                        """
                        UPDATE companies SET
                            inn               = ?,
                            ogrn              = ?,
                            registration_date = ?,
                            first_seen_at     = ?,
                            name              = ?,
                            legal_name        = ?,
                            director          = ?,
                            address           = ?,
                            website           = ?,
                            status_aggregator = ?,
                            last_seen_at      = ?,
                            scrape_date       = ?,
                            status_official      = ?,
                            inn_verified         = ?,
                            legal_name_official  = ?,
                            official_verified_at = ?,
                            provider_name        = ?
                        WHERE company_id = ?
                        """,
                        (
                            merged["inn"],
                            merged["ogrn"],
                            merged["registration_date"],
                            merged["first_seen_at"],
                            merged["name"],
                            merged["legal_name"],
                            merged["director"],
                            merged["address"],
                            merged["website"],
                            merged["status_aggregator"],
                            merged["last_seen_at"],
                            merged["scrape_date"],
                            merged["status_official"],
                            merged["inn_verified"],
                            merged["legal_name_official"],
                            merged["official_verified_at"],
                            merged["provider_name"],
                            company_id,
                        ),
                    )
            else:
                # Новая компания — обновляем всё кроме company_id и first_seen_at
                await conn.execute(
                    """
                    UPDATE companies SET
                        ogrn = ?, registration_date = ?,
                        name = ?, legal_name = ?, director = ?,
                        address = ?, website = ?,
                        status_aggregator = ?,
                        last_seen_at = ?, scrape_date = ?
                    WHERE company_id = ?
                    """,
                    (
                        company.ogrn,
                        company.registration_date,
                        company.name,
                        company.legal_name,
                        company.director,
                        company.address,
                        company.website,
                        company.status_aggregator,
                        company.last_seen_at or now,
                        now,
                        company_id,
                    ),
                )

            # 3. UPSERT источника
            await conn.execute(
                """
                INSERT INTO company_sources
                    (company_id, source_url, site_key, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_url) DO UPDATE SET last_seen_at = excluded.last_seen_at
                """,
                (company_id, company.source_url, site_key, now, now),
            )

            # 4a. Контакты
            for ctype, values in (("phone", company.phones), ("email", company.emails)):
                for val in values:
                    await conn.execute(
                        """
                        INSERT INTO company_contacts
                            (company_id, contact_type, value, first_seen_at, last_seen_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(company_id, contact_type, value)
                        DO UPDATE SET last_seen_at = excluded.last_seen_at
                        """,
                        (company_id, ctype, val, now, now),
                    )

            # 4b. КПП
            for kpp in company.kpp_list:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO company_kpp
                        (company_id, kpp, first_seen_at)
                    VALUES (?, ?, ?)
                    """,
                    (company_id, kpp, now),
                )

            # 5. Завершаем задачу
            await conn.execute(
                """
                UPDATE queue SET status = 'completed', updated_at = CURRENT_TIMESTAMP
                WHERE url = ?
                """,
                (company.source_url,),
            )

            await conn.commit()

    async def update_official_status(
        self,
        company_id: int,
        result: EnrichmentResult,
    ) -> None:
        """Точечно обновляет поля обогащения. Только для EnrichmentChain."""
        async with self._locked() as conn:
            await conn.execute(
                """
                UPDATE companies SET
                    status_official      = ?,
                    inn_verified         = ?,
                    legal_name_official  = ?,
                    official_verified_at = ?,
                    provider_name        = ?
                WHERE company_id = ?
                """,
                (
                    result.status_official,
                    result.inn_verified,
                    result.legal_name_official,
                    result.verified_at,
                    result.provider_name,
                    company_id,
                ),
            )
            await conn.commit()

    # -----------------------------------------------------------------------
    # Публичные методы чтения
    # -----------------------------------------------------------------------

    async def get_companies_for_enrichment(
        self,
        limit: int = 100,
        older_than_days: int = 30,
    ) -> list[Row]:
        """Возвращает company_id + inn для обогащения (не обогащённые / устаревшие)."""
        cutoff = f"datetime('now', '-{older_than_days} days')"
        async with self._locked() as conn:
            return await self._fetchall(
                conn,
                f"""
                SELECT company_id, inn
                  FROM companies
                 WHERE inn IS NOT NULL
                   AND (
                       official_verified_at IS NULL
                       OR official_verified_at < {cutoff}
                   )
                 ORDER BY official_verified_at ASC NULLS FIRST
                 LIMIT ?
                """,
                (limit,),
            )

    async def get_all_companies_with_contacts(self) -> list[Row]:
        """
        Все компании с агрегированными контактами и источниками.
        Используется экспортёром.
        GROUP_CONCAT агрегирует данные на уровне SQL — без постобработки в Python.
        """
        async with self._locked() as conn:
            return await self._fetchall(
                conn,
                """
                SELECT
                    c.company_id,
                    c.name,
                    c.status_aggregator,
                    c.status_official,
                    c.address,
                    (
                        SELECT GROUP_CONCAT(cc.value, char(10))
                          FROM company_contacts cc
                         WHERE cc.company_id   = c.company_id
                           AND cc.contact_type = 'phone'
                    )                                           AS phones,
                    (
                        SELECT GROUP_CONCAT(cc.value, char(10))
                          FROM company_contacts cc
                         WHERE cc.company_id   = c.company_id
                           AND cc.contact_type = 'email'
                    )                                           AS emails,
                    c.director,
                    c.inn,
                    c.ogrn,
                    (
                        SELECT GROUP_CONCAT(ck.kpp, ', ')
                          FROM company_kpp ck
                         WHERE ck.company_id = c.company_id
                    )                                           AS kpp_list,
                    c.legal_name,
                    c.legal_name_official,
                    c.inn_verified,
                    c.registration_date,
                    c.website,
                    c.official_verified_at,
                    c.scrape_date,
                    c.first_seen_at,
                    c.last_seen_at,
                    -- Источники: URL + ключ сайта, каждый с новой строки
                    (
                        SELECT GROUP_CONCAT(
                            cs.site_key || ': ' || cs.source_url, char(10)
                        )
                          FROM company_sources cs
                         WHERE cs.company_id = c.company_id
                         ORDER BY cs.first_seen_at
                    )                                           AS sources
                  FROM companies c
                 ORDER BY c.name NULLS LAST
            """,
            )

    # -----------------------------------------------------------------------
    # Queue API
    # -----------------------------------------------------------------------

    async def add_to_queue(self, urls: list[str], site_key: str) -> int:
        """
        INSERT OR IGNORE URL в очередь с привязкой к site_key.
        Возвращает число реально добавленных новых URL.
        """
        added = 0
        async with self._locked() as conn:
            for url in urls:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO queue (url, site_key, status)
                    VALUES (?, ?, 'pending')
                    """,
                    (url, site_key),
                )
                async with conn.execute("SELECT changes()") as cur:
                    row = await cur.fetchone()
                    if row and row[0]:
                        added += row[0]
            await conn.commit()
        return added

    async def get_next_task(self) -> tuple[str, str] | None:
        """
        Атомарно берёт следующий pending-URL и переводит в 'processing'.
        Возвращает (url, site_key) или None если очередь пуста.

        UPDATE ... RETURNING — один атомарный запрос в SQLite.
        _db_lock гарантирует что два воркера не получат один URL.
        """
        async with self._locked() as conn:
            async with conn.execute("""
                UPDATE queue
                   SET status     = 'processing',
                       updated_at = CURRENT_TIMESTAMP
                 WHERE url = (
                     SELECT url FROM queue
                      WHERE status = 'pending'
                      LIMIT 1
                 )
                 RETURNING url, site_key
                """) as cur:
                row = await cur.fetchone()

            if row is None:
                return None

            await conn.commit()
            return str(row[0]), str(row[1])

    async def mark_task_failed(
        self,
        url: str,
        max_attempts: int = 5,
    ) -> None:
        async with self._locked() as conn:
            existing = await self._fetchone(
                conn, "SELECT attempts FROM queue WHERE url = ?", (url,)
            )
            attempts = (existing["attempts"] + 1) if existing else 1
            new_status = "error" if attempts >= max_attempts else "pending"

            await conn.execute(
                """
                UPDATE queue SET
                    status     = ?,
                    attempts   = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE url = ?
                """,
                (new_status, attempts, url),
            )
            await conn.commit()

    async def reset_processing_tasks(self) -> None:
        """Возвращает зависшие 'processing' задачи в 'pending' при рестарте."""
        async with self._locked() as conn:
            await conn.execute(
                "UPDATE queue SET status = 'pending' WHERE status = 'processing'"
            )
            await conn.commit()

    async def get_pending_tasks_count(self) -> int:
        async with self._locked() as conn:
            async with conn.execute(
                "SELECT COUNT(*) FROM queue WHERE status = 'pending'"
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0
