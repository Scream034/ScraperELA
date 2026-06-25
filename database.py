"""
Модуль работы с базой данных SQLite.
Версия схемы повышена до 3 (добавлена колонка status).
"""

import time
import logging
import aiosqlite
from pathlib import Path
from models import CompanySchema

logger = logging.getLogger("Database")


class DatabaseManager:
    """Менеджер SQLite с очередью задач и автоматической защитой структуры таблиц."""

    SCHEMA_VERSION = 3  # Повысили версию схемы

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)

        # 1. Получаем числовую версию схемы
        async with self._conn.execute("PRAGMA user_version") as cursor:
            row = await cursor.fetchone()
            current_db_version = row[0] if row else 0

        # 2. Физическая проверка наличия новой колонки 'status'
        schema_invalid_physically = False

        async with self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='companies'"
        ) as cursor:
            table_exists = await cursor.fetchone()

        if table_exists:
            async with self._conn.execute("PRAGMA table_info(companies)") as cursor:
                columns = [col_info[1] for col_info in await cursor.fetchall()]
                # Проверяем физическое наличие колонки status
                if "status" not in columns:
                    schema_invalid_physically = True

        # Если схема невалидна — закрываем базу и делаем бэкап старого файла
        if (0 < current_db_version < self.SCHEMA_VERSION) or schema_invalid_physically:
            logger.warning(
                f"Обнаружена несовместимая схема базы данных (отсутствует колонка status). "
                f"Выполняется автоматическое пересоздание БД..."
            )
            await self._conn.close()
            self._conn = None

            timestamp = int(time.time())
            bak_path = self.db_path.with_suffix(f".db.bak_{timestamp}")

            try:
                self.db_path.rename(bak_path)
                logger.info(f"Создан бэкап несовместимой базы данных: {bak_path.name}")
            except Exception as e:
                logger.error(f"Не удалось создать бэкап файла БД: {e}")
                raise e

            self._conn = await aiosqlite.connect(self.db_path)
            current_db_version = 0

        # Включаем оптимизации
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=NORMAL;")
        await self._conn.commit()

        await self._init_db()

        if current_db_version == 0 or schema_invalid_physically:
            await self._conn.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION};")
            await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _init_db(self) -> None:
        assert self._conn is not None

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                url TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Создаем таблицу с новой колонкой status
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                source_url TEXT PRIMARY KEY,
                name TEXT,
                legal_name TEXT,
                status TEXT DEFAULT 'Работает',
                inn TEXT,
                ogrn TEXT,
                kpp TEXT,
                registration_date TEXT,
                director TEXT,
                address TEXT,
                phones TEXT,
                email TEXT,
                website TEXT,
                scrape_date TEXT
            )
        """)
        await self._conn.commit()

    async def add_to_queue(self, urls: list[str]) -> int:
        assert self._conn is not None
        added_count = 0
        async with self._conn.cursor() as cursor:
            for url in urls:
                try:
                    await cursor.execute(
                        "INSERT OR IGNORE INTO queue (url, status) VALUES (?, 'pending')",
                        (url,),
                    )
                    if cursor.rowcount > 0:
                        added_count += 1
                except aiosqlite.Error:
                    continue
        await self._conn.commit()
        return added_count

    async def get_next_task(self) -> str | None:
        assert self._conn is not None
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                "SELECT url FROM queue WHERE status = 'pending' LIMIT 1"
            )
            row = await cursor.fetchone()
            if not row:
                return None

            url = row[0]
            await cursor.execute(
                "UPDATE queue SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE url = ?",
                (url,),
            )
            await self._conn.commit()
            return url

    async def save_company_and_complete_task(self, company: CompanySchema) -> None:
        assert self._conn is not None
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT OR REPLACE INTO companies (
                    source_url, name, legal_name, status, inn, ogrn, kpp, 
                    registration_date, director, address, phones, email, website, scrape_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    company.source_url,
                    company.name,
                    company.legal_name,
                    company.status,
                    company.inn,
                    company.ogrn,
                    company.kpp,
                    company.registration_date,
                    company.director,
                    company.address,
                    company.phones,
                    company.email,
                    company.website,
                    company.scrape_date,
                ),
            )

            await cursor.execute(
                "UPDATE queue SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE url = ?",
                (company.source_url,),
            )
        await self._conn.commit()

    async def mark_task_failed(self, url: str, max_attempts: int = 5) -> None:
        assert self._conn is not None
        async with self._conn.cursor() as cursor:
            await cursor.execute("SELECT attempts FROM queue WHERE url = ?", (url,))
            row = await cursor.fetchone()
            attempts = row[0] + 1 if row else 1

            if attempts >= max_attempts:
                status = "error"
            else:
                status = "pending"

            await cursor.execute(
                """
                UPDATE queue 
                SET status = ?, attempts = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE url = ?
            """,
                (status, attempts, url),
            )
        await self._conn.commit()

    async def reset_processing_tasks(self) -> None:
        assert self._conn is not None
        await self._conn.execute(
            "UPDATE queue SET status = 'pending' WHERE status = 'processing'"
        )
        await self._conn.commit()

    async def get_pending_tasks_count(self) -> int:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT COUNT(*) FROM queue WHERE status = 'pending'"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0
