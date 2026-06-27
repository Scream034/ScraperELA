"""
ScraperELA · database.py
========================
Асинхронный менеджер SQLite (aiosqlite) — единая БД для всех источников.

Схема v5:
  companies        — основная запись юрлица (PK: company_id INTEGER)
  company_sources  — источники откуда взята компания (source_url UNIQUE)
  company_contacts — телефоны и email (аккумулятор)
  company_kpp      — КПП (аккумулятор)
  queue            — очередь задач краулера (+ site_key)

Дедупликация компаний (четыре уровня):
  Уровень 1: по ИНН.
  Уровень 2: по нормализованному website (работает в обе стороны, без ограничения inn).
  Уровень 3: по source_url.
  Уровень 4: по нормализованному имени + пересечению телефонных отпечатков.

Ретроактивная дедупликация:
  run_dedup_pass() — однократный проход по существующим данным.
  Вызывается при RUN_DEDUP_PASS=True в .env.

Потокобезопасность:
  Все операции сериализованы через asyncio.Lock().
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite

from models import CompanySchema, EnrichmentResult, normalize_website

logger = logging.getLogger("Database")

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Утилиты нормализации
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    """Нормализует название компании для нечёткого сравнения (level-4 dedup).

    Удаляет кавычки, дефисы, лишние пробелы, приводит к нижнему регистру.

    Args:
        name: Исходное название.

    Returns:
        Нормализованная строка для сравнения.
    """
    v = name.strip().lower()
    v = re.sub(r'[«»„""\'""\u201c\u201d]', "", v)
    v = re.sub(r"[-–—]", " ", v)
    v = re.sub(r"[().,!?;:]", "", v)
    return re.sub(r"\s+", " ", v).strip()


def _phone_digits(phone: str) -> str:
    """Извлекает цифровой отпечаток телефона.

    Args:
        phone: Телефон в любом формате.

    Returns:
        Строка из цифр, например ``"74951234567"``.
    """
    return "".join(filter(str.isdigit, phone))


# ---------------------------------------------------------------------------
# DatabaseManager
# ---------------------------------------------------------------------------


class DatabaseManager:
    """
    Менеджер единой SQLite-базы для всех источников ScraperELA.

    Публичный API:
      connect() / close()
      add_to_queue(urls, site_key) → int
      get_next_task() → tuple[str, str] | None
      mark_task_failed(url)
      reset_processing_tasks()
      get_pending_tasks_count() → int
      save_company_and_complete_task(company, site_key)
      update_official_status(company_id, result)
      get_companies_for_enrichment(limit, older_than_days) → list[Row]
      get_all_companies_with_contacts() → list[Row]
      run_dedup_pass() → dict[str, int]
    """

    SCHEMA_VERSION: int = 5

    _REQUIRED_TABLES: frozenset[str] = frozenset(
        {"companies", "company_sources", "company_contacts", "company_kpp", "queue"}
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
            "liquidation_date",
            "director_official",
            "address_official",
            "first_seen_at",
            "last_seen_at",
            "scrape_date",
        }
    )
    _FORBIDDEN_COLUMNS: frozenset[str] = frozenset(
        {"source_url", "phones", "email", "kpp", "status"}
    )

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None
        self._db_lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Жизненный цикл
    # -----------------------------------------------------------------------

    async def connect(self) -> None:
        """Подключается к БД, валидирует схему.

        При несовместимости — бэкап + пересоздание.
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
            return False

        if version < self.SCHEMA_VERSION:
            logger.warning(
                f"Схема БД v{version} устарела (требуется v{self.SCHEMA_VERSION})."
            )

        async with self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) as cur:
            existing = {r[0] for r in await cur.fetchall()}

        if missing := self._REQUIRED_TABLES - existing:
            logger.warning(f"Отсутствуют таблицы: {missing}")
            return True

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
            logger.warning(f"Бэкап несовместимой БД: {bak.name}. Создаём заново...")
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
            "PRAGMA cache_size   = -16384;",
            "PRAGMA temp_store   = MEMORY;",
            "PRAGMA mmap_size    = 268435456;",
        ):
            await self._conn.execute(pragma)
        await self._conn.commit()

    async def _init_schema(self) -> None:
        assert self._conn is not None

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

        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                inn                  TEXT,
                ogrn                 TEXT,
                registration_date    TEXT,
                name                 TEXT,
                legal_name           TEXT,
                director             TEXT,
                address              TEXT,
                website              TEXT,
                status_aggregator    TEXT NOT NULL DEFAULT 'Работает',
                status_official      TEXT,
                inn_verified         TEXT,
                legal_name_official  TEXT,
                official_verified_at TEXT,
                provider_name        TEXT,
                liquidation_date     TEXT,
                director_official    TEXT,
                address_official     TEXT,
                first_seen_at        TEXT NOT NULL,
                last_seen_at         TEXT NOT NULL,
                scrape_date          TEXT NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_inn
                ON companies(inn) WHERE inn IS NOT NULL
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_companies_website
                ON companies(website) WHERE website IS NOT NULL
        """)

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
        """Захватывает мьютекс и отдаёт соединение.

        Гарантирует сериализацию всех DB-операций — требование aiosqlite.
        """
        async with self._db_lock:
            yield self._db

    # -----------------------------------------------------------------------
    # Вспомогательные методы чтения
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
        website: str | None = None,
        name: str | None = None,
        phones: list[str] | None = None,
    ) -> tuple[int, bool]:
        """Находит или создаёт запись компании по каскаду из четырёх ключей.

        Порядок приоритетов:
          1. **ИНН** — самый надёжный ключ.
          2. **Website (нормализованный)** — работает в обе стороны независимо
             от наличия ИНН у входящей или существующей записи.
          3. **Source URL** — повторный парсинг того же URL.
          4. **Имя + телефонный отпечаток** — мягкий ключ для компаний без ИНН
             и сайта. Требует точного совпадения нормализованных имён И
             пересечения множеств digit-fingerprint телефонов.

        Args:
            conn:       Соединение (внутри _locked).
            inn:        ИНН или None.
            source_url: URL детальной страницы.
            now:        Текущая дата.
            website:    Нормализованный сайт компании или None.
            name:       Краткое название для level-4 поиска.
            phones:     Список телефонов для level-4 поиска.

        Returns:
            Кортеж ``(company_id, is_new)``.
        """
        # --- Level 1: ИНН ---------------------------------------------------
        if inn:
            row = await self._fetchone(
                conn,
                "SELECT company_id FROM companies WHERE inn = ?",
                (inn,),
            )
            if row:
                return int(row["company_id"]), False

        # --- Level 2: Нормализованный website (без ограничения по ИНН) -----
        if website:
            row = await self._fetchone(
                conn,
                "SELECT company_id FROM companies WHERE website = ?",
                (normalize_website(website),),
            )
            if row:
                logger.debug(
                    f"[DB] Level-2 слияние по website='{website}' "
                    f"(source_url={source_url})."
                )
                return int(row["company_id"]), False

        # --- Level 3: Source URL --------------------------------------------
        row = await self._fetchone(
            conn,
            "SELECT company_id FROM company_sources WHERE source_url = ?",
            (source_url,),
        )
        if row:
            return int(row["company_id"]), False

        # --- Level 4: Имя + телефонный отпечаток ----------------------------
        if name and phones:
            incoming_digits: set[str] = {
                d for p in phones if len(d := _phone_digits(p)) >= 7
            }
            if incoming_digits:
                name_norm = _normalize_name(name)
                inn_cond = "AND c.inn IS NULL" if inn else ""
                candidates = await self._fetchall(
                    conn,
                    f"""
                    SELECT c.company_id,
                           c.name,
                           GROUP_CONCAT(cc.value, '§') AS phones_concat
                      FROM companies c
                      JOIN company_contacts cc
                        ON cc.company_id = c.company_id
                       AND cc.contact_type = 'phone'
                     WHERE c.name IS NOT NULL
                       {inn_cond}
                     GROUP BY c.company_id
                    """,
                )
                for candidate in candidates:
                    cand_name = candidate.get("name") or ""
                    if _normalize_name(cand_name) != name_norm:
                        continue
                    phones_raw = candidate.get("phones_concat") or ""
                    cand_digits: set[str] = {
                        d
                        for p in phones_raw.split("§")
                        if len(d := _phone_digits(p)) >= 7
                    }
                    if incoming_digits & cand_digits:
                        logger.debug(
                            f"[DB] Level-4 слияние по имени+телефону: '{name}' "
                            f"→ company_id={candidate['company_id']}."
                        )
                        return int(candidate["company_id"]), False

        # --- INSERT новой компании ------------------------------------------
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
    # Ретроактивная дедупликация (однократный проход)
    # -----------------------------------------------------------------------

    async def _merge_companies(
        self,
        conn: aiosqlite.Connection,
        winner_id: int,
        loser_id: int,
    ) -> None:
        """Сливает loser в winner: переносит данные, удаляет loser.

        Стратегия полей: ``winner`` сохраняет свои значения; поля, равные
        ``None``, заполняются из ``loser``. Исключение: ``first_seen_at``
        берётся наименьшее из двух.

        Args:
            conn:      Соединение (уже внутри _locked).
            winner_id: company_id, который останется в БД.
            loser_id:  company_id, который будет удалён после слияния.
        """
        winner = await self._fetchone(
            conn, "SELECT * FROM companies WHERE company_id = ?", (winner_id,)
        )
        loser = await self._fetchone(
            conn, "SELECT * FROM companies WHERE company_id = ?", (loser_id,)
        )
        if not winner or not loser:
            return

        # Коалесценция полей: winner берёт отсутствующее из loser.
        coalesce_cols = (
            "inn",
            "ogrn",
            "registration_date",
            "name",
            "legal_name",
            "director",
            "address",
            "website",
            "status_official",
            "inn_verified",
            "legal_name_official",
            "official_verified_at",
            "provider_name",
            "liquidation_date",
            "director_official",
            "address_official",
        )
        updates: dict[str, Any] = {}
        for col in coalesce_cols:
            if winner.get(col) is None and loser.get(col) is not None:
                updates[col] = loser[col]

        # Берём более раннюю дату первого обнаружения.
        w_first = winner.get("first_seen_at") or ""
        l_first = loser.get("first_seen_at") or ""
        if l_first and (not w_first or l_first < w_first):
            updates["first_seen_at"] = l_first

        # Статус агрегатора: «Работает» приоритетнее.
        if (
            loser.get("status_aggregator") == "Работает"
            and winner.get("status_aggregator") != "Работает"
        ):
            updates["status_aggregator"] = "Работает"

        if updates:
            set_clause = ", ".join(f"{col} = ?" for col in updates)
            await conn.execute(
                f"UPDATE companies SET {set_clause} WHERE company_id = ?",
                (*updates.values(), winner_id),
            )

        # Перенос источников (source_url UNIQUE — конфликтов быть не может,
        # т.к. один URL не может принадлежать двум разным компаниям).
        await conn.execute(
            "UPDATE company_sources SET company_id = ? WHERE company_id = ?",
            (winner_id, loser_id),
        )

        # Перенос телефонов с digit-fingerprint дедупликацией.
        winner_phones = await self._fetchall(
            conn,
            "SELECT value FROM company_contacts "
            "WHERE company_id = ? AND contact_type = 'phone'",
            (winner_id,),
        )
        winner_digits: set[str] = {_phone_digits(r["value"]) for r in winner_phones}

        loser_phones = await self._fetchall(
            conn,
            "SELECT value, first_seen_at, last_seen_at FROM company_contacts "
            "WHERE company_id = ? AND contact_type = 'phone'",
            (loser_id,),
        )
        for row in loser_phones:
            digits = _phone_digits(row["value"])
            if len(digits) >= 7 and digits not in winner_digits:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO company_contacts
                        (company_id, contact_type, value, first_seen_at, last_seen_at)
                    VALUES (?, 'phone', ?, ?, ?)
                    """,
                    (
                        winner_id,
                        row["value"],
                        row["first_seen_at"],
                        row["last_seen_at"],
                    ),
                )
                winner_digits.add(digits)

        # Перенос email (INSERT OR IGNORE — UNIQUE по значению).
        loser_emails = await self._fetchall(
            conn,
            "SELECT value, first_seen_at, last_seen_at FROM company_contacts "
            "WHERE company_id = ? AND contact_type = 'email'",
            (loser_id,),
        )
        for row in loser_emails:
            await conn.execute(
                """
                INSERT OR IGNORE INTO company_contacts
                    (company_id, contact_type, value, first_seen_at, last_seen_at)
                VALUES (?, 'email', ?, ?, ?)
                """,
                (winner_id, row["value"], row["first_seen_at"], row["last_seen_at"]),
            )

        # Перенос КПП.
        loser_kpps = await self._fetchall(
            conn,
            "SELECT kpp, first_seen_at FROM company_kpp WHERE company_id = ?",
            (loser_id,),
        )
        for row in loser_kpps:
            await conn.execute(
                "INSERT OR IGNORE INTO company_kpp (company_id, kpp, first_seen_at) "
                "VALUES (?, ?, ?)",
                (winner_id, row["kpp"], row["first_seen_at"]),
            )

        # Удаляем loser. CASCADE удалит оставшиеся контакты/kpp/sources.
        await conn.execute("DELETE FROM companies WHERE company_id = ?", (loser_id,))

    @staticmethod
    def _pick_winner(group: list[Row]) -> tuple[int, list[int]]:
        """Выбирает «победителя» слияния из группы дублей.

        Приоритет: наличие ИНН → больше источников → меньший company_id.

        Args:
            group: Список строк из таблицы companies.

        Returns:
            Кортеж ``(winner_id, [loser_id, ...])``.
        """

        def sort_key(r: Row) -> tuple[int, int, int]:
            has_inn = 0 if r.get("inn") else 1
            return (has_inn, 0, int(r["company_id"]))

        sorted_group = sorted(group, key=sort_key)
        winner_id = int(sorted_group[0]["company_id"])
        loser_ids = [int(r["company_id"]) for r in sorted_group[1:]]
        return winner_id, loser_ids

    async def run_dedup_pass(self) -> dict[str, int]:
        """Ретроактивная дедупликация существующих записей компаний.

        Предназначен для однократного запуска после обновления кода дедупликации,
        чтобы склеить записи, созданные старой версией без level-2 / level-4.

        Алгоритм:
          1. **Website-дубли**: находит группы компаний с одинаковым
             нормализованным website, сливает через _merge_companies.
          2. **Имя + телефон дубли**: в Python группирует по нормализованному
             имени, внутри каждой группы ищет компании с пересекающимися
             телефонными отпечатками.

        «Тигр» (разные сайты + разные телефоны) намеренно НЕ сливается.

        Returns:
            Словарь ``{"website": N, "name_phone": M}`` — число выполненных слияний.
        """
        stats = {"website": 0, "name_phone": 0}

        async with self._locked() as conn:

            # --- Проход 1: website-дубли ------------------------------------
            website_groups = await self._fetchall(
                conn,
                """
                SELECT website, GROUP_CONCAT(company_id, ',') AS ids
                  FROM companies
                 WHERE website IS NOT NULL
                 GROUP BY website
                HAVING COUNT(*) > 1
                """,
            )

            for group_row in website_groups:
                ids = [int(x) for x in group_row["ids"].split(",")]
                rows = []
                for cid in ids:
                    r = await self._fetchone(
                        conn,
                        "SELECT company_id, inn FROM companies WHERE company_id = ?",
                        (cid,),
                    )
                    if r:
                        rows.append(r)

                if len(rows) < 2:
                    continue

                winner_id, loser_ids = self._pick_winner(rows)
                for loser_id in loser_ids:
                    logger.info(
                        f"[Dedup] Website-слияние: company_id={loser_id} "
                        f"→ winner={winner_id} "
                        f"(website='{group_row['website']}')."
                    )
                    await self._merge_companies(conn, winner_id, loser_id)
                    stats["website"] += 1

            await conn.commit()

            # --- Проход 2: имя + телефонный отпечаток -----------------------
            # Загружаем все компании с телефонами одним запросом.
            all_with_phones = await self._fetchall(
                conn,
                """
                SELECT c.company_id,
                       c.inn,
                       c.name,
                       GROUP_CONCAT(cc.value, '§') AS phones_concat
                  FROM companies c
                  JOIN company_contacts cc
                    ON cc.company_id = c.company_id
                   AND cc.contact_type = 'phone'
                 WHERE c.name IS NOT NULL
                 GROUP BY c.company_id
                """,
            )

            # Группируем по нормализованному имени.
            by_name: dict[str, list[Row]] = {}
            for row in all_with_phones:
                key = _normalize_name(row["name"])
                by_name.setdefault(key, []).append(row)

            # Внутри каждой группы ищем пары с пересекающимися телефонами.
            # Union-Find не нужен: при N=2-3 прямой перебор достаточен.
            already_merged: set[int] = set()

            for name_key, group in by_name.items():
                if len(group) < 2:
                    continue

                for i, a in enumerate(group):
                    a_id = int(a["company_id"])
                    if a_id in already_merged:
                        continue
                    a_digits: set[str] = {
                        d
                        for p in (a.get("phones_concat") or "").split("§")
                        if len(d := _phone_digits(p)) >= 7
                    }

                    for b in group[i + 1 :]:
                        b_id = int(b["company_id"])
                        if b_id in already_merged:
                            continue
                        b_digits: set[str] = {
                            d
                            for p in (b.get("phones_concat") or "").split("§")
                            if len(d := _phone_digits(p)) >= 7
                        }

                        if not (a_digits & b_digits):
                            continue

                        # Выбираем победителя между этой парой.
                        winner_id, loser_ids = self._pick_winner([a, b])
                        for loser_id in loser_ids:
                            if loser_id in already_merged:
                                continue
                            logger.info(
                                f"[Dedup] Имя+телефон-слияние: "
                                f"company_id={loser_id} → winner={winner_id} "
                                f"(имя='{name_key}')."
                            )
                            await self._merge_companies(conn, winner_id, loser_id)
                            already_merged.add(loser_id)
                            stats["name_phone"] += 1

            await conn.commit()

        logger.info(
            f"[Dedup] Завершено. "
            f"Слияний по website: {stats['website']}, "
            f"по имени+телефону: {stats['name_phone']}."
        )
        return stats

    # -----------------------------------------------------------------------
    # UPSERT — стратегии слияния
    # -----------------------------------------------------------------------

    @staticmethod
    def _compute_merged(existing: Row, new: CompanySchema) -> dict[str, Any]:
        """Трёхуровневая стратегия слияния данных компании."""

        def coalesce(db_val: Any, new_val: Any) -> Any:
            return db_val if db_val is not None else new_val

        new_is_fresher = (new.scrape_date or "") >= (existing.get("scrape_date") or "")

        def fresh(db_val: Any, new_val: Any) -> Any:
            return new_val if (new_val is not None and new_is_fresher) else db_val

        return {
            "inn": coalesce(existing.get("inn"), new.inn),
            "ogrn": coalesce(existing.get("ogrn"), new.ogrn),
            "registration_date": coalesce(
                existing.get("registration_date"), new.registration_date
            ),
            "first_seen_at": coalesce(existing.get("first_seen_at"), new.scrape_date),
            "name": fresh(existing.get("name"), new.name),
            "legal_name": fresh(existing.get("legal_name"), new.legal_name),
            "director": fresh(existing.get("director"), new.director),
            "address": fresh(existing.get("address"), new.address),
            "website": coalesce(existing.get("website"), new.website),
            "status_aggregator": new.status_aggregator,
            "last_seen_at": new.last_seen_at or new.scrape_date,
            "scrape_date": new.scrape_date,
            "status_official": existing.get("status_official"),
            "inn_verified": existing.get("inn_verified"),
            "legal_name_official": existing.get("legal_name_official"),
            "official_verified_at": existing.get("official_verified_at"),
            "provider_name": existing.get("provider_name"),
            "liquidation_date": existing.get("liquidation_date"),
            "director_official": existing.get("director_official"),
            "address_official": existing.get("address_official"),
        }

    # -----------------------------------------------------------------------
    # Публичные методы сохранения
    # -----------------------------------------------------------------------

    async def save_company_and_complete_task(
        self,
        company: CompanySchema,
        site_key: str,
    ) -> None:
        """Сохраняет компанию и отмечает задачу в очереди как выполненную.

        Args:
            company:  Распарсенная компания.
            site_key: Ключ источника.
        """
        now = company.scrape_date

        async with self._locked() as conn:
            company_id, is_new = await self._resolve_company_id(
                conn,
                company.inn,
                company.source_url,
                now,
                company.website,
                company.name,
                company.phones,
            )

            if not is_new:
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
                            provider_name        = ?,
                            liquidation_date     = ?,
                            director_official    = ?,
                            address_official     = ?
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
                            merged["liquidation_date"],
                            merged["director_official"],
                            merged["address_official"],
                            company_id,
                        ),
                    )
            else:
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

            await conn.execute(
                """
                INSERT INTO company_sources
                    (company_id, source_url, site_key, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_url) DO UPDATE SET last_seen_at = excluded.last_seen_at
                """,
                (company_id, company.source_url, site_key, now, now),
            )

            # Телефоны с digit-fingerprint дедупликацией.
            existing_phone_rows = await self._fetchall(
                conn,
                "SELECT value FROM company_contacts "
                "WHERE company_id = ? AND contact_type = 'phone'",
                (company_id,),
            )
            existing_digits: set[str] = {
                _phone_digits(r["value"]) for r in existing_phone_rows
            }
            for phone in company.phones:
                digits = _phone_digits(phone)
                if not digits or len(digits) < 7 or digits in existing_digits:
                    continue
                await conn.execute(
                    """
                    INSERT INTO company_contacts
                        (company_id, contact_type, value, first_seen_at, last_seen_at)
                    VALUES (?, 'phone', ?, ?, ?)
                    """,
                    (company_id, phone, now, now),
                )
                existing_digits.add(digits)

            for email in company.emails:
                await conn.execute(
                    """
                    INSERT INTO company_contacts
                        (company_id, contact_type, value, first_seen_at, last_seen_at)
                    VALUES (?, 'email', ?, ?, ?)
                    ON CONFLICT(company_id, contact_type, value)
                    DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (company_id, email, now, now),
                )

            for kpp in company.kpp_list:
                await conn.execute(
                    "INSERT OR IGNORE INTO company_kpp "
                    "(company_id, kpp, first_seen_at) VALUES (?, ?, ?)",
                    (company_id, kpp, now),
                )

            await conn.execute(
                "UPDATE queue SET status = 'completed', updated_at = CURRENT_TIMESTAMP "
                "WHERE url = ?",
                (company.source_url,),
            )
            await conn.commit()

    async def update_official_status(
        self,
        company_id: int,
        result: EnrichmentResult,
    ) -> None:
        """Точечно обновляет все поля официального обогащения.

        Args:
            company_id: PK компании.
            result:     Результат от провайдера обогащения.
        """
        async with self._locked() as conn:
            await conn.execute(
                """
                UPDATE companies SET
                    status_official      = ?,
                    inn_verified         = ?,
                    legal_name_official  = ?,
                    official_verified_at = ?,
                    provider_name        = ?,
                    liquidation_date     = ?,
                    director_official    = ?,
                    address_official     = ?
                WHERE company_id = ?
                """,
                (
                    result.status_official,
                    result.inn_verified,
                    result.legal_name_official,
                    result.verified_at,
                    result.provider_name,
                    result.liquidation_date,
                    result.director_official,
                    result.address_official,
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
        filters: dict[str, Any] | None = None,
    ) -> list[Row]:
        """Возвращает компании для обогащения с учётом TTL и приоритетов.

        Args:
            limit:           Максимальное число записей (0 — без лимита).
            older_than_days: TTL верификации в днях.
            filters:         Словарь ENRICHMENT_CONFIG.

        Returns:
            Список строк с полями ``company_id`` и ``inn``.
        """
        cutoff = f"datetime('now', '-{older_than_days} days')"
        where_parts = [
            "c.inn IS NOT NULL",
            f"(c.official_verified_at IS NULL OR c.official_verified_at < {cutoff})",
        ]
        params: list[Any] = []

        if filters:
            if city := filters.get("filter_city"):
                where_parts.append("c.address LIKE ? COLLATE NOCASE")
                params.append(f"%{city}%")
            if site_key := filters.get("filter_site_key"):
                where_parts.append("""
                    EXISTS (
                        SELECT 1 FROM company_sources cs_f
                         WHERE cs_f.company_id = c.company_id
                           AND cs_f.site_key   = ?
                    )
                """)
                params.append(site_key)

        limit_sql = f"LIMIT {limit}" if limit > 0 else ""

        async with self._locked() as conn:
            return await self._fetchall(
                conn,
                f"""
                SELECT c.company_id, c.inn
                  FROM companies c
                 WHERE {' AND '.join(where_parts)}
                 ORDER BY
                     (c.status_official IS NULL) DESC,
                     (c.status_aggregator != 'Работает') DESC,
                     c.official_verified_at ASC
                 {limit_sql}
                """,
                tuple(params),
            )

    async def get_all_companies_with_contacts(self) -> list[Row]:
        """Все компании с агрегированными контактами и источниками."""
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
                         WHERE cc.company_id = c.company_id AND cc.contact_type = 'phone'
                    ) AS phones,
                    (
                        SELECT GROUP_CONCAT(cc.value, char(10))
                          FROM company_contacts cc
                         WHERE cc.company_id = c.company_id AND cc.contact_type = 'email'
                    ) AS emails,
                    c.director,
                    c.inn,
                    c.ogrn,
                    (
                        SELECT GROUP_CONCAT(ck.kpp, ', ')
                          FROM company_kpp ck
                         WHERE ck.company_id = c.company_id
                    ) AS kpp_list,
                    c.legal_name,
                    c.legal_name_official,
                    c.inn_verified,
                    c.registration_date,
                    c.website,
                    c.official_verified_at,
                    c.scrape_date,
                    c.first_seen_at,
                    c.last_seen_at,
                    (
                        SELECT GROUP_CONCAT(cs.source_url, char(10))
                          FROM company_sources cs
                         WHERE cs.company_id = c.company_id
                         ORDER BY cs.first_seen_at
                    ) AS sources
                  FROM companies c
                 ORDER BY c.name NULLS LAST
                """,
            )

    # -----------------------------------------------------------------------
    # Queue API
    # -----------------------------------------------------------------------

    async def add_to_queue(self, urls: list[str], site_key: str) -> int:
        """INSERT OR IGNORE URL в очередь.

        Args:
            urls:     Список URL.
            site_key: Ключ источника.

        Returns:
            Число реально добавленных новых URL.
        """
        added = 0
        async with self._locked() as conn:
            for url in urls:
                await conn.execute(
                    "INSERT OR IGNORE INTO queue (url, site_key, status) "
                    "VALUES (?, ?, 'pending')",
                    (url, site_key),
                )
                async with conn.execute("SELECT changes()") as cur:
                    row = await cur.fetchone()
                    if row and row[0]:
                        added += row[0]
            await conn.commit()
        return added

    async def get_next_task(
        self, site_key: str | None = None
    ) -> tuple[str, str] | None:
        """Атомарно берёт следующий pending-URL и переводит в 'processing'.

        Args:
            site_key: Если задан — только для этого источника.

        Returns:
            Кортеж ``(url, site_key)`` или ``None``.
        """
        async with self._locked() as conn:
            if site_key is not None:
                query = """
                    UPDATE queue
                       SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                     WHERE url = (
                         SELECT url FROM queue
                          WHERE status = 'pending' AND site_key = ?
                          LIMIT 1
                     )
                     RETURNING url, site_key
                """
                params: tuple[Any, ...] = (site_key,)
            else:
                query = """
                    UPDATE queue
                       SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                     WHERE url = (
                         SELECT url FROM queue
                          WHERE status = 'pending'
                          LIMIT 1
                     )
                     RETURNING url, site_key
                """
                params = ()

            async with conn.execute(query, params) as cur:
                row = await cur.fetchone()

            if row is None:
                return None

            await conn.commit()
            return str(row[0]), str(row[1])

    async def mark_task_failed(self, url: str, max_attempts: int = 5) -> None:
        """Увеличивает счётчик попыток; при достижении max_attempts → 'error'.

        Args:
            url:          URL задачи.
            max_attempts: Порог перевода в статус 'error'.
        """
        async with self._locked() as conn:
            existing = await self._fetchone(
                conn, "SELECT attempts FROM queue WHERE url = ?", (url,)
            )
            attempts = (existing["attempts"] + 1) if existing else 1
            new_status = "error" if attempts >= max_attempts else "pending"
            await conn.execute(
                "UPDATE queue SET status = ?, attempts = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE url = ?",
                (new_status, attempts, url),
            )
            await conn.commit()

    async def reset_processing_tasks(self, site_key: str | None = None) -> None:
        """Возвращает зависшие 'processing' задачи в 'pending'.

        Args:
            site_key: Если задан — только для конкретного источника.
        """
        async with self._locked() as conn:
            if site_key is not None:
                await conn.execute(
                    "UPDATE queue SET status = 'pending' "
                    "WHERE status = 'processing' AND site_key = ?",
                    (site_key,),
                )
            else:
                await conn.execute(
                    "UPDATE queue SET status = 'pending' WHERE status = 'processing'"
                )
            await conn.commit()

    async def get_pending_tasks_count(self, site_key: str | None = None) -> int:
        """Возвращает количество задач в статусе 'pending'.

        Args:
            site_key: Если задан — только для конкретного источника.

        Returns:
            Число pending-задач.
        """
        async with self._locked() as conn:
            if site_key is not None:
                q = "SELECT COUNT(*) FROM queue WHERE status = 'pending' AND site_key = ?"
                p: tuple[Any, ...] = (site_key,)
            else:
                q = "SELECT COUNT(*) FROM queue WHERE status = 'pending'"
                p = ()
            async with conn.execute(q, p) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    # -----------------------------------------------------------------------
    # Website Contact Scanner API
    # -----------------------------------------------------------------------

    async def get_companies_for_website_scan(
        self,
        limit: int = 0,
        filters: dict[str, Any] | None = None,
    ) -> list[Row]:
        """Возвращает компании с сайтом для сканирования контактов.

        Приоритет: сначала компании без email-контактов, затем остальные.

        Args:
            limit:   Максимум записей (0 — без лимита).
            filters: Словарь ``config.WEBSITE_SCAN_CONFIG`` с фильтрами.

        Returns:
            Список строк с полями ``company_id`` и ``website``.
        """
        where_parts = [
            "c.website IS NOT NULL",
            "c.website != ''",
        ]
        params: list[Any] = []

        if filters:
            if city := filters.get("filter_city"):
                where_parts.append("c.address LIKE ? COLLATE NOCASE")
                params.append(f"%{city}%")

            if site_key := filters.get("filter_site_key"):
                where_parts.append("""
                    EXISTS (
                        SELECT 1 FROM company_sources cs_f
                         WHERE cs_f.company_id = c.company_id
                           AND cs_f.site_key   = ?
                    )
                """)
                params.append(site_key)

            if status := filters.get("filter_status_official"):
                where_parts.append("c.status_official = ?")
                params.append(status)

            # Только компании без email — экономит время при повторных запусках.
            # Компании, у которых email уже найден, пропускаются.
            if filters.get("filter_only_without_email", True):
                where_parts.append("""
                    NOT EXISTS (
                        SELECT 1 FROM company_contacts cc_f
                         WHERE cc_f.company_id   = c.company_id
                           AND cc_f.contact_type = 'email'
                    )
                """)

        limit_sql = f"LIMIT {limit}" if limit > 0 else ""

        async with self._locked() as conn:
            return await self._fetchall(
                conn,
                f"""
                SELECT c.company_id, c.website
                  FROM companies c
                 WHERE {' AND '.join(where_parts)}
                 ORDER BY c.company_id ASC
                 {limit_sql}
                """,
                tuple(params),
            )

    async def save_website_contacts(
        self,
        company_id: int,
        emails: list[str],
        phones: list[str],
    ) -> tuple[int, int]:
        """Сохраняет контакты, найденные на сайте компании.

        Телефоны дедуплицируются по digit-fingerprint против уже имеющихся.
        Email дедуплицируются через ``ON CONFLICT DO NOTHING``.

        Args:
            company_id: PK компании.
            emails:     Список email-адресов (уже валидированных).
            phones:     Список телефонов (уже нормализованных).

        Returns:
            Кортеж ``(new_emails, new_phones)``.
        """
        from datetime import datetime as _dt

        now = _dt.now().strftime("%Y-%m-%d")
        new_emails = 0
        new_phones = 0

        async with self._locked() as conn:
            for email in emails:
                normalized = email.strip().lower()
                if not normalized:
                    continue
                await conn.execute(
                    """
                    INSERT INTO company_contacts
                        (company_id, contact_type, value, first_seen_at, last_seen_at)
                    VALUES (?, 'email', ?, ?, ?)
                    ON CONFLICT(company_id, contact_type, value) DO NOTHING
                    """,
                    (company_id, normalized, now, now),
                )
                async with conn.execute("SELECT changes()") as cur:
                    row = await cur.fetchone()
                    if row and row[0]:
                        new_emails += row[0]

            existing_rows = await self._fetchall(
                conn,
                "SELECT value FROM company_contacts "
                "WHERE company_id = ? AND contact_type = 'phone'",
                (company_id,),
            )
            existing_digits: set[str] = {
                _phone_digits(r["value"]) for r in existing_rows
            }

            for phone in phones:
                digits = _phone_digits(phone)
                if not digits or len(digits) < 7 or digits in existing_digits:
                    continue
                await conn.execute(
                    """
                    INSERT INTO company_contacts
                        (company_id, contact_type, value, first_seen_at, last_seen_at)
                    VALUES (?, 'phone', ?, ?, ?)
                    """,
                    (company_id, phone, now, now),
                )
                existing_digits.add(digits)
                new_phones += 1

            await conn.commit()

        return new_emails, new_phones