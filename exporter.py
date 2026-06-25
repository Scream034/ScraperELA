"""
Скрипт экспорта данных из SQLite в CSV и XLSX (с новой колонкой Статус).
Полностью совместим со строгой типизацией Pyright/Pylance.
"""

import csv
import datetime
import sqlite3
from pathlib import Path

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


def parse_to_date(date_str: str | None) -> datetime.date | None:
    """Безопасно преобразует строковую дату из SQLite в объект datetime.date."""
    if not date_str:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def export_sqlite_to_csv(db_path: Path | str, csv_path: Path | str) -> None:
    """Экспортирует таблицу companies в стандартный CSV."""
    db_path = Path(db_path)
    csv_path = Path(csv_path)

    if not db_path.exists():
        print(f"Ошибка: файл базы данных {db_path} не найден.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Добавлен status в выборку
    cursor.execute("""
        SELECT inn, ogrn, kpp, address, name, legal_name, status, scrape_date, director, phones, email, website, source_url 
        FROM companies
    """)
    rows = cursor.fetchall()

    headers = [
        "ИНН",
        "ОГРН",
        "КПП",
        "Адрес",
        "Название",
        "Юридическое название",
        "Статус работы",
        "Дата запроса",
        "Директор (ФИО)",
        "Контактный телефон(ы)",
        "Email",
        "Сайт",
        "Ссылка на источник",
    ]

    with open(csv_path, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)
        writer.writerows(rows)

    conn.close()


def export_sqlite_to_xlsx(db_path: Path | str, xlsx_path: Path | str) -> None:
    """
    Экспортирует таблицу компаний в профессионально оформленный файл XLSX.
    Включает новую колонку 'Статус работы'.
    """
    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    if not db_path.exists():
        print(f"Ошибка: файл базы данных {db_path} не найден.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Выбираем status
    cursor.execute("""
        SELECT inn, ogrn, kpp, address, name, legal_name, status, scrape_date, director, phones, email, website, source_url 
        FROM companies
    """)
    rows = cursor.fetchall()

    wb = Workbook()
    ws = wb.active
    assert isinstance(ws, Worksheet), "Не удалось инициализировать лист openpyxl"

    ws.title = "Реестр ЧОП"
    ws.sheet_view.showGridLines = True

    # Заголовки колонок
    headers = [
        "ИНН",
        "ОГРН",
        "КПП",
        "Адрес",
        "Название",
        "Юридическое название",
        "Статус работы",
        "Дата запроса",
        "Директор (ФИО)",
        "Контактный телефон(ы)",
        "Email",
        "Сайт",
        "Ссылка на источник",
    ]

    header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    header_fill = PatternFill(
        start_color="1F4E79", end_color="1F4E79", fill_type="solid"
    )
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_align = Alignment(horizontal="left", vertical="center")

    thin_border_side = Side(border_style="thin", color="D3D3D3")
    cell_border = Border(
        left=thin_border_side,
        right=thin_border_side,
        top=thin_border_side,
        bottom=thin_border_side,
    )

    # Пишем шапку
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        assert isinstance(cell, Cell)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_align
        cell.border = cell_border

    ws.row_dimensions[1].height = 28

    # Запись данных
    for row_idx, row_data in enumerate(rows, 2):
        ws.row_dimensions[row_idx].height = 20

        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            assert isinstance(cell, Cell)
            cell.border = cell_border

            header_name = headers[col_idx - 1]

            if header_name in ("Дата запроса", "Дата регистрации"):
                date_val = parse_to_date(value)
                if date_val:
                    cell.value = date_val
                    cell.number_format = "yyyy-mm-dd"
                    cell.alignment = center_align
                else:
                    cell.value = value
                    cell.alignment = center_align

            elif header_name in ("ИНН", "ОГРН", "КПП"):
                cell.value = str(value) if value is not None else ""
                cell.number_format = "@"
                cell.alignment = center_align

            elif header_name == "Статус работы":
                cell.value = value
                cell.alignment = center_align  # Центрируем статус для красоты

            else:
                cell.value = value
                cell.alignment = left_align

    # Авторасширение колонок
    for col in ws.columns:
        max_len = 0
        for cell in col:
            assert isinstance(cell, Cell)
            val = cell.value
            if val is not None:
                if isinstance(val, datetime.date):
                    val_str = val.strftime("%Y-%m-%d")
                else:
                    val_str = str(val)
                max_len = max(max_len, len(val_str))

        first_cell = col[0]
        assert isinstance(first_cell, Cell)
        col_letter_idx = first_cell.column
        assert isinstance(col_letter_idx, int)

        col_letter = get_column_letter(col_letter_idx)
        ws.column_dimensions[col_letter].width = min(max(max_len + 4, 12), 50)

    wb.save(xlsx_path)
    conn.close()
