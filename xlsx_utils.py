# %%
### Запись XLSX построчно (xlsxwriter, constant_memory).
### pandas.to_excel через openpyxl держит все ячейки в памяти:
### 16 тыс. строк x 89 колонок = ~600 МБ RAM, а файл на 1 млн строк не влезет ни в какой VPS.
### Построчная запись держит в памяти только текущую строку (~5 минут на 1 млн строк).
import os
import math
import datetime
import pandas as pd
import xlsxwriter

# Лимит листа Excel вместе со строкой заголовка
EXCEL_MAX_ROWS = 1048576


def _cell(value):
    # пустые значения (NaN/NaT/None) пишем пустой ячейкой
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    # numpy-числа приводим к обычным python-числам
    if hasattr(value, 'item') and not isinstance(value, (str, datetime.datetime)):
        return value.item()
    return value


def write_xlsx(df, path):
    """Сохраняет df в path как XLSX (через временный файл). Возвращает False, если строк больше лимита Excel."""
    if len(df) + 1 > EXCEL_MAX_ROWS:
        return False

    path = str(path)
    tmp_path = path[:-len('.xlsx')] + '.tmp.xlsx'

    workbook = xlsxwriter.Workbook(tmp_path, {
        'constant_memory': True,
        'default_date_format': 'yyyy-mm-dd hh:mm:ss',
        'strings_to_formulas': False,
        'strings_to_urls': False,
    })
    worksheet = workbook.add_worksheet('Sheet1')

    worksheet.write_row(0, 0, [str(c) for c in df.columns])
    for row_number, row in enumerate(df.itertuples(index=False, name=None), start=1):
        worksheet.write_row(row_number, 0, [_cell(v) for v in row])

    workbook.close()
    os.replace(tmp_path, path)
    return True
