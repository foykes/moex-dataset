# %%
### Работа с файлами датасетов по частям, чтобы не держать весь файл в памяти.
### Датасет 30 лет x 1 час с индикаторами целиком в pandas занимает десятки ГБ,
### а по одному тикеру — несколько МБ. Поэтому файлы читаются кусками и обрабатываются по тикерам.
###
### Договорённость: в файле датасета строки одного тикера идут подряд (файл отсортирован по тикеру).
### Так файлы пишут data_gathering и tech. Если это не так — NotGroupedError, файл можно один раз
### отсортировать через sort_dataset_file.
import os
import math
import gzip
import shutil
import datetime
import pandas as pd
import xlsxwriter

# Лимит листа Excel вместе со строкой заголовка
EXCEL_MAX_ROWS = 1048576

# Сколько строк читать из CSV за раз
CHUNK_ROWS = 200000

# Формат дат в CSV (как отдаёт API MOEX)
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Сохранять ли XLSX (по умолчанию да). Отключить: MOEX_XLSX=0
save_xlsx = os.environ.get('MOEX_XLSX', '1') == '1'


class NotGroupedError(Exception):
    """Строки одного тикера в файле идут не подряд."""


# %%
### Чтение файла по тикерам: отдаёт (тикер, DataFrame тикера) по очереди
def iter_ticker_groups(csv_path, columns=None, chunksize=CHUNK_ROWS):
    seen = set()
    rest = None

    for chunk in pd.read_csv(csv_path, usecols=columns, chunksize=chunksize):
        if columns is not None:
            chunk = chunk[columns]  # usecols не сохраняет порядок колонок
        if rest is not None and len(rest) > 0:
            chunk = pd.concat([rest, chunk], ignore_index=True)
        if len(chunk) == 0:
            continue

        # последний тикер куска может продолжиться в следующем куске — откладываем его
        last_ticker = chunk['ticker'].iloc[-1]
        is_last = chunk['ticker'] == last_ticker
        rest = chunk[is_last]

        for ticker, df_ticker in chunk[~is_last].groupby('ticker', sort=False):
            if ticker in seen:
                raise NotGroupedError(ticker)
            seen.add(ticker)
            yield ticker, df_ticker.reset_index(drop=True)

    if rest is not None and len(rest) > 0:
        ticker = rest['ticker'].iloc[0]
        if ticker in seen:
            raise NotGroupedError(ticker)
        yield ticker, rest.reset_index(drop=True)


### Дописать DataFrame в CSV (с заголовком, если файла ещё нет или он пустой)
def append_csv(df, csv_path):
    first = not os.path.isfile(csv_path) or os.path.getsize(csv_path) == 0
    df.to_csv(csv_path, mode='w' if first else 'a', header=first, index=False, date_format=DATE_FORMAT)
    return len(df)


### Отсортировать файл по тикеру и времени (целиком в памяти — только для старых несортированных файлов)
def sort_dataset_file(csv_path):
    print('Сортирую файл по тикерам: {}'.format(csv_path))
    df = pd.read_csv(csv_path)
    df.sort_values(by=['ticker', 'begin'], kind='stable', inplace=True)
    tmp_path = str(csv_path) + '.sort.tmp'
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, csv_path)


### Количество строк данных в CSV (без заголовка)
def count_rows(csv_path):
    rows = 0
    for chunk in pd.read_csv(csv_path, usecols=[0], chunksize=CHUNK_ROWS):
        rows = rows + len(chunk)
    return rows


# %%
### Запись XLSX построчно (xlsxwriter, constant_memory).
### pandas.to_excel через openpyxl держит все ячейки в памяти:
### 16 тыс. строк x 89 колонок = ~600 МБ RAM. Построчно память не растёт (~5 минут на 1 млн строк).
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


def _open_workbook(tmp_path):
    workbook = xlsxwriter.Workbook(tmp_path, {
        'constant_memory': True,
        'default_date_format': 'yyyy-mm-dd hh:mm:ss',
        'strings_to_formulas': False,
        'strings_to_urls': False,
    })
    return workbook, workbook.add_worksheet('Sheet1')


def write_xlsx(df, path):
    """Сохраняет df в path как XLSX (через временный файл). False, если строк больше лимита Excel."""
    if len(df) + 1 > EXCEL_MAX_ROWS:
        return False

    path = str(path)
    tmp_path = path[:-len('.xlsx')] + '.tmp.xlsx'
    workbook, worksheet = _open_workbook(tmp_path)

    worksheet.write_row(0, 0, [str(c) for c in df.columns])
    for row_number, row in enumerate(df.itertuples(index=False, name=None), start=1):
        worksheet.write_row(row_number, 0, [_cell(v) for v in row])

    workbook.close()
    os.replace(tmp_path, path)
    return True


def write_xlsx_from_csv(csv_path, xlsx_path):
    """XLSX из CSV кусками, не читая CSV целиком. False, если строк больше лимита Excel."""
    if count_rows(csv_path) + 1 > EXCEL_MAX_ROWS:
        return False

    xlsx_path = str(xlsx_path)
    tmp_path = xlsx_path[:-len('.xlsx')] + '.tmp.xlsx'
    workbook, worksheet = _open_workbook(tmp_path)

    row_number = 0
    for chunk in pd.read_csv(csv_path, chunksize=CHUNK_ROWS):
        if row_number == 0:
            worksheet.write_row(0, 0, [str(c) for c in chunk.columns])
            row_number = 1
        for row in chunk.itertuples(index=False, name=None):
            worksheet.write_row(row_number, 0, [_cell(v) for v in row])
            row_number = row_number + 1

    workbook.close()
    os.replace(tmp_path, xlsx_path)
    return True


### Сжатая копия CSV (.csv.gz) — для больших датасетов, которые не влезают в XLSX.
### Gzip сжимает CSV в 4–6 раз, это сильно сокращает загрузку на FTP
def write_gzip(csv_path):
    csv_path = str(csv_path)
    gz_path = csv_path + '.gz'
    tmp_path = csv_path + '.tmp.gz'
    with open(csv_path, 'rb') as source, gzip.open(tmp_path, 'wb', compresslevel=6) as target:
        shutil.copyfileobj(source, target, 16 * 1024 * 1024)
    os.replace(tmp_path, gz_path)
    return gz_path


def _remove(path):
    if os.path.isfile(path):
        os.remove(path)


### Файлы для публикации рядом с CSV:
###   влезает в лист Excel -> XLSX (если не выключен MOEX_XLSX=0), сжатой копии нет
###   не влезает           -> .csv.gz вместо XLSX (на FTP уходит только .csv.gz, без большого .csv)
### Устаревшие XLSX/.csv.gz удаляются, чтобы не выложить старые данные рядом с новым CSV.
### Возвращает 'xlsx', 'gz' или 'csv'
def update_published_files(csv_path):
    csv_path = str(csv_path)
    xlsx_path = csv_path[:-len('.csv')] + '.xlsx'
    gz_path = csv_path + '.gz'

    if count_rows(csv_path) + 1 > EXCEL_MAX_ROWS:
        _remove(xlsx_path)
        write_gzip(csv_path)
        print('{}: больше лимита Excel, сохранил сжатую копию .csv.gz'.format(os.path.basename(csv_path)))
        return 'gz'

    _remove(gz_path)
    if save_xlsx and write_xlsx_from_csv(csv_path, xlsx_path):
        return 'xlsx'

    print('XLSX для {} не сохраняю (MOEX_XLSX=0)'.format(os.path.basename(csv_path)))
    _remove(xlsx_path)
    return 'csv'


### Старое имя, оставлено для совместимости
def update_xlsx(csv_path):
    return update_published_files(csv_path) == 'xlsx'


### После записи нового CSV XLSX и .csv.gz рядом уже не соответствуют данным — удаляем
def remove_published_copies(csv_path):
    csv_path = str(csv_path)
    _remove(csv_path[:-len('.csv')] + '.xlsx')
    _remove(csv_path + '.gz')
