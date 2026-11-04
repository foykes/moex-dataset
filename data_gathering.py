# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json
from io import StringIO
from pathlib import Path
from dataset_files import iter_ticker_groups, append_csv, sort_dataset_file, count_rows, NotGroupedError, CHUNK_ROWS


today = datetime.datetime.now()
df_full = pd.DataFrame()

# Список неудачных запросов к API (словари: тикер, интервал, даты, страница, ошибка).
# В конце main() сохраняется в datasets/exception_list.csv
exception_list = []

# Одна сессия на все запросы к MOEX: соединение переиспользуется, так быстрее
session = requests.Session()

# Пауза между успешными запросами к API (сек.), чтобы не получить бан от MOEX.
# Раньше было 3 сек.; при бане/ошибках 429 увеличить
request_sleep = 1

# MOEX ISS отдаёт свечи страницами максимум по 500 строк
page_size = 500



def _resolve_current_path() -> Path:
    """Каталог проекта, надёжно и кроссплатформенно (Windows/macOS/Linux).

    Один и тот же исходник работает и как .py, и как .ipynb (jupytext),
    поэтому пробуем источники пути по убыванию надёжности:
      1) __file__   — есть при запуске как обычный .py-скрипт;
                      указывает на сам файл и не зависит от того,
                      откуда запущен python;
      2) __vsc_ipynb_file__ — VS Code прокидывает сюда путь .ipynb,
                      тоже не зависит от рабочей директории;
      3) Path.cwd() — универсальный фолбэк для любого Jupyter-фронтенда
                      (по умолчанию ядро стартует в папке ноутбука).
    pathlib сам подставляет правильный разделитель пути для текущей ОС.
    """
    try:
        return Path(__file__).resolve().parent
    except NameError:
        pass

    notebook_path = globals().get("__vsc_ipynb_file__")
    if notebook_path:
        return Path(notebook_path).resolve().parent

    return Path.cwd().resolve()


current_path = _resolve_current_path()


# %%
header = {'User-Agent': ''}
### Выгрузка header для запроса
json_path = current_path / 'settings' / 'user_agents.json'
with open(json_path, 'r', encoding='utf-8') as f:
    headers_full = json.load(f)

header_first = str(headers_full['chrome'][0])
header['User-Agent'] = header_first

# %%
### Выгрузка конфига файлов
## Какой конфиг использовать, задаётся переменной окружения MOEX_CONFIG.
## По умолчанию 1 год (быстро, для проверки). Полная выгрузка 10/30 лет (на VPS):
##   MOEX_CONFIG=datasets_config.json python main.py
config_name = os.environ.get('MOEX_CONFIG', 'datasets_config_1_year.json')
json_path = current_path / 'settings' / config_name

with open(json_path, 'r', encoding='utf-8') as f:
    config = json.load(f)

print('Конфиг датасетов: {}'.format(config_name))

# %%
### Функция для получения следующего header
def get_next_header(current_header=None):
    global headers_full

    user_agents = []

    # Собираем все User-Agent из headers_full в один список
    # Структура headers_full:
    # {
    #     'chrome': [...],
    #     'edge': [...],
    #     'mozilla': [...],
    #     'opera': [...]
    # }
    for browser_name, agents_list in headers_full.items():
        for user_agent in agents_list:
            user_agents.append(user_agent)

    # Если по какой-то причине список пустой
    if len(user_agents) == 0:
        return current_header if current_header is not None else {}

    # Если текущий header не задан
    if current_header is None:
        current_header = {}

    current_user_agent = current_header.get('User-Agent')

    # Если текущего User-Agent нет в списке — начинаем с первого
    if current_user_agent not in user_agents:
        next_user_agent = user_agents[0]
    else:
        current_index = user_agents.index(current_user_agent)
        next_index = (current_index + 1) % len(user_agents)
        next_user_agent = user_agents[next_index]

    # Сохраняем остальные ключи header, если они есть,
    # и меняем только User-Agent
    new_header = current_header.copy()
    new_header['User-Agent'] = next_user_agent

    return new_header


# %%
### Выгрузка датасета доступного на мосбирже
def moex_tickerlists (current_path):
    CSV_URL = 'https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1'
    global header

    with requests.Session() as s:
        download = s.get(CSV_URL, headers = header)

        decoded_content = download.content.decode('cp1251')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)


    df_moex = pd.DataFrame(my_list)
    new_header = df_moex.iloc[0]
    df_moex = df_moex[1:]
    df_moex.columns = new_header

    ticker_lists_dir = current_path / 'datasets' / 'ticker_lists'
    ticker_lists_dir.mkdir(parents=True, exist_ok=True)

    print("Общее количество объектов на Мосбирже: {}".format(len(df_moex)))
    df_moex.to_excel(ticker_lists_dir / 'moex_full.xlsx')
    df_moex.to_csv(ticker_lists_dir / 'moex_full.csv')

    df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")]
    df_moex_stocks.reset_index(drop=True, inplace=True)

    ## moex_stocks_list['CURRENCY'] == '' это заблокированные акции

    print("Количество акций и депозитарных расписок: {}".format(len(df_moex_stocks)))
    # df_moex_stocks.to_excel(ticker_lists_dir / 'moex_stocks.xlsx')
    df_moex_stocks.to_csv(ticker_lists_dir / 'moex_stocks.csv')

    ## Запуск только для акций и депозитарных расписок
    all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
    all_stocks_ru = all_stocks_ru.loc[~all_stocks_ru.duplicated(), :]

    ## Запуск для всего
    # all_stocks_ru = df_moex.filter(['TRADE_CODE'], axis = 1)
    # all_stocks_ru = df_moex.loc[~all_stocks_ru.duplicated(), :]

    df_moex_stocks.reset_index(drop=True, inplace=True)

    return all_stocks_ru

# %%
### Функция запроса одной страницы API с ретраями и сменой User-Agent
## Возвращает (df, error): df — DataFrame страницы или None, если все попытки неуспешны
def moex_query_page(query, max_retries=5, retry_sleep_start=3):
    global header

    # Если global header почему-то не задан или пустой —
    # инициализируем первым User-Agent из headers_full
    if not isinstance(header, dict) or len(header) == 0:
        header = get_next_header(None)

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(query, headers=header, timeout=30)

            # Читаем CSV если статус успешный
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text), sep=';', header=1)

                # Пауза после успешного запроса, чтобы не перегружать API Мосбиржи
                time.sleep(request_sleep)
                return df, None

            last_error = 'status code {}'.format(response.status_code)

        except Exception as e:
            last_error = str(e)

        print('Попытка {}/{} неуспешна: {}'.format(attempt, max_retries, last_error))
        print(query)

        # Если попытка неуспешна — меняем header и ждём перед следующей попыткой
        if attempt < max_retries:
            old_user_agent = header.get('User-Agent') if isinstance(header, dict) else None
            header = get_next_header(header)
            new_user_agent = header.get('User-Agent') if isinstance(header, dict) else None

            print('Меняем User-Agent:')
            print(f'old: {old_user_agent}')
            print(f'new: {new_user_agent}')

            # Экспоненциальная пауза после неудачной попытки:
            # 1-я ошибка -> 3 сек., 2-я -> 6 сек., 3-я -> 12 сек., 4-я -> 24 сек.
            sleep_time = retry_sleep_start * (2 ** (attempt - 1))
            print(f'Ждём {sleep_time} сек. перед следующей попыткой...')
            time.sleep(sleep_time)

    return None, last_error


### Функция запроса к API по тикеру, датам и нужному интервалу
## MOEX отдаёт максимум 500 свечей за запрос, поэтому листаем страницы через &start=0, 500, 1000...
## пока страница не придёт неполной (меньше 500 строк)
def moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval, max_retries=5, retry_sleep_start=3):
    global exception_list

    df_ticker = pd.DataFrame()

    # Определение типа market для корректного запроса
    if ticker_type in ['Инвестиционные паи', 'Депозитарные расписки', 'Акции', 'Ипотечные сертификаты участия']:
        market = "shares"
    elif ticker_type in ['Облигации', 'Еврооблигации']:
        market = "bonds"
    else:
        market = "shares"

    query = f'https://iss.moex.com/iss/engines/stock/markets/{market}/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #универсальный шаблон

    pages = []
    start = 0
    last_begin = None

    while True:
        df, error = moex_query_page(query + '&start={}'.format(start), max_retries, retry_sleep_start)

        if df is None:
            # Страница не скачалась после всех попыток. То, что уже скачано, оставляем:
            # это непрерывный кусок от начала периода, а data_update в следующий раз
            # продолжит с последней даты этого тикера
            print('Не удалось получить данные после {} попыток. ticker: {}, interval: {}, from: {}, till: {}, start: {}, ошибка: {}'.format(
                max_retries, ticker_in, interval, end_date_mx, start_date_mx, start, error))
            exception_list.append({
                'ticker': ticker_in,
                'interval': interval,
                'from': end_date_mx,
                'till': start_date_mx,
                'start': start,
                'error': error,
            })
            break

        if len(df) == 0:
            break

        # Защита от бесконечного цикла: если API вернул ту же страницу ещё раз
        if last_begin is not None and df['begin'].iloc[-1] == last_begin:
            break
        last_begin = df['begin'].iloc[-1]

        pages.append(df)

        # Неполная страница — значит это последняя
        if len(df) < page_size:
            break

        start = start + page_size

    if len(pages) > 0:
        df_ticker = pd.concat(pages, ignore_index=True)
        df_ticker['ticker'] = ticker_in

    return df_ticker


# %%
## Функция выгрузки данных через ручку MOEX
def moex (ticker_in, ticker_type, years, interval):

    df_ticker = pd.DataFrame()

    df = pd.DataFrame()
    global exception_list
    today = datetime.datetime.now()

    for i in range(1, years):

        if i == 1:
            start_date = today
        else:
            d_s = datetime.timedelta(days = 365*(i-1))
            start_date = today - d_s

        d_e = datetime.timedelta(days = 365*i)
        end_date = today - d_e

        start_date_mx = start_date.strftime('%Y-%m-%d')
        end_date_mx = end_date.strftime('%Y-%m-%d')

        try:
            df = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
            if len(df) > 0: df_ticker = pd.concat([df_ticker,df])
        except Exception as e:
            exception_list.append({'ticker': ticker_in, 'interval': interval, 'from': end_date_mx,
                                   'till': start_date_mx, 'start': None, 'error': str(e)})

    return df_ticker

# %%
## Функция выгружает даты торгов по каждому тикеру (когда начали торговать по этому тикеру и конца закончили)
## полезно, чтобы перестать выгружать данные по тем тикерам, которые уже делистили
## Запрос JSON к ISS: 3 попытки с паузой, чтобы одна сетевая ошибка не оставляла тикер без дат
def iss_get_json(session, url, params, attempts=3):
    for attempt in range(1, attempts + 1):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(3 * attempt)


def build_tickers_dates(all_stocks_ru, current_path):
    """
    По уникальным тикерам из all_stocks_ru['TRADE_CODE'] получает:
    - дату начала торгов (ISSUEDATE) => issue_date
    - дату последнего дня торгов, если бумага больше не торгуется => stopped_date

    Возвращает DataFrame tickers_dates с колонками:
    ['TRADE_CODE', 'issue_date', 'stopped_date']
    """

    session = requests.Session()

    # Базовые URL'ы
    desc_url = "https://iss.moex.com/iss/securities/{secid}.json"
    securities_url = "https://iss.moex.com/iss/securities.json"
    history_url = (
        "https://iss.moex.com/iss/history/engines/stock/markets/shares/"
        "boards/{board}/securities/{secid}.json"
    )

    # Берём уникальные тикеры, убираем NaN и пустоты, приводим к верхнему регистру
    tickers = (
        all_stocks_ru["TRADE_CODE"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    tickers = tickers[tickers != ""].str.upper().unique()

    rows = []

    for secid in tickers:
        issue_date = pd.NaT
        stopped_date = pd.NaT
        is_traded = None
        primary_boardid = None

        # --- 1. Дата начала торгов (ISSUEDATE) из description ---
        try:
            params_desc = {
                "iss.meta": "off",
                "iss.only": "description",
                "description.columns": "name,value",
            }
            j = iss_get_json(session, desc_url.format(secid=secid), params_desc)

            desc = j.get("description", {})
            cols = desc.get("columns", [])
            data = desc.get("data", [])

            if "name" in cols and "value" in cols:
                name_idx = cols.index("name")
                value_idx = cols.index("value")

                for row_ in data:
                    if row_[name_idx] == "ISSUEDATE":
                        date_str = row_[value_idx]
                        if date_str:
                            issue_date = pd.to_datetime(date_str)
                        break
        except Exception:
            # если что-то пошло не так — оставляем issue_date = NaT
            pass

        # --- 2. is_traded и primary_boardid из /iss/securities ---
        try:
            params_sec = {
                "q": secid,
                "iss.meta": "off",
                "iss.only": "securities",
                "securities.columns": "secid,group,is_traded,primary_boardid",
            }
            j = iss_get_json(session, securities_url, params_sec)

            sec = j.get("securities", {})
            cols = sec.get("columns", [])
            data = sec.get("data", [])

            if all(c in cols for c in ("secid", "group", "is_traded", "primary_boardid")):
                secid_idx = cols.index("secid")
                group_idx = cols.index("group")
                is_traded_idx = cols.index("is_traded")
                pb_idx = cols.index("primary_boardid")

                for row_ in data:
                    # выбираем акцию или депозитарную расписку (раньше расписки stock_dr не находились) и нужный SECID
                    if str(row_[secid_idx]).upper() == secid and row_[group_idx] in ("stock_shares", "stock_dr"):
                        is_traded = row_[is_traded_idx]
                        primary_boardid = row_[pb_idx]
                        break
        except Exception:
            pass

        # --- 3. Если бумага больше не торгуется (is_traded == 0), берём последний день торгов ---
        if primary_boardid and is_traded == 0:
            try:
                params_hist = {
                    "iss.meta": "off",
                    "iss.only": "history",
                    "history.columns": "TRADEDATE",
                    "sort_column": "TRADEDATE",
                    "sort_order": "desc",
                    "limit": 1,
                }
                j = iss_get_json(session, history_url.format(board=primary_boardid, secid=secid), params_hist)

                hist = j.get("history", {})
                cols = hist.get("columns", [])
                data = hist.get("data", [])

                if "TRADEDATE" in cols and data:
                    td_idx = cols.index("TRADEDATE")
                    date_str = data[0][td_idx]
                    if date_str:
                        stopped_date = pd.to_datetime(date_str)
            except Exception:
                # если история не доступна — оставляем NaT
                pass

        rows.append(
            {
                "TRADE_CODE": secid,
                "issue_date": issue_date,
                # Для торгуемых бумаг будет NaT, для делистнутых — дата последнего дня торгов
                "stopped_date": stopped_date,
            }
        )

    tickers_dates = pd.DataFrame(rows)
    print('Даты тикеров: всего {}, без даты листинга {}, делистнуто {}'.format(
        len(tickers_dates), tickers_dates['issue_date'].isna().sum(), tickers_dates['stopped_date'].notna().sum()))

    ## Сохранение файлов
    ticker_lists_dir = current_path / 'datasets' / 'ticker_lists'
    ticker_lists_dir.mkdir(parents=True, exist_ok=True)
    tickers_dates.to_excel(ticker_lists_dir / 'tickers_dates.xlsx')
    tickers_dates.to_csv(ticker_lists_dir / 'tickers_dates.csv')

    return tickers_dates


# %%
### Вспомогательные функции для выгрузки и обновления датасетов

# Колонки свечей в файле датасета (без индикаторов, их добавляет tech.py)
base_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']


## Безопасное приведение колонки к datetime.
## Сначала разбираем ISO 'YYYY-MM-DD HH:MM:SS' (из API и наших файлов).
## Важно: format='mixed' + dayfirst=True для ISO-дат меняет местами день и месяц
## (2026-11-03 -> 2026-03-11), поэтому так разбираем только то, что не разобралось
## как ISO — старые даты вида 'DD/MM/YYYY HH:MM'.
## errors='coerce' превращает непарсящиеся значения в NaT.
def to_datetime_safe(series):
    result = pd.to_datetime(series, format='ISO8601', errors='coerce')
    not_parsed = result.isna() & series.notna()
    if not_parsed.any():
        result[not_parsed] = pd.to_datetime(series[not_parsed], format='mixed', dayfirst=True, errors='coerce')
    return result


def normalize_dates(df):
    for dt_col in ('begin', 'end'):
        if dt_col in df.columns:
            df[dt_col] = to_datetime_safe(df[dt_col])
    return df


## Тип бумаги из полного каталога MOEX (нужен moex_query для выбора market)
def get_ticker_type(ticker_in, moex_full_catalogue):
    ticker_types = moex_full_catalogue[moex_full_catalogue['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values
    if len(ticker_types) == 0:
        return 'Акции'
    return ticker_types[0]


## Период выгрузки тикера:
## левая граница — самая ранняя нужная дата (today - years) или дата листинга, если она позже
## правая граница — сегодня или последний день торгов, если бумагу делистили
## years = 0 означает всю доступную историю
def ticker_date_range(ticker_in, years, tickers_dates, today):
    if years != 0:
        end_date_mx = (today - datetime.timedelta(days=years*365)).strftime('%Y-%m-%d')
    else:
        end_date_mx = '1991-12-26' #если даты листинга нет, то берём начало истории
    start_date_mx = today.strftime('%Y-%m-%d')

    ticker_dates = tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]
    if len(ticker_dates) > 0:
        issue_date = ticker_dates['issue_date'].iloc[0]
        stopped_date = ticker_dates['stopped_date'].iloc[0]

        # раньше при пустой дате листинга в запрос уходило from=NaT
        if pd.notna(issue_date) and str(issue_date)[:10] > end_date_mx:
            end_date_mx = str(issue_date)[:10]
        if pd.notna(stopped_date):
            start_date_mx = str(stopped_date)[:10]

    return end_date_mx, start_date_mx


## Оставляем только последние years лет (years = 0 — всё)
def cut_years(df, years, today):
    if years == 0 or len(df) == 0:
        return df
    cutoff = today - datetime.timedelta(days=years*365)
    return df[df['begin'] >= cutoff].reset_index(drop=True)


## Свечи одного тикера (старые + новые) -> готовые строки для файла:
## даты в datetime, без дублей, только нужный период, по порядку времени.
## Дубли по (тикер, начало свечи): последняя свеча прошлой выгрузки могла быть
## незавершённой, поэтому оставляем свежую версию из нового запроса
def prepare_ticker_rows(parts, years, today):
    parts = [part for part in parts if len(part) > 0]
    if len(parts) == 0:
        return pd.DataFrame(columns=base_columns)

    df = pd.concat(parts, ignore_index=True)
    df = df[base_columns]
    df = normalize_dates(df)
    df = df.drop_duplicates(subset=['ticker', 'begin'], keep='last')
    df = cut_years(df, years, today)
    df = df.sort_values(by='begin').reset_index(drop=True)
    return df


## После записи нового CSV старый XLSX рядом уже не соответствует данным — удаляем.
## Новый XLSX (уже с индикаторами) запишет tech.py
def remove_stale_xlsx(csv_path):
    xlsx_path = csv_path.with_suffix('.xlsx')
    if xlsx_path.is_file():
        xlsx_path.unlink()


## Короткий период (например 10 лет) вырезаем из файла длинного периода (30 лет) того же интервала,
## читая длинный файл кусками
def cut_dataset_file(long_csv_path, filename, years, today, current_path):
    csv_path = current_path / 'datasets' / (filename + '.csv')
    tmp_path = csv_path.with_name(filename + '.tmp.csv')
    if tmp_path.is_file():
        tmp_path.unlink()

    # даты в файле в ISO-формате, поэтому можно сравнивать строки
    cutoff = (today - datetime.timedelta(days=years*365)).strftime('%Y-%m-%d %H:%M:%S')
    rows = 0
    for chunk in pd.read_csv(long_csv_path, chunksize=CHUNK_ROWS, usecols=base_columns):
        chunk = chunk[base_columns]
        if years != 0:
            chunk = chunk[chunk['begin'].astype(str) >= cutoff]
        rows = rows + append_csv(chunk, tmp_path)

    if rows == 0:
        print('Датасет {} пустой, файл не сохраняю'.format(filename))
        if tmp_path.is_file():
            tmp_path.unlink()
        return 0

    os.replace(tmp_path, csv_path)
    remove_stale_xlsx(csv_path)
    print("{} вырезан из {}: {} записей".format(filename, long_csv_path.name, rows))
    return rows


# %%
### Функция для выгрузки данных с нуля.
### Тикеры выгружаются по алфавиту и сразу дописываются в файл <filename>.partial.csv,
### поэтому в памяти только один тикер. Если выгрузка прервалась (а полная может идти днями),
### следующий запуск продолжит с места остановки: готовые тикеры записаны в <filename>.partial.progress
def full_reload (all_stocks_ru, interval, years, filename, word, current_path, tickers_dates, moex_full_catalogue = None):
    today = datetime.datetime.now()
    current_path = Path(current_path)

    if moex_full_catalogue is None:
        moex_full_catalogue = pd.read_csv(current_path / 'datasets' / 'ticker_lists' / 'moex_full.csv', index_col=0)

    datasets_dir = current_path / 'datasets'
    datasets_dir.mkdir(parents=True, exist_ok=True)
    csv_path = datasets_dir / (filename + '.csv')
    partial_path = datasets_dir / (filename + '.partial.csv')
    progress_path = datasets_dir / (filename + '.partial.progress')

    ## Продолжение прерванной выгрузки: в progress строки "тикер,размер partial-файла после него"
    done_tickers = set()
    if partial_path.is_file() and progress_path.is_file():
        progress = pd.read_csv(progress_path, names=['ticker', 'size'], keep_default_na=False)
        if len(progress) > 0:
            done_tickers = set(progress['ticker'])
            # обрезаем недописанный хвост, если запуск прервался посреди записи тикера
            with open(partial_path, 'r+b') as f:
                f.truncate(int(progress['size'].iloc[-1]))
            print('Продолжаю прерванную выгрузку {}: уже готово тикеров {}'.format(filename, len(done_tickers)))
    else:
        for path in (partial_path, progress_path):
            if path.is_file():
                path.unlink()

    tickers = [t for t in all_stocks_ru['TRADE_CODE'].to_list() if pd.notna(t) and str(t).strip() != '']
    tickers = sorted(set(tickers))

    for i in range(0,len(tickers)):
        ticker_in = tickers[i]
        if ticker_in in done_tickers:
            continue

        ticker_type = get_ticker_type(ticker_in, moex_full_catalogue)
        end_date_mx, start_date_mx = ticker_date_range(ticker_in, years, tickers_dates, today)

        df = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
        df = prepare_ticker_rows([df], years, today)
        if len(df) > 0:
            append_csv(df, partial_path)

        # отмечаем тикер готовым только после записи его строк
        size = partial_path.stat().st_size if partial_path.is_file() else 0
        with open(progress_path, 'a') as f:
            f.write('{},{}\n'.format(ticker_in, size))

        print('[{}/{}] {}: {} записей ({} - {})'.format(i + 1, len(tickers), ticker_in, len(df), end_date_mx, start_date_mx))

    if not partial_path.is_file() or partial_path.stat().st_size == 0:
        print("Записей для промежутка {} лет с интервалом {} {}.: 0, файл не сохраняю".format(years, interval, word))
        for path in (partial_path, progress_path):
            if path.is_file():
                path.unlink()
        return 0

    rows = count_rows(partial_path)
    os.replace(partial_path, csv_path)
    progress_path.unlink()
    remove_stale_xlsx(csv_path)

    print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, rows))
    return rows


# %%
### Обновление одного файла датасета: читаем его по тикерам, к каждому тикеру докачиваем
### свечи с его последней даты, новые тикеры выгружаем за весь период. Пишем в новый файл
### сразу по тикеру, поэтому в памяти только один тикер.
### Обновляются только тикеры из актуального списка MOEX (делистнутые не трогаем)
def update_dataset_file(dataset_path, interval, years, today, ticker_list_actual, tickers_dates, moex_full_catalogue):
    ## 1. последняя дата каждого тикера в файле (читаем только 2 колонки)
    last_dates = {}
    for chunk in pd.read_csv(dataset_path, usecols=['ticker', 'end'], chunksize=CHUNK_ROWS):
        chunk['end'] = to_datetime_safe(chunk['end'])
        for ticker_in, end in chunk.groupby('ticker')['end'].max().items():
            if ticker_in not in last_dates or end > last_dates[ticker_in]:
                last_dates[ticker_in] = end

    ### Новые тикеры (появились с момента последнего обновления) выгружаем за весь период датасета
    actual = set(ticker_list_actual)
    new_tickers = sorted(actual - set(last_dates))
    if len(new_tickers) > 0:
        print('Новых тикеров: {}'.format(len(new_tickers)))

    tmp_path = dataset_path.with_name(dataset_path.stem + '.tmp.csv')
    if tmp_path.is_file():
        tmp_path.unlink()

    rows_before = 0
    rows_after = 0
    new_index = 0
    start_date_mx = today.strftime('%Y-%m-%d')

    ## 2. по тикерам: старые строки + докачка -> новый файл
    for ticker_in, df_old in iter_ticker_groups(dataset_path, base_columns):

        # новые тикеры, которые по алфавиту раньше текущего — чтобы файл остался отсортированным
        while new_index < len(new_tickers) and new_tickers[new_index] < str(ticker_in):
            new_ticker = new_tickers[new_index]
            end_date_mx, new_start_date_mx = ticker_date_range(new_ticker, years, tickers_dates, today)
            df_new = moex_query(new_ticker, get_ticker_type(new_ticker, moex_full_catalogue), end_date_mx, new_start_date_mx, interval)
            rows_after = rows_after + append_csv(prepare_ticker_rows([df_new], years, today), tmp_path)
            print('Новый тикер {}: {} записей'.format(new_ticker, len(df_new)))
            new_index = new_index + 1

        rows_before = rows_before + len(df_old)
        parts = [df_old]

        last_end = last_dates.get(ticker_in)
        if ticker_in in actual and pd.notna(last_end):
            end_date_mx = last_end.strftime('%Y-%m-%d')
            df_new = moex_query(ticker_in, get_ticker_type(ticker_in, moex_full_catalogue), end_date_mx, start_date_mx, interval)
            parts.append(df_new)
            print('{}: +{} записей'.format(ticker_in, len(df_new)))

        rows_after = rows_after + append_csv(prepare_ticker_rows(parts, years, today), tmp_path)

    ## 3. оставшиеся новые тикеры (по алфавиту после последнего тикера файла)
    while new_index < len(new_tickers):
        new_ticker = new_tickers[new_index]
        end_date_mx, new_start_date_mx = ticker_date_range(new_ticker, years, tickers_dates, today)
        df_new = moex_query(new_ticker, get_ticker_type(new_ticker, moex_full_catalogue), end_date_mx, new_start_date_mx, interval)
        rows_after = rows_after + append_csv(prepare_ticker_rows([df_new], years, today), tmp_path)
        print('Новый тикер {}: {} записей'.format(new_ticker, len(df_new)))
        new_index = new_index + 1

    if rows_after == 0:
        print('После обновления {} пустой, оставляю старый файл'.format(dataset_path.name))
        if tmp_path.is_file():
            tmp_path.unlink()
        return rows_before, rows_before

    os.replace(tmp_path, dataset_path)
    remove_stale_xlsx(dataset_path)
    return rows_before, rows_after


### Функция для обновления текущих датасетов по конфигу
## force_reload = True — все файлы выгружаются с нуля
def data_update (config, current_path, all_stocks_ru, tickers_dates, force_reload = False):
    today = datetime.datetime.now()
    current_path = Path(current_path)
    moex_full_catalogue = pd.read_csv(current_path / 'datasets' / 'ticker_lists' / 'moex_full.csv', index_col=0)

    datasets_dir = current_path / 'datasets'

    # актуальный список тикеров MOEX (без пустых/NaN)
    ticker_list_actual = list(set(all_stocks_ru['TRADE_CODE'].to_list()))
    ticker_list_actual = [t for t in ticker_list_actual if pd.notna(t) and str(t).strip() != '']

    # Порядок: по интервалу, внутри интервала сначала самый длинный период.
    # Короткий период (10 лет) потом вырезаем из длинного (30 лет) без повторного скачивания.
    # years = 0 (вся история) считаем самым длинным
    config_sorted = sorted(config, key=lambda c: (c['interval'], -(c['years'] if c['years'] != 0 else 10**6)))

    loaded_interval = None
    loaded_years = None
    loaded_path = None

    for j in range(0,len(config_sorted)):
        filename_j = config_sorted[j]['filename']
        interval = config_sorted[j]['interval']
        years = config_sorted[j]['years']
        word = config_sorted[j]['word']

        ## Этот интервал уже обработан за более длинный период — просто вырезаем нужные годы
        if loaded_interval == interval and (loaded_years == 0 or loaded_years >= years) and loaded_path.is_file():
            cut_dataset_file(loaded_path, filename_j, years, today, current_path)
            continue

        dataset_path = datasets_dir / "{}.csv".format(filename_j)

        if force_reload or not dataset_path.is_file():
            if not force_reload:
                print("Файла не существует, не могу его обновить: \n{}".format(dataset_path))
                print("Начинаю выгружать его с нуля")
            full_reload (all_stocks_ru, interval, years, filename_j, word, current_path, tickers_dates, moex_full_catalogue)

        else:
            print("Обновляю {}".format(filename_j))
            try:
                rows_before, rows_after = update_dataset_file(dataset_path, interval, years, today, ticker_list_actual, tickers_dates, moex_full_catalogue)
            except NotGroupedError:
                # старый файл, где строки тикера идут не подряд — один раз сортируем и обновляем
                sort_dataset_file(dataset_path)
                rows_before, rows_after = update_dataset_file(dataset_path, interval, years, today, ticker_list_actual, tickers_dates, moex_full_catalogue)
            print("Длина {} до обновления {}, после обновления {}".format(filename_j, rows_before, rows_after))

        # запоминаем обработанный интервал, чтобы вырезать из него более короткие периоды
        loaded_interval = interval
        loaded_years = years
        loaded_path = dataset_path


# %%
def main(current_path, force_reload = False):
    global exception_list
    global config

    # На случай, если main вызвали со строковым путём (например, из «старой»
    # переменной current_path, оставшейся в ядре Jupyter) — приводим к Path.
    # Path(Path(...)) идемпотентен, так что уже готовый Path тоже пройдёт без вреда.
    current_path = Path(current_path)

    all_stocks_ru = moex_tickerlists (current_path)
    all_stocks_ru.reset_index(drop=True, inplace=True)

    tickers_dates = build_tickers_dates(all_stocks_ru, current_path)
    
    ## Если нужно с нуля перевыгрузить данные, то необязательный параметр force_reload нужно передать как True
    data_update(config, current_path, all_stocks_ru, tickers_dates, force_reload)


    ## Отчёт по неудачным запросам: печатаем и сохраняем в CSV, чтобы понимать что и когда не выгрузилось
    df_exceptions = pd.DataFrame(exception_list, columns=['ticker', 'interval', 'from', 'till', 'start', 'error'])
    df_exceptions.drop_duplicates(inplace=True)
    print("Неудачных запросов: {}, тикеров: {}".format(len(df_exceptions), df_exceptions['ticker'].nunique()))
    df_exceptions.to_csv(current_path / 'datasets' / 'exception_list.csv', index=False)

# %%
if __name__ == "__main__":
    main(current_path, force_reload = False)
    # main(current_path, force_reload = True)


