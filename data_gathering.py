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
from xlsx_utils import write_xlsx


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

# Сохранять ли XLSX (по умолчанию да). Отключить: MOEX_XLSX=0
save_xlsx = os.environ.get('MOEX_XLSX', '1') == '1'


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
            r = session.get(
                desc_url.format(secid=secid),
                params=params_desc,
                timeout=5,
            )
            r.raise_for_status()
            j = r.json()

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
            r = session.get(
                securities_url,
                params=params_sec,
                timeout=5,
            )
            r.raise_for_status()
            j = r.json()

            sec = j.get("securities", {})
            cols = sec.get("columns", [])
            data = sec.get("data", [])

            if all(c in cols for c in ("secid", "group", "is_traded", "primary_boardid")):
                secid_idx = cols.index("secid")
                group_idx = cols.index("group")
                is_traded_idx = cols.index("is_traded")
                pb_idx = cols.index("primary_boardid")

                for row_ in data:
                    # выбираем именно акцию (group == 'stock_shares') и нужный SECID
                    if str(row_[secid_idx]).upper() == secid and row_[group_idx] == "stock_shares":
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
                r = session.get(
                    history_url.format(board=primary_boardid, secid=secid),
                    params=params_hist,
                    timeout=5,
                )
                r.raise_for_status()
                j = r.json()

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

    ## Сохранение файлов
    ticker_lists_dir = current_path / 'datasets' / 'ticker_lists'
    ticker_lists_dir.mkdir(parents=True, exist_ok=True)
    tickers_dates.to_excel(ticker_lists_dir / 'tickers_dates.xlsx')
    tickers_dates.to_csv(ticker_lists_dir / 'tickers_dates.csv')

    return tickers_dates


# %%
### Вспомогательные функции для выгрузки и обновления датасетов

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


## Сохранение датасета: CSV всегда, XLSX только если влезает в лист Excel (1 048 576 строк с заголовком).
## Пишем во временный файл и потом подменяем, чтобы при падении не остался битый файл
def save_dataset(df, filename, current_path):
    datasets_dir = current_path / 'datasets'
    datasets_dir.mkdir(parents=True, exist_ok=True)

    if len(df) == 0:
        print('Датасет {} пустой, файл не сохраняю'.format(filename))
        return

    # XLSX пишется построчно (xlsx_utils), чтобы не съесть всю память
    xlsx_path = datasets_dir / (filename + '.xlsx')
    xlsx_saved = save_xlsx and write_xlsx(df, xlsx_path)
    if not xlsx_saved:
        print('XLSX для {} не сохраняю ({} строк)'.format(filename, len(df)))
        # старый xlsx больше не соответствует csv — удаляем, чтобы не выложить устаревшие данные
        if xlsx_path.is_file():
            xlsx_path.unlink()

    csv_path = datasets_dir / (filename + '.csv')
    tmp_path = datasets_dir / (filename + '.tmp.csv')
    # date_format: полночь не превращается в дату без времени, формат как у API
    df.to_csv(tmp_path, index = False, date_format='%Y-%m-%d %H:%M:%S')
    os.replace(tmp_path, csv_path)


# %%
### Функция для выгрузки данных с нуля
def full_reload (all_stocks_ru, interval, years, filename, word, current_path, tickers_dates, moex_full_catalogue = None):
    today = datetime.datetime.now()
    parts = []

    if moex_full_catalogue is None:
        moex_full_catalogue = pd.read_csv(current_path / 'datasets' / 'ticker_lists' / 'moex_full.csv', index_col=0)

    for i in range(0,len(all_stocks_ru)):
        ticker_in = all_stocks_ru['TRADE_CODE'][i]

        if len(ticker_in) > 0: #проверка что тикер выгрузился и есть
            ticker_type = get_ticker_type(ticker_in, moex_full_catalogue)
            end_date_mx, start_date_mx = ticker_date_range(ticker_in, years, tickers_dates, today)

            df = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
            if len(df) > 0: parts.append(df)
            print('[{}/{}] {}: {} записей ({} - {})'.format(i + 1, len(all_stocks_ru), ticker_in, len(df), end_date_mx, start_date_mx))
        else:
            print(ticker_in)

    if len(parts) > 0:
        df_full = pd.concat(parts, ignore_index=True)
    else:
        df_full = pd.DataFrame()
    df_full = normalize_dates(df_full)

    print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, len(df_full)))
    save_dataset(df_full, filename, current_path)

    return df_full


# %%
### Функция для обновления текущих датасетов по конфигу
## force_reload = True — все файлы выгружаются с нуля
def data_update (config, current_path, all_stocks_ru, tickers_dates, force_reload = False):
    today = datetime.datetime.now()
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
    loaded_df = None

    for j in range(0,len(config_sorted)):
        filename_j = config_sorted[j]['filename']
        interval = config_sorted[j]['interval']
        years = config_sorted[j]['years']
        word = config_sorted[j]['word']

        ## Этот интервал уже скачан за более длинный период — просто вырезаем нужные годы
        if loaded_interval == interval and (loaded_years == 0 or loaded_years >= years):
            df = cut_years(loaded_df, years, today)
            print("{} вырезан из периода {} лет: {} записей".format(filename_j, loaded_years, len(df)))
            save_dataset(df, filename_j, current_path)
            continue

        dataset_path = datasets_dir / "{}.csv".format(filename_j)

        if force_reload or not dataset_path.is_file():
            if not force_reload:
                print("Файла не существует, не могу его обновить: \n{}".format(dataset_path))
                print("Начинаю выгружать его с нуля")
            df = full_reload (all_stocks_ru, interval, years, filename_j, word, current_path, tickers_dates, moex_full_catalogue)

        else:
            df = pd.read_csv(dataset_path)
            print("Длина {} до обновления {}".format(filename_j, len(df)))

            # убираем колонки теханализа - их потом с нуля пересчитает tech.py
            white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
            columns_to_remove = [i for i in df.columns if i not in white_list_columns]
            df.drop(columns_to_remove, axis=1, inplace=True)

            # Нормализуем даты загруженного файла к datetime, иначе сортировка и
            # сравнение строк ниже работают неверно
            df = normalize_dates(df)

            # для каждого тикера выбираем последнюю дату за которую есть выгрузка
            df_last_date = df.sort_values(by=['end']).drop_duplicates(subset='ticker', keep='last')
            df_last_date = df_last_date.loc[:,['end','ticker']]
            df_last_date = df_last_date.dropna(subset=['end'])
            df_last_date.reset_index(inplace=True,drop=True)

            # обновляем только тикеры, которые сейчас есть в списке MOEX (делистнутые не трогаем)
            df_last_date = df_last_date[df_last_date['ticker'].isin(ticker_list_actual)]

            parts = [df]

            # обновление текущих данных: с последней даты тикера по сегодня
            for i in range(0,len(df_last_date)):
                ticker_in = df_last_date.iloc[i]['ticker']
                ticker_type = get_ticker_type(ticker_in, moex_full_catalogue)

                end_date_mx = df_last_date.iloc[i]['end'].strftime('%Y-%m-%d')
                start_date_mx = today.strftime('%Y-%m-%d')

                df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                parts.append(df_ticker)
                print('[{}/{}] {}: +{} записей'.format(i + 1, len(df_last_date), ticker_in, len(df_ticker)))

            ### Проверка не появилось ли новых тикеров с момента последнего обновления
            ### Новые тикеры выгружаем за весь период датасета (а не только за последний год)
            ticker_list_actual_dataset = set(df['ticker'].to_list())
            delta = sorted(set(ticker_list_actual) - ticker_list_actual_dataset)
            if len(delta) > 0:
                print('Новых тикеров: {}'.format(len(delta)))
            for t in range (0,len(delta)):
                ticker_in = delta[t]
                ticker_type = get_ticker_type(ticker_in, moex_full_catalogue)
                end_date_mx, start_date_mx = ticker_date_range(ticker_in, years, tickers_dates, today)

                df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                parts.append(df_ticker)
                print('Новый тикер {}: {} записей'.format(ticker_in, len(df_ticker)))

            df = pd.concat(parts, ignore_index=True)

            # После конкатенации новые строки из API — это строки ISO, а старые
            # уже datetime. Снова приводим обе колонки к datetime
            df = normalize_dates(df)

            # Дубли по (тикер, начало свечи): последняя свеча прошлой выгрузки могла быть
            # незавершённой, поэтому оставляем свежую версию из нового запроса
            df.drop_duplicates(subset=['ticker', 'begin'], keep='last', inplace=True)

            # убираем то, что вышло за период датасета (иначе файл "10 лет" со временем растёт)
            df = cut_years(df, years, today)

            df.sort_values(by=['ticker','begin'],inplace=True)
            df.reset_index(inplace=True,drop=True)
            print("Длина {} после обновления {}".format(filename_j, len(df)))

            save_dataset(df, filename_j, current_path)

        # запоминаем скачанный интервал, чтобы вырезать из него более короткие периоды
        loaded_interval = interval
        loaded_years = years
        loaded_df = df


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


