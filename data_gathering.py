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


today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []
current_path = sys.path[0]


# %%
header = {'User-Agent': ''}
### Выгрузка header для запроса
json_path = current_path + '/settings/user_agents.json'
with open(json_path, 'r', encoding='utf-8') as f:
    headers_full = json.load(f)

header_first = str(headers_full['chrome'][0])
header['User-Agent'] = header_first

# %%
### Выгрузка конфига файлов
json_path = current_path + '/settings/datasets_config.json'

with open(json_path, 'r', encoding='utf-8') as f:
    config = json.load(f)

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
def _prepare_ticker_catalogue(source):
    """Отдельная рабочая копия: исходный каталог и его экспорт не меняются."""
    if not isinstance(source, pd.DataFrame) or any(
            list(source.columns).count(name) != 1 for name in ('TRADE_CODE', 'SUPERTYPE')):
        raise ValueError('A1_TICKER_CATALOGUE_STRUCTURE')

    groups = {}
    for position, value in enumerate(source['TRADE_CODE']):
        if isinstance(value, str):
            key = value.strip()
            if key:
                groups.setdefault(key, []).append(position)
        elif pd.api.types.is_scalar(value) and pd.isna(value):
            continue
        else:
            # Не превращаем число/контейнер в новый строковый идентификатор.
            raise ValueError('A1_TICKER_CATALOGUE_STRUCTURE')

    positions = []
    for key, group in groups.items():
        raw_codes = {source.iloc[position]['TRADE_CODE'] for position in group}
        if len(raw_codes) > 1:
            # Проверяем всю новую strip-группу до выбора первой строки. Старые
            # exact-code дубли и произвольные различия NAME/CURRENCY не ремонтируем.
            routes = [source.iloc[position]['SUPERTYPE'] for position in group]
            if any(not isinstance(route, str) or not route.strip() for route in routes) or any(
                    route != routes[0] for route in routes[1:]):
                raise ValueError('A1_AMBIGUOUS_TICKER_ROUTING')
            for name in ('ISIN', 'INSTRUMENT_ID'):
                if list(source.columns).count(name) != 1:
                    continue
                hints = []
                for position in group:
                    value = source.iloc[position][name]
                    if not pd.api.types.is_scalar(value) or pd.isna(value):
                        continue
                    if isinstance(value, str) and not value.strip():
                        continue
                    hints.append(value)
                if hints and any(value != hints[0] for value in hints[1:]):
                    raise ValueError('A1_AMBIGUOUS_TICKER_IDENTITY')
        positions.append(group[0])

    if not positions:
        raise ValueError('A1_EMPTY_TICKER_CATALOGUE')
    processing = source.iloc[positions].copy(deep=True)
    processing['TRADE_CODE'] = list(groups)
    processing.reset_index(drop=True, inplace=True)
    return processing


def _a1_counts(received, accepted, instruments):
    return dict(rows_received=received, rows_accepted=accepted,
                rows_quarantined=None, pages=None, instruments=instruments,
                null_reasons={'rows_quarantined': 'NOT_ASSESSED', 'pages': 'NOT_MEASURED'})


def _a1_event(logging_context, level, event, fields):
    if logging_context is None:
        return
    try:
        import run_logging
        receipt = run_logging.emit_event(logging_context, level, event,
            'Сбор данных A1: ' + event, dict(stage='data_gathering', **fields))
        healthy = run_logging.check_logging_health(logging_context)['healthy']
        if not receipt['accepted'] or not healthy or level == 'ERROR' and not receipt['confirmed']:
            raise RuntimeError('LOGGING_INCOMPLETE')
    except BaseException:
        # Не цепляем вторичную ошибку с потенциальными секретами к новому сигналу.
        raise RuntimeError('LOGGING_INCOMPLETE') from None


def _a1_failure(logging_context, error, fields):
    if logging_context is None:
        return
    try:
        notes = getattr(error, '__notes__', [])
        if type(notes) is list and 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE' in notes:
            return
    except BaseException:
        pass
    try:
        import run_logging
        codes = {'A1_TICKER_CATALOGUE_STRUCTURE', 'A1_EMPTY_TICKER_CATALOGUE',
                 'A1_AMBIGUOUS_TICKER_ROUTING', 'A1_AMBIGUOUS_TICKER_IDENTITY',
                 'A1_STORED_TICKER_INCOMPATIBLE'}
        code = error.args[0] if (type(error) is ValueError and len(error.args) == 1 and isinstance(error.args[0], str)
                                 and error.args[0] in codes) else 'APPLICATION_ERROR'
        record = run_logging.make_error(logging_context, error, 'data_gathering', code=code)
        _a1_event(logging_context, 'ERROR', 'a1_failed',
                  dict(fields, outcome='FAILED', error=record))
    except BaseException:
        # Два независимых bounded сигнала; ни один fallback не заменяет primary.
        try:
            print('A1_LOGGING_FAILURE: LOGGING_INCOMPLETE', file=sys.stderr)
        except BaseException:
            pass
        try:
            BaseException.add_note(error, 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE')
        except BaseException:
            pass


def _check_stored_ticker_identity(dataset, keys):
    for value in dataset['ticker']:
        if isinstance(value, str) and value != value.strip() and value.strip() in keys:
            # Проверка всего read dataset до удаления RSI/30-day filter. Это отказ,
            # а не скрытая миграция старых свечей или historical mapping.
            raise ValueError('A1_STORED_TICKER_INCOMPATIBLE')


### Выгрузка датасета доступного на мосбирже
def moex_tickerlists (current_path, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = dict(file='ticker_lists/moex_full.csv')
    _a1_event(logging_context, 'INFO', 'a1_moex_tickerlists_started', active_fields)
    try:
        CSV_URL = 'https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1'
        global header

        with requests.Session() as s:
            download = s.get(CSV_URL, headers = header)

            decoded_content = download.content.decode('cp1251')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)


        if not my_list:
            raise ValueError('A1_EMPTY_TICKER_CATALOGUE')
        df_moex = pd.DataFrame(my_list)
        new_header = df_moex.iloc[0]
        df_moex = df_moex[1:]
        df_moex.columns = new_header

        # Guard до первого writer, но raw source rows/values/order/index сохраняются.
        all_stocks_ru = _prepare_ticker_catalogue(df_moex)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(file='ticker_lists/moex_full.csv',
                 counts=_a1_counts(len(df_moex), len(all_stocks_ru), len(all_stocks_ru))))

        print("Общее количество объектов на Мосбирже: {}".format(len(df_moex)))
        active_fields = dict(file='ticker_lists/moex_full.xlsx')
        df_moex.to_excel(("{}/datasets/ticker_lists/moex_full.xlsx").format(current_path))
        active_fields = dict(file='ticker_lists/moex_full.csv')
        df_moex.to_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path))

        df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")]
        df_moex_stocks.reset_index(drop=True, inplace=True)

        ## moex_stocks_list['CURRENCY'] == '' это заблокированные акции

        print("Количество акций и депозитарных расписок: {}".format(len(df_moex_stocks)))
        # df_moex_stocks.to_excel(("{}/datasets/ticker_lists/moex_stocks.xlsx").format(current_path))
        active_fields = dict(file='ticker_lists/moex_stocks.csv')
        df_moex_stocks.to_csv(("{}/datasets/ticker_lists/moex_stocks.csv").format(current_path))

        _a1_event(logging_context, 'INFO', 'a1_catalogue_returned',
            dict(file='ticker_lists/moex_full.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000),
                 counts=_a1_counts(len(df_moex), len(all_stocks_ru), len(all_stocks_ru))))
        return all_stocks_ru
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
### Функция запроса к API по тикеру, датам и нужному интервалу
def moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval, max_retries=5, retry_sleep_start=3):
    global header
    global headers_full
    
    df_ticker = pd.DataFrame()
    df = pd.DataFrame()

    # Определение типа market для корректного запроса
    if ticker_type in ['Инвестиционные паи', 'Депозитарные расписки', 'Акции', 'Ипотечные сертификаты участия']:
        market = "shares"
    elif ticker_type in ['Облигации', 'Еврооблигации']:
        market = "bonds"
    else:
        market = "shares"
        
    query = f'http://iss.moex.com/iss/engines/stock/markets/{market}/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #универсальный шаблон
    # query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для акций
    # query = f'http://iss.moex.com/iss/engines/stock/markets/bonds/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для облигаций

    attempt = 0
    success = False
    last_status_code = None
    last_error = None

    # Если global header почему-то не задан или пустой —
    # инициализируем первым User-Agent из headers_full
    if not isinstance(header, dict) or len(header) == 0:
        header = get_next_header(None)

    while attempt < max_retries and not success:
        attempt += 1

        try:
            response = requests.get(query, headers=header, timeout=30)
            last_status_code = response.status_code

            # Читаем CSV если статус успешный
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text), sep=';', header=1)

                # Пауза после успешного запроса, чтобы не перегружать API Мосбиржи
                time.sleep(3)

                success = True

            else:
                print(
                    f'Попытка {attempt}/{max_retries} неуспешна. '
                    f'Status code: {response.status_code}, '
                    f'ticker: {ticker_in}, '
                    f'from: {end_date_mx}, till: {start_date_mx}, interval: {interval}'
                )
                print(query)

        except Exception as e:
            last_error = e
            print(
                f'Попытка {attempt}/{max_retries} завершилась ошибкой: {e}. '
                f'ticker: {ticker_in}, '
                f'from: {end_date_mx}, till: {start_date_mx}, interval: {interval}'
            )
            print(query)

        # Если попытка неуспешна — меняем header и ждём перед следующей попыткой
        if not success and attempt < max_retries:
            old_user_agent = header.get('User-Agent') if isinstance(header, dict) else None

            header = get_next_header(header)

            new_user_agent = header.get('User-Agent') if isinstance(header, dict) else None

            print('Меняем User-Agent:')
            print(f'old: {old_user_agent}')
            print(f'new: {new_user_agent}')

            # Экспоненциальная пауза после неудачной попытки:
            # 1-я ошибка -> 3 сек.
            # 2-я ошибка -> 6 сек.
            # 3-я ошибка -> 12 сек.
            # 4-я ошибка -> 24 сек.
            sleep_time = retry_sleep_start * (2 ** (attempt - 1))

            print(f'Ждём {sleep_time} сек. перед следующей попыткой...')
            time.sleep(sleep_time)

    if not success:
        print(
            f'Не удалось получить данные после {max_retries} попыток. '
            f'ticker: {ticker_in}, '
            f'last_status_code: {last_status_code}, '
            f'last_error: {last_error}'
        )
        return df_ticker

    if len(df) > 0:
        df['ticker'] = ticker_in
        df_ticker = pd.concat([df_ticker, df], ignore_index=True)

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
        except:
            exception_list.append(ticker_in)

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
    tickers_dates.to_excel(("{}/datasets/ticker_lists/tickers_dates.xlsx").format(current_path))
    tickers_dates.to_csv(("{}/datasets/ticker_lists/tickers_dates.csv").format(current_path))

    return tickers_dates


# %%
### Функция для выгрузки данных с нуля
def full_reload (all_stocks_ru, interval, years, filename, word, current_path, tickers_dates, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.csv')
    _a1_event(logging_context, 'INFO', 'a1_full_reload_started', active_fields)
    try:
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(active_fields, counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))
        df_full = pd.DataFrame()
        today = datetime.datetime.now()
        start_date = today

        ##определяем границу нужного диапазона выгрузки
        if years != 0:
            date_shift_needed = start_date - datetime.timedelta(days=years*365)
            date_shift_needed = date_shift_needed.strftime('%Y-%m-%d')
        else:
            date_shift_needed = '0'


        for i in range(0,len(all_stocks_ru)):
            ticker_in = all_stocks_ru.iloc[i]['TRADE_CODE']
            ticker_type = all_stocks_ru.iloc[i]['SUPERTYPE']
            active_fields = dict(dataset_id=filename, interval=interval,
                                 instrument=ticker_in, file=filename + '.csv')

            if len(ticker_in) > 0: #проверка что тикер выгрузился и есть

                #определение левой границы выгрузки: или дата листинга или самое раннее нужное значение
                end_date_mx = tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['issue_date'].values[0]
                end_date_mx = str(end_date_mx)[:10]
                if date_shift_needed > end_date_mx:
                    end_date_mx = date_shift_needed

                if tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['stopped_date'].isna().values[0] == True:
                    start_date_mx = start_date.strftime('%Y-%m-%d')
                else:
                    start_date_mx = tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['stopped_date'].values[0]
                    start_date_mx = str(start_date_mx)[:10]

                _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                    dict(dataset_id=filename, interval=interval, instrument=ticker_in, file=filename + '.csv'))
                query_started = time.perf_counter()
                df = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                    dict(active_fields, outcome='RETURNED',
                         duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                         counts=_a1_counts(len(df), len(df), 1)))
                if len(df) > 0: df_full = pd.concat([df_full,df])
            else:
                print(ticker_in)

        print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, len(df_full)))
        active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.xlsx')
        if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename + '.xlsx')),index = False)
        active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.csv')
        if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename + '.csv')),index = False)
        _a1_event(logging_context, 'INFO', 'a1_full_reload_returned',
            dict(dataset_id=filename, interval=interval, file=filename + '.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000), counts=_a1_counts(len(df_full), len(df_full), len(all_stocks_ru))))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
### Функция для обновления текущих датасетов по конфигу
def data_update (config, current_path, all_stocks_ru, tickers_dates, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = {}
    _a1_event(logging_context, 'INFO', 'a1_data_update_started', active_fields)
    try:
        today = datetime.datetime.now()
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))
        active_fields = dict(file='ticker_lists/moex_full.csv')
        moex_full_catalogue = pd.read_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path), index_col=0)
        lookup = _prepare_ticker_catalogue(moex_full_catalogue)
        _a1_event(logging_context, 'INFO', 'a1_lookup_prepared',
            dict(file='ticker_lists/moex_full.csv',
                 counts=_a1_counts(len(moex_full_catalogue), len(lookup), len(lookup))))
        catalogue_keys = set(all_stocks_ru['TRADE_CODE']) | set(lookup['TRADE_CODE'])

        ## обновление готовых файлов
        for j in range(0,len(config)):
            filename_j = config[j]['filename']
            interval = config[j]['interval']
            years = config[j]['years']
            word = config[j]['word']
            active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv')
            _a1_event(logging_context, 'INFO', 'a1_dataset_started', active_fields)

            dataset_path = current_path + "datasets/{}.csv".format(filename_j)

            # проверка что файл существует
            if os.path.isfile(dataset_path) == False:
                print("Файл текущего набора отсутствует, не могу его обновить")
                print("Начинаю выгружать его с нуля")
                _a1_event(logging_context, 'INFO', 'a1_cold_start',
                    dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv', outcome='MISSING_DATASET'))
                full_reload (all_stocks_ru, interval, years, filename_j, word, current_path, tickers_dates,
                             logging_context=logging_context)

            else:
                if dataset_path.endswith('csv') and "~$" not in dataset_path:
                    df = pd.read_csv(dataset_path)
                elif dataset_path.endswith('xlsx') and "~$" not in dataset_path:
                    df = pd.read_excel(dataset_path)

                _check_stored_ticker_identity(df, catalogue_keys)
                print("Записей до обновления: {}".format(len(df)))

                # убираем ненужные колонки теханализа - их потом с нуля пересчитаем
                ## ПРОВЕРИТЬ ЧТО ЕСЛИ ЭТО НЕ ДЕЛАТЬ
                columns = df.columns
                white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end',
                    'ticker']
                columns_to_remove = [i for i in columns if i not in white_list_columns]
                columns_to_remove
                df.drop(columns_to_remove, axis=1,inplace=True)
                # df.head(2)

                # для каждого тикера выбираем последнюю дату за которую есть выгрузка
                df_last_date = df.sort_values(by=['end']).drop_duplicates(subset='ticker', keep='last')
                df_last_date = df_last_date.loc[:,['end','ticker']]
                df_last_date.drop_duplicates(inplace=True)
                df_last_date.reset_index(inplace=True,drop=True)

                # оставляем только тикеры, которые выгружались в последние 30 дней (чтобы не брать тикеры, которые делистили)
                filter_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                df_last_date = df_last_date[df_last_date['end'] >= filter_date]



                # обновление текущих данных
                for i in range(0,len(df_last_date)):
                    end_date = df_last_date.iloc[i]['end']
                    ticker_in = df_last_date.iloc[i]['ticker']
                    active_fields = dict(dataset_id=filename_j, interval=interval,
                                         instrument=ticker_in, file=filename_j + '.csv')
                    start_date = today
                    ticker_type = lookup[lookup['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                    start_date_mx = start_date.strftime('%Y-%m-%d')
                    end_date_mx = (datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')

                    _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                        dict(dataset_id=filename_j, interval=interval, instrument=ticker_in, file=filename_j + '.csv'))
                    query_started = time.perf_counter()
                    df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                    _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                        dict(active_fields, outcome='RETURNED',
                             duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                             counts=_a1_counts(len(df_ticker), len(df_ticker), 1)))
                    df = pd.concat([df, df_ticker])


                ### Проверка не появилось ли новых тикеров с момента последнего обновления

                ticker_list_actual = all_stocks_ru['TRADE_CODE'].to_list()
                ticker_list_actual_dataset = set(df['ticker'].to_list())
                delta = [ticker for ticker in ticker_list_actual if ticker not in ticker_list_actual_dataset]
                if len(delta) > 0:
                    for t in range (0,len(delta)):
                        ticker_in = delta[t]
                        active_fields = dict(dataset_id=filename_j, interval=interval,
                                             instrument=ticker_in, file=filename_j + '.csv')
                        start_date_mx = today.strftime('%Y-%m-%d')
                        end_date_mx = (today - datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
                        ticker_type = lookup[lookup['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                        _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                            dict(dataset_id=filename_j, interval=interval, instrument=ticker_in, file=filename_j + '.csv'))
                        query_started = time.perf_counter()
                        df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                        _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                            dict(active_fields, outcome='RETURNED',
                                 duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                                 counts=_a1_counts(len(df_ticker), len(df_ticker), 1)))
                        df = pd.concat([df, df_ticker])

                df.sort_values(by=['ticker','begin'],inplace=True)
                df.drop_duplicates(inplace=True)
                df.reset_index(inplace=True,drop=True)
                print("Записей после обновления: {}".format(len(df)))

                active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.xlsx')
                if len(df) > 0 and len(df) < 1048576: df.to_excel(('{}/datasets/{}'.format(current_path,filename_j + '.xlsx')),index = False)
                active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv')
                if len(df) > 0: df.to_csv(('{}/datasets/{}'.format(current_path, filename_j + '.csv')),index = False)
        _a1_event(logging_context, 'INFO', 'a1_data_update_returned',
            dict(outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
def main(current_path, force_reload = False, *, logging_context=None):
    global exception_list
    global config

    operation_started = time.perf_counter()
    active_fields = {}
    _a1_event(logging_context, 'INFO', 'a1_main_started', active_fields)
    try:
        all_stocks_ru = moex_tickerlists (current_path, logging_context=logging_context)
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))

        # Builder пишет XLSX, затем CSV; конкретный failed writer здесь неизвестен.
        active_fields = dict(file=None)
        _a1_event(logging_context, 'INFO', 'a1_metadata_started', active_fields)
        metadata_started = time.perf_counter()
        tickers_dates = build_tickers_dates(all_stocks_ru, current_path)
        _a1_event(logging_context, 'INFO', 'a1_metadata_returned',
            dict(file='ticker_lists/tickers_dates.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - metadata_started) * 1000)))
        active_fields = {}

        if force_reload == True: ## Если нужно с нуля перевыгрузить данные, то это этот необязательный параметр нужно передать как True
            _a1_event(logging_context, 'INFO', 'a1_force_reload', dict(outcome='FORCE_RELOAD'))
            for k in range(0, len(config)):
                full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path, tickers_dates,
                            logging_context=logging_context)

        else:
            data_update(config,current_path, all_stocks_ru, tickers_dates, logging_context=logging_context)


        exception_list = list(set(exception_list)) #дедупликация
        print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))
        _a1_event(logging_context, 'INFO', 'a1_main_returned',
            dict(active_fields, outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
if __name__ == "__main__":
    main(current_path, force_reload = False)
    # main(current_path, force_reload = True)


