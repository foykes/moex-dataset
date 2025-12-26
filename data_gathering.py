# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json


today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []
current_path = sys.path[0]


# %%
header = {'User-Agent': ''}
### Выгрузка header для запроса
with open('settings/user_agents.json', 'r', encoding='utf-8') as f:
    headers_full = json.load(f)

header_first = str(headers_full['chrome'][0])
header['User-Agent'] = header_first

# %%
### Выгрузка конфига файлов
with open('settings/datasets_config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

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

    print("Общее количество объектов на Мосбирже: {}".format(len(df_moex)))
    df_moex.to_excel(("{}/datasets/ticker_lists/moex_full.xlsx").format(current_path))
    df_moex.to_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path))

    df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")]
    df_moex_stocks.reset_index(drop=True, inplace=True)

    ## moex_stocks_list['CURRENCY'] == '' это заблокированные акции

    print("Количество акций и депозитарных расписок: {}".format(len(df_moex_stocks)))
    # df_moex_stocks.to_excel(("{}/datasets/ticker_lists/moex_stocks.xlsx").format(current_path))
    df_moex_stocks.to_csv(("{}/datasets/ticker_lists/moex_stocks.csv").format(current_path))

    ## Запуск только для акций и депозитарных расписок
    all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
    all_stocks_ru = all_stocks_ru.loc[~all_stocks_ru.duplicated(), :]

    ## Запуск для всего
    all_stocks_ru = df_moex.filter(['TRADE_CODE'], axis = 1)
    all_stocks_ru = df_moex.loc[~all_stocks_ru.duplicated(), :]

    df_moex_stocks.reset_index(drop=True, inplace=True)

    return all_stocks_ru

# %%
### Функция запроса к API по тикеру, датам и нужному интервалу
def moex_query (ticker_in, ticker_type, end_date_mx, start_date_mx, interval):
    global header
    
    df_ticker = pd.DataFrame()

    #определение типа market для корректного запроса
    if ticker_type in ['Инвестиционные паи','Депозитарные расписки','Акции','Ипотечные сертификаты участия']:
        market = "shares"
    elif ticker_type in ['Облигации','Еврооблигации']:
        market = "bonds"
    else:
        market = "shares"
        
    query = f'http://iss.moex.com/iss/engines/stock/markets/{market}/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #универсальный шаблон
    # query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для акций
    # query = f'http://iss.moex.com/iss/engines/stock/markets/bonds/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для облигаций

    response = requests.get(query, headers = header)
    status_code = response.status_code

    # Читаем CSV если статус успешный
    if status_code == 200:
        df = pd.read_csv(query, sep=';', header=1)
        time.sleep(5) #нужно чтобы не перегружать API Мосбиржи
    else: # Если нет то пауза и вторая попытка
        print(status_code, ticker_in, end_date_mx, start_date_mx, interval)
        print(query)
        time.sleep(60)
        df = pd.read_csv(query, sep=';', header=1)

    # df.rename(columns={'End': 'Date'}, inplace=True) #переименовка колонки, чтобы было всё в одном формате
    df['ticker'] = ticker_in
    # df = df.set_index('Date')

    if len(df) > 0: df_ticker = pd.concat([df_ticker,df])
    # else:
    #     # print(df.head())
    #     print(ticker_in, end_date_mx, start_date_mx, interval)

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
def full_reload (all_stocks_ru, interval, years, filename, word, current_path, tickers_dates):
    df_full = pd.DataFrame()
    today = datetime.datetime.now()
    start_date = today


    ## ручная отладка
    interval = 24
    years = 10
    filename= "10years_data_1d_interval"
    word = "часа"
    ##


    ##определяем границу нужного диапазона выгрузки
    if years != 0:
        date_shift_needed = start_date - datetime.timedelta(days=years*365)
        date_shift_needed = date_shift_needed.strftime('%Y-%m-%d')
    else:
        date_shift_needed = '0'


    for i in range(0,len(all_stocks_ru)):
        ticker_in = all_stocks_ru['TRADE_CODE'][i]
        ticker_type = all_stocks_ru['SUPERTYPE'][i]

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

            df = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
            if len(df) > 0: df_full = pd.concat([df_full,df])
        else:
            print(ticker_in)

    print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, len(df_full)))
    if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename + '.xlsx')),index = False)
    if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename + '.csv')),index = False)

# %%
### Функция для обновления текущих датасетов по конфигу
def data_update (config, current_path, all_stocks_ru):
    today = datetime.datetime.now()
    moex_full_catalogue = pd.read_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path), index_col=0)
    
    ## обновление готовых файлов
    for j in range(0,len(config)):
        filename_j = config[j]['filename']
        interval = config[j]['interval']
        years = config[j]['years']
        word = config[j]['word']

        dataset_path = current_path + "datasets/{}.csv".format(filename_j)

        # проверка что файл существует
        if os.path.isfile(dataset_path) == False:
            print("Файла не существует, не могу его обновить: \n{}".format(dataset_path))
            print("Начинаю выгружать его с нуля")
            full_reload (all_stocks_ru, interval, years, filename_j, word, current_path)

        else:
            if dataset_path.endswith('csv') and "~$" not in dataset_path:
                df = pd.read_csv(dataset_path)
            elif dataset_path.endswith('xlsx') and "~$" not in dataset_path:
                df = pd.read_excel(dataset_path)

            print("Длина {} до обновления {}".format(filename_j, len(df)))

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
                start_date = today
                ticker_type = moex_full_catalogue[moex_full_catalogue['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                start_date_mx = start_date.strftime('%Y-%m-%d')
                end_date_mx = (datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')

                df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                df = pd.concat([df, df_ticker])

            
            ### Проверка не появилось ли новых тикеров с момента последнего обновления

            ticker_list_actual = list(set(all_stocks_ru['TRADE_CODE'].to_list()))
            ticker_list_actual.remove('')

            ticker_list_actual_dataset = list(set(df['ticker'].to_list()))

            delta = list(set(ticker_list_actual) - set(ticker_list_actual_dataset))
            if len(delta) > 0:
                for t in range (0,len(delta)):
                    ticker_in = delta[t]
                    start_date_mx = today.strftime('%Y-%m-%d')
                    end_date_mx = (today - datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
                    ticker_type = moex_full_catalogue[moex_full_catalogue['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                    df_ticker = moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
                    df = pd.concat([df, df_ticker])
            
            df.sort_values(by=['ticker','begin'],inplace=True)
            df.drop_duplicates(inplace=True)
            df.reset_index(inplace=True,drop=True)
            print("Длина {} после обновления {}".format(filename_j, len(df)))

            if len(df) > 0 and len(df) < 1048576: df.to_excel(('{}/datasets/{}'.format(current_path,filename_j + '.xlsx')),index = False)
            if len(df) > 0: df.to_csv(('{}/datasets/{}'.format(current_path, filename_j + '.csv')),index = False)

# %%
def main(current_path, force_reload = False):
    global exception_list
    global config
    
    all_stocks_ru = moex_tickerlists (current_path)
    all_stocks_ru.reset_index(drop=True, inplace=True)

    tickers_dates = build_tickers_dates(all_stocks_ru, current_path)
    
    if force_reload == True: ## Если нужно с нуля перевыгрузить данные, то это этот необязательный параметр нужно передать как True
        for k in range(0, len(config)):
            full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path, tickers_dates)
        
    else:
        data_update(config,current_path, all_stocks_ru)


    exception_list = list(set(exception_list)) #дедупликация
    print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))

# %%
if __name__ == "__main__":
    # main(current_path, force_reload = False)
    main(current_path, force_reload = True)


