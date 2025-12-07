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

    return all_stocks_ru

# %%
### Функция запроса к API по тикеру, датам и нужному интервалу
def moex_query (ticker_in, ticker_type, end_date_mx, start_date_mx, interval):

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

    response = requests.get(query)
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
### Функция для выгрузки данных с нуля
def full_reload (all_stocks_ru, interval, years, filename, word, current_path):
    df_full = pd.DataFrame()

    for i in range(0,len(all_stocks_ru)):
        ticker_in = all_stocks_ru['TRADE_CODE'][i]
        ticker_type = all_stocks_ru['SUPERTYPE'][i]
        df = moex(ticker_in, ticker_type, years, interval)
        if len(df) > 0: df_full = pd.concat([df_full,df])


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

    if force_reload == True: ## Если нужно с нуля перевыгрузить данные, то это этот необязательный параметр нужно передать как True
        for k in range(0, len(config)):
            full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path)
        
    else:
        data_update(config,current_path, all_stocks_ru)


    exception_list = list(set(exception_list)) #дедупликация
    print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))

# %%
if __name__ == "__main__":
    main(current_path, force_reload = False)
    # main(current_path, force_reload = True)


