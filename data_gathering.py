# %%
import datetime, pandas as pd, requests, csv, sys, time

current_path = sys.path[0]

today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}


# %% [markdown]
# Выгрузка доступного на мосбирже

# %%
def moex_tickerlists (current_path):
    CSV_URL = 'https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1'


    with requests.Session() as s:
        download = s.get(CSV_URL, headers = headers)

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
    df_moex_stocks.to_excel(("{}/datasets/ticker_lists/moex_stocks.xlsx").format(current_path))
    df_moex_stocks.to_csv(("{}/datasets/ticker_lists/moex_stocks.csv").format(current_path))

    ## Запуск только для акций и депозитарных расписок
    all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
    all_stocks_ru = all_stocks_ru.loc[~all_stocks_ru.duplicated(), :]

    ## Запуск для всего
    all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
    all_stocks_ru = df_moex_stocks.loc[~all_stocks_ru.duplicated(), :]

    return all_stocks_ru

# %%
def moex_query (ticker_in, end_date_mx, start_date_mx, interval):

    df_ticker = pd.DataFrame()
    
    query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}'
    df = pd.read_csv(query, sep=';', header=1)

    time.sleep(3) #нужно чтобы не перегружать API Мосбиржи

    # df.rename(columns={'End': 'Date'}, inplace=True) #переименовка колонки, чтобы было всё в одном формате
    df['ticker'] = ticker_in
    # df = df.set_index('Date')

    if len(df) > 0: df_ticker = pd.concat([df_ticker,df])

    return df_ticker

# %%
## Функция выгрузки данных через ручку MOEX
def moex (ticker_in, years, interval):

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
            df_ticker = moex_query(ticker_in, end_date_mx, start_date_mx, interval)
            
        except:
            exception_list.append(ticker_in)

    return df_ticker

# %%
### Функция для выгрузки данных по конфигу
def full_reload (all_stocks_ru, interval, years, filename, word, current_path):
    df_full = pd.DataFrame()

    for i in range(0,len(all_stocks_ru)):
        ticker_in = all_stocks_ru['TRADE_CODE'][i]
        df = moex(ticker_in, years, interval)
        if len(df) > 0: df_full = pd.concat([df_full,df])


    print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, len(df_full)))
    if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename + '.xlsx')),index = False)
    if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename + '.csv')),index = False)


# %%
## для тестирования функции
# full_reload(1,10,'1year_data_1m_intervcal',current_path)

# %%
config = [
        {
        'interval': 24,
        'years': 10,
        'filename': '10years_data_1d_interval',
        'word': 'часа'
        },
        {
        'interval': 60,
        'years': 10,
        'filename': '10years_data_1h_interval',
        'word': 'минут'
        },
        {
        'interval': 10,
        'years': 10,
        'filename': '10years_data_10m_interval',
        'word': 'минут'
        },
        {
        'interval': 1,
        'years': 10,
        'filename': '10years_data_1m_interval',
        'word': 'минута'
        },
        {
        'interval': 24,
        'years': 30,
        'filename': '30years_data_1d_interval',
        'word': 'часа'
        },
        {
        'interval': 60,
        'years': 30,
        'filename': '30years_data_1h_interval',
        'word': 'минут'
        },
        {
        'interval': 10,
        'years': 30,
        'filename': '30years_data_10m_interval',
        'word': 'минут'
        },
        {
        'interval': 1,
        'years': 30,
        'filename': '30years_data_1m_interval',
        'word': 'минута'
        }
        ]

# %%
def data_update (config, current_path, all_stocks_ru):

    today = datetime.datetime.now()

    ## обновление готовых файлов
    for j in range(0,len(config)):
        filename_j = config[j]['filename']
        interval = config[j]['interval']

        dataset_path = current_path + "/datasets/{}.csv".format(filename_j)

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

            start_date_mx = start_date.strftime('%Y-%m-%d')
            end_date_mx = (datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')

            df_ticker = moex_query(ticker_in, end_date_mx, start_date_mx, interval)
            df = pd.concat([df, df_ticker])

        
        ### Проверка не появилось ли новых тикеров с момента последнего обновления

        ticker_list_actual = list(set(all_stocks_ru['TRADE_CODE'].to_list()))
        ticker_list_actual.remove('')

        ticker_list_actual_dataset = list(set(df['ticker'].to_list()))

        delta = list(set(ticker_list_actual) - set(ticker_list_actual_dataset))
        if len(delta) > 0:
            for t in range (0,len(delta)):
                ticker_in = delta[t]
                start_date = today
                start_date_mx = start_date.strftime('%Y-%m-%d')
                end_date_mx = (datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')

                df_ticker = moex_query(ticker_in, end_date_mx, start_date_mx, interval)
                df = pd.concat([df, df_ticker])
        

        df.sort_values(by=['ticker','begin'],inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(inplace=True,drop=True)
        print("Длина {} после обновления {}".format(filename_j, len(df)))

        if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename_j + '.xlsx')),index = False)
        if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename_j + '.csv')),index = False)

# %%
def main(current_path):
    global exception_list

    all_stocks_ru = moex_tickerlists (current_path)

    if datetime.datetime.now().day in [1, 14]:
        for k in range(0, len(config)):
            full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path)
        
    else:
        data_update(config,current_path, all_stocks_ru)


    exception_list = list(set(exception_list)) #дедупликация
    print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))

# %%
if __name__ == "__main__":
    main(current_path)


