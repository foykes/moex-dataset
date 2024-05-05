#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime, pandas as pd, requests, csv, sys

current_path = sys.path[0]

today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []


# Выгрузка доступного на мосбирже

# In[ ]:


CSV_URL = 'https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1'


with requests.Session() as s:
    download = s.get(CSV_URL)

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


# In[ ]:


all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
all_stocks_ru = all_stocks_ru.loc[~all_stocks_ru.duplicated(), :]


# In[ ]:


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
            query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}'
            df = pd.read_csv(query, sep=';', header=1)

            # df.rename(columns={'End': 'Date'}, inplace=True) #переименовка колонки, чтобы было всё в одном формате
            df['ticker'] = ticker_in
            # df = df.set_index('Date')

            if len(df) > 0: df_ticker = pd.concat([df_ticker,df])
            
        except:
            exception_list.append(ticker_in)

    return df_ticker


# Данные за 10 лет с интервалом 1 день

# In[ ]:


interval = 24
years = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])


print("Записей для промежутка 10 лет с интервалом 1 день: {}".format(len(df_full)))

if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(("{}/datasets/10years_data_1d_interval.xlsx".format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(("{}/datasets/10years_data_1d_interval.csv".format(current_path)),index = False)


# Данные за 10 лет с интервалом 1 час
# 

# In[ ]:


interval = 60
years = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 10 лет с интервалом 1 час: {}".format(len(df_full)))

if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/10years_data_1h_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_1h_interval.csv'.format(current_path)),index = False)    


# Данные за 10 лет с интервалом 10 минут

# In[ ]:


interval = 10
years = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 10 лет с интервалом 10 минут: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/10years_data_10m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_10m_interval.csv'.format(current_path)),index = False)


# Данные за 10 лет с интервалом 1 минута

# In[ ]:


interval = 1
years = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 10 лет с интервалом 1 минута: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/10years_data_1m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_1m_interval.csv'.format(current_path)),index = False)


# Данные за 30 лет с интервалом 1 день
# 

# In[ ]:


interval = 24
years = 30

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 30 лет с интервалом 1 день: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/30years_data_1d_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1d_interval.csv'.format(current_path)),index = False)  


# Данные за 30 лет с интервалом 1 час
# 

# In[ ]:


interval = 60
years = 30

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 30 лет с интервалом 1 час: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/30years_data_1h_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1h_interval.csv'.format(current_path)),index = False)


# Данные за 30 лет с интервалом 10 минут

# In[ ]:


interval = 10
years = 30

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print("Записей для промежутка 30 лет с интервалом 10 минут: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/30years_data_10m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_10m_interval.csv'.format(current_path)),index = False)


# Данные за 30 лет с интервалом 1 минута

# In[ ]:


interval = 1
years = 30

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex(ticker_in, years, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])


print("Записей для промежутка 30 лет с интервалом 1 минута: {}".format(len(df_full)))
if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/30years_data_1m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1m_interval.csv'.format(current_path)),index = False)


# In[ ]:


exception_list = list(set(exception_list)) #дедупликация
print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))

