#!/usr/bin/env python
# coding: utf-8

# In[18]:


import datetime, pandas as pd, requests, csv, os

current_path = os.getcwd()

today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []


# Выгрузка доступного на мосбирже

# In[19]:


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

print(len(df_moex))
df_moex.to_excel(("{}/datasets/ticker_lists/moex_full.xlsx").format(current_path))
df_moex.to_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path))

df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")]
df_moex_stocks.reset_index(drop=True, inplace=True)

## moex_stocks_list['CURRENCY'] == '' это заблокированные акции

print(len(df_moex_stocks))
df_moex_stocks.to_excel(("{}/datasets/ticker_lists/moex_stocks.xlsx").format(current_path))
df_moex_stocks.to_csv(("{}/datasets/ticker_lists/moex_stocks.csv").format(current_path))


# In[20]:


all_stocks_ru = df_moex_stocks.filter(['TRADE_CODE'], axis = 1)
all_stocks_ru = all_stocks_ru.loc[~all_stocks_ru.duplicated(), :]


# In[21]:


#готовим даты
end_date = datetime.datetime.now()
d = datetime.timedelta(days = 365*10)
start_date = end_date - d


#спец формат даты для MOEX
start_date_mx = start_date.strftime('%Y-%m-%d')
end_date_mx = end_date.strftime('%Y-%m-%d')


# In[22]:


## Функция выгрузки данных через ручку MOEX
def moex (ticker_in, start_date_mx, end_date_mx, interval):

    df = pd.DataFrame()
    global exception_list

    try:
        query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={start_date_mx}&till={end_date_mx}&interval={interval}'
        df = pd.read_csv(query, sep=';', header=1)

        # df.rename(columns={'End': 'Date'}, inplace=True) #переименовка колонки, чтобы было всё в одном формате
        df['ticker'] = ticker_in
        # df = df.set_index('Date')
    except:
        exception_list.append(ticker_in)

    return df


# Данные за 10 лет с интервалом 1 день

# In[23]:


interval = 24

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])


print(len(df_full))

if len(df_full) > 0: df_full.to_excel(("{}/datasets/10years_data_1d_interval.xlsx".format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(("{}/datasets/10years_data_1d_interval.csv".format(current_path)),index = False)
    


# Данные за 10 лет с интервалом 1 час
# 

# In[24]:


interval = 60

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))

if len(df_full) > 0: df_full.to_excel(('{}/datasets/10years_data_1h_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_1h_interval.csv'.format(current_path)),index = False)    


# Данные за 10 лет с интервалом 10 минут

# In[25]:


interval = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/10years_data_10m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_10m_interval.csv'.format(current_path)),index = False)


# Данные за 10 лет с интервалом 1 минута

# In[26]:


interval = 1

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/10years_data_1m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/10years_data_1m_interval.csv'.format(current_path)),index = False)


# In[27]:


#готовим даты для выгрузки за 30 лет
end_date = datetime.datetime.now()
d = datetime.timedelta(days = 365*30)
start_date = end_date - d


#спец формат даты для MOEX
start_date_mx = start_date.strftime('%Y-%m-%d')
end_date_mx = end_date.strftime('%Y-%m-%d')


# Данные за 30 лет с интервалом 1 день
# 

# In[28]:


interval = 24

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/30years_data_1d_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1d_interval.csv'.format(current_path)),index = False)  


# Данные за 30 лет с интервалом 1 час
# 

# In[29]:


interval = 60

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/30years_data_1h_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1h_interval.csv'.format(current_path)),index = False)


# Данные за 30 лет с интервалом 10 минут

# In[30]:


interval = 10

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])



print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/30years_data_10m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_10m_interval.csv'.format(current_path)),index = False)


# Данные за 30 лет с интервалом 1 минута

# In[31]:


interval = 1

df_full = pd.DataFrame()

for i in range(0,len(all_stocks_ru)):
    ticker_in = all_stocks_ru['TRADE_CODE'][i]
    df = moex (ticker_in, start_date_mx, end_date_mx, interval)
    if len(df) > 0: df_full = pd.concat([df_full,df])


print(len(df_full))
if len(df_full) > 0: df_full.to_excel(('{}/datasets/30years_data_1m_interval.xlsx'.format(current_path)),index = False)
if len(df_full) > 0: df_full.to_csv(('{}/datasets/30years_data_1m_interval.csv'.format(current_path)),index = False)


# In[32]:


exception_list = list(set(exception_list)) #дедупликация
print(len(exception_list))

