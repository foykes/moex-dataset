# %% [markdown]
# Для выгрузки данных за всё время (100 лет)

# %%
import data_gathering

import datetime, pandas as pd, requests, csv, sys, time, os

current_path = sys.path[0]

today = datetime.datetime.now()
start_date = today
start_date_mx = today.strftime('%Y-%m-%d')
end_date_mx = "1991-12-26" #если даты листинга нет, то прописываем эту дату
df_full = pd.DataFrame()


# %%
files_config = [
        {
        'interval': 24,
        'years': 100,
        'filename': 'all_data_1d_interval',
        'word': 'часа'
        },
        {
        'interval': 60,
        'years': 100,
        'filename': 'all_data_1h_interval',
        'word': 'минут'
        },
        {
        'interval': 10,
        'years': 100,
        'filename': 'all_data_10m_interval',
        'word': 'минут'
        },
        {
        'interval': 1,
        'years': 100,
        'filename': 'all_data_1m_interval',
        'word': 'минута'
        }
        ]

# %%
# обновление списка тикеров
all_stocks_ru = data_gathering.moex_tickerlists (current_path)
all_stocks_ru.reset_index(drop=True,inplace=True)

# %%
# обновление дат торгов этих тикеров
tickers_dates = data_gathering.build_tickers_dates(all_stocks_ru, current_path)
result = pd.merge(all_stocks_ru[['TRADE_CODE','SUPERTYPE']], tickers_dates[['TRADE_CODE', 'issue_date', 'stopped_date']], on='TRADE_CODE', how='inner')
result = result.reset_index(drop=True)

# %%
## Замена пустых значений

result['issue_date'] = result['issue_date'].fillna(end_date_mx)
# result[result['issue_date'].isna()]

result['stopped_date'] = result['stopped_date'].fillna(start_date_mx)
# result[result['start_date_mx'].isna()]


# %%
interval = files_config[0]['interval']

# %%
for i in range (0, len(result)):
    ticker_in = result['TRADE_CODE'][i]
    ticker_type = result['SUPERTYPE'][i]
    end_date_mx = result['issue_date'][i].strftime('%Y-%m-%d')
    start_date_mx = result['stopped_date'][i].strftime('%Y-%m-%d')

df = data_gathering.moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)

# %%
for j in range (0,len(files_config)):

    interval = files_config[j]['interval']
    filename = files_config[j]['filename']

    for i in range (0, 10): #тестовый прогон
    # for i in range (0, len(result)):
        ticker_in = result['TRADE_CODE'][i]
        ticker_type = result['SUPERTYPE'][i]
        end_date_mx = result['issue_date'][i].strftime('%Y-%m-%d')
        start_date_mx = result['stopped_date'][i].strftime('%Y-%m-%d')

        try:
            df = data_gathering.moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
            if len(df) > 0: df_full = pd.concat([df_full,df])
        except:
            print("error")
            print(ticker_in,ticker_type,end_date_mx,start_date_mx)
            print("**************************")

        time.sleep(5) #чтобы снизить нагрузку на API и избежать бана

    if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename + '.xlsx')),index = False)
    if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename + '.csv')),index = False)


