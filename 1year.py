# %% [markdown]
# Для выгрузки данных за последние 2 года. Облегчённый отдельный датасет

# %%
import data_gathering

import datetime, pandas as pd, requests, csv, sys, time, os

current_path = sys.path[0]

# %%
config = [
        {
        'interval': 24,
        'years': 2,
        'filename': '2years_data_1d_interval',
        'word': 'часа'
        },
        {
        'interval': 60,
        'years': 2,
        'filename': '2years_data_1h_interval',
        'word': 'минут'
        },
        {
        'interval': 10,
        'years': 2,
        'filename': '2years_data_10m_interval',
        'word': 'минут'
        },
        {
        'interval': 1,
        'years': 2,
        'filename': '2years_data_1m_interval',
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

# %%

for k in range(0, len(config)):
    data_gathering.full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path, tickers_dates)


