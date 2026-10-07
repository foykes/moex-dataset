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
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
import argparse, importlib.util
from pathlib import Path
import sys


def _diagnostic_gate():
    parser = argparse.ArgumentParser(description='Явный staging-профиль диагностики дат торгов')
    parser.add_argument('--allow-live-diagnostics', action='store_true')
    parser.add_argument('--profile', choices=['staging'])
    parser.add_argument('--output-root')
    arguments = parser.parse_args()
    if not arguments.allow_live_diagnostics or arguments.profile != 'staging' or not arguments.output_root:
        parser.error('Нужны --allow-live-diagnostics --profile staging --output-root; отдельно согласуйте ограниченный профиль')
    parser.error('Ограниченный staging-профиль не согласован; параметры CLI не дают разрешения на запуск')


def _offline_bootstrap():
    root = Path(__file__).resolve().parent
    guard = sys.modules.get('mds_offline_guard')
    if guard is None:
        specification = importlib.util.spec_from_file_location('mds_offline_guard', root / 'tools/offline_guard.py')
        guard = importlib.util.module_from_spec(specification)
        sys.modules['mds_offline_guard'] = guard
        specification.loader.exec_module(guard)
    guard.ensure_context(root, lane='f3', role='entry', mode='run')


_offline_bootstrap()
if __name__ == '__main__':
    try:
        _diagnostic_gate()
    except SystemExit as error:
        sys.modules['mds_offline_guard'].finish(error.code)
        raise

# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json
header = {'User-Agent': ''}

current_path = sys.path[0]

# %%
## Получение дат торгов акции

def get_ticker_dates (moex_df):
    # /iss/history/engines/[engine]/markets/[market]/securities/[security]/dates

    # market = "shares"
    # ticker_in = "SBER"
    # response = requests.get(query, headers = header)
    global header

    # При import не читаем настройки; обычный вызов функции по-прежнему готовит header.
    ### Выгрузка header для запроса
    if header['User-Agent'] == '':
        with open('settings/user_agents.json', 'r', encoding='utf-8') as f:
            headers_full = json.load(f)
        header_first = str(headers_full['chrome'][0])
        header['User-Agent'] = header_first

    errors = []

    result = []

    for i in range (0, len(moex_df)):
        ticker_type = moex_df['SUPERTYPE'][i]
        ticker_in = moex_df['TRADE_CODE'][i]

        if ticker_type in ['Инвестиционные паи','Депозитарные расписки','Акции','Ипотечные сертификаты участия']:
            market = "shares"
        elif ticker_type in ['Облигации','Еврооблигации']:
            market = "bonds"
        else:
            market = "shares"

        query = f'http://iss.moex.com/iss/history/engines/stock/markets/{market}/securities/{ticker_in}/dates.json' #универсальный шаблон

        response = requests.get(query, headers = header)
        status_code = response.status_code

        if status_code == 200:
            data = response.json()
            tmp = []
            try:
                date_from = data['dates']['data'][0][0]
                # print(date_from)
                date_till = data['dates']['data'][0][1]
                # print(date_till)

                tmp.append(ticker_in)
                tmp.append(date_from)
                tmp.append(date_till)

                result.append(tmp)

                time.sleep(10)
            except:
                errors.append(ticker_in)
        else:
            print(status_code, ticker_in) ### сделать нормальную обработку ошибок

    result = pd.DataFrame(result, columns=['ticker','date_from','date_till'])
    
    return result, errors

# %%
def main():
    # Рабочие действия выполняются только при явном запуске скрипта.
    import data_gathering

    all_stocks_ru = data_gathering.moex_tickerlists(current_path)

    moex_df = all_stocks_ru[all_stocks_ru['TRADE_CODE'] != ''][['SUPERTYPE','TRADE_CODE']]
    moex_df.reset_index(drop=True, inplace=True)
    len(moex_df)

    # moex_df_tmp = moex_df.head(10)
    result, errors = get_ticker_dates(moex_df)

    result.to_excel(("{}/datasets/ticker_lists/ticker_dates.xlsx").format(current_path))

    #тест кейс
    len(result) == len(moex_df)

    result

    errors

    # max(result[result['date_till']!='null']['date_till'])
    # result[result['date_till']!='null']['date_till']
    pd.to_datetime(result['date_till'], errors='coerce').max()

    pd.to_datetime(result['date_from'], errors='coerce').min()

    result

    ticker_in = "SBER"
    ticker_type = "Акции"
    end_date_mx = "2011-11-21"
    start_date_mx = "2025-03-24"
    interval = 24
    df_ticker = data_gathering.moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
    df_ticker

    len(result[result['date_till'] == "2025-03-24"])

    result[result['date_till'] != "2025-03-24"]

# %%
if __name__ == "__main__":
    main()
