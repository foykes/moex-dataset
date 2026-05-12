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
import unittest, json, os, sys
current_path = sys.path[0]

# %%
current_path = sys.path[0]

# %%
import data_gathering, pandas as pd


# %%
class data_gathering___moex_query(unittest.TestCase):
   def tests_moex_query(self):
        # отправляем тестовую строку в функцию
        ticker_in = "YDEX"
        end_date_mx = "2023-10-08"
        start_date_mx = "2024-10-08"
        interval = "24"
        ticker_type = "Акции"
        result = data_gathering.moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval)
        result_len = len(result)

        ## Сохранение эталонного варианта
        # result.to_csv("{}/tests/data_gathering_moex_query_YNDX.csv".format(current_path),index=False)

        ## Ожидаемый результат
        df_ticker_control = pd.read_csv("{}/tests/data_gathering_moex_query_YNDX.csv".format(current_path))
        control_len = len(df_ticker_control)
        self.assertEqual(result_len, control_len)


# %%
class data_gathering___moex(unittest.TestCase):
   def tests_moex_(self):
    ticker_in = "YDEX"
    years = 1
    interval = 24
    ticker_type = "Акции"
    df_ticker = data_gathering.moex(ticker_in, ticker_type, years, interval)

    # print(len(df_ticker))

    # ticker_list = df['ticker'].to_list()
    # ticker_list = list(set(ticker_list))
    self.assertTrue(len(df_ticker) > 0 )


# %%
ticker_in = "SBER"
years = 1
interval = 24
ticker_type = "Акции"
df_ticker = data_gathering.moex(ticker_in, ticker_type, years, interval)
df_ticker

# %%
# запускаем тестирование
if __name__ == '__main__':
    unittest.main() 

# %%
# SBERP
# "YDEX" in ticker_list

# %%
## для тестирования функции
# full_reload(1,10,'1year_data_1m_intervcal',current_path)
