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
    parser = argparse.ArgumentParser(description='Явный live-read профиль MOEX diagnostics')
    parser.add_argument('--allow-live-diagnostics', action='store_true')
    parser.add_argument('--profile', choices=['live-read'])
    arguments = parser.parse_args()
    if not arguments.allow_live_diagnostics or arguments.profile != 'live-read':
        parser.error('Нужны --allow-live-diagnostics --profile live-read; отдельно согласуйте ограниченный профиль')
    parser.error('Ограниченный live-read профиль не согласован; параметры CLI не дают разрешения на запуск')


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
import unittest, json, os, sys
current_path = sys.path[0]

# %%
current_path = sys.path[0]

# %%
import data_gathering, pandas as pd

# %%
@unittest.skip('Live MOEX diagnostics: отдельно согласуйте ограниченный live-read профиль; import не разрешает запуск')
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
@unittest.skip('Live MOEX diagnostics: отдельно согласуйте ограниченный live-read профиль; import не разрешает запуск')
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
def main():
    # Рабочие действия выполняются только при явном запуске скрипта.
    ticker_in = "SBER"
    years = 1
    interval = 24
    ticker_type = "Акции"
    df_ticker = data_gathering.moex(ticker_in, ticker_type, years, interval)
    df_ticker

    # запускаем тестирование
    unittest.main()

# %%
if __name__ == "__main__":
    main()

# %%
# SBERP
# "YDEX" in ticker_list

# %%
## для тестирования функции
# full_reload(1,10,'1year_data_1m_intervcal',current_path)
