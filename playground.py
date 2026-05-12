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
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json


today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []
current_path = sys.path[0]



# %%
current_path = "/Users/nkukharev/Documents/petprojects/prod/moex-dataset/"
header = "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


# %%
### Функция запроса к API по тикеру, датам и нужному интервалу
def moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval, max_retries=5, retry_sleep_start=3):
    global header
    global headers_full
    
    df_ticker = pd.DataFrame()
    df = pd.DataFrame()

    # Определение типа market для корректного запроса
    if ticker_type in ['Инвестиционные паи', 'Депозитарные расписки', 'Акции', 'Ипотечные сертификаты участия']:
        market = "shares"
    elif ticker_type in ['Облигации', 'Еврооблигации']:
        market = "bonds"
    else:
        market = "shares"
        
    query = f'http://iss.moex.com/iss/engines/stock/markets/{market}/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #универсальный шаблон
    # query = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для акций
    # query = f'http://iss.moex.com/iss/engines/stock/markets/bonds/securities/{ticker_in}/candles.csv?from={end_date_mx}&till={start_date_mx}&interval={interval}' #шаблон для облигаций

    attempt = 0
    success = False
    last_status_code = None
    last_error = None

    # Если global header почему-то не задан или пустой —
    # инициализируем первым User-Agent из headers_full
    if not isinstance(header, dict) or len(header) == 0:
        header = get_next_header(None)

    while attempt < max_retries and not success:
        attempt += 1

        try:
            response = requests.get(query, headers=header, timeout=30)
            last_status_code = response.status_code

            # Читаем CSV если статус успешный
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text), sep=';', header=1)

                # Пауза после успешного запроса, чтобы не перегружать API Мосбиржи
                time.sleep(3)

                success = True

            else:
                print(
                    f'Попытка {attempt}/{max_retries} неуспешна. '
                    f'Status code: {response.status_code}, '
                    f'ticker: {ticker_in}, '
                    f'from: {end_date_mx}, till: {start_date_mx}, interval: {interval}'
                )
                print(query)

        except Exception as e:
            last_error = e
            print(
                f'Попытка {attempt}/{max_retries} завершилась ошибкой: {e}. '
                f'ticker: {ticker_in}, '
                f'from: {end_date_mx}, till: {start_date_mx}, interval: {interval}'
            )
            print(query)

        # Если попытка неуспешна — меняем header и ждём перед следующей попыткой
        if not success and attempt < max_retries:
            old_user_agent = header.get('User-Agent') if isinstance(header, dict) else None

            header = get_next_header(header)

            new_user_agent = header.get('User-Agent') if isinstance(header, dict) else None

            print('Меняем User-Agent:')
            print(f'old: {old_user_agent}')
            print(f'new: {new_user_agent}')

            # Экспоненциальная пауза после неудачной попытки:
            # 1-я ошибка -> 3 сек.
            # 2-я ошибка -> 6 сек.
            # 3-я ошибка -> 12 сек.
            # 4-я ошибка -> 24 сек.
            sleep_time = retry_sleep_start * (2 ** (attempt - 1))

            print(f'Ждём {sleep_time} сек. перед следующей попыткой...')
            time.sleep(sleep_time)

    if not success:
        print(
            f'Не удалось получить данные после {max_retries} попыток. '
            f'ticker: {ticker_in}, '
            f'last_status_code: {last_status_code}, '
            f'last_error: {last_error}'
        )
        return df_ticker

    if len(df) > 0:
        df['ticker'] = ticker_in
        df_ticker = pd.concat([df_ticker, df], ignore_index=True)

    return df_ticker

