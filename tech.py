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
import talib, pandas as pd
import sys, os, json
from pathlib import Path
from dataset_files import iter_ticker_groups, append_csv, sort_dataset_file, update_xlsx, NotGroupedError

current_path = Path(sys.path[0])

# %%
### Все доступные функции в пакете TA-lib

# print(talib.get_functions())
# print(talib.get_function_groups())

# %%
### Колонки индикаторов. Перед пересчётом удаляем их из файла,
### чтобы пересчитать индикаторы без перевыгрузки данных.
### Сюда же входят старые имена, которые сейчас не считаются (MAVP, SMA, BETA и т.д.)
indicator_columns = [
    ## Overlap Studies Functions
    'BBANDS_upperband', 'BBANDS_middleband', 'BBANDS_lowerband', 'DEMA', 'EMA', 'HT_TRENDLINE', 'KAMA', 'MA',
    'MAMA_mama', 'MAMA_fama', 'MAVP', 'MIDPOINT', 'MIDPRICE', 'SAR', 'SAREXT', 'SMA', 'T3', 'TEMA', 'TRIMA', 'WMA',
    ## Momentum Indicator Functions
    'ADX', 'ADXR', 'APO', 'AROON_down', 'AROON_up', 'AROONOSC', 'BOP', 'CCI', 'CMO', 'DX',
    'MACD_real', 'MACD_signal', 'MACD_hist', 'MACDEXT_real', 'MACDEXT_signal', 'MACDEXT_hist',
    'MACDFIX_real', 'MACDFIX_signal', 'MACDFIX_hist', 'MFI', 'MINUS_DI', 'MINUS_DM', 'MOM', 'PLUS_DI', 'PLUS_DM',
    'PPO', 'ROC', 'ROCP', 'ROCR', 'ROCR100', 'RSI14', 'STOCH_slowk', 'STOCH_slowd', 'STOCHF_fastk', 'STOCHF_fastd',
    'STOCHRSI_fastk', 'STOCHRSI_fastd', 'TRIX', 'ULTOSC', 'WILLR',
    ## Volatility Indicator Functions
    'ATR', 'NATR', 'TRANGE',
    ## Volume Indicator Functions
    'AD', 'ADOSC', 'OBV',
    ## Price Transform Functions
    'AVGPRICE', 'MEDPRICE', 'TYPPRICE', 'WCLPRICE',
    ## Cycle Indicator Functions
    'HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR_inphase', 'HT_PHASOR_quadrature', 'HT_SINE_sine', 'HT_SINE_leadsine', 'HT_TRENDMODE',
    ## Statistic Functions
    'BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'STDDEV', 'TSF', 'VAR',
    ## Другие показатели
    'HighsLows', 'BullBearPower',
]


### Расчёт всех индикаторов для одного тикера.
### df_ticker — свечи одного тикера, отсортированные по времени.
### Возвращает DataFrame с колонками индикаторов и тем же индексом, что у df_ticker
def ticker_indicators(df_ticker):
    ind = {}

    ## Overlap Studies Functions
    ind['BBANDS_upperband'], ind['BBANDS_middleband'], ind['BBANDS_lowerband'] = talib.BBANDS(df_ticker['close'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
    ind['DEMA'] = talib.DEMA(df_ticker['close'], timeperiod=30)
    ind['EMA'] = talib.EMA(df_ticker['close'], timeperiod=30)
    ind['HT_TRENDLINE'] = talib.HT_TRENDLINE(df_ticker['close'])
    ind['KAMA'] = talib.KAMA(df_ticker['close'], timeperiod=30)
    ind['MA'] = talib.MA(df_ticker['close'], timeperiod=30, matype=0)
    # ind['MAMA_mama'], ind['MAMA_fama'] = talib.MAMA(df_ticker['close'], fastlimit=0, slowlimit=0)
    # разобраться с периодами
    # ind['MAVP'] = talib.MAVP(df_ticker['close'], periods, minperiod=2, maxperiod=30, matype=0)
    ind['MIDPOINT'] = talib.MIDPOINT(df_ticker['close'], timeperiod=14)
    ind['MIDPRICE'] = talib.MIDPRICE(df_ticker['high'], df_ticker['low'], timeperiod=14)
    ind['SAR'] = talib.SAR(df_ticker['high'], df_ticker['low'], acceleration=0, maximum=0)
    ind['SAREXT'] = talib.SAREXT(df_ticker['high'], df_ticker['low'], startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)
    ind['T3'] = talib.T3(df_ticker['high'], timeperiod=5, vfactor=0)
    ind['TEMA'] = talib.TEMA(df_ticker['close'], timeperiod=30)
    ind['TRIMA'] = talib.TRIMA(df_ticker['close'], timeperiod=30)
    ind['WMA'] = talib.WMA(df_ticker['close'], timeperiod=30)

    ## Momentum Indicator Functions
    ind['ADX'] = talib.ADX(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['ADXR'] = talib.ADXR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['APO'] = talib.APO(df_ticker['close'], fastperiod=12, slowperiod=26, matype=0)
    ind['AROON_down'], ind['AROON_up'] = talib.AROON(df_ticker['high'], df_ticker['low'], timeperiod=14)
    ind['AROONOSC'] = talib.AROONOSC(df_ticker['high'], df_ticker['low'], timeperiod=14)
    ind['BOP'] = talib.BOP(df_ticker['open'], df_ticker['high'], df_ticker['low'], df_ticker['close'])
    ind['CCI'] = talib.CCI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['CMO'] = talib.CMO(df_ticker['close'], timeperiod=14) # тут не уверен, в доке real указано брать
    ind['DX'] = talib.DX(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['MACD_real'], ind['MACD_signal'], ind['MACD_hist'] = talib.MACD(df_ticker['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    ind['MACDEXT_real'], ind['MACDEXT_signal'], ind['MACDEXT_hist'] = talib.MACDEXT(df_ticker['close'], fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
    ind['MACDFIX_real'], ind['MACDFIX_signal'], ind['MACDFIX_hist'] = talib.MACDFIX(df_ticker['close'], signalperiod=9)
    ind['MFI'] = talib.MFI(df_ticker['high'], df_ticker['low'], df_ticker['close'], df_ticker['volume'], timeperiod=14)
    ind['MINUS_DI'] = talib.MINUS_DI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['MINUS_DM'] = talib.MINUS_DM(df_ticker['high'], df_ticker['low'], timeperiod=14)
    ind['MOM'] = talib.MOM(df_ticker['close'], timeperiod=100)
    ind['PLUS_DI'] = talib.PLUS_DI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['PLUS_DM'] = talib.PLUS_DM(df_ticker['high'], df_ticker['low'], timeperiod=14)
    ind['PPO'] = talib.PPO(df_ticker['close'], fastperiod=12, slowperiod=26, matype=0)
    ind['ROC'] = talib.ROC(df_ticker['close'], timeperiod=10)
    ind['ROCP'] = talib.ROCP(df_ticker['close'], timeperiod=10)
    ind['ROCR'] = talib.ROCR(df_ticker['close'], timeperiod=10)
    ind['ROCR100'] = talib.ROCR100(df_ticker['close'], timeperiod=10)
    ind['RSI14'] = talib.RSI(df_ticker['close'], timeperiod=14)
    # раньше в STOCH_slowd по ошибке писался slowk
    ind['STOCH_slowk'], ind['STOCH_slowd'] = talib.STOCH(df_ticker['high'], df_ticker['low'], df_ticker['close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    ind['STOCHF_fastk'], ind['STOCHF_fastd'] = talib.STOCHF(df_ticker['high'], df_ticker['low'], df_ticker['close'], fastk_period=5, fastd_period=3, fastd_matype=0)
    ind['STOCHRSI_fastk'], ind['STOCHRSI_fastd'] = talib.STOCHRSI(df_ticker['close'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    # раньше TRIX считался, но из-за опечатки не попадал в файл
    ind['TRIX'] = talib.TRIX(df_ticker['close'], timeperiod=30)
    ind['ULTOSC'] = talib.ULTOSC(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
    ind['WILLR'] = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)

    ## Volatility Indicator Functions
    ind['ATR'] = talib.ATR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['NATR'] = talib.NATR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
    ind['TRANGE'] = talib.TRANGE(df_ticker['high'], df_ticker['low'], df_ticker['close'])

    ## Volume Indicator Functions
    ind['AD'] = talib.AD(df_ticker['high'], df_ticker['low'], df_ticker['close'], df_ticker['volume'])
    ind['ADOSC'] = talib.ADOSC(df_ticker['high'], df_ticker['low'], df_ticker['close'], df_ticker['volume'], fastperiod=3, slowperiod=10)
    ind['OBV'] = talib.OBV(df_ticker['close'], df_ticker['volume'])

    ## Price Transform Functions
    ind['AVGPRICE'] = talib.AVGPRICE(df_ticker['open'], df_ticker['high'], df_ticker['low'], df_ticker['close'])
    ind['MEDPRICE'] = talib.MEDPRICE(df_ticker['high'], df_ticker['low'])
    ind['TYPPRICE'] = talib.TYPPRICE(df_ticker['high'], df_ticker['low'], df_ticker['close'])
    ind['WCLPRICE'] = talib.WCLPRICE(df_ticker['high'], df_ticker['low'], df_ticker['close'])

    ## Cycle Indicator Functions
    ind['HT_DCPERIOD'] = talib.HT_DCPERIOD(df_ticker['close'])
    ind['HT_DCPHASE'] = talib.HT_DCPHASE(df_ticker['close'])
    ind['HT_PHASOR_inphase'], ind['HT_PHASOR_quadrature'] = talib.HT_PHASOR(df_ticker['close'])
    ind['HT_SINE_sine'], ind['HT_SINE_leadsine'] = talib.HT_SINE(df_ticker['close'])
    ind['HT_TRENDMODE'] = talib.HT_TRENDMODE(df_ticker['close'])

    ## Statistic Functions
    # для реализации нужно сперва протащить данные по индексам, так как он используется в значении real10 https://github.com/TA-Lib/ta-lib/blob/master/src/ta_func/ta_BETA.c#L207
    # ind['BETA'] = talib.BETA(real0, df_ticker['close'], timeperiod=5)
    # ind['CORREL'] = talib.CORREL(real10, df_ticker['close'], timeperiod=30)
    ind['LINEARREG'] = talib.LINEARREG(df_ticker['close'], timeperiod=14)
    ind['LINEARREG_ANGLE'] = talib.LINEARREG_ANGLE(df_ticker['close'], timeperiod=14)
    ind['LINEARREG_INTERCEPT'] = talib.LINEARREG_INTERCEPT(df_ticker['close'], timeperiod=14)
    ind['LINEARREG_SLOPE'] = talib.LINEARREG_SLOPE(df_ticker['close'], timeperiod=14)
    ind['STDDEV'] = talib.STDDEV(df_ticker['close'], timeperiod=5, nbdev=1)
    ind['TSF'] = talib.TSF(df_ticker['close'], timeperiod=14)
    ind['VAR'] = talib.VAR(df_ticker['close'], timeperiod=5, nbdev=1)

    ## Другие показатели
    # ind['HighsLows'] = ...
    # ind['BullBearPower'] = ...

    return pd.DataFrame(ind, index=df_ticker.index)


### Пересчёт индикаторов для файла датасета (CSV).
### Файл читается по тикерам и сразу дописывается в новый файл, поэтому в памяти только один тикер
### (датасет 30 лет x 1 час с индикаторами целиком в память не влезает).
### Индикаторы считаются по каждому тикеру отдельно, чтобы не смешивать строки разных тикеров.
### Рядом сохраняется XLSX из того же результата, если включён и влезает в лист Excel
def write_with_indicators(path, tmp_path, keep_columns):
    if tmp_path.is_file():
        tmp_path.unlink()

    rows = 0
    for ticker, df_ticker in iter_ticker_groups(path, keep_columns):
        # индикаторы считаются по порядку свечей, поэтому сортируем по времени
        df_ticker = df_ticker.sort_values(by='begin', kind='stable').reset_index(drop=True)
        df_ticker = df_ticker.join(ticker_indicators(df_ticker))
        rows = rows + append_csv(df_ticker, tmp_path)
    return rows


def calculator(filename, current_path = current_path):
    datasets_dir = Path(current_path) / 'datasets'
    path = datasets_dir / filename
    tmp_path = path.with_name(path.stem + '.tmp.csv')

    # старые значения индикаторов не читаем — они пересчитываются
    header = pd.read_csv(path, nrows=0).columns
    keep_columns = [i for i in header if i not in indicator_columns]

    try:
        rows = write_with_indicators(path, tmp_path, keep_columns)
    except NotGroupedError:
        # старый файл, где строки тикера идут не подряд — один раз сортируем
        sort_dataset_file(path)
        rows = write_with_indicators(path, tmp_path, keep_columns)

    if rows == 0:
        print('Файл {} пустой, пропускаю'.format(filename))
        if tmp_path.is_file():
            tmp_path.unlink()
        return

    os.replace(tmp_path, path)
    update_xlsx(path)
    print('Посчитал индикаторы для {}: {} записей'.format(filename, rows))

# %%
### Ниже блоки для проверки отдельно функции

# filename = "30years_data_1h_interval.csv"
# path =  current_path + "/datasets/" + filename
# df = pd.read_csv(path)
# ticker_list = df['ticker'].to_list()
# ticker_list = list(set(ticker_list))

# for i in range(0,len(ticker_list)):
#     ticker = ticker_list[i]
#     df_ticker = df[(df['ticker'] == ticker)]

# %%
# real = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
# real

# %%
### Конфиг датасетов: тот же, что у data_gathering (переменная окружения MOEX_CONFIG)
### У датасета в конфиге можно выключить расчёт индикаторов: "tech": false
def main(current_path):
    current_path = Path(current_path)
    config_name = os.environ.get('MOEX_CONFIG', 'datasets_config_1_year.json')
    with open(current_path / 'settings' / config_name, 'r', encoding='utf-8') as f:
        config = json.load(f)

    failed = []
    for dataset in config:
        filename = dataset['filename'] + '.csv'

        if not (current_path / 'datasets' / filename).is_file():
            print('Файла {} нет, пропускаю'.format(filename))
            continue
        if dataset.get('tech', True) == False:
            print('Для {} индикаторы выключены в конфиге'.format(filename))
            update_xlsx(current_path / 'datasets' / filename)
            continue

        try:
            print('Начал рассчёт файла {}'.format(filename))
            calculator(filename, current_path)
        except Exception as e:
            print(e)
            print(filename)
            print('------------------------')
            failed.append(filename)

    if len(failed) > 0:
        print('Не удалось посчитать индикаторы для файлов: {}'.format(failed))
    return failed


# %%
if __name__ == "__main__": 
    main(current_path)
    print('Закончил просчитывать тех метрики для всех файлов')


