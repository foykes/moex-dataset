# Тесты tech.py на синтетических свечах (без API)
import numpy as np
import pandas as pd
import talib

import tech
from xlsx_utils import write_xlsx


BASE_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']


def make_candles(ticker, n, seed):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    begin = pd.date_range('2025-01-01', periods=n, freq='h')
    return pd.DataFrame({
        'open': close + rng.normal(0, 0.5, n),
        'close': close,
        'high': close + 2,
        'low': close - 2,
        'value': rng.integers(1000, 5000, n).astype(float),
        'volume': rng.integers(10, 50, n),
        'begin': begin.strftime('%Y-%m-%d %H:%M:%S'),
        'end': (begin + pd.Timedelta(minutes=59)).strftime('%Y-%m-%d %H:%M:%S'),
        'ticker': ticker,
    })


def write_dataset(tmp_path, df, name='test.csv'):
    datasets = tmp_path / 'datasets'
    datasets.mkdir(exist_ok=True)
    df.to_csv(datasets / name, index=False)
    return datasets / name


def test_indicators_are_calculated_per_ticker(tmp_path):
    sber = make_candles('SBER', 300, 1)
    gazp = make_candles('GAZP', 200, 2)
    # перемешиваем строки — calculator сам сортирует по тикеру и времени
    mixed = pd.concat([sber, gazp]).sample(frac=1, random_state=0)
    path = write_dataset(tmp_path, mixed)

    tech.calculator('test.csv', tmp_path)
    df = pd.read_csv(path)

    assert len(df) == 500
    gazp_result = df[df['ticker'] == 'GAZP'].reset_index(drop=True)
    expected_rsi = talib.RSI(gazp['close'], timeperiod=14)
    assert np.allclose(gazp_result['RSI14'], expected_rsi, equal_nan=True)
    # первые 14 значений RSI у второго тикера пустые: строки тикеров не смешались
    assert gazp_result['RSI14'].iloc[:14].isna().all()


def test_columns_and_fixed_bugs(tmp_path):
    path = write_dataset(tmp_path, make_candles('SBER', 300, 3))

    tech.calculator('test.csv', tmp_path)
    df = pd.read_csv(path)

    # базовые колонки остаются первыми и в прежнем порядке
    assert list(df.columns[:9]) == BASE_COLUMNS
    # порядок индикаторов — как в indicator_columns
    indicators = list(df.columns[9:])
    assert indicators == [c for c in tech.indicator_columns if c in indicators]
    assert len(indicators) == 80
    # STOCH_slowd больше не копия slowk, TRIX попадает в файл
    assert not np.allclose(df['STOCH_slowd'], df['STOCH_slowk'], equal_nan=True)
    assert df['TRIX'].notna().sum() > 0


def test_recalculation_replaces_old_indicators(tmp_path):
    path = write_dataset(tmp_path, make_candles('SBER', 120, 4))

    tech.calculator('test.csv', tmp_path)
    first = pd.read_csv(path)
    tech.calculator('test.csv', tmp_path)
    second = pd.read_csv(path)

    assert list(first.columns) == list(second.columns)
    pd.testing.assert_frame_equal(first, second)


def test_xlsx_written_and_readable(tmp_path):
    path = write_dataset(tmp_path, make_candles('SBER', 50, 5))

    tech.calculator('test.csv', tmp_path)

    csv = pd.read_csv(path)
    xlsx = pd.read_excel(path.with_suffix('.xlsx'))
    assert list(xlsx.columns) == list(csv.columns)
    assert len(xlsx) == len(csv)
    assert np.allclose(xlsx['RSI14'], csv['RSI14'], equal_nan=True)
    assert xlsx['volume'].tolist() == csv['volume'].tolist()


def test_write_xlsx_types(tmp_path):
    df = pd.DataFrame({
        'a': np.array([1, 2], dtype='int64'),
        'b': [1.5, np.nan],
        'c': ['=1+1', 'text'],
        'd': pd.to_datetime(['2026-11-03 10:00:00', None]),
    })
    path = tmp_path / 'x.xlsx'

    assert write_xlsx(df, path)
    back = pd.read_excel(path)

    assert back['a'].tolist() == [1, 2]
    assert back['b'].iloc[0] == 1.5 and pd.isna(back['b'].iloc[1])
    # строки не превращаются в формулы
    assert back['c'].tolist() == ['=1+1', 'text']
    assert back['d'].iloc[0] == pd.Timestamp('2026-11-03 10:00:00') and pd.isna(back['d'].iloc[1])
