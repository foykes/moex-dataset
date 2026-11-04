# Тесты обновления датасетов без реального API MOEX: moex_query подменяется заглушкой
import datetime

import pandas as pd
import pytest

import data_gathering


today = datetime.datetime.now()
day = datetime.timedelta(days=1)
COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']


def candle(ticker, begin, close):
    return {'open': 1, 'close': close, 'high': 3, 'low': 0.5, 'value': 100, 'volume': 10,
            'begin': begin.strftime('%Y-%m-%d 00:00:00'), 'end': begin.strftime('%Y-%m-%d 23:59:59'),
            'ticker': ticker}


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Временная папка проекта с каталогом MOEX и заглушкой moex_query."""
    ticker_lists = tmp_path / 'datasets' / 'ticker_lists'
    ticker_lists.mkdir(parents=True)
    pd.DataFrame({'TRADE_CODE': ['SBER', 'GAZP'], 'SUPERTYPE': ['Акции', 'Акции']}).to_csv(ticker_lists / 'moex_full.csv')

    calls = []
    answers = {}

    def fake_moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval, *args, **kwargs):
        calls.append((ticker_in, end_date_mx, start_date_mx, interval))
        return pd.DataFrame(answers.get(ticker_in, []), columns=COLUMNS)

    monkeypatch.setattr(data_gathering, 'moex_query', fake_moex_query)
    return tmp_path, calls, answers


def test_update_dedupes_new_ticker_and_slices_short_period(project):
    current_path, calls, answers = project

    # существующий 30-летний файл: SBER, последняя свеча вчера (close=5, незавершённая)
    # и очень старая свеча 31 год назад, которая должна уйти за границу периода
    old_rows = [
        candle('SBER', today - 31 * 365 * day, 1),
        candle('SBER', today - 20 * 365 * day, 2),
        candle('SBER', today - 2 * day, 4),
        candle('SBER', today - day, 5),
    ]
    pd.DataFrame(old_rows).to_csv(current_path / 'datasets' / 'test30.csv', index=False)

    # API: вчерашняя свеча с новым значением close=6 + сегодняшняя; GAZP — новый тикер
    answers['SBER'] = [candle('SBER', today - day, 6), candle('SBER', today, 7)]
    answers['GAZP'] = [candle('GAZP', today - 5 * 365 * day, 1), candle('GAZP', today, 2)]

    all_stocks_ru = pd.DataFrame({'TRADE_CODE': ['SBER', 'GAZP']})
    tickers_dates = pd.DataFrame({'TRADE_CODE': ['SBER', 'GAZP'],
                                  'issue_date': [pd.NaT, pd.Timestamp(today - 6 * 365 * day)],
                                  'stopped_date': [pd.NaT, pd.NaT]})
    config = [
        {'interval': 24, 'years': 10, 'filename': 'test10', 'word': 'часа'},
        {'interval': 24, 'years': 30, 'filename': 'test30', 'word': 'часа'},
    ]

    data_gathering.data_update(config, current_path, all_stocks_ru, tickers_dates)

    df30 = pd.read_csv(current_path / 'datasets' / 'test30.csv')
    sber = df30[df30['ticker'] == 'SBER']
    # старая свеча (31 год) вырезана, вчерашняя одна и со свежим close
    assert len(sber) == 4
    yesterday = sber[sber['begin'].str.startswith((today - day).strftime('%Y-%m-%d'))]
    assert yesterday['close'].tolist() == [6]
    # новый тикер добавлен за весь период (с даты листинга), а не за 1 год
    assert len(df30[df30['ticker'] == 'GAZP']) == 2
    gazp_call = [c for c in calls if c[0] == 'GAZP'][0]
    assert gazp_call[1] == (today - 6 * 365 * day).strftime('%Y-%m-%d')

    # 10-летний файл вырезан из 30-летнего без повторного скачивания
    assert len(calls) == 2
    df10 = pd.read_csv(current_path / 'datasets' / 'test10.csv')
    assert len(df10) == 5  # SBER: 3 свечи за 10 лет (без 20-летней), GAZP: 2
    assert list(df10.columns) == COLUMNS
    assert (current_path / 'datasets' / 'test10.xlsx').is_file()


def test_missing_file_is_downloaded_from_scratch(project):
    current_path, calls, answers = project
    answers['SBER'] = [candle('SBER', today - day, 1)]

    all_stocks_ru = pd.DataFrame({'TRADE_CODE': ['SBER']})
    tickers_dates = pd.DataFrame({'TRADE_CODE': ['SBER'], 'issue_date': [pd.NaT], 'stopped_date': [pd.NaT]})
    config = [{'interval': 60, 'years': 1, 'filename': 'test1', 'word': 'минут'}]

    data_gathering.data_update(config, current_path, all_stocks_ru, tickers_dates)

    assert len(pd.read_csv(current_path / 'datasets' / 'test1.csv')) == 1
    # пустая дата листинга -> левая граница = сегодня - 1 год, а не NaT
    assert calls[0][1] == (today - 365 * day).strftime('%Y-%m-%d')


def test_ticker_date_range():
    tickers_dates = pd.DataFrame({
        'TRADE_CODE': ['OLD', 'NEW', 'DELISTED'],
        'issue_date': [pd.Timestamp('2000-01-01'), pd.Timestamp('2025-06-01'), pd.NaT],
        'stopped_date': [pd.NaT, pd.NaT, pd.Timestamp('2024-03-15')],
    })
    now = datetime.datetime(2026, 1, 10)

    assert data_gathering.ticker_date_range('OLD', 10, tickers_dates, now) == ('2016-01-13', '2026-01-10')
    assert data_gathering.ticker_date_range('NEW', 10, tickers_dates, now) == ('2025-06-01', '2026-01-10')
    assert data_gathering.ticker_date_range('DELISTED', 10, tickers_dates, now) == ('2016-01-13', '2024-03-15')
    assert data_gathering.ticker_date_range('OLD', 0, tickers_dates, now) == ('2000-01-01', '2026-01-10')
    assert data_gathering.ticker_date_range('UNKNOWN', 0, tickers_dates, now) == ('1991-12-26', '2026-01-10')


def test_big_dataset_skips_xlsx_and_removes_stale_one(tmp_path, monkeypatch):
    datasets = tmp_path / 'datasets'
    datasets.mkdir()
    (datasets / 'big.xlsx').write_text('старый файл')

    df = pd.DataFrame({'a': [1, 2, 3]})
    monkeypatch.setattr(pd.DataFrame, '__len__', lambda self: 2_000_000)
    data_gathering.save_dataset(df, 'big', tmp_path)
    monkeypatch.undo()

    assert (datasets / 'big.csv').is_file()
    assert not (datasets / 'big.xlsx').exists()


def test_to_datetime_safe_keeps_iso_month_and_day():
    series = pd.Series(['2026-11-03 00:00:00', '2026-11-03', '05/06/2025 10:00', None], dtype=object)
    result = data_gathering.to_datetime_safe(series)
    assert result.tolist()[:3] == [pd.Timestamp('2026-11-03'), pd.Timestamp('2026-11-03'), pd.Timestamp('2025-06-05 10:00')]
    assert pd.isna(result.iloc[3])
