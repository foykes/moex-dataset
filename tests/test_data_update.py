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
    # файлы отсортированы по тикеру (GAZP вставлен перед SBER)
    assert df30['ticker'].tolist() == sorted(df30['ticker'].tolist())


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


def test_full_reload_resumes_after_interruption(project):
    current_path, calls, answers = project
    for ticker in ['AAA', 'BBB', 'CCC']:
        answers[ticker] = [candle(ticker, today - 2 * day, 1), candle(ticker, today - day, 2)]

    all_stocks_ru = pd.DataFrame({'TRADE_CODE': ['CCC', 'AAA', 'BBB']})
    tickers_dates = pd.DataFrame({'TRADE_CODE': [], 'issue_date': [], 'stopped_date': []})
    datasets = current_path / 'datasets'

    # имитация прерванного запуска: AAA готов, BBB записан наполовину (обрезанная строка)
    pd.DataFrame(answers['AAA']).to_csv(datasets / 'r.partial.csv', index=False)
    size_after_aaa = (datasets / 'r.partial.csv').stat().st_size
    with open(datasets / 'r.partial.csv', 'a') as f:
        f.write('1,1,3,0.5,100,10,2026-01-01 00:00:00,2026-01-0')
    (datasets / 'r.partial.progress').write_text('AAA,{}\n'.format(size_after_aaa))

    rows = data_gathering.full_reload(all_stocks_ru, 24, 1, 'r', 'часа', current_path, tickers_dates)

    assert rows == 6
    assert [c[0] for c in calls] == ['BBB', 'CCC']
    df = pd.read_csv(datasets / 'r.csv')
    assert df['ticker'].tolist() == ['AAA', 'AAA', 'BBB', 'BBB', 'CCC', 'CCC']
    assert not (datasets / 'r.partial.csv').exists()
    assert not (datasets / 'r.partial.progress').exists()


def test_unsorted_old_file_is_sorted_and_updated(project):
    current_path, calls, answers = project
    rows = [candle('SBER', today - 3 * day, 1), candle('GAZP', today - 3 * day, 1), candle('SBER', today - 2 * day, 2)]
    pd.DataFrame(rows).to_csv(current_path / 'datasets' / 'old.csv', index=False)

    all_stocks_ru = pd.DataFrame({'TRADE_CODE': ['SBER', 'GAZP']})
    tickers_dates = pd.DataFrame({'TRADE_CODE': [], 'issue_date': [], 'stopped_date': []})
    config = [{'interval': 24, 'years': 1, 'filename': 'old', 'word': 'часа'}]

    data_gathering.data_update(config, current_path, all_stocks_ru, tickers_dates)

    df = pd.read_csv(current_path / 'datasets' / 'old.csv')
    assert df['ticker'].tolist() == ['GAZP', 'SBER', 'SBER']


def test_to_datetime_safe_keeps_iso_month_and_day():
    series = pd.Series(['2026-11-03 00:00:00', '2026-11-03', '05/06/2025 10:00', None], dtype=object)
    result = data_gathering.to_datetime_safe(series)
    assert result.tolist()[:3] == [pd.Timestamp('2026-11-03'), pd.Timestamp('2026-11-03'), pd.Timestamp('2025-06-05 10:00')]
    assert pd.isna(result.iloc[3])
