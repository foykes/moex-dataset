# Тесты чтения/записи датасетов по частям
import pandas as pd
import pytest

import dataset_files


def make_file(path, tickers_rows):
    rows = []
    for ticker, n in tickers_rows:
        for i in range(n):
            rows.append({'begin': '2026-01-{:02d} 00:00:00'.format(i + 1), 'close': i, 'ticker': ticker})
    pd.DataFrame(rows).to_csv(path, index=False)


def test_ticker_groups_across_chunk_borders(tmp_path):
    path = tmp_path / 'a.csv'
    make_file(path, [('AAA', 5), ('BBB', 7), ('CCC', 1)])

    groups = [(t, len(df)) for t, df in dataset_files.iter_ticker_groups(path, chunksize=3)]

    assert groups == [('AAA', 5), ('BBB', 7), ('CCC', 1)]


def test_columns_keep_requested_order(tmp_path):
    path = tmp_path / 'a.csv'
    make_file(path, [('AAA', 2)])

    ticker, df = next(dataset_files.iter_ticker_groups(path, ['ticker', 'begin']))

    assert list(df.columns) == ['ticker', 'begin']


def test_not_grouped_file_is_detected(tmp_path):
    path = tmp_path / 'a.csv'
    make_file(path, [('AAA', 2), ('BBB', 2), ('AAA', 2)])

    with pytest.raises(dataset_files.NotGroupedError):
        list(dataset_files.iter_ticker_groups(path, chunksize=2))


def test_update_xlsx_respects_excel_limit(tmp_path, monkeypatch):
    path = tmp_path / 'a.csv'
    make_file(path, [('AAA', 5)])
    (tmp_path / 'a.xlsx').write_text('старый')

    monkeypatch.setattr(dataset_files, 'EXCEL_MAX_ROWS', 5)
    assert not dataset_files.update_xlsx(path)
    assert not (tmp_path / 'a.xlsx').exists()

    monkeypatch.setattr(dataset_files, 'EXCEL_MAX_ROWS', 100)
    assert dataset_files.update_xlsx(path)
    back = pd.read_excel(tmp_path / 'a.xlsx')
    assert len(back) == 5 and list(back.columns) == ['begin', 'close', 'ticker']


def test_big_dataset_published_as_gz_small_as_xlsx(tmp_path, monkeypatch):
    import gzip
    path = tmp_path / 'a.csv'
    make_file(path, [('AAA', 5)])
    (tmp_path / 'a.xlsx').write_text('старый')

    monkeypatch.setattr(dataset_files, 'EXCEL_MAX_ROWS', 5)
    assert dataset_files.update_published_files(path) == 'gz'
    assert not (tmp_path / 'a.xlsx').exists()
    with gzip.open(tmp_path / 'a.csv.gz', 'rb') as f:
        assert f.read() == path.read_bytes()

    monkeypatch.setattr(dataset_files, 'EXCEL_MAX_ROWS', 100)
    assert dataset_files.update_published_files(path) == 'xlsx'
    assert not (tmp_path / 'a.csv.gz').exists()
    assert (tmp_path / 'a.xlsx').is_file()
