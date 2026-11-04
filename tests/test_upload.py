# Тесты upload.py: FTP и Google подменены заглушками, реальных загрузок нет
import ftplib
import json

import numpy as np
import pandas as pd
import pytest

import upload


@pytest.fixture
def project(tmp_path):
    """Папка проекта и папка secrets рядом с ней, как на сервере."""
    project_dir = tmp_path / 'moex-dataset'
    (project_dir / 'datasets' / 'ticker_lists').mkdir(parents=True)
    secrets = tmp_path / 'secrets'
    secrets.mkdir()
    (secrets / 'moex.foykes.com.json').write_text(json.dumps({'domain': 'ftp.test', 'username': 'u', 'password': 'p'}))
    return project_dir


class FakeFTP:
    instances = []

    def __init__(self, host, user, password, timeout=None):
        self.files = {}
        self.fail_on = set()
        self.rename_fails = False
        self.encoding = None
        self.deleted = []
        self.dirs = set()
        FakeFTP.instances.append(self)

    def storbinary(self, command, file):
        name = command.split(' ', 1)[1]
        if any(name.startswith(f) for f in self.fail_on):
            raise ftplib.error_temp('451 test error')
        self.files[name] = file.read()

    def rename(self, old, new):
        if self.rename_fails and new in self.files:
            raise ftplib.error_perm('550 exists')
        self.files[new] = self.files.pop(old)

    def delete(self, name):
        if name not in self.files:
            raise ftplib.error_perm('550 no such file')
        self.files.pop(name)
        self.deleted.append(name)

    def mkd(self, name):
        self.dirs.add(name)

    def quit(self):
        pass


def test_ftp_uploads_all_folders_without_tmp_files(project, monkeypatch):
    FakeFTP.instances = []
    monkeypatch.setattr(upload.ftplib, 'FTP', FakeFTP)
    datasets = project / 'datasets'
    (datasets / '1years_data_1d_interval.csv').write_text('a')
    (datasets / '1years_data_1d_interval.tmp.csv').write_text('half')
    (datasets / 'ticker_lists' / 'moex_full.csv').write_text('b')
    # папки dividends нет — не ошибка

    upload.ftp_upload(str(project))

    files = FakeFTP.instances[0].files
    assert sorted(files) == ['1years_data_1d_interval.csv', 'ticker_lists/moex_full.csv']
    assert not any(name.endswith('.uploading') for name in files)


def test_ftp_error_in_one_file_does_not_stop_others(project, monkeypatch):
    FakeFTP.instances = []

    class FailingFTP(FakeFTP):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fail_on = {'a.csv'}

    monkeypatch.setattr(upload.ftplib, 'FTP', FailingFTP)
    (project / 'datasets' / 'a.csv').write_text('a')
    (project / 'datasets' / 'b.csv').write_text('b')

    with pytest.raises(RuntimeError, match='a.csv'):
        upload.ftp_upload(str(project))

    assert 'b.csv' in FakeFTP.instances[0].files


def test_ftp_store_replaces_existing_file(tmp_path):
    ftp = FakeFTP('h', 'u', 'p')
    ftp.rename_fails = True
    ftp.files['x.csv'] = b'old'
    local = tmp_path / 'x.csv'
    local.write_text('new')

    upload.ftp_store(ftp, str(local), 'x.csv')

    assert ftp.files == {'x.csv': b'new'}


class FakeSheet:
    def __init__(self):
        self.calls = []

    def clear(self):
        self.calls.append(('clear',))

    def resize(self, rows=None, cols=None):
        self.calls.append(('resize', rows, cols))

    def update_values(self, crange, values):
        self.calls.append(('update', crange, values))


def gdoc_setup(project, monkeypatch, n_rows):
    sheet = FakeSheet()

    class FakeClient:
        def open_by_url(self, url):
            return [sheet]

    monkeypatch.setattr(upload.pygsheets, 'authorize', lambda service_file: FakeClient())
    df = pd.DataFrame({
        'open': np.arange(n_rows, dtype=float), 'close': 1.0, 'high': 2.0, 'low': 0.5, 'value': 10.0,
        'volume': np.arange(n_rows), 'begin': '2026-01-01 00:00:00', 'end': '2026-01-01 23:59:59',
        'ticker': 'SBER', 'RSI14': np.nan, 'MACD_real': 1.0,
    })
    df.to_csv(project / 'datasets' / upload.gdoc_dataset, index=False)
    return sheet


def test_gdoc_writes_in_chunks_with_same_columns(project, monkeypatch):
    monkeypatch.setattr(upload, 'gdoc_chunk_rows', 2)
    sheet = gdoc_setup(project, monkeypatch, 3)

    upload.gdoc_upload(str(project), 'https://docs.google.com/test')

    assert sheet.calls[0] == ('clear',)
    assert sheet.calls[1] == ('resize', 4, 10)
    updates = [c for c in sheet.calls if c[0] == 'update']
    assert [u[1] for u in updates] == ['A1', 'A2', 'A4']
    values = updates[0][2] + updates[1][2] + updates[2][2]
    assert values[0] == ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker', 'RSI14']
    assert values[1][9] == 'NaN'
    assert values[3][5] == 2 and type(values[3][5]) is int


def test_gdoc_too_big_does_not_touch_sheet(project, monkeypatch):
    monkeypatch.setattr(upload, 'gdoc_max_cells', 25)
    sheet = gdoc_setup(project, monkeypatch, 3)

    with pytest.raises(RuntimeError, match='не влезают'):
        upload.gdoc_upload(str(project), 'https://docs.google.com/test')

    assert sheet.calls == []


def test_ftp_big_dataset_only_gz_and_old_csv_deleted(project, monkeypatch):
    FakeFTP.instances = []

    class ServerWithOldFiles(FakeFTP):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # на сервере лежит старый большой .csv
            self.files['10years_data_1h_interval.csv'] = b'old big csv'

    monkeypatch.setattr(upload.ftplib, 'FTP', ServerWithOldFiles)
    datasets = project / 'datasets'
    (datasets / '10years_data_1h_interval.csv').write_text('big')
    (datasets / '10years_data_1h_interval.csv.gz').write_bytes(b'gz')
    (datasets / '1years_data_1d_interval.csv').write_text('small')
    (datasets / '1years_data_1d_interval.partial.csv').write_text('half')
    (datasets / 'dividends' / 'dohodru').mkdir(parents=True)
    (datasets / 'dividends' / 'dohodru' / 'all_payments.csv').write_text('d')

    upload.ftp_upload(str(project))

    ftp = FakeFTP.instances[0]
    assert sorted(ftp.files) == ['10years_data_1h_interval.csv.gz', '1years_data_1d_interval.csv',
                                 'dividends/dohodru/all_payments.csv']
    assert ftp.deleted == ['10years_data_1h_interval.csv']
    assert 'dividends/dohodru' in ftp.dirs
