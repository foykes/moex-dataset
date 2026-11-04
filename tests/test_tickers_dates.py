# Тесты build_tickers_dates с заглушкой ISS
import pandas as pd
import requests

import data_gathering


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeSession:
    def __init__(self):
        self.calls = 0
        self.failed_once = False

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        # первая попытка падает сетевой ошибкой — должна сработать повторная
        if not self.failed_once:
            self.failed_once = True
            raise requests.ConnectionError('сеть моргнула')
        if '/boards/TQBR/securities/OKEY.json' in url:
            return FakeResponse({'history': {'columns': ['TRADEDATE'], 'data': [['2025-03-14']]}})
        if url.endswith('/OKEY.json'):
            return FakeResponse({'description': {'columns': ['name', 'value'], 'data': [['ISSUEDATE', '2010-11-05']]}})
        if url.endswith('/securities.json'):
            return FakeResponse({'securities': {'columns': ['secid', 'group', 'is_traded', 'primary_boardid'],
                                                'data': [['RU000A1084E6', 'stock_bonds', 1, 'TQCB'],
                                                         ['OKEY', 'stock_dr', 0, 'TQBR']]}})
        raise AssertionError(url)


def test_depositary_receipt_gets_dates_after_retry(tmp_path, monkeypatch):
    session = FakeSession()
    monkeypatch.setattr(data_gathering.requests, 'Session', lambda: session)
    monkeypatch.setattr(data_gathering.time, 'sleep', lambda s: None)

    result = data_gathering.build_tickers_dates(pd.DataFrame({'TRADE_CODE': ['OKEY']}), tmp_path)

    row = result.iloc[0]
    assert row['issue_date'] == pd.Timestamp('2010-11-05')
    # раньше для расписок (group stock_dr) дата делистинга не находилась
    assert row['stopped_date'] == pd.Timestamp('2025-03-14')
    assert (tmp_path / 'datasets' / 'ticker_lists' / 'tickers_dates.csv').is_file()
