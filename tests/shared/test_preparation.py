"""Literal oracles for existing preparation functions; all IO is offline."""

import hashlib
import importlib.util
from io import StringIO
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / 'tests/fixtures/shared'


def _hashes():
    return {name: hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
            for name in ('candles.csv', 'ticker_dates.json')}


def _forbidden_writer(*args, **kwargs):
    raise AssertionError('Preparation tests forbid real dataset writers')


def _data_gathering(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    import data_gathering
    monkeypatch.setattr(data_gathering, 'headers_full', {'chrome': ['AGENT_A'], 'edge': ['AGENT_B']})
    monkeypatch.setattr(data_gathering, 'header', {'User-Agent': 'AGENT_A'})
    return data_gathering


def test_header_rotation_preserves_fields_and_wraps(monkeypatch):
    module = _data_gathering(monkeypatch)
    original = {'User-Agent': 'AGENT_A', 'Accept': 'text/csv'}
    assert module.get_next_header(None) == {'User-Agent': 'AGENT_A'}
    assert module.get_next_header(original) == {'User-Agent': 'AGENT_B', 'Accept': 'text/csv'}
    assert original == {'User-Agent': 'AGENT_A', 'Accept': 'text/csv'}
    assert module.get_next_header({'User-Agent': 'AGENT_B'}) == {'User-Agent': 'AGENT_A'}
    assert module.get_next_header({'User-Agent': 'UNKNOWN'}) == {'User-Agent': 'AGENT_A'}


def test_candle_retry_503_then_200_has_literal_rows_and_keys(monkeypatch, tmp_path):
    module = _data_gathering(monkeypatch)
    before = _hashes()
    previous = tmp_path / 'previous.csv'
    previous.write_bytes(b'PREVIOUS_ARTIFACT\n')
    previous_hash = hashlib.sha256(previous.read_bytes()).hexdigest()
    content = (FIXTURES / 'candles.csv').read_text(encoding='utf-8')
    responses = iter((SimpleNamespace(status_code=503, text=''),
                      SimpleNamespace(status_code=200, text=content)))
    calls, sleeps = [], []

    def request(url, headers, timeout):
        calls.append((url, dict(headers), timeout))
        return next(responses)

    monkeypatch.setattr(module, 'requests', SimpleNamespace(get=request))
    monkeypatch.setattr(module, 'time', SimpleNamespace(sleep=sleeps.append))
    monkeypatch.setattr(module.pd.DataFrame, 'to_csv', _forbidden_writer)
    monkeypatch.setattr(module.pd.DataFrame, 'to_excel', _forbidden_writer)
    result = module.moex_query('FIXTURE_SHARE', 'Акции', '2024-02-29', '2024-03-01',
                               24, max_retries=2)
    url = ('http://iss.moex.com/iss/engines/stock/markets/shares/securities/'
           'FIXTURE_SHARE/candles.csv?from=2024-02-29&till=2024-03-01&interval=24')
    assert calls == [(url, {'User-Agent': 'AGENT_A'}, 30), (url, {'User-Agent': 'AGENT_B'}, 30)]
    assert sleeps == [3, 3]
    assert result.columns.tolist() == ['open', 'close', 'high', 'low', 'value', 'volume',
                                       'begin', 'end', 'ticker']
    assert result.values.tolist() == [
        [100.0, 101.0, 102.0, 99.0, 202000.0, 2000,
         '2024-02-29 00:00:00', '2024-02-29 23:59:59', 'FIXTURE_SHARE'],
        [101.0, 103.0, 104.0, 100.0, 309000.0, 3000,
         '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'FIXTURE_SHARE'],
    ]
    assert [(ticker, 24, begin) for ticker, begin in zip(result['ticker'], result['begin'])] == [
        ('FIXTURE_SHARE', 24, '2024-02-29 00:00:00'),
        ('FIXTURE_SHARE', 24, '2024-03-01 00:00:00'),
    ]
    assert result.index.tolist() == [0, 1]
    assert _hashes() == before
    assert previous.read_bytes() == b'PREVIOUS_ARTIFACT\n'
    assert hashlib.sha256(previous.read_bytes()).hexdigest() == previous_hash


@pytest.mark.parametrize('bond_payload, expected_rows, expected_errors, expected_sleeps', [
    ('valid_bond', [['FIXTURE_SHARE', '2001-01-01', '2024-02-29'],
                    ['FIXTURE_BOND', '2019-01-01', '2024-03-01']], [], [10, 10]),
    ('malformed_bond', [['FIXTURE_SHARE', '2001-01-01', '2024-02-29']], ['FIXTURE_BOND'], [10]),
])
def test_ticker_dates_valid_and_malformed_structure(monkeypatch, bond_payload,
                                                   expected_rows, expected_errors, expected_sleeps):
    import pandas as pd
    before = _hashes()
    payloads = json.loads((FIXTURES / 'ticker_dates.json').read_text(encoding='utf-8'))
    specification = importlib.util.spec_from_file_location('f3_fixture_ticker_dates', ROOT / 'tests.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    monkeypatch.setattr(module, 'header', {'User-Agent': ''})
    responses = iter((payloads['valid_share'], payloads[bond_payload]))
    calls, sleeps, config_reads = [], [], []

    def settings_open(path, mode='r', encoding=None):
        assert (path, mode, encoding) == ('settings/user_agents.json', 'r', 'utf-8')
        config_reads.append((path, mode, encoding))
        return StringIO('{"chrome": ["AGENT_A"]}')

    def request(url, headers):
        calls.append((url, dict(headers)))
        payload = next(responses)
        return SimpleNamespace(status_code=200, json=lambda: payload)

    monkeypatch.setattr(module, 'requests', SimpleNamespace(get=request))
    monkeypatch.setattr(module, 'time', SimpleNamespace(sleep=sleeps.append))
    monkeypatch.setattr(module, 'open', settings_open, raising=False)
    monkeypatch.setattr(pd.DataFrame, 'to_csv', _forbidden_writer)
    monkeypatch.setattr(pd.DataFrame, 'to_excel', _forbidden_writer)
    frame = pd.DataFrame({'SUPERTYPE': ['Акции', 'Облигации'],
                          'TRADE_CODE': ['FIXTURE_SHARE', 'FIXTURE_BOND']})
    result, errors = module.get_ticker_dates(frame)
    assert calls == [
        ('http://iss.moex.com/iss/history/engines/stock/markets/shares/securities/FIXTURE_SHARE/dates.json', {'User-Agent': 'AGENT_A'}),
        ('http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/FIXTURE_BOND/dates.json', {'User-Agent': 'AGENT_A'}),
    ]
    assert result.columns.tolist() == ['ticker', 'date_from', 'date_till']
    assert result.values.tolist() == expected_rows
    assert result['ticker'].is_unique
    assert errors == expected_errors
    assert sleeps == expected_sleeps
    assert config_reads == [('settings/user_agents.json', 'r', 'utf-8')]
    assert _hashes() == before
