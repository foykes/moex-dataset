"""Literal offline oracles through the original public dividend entrypoints."""

import importlib.util
from io import StringIO
import json
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SEARCH = 'https://iss.moex.com/iss/securities.json?q=RU0009029540&iss.meta=off'
SBER_DIVIDENDS = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
DIVIDENDS = {'dividends': {
    'columns': ['secid', 'isin', 'registryclosedate', 'value', 'currencyid'],
    'data': [['SBER', 'RU0009029540', '2024-07-11', 33.3, 'RUB'],
             ['SBER', 'RU0009029540', '2024-07-11', 1.5, 'USD'],
             ['SBER', 'RU0009029540', '2024-07-11', 7.0, 'RUB']],
}}
EXPECTED = [['RU0009029540', 'SBER', '2024-07-11', 33.3, 'RUB'],
            ['RU0009029540', 'SBER', '2024-07-11', 1.5, 'USD'],
            ['RU0009029540', 'SBER', '2024-07-11', 7.0, 'RUB']]


def load_dividends():
    # Fresh imports isolate test cases; this is not a production repeat-main fix.
    specification = importlib.util.spec_from_file_location('c1_dividends_fixture', ROOT / 'dividends.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def source(monkeypatch, module, search, dividends=DIVIDENDS):
    calls = []
    def open_fixture(url):
        calls.append(url)
        payload = search if '/securities.json?' in url else dividends
        # Record even a wrong SECID URL: RED must expose the old mapping itself.
        return StringIO(json.dumps(payload, ensure_ascii=False))
    monkeypatch.setattr(module.urllib.request, 'urlopen', open_fixture)
    return calls


@pytest.mark.parametrize('ticker,isin,shortname', [
    ('SBER', 'RU0009029540', 'Сбербанк'),
    ('MOEX', 'RU000A0JR4A1', 'МосБиржа'),
])
@pytest.mark.parametrize('reordered', [False, True])
def test_public_loader_uses_named_exact_identity(monkeypatch, ticker, isin, shortname, reordered):
    module = load_dividends()
    columns = ['secid', 'shortname', 'isin']
    row = [ticker, shortname, isin]
    if reordered:
        columns = ['isin', 'secid', 'shortname']
        row = [isin, ticker, shortname]
    search = {'securities': {'columns': columns, 'data': [row]}}
    calls = source(monkeypatch, module, search)
    # Deliberately use the old two-argument API for both RED and GREEN.
    assert module.div_loader(isin, ticker) is None
    assert calls == [
        'https://iss.moex.com/iss/securities.json?q=' + isin + '&iss.meta=off',
        'https://iss.moex.com/iss/securities/' + ticker + '/dividends.json',
    ]
    assert module.divs_all == [[isin, ticker, '2024-07-11', 33.3, 'RUB'],
                              [isin, ticker, '2024-07-11', 1.5, 'USD'],
                              [isin, ticker, '2024-07-11', 7.0, 'RUB']]


def test_fuzzy_result_is_not_a_candidate(monkeypatch):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
        'data': [['../../wrong', 'Неточное совпадение', 'RU000A0JR4A1'],
                 ['SBER', 'Сбербанк', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH, SBER_DIVIDENDS]
    assert module.divs_all == EXPECTED


@pytest.mark.parametrize('returned_isin', [None, 42, [], {}, 'ru0009029540',
                                         ' RU0009029540', 'RU000A0JR4A1'])
def test_nonexact_source_identity_is_excluded(monkeypatch, returned_isin):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'isin'],
        'data': [['../../wrong', returned_isin], ['SBER', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH, SBER_DIVIDENDS]
    assert module.divs_all == EXPECTED


@pytest.mark.parametrize('secid,segment', [('TEST+SEC', 'TEST%2BSEC'),
                                         ('TEST%SEC', 'TEST%25SEC')])
def test_opaque_safe_secid_is_encoded_once(monkeypatch, secid, segment):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'isin'],
                            'data': [[secid, 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH, 'https://iss.moex.com/iss/securities/' + segment + '/dividends.json']
    assert module.divs_all == EXPECTED


def test_identical_matching_secids_make_one_loader_request(monkeypatch):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
        'data': [['SBER', 'Сбербанк', 'RU0009029540'],
                 ['SBER', 'Другое отображаемое имя', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH, SBER_DIVIDENDS]
    assert module.divs_all == EXPECTED


@pytest.mark.parametrize('isin', [None, float('nan'), pd.NA, '', 42,
                                  'RU000902954', 'ru0009029540', ' RU0009029540'])
def test_invalid_isin_refuses_before_source_request(monkeypatch, isin):
    module = load_dividends()
    calls = source(monkeypatch, module, {'securities': {'columns': [], 'data': []}})
    with pytest.raises(ValueError, match='^DIVIDEND_ISIN_INVALID:'):
        module.div_loader(isin, 'SBER')
    assert calls == []
    assert module.divs_all == []


@pytest.mark.parametrize('rows', [[], [['MOEX', 'МосБиржа', 'RU000A0JR4A1']],
                                  [['SBER', 'Сбербанк', None]]])
def test_no_exact_identity_refuses_before_dividend_request(monkeypatch, rows):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'], 'data': rows}}
    calls = source(monkeypatch, module, search)
    with pytest.raises(ValueError, match='^DIVIDEND_IDENTITY_NOT_FOUND:'):
        module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH]
    assert module.divs_all == []


@pytest.mark.parametrize('search', [
    {}, {'securities': None}, {'securities': {'data': []}},
    {'securities': {'columns': ['secid', 'isin'], 'data': {}}},
    {'securities': {'columns': ['secid', 'isin', 42], 'data': []}},
    {'securities': {'columns': ['secid', 'shortname'], 'data': []}},
    {'securities': {'columns': ['secid', 'isin', 'secid'], 'data': []}},
    {'securities': {'columns': ['secid', 'isin', 'isin'], 'data': []}},
    {'securities': {'columns': ['secid', 'isin'], 'data': [['SBER']]}},
    {'securities': {'columns': ['secid', 'isin'], 'data': [['SBER', 'RU0009029540', 'extra']]}},
    {'securities': {'columns': ['secid', 'isin'], 'data': ['SBER']}},
    {'securities': {'columns': ['secid', 'isin'],
                    'data': [['SBER', 'RU0009029540'], ['unrelated']]}},
])
def test_malformed_search_has_distinct_schema_error(monkeypatch, search):
    module = load_dividends()
    calls = source(monkeypatch, module, search)
    with pytest.raises(ValueError, match='^DIVIDEND_SEARCH_SCHEMA_INVALID:'):
        module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH]
    assert module.divs_all == []


@pytest.mark.parametrize('secid', [None, float('nan'), '', 42, 'SBER/../MOEX',
    'SBER?bad', 'SBER#bad', 'SBER\\bad', ' SBER', 'SBER ', 'SBER\t', '.', '..',
    'SBER\x00', 'SBER\x7f', 'SBER\x85', '\ud800'])
def test_invalid_matching_secid_refuses_before_dividend_request(monkeypatch, secid):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
                            'data': [[secid, 'Сбербанк', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    with pytest.raises(ValueError, match='^DIVIDEND_SECID_INVALID:'):
        module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH]
    assert module.divs_all == []


def test_distinct_exact_secids_are_ambiguous(monkeypatch):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
        'data': [['SBER', 'Сбербанк', 'RU0009029540'],
                 ['SBERP', 'Иной код', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    with pytest.raises(ValueError, match='^DIVIDEND_IDENTITY_AMBIGUOUS:'):
        module.div_loader('RU0009029540', 'SBER')
    assert calls == [SEARCH]
    assert module.divs_all == []


def test_correct_mapping_does_not_mask_missing_dividends_block(monkeypatch):
    module = load_dividends()
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
                            'data': [['SBER', 'Сбербанк', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search, {'description': {'data': []}, 'boards': {'data': []}})
    with pytest.raises(KeyError) as caught:
        module.div_loader('RU0009029540', 'SBER')
    assert caught.value.args == ('dividends',)
    assert calls == [SEARCH, SBER_DIVIDENDS]
    assert module.divs_all == []


def test_main_export_input_parity_and_legacy_contract(monkeypatch, tmp_path):
    module = load_dividends()
    module.current_path = str(tmp_path)
    reads, exports = [], []
    def read_fixture(path):
        reads.append(path)
        return pd.DataFrame({'TRADE_CODE': ['SBER'], 'ISIN': ['RU0009029540']})
    monkeypatch.setattr(pd, 'read_excel', read_fixture)
    for method, extension in [('to_excel', 'xlsx'), ('to_csv', 'csv')]:
        monkeypatch.setattr(pd.DataFrame, method,
            lambda frame, path, index, ext=extension: exports.append((ext, path, index, frame.copy())))
    search = {'securities': {'columns': ['secid', 'shortname', 'isin'],
                            'data': [['SBER', 'Сбербанк', 'RU0009029540']]}}
    calls = source(monkeypatch, module, search)
    assert module.main() is None
    assert reads == [str(tmp_path) + '/datasets/ticker_lists/moex_full.xlsx']
    assert calls == [SEARCH, SBER_DIVIDENDS]
    assert [(ext, Path(path).name, index) for ext, path, index, frame in exports] == [
        ('xlsx', 'all.xlsx', False), ('csv', 'all.csv', False)]
    expected = pd.DataFrame(EXPECTED, columns=['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency'])
    for ext, path, index, frame in exports:
        pd.testing.assert_frame_equal(frame, expected)
    # Spies prove input-frame parity only; no serialized files are created.
    assert list(tmp_path.iterdir()) == []
