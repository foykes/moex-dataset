"""Run-local dividend collection; synthetic inputs and captured writer arguments."""

import copy
import hashlib
import importlib.util
from io import StringIO
import json
from pathlib import Path

import pandas as pd
import openpyxl
import pytest


ROOT = Path(__file__).resolve().parents[3]
ISIN = 'RU0009029540'
OTHER_ISIN = 'RU000A0JR4A1'
COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
SOURCE_COLUMNS = ['registryclosedate', 'value', 'currencyid']
PAYOUTS = [['2024-07-11', 33.3, 'RUB'], ['2024-07-11', 1.5, 'USD'],
           ['2024-07-11', 33.3, 'RUB']]
EXPECTED = [[ISIN, 'SBER', *row] for row in PAYOUTS]
OLD = [[OTHER_ISIN, 'MOEX', '2020-01-01', 17.35, 'RUB']]
SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=' + ISIN + '&iss.meta=off'
DETAIL_URL = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
OTHER_SEARCH_URL = ('https://iss.moex.com/iss/securities.json?q='
                    + OTHER_ISIN + '&iss.meta=off')
OTHER_DETAIL_URL = 'https://iss.moex.com/iss/securities/MOEX/dividends.json'


def prepared(monkeypatch, tmp_path):
    # Один import на весь тест: повторный main обязан сам изолировать свой run.
    specification = importlib.util.spec_from_file_location(
        'c1_run_state_fixture', ROOT / 'dividends.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    module.current_path = str(tmp_path)
    monkeypatch.setattr(module, '_network_clock', lambda: 0.0)
    monkeypatch.setattr(module, '_network_wall_clock', lambda: 0.0)
    state = {'scope': [['SBER', ISIN]], 'requests': [], 'writes': [], 'waits': [],
             'writer_error': None, 'responses': {
                 SEARCH_URL: {'securities': {'columns': ['secid', 'isin'],
                                            'data': [['SBER', ISIN]]}},
                 DETAIL_URL: {'dividends': {'columns': SOURCE_COLUMNS,
                                            'data': copy.deepcopy(PAYOUTS)}},
                 OTHER_SEARCH_URL: {'securities': {'columns': ['secid', 'isin'],
                                                  'data': [['MOEX', OTHER_ISIN]]}},
                 OTHER_DETAIL_URL: {'dividends': {'columns': SOURCE_COLUMNS,
                                                 'data': [['2024-07-11', 17.35, 'RUB']]}}}}

    def opened(url, *, timeout=None):
        assert url in state['responses'] and 0 < timeout <= 10
        state['requests'].append(url)
        return StringIO(json.dumps(state['responses'][url], allow_nan=False))

    def read_excel(path):
        assert path == str(tmp_path) + '/datasets/ticker_lists/moex_full.xlsx'
        return pd.DataFrame(copy.deepcopy(state['scope']), columns=['TRADE_CODE', 'ISIN'])

    def writer(kind):
        def write(frame, path, **kwargs):
            state['writes'].append((kind, path, list(frame.columns),
                                    copy.deepcopy(frame.values.tolist()), kwargs))
            if state['writer_error'] is not None:
                raise state['writer_error']
        return write

    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    monkeypatch.setattr(module, '_retry_wait', state['waits'].append)
    monkeypatch.setattr(module.pd, 'read_excel', read_excel)
    monkeypatch.setattr(pd.DataFrame, 'to_excel', writer('xlsx'))
    monkeypatch.setattr(pd.DataFrame, 'to_csv', writer('csv'))
    return module, state


def expected_writes(tmp_path, rows):
    path = str(tmp_path) + '/datasets/dividends/all'
    return [('xlsx', path + '.xlsx', COLUMNS, rows, {'index': False}),
            ('csv', path + '.csv', COLUMNS, rows, {'index': False})]


def test_two_main_calls_do_not_accumulate_rows(monkeypatch, tmp_path):
    module, state = prepared(monkeypatch, tmp_path)
    assert module.main() is None
    assert module.main() is None
    # Буквальный oracle сохраняет обе одинаковые выплаты и исходный порядок.
    assert state['writes'] == expected_writes(tmp_path, EXPECTED) * 2
    assert state['requests'] == [SEARCH_URL, DETAIL_URL] * 2
    assert module.divs_all == [] and state['waits'] == []


def test_two_main_actual_serializers_preserve_csv_checksum_and_xlsx_cells(monkeypatch, tmp_path):
    # Только fresh issued tmp_path: production datasets здесь не читаются/не пишутся.
    excel_serializer = pd.DataFrame.to_excel
    csv_serializer = pd.DataFrame.to_csv
    module, state = prepared(monkeypatch, tmp_path)
    excel_spy = pd.DataFrame.to_excel
    csv_spy = pd.DataFrame.to_csv
    directory = tmp_path / 'datasets' / 'dividends'
    directory.mkdir(parents=True)

    def excel(frame, path, **kwargs):
        excel_spy(frame, path, **kwargs)
        return excel_serializer(frame, path, **kwargs)

    def csv(frame, path, **kwargs):
        csv_spy(frame, path, **kwargs)
        return csv_serializer(frame, path, **kwargs)

    def readback():
        workbook = openpyxl.load_workbook(directory / 'all.xlsx', data_only=False)
        try:
            return [[(cell.coordinate, cell.data_type, cell.value) for cell in row]
                    for row in workbook.active.iter_rows()]
        finally:
            workbook.close()

    monkeypatch.setattr(pd.DataFrame, 'to_excel', excel)
    monkeypatch.setattr(pd.DataFrame, 'to_csv', csv)
    assert module.main() is None
    first_csv = (directory / 'all.csv').read_bytes()
    first_cells = readback()
    assert module.main() is None
    second_csv = (directory / 'all.csv').read_bytes()
    assert second_csv == first_csv
    assert hashlib.sha256(second_csv).digest() == hashlib.sha256(first_csv).digest()
    assert readback() == first_cells
    assert len(first_cells) == len(EXPECTED) + 1
    assert state['writes'] == expected_writes(tmp_path, EXPECTED) * 2
    assert module.divs_all == []


def test_second_main_exports_only_its_new_source_rows(monkeypatch, tmp_path):
    module, state = prepared(monkeypatch, tmp_path)
    assert module.main() is None
    second = [['2111-01-01', 0, 'SUR'], ['2024-02-29', -1e-9, 'UNKNOWN']]
    state['responses'][DETAIL_URL]['dividends']['data'] = second
    assert module.main() is None
    expected_second = [[ISIN, 'SBER', *row] for row in second]
    assert state['writes'] == (expected_writes(tmp_path, EXPECTED)
                               + expected_writes(tmp_path, expected_second))
    assert module.divs_all == [] and state['waits'] == []


def test_standalone_loader_accumulator_is_not_main_input_or_output(monkeypatch, tmp_path):
    module, state = prepared(monkeypatch, tmp_path)
    legacy = module.divs_all
    legacy.extend(copy.deepcopy(OLD))
    assert module.main() is None
    assert module.divs_all is legacy and legacy == OLD
    assert state['writes'] == expected_writes(tmp_path, EXPECTED)
    assert module.div_loader(ISIN, 'SBER') is None
    assert module.divs_all is legacy and legacy == OLD + EXPECTED
    assert module.main() is None
    assert legacy == OLD + EXPECTED
    assert state['writes'] == expected_writes(tmp_path, EXPECTED) * 2


@pytest.mark.parametrize('failure', ['schema', 'semantic'])
def test_partial_failed_main_does_not_contaminate_next_run(monkeypatch, tmp_path, failure):
    module, state = prepared(monkeypatch, tmp_path)
    state['scope'] = [['SBER', ISIN], ['MOEX', OTHER_ISIN]]
    invalid = ['incomplete'] if failure == 'schema' else ['2024-02-30', 1.0, 'RUB']
    state['responses'][OTHER_DETAIL_URL]['dividends']['data'].append(invalid)
    prefix = ('DIVIDEND_SOURCE_SCHEMA_INVALID' if failure == 'schema'
              else 'DIVIDEND_SOURCE_SEMANTIC_INVALID')
    with pytest.raises(ValueError, match='^' + prefix + ':'):
        module.main()
    assert state['writes'] == []
    assert module.divs_all == []
    assert state['requests'] == [SEARCH_URL, DETAIL_URL, OTHER_SEARCH_URL, OTHER_DETAIL_URL]
    state['scope'] = [['SBER', ISIN]]
    assert module.main() is None
    assert state['writes'] == expected_writes(tmp_path, EXPECTED)
    assert module.divs_all == [] and state['waits'] == []


def test_empty_second_main_cannot_republish_the_first_run(monkeypatch, tmp_path):
    module, state = prepared(monkeypatch, tmp_path)
    assert module.main() is None
    state['responses'][DETAIL_URL]['dividends']['data'] = []
    with pytest.raises(ValueError, match='^DIVIDEND_EMPTY_RELEASE_BLOCKED:'):
        module.main()
    assert state['writes'] == expected_writes(tmp_path, EXPECTED)
    assert state['requests'] == [SEARCH_URL, DETAIL_URL] * 2
    assert module.divs_all == [] and state['waits'] == []


def test_writer_failure_preserves_primary_and_does_not_seed_next_main(monkeypatch, tmp_path):
    module, state = prepared(monkeypatch, tmp_path)
    primary = OSError('synthetic writer failure')
    state['writer_error'] = primary
    with pytest.raises(OSError) as caught:
        module.main()
    assert caught.value is primary
    assert state['writes'] == expected_writes(tmp_path, EXPECTED)[:1]
    assert module.divs_all == []
    state['writer_error'] = None
    assert module.main() is None
    assert state['writes'] == (expected_writes(tmp_path, EXPECTED)[:1]
                               + expected_writes(tmp_path, EXPECTED))
    assert state['waits'] == [] and module.divs_all == []
