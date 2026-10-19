"""Literal response oracles; no live source, publication or C1-02 claims."""

import copy
import hashlib
import importlib.util
from io import StringIO
import json
from pathlib import Path
import sys
import types
import urllib.error

import pandas as pd
import pytest

import run_logging


ROOT = Path(__file__).resolve().parents[3]
BASE = '2120fb53e1acd0bf0e30620bda2a3faca7a2bdeb'
ISIN = 'RU0009029540'
MOEX_ISIN = 'RU000A0JR4A1'
SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=RU0009029540&iss.meta=off'
DETAIL_URL = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
MOEX_SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=RU000A0JR4A1&iss.meta=off'
MOEX_DETAIL_URL = 'https://iss.moex.com/iss/securities/MOEX/dividends.json'
SEARCH = {'securities': {'columns': ['secid', 'isin'], 'data': [['SBER', ISIN]]}}
MOEX_SEARCH = {'securities': {'columns': ['secid', 'isin'],
                             'data': [['MOEX', MOEX_ISIN]]}}
COLUMNS = ['secid', 'isin', 'registryclosedate', 'value', 'currencyid']
PAYOUTS = {'dividends': {'columns': COLUMNS, 'data': [
    ['SBER', ISIN, '2024-07-11', 33.3, 'RUB'],
    ['SBER', ISIN, '2024-07-11', 1.5, 'USD'],
    ['SBER', ISIN, '2024-07-11', 33.3, 'RUB'],
]}}
EMPTY = {'dividends': {'columns': COLUMNS, 'data': []}}
EXPECTED = [[ISIN, 'SBER', '2024-07-11', 33.3, 'RUB'],
            [ISIN, 'SBER', '2024-07-11', 1.5, 'USD'],
            [ISIN, 'SBER', '2024-07-11', 33.3, 'RUB']]
OLD = [['RU000A0JR4A1', 'MOEX', '2020-01-01', 17.35, 'RUB']]
OUTPUT_COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
CANARY = 'C2_SYNTHETIC_SECRET_839'


def load_dividends():
    # Свежий import изолирует cases, но не исправляет production repeat-main.
    spec = importlib.util.spec_from_file_location('c2_dividends_fixture', ROOT / 'dividends.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source(monkeypatch, module, detail=PAYOUTS, *, responses=None, raw=None):
    responses = responses if responses is not None else {SEARCH_URL: SEARCH, DETAIL_URL: detail}
    calls = []
    def opened(url, *, timeout=None):
        calls.append(url)
        assert url in responses, 'Unexpected fixture request: ' + url
        payload = responses[url]
        if isinstance(payload, BaseException):
            raise payload
        if raw is not None and url == DETAIL_URL:
            return StringIO(raw)
        return StringIO(json.dumps(payload, ensure_ascii=False))
    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    return calls


def exporters(monkeypatch, module, rows):
    reads, writes = [], []
    def read_excel(path):
        reads.append(path)
        return pd.DataFrame(rows, columns=['TRADE_CODE', 'ISIN'])
    def to_excel(frame, path, **kwargs):
        writes.append(('xlsx', path, list(frame.columns), frame.values.tolist(), kwargs))
    def to_csv(frame, path, **kwargs):
        writes.append(('csv', path, list(frame.columns), frame.values.tolist(), kwargs))
    monkeypatch.setattr(module.pd, 'read_excel', read_excel)
    monkeypatch.setattr(pd.DataFrame, 'to_excel', to_excel)
    monkeypatch.setattr(pd.DataFrame, 'to_csv', to_csv)
    return reads, writes


def previous_files(module, tmp_path):
    module.current_path = str(tmp_path)
    directory = tmp_path / 'datasets' / 'dividends'
    directory.mkdir(parents=True)
    values = {'all.csv': b'OLD_CSV_2020\r\n', 'all.xlsx': b'OLD_XLSX_2020\x00\xff'}
    for name, value in values.items():
        (directory / name).write_bytes(value)
    return directory, values


def assert_previous(directory, expected):
    for name, value in expected.items():
        actual = (directory / name).read_bytes()
        assert actual == value
        assert hashlib.sha256(actual).digest() == hashlib.sha256(value).digest()


@pytest.fixture
def context(tmp_path):
    session = run_logging.configure_logging(tmp_path / 'logs', run_id='c2-disposable',
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    yield session
    run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def records(context):
    assert run_logging.flush_logging(context)['confirmed']
    values = [json.loads(line) for line in
              (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    for event in values:
        assert event['fields'] == {}
        assert 'endpoint' not in event
    return values


def assert_failed(context):
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']
    return report


@pytest.mark.parametrize('reordered', [False, True])
@pytest.mark.parametrize('with_logging', [False, True])
def test_named_response_preserves_literal_rows(monkeypatch, context, reordered, with_logging):
    module = load_dividends()
    detail = PAYOUTS
    if reordered:
        detail = {'dividends': {
            'columns': ['currencyid', 'value', 'note', 'registryclosedate', 'isin', 'secid'],
            'data': [['RUB', 33.3, 'fixture', '2024-07-11', ISIN, 'SBER'],
                     ['USD', 1.5, 'fixture', '2024-07-11', ISIN, 'SBER'],
                     ['RUB', 33.3, 'fixture', '2024-07-11', ISIN, 'SBER']],
        }}
    calls = source(monkeypatch, module, detail)
    kwargs = {'logging_context': context} if with_logging else {}
    assert module.div_loader(ISIN, 'SBER', **kwargs) is None
    assert calls == [SEARCH_URL, DETAIL_URL]
    assert module.divs_all == EXPECTED
    if with_logging:
        events = records(context)
        assert [event['event'] for event in events] == [
            'dividend_identity_search_started', 'dividend_identity_selected',
            'dividend_request_started', 'dividend_loader_returned']
        returned = events[-1]
        assert returned['outcome'] == 'RETURNED'
        assert returned['counts'] == {'rows_received': 3, 'rows_accepted': 3,
            'rows_quarantined': None, 'pages': 1, 'instruments': 1,
            'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
        assert returned['duration_ms'] >= 0


@pytest.mark.parametrize('value', [0, 0.000000001, -0.000000001])
def test_decoded_values_are_not_coerced(monkeypatch, value):
    module = load_dividends()
    detail = {'dividends': {'columns': COLUMNS,
        'data': [['SBER', ISIN, '2111-01-01', value, 'RUB']]}}
    source(monkeypatch, module, detail)
    module.div_loader(ISIN, 'SBER')
    assert module.divs_all == [[ISIN, 'SBER', '2111-01-01', value, 'RUB']]
    assert type(module.divs_all[0][3]) is type(value)
    # Это сохранение representation, не приёмка quality/2111/currency.


@pytest.mark.parametrize('value,currency,rule,field', [
    ('0.000000001', 'RUB', 'VALUE_TYPE', 'value'),
    (None, 'RUB', 'VALUE_TYPE', 'value'),
    (1.0, None, 'CURRENCY_TYPE', 'currency'),
])
def test_new_local_profile_explicitly_refuses_old_untyped_inputs(
        monkeypatch, value, currency, rule, field):
    module = load_dividends()
    detail = {'dividends': {'columns': COLUMNS,
        'data': [['SBER', ISIN, '2111-01-01', value, currency]]}}
    calls = source(monkeypatch, module, detail)
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == (
        'DIVIDEND_SOURCE_SEMANTIC_INVALID: rule={}; field={}; row=0'.format(rule, field))
    assert module.divs_all == [] and calls == [SEARCH_URL, DETAIL_URL]


INVALID = [
    pytest.param(None, id='top-null'),
    pytest.param([], id='top-list'),
    pytest.param('html-like', id='top-string'),
    pytest.param({'dividends': None}, id='block-null'),
    pytest.param({'dividends': []}, id='block-list'),
    pytest.param({'dividends': 'table'}, id='block-string'),
    pytest.param({'dividends': {'data': []}}, id='columns-missing'),
    pytest.param({'dividends': {'columns': None, 'data': []}}, id='columns-null'),
    pytest.param({'dividends': {'columns': 'registryclosedate,value,currencyid', 'data': []}},
                 id='columns-string'),
    pytest.param({'dividends': {'columns': {}, 'data': []}}, id='columns-object'),
    pytest.param({'dividends': {'columns': COLUMNS}}, id='data-missing'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': None}}, id='data-null'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': {}}}, id='data-object'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': ''}}, id='data-string'),
    pytest.param({'dividends': {'columns': COLUMNS + ['value'], 'data': []}}, id='duplicate-required'),
    pytest.param({'dividends': {'columns': COLUMNS + ['extra', 'extra'], 'data': []}},
                 id='duplicate-extra'),
    pytest.param({'dividends': {'columns': COLUMNS + [''], 'data': []}}, id='blank-column'),
    pytest.param({'dividends': {'columns': COLUMNS + ['  '], 'data': []}}, id='whitespace-column'),
    pytest.param({'dividends': {'columns': COLUMNS + [None], 'data': []}}, id='null-column'),
    pytest.param({'dividends': {'columns': COLUMNS + [42], 'data': []}}, id='numeric-column'),
    pytest.param({'dividends': {'columns': ['value', 'currencyid'], 'data': []}}, id='date-missing'),
    pytest.param({'dividends': {'columns': ['registryclosedate', 'currencyid'], 'data': []}},
                 id='value-missing'),
    pytest.param({'dividends': {'columns': ['registryclosedate', 'value'], 'data': []}},
                 id='currency-missing'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': [None]}}, id='row-null'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': [{}]}}, id='row-object'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': ['12345']}}, id='row-string'),
    pytest.param({'dividends': {'columns': COLUMNS, 'data': [['SBER', ISIN]]}}, id='row-short'),
    pytest.param({'dividends': {'columns': COLUMNS,
        'data': [['SBER', ISIN, '2024-07-11', 33.3, 'RUB', 'extra']]}}, id='row-long'),
]


@pytest.mark.parametrize('detail', INVALID)
def test_invalid_response_has_no_contribution(monkeypatch, detail):
    module = load_dividends()
    module.divs_all.extend(copy.deepcopy(OLD))
    before = copy.deepcopy(module.divs_all)
    calls = source(monkeypatch, module, detail)
    with pytest.raises(ValueError, match='^DIVIDEND_SOURCE_SCHEMA_INVALID:'):
        module.div_loader(ISIN, 'SBER')
    assert calls == [SEARCH_URL, DETAIL_URL]
    assert module.divs_all == before


def test_late_bad_row_is_rejected_before_any_response_mutation(monkeypatch, context):
    module = load_dividends()
    module.divs_all.extend(copy.deepcopy(OLD))
    detail = {'dividends': {'columns': COLUMNS,
        'data': [['SBER', ISIN, '2024-07-11', 33.3, 'RUB'], ['SBER', ISIN]]}}
    source(monkeypatch, module, detail)
    with pytest.raises(ValueError, match='^DIVIDEND_SOURCE_SCHEMA_INVALID:'):
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert module.divs_all == OLD
    events = records(context)
    assert not any(event['event'] == 'dividend_loader_returned' for event in events)
    error = events[-1]['error']
    assert error['code'] == 'DIVIDEND_SOURCE_SCHEMA_INVALID' and error['category'] == 'SOURCE'
    assert error['final'] is True and error['retryable'] is False
    assert error['endpoint'] == DETAIL_URL
    assert_failed(context)


def test_explicit_empty_response_is_separate_from_failure(monkeypatch, context):
    module = load_dividends()
    module.divs_all.extend(copy.deepcopy(OLD))
    calls = source(monkeypatch, module, EMPTY)
    assert module.div_loader(ISIN, 'SBER', logging_context=context) is None
    assert module.divs_all == OLD and calls == [SEARCH_URL, DETAIL_URL]
    returned = records(context)[-1]
    assert returned['event'] == 'dividend_loader_returned' and returned['outcome'] == 'VALID_EMPTY'
    assert returned['counts'] == {'rows_received': 0, 'rows_accepted': 0,
        'rows_quarantined': None, 'pages': 1, 'instruments': 1,
        'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
    assert returned['error'] is None


@pytest.mark.parametrize('seeded', [False, True])
@pytest.mark.parametrize('with_logging', [False, True])
def test_all_empty_main_blocks_before_old_rows_can_be_exported(
        monkeypatch, context, tmp_path, capsys, seeded, with_logging):
    module = load_dividends()
    if seeded:
        module.divs_all.extend(copy.deepcopy(OLD))
    before = copy.deepcopy(module.divs_all)
    directory, previous = previous_files(module, tmp_path / 'previous')
    calls = source(monkeypatch, module, EMPTY)
    reads, writes = exporters(monkeypatch, module, [['SBER', ISIN]])
    kwargs = {'logging_context': context} if with_logging else {}
    with pytest.raises(ValueError, match='^DIVIDEND_EMPTY_RELEASE_BLOCKED:'):
        module.main(**kwargs)
    assert calls == [SEARCH_URL, DETAIL_URL] and len(reads) == 1
    assert writes == [] and module.divs_all == before
    assert_previous(directory, previous)
    assert 'Выгружено записей о дивидендах:' not in capsys.readouterr().out
    if with_logging:
        events = records(context)
        assert not any(event['event'] in ('dividend_export_input', 'dividend_collection_returned')
                       for event in events)
        failure = events[-1]
        assert failure['event'] == 'dividend_collection_failed'
        assert failure['level'] == 'ERROR' and failure['outcome'] == 'BLOCKED'
        error = failure['error']
        assert error['code'] == 'DIVIDEND_EMPTY_RELEASE_BLOCKED' and error['category'] == 'QUALITY'
        assert error['final'] is True and error['retryable'] is False
        assert_failed(context)


@pytest.mark.parametrize('seeded', [False, True])
def test_empty_scope_is_unverified_and_never_requests_or_writes(monkeypatch, context, seeded):
    module = load_dividends()
    if seeded:
        module.divs_all.extend(copy.deepcopy(OLD))
    before = copy.deepcopy(module.divs_all)
    calls = source(monkeypatch, module)
    _, writes = exporters(monkeypatch, module, [])
    with pytest.raises(ValueError, match='^DIVIDEND_SCOPE_EMPTY_UNVERIFIED:'):
        module.main(logging_context=context)
    assert calls == [] and writes == [] and module.divs_all == before
    failure = records(context)[-1]
    assert failure['event'] == 'dividend_collection_failed' and failure['outcome'] == 'BLOCKED'
    assert failure['error']['category'] == 'QUALITY'
    assert failure['error']['code'] == 'DIVIDEND_SCOPE_EMPTY_UNVERIFIED'
    assert failure['error']['final'] is True and failure['error']['retryable'] is False
    assert_failed(context)


@pytest.mark.parametrize('empty_first', [False, True])
@pytest.mark.parametrize('seeded', [False, True])
def test_mixed_scope_preserves_literal_export_and_c1_residual(
        monkeypatch, context, tmp_path, empty_first, seeded):
    module = load_dividends()
    module.current_path = str(tmp_path)
    if seeded:
        module.divs_all.extend(copy.deepcopy(OLD))
    rows = [['MOEX', MOEX_ISIN], ['SBER', ISIN]] if empty_first else [
        ['SBER', ISIN], ['MOEX', MOEX_ISIN]]
    responses = {SEARCH_URL: SEARCH, DETAIL_URL: PAYOUTS,
                 MOEX_SEARCH_URL: MOEX_SEARCH, MOEX_DETAIL_URL: EMPTY}
    calls = source(monkeypatch, module, responses=responses)
    reads, writes = exporters(monkeypatch, module, rows)
    assert module.main(logging_context=context) is None
    assert calls == ([MOEX_SEARCH_URL, MOEX_DETAIL_URL, SEARCH_URL, DETAIL_URL] if empty_first else
                     [SEARCH_URL, DETAIL_URL, MOEX_SEARCH_URL, MOEX_DETAIL_URL])
    expected = OLD + EXPECTED if seeded else EXPECTED
    # Old+new при nonempty остаётся явным C1-02 residual, не заявкой на исправление.
    assert module.divs_all == expected
    path = str(tmp_path) + '/datasets/dividends/all'
    assert reads == [str(tmp_path) + '/datasets/ticker_lists/moex_full.xlsx']
    assert writes == [('xlsx', path + '.xlsx', OUTPUT_COLUMNS, expected, {'index': False}),
                      ('csv', path + '.csv', OUTPUT_COLUMNS, expected, {'index': False})]
    events = records(context)
    returned = [event for event in events if event['event'] == 'dividend_loader_returned']
    assert [event['outcome'] for event in returned] == (
        ['VALID_EMPTY', 'RETURNED'] if empty_first else ['RETURNED', 'VALID_EMPTY'])
    assert [event['counts']['rows_accepted'] for event in returned] == (
        [0, 3] if empty_first else [3, 0])
    export = next(event for event in events if event['event'] == 'dividend_export_input')
    assert export['counts']['rows_accepted'] == len(expected)
    assert export['counts']['instruments'] == 2
    assert events[-1]['event'] == 'dividend_collection_returned'
    assert events[-1]['outcome'] == 'RETURNED'


def test_success_then_invalid_response_stops_writers_without_whole_run_rollback(monkeypatch, context):
    module = load_dividends()
    responses = {SEARCH_URL: SEARCH, DETAIL_URL: PAYOUTS, MOEX_SEARCH_URL: MOEX_SEARCH,
        MOEX_DETAIL_URL: {'dividends': {'columns': COLUMNS,
            'data': [['MOEX', MOEX_ISIN, '2024-07-11', 17.35, 'RUB'], ['MOEX']]}}}
    calls = source(monkeypatch, module, responses=responses)
    _, writes = exporters(monkeypatch, module, [['SBER', ISIN], ['MOEX', MOEX_ISIN]])
    with pytest.raises(ValueError, match='^DIVIDEND_SOURCE_SCHEMA_INVALID:'):
        module.main(logging_context=context)
    assert calls == [SEARCH_URL, DETAIL_URL, MOEX_SEARCH_URL, MOEX_DETAIL_URL]
    assert writes == [] and module.divs_all == EXPECTED
    events = records(context)
    assert not any(event['event'] in ('dividend_export_input', 'dividend_collection_returned')
                   for event in events)
    assert events[-1]['event'] == 'dividend_loader_failed'
    assert_failed(context)


@pytest.mark.parametrize('kind', ['missing', 'html', 'json', 'http403', 'http500'])
def test_existing_source_failures_keep_primary_and_previous(monkeypatch, context, tmp_path, kind):
    module = load_dividends()
    module.divs_all.extend(copy.deepcopy(OLD))
    directory, previous = previous_files(module, tmp_path / 'previous')
    primary = None
    detail, raw = {}, None
    error_type = KeyError
    if kind in ('html', 'json'):
        raw = '<html>denied</html>' if kind == 'html' else '{'
        error_type = json.JSONDecodeError
    elif kind.startswith('http'):
        primary = urllib.error.HTTPError(DETAIL_URL, int(kind[4:]), CANARY, {}, None)
        detail = primary
        error_type = urllib.error.HTTPError
    calls = source(monkeypatch, module, detail, raw=raw)
    waits = []
    monkeypatch.setattr(module, '_retry_wait', waits.append, raising=False)
    _, writes = exporters(monkeypatch, module, [['SBER', ISIN]])
    with pytest.raises(error_type) as caught:
        module.main(logging_context=context)
    if kind == 'missing':
        assert caught.value.args == ('dividends',)
    if primary is not None:
        assert caught.value is primary
    assert calls == [SEARCH_URL] + [DETAIL_URL] * (3 if kind == 'http500' else 1)
    assert waits == ([1, 2] if kind == 'http500' else [])
    assert writes == [] and module.divs_all == OLD
    assert_previous(directory, previous)
    events = records(context)
    failure = events[-1]
    assert failure['event'] == 'dividend_loader_failed' and failure['outcome'] == 'FAILED'
    assert failure['error']['category'] == ('ACCESS' if kind == 'http403' else 'SOURCE')
    assert failure['error']['code'] == (
        'DIVIDEND_SOURCE_SCHEMA_INVALID' if kind == 'missing' else 'DIVIDEND_SOURCE_REQUEST_FAILED')
    assert not any(event['event'] in ('dividend_export_input', 'dividend_collection_returned')
                   for event in events)
    assert_failed(context)


@pytest.mark.parametrize('fault', ['rejected_info', 'unhealthy', 'unconfirmed_flush'])
def test_response_logging_barrier_precedes_accumulator_mutation(monkeypatch, context, fault):
    module = load_dividends()
    module.divs_all.extend(copy.deepcopy(OLD))
    calls = source(monkeypatch, module)
    original_emit = run_logging.emit_event
    original_health = run_logging.check_logging_health
    original_flush = run_logging.flush_logging
    returned = []
    def emit(session, level, event, message, fields):
        if event == 'dividend_loader_returned':
            returned.append(event)
            if fault == 'rejected_info':
                return {'accepted': False, 'confirmed': False}
        return original_emit(session, level, event, message, fields)
    def health(session):
        if returned and fault == 'unhealthy':
            return {'healthy': False}
        return original_health(session)
    def flush(session):
        result = original_flush(session)
        return {**result, 'confirmed': False} if fault == 'unconfirmed_flush' else result
    with monkeypatch.context() as faults:
        faults.setattr(run_logging, 'emit_event', emit)
        faults.setattr(run_logging, 'check_logging_health', health)
        faults.setattr(run_logging, 'flush_logging', flush)
        with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$'):
            module.div_loader(ISIN, 'SBER', logging_context=context)
    assert calls == [SEARCH_URL, DETAIL_URL] and returned == ['dividend_loader_returned']
    assert module.divs_all == OLD
    assert records(context)[-1]['error']['code'] == 'LOGGING_INCOMPLETE'
    assert_failed(context)


@pytest.mark.parametrize('fault', ['make_error', 'emit', 'unconfirmed', 'stderr'])
def test_collection_error_retains_primary_when_diagnostics_fail(monkeypatch, context, fault):
    module = load_dividends()
    calls = source(monkeypatch, module)
    _, writes = exporters(monkeypatch, module, [])
    captured = []
    original_failure = module._dividend_failure
    def failure(session, error, *args, **kwargs):
        captured.append(error)
        return original_failure(session, error, *args, **kwargs)
    monkeypatch.setattr(module, '_dividend_failure', failure)
    original_emit = run_logging.emit_event
    def failed(*args, **kwargs):
        raise RuntimeError('secondary ' + CANARY)
    def emit(session, level, event, message, fields):
        if event == 'dividend_collection_failed':
            if fault == 'unconfirmed':
                return {'accepted': True, 'confirmed': False}
            raise RuntimeError('secondary ' + CANARY)
        return original_emit(session, level, event, message, fields)
    with monkeypatch.context() as faults:
        if fault == 'make_error':
            faults.setattr(run_logging, 'make_error', failed)
        else:
            faults.setattr(run_logging, 'emit_event', emit)
        if fault == 'stderr':
            faults.setattr(module, 'print', failed, raising=False)
        with pytest.raises(ValueError, match='^DIVIDEND_SCOPE_EMPTY_UNVERIFIED:') as caught:
            module.main(logging_context=context)
    assert captured == [caught.value] and captured[0] is caught.value
    assert calls == [] and writes == [] and module.divs_all == []
    assert_failed(context)


def test_domain_error_redacts_synthetic_canary(monkeypatch, context, capsys):
    module = load_dividends()
    endpoint = 'https://example.invalid/dividends?token=' + CANARY
    primary = urllib.error.HTTPError(endpoint, 403, 'denied ' + CANARY, {}, None)
    source(monkeypatch, module, primary)
    with pytest.raises(urllib.error.HTTPError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary
    events = records(context)
    report = assert_failed(context)
    output = capsys.readouterr()
    stored = ''.join(path.read_text(encoding='utf-8') for path in
                     context['_run_root'].rglob('*.json*'))
    assert CANARY not in output.out + output.err + stored
    assert CANARY not in json.dumps(events) + json.dumps(report)
    assert events[-1]['error']['endpoint'] == DETAIL_URL


def test_pipeline_stage_failure_never_reaches_later_stages(monkeypatch, context):
    import ast
    from contextlib import contextmanager
    source_text = (ROOT / 'main.py').read_bytes().replace(b'\r\n', b'\n')
    tree = ast.parse(source_text.decode('utf-8'))
    functions = {name: [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
        for name in ('main', '_run_cli')}
    markers = [node for node in tree.body if (
        isinstance(node, ast.Assign)
        and any(isinstance(part, ast.Name) and part.id == 'D2_SOURCE_PROFILE'
                for target in node.targets for part in ast.walk(target))) or (
        isinstance(node, (ast.AnnAssign, ast.AugAssign))
        and any(isinstance(part, ast.Name) and part.id == 'D2_SOURCE_PROFILE'
                for part in ast.walk(node.target)))]
    modern = (len(markers) == 1 and isinstance(markers[0], ast.Assign)
        and len(markers[0].targets) == 1
        and isinstance(markers[0].targets[0], ast.Name)
        and isinstance(markers[0].value, ast.Constant)
        and markers[0].value.value == 'linux-stage-b-v1'
        and all(len(value) == 1 and isinstance(value[0], ast.FunctionDef)
                for value in functions.values()))
    if modern:
        main_args = functions['main'][0].args
        cli_args = functions['_run_cli'][0].args
        modern = (not functions['main'][0].decorator_list
            and not functions['_run_cli'][0].decorator_list
            and not main_args.posonlyargs and not main_args.args
            and main_args.vararg is None and main_args.kwarg is None
            and [arg.arg for arg in main_args.kwonlyargs] == ['logging_context', 'result_sink']
            and all(isinstance(value, ast.Constant) and value.value is None
                    for value in main_args.kw_defaults)
            and not cli_args.posonlyargs and not cli_args.args
            and cli_args.vararg is None and cli_args.kwarg is None
            and not cli_args.kwonlyargs)
    legacy = hashlib.sha256(source_text).hexdigest() == (
        'c035f4439c060391e38c270f6f7adf257b356e6aae2104b135cf53e770509c3a')
    assert legacy or modern, 'D2_FIXTURE_SOURCE_PROFILE_UNSUPPORTED'
    runtime_events = []
    class Runtime:
        def bind_context(self, value):
            assert runtime_events in (['entered'], ['entered', 'bound']) and value is context
            if runtime_events == ['entered']:
                runtime_events.append('bound')
    @contextmanager
    def runtime_scope(*, logging_context=None):
        assert logging_context is None or logging_context is context
        assert runtime_events == []
        runtime_events.append('entered')
        try:
            yield Runtime()
        finally:
            assert runtime_events == ['entered', 'bound']
            runtime_events.append('closed')
    module = load_dividends()
    source(monkeypatch, module, EMPTY)
    _, writes = exporters(monkeypatch, module, [['SBER', ISIN]])
    spec = importlib.util.spec_from_file_location('c2_main_fixture', ROOT / 'main.py')
    pipeline = importlib.util.module_from_spec(spec)
    reached = []
    def initial(root):
        if modern:
            assert runtime_events == ['entered', 'bound'], 'Stage before fixture runtime admission'
        reached.append('data_gathering')
    def forbidden(*args):
        pytest.fail('Later stage was reached after a required dividend failure')
    monkeypatch.setitem(sys.modules, 'data_gathering', types.SimpleNamespace(main=initial))
    monkeypatch.setitem(sys.modules, 'dividends', module)
    for name in ('tech', 'dohodru_data', 'upload'):
        stub = types.SimpleNamespace(main=forbidden)
        if name == 'upload':
            stub.runtime_scope = runtime_scope
            stub._d2_completion_allowed = lambda result_sink: False
        monkeypatch.setitem(sys.modules, name, stub)
    exec(compile(tree, str(ROOT / 'main.py'), 'exec'), pipeline.__dict__)
    with pytest.raises(ValueError, match='^DIVIDEND_EMPTY_RELEASE_BLOCKED:'):
        pipeline.main(logging_context=context)
    assert reached == ['data_gathering'] and writes == []
    events = records(context)
    assert any(event['event'] == 'stage_failed' and event['stage'] == 'dividends'
               and event['outcome'] == 'FAILED' for event in events)
    assert not any(event['event'] == 'stage_returned' and event['stage'] == 'dividends'
                   for event in events)
    assert runtime_events == (['entered', 'bound', 'closed'] if modern else [])
    assert_failed(context)
