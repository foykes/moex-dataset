"""Synthetic response-local profile checks; no captured MOEX or publication claim."""

import copy
import hashlib
import importlib.util
from io import StringIO
import json
from pathlib import Path

import pandas as pd
import pytest

import run_logging


ROOT = Path(__file__).resolve().parents[3]
BASE = 'c98664b7da20375cf0bfed8125a94d64008d0d63'
ISIN = 'RU0009029540'
OTHER_ISIN = 'RU000A0JR4A1'
SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=' + ISIN + '&iss.meta=off'
DETAIL_URL = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
OTHER_SEARCH_URL = ('https://iss.moex.com/iss/securities.json?q=' + OTHER_ISIN
                    + '&iss.meta=off')
OTHER_DETAIL_URL = 'https://iss.moex.com/iss/securities/MOEX/dividends.json'
COLUMNS = ['secid', 'isin', 'registryclosedate', 'value', 'currencyid']
OUTPUT_COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
SEARCH = {'securities': {'columns': ['secid', 'isin'], 'data': [['SBER', ISIN]]}}
OTHER_SEARCH = {'securities': {'columns': ['secid', 'isin'],
                             'data': [['MOEX', OTHER_ISIN]]}}
CANARY = 'C2B_SYNTHETIC_PRIVATE_7281'
OLD = [[OTHER_ISIN, 'MOEX', '2020-01-01', 17.35, 'RUB']]
EVENTS = ['dividend_identity_search_started', 'dividend_identity_selected',
          'dividend_request_started', 'dividend_loader_returned']


def load_dividends(monkeypatch):
    spec = importlib.util.spec_from_file_location('c2b_dividends_fixture', ROOT / 'dividends.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_network_clock', lambda: 0.0)
    monkeypatch.setattr(module, '_network_wall_clock', lambda: 0.0)
    waits = []
    monkeypatch.setattr(module, '_retry_wait', waits.append)
    return module, waits


def detail(*rows):
    return {'dividends': {'columns': COLUMNS, 'data': list(rows)}}


def row(dt='2024-07-11', value=33.3, currency='RUB'):
    return ['SBER', ISIN, dt, value, currency]


def source(monkeypatch, module, payload, *, raw=None, runtime=False, responses=None):
    """Raw strings are synthetic JSON; runtime=True injects after decoding."""
    responses = responses if responses is not None else {SEARCH_URL: SEARCH, DETAIL_URL: payload}
    calls = []

    def opened(url, *, timeout=None):
        calls.append((url, timeout))
        assert url in responses, 'Unexpected synthetic request'
        if raw is not None and url == DETAIL_URL:
            return StringIO(raw)
        # Runtime descriptors deliberately do not claim an original numeric token.
        value = detail(row()) if runtime and url == DETAIL_URL else responses[url]
        return StringIO(json.dumps(value, ensure_ascii=False, allow_nan=False))

    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    if runtime:
        decode = module._decode_json

        def injected(body):
            value = decode(body)
            return payload if type(value) is dict and 'dividends' in value else value

        monkeypatch.setattr(module, '_decode_json', injected)
    return calls


def semantic_message(rule, field, ordinal=0):
    return 'DIVIDEND_SOURCE_SEMANTIC_INVALID: rule={}; field={}; row={}'.format(
        rule, field, ordinal)


@pytest.fixture
def context(tmp_path):
    session = run_logging.configure_logging(tmp_path / 'logs', run_id='c2b-disposable',
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    yield session
    run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def records(context):
    assert run_logging.flush_logging(context)['confirmed']
    events = [json.loads(line) for line in
              (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    for event in events:
        assert event['fields'] == {} and 'endpoint' not in event
    return events


def assert_failed(context):
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']
    return report


INVALID_DATES = [
    pytest.param(None, 'DATE_TYPE', id='null'),
    pytest.param(True, 'DATE_TYPE', id='bool'),
    pytest.param(20240711, 'DATE_TYPE', id='int'),
    pytest.param([], 'DATE_TYPE', id='list'),
    pytest.param({}, 'DATE_TYPE', id='object'),
    pytest.param('2024-7-11', 'DATE_SHAPE', id='short-month'),
    pytest.param('2024-07-11T00:00:00', 'DATE_SHAPE', id='timestamp'),
    pytest.param(' 2024-07-11', 'DATE_SHAPE', id='space'),
    pytest.param('2024-07-11\n', 'DATE_SHAPE', id='newline'),
    pytest.param('２０２４-０７-１１', 'DATE_SHAPE', id='unicode-digits'),
    pytest.param('2024/07/11', 'DATE_SHAPE', id='separator'),
    pytest.param('2023-02-29', 'DATE_CALENDAR', id='nonleap'),
    pytest.param('1900-02-29', 'DATE_CALENDAR', id='century-nonleap'),
    pytest.param('2024-02-30', 'DATE_CALENDAR', id='day-overflow'),
    pytest.param('2024-13-01', 'DATE_CALENDAR', id='month-overflow'),
    pytest.param('0000-01-01', 'DATE_CALENDAR', id='year-zero'),
    pytest.param('2024-00-01', 'DATE_CALENDAR', id='month-zero'),
    pytest.param('2024-01-00', 'DATE_CALENDAR', id='day-zero'),
]


@pytest.mark.parametrize('dt,rule', INVALID_DATES)
def test_invalid_date_is_refused_at_public_loader(monkeypatch, dt, rule):
    module, waits = load_dividends(monkeypatch)
    calls = source(monkeypatch, module, detail(row(dt=dt)))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message(rule, 'dt')
    assert module.divs_all == [] and waits == []
    assert calls == [(SEARCH_URL, 10), (DETAIL_URL, 10)]


@pytest.mark.parametrize('value', [
    pytest.param(None, id='null'), pytest.param(True, id='true'),
    pytest.param(False, id='false'), pytest.param('0.000000001', id='numeric-string'),
    pytest.param([], id='list'), pytest.param({}, id='object'),
])
def test_invalid_amount_type_is_refused_without_coercion(monkeypatch, value):
    module, waits = load_dividends(monkeypatch)
    calls = source(monkeypatch, module, detail(row(value=value)))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message('VALUE_TYPE', 'value')
    assert module.divs_all == [] and waits == [] and len(calls) == 2


@pytest.mark.parametrize('kind,token,value', [
    pytest.param('STANDARD_JSON', '1e309', None, id='standard-json-overflow'),
    pytest.param('NONSTANDARD_JSON_EXTENSION', 'NaN', None, id='synthetic-bare-nan'),
    pytest.param('NONSTANDARD_JSON_EXTENSION', 'Infinity', None, id='synthetic-bare-inf'),
    pytest.param('NONSTANDARD_JSON_EXTENSION', '-Infinity', None, id='synthetic-bare-negative-inf'),
    pytest.param('PYTHON_RUNTIME', None, float('nan'), id='runtime-nan'),
    pytest.param('PYTHON_RUNTIME', None, float('inf'), id='runtime-inf'),
    pytest.param('PYTHON_RUNTIME', None, float('-inf'), id='runtime-negative-inf'),
])
def test_nonfinite_inputs_are_separate_and_nonretryable(monkeypatch, kind, token, value):
    module, waits = load_dividends(monkeypatch)
    if kind == 'PYTHON_RUNTIME':
        calls = source(monkeypatch, module, detail(row(value=value)), runtime=True)
        assert token is None  # No fabricated raw source reference for injection.
    else:
        body = ('{"dividends":{"columns":' + json.dumps(COLUMNS)
                + ',"data":[["SBER","' + ISIN + '","2024-07-11",'
                + token + ',"RUB"]]}}')
        calls = source(monkeypatch, module, None, raw=body)
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message('VALUE_NONFINITE', 'value')
    assert module.divs_all == [] and waits == [] and len(calls) == 2


@pytest.mark.parametrize('currency,rule', [
    pytest.param(None, 'CURRENCY_TYPE', id='null'),
    pytest.param(True, 'CURRENCY_TYPE', id='bool'),
    pytest.param(643, 'CURRENCY_TYPE', id='int'),
    pytest.param([], 'CURRENCY_TYPE', id='list'),
    pytest.param({}, 'CURRENCY_TYPE', id='object'),
    pytest.param('', 'CURRENCY_BLANK', id='empty'),
    pytest.param(' \t\r\n', 'CURRENCY_BLANK', id='ascii-blank'),
    pytest.param('\u00a0\u2003', 'CURRENCY_BLANK', id='unicode-blank'),
])
def test_invalid_currency_type_or_blank_is_refused(monkeypatch, currency, rule):
    module, waits = load_dividends(monkeypatch)
    calls = source(monkeypatch, module, detail(row(currency=currency)))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message(rule, 'currency')
    assert module.divs_all == [] and waits == [] and len(calls) == 2


@pytest.mark.parametrize('codepoint', list(range(9)) + [11, 12] + list(range(14, 32)))
def test_specific_xlsx_illegal_c0_is_refused_before_writers(monkeypatch, codepoint):
    module, waits = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row(currency='R' + chr(codepoint) + 'UB')))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message('CURRENCY_CONTROL', 'currency')
    assert module.divs_all == [] and waits == []


@pytest.mark.parametrize('dt', ['0001-01-01', '2000-02-29', '2024-02-29', '2111-01-01', '9999-12-31'])
def test_calendar_valid_dates_are_preserved_without_future_policy(monkeypatch, dt):
    module, waits = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row(dt=dt)))
    assert module.div_loader(ISIN, 'SBER') is None
    assert module.divs_all == [[ISIN, 'SBER', dt, 33.3, 'RUB']] and waits == []


@pytest.mark.parametrize('value', [
    pytest.param(0, id='zero-int'), pytest.param(-0.0, id='negative-zero'),
    pytest.param(1e-9, id='tiny'), pytest.param(-1e-9, id='negative-tiny'),
    pytest.param(-33, id='negative-int'), pytest.param(10 ** 400, id='huge-int-no-float-cast'),
])
def test_finite_amounts_preserve_exact_python_type_and_value(monkeypatch, value):
    module, waits = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row(value=value)))
    assert module.div_loader(ISIN, 'SBER') is None
    actual = module.divs_all[0][3]
    assert type(actual) is type(value) and actual == value and waits == []
    if type(value) is float:
        assert actual.hex() == value.hex()


def test_standard_json_underflow_is_preserved_as_observed_zero(monkeypatch):
    module, _ = load_dividends(monkeypatch)
    body = ('{"dividends":{"columns":' + json.dumps(COLUMNS)
            + ',"data":[["SBER","' + ISIN + '","2111-01-01",1e-400,"RUB"]]}}')
    source(monkeypatch, module, None, raw=body)
    assert module.div_loader(ISIN, 'SBER') is None
    actual = module.divs_all[0][3]
    assert type(actual) is float and actual.hex() == '0x0.0p+0'
    # The guard sees decoded zero; this does not claim source precision preservation.


@pytest.mark.parametrize('currency', ['SUR', 'rub', ' RUB ', 'UNKNOWN', '\tRUB', 'R\nUB', 'R\rUB', '\u007fRUB'])
def test_currency_has_no_whitelist_case_or_whitespace_normalization(monkeypatch, currency):
    module, _ = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row(currency=currency)))
    assert module.div_loader(ISIN, 'SBER') is None
    assert module.divs_all[0][4] == currency
    # Only the agreed local C0 profile is claimed, not complete serializer safety.


class TextSubclass(str):
    pass


class IntSubclass(int):
    pass


class FloatSubclass(float):
    pass


class NoRepresentation:
    def __str__(self):
        raise AssertionError('Source __str__ must not be evaluated')

    def __repr__(self):
        raise AssertionError('Source __repr__ must not be evaluated')


@pytest.mark.parametrize('field,value,rule', [
    pytest.param('dt', TextSubclass('2024-07-11'), 'DATE_TYPE', id='date-subclass'),
    pytest.param('value', IntSubclass(1), 'VALUE_TYPE', id='int-subclass'),
    pytest.param('value', FloatSubclass(1.0), 'VALUE_TYPE', id='float-subclass'),
    pytest.param('currency', TextSubclass('RUB'), 'CURRENCY_TYPE', id='currency-subclass'),
    pytest.param('dt', NoRepresentation(), 'DATE_TYPE', id='date-no-repr'),
    pytest.param('value', NoRepresentation(), 'VALUE_TYPE', id='amount-no-repr'),
    pytest.param('currency', NoRepresentation(), 'CURRENCY_TYPE', id='currency-no-repr'),
])
def test_runtime_injections_require_exact_types_without_repr(monkeypatch, field, value, rule):
    module, waits = load_dividends(monkeypatch)
    values = {'dt': '2024-07-11', 'value': 33.3, 'currency': 'RUB'}
    values[field] = value
    source(monkeypatch, module, detail(row(**values)), runtime=True)
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message(rule, field)
    assert module.divs_all == [] and waits == []


@pytest.mark.parametrize('rows,rule,field,ordinal', [
    pytest.param([row(dt=None, value=None, currency=None)], 'DATE_TYPE', 'dt', 0,
                 id='date-before-amount-and-currency'),
    pytest.param([row(value=None, currency=None)], 'VALUE_TYPE', 'value', 0,
                 id='amount-before-currency'),
    pytest.param([row(currency=None), row(dt=None)], 'CURRENCY_TYPE', 'currency', 0,
                 id='earlier-row-before-later-date'),
    pytest.param([row(), row(), row(value=None)], 'VALUE_TYPE', 'value', 2,
                 id='late-row-ordinal'),
])
def test_first_semantic_failure_is_deterministic_and_response_atomic(
        monkeypatch, rows, rule, field, ordinal):
    module, waits = load_dividends(monkeypatch)
    module.divs_all.extend(copy.deepcopy(OLD))
    calls = source(monkeypatch, module, detail(*rows))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert str(caught.value) == semantic_message(rule, field, ordinal)
    assert module.divs_all == OLD and waits == [] and len(calls) == 2


def test_full_structural_parse_precedes_semantic_checks(monkeypatch):
    module, _ = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row(dt=None), ['SBER']))
    with pytest.raises(ValueError, match='^DIVIDEND_SOURCE_SCHEMA_INVALID:'):
        module.div_loader(ISIN, 'SBER')
    assert module.divs_all == []


def test_semantic_failure_has_quality_envelope_and_no_source_echo(monkeypatch, context, capsys):
    module, waits = load_dividends(monkeypatch)
    calls = source(monkeypatch, module, detail(row(currency=CANARY + '\u0001')))
    with pytest.raises(ValueError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert str(caught.value) == semantic_message('CURRENCY_CONTROL', 'currency')
    events = records(context)
    assert [event['event'] for event in events] == EVENTS[:-1] + ['dividend_loader_failed']
    failure = events[-1]
    assert failure['outcome'] == 'FAILED' and failure['level'] == 'ERROR'
    assert failure['error']['code'] == 'DIVIDEND_SOURCE_SEMANTIC_INVALID'
    assert failure['error']['category'] == 'QUALITY'
    assert failure['error']['retryable'] is False and failure['error']['final'] is True
    assert failure['error']['endpoint'] == DETAIL_URL
    assert failure['error']['instrument'] == 'SBER' and failure['error']['attempt'] == 1
    assert 'request_phase=DECODE' not in failure['message']
    report = assert_failed(context)
    output = capsys.readouterr()
    stored = ''.join(path.read_text(encoding='utf-8') for path in
                     context['_run_root'].rglob('*.json*'))
    assert CANARY not in str(caught.value) + output.out + output.err + stored
    assert CANARY not in json.dumps(events) + json.dumps(report)
    assert module.divs_all == [] and waits == [] and len(calls) == 2


@pytest.mark.parametrize('fault', ['make_error', 'emit', 'unconfirmed', 'health', 'stderr'])
def test_semantic_primary_identity_survives_secondary_diagnostic_faults(
        monkeypatch, context, fault):
    module, waits = load_dividends(monkeypatch)
    calls = source(monkeypatch, module, detail(row(value=None)))
    captured = []
    original_failure = module._dividend_failure
    original_emit = run_logging.emit_event
    original_health = run_logging.check_logging_health
    failed_event = []

    def failure(session, error, *args, **kwargs):
        captured.append(error)
        return original_failure(session, error, *args, **kwargs)

    def secondary(*args, **kwargs):
        raise RuntimeError('secondary ' + CANARY)

    def emit(session, level, event, message, fields):
        if event == 'dividend_loader_failed':
            failed_event.append(event)
            if fault == 'unconfirmed':
                return {'accepted': True, 'confirmed': False}
            if fault in ('emit', 'stderr'):
                return secondary()
        return original_emit(session, level, event, message, fields)

    def health(session):
        return {'healthy': False} if failed_event and fault == 'health' else original_health(session)

    monkeypatch.setattr(module, '_dividend_failure', failure)
    with monkeypatch.context() as faults:
        faults.setattr(run_logging, 'emit_event', emit)
        faults.setattr(run_logging, 'check_logging_health', health)
        if fault == 'make_error':
            faults.setattr(run_logging, 'make_error', secondary)
        if fault == 'stderr':
            faults.setattr(module, 'print', secondary, raising=False)
        with pytest.raises(ValueError) as caught:
            module.div_loader(ISIN, 'SBER', logging_context=context)
    assert len(captured) == 1 and captured[0] is caught.value
    assert str(caught.value) == semantic_message('VALUE_TYPE', 'value')
    assert module.divs_all == [] and waits == [] and len(calls) == 2
    assert_failed(context)


@pytest.mark.parametrize('fault', ['flush', 'health'])
def test_success_diagnostic_barrier_fault_cannot_append_response(monkeypatch, context, fault):
    module, _ = load_dividends(monkeypatch)
    source(monkeypatch, module, detail(row()))
    module.divs_all.extend(copy.deepcopy(OLD))
    original_emit = run_logging.emit_event
    original_flush = run_logging.flush_logging
    original_health = run_logging.check_logging_health
    returned = []
    primary = RuntimeError('LOGGING_INCOMPLETE')
    captured = []
    original_failure = module._dividend_failure

    def failure(session, error, *args, **kwargs):
        captured.append(error)
        return original_failure(session, error, *args, **kwargs)

    def emit(session, level, event, message, fields):
        if event == 'dividend_loader_returned':
            returned.append(event)
        return original_emit(session, level, event, message, fields)

    def flush(session):
        if returned and fault == 'flush':
            raise primary
        return original_flush(session)

    def health(session):
        if returned and fault == 'health':
            raise primary
        return original_health(session)

    monkeypatch.setattr(module, '_dividend_failure', failure)
    with monkeypatch.context() as faults:
        faults.setattr(run_logging, 'emit_event', emit)
        faults.setattr(run_logging, 'flush_logging', flush)
        faults.setattr(run_logging, 'check_logging_health', health)
        with pytest.raises(RuntimeError) as caught:
            module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary and captured == [primary]
    assert returned == ['dividend_loader_returned'] and module.divs_all == OLD
    assert_failed(context)


def test_healthy_diagnostic_preserves_events_counts_and_uncertainty(monkeypatch, context):
    module, waits = load_dividends(monkeypatch)
    rows = [row(dt='2111-01-01', value=0, currency='SUR'),
            row(value=-1e-9, currency=' rub '), row(value=-1e-9, currency=' rub ')]
    source(monkeypatch, module, detail(*rows))
    assert module.div_loader(ISIN, 'SBER', logging_context=context) is None
    assert module.divs_all == [[ISIN, 'SBER'] + item[2:] for item in rows]
    assert waits == []
    events = records(context)
    assert [event['event'] for event in events] == EVENTS
    returned = events[-1]
    assert returned['outcome'] == 'RETURNED' and returned['error'] is None
    assert returned['counts'] == {'rows_received': 3, 'rows_accepted': 3,
        'rows_quarantined': None, 'pages': 1, 'instruments': 1,
        'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
    assert 'source_semantics=NOT_VERIFIED' in returned['message']
    assert 'precision=NOT_VERIFIED' in returned['message']
    assert 'PASS' not in returned['message'] and 'COMPLETE' not in returned['message']


def test_success_then_semantic_failure_keeps_c1_rows_and_previous_disposable_files(
        monkeypatch, context, tmp_path):
    module, waits = load_dividends(monkeypatch)
    module.current_path = str(tmp_path)
    directory = tmp_path / 'datasets' / 'dividends'
    directory.mkdir(parents=True)
    previous = {'all.csv': b'SYNTHETIC_PREVIOUS_CSV\r\n',
                'all.xlsx': b'SYNTHETIC_PREVIOUS_XLSX\x00\xff'}
    digests = {}
    for name, value in previous.items():
        (directory / name).write_bytes(value)
        digests[name] = hashlib.sha256(value).hexdigest()
    responses = {SEARCH_URL: SEARCH, DETAIL_URL: detail(row()),
                 OTHER_SEARCH_URL: OTHER_SEARCH,
                 OTHER_DETAIL_URL: detail(['MOEX', OTHER_ISIN, '2024-07-11', 17.35, 'RUB'],
                                          ['MOEX', OTHER_ISIN, '2024-02-30', 1.0, 'RUB'])}
    calls = source(monkeypatch, module, None, responses=responses)
    monkeypatch.setattr(module.pd, 'read_excel', lambda path: pd.DataFrame(
        [['SBER', ISIN], ['MOEX', OTHER_ISIN]], columns=['TRADE_CODE', 'ISIN']))
    writes = []
    monkeypatch.setattr(pd.DataFrame, 'to_excel', lambda *args, **kwargs: writes.append('xlsx'))
    monkeypatch.setattr(pd.DataFrame, 'to_csv', lambda *args, **kwargs: writes.append('csv'))
    with pytest.raises(ValueError) as caught:
        module.main(logging_context=context)
    assert str(caught.value) == semantic_message('DATE_CALENDAR', 'dt', 1)
    assert module.divs_all == [[ISIN, 'SBER', '2024-07-11', 33.3, 'RUB']]
    assert [url for url, _ in calls] == [SEARCH_URL, DETAIL_URL, OTHER_SEARCH_URL, OTHER_DETAIL_URL]
    assert writes == [] and waits == []
    for name, value in previous.items():
        actual = (directory / name).read_bytes()
        assert actual == value and hashlib.sha256(actual).hexdigest() == digests[name]
    events = records(context)
    assert len([event for event in events if event['event'] == 'dividend_loader_returned']) == 1
    assert events[-1]['event'] == 'dividend_loader_failed'
    assert not any(event['event'] in ('dividend_export_input', 'dividend_collection_returned')
                   for event in events)
    assert_failed(context)


def test_healthy_main_keeps_five_columns_writer_order_kwargs_and_none(monkeypatch, tmp_path):
    module, _ = load_dividends(monkeypatch)
    module.current_path = str(tmp_path)
    rows = [row(dt='2111-01-01', value=0), row(value=-1e-9, currency='SUR'),
            row(value=-1e-9, currency='SUR')]
    source(monkeypatch, module, detail(*rows))
    monkeypatch.setattr(module.pd, 'read_excel', lambda path: pd.DataFrame(
        [['SBER', ISIN]], columns=['TRADE_CODE', 'ISIN']))
    writes = []

    def writer(kind):
        def write(frame, path, **kwargs):
            writes.append((kind, path, list(frame.columns), frame.values.tolist(), kwargs))
        return write

    monkeypatch.setattr(pd.DataFrame, 'to_excel', writer('xlsx'))
    monkeypatch.setattr(pd.DataFrame, 'to_csv', writer('csv'))
    assert module.main() is None
    expected = [[ISIN, 'SBER'] + item[2:] for item in rows]
    path = str(tmp_path) + '/datasets/dividends/all'
    assert writes == [('xlsx', path + '.xlsx', OUTPUT_COLUMNS, expected, {'index': False}),
                      ('csv', path + '.csv', OUTPUT_COLUMNS, expected, {'index': False})]
    assert module.divs_all == expected
