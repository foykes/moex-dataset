"""Closed offline oracles for the A2_CANDLES_CSV_V1 consumer profile."""

import csv
import hashlib
import io
import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests


RAW_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']
OUTPUT_COLUMNS = RAW_COLUMNS + ['ticker']
HEADER = 'open;close;high;low;value;volume;begin;end\n'
ROW_ONE = '100.0;101.0;102.0;99.0;202000.0;2000;2024-02-29 00:00:00;2024-02-29 23:59:59\n'
ROW_TWO = '101.0;103.0;104.0;100.0;309000.0;3000;2024-03-01 00:00:00;2024-03-01 23:59:59\n'
BODY = 'candles\n' + HEADER + ROW_ONE + ROW_TWO
ROWS = [
    [100.0, 101.0, 102.0, 99.0, 202000.0, 2000,
     '2024-02-29 00:00:00', '2024-02-29 23:59:59', 'FIXTURE_SHARE'],
    [101.0, 103.0, 104.0, 100.0, 309000.0, 3000,
     '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'FIXTURE_SHARE'],
]
DTYPES = ['float64'] * 5 + ['int64', 'object', 'object', 'object']
SCHEMA_CODE = 'A2_ISS_RESPONSE_SCHEMA_INVALID'
DECODE_CODE = 'A2_ISS_RESPONSE_DECODE_ERROR'
HTTPS_QUERY = ('https://iss.moex.com/iss/engines/stock/markets/shares/securities/'
               'FIXTURE_SHARE/candles.csv?from=2024-02-29&till=2024-03-01&interval=24')


def response(body=BODY, content_type='text/csv', status=200):
    result = requests.Response()
    result.status_code = status
    result._content = body if type(body) is bytes else body.encode('utf-8')
    result.encoding = 'utf-8'  # Baseline-compatible; V1 must decode content itself.
    if content_type is not None:
        result.headers['Content-Type'] = content_type
    return result


def transport(gathering, monkeypatch, outcomes):
    sequence = iter(outcomes)
    calls, sleeps = [], []
    elapsed = iter(range(1000))

    def get(url, headers, timeout, verify=None, allow_redirects=None):
        calls.append((url, dict(headers), timeout, verify, allow_redirects))
        item = next(sequence)
        if isinstance(item, BaseException):
            raise item
        return item

    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'header', {'User-Agent': 'AGENT_A'})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': 'AGENT_B'})
    monkeypatch.setattr(gathering, 'time',
                        SimpleNamespace(sleep=sleeps.append, perf_counter=lambda: next(elapsed)))
    return calls, sleeps


def query(gathering, **kwargs):
    return gathering.moex_query('FIXTURE_SHARE', 'Акции', '2024-02-29', '2024-03-01',
                                24, **kwargs)


def changed_row(column, value):
    cells = ROW_ONE.rstrip('\n').split(';')
    cells[RAW_COLUMNS.index(column)] = value
    return 'candles\n' + HEADER + ';'.join(cells) + '\n' + ROW_TWO


def assert_refusal(gathering, monkeypatch, body, content_type='text/csv'):
    calls, sleeps = transport(gathering, monkeypatch, [response(body, content_type)])
    with pytest.raises((ValueError, csv.Error)) as captured:
        query(gathering, max_retries=1)
    if type(captured.value) is ValueError:
        assert captured.value.args == (SCHEMA_CODE,)
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A'}, 30, True, False)]
    assert sleeps == []


@pytest.mark.parametrize('body,content_type', [
    pytest.param(BODY, 'text/csv', id='default-cp1251'),
    pytest.param(BODY, 'TEXT/CSV', id='mime-case'),
    pytest.param(BODY, 'text/csv; charset=cp1251', id='cp1251'),
    pytest.param(BODY, 'text/csv; charset=windows-1251', id='windows-1251'),
    pytest.param(BODY, 'text/csv; charset="CP1251"', id='quoted-cp1251'),
    pytest.param(BODY, 'text/csv; charset=utf8', id='utf8'),
    pytest.param(BODY, 'TEXT/CSV; CHARSET="UTF-8"', id='quoted-utf8-case'),
    pytest.param(BODY.replace('\n', '\r\n'), 'text/csv', id='crlf'),
    pytest.param(BODY.rstrip('\n'), 'text/csv', id='eof-without-newline'),
    pytest.param(BODY.replace('\n', '\r\n').rstrip('\r\n'), 'text/csv', id='crlf-eof'),
    pytest.param('\n\ncandles\n\n' + HEADER + ROW_ONE + '\n' + ROW_TWO + '\n',
                 'text/csv', id='physical-blank-records'),
    pytest.param('\ufeff' + BODY, 'text/csv; charset=utf-8', id='one-explicit-utf8-bom'),
    pytest.param('candles\n' + HEADER +
                 '"100.0";"101.0";"102.0";"99.0";"202000.0";"2000";'
                 '"2024-02-29 00:00:00";"2024-02-29 23:59:59"\n' + ROW_TWO,
                 'text/csv', id='csv-quoted-cells'),
    pytest.param(changed_row('open', ' \t100.0\t '), 'text/csv', id='numeric-ascii-whitespace'),
])
def test_accepted_profile_preserves_literal_values_dtypes_and_index(
        gathering, monkeypatch, body, content_type, event_log):
    calls, sleeps = transport(gathering, monkeypatch, [response(body, content_type)])
    result = query(gathering, logging_context=event_log.context)
    assert result.columns.tolist() == OUTPUT_COLUMNS
    assert result.values.tolist() == ROWS
    assert result.dtypes.astype(str).tolist() == DTYPES
    assert result.index.tolist() == [0, 1]
    assert list(zip(result.ticker, [24, 24], result.begin)) == [
        ('FIXTURE_SHARE', 24, '2024-02-29 00:00:00'),
        ('FIXTURE_SHARE', 24, '2024-03-01 00:00:00'),
    ]
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A'}, 30, True, False)]
    assert sleeps == [3]
    returned = event_log.events[-1]
    assert (returned['event'], returned['outcome']) == (
        'a2_query_returned', 'QUERY_RETURNED_UNVALIDATED')
    assert returned['counts'] == {
        'rows_received': 2, 'rows_accepted': None, 'rows_quarantined': None,
        'pages': None, 'instruments': None,
        'null_reasons': {'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED',
                         'pages': 'NOT_PAGINATION_EVIDENCE', 'instruments': 'NOT_ASSESSED'},
    }


@pytest.mark.parametrize('column,token,expected,dtypes', [
    pytest.param('open', '+1.00e2', 100.0, DTYPES, id='scientific-open'),
    pytest.param('volume', '2e3', 2000.0,
                 ['float64'] * 6 + ['object'] * 3, id='scientific-volume-inference'),
    pytest.param('volume', '2000.5', 2000.5,
                 ['float64'] * 6 + ['object'] * 3, id='fractional-volume'),
    pytest.param('value', '-202000.0', -202000.0, DTYPES, id='finite-negative-value'),
    pytest.param('volume', '-2000', -2000, DTYPES, id='finite-negative-volume'),
    pytest.param('open', '-0.0', -0.0, DTYPES, id='negative-zero-preserved'),
    pytest.param('high', '90.0', 90.0, DTYPES, id='ohlc-anomaly-structural-only'),
    pytest.param('end', '2024-02-28 23:59:59', '2024-02-28 23:59:59', DTYPES,
                 id='end-before-begin-structural-only'),
])
def test_numeric_inference_and_quality_boundaries_preserve_raw_values(
        gathering, monkeypatch, column, token, expected, dtypes):
    calls, sleeps = transport(gathering, monkeypatch, [response(changed_row(column, token))])
    result = query(gathering)
    literal = [list(row) for row in ROWS]
    literal[0][RAW_COLUMNS.index(column)] = expected
    assert result.columns.tolist() == OUTPUT_COLUMNS
    assert result.values.tolist() == literal
    assert result.dtypes.astype(str).tolist() == dtypes
    assert result.index.tolist() == [0, 1]
    if token == '-0.0':
        assert math.copysign(1.0, result.iloc[0]['open']) == -1.0
    assert len(calls) == 1 and sleeps == [3]


@pytest.mark.parametrize('body,content_type', [
    pytest.param(BODY, None, id='missing-mime'),
    pytest.param(BODY, 'text/plain', id='wrong-mime'),
    pytest.param(BODY, 'text/html; charset=utf-8', id='html-mime'),
    pytest.param(BODY, 'application/json', id='json-mime'),
    pytest.param(BODY, 'text/csv; boundary=fixture', id='unknown-mime-parameter'),
    pytest.param(BODY, 'text/csv; charset=latin-1', id='unknown-charset'),
    pytest.param(BODY, 'text/csv; charset=', id='empty-charset'),
    pytest.param(BODY, 'text/csv; charset=utf-8; charset=utf-8', id='duplicate-charset'),
    pytest.param(BODY, 'text/csv; charset="utf-8', id='unclosed-charset-quote'),
    pytest.param(BODY, 'text/csv; charſet=utf8', id='nonascii-charset-name'),
    pytest.param(BODY.replace('\n', '\r'), 'text/csv', id='bare-cr-records'),
    pytest.param('', 'text/csv', id='empty-body'),
    pytest.param(HEADER + ROW_ONE, 'text/csv', id='missing-block'),
    pytest.param(BODY.replace('candles\n', 'other\n', 1), 'text/csv', id='wrong-block'),
    pytest.param('candles\n', 'text/csv', id='missing-header'),
    pytest.param('candles\n' + HEADER + '\t\n' + ROW_ONE, 'text/csv', id='whitespace-record'),
    pytest.param('candles\n' + HEADER + ';;;;;;;\n', 'text/csv', id='eight-empty-fields'),
    pytest.param(BODY + 'candles\n' + HEADER, 'text/csv', id='second-block'),
    pytest.param(BODY + 'candles.cursor\nINDEX;TOTAL\n0;2\n', 'text/csv', id='trailer-block'),
    pytest.param(BODY + 'UNDECLARED_TRAILER\n', 'text/csv', id='trailing-nonempty-record'),
    pytest.param('candles\n' + HEADER + ROW_ONE.replace(';2000;', ';', 1),
                 'text/csv', id='short-row'),
    pytest.param('candles\n' + HEADER + ROW_ONE.rstrip('\n') + ';extra\n',
                 'text/csv', id='long-row'),
    pytest.param(BODY + ROW_ONE.replace(';2000;', ';', 1), 'text/csv', id='good-prefix-bad-tail'),
    pytest.param('candles\n' + HEADER + '"100.0;101.0;102.0;99.0;202000.0;2000;'
                 '2024-02-29 00:00:00;2024-02-29 23:59:59\n',
                 'text/csv', id='unclosed-csv-quote'),
    pytest.param('\ufeff' + BODY, 'text/csv', id='bom-with-default-cp1251'),
    pytest.param('\ufeff' + BODY, 'text/csv; charset=cp1251', id='bom-with-explicit-cp1251'),
    pytest.param('\ufeff\ufeff' + BODY, 'text/csv; charset=utf-8', id='repeated-utf8-bom'),
    pytest.param(changed_row('open', '100.0\ufeff'), 'text/csv; charset=utf-8', id='embedded-bom'),
])
def test_rejects_invalid200_envelope_and_mime(gathering, monkeypatch, body, content_type):
    assert_refusal(gathering, monkeypatch, body, content_type)


@pytest.mark.parametrize('column', RAW_COLUMNS, ids=RAW_COLUMNS)
def test_rejects_missing_raw_column(gathering, monkeypatch, column):
    position = RAW_COLUMNS.index(column)
    header = [item for offset, item in enumerate(RAW_COLUMNS) if offset != position]
    rows = []
    for source in (ROW_ONE, ROW_TWO):
        cells = source.rstrip('\n').split(';')
        rows.append(';'.join(item for offset, item in enumerate(cells) if offset != position))
    body = 'candles\n' + ';'.join(header) + '\n' + '\n'.join(rows) + '\n'
    assert_refusal(gathering, monkeypatch, body)


@pytest.mark.parametrize('column', RAW_COLUMNS, ids=RAW_COLUMNS)
def test_rejects_duplicate_raw_column(gathering, monkeypatch, column):
    position = RAW_COLUMNS.index(column)
    headers = list(RAW_COLUMNS)
    headers[(position + 1) % len(headers)] = column
    body = 'candles\n' + ';'.join(headers) + '\n' + ROW_ONE + ROW_TWO
    assert_refusal(gathering, monkeypatch, body)


@pytest.mark.parametrize('body', [
    pytest.param('candles\nclose;open;high;low;value;volume;begin;end\n' + ROW_ONE + ROW_TWO,
                 id='reordered-header'),
    pytest.param('candles\n' + HEADER.rstrip('\n') + ';ticker\n' +
                 ROW_ONE.rstrip('\n') + ';FOREIGN_ID\n', id='foreign-ticker-column'),
    pytest.param('candles\n' + HEADER.rstrip('\n') + ';extra\n' +
                 ROW_ONE.rstrip('\n') + ';extra\n', id='extra-column'),
])
def test_rejects_nonexact_ordered_raw_header(gathering, monkeypatch, body):
    assert_refusal(gathering, monkeypatch, body)


@pytest.mark.parametrize('column', RAW_COLUMNS[:6], ids=RAW_COLUMNS[:6])
@pytest.mark.parametrize('token', [
    pytest.param('', id='empty'), pytest.param('NA', id='null-na'),
    pytest.param('NaN', id='nan'), pytest.param('Inf', id='infinity'),
    pytest.param('-inf', id='negative-infinity'), pytest.param('1e309', id='overflow'),
    pytest.param('not-a-number', id='text'), pytest.param('1,000', id='comma-number'),
    pytest.param('1_000', id='underscore-number'), pytest.param('True', id='boolean-text'),
])
def test_rejects_invalid_numeric_token(gathering, monkeypatch, column, token):
    assert_refusal(gathering, monkeypatch, changed_row(column, token))


@pytest.mark.parametrize('column', ['begin', 'end'], ids=['begin', 'end'])
@pytest.mark.parametrize('value', [
    pytest.param('', id='empty'),
    pytest.param('2024-02-30 00:00:00', id='invalid-calendar-day'),
    pytest.param('2023-02-29 00:00:00', id='nonleap-day'),
    pytest.param('2024-13-01 00:00:00', id='invalid-month'),
    pytest.param('2024-02-29 25:00:00', id='invalid-hour'),
    pytest.param('2024-2-29 00:00:00', id='unpadded-month'),
    pytest.param('2024-02-29T00:00:00', id='iso-separator'),
    pytest.param('2024-02-29 00:00:00Z', id='timezone-suffix'),
    pytest.param('2024-02-29 00:00:00.1', id='fractional-seconds'),
    pytest.param(' 2024-02-29 00:00:00', id='leading-space'),
    pytest.param('２０２４-02-29 00:00:00', id='nonascii-year'),
])
def test_rejects_invalid_timestamp(gathering, monkeypatch, column, value):
    assert_refusal(gathering, monkeypatch, changed_row(column, value))


@pytest.mark.parametrize('tail', [
    pytest.param(ROW_ONE, id='identical'),
    pytest.param(ROW_TWO.replace('2024-03-01 00:00:00', '2024-02-29 00:00:00'), id='conflicting'),
])
def test_rejects_response_local_duplicate_begin(gathering, monkeypatch, tail):
    assert_refusal(gathering, monkeypatch, 'candles\n' + HEADER + ROW_ONE + tail)


@pytest.mark.parametrize('body,content_type', [
    pytest.param('candles\n' + HEADER, 'text/csv', id='header-only'),
    pytest.param('\n\ncandles\n\n' + HEADER + '\n\n', 'text/csv', id='blank-records'),
    pytest.param('\ufeffcandles\r\n' + HEADER.replace('\n', '\r\n'),
                 'text/csv; charset=utf-8', id='explicit-utf8-bom-crlf'),
])
def test_explicit_structural_empty_keeps_legacy_shape_and_unvalidated_outcome(
        gathering, monkeypatch, event_log, body, content_type):
    calls, sleeps = transport(gathering, monkeypatch, [response(body, content_type)])
    result = query(gathering, logging_context=event_log.context)
    assert result.empty and result.columns.tolist() == [] and result.index.tolist() == []
    assert len(calls) == 1 and sleeps == [3]
    returned = event_log.events[-1]
    assert (returned['event'], returned['outcome']) == (
        'a2_query_returned', 'QUERY_EMPTY_UNVALIDATED')
    assert returned['counts']['rows_received'] == 0
    assert returned['counts']['rows_accepted'] is None
    assert returned['counts']['rows_quarantined'] is None
    assert returned['counts']['pages'] is None and returned['counts']['instruments'] is None
    assert returned['counts']['null_reasons'] == {
        'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED',
        'pages': 'NOT_PAGINATION_EVIDENCE', 'instruments': 'NOT_ASSESSED',
    }


def test_bond_route_keeps_request_bound_identity_without_payload_identity_claim(
        gathering, monkeypatch):
    calls, sleeps = transport(gathering, monkeypatch, [response()])
    result = gathering.moex_query('FIXTURE_BOND', 'Облигации', '2024-02-29', '2024-03-01', 24)
    assert result.values.tolist() == [row[:-1] + ['FIXTURE_BOND'] for row in ROWS]
    assert result.columns.tolist() == OUTPUT_COLUMNS and result.index.tolist() == [0, 1]
    assert result.dtypes.astype(str).tolist() == DTYPES
    assert calls == [(HTTPS_QUERY.replace('/shares/', '/bonds/').replace('FIXTURE_SHARE', 'FIXTURE_BOND'),
                     {'User-Agent': 'AGENT_A'}, 30, True, False)]
    assert sleeps == [3]


@pytest.mark.parametrize('body,content_type', [
    pytest.param(BODY.encode('utf-8').replace(b'100.0;', b'100.0\x98;', 1),
                 'text/csv; charset=utf-8', id='invalid-utf8'),
    pytest.param(BODY.encode('utf-8').replace(b'100.0;', b'100.0\x98;', 1),
                 'text/csv', id='undefined-default-cp1251'),
    pytest.param(BODY.encode('utf-8').replace(b'100.0;', b'100.0\xc0\xaf;', 1),
                 'text/csv; charset=utf8', id='overlong-utf8'),
])
def test_decode_failure_keeps_native_primary_and_safe_closed_record(
        gathering, monkeypatch, event_log, body, content_type):
    calls, sleeps = transport(gathering, monkeypatch, [response(body, content_type)])
    original = gathering._a2_diagnostic
    observed = []

    def diagnostic(context, level, event, message, fields, primary=None):
        if level == 'ERROR':
            observed.append((primary, primary.args))
        return original(context, level, event, message, fields, primary)

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    with pytest.raises(UnicodeDecodeError) as captured:
        query(gathering, max_retries=1, logging_context=event_log.context)
    assert observed == [(captured.value, captured.value.args)]
    assert captured.value.object == body
    assert len(calls) == 1 and sleeps == []
    final = event_log.events[-1]
    assert (final['event'], final['outcome'], final['error']['code'], final['error']['category']) == (
        'a2_query_failed', 'RETRY_EXHAUSTED', DECODE_CODE, 'SOURCE')
    assert final['error']['final'] is True and final['error']['retryable'] is False
    assert final['retries'] == {'attempt': 1, 'limit': 1}
    assert final['fields'] == {}


def test_schema_failure_retains_legacy_attempt_limit_header_rotation_and_backoff(
        gathering, monkeypatch, event_log):
    invalid = changed_row('open', 'INVALID_NUMBER')
    calls, sleeps = transport(gathering, monkeypatch, [response(invalid)] * 5 + [response()])
    with pytest.raises(ValueError) as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value.args == (SCHEMA_CODE,)
    assert len(calls) == 5 and sleeps == [3, 6, 12, 24]
    assert [item[1] for item in calls] == [{'User-Agent': 'AGENT_A'}] + [{'User-Agent': 'AGENT_B'}] * 4
    retries = [item for item in event_log.events if item['event'] == 'a2_query_retry']
    assert len(retries) == 4
    assert [item['retries'] for item in retries] == [
        {'attempt': attempt, 'limit': 5} for attempt in range(1, 5)]
    assert all(item['outcome'] == 'RETRY_PENDING' and item['error']['code'] == SCHEMA_CODE
               and item['error']['category'] == 'SOURCE' and item['error']['final'] is False
               and item['error']['retryable'] is True and item['fields'] == {} for item in retries)
    final = event_log.events[-1]
    assert (final['event'], final['outcome']) == ('a2_query_failed', 'RETRY_EXHAUSTED')
    assert final['retries'] == {'attempt': 5, 'limit': 5}
    assert final['error']['code'] == SCHEMA_CODE
    assert final['error']['final'] is True and final['error']['retryable'] is False
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


def test_schema_then_success_keeps_literal_frame_and_both_sleeps(gathering, monkeypatch):
    calls, sleeps = transport(gathering, monkeypatch, [response(changed_row('open', 'BAD')), response()])
    result = query(gathering, max_retries=2)
    assert result.values.tolist() == ROWS and result.columns.tolist() == OUTPUT_COLUMNS
    assert result.dtypes.astype(str).tolist() == DTYPES and result.index.tolist() == [0, 1]
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A'}, 30, True, False),
                     (HTTPS_QUERY, {'User-Agent': 'AGENT_B'}, 30, True, False)]
    assert sleeps == [3, 3]


@pytest.mark.parametrize('with_context', [False, True], ids=['none', 'context'])
@pytest.mark.parametrize('kind', ['mime', 'schema', 'decode', 'csv'], ids=['mime', 'schema', 'decode', 'csv'])
def test_response_validation_console_and_logger_canaries_remain_safe(
        gathering, monkeypatch, event_log, capsys, with_context, kind):
    markers = ('A2_V1_INSTRUMENT_CANARY', 'A2_V1_CONTEXT_CANARY', 'A2_V1_UA_CANARY',
               'A2_V1_BODY_CANARY', 'A2_V1_HEADER_CANARY')
    raw_url = 'https://user:password@other.invalid/' + markers[3]
    content_type = 'text/csv; charset=utf-8'
    body = changed_row('open', markers[3] + raw_url)
    if kind == 'mime':
        body = BODY
        content_type += '; secret=' + markers[4]
    elif kind == 'decode':
        body = body.encode('utf-8').replace(b'candles\n', b'candles\n\x98', 1)
    elif kind == 'csv':
        body = 'candles\n' + HEADER + '"' + markers[3] + raw_url + '\n'
    calls, sleeps = transport(gathering, monkeypatch, [response(body, content_type)])
    monkeypatch.setattr(gathering, 'header', {'User-Agent': markers[2]})
    event_log.context['_secrets'] += markers
    event_log.context.update(snapshot_id=markers[1], dataset_id=markers[1], file=markers[1])
    context = event_log.context if with_context else None
    original = gathering._a2_diagnostic
    observed = []

    def diagnostic(context, level, event, message, fields, primary=None):
        if level == 'ERROR':
            observed.append((primary, primary.args))
        return original(context, level, event, message, fields, primary)

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    with pytest.raises((ValueError, csv.Error, UnicodeDecodeError)) as captured:
        gathering.moex_query(markers[0], 'Акции', '2024-02-29', '2024-03-01', 24,
                             max_retries=1, logging_context=context)
    assert observed == [(captured.value, captured.value.args)]
    assert not getattr(captured.value, '__notes__', [])
    assert len(calls) == 1 and sleeps == []
    output = capsys.readouterr()
    serialized = json.dumps(event_log.events, ensure_ascii=False)
    combined = output.out + output.err + serialized
    assert all(marker not in combined for marker in markers)
    assert all(value not in combined for value in (raw_url, 'https://', 'User-Agent', 'user:password'))
    assert all(len(line) <= 256 for line in (output.out + output.err).splitlines())
    assert bool(event_log.events) is with_context
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)
    if with_context:
        final = event_log.events[-1]
        assert final['error']['code'] == (DECODE_CODE if kind == 'decode' else SCHEMA_CODE)
        assert final['error']['category'] == 'SOURCE' and final['fields'] == {}


@pytest.mark.parametrize('point,failure', [
    pytest.param('error', 'throw', id='error-throw'),
    pytest.param('error', 'rejected', id='error-rejected'),
    pytest.param('error', 'unconfirmed', id='error-unconfirmed'),
    pytest.param('error', 'unhealthy', id='error-unhealthy'),
    pytest.param('error', 'health_throw', id='error-health-throw'),
    pytest.param('retry', 'throw', id='retry-throw'),
    pytest.param('retry', 'rejected', id='retry-rejected'),
    pytest.param('retry', 'unhealthy', id='retry-unhealthy'),
    pytest.param('retry', 'health_throw', id='retry-health-throw'),
])
@pytest.mark.parametrize('kind', ['schema', 'decode'], ids=['schema', 'decode'])
def test_response_primary_precedes_failed_diagnostics_without_another_request(
        gathering, monkeypatch, event_log, capsys, point, failure, kind):
    body = changed_row('open', 'A2_V1_PRIMARY_BODY_CANARY')
    if kind == 'decode':
        body = body.encode('utf-8').replace(b'candles\n', b'candles\n\x98', 1)
    calls, sleeps = transport(gathering, monkeypatch,
                             [response(body, 'text/csv; charset=utf-8')] * 2)
    target = 'a2_query_failed' if point == 'error' else 'a2_query_retry'
    original_diagnostic = gathering._a2_diagnostic
    original_emit = event_log.logger.emit_event
    observed, targeted = [], []

    def diagnostic(context, level, event, message, fields, primary=None):
        if event == target:
            observed.append((primary, primary.args))
        return original_diagnostic(context, level, event, message, fields, primary)

    def emit(context, level, event, message, fields):
        if event == target:
            targeted.append(event)
            if failure == 'throw':
                raise OSError('A2_V1_LOGGING_CANARY')
            event_log.accepted = failure != 'rejected'
            event_log.confirmed = failure != 'unconfirmed'
            event_log.healthy = failure != 'unhealthy'
        return original_emit(context, level, event, message, fields)

    def health(context):
        if targeted and failure == 'health_throw':
            raise OSError('A2_V1_HEALTH_CANARY')
        return {'healthy': event_log.healthy}

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(event_log.logger, 'check_logging_health', health)
    expected = UnicodeDecodeError if kind == 'decode' else ValueError
    with pytest.raises(expected) as captured:
        query(gathering, max_retries=1 if point == 'error' else 2,
              logging_context=event_log.context)
    assert observed == [(captured.value, captured.value.args)]
    assert captured.value.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    assert len(calls) == 1 and sleeps == []
    assert capsys.readouterr().err == 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'
    serialized = json.dumps(event_log.events, ensure_ascii=False)
    assert all(marker not in serialized for marker in (
        'A2_V1_PRIMARY_BODY_CANARY', 'A2_V1_LOGGING_CANARY', 'A2_V1_HEALTH_CANARY'))
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


@pytest.mark.parametrize('operation', ['annual', 'reload', 'update'], ids=['annual', 'reload', 'update'])
@pytest.mark.parametrize('with_context', [False, True], ids=['none', 'context'])
def test_later_invalid_response_prevents_partial_return_and_local_writers(
        gathering, monkeypatch, catalogue, snapshot, candle_frame, fixture_root,
        writers, save_dataset, event_log, operation, with_context):
    root_path, root = fixture_root
    target = root_path / 'datasets/fixture_v1_previous.csv'
    target.parent.mkdir(parents=True)
    previous = {}
    for suffix in ('csv', 'xlsx'):
        path = target.with_suffix('.' + suffix)
        path.write_bytes(('A2_V1_PREVIOUS_' + suffix).encode('utf-8'))
        previous[path] = (path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest())
    if operation == 'update':
        content = save_dataset(target, candle_frame)
        previous[target] = (content, hashlib.sha256(content).hexdigest())
        original_read = gathering.pd.read_csv
        catalogue_path = root_path / 'datasets/ticker_lists/moex_full.csv'

        def read(path, *args, **kwargs):
            if isinstance(path, io.StringIO):
                return original_read(path, *args, **kwargs)
            if Path(path) == catalogue_path:
                assert args == () and kwargs == {'index_col': 0}
                return catalogue.copy(deep=True)
            assert Path(path) == target and args == () and kwargs == {}
            return original_read(path, *args, **kwargs)

        monkeypatch.setattr(gathering.pd, 'read_csv', read)
    invalid = changed_row('open', 'INVALID_LATER_NUMBER')
    calls, sleeps = transport(gathering, monkeypatch, [response()] + [response(invalid)] * 5)
    context = event_log.context if with_context else None
    with pytest.raises(ValueError) as captured:
        if operation == 'annual':
            gathering.moex('FIXTURE_SHARE', 'Акции', 3, 24, logging_context=context)
        elif operation == 'reload':
            gathering.full_reload(catalogue, 24, 10, 'fixture_v1_previous', 'часа', root,
                                  snapshot, logging_context=context)
        else:
            gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture_v1_previous',
                                    'word': 'часа'}], root, catalogue, snapshot, logging_context=context)
    assert captured.value.args == (SCHEMA_CODE,)
    assert len(calls) == 6 and sleeps == [3, 3, 6, 12, 24] and writers == []
    assert all((path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest()) == value
               for path, value in previous.items())
    if operation == 'annual':
        assert gathering.exception_list == ['FIXTURE_SHARE']
    assert not any(item['event'] in ('a1_full_reload_returned', 'a1_data_update_returned')
                   for item in event_log.events)
    if with_context:
        query_events = [item for item in event_log.events if item['event'].startswith('a2_query_')]
        assert sum(item['event'] == 'a2_query_returned' for item in query_events) == 1
        assert query_events[-1]['event'] == 'a2_query_failed'
        assert query_events[-1]['error']['code'] == SCHEMA_CODE


@pytest.mark.parametrize('year', ['0001', '0999'], ids=['year-0001', 'year-0999'])
def test_calendar_grammar_does_not_add_a_platform_year_limit(gathering, monkeypatch, year):
    begin = year + '-01-01 00:00:00'
    calls, sleeps = transport(gathering, monkeypatch, [response(changed_row('begin', begin))])
    result = query(gathering)
    literal = [list(row) for row in ROWS]
    literal[0][6] = begin
    assert result.values.tolist() == literal
    assert result.columns.tolist() == OUTPUT_COLUMNS
    assert result.dtypes.astype(str).tolist() == DTYPES
    assert result.index.tolist() == [0, 1]
    assert len(calls) == 1 and sleeps == [3]
