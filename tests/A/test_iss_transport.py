"""Literal offline candle transport/failure regressions for A2-01."""

import builtins
import hashlib
import io
import json
import sys
import uuid
from types import SimpleNamespace

import pytest
import requests
from urllib3._collections import HTTPHeaderDict


CANDLE_CSV = (
    'candles\n'
    'open;close;high;low;value;volume;begin;end\n'
    '100.0;101.0;102.0;99.0;202000.0;2000;2024-02-29 00:00:00;2024-02-29 23:59:59\n'
    '101.0;103.0;104.0;100.0;309000.0;3000;2024-03-01 00:00:00;2024-03-01 23:59:59\n'
)
EMPTY_CSV = 'candles\nopen;close;high;low;value;volume;begin;end\n'
CANDLE_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
CANDLE_ROWS = [
    [100.0, 101.0, 102.0, 99.0, 202000.0, 2000,
     '2024-02-29 00:00:00', '2024-02-29 23:59:59', 'FIXTURE_SHARE'],
    [101.0, 103.0, 104.0, 100.0, 309000.0, 3000,
     '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'FIXTURE_SHARE'],
]
HTTPS_QUERY = ('https://iss.moex.com/iss/engines/stock/markets/shares/securities/'
               'FIXTURE_SHARE/candles.csv?from=2024-02-29&till=2024-03-01&interval=24&start=0')


def response(status, text=''):
    result = requests.Response()
    result.status_code = status
    result._content = text.encode('utf-8')
    result.encoding = 'utf-8'
    result.headers['Content-Type'] = 'text/csv; charset=utf-8'
    class RawBody(io.BytesIO):
        def read(self, amt=None, decode_content=False):
            assert type(amt) is int and 0 < amt <= 65536
            assert decode_content is False
            return super().read(amt)

    result.raw = RawBody(result._content)
    result.raw.headers = HTTPHeaderDict(result.headers)
    return result


def transport(gathering, monkeypatch, outcomes):
    sequence = iter(outcomes)
    calls, sleeps = [], []
    def get(url, headers, timeout, verify=None, allow_redirects=None, stream=None):
        # Accept baseline kwargs: missing HTTPS policy is an assertion failure,
        # never an accidental TypeError in an obsolete request double.
        calls.append((url, dict(headers), timeout, verify, allow_redirects, stream))
        outcome = next(sequence)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'header', {'User-Agent': 'AGENT_A'})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': 'AGENT_B'})
    monkeypatch.setattr(gathering, 'time',
                        SimpleNamespace(sleep=sleeps.append, perf_counter=lambda: 0.0,
                                        time=lambda: 1709380800.0))
    return calls, sleeps


def query(gathering, **kwargs):
    return gathering.moex_query('FIXTURE_SHARE', 'Акции', '2024-02-29', '2024-03-01',
                                24, **kwargs)


def test_https_verification_and_no_redirects_keep_literal_rows(gathering, monkeypatch):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    result = query(gathering)
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True),
                     (HTTPS_QUERY.replace('start=0', 'start=2'), {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True)]
    assert sleeps == [3, 3]
    assert result.columns.tolist() == CANDLE_COLUMNS
    assert result.values.tolist() == CANDLE_ROWS
    assert result.index.tolist() == [0, 1]
    assert list(zip(result.ticker, [24, 24], result.begin)) == [
        ('FIXTURE_SHARE', 24, '2024-02-29 00:00:00'),
        ('FIXTURE_SHARE', 24, '2024-03-01 00:00:00'),
    ]


@pytest.mark.parametrize('console_level,visible', [
    ('DEBUG', list(range(14))),
    ('INFO', [0, 3, 5, 8, 9, 10, 13]),
    ('WARNING', [3, 13]),
    ('ERROR', [13]),
])
def test_review_console_uses_real_flog_levels_without_duplicates(
        gathering, monkeypatch, tmp_path, capsys, console_level, visible):
    # No event_log fixture: exercise the real queue, redactor, writer and health.
    import run_logging
    markers = ('A2_REAL_INSTRUMENT_CANARY', 'A2_REAL_CONTEXT_CANARY',
               'A2_REAL_UA_A_CANARY', 'A2_REAL_UA_B_CANARY',
               'A2_REAL_BODY_CANARY', 'A2_REAL_EXCEPTION_CANARY')
    primary = requests.exceptions.SSLError(markers[-1])
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [response(503, markers[4]), response(200, CANDLE_CSV), response(200, EMPTY_CSV), primary, response(200, EMPTY_CSV)])
    monkeypatch.setattr(gathering, 'header', {'User-Agent': markers[2]})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': markers[3]})
    context = run_logging.configure_logging(tmp_path, run_id='a2-console-' + uuid.uuid4().hex,
        environment='offline', producer_sha='a6e4a07', sensitive_values=markers,
        config={'console_level': console_level})
    context['file'] = markers[1]
    try:
        result = gathering.moex_query(markers[0], 'Акции', '2024-02-29', '2024-03-01', 24,
                                     logging_context=context)
        assert result.columns.tolist() == CANDLE_COLUMNS
        assert result.values.tolist() == [row[:-1] + [markers[0]] for row in CANDLE_ROWS]
        assert result.index.tolist() == [0, 1]
        with pytest.raises(requests.exceptions.SSLError) as captured:
            gathering.moex_query(markers[0], 'Акции', '2024-02-29', '2024-03-01', 24,
                                 logging_context=context)
        assert captured.value is primary and primary.args == original_args
        assert len(calls) == 4 and sleeps == [3, 3, 3]
        assert run_logging.flush_logging(context)['confirmed'] is True
        assert run_logging.check_logging_health(context)['healthy'] is True
        persisted = (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8')
        events = [json.loads(line) for line in persisted.splitlines()]
        assert [(item['event'], item['level']) for item in events] == [
            ('a2_query_started', 'INFO'), ('a3_page_started', 'DEBUG'),
            ('a2_query_attempt_started', 'DEBUG'), ('a2_query_retry', 'WARNING'),
            ('a2_query_attempt_started', 'DEBUG'), ('a3_page_returned', 'INFO'),
            ('a3_page_started', 'DEBUG'), ('a2_query_attempt_started', 'DEBUG'),
            ('a3_page_terminal', 'INFO'), ('a2_query_returned', 'INFO'),
            ('a2_query_started', 'INFO'), ('a3_page_started', 'DEBUG'),
            ('a2_query_attempt_started', 'DEBUG'), ('a2_query_failed', 'ERROR'),
        ]
        output = capsys.readouterr()
        assert output.out == ''
        assert output.err.splitlines() == [
            events[index]['level'] + ' ' + events[index]['message'] for index in visible]
        combined = output.out + output.err + persisted
        assert all(marker not in combined for marker in markers)
        assert all(value not in combined for value in ('http://', 'https://', 'User-Agent'))
        assert all(len(line) <= 256 for line in output.err.splitlines())
    finally:
        final = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert final['delivery_outcome'] == 'COMPLETE'
    # Critical lifecycle console delivery is separate from query delivery.
    finalized = capsys.readouterr()
    assert all(marker not in finalized.out + finalized.err for marker in markers)


@pytest.mark.parametrize('primary', [RuntimeError('A2_SUCCESS_SLEEP'), KeyboardInterrupt('A2_SUCCESS_SLEEP')])
def test_review_interrupted_success_sleep_never_reports_returned(
        gathering, monkeypatch, event_log, primary):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])

    def sleep(seconds):
        sleeps.append(seconds)
        raise primary

    gathering.time.sleep = sleep
    with pytest.raises(type(primary)) as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value is primary and captured.value.args == ('A2_SUCCESS_SLEEP',)
    assert len(calls) == 1 and sleeps == [3]
    assert [item['event'] for item in event_log.events] == [
        'a2_query_started', 'a3_page_started', 'a2_query_attempt_started']
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


def test_review_returned_duration_includes_retry_and_completed_success_sleep(
        gathering, monkeypatch, event_log):
    calls, sleeps = transport(gathering, monkeypatch, [response(503), response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    elapsed = [0.0]
    original_get = gathering.requests.get

    def get(*args, **kwargs):
        elapsed[0] += 0.25
        return original_get(*args, **kwargs)

    def sleep(seconds):
        assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)
        sleeps.append(seconds)
        elapsed[0] += seconds

    monkeypatch.setattr(gathering.requests, 'get', get)
    gathering.time.perf_counter = lambda: elapsed[0]
    gathering.time.sleep = sleep
    result = query(gathering, logging_context=event_log.context)
    assert result.values.tolist() == CANDLE_ROWS and result.index.tolist() == [0, 1]
    assert len(calls) == 3 and sleeps == [3, 3, 3]
    returned = event_log.events[-1]
    assert returned['event'] == 'a2_query_returned'
    assert returned['duration_ms'] == 9750.0


def test_http_exhaustion_raises_native_last_failure(gathering, monkeypatch):
    responses = [response(503) for _ in range(5)]
    calls, sleeps = transport(gathering, monkeypatch, [*responses, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as captured:
        query(gathering)
    assert captured.value.response is responses[-1]
    assert len(calls) == 5
    assert sleeps == [3, 6, 12, 24]


def test_tls_primary_is_immediate_without_sleep(gathering, monkeypatch):
    primary = requests.exceptions.SSLError('A2_NATIVE_TLS_PRIMARY')
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*[primary] * 5, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.SSLError) as captured:
        query(gathering)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 1 and sleeps == []


def test_second_annual_window_primary_prevents_partial_history(gathering, candle_frame,
                                                              monkeypatch):
    primary = requests.exceptions.Timeout('A2_SECOND_WINDOW_PRIMARY')
    original_args = primary.args
    calls = []

    def window(*args, logging_context=None):
        calls.append(args)
        if len(calls) == 1:
            return candle_frame.copy(deep=True)
        raise primary

    monkeypatch.setattr(gathering, 'moex_query', window)
    with pytest.raises(requests.exceptions.Timeout) as captured:
        gathering.moex('SBER', 'Акции', 4, 24)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 2
    assert gathering.exception_list == ['SBER']


@pytest.mark.parametrize('status', [300, 301, 302, 307, 308, 399])
@pytest.mark.parametrize('destination', [
    'http://iss.moex.com/A2_LOCATION_CANARY',
    'https://iss.moex.com/A2_LOCATION_CANARY',
    'https://other.invalid/A2_LOCATION_CANARY',
])
def test_redirect_response_never_requests_location(gathering, monkeypatch, status, destination):
    rejected = response(status)
    rejected.headers['Location'] = destination
    calls, sleeps = transport(gathering, monkeypatch, [*[rejected] * 5, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.TooManyRedirects) as captured:
        query(gathering)
    assert captured.value.args == ('A2_ISS_REDIRECT_REJECTED',)
    assert captured.value.response is rejected
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True)]
    assert sleeps == []


def test_native_redirect_primary_is_immediate(gathering, monkeypatch):
    primary = requests.exceptions.TooManyRedirects('A2_NATIVE_REDIRECT_PRIMARY')
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*[primary] * 5, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.TooManyRedirects) as captured:
        query(gathering)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 1 and sleeps == []


@pytest.mark.parametrize('failure', ['http', 'timeout'])
def test_transient_retry_retains_rotation_delays_and_literal_result(gathering, monkeypatch, failure):
    first = response(503) if failure == 'http' else requests.exceptions.Timeout('A2_TRANSIENT')
    calls, sleeps = transport(gathering, monkeypatch, [first, response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    result = query(gathering)
    assert calls == [(HTTPS_QUERY, {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True),
                     (HTTPS_QUERY, {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True),
                     (HTTPS_QUERY.replace('start=0', 'start=2'), {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'identity'}, (5, 30), True, False, True)]
    assert sleeps == [3, 3, 3]
    assert result.columns.tolist() == CANDLE_COLUMNS
    assert result.values.tolist() == CANDLE_ROWS
    assert result.index.tolist() == [0, 1]


def test_timeout_exhaustion_retains_last_native_object(gathering, monkeypatch):
    failures = [requests.exceptions.Timeout('A2_TIMEOUT_' + str(index)) for index in range(5)]
    calls, sleeps = transport(gathering, monkeypatch, [*failures, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.Timeout) as captured:
        query(gathering)
    assert captured.value is failures[-1]
    assert captured.value.args == ('A2_TIMEOUT_4',)
    assert len(calls) == 5 and sleeps == [3, 6, 12, 24]


@pytest.mark.parametrize('last', ['http', 'timeout'])
def test_last_attempt_type_takes_precedence_over_earlier_failure(gathering, monkeypatch, last):
    timeout = requests.exceptions.Timeout('A2_MIXED_TIMEOUT')
    rejected = response(503)
    outcomes = [timeout, rejected] if last == 'http' else [rejected, timeout]
    calls, sleeps = transport(gathering, monkeypatch, [*outcomes, response(200, EMPTY_CSV)])
    exception_type = requests.exceptions.HTTPError if last == 'http' else requests.exceptions.Timeout
    with pytest.raises(exception_type) as captured:
        query(gathering, max_retries=2)
    if last == 'http':
        assert captured.value.response is rejected
        assert captured.value.args == ('A2_ISS_HTTP_STATUS',)
    else:
        assert captured.value is timeout and timeout.args == ('A2_MIXED_TIMEOUT',)
    assert len(calls) == 2 and sleeps == [3]


@pytest.mark.parametrize('empty', [False, True])
def test_closed_returned_event_reports_unvalidated_rows(gathering, monkeypatch, event_log, empty):
    content = CANDLE_CSV.splitlines(keepends=True)[:2] if empty else [CANDLE_CSV]
    calls, sleeps = transport(gathering, monkeypatch, [response(200, ''.join(content)), response(200, EMPTY_CSV)])
    result = query(gathering, logging_context=event_log.context)
    assert len(calls) == (1 if empty else 2) and sleeps == [3] * (1 if empty else 2)
    if empty:
        assert result.empty and result.columns.tolist() == []
    else:
        assert result.columns.tolist() == CANDLE_COLUMNS and result.values.tolist() == CANDLE_ROWS
    events = event_log.events
    assert [(item['event'], item['level'], item['outcome'], item['retries']) for item in events] == [
        ('a2_query_started', 'INFO', 'QUERY_STARTED', {'attempt': 0, 'limit': 5000}),
        ('a3_page_started', 'DEBUG', 'PAGE_STARTED', {'attempt': 0, 'limit': 5}),
        ('a2_query_attempt_started', 'DEBUG', 'REQUEST_ADMISSION_STARTED', {'attempt': 0, 'limit': 5}),
    ] + ([] if empty else [
        ('a3_page_returned', 'INFO', 'PAGE_STAGED', {'attempt': 1, 'limit': 5}),
        ('a3_page_started', 'DEBUG', 'PAGE_STARTED', {'attempt': 0, 'limit': 5}),
        ('a2_query_attempt_started', 'DEBUG', 'REQUEST_ADMISSION_STARTED', {'attempt': 0, 'limit': 5}),
    ]) + [
        ('a3_page_terminal', 'INFO', 'OFFSET_TERMINAL_EMPTY', {'attempt': 1, 'limit': 5}),
        ('a2_query_returned', 'INFO',
         'QUERY_EMPTY_UNVALIDATED' if empty else 'QUERY_RETURNED_UNVALIDATED',
         {'attempt': 1 if empty else 2, 'limit': 5000}),
    ]
    assert all(item['stage'] == 'data_gathering' and item['fields'] == {} and item['error'] is None
               for item in events)
    assert events[-1]['counts'] == {
        'rows_received': 0 if empty else 2, 'rows_accepted': None, 'rows_quarantined': None,
        'pages': 1 if empty else 2, 'instruments': None,
        'null_reasons': {'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED',
                         'instruments': 'NOT_ASSESSED'},
    }


@pytest.mark.parametrize('kind,code,category,outcome', [
    ('tls', 'A2_ISS_TLS_FAILED', 'SOURCE', 'TLS_FAILED'),
    ('redirect', 'A2_ISS_REDIRECT_REJECTED', 'SOURCE', 'REDIRECT_REJECTED'),
    ('http', 'A2_ISS_HTTP_STATUS', 'SOURCE', 'RETRY_EXHAUSTED'),
    ('timeout', 'A2_ISS_TIMEOUT', 'TIMEOUT', 'RETRY_EXHAUSTED'),
    ('request', 'A2_ISS_REQUEST_ERROR', 'SOURCE', 'REQUEST_FAILED'),
    ('parse', 'A2_ISS_PARSE_ERROR', 'SOURCE', 'RESPONSE_REJECTED'),
])
def test_closed_failure_event_uses_exact_error_contract(gathering, monkeypatch, event_log,
                                                       kind, code, category, outcome):
    exceptions = {'tls': requests.exceptions.SSLError('A2_FAILURE'),
                  'redirect': requests.exceptions.TooManyRedirects('A2_FAILURE'),
                  'timeout': requests.exceptions.Timeout('A2_FAILURE'),
                  'request': requests.exceptions.ConnectionError('A2_FAILURE'),
                  'parse': gathering.pd.errors.ParserError('A2_FAILURE')}
    first = response(503) if kind == 'http' else exceptions[kind]
    if kind == 'parse':
        def parse(*args, **kwargs):
            raise first
        monkeypatch.setattr(gathering.pd, 'read_csv', parse)
        outcomes = [response(200, CANDLE_CSV)] * 5
    else:
        outcomes = [first] * 5
    calls, sleeps = transport(gathering, monkeypatch, [*outcomes, response(200, EMPTY_CSV)])
    expected_type = requests.exceptions.HTTPError if kind == 'http' else type(first)
    with pytest.raises(expected_type) as captured:
        query(gathering, logging_context=event_log.context)
    if kind != 'http':
        assert captured.value is first
    event = event_log.events[-1]
    assert (event['event'], event['level'], event['outcome']) == ('a2_query_failed', 'ERROR', outcome)
    assert event['stage'] == 'data_gathering' and event['fields'] == {}
    expected_attempt = 1 if kind in ('tls', 'redirect', 'request', 'parse') else 5
    assert event['retries'] == {'attempt': expected_attempt, 'limit': 5000}
    error = event['error']
    assert (error['code'], error['category'], error['stage']) == (code, category, 'data_gathering')
    assert error['final'] is True and error['retryable'] is False
    assert error['endpoint'] == 'ISS_CANDLES' and error['attempt'] == expected_attempt
    assert set(error) == {'category', 'code', 'stage', 'retryable', 'final', 'message', 'null_reasons',
                          'run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval', 'page', 'file',
                          'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type'}
    assert len(calls) == expected_attempt
    assert sleeps == ([] if expected_attempt == 1 else [3, 6, 12, 24])
    retries = [item for item in event_log.events if item['event'] == 'a2_query_retry']
    assert len(retries) == expected_attempt - 1
    assert [item['retries'] for item in retries] == [
        {'attempt': attempt, 'limit': 5} for attempt in range(1, expected_attempt)]
    assert all(item['level'] == 'WARNING' and item['outcome'] == 'RETRY_PENDING'
               and item['error']['code'] == code and item['error']['category'] == category
               and item['error']['final'] is False and item['error']['retryable'] is True
               and item['fields'] == {} for item in retries)


@pytest.mark.parametrize('parameter,value', [
    ('max_retries', 0), ('max_retries', -1), ('max_retries', 1.0),
    ('max_retries', True), ('max_retries', None), ('max_retries', '5'),
    ('retry_sleep_start', -1), ('retry_sleep_start', float('nan')),
    ('retry_sleep_start', float('inf')), ('retry_sleep_start', float('-inf')),
    ('retry_sleep_start', True), ('retry_sleep_start', '3'),
])
def test_invalid_retry_arguments_fail_before_network(gathering, monkeypatch, event_log,
                                                     parameter, value):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, EMPTY_CSV)])
    with pytest.raises(ValueError, match='^A2_RETRY_ARGUMENTS$') as captured:
        query(gathering, logging_context=event_log.context, **{parameter: value})
    assert captured.value.args == ('A2_RETRY_ARGUMENTS',)
    assert calls == [] and sleeps == []
    assert len(event_log.events) == 1
    event = event_log.events[0]
    assert (event['event'], event['level'], event['outcome']) == ('a2_query_failed', 'ERROR', 'CONFIG_REJECTED')
    assert event['retries'] is None and event['null_reasons']['retries'] == 'INVALID_CONFIGURATION'
    assert (event['error']['code'], event['error']['category'], event['error']['attempt']) == (
        'A2_RETRY_ARGUMENTS', 'CONFIG', 0)


def test_large_attempt_limit_is_rejected_as_a_resource_cap_before_network(gathering, monkeypatch):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    with pytest.raises(ValueError) as captured:
        query(gathering, max_retries=1000000, retry_sleep_start=0)
    assert captured.value.args == ('A2_RETRY_ARGUMENTS',)
    assert calls == [] and sleeps == []
    # Finite-policy refusal does not alter the successful literal output oracle.
    result = query(gathering, max_retries=5, retry_sleep_start=0)
    assert len(calls) == 2 and sleeps == [3, 3] and result.values.tolist() == CANDLE_ROWS


def test_no_context_imports_or_calls_no_logger(gathering, monkeypatch):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    original_import = builtins.__import__
    logger_imports = []

    def importing(name, *args, **kwargs):
        if name == 'run_logging':
            logger_imports.append(name)
            raise AssertionError('A2 no-context imported logger')
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', importing)
    result = query(gathering, logging_context=None)
    assert logger_imports == [] and len(calls) == 2 and sleeps == [3, 3]
    assert result.values.tolist() == CANDLE_ROWS


def test_no_context_safe_fallback_has_no_attempt_debug_flood(gathering, monkeypatch, capsys):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    result = query(gathering, logging_context=None)
    output = capsys.readouterr()
    lines = output.out.splitlines()
    assert len(lines) == 4 and output.err == ''
    assert lines[0].startswith('Свечи QUERY_STARTED:')
    assert lines[3].startswith('Свечи QUERY_RETURNED_UNVALIDATED:')
    assert 'REQUEST_ADMISSION_STARTED' not in output.out
    assert all(len(line) <= 256 for line in lines)
    assert len(calls) == 2 and sleeps == [3, 3] and result.values.tolist() == CANDLE_ROWS


@pytest.mark.parametrize('with_context', [False, True])
@pytest.mark.parametrize('kind', ['tls', 'redirect', 'exhaustion', 'parse', 'rotation'])
def test_console_and_logger_canaries_are_safe_together(gathering, monkeypatch, event_log,
                                                      capsys, with_context, kind):
    markers = ('A2_EXCEPTION_CANARY', 'A2_INSTRUMENT_CANARY', 'A2_START_CANARY',
               'A2_END_CANARY', 'A2_INTERVAL_CANARY', 'A2_OLD_UA_CANARY',
               'A2_NEW_UA_CANARY', 'A2_LOCATION_CANARY', 'A2_BODY_CANARY')
    event_log.context['_secrets'] += markers
    primary = (requests.exceptions.SSLError(markers[0]) if kind == 'tls'
               else gathering.pd.errors.ParserError(markers[0]) if kind == 'parse'
               else requests.exceptions.Timeout(markers[0]))
    original_args = primary.args
    rejected = response(302, markers[-1])
    rejected.headers['Location'] = 'http://user:password@other.invalid/' + markers[-2]
    rejected.url = 'https://user:password@other.invalid/' + markers[0]
    if kind == 'redirect':
        outcomes = [rejected] * 5
    elif kind == 'parse':
        def parse(*args, **kwargs):
            raise primary
        monkeypatch.setattr(gathering.pd, 'read_csv', parse)
        # Парсер получает допустимый envelope; body canary остаётся в header,
        # а primary canary — в исходном pandas exception без изменения args.
        parsed_response = response(200, CANDLE_CSV)
        parsed_response.headers['X-A2-Canary'] = markers[-1]
        outcomes = [parsed_response] * 5
    elif kind == 'rotation':
        outcomes = [primary, response(200, CANDLE_CSV)]
    else:
        outcomes = [primary] * 5
    calls, sleeps = transport(gathering, monkeypatch, [*outcomes, response(200, EMPTY_CSV)])
    monkeypatch.setattr(gathering, 'header', {'User-Agent': markers[5]})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': markers[6]})
    raw_events = []
    original_emit = event_log.logger.emit_event

    def emit(context, level, event, message, fields):
        raw_events.append((level, event, message, fields))
        return original_emit(context, level, event, message, fields)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    context = event_log.context if with_context else None

    def invoke():
        return gathering.moex_query(markers[1], 'Акции', markers[2], markers[3], markers[4],
                                    logging_context=context)

    if kind == 'rotation':
        result = invoke()
        assert result['ticker'].tolist() == [markers[1], markers[1]]
    else:
        exception_type = requests.exceptions.TooManyRedirects if kind == 'redirect' else type(primary)
        with pytest.raises(exception_type) as captured:
            invoke()
        if kind != 'redirect':
            assert captured.value is primary and primary.args == original_args
    output = capsys.readouterr()
    # Typed instrument/context fields enter the existing F-LOG redactor. Raw
    # human messages and error text must already be safe before that boundary.
    raw_diagnostics = [(level, event, message, (fields.get('error') or {}).get('message'),
                        (fields.get('error') or {}).get('exception_type'))
                       for level, event, message, fields in raw_events]
    serialized = json.dumps(raw_diagnostics, ensure_ascii=False) + json.dumps(event_log.events, ensure_ascii=False)
    combined = output.out + output.err + serialized
    assert all(marker not in combined for marker in markers)
    assert all(value not in combined for value in ('http://', 'https://', 'user:password', 'User-Agent'))
    assert all(len(line) <= 256 for line in (output.out + output.err).splitlines())
    assert all(len(message) <= 256 and len((fields.get('error') or {}).get('message', '')) <= 256
               for _, _, message, fields in raw_events)
    assert bool(raw_events) is with_context and bool(event_log.events) is with_context
    expected_attempts = 1 if kind in ('tls', 'redirect', 'parse') else 3 if kind == 'rotation' else 5
    assert len(calls) == expected_attempts
    assert sleeps == ([] if expected_attempts == 1 else [3, 3, 3] if kind == 'rotation' else [3, 6, 12, 24])
    if kind in ('rotation', 'exhaustion'):
        if with_context:
            assert output.out == ''
            assert any('HEADER_ROTATED' in item['message'] for item in event_log.events)
        else:
            assert 'HEADER_ROTATED' in output.out
    elif kind == 'parse':
        assert 'HEADER_ROTATED' not in output.out
        assert not any('HEADER_ROTATED' in item['message'] for item in event_log.events)


@pytest.mark.parametrize('failure', ['throw', 'rejected', 'unconfirmed', 'unhealthy'])
def test_primary_precedes_failed_error_diagnostics(gathering, monkeypatch, event_log,
                                                  capsys, failure):
    primary = requests.exceptions.SSLError('A1_SYNTHETIC_CANARY')
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*[primary] * 5, response(200, EMPTY_CSV)])
    original_emit = event_log.logger.emit_event

    def emit(context, level, *args):
        if level == 'ERROR':
            if failure == 'throw':
                raise OSError('A1_SYNTHETIC_CANARY')
            event_log.accepted = failure != 'rejected'
            event_log.confirmed = failure != 'unconfirmed'
            event_log.healthy = failure != 'unhealthy'
        return original_emit(context, level, *args)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    with pytest.raises(requests.exceptions.SSLError) as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 1 and sleeps == []
    assert primary.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    output = capsys.readouterr()
    assert output.err == 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'
    assert 'A1_SYNTHETIC_CANARY' not in output.out + output.err + json.dumps(event_log.events)


@pytest.mark.parametrize('failure', ['throw', 'rejected', 'unhealthy'])
def test_diagnostics_without_primary_stop_before_network(gathering, monkeypatch, event_log,
                                                        failure):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, EMPTY_CSV)])
    event_log.fail = failure == 'throw'
    event_log.accepted = failure != 'rejected'
    event_log.healthy = failure != 'unhealthy'
    with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$') as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value.args == ('LOGGING_INCOMPLETE',)
    assert captured.value.__cause__ is None and captured.value.__suppress_context__
    assert calls == [] and sleeps == []


@pytest.mark.parametrize('failure', ['throw', 'rejected', 'unhealthy'])
@pytest.mark.parametrize('point', ['retry', 'next_attempt'])
def test_failure_after_transient_primary_stops_before_next_request(gathering, monkeypatch,
                                                                 event_log, failure, point, capsys):
    primary = requests.exceptions.Timeout('A1_SYNTHETIC_CANARY')
    calls, sleeps = transport(gathering, monkeypatch, [primary, response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    original_emit = event_log.logger.emit_event

    def emit(context, level, event, message, fields):
        targeted = (event == 'a2_query_retry' if point == 'retry' else
                    event == 'a2_query_attempt_started' and fields['retries']['attempt'] == 1)
        if targeted:
            if failure == 'throw':
                raise OSError('A1_SYNTHETIC_CANARY')
            event_log.accepted = failure != 'rejected'
            event_log.healthy = failure != 'unhealthy'
        return original_emit(context, level, event, message, fields)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    with pytest.raises(requests.exceptions.Timeout) as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value is primary and primary.args == ('A1_SYNTHETIC_CANARY',)
    assert len(calls) == 1 and sleeps == ([] if point == 'retry' else [3])
    assert primary.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    assert capsys.readouterr().err == 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'


@pytest.mark.parametrize('with_context', [False, True])
def test_annual_windows_forward_context_identity(gathering, candle_frame, monkeypatch,
                                                event_log, with_context):
    context = event_log.context if with_context else None
    calls = []

    def window(*args, logging_context=None):
        calls.append((args, logging_context))
        return candle_frame.copy(deep=True)

    monkeypatch.setattr(gathering, 'moex_query', window)
    result = gathering.moex('SBER', 'Акции', 3, 24, logging_context=context)
    assert len(calls) == 2 and all(item[1] is context for item in calls)
    assert [item[0] for item in calls] == [
        ('SBER', 'Акции', '2023-03-03', '2024-03-02', 24),
        ('SBER', 'Акции', '2022-03-03', '2023-03-03', 24),
    ]
    assert result.values.tolist() == candle_frame.values.tolist() * 2
    assert result.index.tolist() == [0, 0]


@pytest.mark.parametrize('operation', ['reload', 'update'])
@pytest.mark.parametrize('with_context', [False, True])
def test_later_ticker_failure_preserves_local_previous_and_has_no_writers(
        gathering, catalogue, snapshot, candle_frame, fixture_root, exact_reads,
        writers, save_dataset, monkeypatch, event_log, operation, with_context):
    root_path, root = fixture_root
    target = root_path / 'datasets/fixture_previous.csv'
    target.parent.mkdir(parents=True)
    previous = {}
    for suffix in ('csv', 'xlsx'):
        path = target.with_suffix('.' + suffix)
        path.write_bytes(('A2_PREVIOUS_' + suffix).encode('utf-8'))
        previous[path] = (path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest())
    if operation == 'update':
        content = save_dataset(target, candle_frame)
        previous[target] = (content, hashlib.sha256(content).hexdigest())
        exact_reads.declared[target] = candle_frame
    primary = requests.exceptions.Timeout('A1_SYNTHETIC_CANARY')
    original_args = primary.args
    calls = []
    context = event_log.context if with_context else None

    def candles(*args, logging_context=None):
        calls.append(args)
        assert logging_context is context
        if len(calls) == 1:
            result = candle_frame.copy(deep=True)
            result['ticker'] = args[0]
            return result
        raise primary

    monkeypatch.setattr(gathering, 'moex_query', candles)
    with pytest.raises(requests.exceptions.Timeout) as captured:
        if operation == 'reload':
            gathering.full_reload(catalogue, 24, 10, 'fixture_previous', 'часа', root, snapshot,
                                  logging_context=context)
        else:
            gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture_previous', 'word': 'часа'}],
                                  root, catalogue, snapshot, logging_context=context)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 2 and writers == []
    assert all((path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest()) == value
               for path, value in previous.items())
    assert not any(item['event'] in ('a1_full_reload_returned', 'a1_data_update_returned')
                   for item in event_log.events)


@pytest.mark.parametrize('with_context', [False, True])
@pytest.mark.parametrize('has_primary', [False, True])
def test_broken_console_and_stderr_preserve_failure_precedence(gathering, monkeypatch,
                                                              event_log, capsys,
                                                              with_context, has_primary):
    primary = requests.exceptions.SSLError('A1_SYNTHETIC_CANARY')
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*([primary] * 5 if has_primary else []), response(200, EMPTY_CSV)])
    original_print = builtins.print
    print_calls = []

    def printing(*args, **kwargs):
        print_calls.append((args, kwargs.get('file')))
        targeted = ('TLS_FAILED' in args[0] if has_primary else 'QUERY_STARTED' in args[0])
        if targeted or kwargs.get('file') is sys.stderr:
            raise OSError('A2_CONSOLE_FAILURE_CANARY')
        return original_print(*args, **kwargs)

    monkeypatch.setattr(builtins, 'print', printing)
    if with_context:
        # F-LOG owns context console delivery. Model its failure at emit, while
        # keeping the real-writer level/duplicate oracle in the separate test.
        original_emit = event_log.logger.emit_event

        def emit(context, level, event, message, fields):
            targeted = (event == 'a2_query_failed' if has_primary else event == 'a2_query_started')
            if targeted:
                raise OSError('A2_CONSOLE_FAILURE_CANARY')
            return original_emit(context, level, event, message, fields)

        monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    context = event_log.context if with_context else None
    expected_type = requests.exceptions.SSLError if has_primary else RuntimeError
    with pytest.raises(expected_type) as captured:
        query(gathering, logging_context=context)
    if has_primary:
        assert captured.value is primary and primary.args == original_args
        assert primary.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
        assert len(calls) == 1
        assert any(file is sys.stderr and args == ('A2_LOGGING_FAILURE: LOGGING_INCOMPLETE',)
                   for args, file in print_calls)
    else:
        assert captured.value.args == ('LOGGING_INCOMPLETE',)
        assert captured.value.__cause__ is None and captured.value.__suppress_context__
        assert calls == []
    assert sleeps == []
    output = capsys.readouterr()
    serialized = output.out + output.err + json.dumps(event_log.events, ensure_ascii=False)
    assert 'A1_SYNTHETIC_CANARY' not in serialized and 'A2_CONSOLE_FAILURE_CANARY' not in serialized
    assert not any(item['event'] in ('a2_query_returned', 'a2_query_retry') for item in event_log.events)
    if not with_context:
        assert event_log.events == []


@pytest.mark.parametrize('has_primary', [False, True])
def test_throwing_health_check_preserves_failure_precedence(gathering, monkeypatch,
                                                          event_log, capsys, has_primary):
    primary = requests.exceptions.SSLError('A1_SYNTHETIC_CANARY')
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*([primary] * 5 if has_primary else []), response(200, EMPTY_CSV)])
    original_emit = event_log.logger.emit_event
    last_level = []

    def emit(context, level, *args):
        last_level.append(level)
        return original_emit(context, level, *args)

    def health(context):
        if not has_primary or last_level[-1] == 'ERROR':
            raise OSError('A2_HEALTH_FAILURE_CANARY')
        return {'healthy': True}

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(event_log.logger, 'check_logging_health', health)
    expected_type = requests.exceptions.SSLError if has_primary else RuntimeError
    with pytest.raises(expected_type) as captured:
        query(gathering, logging_context=event_log.context)
    if has_primary:
        assert captured.value is primary and primary.args == original_args
        assert primary.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
        assert len(calls) == 1
        assert event_log.events[-1]['event'] == 'a2_query_failed'
    else:
        assert captured.value.args == ('LOGGING_INCOMPLETE',)
        assert captured.value.__cause__ is None and captured.value.__suppress_context__
        assert calls == []
    assert sleeps == []
    output = capsys.readouterr()
    assert output.err == ('A2_LOGGING_FAILURE: LOGGING_INCOMPLETE\n' if has_primary else '')
    serialized = output.out + output.err + json.dumps(event_log.events, ensure_ascii=False)
    assert 'A1_SYNTHETIC_CANARY' not in serialized and 'A2_HEALTH_FAILURE_CANARY' not in serialized


@pytest.mark.parametrize('failure', ['throw', 'rejected', 'unhealthy', 'health_throw'])
def test_returned_diagnostic_failure_after_parse_never_retries_http(gathering, monkeypatch,
                                                                  event_log, failure):
    calls, sleeps = transport(gathering, monkeypatch, [response(200, CANDLE_CSV), response(200, EMPTY_CSV)])
    original_emit = event_log.logger.emit_event
    original_health = event_log.logger.check_logging_health
    returned = []

    def emit(context, level, event, *args):
        if event == 'a2_query_returned':
            returned.append(event)
            if failure == 'throw':
                raise OSError('A1_SYNTHETIC_CANARY')
            event_log.accepted = failure != 'rejected'
            event_log.healthy = failure != 'unhealthy'
        return original_emit(context, level, event, *args)

    def health(context):
        if returned and failure == 'health_throw':
            raise OSError('A1_SYNTHETIC_CANARY')
        return original_health(context)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(event_log.logger, 'check_logging_health', health)
    with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$') as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value.args == ('LOGGING_INCOMPLETE',)
    assert captured.value.__cause__ is None and captured.value.__suppress_context__
    assert returned == ['a2_query_returned']
    assert len(calls) == 2 and sleeps == [3, 3]
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_failed') for item in event_log.events)


@pytest.mark.parametrize('with_context', [False, True])
def test_real_reload_console_and_a2_a1_logs_redact_registered_canaries(
        gathering, catalogue, snapshot, fixture_root, writers, monkeypatch,
        event_log, capsys, with_context):
    markers = ('A2_CALLER_INSTRUMENT_CANARY', 'A2_CALLER_CONTEXT_CANARY',
               'A2_CALLER_EXCEPTION_CANARY', 'A2_CALLER_HEADER_CANARY')
    event_log.context['_secrets'] += markers
    event_log.context.update(snapshot_id=markers[1], dataset_id=markers[1],
                             artifact_id=markers[1], target_id=markers[1])
    source = catalogue.copy(deep=True)
    source.loc[0, 'TRADE_CODE'] = markers[0]
    dates = snapshot.copy(deep=True)
    dates.loc[0, 'TRADE_CODE'] = markers[0]
    raw_url = 'https://user:password@other.invalid/' + markers[2] + '?secret=' + markers[1]
    primary = requests.exceptions.SSLError(markers[2] + ' ' + raw_url)
    original_args = primary.args
    calls, sleeps = transport(gathering, monkeypatch, [*[primary] * 5, response(200, EMPTY_CSV)])
    monkeypatch.setattr(gathering, 'header', {'User-Agent': markers[3]})
    _, root = fixture_root
    context = event_log.context if with_context else None
    with pytest.raises(requests.exceptions.SSLError) as captured:
        gathering.full_reload(source, 24, 10, 'fixture', 'часа', root, dates,
                              logging_context=context)
    assert captured.value is primary and primary.args == original_args
    assert len(calls) == 1 and sleeps == [] and writers == []
    assert calls[0][0].startswith('https://iss.moex.com/')
    assert calls[0][3:] == (True, False, True)
    assert not getattr(primary, '__notes__', [])
    output = capsys.readouterr()
    serialized = json.dumps(event_log.events, ensure_ascii=False)
    combined = output.out + output.err + serialized
    assert all(marker not in combined for marker in markers)
    assert raw_url not in combined and output.err == ''
    assert all(len(line) <= 256 for line in output.out.splitlines())
    if with_context:
        assert [item['event'] for item in event_log.events if item['level'] == 'ERROR'] == [
            'a2_query_failed', 'a1_failed']
    else:
        assert event_log.events == []


def test_allowed_correlation_fields_mirror_in_closed_event_and_error(gathering, monkeypatch,
                                                                   event_log):
    linkage = {'snapshot_id': 'fixture_snapshot', 'dataset_id': 'fixture_dataset',
               'file': 'fixture.csv', 'artifact_id': 'fixture_artifact',
               'target_id': 'fixture_target', 'page': 2}
    event_log.context.update(linkage)
    primary = requests.exceptions.SSLError('A2_PRIMARY')
    calls, sleeps = transport(gathering, monkeypatch, [*[primary] * 5, response(200, EMPTY_CSV)])
    with pytest.raises(requests.exceptions.SSLError) as captured:
        query(gathering, logging_context=event_log.context)
    assert captured.value is primary and primary.args == ('A2_PRIMARY',)
    assert len(calls) == 1 and sleeps == []
    assert all({key: item[key] for key in linkage} == linkage and item['fields'] == {}
               for item in event_log.events)
    final = event_log.events[-1]
    assert {key: final['error'][key] for key in linkage} == linkage
    assert final['error']['run_id'] == final['run_id'] == 'a1-fixture'
    assert final['error']['instrument'] == final['instrument'] == 'FIXTURE_SHARE'
    assert final['error']['interval'] == final['interval'] == 24
