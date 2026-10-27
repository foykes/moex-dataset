"""Offline oracles for the accepted A2-03 finite consumer policy."""

import datetime
import errno
import io
import json
import socket
import time as native_time
from http.client import IncompleteRead
from types import SimpleNamespace

import pytest
import requests
from urllib3._collections import HTTPHeaderDict
from urllib3.exceptions import ProtocolError, ReadTimeoutError


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
BODY_CAP = 8 * 1024 * 1024
FIXED_WALL = datetime.datetime(2024, 3, 2, 12, tzinfo=datetime.timezone.utc).timestamp()


class RawBody:
    """Finite native-like raw stream with original header occurrences."""

    def __init__(self, body, headers=(), read_error=None, close_error=None):
        self.buffer = io.BytesIO(body)
        self.headers = HTTPHeaderDict()
        for name, value in headers:
            self.headers.add(name, value)
        self.read_error = read_error
        self.close_error = close_error
        self.reads = []
        self.close_calls = 0
        self.release_calls = 0
        self.after_read = None
        self.after_close = None

    def read(self, amt=None, decode_content=None, **kwargs):
        self.reads.append((amt, decode_content))
        if self.read_error is not None:
            raise self.read_error
        result = self.buffer.read(amt)
        if self.after_read is not None:
            self.after_read()
        return result

    def close(self):
        self.close_calls += 1
        if self.after_close is not None:
            self.after_close()
        if self.close_error is not None:
            raise self.close_error

    def release_conn(self):
        self.release_calls += 1


def response(status=200, body=CANDLE_CSV, headers=(), **raw_options):
    result = requests.Response()
    result.status_code = status
    content = body.encode('utf-8') if type(body) is str else body
    original = [('Content-Type', 'text/csv; charset=utf-8'), *headers]
    result.raw = RawBody(content, original, **raw_options)
    for name, value in original:
        result.headers[name] = value
    # Cached bytes make these doubles substantive on the unchanged A2-02 pair,
    # whose eager access must not cause a mock-signature or helper-only RED.
    result._content = content
    result._content_consumed = False
    result.encoding = 'utf-8'
    return result


def transport(gathering, monkeypatch, outcomes):
    sequence = iter(outcomes)
    state = SimpleNamespace(calls=[], sleeps=[], elapsed=0.0, after_get=None,
                            after_sleep=None)

    def get(url, headers, timeout, verify=None, allow_redirects=None, stream=None):
        state.calls.append(dict(url=url, headers=dict(headers), timeout=timeout,
                                verify=verify, allow_redirects=allow_redirects,
                                stream=stream))
        outcome = next(sequence)
        if state.after_get is not None:
            state.after_get()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def sleep(seconds):
        state.sleeps.append(seconds)
        state.elapsed += seconds
        if state.after_sleep is not None:
            state.after_sleep()

    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'header', {'User-Agent': 'AGENT_A'})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': 'AGENT_B'})
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(
        sleep=sleep, perf_counter=lambda: state.elapsed, time=lambda: FIXED_WALL,
        gmtime=native_time.gmtime))
    return state


def query(gathering, **kwargs):
    return gathering.moex_query('FIXTURE_SHARE', 'Акции', '2024-02-29', '2024-03-01',
                                24, **kwargs)


def test_red_permanent_404_does_not_consume_prepared_success(gathering, monkeypatch):
    rejected = response(404, 'A2_REJECTED_BODY')
    unused = response()
    state = transport(gathering, monkeypatch, [rejected, unused, response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering)
    assert caught.value.response is rejected
    assert caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert unused.raw.reads == []


def test_red_invalid_schema_does_not_consume_prepared_success(gathering, monkeypatch):
    unused = response()
    state = transport(gathering, monkeypatch, [response(body='not_candles\n'), unused, response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == ('A2_ISS_RESPONSE_SCHEMA_INVALID',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert unused.raw.reads == []


def test_red_million_attempts_rejected_before_network(gathering, monkeypatch):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering, max_retries=1000000)
    assert caught.value.args == ('A2_RETRY_ARGUMENTS',)
    assert state.calls == [] and state.sleeps == []


def test_red_retry_after_31_preserves_http_primary_without_retry(gathering, monkeypatch):
    rejected = response(503, headers=[('Retry-After', '31')])
    unused = response()
    state = transport(gathering, monkeypatch, [rejected, unused, response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering)
    assert caught.value.response is rejected
    assert caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []


def test_red_body_cap_plus_one_is_never_accepted(gathering, monkeypatch):
    encoded = CANDLE_CSV.encode('utf-8')
    rejected = response(body=encoded + b'\n' * (BODY_CAP + 1 - len(encoded)))
    state = transport(gathering, monkeypatch, [rejected, response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == ('A2_ISS_BODY_LIMIT_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == []


def test_red_successful_200_overrun_never_reports_returned(gathering, monkeypatch, event_log):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    state.after_get = lambda: setattr(state, 'elapsed', 181.0)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


def assert_frame(frame):
    assert frame.columns.tolist() == CANDLE_COLUMNS
    assert frame.values.tolist() == CANDLE_ROWS
    assert frame.index.tolist() == [0, 1]
    assert [str(dtype) for dtype in frame.dtypes] == [
        'float64', 'float64', 'float64', 'float64', 'float64', 'int64',
        'object', 'object', 'object']
    assert list(zip(frame.ticker, [24, 24], frame.begin)) == [
        ('FIXTURE_SHARE', 24, '2024-02-29 00:00:00'),
        ('FIXTURE_SHARE', 24, '2024-03-01 00:00:00')]


def assert_terminal(event_log, code, category, outcome, attempt, limit=5):
    terminal = event_log.events[-1]
    assert (terminal['event'], terminal['level'], terminal['outcome']) == (
        'a2_query_failed', 'ERROR', outcome)
    assert terminal['stage'] == 'data_gathering' and terminal['fields'] == {}
    assert terminal['retries'] == {'attempt': attempt, 'limit': 1000 * limit}
    error = terminal['error']
    assert (error['code'], error['category'], error['attempt'],
            error['final'], error['retryable']) == (code, category, attempt, True, False)
    assert error['endpoint'] == 'ISS_CANDLES'
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)
    assert set(error) == {'category', 'code', 'stage', 'retryable', 'final', 'message',
        'null_reasons', 'run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval',
        'page', 'file', 'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type'}


def clock_after_diagnostic(gathering, monkeypatch, state, event_name, elapsed):
    original = gathering._a2_diagnostic

    def diagnostic(context, level, event, message, fields, primary=None):
        result = original(context, level, event, message, fields, primary)
        if event == event_name:
            state.elapsed = elapsed
        return result

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)


@pytest.mark.parametrize('status', [408, 429, 500, 502, 503, 504])
def test_closed_retryable_status_set_preserves_literal_result(
        gathering, monkeypatch, event_log, status):
    rejected, accepted = response(status), response()
    state = transport(gathering, monkeypatch, [rejected, accepted, response(body=EMPTY_CSV)])
    assert_frame(query(gathering, logging_context=event_log.context))
    assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]
    assert rejected.raw.reads == []
    assert rejected.raw.close_calls == 1 and accepted.raw.close_calls == 1
    retry = [item for item in event_log.events if item['event'] == 'a2_query_retry']
    assert len(retry) == 1
    assert (retry[0]['error']['code'], retry[0]['error']['category']) == (
        'A2_ISS_HTTP_STATUS', 'QUOTA_429' if status == 429 else 'SOURCE')
    assert retry[0]['error']['final'] is False and retry[0]['error']['retryable'] is True


@pytest.mark.parametrize('status', [201, 204, 400, 401, 403, 404, 409, 418, 422, 501, 505, 599])
def test_other_status_is_terminal_without_body_or_prepared_success(
        gathering, monkeypatch, event_log, status):
    rejected = response(status, read_error=AssertionError('rejected body was read'))
    unused = response()
    state = transport(gathering, monkeypatch, [rejected, unused, response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is rejected and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert rejected.raw.reads == [] and rejected.raw.close_calls == 1
    assert unused.raw.reads == [] and unused.raw.close_calls == 0
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'SOURCE', 'HTTP_NONRETRYABLE', 1)


@pytest.mark.parametrize('factory', [
    lambda: requests.exceptions.ConnectTimeout('A2_TYPED_TIMEOUT'),
    lambda: requests.exceptions.ReadTimeout('A2_TYPED_TIMEOUT'),
    lambda: requests.exceptions.Timeout('A2_TYPED_TIMEOUT'),
    lambda: requests.exceptions.ConnectionError(ConnectionResetError(errno.ECONNRESET, 'A2_RESET')),
    lambda: requests.exceptions.ConnectionError(ConnectionAbortedError(errno.ECONNABORTED, 'A2_ABORT')),
    lambda: requests.exceptions.ConnectionError(socket.gaierror(socket.EAI_AGAIN, 'A2_DNS_TEMP')),
])
def test_only_typed_transient_request_failure_can_retry(gathering, monkeypatch, factory):
    primary = factory()
    original_args = primary.args
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert primary.args == original_args
    assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]


@pytest.mark.parametrize('factory', [
    lambda: requests.exceptions.ConnectionError('timeout reset temporary DNS text is not evidence'),
    lambda: requests.exceptions.ConnectionError(socket.gaierror(socket.EAI_NONAME, 'A2_DNS_PERMANENT')),
    lambda: requests.exceptions.ProxyError('A2_PROXY'),
    lambda: requests.exceptions.InvalidURL('A2_INVALID_URL'),
    lambda: RuntimeError('A2_UNKNOWN_IMPLEMENTATION'),
])
def test_unknown_permanent_and_configuration_errors_do_not_speculate(
        gathering, monkeypatch, factory):
    primary = factory()
    original_args = primary.args
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(type(primary)) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == original_args
    assert len(state.calls) == 1 and state.sleeps == []


@pytest.mark.parametrize('kind', ['tls', 'proxy'])
def test_tls_or_config_wrapper_dominates_nested_timeout(gathering, monkeypatch, kind):
    nested = requests.exceptions.Timeout('A2_NESTED_TIMEOUT')
    primary = (requests.exceptions.SSLError(nested) if kind == 'tls'
               else requests.exceptions.ProxyError(nested))
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(type(primary)) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == (nested,)
    assert len(state.calls) == 1 and state.sleeps == []


def test_historical_context_does_not_make_unknown_error_retryable(gathering, monkeypatch):
    primary = requests.exceptions.ConnectionError('A2_UNKNOWN_CURRENT')
    primary.__context__ = requests.exceptions.Timeout('A2_HISTORICAL_TIMEOUT')
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.ConnectionError) as caught:
        query(gathering)
    assert caught.value is primary
    assert len(state.calls) == 1 and state.sleeps == []


@pytest.mark.parametrize('parameter,value', [
    ('max_retries', 6), ('max_retries', 1000000), ('max_retries', True),
    ('retry_sleep_start', 30.0001), ('retry_sleep_start', float('inf')),
    ('retry_sleep_start', float('nan')), ('retry_sleep_start', True),
    ('retry_jitter', -0.001), ('retry_jitter', 1.001), ('retry_jitter', True),
    ('retry_jitter', None), ('retry_jitter', '0'),
    ('retry_jitter', float('inf')), ('retry_jitter', float('nan')),
])
def test_closed_policy_arguments_refuse_before_get(gathering, monkeypatch, event_log,
                                                 parameter, value):
    state = transport(gathering, monkeypatch, [response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering, logging_context=event_log.context, **{parameter: value})
    assert caught.value.args == ('A2_RETRY_ARGUMENTS',)
    assert state.calls == [] and state.sleeps == []
    assert len(event_log.events) == 1
    event = event_log.events[0]
    assert event['retries'] is None and event['null_reasons']['retries'] == 'INVALID_CONFIGURATION'
    assert (event['error']['code'], event['error']['category'], event['error']['attempt']) == (
        'A2_RETRY_ARGUMENTS', 'CONFIG', 0)


@pytest.mark.parametrize('count', [1, 2, 3, 4, 5])
@pytest.mark.parametrize('base', [0, 30])
def test_policy_argument_endpoints_keep_success_semantics(gathering, monkeypatch, count, base):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    assert_frame(query(gathering, max_retries=count, retry_sleep_start=base))
    assert len(state.calls) == 2 and state.sleeps == [3, 3]


@pytest.mark.parametrize('jitter', [0.0, 1.0])
@pytest.mark.parametrize('selected', [0.0, 1.0])
def test_opt_in_jitter_has_independent_min_max_oracle(gathering, monkeypatch, jitter, selected):
    state = transport(gathering, monkeypatch, [response(503), response(), response(body=EMPTY_CSV)])
    rng_calls = []

    def uniform(low, high):
        rng_calls.append((low, high))
        return selected * high

    monkeypatch.setattr(gathering.random, 'uniform', uniform)
    assert_frame(query(gathering, retry_jitter=jitter))
    assert state.sleeps == [3 + jitter * selected, 3, 3]
    assert rng_calls == ([] if jitter == 0 else [(0, jitter)])


def test_exponential_jitter_wait_is_capped_and_final_attempt_has_no_wait(gathering, monkeypatch):
    responses = [response(503) for _ in range(5)]
    state = transport(gathering, monkeypatch, [*responses, response(body=EMPTY_CSV)])
    monkeypatch.setattr(gathering.random, 'uniform', lambda low, high: high)
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, retry_sleep_start=30, retry_jitter=1)
    assert caught.value.response is responses[-1]
    assert len(state.calls) == 5 and state.sleeps == [30, 30, 30, 30]
    assert all(item.raw.close_calls == 1 and item.raw.reads == [] for item in responses)


def test_zero_jitter_preserves_literal_default_exhaustion_waits(gathering, monkeypatch):
    failures = [requests.exceptions.ReadTimeout('A2_LAST_' + str(i)) for i in range(5)]
    state = transport(gathering, monkeypatch, [*failures, response(body=EMPTY_CSV)])

    def forbidden_uniform(*args):
        pytest.fail('Default zero jitter must not consult RNG')

    monkeypatch.setattr(gathering.random, 'uniform', forbidden_uniform)
    with pytest.raises(requests.exceptions.ReadTimeout) as caught:
        query(gathering)
    assert caught.value is failures[-1] and caught.value.args == ('A2_LAST_4',)
    assert len(state.calls) == 5 and state.sleeps == [3, 6, 12, 24]


@pytest.mark.parametrize('header,wait', [
    (None, 3), ('', 3), ('-1', 3), ('+1', 3), ('1.5', 3), ('１２', 3),
    ('junk', 3), ('0', 3), ('2', 3), ('7', 7), ('30', 30),
    ('Sat, 02 Mar 2024 12:00:07 GMT', 7),
    ('Saturday, 02-Mar-24 12:00:07 GMT', 7),
    ('Sat Mar  2 12:00:07 2024', 7),
    ('Sat, 02 Mar 2024 11:59:59 GMT', 3),
])
def test_retry_after_absent_invalid_and_valid_delay_oracles(gathering, monkeypatch, header, wait):
    headers = [] if header is None else [('Retry-After', header)]
    rejected = response(503, headers=headers)
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert len(state.calls) == 3 and state.sleeps == [wait, 3, 3]
    assert rejected.raw.reads == [] and rejected.raw.close_calls == 1


@pytest.mark.parametrize('headers', [
    [('Retry-After', '5'), ('Retry-After', '5')],
    [('Retry-After', '5'), ('Retry-After', '7')],
    [('Retry-After', '5, 5')],
    [('Retry-After', 'Sat, 02 Mar 2024 12:00:07 GMT, 5')],
    [('Retry-After', 'Sat, 02 Mar 2024 12:00:07 GMT, Sat, 02 Mar 2024 12:00:08 GMT')],
    [('Retry-After', 'Wed, junk')],
    [('Retry-After', 'x' * 129)],
    [('Retry-After', '0' * 129)],
    [('Retry-After', '31')],
    [('Retry-After', '9999999999999999999999999999999999999999')],
    [('Retry-After', 'Sat, 02 Mar 2024 12:00:31 GMT')],
])
def test_retry_after_closed_refusal_preserves_current_http_object(
        gathering, monkeypatch, event_log, headers):
    rejected = response(503, headers=headers)
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is rejected and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert rejected.raw.reads == [] and rejected.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'SOURCE', 'RETRY_NOT_ADMITTED', 1)
    assert not any(item['event'] == 'a2_query_retry' for item in event_log.events)


@pytest.mark.parametrize('value,expected_wait', [('0' * 128, 3), ('x' * 128, 3), ('\t 7 \t', 7)])
def test_retry_after_length_and_ows_boundary(gathering, monkeypatch, value, expected_wait):
    state = transport(gathering, monkeypatch, [response(503, headers=[('Retry-After', value)]), response(), response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert len(state.calls) == 3 and state.sleeps == [expected_wait, 3, 3]


@pytest.mark.parametrize('header', [
    'Sat, 02 Mar 2024 12:00:07 UTC',
    'Sat, 02 Mar 2024 12:00:07 GMT trailing',
    'Sat, 31 Feb 2024 12:00:07 GMT',
    'Sat, 02 Mar 2024 12:00:60 GMT',
])
def test_comma_date_malformed_or_non_http_grammar_is_ambiguous(gathering, monkeypatch, header):
    rejected = response(503, headers=[('Retry-After', header)])
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering)
    assert caught.value.response is rejected
    assert len(state.calls) == 1 and state.sleeps == []


@pytest.mark.parametrize('future_offset,should_retry', [(0, False), (1, True)])
def test_rfc850_two_digit_year_uses_exact_fifty_year_boundary(
        gathering, monkeypatch, future_offset, should_retry):
    # Exact fifty-year future remains 2074 and is too distant; one second beyond
    # the boundary must be interpreted as the most recent matching year, 1974.
    boundary = datetime.datetime(2074, 3, 2, 12, tzinfo=datetime.timezone.utc)
    represented = boundary + datetime.timedelta(seconds=future_offset)
    year = 1974 if should_retry else 2074
    weekday = represented.replace(year=year).strftime('%A')
    header = weekday + represented.strftime(', %d-%b-%y %H:%M:%S GMT')
    rejected = response(503, headers=[('Retry-After', header)])
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    if should_retry:
        assert_frame(query(gathering))
        assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]
    else:
        with pytest.raises(requests.exceptions.HTTPError) as caught:
            query(gathering)
        assert caught.value.response is rejected
        assert len(state.calls) == 1 and state.sleeps == []


@pytest.mark.parametrize('header_kind', ['retry-after', 'encoding'])
def test_missing_raw_header_provenance_is_fail_closed(gathering, monkeypatch, header_kind):
    rejected = response(503 if header_kind == 'retry-after' else 200)
    rejected.raw.headers = None
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    exception_type = requests.exceptions.HTTPError if header_kind == 'retry-after' else ValueError
    with pytest.raises(exception_type) as caught:
        query(gathering)
    if header_kind == 'retry-after':
        assert caught.value.response is rejected
    else:
        assert caught.value.args == ('A2_ISS_CONTENT_ENCODING_UNSUPPORTED',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert rejected.raw.reads == [] and rejected.raw.close_calls == 1


@pytest.mark.parametrize('headers', [[], [('Content-Encoding', 'identity')],
    [('Content-Encoding', 'IDENTITY')], [('Content-Encoding', ' \tIdentity\t ')]])
def test_content_encoding_absent_or_exact_single_identity_is_accepted(
        gathering, monkeypatch, headers):
    accepted = response(headers=headers)
    state = transport(gathering, monkeypatch, [accepted, response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert len(state.calls) == 2 and state.sleeps == [3, 3]
    assert accepted.raw.close_calls == 1
    assert all(1 <= amount <= 65536 and decode is False for amount, decode in accepted.raw.reads)


@pytest.mark.parametrize('headers', [
    [('Content-Encoding', '')], [('Content-Encoding', 'gzip')],
    [('Content-Encoding', 'xidentity')], [('Content-Encoding', 'identitygzip')],
    [('Content-Encoding', 'identity;q=1')], [('Content-Encoding', 'identity, identity')],
    [('Content-Encoding', 'identity'), ('Content-Encoding', 'identity')],
    [('Content-Encoding', 'identity'), ('Content-Encoding', 'gzip')],
    [('Content-Encoding', 'identity\r\n')],
])
def test_unknown_multiple_conflicting_or_substring_encoding_is_rejected_before_read(
        gathering, monkeypatch, event_log, headers):
    rejected = response(headers=headers, read_error=AssertionError('coding body must not be read'))
    state = transport(gathering, monkeypatch, [rejected, response(), response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_CONTENT_ENCODING_UNSUPPORTED',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert rejected.raw.reads == [] and rejected.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_CONTENT_ENCODING_UNSUPPORTED', 'SOURCE', 'RESPONSE_REJECTED', 1)


def test_identity_accept_header_is_request_local_across_rotation(gathering, monkeypatch):
    state = transport(gathering, monkeypatch, [response(503), response(), response(body=EMPTY_CSV)])
    original = {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'gzip'}
    rotated = {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'br'}
    monkeypatch.setattr(gathering, 'header', original)
    monkeypatch.setattr(gathering, 'get_next_header', lambda previous: rotated)
    assert_frame(query(gathering))
    assert original == {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'gzip'}
    assert rotated == {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'br'}
    assert [call['headers'] for call in state.calls] == [
        {'User-Agent': 'AGENT_A', 'Accept-Encoding': 'identity'},
        {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'identity'},
        {'User-Agent': 'AGENT_B', 'Accept-Encoding': 'identity'}]
    assert all(call['verify'] is True and call['allow_redirects'] is False
               and call['stream'] is True and call['timeout'] == (5, 30)
               and call['url'].startswith('https://iss.moex.com/') for call in state.calls)


@pytest.mark.parametrize('offset', [-1, 0, 1])
def test_body_cap_boundary_is_complete_response_or_whole_failure(
        gathering, monkeypatch, offset):
    prefix = CANDLE_CSV.encode('utf-8')
    body = prefix + b'\n' * (BODY_CAP + offset - len(prefix))
    item = response(body=body)
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    if offset <= 0:
        assert_frame(query(gathering))
        assert state.sleeps == [3, 3]
    else:
        def pandas_forbidden(*args, **kwargs):
            pytest.fail('Overflow prefix must never reach pandas')
        monkeypatch.setattr(gathering.pd, 'read_csv', pandas_forbidden)
        with pytest.raises(ValueError) as caught:
            query(gathering)
        assert caught.value.args == ('A2_ISS_BODY_LIMIT_EXCEEDED',)
        assert state.sleeps == []
    assert len(state.calls) == (2 if offset <= 0 else 1) and item.raw.close_calls == 1
    assert all(1 <= amount <= 65536 and decode is False for amount, decode in item.raw.reads)
    assert item.raw.buffer.tell() <= BODY_CAP + 1


@pytest.mark.parametrize('declared', [None, '', 'junk', '-1', '1'])
def test_missing_malformed_or_false_small_content_length_never_disables_counter(
        gathering, monkeypatch, declared):
    headers = [] if declared is None else [('Content-Length', declared)]
    item = response(body=b'x' * (BODY_CAP + 1), headers=headers)
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == ('A2_ISS_BODY_LIMIT_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert item.raw.buffer.tell() <= BODY_CAP + 1


def test_declared_oversize_may_refuse_before_body(gathering, monkeypatch):
    item = response(headers=[('Content-Length', str(BODY_CAP + 1))])
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    try:
        result = query(gathering)
    except ValueError as caught:
        assert caught.args == ('A2_ISS_BODY_LIMIT_EXCEEDED',)
        assert state.sleeps == [] and item.raw.reads == []
    else:
        # Early refusal from an untrusted declaration is permitted, not required;
        # choosing the actual complete byte counter also satisfies the policy.
        assert_frame(result)
        assert state.sleeps == [3, 3]
    assert len(state.calls) == (2 if state.sleeps else 1) and item.raw.close_calls == 1


def test_no_eager_content_text_or_json_on_stream_response(gathering, monkeypatch):
    class StreamingResponse(requests.Response):
        @property
        def content(self):
            pytest.fail('streaming code accessed eager content')

        @property
        def text(self):
            pytest.fail('streaming code accessed eager text')

        def json(self, *args, **kwargs):
            pytest.fail('streaming code accessed eager JSON')

    item = StreamingResponse()
    item.status_code = 200
    item.headers['Content-Type'] = 'text/csv; charset=utf-8'
    item.raw = RawBody(CANDLE_CSV.encode('utf-8'), [('Content-Type', item.headers['Content-Type'])])
    item._content_consumed = False
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert len(state.calls) == 2 and state.sleeps == [3, 3] and item.raw.close_calls == 1


@pytest.mark.parametrize('last', ['http', 'read'])
def test_raw_read_native_primary_and_later_http_have_current_attempt_precedence(
        gathering, monkeypatch, last):
    native = ReadTimeoutError(None, '/A2_RAW_PATH_CANARY', 'A2_RAW_NATIVE_CANARY')
    original_args = native.args
    reading = response(read_error=native)
    rejected = response(503)
    sequence = [reading, rejected] if last == 'http' else [rejected, reading]
    state = transport(gathering, monkeypatch, [*sequence, response(body=EMPTY_CSV)])
    expected = requests.exceptions.HTTPError if last == 'http' else ReadTimeoutError
    with pytest.raises(expected) as caught:
        query(gathering, max_retries=2)
    if last == 'http':
        assert caught.value.response is rejected and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    else:
        assert caught.value is native and native.args == original_args
    assert len(state.calls) == 2 and state.sleeps == [3]
    assert reading.raw.close_calls == 1 and rejected.raw.close_calls == 1


def test_close_only_failure_is_native_terminal_io_without_retry(gathering, monkeypatch, event_log):
    primary = OSError('A2_CLOSE_CANARY')
    item = response(close_error=primary)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(OSError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_CLOSE_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_CLOSE_ERROR', 'IO', 'REQUEST_FAILED', 1)


def test_falsey_http_response_is_closed_and_close_failure_cannot_replace_http(
        gathering, monkeypatch, event_log):
    item = response(503, close_error=OSError('A2_SECONDARY_CLOSE_CANARY'))
    assert not item
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is item and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert not any(event['event'] == 'a2_query_retry' for event in event_log.events)


def test_exception_attached_response_is_closed_without_primary_rewrite(gathering, monkeypatch):
    item = response(503)
    primary = requests.exceptions.SSLError('A2_TLS_ATTACHED', response=item)
    state = transport(gathering, monkeypatch, [primary, response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.SSLError) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == ('A2_TLS_ATTACHED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1


def test_memory_error_is_not_retryable_or_rewritten(gathering, monkeypatch, event_log):
    primary = MemoryError('A2_MEMORY_CANARY')
    item = response(read_error=primary)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(MemoryError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_MEMORY_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_MEMORY_ERROR', 'MEMORY_ERROR', 'REQUEST_FAILED', 1)


def test_deadline_before_started_has_zero_attempt_and_exact_native_timeout(
        gathering, monkeypatch, event_log):
    state = transport(gathering, monkeypatch, [response(body=EMPTY_CSV)])
    ticks = [0.0]

    def expired_after_entry():
        if ticks:
            return ticks.pop()
        return 180.0

    gathering.time.perf_counter = expired_after_entry
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert type(caught.value) is requests.exceptions.Timeout
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert state.calls == [] and state.sleeps == []
    assert [item['event'] for item in event_log.events] == ['a2_query_failed']
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 0)


@pytest.mark.parametrize('expiry_event,expected_events', [
    ('a2_query_started', ['a2_query_started', 'a2_query_failed']),
    ('a2_query_attempt_started',
     ['a2_query_started', 'a3_page_started', 'a2_query_attempt_started', 'a2_query_failed']),
])
def test_deadline_after_successful_diagnostics_never_admits_first_get(
        gathering, monkeypatch, event_log, expiry_event, expected_events):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    clock_after_diagnostic(gathering, monkeypatch, state, expiry_event, 180.0)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert state.calls == [] and state.sleeps == []
    assert [item['event'] for item in event_log.events] == expected_events
    assert all(item['retries']['attempt'] == 0 for item in event_log.events)
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 0)


@pytest.mark.parametrize('failure_event', ['a2_query_started', 'a2_query_attempt_started'])
def test_logging_failure_without_primary_during_expiry_remains_logging_incomplete(
        gathering, monkeypatch, event_log, failure_event):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    original = event_log.logger.emit_event

    def emit(context, level, event, message, fields):
        if event == failure_event:
            state.elapsed = 180.0
            raise OSError('A2_LOGGING_DEADLINE_CANARY')
        return original(context, level, event, message, fields)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    with pytest.raises(RuntimeError) as caught:
        query(gathering, logging_context=event_log.context)
    assert type(caught.value) is RuntimeError and caught.value.args == ('LOGGING_INCOMPLETE',)
    assert caught.value.__cause__ is None
    assert state.calls == [] and state.sleeps == []
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned', 'a2_query_failed')
                   for item in event_log.events)


def test_deadline_after_failed_http_preserves_current_native_http_error(
        gathering, monkeypatch, event_log):
    item = response(503)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    state.after_get = lambda: setattr(state, 'elapsed', 180.0)
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_HTTP_STATUS',) and caught.value.response is item
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'SOURCE', 'DEADLINE_EXCEEDED', 1)
    assert not any(event['event'] == 'a2_query_retry' for event in event_log.events)


def test_retry_after_budget_infeasible_preserves_http_without_wait_or_warning(
        gathering, monkeypatch, event_log):
    item = response(429, headers=[('Retry-After', '7')])
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    state.after_get = lambda: setattr(state, 'elapsed', 174.0)
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is item and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'QUOTA_429', 'RETRY_NOT_ADMITTED', 1)
    assert not any(event['event'] == 'a2_query_retry' for event in event_log.events)


def test_expiry_in_retry_warning_keeps_plan_record_but_never_waits_or_gets(
        gathering, monkeypatch, event_log):
    item = response(503)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    clock_after_diagnostic(gathering, monkeypatch, state, 'a2_query_retry', 180.0)
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is item and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert len([item for item in event_log.events if item['event'] == 'a2_query_retry']) == 1
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'SOURCE', 'DEADLINE_EXCEEDED', 1)


def test_expiry_during_completed_retry_wait_has_no_second_get(gathering, monkeypatch, event_log):
    item = response(503)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    state.after_sleep = lambda: setattr(state, 'elapsed', 181.0)
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is item and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == [3]
    assert_terminal(event_log, 'A2_ISS_HTTP_STATUS', 'SOURCE', 'DEADLINE_EXCEEDED', 1)


def test_admission_expiry_after_prior_failure_preserves_same_primary_object(
        gathering, monkeypatch, event_log):
    primary = requests.exceptions.Timeout('A2_LAST_PRIMARY_CANARY')
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    original = gathering._a2_diagnostic
    admissions = []

    def diagnostic(context, level, event, message, fields, pending=None):
        result = original(context, level, event, message, fields, pending)
        if event == 'a2_query_attempt_started':
            admissions.append(fields['retries']['attempt'])
            if len(admissions) == 2:
                state.elapsed = 180.0
        return result

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_LAST_PRIMARY_CANARY',)
    assert admissions == [0, 1] and len(state.calls) == 1 and state.sleeps == [3]
    assert_terminal(event_log, 'A2_ISS_TIMEOUT', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


def test_new_successful_get_expiry_never_resurrects_previous_http_failure(
        gathering, monkeypatch, event_log):
    rejected, accepted = response(503), response()
    state = transport(gathering, monkeypatch, [rejected, accepted, response(body=EMPTY_CSV)])

    def advance_second_get():
        if len(state.calls) == 2:
            state.elapsed = 180.0

    state.after_get = advance_second_get
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert type(caught.value) is requests.exceptions.Timeout
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 2 and state.sleeps == [3]
    assert rejected.raw.close_calls == 1 and accepted.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 2)


@pytest.mark.parametrize('phase', ['read', 'codec', 'records', 'pandas', 'frame'])
def test_expiry_after_each_successful_processing_phase_discards_frame(
        gathering, monkeypatch, event_log, phase):
    item = response()
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    if phase == 'read':
        item.raw.after_read = lambda: setattr(state, 'elapsed', 180.0)
    else:
        owner, name = ((gathering.pd, 'read_csv') if phase == 'pandas' else
                       (gathering, {'codec': '_a2_candle_codec', 'records': '_a2_candle_records',
                                    'frame': '_a2_candle_frame'}[phase]))
        original = getattr(owner, name)

        def process(*args, **kwargs):
            result = original(*args, **kwargs)
            state.elapsed = 180.0
            return result

        monkeypatch.setattr(owner, name, process)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


@pytest.mark.parametrize('remaining', [2.0, 3.0])
def test_insufficient_success_sleep_budget_never_shortens_or_starts_sleep(
        gathering, monkeypatch, event_log, remaining):
    item = response()
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    item.raw.after_close = lambda: setattr(state, 'elapsed', 180.0 - remaining)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


@pytest.mark.parametrize('elapsed', [180.0, 181.0])
def test_complete_success_sleep_overrun_never_emits_returned(gathering, monkeypatch, event_log, elapsed):
    item = response()
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    state.after_sleep = lambda: setattr(state, 'elapsed', elapsed)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == [3] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


@pytest.mark.parametrize('primary', [RuntimeError('A2_SLEEP_CANARY'), KeyboardInterrupt('A2_SLEEP_CANARY')])
def test_success_sleep_native_interruption_precedes_simultaneous_expiry(
        gathering, monkeypatch, event_log, primary):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])

    def interrupt():
        state.elapsed = 180.0
        raise primary

    state.after_sleep = interrupt
    with pytest.raises(type(primary)) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_SLEEP_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == [3]
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


def test_native_read_primary_survives_cleanup_and_deadline_together(
        gathering, monkeypatch, event_log):
    primary = ReadTimeoutError(None, '/A2_PATH_CANARY', 'A2_READ_CANARY')
    original_args = primary.args
    item = response(read_error=primary, close_error=OSError('A2_CLOSE_CANARY'))
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    item.raw.after_close = lambda: setattr(state, 'elapsed', 180.0)
    with pytest.raises(ReadTimeoutError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == original_args
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_TIMEOUT', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


def test_cleanup_only_primary_survives_same_operation_deadline(gathering, monkeypatch, event_log):
    primary = OSError('A2_CLOSE_PRIMARY_CANARY')
    item = response(close_error=primary)
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    item.raw.after_close = lambda: setattr(state, 'elapsed', 180.0)
    with pytest.raises(OSError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_CLOSE_PRIMARY_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert_terminal(event_log, 'A2_ISS_CLOSE_ERROR', 'IO', 'REQUEST_FAILED', 1)


def test_created_deadline_timeout_survives_following_close_failure(
        gathering, monkeypatch, event_log):
    item = response(close_error=OSError('A2_SECONDARY_CLOSE_CANARY'))
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    state.after_get = lambda: setattr(state, 'elapsed', 180.0)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 1)


@pytest.mark.parametrize('with_primary', [False, True])
def test_logging_failure_and_expiry_preserve_required_precedence(
        gathering, monkeypatch, event_log, capsys, with_primary):
    primary = requests.exceptions.ReadTimeout('A2_LOGGING_PRIMARY_CANARY')
    state = transport(gathering, monkeypatch, [primary if with_primary else response(), response(body=EMPTY_CSV)])
    original = event_log.logger.emit_event
    failure_event = 'a2_query_retry' if with_primary else 'a2_query_returned'

    def emit(context, level, event, message, fields):
        if event == failure_event:
            state.elapsed = 180.0
            raise OSError('A2_LOGGING_SECONDARY_CANARY')
        return original(context, level, event, message, fields)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    expected_type = requests.exceptions.ReadTimeout if with_primary else RuntimeError
    with pytest.raises(expected_type) as caught:
        query(gathering, logging_context=event_log.context)
    if with_primary:
        assert caught.value is primary and primary.args == ('A2_LOGGING_PRIMARY_CANARY',)
        assert state.sleeps == []
    else:
        assert caught.value.args == ('LOGGING_INCOMPLETE',) and caught.value.__cause__ is None
        assert state.sleeps == [3, 3]
    assert len(state.calls) == (1 if with_primary else 2)
    assert not any(item['event'] in ('a2_query_returned', 'a2_query_failed') for item in event_log.events)
    output = capsys.readouterr()
    assert all(marker not in output.out + output.err for marker in (
        'A2_LOGGING_PRIMARY_CANARY', 'A2_LOGGING_SECONDARY_CANARY'))
    assert all(len(line) <= 256 for line in (output.out + output.err).splitlines())


def test_timeout_tuple_is_clipped_to_admitted_positive_remaining_budget(
        gathering, monkeypatch, event_log):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    clock_after_diagnostic(gathering, monkeypatch, state, 'a2_query_attempt_started', 178.0)
    with pytest.raises(requests.exceptions.Timeout):
        query(gathering, logging_context=event_log.context)
    assert len(state.calls) == 1 and state.calls[0]['timeout'] == (2.0, 2.0)
    assert state.sleeps == []


def test_returned_duration_includes_cleanup_and_full_success_sleep(
        gathering, monkeypatch, event_log):
    item = response()
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    state.after_get = lambda: setattr(state, 'elapsed', state.elapsed + 0.25)
    item.raw.after_close = lambda: setattr(state, 'elapsed', state.elapsed + 0.5)
    assert_frame(query(gathering, logging_context=event_log.context))
    assert len(state.calls) == 2 and state.sleeps == [3, 3]
    returned = event_log.events[-1]
    assert returned['event'] == 'a2_query_returned' and returned['duration_ms'] == 7000.0
    assert returned['retries'] == {'attempt': 2, 'limit': 5000}
    assert all(item['outcome'] == 'REQUEST_ADMISSION_STARTED' and item['retries']['attempt'] == 0
               for item in event_log.events if item['event'] == 'a2_query_attempt_started')


def test_deep_typed_wrapper_does_not_search_beyond_closed_depth(gathering, monkeypatch):
    primary = requests.exceptions.Timeout('A2_DEEP_TIMEOUT')
    for _ in range(10):
        primary = requests.exceptions.ConnectionError(primary)
    original_args = primary.args
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.ConnectionError) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == original_args
    assert len(state.calls) == 1 and state.sleeps == []


def test_cyclic_transport_wrapper_terminates_without_text_classification(gathering, monkeypatch):
    primary = requests.exceptions.ConnectionError('A2_CYCLE')
    primary.args = (primary,)
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.ConnectionError) as caught:
        query(gathering)
    assert caught.value is primary and primary.args[0] is primary
    assert len(state.calls) == 1 and state.sleeps == []


def test_raw_truncated_response_keeps_native_protocol_failure_and_no_prefix_return(
        gathering, monkeypatch, event_log):
    primary = ProtocolError('A2_TRUNCATED_CANARY', IncompleteRead(b'A2_PREFIX', 20))
    original_args = primary.args
    item = response(read_error=primary)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(ProtocolError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == original_args
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1
    assert not any(event['event'] in ('a2_query_returned', 'a2_query_retry') for event in event_log.events)


@pytest.mark.parametrize('primary', [KeyboardInterrupt('A2_READ_INTERRUPTED'), SystemExit('A2_READ_INTERRUPTED')])
def test_raw_interruption_still_closes_response_and_keeps_native_primary(
        gathering, monkeypatch, primary):
    item = response(read_error=primary)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(type(primary)) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == ('A2_READ_INTERRUPTED',)
    assert len(state.calls) == 1 and state.sleeps == [] and item.raw.close_calls == 1


def test_native_pandas_error_is_terminal_without_schema_recovery(gathering, monkeypatch, event_log):
    primary = gathering.pd.errors.ParserError('A2_PANDAS_PRIMARY_CANARY')
    first, unused = response(), response()
    state = transport(gathering, monkeypatch, [first, unused, response(body=EMPTY_CSV)])

    def parse(*args, **kwargs):
        raise primary

    monkeypatch.setattr(gathering.pd, 'read_csv', parse)
    with pytest.raises(gathering.pd.errors.ParserError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_PANDAS_PRIMARY_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert first.raw.close_calls == 1 and unused.raw.close_calls == 0
    assert_terminal(event_log, 'A2_ISS_PARSE_ERROR', 'SOURCE', 'RESPONSE_REJECTED', 1)


def test_strict_decode_failure_preserves_native_object_without_next_response(
        gathering, monkeypatch, event_log):
    first, unused = response(body=b'\xff'), response()
    state = transport(gathering, monkeypatch, [first, unused, response(body=EMPTY_CSV)])
    with pytest.raises(UnicodeDecodeError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.encoding == 'utf-8' and caught.value.object == b'\xff'
    assert caught.value.start == 0 and caught.value.end == 1
    assert len(state.calls) == 1 and state.sleeps == []
    assert first.raw.close_calls == 1 and unused.raw.close_calls == 0
    assert_terminal(event_log, 'A2_ISS_RESPONSE_DECODE_ERROR', 'SOURCE', 'RESPONSE_REJECTED', 1)


@pytest.mark.parametrize('with_context', [False, True])
@pytest.mark.parametrize('kind', ['retry-after', 'coding', 'read', 'cleanup', 'header-rotation'])
def test_retry_resource_canaries_remain_absent_from_console_and_logger(
        gathering, monkeypatch, event_log, capsys, with_context, kind):
    markers = ('A203_RETRY_HEADER_CANARY', 'A203_CODING_CANARY',
               'A203_RAW_READ_CANARY', 'A203_CLOSE_CANARY', 'A203_OLD_HEADER_CANARY',
               'A203_NEW_HEADER_CANARY', 'A203_RAW_BODY_CANARY', 'A203_CONTEXT_CANARY')
    event_log.context['_secrets'] += markers
    event_log.context['file'] = markers[-1]
    original_args = None
    primary = None
    if kind == 'retry-after':
        items = [response(503, headers=[('Retry-After', markers[0])]), response()]
    elif kind == 'coding':
        items = [response(headers=[('Content-Encoding', markers[1])])]
    elif kind == 'read':
        primary = ReadTimeoutError(None, 'https://user:password@invalid/' + markers[2], markers[2])
        original_args = primary.args
        items = [response(read_error=primary)]
    elif kind == 'cleanup':
        primary = OSError(markers[3])
        original_args = primary.args
        items = [response(close_error=primary)]
    else:
        items = [response(503, markers[-2]), response()]
    state = transport(gathering, monkeypatch, [*items, response(body=EMPTY_CSV)])
    monkeypatch.setattr(gathering, 'header', {'User-Agent': markers[4]})
    monkeypatch.setattr(gathering, 'get_next_header', lambda previous: {'User-Agent': markers[5]})
    context = event_log.context if with_context else None
    raw_events = []
    original_emit = event_log.logger.emit_event

    def emit(context, level, event, message, fields):
        raw_events.append((level, event, message, (fields.get('error') or {}).get('message')))
        return original_emit(context, level, event, message, fields)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    if kind in ('retry-after', 'header-rotation'):
        assert_frame(query(gathering, logging_context=context))
        assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]
    else:
        expected_type = ValueError if kind == 'coding' else type(primary)
        with pytest.raises(expected_type) as caught:
            query(gathering, max_retries=1, logging_context=context)
        if primary is not None:
            assert caught.value is primary and primary.args == original_args
        assert len(state.calls) == 1 and state.sleeps == []
    captured = capsys.readouterr()
    combined = captured.out + captured.err + json.dumps(raw_events, ensure_ascii=False)
    combined += json.dumps(event_log.events, ensure_ascii=False)
    assert all(marker not in combined for marker in markers)
    assert all(raw not in combined for raw in ('http://', 'https://', 'user:password', 'User-Agent'))
    assert all(len(line) <= 256 for line in (captured.out + captured.err).splitlines())
    assert all(len(message) <= 256 and (error is None or len(error) <= 256)
               for _, _, message, error in raw_events)
    assert bool(raw_events) is with_context and bool(event_log.events) is with_context
    if with_context:
        assert captured.out == ''


def test_final_admission_sample_crossing_deadline_does_not_call_get(
        gathering, monkeypatch, event_log):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])
    admission = SimpleNamespace(active=False, samples=0)
    original = gathering._a2_diagnostic

    def diagnostic(context, level, event, message, fields, primary=None):
        result = original(context, level, event, message, fields, primary)
        if event == 'a2_query_attempt_started':
            admission.active = True
        return result

    def perf_counter():
        if not admission.active:
            return 0.0
        admission.samples += 1
        return 179.999 if admission.samples == 1 else 180.0

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    gathering.time.perf_counter = perf_counter
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert state.calls == [] and state.sleeps == []
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 0)


def test_header_preparation_overhead_is_checked_before_first_get(
        gathering, monkeypatch, event_log):
    state = transport(gathering, monkeypatch, [response(), response(body=EMPTY_CSV)])

    class ExpensiveHeader(dict):
        def items(self):
            state.elapsed = 180.0
            return super().items()

    monkeypatch.setattr(gathering, 'header', ExpensiveHeader({'User-Agent': 'AGENT_A'}))
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert state.calls == [] and state.sleeps == []
    assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 0)


def test_raw_encoding_header_memory_error_remains_native_memory_failure(
        gathering, monkeypatch, event_log):
    primary = MemoryError('A2_HEADER_MEMORY_CANARY')

    class FailingHeaders(HTTPHeaderDict):
        def getlist(self, name):
            if name.lower() == 'content-encoding':
                raise primary
            return super().getlist(name)

    item = response()
    item.raw.headers = FailingHeaders(item.raw.headers)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(MemoryError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_HEADER_MEMORY_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert item.raw.reads == [] and item.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_MEMORY_ERROR', 'MEMORY_ERROR', 'REQUEST_FAILED', 1)


def test_retry_after_header_memory_error_cannot_replace_current_http_primary(
        gathering, monkeypatch, event_log):
    secondary = MemoryError('A2_RETRY_HEADER_MEMORY_CANARY')

    class FailingHeaders(HTTPHeaderDict):
        def getlist(self, name):
            if name.lower() == 'retry-after':
                raise secondary
            return super().getlist(name)

    item = response(503)
    item.raw.headers = FailingHeaders(item.raw.headers)
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.HTTPError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value.response is item and caught.value.args == ('A2_ISS_HTTP_STATUS',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert item.raw.reads == [] and item.raw.close_calls == 1
    terminal = event_log.events[-1]
    assert (terminal['event'], terminal['level'], terminal['error']['code'],
            terminal['error']['category'], terminal['error']['attempt']) == (
        'a2_query_failed', 'ERROR', 'A2_ISS_HTTP_STATUS', 'SOURCE', 1)
    assert terminal['error']['final'] is True and terminal['error']['retryable'] is False
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)


def test_obtained_response_and_distinct_exception_response_are_both_closed(
        gathering, monkeypatch, event_log):
    attached = response(503)
    primary = requests.exceptions.ReadTimeout('A2_DISTINCT_RESPONSE_CANARY', response=attached)
    obtained = response(read_error=primary)
    state = transport(gathering, monkeypatch, [obtained, response(body=EMPTY_CSV)])
    with pytest.raises(requests.exceptions.ReadTimeout) as caught:
        query(gathering, max_retries=1, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A2_DISTINCT_RESPONSE_CANARY',)
    assert len(state.calls) == 1 and state.sleeps == []
    assert obtained.raw.close_calls == 1 and attached.raw.close_calls == 1
    assert_terminal(event_log, 'A2_ISS_TIMEOUT', 'TIMEOUT', 'RETRY_EXHAUSTED', 1, limit=1)


@pytest.mark.parametrize('header', [
    'Sat, 02 Mar 2024 12:00:07 GMT',
    'Saturday, 02-Mar-24 12:00:07 GMT',
    'Sat Mar  2 12:00:07 2024',
])
def test_http_date_parsing_is_independent_of_callers_lc_time(
        gathering, monkeypatch, header):
    native_datetime = gathering.datetime.datetime

    class NonEnglishClock(native_datetime):
        @classmethod
        def strptime(cls, text, date_format):
            if any(token in date_format for token in ('%a', '%A', '%b', '%B')):
                raise ValueError('A2_SYNTHETIC_NON_ENGLISH_LC_TIME')
            return native_datetime.strptime(text, date_format)

    monkeypatch.setattr(gathering.datetime, 'datetime', NonEnglishClock)
    state = transport(gathering, monkeypatch, [response(503, headers=[('Retry-After', header)]), response(), response(body=EMPTY_CSV)])
    assert_frame(query(gathering))
    assert len(state.calls) == 3 and state.sleeps == [7, 3, 3]


@pytest.mark.parametrize('header,retry', [
    ('Fri, 02 Mar 2024 12:00:07 GMT', False),
    ('Friday, 02-Mar-24 12:00:07 GMT', False),
    ('Fri Mar  2 12:00:07 2024', True),
])
def test_http_date_weekday_must_match_calendar_before_server_delay_is_used(
        gathering, monkeypatch, header, retry):
    # Wrong weekday invalidates the date. Comma-bearing values are ambiguous;
    # non-comma asctime is ordinary-invalid and uses the local backoff.
    item = response(503, headers=[('Retry-After', header)])
    state = transport(gathering, monkeypatch, [item, response(), response(body=EMPTY_CSV)])
    if retry:
        assert_frame(query(gathering))
        assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]
    else:
        with pytest.raises(requests.exceptions.HTTPError) as caught:
            query(gathering)
        assert caught.value.response is item
        assert len(state.calls) == 1 and state.sleeps == []


@pytest.mark.parametrize('status,exception_type,outcome', [
    (302, requests.exceptions.TooManyRedirects, 'REDIRECT_REJECTED'),
    (404, requests.exceptions.HTTPError, 'HTTP_NONRETRYABLE'),
    (503, requests.exceptions.HTTPError, 'RETRY_EXHAUSTED'),
])
def test_rejected_status_never_uses_eager_or_raw_body(
        gathering, monkeypatch, event_log, status, exception_type, outcome):
    class BodyTrap(requests.Response):
        @property
        def content(self):
            pytest.fail('Rejected status accessed eager content')

        @property
        def text(self):
            pytest.fail('Rejected status accessed eager text')

        def json(self, *args, **kwargs):
            pytest.fail('Rejected status accessed JSON')

    item = BodyTrap()
    item.status_code = status
    item.raw = RawBody(b'A2_REJECTED_BODY_CANARY', read_error=AssertionError('body trap'))
    item._content_consumed = False
    state = transport(gathering, monkeypatch, [item, response(body=EMPTY_CSV)])
    with pytest.raises(exception_type) as caught:
        query(gathering, max_retries=1, logging_context=event_log.context)
    assert caught.value.response is item
    assert len(state.calls) == 1 and state.sleeps == []
    assert item.raw.reads == [] and item.raw.close_calls == 1
    code = 'A2_ISS_REDIRECT_REJECTED' if status == 302 else 'A2_ISS_HTTP_STATUS'
    assert_terminal(event_log, code, 'SOURCE', outcome, 1, limit=1)


@pytest.mark.parametrize('factory,category', [
    (lambda: requests.exceptions.ConnectionError('A2_UNTYPED_CONNECTION'), 'SOURCE'),
    (lambda: requests.exceptions.ConnectionError(socket.gaierror(socket.EAI_NONAME, 'A2_PERMANENT_DNS')), 'SOURCE'),
    (lambda: requests.exceptions.ProxyError(requests.exceptions.Timeout('A2_NESTED_TIMEOUT')), 'CONFIG'),
    (lambda: requests.exceptions.InvalidURL('A2_INVALID_URL'), 'CONFIG'),
    (lambda: requests.exceptions.InvalidHeader('A2_INVALID_HEADER'), 'CONFIG'),
    (lambda: OSError('A2_UNTYPED_OS_ERROR'), 'SOURCE'),
    (lambda: RuntimeError('A2_UNKNOWN_IMPLEMENTATION'), 'UNKNOWN'),
])
def test_permanent_request_factory_has_exact_closed_terminal_category(
        gathering, monkeypatch, event_log, factory, category):
    primary = factory()
    original_args = primary.args
    state = transport(gathering, monkeypatch, [primary, response(), response(body=EMPTY_CSV)])
    with pytest.raises(type(primary)) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == original_args
    assert len(state.calls) == 1 and state.sleeps == []
    assert_terminal(event_log, 'A2_ISS_REQUEST_ERROR', category, 'REQUEST_FAILED', 1)


@pytest.mark.parametrize('failure', ['throw', 'unconfirmed', 'unhealthy'])
def test_new_zero_attempt_deadline_timeout_survives_failed_terminal_delivery(
        gathering, monkeypatch, event_log, capsys, failure):
    state = transport(gathering, monkeypatch, [response(body=EMPTY_CSV)])
    entry = [0.0]
    gathering.time.perf_counter = lambda: entry.pop() if entry else 180.0
    originals = []
    original_diagnostic = gathering._a2_diagnostic
    original_emit = event_log.logger.emit_event

    def diagnostic(context, level, event, message, fields, primary=None):
        if event == 'a2_query_failed':
            originals.append(primary)
        return original_diagnostic(context, level, event, message, fields, primary)

    def emit(context, level, event, message, fields):
        if event == 'a2_query_failed':
            if failure == 'throw':
                raise OSError('A2_TERMINAL_DELIVERY_CANARY')
            event_log.confirmed = failure != 'unconfirmed'
            event_log.healthy = failure != 'unhealthy'
        return original_emit(context, level, event, message, fields)

    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context)
    assert len(originals) == 1 and caught.value is originals[0]
    assert caught.value.args == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert caught.value.__notes__ == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    assert state.calls == [] and state.sleeps == []
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)
    if failure != 'throw':
        assert_terminal(event_log, 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED', 0)
    output = capsys.readouterr()
    assert output.out == ''
    assert output.err.splitlines() == ['A2_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    assert 'A2_TERMINAL_DELIVERY_CANARY' not in output.out + output.err
