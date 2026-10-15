"""Guarded offline Requests oracles; no live source or publication evidence."""

import errno
import importlib.util
import json
from email.utils import formatdate
from pathlib import Path
import socket
import ssl

import pandas as pd
import pytest
import requests
from urllib3 import exceptions as ue

import run_logging


ROOT = Path(__file__).resolve().parents[3]
BASE = '3b6e78f11f7681cdcd9c813fff4236a8c63ab677'
URL = 'https://www.dohod.ru/ik/analytics/dividend'
DETAIL = URL + '/aaa'
CANARY = 'C2B_SYNTHETIC_SECRET_713'
DISCOVERY = b'<a href="/ik/analytics/dividend/aaa">AAA</a>'


class Raw:
    def __init__(self, chunks, *, error=None, after_read=None, close_error=None):
        self.chunks = chunks
        self.error = error
        self.after_read = after_read
        self.close_error = close_error
        self.reads = self.closes = self.releases = 0

    def stream(self, size, decode_content=True):
        self.reads += 1
        yield from self.chunks
        if self.after_read:
            self.after_read()
        if self.error is not None:
            raise self.error

    def close(self):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error

    def release_conn(self):
        self.releases += 1


def reply(body=b'overview', *, status=200, retry_after=None, error=None,
          after_read=None, close_error=None, chunks=None):
    # Actual Response.content, iter_content, text, bool and raise_for_status.
    response = requests.Response()
    response.status_code = status
    response.url = URL
    response.reason = 'fixture ' + CANARY
    response.encoding = 'utf-8'
    response.request = requests.Request('GET', URL).prepare()
    if retry_after is not None:
        response.headers['Retry-After'] = retry_after
    response.raw = Raw([body] if chunks is None else chunks, error=error,
                       after_read=after_read, close_error=close_error)
    response.close_calls = 0
    real_close = response.close
    def close():
        response.close_calls += 1
        real_close()
    response.close = close
    return response


@pytest.fixture
def model(monkeypatch):
    spec = importlib.util.spec_from_file_location('c2b_dohod_fixture', ROOT / 'dohodru_data.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    clock = [0.0, 1700000000.0]
    waits = []
    def wait(delay):
        waits.append(delay)
        clock[0] += delay
        clock[1] += delay
    monkeypatch.setattr(module, '_network_clock', lambda: clock[0], raising=False)
    monkeypatch.setattr(module, '_network_wall_clock', lambda: clock[1], raising=False)
    monkeypatch.setattr(module, '_retry_wait', wait, raising=False)
    return module, clock, waits


@pytest.fixture
def context(tmp_path):
    session = run_logging.configure_logging(tmp_path / 'logs', run_id='c2b-disposable',
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    yield session
    run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def events(context):
    assert run_logging.flush_logging(context)['confirmed']
    values = [json.loads(line) for line in
        (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    assert all(value['fields'] == {} for value in values)
    assert CANARY not in json.dumps(values)
    return values


def failed(context):
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']


def source(monkeypatch, module, sequence):
    queue = list(sequence)
    calls = []
    def get(url, *, headers, timeout=None, stream=False):
        calls.append((url, timeout, stream, dict(headers)))
        assert queue, 'Unexpected fixture request'
        expected, result = queue.pop(0)
        assert url == expected
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(module.requests, 'get', get)
    return calls


def _html_bytes(reader, *, encoding=None):
    # C2 keeps literal body/transport oracles across the C3 byte-reader boundary.
    assert encoding is None
    assert not isinstance(reader, (str, bytes))
    assert reader.seekable() and reader.tell() == 0
    raw = reader.read()
    assert type(raw) is bytes
    assert reader.tell() == len(raw)
    assert reader.seek(0) == 0 and reader.tell() == 0
    return raw


def parser(monkeypatch, module, *, detail_count=3, after_parse=None, overview_empty=False):
    calls = []
    def read(reader, *, encoding=None):
        raw = _html_bytes(reader, encoding=encoding)
        calls.append(raw)
        assert type(raw) is bytes  # Original body bytes; real HTML parsing is tested by C3.
        if after_parse:
            after_parse(raw)
        if raw == b'overview':
            frame = pd.DataFrame([['AAA', 10, 'drop', 'drop']], columns=[
                'Тикер', 'Дивиденд', 'Unnamed: 0', 'Капитализация, sorting'])
            return [pd.DataFrame([['unused']]), frame.iloc[:0] if overview_empty else frame]
        assert raw == b'detail'
        rate = pd.DataFrame([[0.1, 0.2, 0.3],
            ['текущая доходность', 'доля от прибыли', 'индекс DSI'],
            [0.4, 0.5, 0.6]])
        years = pd.DataFrame([[2024, 10], [2024, 10]], columns=['Год', 'Сумма'], index=[4, 4])
        payments = pd.DataFrame([['2024-07-11', 10]], columns=['Дата', 'Выплата'], index=[9])
        return [rate, years, payments][:detail_count]
    monkeypatch.setattr(module.pd, 'read_html', read)
    return calls


def writers(monkeypatch, module, tmp_path, *, action=None):
    module.current_path = str(tmp_path)
    calls = []
    def write(frame, path, *, method, **kwargs):
        assert Path(path) == tmp_path / 'datasets/dividends/dohodru' / Path(path).name
        calls.append((method, Path(path).name, kwargs, frame.copy(deep=True)))
        if action:
            action(len(calls), method, Path(path).name)
    for method in ('to_excel', 'to_csv'):
        monkeypatch.setattr(pd.DataFrame, method,
            lambda frame, path, method=method, **kw: write(frame, path, method=method, **kw))
    return calls


def overview(monkeypatch, model, tmp_path, sequence):
    module, _, _ = model
    calls = source(monkeypatch, module, [(URL, item) for item in sequence])
    parsed = parser(monkeypatch, module)
    written = writers(monkeypatch, module, tmp_path)
    return calls, parsed, written


def assert_requests(calls):
    assert all(call[1] == (10, 10) and call[2] is True for call in calls)
    assert all(call[3] == {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'} for call in calls)


@pytest.mark.parametrize('status,attempts,category', [
    (401, 1, 'ACCESS'), (403, 1, 'ACCESS'), (400, 1, 'SOURCE'),
    (404, 1, 'SOURCE'), (501, 1, 'SOURCE'), (429, 3, 'QUOTA_429'),
    (500, 3, 'SOURCE'), (502, 3, 'SOURCE'), (503, 3, 'SOURCE'), (504, 3, 'SOURCE'),
    (101, 1, 'SOURCE'), (302, 1, 'SOURCE')])
def test_http_status_before_body_and_typed_failure(monkeypatch, model, tmp_path,
                                                  context, status, attempts, category):
    module, _, waits = model
    responses = [reply(status=status) for _ in range(attempts)]
    calls, parsed, written = overview(monkeypatch, model, tmp_path, responses)
    with pytest.raises(requests.HTTPError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value.response is responses[-1]
    assert len(calls) == attempts and waits == ([1, 2] if attempts == 3 else [])
    assert parsed == written == []
    assert all(r.raw.reads == 0 and r.close_calls == 1 and
               r.raw.closes == r.raw.releases == 1 for r in responses)
    assert_requests(calls)
    final = [e for e in events(context) if e['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['category'] == category
    assert final[0]['error']['attempt'] == attempts
    assert final[0]['error']['final'] and not final[0]['error']['retryable']
    assert 'request_phase=OPEN' in final[0]['message']
    failed(context)


@pytest.mark.parametrize('factory,retryable,category', [
    (lambda: requests.ConnectTimeout(CANARY), True, 'TIMEOUT'),
    (lambda: requests.ReadTimeout(CANARY), True, 'TIMEOUT'),
    (lambda: requests.ConnectionError(ConnectionResetError(errno.ECONNRESET, CANARY)), True, 'SOURCE'),
    (lambda: requests.ConnectionError(ConnectionAbortedError(errno.ECONNABORTED, CANARY)), True, 'SOURCE'),
    (lambda: requests.ConnectionError(socket.gaierror(socket.EAI_AGAIN, CANARY)), True, 'SOURCE'),
    (lambda: requests.ConnectionError(socket.gaierror(socket.EAI_NONAME, CANARY)), False, 'SOURCE'),
    (lambda: requests.ConnectionError(ConnectionRefusedError(errno.ECONNREFUSED, CANARY)), False, 'SOURCE'),
    (lambda: requests.ConnectionError('timeout reset DNS ' + CANARY), False, 'SOURCE'),
    (lambda: requests.exceptions.SSLError(ConnectionResetError(errno.ECONNRESET, CANARY)), False, 'SOURCE'),
    (lambda: requests.exceptions.ProxyError(requests.ReadTimeout(CANARY)), False, 'SOURCE'),
    (lambda: ssl.SSLError(CANARY), False, 'SOURCE'),
])
def test_typed_open_failures_identity(monkeypatch, model, tmp_path, context,
                                     factory, retryable, category):
    module, _, waits = model
    errors = [factory() for _ in range(3 if retryable else 1)]
    original_args = [e.args for e in errors]
    calls, parsed, written = overview(monkeypatch, model, tmp_path, errors)
    with pytest.raises(type(errors[-1])) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is errors[-1] and [e.args for e in errors] == original_args
    assert len(calls) == len(errors) and waits == ([1, 2] if retryable else [])
    assert parsed == written == []
    assert events(context)[-1]['error']['category'] == category
    failed(context)


@pytest.mark.parametrize('inner,outer,category', [
    (lambda: ue.ReadTimeoutError(None, URL, CANARY), requests.ConnectionError, 'TIMEOUT'),
    (lambda: ue.ProtocolError('fixture ' + CANARY,
        ConnectionResetError(errno.ECONNRESET, CANARY)), requests.exceptions.ChunkedEncodingError, 'SOURCE'),
])
def test_real_content_conversions_partial_bytes_never_parse(monkeypatch, model, tmp_path,
                                                           context, inner, outer, category):
    module, _, waits = model
    inners = [inner() for _ in range(3)]
    responses = [reply(chunks=[b'<partial>'], error=e) for e in inners]
    calls, parsed, written = overview(monkeypatch, model, tmp_path, responses)
    classified = []
    real_policy = module._network_policy
    def policy(error):
        classified.append(error)
        return real_policy(error)
    monkeypatch.setattr(module, '_network_policy', policy)
    with pytest.raises(outer) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is classified[-1] and caught.value.args == (inners[-1],)
    assert len(calls) == 3 and waits == [1, 2] and parsed == written == []
    assert all(r.raw.reads == r.close_calls == r.raw.closes == r.raw.releases == 1 for r in responses)
    assert events(context)[-1]['error']['category'] == category
    assert 'request_phase=READ' in events(context)[-1]['message']
    failed(context)


def test_falsey_error_attached_response_closed_once(monkeypatch, model, tmp_path):
    module, _, waits = model
    response = reply(status=403)
    assert not response
    error = requests.HTTPError(CANARY, response=response)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [error])
    with pytest.raises(requests.HTTPError) as caught:
        module.get_main(URL)
    assert caught.value is error and response.close_calls == 1
    assert response.raw.closes == response.raw.releases == 1
    assert len(calls) == 1 and parsed == written == waits == []


def test_close_only_is_io_without_retry(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    response = reply()
    error = OSError('fixture close ' + CANARY)
    real_close = response.close
    def close():
        real_close()
        raise error
    response.close = close
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [response])
    with pytest.raises(OSError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is error
    assert response.close_calls == 1 and response.raw.closes == 0 and response.raw.releases == 1
    assert len(calls) == 1 and parsed == written == waits == []
    assert events(context)[-1]['error']['category'] == 'IO'
    assert 'request_phase=CLOSE' in events(context)[-1]['message']
    failed(context)


def test_secondary_close_and_logging_preserve_primary(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    primary = requests.exceptions.SSLError(CANARY)
    response = reply(status=403, close_error=OSError('secondary'))
    primary.response = response
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [primary])
    monkeypatch.setattr(run_logging, 'emit_event', lambda *a, **kw: (_ for _ in ()).throw(OSError('diagnostic')))
    with pytest.raises(requests.exceptions.SSLError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is primary and caught.value.args == (CANARY,)
    assert response.close_calls == response.raw.closes == 1 and response.raw.releases == 0
    assert len(calls) == 1 and parsed == written == waits == []


@pytest.mark.parametrize('value,expected', [
    ('4', [4]), ('00000000010', [10]), ('0', [1]), ('-2', [1]),
    ('1.5', [1]), ('invalid', [1]), ('９', [1]), (5, [1]),
    (formatdate(1700000006, usegmt=True), [6]),
    (formatdate(1699999999, usegmt=True), [1])])
def test_retry_after_literal_wait(monkeypatch, model, tmp_path, value, expected):
    module, _, waits = model
    bad, good = reply(status=429, retry_after=value), reply()
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [bad, good])
    frame = module.get_main(URL)
    assert len(frame) == 1 and waits == expected and len(calls) == 2
    assert len(written) == 1 and parsed == [b'overview']
    assert good.close_calls == 1 and good.raw.closes == 0 and good.raw.releases == 1
    assert_requests(calls)


@pytest.mark.parametrize('value', ['11', '9' * 5000, formatdate(1700000011, usegmt=True)])
def test_retry_after_above_ten_refuses_original(monkeypatch, model, tmp_path, context, value):
    module, _, waits = model
    bad = reply(status=503, retry_after=value)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [bad])
    with pytest.raises(requests.HTTPError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value.response is bad and len(calls) == 1
    assert waits == parsed == written == []
    assert not any(e['event'] == 'dohod_request_retry' for e in events(context))
    assert 'RETRY_AFTER_REFUSED' in events(context)[-1]['message']
    failed(context)


def test_current_timeout_cannot_borrow_historical_http(monkeypatch, model, tmp_path):
    module, _, waits = model
    historical = requests.HTTPError('historical', response=reply(status=429, retry_after='11'))
    current = requests.ConnectTimeout(CANARY)
    current.__cause__ = historical
    calls, _, _ = overview(monkeypatch, model, tmp_path, [current, reply()])
    module.get_main(URL)
    assert len(calls) == 2 and waits == [1]


@pytest.mark.parametrize('where', ['read', 'close', 'parser', 'barrier'])
def test_expired_operation_before_export(monkeypatch, model, tmp_path, context, where):
    module, clock, waits = model
    def expire():
        clock[0] = 35
    response = reply(after_read=expire if where == 'read' else None)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [response])
    if where == 'close':
        real_close = response.close
        def close():
            real_close()
            expire()
        response.close = close
    if where == 'parser':
        parser(monkeypatch, module, after_parse=lambda raw: expire())
    if where == 'barrier':
        real_flush = run_logging.flush_logging
        def flush(ctx):
            result = real_flush(ctx)
            expire()
            return result
        monkeypatch.setattr(run_logging, 'flush_logging', flush)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        module.get_main(URL, logging_context=context)
    assert len(calls) == 1 and waits == written == [] and response.close_calls == 1
    assert events(context)[-1]['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED'
    failed(context)


@pytest.mark.parametrize('after_warning', [False, True])
def test_infeasible_retry_no_new_request(monkeypatch, model, tmp_path, context, after_warning):
    module, clock, waits = model
    error = requests.ReadTimeout(CANARY)
    def get(url, **kwargs):
        clock[0] = 0 if after_warning else 34
        raise error
    calls = []
    def recorded(url, **kw):
        calls.append(url)
        return get(url, **kw)
    monkeypatch.setattr(module.requests, 'get', recorded)
    written = writers(monkeypatch, module, tmp_path)
    if after_warning:
        real_emit = run_logging.emit_event
        def emit(*a, **kw):
            result = real_emit(*a, **kw)
            if a[2] == 'dohod_request_retry':
                clock[0] = 35
            return result
        monkeypatch.setattr(run_logging, 'emit_event', emit)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED') as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value.__cause__ is error and calls == [URL] and waits == written == []
    assert sum(e['event'] == 'dohod_request_retry' for e in events(context)) == int(after_warning)
    failed(context)


def test_retry_logging_refusal_preserves_network_error(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    error = requests.ReadTimeout(CANARY)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [error])
    real_emit = run_logging.emit_event
    def emit(*a, **kw):
        if a[2] == 'dohod_request_retry':
            return {'accepted': False, 'confirmed': False}
        return real_emit(*a, **kw)
    monkeypatch.setattr(run_logging, 'emit_event', emit)
    with pytest.raises(requests.ReadTimeout) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is error and len(calls) == 1 and waits == parsed == written == []
    assert 'RETRY_LOGGING_FAILED' in events(context)[-1]['message']
    failed(context)


def test_transport_receipt_barrier_is_not_publication(monkeypatch, model, tmp_path, context):
    module, _, _ = model
    overview(monkeypatch, model, tmp_path, [reply()])
    module.get_main(URL, logging_context=context)
    values = events(context)
    assert [e['event'] for e in values] == ['dohod_request_received']
    assert values[0]['outcome'] == 'RECEIVED'
    assert values[0]['error'] is None
    assert not any(e['outcome'] in ('COMPLETE', 'PUBLISHED') for e in values)


@pytest.mark.parametrize('status', [401, 403])
def test_positional_access_failure_never_reads_body(monkeypatch, model, tmp_path, status):
    module, _, waits = model
    response = reply(status=status)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [response])
    with pytest.raises(requests.HTTPError) as caught:
        module.get_main(URL)
    assert caught.value.response is response
    assert len(calls) == 1 and response.raw.reads == 0 and response.close_calls == 1
    assert parsed == written == waits == []


@pytest.mark.parametrize('temporary', [False, True])
def test_real_name_resolution_wrapper_is_not_connect_timeout(monkeypatch, model, tmp_path,
                                                            context, temporary):
    module, _, waits = model
    code = socket.EAI_AGAIN if temporary else socket.EAI_NONAME
    def error():
        reason = ue.NameResolutionError('fixture.invalid', None, socket.gaierror(code, CANARY))
        return requests.ConnectionError(ue.MaxRetryError(None, URL, reason))
    errors = [error() for _ in range(3 if temporary else 1)]
    calls, _, written = overview(monkeypatch, model, tmp_path, errors)
    with pytest.raises(requests.ConnectionError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is errors[-1] and len(calls) == len(errors) and written == []
    assert waits == ([1, 2] if temporary else [])
    assert events(context)[-1]['error']['category'] == 'SOURCE'
    failed(context)


def test_new_connection_nested_typed_reset_retry(monkeypatch, model, tmp_path):
    module, _, waits = model
    nested = ue.NewConnectionError(None, 'fixture')
    nested.__cause__ = ConnectionResetError(errno.ECONNRESET, CANARY)
    error = requests.ConnectionError(ue.MaxRetryError(None, URL, nested))
    calls, _, _ = overview(monkeypatch, model, tmp_path, [error, reply()])
    module.get_main(URL)
    assert len(calls) == 2 and waits == [1]


def test_retry_remaining_timeout_and_no_real_wait(monkeypatch, model, tmp_path):
    module, clock, waits = model
    error = requests.ConnectTimeout(CANARY)
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            clock[0] = 33
            raise error
        return reply()
    monkeypatch.setattr(module.requests, 'get', get)
    parser(monkeypatch, module)
    writers(monkeypatch, module, tmp_path)
    module.get_main(URL)
    assert waits == [1] and [c['timeout'] for c in calls] == [(10, 10), (1, 1)]


def test_fake_wait_overrun_no_next_request(monkeypatch, model, tmp_path):
    module, clock, _ = model
    error = requests.ReadTimeout(CANARY)
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [error])
    monkeypatch.setattr(module, '_retry_wait', lambda delay: clock.__setitem__(0, 35))
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED') as caught:
        module.get_main(URL)
    assert caught.value.__cause__ is error and len(calls) == 1 and parsed == written == []


@pytest.mark.parametrize('where', ['receipt', 'flush', 'unconfirmed', 'health'])
def test_logging_admission_failure_io_zero_writers(monkeypatch, model, tmp_path, context, where):
    module, _, waits = model
    calls, _, written = overview(monkeypatch, model, tmp_path, [reply()])
    primary = OSError('unregistered diagnostic marker')
    if where == 'receipt':
        real_emit = run_logging.emit_event
        def emit(*a, **kw):
            if a[2] == 'dohod_request_received':
                raise primary
            return real_emit(*a, **kw)
        monkeypatch.setattr(run_logging, 'emit_event', emit)
    elif where == 'flush':
        monkeypatch.setattr(run_logging, 'flush_logging', lambda *a: (_ for _ in ()).throw(primary))
    elif where == 'unconfirmed':
        monkeypatch.setattr(run_logging, 'flush_logging', lambda *a: {'confirmed': False})
    else:
        monkeypatch.setattr(run_logging, 'check_logging_health', lambda *a: {'healthy': False})
    with pytest.raises(RuntimeError, match='LOGGING_INCOMPLETE'):
        module.get_main(URL, logging_context=context)
    assert len(calls) == 1 and waits == written == []
    monkeypatch.undo()
    final = [e for e in events(context) if e['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['category'] == 'IO'
    assert final[0]['error']['code'] == 'LOGGING_INCOMPLETE'
    failed(context)


def test_safe_diagnostics_without_registered_secret(monkeypatch, model, tmp_path, context):
    module, _, _ = model
    marker = 'UNREGISTERED_RAW_EXCEPTION_BODY_837'
    response = reply(status=403)
    response.reason = marker
    response.url = 'https://user:' + marker + '@www.dohod.ru/ik/analytics/dividend?token=' + marker + '#fragment'
    overview(monkeypatch, model, tmp_path, [response])
    with pytest.raises(requests.HTTPError):
        module.get_main(URL, logging_context=context)
    assert marker not in json.dumps(events(context))
    failed(context)


def test_unrelated_context_and_cycle_do_not_guess_retry(monkeypatch, model, tmp_path):
    module, _, waits = model
    error = requests.ConnectionError('timeout reset string')
    error.__context__ = requests.ReadTimeout('historical')
    error.__cause__ = error
    calls, _, _ = overview(monkeypatch, model, tmp_path, [error])
    with pytest.raises(requests.ConnectionError) as caught:
        module.get_main(URL)
    assert caught.value is error and len(calls) == 1 and waits == []


@pytest.mark.parametrize('factory', [requests.exceptions.ContentDecodingError,
    ue.DecodeError, ue.InvalidHeader, ue.TimeoutStateError])
def test_current_permanent_decoding_or_config_failure_beats_historical_timeout(
        monkeypatch, model, tmp_path, context, factory):
    module, _, waits = model
    error = factory('fixture bad encoding/config')
    error.__cause__ = requests.ReadTimeout('historical')
    calls, parsed, written = overview(monkeypatch, model, tmp_path, [error])
    with pytest.raises(factory) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is error and len(calls) == 1 and parsed == written == waits == []
    assert events(context)[-1]['error']['category'] == 'SOURCE'
    failed(context)


@pytest.mark.parametrize('temporary', [False, True])
def test_current_dns_reason_beats_historical_timeout(monkeypatch, model, tmp_path, context, temporary):
    module, _, waits = model
    def error():
        nested = ue.NameResolutionError('fixture.invalid', None,
            socket.gaierror(socket.EAI_AGAIN if temporary else socket.EAI_NONAME, 'current'))
        nested.__cause__ = requests.ReadTimeout('historical')
        return requests.ConnectionError(ue.MaxRetryError(None, URL, nested))
    errors = [error() for _ in range(3 if temporary else 1)]
    calls, parsed, written = overview(monkeypatch, model, tmp_path, errors)
    with pytest.raises(requests.ConnectionError) as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value is errors[-1] and len(calls) == len(errors) and parsed == written == []
    assert waits == ([1, 2] if temporary else [])
    assert events(context)[-1]['error']['category'] == 'SOURCE'
    failed(context)
