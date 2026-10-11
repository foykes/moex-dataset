"""Public-entrypoint, literal transport oracles. Cooperative timing only."""

import errno
from email.utils import formatdate
import importlib.util
from io import StringIO
import json
from pathlib import Path
import socket
import ssl
import urllib.error

import pandas as pd
import pytest

import run_logging


ROOT = Path(__file__).resolve().parents[3]
BASE = '42830d91b4c701b899cbadb5ebdf8829eec6b5cf'
ISIN = 'RU0009029540'
SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=RU0009029540&iss.meta=off'
DETAIL_URL = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
SEARCH = {'securities': {'columns': ['isin', 'secid'], 'data': [[ISIN, 'SBER']]}}
DETAIL = {'dividends': {'columns': ['currencyid', 'value', 'registryclosedate'],
    'data': [['RUB', 33.3, '2024-07-11'], ['USD', 1.5, '2024-07-11'],
             ['RUB', 33.3, '2024-07-11']]}}
EXPECTED = [[ISIN, 'SBER', '2024-07-11', 33.3, 'RUB'],
            [ISIN, 'SBER', '2024-07-11', 1.5, 'USD'],
            [ISIN, 'SBER', '2024-07-11', 33.3, 'RUB']]
CANARY = 'C2A1_SYNTHETIC_SECRET_291'


class Reply(StringIO):
    def __init__(self, payload, *, read_error=None, close_error=None, after_read=None):
        super().__init__(json.dumps(payload) if type(payload) is dict else payload)
        self.read_error = read_error
        self.close_error = close_error
        self.after_read = after_read
        self.close_calls = 0

    def read(self, *args):
        if self.after_read:
            self.after_read()
        if self.read_error:
            raise self.read_error
        return super().read(*args)

    def close(self):
        self.close_calls += 1
        super().close()
        if self.close_error:
            raise self.close_error


@pytest.fixture
def model(monkeypatch):
    spec = importlib.util.spec_from_file_location('c2a1_dividends_fixture', ROOT / 'dividends.py')
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
    session = run_logging.configure_logging(tmp_path / 'logs', run_id='c2a1-disposable',
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    yield session
    run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def source(monkeypatch, module, *, search=None, detail=None):
    queues = {SEARCH_URL: list(search) if search is not None else [Reply(SEARCH)],
              DETAIL_URL: list(detail) if detail is not None else [Reply(DETAIL)]}
    calls = []
    def opened(url, *, timeout=None):
        calls.append((url, timeout))
        assert url in queues and queues[url], 'Unexpected fixture request'
        result = queues[url].pop(0)
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    return calls


def events(context):
    assert run_logging.flush_logging(context)['confirmed']
    values = [json.loads(line) for line in
        (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    assert all(event['fields'] == {} for event in values)
    return values


def failed(context):
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']
    return report


def writers(monkeypatch, module, *, prepare=None):
    calls = []
    def read(path):
        if prepare:
            prepare()
        return pd.DataFrame([['SBER', ISIN]], columns=['TRADE_CODE', 'ISIN'])
    monkeypatch.setattr(module.pd, 'read_excel', read)
    for name in ('to_excel', 'to_csv'):
        monkeypatch.setattr(pd.DataFrame, name,
            lambda *a, method=name, **kw: calls.append((method, a, kw)))
    return calls


def http(status, *, retry_after=None, body=None):
    headers = {} if retry_after is None else {'Retry-After': retry_after}
    return urllib.error.HTTPError('https://user:' + CANARY + '@example.invalid/?token=' + CANARY,
        status, 'fixture ' + CANARY, headers, body)


def test_clean_success_has_socket_limits_and_literal_rows(monkeypatch, model, context):
    module, clock, waits = model
    calls = source(monkeypatch, module)
    assert module.div_loader(ISIN, 'SBER', logging_context=context) is None
    assert calls == [(SEARCH_URL, 10), (DETAIL_URL, 10)] and waits == []
    assert module.divs_all == EXPECTED
    assert [event['event'] for event in events(context)] == [
        'dividend_identity_search_started', 'dividend_identity_selected',
        'dividend_request_started', 'dividend_loader_returned']


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('kind', ['timeout', 'reset', '429', '500', '502', '503', '504'])
def test_transient_then_success_repeats_only_its_endpoint(monkeypatch, model, endpoint, kind):
    module, clock, waits = model
    error = (TimeoutError('fixture') if kind == 'timeout' else
             ConnectionResetError(errno.ECONNRESET, 'fixture') if kind == 'reset' else http(int(kind)))
    payload = SEARCH if endpoint == 'search' else DETAIL
    calls = source(monkeypatch, module, **{endpoint: [error, Reply(payload)]})
    assert module.div_loader(ISIN, 'SBER') is None
    assert [url for url, timeout in calls] == (
        [SEARCH_URL, SEARCH_URL, DETAIL_URL] if endpoint == 'search' else
        [SEARCH_URL, DETAIL_URL, DETAIL_URL])
    assert waits == [1] and all(timeout == 10 for url, timeout in calls)
    assert module.divs_all == EXPECTED


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('wrapped', [False, True])
def test_exhaustion_keeps_last_original_and_never_writes(
        monkeypatch, model, context, endpoint, wrapped):
    module, clock, waits = model
    errors = [TimeoutError('attempt ' + str(i)) for i in (1, 2, 3)]
    if wrapped:
        errors = [urllib.error.URLError(error) for error in errors]
    calls = source(monkeypatch, module, **{endpoint: errors})
    exports = writers(monkeypatch, module)
    with pytest.raises(type(errors[-1])) as caught:
        module.main(logging_context=context)
    assert caught.value is errors[-1]
    assert [url for url, timeout in calls] == (
        [SEARCH_URL] * 3 if endpoint == 'search' else [SEARCH_URL] + [DETAIL_URL] * 3)
    assert waits == [1, 2] and exports == [] and module.divs_all == []
    record = events(context)[-1]
    assert record['error']['category'] == 'TIMEOUT'
    assert record['error']['attempt'] == 3 and record['error']['final']
    assert record['retries'] == {'attempt': 3, 'limit': 3}
    assert 'ATTEMPTS_EXHAUSTED' in record['message']
    failed(context)


ERRORS = [
    pytest.param(lambda: http(401), 'ACCESS', 1, id='401'),
    pytest.param(lambda: http(403), 'ACCESS', 1, id='403'),
    pytest.param(lambda: http(400), 'SOURCE', 1, id='400'),
    pytest.param(lambda: http(404), 'SOURCE', 1, id='404'),
    pytest.param(lambda: http(501), 'SOURCE', 1, id='501'),
    pytest.param(lambda: http(505), 'SOURCE', 1, id='505'),
    pytest.param(lambda: http(429), 'QUOTA_429', 3, id='429'),
    pytest.param(lambda: http(503), 'SOURCE', 3, id='503'),
    pytest.param(lambda: socket.timeout('fixture'), 'TIMEOUT', 3, id='socket-timeout'),
    pytest.param(lambda: urllib.error.URLError(TimeoutError('fixture')), 'TIMEOUT', 3, id='reason-timeout'),
    pytest.param(lambda: ConnectionAbortedError(errno.ECONNABORTED, 'fixture'), 'SOURCE', 3, id='abort'),
    pytest.param(lambda: OSError(errno.ECONNRESET, 'fixture'), 'SOURCE', 3, id='reset-errno'),
    pytest.param(lambda: socket.gaierror(socket.EAI_AGAIN, 'fixture'), 'SOURCE', 3, id='dns-again'),
    pytest.param(lambda: socket.gaierror(socket.EAI_NONAME, 'fixture'), 'SOURCE', 1, id='dns-permanent'),
    pytest.param(lambda: ssl.SSLCertVerificationError(1, 'fixture'), 'SOURCE', 1, id='certificate'),
    pytest.param(lambda: ssl.SSLError(1, 'fixture'), 'SOURCE', 1, id='tls'),
    pytest.param(lambda: OSError('unknown'), 'SOURCE', 1, id='unknown'),
    pytest.param(lambda: urllib.error.URLError('timeout text'), 'SOURCE', 1, id='string-reason'),
]


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('factory,category,attempts', ERRORS)
def test_classified_public_failure_policy(monkeypatch, model, context, endpoint, factory, category, attempts):
    module, clock, waits = model
    primary = factory()
    calls = source(monkeypatch, module, **{endpoint: [primary] * attempts})
    with pytest.raises(type(primary)) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary
    assert [url for url, timeout in calls] == (
        [SEARCH_URL] * attempts if endpoint == 'search' else [SEARCH_URL] + [DETAIL_URL] * attempts)
    assert waits == ([1, 2] if attempts == 3 else []) and module.divs_all == []
    error = events(context)[-1]['error']
    assert error['category'] == category and error['final'] and not error['retryable']
    assert error['attempt'] == attempts
    failed(context)


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('status', [429, 503])
@pytest.mark.parametrize('header,expected', [('0', [1, 2]), ('3', [3, 3]), ('10', [10, 10]),
    ('invalid', [1, 2]), ('-1', [1, 2]), ('1.5', [1, 2])])
def test_retry_after_seconds_and_invalid_fallback(monkeypatch, model, endpoint, status, header, expected):
    module, clock, waits = model
    primary = http(status, retry_after=header)
    source(monkeypatch, module, **{endpoint: [primary] * 3})
    with pytest.raises(urllib.error.HTTPError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert caught.value is primary and waits == expected


@pytest.mark.parametrize('offset,expected', [(4, [4, 2]), (-10, [1, 2]), (0, [1, 2])])
def test_retry_after_http_date_uses_separate_wall_clock(monkeypatch, model, offset, expected):
    module, clock, waits = model
    primary = http(429, retry_after=formatdate(clock[1] + offset, usegmt=True))
    source(monkeypatch, module, detail=[primary] * 3)
    with pytest.raises(urllib.error.HTTPError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert caught.value is primary and waits == expected


@pytest.mark.parametrize('header', ['11', '99999999999999999999999999999999999999999'])
def test_large_retry_after_is_not_clamped_or_repeated(monkeypatch, model, header):
    module, clock, waits = model
    primary = http(429, retry_after=header)
    calls = source(monkeypatch, module, detail=[primary])
    with pytest.raises(urllib.error.HTTPError) as caught:
        module.div_loader(ISIN, 'SBER')
    assert caught.value is primary and waits == []
    assert [url for url, timeout in calls] == [SEARCH_URL, DETAIL_URL]


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('phase', ['read', 'parse'])
def test_late_return_is_rejected_without_next_request_or_append(monkeypatch, model, endpoint, phase):
    module, clock, waits = model
    advance = lambda: clock.__setitem__(0, clock[0] + 35)
    response = Reply(SEARCH if endpoint == 'search' else DETAIL,
        after_read=advance if phase == 'read' else None)
    calls = source(monkeypatch, module, **{endpoint: [response]})
    if phase == 'parse':
        name = '_select_secid' if endpoint == 'search' else '_parse_dividend_rows'
        original = getattr(module, name)
        def parse(*args):
            result = original(*args)
            advance()
            return result
        monkeypatch.setattr(module, name, parse)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
        module.div_loader(ISIN, 'SBER')
    assert [url for url, timeout in calls] == ([SEARCH_URL] if endpoint == 'search' else
                                               [SEARCH_URL, DETAIL_URL])
    assert response.close_calls == 1 and waits == [] and module.divs_all == []


def test_budget_includes_barrier_checkpoint(monkeypatch, model, context):
    module, clock, waits = model
    source(monkeypatch, module)
    original = run_logging.flush_logging
    def flush(session):
        result = original(session)
        clock[0] = 35
        return result
    with monkeypatch.context() as fault:
        fault.setattr(run_logging, 'flush_logging', flush)
        with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
            module.div_loader(ISIN, 'SBER', logging_context=context)
    assert module.divs_all == []
    assert events(context)[-1]['error']['category'] == 'TIMEOUT'
    failed(context)


def test_source_budget_starts_before_scope_and_reduces_socket_timeout(monkeypatch, model, context):
    module, clock, waits = model
    exports = writers(monkeypatch, module, prepare=lambda: clock.__setitem__(0, 599.75))
    calls = source(monkeypatch, module, search=[Reply(SEARCH,
        after_read=lambda: clock.__setitem__(0, 600))])
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
        module.main(logging_context=context)
    assert calls == [(SEARCH_URL, 0.25)] and waits == [] and exports == []
    failed(context)


def test_expired_scope_never_requests_or_writes(monkeypatch, model, context):
    module, clock, waits = model
    exports = writers(monkeypatch, module, prepare=lambda: clock.__setitem__(0, 600))
    calls = source(monkeypatch, module)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
        module.main(logging_context=context)
    assert calls == [] and waits == [] and exports == [] and module.divs_all == []
    assert events(context)[-1]['error']['category'] == 'TIMEOUT'
    failed(context)


@pytest.mark.parametrize('oversleep', [False, True])
def test_budget_exhaustion_during_retry_preserves_network_cause(monkeypatch, model, context, oversleep):
    module, clock, waits = model
    primary = TimeoutError('network original')
    if oversleep:
        def wait(delay):
            waits.append(delay)
            clock[0] = 35
        monkeypatch.setattr(module, '_retry_wait', wait)
        payload = primary
    else:
        payload = Reply(SEARCH, read_error=primary, after_read=lambda: clock.__setitem__(0, 34.5))
    calls = source(monkeypatch, module, search=[payload])
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:') as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is not primary and caught.value.__cause__ is primary
    assert [url for url, timeout in calls] == [SEARCH_URL]
    assert waits == ([1] if oversleep else []) and module.divs_all == []
    failed(context)


def test_read_primary_survives_close_and_retry_diagnostic_failures(monkeypatch, model, context):
    module, clock, waits = model
    primary = TimeoutError('network ' + CANARY)
    reply = Reply(SEARCH, read_error=primary, close_error=OSError('secondary ' + CANARY))
    calls = source(monkeypatch, module, search=[reply])
    emitter = run_logging.emit_event
    def emit(session, level, event, message, fields):
        if event == 'dividend_request_retry':
            raise RuntimeError('diagnostic ' + CANARY)
        return emitter(session, level, event, message, fields)
    monkeypatch.setattr(run_logging, 'emit_event', emit)
    with pytest.raises(TimeoutError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary and reply.close_calls == 1 and waits == []
    assert [url for url, timeout in calls] == [SEARCH_URL] and module.divs_all == []
    assert 'request_phase=READ' in events(context)[-1]['message']
    failed(context)


def test_close_only_timeout_is_io_and_not_retried(monkeypatch, model, context):
    module, clock, waits = model
    primary = TimeoutError('close original')
    reply = Reply(SEARCH, close_error=primary)
    calls = source(monkeypatch, module, search=[reply])
    with pytest.raises(TimeoutError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary and reply.close_calls == 1 and waits == []
    assert [url for url, timeout in calls] == [SEARCH_URL]
    assert events(context)[-1]['error']['category'] == 'IO'
    failed(context)


def test_nested_http_error_closes_inner_body_and_redacts_retry_events(monkeypatch, model, context, capsys):
    module, clock, waits = model
    bodies = [Reply('') for i in range(3)]
    errors = [urllib.error.URLError(http(503, body=body)) for body in bodies]
    calls = source(monkeypatch, module, detail=errors)
    with pytest.raises(urllib.error.URLError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is errors[-1] and [body.close_calls for body in bodies] == [1, 1, 1]
    assert waits == [1, 2] and [url for url, timeout in calls] == [SEARCH_URL] + [DETAIL_URL] * 3
    values = events(context)
    retries = [event for event in values if event['event'] == 'dividend_request_retry']
    assert [event['retries'] for event in retries] == [{'attempt': 1, 'limit': 3}, {'attempt': 2, 'limit': 3}]
    assert all(event['error']['category'] == 'SOURCE' and event['error']['retryable']
               and not event['error']['final'] for event in retries)
    failed(context)
    output = capsys.readouterr()
    stored = ''.join(path.read_text(encoding='utf-8') for path in context['_run_root'].rglob('*.json*'))
    assert CANARY not in stored + output.out + output.err and '?token=' not in stored


def test_decode_return_is_within_same_operation_budget(monkeypatch, model):
    module, clock, waits = model
    source(monkeypatch, module)
    def decode(raw):
        result = json.loads(raw)
        clock[0] += 35
        return result
    monkeypatch.setattr(module, '_decode_json', decode, raising=False)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
        module.div_loader(ISIN, 'SBER')
    assert module.divs_all == [] and waits == []


def test_operation_budget_is_not_reset_by_retry(monkeypatch, model):
    module, clock, waits = model
    first = Reply(SEARCH, read_error=TimeoutError('fixture'),
        after_read=lambda: clock.__setitem__(0, 30))
    calls = source(monkeypatch, module, search=[first, Reply(SEARCH)])
    assert module.div_loader(ISIN, 'SBER') is None
    assert calls == [(SEARCH_URL, 10), (SEARCH_URL, 4), (DETAIL_URL, 10)]
    assert waits == [1] and first.close_calls == 1 and module.divs_all == EXPECTED


@pytest.mark.parametrize('phase', ['before-operation', 'retry-logging', 'before-export'])
def test_logging_time_cannot_start_next_action_after_budget(monkeypatch, model, context, phase):
    module, clock, waits = model
    primary = TimeoutError('network original')
    calls = source(monkeypatch, module, search=[primary] if phase == 'retry-logging' else None)
    exports = writers(monkeypatch, module)
    original = module._dividend_event
    target = {'before-operation': 'dividend_identity_search_started',
              'retry-logging': 'dividend_request_retry', 'before-export': 'dividend_export_input'}[phase]
    def event(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[2] == target:
            clock[0] = 600 if phase == 'before-export' else 35
        return result
    monkeypatch.setattr(module, '_dividend_event', event)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:') as caught:
        module.main(logging_context=context)
    assert exports == [] and waits == []
    assert [url for url, timeout in calls] == ([] if phase == 'before-operation' else
        [SEARCH_URL] if phase == 'retry-logging' else [SEARCH_URL, DETAIL_URL])
    if phase == 'retry-logging':
        assert caught.value.__cause__ is primary
    failed(context)


def test_one_source_deadline_covers_all_instruments(monkeypatch, model, context):
    module, clock, waits = model
    scope = [['T' + str(i), 'RU' + str(i).zfill(10)] for i in range(11)]
    monkeypatch.setattr(module.pd, 'read_excel', lambda path:
        pd.DataFrame(scope, columns=['TRADE_CODE', 'ISIN']))
    exports = []
    for name in ('to_excel', 'to_csv'):
        monkeypatch.setattr(pd.DataFrame, name, lambda *a, **kw: exports.append(a))
    calls = []
    def opened(url, *, timeout=None):
        calls.append((url, timeout))
        number = (len(calls) - 1) // 2
        ticker, isin = scope[number]
        searching = len(calls) % 2 == 1
        expected_url = ('https://iss.moex.com/iss/securities.json?q=' + isin + '&iss.meta=off'
            if searching else 'https://iss.moex.com/iss/securities/' + ticker + '/dividends.json')
        assert url == expected_url
        payload = {'securities': {'columns': ['secid', 'isin'], 'data': [[ticker, isin]]}} if searching else {
            'dividends': {'columns': ['registryclosedate', 'value', 'currencyid'],
                          'data': [['2024-07-11', 1.5, 'RUB']]}}
        return Reply(payload, after_read=lambda: clock.__setitem__(0, clock[0] + 29))
    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:'):
        module.main(logging_context=context)
    assert len(calls) == 21 and calls[-1][1] == 10
    assert module.divs_all == [[isin, ticker, '2024-07-11', 1.5, 'RUB'] for ticker, isin in scope[:10]]
    assert waits == [] and exports == []
    failed(context)


@pytest.mark.parametrize('suppressed', [False, True])
def test_nested_context_is_cycle_safe_and_respects_suppression(monkeypatch, model, context, suppressed):
    module, clock, waits = model
    primary = urllib.error.URLError('unknown wrapper')
    nested = TimeoutError('fixture')
    primary.__context__ = nested
    primary.__suppress_context__ = suppressed
    nested.__context__ = primary
    attempts = 1 if suppressed else 3
    calls = source(monkeypatch, module, search=[primary] * attempts)
    with pytest.raises(urllib.error.URLError) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary and len(calls) == attempts
    assert waits == ([] if suppressed else [1, 2])
    assert events(context)[-1]['error']['category'] == ('SOURCE' if suppressed else 'TIMEOUT')
    failed(context)


@pytest.mark.parametrize('kind', ['json', 'unicode', 'schema'])
def test_non_network_failures_never_retry(monkeypatch, model, context, kind):
    module, clock, waits = model
    response = Reply('{' if kind == 'json' else {'dividends': {'columns': [], 'data': []}},
        read_error=UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'fixture') if kind == 'unicode' else None)
    calls = source(monkeypatch, module, detail=[response])
    expected = json.JSONDecodeError if kind == 'json' else UnicodeDecodeError if kind == 'unicode' else ValueError
    with pytest.raises(expected):
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert [url for url, timeout in calls] == [SEARCH_URL, DETAIL_URL]
    assert response.close_calls == 1 and waits == [] and module.divs_all == []
    assert events(context)[-1]['error']['category'] == 'SOURCE'
    failed(context)


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('kind', ['timeout-http', 'tls-http', 'unicode-timeout'])
def test_primary_typed_failure_precedes_historical_causes(monkeypatch, model, context, endpoint, kind):
    module, clock, waits = model
    if kind == 'timeout-http':
        primary = TimeoutError('current timeout')
        primary.__cause__ = http(503, retry_after='99')
    elif kind == 'tls-http':
        certificate = ssl.SSLCertVerificationError('current certificate failure')
        certificate.__context__ = http(403)
        primary = urllib.error.URLError(certificate)
    else:
        primary = UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'current decode failure')
        primary.__cause__ = TimeoutError('historical timeout')
    calls = source(monkeypatch, module, **{endpoint: [primary] * 3})
    with pytest.raises(type(primary)) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    attempts = 3 if kind == 'timeout-http' else 1
    expected_urls = ([SEARCH_URL] * attempts if endpoint == 'search' else
                     [SEARCH_URL] + [DETAIL_URL] * attempts)
    assert caught.value is primary and [url for url, timeout in calls] == expected_urls
    assert waits == ([1, 2] if kind == 'timeout-http' else [])
    assert events(context)[-1]['error']['category'] == ('TIMEOUT' if kind == 'timeout-http' else 'SOURCE')
    assert module.divs_all == []
    failed(context)


def test_budget_expiring_between_wait_checkpoint_and_attempt_keeps_cause(monkeypatch, model, context):
    module, clock, waits = model
    primary = TimeoutError('network original')
    calls = source(monkeypatch, module, search=[primary])
    after_wait = []
    def wait(delay):
        waits.append(delay)
        after_wait.append(True)
    def now():
        if after_wait:
            if len(after_wait) == 1:
                after_wait.append(False)
                return 1
            return 35
        return 0
    monkeypatch.setattr(module, '_retry_wait', wait)
    monkeypatch.setattr(module, '_network_clock', now)
    exports = writers(monkeypatch, module)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:') as caught:
        module.main(logging_context=context)
    assert caught.value.__cause__ is primary
    assert calls == [(SEARCH_URL, 10)] and waits == [1] and exports == []
    assert module.divs_all == [] and events(context)[-1]['error']['category'] == 'TIMEOUT'
    failed(context)


@pytest.mark.parametrize('endpoint', ['search', 'detail'])
@pytest.mark.parametrize('phase', ['OPEN', 'READ', 'CLOSE', 'DECODE'])
def test_request_failure_phase_is_literal_and_preserves_envelope(monkeypatch, model, context, endpoint, phase):
    module, clock, waits = model
    payload = SEARCH if endpoint == 'search' else DETAIL
    primary = (UnicodeDecodeError('utf-8', b'\xff', 0, 1, CANARY) if phase == 'DECODE' else
               OSError('close ' + CANARY) if phase == 'CLOSE' else TimeoutError(CANARY))
    original_args = primary.args
    if phase == 'OPEN':
        responses = [primary] * 3
    elif phase == 'READ':
        responses = [Reply(payload, read_error=primary) for i in range(3)]
    elif phase == 'CLOSE':
        responses = [Reply(payload, close_error=primary)]
    else:
        responses = [Reply(payload)]
        original_decode = module._decode_json
        def decode(raw):
            if raw == json.dumps(payload):
                raise primary
            return original_decode(raw)
        monkeypatch.setattr(module, '_decode_json', decode)
    calls = source(monkeypatch, module, **{endpoint: responses})
    exports = writers(monkeypatch, module)
    with pytest.raises(type(primary)) as caught:
        module.main(logging_context=context)
    attempts = 3 if phase in ('OPEN', 'READ') else 1
    assert caught.value is primary and caught.value.args == original_args
    assert [url for url, timeout in calls] == (
        [SEARCH_URL] * attempts if endpoint == 'search' else [SEARCH_URL] + [DETAIL_URL] * attempts)
    assert waits == ([1, 2] if attempts == 3 else []) and exports == [] and module.divs_all == []
    values = events(context)
    diagnostics = [event for event in values if event['event'] in ('dividend_request_retry', 'dividend_loader_failed')]
    assert len(diagnostics) == attempts
    assert all('request_phase=' + phase in event['message'] for event in diagnostics)
    assert all(CANARY not in json.dumps(event) for event in values)
    assert all(event['error']['code'] == ('DIVIDEND_SEARCH_FAILED' if endpoint == 'search' else
               'DIVIDEND_SOURCE_REQUEST_FAILED') for event in diagnostics)
    assert all(event['error']['category'] == ('IO' if phase == 'CLOSE' else
               'SOURCE' if phase == 'DECODE' else 'TIMEOUT') for event in diagnostics)
    assert diagnostics[-1]['error']['final'] and not diagnostics[-1]['error']['retryable']
    assert diagnostics[-1]['error']['attempt'] == attempts
    assert all(reply.close_calls == 1 for reply in responses if isinstance(reply, Reply))
    failed(context)


@pytest.mark.parametrize('remaining', [0.5, 1.0])
def test_infeasible_retry_has_no_planned_warning(monkeypatch, model, context, remaining):
    module, clock, waits = model
    primary = TimeoutError('network original')
    response = Reply(SEARCH, read_error=primary,
        after_read=lambda: clock.__setitem__(0, 35 - remaining))
    calls = source(monkeypatch, module, search=[response])
    exports = writers(monkeypatch, module)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:') as caught:
        module.main(logging_context=context)
    assert caught.value is not primary and caught.value.__cause__ is primary
    assert primary.args == ('network original',)
    assert calls == [(SEARCH_URL, 10)] and waits == [] and response.close_calls == 1
    assert exports == [] and module.divs_all == []
    values = events(context)
    assert not any(event['event'] == 'dividend_request_retry' for event in values)
    assert values[-1]['error']['category'] == 'TIMEOUT'
    assert values[-1]['error']['code'] == 'DIVIDEND_BUDGET_EXHAUSTED'
    assert values[-1]['error']['attempt'] == 1
    failed(context)


@pytest.mark.parametrize('phase', ['OPEN', 'READ'])
def test_planned_retry_expiring_in_diagnostics_does_not_request_or_wait(monkeypatch, model, context, phase):
    module, clock, waits = model
    primary = TimeoutError('network original')
    response = primary if phase == 'OPEN' else Reply(SEARCH, read_error=primary)
    calls = source(monkeypatch, module, search=[response])
    exports = writers(monkeypatch, module)
    original_emit = run_logging.emit_event
    def emit(session, level, event, message, fields):
        receipt = original_emit(session, level, event, message, fields)
        if event == 'dividend_request_retry':
            clock[0] = 35
        return receipt
    monkeypatch.setattr(run_logging, 'emit_event', emit)
    with pytest.raises(TimeoutError, match='^DIVIDEND_BUDGET_EXHAUSTED:') as caught:
        module.main(logging_context=context)
    assert caught.value.__cause__ is primary and primary.args == ('network original',)
    assert calls == [(SEARCH_URL, 10)] and waits == [] and exports == [] and module.divs_all == []
    values = events(context)
    warnings = [event for event in values if event['event'] == 'dividend_request_retry']
    assert len(warnings) == 1 and 'Запланирован повтор' in warnings[0]['message']
    assert 'request_phase=' + phase in warnings[0]['message']
    assert warnings[0]['error']['attempt'] == 1 and warnings[0]['retries'] == {'attempt': 1, 'limit': 3}
    assert warnings[0]['error']['retryable'] and not warnings[0]['error']['final']
    assert values[-1]['error']['category'] == 'TIMEOUT'
    assert values[-1]['error']['code'] == 'DIVIDEND_BUDGET_EXHAUSTED'
    assert values[-1]['error']['attempt'] == 1
    failed(context)


@pytest.mark.parametrize('phase', ['parser', 'barrier'])
def test_post_request_failure_does_not_claim_decode_phase(monkeypatch, model, context, phase):
    module, clock, waits = model
    calls = source(monkeypatch, module)
    primary = ValueError('DIVIDEND_SOURCE_SCHEMA_INVALID: fixture') if phase == 'parser' else RuntimeError('LOGGING_INCOMPLETE')
    if phase == 'parser':
        def parse(*args):
            raise primary
        monkeypatch.setattr(module, '_parse_dividend_rows', parse)
    else:
        original = module._dividend_event
        def event(*args, **kwargs):
            if args[2] == 'dividend_loader_returned':
                raise primary
            return original(*args, **kwargs)
        monkeypatch.setattr(module, '_dividend_event', event)
    with pytest.raises(type(primary)) as caught:
        module.div_loader(ISIN, 'SBER', logging_context=context)
    assert caught.value is primary and calls == [(SEARCH_URL, 10), (DETAIL_URL, 10)]
    assert waits == [] and module.divs_all == []
    assert 'request_phase=' not in events(context)[-1]['message']
    failed(context)
