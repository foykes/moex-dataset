"""Independent offline membership/value oracles for A3-01 pagination."""

import hashlib
import io
import json
import math
from pathlib import Path
import time as native_time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from urllib3._collections import HTTPHeaderDict


HEADER = 'candles\nopen;close;high;low;value;volume;begin;end\n'
EMPTY = HEADER
FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures/a3/pagination_cases.json'
FIXTURE_SHA = '5c7951bfecd46fe54c484876eb78c4c614cfae89d4f7f6b56d13d524453a63f0'
COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']


class RawBody:
    def __init__(self, content, headers, read_error=None, close_error=None):
        self.buffer = io.BytesIO(content)
        self.headers = HTTPHeaderDict(headers)
        self.read_error = read_error
        self.close_error = close_error
        self.reads = []
        self.close_calls = 0

    def read(self, amt=None, decode_content=False):
        assert type(amt) is int and 0 < amt <= 65536
        assert decode_content is False
        self.reads.append(amt)
        if self.read_error is not None:
            raise self.read_error
        return self.buffer.read(amt)

    def close(self):
        self.close_calls += 1
        self.buffer.close()
        if self.close_error is not None:
            raise self.close_error

    def release_conn(self):
        pass


def response(body=EMPTY, status=200, **raw_options):
    item = requests.Response()
    item.status_code = status
    content = body.encode('utf-8') if type(body) is str else body
    item._content = content
    item.encoding = 'utf-8'
    item.headers['Content-Type'] = 'text/csv; charset=utf-8'
    item.raw = RawBody(content, item.headers.items(), **raw_options)
    return item


@pytest.fixture
def literal_fixture():
    content = FIXTURE.read_bytes()
    assert hashlib.sha256(content).hexdigest() == FIXTURE_SHA
    source = json.loads(content)
    assert len(source['wire_rows']) == len(source['expected_rows']) == 1001
    assert source['expected_rows'][0][6] == '2024-03-01 00:00:00'
    assert source['expected_rows'][1000][6] == '2024-03-01 16:40:00'
    return source


def payload(source, positions):
    return HEADER + ''.join(source['wire_rows'][position] + '\n' for position in positions)


def transport(gathering, monkeypatch, outcomes):
    prepared = iter(outcomes)
    state = SimpleNamespace(calls=[], sleeps=[], elapsed=0.0, after_get=None,
                            after_sleep=None, frozen=False)

    def get(url, headers, timeout, verify=None, allow_redirects=None, stream=None):
        parsed = urlsplit(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        state.calls.append(dict(url=url, path=parsed.path, params=params,
                                start=int(params.get('start', ['0'])[0]),
                                headers=dict(headers), timeout=timeout, verify=verify,
                                allow_redirects=allow_redirects, stream=stream))
        try:
            outcome = next(prepared)
        except StopIteration:
            pytest.fail('Unexpected request outside the literal page sequence')
        if state.after_get is not None:
            state.after_get()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def sleep(seconds):
        state.sleeps.append(seconds)
        if not state.frozen:
            state.elapsed += seconds
        if state.after_sleep is not None:
            state.after_sleep()

    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'header', {'User-Agent': 'AGENT_A'})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': 'AGENT_B'})
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(
        sleep=sleep, perf_counter=lambda: state.elapsed, time=lambda: 1709380800.0,
        gmtime=native_time.gmtime))
    return state


def query(gathering, **kwargs):
    return gathering.moex_query('FIXTURE_SHARE', 'Акции', '2024-03-01', '2024-03-01',
                                1, **kwargs)


def assert_rows(frame, expected, dtypes=None):
    if not expected:
        assert frame.empty and frame.columns.tolist() == []
        return
    assert frame.columns.tolist() == COLUMNS
    assert frame.index.tolist() == list(range(len(expected)))
    for position, column in enumerate(COLUMNS):
        assert frame[column].tolist() == [row[position] for row in expected]
    if dtypes is not None:
        assert frame.dtypes.astype(str).tolist() == dtypes


@pytest.mark.parametrize('sizes,offsets,total', [
    ([0], [0], 0),
    ([1, 0], [0, 1], 1),
    ([500, 0], [0, 500], 500),
    ([500, 1, 0], [0, 500, 501], 501),
    ([500, 500, 0], [0, 500, 1000], 1000),
    ([500, 500, 1, 0], [0, 500, 1000, 1001], 1001),
    ([250, 250, 0], [0, 250, 500], 500),
])
def test_red_literal_membership_and_terminal(gathering, monkeypatch, literal_fixture,
                                           sizes, offsets, total):
    responses = []
    cursor = 0
    for size in sizes:
        responses.append(response(payload(literal_fixture, range(cursor, cursor + size))))
        cursor += size
    state = transport(gathering, monkeypatch, responses)
    result = query(gathering)
    assert_rows(result, literal_fixture['expected_rows'][:total], literal_fixture['dtypes'])
    assert [item['start'] for item in state.calls] == offsets
    assert state.sleeps == [3] * len(sizes)
    assert all(item.raw.close_calls == 1 for item in responses)


@pytest.mark.parametrize('kind', ['tls', 'schema', 'http-exhausted'])
def test_red_terminal_failure_never_returns_prefix(gathering, monkeypatch, literal_fixture,
                                                  event_log, kind):
    primary = requests.exceptions.SSLError('A3_TERMINAL_TLS_CANARY')
    tail = ([primary] if kind == 'tls' else [response('not_candles\n')]
            if kind == 'schema' else [response(status=503) for _ in range(5)])
    state = transport(gathering, monkeypatch,
                      [response(payload(literal_fixture, [0])), *tail])
    expected_type = requests.exceptions.SSLError if kind == 'tls' else (
        ValueError if kind == 'schema' else requests.exceptions.HTTPError)
    with pytest.raises(expected_type) as caught:
        query(gathering, logging_context=event_log.context)
    if kind == 'tls':
        assert caught.value is primary
        assert primary.args == ('A3_TERMINAL_TLS_CANARY',)
    assert len(state.calls) == (6 if kind == 'http-exhausted' else 2)
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


@pytest.mark.parametrize('pages,code', [
    (([0, 1], [0, 1]), 'A3_ISS_PAGE_OVERLAP'),
    (([0, 1], [1, 2]), 'A3_ISS_PAGE_OVERLAP'),
    (([0, 1], [3, 2]), 'A3_ISS_PAGE_ORDER_INVALID'),
    (([1, 0],), 'A3_ISS_PAGE_ORDER_INVALID')])
def test_red_progress_refuses_repeat_overlap_and_order(gathering, monkeypatch,
                                                     literal_fixture, pages, code):
    state = transport(gathering, monkeypatch,
                      [response(payload(literal_fixture, page)) for page in pages])
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == (code,)
    assert len(state.calls) == len(pages)


def numeric_body(volume, begin='2024-03-01 10:00:00', end='2024-03-01 10:00:59'):
    return HEADER + f'100.0;101.0;102.0;99.0;202000.0;{volume};{begin};{end}\n'


def test_red_unsafe_numeric_promotion_refuses_whole_query(gathering, monkeypatch):
    state = transport(gathering, monkeypatch, [
        response(numeric_body('9007199254740993')),
        response(numeric_body('0.5', '2024-03-01 10:01:00', '2024-03-01 10:01:59')),
        response(),
    ])
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == ('A3_ISS_NUMERIC_ASSEMBLY_LOSS',)
    assert [item['start'] for item in state.calls] == [0, 1, 2]


@pytest.mark.parametrize('interval,end', [(24, '23:59:59'), (60, '10:59:59'),
                                        (10, '10:09:59'), (1, '10:00:59')])
@pytest.mark.parametrize('kind,market,security', [('Акции', 'shares', 'FIXTURE_SHARE'),
                                               ('Облигации', 'bonds', 'FIXTURE_BOND')])
def test_red_from_equals_till_all_intervals_and_retries(
        gathering, monkeypatch, interval, end, kind, market, security):
    body = numeric_body('2000', '2024-03-01 10:00:00', '2024-03-01 ' + end)
    prepared = [response(status=503), response(body), response(status=503), response()]
    state = transport(gathering, monkeypatch, prepared)
    result = gathering.moex_query(security, kind, '2024-03-01', '2024-03-01', interval)
    assert len(result) == 1 and result['ticker'].tolist() == [security]
    assert [item['start'] for item in state.calls] == [0, 0, 1, 1]
    assert [item['headers']['User-Agent'] for item in state.calls] == [
        'AGENT_A', 'AGENT_B', 'AGENT_B', 'AGENT_B']
    for call in state.calls:
        assert call['path'] == f'/iss/engines/stock/markets/{market}/securities/{security}/candles.csv'
        assert call['params'] == {'from': ['2024-03-01'], 'till': ['2024-03-01'],
                                  'interval': [str(interval)], 'start': [str(call['start'])]}
        assert call['verify'] is True and call['allow_redirects'] is False and call['stream'] is True
    assert state.sleeps == [3, 3, 3, 3]


@pytest.mark.parametrize('caller', ['reload', 'update'])
def test_red_actual_caller_late_fault_preserves_affected_dataset(
        gathering, monkeypatch, fixture_root, save_dataset, writers, catalogue, snapshot,
        candle_frame, event_log, caller):
    root, root_arg = fixture_root
    dataset_path = root / 'datasets/fixture_a3.csv'
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    previous = candle_frame.copy(deep=True)
    previous_bytes = save_dataset(dataset_path, previous)
    xlsx_path = root / 'datasets/fixture_a3.xlsx'
    xlsx_bytes = b'A3_PREVIOUS_XLSX_BYTES'
    xlsx_path.write_bytes(xlsx_bytes)
    hashes = [hashlib.sha256(content).hexdigest() for content in (previous_bytes, xlsx_bytes)]
    primary = requests.exceptions.SSLError('A3_CALLER_TERMINAL_CANARY')
    data = numeric_body('2000', '2024-03-01 00:00:00', '2024-03-01 23:59:59')
    if caller == 'reload':
        prepared = [response(data), response(), response(data), primary]
        source = catalogue
    else:
        prepared = [response(data), primary]
        source = catalogue[catalogue['TRADE_CODE'] == 'SBER'].copy()
        original_read = gathering.pd.read_csv
        catalogue_path = root / 'datasets/ticker_lists/moex_full.csv'

        def read(path, *args, **kwargs):
            if isinstance(path, io.StringIO):
                return original_read(path, *args, **kwargs)
            target = Path(path)
            if target == catalogue_path:
                assert kwargs == {'index_col': 0}
                return source.copy(deep=True)
            if target == dataset_path:
                return original_read(path, *args, **kwargs)
            pytest.fail('Unexpected file read outside affected fixture dataset')

        monkeypatch.setattr(gathering.pd, 'read_csv', read)
    state = transport(gathering, monkeypatch, prepared)
    with pytest.raises(requests.exceptions.SSLError) as caught:
        if caller == 'reload':
            gathering.full_reload(source, 24, 10, 'fixture_a3', 'часа', root_arg,
                                  snapshot, logging_context=event_log.context)
        else:
            gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture_a3',
                                    'word': 'часа'}], root_arg, source, snapshot,
                                   logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('A3_CALLER_TERMINAL_CANARY',)
    assert len(state.calls) == (4 if caller == 'reload' else 2)
    assert writers == []
    assert dataset_path.read_bytes() == previous_bytes and xlsx_path.read_bytes() == xlsx_bytes
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in (dataset_path, xlsx_path)] == hashes
    assert not any(item['event'] in ('a1_full_reload_returned', 'a1_data_update_returned')
                   for item in event_log.events)


@pytest.mark.parametrize('first,second,expected,dtype', [
    ('2000', '2001', [2000, 2001], 'int64'),
    ('2000', '2000.5', [2000.0, 2000.5], 'float64'),
    ('2000', '2e3', [2000.0, 2000.0], 'float64'),
    ('9007199254740992', '9007199254740994.0',
     [9007199254740992.0, 9007199254740994.0], 'float64'),
    ('0', '-0.0', [0.0, -0.0], 'float64'),
])
def test_numeric_compatible_pages_have_literal_lossless_oracles(
        gathering, monkeypatch, first, second, expected, dtype):
    state = transport(gathering, monkeypatch, [response(numeric_body(first)),
        response(numeric_body(second, '2024-03-01 10:01:00', '2024-03-01 10:01:59')), response()])
    frame = query(gathering)
    assert frame['volume'].tolist() == expected
    assert str(frame['volume'].dtype) == dtype
    assert [math.copysign(1, value) for value in frame['volume']] == [
        math.copysign(1, value) for value in expected]
    assert frame.dtypes.astype(str).tolist() == ['float64'] * 5 + [dtype, 'object', 'object', 'object']
    assert [item['start'] for item in state.calls] == [0, 1, 2]


def test_large_integer_homogeneous_pages_preserve_literal_int64(gathering, monkeypatch):
    state = transport(gathering, monkeypatch, [response(numeric_body('9007199254740993')), response(
        numeric_body('9007199254740995', '2024-03-01 10:01:00', '2024-03-01 10:01:59')), response()])
    result = query(gathering)
    assert result['volume'].tolist() == [9007199254740993, 9007199254740995]
    assert str(result['volume'].dtype) == 'int64'
    assert [call['start'] for call in state.calls] == [0, 1, 2]


@pytest.mark.parametrize('fault', [requests.exceptions.Timeout('CAPTURE_TIMEOUT_CANARY'),
    ConnectionResetError('CAPTURE_RESET_CANARY'), MemoryError('CAPTURE_MEMORY_CANARY'),
    KeyboardInterrupt('CAPTURE_INTERRUPT_CANARY')])
@pytest.mark.parametrize('after_prefix', [False, True])
def test_preliminary_raw_capture_failure_is_native_terminal_without_retry(
        gathering, monkeypatch, event_log, fault, after_prefix):
    capture = gathering._a3_capture
    def fail(content, page, offset, attempt):
        if page == (2 if after_prefix else 1):
            raise fault
        return capture(content, page, offset, attempt)
    monkeypatch.setattr(gathering, '_a3_capture', fail)
    prepared = ([response(numeric_body('2000'))] if after_prefix else []) + [response()]
    state = transport(gathering, monkeypatch, prepared)
    original_args = fault.args
    with pytest.raises(type(fault)) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is fault and fault.args == original_args
    assert len(state.calls) == len(prepared)
    assert state.sleeps == ([3] if after_prefix else [])
    assert all(item.raw.close_calls == 1 for item in prepared)
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)
    assert event_log.events[-1]['error']['code'] == 'A3_ISS_RAW_CAPTURE_FAILED'


@pytest.mark.parametrize('local', [False, True])
def test_both_conflicting_raw_versions_and_provenance_survive(gathering, monkeypatch, local):
    first = numeric_body('  +02000  ')
    conflicting = numeric_body('2.001e3').splitlines()[2] + '\n'
    pages = [response(first + conflicting)] if local else [response(first), response(HEADER + conflicting)]
    state = transport(gathering, monkeypatch, pages)
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value.args == (('A2_ISS_RESPONSE_SCHEMA_INVALID',) if local else ('A3_ISS_PAGE_OVERLAP',))
    evidence = caught.value.a3_evidence
    assert evidence['raw_cells'] == (
        ('100.0', '101.0', '102.0', '99.0', '202000.0', '  +02000  ',
         '2024-03-01 10:00:00', '2024-03-01 10:00:59'),
        ('100.0', '101.0', '102.0', '99.0', '202000.0', '2.001e3',
         '2024-03-01 10:00:00', '2024-03-01 10:00:59'))
    meta = evidence['metadata']
    assert meta['format'] == 'RAW_CSV_CELLS_V1'
    assert meta['reason'] == ('LOCAL_DUPLICATE' if local else 'CROSS_PAGE_OVERLAP')
    assert meta['row_cap'] == 2 and meta['cell_bytes_cap'] == 16777216
    assert meta['metadata_bytes_cap'] == 16384 and meta['persistence'] == 'NOT_REQUESTED'
    assert meta['source_identity'] == meta['availability'] == 'NOT_VERIFIED'
    assert meta['board'] is None and meta['board_reason'] == 'NOT_PRESENT_IN_REQUEST_A5_UNVERIFIED'
    assert meta['request']['market'] == 'shares' and meta['request']['security'] == 'FIXTURE_SHARE'
    assert meta['request']['interval'] == '1' and meta['request']['from_date'] == meta['request']['till_date'] == '2024-03-01'
    assert len(meta['request']['request_id']) == 32 and meta['captured_utc'].endswith('+00:00')
    assert [(row['page_ordinal'], row['start'], row['attempt'], row['data_record_ordinal'])
            for row in meta['rows']] == ([(1, 0, 1, 1), (1, 0, 1, 2)] if local
                                         else [(1, 0, 1, 1), (2, 1, 1, 1)])
    bodies = [first + conflicting] * 2 if local else [first, HEADER + conflicting]
    assert [row['body_sha256'] for row in meta['rows']] == [
        hashlib.sha256(body.encode('utf-8')).hexdigest() for body in bodies]
    assert all(row['codec'] == 'utf-8' for row in meta['rows'])
    assert len(json.dumps(meta, ensure_ascii=False).encode('utf-8')) <= 16384
    assert sum(len(cell.encode('utf-8')) for row in evidence['raw_cells'] for cell in row) == meta['cell_bytes']
    assert len(state.calls) == len(pages)


@pytest.mark.parametrize('kind', ['metadata', 'cap', 'attribute'])
def test_secondary_evidence_failure_preserves_native_primary(gathering, monkeypatch, kind):
    class NativeTLS(requests.exceptions.SSLError):
        def __setattr__(self, name, value):
            if name == 'a3_evidence' and kind == 'attribute':
                raise OSError('EVIDENCE_ATTRIBUTE_CANARY')
            super().__setattr__(name, value)
    primary = NativeTLS('SOURCE_PRIMARY_CANARY')
    if kind == 'metadata':
        def fail(value):
            raise requests.exceptions.Timeout('SECONDARY_EVIDENCE_CANARY')
        monkeypatch.setattr(gathering, '_a3_metadata', fail)
    if kind == 'cap':
        monkeypatch.setattr(gathering, '_A3_METADATA_BYTES', 1)
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), primary])
    with pytest.raises(NativeTLS) as caught:
        query(gathering)
    assert caught.value is primary and primary.args == ('SOURCE_PRIMARY_CANARY',)
    assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in primary.__notes__
    assert len(state.calls) == 2 and state.sleeps == [3]


@pytest.mark.parametrize('local', [False, True])
def test_conflict_primary_object_survives_raw_evidence_capture_fault(gathering, monkeypatch, local):
    observed = []
    original = gathering._a3_evidence
    def observe(primary, *args, **kwargs):
        observed.append((primary, primary.args))
        return original(primary, *args, **kwargs)
    def fail(value):
        raise ConnectionResetError('RAW_EVIDENCE_SECONDARY_CANARY')
    monkeypatch.setattr(gathering, '_a3_evidence', observe)
    monkeypatch.setattr(gathering, '_a3_metadata', fail)
    body = numeric_body('2000')
    prepared = [response(body + body.splitlines()[2] + '\n')] if local else [response(body), response(body)]
    state = transport(gathering, monkeypatch, prepared)
    with pytest.raises(ValueError) as caught:
        query(gathering)
    assert caught.value is observed[0][0] and caught.value.args == observed[0][1]
    assert caught.value.args == (('A2_ISS_RESPONSE_SCHEMA_INVALID',) if local else ('A3_ISS_PAGE_OVERLAP',))
    assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in caught.value.__notes__
    assert len(state.calls) == len(prepared)


def test_oversize_metadata_is_hashed_without_public_argument_rejection(gathering, monkeypatch):
    security = 'S' * 129
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), response(numeric_body('2001'))])
    with pytest.raises(ValueError) as caught:
        gathering.moex_query(security, 'Акции', '2024-03-01', '2024-03-01', 1)
    meta = caught.value.a3_evidence['metadata']
    item = meta['request']['security']
    assert item['value'] is None and item['bytes'] == 129
    assert item['sha256'] == hashlib.sha256(security.encode()).hexdigest()
    assert meta['metadata_exact'] is False and meta['omitted_fields'] == ['security']
    assert len(state.calls) == 2 and security in state.calls[0]['path']


def test_metadata_lone_surrogate_has_explicit_hash_projection(gathering, monkeypatch):
    primary = requests.exceptions.SSLError('SURROGATE_SOURCE_CANARY')
    state = transport(gathering, monkeypatch, [primary])
    with pytest.raises(requests.exceptions.SSLError) as caught:
        query(gathering, logging_context=None, max_retries=1)
    assert caught.value is primary
    projected = gathering._a3_metadata('\ud800')
    assert projected == {'value': None, 'state': 'HASH_ONLY_OVERSIZE_OR_ENCODING', 'bytes': 3,
        'sha256': hashlib.sha256(b'\xed\xa0\x80').hexdigest(), 'encoding': 'UTF8_SURROGATEPASS'}
    assert len(state.calls) == 1


@pytest.mark.parametrize('terminal', [True, False])
def test_page_cap_includes_terminal_and_never_requests_next(gathering, monkeypatch, terminal):
    assert gathering._A3_MAX_PAGES == 1000 and gathering._A3_MAX_BYTES == 67108864
    monkeypatch.setattr(gathering, '_A3_MAX_PAGES', 2)
    pages = [response(numeric_body('2000')), response() if terminal else response(
        numeric_body('2001', '2024-03-01 10:01:00', '2024-03-01 10:01:59'))]
    state = transport(gathering, monkeypatch, pages)
    state.frozen = True
    if terminal:
        assert len(query(gathering)) == 1
    else:
        with pytest.raises(ValueError, match='^A3_ISS_PAGE_LIMIT_EXCEEDED$'):
            query(gathering)
    assert [call['start'] for call in state.calls] == [0, 1]


@pytest.mark.parametrize('overflow', [0, 1])
def test_query_byte_cap_exact_boundary_including_empty(gathering, monkeypatch, overflow):
    body = numeric_body('2000')
    cap = len(body.encode()) + len(EMPTY.encode()) - overflow
    monkeypatch.setattr(gathering, '_A3_MAX_BYTES', cap)
    pages = [response(body), response()]
    state = transport(gathering, monkeypatch, pages)
    if overflow:
        with pytest.raises(ValueError, match='^A3_ISS_QUERY_BYTES_EXCEEDED$'):
            query(gathering)
    else:
        assert len(query(gathering)) == 1
    assert [call['start'] for call in state.calls] == [0, 1]
    assert all(item.raw.close_calls == 1 for item in pages)


def test_failed_attempt_read_bytes_are_in_query_cap(gathering, monkeypatch):
    body = numeric_body('2000')
    broken = response()
    count = [0]
    primary = requests.exceptions.Timeout('RETRY_BODY_CANARY')
    def read(amt=None, decode_content=False):
        count[0] += 1
        if count[0] == 1:
            return b'1234567'
        raise primary
    broken.raw.read = read
    monkeypatch.setattr(gathering, '_A3_MAX_BYTES', len(body.encode()) + len(EMPTY.encode()) + 6)
    state = transport(gathering, monkeypatch, [broken, response(body), response()])
    with pytest.raises(ValueError, match='^A3_ISS_QUERY_BYTES_EXCEEDED$'):
        query(gathering)
    assert [call['start'] for call in state.calls] == [0, 0, 1]
    assert state.sleeps == [3, 3]


def test_shared_budget_exhaustion_after_staged_pages_refuses_prefix(
        gathering, monkeypatch, literal_fixture, event_log):
    pages = [response(payload(literal_fixture, [index])) for index in range(60)]
    state = transport(gathering, monkeypatch, pages)
    with pytest.raises(requests.exceptions.Timeout, match='^A2_ISS_DEADLINE_EXCEEDED$'):
        query(gathering, logging_context=event_log.context)
    assert len(state.calls) == 60 and state.sleeps == [3] * 59
    assert [call['start'] for call in state.calls] == list(range(60))
    assert event_log.events[-1]['counts']['rows_received'] == 60
    assert not any(item['event'] == 'a2_query_returned' for item in event_log.events)


@pytest.mark.parametrize('caller_page', [None, 'caller-page', 8])
def test_page_events_closed_counts_and_caller_correlation(gathering, monkeypatch, event_log, caller_page):
    if caller_page is not None:
        event_log.context['page'] = caller_page
    before = dict(event_log.context)
    state = transport(gathering, monkeypatch, [response(status=503), response(numeric_body('2000')), response()])
    query(gathering, logging_context=event_log.context)
    assert event_log.context == before
    events = event_log.events
    assert [item['event'] for item in events] == ['a2_query_started', 'a3_page_started',
        'a2_query_attempt_started', 'a2_query_retry', 'a2_query_attempt_started',
        'a3_page_returned', 'a3_page_started', 'a2_query_attempt_started',
        'a3_page_terminal', 'a2_query_returned']
    assert [item['page'] for item in events] == ([caller_page] * 10 if caller_page is not None
        else [None, 0, 0, 0, 0, 0, 1, 1, 1, None])
    assert events[-1]['retries'] == {'attempt': 3, 'limit': 5000}
    assert events[-1]['counts'] == {'rows_received': 1, 'rows_accepted': None,
        'rows_quarantined': None, 'pages': 2, 'instruments': None,
        'null_reasons': {'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED', 'instruments': 'NOT_ASSESSED'}}
    assert 'p=2; start=1; got=0; add=0; sum=1; get=3; retry=1; term=OFFSET_TERMINAL_EMPTY' in events[-2]['message']
    assert all(item['fields'] == {} and len(item['message']) <= 256 for item in events)
    assert len(state.calls) == 3


@pytest.mark.parametrize('stage', ['a3_page_started', 'a3_page_returned', 'a3_page_terminal', 'a2_query_returned'])
def test_page_log_delivery_fault_refuses_without_transport_retry(gathering, monkeypatch, event_log, stage):
    original = event_log.logger.emit_event
    def emit(context, level, event, message, fields):
        if event == stage:
            raise OSError('PAGE_LOG_CANARY')
        return original(context, level, event, message, fields)
    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), response()])
    with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$'):
        query(gathering, logging_context=event_log.context)
    assert len(state.calls) == (0 if stage == 'a3_page_started' else 1 if stage == 'a3_page_returned' else 2)
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)


def test_late_native_primary_survives_logger_and_evidence_faults(gathering, monkeypatch, event_log):
    primary = requests.exceptions.SSLError('LATE_PRIMARY_CANARY')
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), primary])
    state.after_get = lambda: setattr(event_log, 'fail', len(state.calls) == 2)
    def fail(value):
        raise OSError('EVIDENCE_CANARY')
    monkeypatch.setattr(gathering, '_a3_metadata', fail)
    with pytest.raises(requests.exceptions.SSLError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == ('LATE_PRIMARY_CANARY',)
    assert 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE' in primary.__notes__
    assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in primary.__notes__
    assert len(state.calls) == 2 and state.sleeps == [3]


# Literal, distinct raw lexemes; no duplicate key can hide an ordering refusal.
ORDER_PREFIX = ('100.0;101.0;102.0;99.0;202000.0;0900;'
                '2024-03-01 09:00:00;2024-03-01 09:00:59\n')
ORDER_ZERO = ('100.0;101.0;102.0;99.0;202000.0;0000;'
              '2024-03-01 10:00:00;2024-03-01 10:00:59\n')
ORDER_FOUR = ('+1.00e2;101.00;102.0;99.0;202000.0;  +04000  ;'
              '2024-03-01 10:04:00;2024-03-01 10:04:59\n')
ORDER_THREE = ('100.00;1.01e2;102.0;99.0;202000.0;3.003e3;'
               '2024-03-01 10:03:00;2024-03-01 10:03:59\n')
ORDER_TWO = ('100.0;101.0;1.020e2;99.00;202000.00; 02002 ;'
             '2024-03-01 10:02:00;2024-03-01 10:02:59\n')
ORDER_FIRST = ('+1.00e2', '101.00', '102.0', '99.0', '202000.0', '  +04000  ',
               '2024-03-01 10:04:00', '2024-03-01 10:04:59')
ORDER_INTRA_SECOND = ('100.00', '1.01e2', '102.0', '99.0', '202000.0', '3.003e3',
                      '2024-03-01 10:03:00', '2024-03-01 10:03:59')
ORDER_CROSS_SECOND = ('100.0', '101.0', '1.020e2', '99.00', '202000.00', ' 02002 ',
                      '2024-03-01 10:02:00', '2024-03-01 10:02:59')


def ordering_case(kind):
    if kind == 'intra':
        bodies = [HEADER + ORDER_ZERO + ORDER_FOUR + ORDER_THREE + ORDER_TWO]
        return bodies, [0], (ORDER_FIRST, ORDER_INTRA_SECOND), [(1, 0, 1, 2), (1, 0, 1, 3)], [0, 0]
    bodies = [HEADER + ORDER_ZERO + ORDER_FOUR, HEADER + ORDER_TWO + ORDER_THREE]
    if kind == 'cross-prefix':
        return [HEADER + ORDER_PREFIX, *bodies], [0, 1, 3], (
            ORDER_FIRST, ORDER_CROSS_SECOND), [(2, 1, 1, 2), (3, 3, 1, 1)], [1, 2]
    assert kind == 'cross'
    return bodies, [0, 2], (ORDER_FIRST, ORDER_CROSS_SECOND), [(1, 0, 1, 2), (2, 2, 1, 1)], [0, 1]


@pytest.mark.parametrize('kind', ['intra', 'cross', 'cross-prefix'])
@pytest.mark.parametrize('with_context', [False, True])
def test_review_a_ordering_has_first_raw_pair_and_native_primary(
        gathering, monkeypatch, event_log, writers, kind, with_context):
    bodies, offsets, cells, provenance, body_positions = ordering_case(kind)
    prepared = [response(body) for body in bodies]
    unused = response()
    state = transport(gathering, monkeypatch, [*prepared, unused])
    observed = []
    original = gathering._a3_evidence
    def evidence(primary, *args, **kwargs):
        observed.append((primary, primary.args))
        return original(primary, *args, **kwargs)
    monkeypatch.setattr(gathering, '_a3_evidence', evidence)
    with pytest.raises(ValueError) as caught:
        query(gathering, logging_context=event_log.context if with_context else None)
    primary = caught.value
    assert type(primary) is ValueError and primary.args == ('A3_ISS_PAGE_ORDER_INVALID',)
    assert primary is observed[0][0] and primary.args == observed[0][1]
    raw = primary.a3_evidence
    assert raw['raw_cells'] == cells
    meta = raw['metadata']
    assert meta['reason'] == ('INTRA_PAGE_ORDER' if kind == 'intra' else 'CROSS_PAGE_ORDER')
    assert [(row['page_ordinal'], row['start'], row['attempt'], row['data_record_ordinal'])
            for row in meta['rows']] == provenance
    assert [row['codec'] for row in meta['rows']] == ['utf-8', 'utf-8']
    assert [row['body_sha256'] for row in meta['rows']] == [
        hashlib.sha256(bodies[position].encode('utf-8')).hexdigest() for position in body_positions]
    assert [row['body_bytes'] for row in meta['rows']] == [len(bodies[position].encode()) for position in body_positions]
    assert [row['admission_state'] for row in meta['rows']] == (
        ['A2_VALIDATED', 'A2_VALIDATED'] if kind == 'intra' else ['A3_STAGED', 'A2_VALIDATED'])
    assert (meta['row_cap'], meta['cell_bytes_cap'], meta['metadata_bytes_cap']) == (2, 16777216, 16384)
    assert meta['source_identity'] == meta['availability'] == 'NOT_VERIFIED'
    assert meta['persistence'] == 'NOT_REQUESTED' and meta['verified_sha'] is None
    assert [call['start'] for call in state.calls] == offsets
    assert state.sleeps == [3] * len(bodies) and writers == []
    assert all(item.raw.close_calls == 1 for item in prepared)
    assert unused.raw.reads == [] and unused.raw.close_calls == 0
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)


@pytest.mark.parametrize('kind', ['intra', 'cross', 'cross-prefix'])
@pytest.mark.parametrize('fault', ['metadata', 'cap', 'helper', 'logger'])
def test_review_a_ordering_secondary_fault_keeps_original_primary(
        gathering, monkeypatch, event_log, writers, kind, fault):
    bodies, offsets, cells, provenance, body_positions = ordering_case(kind)
    state = transport(gathering, monkeypatch, [response(body) for body in bodies])
    observed = []
    original = gathering._a3_evidence
    def evidence(primary, reason, request, pairs=(), logging_context=None):
        observed.append((primary, primary.args, reason, tuple(pairs)))
        if fault == 'helper':
            raise requests.exceptions.Timeout('ORDER_EVIDENCE_CANARY')
        return original(primary, reason, request, pairs, logging_context)
    monkeypatch.setattr(gathering, '_a3_evidence', evidence)
    if fault == 'metadata':
        def fail(value):
            raise ConnectionResetError('ORDER_METADATA_CANARY')
        monkeypatch.setattr(gathering, '_a3_metadata', fail)
    if fault == 'cap':
        monkeypatch.setattr(gathering, '_A3_EVIDENCE_BYTES', 1)
    if fault == 'logger':
        original_emit = event_log.logger.emit_event
        def emit(context, level, event, message, fields):
            if event == 'a2_query_failed':
                raise OSError('ORDER_LOG_CANARY')
            return original_emit(context, level, event, message, fields)
        monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    with pytest.raises(ValueError) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is observed[0][0] and caught.value.args == observed[0][1] == ('A3_ISS_PAGE_ORDER_INVALID',)
    assert observed[0][2] == ('INTRA_PAGE_ORDER' if kind == 'intra' else 'CROSS_PAGE_ORDER')
    assert tuple(tuple(pair[2]) for pair in observed[0][3]) == cells
    if fault == 'logger':
        assert caught.value.a3_evidence['raw_cells'] == cells
        assert 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE' in caught.value.__notes__
    else:
        assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in caught.value.__notes__
    assert [call['start'] for call in state.calls] == offsets and writers == []
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)


@pytest.mark.parametrize('with_context', [False, True])
@pytest.mark.parametrize('elapsed', [179.0, 180.0, 181.0])
def test_review_b_final_assembly_deadline_is_a2_compatible(
        gathering, monkeypatch, event_log, writers, capsys, with_context, elapsed):
    pages = [response(numeric_body('2000')), response()]
    unused = response()
    state = transport(gathering, monkeypatch, [*pages, unused])
    original_assemble = gathering._a3_assemble
    original_diagnostic = gathering._a2_diagnostic
    observed = []
    def assemble(frames, ticker):
        assert len(state.calls) == 2 and state.sleeps == [3, 3]
        assert [item['event'] for item in event_log.events][-1:] == (['a3_page_terminal'] if with_context else [])
        result = original_assemble(frames, ticker)
        state.elapsed = elapsed
        return result
    def diagnostic(context, level, event, message, fields, primary=None):
        observed.append((event, fields, primary, None if primary is None else primary.args))
        return original_diagnostic(context, level, event, message, fields, primary)
    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    if elapsed < 180:
        result = query(gathering, logging_context=event_log.context if with_context else None)
        assert result['volume'].tolist() == [2000] and result['ticker'].tolist() == ['FIXTURE_SHARE']
        assert observed[-1][0] == 'a2_query_returned'
    else:
        with pytest.raises(requests.exceptions.Timeout) as caught:
            query(gathering, logging_context=event_log.context if with_context else None)
        terminal = observed[-1]
        assert caught.value is terminal[2] and caught.value.args == terminal[3] == ('A2_ISS_DEADLINE_EXCEEDED',)
        fields = terminal[1]
        assert (terminal[0], fields['outcome'], fields['error']['code'], fields['error']['category']) == (
            'a2_query_failed', 'DEADLINE_EXCEEDED', 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT')
        assert fields['error']['final'] is True and fields['error']['retryable'] is False
        assert fields['retries'] == {'attempt': 2, 'limit': 5000}
        assert fields['counts']['rows_received'] == 1 and fields['counts']['pages'] == 2
        assert [item[0] for item in observed].count('a2_query_failed') == 1
        assert not any(item[0] in ('a2_query_retry', 'a2_query_returned') for item in observed)
        output = capsys.readouterr()
        if not with_context:
            assert 'DEADLINE_EXCEEDED: A2_ISS_DEADLINE_EXCEEDED' in output.out
    assert [call['start'] for call in state.calls] == [0, 1] and state.sleeps == [3, 3]
    assert all(item.raw.close_calls == 1 for item in pages) and writers == []
    assert unused.raw.reads == [] and unused.raw.close_calls == 0


@pytest.mark.parametrize('with_context', [False, True])
@pytest.mark.parametrize('secondary', ['evidence', 'delivery', 'both'])
def test_review_b_final_deadline_primary_precedes_secondary_faults(
        gathering, monkeypatch, event_log, writers, with_context, secondary):
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), response()])
    original_assemble = gathering._a3_assemble
    original_evidence = gathering._a3_evidence
    original_diagnostic = gathering._a2_diagnostic
    observed, source = [], []
    def assemble(frames, ticker):
        result = original_assemble(frames, ticker)
        state.elapsed = 180.0
        return result
    def evidence(primary, *args, **kwargs):
        source.append((primary, primary.args))
        return original_evidence(primary, *args, **kwargs)
    def diagnostic(context, level, event, message, fields, primary=None):
        observed.append((event, fields, primary))
        return original_diagnostic(context, level, event, message, fields, primary)
    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    monkeypatch.setattr(gathering, '_a3_evidence', evidence)
    monkeypatch.setattr(gathering, '_a2_diagnostic', diagnostic)
    if secondary in ('evidence', 'both'):
        def fail(value):
            raise OSError('FINAL_EVIDENCE_CANARY')
        monkeypatch.setattr(gathering, '_a3_metadata', fail)
    if secondary in ('delivery', 'both'):
        if with_context:
            original_emit = event_log.logger.emit_event
            def emit(context, level, event, message, fields):
                if event == 'a2_query_failed':
                    raise OSError('FINAL_LOG_CANARY')
                return original_emit(context, level, event, message, fields)
            monkeypatch.setattr(event_log.logger, 'emit_event', emit)
        else:
            import builtins
            original_print = builtins.print
            def printing(*args, **kwargs):
                if args and type(args[0]) is str and args[0].startswith('Свечи DEADLINE_EXCEEDED:'):
                    raise OSError('FINAL_CONSOLE_CANARY')
                return original_print(*args, **kwargs)
            monkeypatch.setattr(builtins, 'print', printing)
    with pytest.raises(requests.exceptions.Timeout) as caught:
        query(gathering, logging_context=event_log.context if with_context else None)
    assert caught.value is source[0][0] and caught.value.args == source[0][1] == ('A2_ISS_DEADLINE_EXCEEDED',)
    fields = observed[-1][1]
    assert (fields['outcome'], fields['error']['code'], fields['error']['category']) == (
        'DEADLINE_EXCEEDED', 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT')
    assert caught.value is observed[-1][2]
    if secondary in ('evidence', 'both'):
        assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in caught.value.__notes__
    if secondary in ('delivery', 'both'):
        assert 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE' in caught.value.__notes__
    assert len(state.calls) == 2 and state.sleeps == [3, 3] and writers == []
    assert not any(item[0] in ('a2_query_retry', 'a2_query_returned') for item in observed)


@pytest.mark.parametrize('primary', [RuntimeError('ASSEMBLY_NATIVE_CANARY'),
    requests.exceptions.Timeout('A2_ISS_DEADLINE_EXCEEDED')])
def test_review_b_native_assembly_failure_is_not_replaced_by_expired_clock(
        gathering, monkeypatch, event_log, writers, primary):
    state = transport(gathering, monkeypatch, [response(numeric_body('2000')), response()])
    original_args = primary.args
    def assemble(frames, ticker):
        state.elapsed = 180.0
        raise primary
    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    with pytest.raises(type(primary)) as caught:
        query(gathering, logging_context=event_log.context)
    assert caught.value is primary and primary.args == original_args
    assert event_log.events[-1]['error']['code'] != 'A2_ISS_DEADLINE_EXCEEDED'
    assert len(state.calls) == 2 and state.sleeps == [3, 3] and writers == []
    assert not any(item['event'] in ('a2_query_retry', 'a2_query_returned') for item in event_log.events)


@pytest.mark.parametrize('caller', ['reload', 'update'])
def test_review_a_actual_caller_order_fault_preserves_affected_previous(
        gathering, monkeypatch, fixture_root, save_dataset, writers, catalogue,
        snapshot, candle_frame, event_log, caller):
    root, root_arg = fixture_root
    target = root / 'datasets/fixture_order_previous.csv'
    target.parent.mkdir(parents=True, exist_ok=True)
    previous_csv = save_dataset(target, candle_frame)
    excel = target.with_suffix('.xlsx')
    previous_excel = b'ORDER_PREVIOUS_XLSX_BYTES'
    excel.write_bytes(previous_excel)
    hashes = [hashlib.sha256(item).hexdigest() for item in (previous_csv, previous_excel)]
    source = catalogue[catalogue['TRADE_CODE'] == 'SBER'].copy(deep=True)
    if caller == 'update':
        original_read = gathering.pd.read_csv
        catalogue_path = root / 'datasets/ticker_lists/moex_full.csv'
        def read(path, *args, **kwargs):
            if isinstance(path, io.StringIO):
                return original_read(path, *args, **kwargs)
            if Path(path) == catalogue_path:
                assert args == () and kwargs == {'index_col': 0}
                return source.copy(deep=True)
            assert Path(path) == target and args == () and kwargs == {}
            return original_read(path, *args, **kwargs)
        monkeypatch.setattr(gathering.pd, 'read_csv', read)
    bodies, offsets, cells, provenance, body_positions = ordering_case('cross')
    unused = response()
    state = transport(gathering, monkeypatch, [*[response(body) for body in bodies], unused])
    observed = []
    original_evidence = gathering._a3_evidence
    def evidence(primary, *args, **kwargs):
        observed.append((primary, primary.args))
        return original_evidence(primary, *args, **kwargs)
    monkeypatch.setattr(gathering, '_a3_evidence', evidence)
    with pytest.raises(ValueError) as caught:
        if caller == 'reload':
            gathering.full_reload(source, 1, 10, 'fixture_order_previous', 'минута', root_arg,
                                  snapshot, logging_context=event_log.context)
        else:
            gathering.data_update([{'interval': 1, 'years': 10, 'filename': 'fixture_order_previous',
                                    'word': 'минута'}], root_arg, source, snapshot,
                                   logging_context=event_log.context)
    assert caught.value is observed[0][0] and caught.value.args == observed[0][1] == ('A3_ISS_PAGE_ORDER_INVALID',)
    assert caught.value.a3_evidence['raw_cells'] == cells
    meta = caught.value.a3_evidence['metadata']
    assert meta['request']['security'] == 'SBER'
    assert [(row['page_ordinal'], row['start'], row['attempt'], row['data_record_ordinal'])
            for row in meta['rows']] == provenance
    assert [call['start'] for call in state.calls] == offsets and writers == []
    assert unused.raw.reads == [] and unused.raw.close_calls == 0
    assert target.read_bytes() == previous_csv and excel.read_bytes() == previous_excel
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in (target, excel)] == hashes
    assert not any(item['event'] in ('a2_query_returned', 'a1_full_reload_returned', 'a1_data_update_returned')
                   for item in event_log.events)
