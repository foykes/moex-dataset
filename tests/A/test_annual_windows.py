"""Independent literal A3-02 calendar/union oracles under the native A guard.

Endpoint models below are synthetic. They establish neither ISS inclusivity nor
availability, membership, timezone or finality. No production helper supplies an
expected date, key, numeric identity or output frame.
"""

import datetime
import hashlib
import io
import json
import math
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from urllib3._collections import HTTPHeaderDict


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures/a3/annual_windows.json'
COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
NUMERIC = COLUMNS[:6]


@pytest.fixture
def annual_fixture():
    result = json.loads(FIXTURE.read_text(encoding='utf-8'))
    assert result['source_inclusivity_membership_finality'] == 'NOT_VERIFIED'
    assert result['columns'] == COLUMNS
    assert len(result['ordinary_requests']) == 30
    return result


def freeze(gathering, monkeypatch, anchor, *, advance=False, unpadded=False):
    """A platform without %Y padding, and a clock that exposes extra now calls."""
    year, month, day = [int(part) for part in anchor.split('-')]
    calls = []

    class CalendarDate(datetime.date):
        def strftime(self, pattern):
            if unpadded and pattern == '%Y-%m-%d':
                return f'{self.year}-{self.month:02d}-{self.day:02d}'
            return super().strftime(pattern)

    class Clock(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            calls.append(tz)
            value = cls(year, month, day, 12, 0, 0, tzinfo=tz)
            return value + datetime.timedelta(days=len(calls) - 1 if advance else 0)

        def date(self):
            return CalendarDate(self.year, self.month, self.day)

        def strftime(self, pattern):
            if unpadded and pattern == '%Y-%m-%d':
                return self.date().strftime(pattern)
            return super().strftime(pattern)

    monkeypatch.setattr(gathering, 'datetime',
                        SimpleNamespace(datetime=Clock, timedelta=datetime.timedelta))
    return calls


def bar(begin='2024-03-01 00:00:00', *, end=None, ticker='SBER', open_value=100.0):
    return [open_value, 101.0, 102.0, 99.0, 202000.0, 2000,
            begin, end if end is not None else begin[:10] + ' 23:59:59', ticker]


def frame(gathering, rows, *, open_dtype=None):
    result = gathering.pd.DataFrame(rows, columns=COLUMNS)
    if open_dtype is not None:
        result['open'] = gathering.pd.Series([row[0] for row in rows], dtype=open_dtype)
    return result


def windows(gathering, monkeypatch, outcomes):
    prepared = iter(outcomes)
    calls = []

    def query(*args, logging_context=None):
        calls.append((args, logging_context))
        try:
            item = next(prepared)
        except StopIteration:
            pytest.fail('Unexpected annual request beyond the independent schedule')
        if isinstance(item, BaseException):
            raise item
        return item

    monkeypatch.setattr(gathering, 'moex_query', query)
    return calls


def assert_rows(result, expected, *, dtypes=None):
    assert result.columns.tolist() == COLUMNS
    assert result.index.tolist() == list(range(len(expected)))
    assert type(result.index).__name__ == 'RangeIndex'
    for position, column in enumerate(COLUMNS):
        assert result[column].tolist() == [row[position] for row in expected]
    if dtypes is not None:
        assert result.dtypes.astype(str).tolist() == dtypes


@pytest.mark.parametrize('years', [1, 2, 3, 4, 10, 30])
@pytest.mark.parametrize('interval', [24, 60, 10, 1])
@pytest.mark.parametrize('with_context', [False, True])
def test_red_exact_n_literal_requests_and_one_clock(gathering, monkeypatch, annual_fixture,
                                                  event_log, years, interval, with_context):
    clock = freeze(gathering, monkeypatch, '2024-03-02', advance=True)
    context = event_log.context if with_context else None
    original_context = dict(context) if context is not None else None
    calls = windows(gathering, monkeypatch, [gathering.pd.DataFrame() for _ in range(years)])
    result = gathering.moex(' RAW_TOKEN ', 'Облигации', years, interval,
                            logging_context=context)
    assert [item[0] for item in calls] == [
        (' RAW_TOKEN ', 'Облигации', lower, upper, interval)
        for lower, upper in annual_fixture['ordinary_requests'][:years]]
    assert all(item[1] is context for item in calls)
    assert clock == [None]
    assert result.empty and result.columns.tolist() == []
    assert context == original_context


@pytest.mark.parametrize('case_id', ['year0001', 'year0999', 'year1000', 'leap-recovery',
                                    'century1900', 'century2000', 'century2100', 'march1',
                                    'feb28', 'january1', 'december31'])
def test_red_portable_literal_calendar_schedule(gathering, monkeypatch, annual_fixture, case_id):
    case = next(item for item in annual_fixture['calendar_cases'] if item['id'] == case_id)
    clock = freeze(gathering, monkeypatch, case['anchor'], unpadded=True)
    calls = windows(gathering, monkeypatch,
                    [gathering.pd.DataFrame() for _ in range(case['years'])])
    result = gathering.moex('SBER', 'Акции', case['years'], 24)
    assert [item[0][2:4] for item in calls] == [tuple(bounds) for bounds in case['requests']]
    assert len(calls) == case['years'] and clock == [None]
    assert all(len(token) == 10 for args, _ in calls for token in args[2:4])
    assert result.empty and result.columns.tolist() == []


@pytest.mark.parametrize('anchor,years', [('0001-01-01', 1), ('0002-01-01', 2)])
def test_red_complete_schedule_underflow_before_first_query(gathering, monkeypatch,
                                                           writers, anchor, years):
    freeze(gathering, monkeypatch, anchor, unpadded=True)
    calls = windows(gathering, monkeypatch, [gathering.pd.DataFrame()] * years)
    with pytest.raises(ValueError):
        gathering.moex('SBER', 'Акции', years, 24)
    assert calls == [] and writers == []


@pytest.mark.parametrize('years', [0, -1, -30, False, True])
def test_legacy_no_query_empty_years_profile(gathering, monkeypatch, years):
    calls = windows(gathering, monkeypatch, [])
    result = gathering.moex('SBER', 'Акции', years, 24)
    assert calls == [] and result.empty and result.columns.tolist() == []


@pytest.mark.parametrize('years', [1.0, '1', None, object()])
def test_legacy_non_indexable_typeerror_precedes_queries(gathering, monkeypatch, years):
    calls = windows(gathering, monkeypatch, [])
    with pytest.raises(TypeError):
        gathering.moex('SBER', 'Акции', years, 24)
    assert calls == []


def test_red_closed_closed_halo_repeats_two_whole_dates(gathering, monkeypatch, annual_fixture):
    fixture = annual_fixture['closed_closed']
    freeze(gathering, monkeypatch, fixture['anchor'])
    original = [frame(gathering, rows) for rows in fixture['windows']]
    before = [item.copy(deep=True) for item in original]
    calls = windows(gathering, monkeypatch, original)
    result = gathering.moex('SBER', 'Акции', 2, 24)
    assert [item[0][2:4] for item in calls] == [tuple(pair) for pair in fixture['requests']]
    assert sum(len(item) for item in original) == 6
    assert_rows(result, fixture['expected_rows'], dtypes=['float64'] * 5 + ['int64'] + ['object'] * 3)
    for actual, expected in zip(original, before):
        gathering.pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('model_id', ['closed-closed', 'closed-open', 'open-closed', 'open-open'])
def test_red_synthetic_boundary_models_keep_join_without_claiming_outer_edges(
        gathering, monkeypatch, annual_fixture, model_id):
    model = next(item for item in annual_fixture['endpoint_models'] if item['id'] == model_id)
    keys = annual_fixture['model_begin_keys']
    freeze(gathering, monkeypatch, '2025-03-01')
    calls = windows(gathering, monkeypatch,
                    [frame(gathering, [bar(keys[index]) for index in positions])
                     for positions in model['windows']])
    result = gathering.moex('SBER', 'Акции', 2, 1)
    assert [args[2:4] for args, _ in calls] == [('2024-03-01', '2025-03-01'),
                                               ('2023-03-01', '2024-03-02')]
    assert_rows(result, [bar(keys[index]) for index in model['expected']])
    assert '2024-03-01 00:00:00' in result.begin.tolist()
    assert '2024-03-02 00:00:00' in result.begin.tolist()
    assert ('2023-03-01 00:00:00' in result.begin.tolist()) == model_id.startswith('closed')
    assert ('2025-03-01 00:00:00' in result.begin.tolist()) == model_id.endswith('closed')


def test_red_union_does_not_filter_or_rewrite_returned_membership(gathering, monkeypatch):
    # These admitted rows deliberately lie outside nominal/requested bounds.
    original = [frame(gathering, [bar('2030-01-01 00:00:00')]),
                frame(gathering, [bar('1900-01-01 00:00:00')])]
    windows(gathering, monkeypatch, original)
    result = gathering.moex('SBER', 'Акции', 2, 24)
    assert_rows(result, [bar('1900-01-01 00:00:00'), bar('2030-01-01 00:00:00')])


def test_red_empty_newest_does_not_stop_older_history(gathering, monkeypatch):
    calls = windows(gathering, monkeypatch, [gathering.pd.DataFrame(),
                    frame(gathering, [bar('2022-03-02 00:00:00')]), gathering.pd.DataFrame()])
    result = gathering.moex('SBER', 'Акции', 3, 24)
    assert len(calls) == 3
    assert_rows(result, [bar('2022-03-02 00:00:00')])


@pytest.mark.parametrize('column,replacement', [('open', 100.5), ('close', 101.5),
    ('high', 102.5), ('low', 98.5), ('value', 202001.0), ('volume', 2001),
    ('end', '2024-03-01 23:59:58'), ('ticker', 'MOEX')])
def test_red_every_same_key_value_conflict_refuses_before_assembly(
        gathering, monkeypatch, writers, column, replacement):
    first = frame(gathering, [bar()])
    second = first.copy(deep=True)
    second.loc[0, column] = replacement
    before = [item.copy(deep=True) for item in (first, second)]
    calls = windows(gathering, monkeypatch, [first, second, first.copy(deep=True)])
    assembled = []
    original = gathering._a3_assemble

    def assemble(*args):
        assembled.append(args)
        return original(*args)

    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    with pytest.raises(ValueError, match='^A3_ISS_ANNUAL_KEY_CONFLICT$'):
        gathering.moex('SBER', 'Акции', 3, 24)
    assert len(calls) == 2 and assembled == [] and writers == []
    for actual, expected in zip((first, second), before):
        gathering.pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('years', [3, 4])
@pytest.mark.parametrize('with_context', [False, True])
def test_red_third_window_late_end_conflict_retains_first_and_third(
        gathering, monkeypatch, writers, event_log, years, with_context):
    originals = [frame(gathering, [bar()]) for _ in range(years)]
    originals[2].loc[0, 'end'] = '2024-03-01 23:59:58'
    before = [item.copy(deep=True) for item in originals]
    calls = windows(gathering, monkeypatch, originals)
    context = event_log.context if with_context else None
    with pytest.raises(ValueError, match='^A3_ISS_ANNUAL_KEY_CONFLICT$') as captured:
        gathering.moex('SBER', 'Акции', years, 24, logging_context=context)
    assert len(calls) == 3 and all(item[1] is context for item in calls) and writers == []
    evidence = captured.value.a3_annual_evidence
    assert evidence['metadata']['format'] == 'A3_ANNUAL_TYPED_ROWS_V1'
    assert evidence['metadata']['state'] == 'CAPTURED'
    assert [item['window_ordinal'] for item in evidence['metadata']['rows']] == [1, 3]
    assert [item['row_position'] for item in evidence['metadata']['rows']] == [0, 0]
    assert [(item['from_date'], item['till_date']) for item in evidence['metadata']['rows']] == [
        ('2023-03-02', '2024-03-02'), ('2021-03-02', '2022-03-03')]
    assert len(evidence['typed_rows']) == 2
    for typed in evidence['typed_rows']:
        assert len(typed) == 8
        assert typed[0]['dtype'] == 'float64' and typed[0]['kind'] == 'float'
        assert typed[0]['hex'] == '0x1.9000000000000p+6'
        assert list(typed[0]['ratio']) == [100, 1] and typed[0]['negative_zero'] is False
        assert typed[5]['dtype'] == 'int64' and typed[5]['kind'] == 'integer'
        assert typed[5]['decimal'] == '2000'
        assert typed[6]['kind'] == 'text' and typed[6]['value'] == '2024-03-01 00:00:00'
    assert evidence['typed_rows'][0][7]['value'] == '2024-03-01 23:59:59'
    assert evidence['typed_rows'][1][7]['value'] == '2024-03-01 23:59:58'
    serialized = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    assert '2024-03-01 23:59:59' in serialized and '2024-03-01 23:59:58' in serialized
    assert all(key not in serialized for key in ('RAW_CSV_CELLS', 'body_sha256', 'codec', 'page_ordinal'))
    for actual, expected in zip(originals, before):
        gathering.pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('open_values,open_dtypes,expected_dtype,expected_value', [
    ([100, 100, 100], ['int64', 'int64', 'int64'], 'int64', 100),
    ([100, 100.0, 100], ['int64', 'float64', 'int64'], 'float64', 100.0),
    ([9007199254740994, 9007199254740994.0, 9007199254740994],
     ['int64', 'float64', 'int64'], 'float64', 9007199254740994.0),
])
def test_red_triple_equality_validates_all_original_numeric_frames_before_dedup(
        gathering, monkeypatch, open_values, open_dtypes, expected_dtype, expected_value):
    originals = [frame(gathering, [bar(open_value=value)], open_dtype=dtype)
                 for value, dtype in zip(open_values, open_dtypes)]
    before = [item.copy(deep=True) for item in originals]
    calls = windows(gathering, monkeypatch, originals)
    assemblies = []
    original = gathering._a3_assemble

    def assemble(frames, ticker):
        assemblies.append(list(frames))
        return original(frames, ticker)

    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    result = gathering.moex('SBER', 'Акции', 3, 24)
    assert len(calls) == 3 and len(assemblies) == 1
    assert len(assemblies[0]) == 3
    assert all(actual is expected for actual, expected in zip(assemblies[0], originals))
    assert_rows(result, [bar(open_value=expected_value)])
    assert str(result['open'].dtype) == expected_dtype
    assert result['open'].iloc[0] == expected_value
    for actual, expected in zip(originals, before):
        gathering.pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('values,dtypes,expected,expected_dtype', [
    ([9007199254740993, 9007199254740995], ['int64', 'int64'],
     [9007199254740995, 9007199254740993], 'int64'),
    ([2000, 2000.5], ['int64', 'float64'], [2000.5, 2000.0], 'float64'),
    ([0.0, -0.0], ['float64', 'float64'], [-0.0, 0.0], 'float64'),
])
def test_red_distinct_keys_keep_values_dtypes_and_signed_zero(
        gathering, monkeypatch, values, dtypes, expected, expected_dtype):
    originals = [frame(gathering, [bar('2024-03-01 00:00:00', open_value=values[0])], open_dtype=dtypes[0]),
                 frame(gathering, [bar('2023-03-01 00:00:00', open_value=values[1])], open_dtype=dtypes[1])]
    windows(gathering, monkeypatch, originals)
    result = gathering.moex('SBER', 'Акции', 2, 24)
    assert_rows(result, [bar('2023-03-01 00:00:00', open_value=expected[0]),
                         bar('2024-03-01 00:00:00', open_value=expected[1])])
    assert str(result['open'].dtype) == expected_dtype
    if values == [0.0, -0.0]:
        assert [math.copysign(1, value) for value in result['open']] == [-1.0, 1.0]


def test_red_unsafe_cross_window_numeric_promotion_refuses_without_prefix(gathering, monkeypatch, writers):
    originals = [frame(gathering, [bar(open_value=9007199254740993)], open_dtype='int64'),
                 frame(gathering, [bar('2023-03-01 00:00:00', open_value=2000.5)], open_dtype='float64')]
    calls = windows(gathering, monkeypatch, originals)
    with pytest.raises(ValueError, match='^A3_ISS_NUMERIC_ASSEMBLY_LOSS$'):
        gathering.moex('SBER', 'Акции', 2, 24)
    assert len(calls) == 2 and writers == []
    assert originals[0]['open'].iloc[0] == 9007199254740993


def test_red_same_key_signed_zero_is_a_conflict(gathering, monkeypatch):
    windows(gathering, monkeypatch, [frame(gathering, [bar(open_value=0.0)]),
                                    frame(gathering, [bar(open_value=-0.0)])])
    with pytest.raises(ValueError, match='^A3_ISS_ANNUAL_KEY_CONFLICT$'):
        gathering.moex('SBER', 'Акции', 2, 24)


@pytest.mark.parametrize('position', [1, 2, 3])
@pytest.mark.parametrize('kind', ['native', 'schema', 'order', 'terminal', 'resource'])
@pytest.mark.parametrize('with_context', [False, True])
def test_annual_source_failure_keeps_native_primary_and_stops_future_windows(
        gathering, monkeypatch, writers, event_log, position, kind, with_context):
    primary = {'native': OSError('ANNUAL_NATIVE'),
               'schema': ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID'),
               'order': ValueError('A3_ISS_PAGE_ORDER_INVALID'),
               'terminal': requests.ReadTimeout('ANNUAL_TERMINAL'),
               'resource': TimeoutError('A2_ISS_DEADLINE_EXCEEDED')}[kind]
    primary.a3_evidence = {'source': 'existing-native-raw-evidence'}
    raw_evidence = primary.a3_evidence
    args = primary.args
    outcomes = [frame(gathering, [bar(f'202{4-i}-03-01 00:00:00')]) for i in range(3)]
    outcomes[position - 1] = primary
    calls = windows(gathering, monkeypatch, outcomes)
    context = event_log.context if with_context else None
    with pytest.raises(type(primary)) as captured:
        gathering.moex('SBER', 'Акции', 3, 24, logging_context=context)
    assert captured.value is primary and primary.args == args and primary.a3_evidence is raw_evidence
    assert len(calls) == position and all(item[1] is context for item in calls) and writers == []
    assert gathering.exception_list == ['SBER']


@pytest.mark.parametrize('has_source_primary', [False, True])
def test_secondary_exception_list_failure_cannot_replace_primary(gathering, monkeypatch,
                                                                has_source_primary):
    primary = OSError('ANNUAL_SOURCE_OR_CAPTURE')

    def append(*args):
        raise RuntimeError('SECONDARY_EXCEPTION_LIST')

    monkeypatch.setattr(gathering, 'exception_list', SimpleNamespace(append=append))
    calls = windows(gathering, monkeypatch,
                    [primary] if has_source_primary else [frame(gathering, [bar()])])
    if not has_source_primary:
        def capture(*args, **kwargs):
            raise primary
        monkeypatch.setattr(gathering, '_a3_annual_row', capture, raising=False)
    with pytest.raises(OSError) as captured:
        gathering.moex('SBER', 'Акции', 3, 24)
    assert captured.value is primary and primary.args == ('ANNUAL_SOURCE_OR_CAPTURE',)
    assert len(calls) == 1


@pytest.mark.parametrize('primary', [MemoryError('ANNUAL_STORAGE'),
                                    requests.ConnectionError('ANNUAL_CAPTURE'),
                                    KeyboardInterrupt('ANNUAL_CAPTURE')])
def test_preliminary_annual_capture_fault_is_terminal_not_transport_retry(
        gathering, monkeypatch, writers, primary):
    calls = windows(gathering, monkeypatch, [frame(gathering, [bar()])])
    def capture(*args, **kwargs):
        raise primary
    monkeypatch.setattr(gathering, '_a3_annual_row', capture, raising=False)
    args = primary.args
    with pytest.raises(type(primary)) as captured:
        gathering.moex('SBER', 'Акции', 3, 24)
    assert captured.value is primary and primary.args == args
    assert len(calls) == 1 and writers == []


@pytest.mark.parametrize('fault', ['capture', 'payload-cap', 'metadata-cap', 'attribute'])
def test_conflict_evidence_fault_preserves_same_primary(gathering, monkeypatch, fault):
    originals = [frame(gathering, [bar()]),
                 frame(gathering, [bar(end='2024-03-01 23:59:58')])]
    calls = windows(gathering, monkeypatch, originals)
    primaries = []
    if fault == 'attribute':
        class RejectEvidence(ValueError):
            def __setattr__(self, name, value):
                if name == 'a3_annual_evidence':
                    primaries.append((self, self.args))
                    raise OSError('SECONDARY_ATTRIBUTE')
                return super().__setattr__(name, value)
        monkeypatch.setattr(gathering, 'ValueError', RejectEvidence, raising=False)
    else:
        original = getattr(gathering, '_a3_annual_evidence', None)
        def evidence(primary, *args, **kwargs):
            primaries.append((primary, primary.args))
            if fault == 'capture':
                raise OSError('SECONDARY_EVIDENCE')
            return original(primary, *args, **kwargs)
        monkeypatch.setattr(gathering, '_a3_annual_evidence', evidence, raising=False)
        if fault == 'payload-cap':
            monkeypatch.setattr(gathering, '_A3_ANNUAL_EVIDENCE_BYTES', 0, raising=False)
        if fault == 'metadata-cap':
            monkeypatch.setattr(gathering, '_A3_ANNUAL_METADATA_BYTES', 0, raising=False)
    with pytest.raises(ValueError) as captured:
        gathering.moex('SBER', 'Акции', 2, 24)
    assert primaries and captured.value is primaries[0][0]
    assert captured.value.args == primaries[0][1] == ('A3_ISS_ANNUAL_KEY_CONFLICT',)
    assert len(calls) == 2
    assert 'A3_EVIDENCE_FAILED_UNVERIFIED' in getattr(captured.value, '__notes__', [])


def test_typed_evidence_is_bounded_private_and_has_no_raw_csv_claim(
        gathering, monkeypatch, event_log, capsys):
    class Unrenderable:
        def __repr__(self):
            raise AssertionError('Unknown context object was reprd')
        def __str__(self):
            raise AssertionError('Unknown context object was stringified')

    context = dict(event_log.context, snapshot_id=Unrenderable(), dataset_id='META_CANARY_' * 2000)
    original_context = dict(context)
    calls = windows(gathering, monkeypatch, [frame(gathering, [bar()]),
                    frame(gathering, [bar(end='2024-03-01 23:59:58')])])
    with pytest.raises(ValueError, match='^A3_ISS_ANNUAL_KEY_CONFLICT$') as captured:
        gathering.moex('SBER', 'Акции', 2, 24, logging_context=context)
    evidence = captured.value.a3_annual_evidence
    assert len(evidence['typed_rows']) == 2
    metadata = evidence['metadata']
    assert metadata['format'] == 'A3_ANNUAL_TYPED_ROWS_V1' and metadata['state'] == 'CAPTURED'
    encoded_metadata = json.dumps(metadata, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
    assert len(encoded_metadata) <= 16 * 1024
    assert len(json.dumps(evidence['typed_rows'], ensure_ascii=True).encode()) <= 64 * 1024
    assert 'META_CANARY_' not in encoded_metadata.decode()
    assert context == original_context and all(item[1] is context for item in calls)
    public = json.dumps(event_log.events, ensure_ascii=True) + capsys.readouterr().out
    assert 'META_CANARY_' not in public and 'typed_rows' not in public


class AnnualRawBody:
    def __init__(self, content, headers):
        self.buffer = io.BytesIO(content)
        self.headers = HTTPHeaderDict(headers)
        self.close_calls = 0

    def read(self, amt=None, decode_content=False):
        assert type(amt) is int and 0 < amt <= 65536 and decode_content is False
        return self.buffer.read(amt)

    def close(self):
        self.close_calls += 1
        self.buffer.close()

    def release_conn(self):
        pass


def response(text, status=200):
    item = requests.Response()
    item.status_code = status
    item._content = text.encode('utf-8')
    item.encoding = 'utf-8'
    item.headers['Content-Type'] = 'text/csv; charset=utf-8'
    item.raw = AnnualRawBody(item._content, item.headers.items())
    return item


def http(gathering, monkeypatch, outcomes):
    prepared = iter(outcomes)
    state = SimpleNamespace(calls=[], sleeps=[], elapsed=0.0, after_get=None)
    def get(url, headers, timeout, verify=None, allow_redirects=None, stream=None):
        params = parse_qs(urlsplit(url).query, keep_blank_values=True)
        state.calls.append(dict(url=url, params=params, start=int(params['start'][0]),
                                headers=dict(headers), timeout=timeout, verify=verify,
                                allow_redirects=allow_redirects, stream=stream))
        try:
            result = next(prepared)
        except StopIteration:
            pytest.fail('Unexpected HTTP request beyond literal annual/page script')
        if state.after_get is not None:
            state.after_get()
        if isinstance(result, BaseException):
            raise result
        return result
    def sleep(seconds):
        state.sleeps.append(seconds)
        state.elapsed += seconds
    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'header', {'User-Agent': 'AGENT_A'})
    monkeypatch.setattr(gathering, 'get_next_header', lambda current: {'User-Agent': 'AGENT_B'})
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(
        sleep=sleep, perf_counter=lambda: state.elapsed, time=lambda: 1709380800.0))
    return state


@pytest.mark.parametrize('interval', [24, 60, 10, 1])
@pytest.mark.parametrize('kind,market', [('Акции', 'shares'), ('Облигации', 'bonds')])
def test_red_real_csv_halo_extra_page_retry_and_terminal_are_literal(
        gathering, monkeypatch, annual_fixture, interval, kind, market):
    wire = annual_fixture['wire']
    header = wire['header']
    payload = lambda rows: header + ''.join(row + '\n' for row in rows)
    prepared = [response('', 503), response(payload(wire['newer'])), response(header),
                response(payload(wire['older'][:1])), response(payload(wire['older'][1:2])),
                response(payload(wire['older'][2:])), response(header)]
    state = http(gathering, monkeypatch, prepared)
    result = gathering.moex('FIXTURE_SHARE', kind, 2, interval)
    assert [item['start'] for item in state.calls] == wire['starts_with_retry']
    assert state.sleeps == wire['sleep_with_retry']
    for position, call in enumerate(state.calls):
        assert urlsplit(call['url']).path == f'/iss/engines/stock/markets/{market}/securities/FIXTURE_SHARE/candles.csv'
        assert call['params']['from'] == ['2023-03-02' if position < 3 else '2022-03-02']
        assert call['params']['till'] == ['2024-03-02' if position < 3 else '2023-03-03']
        assert call['params']['interval'] == [str(interval)]
        assert call['headers'] == {'User-Agent': 'AGENT_A' if position == 0 else 'AGENT_B',
                                   'Accept-Encoding': 'identity'}
        assert call['verify'] is True and call['allow_redirects'] is False and call['stream'] is True
        assert call['timeout'] == (5, 30)
    assert all(item.raw.close_calls == 1 for item in prepared)
    assert result.begin.tolist() == wire['expected_begin']
    assert result.index.tolist() == [0, 1, 2, 3] and result.columns.tolist() == COLUMNS
    assert result.ticker.tolist() == ['FIXTURE_SHARE'] * 4
    assert gathering._A3_MAX_PAGES == 1000 and gathering._A3_MAX_BYTES == 64 * 1024 * 1024


def test_real_within_query_duplicate_remains_a2_refusal(gathering, monkeypatch, annual_fixture):
    wire = annual_fixture['wire']
    body = wire['header'] + (wire['newer'][0] + '\n') * 2
    state = http(gathering, monkeypatch, [response(body)])
    primaries = []
    original = gathering.moex_query
    def query(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except BaseException as primary:
            primaries.append(primary)
            raise
    monkeypatch.setattr(gathering, 'moex_query', query)
    with pytest.raises(ValueError, match='^A2_ISS_RESPONSE_SCHEMA_INVALID$') as captured:
        gathering.moex('SBER', 'Акции', 2, 24)
    assert captured.value is primaries[0] and len(state.calls) == 1
    assert captured.value.a3_evidence['metadata']['format'] == 'RAW_CSV_CELLS_V1'


def test_red_late_real_query_deadline_preserves_primary_and_has_no_next_window(
        gathering, monkeypatch, annual_fixture, writers):
    wire = annual_fixture['wire']
    header = wire['header']
    payload = lambda rows: header + ''.join(row + '\n' for row in rows)
    prepared = [response(payload(wire['newer'])), response(header),
                response(payload(wire['older'][:1])), response(payload(wire['older'][1:2]))]
    state = http(gathering, monkeypatch, prepared)
    def expire():
        if len(state.calls) == 4:
            state.elapsed = 186.0  # Query2 started at6; its own180s budget expires.
    state.after_get = expire
    primaries = []
    original = gathering.moex_query
    def query(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except BaseException as primary:
            primaries.append((primary, primary.args))
            raise
    monkeypatch.setattr(gathering, 'moex_query', query)
    with pytest.raises(requests.exceptions.Timeout) as captured:
        gathering.moex('FIXTURE_SHARE', 'Акции', 3, 1)
    assert primaries and captured.value is primaries[0][0]
    assert captured.value.args == primaries[0][1] == ('A2_ISS_DEADLINE_EXCEEDED',)
    assert [item['start'] for item in state.calls] == [0, 3, 0, 1]
    assert state.sleeps == [3, 3, 3] and writers == []
    assert all(item.raw.close_calls == 1 for item in prepared)


@pytest.mark.parametrize('operation', ['reload', 'update'])
@pytest.mark.parametrize('with_context', [False, True])
def test_callers_late_failure_preserves_previous_without_a_new_writer(
        gathering, monkeypatch, catalogue, snapshot, candle_frame, fixture_root,
        exact_reads, writers, save_dataset, event_log, operation, with_context):
    root_path, root = fixture_root
    previous = {}
    target = root_path / 'datasets/annual_previous.csv'
    target.parent.mkdir(parents=True)
    for suffix in ('csv', 'xlsx'):
        path = target.with_suffix('.' + suffix)
        path.write_bytes(('PREVIOUS_ANNUAL_' + suffix).encode())
        previous[path] = (path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest())
    if operation == 'update':
        content = save_dataset(target, candle_frame)
        previous[target] = (content, hashlib.sha256(content).hexdigest())
        exact_reads.declared[target] = candle_frame
    primary = requests.ReadTimeout('LATE_CALLER_SOURCE')
    calls = []
    context = event_log.context if with_context else None
    def query(*args, logging_context=None):
        calls.append((args, logging_context))
        if len(calls) == 2:
            raise primary
        result = candle_frame.copy(deep=True)
        result['ticker'] = args[0]
        return result
    monkeypatch.setattr(gathering, 'moex_query', query)
    with pytest.raises(requests.ReadTimeout) as captured:
        if operation == 'reload':
            gathering.full_reload(catalogue, 24, 30, 'annual_previous', 'часа', root, snapshot,
                                  logging_context=context)
        else:
            gathering.data_update([{'interval': 24, 'years': 30, 'filename': 'annual_previous', 'word': 'часа'}],
                                  root, catalogue, snapshot, logging_context=context)
    assert captured.value is primary and primary.args == ('LATE_CALLER_SOURCE',)
    assert len(calls) == 2 and all(item[1] is context for item in calls) and writers == []
    assert all((path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest()) == value
               for path, value in previous.items())


@pytest.mark.parametrize('anchor,lower', [('0002-03-01', '0001-03-01'),
                                        ('1000-03-01', '0999-03-01'),
                                        ('1001-03-01', '1000-03-01'),
                                        ('2024-02-29', '2023-02-28')])
@pytest.mark.parametrize('operation', ['reload', 'update'])
def test_red_both_caller_calendar_sites_use_portable_literal_tokens(
        gathering, monkeypatch, catalogue, snapshot, candle_frame, fixture_root,
        exact_reads, writers, save_dataset, anchor, lower, operation):
    clock = freeze(gathering, monkeypatch, anchor, unpadded=True)
    root_path, root = fixture_root
    queries = []
    def query(*args, logging_context=None):
        queries.append(args)
        return gathering.pd.DataFrame()
    monkeypatch.setattr(gathering, 'moex_query', query)
    if operation == 'reload':
        dates = gathering.pd.DataFrame({'TRADE_CODE': ['SBER'], 'issue_date': ['0001-01-01'],
                                        'stopped_date': [gathering.pd.NaT]})
        gathering.full_reload(catalogue.iloc[[1]].copy(), 24, 1, 'portable', 'часа', root, dates)
        assert queries == [('SBER', 'Акции', lower, anchor, 24)]
    else:
        old = candle_frame.copy(deep=True)
        old['begin'] = '0001-01-01 00:00:00'
        old['end'] = '0001-01-01 23:59:59'
        target = root_path / 'datasets/portable.csv'
        target.parent.mkdir(parents=True)
        previous = save_dataset(target, old)
        exact_reads.declared[target] = old
        gathering.data_update([{'interval': 24, 'years': 30, 'filename': 'portable', 'word': 'часа'}],
                              root, catalogue, snapshot)
        assert queries == [('MOEX', 'Акции', lower, anchor, 24)]
        assert target.read_bytes() == previous
    # Keep the separate legacy 30-day-filter now() outside the calendar fix.
    assert clock == ([None] if operation == 'reload' else [None, None])


def test_full_reload_zero_years_keeps_listing_and_stopped_precedence(
        gathering, monkeypatch, catalogue, snapshot, fixture_root, writers):
    _, root = fixture_root
    calls = windows(gathering, monkeypatch, [gathering.pd.DataFrame(), gathering.pd.DataFrame()])
    gathering.full_reload(catalogue, 24, 0, 'all_history_sentinel', 'часа', root, snapshot)
    assert [args for args, _ in calls] == [('MOEX', 'Акции', '2020-02-29', '2024-03-01', 24),
                                          ('SBER', 'Акции', '2000-01-03', '2024-03-02', 24)]
    assert writers == []
