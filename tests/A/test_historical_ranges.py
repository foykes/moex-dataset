"""Literal A3-FINAL oracles. Endpoint models establish no real ISS contract."""

import datetime
import copy
import hashlib
import io
import json
import math
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest
import requests
import time
from urllib3._collections import HTTPHeaderDict


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures/a3/historical_ranges.json'
LITERALS = json.loads(FIXTURE.read_text(encoding='utf-8'))
COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
HEADER = 'candles\nopen;close;high;low;value;volume;begin;end\n'
GiB = 1024 ** 3


def candle(begin='2024-03-01 00:00:00', *, ticker='SBER', number=100.0, end=None):
    return pd.DataFrame([[number, 101., 102., 99., 202000., 2000, begin,
                          end or begin[:10] + ' 23:59:59', ticker]], columns=COLUMNS)


def mock_queries(gathering, monkeypatch, outcomes):
    prepared = iter(outcomes)
    calls = []
    def query(*args, logging_context=None):
        calls.append((args, logging_context))
        result = next(prepared)
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(gathering, 'moex_query', query)
    return calls


class Raw:
    def __init__(self, content, headers):
        self.body = io.BytesIO(content)
        self.headers = HTTPHeaderDict(headers)
    def read(self, amount, decode_content=False):
        assert 0 < amount <= 65536 and decode_content is False
        return self.body.read(amount)
    def close(self):
        self.body.close()
    def release_conn(self):
        pass


def response(rows=(), status=200):
    value = requests.Response()
    value.status_code = status
    value.headers['Content-Type'] = 'text/csv; charset=utf-8'
    value.raw = Raw((HEADER + ''.join(row + '\n' for row in rows)).encode(), value.headers.items())
    return value


def wire(gathering, monkeypatch, outcomes):
    prepared = iter(outcomes)
    state = SimpleNamespace(calls=[], sleeps=[], elapsed=0.)
    def get(url, **kwargs):
        params = parse_qs(urlsplit(url).query)
        state.calls.append((params, kwargs))
        result = next(prepared)
        if isinstance(result, BaseException):
            raise result
        return result
    def sleep(seconds):
        state.sleeps.append(seconds)
        state.elapsed += seconds
    monkeypatch.setattr(gathering.requests, 'get', get)
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(
        perf_counter=lambda: state.elapsed, sleep=sleep, time=lambda: 1709380800.))
    return state


@pytest.mark.parametrize('case', LITERALS['calendar_cases'])
def test_literal_calendar_leafs_and_unchanged_outer_bounds(gathering, case):
    plan = gathering._a3_range_plan(case['from'], case['till'], case['interval'])
    assert [(leaf['from_date'], leaf['till_date']) for leaf in plan] == [tuple(item) for item in case['requests']]
    assert all(case['from'] <= leaf['from_date'] <= leaf['till_date'] <= case['till'] for leaf in plan)
    assert plan[0]['till_date'] == case['till'] and plan[-1]['from_date'] == case['from']


@pytest.mark.parametrize('years', [1, 2, 10, 30])
@pytest.mark.parametrize('interval,factor', [(24, 1), (60, 1), (10, 12), (1, 53)])
def test_logical_n_physical_n_12n_53n(gathering, monkeypatch, years, interval, factor):
    calls = mock_queries(gathering, monkeypatch, [pd.DataFrame()] * (years * factor))
    result = gathering.moex('SBER', 'Акции', years, interval)
    assert len(calls) == years * factor
    assert calls[0][0][3] == '2024-03-02'
    assert calls[-1][0][2] == f'{2024-years:04d}-03-02'
    assert result.empty and result.columns.tolist() == []


@pytest.mark.parametrize('bounds', [('2024-03-02', '2024-03-01'), ('NaT', '2024-03-02'),
                                   ('2024-02-30', '2024-03-02'), ('1-01-01', '2024-03-02')])
def test_invalid_explicit_bounds_refuse_before_get(gathering, monkeypatch, bounds):
    calls = mock_queries(gathering, monkeypatch, [])
    with pytest.raises(ValueError, match='A3_ISS_RANGE_BOUNDS_INVALID'):
        gathering._a3_history('SBER', 'Акции', *bounds, 1)
    assert calls == []


def test_sparse_empty_middle_does_not_end_older_acquisition(gathering, monkeypatch):
    newer = candle('2024-02-29 00:00:00')
    older = candle('2024-02-10 00:00:00', number=90.)
    calls = mock_queries(gathering, monkeypatch, [newer, pd.DataFrame(), older])
    result = gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(calls) == 3
    assert result.begin.tolist() == ['2024-02-10 00:00:00', '2024-02-29 00:00:00']
    assert result.open.tolist() == [90., 100.]
    assert result.columns.tolist() == COLUMNS and result.index.tolist() == [0, 1]


def test_flat_originals_late_conflict_and_first_locator(gathering, monkeypatch):
    first = candle('2024-02-25 00:00:00')
    repeat = first.copy(deep=True)
    conflict = candle('2024-02-25 00:00:00', end='2024-02-25 23:59:58')
    calls = mock_queries(gathering, monkeypatch, [first, repeat, conflict])
    with pytest.raises(ValueError, match='A3_ISS_RANGE_KEY_CONFLICT') as captured:
        gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(calls) == 3
    evidence = captured.value.a3_range_evidence
    assert [row['physical_ordinal'] for row in evidence['metadata']['rows']] == [1, 3]
    assert len(evidence['typed_rows']) == 2
    pd.testing.assert_frame_equal(first, repeat)


@pytest.mark.parametrize('bad_position', [0, 1, 2])
@pytest.mark.parametrize('primary', [requests.exceptions.Timeout('literal-timeout'),
                                    requests.exceptions.SSLError('literal-tls'), MemoryError('literal-memory')])
def test_first_middle_final_primary_no_later_query_or_writer(gathering, monkeypatch, writers,
                                                            bad_position, primary):
    outcomes = [candle()] * bad_position + [primary]
    calls = mock_queries(gathering, monkeypatch, outcomes)
    original_args = primary.args
    with pytest.raises(type(primary)) as captured:
        gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert captured.value is primary and captured.value.args == original_args
    assert len(calls) == bad_position + 1 and writers == []


def test_original_numeric_audit_precedes_removing_repeats(gathering, monkeypatch):
    first = candle('2024-02-25 00:00:00', number=9007199254740993)
    first['open'] = pd.Series([9007199254740993], dtype='int64')
    second = candle('2024-02-18 00:00:00', number=1.5)
    calls = mock_queries(gathering, monkeypatch, [first, first.copy(), second])
    with pytest.raises(ValueError, match='A3_ISS_NUMERIC_ASSEMBLY_LOSS'):
        gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(calls) == 3 and int(first.open.iloc[0]) == 9007199254740993


@pytest.mark.parametrize('interval', [24, 60, 10, 1])
def test_equal_bounds_terminal_and_retry_keep_all_request_parameters(gathering, monkeypatch, interval):
    row = '100;101;102;99;202000;2000;2024-03-01 00:00:00;2024-03-01 23:59:59'
    state = wire(gathering, monkeypatch, [response(status=503), response([row]), response()])
    result = gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-01', interval)
    assert len(result) == 1 and result.volume.tolist() == [2000]
    assert [int(item[0]['start'][0]) for item in state.calls] == [0, 0, 1]
    assert state.sleeps == [3, 3, 3]
    for params, options in state.calls:
        assert params['from'] == params['till'] == ['2024-03-01']
        assert params['interval'] == [str(interval)]
        assert options['verify'] is True and options['allow_redirects'] is False


def memory_snapshot(gathering):
    return dict(status='AVAILABLE', sampled_at=gathering.time.perf_counter(),
                total_phys=64*GiB, available_phys=48*GiB,
                commit_limit=128*GiB, commit_available=64*GiB,
                process_available=16*GiB, quota_available=16*GiB,
                private_bytes=512*1024**2, working_set=256*1024**2,
                page_size=4096, quotas_verified=True)


@pytest.mark.parametrize('failure', ['unavailable', 'stale', 'physical', 'commit', 'quota', 'page-size'])
def test_native_memory_admission_fails_closed(gathering, monkeypatch, failure):
    sample = memory_snapshot(gathering)
    if failure == 'unavailable':
        sample = None
    elif failure == 'stale':
        sample['sampled_at'] -= 2
    elif failure == 'physical':
        sample['available_phys'] = 16*GiB
    elif failure == 'commit':
        sample['commit_available'] = 8*GiB
    elif failure == 'quota':
        sample['quota_available'] = 1024
    else:
        sample['page_size'] = 0
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
        gathering._a3_memory_admit(GiB)


def test_memory_admission_uses_native_reserve_not_dataframe_payload(gathering, monkeypatch):
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: memory_snapshot(gathering))
    gathering._a3_memory_admit(GiB)
    with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
        gathering._a3_memory_admit(8*GiB)


@pytest.mark.parametrize('model', LITERALS['capacity_models'])
@pytest.mark.parametrize('instruments', [1, 10, 50, 200])
@pytest.mark.parametrize('density', ['dense', 'sparse', 'empty', 'inactive'])
def test_resource_workload_literal_arithmetic(gathering, model, instruments, density):
    rows = 0 if density == 'empty' else model[density+'_rows']
    gets = model['ranges'] if density == 'empty' else model[density+'_gets']
    ranges = model['ranges'] * instruments
    assert ranges <= 1048576
    assert gets * instruments <= 16777216
    assert 3 * gets * instruments >= 3 * ranges
    admissible = rows * instruments <= 128000000
    if density == 'dense' and model['interval'] == 1 and instruments == 200:
        assert not admissible
    assert LITERALS['source_inclusivity_membership_finality'] == 'NOT_VERIFIED'


def test_live_protocol_is_eight_bounded_cases_with_no_implicit_authority(gathering):
    plan = gathering._a3_source_diagnostic_plan()
    assert len(plan) == 8
    assert {(row['ticker'], row['interval']) for row in plan} == {
        (ticker, interval) for ticker in ['SBER', 'MOEX'] for interval in [24, 60, 10, 1]}
    assert all(row['authority'] == 'SEPARATE_LIVE_GRANT_REQUIRED' for row in plan)
    assert LITERALS['source_diagnostic']['max_gets'] == 512


def test_real_native_probe_reports_process_and_job_headroom(gathering):
    sample = gathering._a3_memory_snapshot()
    if gathering.sys.platform == 'win32':
        assert sample is not None and sample['status'] == 'AVAILABLE'
        assert sample['page_size'] in (4096, 65536)
        assert sample['private_bytes'] > 0 and sample['working_set'] > 0
        assert 0 <= sample['quota_available'] <= sample['app_available_commit']
        assert sample['quotas_verified'] is (not sample['job_member'])
        if sample['job_member']:
            assert sample['quota_basis'] == 'UNKNOWN_ANCESTOR_JOBS'
            with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
                gathering._a3_memory_admit(GiB)
    elif sample is None:
        assert gathering.sys.platform.startswith('linux')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_memory_admit(GiB)
    else:
        assert sample['status'] == 'AVAILABLE' and sample['platform'] == 'linux'
        assert sample['page_size'] > 0 and sample['private_bytes'] is None
        assert sample['vm_size_bytes'] >= sample['vm_data_bytes'] >= 0
        assert sample['working_set'] >= 0 and sample['quotas_verified'] is True
        assert sample['quota_basis'] == 'INITIAL_CGROUP_NAMESPACE_FULL_ANCESTRY'
        assert 0 <= sample['quota_available'] <= sample['process_available']
    print('A3_NATIVE_MEMORY_SNAPSHOT=' + json.dumps(sample, sort_keys=True))


@pytest.mark.parametrize('interval', [24, 60, 10, 1])
def test_mocked_wire_dense_membership_every_leaf_and_terminal(gathering, monkeypatch, interval):
    # Three days, two records/page: independent literal membership and wire offsets.
    rows = ['100;101;102;99;202000;2000;2024-02-29 00:00:00;2024-02-29 23:59:59',
            '101;102;103;100;202001;2001;2024-03-01 00:00:00;2024-03-01 23:59:59',
            '102;103;104;101;202002;2002;2024-03-02 00:00:00;2024-03-02 23:59:59']
    state = wire(gathering, monkeypatch, [response(rows[:2]), response(rows[2:]), response()])
    result = gathering._a3_history('SBER', 'Акции', '2024-02-29', '2024-03-02', interval)
    assert result.begin.tolist() == ['2024-02-29 00:00:00', '2024-03-01 00:00:00', '2024-03-02 00:00:00']
    assert result.open.tolist() == [100, 101, 102] and result.volume.tolist() == [2000, 2001, 2002]
    assert [params['start'] for params, _ in state.calls] == [['0'], ['2'], ['3']]
    assert state.sleeps == [3, 3, 3]
    assert result.columns.tolist() == COLUMNS and result.index.tolist() == [0, 1, 2]


@pytest.mark.parametrize('scope', ['job', 'dataset', 'run'])
@pytest.mark.parametrize('counter,limit', [('gets', 1), ('rows', 0), ('bytes', 10), ('ranges', 0)])
def test_real_ledger_caps_no_prefix_next_get_or_writer(gathering, monkeypatch, writers, scope, counter, limit):
    limits = copy.deepcopy(gathering._A3_RANGE_LIMITS)
    limits[scope][counter] = limit
    monkeypatch.setattr(gathering, '_A3_RANGE_LIMITS', limits)
    row = '100;101;102;99;202000;2000;2024-03-01 00:00:00;2024-03-01 23:59:59'
    state = wire(gathering, monkeypatch, [response([row]), response()])
    with pytest.raises(ValueError, match='^A3_ISS_RANGE_CAPACITY_EXCEEDED$'):
        gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 1)
    assert len(state.calls) == (0 if counter == 'ranges' else 1)
    assert writers == [] and gathering._A3_RANGE_STATE.get() is None


@pytest.mark.parametrize('scope', ['dataset', 'run'])
def test_ledger_retains_counters_across_jobs(gathering, monkeypatch, scope):
    limits = copy.deepcopy(gathering._A3_RANGE_LIMITS)
    limits[scope]['rows'] = 1
    monkeypatch.setattr(gathering, '_A3_RANGE_LIMITS', limits)
    calls = mock_queries(gathering, monkeypatch, [candle(), candle(), candle()])
    with gathering._a3_scope('dataset') as ledger:
        gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
        assert ledger[scope]['rows'] == 1
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('MOEX', 'Акции', '2024-03-01', '2024-03-02', 24)
        assert ledger[scope]['rows'] == 1
    assert len(calls) == 2


@pytest.mark.parametrize('counter', ['datasets', 'jobs'])
def test_run_cardinality_caps_are_not_reset(gathering, monkeypatch, counter):
    limits = copy.deepcopy(gathering._A3_RANGE_LIMITS)
    limits['run'][counter] = 1
    monkeypatch.setattr(gathering, '_A3_RANGE_LIMITS', limits)
    calls = mock_queries(gathering, monkeypatch, [pd.DataFrame(), pd.DataFrame()])
    with gathering._a3_scope('run'):
        gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('MOEX', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert len(calls) == 1


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('with_context', [False, True])
def test_deadline_after_terminal_keeps_native_precedence(gathering, monkeypatch, event_log, native, with_context):
    state = wire(gathering, monkeypatch, [response()])
    original = gathering._a3_assemble
    calls = []
    def assemble(*args):
        calls.append(args)
        if native:
            state.elapsed = 180.
        else:
            # Native empty-query assembly succeeds first. Expire aggregate assembly only.
            if len(calls) == 2:
                state.elapsed = 22000.
        return original(*args)
    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    context = event_log.context if with_context else None
    with pytest.raises(requests.Timeout) as captured:
        gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24, logging_context=context)
    assert captured.value.args == (('A2_ISS_DEADLINE_EXCEEDED' if native else 'A3_ISS_RANGE_DEADLINE_EXCEEDED'),)
    assert len(state.calls) == 1 and state.sleeps == [3]
    assert not any(event['event'] == 'a3_ranges_returned' for event in event_log.events)
    if native:
        assert not any(event['event'] == 'a2_query_returned' for event in event_log.events)
    if with_context:
        event = next(item for item in event_log.events if item.get('error') and item['error']['code'] == captured.value.args[0])
        assert event['error']['category'] == 'TIMEOUT'


@pytest.mark.parametrize('fault', ['capture', 'assembly', 'evidence', 'logging'])
def test_fault_injection_is_terminal_and_primary_is_preserved(gathering, monkeypatch, writers, event_log, fault):
    primary = MemoryError('PRIVATE_MEMORY_CANARY')
    rows = '100;101;102;99;202000;2000;2024-03-01 00:00:00;2024-03-01 23:59:59'
    state = wire(gathering, monkeypatch, [response([rows]), response()])
    context = event_log.context
    if fault == 'capture':
        monkeypatch.setattr(gathering, '_a3_capture', lambda *a: (_ for _ in ()).throw(primary))
    elif fault == 'assembly':
        monkeypatch.setattr(gathering, '_a3_assemble', lambda *a: (_ for _ in ()).throw(primary))
    elif fault == 'logging':
        original = event_log.logger.emit_event
        def emit(context, level, event, *args):
            if event == 'a2_query_failed':
                raise OSError('SECONDARY_LOG_CANARY')
            return original(context, level, event, *args)
        monkeypatch.setattr(event_log.logger, 'emit_event', emit)
        monkeypatch.setattr(gathering, '_a3_capture', lambda *a: (_ for _ in ()).throw(primary))
    else:
        monkeypatch.setattr(gathering, '_a3_capture', lambda *a: (_ for _ in ()).throw(primary))
        monkeypatch.setattr(gathering, '_a3_evidence', lambda *a, **kw: (_ for _ in ()).throw(OSError('SECONDARY_EVIDENCE_CANARY')))
    original_args = primary.args
    with pytest.raises(MemoryError) as captured:
        gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1, logging_context=context)
    assert captured.value is primary and primary.args == original_args
    assert len(state.calls) == (2 if fault == 'assembly' else 1) and writers == []
    assert not any(item['event'] in ('a2_query_returned', 'a3_ranges_returned') for item in event_log.events)


def test_original_frames_all_reach_flat_assembly_before_dedup(gathering, monkeypatch):
    first = candle('2024-02-25 00:00:00')
    originals = [first, first.copy(), candle('2024-02-18 00:00:00')]
    observed = []
    original = gathering._a3_assemble
    def assemble(frames, ticker):
        observed.append(list(frames))
        return original(frames, ticker)
    monkeypatch.setattr(gathering, '_a3_assemble', assemble)
    mock_queries(gathering, monkeypatch, originals)
    result = gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(observed) == 1 and all(a is b for a, b in zip(observed[0], originals))
    assert len(observed[0]) == 3 and sum(len(frame) for frame in observed[0]) == 3
    assert result.begin.tolist() == ['2024-02-18 00:00:00', '2024-02-25 00:00:00']


def test_signed_zero_is_a_conflict_not_keep_first(gathering, monkeypatch):
    first, second = candle(number=0.0), candle(number=-0.0)
    mock_queries(gathering, monkeypatch, [first, second])
    with pytest.raises(ValueError, match='A3_ISS_RANGE_KEY_CONFLICT') as captured:
        gathering._a3_history('SBER', 'Акции', '2024-02-17', '2024-03-02', 1)
    typed = captured.value.a3_range_evidence['typed_rows']
    assert len(typed) == 2
    assert math.copysign(1, first.open.iloc[0]) == 1 and math.copysign(1, second.open.iloc[0]) == -1


def test_probe_deterioration_after_received_leaf_refuses_without_next(gathering, monkeypatch):
    calls = mock_queries(gathering, monkeypatch, [candle(), candle(), candle()])
    def snapshot():
        sample = memory_snapshot(gathering)
        if len(calls) >= 1:
            sample['quota_available'] = 0
        return sample
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', snapshot)
    monkeypatch.setattr(gathering, '_a3_allocation_admit', lambda growth, *args: gathering._a3_memory_admit(growth))
    with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
        gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(calls) == 1


@pytest.mark.parametrize('operation', ['reload', 'existing', 'new', 'cold'])
@pytest.mark.parametrize('failure', ['source', 'conflict', 'capacity', 'diagnostic'])
@pytest.mark.parametrize('with_context', [False, True])
def test_all_callers_late_failure_preserves_affected_previous_and_stops_dataset(
        gathering, catalogue, snapshot, candle_frame, fixture_root, exact_reads,
        save_dataset, writers, monkeypatch, event_log, operation, failure, with_context):
    root_path, root = fixture_root
    target = root_path / 'datasets/final_previous.csv'
    target.parent.mkdir(parents=True)
    previous = {}
    for suffix in ('csv', 'xlsx'):
        path = target.with_suffix('.' + suffix)
        if operation == 'cold' and suffix == 'csv':
            continue
        path.write_bytes(('PREVIOUS_A3_FINAL_' + suffix).encode())
        previous[path] = hashlib.sha256(path.read_bytes()).hexdigest()
    source = catalogue.iloc[[1]].copy() if operation != 'new' else catalogue
    dates = snapshot.copy(deep=True)
    dates['issue_date'] = ['2024-02-10', '2024-02-10']
    dates['stopped_date'] = pd.NaT
    if operation in ('existing', 'new'):
        stored = candle('2024-02-10 00:00:00', end='2024-02-10 23:59:59')
        if operation == 'new':
            stored['begin'], stored['end'] = '2023-01-01 00:00:00', '2023-01-01 23:59:59'
        content = save_dataset(target, stored)
        previous[target] = hashlib.sha256(content).hexdigest()
        exact_reads.declared[target] = stored
    primary = requests.exceptions.SSLError('A3_PRIVATE_SOURCE_CANARY')
    context = event_log.context if with_context else None
    calls = []
    def query(*args, logging_context=None):
        calls.append(args)
        assert logging_context is context
        if len(calls) == 2 and failure == 'source':
            raise primary
        result = candle(ticker=args[0])
        if len(calls) == 2 and failure == 'conflict':
            result.loc[0, 'close'] = 101.5
        return result
    monkeypatch.setattr(gathering, 'moex_query', query)
    if failure == 'capacity':
        limits = copy.deepcopy(gathering._A3_RANGE_LIMITS)
        limits['job']['rows'] = 1
        monkeypatch.setattr(gathering, '_A3_RANGE_LIMITS', limits)
    if failure == 'diagnostic':
        original = gathering._a3_range_event
        def diagnostic(context, event, *args):
            if event == 'a3_ranges_returned':
                raise RuntimeError('LOGGING_INCOMPLETE')
            return original(context, event, *args)
        monkeypatch.setattr(gathering, '_a3_range_event', diagnostic)
    expected_type = requests.exceptions.SSLError if failure == 'source' else RuntimeError if failure == 'diagnostic' else ValueError
    specs = [dict(interval=1, years=30, filename='final_previous', word='минута'),
             dict(interval=1, years=30, filename='must_not_start', word='минута')]
    with pytest.raises(expected_type) as captured:
        if operation == 'reload':
            gathering.full_reload(source, 1, 30, 'final_previous', 'минута', root, dates, logging_context=context)
        else:
            gathering.data_update(specs, root, source, dates, logging_context=context)
    if failure == 'source':
        assert captured.value is primary and primary.args == ('A3_PRIVATE_SOURCE_CANARY',)
    assert len(calls) == (53 if operation == 'new' else 3) if failure == 'diagnostic' else len(calls) == 2
    assert writers == [] and gathering._A3_RANGE_STATE.get() is None
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest for path, digest in previous.items())
    assert target.exists() is (operation != 'cold')


def test_scaled_offline_profile_cannot_admit_large_data(gathering, monkeypatch):
    with gathering._a3_scope('job', 1) as state:
        assert state['scaled_offline'] is True
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_allocation_admit(129 * 1024**2)
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_charge('rows', 100001)
        assert state['job']['rows'] == 0


def test_unknown_quota_cannot_admit_large_operation_outside_offline_profile(gathering, monkeypatch):
    monkeypatch.delenv('MDS_OFFLINE_RUN_ID')
    calls = mock_queries(gathering, monkeypatch, [])
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: None)
    with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
        gathering._a3_history('SBER', 'Акции', '2023-03-02', '2024-03-02', 1)
    assert calls == []


def test_compatible_numeric_and_empty_update_dtype_participation(gathering, monkeypatch):
    first = candle()
    empty = first.iloc[:0].copy()
    empty['open'] = pd.Series([], dtype='float64')
    first['open'] = pd.Series([100], dtype='int64')
    with gathering._a3_scope('dataset'):
        frames = []
        gathering._a3_dataset_append(frames, first)
        gathering._a3_dataset_append(frames, pd.DataFrame())
        gathering._a3_dataset_append(frames, empty)
        result = gathering._a3_dataset_union(frames)
    assert result.open.tolist() == [100.0] and str(result.open.dtype) == 'float64'
    assert str(first.open.dtype) == 'int64' and str(empty.open.dtype) == 'float64'
    assert result.ticker.tolist() == ['SBER'] and result.index.tolist() == [0]


def test_caller_union_refuses_unsafe_numeric_before_writer(gathering, monkeypatch, writers):
    first, second = candle(), candle('2024-02-29 00:00:00')
    first['open'] = pd.Series([9007199254740993], dtype='int64')
    with gathering._a3_scope('dataset'):
        frames = []
        gathering._a3_dataset_append(frames, first)
        gathering._a3_dataset_append(frames, second)
        with pytest.raises(ValueError, match='A3_ISS_NUMERIC_ASSEMBLY_LOSS'):
            gathering._a3_dataset_union(frames)
    assert writers == [] and first.open.tolist() == [9007199254740993]


@pytest.mark.parametrize('page_size', [4096, 8192, 65536])
def test_native_api_page_units_and_private_usage_are_not_rss(gathering, monkeypatch, page_size):
    if gathering.sys.platform.startswith('linux'):
        facts, _, _ = linux_memory_fixture(gathering, monkeypatch)
        facts['page_size'] = page_size
        sample = gathering._a3_memory_snapshot()
        assert sample['page_size'] == page_size
        assert sample['total_phys'] == 68719476736  # kB conversion is independent of page size.
        assert sample['vm_size_bytes'] == 2147483648 and sample['private_bytes'] is None
        return
    actual = gathering._a3_memory_snapshot()
    assert actual is not None
    _, Memory, Performance, Process, Job, AppMemory = gathering._A3_MEMORY_API
    class Api:
        def GlobalMemoryStatusEx(self, pointer):
            pointer._obj.total_phys = 64*GiB
            pointer._obj.available_phys = 48*GiB
            pointer._obj.available_pagefile = 20*GiB
            pointer._obj.available_virtual = 24*GiB
            return True
        def K32GetPerformanceInfo(self, pointer, length):
            assert length > 0
            pointer._obj.commit_limit = 10000
            pointer._obj.commit_total = 1234
            pointer._obj.page_size = page_size
            return True
        def GetCurrentProcess(self):
            return -1
        def K32GetProcessMemoryInfo(self, handle, pointer, length):
            assert handle == -1 and length > 0
            pointer._obj.private_bytes = 11111
            pointer._obj.working_set = 22222
            return True
        def GetProcessInformation(self, handle, kind, pointer, length):
            assert handle == -1 and kind == 2 and length == 32
            pointer._obj.available_commit = 18*GiB
            return True
        def IsProcessInJob(self, handle, job, pointer):
            assert job is None
            pointer._obj.value = False
            return True
    monkeypatch.setattr(gathering, '_A3_MEMORY_API', (Api(), Memory, Performance, Process, Job, AppMemory))
    sample = gathering._a3_memory_snapshot()
    assert sample['commit_limit'] == 10000*page_size
    assert sample['commit_available'] == 8766*page_size
    assert sample['private_bytes'] == 11111 and sample['working_set'] == 22222
    assert sample['quota_available'] == 18*GiB and sample['quotas_verified'] is True


def test_source_protocol_aggregate_caps_and_eight_offline_cases(gathering, monkeypatch):
    assert gathering._A3_SOURCE_DIAGNOSTIC_LIMITS == dict(
        seconds=2700, gets=512, bytes=134217728, rows=250000,
        final_bytes=134217728)
    assert gathering._A3_SOURCE_DIAGNOSTIC_REFERENCE_LIMITS == dict(evidence_bytes=268435456)
    calls = mock_queries(gathering, monkeypatch, [pd.DataFrame()] * 128)
    with gathering._a3_source_diagnostic_scope() as state:
        for case in gathering._a3_source_diagnostic_plan():
            result = gathering._a3_history(case['ticker'], 'Акции', case['from_date'], case['till_date'], case['interval'])
            assert result.empty and result.columns.tolist() == []
        assert state['run']['jobs'] == 8 and state['run']['seconds'] == 2700
        gathering._a3_charge('gets', 512)
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_charge('gets', 1)
    assert len(calls) == 16  # 2*(2 annual daily +2 annual hourly +2 monthly +2 weekly)


def test_range_logging_correlation_counters_and_native_schema(gathering, monkeypatch, event_log):
    row = '100;101;102;99;202000;2000;2024-03-01 00:00:00;2024-03-01 23:59:59'
    state = wire(gathering, monkeypatch, [response(status=503), response([row]), response()])
    context = dict(event_log.context, page='CALLER_PAGE', dataset_id='fixture_dataset')
    gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 1, logging_context=context)
    events = event_log.events
    returned = next(item for item in events if item['event'] == 'a3_ranges_returned')
    assert returned['fields'] == {} and returned['page'] == 'CALLER_PAGE'
    assert returned['dataset_id'] == 'fixture_dataset'
    assert returned['outcome'] == 'CLIENT_PLAN_COMPLETE_SOURCE_UNVERIFIED'
    assert returned['counts']['rows_received'] == 1 and returned['counts']['pages'] == 2
    assert events.index(returned) > next(i for i, item in enumerate(events) if item['event'] == 'a2_query_returned')
    assert len(state.calls) == 3 and state.sleeps == [3, 3, 3]


def test_dense_monolithic_exceeds_180_but_bounded_plan_completes(gathering, monkeypatch, writers):
    groups = []
    for day in ['2024-02-16', '2024-02-23', '2024-03-01']:
        groups.append([f'100;101;102;99;202000;2000;{day} 00:{minute:02d}:00;{day} 00:{minute:02d}:59'
                       for minute in range(20)])
    original = wire(gathering, monkeypatch, [response([row]) for group in groups for row in group] + [response()])
    with pytest.raises(requests.Timeout, match='A2_ISS_DEADLINE_EXCEEDED'):
        gathering.moex_query('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(original.calls) == 60 and original.sleeps == [3]*59
    split = wire(gathering, monkeypatch, [page for group in reversed(groups)
                 for page in [*[response([row]) for row in group], response()]])
    result = gathering._a3_history('SBER', 'Акции', '2024-02-10', '2024-03-02', 1)
    assert len(split.calls) == 63 and split.sleeps == [3]*63 and split.elapsed == 189
    assert result.begin.tolist() == [f'{day} 00:{minute:02d}:00'
                                    for day in ['2024-02-16','2024-02-23','2024-03-01'] for minute in range(20)]
    assert result.open.tolist() == [100]*60 and result.columns.tolist() == COLUMNS
    assert result.index.tolist() == list(range(60)) and writers == []


@pytest.mark.parametrize('fault', ['deadline', 'capacity'])
def test_pending_native_primary_wins_over_aggregate_exhaustion(gathering, monkeypatch, event_log, fault):
    primary = requests.exceptions.ReadTimeout('PRIVATE_PRIMARY')
    state = wire(gathering, monkeypatch, [primary, response()])
    # A pre-existing retryable primary must not be replaced at next admission.
    original = gathering._a3_checkpoint
    def checkpoint(pending=None):
        if pending is not None:
            current = gathering._A3_RANGE_STATE.get()
            if fault == 'deadline':
                current['job']['seconds'] = 0
            else:
                # No retry GET fits, but the native timeout already exists.
                gathering._A3_RANGE_LIMITS['job']['gets'] = 1
        return original(pending)
    monkeypatch.setattr(gathering, '_A3_RANGE_LIMITS', copy.deepcopy(gathering._A3_RANGE_LIMITS))
    monkeypatch.setattr(gathering, '_a3_checkpoint', checkpoint)
    with pytest.raises(requests.exceptions.ReadTimeout) as captured:
        gathering._a3_history('SBER','Акции','2024-03-01','2024-03-02',1,logging_context=event_log.context)
    assert captured.value is primary and primary.args == ('PRIVATE_PRIMARY',)
    assert len(state.calls) == 1 and state.sleeps == ([] if fault == 'deadline' else [3])
    failed = [event for event in event_log.events if event['event'] == 'a2_query_failed']
    assert len(failed) == 1 and failed[0]['error']['code'] == 'A2_ISS_TIMEOUT'


def test_disposable_ten_thousand_row_benchmark(gathering, monkeypatch):
    dates = pd.date_range('2024-02-10', periods=10000, freq='min')
    source = pd.DataFrame(dict(open=100., close=101., high=102., low=99.,
        value=202000., volume=2000, begin=dates.strftime('%Y-%m-%d %H:%M:%S'),
        end=(dates+pd.Timedelta(seconds=59)).strftime('%Y-%m-%d %H:%M:%S'), ticker='SBER'))
    calls = mock_queries(gathering, monkeypatch, [source])
    started = time.perf_counter()
    result = gathering._a3_history('SBER','Акции','2024-02-10','2024-02-17',1)
    elapsed = time.perf_counter()-started
    assert len(calls) == 1 and len(result) == 10000 and elapsed < 60
    assert result.begin.iloc[0] == '2024-02-10 00:00:00' and result.begin.iloc[-1] == '2024-02-16 22:39:00'
    assert result.open.tolist() == [100.]*10000 and result.volume.tolist() == [2000]*10000
    pd.testing.assert_frame_equal(result,source)
    print('A3_DISPOSABLE_BENCHMARK=' + json.dumps(dict(rows=10000,seconds=elapsed,
        frame_bytes=int(result.memory_usage(index=True,deep=True).sum()), wire_bytes=0, source='MOCKED')))


def linux_memory_fixture(gathering, monkeypatch, version='v2'):
    # Literal kernel-file fixtures; no host /proc, cgroup mount or live data.
    facts = dict(namespace_inode=4026531835, namespace_link='cgroup:[4026531835]',
        page_size=4096, rlimit_as=(34359738368, 34359738368),
        rlimit_data=(17179869184, 17179869184))
    files = {
        '/proc/meminfo': ('MemTotal: 67108864 kB\nMemAvailable: 50331648 kB\n'
            'CommitLimit: 134217728 kB\nCommitted_AS: 67108864 kB\nSwapTotal: 16777216 kB\n'),
        '/proc/self/status': 'VmSize: 2097152 kB\nVmData: 1048576 kB\nVmRSS: 524288 kB\n',
        '/proc/sys/vm/overcommit_memory': '0\n',
        '/proc/cgroups': '#subsys_name hierarchy num_cgroups enabled\nmemory 0 3 1\n',
        '/proc/thread-self/cgroup': '0::/team/leaf\n',
        '/proc/self/mountinfo': '30 23 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n',
        '/sys/fs/cgroup/cgroup.controllers': 'cpu memory pids\n',
        '/sys/fs/cgroup/cgroup.subtree_control': 'memory\n',
        '/sys/fs/cgroup/team/cgroup.subtree_control': 'memory\n',
        '/sys/fs/cgroup/team/memory.max': '42949672960\n',
        '/sys/fs/cgroup/team/memory.current': '8589934592\n',
        '/sys/fs/cgroup/team/memory.swap.max': '17179869184\n',
        '/sys/fs/cgroup/team/memory.swap.current': '2147483648\n',
        '/sys/fs/cgroup/team/leaf/memory.max': '25769803776\n',
        '/sys/fs/cgroup/team/leaf/memory.current': '4294967296\n',
        '/sys/fs/cgroup/team/leaf/memory.swap.max': '4294967296\n',
        '/sys/fs/cgroup/team/leaf/memory.swap.current': '1073741824\n',
    }
    if version == 'v1':
        files['/proc/cgroups'] = 'memory 5 3 1\n'
        files['/proc/thread-self/cgroup'] = '5:memory:/team/leaf\n'
        files['/proc/self/mountinfo'] = '30 23 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n'
        for suffix, limit, used, memsw_limit, memsw_used in (
                ('team', '42949672960', '8589934592', '60129542144', '10737418240'),
                ('team/leaf', '25769803776', '4294967296', '30064771072', '5368709120')):
            path = '/sys/fs/cgroup/memory/' + suffix
            files[path+'/memory.limit_in_bytes'] = limit+'\n'
            files[path+'/memory.usage_in_bytes'] = used+'\n'
            files[path+'/memory.memsw.limit_in_bytes'] = memsw_limit+'\n'
            files[path+'/memory.memsw.usage_in_bytes'] = memsw_used+'\n'
    calls = []
    def read(path):
        calls.append(path)
        if path not in files:
            raise FileNotFoundError('SYNTHETIC_KERNEL_FILE_MISSING')
        return files[path]
    monkeypatch.setattr(gathering, '_a3_linux_read', read, raising=False)
    monkeypatch.setattr(gathering, '_a3_linux_runtime', lambda: dict(facts), raising=False)
    return facts, files, calls


@pytest.mark.parametrize('version', ['v1', 'v2'])
def test_review_linux_admission_success_and_literal_units(gathering, monkeypatch, version):
    facts, files, calls = linux_memory_fixture(gathering, monkeypatch, version)
    platform = SimpleNamespace(**vars(gathering.sys))
    platform.platform = 'linux'
    monkeypatch.setattr(gathering, 'sys', platform)
    sample = gathering._a3_memory_snapshot()
    assert sample['platform'] == 'linux' and sample['quotas_verified'] is True
    assert sample['total_phys'] == 68719476736 and sample['available_phys'] == 51539607552
    assert sample['commit_limit'] == 137438953472 and sample['commit_available'] == 68719476736
    assert sample['vm_size_bytes'] == 2147483648 and sample['vm_data_bytes'] == 1073741824
    assert sample['working_set'] == 536870912 and sample['private_bytes'] is None
    assert sample['process_available'] == 16106127360 and sample['quota_available'] == 16106127360
    assert sample['cgroup_version'] == version and sample['page_size'] == 4096
    assert '/proc/self/mountinfo' in calls and calls.count('/proc/thread-self/cgroup') == 2
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    assert gathering._a3_memory_admit(GiB) is sample
    with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
        gathering._a3_memory_admit(8*GiB)


@pytest.mark.parametrize('failure', ['namespace', 'partial-mount', 'missing-parent', 'missing-swap',
    'invalid-units', 'invalid-number', 'invalid-page', 'invalid-rlimit', 'unknown-controller', 'race', 'stale'])
def test_review_linux_unknown_invalid_stale_has_no_get_or_writer(gathering, monkeypatch, writers, failure):
    facts, files, _ = linux_memory_fixture(gathering, monkeypatch)
    if failure == 'namespace':
        facts['namespace_inode'] = 4026533000
        facts['namespace_link'] = 'cgroup:[4026533000]'
    elif failure == 'partial-mount':
        files['/proc/self/mountinfo'] = '30 23 0:28 /hidden /sys/fs/cgroup rw - cgroup2 cgroup rw\n'
    elif failure == 'missing-parent':
        del files['/sys/fs/cgroup/team/memory.max']
    elif failure == 'missing-swap':
        del files['/sys/fs/cgroup/team/memory.swap.max']
    elif failure == 'invalid-units':
        files['/proc/meminfo'] = files['/proc/meminfo'].replace('MemTotal: 67108864 kB', 'MemTotal: 67108864 MB')
    elif failure == 'invalid-number':
        files['/sys/fs/cgroup/team/memory.current'] = '-1\n'
    elif failure == 'invalid-page':
        facts['page_size'] = 0
    elif failure == 'invalid-rlimit':
        facts['rlimit_as'] = (34359738368, 1024)
    elif failure == 'unknown-controller':
        files['/proc/cgroups'] = 'memory 7 3 1\n'
    elif failure == 'race':
        original = gathering._a3_linux_read
        seen = []
        def read(path):
            if path == '/proc/thread-self/cgroup':
                seen.append(path)
                if len(seen) == 2:
                    return '0::/moved\n'
            return original(path)
        monkeypatch.setattr(gathering, '_a3_linux_read', read)
    else:
        clock = iter([100., 102., 102., 102., 102.])
        monkeypatch.setattr(gathering, 'time', SimpleNamespace(perf_counter=lambda: next(clock, 102.)))
    sample = gathering._a3_linux_memory_snapshot()
    if failure != 'stale':
        assert sample is None
    else:
        assert sample['sampled_at'] == 100.
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    # Remove only the source's test-profile observation; native guard stays active.
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert calls == [] and writers == []


@pytest.mark.parametrize('version', ['v1', 'v2'])
def test_review_linux_exhausted_ancestor_refuses_before_get(gathering, monkeypatch, writers, version):
    _, files, _ = linux_memory_fixture(gathering, monkeypatch, version)
    if version == 'v2':
        files['/sys/fs/cgroup/team/memory.max'] = '8589934592\n'
    else:
        files['/sys/fs/cgroup/memory/team/memory.limit_in_bytes'] = '8589934592\n'
    sample = gathering._a3_linux_memory_snapshot()
    assert sample['quota_available'] == 0 and sample['quotas_verified'] is True
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert calls == [] and writers == []


@pytest.mark.parametrize('case', ['unlimited-v1', 'unlimited-v2', 'disabled', 'subtree-disabled'])
def test_review_linux_known_unlimited_and_disabled_controller(gathering, monkeypatch, case):
    facts, files, _ = linux_memory_fixture(gathering, monkeypatch, 'v1' if case == 'unlimited-v1' else 'v2')
    facts['rlimit_as'] = facts['rlimit_data'] = (-1, -1)
    if case == 'disabled':
        files['/proc/cgroups'] = 'memory 0 1 0\n'
        files['/sys/fs/cgroup/cgroup.controllers'] = 'cpu pids\n'
    elif case.startswith('unlimited'):
        for name in list(files):
            if name.endswith(('/memory.max', '/memory.swap.max')):
                files[name] = 'max\n'
            elif name.endswith(('/memory.limit_in_bytes', '/memory.memsw.limit_in_bytes')):
                files[name] = '9223372036854771712\n'
    if case in ('disabled', 'subtree-disabled'):
        files['/sys/fs/cgroup/cgroup.subtree_control'] = ''
        files['/sys/fs/cgroup/team/cgroup.subtree_control'] = ''
    sample = gathering._a3_linux_memory_snapshot()
    assert sample['quotas_verified'] is True
    assert sample['cgroup_version'] == ('disabled' if case == 'disabled' else 'v1' if case == 'unlimited-v1' else 'v2')
    assert sample['process_available'] == sample['quota_available'] == 68719476736


@pytest.mark.parametrize('identity', ['absent', 'fake-module', 'unfinished-bootstrap', 'finished', 'foreign-root', 'uninstalled'])
def test_review_environment_alone_never_selects_scaled_profile(gathering, monkeypatch, writers, identity):
    import types
    real = gathering.sys.modules['mds_offline_guard']
    context = real._CONTEXT
    # Restore the observation before pytest/guard report hooks run.
    with monkeypatch.context() as observation:
        observation.setenv('MDS_OFFLINE_ROOT', str(Path(gathering.__file__).parent))
        observation.setenv('MDS_OFFLINE_SOURCE_MODE', context.source_mode)
        observation.setenv('MDS_OFFLINE_RUN_ID', context.run_id)
        if identity == 'absent':
            observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        elif identity == 'fake-module':
            fake = types.ModuleType('mds_offline_guard')
            fake.__file__, fake._INSTALLED, fake._CONTEXT = real.__file__, True, copy.copy(context)
            observation.setitem(gathering.sys.modules, 'mds_offline_guard', fake)
        else:
            altered = copy.copy(context)
            observation.setattr(real, '_CONTEXT', altered)
            if identity == 'unfinished-bootstrap':
                altered.started = False
            elif identity == 'finished':
                altered.finished = True
            elif identity == 'foreign-root':
                altered.root = altered.root / 'unrelated'
            else:
                observation.setattr(real, '_INSTALLED', False)
                altered.native_counts = None
        primary = MemoryError('NATIVE_ADMISSION_PRIMARY')
        def refuse(growth):
            raise primary
        observation.setattr(gathering, '_a3_memory_admit', refuse)
        with gathering._a3_scope('job', 1) as state:
            assert state['scaled_offline'] is False
            with pytest.raises(MemoryError) as captured:
                gathering._a3_allocation_admit(1024)
            assert captured.value is primary and primary.args == ('NATIVE_ADMISSION_PRIMARY',)
    assert writers == []


def test_review_evidence_runtime_and_reference_budgets_are_separate(gathering):
    assert all('evidence_bytes' not in caps for caps in gathering._A3_RANGE_LIMITS.values())
    assert gathering._A3_SOURCE_DIAGNOSTIC_LIMITS == dict(
        seconds=2700, gets=512, bytes=134217728, rows=250000, final_bytes=134217728)
    assert gathering._A3_SOURCE_DIAGNOSTIC_REFERENCE_LIMITS == dict(evidence_bytes=268435456)
    with gathering._a3_source_diagnostic_scope() as state:
        assert 'evidence_bytes' not in state['limits']['run']


@pytest.mark.parametrize('cross_annual', [False, True])
def test_review_conflict_evidence_scope_preserves_first_and_late(gathering, monkeypatch, writers, cross_annual):
    first, repeat, late = candle(), candle(), candle(number=100.5)
    calls = mock_queries(gathering, monkeypatch, [first, repeat, late])
    low = '2022-03-02' if cross_annual else '2024-02-10'
    interval = 24 if cross_annual else 1
    code = 'A3_ISS_ANNUAL_KEY_CONFLICT' if cross_annual else 'A3_ISS_RANGE_KEY_CONFLICT'
    # Cross-annual needs three logical windows to exercise an early exact repeat.
    if cross_annual:
        low = '2021-03-02'
    with pytest.raises(ValueError, match=code) as captured:
        gathering._a3_history('SBER', 'Акции', low, '2024-03-02', interval)
    primary = captured.value
    evidence = primary.a3_range_evidence
    assert primary.args == (code,) and len(calls) == 3 and writers == []
    assert hasattr(primary, 'a3_annual_evidence') is cross_annual
    assert evidence['metadata']['conflict_scope'] == ('CROSS_ANNUAL' if cross_annual else 'INTERNAL_PHYSICAL')
    assert [row['physical_ordinal'] for row in evidence['metadata']['rows']] == [1, 3]
    assert [row['logical_ordinal'] for row in evidence['metadata']['rows']] == ([1, 3] if cross_annual else [1, 1])
    assert evidence['typed_rows'][0][0]['hex'] == '0x1.9000000000000p+6'
    assert evidence['typed_rows'][1][0]['hex'] == '0x1.9200000000000p+6'
    assert evidence['metadata']['cell_bytes'] <= 65536
    assert len(json.dumps(evidence['metadata'],ensure_ascii=True,sort_keys=True,separators=(',',':')).encode()) <= 16384


def test_review_internal_evidence_failure_keeps_native_primary(gathering, monkeypatch, writers):
    primary = ValueError('A3_ISS_RANGE_KEY_CONFLICT')
    row = gathering._a3_annual_row(candle(), 0)
    leaf = dict(logical_ordinal=1, physical_ordinal=1, from_date='2024-02-23', till_date='2024-03-02',
        logical_lower='2024-02-10', logical_upper='2024-03-02')
    monkeypatch.setattr(gathering, '_a3_numeric_identity', lambda value: (_ for _ in ()).throw(MemoryError('SECONDARY_TYPED_CAPTURE')))
    gathering._a3_range_evidence(primary, dict(market='shares', security='SBER', interval='1'),
        ((row, leaf), (row, dict(leaf, physical_ordinal=3))), None)
    assert type(primary) is ValueError and primary.args == ('A3_ISS_RANGE_KEY_CONFLICT',)
    assert not hasattr(primary, 'a3_annual_evidence') and not hasattr(primary, 'a3_range_evidence')
    assert writers == []


def test_review_linux_native_memory_error_keeps_object_and_stops(gathering, monkeypatch, writers):
    linux_memory_fixture(gathering, monkeypatch)
    primary = MemoryError('NATIVE_KERNEL_CAPTURE_PRIMARY')
    def read(path):
        raise primary
    monkeypatch.setattr(gathering, '_a3_linux_read', read)
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', gathering._a3_linux_memory_snapshot)
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(MemoryError) as captured:
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
        assert captured.value is primary and primary.args == ('NATIVE_KERNEL_CAPTURE_PRIMARY',)
    assert calls == [] and writers == []


def test_review_linux_metadata_reader_has_finite_bound(gathering, tmp_path):
    path = tmp_path/'synthetic_kernel.txt'
    path.write_text('0'*65537, encoding='ascii')
    with pytest.raises(ValueError, match='A3_MEMORY_INPUT_LIMIT'):
        gathering._a3_linux_read(path)


def test_review_linux_uses_calling_thread_not_leader_quota(gathering, monkeypatch):
    _, files, calls = linux_memory_fixture(gathering, monkeypatch)
    files['/proc/self/cgroup'] = '0::/leader-with-different-quota\n'
    files['/sys/fs/cgroup/team/memory.max'] = '8589934592\n'
    sample = gathering._a3_linux_memory_snapshot()
    assert sample['quotas_verified'] is True and sample['quota_available'] == 0
    assert calls.count('/proc/thread-self/cgroup') == 2 and '/proc/self/cgroup' not in calls


def checkpoint_linux_profile(gathering, monkeypatch, profile):
    version = 'v1' if profile.endswith('v1') else 'v2'
    facts, files, calls = linux_memory_fixture(gathering, monkeypatch, version)
    if profile.startswith('nested'):
        facts.update(namespace_inode=4026534001, namespace_link='cgroup:[4026534001]')
    if 'root-only' in profile:
        files['/proc/cgroups'] = 'memory 5 1 1\n' if version == 'v1' else 'memory 0 1 1\n'
        files['/proc/thread-self/cgroup'] = '5:memory:/\n' if version == 'v1' else '0::/\n'
    if profile == 'nested-disabled':
        files['/proc/cgroups'] = 'memory 0 1 0\n'
        files['/proc/thread-self/cgroup'] = '0::/\n'
    return facts, files, calls


@pytest.mark.parametrize('profile,basis', [
    ('nested-disabled', 'GLOBAL_MEMORY_CONTROLLER_DISABLED'),
    ('nested-root-only-v1', 'GLOBAL_SINGLE_ROOT_MEMORY_HIERARCHY'),
    ('nested-root-only-v2', 'GLOBAL_SINGLE_ROOT_MEMORY_HIERARCHY')])
def test_linux_checkpoint_proven_nested_profiles_keep_real_admission(gathering, monkeypatch, writers, profile, basis):
    _, _, reads = checkpoint_linux_profile(gathering, monkeypatch, profile)
    sample = gathering._a3_linux_memory_snapshot()
    assert sample is not None and sample['quotas_verified'] is True
    assert sample['quota_basis'] == basis and sample['namespace_initial'] is False
    assert sample['quota_available'] == 16106127360
    assert reads.count('/proc/cgroups') == 2
    assert not any(path.endswith(('/memory.max', '/memory.limit_in_bytes')) for path in reads)
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', gathering._a3_linux_memory_snapshot)
    source = candle()
    calls = mock_queries(gathering, monkeypatch, [source])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        result = gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert len(calls) == 1 and writers == []
    pd.testing.assert_frame_equal(result, source)


@pytest.mark.parametrize('profile,fault', [
    ('initial-v1', 'partial-mount'), ('initial-v2', 'partial-mount'),
    ('nested-private-v1', 'visible-leaf'), ('nested-private-v2', 'visible-leaf'),
    ('nested-private-v2', 'unlimited-leaf'), ('nested-root-only-v2', 'partial-mount'),
    ('nested-root-only-v2', 'missing-proc'), ('nested-root-only-v2', 'missing-link'),
    ('nested-root-only-v2', 'link-mismatch'), ('nested-root-only-v1', 'nonroot-membership'),
    ('initial-v2', 'zero-groups'), ('initial-v2', 'invalid-groups'),
    ('initial-v2', 'duplicate-controller'), ('nested-root-only-v2', 'counter-race')])
def test_linux_checkpoint_unproven_profiles_stop_before_get(gathering, monkeypatch, writers, profile, fault):
    facts, files, _ = checkpoint_linux_profile(gathering, monkeypatch, profile)
    if fault == 'partial-mount':
        files['/proc/self/mountinfo'] = files['/proc/self/mountinfo'].replace(' / /sys/', ' /delegated /sys/')
    elif fault == 'unlimited-leaf':
        for key in files:
            if key.endswith(('/memory.max', '/memory.swap.max')):
                files[key] = 'max\n'
    elif fault == 'missing-proc':
        del files['/proc/cgroups']
    elif fault == 'missing-link':
        facts['namespace_link'] = None
    elif fault == 'link-mismatch':
        facts['namespace_link'] = 'cgroup:[4026534999]'
    elif fault == 'nonroot-membership':
        files['/proc/thread-self/cgroup'] = '5:memory:/leaf\n'
    elif fault == 'zero-groups':
        files['/proc/cgroups'] = 'memory 0 0 1\n'
    elif fault == 'invalid-groups':
        files['/proc/cgroups'] = 'memory 0 unknown 1\n'
    elif fault == 'duplicate-controller':
        files['/proc/cgroups'] += 'memory 0 1 1\n'
    elif fault == 'counter-race':
        original = gathering._a3_linux_read
        seen = []
        def read(path):
            if path == '/proc/cgroups':
                seen.append(path)
                if len(seen) % 2 == 0:
                    return 'memory 0 2 1\n'
            return original(path)
        monkeypatch.setattr(gathering, '_a3_linux_read', read)
    assert gathering._a3_linux_memory_snapshot() is None
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', gathering._a3_linux_memory_snapshot)
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert calls == [] and writers == []


@pytest.mark.parametrize('total', [1, 2, 4, 8])
@pytest.mark.parametrize('profile', ['initial-v2', 'nested-root-only-v2'])
def test_linux_checkpoint_small_vps_is_not_admitted(gathering, monkeypatch, writers, total, profile):
    _, files, _ = checkpoint_linux_profile(gathering, monkeypatch, profile)
    # Even the ideal all-RAM-available upper bound cannot pay reserve8GiB+1GiB.
    kib = str(total * 1048576)
    files['/proc/meminfo'] = files['/proc/meminfo'].replace('67108864 kB', kib+' kB', 1).replace('50331648 kB', kib+' kB')
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', gathering._a3_linux_memory_snapshot)
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert calls == [] and writers == []


@pytest.mark.parametrize('total,available,growth,accepted', [
    (8,8,0,False), (9,9,0,True), (10,10,1,False), (11,11,1,True),
    (12,10,1,False), (12,11,1,True), (16,15,3,True), (16,14,3,False),
    (32,31,11,True), (32,30,11,False), (64,63,23,True), (64,62,23,False)])
def test_linux_checkpoint_literal_ram_reserve_boundary(gathering, monkeypatch, total, available, growth, accepted):
    facts, files, _ = checkpoint_linux_profile(gathering, monkeypatch, 'nested-root-only-v2')
    facts['rlimit_as'] = facts['rlimit_data'] = (-1,-1)
    files['/proc/meminfo'] = ('MemTotal: '+str(total*1048576)+' kB\nMemAvailable: '+str(available*1048576)+
        ' kB\nCommitLimit: 134217728 kB\nCommitted_AS: 67108864 kB\nSwapTotal: 16777216 kB\n')
    sample = gathering._a3_linux_memory_snapshot()
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    if accepted:
        assert gathering._a3_memory_admit(growth*GiB) is sample
    else:
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_memory_admit(growth*GiB)


@pytest.mark.parametrize('magic', [0x65735546, 0x794c7630, 0])
def test_linux_checkpoint_reader_rejects_proxy_metadata(gathering, monkeypatch, tmp_path, magic):
    path = tmp_path/'proxy_metadata.txt'
    path.write_text('memory 0 1 0\n', encoding='ascii')
    monkeypatch.setattr(gathering, '_a3_linux_fs_type', lambda descriptor: magic, raising=False)
    with pytest.raises(ValueError, match='A3_MEMORY_FILESYSTEM_UNKNOWN'):
        gathering._a3_linux_read(path)


@pytest.mark.parametrize('magic', [0x27e0eb, 0x63677270])
def test_linux_checkpoint_reader_checks_the_open_descriptor(gathering, monkeypatch, tmp_path, magic):
    path = tmp_path/'kernel_metadata.txt'
    path.write_bytes(b'literal\n')
    seen = []
    def file_system(descriptor):
        seen.append(gathering.os.fstat(descriptor).st_size)
        return magic
    monkeypatch.setattr(gathering, '_a3_linux_fs_type', file_system)
    assert gathering._a3_linux_read(path) == 'literal\n' and seen == [8]


@pytest.mark.parametrize('outcome', ['success', 'unavailable', 'memory-error'])
def test_linux_checkpoint_statfs_has_literal_abi_and_failure_precedence(gathering, monkeypatch, outcome):
    import ctypes
    import errno
    facts = SimpleNamespace(**vars(gathering.os))
    facts.uname = lambda: SimpleNamespace(machine='x86_64')
    monkeypatch.setattr(gathering, 'os', facts)
    primary = MemoryError('NATIVE_FSTATFS_PRIMARY')
    seen = []
    def fstatfs(descriptor, pointer):
        seen.append((descriptor, ctypes.sizeof(pointer._obj)))
        if outcome == 'memory-error':
            raise primary
        if outcome == 'unavailable':
            ctypes.set_errno(errno.EACCES)
            return -1
        pointer._obj.f_type = 0x9fa0
        return 0
    def library(name, use_errno):
        assert name is None and use_errno is True
        return SimpleNamespace(fstatfs=fstatfs)
    monkeypatch.setattr(ctypes, 'CDLL', library)
    if outcome == 'success':
        assert gathering._a3_linux_fs_type(17) == 0x9fa0
    elif outcome == 'unavailable':
        with pytest.raises(OSError) as captured:
            gathering._a3_linux_fs_type(17)
        assert captured.value.errno == errno.EACCES
    else:
        with pytest.raises(MemoryError) as captured:
            gathering._a3_linux_fs_type(17)
        assert captured.value is primary and primary.args == ('NATIVE_FSTATFS_PRIMARY',)
    assert seen == [(17,120)]


def test_linux_checkpoint_proven_namespace_stale_sample_has_no_get(gathering, monkeypatch, writers):
    checkpoint_linux_profile(gathering, monkeypatch, 'nested-root-only-v2')
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(perf_counter=lambda: 100.))
    sample = gathering._a3_linux_memory_snapshot()
    assert sample['sampled_at'] == 100. and sample['quotas_verified'] is True
    monkeypatch.setattr(gathering, 'time', SimpleNamespace(perf_counter=lambda: 102.))
    monkeypatch.setattr(gathering, '_a3_memory_snapshot', lambda: sample)
    calls = mock_queries(gathering, monkeypatch, [])
    with monkeypatch.context() as observation:
        observation.delitem(gathering.sys.modules, 'mds_offline_guard')
        with pytest.raises(ValueError, match='A3_ISS_RANGE_CAPACITY_EXCEEDED'):
            gathering._a3_history('SBER', 'Акции', '2024-03-01', '2024-03-02', 24)
    assert calls == [] and writers == []


# F07 committed diagnostic helpers: explicit offline transport only.
# Origin SHA256: 052f3e6b51f1f5f19c749cf86dee16ec9ba331463d524812dd4323943e56f89a.
# Plain helpers are prefixed here to preserve the original historical tests;
# the actual product is loaded in full from its canonical admitted source file.
import base64 as _f07_base64
from contextlib import contextmanager as _f07_contextmanager
from datetime import date as _f07_date
import importlib.util as _f07_importlib
import csv as _f07_csv
import numbers as _f07_numbers
import sys as _f07_sys
import uuid as _f07_uuid

_F07_LIMITS = dict(seconds=2700, gets=512, bytes=134217728, rows=250000,
              frame_bytes=134217728, evidence_bytes=268435456,
              body_bytes=8388608)
_F07_ROLES = ("REFERENCE_BEFORE", "CANDIDATE", "REFERENCE_AFTER")
_F07_CASES = tuple(
    dict(ticker=ticker, interval=interval, from_date=bounds[0], till_date=bounds[1])
    for ticker in ("SBER", "MOEX")
    for interval, bounds in (
        (24, ("2023-02-27", "2024-03-02")),
        (60, ("2023-02-27", "2024-03-02")),
        (10, ("2024-01-30", "2024-03-02")),
        (1, ("2024-02-23", "2024-03-02")),
    )
)


def _f07_encoded(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _f07_digest(value):
    return hashlib.sha256(value).hexdigest()


def _f07_annotate(primary, label):
    try:
        primary.add_note(label)
    except BaseException:
        pass


class _F07Refusal(ValueError):
    pass


class _F07Sink:
    """One new private root; every output byte reserves the same finite quota.

    Reservation is monotone even on partial writes. Complete sealing requires
    actual bytes == reserved bytes, a known file inventory and matching hashes.
    Source code, old evidence and arbitrary external files are never imported.
    """
    def __init__(self, root, quota, writer=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.quota = quota
        self.reserved = 0
        self.files = {}
        self.failed = False
        self.writer = writer or (lambda stream, data: stream.write(data))

    def reserve(self, size):
        if self.failed or size < 0 or self.reserved + size > self.quota:
            self.failed = True
            raise _F07Refusal("F07_TOTAL_EVIDENCE_CAPACITY")
        self.reserved += size

    def write(self, name, data, *, reserved=False):
        if self.failed or Path(name).name != name or name in self.files:
            self.failed = True
            raise _F07Refusal("F07_EVIDENCE_STATE_OR_PATH")
        if not reserved:
            self.reserve(len(data))
        path = self.root / name
        try:
            with path.open("xb") as stream:
                amount = self.writer(stream, data)
                if amount != len(data):
                    raise OSError("F07_SHORT_EVIDENCE_WRITE")
                stream.flush()
            if path.is_symlink() or path.stat().st_size != len(data):
                raise OSError("F07_EVIDENCE_SIZE_MISMATCH")
            if _f07_digest(path.read_bytes()) != _f07_digest(data):
                raise OSError("F07_EVIDENCE_HASH_MISMATCH")
            self.files[name] = dict(bytes=len(data), sha256=_f07_digest(data))
        except BaseException:
            self.failed = True
            raise

    def audit(self):
        if self.failed:
            raise _F07Refusal("F07_EVIDENCE_PREVIOUS_FAILURE")
        actual = list(self.root.iterdir())
        if {path.name for path in actual} != set(self.files):
            self.failed = True
            raise _F07Refusal("F07_EVIDENCE_INVENTORY_MISMATCH")
        size = 0
        for path in actual:
            fact = self.files[path.name]
            if (path.is_symlink() or not path.is_file()
                    or path.stat().st_size != fact["bytes"]
                    or _f07_digest(path.read_bytes()) != fact["sha256"]):
                self.failed = True
                raise _F07Refusal("F07_EVIDENCE_CONTENT_MISMATCH")
            size += fact["bytes"]
        if size != self.reserved or size > self.quota:
            self.failed = True
            raise _F07Refusal("F07_EVIDENCE_ACCOUNTING_MISMATCH")
        return size


class _F07Harness:
    def __init__(self, root, *, limits=None, clock=time.monotonic, writer=None):
        self.limits = dict(_F07_LIMITS if limits is None else limits)
        if set(self.limits) != set(_F07_LIMITS) or any(
                (not isinstance(value, (int, float)) if name == "seconds" else type(value) is not int)
                or isinstance(value, bool) or not math.isfinite(value)
                or value <= 0 or value > _F07_LIMITS[name]
                for name, value in self.limits.items()):
            raise _F07Refusal("F07_LIMITS_MUST_NOT_EXCEED_APPROVED_PROTOCOL")
        self.clock = clock
        self.started = clock()
        self.sink = _F07Sink(root, self.limits["evidence_bytes"], writer)
        self.used = dict(gets=0, bytes=0, rows=0)
        self.attempts = {role: 0 for role in _F07_ROLES}
        self.records = []
        self.active = None
        self.failed = False
        self.sealed = False

    def check(self):
        if self.failed or self.sealed or self.sink.failed:
            raise _F07Refusal("F07_CAPTURE_NOT_ACTIVE")
        if self.clock() - self.started >= self.limits["seconds"]:
            self.failed = True
            raise TimeoutError("F07_DEADLINE")

    def admit_rows(self, amount, *, frame_bytes=0):
        self.check()
        if (not isinstance(amount, int) or amount < 0
                or self.used["rows"] + amount > self.limits["rows"]
                or frame_bytes < 0 or frame_bytes > self.limits["frame_bytes"]):
            self.failed = True
            raise _F07Refusal("F07_ROWS_OR_FRAME_CAPACITY")
        self.used["rows"] += amount

    def get(self, role, request, case, start, *, attempt=1, request_bounds=None, **kwargs):
        """Call injected request(case,start,**kwargs), never requests.get itself.

        All retry attempts and terminal requests use this method. Policy (retry,
        HTTPS, endpoints, 3s success sleep, native exception) stays with the caller.
        A response body is accessible only through bounded .raw.read().
        """
        self.check()
        if (role not in _F07_ROLES or case not in _F07_CASES or type(start) is not int
                or start < 0 or type(attempt) is not int or attempt < 1
                or self.active is not None):
            self.failed = True
            raise _F07Refusal("F07_ATTEMPT_SCOPE_OR_UNCLOSED_RESPONSE")
        bounds = dict(from_date=case['from_date'], till_date=case['till_date']) if request_bounds is None else request_bounds
        if (type(bounds) is not dict or set(bounds) != {'from_date', 'till_date'}
                or any(type(value) is not str for value in bounds.values())
                or not case['from_date'] <= bounds['from_date'] <= bounds['till_date'] <= case['till_date']):
            self.failed = True
            raise _F07Refusal('F07_ATTEMPT_REQUEST_BOUNDS')
        if self.used["gets"] >= self.limits["gets"]:
            self.failed = True
            raise _F07Refusal("F07_GET_CAPACITY")
        # Physical attempts are charged before invoking even a failing transport.
        self.used["gets"] += 1
        self.attempts[role] += 1
        record = dict(role=role, case=dict(case), start=start, attempt=attempt,
                      ordinal=self.used["gets"], status=None, codec=None,
                      request_bounds=dict(bounds),
                      captured_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      terminal=False, body_complete=False, body_sha256=None, body_bytes=0,
                      source_error_type=None)
        try:
            response = request(case, start, **kwargs)
        except BaseException as primary:
            record["source_error_type"] = type(primary).__name__
            try:
                self._retain(record, b"")
            except BaseException:
                _f07_annotate(primary, "F07_SECONDARY_EVIDENCE_FAILED")
            raise
        try:
            wrapped = _F07Response(self, response, record)
        except BaseException as primary:
            record["source_error_type"] = type(primary).__name__
            try:
                response.close()
            except BaseException:
                _f07_annotate(primary, "F07_SECONDARY_NATIVE_CLOSE_FAILED")
            try:
                self._retain(record, b"")
            except BaseException:
                _f07_annotate(primary, "F07_SECONDARY_EVIDENCE_FAILED")
            raise
        self.active = wrapped
        return wrapped

    def _retain(self, record, body):
        record = dict(record, body_bytes=len(body), body_sha256=_f07_digest(body))
        # Both raw and its encoded duplicate are retained and charged. No
        # convenient raw-body-only approximation may substitute for byte totals.
        envelope = _f07_encoded(dict(record, body_base64=_f07_base64.b64encode(body).decode("ascii")))
        prefix = f"attempt-{record['ordinal']:04d}"
        self.sink.reserve(len(body) + len(envelope))
        self.sink.write(prefix + ".body", body, reserved=True)
        self.sink.write(prefix + ".json", envelope, reserved=True)
        self.records.append(record)

    def seal(self):
        self.check()
        if self.active is not None or len(self.records) != self.used["gets"]:
            self.failed = True
            raise _F07Refusal("F07_INCOMPLETE_ATTEMPT_INVENTORY")
        self.sink.audit()
        manifest = _f07_encoded(dict(payload_files=self.sink.files, used=self.used,
                                attempts=self.attempts, records=self.records))
        receipt = dict(status="CAPTURE_RECORDED_NOT_ACCEPTANCE",
                       source_acceptance="NOT_VERIFIED", live_authority="NOT_GRANTED_BY_ARTIFACT",
                       manifest_sha256=_f07_digest(manifest), used=self.used,
                       attempts=self.attempts, files=len(self.sink.files) + 2,
                       evidence_bytes=0)
        while True:
            data = _f07_encoded(receipt)
            final_size = self.sink.reserved + len(manifest) + len(data)
            if receipt["evidence_bytes"] == final_size:
                break
            receipt["evidence_bytes"] = final_size
        # Reserve BOTH final outputs before either write. The receipt has no
        # COMPLETE label; only the returned result follows the final audit.
        self.sink.reserve(len(manifest) + len(data))
        self.sink.write("manifest.json", manifest, reserved=True)
        self.sink.write("receipt.json", data, reserved=True)
        actual = self.sink.audit()
        self.check()
        self.sealed = True
        return dict(status="OFFLINE_ACCOUNTED_CAPTURE_COMPLETE_SOURCE_UNVERIFIED",
                    evidence_bytes=actual, files=len(self.sink.files),
                    manifest_sha256=_f07_digest(manifest), receipt_sha256=_f07_digest(data),
                    used=dict(self.used), attempts=dict(self.attempts))

    def run(self, work):
        """No partial result. Native/source primary wins over evidence cleanup.

        work(harness) is trusted diagnostic orchestration using injected sessions.
        Successful returned work alone is insufficient: sealing must also pass.
        """
        try:
            result = work(self)
            seal = self.seal()
            return result, seal
        except BaseException as primary:
            self.failed = True
            if self.active is not None:
                try:
                    self.active.close(primary=primary)
                except BaseException:
                    _f07_annotate(primary, "F07_SECONDARY_CLOSE_OR_EVIDENCE_FAILED")
            # No success receipt is returned or written. Partial files remain in
            # this new root; the native caller retains its failure externally.
            raise


class _F07Response:
    def __init__(self, harness, response, record):
        self.harness, self.response, self.record = harness, response, record
        self.status_code = response.status_code
        self.headers = response.headers
        self.encoding = response.encoding
        self.record.update(status=self.status_code, codec=self.encoding)
        self.raw = _F07RawBody(self)
        self.body = bytearray()
        self.closed = False
        self.eof = False
        self.read_primary = None

    def read(self, amount, *, decode_content=False):
        self.harness.check()
        if self.closed or type(amount) is not int or amount <= 0 or decode_content:
            self.harness.failed = True
            raise _F07Refusal("F07_BOUNDED_IDENTITY_READ_REQUIRED")
        available = self.harness.limits["bytes"] - self.harness.used["bytes"]
        if available <= 0:
            self.harness.failed = True
            raise _F07Refusal("F07_RETURNED_BYTES_CAPACITY")
        take = min(amount, 65536, available,
                   self.harness.limits["body_bytes"] - len(self.body) + 1)
        if take <= 0:
            self.harness.failed = True
            raise _F07Refusal("F07_BODY_CAPACITY")
        try:
            part = self.response.raw.read(take, decode_content=False)
        except BaseException as primary:
            # A2 may keep this error in a local and close outside its except
            # block. sys.exception() then cannot identify the pending source
            # fault; retain its exact identity without declaring capture failed.
            self.read_primary = primary
            self.record['source_error_type'] = type(primary).__name__
            self.record['read_failed'] = True
            raise
        if not isinstance(part, bytes) or len(part) > take:
            self.harness.failed = True
            raise _F07Refusal("F07_TRANSPORT_READ_CONTRACT")
        self.harness.used["bytes"] += len(part)
        self.body.extend(part)
        if len(self.body) > self.harness.limits["body_bytes"]:
            self.harness.failed = True
            raise _F07Refusal("F07_BODY_CAPACITY")
        self.eof = not part
        return part

    def mark_terminal(self):
        # Parser, not empty transport body, decides that candles has zero rows.
        if not self.eof:
            self.harness.failed = True
            raise _F07Refusal("F07_TERMINAL_BEFORE_BODY_COMPLETE")
        self.record["terminal"] = True

    def raise_for_status(self):
        return self.response.raise_for_status()

    def close(self, *, primary=None):
        if self.closed:
            return
        # Native callers close in finally: preserve the exception currently
        # propagating even when the caller did not pass it explicitly.
        if primary is None:
            primary = _f07_sys.exception()
        self.closed = True
        observed_read_only = primary is None and self.read_primary is not None
        pending = self.read_primary if observed_read_only else primary
        cleanup_failed = False
        self.record["body_complete"] = self.eof
        if pending is not None:
            self.record["source_error_type"] = type(pending).__name__
        elif self.status_code == 200 and not self.eof:
            pending = _F07Refusal("F07_SUCCESS_BODY_NOT_COMPLETE")
        try:
            self.response.close()
        except BaseException as close_error:
            cleanup_failed = True
            if pending is None:
                pending = close_error
                self.record["source_error_type"] = type(close_error).__name__
            else:
                _f07_annotate(pending, "F07_SECONDARY_NATIVE_CLOSE_FAILED")
        try:
            self.harness._retain(self.record, bytes(self.body))
        except BaseException as evidence_error:
            cleanup_failed = True
            if pending is None:
                pending = evidence_error
            else:
                _f07_annotate(pending, "F07_SECONDARY_EVIDENCE_FAILED")
        finally:
            self.harness.active = None
            self.body.clear()
        if pending is not None and pending is not primary:
            # The real caller already owns the read error. A clean close must
            # return normally so its existing retry policy remains effective.
            # Secondary cleanup/evidence failure still signals that same error
            # to native A2, which stops rather than starting another GET.
            if observed_read_only and not cleanup_failed:
                return
            self.harness.failed = True
            raise pending


class _F07RawBody:
    def __init__(self, response):
        self.response = response
        # Preserve the original multi-occurrence header object. Flattened
        # response.headers is not a native raw-header proof.
        self.headers = response.response.raw.headers

    def read(self, amount, decode_content=False):
        return self.response.read(amount, decode_content=decode_content)

    def close(self):
        return self.response.close()

    def release_conn(self):
        return self.response.response.raw.release_conn()


class _F07CaseSession:
    """Explicit session facade. Adds no source-access or runtime authority."""
    def __init__(self, harness, role, case, session):
        if role not in _F07_ROLES or case not in _F07_CASES:
            raise _F07Refusal("F07_SESSION_SCOPE")
        self.harness, self.role = harness, role
        self.case, self.session = dict(case), session
        self.attempts = {}

    def get(self, url, **kwargs):
        self.harness.check()
        try:
            parts = urlsplit(url)
            params = parse_qs(parts.query, keep_blank_values=True, strict_parsing=True)
            expected_path = ("/iss/engines/stock/markets/shares/securities/"
                             + self.case["ticker"] + "/candles.csv")
            valid = (parts.scheme == "https" and parts.netloc == "iss.moex.com"
                     and not parts.fragment and parts.path == expected_path
                     and set(kwargs) <= {"headers", "timeout", "verify", "allow_redirects", "stream"}
                     and set(params) == {"from", "till", "interval", "start"}
                     and all(len(value) == 1 for value in params.values())
                     and params["interval"] == [str(self.case["interval"])]
                     and 1 <= len(params["start"][0]) <= 10
                     and params["start"][0].isascii() and params["start"][0].isdigit()
                     and kwargs.get("verify") is True
                     and kwargs.get("allow_redirects") is False
                     and kwargs.get("stream") is True)
            if valid:
                lower, upper = params["from"][0], params["till"][0]
                valid = (_f07_date.fromisoformat(lower).isoformat() == lower
                         and _f07_date.fromisoformat(upper).isoformat() == upper
                         and self.case["from_date"] <= lower <= upper <= self.case["till_date"])
            if not valid:
                raise ValueError("scope")
        except (ValueError, TypeError, KeyError):
            self.harness.failed = True
            raise _F07Refusal("F07_ENDPOINT_BOUNDARY") from None
        attempt = self.attempts.get(url, 0) + 1
        self.attempts[url] = attempt
        return self.harness.get(self.role, lambda case, start: self.session.get(url, **kwargs),
                                self.case, int(params["start"][0]), attempt=attempt,
                                request_bounds=dict(from_date=lower, till_date=upper))


class _F07RequestsFacade:
    """Only the candle path's get and original exception identities are exposed."""
    def __init__(self, session, original):
        self.get = session.get
        self.exceptions = original.exceptions
        self.Timeout = original.Timeout


@_f07_contextmanager
def _f07_candidate_requests(namespace, session):
    """Bind only an approved fresh fixture namespace, never global requests.get.

    Loading/source admission is external. Real data_gathering module globals and
    the requests module dictionary are rejected. The namespace must already have
    exact admitted definitions, with a distinct _f07_candidate_* module name.
    """
    if (not isinstance(namespace, dict)
            or not str(namespace.get("__name__", "")).startswith("_f07_candidate_")
            or "requests" not in namespace):
        raise _F07Refusal("F07_OWNED_FIXTURE_NAMESPACE_REQUIRED")
    original = namespace["requests"]
    if namespace is getattr(original, "__dict__", None):
        raise _F07Refusal("F07_GLOBAL_REQUESTS_NAMESPACE_FORBIDDEN")
    facade = _F07RequestsFacade(session, original)
    namespace["requests"] = facade
    try:
        yield facade
    finally:
        primary = _f07_sys.exception()
        if namespace.get("requests") is not facade:
            session.harness.failed = True
            if primary is None:
                raise _F07Refusal("F07_FIXTURE_BINDING_DRIFT")
            _f07_annotate(primary, "F07_FIXTURE_BINDING_DRIFT")
        else:
            namespace["requests"] = original



class _F07LiteralReply:
    def __init__(self, body=b"", *, status=200, error=None):
        self.raw = self
        self.stream = io.BytesIO(body)
        self.status_code = status
        self.headers = {"Content-Type": "text/csv"}
        self.encoding = "utf-8"
        self.error = error
        self.closed = False

    def read(self, amount, decode_content=False):
        if self.error is not None:
            raise self.error
        return self.stream.read(amount)

    def close(self):
        self.closed = True

    def release_conn(self):
        return "released"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise OSError("literal HTTP fault", self.status_code)


def _f07_consume(harness, role, request, *, case=_F07_CASES[0], start=0, attempt=1, terminal=False):
    response = harness.get(role, request, case, start, attempt=attempt)
    chunks = []
    try:
        while True:
            part = response.raw.read(65536, decode_content=False)
            if not part:
                break
            chunks.append(part)
        if terminal:
            response.mark_terminal()
        return b"".join(chunks)
    finally:
        response.close()


def test_f07_accounting_eight_cases_literal_dates_no_source_acceptance(tmp_path):
    assert [(c["ticker"], c["interval"], c["from_date"], c["till_date"]) for c in _F07_CASES] == [
        ("SBER", 24, "2023-02-27", "2024-03-02"),
        ("SBER", 60, "2023-02-27", "2024-03-02"),
        ("SBER", 10, "2024-01-30", "2024-03-02"),
        ("SBER", 1, "2024-02-23", "2024-03-02"),
        ("MOEX", 24, "2023-02-27", "2024-03-02"),
        ("MOEX", 60, "2023-02-27", "2024-03-02"),
        ("MOEX", 10, "2024-01-30", "2024-03-02"),
        ("MOEX", 1, "2024-02-23", "2024-03-02"),
    ]
    h = _F07Harness(tmp_path / "capture")
    def work(current):
        for case in _F07_CASES:
            for role in _F07_ROLES:
                _f07_consume(current, role, lambda *_: _F07LiteralReply(b"literal"), case=case, terminal=True)
        return "observed offline only"
    result, seal = h.run(work)
    assert result == "observed offline only"
    assert seal["used"] == {"gets": 24, "bytes": 168, "rows": 0}
    assert seal["attempts"] == {role: 8 for role in _F07_ROLES}
    assert seal["files"] == 50
    receipt = json.loads((h.sink.root / "receipt.json").read_bytes())
    assert receipt["source_acceptance"] == "NOT_VERIFIED"
    assert receipt["status"] == "CAPTURE_RECORDED_NOT_ACCEPTANCE"
    assert receipt["evidence_bytes"] == sum(p.stat().st_size for p in h.sink.root.iterdir())


def test_f07_accounting_raw_and_base64_envelope_and_final_files_share_exact_quota(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    h.run(lambda current: _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(b"abc"), terminal=True))
    body = (h.sink.root / "attempt-0001.body").read_bytes()
    envelope_bytes = (h.sink.root / "attempt-0001.json").read_bytes()
    envelope = json.loads(envelope_bytes)
    assert body == b"abc" and envelope["body_base64"] == "YWJj"
    assert _f07_base64.b64decode(envelope["body_base64"]) == b"abc"
    assert h.sink.reserved == sum(p.stat().st_size for p in h.sink.root.iterdir())
    assert h.sink.reserved > 3 + 4 + len(envelope_bytes)
    assert set(h.sink.files) == {"attempt-0001.body", "attempt-0001.json", "manifest.json", "receipt.json"}


@pytest.mark.parametrize("body", [b"", b"abc"])
def test_f07_accounting_empty_body_metadata_and_base64_refuse_raw_only_budget(tmp_path, body):
    limits = dict(_F07_LIMITS, evidence_bytes=max(1, len(body)))
    h = _F07Harness(tmp_path / "capture", limits=limits)
    with pytest.raises(_F07Refusal, match="F07_TOTAL_EVIDENCE_CAPACITY"):
        h.run(lambda current: _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(body)))
    assert not h.sealed
    assert not (h.sink.root / "receipt.json").exists()


def test_f07_accounting_final_manifest_receipt_must_also_fit(tmp_path):
    first = _F07Harness(tmp_path / "first")
    _f07_consume(first, "CANDIDATE", lambda *_: _F07LiteralReply(b"abc"))
    payload_size = first.sink.reserved
    limits = dict(_F07_LIMITS, evidence_bytes=payload_size)
    h = _F07Harness(tmp_path / "second", limits=limits)
    with pytest.raises(_F07Refusal, match="F07_TOTAL_EVIDENCE_CAPACITY"):
        h.run(lambda current: _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(b"abc")))
    assert not h.sealed
    assert not (h.sink.root / "manifest.json").exists()
    assert not (h.sink.root / "receipt.json").exists()


@pytest.mark.parametrize("physical_count", [512, 513])
def test_f07_accounting_exact_512_aggregate_candidate_references_retry_and_terminal(tmp_path, physical_count):
    h = _F07Harness(tmp_path / "capture")
    calls = []
    def request(case, start):
        calls.append(start)
        return _F07LiteralReply(b"", status=503 if len(calls) == 1 else 200)
    def work(current):
        for i in range(physical_count):
            role = _F07_ROLES[i % 3]
            _f07_consume(current, role, request, start=i, attempt=2 if i == 1 else 1, terminal=i == 511)
    if physical_count == 513:
        with pytest.raises(_F07Refusal, match="F07_GET_CAPACITY"):
            h.run(work)
        assert not h.sealed and not (h.sink.root / "receipt.json").exists()
    else:
        _, seal = h.run(work)
        assert seal["used"]["gets"] == 512 and h.sealed
    assert len(calls) == 512
    assert h.used == {"gets": 512, "bytes": 0, "rows": 0}
    assert h.attempts == {"REFERENCE_BEFORE": 171, "CANDIDATE": 171, "REFERENCE_AFTER": 170}
    assert h.records[0]["status"] == 503
    assert h.records[1]["attempt"] == 2
    assert h.records[-1]["terminal"] is True
    assert len(calls) == 512  # 513 refuses BEFORE another transport call


def test_f07_accounting_failed_transport_attempt_charged_before_call_and_no_partial_return(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    primary = OSError("source fault", 37)
    calls = []
    def request(*_):
        calls.append(h.used["gets"])
        raise primary
    with pytest.raises(OSError) as caught:
        h.run(lambda current: current.get("CANDIDATE", request, _F07_CASES[0], 0))
    assert caught.value is primary and caught.value.args == ("source fault", 37)
    assert calls == [1] and h.used["gets"] == 1
    assert h.records[0]["source_error_type"] == "OSError"
    assert not h.sealed and not (h.sink.root / "receipt.json").exists()


def test_f07_accounting_native_attempt_fault_can_follow_existing_retry_policy(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    primary = TimeoutError("same retry primary", 180)
    observed = []
    def request(*_):
        observed.append(h.used["gets"])
        if len(observed) == 1:
            raise primary
        return _F07LiteralReply(b"literal")
    def work(current):
        try:
            current.get("CANDIDATE", request, _F07_CASES[0], 0)
        except TimeoutError as caught:
            assert caught is primary and caught.args == ("same retry primary", 180)
        assert not current.failed
        return _f07_consume(current, "CANDIDATE", request, attempt=2, terminal=True)
    result, seal = h.run(work)
    assert result == b"literal" and observed == [1, 2]
    assert seal["used"] == {"gets": 2, "bytes": 7, "rows": 0}
    assert h.records[0]["source_error_type"] == "TimeoutError"
    assert h.records[1]["attempt"] == 2 and h.records[1]["terminal"]


def test_f07_accounting_read_fault_native_primary_survives_secondary_evidence_failure(tmp_path):
    primary = TimeoutError("original native", 180)
    def failing_writer(stream, data):
        stream.write(data[:1])
        raise OSError("secondary evidence failure")
    h = _F07Harness(tmp_path / "capture", writer=failing_writer)
    def work(current):
        response = current.get("CANDIDATE", lambda *_: _F07LiteralReply(error=primary), _F07_CASES[0], 0)
        response.raw.read(3)
        raise AssertionError("unreachable")
    with pytest.raises(TimeoutError) as caught:
        h.run(work)
    assert caught.value is primary and caught.value.args == ("original native", 180)
    assert not h.sealed and not (h.sink.root / "receipt.json").exists()


def test_f07_accounting_native_finally_close_preserves_read_primary_object(tmp_path):
    primary = TimeoutError("native finally primary", 180)
    def failing_writer(stream, data):
        raise OSError("secondary close evidence")
    h = _F07Harness(tmp_path / "capture", writer=failing_writer)
    with pytest.raises(TimeoutError) as caught:
        h.run(lambda current: _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(error=primary)))
    assert caught.value is primary and primary.args == ("native finally primary", 180)
    assert not h.sealed


def test_f07_accounting_partial_evidence_write_is_failure_no_success_receipt(tmp_path):
    def partial_writer(stream, data):
        if data:
            stream.write(data[:1])
            return 1
        return 0
    h = _F07Harness(tmp_path / "capture", writer=partial_writer)
    with pytest.raises(OSError, match="F07_SHORT_EVIDENCE_WRITE"):
        h.run(lambda current: _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(b"abcd")))
    assert (h.sink.root / "attempt-0001.body").read_bytes() == b"a"
    assert h.sink.reserved > 1 and h.sink.failed and not h.sealed
    assert not (h.sink.root / "receipt.json").exists()


@pytest.mark.parametrize("tamper", ["extra", "changed", "missing"])
def test_f07_accounting_inventory_content_tamper_cannot_seal(tmp_path, tamper):
    h = _F07Harness(tmp_path / "capture")
    def work(current):
        _f07_consume(current, "CANDIDATE", lambda *_: _F07LiteralReply(b"abc"))
        body = current.sink.root / "attempt-0001.body"
        if tamper == "extra":
            (current.sink.root / "unaccounted").write_bytes(b"x")
        elif tamper == "changed":
            body.write_bytes(b"xyz")
        else:
            body.unlink()
        return "must never return"
    with pytest.raises(_F07Refusal, match="F07_EVIDENCE_(INVENTORY|CONTENT)_MISMATCH"):
        h.run(work)
    assert not h.sealed and not (h.sink.root / "receipt.json").exists()


def test_f07_accounting_wire_body_rows_deadline_caps_and_no_next_transport(tmp_path):
    calls = []
    h = _F07Harness(tmp_path / "body", limits=dict(_F07_LIMITS, body_bytes=3))
    def request(*_):
        calls.append(1)
        return _F07LiteralReply(b"abcd")
    with pytest.raises(_F07Refusal, match="F07_BODY_CAPACITY"):
        h.run(lambda current: _f07_consume(current, "CANDIDATE", request))
    assert calls == [1] and h.used["bytes"] == 4 and not h.sealed
    assert not (h.sink.root / "receipt.json").exists()
    rows = _F07Harness(tmp_path / "rows", limits=dict(_F07_LIMITS, rows=1))
    with pytest.raises(_F07Refusal, match="F07_ROWS_OR_FRAME_CAPACITY"):
        rows.run(lambda current: current.admit_rows(2))
    ticks = iter([0, 2700])
    deadline = _F07Harness(tmp_path / "time", clock=lambda: next(ticks))
    with pytest.raises(TimeoutError, match="F07_DEADLINE"):
        deadline.run(lambda current: current.get("CANDIDATE", request, _F07_CASES[0], 0))
    assert calls == [1] and deadline.used["gets"] == 0


def test_f07_accounting_source_primary_after_prefix_suppresses_later_role_and_result(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    primary = RuntimeError("late original", 9)
    roles = []
    def work(current):
        roles.append("REFERENCE_BEFORE")
        _f07_consume(current, roles[-1], lambda *_: _F07LiteralReply(b"exact raw 9007199254740993"))
        raise primary
    with pytest.raises(RuntimeError) as caught:
        h.run(work)
    assert caught.value is primary and primary.args == ("late original", 9)
    assert roles == ["REFERENCE_BEFORE"] and h.used["gets"] == 1
    assert not h.sealed and not (h.sink.root / "receipt.json").exists()


def test_f07_accounting_no_multiple_unclosed_responses_or_unbounded_reads(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    response = h.get("CANDIDATE", lambda *_: _F07LiteralReply(b"abc"), _F07_CASES[0], 0)
    with pytest.raises(_F07Refusal, match="UNCLOSED_RESPONSE"):
        h.get("REFERENCE_AFTER", lambda *_: _F07LiteralReply(), _F07_CASES[0], 0)
    with pytest.raises(_F07Refusal, match="F07_SUCCESS_BODY_NOT_COMPLETE"):
        response.close()
    assert h.used["gets"] == 1 and h.failed
    other = _F07Harness(tmp_path / "other")
    response = other.get("CANDIDATE", lambda *_: _F07LiteralReply(b"abc"), _F07_CASES[0], 0)
    with pytest.raises(_F07Refusal, match="F07_BOUNDED_IDENTITY_READ_REQUIRED"):
        response.raw.read(-1)
    with pytest.raises(_F07Refusal, match="F07_SUCCESS_BODY_NOT_COMPLETE"):
        response.close()
    assert other.used["gets"] == 1 and other.failed


_F07_URL = ("https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER/"
       "candles.csv?from=2023-02-27&till=2024-03-02&interval=24&start=0")
_F07_REQUEST_KWARGS = dict(verify=True, allow_redirects=False, stream=True)


def test_f07_accounting_explicit_session_and_fixture_namespace_intercept_all_three_roles(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    calls = []
    replies = []
    original = SimpleNamespace(exceptions=SimpleNamespace(RequestException=OSError), Timeout=TimeoutError)
    def forbidden_global_get(*args, **kwargs):
        raise AssertionError("global transport must not be used")
    original.get = forbidden_global_get
    class Session:
        def get(self, url, **kwargs):
            calls.append((url, kwargs))
            response = _F07LiteralReply(b"literal")
            replies.append(response)
            return response
    # A fresh namespace performs the same requests global lookup as the native
    # candle functions. It is deliberately NOT the real product/module loader.
    namespace = {"__name__": "_f07_candidate_literal", "requests": original}
    exec("def candidate(url, kwargs):\n    return requests.get(url, **kwargs)\n", namespace)
    def work(current):
        bodies = []
        for role in _F07_ROLES:
            session = _F07CaseSession(current, role, _F07_CASES[0], Session())
            with _f07_candidate_requests(namespace, session) as facade:
                assert facade.exceptions is original.exceptions and facade.Timeout is TimeoutError
                response = namespace["candidate"](_F07_URL, _F07_REQUEST_KWARGS)
                assert response.raw.headers is replies[-1].raw.headers
                assert response.raise_for_status() is None
                assert response.raw.release_conn() == "released"
                bodies.append(response.raw.read(65536))
                assert response.raw.read(65536) == b""
                response.mark_terminal()
                response.close()
            assert namespace["requests"] is original and original.get is forbidden_global_get
        return bodies
    result, seal = h.run(work)
    assert result == [b"literal", b"literal", b"literal"]
    assert len(calls) == 3 and all(reply.closed for reply in replies)
    assert seal["used"] == {"gets": 3, "bytes": 21, "rows": 0}
    assert seal["attempts"] == {role: 1 for role in _F07_ROLES}


@pytest.mark.parametrize("mutation", ["http", "host", "ticker", "date", "interval", "duplicate", "redirect", "params",
                                       "params_none", "proxies", "auth", "hooks", "cookies", "data", "json", "unknown"])
def test_f07_accounting_explicit_session_boundary_refuses_before_transport(tmp_path, mutation):
    h = _F07Harness(tmp_path / "capture")
    calls = []
    class Session:
        def get(self, *args, **kwargs):
            calls.append(1)
            raise AssertionError("transport must not run")
    url = _F07_URL
    replacements = {"http": ("https:", "http:"), "host": ("iss.moex.com", "example.com"),
                    "ticker": ("SBER/", "MOEX/"), "date": ("2023-02-27", "2023-02-26"),
                    "interval": ("interval=24", "interval=1"), "duplicate": ("start=0", "start=0&start=1")}
    kwargs = dict(_F07_REQUEST_KWARGS)
    if mutation == "redirect":
        kwargs["allow_redirects"] = True
    elif mutation == "params":
        # requests applies params after this URL was validated. This must be
        # rejected before GET rather than silently changing the charged scope.
        kwargs["params"] = {"interval": "1", "from": "1900-01-01"}
    elif mutation == "params_none":
        kwargs["params"] = None
    elif mutation in {"proxies", "auth", "hooks", "cookies", "data", "json", "unknown"}:
        # Reject unsupported options even with an inert value. The endpoint
        # contract must not depend on requests silently ignoring a value.
        kwargs[mutation] = None
    else:
        before, after = replacements[mutation]
        url = url.replace(before, after)
    session = _F07CaseSession(h, "CANDIDATE", _F07_CASES[0], Session())
    with pytest.raises(_F07Refusal, match="F07_ENDPOINT_BOUNDARY"):
        h.run(lambda current: session.get(url, **kwargs))
    assert not calls and h.used["gets"] == 0 and not h.sealed


def test_f07_accounting_real_product_namespace_is_not_mutated(tmp_path):
    h = _F07Harness(tmp_path / "capture")
    session = _F07CaseSession(h, "CANDIDATE", _F07_CASES[0], SimpleNamespace())
    namespace = {"__name__": "data_gathering", "requests": object()}
    before = dict(namespace)
    with pytest.raises(_F07Refusal, match="F07_OWNED_FIXTURE_NAMESPACE_REQUIRED"):
        with _f07_candidate_requests(namespace, session):
            raise AssertionError("must not enter")
    assert namespace == before


# The following orchestration is deliberately offline: a live session/entrypoint
# is absent. Full-source loading and the existing native pytest boundary supply
# candidate identity; the reference uses its own CSV and numeric comparisons.
_F07_PRODUCT_SHA256 = '6e3697bb1f99031e40211996757b1b4a89c34cb46b954b1ae0cd4c80a84adabb'
_F07_PUBLIC = ('open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker')
_F07_DATES = {
    24: ('2023-02-28', '2023-03-02', '2023-03-03', '2024-03-01'),
    60: ('2023-02-28', '2023-03-02', '2023-03-03', '2024-03-01'),
    10: ('2024-01-31', '2024-02-02', '2024-02-03', '2024-03-01'),
    1: ('2024-02-23', '2024-02-24', '2024-02-25', '2024-03-01'),
}
_F07_LEAVES = {
    24: (('2023-03-02', '2024-03-02'), ('2023-02-27', '2023-03-03')),
    60: (('2023-03-02', '2024-03-02'), ('2023-02-27', '2023-03-03')),
    10: (('2024-02-02', '2024-03-02'), ('2024-01-30', '2024-02-03')),
    1: (('2024-02-24', '2024-03-02'), ('2024-02-23', '2024-02-25')),
}


class _F07Clock:
    def __init__(self):
        self.elapsed = 0.
        self.sleeps = []

    def now(self):
        return self.elapsed

    def sleep(self, seconds):
        assert seconds == 3
        self.sleeps.append(seconds)
        self.elapsed += seconds


@pytest.fixture
def f07_candidate(monkeypatch):
    import mds_offline_guard as guard

    root = Path(__file__).resolve().parents[2]
    source = root / 'data_gathering.py'
    context = guard._CONTEXT
    metadata = guard.report_metadata()
    assert guard._INSTALLED is True and context.started and not context.finished
    assert context.root == root and metadata['source_mode'] == 'commit'
    assert metadata['pid'] == __import__('os').getpid()
    assert metadata['source_digest'] == context.source_digest
    assert hashlib.sha256(source.read_bytes()).hexdigest() == _F07_PRODUCT_SHA256
    monkeypatch.syspath_prepend(str(root))
    name = '_f07_candidate_' + _f07_uuid.uuid4().hex
    specification = _f07_importlib.spec_from_file_location(name, source)
    module = _f07_importlib.module_from_spec(specification)
    _f07_sys.modules[name] = module
    original_get = requests.get
    try:
        specification.loader.exec_module(module)
        assert Path(module.__file__) == source
        for function in (module.moex_query, module._a3_history, module._a2_candle_frame):
            assert function.__globals__ is vars(module)
            assert Path(function.__code__.co_filename) == source
        assert module.requests is requests and module._a3_offline_test_profile() is True
        yield module
    finally:
        assert requests.get is original_get
        assert guard._CONTEXT is context and guard._INSTALLED is True
        assert context.started and not context.finished
        _f07_sys.modules.pop(name, None)


def _f07_literal_rows(case, *, unsafe=False):
    rows = []
    for position, day in enumerate(_F07_DATES[case['interval']]):
        opening = ('0.5' if position == 3 else '9007199254740993') if unsafe else '100'
        rows.append((opening, '101', '102', '99', '202000', '2000',
                     day + ' 00:00:00', day + ' 00:00:59'))
    return rows


def _f07_body(rows):
    return (HEADER + ''.join(';'.join(row) + '\n' for row in rows)).encode('utf-8')


class _F07ModelSession:
    """Literal closed/closed endpoint model; no assertion about real ISS."""
    def __init__(self, role, case, calls, *, retry=False, primary=None,
                 changed=False, malformed=False, unsafe=False, empty=False, read_primary=None):
        self.role, self.case, self.calls = role, case, calls
        self.retry, self.primary = retry, primary
        self.changed, self.malformed, self.unsafe, self.empty = changed, malformed, unsafe, empty
        self.read_primary = read_primary
        self.called = 0
        self.replies = []

    def get(self, url, **kwargs):
        params = parse_qs(urlsplit(url).query, strict_parsing=True)
        self.called += 1
        self.calls.append(dict(role=self.role, case=dict(self.case), params=params,
                               verify=kwargs.get('verify'), redirects=kwargs.get('allow_redirects'),
                               stream=kwargs.get('stream')))
        if self.primary is not None:
            raise self.primary
        status = 503 if self.retry and self.called == 1 else 200
        rows = [] if self.empty else _f07_literal_rows(self.case, unsafe=self.unsafe)
        if self.changed:
            rows[0] = ('999', *rows[0][1:])
        lower, upper = params['from'][0], params['till'][0]
        rows = [row for row in rows if lower <= row[6][:10] <= upper]
        start = int(params['start'][0])
        # Unsafe fixtures split numeric pages so the real assembly must audit
        # int64/float64 promotion; the reference still sees raw literal cells.
        page = rows[start:start + 1] if self.unsafe and self.role == 'CANDIDATE' else rows[start:]
        body = b'INVALID\n' if self.malformed else _f07_body(page)
        if status != 200:
            body = b''
        value = response(status=status)
        value.encoding = 'utf-8'
        value.raw = Raw(body, value.headers.items())
        if self.read_primary is not None and self.called == 1:
            primary = self.read_primary

            class FailedRead(Raw):
                def read(self, amount, decode_content=False):
                    assert type(amount) is int and 0 < amount <= 65536
                    assert decode_content is False
                    raise primary

            value.raw = FailedRead(body, value.headers.items())
        self.replies.append(value)
        return value


def _f07_url(case, start):
    return ('https://iss.moex.com/iss/engines/stock/markets/shares/securities/'
            + case['ticker'] + '/candles.csv?from=' + case['from_date']
            + '&till=' + case['till_date'] + '&interval=' + str(case['interval'])
            + '&start=' + str(start))


def _f07_reference_records(body, codec='utf-8'):
    text = body.decode(codec, errors='strict')
    table = list(_f07_csv.reader(io.StringIO(text, newline=''), delimiter=';', strict=True))
    if (len(table) < 2 or table[0] != ['candles']
            or table[1] != list(_F07_PUBLIC[:8])):
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    rows = [tuple(row) for row in table[2:] if row]
    if any(len(row) != 8 for row in rows):
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    return rows


def _f07_reference_codec(headers):
    # Independent closed consumer profile, never requests' encoding guess.
    values = headers.getlist('Content-Type')
    encodings = headers.getlist('Content-Encoding')
    if (len(values) != 1 or len(encodings) > 1
            or encodings and encodings[0].strip().lower() != 'identity'):
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    value = values[0]
    if type(value) is not str:
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    parts = [item.strip().lower() for item in value.split(';')]
    if parts[0] != 'text/csv' or len(parts) > 2:
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    if len(parts) == 1:
        return 'cp1251'
    parameter = parts[1].split('=', 1)
    if len(parameter) != 2 or parameter[0].strip() != 'charset':
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    name = parameter[1].strip().strip('"')
    codec = {'utf-8': 'utf-8', 'utf8': 'utf-8', 'cp1251': 'cp1251',
             'windows-1251': 'cp1251'}.get(name)
    if codec is None:
        raise _F07Refusal('F07_REFERENCE_SCHEMA')
    return codec


def _f07_reference(harness, role, case, session, clock):
    adapter = _F07CaseSession(harness, role, case, session)
    start, result, seen = 0, [], set()
    while True:
        current = adapter.get(_f07_url(case, start), headers={'Accept-Encoding': 'identity'},
                              timeout=(5, 30), verify=True, allow_redirects=False, stream=True)
        try:
            current.raise_for_status()
            if current.status_code != 200:
                raise _F07Refusal('F07_REFERENCE_TRANSPORT')
            chunks = []
            while True:
                part = current.raw.read(65536, decode_content=False)
                if not part:
                    break
                chunks.append(part)
            codec = _f07_reference_codec(current.raw.headers)
            rows = _f07_reference_records(b''.join(chunks), codec)
            current.record['codec'] = codec
            harness.admit_rows(len(rows))
            current.record['validated_rows'] = len(rows)
            if not rows:
                current.mark_terminal()
        finally:
            current.close()
        clock.sleep(3)
        harness.check()
        if not rows:
            return result
        for row in rows:
            if row[6] in seen or result and row[6] <= result[-1][6]:
                raise _F07Refusal('F07_REFERENCE_ORDER_OR_OVERLAP')
            seen.add(row[6])
            result.append(row)
        start += len(rows)


def _f07_number(value):
    if isinstance(value, _f07_numbers.Integral) and not isinstance(value, bool):
        return int(value), 1, False
    if not isinstance(value, _f07_numbers.Real) or not math.isfinite(value):
        raise _F07Refusal('F07_NUMERIC_VALUE')
    native = float(value)
    return (*native.as_integer_ratio(), native == 0 and math.copysign(1, native) < 0)


def _f07_compare(case, before, frame, after):
    if before != after:
        raise _F07Refusal('F07_SOURCE_CHANGED_NOT_VERIFIED')
    expected = {}
    for row in before:
        if row[6] in expected or expected and row[6] <= next(reversed(expected)):
            raise _F07Refusal('F07_REFERENCE_ORDER_OR_OVERLAP')
        expected[row[6]] = row
    if list(frame.columns) != (list(_F07_PUBLIC) if expected else []):
        raise _F07Refusal('F07_CANDIDATE_SCHEMA')
    actual = {}
    for row in frame.itertuples(index=False, name=None):
        if row[6] in actual or actual and row[6] <= next(reversed(actual)):
            raise _F07Refusal('F07_CANDIDATE_ORDER_OR_DUPLICATE')
        actual[row[6]] = row
    if set(actual) != set(expected):
        raise _F07Refusal('F07_MEMBERSHIP_OR_NUMERIC_MISMATCH_NOT_VERIFIED')
    for key, source in expected.items():
        parsed = actual[key]
        for position in range(6):
            token = source[position]
            literal = int(token) if token.lstrip('+-').isdigit() else float(token)
            if _f07_number(parsed[position]) != _f07_number(literal):
                raise _F07Refusal('F07_MEMBERSHIP_OR_NUMERIC_MISMATCH_NOT_VERIFIED')
        if parsed[6:8] != source[6:8] or parsed[8] != case['ticker']:
            raise _F07Refusal('F07_MEMBERSHIP_OR_NUMERIC_MISMATCH_NOT_VERIFIED')
    return dict(case=dict(case), status='OBSERVED_RANGE_MATCH_SOURCE_ACCEPTANCE_OPEN',
                rows=len(frame), source_acceptance='NOT_VERIFIED')


@_f07_contextmanager
def _f07_candidate_binding(module, harness, case, session):
    original_charge, original_frame = module._a3_charge, module._a2_candle_frame
    original_codec = module._a2_candle_codec

    def charge(counter, amount, primary=None):
        result = original_charge(counter, amount, primary)
        if counter == 'rows':
            try:
                harness.admit_rows(amount)
            except BaseException:
                if primary is not None:
                    raise primary
                raise
        return result

    def validated(frame, rows):
        result = original_frame(frame, rows)
        harness.admit_rows(0, frame_bytes=int(frame.memory_usage(index=True, deep=True).sum()))
        current = harness.active
        if current is None or current.record['role'] != 'CANDIDATE':
            raise _F07Refusal('F07_CANDIDATE_RESPONSE_IDENTITY')
        current.record['validated_rows'] = len(rows)
        if not rows:
            current.mark_terminal()
        return result

    def codec(response, content):
        result = original_codec(response, content)
        current = harness.active
        if current is None or current.record['role'] != 'CANDIDATE':
            raise _F07Refusal('F07_CANDIDATE_RESPONSE_IDENTITY')
        current.record['codec'] = result
        return result

    module._a3_charge, module._a2_candle_frame = charge, validated
    module._a2_candle_codec = codec
    try:
        adapter = _F07CaseSession(harness, 'CANDIDATE', case, session)
        with _f07_candidate_requests(vars(module), adapter):
            yield
    finally:
        module._a3_charge, module._a2_candle_frame = original_charge, original_frame
        module._a2_candle_codec = original_codec


def _f07_run_protocol(module, harness, clock, calls, *, cases=_F07_CASES, retry=False,
                      primary=None, changed=False, malformed=False, unsafe=False, empty=False,
                      read_primary=None):
    import mds_offline_guard as guard

    metadata = guard.report_metadata()
    if metadata['source_mode'] != 'commit' or module._a3_offline_test_profile() is not True:
        raise _F07Refusal('F07_NATIVE_SOURCE_ADMISSION')
    original_time = module.time
    original_get = requests.get
    module.time = SimpleNamespace(perf_counter=clock.now, sleep=clock.sleep, time=lambda: 1709380800.)

    def work(current):
        provenance = dict(source_sha=metadata['tested_sha'], source_digest=metadata['source_digest'],
                          source_mode=metadata['source_mode'], product_sha256=_F07_PRODUCT_SHA256,
                          bootstrap_digest=metadata['bootstrap_digest'], ticket_id=metadata['ticket_id'],
                          run_id=metadata['run_id'], role=metadata['role'], pid=metadata['pid'],
                          candidate='FULL_CANONICAL_SOURCE_IMPORT', network='MOCKED_ONLY',
                          source_acceptance='NOT_VERIFIED', live_authority='NOT_GRANTED')
        current.sink.write('source-binding.json', _f07_encoded(provenance))
        reports = []
        with module._a3_source_diagnostic_scope():
            for ordinal, case in enumerate(cases, 1):
                before_session = _F07ModelSession('REFERENCE_BEFORE', case, calls, unsafe=unsafe, empty=empty)
                before = _f07_reference(current, 'REFERENCE_BEFORE', case, before_session, clock)
                candidate_session = _F07ModelSession('CANDIDATE', case, calls, retry=retry,
                    primary=primary, malformed=malformed, unsafe=unsafe, empty=empty,
                    read_primary=read_primary)
                with _f07_candidate_binding(module, current, case, candidate_session):
                    frame = module._a3_history(case['ticker'], 'Акции', case['from_date'],
                                               case['till_date'], case['interval'])
                current.admit_rows(0, frame_bytes=int(frame.memory_usage(index=True, deep=True).sum()))
                after_session = _F07ModelSession('REFERENCE_AFTER', case, calls,
                                                 changed=changed, unsafe=unsafe, empty=empty)
                after = _f07_reference(current, 'REFERENCE_AFTER', case, after_session, clock)
                report = _f07_compare(case, before, frame, after)
                current.sink.write(f'comparison-{ordinal:02d}.json', _f07_encoded(report))
                reports.append(report)
        return reports

    try:
        return harness.run(work)
    finally:
        module.time = original_time
        assert requests.get is original_get


def test_f07_native_eight_case_candidate_reference_accounting(f07_candidate, tmp_path, writers):
    clock, calls = _F07Clock(), []
    harness = _F07Harness(tmp_path / 'capture', clock=clock.now)
    reports, seal = _f07_run_protocol(f07_candidate, harness, clock, calls, retry=True)
    assert len(reports) == 8 and sum(item['rows'] for item in reports) == 32
    assert [item['case'] for item in reports] == list(_F07_CASES)
    assert all(item['status'] == 'OBSERVED_RANGE_MATCH_SOURCE_ACCEPTANCE_OPEN' for item in reports)
    assert seal['used']['gets'] == 72 and seal['used']['rows'] == 112
    assert seal['attempts'] == dict(REFERENCE_BEFORE=16, CANDIDATE=40, REFERENCE_AFTER=16)
    assert clock.sleeps == [3] * 72 and clock.elapsed == 216
    assert len(calls) == 72 and writers == []
    for case in _F07_CASES:
        current = [item for item in calls if item['case'] == case and item['role'] == 'CANDIDATE']
        bounds = [(item['params']['from'][0], item['params']['till'][0]) for item in current]
        first, second = _F07_LEAVES[case['interval']]
        assert bounds == [first, first, first, second, second]
        assert [item['params']['start'][0] for item in current] == ['0', '0', '3', '0', '3']
        assert all(item['params']['interval'] == [str(case['interval'])] for item in current)
        assert all(item['verify'] is True and item['redirects'] is False and item['stream'] is True
                   for item in current)
    assert sum(record['terminal'] for record in harness.records) == 32
    candidate = [record for record in harness.records if record['role'] == 'CANDIDATE']
    assert sum(record.get('validated_rows', 0) for record in candidate) == 48
    assert sum(record['status'] == 503 for record in candidate) == 8
    assert seal['used']['bytes'] == sum(record['body_bytes'] for record in harness.records)
    assert seal['evidence_bytes'] == sum(path.stat().st_size for path in harness.sink.root.iterdir())
    assert seal['evidence_bytes'] > seal['used']['bytes'] * 2
    assert seal['files'] == 155  # 72 bodies +72 envelopes +8 comparisons +provenance +manifest/receipt.
    assert harness.sink.audit() == seal['evidence_bytes']


def test_f07_native_empty_cases_keep_all_terminals_and_zero_columns(f07_candidate, tmp_path, writers):
    clock, calls = _F07Clock(), []
    harness = _F07Harness(tmp_path / 'capture', clock=clock.now)
    reports, seal = _f07_run_protocol(f07_candidate, harness, clock, calls, empty=True)
    assert [item['rows'] for item in reports] == [0] * 8
    assert seal['used']['gets'] == 32 and seal['used']['rows'] == 0
    assert seal['attempts'] == dict(REFERENCE_BEFORE=8, CANDIDATE=16, REFERENCE_AFTER=8)
    assert all(item['terminal'] for item in harness.records)
    assert clock.sleeps == [3] * 32 and writers == []


@pytest.mark.parametrize('fault', ['gets', 'rows', 'evidence', 'source_changed', 'schema', 'unsafe'])
def test_f07_native_refusal_after_prefix_has_no_next_case_or_writer(
        f07_candidate, tmp_path, writers, fault):
    clock, calls = _F07Clock(), []
    limits = dict(_F07_LIMITS)
    if fault == 'gets':
        limits['gets'] = 3  # Reference-before completed; only first candidate page fits.
    elif fault == 'rows':
        limits['rows'] = 6  # Four reference rows +first candidate's three refuses.
    elif fault == 'evidence':
        limits['evidence_bytes'] = 1
    harness = _F07Harness(tmp_path / 'capture', clock=clock.now, limits=limits)
    expected = dict(gets='F07_GET_CAPACITY', rows='F07_ROWS_OR_FRAME_CAPACITY',
                    evidence='F07_TOTAL_EVIDENCE_CAPACITY',
                    source_changed='F07_SOURCE_CHANGED_NOT_VERIFIED',
                    schema='A2_ISS_RESPONSE_SCHEMA_INVALID', unsafe='A3_ISS_NUMERIC_ASSEMBLY_LOSS')[fault]
    with pytest.raises(ValueError, match='^' + expected + '$'):
        _f07_run_protocol(f07_candidate, harness, clock, calls,
                          changed=fault == 'source_changed', malformed=fault == 'schema', unsafe=fault == 'unsafe')
    assert harness.failed and not harness.sealed and writers == []
    assert all(item['case'] == _F07_CASES[0] for item in calls)
    assert not (harness.sink.root / 'receipt.json').exists()
    if fault == 'gets':
        assert len(calls) == 3 and calls[-1]['role'] == 'CANDIDATE'
    if fault == 'evidence':
        assert calls == []
    if fault not in ('source_changed',):
        assert not any(item['role'] == 'REFERENCE_AFTER' for item in calls)


def test_f07_native_primary_survives_late_evidence_failure(f07_candidate, tmp_path, writers):
    clock, calls = _F07Clock(), []
    primary = requests.exceptions.SSLError('F07_LITERAL_NATIVE_PRIMARY')
    writes = []

    def write(stream, data):
        writes.append(stream.name)
        # Source binding and both reference pages are retained first. Evidence
        # on the candidate's failed physical attempt must preserve its primary.
        if len(writes) >= 6:
            raise OSError('F07_LITERAL_SECONDARY_EVIDENCE')
        return stream.write(data)

    harness = _F07Harness(tmp_path / 'capture', clock=clock.now, writer=write)
    with pytest.raises(requests.exceptions.SSLError) as captured:
        _f07_run_protocol(f07_candidate, harness, clock, calls, primary=primary)
    assert captured.value is primary and type(primary) is requests.exceptions.SSLError
    assert primary.args == ('F07_LITERAL_NATIVE_PRIMARY',)
    assert 'F07_SECONDARY_EVIDENCE_FAILED' in primary.__notes__
    assert [item['role'] for item in calls] == ['REFERENCE_BEFORE', 'REFERENCE_BEFORE', 'CANDIDATE']
    assert harness.used['gets'] == 3 and harness.failed and not harness.sealed
    assert writers == [] and not (harness.sink.root / 'receipt.json').exists()


def test_f07_native_read_timeout_preserves_retry_policy_and_full_sleep(
        f07_candidate, tmp_path, writers):
    clock, calls = _F07Clock(), []
    primary = requests.exceptions.ReadTimeout('F07_LITERAL_READ_PRIMARY')
    harness = _F07Harness(tmp_path / 'capture', clock=clock.now)
    reports, seal = _f07_run_protocol(f07_candidate, harness, clock, calls,
                                      cases=_F07_CASES[:1], read_primary=primary)
    assert len(reports) == 1 and reports[0]['rows'] == 4
    assert seal['used']['gets'] == 9 and seal['used']['rows'] == 14
    assert seal['attempts'] == dict(REFERENCE_BEFORE=2, CANDIDATE=5, REFERENCE_AFTER=2)
    assert clock.sleeps == [3] * 9 and clock.elapsed == 27
    candidate = [item for item in harness.records if item['role'] == 'CANDIDATE']
    assert candidate[0]['source_error_type'] == 'ReadTimeout'
    assert candidate[0]['body_complete'] is False
    assert candidate[0]['body_bytes'] == 0 and candidate[0]['attempt'] == 1
    assert candidate[1]['attempt'] == 2 and candidate[1]['start'] == 0
    assert primary.args == ('F07_LITERAL_READ_PRIMARY',) and writers == []
    assert seal['evidence_bytes'] == sum(path.stat().st_size for path in harness.sink.root.iterdir())


def test_f07_native_read_primary_survives_secondary_evidence_failure_without_retry(
        f07_candidate, tmp_path, writers):
    clock, calls = _F07Clock(), []
    primary = requests.exceptions.ReadTimeout('F07_LITERAL_READ_AND_EVIDENCE_PRIMARY', 180)
    writes = []

    def write(stream, data):
        writes.append(stream.name)
        if len(writes) >= 6:
            raise OSError('F07_LITERAL_READ_SECONDARY_EVIDENCE')
        return stream.write(data)

    harness = _F07Harness(tmp_path / 'capture', clock=clock.now, writer=write)
    with pytest.raises(requests.exceptions.ReadTimeout) as captured:
        _f07_run_protocol(f07_candidate, harness, clock, calls,
                          cases=_F07_CASES[:1], read_primary=primary)
    assert captured.value is primary and type(primary) is requests.exceptions.ReadTimeout
    assert primary.args == ('F07_LITERAL_READ_AND_EVIDENCE_PRIMARY', 180)
    assert [item['role'] for item in calls] == ['REFERENCE_BEFORE', 'REFERENCE_BEFORE', 'CANDIDATE']
    assert harness.used['gets'] == 3 and harness.failed and not harness.sealed
    assert clock.sleeps == [3, 3] and writers == []
    assert not (harness.sink.root / 'receipt.json').exists()


@pytest.mark.parametrize('fault', ['membership', 'numeric', 'signed_zero', 'source_changed', 'ticker'])
def test_f07_independent_comparison_refuses_literal_corruption(fault):
    case = _F07_CASES[0]
    source = [('9007199254740993', '101', '102', '99', '202000', '2000',
               '2024-03-01 00:00:00', '2024-03-01 00:00:59')]
    after = list(source)
    values = [[9007199254740993, 101, 102, 99, 202000, 2000, source[0][6], source[0][7], 'SBER']]
    if fault == 'membership':
        values = []
    elif fault == 'numeric':
        values[0][0] = float(9007199254740993)
    elif fault == 'signed_zero':
        source = [('-0.0', *source[0][1:])]
        after = list(source)
        values[0][0] = 0.
    elif fault == 'source_changed':
        after = [('100', *source[0][1:])]
    else:
        values[0][8] = 'MOEX'
    frame = pd.DataFrame(values, columns=_F07_PUBLIC)
    expected = ('F07_SOURCE_CHANGED_NOT_VERIFIED' if fault == 'source_changed'
                else 'F07_MEMBERSHIP_OR_NUMERIC_MISMATCH_NOT_VERIFIED')
    with pytest.raises(ValueError, match='^' + expected + '$'):
        _f07_compare(case, source, frame, after)
