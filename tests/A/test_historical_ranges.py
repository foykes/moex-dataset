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
