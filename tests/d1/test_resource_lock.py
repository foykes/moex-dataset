"""Target admission, independent mutex naming and actual overlapping processes."""

import ctypes
import hashlib
import importlib
import json
from pathlib import Path
import sys
import threading
import os
import stat
from types import SimpleNamespace

import pytest


PROCESS_CASES = ('deny_second', 'bounded_wait', 'serialize_updates', 'independent_directories',
    'same_directory_siblings', 'abandoned_before_commit', 'late_restart_before_commit',
    'crash_after_commit', 'late_restart_after_commit', 'directory_target_switch',
    'retry_false', 'retry_unverified', 'retry_exception', 'reader_visibility', 'late_restart_invalid_before_commit')
CRASH_CASES = {'abandoned_before_commit', 'late_restart_before_commit', 'crash_after_commit',
    'late_restart_after_commit', 'directory_target_switch', 'late_restart_invalid_before_commit'}


@pytest.mark.parametrize('case', PROCESS_CASES)
def test_process_cases(case, offline_guard, assert_artifact, oracle, project_root):
    result = offline_guard.run_d1_case(case)
    assert result['report_errors'] == []
    assert result['case'] == case
    slots = result['slots']
    root = Path(result['case_root'])
    if case in CRASH_CASES:
        dead = slots['holder']
        assert dead['actual_exit'] == 124 and dead['timed_out'] is False
        assert set(dead['report_errors']) == {'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'}
        assert dead['validated'] is False
        assert dead['termination'] is not None
        assert dead['termination']['phase'] == ('POSTCOMMIT_READY' if 'after_commit' in case else 'CANDIDATE_READY')
        assert dead['payload'] is None
        assert dead['counters'] is None
        # No controller invents a logging final or successful summary for this PID.
        dead_logs = project_root / '.f-log/d1' / offline_guard.ensure_context(project_root).run_id / case / 'holder'
        assert list(dead_logs.rglob('run_summary.json')) == []
        assert list(dead_logs.rglob('summary.sha256')) == []
        terminal = {'run_completed', 'run_failed', 'run_interrupted', 'worker_completed',
            'worker_failed', 'worker_interrupted', 'd1_lock_released'}
        for path in dead_logs.rglob('*.jsonl'):
            for line in path.read_text(encoding='utf-8').splitlines():
                record = json.loads(line)
                event = record['event']
                assert (event['event'] if type(event) is dict else event) not in terminal
        if case.startswith('late_restart_'):
            assert result['mutex_absent'] is True
    for slot, process in slots.items():
        if slot == 'holder' and case in CRASH_CASES:
            continue
        assert process['actual_exit'] == 0 and process['timed_out'] is False
        assert process['validated'] is True and process['report_errors'] == []
        assert process['payload'] is not None
    if case == 'serialize_updates':
        assert_artifact(root / 'shared/current.csv', 'two_updates')
        frames = slots['contender']['frames']
        assert any(frame['phase'] == 'CONTENDED' for frame in frames)
        admitted = slots['contender']['payload']['receipt']
        assert admitted['lease_entry_observation']['sha256'] == oracle['replacement']['sha256']
    elif case == 'independent_directories':
        assert_artifact(root / 'a/current.csv', 'replacement')
        assert_artifact(root / 'b/current.csv', 'replacement')
        names = [slots[slot]['payload']['mutex_name'] for slot in ('a', 'b')]
        assert names[0] != names[1]
        assert (root / 'foreign-sentinel').read_bytes() == b'D1_FOREIGN_SENTINEL\n'
    elif case == 'same_directory_siblings':
        assert_artifact(root / 'shared/A.csv', 'replacement')
        assert_artifact(root / 'shared/B.csv', 'previous')
        assert slots['holder']['payload']['mutex_name'] == slots['contender']['payload']['mutex_name']
        assert slots['contender']['payload']['status'] == 'DENIED'
    elif case == 'directory_target_switch':
        assert_artifact(root / 'shared/A.csv', 'previous')
        assert (root / 'shared/B.csv').read_bytes() == b'D1_INVALID_CURRENT\n'
        rejected = slots['next']['payload']
        assert rejected['status'] == 'REJECTED' and rejected['validator_calls'] == 1
        assert rejected['receipt']['target_validation'] == 'REJECTED'
        assert rejected['receipt']['commit_state'] == 'NOT_ATTEMPTED'
    elif case.startswith('retry_'):
        assert_artifact(root / 'shared/current.csv', 'previous')
        assert slots['worker']['payload']['validator_calls'] == 2
        assert slots['worker']['payload']['status'] == 'REJECTED'
    elif case == 'late_restart_invalid_before_commit':
        assert (root / 'shared/current.csv').read_bytes() == b'D1_INVALID_CURRENT\n'
        survivor = slots['restart']['payload']
        assert survivor['validator_calls'] == 1 and survivor['status'] == 'REJECTED'
        assert survivor['receipt']['lock_observation'] == 'NORMAL'
        assert survivor['receipt']['target_validation'] == 'REJECTED'
        assert survivor['receipt']['prepared_candidate'] is None
        assert survivor['receipt']['commit_state'] == 'NOT_ATTEMPTED'
        assert list((root / 'shared').glob('.d1-*.tmp'))
    elif case in ('abandoned_before_commit', 'late_restart_before_commit'):
        assert_artifact(root / 'shared/current.csv', 'previous')
        survivor = slots['recovery' if case.startswith('abandoned') else 'restart']['payload']
        assert survivor['validator_calls'] == 1
        assert survivor['receipt']['lock_observation'] == ('ABANDONED' if case.startswith('abandoned') else 'NORMAL')
        assert list((root / 'shared').glob('.d1-*.tmp'))
    else:
        assert_artifact(root / 'shared/current.csv', 'replacement')
        if case in ('deny_second', 'bounded_wait'):
            assert slots['contender']['payload']['status'] == 'DENIED'
            assert slots['contender']['payload']['validator_calls'] == 0
            assert slots['holder']['payload']['mutex_name'] == slots['contender']['payload']['mutex_name']
            assert slots['holder']['frames'][0]['pid'] != slots['contender']['frames'][0]['pid']
        elif case in ('crash_after_commit', 'late_restart_after_commit'):
            checkpoint = next(frame for frame in slots['holder']['frames'] if frame['phase'] == 'POSTCOMMIT_READY')
            assert checkpoint['payload']['receipt']['commit_state'] == 'REPLACED'
            survivor = slots['recovery' if case.startswith('crash') else 'restart']['payload']
            assert survivor['receipt']['lease_entry_observation']['sha256'] == oracle['replacement']['sha256']
            assert survivor['receipt']['lock_observation'] == ('ABANDONED' if case.startswith('crash') else 'NORMAL')
        elif case == 'reader_visibility':
            frames = slots['reader']['frames']
            assert [frame['phase'] for frame in frames] == ['GUARD_READY', 'ARMED', 'TARGET_READY', 'RESULT']
            observations = [(frame['phase'], frame['payload']['observed_sha256'], frame['payload']['observed_bytes'])
                for frame in frames if frame['phase'] in ('TARGET_READY', 'RESULT')]
            assert observations == [('TARGET_READY', oracle['previous']['sha256'], oracle['previous']['bytes']),
                ('RESULT', oracle['replacement']['sha256'], oracle['replacement']['bytes'])]
            assert slots['reader']['payload'] == frames[-1]['payload']
    canaries = ('D1_WORKER_TOKEN_b328', 'D1_WORKER_COOKIE_4b6e', 'D1_WORKER_KEY_95de')
    serialized = json.dumps({key: value for key, value in result.items() if key != 'case_root'})
    logging_root = project_root / '.f-log/d1' / offline_guard.ensure_context(project_root).run_id / case
    for path in logging_root.rglob('*'):
        if path.is_file():
            serialized += path.read_bytes().decode('utf-8', errors='replace')
    assert all(value not in serialized for value in canaries)


@pytest.mark.parametrize('answer', [False, None, 1, 'verified'])
def test_admission_requires_exact_true(answer, acquire, target, assert_artifact):
    calls, receipt = [], {}
    with pytest.raises(ValueError, match='D1_RECOVERY_REQUIRED'):
        with acquire(validate=lambda stream: calls.append(stream.read()) or answer, receipt=receipt):
            pytest.fail('invalid target issued lease')
    assert len(calls) == 1
    assert_artifact(target, 'previous')
    assert receipt['target_validation'] == 'REJECTED'
    assert receipt['prepared_candidate'] is None


@pytest.mark.parametrize('allow', [True, False, None, 1])
def test_absent_requires_explicit_exact_true(allow, helper, target, logging_session, oracle):
    target.unlink()
    calls, receipt = [], {}
    def validate(stream):
        calls.append(stream)
        return allow
    context = helper.resource_lock(target.name, resource_root=target.parent, validate_target=validate,
        logging_session=logging_session, receipt=receipt)
    if allow is True:
        with context as lease:
            assert receipt['lease_entry_observation'] == {'exists': False, 'sha256': None, 'bytes': None}
            helper.atomic_write_file(lease, lambda stream: stream.write(oracle['replacement']['data']), lambda stream: True)
        assert target.read_bytes() == oracle['replacement']['data']
    else:
        with pytest.raises(ValueError, match='D1_RECOVERY_REQUIRED'):
            with context:
                pytest.fail('unapproved absent target issued lease')
        assert not target.exists() and list(target.parent.glob('.d1-*.tmp')) == []
    assert calls == [None]


def test_each_new_acquisition_revalidates_and_preserves_original_exception(acquire, target, assert_artifact):
    error = RuntimeError('fixed target validation failure')
    calls = []
    def validate(stream):
        assert stream.tell() == 0 and not stream.writable()
        calls.append(stream.read())
        raise error
    for attempt in range(2):
        receipt = {}
        with pytest.raises(RuntimeError) as caught:
            with acquire(validate=validate, receipt=receipt):
                pytest.fail('raising validator issued lease')
        assert caught.value is error
        assert receipt['target_validation'] == 'ERROR'
        assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert len(calls) == 2
    assert_artifact(target, 'previous')


def test_target_changed_during_admission_is_not_absent(acquire, target):
    def validate(stream):
        target.write_bytes(b'changed while validating')
        return True
    receipt = {}
    with pytest.raises(Exception):
        with acquire(validate=validate, receipt=receipt):
            pytest.fail('changed target issued lease')
    assert receipt['target_state'] in ('CHANGED', 'UNKNOWN')
    assert receipt['prepared_candidate'] is None
    assert target.read_bytes() == b'changed while validating'


def test_import_has_no_backend_or_logger_side_effects(helper, monkeypatch):
    import run_logging
    def forbidden(*args, **kwargs):
        raise AssertionError('import activated a backend or logger')
    monkeypatch.setattr(ctypes, 'WinDLL', forbidden)
    monkeypatch.setattr(run_logging, 'configure_logging', forbidden)
    importlib.reload(helper)


def test_unsupported_platform_refuses_before_backend_or_callback(helper, acquire, target, assert_artifact, monkeypatch):
    calls = []
    monkeypatch.setattr(helper, 'sys', SimpleNamespace(platform='unsupported-d1'))
    monkeypatch.setattr(helper, '_windows_backend', lambda: calls.append('backend'))
    with pytest.raises(OSError, match='D1_UNSUPPORTED_PLATFORM'):
        with acquire(validate=lambda stream: calls.append('validator')):
            pytest.fail('unsupported platform issued lease')
    assert calls == []
    assert_artifact(target, 'previous')


@pytest.mark.parametrize('path', ['../current.csv', '/current.csv', 'C:\\current.csv', 'current.csv:stream',
    'NUL', 'COM1.csv', 'current.csv.', 'current.csv ', 'missing/current.csv', '\\\\server\\share\\x.csv', 'CURRENT~1.CSV'])
def test_invalid_paths_refuse_before_callbacks(path, helper, target, logging_session, assert_artifact):
    calls, receipt = [], {}
    with pytest.raises(Exception):
        with helper.resource_lock(path, resource_root=target.parent, validate_target=lambda stream: calls.append(1),
                logging_session=logging_session, receipt=receipt):
            pytest.fail('unsafe path issued lease')
    assert calls == []
    assert_artifact(target, 'previous')


@pytest.mark.parametrize('alias', ['hardlink', 'reparse', 'symlink'])
def test_unsafe_target_stat_refuses_before_callbacks(alias, helper, acquire, target, assert_artifact, monkeypatch):
    original = helper.os.lstat
    def unsafe(path, *args, **kwargs):
        details = original(path, *args, **kwargs)
        if os.path.normcase(os.path.abspath(path)) != os.path.normcase(str(target)):
            return details
        value = SimpleNamespace(**{name: getattr(details, name) for name in dir(details) if name.startswith('st_')})
        if alias == 'hardlink':
            value.st_nlink = 2
        elif alias == 'reparse':
            value.st_file_attributes = getattr(value, 'st_file_attributes', 0) | 0x400
        else:
            value.st_mode = stat.S_IFLNK | stat.S_IMODE(value.st_mode)
        return value
    monkeypatch.setattr(helper.os, 'lstat', unsafe)
    calls, receipt = [], {}
    with pytest.raises(ValueError):
        with acquire(validate=lambda stream: calls.append('validator') or True, receipt=receipt):
            pytest.fail('unsafe target issued lease')
    assert calls == []
    assert receipt['prepared_candidate'] is None
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    monkeypatch.undo()
    assert_artifact(target, 'previous')


@pytest.mark.parametrize('timeout', [-1, 61, float('nan'), float('inf'), True, '1'])
def test_invalid_timeout_refuses_before_native(timeout, helper, target, logging_session, monkeypatch):
    calls = []
    monkeypatch.setattr(helper, '_windows_backend', lambda: calls.append('backend'))
    with pytest.raises(ValueError):
        with helper.resource_lock(target.name, resource_root=target.parent, validate_target=lambda stream: calls.append('validate'),
                logging_session=logging_session, receipt={}, timeout_s=timeout):
            pytest.fail('invalid timeout issued lease')
    assert calls == []


def test_lease_is_single_use_thread_bound_and_expires(helper, acquire, oracle):
    receipt = {}
    with acquire(receipt=receipt) as lease:
        errors = []
        def wrong_thread():
            try:
                helper.atomic_write_file(lease, lambda stream: None, lambda stream: True)
            except ValueError:
                errors.append('refused')
        thread = threading.Thread(target=wrong_thread)
        thread.start()
        thread.join(1)
        assert not thread.is_alive() and errors == ['refused']
        with pytest.raises(ValueError, match='D1_REENTRANT_LOCK'):
            with acquire():
                pytest.fail('recursive lease issued')
        helper.atomic_write_file(lease, lambda stream: stream.write(oracle['replacement']['data']), lambda stream: True)
        with pytest.raises(ValueError, match='D1_LEASE_ALREADY_USED'):
            helper.atomic_write_file(lease, lambda stream: None, lambda stream: True)
    for invalid in (lease, object(), {}, receipt):
        with pytest.raises(ValueError, match='D1_LEASE_INVALID'):
            helper.atomic_write_file(invalid, lambda stream: None, lambda stream: True)


def test_worker_logging_context_refused_without_parent_health(helper, target, logging_session, monkeypatch, assert_artifact):
    import run_logging
    worker = run_logging.configure_worker(run_logging.prepare_worker(logging_session, 'd1-unsupported-worker'))
    calls = []
    monkeypatch.setattr(run_logging, 'check_logging_health', lambda session: calls.append('health'))
    monkeypatch.setattr(helper, '_windows_backend', lambda: calls.append('backend'))
    with pytest.raises(ValueError, match='D1_PARENT_LOGGING_SESSION_REQUIRED'):
        with helper.resource_lock(target.name, resource_root=target.parent, validate_target=lambda stream: calls.append('validator'),
                logging_session=worker, receipt={}):
            pytest.fail('worker context issued lease')
    assert calls == []
    assert_artifact(target, 'previous')
    monkeypatch.undo()
    run_logging.finish_worker(worker, execution_outcome='FAILED', exit_status=1)
    run_logging.record_worker_outcome(logging_session, 'd1-unsupported-worker', started=False, exit_status=None)
    summary = run_logging.finalize_logging(logging_session, execution_outcome='FAILED', exit_status=1)
    assert summary['exit_status'] == 1
    assert summary['execution_outcome'] == 'FAILED'


def test_exact_name_at_native_boundary_fixed_vector(helper, acquire, oracle, monkeypatch):
    expected = 'Global\\MDS-D1-v1-' + hashlib.sha256(bytes.fromhex(oracle['name_oracle']['preimage_hex'])).hexdigest()
    backend = helper._windows_backend()
    trace = []
    def create(attributes, owner, name):
        trace.append((attributes, owner, name))
        return 0xd101
    backend['CreateMutexW'] = create
    backend['WaitForSingleObject'] = lambda handle, milliseconds: 0 if handle == 0xd101 else pytest.fail('foreign handle')
    backend['ReleaseMutex'] = lambda handle: handle == 0xd101
    backend['CloseHandle'] = lambda handle: handle == 0xd101
    monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    monkeypatch.setattr(helper, '_parent_identity', lambda path, backend: (0x0102030405060708, bytes(range(16))))
    with acquire():
        pass
    assert trace == [(None, False, expected)]


def test_same_directory_representations_and_siblings_share_exact_name(helper, acquire, target, monkeypatch):
    backend = helper._windows_backend()
    original = backend['CreateMutexW']
    names = []
    backend['CreateMutexW'] = lambda attributes, owner, name: names.append(name) or original(attributes, owner, name)
    monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    sibling = target.parent / 'sibling.csv'
    sibling.write_bytes(target.read_bytes())
    roots = [str(target.parent), str(target.parent).replace('\\', '/'), str(target.parent) + '\\',
        str(target.parent)[0].swapcase() + str(target.parent)[1:]]
    for root in roots:
        with acquire(root=root):
            pass
    with acquire(path=sibling.name):
        pass
    assert len(names) == 5 and len(set(names)) == 1


@pytest.mark.parametrize('fault', ['identity', 'namespace', 'access', 'open', 'wait_failed', 'wait_unexpected'])
def test_native_failures_never_fallback_or_issue_lease(fault, helper, acquire, target, assert_artifact, monkeypatch):
    backend = helper._windows_backend()
    trace, callbacks = [], []
    real_create = backend['CreateMutexW']
    def create(attributes, owner, name):
        trace.append(name)
        if fault in ('namespace', 'access', 'open'):
            backend['ctypes'].set_last_error(5 if fault == 'access' else 123)
            return None
        return real_create(attributes, owner, name)
    backend['CreateMutexW'] = create
    if fault == 'identity':
        backend['GetFileInformationByHandleEx'] = lambda *args: False
    elif fault in ('wait_failed', 'wait_unexpected'):
        backend['WaitForSingleObject'] = lambda *args: 0xffffffff if fault == 'wait_failed' else 0x51
    monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    monkeypatch.setattr(helper.os, 'replace', lambda *args: callbacks.append('replace'))
    with pytest.raises(Exception):
        with acquire(validate=lambda stream: callbacks.append('target') or True) as lease:
            helper.atomic_write_file(lease, lambda stream: callbacks.append('prepare'), lambda stream: callbacks.append('candidate') or True)
    assert callbacks == []
    assert len(trace) == (0 if fault == 'identity' else 1)
    assert all(name.startswith('Global\\MDS-D1-v1-') for name in trace)
    assert_artifact(target, 'previous')


@pytest.mark.parametrize('mutation', ['local', 'unprefixed', 'pid', 'session', 'salt', 'python_hash'])
def test_independent_exact_name_oracle_rejects_negative_vectors(mutation, oracle, helper, acquire, target, assert_artifact, monkeypatch):
    preimage = bytes.fromhex(oracle['name_oracle']['preimage_hex'])
    digest = hashlib.sha256(preimage).hexdigest()
    expected = 'Global\\MDS-D1-v1-' + digest
    names = {'local': 'Local\\MDS-D1-v1-' + digest, 'unprefixed': 'MDS-D1-v1-' + digest,
        'pid': 'Global\\MDS-D1-v1-' + hashlib.sha256(preimage + b'pid=1234').hexdigest(),
        'session': 'Global\\MDS-D1-v1-' + hashlib.sha256(preimage + b'session=1').hexdigest(),
        'salt': 'Global\\MDS-D1-v1-' + hashlib.sha256(preimage + b'salt=fixture').hexdigest(),
        'python_hash': 'Global\\MDS-D1-v1-' + format(abs(hash(preimage)), '064x')}
    native_calls = []
    def independent_boundary(attributes, owner, name):
        assert name == expected
        native_calls.append(name)
        raise AssertionError('negative control reached a native call')
    backend = helper._windows_backend()
    backend['CreateMutexW'] = independent_boundary
    monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    monkeypatch.setattr(helper, '_parent_identity', lambda path, backend: (0x0102030405060708, bytes(range(16))))
    monkeypatch.setattr(helper, '_mutex_name', lambda identity: names[mutation])
    with pytest.raises(AssertionError):
        with acquire():
            pytest.fail('mutated name issued a lease')
    assert native_calls == []
    assert_artifact(target, 'previous')
