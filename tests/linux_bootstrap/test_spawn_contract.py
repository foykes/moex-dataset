"""Native oracles: refusing Windows is not Linux runtime acceptance."""

import errno
import importlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

import pytest


pytestmark = pytest.mark.native
CASES = (
    'success', 'child_error', 'exit23', 'hang',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'bootstrap_pipe_failure', 'foreign_fd',
    'listener_failure', 'ack_failure', 'auxiliary_settlement',
    'canary_positive_control',
)
ATTESTED_SOURCES = {
    'context.py', 'reduction.py', 'queues.py', 'synchronize.py',
    'sharedctypes.py', 'heap.py', 'resource_tracker.py', 'spawn.py',
    'popen_spawn_posix.py', 'util.py',
    'connection.py',
}
CASE_FIELDS = {
    'schema', 'case', 'status', 'failure_code', 'application_exit',
    'deadline_seconds', 'deadline_detected', 'cleanup_requested',
    'all_workers_settled', 'two_workers_registered', 'worker_records',
    'logging', 'auxiliary', 'source_attestation', 'console_canary_absent',
}
WORKER_FIELDS = {
    'role', 'pid', 'pidfd_bound', 'session_owned', 'exitcode', 'reaped',
    'events', 'foreign_fd_absent', 'stdio_redirected',
}


@pytest.fixture
def native_guard(repo):
    # An accidental local selection fails instead of silently becoming skip/PASS.
    assert sys.platform == 'linux', 'native Linux acceptance is NOT VERIFIED on this platform'
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    module = importlib.import_module('tools.offline_guard_linux')
    assert tuple(module.NATIVE_CASES) == CASES
    return module


def before(events, first, second):
    assert first in events
    assert second in events
    assert events.index(first) < events.index(second)


@pytest.fixture
def native_scratch(native_results):
    # The real consumer supplies this root inside its own disposable fixture
    # tree. Do not borrow the repository, another checkout, or a mutable venv.
    configured = os.environ.get('LNX03_NATIVE_ROOT')
    assert configured, 'missing admitted native fixture root'
    parent = Path(configured)
    assert parent.is_absolute() and parent.is_dir() and not parent.is_symlink()
    root = parent / ('case-' + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    previous = len(native_results)
    yield root
    # An unknown process lifetime preserves its fixture for outer fail-closed
    # cleanup. Only actual settlement permits this independent fixture cleanup.
    if (len(native_results) > previous and native_results[-1]['all_workers_settled'] is True
            and native_results[-1]['auxiliary']['settled'] is True):
        assert root.resolve().parent == parent.resolve()
        shutil.rmtree(root)
        assert not root.exists()


@pytest.mark.parametrize('case', CASES)
def test_native_process_logging_and_bootstrap_oracles(
        native_guard, repo, native_scratch, native_results, case):
    started = time.monotonic()
    record = native_guard.run_native_case(case, repo, native_scratch)
    elapsed = time.monotonic() - started
    native_results.append(record)
    assert set(record) == CASE_FIELDS
    assert record['schema'] == 'lnx03-native-case-v1'
    assert record['case'] == case
    # A successful negative test retains the application's actual failed exit.
    assert record['status'] == 'PASS'
    assert record['failure_code'] is None
    assert type(record['application_exit']) is int
    assert record['deadline_seconds'] == 15
    assert record['all_workers_settled'] is True
    assert record['two_workers_registered'] is True
    assert record['console_canary_absent'] is True
    assert set(record['source_attestation']) == ATTESTED_SOURCES
    assert all(re.fullmatch(r'[0-9a-f]{64}', value)
               for value in record['source_attestation'].values())

    workers = record['worker_records']
    assert len(workers) == 2
    assert {worker['role'] for worker in workers} == {'google', 'ftp'}
    assert len({worker['pid'] for worker in workers}) == 2
    for worker in workers:
        assert set(worker) == WORKER_FIELDS
        assert type(worker['pid']) is int and worker['pid'] > 0
        assert worker['pidfd_bound'] is True
        assert worker['session_owned'] is True
        assert worker['reaped'] is True
        assert type(worker['exitcode']) is int
        assert worker['foreign_fd_absent'] is True
        assert worker['stdio_redirected'] is True
        assert 'GUARD_DENIALS_CONFIRMED' in worker['events']
        for boundary in ('PREPARATION_BOUNDARY', 'BOOTSTRAP_IMPORT_BOUNDARY', 'UNPICKLE_BOUNDARY'):
            if boundary in worker['events']:
                before(worker['events'], 'GUARD_DENIALS_CONFIRMED', boundary)

    auxiliary = record['auxiliary']
    assert set(auxiliary) == {'owned', 'pid', 'pidfd_bound', 'settled', 'reaped', 'exitcode'}
    assert all(auxiliary[key] is True for key in ('owned', 'pidfd_bound', 'settled', 'reaped'))
    assert type(auxiliary['pid']) is int and auxiliary['pid'] > 0
    assert type(auxiliary['exitcode']) is int and auxiliary['exitcode'] == 0
    assert auxiliary['pid'] not in {worker['pid'] for worker in workers}

    logging = record['logging']
    assert set(logging) == {'delivery_outcome', 'storage_sealed', 'exit_status', 'worker_final_count'}
    assert type(logging['storage_sealed']) is bool
    assert type(logging['exit_status']) is int
    assert type(logging['worker_final_count']) is int
    assert 0 <= logging['worker_final_count'] <= 2
    if case in {'success', 'foreign_fd', 'auxiliary_settlement'}:
        assert record['application_exit'] == 0
        assert all(worker['exitcode'] == 0 for worker in workers)
        assert logging['delivery_outcome'] == 'COMPLETE'
        assert logging['storage_sealed'] is True
        assert logging['exit_status'] == 0
        assert logging['worker_final_count'] == 2
        for worker in workers:
            before(worker['events'], 'PREPARATION_OK', 'UNPICKLE_BOUNDARY')
            before(worker['events'], 'UNPICKLE_OK', 'WORKER_WRAPPER')
            before(worker['events'], 'WORKER_WRAPPER', 'FINAL_CONFIRMED')
    else:
        assert record['application_exit'] == 1

    if case == 'exit23':
        failed = [worker for worker in workers if worker['exitcode'] == 23]
        assert len(failed) == 1
        events = failed[0]['events']
        before(events, 'WORKER_WRAPPER', 'EXIT23_FLUSH_CONFIRMED')
        before(events, 'EXIT23_FLUSH_CONFIRMED', 'EXIT23')
        assert 'FINAL_CONFIRMED' not in events and 'FINAL_UNCONFIRMED' not in events
        assert logging['worker_final_count'] < 2
    elif case == 'foreign_fd':
        assert any('FOREIGN_FD_REFUSED' in worker['events'] for worker in workers)
    elif case == 'child_error':
        assert any(worker['exitcode'] != 0 and 'WORKER_ERROR' in worker['events'] for worker in workers)
    elif case == 'hang':
        assert record['deadline_detected'] is True
        assert record['cleanup_requested'] is True
        assert all('HANG' in worker['events'] and worker['exitcode'] != 0 for worker in workers)
        assert elapsed >= 15
    elif case in {'bootstrap_replay_failure', 'bootstrap_import_failure', 'bootstrap_unpickle_failure'}:
        boundary = {
            'bootstrap_replay_failure': 'PREPARATION_BOUNDARY',
            'bootstrap_import_failure': 'BOOTSTRAP_IMPORT_BOUNDARY',
            'bootstrap_unpickle_failure': 'UNPICKLE_BOUNDARY',
        }[case]
        failing = [worker for worker in workers if worker['exitcode'] != 0]
        assert failing
        assert any(boundary in worker['events'] for worker in failing)
        assert all('WORKER_WRAPPER' not in worker['events'] for worker in failing)
        assert logging['worker_final_count'] < 2
    elif case == 'bootstrap_pipe_failure':
        assert record['cleanup_requested'] is True
        assert any('BOOTSTRAP_PIPE_WRITE_REFUSED' in worker['events'] for worker in workers)
        assert any(worker['exitcode'] != 0 for worker in workers)
    elif case in {'listener_failure', 'ack_failure'}:
        assert any('WORKER_WRAPPER' in worker['events'] for worker in workers)
        assert logging['delivery_outcome'] != 'COMPLETE'
        assert logging['exit_status'] != 0
    elif case == 'canary_positive_control':
        # The native implementation must prove the scanner rejected its positive
        # control, not merely report that redirected stdout was empty.
        assert any('CANARY_REJECTED' in worker['events'] for worker in workers)


def _native_fd_inventory():
    """Observe every live FD identity after listdir's own directory FD closes."""
    result = {}
    for name in os.listdir('/proc/self/fd'):
        descriptor = int(name)
        try:
            info = os.fstat(descriptor)
        except OSError as error:
            if error.errno != errno.EBADF:
                raise
            continue
        result[descriptor] = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
    return result


def _native_assert_reaped(process):
    assert type(process.returncode) is int
    # WNOWAIT never competes with Popen's consuming reap. ECHILD proves that
    # the test did not need to repair an unreaped child to obtain its result.
    with pytest.raises(ChildProcessError):
        os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)


@pytest.mark.parametrize('fault', [
    'google_launch_refused', 'ftp_launch_refused', 'ftp_ticket_refused',
    'google_pidfd_refused', 'google_identity_refused', 'google_registry_refused',
    'google_registration_refused', 'ftp_registration_refused',
])
def test_native_partial_launch_preserves_real_outcomes_and_owned_fds(
        native_guard, repo, monkeypatch, fault):
    """Additional native faults are not replacements for the original 13 cases.

    Every observer delegates to real Popen/F-LOG/OS operations. Constructor
    uncertainty is a separate portable oracle: these pre-birth refusals occur
    before entering Popen, so absence of a birth is independently established.
    """
    import run_logging

    configured = os.environ.get('LNX03_NATIVE_ROOT')
    assert configured
    parent = Path(configured)
    assert parent.is_absolute() and parent.resolve() == parent and parent.is_dir()
    root = parent / ('fault-' + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    state = dict(armed=False, reached=False, active_role=None, active_owner=None,
                 births=[], owners={}, publications=[], outcomes=[], reports=[])
    processes = []
    foreign = None
    foreign_identity = None
    verified = False
    real_launch = native_guard._launch
    real_popen = native_guard.subprocess.Popen
    real_pidfd = native_guard.os.pidfd_open
    real_identity = native_guard._process_identity
    real_ticket = native_guard._ticket
    real_registry = native_guard._registry
    real_prepare = run_logging.prepare_worker
    real_outcome = run_logging.record_worker_outcome
    real_finalize = run_logging.finalize_logging

    def ticket_role(path):
        role = Path(path).stem
        assert role in {'tracker', 'google', 'ftp'}
        assert Path(path).is_relative_to(root)
        return role

    def observe_popen(command, *args, **kwargs):
        assert tuple(command)[-2] == '--ticket'
        assert Path(command[-3]) == repo / 'tests/linux_bootstrap/native_worker.py'
        role = ticket_role(command[-1])
        process = real_popen(command, *args, **kwargs)
        processes.append(process)
        if state['armed']:
            state['births'].append((role, process))
        return process

    def observe_launch(actual_repo, ticket, descriptors, *, owner=None):
        role = ticket_role(ticket)
        previous = state['active_role'], state['active_owner']
        state['active_role'], state['active_owner'] = role, owner
        try:
            if state['armed'] and fault == role + '_launch_refused':
                state['reached'] = True
                # No constructor call, native birth, or fabricated exit.
                raise native_guard.NativeAdmissionError('LNX03_TEST_BEFORE_BIRTH')
            return real_launch(actual_repo, ticket, descriptors, owner=owner)
        finally:
            if state['armed']:
                assert type(owner) is dict
                state['owners'][role] = {
                    key: owner.get(key) for key in ('process', 'pidfd', 'identity', 'birth_attempted')}
            state['active_role'], state['active_owner'] = previous

    def observe_pidfd(pid, flags=0):
        if state['armed'] and state['active_role'] in {'tracker', 'google', 'ftp'}:
            owner = state['active_owner']
            process = next(process for role, process in state['births'] if process.pid == pid)
            published = type(owner) is dict and owner.get('process') is process
            state['publications'].append((state['active_role'], published))
            assert published, 'Popen ownership must precede pidfd binding'
            if fault == state['active_role'] + '_pidfd_refused':
                state['reached'] = True
                raise PermissionError('LNX03_TEST_PIDFD_REFUSED')
        return real_pidfd(pid, flags)

    def observe_identity(pid):
        if (state['armed'] and state['active_role'] is not None
                and fault == state['active_role'] + '_identity_refused'):
            assert state['active_owner']['process'].pid == pid
            state['reached'] = True
            raise OSError('LNX03_TEST_IDENTITY_REFUSED')
        return real_identity(pid)

    def observe_ticket(path, value):
        if state['armed'] and fault == value['role'] + '_ticket_refused':
            assert any(role == 'google' for role, process in state['births'])
            assert not any(role == 'ftp' for role, process in state['births'])
            state['reached'] = True
            raise OSError('LNX03_TEST_TICKET_REFUSED')
        return real_ticket(path, value)

    def observe_registry(work, children, tracker):
        if (state['armed'] and fault == 'google_registry_refused'
                and any(item.get('role') == 'google' and item.get('process') is not None
                        for item in children)):
            state['reached'] = True
            raise OSError('LNX03_TEST_REGISTRY_REFUSED')
        return real_registry(work, children, tracker)

    def observe_prepare(session, role):
        if state['armed'] and fault == role + '_registration_refused':
            state['reached'] = True
            raise OSError('LNX03_TEST_REGISTRATION_REFUSED')
        return real_prepare(session, role)

    def observe_outcome(session, role, **kwargs):
        result = real_outcome(session, role, **kwargs)
        if state['armed']:
            # Primitive snapshots only: retaining session/Queue references would
            # itself prevent real semaphore cleanup before tracker EOF.
            state['outcomes'].append((role, dict(session['_outcomes'][role])))
        return result

    def observe_finalize(session, **kwargs):
        expected = set(session['_expected'])
        report = real_finalize(session, **kwargs)
        if state['armed']:
            state['reports'].append((expected, report))
        return report

    monkeypatch.setattr(native_guard.subprocess, 'Popen', observe_popen)
    monkeypatch.setattr(native_guard, '_launch', observe_launch)
    monkeypatch.setattr(native_guard.os, 'pidfd_open', observe_pidfd)
    monkeypatch.setattr(native_guard, '_process_identity', observe_identity)
    monkeypatch.setattr(native_guard, '_ticket', observe_ticket)
    monkeypatch.setattr(native_guard, '_registry', observe_registry)
    monkeypatch.setattr(run_logging, 'prepare_worker', observe_prepare)
    monkeypatch.setattr(run_logging, 'record_worker_outcome', observe_outcome)
    monkeypatch.setattr(run_logging, 'finalize_logging', observe_finalize)
    try:
        # RawValue's attested global Arena cache legitimately keeps backing FDs.
        # Warm it with actual successful transport before the complete FD oracle.
        warm = native_guard.run_native_case('success', repo, root)
        assert warm['status'] == 'PASS' and warm['application_exit'] == 0
        for process in processes:
            _native_assert_reaped(process)
        foreign = os.open(root / 'unrelated-fd', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        info = os.fstat(foreign)
        foreign_identity = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
        before_fds = _native_fd_inventory()
        assert before_fds[foreign] == foreign_identity
        state['armed'] = True
        record = native_guard.run_native_case('success', repo, root)
        assert state['reached'], 'the selected fault boundary was not reached'
        assert set(record) == CASE_FIELDS
        assert record['status'] in {'FAIL', 'INCOMPLETE'}
        assert record['application_exit'] == 1
        assert record['failure_code'] is not None

        expected = {'google', 'ftp'}
        if fault == 'google_registration_refused':
            expected = set()
        elif fault == 'ftp_registration_refused':
            expected = {'google'}
        assert len(state['reports']) == 1
        registered, report = state['reports'][0]
        assert registered == expected
        assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
        assert set(report['worker_outcomes']) == expected
        assert record['two_workers_registered'] is (expected == {'google', 'ftp'})

        born = {role: process for role, process in state['births'] if role != 'tracker'}
        expected_born = set() if fault in {
            'google_launch_refused', 'google_registration_refused', 'ftp_registration_refused'} else {'google'}
        assert set(born) == expected_born
        for process in processes:
            _native_assert_reaped(process)
        assert all(published for role, published in state['publications'])
        for role in expected:
            outcome = report['worker_outcomes'][role]
            assert outcome['started'] is (role in born)
            assert outcome['exit_status'] == (born[role].returncode if role in born else None)
        assert {role for role, outcome in state['outcomes']} == expected
        for role, outcome in state['outcomes']:
            assert outcome['started'] is (role in born)
            assert outcome['exit_status'] == (born[role].returncode if role in born else None)
        for role, owner in state['owners'].items():
            if role in born:
                assert owner['process'] is born[role] and owner['birth_attempted'] is True
            elif fault == role + '_launch_refused':
                assert owner['process'] is None and owner['birth_attempted'] is False

        workers = {worker['role']: worker for worker in record['worker_records']}
        assert set(workers) == set(born)
        for role, worker in workers.items():
            assert set(worker) == WORKER_FIELDS
            assert worker['pid'] == born[role].pid
            assert worker['exitcode'] == born[role].returncode and worker['reaped'] is True
            if fault == 'google_pidfd_refused':
                assert worker['pidfd_bound'] is False
        actual_finals = {row['worker_id']: row['final_report']
                         for row in report['expected_producers'] if row['worker_id'] != 'parent'}
        for role in expected - set(born):
            assert actual_finals[role] is None
        if fault in {'google_pidfd_refused', 'google_identity_refused', 'google_registry_refused'}:
            assert actual_finals['google'] is None
            assert 'WORKER_WRAPPER' not in workers['google']['events']
        assert record['logging'] == {
            'delivery_outcome': report['delivery_outcome'], 'storage_sealed': report['storage_sealed'],
            'exit_status': report['exit_status'],
            'worker_final_count': sum(value is not None for value in actual_finals.values()),
        }
        tracker = [process for role, process in state['births'] if role == 'tracker']
        assert len(tracker) == 1
        assert record['auxiliary']['pid'] == tracker[0].pid
        assert record['auxiliary']['settled'] is True and record['auxiliary']['reaped'] is True
        assert record['auxiliary']['exitcode'] == tracker[0].returncode == 0
        assert record['all_workers_settled'] is True
        assert _native_fd_inventory() == before_fds
        assert _native_fd_inventory()[foreign] == foreign_identity
        verified = True
    finally:
        # Independent fixture safety after a failed assertion; this cannot make
        # the earlier settlement/FD assertions pass or authorize another case.
        deadline = time.monotonic() + 5
        for process in processes:
            if process.poll() is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
        for process in processes:
            if process.poll() is None:
                try:
                    process.wait(timeout=min(1, max(0, deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
        for process in processes:
            if process.poll() is None:
                try:
                    process.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
        if foreign is not None and foreign_identity is not None:
            try:
                info = os.fstat(foreign)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise
            else:
                # A failed subject may have closed/reused the integer. Never
                # close another owner's replacement while cleaning this test.
                current = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
                if current == foreign_identity:
                    os.close(foreign)
        if verified and all(process.returncode is not None for process in processes):
            assert root.resolve().parent == parent
            shutil.rmtree(root)
