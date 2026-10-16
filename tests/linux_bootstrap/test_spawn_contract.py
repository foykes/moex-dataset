"""Native oracles: refusing Windows is not Linux runtime acceptance."""

import errno
import hashlib
import importlib
import json
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


def test_native_real_constructor_error_retains_unknown_until_fixture_exit(
        native_guard, repo, monkeypatch, request):
    """A real failed constructor is not a missing-object no-birth proof.

    This must be the final collected test. The supervisor's conservative marker
    stays installed, and a subsequent API call must refuse. The independent
    observers establish the failed constructor's actual internal child reap;
    they never supply that PID or exit to the supervisor's worker ledger.
    """
    import run_logging

    assert request.session.items[-1] is request.node
    assert native_guard._unsettled_transports == []
    subprocess_source = Path(subprocess.__file__).read_bytes().replace(b'\r\n', b'\n')
    assert hashlib.sha256(subprocess_source).hexdigest() == (
        '7fa4af83422024a19e64a80937f6c8094244c3397372a495b348dfbe44a5dd72')
    assert type(subprocess.Popen) is type and subprocess.Popen.__module__ == 'subprocess'
    assert subprocess._fork_exec.__module__ == '_posixsubprocess'
    assert os.waitpid.__module__ == 'posix' and os.close.__module__ == 'posix'
    configured = os.environ.get('LNX03_NATIVE_ROOT')
    assert configured
    parent = Path(configured)
    assert parent.is_absolute() and parent.resolve() == parent and parent.is_dir()
    root = parent / ('constructor-' + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    missing_cwd = root / 'missing-ftp-cwd'
    assert not missing_cwd.exists()
    state = dict(armed=False, active_role=None, active_owner=None,
                 constructor_active=False, entered=False, raised=False,
                 google_identity=None, ftp_fork_pid=None, error_errno=None,
                 trace=[], births={}, owners={}, forks=[], waits=[],
                 outcomes=[], reports=[], closes=[], close_errors=[])
    processes = []
    foreign = None
    foreign_identity = None
    verified = False
    real_launch = native_guard._launch
    real_popen = subprocess.Popen
    real_fork_exec = subprocess._fork_exec
    real_waitpid = os.waitpid
    real_close = os.close
    real_identity = native_guard._process_identity
    real_outcome = run_logging.record_worker_outcome
    real_finalize = run_logging.finalize_logging

    def role_from_ticket(path):
        role = Path(path).stem
        assert role in {'tracker', 'google', 'ftp'}
        assert Path(path).is_relative_to(root)
        return role

    def observe_launch(actual_repo, ticket, descriptors, *, owner):
        assert actual_repo == repo
        role = role_from_ticket(ticket)
        previous = state['active_role'], state['active_owner']
        state['active_role'], state['active_owner'] = role, owner
        if state['armed']:
            state['owners'][role] = owner
        try:
            return real_launch(actual_repo, ticket, descriptors, owner=owner)
        finally:
            state['active_role'], state['active_owner'] = previous

    def observe_identity(pid):
        identity = real_identity(pid)
        if state['armed'] and state['active_role'] == 'google':
            process = state['births']['google']
            assert process.pid == pid and state['active_owner']['process'] is process
            assert type(state['active_owner']['pidfd']) is int
            assert identity['parent_pid'] == os.getpid()
            assert identity['session'] == pid
            state['google_identity'] = dict(identity)
            state['trace'].append('GOOGLE_IDENTITY_CONFIRMED')
        return identity

    def observe_fork_exec(*arguments, **options):
        role = state['active_role']
        if state['armed'] and role == 'ftp':
            assert state['constructor_active'] and state['entered']
            assert not options and len(arguments) == 22
            assert tuple(arguments[0]) == state['ftp_command']
            assert tuple(arguments[1]) == (os.fsencode(state['ftp_command'][0]),)
            assert arguments[2] is True and arguments[15] is True
            assert Path(arguments[4]) == missing_cwd and arguments[21] is None
            assert set(arguments[3]) == set(state['ftp_pass_fds']) | {arguments[13]}
        pid = real_fork_exec(*arguments, **options)
        if state['armed']:
            assert role in {'tracker', 'google', 'ftp'} and type(pid) is int and pid > 1
            state['forks'].append((role, pid))
            if role == 'ftp':
                assert state['ftp_fork_pid'] is None
                state['ftp_fork_pid'] = pid
                state['trace'].append('FTP_FORK_EXEC_RETURNED')
        return pid

    def observe_waitpid(pid, options):
        result = real_waitpid(pid, options)
        if (state['armed'] and state['constructor_active']
                and pid == state['ftp_fork_pid']):
            assert options == 0 and result[0] == pid
            state['waits'].append(result)
            state['trace'].append('FTP_WAITPID_RETURNED')
        return result

    def observe_close(descriptor):
        try:
            result = real_close(descriptor)
        except OSError as error:
            if state['armed']:
                state['close_errors'].append((descriptor, error.errno))
            raise
        if state['armed']:
            state['closes'].append(descriptor)
        return result

    def observe_popen(command, *arguments, **options):
        assert not arguments
        role = role_from_ticket(command[-1])
        expected = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
                    str(repo / 'tests/linux_bootstrap/native_worker.py'), '--ticket', str(command[-1]))
        assert tuple(command) == expected
        assert set(options) == {'executable', 'cwd', 'env', 'stdin', 'stdout', 'stderr',
                                'close_fds', 'pass_fds', 'start_new_session'}
        assert options['executable'] == expected[0] and options['cwd'] == repo
        assert all(options[key] == subprocess.DEVNULL for key in ('stdin', 'stdout', 'stderr'))
        assert options['close_fds'] is True and options['start_new_session'] is True
        assert type(options['pass_fds']) is tuple
        if state['armed'] and role == 'ftp':
            google = state['births']['google']
            assert state['google_identity'] is not None
            assert state['owners']['google']['identity'] == state['google_identity']
            assert state['owners']['google']['process'] is google
            assert not missing_cwd.exists() and missing_cwd.parent == root
            state['entered'] = True
            state['constructor_active'] = True
            state['ftp_command'] = expected
            state['ftp_pass_fds'] = options['pass_fds']
            state['trace'].append('FTP_REAL_CONSTRUCTOR_ENTERED')
            altered = dict(options, cwd=missing_cwd)
            assert {key: value for key, value in altered.items() if key != 'cwd'} == {
                key: value for key, value in options.items() if key != 'cwd'}
            try:
                process = real_popen(command, **altered)
            except OSError as error:
                assert type(error) is FileNotFoundError and error.errno == errno.ENOENT
                assert Path(error.filename) == missing_cwd
                assert state['ftp_fork_pid'] is not None and len(state['waits']) == 1
                state['raised'] = True
                state['error_errno'] = error.errno
                state['trace'].append('FTP_REAL_CONSTRUCTOR_RAISED')
                raise
            finally:
                state['constructor_active'] = False
        else:
            process = real_popen(command, **options)
        processes.append(process)
        if state['armed']:
            state['births'][role] = process
            if role == 'google':
                state['trace'].append('GOOGLE_POPEN_RETURNED')
        return process

    def observe_outcome(session, role, **options):
        result = real_outcome(session, role, **options)
        if state['armed']:
            state['outcomes'].append((role, dict(session['_outcomes'][role])))
        return result

    def observe_finalize(session, **options):
        report = real_finalize(session, **options)
        if state['armed']:
            # No session, Queue or bootstrap reference escapes this callback.
            state['reports'].append(report)
        return report

    monkeypatch.setattr(native_guard, '_launch', observe_launch)
    monkeypatch.setattr(native_guard, '_process_identity', observe_identity)
    monkeypatch.setattr(subprocess, 'Popen', observe_popen)
    monkeypatch.setattr(subprocess, '_fork_exec', observe_fork_exec)
    monkeypatch.setattr(os, 'waitpid', observe_waitpid)
    monkeypatch.setattr(os, 'close', observe_close)
    monkeypatch.setattr(run_logging, 'record_worker_outcome', observe_outcome)
    monkeypatch.setattr(run_logging, 'finalize_logging', observe_finalize)
    try:
        warm = native_guard.run_native_case('success', repo, root)
        assert warm['status'] == 'PASS' and warm['application_exit'] == 0
        for process in processes:
            _native_assert_reaped(process)
        assert native_guard._unsettled_transports == []
        foreign = os.open(root / 'unrelated-fd', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        info = os.fstat(foreign)
        foreign_identity = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
        before_fds = _native_fd_inventory()
        assert before_fds[foreign] == foreign_identity
        state['armed'] = True
        record = native_guard.run_native_case('success', repo, root)
        assert state['entered'] and state['raised'] and state['error_errno'] == errno.ENOENT
        assert state['trace'] == [
            'GOOGLE_POPEN_RETURNED', 'GOOGLE_IDENTITY_CONFIRMED',
            'FTP_REAL_CONSTRUCTOR_ENTERED', 'FTP_FORK_EXEC_RETURNED',
            'FTP_WAITPID_RETURNED', 'FTP_REAL_CONSTRUCTOR_RAISED',
        ]
        assert set(state['births']) == {'tracker', 'google'}
        assert [role for role, pid in state['forks']] == ['tracker', 'google', 'ftp']
        for role in ('tracker', 'google'):
            assert dict(state['forks'])[role] == state['births'][role].pid
        failed_pid = state['ftp_fork_pid']
        assert type(failed_pid) is int and failed_pid not in {
            process.pid for process in processes}
        assert len(state['waits']) == 1
        waited_pid, wait_status = state['waits'][0]
        assert waited_pid == failed_pid and os.WIFEXITED(wait_status)
        failed_exit = os.waitstatus_to_exitcode(wait_status)
        assert failed_exit != 0
        with pytest.raises(ChildProcessError):
            os.waitid(os.P_PID, failed_pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        for process in processes:
            _native_assert_reaped(process)

        assert set(record) == CASE_FIELDS
        assert record['status'] == 'INCOMPLETE' and record['application_exit'] == 1
        assert record['failure_code'] == 'LNX03_NATIVE_CLEANUP_INCOMPLETE'
        assert record['two_workers_registered'] is True and record['all_workers_settled'] is False
        assert len(record['worker_records']) == 1
        google = state['births']['google']
        worker = record['worker_records'][0]
        assert worker['role'] == 'google' and worker['pid'] == google.pid
        assert worker['exitcode'] == google.returncode and worker['reaped'] is True
        assert state['owners']['google']['process'] is google
        ftp_owner = state['owners']['ftp']
        assert ftp_owner['birth_attempted'] is True
        assert all(ftp_owner[key] is None for key in ('process', 'pidfd', 'identity', 'witness'))
        assert len(native_guard._unsettled_transports) == 1
        retained = native_guard._unsettled_transports[0]
        assert set(retained) == {'unknown_births'} and len(retained['unknown_births']) == 1
        assert retained['unknown_births'][0] is ftp_owner
        assert len(state['reports']) == 1
        report = state['reports'][0]
        assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
        assert report['delivery_outcome'] == 'INCOMPLETE' and report['storage_sealed'] is False
        assert set(report['worker_outcomes']) == {'google'}
        outcome = report['worker_outcomes']['google']
        assert outcome['started'] is True and outcome['exit_status'] == google.returncode
        assert state['outcomes'] and all(role == 'google' and value == outcome
                                         for role, value in state['outcomes'])
        finals = {row['worker_id']: row['final_report']
                  for row in report['expected_producers'] if row['worker_id'] != 'parent'}
        assert set(finals) == {'google', 'ftp'} and finals['ftp'] is None
        assert record['logging'] == {
            'delivery_outcome': report['delivery_outcome'], 'storage_sealed': report['storage_sealed'],
            'exit_status': report['exit_status'],
            'worker_final_count': sum(value is not None for value in finals.values()),
        }
        tracker = state['births']['tracker']
        assert record['auxiliary']['pid'] == tracker.pid
        assert record['auxiliary']['settled'] is True and record['auxiliary']['reaped'] is True
        assert record['auxiliary']['exitcode'] == tracker.returncode == 0
        assert not any(thread.is_alive() and thread.name in {'FLOG-writer', 'QueueFeederThread'}
                       for thread in native_guard.threading.enumerate())
        assert _native_fd_inventory() == before_fds
        assert state['closes'] and state['close_errors'] == []
        before_refusal = (len(processes), len(state['forks']), len(state['outcomes']),
                          len(state['reports']), len(state['closes']), tuple(root.iterdir()))
        with pytest.raises(native_guard.NativeAdmissionError) as refusal:
            native_guard.run_native_case('success', repo, root)
        assert refusal.value.args == ('LNX03_NATIVE_FOREIGN_TRACKER',)
        assert before_refusal == (len(processes), len(state['forks']), len(state['outcomes']),
                                  len(state['reports']), len(state['closes']), tuple(root.iterdir()))
        assert native_guard._unsettled_transports == [retained]
        assert retained['unknown_births'][0] is ftp_owner
        assert _native_fd_inventory() == before_fds
        assert state['close_errors'] == []
        witness = dict(schema='lnx03-constructor-oracle-v1', status='PASS',
            source_sha=os.environ['LNX03_SOURCE_SHA'], constructor_entered=True,
            google_identity_before_constructor=True, fork_pid=failed_pid,
            waitpid_pid=waited_pid, wait_status=wait_status, internal_exit=failed_exit,
            internal_reaped=True, error_errno=state['error_errno'],
            google_pid=google.pid, google_exit=google.returncode, google_reaped=True,
            tracker_pid=tracker.pid, tracker_exit=tracker.returncode, tracker_reaped=True,
            simulated_status=record['status'], simulated_application_exit=record['application_exit'],
            logging_started=True, logging_google_exit=outcome['exit_status'],
            logging_delivery=report['delivery_outcome'], logging_sealed=report['storage_sealed'],
            worker_final_count=record['logging']['worker_final_count'],
            ftp_outcome_omitted=True, ftp_final_absent=True, fd_inventory_equal=True,
            close_error_count=len(state['close_errors']),
            unknown_retained=True, later_refused=True, no_later_birth=True)
        assert re.fullmatch('[0-9a-f]{40}', witness['source_sha'])
        # This is a local primitive witness, not an additional exported case.
        # Native proof is bound to this exact test source and its pytest result.
        assert json.loads(json.dumps(witness, sort_keys=True,
                          separators=(',', ':'), allow_nan=False)) == witness
        verified = True
    finally:
        # This safety net owns only returned real Popen objects. It must not
        # consume/retry the failed constructor child's independently seen reap.
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
                current = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
                if current == foreign_identity:
                    os.close(foreign)
        if verified and all(process.returncode is not None for process in processes):
            assert root.resolve().parent == parent
            shutil.rmtree(root)
