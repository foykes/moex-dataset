"""Native oracles: refusing Windows is not Linux runtime acceptance."""

import importlib
import os
from pathlib import Path
import re
import shutil
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
        assert any(worker['exitcode'] == 23 and 'EXIT23' in worker['events'] for worker in workers)
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
