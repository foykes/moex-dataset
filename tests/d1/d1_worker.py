"""Fixed disposable D1 worker; all launch authority belongs to the controller."""

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
specification = importlib.util.spec_from_file_location('mds_offline_guard', ROOT / 'tools/offline_guard.py')
guard = importlib.util.module_from_spec(specification)
sys.modules['mds_offline_guard'] = guard
specification.loader.exec_module(guard)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', required=True)
    parser.add_argument('--slot', required=True)
    arguments = parser.parse_args()
    context = guard.d1_worker_bootstrap(ROOT, arguments.case, arguments.slot)
    contract = context.d1_contract
    mode = contract['mode']
    if mode == 'exit_before_ready_7':
        os._exit(7)
    if mode == 'malformed_ready':
        sys.stdout.write('not-json\n')
        sys.stdout.flush()
        sys.stdin.readline()
        os._exit(7)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import dataset_io
    import run_logging

    receipt = {}
    validator_calls = 0
    mutex_name = None
    current = None
    status = 'VALIDATED'
    session = run_logging.configure_logging(ROOT,
        run_id='d1-' + context.run_id[:20] + '-' + arguments.case + '-' + arguments.slot,
        environment='offline', producer_sha=context.tested_sha,
        config={'log_root': Path(contract['logging_root']).relative_to(ROOT).as_posix()},
        sensitive_values=('D1_WORKER_TOKEN_b328', 'D1_WORKER_COOKIE_4b6e', 'D1_WORKER_KEY_95de'))
    target = Path(contract['case_root']) / contract['target']

    def payload(state=None, data=None):
        observed = data if data is not None else current
        return {'receipt': receipt or None, 'status': state or status,
            'observed_sha256': hashlib.sha256(observed).hexdigest() if observed is not None else None,
            'observed_bytes': len(observed) if observed is not None else None,
            'validator_calls': validator_calls, 'mutex_name': mutex_name}

    backend = dataset_io._windows_backend()
    create, wait = backend['CreateMutexW'], backend['WaitForSingleObject']
    contended = False
    def traced_create(attributes, owner, name):
        nonlocal mutex_name
        mutex_name = name
        return create(attributes, owner, name)
    def traced_wait(handle, milliseconds):
        nonlocal contended
        observed = wait(handle, milliseconds)
        if observed == 0x102 and not contended:
            contended = True
            guard.d1_send_frame('CONTENDED', payload('DENIED'))
        return observed
    backend['CreateMutexW'], backend['WaitForSingleObject'] = traced_create, traced_wait
    dataset_io._windows_backend = lambda: backend

    def validate(stream):
        nonlocal validator_calls, current
        validator_calls += 1
        current = stream.read() if stream is not None else None
        if mode in ('retry_false', 'wait_reject_b', 'validate_reject_only'):
            return False
        if mode == 'retry_unverified':
            return None
        if mode == 'retry_exception':
            class Malicious:
                def __str__(self):
                    raise AssertionError('unexpected string conversion')
                def __repr__(self):
                    raise AssertionError('unexpected representation conversion')
            raise ValueError({'token': 'D1_WORKER_TOKEN_b328', 'cookie': ['D1_WORKER_COOKIE_4b6e', Malicious()],
                'url': 'https://user:D1_WORKER_KEY_95de@example.invalid/?token=D1_WORKER_TOKEN_b328'})
        seed = b'ticker,begin,close\nSEED,2024-01-01 10:00:00,100.0\n'
        first = seed + b'A,2024-01-01 10:00:00,200.0\n'
        second = first + b'B,2024-01-01 10:00:00,300.0\n'
        return current in (seed, first, second)

    def lock(timeout=None):
        return dataset_io.resource_lock(target.name, resource_root=target.parent,
            validate_target=validate, logging_session=session, receipt=receipt,
            timeout_s=contract['timeout_s'] if timeout is None else timeout)

    def candidate(stream):
        added = b'B,2024-01-01 10:00:00,300.0\n' if mode == 'retry_after_denial' else b'A,2024-01-01 10:00:00,200.0\n'
        stream.write(current + added)

    def candidate_valid(stream):
        return stream.read() == current + (b'B,2024-01-01 10:00:00,300.0\n' if mode == 'retry_after_denial' else b'A,2024-01-01 10:00:00,200.0\n')

    guard.d1_send_frame('ARMED')
    guard.d1_wait_command('GO')
    if mode == 'park_without_release':
        with lock():
            guard.d1_send_frame('TARGET_READY', payload())
            guard.d1_wait_command('RELEASE')
    elif mode == 'attempt_extra_child':
        import subprocess
        try:
            subprocess.Popen([sys.executable, '-I', '-B', '-c', 'raise SystemExit(0)'])
        except RuntimeError:
            pass
    elif mode == 'read_final':
        with guard.d1_native_reader(target) as stream:
            current = stream.read()
        guard.d1_send_frame('TARGET_READY', payload('VALIDATED'))
        guard.d1_wait_command('RELEASE')
        with guard.d1_native_reader(target) as stream:
            current = stream.read()
    elif mode in ('omit_guard_final', 'corrupt_guard_final', 'foreign_report', 'duplicate_report', 'replayed_report', 'malformed_ready'):
        pass
    elif mode in ('retry_false', 'retry_unverified', 'retry_exception'):
        for attempt in range(2):
            receipt = {}
            try:
                with lock():
                    raise AssertionError('rejected target acquired a lease')
            except ValueError:
                status = 'REJECTED'
    elif mode == 'retry_after_denial':
        try:
            with lock(0):
                raise AssertionError('contender acquired occupied mutex')
        except TimeoutError:
            status = 'DENIED'
        guard.d1_wait_command('RELEASE')
        receipt = {}
        with lock(5) as lease:
            guard.d1_send_frame('TARGET_READY', payload('VALIDATED'))
            dataset_io.atomic_write_file(lease, candidate, candidate_valid)
        status = 'SUCCEEDED'
        current = target.read_bytes()
    else:
        try:
            with lock() as lease:
                guard.d1_send_frame('TARGET_READY', payload('VALIDATED'))
                if mode in ('hold_update', 'hold_update_a', 'write_gated'):
                    guard.d1_wait_command('RELEASE')
                    dataset_io.atomic_write_file(lease, candidate, candidate_valid)
                    status = 'SUCCEEDED'
                elif mode in ('hold_partial', 'hold_partial_a'):
                    def partial(stream):
                        stream.write(b'ticker,begin,close\npartial')
                        stream.flush()
                        os.fsync(stream.fileno())
                        emitted = run_logging.emit_event(session, 'INFO', 'd1_checkpoint',
                            'Подтверждена контрольная точка', {'stage': 'runtime', 'fields': {}})
                        flushed = run_logging.flush_logging(session)
                        assert emitted['accepted'] and flushed['confirmed']
                        assert run_logging.check_logging_health(session)['healthy']
                        guard.d1_send_frame('CANDIDATE_READY', payload('VALIDATED', b'ticker,begin,close\npartial'))
                        guard.d1_wait_command('RELEASE')
                    dataset_io.atomic_write_file(lease, partial, lambda stream: False)
                elif mode == 'hold_after_replace':
                    original = dataset_io._readback
                    def postcommit(opaque_lease):
                        assert receipt['commit_state'] == 'REPLACED'
                        guard.d1_send_frame('POSTCOMMIT_READY', payload('SUCCEEDED'))
                        guard.d1_wait_command('RELEASE')
                        return original(opaque_lease)
                    dataset_io._readback = postcommit
                    dataset_io.atomic_write_file(lease, candidate, candidate_valid)
                    status = 'SUCCEEDED'
                else:
                    status = 'VALIDATED'
            current = target.read_bytes()
        except TimeoutError:
            status = 'DENIED'
        except ValueError:
            if mode not in ('wait_reject_b', 'validate_reject_only'):
                raise
            status = 'REJECTED'
    if mode not in ('park_without_release',):
        result = run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
        assert result['exit_status'] == 0
    guard.d1_send_frame('RESULT', payload())
    guard.d1_finish_worker(exit_code=0)
    return 0


if __name__ == '__main__':
    try:
        exit_code = main()
    except BaseException:
        # Application exceptions stay inside the process; argv, traceback source
        # and exception args never become controller evidence or stderr text.
        sys.stderr.write('D1_WORKER_FAILED\n')
        sys.stderr.flush()
        try:
            guard.finish(1)
        except BaseException:
            sys.stderr.write('D1_GUARD_FINAL_FAILED\n')
            sys.stderr.flush()
        exit_code = 1
    raise SystemExit(exit_code)
