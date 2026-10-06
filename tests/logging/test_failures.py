import errno
import json
import os
from pathlib import Path
import queue
import time
import uuid

import pytest

from _probe import ROOT
from _probe import admission_race


@pytest.mark.parametrize('level', ['INFO', 'ERROR', 'FLUSH'])
@pytest.mark.parametrize('timing', ['completion', 'timeout'])
def test_admitted_operation_finalize_race(log, level, timing):
    context = log.configure_logging(ROOT, run_id='race-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8',
        config={'shutdown_deadline_s': 0.4, 'console_level': 'ERROR'})
    result = admission_race(log, context, level, timing)
    report = result['report']
    assert not result['health_during_freeze']['healthy']
    assert result['before'] == result['after'] and result['cached_unchanged']
    assert result['fallback_calls_after_release'] == 0
    if timing == 'timeout':
        assert report['delivery_outcome'] == 'INCOMPLETE' and report['exit_status'] == 1
        assert not report['storage_sealed'] and report['evidence_incomplete']
        assert report['terminal_ref'] is None
        assert report['null_reasons']['authoritative_summary'] == 'ADMISSION_TIMEOUT'
        assert report['event_counters']['scope'] == 'LOWER_BOUND'
        assert not (context['_run_root'] / 'run_summary.json').exists()
        assert not (context['_run_root'] / 'summary.sha256').exists()
        assert not log.check_logging_health(context)['healthy']
    else:
        assert report['exit_status'] == 0 and report['storage_sealed']
        assert result['receipt']['accepted']
        assert log._read_summary(context['_run_root']) == report
        rows = [json.loads(line) for line in (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
        expected = ['run_completed'] if level == 'FLUSH' else ['admitted_fixture', 'run_completed']
        assert [row['event'] for row in rows] == expected
        assert sum(report['logical_event_counts_by_level'].values()) == len(expected)


def test_shutdown_deadline_includes_admission_mutex(log):
    import threading
    context = log.configure_logging(ROOT, run_id='mutex-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8', config={'shutdown_deadline_s': 0.2})
    assert log._admit(context)
    finished = threading.Event()
    results = []
    def finalize():
        try:
            results.append(log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0))
        finally:
            finished.set()
    context['_admission'].acquire()
    thread = threading.Thread(target=finalize)
    before = time.monotonic()
    try:
        thread.start()
        assert finished.wait(1)
        assert time.monotonic() - before < 1
        assert results[0]['null_reasons']['authoritative_summary'] == 'ADMISSION_TIMEOUT'
        assert not results[0]['storage_sealed'] and results[0]['exit_status'] == 1
        assert not (context['_run_root'] / 'run_summary.json').exists()
    finally:
        context['_admission'].release()
        log._release(context)
        thread.join(2)
    assert not thread.is_alive()


def test_listener_owns_files_past_deadline_no_authoritative_summary(log, monkeypatch):
    import threading
    context = log.configure_logging(ROOT, run_id='listener-held-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8', config={'shutdown_deadline_s': 0.2})
    entered, release = threading.Event(), threading.Event()
    original = log._write_record
    def write(session, wrapper):
        if wrapper['event']['event'] == 'listener_fixture':
            entered.set()
            assert release.wait(3)
        return original(session, wrapper)
    monkeypatch.setattr(log, '_write_record', write)
    try:
        assert log.emit_event(context, 'INFO', 'listener_fixture', 'safe', {})['accepted']
        assert entered.wait(2)
        report = log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
        snapshot = json.loads(json.dumps(report))
        assert report['exit_status'] == 1 and not report['storage_sealed']
        assert not (context['_run_root'] / 'run_summary.json').exists()
        assert not (context['_run_root'] / 'summary.sha256').exists()
    finally:
        release.set()
        context['_thread'].join(2)
    assert not context['_thread'].is_alive()
    assert log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0) == snapshot
    assert not log.check_logging_health(context)['healthy']


def test_rejected_admission_retries_deferred_cleanup(log, monkeypatch):
    context = log.configure_logging(ROOT, run_id='cleanup-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8', config={'shutdown_deadline_s': 0.05})
    assert log._admit(context)
    log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
    context['_thread'].join(2)
    assert not context['_thread'].is_alive()
    original = log._cleanup_abandoned
    skipped = []
    def contended(producer):
        # Reproduce one lost nonblocking attempt from the last permit release.
        if not skipped:
            skipped.append(True)
            return
        return original(producer)
    monkeypatch.setattr(log, '_cleanup_abandoned', contended)
    log._release(context)
    assert not context['_ack'].closed
    with pytest.raises(ValueError, match='LOG_CONTEXT_CLOSED'):
        log.emit_event(context, 'INFO', 'late', 'safe', {})
    assert context['_ack'].closed and context['_transport_closing']


@pytest.mark.parametrize('same_id', [True, False])
def test_concurrent_registration_keeps_expected_producers_bounded(log, monkeypatch, same_id):
    import threading
    context = log.configure_logging(ROOT, run_id='registration-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8', config={'max_workers': 1})
    rendezvous = threading.Barrier(2, timeout=2)
    original = context['_spawn'].Pipe
    results, errors = [], []
    def pipe(*args, **kwargs):
        pair = original(*args, **kwargs)
        rendezvous.wait()
        return pair
    monkeypatch.setattr(context['_spawn'], 'Pipe', pipe)
    def register(worker):
        try:
            results.append(log.prepare_worker(context, worker))
        except ValueError as error:
            errors.append(str(error))
    workers = ['same', 'same' if same_id else 'other']
    threads = [threading.Thread(target=register, args=(worker,)) for worker in workers]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(3)
        assert all(not thread.is_alive() for thread in threads)
        assert len(results) == 1 and errors == ['LOG_WORKER_REGISTRATION']
        assert len(context['_expected']) == 1 and len(context['_ack_senders']) == 2
        worker = next(iter(context['_expected']))
        log.record_worker_outcome(context, worker, started=False, exit_status=None)
    finally:
        log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)


def retention_config(root, **overrides):
    result = {'log_root': root.relative_to(ROOT).as_posix(), 'retained_runs': 1,
        'rotation_bytes': 65536, 'rotation_segments': 1, 'protected_bytes': 65536, 'max_workers': 1,
        'fallback_bytes_per_producer': 65536, 'context_bytes': 65536,
        'summary_bytes': 65536, 'root_budget_bytes': 2000000}
    result.update(overrides)
    return result


def retained_run(log, config, name, incomplete=False, failed=False):
    context = log.configure_logging(ROOT, run_id=name, environment='offline', producer_sha='011a9d8', config=config)
    (context['_run_root'] / 'sentinel').write_bytes(b'UNCHANGED_REVIEW_EVIDENCE')
    if incomplete:
        log.prepare_worker(context, 'never-started')
        log.record_worker_outcome(context, 'never-started', started=False, exit_status=None)
    report = log.finalize_logging(context, execution_outcome='FAILED' if failed else 'COMPLETED', exit_status=1 if failed else 0)
    assert report['storage_sealed']
    assert report['evidence_incomplete'] is incomplete
    return context['_run_root']


def evidence_hashes(root):
    import hashlib
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}


@pytest.mark.parametrize('failed', [False, True])
@pytest.mark.parametrize('policy', ['count', 'age'])
def test_retention_protects_sealed_incomplete_and_prunes_complete(log, tmp_path, monkeypatch, failed, policy):
    config = retention_config(tmp_path / 'review-retention', retained_runs=1 if policy == 'count' else 3,
        retention_days=1)
    protected = retained_run(log, config, 'incomplete', incomplete=True)
    before = evidence_hashes(protected)
    eligible = retained_run(log, config, 'complete', failed=failed)
    assert protected.exists() and evidence_hashes(protected) == before
    if policy == 'age':
        original = log.datetime.datetime
        class Future(original):
            @classmethod
            def now(cls, tz=None):
                return original.now(tz) + log.datetime.timedelta(days=2)
        monkeypatch.setattr(log.datetime, 'datetime', Future)
    retained_run(log, config, 'next')
    assert protected.exists() and evidence_hashes(protected) == before
    assert not eligible.exists()


@pytest.mark.parametrize('older_protected_bytes', [65536, 131072])
def test_retention_incomplete_full_reservation_refuses_budget(log, tmp_path, older_protected_bytes):
    root = tmp_path / 'review-budget'
    old = retention_config(root, protected_bytes=older_protected_bytes)
    protected = retained_run(log, old, 'incomplete', incomplete=True)
    before = evidence_hashes(protected)
    new = retention_config(root)
    previous_reserve = 393281 if older_protected_bytes == 65536 else 458817
    new['root_budget_bytes'] = previous_reserve + 393281 - 1
    with pytest.raises(ValueError, match='LOG_ROOT_BUDGET_UNAVAILABLE'):
        log.configure_logging(ROOT, run_id='refused', environment='offline', producer_sha='011a9d8', config=new)
    assert evidence_hashes(protected) == before and not (root / 'refused').exists()


def test_retention_previous_reservation_not_limited_by_new_context_cap(log, tmp_path):
    root = tmp_path / 'old-context'
    old = retention_config(root, protected_bytes=131072)
    protected = retained_run(log, old, 'incomplete-' + 'x' * 65, incomplete=True)
    before = evidence_hashes(protected)
    new = retention_config(root)
    new['context_bytes'] = (protected / 'context.json').stat().st_size - 40
    new_reserve = 327745 + new['context_bytes']
    new['root_budget_bytes'] = 458817 + new_reserve - 1
    fit = dict(new, log_root=(tmp_path / 'fit-context').relative_to(ROOT).as_posix())
    control = log.configure_logging(ROOT, run_id='next', environment='offline', producer_sha='011a9d8', config=fit)
    assert (control['_run_root'] / 'context.json').stat().st_size <= new['context_bytes']
    assert log.finalize_logging(control, execution_outcome='COMPLETED', exit_status=0)['exit_status'] == 0
    with pytest.raises(ValueError, match='LOG_ROOT_BUDGET_UNAVAILABLE'):
        log.configure_logging(ROOT, run_id='next', environment='offline', producer_sha='011a9d8', config=new)
    assert evidence_hashes(protected) == before and not (root / 'next').exists()


@pytest.mark.parametrize('fault', ['missing_summary', 'corrupt_seal'])
def test_retention_preserves_corrupt_evidence_reservation(log, tmp_path, fault):
    root = tmp_path / 'corrupt'
    config = retention_config(root)
    protected = retained_run(log, config, 'corrupt')
    if fault == 'missing_summary':
        (protected / 'run_summary.json').unlink()
    else:
        (protected / 'summary.sha256').write_bytes(b'0' * 64 + b'\n')
    before = evidence_hashes(protected)
    config['root_budget_bytes'] = 786561
    with pytest.raises(ValueError, match='LOG_ROOT_BUDGET_UNAVAILABLE'):
        log.configure_logging(ROOT, run_id='next', environment='offline', producer_sha='011a9d8', config=config)
    assert evidence_hashes(protected) == before


def test_listener_failure_no_false_success(log, session, monkeypatch):
    def fail(*args):
        raise OSError(errno.ENOSPC, 'private disk path')
    monkeypatch.setattr(log, '_write_record', fail)
    receipt = log.emit_event(session, 'ERROR', 'failure', 'safe', {})
    assert not receipt['confirmed']
    assert not log.check_logging_health(session)['healthy']
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert report['exit_status'] == 1 and report['delivery_outcome'] == 'INCOMPLETE'
    assert list(session['_run_root'].glob('fallback-*.jsonl'))


@pytest.mark.parametrize('fault', ['transport', 'inventory', 'summary_write', 'summary_replace'])
def test_late_failures_summary_last_never_authoritative_zero(log, session, monkeypatch, fault):
    log.emit_event(session, 'INFO', 'ordinary', 'safe', {})
    def fail(*args, **kwargs):
        raise OSError(errno.ENOSPC, 'private failure')
    if fault == 'transport':
        monkeypatch.setattr(log, '_close_transport', fail)
    elif fault == 'inventory':
        monkeypatch.setattr(log, '_inventory', fail)
    elif fault == 'summary_write':
        original = log._json_file
        def writer(path, *args, **kwargs):
            if path.name == 'run_summary.json.tmp':
                fail()
            return original(path, *args, **kwargs)
        monkeypatch.setattr(log, '_json_file', writer)
    else:
        monkeypatch.setattr(log.os, 'replace', fail)
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1
    path = session['_run_root'] / 'run_summary.json'
    assert not path.exists() or json.loads(path.read_text(encoding='utf-8'))['exit_status'] == 1
    if path.exists():
        assert json.loads(path.read_text(encoding='utf-8')) == result
    assert log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0) is result


def test_no_io_after_summary_replace_and_frozen(log, session, monkeypatch):
    original = log.os.replace
    def replace(source, destination):
        result = original(source, destination)
        if Path(destination).name == 'run_summary.json':
            monkeypatch.setattr(log, '_write', lambda *a: pytest.fail('post-final write'))
            monkeypatch.setattr(log, '_inventory', lambda *a: pytest.fail('post-final hash'))
            monkeypatch.setattr(log, '_close_transport', lambda *a: pytest.fail('post-final close'))
        return result
    monkeypatch.setattr(log.os, 'replace', replace)
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert report['exit_status'] == 0
    with pytest.raises(ValueError):
        log.emit_event(session, 'INFO', 'late', 'late', {})
    with pytest.raises(ValueError):
        log.prepare_worker(session, 'late')
    assert not log.flush_logging(session)['accepted']
    with pytest.raises(ValueError):
        log.mark_unreached(session, 'late')


def test_summary_size_no_silent_truncation(log):
    context = log.configure_logging(ROOT, run_id='tiny-' + uuid.uuid4().hex, environment='offline',
        producer_sha='c4f2210', config={'summary_bytes': 100})
    report = log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
    assert report['exit_status'] == 1
    assert not (context['_run_root'] / 'run_summary.json').exists()


def test_queue_full_is_sticky_and_shutdown_bounded(log, session, monkeypatch):
    original = session['_queue']
    class Full:
        def put_nowait(self, *a):
            raise queue.Full
        def put(self, *a, **k):
            raise queue.Full
        def cancel_join_thread(self):
            original.cancel_join_thread()
        def close(self):
            original.close()
    session['_queue'] = Full()
    assert not log.emit_event(session, 'INFO', 'drop', 'safe', {})['accepted']
    before = time.monotonic()
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert time.monotonic() - before < 3
    assert report['event_counters']['dropped'] == 1 and report['exit_status'] == 1


def test_rotation_retention_and_single_segment(log):
    context = log.configure_logging(ROOT, run_id='rotate-' + uuid.uuid4().hex, environment='offline',
        producer_sha='c4f2210', config={'rotation_bytes': 65536, 'rotation_segments': 1, 'console_level': 'ERROR'})
    for i in range(3):
        log.emit_event(context, 'INFO', 'large', 'x' * 30000, {})
        assert log.flush_logging(context)['confirmed']
    report = log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
    assert report['exit_status'] == 0 and report['rotation_count'] >= 1
    assert not list(context['_run_root'].glob('events.*.jsonl'))
    assert log._read_summary(context['_run_root']) == report


def test_path_alias_hardlink_sentinel(log):
    folder = ROOT / '.f-log/alias-tests' / uuid.uuid4().hex
    folder.mkdir(parents=True)
    sentinel = folder / 'sentinel'
    sentinel.write_bytes(b'UNCHANGED')
    alias = folder / 'alias'
    os.link(sentinel, alias)
    with pytest.raises(ValueError, match='LOG_PATH_ALIAS'):
        log._safe_path(alias, ROOT)
    assert sentinel.read_bytes() == b'UNCHANGED'


def test_inaccessible_root_before_stage(log, monkeypatch):
    original = log._prepare_root
    def deny(*a):
        raise PermissionError('private path')
    monkeypatch.setattr(log, '_prepare_root', deny)
    with pytest.raises(PermissionError):
        log.configure_logging(ROOT, run_id='denied-' + uuid.uuid4().hex, environment='offline', producer_sha='c4f2210')


@pytest.mark.parametrize('mode', ['flush', 'close'])
def test_terminal_sink_failure_nonzero(log, session, mode):
    original = session['_primary']
    class Fault:
        last = b''
        @property
        def closed(self):
            return original.closed
        def tell(self):
            return original.tell()
        def write(self, data):
            self.last = data
            return original.write(data)
        def flush(self):
            if mode == 'flush' and b'run_completed' in self.last:
                raise OSError(errno.ENOSPC, 'private canary path')
            return original.flush()
        def close(self):
            original.close()
            if mode == 'close':
                raise OSError('private close path')
    session['_primary'] = Fault()
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1 and result['delivery_outcome'] == 'INCOMPLETE'


def test_both_primary_and_fallback_unavailable(log, session, monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise OSError(errno.ENOSPC, 'unsafe path')
    monkeypatch.setattr(log, '_write', fail)
    assert not log.emit_event(session, 'ERROR', 'error', 'password=NO_SINK_CANARY', {})['confirmed']
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1 and not (session['_run_root'] / 'run_summary.json').exists()
    assert 'NO_SINK_CANARY' not in capsys.readouterr().err


def test_deadline_includes_producer_lock(log, session):
    session['_lock'].acquire()
    before = time.monotonic()
    try:
        assert not log.emit_event(session, 'ERROR', 'error', 'safe', {})['confirmed']
    finally:
        session['_lock'].release()
    assert time.monotonic() - before < 2
    assert not log.check_logging_health(session)['healthy']


def test_real_windows_junction_refused_sentinel_unchanged(log):
    import _winapi
    folder = ROOT / '.f-log/alias-tests' / uuid.uuid4().hex
    target = folder / 'target'
    target.mkdir(parents=True)
    sentinel = target / 'sentinel'
    sentinel.write_bytes(b'UNCHANGED')
    junction = folder / 'junction'
    _winapi.CreateJunction(str(target), str(junction))
    with pytest.raises(ValueError, match='LOG_PATH_ALIAS'):
        log._safe_path(junction / 'sentinel', ROOT)
    assert sentinel.read_bytes() == b'UNCHANGED'


def test_retention_preserves_active_and_refuses_unavailable_budget(log, tmp_path):
    root = tmp_path / 'bounded'
    config = {'log_root': root.relative_to(ROOT).as_posix(), 'retained_runs': 1,
        'rotation_bytes': 65536, 'rotation_segments': 1, 'protected_bytes': 65536, 'max_workers': 1,
        'fallback_bytes_per_producer': 65536, 'context_bytes': 65536,
        'summary_bytes': 65536, 'root_budget_bytes': 400000}
    active = log.configure_logging(ROOT, run_id='active', environment='offline', producer_sha='c4f2210', config=config)
    before = (active['_run_root'] / 'context.json').read_bytes()
    try:
        with pytest.raises(ValueError, match='LOG_ROOT_BUDGET_UNAVAILABLE'):
            log.configure_logging(ROOT, run_id='second', environment='offline', producer_sha='c4f2210', config=config)
        assert (active['_run_root'] / 'context.json').read_bytes() == before
    finally:
        log.finalize_logging(active, execution_outcome='COMPLETED', exit_status=0)
    second = log.configure_logging(ROOT, run_id='second', environment='offline', producer_sha='c4f2210', config=config)
    assert not (root / 'active').exists()
    log.finalize_logging(second, execution_outcome='COMPLETED', exit_status=0)


def test_guards_reject_live_actions():
    import socket
    import subprocess
    import sys
    for action in (lambda: socket.create_connection(('iss.moex.com', 443)),
        lambda: (ROOT / 'secrets/canary.json').read_text(),
        lambda: (ROOT / 'datasets/forbidden.csv').read_bytes(),
        lambda: (ROOT / 'FORBIDDEN_WRITE').write_text('x'),
        lambda: subprocess.run([sys.executable, '-c', 'print(1)'])):
        with pytest.raises(RuntimeError, match='FLOG_OFFLINE_'):
            action()
