import errno
import json
import os
from pathlib import Path
import queue
import time
import uuid

import pytest

from _probe import ROOT


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
