import json
import multiprocessing
import sys
import time

import pytest

from _probe import ROOT, worker_entry


@pytest.mark.parametrize('level', ['INFO', 'ERROR', 'FLUSH'])
@pytest.mark.parametrize('timing', ['completion', 'timeout'])
def test_real_spawn_worker_admission_finish_race(log, level, timing, record_property):
    import uuid
    session = log.configure_logging(ROOT, run_id='worker-race-' + uuid.uuid4().hex,
        environment='offline', producer_sha='011a9d8',
        config={'shutdown_deadline_s': 0.4, 'console_level': 'ERROR'})
    spawn = multiprocessing.get_context('spawn')
    receive, send = spawn.Pipe(duplex=False)
    bootstrap = log.prepare_worker(session, 'race')
    bootstrap['_fixture_reply'] = send
    process = spawn.Process(target=worker_entry, args=(bootstrap, 'race:' + level + ':' + timing, 0))
    try:
        process.start()
        assert receive.poll(10)
        result = receive.recv()
        assert not settle(log, session, 'race', process)
        assert process.exitcode == 0
        assert result['before'] == result['after'] and result['cached_unchanged']
        assert result['fallback_calls_after_release'] == 0
        if timing == 'timeout':
            assert not result['report']['confirmed']
            assert result['report']['cleanup_error'] == 'ADMISSION_TIMEOUT'
        else:
            assert result['report']['confirmed'] and result['receipt']['accepted']
        report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
        assert report['evidence_incomplete'] is (timing == 'timeout')
        rows = [json.loads(line) for line in (session['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
        worker_rows = [r['event'] for r in rows if r['worker'] == 'race']
        expected = [] if timing == 'timeout' else (['worker_completed'] if level == 'FLUSH' else ['admitted_fixture', 'worker_completed'])
        assert worker_rows == expected
        record_property('platform', sys.platform)
        record_property('start_method', 'spawn')
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(5)
        receive.close()
        send.close()
        log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def start_worker(log, session, worker, mode='normal', count=100):
    bootstrap = log.prepare_worker(session, worker)
    assert '_secrets' not in bootstrap
    process = multiprocessing.get_context('spawn').Process(target=worker_entry, args=(bootstrap, mode, count))
    process.start()
    return process


def settle(log, session, worker, process):
    process.join(12)
    timed_out = process.is_alive()
    if timed_out:
        process.terminate()
        process.join(5)
    log.record_worker_outcome(session, worker, started=True, exit_status=process.exitcode, timed_out=timed_out)
    return timed_out


def test_real_two_workers_spawn_order_and_final(log, session, record_property):
    children = [start_worker(log, session, 'w' + str(i)) for i in range(2)]
    for i, process in enumerate(children):
        assert not settle(log, session, 'w' + str(i), process)
        assert process.exitcode == 0
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 0
    rows = [json.loads(line) for line in (session['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    for worker in ('w0', 'w1'):
        assert [r['page'] for r in rows if r['worker'] == worker and r['event'] == 'worker_progress'] == list(range(100))
        assert sum(r['event'] == 'worker_completed' and r['worker'] == worker for r in rows) == 1
    assert sum(r['event'] == 'run_completed' for r in rows) == 1
    assert result['logical_event_counts_by_level'] == {'DEBUG': 0, 'INFO': 203, 'WARNING': 0, 'ERROR': 2}
    assert 'child-canary-719' not in json.dumps(rows) + json.dumps(result)
    record_property('platform', sys.platform)
    record_property('start_method', 'spawn')


@pytest.mark.parametrize('mode', ['before', 'no_final', 'after'])
def test_actual_exit_separate_from_final_ack(log, session, mode):
    process = start_worker(log, session, 'worker', mode, 2)
    assert not settle(log, session, 'worker', process)
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] != 0 and result['evidence_incomplete']
    assert result['worker_outcomes']['worker']['started']


class AckFault:
    def __init__(self, original, log, token, mode):
        self.original, self.log, self.token, self.mode = original, log, token, mode
    def send_bytes(self, frame):
        version, token, sequence, status = self.log.ACK.unpack(frame)
        if sequence == 3 and self.mode == 'duplicate':
            self.original.send_bytes(frame)
            self.original.send_bytes(frame)
            return
        if sequence == 5:  # 2 progress, error, lifecycle, worker-final control
            if self.mode == 'lost':
                return
            if self.mode == 'late':
                time.sleep(1.2)
            if self.mode == 'cross':
                frame = self.log.ACK.pack(version, token ^ 1, sequence, status)
            if self.mode == 'stale':
                frame = self.log.ACK.pack(version, token, sequence - 1, status)
        self.original.send_bytes(frame)
    def close(self):
        self.original.close()


@pytest.mark.parametrize('mode', ['lost', 'late', 'cross', 'stale', 'duplicate'])
def test_final_control_ack_fault_cannot_restore_health(log, session, mode):
    bootstrap = log.prepare_worker(session, 'fault')
    token = bootstrap['_token']
    session['_ack_senders'][token] = AckFault(session['_ack_senders'][token], log, token, mode)
    process = multiprocessing.get_context('spawn').Process(target=worker_entry, args=(bootstrap, 'normal', 2))
    process.start()
    assert not settle(log, session, 'fault', process)
    assert process.exitcode == 0
    assert not log.check_logging_health(session)['healthy']
    if mode != 'duplicate':
        assert token in session['_worker_finals']  # data already reached writer before ACK fault
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1 and result['delivery_outcome'] == 'INCOMPLETE'


def test_expected_never_started_is_not_success(log, session):
    log.prepare_worker(session, 'not-launched')
    log.record_worker_outcome(session, 'not-launched', started=False, exit_status=None)
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1 and result['storage_sealed']


def test_missing_outcome_is_unsealed(log, session):
    log.prepare_worker(session, 'unreported')
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1 and not result['storage_sealed']
