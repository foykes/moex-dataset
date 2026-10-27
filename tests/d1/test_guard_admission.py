"""Compare complete raw refusal evidence, retaining failed guard validation."""

import json
import copy
import ctypes
import hashlib
import io
import queue
import threading
from types import SimpleNamespace

import pytest


CONTROLS = {
    'exit_before_ready_7': ({'D1_READY_NOT_REACHED', 'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'}, 7, False),
    'omit_guard_final': ({'MISSING_FINAL_REPORT'}, 0, False),
    'corrupt_guard_final': ({'CORRUPT_FINAL_REPORT'}, 0, False),
    'park_without_release': ({'D1_UNDECLARED_TIMEOUT', 'MISSING_FINAL_REPORT', 'TIMEOUT', 'UNEXPECTED_EXIT'}, 124, True),
    'attempt_extra_child': ({'UNEXPECTED_COUNTERS'}, 0, False),
    'foreign_report': ({'FOREIGN_FINAL_REPORT'}, 0, False),
    'duplicate_report': ({'DUPLICATE_FINAL_REPORT'}, 0, False),
    'replayed_report': ({'DUPLICATE_FINAL_REPORT'}, 0, False),
    'malformed_ready': ({'D1_FRAME_FORMAT', 'D1_READY_NOT_REACHED', 'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'}, 124, False),
}


@pytest.mark.parametrize('case', tuple(CONTROLS))
def test_control_cases(case, offline_guard, project_root):
    result = offline_guard.run_d1_case(case)
    errors, actual_exit, timed_out = CONTROLS[case]
    child = result['slots']['worker']
    assert result['expected_controls'] is True
    assert result['report_errors'] == []
    assert set(child['report_errors']) == errors
    assert child['validated'] is False
    assert child['actual_exit'] == actual_exit
    assert child['timed_out'] is timed_out
    assert child['termination'] is None
    frames = child['frames']
    assert frames and frames[0]['phase'] == 'GUARD_READY'
    assert all(frame['nodeid'] == 'tests/d1/test_guard_admission.py::test_control_cases[' + case + ']' for frame in frames)
    assert len({frame['pid'] for frame in frames}) == 1
    if case == 'attempt_extra_child':
        assert child['counters'] == {'children': 1}
        context = offline_guard.ensure_context(project_root)
        ticket_id = frames[0]['ticket_id']
        journals = [json.loads(path.read_text(encoding='utf-8')) for path in context.report_dir.rglob('*.json')
            if ticket_id in path.name]
        violations = [record for record in journals if record.get('kind') == 'children']
        assert len(violations) == 1
        assert violations[0]['nodeid'] == frames[0]['nodeid']
    elif case in ('exit_before_ready_7', 'omit_guard_final', 'corrupt_guard_final', 'park_without_release', 'malformed_ready'):
        assert child['counters'] is None
    elif child['counters'] is not None:
        assert child['counters'] == {}


def test_wrong_ticket(offline_guard):
    result = offline_guard.run_d1_case('wrong_ticket')
    assert result['launched'] is False
    assert result['slots'] == {}
    assert result['report_errors'] == ['D1_ADMISSION_TICKET']
    assert result['expected_controls'] is True


def test_wrong_node(offline_guard):
    result = offline_guard.run_d1_case('wrong_node')
    assert result['launched'] is False
    assert result['slots'] == {}
    assert result['report_errors'] == ['D1_ADMISSION_NODE']
    assert result['expected_controls'] is True


def test_arbitrary_case_has_no_launch_capability(offline_guard):
    with pytest.raises(ValueError, match='D1_ADMISSION_CASE'):
        offline_guard.run_d1_case('arbitrary-command-is-not-a-case')


def closed_payload():
    receipt = {'receipt_version': 1, 'operation_id': 'a' * 32, 'file_ref': 'd1-file-' + 'a' * 32,
        'lock_observation': 'NOT_ACQUIRED', 'lock_state': 'NOT_ACQUIRED', 'target_state': 'UNKNOWN',
        'target_validation': 'NOT_RUN', 'lease_entry_observation': None, 'attempt_baseline': None,
        'final_observation': None, 'prepared_candidate': None, 'commit_state': 'NOT_ATTEMPTED',
        'final_relation': 'UNKNOWN', 'write_status': 'NOT_RUN', 'diagnostics_state': 'NOT_CONFIRMED',
        'mandatory_sequences': [], 'barrier_sequence': None, 'errors': [],
        'null_reasons': {key: 'NOT_REACHED' for key in ('lease_entry_observation', 'attempt_baseline',
            'final_observation', 'prepared_candidate', 'barrier_sequence')}}
    return {'receipt': receipt, 'status': 'VALIDATED', 'observed_sha256': None,
        'observed_bytes': None, 'validator_calls': 0, 'mutex_name': None}


@pytest.mark.parametrize('mutation', ['enum', 'hash', 'bytes', 'boolean_bytes', 'identifier', 'pathname',
    'nested', 'error', 'null_reason', 'sequence', 'barrier', 'status', 'extra', 'observed_hash',
    'malicious_hash', 'malicious_name', 'nested_status'])
def test_worker_serializer_refuses_untyped_receipts_without_output(mutation, offline_guard, capsys):
    payload = closed_payload()
    assert offline_guard._d1_payload(payload) is True
    receipt = payload['receipt']
    class Malicious:
        def __str__(self):
            raise AssertionError('serializer called str on an unknown value')
        def __repr__(self):
            raise AssertionError('serializer called repr on an unknown value')
    mutations = {
        'enum': lambda: receipt.update(commit_state='D1_CANARY_ENUM_a32b'),
        'hash': lambda: receipt.update(final_observation={'exists': True, 'sha256': 'not-a-digest', 'bytes': 1}),
        'bytes': lambda: receipt.update(prepared_candidate={'sha256': 'b' * 64, 'bytes': -1}),
        'boolean_bytes': lambda: receipt.update(prepared_candidate={'sha256': 'b' * 64, 'bytes': True}),
        'identifier': lambda: receipt.update(operation_id='D1_CANARY_ID_652d'),
        'pathname': lambda: receipt.update(file_ref='C:\\private\\D1_CANARY_PATH_28f6.csv'),
        'nested': lambda: receipt.update(final_observation={'exists': True, 'sha256': 'b' * 64, 'bytes': 1,
            'nested': {'Authorization': 'D1_CANARY_TOKEN_d472'}}),
        'error': lambda: receipt.update(errors=[{'stage': 'runtime', 'code': 'D1_ERROR', 'message': 'D1_CANARY_ERROR_842d'}]),
        'null_reason': lambda: receipt.update(null_reasons={'final_observation': 'D1_CANARY_REASON_982c'}),
        'sequence': lambda: receipt.update(mandatory_sequences=[True]),
        'barrier': lambda: receipt.update(barrier_sequence=True),
        'status': lambda: payload.update(status='D1_CANARY_STATUS_547a'),
        'extra': lambda: receipt.update(token='D1_CANARY_EXTRA_437a'),
        'observed_hash': lambda: payload.update(observed_sha256='D1_CANARY_HASH_f2b1'),
        'malicious_hash': lambda: payload.update(observed_sha256=Malicious()),
        'malicious_name': lambda: payload.update(mutex_name=Malicious()),
        'nested_status': lambda: payload.update(status={'token': 'D1_CANARY_STATUS_329d'}),
    }
    mutations[mutation]()
    assert offline_guard._d1_payload(payload) is False
    captured = capsys.readouterr()
    assert captured.out == '' and captured.err == ''


@pytest.mark.parametrize('loader', ['cdll', 'windll'])
def test_alternative_native_loaders_refused(loader, offline_guard, project_root):
    context = offline_guard.ensure_context(project_root)
    before = context.counts.get('native', 0)
    journal_count = len(context.violations)
    with offline_guard.expect_fault('native'):
        with pytest.raises(offline_guard.OfflineViolation, match='F3_OFFLINE_NATIVE'):
            if loader == 'cdll':
                ctypes.CDLL('kernel32')
            else:
                ctypes.windll.kernel32
    assert context.counts.get('native', 0) - before == 1
    assert len(context.violations) - journal_count == 1
    record = context.violations[-1]
    assert record['kind'] == 'native'
    assert record['nodeid'] == 'tests/d1/test_guard_admission.py::test_alternative_native_loaders_refused[' + loader + ']'
    path = context.report_dir / 'processes' / (context.process_id + '.violation.' + str(record['sequence']) + '.json')
    assert json.loads(path.read_text(encoding='utf-8')) == record


READY_FAULTS = [(phase, mutation) for phase in ('TARGET_READY', 'CANDIDATE_READY', 'POSTCOMMIT_READY')
    for mutation in ('empty', 'receipt', 'ownership', 'admission', 'diagnostics', 'barrier', 'entry', 'commit', 'sequences')]
READY_FAULTS += [('CANDIDATE_READY', 'baseline'), ('CANDIDATE_READY', 'observation'), ('POSTCOMMIT_READY', 'candidate')]


@pytest.mark.parametrize('phase,mutation', READY_FAULTS)
def test_authenticated_invalid_ready_never_authorizes_death(phase, mutation, offline_guard, project_root, oracle):
    payload = closed_payload()
    receipt = payload['receipt']
    entry = {'exists': True, 'sha256': oracle['previous']['sha256'], 'bytes': oracle['previous']['bytes']}
    receipt.update(lock_observation='NORMAL', lock_state='OWNED', target_state='PRESENT',
        target_validation='ADMITTED', lease_entry_observation=entry, diagnostics_state='PRECOMMIT_CONFIRMED',
        mandatory_sequences=[1, 2], barrier_sequence=3)
    payload.update(validator_calls=1, mutex_name='Global\\MDS-D1-v1-' + 'c' * 64,
        observed_sha256=entry['sha256'], observed_bytes=entry['bytes'])
    if phase != 'TARGET_READY':
        receipt['attempt_baseline'] = dict(entry)
    if phase == 'CANDIDATE_READY':
        partial = b'ticker,begin,close\npartial'
        payload.update(observed_sha256=hashlib.sha256(partial).hexdigest(), observed_bytes=len(partial))
    if phase == 'POSTCOMMIT_READY':
        receipt.update(commit_state='REPLACED', prepared_candidate={key: oracle['replacement'][key] for key in ('sha256', 'bytes')},
            mandatory_sequences=[1, 2, 3, 4, 5], barrier_sequence=6)
        payload['status'] = 'SUCCEEDED'
    nullable = ('lease_entry_observation', 'attempt_baseline', 'final_observation', 'prepared_candidate', 'barrier_sequence')
    receipt['null_reasons'] = {key: 'NOT_REACHED' for key in nullable if receipt[key] is None}
    mode = 'hold_after_replace' if phase == 'POSTCOMMIT_READY' else 'hold_partial'
    assert offline_guard._d1_phase_payload(phase, payload, mode) is True
    if mutation == 'empty':
        payload = None
    elif mutation == 'receipt':
        payload['receipt'] = None
    elif mutation == 'ownership':
        receipt['lock_state'] = 'RELEASED'
    elif mutation == 'admission':
        receipt['target_validation'] = 'REJECTED'
    elif mutation == 'diagnostics':
        receipt['diagnostics_state'] = 'NOT_CONFIRMED'
    elif mutation == 'barrier':
        receipt['barrier_sequence'] = receipt['mandatory_sequences'][-1]
    elif mutation == 'entry':
        receipt['lease_entry_observation'] = None
        receipt['null_reasons']['lease_entry_observation'] = 'NOT_REACHED'
    elif mutation == 'commit':
        receipt['commit_state'] = 'NOT_ATTEMPTED' if phase == 'POSTCOMMIT_READY' else 'REPLACED'
    elif mutation == 'baseline':
        receipt['attempt_baseline'] = None
        receipt['null_reasons']['attempt_baseline'] = 'NOT_REACHED'
    elif mutation == 'observation':
        payload['observed_sha256'] = 'b' * 64
    elif mutation == 'candidate':
        receipt['prepared_candidate'] = None
        receipt['null_reasons']['prepared_candidate'] = 'NOT_REACHED'
    elif mutation == 'sequences':
        receipt['mandatory_sequences'] = []
    context = offline_guard.ensure_context(project_root)
    case = 'crash_after_commit' if phase == 'POSTCOMMIT_READY' else 'abandoned_before_commit'
    ticket = {key: getattr(context, key) for key in ('run_id', 'tested_sha', 'source_mode', 'source_digest', 'policy_digest', 'bootstrap_digest')}
    ticket.update(ticket_id='d' * 32, report_dir=str(context.report_dir),
        nodeid='tests/d1/test_resource_lock.py::test_process_cases[' + case + ']',
        d1_contract={'case': case, 'slot': 'holder', 'mode': mode, 'nonce': 'e' * 32, 'worker_digest': 'f' * 64})
    child = {'ticket': ticket, 'process': SimpleNamespace(pid=4321), 'sequence': 2,
        'last_phase': 'ARMED' if phase == 'TARGET_READY' else 'TARGET_READY', 'frames': [], 'ready': True}
    header = offline_guard._d1_header(ticket, 4321)
    frame = dict(header, phase=phase, sequence=3, payload=payload)
    assert offline_guard._d1_phase_payload(phase, payload, mode) is False
    with pytest.raises(ValueError, match='D1_FRAME_FORMAT'):
        offline_guard._d1_frame(child, json.dumps(frame).encode())
    assert child['frames'] == [] and child['sequence'] == 2
    with pytest.raises(ValueError, match='D1_TERMINATION_SCOPE'):
        offline_guard._d1_terminate(child, 'POSTCOMMIT_READY' if phase == 'POSTCOMMIT_READY' else 'CANDIDATE_READY')
    assert not (context.report_dir / 'tickets' / ('d' * 32 + '.d1-termination.json')).exists()


def capture_policy_refusals(guard, monkeypatch):
    # Unit policy seams use no real native/descendant delegate or fault waiver.
    refusals = []
    def refuse(kind):
        refusals.append(kind)
        raise guard.OfflineViolation('D1_SYNTHETIC_POLICY_REFUSAL')
    monkeypatch.setattr(guard, 'reject', refuse)
    return refusals


@pytest.mark.parametrize('channel,bound', [('stdout', 65536), ('stderr', 131072)])
def test_pipe_overrun_retains_exact_bounded_evidence(channel, bound, offline_guard):
    raw = b'x' * 20 + b'\n'
    process = SimpleNamespace(**{channel: io.BytesIO(raw)})
    child = {'process': process, channel: bytearray(b'p' * (bound - 10)),
        'drain_errors': [], 'stdout_closed': threading.Event(), 'queue': queue.Queue(maxsize=32)}
    offline_guard._d1_drain(child, channel)
    assert bytes(child[channel]) == b'p' * (bound - 10) + raw[:10]
    assert len(child[channel]) == bound
    assert child['drain_errors'] == ['D1_' + channel.upper() + '_BOUND']
    assert child['queue'].empty()
    assert child['stdout_closed'].is_set() is (channel == 'stdout')


@pytest.mark.parametrize('kind', ['file', 'directory'])
def test_only_directory_identity_authorizes_mutex_name(kind, offline_guard, monkeypatch, target):
    refusals = capture_policy_refusals(offline_guard, monkeypatch)
    handles = {0xd111: {'kind': kind, 'path': target if kind == 'file' else target.parent}}
    names = set()
    monkeypatch.setattr(offline_guard, '_D1_NATIVE_HANDLES', handles)
    monkeypatch.setattr(offline_guard, '_D1_MUTEX_NAMES', names)
    raw = bytes.fromhex('0807060504030201000102030405060708090a0b0c0d0e0f')
    identity = ctypes.create_string_buffer(raw)
    query_calls, mutex_calls = [], []
    def query(*args):
        query_calls.append(args)
        return 1
    expected = 'Global\\MDS-D1-v1-' + hashlib.sha256(bytes.fromhex(
        '4d44532d44312d7631000807060504030201000102030405060708090a0b0c0d0e0f')).hexdigest()
    args = (0xd111, 18, ctypes.byref(identity), 24)
    assert offline_guard._d1_native_call('GetFileInformationByHandleEx', query, args) == 1
    assert query_calls == [args]
    assert names == ({expected} if kind == 'directory' else set())
    def create(*args):
        mutex_calls.append(args)
        return 0xd112
    if kind == 'file':
        with pytest.raises(offline_guard.OfflineViolation, match='D1_SYNTHETIC_POLICY_REFUSAL'):
            offline_guard._d1_native_call('CreateMutexW', create, (None, False, expected))
        assert mutex_calls == [] and refusals == ['native']
        assert 0xd112 not in handles
    else:
        assert offline_guard._d1_native_call('CreateMutexW', create, (None, False, expected)) == 0xd112
        assert mutex_calls == [(None, False, expected)] and refusals == []
        assert handles[0xd112] == {'kind': 'mutex', 'name': expected, 'owned': False}


def test_controller_active_does_not_authorize_unissued_job(offline_guard, monkeypatch):
    refusals = capture_policy_refusals(offline_guard, monkeypatch)
    handles, calls = {}, []
    monkeypatch.setattr(offline_guard, '_D1_NATIVE_HANDLES', handles)
    monkeypatch.setattr(offline_guard, '_D1_CONTROLLER_ACTIVE', True)
    monkeypatch.setattr(offline_guard._THREAD_STATE, 'pending', [], raising=False)
    def create(*args):
        calls.append(args)
        return 0xd121
    with pytest.raises(offline_guard.OfflineViolation, match='D1_SYNTHETIC_POLICY_REFUSAL'):
        offline_guard._d1_native_call('CreateJobObjectW', create, (None, None))
    assert calls == [] and handles == {} and refusals == ['native']
    token = {'role': 'd1-worker', 'ticket_id': 'a' * 32}
    monkeypatch.setattr(offline_guard._THREAD_STATE, 'pending', [token])
    assert offline_guard._d1_native_call('CreateJobObjectW', create, (None, None)) == 0xd121
    assert calls == [(None, None)]
    assert handles == {0xd121: {'kind': 'job', 'ticket_id': token['ticket_id']}}


@pytest.mark.parametrize('exit_code', [0, 1, 255])
def test_failed_launch_cleanup_requires_exit_124(exit_code, offline_guard, monkeypatch):
    refusals = capture_policy_refusals(offline_guard, monkeypatch)
    token = {'role': 'd1-worker', 'ticket_id': 'b' * 32}
    handles = {0xd131: {'kind': 'process', 'ticket_id': token['ticket_id']}}
    calls = []
    monkeypatch.setattr(offline_guard, '_D1_NATIVE_HANDLES', handles)
    monkeypatch.setattr(offline_guard._THREAD_STATE, 'pending', [token], raising=False)
    def terminate(*args):
        calls.append(args)
        return 1
    with pytest.raises(offline_guard.OfflineViolation, match='D1_SYNTHETIC_POLICY_REFUSAL'):
        offline_guard._d1_native_call('TerminateProcess', terminate, (0xd131, exit_code))
    assert calls == [] and refusals == ['native']
    assert offline_guard._d1_native_call('TerminateProcess', terminate, (0xd131, 124)) == 1
    assert calls == [(0xd131, 124)]


@pytest.mark.parametrize('role', ['f2-probe', 'flog-probe'])
def test_old_role_report_errors_keep_original_order(role, offline_guard, project_root, tmp_path):
    context = offline_guard.ensure_context(project_root)
    report_dir = tmp_path / 'synthetic-reports'
    (report_dir / 'processes').mkdir(parents=True)
    (report_dir / 'tickets').mkdir()
    ticket = {key: getattr(context, key) for key in ('run_id', 'tested_sha', 'source_mode',
        'source_digest', 'bootstrap_digest', 'policy_digest')}
    ticket.update(ticket_id='c' * 32, report_dir=str(report_dir), lane='f2' if role == 'f2-probe' else 'flog',
        mode='offline', role=role, parent_id=context.process_id, argv_digest='d' * 64)
    errors, final = offline_guard._validate_ticket(ticket)
    assert errors == ['MISSING_START_REPORT', 'MISSING_FINAL_REPORT', 'MISSING_CHILD_EXIT']
    assert final is None


@pytest.mark.parametrize('operation', ['notify_child', 'register_fixture_script'])
def test_worker_descendant_admission_refuses_before_arguments(operation, offline_guard, project_root, monkeypatch):
    context = offline_guard.ensure_context(project_root)
    counts, violations = dict(context.counts), list(context.violations)
    refusals = capture_policy_refusals(offline_guard, monkeypatch)
    monkeypatch.setattr(context, 'role', 'd1-worker')
    class Poison:
        def __iter__(self):
            raise AssertionError('descendant arguments were interpreted before refusal')
        def __fspath__(self):
            raise AssertionError('descendant path was interpreted before refusal')
        def __str__(self):
            raise AssertionError('descendant argument was stringified before refusal')
    with pytest.raises(offline_guard.OfflineViolation, match='D1_SYNTHETIC_POLICY_REFUSAL'):
        if operation == 'notify_child':
            offline_guard.notify_child(Poison(), cwd=Poison(), env=Poison())
        else:
            offline_guard.register_fixture_script(Poison(), Poison())
    assert refusals == ['children']
    assert context.counts == counts and context.violations == violations
