"""Independent byte, state and delivery regression checks for one-file writes."""

import importlib
import errno
import hashlib
import io
import json
import os
import math
import traceback
import zipfile

import pytest


RECEIPT_KEYS = {'receipt_version', 'operation_id', 'file_ref', 'lock_observation',
    'lock_state', 'target_state', 'target_validation', 'lease_entry_observation',
    'attempt_baseline', 'final_observation', 'prepared_candidate', 'commit_state',
    'final_relation', 'write_status', 'diagnostics_state', 'mandatory_sequences',
    'barrier_sequence', 'errors', 'null_reasons'}


def observation(expected):
    return {'exists': True, 'sha256': expected['sha256'], 'bytes': expected['bytes']}


def write(helper, acquire, data, receipt, validate=None):
    with acquire(receipt=receipt) as lease:
        snapshot = helper.atomic_write_file(lease, lambda stream: stream.write(data),
            validate or (lambda stream: stream.read() == data))
        assert snapshot is not receipt
    return snapshot


def test_public_api_exists():
    helper = importlib.import_module('dataset_io')
    assert callable(getattr(helper, 'resource_lock', None))
    assert callable(getattr(helper, 'atomic_write_file', None))


def test_success_snapshot_and_closed_receipt(helper, acquire, target, oracle, assert_artifact, logging_session, project_root):
    receipt = {}
    snapshot = write(helper, acquire, oracle['replacement']['data'], receipt)
    assert_artifact(target, 'replacement')
    assert set(receipt) == RECEIPT_KEYS
    assert receipt['lease_entry_observation'] == observation(oracle['previous'])
    assert receipt['attempt_baseline'] == observation(oracle['previous'])
    assert receipt['final_observation'] == observation(oracle['replacement'])
    assert receipt['prepared_candidate'] == {key: oracle['replacement'][key] for key in ('sha256', 'bytes')}
    assert receipt['commit_state'] == 'REPLACED'
    assert receipt['final_relation'] == 'MATCHES_CANDIDATE'
    assert receipt['write_status'] == 'SUCCEEDED'
    assert receipt['lock_state'] == 'RELEASED'
    assert receipt['diagnostics_state'] == 'COMPLETE'
    assert snapshot['diagnostics_state'] != 'COMPLETE'
    assert max(receipt['mandatory_sequences']) <= receipt['barrier_sequence']
    assert snapshot['lock_state'] == 'OWNED'
    records = []
    for path in (project_root / logging_session['config']['log_root']).rglob('events.jsonl'):
        records.extend(json.loads(line) for line in path.read_text(encoding='utf-8').splitlines())
    events = [record for record in records if record['event'].startswith('d1_')]
    assert [record['event'] for record in events] == ['d1_lock_acquired', 'd1_target_admitted',
        'd1_temp_prepared', 'd1_temp_validated', 'd1_replace_precommit', 'd1_file_replaced', 'd1_lock_released']
    assert all(record['fields'] == {} and record['file'] == receipt['file_ref'] for record in events)
    assert all(record['run_id'] == logging_session['run_id'] for record in events)
    elapsed = [record['elapsed_ms'] for record in events]
    assert all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in elapsed)
    assert elapsed == sorted(elapsed)
    assert all(record['duration_ms'] is None for record in events)


@pytest.mark.parametrize('value', [False, None, 1, 'verified', {}, []], ids=['false', 'none', 'one', 'string', 'dict', 'list'])
def test_candidate_requires_exact_true(value, helper, acquire, target, oracle, assert_artifact, monkeypatch):
    replaces = []
    monkeypatch.setattr(helper.os, 'replace', lambda *args: replaces.append(args))
    receipt = {}
    with pytest.raises(Exception):
        write(helper, acquire, oracle['replacement']['data'], receipt, lambda stream: value)
    assert replaces == []
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert receipt['final_relation'] == 'MATCHES_BASELINE'
    assert receipt['write_status'] == 'FAILED'


@pytest.mark.parametrize('fault', ['partial_csv', 'prepare_exception', 'enospc', 'fsync', 'incomplete_xlsx'])
def test_precommit_faults_preserve_baseline(fault, helper, acquire, target, oracle, assert_artifact, monkeypatch):
    primary = OSError(errno.ENOSPC, 'fixed disk-full fixture') if fault == 'enospc' else RuntimeError('fixed preparation fixture')
    replaces = []
    monkeypatch.setattr(helper.os, 'replace', lambda *args: replaces.append(args))
    receipt = {}
    foreign = target.parent / '.d1-foreign.tmp'
    foreign.write_bytes(b'foreign temp sentinel')
    def prepare(stream):
        stream.write(b'PK\x03\x04truncated' if fault == 'incomplete_xlsx' else b'ticker,begin,close\npartial')
        if fault in ('prepare_exception', 'enospc'):
            raise primary
    def validate(stream):
        if fault == 'incomplete_xlsx':
            try:
                with zipfile.ZipFile(stream) as archive:
                    return archive.testzip() is None
            except zipfile.BadZipFile:
                return False
        return stream.read() == oracle['replacement']['data']
    if fault == 'fsync':
        monkeypatch.setattr(helper.os, 'fsync', lambda fd: (_ for _ in ()).throw(primary))
    with pytest.raises(Exception) as caught:
        with acquire(receipt=receipt) as lease:
            helper.atomic_write_file(lease, prepare, validate)
    if fault in ('prepare_exception', 'enospc', 'fsync'):
        assert caught.value is primary
        assert any(frame.name == 'prepare' for frame in traceback.extract_tb(caught.value.__traceback__)) or fault == 'fsync'
    assert replaces == []
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert receipt['final_relation'] == 'MATCHES_BASELINE'
    assert foreign.read_bytes() == b'foreign temp sentinel'
    assert list(target.parent.glob('.d1-*.tmp')) == [foreign]


def test_receipt_mutation_cannot_replace_private_baseline(helper, acquire, target, oracle, assert_artifact):
    receipt = {}
    with acquire(receipt=receipt) as lease:
        receipt['attempt_baseline'] = {'exists': False, 'sha256': None, 'bytes': None}
        receipt['commit_state'] = 'REPLACED'
        snapshot = helper.atomic_write_file(lease, lambda stream: stream.write(oracle['replacement']['data']), lambda stream: True)
    assert_artifact(target, 'replacement')
    assert snapshot['attempt_baseline'] == observation(oracle['previous'])
    assert receipt['attempt_baseline'] == observation(oracle['previous'])


def test_changed_target_before_replace_is_not_preservation(helper, acquire, target, oracle, monkeypatch):
    receipt = {}
    calls = []
    monkeypatch.setattr(helper.os, 'replace', lambda *args: calls.append(args))
    def validate(stream):
        target.write_bytes(b'concurrent noncooperative mutation')
        return True
    with pytest.raises(Exception):
        write(helper, acquire, oracle['replacement']['data'], receipt, validate)
    assert calls == []
    assert target.read_bytes() == b'concurrent noncooperative mutation'
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert receipt['final_relation'] == 'OTHER'


def test_precommit_accepted_without_confirmed_is_refused(helper, acquire, target, oracle, assert_artifact, monkeypatch):
    import run_logging
    receipt = {}
    with acquire(receipt=receipt) as lease:
        replaces = []
        monkeypatch.setattr(helper.os, 'replace', lambda *args: replaces.append(args))
        monkeypatch.setattr(run_logging, 'flush_logging', lambda session: {'accepted': True, 'confirmed': False, 'sequence': 999})
        with pytest.raises(Exception):
            helper.atomic_write_file(lease, lambda stream: stream.write(oracle['replacement']['data']), lambda stream: True)
        assert replaces == []
        assert receipt['commit_state'] == 'NOT_ATTEMPTED'
        monkeypatch.undo()
    assert_artifact(target, 'previous')


@pytest.mark.parametrize('point', ['event', 'flush', 'health', 'readback', 'release'])
def test_postcommit_failures_keep_replaced(point, helper, acquire, target, oracle, assert_artifact, monkeypatch):
    import run_logging
    primary = RuntimeError('fixed postcommit fault')
    receipt = {}
    real_replace = helper.os.replace
    real_emit, real_flush, real_health = run_logging.emit_event, run_logging.flush_logging, run_logging.check_logging_health
    replaced = False
    def replace(source, destination):
        nonlocal replaced
        real_replace(source, destination)
        replaced = True
    monkeypatch.setattr(helper.os, 'replace', replace)
    if point == 'event':
        def emit(session, *args, **kwargs):
            if replaced:
                raise primary
            return real_emit(session, *args, **kwargs)
        monkeypatch.setattr(run_logging, 'emit_event', emit)
    elif point == 'flush':
        monkeypatch.setattr(run_logging, 'flush_logging', lambda session: (_ for _ in ()).throw(primary) if replaced else real_flush(session))
    elif point == 'health':
        monkeypatch.setattr(run_logging, 'check_logging_health', lambda session: {'healthy': False} if replaced else real_health(session))
    elif point == 'readback':
        monkeypatch.setattr(helper, '_readback', lambda lease: (_ for _ in ()).throw(primary))
    else:
        backend = helper._windows_backend()
        real_release = backend['ReleaseMutex']
        def release(handle):
            real_release(handle)
            return False
        backend['ReleaseMutex'] = release
        monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    with pytest.raises(Exception):
        write(helper, acquire, oracle['replacement']['data'], receipt)
    assert replaced
    assert_artifact(target, 'replacement')
    assert receipt['commit_state'] == 'REPLACED'
    assert receipt['diagnostics_state'] == 'INCOMPLETE'
    assert receipt['write_status'] == 'FAILED'
    assert receipt['final_relation'] != 'MATCHES_BASELINE'
    if point == 'readback':
        assert receipt['final_observation'] is None


def test_primary_exception_survives_secondary_diagnostics(helper, acquire, target, oracle, assert_artifact, monkeypatch):
    import run_logging
    primary = RuntimeError('original fixed application failure')
    receipt = {}
    with pytest.raises(RuntimeError) as caught:
        with acquire(receipt=receipt) as lease:
            monkeypatch.setattr(run_logging, 'emit_event', lambda *args, **kwargs: (_ for _ in ()).throw(OSError('secondary logger failure')))
            def prepare(stream):
                raise primary
            helper.atomic_write_file(lease, prepare, lambda stream: True)
    assert caught.value is primary
    assert any(frame.name == 'prepare' for frame in traceback.extract_tb(caught.value.__traceback__))
    assert_artifact(target, 'previous')
    assert receipt['errors'][0]['stage'] == 'preparation'


def test_measured_postcommit_mismatch_is_retained(helper, acquire, target, oracle, monkeypatch):
    receipt = {}
    real_readback = helper._readback
    foreign = b'D1_MEASURED_FOREIGN_FINAL\n'
    observations = []
    def readback(lease):
        assert receipt['commit_state'] == 'REPLACED'
        # A real postcommit mutation gives an independent measured observation.
        target.write_bytes(foreign)
        measured = real_readback(lease)
        observations.append(measured)
        return measured
    monkeypatch.setattr(helper, '_readback', readback)
    with pytest.raises(ValueError, match='^D1_FINAL_MISMATCH$'):
        write(helper, acquire, oracle['replacement']['data'], receipt)
    expected = {'exists': True, 'sha256': hashlib.sha256(foreign).hexdigest(), 'bytes': len(foreign)}
    assert observations == [expected]
    assert target.read_bytes() == foreign
    assert receipt['lease_entry_observation'] == observation(oracle['previous'])
    assert receipt['attempt_baseline'] == observation(oracle['previous'])
    assert receipt['prepared_candidate'] == {key: oracle['replacement'][key] for key in ('sha256', 'bytes')}
    assert receipt['final_observation'] == expected
    assert 'final_observation' not in receipt['null_reasons']
    assert receipt['final_relation'] == 'OTHER'
    assert receipt['commit_state'] == 'REPLACED'
    assert receipt['write_status'] == 'FAILED'
    assert receipt['diagnostics_state'] == 'INCOMPLETE'
    assert receipt['lock_state'] == 'RELEASED'


def test_directory_identity_query_primary_survives_raised_close(helper, acquire, target, assert_artifact, monkeypatch):
    backend = helper._windows_backend()
    real_close = backend['CloseHandle']
    primary, secondary = RuntimeError('D1_FIXED_IDENTITY_QUERY_FAILURE'), OSError('D1_FIXED_HANDLE_CLOSE_FAILURE')
    handles = []
    def query(handle, *args):
        handles.append(handle)
        raise primary
    def close(handle):
        assert real_close(handle)
        raise secondary
    backend['GetFileInformationByHandleEx'] = query
    backend['CloseHandle'] = close
    monkeypatch.setattr(helper, '_windows_backend', lambda: backend)
    callbacks, receipt = [], {}
    with pytest.raises(RuntimeError) as caught:
        with acquire(validate=lambda stream: callbacks.append(stream) or True, receipt=receipt):
            pytest.fail('failed identity query issued a lease')
    assert caught.value is primary
    assert any(frame.name == 'query' for frame in traceback.extract_tb(caught.value.__traceback__))
    assert len(handles) == 1 and callbacks == []
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert receipt['prepared_candidate'] is None
    assert_artifact(target, 'previous')


def test_real_windows_deny_delete(helper, acquire, target, oracle, assert_artifact, offline_guard):
    receipt = {}
    with offline_guard.d1_native_reader(target, deny_delete=True):
        with pytest.raises(OSError):
            write(helper, acquire, oracle['replacement']['data'], receipt)
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'UNKNOWN'
    assert receipt['final_relation'] == 'MATCHES_BASELINE'


@pytest.mark.parametrize('setting', ['file_level', 'console_level'])
def test_filtered_info_refuses_before_target_write(setting, helper, target, oracle, assert_artifact, log_factory, monkeypatch):
    session = log_factory({setting: 'WARNING'})
    callbacks = []
    receipt = {}
    with pytest.raises(Exception):
        with helper.resource_lock(target.name, resource_root=target.parent, validate_target=lambda stream: callbacks.append('target') or True,
                logging_session=session, receipt=receipt) as lease:
            helper.atomic_write_file(lease, lambda stream: callbacks.append('candidate'), lambda stream: True)
    assert callbacks == []
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'


def test_closed_logging_session_refused(helper, target, logging_session, assert_artifact):
    import run_logging
    run_logging.finalize_logging(logging_session, execution_outcome='COMPLETED', exit_status=0)
    receipt = {}
    calls = []
    with pytest.raises(Exception):
        with helper.resource_lock(target.name, resource_root=target.parent, validate_target=lambda stream: calls.append(1),
                logging_session=logging_session, receipt=receipt):
            pytest.fail('closed session issued lease')
    assert calls == []
    assert_artifact(target, 'previous')


def test_canary_receipt_logs_and_console(helper, target, log_factory, capsys, project_root, monkeypatch):
    literals = ('D1_CANARY_TOKEN_6ac7', 'D1_CANARY_COOKIE_58aa', 'D1_CANARY_PRIVATE_KEY_cdd1', 'D1_CANARY_PATH_08ee')
    session = log_factory(sensitive_values=literals)
    receipt = {}
    class Malicious:
        def __str__(self):
            raise AssertionError('exception __str__ must not be called')
        def __repr__(self):
            raise AssertionError('exception __repr__ must not be called')
    primary = RuntimeError({'Authorization': literals[0], 'cookie': [literals[1], Malicious()],
        'url': 'https://user:' + literals[2] + '@example.invalid/?token=' + literals[0]})
    sensitive_target = target.parent / (literals[3] + '.csv')
    sensitive_target.write_bytes(target.read_bytes())
    def validation(stream):
        raise primary
    with pytest.raises(RuntimeError) as caught:
        with helper.resource_lock(sensitive_target.name, resource_root=target.parent, validate_target=validation,
                logging_session=session, receipt=receipt):
            pytest.fail('raising validator issued lease')
    assert caught.value is primary
    import run_logging
    run_logging.finalize_logging(session, execution_outcome='FAILED', exit_status=1,
        application_error=run_logging.make_error(session, primary, 'runtime'))
    evidence = json.dumps(receipt) + ''.join(capsys.readouterr())
    logging_root = project_root / session['config']['log_root']
    for path in logging_root.rglob('*'):
        if path.is_file():
            evidence += path.read_bytes().decode('utf-8', errors='replace')
    for value in literals:
        assert value not in evidence
    assert str(target.parent) not in json.dumps(receipt)
    assert set(receipt) == RECEIPT_KEYS


def test_independent_oracles_detect_unlocked_lost_update_and_direct_partial(target, oracle, assert_artifact):
    # Two separately captured reads expose the exact lost-update bug without the helper.
    stale_a, stale_b = target.read_bytes(), target.read_bytes()
    target.write_bytes(stale_a + b'A,2024-01-01 10:00:00,200.0\n')
    target.write_bytes(stale_b + b'B,2024-01-01 10:00:00,300.0\n')
    with pytest.raises(AssertionError):
        assert_artifact(target, 'two_updates')
    assert b'A,2024' not in target.read_bytes()
    target.write_bytes(b'ticker,begin,close\npartial')
    with pytest.raises(AssertionError):
        assert_artifact(target, 'previous')


def test_borrowed_streams_and_candidate_mutation(helper, acquire, target, oracle, assert_artifact):
    receipt = {}
    observations = []
    def prepare(stream):
        assert stream.tell() == 0 and stream.writable()
        stream.write(oracle['replacement']['data'])
    def validate(stream):
        assert stream.tell() == 0 and not stream.writable()
        observations.append(stream.read())
        temp = next(target.parent.glob('.d1-*.tmp'))
        temp.write_bytes(b'changed candidate after verification read')
        return True
    with pytest.raises(Exception):
        with acquire(receipt=receipt) as lease:
            helper.atomic_write_file(lease, prepare, validate)
    assert observations == [oracle['replacement']['data']]
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'


def test_parent_identity_change_before_commit_refuses(helper, acquire, target, oracle, assert_artifact, monkeypatch):
    receipt = {}
    original = helper._parent_identity
    changed = False
    def identity(path, backend):
        serial, identifier = original(path, backend)
        return (serial, bytes([identifier[0] ^ 1]) + identifier[1:]) if changed else (serial, identifier)
    def candidate_validator(stream):
        nonlocal changed
        changed = True
        return True
    monkeypatch.setattr(helper, '_parent_identity', identity)
    with pytest.raises(ValueError, match='D1_PARENT_CHANGED'):
        write(helper, acquire, oracle['replacement']['data'], receipt, candidate_validator)
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'


def test_foreign_inode_at_owned_temp_name_is_never_cleaned_or_promoted(helper, acquire, target, oracle, assert_artifact, monkeypatch):
    original = helper._observe
    swapped = []
    foreign = b'D1_FOREIGN_TEMP_SENTINEL\n'
    def observe(path, backend):
        candidate = __import__('pathlib').Path(path)
        if candidate.suffix == '.tmp' and not swapped:
            saved = candidate.with_name('owned-candidate.saved')
            candidate.replace(saved)
            candidate.write_bytes(foreign)
            swapped.append((candidate, saved))
        return original(path, backend)
    monkeypatch.setattr(helper, '_observe', observe)
    receipt = {}
    with pytest.raises(ValueError, match='D1_TEMP_IDENTITY_CHANGED'):
        write(helper, acquire, oracle['replacement']['data'], receipt)
    assert len(swapped) == 1
    candidate, saved = swapped[0]
    assert candidate.read_bytes() == foreign
    assert saved.read_bytes() == oracle['replacement']['data']
    assert_artifact(target, 'previous')
    assert receipt['commit_state'] == 'NOT_ATTEMPTED'
    assert any(error['code'] == 'D1_TEMP_CLEANUP_FAILED' for error in receipt['errors'])


@pytest.mark.parametrize('point', ['emit_none_producer', 'emit_bool_producer', 'emit_large_producer',
    'emit_zero_sequence', 'flush_none_producer', 'flush_bool_sequence', 'flush_earlier_sequence'])
def test_malformed_delivery_cannot_authorize_commit(point, helper, acquire, target, oracle, assert_artifact, monkeypatch):
    import run_logging
    receipt = {}
    with acquire(receipt=receipt) as lease:
        if point.startswith('emit_'):
            original = run_logging.emit_event
            def emit(*args, **kwargs):
                result = original(*args, **kwargs)
                field, value = {'emit_none_producer': ('producer', None), 'emit_bool_producer': ('producer', True),
                    'emit_large_producer': ('producer', 2**64), 'emit_zero_sequence': ('sequence', 0)}[point]
                return dict(result, **{field: value})
            monkeypatch.setattr(run_logging, 'emit_event', emit)
        else:
            original = run_logging.flush_logging
            def flush(session):
                result = original(session)
                field, value = {'flush_none_producer': ('producer', None), 'flush_bool_sequence': ('sequence', True),
                    'flush_earlier_sequence': ('sequence', receipt['mandatory_sequences'][-1])}[point]
                return dict(result, **{field: value})
            monkeypatch.setattr(run_logging, 'flush_logging', flush)
        with pytest.raises(Exception):
            helper.atomic_write_file(lease, lambda stream: stream.write(oracle['replacement']['data']), lambda stream: True)
        assert receipt['commit_state'] == 'NOT_ATTEMPTED'
        monkeypatch.undo()
    assert_artifact(target, 'previous')
