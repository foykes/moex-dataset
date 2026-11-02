"""Rejected admission has no launch or private-data side effects."""

import ctypes
import json
import os
import sys
import types
import pytest


def test_unknown_case_refused(offline_guard):
    before = len(offline_guard._CONTEXT.children)
    with pytest.raises(ValueError, match='D2_ADMISSION_CASE'):
        offline_guard.run_d2_case('arbitrary-case')
    assert len(offline_guard._CONTEXT.children) == before


def test_wrong_node_refused(offline_guard):
    before = len(offline_guard._CONTEXT.children)
    with pytest.raises(ValueError, match='D2_ADMISSION_CASE'):
        offline_guard.run_d2_case('success')
    assert len(offline_guard._CONTEXT.children) == before


@pytest.mark.parametrize('lane, role', [
    ('f1', 'worker'), ('f1', 'lane-worker'),
    ('f2', 'worker'), ('f2', 'lane-worker'),
    ('flog', 'worker'), ('flog', 'lane-worker'),
    ('f3', 'worker'), ('f3', 'lane-worker'),
    ('d1', 'worker'), ('d1', 'lane-worker'),
])
def test_legacy_lane_refuses_d2_child_command(offline_guard, monkeypatch, lane, role):
    c = offline_guard._CONTEXT
    command = [sys.executable, '-I', '-B', '-X', 'utf8',
        str(c.root / 'tools/offline_tests.py'), '--execute-lane', lane]
    with monkeypatch.context() as scoped:
        scoped.setattr(c, 'lane', lane)
        scoped.setattr(c, 'role', 'supervisor')
        assert offline_guard._allowed_command(command, role) is True
        command[-1] = 'd2'
        assert offline_guard._allowed_command(command, role) is False


def test_fixture_profile_requires_issued_controller(offline_guard):
    assert offline_guard._d2_fixture_profile_admitted('success') is False
    assert offline_guard._d2_fixture_profile_admitted('unregistered') is False


@pytest.mark.parametrize('role, binary', [
    ('lane-worker', 'foreign'), ('lane-worker', 'relative'), ('lane-worker', 'empty'),
    ('d2-controller', 'foreign'), ('d2-controller', 'relative'), ('d2-controller', 'empty'),
])
def test_d2_child_command_requires_pinned_interpreter(offline_guard, tmp_path, monkeypatch, role, binary):
    c = offline_guard._CONTEXT
    if role == 'lane-worker':
        parent_role = 'supervisor'
        target = [str(c.root / 'tools/offline_tests.py'), '--execute-lane', 'd2']
    else:
        parent_role = 'lane-worker'
        target = [str(c.root / 'tests/d2/d2_worker.py'), '--case', 'success']
    command = [sys.executable, '-I', '-B', '-X', 'utf8', *target]
    before = len(c.children)
    with monkeypatch.context() as scoped:
        scoped.setattr(c, 'role', parent_role)
        assert offline_guard._allowed_command(command, role) is True
        if binary == 'foreign':
            command[0] = str(tmp_path / 'foreign-python.exe')
        elif binary == 'relative':
            command[0] = 'python.exe'
        else:
            command = []
        assert offline_guard._allowed_command(command, role) is False
    assert len(c.children) == before
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('worker_id', ['google', 'ftp', 'holder', 'contender'])
def test_controller_role_requires_controller_slot(offline_guard, tmp_path, monkeypatch, worker_id):
    c = offline_guard._CONTEXT
    contract = dict(case='success', worker_id='controller', result_root=str(tmp_path),
        log_root=str(tmp_path / 'logging'))
    with monkeypatch.context() as scoped:
        scoped.setattr(c, 'role', 'lane-worker')
        scoped.setattr(c, 'nodeid', offline_guard._d2_node('success'), raising=False)
        scoped.setattr(c, 'children', [])
        offline_guard._d2_validate_contract('d2-controller', contract)
        contract['worker_id'] = worker_id
        with pytest.raises(ValueError, match='D2_ADMISSION_ROLE_CASE'):
            offline_guard._d2_validate_contract('d2-controller', contract)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('mutation', ['function_identity', 'keyword', 'worker', 'case', 'root'])
def test_fixture_process_arguments_are_closed(offline_guard, project_root, mutation):
    import d2_worker
    root = str(project_root / '.f3/d2/tmp/never-launched-admission')
    process = types.SimpleNamespace(_target=d2_worker.baseline_worker_entry,
        _args=('google', 'baseline_exit23', root), _kwargs={})
    if mutation == 'function_identity':
        def impostor(*arguments):
            raise AssertionError('must never run')
        impostor.__name__, impostor.__module__ = 'baseline_worker_entry', 'd2_worker'
        process._target = impostor
    elif mutation == 'keyword':
        process._kwargs = {'arbitrary': True}
    elif mutation == 'worker':
        process._args = ('ftp', 'baseline_exit23', root)
    elif mutation == 'case':
        process._args = ('google', 'success', root)
    else:
        process._args = ('google', 'baseline_exit23', str(project_root))
    before = len(offline_guard._CONTEXT.children)
    with pytest.raises(ValueError, match='D2_FIXTURE_START_TARGET'):
        offline_guard._d2_validate_fixture_process(process, 'd2-baseline-worker', 'google',
            'baseline_exit23', {'result_root': root})
    assert len(offline_guard._CONTEXT.children) == before


def test_upload_arguments_require_exact_supervisor_frame(offline_guard):
    before = len(offline_guard._CONTEXT.children)
    with pytest.raises(ValueError, match='D2_UPLOAD_START_ARGUMENTS'):
        offline_guard._d2_validate_upload_arguments(types.SimpleNamespace(_args=()), 'google', 'success')
    assert len(offline_guard._CONTEXT.children) == before


@pytest.mark.parametrize('loader', ['cdll', 'windll'])
def test_alternative_native_loaders_refused(offline_guard, loader):
    with offline_guard.expected_fault('native'):
        with pytest.raises(offline_guard.OfflineViolation):
            getattr(ctypes, loader).LoadLibrary('kernel32')


@pytest.mark.parametrize('mutation', ['foreign_run', 'wrong_pid', 'wrong_count', 'unknown_name'])
def test_fault_declaration_requires_exact_ticket_phase_and_pid(offline_guard, tmp_path, mutation):
    c = offline_guard._CONTEXT
    tickets, processes = tmp_path / 'tickets', tmp_path / 'processes'
    tickets.mkdir()
    processes.mkdir()
    identifier = 'a' * 32
    node = 'tests/d2/test_supervision.py::test_native_case[exit23]'
    ticket = dict(ticket_id=identifier, run_id=c.run_id, lane='d2', role='d2-worker',
        tested_sha=c.tested_sha, source_digest=c.source_digest, nodeid=node,
        d2_contract=dict(case='exit23', worker_id='google'))
    ticket['digest'] = offline_guard._digest(ticket)
    record = dict(version=1, case='exit23', worker_id='google', phase='target_exit23', count=1,
        pid=12345, source='worker', ticket_id=identifier, run_id=c.run_id, nodeid=node)
    start = dict(ticket_id=identifier, run_id=c.run_id, tested_sha=c.tested_sha,
        source_digest=c.source_digest, pid=12345)
    path = tickets / (identifier + '.d2-fault-google-target_exit23.json')
    (tickets / (identifier + '.json')).write_text(json.dumps(ticket), encoding='utf-8')
    (processes / (identifier + '.start.json')).write_text(json.dumps(start), encoding='utf-8')
    path.write_text(json.dumps(record), encoding='utf-8')
    assert offline_guard._d2_read_declaration(path) == record
    if mutation == 'foreign_run':
        record['run_id'] = 'foreign'
    elif mutation == 'wrong_pid':
        record['pid'] += 1
    elif mutation == 'wrong_count':
        record['count'] = 2
    else:
        path = tickets / (identifier + '.d2-arbitrary.json')
    path.write_text(json.dumps(record), encoding='utf-8')
    with pytest.raises(ValueError, match='D2_'):
        offline_guard._d2_read_declaration(path)


def test_checkpoint_reader_native_lifetime(upload, offline_guard, tmp_path, monkeypatch):
    path, pending = tmp_path / 'google.json', tmp_path / 'google.pending'
    path.write_bytes(b'old checkpoint')
    pending.write_bytes(b'new checkpoint')
    assert upload._d2_read_json_bytes(path, 3) == b'old '
    row = offline_guard._CONTEXT.d2_reader_history[-1]
    assert row == dict(handle_created=True, descriptor_transferred=True,
        native_close_calls=0, fd_close_calls=1, descriptor_closed=True,
        stream_closed=True, fdopen_events=1, lifetime_complete=True,
        post_close_observation='EBADF')
    assert offline_guard._THREAD_STATE.d2_reader is None
    # The previous owner's real close precedes legacy MoveFileEx replacement.
    os.replace(pending, path)
    assert upload._d2_read_json_bytes(path, 20) == b'new checkpoint'
    assert offline_guard._CONTEXT.d2_reader_history[-1]['lifetime_complete'] is True
    assert offline_guard._CONTEXT.d2_reader_history[-1]['fd_close_calls'] == 1


def test_checkpoint_reader_open_destination_conflict_is_negative(upload, offline_guard, tmp_path, monkeypatch):
    path, pending = tmp_path / 'google.json', tmp_path / 'google.pending'
    path.write_bytes(b'old checkpoint')
    pending.write_bytes(b'new checkpoint')
    retained_fdopen = offline_guard._ORIGINAL_FDOPEN
    observed = []
    def conflict_after_open(descriptor, *arguments, **options):
        stream = retained_fdopen(descriptor, *arguments, **options)
        try:
            try:
                os.replace(pending, path)
            except OSError as error:
                observed.append((error.errno, error.winerror, not stream.closed))
            else:
                raise AssertionError('LEGACY_REPLACE_OPEN_DESTINATION_DID_NOT_FAIL')
            return stream
        except BaseException:
            stream.close()
            raise
    with monkeypatch.context() as scoped:
        scoped.setattr(offline_guard, '_ORIGINAL_FDOPEN', conflict_after_open)
        assert upload._d2_read_json_bytes(path, 20) == b'old checkpoint'
    assert observed == [(13, 5, True)]
    assert offline_guard._THREAD_STATE.d2_reader is None
    row = offline_guard._CONTEXT.d2_reader_history[-1]
    assert row['lifetime_complete'] is True and row['fd_close_calls'] == 1
    assert path.read_bytes() == b'old checkpoint'
    assert pending.read_bytes() == b'new checkpoint'
    os.replace(pending, path)
    assert upload._d2_read_json_bytes(path, 20) == b'new checkpoint'


@pytest.mark.parametrize('fault', ['worker_role', 'live_slot'])
def test_checkpoint_reader_role_and_phase_denied_before_metadata(upload, offline_guard, tmp_path, monkeypatch, fault):
    root = tmp_path / 'results'
    root.mkdir()
    path = root / 'google.json'
    path.write_bytes(b'fixed checkpoint')
    effects = []
    retained_lstat = offline_guard._ORIGINAL_LSTAT
    def leaf_metadata(selected, *arguments, **options):
        if os.path.abspath(selected) == str(path):
            effects.append('METADATA')
        return retained_lstat(selected, *arguments, **options)
    before = dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {}))
    with monkeypatch.context() as scoped:
        c = offline_guard._CONTEXT
        scoped.setattr(offline_guard, '_ORIGINAL_LSTAT', leaf_metadata)
        scoped.setattr(c, 'role', 'd2-worker' if fault == 'worker_role' else 'd2-controller')
        scoped.setattr(c, 'ticket', {'d2_contract': dict(case='success', worker_id='google', result_root=str(tmp_path))})
        if fault == 'live_slot':
            scoped.setattr(c, 'd2_go', types.SimpleNamespace(value=1), raising=False)
            scoped.setattr(c, 'children', [])
        with pytest.raises(RuntimeError, match='D2_CHECKPOINT_INVALID'):
            upload._d2_read_json_bytes(path, 20)
    assert effects == []
    assert dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {})) == before
    assert getattr(offline_guard._THREAD_STATE, 'd2_reader', None) is None


@pytest.mark.parametrize('operation', ['metadata', 'open', 'full_inventory'])
def test_worker_live_sibling_slot_refused_before_effect(upload, offline_guard, tmp_path, monkeypatch, operation):
    root = tmp_path / 'results'
    root.mkdir()
    own, sibling = root / 'google.json', root / 'ftp.json'
    own.write_bytes(b'D2_OWN_SLOT_PRESERVED')
    sibling.write_bytes(b'D2_LIVE_SIBLING_PRESERVED')
    effects, own_effects = [], []
    retained_lstat = offline_guard._ORIGINAL_LSTAT
    retained_open = offline_guard._ORIGINAL_OS_OPEN
    retained_scandir = os.scandir
    def metadata(selected, *arguments, **options):
        if os.path.abspath(selected) == str(sibling):
            effects.append('METADATA')
        elif os.path.abspath(selected) == str(own):
            own_effects.append('METADATA')
        return retained_lstat(selected, *arguments, **options)
    def opened(selected, *arguments, **options):
        if os.path.abspath(selected) == str(sibling):
            effects.append('OPEN')
        elif os.path.abspath(selected) == str(own):
            own_effects.append('OPEN')
        return retained_open(selected, *arguments, **options)
    def scanned(selected, *arguments, **options):
        if os.path.abspath(selected) == str(root):
            effects.append('FULL_INVENTORY')
        return retained_scandir(selected, *arguments, **options)
    observed = {}
    with monkeypatch.context() as scoped:
        c = offline_guard._CONTEXT
        scoped.setattr(c, 'role', 'd2-worker')
        scoped.setattr(c, 'ticket', {'d2_contract': dict(case='success', worker_id='google', result_root=str(root))})
        scoped.setattr(c, 'd2_slot_effects', dict(own_metadata=0, own_opens=0,
            peer_slot_attempts=0, admitted_writes=0), raising=False)
        scoped.setattr(offline_guard, '_ORIGINAL_LSTAT', metadata)
        scoped.setattr(offline_guard, '_ORIGINAL_OS_OPEN', opened)
        scoped.setattr(os, 'scandir', scanned)
        assert os.lstat(own).st_size == len(b'D2_OWN_SLOT_PRESERVED')
        descriptor = os.open(own, os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT)
        os.close(descriptor)
        if operation == 'metadata':
            with pytest.raises(ValueError, match='D2_PEER_SLOT_EFFECT_REFUSED'):
                os.lstat(sibling)
        elif operation == 'open':
            with pytest.raises(ValueError, match='D2_PEER_SLOT_EFFECT_REFUSED'):
                os.open(sibling, os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT)
        else:
            with pytest.raises(RuntimeError, match='D2_FULL_INVENTORY_PHASE'):
                upload._d2_run_inventory(root, upload._d2_validate_limits())
        observed.update(c.d2_slot_effects)
    assert effects == []
    assert {'METADATA', 'OPEN'} <= set(own_effects)
    assert observed['peer_slot_attempts'] == int(operation != 'full_inventory')
    assert observed['own_metadata'] > 0 and observed['own_opens'] > 0
    assert own.read_bytes() == b'D2_OWN_SLOT_PRESERVED'
    assert sibling.read_bytes() == b'D2_LIVE_SIBLING_PRESERVED'
    assert {path.name for path in root.iterdir()} == {'google.json', 'ftp.json'}


@pytest.mark.parametrize('phase', ['create', 'conversion', 'fdopen', 'fdopen_after_audit',
    'read', 'foreign_fd', 'foreign_handle'])
def test_checkpoint_reader_failed_phase_closes_owner(upload, offline_guard, tmp_path, monkeypatch, phase):
    path = tmp_path / 'google.json'
    path.write_bytes(b'fixed checkpoint')
    foreign = os.open(path, os.O_RDONLY) if phase in {'foreign_fd', 'foreign_handle'} else None
    retained_fdopen = offline_guard._ORIGINAL_FDOPEN
    def failure(*arguments, **options):
        raise OSError('D2_FIXED_READER_FAILURE')
    if phase == 'create':
        monkeypatch.setattr(offline_guard, 'd2_checkpoint_create_file', failure)
    elif phase == 'conversion':
        monkeypatch.setattr(offline_guard, 'd2_checkpoint_open_osfhandle', failure)
    elif phase == 'fdopen':
        monkeypatch.setattr(offline_guard, '_ORIGINAL_FDOPEN', failure)
    elif phase == 'fdopen_after_audit':
        def failed_construction(descriptor, *arguments, **options):
            stream = retained_fdopen(descriptor, *arguments, **options)
            stream.close()
            raise OSError('D2_FIXED_READER_FAILURE')
        monkeypatch.setattr(offline_guard, '_ORIGINAL_FDOPEN', failed_construction)
    elif phase == 'foreign_fd':
        def foreign_stream(*arguments, **options):
            return retained_fdopen(foreign, 'rb', closefd=False)
        monkeypatch.setattr(offline_guard, '_ORIGINAL_FDOPEN', foreign_stream)
    elif phase == 'foreign_handle':
        retained_transfer = offline_guard.d2_checkpoint_open_osfhandle
        foreign_handle = offline_guard._D2_READER_ORIGINAL['get_osfhandle'](foreign)
        def foreign_transfer(token, handle, flags):
            return retained_transfer(token, foreign_handle, flags)
        monkeypatch.setattr(offline_guard, 'd2_checkpoint_open_osfhandle', foreign_transfer)
    else:
        class ReadFailure:
            def __init__(self, stream):
                self.stream = stream
            @property
            def closed(self):
                return self.stream.closed
            def close(self):
                self.stream.close()
            def read(self, limit):
                raise OSError('D2_FIXED_READER_FAILURE')
            def __enter__(self):
                return self
            def __exit__(self, *arguments):
                self.close()
        def read_failure(descriptor, *arguments, **options):
            return ReadFailure(retained_fdopen(descriptor, *arguments, **options))
        monkeypatch.setattr(offline_guard, '_ORIGINAL_FDOPEN', read_failure)
    try:
        with pytest.raises(RuntimeError, match='D2_CHECKPOINT_INVALID'):
            upload._d2_read_json_bytes(path, 20)
        row = offline_guard._CONTEXT.d2_reader_history[-1]
        assert row['lifetime_complete'] is True and row['descriptor_closed'] is True
        assert row['stream_closed'] is True
        assert row['native_close_calls'] == int(phase in {'conversion', 'foreign_handle'})
        assert row['fd_close_calls'] == int(phase in {'fdopen', 'fdopen_after_audit', 'read', 'foreign_fd'})
        assert row['handle_created'] is (phase != 'create')
        assert row['fdopen_events'] == int(phase in {'read', 'fdopen_after_audit'})
        assert offline_guard._THREAD_STATE.d2_reader is None
        if foreign is not None:
            assert os.fstat(foreign).st_size == len(b'fixed checkpoint')
    finally:
        if foreign is not None:
            os.close(foreign)


@pytest.mark.parametrize('fault', ['filename', 'root', 'alias'])
def test_checkpoint_reader_invalid_path_has_no_native_effect(upload, offline_guard, tmp_path, monkeypatch, fault):
    path = tmp_path / 'google.json'
    path.write_bytes(b'fixed checkpoint')
    alias = None
    if fault == 'filename':
        path = tmp_path / 'unknown.json'
        path.write_bytes(b'fixed checkpoint')
    elif fault == 'root':
        retained_owned = offline_guard._owned
        monkeypatch.setattr(offline_guard, '_owned', lambda selected:
            False if selected == path else retained_owned(selected))
    else:
        alias = tmp_path / 'alias.json'
        os.link(path, alias)
    before = dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {}))
    try:
        with pytest.raises(RuntimeError, match='D2_CHECKPOINT_INVALID'):
            upload._d2_read_json(path, 20)
        assert dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {})) == before
        assert getattr(offline_guard._THREAD_STATE, 'd2_reader', None) is None
    finally:
        if alias is not None:
            alias.unlink()


@pytest.mark.parametrize('index,value', [(1, 0x40000000), (2, 3), (3, 1), (4, 2),
    (5, 0x80), (6, 1), (3, False), (6, False)])
def test_checkpoint_reader_parameters_are_closed(offline_guard, tmp_path, index, value):
    path = tmp_path / 'google.json'
    token = {'path': path}
    parameters = [str(path), 0x80000000, 7, 0, 3, 0x00200080, 0]
    offline_guard._d2_checkpoint_parameters(token, tuple(parameters))
    parameters[index] = value
    with pytest.raises(ValueError, match='D2_READER_PARAMETERS'):
        offline_guard._d2_checkpoint_parameters(token, tuple(parameters))


def test_checkpoint_reader_foreign_callers_are_refused(offline_guard, tmp_path):
    path = tmp_path / 'google.json'
    before = dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {}))
    bindings = {name: offline_guard._D2_READER_ORIGINAL[name]
        for name in ('CreateFile', 'CloseHandle', 'open_osfhandle')}
    with pytest.raises(ValueError, match='D2_READER_CALLER'):
        offline_guard.d2_begin_checkpoint_read(path, 20, bindings)
    token = dict(path=path, handle=123, fd=None, frame=None, code=None)
    for function, arguments in ((offline_guard.d2_checkpoint_create_file,
            (str(path), 0x80000000, 7, 0, 3, 0x00200080, 0)),
            (offline_guard.d2_checkpoint_open_osfhandle, (123, 32896)),
            (offline_guard.d2_checkpoint_close_handle, (123,))):
        with pytest.raises(ValueError, match='D2_READER_CALLER'):
            function(token, *arguments)
    assert dict(getattr(offline_guard._CONTEXT, 'd2_reader_counts', {})) == before


def test_checkpoint_reader_fd_reuse_does_not_close_foreign_owner(upload, offline_guard, tmp_path, monkeypatch):
    path = tmp_path / 'google.json'
    path.write_bytes(b'fixed checkpoint')
    retained_close = offline_guard._ORIGINAL_OS_CLOSE
    reused = {}
    def close_then_reuse(descriptor):
        result = retained_close(descriptor)
        if getattr(offline_guard._THREAD_STATE, 'd2_reader', None) is not None:
            reused['old'] = descriptor
            reused['new'] = os.open(path, os.O_RDONLY)
        return result
    monkeypatch.setattr(offline_guard, '_ORIGINAL_OS_CLOSE', close_then_reuse)
    try:
        assert upload._d2_read_json_bytes(path, 20) == b'fixed checkpoint'
        assert reused['new'] == reused['old']
        row = offline_guard._CONTEXT.d2_reader_history[-1]
        assert row['post_close_observation'] == 'REUSED_OPEN'
        assert row['lifetime_complete'] is True and row['fd_close_calls'] == 1
        assert os.fstat(reused['new']).st_size == len(b'fixed checkpoint')
        assert offline_guard._d2_checkpoint_fdopen_grant(reused['new'], 'r', 0) is False
    finally:
        if 'new' in reused:
            os.close(reused['new'])
