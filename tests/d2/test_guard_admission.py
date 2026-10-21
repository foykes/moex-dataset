"""Closed admission, role/phase refusal and security boundary negatives."""

import ast
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import types

import pytest


@pytest.mark.parametrize('case', ['unknown', 'success --timeout 1', '', '../success'])
def test_unknown_case_refused_before_process_or_file_work(guard, tmp_path, monkeypatch, case):
    reached = []
    monkeypatch.setattr(guard, 'attest_runtime', lambda: reached.append(True))
    with pytest.raises(guard.NativeAdmissionError):
        guard.run_d2_case(case, tmp_path, tmp_path)
    assert reached == []


def test_fixture_profile_requires_issued_controller(guard, tmp_path):
    assert guard.verify_d2_profile({}) is False
    with pytest.raises(guard.NativeAdmissionError):
        guard.issue_d2_profile(tmp_path, tmp_path, 'success', tmp_path)


def test_valid_case_with_spoofed_environment_has_no_runner_authority(repo, tmp_path, monkeypatch):
    specification = importlib.util.spec_from_file_location('d2_unissued_guard', repo / 'tools/offline_guard_linux.py')
    independent = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(independent)
    reached = []
    monkeypatch.setattr(independent.subprocess, 'Popen', lambda *args, **kwargs: reached.append(True))
    monkeypatch.setenv('MDS_D2_SOURCE_SHA', 'a' * 40)
    monkeypatch.setenv('MDS_D2_NATIVE_ROOT', str(tmp_path))
    with pytest.raises(independent.NativeAdmissionError) as raised:
        independent.run_d2_case('success', repo, tmp_path)
    assert raised.value.args == ('D2_NATIVE_RUNNER_UNISSUED',)
    assert reached == []


@pytest.mark.parametrize('private', [
    b'D2_PRIVATE_CANARY_4c1e11', b'LNX03_NATIVE_PRIVATE_CANARY_7fe94d31',
    b'https://private.invalid/a', b'C:\\private\\data', b'"/private/data"',
    b'Traceback (most recent call last)',
], ids=['d2_canary', 'bootstrap_canary', 'url', 'windows_path', 'posix_path', 'traceback'])
def test_export_negative_canary_oracle_rejects_each_surface(repo, private):
    specification = importlib.util.spec_from_file_location('d2_export_policy', repo / 'tools/offline_linux.py')
    policy = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(policy)
    with pytest.raises(policy.Refusal):
        policy.d2_admit_export_bytes(private)


def test_expected_node_inventory_is_unique_source_bound_and_closed(oracle):
    identifiers = oracle['expected_node_ids']
    assert len(identifiers) == 256
    assert len(identifiers) == len(set(identifiers))
    assert all(name.startswith('tests/d2/test_') and '::test_' in name for name in identifiers)
    assert {name.split('::', 1)[0] for name in identifiers} == {
        'tests/d2/test_supervision.py', 'tests/d2/test_targets.py',
        'tests/d2/test_logging_contract.py', 'tests/d2/test_guard_admission.py',
    }
    assert len([name for name in identifiers if '::test_native_case[' in name]) == 53
    data = ('\n'.join(identifiers) + '\n').encode('utf-8')
    assert hashlib.sha256(data).hexdigest() == oracle['expected_node_ids_sha256']


def test_linux_policy_does_not_admit_d2_child_as_arbitrary_command(guard):
    policy = guard._ProcessGuard()
    command = [sys.executable, '-I', '-S', '-B', '-X', 'utf8', 'upload.py', '--d2-worker', 'unissued']
    with pytest.raises(guard.NativeAdmissionError):
        policy('subprocess.Popen', (sys.executable, command, None, {}))


@pytest.mark.parametrize('path,digest', [
    ('tools/offline_tests.py', '905f293ea96389e70ed8396ca489ea81b9707038c8671626feeb911bbd2bec62'),
    ('tools/offline_guard.py', 'cd32dea805ba985bfcc69f34e0df70a4665122b7a4fc971dd82ff6c11192aebb'),
    ('run_logging.py', '93cc3c5f5a191ceede11e668b330881f3cba4307463c7a500b18f016cdf1a149'),
])
def test_windows_policies_and_flog_remain_source_bound_to_development_base(repo, path, digest):
    # This establishes unchanged source, not Windows runtime acceptance. Actual
    # Windows mandatory lanes remain separately required on exact HEAD.
    assert hashlib.sha256((repo / path).read_bytes().replace(b'\r\n', b'\n')).hexdigest() == digest


@pytest.mark.parametrize('role', ['parent', 'google', 'ftp', 'tracker', 'baseline-google', 'baseline-ftp'])
def test_worker_ticket_without_actual_issued_profile_is_refused(guard, role):
    with pytest.raises(guard.NativeAdmissionError):
        guard.d2_worker_admission({'schema': 'd2-worker-ticket-v1', 'role': role, 'profile': {}})


@pytest.mark.parametrize('fault', ['foreign_interpreter', 'relative_interpreter', 'empty_interpreter',
    'foreign_script', 'extra_argument', 'wrong_role', 'foreign_fd', 'duplicate_fd'])
def test_closed_launch_arguments_refuse_before_native_effects(guard, repo, tmp_path, monkeypatch, fault):
    # Explicit policy doubles admit only ticket decoding, never a process or
    # native descriptor. The real predicate is exercised with literal negatives.
    ticket = tmp_path / 'google.ticket.json'
    value = {'schema': 'd2-worker-ticket-v1', 'profile': {}, 'role': 'google',
             'parent_pid': os.getpid(), 'parent_start_ticks': 42,
             'repo_root': str(repo), 'run_root': str(tmp_path), 'expected_fds': [90000]}
    value['profile'] = {'scratch': str(tmp_path)}
    monkeypatch.setattr(guard, '_d2_read', lambda *args: value)
    monkeypatch.setattr(guard, 'verify_d2_profile', lambda profile: True)
    monkeypatch.setattr(guard, '_d2_ticks', lambda pid: 42)
    command = [str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
               str(repo / 'upload.py'), '--d2-worker', str(ticket)]
    descriptors = [90000]
    if fault == 'foreign_interpreter':
        command[0] = '/unissued/python'
    elif fault == 'relative_interpreter':
        command[0] = 'python'
    elif fault == 'empty_interpreter':
        command[0] = ''
    elif fault == 'foreign_script':
        command[-3] = str(repo / 'tests.py')
    elif fault == 'extra_argument':
        command.append('--timeout=1')
    elif fault == 'wrong_role':
        value['role'] = 'unissued-role'
    elif fault == 'foreign_fd':
        descriptors = [90001]
    else:
        descriptors.append(90000)
    reached = []
    def forbidden(*args, **options):
        reached.append(True)
        raise AssertionError('invalid argv reached native effect')
    monkeypatch.setattr(guard, '_admit_descriptors', forbidden)
    monkeypatch.setattr(guard, 'install_process_guard', forbidden)
    with pytest.raises(guard.NativeAdmissionError) as raised:
        with guard.d2_launch_scope(command, descriptors, ticket):
            raise AssertionError('invalid argv admitted')
    assert raised.value.args == ('D2_NATIVE_LAUNCH_TICKET' if fault == 'wrong_role' else 'D2_NATIVE_LAUNCH_ARGUMENTS',)
    assert reached == []


@pytest.mark.parametrize('profile', [None, {}, {'deadline_seconds': 1}, {'approved': True}])
def test_upload_test_profile_is_not_a_public_bypass(upload, profile):
    with pytest.raises(Exception):
        with upload._d2_test_profile(profile):
            raise AssertionError('unissued profile admitted')


@pytest.mark.parametrize('argument', ['--lane d2', '--timeout 1', '--module os', '--script arbitrary.py', '--callback x'])
def test_runner_retains_a_closed_d2_consumer_argv(repo, monkeypatch, capsys, argument):
    specification = importlib.util.spec_from_file_location('d2_closed_cli', repo / 'tools/offline_linux.py')
    policy = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(policy)
    replacement = types.SimpleNamespace(**vars(sys))
    replacement.argv = ['offline_linux.py', *argument.split()]
    monkeypatch.setattr(policy, 'sys', replacement)
    reached = []
    def forbidden(*args, **kwargs):
        reached.append(True)
        raise AssertionError('unknown CLI reached ticket/launch')
    monkeypatch.setattr(policy, 'validate_ticket', forbidden)
    monkeypatch.setattr(policy, 'd2_validate_ticket', forbidden)
    assert policy.main() != 0
    assert reached == []
    output = capsys.readouterr()
    assert argument not in output.out + output.err


def _checkpoint(upload):
    items = [{'output_id': 'google', 'group': 'google'}]
    document = upload._d2_checkpoint('a' * 32, 'b' * 32, 'c' * 40, 'google', items)
    return document, items


@pytest.mark.parametrize('fault', ['run', 'attempt', 'pid', 'sequence', 'worker', 'producer', 'extra', 'output', 'commit', 'boolean_sequence',
    'state', 'group', 'count', 'duplicate', 'normal_marker', 'unhashable_state', 'diagnostics', 'boolean_pid', 'negative_pid'])
def test_checkpoint_schema_identity_and_corruption_fail_closed(upload, fault):
    document, items = _checkpoint(upload)
    actual_pid = document['pid']
    if fault == 'run':
        document['run_id'] = 'd' * 32
    elif fault == 'attempt':
        document['attempt_id'] = 'd' * 32
    elif fault == 'pid':
        document['pid'] += 1
    elif fault == 'sequence':
        document['sequence'] = 0
    elif fault == 'worker':
        document['worker_id'] = 'ftp'
    elif fault == 'producer':
        document['producer_sha'] = 'd' * 40
    elif fault == 'extra':
        document['unknown'] = True
    elif fault == 'output':
        document['outputs'][0]['output_id'] = 'foreign'
    elif fault == 'commit':
        document['outputs'][0].update(transfer_state='ACKNOWLEDGED', commit_state='COMMITTED')
    elif fault == 'boolean_sequence':
        document['sequence'] = True
    elif fault == 'state':
        document['execution_state'] = 'VERIFIED_RELEASE'
    elif fault == 'group':
        document['outputs'][0]['group'] = 'root'
    elif fault == 'count':
        document['outputs'] = []
    elif fault == 'duplicate':
        document['outputs'].append(copy.deepcopy(document['outputs'][0]))
    elif fault == 'normal_marker':
        document['normal_return_observed'] = True
    elif fault == 'unhashable_state':
        document['execution_state'] = []
    elif fault == 'diagnostics':
        document['diagnostics_state'] = 'COMPLETE_AND_VERIFIED'
    elif fault == 'boolean_pid':
        document['pid'] = True
    else:
        document['pid'] = -1
    with pytest.raises(Exception):
        upload._d2_validate_checkpoint_record(document, 'google', run_id='a' * 32,
            attempt_id='b' * 32, pid=actual_pid, outputs=items, producer_sha='c' * 40)


@pytest.mark.parametrize('fault', ['rewind', 'same_sequence_change', 'ack_regression', 'terminal_reopened'])
def test_checkpoint_lifecycle_cannot_regress(upload, tmp_path, fault):
    old, items = _checkpoint(upload)
    old.update(sequence=3, execution_state='RETURNED', normal_return_observed=True, diagnostics_state='COMPLETE')
    old['outputs'][0].update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    current = copy.deepcopy(old)
    if fault == 'rewind':
        current['sequence'] = 2
    elif fault == 'same_sequence_change':
        current['error_code'] = 'D2_FAILED'
    elif fault == 'ack_regression':
        current['sequence'] = 4
        current['outputs'][0].update(transfer_state='UNKNOWN', commit_state='UNKNOWN')
    else:
        current.update(sequence=4, execution_state='RUNNING', normal_return_observed=False, diagnostics_state='ACTIVE')
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    (root / 'checkpoint.json').write_bytes(upload._d2_bytes(current))
    (root / 'checkpoint.json').chmod(0o600)
    with pytest.raises(Exception):
        upload._d2_read_checkpoint(tmp_path, 'google', run_id='a' * 32, attempt_id='b' * 32,
            pid=old['pid'], outputs=items, producer_sha='c' * 40, previous=old)


def test_identical_checkpoint_reread_is_valid(upload, tmp_path):
    document, items = _checkpoint(upload)
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    (root / 'checkpoint.json').write_bytes(upload._d2_bytes(document))
    (root / 'checkpoint.json').chmod(0o600)
    read = upload._d2_read_checkpoint(tmp_path, 'google', run_id='a' * 32, attempt_id='b' * 32,
        pid=document['pid'], outputs=items, producer_sha='c' * 40, previous=copy.deepcopy(document))
    assert read == document


@pytest.mark.parametrize('fault', ['hardlink', 'symlink', 'directory', 'fifo'])
def test_checkpoint_reader_rejects_aliases_and_special_files(upload, tmp_path, fault):
    assert sys.platform == 'linux', 'native no-follow/special-file oracle requires Linux'
    path = tmp_path / 'checkpoint.json'
    target = tmp_path / 'real.json'
    target.write_bytes(b'{}\n')
    if fault == 'hardlink':
        os.link(target, path)
    elif fault == 'symlink':
        path.symlink_to(target)
    elif fault == 'directory':
        path.mkdir()
    else:
        os.mkfifo(path)
    with pytest.raises(Exception):
        upload._d2_read_json(path, 65536)


def test_full_inventory_refuses_live_phase_before_metadata(upload, tmp_path, monkeypatch):
    reached = []
    def forbidden(*args, **kwargs):
        reached.append(True)
        raise AssertionError('live inventory reached metadata')
    monkeypatch.setattr(upload, '_d2_private_directory', forbidden)
    with pytest.raises(Exception):
        upload._d2_run_inventory(tmp_path, upload._d2_validate_limits(), settled=False)
    assert reached == []


@pytest.mark.parametrize('fault', ['pending', 'unknown', 'hardlink'])
def test_final_full_inventory_rejects_residue_and_aliases(upload, tmp_path, fault):
    path = tmp_path / ('parent-result.json.pending' if fault == 'pending' else 'unexpected.json' if fault == 'unknown' else 'parent-result.json')
    if fault == 'hardlink':
        original = tmp_path / 'plan.json'
        original.write_bytes(b'{}\n')
        os.link(original, path)
    else:
        path.write_bytes(b'{}\n')
    with pytest.raises(Exception):
        upload._d2_run_inventory(tmp_path, upload._d2_validate_limits(), settled=True)


def test_run_inventory_reduced_byte_boundary_and_plus_one(upload, tmp_path):
    path = tmp_path / 'parent-result.json'
    path.write_bytes(b'{}\n')
    limits = upload._d2_validate_limits({'max_run_bytes': 3})
    assert upload._d2_run_inventory(tmp_path, limits, settled=True) == 3
    path.write_bytes(b'{}\n ')
    with pytest.raises(Exception):
        upload._d2_run_inventory(tmp_path, limits, settled=True)


def test_slot_writer_never_inspects_sibling_metadata(upload, tmp_path, monkeypatch):
    (tmp_path / 'google').mkdir(mode=0o700)
    (tmp_path / 'ftp').mkdir(mode=0o700)
    document, items = _checkpoint(upload)
    visited = []
    original = upload._d2_checked_path
    def checked(path, **options):
        path = Path(path)
        visited.append(path)
        assert not path.is_relative_to(tmp_path / 'ftp'), 'own-slot write inspected live sibling'
        return original(path, **options)
    monkeypatch.setattr(upload, '_d2_checked_path', checked)
    upload._d2_slot_atomic_json(tmp_path, 'google', document, upload._d2_validate_limits())
    assert (tmp_path / 'google/checkpoint.json').exists()
    assert not (tmp_path / 'google/checkpoint.json.pending').exists()


def test_slot_writer_refuses_existing_pending_without_overwriting(upload, tmp_path):
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    pending = root / 'checkpoint.json.pending'
    pending.write_bytes(b'prior pending evidence\n')
    document, items = _checkpoint(upload)
    with pytest.raises(Exception):
        upload._d2_slot_atomic_json(tmp_path, 'google', document, upload._d2_validate_limits())
    assert pending.read_bytes() == b'prior pending evidence\n'


@pytest.mark.parametrize('operation', ['read', 'write', 'full_inventory'])
def test_worker_live_sibling_slot_refused_before_effect(upload, tmp_path, monkeypatch, operation):
    monkeypatch.setattr(upload, '_D2_WORKER_ROLE', 'google')
    visited = []
    def forbidden(*args, **kwargs):
        visited.append(True)
        raise AssertionError('sibling metadata was inspected')
    monkeypatch.setattr(upload, '_d2_read_json', forbidden)
    monkeypatch.setattr(upload, '_d2_atomic_json', forbidden)
    monkeypatch.setattr(upload, '_d2_private_directory', forbidden)
    with pytest.raises(Exception):
        if operation == 'read':
            upload._d2_read_checkpoint(tmp_path, 'ftp', run_id='a' * 32, attempt_id='b' * 32,
                pid=123, outputs=[], producer_sha='c' * 40)
        elif operation == 'write':
            upload._d2_slot_atomic_json(tmp_path, 'ftp', {'worker_id': 'ftp'}, upload._d2_validate_limits())
        else:
            upload._d2_run_inventory(tmp_path, upload._d2_validate_limits(), settled=True)
    assert visited == []


def test_parent_live_checkpoint_refused_before_open(upload, tmp_path, monkeypatch):
    process = types.SimpleNamespace(poll=lambda: None)
    active = types.SimpleNamespace(root=tmp_path, go_released=True,
                                  children=[{'role': 'google', 'process': process}])
    monkeypatch.setattr(upload, '_D2_ACTIVE', active)
    reached = []
    def forbidden(*args, **kwargs):
        reached.append(True)
        raise AssertionError('live worker checkpoint was opened')
    monkeypatch.setattr(upload, '_d2_read_json', forbidden)
    with pytest.raises(Exception):
        upload._d2_read_checkpoint(tmp_path, 'google', run_id='a' * 32, attempt_id='b' * 32,
            pid=123, outputs=[], producer_sha='c' * 40)
    assert reached == []


@pytest.mark.parametrize('fault', ['missing', 'partial', 'nan', 'duplicate_keys', 'trailing', 'oversize'])
def test_raw_checkpoint_negative_boundary_is_actually_reached(upload, tmp_path, fault):
    document, items = _checkpoint(upload)
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    path = root / 'checkpoint.json'
    data = upload._d2_bytes(document)
    if fault == 'partial':
        data = b'{broken'
    elif fault == 'nan':
        data = data.replace(b'"sequence":1', b'"sequence":NaN')
    elif fault == 'duplicate_keys':
        data = data.replace(b'"sequence":1', b'"sequence":0,"sequence":1')
    elif fault == 'trailing':
        data += b'{}'
    elif fault == 'oversize':
        data = b'x' * 65537
    if fault != 'missing':
        path.write_bytes(data)
        path.chmod(0o600)
    with pytest.raises(Exception):
        upload._d2_read_checkpoint(tmp_path, 'google', run_id='a' * 32, attempt_id='b' * 32,
            pid=document['pid'], outputs=items, producer_sha='c' * 40)


def test_worker_sequence_cannot_rewind_and_skipped_polls_are_valid(upload, tmp_path):
    document, items = _checkpoint(upload)
    document.update(sequence=2, execution_state='RUNNING')
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    path = root / 'checkpoint.json'
    path.write_bytes(upload._d2_bytes(document))
    path.chmod(0o600)
    options = dict(run_id='a' * 32, attempt_id='b' * 32, pid=document['pid'], outputs=items, producer_sha='c' * 40)
    assert upload._d2_read_checkpoint(tmp_path, 'google', minimum_sequence=2, **options) == document
    with pytest.raises(Exception):
        upload._d2_read_checkpoint(tmp_path, 'google', minimum_sequence=3, **options)
    later = copy.deepcopy(document)
    later.update(sequence=7, execution_state='RETURNED', normal_return_observed=True, diagnostics_state='COMPLETE')
    later['outputs'][0].update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    path.write_bytes(upload._d2_bytes(later))
    assert upload._d2_read_checkpoint(tmp_path, 'google', previous=document, **options) == later


def test_checkpoint_reader_fd_reuse_does_not_close_foreign_owner(upload, tmp_path, monkeypatch):
    assert sys.platform == 'linux'
    path = tmp_path / 'checkpoint.json'
    path.write_bytes(b'{invalid')
    path.chmod(0o600)
    original = upload._d2_fdopen
    owned = []
    @contextmanager
    def reused(descriptor, *args, **options):
        try:
            with original(descriptor, *args, **options) as handle:
                yield handle
        finally:
            replacement = os.open(tmp_path / 'foreign-owner', os.O_RDWR | os.O_CREAT, 0o600)
            owned.append((replacement, os.fstat(replacement)))
            assert replacement == descriptor
    monkeypatch.setattr(upload, '_d2_fdopen', reused)
    try:
        with pytest.raises(Exception):
            upload._d2_read_json(path, 65536)
        assert len(owned) == 1
        descriptor, identity = owned[0]
        assert os.fstat(descriptor) == identity
    finally:
        for descriptor, identity in owned:
            try:
                os.close(descriptor)
            except OSError:
                pass


def test_deadline_is_rechecked_before_go_has_any_effect(upload, monkeypatch):
    runtime = types.SimpleNamespace(context={}, deadline=15, go_released=False, owns_tracker=False,
                                    close_fd=lambda descriptor: None)
    reached = []
    monkeypatch.setitem(sys.modules, 'run_logging', types.SimpleNamespace(
        check_logging_health=lambda context: {'healthy': True}))
    monkeypatch.setattr(upload, 'time', types.SimpleNamespace(monotonic=lambda: 15))
    monkeypatch.setattr(upload.os, 'write', lambda *args: reached.append(True))
    with pytest.raises(Exception):
        upload._d2_release_go(runtime, [{'go': 90000}])
    assert reached == [] and runtime.go_released is False


def test_go_visibility_is_recorded_before_interrupted_second_release(upload, monkeypatch):
    runtime = types.SimpleNamespace(context={}, deadline=15, go_released=False, owns_tracker=False,
                                    close_fd=lambda descriptor: None)
    calls = []
    monkeypatch.setitem(sys.modules, 'run_logging', types.SimpleNamespace(
        check_logging_health=lambda context: {'healthy': True}))
    monkeypatch.setattr(upload, 'time', types.SimpleNamespace(monotonic=lambda: 1))
    def writing(descriptor, value):
        calls.append(descriptor)
        assert runtime.go_released is True
        if len(calls) == 2:
            raise KeyboardInterrupt()
        return 1
    monkeypatch.setattr(upload.os, 'write', writing)
    with pytest.raises(KeyboardInterrupt):
        upload._d2_release_go(runtime, [{'go': 90000}, {'go': 90001}])
    assert calls == [90000, 90001] and runtime.go_released is True


@pytest.mark.parametrize('fault', ['own_oversize', 'own_hardlink', 'own_symlink'])
def test_slot_writer_preserves_own_invalid_file_without_replacement(upload, tmp_path, fault):
    assert sys.platform == 'linux'
    root = tmp_path / 'google'
    root.mkdir(mode=0o700)
    destination = root / 'checkpoint.json'
    target = tmp_path / 'old-evidence.json'
    target.write_bytes(b'old evidence must remain\n')
    if fault == 'own_oversize':
        destination.write_bytes(b'x' * 65537)
    elif fault == 'own_hardlink':
        os.link(target, destination)
    else:
        destination.symlink_to(target)
    before = destination.read_bytes()
    document, items = _checkpoint(upload)
    with pytest.raises(Exception):
        upload._d2_slot_atomic_json(tmp_path, 'google', document, upload._d2_validate_limits())
    assert destination.read_bytes() == before


def _authority():
    sha, base = 'a' * 40, '25dee502a68128aea752f24ff67dfcb16b615a36'
    grant = dict(approved=True, consumer='d2-linux', repository='foykes/moex-dataset', sha=sha,
        base=base, event='push', ref='refs/heads/codex/mds-d2-linux-stage-b', actor='foykes',
        triggering_actor='foykes', workflow='.github/workflows/d2-linux-offline.yml', job='d2-linux',
        run_number=31, run_id='731009', attempt=2, ticket_id='c' * 64)
    env = {'GITHUB_REPOSITORY': grant['repository'], 'GITHUB_SHA': sha,
        'GITHUB_EVENT_NAME': grant['event'], 'GITHUB_REF': grant['ref'],
        'GITHUB_ACTOR': grant['actor'], 'GITHUB_TRIGGERING_ACTOR': grant['triggering_actor'],
        'GITHUB_RUN_NUMBER': '31', 'GITHUB_RUN_ATTEMPT': '2', 'GITHUB_RUN_ID': '731009',
        'GITHUB_JOB': grant['job'], 'GITHUB_WORKFLOW_SHA': sha,
        'GITHUB_WORKFLOW_REF': grant['repository'] + '/' + grant['workflow'] + '@' + grant['ref'],
        'RUNNER_ENVIRONMENT': 'github-hosted', 'TASK_RUNNER_ENVIRONMENT': 'github-hosted',
        'TASK_DELETED': 'false', 'GITHUB_ACTIONS': 'true', 'ImageOS': 'ubuntu24',
        'TASK_WORKFLOW': grant['workflow']}
    for task, native in {'TASK_EVENT': 'GITHUB_EVENT_NAME', 'TASK_EVENT_SHA': 'GITHUB_SHA',
        'TASK_EVENT_REF': 'GITHUB_REF', 'TASK_REPOSITORY': 'GITHUB_REPOSITORY', 'TASK_ACTOR': 'GITHUB_ACTOR',
        'TASK_TRIGGERING_ACTOR': 'GITHUB_TRIGGERING_ACTOR', 'TASK_JOB': 'GITHUB_JOB',
        'TASK_RUN_NUMBER': 'GITHUB_RUN_NUMBER', 'TASK_RUN_ATTEMPT': 'GITHUB_RUN_ATTEMPT',
        'TASK_RUN_ID': 'GITHUB_RUN_ID'}.items():
        env[task] = env[native]
    env['MDS_D2_LINUX_ADMISSION_GRANT'] = json.dumps(grant)
    env['MDS_D2_LINUX_SETUP_GRANT'] = json.dumps(dict(grant, capability='network-setup'))
    return env, grant, sha, base


def test_d2_authority_has_a_positive_exact_source_bound_vector(helper):
    env, grant, sha, base = _authority()
    assert helper.d2_validate_grant(env, sha, base) == grant


@pytest.mark.parametrize('fault', ['sha', 'base', 'ref', 'actor', 'triggering_actor', 'run_id', 'attempt', 'consumer'])
def test_d2_authority_rejects_foreign_identity_before_setup(helper, fault):
    env, grant, sha, base = _authority()
    changes = {'sha': 'd' * 40, 'base': 'd' * 40, 'ref': 'refs/heads/foreign', 'actor': 'foreign',
               'triggering_actor': 'foreign', 'run_id': '731010', 'attempt': 3, 'consumer': 'linux-bootstrap'}
    grant[fault] = changes[fault]
    env['MDS_D2_LINUX_ADMISSION_GRANT'] = json.dumps(grant)
    with pytest.raises(helper.PolicyError):
        helper.d2_validate_grant(env, sha, base)


def test_d2_missing_network_grant_does_not_inherit_legacy_authority(helper):
    env, grant, sha, base = _authority()
    env.pop('MDS_D2_LINUX_SETUP_GRANT')
    env['MDS_LNX03_SETUP_GRANT'] = json.dumps(dict(grant, capability='network-setup'))
    with pytest.raises(helper.PolicyError):
        helper.d2_validate_grant(env, sha, base)
    assert helper.d2_validate_grant(env, sha, base, require_setup=False) == grant


def test_d2_ticket_cannot_be_claimed_twice_or_by_another_run(helper, tmp_path):
    env, grant, sha, base = _authority()
    digest = helper.d2_claim(tmp_path, grant)
    path = tmp_path / (grant['ticket_id'] + '.json')
    before = path.read_bytes()
    assert hashlib.sha256(before).hexdigest() == digest
    for value in (grant, dict(grant, run_id='731010')):
        with pytest.raises(helper.PolicyError):
            helper.d2_claim(tmp_path, value)
    assert path.read_bytes() == before
