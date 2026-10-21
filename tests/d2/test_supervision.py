"""Independent native outcomes, settlement and complete byte boundaries."""

import ast
import copy
import hashlib
import json
import os
from pathlib import Path
import types

import pytest


BASELINE_CASES = (
    'baseline_google_swallowed', 'baseline_ftp_root_swallowed',
    'baseline_ftp_dividends_swallowed', 'baseline_ftp_tickers_swallowed',
    'baseline_exit23', 'baseline_hang',
)
NATIVE_CASES = (
    'success', 'google_error', 'ftp_error', 'exit23', 'system_exit0',
    'system_exit_string', 'abnormal_before_ready', 'abnormal_after_final',
    'normal_without_final', 'final_then_exit23', 'start_google_before',
    'start_ftp_before', 'start_google_after_native', 'start_ftp_after_native',
    'constructor_error', 'hang_google', 'hang_ftp', 'cleanup_unsettled',
    'partial_google', 'partial_ftp', 'checkpoint_missing', 'checkpoint_corrupt',
    'checkpoint_oversize', 'checkpoint_stale_run', 'checkpoint_stale_pid',
    'checkpoint_stale_sequence', 'listener_failure', 'ack_failure',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'foreign_fd', 'canary_positive_control',
    'parent_finalization_raises', 'parent_finalization_incomplete',
    'postseal_cleanup_failure', 'cancel_google', 'cancel_ftp',
    'ticket_ftp_failure', 'pidfd_google_failure', 'identity_google_failure',
    'registry_google_failure', 'registration_google_failure',
    'registration_ftp_failure', 'bootstrap_pipe_failure', 'replace_failure', 'tracker_death',
    'preupload_stage_failure',
    'post_context_bind_failure',
    'serialization_write_failure',
)


def serialized(value):
    return (json.dumps(value, ensure_ascii=True, sort_keys=True,
                       separators=(',', ':'), allow_nan=False) + '\n').encode('ascii')


def test_fixed_native_inventory_is_independent(guard):
    assert guard.D2_CASES == NATIVE_CASES
    assert guard.D2_BASELINE_CASES == BASELINE_CASES
    assert guard.D2_CONTROLLER_SECONDS == 75


@pytest.mark.native
@pytest.mark.parametrize('case', BASELINE_CASES + NATIVE_CASES)
def test_native_case(case, guard, repo, native_scratch, native_results):
    record = guard.run_d2_case(case, repo, native_scratch)
    native_results.append(record)
    assert set(record) == {'schema_version', 'case', 'controller', 'observation'}
    assert record['schema_version'] == 'd2-native-case-v1' and record['case'] == case
    controller = record['controller']
    assert controller['started'] is True and controller['pidfd_bound'] is True
    assert type(controller['pid']) is int and controller['pid'] > 1
    assert controller['settled'] is True and controller['timed_out'] is False
    assert controller['exitcode'] == 0
    observation = record['observation']
    assert type(observation) is dict and observation['case'] == case
    actual = observation['observations']
    assert all(type(row['pid']) is int and row['pid'] > 1 for row in actual)
    assert len({row['pid'] for row in actual}) == len(actual)
    if case in BASELINE_CASES:
        assert observation['baseline_scope'] == 'ORIGINAL_TARGET_AND_PARENT_FLOW_FIXED_PROCESS_ADAPTER'
        assert observation['baseline_red'] is True and observation['all_settled'] is True
        assert len(actual) == 2 and all(row['settled'] is True for row in actual)
        if case == 'baseline_hang':
            assert observation['legacy_hang_observed'] is True
            assert observation['legacy_parent_returned_none'] is False
        else:
            assert observation['legacy_parent_returned_none'] is True
            assert observation['legacy_false_completion_observed'] is True
            if case == 'baseline_exit23':
                assert any(row['exitcode'] == 23 for row in actual)
            else:
                assert observation['legacy_operation_fault_observed'] is True
                assert all(row['exitcode'] == 0 for row in actual)
        return
    result = observation['result']
    assert result['committed_outputs'] == []
    assert set(result['workers']) == {'google', 'ftp'}
    assert all(worker['required'] is True for worker in result['workers'].values())
    assert 'release_result' not in result and 'current_generation' not in result and 'previous_preserved' not in result
    assert observation['console_canary_absent'] is True
    assert observation['console_redaction_valid'] is True
    assert observation['foreign_fd_intact'] is True
    assert observation['fixture_cleanup_settled'] is True
    assert observation['fixture_tracker_settled'] is True
    assert observation['raw_fd_inventory_clean'] is observation['fd_inventory_unchanged']
    if observation['owned_fd_inventory_clean'] and not observation['raw_fd_inventory_clean']:
        assert observation['fd_attribution'] == 'PROVEN_CPYTHON_CACHE'
        assert observation['cache_provenance']['status'] == 'OBSERVED'
        assert observation['fd_inventory_delta']['removed'] == observation['fd_inventory_delta']['changed'] == []
        assert len(observation['fd_inventory_delta']['added']) == 2
    if case not in {'constructor_error', 'cleanup_unsettled', 'parent_finalization_raises',
                    'postseal_cleanup_failure'}:
        assert observation['owned_fd_inventory_clean'] is True, 'settled failure must release every owned descriptor'
    platform = observation['platform_evidence']
    assert platform['platform'] == 'linux' and platform['implementation'] == 'cpython'
    assert platform['version'] == [3, 14, 8] and platform['architecture'] == 'x86_64'
    assert platform['filesystem_encoding'] == 'utf-8'
    assert platform['actual_path_unicode'] is platform['actual_path_space'] is platform['actual_cwd_foreign'] is True
    by_role = {row['worker_id']: row for row in actual if row['worker_id'] in {'google', 'ftp'}}
    assert any(row['worker_id'] == 'tracker' for row in actual)
    for role, worker in result['workers'].items():
        assert {item['commit_state'] for item in worker['outputs']} <= {'NOT_ATTEMPTED', 'UNKNOWN'}
        if worker['started'] is True:
            assert role in by_role and worker['pid'] == by_role[role]['pid']
            if worker['settled']:
                assert worker['exitcode'] == by_role[role]['exitcode']
        elif worker['started'] is None:
            assert worker['settled'] is False and worker['exitcode'] is None
        else:
            assert worker['started'] is False and worker['pid'] is None and worker['exitcode'] is None
    persisted = observation['persisted_result']
    if persisted is not None:
        assert persisted['receipt_scope'] == 'PREFINAL_SUPERVISION'
        assert persisted['finalization_state'] == 'NOT_OBSERVED'
        assert persisted['diagnostic_success_allowed'] is False
        assert persisted['committed_outputs'] == []
    for row in observation['phase_reads']:
        if row['phase'] == 'AFTER_GO':
            assert type(row['actual_exit']) is int, 'mutable checkpoint opened before actual worker settlement'
    for row in observation['go_releases']:
        assert row['ready_reads'] >= 2
        assert row['readers_closed'] is True
    assert observation['readers_closed_before_go'] is True
    for row in observation['inventory_observations']:
        if row['phase'] == 'AFTER_GO':
            assert len(row['worker_pids']) == 2
            assert all(type(code) is int for code in row['worker_exits'])
    report = observation['logging']
    if case in {'success', 'foreign_fd'}:
        assert observation['application_exit'] == 0
        assert observation['success_banner'] is True
        assert observation['owned_fd_inventory_clean'] is True
        assert observation['google_projection_observed'] is True
        assert observation['ftp_targets_observed'] is True
        assert result['parent_outcome'] == 'RETURNED'
        assert result['final_exit_status'] == 0 and result['diagnostic_success_allowed'] is True
        assert result['diagnostics_state'] == 'COMPLETE'
        assert set(by_role) == {'google', 'ftp'}
        assert all(row['exitcode'] == 0 and row['settled'] for row in by_role.values())
        assert report['run_id'] == result['run_id']
        assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed'] is True
        assert report['exit_status'] == 0 and report['release_result'] is None
        producers = {row['worker_id']: row for row in report['expected_producers']}
        assert set(producers) == {'parent', 'google', 'ftp'}
        for role in ('google', 'ftp'):
            assert producers[role]['final_report']['execution_outcome'] == 'COMPLETED'
            assert producers[role]['final_report']['exit_status'] == 0
        assert result['unknown_outputs'], 'ACK is not verified remote commit'
        assert observation['inventory_observations']
        for role in ('google', 'ftp'):
            assert 'NATIVE_ENTRY' in observation['worker_witnesses'][role]
            assert 'CANARY_WRITES' in observation['worker_witnesses'][role]
    else:
        assert observation['application_exit'] != 0
        assert result['diagnostic_success_allowed'] is False
        assert result['final_exit_status'] != 0
        assert observation['success_banner'] is False
        if case not in {'preupload_stage_failure', 'post_context_bind_failure', 'serialization_write_failure'}:
            witnesses = observation['fault_witnesses']
            reached = [row for row in witnesses if row['case'] == case]
            assert reached and all(row['pid'] > 1 and row['count'] == 1 for row in reached), 'requested fault boundary was not reached'
    if case in {'preupload_stage_failure', 'post_context_bind_failure'}:
        witness = observation['preupload_witness']
        assert witness['stage_entered'] is (case == 'preupload_stage_failure')
        assert witness['original_exception_preserved'] is True
        assert witness['source_profile_verified'] is True and witness['pending_cleared'] is True
        assert witness['runtime_closed'] is witness['runtime_cleanup_complete'] is True
        assert witness['runtime_transport_complete'] is witness['tracker_wrappers_released'] is True
        assert witness['api_calls'] == witness['upload_calls'] == 0
        assert len(actual) == 1 and actual[0]['worker_id'] == 'tracker'
        assert actual[0]['settled'] is True and actual[0]['exitcode'] == 0
        assert by_role == {} and observation['owned_fd_inventory_clean'] is True
        assert observation['go_releases'] == [] and observation['phase_reads'] == []
        assert observation['google_projection_observed'] is observation['ftp_targets_observed'] is None
        forks = [row for row in observation['constructor_observations'] if row['event'] == 'FORK_EXEC_RETURNED']
        assert len(forks) == 1 and forks[0]['worker_id'] == 'tracker'
        assert report['execution_outcome'] == 'FAILED' and report['exit_status'] != 0
        assert report['release_result'] is None and report['worker_outcomes'] == {}
        assert len(report['expected_producers']) == 1
        assert report['expected_producers'][0]['worker_id'] == 'parent'
        if case == 'post_context_bind_failure':
            assert observation['post_bind_witness'] == dict(context_created=True, fault_injected=True, fault_calls=1)
    if case == 'serialization_write_failure':
        witness = observation['serialization_witness']
        assert witness['fault_calls'] == 1 and witness['reduced_fd_count'] > 0
        assert all(witness[name] is True for name in ('real_dump_reached', 'original_exception_preserved',
            'exception_args_preserved', 'pinned_dump_object_cleared', 'own_bootstrap_alias_cleared',
            'foreign_frame_preserved', 'late_gc_no_native_birth', 'late_gc_pending_cleared'))
        assert by_role == {} and len(actual) == 1 and actual[0]['worker_id'] == 'tracker'
        assert actual[0]['settled'] is True and actual[0]['exitcode'] == 0
        assert observation['go_releases'] == [] and observation['phase_reads'] == []
        assert observation['google_projection_observed'] is observation['ftp_targets_observed'] is None
        forks = [row for row in observation['constructor_observations'] if row['event'] == 'FORK_EXEC_RETURNED']
        assert len(forks) == 1 and forks[0]['worker_id'] == 'tracker'
        assert all(worker['started'] is False for worker in result['workers'].values())
    if case in {'exit23', 'final_then_exit23', 'abnormal_before_ready', 'abnormal_after_final'}:
        assert any(row['exitcode'] == 23 for row in by_role.values())
    if case in {'hang_google', 'hang_ftp', 'cleanup_unsettled'}:
        assert observation['deadline_detected'] is True
        assert observation['cleanup_requested'] is True
        assert observation['target_entered_before_deadline'] is True
        assert observation['parent_elapsed_seconds'] >= 15
    if case == 'cleanup_unsettled':
        assert any(not worker['settled'] for worker in result['workers'].values())
        assert persisted is None
        assert not any(row['phase'] == 'AFTER_GO' for row in observation['inventory_observations'])
    if case in {'partial_ftp', 'partial_google'} or case.startswith('checkpoint_'):
        assert result['unknown_outputs']
    if case in {'cancel_google', 'cancel_ftp'}:
        assert observation['application_exit'] == 130
        assert result['parent_outcome'] == 'INTERRUPTED'
        assert result['unknown_outputs']
    if case == 'partial_ftp':
        assert any(item['transfer_state'] == 'ACKNOWLEDGED' for item in result['workers']['ftp']['outputs'])
        assert any(item['transfer_state'] == 'UNKNOWN' for item in result['workers']['ftp']['outputs'])
    if case == 'constructor_error':
        assert any(worker['started'] is None and not worker['settled'] for worker in result['workers'].values())
        forks = [row for row in observation['constructor_observations'] if row['event'] == 'FORK_EXEC_RETURNED']
        waits = [row for row in observation['constructor_observations'] if row['event'] == 'WAITPID_RETURNED']
        assert forks and any(row['worker_id'] == 'ftp' for row in waits)
        assert any(row['exitcode'] != 0 for row in waits if row['worker_id'] == 'ftp')
        assert report is not None and 'ftp' not in report['worker_outcomes']
        producers = {row['worker_id']: row for row in report['expected_producers']}
        assert producers['ftp']['final_report'] is None
    if case == 'tracker_death':
        trackers = [row for row in actual if row['worker_id'] == 'tracker']
        assert len(trackers) == 1 and trackers[0]['settled'] is True
        assert type(trackers[0]['exitcode']) is int and trackers[0]['exitcode'] != 0
        assert observation['go_releases'] == []
        assert observation['google_projection_observed'] is None
        assert observation['ftp_targets_observed'] is None
        assert all('GO_ENTERED' not in events for events in observation['worker_witnesses'].values())
        forks = [row for row in observation['constructor_observations'] if row['event'] == 'FORK_EXEC_RETURNED']
        assert len(forks) == 3 and {row['worker_id'] for row in forks} == {'tracker', 'google', 'ftp'}
        assert len({row['pid'] for row in forks}) == 3, 'resource tracker must not silently relaunch'
        probes = observation['tracker_probes']
        assert probes['actual_exit'] == trackers[0]['exitcode'] and probes['before_go'] is True
        assert all(probes[name] is True for name in (
            'ensure_running_refused', 'register_refused', 'launch_refused', 'teardown_refused'))
        assert type(probes['failed_unregisters']) is int and probes['failed_unregisters'] >= 0
    if case.startswith('bootstrap_'):
        boundary = {'bootstrap_replay_failure': 'PREPARATION_BOUNDARY',
                    'bootstrap_import_failure': 'BOOTSTRAP_IMPORT_BOUNDARY',
                    'bootstrap_unpickle_failure': 'UNPICKLE_BOUNDARY',
                    'bootstrap_pipe_failure': None}[case]
        if boundary is not None:
            assert boundary in observation['worker_witnesses']['google']
            assert 'NATIVE_ENTRY' in observation['worker_witnesses']['google']
            assert 'CANARY_WRITES' in observation['worker_witnesses']['google']
    if case in {'parent_finalization_raises', 'parent_finalization_incomplete'}:
        assert persisted is not None
        before = observation['preseal_preconditions']
        assert before['actual_children_settled'] == before['actual_children_exit_zero'] == 2
        assert before['worker_final_completed_zero'] == 2
        assert before['final_absent'] is True and before['summary_absent'] is True
        assert observation['persisted_bytes_unchanged'] is True
        assert all(row['exitcode'] == 0 for row in by_role.values())
        if case == 'parent_finalization_raises':
            assert before['fault_boundary'] == 'FREEZE_ENTRY'
            assert result['storage_sealed'] == 'UNKNOWN'
        else:
            assert before['fault_boundary'] == 'SEAL_INVENTORY'
            assert report['delivery_outcome'] == 'INCOMPLETE' and report['storage_sealed'] is False
    if case == 'postseal_cleanup_failure':
        assert report['storage_sealed'] is True, 'post-seal error must not rewrite F-LOG history'
        assert observation['application_exit'] != 0


@pytest.mark.parametrize('name,maximum', [('google.json', 65536), ('result.json', 131072)])
def test_serialized_byte_boundary_exact_and_plus_one(upload, tmp_path, name, maximum):
    document = {'padding': ''}
    document['padding'] = 'x' * (maximum - len(serialized(document)))
    assert len(serialized(document)) == maximum
    upload._d2_atomic_json(tmp_path, name, document, maximum)
    before = (tmp_path / name).read_bytes()
    assert before == serialized(document) and len(before) == maximum
    document['padding'] += 'x'
    assert len(serialized(document)) == maximum + 1
    with pytest.raises(Exception):
        upload._d2_atomic_json(tmp_path, name, document, maximum)
    assert (tmp_path / name).read_bytes() == before


def test_complete_serialization_rejects_nan_without_replacing_prior_bytes(upload, tmp_path):
    upload._d2_atomic_json(tmp_path, 'google.json', {'unicode': 'Привет'}, 65536)
    before = (tmp_path / 'google.json').read_bytes()
    assert before == serialized({'unicode': 'Привет'})
    with pytest.raises(Exception):
        upload._d2_atomic_json(tmp_path, 'google.json', {'value': float('nan')}, 65536)
    assert (tmp_path / 'google.json').read_bytes() == before


def test_valid_full_serialization_limit_and_minus_one(upload, tmp_path):
    document = upload._d2_checkpoint('a' * 32, 'b' * 32, 'c' * 40, 'google',
                                    [{'output_id': 'google-000001', 'group': 'google'}])
    encoded = serialized(document)
    upload._d2_atomic_json(tmp_path, 'checkpoint.json', document, len(encoded))
    assert (tmp_path / 'checkpoint.json').read_bytes() == encoded
    with pytest.raises(Exception):
        upload._d2_atomic_json(tmp_path, 'checkpoint.json', document, len(encoded) - 1)
    assert (tmp_path / 'checkpoint.json').read_bytes() == encoded


def test_full_512_output_serialization_has_no_silent_truncation(upload):
    rows = [{'output_id': 'ftp-' + format(index, '04d'), 'group': 'root',
             'transfer_state': 'ACKNOWLEDGED', 'commit_state': 'UNKNOWN'} for index in range(512)]
    document = {'outputs': rows}
    data = upload._d2_bytes(document)
    assert data == serialized(document)
    assert len(json.loads(data)['outputs']) == 512


def test_checkpoint_full_512_rows_all_transitions_fit_64k(upload, tmp_path):
    items = [{'output_id': 'ftp-' + format(index + 1, '06d'), 'group': 'ticker_lists'} for index in range(512)]
    document = upload._d2_checkpoint('a' * 32, 'b' * 32, 'c' * 40, 'ftp', items)
    for state in ('READY', 'RUNNING', 'RETURNED', 'FAILED', 'INTERRUPTED'):
        for transfer, commit in (('NOT_ATTEMPTED', 'NOT_ATTEMPTED'), ('UNKNOWN', 'UNKNOWN'), ('ACKNOWLEDGED', 'UNKNOWN')):
            current = copy.deepcopy(document)
            current.update(execution_state=state, normal_return_observed=state == 'RETURNED',
                           diagnostics_state='ACTIVE' if state in {'READY', 'RUNNING'} else 'COMPLETE' if state == 'RETURNED' else 'INCOMPLETE',
                           error_code='D2_SOURCE_CHANGED' if state in {'FAILED', 'INTERRUPTED'} else None)
            for row in current['outputs']:
                row.update(transfer_state=transfer, commit_state=commit)
            expected = serialized(current)
            assert len(expected) <= 65536
            upload._d2_atomic_json(tmp_path, 'checkpoint.json', current, 65536)
            assert (tmp_path / 'checkpoint.json').read_bytes() == expected
            assert len(json.loads(expected)['outputs']) == 512


def test_full_parent_result_with_512_outputs_fits_128k(upload):
    plan = {'google': [{'output_id': 'google-000001', 'group': 'google'}],
            'ftp': [{'output_id': 'ftp-' + format(index + 1, '06d'), 'group': 'ticker_lists'} for index in range(512)]}
    result = upload._d2_result('a' * 32, 'b' * 32, 'c' * 40, plan)
    for transfer, commit in (('NOT_ATTEMPTED', 'NOT_ATTEMPTED'), ('UNKNOWN', 'UNKNOWN'), ('ACKNOWLEDGED', 'UNKNOWN')):
        current = copy.deepcopy(result)
        for role, row in current['workers'].items():
            row.update(started=True, pid=8000 if role == 'google' else 8001, exitcode=1,
                       settled=True, alive=False, diagnostics_state='INCOMPLETE', error_code='D2_SOURCE_CHANGED')
            for item in row['outputs']:
                item.update(transfer_state=transfer, commit_state=commit)
        upload._d2_aggregate(current)
        data = serialized(current)
        assert upload._d2_bytes(current) == data and len(data) <= 131072
        assert len(json.loads(data)['workers']['ftp']['outputs']) == 512


def test_read_json_is_bounded_and_refuses_partial_evidence(upload, tmp_path):
    path = tmp_path / 'google.json'
    for data in (b'{', b'x' * 65537):
        path.write_bytes(data)
        path.chmod(0o600)
        with pytest.raises(Exception):
            upload._d2_read_json(path, 65536)


def test_atomic_replace_failure_preserves_original_bytes(upload, tmp_path, monkeypatch):
    upload._d2_atomic_json(tmp_path, 'google.json', {'sequence': 1}, 65536)
    before = (tmp_path / 'google.json').read_bytes()
    reached = []
    def refusal(source, target, *args, **kwargs):
        reached.append(True)
        raise PermissionError('D2_PRIVATE_CANARY_4c1e11')
    monkeypatch.setattr(upload.os, 'replace', refusal)
    with pytest.raises(Exception):
        upload._d2_atomic_json(tmp_path, 'google.json', {'sequence': 2}, 65536)
    assert reached == [True] and (tmp_path / 'google.json').read_bytes() == before


@pytest.mark.parametrize('at_deadline', [False, True])
def test_deadline_rechecked_after_actual_settlement_control_flow(upload, repo, at_deadline):
    """Exact production loop with explicit OS doubles, supplementing native hangs."""
    tree = ast.parse((repo / 'upload.py').read_text(encoding='utf-8'))
    supervise = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_d2_supervise')
    loops = [node for node in ast.walk(supervise) if isinstance(node, ast.While)
             and isinstance(node.test, ast.Constant) and node.test.value is True
             and any(isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name)
                     and statement.targets[0].id == 'alive' for statement in node.body)]
    assert len(loops) == 1
    function = ast.FunctionDef(name='observe_settlement', args=ast.arguments(posonlyargs=[],
        args=[ast.arg(arg='runtime'), ast.arg(arg='session')], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=[copy.deepcopy(loops[0]), ast.Return(value=ast.Constant(None))], decorator_list=[],
        returns=None, type_comment=None, type_params=[])
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    environment = {'time': types.SimpleNamespace(monotonic=lambda: 15 if at_deadline else 14.9),
                   '_d2_error': upload._d2_error, '_d2_check_tracker': upload._d2_check_tracker}
    exec(compile(module, '<D2-exact-settlement-loop>', 'exec'), environment)
    runtime = types.SimpleNamespace(children=[], deadline=15, deadline_detected=False, owns_tracker=False)
    if at_deadline:
        with pytest.raises(Exception, match='D2_DEADLINE'):
            environment['observe_settlement'](runtime, {})
        assert runtime.deadline_detected is True
    else:
        assert environment['observe_settlement'](runtime, {}) is None
        assert runtime.deadline_detected is False


@pytest.mark.parametrize('failed_allocation', [2, 3, 4])
def test_partial_pipe_allocation_closes_every_actual_owned_fd_once(upload, repo, tmp_path, monkeypatch, failed_allocation):
    """Execute the exact acquisition/finally source with real pipe descriptors."""
    tree = ast.parse((repo / 'upload.py').read_text(encoding='utf-8'))
    supervise = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_d2_supervise')
    candidates = [node for node in ast.walk(supervise) if isinstance(node, ast.Try)
        and node.body and isinstance(node.body[0], ast.Assign)
        and isinstance(node.body[0].targets[0], ast.Tuple)
        and [value.id for value in node.body[0].targets[0].elts] == ['read_end', 'write_end']]
    assert len(candidates) == 1 and candidates[0].finalbody
    runtime = object.__new__(upload._D2Runtime)
    runtime.owned_fds, runtime.fd_error = {}, False
    real_pipe, real_close = os.pipe, os.close
    foreign = os.open(tmp_path / 'foreign-owner', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    foreign_identity = os.fstat(foreign)
    allocations, closes, launches = [], [], []
    def pipe():
        if len(allocations) + 1 == failed_allocation:
            raise OSError('D2_FIXED_PIPE_ALLOCATION_FAILURE')
        pair = real_pipe()
        allocations.append(pair)
        return pair
    def closing(descriptor):
        closes.append(descriptor)
        real_close(descriptor)
    runtime.launch = lambda *args, **kwargs: launches.append(True)
    monkeypatch.setattr(upload.os, 'pipe', pipe)
    monkeypatch.setattr(upload.os, 'close', closing)
    environment = dict(vars(upload), runtime=runtime, local=[], item={}, payload=b'', descriptors=set())
    try:
        module = ast.fix_missing_locations(ast.Module(body=[copy.deepcopy(candidates[0])], type_ignores=[]))
        with pytest.raises(OSError, match='D2_FIXED_PIPE_ALLOCATION_FAILURE'):
            exec(compile(module, '<D2-exact-pipe-acquisition>', 'exec'), environment)
        owned = [descriptor for pair in allocations for descriptor in pair]
        assert len(allocations) == failed_allocation - 1
        assert sorted(closes) == sorted(owned) and len(closes) == len(set(closes))
        assert launches == [] and runtime.owned_fds == {} and runtime.fd_error is False
        assert os.fstat(foreign) == foreign_identity
        for descriptor in owned:
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        for descriptor in tuple(runtime.owned_fds):
            runtime.close_fd(descriptor)
        real_close(foreign)


def test_runtime_fd_identity_change_refuses_foreign_close_and_marks_incomplete(upload, tmp_path):
    runtime = object.__new__(upload._D2Runtime)
    runtime.owned_fds, runtime.fd_error = {}, False
    read_end, write_end = runtime.pipe()
    os.close(read_end)
    foreign = os.open(tmp_path / 'foreign-reused-owner', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        assert foreign == read_end
        identity = os.fstat(foreign)
        runtime.close_fd(read_end)
        runtime.close_fd(read_end)
        assert runtime.fd_error is True and read_end not in runtime.owned_fds
        assert os.fstat(foreign) == identity
    finally:
        runtime.close_fd(write_end)
        os.close(foreign)


@pytest.mark.parametrize('actual_exit', [0, 1, 2, 3])
def test_actual_tracker_wrapper_preserves_its_systemexit_status(upload, repo, monkeypatch, actual_exit):
    """Exact reviewed tracker branch; no synthetic native exit observation."""
    tree = ast.parse((repo / 'upload.py').read_text(encoding='utf-8'))
    worker = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_d2_worker_entry')
    branch = next(node for node in worker.body if isinstance(node, ast.If)
        and ast.unparse(node.test) == "ticket['role'] == 'tracker'")
    function = ast.FunctionDef(name='tracker_wrapper', args=ast.arguments(posonlyargs=[],
        args=[ast.arg(arg='ticket')], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=copy.deepcopy(branch.body), decorator_list=[], returns=None, type_comment=None, type_params=[])
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    calls = []
    def actual_tracker_boundary(descriptor):
        calls.append(descriptor)
        raise SystemExit(actual_exit)
    monkeypatch.setitem(__import__('sys').modules, 'multiprocessing.resource_tracker',
                        types.SimpleNamespace(main=actual_tracker_boundary))
    namespace = {}
    exec(compile(module, '<D2-exact-tracker-wrapper>', 'exec'), namespace)
    assert namespace['tracker_wrapper']({'tracker_fd': 90000}) == actual_exit
    assert calls == [90000]


@pytest.mark.parametrize('mutation', ['none', 'same_inode_rebind'])
def test_cache_custody_measurement_rejects_actual_same_slot_substitution(upload, fixture_worker, mutation):
    """A private Heap/mmap, never global BufferWrapper cache or warmup."""
    from multiprocessing import heap, util
    observer = fixture_worker._CacheProvenance(upload)
    private = heap.Heap()
    arena, backup = None, None
    try:
        block = private.malloc(1)
        arena = block[0]
        private.free(block)
        before = observer.measure(heap=private, record_arenas=(arena,))
        assert before['status'] == 'OBSERVED' and before['protected_touches'] == []
        assert before['owned_history'] == before['owned_current'] == []
        assert len(before['arenas']) == 1 and before['arenas'][0]['free_indexes_consistent'] is True
        if mutation == 'same_inode_rebind':
            pair = before['arenas'][0]['birth_pair']
            victim = next(row['fd'] for row in pair if row['fd'] != arena.fd)
            original_identity = os.fstat(victim)
            backup = observer.originals['dup'](arena.fd)
            # Connection._close's source-pinned default captured os.close
            # before instrumentation. Exercise that real alias, not its wrapper.
            connection = observer.connection.Connection(victim)
            connection.close()
            os.dup2(backup, victim)
            assert os.fstat(victim) == original_identity, 'negative control must retain the same inode identity'
        after = observer.measure(heap=private, record_arenas=(arena,))
        assert after['arenas'][0]['birth_pair_still_same'] is True
        assert after['arenas'][0]['same_buffer'] is True and after['arenas'][0]['buffer_closed'] is False
        assert after['arenas'][0]['buffer_size'] == arena.size
        if mutation == 'same_inode_rebind':
            assert after['status'] == 'UNKNOWN'
            assert {'connection_close', 'dup2_target'} <= {row['operation'] for row in after['protected_touches']}
        else:
            assert after['status'] == 'OBSERVED' and after['protected_touches'] == []
    finally:
        observer.close()
        if backup is not None:
            os.close(backup)
        if arena is not None:
            arena.buffer.close()
            for finalizer in list(util._finalizer_registry.values()):
                reference = getattr(finalizer, '_weakref', None)
                if reference is not None and reference() is arena:
                    finalizer()


@pytest.mark.parametrize('mutation', ['none', 'third_dup', 'foreign_pipe', 'changed_baseline',
    'pending_block', 'live_block', 'foreign_arena', 'foreign_mmap', 'wrong_source', 'incomplete_witness'])
def test_owned_fd_attribution_requires_the_entire_causal_proof(fixture_worker, oracle, mutation):
    """Schema-only mutations supplement the actual mmap substitution control."""
    pins = dict(oracle['cache_source_sha256'], upload='a' * 64)
    identity = [27, 100, 33152, 0]
    before = {3: (1, 3, 32768, 0)}
    after = dict(before)
    after.update({10: tuple(identity), 11: tuple(identity)})
    prior = dict(heap_object=1000, source_sha256=pins['heap'], allocated_block_count=0,
        pending_free_block_count=0, arenas=[], discard_threshold_bytes=4194304)
    arena = dict(arena_object=2000, fd=10, fd_identity=identity, size=4096,
                 allocated_blocks=0, pending_free_blocks=0)
    later = dict(prior, arenas=[arena])
    custody = dict(arena_object=2000, buffer_object=3000, buffer_type='mmap.mmap', size=4096,
        arena_fd=10, birth_pair=[{'fd': 10, 'identity': identity}, {'fd': 11, 'identity': identity}],
        birth_baseline_unchanged=True, finalizer_attested=True, in_actual_heap=True, same_arena_fd=True,
        same_buffer=True, buffer_closed=False, buffer_size=4096, birth_pair_still_same=True,
        allocated_blocks=0, pending_free_blocks=0, free_indexes_consistent=True)
    proof = dict(status='OBSERVED', observer_installed=True, observer_healthy=True,
        connection_default_attested=True, source_pins=pins, protected_touches=[],
        owned_history=[{'fd': 20, 'identity': [1, 100, 448, 0], 'physically_closed': True}],
        owned_current=[], owned_physically_closed=True, protected_disjoint_from_owned=True,
        arenas=[custody], event_counts={'arena_init': 1})
    if mutation == 'third_dup':
        after[12] = tuple(identity)
    elif mutation == 'foreign_pipe':
        after[12] = (1, 999, 4480, 0)
    elif mutation == 'changed_baseline':
        after[3] = (1, 333, 32768, 0)
    elif mutation == 'pending_block':
        later['pending_free_block_count'] = 1
    elif mutation == 'live_block':
        later['allocated_block_count'] = 1
    elif mutation == 'foreign_arena':
        custody['arena_object'] = 2001
    elif mutation == 'foreign_mmap':
        custody['same_buffer'] = False
    elif mutation == 'wrong_source':
        proof['source_pins'] = dict(pins, heap='b' * 64)
    elif mutation == 'incomplete_witness':
        proof.pop('connection_default_attested')
    actual = fixture_worker._owned_fd_attribution(before, after, prior, later, proof, pins)
    assert actual == ((True, 'PROVEN_CPYTHON_CACHE') if mutation == 'none'
                      else (False, 'ATTRIBUTION_UNKNOWN'))
