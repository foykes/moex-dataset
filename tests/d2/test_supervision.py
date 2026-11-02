"""Independent native outcomes and byte/schema boundaries for the D2 parent."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import types

import pytest


NATIVE_CASES = (
    'baseline_google_swallowed','baseline_ftp_root_swallowed','baseline_ftp_dividends_swallowed',
    'baseline_ftp_tickers_swallowed','baseline_exit23','baseline_hang',
    'success','google_error','ftp_error','exit23','system_exit0','system_exit_string',
    'abnormal_before_ready','abnormal_after_final','start_google_before','start_ftp_before',
    'start_google_after_native','start_ftp_after_native','hang_google','hang_ftp','cleanup_unsettled',
    'partial_ftp','partial_google','checkpoint_missing','checkpoint_corrupt','checkpoint_oversize',
    'checkpoint_stale_run','checkpoint_stale_pid','checkpoint_stale_sequence',
    'listener_failure','ack_failure','normal_without_final','final_then_exit23',
    'bootstrap_replay_failure','bootstrap_import_failure','bootstrap_unpickle_failure',
    'foreign_handle','canary_positive_control','native_nul_read_fault','native_nul_write_fault',
    'native_dup_read_fault','native_dup_write_fault','native_create_fault','d1_killed_holder',
    'parent_finalization_raises','parent_finalization_incomplete',
)

# Literal requested-operation oracle, independent of guard admission decisions.
FAULT_PHASES = {
    'baseline_google_swallowed': ('google', 'legacy_google_clear'),
    'baseline_ftp_root_swallowed': ('ftp', 'legacy_ftp_root'),
    'baseline_ftp_dividends_swallowed': ('ftp', 'legacy_ftp_dividends'),
    'baseline_ftp_tickers_swallowed': ('ftp', 'legacy_ftp_tickers'),
    'baseline_exit23': ('google', 'legacy_exit23'),
    'baseline_hang': ('google', 'legacy_hang'),
    'google_error': ('google', 'target_error'), 'ftp_error': ('ftp', 'target_error'),
    'exit23': ('google', 'target_exit23'), 'system_exit0': ('google', 'target_system_exit0'),
    'system_exit_string': ('google', 'target_system_exit_string'),
    'abnormal_before_ready': ('google', 'exit_before_ready'),
    'abnormal_after_final': ('google', 'exit_after_worker_final'),
    'normal_without_final': ('google', 'without_worker_final'),
    'final_then_exit23': ('google', 'exit_after_checkpoint'),
    'start_google_before': ('google', 'start_before_native'),
    'start_ftp_before': ('ftp', 'start_before_native'),
    'start_google_after_native': ('google', 'start_serialization_failure'),
    'start_ftp_after_native': ('ftp', 'start_serialization_failure'),
    'bootstrap_replay_failure': ('google', 'bootstrap_replay'),
    'bootstrap_import_failure': ('google', 'bootstrap_dependency_import'),
    'bootstrap_unpickle_failure': ('google', 'bootstrap_unpickle'),
    'hang_google': ('google', 'target_hang'), 'hang_ftp': ('ftp', 'target_hang'),
    'cleanup_unsettled': ('google', 'target_hang'),
    'partial_google': ('google', 'partial_transfer'), 'partial_ftp': ('ftp', 'partial_transfer'),
    'ack_failure': ('google', 'ack_closed'), 'listener_failure': ('controller', 'listener_stopped'),
    'parent_finalization_raises': ('controller', 'finalization_raises'),
    'parent_finalization_incomplete': ('controller', 'seal_inventory_failure'),
    'd1_killed_holder': ('holder', 'lock_holder_ready'),
}
for checkpoint_case in ('checkpoint_missing','checkpoint_corrupt','checkpoint_oversize',
                        'checkpoint_stale_run','checkpoint_stale_pid','checkpoint_stale_sequence'):
    FAULT_PHASES[checkpoint_case] = ('google', checkpoint_case)
for native_case in ('native_nul_read_fault','native_nul_write_fault','native_dup_read_fault',
                    'native_dup_write_fault','native_create_fault'):
    FAULT_PHASES[native_case] = ('google', native_case)


@pytest.mark.parametrize('case', NATIVE_CASES)
def test_native_case(case, offline_guard):
    observed = offline_guard.run_d2_case(case)
    assert observed['case'] == case
    assert observed['report_errors'] == []
    for native_row in observed['actual_pid_settlement']:
        assert native_row['witness'] == 'GUARD_RETAINED_HANDLE'
        if native_row['settled']:
            assert native_row['exit_receipt_verified'] is True
    if case in FAULT_PHASES:
        worker_id, phase = FAULT_PHASES[case]
        witnesses = observed['bundle']['fault_witnesses'][worker_id]
        matches = [row for row in witnesses if row['phase'] == phase]
        assert len(matches) == 1
        witness = matches[0]
        assert witness['count'] == 1 and witness['case'] == case
        assert witness['worker_id'] == worker_id and type(witness['pid']) is int and witness['pid'] > 0
        assert witness['source'] in {'worker','native','controller'}
        assert len(witness['ticket_id']) == 32
    if case.startswith('baseline_'):
        assert observed['baseline_red'] is True
        assert observed['actual_pid_settlement']
        assert all(row['settled'] for row in observed['actual_pid_settlement'])
        if case.endswith('swallowed'):
            assert observed['bundle']['legacy_operation_fault_observed'] is True
            assert observed['bundle']['legacy_parent_returned_none'] is True
            assert observed['bundle']['legacy_false_completion_observed'] is True
            assert len(observed['actual_pid_settlement']) == 2
            assert all(row['actual_exit'] == 0 for row in observed['actual_pid_settlement'])
        if case == 'baseline_exit23':
            assert any(row['actual_exit'] == 23 for row in observed['actual_pid_settlement'])
        if case == 'baseline_hang':
            assert observed['job_used_for_settlement'] is True
        return
    if case == 'canary_positive_control':
        assert observed['capture_positive_control'] is True
        assert observed['evidence_capture_positive_control'] is True
        return
    assert observed['watchdog_timed_out'] is False
    assert observed['console_canary_absent'] is True
    assert observed['evidence_canary_absent'] is True
    assert observed['canary_absent'] is True
    bundle = observed['bundle']
    if case == 'd1_killed_holder':
        assert bundle['holder_settled'] is True
        assert len(observed['actual_pid_settlement']) == 2
        assert {row['worker_id']: row['actual_exit'] for row in observed['actual_pid_settlement']} == {'holder': -15, 'contender': 0}
        assert bundle['result']['holder_final_present'] is False
        assert bundle['result']['holder_terminal_present'] is False
        assert bundle['contender_acquired'] is True
        assert bundle['same_physical_target'] is True
        assert bundle['previous_bytes_preserved'] is True
        assert bundle['previous_hash_preserved'] is True
        return
    reader_lifetime = bundle['reader_lifetime']
    assert reader_lifetime['lifetime_complete'] == reader_lifetime['reads']
    result = bundle['result']
    assert bundle['logging_report']['run_id'] == result['run_id']
    assert bundle['logging_report']['release_result'] is None
    producers = {row['worker_id']: row for row in bundle['logging_report']['expected_producers']}
    assert set(producers) == {'parent', 'google', 'ftp'}
    assert result['committed_outputs'] == []
    assert set(result['workers']) == {'google','ftp'}
    assert all(row['required'] is True for row in result['workers'].values())
    assert 'release_result' not in result
    assert 'current_generation' not in result
    assert 'previous_preserved' not in result
    assert {row['commit_state'] for worker in result['workers'].values() for row in worker['outputs']} <= {'NOT_ATTEMPTED','UNKNOWN'}
    persisted = bundle.get('persisted_result')
    if persisted is not None:
        assert persisted['receipt_scope'] == 'PREFINAL_SUPERVISION'
        assert persisted['finalization_state'] == 'NOT_OBSERVED'
        assert persisted['diagnostic_success_allowed'] is False
        assert persisted['committed_outputs'] == []
    os_rows = {row['worker_id']:row for row in observed['actual_pid_settlement']}
    phases = bundle['phase_witnesses']
    for phase_row in phases:
        if phase_row['kind'] == 'CHECKPOINT_READ' and phase_row['go_released']:
            assert phase_row['phase'] == 'OS_SETTLED'
            if phase_row['worker_id'] in {'google', 'ftp'}:
                actual = os_rows[phase_row['worker_id']]
                assert phase_row['settled'] is True and phase_row['exit_receipt_verified'] is True
                assert phase_row['witness'] == 'GUARD_RETAINED_HANDLE'
                assert phase_row['pid'] == actual['pid'] and phase_row['actual_exit'] == actual['actual_exit']
        if phase_row['kind'] == 'FULL_INVENTORY':
            assert phase_row['reader_active'] is False
            assert phase_row['single_link_regular'] is True
            if phase_row['go_released']:
                assert phase_row['phase'] == 'OS_SETTLED' and phase_row['graph_quiescent'] is True
    for worker_id, slot in bundle['slot_effects']['workers'].items():
        assert slot['pid'] == os_rows[worker_id]['pid'] and slot['exit_receipt_verified'] is True
        assert slot['witness'] == 'GUARD_WORKER_FINAL'
        assert slot['peer_slot_attempts'] == 0
        assert slot['admitted_writes'] > 0 and slot['own_metadata'] > 0 and slot['own_opens'] > 0
        assert slot['phase_witnesses']
        assert all(row['kind'] == 'SLOT_WRITE' and row['phase'] == 'OWN_SLOT'
                   and row['worker_id'] == worker_id for row in slot['phase_witnesses'])
    if case in {'success', 'foreign_handle', 'parent_finalization_raises', 'parent_finalization_incomplete'}:
        assert set(bundle['ready_reader_witnesses']) == {'google', 'ftp'}
        assert set(bundle['slot_effects']['workers']) == {'google', 'ftp'}
        for worker_id, reader in bundle['ready_reader_witnesses'].items():
            assert reader['pid'] == os_rows[worker_id]['pid']
            assert reader['lifetime_complete'] is True and reader['stream_closed'] is True
            assert reader['fd_close_calls'] == 1 and reader['native_close_calls'] == 0
        assert any(row['kind'] == 'FULL_INVENTORY' and row['phase'] == 'READY_BARRIER'
                   and row['ready_workers_verified'] == 2 and row['file_count'] == 2 for row in phases)
        assert any(row['kind'] == 'FULL_INVENTORY' and row['phase'] == 'OS_SETTLED'
                   and row['graph_quiescent'] is True for row in phases)
    for worker_id, worker in result['workers'].items():
        if worker['started']:
            assert worker_id in os_rows
            assert worker['pid'] == os_rows[worker_id]['pid']
            if worker['settled']:
                assert worker['exitcode'] == os_rows[worker_id]['actual_exit']
    if case in {'success','foreign_handle'}:
        assert reader_lifetime['reads'] > 0
        assert reader_lifetime['native_closes'] == 0
        assert reader_lifetime['fd_closes'] == reader_lifetime['reads']
        assert observed['actual_exit'] == 0
        assert result['parent_outcome'] == 'RETURNED'
        assert result['final_exit_status'] == 0
        assert result['diagnostics_state'] == 'COMPLETE'
        assert result['diagnostic_success_allowed'] is True
        assert result['unknown_outputs']
        assert observed['job_used_for_settlement'] is False
        assert all(worker['started'] and worker['settled'] and worker['alive'] is False
                   and worker['exitcode'] == 0 and worker['normal_return_observed']
                   and worker['diagnostics_state'] == 'COMPLETE' for worker in result['workers'].values())
        assert len(observed['actual_pid_settlement']) == 2
        assert all(row['settled'] and row['actual_exit'] == 0 for row in observed['actual_pid_settlement'])
        assert len({row['pid'] for row in observed['actual_pid_settlement']}) == 2
        assert producers['parent']['final_confirmed'] is True
        for worker_id in ('google', 'ftp'):
            assert producers[worker_id]['final_report']['execution_outcome'] == 'COMPLETED'
            assert producers[worker_id]['final_report']['exit_status'] == 0
        for worker_id in ('google','ftp'):
            assert any(row['phase'] == 'stdio_canary' and row['count'] == 1
                       for row in bundle['fault_witnesses'][worker_id])
        if case == 'success':
            assert all(bundle['dependency_observation'].values())
        if case == 'foreign_handle':
            assert bundle['foreign_event_unchanged'] is True
            assert bundle['foreign_file_bytes_preserved'] is True
            assert bundle['caller_inheritance_preserved'] is True
            assert bundle['native_stdio_handle_list_exact'] is True
    else:
        assert observed['actual_exit'] != 0
        assert result['final_exit_status'] != 0
        assert result['diagnostic_success_allowed'] is False
        assert result['parent_outcome'] != 'RETURNED' or result['diagnostics_state'] == 'INCOMPLETE'
        if case == 'cleanup_unsettled':
            assert bundle['parent_deadline_expired'] is True
            assert bundle['parent_cleanup_requested'] is True
            assert bundle['parent_observed_unsettled'] is True
            assert any(row['settled'] is False and row['alive'] is True for row in result['workers'].values())
            assert observed['job_used_for_settlement'] is True
        else:
            assert observed['job_used_for_settlement'] is False
            assert all(row['settled'] for row in observed['actual_pid_settlement'])
        if case in {'hang_google','hang_ftp'}:
            assert bundle['parent_deadline_expired'] is True
            assert bundle['parent_cleanup_requested'] is True
            assert bundle['target_entered_before_deadline'] is True
            assert 14.0 <= bundle['parent_deadline_elapsed_s'] < 25.0
            assert any(row['timed_out'] for row in result['workers'].values())
        if case in {'partial_ftp','partial_google'}:
            assert result['unknown_outputs']
            worker = result['workers']['ftp' if case == 'partial_ftp' else 'google']
            assert any(row['transfer_state'] == 'UNKNOWN' for row in worker['outputs'])
            if case == 'partial_ftp':
                assert any(row['transfer_state'] == 'ACKNOWLEDGED' for row in worker['outputs'])
        if case in {'exit23','final_then_exit23'}:
            assert any(row['actual_exit'] == 23 for row in os_rows.values())
        if case in {'abnormal_before_ready','abnormal_after_final'}:
            assert os_rows['google']['actual_exit'] == 23
        if case == 'abnormal_before_ready':
            assert result['workers']['google']['checkpoint_state'] == 'MISSING'
            assert producers['google']['final_report'] is None
        if case == 'abnormal_after_final':
            assert result['workers']['google']['execution_state'] == 'RUNNING'
            assert producers['google']['final_report']['execution_outcome'] == 'COMPLETED'
            assert producers['google']['final_report']['exit_status'] == 0
        if case == 'final_then_exit23':
            assert result['workers']['google']['execution_state'] == 'RETURNED'
            assert producers['google']['final_report']['execution_outcome'] == 'COMPLETED'
            assert producers['google']['final_report']['exit_status'] == 0
        if case == 'normal_without_final':
            assert producers['google']['final_report'] is None
            assert result['workers']['google']['execution_state'] == 'RETURNED'
            assert result['workers']['google']['normal_return_observed'] is True
            assert result['workers']['google']['diagnostics_state'] == 'INCOMPLETE'
            assert result['workers']['google']['error_code'] == 'D2_DIAGNOSTICS_INCOMPLETE'
        if case in {'google_error','ftp_error'}:
            failed_worker = 'ftp' if case == 'ftp_error' else 'google'
            assert producers[failed_worker]['final_report']['execution_outcome'] == 'FAILED'
            assert producers[failed_worker]['final_report']['exit_status'] == 1
        if case in {'system_exit0', 'system_exit_string'}:
            assert producers['google']['final_report']['execution_outcome'] == 'INTERRUPTED'
            assert producers['google']['final_report']['exit_status'] == 1
        if case in {'google_error','ftp_error','system_exit0','system_exit_string','normal_without_final'}:
            worker_id = 'ftp' if case == 'ftp_error' else 'google'
            actual_exit = os_rows[worker_id]['actual_exit']
            if actual_exit == -15:
                # A failed final/checkpoint precedes process exit. Parent cleanup
                # may win that race; independently prove its accepted native request.
                requests = [row for row in bundle['cleanup_requests']
                    if row['worker_id'] == worker_id and row['pid'] == os_rows[worker_id]['pid']]
                assert any(row['witness'] == 'GUARD_RETAINED_HANDLE' and row['alive'] is True
                    and row['accepted'] is True and row['native_calls'] == 1
                    and row['phase'] in {'TERMINATE','KILL'} and row['requested_exit'] == 65536
                    and row['case'] == case and row['run_id'] == witness['run_id']
                    and row['nodeid'] == 'tests/d2/test_supervision.py::test_native_case[' + case + ']'
                    for row in requests)
            else:
                assert actual_exit == 1
        if case in {'start_google_after_native','start_ftp_after_native'}:
            worker_id = 'google' if 'google' in case else 'ftp'
            assert result['workers'][worker_id]['started'] is True
            assert worker_id in os_rows and os_rows[worker_id]['settled'] is True
        if case.startswith('bootstrap_'):
            assert os_rows
            assert any(row['actual_exit'] != 0 for row in os_rows.values())
            assert any(row['normal_return_observed'] is False for row in result['workers'].values())
        if case.startswith('checkpoint_'):
            expected_state = 'MISSING' if case == 'checkpoint_missing' else 'INVALID'
            assert any(row['checkpoint_state'] == expected_state for row in result['workers'].values())
            assert result['unknown_outputs']
        if case in {'parent_finalization_raises','parent_finalization_incomplete'}:
            assert result['parent_outcome'] == 'RETURNED'
            assert result['diagnostics_state'] == 'INCOMPLETE'
            assert bundle['persisted_bytes_unchanged'] is True
            assert bundle['persisted_sha256_before'] == bundle['persisted_sha256_after']
            assert persisted is not None
            counter = 'seal_inventory_fail_count' if case.endswith('incomplete') else 'finalization_raise_count'
            assert bundle['parent_faults'][counter] == 1
            assert bundle['parent_faults']['application_finalize_count'] == 1
            assert bundle['main_status'] != 0
            preseal = bundle['preseal_preconditions']
            assert preseal['final_absent'] is True and preseal['summary_absent'] is True
            assert preseal['finalizer_lock_held'] is True and preseal['application_lock_released'] is True
            assert preseal['actual_children_settled'] == preseal['actual_children_exit_zero'] == 2
            assert preseal['worker_final_completed_zero'] == 2 and preseal['persisted_operation_returned'] is True
            assert len({row['pid'] for row in os_rows.values()}) == 2
            assert all(row['settled'] and row['actual_exit'] == 0 for row in os_rows.values())
            assert all(worker['execution_state'] == 'RETURNED' and worker['normal_return_observed']
                       and worker['diagnostics_state'] == 'COMPLETE'
                       for worker in result['workers'].values())
            assert all(producers[worker]['final_report']['execution_outcome'] == 'COMPLETED'
                       and producers[worker]['final_report']['exit_status'] == 0
                       for worker in ('google', 'ftp'))
            if case.endswith('raises'):
                assert preseal['fault_boundary'] == 'FREEZE_ENTRY' and preseal['application_state'] == 'ACTIVE'
                assert preseal['registration_frozen'] is False and preseal['transport_closed'] is False
                assert result['storage_sealed'] == 'UNKNOWN'
                assert bundle['logging_report_scope'] == 'TEST_CLEANUP'
                assert bundle['parent_faults']['cleanup_finalize_count'] == 1
                assert bundle['logging_report']['execution_outcome'] == 'FAILED'
                assert bundle['logging_report']['exit_status'] != 0
            else:
                assert preseal['fault_boundary'] == 'SEAL_INVENTORY' and preseal['application_state'] == 'FINALIZING'
                assert preseal['registration_frozen'] is True and preseal['transport_closed'] is True
                assert result['storage_sealed'] is False
                assert bundle['logging_report_scope'] == 'APPLICATION'
                assert bundle['parent_faults']['cleanup_finalize_count'] == 0
                assert bundle['logging_report']['delivery_outcome'] == 'INCOMPLETE'
                assert bundle['logging_report']['evidence_incomplete'] is True
                assert bundle['logging_report']['storage_sealed'] is False
                assert bundle['authoritative_summary_reason'] == 'LOG_SEAL_FAILED'


def checkpoint(count=1):
    return dict(kind='D2_WORKER_CHECKPOINT', receipt_version=1, run_id='r' * 32,
        attempt_id='a' * 32, producer_sha='b' * 40, worker_id='ftp', pid=42,
        sequence=1, execution_state='READY', normal_return_observed=False,
        diagnostics_state='ACTIVE', lock_state='NOT_USED', error_code=None,
        outputs=[dict(output_id='ftp-' + str(i + 1).zfill(6), group='ticker_lists',
                      transfer_state='NOT_ATTEMPTED', commit_state='NOT_ATTEMPTED') for i in range(count)])


def serialized(document):
    return (json.dumps(document, ensure_ascii=True, allow_nan=False, sort_keys=True,
                       separators=(',', ':')) + '\n').encode('utf-8')


def test_checkpoint_full_512_rows_all_transitions_fit_64k(upload, tmp_path):
    document = checkpoint(512)
    independent = serialized(document)
    assert len(independent) < 65536
    assert upload._d2_bytes(document) == independent
    # 113 bytes for the longest closed row, plus this oracle's trailing LF.
    assert len(serialized(document['outputs'][0])) == 114
    assert all(len(serialized(row)) <= 114 for row in document['outputs'])
    for state, transfer, commit in [('READY','NOT_ATTEMPTED','NOT_ATTEMPTED'),
            ('RUNNING','UNKNOWN','UNKNOWN'),('RETURNED','ACKNOWLEDGED','UNKNOWN'),
            ('FAILED','UNKNOWN','UNKNOWN'),('INTERRUPTED','UNKNOWN','UNKNOWN')]:
        document['execution_state'] = state
        document['normal_return_observed'] = state == 'RETURNED'
        document['diagnostics_state'] = 'COMPLETE' if state == 'RETURNED' else 'INCOMPLETE'
        document['error_code'] = None if state == 'RETURNED' else 'D2_CLEANUP_UNSETTLED'
        for row in document['outputs']:
            row.update(transfer_state=transfer, commit_state=commit)
        expected = serialized(document)
        assert len(expected) < 65536
        upload._d2_write_checkpoint(tmp_path, 'ftp', document)
        data = (tmp_path / 'ftp.json').read_bytes()
        assert data == expected
        assert len(json.loads(data)['outputs']) == 512


def test_serialized_byte_boundary_exact_and_plus_one(upload, tmp_path):
    # Diagnostic byte admission is independently tested; this payload is not a worker schema claim.
    document = {'padding':''}
    overhead = len(serialized(document))
    document['padding'] = 'x' * (65536 - overhead)
    assert len(serialized(document)) == 65536
    upload._d2_write_checkpoint(tmp_path, 'ftp', document)
    before = (tmp_path / 'ftp.json').read_bytes()
    assert len(before) == 65536 and before == serialized(document)
    document['padding'] += 'x'
    assert len(serialized(document)) == 65537
    with pytest.raises(Exception, match='D2_RESULT_LIMIT'):
        upload._d2_write_checkpoint(tmp_path, 'ftp', document)
    assert (tmp_path / 'ftp.json').read_bytes() == before
    assert hashlib.sha256(before).digest() == hashlib.sha256((tmp_path / 'ftp.json').read_bytes()).digest()


def test_valid_full_serialization_limit_and_minus_one(upload, tmp_path):
    document = checkpoint()
    document['sequence'] = 2
    actual = serialized(document)
    upload._d2_write_checkpoint(tmp_path, 'ftp', document, {'max_checkpoint_bytes':len(actual)})
    assert (tmp_path / 'ftp.json').read_bytes() == actual
    with pytest.raises(Exception, match='D2_RESULT_LIMIT'):
        upload._d2_write_checkpoint(tmp_path, 'ftp', document, {'max_checkpoint_bytes':len(actual)-1})
    assert (tmp_path / 'ftp.json').read_bytes() == actual


@pytest.mark.parametrize('fault', ['missing','corrupt','oversize','run','attempt','pid','sequence',
    'foreign_key','state','commit','producer','worker','output_id','group','count','duplicate',
    'nan','duplicate_keys','trailing','normal_marker','unhashable_state'])
def test_checkpoint_schema_identity_and_corruption_fail_closed(upload, tmp_path, fault):
    document = checkpoint()
    path = tmp_path / 'ftp.json'
    if fault == 'corrupt':
        path.write_bytes(b'{broken')
    elif fault == 'oversize':
        path.write_bytes(b'x' * 65537)
    elif fault != 'missing':
        if fault == 'run': document['run_id'] = 'other'
        if fault == 'attempt': document['attempt_id'] = 'other'
        if fault == 'pid': document['pid'] = 43
        if fault == 'sequence': document['sequence'] = 0
        if fault == 'foreign_key': document['private_path'] = 'D2_SYNTHETIC_SECRET_CANARY'
        if fault == 'state': document['execution_state'] = 'VERIFIED_RELEASE'
        if fault == 'commit': document['outputs'][0]['commit_state'] = 'VERIFIED'
        if fault == 'producer': document['producer_sha'] = 'c' * 40
        if fault == 'worker': document['worker_id'] = 'google'
        if fault == 'output_id': document['outputs'][0]['output_id'] = 'ftp-999999'
        if fault == 'group': document['outputs'][0]['group'] = 'root'
        if fault == 'count': document['outputs'] = []
        if fault == 'duplicate': document['outputs'].append(copy.deepcopy(document['outputs'][0]))
        if fault == 'normal_marker': document['normal_return_observed'] = True
        if fault == 'unhashable_state': document['execution_state'] = []
        data = serialized(document)
        if fault == 'nan': data = data.replace(b'"sequence":1', b'"sequence":NaN')
        if fault == 'duplicate_keys': data = data.replace(b'"sequence":1', b'"sequence":0,"sequence":1')
        if fault == 'trailing': data += b'{}'
        path.write_bytes(data)
    with pytest.raises(Exception, match='D2_CHECKPOINT_INVALID'):
        upload._d2_read_checkpoint(tmp_path, 'ftp', run_id='r' * 32, attempt_id='a' * 32,
            pid=42, producer_sha='b' * 40, outputs=checkpoint()['outputs'])


def test_worker_sequence_cannot_rewind(upload, tmp_path):
    document = checkpoint()
    document['sequence'] = 2
    upload._d2_write_checkpoint(tmp_path, 'ftp', document)
    observed = upload._d2_read_checkpoint(tmp_path, 'ftp', run_id='r' * 32, attempt_id='a' * 32,
        pid=42, producer_sha='b' * 40, outputs=checkpoint()['outputs'], minimum_sequence=2)
    assert observed['sequence'] == 2
    with pytest.raises(Exception, match='D2_CHECKPOINT_INVALID'):
        upload._d2_read_checkpoint(tmp_path, 'ftp', run_id='r' * 32, attempt_id='a' * 32,
            pid=42, producer_sha='b' * 40, outputs=checkpoint()['outputs'], minimum_sequence=3)


@pytest.mark.parametrize('fault', ['early_final', 'same_sequence_change', 'ack_regression', 'terminal_reopened'])
def test_checkpoint_lifecycle_and_content_cannot_regress(upload, tmp_path, fault):
    previous = checkpoint(2)
    previous.update(sequence=4, execution_state='RUNNING')
    previous['outputs'][0].update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    row = upload._d2_empty_worker('ftp', previous['outputs'])
    row.update(started=True, pid=42)
    current = copy.deepcopy(previous)
    floor = 4
    if fault == 'early_final':
        previous = checkpoint(2)
        floor = 1
        current = copy.deepcopy(previous)
        current.update(execution_state='RETURNED', normal_return_observed=True, diagnostics_state='COMPLETE')
        for output in current['outputs']:
            output.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    elif fault == 'same_sequence_change':
        for output in current['outputs']:
            output.update(transfer_state='UNKNOWN', commit_state='UNKNOWN')
    elif fault == 'ack_regression':
        current['sequence'] = 5
        current['outputs'][0].update(transfer_state='UNKNOWN', commit_state='UNKNOWN')
        current['outputs'][1].update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    else:
        previous.update(sequence=3, execution_state='FAILED', error_code='D2_CHILD_FAILED')
        for output in previous['outputs']:
            output.update(transfer_state='NOT_ATTEMPTED', commit_state='NOT_ATTEMPTED')
        floor = 3
    upload._d2_write_checkpoint(tmp_path, 'ftp', current)
    before = copy.deepcopy(row)
    with pytest.raises(Exception, match='D2_CHECKPOINT_INVALID'):
        upload._d2_accept_checkpoint(row, tmp_path, 'r' * 32, 'a' * 32, 'b' * 40,
            upload._d2_validate_limits(None), floor, previous_record=previous)
    assert row == before


def test_checkpoint_skipped_polls_and_identical_reread_are_valid(upload, tmp_path):
    previous = checkpoint(2)
    current = copy.deepcopy(previous)
    current.update(sequence=7, execution_state='RETURNED', normal_return_observed=True,
        diagnostics_state='COMPLETE')
    for output in current['outputs']:
        output.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    row = upload._d2_empty_worker('ftp', previous['outputs'])
    row.update(started=True, pid=42)
    upload._d2_write_checkpoint(tmp_path, 'ftp', current)
    observed = upload._d2_accept_checkpoint(row, tmp_path, 'r' * 32, 'a' * 32, 'b' * 40,
        upload._d2_validate_limits(None), 1, previous_record=previous)
    assert observed == current
    reread = upload._d2_accept_checkpoint(row, tmp_path, 'r' * 32, 'a' * 32, 'b' * 40,
        upload._d2_validate_limits(None), 7, previous_record=observed)
    assert reread == observed and serialized(reread) == serialized(current)


def test_parent_result_byte_boundary_and_plus_one(upload, tmp_path):
    document = {'padding':''}
    document['padding'] = 'x' * (131072 - len(serialized(document)))
    limits = upload._d2_validate_limits(None)
    assert len(serialized(document)) == 131072
    upload._d2_atomic_json(tmp_path, 'result.json', document, 131072, limits)
    before = (tmp_path / 'result.json').read_bytes()
    assert before == serialized(document)
    document['padding'] += 'x'
    with pytest.raises(Exception, match='D2_RESULT_LIMIT'):
        upload._d2_atomic_json(tmp_path, 'result.json', document, 131072, limits)
    assert (tmp_path / 'result.json').read_bytes() == before


def test_total_run_byte_boundary_and_plus_one(upload, tmp_path):
    # Exercise total-byte admission independently of checkpoint schema validity.
    # Valid six-file traffic is smaller than 1 MiB; corrupted existing bytes still count.
    document = {'status':'BOUNDARY'}
    incoming = serialized(document)
    other = tmp_path / 'ftp.json'
    other.write_bytes(b'x' * (1048576 - len(incoming)))
    limits = upload._d2_validate_limits(None)
    upload._d2_atomic_json(tmp_path, 'result.json', document, 131072, limits)
    assert sum(path.stat().st_size for path in tmp_path.iterdir()) == 1048576
    preserved = (tmp_path / 'result.json').read_bytes()
    (tmp_path / 'result.json').unlink()
    other.write_bytes(other.read_bytes() + b'x')
    before = other.read_bytes()
    with pytest.raises(Exception, match='D2_RESULT_LIMIT'):
        upload._d2_atomic_json(tmp_path, 'result.json', document, 131072, limits)
    assert other.read_bytes() == before
    assert not (tmp_path / 'result.json').exists()
    assert preserved == incoming


def slot_fixture(upload, root):
    plan = {'google': [dict(output_id='google-000001', group='google')],
            'ftp': [dict(output_id='ftp-000001', group='root')]}
    limits = upload._d2_validate_limits(None)
    info = root.lstat()
    reservation = upload._d2_make_reservation(plan, limits, 'r' * 32, 'a' * 32,
        'b' * 40, (info.st_dev, info.st_ino))
    record = upload._d2_checkpoint('r' * 32, 'a' * 32, 'b' * 40, 'google', plan['google'])
    record['sequence'] = 1
    assert record['pid'] == os.getpid()
    return limits, reservation, record


@pytest.mark.parametrize('fault', ['none', 'own_oversize', 'own_hardlink', 'own_reparse'])
def test_slot_writer_preserves_own_checks_without_sibling_metadata(upload, tmp_path, monkeypatch, fault):
    limits, reservation, record = slot_fixture(upload, tmp_path)
    own, sibling = tmp_path / 'google.json', tmp_path / 'ftp.json'
    before = serialized(record)
    own.write_bytes(before)
    sibling_bytes = b'SIBLING_BYTES_MUST_REMAIN_UNOBSERVED'
    sibling.write_bytes(sibling_bytes)
    if fault == 'own_oversize':
        bound = upload._d2_validate_reservation(reservation, limits)['budget']['checkpoint_bytes'][0]
        own.write_bytes(b'X' * (bound + 1))
        before = own.read_bytes()
    retained = Path.lstat
    observed = []
    def inspect_leaf(path, *arguments, **options):
        if path in {sibling, tmp_path / 'ftp.pending'}:
            raise AssertionError('LIVE_SIBLING_METADATA_ATTEMPT')
        info = retained(path, *arguments, **options)
        if path == own:
            observed.append(path.name)
            if fault in {'own_hardlink', 'own_reparse'}:
                values = {name: getattr(info, name) for name in dir(info) if name.startswith('st_')}
                values['st_nlink'] = 2 if fault == 'own_hardlink' else 1
                values['st_file_attributes'] = 1024 if fault == 'own_reparse' else 0
                return types.SimpleNamespace(**values)
        return info
    with monkeypatch.context() as guarded:
        guarded.setattr(Path, 'lstat', inspect_leaf)
        if fault == 'none':
            upload._d2_write_checkpoint(tmp_path, 'google', record, limits, reservation=reservation)
        else:
            with pytest.raises(RuntimeError, match='D2_'):
                upload._d2_write_checkpoint(tmp_path, 'google', record, limits, reservation=reservation)
    assert observed, 'Own-slot security metadata was not checked'
    assert own.read_bytes() == before
    assert sibling.read_bytes() == sibling_bytes
    assert not (tmp_path / 'google.pending').exists()


@pytest.mark.parametrize('fault', ['bounds', 'limits', 'identity'])
def test_slot_writer_refuses_forged_reservation(upload, tmp_path, monkeypatch, fault):
    limits, reservation, record = slot_fixture(upload, tmp_path)
    forged = list(reservation)
    if fault == 'bounds':
        forged[6] = tuple(value - 1 for value in forged[6])
    elif fault == 'limits':
        forged[5] = tuple((name, value + 1) for name, value in forged[5])
    else:
        forged[0] = 's' * 32
    effects = []
    retained = Path.lstat
    def inspect_leaf(path, *arguments, **options):
        if path.parent == tmp_path:
            effects.append(path.name)
        return retained(path, *arguments, **options)
    monkeypatch.setattr(Path, 'lstat', inspect_leaf)
    with pytest.raises(RuntimeError, match='D2_'):
        upload._d2_write_checkpoint(tmp_path, 'google', record, limits, reservation=tuple(forged))
    assert effects == []
    assert not (tmp_path / 'google.json').exists()
    assert not (tmp_path / 'google.pending').exists()


def test_slot_writer_refuses_existing_pending(upload, tmp_path):
    limits, reservation, record = slot_fixture(upload, tmp_path)
    own, pending = tmp_path / 'google.json', tmp_path / 'google.pending'
    old, pending_bytes = serialized(record), b'RETAIN_FAILED_PENDING'
    own.write_bytes(old)
    pending.write_bytes(pending_bytes)
    with pytest.raises(RuntimeError, match='D2_'):
        upload._d2_write_checkpoint(tmp_path, 'google', record, limits, reservation=reservation)
    assert own.read_bytes() == old
    assert pending.read_bytes() == pending_bytes


@pytest.mark.parametrize('field', ['max_checkpoint_bytes', 'max_result_bytes', 'max_run_bytes'])
def test_slot_reservation_enforces_reduced_limits(upload, tmp_path, field):
    limits, reservation, record = slot_fixture(upload, tmp_path)
    verified = upload._d2_validate_reservation(reservation, limits)
    budget = verified['budget']
    boundary = {'max_checkpoint_bytes': max(budget['checkpoint_bytes']),
                'max_result_bytes': budget['result_bytes'],
                'max_run_bytes': budget['peak_run_bytes']}[field]
    exact = dict(limits, **{field: boundary})
    identity = (tmp_path.lstat().st_dev, tmp_path.lstat().st_ino)
    accepted = upload._d2_make_reservation(verified['plan'], exact, 'r' * 32,
        'a' * 32, 'b' * 40, identity)
    assert upload._d2_validate_reservation(accepted, exact)['budget'] == budget
    with pytest.raises(RuntimeError, match='D2_RESULT_LIMIT'):
        upload._d2_make_reservation(verified['plan'], dict(exact, **{field: boundary - 1}),
            'r' * 32, 'a' * 32, 'b' * 40, identity)
    assert list(tmp_path.iterdir()) == []


def test_slot_reservation_bounds_full_transition_shapes(upload, tmp_path):
    plan = {'google': [dict(output_id='google-000001', group='google')],
            'ftp': [dict(output_id=f'ftp-{index:06d}', group=('root', 'dividends', 'ticker_lists')[index % 3])
                    for index in range(1, 513)]}
    limits = upload._d2_validate_limits(None)
    info = tmp_path.lstat()
    reservation = upload._d2_make_reservation(plan, limits, 'r' * 32, 'a' * 32,
        'b' * 40, (info.st_dev, info.st_ino))
    bounds = upload._d2_validate_reservation(reservation, limits)['budget']['checkpoint_bytes']
    progress = {'NOT_ATTEMPTED': 0, 'UNKNOWN': 1, 'ACKNOWLEDGED': 2}
    for order, worker in enumerate(('google', 'ftp')):
        for transfer in ('NOT_ATTEMPTED', 'UNKNOWN', 'ACKNOWLEDGED'):
            for state in ('RUNNING', 'FAILED', 'INTERRUPTED'):
                record = upload._d2_checkpoint('r' * 32, 'a' * 32, 'b' * 40, worker, plan[worker])
                record.update(pid=4294967295, sequence=(2 if state == 'RUNNING' else 3)
                    + progress[transfer] * len(plan[worker]), execution_state=state,
                    diagnostics_state='ACTIVE' if state == 'RUNNING' else 'INCOMPLETE',
                    error_code=None if state == 'RUNNING' else 'D2_DIAGNOSTICS_INCOMPLETE')
                for output in record['outputs']:
                    output.update(transfer_state=transfer,
                        commit_state='NOT_ATTEMPTED' if transfer == 'NOT_ATTEMPTED' else 'UNKNOWN')
                data = serialized(record)
                assert len(data) <= bounds[order] <= 65536
                decoded = json.loads(data)
                assert len(decoded['outputs']) == len(plan[worker])
                assert [row['output_id'] for row in decoded['outputs']] == [row['output_id'] for row in plan[worker]]
                assert data.endswith(b'\n') and data == upload._d2_bytes(record)


def supervise_unit(upload, root, monkeypatch, *, unsettled=False, closure_error=False, left_pending=False,
                   release_interrupt=False):
    # Pure control-flow oracle. Native case witnesses separately prove actual OS settlement.
    clock = types.SimpleNamespace(now=0.0)
    states, last_observed, closed_ready = {}, {}, set()
    reads, scans = [], []
    class Gate:
        _value = 0
        @property
        def value(self):
            return self._value
        @value.setter
        def value(self, value):
            assert value == 1 and closed_ready == {'google', 'ftp'}
            self._value = value
            if release_interrupt:
                raise KeyboardInterrupt()
    go = Gate()
    monkeypatch.setattr(upload, 'time', types.SimpleNamespace(monotonic=lambda: clock.now,
        sleep=lambda duration: setattr(clock, 'now', clock.now + duration)))
    monkeypatch.setattr(upload, '_d2_runtime_profile', lambda case: dict(work_seconds=15.0,
        poll_seconds=0.1, cleanup_seconds=10.0, logging_seconds=10.0))
    log = types.ModuleType('run_logging')
    log.prepare_worker = lambda context, worker: {'worker_id': worker}
    log.emit_event = lambda *args, **kwargs: None
    log.flush_logging = lambda context: {'confirmed': True}
    log.check_logging_health = lambda context: {'healthy': True}
    log.record_worker_outcome = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, 'run_logging', log)
    spawn = types.SimpleNamespace(RawValue=lambda *args: go,
        Process=lambda **kwargs: types.SimpleNamespace(name=kwargs['name'], join=lambda timeout: None))
    monkeypatch.setattr(upload.multiprocessing, 'get_context', lambda method: spawn)
    for name in ('_d2_windows_profile', '_d2_checkpoint_reader_bindings', '_d2_recheck_source_plan',
                 '_d2_before_start', '_d2_after_start'):
        monkeypatch.setattr(upload, name, lambda *args, **kwargs: None)
    def start(process, state):
        worker = process.name.removeprefix('D2-')
        state.update(started=True, pid=41 if worker == 'google' else 42,
                     worker=worker, _start_returned=True)
        states[worker] = state
        (root / (worker + '.json')).write_bytes(b'CONTROLLED_READY_BYTES')
        return True
    monkeypatch.setattr(upload, '_d2_start_worker', start)
    def observe(row, state):
        worker = state.get('worker')
        settled = bool(state.get('terminated'))
        if go.value:
            settled = worker == 'google' and not unsettled or worker == 'ftp' and clock.now >= 0.2
        last_observed[worker] = settled
        observation = dict(started=state.get('started', False), pid=state.get('pid'),
            settled=settled, exitcode=0 if settled else None, alive=not settled, error_code=None)
        row.update({key: observation[key] for key in ('started', 'pid', 'settled', 'exitcode', 'alive')})
        if left_pending and worker == 'ftp' and go.value and settled:
            (root / 'ftp.pending').write_bytes(b'UNEXPECTED_REMAINDER')
        return observation
    monkeypatch.setattr(upload, '_d2_observe_row', observe)
    monkeypatch.setattr(upload, '_d2_native_terminate', lambda state: state.update(terminated=True))
    monkeypatch.setattr(upload, '_d2_close_launch_state', lambda state: True)
    def accept(row, *args, **kwargs):
        worker, normal = row['worker_id'], bool(go.value)
        if normal:
            assert last_observed[worker], 'Parent attempted terminal read before settlement'
            if release_interrupt:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
        elif closure_error and worker == 'ftp':
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        else:
            closed_ready.add(worker)
        reads.append((worker, go.value, last_observed[worker]))
        row.update(checkpoint_state='VALID', execution_state='RETURNED' if normal else 'READY',
            normal_return_observed=normal, diagnostics_state='COMPLETE' if normal else 'ACTIVE')
        if normal:
            for output in row['outputs']:
                output.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
        return dict(sequence=5 if normal else 1, execution_state=row['execution_state'],
            normal_return_observed=normal, diagnostics_state=row['diagnostics_state'],
            outputs=row['outputs'], error_code=None)
    monkeypatch.setattr(upload, '_d2_accept_checkpoint', accept)
    retained_stat, retained_exists, retained_iterdir = Path.lstat, Path.exists, Path.iterdir
    def leaf_allowed(path):
        if go.value and path.parent == root and path.name in {'google.json', 'ftp.json', 'google.pending', 'ftp.pending'}:
            assert last_observed[path.name.split('.')[0]], 'Unsettled checkpoint metadata attempted'
    def stat_leaf(path, *args, **kwargs):
        leaf_allowed(path)
        return retained_stat(path, *args, **kwargs)
    def exists_leaf(path):
        leaf_allowed(path)
        return retained_exists(path)
    def inventory(path):
        if path == root:
            if go.value:
                assert all(last_observed.get(worker) for worker in ('google', 'ftp'))
            scans.append(go.value)
        return retained_iterdir(path)
    monkeypatch.setattr(Path, 'lstat', stat_leaf)
    monkeypatch.setattr(Path, 'exists', exists_leaf)
    monkeypatch.setattr(Path, 'iterdir', inventory)
    plan = {'google': [dict(output_id='google-000001', group='google')],
            'ftp': [dict(output_id='ftp-000001', group='root')]}
    context = dict(run_id='r' * 32, producer_sha='b' * 40,
        config=dict(shutdown_deadline_s=10, critical_deadline_s=10))
    result = upload._d2_supervise(context, plan, root, 'r' * 32, 'a' * 32,
        None, 'D2_UNIT_SOURCE', 'D2_UNIT_TARGET')
    return result, go, reads, scans


def test_parent_reads_only_settled_checkpoint(upload, tmp_path, monkeypatch):
    result, go, reads, scans = supervise_unit(upload, tmp_path, monkeypatch)
    assert result['parent_outcome'] == 'RETURNED'
    assert go.value == 1
    assert {worker for worker, released, settled in reads if released and settled} == {'google', 'ftp'}
    assert not any(released and not settled for _, released, settled in reads)


def test_go_visible_interruption_marks_unproven_outputs_unknown(upload, tmp_path, monkeypatch):
    result, go, reads, scans = supervise_unit(upload, tmp_path, monkeypatch, release_interrupt=True)
    assert go.value == 1 and result['parent_outcome'] == 'INTERRUPTED'
    assert result['error_code'] == 'D2_INTERRUPTED'
    assert all(row['settled'] for row in result['workers'].values())
    assert all(output['transfer_state'] == 'UNKNOWN' and output['commit_state'] == 'UNKNOWN'
               for row in result['workers'].values() for output in row['outputs'])
    assert result['committed_outputs'] == [] and result['unknown_outputs']


def test_unsettled_cleanup_skips_peer_metadata_and_result_write(upload, tmp_path, monkeypatch):
    result, go, reads, scans = supervise_unit(upload, tmp_path, monkeypatch, unsettled=True)
    assert go.value == 1 and result['parent_outcome'] == 'FAILED'
    assert result['error_code'] is not None and result['unknown_outputs']
    assert result['workers']['google']['settled'] is False
    assert not any(worker == 'google' and released for worker, released, _ in reads)
    assert 1 not in scans
    assert not (tmp_path / 'result.json').exists()


@pytest.mark.parametrize('fault', ['none', 'closure_error'])
def test_ready_reader_closed_before_go(upload, tmp_path, monkeypatch, fault):
    result, go, reads, scans = supervise_unit(upload, tmp_path, monkeypatch, closure_error=fault == 'closure_error')
    if fault == 'none':
        assert result['parent_outcome'] == 'RETURNED' and go.value == 1
        assert {worker for worker, released, _ in reads if not released} == {'google', 'ftp'}
    else:
        assert result['parent_outcome'] == 'FAILED' and go.value == 0
        assert not any(released for _, released, _ in reads)
        assert not any(output['transfer_state'] in {'ACKNOWLEDGED', 'UNKNOWN'}
                       for row in result['workers'].values() for output in row['outputs'])


@pytest.mark.parametrize('fault', ['clean', 'left_pending'])
def test_full_inventory_requires_stable_phase(upload, tmp_path, monkeypatch, fault):
    result, go, reads, scans = supervise_unit(upload, tmp_path, monkeypatch, left_pending=fault == 'left_pending')
    assert go.value == 1 and 0 in scans and 1 in scans
    if fault == 'clean':
        assert result['parent_outcome'] == 'RETURNED'
        assert {path.name for path in tmp_path.iterdir()} == {'google.json', 'ftp.json', 'result.json'}
    else:
        assert result['parent_outcome'] == 'FAILED'
        assert (tmp_path / 'ftp.pending').read_bytes() == b'UNEXPECTED_REMAINDER'


@pytest.mark.parametrize('expires_at', ['release', 'settlement'])
def test_deadline_rechecked_at_release_and_settlement(upload, tmp_path, monkeypatch, expires_at):
    # Deterministic control-flow oracle supplements the real guarded 15-second hangs.
    # No process, logger, source client or alternative production timeout is created.
    clock = types.SimpleNamespace(now=0.0)
    go = types.SimpleNamespace(value=0)
    flushes, observations = [], []
    monkeypatch.setattr(upload, 'time', types.SimpleNamespace(
        monotonic=lambda: clock.now,
        sleep=lambda duration: setattr(clock, 'now', clock.now + duration)))
    monkeypatch.setattr(upload, '_d2_runtime_profile', lambda case: dict(
        work_seconds=15.0, poll_seconds=0.1, cleanup_seconds=10.0, logging_seconds=10.0))
    fake_log = types.ModuleType('run_logging')
    fake_log.prepare_worker = lambda context, worker: {'worker_id': worker}
    fake_log.emit_event = lambda *args, **kwargs: None
    fake_log.check_logging_health = lambda context: {'healthy': True}
    fake_log.record_worker_outcome = lambda *args, **kwargs: None
    def flush(context):
        flushes.append(go.value)
        if expires_at == 'release' and len(flushes) == 2:
            clock.now = 15.1
        return {'confirmed': True}
    fake_log.flush_logging = flush
    monkeypatch.setitem(sys.modules, 'run_logging', fake_log)
    spawn = types.SimpleNamespace(RawValue=lambda *args: go,
        Process=lambda **kwargs: types.SimpleNamespace(name=kwargs['name'], join=lambda timeout: None))
    monkeypatch.setattr(upload.multiprocessing, 'get_context', lambda method: spawn)
    for name in ('_d2_windows_profile', '_d2_checkpoint_reader_bindings', '_d2_recheck_source_plan',
                 '_d2_before_start', '_d2_after_start'):
        monkeypatch.setattr(upload, name, lambda *args, **kwargs: None)
    def start(process, state):
        worker = process.name.removeprefix('D2-')
        state.update(started=True, pid=41 if worker == 'google' else 42, worker=worker)
        (tmp_path / (worker + '.json')).write_bytes(b'unit-observation')
        return True
    monkeypatch.setattr(upload, '_d2_start_worker', start)
    def observe(row, state):
        settled = bool(go.value or state.get('terminated'))
        if expires_at == 'settlement' and go.value and state.get('worker') == 'ftp':
            clock.now = 15.1
        observation = dict(started=state.get('started', False), pid=state.get('pid'),
            settled=settled, exitcode=0 if settled else None, alive=not settled, error_code=None)
        row.update({key: observation[key] for key in ('started','pid','settled','exitcode','alive')})
        observations.append((state.get('worker'), go.value, settled))
        return observation
    monkeypatch.setattr(upload, '_d2_observe_row', observe)
    monkeypatch.setattr(upload, '_d2_native_terminate', lambda state: state.update(terminated=True))
    monkeypatch.setattr(upload, '_d2_close_launch_state', lambda state: True)
    def accept(row, *args, **kwargs):
        normal = bool(go.value)
        row.update(checkpoint_state='VALID', execution_state='RETURNED' if normal else 'READY',
            normal_return_observed=normal, diagnostics_state='COMPLETE' if normal else 'ACTIVE')
        if normal:
            for output in row['outputs']:
                output.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
        return dict(sequence=5 if normal else 1, execution_state=row['execution_state'],
            normal_return_observed=normal, diagnostics_state=row['diagnostics_state'],
            outputs=row['outputs'], error_code=None)
    monkeypatch.setattr(upload, '_d2_accept_checkpoint', accept)
    plan = {worker: [dict(output_id=worker + '-000001', group='google' if worker == 'google' else 'root')]
            for worker in ('google','ftp')}
    context = dict(run_id='r' * 32, producer_sha='b' * 40,
        config=dict(shutdown_deadline_s=10, critical_deadline_s=10))
    result = upload._d2_supervise(context, plan, tmp_path, 'r' * 32, 'a' * 32,
        None, 'D2_UNIT_SOURCE', 'D2_UNIT_TARGET')
    assert result['parent_outcome'] == 'FAILED'
    assert result['error_code'] == 'D2_TIMEOUT'
    assert result['diagnostic_success_allowed'] is False
    assert all(row['settled'] for row in result['workers'].values())
    if expires_at == 'release':
        assert go.value == 0
        assert not any(released for _, released, _ in observations)
    else:
        assert go.value == 1
        assert {worker for worker, released, settled in observations if released and settled} == {'google','ftp'}
