"""Real admitted descendants: zero exit alone never proves guarded success."""

from pathlib import Path
import sys
import time

import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[2]


def _control(case, tmp_path, timeout=30):
    command = [sys.executable, '-I', '-B', '-X', 'utf8',
               str(ROOT / 'tools/offline_guard.py'), 'control', case]
    return guard.run_child(command, role='f3-control', roots=[tmp_path], timeout=timeout)


def test_normal_real_child_reports_current_run_lane_role_and_sha(tmp_path):
    context = guard.ensure_context(ROOT)
    result = _control('normal', tmp_path)
    assert result.returncode == 0 and result.actual_exit == 0
    assert result.validated and result.report_errors == []
    assert result.report['run_id'] == context.run_id
    assert result.report['lane'] == 'f3'
    assert result.report['role'] == 'f3-control'
    assert result.report['tested_sha'] == context.tested_sha
    assert result.report['source_digest'] == context.source_digest
    assert result.report['parent_id'] == context.process_id
    assert result.report['counters'] == {}
    assert result.report['children'] == []


def test_explicit_real_child_requires_actual_exit_evidence(tmp_path):
    result = _control('normal', tmp_path)
    assert result.validated and result.actual_exit == 0
    ticket = dict(result.ticket)
    replica = tmp_path / 'missing-exit-evidence'
    (replica / 'processes').mkdir(parents=True)
    (replica / 'tickets').mkdir()
    original = Path(ticket['report_dir'])
    identifier = ticket['ticket_id']
    for suffix in ('start', 'final'):
        name = identifier + '.' + suffix + '.json'
        (replica / 'processes' / name).write_bytes((original / 'processes' / name).read_bytes())
    ticket['report_dir'] = str(replica)
    errors, final = guard._validate_ticket(ticket)
    assert final is not None and final['tested_sha'] == result.report['tested_sha']
    assert errors == ['MISSING_CHILD_EXIT']
    name = identifier + '.exit.json'
    (replica / 'tickets' / name).write_bytes((original / 'tickets' / name).read_bytes())
    assert guard._validate_ticket(ticket)[0] == []


@pytest.mark.parametrize('case, kind', [
    ('network', 'network'), ('dns', 'network'), ('write', 'writers'),
    ('sleep', 'sleep'), ('caught-network', 'network'), ('caught-sleep', 'sleep'),
    ('secrets', 'secrets'), ('dataset-read', 'dataset_reads'), ('extra-child', 'children'),
])
def test_real_descendant_fault_even_if_caught_rejects_zero_exit(case, kind, tmp_path):
    result = _control(case, tmp_path)
    assert result.actual_exit == 0
    assert result.returncode != 0 and not result.validated
    assert result.report_errors
    assert result.report['counters'] == {kind: 1}
    assert result.report['violations'] == 1


@pytest.mark.parametrize('case', ['missing-report', 'corrupt-report', 'foreign-report', 'stale-report',
                                'duplicate-report'])
def test_zero_exit_without_current_correct_report_is_rejected(case, tmp_path):
    result = _control(case, tmp_path)
    assert result.actual_exit == 0
    assert result.returncode != 0 and not result.validated
    assert result.report_errors


def test_timeout_is_bounded_and_only_the_owned_child_is_cleaned_up(tmp_path):
    started = time.monotonic()
    result = _control('timeout', tmp_path, timeout=2)
    assert time.monotonic() - started < 8
    assert result.actual_exit != 0
    assert result.returncode != 0 and not result.validated
    assert result.report_errors
