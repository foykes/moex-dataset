"""Real admitted descendants: zero exit alone never proves guarded success."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[2]


GIT_REVIEW_CASES = ('git-diff-output-equals', 'git-diff-output-separated',
                    'git-symbolic-ref-mutates')
GIT_REVIEW_CANARY = b'F3_CODE01_SYNTHETIC_EXTERNAL_CANARY\x00\xff\n'


def _review_control(case, root, ticket):
    """Fixed synthetic Git payload, using the real native F2 admission path."""
    if case not in GIT_REVIEW_CASES or ticket['role'] != 'f3-control':
        raise ValueError('OFFLINE_GIT_REVIEW_CONTROL')
    root = guard.checked_path(root)
    issued = guard.checked_path(ticket['roots'][0], root / '.f3/tmp')
    repository = guard.checked_path(issued / '.f2/git-admission', issued)
    external = guard.checked_path(issued.parent / ('git-read-canary-' + ticket['ticket_id']),
                                  root / '.f3/tmp')
    repository.mkdir(parents=True, exist_ok=False)
    external.mkdir(exist_ok=False)
    canary = external / 'external.canary'
    canary.write_bytes(GIT_REVIEW_CANARY)
    (repository / '.f2/empty-hooks').mkdir(parents=True)
    sample = repository / 'sample.txt'
    sample.write_bytes(b'original sample\n')
    git = shutil.which('git')
    if git is None:
        raise ValueError('OFFLINE_GIT_MISSING')
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith('GIT_')}
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
                       GIT_TERMINAL_PROMPT='0')

    def run(*arguments, check=True):
        return subprocess.run([git, *arguments], cwd=repository, env=environment,
                              capture_output=True, check=check, timeout=10)

    # Trusted fixture preparation occurs before workload guard installation.
    # All commands and destinations are fixed here; no project hook is used.
    run('init', '--initial-branch=codex/fixture')
    for name, value in (('user.name', 'F2 disposable fixture'),
                        ('user.email', 'fixture@example.invalid'),
                        ('commit.gpgsign', 'false'), ('core.autocrlf', 'false')):
        run('config', '--local', name, value)
    run('add', '--', '.')
    run('-c', 'core.hooksPath=' + str(repository / '.f2/empty-hooks'),
        'commit', '-m', 'N0 P0 baseline')
    sample.write_bytes(b'changed sample\n')
    head = repository / '.git/HEAD'
    reference = repository / '.git/refs/heads/codex/fixture'
    before = (canary.read_bytes(), head.read_bytes(), reference.read_bytes())

    # This audit observer counts actual Popen events, after Python admission.
    # It changes no guard, launcher, native command, or tested leaf operation.
    observed = {'payload': False, 'native_launches': 0}

    def observe(event, arguments):
        if event == 'subprocess.Popen' and observed['payload']:
            observed['native_launches'] += 1

    sys.addaudithook(observe)
    context = guard.ensure_context(root, install=False)
    specification = importlib.util.spec_from_file_location(
        '_f3_review_native_f2', root / 'tests/f2/_probe.py')
    probe = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(probe)
    native_counts = probe.install_guards(issued, 'harness')
    positive_head = run('rev-parse', 'HEAD').stdout.decode('ascii').strip()
    positive_ref = run('symbolic-ref', 'HEAD').stdout.decode('ascii').strip()
    if case == 'git-diff-output-equals':
        arguments = ('diff', '--output=' + str(canary), '--', 'sample.txt')
    elif case == 'git-diff-output-separated':
        arguments = ('diff', '--output', str(canary), '--', 'sample.txt')
    else:
        arguments = ('symbolic-ref', 'HEAD', 'refs/heads/f3-review-mutated')
    caught = None
    refusal = None
    actual_exit = None
    observed['payload'] = True
    try:
        actual_exit = run(*arguments, check=False).returncode
    except RuntimeError as error:
        caught = type(error).__name__
        refusal = str(error) if str(error) in {
            'OFFLINE_CHILD_NOT_ADMITTED', 'F2_OFFLINE_CONVERSION'} else 'UNEXPECTED_REFUSAL'
    finally:
        observed['payload'] = False
    after = (canary.read_bytes(), head.read_bytes(), reference.read_bytes())
    result = dict(case=case, actual_exit=actual_exit, caught=caught, refusal=refusal,
                  payload_native_launches=observed['native_launches'],
                  before_size=len(before[0]), after_size=len(after[0]),
                  before_sha256=hashlib.sha256(before[0]).hexdigest(),
                  after_sha256=hashlib.sha256(after[0]).hexdigest(),
                  canary_preserved=before[0] == after[0],
                  head_preserved=before[1] == after[1], ref_preserved=before[2] == after[2],
                  positive_head=positive_head, positive_ref=positive_ref,
                  counters=dict(context.counts), native_counters=dict(native_counts))
    print('F3_GIT_CONTROL:' + json.dumps(result, sort_keys=True))
    guard.finish(0)
    return 0


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


@pytest.mark.parametrize('case', GIT_REVIEW_CASES)
def test_native_f2_git_mutating_read_forms_rejected_before_launch_even_if_caught(case, tmp_path):
    result = _control(case, tmp_path)
    assert result.actual_exit == 0
    assert result.returncode != 0 and not result.validated
    assert result.report_errors == ['UNEXPECTED_COUNTERS']
    assert result.report['counters'] == {'conversion': 1}
    assert result.report['violations'] == 1
    lines = result.stdout.decode('utf-8').splitlines()
    controls = [line.removeprefix('F3_GIT_CONTROL:') for line in lines
                if line.startswith('F3_GIT_CONTROL:')]
    assert len(controls) == 1 and len(controls[0]) < 2048
    control = json.loads(controls[0])
    assert control['case'] == case
    assert control['actual_exit'] is None and control['caught'] == 'RuntimeError'
    assert control['refusal'] == 'OFFLINE_CHILD_NOT_ADMITTED'
    assert control['payload_native_launches'] == 0
    assert control['counters'] == {'conversion': 1}
    assert control['native_counters'] == {'network': 0, 'conversion': 1, 'writers': 0,
                                         'secrets': 0, 'dataset_reads': 0}
    expected_digest = hashlib.sha256(GIT_REVIEW_CANARY).hexdigest()
    assert control['before_size'] == control['after_size'] == len(GIT_REVIEW_CANARY)
    assert control['before_sha256'] == control['after_sha256'] == expected_digest
    assert control['canary_preserved'] and control['head_preserved'] and control['ref_preserved']
    assert control['positive_ref'] == 'refs/heads/codex/fixture'
    assert len(control['positive_head']) == 40
    # Independent parent reads verify the child's preservation claims.
    canary = tmp_path.parent / ('git-read-canary-' + result.ticket['ticket_id']) / 'external.canary'
    assert canary.read_bytes() == GIT_REVIEW_CANARY
    assert canary.stat().st_size == len(GIT_REVIEW_CANARY)
    assert hashlib.sha256(canary.read_bytes()).hexdigest() == expected_digest
    repository = tmp_path / '.f2/git-admission'
    assert (repository / '.git/HEAD').read_bytes() == b'ref: refs/heads/codex/fixture\n'
    assert (repository / '.git/refs/heads/codex/fixture').read_text('ascii').strip() == control['positive_head']
    identifier = result.report['ticket_id']
    journals = list(Path(result.ticket['report_dir']).joinpath('processes').glob(
        identifier + '.violation.*.json'))
    assert len(journals) == 1
    assert json.loads(journals[0].read_text('utf-8'))['kind'] == 'conversion'
