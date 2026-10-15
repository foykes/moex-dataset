"""Independent LNX-03 authority, ticket lifetime and closed command refusals."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
import socket
import sys
from types import SimpleNamespace

import pytest


SOURCE_PATHS = (
    '.github/lnx02/verify_env.py', '.github/workflows/d2-linux-offline.yml',
    'tools/offline_linux.py', 'tools/offline_guard_linux.py',
    'tests/linux_bootstrap/conftest.py', 'tests/linux_bootstrap/native_worker.py',
    'tests/linux_bootstrap/test_environment_contract.py',
    'tests/linux_bootstrap/test_admission.py', 'tests/linux_bootstrap/test_spawn_contract.py',
    'docs/d2_linux_ci.md', 'docs/testing.md', 'docs/coordination.md',
    'run_logging.py', 'requirements/linux-cp314-runtime.lock',
    'requirements/linux-cp314-test.lock', 'requirements/linux-cp314-dev.lock',
)


def install_grants(values, grant, setup=True):
    values['MDS_LNX03_ADMISSION_GRANT'] = json.dumps(grant, sort_keys=True)
    if setup:
        values['MDS_LNX03_SETUP_GRANT'] = json.dumps(
            dict(grant, capability='network-setup'), sort_keys=True)


def test_exact_authority_derives_actual_run_id(helper, admission_environment):
    values, sha, base, grant = admission_environment
    actual = helper.lnx03_validate_grant(values, sha, base)
    assert actual == dict(grant, run_id='731009')
    assert 'run_id' not in json.loads(values['MDS_LNX03_ADMISSION_GRANT'])


@pytest.mark.parametrize('key,value', [
    ('GITHUB_REPOSITORY', 'someone-else/moex-dataset'),
    ('GITHUB_SHA', 'd' * 40), ('GITHUB_REF', 'refs/heads/codex/lnx-02'),
    ('GITHUB_ACTOR', 'someone-else'), ('GITHUB_TRIGGERING_ACTOR', 'someone-else'),
    ('GITHUB_EVENT_NAME', 'pull_request_target'),
    ('GITHUB_WORKFLOW_REF', 'foykes/moex-dataset/.github/workflows/other.yml@refs/heads/main'),
    ('GITHUB_WORKFLOW_SHA', 'd' * 40), ('GITHUB_JOB', 'other-job'),
    ('GITHUB_RUN_NUMBER', '32'), ('GITHUB_RUN_NUMBER', '031'),
    ('GITHUB_RUN_ATTEMPT', '2'), ('GITHUB_RUN_ATTEMPT', '01'),
    ('GITHUB_RUN_ID', 'other-run'), ('RUNNER_ENVIRONMENT', 'self-hosted'),
    ('TASK_RUNNER_ENVIRONMENT', 'self-hosted'), ('ImageOS', 'ubuntu22'),
    ('TASK_DELETED', 'true'), ('TASK_EVENT', 'workflow_dispatch'),
    ('TASK_EVENT_SHA', 'd' * 40), ('TASK_EVENT_REF', 'refs/heads/main'),
    ('TASK_REPOSITORY', 'someone-else/moex-dataset'), ('TASK_ACTOR', 'someone-else'),
    ('TASK_TRIGGERING_ACTOR', 'someone-else'), ('TASK_JOB', 'other-job'),
    ('TASK_WORKFLOW', '.github/workflows/linux-environment.yml'),
    ('TASK_RUN_NUMBER', '32'), ('TASK_RUN_ATTEMPT', '2'), ('TASK_RUN_ID', '731010'),
])
def test_identity_mismatch_refused(helper, admission_environment, key, value):
    values, sha, base, _ = admission_environment
    values[key] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


@pytest.mark.parametrize('key,value', [
    ('approved', False), ('approved', 'true'), ('consumer', 'another-consumer'),
    ('repository', 'someone-else/moex-dataset'), ('sha', 'd' * 40), ('base', 'd' * 40),
    ('event', 'workflow_dispatch'), ('ref', 'refs/heads/codex/lnx-02'),
    ('actor', 'someone-else'), ('triggering_actor', 'someone-else'),
    ('workflow', '.github/workflows/linux-environment.yml'), ('job', 'other-job'),
    ('run_number', 32), ('run_number', True), ('run_number', '31'),
    ('attempt', 2), ('attempt', True), ('ticket_id', 'c' * 63), ('ticket_id', 'C' * 64),
])
def test_wrong_or_ill_typed_grant_refused(helper, admission_environment, key, value):
    values, sha, base, grant = admission_environment
    grant[key] = value
    install_grants(values, grant)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


@pytest.mark.parametrize('which', ['MDS_LNX03_ADMISSION_GRANT', 'MDS_LNX03_SETUP_GRANT'])
@pytest.mark.parametrize('bad', ['not-json', '[]', 'null', '{}', 'true'])
def test_malformed_grant_refused(helper, admission_environment, which, bad):
    values, sha, base, _ = admission_environment
    values[which] = bad
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


def test_unknown_grant_field_refused(helper, admission_environment):
    values, sha, base, grant = admission_environment
    grant['command'] = 'arbitrary-python-module'
    install_grants(values, grant)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


def test_missing_setup_refused_before_network(helper, admission_environment, monkeypatch):
    values, sha, base, _ = admission_environment
    values.pop('MDS_LNX03_SETUP_GRANT')
    calls = []

    def unexpected_network(*args, **kwargs):
        calls.append('DNS')
        raise AssertionError('network must remain unreachable')

    monkeypatch.setattr(socket, 'getaddrinfo', unexpected_network)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)
    assert calls == []
    # A pure admission check is permitted; it grants no SETUP capability.
    admitted = helper.lnx03_validate_grant(values, sha, base, require_setup=False)
    assert admitted['consumer'] == 'linux-bootstrap'


def test_setup_grant_cannot_authorize_another_ticket(helper, admission_environment):
    values, sha, base, grant = admission_environment
    values['MDS_LNX03_SETUP_GRANT'] = json.dumps(
        dict(grant, ticket_id='d' * 64, capability='network-setup'))
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


def test_legacy_grants_do_not_authorize_linux_consumer(helper, admission_environment, legacy_environment):
    values, sha, base, _ = admission_environment
    values.pop('MDS_LNX03_ADMISSION_GRANT')
    values.pop('MDS_LNX03_SETUP_GRANT')
    legacy, _, _ = legacy_environment
    values.update({key: value for key, value in legacy.items() if key.startswith('MDS_LNX02_')})
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


def test_linux_grants_do_not_authorize_legacy_consumer(helper, admission_environment):
    values, sha, base, _ = admission_environment
    for mode in ('diagnostic', 'generate', 'verify'):
        with pytest.raises(helper.PolicyError):
            helper.validate_grant(values, mode, sha, base)


def test_single_use_claim_persists_on_replay(helper, admission_environment, tmp_path):
    values, sha, base, grant = admission_environment
    admitted = helper.lnx03_validate_grant(values, sha, base)
    root = tmp_path / 'claims'
    root.mkdir(mode=0o700)
    actual = helper.lnx03_claim(root, admitted)
    leaves = list(root.iterdir())
    assert [leaf.name for leaf in leaves] == [grant['ticket_id'] + '.json']
    original = leaves[0].read_bytes()
    assert actual == hashlib.sha256(original).hexdigest()
    with pytest.raises(helper.PolicyError):
        helper.lnx03_claim(root, admitted)
    assert leaves[0].read_bytes() == original


def test_foreign_claim_cannot_be_replaced(helper, admission_environment, tmp_path):
    values, sha, base, grant = admission_environment
    admitted = helper.lnx03_validate_grant(values, sha, base)
    root = tmp_path / 'claims'
    root.mkdir(mode=0o700)
    leaf = root / (grant['ticket_id'] + '.json')
    original = b'{"foreign":true}\n'
    leaf.write_bytes(original)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_claim(root, admitted)
    assert leaf.read_bytes() == original


def test_claim_exclusive_create_under_concurrent_attempts(helper, admission_environment, tmp_path):
    values, sha, base, _ = admission_environment
    admitted = helper.lnx03_validate_grant(values, sha, base)
    root = tmp_path / 'claims'
    root.mkdir(mode=0o700)

    def attempt(_):
        try:
            helper.lnx03_claim(root, admitted)
            return 'CLAIMED'
        except helper.PolicyError:
            return 'REFUSED'

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(16)))
    assert outcomes.count('CLAIMED') == 1
    assert outcomes.count('REFUSED') == 15
    assert len(list(root.iterdir())) == 1


def test_duplicate_json_grant_keys_are_refused(helper, admission_environment):
    values, sha, base, _ = admission_environment
    original = values['MDS_LNX03_ADMISSION_GRANT']
    values['MDS_LNX03_ADMISSION_GRANT'] = original[:-1] + ', "approved": true}'
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_grant(values, sha, base)


def test_supervised_namespace_requires_separate_exact_capability(helper, admission_environment):
    values, _, _, grant = admission_environment
    with pytest.raises(helper.PolicyError):
        helper.lnx03_capability(values, grant, 'MDS_LNX03_SUPERVISED_NETNS_GRANT', 'supervised-netns')
    # Neither the existing LNX-02 opt-in nor network SETUP authority permits sudo.
    values['MDS_LNX02_ALLOW_SUPERVISED_NETNS'] = 'true'
    with pytest.raises(helper.PolicyError):
        helper.lnx03_capability(values, grant, 'MDS_LNX03_SUPERVISED_NETNS_GRANT', 'supervised-netns')
    values['MDS_LNX03_SUPERVISED_NETNS_GRANT'] = json.dumps(dict(grant, capability='network-setup'))
    with pytest.raises(helper.PolicyError):
        helper.lnx03_capability(values, grant, 'MDS_LNX03_SUPERVISED_NETNS_GRANT', 'supervised-netns')
    values['MDS_LNX03_SUPERVISED_NETNS_GRANT'] = json.dumps(dict(grant, capability='supervised-netns'))
    helper.lnx03_capability(values, grant, 'MDS_LNX03_SUPERVISED_NETNS_GRANT', 'supervised-netns')


@pytest.mark.parametrize('extra', [
    ['--mode', 'generate'], ['--module', 'main'], ['--script', 'main.py'],
    ['--command', 'python'], ['--callback', 'main.main'], ['--target', 'tests.py'],
    ['--', 'tests.py'], ['--timeout', '1'],
])
def test_consumer_unknown_arguments_refused_without_execution(helper, monkeypatch, capsys, extra):
    monkeypatch.setattr(sys, 'argv', ['verify_env.py', 'linux-bootstrap',
                                    '--repo-root', 'unused', '--output', 'unused',
                                    '--expected-sha', 'a' * 40, '--expected-base', 'b' * 40] + extra)
    assert helper.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert result == {'status': 'FAIL', 'failure_code': 'invalid-cli'}


def workflow_authority_gate(repo):
    """Execute the actual pre-checkout gate, independently of helper routing."""
    lines = (repo / '.github/workflows/d2-linux-offline.yml').read_text(encoding='utf-8').splitlines()
    start = lines.index("          /usr/bin/python3 -I -S -B -X utf8 - <<'PY'") + 1
    end = lines.index('          PY', start)
    assert all(line.startswith('          ') or not line for line in lines[start:end])
    body = '\n'.join(line[10:] if line else '' for line in lines[start:end])
    return compile(body, 'lnx03-fixed-precheckout-authority', 'exec')


def test_workflow_gate_accepts_exact_identity_only_after_validation(
        repo, admission_environment, tmp_path, monkeypatch, capsys):
    values, sha, base, _ = admission_environment
    output = tmp_path / 'github-env'
    output.write_text('EXISTING=preserved\n', encoding='utf-8')
    values['GITHUB_ENV'] = str(output)
    monkeypatch.setattr(os, 'environ', values)
    exec(workflow_authority_gate(repo), {'__name__': 'lnx03_precheckout_oracle'})
    assert capsys.readouterr().out == 'LNX03_AUTHORITY_ACCEPTED\n'
    assert output.read_text(encoding='utf-8') == (
        'EXISTING=preserved\nTASK_APPROVED_SHA=' + sha + '\nTASK_APPROVED_BASE=' + base
        + '\nTASK_RUNNER_ENVIRONMENT=github-hosted\n')


@pytest.mark.parametrize('key,value', [
    ('GITHUB_SHA', 'd' * 40), ('GITHUB_REF', 'refs/heads/codex/lnx-02'),
    ('GITHUB_ACTOR', 'someone-else'), ('GITHUB_TRIGGERING_ACTOR', 'someone-else'),
    ('GITHUB_RUN_NUMBER', '32'), ('GITHUB_RUN_ATTEMPT', '2'),
    ('GITHUB_RUN_ID', '731010'), ('GITHUB_JOB', 'other-job'),
    ('GITHUB_WORKFLOW_SHA', 'd' * 40), ('ImageOS', 'ubuntu22'),
    ('RUNNER_ENVIRONMENT', 'self-hosted'), ('MDS_LNX03_SETUP_GRANT', ''),
    ('MDS_LNX03_ADMISSION_GRANT', '{"private_key":"secret-canary"}'),
])
def test_workflow_refusal_has_no_environment_append(
        repo, admission_environment, tmp_path, monkeypatch, capsys, key, value):
    values, _, _, _ = admission_environment
    output = tmp_path / 'github-env'
    original = b'EXISTING=preserved\n'
    output.write_bytes(original)
    values.update({key: value, 'GITHUB_ENV': str(output)})
    monkeypatch.setattr(os, 'environ', values)
    with pytest.raises(SystemExit) as exit_info:
        exec(workflow_authority_gate(repo), {'__name__': 'lnx03_precheckout_oracle'})
    assert exit_info.value.code == 1
    assert capsys.readouterr().out == 'LNX03_AUTHORITY_REFUSED\n'
    assert output.read_bytes() == original


def failed_result(helper, admission_environment):
    values, sha, base, _ = admission_environment
    return {
        'schema_version': 'lnx03-linux-bootstrap-v1',
        'consumer': 'linux-bootstrap', 'scope': 'APPLICATION_ADMISSION',
        'status': 'INCOMPLETE', 'failure_code': 'lnx03-native-not-executed',
        'source': {'sha': sha, 'base': base, 'helper_sha256': 'd' * 64,
                   'files_sha256': dict.fromkeys(SOURCE_PATHS, 'd' * 64)},
        'authority': helper.lnx03_validate_grant(values, sha, base),
        'environment': {'identity': None, 'fingerprint_before': None,
                        'fingerprint_after': None, 'inventory': None, 'pip_check_exit': None},
        'setup': {'authorized': True, 'completed': False, 'wheel_count': 0, 'stages': []},
        'isolation': {'mode': None, 'primitive': None, 'preflight': None},
        'cases': [], 'cleanup': {'state': 'NOT_CREATED', 'owned_deleted': False},
    }


def test_incomplete_result_does_not_fabricate_native_observations(helper, admission_environment):
    report = failed_result(helper, admission_environment)
    assert helper.lnx03_validate_result(report) == report
    assert report['environment']['pip_check_exit'] is None
    assert report['cases'] == []


def test_incomplete_result_cannot_be_promoted_to_pass(helper, admission_environment):
    report = failed_result(helper, admission_environment)
    report.update(status='PASS', failure_code=None)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


def partial_bootstrap_cases():
    """Schema vectors, with absent OS observations kept explicitly unknown."""
    sources = ('context.py', 'connection.py', 'reduction.py', 'queues.py', 'synchronize.py',
               'sharedctypes.py', 'heap.py', 'resource_tracker.py', 'spawn.py',
               'popen_spawn_posix.py', 'util.py')
    return [{
        'schema': 'lnx03-native-case-v1', 'case': name, 'status': 'FAIL',
        'failure_code': 'LNX03_NATIVE_ORACLE_FAILED', 'application_exit': 1,
        'deadline_seconds': 15, 'deadline_detected': False, 'cleanup_requested': False,
        'all_workers_settled': False, 'two_workers_registered': False,
        'worker_records': [], 'console_canary_absent': False,
        'source_attestation': dict.fromkeys(sources, 'd' * 64),
        'logging': {'delivery_outcome': 'INCOMPLETE', 'storage_sealed': False,
                    'exit_status': 1, 'worker_final_count': 0},
        'auxiliary': {'owned': False, 'pid': None, 'pidfd_bound': False,
                      'settled': False, 'reaped': False, 'exitcode': None},
    } for name in ('bootstrap_replay_failure', 'bootstrap_import_failure',
                   'bootstrap_unpickle_failure')]


def incomplete_application_report(helper, admission_environment):
    report = failed_result(helper, admission_environment)
    report['cases'] = partial_bootstrap_cases()
    report['setup']['stages'] = [
        {'kind': 'preflight', 'status': 'FAIL', 'observations': {},
         'guard': {'registered_before_third_party': True, 'pid': 123,
                   'expected_denials': {}, 'unexpected_denials': {}}},
        {'kind': 'bootstrap', 'status': 'FAIL',
         'observations': {'pip_operation': {'exit_code': 1}}},
        {'kind': 'application', 'status': 'FAIL', 'observations': {
            'process': {'started': True, 'pid': 124, 'pidfd_bound': True,
                        'exit_code': 1, 'timed_out': False, 'termination_confirmed': True},
            'tests': {'schema_version': 'lnx03-runner-result-v1', 'status': 'FAIL',
                      'source_sha': report['source']['sha'], 'collected': 3,
                      'passed': 0, 'failed': 3, 'deselected': 0, 'exit_code': 1,
                      'native_cases': copy.deepcopy(report['cases']),
                      'failure_code': 'lnx03-native-oracles-failed'},
        }},
    ]
    return report


def test_incomplete_bundle_preserves_three_bootstrap_failures(helper, admission_environment):
    report = incomplete_application_report(helper, admission_environment)
    assert helper.lnx03_validate_result(report) == report
    assert len(report['cases']) == 3
    assert all(case['status'] == 'FAIL' and not case['worker_records']
               and case['auxiliary']['exitcode'] is None for case in report['cases'])
    assert report['setup']['stages'][-1]['observations']['tests']['native_cases'] == report['cases']


@pytest.mark.parametrize('mutation', [
    'raw-stage', 'identity', 'primitive', 'preflight', 'stage-detail',
    'stage-identity', 'stage-guard', 'guard-event', 'guard-count-type',
    'observations', 'pip-operation', 'pip-operation-boolean-exit',
    'application-process', 'application-process-boolean-exit', 'application-tests',
    'application-tests-count', 'application-tests-source', 'application-tests-zero-exit',
    'nested-case', 'nested-worker', 'environment-inventory', 'stage-inventory',
    'negative-probes', 'stdout-summary',
])
def test_incomplete_bundle_cannot_export_unvalidated_nested_evidence(
        helper, admission_environment, mutation):
    report = incomplete_application_report(helper, admission_environment)
    stages = report['setup']['stages']
    marker = 'RAW_EXCEPTION_CANARY'
    if mutation == 'raw-stage':
        report['setup']['stages'] = [{'detail': marker}]
    elif mutation == 'identity':
        report['environment']['identity'] = {'detail': marker}
    elif mutation in ('primitive', 'preflight'):
        report['isolation'][mutation] = {'detail': marker}
    elif mutation == 'stage-detail':
        stages[0]['detail'] = marker
    elif mutation == 'stage-identity':
        stages[0]['identity'] = {'detail': marker}
    elif mutation == 'stage-guard':
        stages[0]['guard']['detail'] = marker
    elif mutation == 'guard-event':
        stages[0]['guard']['expected_denials']['arbitrary.callback'] = 1
    elif mutation == 'guard-count-type':
        stages[0]['guard']['expected_denials']['socket.getaddrinfo'] = True
    elif mutation == 'observations':
        stages[0]['observations']['detail'] = marker
    elif mutation == 'pip-operation':
        stages[1]['observations']['pip_operation']['detail'] = marker
    elif mutation == 'pip-operation-boolean-exit':
        stages[1]['observations']['pip_operation']['exit_code'] = True
    elif mutation == 'application-process':
        stages[2]['observations']['process']['detail'] = marker
    elif mutation == 'application-process-boolean-exit':
        stages[2]['observations']['process']['exit_code'] = True
    elif mutation == 'application-tests':
        stages[2]['observations']['tests']['detail'] = marker
    elif mutation == 'application-tests-count':
        stages[2]['observations']['tests']['failed'] = True
    elif mutation == 'application-tests-source':
        stages[2]['observations']['tests']['source_sha'] = 'e' * 40
    elif mutation == 'application-tests-zero-exit':
        stages[2]['observations']['tests']['exit_code'] = 0
    elif mutation == 'nested-case':
        stages[2]['observations']['tests']['native_cases'][0]['detail'] = marker
    elif mutation == 'nested-worker':
        stages[2]['observations']['tests']['native_cases'][0]['worker_records'] = [{'detail': marker}]
    elif mutation == 'environment-inventory':
        report['environment']['inventory'] = {'arbitrary-package': '1.0'}
    elif mutation == 'stage-inventory':
        stages[1] = {'kind': 'check', 'status': 'FAIL',
                     'observations': {'inventory': {'arbitrary-package': '1.0'}}}
    elif mutation == 'negative-probes':
        stages[0]['negative_probes'] = {'detail': marker}
    elif mutation == 'stdout-summary':
        stages[0]['stdout'] = {'detail': marker}
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('runner_raises', [False, True], ids=['reported-failures', 'runner-fails-before-result'])
def test_failed_runner_cases_survive_actual_application_worker_boundary(
        helper, admission_environment, repo, tmp_path, monkeypatch, runner_raises):
    """Exercise the real worker catch/write path with OS operations replaced.

    The receipt is a schema-only vector, not hosted runtime evidence. The witness
    proves execution reached lnx03_runner and retained its observed failures.
    """
    values, sha, base, _ = admission_environment
    authority = helper.lnx03_validate_grant(values, sha, base)
    root = tmp_path / 'worker-root'
    (root / 'jobs').mkdir(parents=True)
    source = dict.fromkeys(SOURCE_PATHS, 'd' * 64)
    outcome = incomplete_application_report(helper, admission_environment)['setup']['stages'][-1]['observations']
    witness = []

    def observed_runner(actual_root, job):
        assert actual_root == root and job['authority'] == authority
        witness.append('actual-runner-boundary')
        if runner_raises:
            raise helper.PolicyError('lnx03-runner-exit-or-result-incomplete')
        return copy.deepcopy(outcome)

    monkeypatch.setattr(helper, 'flags', lambda: None)
    monkeypatch.setattr(helper, 'lnx03_owned', lambda path, nonce: root)
    monkeypatch.setattr(helper, 'lnx03_source_manifest', lambda actual_repo: source)
    monkeypatch.setattr(helper, 'identity', lambda: {'unit_scope': 'SCHEMA_ONLY'})
    monkeypatch.setattr(helper, 'namespace_proof', lambda parent, mode: {'unit_scope': 'SCHEMA_ONLY'})
    monkeypatch.setattr(helper, 'pip_load', lambda actual_root, bootstrap: {})
    monkeypatch.setattr(helper, 'negative_probes', lambda use_requests: {})
    monkeypatch.setattr(helper, 'installed_inventory', lambda: {'pip': helper.PIP_VERSION})
    monkeypatch.setattr(helper, 'lnx03_runner', observed_runner)
    monkeypatch.setattr(os, 'environ', values)
    result = root / 'jobs/result.json'
    job = {'format': 'lnx03-worker-v1', 'root': str(root), 'nonce': 'e' * 64,
           'kind': 'application', 'result': str(result),
           'helper_sha256': helper.digest(helper.Path(helper.__file__)), 'parent': {},
           'isolation_mode': 'rootless_user_net', 'rootless_primitive': None,
           'authority': authority, 'repo': str(repo), 'source_files_sha256': source,
           'versions': {}, 'tests_result': str(root / 'jobs/tests.json'),
           'native_root': str(root / 'fixtures/native')}
    job_path = root / 'jobs/job.json'
    job_path.write_bytes(helper.json_bytes(job))
    code = helper.lnx03_worker(SimpleNamespace(job=str(job_path), job_sha256=helper.digest(job_path)))
    persisted = json.loads(result.read_text(encoding='utf-8'))
    assert witness == ['actual-runner-boundary']
    assert code == 1 and persisted['status'] == 'FAIL'
    if runner_raises:
        assert persisted['failure_code'] == 'lnx03-runner-exit-or-result-incomplete'
        assert persisted['observations'] == {}
        assert 'pip' not in persisted['observations']
        # This failed application projection remains admissible without
        # inventing a runner receipt, child record, or successful test count.
        helper.lnx03_validate_application_observations(persisted['observations'], sha, complete=False)
    else:
        assert persisted['failure_code'] == 'lnx03-application-tests-failed'
        assert persisted['observations'] == outcome
        assert len(persisted['observations']['tests']['native_cases']) == 3


@pytest.mark.parametrize('section', [None, 'source', 'authority', 'environment', 'setup', 'isolation', 'cleanup'])
def test_result_unknown_fields_refused(helper, admission_environment, section):
    report = failed_result(helper, admission_environment)
    target = report if section is None else report[section]
    target['unexpected'] = True
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('section,key,value', [
    (None, 'schema_version', 'legacy-schema'), (None, 'scope', 'VERIFIED_RELEASE'),
    (None, 'consumer', 'arbitrary-command'), (None, 'status', 'SUCCESS'),
    (None, 'failure_code', None), (None, 'failure_code', 'C:\\private\\canary'),
    ('source', 'sha', 'd' * 39), ('source', 'base', 'd' * 39),
    ('source', 'helper_sha256', 'e' * 64),
    ('authority', 'sha', 'd' * 40), ('authority', 'base', 'd' * 40),
    ('authority', 'approved', False), ('authority', 'attempt', 2),
    ('authority', 'attempt', True), ('authority', 'run_number', True),
    ('authority', 'run_id', 'not-a-run'), ('authority', 'ticket_id', 'e' * 63),
    ('authority', 'repository', 'someone-else/moex-dataset'),
    ('authority', 'actor', 'someone-else'), ('authority', 'triggering_actor', 'someone-else'),
    ('authority', 'event', 'pull_request_target'), ('authority', 'ref', 'refs/heads/codex/lnx-02'),
    ('authority', 'workflow', '.github/workflows/other.yml'), ('authority', 'job', 'other-job'),
])
def test_result_identity_scope_and_failure_refusals(
        helper, admission_environment, section, key, value):
    report = failed_result(helper, admission_environment)
    target = report if section is None else report[section]
    target[key] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('name', ['foreign.py', 'C:\\private\\canary', '../outside.py'])
def test_result_source_manifest_does_not_admit_foreign_paths(helper, admission_environment, name):
    report = failed_result(helper, admission_environment)
    report['source']['files_sha256'][name] = 'd' * 64
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


def test_result_source_manifest_requires_all_fixed_inputs(helper, admission_environment):
    report = failed_result(helper, admission_environment)
    del report['source']['files_sha256']['run_logging.py']
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('field', ['sha', 'base', 'helper_sha256'])
@pytest.mark.parametrize('value', [None, 7, ['not-a-digest']])
def test_result_source_malformed_digest_types_are_closed_refusals(
        helper, admission_environment, field, value):
    report = failed_result(helper, admission_environment)
    report['source'][field] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('value', [None, 7, ['not-a-digest']])
def test_result_source_manifest_malformed_values_are_closed_refusals(
        helper, admission_environment, value):
    report = failed_result(helper, admission_environment)
    report['source']['files_sha256']['run_logging.py'] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('value', [None, 7, ['not-a-manifest']])
def test_result_source_manifest_malformed_container_is_closed_refusal(
        helper, admission_environment, value):
    report = failed_result(helper, admission_environment)
    report['source']['files_sha256'] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('field', ['run_id', 'ticket_id'])
@pytest.mark.parametrize('value', [None, 7, ['not-an-identity']])
def test_result_authority_malformed_types_are_closed_refusals(
        helper, admission_environment, field, value):
    report = failed_result(helper, admission_environment)
    report['authority'][field] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('field', ['fingerprint_before', 'fingerprint_after'])
@pytest.mark.parametrize('value', [7, ['not-a-fingerprint']])
def test_pass_result_malformed_fingerprint_types_are_closed_refusals(
        helper, admission_environment, field, value):
    report = failed_result(helper, admission_environment)
    report.update(status='PASS', failure_code=None)
    report['setup'].update(completed=True, wheel_count=82)
    report['cleanup'] = {'state': 'SETTLED', 'owned_deleted': True}
    report['environment'].update(pip_check_exit=0, fingerprint_before='e' * 64,
                                 fingerprint_after='e' * 64)
    report['environment'][field] = value
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


def test_result_unhashable_status_is_closed_refusal(helper, admission_environment):
    report = failed_result(helper, admission_environment)
    report['status'] = ['PASS']
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


def schema_only_success(helper, admission_environment, repo, monkeypatch):
    """Isolate outer receipt admission; these sentinel validators prove no OS fact.

    Actual native identity, namespace and thirteen native cases have independent
    hosted tests. This fixture replaces only those downstream validators so an
    empty stage list or forged runner counts cannot fail for an unrelated reason.
    """
    report = failed_result(helper, admission_environment)
    inventory = {name: record[0] for name, record in helper.parse_lock(
        (repo / 'requirements/linux-cp314-dev.lock').read_text(encoding='utf-8')).items()}
    inventory['pip'] = '26.2.1'
    identity = {'unit_scope': 'SCHEMA_ONLY_IDENTITY'}
    namespace = {'unit_scope': 'SCHEMA_ONLY_NAMESPACE'}
    cases = [{'unit_scope': 'SCHEMA_ONLY_CASES'}]
    reached = []

    def identity_sentinel(value, helper_sha):
        assert value == identity and helper_sha == 'd' * 64
        if 'identity' not in reached:
            reached.append('identity')

    def namespace_sentinel(value, mode):
        assert value == namespace and mode == 'rootless_user_net'
        if 'namespace' not in reached:
            reached.append('namespace')

    def cases_sentinel(value, require_complete=True):
        assert value == cases
        if 'cases' not in reached:
            reached.append('cases')

    monkeypatch.setattr(helper, 'validate_native_identity_evidence', identity_sentinel)
    monkeypatch.setattr(helper, 'lnx03_validate_identity', identity_sentinel)
    monkeypatch.setattr(helper, 'validate_namespace_evidence', namespace_sentinel)
    monkeypatch.setattr(helper, 'lnx03_validate_native_cases', cases_sentinel)
    stages = [{'kind': kind, 'status': 'PASS', 'exit_code': 0,
               'termination_confirmed': True, 'timed_out': False, 'observations': {}}
              for kind in ('preflight', 'bootstrap', 'install', 'check', 'application')]
    stages[0]['namespace'] = namespace
    stages[3]['observations'] = {'inventory': dict(inventory), 'pip_operation': {'exit_code': 0}}
    stages[4]['observations'] = {
        'process': {'started': True, 'pidfd_bound': True, 'termination_confirmed': True,
                    'timed_out': False, 'exit_code': 0},
        'tests': {'schema_version': 'lnx03-runner-result-v1', 'status': 'PASS',
                  'source_sha': report['source']['sha'], 'collected': 29, 'passed': 29,
                  'failed': 0, 'deselected': 0, 'exit_code': 0,
                  'native_cases': cases, 'failure_code': None},
    }
    report.update(status='PASS', failure_code=None, cases=cases)
    report['environment'].update(identity=identity, fingerprint_before='e' * 64,
                                 fingerprint_after='e' * 64, inventory=inventory, pip_check_exit=0)
    report['setup'].update(completed=True, wheel_count=82, stages=stages)
    report['isolation'].update(mode='rootless_user_net', preflight=stages[0])
    report['cleanup'] = {'state': 'SETTLED', 'owned_deleted': True}
    return report, reached


def test_complete_outer_schema_reaches_all_isolated_validators(
        helper, admission_environment, repo, monkeypatch):
    report, reached = schema_only_success(helper, admission_environment, repo, monkeypatch)
    assert helper.lnx03_validate_result(report) == report
    assert reached == ['identity', 'namespace', 'cases']


@pytest.mark.parametrize('mutation', [
    'empty-stages', 'missing-check', 'duplicate-stage', 'reordered-stages', 'extra-stage',
    'unsettled-stage', 'timedout-stage', 'boolean-stage-exit', 'failed-stage',
    'preflight-disagrees', 'inventory-disagrees', 'pip-check-disagrees', 'boolean-pip-check',
    'process-not-started', 'process-no-pidfd', 'process-unsettled', 'process-timeout',
    'process-failed', 'boolean-process-exit', 'collected-zero', 'collected-native-only', 'passed-disagrees',
    'failed-count', 'deselected-count', 'runner-exit', 'boolean-count',
    'runner-source', 'runner-cases', 'runner-schema', 'runner-unknown-field',
])
def test_success_requires_complete_ordered_stages_and_bound_runner_observations(
        helper, admission_environment, repo, monkeypatch, mutation):
    report, reached = schema_only_success(helper, admission_environment, repo, monkeypatch)
    stages = report['setup']['stages']
    process = stages[4]['observations']['process']
    tests = stages[4]['observations']['tests']
    if mutation == 'empty-stages':
        stages.clear()
    elif mutation == 'missing-check':
        del stages[3]
    elif mutation == 'duplicate-stage':
        stages[2] = dict(stages[1])
    elif mutation == 'reordered-stages':
        stages[1], stages[2] = stages[2], stages[1]
    elif mutation == 'extra-stage':
        stages.append(dict(stages[1]))
    elif mutation == 'unsettled-stage':
        stages[2]['termination_confirmed'] = False
    elif mutation == 'timedout-stage':
        stages[2]['timed_out'] = True
    elif mutation == 'boolean-stage-exit':
        stages[2]['exit_code'] = False
    elif mutation == 'failed-stage':
        stages[2]['exit_code'] = 1
    elif mutation == 'preflight-disagrees':
        report['isolation']['preflight'] = dict(stages[0], extra='forged')
    elif mutation == 'inventory-disagrees':
        name = next(name for name in report['environment']['inventory'] if name != 'pip')
        report['environment']['inventory'][name] = '0.0'
    elif mutation == 'pip-check-disagrees':
        stages[3]['observations']['pip_operation']['exit_code'] = 1
    elif mutation == 'boolean-pip-check':
        stages[3]['observations']['pip_operation']['exit_code'] = False
    elif mutation == 'process-not-started':
        process['started'] = False
    elif mutation == 'process-no-pidfd':
        process['pidfd_bound'] = False
    elif mutation == 'process-unsettled':
        process['termination_confirmed'] = False
    elif mutation == 'process-timeout':
        process['timed_out'] = True
    elif mutation == 'process-failed':
        process['exit_code'] = 1
    elif mutation == 'boolean-process-exit':
        process['exit_code'] = False
    elif mutation == 'collected-zero':
        tests.update(collected=0, passed=0)
    elif mutation == 'collected-native-only':
        tests.update(collected=13, passed=13)
    elif mutation == 'passed-disagrees':
        tests['passed'] = 28
    elif mutation == 'failed-count':
        tests['failed'] = 1
    elif mutation == 'deselected-count':
        tests['deselected'] = 1
    elif mutation == 'runner-exit':
        tests['exit_code'] = 1
    elif mutation == 'boolean-count':
        tests['failed'] = False
    elif mutation == 'runner-source':
        tests['source_sha'] = 'f' * 40
    elif mutation == 'runner-cases':
        tests['native_cases'] = []
    elif mutation == 'runner-schema':
        tests['schema_version'] = 'legacy-schema'
    elif mutation == 'runner-unknown-field':
        tests['arbitrary_target'] = 'main.py'
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)
    assert 'cases' not in reached


def test_success_inventory_cannot_be_jointly_relabelled_by_environment_and_check_stage(
        helper, admission_environment, repo, monkeypatch):
    report, _ = schema_only_success(helper, admission_environment, repo, monkeypatch)
    name = next(name for name in report['environment']['inventory'] if name != 'pip')
    report['environment']['inventory'][name] = '0.0'
    report['setup']['stages'][3]['observations']['inventory'][name] = '0.0'
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


@pytest.mark.parametrize('field', ['source', 'authority', 'environment', 'setup', 'isolation', 'cleanup', 'cases'])
def test_result_missing_sections_refused(helper, admission_environment, field):
    report = failed_result(helper, admission_environment)
    del report[field]
    with pytest.raises(helper.PolicyError):
        helper.lnx03_validate_result(report)


def runner_ticket(runner, helper, admission_environment, repo, tmp_path):
    values, sha, base, _ = admission_environment
    root = tmp_path / 'lnx03-unit-owned'
    root.mkdir(mode=0o700)
    (root / 'jobs').mkdir(mode=0o700)
    (root / 'fixtures').mkdir(mode=0o700)
    (root / 'fixtures/native').mkdir(mode=0o700)
    nonce = 'c' * 32
    marker = root / '.lnx02-owner.json'
    marker.write_bytes(runner.json_bytes({'nonce': nonce, 'purpose': 'LNX-03'}))
    marker.chmod(0o600)
    ticket = {
        'schema_version': 'lnx03-runner-ticket-v1', 'root': str(root), 'nonce': nonce,
        'repo': str(repo), 'parent_pid': os.getppid(), 'source_sha': sha,
        'source_files_sha256': {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
                                for name in SOURCE_PATHS},
        'authority': helper.lnx03_validate_grant(values, sha, base),
        'result': str(root / 'jobs/application-tests.json'),
        'native_root': str(root / 'fixtures/native'),
    }
    return root, ticket


def publish_runner_ticket(runner, root, ticket):
    path = root / 'jobs/application-ticket.json'
    data = runner.json_bytes(ticket)
    path.write_bytes(data)
    path.chmod(0o600)
    return {'MDS_LNX03_RUN_TICKET': str(path),
            'MDS_LNX03_RUN_TICKET_SHA256': hashlib.sha256(data).hexdigest()}


def test_runner_accepts_only_closed_source_bound_ticket(
        runner, helper, admission_environment, repo, tmp_path):
    assert tuple(runner.SOURCE_PATHS) == SOURCE_PATHS
    root, ticket = runner_ticket(runner, helper, admission_environment, repo, tmp_path)
    environment = publish_runner_ticket(runner, root, ticket)
    assert runner.validate_ticket(environment, repo) == ticket
    runner.claim_ticket(ticket)
    original = (root / 'jobs/application-ticket.claimed').read_bytes()
    with pytest.raises(runner.Refusal):
        runner.claim_ticket(ticket)
    assert (root / 'jobs/application-ticket.claimed').read_bytes() == original


def test_runner_missing_ticket_refused_before_native_import(runner, repo):
    with pytest.raises(runner.Refusal):
        runner.validate_ticket({}, repo)


def test_runner_export_scanner_retains_complete_admitted_bytes(runner):
    payload = b'{"status":"PASS"}'
    assert runner.admit_export_bytes(payload) is payload
    boundary = b' ' * (1024 * 1024 - 2) + b'{}'
    admitted = runner.admit_export_bytes(boundary)
    assert admitted is boundary and len(admitted) == 1024 * 1024


@pytest.mark.parametrize('payload', [
    b'LNX03_NATIVE_PRIVATE_CANARY_7fe94d31',
    b'{"detail":"https://example.invalid/canary"}',
    b'{"detail":"http://example.invalid/canary"}',
    b'{"detail":"file:///private/canary"}',
    b'Traceback (most recent call last):',
    b' ' * (1024 * 1024 + 1), None, 7, bytearray(b'{}'),
], ids=['canary', 'https', 'http', 'file-url', 'traceback', 'boundary-plus-one',
        'null', 'integer', 'bytearray'])
def test_runner_export_scanner_refuses_canary_urls_traceback_and_overflow(runner, payload):
    with pytest.raises(runner.Refusal):
        runner.admit_export_bytes(payload)


@pytest.mark.parametrize('field,value', [
    ('schema_version', 'foreign-schema'), ('nonce', 'a' * 31),
    ('parent_pid', -1), ('parent_pid', True), ('source_sha', 'd' * 39),
    ('result', 'application-tests.json'), ('native_root', 'fixtures/native'),
    ('repo', 'foreign-checkout'),
])
def test_runner_ticket_identity_mutations_refused(
        runner, helper, admission_environment, repo, tmp_path, field, value):
    root, ticket = runner_ticket(runner, helper, admission_environment, repo, tmp_path)
    ticket[field] = value
    environment = publish_runner_ticket(runner, root, ticket)
    with pytest.raises((runner.Refusal, FileNotFoundError)):
        runner.validate_ticket(environment, repo)


def test_runner_ticket_digest_and_foreign_parent_refused(
        runner, helper, admission_environment, repo, tmp_path):
    root, ticket = runner_ticket(runner, helper, admission_environment, repo, tmp_path)
    environment = publish_runner_ticket(runner, root, ticket)
    environment['MDS_LNX03_RUN_TICKET_SHA256'] = '0' * 64
    with pytest.raises(runner.Refusal):
        runner.validate_ticket(environment, repo)
    ticket['parent_pid'] = os.getppid() + 1
    environment = publish_runner_ticket(runner, root, ticket)
    with pytest.raises(runner.Refusal):
        runner.validate_ticket(environment, repo)


@pytest.mark.parametrize('mutation', ['foreign-leaf', 'foreign-root', 'missing-source', 'wrong-source',
                                      'foreign-source', 'foreign-authority', 'result-exists'])
def test_runner_rejects_foreign_roots_and_source_maps(
        runner, helper, admission_environment, repo, tmp_path, mutation):
    root, ticket = runner_ticket(runner, helper, admission_environment, repo, tmp_path)
    if mutation == 'foreign-root':
        other = tmp_path / 'lnx03-foreign-root'
        other.mkdir(mode=0o700)
        ticket['root'] = str(other)
    elif mutation == 'missing-source':
        del ticket['source_files_sha256']['run_logging.py']
    elif mutation == 'wrong-source':
        ticket['source_files_sha256']['run_logging.py'] = 'e' * 64
    elif mutation == 'foreign-source':
        ticket['source_files_sha256']['main.py'] = 'e' * 64
    elif mutation == 'foreign-authority':
        ticket['authority']['ref'] = 'refs/heads/codex/lnx-02'
    elif mutation == 'result-exists':
        (root / 'jobs/application-tests.json').write_text('{}', encoding='utf-8')
    environment = publish_runner_ticket(runner, root, ticket)
    if mutation == 'foreign-leaf':
        original = root / 'jobs/application-ticket.json'
        foreign = root / 'jobs/foreign-ticket.json'
        foreign.write_bytes(original.read_bytes())
        foreign.chmod(0o600)
        environment['MDS_LNX03_RUN_TICKET'] = str(foreign)
    with pytest.raises(runner.Refusal):
        runner.validate_ticket(environment, repo)


@pytest.mark.parametrize('arguments', [[], ['--lane', 'd2'], ['--lane', 'linux-bootstrap', 'tests.py'],
                                      ['--lane', 'linux-bootstrap', '--timeout', '1'], ['--help']])
def test_runner_cli_refusal_is_closed(runner, monkeypatch, capsys, arguments):
    monkeypatch.setattr(sys, 'argv', ['offline_linux.py'] + arguments)
    assert runner.main() == 1
    assert capsys.readouterr().out == 'LNX03_RUNNER_REFUSED\n'


def test_runner_unsupported_platform_refuses_before_ticket_or_imports(runner, monkeypatch, capsys):
    calls = []

    def unexpected(*args):
        calls.append('ticket')
        raise AssertionError('platform must be checked first')

    monkeypatch.setattr(runner, 'validate_ticket', unexpected)
    monkeypatch.setattr(sys, 'argv', ['offline_linux.py', '--lane', 'linux-bootstrap'])
    monkeypatch.setattr(sys, 'platform', 'win32')
    assert runner.main() == 1
    assert calls == []
    assert capsys.readouterr().out == 'LNX03_RUNNER_REFUSED\n'
