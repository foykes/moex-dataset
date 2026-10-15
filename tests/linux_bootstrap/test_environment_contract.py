"""Source-bound legacy compatibility and immutable Linux profile oracles."""

import ast
import hashlib
import json
from types import SimpleNamespace

import pytest


# Exact function-source fingerprints from reviewed e411a154b47446997fd4697c5cb4d7cc604519bf.
# These bind existing grant/result/identity semantics, rather than snapshotting
# the new helper and then declaring its current behaviour to be expected.
LEGACY_FUNCTION_SHA256 = {
    'validate_grant': 'f15c26323c3aee8cfabddbc1df84626f59e814a87d82e43dde0fc7ce948df081',
    'validate_supervised_grant': '0adec01fcda1c359c9723c3d032a8e1c6ffcaafa2f001982bb6e3fcd469aeb99',
    'source_identity': 'c593ccadeaf88c8ab21feae074a66580f76a41f388fe8aa4568fa229935a2002',
    'source_manifest': 'a1d08d78da0e450b5b1ccecfefc51a1df324d6216999909a82c619e638218f57',
    'validate_success_evidence': 'b9014ec9369208a15c082f5f6073450fddca23602f79ee2ab77fdb16715c2190',
    'validate_diagnostic_report': '74cc422845bdaa1d18b6c24f18c5ba0d7420d49e009433c1f6409995ec9da041',
    'audit_output': '1d9344a02d4a79183c0af151011a075435ab0022d359f8188e3eca0dc2c05d17',
    'expected_profiles': '5561b577057e2d329b8c7351c9f63803ee70a98cb5ac882829a54d59d95568e2',
    'parse_lock': '2bbc69601f3fa911ecc059de96f163eec1ee6b4a96897638c7f84d3c49607809',
    'cleanup': '1636a8f5560ee6279917cd5bb7f9c01aa920689a4e5601ab1f1cdaaa861d2d53',
    'new_scratch': '81314668764cbe767d0642e831722dec97e6e0dcf9b91f5981db9ac0303c9740',
    'finalize_output': 'c7675fc3b40abec6e6d260959b3659c2251dbe8f663f796056e36e4d22607906',
}


@pytest.mark.parametrize('name,expected', LEGACY_FUNCTION_SHA256.items())
def test_legacy_source_bound_function(repo, name, expected):
    source = (repo / '.github/lnx02/verify_env.py').read_text(encoding='utf-8')
    definitions = {node.name: node for node in ast.parse(source).body
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    actual = ast.get_source_segment(source, definitions[name])
    assert hashlib.sha256(actual.encode('utf-8')).hexdigest() == expected


def test_linux_locks_are_exact_unchanged_closures(helper, repo):
    profiles = {}
    for profile, count in [('runtime', 42), ('test', 46), ('dev', 82)]:
        data = (repo / ('requirements/linux-cp314-' + profile + '.lock')).read_text(encoding='utf-8')
        profiles[profile] = helper.parse_lock(data)
        assert len(profiles[profile]) == count
        assert 'pywin32' not in profiles[profile]
    for name, record in profiles['runtime'].items():
        assert profiles['test'][name] == record
        assert profiles['dev'][name] == record
    for name, record in profiles['test'].items():
        assert profiles['dev'][name] == record
    assert helper.PIP_VERSION == '26.2.1'
    assert helper.PIP_SHA256 == '71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e'


@pytest.mark.parametrize('mode', ['diagnostic', 'generate', 'verify'])
def test_legacy_valid_push_grant_remains_accepted(helper, legacy_environment, mode):
    values, sha, base = legacy_environment
    values['MDS_LNX02_OPERATION'] = mode
    helper.validate_grant(values, mode, sha, base)


@pytest.mark.parametrize('key,value', [
    ('GITHUB_SHA', 'd' * 40), ('GITHUB_ACTOR', 'someone-else'),
    ('GITHUB_TRIGGERING_ACTOR', 'someone-else'),
    ('GITHUB_REF', 'refs/heads/codex/lnx-03-d2-bootstrap'),
    ('TASK_RUNNER_ENVIRONMENT', 'self-hosted'),
    ('TASK_DELETED', 'true'), ('MDS_LNX02_OPERATION', 'linux-bootstrap'),
    ('MDS_LNX02_ALLOW_SUPERVISED_NETNS', 'TRUE'),
])
def test_legacy_negative_grant_rules_remain_closed(helper, legacy_environment, key, value):
    values, sha, base = legacy_environment
    values[key] = value
    with pytest.raises(helper.PolicyError):
        helper.validate_grant(values, 'verify', sha, base)


def test_legacy_dispatch_generate_still_refused(helper, legacy_environment):
    values, sha, _ = legacy_environment
    values.update(GITHUB_EVENT_NAME='workflow_dispatch', TASK_EVENT='workflow_dispatch',
                  GITHUB_REF='refs/heads/main', TASK_EVENT_REF='refs/heads/main',
                  MDS_LNX02_APPROVED_BASE=sha, TASK_DISPATCH_ALLOW='true', TASK_DISPATCH_SHA=sha,
                  MDS_LNX02_OPERATION='generate')
    with pytest.raises(helper.PolicyError):
        helper.validate_grant(values, 'generate', sha, sha)


def test_new_source_manifest_reports_actual_helper_bytes(helper, repo):
    manifest = helper.lnx03_source_manifest(repo)
    key = '.github/lnx02/verify_env.py'
    actual = hashlib.sha256((repo / key).read_bytes()).hexdigest()
    assert manifest[key] == actual
    assert key in helper.source_manifest(repo, 'verify')
    assert helper.source_manifest(repo, 'verify')[key] == actual
    assert set('requirements/linux-cp314-' + profile + '.lock'
               for profile in ('runtime', 'test', 'dev')).issubset(manifest)


@pytest.mark.parametrize('bad', [
    'a==1.0', 'a>=1.0', '--index-url https://example.invalid',
    '-e a==1.0', '-r requirements.txt',
    'a==1.0 --hash=sha256:' + '0' * 64 + ' --hash=sha256:' + '1' * 64,
])
def test_legacy_hash_lock_parser_refusals(helper, bad):
    with pytest.raises(helper.PolicyError):
        helper.parse_lock(bad)


def test_legacy_failed_bundle_closed_names_and_manifest(helper, tmp_path, monkeypatch):
    output = tmp_path / 'bundle'
    output.mkdir(mode=0o700)
    report = {'schema_version': 1, 'mode': 'verify', 'status': 'FAIL',
              'source_sha': 'a' * 40, 'base_sha': 'b' * 40,
              'failure_code': 'invalid-cli'}
    helper.finalize_output(output, report)
    assert {path.name for path in output.iterdir()} == {'report.json', 'manifest.json'}
    data = (output / 'report.json').read_bytes()
    assert json.loads(data) == report
    assert json.loads((output / 'manifest.json').read_bytes()) == {
        'report.json': hashlib.sha256(data).hexdigest()}
    monkeypatch.setattr(helper, 'flags', lambda: None)
    args = SimpleNamespace(output=str(output), expected_sha='a' * 40,
                           expected_base='b' * 40, mode='verify', success='false')
    assert helper.audit_output(args)['artifact_count'] == 2
    (output / 'unexpected.json').write_text('{}', encoding='utf-8')
    with pytest.raises(helper.PolicyError):
        helper.audit_output(args)


@pytest.mark.parametrize('payload', [
    {'secret': 'canary'}, {'nested': {'private_key': 'canary'}},
    {'message': 'C:\\private\\canary'}, {'message': 'file:///private/canary'},
    {'message': 'Bearer canary'}, {'message': '/private/canary'},
])
def test_legacy_export_refuses_private_evidence(helper, payload):
    with pytest.raises(helper.PolicyError):
        helper.sanitized(payload)


def test_legacy_source_identity_requires_actual_clean_head(helper, repo, monkeypatch):
    sha, base = 'a' * 40, 'b' * 40
    seen = []

    def read(_, arguments, allowed_failure=False):
        seen.append(tuple(arguments))
        if arguments[:1] == ['rev-parse']:
            value = sha if arguments[1] == 'HEAD' else base
            return SimpleNamespace(returncode=0, stdout=value.encode('ascii'))
        if arguments[:1] == ['diff']:
            return SimpleNamespace(returncode=1, stdout=b'')
        return SimpleNamespace(returncode=0, stdout=b'')

    monkeypatch.setattr(helper, 'git_read', read)
    with pytest.raises(helper.PolicyError, match='tracked-source-is-dirty'):
        helper.source_identity(repo, sha, base)
    assert seen == [('rev-parse', 'HEAD'), ('rev-parse', 'refs/remotes/origin/main'),
                    ('merge-base', '--is-ancestor', base, sha), ('diff', '--exit-code', 'HEAD')]


def test_legacy_supervised_requires_its_own_opt_in(helper, admission_environment):
    values, _, _, grant = admission_environment
    values['MDS_LNX03_SUPERVISED_NETNS_GRANT'] = json.dumps(
        dict(grant, capability='supervised-netns'))
    with pytest.raises(helper.PolicyError):
        helper.validate_supervised_grant(values)
