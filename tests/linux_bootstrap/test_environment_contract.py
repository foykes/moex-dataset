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


def test_lnx03_source_accepts_exact_scope_and_approved_ancestor(helper, source_repository):
    fixture = source_repository
    assert len(fixture['allowed']) == 12 and len(fixture['protected']) == 10
    assert set(helper.LNX03_PATHS) == set(fixture['allowed'])
    assert set(helper.LNX03_DEPENDENCY_PATHS) == set(fixture['allowed'] + fixture['protected'])
    helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], fixture['base'])
    assert fixture['seen'] == [
        ('rev-parse', 'HEAD'), ('cat-file', '-e', fixture['base'] + '^{commit}'),
        ('merge-base', '--is-ancestor', fixture['base'], fixture['head']),
        ('status', '--porcelain=v1', '--untracked-files=all'),
        ('diff', '--name-status', '--no-renames', '-z', fixture['base'], fixture['head'], '--'),
        ('merge-base', '--is-ancestor', fixture['base'], fixture['base']),
        ('diff', '--name-only', '--no-renames', '-z', fixture['base'], fixture['base'], '--'),
    ]


@pytest.mark.parametrize('scope', ['empty', 'one-file'])
def test_lnx03_source_permits_only_add_or_modify_subset_and_detached_head(
        helper, source_repository, scope):
    fixture = source_repository
    changes = b'' if scope == 'empty' else b'A\0' + fixture['allowed'][0].encode('utf-8') + b'\0'
    fixture['replies'][('diff', '--name-status', '--no-renames', '-z', fixture['base'],
                        fixture['head'], '--')] = (0, changes)
    helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], fixture['base'])
    # Detached hosted checkouts are supported: no branch/symbolic-ref query.
    assert not any('symbolic-ref' in command for command in fixture['seen'])


@pytest.mark.parametrize('mutation,code', [
    ('wrong-source', 'lnx03-source-head-drift'),
    ('missing-base', 'lnx03-approved-base-missing'),
    ('missing-ancestry', 'lnx03-base-is-not-source-ancestor'),
    ('dirty-tracked', 'lnx03-source-is-dirty'),
    ('dirty-untracked', 'lnx03-source-is-dirty'),
    ('outside-allowlist', 'lnx03-source-outside-approved-paths'),
    ('delete', 'lnx03-source-outside-approved-paths'),
    ('rename', 'lnx03-source-outside-approved-paths'),
    ('type-change', 'lnx03-source-outside-approved-paths'),
])
def test_lnx03_source_independent_git_reply_negative_boundaries(helper, source_repository, mutation, code):
    fixture = source_repository
    root, replies = fixture['root'], fixture['replies']
    sha, base = fixture['head'], fixture['base']
    if mutation == 'wrong-source':
        replies[('rev-parse', 'HEAD')] = (0, (base + '\n').encode('ascii'))
    elif mutation == 'missing-base':
        replies[('cat-file', '-e', base + '^{commit}')] = (1, b'')
    elif mutation == 'missing-ancestry':
        replies[('merge-base', '--is-ancestor', base, sha)] = (1, b'')
    elif mutation == 'dirty-tracked':
        replies[('status', '--porcelain=v1', '--untracked-files=all')] = (0, b' M docs/testing.md\n')
    elif mutation == 'dirty-untracked':
        replies[('status', '--porcelain=v1', '--untracked-files=all')] = (0, b'?? untracked.txt\n')
    else:
        name = fixture['allowed'][0].encode('utf-8')
        if mutation == 'outside-allowlist':
            changes = b'M\0unrelated.txt\0'
        elif mutation == 'delete':
            changes = b'D\0' + name + b'\0'
        elif mutation == 'rename':
            # --no-renames must still refuse the delete/add representation.
            changes = b'D\0' + name + b'\0A\0outside-renamed.py\0'
        else:
            changes = b'T\0' + name + b'\0'
        replies[('diff', '--name-status', '--no-renames', '-z', base, sha, '--')] = (0, changes)
    with pytest.raises(helper.PolicyError, match='^' + code + '$'):
        helper.lnx03_source_identity(root, sha, base, fixture['base'])


@pytest.mark.parametrize('changes', [b'M\0docs/testing.md', b'M\0', b'R100\0old.py\0new.py\0',
                                    b'C100\0old.py\0new.py\0', b'M\0\xff\0'])
def test_lnx03_source_refuses_malformed_or_ambiguous_diff_records(
        helper, source_repository, changes):
    fixture = source_repository
    fixture['replies'][('diff', '--name-status', '--no-renames', '-z', fixture['base'],
                        fixture['head'], '--')] = (0, changes)
    with pytest.raises(helper.PolicyError):
        helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'])


@pytest.mark.parametrize('index', range(10))
def test_lnx03_source_cannot_amend_readonly_dependencies(helper, source_repository, index):
    fixture = source_repository
    fixture['replies'][('diff', '--name-status', '--no-renames', '-z', fixture['base'],
                        fixture['head'], '--')] = (
                            0, b'M\0' + fixture['protected'][index].encode('utf-8') + b'\0')
    with pytest.raises(helper.PolicyError, match='^lnx03-source-outside-approved-paths$'):
        helper.lnx03_source_identity(fixture['root'], fixture['head'],
                                    fixture['base'], fixture['base'])


@pytest.mark.parametrize('mutation', ['extra', 'missing', 'duplicate', 'reordered'])
def test_lnx03_approved_allowlist_cannot_be_caller_redefined(
        helper, source_repository, monkeypatch, mutation):
    paths = list(source_repository['allowed'])
    if mutation == 'extra':
        paths.append('unrelated.txt')
    elif mutation == 'missing':
        paths.pop()
    elif mutation == 'duplicate':
        paths[-1] = paths[0]
    else:
        paths[0], paths[1] = paths[1], paths[0]
    monkeypatch.setattr(helper, 'LNX03_PATHS', tuple(paths))
    with pytest.raises(helper.PolicyError, match='^lnx03-approved-path-contract-changed$'):
        helper.lnx03_source_identity(source_repository['root'], source_repository['head'],
                                    source_repository['base'])


@pytest.mark.parametrize('index', range(22))
def test_lnx03_observed_main_rejects_each_dependent_interface(
        helper, source_repository, index):
    fixture = source_repository
    main = fixture['main']
    path = (fixture['allowed'] + fixture['protected'])[index]
    fixture['replies'][('diff', '--name-only', '--no-renames', '-z', fixture['base'], main, '--')] = (
        0, path.encode('utf-8') + b'\0')
    with pytest.raises(helper.PolicyError, match='^lnx03-main-dependent-interface-drift$'):
        helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], main)


def test_lnx03_accepts_disjoint_main_then_ignores_post_admission_ref_drift(helper, source_repository):
    fixture = source_repository
    main = fixture['main']
    helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], main)
    incompatible = 'd' * 40
    fixture['replies'][('rev-parse', 'refs/remotes/origin/main')] = (0, incompatible.encode('ascii') + b'\n')
    fixture['replies'][('merge-base', '--is-ancestor', fixture['base'], incompatible)] = (0, b'')
    fixture['replies'][('diff', '--name-only', '--no-renames', '-z', fixture['base'], incompatible, '--')] = (
        0, b'run_logging.py\0')
    # The immutable admission snapshot remains auditable. Final exact-SHA
    # validation does not silently substitute a newly moved remote main.
    helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], main)
    helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'])
    assert ('rev-parse', 'refs/remotes/origin/main') not in fixture['seen']
    with pytest.raises(helper.PolicyError, match='^lnx03-main-dependent-interface-drift$'):
        helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], incompatible)


@pytest.mark.parametrize('main', ['malformed', 'missing', 'unrelated-history'])
def test_lnx03_main_snapshot_must_be_valid_descendant(helper, source_repository, main):
    fixture = source_repository
    if main == 'malformed':
        observed = 'not-a-sha'
        code = 'lnx03-main-observation-invalid'
    elif main == 'missing':
        observed = '0' * 40
        code = 'lnx03-main-is-not-base-descendant'
    else:
        observed = 'e' * 40
        code = 'lnx03-main-is-not-base-descendant'
    if main != 'malformed':
        fixture['replies'][('merge-base', '--is-ancestor', fixture['base'], observed)] = (1, b'')
    with pytest.raises(helper.PolicyError, match='^' + code + '$'):
        helper.lnx03_source_identity(fixture['root'], fixture['head'], fixture['base'], observed)
