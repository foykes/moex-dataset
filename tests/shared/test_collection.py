"""Actual prepared-environment commands, separate from the canonical launcher."""

import importlib.util
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[2]
LEGACY_NODES = {
    'main_tests.py::data_gathering___moex_query::tests_moex_query',
    'main_tests.py::data_gathering___moex::tests_moex_',
}
_command_context = guard.ensure_context(ROOT)
pytestmark = pytest.mark.skipif(
    os.environ.get('MDS_OFFLINE_COMMAND_CHECK') == '1' and _command_context.role == 'command',
    reason='This actual default command executes every other shared test; command probes do not recurse',
)


def _launcher():
    module = sys.modules.get('mds_offline_tests')
    if module is None:
        specification = importlib.util.spec_from_file_location('mds_offline_tests', ROOT / 'tools/offline_tests.py')
        module = importlib.util.module_from_spec(specification)
        sys.modules['mds_offline_tests'] = module
        specification.loader.exec_module(module)
    return module


def _command(profile):
    result = _launcher().run_command_check(profile)
    assert set(result) >= {'exit_code', 'stdout', 'stderr', 'report'}
    return result


@pytest.mark.parametrize('profile', ['console-pytest', 'module-pytest', 'unittest-discover'])
def test_actual_default_commands_have_substantive_tests(profile):
    result = _command(profile)
    assert result['exit_code'] == 0, result['stdout'] + result['stderr']
    nodes = result['report']['collected']
    assert nodes and all(node.startswith('tests/shared/') for node in nodes)
    assert any('test_candle_retry_503_then_200_has_literal_rows_and_keys' in node for node in nodes)
    assert any('test_make_error_has_literal_redacted_contract' in node for node in nodes)
    reports = result['report']['reports']
    assert any(report['outcome'] == 'passed' for report in reports)
    # Only the named command-probe recursion guard may skip in this nested run.
    skipped = [report for report in reports if report['outcome'] == 'skipped']
    assert all('tests/shared/test_collection.py::' in str(report.get('test', report.get('nodeid', '')))
               for report in skipped)


@pytest.mark.parametrize('profile', ['legacy-collect', 'legacy-module-collect', 'main-collect', 'main-module-collect'])
def test_actual_explicit_pytest_collection_has_two_original_nodes(profile):
    result = _command(profile)
    assert result['exit_code'] == 0, result['stdout'] + result['stderr']
    assert set(result['report']['collected']) == LEGACY_NODES
    assert result['report']['reports'] == []
    assert result['report']['mode'] == 'collect'


def test_canonical_environment_drops_inherited_git_metadata_overrides(monkeypatch, tmp_path):
    names = ('GIT_COMMON_DIR', 'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE',
             'GIT_OBJECT_DIRECTORY', 'GIT_EXEC_PATH', 'GIT_CONFIG_COUNT')
    for name in names:
        monkeypatch.setenv(name, str(tmp_path / 'synthetic-foreign-metadata'))
    context = guard.ensure_context(ROOT)
    environment = _launcher().prepared_environment(context)
    assert all(name not in environment for name in names)
    assert environment['GIT_CONFIG_GLOBAL'] == os.devnull
    assert environment['GIT_CONFIG_NOSYSTEM'] == '1'


@pytest.mark.parametrize('profile', ['legacy-pytest', 'legacy-module-pytest', 'main-only-pytest',
                                    'main-only-module', 'marker-live', 'env-live'])
def test_explicit_legacy_runtime_and_selections_cannot_unskip(profile):
    result = _command(profile)
    assert result['exit_code'] == 0, result['stdout'] + result['stderr']
    assert set(result['report']['collected']) == LEGACY_NODES
    skipped = [report for report in result['report']['reports'] if report['outcome'] == 'skipped']
    assert len(skipped) == 2
    assert {report.get('test', report.get('nodeid')) for report in skipped} == LEGACY_NODES
    assert not any(report['outcome'] == 'passed' and report.get('phase', report.get('when')) == 'call'
                   for report in result['report']['reports'])


def test_actual_explicit_unittest_import_has_two_skips():
    result = _command('legacy-unittest')
    output = result['stdout'] + result['stderr']
    assert result['exit_code'] == 0, output
    assert 'Ran 2 tests' in output and 'skipped=2' in output


@pytest.mark.parametrize('profile', ['empty-tests-pytest', 'empty-tests-module',
                                    'empty-tests-collect', 'empty-tests-module-collect'])
def test_tests_module_alone_is_zero_collection_exit_five(profile):
    result = _command(profile)
    assert result['exit_code'] == 5
    assert result['report']['collected'] == []
    assert result['report']['reports'] == []


def test_unknown_marker_and_forwarded_arguments_are_refused():
    result = _command('unknown-marker')
    assert result['exit_code'] != 0
    assert 'marker' in (result['stdout'] + result['stderr']).lower()
    with pytest.raises(ValueError):
        _launcher().run_command_check('module-pytest', arguments=('main.py',))


@pytest.mark.parametrize('profile', ['diagnostic-live-read', 'diagnostic-staging'])
def test_flags_alone_do_not_grant_diagnostic_execution(profile):
    result = _command(profile)
    assert result['exit_code'] == 2
    output = result['stdout'] + result['stderr']
    assert 'не согласован' in output
    assert 'параметры CLI не дают разрешения' in output


@pytest.mark.parametrize('profile, kind', [
    ('collection-network', 'network'), ('collection-sleep', 'sleep'),
    ('collection-write', 'writers'), ('collection-secret', 'secrets'),
    ('collection-data', 'dataset_reads'),
])
def test_actual_collection_import_fault_is_not_hidden_by_zero_tests(profile, kind):
    result = _command(profile)
    assert result['exit_code'] != 0
    assert result['actual_exit'] == 2
    assert result['report']['collected'] == []
    assert result['report']['reports'] == []
    assert result['guard_report']['counters'] == {kind: 1}
    assert result['guard_report']['violations'] == 1
    assert 'F3_COLLECTION_BODY_CALLED' not in result['stdout'] + result['stderr']
    assert 'F3_SLEEP_LEAF_CALLED' not in result['stdout'] + result['stderr']


A_SMOKE = 'tests/A/test_admission_smoke.py'
A_SMOKE_NODE = A_SMOKE + '::test_admission_smoke'


@pytest.mark.parametrize('target, collect_only', [
    ('tests/A', True), (A_SMOKE, True), (A_SMOKE, False),
    (A_SMOKE_NODE, True), (A_SMOKE_NODE, False),
])
def test_actual_a_admission_has_run_bound_guarded_smoke(target, collect_only):
    context = guard.ensure_context(ROOT)
    mode = 'collect' if collect_only else 'run'
    arguments = ['-c', 'pyproject.toml', '-q', target]
    if collect_only:
        arguments.append('--collect-only')
    command = [sys.executable, '-I', '-B', '-X', 'utf8', '-m', 'pytest', *arguments]
    environment = _launcher().prepared_environment(context, command=True)
    ticket = guard.issue_ticket('command', command, expected_exit=0, mode=mode)
    result = guard.run_child(command, role='command', ticket=ticket, env=environment,
                             cwd=ROOT, timeout=60)
    assert result.actual_exit == result.returncode == 0
    assert result.validated and result.report_errors == []
    assert result.report['counters'] == {} and result.report['violations'] == 0
    report_path = context.report_dir / ('pytest-' + ticket['ticket_id'] + '.json')
    report = json.loads(report_path.read_text(encoding='utf-8'))
    for key in ('run_id', 'lane', 'mode', 'tested_sha', 'source_mode',
                'source_digest', 'role', 'ticket_id'):
        assert report[key] == ticket[key]
    assert report['exit_code'] == 0
    assert A_SMOKE_NODE in report['collected']
    assert all(node.startswith('tests/A/') for node in report['collected'])
    if collect_only:
        assert report['reports'] == []
    else:
        assert report['collected'] == [A_SMOKE_NODE]
        assert [record for record in report['reports'] if record['phase'] == 'call'] == [
            {'test': A_SMOKE_NODE, 'phase': 'call', 'outcome': 'passed'},
        ]


def _a_admission_replica(monkeypatch, tmp_path, target):
    replica = tmp_path / 'source'
    for name in ('tests/A/test_contract.py', 'tests/A/helper.py',
                 'tests/shared/test_shared.py', 'tests/AA/test_never.py',
                 'tests/B/test_never.py', 'outside.py'):
        path = replica / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'raise AssertionError("ADMISSION_TARGET_IMPORTED")\n')
    external = tmp_path / 'external/test_never.py'
    external.parent.mkdir()
    external.write_bytes(b'raise AssertionError("ADMISSION_EXTERNAL_IMPORTED")\n')
    context = guard.ensure_context(ROOT)
    module = _launcher()
    monkeypatch.setattr(module, 'ROOT', replica)
    monkeypatch.setattr(module, 'bootstrap_entry', lambda: context)
    registrations = []
    config = SimpleNamespace(
        option=SimpleNamespace(collectonly=False, basetemp=None),
        args=[str(external) if target == 'external' else str(replica / target)],
        inicfg={}, pluginmanager=SimpleNamespace(
            register=lambda plugin, name: registrations.append((plugin, name))),
    )
    return module, config, replica, context, registrations


@pytest.mark.parametrize('target', [
    'tests/A', 'tests/A/test_contract.py', 'tests/A/test_contract.py::test_node',
])
def test_a_admission_keeps_existing_report_plugin(monkeypatch, tmp_path, target):
    module, config, replica, context, registrations = _a_admission_replica(
        monkeypatch, tmp_path, target)
    before = dict(context.counts)
    module.configure_shared(config)
    assert len(registrations) == 1
    plugin, name = registrations[0]
    assert type(plugin) is module.ReportPlugin and plugin.context is context
    assert name == 'mds-offline-report'
    assert Path(config.option.basetemp).is_relative_to(replica / '.f3/tmp')
    assert Path(config.inicfg['cache_dir']).is_relative_to(replica / '.f3/cache')
    assert context.counts == before


@pytest.mark.parametrize('target', [
    'tests/AA/test_never.py', 'tests/B/test_never.py', 'external',
    'tests/A/helper.py', 'tests/A/../../outside.py',
    'tests/shared/../A/test_contract.py',
    'tests/A/../A/test_contract.py',
])
def test_a_admission_refuses_siblings_external_and_traversal(monkeypatch, tmp_path, target):
    module, config, replica, context, registrations = _a_admission_replica(
        monkeypatch, tmp_path, target)
    canary = replica / 'outside.py'
    before_bytes, before_counts = canary.read_bytes(), dict(context.counts)
    with pytest.raises(pytest.UsageError):
        module.configure_shared(config)
    assert registrations == []
    assert canary.read_bytes() == before_bytes
    assert context.counts == before_counts


def test_a_admission_refuses_real_hardlink_alias(monkeypatch, tmp_path):
    module, config, replica, context, registrations = _a_admission_replica(
        monkeypatch, tmp_path, 'tests/A/test_alias.py')
    canary = tmp_path / 'admission-current'
    payload = b'ADMISSION_SYNTHETIC_PREVIOUS\x00\xff\n'
    canary.write_bytes(payload)
    before = canary.stat()
    before_counts = dict(context.counts)
    alias = replica / 'tests/A/test_alias.py'
    os.link(canary, alias)
    try:
        assert canary.stat().st_nlink == 2
        with pytest.raises(pytest.UsageError, match='Unsafe A test target'):
            module.configure_shared(config)
        assert registrations == []
    finally:
        # The existing owned *current unlink rule removes the link itself.
        canary.unlink()
        alias.replace(canary)
    after = canary.stat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert after.st_nlink == 1 and canary.read_bytes() == payload
    assert context.counts == before_counts
