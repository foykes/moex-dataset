"""Actual prepared-environment commands, separate from the canonical launcher."""

import importlib.util
import os
from pathlib import Path
import sys

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
