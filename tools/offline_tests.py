"""Fixed offline entrypoints. No install, live profile, arbitrary target or merge."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[1]
LANE_TARGETS = {'f1': 'tests/environment', 'f2': 'tests/f2',
                'flog': 'tests/logging', 'f3': 'tests/shared'}
PAIRS = ('1year', 'all', 'count_check', 'data_gathering', 'dividends',
         'dohodru_data', 'main_tests', 'tech', 'tests', 'upload')
sys.modules.setdefault('mds_offline_tests', sys.modules[__name__])


def _module(name, path):
    if name not in sys.modules:
        specification = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(specification)
        sys.modules[name] = module
        specification.loader.exec_module(module)
    return sys.modules[name]


def guard_module():
    return _module('mds_offline_guard', ROOT / 'tools/offline_guard.py')


def prepared_environment(context, *, command=False):
    """Child-only E. The caller's environment and immutable interpreter stay intact."""
    environment = dict(os.environ)
    for name in list(environment):
        if name in {'PYTEST_ADDOPTS', 'PYTEST_PLUGINS', 'PYTHONPATH', 'PYTHONHOME',
                    'MDS_OFFLINE_TICKET', 'MDS_ALLOW_LIVE_DIAGNOSTICS'} or name.startswith('GIT_'):
            environment.pop(name, None)
    environment.update(PYTHONDONTWRITEBYTECODE='1', PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',
        PYTHONUTF8='1', PYTHONIOENCODING='utf-8', GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM='1', GIT_TERMINAL_PROMPT='0')
    temporary = context.root / guard_module().LANES[context.lane] / 'tmp'
    temporary.mkdir(parents=True, exist_ok=True)
    environment['TEMP'] = environment['TMP'] = str(temporary)
    # Needed by native fixture hooks; no executable or credential fallback.
    environment['PATH'] = str(Path(sys.executable).parent) + os.pathsep + environment.get('PATH', '')
    environment['MDS_ENVIRONMENT_PROFILE'] = 'dev'
    if context.lane == 'f1':
        environment['MDS_ENVIRONMENT_OUTPUT_ROOT'] = str(context.report_dir)
    if command:
        environment['MDS_OFFLINE_COMMAND_CHECK'] = '1'
    else:
        environment.pop('MDS_OFFLINE_COMMAND_CHECK', None)
    return environment


def bootstrap_entry(root=ROOT):
    guard = guard_module()
    mode = 'collect' if '--collect-only' in sys.argv[1:] else 'run'
    context = guard.ensure_context(root, lane='f3', role='entry', mode=mode)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    temporary = next((p for p in context.roots if p.is_relative_to(root / '.f3/tmp')), root / '.f3/tmp')
    temporary.mkdir(parents=True, exist_ok=True)
    os.environ['TEMP'] = os.environ['TMP'] = str(temporary)
    tempfile.tempdir = str(temporary)
    return context


def _report_name(context):
    return 'pytest-' + context.process_id + '.json' if context.role == 'command' else 'pytest.json'


class ReportPlugin:
    """Small pytest adapter: collection, node-scoped native faults, outward verdict."""
    def __init__(self, context):
        self.context = context
        self.collected = []
        self.reports = []
        self.before = {}

    def pytest_collection_modifyitems(self, items):
        import pytest
        for item in items:
            legacy = item.nodeid.startswith('main_tests.py::')
            item.add_marker(pytest.mark.live_read if legacy else pytest.mark.offline)
        self.collected = [item.nodeid for item in items]

    def pytest_runtest_setup(self, item):
        guard_module().set_node(item.nodeid)
        self.before = dict(self.context.counts)

    def pytest_runtest_logreport(self, report):
        self.reports.append({'test': report.nodeid, 'phase': report.when,
                             'outcome': report.outcome})
        if report.when != 'teardown':
            return
        native_controls = {
            'tests/environment/test_environment.py::test_offline_guards': {'network': 4, 'children': 9},
            'tests/environment/test_environment.py::test_cli_and_report_normal_first_and_repeat':
                {'network': 8, 'children': 18},
            'tests/logging/test_failures.py::test_guards_reject_live_actions':
                {'network': 1, 'secrets': 1, 'writers': 2, 'children': 1},
        }
        expected = native_controls.get(report.nodeid)
        if expected is not None:
            delta = {k: v - self.before.get(k, 0) for k, v in self.context.counts.items()
                     if v != self.before.get(k, 0)}
            partial_cli = (report.nodeid == 'tests/environment/test_environment.py::test_cli_and_report_normal_first_and_repeat'
                           and delta == {'network': 4, 'children': 9})
            if (delta == expected or partial_cli) and report.outcome == 'passed':
                total = dict(self.context.expected_counts)
                for kind, count in delta.items():
                    total[kind] = total.get(kind, 0) + count
                guard_module().set_expected_counts(total)
        guard_module().set_node('')

    def pytest_sessionfinish(self, session, exitstatus):
        guard = guard_module()
        result = dict(guard.report_metadata(), exit_code=int(exitstatus),
                      collected=self.collected, reports=self.reports)
        # Native conftests keep their original substantive report separately.
        name = _report_name(self.context) if self.context.lane == 'f3' else 'collection.json'
        guard.exclusive_json(self.context.report_dir / name, result)
        issues = guard.validate_reports(self.context)
        actual = {k: v for k, v in self.context.counts.items() if v}
        expected = {k: v for k, v in self.context.expected_counts.items() if v}
        if actual != expected:
            issues = sorted(set([*issues, 'UNEXPECTED_PARENT_COUNTERS']))
        if issues and int(exitstatus) == 0:
            session.exitstatus = 86
        guard.finish(int(session.exitstatus))
        if issues:
            sys.stderr.write('OFFLINE_RECONCILIATION: ' + ','.join(issues) + '\n')


def configure_shared(config):
    """Called before collection by actual pytest conftest, including console pytest."""
    import pytest
    context = bootstrap_entry()
    expected_mode = 'collect' if config.option.collectonly else 'run'
    if context.mode != expected_mode and context.mode != 'legacy':
        raise pytest.UsageError('Collection mode must match the run-bound bootstrap')
    if os.environ.get('PYTEST_DISABLE_PLUGIN_AUTOLOAD') != '1' or os.environ.get('PYTEST_ADDOPTS') or os.environ.get('PYTEST_PLUGINS'):
        raise pytest.UsageError('Use the prepared immutable E; disable inherited pytest plugins/options')
    allowed = [ROOT / 'tests/shared', ROOT / 'tests.py', ROOT / 'main_tests.py']
    if context.role == 'command':
        allowed.extend(Path(p) for p in context.roots)
        allowed.extend(Path(p).absolute() for p in context.ticket['argv']
                       if p.endswith('.py') and Path(p).absolute().is_relative_to(ROOT / '.f3/tmp'))
    for argument in config.args:
        requested = Path(argument.split('::', 1)[0])
        if '..' in requested.parts:
            raise pytest.UsageError('Test targets must not traverse parent directories')
        target = requested.absolute()
        a_root = ROOT / 'tests/A'
        if target.is_relative_to(a_root):
            try:
                target = guard_module().checked_path(target, a_root)
            except (OSError, ValueError):
                raise pytest.UsageError('Unsafe A test target') from None
            if target.is_dir() or (target.is_file() and target.name.startswith('test_')
                                   and target.suffix == '.py'):
                continue
            raise pytest.UsageError('A targets require directories or test_*.py files')
        if not any(target == path or (path.is_dir() and target.is_relative_to(path)) for path in allowed):
            raise pytest.UsageError('Only shared or explicit legacy targets; native suites need their canonical lane')
    temporary = ROOT / '.f3/tmp' / ('pytest-' + context.run_id + '-' + context.process_id[:8])
    if context.role == 'command':
        issued = next((p for p in context.roots if p.parent == ROOT / '.f3/tmp' and p.name.startswith('pytest-')), None)
        if issued is not None:
            temporary = issued / context.process_id
    cache = ROOT / '.f3/cache/runs' / context.run_id / 'pytest'
    guard_module().checked_path(temporary, ROOT / '.f3/tmp')
    guard_module().checked_path(cache, ROOT / '.f3/cache')
    config.option.basetemp = str(temporary)
    config.inicfg['cache_dir'] = str(cache)
    if not getattr(config, '_mds_offline_plugin', False):
        config.pluginmanager.register(ReportPlugin(context), 'mds-offline-report')
        config._mds_offline_plugin = True


def _native_bootstrap(lane):
    path = ROOT / LANE_TARGETS[lane]
    name = '_safety' if lane == 'f1' else '_probe'
    module = _module(name, path / (name + '.py'))
    if lane == 'f2':
        module.install_guards(ROOT, 'harness')
    else:
        module.install_guards()
    return guard_module().ensure_context(ROOT, lane=lane, install=False)


def _execute_lane(lane, collect_only=False, legacy=False):
    guard = guard_module()
    context = _native_bootstrap(lane) if lane != 'f3' else bootstrap_entry()
    if context.ticket is None or context.role != 'lane-worker' or context.lane != lane:
        raise ValueError('Internal lane entry requires its current one-use ticket')
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import pytest
    prefix = guard.LANES[lane]
    basetemp = ROOT / prefix / 'tmp' / ('pytest-dev' if lane == 'f1' else 'pytest-' + context.run_id)
    cache = ROOT / prefix / 'cache/runs' / context.run_id / 'pytest'
    arguments = ['-c', str(ROOT / 'pyproject.toml'), '--confcutdir', str(ROOT / LANE_TARGETS[lane]),
                 str(ROOT / LANE_TARGETS[lane]), '-q', '--tb=short', '--basetemp', str(basetemp),
                 '-o', 'cache_dir=' + str(cache)]
    plugin = ReportPlugin(context)
    if lane == 'f3':
        # The root conftest is intentionally included for the actual F3 boundary.
        arguments = ['-c', str(ROOT / 'pyproject.toml'), '-q']
        if legacy:
            arguments.extend([str(ROOT / 'tests.py'), str(ROOT / 'main_tests.py')])
    if collect_only or legacy:
        arguments.append('--collect-only')
    result = int(pytest.main(arguments, plugins=[] if lane == 'f3' else [plugin]))
    guard.finish(result)
    issues = guard.validate_reports(context)
    return 86 if issues else result


COMMANDS = {
    'console-pytest': ('console', ['-q']), 'module-pytest': ('pytest', ['-q']),
    'unittest-discover': ('unittest', ['discover']),
    'legacy-pytest': ('console', ['-q', 'tests.py', 'main_tests.py']),
    'legacy-module-pytest': ('pytest', ['-q', 'tests.py', 'main_tests.py']),
    'legacy-collect': ('console', ['--collect-only', '-q', 'tests.py', 'main_tests.py']),
    'legacy-module-collect': ('pytest', ['--collect-only', '-q', 'tests.py', 'main_tests.py']),
    'legacy-unittest': ('unittest', ['main_tests']),
    'empty-tests-pytest': ('console', ['-q', 'tests.py']),
    'empty-tests-module': ('pytest', ['-q', 'tests.py']),
    'empty-tests-collect': ('console', ['--collect-only', '-q', 'tests.py']),
    'empty-tests-module-collect': ('pytest', ['--collect-only', '-q', 'tests.py']),
    'main-only-pytest': ('console', ['-q', 'main_tests.py']),
    'main-only-module': ('pytest', ['-q', 'main_tests.py']),
    'main-collect': ('console', ['--collect-only', '-q', 'main_tests.py']),
    'main-module-collect': ('pytest', ['--collect-only', '-q', 'main_tests.py']),
    'marker-live': ('pytest', ['-q', 'main_tests.py', '-m', 'live_read']),
    'env-live': ('pytest', ['-q', 'main_tests.py']),
    'diagnostic-live-read': ('script', ['main_tests.py', '--allow-live-diagnostics', '--profile', 'live-read']),
    'diagnostic-staging': ('script', ['tests.py', '--allow-live-diagnostics', '--profile', 'staging', '--output-root']),
}


def run_command_check(profile, arguments=()):
    """Actual prepared commands; this API accepts names, never arbitrary argv."""
    if arguments or profile not in COMMANDS and profile not in {
            'unknown-marker', 'collection-network', 'collection-sleep', 'collection-write',
            'collection-secret', 'collection-data'}:
        raise ValueError('Unknown offline command profile or forwarded arguments')
    guard = guard_module()
    context = bootstrap_entry()
    environment = prepared_environment(context, command=True)
    roots = None
    if profile.startswith('collection-') or profile == 'unknown-marker':
        fixture = ROOT / '.f3/tmp' / ('command-' + uuid.uuid4().hex)
        fixture.mkdir(parents=True)
        permitted = fixture / 'owned'
        permitted.mkdir()
        external = fixture / 'external'
        external.mkdir()
        (external / 'datasets').mkdir()
        canary = external / 'datasets/canary.csv'
        canary.write_bytes(b'F3_SYNTHETIC_DATA_CANARY\n')
        secret = permitted / '.env'
        payload = permitted / 'payload'
        payload.write_bytes(b'F3_SYNTHETIC_SECRET_DO_NOT_PRINT')
        payload.rename(secret)
        # Test-only early bootstrap for a disposable import fault, never source patches.
        (fixture / 'conftest.py').write_text(
            'import importlib.util,sys\nfrom pathlib import Path\n'
            f's=importlib.util.spec_from_file_location("mds_offline_tests", {str(ROOT / "tools/offline_tests.py")!r})\n'
            'm=importlib.util.module_from_spec(s);sys.modules[s.name]=m;s.loader.exec_module(m)\n'
            'm.bootstrap_entry()\n'
            'import pytest\n@pytest.hookimpl(tryfirst=True)\n'
            'def pytest_configure(config): m.configure_shared(config)\n', encoding='utf-8')
        operations = {
            'collection-network': 'import socket\nsocket.getaddrinfo("F3_COLLECTION.invalid",443)',
            'collection-sleep': ('import time,mds_offline_guard\n'
                'mds_offline_guard._ORIGINAL_SLEEP=lambda seconds: print("F3_SLEEP_LEAF_CALLED")\n'
                'time.sleep(60)'),
            'collection-write': f'from pathlib import Path\nPath({str(external / "canary")!r}).write_bytes(b"BAD")',
            'collection-secret': f'from pathlib import Path\nPath({str(secret)!r}).read_bytes()',
            'collection-data': f'from pathlib import Path\nPath({str(canary)!r}).read_bytes()',
            'unknown-marker': 'import pytest\n@pytest.mark.F3_UNKNOWN_MARKER\ndef test_unknown(): pass',
        }
        (fixture / 'test_import_fault.py').write_text(operations[profile] +
            '\n\ndef test_body():\n    print("F3_COLLECTION_BODY_CALLED")\n', encoding='utf-8')
        kind, args = 'pytest', ['--collect-only', '-q', str(fixture / 'test_import_fault.py')]
        roots = [permitted, context.report_dir, ROOT / '.f3/cache/runs' / context.run_id,
                 ROOT / '.f3/tmp' / ('pytest-' + context.run_id)]
    else:
        kind, args = COMMANDS[profile]
        args = list(args)
    if kind == 'console':
        command = [str(Path(sys.executable).with_name('pytest.exe' if sys.platform == 'win32' else 'pytest')), *args]
    elif kind == 'script':
        if profile == 'diagnostic-staging':
            args.append(str(ROOT / '.f3/tmp' / ('staging-' + uuid.uuid4().hex)))
        command = [sys.executable, '-B', '-X', 'utf8', *args]
    else:
        command = [sys.executable, '-B', '-X', 'utf8', '-m', kind, *args]
    if profile == 'env-live':
        environment['MDS_ALLOW_LIVE_DIAGNOSTICS'] = '1'
        environment['MDS_DIAGNOSTIC_PROFILE'] = 'live-read'
    expected_exit = 5 if profile.startswith('empty-tests') else 2 if profile.startswith('diagnostic-') or profile.startswith('collection-') or profile == 'unknown-marker' else 0
    ticket = guard.issue_ticket('command', command, roots=roots, expected_exit=expected_exit,
                                mode='collect' if '--collect-only' in args else 'run')
    if profile.startswith('collection-'):
        node = getattr(context, 'nodeid', '')
        if not node.startswith('tests/shared/test_collection.py::'):
            raise ValueError('Collection controls require their named regression')
        guard.exclusive_json(context.report_dir / 'tickets' / (ticket['ticket_id'] + '.command-control.json'),
            dict(ticket_id=ticket['ticket_id'], run_id=context.run_id, nodeid=node, profile=profile,
                 expected_kind={'secret': 'secrets', 'data': 'dataset_reads', 'write': 'writers'}.get(profile[11:], profile[11:])))
    result = guard.run_child(command, role='command', ticket=ticket,
                             timeout=30 if profile.startswith('collection-') else 180,
                             cwd=ROOT, env=environment)
    destination = context.report_dir / ('pytest-' + ticket['ticket_id'] + '.json')
    report = {'collected': [], 'reports': []}
    errors = list(result.report_errors)
    needs_pytest_report = kind != 'script' and profile != 'legacy-unittest'
    if destination.exists():
        try:
            guard.checked_path(destination, context.report_dir)
            report = json.loads(destination.read_text(encoding='utf-8'))
            keys = ('run_id', 'lane', 'mode', 'tested_sha', 'source_digest', 'role', 'ticket_id')
            expected = {**ticket, 'ticket_id': ticket['ticket_id']}
            if any(report.get(key) != expected[key] for key in keys):
                errors.append('FOREIGN_PYTEST_REPORT')
            if not isinstance(report.get('collected'), list) or not isinstance(report.get('reports'), list):
                errors.append('CORRUPT_PYTEST_REPORT')
        except (OSError, ValueError, TypeError):
            errors.append('CORRUPT_PYTEST_REPORT')
    elif needs_pytest_report:
        errors.append('MISSING_PYTEST_REPORT')
    return dict(exit_code=86 if errors else result.returncode, actual_exit=result.actual_exit,
                stdout=result.stdout.decode('utf-8', errors='replace'),
                stderr=result.stderr.decode('utf-8', errors='replace'),
                report=report, guard_report=result.report, report_errors=errors)


def _notebook_check(sha):
    guard = guard_module()
    context = guard.ensure_context(ROOT, lane='f3', role='notebook-check', install=False)
    if sha != context.tested_sha or context.source_mode != 'commit':
        raise ValueError('Notebook check requires the current clean exact commit')
    _native_bootstrap('f2')
    tool = _module('mds_notebook_check', ROOT / 'tools/notebook_sync.py')
    if set(tool.PAIRS) != set(PAIRS) or len(tool.PAIRS) != len(PAIRS):
        raise ValueError('Exact notebook pair inventory mismatch')
    for name in PAIRS:
        for suffix in ('.py', '.ipynb'):
            guard.checked_path(ROOT / (name + suffix), ROOT)
            if not (ROOT / (name + suffix)).is_file():
                raise ValueError('Missing notebook member')
    result = int(tool.main(['check', '--ref', sha]) or 0)
    guard.exclusive_json(context.report_dir / 'notebook.json', dict(guard.report_metadata(),
                         pairs=list(PAIRS), members=20, exit_code=result))
    guard.finish(result)
    return 86 if guard.validate_reports(context) else result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument('--lane', choices=LANE_TARGETS)
    selection.add_argument('--notebook-check', action='store_true')
    selection.add_argument('--execute-lane', choices=LANE_TARGETS, help=argparse.SUPPRESS)
    parser.add_argument('--ticket', help=argparse.SUPPRESS)
    parser.add_argument('--collect-only', action='store_true')
    parser.add_argument('--legacy-collection', action='store_true')
    parser.add_argument('--ref')
    args = parser.parse_args(argv)
    lane = args.lane or args.execute_lane
    if args.legacy_collection and (lane != 'f3' or args.collect_only):
        parser.error('--legacy-collection is f3-only and already collection-only')
    if args.notebook_check:
        if not args.ref or not re.fullmatch('[0-9a-f]{40}', args.ref) or args.collect_only or args.legacy_collection:
            parser.error('--notebook-check needs --ref full current SHA only')
        return _notebook_check(args.ref)
    if args.ref:
        parser.error('--ref belongs to notebook check only')
    if args.execute_lane:
        if not args.ticket or args.ticket != os.environ.get('MDS_OFFLINE_TICKET'):
            parser.error('Internal worker ticket mismatch')
        return _execute_lane(lane, args.collect_only, args.legacy_collection)
    if args.ticket:
        parser.error('Tickets are internal-only')
    guard = guard_module()
    context = guard.ensure_context(ROOT, lane=lane, role='supervisor',
        mode='legacy' if args.legacy_collection else 'collect' if args.collect_only else 'run', install=False)
    guard.mark_guard_ready()
    command = [sys.executable, '-I', '-B', '-X', 'utf8', str(ROOT / 'tools/offline_tests.py'), '--execute-lane', lane]
    if args.collect_only:
        command.append('--collect-only')
    if args.legacy_collection:
        command.append('--legacy-collection')
    # The issued command records the full exact argv, including the ticket path.
    # Environment carries the current ticket; the exact workload argv stays fixed.
    ticket = guard.issue_ticket('lane-worker', command, expected_exit=None)
    result = guard.run_child(ticket['argv'], role='lane-worker', ticket=ticket,
        timeout=900, env={**prepared_environment(context), 'MDS_OFFLINE_INTERNAL_TICKET': ticket['path']})
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    guard.finish(result.returncode)
    issues = guard.validate_reports(context)
    outward = 86 if issues or result.report_errors else result.returncode
    guard.exclusive_json(context.report_dir / 'run.json', dict(guard.report_metadata(),
        actual_exit=result.actual_exit, exit_code=outward, errors=issues,
        collected_only=args.collect_only or args.legacy_collection))
    print('OFFLINE_RESULT ' + json.dumps({'lane': lane, 'mode': context.mode,
          'exit_code': outward, 'report_root': str(context.report_dir)}, sort_keys=True))
    return outward


if __name__ == '__main__':
    # A lane ticket is passed by environment too; transport does not change argv.
    incoming = sys.argv[1:]
    if '--execute-lane' in incoming and '--ticket' not in incoming:
        incoming += ['--ticket', os.environ.get('MDS_OFFLINE_TICKET', '')]
    try:
        sys.exit(main(incoming))
    except (ValueError, OSError) as error:
        guard = guard_module()
        context = guard._CONTEXT
        if context is None:
            boundary = guard.checked_path(ROOT / '.f3/evidence/preflight', ROOT / '.f3')
            destination = boundary / (uuid.uuid4().hex + '.json')
            metadata = {'version': 1, 'phase': 'preflight'}
        else:
            destination = context.report_dir / ('failure-' + context.process_id + '.json')
            metadata = guard.report_metadata()
        guard.exclusive_json(destination, dict(metadata, exit_code=2, error_type=type(error).__name__))
        guard.finish(2)
        sys.stderr.write('OFFLINE_PREFLIGHT_REJECTED ' + type(error).__name__ + '\n')
        sys.exit(2)
