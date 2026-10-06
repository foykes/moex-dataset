"""Guard collection as well as tests; never collect live legacy scripts."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _probe import ROOT, install_guards, safe_tree

GUARDS = install_guards()

import json
import os
import pytest

REPORTS = []


def pytest_configure(config):
    boundary = ROOT / '.f-log'
    for path in (config.option.basetemp, config.getini('cache_dir')):
        if not path or not Path(path).absolute().is_relative_to(boundary):
            raise pytest.UsageError('F-LOG temp/cache must stay under .f-log')
        try:
            safe_tree(path)
        except ValueError:
            raise pytest.UsageError('Reject aliases before F-LOG pytest cleanup/cache writes') from None
    for argument in config.args:
        if not Path(argument.split('::')[0]).absolute().is_relative_to(ROOT / 'tests/logging'):
            raise pytest.UsageError('Collect only tests/logging')
    if os.environ.get('PYTEST_DISABLE_PLUGIN_AUTOLOAD') != '1' or os.environ.get('PYTEST_ADDOPTS') or os.environ.get('PYTEST_PLUGINS'):
        raise pytest.UsageError('Disable inherited pytest plugins/options')


@pytest.fixture
def log(monkeypatch, tmp_path):
    import run_logging
    original = run_logging.configure_logging
    def configure(root, **kwargs):
        config = dict(kwargs.get('config') or {})
        config.setdefault('log_root', (tmp_path.relative_to(ROOT) / 'logs').as_posix())
        kwargs['config'] = config
        return original(root, **kwargs)
    monkeypatch.setattr(run_logging, 'configure_logging', configure)
    return run_logging


@pytest.fixture
def session(log, request):
    context = log.configure_logging(ROOT, run_id='test-' + __import__('uuid').uuid4().hex,
                                    environment='offline', producer_sha='c4f2210',
                                    config={'console_level': 'ERROR'})
    yield context
    log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)


def pytest_runtest_logreport(report):
    REPORTS.append({'test': report.nodeid, 'phase': report.when, 'outcome': report.outcome,
                    'seconds': round(report.duration, 6), 'properties': dict(report.user_properties)})


def pytest_sessionfinish(session, exitstatus):
    directory = ROOT / '.f-log/evidence'
    safe_tree(directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'pytest.json').write_text(json.dumps({'exit_code': int(exitstatus),
        'platform': sys.platform, 'guards': GUARDS, 'reports': REPORTS}, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
