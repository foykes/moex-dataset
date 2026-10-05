"""F2-only collection and fixture roots, independently of F1's no-child guard."""

from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _probe import install_guards, safe_tree

ROOT = Path(__file__).resolve().parents[2]
install_guards(ROOT, 'harness')

import datetime
import json
import os
import re

_reports = []


def pytest_configure(config):
    import pytest
    try:
        boundary = ROOT / '.f2'
        basetemp = safe_tree(config.option.basetemp or '', boundary)
        if basetemp.parent != boundary / 'tmp' or not re.fullmatch(r'pytest(?:-[a-zA-Z0-9-]+)?', basetemp.name):
            raise ValueError('Pass a dedicated .f2/tmp/pytest[-run] basetemp')
        cache = safe_tree(config.getini('cache_dir'), boundary)
        if not cache.is_relative_to(boundary / 'cache'):
            raise ValueError('F2 pytest cache must stay in .f2/cache')
        safe_tree(boundary / 'evidence', boundary)
        for argument in config.args:
            target = Path(argument.split('::', 1)[0]).resolve()
            if not target.is_relative_to(ROOT / 'tests/f2'):
                raise ValueError('Collect only tests/f2')
        if os.environ.get('PYTEST_DISABLE_PLUGIN_AUTOLOAD') != '1' or os.environ.get('PYTEST_ADDOPTS') or os.environ.get('PYTEST_PLUGINS'):
            raise ValueError('Disable third-party pytest autoload and inherited options')
        # These children inherit only the task-specific cache/temp profile.
        os.environ['F2_TASK_ROOT'] = str(ROOT)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error


def pytest_runtest_logreport(report):
    item = {'test': report.nodeid, 'phase': report.when, 'outcome': report.outcome,
            'seconds': round(report.duration, 6)}
    if report.user_properties:
        item['properties'] = dict(report.user_properties)
    _reports.append(item)


def pytest_sessionfinish(session, exitstatus):
    boundary = ROOT / '.f2'
    safe_tree(boundary / 'evidence', boundary)
    destination = boundary / 'evidence/pytest.json'
    if destination.exists() and (destination.is_symlink() or destination.stat().st_nlink != 1):
        raise ValueError('Unsafe F2 evidence destination')
    destination.parent.mkdir(parents=True, exist_ok=True)
    result = {'finished_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'exit_code': int(exitstatus), 'network': 'forbidden',
              'profile': 'Windows offline F2, controlled fixture children', 'reports': _reports}
    destination.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
