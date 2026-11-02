"""D2 bootstrap runs before upload, logger, backend mocks and collection."""

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if 'mds_offline_guard' not in sys.modules:
    specification = importlib.util.spec_from_file_location('mds_offline_guard', ROOT / 'tools/offline_guard.py')
    guard = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = guard
    specification.loader.exec_module(guard)
else:
    guard = sys.modules['mds_offline_guard']
CONTEXT = guard.ensure_context(ROOT, lane='d2', install=True)
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

import json
import uuid
import pytest


@pytest.fixture
def project_root():
    return ROOT


@pytest.fixture
def offline_guard():
    return guard


@pytest.fixture(name='guard')
def guard_fixture():
    return guard


@pytest.fixture
def oracle():
    return json.loads((ROOT / 'tests/fixtures/d2/expected.json').read_text(encoding='utf-8'))


@pytest.fixture
def upload():
    import upload
    return upload


@pytest.fixture
def log_factory():
    import run_logging
    sessions = []
    def configure(config=None, sensitive_values=()):
        identifier = 'd2-unit-' + uuid.uuid4().hex
        settings = dict(config or {})
        settings.setdefault('log_root', '.f-log/d2/' + CONTEXT.run_id + '/unit/' + identifier + '/logs')
        session = run_logging.configure_logging(ROOT, run_id=identifier, environment='offline',
            producer_sha=CONTEXT.tested_sha, config=settings, sensitive_values=sensitive_values)
        sessions.append(session)
        return session
    yield configure
    for session in reversed(sessions):
        try:
            run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
        except (RuntimeError, ValueError, OSError):
            pass


@pytest.fixture
def logging_session(log_factory):
    return log_factory()
