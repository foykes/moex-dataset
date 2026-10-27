"""Install the fixed offline guard before helper, logger or backend collection."""

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if 'mds_offline_guard' not in sys.modules:
    specification = importlib.util.spec_from_file_location(
        'mds_offline_guard', ROOT / 'tools/offline_guard.py')
    guard = importlib.util.module_from_spec(specification)
    sys.modules['mds_offline_guard'] = guard
    specification.loader.exec_module(guard)
else:
    guard = sys.modules['mds_offline_guard']
CONTEXT = guard.ensure_context(ROOT, lane='d1', install=True)

import pytest
import csv
import hashlib
import io
import json
import uuid


@pytest.fixture
def project_root():
    return ROOT


@pytest.fixture
def offline_guard():
    return guard


@pytest.fixture
def oracle():
    result = json.loads((ROOT / 'tests/fixtures/dataset_io/expected.json').read_text(encoding='utf-8'))
    previous = (ROOT / 'tests/fixtures/dataset_io/previous.csv').read_bytes()
    checkout = {'bytes': len(previous), 'sha256': hashlib.sha256(previous).hexdigest()}
    assert checkout in result['previous_checkout_variants']
    canonical = result['previous']['text'].encode('utf-8')
    assert previous in (canonical, canonical.replace(b'\n', b'\r\n'))
    # Only this enumerated test fixture is canonicalized, never helper input.
    result['previous']['data'] = canonical
    for key in ('replacement', 'two_updates'):
        result[key]['data'] = result[key]['text'].encode('utf-8')
        assert len(result[key]['data']) == result[key]['bytes']
        assert hashlib.sha256(result[key]['data']).hexdigest() == result[key]['sha256']
    return result


@pytest.fixture
def assert_artifact(oracle):
    def check(path, kind):
        data = path.read_bytes()
        expected = oracle[kind]
        assert data == expected['data']
        assert len(data) == expected['bytes']
        assert hashlib.sha256(data).hexdigest() == expected['sha256']
        rows = list(csv.reader(io.StringIO(data.decode('utf-8'), newline='')))
        assert rows[0] == oracle['schema']
        assert rows[1:] == expected['rows']
        assert [row[:2] for row in rows[1:]] == expected['keys']
    return check


@pytest.fixture
def helper():
    import dataset_io
    return dataset_io


@pytest.fixture
def target(tmp_path, oracle):
    path = tmp_path / 'current.csv'
    path.write_bytes(oracle['previous']['data'])
    return path


@pytest.fixture
def log_factory():
    import run_logging
    sessions = []
    def configure(config=None, sensitive_values=()):
        identifier = 'unit-' + uuid.uuid4().hex
        settings = dict(config or {})
        settings.setdefault('log_root', '.f-log/d1/' + CONTEXT.run_id + '/unit/' + identifier + '/logs')
        context = run_logging.configure_logging(ROOT, run_id=identifier, environment='offline',
            producer_sha=CONTEXT.tested_sha, config=settings, sensitive_values=sensitive_values)
        sessions.append(context)
        return context
    yield configure
    for context in reversed(sessions):
        try:
            run_logging.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
        except (RuntimeError, ValueError, OSError):
            pass


@pytest.fixture
def logging_session(log_factory):
    return log_factory()


@pytest.fixture
def acquire(helper, target, logging_session):
    def lock(*, path=None, root=None, validate=None, session=None, receipt=None, timeout_s=0):
        return helper.resource_lock(path or target.name, resource_root=root or target.parent,
            validate_target=validate if validate is not None else (lambda stream: stream is not None),
            logging_session=session if session is not None else logging_session,
            receipt=receipt if receipt is not None else {}, timeout_s=timeout_s)
    return lock
