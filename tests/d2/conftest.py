"""Closed Linux D2 fixtures. Collection is not native process authority."""

import ast
import hashlib
import importlib
import importlib.util
import json
import os
import stat
from pathlib import Path
import sys
import types
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
NATIVE_RESULTS = []


def pytest_configure(config):
    config.addinivalue_line('markers', 'native: admitted Linux OS lifecycle acceptance')


def load_source(path, name):
    specification = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


@pytest.fixture
def repo():
    return ROOT


@pytest.fixture
def oracle():
    return json.loads((ROOT / 'tests/fixtures/d2/expected.json').read_bytes())


@pytest.fixture
def upload(monkeypatch):
    # upload is required to have an inert stdlib-only import. main is never
    # imported here: its orchestration unit tests use exact AST definitions.
    module = load_source(ROOT / 'upload.py', 'd2_test_upload')
    monkeypatch.setitem(sys.modules, 'upload', module)
    return module


@pytest.fixture
def guard():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    # Preserve the process-local one-use runner admission. A second module
    # instance would have no authority and must not imitate that admission.
    return importlib.import_module('tools.offline_guard_linux')


@pytest.fixture
def fixture_worker():
    return load_source(ROOT / 'tests/d2/d2_worker.py', 'd2_fixture_definitions')


@pytest.fixture
def helper():
    # Reuse the accepted portable definition loader. It removes only the three
    # known permanent audit-installation statements, never grants process or
    # network authority, and does not execute a consumer or collect its tests.
    foundation = load_source(ROOT / 'tests/linux_bootstrap/conftest.py', 'd2_foundation_definitions')
    return foundation.load_definitions(ROOT / '.github/lnx02/verify_env.py', 'd2_portable_helper')


@pytest.fixture
def dispose_fault_leaf():
    """Explicit disposal only; never an automatic test/native finalizer."""
    def dispose(path, identity, *, scope, target=None, target_identity=None, target_bytes=None):
        path, scope = Path(path), Path(scope)
        assert path.parent == scope and scope.resolve(strict=True) == scope.absolute()
        current = path.lstat()
        assert (current.st_dev, current.st_ino, current.st_mode, current.st_rdev) == (
            identity.st_dev, identity.st_ino, identity.st_mode, identity.st_rdev)
        if stat.S_ISDIR(current.st_mode):
            path.rmdir()
        else:
            path.unlink()
        assert not os.path.lexists(path)
        if target is not None:
            after = Path(target).lstat()
            assert stat.S_ISREG(after.st_mode)
            assert (after.st_dev, after.st_ino) == (target_identity.st_dev, target_identity.st_ino)
            assert after.st_nlink == target_identity.st_nlink
            assert Path(target).read_bytes() == target_bytes
    return dispose


@pytest.fixture
def source(tmp_path, oracle, fixture_worker, upload, monkeypatch):
    fixture = fixture_worker.make_source_fixture(tmp_path, oracle)
    monkeypatch.setattr(upload, 'datasets_path', str(fixture['datasets']) + '/')
    return fixture


@pytest.fixture
def native_results():
    return NATIVE_RESULTS


@pytest.fixture
def native_scratch(request):
    assert sys.platform == 'linux', 'D2 Linux native acceptance is NOT VERIFIED on this platform'
    configured = os.environ.get('MDS_D2_NATIVE_ROOT')
    assert configured, 'D2 native fixture root was not admitted'
    parent = Path(configured)
    assert parent.is_absolute() and parent.resolve() == parent and parent.is_dir()
    root = parent / ('case-' + hashlib.sha256(request.node.nodeid.encode()).hexdigest())
    root.mkdir(mode=0o700)
    # The outer controller owns deletion only after independently observed
    # settlement. A pytest finalizer must not repair a failed native oracle.
    return root


@pytest.fixture
def tmp_path(request, tmp_path_factory):
    # Avoid pytest's optional current symlink in the closed fixture tree.
    name = 'd2-' + hashlib.sha256(request.node.nodeid.encode()).hexdigest()
    return tmp_path_factory.mktemp(name, numbered=False)


def main_definitions(path):
    source = path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=str(path))
    permitted = (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.ClassDef)
    tree.body = [node for node in tree.body if isinstance(node, permitted)
                 or (isinstance(node, ast.Assign) and any(
                     isinstance(target, ast.Name) and target.id == 'D2_SOURCE_PROFILE'
                     for target in node.targets))]
    module = types.ModuleType('d2_main_unit')
    module.__file__ = str(path)
    exec(compile(tree, str(path), 'exec'), module.__dict__)
    return module


@pytest.fixture
def pipeline(repo):
    return main_definitions(repo / 'main.py')
