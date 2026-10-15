"""Closed LNX-03 fixtures; importing these tests grants no native authority."""

import ast
import copy
import hashlib
import json
from pathlib import Path
import types

import pytest


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / '.github/lnx02/verify_env.py'
NATIVE_RESULTS = []


def pytest_configure(config):
    config.addinivalue_line('markers', 'native: requires admitted Linux native lifecycle evidence')


@pytest.fixture
def native_results():
    return NATIVE_RESULTS


@pytest.fixture
def tmp_path(request, tmp_path_factory):
    # pytest's default numbered=True path also creates a best-effort `current`
    # symlink. The closed consumer permits no aliases in its owned scratch.
    # Node IDs are unique within this fixed suite; an existing leaf must fail.
    name = 'case-' + hashlib.sha256(request.node.nodeid.encode('utf-8')).hexdigest()
    return tmp_path_factory.mktemp(name, numbered=False)


def load_definitions(path, name):
    """Keep tested definitions exact without installing a permanent audit hook.

    Native entrypoints must execute the real source in a separate process. This
    loader is solely for portable parsers and refusals, never sandbox evidence.
    """
    source = path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=str(path))
    forbidden = {
        'sys.addaudithook(GUARD)',
        'GUARD.registered = True',
        'socket.has_ipv6 = False',
    }
    tree.body = [node for node in tree.body
                 if ast.get_source_segment(source, node) not in forbidden]
    module = types.ModuleType(name)
    module.__file__ = str(path)
    # Do not select the module's real __main__ path or register a plugin.
    exec(compile(tree, str(path), 'exec'), module.__dict__)
    return module


@pytest.fixture
def helper():
    return load_definitions(HELPER, 'lnx03_portable_helper')


@pytest.fixture
def runner():
    return load_definitions(ROOT / 'tools/offline_linux.py', 'lnx03_portable_runner')


@pytest.fixture
def process_policy():
    # Instantiating the callable is a policy unit test, not sys.addaudithook,
    # native admission, an OS process birth, or Linux lifecycle evidence.
    return load_definitions(ROOT / 'tools/offline_guard_linux.py', 'lnx03_portable_process_policy')


@pytest.fixture
def repo():
    return ROOT


@pytest.fixture
def legacy_environment():
    sha, base = 'a' * 40, 'b' * 40
    values = {
        'MDS_LNX02_SETUP_APPROVED': 'true',
        'MDS_LNX02_APPROVED_SHA': sha,
        'MDS_LNX02_APPROVED_BASE': base,
        'MDS_LNX02_OPERATION': 'verify',
        'GITHUB_REPOSITORY': 'foykes/moex-dataset',
        'GITHUB_ACTOR': 'foykes',
        'GITHUB_TRIGGERING_ACTOR': 'foykes',
        'GITHUB_SHA': sha,
        'GITHUB_REF': 'refs/heads/codex/lnx-02',
        'GITHUB_EVENT_NAME': 'push',
        'TASK_RUNNER_ENVIRONMENT': 'github-hosted',
        'TASK_DELETED': 'false',
    }
    for task, github in {
        'TASK_EVENT': 'GITHUB_EVENT_NAME', 'TASK_EVENT_SHA': 'GITHUB_SHA',
        'TASK_EVENT_REF': 'GITHUB_REF', 'TASK_REPOSITORY': 'GITHUB_REPOSITORY',
        'TASK_ACTOR': 'GITHUB_ACTOR', 'TASK_TRIGGERING_ACTOR': 'GITHUB_TRIGGERING_ACTOR',
    }.items():
        values[task] = values[github]
    return values, sha, base


@pytest.fixture
def copied():
    return copy.deepcopy


@pytest.fixture
def admission_environment():
    sha, base = 'a' * 40, 'b' * 40
    grant = {
        'approved': True, 'consumer': 'linux-bootstrap',
        'repository': 'foykes/moex-dataset', 'sha': sha, 'base': base,
        'event': 'push', 'ref': 'refs/heads/codex/lnx-03-d2-bootstrap',
        'actor': 'foykes', 'triggering_actor': 'foykes',
        'workflow': '.github/workflows/d2-linux-offline.yml',
        'job': 'linux-bootstrap', 'run_number': 31, 'run_id': '731009', 'attempt': 2,
        'ticket_id': 'c' * 64,
    }
    values = {
        'GITHUB_REPOSITORY': grant['repository'], 'GITHUB_SHA': sha,
        'GITHUB_EVENT_NAME': grant['event'], 'GITHUB_REF': grant['ref'],
        'GITHUB_ACTOR': grant['actor'], 'GITHUB_TRIGGERING_ACTOR': grant['triggering_actor'],
        'GITHUB_RUN_ID': '731009', 'GITHUB_RUN_NUMBER': '31',
        'GITHUB_RUN_ATTEMPT': '2', 'GITHUB_JOB': grant['job'],
        'GITHUB_WORKFLOW_REF': 'foykes/moex-dataset/' + grant['workflow'] + '@' + grant['ref'],
        'GITHUB_WORKFLOW_SHA': sha,
        'RUNNER_ENVIRONMENT': 'github-hosted', 'TASK_RUNNER_ENVIRONMENT': 'github-hosted',
        'TASK_DELETED': 'false', 'GITHUB_ACTIONS': 'true', 'ImageOS': 'ubuntu24',
        'TASK_WORKFLOW': grant['workflow'], 'TASK_JOB': grant['job'],
        'TASK_RUN_NUMBER': '31', 'TASK_RUN_ATTEMPT': '2', 'TASK_RUN_ID': '731009',
        'MDS_LNX03_ADMISSION_GRANT': json.dumps(grant, sort_keys=True),
        'MDS_LNX03_SETUP_GRANT': json.dumps(dict(grant, capability='network-setup'), sort_keys=True),
    }
    for task, github in {
        'TASK_EVENT': 'GITHUB_EVENT_NAME', 'TASK_EVENT_SHA': 'GITHUB_SHA',
        'TASK_EVENT_REF': 'GITHUB_REF', 'TASK_REPOSITORY': 'GITHUB_REPOSITORY',
        'TASK_ACTOR': 'GITHUB_ACTOR', 'TASK_TRIGGERING_ACTOR': 'GITHUB_TRIGGERING_ACTOR',
    }.items():
        values[task] = values[github]
    return values, sha, base, grant


@pytest.fixture
def source_repository(helper, tmp_path, monkeypatch):
    """Independent Git protocol vectors, with no subprocess authority.

    Hosted pytest is inside the application process guard. Literal replies
    exercise the actual source parser; real Git integration belongs to the
    trusted controller outside this consumer and is separately evidenced.
    """
    root = tmp_path / 'source-repository'
    root.mkdir()
    paths = (
        '.github/lnx02/verify_env.py', '.github/workflows/d2-linux-offline.yml',
        'tools/offline_linux.py', 'tools/offline_guard_linux.py',
        'tests/linux_bootstrap/conftest.py', 'tests/linux_bootstrap/native_worker.py',
        'tests/linux_bootstrap/test_environment_contract.py',
        'tests/linux_bootstrap/test_admission.py', 'tests/linux_bootstrap/test_spawn_contract.py',
        'docs/d2_linux_ci.md', 'docs/testing.md', 'docs/coordination.md',
    )
    protected = ('run_logging.py',) + tuple(
        'requirements/' + name for name in (
            'linux-cp314-runtime.lock', 'linux-cp314-test.lock', 'linux-cp314-dev.lock',
            'windows-cp314-runtime.lock', 'windows-cp314-test.lock', 'windows-cp314-dev.lock',
            'runtime.in', 'test.in', 'dev.in'))
    sha, base, main = 'a' * 40, 'b' * 40, 'c' * 40
    replies = {
        ('rev-parse', 'HEAD'): (0, (sha + '\n').encode('ascii')),
        ('cat-file', '-e', base + '^{commit}'): (0, b''),
        ('merge-base', '--is-ancestor', base, sha): (0, b''),
        ('status', '--porcelain=v1', '--untracked-files=all'): (0, b''),
        ('diff', '--name-status', '--no-renames', '-z', base, sha, '--'):
            (0, b''.join(b'M\0' + name.encode('utf-8') + b'\0' for name in paths)),
        ('merge-base', '--is-ancestor', base, base): (0, b''),
        ('diff', '--name-only', '--no-renames', '-z', base, base, '--'): (0, b''),
        ('merge-base', '--is-ancestor', base, main): (0, b''),
        ('diff', '--name-only', '--no-renames', '-z', base, main, '--'): (0, b'unrelated.txt\0'),
    }
    seen = []

    def read(actual_root, arguments, allowed_failure=False):
        assert actual_root == root
        command = tuple(arguments)
        seen.append(command)
        assert command in replies, 'unexpected Git query: ' + repr(command)
        code, stdout = replies[command]
        return types.SimpleNamespace(returncode=code, stdout=stdout)

    monkeypatch.setattr(helper, 'git_read', read)
    return {'root': root, 'base': base, 'head': sha, 'main': main,
            'replies': replies, 'seen': seen, 'allowed': paths, 'protected': protected}
