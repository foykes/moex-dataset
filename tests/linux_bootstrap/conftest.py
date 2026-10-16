"""Closed LNX-03 fixtures; importing these tests grants no native authority."""

import ast
import copy
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
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
def native_lifecycle(process_policy, tmp_path, monkeypatch):
    """Exercise real control flow and owned FDs without creating a process.

    Popen, kernel process observations and F-LOG are explicit doubles. These
    tests prove portable fault handling only, never native birth or settlement.
    Actual pipe/open/close calls provide an independent FD lifetime oracle.
    """
    from multiprocessing import resource_tracker

    module = process_policy
    scratch = tmp_path / 'lifecycle'
    scratch.mkdir()
    state = types.SimpleNamespace(
        fault=None, fault_reached=False, trace=[], processes=[], allocations=[],
        active={}, unknown_closes=[], outcomes=[], reports=[], session=None,
        result=None, ran=False, pipe_calls=0, registry_roles=(),
    )
    sentinels = os.pipe()
    sentinel_identities = {descriptor: os.fstat(descriptor) for descriptor in sentinels}
    real_close, real_open, real_pipe = os.close, os.open, os.pipe

    def allocate(descriptor, kind, role=None, name=None):
        assert descriptor not in state.active
        record = dict(fd=descriptor, kind=kind, role=role, name=name, closed=0)
        state.allocations.append(record)
        state.active[descriptor] = record
        state.trace.append(('open', kind, name))
        return descriptor

    def pipe():
        state.pipe_calls += 1
        if state.fault == 'pipe_acquisition_' + str(state.pipe_calls):
            fault('pipe:' + str(state.pipe_calls))
        pair = real_pipe()
        for descriptor in pair:
            allocate(descriptor, 'pipe')
        return pair

    def open_file(path, flags, mode=0o777):
        descriptor = real_open(path, flags, mode)
        name = Path(path).name
        return allocate(descriptor, 'marker' if name == 'foreign-marker' else 'file', name=name)

    def write(descriptor, data):
        record = state.active[descriptor]
        if state.fault == 'ticket_write_failure' and record['name'] == 'ftp.json':
            fault('write:ftp.json')
        if (state.fault == 'registry_write_failure' and record['name'] == 'pid-registry.pending'
                and 'ftp' in state.registry_roles):
            fault('write:pid-registry.pending')
        return os.write(descriptor, data)

    def record_close(descriptor):
        record = state.active.pop(descriptor, None)
        if record is None:
            state.unknown_closes.append(descriptor)
            # Do not allow a broken test subject to close pytest's own FDs.
            raise AssertionError('unexpected descriptor close')
        record['closed'] += 1
        state.trace.append(('close', record['kind'], record['role']))

    def close(descriptor):
        record_close(descriptor)
        real_close(descriptor)

    @contextmanager
    def fdopen(descriptor, *arguments, **options):
        # Source-bound 95a uses fdopen; the repaired writer uses raw close.
        # Delegate the real file lifetime and count either ownership path once.
        handle = os.fdopen(descriptor, *arguments, **options)
        try:
            with handle:
                yield handle
        finally:
            record_close(descriptor)

    def fault(boundary):
        state.fault_reached = True
        state.trace.append(('fault', boundary))
        raise OSError('LNX03_PORTABLE_LIFECYCLE_FAULT')

    def popen(command, **arguments):
        # The command is inspected, never executed. No process authority is
        # borrowed from the native consumer by a portable policy unit test.
        role = Path(command[-1]).stem
        state.trace.append(('popen-attempt', role))
        if state.fault == 'second_launch_constructor_refusal' and role == 'ftp':
            fault('popen:ftp')
        child = types.SimpleNamespace(
            role=role, pid=900000 + len(state.processes), returncode=None,
            killed=False, waits=[],
        )

        def poll():
            return child.returncode

        def kill():
            child.killed = True
            state.trace.append(('kill', role))

        def wait(timeout=None):
            child.waits.append(timeout)
            state.trace.append(('wait', role))
            if child.returncode is None:
                child.returncode = -9 if child.killed else 0
            return child.returncode

        child.poll, child.kill, child.wait = poll, kill, wait
        state.processes.append(child)
        state.trace.append(('popen-return', role))
        return child

    def process_for(pid):
        return next(process for process in state.processes if process.pid == pid)

    def pidfd_open(pid, flags):
        child = process_for(pid)
        state.trace.append(('pidfd-bind', child.role))
        if state.fault == 'second_pidfd_bind_failure' and child.role == 'ftp':
            fault('pidfd:ftp')
        return allocate(real_open(os.devnull, os.O_RDONLY), 'pidfd', child.role)

    def process_identity(pid):
        return dict(start_ticks=pid + 10, parent_pid=os.getpid(), session=pid)

    def descriptor_identity(descriptor):
        # The product's Linux device field is part of its native admission.
        # Windows stat_result lacks it: only this explicit portable kernel
        # double supplies zero while keeping real descriptor validity/stat.
        info = os.fstat(descriptor)
        return info.st_dev, info.st_ino, info.st_mode, getattr(info, 'st_rdev', 0)

    original_ticket, original_registry = module._ticket, module._registry

    def ticket(path, value):
        role = value['role']
        state.trace.append(('ticket', role))
        if state.fault in {'second_ticket', 'unsettled_after_second_ticket'} and role == 'ftp':
            fault('ticket:ftp')
        return original_ticket(path, value)

    def registry(work, children, tracker):
        if (state.fault == 'second_registry_refusal'
                and any(item.get('role') == 'ftp' and item.get('process') is not None
                        for item in children)):
            fault('registry:ftp')
        state.registry_roles = tuple(item.get('role') for item in children
                                     if item.get('process') is not None)
        try:
            return original_registry(work, children, tracker)
        finally:
            state.registry_roles = ()

    def settle(children, timeout=10):
        # Deliberately a process double: the native suite proves OS settlement.
        state.trace.append(('cleanup-requested',))
        if state.fault == 'unsettled_after_second_ticket':
            return
        for item in children:
            child = item.get('process')
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=timeout)

    logger = types.ModuleType('run_logging')

    def configure_logging(*arguments, **options):
        state.session = {
            '_expected': {}, '_final': None, '_worker_finals': {},
            '_queue': types.SimpleNamespace(_thread=None, close=lambda: None),
            '_thread': types.SimpleNamespace(is_alive=lambda: False),
        }
        state.trace.append(('logging-configured',))
        return state.session

    def prepare_worker(session, role):
        assert session is state.session
        state.trace.append(('prepare-attempt', role))
        if state.fault == 'second_prepare_registration' and role == 'ftp':
            fault('prepare:ftp')
        session['_expected'][role] = role
        state.trace.append(('registered', role))
        return dict(role=role, _token=role)

    def write_bootstrap(descriptor, *arguments):
        state.trace.append(('bootstrap-write',))
        if state.fault == 'bootstrap_write_failure':
            fault('bootstrap-write')

    def read_events(descriptor):
        state.trace.append(('witness-read',))
        if state.fault == 'witness_read_failure' and not state.fault_reached:
            fault('witness-read')
        return []

    def record_worker_outcome(session, role, **outcome):
        assert session is state.session
        state.outcomes.append(dict(role=role, **outcome))
        state.trace.append(('logging-outcome', role, outcome['started']))

    def finalize_logging(session, **outcome):
        assert session is state.session
        report = dict(outcome, delivery_outcome='INCOMPLETE', storage_sealed=False)
        report.setdefault('exit_status', 1)
        session['_final'] = report
        state.reports.append(report)
        state.trace.append(('logging-finalized',))
        return report

    logger.configure_logging = configure_logging
    logger.prepare_worker = prepare_worker
    logger.record_worker_outcome = record_worker_outcome
    logger.finalize_logging = finalize_logging
    os_proxy = types.SimpleNamespace(**vars(os))
    os_proxy.pipe, os_proxy.open, os_proxy.close, os_proxy.write = pipe, open_file, close, write
    os_proxy.fdopen = fdopen
    os_proxy.pidfd_open, os_proxy.getsid = pidfd_open, lambda pid: pid
    os_proxy.O_NOFOLLOW = getattr(os, 'O_NOFOLLOW', 0)
    monkeypatch.setattr(module, 'os', os_proxy)
    monkeypatch.setattr(module, 'subprocess', types.SimpleNamespace(
        Popen=popen, DEVNULL=subprocess.DEVNULL, TimeoutExpired=subprocess.TimeoutExpired))
    monkeypatch.setattr(module, 'install_process_guard', lambda: types.SimpleNamespace(
        command=None, owner=None))
    monkeypatch.setattr(module, '_native_root', lambda repo: scratch)
    monkeypatch.setattr(module, '_process_identity', process_identity)
    monkeypatch.setattr(module, '_fd_identity', descriptor_identity)
    monkeypatch.setattr(module, '_ticket', ticket)
    monkeypatch.setattr(module, '_registry', registry)
    monkeypatch.setattr(module, '_serialize', lambda bootstrap, repo: (b'prep', b'payload', set()))
    monkeypatch.setattr(module, '_write_bootstrap', write_bootstrap)
    monkeypatch.setattr(module, '_read_events', read_events)
    monkeypatch.setattr(module, '_pidfd_exited', lambda descriptor: True)
    monkeypatch.setattr(module, '_settle', settle)
    monkeypatch.setattr(resource_tracker, '_resource_tracker', types.SimpleNamespace(_fd=None, _pid=None))
    monkeypatch.setitem(sys.modules, 'run_logging', logger)
    monkeypatch.setenv('LNX03_SOURCE_SHA', 'a' * 40)

    def run(selected_fault):
        assert not state.ran
        state.ran, state.fault = True, selected_fault
        state.result = module._run_native_case('success', ROOT, scratch, {})
        return state

    def unowned_intact():
        return all(os.fstat(descriptor) == identity
                   for descriptor, identity in sentinel_identities.items())

    state.run, state.unowned_intact = run, unowned_intact
    state.retained = module._unsettled_transports
    yield state
    # Test failures must not leak their real local FD fixtures into later tests.
    # This cleanup is independent and is never counted as supervisor success.
    for descriptor in tuple(state.active):
        try:
            real_close(descriptor)
        except OSError:
            pass
    for descriptor in sentinels:
        real_close(descriptor)


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
