"""Test-only, stdlib guard and evidence for the four fixed offline lanes.

This prevents accidental side effects. It is not a sandbox for hostile Python.
Native lanes retain their own policies and report through this small adapter.
"""

import atexit
from contextlib import contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import uuid
import weakref


sys.modules.setdefault('mds_offline_guard', sys.modules[__name__])
LANES = {'f1': '.f1', 'f2': '.f2', 'flog': '.f-log', 'f3': '.f3'}
ROLES = {'entry', 'worker', 'supervisor', 'lane-worker', 'notebook-check', 'f2-probe', 'f2-preflight',
         'f2-script', 'f2-notebook', 'f2-hook', 'f2-precommit',
         'f2-hook-check', 'f2-spawn', 'flog-probe', 'flog-worker',
         'native-git', 'f3-control', 'command'}
_CONTEXT = None
_ORIGINAL_SLEEP = time.sleep
_ORIGINAL_POPEN = subprocess.Popen.__init__
_ORIGINAL_OPEN = open
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_FDOPEN = os.fdopen
_ORIGINAL_OS_CLOSE = os.close
_OWN_FDS = {0, 1, 2}
_WRITABLE_FDS = {}
_INSTALLED = False
_THREAD_STATE = threading.local()
_CHILD_JOBS = {}
_FIXTURE_SCRIPTS = {}


class OfflineViolation(RuntimeError):
    """An operation crossed the declared offline workload boundary."""


def _absolute(value):
    return Path(os.path.abspath(os.fsdecode(value)))


def _linked(info):
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, 'st_file_attributes', 0)
        & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0))


def checked_path(value, boundary=None):
    """Check lexical components before resolve/open; never expand the scope."""
    path = _absolute(value)
    if boundary is not None and not path.is_relative_to(_absolute(boundary)):
        raise ValueError('OFFLINE_PATH_OUTSIDE_ROOT')
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if _linked(info) or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1):
            raise ValueError('OFFLINE_PATH_ALIAS')
        if current != path and not stat.S_ISDIR(info.st_mode):
            raise ValueError('OFFLINE_PATH_COMPONENT')
    return path


@contextmanager
def _internal():
    _THREAD_STATE.internal = getattr(_THREAD_STATE, 'internal', 0) + 1
    try:
        yield
    finally:
        _THREAD_STATE.internal -= 1


def internal_report_io(event, args):
    """Only the adapter's own synchronous report writes bypass native writers."""
    if not getattr(_THREAD_STATE, 'internal', 0) or _CONTEXT is None:
        return False
    if event not in {'open', 'os.mkdir', 'os.remove', 'os.rename'}:
        return False
    values = args[:2] if event == 'os.rename' else args[:1]
    for value in values:
        if not isinstance(value, (str, bytes, os.PathLike)):
            return False
        try:
            if not _absolute(value).is_relative_to(_CONTEXT.report_dir):
                return False
        except (TypeError, ValueError):
            return False
    return True


def exclusive_json(path, obj):
    path = checked_path(path)
    # Native guards see only the exact report operation, never a workload flag.
    with _internal():
        path.parent.mkdir(parents=True, exist_ok=True)
        checked_path(path)
        with _ORIGINAL_OPEN(path, 'x', encoding='utf-8', newline='\n') as stream:
            json.dump(obj, stream, ensure_ascii=True, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
    return path


def _read_json(path):
    checked_path(path)
    with _ORIGINAL_OPEN(path, encoding='utf-8') as stream:
        result = json.load(stream)
    if not isinstance(result, dict):
        raise ValueError('OFFLINE_REPORT_FORMAT')
    return result


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def _normal_argv(argv):
    values = [os.fsdecode(v) for v in argv]
    result = []
    skip = False
    for position, value in enumerate(values):
        if skip:
            skip = False
            continue
        if value == '--ticket':
            skip = True
            continue
        if position == 0:
            binary = shutil.which(value) or value
            value = os.path.normcase(os.path.abspath(binary))
        elif value.endswith('.py'):
            value = str(_absolute(value))
        result.append(value)
    return result


def _source_inventory(root):
    """Check metadata and deny rules before any candidate content or Git status."""
    paths = []
    pending = [(root, False), (root / 'tools', True), (root / 'tests', True),
               (root / '.github/workflows', True)]
    while pending:
        directory, recursive = pending.pop()
        checked_path(directory, root)
        if not directory.exists():
            continue
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                info = path.lstat()
                candidate = path.suffix.lower() in {'.py', '.ipynb', '.toml', '.yml', '.yaml'}
                if _secret(path) and (candidate or recursive and stat.S_ISDIR(info.st_mode)):
                    raise ValueError('OFFLINE_SOURCE_PRIVATE_DATA')
                if candidate or recursive and (stat.S_ISDIR(info.st_mode) or _linked(info)):
                    checked_path(path, root)
                if recursive and stat.S_ISDIR(info.st_mode):
                    pending.append((path, True))
                elif candidate and stat.S_ISREG(info.st_mode):
                    paths.append(path)
    return sorted(set(paths))


def source_identity(root):
    """Local Git only; unsafe source inventory fails before content reads."""
    root = checked_path(root)
    inventory = _source_inventory(root)
    git = shutil.which('git')
    if not git:
        raise ValueError('OFFLINE_GIT_MISSING')
    environment = dict(os.environ)
    for name in list(environment):
        if name.startswith('GIT_'):
            environment.pop(name, None)
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
                       GIT_TERMINAL_PROMPT='0')
    prefix = [git, '-c', 'core.fsmonitor=false', '-c', 'core.untrackedCache=false']
    result = subprocess.run([*prefix, 'rev-parse', 'HEAD'], cwd=root, env=environment,
                            capture_output=True, timeout=10, check=True)
    sha = result.stdout.decode('ascii').strip()
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('OFFLINE_SHA_INVALID')
    result = subprocess.run([*prefix, 'status', '--porcelain', '--untracked-files=all'],
                            cwd=root, env=environment, capture_output=True, timeout=10, check=True)
    paths = []
    for path in inventory:
        checked_path(path, root)
        if _secret(path):
            raise ValueError('OFFLINE_SOURCE_PRIVATE_DATA')
        paths.append((path.relative_to(root).as_posix(), hashlib.sha256(path.read_bytes()).hexdigest()))
    return sha, 'working-tree' if result.stdout else 'commit', _digest(sorted(set(paths)))


def report_root(root, lane='f3'):
    if lane not in LANES:
        raise ValueError('OFFLINE_LANE_INVALID')
    root = checked_path(root)
    if _CONTEXT is not None and _CONTEXT.root == root and _CONTEXT.lane == lane:
        return _CONTEXT.report_dir
    boundary = root / LANES[lane] / 'evidence' / 'runs'
    requested = os.environ.get('MDS_ENVIRONMENT_OUTPUT_ROOT' if lane == 'f1' else 'MDS_OFFLINE_REPORT_ROOT')
    if requested:
        path = checked_path(requested, boundary)
        if path.parent != boundary or not re.fullmatch('[0-9a-f]{32}', path.name):
            raise ValueError('OFFLINE_REPORT_ROOT_INVALID')
        if path.exists():
            raise ValueError('OFFLINE_EXISTING_REPORT_ROOT')
        path.mkdir(parents=True, exist_ok=False)
    else:
        path = boundary / uuid.uuid4().hex
        checked_path(path, boundary)
        path.mkdir(parents=True, exist_ok=False)
    return path


def _wait_gate(ticket):
    gate = ticket.get('gate')
    if not gate:
        return
    deadline = time.monotonic() + 5
    while not Path(gate).is_file():
        if time.monotonic() >= deadline:
            raise ValueError('OFFLINE_JOB_ASSIGNMENT_TIMEOUT')
        _ORIGINAL_SLEEP(.01)


def _load_ticket(root, implicit=False):
    value = os.environ.get('MDS_OFFLINE_TICKET')
    if not value:
        return None
    root = checked_path(root)
    path = checked_path(value)
    if not any(path.is_relative_to(root / d / 'evidence/runs') for d in LANES.values()):
        raise ValueError('OFFLINE_TICKET_ROOT')
    ticket = _read_json(path)
    if ticket.get('version') != 1 or ticket.get('role') not in ROLES:
        raise ValueError('OFFLINE_TICKET_ROLE')
    if ticket.get('root') != str(root) or not re.fullmatch('[0-9a-f]{32}', ticket.get('run_id', '')):
        raise ValueError('OFFLINE_TICKET_IDENTITY')
    if _digest({k: v for k, v in ticket.items() if k != 'digest'}) != ticket.get('digest'):
        raise ValueError('OFFLINE_TICKET_CORRUPT')
    if ticket.get('bootstrap_digest') != hashlib.sha256(Path(__file__).read_bytes()).hexdigest():
        raise ValueError('OFFLINE_BOOTSTRAP_CHANGED')
    if ticket['role'] == 'f2-script':
        script = checked_path(ticket.get('script_path', ''), root / '.f2/tmp')
        if (script.name != 'cold_converter_import.py'
                or ticket.get('script_digest') != hashlib.sha256(script.read_bytes()).hexdigest()):
            raise ValueError('OFFLINE_FIXTURE_SCRIPT_CHANGED')
    if ticket['role'] not in {'f2-spawn', 'flog-worker'} and not (implicit and ticket['role'] in {'native-git', 'f2-precommit'}):
        expected = _normal_argv(ticket['argv'])
        if ticket['role'] == 'f2-precommit':
            expected = _normal_argv([sys.executable, '-I', '-B', '-X', 'utf8', str(root / 'tools/offline_guard.py'), '--role', 'f2-precommit'])
        actual = _normal_argv(sys.orig_argv)
        if ticket['role'] == 'command' and Path(ticket['argv'][0]).name.lower() in {'pytest', 'pytest.exe'}:
            # Windows distlib's console launcher starts the same interpreter.
            console = _normal_argv([sys.executable, *ticket['argv']])
            if actual == console:
                actual = expected
        if actual != expected:
            raise ValueError('OFFLINE_TICKET_ARGV')
    ticket['path'] = str(path)
    _wait_gate(ticket)
    return ticket


def ensure_context(root, lane='f3', role='entry', mode='run', native_counts=None, install=None):
    global _CONTEXT
    if _CONTEXT is not None:
        if native_counts is not None:
            _CONTEXT.native_counts = native_counts
        return _CONTEXT
    root = checked_path(root)
    ticket = _load_ticket(root)
    if ticket is not None:
        lane, role, mode = ticket['lane'], ticket['role'], ticket['mode']
        directory = checked_path(ticket['report_dir'])
        sha, source_mode, source_digest = ticket['tested_sha'], ticket['source_mode'], ticket['source_digest']
        process_id, parent_id = ticket['ticket_id'], ticket['parent_id']
        run_id = ticket['run_id']
        roots = [checked_path(p) for p in ticket['roots']]
        expected_counts = dict(ticket.get('expected_counts', {}))
    else:
        if lane not in LANES or role not in ROLES or mode not in {'run', 'collect', 'legacy'}:
            raise ValueError('OFFLINE_CONTEXT_INVALID')
        directory = report_root(root, lane)
        # Inherited strings are not evidence. Only the issued ticket may carry
        # the issuer's identity; a fresh entry verifies the actual local tree.
        sha, source_mode, source_digest = source_identity(root)
        run_id = directory.name
        process_id, parent_id = uuid.uuid4().hex, None
        roots = [root / LANES[lane] / 'tmp', root / LANES[lane] / 'cache' / 'runs' / run_id, directory]
        expected_counts = {}
    directory.mkdir(parents=True, exist_ok=True)
    # Pytest's parent mkdir calls happen before fixture creation. Allocate only
    # the already-issued roots before guards; ancestors aren't workload scopes.
    for issued_root in roots:
        checked_path(issued_root)
        issued_root.mkdir(parents=True, exist_ok=True)
    _CONTEXT = SimpleNamespace(root=root, lane=lane, role=role, mode=mode,
        report_dir=directory, run_id=run_id, process_id=process_id, parent_id=parent_id,
        tested_sha=sha, source_mode=source_mode, source_digest=source_digest,
        roots=roots, counts={}, native_counts=native_counts, expected_counts=expected_counts,
        violations=[], children=[], finished=False, report_error=False, ticket=ticket,
        internal_paths=set(), expected_exit=ticket.get('expected_exit') if ticket else None)
    _CONTEXT.started = False
    _CONTEXT.argv_digest = ticket.get('argv_digest') if ticket else _digest(list(sys.orig_argv))
    _CONTEXT.bootstrap_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    _CONTEXT.policy_digest = _digest({'version': 1, 'lane': lane, 'roots': [str(p) for p in roots]})
    os.environ['MDS_OFFLINE_ROOT'] = str(root)
    os.environ['MDS_OFFLINE_REPORT_ROOT'] = str(directory)
    if lane == 'f1':
        os.environ['MDS_ENVIRONMENT_OUTPUT_ROOT'] = str(directory)
    os.environ['MDS_OFFLINE_TESTED_SHA'] = sha
    os.environ['MDS_OFFLINE_SOURCE_MODE'] = source_mode
    os.environ['MDS_OFFLINE_SOURCE_DIGEST'] = source_digest
    os.environ['MDS_OFFLINE_RUN_ID'] = run_id
    if install is None:
        install = lane == 'f3'
    if install:
        install_guards()
        mark_guard_ready()
    atexit.register(_atexit_finish)
    return _CONTEXT


def attach_native(root, lane, role, counts):
    return ensure_context(root, lane=lane, role=role, native_counts=counts, install=False)


def report_metadata():
    if _CONTEXT is None:
        return {}
    c = _CONTEXT
    return dict(version=1, run_id=c.run_id, lane=c.lane, mode=c.mode,
                tested_sha=c.tested_sha, source_mode=c.source_mode,
                source_digest=c.source_digest, ticket_id=c.process_id,
                parent_id=c.parent_id, role=c.role, pid=os.getpid(),
                argv_digest=getattr(c, 'argv_digest', None),
                bootstrap_digest=getattr(c, 'bootstrap_digest', None),
                policy_digest=getattr(c, 'policy_digest', None))


def mark_guard_ready():
    if _CONTEXT is None:
        raise ValueError('OFFLINE_CONTEXT_MISSING')
    c = _CONTEXT
    if c.started:
        return
    with _internal():
        (c.report_dir / 'processes').mkdir(exist_ok=True)
        (c.report_dir / 'tickets').mkdir(exist_ok=True)
        exclusive_json(c.report_dir / 'processes' / (c.process_id + '.start.json'), report_metadata())
    c.started = True


def native_notice(kind, role=None):
    if _CONTEXT is None:
        raise RuntimeError('OFFLINE_CONTEXT_MISSING')
    c = _CONTEXT
    c.counts[kind] = c.counts.get(kind, 0) + 1
    item = dict(report_metadata(), kind=kind, sequence=len(c.violations) + 1,
                nodeid=os.environ.get('PYTEST_CURRENT_TEST', '').split(' (')[0])
    c.violations.append(item)
    try:
        exclusive_json(c.report_dir / 'processes' / (c.process_id + '.violation.' + str(item['sequence']) + '.json'), item)
    except BaseException:
        c.report_error = True
        raise RuntimeError('OFFLINE_EVIDENCE_WRITE_FAILED') from None


def reject(kind):
    native_notice(kind)
    raise OfflineViolation('F3_OFFLINE_' + kind.upper())


def set_expected_counts(counts):
    if _CONTEXT is None:
        raise RuntimeError('OFFLINE_CONTEXT_MISSING')
    c = _CONTEXT
    # Native plugin calls this only after exact node-bound controls. Preserve
    # their identity in a separate immutable ledger, not a blanket lane bypass.
    changed = {k: v - c.expected_counts.get(k, 0) for k, v in counts.items()
               if v != c.expected_counts.get(k, 0)}
    if changed:
        node = getattr(c, 'nodeid', os.environ.get('PYTEST_CURRENT_TEST', '').split(' (')[0])
        _fault_ledger(node, changed)
    c.expected_counts = dict(counts)


def _fault_scope(node, delta):
    allowed = {
        'tests/environment/test_environment.py::test_offline_guards': {'network': 4, 'children': 9},
        'tests/logging/test_failures.py::test_guards_reject_live_actions': {'network': 1, 'secrets': 1, 'writers': 2, 'children': 1},
    }
    cli_control = (node == 'tests/environment/test_environment.py::test_cli_and_report_normal_first_and_repeat'
                   and delta in ({'network': 4, 'children': 9}, {'network': 8, 'children': 18}))
    shared_control = (node.startswith('tests/shared/test_guard.py::')
                      and len(delta) == 1 and next(iter(delta.values()), None) == 1
                      and next(iter(delta), None) in {'network', 'writers', 'sleep', 'secrets',
                                                     'dataset_reads', 'aliases', 'descriptors', 'children'})
    return shared_control or delta == allowed.get(node) or cli_control


def _fault_ledger(node, delta):
    c = _CONTEXT
    if not _fault_scope(node, delta):
        raise ValueError('OFFLINE_EXPECTED_FAULT_SCOPE')
    path = c.report_dir / 'processes' / (c.process_id + '.expected.' + uuid.uuid4().hex + '.json')
    exclusive_json(path, dict(report_metadata(), nodeid=node, counts=delta))


@contextmanager
def expect_fault(kind, count=1):
    """Test-only exact expectation; the operation is still refused and journaled."""
    before = _CONTEXT.counts.get(kind, 0)
    node = os.environ.get('PYTEST_CURRENT_TEST', '').split(' (')[0]
    if not node.startswith('tests/shared/') or count != 1:
        raise ValueError('OFFLINE_FAULT_SCOPE')
    yield
    if _CONTEXT.counts.get(kind, 0) - before != count:
        raise AssertionError('Expected exactly one refused control')
    _fault_ledger(node, {kind: count})
    _CONTEXT.expected_counts[kind] = _CONTEXT.expected_counts.get(kind, 0) + count


def finish(exit_code=0):
    if _CONTEXT is None or _CONTEXT.finished:
        return None
    c = _CONTEXT
    obj = dict(report_metadata(), exit_code=exit_code, counters=dict(c.counts),
               native_counters=dict(c.native_counts or {}), native_guard=c.native_counts is not None,
               expected_counts=dict(c.expected_counts),
               violations=len(c.violations), children=[t['ticket_id'] for t in c.children],
               report_error=c.report_error)
    try:
        exclusive_json(c.report_dir / 'processes' / (c.process_id + '.final.json'), obj)
        c.finished = True
    except BaseException:
        c.report_error = True
        raise RuntimeError('OFFLINE_EVIDENCE_WRITE_FAILED') from None
    return obj


def _atexit_finish():
    try:
        finish(None)
    except BaseException:
        # A missing final report is a mandatory parent failure, not a silent PASS.
        sys.stderr.write('OFFLINE_FINAL_REPORT_FAILED\n')
    finally:
        for identifier in list(_CHILD_JOBS):
            _job_close(_CHILD_JOBS.pop(identifier), terminate=True)


def _owned(value):
    try:
        path = checked_path(value)
        return any(path.is_relative_to(p) for p in _CONTEXT.roots)
    except (TypeError, ValueError):
        return False


def _secret(path):
    parts = [p.lower() for p in path.parts]
    name = path.name.lower()
    return 'secrets' in parts or name == '.env' or name.startswith('.env.') or name in {
        'credentials.json', 'service_account.json', 'service-account.json',
        'ftp_credentials.json', 'ftp_credentials.py', 'token.json', 'credentials.ini'}


def _read_allowed(path):
    """Content reads are source/settings/interpreter or issued fixture reads."""
    if os.path.normcase(str(path)) == os.path.normcase(os.path.abspath(os.devnull)):
        return True
    if _owned(path):
        return True
    c = _CONTEXT
    for value in (sys.prefix, sys.base_prefix, str(Path(sys.executable).parent)):
        if path.is_relative_to(_absolute(value)):
            return True
    if path.is_relative_to(c.root):
        relative = path.relative_to(c.root)
        parts = [p.lower() for p in relative.parts]
        if parts and parts[0] == 'datasets':
            return False
        if path.suffix.lower() in {'.csv', '.xlsx', '.xls', '.parquet', '.feather'}:
            return path.is_relative_to(c.root / 'tests/fixtures')
        return True
    return False


def _descriptor_writable(descriptor):
    """A numeric FD alone does not grant mutation of its current object."""
    record = _WRITABLE_FDS.get(descriptor)
    if record is None:
        return False
    path, identity, owner = record
    try:
        checked_path(path)
        info, current = os.fstat(descriptor), path.stat()
        stream = owner() if owner is not None else None
        valid = ((owner is None or stream is not None and not stream.closed)
                 and _owned(path) and stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                 and (info.st_dev, info.st_ino) == identity
                 and (current.st_dev, current.st_ino) == identity)
    except (OSError, ValueError):
        valid = False
    return valid


def _writable_descriptor(descriptor):
    if not _descriptor_writable(descriptor):
        reject('descriptors')


def _audit(event, args):
    if internal_report_io(event, args):
        return
    if event in {'socket.connect', 'socket.bind', 'socket.sendto', 'socket.getaddrinfo',
                 'socket.gethostbyname', 'socket.gethostbyaddr', 'socket.getnameinfo'}:
        reject('network')
    if event == 'open':
        value, mode, flags = args
        if isinstance(value, int):
            if getattr(_THREAD_STATE, 'launch', 0):
                _OWN_FDS.add(value)
            if value not in _OWN_FDS:
                reject('descriptors')
            return
        if not isinstance(value, (str, bytes, os.PathLike)):
            return
        path = _absolute(value)
        if _secret(path):
            reject('secrets')
        writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
        null = os.path.normcase(str(path)) == os.path.normcase(os.path.abspath(os.devnull))
        if writing and not null and not _owned(path):
            reject('writers')
        if not writing and not _read_allowed(path):
            reject('dataset_reads')
        try:
            checked_path(path)
        except ValueError:
            reject('aliases')
    if event == 'os.truncate' and isinstance(args[0], int):
        _writable_descriptor(args[0])
    if event in {'os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.chmod', 'os.link', 'os.symlink', 'os.truncate'}:
        if event == 'os.truncate' and isinstance(args[0], int):
            return
        paths = args[:2] if event in {'os.rename', 'os.link', 'os.symlink'} else args[:1]
        # Unlinking pytest's owned final *current link does not follow its target.
        if event == 'os.remove':
            p = _absolute(args[0])
            if p.name.endswith('current') and _owned(p.parent):
                return
        if not all(_owned(p) for p in paths):
            reject('writers')
    if event in {'os.listdir', 'os.scandir'} and args and isinstance(args[0], (str, bytes, os.PathLike)):
        path = _absolute(args[0])
        if _secret(path):
            reject('secrets')
        if 'datasets' in [p.lower() for p in path.parts] and not _owned(path):
            reject('dataset_reads')
    if event in {'os.system', 'os.exec', 'os.spawn', 'os.posix_spawn', 'os.startfile',
                 'os.fork', 'os.forkpty', '_winapi.CreateProcess', 'subprocess.Popen'}:
        if not getattr(_THREAD_STATE, 'launch', 0):
            reject('children')


def install_guards():
    global _INSTALLED
    if _INSTALLED:
        return
    sys.dont_write_bytecode = True
    socket.has_ipv6 = False
    for name in ('connect', 'connect_ex', 'bind', 'sendto', 'sendmsg'):
        if hasattr(socket.socket, name):
            setattr(socket.socket, name, lambda *a, **kw: reject('network'))
    for name in ('getaddrinfo', 'gethostbyname', 'gethostbyname_ex', 'gethostbyaddr',
                 'getnameinfo', 'create_connection'):
        setattr(socket, name, lambda *a, **kw: reject('network'))

    def workload_sleep(seconds):
        if getattr(_THREAD_STATE, 'controller', 0):
            return _ORIGINAL_SLEEP(seconds)
        reject('sleep')
    time.sleep = workload_sleep

    def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
        if dir_fd is not None:
            reject('descriptors')
        descriptor = _ORIGINAL_OS_OPEN(path, flags, mode, dir_fd=dir_fd)
        _OWN_FDS.add(descriptor)
        _WRITABLE_FDS.pop(descriptor, None)
        if flags & (os.O_WRONLY | os.O_RDWR) and _owned(path):
            info = os.fstat(descriptor)
            if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                _WRITABLE_FDS[descriptor] = (checked_path(path), (info.st_dev, info.st_ino), None)
        return descriptor

    def guarded_close(fd):
        _OWN_FDS.discard(fd)
        _WRITABLE_FDS.pop(fd, None)
        return _ORIGINAL_OS_CLOSE(fd)
    os.open = guarded_open
    os.close = guarded_close
    def guarded_fdopen(fd, *args, **kwargs):
        stream = _ORIGINAL_FDOPEN(fd, *args, **kwargs)
        record = _WRITABLE_FDS.pop(fd, None)
        if record is not None and stream.writable():
            _WRITABLE_FDS[fd] = (record[0], record[1], weakref.ref(stream))
        return stream
    os.fdopen = guarded_fdopen
    original_dup = os.dup
    def guarded_dup(fd):
        if fd not in _OWN_FDS:
            reject('descriptors')
        if fd in _WRITABLE_FDS:
            _writable_descriptor(fd)
        descriptor = original_dup(fd)
        _OWN_FDS.add(descriptor)
        _WRITABLE_FDS.pop(descriptor, None)
        if fd in _WRITABLE_FDS:
            record = _WRITABLE_FDS[fd]
            _WRITABLE_FDS[descriptor] = (record[0], record[1], None)
        return descriptor
    os.dup = guarded_dup
    original_dup2 = os.dup2
    def guarded_dup2(fd, target, inheritable=True):
        # Preserve capture/stdio redirection. Duplication itself grants no
        # truncate permission unless its source still has a valid write grant.
        record = _WRITABLE_FDS.get(fd) if _descriptor_writable(fd) else None
        result = original_dup2(fd, target, inheritable=inheritable)
        if fd in _OWN_FDS:
            _OWN_FDS.add(target)
        _WRITABLE_FDS.pop(target, None)
        if record is not None:
            _WRITABLE_FDS[target] = record if fd == target else (record[0], record[1], None)
        return result
    os.dup2 = guarded_dup2

    def guarded_popen(self, argv, *args, **kwargs):
        if not getattr(_THREAD_STATE, 'launch', 0):
            reject('children')
        return _ORIGINAL_POPEN(self, argv, *args, **kwargs)
    subprocess.Popen.__init__ = guarded_popen
    sys.addaudithook(_audit)
    _INSTALLED = True


def set_node(nodeid):
    if _CONTEXT is not None:
        _CONTEXT.nodeid = nodeid


expected_fault = expect_fault


def issue_ticket(role, argv, roots=None, expected_counts=None, expected_exit=0,
                 parent_id=None, native=False, required_roles=None, gate=False, mode=None):
    if _CONTEXT is None or role not in ROLES:
        raise ValueError('OFFLINE_CHILD_ROLE')
    c = _CONTEXT
    command = [os.fsdecode(p) for p in argv]
    if not command:
        raise ValueError('OFFLINE_CHILD_ARGV')
    issued_roots = [checked_path(p) for p in (roots or c.roots)]
    for p in issued_roots:
        if not any(p.is_relative_to(base) for base in c.roots):
            raise ValueError('OFFLINE_CHILD_ROOT')
    ticket_id = uuid.uuid4().hex
    ticket = dict(version=1, ticket_id=ticket_id, parent_id=parent_id or c.process_id,
                  root=str(c.root), lane=c.lane, role=role, run_id=c.run_id, mode=mode or c.mode,
                  report_dir=str(c.report_dir), tested_sha=c.tested_sha,
                  source_mode=c.source_mode, source_digest=c.source_digest,
                  argv=command, argv_digest=_digest(command), roots=[str(p) for p in issued_roots],
                  expected_counts=dict(expected_counts or {}), expected_exit=expected_exit,
                  native=native, required_roles=list(required_roles or []),
                  bootstrap_digest=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  policy_digest=_digest({'version': 1, 'lane': c.lane, 'roots': [str(p) for p in issued_roots]}),
                  nodeid=getattr(c, 'nodeid', os.environ.get('PYTEST_CURRENT_TEST', '').split(' (')[0]))
    if gate or role in {'lane-worker', 'worker', 'f3-control', 'command'}:
        ticket['gate'] = str(c.report_dir / 'tickets' / (ticket_id + '.gate'))
    if role == 'f3-control' and command[-1] == 'dataset-read':
        ticket['canary_path'] = str(issued_roots[0].parent / ('replica-' + ticket_id) / 'datasets/F3_SYNTHETIC_ONLY.csv')
    if role == 'f2-script':
        index = next(i for i, value in enumerate(command) if value.endswith('.py'))
        record = _registered_script(command[index], command[index + 1:])
        ticket['script_path'], ticket['script_digest'] = record['path'], record['digest']
    ticket['digest'] = _digest(ticket)
    path = c.report_dir / 'tickets' / (ticket_id + '.json')
    exclusive_json(path, ticket)
    ticket['path'] = str(path)
    c.children.append(ticket)
    return ticket


def child_env(ticket, environment=None):
    environment = dict(os.environ if environment is None else environment)
    if ticket.get('native'):
        unsafe_index = environment.get('GIT_INDEX_FILE')
        index_fixture = (ticket.get('nodeid') ==
            'tests/f2/test_notebook_sync.py::test_hook_refuses_explicit_foreign_index_without_writes'
            and ticket['argv'][1:] == ['-c', 'core.quotePath=false', 'rev-parse', '--git-path', 'index'])
        if index_fixture and unsafe_index:
            path = checked_path(unsafe_index, Path(ticket['root']) / '.f2/tmp')
            if path.name != 'foreign-index-canary' or not path.is_file():
                raise ValueError('OFFLINE_UNSAFE_INDEX_FIXTURE')
        # Git's generated hook may carry its own internal variables. Each new
        # admitted native command uses its explicit cwd, never inherited metadata.
        environment = {k: v for k, v in environment.items() if not k.startswith('GIT_')}
        environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
                           GIT_TERMINAL_PROMPT='0')
        if index_fixture and unsafe_index:
            # A single read-only identity query retains the real native
            # rejection oracle. No writer inherits this fixture override.
            environment['GIT_INDEX_FILE'] = str(path)
    environment.update(MDS_OFFLINE_ROOT=ticket['root'], MDS_OFFLINE_TICKET=ticket['path'],
        MDS_OFFLINE_REPORT_ROOT=ticket['report_dir'], MDS_OFFLINE_RUN_ID=ticket['run_id'],
        MDS_OFFLINE_TESTED_SHA=ticket['tested_sha'], MDS_OFFLINE_SOURCE_MODE=ticket['source_mode'],
        MDS_OFFLINE_SOURCE_DIGEST=ticket['source_digest'], PYTHONDONTWRITEBYTECODE='1',
        PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    return environment


def record_child_exit(ticket, exit_code, timed_out=False):
    if exit_code is None:
        return
    path = Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.exit.json')
    if path.exists():
        existing = _read_json(path)
        if (existing.get('actual_exit') != exit_code or existing.get('ticket_id') != ticket['ticket_id']
                or existing.get('run_id') != ticket['run_id']):
            raise ValueError('OFFLINE_CHILD_EXIT_CHANGED')
        return
    exclusive_json(path, dict(ticket_id=ticket['ticket_id'], run_id=ticket['run_id'],
                              actual_exit=exit_code, timed_out=bool(timed_out)))


def attach_process(process, ticket):
    """Preserve Popen class/protocol, observe only this admitted instance."""
    original_wait, original_communicate, original_poll = process.wait, process.communicate, process.poll

    def observe(code):
        if code is not None:
            record_child_exit(ticket, code)
            _close_child_job(ticket)
        return code

    def wait(*a, **kw):
        try:
            return observe(original_wait(*a, **kw))
        except subprocess.TimeoutExpired:
            _close_child_job(ticket, terminate=True)
            process.kill()
            observe(original_wait(timeout=5))
            raise

    def communicate(*a, **kw):
        try:
            result = original_communicate(*a, **kw)
        except subprocess.TimeoutExpired:
            _close_child_job(ticket, terminate=True)
            process.kill()
            original_wait(timeout=5)
            record_child_exit(ticket, process.returncode, timed_out=True)
            raise
        observe(process.returncode)
        return result

    def poll(*a, **kw):
        return observe(original_poll(*a, **kw))
    process.wait, process.communicate, process.poll = wait, communicate, poll
    return process


def _git_read_arguments(tail):
    """Only the queries used by notebook fixtures and installed pre-commit."""
    verb, arguments = tail[0], tuple(tail[1:])
    pairs = {'1year', 'all', 'count_check', 'data_gathering', 'dividends',
             'dohodru_data', 'main_tests', 'tech', 'tests', 'upload'}
    members = {name + suffix for name in pairs for suffix in ('.py', '.ipynb')}
    members.update({'tools/notebook_sync.py', '.pre-commit-config.yaml', 'pyproject.toml'})
    def member(value):
        revision, separator, path = value.partition(':')
        return bool(separator and path in members and
                    (revision in {'', 'HEAD'} or re.fullmatch('[0-9a-f]{40}', revision)))
    if verb == 'rev-parse':
        fixed = {('HEAD',), ('--show-toplevel',), ('--absolute-git-dir',),
                 ('--show-cdup',), ('--is-inside-git-dir',), ('--git-dir',),
                 ('--git-common-dir',), ('--git-path', 'index')}
        return (arguments in fixed or len(arguments) == 1 and member(arguments[0])
                or len(arguments) == 2 and arguments[0] == '--verify'
                and bool(re.fullmatch(r'(HEAD|[0-9a-fA-F]{40})\^\{commit\}', arguments[1])))
    if verb == 'symbolic-ref':
        return arguments in {('HEAD',), ('--quiet', '--short', 'HEAD')}
    if verb == 'ls-files':
        return arguments in {('--stage',), ('--stage', '-z'), ('--unmerged',), ('-z',)}
    if verb == 'show':
        reviewed_callbacks = {
            ('07945ed4ded747145b40d7a7c25e8ce97abeaa0d:tests/f2/conftest.py',),
            ('07945ed4ded747145b40d7a7c25e8ce97abeaa0d:tests/logging/conftest.py',),
            ('07945ed4ded747145b40d7a7c25e8ce97abeaa0d:tests/f2/_probe.py',),
            ('07945ed4ded747145b40d7a7c25e8ce97abeaa0d:tests/logging/_probe.py',),
        }
        return arguments in reviewed_callbacks or len(arguments) == 1 and member(arguments[0])
    if verb == 'diff':
        return arguments in {
            ('--cached', '--binary', '--no-ext-diff'),
            ('--cached', '--name-status', '--find-renames', '-z', 'HEAD', '--'),
            ('--no-ext-diff', '--ignore-submodules', '--diff-filter=A', '--name-only', '-z'),
            ('--staged', '--name-only', '--no-ext-diff', '-z', '--diff-filter=ACMRTUXB'),
            ('--quiet', '--no-ext-diff', '.pre-commit-config.yaml'),
            ('--no-ext-diff', '--no-textconv', '--ignore-submodules'),
        }
    return False


def _child_command(argv, cwd, environment):
    c = _CONTEXT
    command = [os.fsdecode(p) for p in argv]
    cwd = checked_path(cwd or os.getcwd())
    if cwd != c.root and not any(cwd.is_relative_to(p) for p in c.roots):
        raise ValueError('OFFLINE_CHILD_CWD')
    binary = shutil.which(command[0], path=environment.get('PATH')) or command[0]
    actual_binary = os.path.normcase(os.path.abspath(binary))
    git = shutil.which('git', path=environment.get('PATH'))
    if git and actual_binary == os.path.normcase(os.path.abspath(git)):
        position = 1
        configs = []
        while position + 1 < len(command) and command[position] == '-c':
            configs.append(command[position + 1]); position += 2
        tail = command[position:]
        if not tail:
            raise ValueError('OFFLINE_GIT_ARGV')
        verb = tail[0]
        readers = {'rev-parse', 'symbolic-ref', 'ls-files', 'diff', 'show'}
        writers = {'init', 'config', 'add', 'commit', 'rm', 'mv', 'update-index', 'hash-object',
                   'write-tree', 'diff-index', 'checkout', 'apply'}
        if verb not in readers | writers:
            raise ValueError('OFFLINE_GIT_VERB')
        if verb in readers and not _git_read_arguments(tail):
            raise ValueError('OFFLINE_GIT_READ_ARGV')
        if any(x in {'--ext-diff', '--textconv', '--no-index'} for x in tail):
            raise ValueError('OFFLINE_GIT_EXTERNAL_HELPER')
        if verb in {'show', 'cat-file'}:
            for argument in tail[1:]:
                lowered = argument.lower()
                if any(part in lowered.split('/') for part in ('secrets', 'datasets')) or '.env' in lowered:
                    raise ValueError('OFFLINE_GIT_PRIVATE_DATA')
        if verb in writers and not any(cwd.is_relative_to(p) for p in c.roots):
            raise ValueError('OFFLINE_GIT_ROOT')
        if verb == 'config' and ('--local' not in tail or not any(
                name in tail for name in ('user.name', 'user.email', 'commit.gpgsign', 'core.autocrlf'))):
            raise ValueError('OFFLINE_GIT_CONFIG')
        config_values = {'user.name': {'F2 disposable fixture', 'F2 foreign fixture'},
                         'user.email': {'fixture@example.invalid', 'foreign-fixture@example.invalid'},
                         'commit.gpgsign': {'false'}, 'core.autocrlf': {'false'}}
        if verb == 'config' and (len(tail) != 4 or tail[3] not in config_values.get(tail[2], set())):
            raise ValueError('OFFLINE_GIT_CONFIG')
        fixture_tuples = {
            'add': {('add', '--', '.'), ('add', '--', '.gitignore', 'sentinel.txt'),
                    ('add', '--', 'all.ipynb'), ('add', '--', 'all.ipynb', 'all.py'),
                    ('add', '--', 'all.py', 'all.ipynb'), ('add', '--intent-to-add', '--', 'all.py')},
            'rm': {('rm', '--', 'all.py'), ('rm', '--cached', '--', 'all.py')},
            'mv': {('mv', '--', 'all.py', 'renamed.py')},
            'update-index': {('update-index', '--index-info')},
            'hash-object': {('hash-object', '-w', '--stdin')},
        }
        if verb in fixture_tuples and tuple(tail) not in fixture_tuples[verb]:
            raise ValueError('OFFLINE_GIT_FIXTURE_ARGV')
        messages = {'N0 P0 baseline', 'N1 P1 after explicit restaging', 'N1 P1 without restaging',
                    'N2 P2 after explicit restaging', 'N2 P2 without restaging',
                    'ambiguous partially staged pair', 'divergent Python change',
                    'first local system hook', 'foreign baseline',
                    'invalid deleted pair', 'invalid intent pair', 'invalid missing pair',
                    'invalid renamed pair', 'invalid unmerged pair', 'invalid untracked pair',
                    'non-source metadata with CRLF working tree',
                    'partially staged hook controller', 'unsafe cache alias'}
        if verb == 'commit' and (len(tail) != 3 or tail[1] != '-m' or tail[2] not in messages):
            raise ValueError('OFFLINE_GIT_FIXTURE_COMMIT')
        if len({value.split('=', 1)[0] for value in configs}) != len(configs):
            raise ValueError('OFFLINE_GIT_CONFIG')
        for value in configs:
            if value.startswith('core.hooksPath='):
                if verb != 'commit':
                    raise ValueError('OFFLINE_GIT_CONFIG')
                hook_root = checked_path(value.split('=', 1)[1], cwd)
                if hook_root not in {cwd / '.f2/hooks', cwd / '.f2/empty-hooks'}:
                    raise ValueError('OFFLINE_GIT_HOOK_ROOT')
                if verb == 'commit' and hook_root.name == 'hooks':
                    _verify_fixture_hook(hook_root / 'pre-commit', cwd)
            elif not ((value == 'core.quotePath=false' and verb in readers)
                      or (value == 'core.autocrlf=false' and verb == 'apply')
                      or (value == 'submodule.recurse=0' and verb == 'checkout')):
                raise ValueError('OFFLINE_GIT_CONFIG')
        if verb == 'init' and tail not in [['init', '--initial-branch=codex/fixture'], ['init', '--initial-branch=codex/foreign-fixture']]:
            raise ValueError('OFFLINE_GIT_INIT')
        if verb == 'checkout' and (tail != ['checkout', '--', '.'] or 'submodule.recurse=0' not in configs):
            raise ValueError('OFFLINE_GIT_CHECKOUT')
        if verb == 'apply':
            if len(tail) != 3 or tail[1] != '--whitespace=nowarn':
                raise ValueError('OFFLINE_GIT_APPLY')
            patch = checked_path(tail[2], cwd)
            if not patch.name.startswith('patch'):
                raise ValueError('OFFLINE_GIT_APPLY')
        if verb == 'diff-index' and (len(tail) != 8 or tail[1:6] != ['--ignore-submodules', '--binary', '--exit-code', '--no-color', '--no-ext-diff'] or tail[-1] != '--' or not re.fullmatch('[0-9a-f]{40}', tail[-2])):
            raise ValueError('OFFLINE_GIT_DIFF_INDEX')
        if verb == 'write-tree' and tail != ['write-tree']:
            raise ValueError('OFFLINE_GIT_WRITE_TREE')
        # Fixed fixture paths/options only; remote/ext hooks cannot be selected.
        if any(x.startswith(('--exec-path', '--upload-pack', '--receive-pack')) for x in tail):
            raise ValueError('OFFLINE_GIT_ARGV')
        required = ['f2-hook'] if verb == 'commit' and any(v.startswith('core.hooksPath=') and not v.endswith('empty-hooks') for v in configs) else []
        return 'native-git', command, True, required
    if actual_binary != os.path.normcase(os.path.abspath(sys.executable)):
        raise ValueError('OFFLINE_CHILD_BINARY')
    if '-I' not in command or '-B' not in command:
        raise ValueError('OFFLINE_CHILD_FLAGS')
    if '-m' in command:
        module = command[command.index('-m') + 1:]
        if module == ['pre_commit', 'run', '--hook-stage', 'pre-commit']:
            return 'f2-precommit', command, False, ['f2-hook-check']
        raise ValueError('OFFLINE_CHILD_MODULE')
    scripts = [(i, p) for i, p in enumerate(command) if p.endswith('.py')]
    if len(scripts) == 2 and Path(scripts[0][1]).name == 'cold_converter_import.py' and Path(scripts[1][1]).name == 'notebook_sync.py':
        cold = checked_path(scripts[0][1], c.root / '.f2/tmp')
        copied = checked_path(scripts[1][1], c.root / '.f2/tmp')
        if scripts[1][0] != len(command) - 1 or cold.parent.name != 'tmp':
            raise ValueError('OFFLINE_CHILD_COLD_CONVERTER')
        _registered_script(cold, [str(copied)])
        return 'f2-script', command, False, []
    if len(scripts) != 1:
        raise ValueError('OFFLINE_CHILD_SCRIPT')
    position, script = scripts[0]
    script = checked_path(script if os.path.isabs(script) else cwd / script)
    tail = command[position + 1:]
    if script == c.root / 'tests/f2/_probe.py' and tail and tail[0] in {'import', 'spawn', 'guard-controls', 'preflight', 'run'}:
        modules = {'main', '1year', 'all', 'tests', 'count_check', 'main_tests',
                   'data_gathering', 'tech', 'dividends', 'dohodru_data', 'upload'}
        if tail[0] == 'import' and (len(tail) != 3 or tail[2] not in modules):
            raise ValueError('OFFLINE_F2_IMPORT_ARGV')
        if tail[0] in {'spawn', 'guard-controls'} and len(tail) != 2:
            raise ValueError('OFFLINE_F2_PROBE_ARGV')
        if tail[0] == 'spawn' and _absolute(tail[1]) != c.root:
            raise ValueError('OFFLINE_F2_SPAWN_ROOT')
        if tail[0] == 'preflight' and (len(tail) != 3 or not re.fullmatch('[A-Za-z0-9-]+', tail[2])):
            raise ValueError('OFFLINE_F2_PREFLIGHT_ARGV')
        if tail[0] == 'run':
            if len(tail) != 3:
                raise ValueError('OFFLINE_F2_RUN_ARGV')
            _registered_script(tail[1], tail[2:])
        return 'f2-preflight' if tail[0] == 'preflight' else 'f2-probe', command, False, []
    if script == c.root / 'tests/logging/_probe.py' and tail in [['normal'], ['error']]:
        return 'flog-probe', command, False, []
    if script.name == 'notebook_sync.py' and any(script.is_relative_to(p) for p in c.roots):
        pairs = {'1year', 'all', 'count_check', 'data_gathering', 'dividends',
                 'dohodru_data', 'main_tests', 'tech', 'tests', 'upload'}
        check = tail in [['check', '--worktree'], ['check', '--index']]
        check = check or (len(tail) == 3 and tail[:2] == ['check', '--ref'] and bool(re.fullmatch('[0-9a-f]{40}', tail[2])))
        sync = (len(tail) >= 5 and tail[:2] == ['sync', '--base']
                and bool(re.fullmatch('[0-9a-f]{40}', tail[2])) and tail[3] == '--pairs'
                and set(tail[4:]).issubset(pairs) and len(set(tail[4:])) == len(tail[4:]))
        if not (check or sync or tail in [['hook'], ['hook-check']]):
            raise ValueError('OFFLINE_CHILD_NOTEBOOK_ARGV')
        if script.parent.name != 'tools':
            raise ValueError('OFFLINE_FIXTURE_TOOL_PATH')
        bootstrap = fixture_bootstrap_source(c.root, lane='f2', role='f2-notebook').encode('utf-8')
        body = script.read_bytes()
        if body.count(bootstrap) != 1 or body.replace(bootstrap, b'', 1) != (c.root / 'tools/notebook_sync.py').read_bytes():
            raise ValueError('OFFLINE_FIXTURE_TOOL_BYTES')
        return ('f2-hook-check' if tail[0] == 'hook-check' else 'f2-hook' if tail[0] == 'hook' else 'f2-notebook'), command, False, []
    raise ValueError('OFFLINE_CHILD_SCRIPT')


def register_fixture_script(path, arguments):
    path = checked_path(path, _CONTEXT.root / '.f2/tmp')
    args = [os.fsdecode(p) for p in arguments]
    if (path.name != 'cold_converter_import.py' or path.parent.name != 'tmp'
            or path.parent.parent.name != '.f2' or len(args) != 1
            or _absolute(args[0]) != path.parent.parent.parent / 'tools/notebook_sync.py'):
        raise ValueError('OFFLINE_FIXTURE_SCRIPT_ROLE')
    record = dict(path=str(path), argv=args, digest=hashlib.sha256(path.read_bytes()).hexdigest())
    key = _digest([str(path), args])
    if key in _FIXTURE_SCRIPTS:
        raise ValueError('OFFLINE_FIXTURE_SCRIPT_ALREADY_REGISTERED')
    exclusive_json(_CONTEXT.report_dir / 'fixtures' / (key + '.json'),
                   dict(report_metadata(), **record))
    _FIXTURE_SCRIPTS[key] = record
    return record


def _registered_script(path, arguments):
    path = checked_path(path, _CONTEXT.root / '.f2/tmp')
    args = [os.fsdecode(p) for p in arguments]
    record = _FIXTURE_SCRIPTS.get(_digest([str(path), args]))
    if record is None or record['digest'] != hashlib.sha256(path.read_bytes()).hexdigest():
        raise ValueError('OFFLINE_FIXTURE_SCRIPT_NOT_REGISTERED')
    return record


def _verify_fixture_hook(path, repository):
    import importlib.util
    specification = importlib.util.spec_from_file_location('_f3_hook_template', _CONTEXT.root / 'tools/notebook_sync.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    expected = module.hook_template(sys.executable, root=repository).encode('utf-8')
    if checked_path(path, repository).read_bytes() != expected:
        raise ValueError('OFFLINE_FIXTURE_HOOK_BYTES')


def notify_child(argv, cwd=None, env=None, role=None):
    environment = dict(os.environ if env is None else env)
    try:
        detected, actual, native, required = _child_command(argv, cwd, environment)
    except (ValueError, TypeError):
        kind = 'children' if _CONTEXT.lane == 'flog' else 'conversion'
        if _CONTEXT.native_counts is not None:
            _CONTEXT.native_counts[kind] = _CONTEXT.native_counts.get(kind, 0) + 1
        native_notice(kind)
        raise RuntimeError('OFFLINE_CHILD_NOT_ADMITTED') from None
    if role is not None and role != detected:
        raise ValueError('OFFLINE_CHILD_ROLE_MISMATCH')
    counts = {}
    if detected == 'f2-probe' and 'guard-controls' in actual:
        counts = dict(network=1, conversion=1, writers=1, secrets=1)
    expected_exit = 1 if detected == 'flog-probe' and actual[-1] == 'error' else None
    if native:
        environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1')
    ticket = issue_ticket(detected, actual, expected_counts=counts, expected_exit=expected_exit,
                          native=native, required_roles=required)
    if detected == 'f2-precommit':
        actual = [sys.executable, '-I', '-B', '-X', 'utf8', str(_CONTEXT.root / 'tools/offline_guard.py'),
                  '--role', 'f2-precommit', '--ticket', ticket['path']]
    return actual, child_env(ticket, environment), ticket


def issue_spawn(role, args_digest, roots=None, expected_exit=0):
    return issue_ticket(role, ['multiprocessing.spawn', args_digest], roots=roots,
                        expected_exit=expected_exit)


def prepare_spawn(process, role):
    target = getattr(process, '_target', None)
    name = getattr(target, '__name__', '')
    args = list(getattr(process, '_args', ()))
    if (role, name) not in {('f2-spawn', 'spawn_one'), ('flog-worker', 'worker_entry')}:
        raise ValueError('OFFLINE_SPAWN_TARGET')
    if role == 'f2-spawn':
        if len(args) != 2 or _absolute(args[0]) != _CONTEXT.root or not hasattr(args[1], 'send'):
            raise ValueError('OFFLINE_F2_SPAWN_ARGS')
    else:
        if len(args) != 3 or not isinstance(args[0], dict):
            raise ValueError('OFFLINE_FLOG_SPAWN_ARGS')
        mode, count = args[1:]
        if not ((mode == 'normal' and count in {2, 100})
                or (mode in {'before', 'no_final', 'after'} and count == 2)
                or (mode in {'race:' + level + ':' + timing for level in ('INFO', 'ERROR', 'FLUSH')
                             for timing in ('completion', 'timeout')} and count == 0)):
            raise ValueError('OFFLINE_FLOG_SPAWN_ARGS')
    ticket = issue_spawn(role, _digest([name, repr(args[1:])]), expected_exit=None)
    if role == 'f2-spawn':
        args.append(ticket['path'])
    else:
        bootstrap = dict(args[0])
        bootstrap['_f3_guard_ticket'] = ticket['path']
        args[0] = bootstrap
    process._args = tuple(args)
    return ticket


def bind_spawn_ticket(ticket):
    if isinstance(ticket, dict):
        ticket = ticket['path']
    os.environ['MDS_OFFLINE_TICKET'] = str(ticket)


def attach_spawn(process, ticket):
    original_join = process.join
    def join(*a, **kw):
        result = original_join(*a, **kw)
        record_child_exit(ticket, process.exitcode)
        if process.exitcode is not None:
            _close_child_job(ticket)
        return result
    process.join = join
    return process


def fixture_bootstrap(root, lane='f2', role='f2-notebook'):
    """A disposable tool copy uses only its issued parent slot, never new policy."""
    root = checked_path(root)
    if len(sys.argv) > 1 and sys.argv[1] in {'hook', 'hook-check'}:
        role = 'f2-hook' if sys.argv[1] == 'hook' else 'f2-hook-check'
    parent = _load_ticket(root, implicit=True)
    if parent is not None and parent['role'] in {'native-git', 'f2-precommit'} and role != parent['role']:
        if role not in parent.get('required_roles', []):
            raise ValueError('OFFLINE_IMPLICIT_CHILD_ROLE')
        # Admission is allocated by the fixed native-Git hook slot; no arbitrary role.
        child = {k: v for k, v in parent.items() if k not in {'path', 'digest', 'gate'}}
        child.update(ticket_id=uuid.uuid4().hex, parent_id=parent['ticket_id'], role=role,
                     native=False, required_roles=[], expected_exit=None,
                     owning_exit=parent['ticket_id'],
                     argv=list(sys.orig_argv), argv_digest=_digest(list(sys.orig_argv)))
        child['digest'] = _digest(child)
        path = Path(parent['report_dir']) / 'tickets' / (child['ticket_id'] + '.json')
        exclusive_json(path, child)
        os.environ['MDS_OFFLINE_TICKET'] = str(path)
    return ensure_context(root, lane=lane, role=role, install=False)


def fixture_bootstrap_source(root, lane='f2', role='f2-notebook'):
    # Inserted into the disposable copy only; deleting it restores source bytes.
    return ("\n# F3 test-only early bootstrap\n"
            "import importlib.util as _f3_importlib, sys as _f3_sys\n"
            "_f3_spec = _f3_importlib.spec_from_file_location('mds_offline_guard', " + repr(str(Path(root) / 'tools/offline_guard.py')) + ")\n"
            "_f3_guard = _f3_sys.modules.get('mds_offline_guard')\n"
            "if _f3_guard is None:\n"
            "    _f3_guard = _f3_importlib.module_from_spec(_f3_spec)\n"
            "    _f3_sys.modules['mds_offline_guard'] = _f3_guard\n"
            "    _f3_spec.loader.exec_module(_f3_guard)\n"
            "_f3_context = _f3_guard.fixture_bootstrap(" + repr(str(root)) + ", " + repr(lane) + ", " + repr(role) + ")\n"
            "_f3_probe_spec = _f3_importlib.spec_from_file_location('_f3_native_probe', " + repr(str(Path(root) / 'tests/f2/_probe.py')) + ")\n"
            "_f3_probe = _f3_importlib.module_from_spec(_f3_probe_spec)\n"
            "_f3_probe_spec.loader.exec_module(_f3_probe)\n"
            "if _f3_context.native_counts is None:\n"
            "    _f3_probe.install_guards(" + repr(str(root)) + ", 'harness')\n"
            "# F3 test-only bootstrap end\n\n")


def _job_create():
    if sys.platform != 'win32':
        return None
    from ctypes import wintypes
    class BasicLimit(ctypes.Structure):
        _fields_ = [('process_time', ctypes.c_int64), ('job_time', ctypes.c_int64),
                    ('flags', wintypes.DWORD), ('min_working', ctypes.c_size_t),
                    ('max_working', ctypes.c_size_t), ('active_limit', wintypes.DWORD),
                    ('affinity', ctypes.c_size_t), ('priority', wintypes.DWORD),
                    ('scheduling', wintypes.DWORD)]
    class IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in
                    ('read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes')]
    class ExtendedLimit(ctypes.Structure):
        _fields_ = [('basic', BasicLimit), ('io', IoCounters),
                    ('process_memory', ctypes.c_size_t), ('job_memory', ctypes.c_size_t),
                    ('peak_process_memory', ctypes.c_size_t), ('peak_job_memory', ctypes.c_size_t)]
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.ResumeThread.argtypes = [wintypes.HANDLE]
    kernel.ResumeThread.restype = wintypes.DWORD
    kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.CreateJobObjectW(None, None)
    if not handle:
        raise OSError('OFFLINE_JOB_CREATE_FAILED')
    limits = ExtendedLimit()
    limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE; no breakaway.
    if not kernel.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        kernel.CloseHandle(handle)
        raise OSError('OFFLINE_JOB_LIMIT_FAILED')
    return kernel, handle


def _job_assign(job, process):
    if job is not None and not job[0].AssignProcessToJobObject(job[1], int(process._handle)):
        raise OSError('OFFLINE_JOB_ASSIGNMENT_FAILED')


def _job_close(job, terminate=False):
    if job is not None:
        if terminate:
            job[0].TerminateJobObject(job[1], 124)
        job[0].CloseHandle(job[1])


def begin_native_launch(ticket):
    pending = getattr(_THREAD_STATE, 'pending', None)
    if pending is None:
        pending = _THREAD_STATE.pending = []
    pending.append(ticket)


def end_native_launch():
    pending = getattr(_THREAD_STATE, 'pending', [])
    if pending:
        pending.pop()


def _close_child_job(ticket, terminate=False):
    job = _CHILD_JOBS.pop(ticket['ticket_id'], None)
    _job_close(job, terminate)


def native_create_process(original, args, kwargs):
    """Assign the admitted native process before its thread can create children."""
    pending = getattr(_THREAD_STATE, 'pending', [])
    if not pending or sys.platform != 'win32':
        return original(*args, **kwargs)
    ticket = pending[-1]
    parameters = list(args)
    options = dict(kwargs)
    if len(parameters) > 5:
        parameters[5] |= 0x4  # CREATE_SUSPENDED
    elif 'creation_flags' in options:
        options['creation_flags'] |= 0x4
    else:
        raise ValueError('OFFLINE_CREATE_PROCESS_SIGNATURE')
    job = _job_create()
    result = None
    try:
        result = original(*parameters, **options)
        process_handle, thread_handle = result[:2]
        if not job[0].AssignProcessToJobObject(job[1], int(process_handle)):
            raise OSError('OFFLINE_JOB_ASSIGNMENT_FAILED')
        _CHILD_JOBS[ticket['ticket_id']] = job
        if job[0].ResumeThread(int(thread_handle)) == 0xffffffff:
            raise OSError('OFFLINE_JOB_RESUME_FAILED')
        if ticket.get('gate'):
            exclusive_json(ticket['gate'], {'released': True})
        return result
    except BaseException:
        if result is not None:
            job[0].TerminateProcess(int(result[0]), 124)
            import _winapi
            _winapi.CloseHandle(result[1])
            _winapi.CloseHandle(result[0])
        _CHILD_JOBS.pop(ticket['ticket_id'], None)
        _job_close(job, terminate=True)
        raise


CONTROL_KINDS = {
    'normal': None, 'network': 'network', 'dns': 'network', 'write': 'writers',
    'sleep': 'sleep', 'caught-network': 'network', 'caught-sleep': 'sleep',
    'secrets': 'secrets', 'dataset-read': 'dataset_reads', 'extra-child': 'children',
    'missing-report': None, 'corrupt-report': None, 'foreign-report': None,
    'stale-report': None, 'duplicate-report': None, 'timeout': None,
    'git-diff-output-equals': 'conversion', 'git-diff-output-separated': 'conversion',
    'git-symbolic-ref-mutates': 'conversion',
    **{kind + '-' + target: 'writers' if kind == 'truncate' else 'descriptors'
       for kind in ('truncate', 'ftruncate') for target in ('external', 'source', 'previous', 'alias')},
    **{'source-identity-' + case: None for case in ('ftp', 'nested', 'alias', 'normal')},
}


def _control_scope(case, node):
    if case.startswith(('truncate-', 'ftruncate-')):
        return node.split('[', 1)[0] == 'tests/shared/test_guard.py::test_truncate_in_permitted_child_preserves_forbidden_canary'
    if case.startswith('source-identity-'):
        return node.split('[', 1)[0] == 'tests/shared/test_preparation.py::test_source_identity_preflight_checks_before_content_read'
    return node.startswith('tests/shared/test_children.py::')


def _control_case(argv):
    command = [os.fsdecode(a) for a in argv]
    script = str(_CONTEXT.root / 'tools/offline_guard.py')
    if len(command) == 8 and command[1:5] == ['-I', '-B', '-X', 'utf8'] and command[5:7] == [script, 'control'] and command[7] in CONTROL_KINDS:
        return command[7]
    return None


def _allowed_command(argv, role):
    command = [os.fsdecode(a) for a in argv]
    if role == 'f3-control':
        return _control_case(command) is not None
    if role in {'worker', 'lane-worker'}:
        return (len(command) >= 8 and command[1:5] == ['-I', '-B', '-X', 'utf8']
                and command[5] == str(_CONTEXT.root / 'tools/offline_tests.py')
                and command[6] == '--execute-lane' and command[7] in LANES)
    if role == 'command':
        # The root launcher additionally chooses a named fixed command profile.
        if os.path.normcase(os.path.abspath(command[0])) == os.path.normcase(os.path.abspath(sys.executable)):
            tail = command[1:]
            while tail and tail[0] in {'-B', '-I'}:
                tail = tail[1:]
            if tail[:2] == ['-X', 'utf8']:
                tail = tail[2:]
            if tail and tail[0] == str(_CONTEXT.root / 'main_tests.py'):
                return tail[1:] == ['--allow-live-diagnostics', '--profile', 'live-read']
            if tail and tail[0] == str(_CONTEXT.root / 'tests.py'):
                return (len(tail) == 6 and tail[1:5] == ['--allow-live-diagnostics', '--profile', 'staging', '--output-root']
                        and _absolute(tail[5]).is_relative_to(_CONTEXT.root / '.f3/tmp'))
            if tail[:2] == ['-m', 'unittest']:
                return tail[2:] in [['discover'], ['main_tests']]
            if tail[:2] == ['-m', 'pytest']:
                tail = tail[2:]
            else:
                return False
        else:
            executable = Path(command[0])
            if executable.name.lower() not in {'pytest', 'pytest.exe'} or executable.parent != Path(sys.executable).parent:
                return False
            tail = command[1:]
        known = {'-q', '--collect-only', 'tests.py', 'main_tests.py', '-m', 'live_read', 'staging', 'offline', 'not offline', 'F3_UNREGISTERED_MARKER'}
        return all(p in known or (_absolute(p).is_relative_to(_CONTEXT.root / '.f3/tmp') and p.endswith('.py')) for p in tail)
    return False


def run_child(argv, role='f3-control', roots=None, timeout=30, expected_counts=None,
              expected_exit=0, ticket=None, env=None, cwd=None, input=None, **kwargs):
    """Run one fixed role, then return the reconciled outward verdict."""
    if ticket is None:
        if not _allowed_command(argv, role):
            reject('children')
        case = _control_case(argv) if role == 'f3-control' else None
        ticket = issue_ticket(role, argv, roots=roots, expected_counts=expected_counts,
                              expected_exit=expected_exit, gate=True)
        if case and case != 'normal':
            node = getattr(_CONTEXT, 'nodeid', os.environ.get('PYTEST_CURRENT_TEST', '').split(' (')[0])
            if not _control_scope(case, node):
                raise ValueError('OFFLINE_CONTROL_SCOPE')
            # No ticket rewrite: a separate exclusive declared-fixture record.
            exclusive_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.control.json'),
                           dict(ticket_id=ticket['ticket_id'], case=case, nodeid=node,
                                run_id=ticket['run_id'], expected_kind=CONTROL_KINDS[case]))
    elif ticket.get('role') != role or ticket.get('argv_digest') != _digest([os.fsdecode(a) for a in argv]):
        raise ValueError('OFFLINE_CHILD_TICKET_MISMATCH')
    if role == 'native-git':
        detected, _, native, _ = _child_command(argv, cwd or _CONTEXT.root, dict(os.environ if env is None else env))
        if detected != role or not native or not ticket.get('native'):
            raise ValueError('OFFLINE_CHILD_NATIVE_ROLE')
    environment = child_env(ticket, env)
    if ticket.get('canary_path'):
        canary = checked_path(ticket['canary_path'], _CONTEXT.root / '.f3/tmp')
        canary.parent.mkdir(parents=True, exist_ok=False)
        canary.write_bytes(b'F3_SYNTHETIC_DATA_CANARY\n')
    job, process, timed_out = None, None, False
    stdout, stderr = b'', b''
    try:
        _THREAD_STATE.launch = getattr(_THREAD_STATE, 'launch', 0) + 1
        original_create = None
        try:
            if sys.platform == 'win32':
                import _winapi
                original_create = _winapi.CreateProcess
                begin_native_launch(ticket)
                _winapi.CreateProcess = lambda *a, **k: native_create_process(original_create, a, k)
            process = subprocess.Popen(argv, cwd=cwd or _CONTEXT.root, env=environment,
                stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        finally:
            if original_create is not None:
                _winapi.CreateProcess = original_create
                end_native_launch()
            _THREAD_STATE.launch -= 1
        job = _CHILD_JOBS.pop(ticket['ticket_id'], None)
        if ticket.get('gate') and not Path(ticket['gate']).exists():
            exclusive_json(ticket['gate'], {'released': True})
        _THREAD_STATE.controller = getattr(_THREAD_STATE, 'controller', 0) + 1
        try:
            stdout, stderr = process.communicate(input=input, timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            if job is not None:
                job[0].TerminateJobObject(job[1], 124)
            else:
                process.kill()
            stdout, stderr = process.communicate(timeout=5)
        finally:
            _THREAD_STATE.controller -= 1
        record_child_exit(ticket, process.returncode, timed_out)
    except BaseException:
        if process is not None and process.poll() is None:
            if job is not None:
                job[0].TerminateJobObject(job[1], 124)
            else:
                process.kill()
            process.wait(timeout=5)
        raise
    finally:
        _job_close(job, terminate=timed_out)
        remaining = _CHILD_JOBS.pop(ticket['ticket_id'], None)
        _job_close(remaining, terminate=True)
    errors, final = _validate_ticket(ticket)
    outward = 86 if errors else process.returncode
    result = subprocess.CompletedProcess(list(argv), outward, stdout, stderr)
    result.actual_exit = process.returncode
    result.report_errors = errors
    result.validated = not errors
    result.report = final
    result.ticket = ticket
    result.timed_out = timed_out
    return result


def _validate_ticket(ticket, require_exit=True):
    errors = []
    directory = Path(ticket['report_dir'])
    identifier = ticket['ticket_id']
    expected = {key: ticket[key] for key in ('run_id', 'lane', 'mode', 'tested_sha', 'source_mode', 'source_digest', 'role', 'parent_id', 'argv_digest', 'bootstrap_digest', 'policy_digest')}
    final = None
    if ticket.get('native'):
        path = directory / 'tickets' / (identifier + '.exit.json')
        if not path.is_file():
            errors.append('MISSING_NATIVE_EXIT')
        else:
            try:
                observed = _read_json(path)
                if (observed.get('ticket_id') != identifier or observed.get('run_id') != ticket['run_id']
                        or type(observed.get('actual_exit')) is not int or type(observed.get('timed_out')) is not bool):
                    errors.append('FOREIGN_NATIVE_EXIT')
                if ticket.get('expected_exit') is not None and observed.get('actual_exit') != ticket['expected_exit']:
                    errors.append('UNEXPECTED_EXIT')
                if observed.get('timed_out'):
                    errors.append('TIMEOUT')
            except (ValueError, OSError, UnicodeError):
                errors.append('CORRUPT_NATIVE_EXIT')
        return errors, None
    for suffix in ('start', 'final'):
        path = directory / 'processes' / (identifier + '.' + suffix + '.json')
        try:
            obj = _read_json(path)
        except FileNotFoundError:
            errors.append('MISSING_' + suffix.upper() + '_REPORT'); continue
        except (ValueError, OSError, UnicodeError):
            errors.append('CORRUPT_' + suffix.upper() + '_REPORT'); continue
        if any(obj.get(k) != v for k, v in expected.items()) or obj.get('ticket_id') != identifier:
            errors.append('FOREIGN_' + suffix.upper() + '_REPORT')
        if suffix == 'final':
            final = obj
        if len(list((directory / 'processes').glob(identifier + '.' + suffix + '*.json'))) != 1:
            errors.append('DUPLICATE_' + suffix.upper() + '_REPORT')
    journals = []
    journal_counts = {}
    journal_nodes = {}
    try:
        for path in (directory / 'processes').glob(identifier + '.violation.*.json'):
            record = _read_json(path)
            if (record.get('ticket_id') != identifier or record.get('run_id') != ticket['run_id']
                    or record.get('tested_sha') != ticket['tested_sha'] or not isinstance(record.get('kind'), str)
                    or type(record.get('sequence')) is not int
                    or path.name != identifier + '.violation.' + str(record['sequence']) + '.json'):
                raise ValueError('OFFLINE_VIOLATION_IDENTITY')
            journals.append(record)
            kind = record['kind']
            journal_counts[kind] = journal_counts.get(kind, 0) + 1
            node = record.get('nodeid', '')
            group = journal_nodes.setdefault(node, {})
            group[kind] = group.get(kind, 0) + 1
        if sorted(record['sequence'] for record in journals) != list(range(1, len(journals) + 1)):
            raise ValueError('OFFLINE_VIOLATION_SEQUENCE')
    except (ValueError, OSError, KeyError):
        errors.append('CORRUPT_VIOLATION_JOURNAL')
    if final is None and journal_counts:
        errors.append('UNEXPECTED_COUNTERS')
    if final is not None:
        counts = {k: v for k, v in final.get('counters', {}).items() if v}
        wanted = {k: v for k, v in ticket.get('expected_counts', {}).items() if v}
        if ticket['role'] in {'lane-worker', 'worker', 'command'}:
            declared = {}
            declared_nodes = {}
            try:
                for path in (directory / 'processes').glob(identifier + '.expected.*.json'):
                    record = _read_json(path)
                    if (record.get('ticket_id') != identifier or record.get('run_id') != ticket['run_id']
                            or record.get('tested_sha') != ticket['tested_sha']):
                        raise ValueError('OFFLINE_FAULT_LEDGER')
                    node = record.get('nodeid', '')
                    delta = record.get('counts')
                    if not isinstance(delta, dict) or not _fault_scope(node, delta):
                        raise ValueError('OFFLINE_FAULT_LEDGER_SCOPE')
                    for kind, count in record['counts'].items():
                        if type(count) is not int or count <= 0:
                            raise ValueError('OFFLINE_FAULT_LEDGER_COUNT')
                        declared[kind] = declared.get(kind, 0) + count
                        group = declared_nodes.setdefault(node, {})
                        group[kind] = group.get(kind, 0) + count
                wanted = declared
                if declared != {k: v for k, v in final.get('expected_counts', {}).items() if v}:
                    errors.append('EXPECTED_FAULT_LEDGER_MISMATCH')
                if counts == declared and declared_nodes != journal_nodes:
                    errors.append('EXPECTED_FAULT_NODE_MISMATCH')
            except (ValueError, KeyError, OSError, TypeError):
                errors.append('CORRUPT_EXPECTED_FAULT_LEDGER')
        if counts != wanted:
            errors.append('UNEXPECTED_COUNTERS')
        if final.get('native_guard') and {k: v for k, v in final.get('native_counters', {}).items() if v} != counts:
            errors.append('NATIVE_COUNTER_MISMATCH')
        if final.get('report_error'):
            errors.append('REPORT_WRITE_FAILED')
        try:
            if len(journals) != final.get('violations') or journal_counts != counts:
                errors.append('VIOLATION_JOURNAL_MISMATCH')
        except (ValueError, OSError):
            errors.append('CORRUPT_VIOLATION_JOURNAL')
    exit_path = directory / 'tickets' / (identifier + '.exit.json')
    if exit_path.is_file():
        observed = _read_json(exit_path)
        if (observed.get('ticket_id') != identifier or observed.get('run_id') != ticket['run_id']
                or type(observed.get('actual_exit')) is not int or type(observed.get('timed_out')) is not bool):
            errors.append('FOREIGN_CHILD_EXIT')
        if ticket.get('expected_exit') is not None and observed.get('actual_exit') != ticket['expected_exit']:
            errors.append('UNEXPECTED_EXIT')
        if observed.get('timed_out'):
            errors.append('TIMEOUT')
        if final is not None and final.get('exit_code') is not None and final['exit_code'] != observed.get('actual_exit'):
            errors.append('EXIT_REPORT_MISMATCH')
    elif require_exit:
        # Only the two generated hook slots use their owning process's exit.
        owner = ticket.get('owning_exit')
        owner_roles = {'f2-hook': 'native-git', 'f2-hook-check': 'f2-precommit'}
        try:
            owning_ticket = _read_json(directory / 'tickets' / (owner + '.json')) if owner else {}
            owning_exit = _read_json(directory / 'tickets' / (owner + '.exit.json')) if owner else {}
            implicit = (owner == ticket.get('parent_id')
                        and owning_ticket.get('role') == owner_roles.get(ticket['role'])
                        and ticket['role'] in owner_roles
                        and owning_ticket.get('run_id') == ticket['run_id']
                        and owning_exit.get('ticket_id') == owner
                        and owning_exit.get('run_id') == ticket['run_id']
                        and type(owning_exit.get('actual_exit')) is int)
        except (ValueError, OSError, TypeError):
            implicit = False
        if not implicit:
            errors.append('MISSING_CHILD_EXIT')
    return errors, final


def _expected_control(ticket, errors, final):
    path = Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.control.json')
    command_path = Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.command-control.json')
    if ticket['role'] == 'command' and command_path.is_file():
        record = _read_json(command_path)
        choices = {'collection-network': 'network', 'collection-sleep': 'sleep',
                   'collection-write': 'writers', 'collection-secret': 'secrets',
                   'collection-data': 'dataset_reads'}
        kind = choices.get(record.get('profile'))
        return (kind is not None and record.get('expected_kind') == kind
                and record.get('nodeid', '').startswith('tests/shared/test_collection.py::')
                and record.get('run_id') == ticket['run_id'] and record.get('ticket_id') == ticket['ticket_id']
                and set(errors) == {'UNEXPECTED_COUNTERS'} and final is not None
                and {k: v for k, v in final.get('counters', {}).items() if v} == {kind: 1}
                and final.get('exit_code') == 2)
    if not path.is_file():
        return False
    record = _read_json(path)
    case = record.get('case')
    if (ticket['role'] != 'f3-control' or case not in CONTROL_KINDS
            or not ticket.get('argv') or case != ticket['argv'][-1]
            or record.get('expected_kind') != CONTROL_KINDS.get(case)
            or not _control_scope(case, record.get('nodeid', ''))
            or record.get('run_id') != ticket['run_id'] or record.get('ticket_id') != ticket['ticket_id']):
        return False
    kind = CONTROL_KINDS[case]
    if kind:
        return (set(errors) == {'UNEXPECTED_COUNTERS'} and final is not None
                and {k: v for k, v in final.get('counters', {}).items() if v} == {kind: 1})
    permitted = {
        'missing-report': {'MISSING_FINAL_REPORT'},
        'corrupt-report': {'CORRUPT_FINAL_REPORT'},
        'foreign-report': {'FOREIGN_FINAL_REPORT'},
        'stale-report': {'FOREIGN_FINAL_REPORT'},
        'duplicate-report': {'DUPLICATE_FINAL_REPORT'},
        'timeout': {'MISSING_FINAL_REPORT', 'TIMEOUT', 'UNEXPECTED_EXIT'},
    }
    return set(errors) == permitted.get(case, set()) and bool(errors)


def validate_reports(context=None):
    """Reconcile issued graph; old/extra reports cannot stand in for this run."""
    c = context or _CONTEXT
    if c is None:
        return ['CONTEXT_MISSING']
    errors = []
    tickets = {}
    for path in (c.report_dir / 'tickets').glob('*.json'):
        if path.name.endswith(('.exit.json', '.control.json', '.command-control.json')):
            continue
        try:
            ticket = _read_json(path)
            if ticket.get('run_id') != c.run_id or ticket.get('tested_sha') != c.tested_sha or ticket.get('source_digest') != c.source_digest:
                errors.append('FOREIGN_TICKET'); continue
            if ticket.get('digest') != _digest({k: v for k, v in ticket.items() if k != 'digest'}):
                errors.append('CORRUPT_TICKET'); continue
            tickets[ticket['ticket_id']] = ticket
        except (ValueError, OSError, KeyError):
            errors.append('CORRUPT_TICKET')
    all_known = {c.process_id, *tickets}
    # The sole ticketless top-level issuer has a current run-bound start. A
    # nested worker's ticket explicitly names it, so it is a valid ancestor.
    for path in (c.report_dir / 'processes').glob('*.start.json'):
        try:
            record = _read_json(path)
        except (ValueError, OSError):
            continue
        if (record.get('role') == 'supervisor' and record.get('parent_id') is None
                and record.get('run_id') == c.run_id and record.get('tested_sha') == c.tested_sha
                and record.get('source_digest') == c.source_digest):
            all_known.add(record.get('ticket_id'))
    # A child session validates its own subtree; the outer supervisor owns the
    # full graph. Ancestor/sibling records are not foreign to the shared run.
    selected = {c.process_id}
    changed = True
    while changed:
        before = len(selected)
        selected.update(identifier for identifier, t in tickets.items() if t['parent_id'] in selected)
        changed = len(selected) != before
    if c.role == 'supervisor' and any(identifier not in selected for identifier in tickets):
        errors.append('UNCLAIMED_CHILD')
    for ticket in tickets.values():
        if ticket['ticket_id'] not in selected:
            continue
        if ticket['ticket_id'] == c.process_id and not c.finished:
            # The session checks children before it emits its own final. Its
            # external parent still requires that final after actual exit.
            continue
        if ticket['parent_id'] not in all_known:
            errors.append('UNCLAIMED_CHILD')
        # This process cannot observe its own actual OS exit yet. Its external
        # parent uses the default mandatory exit check after wait/communicate.
        issues, final = _validate_ticket(ticket, require_exit=ticket['ticket_id'] != c.process_id)
        if issues and not _expected_control(ticket, issues, final):
            errors.extend(issues)
        # A successful hook-bearing native command must prove its Python hook.
        exit_path = c.report_dir / 'tickets' / (ticket['ticket_id'] + '.exit.json')
        observed = _read_json(exit_path) if exit_path.is_file() else {}
        if observed.get('actual_exit') == 0:
            children = [t['role'] for t in tickets.values() if t['parent_id'] == ticket['ticket_id']]
            for role in ticket.get('required_roles', []):
                if children.count(role) != 1:
                    errors.append('MISSING_OR_EXTRA_REQUIRED_CHILD')
    for path in (c.report_dir / 'processes').glob('*.start.json'):
        if path.name.split('.', 1)[0] not in all_known:
            # Supervisor start is the one parent without a ticket.
            record = _read_json(path)
            if record.get('ticket_id') == c.parent_id and record.get('run_id') == c.run_id and record.get('tested_sha') == c.tested_sha:
                continue
            errors.append('UNEXPECTED_PROCESS_REPORT')
    for pattern in ('*.final*.json', '*.violation.*.json'):
        for path in (c.report_dir / 'processes').glob(pattern):
            if path.name.split('.', 1)[0] not in all_known:
                errors.append('UNEXPECTED_PROCESS_REPORT')
    final_path = c.report_dir / 'processes' / (c.process_id + '.final.json')
    if final_path.is_file():
        own = _read_json(final_path)
        actual = {k: v for k, v in own.get('counters', {}).items() if v}
        wanted = {k: v for k, v in own.get('expected_counts', {}).items() if v}
        own_ticket = tickets.get(c.process_id)
        scoped_rejection = (own_ticket is not None
                            and _expected_control(own_ticket, ['UNEXPECTED_COUNTERS'], own))
        if actual != wanted and not scoped_rejection:
            errors.append('UNEXPECTED_PARENT_COUNTERS')
    elif c.finished:
        errors.append('MISSING_PARENT_FINAL')
    return sorted(set(errors))


def _control(case):
    root = Path(os.environ['MDS_OFFLINE_ROOT'])
    ticket = _load_ticket(root)
    if ticket is None or ticket['role'] != 'f3-control' or case not in CONTROL_KINDS:
        raise ValueError('OFFLINE_CONTROL_TICKET')
    module = ('test_guard.py' if case.startswith(('truncate-', 'ftruncate-')) else
              'test_preparation.py' if case.startswith('source-identity-') else
              'test_children.py' if case.startswith('git-') else None)
    if module:
        if not _control_scope(case, ticket.get('nodeid', '')):
            raise ValueError('OFFLINE_CONTROL_SCOPE')
        fixture = checked_path(root / 'tests/shared' / module, root)
        return runpy.run_path(str(fixture))['_review_control'](case, root, ticket)
    # Only synthetic bytes, seeded in the issued disposable root before guard.
    temporary = checked_path(ticket['roots'][0])
    temporary.mkdir(parents=True, exist_ok=True)
    secret = temporary / 'secrets' / 'canary.json'
    if case == 'secrets':
        secret.parent.mkdir()
        secret.write_bytes(b'F3_SYNTHETIC_SECRET_DO_NOT_PRINT')
    c = ensure_context(root, role='f3-control')
    if case == 'timeout':
        import threading
        threading.Event().wait(60)
    if case == 'missing-report':
        os._exit(0)
    if case in {'corrupt-report', 'foreign-report', 'stale-report'}:
        target = c.report_dir / 'processes' / (c.process_id + '.final.json')
        if case == 'corrupt-report':
            with _internal():
                with _ORIGINAL_OPEN(target, 'xb') as stream:
                    stream.write(b'{')
        else:
            obj = dict(report_metadata(), exit_code=0, counters={}, native_counters={},
                       expected_counts={}, violations=0, children=[], report_error=False)
            obj['run_id' if case == 'foreign-report' else 'tested_sha'] = '0' * (32 if case == 'foreign-report' else 40)
            exclusive_json(target, obj)
        os._exit(0)
    if case == 'duplicate-report':
        obj = finish(0)
        exclusive_json(c.report_dir / 'processes' / (c.process_id + '.final.duplicate.json'), obj)
        os._exit(0)
    action = {
        'network': lambda: socket.create_connection(('127.0.0.1', 9)),
        'caught-network': lambda: socket.create_connection(('127.0.0.1', 9)),
        'dns': lambda: socket.getaddrinfo('F3_INVALID_DNS_CONTROL.invalid', 9),
        'write': lambda: (root / 'F3_FORBIDDEN_WRITE').write_bytes(b'forbidden'),
        'sleep': lambda: time.sleep(60),
        'caught-sleep': lambda: time.sleep(60),
        'secrets': lambda: secret.read_bytes(),
        'dataset-read': lambda: Path(ticket['canary_path']).read_bytes(),
        'extra-child': lambda: subprocess.run([sys.executable, '-c', 'raise SystemExit(0)']),
    }.get(case)
    if action:
        try:
            action()
        except OfflineViolation:
            pass
    finish(0)
    return 0


def main():
    if len(sys.argv) == 3 and sys.argv[1] == 'control':
        return _control(sys.argv[2])
    if len(sys.argv) == 5 and sys.argv[1:3] == ['--role', 'f2-precommit'] and sys.argv[3] == '--ticket':
        os.environ['MDS_OFFLINE_TICKET'] = sys.argv[4]
        root = Path(os.environ['MDS_OFFLINE_ROOT'])
        c = ensure_context(root, lane='f2', role='f2-precommit', install=False)
        sys.path.insert(0, str(root / 'tests/f2'))
        import importlib.util
        spec = importlib.util.spec_from_file_location('_f3_native_precommit', root / 'tests/f2/_probe.py')
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        probe.install_guards(root, 'harness')
        sys.argv = ['pre_commit', 'run', '--hook-stage', 'pre-commit']
        code = 1
        try:
            runpy.run_module('pre_commit', run_name='__main__', alter_sys=True)
            code = 0
        except SystemExit as error:
            code = error.code if isinstance(error.code, int) else 1
        finally:
            finish(code)
        return code
    raise ValueError('OFFLINE_ENTRYPOINT_NOT_ADMITTED')


if __name__ == '__main__':
    raise SystemExit(main())
