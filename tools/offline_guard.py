"""Test-only, stdlib guard and evidence for the fixed offline lanes.

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
import queue
import re
import runpy
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
from types import CodeType, SimpleNamespace
import uuid
import weakref


sys.modules.setdefault('mds_offline_guard', sys.modules[__name__])
LANES = {'f1': '.f1', 'f2': '.f2', 'flog': '.f-log', 'f3': '.f3', 'd1': '.f3/d1',
         'd2': '.f3/d2'}
ROLES = {'entry', 'worker', 'supervisor', 'lane-worker', 'notebook-check', 'f2-probe', 'f2-preflight',
         'f2-script', 'f2-notebook', 'f2-hook', 'f2-precommit',
         'f2-hook-check', 'f2-spawn', 'flog-probe', 'flog-worker',
         'native-git', 'f3-control', 'command', 'd1-worker', 'd2-controller',
         'd2-worker', 'd2-baseline-worker', 'd2-lock-worker'}
_CONTEXT = None
_ORIGINAL_SLEEP = time.sleep
_ORIGINAL_POPEN = subprocess.Popen.__init__
_ORIGINAL_OPEN = open
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_FDOPEN = os.fdopen
_ORIGINAL_OS_CLOSE = os.close
_ORIGINAL_FSTAT = os.fstat
_ORIGINAL_LSTAT = os.lstat
_ORIGINAL_STAT = os.stat
_ORIGINAL_WINDLL = getattr(ctypes, 'WinDLL', None)
_OWN_FDS = {0, 1, 2}
_WRITABLE_FDS = {}
_INSTALLED = False
_THREAD_STATE = threading.local()
_CHILD_JOBS = {}
_FIXTURE_SCRIPTS = {}
_D1_NATIVE_HANDLES = {}
_D1_MUTEX_NAMES = set()
_D1_CONTROLLER_ACTIVE = False
_D2_REQUESTS = {}
_D2_PROCESS_TICKETS = weakref.WeakKeyDictionary()
_D2_NATIVE_HANDLES = {}
_D2_CASE_ACTIVE = None
if sys.platform == 'win32':
    import _winapi as _d2_winapi
    import msvcrt as _d2_msvcrt
    _D2_WINAPI_ORIGINAL = {name: getattr(_d2_winapi, name) for name in
        ('CreateProcess', 'DuplicateHandle', 'WaitForSingleObject', 'GetExitCodeProcess', 'TerminateProcess')}
    _D2_READER_ORIGINAL = dict(CreateFile=_d2_winapi.CreateFile,
        CloseHandle=_d2_winapi.CloseHandle, open_osfhandle=_d2_msvcrt.open_osfhandle,
        get_osfhandle=_d2_msvcrt.get_osfhandle)
else:
    _D2_WINAPI_ORIGINAL = {}
    _D2_READER_ORIGINAL = {}


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
    if _CONTEXT is not None and _CONTEXT.lane == 'd2':
        _d2_slot_access(path, 'METADATA')
    if boundary is not None and not path.is_relative_to(_absolute(boundary)):
        raise ValueError('OFFLINE_PATH_OUTSIDE_ROOT')
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = _ORIGINAL_LSTAT(current) if _CONTEXT is not None and _CONTEXT.lane == 'd2' else current.lstat()
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
    if ticket['role'] not in {'f2-spawn', 'flog-worker', 'd2-worker', 'd2-baseline-worker', 'd2-lock-worker'} and not (implicit and ticket['role'] in {'native-git', 'f2-precommit'}):
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
        if lane in {'d1', 'd2'}:
            # Parent владеет только текущим D1 envelope. Каждый child далее
            # получает свой slot; чужие F-LOG runs не входят в writable roots.
            roots.append(root / ('.f-log/' + lane) / run_id)
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
        install = lane in {'f3', 'd1', 'd2'}
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
    if c.lane == 'd2' and kind == 'native':
        item['native_origin'] = _d2_safe_native_origin()
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
    if node in {
            'tests/d2/test_guard_admission.py::test_alternative_native_loaders_refused[cdll]',
            'tests/d2/test_guard_admission.py::test_alternative_native_loaders_refused[windll]'}:
        return _CONTEXT.lane == 'd2' and delta == {'native': 1}
    if node in {
            'tests/d1/test_guard_admission.py::test_alternative_native_loaders_refused[cdll]',
            'tests/d1/test_guard_admission.py::test_alternative_native_loaders_refused[windll]'}:
        return _CONTEXT.lane == 'd1' and delta == {'native': 1}
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
    d1_native = kind == 'native' and _CONTEXT.lane in {'d1', 'd2'} and _fault_scope(node, {'native': 1})
    if (not node.startswith('tests/shared/') and not d1_native) or count != 1:
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
    if c.lane == 'd2' and c.role == 'd2-worker':
        obj['slot_effects'] = dict(getattr(c, 'd2_slot_effects', {}))
        obj['slot_phase_witnesses'] = list(getattr(c, 'd2_phase_history', []))
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
            witness = None
            if _CONTEXT is not None and _CONTEXT.lane == 'd2':
                witness = next((handle for handle, record in _D2_NATIVE_HANDLES.items()
                    if record.get('ticket_id') == identifier and record.get('retained')), None)
            _job_close(_CHILD_JOBS.pop(identifier), terminate=True)
            if witness is not None:
                try:
                    settled = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](witness, 10000) == 0
                    code = int(_D2_WINAPI_ORIGINAL['GetExitCodeProcess'](witness)) if settled else None
                    proof = _D2_NATIVE_HANDLES[witness]
                    exclusive_json(_CONTEXT.report_dir / 'tickets' / (identifier + '.d2-containment.json'),
                        dict(ticket_id=identifier, run_id=_CONTEXT.run_id, pid=proof.get('pid'),
                            actual_exit=-15 if code == 65536 else code, raw_windows_exit=code,
                            settled=settled, witness='GUARD_RETAINED_HANDLE', purpose='GUARD_EXIT_CONTAINMENT'))
                except (OSError, ValueError):
                    pass


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
        if _CONTEXT.lane == 'd2' and isinstance(value, (str, bytes, os.PathLike)):
            _d2_slot_access(value, 'OPEN')
        if isinstance(value, int):
            if _CONTEXT.lane == 'd2' and _d2_checkpoint_fdopen_grant(value, mode, flags):
                return
            if _CONTEXT.lane == 'd2' and _d2_bootstrap_read_grant(value, mode, flags):
                return
            if getattr(_THREAD_STATE, 'launch', 0):
                _OWN_FDS.add(value)
            if value not in _OWN_FDS:
                reject('descriptors')
            return
        if not isinstance(value, (str, bytes, os.PathLike)):
            return
        path = _absolute(value)
        if _secret(path) and not _d2_fake_secret(path):
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
            if (_CONTEXT.lane == 'd2' and getattr(_CONTEXT, 'nodeid', '') ==
                    'tests/d2/test_targets.py::test_hardlink_source_alias_refused'
                    and p.name == 'alias.csv' and _owned(p.parent)):
                info, source = _ORIGINAL_LSTAT(p), _ORIGINAL_LSTAT(p.parent / 'README.md')
                if (stat.S_ISREG(info.st_mode) and info.st_nlink == source.st_nlink == 2
                        and (info.st_dev, info.st_ino) == (source.st_dev, source.st_ino)):
                    return
            if (_CONTEXT.lane == 'd2' and getattr(_CONTEXT, 'nodeid', '') ==
                    'tests/d2/test_guard_admission.py::test_checkpoint_reader_invalid_path_has_no_native_effect[alias]'
                    and p.name == 'alias.json' and _owned(p.parent)):
                info, source = _ORIGINAL_LSTAT(p), _ORIGINAL_LSTAT(p.parent / 'google.json')
                if (stat.S_ISREG(info.st_mode) and info.st_nlink == source.st_nlink == 2
                        and (info.st_dev, info.st_ino) == (source.st_dev, source.st_ino)):
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
    if _CONTEXT.lane == 'd2' and event in {'ctypes.dlopen', 'ctypes.dlsym'}:
        if not getattr(_THREAD_STATE, 'd2_binding', False):
            reject('native')
    if _CONTEXT.lane == 'd2' and event == '_winapi.CreateFile':
        token = getattr(_THREAD_STATE, 'd2_reader', None)
        # Existing pinned multiprocessing named-pipe creation has its own
        # launch/bootstrap contract. This request validates only the new reader.
        if token is not None and (not token.get('creating') or
                tuple(args) != (str(token['path']), 0x80000000, 7, 3, 0x00200080)):
            reject('native')
    if _CONTEXT.lane == 'd2' and event == 'msvcrt.open_osfhandle':
        token = getattr(_THREAD_STATE, 'd2_reader', None)
        if token is not None and (not token.get('transferring') or
                tuple(args) != (token['handle'], os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT)):
            reject('descriptors')
    if _CONTEXT.lane == 'd1' and event == 'ctypes.dlopen':
        if (not getattr(_THREAD_STATE, 'd1_binding', False) or not args
                or type(args[0]) is not str or args[0].lower() not in {'kernel32', 'kernel32.dll'}):
            reject('native')
    if _CONTEXT.lane == 'd1' and event == 'ctypes.dlsym':
        if (not getattr(_THREAD_STATE, 'd1_binding', False) or len(args) < 2
                or args[1] not in _D1_NATIVE_FUNCTIONS):
            reject('native')


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
        caller = sys._getframe(1)
        if (_CONTEXT.lane == 'd2' and _d2_fixture_profile_admitted()
                and seconds == .1 and caller.f_code.co_name in {'_d2_supervise', '_d2_worker_entry'}
                and _absolute(caller.f_code.co_filename) == _CONTEXT.root / 'upload.py'):
            return _ORIGINAL_SLEEP(seconds)
        reject('sleep')
    time.sleep = workload_sleep

    def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
        if dir_fd is not None:
            reject('descriptors')
        if _CONTEXT.lane == 'd2' and isinstance(path, (str, bytes, os.PathLike)):
            _d2_slot_access(path, 'OPEN')
        request = getattr(_THREAD_STATE, 'd2_request', None)
        if request is not None:
            case = request['ticket']['d2_contract']['case']
            null = os.path.normcase(os.path.abspath(os.fsdecode(path))) == os.path.normcase(os.path.abspath(os.devnull))
            mode_name = 'read' if not flags & (os.O_WRONLY | os.O_RDWR) else 'write'
            if null and case == 'native_nul_' + mode_name + '_fault':
                d2_fault_witness(case, request=request)
                raise OSError('D2_NATIVE_NUL_' + mode_name.upper() + '_FAILED')
        descriptor = _ORIGINAL_OS_OPEN(path, flags, mode, dir_fd=dir_fd)
        request = getattr(_THREAD_STATE, 'd2_request', None)
        if request is not None and os.path.normcase(os.path.abspath(os.fsdecode(path))) == os.path.normcase(os.path.abspath(os.devnull)):
            import msvcrt
            request['nul_sources'][int(msvcrt.get_osfhandle(descriptor))] = 'read' if not flags & (os.O_WRONLY | os.O_RDWR) else 'write'
        _OWN_FDS.add(descriptor)
        _WRITABLE_FDS.pop(descriptor, None)
        if flags & (os.O_WRONLY | os.O_RDWR) and _owned(path):
            info = os.fstat(descriptor)
            if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                _WRITABLE_FDS[descriptor] = (checked_path(path), (info.st_dev, info.st_ino), None)
        return descriptor

    def guarded_close(fd):
        reader = getattr(_THREAD_STATE, 'd2_reader', None)
        if reader is not None:
            _d2_checkpoint_reader_frame(reader, sys._getframe(1))
            if (fd != reader.get('fd') or reader.get('fd_closed')
                    or _D2_READER_ORIGINAL['get_osfhandle'](fd) != reader.get('transferred_handle')):
                raise ValueError('D2_READER_CLOSE_OWNER')
            result = _ORIGINAL_OS_CLOSE(fd)
            reader['fd_closed'] = True
            reader['fd_close_calls'] += 1
            return result
        _OWN_FDS.discard(fd)
        _WRITABLE_FDS.pop(fd, None)
        return _ORIGINAL_OS_CLOSE(fd)
    os.open = guarded_open
    os.close = guarded_close
    def guarded_fdopen(fd, *args, **kwargs):
        reader = getattr(_THREAD_STATE, 'd2_reader', None)
        if reader is not None:
            _d2_checkpoint_reader_frame(reader, sys._getframe(1))
            if fd != reader.get('fd') or args != ('rb',) or kwargs != {'closefd': False}:
                raise ValueError('D2_READER_STREAM_OWNER')
            reader['fdopening'] = True
        try:
            stream = _ORIGINAL_FDOPEN(fd, *args, **kwargs)
        finally:
            if reader is not None:
                reader['fdopening'] = False
        if reader is not None:
            reader['stream'] = stream
        record = _WRITABLE_FDS.pop(fd, None)
        if record is not None and stream.writable():
            _WRITABLE_FDS[fd] = (record[0], record[1], weakref.ref(stream))
        return stream
    os.fdopen = guarded_fdopen
    def guarded_stat(path, *arguments, **options):
        if _CONTEXT.lane == 'd2' and isinstance(path, (str, bytes, os.PathLike)):
            _d2_slot_access(path, 'METADATA')
        return _ORIGINAL_STAT(path, *arguments, **options)
    def guarded_lstat(path, *arguments, **options):
        if _CONTEXT.lane == 'd2' and isinstance(path, (str, bytes, os.PathLike)):
            _d2_slot_access(path, 'METADATA')
        return _ORIGINAL_LSTAT(path, *arguments, **options)
    if _CONTEXT.lane == 'd2':
        os.stat, os.lstat = guarded_stat, guarded_lstat
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
        if _CONTEXT.lane in {'d1', 'd2'}:
            # Один token относится только к одной полной launch-заявке. Даже
            # внутри controller нельзя заменить argv, env, cwd или pipes.
            token = getattr(_THREAD_STATE, 'd1_launch_token' if _CONTEXT.lane == 'd1' else 'd2_launch_token', None)
            if (token is None or token.get('used') or args
                    or [os.fsdecode(p) for p in argv] != token['argv']
                    or kwargs != token['kwargs']):
                reject('children')
            token['used'] = True
        return _ORIGINAL_POPEN(self, argv, *args, **kwargs)
    subprocess.Popen.__init__ = guarded_popen
    if _CONTEXT.lane == 'd1' and _ORIGINAL_WINDLL is not None:
        ctypes.WinDLL = _d1_windll
        # Colorama preload не оставляет cached DLL loader или raw native leaf
        # доступными workload. Console APIs не входят в D1 allowlist.
        ctypes.windll = ctypes.LibraryLoader(_d1_reject_loader)
        ctypes.cdll = ctypes.LibraryLoader(_d1_reject_loader)
        console = sys.modules.get('colorama.win32')
        if console is not None:
            console.windll = ctypes.windll
            for name in ('_GetStdHandle', '_GetConsoleScreenBufferInfo', '_SetConsoleTextAttribute',
                    '_SetConsoleCursorPosition', '_FillConsoleOutputCharacterA', '_FillConsoleOutputAttribute',
                    '_SetConsoleTitleW', '_GetConsoleMode', '_SetConsoleMode'):
                setattr(console, name, _d1_reject_console_call)
            sys.modules['colorama.ansitowin32'].windll = ctypes.windll
    if _CONTEXT.lane == 'd2' and _ORIGINAL_WINDLL is not None:
        ctypes.WinDLL = _d2_windll
        ctypes.windll = ctypes.LibraryLoader(_d1_reject_loader)
        ctypes.cdll = ctypes.LibraryLoader(_d1_reject_loader)
        console = sys.modules.get('colorama.win32')
        if console is not None:
            console.windll = ctypes.windll
            for name in ('_GetStdHandle', '_GetConsoleScreenBufferInfo', '_SetConsoleTextAttribute',
                    '_SetConsoleCursorPosition', '_FillConsoleOutputCharacterA', '_FillConsoleOutputAttribute',
                    '_SetConsoleTitleW', '_GetConsoleMode', '_SetConsoleMode'):
                setattr(console, name, _d1_reject_console_call)
            sys.modules['colorama.ansitowin32'].windll = ctypes.windll
    sys.addaudithook(_audit)
    _INSTALLED = True


def set_node(nodeid):
    if _CONTEXT is not None:
        _CONTEXT.nodeid = nodeid


expected_fault = expect_fault


def issue_ticket(role, argv, roots=None, expected_counts=None, expected_exit=0,
                 parent_id=None, native=False, required_roles=None, gate=False, mode=None,
                 d1_contract=None, d2_contract=None):
    if _CONTEXT is None or role not in ROLES:
        raise ValueError('OFFLINE_CHILD_ROLE')
    c = _CONTEXT
    if c.lane == 'd2' and role not in _D2_ROLES | {'lane-worker'}:
        raise ValueError('D2_ADMISSION_ROLE')
    if c.lane == 'd1' and c.role == 'd1-worker':
        reject('children')
    if role == 'd1-worker' and (c.lane != 'd1' or not _D1_CONTROLLER_ACTIVE
                               or not isinstance(d1_contract, dict)):
        raise ValueError('D1_ADMISSION_TICKET')
    if role.startswith('d2-'):
        _d2_validate_contract(role, d2_contract)
    command = [os.fsdecode(p) for p in argv]
    if not command:
        raise ValueError('OFFLINE_CHILD_ARGV')
    if c.lane == 'd2' and role == 'lane-worker' and not _allowed_command(command, role):
        raise ValueError('D2_CHILD_COMMAND_SCOPE')
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
    if role == 'd1-worker':
        ticket['d1_contract'] = dict(d1_contract)
    if role.startswith('d2-'):
        ticket['d2_contract'] = dict(d2_contract)
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
    if _CONTEXT.lane == 'd1' and _CONTEXT.role == 'd1-worker':
        reject('children')
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
    if _CONTEXT.lane == 'd1' and _CONTEXT.role == 'd1-worker':
        reject('children')
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
    if _CONTEXT is not None and _CONTEXT.lane in {'d1', 'd2'}:
        _THREAD_STATE.d1_job_limits = ctypes.string_at(ctypes.byref(limits), ctypes.sizeof(limits))
    try:
        configured = kernel.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits))
    finally:
        _THREAD_STATE.d1_job_limits = None
    if not configured:
        kernel.CloseHandle(handle)
        raise OSError('OFFLINE_JOB_LIMIT_FAILED')
    return kernel, handle


def _job_assign(job, process):
    if job is not None and not job[0].AssignProcessToJobObject(job[1], int(process._handle)):
        raise OSError('OFFLINE_JOB_ASSIGNMENT_FAILED')


def _job_close(job, terminate=False):
    if job is not None:
        _THREAD_STATE.d1_closing_job = _d1_handle(job[1])
        try:
            if terminate:
                job[0].TerminateJobObject(job[1], 124)
            job[0].CloseHandle(job[1])
        finally:
            _THREAD_STATE.d1_closing_job = None


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
        if _CONTEXT is not None and _CONTEXT.lane == 'd1':
            _D1_NATIVE_HANDLES[int(process_handle)] = dict(kind='process', ticket_id=ticket['ticket_id'])
            _D1_NATIVE_HANDLES[int(thread_handle)] = dict(kind='thread', ticket_id=ticket['ticket_id'])
        if _CONTEXT is not None and _CONTEXT.lane == 'd2':
            _D2_NATIVE_HANDLES[int(process_handle)] = dict(kind='process', ticket_id=ticket['ticket_id'], pid=int(result[2]))
            _D2_NATIVE_HANDLES[int(thread_handle)] = dict(kind='thread', ticket_id=ticket['ticket_id'])
            request = _D2_REQUESTS.get(ticket['ticket_id'])
            if request is not None:
                request['observed_pid'] = int(result[2])
                request['original_process_handle'] = int(process_handle)
                exclusive_json(_CONTEXT.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-native-created.json'),
                    dict(version=1, ticket_id=ticket['ticket_id'], run_id=ticket['run_id'],
                        nodeid=ticket['nodeid'], case=ticket['d2_contract']['case'],
                        worker_id=ticket['d2_contract']['worker_id'], pid=int(result[2]),
                        controller_pid=os.getpid(), native_calls=1, witness='GUARD_CREATEPROCESS'))
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
            if _CONTEXT is not None and _CONTEXT.lane == 'd2':
                if _winapi.WaitForSingleObject(int(result[0]), 10000) == 0:
                    record_child_exit(ticket, int(_winapi.GetExitCodeProcess(int(result[0]))))
            _winapi.CloseHandle(result[1])
            _winapi.CloseHandle(result[0])
        _CHILD_JOBS.pop(ticket['ticket_id'], None)
        _job_close(job, terminate=True)
        raise


# %% D1: только проверенные Windows handles, без общего native bypass
_D1_NATIVE_FUNCTIONS = {
    'CreateFileW', 'GetFileInformationByHandleEx', 'GetFileInformationByHandle',
    'GetDriveTypeW', 'CreateMutexW', 'OpenMutexW', 'WaitForSingleObject',
    'ReleaseMutex', 'CloseHandle', 'CreateJobObjectW', 'SetInformationJobObject',
    'AssignProcessToJobObject', 'TerminateJobObject', 'ResumeThread', 'TerminateProcess',
}
_D1_JOB_FUNCTIONS = {'CreateJobObjectW', 'SetInformationJobObject',
    'AssignProcessToJobObject', 'TerminateJobObject', 'ResumeThread', 'TerminateProcess'}


def _d1_handle(value):
    return int(getattr(value, 'value', value) or 0)


def _d1_native_call(name, original, args):
    c = _CONTEXT
    if c is None or c.lane != 'd1' or name not in _D1_NATIVE_FUNCTIONS:
        reject('native')
    record = _D1_NATIVE_HANDLES.get(_d1_handle(args[0])) if args and name not in {
        'CreateFileW', 'CreateMutexW', 'OpenMutexW', 'GetDriveTypeW', 'CreateJobObjectW'} else None
    if name in _D1_JOB_FUNCTIONS:
        pending = getattr(_THREAD_STATE, 'pending', [])
        launching = bool(pending and pending[-1]['role'] in {'d1-worker', 'lane-worker'})
        closing = (name == 'TerminateJobObject' and record is not None and record['kind'] == 'job'
            and getattr(_THREAD_STATE, 'd1_closing_job', None) == _d1_handle(args[0])
            and any(t['ticket_id'] == record.get('ticket_id') for t in c.children))
        if c.role == 'd1-worker' or not (launching or _D1_CONTROLLER_ACTIVE or closing):
            reject('native')
        if name == 'CreateJobObjectW':
            if not launching or tuple(args) != (None, None):
                reject('native')
        elif name in {'SetInformationJobObject', 'AssignProcessToJobObject', 'TerminateJobObject'}:
            if record is None or record['kind'] != 'job':
                reject('native')
            if name == 'SetInformationJobObject':
                limits = getattr(_THREAD_STATE, 'd1_job_limits', None)
                if (not launching or args[1] != 9 or limits is None or args[3] != len(limits)
                        or ctypes.string_at(args[2], args[3]) != limits):
                    reject('native')
            if name == 'AssignProcessToJobObject':
                process = _D1_NATIVE_HANDLES.get(_d1_handle(args[1]))
                if (not launching or process is None or process['kind'] != 'process'
                        or process.get('ticket_id') != pending[-1]['ticket_id']):
                    reject('native')
            if name == 'TerminateJobObject' and args[1] != 124:
                reject('native')
        elif (not launching or record is None or record['kind'] !=
              ('thread' if name == 'ResumeThread' else 'process')
              or record.get('ticket_id') != pending[-1]['ticket_id']
              or name == 'TerminateProcess' and args[1] != 124):
            # Resume/failed-launch cleanup относятся только к только что
            # созданным процессу/thread из suspended CreateProcess.
            reject('native')
    elif name == 'CreateFileW':
        path = checked_path(args[0])
        read_attributes = args[1] == 0x80 and args[2] == 7 and args[5] == 0x02200000
        reader = args[1] == 0x80000000 and args[2] in {3, 7} and args[5] == 0x00200000
        native_reader = getattr(_THREAD_STATE, 'd1_reader', None)
        if (not _owned(path) or args[3] is not None or args[4] != 3 or args[6] is not None
                or not (read_attributes or reader and native_reader == str(path))):
            reject('native')
    elif name == 'GetDriveTypeW':
        drive = _absolute(args[0])
        if str(drive) != drive.anchor or drive.anchor.lower() not in {p.anchor.lower() for p in c.roots}:
            reject('native')
    elif name in {'CreateMutexW', 'OpenMutexW'}:
        mutex_name = args[2]
        if (mutex_name not in _D1_MUTEX_NAMES
                or not re.fullmatch(r'Global\\MDS-D1-v1-[0-9a-f]{64}', str(mutex_name))
                or args[1] not in {False, 0}):
            reject('native')
        if name == 'CreateMutexW' and args[0] is not None:
            reject('native')
        if name == 'OpenMutexW' and (args[0] != 0x00100000 or not getattr(_THREAD_STATE, 'd1_late_probe', False)):
            reject('native')
    elif name in {'GetFileInformationByHandleEx', 'GetFileInformationByHandle'}:
        if record is None or record['kind'] not in {'file', 'directory'}:
            reject('native')
        if name == 'GetFileInformationByHandleEx' and (args[1] != 18 or args[3] != 24):
            reject('native')
    elif name == 'WaitForSingleObject':
        if record is None or record['kind'] != 'mutex' or type(args[1]) is not int or not 0 <= args[1] <= 100:
            reject('native')
    elif name == 'ReleaseMutex':
        if record is None or record['kind'] != 'mutex' or not record.get('owned'):
            reject('native')
    elif name == 'CloseHandle':
        if record is None or record['kind'] == 'job' and c.role == 'd1-worker':
            reject('native')
    result = original(*args)
    handle = _d1_handle(result)
    if name == 'CreateFileW' and handle not in {0, ctypes.c_void_p(-1).value}:
        _D1_NATIVE_HANDLES[handle] = dict(kind='directory' if path.is_dir() else 'file', path=path)
    elif name in {'CreateMutexW', 'OpenMutexW'} and handle:
        _D1_NATIVE_HANDLES[handle] = dict(kind='mutex', name=mutex_name, owned=False)
    elif name == 'CreateJobObjectW' and handle:
        _D1_NATIVE_HANDLES[handle] = dict(kind='job', ticket_id=pending[-1]['ticket_id'])
    elif name == 'GetFileInformationByHandleEx' and result and record['kind'] == 'directory':
        # Независимый exact-name oracle guard использует все 24 bytes FILE_ID_INFO.
        raw = ctypes.string_at(args[2], 24)
        digest = hashlib.sha256(b'MDS-D1-v1\0' + raw[:8] + raw[8:24]).hexdigest()
        _D1_MUTEX_NAMES.add('Global\\MDS-D1-v1-' + digest)
    elif name == 'WaitForSingleObject':
        if result in {0, 0x80}:
            record['owned'] = True
        elif result == 0x102:
            c.d1_contended = True
    elif name == 'ReleaseMutex' and result:
        record['owned'] = False
    elif name == 'CloseHandle' and result:
        _D1_NATIVE_HANDLES.pop(_d1_handle(args[0]), None)
    return result


def _d1_windll(name, *args, **kwargs):
    if str(name).lower() not in {'kernel32', 'kernel32.dll'} or args or kwargs != {'use_last_error': True}:
        reject('native')
    _THREAD_STATE.d1_binding = True
    try:
        actual = _ORIGINAL_WINDLL(name, **kwargs)
    finally:
        _THREAD_STATE.d1_binding = False
    functions = {}

    class KernelBindings:
        def __getattr__(self, symbol):
            if symbol not in _D1_NATIVE_FUNCTIONS:
                reject('native')
            if symbol not in functions:
                _THREAD_STATE.d1_binding = True
                try:
                    leaf = getattr(actual, symbol)
                finally:
                    _THREAD_STATE.d1_binding = False
                def bound(*values):
                    if hasattr(bound, 'argtypes'):
                        leaf.argtypes = bound.argtypes
                    if hasattr(bound, 'restype'):
                        leaf.restype = bound.restype
                    return _d1_native_call(symbol, leaf, values)
                functions[symbol] = bound
            return functions[symbol]
    return KernelBindings()


def _d1_reject_loader(name):
    reject('native')


def _d1_reject_console_call(*args, **kwargs):
    reject('native')


def _preload_console(root, lane):
    """Admit only the fixed pytest console import before workload guards."""
    if _CONTEXT is not None or sys.platform != 'win32':
        raise ValueError('D1_CONSOLE_BOOTSTRAP_SCOPE')
    ticket = _load_ticket(checked_path(root))
    if ticket is None or ticket['lane'] != lane or ticket['role'] != 'lane-worker':
        raise ValueError('D1_CONSOLE_BOOTSTRAP_TICKET')
    # Pytest импортирует colorama до capture. Проверяем точные package bytes
    # из разрешённого interpreter, не вызываем init()/console mutation. После
    # bootstrap DLL loaders и все сохранённые console bindings закрываются.
    package = checked_path(Path(sys.executable).parents[1] / 'Lib/site-packages/colorama')
    hashes = {
        '__init__.py': 'c1e3d0038536d2d2a060047248b102d38eee70d5fe83ca512e9601ba21e52dbf',
        'ansi.py': '4e8a7811e12e69074159db5e28c11c18e4de29e175f50f96a3febf0a3e643b34',
        'ansitowin32.py': 'bcf3586b73996f18dbb85c9a568d139a19b2d4567594a3160a74fba1d5e922d9',
        'initialise.py': 'fa1227cbce82957a37f62c61e624827d421ad9ffe1fdb80a4435bb82ab3e28b5',
        'win32.py': '61038ac0c4f0b4605bb18e1d2f91d84efc1378ff70210adae4cbcf35d769c59b',
        'winterm.py': '5c24050c78cf8ba00760d759c32d2d034d87f89878f09a7e1ef0a378b78ba775',
    }
    if set(path.name for path in package.glob('*.py')) != set(hashes):
        raise ValueError('D1_CONSOLE_BOOTSTRAP_DIGEST')
    for name, expected in hashes.items():
        path = checked_path(package / name, package)
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('D1_CONSOLE_BOOTSTRAP_DIGEST')
    import colorama
    if checked_path(colorama.__file__) != package / '__init__.py':
        raise ValueError('D1_CONSOLE_BOOTSTRAP_ORIGIN')


def d1_preload_console(root):
    return _preload_console(root, 'd1')


def d2_preload_console(root):
    return _preload_console(root, 'd2')


@contextmanager
def d1_native_reader(path, deny_delete=False):
    """Fixed read-only Windows fixture; the caller never receives its HANDLE."""
    c = _CONTEXT
    node = getattr(c, 'nodeid', '').split('[', 1)[0]
    mode = getattr(c, 'd1_contract', {}).get('mode')
    permitted = (not deny_delete and mode == 'read_final') or (
        deny_delete and node == 'tests/d1/test_atomic_write.py::test_real_windows_deny_delete')
    if c.lane != 'd1' or not permitted or sys.platform != 'win32':
        raise ValueError('D1_NATIVE_READER_SCOPE')
    path = checked_path(path)
    if not _owned(path):
        raise ValueError('D1_NATIVE_READER_ROOT')
    from ctypes import wintypes
    import msvcrt
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    _THREAD_STATE.d1_reader = str(path)
    try:
        handle = kernel.CreateFileW(str(path), 0x80000000, 3 if deny_delete else 7, None, 3, 0x00200000, None)
    finally:
        _THREAD_STATE.d1_reader = None
    if _d1_handle(handle) in {0, ctypes.c_void_p(-1).value}:
        raise OSError('D1_NATIVE_READER_FAILED')
    try:
        descriptor = msvcrt.open_osfhandle(_d1_handle(handle), os.O_RDONLY | os.O_BINARY)
    except BaseException:
        kernel.CloseHandle(handle)
        raise
    _D1_NATIVE_HANDLES.pop(_d1_handle(handle), None)
    _OWN_FDS.add(descriptor)
    _THREAD_STATE.d1_open_reader = (str(path), descriptor)
    _THREAD_STATE.d1_closed_reader = None
    try:
        with os.fdopen(descriptor, 'rb') as stream:
            yield stream
            # Законченный h0 read закрывается до replace: Windows legacy
            # rename может отказать даже при FILE_SHARE_DELETE. Witness
            # сообщает bytes одного завершённого read, не live-handle promise.
            if mode == 'read_final' and not deny_delete:
                stream.seek(0)
                data = stream.read(107)
                if len(data) > 106:
                    raise ValueError('D1_NATIVE_READER_BOUND')
                observed = (str(path), hashlib.sha256(data).hexdigest(), len(data))
        if mode == 'read_final' and not deny_delete:
            _THREAD_STATE.d1_closed_reader = observed
    finally:
        _THREAD_STATE.d1_open_reader = None
        _OWN_FDS.discard(descriptor)


# %% D1: закрытая таблица case/slot/mode/node; CLI не задаёт capabilities
_D1_PROCESS_NODE = 'tests/d1/test_resource_lock.py::test_process_cases'
_D1_CONTROL_NODE = 'tests/d1/test_guard_admission.py::test_control_cases'
D1_CASES = {
    'deny_second': {'holder': ('hold_update', 'shared/current.csv', 0), 'contender': ('deny', 'shared/current.csv', 0)},
    'bounded_wait': {'holder': ('hold_update', 'shared/current.csv', 0), 'contender': ('wait_500ms', 'shared/current.csv', .5)},
    'serialize_updates': {'holder': ('hold_update', 'shared/current.csv', 0), 'contender': ('retry_after_denial', 'shared/current.csv', 0)},
    'independent_directories': {'a': ('hold_update', 'a/current.csv', 0), 'b': ('hold_update', 'b/current.csv', 0)},
    'same_directory_siblings': {'holder': ('hold_update_a', 'shared/A.csv', 0), 'contender': ('deny_b', 'shared/B.csv', 0)},
    'abandoned_before_commit': {'holder': ('hold_partial', 'shared/current.csv', 0), 'recovery': ('wait_recover', 'shared/current.csv', 5)},
    'late_restart_before_commit': {'holder': ('hold_partial', 'shared/current.csv', 0), 'restart': ('validate_only', 'shared/current.csv', 0)},
    'late_restart_invalid_before_commit': {'holder': ('hold_partial', 'shared/current.csv', 0), 'restart': ('validate_reject_only', 'shared/current.csv', 0)},
    'crash_after_commit': {'holder': ('hold_after_replace', 'shared/current.csv', 0), 'recovery': ('wait_validate_only', 'shared/current.csv', 5)},
    'late_restart_after_commit': {'holder': ('hold_after_replace', 'shared/current.csv', 0), 'restart': ('validate_only', 'shared/current.csv', 0)},
    'directory_target_switch': {'holder': ('hold_partial_a', 'shared/A.csv', 0), 'next': ('wait_reject_b', 'shared/B.csv', 5)},
    'reader_visibility': {'writer': ('write_gated', 'shared/current.csv', 0), 'reader': ('read_final', 'shared/current.csv', 0)},
    **{case: {'worker': (case, 'shared/current.csv', 0)} for case in ('retry_false', 'retry_unverified', 'retry_exception')},
    **{case: {'worker': (case, 'shared/current.csv', 0)} for case in (
        'exit_before_ready_7', 'omit_guard_final', 'corrupt_guard_final', 'park_without_release',
        'attempt_extra_child', 'foreign_report', 'duplicate_report', 'replayed_report', 'malformed_ready')},
}
_D1_CONTROL_ERRORS = {
    'exit_before_ready_7': {'D1_READY_NOT_REACHED', 'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'},
    'omit_guard_final': {'MISSING_FINAL_REPORT'},
    'corrupt_guard_final': {'CORRUPT_FINAL_REPORT'},
    'park_without_release': {'D1_UNDECLARED_TIMEOUT', 'MISSING_FINAL_REPORT', 'TIMEOUT', 'UNEXPECTED_EXIT'},
    'attempt_extra_child': {'UNEXPECTED_COUNTERS'},
    'foreign_report': {'FOREIGN_FINAL_REPORT'},
    'duplicate_report': {'DUPLICATE_FINAL_REPORT'},
    'replayed_report': {'DUPLICATE_FINAL_REPORT'},
    'malformed_ready': {'D1_FRAME_FORMAT', 'D1_READY_NOT_REACHED', 'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'},
}
_D1_CRASH_PHASE = {
    'abandoned_before_commit': 'CANDIDATE_READY', 'late_restart_before_commit': 'CANDIDATE_READY',
    'late_restart_invalid_before_commit': 'CANDIDATE_READY',
    'directory_target_switch': 'CANDIDATE_READY', 'crash_after_commit': 'POSTCOMMIT_READY',
    'late_restart_after_commit': 'POSTCOMMIT_READY',
}
_D1_FRAME_KEYS = {'phase', 'sequence', 'ticket_id', 'run_id', 'nodeid', 'case', 'slot',
    'nonce', 'pid', 'tested_sha', 'source_mode', 'source_digest', 'bootstrap_digest', 'policy_digest', 'worker_digest', 'payload'}
_D1_RECEIPT_KEYS = {'receipt_version', 'operation_id', 'file_ref', 'lock_observation', 'lock_state',
    'target_state', 'target_validation', 'lease_entry_observation', 'attempt_baseline', 'final_observation',
    'prepared_candidate', 'commit_state', 'final_relation', 'write_status', 'diagnostics_state',
    'mandatory_sequences', 'barrier_sequence', 'errors', 'null_reasons'}


def _d1_header(ticket, pid):
    contract = ticket['d1_contract']
    return {**{key: ticket[key] for key in ('ticket_id', 'run_id', 'tested_sha', 'source_mode',
        'source_digest', 'bootstrap_digest', 'policy_digest', 'nodeid')}, 'pid': pid,
        **{key: contract[key] for key in ('case', 'slot', 'nonce', 'worker_digest')}}


def _d1_payload(payload):
    if payload is None:
        return True
    keys = {'receipt', 'status', 'observed_sha256', 'observed_bytes', 'validator_calls', 'mutex_name'}
    if not isinstance(payload, dict) or set(payload) != keys:
        return False
    if type(payload['status']) is not str or payload['status'] not in {'SUCCEEDED', 'DENIED', 'REJECTED', 'FAILED', 'VALIDATED'}:
        return False
    if payload['observed_sha256'] is not None and (type(payload['observed_sha256']) is not str
            or not re.fullmatch('[0-9a-f]{64}', payload['observed_sha256'])):
        return False
    if payload['observed_bytes'] is not None and (type(payload['observed_bytes']) is not int or payload['observed_bytes'] < 0):
        return False
    if type(payload['validator_calls']) is not int or not 0 <= payload['validator_calls'] <= 4:
        return False
    if payload['mutex_name'] is not None and (type(payload['mutex_name']) is not str
            or not re.fullmatch(r'Global\\MDS-D1-v1-[0-9a-f]{64}', payload['mutex_name'])):
        return False
    receipt = payload['receipt']
    if receipt is None:
        return True
    if (not isinstance(receipt, dict) or set(receipt) != _D1_RECEIPT_KEYS
            or type(receipt['receipt_version']) is not int or receipt['receipt_version'] != 1):
        return False
    # Каждое поле проверяется отдельно. Regex «без опасных символов» не является
    # redaction: даже простая строка может быть canary, pathname или exception.
    operation = receipt['operation_id']
    if (type(operation) is not str or not re.fullmatch('[0-9a-f]{32}', operation)
            or type(receipt['file_ref']) is not str or receipt['file_ref'] != 'd1-file-' + operation):
        return False
    enums = {
        'lock_observation': {'NOT_ACQUIRED', 'NORMAL', 'ABANDONED'},
        'lock_state': {'NOT_ACQUIRED', 'OWNED', 'RELEASED', 'UNKNOWN'},
        'target_state': {'ABSENT', 'PRESENT', 'CHANGED', 'UNKNOWN'},
        'target_validation': {'NOT_RUN', 'ADMITTED', 'REJECTED', 'ERROR'},
        'commit_state': {'NOT_ATTEMPTED', 'UNKNOWN', 'REPLACED'},
        'final_relation': {'MATCHES_BASELINE', 'MATCHES_CANDIDATE', 'OTHER', 'UNKNOWN'},
        'write_status': {'NOT_RUN', 'SUCCEEDED', 'FAILED'},
        'diagnostics_state': {'NOT_CONFIRMED', 'PRECOMMIT_CONFIRMED', 'POSTCOMMIT_CONFIRMED', 'COMPLETE', 'INCOMPLETE'},
    }
    if any(type(receipt[key]) is not str or receipt[key] not in values for key, values in enums.items()):
        return False
    def nonnegative(value):
        return type(value) is int and 0 <= value < 2**63
    def digest_size(value, keys):
        return (isinstance(value, dict) and set(value) == keys
            and type(value['sha256']) is str and re.fullmatch('[0-9a-f]{64}', value['sha256']) is not None
            and nonnegative(value['bytes']))
    observations = ('lease_entry_observation', 'attempt_baseline', 'final_observation')
    for key in observations:
        value = receipt[key]
        if value is None:
            continue
        if not isinstance(value, dict) or set(value) != {'exists', 'sha256', 'bytes'} or type(value['exists']) is not bool:
            return False
        if value['exists']:
            if not digest_size(value, {'exists', 'sha256', 'bytes'}):
                return False
        elif value['sha256'] is not None or value['bytes'] is not None:
            return False
    if receipt['prepared_candidate'] is not None and not digest_size(receipt['prepared_candidate'], {'sha256', 'bytes'}):
        return False
    sequences = receipt['mandatory_sequences']
    if (type(sequences) is not list or len(sequences) > 64
            or any(not nonnegative(value) or value == 0 for value in sequences)
            or any(a >= b for a, b in zip(sequences, sequences[1:]))):
        return False
    barrier = receipt['barrier_sequence']
    if barrier is not None and (not nonnegative(barrier) or barrier == 0):
        return False
    stages = {'admission', 'diagnostics', 'path', 'lock', 'caller', 'release',
        'baseline', 'preparation', 'candidate_validation', 'precommit', 'replace', 'readback', 'cleanup'}
    secondary = {'diagnostics': {'D1_FAILURE_EVENT_FAILED', 'D1_DENIAL_EVENT_FAILED', 'D1_REJECTION_EVENT_FAILED',
        'D1_RELEASE_DIAGNOSTICS_FAILED'}, 'release': {'D1_HANDLE_CLOSE_FAILED'},
        'cleanup': {'D1_TEMP_CLOSE_FAILED', 'D1_TEMP_CLEANUP_FAILED'}, 'readback': {'D1_FAILURE_READBACK_FAILED'}}
    errors = receipt['errors']
    if type(errors) is not list or len(errors) > 16:
        return False
    for error in errors:
        if not isinstance(error, dict) or set(error) != {'stage', 'code'} or type(error['stage']) is not str or error['stage'] not in stages:
            return False
        if type(error['code']) is not str or error['code'] not in ({'D1_' + error['stage'].upper() + '_FAILED'} | secondary.get(error['stage'], set())):
            return False
    reasons = receipt['null_reasons']
    nullable = set(observations) | {'prepared_candidate', 'barrier_sequence'}
    if not isinstance(reasons, dict) or set(reasons) != {key for key in nullable if receipt[key] is None}:
        return False
    return all(type(reason) is str and (reason == 'NOT_REACHED'
        or key == 'final_observation' and reason == 'READBACK_FAILED') for key, reason in reasons.items())


def _d1_phase_payload(phase, payload, mode):
    """Validate actual admission witnesses, not only an authenticated header."""
    if not _d1_payload(payload):
        return False
    if phase in {'GUARD_READY', 'ARMED'}:
        return payload is None
    if phase not in {'TARGET_READY', 'CANDIDATE_READY', 'POSTCOMMIT_READY'}:
        return payload is not None
    if payload is None or payload['observed_sha256'] is None or payload['observed_bytes'] is None:
        return False
    if mode == 'read_final':
        return (phase == 'TARGET_READY' and payload['receipt'] is None and payload['status'] == 'VALIDATED'
            and payload['mutex_name'] is None and payload['validator_calls'] == 0)
    receipt = payload['receipt']
    if (receipt is None or receipt['lock_state'] != 'OWNED' or receipt['target_validation'] != 'ADMITTED'
            or receipt['diagnostics_state'] != 'PRECOMMIT_CONFIRMED' or payload['mutex_name'] is None
            or receipt['target_state'] != 'PRESENT' or receipt['lock_observation'] not in {'NORMAL', 'ABANDONED'}
            or payload['validator_calls'] != 1 or receipt['lease_entry_observation'] is None
            or not receipt['lease_entry_observation']['exists'] or receipt['barrier_sequence'] is None
            or len(receipt['mandatory_sequences']) < (5 if phase == 'POSTCOMMIT_READY' else 2)
            or receipt['barrier_sequence'] <= max(receipt['mandatory_sequences'], default=0)):
        return False
    entry = receipt['lease_entry_observation']
    if phase == 'TARGET_READY':
        return (receipt['commit_state'] == 'NOT_ATTEMPTED' and payload['status'] == 'VALIDATED'
            and payload['observed_sha256'] == entry['sha256'] and payload['observed_bytes'] == entry['bytes'])
    if receipt['attempt_baseline'] != entry:
        return False
    if phase == 'CANDIDATE_READY':
        partial = b'ticker,begin,close\npartial'
        return (receipt['commit_state'] == 'NOT_ATTEMPTED' and receipt['prepared_candidate'] is None
            and payload['status'] == 'VALIDATED' and payload['observed_bytes'] == len(partial)
            and payload['observed_sha256'] == hashlib.sha256(partial).hexdigest())
    return (receipt['commit_state'] == 'REPLACED' and receipt['prepared_candidate'] is not None
        and receipt['final_observation'] is None and payload['status'] == 'SUCCEEDED')


def d1_worker_bootstrap(root, case, slot):
    c = ensure_context(root, lane='d1', role='d1-worker', install=True)
    ticket = c.ticket
    if (ticket is None or c.role != 'd1-worker' or c.lane != 'd1'
            or case not in D1_CASES or slot not in D1_CASES[case]):
        raise ValueError('D1_ADMISSION_TICKET')
    contract = ticket.get('d1_contract', {})
    mode, target, timeout = D1_CASES[case][slot]
    if ((contract.get('case'), contract.get('slot'), contract.get('mode'), contract.get('target'), contract.get('timeout_s'))
            != (case, slot, mode, target, timeout)
            or contract.get('worker_digest') != hashlib.sha256((c.root / 'tests/d1/d1_worker.py').read_bytes()).hexdigest()
            or ticket['nodeid'] != _d1_node(case)):
        raise ValueError('D1_ADMISSION_TICKET')
    c.d1_contract = dict(contract)
    c.d1_frame_sequence = c.d1_command_sequence = 0
    c.d1_last_phase = None
    set_node(ticket['nodeid'])
    os.environ['PYTEST_CURRENT_TEST'] = ticket['nodeid'] + ' (call)'
    d1_send_frame('GUARD_READY')
    return c


def d1_send_frame(phase, payload=None):
    c = _CONTEXT
    phases = {'GUARD_READY', 'ARMED', 'TARGET_READY', 'CANDIDATE_READY', 'CONTENDED', 'POSTCOMMIT_READY', 'RESULT'}
    if c.role != 'd1-worker' or phase not in phases or not _d1_phase_payload(phase, payload, c.d1_contract['mode']):
        raise ValueError('D1_FRAME_FORMAT')
    if phase == 'CONTENDED' and not getattr(c, 'd1_contended', False):
        raise ValueError('D1_CONTENDED_NOT_OBSERVED')
    if phase in {'TARGET_READY', 'CANDIDATE_READY', 'POSTCOMMIT_READY'} and c.d1_contract['mode'] != 'read_final':
        if not any(r['kind'] == 'mutex' and r.get('owned') for r in _D1_NATIVE_HANDLES.values()):
            raise ValueError('D1_READY_NOT_OWNED')
    if phase == 'TARGET_READY' and c.d1_contract['mode'] == 'read_final':
        witness = getattr(_THREAD_STATE, 'd1_closed_reader', None)
        target = str(Path(c.d1_contract['case_root']) / c.d1_contract['target'])
        if (getattr(_THREAD_STATE, 'd1_open_reader', None) is not None or witness is None
                or witness != (target, payload['observed_sha256'], payload['observed_bytes'])):
            raise ValueError('D1_READY_NOT_OWNED')
    if phase == 'POSTCOMMIT_READY' and (payload is None or payload['receipt'] is None
            or payload['receipt']['commit_state'] != 'REPLACED'):
        raise ValueError('D1_REPLACED_NOT_WITNESSED')
    c.d1_frame_sequence += 1
    frame = dict(_d1_header(c.ticket, os.getpid()), phase=phase, sequence=c.d1_frame_sequence, payload=payload)
    raw = json.dumps(frame, sort_keys=True, ensure_ascii=True).encode() + b'\n'
    if len(raw) > 4096 or c.d1_frame_sequence > 32:
        raise ValueError('D1_FRAME_BOUND')
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    c.d1_last_phase = phase


def d1_wait_command(expected):
    c = _CONTEXT
    if c.role != 'd1-worker' or expected not in {'GO', 'RELEASE'}:
        raise ValueError('D1_COMMAND_SCOPE')
    raw = sys.stdin.buffer.readline(4097)
    try:
        command = json.loads(raw)
    except (ValueError, UnicodeError):
        raise ValueError('D1_COMMAND_FORMAT') from None
    c.d1_command_sequence += 1
    wanted = dict(_d1_header(c.ticket, os.getpid()), command=expected, sequence=c.d1_command_sequence)
    if len(raw) > 4096 or command != wanted:
        raise ValueError('D1_COMMAND_IDENTITY')
    return expected


def d1_finish_worker(exit_code=0):
    c = _CONTEXT
    if c.role != 'd1-worker' or type(exit_code) is not int:
        raise ValueError('D1_FINISH_SCOPE')
    mode = c.d1_contract['mode']
    path = c.report_dir / 'processes' / (c.process_id + '.final.json')
    if mode == 'omit_guard_final':
        c.finished = True
        return
    if mode == 'corrupt_guard_final':
        with _internal():
            with _ORIGINAL_OPEN(path, 'xb') as stream:
                stream.write(b'{D1_CORRUPT_FINAL\n')
        c.finished = True
        return
    if mode == 'foreign_report':
        final = dict(report_metadata(), exit_code=exit_code, counters={}, native_counters={}, native_guard=False,
            expected_counts={}, violations=0, children=[], report_error=False)
        final['run_id'] = '0' * 32
        exclusive_json(path, final)
        c.finished = True
        return
    final = finish(exit_code)
    if mode in {'duplicate_report', 'replayed_report'}:
        exclusive_json(path.with_name(c.process_id + '.final.duplicate.json'), final)


def _d1_node(case):
    if case in {'wrong_ticket', 'wrong_node'}:
        return 'tests/d1/test_guard_admission.py::test_' + case
    base = _D1_CONTROL_NODE if case in _D1_CONTROL_ERRORS else _D1_PROCESS_NODE
    return base + '[' + case + ']'


def _d1_command(child, command):
    phase = child['last_phase']
    if command == 'GO' and phase != 'ARMED' or command == 'RELEASE' and phase not in {
            'TARGET_READY', 'CONTENDED', 'CANDIDATE_READY', 'POSTCOMMIT_READY'}:
        raise ValueError('D1_COMMAND_ORDER')
    child['command_sequence'] += 1
    frame = dict(_d1_header(child['ticket'], child['process'].pid), command=command,
                 sequence=child['command_sequence'])
    raw = json.dumps(frame, sort_keys=True, ensure_ascii=True).encode() + b'\n'
    if len(raw) > 4096:
        raise ValueError('D1_COMMAND_BOUND')
    child['process'].stdin.write(raw)
    child['process'].stdin.flush()


def _d1_drain(child, channel):
    stream = getattr(child['process'], channel)
    bound = 65536 if channel == 'stdout' else 131072
    try:
        while True:
            raw = stream.readline(4097) if channel == 'stdout' else stream.read(4096)
            if not raw:
                break
            remaining = bound - len(child[channel])
            child[channel].extend(raw[:remaining])
            if len(raw) > remaining:
                child['drain_errors'].append('D1_' + channel.upper() + '_BOUND')
                break
            if channel == 'stdout':
                if len(raw) > 4096 or not raw.endswith(b'\n'):
                    child['drain_errors'].append('D1_FRAME_BOUND')
                    break
                try:
                    child['queue'].put_nowait(raw)
                except queue.Full:
                    child['drain_errors'].append('D1_FRAME_BOUND')
                    break
    finally:
        if channel == 'stdout':
            child['stdout_closed'].set()


def _d1_start(case, slot, case_root, deadline):
    c = _CONTEXT
    _d1_validate_admission(case, _d1_admission_claim())
    mode, target, timeout = D1_CASES[case][slot]
    own = case_root / 'workers' / slot
    reports = c.report_dir / 'd1' / case / slot
    logging = c.root / '.f-log/d1' / c.run_id / case / slot
    for path in (own, reports, logging):
        checked_path(path)
        path.mkdir(parents=True, exist_ok=False)
    worker = checked_path(c.root / 'tests/d1/d1_worker.py', c.root / 'tests/d1')
    # Windows venv python.exe — redirector с отдельным PID. Worker использует
    # тот же approved base напрямую; gate остаётся в dev venv. Никакого argv0/
    # prefix spoofing и launcher environment: Popen PID равен worker PID.
    binary = checked_path(c.root / '.f1/python/python.exe', c.root / '.f1/python')
    if (checked_path(sys._base_executable) != binary or sys.version_info[:3] != (3, 14, 8)
            or sys.maxsize != 2**63 - 1 or not sys._is_gil_enabled()
            or hashlib.sha256(binary.read_bytes()).hexdigest() !=
                '434b361fe0c5960d404974feed493db47f2603a1ace2574312104fbed0182c6f'):
        raise ValueError('D1_WORKER_INTERPRETER')
    argv = [str(binary), '-I', '-B', '-X', 'utf8', str(worker), '--case', case, '--slot', slot]
    contract = dict(case=case, slot=slot, mode=mode, target=target, timeout_s=timeout,
        case_root=str(case_root), test_root=str(own), logging_root=str(logging / 'logs'),
        nonce=uuid.uuid4().hex, worker_digest=hashlib.sha256(worker.read_bytes()).hexdigest())
    roots = [case_root / Path(target).parent, own, reports, logging]
    ticket = issue_ticket('d1-worker', argv, roots=roots, expected_exit=0, gate=True, d1_contract=contract)
    if case in _D1_CONTROL_ERRORS:
        _d1_declare_control(ticket)
    environment = child_env(ticket)
    for name in ('PYTEST_ADDOPTS', 'PYTEST_PLUGINS', 'PYTHONPATH', 'PYTHONHOME', 'MDS_OFFLINE_INTERNAL_TICKET'):
        environment.pop(name, None)
    environment['TEMP'] = environment['TMP'] = str(own)
    options = dict(cwd=c.root, env=environment, stdin=subprocess.PIPE,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    original_create = None
    _THREAD_STATE.launch = getattr(_THREAD_STATE, 'launch', 0) + 1
    _THREAD_STATE.d1_launch_token = dict(argv=argv, kwargs=options, used=False)
    try:
        import _winapi
        original_create = _winapi.CreateProcess
        begin_native_launch(ticket)
        _winapi.CreateProcess = lambda *a, **k: native_create_process(original_create, a, k)
        process = subprocess.Popen(argv, **options)
    finally:
        if original_create is not None:
            _winapi.CreateProcess = original_create
            end_native_launch()
        _THREAD_STATE.d1_launch_token = None
        _THREAD_STATE.launch -= 1
    job = _CHILD_JOBS.pop(ticket['ticket_id'], None)
    if job is None:
        process.kill()
        process.wait(timeout=5)
        raise ValueError('D1_JOB_MISSING')
    child = dict(ticket=ticket, process=process, job=job, queue=queue.Queue(maxsize=32),
        stdout=bytearray(), stderr=bytearray(), stdout_closed=threading.Event(), drain_errors=[],
        frames=[], protocol_errors=[], last_phase=None, sequence=0, command_sequence=0,
        ready=False, timed_out=False, termination=None, terminated=False, settled=False, deadline=deadline)
    # Popen уже закрыл primary thread HANDLE. Не оставляем stale native grant.
    for handle, record in list(_D1_NATIVE_HANDLES.items()):
        if record['kind'] == 'thread' and record.get('ticket_id') == ticket['ticket_id']:
            _D1_NATIVE_HANDLES.pop(handle)
    child['threads'] = [threading.Thread(target=_d1_drain, args=(child, name), daemon=True,
                        name='D1-' + name) for name in ('stdout', 'stderr')]
    for thread in child['threads']:
        thread.start()
    return child


def _d1_frame(child, raw):
    try:
        frame = json.loads(raw)
    except (ValueError, UnicodeError):
        raise ValueError('D1_FRAME_FORMAT') from None
    if (not isinstance(frame, dict) or set(frame) != _D1_FRAME_KEYS
            or not _d1_phase_payload(frame.get('phase'), frame.get('payload'), child['ticket']['d1_contract']['mode'])):
        raise ValueError('D1_FRAME_FORMAT')
    expected = _d1_header(child['ticket'], child['process'].pid)
    if any(frame.get(k) != v for k, v in expected.items()):
        raise ValueError('D1_FRAME_IDENTITY')
    if type(frame['sequence']) is not int or frame['sequence'] != child['sequence'] + 1 or frame['sequence'] > 32:
        raise ValueError('D1_FRAME_ORDER')
    phase = frame['phase']
    previous = child['last_phase']
    transitions = {None: {'GUARD_READY'}, 'GUARD_READY': {'ARMED'},
        'ARMED': {'TARGET_READY', 'CONTENDED', 'RESULT'}, 'CONTENDED': {'TARGET_READY', 'RESULT'},
        'TARGET_READY': {'CANDIDATE_READY', 'POSTCOMMIT_READY', 'RESULT'},
        'CANDIDATE_READY': {'RESULT'}, 'POSTCOMMIT_READY': {'RESULT'}, 'RESULT': set()}
    if phase not in transitions.get(previous, set()):
        raise ValueError('D1_FRAME_ORDER')
    if phase == 'GUARD_READY':
        start = _read_json(Path(child['ticket']['report_dir']) / 'processes' /
                           (child['ticket']['ticket_id'] + '.start.json'))
        keys = ('ticket_id', 'run_id', 'pid', 'tested_sha', 'source_mode', 'source_digest', 'bootstrap_digest', 'policy_digest')
        if any(start.get(key) != expected[key] for key in keys):
            raise ValueError('D1_START_IDENTITY')
    if phase == 'ARMED':
        child['ready'] = True
    if phase in {'CANDIDATE_READY', 'POSTCOMMIT_READY'}:
        contract = child['ticket']['d1_contract']
        target = checked_path(Path(contract['case_root']) / contract['target'])
        if phase == 'CANDIDATE_READY':
            candidates = [checked_path(path, target.parent) for path in target.parent.glob('.d1-*.tmp')]
            if len(candidates) != 1:
                raise ValueError('D1_CANDIDATE_NOT_WITNESSED')
            data = candidates[0].read_bytes()
            observation = {'sha256': frame['payload']['observed_sha256'], 'bytes': frame['payload']['observed_bytes']}
        else:
            data = target.read_bytes()
            observation = frame['payload']['receipt']['prepared_candidate']
        # Parent измеряет реальные bytes checkpoint; payload или WAIT_ABANDONED
        # сами по себе не доказывают ни prepared temp, ни уже выполненный replace.
        if len(data) != observation['bytes'] or hashlib.sha256(data).hexdigest() != observation['sha256']:
            raise ValueError('D1_CANDIDATE_NOT_WITNESSED' if phase == 'CANDIDATE_READY' else 'D1_REPLACED_NOT_WITNESSED')
    if phase == 'POSTCOMMIT_READY' and (frame['payload'] is None or
            frame['payload']['receipt'] is None or frame['payload']['receipt']['commit_state'] != 'REPLACED'):
        raise ValueError('D1_REPLACED_NOT_WITNESSED')
    child['sequence'], child['last_phase'] = frame['sequence'], phase
    child['frames'].append(frame)
    return frame


def _d1_wait(child, wanted, deadline):
    state_deadline = min(deadline, time.monotonic() + 10)
    while time.monotonic() < state_deadline:
        if child['drain_errors']:
            raise ValueError(child['drain_errors'][0])
        try:
            raw = child['queue'].get(timeout=min(.05, max(0, state_deadline - time.monotonic())))
        except queue.Empty:
            if child['stdout_closed'].is_set():
                raise ValueError('D1_READY_NOT_REACHED' if not child['ready'] else 'D1_RESULT_NOT_REACHED')
            continue
        frame = _d1_frame(child, raw)
        if frame['phase'] == wanted:
            return frame
    raise ValueError('D1_STATE_TIMEOUT')


def _d1_armed(case, slot, root, children, deadline):
    if sum(p['process'].poll() is None for p in children.values()) >= 2:
        raise ValueError('D1_PROCESS_BOUND')
    child = _d1_start(case, slot, root, deadline + 5)
    children[slot] = child
    _d1_wait(child, 'ARMED', deadline)
    return child


def _d1_declare_control(ticket):
    case = ticket['d1_contract']['case']
    record = dict(version=1, ticket_id=ticket['ticket_id'], run_id=ticket['run_id'],
        nodeid=ticket['nodeid'], case=case, slot=ticket['d1_contract']['slot'],
        source_digest=ticket['source_digest'], worker_digest=ticket['d1_contract']['worker_digest'],
        expected_errors=sorted(_D1_CONTROL_ERRORS[case]))
    exclusive_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.d1-control.json'), record)


def _d1_terminate(child, phase=None, timed_out=False):
    ticket = child['ticket']
    if phase is not None:
        case = ticket['d1_contract']['case']
        if (case not in _D1_CRASH_PHASE or _D1_CRASH_PHASE[case] != phase
                or ticket['d1_contract']['slot'] != 'holder' or not child['ready']
                or child['last_phase'] != phase or child['process'].poll() is not None
                or not child['frames'] or not _d1_phase_payload(phase, child['frames'][-1]['payload'],
                    ticket['d1_contract']['mode'])):
            raise ValueError('D1_TERMINATION_SCOPE')
        if list((Path(ticket['report_dir']) / 'processes').glob(ticket['ticket_id'] + '.violation.*.json')):
            raise ValueError('D1_TERMINATION_COUNTERS')
        witness = child['frames'][-1]
        declaration = dict(version=1, ticket_id=ticket['ticket_id'], run_id=ticket['run_id'],
            nodeid=ticket['nodeid'], case=case, slot='holder', pid=child['process'].pid,
            phase=phase, ready_witness=witness, ready_digest=_digest(witness),
            expected_errors=['MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'])
        exclusive_json(Path(ticket['report_dir']) / 'tickets' /
                       (ticket['ticket_id'] + '.d1-termination.json'), declaration)
        child['termination'] = declaration
    child['timed_out'] = timed_out
    if not child['job'][0].TerminateJobObject(child['job'][1], 124):
        raise OSError('D1_TERMINATION_FAILED')
    child['terminated'] = True


def _d1_collect(child):
    if child['settled']:
        return child['result']
    process = child['process']
    final, errors = None, []
    settlement_deadline = min(child['deadline'], time.monotonic() + 5)
    try:
        try:
            process.wait(timeout=max(0, settlement_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            if not child['terminated']:
                child['protocol_errors'].append('D1_UNDECLARED_TIMEOUT')
                _d1_terminate(child, timed_out=True)
            try:
                process.wait(timeout=max(0, settlement_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child['protocol_errors'].append('D1_SETTLE_TIMEOUT')
        joins_deadline = settlement_deadline
        for thread in child['threads']:
            thread.join(timeout=max(0, joins_deadline - time.monotonic()))
            if thread.is_alive():
                child['protocol_errors'].append('D1_PIPE_NOT_CLOSED')
        while not child['queue'].empty():
            try:
                _d1_frame(child, child['queue'].get_nowait())
            except ValueError as error:
                child['protocol_errors'].append(str(error))
        child['protocol_errors'].extend(child['drain_errors'])
        record_child_exit(child['ticket'], process.returncode, child['timed_out'])
        # Parent observation не заменяет отсутствующий final умершего worker.
        observation = dict(version=1, ticket_id=child['ticket']['ticket_id'],
            run_id=child['ticket']['run_id'], pid=process.pid,
            nodeid=child['ticket']['nodeid'], protocol_errors=sorted(set(child['protocol_errors'])))
        destination = Path(child['ticket']['report_dir']) / 'd1/observations' / (child['ticket']['ticket_id'] + '.json')
        exclusive_json(destination, observation)
        errors, final = _validate_ticket(child['ticket'])
    except Exception:
        # Cleanup выполняется даже при corrupt/read/report ошибке. Исходный
        # exception/path не публикуется и не превращается в expected waiver.
        errors.append('D1_REPORT_RECONCILIATION_FAILED')
    finally:
        # Job и pipes owned до reconciliation; каждый ресурс закрывается даже
        # если предыдущий close или report parser отказал. Нет fabricated exit.
        try:
            _job_close(child['job'], terminate=process.poll() is None)
        except Exception:
            errors.append('D1_JOB_CLOSE_FAILED')
        for stream in (process.stdin, process.stdout, process.stderr):
            try:
                stream.close()
            except Exception:
                errors.append('D1_PIPE_CLOSE_FAILED')
        if process.returncode is not None:
            handle = _d1_handle(process._handle)
            try:
                process._handle.Close()
            except Exception:
                errors.append('D1_PROCESS_HANDLE_CLOSE_FAILED')
            _D1_NATIVE_HANDLES.pop(handle, None)
    errors.extend(child['protocol_errors'])
    payload = next((f['payload'] for f in reversed(child['frames']) if f['phase'] == 'RESULT'), None)
    result = dict(actual_exit=process.returncode, timed_out=child['timed_out'],
        report_errors=sorted(set(errors)), validated=not errors, payload=payload,
        frames=child['frames'], termination=child['termination'],
        stdout=bytes(child['stdout']).decode('utf-8', errors='replace'),
        stderr=bytes(child['stderr']).decode('utf-8', errors='replace'),
        counters=final.get('counters') if final is not None else None,
        logging_run_state='INCOMPLETE' if child['termination'] is not None or process.returncode != 0 else 'CALLER_OWNED')
    child['settled'], child['result'] = True, result
    return result


def _d1_probe_mutex_absent(directory):
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel.GetFileInformationByHandleEx.restype = wintypes.BOOL
    kernel.OpenMutexW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    kernel.OpenMutexW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.CreateFileW(str(directory), 0x80, 7, None, 3, 0x02200000, None)
    if _d1_handle(handle) in {0, ctypes.c_void_p(-1).value}:
        raise OSError('D1_PROBE_IDENTITY_FAILED')
    try:
        data = ctypes.create_string_buffer(24)
        if not kernel.GetFileInformationByHandleEx(handle, 18, ctypes.byref(data), 24):
            raise OSError('D1_PROBE_IDENTITY_FAILED')
        name = 'Global\\MDS-D1-v1-' + hashlib.sha256(b'MDS-D1-v1\0' + data.raw).hexdigest()
    finally:
        kernel.CloseHandle(handle)
    _THREAD_STATE.d1_late_probe = True
    try:
        ctypes.set_last_error(0)
        mutex = kernel.OpenMutexW(0x00100000, False, name)
        error = ctypes.get_last_error()
    finally:
        _THREAD_STATE.d1_late_probe = False
    if mutex:
        kernel.CloseHandle(mutex)
        raise ValueError('D1_OLD_MUTEX_STILL_EXISTS')
    if error != 2:
        raise OSError('D1_MUTEX_ABSENCE_UNVERIFIED')
    return True


def run_d1_case(case):
    """Fixed asynchronous controller; only typed evidence escapes this function."""
    global _D1_CONTROLLER_ACTIVE
    c = _CONTEXT
    node = getattr(c, 'nodeid', '') if c is not None else ''
    if c is None or c.lane != 'd1' or c.role != 'lane-worker' or sys.platform != 'win32':
        raise ValueError('D1_ADMISSION_CONTEXT')
    if case not in D1_CASES and case not in {'wrong_ticket', 'wrong_node'}:
        raise ValueError('D1_ADMISSION_CASE')
    if node != _d1_node(case):
        raise ValueError('D1_ADMISSION_NODE')
    if case in {'wrong_ticket', 'wrong_node'}:
        # Та же admission, которая предшествует реальной выдаче ticket/Popen.
        # Изменённый private claim не создаёт ни ticket, ни missing-report waiver.
        claim = _d1_admission_claim()
        claim['ticket_id' if case == 'wrong_ticket' else 'nodeid'] = '0' * 32
        try:
            _d1_validate_admission(case, claim)
        except ValueError as error:
            return dict(case=case, launched=False, slots={}, expected_controls=True,
                report_errors=[str(error)])
        raise ValueError('D1_NEGATIVE_ADMISSION_ACCEPTED')
    if _D1_CONTROLLER_ACTIVE:
        raise ValueError('D1_CONTROLLER_REENTRY')
    _D1_CONTROLLER_ACTIVE = True
    root = checked_path(c.root / '.f3/d1/tmp' / c.run_id / case, c.root / '.f3/d1/tmp')
    children, errors, mutex_absent = {}, [], None
    # Из 40 seconds пять зарезервированы для общей reconciliation. Все waits
    # и joins используют один absolute deadline, не новые бюджеты на child.
    overall_deadline = time.monotonic() + 40
    deadline = overall_deadline - 5
    try:
        root.mkdir(parents=True, exist_ok=False)
        (root / 'foreign-sentinel').write_bytes(b'D1_FOREIGN_SENTINEL\n')
        fixture = (c.root / 'tests/fixtures/dataset_io/previous.csv').read_bytes()
        if hashlib.sha256(fixture).hexdigest() not in {
                '91ad749933aa05b9ad736e2f477f964f35fca09b2e750242bdec1249e84177e2',
                '9b47f248a78dd2062327ed5f91894820863a9291412e7c73cb87012b4b41e967'}:
            raise ValueError('D1_FIXTURE_CHANGED')
        # Только две независимо перечисленные checkout формы disposable fixture.
        # Пользовательский current/candidate никогда не нормализуется.
        seed = b'ticker,begin,close\nSEED,2024-01-01 10:00:00,100.0\n'
        for target in {row[1] for row in D1_CASES[case].values()}:
            path = checked_path(root / target, root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'D1_INVALID_CURRENT\n' if case == 'directory_target_switch' and path.name == 'B.csv' else seed)
        slots = list(D1_CASES[case])
        if case in _D1_CONTROL_ERRORS:
            child = _d1_start(case, 'worker', root, overall_deadline)
            children['worker'] = child
            try:
                _d1_wait(child, 'ARMED', deadline)
                _d1_command(child, 'GO')
                if case == 'park_without_release':
                    _d1_wait(child, 'TARGET_READY', deadline)
                    try:
                        child['process'].wait(timeout=.5)
                        raise ValueError('D1_PARK_EXITED')
                    except subprocess.TimeoutExpired:
                        child['protocol_errors'].append('D1_UNDECLARED_TIMEOUT')
                        _d1_terminate(child, timed_out=True)
                else:
                    _d1_wait(child, 'RESULT', deadline)
            except ValueError as error:
                child['protocol_errors'].append(str(error))
                if not child['ready'] and str(error) != 'D1_READY_NOT_REACHED':
                    child['protocol_errors'].append('D1_READY_NOT_REACHED')
                if case == 'exit_before_ready_7':
                    try:
                        child['process'].wait(timeout=min(5, max(0, overall_deadline - time.monotonic())))
                    except subprocess.TimeoutExpired:
                        child['protocol_errors'].append('D1_UNDECLARED_TIMEOUT')
                if child['process'].poll() is None:
                    _d1_terminate(child)
        elif case.startswith('retry_'):
            child = _d1_armed(case, 'worker', root, children, deadline)
            _d1_command(child, 'GO')
            _d1_wait(child, 'RESULT', deadline)
        elif case in {'independent_directories', 'reader_visibility'}:
            first = _d1_armed(case, slots[0], root, children, deadline)
            _d1_command(first, 'GO')
            _d1_wait(first, 'TARGET_READY', deadline)
            second = _d1_armed(case, slots[1], root, children, deadline)
            _d1_command(second, 'GO')
            _d1_wait(second, 'TARGET_READY', deadline)
            _d1_command(first, 'RELEASE')
            _d1_wait(first, 'RESULT', deadline)
            _d1_command(second, 'RELEASE')
            _d1_wait(second, 'RESULT', deadline)
        else:
            holder = _d1_armed(case, 'holder', root, children, deadline)
            _d1_command(holder, 'GO')
            _d1_wait(holder, 'TARGET_READY', deadline)
            if case in _D1_CRASH_PHASE:
                phase = _D1_CRASH_PHASE[case]
                _d1_wait(holder, phase, deadline)
                if case.startswith('late_restart_'):
                    _d1_terminate(holder, phase)
                    _d1_collect(holder)
                    mutex_absent = _d1_probe_mutex_absent(root / Path(D1_CASES[case]['holder'][1]).parent)
                    if case == 'late_restart_invalid_before_commit':
                        (root / D1_CASES[case]['restart'][1]).write_bytes(b'D1_INVALID_CURRENT\n')
                    follower = _d1_armed(case, slots[1], root, children, deadline)
                    _d1_command(follower, 'GO')
                else:
                    follower = _d1_armed(case, slots[1], root, children, deadline)
                    _d1_command(follower, 'GO')
                    _d1_wait(follower, 'CONTENDED', deadline)
                    _d1_terminate(holder, phase)
                _d1_wait(follower, 'RESULT', deadline)
            else:
                contender = _d1_armed(case, 'contender', root, children, deadline)
                _d1_command(contender, 'GO')
                if case == 'serialize_updates':
                    _d1_wait(contender, 'CONTENDED', deadline)
                    _d1_command(holder, 'RELEASE')
                    _d1_wait(holder, 'RESULT', deadline)
                    _d1_command(contender, 'RELEASE')
                    _d1_wait(contender, 'RESULT', deadline)
                else:
                    _d1_wait(contender, 'RESULT', deadline)
                    _d1_command(holder, 'RELEASE')
                    _d1_wait(holder, 'RESULT', deadline)
    except (ValueError, OSError) as error:
        # Stable protocol codes only; raw application/path exceptions do not
        # become public harness evidence.
        code = str(error)
        errors.append(code if re.fullmatch('D1_[A-Z_]+', code) else 'D1_CONTROLLER_FAILURE')
    finally:
        results = {}
        try:
            cleanup_deadline = min(overall_deadline, time.monotonic() + 5)
            for child in children.values():
                child['deadline'] = min(child['deadline'], cleanup_deadline)
            # Убиваем все незавершённые children прежде последовательного
            # ожидания: второй worker не расходует отдельный settlement budget.
            for child in children.values():
                if (not child['settled'] and not child['terminated']
                        and child['process'].poll() is None and child['last_phase'] != 'RESULT'):
                    child['protocol_errors'].append('D1_UNDECLARED_TIMEOUT')
                    try:
                        _d1_terminate(child, timed_out=True)
                    except (OSError, ValueError):
                        child['protocol_errors'].append('D1_TERMINATION_FAILED')
            for slot, child in children.items():
                results[slot] = _d1_collect(child) if not child['settled'] else child['result']
        finally:
            _D1_CONTROLLER_ACTIVE = False
    recognized = True
    for slot, result in results.items():
        child = children[slot]
        if result['report_errors'] and not _expected_d1_control(child['ticket'], result['report_errors'],
                result['counters'], result['actual_exit'], result['timed_out']):
            errors.extend(result['report_errors'])
            recognized = False
    result = dict(case=case, case_root=root, launched=bool(children), slots=results,
        report_errors=sorted(set(errors)), expected_controls=recognized, mutex_absent=mutex_absent)
    # Review evidence находится вне signed ticket JSON и не выдаёт authority.
    evidence = {key: value for key, value in result.items() if key != 'case_root'}
    exclusive_json(c.report_dir / 'd1' / case / 'case-result.json',
        dict(version=1, run_id=c.run_id, nodeid=node, tested_sha=c.tested_sha,
             source_digest=c.source_digest, result=evidence))
    return result


def _d1_admission_claim():
    c = _CONTEXT
    keys = ('ticket_id', 'run_id', 'lane', 'role', 'tested_sha', 'source_mode',
        'source_digest', 'bootstrap_digest', 'policy_digest')
    if c is None or c.ticket is None:
        raise ValueError('D1_ADMISSION_TICKET')
    claim = {key: c.ticket.get(key) for key in keys}
    claim['nodeid'] = getattr(c, 'nodeid', '')
    return claim


def _d1_validate_admission(case, claim):
    c = _CONTEXT
    expected = _d1_admission_claim()
    if (not isinstance(claim, dict) or set(claim) != set(expected)
            or any(claim[key] != expected[key] for key in expected if key != 'nodeid')
            or expected['ticket_id'] != c.process_id or expected['run_id'] != c.run_id
            or expected['lane'] != 'd1' or expected['role'] != 'lane-worker'):
        raise ValueError('D1_ADMISSION_TICKET')
    if claim['nodeid'] != getattr(c, 'nodeid', '') or claim['nodeid'] != _d1_node(case):
        raise ValueError('D1_ADMISSION_NODE')


def _d1_read_declaration(path, ticket=None):
    record = _read_json(path)
    identifier = path.name.split('.', 1)[0]
    if ticket is None:
        ticket = _read_json(path.parent / (identifier + '.json'))
    case = ticket.get('d1_contract', {}).get('case')
    common = (ticket.get('role') == 'd1-worker' and ticket.get('lane') == 'd1'
        and ticket.get('run_id') == _CONTEXT.run_id
        and record.get('version') == 1 and record.get('ticket_id') == identifier == ticket['ticket_id']
        and record.get('run_id') == ticket['run_id'] and record.get('nodeid') == ticket['nodeid'] == _d1_node(case)
        and record.get('case') == case and record.get('slot') == ticket['d1_contract']['slot'])
    if not common:
        raise ValueError('D1_DECLARATION_IDENTITY')
    if path.name.endswith('.d1-control.json'):
        keys = {'version', 'ticket_id', 'run_id', 'nodeid', 'case', 'slot', 'source_digest', 'worker_digest', 'expected_errors'}
        if (set(record) != keys or case not in _D1_CONTROL_ERRORS
                or record['expected_errors'] != sorted(_D1_CONTROL_ERRORS[case])
                or record['source_digest'] != ticket['source_digest']
                or record['worker_digest'] != ticket['d1_contract']['worker_digest']):
            raise ValueError('D1_CONTROL_DECLARATION')
    elif path.name.endswith('.d1-termination.json'):
        keys = {'version', 'ticket_id', 'run_id', 'nodeid', 'case', 'slot', 'pid', 'phase',
                'ready_witness', 'ready_digest', 'expected_errors'}
        witness = record.get('ready_witness')
        if (set(record) != keys or case not in _D1_CRASH_PHASE or record['slot'] != 'holder'
                or record['phase'] != _D1_CRASH_PHASE[case]
                or record['expected_errors'] != ['MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT']
                or not isinstance(witness, dict) or set(witness) != _D1_FRAME_KEYS
                or record['ready_digest'] != _digest(witness) or witness['phase'] != record['phase']
                or not _d1_phase_payload(witness.get('phase'), witness.get('payload'), ticket['d1_contract']['mode'])
                or any(witness.get(k) != v for k, v in _d1_header(ticket, record['pid']).items())):
            raise ValueError('D1_TERMINATION_DECLARATION')
        start = _read_json(path.parent.parent / 'processes' / (identifier + '.start.json'))
        if start.get('pid') != record['pid']:
            raise ValueError('D1_TERMINATION_PID')
    else:
        raise ValueError('D1_DECLARATION_NAME')
    return record


def _expected_d1_control(ticket, errors, counters, actual_exit, timed_out):
    if ticket.get('role') != 'd1-worker' or ticket.get('lane') != 'd1':
        return False
    directory = Path(ticket['report_dir'])
    identifier = ticket['ticket_id']
    case = ticket['d1_contract']['case']
    termination = directory / 'tickets' / (identifier + '.d1-termination.json')
    control = directory / 'tickets' / (identifier + '.d1-control.json')
    try:
        if termination.is_file():
            _d1_read_declaration(termination, ticket)
            return (set(errors) == {'MISSING_FINAL_REPORT', 'UNEXPECTED_EXIT'}
                    and actual_exit == 124 and timed_out is False and counters is None
                    and not list((directory / 'processes').glob(identifier + '.violation.*.json'))
                    and not list((directory / 'processes').glob(identifier + '.final*.json')))
        if not control.is_file() or case not in _D1_CONTROL_ERRORS:
            return False
        _d1_read_declaration(control, ticket)
        if set(errors) != _D1_CONTROL_ERRORS[case]:
            return False
        expected_exit = 7 if case == 'exit_before_ready_7' else 124 if case in {'park_without_release', 'malformed_ready'} else 0
        if actual_exit != expected_exit or timed_out != (case == 'park_without_release'):
            return False
        journals = [_read_json(p) for p in (directory / 'processes').glob(identifier + '.violation.*.json')]
        if case == 'attempt_extra_child':
            return (counters == {'children': 1} and len(journals) == 1
                    and journals[0].get('kind') == 'children' and journals[0].get('nodeid') == ticket['nodeid'])
        return not journals and (counters in (None, {}))
    except (OSError, ValueError, KeyError, TypeError):
        return False


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
    if _CONTEXT.lane == 'd2' and role in {'lane-worker', 'd2-controller'}:
        if (not command or os.path.normcase(os.path.abspath(command[0]))
                != os.path.normcase(os.path.abspath(sys.executable))):
            return False
    if role == 'd2-controller':
        return (_CONTEXT.lane == 'd2' and _CONTEXT.role == 'lane-worker'
                and len(command) == 8 and command[1:5] == ['-I', '-B', '-X', 'utf8']
                and command[5] == str(_CONTEXT.root / 'tests/d2/d2_worker.py')
                and command[6] == '--case' and command[7] in D2_CASES)
    if role == 'f3-control':
        return _control_case(command) is not None
    if role in {'worker', 'lane-worker'}:
        if _CONTEXT.lane == 'd2':
            return (_CONTEXT.role == 'supervisor' and role == 'lane-worker'
                and len(command) >= 8 and command[1:5] == ['-I', '-B', '-X', 'utf8']
                and command[5] == str(_CONTEXT.root / 'tools/offline_tests.py')
                and command[6:8] == ['--execute-lane', 'd2']
                and command[8:] in ([], ['--collect-only'],
                    ['--d2-scope', 'previous-failures'], ['--d2-scope', 'r31a-contract']))
        return (len(command) >= 8 and command[1:5] == ['-I', '-B', '-X', 'utf8']
                and command[5] == str(_CONTEXT.root / 'tools/offline_tests.py')
                and command[6] == '--execute-lane' and command[7] in {'f1', 'f2', 'flog', 'f3', 'd1'})
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
    if _CONTEXT.lane == 'd1' and _CONTEXT.role == 'd1-worker':
        reject('children')
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
    if _CONTEXT.lane == 'd2' and not _allowed_command(argv, role):
        raise ValueError('D2_CHILD_COMMAND_SCOPE')
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
            options = dict(cwd=cwd or _CONTEXT.root, env=environment,
                stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
            if _CONTEXT.lane == 'd1':
                if role != 'lane-worker' or _CONTEXT.role != 'supervisor':
                    raise ValueError('D1_SYNC_CHILD_REFUSED')
                _THREAD_STATE.d1_launch_token = dict(argv=list(argv), kwargs=options, used=False)
            if _CONTEXT.lane == 'd2':
                if role not in {'lane-worker', 'd2-controller'}:
                    raise ValueError('D2_SYNC_CHILD_REFUSED')
                _THREAD_STATE.d2_launch_token = dict(argv=list(argv), kwargs=options, used=False)
            process = subprocess.Popen(argv, **options)
        finally:
            _THREAD_STATE.d1_launch_token = None
            _THREAD_STATE.d2_launch_token = None
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
    if ticket.get('role') == 'd1-worker':
        path = directory / 'd1/observations' / (identifier + '.json')
        try:
            observed = _read_json(path)
            start_identity = _read_json(directory / 'processes' / (identifier + '.start.json'))
            keys = {'version', 'ticket_id', 'run_id', 'pid', 'nodeid', 'protocol_errors'}
            if (set(observed) != keys or observed['version'] != 1
                    or observed['ticket_id'] != identifier or observed['run_id'] != ticket['run_id']
                    or observed['nodeid'] != ticket['nodeid'] or type(observed['pid']) is not int
                    or observed['pid'] != start_identity.get('pid')
                    or not isinstance(observed['protocol_errors'], list)
                    or any(not isinstance(p, str) or not re.fullmatch('D1_[A-Z_]+', p)
                           for p in observed['protocol_errors'])):
                raise ValueError('D1_OBSERVATION_FORMAT')
            errors.extend(observed['protocol_errors'])
        except FileNotFoundError:
            errors.append('D1_MISSING_OBSERVATION')
        except (OSError, ValueError, KeyError, TypeError):
            errors.append('D1_CORRUPT_OBSERVATION')
    return (sorted(set(errors)) if ticket.get('role') == 'd1-worker' else errors), final


def _expected_control(ticket, errors, final):
    if ticket.get('role') in _D2_ROLES:
        return _expected_d2_control(ticket, errors, final)
    if ticket.get('role') == 'd1-worker':
        path = Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.exit.json')
        try:
            observed = _read_json(path)
            return _expected_d1_control(ticket, errors, final.get('counters') if final is not None else None,
                                       observed.get('actual_exit'), observed.get('timed_out'))
        except (OSError, ValueError):
            return False
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
        if c.lane == 'd1' and path.name.endswith(('.d1-control.json', '.d1-termination.json')):
            try:
                _d1_read_declaration(path)
            except (OSError, ValueError, KeyError, TypeError):
                errors.append('D1_CORRUPT_DECLARATION')
            continue
        if path.name.endswith(('.exit.json', '.control.json', '.command-control.json')):
            continue
        if c.lane == 'd2' and '.d2-' in path.name:
            try:
                declaration = _d2_read_declaration(path)
                if path.name.endswith('.d2-controller-cleanup.json') and (
                        declaration['settled'] is not True or declaration.get('error_code') is not None):
                    errors.append('D2_CONTROLLER_CLEANUP_FAILED')
            except (OSError, ValueError, KeyError, TypeError):
                errors.append('D2_CORRUPT_DECLARATION')
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


# %% D2: закрытые disposable cases и собственные Windows witnesses
D2_CASES = frozenset({
    'baseline_google_swallowed', 'baseline_ftp_root_swallowed',
    'baseline_ftp_dividends_swallowed', 'baseline_ftp_tickers_swallowed',
    'baseline_exit23', 'baseline_hang', 'success', 'google_error', 'ftp_error',
    'exit23', 'system_exit0', 'system_exit_string', 'abnormal_before_ready',
    'abnormal_after_final', 'start_google_before', 'start_ftp_before',
    'start_google_after_native', 'start_ftp_after_native', 'hang_google', 'hang_ftp',
    'cleanup_unsettled', 'partial_ftp', 'partial_google', 'checkpoint_missing',
    'checkpoint_corrupt', 'checkpoint_oversize', 'checkpoint_stale_run',
    'checkpoint_stale_pid', 'checkpoint_stale_sequence', 'listener_failure',
    'ack_failure', 'normal_without_final', 'final_then_exit23',
    'bootstrap_replay_failure', 'bootstrap_import_failure', 'bootstrap_unpickle_failure',
    'foreign_handle', 'canary_positive_control', 'native_nul_read_fault',
    'native_nul_write_fault', 'native_dup_read_fault', 'native_dup_write_fault',
    'native_create_fault', 'd1_killed_holder',
    'parent_finalization_raises', 'parent_finalization_incomplete',
})
_D2_ROLES = {'d2-controller', 'd2-worker', 'd2-baseline-worker', 'd2-lock-worker'}
_D2_PREFIX = ("import os,sys,importlib.util;"
    "_s=importlib.util.spec_from_file_location('mds_offline_guard',os.path.join(os.environ['MDS_OFFLINE_ROOT'],'tools','offline_guard.py'));"
    "_g=importlib.util.module_from_spec(_s);sys.modules[_s.name]=_g;_s.loader.exec_module(_g);"
    "_g.d2_early_bootstrap();")
_D2_QUIET_PREFIX = ("import os,sys;_d2_null=open(os.devnull,'w');"
    "os.dup2(_d2_null.fileno(),1,inheritable=False);"
    "os.dup2(_d2_null.fileno(),2,inheritable=False);"
    "sys.stdout=sys.stderr=sys.__stdout__=sys.__stderr__=_d2_null;")
_D2_CANARY_TEXT = ('D2_SYNTHETIC_CANARY_RAW D2_FAKE_PASSWORD D2_FAKE_PRIVATE_KEY '
    'D2_SYNTHETIC_CANARY_URL=https://D2_SYNTHETIC_CANARY.invalid/private?token=D2_FAKE_PASSWORD '
    'D2_SYNTHETIC_CANARY_PATH=C:/D2_SYNTHETIC_CANARY_PRIVATE/secret.json')
_D2_FAULT_PHASES = {
    'baseline_google_swallowed': ('google', 'legacy_google_clear'),
    'baseline_ftp_root_swallowed': ('ftp', 'legacy_ftp_root'),
    'baseline_ftp_dividends_swallowed': ('ftp', 'legacy_ftp_dividends'),
    'baseline_ftp_tickers_swallowed': ('ftp', 'legacy_ftp_tickers'),
    'baseline_exit23': ('google', 'legacy_exit23'),
    'baseline_hang': ('google', 'legacy_hang'),
    'google_error': ('google', 'target_error'), 'ftp_error': ('ftp', 'target_error'),
    'exit23': ('google', 'target_exit23'), 'system_exit0': ('google', 'target_system_exit0'),
    'system_exit_string': ('google', 'target_system_exit_string'),
    'abnormal_before_ready': ('google', 'exit_before_ready'),
    'abnormal_after_final': ('google', 'exit_after_worker_final'),
    'normal_without_final': ('google', 'without_worker_final'),
    'final_then_exit23': ('google', 'exit_after_checkpoint'),
    'start_google_before': ('google', 'start_before_native'),
    'start_ftp_before': ('ftp', 'start_before_native'),
    'start_google_after_native': ('google', 'start_serialization_failure'),
    'start_ftp_after_native': ('ftp', 'start_serialization_failure'),
    'bootstrap_replay_failure': ('google', 'bootstrap_replay'),
    'bootstrap_import_failure': ('google', 'bootstrap_dependency_import'),
    'bootstrap_unpickle_failure': ('google', 'bootstrap_unpickle'),
    'hang_google': ('google', 'target_hang'), 'hang_ftp': ('ftp', 'target_hang'),
    'cleanup_unsettled': ('google', 'target_hang'),
    'partial_google': ('google', 'partial_transfer'), 'partial_ftp': ('ftp', 'partial_transfer'),
    'ack_failure': ('google', 'ack_closed'), 'listener_failure': ('controller', 'listener_stopped'),
    'parent_finalization_raises': ('controller', 'finalization_raises'),
    'parent_finalization_incomplete': ('controller', 'seal_inventory_failure'),
    'd1_killed_holder': ('holder', 'lock_holder_ready'),
}
for _d2_case in D2_CASES:
    if _d2_case.startswith('checkpoint_'):
        _D2_FAULT_PHASES[_d2_case] = ('google', _d2_case)
    elif _d2_case.startswith('native_'):
        _D2_FAULT_PHASES[_d2_case] = ('google', _d2_case)


def d2_fault_witness(phase, *, worker_id=None, request=None, attempted=None):
    """Exclusive fixed-phase receipt, emitted at the actual fixture operation."""
    c = _CONTEXT
    if c is None or c.lane != 'd2' or c.role not in _D2_ROLES:
        raise ValueError('D2_FAULT_SCOPE')
    ticket = request['ticket'] if request is not None else c.ticket
    contract = ticket['d2_contract']
    selected = contract['worker_id'] if worker_id is None else worker_id
    case = contract['case']
    expected = _D2_FAULT_PHASES.get(case)
    if phase == 'stdio_canary':
        valid = selected in {'google', 'ftp'} and c.role == 'd2-worker' and attempted == 6
    else:
        valid = expected == (selected, phase)
    if (not valid or request is not None and _D2_REQUESTS.get(ticket['ticket_id']) is not request
            or request is None and selected != contract['worker_id'] and c.role != 'd2-controller'):
        raise ValueError('D2_FAULT_PHASE')
    source = 'native' if request is not None else 'controller' if c.role == 'd2-controller' else 'worker'
    record = dict(version=1, case=case, worker_id=selected, phase=phase, count=1,
        pid=os.getpid(), source=source, ticket_id=ticket['ticket_id'], run_id=ticket['run_id'], nodeid=ticket['nodeid'])
    if phase == 'stdio_canary':
        record['attempted'] = attempted
    path = c.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-fault-' + selected + '-' + phase + '.json')
    exclusive_json(path, record)
    return record


def d2_write_canary_streams():
    if _CONTEXT.role != 'd2-worker' or not _d2_fixture_profile_admitted():
        raise ValueError('D2_STDIO_CANARY_SCOPE')
    raw = (_D2_CANARY_TEXT + '\n').encode('ascii')
    attempted = 0
    for fd in (1, 2):
        os.write(fd, raw)
        attempted += 1
    for stream in (sys.stdout, sys.stderr, sys.__stdout__, sys.__stderr__):
        stream.write(_D2_CANARY_TEXT + '\n')
        stream.flush()
        attempted += 1
    if attempted != 6:
        raise ValueError('D2_STDIO_CANARY_COUNT')
    record = d2_fault_witness('stdio_canary', attempted=attempted)
    return record


def d2_fault_witnesses():
    c = _CONTEXT
    if c.role != 'd2-controller':
        raise ValueError('D2_FAULT_READ_SCOPE')
    tickets = {ticket['ticket_id']: ticket for ticket in [c.ticket, *c.children]}
    found = {}
    for path in c.report_dir.glob('tickets/*.d2-fault-*.json'):
        record = _d2_read_declaration(path)
        ticket = tickets.get(record.get('ticket_id'))
        if ticket is None:
            continue
        contract = ticket['d2_contract']
        if (record.get('version') != 1 or record.get('count') != 1 or record.get('case') != contract['case']
                or record.get('run_id') != ticket['run_id'] or record.get('nodeid') != ticket['nodeid']):
            raise ValueError('D2_FAULT_WITNESS_IDENTITY')
        found.setdefault(record['worker_id'], []).append(record)
    return found


def d2_atomic_fault(error):
    """Fixed fixture diagnostic for the actual publication syscall failure."""
    c = _CONTEXT
    caller = sys._getframe(1)
    if (c.role != 'd2-worker' or caller.f_code.co_name != 'atomic_checkpoint'
            or _absolute(caller.f_code.co_filename) != c.root / 'tests/d2/d2_worker.py'):
        raise ValueError('D2_ATOMIC_FAULT_SCOPE')
    upload = sys.modules['upload']
    with _ORIGINAL_OPEN(c.root / 'upload.py', encoding='utf-8-sig') as stream:
        lines = stream.read().splitlines()
    operation, errno, winerror = 'OTHER', None, None
    current, seen = error, set()
    for _ in range(8):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if isinstance(current, OSError):
            errno = current.errno if type(current.errno) is int and 0 <= current.errno <= 65535 else None
            value = getattr(current, 'winerror', None)
            winerror = value if type(value) is int and 0 <= value <= 65535 else None
            trace = current.__traceback__
            while trace is not None:
                if (_absolute(trace.tb_frame.f_code.co_filename) == c.root / 'upload.py'
                        and trace.tb_frame.f_code.co_name in {'_d2_atomic_json', '_d2_slot_atomic_json'}):
                    line = lines[trace.tb_lineno - 1].strip()
                    operation = ('REPLACE' if line == 'os.replace(pending, path)' else
                        'OPEN_PENDING' if "with pending.open('xb')" in line else
                        'INVENTORY' if 'child.lstat()' in line else 'OTHER')
                trace = trace.tb_next
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    count = getattr(c, 'd2_atomic_fault_count', 0) + 1
    c.d2_atomic_fault_count = count
    contract = c.ticket['d2_contract']
    code = upload._d2_code(error, fallback='D2_RESULT_WRITE_FAILED')
    if code not in {'D2_RESULT_WRITE_FAILED', 'D2_RESULT_LIMIT', 'D2_CHECKPOINT_INVALID', 'D2_SOURCE_INVALID', 'D2_LIMITS_INVALID'}:
        code = 'D2_RESULT_WRITE_FAILED'
    record = dict(version=1, ticket_id=c.process_id, run_id=c.run_id, nodeid=c.ticket['nodeid'],
        case=contract['case'], worker_id=contract['worker_id'], pid=os.getpid(), count=count,
        operation=operation, error_code=code,
        errno=errno, winerror=winerror)
    exclusive_json(c.report_dir / 'tickets' / (c.process_id + '.d2-atomic-fault-' + str(count) + '.json'), record)


def _d2_read_declaration(path):
    """Validate every D2 side receipt against the immutable issued graph."""
    c = _CONTEXT
    identifier = path.name.split('.', 1)[0]
    ticket = _read_json(path.parent / (identifier + '.json'))
    record = _read_json(path)
    contract = ticket.get('d2_contract', {})
    case, worker = contract.get('case'), contract.get('worker_id')
    if (c is None or c.lane != 'd2' or ticket.get('lane') != 'd2'
            or ticket.get('role') not in _D2_ROLES or case not in D2_CASES
            or ticket.get('ticket_id') != identifier or record.get('ticket_id') != identifier
            or ticket.get('run_id') != c.run_id or record.get('run_id') != c.run_id
            or ticket.get('tested_sha') != c.tested_sha or ticket.get('source_digest') != c.source_digest
            or ticket.get('digest') != _digest({k: v for k, v in ticket.items() if k != 'digest'})
            or ticket.get('nodeid') != _d2_node(case)):
        raise ValueError('D2_DECLARATION_IDENTITY')
    start_path = path.parent.parent / 'processes' / (identifier + '.start.json')
    start = _read_json(start_path) if start_path.is_file() else None
    if start is not None and (start.get('ticket_id') != identifier or start.get('run_id') != c.run_id
            or start.get('tested_sha') != ticket['tested_sha'] or start.get('source_digest') != ticket['source_digest']):
        raise ValueError('D2_DECLARATION_START')
    suffix = path.name[len(identifier):]
    if suffix == '.d2-native-created.json':
        keys = {'version', 'ticket_id', 'run_id', 'nodeid', 'case', 'worker_id', 'pid',
            'controller_pid', 'native_calls', 'witness'}
        parent = _read_json(path.parent.parent / 'processes' / (ticket['parent_id'] + '.start.json'))
        if (set(record) != keys or record.get('version') != 1 or record.get('case') != case
                or record.get('nodeid') != ticket['nodeid'] or record.get('worker_id') != worker
                or ticket['role'] == 'd2-controller' or type(record.get('pid')) is not int or record['pid'] <= 0
                or type(record.get('native_calls')) is not int or record['native_calls'] != 1
                or record.get('witness') != 'GUARD_CREATEPROCESS'
                or parent.get('run_id') != c.run_id or parent.get('role') != 'd2-controller'
                or record.get('controller_pid') != parent.get('pid')
                or start is not None and start.get('pid') != record['pid']):
            raise ValueError('D2_NATIVE_CREATED_DECLARATION')
    elif suffix.startswith('.d2-atomic-fault-'):
        keys = {'version', 'ticket_id', 'run_id', 'nodeid', 'case', 'worker_id', 'pid', 'count',
            'operation', 'error_code', 'errno', 'winerror'}
        if (set(record) != keys or record.get('version') != 1 or record.get('case') != case
                or record.get('nodeid') != ticket['nodeid'] or record.get('worker_id') != worker
                or ticket['role'] != 'd2-worker' or start is None or record.get('pid') != start.get('pid')
                or type(record.get('count')) is not int or not 1 <= record['count'] <= 1036
                or suffix != '.d2-atomic-fault-' + str(record['count']) + '.json'
                or record.get('operation') not in {'REPLACE', 'OPEN_PENDING', 'INVENTORY', 'OTHER'}
                or record.get('error_code') not in {'D2_RESULT_WRITE_FAILED', 'D2_RESULT_LIMIT',
                    'D2_CHECKPOINT_INVALID', 'D2_SOURCE_INVALID', 'D2_LIMITS_INVALID'}
                or any(record.get(key) is not None and (type(record[key]) is not int or not 0 <= record[key] <= 65535)
                    for key in ('errno', 'winerror'))):
            raise ValueError('D2_ATOMIC_FAULT_DECLARATION')
    elif suffix.startswith('.d2-fault-'):
        keys = {'version', 'case', 'worker_id', 'phase', 'count', 'pid', 'source', 'ticket_id', 'run_id', 'nodeid'}
        selected, phase, source = record.get('worker_id'), record.get('phase'), record.get('source')
        if phase == 'stdio_canary':
            keys.add('attempted')
            admitted = (ticket['role'] == 'd2-worker' and selected == worker and worker in {'google', 'ftp'}
                and source == 'worker' and type(record.get('attempted')) is int and record['attempted'] == 6)
        else:
            admitted = _D2_FAULT_PHASES.get(case) == (selected, phase)
        if (set(record) != keys or not admitted or record.get('version') != 1
                or type(record.get('count')) is not int or record['count'] != 1
                or record.get('case') != case or record.get('nodeid') != ticket['nodeid']
                or suffix != '.d2-fault-' + selected + '-' + phase + '.json'
                or type(record.get('pid')) is not int or record['pid'] <= 0):
            raise ValueError('D2_FAULT_DECLARATION')
        if source == 'worker':
            owner = start
            admitted = ticket['role'] != 'd2-controller' and selected == worker
        elif source == 'controller':
            owner = start
            admitted = ticket['role'] == 'd2-controller'
        elif source == 'native':
            owner = _read_json(path.parent.parent / 'processes' / (ticket['parent_id'] + '.start.json'))
            admitted = ticket['role'] == 'd2-worker' and selected == worker and owner.get('role') == 'd2-controller'
        else:
            owner, admitted = None, False
        if (not admitted or owner is None or owner.get('pid') != record['pid']
                or owner.get('run_id') != c.run_id):
            raise ValueError('D2_FAULT_PID')
    elif suffix == '.d2-observation.json':
        keys = {'version', 'ticket_id', 'run_id', 'nodeid', 'case', 'worker_id', 'pid', 'actual_exit',
            'not_started', 'fault_witnesses', 'worker_fence', 'deliberately_unsettled', 'cleanup_requests'}
        if (set(record) != keys or record.get('version') != 1 or record.get('nodeid') != ticket['nodeid']
                or record.get('case') != case or record.get('worker_id') != worker
                or ticket['role'] == 'd2-controller' or type(record['not_started']) is not bool
                or type(record['deliberately_unsettled']) is not bool or not isinstance(record['fault_witnesses'], dict)):
            raise ValueError('D2_OBSERVATION_DECLARATION')
        if record['not_started']:
            if start is not None or record['pid'] is not None or record['actual_exit'] is not None:
                raise ValueError('D2_OBSERVATION_NOT_STARTED')
        elif (type(record['pid']) is not int or
                start is not None and record['pid'] != start.get('pid') or
                start is None and not _d2_before_bootstrap_pid(ticket, record['pid'])):
            raise ValueError('D2_OBSERVATION_PID')
        if record['deliberately_unsettled'] and (case != 'cleanup_unsettled' or worker != 'google'
                or record['not_started'] or record['actual_exit'] is not None):
            raise ValueError('D2_OBSERVATION_UNSETTLED')
        if record['actual_exit'] is not None:
            exit_record = _read_json(path.parent / (identifier + '.exit.json'))
            if (type(record['actual_exit']) is not int or exit_record.get('ticket_id') != identifier
                    or exit_record.get('run_id') != c.run_id or exit_record.get('actual_exit') != record['actual_exit']):
                raise ValueError('D2_OBSERVATION_EXIT')
        for selected, witnesses in record['fault_witnesses'].items():
            if selected not in {'controller', 'google', 'ftp', 'holder', 'contender'} or not isinstance(witnesses, list):
                raise ValueError('D2_OBSERVATION_FAULTS')
            for witness in witnesses:
                fault_path = path.parent / (witness['ticket_id'] + '.d2-fault-' + selected + '-' + witness['phase'] + '.json')
                if _d2_read_declaration(fault_path) != witness or witness['case'] != case:
                    raise ValueError('D2_OBSERVATION_FAULTS')
        fence = record['worker_fence']
        if fence is not None and (not isinstance(fence, dict) or set(fence) != {
                'checkpoint_present', 'checkpoint_execution', 'normal_return', 'checkpoint_pid', 'checkpoint_sequence',
                'checkpoint_diagnostics', 'checkpoint_error_code', 'checkpoint_output_states', 'flog_final'}):
            raise ValueError('D2_OBSERVATION_FENCE')
        if not isinstance(record['cleanup_requests'], list):
            raise ValueError('D2_OBSERVATION_CLEANUP')
        for request in record['cleanup_requests']:
            request_path = path.parent / (request['ticket_id'] + '.d2-cleanup-request-' + request['phase'] + '.json')
            if _d2_read_declaration(request_path) != request or request['case'] != case:
                raise ValueError('D2_OBSERVATION_CLEANUP')
    elif suffix.startswith('.d2-cleanup-request-'):
        keys = {'version', 'ticket_id', 'run_id', 'case', 'nodeid', 'worker_id', 'pid', 'phase', 'alive',
            'accepted', 'native_calls', 'requested_exit', 'settled_after', 'raw_windows_exit_after', 'witness'}
        if (set(record) != keys or record.get('version') != 1 or record.get('case') != case
                or record.get('worker_id') != worker or ticket['role'] != 'd2-worker'
                or record.get('nodeid') != ticket['nodeid'] or record.get('phase') not in {'TERMINATE', 'KILL'}
                or suffix != '.d2-cleanup-request-' + record['phase'] + '.json'
                or start is not None and record.get('pid') != start.get('pid')
                or start is None and not _d2_before_bootstrap_pid(ticket, record.get('pid'))
                or any(type(record.get(key)) is not bool for key in ('alive', 'accepted', 'settled_after'))
                or type(record.get('native_calls')) is not int or record['native_calls'] not in {0, 1}
                or record['accepted'] and record['native_calls'] != 1 or record.get('requested_exit') != 65536
                or record.get('witness') != 'GUARD_RETAINED_HANDLE'
                or record['settled_after'] and type(record.get('raw_windows_exit_after')) is not int
                or not record['settled_after'] and record.get('raw_windows_exit_after') is not None):
            raise ValueError('D2_CLEANUP_REQUEST_DECLARATION')
    elif suffix in {'.d2-containment.json', '.d2-controller-cleanup.json'}:
        keys = {'ticket_id', 'run_id', 'pid', 'actual_exit', 'raw_windows_exit', 'settled', 'witness', 'purpose'}
        cleanup = suffix == '.d2-controller-cleanup.json'
        if cleanup:
            keys.add('error_code')
        if (set(record) != keys or type(record.get('settled')) is not bool
                or record.get('witness') != 'GUARD_RETAINED_HANDLE'
                or record.get('purpose') not in ({'CONTROLLER_EXCEPTION_CONTAINMENT'} if cleanup else
                    {'AFTER_RESULT_CONTAINMENT', 'GUARD_EXIT_CONTAINMENT'})
                or cleanup and record.get('error_code') not in {None, 'D2_CONTROLLER_CLEANUP_WITNESS',
                    'D2_CONTROLLER_CLEANUP_UNSETTLED', 'D2_CONTROLLER_VENDOR_CLOSE_FAILED', 'D2_CONTROLLER_CLEANUP_FAILED'}):
            raise ValueError('D2_CONTAINMENT_DECLARATION')
        if (start is not None and record.get('pid') != start.get('pid')
                or start is None and not _d2_before_bootstrap_pid(ticket, record.get('pid'))):
            raise ValueError('D2_CONTAINMENT_PID')
        raw = record['raw_windows_exit']
        if (record['settled'] and (type(raw) is not int or record['actual_exit'] != (-15 if raw == 65536 else raw))
                or not record['settled'] and (raw is not None or record['actual_exit'] is not None)):
            raise ValueError('D2_CONTAINMENT_EXIT')
    else:
        raise ValueError('D2_DECLARATION_NAME')
    return record


def _d2_before_bootstrap_pid(ticket, pid):
    """Native creation is distinct from entry into Python child bootstrap."""
    contract = ticket['d2_contract']
    if (contract['case'] != 'start_' + contract['worker_id'] + '_after_native'
            or ticket['role'] != 'd2-worker' or type(pid) is not int or pid <= 0):
        return False
    directory = Path(ticket['report_dir']) / 'tickets'
    try:
        created = _d2_read_declaration(directory / (ticket['ticket_id'] + '.d2-native-created.json'))
        phase = _d2_read_declaration(directory / (ticket['ticket_id'] + '.d2-fault-'
            + contract['worker_id'] + '-start_serialization_failure.json'))
        return (created['pid'] == pid and phase['source'] == 'native'
            and phase['pid'] == created['controller_pid'] and phase['count'] == 1)
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _d2_node(case):
    return 'tests/d2/test_supervision.py::test_native_case[' + case + ']'


def _d2_validate_contract(role, contract):
    c = _CONTEXT
    if (c is None or c.lane != 'd2' or not isinstance(contract, dict)
            or set(contract) != {'case', 'worker_id', 'result_root', 'log_root'}
            or contract['case'] not in D2_CASES
            or contract['worker_id'] not in {'controller', 'google', 'ftp', 'holder', 'contender'}):
        raise ValueError('D2_ADMISSION_CONTRACT')
    case = contract['case']
    if role == 'd2-controller':
        if contract['worker_id'] != 'controller':
            raise ValueError('D2_ADMISSION_ROLE_CASE')
        if c.role != 'lane-worker' or getattr(c, 'nodeid', '') != _d2_node(case):
            raise ValueError('D2_ADMISSION_NODE')
    elif (role == 'd2-baseline-worker' and (not case.startswith('baseline_') or contract['worker_id'] not in {'google', 'ftp'})
          or role == 'd2-lock-worker' and (case != 'd1_killed_holder' or contract['worker_id'] not in {'holder', 'contender'})
          or role == 'd2-worker' and (case.startswith('baseline_') or case == 'd1_killed_holder' or contract['worker_id'] not in {'google', 'ftp'})):
        raise ValueError('D2_ADMISSION_ROLE_CASE')
    elif (c.role != 'd2-controller' or c.ticket['d2_contract']['case'] != case
          or getattr(c, 'nodeid', '') != c.ticket['nodeid']):
        raise ValueError('D2_ADMISSION_OWNER')
    if not _owned(contract['result_root']) or not _owned(contract['log_root']):
        raise ValueError('D2_ADMISSION_ROOT')
    if any(ticket['role'] == role and ticket.get('d2_contract', {}).get('case') == case
           and ticket['d2_contract']['worker_id'] == contract['worker_id'] for ticket in c.children):
        raise ValueError('D2_ADMISSION_DUPLICATE')


def _d2_fake_secret(path):
    c = _CONTEXT
    if c is None or c.lane != 'd2' or c.role not in _D2_ROLES or not c.ticket:
        return False
    root = Path(c.ticket['d2_contract']['result_root'])
    return path in {root / 'secrets/moex.foykes.com.json',
                   root / 'secrets/python-304621-1e403209f05f.json'} and _owned(path)


def _d2_fixture_profile_admitted(case=None):
    c = _CONTEXT
    if c is None or c.lane != 'd2' or c.role not in _D2_ROLES or c.ticket is None:
        return False
    contract = c.ticket.get('d2_contract', {})
    return contract.get('case') in D2_CASES and (case is None or contract['case'] == case)


def d2_early_bootstrap():
    """Runs in the fixed quiet -c prefix, before prepare/replay/unpickling."""
    root = checked_path(os.environ['MDS_OFFLINE_ROOT'])
    ticket = _load_ticket(root)
    if ticket is None or ticket['lane'] != 'd2' or ticket['role'] not in _D2_ROLES - {'d2-controller'}:
        raise ValueError('D2_BOOTSTRAP_TICKET')
    argv = list(sys.orig_argv)
    pattern = re.escape(_D2_QUIET_PREFIX + _D2_PREFIX) + r'from multiprocessing.spawn import spawn_main; spawn_main\(parent_pid=(\d+), pipe_handle=(\d+)\)'
    matched = re.fullmatch(pattern, argv[7]) if len(argv) == 9 else None
    if not (len(argv) == 9 and argv[1:7] == ['-I', '-S', '-B', '-X', 'utf8', '-c']
            and matched and argv[8] == '--multiprocessing-fork'):
        raise ValueError('D2_BOOTSTRAP_COMMAND')
    c = ensure_context(root, lane='d2', install=True)
    c.nodeid = ticket['nodeid']
    c.d2_contract = dict(ticket['d2_contract'])
    parent = _read_json(c.report_dir / 'processes' / (ticket['parent_id'] + '.start.json'))
    parent_pid, pipe_handle = map(int, matched.groups())
    if (parent.get('ticket_id') != ticket['parent_id'] or parent.get('run_id') != ticket['run_id']
            or parent.get('role') != 'd2-controller' or parent.get('pid') != parent_pid or pipe_handle <= 0):
        raise ValueError('D2_BOOTSTRAP_PARENT')
    import multiprocessing.spawn as spawn
    c.d2_bootstrap = dict(parent_pid=parent_pid, pipe_handle=pipe_handle, consumed=False,
        main_code=spawn._main.__code__, spawn_code=spawn.spawn_main.__code__)
    return c


def _d2_bootstrap_read_grant(descriptor, mode, flags):
    """One pinned vendor fdopen event; the numeric FD gets no enduring grant."""
    c = _CONTEXT
    proof = getattr(c, 'd2_bootstrap', None)
    if (c.role not in _D2_ROLES - {'d2-controller'} or proof is None or proof['consumed']
            or mode not in {'r', 'rb'} or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)):
        return False
    frame = sys._getframe(2)
    if frame.f_code is not _ORIGINAL_FDOPEN.__code__ or frame.f_locals.get('fd') != descriptor or frame.f_locals.get('mode') != 'rb':
        return False
    frame = frame.f_back
    if frame is None or frame.f_code.co_name != 'guarded_fdopen' or _absolute(frame.f_code.co_filename) != c.root / 'tools/offline_guard.py':
        return False
    main = frame.f_back
    vendor = main.f_back if main else None
    if (main is None or main.f_code is not proof['main_code'] or main.f_locals.get('fd') != descriptor
            or vendor is None or vendor.f_code is not proof['spawn_code'] or vendor.f_locals.get('fd') != descriptor
            or vendor.f_locals.get('parent_pid') != proof['parent_pid']
            or vendor.f_locals.get('pipe_handle') != proof['pipe_handle'] or vendor.f_locals.get('source_process') is None):
        return False
    import msvcrt
    if msvcrt.get_osfhandle(descriptor) != vendor.f_locals.get('new_handle'):
        return False
    proof['consumed'] = True
    return True


def d2_worker_context(case, worker_id):
    c = _CONTEXT
    if (not _d2_fixture_profile_admitted(case) or c.role not in _D2_ROLES - {'d2-controller'}
            or c.ticket['d2_contract']['worker_id'] != worker_id):
        raise ValueError('D2_WORKER_TICKET')
    import d2_worker
    d2_worker.worker_setup(case, worker_id)
    return c


def _d2_prepare_upload_start(process, worker_id, case):
    if not _d2_fixture_profile_admitted(case) or _CONTEXT.role != 'd2-controller':
        raise ValueError('D2_UPLOAD_START_SCOPE')
    target = getattr(process, '_target', None)
    args = getattr(process, '_args', ())
    if (getattr(target, '__module__', '') != 'upload'
            or getattr(target, '__name__', '') != '_d2_worker_entry'
            or target is not getattr(sys.modules.get('upload'), '_d2_worker_entry', None)
            or getattr(process, '_kwargs', None) != {}
            or len(args) != 12 or args[1] != worker_id or worker_id not in {'google', 'ftp'}
            or args[9] != case or not isinstance(args[0], dict)
            or not _owned(args[3])):
        raise ValueError('D2_UPLOAD_START_TARGET')
    fixture = args[8]
    if fixture is not getattr(sys.modules.get('d2_worker'), worker_id + '_target', None):
        raise ValueError('D2_UPLOAD_FIXTURE_TARGET')
    _d2_validate_upload_arguments(process, worker_id, case)
    if case == 'start_' + worker_id + '_before':
        d2_fault_witness('start_before_native', worker_id=worker_id)
        raise RuntimeError('D2_START_FAILED')
    if case == 'start_' + worker_id + '_after_native' or case == 'bootstrap_unpickle_failure' and worker_id == 'google':
        import d2_worker
        args[0]['_d2_fixture_fault'] = d2_worker.SpawnFault('pickle' if case.endswith('after_native') else 'unpickle')
    if case == 'bootstrap_import_failure' and worker_id == 'google':
        args[0]['_d2_fixture_fault'] = sys.modules['d2_import_fault'].fixed_import_target
    parent = _CONTEXT.ticket['d2_contract']
    contract = dict(case=case, worker_id=worker_id, result_root=str(args[3]), log_root=parent['log_root'])
    ticket = issue_ticket('d2-worker', ['multiprocessing.spawn', worker_id],
        roots=_CONTEXT.roots, expected_exit=None, d2_contract=contract, gate=True)
    _D2_PROCESS_TICKETS[process] = ticket
    return ticket


def _d2_validate_fixture_process(process, role, worker_id, case, parent):
    target = getattr(process, '_target', None)
    expected = {'d2-baseline-worker': 'baseline_worker_entry', 'd2-lock-worker': 'lock_worker_entry'}
    if (role not in expected or target is not getattr(sys.modules.get('d2_worker'), expected.get(role, ''), None)
            or target is None or getattr(process, '_kwargs', None) != {}
            or getattr(process, '_args', None) != (worker_id, case, parent['result_root'])
            or worker_id not in ({'google', 'ftp'} if role == 'd2-baseline-worker' else {'holder', 'contender'})
            or (role == 'd2-baseline-worker') != case.startswith('baseline_')
            or role == 'd2-lock-worker' and case != 'd1_killed_holder'):
        raise ValueError('D2_FIXTURE_START_TARGET')


def _d2_validate_upload_arguments(process, worker_id, case):
    import upload
    caller = sys._getframe(2)
    pinned = getattr(_CONTEXT, 'd2_supervise_code', None)
    while caller is not None and caller.f_code is not pinned:
        caller = caller.f_back
    if caller is None:
        raise ValueError('D2_UPLOAD_START_ARGUMENTS')
    values = caller.f_locals
    args = process._args
    session, plan = values['logging_context'], values['source_plan']
    bootstrap = args[0]
    root = Path(_CONTEXT.ticket['d2_contract']['result_root'])
    if (args[2] is not plan[worker_id] or args[3] != str(root / 'results')
            or args[4] != values['attempt_id'] or not re.fullmatch('[0-9a-f]{32}', args[4])
            or args[5] is not values['limits'] or args[6] != str(root / 'project')
            or args[7] != 'D2_FAKE_GOOGLE_DESTINATION' or args[10] is not values['go']
            or args[11] is not values['reservation']
            or values['fixture_case'] != case or values['run_id'] != session['run_id']
            or bootstrap.get('run_id') != session['run_id'] or bootstrap.get('producer_sha') != _CONTEXT.tested_sha
            or bootstrap.get('worker_id') != worker_id or bootstrap.get('_queue') is not session['_queue']
            or bootstrap.get('_token') != session['_expected'][worker_id]
            or bootstrap.get('_run_failed') is not session['_run_failed']
            or bootstrap.get('config') is not session['config']
            or bootstrap.get('_run_root') != session['_run_root']
            or bootstrap.get('_ack') not in session['_receivers'] or not _owned(root / 'results')):
        raise ValueError('D2_UPLOAD_START_ARGUMENTS')
    deadline = values.get('deadline')
    if type(deadline) is not float:
        raise ValueError('D2_UPLOAD_START_DEADLINE')
    if hasattr(_CONTEXT, 'd2_work_deadline') and _CONTEXT.d2_work_deadline != deadline:
        raise ValueError('D2_UPLOAD_START_DEADLINE_CHANGED')
    _CONTEXT.d2_work_deadline = deadline
    proof = upload._d2_validate_reservation(args[11], args[5])
    if (proof['run_id'] != session['run_id'] or proof['attempt_id'] != args[4]
            or proof['producer_sha'] != _CONTEXT.tested_sha
            or any(tuple((item['output_id'], item['group']) for item in plan[name])
                != args[11][3 if name == 'google' else 4] for name in ('google', 'ftp'))):
        raise ValueError('D2_UPLOAD_START_RESERVATION')
    _CONTEXT.d2_go = args[10]
    _CONTEXT.d2_reservation = args[11]


def d2_bind_controller_source(upload):
    c = _CONTEXT
    caller = sys._getframe(1)
    if (c.role != 'd2-controller' or upload is not sys.modules.get('upload')
            or _absolute(caller.f_code.co_filename) != c.root / 'tests/d2/d2_worker.py'
            or caller.f_code.co_name != 'controller' or hasattr(c, 'd2_supervise_code')):
        raise ValueError('D2_CONTROLLER_SOURCE_BINDING')
    target = upload._d2_supervise
    if target.__globals__ is not upload.__dict__ or _absolute(target.__code__.co_filename) != c.root / 'upload.py':
        raise ValueError('D2_CONTROLLER_SOURCE_IDENTITY')
    c.d2_supervise_code = target.__code__
    c.d2_terminate = upload._d2_native_terminate


_D2_READER_UNIT_BASES = {
    'tests/d2/test_supervision.py::test_checkpoint_schema_identity_and_corruption_fail_closed',
    'tests/d2/test_supervision.py::test_worker_sequence_cannot_rewind',
    'tests/d2/test_supervision.py::test_checkpoint_lifecycle_and_content_cannot_regress',
    'tests/d2/test_supervision.py::test_checkpoint_skipped_polls_and_identical_reread_are_valid',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_native_lifetime',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_failed_phase_closes_owner',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_invalid_path_has_no_native_effect',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_parameters_are_closed',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_foreign_callers_are_refused',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_fd_reuse_does_not_close_foreign_owner',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_open_destination_conflict_is_negative',
    'tests/d2/test_guard_admission.py::test_checkpoint_reader_role_and_phase_denied_before_metadata',
    'tests/d2/test_supervision.py::test_parent_reads_only_settled_checkpoint',
    'tests/d2/test_supervision.py::test_ready_reader_closed_before_go',
}
_D2_SLOT_UNIT_BASES = {
    'tests/d2/test_supervision.py::test_slot_writer_preserves_own_checks_without_sibling_metadata',
    'tests/d2/test_supervision.py::test_slot_writer_refuses_forged_reservation',
    'tests/d2/test_supervision.py::test_slot_writer_refuses_existing_pending',
}
_D2_INVENTORY_UNIT_BASES = _D2_READER_UNIT_BASES | {
    'tests/d2/test_supervision.py::test_checkpoint_full_512_rows_all_transitions_fit_64k',
    'tests/d2/test_supervision.py::test_serialized_byte_boundary_exact_and_plus_one',
    'tests/d2/test_supervision.py::test_valid_full_serialization_limit_and_minus_one',
    'tests/d2/test_supervision.py::test_parent_result_byte_boundary_and_plus_one',
    'tests/d2/test_supervision.py::test_total_run_byte_boundary_and_plus_one',
    'tests/d2/test_supervision.py::test_deadline_rechecked_at_release_and_settlement',
    'tests/d2/test_supervision.py::test_parent_reads_only_settled_checkpoint',
    'tests/d2/test_supervision.py::test_unsettled_cleanup_skips_peer_metadata_and_result_write',
    'tests/d2/test_supervision.py::test_go_visible_interruption_marks_unproven_outputs_unknown',
    'tests/d2/test_supervision.py::test_ready_reader_closed_before_go',
    'tests/d2/test_supervision.py::test_full_inventory_requires_stable_phase',
}
_D2_SLOT_NAMES = {'google.json', 'google.pending', 'ftp.json', 'ftp.pending', 'result.json', 'result.pending'}


def _d2_source_frame(name, caller):
    """Bind a private hook to the actual checked-in function, before effects."""
    c, upload = _CONTEXT, sys.modules.get('upload')
    function = getattr(upload, name, None)
    if (c is None or c.lane != 'd2' or function is None or function.__globals__ is not upload.__dict__
            or caller.f_code is not function.__code__ or caller.f_globals is not upload.__dict__
            or _absolute(caller.f_code.co_filename) != c.root / 'upload.py'):
        raise ValueError('D2_PRIVATE_SOURCE_CALLER')
    codes = getattr(c, 'd2_private_codes', None)
    if codes is None:
        codes = c.d2_private_codes = {}
    if name not in codes:
        with _ORIGINAL_OPEN(c.root / 'upload.py', 'rb') as stream:
            compiled = compile(stream.read(), str(c.root / 'upload.py'), 'exec', dont_inherit=True)
        expected = next((part for part in compiled.co_consts if isinstance(part, CodeType) and part.co_name == name), None)
        if expected != function.__code__:
            raise ValueError('D2_PRIVATE_SOURCE_IDENTITY')
        codes[name] = function.__code__
    elif codes[name] is not function.__code__:
        raise ValueError('D2_PRIVATE_SOURCE_IDENTITY')
    return upload


def _d2_phase_record(kind, phase, worker_id, **fields):
    c = _CONTEXT
    history = getattr(c, 'd2_phase_history', None)
    if history is None:
        history = c.d2_phase_history = []
    row = dict(kind=kind, phase=phase, worker_id=worker_id,
        go_released=bool(getattr(c, 'd2_go', None) is not None and c.d2_go.value == 1), **fields)
    history.append(row)
    del history[:-128]
    return row


def _d2_settled_slot(worker_id):
    rows = d2_actual_pid_settlement()
    selected = [row for row in rows if row['worker_id'] == worker_id]
    if (len(selected) != 1 or selected[0]['settled'] is not True
            or selected[0].get('exit_receipt_verified') is not True):
        raise ValueError('D2_SLOT_UNSETTLED')
    return selected[0]


def _d2_graph_quiescent():
    c = _CONTEXT
    rows = d2_actual_pid_settlement()
    created = [request for ticket in c.children
        if ticket['role'] in _D2_ROLES - {'d2-controller'}
        for request in [_D2_REQUESTS.get(ticket['ticket_id'])]
        if request is not None and request.get('observed_pid') is not None]
    absent = [request for ticket in c.children if ticket['role'] in _D2_ROLES - {'d2-controller'}
        for request in [_D2_REQUESTS.get(ticket['ticket_id'])]
        if request is None or request.get('observed_pid') is None]
    no_unknown_birth = all(request is not None and (not request['entered'] or request['rejected_before_create'])
        for request in absent)
    return (no_unknown_birth and len(rows) == len(created)
        and all(row['settled'] and row['exit_receipt_verified'] for row in rows))


def _d2_slot_access(value, operation):
    """Reject a live peer leaf before stat/open can create a native handle."""
    c = _CONTEXT
    if c is None or c.lane != 'd2' or c.role not in {'d2-worker', 'd2-controller'}:
        return
    contract = c.ticket.get('d2_contract', {}) if c.ticket else {}
    root = contract.get('result_root')
    if root is None:
        return
    root = _absolute(root) / 'results' if c.role == 'd2-controller' else _absolute(root)
    selected = _absolute(value)
    if selected.parent != root or selected.name not in _D2_SLOT_NAMES:
        return
    effects = getattr(c, 'd2_slot_effects', None)
    if effects is None:
        effects = c.d2_slot_effects = dict(own_metadata=0, own_opens=0, peer_slot_attempts=0, admitted_writes=0)
    worker_id = selected.name.split('.', 1)[0]
    if c.role == 'd2-worker':
        if worker_id != contract.get('worker_id'):
            effects['peer_slot_attempts'] += 1
            raise ValueError('D2_PEER_SLOT_EFFECT_REFUSED')
        effects['own_metadata' if operation == 'METADATA' else 'own_opens'] += 1
    elif getattr(c, 'd2_go', None) is not None and c.d2_go.value == 1:
        if worker_id in {'google', 'ftp'}:
            _d2_settled_slot(worker_id)
        elif not _d2_graph_quiescent():
            raise ValueError('D2_PARENT_RESULT_UNSETTLED')


def _d2_reader_admission(path):
    c = _CONTEXT
    selected = _absolute(path)
    if Path(path) != selected or selected.name not in {'google.json', 'ftp.json', 'result.json'}:
        raise ValueError('D2_READER_PATH')
    if c.role == 'd2-worker':
        raise ValueError('D2_READER_SCOPE')
    if c.role == 'd2-controller':
        contract = c.ticket['d2_contract']
        if (not _d2_fixture_profile_admitted(contract['case'])
                or selected.parent != _absolute(contract['result_root']) / 'results'):
            raise ValueError('D2_READER_ROOT')
        _d2_slot_access(selected, 'READ')
        worker_id = selected.name.split('.', 1)[0]
        if getattr(c, 'd2_go', None) is not None and c.d2_go.value == 1:
            if worker_id == 'result':
                if not _d2_graph_quiescent():
                    raise ValueError('D2_READER_PHASE')
                return selected, 'OS_SETTLED', worker_id, {}
            row = _d2_settled_slot(worker_id)
            return selected, 'OS_SETTLED', worker_id, {key: row[key] for key in
                ('ticket_id', 'pid', 'actual_exit', 'settled', 'exit_receipt_verified', 'witness')}
        if worker_id == 'result' and c.children and not _d2_graph_quiescent():
            raise ValueError('D2_READER_PHASE')
        return selected, 'READY_BARRIER', worker_id, {}
    if c.role != 'lane-worker' or getattr(c, 'nodeid', '').split('[', 1)[0] not in _D2_READER_UNIT_BASES:
        raise ValueError('D2_READER_SCOPE')
    return selected, 'UNIT', selected.name.split('.', 1)[0], {}


def d2_admit_checkpoint_path(path):
    try:
        _d2_source_frame('_d2_read_json_bytes', sys._getframe(1))
        selected, phase, worker_id, fields = _d2_reader_admission(path)
        _d2_phase_record('CHECKPOINT_READ', phase, worker_id, **fields)
    except (ValueError, KeyError):
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def d2_admit_slot_write(root, name, document, limit, limits, reservation):
    """An independently regenerated reservation grants only this worker's slot."""
    c = _CONTEXT
    try:
        upload = _d2_source_frame('_d2_slot_atomic_json', sys._getframe(1))
        node = getattr(c, 'nodeid', '').split('[', 1)[0]
        if (c.role != 'd2-worker' and (c.role != 'lane-worker' or node not in _D2_SLOT_UNIT_BASES)):
            raise ValueError('D2_SLOT_WRITE_ROLE')
        worker_id = name.removesuffix('.json') if type(name) is str else None
        if worker_id not in {'google', 'ftp'}:
            raise ValueError('D2_SLOT_WRITE_NAME')
        proof = upload._d2_validate_reservation(reservation, limits)
        outputs = upload._d2_outputs(proof['plan'][worker_id])
        upload._d2_validate_checkpoint_record(document, worker_id, run_id=proof['run_id'],
            attempt_id=proof['attempt_id'], producer_sha=proof['producer_sha'], pid=os.getpid(), outputs=outputs)
        if type(limit) is not int or limit != limits['max_checkpoint_bytes']:
            raise ValueError('D2_SLOT_WRITE_LIMIT')
        if c.role == 'd2-worker':
            contract = c.ticket['d2_contract']
            entry = sys._getframe(1)
            while entry is not None and entry.f_code.co_name != '_d2_worker_entry':
                entry = entry.f_back
            if (not _d2_fixture_profile_admitted(contract['case']) or worker_id != contract['worker_id']
                    or _absolute(root) != _absolute(contract['result_root']) or entry is None
                    or entry.f_code is not upload._d2_worker_entry.__code__
                    or entry.f_locals.get('reservation') is not reservation
                    or entry.f_locals['bootstrap']['run_id'] != proof['run_id']
                    or entry.f_locals['bootstrap']['producer_sha'] != proof['producer_sha']
                    or entry.f_locals['attempt_id'] != proof['attempt_id']
                    or tuple((item['output_id'], item['group']) for item in entry.f_locals['channel_plan'])
                        != reservation[3 if worker_id == 'google' else 4]):
                raise ValueError('D2_SLOT_WRITE_RESERVATION')
            c.d2_go = entry.f_locals['go']
            effects = getattr(c, 'd2_slot_effects', None)
            if effects is None:
                effects = c.d2_slot_effects = dict(own_metadata=0, own_opens=0, peer_slot_attempts=0, admitted_writes=0)
            effects['admitted_writes'] += 1
        _d2_phase_record('SLOT_WRITE', 'OWN_SLOT', worker_id,
            reserved_peak_bytes=reservation[6][3], reserved_slot_bytes=reservation[6][0 if worker_id == 'google' else 1])
    except (ValueError, KeyError):
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def d2_admit_full_inventory(root):
    c, caller = _CONTEXT, sys._getframe(1)
    _d2_source_frame('_d2_run_inventory', caller)
    if c.role == 'd2-worker':
        raise RuntimeError('D2_FULL_INVENTORY_PHASE')
    if c.role == 'd2-controller':
        if _absolute(root) != _absolute(c.ticket['d2_contract']['result_root']) / 'results':
            raise RuntimeError('D2_FULL_INVENTORY_ROOT')
        go = getattr(c, 'd2_go', None)
        if go is not None and go.value == 1:
            if not _d2_graph_quiescent():
                raise RuntimeError('D2_FULL_INVENTORY_PHASE')
            phase = 'OS_SETTLED'
        elif c.children:
            frame = caller.f_back
            while frame is not None and frame.f_code is not getattr(c, 'd2_supervise_code', None):
                frame = frame.f_back
            if (frame is None or frame.f_locals.get('ready') != {'google', 'ftp'}
                    or getattr(_THREAD_STATE, 'd2_reader', None) is not None):
                if not _d2_graph_quiescent():
                    raise RuntimeError('D2_FULL_INVENTORY_PHASE')
                phase = 'OS_SETTLED'
            else:
                if set(getattr(c, 'd2_closed_ready', {})) != {'google', 'ftp'}:
                    raise RuntimeError('D2_FULL_INVENTORY_READY_READERS')
                phase = 'READY_BARRIER'
        else:
            phase = 'PREFLIGHT'
    elif c.role == 'lane-worker' and getattr(c, 'nodeid', '').split('[', 1)[0] in _D2_INVENTORY_UNIT_BASES:
        phase = 'UNIT'
    else:
        raise RuntimeError('D2_FULL_INVENTORY_SCOPE')
    # Metadata is inspected only after the independent phase fence above.
    total, files = 0, 0
    with os.scandir(root) as entries:
        for entry in entries:
            if entry.name not in _D2_SLOT_NAMES:
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            info = _ORIGINAL_LSTAT(entry.path)
            if _linked(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            total += info.st_size
            files += 1
    _d2_phase_record('FULL_INVENTORY', phase, 'parent', existing_total_bytes=total,
        file_count=files, single_link_regular=True, graph_quiescent=phase in {'OS_SETTLED', 'PREFLIGHT'},
        reader_active=getattr(_THREAD_STATE, 'd2_reader', None) is not None,
        ready_workers_verified=len(getattr(c, 'd2_closed_ready', {})) if phase == 'READY_BARRIER' else 0)


def _d2_checkpoint_reader_frame(token, frame):
    if (getattr(_THREAD_STATE, 'd2_reader', None) is not token or token.get('ended')
            or frame is not token.get('frame') or frame.f_code is not token.get('code')):
        raise ValueError('D2_READER_CALLER')


def d2_begin_checkpoint_read(path, limit, bindings):
    """A synchronous read owns one native HANDLE, then one CRT descriptor."""
    c, caller = _CONTEXT, sys._getframe(1)
    upload = sys.modules.get('upload')
    function = getattr(upload, '_d2_read_json_bytes', None)
    if (c is None or c.lane != 'd2' or sys.platform != 'win32'
            or function is None or function.__globals__ is not upload.__dict__
            or caller.f_code is not function.__code__ or caller.f_globals is not upload.__dict__
            or _absolute(caller.f_code.co_filename) != c.root / 'upload.py'
            or getattr(_THREAD_STATE, 'd2_reader', None) is not None):
        raise ValueError('D2_READER_CALLER')
    known_code = getattr(c, 'd2_reader_code', None)
    if known_code is None:
        with _ORIGINAL_OPEN(c.root / 'upload.py', 'rb') as stream:
            compiled = compile(stream.read(), str(c.root / 'upload.py'), 'exec', dont_inherit=True)
        expected = next((part for part in compiled.co_consts
            if isinstance(part, CodeType) and part.co_name == '_d2_read_json_bytes'), None)
        if expected != function.__code__:
            raise ValueError('D2_READER_SOURCE')
        c.d2_reader_code = function.__code__
    elif known_code is not function.__code__:
        raise ValueError('D2_READER_SOURCE')
    if (type(bindings) is not dict or set(bindings) != {'CreateFile', 'CloseHandle', 'open_osfhandle'}
            or any(bindings[name] is not _D2_READER_ORIGINAL[name] for name in bindings)
            or type(limit) is not int or not 0 < limit <= 131072):
        raise ValueError('D2_READER_BINDINGS')
    selected, phase, worker_id, fields = _d2_reader_admission(path)
    selected = checked_path(selected)
    if not _owned(selected):
        raise ValueError('D2_READER_ROOT')
    token = dict(frame=caller, code=caller.f_code, path=selected, limit=limit,
        phase=phase, worker_id=worker_id,
        handle=None, fd=None, creating=False, transferring=False, fdopening=False, audit_consumed=False,
        native_close_calls=0, fd_close_calls=0, fd_closed=False, stream_closed=False, ended=False)
    _THREAD_STATE.d2_reader = token
    return token


def _d2_checkpoint_parameters(token, parameters):
    expected = (str(token['path']), 0x80000000, 7, 0, 3, 0x00200080, 0)
    if (type(parameters) is not tuple or len(parameters) != 7 or parameters != expected
            or any(type(value) is not int for value in parameters[1:])
            or type(parameters[0]) is not str):
        raise ValueError('D2_READER_PARAMETERS')


def d2_checkpoint_create_file(token, *parameters):
    _d2_checkpoint_reader_frame(token, sys._getframe(1))
    _d2_checkpoint_parameters(token, parameters)
    if token['handle'] is not None or token['fd'] is not None:
        raise ValueError('D2_READER_HANDLE_OWNER')
    token['creating'] = True
    try:
        handle = _D2_READER_ORIGINAL['CreateFile'](*parameters)
    finally:
        token['creating'] = False
    token['handle'] = handle
    token['created'] = True
    return handle


def d2_checkpoint_open_osfhandle(token, handle, flags):
    _d2_checkpoint_reader_frame(token, sys._getframe(1))
    if (type(handle) is not int or handle != token['handle'] or token['fd'] is not None
            or type(flags) is not int or flags != os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT):
        raise ValueError('D2_READER_TRANSFER_OWNER')
    token['transferring'] = True
    try:
        descriptor = _D2_READER_ORIGINAL['open_osfhandle'](handle, flags)
    finally:
        token['transferring'] = False
    # The native API has transferred ownership; no fallible check follows it.
    token['fd'], token['handle'], token['transferred_handle'] = descriptor, None, handle
    return descriptor


def d2_checkpoint_close_handle(token, handle):
    _d2_checkpoint_reader_frame(token, sys._getframe(1))
    if type(handle) is not int or handle != token['handle'] or token['fd'] is not None:
        raise ValueError('D2_READER_CLOSE_OWNER')
    result = _D2_READER_ORIGINAL['CloseHandle'](handle)
    token['native_close_calls'] += 1
    token['handle'] = None
    return result


def _d2_checkpoint_fdopen_grant(descriptor, mode, flags):
    token = getattr(_THREAD_STATE, 'd2_reader', None)
    if token is None:
        return False
    caller = sys._getframe(1)
    while caller is not None and caller is not token['frame']:
        caller = caller.f_back
    if (caller is None or not token['fdopening'] or token['fd'] != descriptor or token['audit_consumed']
            or mode not in {'r', 'rb'} or token['fd_closed'] or flags & (os.O_WRONLY | os.O_RDWR)
            or caller.f_locals.get('descriptor') != descriptor
            or _D2_READER_ORIGINAL['get_osfhandle'](descriptor) is None
            or os.get_inheritable(descriptor)):
        raise ValueError('D2_READER_DESCRIPTOR')
    # fd owns exactly the HANDLE just transferred by open_osfhandle.
    if _D2_READER_ORIGINAL['get_osfhandle'](descriptor) != token.get('transferred_handle'):
        raise ValueError('D2_READER_DESCRIPTOR')
    token['audit_consumed'] = True
    return True


def d2_checkpoint_stream_closed(token, stream):
    _d2_checkpoint_reader_frame(token, sys._getframe(1))
    if (stream is not token.get('stream') or stream is not token['frame'].f_locals.get('stream') or not stream.closed
            or not token['audit_consumed'] or token['fd_closed']):
        raise ValueError('D2_READER_STREAM_OWNER')
    token['stream_closed'] = True


def d2_end_checkpoint_read(token):
    _d2_checkpoint_reader_frame(token, sys._getframe(1))
    descriptor_closed = token['fd'] is None
    post_close = 'NOT_TRANSFERRED'
    if token['fd'] is not None and token['fd_closed'] and token['fd_close_calls'] == 1:
        # The exact owner binding was checked immediately before accepted close.
        # A listener can already own a reused numerical fd when this returns.
        descriptor_closed = True
        try:
            _ORIGINAL_FSTAT(token['fd'])
            post_close = 'REUSED_OPEN'
        except OSError as error:
            post_close = 'EBADF' if error.errno == 9 else 'OS_ERROR'
    stream = token['frame'].f_locals.get('stream')
    complete = (token['handle'] is None and descriptor_closed
        and (stream is None or stream.closed and token['stream_closed']))
    row = dict(handle_created=token.get('created', False), descriptor_transferred=token['fd'] is not None,
        native_close_calls=token['native_close_calls'], fd_close_calls=token['fd_close_calls'],
        descriptor_closed=descriptor_closed, stream_closed=stream is None or stream.closed,
        fdopen_events=int(token['audit_consumed']), lifetime_complete=complete,
        post_close_observation=post_close)
    history = getattr(_CONTEXT, 'd2_reader_history', None)
    if history is None:
        history = _CONTEXT.d2_reader_history = []
    history.append(row)
    del history[:-128]
    counts = getattr(_CONTEXT, 'd2_reader_counts', None)
    if counts is None:
        counts = _CONTEXT.d2_reader_counts = dict(reads=0, lifetime_complete=0, native_closes=0, fd_closes=0)
    counts['reads'] += 1
    counts['lifetime_complete'] += int(complete)
    counts['native_closes'] += row['native_close_calls']
    counts['fd_closes'] += row['fd_close_calls']
    if (complete and _CONTEXT.role == 'd2-controller' and token['phase'] == 'READY_BARRIER'
            and token['worker_id'] in {'google', 'ftp'}):
        data = token['frame'].f_locals.get('data')
        reservation = getattr(_CONTEXT, 'd2_reservation', None)
        if isinstance(data, bytes) and len(data) <= 65536 and reservation is not None:
            try:
                record = json.loads(data)
            except (ValueError, UnicodeError):
                record = None
            worker_id = token['worker_id']
            births = [request['observed_pid'] for request in _D2_REQUESTS.values()
                if request['ticket']['d2_contract']['worker_id'] == worker_id and request.get('observed_pid') is not None]
            if (isinstance(record, dict) and record.get('kind') == 'D2_WORKER_CHECKPOINT'
                    and record.get('receipt_version') == 1 and record.get('run_id') == reservation[0]
                    and record.get('attempt_id') == reservation[1] and record.get('producer_sha') == reservation[2]
                    and record.get('worker_id') == worker_id and births == [record.get('pid')]
                    and record.get('execution_state') == 'READY' and record.get('normal_return_observed') is False
                    and record.get('diagnostics_state') == 'ACTIVE' and record.get('error_code') is None):
                ready = getattr(_CONTEXT, 'd2_closed_ready', None)
                if ready is None:
                    ready = _CONTEXT.d2_closed_ready = {}
                ready[worker_id] = dict(pid=record['pid'], fd_close_calls=row['fd_close_calls'],
                    native_close_calls=row['native_close_calls'], stream_closed=row['stream_closed'],
                    lifetime_complete=row['lifetime_complete'])
    token['ended'] = True
    token.pop('frame')
    _THREAD_STATE.d2_reader = None
    if not complete:
        raise ValueError('D2_READER_LIFETIME')


def d2_site_positive_control(path):
    c = _CONTEXT
    root = Path(c.ticket['d2_contract']['result_root'])
    if (c.role != 'd2-controller' or not _d2_fixture_profile_admitted('success')
            or Path(path) != root / 'issued-site/site-packages' or not _owned(path)):
        raise ValueError('D2_SITE_CONTROL_SCOPE')
    import site
    marker = root / 'pth-executed'
    if marker.exists():
        raise ValueError('D2_SITE_CONTROL_PREEXECUTED')
    site.addsitedir(str(path))
    return marker.read_bytes() == b'BAD'


def _d2_prepare_fixture_start(process, role, worker_id, case):
    if not _d2_fixture_profile_admitted(case) or _CONTEXT.role != 'd2-controller':
        raise ValueError('D2_FIXTURE_START_SCOPE')
    parent = _CONTEXT.ticket['d2_contract']
    _d2_validate_fixture_process(process, role, worker_id, case, parent)
    if not _owned(parent['result_root']) or not _owned(parent['log_root']):
        raise ValueError('D2_FIXTURE_START_ROOT')
    ticket = issue_ticket(role, ['multiprocessing.spawn', worker_id], expected_exit=None,
        d2_contract=dict(parent, worker_id=worker_id), gate=True)
    _D2_PROCESS_TICKETS[process] = ticket
    return ticket


def d2_begin_native_launch(process):
    import _winapi
    ticket = _D2_PROCESS_TICKETS.get(process)
    if (not isinstance(ticket, dict) or ticket not in _CONTEXT.children
            or ticket['role'] not in _D2_ROLES - {'d2-controller'}
            or getattr(_THREAD_STATE, 'd2_request', None) is not None):
        raise ValueError('D2_NATIVE_LAUNCH_SCOPE')
    original, duplicate = _winapi.CreateProcess, _winapi.DuplicateHandle
    if original is not _D2_WINAPI_ORIGINAL['CreateProcess'] or duplicate is not _D2_WINAPI_ORIGINAL['DuplicateHandle']:
        raise ValueError('D2_NATIVE_FOREIGN_BINDING')
    token = dict(ticket=ticket, original=original, duplicate=duplicate, owner=threading.get_ident(),
        process=process, nul_sources={}, nul_duplicates={}, stdio=None, observed_pid=None,
        original_process_handle=None, entered=False, rejected_before_create=False)
    caller = sys._getframe(1)
    import upload
    if (caller.f_code is not upload._d2_start_worker.__code__ or caller.f_locals.get('process') is not process
            or not isinstance(caller.f_locals.get('state'), dict)):
        raise ValueError('D2_NATIVE_ADAPTER_CALLER')
    token['state'] = caller.f_locals['state']
    arguments = getattr(process, '_args', ())
    bootstrap = arguments[0] if arguments and isinstance(arguments[0], dict) else {}
    channel = bootstrap.get('_queue')
    connections = [bootstrap.get('_ack')]
    if channel is not None:
        connections.extend([channel._reader, channel._writer])
    token['connection_handles'] = {value.fileno() for value in connections if value is not None}
    token['semlock_handles'] = {value._semlock.handle for value in
        (getattr(channel, '_rlock', None), getattr(channel, '_wlock', None), getattr(channel, '_sem', None))
        if value is not None}
    def create(*args, **kwargs):
        if threading.get_ident() != token['owner']:
            return original(*args, **kwargs)
        if getattr(_THREAD_STATE, 'd2_request', None) is not token or token['entered']:
            reject('children')
        token['entered'] = True
        if ticket['d2_contract']['case'] == 'native_create_fault':
            token['rejected_before_create'] = True
            d2_fault_witness('native_create_fault', request=token)
            raise OSError('D2_NATIVE_CREATE_FAILED')
        if len(args) != 9 or kwargs or token['stdio'] is None:
            reject('native')
        startup = args[8]
        read, write = token['stdio']
        handle_list = getattr(startup, 'lpAttributeList', {}).get('handle_list')
        if (not args[4] or startup.hStdInput != read or startup.hStdOutput != write
                or startup.hStdError != write or handle_list != [read, write]
                or not startup.dwFlags & 0x100):
            reject('native')
        return native_create_process(original, args, kwargs)
    def dup(*args, **kwargs):
        if threading.get_ident() != token['owner']:
            return duplicate(*args, **kwargs)
        if kwargs or len(args) != 6:
            reject('native')
        source = int(args[1])
        if source in token['nul_sources']:
            if args[0] != _winapi.GetCurrentProcess() or args[2] != args[0] or args[3:] != (0, True, 2):
                reject('native')
            if ticket['d2_contract']['case'] == 'native_dup_' + token['nul_sources'][source] + '_fault':
                d2_fault_witness(ticket['d2_contract']['case'], request=token)
                raise OSError('D2_NATIVE_DUP_FAILED')
            value = duplicate(*args)
            token['nul_duplicates'][int(value)] = token['nul_sources'][source]
            return value
        record = _D2_NATIVE_HANDLES.get(source)
        if record is not None and record['kind'] == 'process' and record['ticket_id'] == ticket['ticket_id']:
            if args[0] != _winapi.GetCurrentProcess() or args[2] != args[0] or args[3:] != (0, False, 2):
                reject('native')
            value = duplicate(*args)
            _D2_NATIVE_HANDLES[int(value)] = dict(record, retained=True)
            return value
        # Queue/Pipe reduction is the pinned stdlib producer bootstrap. It
        # never grants adapter stdio or an arbitrary workload native handle.
        import multiprocessing.reduction as reduction
        caller = sys._getframe(1)
        from multiprocessing.popen_spawn_win32 import Popen
        from multiprocessing.synchronize import SemLock
        spawning = __import__('multiprocessing.context', fromlist=['get_spawning_popen']).get_spawning_popen()
        lock_reduction = (caller.f_code is reduction.duplicate.__code__
            and caller.f_back.f_code is Popen.duplicate_for_child.__code__
            and spawning is caller.f_back.f_locals.get('self')
            and getattr(spawning, 'pid', None) == token['observed_pid']
            and int(args[2]) == token['original_process_handle'] and args[3:] == (0, False, 2)
            and source in token['semlock_handles'] and caller.f_back.f_back.f_code is SemLock.__getstate__.__code__
            and caller.f_back.f_back.f_locals['self']._semlock.handle == source)
        from multiprocessing.connection import reduce_pipe_connection
        pipe_reduction = (caller.f_code is reduction.DupHandle.__init__.__code__
            and caller.f_back.f_code is reduce_pipe_connection.__code__
            and source in token['connection_handles'] and caller.f_back.f_locals['conn'].fileno() == source)
        if not pipe_reduction and not lock_reduction:
            reject('native')
        return duplicate(*args)
    create._mds_d2_native_guard = True
    create._mds_d2_native_original = original
    token['wrapper'], token['duplicate_wrapper'] = create, dup
    _D2_REQUESTS[ticket['ticket_id']] = token
    _THREAD_STATE.d2_request = token
    _THREAD_STATE.launch = getattr(_THREAD_STATE, 'launch', 0) + 1
    begin_native_launch(ticket)
    _winapi.CreateProcess, _winapi.DuplicateHandle = create, dup
    return token


def d2_native_identity(current, token):
    return (isinstance(token, dict) and getattr(_THREAD_STATE, 'd2_request', None) is token
        and current is token['wrapper'] and current._mds_d2_native_original is token['original']
        and _D2_REQUESTS.get(token['ticket']['ticket_id']) is token)


def d2_register_native_stdio(token, read_handle, write_handle):
    if (getattr(_THREAD_STATE, 'd2_request', None) is not token
            or token['nul_duplicates'].get(int(read_handle)) != 'read'
            or token['nul_duplicates'].get(int(write_handle)) != 'write'
            or read_handle == write_handle or len(token['nul_duplicates']) != 2):
        raise ValueError('D2_NATIVE_STDIO_OWNERSHIP')
    token['stdio'] = (int(read_handle), int(write_handle))


def d2_child_environment(token, environment):
    if getattr(_THREAD_STATE, 'd2_request', None) is not token:
        raise ValueError('D2_NATIVE_ENV_SCOPE')
    return child_env(token['ticket'], environment)


def d2_bootstrap_prefix(token):
    if getattr(_THREAD_STATE, 'd2_request', None) is not token:
        raise ValueError('D2_NATIVE_PREFIX_SCOPE')
    return _D2_PREFIX


def d2_end_native_launch(token):
    import _winapi
    if getattr(_THREAD_STATE, 'd2_request', None) is not token or _winapi.CreateProcess is not token['wrapper']:
        raise ValueError('D2_NATIVE_RESTORE_IDENTITY')
    _winapi.CreateProcess = token['original']
    if _winapi.DuplicateHandle is not token['duplicate_wrapper']:
        raise ValueError('D2_NATIVE_DUP_RESTORE_IDENTITY')
    _winapi.DuplicateHandle = token['duplicate']
    end_native_launch()
    _THREAD_STATE.launch -= 1
    _THREAD_STATE.d2_request = None


def _d2_complete_upload_start(process, state, worker_id, case):
    ticket = _D2_PROCESS_TICKETS.get(process)
    if ticket is None and case == 'start_' + worker_id + '_before':
        return dict(ticket_id=None, pid=None, native_entered=False)
    if ticket is None or ticket['d2_contract']['case'] != case or ticket['d2_contract']['worker_id'] != worker_id:
        raise ValueError('D2_START_COMPLETION_SCOPE')
    request = _D2_REQUESTS.get(ticket['ticket_id'])
    if request is not None:
        request['state'] = state
    pid = request.get('observed_pid') if request is not None else None
    process._mds_d2_observed_pid = pid
    if process.pid is not None:
        if process.pid != pid:
            raise ValueError('D2_START_PID_MISMATCH')
        attach_spawn(process, ticket)
    return dict(ticket_id=ticket['ticket_id'], pid=pid,
                native_entered=bool(request and request['entered']))


def d2_record_fixture_exit(process, *, containment=False):
    ticket = _D2_PROCESS_TICKETS.get(process)
    if ticket is None:
        raise ValueError('D2_EXIT_SCOPE')
    code = process.exitcode
    if code is not None:
        record_child_exit(ticket, code)
        _close_child_job(ticket, terminate=containment)
    return code


def d2_baseline_containment(process):
    ticket = _D2_PROCESS_TICKETS.get(process)
    if (not _d2_fixture_profile_admitted('baseline_hang') or _CONTEXT.role != 'd2-controller'
            or ticket is None or ticket['role'] != 'd2-baseline-worker'
            or ticket['d2_contract']['worker_id'] != 'google'):
        raise ValueError('D2_BASELINE_CONTAINMENT_SCOPE')
    if ticket['ticket_id'] not in _CHILD_JOBS:
        raise ValueError('D2_BASELINE_CONTAINMENT_JOB')
    _close_child_job(ticket, terminate=True)
    return True


def d2_observe_native_state(state, observation):
    """Record actual retained-handle exit, including a failed Process.start."""
    if state.get('_closed'):
        return
    pid = state.get('_native_pid')
    request = next((request for request in _D2_REQUESTS.values() if request.get('observed_pid') == pid), None) if pid is not None else None
    if request is None:
        return
    ticket = request['ticket']
    if request.get('after_result_containment'):
        return
    handle = state.get('_native_handle')
    record = _D2_NATIVE_HANDLES.get(int(handle)) if handle is not None else None
    owned_original = handle == request.get('original_process_handle') and not state.get('_vendor_transferred')
    if record is None or record.get('ticket_id') != ticket['ticket_id'] or not (record.get('retained') or owned_original):
        return
    wait = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0)
    if wait not in {0, 258}:
        raise ValueError('D2_GUARD_NATIVE_WAIT')
    raw = int(_D2_WINAPI_ORIGINAL['GetExitCodeProcess'](handle)) if wait == 0 else None
    code = -15 if raw == 65536 else raw
    request['state'] = state
    request['os_observation'] = dict(worker_id=ticket['d2_contract']['worker_id'],
        ticket_id=ticket['ticket_id'], pid=pid, actual_exit=code, raw_windows_exit=raw,
        settled=wait == 0, alive=wait == 258, witness='GUARD_RETAINED_HANDLE')
    if wait == 0:
        record_child_exit(ticket, code)
        _close_child_job(ticket)


def d2_actual_pid_settlement():
    c = _CONTEXT
    if c.role != 'd2-controller':
        raise ValueError('D2_OS_ORACLE_SCOPE')
    found = []
    for ticket in c.children:
        request = _D2_REQUESTS.get(ticket['ticket_id'])
        if request is None or request.get('observed_pid') is None:
            continue
        state = request.get('state')
        if state is not None:
            d2_observe_native_state(state, None)
        observed = request.get('os_observation')
        if observed is None or observed['pid'] != request['observed_pid']:
            raise ValueError('D2_OS_ORACLE_MISSING')
        row = dict(observed)
        exit_path = c.report_dir / 'tickets' / (ticket['ticket_id'] + '.exit.json')
        if row['settled']:
            receipt = _read_json(exit_path)
            if (receipt.get('ticket_id') != ticket['ticket_id'] or receipt.get('run_id') != ticket['run_id']
                    or receipt.get('actual_exit') != row['actual_exit']):
                raise ValueError('D2_OS_ORACLE_EXIT')
            row['exit_receipt_verified'] = True
        else:
            if exit_path.exists():
                raise ValueError('D2_OS_ORACLE_LIVE_EXIT')
            row['exit_receipt_verified'] = False
        found.append(row)
    return found


def d2_slot_write_witnesses():
    """Read only settled children's actual guard finals, outside slot storage."""
    c = _CONTEXT
    if c.role != 'd2-controller':
        raise ValueError('D2_SLOT_WITNESS_SCOPE')
    workers = {}
    for observed in d2_actual_pid_settlement():
        if not observed['settled'] or observed['worker_id'] not in {'google', 'ftp'}:
            continue
        identifier = observed['ticket_id']
        path = c.report_dir / 'processes' / (identifier + '.final.json')
        if not path.is_file():
            continue
        final = _read_json(path)
        if (final.get('ticket_id') != identifier or final.get('run_id') != c.run_id
                or final.get('pid') != observed['pid'] or final.get('tested_sha') != c.tested_sha):
            raise ValueError('D2_SLOT_WITNESS_IDENTITY')
        effects = final.get('slot_effects')
        if effects is None:
            continue
        keys = {'own_metadata', 'own_opens', 'peer_slot_attempts', 'admitted_writes'}
        if set(effects) != keys or any(type(value) is not int or value < 0 for value in effects.values()):
            raise ValueError('D2_SLOT_WITNESS_FORMAT')
        phases = final.get('slot_phase_witnesses')
        if (not isinstance(phases, list) or len(phases) > 128 or any(
                not isinstance(row, dict) or set(row) != {'kind', 'phase', 'worker_id', 'go_released',
                    'reserved_peak_bytes', 'reserved_slot_bytes'} or row['kind'] != 'SLOT_WRITE'
                or row['phase'] != 'OWN_SLOT' or row['worker_id'] != observed['worker_id']
                or type(row['go_released']) is not bool
                or any(type(row[key]) is not int or row[key] <= 0 for key in ('reserved_peak_bytes', 'reserved_slot_bytes'))
                for row in phases)):
            raise ValueError('D2_SLOT_PHASE_WITNESS_FORMAT')
        workers[observed['worker_id']] = dict(effects, ticket_id=identifier, pid=observed['pid'],
            actual_exit=observed['actual_exit'], exit_receipt_verified=True,
            witness='GUARD_WORKER_FINAL', phase_witnesses=phases)
    return dict(workers=workers)


def d2_request_worker_cleanup(state, callback):
    """Observe the actual native termination request using retained OS APIs."""
    c = _CONTEXT
    caller = sys._getframe(1)
    pid = state.get('_native_pid')
    request = next((value for value in _D2_REQUESTS.values() if value.get('observed_pid') == pid), None)
    phase = state.get('_cleanup_phase')
    disabled = c.ticket['d2_contract']['case'] == 'cleanup_unsettled'
    if (c.role != 'd2-controller' or caller.f_code.co_name != 'terminate'
            or _absolute(caller.f_code.co_filename) != c.root / 'tests/d2/d2_worker.py'
            or callback is not c.d2_terminate or request is None or request.get('state') is not state
            or phase not in {'TERMINATE', 'KILL'}):
        raise ValueError('D2_CLEANUP_REQUEST_SCOPE')
    import _winapi
    original = _D2_WINAPI_ORIGINAL['TerminateProcess']
    if _winapi.TerminateProcess is not original:
        raise ValueError('D2_CLEANUP_REQUEST_BINDING')
    ticket = request['ticket']
    handle = state.get('_native_handle')
    proof = _D2_NATIVE_HANDLES.get(int(handle)) if handle is not None else None
    if proof is None or proof.get('ticket_id') != ticket['ticket_id'] or not proof.get('retained'):
        raise ValueError('D2_CLEANUP_REQUEST_HANDLE')
    before = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0)
    if before not in {0, 258}:
        raise ValueError('D2_CLEANUP_REQUEST_WAIT')
    accepted, calls = False, 0
    owner = threading.get_ident()
    def observe_native(actual_handle, exit_code):
        nonlocal accepted, calls
        if threading.get_ident() != owner:
            return original(actual_handle, exit_code)
        frame = sys._getframe(1)
        if (frame.f_code is not callback.__code__ or frame.f_locals.get('state') is not state
                or actual_handle != handle or exit_code != 65536 or calls):
            raise ValueError('D2_CLEANUP_REQUEST_ARGUMENTS')
        calls += 1
        result = original(actual_handle, exit_code)
        accepted = True
        return result
    _winapi.TerminateProcess = observe_native
    try:
        result = False if disabled else callback(state)
    finally:
        if _winapi.TerminateProcess is not observe_native:
            raise ValueError('D2_CLEANUP_REQUEST_RESTORE')
        _winapi.TerminateProcess = original
    after = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0)
    if after not in {0, 258}:
        raise ValueError('D2_CLEANUP_REQUEST_WAIT')
    raw = int(_D2_WINAPI_ORIGINAL['GetExitCodeProcess'](handle)) if after == 0 else None
    row = dict(version=1, ticket_id=ticket['ticket_id'], run_id=c.run_id, case=ticket['d2_contract']['case'],
        nodeid=ticket['nodeid'], worker_id=ticket['d2_contract']['worker_id'], pid=pid, phase=phase,
        alive=before == 258, accepted=accepted, native_calls=calls, requested_exit=65536,
        settled_after=after == 0, raw_windows_exit_after=raw, witness='GUARD_RETAINED_HANDLE')
    exclusive_json(c.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-cleanup-request-' + phase + '.json'), row)
    return result, row


def _d2_foreign_kernel32():
    c = _CONTEXT
    if c.role not in {'d2-controller', 'd2-worker'} or not _d2_fixture_profile_admitted('foreign_handle'):
        raise ValueError('D2_FOREIGN_NATIVE_SCOPE')
    if hasattr(c, 'd2_foreign_kernel'):
        return c.d2_foreign_kernel
    _THREAD_STATE.d2_binding = True
    try:
        kernel = _ORIGINAL_WINDLL('kernel32', use_last_error=True)
        create, signal, wait, close = kernel.CreateEventW, kernel.SetEvent, kernel.WaitForSingleObject, kernel.CloseHandle
        create.argtypes, create.restype = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_wchar_p], ctypes.c_void_p
        signal.argtypes, signal.restype = [ctypes.c_void_p], ctypes.c_int
        wait.argtypes, wait.restype = [ctypes.c_void_p, ctypes.c_uint32], ctypes.c_uint32
        close.argtypes, close.restype = [ctypes.c_void_p], ctypes.c_int
    finally:
        _THREAD_STATE.d2_binding = False
    def call(name, function, args):
        caller = sys._getframe(2).f_code
        own = getattr(c, 'd2_foreign_fixture', {})
        if name == 'create':
            valid = (c.role == 'd2-controller' and caller is d2_create_foreign_fixture.__code__
                and args == (None, True, False, None) and not own)
        elif name == 'signal':
            probe = getattr(c, 'd2_foreign_probe_payload', {})
            valid = (c.role == 'd2-worker' and caller is d2_foreign_inheritance_probe.__code__
                and args == (probe.get('event'),) and probe.get('fd') == 200)
        elif name == 'wait':
            valid = (c.role == 'd2-controller' and caller is d2_finish_foreign_fixture.__code__
                and args == (own.get('event'), 0))
        else:
            valid = (c.role == 'd2-controller' and caller in {
                d2_create_foreign_fixture.__code__, d2_finish_foreign_fixture.__code__}
                and args == (own.get('event'),))
        if not valid:
            reject('native')
        return function(*args)
    def owned_create(*args):
        return call('create', create, args)
    def owned_signal(*args):
        return call('signal', signal, args)
    def owned_wait(*args):
        return call('wait', wait, args)
    def owned_close(*args):
        return call('close', close, args)
    c.d2_foreign_kernel = (owned_create, owned_signal, owned_wait, owned_close)
    return c.d2_foreign_kernel


def d2_create_foreign_fixture(descriptor):
    c = _CONTEXT
    if c.role != 'd2-controller' or not _d2_fixture_profile_admitted('foreign_handle') or descriptor != 200:
        raise ValueError('D2_FOREIGN_FIXTURE_SCOPE')
    import _winapi
    import msvcrt
    _writable_descriptor(descriptor)
    handle = int(msvcrt.get_osfhandle(descriptor))
    event = _d2_foreign_kernel32()[0](None, True, False, None)
    if not event:
        raise OSError('D2_FOREIGN_EVENT_CREATE_FAILED')
    event = int(event)
    os.set_handle_inheritable(event, True)
    c.d2_foreign_fixture = dict(fd=descriptor, handle=handle, event=event,
        file_inheritable=os.get_handle_inheritable(handle), event_inheritable=os.get_handle_inheritable(event))
    if c.d2_foreign_fixture['file_inheritable'] is not True or c.d2_foreign_fixture['event_inheritable'] is not True:
        _d2_foreign_kernel32()[3](event)
        raise ValueError('D2_FOREIGN_FIXTURE_INHERITABILITY')
    return dict(c.d2_foreign_fixture)


def d2_foreign_inheritance_probe(payload):
    if (_CONTEXT.role != 'd2-worker' or not _d2_fixture_profile_admitted('foreign_handle')
            or _CONTEXT.ticket['d2_contract']['worker_id'] != 'google'
            or set(payload) != {'fd', 'handle', 'event', 'file_inheritable', 'event_inheritable'}
            or payload['fd'] != 200):
        raise ValueError('D2_FOREIGN_PROBE_SCOPE')
    import _winapi
    file_visible, file_written, event_set = False, False, False
    declared = _read_json(Path(_CONTEXT.ticket['d2_contract']['result_root']).parent / 'foreign.json')
    if declared != payload:
        raise ValueError('D2_FOREIGN_PAYLOAD_IDENTITY')
    _CONTEXT.d2_foreign_probe_payload = dict(payload)
    try:
        if _winapi.GetFileType(payload['handle']) == 1:
            data, error = _winapi.ReadFile(payload['handle'], len(b'D2_FOREIGN_UNCHANGED'))
            file_visible = error == 0 and data == b'D2_FOREIGN_UNCHANGED'
            if file_visible:
                _winapi.WriteFile(payload['handle'], b'D2_SYNTHETIC_CANARY_FOREIGN_EFFECT')
                file_written = True
    except OSError:
        pass
    try:
        event_set = bool(_d2_foreign_kernel32()[1](payload['event']))
    except OSError:
        pass
    return dict(file_handle_visible=file_visible, file_write_succeeded=file_written, event_set_succeeded=event_set)


def d2_finish_foreign_fixture(payload):
    import _winapi
    if getattr(_CONTEXT, 'd2_foreign_fixture', None) != payload:
        raise ValueError('D2_FOREIGN_FINISH_SCOPE')
    unchanged = _d2_foreign_kernel32()[2](payload['event'], 0) == 258
    unchanged = unchanged and os.get_handle_inheritable(payload['handle']) is True and os.get_handle_inheritable(payload['event']) is True
    if not _d2_foreign_kernel32()[3](payload['event']):
        raise OSError('D2_FOREIGN_EVENT_CLOSE_FAILED')
    return unchanged


def d2_declare_completed_case(bundle):
    c = _CONTEXT
    if c.role != 'd2-controller' or bundle.get('case') != c.ticket['d2_contract']['case']:
        raise ValueError('D2_DECLARATION_OWNER')
    for ticket in c.children:
        if ticket['role'] not in _D2_ROLES - {'d2-controller'}:
            continue
        request = _D2_REQUESTS.get(ticket['ticket_id'])
        created = request.get('observed_pid') if request else None
        exit_path = c.report_dir / 'tickets' / (ticket['ticket_id'] + '.exit.json')
        actual = _read_json(exit_path).get('actual_exit') if exit_path.exists() else None
        not_started = (request is not None and created is None
            and (not request['entered'] or request['rejected_before_create']))
        record = dict(version=1, ticket_id=ticket['ticket_id'], run_id=ticket['run_id'],
            nodeid=ticket['nodeid'], case=ticket['d2_contract']['case'],
            worker_id=ticket['d2_contract']['worker_id'], pid=created,
            actual_exit=actual, not_started=not_started,
            fault_witnesses=bundle.get('fault_witnesses', {}),
            worker_fence=bundle.get('worker_fences', {}).get(ticket['d2_contract']['worker_id']),
            cleanup_requests=bundle.get('cleanup_requests', []),
            deliberately_unsettled=bundle['case'] == 'cleanup_unsettled' and created is not None and actual is None)
        exclusive_json(c.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-observation.json'), record)


def d2_contain_after_result(case):
    """Negative oracle is immutable before the fixture Job kills its worker."""
    c = _CONTEXT
    if case != 'cleanup_unsettled' or c.role != 'd2-controller' or not _d2_fixture_profile_admitted(case):
        raise ValueError('D2_AFTER_RESULT_CONTAINMENT_SCOPE')
    result_path = Path(c.ticket['d2_contract']['result_root']) / 'controller-result.json'
    bundle = _read_json(result_path)
    if bundle.get('case') != case or bundle['result']['diagnostic_success_allowed'] is not False:
        raise ValueError('D2_AFTER_RESULT_NEGATIVE_ORACLE')
    import upload
    for ticket in c.children:
        request = _D2_REQUESTS.get(ticket['ticket_id'])
        if request is None or request.get('state') is None or ticket['ticket_id'] not in _CHILD_JOBS:
            continue
        state = request['state']
        handle = state.get('_native_handle')
        record = _D2_NATIVE_HANDLES.get(int(handle)) if handle is not None else None
        if (record is None or record.get('retained') is not True
                or record.get('ticket_id') != ticket['ticket_id'] or state.get('_native_pid') != request['observed_pid']):
            raise ValueError('D2_AFTER_RESULT_NATIVE_WITNESS')
        if _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0) == 0:
            continue
        request['after_result_containment'] = True
        _close_child_job(ticket, terminate=True)
        deadline = time.monotonic() + 10
        wait = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0)
        while wait == 258 and time.monotonic() < deadline:
            wait = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 100)
        if wait not in {0, 258}:
            raise ValueError('D2_AFTER_RESULT_NATIVE_WAIT')
        raw = int(_D2_WINAPI_ORIGINAL['GetExitCodeProcess'](handle)) if wait == 0 else None
        code = -15 if raw == 65536 else raw
        request['containment_os_observation'] = dict(pid=request['observed_pid'], actual_exit=code,
            raw_windows_exit=raw, settled=wait == 0, witness='GUARD_RETAINED_HANDLE')
        exclusive_json(c.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-containment.json'),
            dict(ticket_id=ticket['ticket_id'], run_id=c.run_id, pid=request['observed_pid'],
                actual_exit=code, raw_windows_exit=raw, settled=wait == 0,
                witness='GUARD_RETAINED_HANDLE', purpose='AFTER_RESULT_CONTAINMENT'))
        if wait != 0:
            raise RuntimeError('D2_AFTER_RESULT_CONTAINMENT_FAILED')
        record_child_exit(ticket, code)
        if not upload._d2_close_launch_state(state):
            raise RuntimeError('D2_AFTER_RESULT_VENDOR_CLOSE_FAILED')
        if state.get('_start_returned'):
            request['process'].join(0)


def d2_controller_error(error):
    c = _CONTEXT
    if c is None or c.role != 'd2-controller':
        return
    caller = sys._getframe(1)
    if _absolute(caller.f_code.co_filename) != c.root / 'tests/d2/d2_worker.py' or caller.f_code.co_name != '<module>':
        raise ValueError('D2_CONTROLLER_ERROR_CALLER')
    kind = ('RUNTIME' if type(error) is RuntimeError else 'VALUE' if type(error) is ValueError
        else 'OS' if isinstance(error, OSError) else 'IMPORT' if isinstance(error, ImportError) else 'OTHER')
    upload = sys.modules.get('upload')
    code = upload._d2_code(error, fallback='D2_CONTROLLER_FAILED') if upload is not None else 'D2_CONTROLLER_FAILED'
    if upload is None or code not in upload._D2_CODES:
        code = 'D2_CONTROLLER_FAILED'
    stages = {'FIXTURE_SEED', 'IMPORT_UPLOAD', 'BASELINE_EXECUTION', 'LOCK_EXECUTION',
        'LOGGER_SETUP', 'SOURCE_PLAN', 'PREWRAPPER_SETUP', 'NATIVE_SUPERVISION',
        'PARENT_FINALIZATION', 'RESULT_EXPORT', 'DECLARATION', 'AFTER_RESULT_CONTAINMENT', 'GUARD_FINALIZATION'}
    stage = getattr(c, 'd2_controller_stage', None)
    stage = stage if stage in stages else 'UNKNOWN'
    exclusive_json(Path(c.ticket['d2_contract']['result_root']) / 'controller-error.json',
        dict(version=1, case=c.ticket['d2_contract']['case'], controller_pid=os.getpid(),
            ticket_id=c.ticket['ticket_id'], run_id=c.run_id, stage=stage,
            exception_kind=kind, error_code=code, declared_status=1))


def d2_controller_exception_cleanup():
    """Bounded fixture containment on a failed controller, never a PASS oracle."""
    c = _CONTEXT
    if c is None or c.role != 'd2-controller':
        return
    caller = sys._getframe(1)
    if _absolute(caller.f_code.co_filename) != c.root / 'tests/d2/d2_worker.py' or caller.f_code.co_name != '<module>':
        raise ValueError('D2_CONTROLLER_CLEANUP_CALLER')
    import upload
    deadline = time.monotonic() + 10
    pending = []
    failed = False
    # Contain every owned sibling before one bad observation can abort cleanup.
    for ticket in c.children:
        request = _D2_REQUESTS.get(ticket['ticket_id'])
        if request is None or ticket['ticket_id'] not in _CHILD_JOBS:
            continue
        state = request.get('state') or {}
        handle = state.get('_native_handle')
        if handle is None:
            handle = next((value for value, proof in _D2_NATIVE_HANDLES.items()
                if proof.get('ticket_id') == ticket['ticket_id'] and proof.get('kind') == 'process'
                and proof.get('retained')), request.get('original_process_handle'))
        try:
            _close_child_job(ticket, terminate=True)
        except BaseException:
            failed = True
        pending.append((ticket, request, state, handle))
    for ticket, request, state, handle in pending:
        error_code, raw, settled = None, None, False
        try:
            proof = _D2_NATIVE_HANDLES.get(int(handle)) if handle is not None else None
            if proof is None or proof.get('ticket_id') != ticket['ticket_id'] or proof.get('kind') != 'process':
                error_code = 'D2_CONTROLLER_CLEANUP_WITNESS'
            else:
                wait = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 0)
                while wait == 258 and time.monotonic() < deadline:
                    wait = _D2_WINAPI_ORIGINAL['WaitForSingleObject'](handle, 100)
                settled = wait == 0
                if settled:
                    raw = int(_D2_WINAPI_ORIGINAL['GetExitCodeProcess'](handle))
                    record_child_exit(ticket, -15 if raw == 65536 else raw)
                    if state.get('_native_handle') is not None and not state.get('_closed'):
                        if not upload._d2_close_launch_state(state):
                            error_code = 'D2_CONTROLLER_VENDOR_CLOSE_FAILED'
                        elif state.get('_start_returned'):
                            request['process'].join(0)
                else:
                    error_code = 'D2_CONTROLLER_CLEANUP_UNSETTLED'
        except BaseException:
            error_code = 'D2_CONTROLLER_CLEANUP_FAILED'
        code = -15 if raw == 65536 else raw
        exclusive_json(c.report_dir / 'tickets' / (ticket['ticket_id'] + '.d2-controller-cleanup.json'),
            dict(ticket_id=ticket['ticket_id'], run_id=c.run_id, pid=request['observed_pid'],
                actual_exit=code, raw_windows_exit=raw, settled=settled, error_code=error_code,
                witness='GUARD_RETAINED_HANDLE', purpose='CONTROLLER_EXCEPTION_CONTAINMENT'))
        failed = failed or error_code is not None
    if failed:
        raise RuntimeError('D2_CONTROLLER_CLEANUP_FAILED')


def _expected_d2_control(ticket, errors, final):
    if ticket['role'] == 'd2-controller':
        return False
    try:
        record = _d2_read_declaration(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.d2-observation.json'))
        contract = ticket['d2_contract']
        if (record.get('version') != 1 or record.get('ticket_id') != ticket['ticket_id']
                or record.get('run_id') != ticket['run_id'] or record.get('nodeid') != _d2_node(contract['case'])
                or record.get('case') != contract['case'] or record.get('worker_id') != contract['worker_id']):
            return False
        counts = {key: value for key, value in (final or {}).get('counters', {}).items() if value}
        if counts:
            return False
        phase_spec = _D2_FAULT_PHASES.get(contract['case'])
        witnesses = record.get('fault_witnesses', {})
        phase = next((row for row in witnesses.get(phase_spec[0], [])
            if row.get('phase') == phase_spec[1]), None) if phase_spec else None
        if (phase is None or phase.get('count') != 1 or phase.get('case') != contract['case']
                or phase.get('run_id') != ticket['run_id'] or phase.get('nodeid') != ticket['nodeid']):
            return False
        fault_ticket = _read_json(Path(ticket['report_dir']) / 'tickets' / (phase['ticket_id'] + '.json'))
        fault_path = Path(ticket['report_dir']) / 'tickets' / (phase['ticket_id'] + '.d2-fault-'
            + phase['worker_id'] + '-' + phase['phase'] + '.json')
        if (_read_json(fault_path) != phase or fault_ticket['run_id'] != ticket['run_id']
                or fault_ticket['d2_contract']['case'] != contract['case']):
            return False
        if phase['source'] == 'worker':
            fault_start = _read_json(Path(ticket['report_dir']) / 'processes' / (phase['ticket_id'] + '.start.json'))
            if (fault_start.get('pid') != phase['pid']
                    or fault_ticket['d2_contract']['worker_id'] != phase['worker_id']):
                return False
        elif phase['source'] in {'native', 'controller'}:
            parent_id = fault_ticket['parent_id'] if phase['source'] == 'native' else fault_ticket['ticket_id']
            if phase['pid'] != _read_json(Path(ticket['report_dir']) / 'processes' / (parent_id + '.start.json')).get('pid'):
                return False
        else:
            return False
        if record['not_started']:
            return (record['pid'] is None and record['actual_exit'] is None
                and contract['worker_id'] == 'google' and phase['source'] == 'native'
                and phase['ticket_id'] == ticket['ticket_id'] and phase['phase'] == contract['case']
                and contract['case'] in {'native_nul_read_fault', 'native_nul_write_fault',
                    'native_dup_read_fault', 'native_dup_write_fault', 'native_create_fault'}
                and set(errors) == {'MISSING_START_REPORT', 'MISSING_FINAL_REPORT', 'MISSING_CHILD_EXIT'})
        if record['deliberately_unsettled']:
            if (contract['case'] != 'cleanup_unsettled' or contract['worker_id'] != 'google'
                    or type(record['pid']) is not int or record['actual_exit'] is not None):
                return False
            if set(errors) == {'MISSING_FINAL_REPORT', 'MISSING_CHILD_EXIT'}:
                return True
            containment = _read_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.d2-containment.json'))
            actual = _read_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.exit.json'))
            return (set(errors) == {'MISSING_FINAL_REPORT'} and containment.get('purpose') == 'AFTER_RESULT_CONTAINMENT'
                and containment.get('settled') is True and containment.get('pid') == record['pid']
                and containment.get('run_id') == ticket['run_id'] and containment.get('ticket_id') == ticket['ticket_id']
                and containment.get('actual_exit') == actual.get('actual_exit') == 124)
        expected = {
            ('baseline_exit23', 'google'): 23, ('baseline_hang', 'google'): 124,
            ('exit23', 'google'): 23, ('abnormal_before_ready', 'google'): 23,
            ('abnormal_after_final', 'google'): 23, ('final_then_exit23', 'google'): 23,
            ('hang_google', 'google'): -15, ('hang_ftp', 'ftp'): -15,
            ('start_google_after_native', 'google'): 1, ('start_ftp_after_native', 'ftp'): 1,
            ('start_ftp_before', 'google'): -15, ('start_ftp_after_native', 'google'): -15,
            ('d1_killed_holder', 'holder'): -15,
        }
        # Exact sibling cancellation: it can be absent only when the primary
        # fixed fault was actually witnessed and this owned sibling was killed.
        for case in ('google_error', 'exit23', 'system_exit0', 'system_exit_string',
                'abnormal_before_ready', 'abnormal_after_final', 'final_then_exit23',
                'normal_without_final', 'partial_google', 'ack_failure',
                'bootstrap_replay_failure', 'bootstrap_import_failure', 'bootstrap_unpickle_failure'):
            expected[(case, 'ftp')] = -15
        for case in ('ftp_error', 'partial_ftp'):
            expected[(case, 'google')] = -15
        for case in D2_CASES:
            if case.startswith('checkpoint_'):
                expected[(case, 'google')] = -15
                expected[(case, 'ftp')] = -15
        wanted = expected.get((contract['case'], contract['worker_id']))
        primary_worker = {'system_exit0': 'google', 'system_exit_string': 'google',
            'google_error': 'google', 'ftp_error': 'ftp', 'normal_without_final': 'google'}
        primary_cancellation = (contract['worker_id'] == primary_worker.get(contract['case'])
            and record['actual_exit'] == -15)
        if primary_cancellation:
            fence = record.get('worker_fence') or {}
            terminal = fence.get('flog_final') or {}
            cleanup = next((row for row in record.get('cleanup_requests', [])
                if row.get('ticket_id') == ticket['ticket_id'] and row.get('pid') == record['pid']
                and row.get('alive') is True and row.get('accepted') is True
                and row.get('native_calls') == 1 and row.get('requested_exit') == 65536
                and row.get('phase') in {'TERMINATE', 'KILL'} and row.get('witness') == 'GUARD_RETAINED_HANDLE'), None)
            if contract['case'] in {'system_exit0', 'system_exit_string'}:
                final_fence = terminal == {'execution_outcome': 'INTERRUPTED', 'exit_status': 1}
            elif contract['case'] == 'normal_without_final':
                final_fence = (fence.get('flog_final') is None and fence.get('checkpoint_execution') == 'RETURNED'
                    and fence.get('normal_return') is True and fence.get('checkpoint_diagnostics') == 'INCOMPLETE'
                    and fence.get('checkpoint_error_code') == 'D2_DIAGNOSTICS_INCOMPLETE'
                    and fence.get('checkpoint_output_states') == ['ACKNOWLEDGED'])
            else:
                final_fence = terminal == {'execution_outcome': 'FAILED', 'exit_status': 1}
            if cleanup is None or not final_fence:
                return False
            cleanup_path = Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.d2-cleanup-request-' + cleanup['phase'] + '.json')
            actual = _read_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.exit.json'))
            if _d2_read_declaration(cleanup_path) != cleanup or actual.get('actual_exit') != -15:
                return False
            wanted = -15
        permitted_errors = {'MISSING_FINAL_REPORT'}
        partial_start = (contract['case'] == 'start_' + contract['worker_id'] + '_after_native'
            and phase['source'] == 'native' and phase['phase'] == 'start_serialization_failure'
            and phase['ticket_id'] == ticket['ticket_id'] and record['actual_exit'] in {1, -15})
        if partial_start:
            actual = _read_json(Path(ticket['report_dir']) / 'tickets' / (ticket['ticket_id'] + '.exit.json'))
            if actual.get('actual_exit') != record['actual_exit']:
                return False
            if record['actual_exit'] == -15:
                cleanup = next((row for row in record.get('cleanup_requests', [])
                    if row.get('ticket_id') == ticket['ticket_id'] and row.get('pid') == record['pid']
                    and row.get('alive') is True and row.get('accepted') is True
                    and row.get('native_calls') == 1 and row.get('requested_exit') == 65536), None)
                if cleanup is None:
                    return False
            start_path = Path(ticket['report_dir']) / 'processes' / (ticket['ticket_id'] + '.start.json')
            if not start_path.is_file():
                if not _d2_before_bootstrap_pid(ticket, record['pid']):
                    return False
                permitted_errors.add('MISSING_START_REPORT')
            wanted = record['actual_exit']
        if (wanted is None or type(record['pid']) is not int or record['actual_exit'] != wanted
                or set(errors) != permitted_errors):
            return False
        if phase['source'] == 'worker' and phase['worker_id'] == contract['worker_id']:
            if phase['pid'] != record['pid'] or phase['ticket_id'] != ticket['ticket_id']:
                return False
        fence = record.get('worker_fence') or {}
        terminal = fence.get('flog_final') or {}
        if contract['case'] == 'abnormal_before_ready' and contract['worker_id'] == 'google':
            return fence.get('checkpoint_present') is False and fence.get('flog_final') is None
        if contract['case'] == 'abnormal_after_final' and contract['worker_id'] == 'google':
            return (fence.get('checkpoint_execution') == 'RUNNING'
                and terminal.get('execution_outcome') == 'COMPLETED' and terminal.get('exit_status') == 0)
        if contract['case'] == 'final_then_exit23' and contract['worker_id'] == 'google':
            return (fence.get('checkpoint_execution') == 'RETURNED' and fence.get('normal_return') is True
                and terminal.get('execution_outcome') == 'COMPLETED' and terminal.get('exit_status') == 0)
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _d2_windll(name, *args, **kwargs):
    c = _CONTEXT
    dependency = _d2_dateutil_resource_loader(name, args, kwargs)
    if dependency is not None:
        return dependency
    pending = getattr(_THREAD_STATE, 'pending', [])
    if (c is None or c.lane != 'd2' or str(name).lower() not in {'kernel32', 'kernel32.dll'}
            or args or kwargs != {'use_last_error': True}
            or not (pending or c.role == 'd2-lock-worker' or getattr(_THREAD_STATE, 'd1_closing_job', None))):
        reject('native')
    _THREAD_STATE.d2_binding = True
    try:
        actual = _ORIGINAL_WINDLL(name, **kwargs)
    finally:
        _THREAD_STATE.d2_binding = False
    functions = {}
    class Bindings:
        def __getattr__(self, symbol):
            permitted = _D1_NATIVE_FUNCTIONS if c.role == 'd2-lock-worker' else _D1_JOB_FUNCTIONS | {'CloseHandle'}
            if symbol not in permitted:
                reject('native')
            if symbol not in functions:
                _THREAD_STATE.d2_binding = True
                try:
                    leaf = getattr(actual, symbol)
                finally:
                    _THREAD_STATE.d2_binding = False
                def bound(*values):
                    if hasattr(bound, 'argtypes'):
                        leaf.argtypes = bound.argtypes
                    if hasattr(bound, 'restype'):
                        leaf.restype = bound.restype
                    return _d2_native_call(symbol, leaf, values)
                functions[symbol] = bound
            return functions[symbol]
    return Bindings()


def _d2_dateutil_resource_loader(name, args, kwargs):
    """Pinned pandas import reads only Windows timezone string resources."""
    c = _CONTEXT
    unit_nodes = {'tests/d2/test_targets.py::test_google_projection_and_write_arguments_are_literal',
        'tests/d2/test_targets.py::test_google_partial_mutation_stays_unknown[clear]',
        'tests/d2/test_targets.py::test_google_partial_mutation_stays_unknown[write]'}
    eligible = (c is not None and c.lane == 'd2' and (c.role == 'lane-worker' and getattr(c, 'nodeid', '') in unit_nodes
        or c.role == 'd2-worker' and c.ticket['d2_contract']['case'] == 'success'
        and c.ticket['d2_contract']['worker_id'] == 'google'))
    if not eligible or type(name) is not str or name not in {'user32', 'tzres.dll'} or args or kwargs:
        return None
    module = sys.modules.get('dateutil.tz.win')
    path = c.root / '.f1/venv-dev/Lib/site-packages/dateutil/tz/win.py'
    if module is None or _absolute(getattr(module, '__file__', '')) != path:
        return None
    with _ORIGINAL_OPEN(checked_path(path), 'rb') as stream:
        source = stream.read()
    if hashlib.sha256(source).hexdigest() != 'c49b335a04b0135c4fc7f1c98f86647a9cae902fe1372d35e9631c5e16d1681f':
        raise ValueError('D2_DEPENDENCY_SOURCE_CHANGED')
    def code_at(parent, selected):
        return next(value for value in parent.co_consts if isinstance(value, CodeType) and value.co_name == selected)
    compiled = compile(source, str(path), 'exec', dont_inherit=True)
    resource_code = code_at(compiled, 'tzres')
    expected = {name: code_at(resource_code, name) for name in ('__init__', 'load_name', 'name_from_string')}
    dictionary_code = code_at(compiled, 'valuestodict')
    constructors = [code_at(code_at(compiled, selected), '__init__') for selected in ('tzwin', 'tzwinlocal')]
    resource_type = module.tzres
    if (any(getattr(resource_type, selected).__code__ != value for selected, value in expected.items())
            or module.valuestodict.__code__ != dictionary_code
            or any(getattr(module, selected).__init__.__code__ != value
                for selected, value in zip(('tzwin', 'tzwinlocal'), constructors))):
        raise ValueError('D2_DEPENDENCY_CODE_CHANGED')
    frame = sys._getframe(2)
    dictionary = frame.f_back
    constructor = dictionary.f_back if dictionary is not None else None
    instance = frame.f_locals.get('self')
    if (frame.f_code is not resource_type.__init__.__code__ or type(instance) is not resource_type
            or frame.f_locals.get('tzres_loc') != 'tzres.dll' or dictionary is None
            or dictionary.f_code is not module.valuestodict.__code__ or constructor is None
            or constructor.f_code not in {module.tzwin.__init__.__code__, module.tzwinlocal.__init__.__code__}
            or dictionary.f_locals.get('dtype') != module.winreg.REG_SZ
            or type(dictionary.f_locals.get('value')) is not str
            or re.fullmatch(r'@tzres\.dll,-[0-9]+', dictionary.f_locals['value']) is None):
        return None
    import_frame = constructor.f_back
    while import_frame is not None:
        filename = _absolute(import_frame.f_code.co_filename)
        if (import_frame.f_code.co_name == '<module>'
                and filename.is_relative_to(c.root / '.f1/venv-dev/Lib/site-packages/pandas')):
            break
        import_frame = import_frame.f_back
    if import_frame is None:
        return None
    records = getattr(c, 'd2_dateutil_resources', None)
    if records is None:
        records = c.d2_dateutil_resources = {}
    record = records.setdefault(id(instance), dict(instance=weakref.ref(instance)))
    if record['instance']() is not instance or name in record:
        raise ValueError('D2_DEPENDENCY_RESOURCE_IDENTITY')
    # Fixed system search prevents current-directory DLL selection.
    _THREAD_STATE.d2_binding = True
    try:
        actual = _ORIGINAL_WINDLL(name, winmode=0x800)
        leaf = actual.LoadStringW if name == 'user32' else None
    finally:
        _THREAD_STATE.d2_binding = False
    record[name] = actual
    if name == 'tzres.dll':
        class Resource:
            def __getattr__(self, symbol):
                caller = sys._getframe(1)
                if (symbol != '_handle' or caller.f_code is not resource_type.load_name.__code__
                        or caller.f_locals.get('self') is not record['instance']()):
                    reject('native')
                return actual._handle
        return Resource()
    def load_string(handle, offset, buffer, size):
        caller = sys._getframe(1)
        parser = caller.f_back
        dictionary = parser.f_back if parser is not None else None
        resource = caller.f_locals.get('resource')
        value = parser.f_locals.get('tzname_str') if parser is not None else None
        if (caller.f_code is not resource_type.load_name.__code__ or caller.f_locals.get('self') is not instance
                or parser is None or parser.f_code is not resource_type.name_from_string.__code__
                or parser.f_locals.get('self') is not instance or dictionary is None
                or dictionary.f_code is not module.valuestodict.__code__ or dictionary.f_locals.get('tz_res') is not instance
                or type(value) is not str or re.fullmatch(r'@tzres\.dll,-[0-9]+', value) is None
                or dictionary.f_locals.get('value') != value or type(offset) is not int
                or not 1 <= offset <= 65535 or offset != int(value.split(',-')[1])
                or handle != record.get('tzres.dll')._handle or size != 0
                or type(resource) is not resource_type.p_wchar
                or ctypes.cast(buffer, ctypes.c_void_p).value != ctypes.addressof(resource)
                or getattr(load_string, 'argtypes', None) != (module.wintypes.HINSTANCE,
                    module.wintypes.UINT, module.wintypes.LPWSTR, ctypes.c_int)):
            reject('native')
        leaf.argtypes = load_string.argtypes
        return leaf(handle, offset, buffer, size)
    class UserStrings:
        def __getattr__(self, symbol):
            if symbol != 'LoadStringW':
                reject('native')
            return load_string
    return UserStrings()


def _d2_safe_native_origin():
    """Closed rejection location only; never exception text or caller args."""
    owner, effect, library, symbol = 'OTHER', 'OTHER', 'OTHER', 'OTHER'
    frame = sys._getframe(1)
    known = {'numpy': 'NUMPY', 'pandas': 'PANDAS', 'dateutil': 'DATEUTIL',
        'google': 'GOOGLE', 'pygsheets': 'PYGSHEETS', 'cffi': 'CFFI', 'colorama': 'COLORAMA'}
    for _ in range(12):
        if frame is None:
            break
        name = frame.f_code.co_name
        if name == '_audit':
            event = frame.f_locals.get('event')
            effect = {'ctypes.dlopen': 'DLOPEN', 'ctypes.dlsym': 'DLSYM'}.get(event, 'OTHER')
            values = frame.f_locals.get('args', ())
            if event == 'ctypes.dlsym' and len(values) >= 2 and type(values[1]) is str:
                symbol = values[1] if values[1] in {'PyCapsule_New', 'PyCapsule_GetPointer', 'PyCapsule_IsValid',
                    'Py_IncRef', 'Py_DecRef', 'GetCurrentProcess', 'GetModuleFileNameW'} else 'OTHER'
            candidate = values[0] if event == 'ctypes.dlopen' and values else None
            if type(candidate) is str:
                library = {'kernel32': 'KERNEL32', 'kernel32.dll': 'KERNEL32', 'user32': 'USER32',
                    'tzres.dll': 'TZRES', 'ntdll': 'NTDLL', 'psapi': 'PSAPI', 'msvcrt': 'MSVCRT'}.get(candidate.lower(), 'OTHER')
        elif name == '_d2_windll':
            effect = 'WINDLL'
            candidate = frame.f_locals.get('name')
            if type(candidate) is str:
                library = {'kernel32': 'KERNEL32', 'kernel32.dll': 'KERNEL32', 'user32': 'USER32',
                    'tzres.dll': 'TZRES', 'ntdll': 'NTDLL', 'psapi': 'PSAPI'}.get(candidate.lower(), 'OTHER')
        elif name == '_d1_reject_loader':
            effect = 'CACHED_LOADER'
        filename = _absolute(frame.f_code.co_filename)
        site = _CONTEXT.root / '.f1/venv-dev/Lib/site-packages'
        if filename.is_relative_to(site):
            owner = known.get(filename.relative_to(site).parts[0].lower(), 'OTHER')
            break
        if filename == _CONTEXT.root / '.f1/python/Lib/ctypes/util.py':
            owner = 'CTYPES_UTIL'
            break
        frame = frame.f_back
    return dict(owner=owner, effect=effect, library=library, symbol=symbol)


def _d2_native_call(name, original, args):
    c = _CONTEXT
    pending = getattr(_THREAD_STATE, 'pending', [])
    record = _D2_NATIVE_HANDLES.get(_d1_handle(args[0])) if args and name not in {
        'CreateFileW', 'CreateMutexW', 'OpenMutexW', 'GetDriveTypeW', 'CreateJobObjectW'} else None
    if name == 'CreateJobObjectW':
        if not pending or tuple(args) != (None, None):
            reject('native')
    elif name in {'SetInformationJobObject', 'AssignProcessToJobObject', 'TerminateJobObject'}:
        if record is None or record['kind'] != 'job':
            reject('native')
        if name == 'SetInformationJobObject':
            limits = getattr(_THREAD_STATE, 'd1_job_limits', None)
            if (not pending or args[1] != 9 or limits is None or args[3] != len(limits)
                    or ctypes.string_at(args[2], args[3]) != limits):
                reject('native')
        if name == 'AssignProcessToJobObject' and _D2_NATIVE_HANDLES.get(_d1_handle(args[1]), {}).get('ticket_id') != record['ticket_id']:
            reject('native')
        if name == 'TerminateJobObject':
            closing = getattr(_THREAD_STATE, 'd1_closing_job', None) == _d1_handle(args[0])
            caller = sys._getframe(2)
            current_job = caller.f_locals.get('job')
            issued = caller.f_locals.get('ticket')
            timeout_branch = (caller.f_code is run_child.__code__ and c.lane == 'd2'
                and c.role in {'supervisor', 'lane-worker'} and isinstance(issued, dict)
                and issued.get('role') in {'lane-worker', 'd2-controller'}
                and issued in c.children and record.get('ticket_id') == issued['ticket_id']
                and current_job is not None and _d1_handle(current_job[1]) == _d1_handle(args[0])
                and caller.f_locals.get('process') is not None and getattr(_THREAD_STATE, 'controller', 0))
            if args[1] != 124 or not (closing or timeout_branch):
                reject('native')
    elif name in {'ResumeThread', 'TerminateProcess'}:
        if (record is None or not pending or record.get('ticket_id') != pending[-1]['ticket_id']
                or record['kind'] != ('thread' if name == 'ResumeThread' else 'process')
                or name == 'TerminateProcess' and args[1] != 124):
            reject('native')
    elif name == 'CloseHandle':
        if record is None:
            reject('native')
    elif c.role != 'd2-lock-worker':
        reject('native')
    elif name == 'CreateFileW':
        path = checked_path(args[0])
        if not _owned(path) or args[1:] != (0x80, 7, None, 3, 0x02200000, None):
            reject('native')
    elif name == 'GetDriveTypeW':
        if str(_absolute(args[0])) != _absolute(args[0]).anchor:
            reject('native')
    elif name in {'GetFileInformationByHandleEx', 'GetFileInformationByHandle'}:
        if record is None or record['kind'] != 'directory' or name == 'GetFileInformationByHandleEx' and (args[1] != 18 or args[3] != 24):
            reject('native')
    elif name in {'CreateMutexW', 'OpenMutexW'}:
        if args[2] not in _D1_MUTEX_NAMES or args[1] not in {False, 0} or name == 'CreateMutexW' and args[0] is not None:
            reject('native')
    elif name == 'WaitForSingleObject':
        if record is None or record['kind'] != 'mutex' or type(args[1]) is not int or not 0 <= args[1] <= 100:
            reject('native')
    elif name == 'ReleaseMutex':
        if record is None or record['kind'] != 'mutex' or not record.get('owned'):
            reject('native')
    result = original(*args)
    handle = _d1_handle(result)
    if name == 'CreateJobObjectW' and handle:
        _D2_NATIVE_HANDLES[handle] = dict(kind='job', ticket_id=pending[-1]['ticket_id'])
    elif name == 'CreateFileW' and handle not in {0, ctypes.c_void_p(-1).value}:
        _D2_NATIVE_HANDLES[handle] = dict(kind='directory', path=path)
    elif name in {'CreateMutexW', 'OpenMutexW'} and handle:
        _D2_NATIVE_HANDLES[handle] = dict(kind='mutex', owned=False)
    elif name == 'GetFileInformationByHandleEx' and result:
        _D1_MUTEX_NAMES.add('Global\\MDS-D1-v1-' + hashlib.sha256(b'MDS-D1-v1\0' + ctypes.string_at(args[2], 24)).hexdigest())
    elif name == 'WaitForSingleObject' and result in {0, 0x80}:
        record['owned'] = True
    elif name == 'ReleaseMutex' and result:
        record['owned'] = False
    elif name == 'CloseHandle' and result:
        _D2_NATIVE_HANDLES.pop(_d1_handle(args[0]), None)
    return result


def _d2_canary_detected(data):
    markers = (b'D2_SYNTHETIC_CANARY', b'D2_SYNTHETIC_CANARY_URL', b'D2_SYNTHETIC_CANARY_PATH',
        b'D2_FAKE_PASSWORD', b'D2_FAKE_PRIVATE_KEY', b'D2_FAKE_USERNAME')
    return any(marker in data for marker in markers)


def _d2_physical_graph_quiescence(controller_ticket):
    """No slot metadata is inspected before native birth/exit reconciliation."""
    c = _CONTEXT
    rows = []
    for path in (c.report_dir / 'tickets').glob('*.json'):
        if re.fullmatch(r'[0-9a-f]{32}\.json', path.name) is None:
            continue
        ticket = _read_json(path)
        if ticket.get('parent_id') != controller_ticket['ticket_id']:
            continue
        if (ticket.get('role') not in _D2_ROLES - {'d2-controller'}
                or ticket.get('run_id') != c.run_id or ticket.get('tested_sha') != c.tested_sha
                or ticket.get('source_digest') != c.source_digest
                or ticket.get('digest') != _digest({key: value for key, value in ticket.items() if key != 'digest'})):
            raise ValueError('D2_PHYSICAL_GRAPH_IDENTITY')
        identifier = ticket['ticket_id']
        birth_path = c.report_dir / 'tickets' / (identifier + '.d2-native-created.json')
        declaration_path = c.report_dir / 'tickets' / (identifier + '.d2-observation.json')
        if not birth_path.is_file():
            declared = _d2_read_declaration(declaration_path)
            if not declared['not_started'] or declared['pid'] is not None:
                return False, rows
            rows.append(dict(ticket_id=identifier, worker_id=ticket['d2_contract']['worker_id'],
                pid=None, settled=True, witness='GUARD_NOT_STARTED_DECLARATION'))
            continue
        birth = _d2_read_declaration(birth_path)
        exit_path = c.report_dir / 'tickets' / (identifier + '.exit.json')
        if not exit_path.is_file():
            return False, rows
        exited = _read_json(exit_path)
        if (exited.get('ticket_id') != identifier or exited.get('run_id') != c.run_id
                or type(exited.get('actual_exit')) is not int or exited.get('timed_out') is not False):
            return False, rows
        declared = _d2_read_declaration(declaration_path)
        if declared['deliberately_unsettled']:
            containment = _d2_read_declaration(c.report_dir / 'tickets' / (identifier + '.d2-containment.json'))
            if not containment['settled'] or containment['pid'] != birth['pid'] or containment['actual_exit'] != exited['actual_exit']:
                return False, rows
        elif declared['actual_exit'] != exited['actual_exit'] or declared['pid'] != birth['pid']:
            return False, rows
        rows.append(dict(ticket_id=identifier, worker_id=ticket['d2_contract']['worker_id'],
            pid=birth['pid'], actual_exit=exited['actual_exit'], settled=True,
            witness='GUARD_NATIVE_BIRTH_AND_EXIT'))
    return True, rows


def _d2_scan_diagnostics(case_root, log_root, report_root):
    counts = dict(results=0, flog=0, guard=0)
    detected = False
    paths = [('results', case_root / name) for name in ('controller-result.json', 'controller-error.json')]
    result_root = case_root / 'results'
    for name in ('google.json', 'google.pending', 'ftp.json', 'ftp.pending', 'result.json', 'result.pending'):
        paths.append(('results', result_root / name))
    for path in log_root.rglob('*'):
        if path.is_file():
            paths.append(('flog', path))
    for path in report_root.rglob('*'):
        if path.is_file() and path.suffix in {'.json', '.jsonl'}:
            paths.append(('guard', path))
    for kind, path in paths:
        if path.is_file():
            data = path.read_bytes()
            detected = detected or _d2_canary_detected(data)
            counts[kind] += 1
    # This literal exists only in RAM, and exercises exactly the same detector
    # used on real result/checkpoint/F-LOG/guard bytes above.
    positive = _d2_canary_detected(_D2_CANARY_TEXT.encode('ascii'))
    return not detected, positive, counts


def run_d2_case(case):
    """One immutable registry case; raw child streams stay in controller RAM."""
    c = _CONTEXT
    if (c is None or c.lane != 'd2' or c.role != 'lane-worker' or case not in D2_CASES
            or getattr(c, 'nodeid', '') != _d2_node(case) or sys.platform != 'win32'):
        raise ValueError('D2_ADMISSION_CASE')
    case_root = checked_path(c.root / '.f3/d2/tmp' / ('case-' + uuid.uuid4().hex))
    case_root.mkdir(parents=True)
    log_root = checked_path(c.root / '.f-log/d2' / c.run_id / case_root.name)
    log_root.mkdir(parents=True)
    command = [sys.executable, '-I', '-B', '-X', 'utf8', str(c.root / 'tests/d2/d2_worker.py'), '--case', case]
    contract = dict(case=case, worker_id='controller', result_root=str(case_root), log_root=str(log_root))
    ticket = issue_ticket('d2-controller', command, roots=[case_root, log_root, c.report_dir],
        expected_exit=None, d2_contract=contract, gate=True)
    result = run_child(command, role='d2-controller', ticket=ticket, timeout=75,
        env={'PYTHONDONTWRITEBYTECODE': '1', **dict(os.environ)})
    raw = result.stdout + result.stderr
    console_absent = not _d2_canary_detected(raw)
    path = case_root / 'controller-result.json'
    bundle = _read_json(path) if path.is_file() else {}
    legacy_completed = 'Скрипт загрузки данных закончил работу.'.encode('utf-8') in result.stdout
    if case.startswith('baseline_'):
        bundle['legacy_false_completion_observed'] = legacy_completed
    containment = []
    for observed_path in c.report_dir.glob('tickets/*.d2-containment.json'):
        observed = _read_json(observed_path)
        issued = _read_json(c.report_dir / 'tickets' / (observed['ticket_id'] + '.json'))
        if issued.get('parent_id') == ticket['ticket_id']:
            containment.append(observed)
    if containment:
        bundle['job_used_for_settlement'] = all(row['settled'] for row in containment)
        bundle['after_result_containment'] = containment
    try:
        physically_quiescent, physical_rows = _d2_physical_graph_quiescence(ticket)
    except (OSError, ValueError, KeyError, TypeError):
        physically_quiescent, physical_rows = False, []
    if physically_quiescent:
        evidence_absent, evidence_positive, scan_counts = _d2_scan_diagnostics(case_root, log_root, c.report_dir)
    else:
        evidence_absent, evidence_positive, scan_counts = False, _d2_canary_detected(_D2_CANARY_TEXT.encode('ascii')), {}
        result.report_errors.append('D2_DIAGNOSTIC_SCAN_UNSETTLED')
    bundle['diagnostic_scan_quiescent'] = physically_quiescent
    bundle['diagnostic_scan_physical_graph'] = physical_rows
    if (bundle.get('case') != case or bundle.get('version') != 1 or result.report is None
            or bundle.get('controller_pid') != result.report.get('pid')):
        result.report_errors.append('D2_CONTROLLER_RESULT_IDENTITY')
    return dict(case=case, bundle=bundle, baseline_red=bundle.get('baseline_red', False),
        actual_exit=result.actual_exit, actual_pid_settlement=bundle.get('actual_pid_settlement', []),
        watchdog_timed_out=result.timed_out, job_used_for_settlement=bundle.get('job_used_for_settlement', False),
        baseline_false_completion_observed=legacy_completed,
        console_canary_absent=console_absent, evidence_canary_absent=evidence_absent,
        evidence_capture_positive_control=evidence_positive, diagnostic_files_scanned=scan_counts,
        canary_absent=console_absent and evidence_absent,
        capture_positive_control=not console_absent if case == 'canary_positive_control' else False,
        report_errors=list(result.report_errors))


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
