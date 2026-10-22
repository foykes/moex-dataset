"""Closed LNX-03 fixtures and D2 test admission, separate from publication.

Popen is the sole reaper. Multiprocessing contributes only its attested spawn
serialization of the real F-LOG Queue/Pipe/RawValue objects. No Process._popen
is fabricated, and neither multiprocessing nor waitid consumes worker exits.
"""

import gc
import hashlib
import io
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import struct
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager


NATIVE_CASES = (
    'success', 'child_error', 'exit23', 'hang',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'bootstrap_pipe_failure', 'foreign_fd',
    'listener_failure', 'ack_failure', 'auxiliary_settlement',
    'canary_positive_control',
)
DEADLINE_SECONDS = 15
CANARY = b'LNX03_NATIVE_PRIVATE_CANARY_7fe94d31'
# Normalized LF bytes from the verified official CPython 3.14.8 full archive.
# A distribution patch is unsupported, even when the public version matches.
SPAWN_SOURCES = {
    'context.py': '6d9058b6770152b5d26f2f3ece22de423decf1c751038f6bcd81995096aa9ab6',
    'connection.py': 'c8123946008ec2bd57266e61a3a388d3914b75856cb2c8af8685978658fef879',
    'reduction.py': '7cd1652f08e4685f380f2484ad3441d363fe58fe5b328f50050fb0f281a535f9',
    'queues.py': '2749328cec74ac7abfb2f1b06df3eb70955d3951ea9c90aea21f25a729369cc7',
    'synchronize.py': 'd77f983907f050516dceae8e25af70628c644b720d51ae13d10f8b46442645c2',
    'sharedctypes.py': '77ef522912474652490b7df523112858e51721e63dcf109b8567a35ce9b31b0d',
    'heap.py': 'f6bb79bb99b9ae484935f0d68822e9603a1622dd0b6c4966c79db232a93ba614',
    'resource_tracker.py': 'a11ea85a6c41f9d5482cc3efae5d7687834f970e00881af9807481d14aea1b59',
    'spawn.py': 'e38307ad8d7a6107ca90a943e2d7a01c9a16fbe4cbac6d0f5087a8bfef4ebf2a',
    'popen_spawn_posix.py': '3738d2da3f5f57c83149232120a389a3b03cb4ed6f275957b884b18eb227628a',
    'util.py': 'a03138826235c4806bd0baea705b33a42edd9727eb55ff50fc6d2e4cacb0bfea',
}
EVENTS = frozenset({
    'ENTRY', 'STDIO_BOUND', 'FOREIGN_FD_ABSENT', 'PREPARATION_BOUNDARY',
    'PREPARATION_OK', 'UNPICKLE_BOUNDARY', 'UNPICKLE_OK', 'WORKER_WRAPPER',
    'WORKER_ERROR', 'EXIT23', 'EXIT23_FLUSH_CONFIRMED', 'HANG', 'FINAL_CONFIRMED', 'FINAL_UNCONFIRMED',
    'BOOTSTRAP_IMPORT_BOUNDARY', 'BOOTSTRAP_PIPE_WRITE_REFUSED',
    'CANARY_REJECTED', 'TRACKER_ENTRY', 'AUXILIARY_EOF', 'SESSION_OWNED', 'PDEATH_BOUND',
    'FOREIGN_FD_REFUSED',
    'GUARD_DENIALS_CONFIRMED',
})
_guard = None
_case_lock = threading.Lock()
# Preserve actual transport ownership if bounded thread shutdown fails. A later
# case must not hide that lifetime by replacing the global tracker descriptor.
_unsettled_transports = []


class NativeAdmissionError(RuntimeError):
    """Only closed constant codes cross the fixture boundary."""


class _ProcessGuard:
    def __init__(self):
        self.command = None
        self.owner = None
        self.denials = {}

    def __call__(self, event, args):
        if event == 'subprocess.Popen':
            allowed = (self.command is not None and self.owner == threading.get_ident()
                       and len(args) >= 2 and args[0] == self.command[0]
                       and type(args[1]) in {list, tuple} and tuple(args[1]) == self.command)
            if allowed:
                return
        elif event == '_posixsubprocess.fork_exec':
            # CPython 3.14's internal event also covers direct tracker relaunch
            # via util.spawnv_passfds, which does not emit subprocess.Popen.
            allowed = (self.command is not None and self.owner == threading.get_ident()
                       and len(args) >= 2 and type(args[0]) in {list, tuple}
                       and tuple(args[0]) == (os.fsencode(self.command[0]),)
                       and type(args[1]) in {list, tuple} and tuple(args[1]) == self.command)
            if allowed:
                return
        elif not (event.startswith('socket.') or event in {
                'os.system', 'os.fork', 'os.forkpty', 'os.posix_spawn',
                'os.exec', 'os.spawn', 'pty.spawn'}):
            return
        self.denials[event] = self.denials.get(event, 0) + 1
        raise NativeAdmissionError('LNX03_NATIVE_AUDIT_DENIED')


def install_process_guard():
    """Install before pytest or third-party imports; network is never admitted."""
    global _guard
    if _guard is None:
        _guard = _ProcessGuard()
        sys.addaudithook(_guard)
    return _guard


def _safe_directory(value):
    path = Path(os.path.abspath(value))
    if path.resolve() != path:
        raise NativeAdmissionError('LNX03_NATIVE_PATH_ALIAS')
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise NativeAdmissionError('LNX03_NATIVE_PATH_OWNER')
    return path


def _native_root(repo):
    value = os.environ.get('LNX03_NATIVE_ROOT')
    if value is None:
        raise NativeAdmissionError('LNX03_NATIVE_ROOT_UNDECLARED')
    root = _safe_directory(value)
    if root.stat().st_mode & 0o077:
        raise NativeAdmissionError('LNX03_NATIVE_ROOT_PERMISSION')
    return root


def attest_runtime():
    if (sys.platform != 'linux' or sys.implementation.name != 'cpython'
            or sys.version_info[:3] != (3, 14, 8)
            or os.uname().machine != 'x86_64' or struct.calcsize('P') != 8
            or sysconfig_gil_disabled() or not hasattr(os, 'pidfd_open')
            or not hasattr(os, 'P_PIDFD') or not hasattr(os, 'O_NOFOLLOW')):
        raise NativeAdmissionError('LNX03_NATIVE_PLATFORM_UNSUPPORTED')
    import multiprocessing
    directory = Path(multiprocessing.__file__).resolve().parent
    observed = {}
    for name, expected in SPAWN_SOURCES.items():
        data = (directory / name).read_bytes().replace(b'\r\n', b'\n')
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected:
            raise NativeAdmissionError('LNX03_NATIVE_SOURCE_UNSUPPORTED')
        observed[name] = digest
    return observed


def sysconfig_gil_disabled():
    import sysconfig
    return bool(sysconfig.get_config_var('Py_GIL_DISABLED'))


def _json_bytes(value):
    return (json.dumps(value, separators=(',', ':'), sort_keys=True,
                       ensure_ascii=True, allow_nan=False) + '\n').encode('ascii')


def _write_exclusive(path, data):
    """Own the raw FD through every write failure; no fdopen handoff gap."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, memoryview(data)[offset:])
            if written <= 0:
                raise NativeAdmissionError('LNX03_NATIVE_FILE_WRITE_FAILED')
            offset += written
    finally:
        os.close(descriptor)


def _ticket(path, value):
    data = _json_bytes(value)
    if len(data) > 65536:
        raise NativeAdmissionError('LNX03_NATIVE_TICKET_BOUND')
    _write_exclusive(path, data)


class _PassedFd:
    def __init__(self, descriptor):
        self.descriptor = descriptor

    def detach(self):
        return self.descriptor


class _SpawnSerialization:
    """The two methods exercised by the pinned POSIX reductions, only."""
    DupFd = _PassedFd

    def __init__(self):
        self.descriptors = set()

    def duplicate_for_child(self, descriptor):
        os.fstat(descriptor)
        self.descriptors.add(descriptor)
        return descriptor


def _serialize(bootstrap, repo):
    from multiprocessing import context, reduction
    if context.get_spawning_popen() is not None:
        raise NativeAdmissionError('LNX03_NATIVE_NESTED_SPAWN')
    adapter = _SpawnSerialization()
    preparation = {
        'name': 'lnx03-fixture', 'sys_path': list(sys.path),
        'sys_argv': ['lnx03-fixture'], 'dir': str(repo),
        'orig_dir': str(repo), 'start_method': 'spawn',
    }
    first, second = io.BytesIO(), io.BytesIO()
    context.set_spawning_popen(adapter)
    try:
        reduction.dump(preparation, first)
        reduction.dump(bootstrap, second)
    finally:
        context.set_spawning_popen(None)
    if len(first.getvalue()) + len(second.getvalue()) > 65536:
        raise NativeAdmissionError('LNX03_NATIVE_PAYLOAD_BOUND')
    return first.getvalue(), second.getvalue(), adapter.descriptors


def _launch(repo, ticket, descriptors, *, owner):
    guard = install_process_guard()
    command = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
               str(repo / 'tests/linux_bootstrap/native_worker.py'), '--ticket', str(ticket))
    if guard.command is not None:
        raise NativeAdmissionError('LNX03_NATIVE_LAUNCH_BUSY')
    guard.command, guard.owner = command, threading.get_ident()
    try:
        # Publish each observation before the next fallible operation. Even a
        # binding/registration failure leaves the sole Popen reaper with us.
        owner['birth_attempted'] = True
        child = subprocess.Popen(command, executable=command[0], cwd=repo,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
                 'LNX03_NATIVE_ROOT': str(_native_root(repo))},
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            close_fds=True, pass_fds=tuple(sorted(descriptors)), start_new_session=True)
        owner['process'] = child
    finally:
        guard.command, guard.owner = None, None
    descriptor = os.pidfd_open(child.pid, 0)
    owner['pidfd'] = descriptor
    identity = _process_identity(child.pid)
    owner['identity'] = identity
    return child, descriptor, identity


def _admit_descriptors(expected, requested):
    if (type(expected) is not set or type(requested) is not set or expected != requested
            or any(type(item) is not int or item < 3 for item in requested)):
        raise NativeAdmissionError('LNX03_NATIVE_FOREIGN_FD_REFUSED')
    for descriptor in requested:
        os.fstat(descriptor)


def _process_identity(pid):
    data = Path('/proc/' + str(pid) + '/stat').read_text(encoding='ascii')
    fields = data.rsplit(')', 1)[1].split()
    if len(fields) < 20 or int(fields[1]) != os.getpid():
        raise NativeAdmissionError('LNX03_NATIVE_PID_IDENTITY')
    return dict(start_ticks=int(fields[19]), parent_pid=int(fields[1]), session=int(fields[3]))


def _registry(work, children, tracker):
    """Owned emergency inventory; no claim of settlement merely from a PID."""
    records = []
    for item in ([tracker] if tracker is not None else []) + children:
        if item['process'] is None:
            continue
        records.append(dict(pid=item['process'].pid, **item['identity'],
                            role=item.get('role', 'tracker'), pidfd_bound=True))
    path = work / 'pid-registry.json'
    temporary = work / 'pid-registry.pending'
    _write_exclusive(temporary, _json_bytes(dict(schema='lnx03-pid-registry-v1', processes=records)))
    os.replace(temporary, path)


def settle_native_root(root):
    """Emergency OS-exit proof for registered fixtures, never a case PASS.

    The controller may observe grandchildren through pidfds; it cannot claim
    to have reaped them. A missing already-exited PID is reported separately
    from a pidfd-confirmed exit. Mismatched/reused identities are never killed.
    This inventory supplements the controller's runner/descendant containment;
    it cannot prove the absence of an unregistered birth.
    """
    if sys.platform != 'linux':
        raise NativeAdmissionError('LNX03_NATIVE_PLATFORM_UNSUPPORTED')
    root = _safe_directory(root)
    if root.stat().st_mode & 0o077:
        raise NativeAdmissionError('LNX03_NATIVE_ROOT_PERMISSION')
    report = dict(schema='lnx03-emergency-settlement-v1', scope='REGISTERED_FIXTURES_ONLY',
                  status='INCOMPLETE', pidfd_confirmed=0, already_absent=0,
                  unsettled=0, identity_refusals=0, reaped_by_controller=False)
    bound = []
    try:
        for path in root.rglob('pid-registry.json'):
            if path.resolve() != path:
                raise NativeAdmissionError('LNX03_NATIVE_REGISTRY_ALIAS')
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, 'rb') as handle:
                info = os.fstat(handle.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > 65536):
                    raise NativeAdmissionError('LNX03_NATIVE_REGISTRY_FILE')
                record = json.loads(handle.read(65537))
            if (type(record) is not dict or set(record) != {'schema', 'processes'}
                    or record['schema'] != 'lnx03-pid-registry-v1'
                    or type(record['processes']) is not list or len(record['processes']) > 3):
                raise NativeAdmissionError('LNX03_NATIVE_REGISTRY_SCHEMA')
            for item in record['processes']:
                if (type(item) is not dict or set(item) != {'pid', 'start_ticks', 'parent_pid', 'session', 'role', 'pidfd_bound'}
                        or any(type(item[key]) is not int or item[key] <= 1 for key in ('pid', 'parent_pid', 'session', 'start_ticks'))
                        or item['role'] not in {'google', 'ftp', 'tracker'} or item['pidfd_bound'] is not True
                        or item['pid'] != item['session']):
                    raise NativeAdmissionError('LNX03_NATIVE_REGISTRY_SCHEMA')
                descriptor = None
                try:
                    descriptor = os.pidfd_open(item['pid'], 0)
                    data = Path('/proc/' + str(item['pid']) + '/stat').read_text(encoding='ascii')
                    fields = data.rsplit(')', 1)[1].split()
                    if int(fields[19]) != item['start_ticks'] or int(fields[3]) != item['session']:
                        report['identity_refusals'] += 1
                        os.close(descriptor)
                        continue
                    bound.append(descriptor)
                except (ProcessLookupError, FileNotFoundError):
                    if descriptor is not None:
                        os.close(descriptor)
                    report['already_absent'] += 1
        deadline = time.monotonic() + 10
        for descriptor in bound:
            if not select.select([descriptor], [], [], 0)[0]:
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        term_end = min(deadline, time.monotonic() + 5)
        while time.monotonic() < term_end and any(not select.select([fd], [], [], 0)[0] for fd in bound):
            time.sleep(0.02)
        for descriptor in bound:
            if not select.select([descriptor], [], [], 0)[0]:
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for descriptor in bound:
            if select.select([descriptor], [], [], max(0, deadline - time.monotonic()))[0]:
                report['pidfd_confirmed'] += 1
            else:
                report['unsettled'] += 1
        if not report['unsettled'] and not report['identity_refusals']:
            report['status'] = 'REGISTERED_EXITED'
    except BaseException:
        report['unsettled'] += 1
    finally:
        for descriptor in bound:
            os.close(descriptor)
    return report


def _pidfd_exited(descriptor):
    readable, _, _ = select.select([descriptor], [], [], 0)
    if not readable:
        return False
    # WNOWAIT: Popen alone consumes/reaps the exit.
    result = os.waitid(os.P_PIDFD, descriptor, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    return result is not None and result.si_pid > 0


def _write_bootstrap(descriptor, data, deadline):
    """Bound pipe transfer by the same deadline; pipe capacity is not assumed."""
    os.set_blocking(descriptor, False)
    offset = 0
    while offset < len(data):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise NativeAdmissionError('LNX03_NATIVE_BOOTSTRAP_DEADLINE')
        if not select.select([], [descriptor], [], remaining)[1]:
            raise NativeAdmissionError('LNX03_NATIVE_BOOTSTRAP_DEADLINE')
        try:
            written = os.write(descriptor, memoryview(data)[offset:])
        except BlockingIOError:
            continue
        if written <= 0:
            raise NativeAdmissionError('LNX03_NATIVE_BOOTSTRAP_WRITE_FAILED')
        offset += written


def _settle(children, timeout=10):
    """One shared TERM/KILL budget, never one budget per child."""
    deadline = time.monotonic() + timeout
    for item in children:
        if item['process'].poll() is None:
            try:
                if item['pidfd'] is not None:
                    signal.pidfd_send_signal(item['pidfd'], signal.SIGTERM)
                else:
                    # Only the already-owned Popen, never an unbound PID from
                    # a ticket/registry. This covers post-birth bind refusal.
                    item['process'].terminate()
            except OSError:
                pass
    term_deadline = min(deadline, time.monotonic() + 5)
    while time.monotonic() < term_deadline and any(item['process'].poll() is None for item in children):
        time.sleep(0.02)
    for item in children:
        if item['process'].poll() is None:
            try:
                if item['pidfd'] is not None:
                    signal.pidfd_send_signal(item['pidfd'], signal.SIGKILL)
                else:
                    item['process'].kill()
            except OSError:
                pass
    for item in children:
        try:
            item['process'].wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass


def _read_events(descriptor):
    result, data = [], bytearray()
    os.set_blocking(descriptor, False)
    while True:
        try:
            chunk = os.read(descriptor, 4096)
        except BlockingIOError:
            break
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > 16384:
            raise NativeAdmissionError('LNX03_NATIVE_WITNESS_BOUND')
    for line in data.splitlines():
        try:
            event = line.decode('ascii')
        except UnicodeError:
            raise NativeAdmissionError('LNX03_NATIVE_WITNESS_INVALID') from None
        if event not in EVENTS:
            raise NativeAdmissionError('LNX03_NATIVE_WITNESS_INVALID')
        result.append(event)
    return result


def _case_passed(case, result):
    records = result['worker_records']
    if (len(records) != 2 or not result['two_workers_registered']
            or not result['all_workers_settled'] or not result['auxiliary']['settled']
            or not result['auxiliary']['reaped'] or result['auxiliary']['exitcode'] != 0
            or not result['console_canary_absent']
            or not all(record['pidfd_bound'] and record['reaped'] and record['session_owned']
                       and record['stdio_redirected'] and record['foreign_fd_absent']
                       and 'GUARD_DENIALS_CONFIRMED' in record['events'] for record in records)):
        return False
    google = records[0]
    events = google['events']
    if case in {'success', 'foreign_fd', 'auxiliary_settlement'}:
        return (result['application_exit'] == 0 and all(record['exitcode'] == 0 for record in records)
                and result['logging']['delivery_outcome'] == 'COMPLETE'
                and result['logging']['worker_final_count'] == 2
                and (case != 'foreign_fd' or 'FOREIGN_FD_REFUSED' in records[0]['events']))
    if result['application_exit'] == 0:
        return False
    if case == 'hang':
        return result['deadline_detected'] and result['cleanup_requested'] and 'HANG' in events
    if case == 'child_error':
        return 'WORKER_ERROR' in events and google['exitcode'] != 0
    if case == 'exit23':
        return ('EXIT23_FLUSH_CONFIRMED' in events and 'EXIT23' in events
                and events.index('EXIT23_FLUSH_CONFIRMED') < events.index('EXIT23')
                and google['exitcode'] == 23 and result['logging']['worker_final_count'] < 2
                and not {'FINAL_CONFIRMED', 'FINAL_UNCONFIRMED'}.intersection(events))
    if case == 'bootstrap_import_failure':
        return 'BOOTSTRAP_IMPORT_BOUNDARY' in events and 'WORKER_WRAPPER' not in events
    if case == 'bootstrap_replay_failure':
        return 'PREPARATION_BOUNDARY' in events and 'PREPARATION_OK' not in events
    if case == 'bootstrap_unpickle_failure':
        return 'UNPICKLE_BOUNDARY' in events and 'UNPICKLE_OK' not in events
    if case == 'bootstrap_pipe_failure':
        return 'BOOTSTRAP_PIPE_WRITE_REFUSED' in events and 'WORKER_WRAPPER' not in events
    if case in {'ack_failure', 'listener_failure'}:
        return ('WORKER_WRAPPER' in events
                and result['logging']['delivery_outcome'] == 'INCOMPLETE')
    return case == 'canary_positive_control' and 'CANARY_REJECTED' in events


def run_native_case(case, repo_root, scratch):
    """Run exactly two fixed fixture workers, no publication or arbitrary target."""
    if case not in NATIVE_CASES:
        raise NativeAdmissionError('LNX03_NATIVE_CASE_INVALID')
    attestation = attest_runtime()
    repo, scratch = _safe_directory(repo_root), _safe_directory(scratch)
    if not scratch.is_relative_to(_native_root(repo)):
        raise NativeAdmissionError('LNX03_NATIVE_SCRATCH_OUTSIDE')
    source_sha = os.environ.get('LNX03_SOURCE_SHA', '')
    if re.fullmatch(r'[0-9a-f]{40}', source_sha) is None:
        raise NativeAdmissionError('LNX03_NATIVE_SOURCE_IDENTITY_MISSING')
    if not _case_lock.acquire(blocking=False):
        raise NativeAdmissionError('LNX03_NATIVE_CONCURRENT_CASE')
    try:
        return _run_native_case(case, repo, scratch, attestation)
    finally:
        _case_lock.release()


def _run_native_case(case, repo, scratch, attestation):
    import run_logging
    from multiprocessing import resource_tracker
    tracker_state = resource_tracker._resource_tracker
    if _unsettled_transports or tracker_state._fd is not None or tracker_state._pid is not None:
        raise NativeAdmissionError('LNX03_NATIVE_FOREIGN_TRACKER')
    work = scratch / ('native-' + uuid.uuid4().hex)
    work.mkdir(mode=0o700)
    children, session, bootstraps = [], None, []
    tracker = dict(role='tracker', process=None, pidfd=None, identity=None, birth_attempted=False)
    owned_fds = set()
    tracker_read = tracker_write = foreign = None
    transport_settled = True
    execution_complete = cleanup_attempted = cleanup_failed = False

    def owned_pipe():
        pair = os.pipe()
        owned_fds.update(pair)
        return pair

    def close_owned(descriptor):
        nonlocal cleanup_failed
        if descriptor in owned_fds:
            # Relinquish once before close, even if close raises: retrying an
            # integer later could close an unrelated reused descriptor.
            owned_fds.remove(descriptor)
            try:
                os.close(descriptor)
            except OSError:
                cleanup_failed = True

    result = dict(schema='lnx03-native-case-v1', case=case, status='INCOMPLETE',
        failure_code='LNX03_NATIVE_INCOMPLETE', application_exit=1,
        deadline_seconds=DEADLINE_SECONDS, deadline_detected=False, cleanup_requested=False,
        all_workers_settled=False, two_workers_registered=False, worker_records=[],
        logging=dict(delivery_outcome='INCOMPLETE', storage_sealed=False, exit_status=1, worker_final_count=0),
        auxiliary=dict(owned=False, pid=None, pidfd_bound=False, settled=False, reaped=False, exitcode=None),
        source_attestation=attestation, console_canary_absent=False)
    try:
        tracker_read, tracker_write = owned_pipe()
        foreign = os.open(work / 'foreign-marker', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        owned_fds.add(foreign)
        os.set_inheritable(foreign, True)
        foreign_identity = list(_fd_identity(foreign))
        tracker_ticket = work / 'tracker.json'
        _ticket(tracker_ticket, dict(schema='lnx03-native-ticket-v1', role='tracker',
            case=case, tracker_fd=tracker_read, repo=str(repo), parent_pid=os.getpid(),
            expected_fds=[tracker_read]))
        process, descriptor, identity = _launch(repo, tracker_ticket, {tracker_read}, owner=tracker)
        result['auxiliary'].update(owned=True, pid=process.pid, pidfd_bound=True)
        _registry(work, children, tracker)
        close_owned(tracker_read)
        # The pinned resource tracker checks this existing pipe. Its _pid stays
        # None: our Popen owns this tracker and is the only allowed reaper.
        tracker_state._fd = tracker_write
        session = run_logging.configure_logging(work, run_id='lnx03-' + uuid.uuid4().hex,
            environment='offline', producer_sha=os.environ['LNX03_SOURCE_SHA'],
            config={'log_root': '.f-log/lnx03', 'retained_runs': 1, 'critical_deadline_s': 0.5,
                    'shutdown_deadline_s': 2.0}, sensitive_values=(CANARY.decode('ascii'),))
        for role in ('google', 'ftp'):
            bootstraps.append(run_logging.prepare_worker(session, role))
        result['two_workers_registered'] = len(session['_expected']) == 2
        if case == 'listener_failure':
            # The real listener encounters a closed native output stream on
            # its next event. No diagnostic flag or successful seal is forged.
            session['_primary'].close()
        if case == 'ack_failure':
            # Real transport failure before workers send a final. Never a
            # successful seal followed by a fabricated exception.
            session['_ack_senders'][bootstraps[0]['_token']].close()
        deadline = time.monotonic() + DEADLINE_SECONDS
        for index, role in enumerate(('google', 'ftp')):
            item = dict(role=role, process=None, pidfd=None, identity=None,
                        birth_attempted=False, witness=None, parent_events=[], session_owned=False)
            children.append(item)
            local_fds = []
            try:
                preparation, payload, descriptors = _serialize(bootstraps[index], repo)
                read_end, write_end = owned_pipe()
                local_fds.extend((read_end, write_end))
                witness_read, witness_write = owned_pipe()
                local_fds.extend((witness_read, witness_write))
                ticket = work / (role + '.json')
                inherited = descriptors | {read_end, witness_write, tracker_write}
                if case == 'foreign_fd' and index == 0:
                    try:
                        _admit_descriptors(inherited, inherited | {foreign})
                    except NativeAdmissionError:
                        item['parent_events'].append('FOREIGN_FD_REFUSED')
                    else:
                        raise NativeAdmissionError('LNX03_NATIVE_FOREIGN_FD_ACCEPTED')
                _admit_descriptors(inherited, inherited)
                _ticket(ticket, dict(schema='lnx03-native-ticket-v1', role=role, case=case,
                    tracker_fd=tracker_write, bootstrap_fd=read_end, witness_fd=witness_write,
                    foreign_fd=foreign, foreign_identity=foreign_identity, repo=str(repo),
                    parent_pid=os.getpid(), expected_fds=sorted(inherited)))
                process, descriptor, identity = _launch(repo, ticket, inherited, owner=item)
                # Only a successful launch transfers this reader to the final
                # witness collection. All other pipe ends stay scope-owned.
                item['witness'] = witness_read
                local_fds.remove(witness_read)
                close_owned(read_end)
                close_owned(witness_write)
                _registry(work, children, tracker)
                try:
                    item['session_owned'] = os.getsid(process.pid) == process.pid
                except ProcessLookupError:
                    pass
                if case == 'bootstrap_pipe_failure' and index == 0:
                    # A real write failure after native birth, before bootstrap
                    # transfer. No other file operation can reuse this fd
                    # between the intentional close and the failing write.
                    refused = write_end
                    close_owned(write_end)
                    try:
                        os.write(refused, preparation + payload)
                    except OSError:
                        item['parent_events'].append('BOOTSTRAP_PIPE_WRITE_REFUSED')
                        result['cleanup_requested'] = True
                else:
                    if case == 'bootstrap_unpickle_failure' and index == 0:
                        payload = b'!invalid-pickle!'
                    try:
                        _write_bootstrap(write_end, preparation + payload, deadline)
                    except BrokenPipeError:
                        item['parent_events'].append('BOOTSTRAP_PIPE_WRITE_REFUSED')
            finally:
                for descriptor in local_fds:
                    close_owned(descriptor)
        # No mutable checkpoint polling occurs. Only pidfds/process handles and
        # F-LOG health are observed while workers execute.
        while True:
            alive = []
            for item in children:
                if item['process'].returncode is None and _pidfd_exited(item['pidfd']):
                    item['process'].wait(timeout=0)
                if item['process'].returncode is None:
                    alive.append(item)
            if not alive:
                break
            if time.monotonic() >= deadline:
                result['deadline_detected'] = True
                result['cleanup_requested'] = True
                cleanup_attempted = True
                _settle(children)
                break
            # Fixed fixture peers are allowed to finish; a failed sibling never
            # makes them successful. A hanging peer is still bounded by this
            # same parent deadline. This avoids inventing pre-entry witnesses.
            time.sleep(0.02)
        execution_complete = True
    except BaseException:
        # Fault details, raw streams, exception strings and tracebacks never
        # cross the result boundary. Missing evidence remains INCOMPLETE.
        result['failure_code'] = 'LNX03_NATIVE_EXECUTION_INCOMPLETE'
    finally:
        born = [item for item in children if item['process'] is not None]
        unknown_birth = any(item['birth_attempted'] and item['process'] is None
                            for item in [tracker] + children)
        if not cleanup_attempted and any(item['process'].poll() is None for item in born):
            result['cleanup_requested'] = True
            try:
                _settle(born)
            except BaseException:
                cleanup_failed = True
        result['all_workers_settled'] = (not unknown_birth and
                                        all(item['process'].returncode is not None for item in born))
        for item in born:
            process = item['process']
            settled = process.returncode is not None
            events = list(item['parent_events'])
            try:
                if settled and item['witness'] is not None:
                    events += _read_events(item['witness'])
            except BaseException:
                cleanup_failed = True
            result['worker_records'].append(dict(role=item['role'], pid=process.pid,
                pidfd_bound=item['pidfd'] is not None,
                session_owned=item['session_owned'] or 'SESSION_OWNED' in events,
                exitcode=process.returncode, reaped=settled, events=events,
                foreign_fd_absent='FOREIGN_FD_ABSENT' in events, stdio_redirected='STDIO_BOUND' in events))
        if session is not None:
            result['two_workers_registered'] = set(session['_expected']) == {'google', 'ftp'}
            for role in session['_expected']:
                item = next((item for item in children if item['role'] == role), None)
                if item is not None and item['birth_attempted'] and item['process'] is None:
                    # A constructor exception supplies neither a no-birth proof
                    # nor an exit observation. Omit it: F-LOG stays incomplete.
                    continue
                process = item['process'] if item is not None else None
                try:
                    run_logging.record_worker_outcome(session, role, started=process is not None,
                        exit_status=process.returncode if process is not None else None,
                        timed_out=result['deadline_detected'] and process is not None and process.returncode != 0)
                except BaseException:
                    cleanup_failed = True
            actual_success = (execution_complete and not cleanup_failed and len(born) == 2
                              and result['two_workers_registered'] and result['all_workers_settled']
                              and not result['deadline_detected']
                              and all(item['process'].returncode == 0 for item in born))
            try:
                report = run_logging.finalize_logging(session,
                    execution_outcome='COMPLETED' if actual_success else 'FAILED',
                    exit_status=0 if actual_success else 1)
                result['logging'] = dict(delivery_outcome=report['delivery_outcome'],
                    storage_sealed=report['storage_sealed'], exit_status=report['exit_status'],
                    worker_final_count=len(session['_worker_finals']))
                result['application_exit'] = 0 if (actual_success and report['delivery_outcome'] == 'COMPLETE'
                                                  and report['storage_sealed']) else 1
            except BaseException:
                cleanup_failed = True
        result['console_canary_absent'] = (len(born) == 2 and
                                           all(record['stdio_redirected'] for record in result['worker_records']))
        for item in born:
            close_owned(item['witness'])
            if item['pidfd'] is not None and item['process'].returncode is not None:
                owned_fds.add(item['pidfd'])
                close_owned(item['pidfd'])
        if session is not None:
            # F-LOG has finalized before this fixture-only ownership cleanup.
            # cancel_join_thread() is not a feeder exit witness: its real
            # Thread still owns Queue semaphores until consuming the sentinel.
            # Retain no Queue alias across the subsequent collection.
            feeder = session['_queue']._thread
            try:
                session['_queue'].close()
                if feeder is not None:
                    feeder.join(timeout=2)
                transport_settled = (all(item['process'].returncode is not None for item in born) and
                                     (feeder is None or not feeder.is_alive())
                                     and not session['_thread'].is_alive())
            except BaseException:
                transport_settled = False
            finally:
                feeder = None
        # Unknown constructor birth cannot become successful. Once our known
        # transports stopped, close owned pipe ends; the outer OS containment
        # still owns any unobserved birth and later cases remain refused.
        if transport_settled:
            bootstraps.clear()
            session = None
            gc.collect()  # SemLock finalizers must send UNREGISTER before EOF.
            if tracker_state._fd == tracker_write:
                tracker_state._fd = None
            close_owned(tracker_write)
        else:
            # No synthetic settlement, premature tracker EOF, or later-case
            # recovery. Preserve live references and the registered tracker for
            # fail-closed runner/controller teardown and its retained scratch.
            _unsettled_transports.append(dict(session=session, bootstraps=bootstraps,
                                               tracker=tracker, tracker_write=tracker_write, children=children))
            result['application_exit'] = 1
            result['cleanup_requested'] = True
        for descriptor in tuple(owned_fds):
            if transport_settled or descriptor != tracker_write:
                close_owned(descriptor)
        if tracker['process'] is not None and transport_settled:
            process = tracker['process']
            result['auxiliary'].update(owned=True, pid=process.pid, pidfd_bound=tracker['pidfd'] is not None)
            try:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    _settle([tracker], timeout=5)
            except BaseException:
                cleanup_failed = True
            result['auxiliary'].update(settled=process.returncode is not None,
                                       reaped=process.returncode is not None, exitcode=process.returncode)
            if tracker['pidfd'] is not None and process.returncode is not None:
                owned_fds.add(tracker['pidfd'])
                close_owned(tracker['pidfd'])
            elif process.returncode is None:
                _unsettled_transports.append(dict(tracker=tracker))
        if unknown_birth:
            _unsettled_transports.append(dict(unknown_births=[item for item in [tracker] + children
                if item['birth_attempted'] and item['process'] is None]))
    if (not transport_settled or unknown_birth or cleanup_failed
            or not result['all_workers_settled'] or not result['auxiliary']['settled']):
        result['application_exit'] = 1
        result['status'] = 'INCOMPLETE'
        result['failure_code'] = 'LNX03_NATIVE_CLEANUP_INCOMPLETE'
        return result
    passed = execution_complete and _case_passed(case, result)
    result['status'] = 'PASS' if passed else 'FAIL'
    result['failure_code'] = None if passed else 'LNX03_NATIVE_ORACLE_FAILED'
    return result


def _fd_identity(descriptor):
    info = os.fstat(descriptor)
    return info.st_dev, info.st_ino, info.st_mode, info.st_rdev


# Stage B has a separate closed authority. None of these names extend the
# LNX-03 fixture cases, source manifest, runner ticket or native launcher.
D2_CASES = (
    'success', 'google_error', 'ftp_error', 'exit23', 'system_exit0',
    'system_exit_string', 'abnormal_before_ready', 'abnormal_after_final',
    'normal_without_final', 'final_then_exit23', 'start_google_before',
    'start_ftp_before', 'start_google_after_native', 'start_ftp_after_native',
    'constructor_error', 'hang_google', 'hang_ftp', 'cleanup_unsettled',
    'partial_google', 'partial_ftp', 'checkpoint_missing', 'checkpoint_corrupt',
    'checkpoint_oversize', 'checkpoint_stale_run', 'checkpoint_stale_pid',
    'checkpoint_stale_sequence', 'listener_failure', 'ack_failure',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'foreign_fd', 'canary_positive_control',
    'parent_finalization_raises', 'parent_finalization_incomplete',
    'postseal_cleanup_failure', 'cancel_google', 'cancel_ftp',
    'ticket_ftp_failure', 'pidfd_google_failure', 'identity_google_failure',
    'registry_google_failure', 'registration_google_failure',
    'registration_ftp_failure', 'bootstrap_pipe_failure', 'replace_failure', 'tracker_death',
    'preupload_stage_failure',
    'post_context_bind_failure',
    'serialization_write_failure',
)
D2_BASELINE_CASES = (
    'baseline_google_swallowed', 'baseline_ftp_root_swallowed',
    'baseline_ftp_dividends_swallowed', 'baseline_ftp_tickers_swallowed',
    'baseline_exit23', 'baseline_hang',
)
D2_SOURCE_PATHS = (
    'upload.py', 'upload.ipynb', 'main.py', '.github/lnx02/verify_env.py',
    '.github/workflows/d2-linux-offline.yml', 'tools/offline_linux.py',
    'tools/offline_guard_linux.py', 'tests/d2/conftest.py',
    'tests/d2/d2_worker.py', 'tests/d2/test_supervision.py',
    'tests/d2/test_targets.py', 'tests/d2/test_logging_contract.py',
    'tests/d2/test_guard_admission.py', 'tests/fixtures/d2/expected.json',
    'docs/d2_supervision.md', 'docs/testing.md', 'docs/coordination.md',
)
D2_CONTROLLER_SECONDS = 75
_d2_controller = None
_d2_profile = None
_d2_runner = None
_d2_case_lock = threading.Lock()
_d2_legacy_issued = {}
_d2_worker_launches = set()


def _d2_require(value, code):
    if not value:
        raise NativeAdmissionError(code)


def _d2_path(value, directory=False):
    path = Path(os.path.abspath(value))
    for component in (path, *path.parents):
        info = component.lstat()
        _d2_require(not stat.S_ISLNK(info.st_mode), 'D2_NATIVE_PATH_ALIAS')
    info = path.lstat()
    _d2_require(path.resolve() == path and info.st_uid == os.getuid(), 'D2_NATIVE_PATH_OWNER')
    _d2_require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode),
                'D2_NATIVE_PATH_KIND')
    if not directory:
        _d2_require(info.st_nlink == 1, 'D2_NATIVE_PATH_LINK')
    return path


def _d2_read(path, maximum=1024 * 1024):
    path = _d2_path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        handle = os.fdopen(descriptor, 'rb')
    except BaseException:
        os.close(descriptor)
        raise
    with handle:
        info = os.fstat(handle.fileno())
        _d2_require(info.st_nlink == 1 and info.st_uid == os.getuid()
                    and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size <= maximum,
                    'D2_NATIVE_RECORD_BOUND')
        data = handle.read(maximum + 1)
    _d2_require(len(data) <= maximum, 'D2_NATIVE_RECORD_BOUND')
    def pairs(items):
        value = {}
        for key, item in items:
            _d2_require(key not in value, 'D2_NATIVE_RECORD_DUPLICATE')
            value[key] = item
        return value
    def constant(value):
        raise NativeAdmissionError('D2_NATIVE_RECORD_CONSTANT')
    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _d2_ticks(pid):
    fields = Path('/proc/' + str(pid) + '/stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
    return int(fields[19])


def _d2_sources(repo):
    return {name: hashlib.sha256(_d2_path(repo / name).read_bytes()).hexdigest()
            for name in D2_SOURCE_PATHS}


def d2_runner_admission(ticket):
    """Bind the one-use runner ticket, not an environment-only test switch."""
    global _d2_runner
    _d2_require(_d2_runner is None, 'D2_NATIVE_RUNNER_REUSED')
    path = _d2_path(os.environ.get('MDS_D2_LINUX_RUN_TICKET', '/d2-undeclared'))
    raw = path.read_bytes()
    _d2_require(hashlib.sha256(raw).hexdigest() == os.environ.get('MDS_D2_LINUX_RUN_TICKET_SHA256')
                and _d2_read(path, 65536) == ticket, 'D2_NATIVE_RUNNER_TICKET')
    root = _d2_path(ticket['root'], True)
    authority = ticket['authority']
    _d2_require(ticket['schema_version'] == 'd2-linux-runner-ticket-v1'
                and ticket['phase'] == 'application' and ticket['parent_pid'] == os.getppid()
                and path == root / 'jobs' / 'application-application-ticket.json'
                and path.with_suffix('.claimed').is_file()
                and _d2_read(root / '.lnx02-owner.json', 65536) == {'nonce': ticket['nonce'], 'purpose': 'D2-LINUX'}
                and authority['approved'] is True and authority['consumer'] == authority['job'] == 'd2-linux'
                and authority['sha'] == ticket['source_sha'] == os.environ.get('MDS_D2_SOURCE_SHA')
                and authority['actor'] == authority['triggering_actor'] == 'foykes'
                and authority['attempt'] == 2 and type(authority['attempt']) is int,
                'D2_NATIVE_RUNNER_IDENTITY')
    repo = _d2_path(ticket['repo'], True)
    _d2_require(repo == Path(__file__).absolute().parents[1]
                and all(ticket['source_files_sha256'].get(name) == digest
                        for name, digest in _d2_sources(repo).items())
                and stat.S_IMODE(root.stat().st_mode) == 0o700
                and ticket['native_root'] == str(root / 'fixtures' / 'd2'), 'D2_NATIVE_RUNNER_SOURCE')
    _d2_runner = dict(pid=os.getpid(), start_ticks=_d2_ticks(os.getpid()), root=str(root),
        native_root=ticket['native_root'], repo=str(repo), source_sha=ticket['source_sha'],
        source_files_sha256=_d2_sources(repo))
    return True


def d2_controller_admission(ticket):
    """Called only by the fixed, source-bound controller before vendor imports."""
    global _d2_controller
    _d2_require(_d2_controller is None, 'D2_NATIVE_CONTROLLER_REUSED')
    root = _d2_path(os.environ.get('MDS_D2_CONTROLLER_ROOT', '/d2-undeclared'), True)
    path = root / 'controller.ticket.json'
    raw = path.read_bytes()
    _d2_require(hashlib.sha256(raw).hexdigest() == os.environ.get('MDS_D2_CONTROLLER_TICKET_SHA256')
                and _d2_read(path, 65536) == ticket, 'D2_NATIVE_CONTROLLER_TICKET')
    keys = {'schema_version', 'role', 'case', 'repo_root', 'scratch', 'source_sha',
            'parent_pid', 'nonce', 'source_files_sha256', 'result'}
    _d2_require(type(ticket) is dict and set(ticket) == keys
                and ticket['schema_version'] == 'd2-controller-ticket-v1'
                and ticket['role'] == 'controller'
                and ticket['case'] in D2_CASES + D2_BASELINE_CASES
                and ticket['parent_pid'] == os.getppid()
                and ticket['scratch'] == str(root)
                and ticket['result'] == str(root / 'controller-result.json')
                and re.fullmatch('[0-9a-f]{32}', ticket['nonce'])
                and ticket['source_sha'] == os.environ.get('MDS_D2_SOURCE_SHA'),
                'D2_NATIVE_CONTROLLER_IDENTITY')
    repo = _d2_path(ticket['repo_root'], True)
    _d2_require(repo == Path(__file__).absolute().parents[1]
                and _d2_sources(repo) == ticket['source_files_sha256']
                and stat.S_IMODE(root.stat().st_mode) == 0o700,
                'D2_NATIVE_CONTROLLER_SOURCE')
    claim = os.open(root / 'controller.claim', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(claim)
    install_process_guard()
    _d2_controller = dict(ticket, controller_pid=os.getpid(), controller_start_ticks=_d2_ticks(os.getpid()))
    return dict(_d2_controller)


def issue_d2_profile(repo, scratch, case, vendor_root):
    global _d2_profile
    _d2_require(_d2_controller is not None and _d2_profile is None
                and case in D2_CASES and case == _d2_controller['case'], 'D2_NATIVE_PROFILE_REFUSED')
    repo, scratch, vendor = (_d2_path(repo, True), _d2_path(scratch, True), _d2_path(vendor_root, True))
    owner = _d2_path(_d2_controller['scratch'], True)
    _d2_require(repo == Path(_d2_controller['repo_root']) and scratch.is_relative_to(owner)
                and vendor.is_relative_to(owner), 'D2_NATIVE_PROFILE_ROOT')
    names = {path.name for path in vendor.iterdir()}
    _d2_require(names == {'pygsheets.py', 'ftplib.py', 'scenario.json'}, 'D2_NATIVE_VENDOR_FILES')
    files = {name: hashlib.sha256(_d2_path(vendor / name).read_bytes()).hexdigest() for name in sorted(names)}
    _d2_profile = dict(schema_version='d2-test-profile-v1', case=case, repo_root=str(repo),
        scratch=str(scratch), vendor_root=str(vendor), vendor_files_sha256=files,
        source_files_sha256=_d2_sources(repo), controller_pid=os.getpid(),
        controller_start_ticks=_d2_ticks(os.getpid()), nonce=_d2_controller['nonce'], deadline_seconds=15)
    return dict(_d2_profile)


def _d2_profile_identity(profile, worker=False):
    keys = {'schema_version', 'case', 'repo_root', 'scratch', 'vendor_root', 'vendor_files_sha256',
            'source_files_sha256', 'controller_pid', 'controller_start_ticks', 'nonce', 'deadline_seconds'}
    _d2_require(type(profile) is dict and set(profile) == keys
                and profile['schema_version'] == 'd2-test-profile-v1'
                and profile['case'] in D2_CASES and profile['deadline_seconds'] == 15
                and type(profile['controller_pid']) is int
                and profile['controller_pid'] == (os.getppid() if worker else os.getpid())
                and profile['controller_start_ticks'] == _d2_ticks(profile['controller_pid'])
                and re.fullmatch('[0-9a-f]{32}', profile['nonce']), 'D2_NATIVE_PROFILE_IDENTITY')
    repo = _d2_path(profile['repo_root'], True)
    scratch, vendor = _d2_path(profile['scratch'], True), _d2_path(profile['vendor_root'], True)
    _d2_require(repo == Path(__file__).absolute().parents[1]
                and _d2_sources(repo) == profile['source_files_sha256']
                and vendor.is_relative_to(scratch.parent)
                and profile['vendor_files_sha256']
                and set(profile['vendor_files_sha256']) == {'pygsheets.py', 'ftplib.py', 'scenario.json'},
                'D2_NATIVE_PROFILE_SOURCE')
    for name, digest in profile['vendor_files_sha256'].items():
        _d2_require(hashlib.sha256(_d2_path(vendor / name).read_bytes()).hexdigest() == digest,
                    'D2_NATIVE_VENDOR_CHANGED')


def verify_d2_profile(profile):
    try:
        _d2_require(_d2_controller is not None and profile == _d2_profile,
                    'D2_NATIVE_PROFILE_UNISSUED')
        _d2_profile_identity(profile)
        return True
    except BaseException:
        return False


@contextmanager
def d2_launch_scope(command, inherited, ticket):
    """Permit only the fixed production entry and issued controller profile."""
    value = _d2_read(ticket, 65536)
    _d2_require(verify_d2_profile(value.get('profile'))
                and value.get('schema') == 'd2-worker-ticket-v1'
                and value.get('role') in {'google', 'ftp', 'tracker'}
                and value.get('parent_pid') == os.getpid()
                and value.get('parent_start_ticks') == _d2_ticks(os.getpid()),
                'D2_NATIVE_LAUNCH_TICKET')
    repo = _d2_path(value['repo_root'], True)
    _d2_require(repo == Path(value['profile']['repo_root'])
                and _d2_sources(repo) == value['profile']['source_files_sha256']
                and value.get('source_files_sha256') == {
                    name: hashlib.sha256(_d2_path(repo / name).read_bytes()).hexdigest()
                    for name in ('upload.py', 'run_logging.py')},
                'D2_NATIVE_LAUNCH_SOURCE')
    run_root = _d2_path(value['run_root'], True)
    _d2_require(Path(ticket).absolute() == run_root / (value['role'] + '.ticket.json')
                and run_root.is_relative_to(Path(value['profile']['scratch']))
                and str(Path(ticket).absolute()) not in _d2_worker_launches,
                'D2_NATIVE_LAUNCH_REPLAY_OR_ROOT')
    expected = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
                str(repo / 'upload.py'), '--d2-worker', str(Path(ticket).absolute()))
    _d2_require(tuple(command) == expected and set(inherited) == set(value['expected_fds'])
                and len(set(inherited)) == len(inherited), 'D2_NATIVE_LAUNCH_ARGUMENTS')
    _admit_descriptors(set(value['expected_fds']), set(inherited))
    guard = install_process_guard()
    _d2_require(guard.command is None, 'D2_NATIVE_LAUNCH_BUSY')
    _d2_worker_launches.add(str(Path(ticket).absolute()))
    guard.command, guard.owner = expected, threading.get_ident()
    try:
        yield
    finally:
        guard.command, guard.owner = None, None


def d2_worker_admission(ticket):
    """A test worker receives no process/network permission after bootstrap."""
    _d2_require(type(ticket) is dict and ticket.get('schema') == 'd2-worker-ticket-v1'
                and ticket.get('role') in {'google', 'ftp', 'tracker'}, 'D2_NATIVE_WORKER_TICKET')
    _d2_profile_identity(ticket.get('profile'), worker=True)
    _d2_require(ticket['parent_pid'] == os.getppid()
                and ticket['parent_start_ticks'] == _d2_ticks(os.getppid()), 'D2_NATIVE_WORKER_PARENT')
    install_process_guard()
    return True


def _d2_launch_fixed(repo, ticket, environment, *, owner=None):
    guard = install_process_guard()
    command = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
               str(repo / 'tests/d2/d2_worker.py'), '--ticket', str(ticket))
    _d2_require(guard.command is None, 'D2_NATIVE_LAUNCH_BUSY')
    guard.command, guard.owner = command, threading.get_ident()
    process = descriptor = None
    external_owner = owner is not None
    if owner is None:
        owner = {}
    owner.update(process=None, pidfd=None, identity=None, birth_attempted=False,
                 birth_unknown=False, pidfd_bound=False)
    try:
        owner['birth_attempted'] = True
        process = subprocess.Popen(command, executable=command[0], cwd=repo, env=environment,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            close_fds=True, start_new_session=True)
        owner['process'] = process
        descriptor = os.pidfd_open(process.pid, 0)
        owner.update(pidfd=descriptor, pidfd_bound=True)
        owner['identity'] = _process_identity(process.pid)
        return owner
    except BaseException:
        owner['birth_unknown'] = owner['birth_attempted'] and process is None
        if process is not None:
            try:
                process.kill()
                process.wait(timeout=10)
            except BaseException:
                pass
        if descriptor is not None and not external_owner:
            os.close(descriptor)
            owner['pidfd'] = None
        raise
    finally:
        guard.command, guard.owner = None, None


def run_d2_case(case, repo_root, scratch):
    _d2_require(_d2_runner is not None and _d2_runner['pid'] == os.getpid()
                and _d2_runner['start_ticks'] == _d2_ticks(os.getpid()), 'D2_NATIVE_RUNNER_UNISSUED')
    _d2_require(_d2_case_lock.acquire(blocking=False), 'D2_NATIVE_CONCURRENT_CASE')
    try:
        return _run_d2_case(case, repo_root, scratch)
    finally:
        _d2_case_lock.release()


def _run_d2_case(case, repo_root, scratch):
    """One fixed controller lifetime per case; its OS result is never invented."""
    _d2_require(case in D2_CASES + D2_BASELINE_CASES, 'D2_NATIVE_CASE_INVALID')
    attest_runtime()
    repo, scratch = _d2_path(repo_root, True), _d2_path(scratch, True)
    native = _d2_path(_d2_runner['native_root'], True)
    _d2_require(scratch.is_relative_to(native) and str(repo) == _d2_runner['repo']
                and _d2_sources(repo) == _d2_runner['source_files_sha256'],
                'D2_NATIVE_CASE_ROOT')
    sha = os.environ.get('MDS_D2_SOURCE_SHA', '')
    _d2_require(sha == _d2_runner['source_sha'] and re.fullmatch('[0-9a-f]{40}', sha),
                'D2_NATIVE_SOURCE_IDENTITY')
    work = scratch / ('controller-' + uuid.uuid4().hex)
    work.mkdir(mode=0o700)
    path = work / 'controller.ticket.json'
    value = dict(schema_version='d2-controller-ticket-v1', role='controller', case=case,
        repo_root=str(repo), scratch=str(work), source_sha=sha, parent_pid=os.getpid(),
        nonce=uuid.uuid4().hex, source_files_sha256=_d2_sources(repo), result=str(work / 'controller-result.json'))
    _ticket(path, value)
    environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
        'MDS_D2_CONTROLLER_ROOT': str(work), 'MDS_D2_CONTROLLER_TICKET_SHA256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'MDS_D2_SOURCE_SHA': sha}
    result = dict(schema_version='d2-native-case-v1', case=case,
        controller=dict(started=False, pid=None, pidfd_bound=False, exitcode=None,
                        settled=False, timed_out=False, birth_attempted=False, birth_unknown=False), observation=None)
    record = {}
    try:
        _d2_launch_fixed(repo, path, environment, owner=record)
        process = record['process']
        result['controller'].update(started=True, pid=process.pid, pidfd_bound=True)
        try:
            process.wait(timeout=D2_CONTROLLER_SECONDS)
        except subprocess.TimeoutExpired:
            result['controller']['timed_out'] = True
            _settle([record], timeout=10)
        result['controller'].update(exitcode=process.poll(),
            settled=process.returncode is not None and bool(select.select([record['pidfd']], [], [], 0)[0]))
        if result['controller']['settled'] and Path(value['result']).exists():
            public = Path(value['result']).read_bytes()
            _d2_require(len(public) <= 1024 * 1024 and b'D2_PRIVATE_CANARY_4c1e11' not in public
                and re.search(rb'(?i)(https?://|file://|Traceback [(]most recent|[A-Z]:[\\/])', public) is None
                and re.search(rb'(?:"|[\s])/[-A-Za-z0-9_.]', public) is None, 'D2_NATIVE_EXPORT_REFUSED')
            result['observation'] = _d2_read(value['result'])
    except BaseException:
        # Preserve actual birth observations even if binding/registration did
        # not finish. No no-birth proof follows from a constructor exception.
        pass
    finally:
        process = record.get('process')
        result['controller'].update(birth_attempted=record.get('birth_attempted', False),
                                    birth_unknown=record.get('birth_unknown', False))
        if record.get('birth_unknown'):
            result['controller']['started'] = None
        if process is not None:
            if process.poll() is None:
                _settle([record], timeout=10)
            result['controller'].update(started=True, pid=process.pid,
                pidfd_bound=record['pidfd_bound'], exitcode=process.poll(),
                settled=process.returncode is not None and (record['pidfd'] is None
                    or bool(select.select([record['pidfd']], [], [], 0)[0])))
        if record.get('pidfd') is not None:
            os.close(record['pidfd'])
            record['pidfd'] = None
    return result


def issue_d2_legacy_ticket(repo, scratch, case, role, original_sha256):
    _d2_require(_d2_controller is not None and case == _d2_controller['case']
                and case in D2_BASELINE_CASES and role in {'baseline-google', 'baseline-ftp'},
                'D2_NATIVE_BASELINE_REFUSED')
    repo, scratch = _d2_path(repo, True), _d2_path(scratch, True)
    _d2_require(repo == Path(_d2_controller['repo_root'])
                and scratch.is_relative_to(Path(_d2_controller['scratch']))
                and re.fullmatch('[0-9a-f]{64}', original_sha256), 'D2_NATIVE_BASELINE_SOURCE')
    fixture = json.loads((repo / 'tests/fixtures/d2/expected.json').read_bytes())
    _d2_require(original_sha256 == fixture.get('baseline_upload_sha256')
                == '95945e89d8bc766f934132270a8ee5df75d5382cad86ff44771eb352d5e5acb1'
                and fixture.get('baseline_upload_git_blob') == 'dc1f12bc63eadebe589a666ffcb6a0a2bb908747'
                and fixture.get('baseline_upload_bytes') == 7210, 'D2_NATIVE_BASELINE_SOURCE')
    path = scratch / (role + '.ticket.json')
    value = dict(schema_version='d2-baseline-ticket-v1', role=role, case=case,
        repo_root=str(repo), scratch=str(scratch), parent_pid=os.getpid(),
        parent_start_ticks=_d2_ticks(os.getpid()), original_sha256=original_sha256,
        controller_root=_d2_controller['scratch'], source_files_sha256=_d2_sources(repo))
    _ticket(path, value)
    _d2_legacy_issued[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return path


def launch_d2_legacy(ticket):
    value = _d2_read(ticket, 65536)
    _d2_require(_d2_controller is not None and value['case'] == _d2_controller['case']
                and value['parent_pid'] == os.getpid()
                and value['parent_start_ticks'] == _d2_ticks(os.getpid()), 'D2_NATIVE_BASELINE_PARENT')
    expected = _d2_legacy_issued.pop(str(Path(ticket)), None)
    _d2_require(expected is not None and expected == hashlib.sha256(Path(ticket).read_bytes()).hexdigest()
                and value['schema_version'] == 'd2-baseline-ticket-v1'
                and value['role'] in {'baseline-google', 'baseline-ftp'}
                and value['controller_root'] == _d2_controller['scratch']
                and value['source_files_sha256'] == _d2_sources(Path(value['repo_root'])),
                'D2_NATIVE_BASELINE_UNISSUED')
    root = Path(_d2_controller['scratch'])
    environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
        'MDS_D2_CONTROLLER_ROOT': str(root),
        'MDS_D2_CONTROLLER_TICKET_SHA256': os.environ['MDS_D2_CONTROLLER_TICKET_SHA256'],
        'MDS_D2_SOURCE_SHA': os.environ['MDS_D2_SOURCE_SHA']}
    return _d2_launch_fixed(Path(value['repo_root']), Path(ticket), environment)


def settle_d2_legacy(records):
    _settle(records, timeout=10)
    complete = True
    for record in records:
        if record.get('pidfd') is None:
            if record['process'].returncode is None or not record.get('pidfd_exit_observed'):
                complete = False
            continue
        if record['process'].returncode is None or not select.select([record['pidfd']], [], [], 0)[0]:
            complete = False
        elif record.get('pidfd') is not None:
            record['pidfd_exit_observed'] = True
            os.close(record['pidfd'])
            record['pidfd'] = None
    return complete
