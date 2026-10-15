"""Closed LNX-03 fixture admission. This is not a production uploader adapter.

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


def _ticket(path, value):
    data = _json_bytes(value)
    if len(data) > 65536:
        raise NativeAdmissionError('LNX03_NATIVE_TICKET_BOUND')
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(data)


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


def _launch(repo, ticket, descriptors):
    guard = install_process_guard()
    command = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
               str(repo / 'tests/linux_bootstrap/native_worker.py'), '--ticket', str(ticket))
    if guard.command is not None:
        raise NativeAdmissionError('LNX03_NATIVE_LAUNCH_BUSY')
    guard.command, guard.owner = command, threading.get_ident()
    try:
        child = subprocess.Popen(command, executable=command[0], cwd=repo,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
                 'LNX03_NATIVE_ROOT': str(_native_root(repo))},
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            close_fds=True, pass_fds=tuple(sorted(descriptors)), start_new_session=True)
    finally:
        guard.command, guard.owner = None, None
    # Binding failure is never permission to abandon a process already born.
    descriptor = None
    try:
        descriptor = os.pidfd_open(child.pid, 0)
        identity = _process_identity(child.pid)
    except BaseException:
        try:
            child.kill()
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            raise NativeAdmissionError('LNX03_NATIVE_BIRTH_UNSETTLED') from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
        raise NativeAdmissionError('LNX03_NATIVE_PIDFD_BIND_FAILED') from None
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
        records.append(dict(pid=item['process'].pid, **item['identity'],
                            role=item.get('role', 'tracker'), pidfd_bound=True))
    path = work / 'pid-registry.json'
    temporary = work / 'pid-registry.pending'
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(_json_bytes(dict(schema='lnx03-pid-registry-v1', processes=records)))
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
                signal.pidfd_send_signal(item['pidfd'], signal.SIGTERM)
            except ProcessLookupError:
                pass
    term_deadline = min(deadline, time.monotonic() + 5)
    while time.monotonic() < term_deadline and any(item['process'].poll() is None for item in children):
        time.sleep(0.02)
    for item in children:
        if item['process'].poll() is None:
            try:
                signal.pidfd_send_signal(item['pidfd'], signal.SIGKILL)
            except ProcessLookupError:
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
    tracker = None
    transport_settled = True
    tracker_read, tracker_write = os.pipe()
    foreign = os.open(work / 'foreign-marker', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    os.set_inheritable(foreign, True)
    foreign_identity = list(_fd_identity(foreign))
    result = dict(schema='lnx03-native-case-v1', case=case, status='INCOMPLETE',
        failure_code='LNX03_NATIVE_INCOMPLETE', application_exit=1,
        deadline_seconds=DEADLINE_SECONDS, deadline_detected=False, cleanup_requested=False,
        all_workers_settled=False, two_workers_registered=False, worker_records=[],
        logging=dict(delivery_outcome='INCOMPLETE', storage_sealed=False, exit_status=1, worker_final_count=0),
        auxiliary=dict(owned=False, pid=None, pidfd_bound=False, settled=False, reaped=False, exitcode=None),
        source_attestation=attestation, console_canary_absent=False)
    try:
        tracker_ticket = work / 'tracker.json'
        _ticket(tracker_ticket, dict(schema='lnx03-native-ticket-v1', role='tracker',
            case=case, tracker_fd=tracker_read, repo=str(repo), parent_pid=os.getpid(),
            expected_fds=[tracker_read]))
        process, descriptor, identity = _launch(repo, tracker_ticket, {tracker_read})
        tracker = dict(process=process, pidfd=descriptor, identity=identity)
        result['auxiliary'].update(owned=True, pid=process.pid, pidfd_bound=True)
        _registry(work, children, tracker)
        os.close(tracker_read)
        tracker_read = None
        # The pinned resource tracker checks this existing pipe. Its _pid stays
        # None: our Popen owns this tracker and is the only allowed reaper.
        tracker_state._fd = tracker_write
        session = run_logging.configure_logging(work, run_id='lnx03-' + uuid.uuid4().hex,
            environment='offline', producer_sha=os.environ['LNX03_SOURCE_SHA'],
            config={'log_root': '.f-log/lnx03', 'retained_runs': 1, 'critical_deadline_s': 0.5,
                    'shutdown_deadline_s': 2.0}, sensitive_values=(CANARY.decode('ascii'),))
        bootstraps = [run_logging.prepare_worker(session, role) for role in ('google', 'ftp')]
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
            preparation, payload, descriptors = _serialize(bootstraps[index], repo)
            read_end, write_end = os.pipe()
            witness_read, witness_write = os.pipe()
            ticket = work / (role + '.json')
            inherited = descriptors | {read_end, witness_write, tracker_write}
            admission_events = []
            if case == 'foreign_fd' and index == 0:
                try:
                    _admit_descriptors(inherited, inherited | {foreign})
                except NativeAdmissionError:
                    admission_events.append('FOREIGN_FD_REFUSED')
                else:
                    raise NativeAdmissionError('LNX03_NATIVE_FOREIGN_FD_ACCEPTED')
            _admit_descriptors(inherited, inherited)
            _ticket(ticket, dict(schema='lnx03-native-ticket-v1', role=role, case=case,
                tracker_fd=tracker_write, bootstrap_fd=read_end, witness_fd=witness_write,
                foreign_fd=foreign, foreign_identity=foreign_identity, repo=str(repo),
                parent_pid=os.getpid(), expected_fds=sorted(inherited)))
            try:
                process, descriptor, identity = _launch(repo, ticket, inherited)
            finally:
                os.close(read_end)
                os.close(witness_write)
            item = dict(role=role, process=process, pidfd=descriptor,
                        witness=witness_read, parent_events=admission_events, session_owned=False, identity=identity)
            children.append(item)
            _registry(work, children, tracker)
            try:
                item['session_owned'] = os.getsid(process.pid) == process.pid
            except ProcessLookupError:
                pass
            try:
                if case == 'bootstrap_pipe_failure' and index == 0:
                    # A real write failure after native birth, before bootstrap
                    # transfer. No other file operation can reuse this fd
                    # between the intentional close and the failing write.
                    refused = write_end
                    os.close(write_end)
                    write_end = None
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
                if write_end is not None:
                    os.close(write_end)
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
                _settle(children)
                break
            # Fixed fixture peers are allowed to finish; a failed sibling never
            # makes them successful. A hanging peer is still bounded by this
            # same parent deadline. This avoids inventing pre-entry witnesses.
            time.sleep(0.02)
        for item in children:
            process = item['process']
            settled = process.returncode is not None
            events = item['parent_events'] + (_read_events(item['witness']) if settled else [])
            result['worker_records'].append(dict(role=item['role'], pid=process.pid,
                pidfd_bound=True, session_owned=item['session_owned'] or 'SESSION_OWNED' in events, exitcode=process.returncode,
                reaped=settled, events=events, foreign_fd_absent='FOREIGN_FD_ABSENT' in events,
                stdio_redirected='STDIO_BOUND' in events))
            run_logging.record_worker_outcome(session, item['role'], started=True,
                exit_status=process.returncode, timed_out=result['deadline_detected'] and process.returncode != 0)
        result['all_workers_settled'] = all(item['process'].returncode is not None for item in children)
        actual_success = (result['all_workers_settled'] and not result['deadline_detected']
                          and all(item['process'].returncode == 0 for item in children))
        report = run_logging.finalize_logging(session,
            execution_outcome='COMPLETED' if actual_success else 'FAILED', exit_status=0 if actual_success else 1)
        result['logging'] = dict(delivery_outcome=report['delivery_outcome'],
            storage_sealed=report['storage_sealed'], exit_status=report['exit_status'],
            worker_final_count=len(session['_worker_finals']))
        result['application_exit'] = 0 if (actual_success and report['delivery_outcome'] == 'COMPLETE'
                                          and report['storage_sealed']) else 1
        result['console_canary_absent'] = all(record['stdio_redirected'] for record in result['worker_records'])
    except BaseException:
        # Fault details, raw streams, exception strings and tracebacks never
        # cross the result boundary. Missing evidence remains INCOMPLETE.
        result['failure_code'] = 'LNX03_NATIVE_EXECUTION_INCOMPLETE'
    finally:
        if any(item['process'].poll() is None for item in children):
            result['cleanup_requested'] = True
            _settle(children)
        if session is not None and session.get('_final') is None:
            try:
                for role in session['_expected']:
                    run_logging.record_worker_outcome(session, role, started=False, exit_status=None)
                run_logging.finalize_logging(session, execution_outcome='FAILED', exit_status=1)
            except BaseException:
                pass
        for item in children:
            os.close(item['witness'])
            os.close(item['pidfd'])
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
                transport_settled = ((feeder is None or not feeder.is_alive())
                                     and not session['_thread'].is_alive())
            except BaseException:
                transport_settled = False
            finally:
                feeder = None
        if transport_settled:
            bootstraps.clear()
            session = None
            gc.collect()  # SemLock finalizers must send UNREGISTER before EOF.
            if tracker_state._fd == tracker_write:
                tracker_state._fd = None
            os.close(tracker_write)
        else:
            # No synthetic settlement, premature tracker EOF, or later-case
            # recovery. Preserve live references and the registered tracker for
            # fail-closed runner/controller teardown and its retained scratch.
            _unsettled_transports.append(dict(session=session, bootstraps=bootstraps,
                                               tracker=tracker, tracker_write=tracker_write))
            result['application_exit'] = 1
            result['cleanup_requested'] = True
        if tracker_read is not None:
            os.close(tracker_read)
        os.close(foreign)
        if tracker is not None and transport_settled:
            try:
                tracker['process'].wait(timeout=5)
            except subprocess.TimeoutExpired:
                _settle([tracker], timeout=5)
            result['auxiliary'].update(settled=tracker['process'].returncode is not None,
                                       reaped=tracker['process'].returncode is not None,
                                       exitcode=tracker['process'].returncode)
            os.close(tracker['pidfd'])
    if not transport_settled:
        result['status'] = 'INCOMPLETE'
        result['failure_code'] = 'LNX03_NATIVE_TRANSPORT_UNSETTLED'
        return result
    passed = _case_passed(case, result)
    result['status'] = 'PASS' if passed else 'FAIL'
    result['failure_code'] = None if passed else 'LNX03_NATIVE_ORACLE_FAILED'
    return result


def _fd_identity(descriptor):
    info = os.fstat(descriptor)
    return info.st_dev, info.st_ino, info.st_mode, info.st_rdev
