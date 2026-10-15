"""Fixed native LNX-03 fixtures. No arbitrary module/command admission."""

import json
import os
from pathlib import Path
import pickle
import stat
import sys


def _read_ticket():
    if len(sys.argv) != 3 or sys.argv[1] != '--ticket':
        raise ValueError('LNX03_NATIVE_ARGUMENTS')
    path = Path(os.path.abspath(sys.argv[2]))
    repo = Path(__file__).resolve().parents[2]
    root = Path(os.path.abspath(os.environ.get('LNX03_NATIVE_ROOT', '/lnx03-undeclared')))
    root_info = root.lstat()
    if (path.resolve() != path or root.resolve() != root or not path.is_relative_to(root)
            or not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid()
            or root_info.st_mode & 0o077):
        raise ValueError('LNX03_NATIVE_TICKET_PATH')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 65536):
            raise ValueError('LNX03_NATIVE_TICKET_FILE')
        value = json.loads(handle.read(65537))
    common = {'schema', 'role', 'case', 'tracker_fd', 'repo', 'parent_pid', 'expected_fds'}
    workers = {'bootstrap_fd', 'witness_fd', 'foreign_fd', 'foreign_identity'}
    if (not isinstance(value, dict) or value.get('schema') != 'lnx03-native-ticket-v1'
            or value.get('role') not in {'google', 'ftp', 'tracker'}
            or set(value) != (common if value['role'] == 'tracker' else common | workers)
            or value.get('repo') != str(repo)):
        raise ValueError('LNX03_NATIVE_TICKET_SCHEMA')
    for name in ('tracker_fd',) if value['role'] == 'tracker' else ('tracker_fd', 'bootstrap_fd', 'witness_fd', 'foreign_fd'):
        if type(value[name]) is not int or value[name] < 3:
            raise ValueError('LNX03_NATIVE_TICKET_FD')
    if (type(value['parent_pid']) is not int or value['parent_pid'] <= 1
            or type(value['expected_fds']) is not list
            or any(type(item) is not int or item < 3 for item in value['expected_fds'])
            or sorted(set(value['expected_fds'])) != value['expected_fds']):
        raise ValueError('LNX03_NATIVE_TICKET_PROCESS')
    claim = path.with_suffix('.claim')
    claim_descriptor = os.open(claim, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(claim_descriptor)
    # -I -S intentionally omits the checkout and site-packages. Only the fixed
    # checkout is added here; pinned spawn preparation restores parent sys.path.
    sys.path.insert(0, str(repo))
    value['_ticket_path'] = str(path)
    return value


def _event(ticket, event):
    os.write(ticket['witness_fd'], (event + '\n').encode('ascii'))


def _entry(ticket):
    # Install Linux parent-death containment before importing project modules or
    # unpickling. Recheck PPID to close the parent-death-before-prctl race.
    import ctypes
    import signal
    library = ctypes.CDLL(None, use_errno=True)
    library.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                             ctypes.c_ulong, ctypes.c_ulong)
    library.prctl.restype = ctypes.c_int
    if library.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != ticket['parent_pid']:
        return 1
    from tools import offline_guard_linux as guard
    if ticket['case'] not in guard.NATIVE_CASES:
        raise ValueError('LNX03_NATIVE_CASE')
    guard.attest_runtime()
    guard.install_process_guard()
    if ticket['role'] == 'tracker':
        from multiprocessing.resource_tracker import main
        main(ticket['tracker_fd'])
        return 0
    _event(ticket, 'ENTRY')
    _event(ticket, 'PDEATH_BOUND')
    if os.getsid(0) != os.getpid() or os.getpgrp() != os.getpid():
        raise ValueError('LNX03_NATIVE_SESSION')
    _event(ticket, 'SESSION_OWNED')
    null = os.stat('/dev/null')
    for descriptor in (0, 1, 2):
        info = os.fstat(descriptor)
        if not stat.S_ISCHR(info.st_mode) or info.st_rdev != null.st_rdev:
            raise ValueError('LNX03_NATIVE_STDIO')
    _event(ticket, 'STDIO_BOUND')
    # Both writes occur before wrapper/import/unpickle fault boundaries. Native
    # exec redirection, not Python flags or an exception wrapper, contains them.
    os.write(1, guard.CANARY)
    os.write(2, guard.CANARY)
    try:
        identity = list(guard._fd_identity(ticket['foreign_fd']))
    except OSError:
        identity = None
    if identity == ticket['foreign_identity']:
        raise ValueError('LNX03_NATIVE_FOREIGN_FD_INHERITED')
    _event(ticket, 'FOREIGN_FD_ABSENT')
    observed = []
    for item in os.listdir('/proc/self/fd'):
        descriptor = int(item)
        if descriptor < 3:
            continue
        try:
            os.fstat(descriptor)
        except OSError:
            continue  # os.listdir's transient directory fd has already closed.
        observed.append(descriptor)
    if sorted(observed) != ticket['expected_fds']:
        raise ValueError('LNX03_NATIVE_FD_INVENTORY')
    # Challenge the installed hook, rather than a synthetic guard object. A
    # kernel DNS error is not an audit refusal. If the child probe is ever
    # allowed to be born, reap it within a fixed budget and fail this fixture.
    import socket
    import subprocess
    dns_denied = False
    try:
        socket.getaddrinfo('lnx03-audit.invalid', 443)
    except guard.NativeAdmissionError:
        dns_denied = True
    if not dns_denied:
        raise ValueError('LNX03_NATIVE_DNS_PROBE_NOT_DENIED')
    process_denied = False
    try:
        unexpected = subprocess.Popen(['/bin/true'], executable='/bin/true',
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            close_fds=True)
    except guard.NativeAdmissionError:
        process_denied = True
    else:
        try:
            unexpected.kill()
        except ProcessLookupError:
            pass
        unexpected.wait(timeout=1)
    # Policy-only challenge of the installed CPython 3.14 low-level event. This
    # dispatch does not create a process; the real Popen probe remains separate.
    low_level_denied = False
    try:
        sys.audit('_posixsubprocess.fork_exec', (b'/bin/true',), ['/bin/true'], [])
    except guard.NativeAdmissionError:
        low_level_denied = True
    if (not process_denied or not low_level_denied or guard.install_process_guard().denials != {
            'socket.getaddrinfo': 1, 'subprocess.Popen': 1, '_posixsubprocess.fork_exec': 1}):
        raise ValueError('LNX03_NATIVE_PROCESS_PROBE_NOT_DENIED')
    _event(ticket, 'GUARD_DENIALS_CONFIRMED')
    injected = ticket['role'] == 'google'
    if injected and ticket['case'] == 'bootstrap_import_failure':
        _event(ticket, 'BOOTSTRAP_IMPORT_BOUNDARY')
        __import__('lnx03_missing_' + guard.CANARY.decode('ascii'))
    from multiprocessing import resource_tracker, spawn
    tracker = resource_tracker._resource_tracker
    if tracker._fd is not None or tracker._pid is not None:
        raise ValueError('LNX03_NATIVE_TRACKER_CHANGED')
    tracker._fd = ticket['tracker_fd']
    with os.fdopen(ticket['bootstrap_fd'], 'rb') as handle:
        _event(ticket, 'PREPARATION_BOUNDARY')
        preparation = pickle.load(handle)
        if injected and ticket['case'] == 'bootstrap_replay_failure':
            preparation['dir'] = '/lnx03-intentionally-absent/' + guard.CANARY.decode('ascii')
        spawn.prepare(preparation)
        _event(ticket, 'PREPARATION_OK')
        _event(ticket, 'UNPICKLE_BOUNDARY')
        bootstrap = pickle.load(handle)
        _event(ticket, 'UNPICKLE_OK')
    _event(ticket, 'WORKER_WRAPPER')
    import run_logging
    context = run_logging.configure_worker(bootstrap, sensitive_values=(guard.CANARY.decode('ascii'),))
    code = 0
    try:
        run_logging.emit_event(context, 'INFO', 'fixture_started', 'Fixture started', {'outcome': 'UNKNOWN'})
        if ticket['case'] == 'hang':
            _event(ticket, 'HANG')
            import time
            while True:
                time.sleep(1)
        if injected and ticket['case'] == 'exit23':
            _event(ticket, 'EXIT23')
            os._exit(23)
        if injected and ticket['case'] == 'child_error':
            _event(ticket, 'WORKER_ERROR')
            raise RuntimeError(guard.CANARY.decode('ascii'))
        if injected and ticket['case'] == 'canary_positive_control':
            # Reach the same gate used by the real runner before result create.
            # Only its exact refusal authorizes this closed witness; merely
            # reading a known literal does not prove export refusal.
            from tools import offline_linux as runner
            path = Path(ticket['_ticket_path']).parent / ('canary-' + str(os.getpid()))
            with path.open('xb') as handle:
                handle.write(guard.CANARY)
            try:
                try:
                    runner.admit_export_bytes(path.read_bytes())
                except runner.Refusal as refusal:
                    if str(refusal) != 'lnx03-runner-export-refused':
                        raise ValueError('LNX03_NATIVE_WRONG_EXPORT_REFUSAL') from None
                    _event(ticket, 'CANARY_REJECTED')
                    code = 1
                else:
                    raise ValueError('LNX03_NATIVE_EXPORT_REFUSAL_MISSING')
            finally:
                path.unlink()
    except BaseException:
        code = 1
    final = run_logging.finish_worker(context, execution_outcome='COMPLETED' if code == 0 else 'FAILED', exit_status=code)
    _event(ticket, 'FINAL_CONFIRMED' if final['confirmed'] else 'FINAL_UNCONFIRMED')
    return code if final['confirmed'] else 1


def main():
    # All bootstrap diagnostics are already /dev/null at native exec. Raw
    # exceptions intentionally do not enter any result or console channel.
    try:
        return _entry(_read_ticket())
    except SystemExit as error:
        return error.code if type(error.code) is int and 0 <= error.code <= 255 else 1
    except BaseException:
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
