"""Offline F-LOG fixtures. Every fresh interpreter installs guards first."""

import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import runpy
import socket
import stat
import subprocess
import sys
import types

ROOT = Path(__file__).resolve().parents[2]
_guard_counts = None


def offline_guard():
    """Use the same stdlib adapter in the harness and fresh native children."""
    name = 'mds_offline_guard'
    if name not in sys.modules:
        specification = importlib.util.spec_from_file_location(name, ROOT / 'tools/offline_guard.py')
        module = importlib.util.module_from_spec(specification)
        sys.modules[name] = module
        specification.loader.exec_module(module)
    return sys.modules[name]


def safe_tree(path):
    boundary = ROOT / '.f-log'
    path = Path(os.path.abspath(path))
    if not path.is_relative_to(boundary) or boundary.resolve() != boundary:
        raise ValueError('FLOG_OUTPUT_UNSAFE')
    current = boundary
    for part in ('', *path.relative_to(boundary).parts):
        if part:
            current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT or stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise ValueError('FLOG_OUTPUT_ALIAS')
    if path.is_dir():
        for directory, dirs, files in os.walk(path, followlinks=False):
            for name in dirs + files:
                info = (Path(directory) / name).lstat()
                if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT or stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                    raise ValueError('FLOG_OUTPUT_ALIAS')
    return path


def install_guards():
    global _guard_counts
    if _guard_counts is not None:
        return _guard_counts
    boundary = ROOT / '.f-log'
    counts = dict(network=0, writers=0, secrets=0, children=0)
    guard = offline_guard()
    guard.attach_native(ROOT, 'flog', 'flog-probe', counts)
    active_launch = 0

    def reject(kind):
        counts[kind] += 1
        guard.native_notice(kind)
        raise RuntimeError('FLOG_OFFLINE_' + kind.upper())

    def owned(value):
        try:
            path = Path(os.path.abspath(value))
            if not path.is_relative_to(boundary) or boundary.resolve() != boundary:
                return False
            if (path.is_relative_to(boundary / 'evidence')
                    and not path.is_relative_to(guard.report_root(ROOT, 'flog'))):
                return False
            current = boundary
            for part in ('', *path.relative_to(boundary).parts):
                if part:
                    current = current / part
                try:
                    info = current.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT or stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                    return False
            return True
        except (TypeError, ValueError):
            return False

    def audit(event, args):
        if guard.internal_report_io(event, args):
            return
        if event in {'socket.connect', 'socket.bind', 'socket.getaddrinfo',
                     'socket.gethostbyname', 'socket.gethostbyaddr', 'socket.sendto'}:
            reject('network')
        if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
            path = Path(os.fsdecode(args[0]))
            actual = path.resolve()
            if 'secrets' in [p.lower() for p in actual.parts] or actual.name == '.env':
                reject('secrets')
            writing = args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
            if 'datasets' in path.parts:
                reject('writers')
            if writing and not owned(path) and os.path.normcase(str(path.absolute())) != os.path.normcase(os.path.abspath(os.devnull)):
                reject('writers')
        if event in {'os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.link', 'os.symlink', 'os.chmod'}:
            paths = args[:2] if event in {'os.rename', 'os.link', 'os.symlink'} else args[:1]
            # Pytest creates numbered fixture-directory `test_*current` links.
            # Unlinking this final component does not traverse its target.
            if event == 'os.remove':
                path = Path(os.path.abspath(args[0]))
                if (path.is_relative_to(boundary / 'tmp') and path.name.startswith('test_')
                        and path.name.endswith('current') and owned(path.parent)):
                    info = path.lstat()
                    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                        return
            if not all(owned(p) for p in paths):
                reject('writers')
        if event in {'os.system', 'os.exec', 'os.spawn', 'os.posix_spawn', 'os.startfile'}:
            reject('children')

    original_popen = subprocess.Popen.__init__

    def guarded_popen(self, argv, *args, **kwargs):
        nonlocal active_launch
        if args or kwargs.get('shell') or not isinstance(argv, (list, tuple)):
            reject('children')
        command = [os.fsdecode(p) for p in argv]
        if (len(command) != 7 or os.path.normcase(command[0]) != os.path.normcase(sys.executable)
                or command[1:6] != ['-I', '-B', '-X', 'utf8', str(Path(__file__).resolve())]
                or command[6] not in {'error', 'normal'}):
            reject('children')
        actual_argv, actual_env, ticket = guard.notify_child(
            argv, cwd=kwargs.get('cwd'), env=kwargs.get('env'))
        kwargs['env'] = actual_env
        guard.begin_native_launch(ticket)
        active_launch += 1
        try:
            original_popen(self, actual_argv, **kwargs)
            guard.attach_process(self, ticket)
        finally:
            active_launch -= 1
            guard.end_native_launch()

    original_start = multiprocessing.process.BaseProcess.start

    def guarded_start(process):
        nonlocal active_launch
        if process._target is not worker_entry:
            reject('children')
        ticket = guard.prepare_spawn(process, 'flog-worker')
        guard.begin_native_launch(ticket)
        active_launch += 1
        try:
            result = original_start(process)
            guard.attach_spawn(process, ticket)
            return result
        finally:
            active_launch -= 1
            guard.end_native_launch()

    subprocess.Popen.__init__ = guarded_popen
    multiprocessing.process.BaseProcess.start = guarded_start
    if sys.platform == 'win32':
        import _winapi
        original_create = _winapi.CreateProcess
        original_junction = _winapi.CreateJunction
        def create(*args, **kwargs):
            if not active_launch:
                reject('children')
            return guard.native_create_process(original_create, args, kwargs)
        def junction(source, destination):
            if not owned(source) or not owned(destination):
                reject('writers')
            return original_junction(source, destination)
        _winapi.CreateProcess = create
        _winapi.CreateJunction = junction
    socket.has_ipv6 = False
    for name in ('connect', 'connect_ex', 'bind', 'sendto', 'sendmsg'):
        if hasattr(socket.socket, name):
            setattr(socket.socket, name, lambda *a, **k: reject('network'))
    for name in ('getaddrinfo', 'gethostbyname', 'gethostbyname_ex', 'gethostbyaddr', 'create_connection'):
        setattr(socket, name, lambda *a, **k: reject('network'))
    sys.addaudithook(audit)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT))
    _guard_counts = counts
    guard.mark_guard_ready()
    return counts


def admission_race(log, producer, level, timing, worker=False):
    """Bounded latch race, also executed inside a real Windows spawn child."""
    import hashlib
    import threading
    entered, release, frozen, finished = [threading.Event() for _ in range(4)]
    original_send, original_clock, original_fallback = log._send, log._clock, log._fallback
    receipts, finals, errors, fallback_calls = [], [], [], []
    def send(context, kind, record=None, **kwargs):
        if threading.current_thread().name == 'FLOG-fixture-emitter':
            entered.set()
            if not release.wait(5):
                raise AssertionError('fixture release timeout')
        return original_send(context, kind, record, **kwargs)
    def clock():
        if threading.current_thread().name == 'FLOG-fixture-finalizer' and producer.get('_registration_frozen'):
            frozen.set()
        return original_clock()
    def fallback(*args):
        fallback_calls.append(True)
        return original_fallback(*args)
    def operation():
        try:
            if level == 'FLUSH':
                receipts.append(log.flush_logging(producer))
            else:
                receipts.append(log.emit_event(producer, level, 'admitted_fixture', 'safe', {}))
        except BaseException as error:
            errors.append(type(error).__name__)
    def finalize():
        try:
            function = log.finish_worker if worker else log.finalize_logging
            finals.append(function(producer, execution_outcome='COMPLETED', exit_status=0))
        except BaseException as error:
            errors.append(type(error).__name__)
        finally:
            finished.set()
    def files():
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in producer['_run_root'].iterdir() if path.is_file()}
    emitter = threading.Thread(target=operation, name='FLOG-fixture-emitter')
    finalizer = threading.Thread(target=finalize, name='FLOG-fixture-finalizer')
    log._send, log._clock, log._fallback = send, clock, fallback
    try:
        emitter.start()
        assert entered.wait(3)
        finalizer.start()
        assert frozen.wait(3)
        health = None if worker else log.check_logging_health(producer)
        if timing == 'completion':
            release.set()
        assert finished.wait(3)
        before = files()
        snapshot = json.loads(json.dumps(finals[0]))
        calls_before = len(fallback_calls)
        release.set()
        emitter.join(3)
        finalizer.join(3)
        assert not emitter.is_alive() and not finalizer.is_alive()
        assert not errors
        after = files()
        cached = (log.finish_worker if worker else log.finalize_logging)(
            producer, execution_outcome='COMPLETED', exit_status=0)
        return dict(report=finals[0], snapshot=snapshot, cached_unchanged=cached == snapshot,
            receipt=receipts[0], before=before, after=after, health_during_freeze=health,
            fallback_calls_after_release=len(fallback_calls) - calls_before)
    finally:
        release.set()
        if emitter.ident is not None:
            emitter.join(3)
        if finalizer.ident is not None:
            finalizer.join(3)
        log._send, log._clock, log._fallback = original_send, original_clock, original_fallback


def worker_entry(bootstrap, mode='normal', count=100):
    bootstrap = dict(bootstrap)
    ticket = bootstrap.pop('_f3_guard_ticket', None)
    if ticket is not None:
        offline_guard().bind_spawn_ticket(ticket)
    install_guards()
    code = 0
    try:
        return _worker_body(bootstrap, mode, count)
    except SystemExit as error:
        code = error.code if isinstance(error.code, int) else 1
        raise
    except BaseException:
        code = 1
        raise
    finally:
        offline_guard().finish(code)


def _worker_body(bootstrap, mode, count):
    import run_logging as log
    if mode == 'before':
        raise SystemExit(7)
    # Construct the secret here: it is never passed in spawn arguments.
    secret = 'child-' + 'canary-719'
    producer = log.configure_worker(bootstrap, sensitive_values=(secret,))
    if mode.startswith('race:'):
        _, level, timing = mode.split(':')
        result = admission_race(log, producer, level, timing, worker=True)
        bootstrap['_fixture_reply'].send(result)
        bootstrap['_fixture_reply'].close()
        return
    for i in range(count):
        log.emit_event(producer, 'INFO', 'worker_progress', 'строка ' + str(i), {'page': i})
    log.emit_event(producer, 'ERROR', 'worker_error', secret, {})
    if mode == 'no_final':
        return
    log.finish_worker(producer, execution_outcome='COMPLETED', exit_status=0)
    if mode == 'after':
        raise SystemExit(9)


def cli_probe(mode):
    install_guards()
    calls = []
    for name in ('data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload'):
        module = types.ModuleType(name)

        def stage(*args, name=name, **kwargs):
            calls.append(name)
            if name == 'data_gathering' and mode == 'error':
                raise ValueError('password=CLI_CANARY_823')

        module.main = stage
        sys.modules[name] = module
    sys.argv = [str(ROOT / 'main.py')]
    # No sensitive literal registration is needed for sensitive key=value text.
    runpy.run_path(str(ROOT / 'main.py'), run_name='__main__')


if __name__ == '__main__':
    code = 0
    try:
        cli_probe(sys.argv[1])
    except SystemExit as error:
        code = error.code if isinstance(error.code, int) else 1
        raise
    except BaseException:
        code = 1
        raise
    finally:
        offline_guard().finish(code)
