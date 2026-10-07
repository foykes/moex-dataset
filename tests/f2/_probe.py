"""Bounded offline launchers; no project import before installing guards."""

import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import runpy
import shutil
import socket
import stat
import subprocess
import sys
import time


_guard_counts = None
_guard_identity = None


def offline_guard():
    """Load the stdlib reporting adapter once, without importing project code."""
    name = 'mds_offline_guard'
    if name not in sys.modules:
        path = Path(__file__).resolve().parents[2] / 'tools/offline_guard.py'
        specification = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(specification)
        sys.modules[name] = module
        specification.loader.exec_module(module)
    return sys.modules[name]


MODULES = ('main', '1year', 'all', 'tests', 'count_check', 'main_tests',
           'data_gathering', 'tech', 'dividends', 'dohodru_data', 'upload')


def safe_tree(path, boundary, recursive=True):
    """Reject existing aliases before pytest cleanup or cache creation."""
    path = Path(os.path.abspath(path))
    boundary = Path(os.path.abspath(boundary))
    if not path.is_relative_to(boundary):
        raise ValueError('F2 output must stay inside the task .f2 directory')
    for entry in (boundary, *path.relative_to(boundary).parents):
        # Relative parents are checked by the component walk below.
        if not entry.is_absolute():
            continue
        if entry.exists() and entry.resolve() != entry:
            raise ValueError('F2 aliases are forbidden')
    current = boundary.parent
    for part in path.relative_to(boundary.parent).parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError('F2 reparse points are forbidden')
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('F2 directory component is not a directory')
    if path.exists() and recursive:
        for directory, dirs, files in os.walk(path, followlinks=False):
            for name in dirs + files:
                info = (Path(directory) / name).lstat()
                if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1):
                    raise ValueError('F2 existing output contains an alias')
    return path


def install_guards(root, mode='import'):
    """Probes forbid all children/writers; harness permits bounded local fixtures."""
    global _guard_counts, _guard_identity
    root = Path(root).resolve()
    identity = (root, mode)
    if _guard_counts is not None:
        if _guard_identity != identity:
            raise ValueError('F2 guard context cannot change in one interpreter')
        return _guard_counts
    boundary = root / '.f2'
    counts = dict(network=0, conversion=0, writers=0, secrets=0, dataset_reads=0)
    guard = offline_guard()
    guard.attach_native(Path(__file__).resolve().parents[2], 'f2', 'f2-probe', counts)
    git = shutil.which('git')
    interpreter = os.path.normcase(os.path.abspath(sys.executable))
    active_launch = 0

    def reject(kind):
        counts[kind] += 1
        guard.native_notice(kind)
        raise RuntimeError('F2_OFFLINE_' + kind.upper())

    def owned(value):
        try:
            return Path(os.path.abspath(value)).is_relative_to(boundary)
        except (TypeError, ValueError):
            return False

    def evidence_write(value):
        """Native fixtures may write their roots, never an earlier run's reports."""
        try:
            path = Path(os.path.abspath(value))
            current = guard.report_root(Path(__file__).resolve().parents[2], 'f2')
            for candidate in (path, path.resolve()):
                if candidate.is_relative_to(boundary / 'evidence') and not candidate.is_relative_to(current):
                    return False
            return True
        except (TypeError, ValueError):
            return False

    def audit(event, args):
        if guard.internal_report_io(event, args):
            return
        if event.startswith('socket.') and event in {'socket.connect', 'socket.bind', 'socket.getaddrinfo', 'socket.gethostbyname', 'socket.gethostbyaddr', 'socket.sendto'}:
            reject('network')
        if event == 'open':
            value, file_mode, flags = args
            if isinstance(value, (str, bytes, os.PathLike)):
                parts = Path(os.fsdecode(value)).parts
                if any(part.lower() == 'secrets' for part in parts) or Path(os.fsdecode(value)).name == '.env':
                    reject('secrets')
                writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
                null_sink = os.path.normcase(os.path.abspath(value)) == os.path.normcase(os.path.abspath(os.devnull))
                if writing and not null_sink and not evidence_write(value):
                    reject('writers')
                if writing and mode == 'harness':
                    try:
                        info = Path(os.fsdecode(value)).lstat()
                    except FileNotFoundError:
                        pass
                    else:
                        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                            reject('writers')
                if writing and not null_sink and not (mode == 'harness' and owned(value)):
                    reject('writers')
                if mode == 'import' and not writing and 'datasets' in parts:
                    reject('dataset_reads')
        if event in {'os.remove', 'os.rmdir', 'os.mkdir', 'os.rename', 'os.chmod', 'os.link', 'os.symlink'}:
            destinations = args[:2] if event in {'os.rename', 'os.link', 'os.symlink'} else args[:1]
            if not all(evidence_write(value) for value in destinations):
                reject('writers')
            if not (mode == 'harness' and all(owned(value) for value in destinations)):
                reject('writers')
        if event in {'os.system', 'os.exec', 'os.spawn', 'os.posix_spawn', 'os.startfile'}:
            reject('conversion')
        if event == 'subprocess.Popen':
            if mode != 'harness' or not active_launch:
                reject('conversion')

    original_popen_init = subprocess.Popen.__init__

    def guarded_popen(self, argv, *positional, **kwargs):
        nonlocal active_launch
        if mode != 'harness' or positional or kwargs.get('shell') or not isinstance(argv, (list, tuple)):
            reject('conversion')
        command = [os.fsdecode(item) for item in argv]
        executable = kwargs.get('executable') or command[0]
        executable = shutil.which(executable, path=(kwargs.get('env') or os.environ).get('PATH')) or executable
        binary = os.path.normcase(os.path.abspath(executable))
        actual_cwd = Path(kwargs.get('cwd') or os.getcwd()).resolve()
        allowed_git = git is not None and binary == os.path.normcase(os.path.abspath(git))
        if allowed_git:
            position = 1
            while position < len(command) and command[position] == '-c':
                position += 2
            verb = command[position] if position < len(command) else ''
            read_verbs = {'rev-parse', 'symbolic-ref', 'ls-files', 'diff', 'show', 'cat-file'}
            fixture_verbs = {'init', 'config', 'add', 'commit', 'rm', 'mv', 'update-index', 'hash-object',
                             'write-tree', 'diff-index', 'checkout', 'apply'}
            if verb not in read_verbs | fixture_verbs:
                reject('conversion')
            if verb in fixture_verbs and not actual_cwd.is_relative_to(boundary):
                reject('writers')
            if verb == 'config' and '--local' not in command:
                reject('writers')
        allowed_python = binary == interpreter and '-I' in command and '-B' in command
        if allowed_python:
            if '-m' in command:
                module = command[command.index('-m') + 1]
                allowed_python = module in {'pre_commit', 'pytest'}
            else:
                scripts = [Path(item) for item in command[1:] if item.endswith('.py')]
                allowed_python = bool(scripts) and all(
                    script.absolute().is_relative_to(boundary) or script.absolute().is_relative_to(root / 'tests/f2')
                    for script in scripts)
        if not (allowed_git or allowed_python) or not (actual_cwd == root or actual_cwd.is_relative_to(boundary) or actual_cwd.is_relative_to(root / 'tests/f2')):
            reject('conversion')
        actual_argv, actual_env, ticket = guard.notify_child(
            argv, cwd=actual_cwd, env=kwargs.get('env'))
        kwargs['env'] = actual_env
        guard.begin_native_launch(ticket)
        active_launch += 1
        try:
            original_popen_init(self, actual_argv, **kwargs)
            guard.attach_process(self, ticket)
        finally:
            active_launch -= 1
            guard.end_native_launch()
    subprocess.Popen.__init__ = guarded_popen
    if mode == 'harness':
        original_start = multiprocessing.process.BaseProcess.start

        def guarded_start(process):
            nonlocal active_launch
            if process._target is not spawn_one:
                reject('conversion')
            ticket = guard.prepare_spawn(process, 'f2-spawn')
            guard.begin_native_launch(ticket)
            active_launch += 1
            try:
                result = original_start(process)
                guard.attach_spawn(process, ticket)
                return result
            finally:
                active_launch -= 1
                guard.end_native_launch()

        multiprocessing.process.BaseProcess.start = guarded_start
        if sys.platform == 'win32':
            import _winapi
            original_create = _winapi.CreateProcess

            def guarded_create(*args, **kwargs):
                if not active_launch:
                    reject('conversion')
                return guard.native_create_process(original_create, args, kwargs)

            _winapi.CreateProcess = guarded_create
    sys.addaudithook(audit)
    # urllib3's import-time IPv6 capability probe binds a loopback socket.
    # The offline profile disables that capability before requests is imported;
    # every actual bind/connect remains forbidden and is counted.
    socket.has_ipv6 = False
    # sendto and connect_ex can avoid a useful exception from a socket audit event.
    for name in ('connect', 'connect_ex', 'bind', 'sendto', 'sendmsg'):
        if hasattr(socket.socket, name):
            setattr(socket.socket, name, lambda *a, **k: reject('network'))
    for name in ('getaddrinfo', 'gethostbyname', 'gethostbyname_ex', 'gethostbyaddr', 'getnameinfo', 'create_connection'):
        setattr(socket, name, lambda *a, **k: reject('network'))
    if mode == 'import':
        # Windows spawn goes directly through CreateProcess, not Popen.
        multiprocessing.process.BaseProcess.start = lambda *a, **k: reject('conversion')
        if sys.platform == 'win32':
            import _winapi
            _winapi.CreateProcess = lambda *a, **k: reject('conversion')
    _guard_counts, _guard_identity = counts, identity
    guard.mark_guard_ready()
    return counts


def import_one(root, name):
    root = Path(root).resolve()
    counts = install_guards(root)
    # Legacy library settings reads still use the declared launcher root (#45).
    sys.path.insert(0, str(root))
    result = {'module': name, 'sentinel': 'F2_IMPORT_OK', 'counts': counts}
    try:
        spec = importlib.util.spec_from_file_location('f2_entry_' + name, root / (name + '.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result['functions'] = sorted(key for key, value in vars(module).items() if callable(value) and getattr(value, '__module__', None) == module.__name__)
        result['outcome'] = 'PASS'
    except BaseException as error:
        result['outcome'] = 'FAIL'
        result['error_type'] = type(error).__name__
    return result


def spawn_one(root, connection, ticket=None):
    if ticket is not None:
        offline_guard().bind_spawn_ticket(ticket)
    try:
        result = import_one(root, 'main')
        result['sentinel'] = 'F2_SPAWN_OK'
        connection.send(result)
    finally:
        connection.close()
        offline_guard().finish()


def main():
    action = sys.argv[1]
    if action == 'import':
        result = import_one(sys.argv[2], sys.argv[3])
        print('F2_RESULT:' + json.dumps(result, ensure_ascii=False))
        return 0 if result['outcome'] == 'PASS' and not any(result['counts'].values()) else 1
    if action == 'spawn':
        install_guards(sys.argv[2], 'harness')
        context = multiprocessing.get_context('spawn')
        records = []
        for _ in range(2):
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=spawn_one, args=(sys.argv[2], child))
            started = time.monotonic()
            deadline = started + 30
            try:
                process.start()
                child.close()
                if not parent.poll(max(0, deadline - time.monotonic())):
                    raise RuntimeError('Spawn timeout')
                record = parent.recv()
                process.join(max(0, deadline - time.monotonic()))
                if process.is_alive():
                    raise RuntimeError('Spawn join timeout')
                record['exit_code'] = process.exitcode
                record['elapsed_seconds'] = round(time.monotonic() - started, 6)
                records.append(record)
            finally:
                parent.close()
                child.close()
                if process.pid is not None and process.is_alive():
                    # Cleanup is bounded separately and targets only this child.
                    process.terminate()
                    process.join(5)
        result = {'start_method': 'spawn', 'children': records}
        print('F2_RESULT:' + json.dumps(result, ensure_ascii=False))
        return 0 if all(r['exit_code'] == 0 and r['outcome'] == 'PASS' and not any(r['counts'].values()) for r in records) else 1
    if action == 'run':
        script = Path(sys.argv[2]).resolve()
        install_guards(Path(os.environ['F2_TASK_ROOT']), 'harness')
        sys.argv = sys.argv[2:]
        runpy.run_path(str(script), run_name='__main__')
        return 0
    if action == 'preflight':
        import re
        root = Path(sys.argv[2]).absolute()
        install_guards(root, 'harness')
        label = sys.argv[3]
        if not re.fullmatch('[A-Za-z0-9-]+', label):
            raise ValueError('Invalid evidence label')
        boundary = root / '.f2'
        safe_tree(boundary / 'tmp', boundary, recursive=False)
        safe_tree(boundary / 'cache/pytest', boundary)
        safe_tree(boundary / 'evidence', boundary)
        for name in (label + '.log', label + '.json', 'pytest.json'):
            path = boundary / 'evidence' / name
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                raise ValueError('Unsafe evidence destination')
        print('F2_PREFLIGHT PASS')
        return 0
    if action == 'guard-controls':
        root = Path(sys.argv[2]).resolve()
        counts = install_guards(root)
        actions = (lambda: socket.create_connection(('localhost', 9)),
                   lambda: os.system('F2_FORBIDDEN_CONVERTER'),
                   lambda: (root / 'F2_FORBIDDEN_WRITE').write_text('forbidden'),
                   lambda: (root / 'secrets/canary.json').read_text())
        for call in actions:
            try:
                call()
            except RuntimeError:
                continue
            raise RuntimeError('Guard did not reject a control')
        print('F2_RESULT:' + json.dumps({'counts': counts}))
        return 0
    raise ValueError('Unknown probe action')


if __name__ == '__main__':
    code = 1
    try:
        code = main()
    except SystemExit as error:
        code = error.code if isinstance(error.code, int) else 1
        raise
    finally:
        offline_guard().finish(code)
    raise SystemExit(code)
