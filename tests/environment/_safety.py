"""Process-wide guards for the isolated, offline environment checks."""

import os
from contextlib import contextmanager
import multiprocessing.process
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile


class OfflineViolation(RuntimeError):
    """A check attempted network access or child-process execution."""


_installed = False


def _blocked(*args, **kwargs):
    # Аргументы могут содержать приватный URL; не включаем их в сообщение.
    raise OfflineViolation("F1 checks forbid network access and child processes")


def _audit_guard(event, args):
    if event in {
        "socket.connect", "socket.bind", "socket.getaddrinfo",
        "socket.gethostbyname", "socket.gethostbyaddr", "socket.getnameinfo",
        "subprocess.Popen", "os.system", "os.posix_spawn", "os.spawn",
        "os.exec", "os.startfile", "os.startfile/2", "os.fork", "os.forkpty",
        "_winapi.CreateProcess",
    }:
        _blocked()


def install_guards():
    """Install before collection/imports; intentionally keep guards until exit."""
    global _installed
    if _installed:
        return

    # Блокируем DNS, соединения и UDP, включая вызовы из импортируемых пакетов.
    for name in (
        "create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex",
        "gethostbyaddr", "getnameinfo",
    ):
        setattr(socket, name, _blocked)
    for name in ("connect", "connect_ex", "bind", "sendto", "sendmsg"):
        if hasattr(socket.socket, name):
            setattr(socket.socket, name, _blocked)
    # asyncio наследуется от Popen при импорте: сохраняем сам класс.
    subprocess.Popen.__init__ = _blocked
    # Windows multiprocessing запускает child напрямую через _winapi.
    multiprocessing.process.BaseProcess.start = _blocked
    if sys.platform == "win32":
        import _winapi

        _winapi.CreateProcess = _blocked
    for name in ("run", "call", "check_call", "check_output"):
        setattr(subprocess, name, _blocked)
    for name in (
        "system", "popen", "startfile", "posix_spawn", "posix_spawnp",
        "spawnl", "spawnle", "spawnlp", "spawnlpe", "spawnv", "spawnve",
        "spawnvp", "spawnvpe", "execl", "execle", "execlp", "execlpe",
        "execv", "execve", "execvp", "execvpe", "fork", "forkpty",
    ):
        if hasattr(os, name):
            setattr(os, name, _blocked)
    sys.addaudithook(_audit_guard)
    _installed = True


def _is_link(info):
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def isolated_path(value):
    """Keep a real directory boundary; reject aliases instead of following them."""
    project_root = Path(__file__).resolve().parents[2]
    allowed = project_root / ".f1"
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    path = Path(os.path.abspath(path))
    if not path.is_relative_to(allowed):
        raise ValueError("F1 output and cache directories must stay inside .f1")
    # lstat видит dangling links и Windows junction/reparse points.
    directories = [allowed]
    directory = allowed
    for part in path.relative_to(allowed).parts:
        directory = directory / part
        directories.append(directory)
    for directory in directories:
        try:
            info = directory.lstat()
        except FileNotFoundError:
            if directory == allowed:
                raise ValueError("Create a real .f1 directory before F1 checks") from None
            continue
        if _is_link(info) or not stat.S_ISDIR(info.st_mode):
            raise ValueError("F1 directories must be real directories without links or reparse points")
    # resolve подтверждает границу, но никогда не расширяет её.
    if path.resolve() != path:
        raise ValueError("F1 directory aliases are forbidden")
    return path


def validate_output_file(value):
    """Refuse existing symlinks, hardlinks and non-files before any write."""
    path = Path(value)
    project_root = Path(__file__).resolve().parents[2]
    if not path.is_absolute():
        path = project_root / path
    path = Path(os.path.abspath(path))
    isolated_path(path.parent)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    if _is_link(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("F1 output files must be regular files with exactly one link")
    return path


@contextmanager
def isolated_file(value):
    """Write a new exclusive sibling, then replace only a safe destination."""
    path = validate_output_file(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    validate_output_file(path)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".f1-output-", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    identity = os.fstat(descriptor)
    try:
        with os.fdopen(descriptor, "w+b") as stream:
            yield stream
        validate_output_file(path)
        temporary_info = temporary.lstat()
        if _is_link(temporary_info) or not stat.S_ISREG(temporary_info.st_mode) or temporary_info.st_nlink != 1 or (temporary_info.st_dev, temporary_info.st_ino) != (identity.st_dev, identity.st_ino):
            raise ValueError("The exclusively created F1 temporary file changed")
        os.replace(temporary, path)
    finally:
        # Удаляем только собственный временный inode; чужую ссылку не трогаем.
        try:
            isolated_path(temporary.parent)
            info = temporary.lstat()
        except (FileNotFoundError, ValueError):
            pass
        else:
            if not _is_link(info) and info.st_nlink == 1 and (info.st_dev, info.st_ino) == (identity.st_dev, identity.st_ino):
                temporary.unlink()


def write_isolated_text(value, text):
    with isolated_file(value) as stream:
        stream.write(text.encode("utf-8"))


def validate_pytest_basetemp(value):
    """Allow recursive pytest cleanup only in a dedicated direct tmp child."""
    project_root = Path(__file__).resolve().parents[2]
    requested = Path(value)
    if not requested.is_absolute():
        requested = project_root / requested
    requested = Path(os.path.abspath(requested))
    path = isolated_path(requested)
    names = {"pytest", "pytest-runtime", "pytest-test", "pytest-dev"}
    # pytest удаляет basetemp рекурсивно. Корни, venv и symlink alias запрещены.
    if requested.parent != project_root / ".f1" / "tmp" or requested.name not in names or path != requested:
        raise ValueError("Pytest basetemp must be a direct .f1/tmp/pytest[-profile] directory without symlink aliases")
    return path


def validate_pytest_cache(value):
    """Keep pytest cache writes away from interpreters, wheels and evidence."""
    project_root = Path(__file__).resolve().parents[2]
    requested = Path(value)
    if not requested.is_absolute():
        requested = project_root / requested
    requested = Path(os.path.abspath(requested))
    path = isolated_path(requested)
    cache_root = project_root / ".f1" / "cache"
    profile_names = {"pytest-cache-runtime", "pytest-cache-test", "pytest-cache-dev"}
    profile_cache = requested.parent == project_root / ".f1" and requested.name in profile_names
    if path != requested or not (requested.is_relative_to(cache_root) or profile_cache):
        raise ValueError("Pytest cache must stay under .f1/cache or .f1/pytest-cache-<profile> without symlink aliases")
    return path
