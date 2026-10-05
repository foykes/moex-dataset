"""Process-wide guards for the isolated, offline environment checks."""

import os
import multiprocessing.process
from pathlib import Path
import socket
import subprocess
import sys


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


def isolated_path(value):
    """Resolve an output/cache directory only within this worktree's .f1."""
    project_root = Path(__file__).resolve().parents[2]
    allowed = (project_root / ".f1").resolve()
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    path = path.resolve()
    if not allowed.is_relative_to(project_root) or not path.is_relative_to(allowed):
        raise ValueError("F1 output and cache directories must stay inside .f1")
    return path


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
