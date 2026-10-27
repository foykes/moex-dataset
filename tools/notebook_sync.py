"""Explicit, non-executing notebook pairing and the guarded commit workflow."""

import argparse
import ast
import hashlib
import importlib
from importlib import metadata
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import struct
import subprocess
import sys
import sysconfig
import tempfile
import tokenize


PAIRS = (
    "1year", "all", "count_check", "data_gathering", "dividends",
    "dohodru_data", "main_tests", "tech", "tests", "upload",
)
CONTROL_FILES = ("tools/notebook_sync.py", ".pre-commit-config.yaml", "pyproject.toml")
RESTAGE_REQUIRED = 3
SYNC_BASE_ENV = "MDS_NOTEBOOK_SYNC_BASE"
SYNC_INDEX_ENV = "MDS_NOTEBOOK_SYNC_INDEX"
ERROR_CODES = {
    "UNSAFE_PATH", "MISSING_ROOT", "GIT_FAILED", "EXACT_SHA_REQUIRED",
    "FEATURE_BRANCH_REQUIRED", "MISSING_TOOL", "TOOL_VERSION", "UNSUPPORTED_PAIR",
    "CONVERTER_FAILED", "PAIR_MEMBER_STATE", "INVALID_SNAPSHOT", "PAIR_MISMATCH",
    "SOURCE_CHANGED", "UNKNOWN_PAIR", "UNSYNCED_BASE", "SOURCE_CONFLICT",
    "PARTIAL_PAIR", "CONTROL_SOURCE_UNSTAGED", "INTERPRETER_PROFILE",
    "HOOK_CONFIG_UNSAFE", "BYPASS_REJECTED", "HEAD_CHANGED", "INDEX_CHANGED",
    "HOOK_IDENTITY",
}


def _fail(code, detail=""):
    raise ValueError(code + (": " + detail if detail else ""))


def _checked_path(path, directory=None):
    """Check lexical ancestors before resolving or creating anything."""
    path = Path(path).absolute()
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or (
            getattr(info, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        ):
            _fail("UNSAFE_PATH", "linked path")
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            _fail("UNSAFE_PATH", "hardlinked file")
        if not stat.S_ISDIR(info.st_mode) and not stat.S_ISREG(info.st_mode):
            _fail("UNSAFE_PATH", "non-regular entry")
        if current != path and not stat.S_ISDIR(info.st_mode):
            _fail("UNSAFE_PATH", "non-directory ancestor")
    if path.exists():
        if directory is True and not path.is_dir():
            _fail("UNSAFE_PATH", "expected directory")
        if directory is False and not path.is_file():
            _fail("UNSAFE_PATH", "expected file")
    return path


def _root(root=None):
    # resolve() до lstat спрятал бы junction/symlink от ранней проверки.
    path = _checked_path(root or Path(__file__).absolute().parents[1], True)
    if not path.is_dir():
        _fail("MISSING_ROOT")
    return path


def _git_executable():
    executable = shutil.which("git")
    if not executable:
        _fail("MISSING_TOOL", "git")
    return str(Path(executable).absolute())


def _git(args, root):
    result = subprocess.run(
        [_git_executable(), "-c", "core.quotePath=false", *args], cwd=root,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=False, timeout=30, check=False,
    )
    if result.returncode:
        # stderr Git может содержать приватный путь: наружу только код операции.
        _fail("GIT_FAILED", args[0] + " exit=" + str(result.returncode))
    return result.stdout


def _head(root):
    return _git(["rev-parse", "--verify", "HEAD^{commit}"], root).decode().strip()


def _ref(ref, root):
    if not re.fullmatch(r"[0-9a-fA-F]{40}", ref or ""):
        _fail("EXACT_SHA_REQUIRED")
    resolved = _git(["rev-parse", "--verify", ref + "^{commit}"], root).decode().strip()
    if resolved.lower() != ref.lower():
        _fail("EXACT_SHA_REQUIRED")
    return resolved


def _feature_branch(root):
    result = subprocess.run(
        [_git_executable(), "symbolic-ref", "--quiet", "--short", "HEAD"], cwd=root,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=False, timeout=30, check=False,
    )
    if result.returncode or not result.stdout.decode().strip().startswith("codex/"):
        _fail("FEATURE_BRANCH_REQUIRED")


def get_jupytext():
    try:
        version = metadata.version("jupytext")
    except metadata.PackageNotFoundError:
        _fail("MISSING_TOOL", "jupytext")
    if version != "1.19.2":
        _fail("TOOL_VERSION", "jupytext")
    previous_initializer = subprocess.Popen.__init__

    def no_optional_converter(*args, **kwargs):
        # Jupytext при import ищет optional pandoc через Popen. Для JSON/percent
        # он не нужен; FileNotFoundError штатно отключает эту discovery.
        raise FileNotFoundError("Optional converter discovery is disabled")

    # Сохраняем сам класс Popen: asyncio может наследоваться от него при import.
    subprocess.Popen.__init__ = no_optional_converter
    try:
        return importlib.import_module("jupytext")
    except ImportError:
        _fail("MISSING_TOOL", "jupytext dependency")
    finally:
        subprocess.Popen.__init__ = previous_initializer


def _text(source):
    if isinstance(source, bytes):
        return source.decode("utf-8")
    return source


def _newlines(source):
    return _text(source).replace("\r\n", "\n")


def source_signature(source, fmt):
    """AST permits code formatting; comments and cell semantics stay explicit."""
    jupytext = get_jupytext()
    try:
        notebook = jupytext.reads(_text(source), fmt=fmt)
        cells = []
        for cell in notebook.cells:
            cell_source = _newlines(cell.source)
            cell_metadata = dict(cell.get("metadata", {}))
            cell_metadata.pop("lines_to_next_cell", None)
            if cell_metadata.get("tags") == []:
                cell_metadata.pop("tags")
            if cell.cell_type == "code":
                code = ast.dump(ast.parse(cell_source), include_attributes=False)
                comments = tuple(
                    token.string.rstrip()
                    for token in tokenize.generate_tokens(io.StringIO(cell_source).readline)
                    if token.type == tokenize.COMMENT
                )
                content = (code, comments)
            elif cell.cell_type in {"markdown", "raw"}:
                content = cell_source.rstrip("\n")
            else:
                _fail("UNSUPPORTED_PAIR", "cell type")
            cells.append((
                cell.cell_type, content,
                json.dumps(cell_metadata, ensure_ascii=False, sort_keys=True),
            ))
        if fmt == "py:percent":
            ast.parse(_text(source))
        return tuple(cells)
    except ValueError as error:
        if str(error).startswith("UNSUPPORTED_PAIR"):
            raise
        _fail("UNSUPPORTED_PAIR", "invalid source or metadata")
    except (SyntaxError, tokenize.TokenError, TypeError):
        _fail("UNSUPPORTED_PAIR", "invalid source or metadata")


def notebook_export(notebook_source):
    """The API serializes in memory: no kernel, CLI, notary or file writer."""
    jupytext = get_jupytext()
    try:
        notebook = jupytext.reads(_text(notebook_source), fmt="ipynb")
        output = jupytext.writes(notebook, fmt="py:percent")
        if not output.endswith("\n"):
            output += "\n"
        ast.parse(output)
        if source_signature(notebook_source, "ipynb") != source_signature(output, "py:percent"):
            _fail("UNSUPPORTED_PAIR", "export changes cell semantics")
        return output
    except ValueError as error:
        if str(error).startswith("UNSUPPORTED_PAIR"):
            raise
        _fail("CONVERTER_FAILED")
    except Exception:
        # Не печатаем notebook content либо traceback стороннего converter.
        _fail("CONVERTER_FAILED")


def _read_member(path, snapshot, ref, root):
    if snapshot == "worktree":
        member = _checked_path(root / path, False)
        if not member.is_file():
            _fail("PAIR_MEMBER_STATE", path)
        return member.read_bytes()
    revision = ":" if snapshot == "index" else ref + ":"
    return _git(["show", revision + path], root)


def check(snapshot="worktree", ref=None, root=None):
    root = _root(root)
    if snapshot not in {"worktree", "index", "ref"}:
        _fail("INVALID_SNAPSHOT")
    if snapshot == "ref":
        ref = _ref(ref, root)
    failures = []
    for stem in PAIRS:
        notebook = _read_member(stem + ".ipynb", snapshot, ref, root)
        script = _read_member(stem + ".py", snapshot, ref, root)
        emitted = notebook_export(notebook)
        if source_signature(emitted, "py:percent") != source_signature(script, "py:percent"):
            failures.append(stem)
    if failures:
        _fail("PAIR_MISMATCH", ", ".join(failures))
    print("NOTEBOOK_CHECK snapshot=" + snapshot + " pairs=" + str(len(PAIRS)) + " PASS")
    return 0


def _digest(source):
    return hashlib.sha256(source).hexdigest()


def _verify_inputs(inputs):
    for path, digest in inputs.items():
        _checked_path(path, False)
        if not path.is_file() or _digest(path.read_bytes()) != digest:
            _fail("SOURCE_CHANGED", path.name)


def _owned_temporary(path, identity):
    _checked_path(path, False)
    info = path.lstat()
    if (info.st_dev, info.st_ino) != identity:
        _fail("UNSAFE_PATH", "temporary identity changed")


def _atomic_script(path, content, inputs):
    _verify_inputs(inputs)
    temporary = None
    temporary_identity = None
    try:
        # Соседний exclusive temp даёт atomic replace одного файла, не всей пары.
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix="." + path.name + ".f2-",
            suffix=".tmp", delete=False,
        ) as stream:
            temporary = Path(stream.name)
            info = os.fstat(stream.fileno())
            temporary_identity = (info.st_dev, info.st_ino)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        _owned_temporary(temporary, temporary_identity)
        os.chmod(temporary, stat.S_IMODE(path.stat().st_mode))
        _verify_inputs(inputs)
        _owned_temporary(temporary, temporary_identity)
        if _digest(temporary.read_bytes()) != _digest(content):
            _fail("SOURCE_CHANGED", "temporary content")
        os.replace(temporary, path)
        inputs[path] = _digest(content)
    finally:
        if temporary is not None:
            try:
                temporary.lstat()
            except FileNotFoundError:
                pass
            else:
                # Не удаляем чужую ordinary replacement, даже без symlink.
                _owned_temporary(temporary, temporary_identity)
                temporary.unlink()


def sync(base, pairs, root=None):
    root = _root(root)
    _feature_branch(root)
    base = _ref(base, root)
    pairs = tuple(dict.fromkeys(pairs))
    if any(stem not in PAIRS for stem in pairs):
        _fail("UNKNOWN_PAIR")
    candidates = []
    inputs = {}
    for stem in pairs:
        notebook_path = root / (stem + ".ipynb")
        script_path = root / (stem + ".py")
        notebook = _read_member(stem + ".ipynb", "worktree", None, root)
        script = _read_member(stem + ".py", "worktree", None, root)
        inputs[notebook_path] = _digest(notebook)
        inputs[script_path] = _digest(script)
        base_notebook = _read_member(stem + ".ipynb", "ref", base, root)
        base_script = _read_member(stem + ".py", "ref", base, root)
        base_output = notebook_export(base_notebook)
        if source_signature(base_output, "py:percent") != source_signature(base_script, "py:percent"):
            _fail("UNSYNCED_BASE", stem)
        candidate = notebook_export(notebook)
        script_signature = source_signature(script, "py:percent")
        # Важен этот порядок: restaged пара уже согласована, хотя .py != HEAD.
        if source_signature(candidate, "py:percent") == script_signature:
            continue
        if source_signature(base_script, "py:percent") != script_signature:
            _fail("SOURCE_CONFLICT", stem)
        candidates.append((script_path, candidate.encode("utf-8")))
    _verify_inputs(inputs)
    for path, content in candidates:
        _atomic_script(path, content, inputs)
        print("NOTEBOOK_EXPORT pair=" + path.stem + " outcome=RESTAGE_REQUIRED")
    if candidates:
        return RESTAGE_REQUIRED
    print("NOTEBOOK_SYNC pairs=" + str(len(pairs)) + " outcome=NOOP")
    return 0


def _index_entries(root):
    entries = {}
    raw = _git(["ls-files", "--stage", "-z"], root)
    for record in raw.split(b"\0"):
        if record:
            info, name = record.split(b"\t", 1)
            mode, digest, stage = info.decode().split()
            entries.setdefault(name.decode("utf-8"), []).append((mode, digest, stage))
    return entries, _digest(raw)


def _selected_pairs(root):
    raw = _git(["diff", "--cached", "--name-status", "--find-renames", "-z", "HEAD", "--"], root)
    parts = raw.decode("utf-8").split("\0")
    selected = set()
    position = 0
    known = {stem + suffix: stem for stem in PAIRS for suffix in (".py", ".ipynb")}
    while position < len(parts) and parts[position]:
        status = parts[position]
        count = 2 if status[0] in {"R", "C"} else 1
        names = parts[position + 1:position + 1 + count]
        position += 1 + count
        for name in names:
            if name in known:
                if status[0] not in {"A", "M"}:
                    _fail("PAIR_MEMBER_STATE", "deleted, renamed or unmerged member")
                selected.add(known[name])
            elif name.endswith(".ipynb") and "/" not in name:
                _fail("UNKNOWN_PAIR")
    return tuple(stem for stem in PAIRS if stem in selected)


def _index_matches_worktree(path, root, entries, code):
    entry = entries.get(path, [])
    if len(entry) != 1 or entry[0][2] != "0" or entry[0][0] not in {"100644", "100755"}:
        _fail("PAIR_MEMBER_STATE", path)
    worktree = _read_member(path, "worktree", None, root)
    index = _read_member(path, "index", None, root)
    # Windows checkout может быть CRLF, а Git blob LF; остальные bytes не скрываем.
    if _newlines(worktree) != _newlines(index):
        _fail(code, path)


def _profile():
    if (
        sys.version_info[:3] != (3, 14, 8) or sys.platform != "win32"
        or struct.calcsize("P") != 8 or sysconfig.get_config_var("Py_GIL_DISABLED") != 0
    ):
        _fail("INTERPRETER_PROFILE")
    if not _checked_path(sys.executable, False).is_file():
        _fail("INTERPRETER_PROFILE", "missing executable")
    for name, expected in (("jupytext", "1.19.2"), ("pre-commit", "4.3.0")):
        try:
            if metadata.version(name) != expected:
                _fail("TOOL_VERSION", name)
        except metadata.PackageNotFoundError:
            _fail("MISSING_TOOL", name)


def _cache_preflight(root):
    cache = _checked_path(root / ".f2" / "cache" / "precommit", True)
    if cache.exists():
        for directory, dirs, files in os.walk(cache, followlinks=False):
            for name in dirs + files:
                _checked_path(Path(directory) / name)
    return cache


def _hook_config(root):
    # Проверяем до pre_commit: remote/python hooks иначе могут установить deps.
    yaml = importlib.import_module("yaml")
    config = yaml.safe_load((root / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    repos = config.get("repos", []) if isinstance(config, dict) else []
    if len(repos) != 1 or repos[0].get("repo") != "local" or not config.get("fail_fast"):
        _fail("HOOK_CONFIG_UNSAFE")
    hooks = repos[0].get("hooks", [])
    if len(hooks) != 1:
        _fail("HOOK_CONFIG_UNSAFE")
    item = hooks[0]
    expected_entry = ["python", "-I", "-B", "tools/notebook_sync.py", "hook-check"]
    if (
        item.get("language") != "system" or item.get("additional_dependencies")
        or shlex.split(item.get("entry", "")) != expected_entry
        or item.get("pass_filenames") is not False or item.get("always_run") is not True
        or item.get("require_serial") is not True or item.get("stages") != ["pre-commit"]
    ):
        _fail("HOOK_CONFIG_UNSAFE")


def _hook_identity(root):
    # Absolute template нельзя применять к commit другого worktree. У manual
    # check/sync другой CWD допустим, но Git hook обслуживает только свой index.
    if _checked_path(Path.cwd(), True).resolve() != root.resolve():
        _fail("HOOK_IDENTITY", "invocation root")
    actual_root = _checked_path(
        Path(_git(["rev-parse", "--show-toplevel"], root).decode().strip()), True,
    )
    if actual_root.resolve() != root.resolve():
        _fail("HOOK_IDENTITY", "Git worktree")
    marker = _checked_path(root / ".git")
    if marker.is_dir():
        expected_gitdir = marker
    elif marker.is_file():
        text = marker.read_text(encoding="utf-8").strip()
        if not text.startswith("gitdir: "):
            _fail("HOOK_IDENTITY", "Git marker")
        expected_gitdir = Path(text[len("gitdir: "):])
        if not expected_gitdir.is_absolute():
            expected_gitdir = root / expected_gitdir
    else:
        _fail("HOOK_IDENTITY", "Git marker")
    actual_gitdir = _checked_path(
        Path(_git(["rev-parse", "--absolute-git-dir"], root).decode().strip()), True,
    )
    if actual_gitdir.resolve() != _checked_path(expected_gitdir, True).resolve():
        _fail("HOOK_IDENTITY", "Git directory")
    index = Path(_git(["rev-parse", "--git-path", "index"], root).decode().strip())
    if not index.is_absolute():
        index = root / index
    if _checked_path(index, False).resolve() != (actual_gitdir / "index").resolve():
        _fail("HOOK_IDENTITY", "alternate index")


def _hook_preflight(root):
    _hook_identity(root)
    _profile()
    _feature_branch(root)
    if any(os.environ.get(name) for name in ("SKIP", "PRE_COMMIT_ALLOW_NO_CONFIG", "_PRE_COMMIT_SKIP_POST_CHECKOUT")):
        _fail("BYPASS_REJECTED")
    cache = _cache_preflight(root)
    head = _head(root)
    entries, index_digest = _index_entries(root)
    if any(entry[2] != "0" for items in entries.values() for entry in items):
        _fail("PAIR_MEMBER_STATE", "unmerged index")
    pairs = _selected_pairs(root)
    intent = _git([
        "diff", "--no-ext-diff", "--ignore-submodules", "--diff-filter=A", "--name-only", "-z",
    ], root).decode("utf-8").split("\0")
    for stem in pairs:
        for suffix in (".ipynb", ".py"):
            path = stem + suffix
            if path in intent:
                _fail("PAIR_MEMBER_STATE", "intent-to-add member")
            _index_matches_worktree(path, root, entries, "PARTIAL_PAIR")
    for path in CONTROL_FILES:
        _index_matches_worktree(path, root, entries, "CONTROL_SOURCE_UNSTAGED")
    check("ref", head, root)
    _hook_config(root)
    if _head(root) != head or _index_entries(root)[1] != index_digest:
        _fail("INDEX_CHANGED")
    return head, pairs, index_digest, cache


def hook(root=None):
    root = _root(root)
    head, pairs, index_digest, cache = _hook_preflight(root)
    print("SYNC_BASE_SHA=" + head, flush=True)
    environment = os.environ.copy()
    environment["PRE_COMMIT_HOME"] = str(cache)
    environment[SYNC_BASE_ENV] = head
    environment[SYNC_INDEX_ENV] = index_digest
    environment["MDS_NOTEBOOK_SYNC_PYTHON"] = str(Path(sys.executable).absolute())
    environment["PATH"] = str(Path(sys.executable).parent) + os.pathsep + environment.get("PATH", "")
    found = shutil.which("python", path=environment["PATH"])
    if not found or Path(found).resolve() != Path(sys.executable).resolve():
        _fail("INTERPRETER_PROFILE", "hook PATH")
    _checked_path(cache, True)
    cache.mkdir(parents=True, exist_ok=True)
    _checked_path(cache, True)
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-m", "pre_commit", "run", "--hook-stage", "pre-commit"],
        cwd=root, env=environment, shell=False, check=False, timeout=120,
    )
    if _head(root) != head:
        _fail("HEAD_CHANGED")
    if _index_entries(root)[1] != index_digest:
        _fail("INDEX_CHANGED")
    if result.returncode:
        return result.returncode
    check("index", root=root)
    return 0


def hook_check(root=None):
    root = _root(root)
    _hook_identity(root)
    _profile()
    base = _ref(os.environ.get(SYNC_BASE_ENV), root)
    expected_python = os.environ.get("MDS_NOTEBOOK_SYNC_PYTHON")
    if not expected_python or Path(expected_python).resolve() != Path(sys.executable).resolve():
        _fail("INTERPRETER_PROFILE", "inner hook")
    if _head(root) != base:
        _fail("HEAD_CHANGED")
    _, index_digest = _index_entries(root)
    if os.environ.get(SYNC_INDEX_ENV) != index_digest:
        _fail("INDEX_CHANGED")
    result = sync(base, _selected_pairs(root), root)
    if _head(root) != base or _index_entries(root)[1] != index_digest:
        _fail("INDEX_CHANGED")
    if result:
        return result
    return check("index", root=root)


def hook_template(interpreter, root=None):
    root = _root(root)
    interpreter = _checked_path(interpreter, False)
    if not interpreter.is_file():
        _fail("INTERPRETER_PROFILE", "missing executable")
    # Только fixed trusted paths; notebook filenames никогда не идут через shell.
    return (
        "#!/bin/sh\n"
        "# F2 per-command hook; does not install or replace shared hooks.\n"
        "exec " + shlex.quote(str(interpreter)) + " -I -B "
        + shlex.quote(str(root / "tools" / "notebook_sync.py")) + ' hook "$@"\n'
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    checker = commands.add_parser("check")
    snapshots = checker.add_mutually_exclusive_group(required=True)
    snapshots.add_argument("--worktree", action="store_true")
    snapshots.add_argument("--index", action="store_true")
    snapshots.add_argument("--ref")
    synchronizer = commands.add_parser("sync")
    synchronizer.add_argument("--base", required=True)
    synchronizer.add_argument("--pairs", nargs="+", choices=PAIRS, required=True)
    commands.add_parser("hook")
    commands.add_parser("hook-check")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            snapshot = "ref" if args.ref else "index" if args.index else "worktree"
            return check(snapshot, args.ref)
        if args.command == "sync":
            return sync(args.base, args.pairs)
        if args.command == "hook":
            return hook()
        return hook_check()
    except ValueError as error:
        message = str(error)
        if message.split(":", 1)[0] not in ERROR_CODES:
            message = "WORKFLOW_FAILED"
        print("ERROR " + message, file=sys.stderr)
        return 1
    except Exception:
        # Не раскрываем content, credentials, environment или private paths.
        print("ERROR WORKFLOW_FAILED", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
