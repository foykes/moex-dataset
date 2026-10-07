"""Independent source oracles and real disposable Git/pre-commit regressions."""

import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest
from _probe import offline_guard


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOOL_PATH = PROJECT_ROOT / "tools" / "notebook_sync.py"
GIT = shutil.which("git")
PAIR_NAMES = (
    "1year", "all", "count_check", "data_gathering", "dividends",
    "dohodru_data", "main_tests", "tech", "tests", "upload",
)


def _load_tool():
    specification = importlib.util.spec_from_file_location("f2_sync_under_test", TOOL_PATH)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    assert len(module.PAIRS) == len(set(module.PAIRS))
    assert set(module.PAIRS) == set(PAIR_NAMES)
    assert all((PROJECT_ROOT / (stem + suffix)).is_file()
               for stem in PAIR_NAMES for suffix in ('.py', '.ipynb'))
    return module


def _notebook(value=0, *, source=None, metadata=None, execution_count=None):
    # Oracle не использует converter: notebook и script заданы независимо.
    if source is None:
        source = f"# Независимый контроль источника\nvalue = {value}\n"
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"jupytext": {"formats": "ipynb,py:percent"}},
        "cells": [{
            "cell_type": "code", "id": "independent-fixture-cell",
            "metadata": metadata or {}, "source": source,
            "execution_count": execution_count, "outputs": [],
        }],
    }


def _write_notebook(path, value=0, **kwargs):
    content = json.dumps(_notebook(value, **kwargs), ensure_ascii=False, indent=2) + "\n"
    path.write_text(content, encoding="utf-8", newline="")


def _script(value=0):
    return f"# %%\n# Независимый контроль источника\nvalue = {value}\n"


def _value(source):
    tree = ast.parse(source)
    assignments = [
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "value" for target in node.targets)
    ]
    assert len(assignments) == 1
    return ast.literal_eval(assignments[0].value)


def _notebook_value(raw):
    notebook = json.loads(raw)
    assert len(notebook["cells"]) == 1
    return _value("".join(notebook["cells"][0]["source"]))


def _environment(repository):
    environment = dict(os.environ)
    for name in (
        "SKIP", "PRE_COMMIT_ALLOW_NO_CONFIG", "GIT_INDEX_FILE", "GIT_DIR",
        "GIT_WORK_TREE", "GIT_CONFIG_COUNT", "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES", "PYTEST_ADDOPTS", "PYTEST_PLUGINS",
    ):
        environment.pop(name, None)
    temporary = repository / ".f2" / "tmp"
    temporary.mkdir(parents=True, exist_ok=True)
    environment.update({
        "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
        "PRE_COMMIT_HOME": str(repository / ".f2" / "cache" / "precommit"),
        "PYTHONDONTWRITEBYTECODE": "1", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "TMP": str(temporary), "TEMP": str(temporary),
    })
    return environment


def _run(repository, arguments, *, input_bytes=None, expected=None, cwd=None, environment=None):
    result = subprocess.run(
        arguments, cwd=cwd or repository,
        env=environment or _environment(repository), input=input_bytes,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False,
    )
    if expected is not None:
        assert result.returncode == expected, _output(result)
    return result


def _output(result):
    return (result.stdout + result.stderr).decode("utf-8", errors="replace")


def _git(repository, *arguments, expected=0, input_bytes=None):
    assert GIT is not None, "The approved fixture profile needs Git, without installing it"
    return _run(repository, [GIT, *arguments], input_bytes=input_bytes, expected=expected)


def _cli(repository, *arguments, expected=None, cwd=None):
    return _run(
        repository, [sys.executable, "-I", "-B", str(repository / "tools" / "notebook_sync.py"), *arguments],
        expected=expected, cwd=cwd,
    )


def _head(repository):
    return _git(repository, "rev-parse", "HEAD").stdout.decode("ascii").strip()


def _blob(repository, revision, filename):
    return _git(repository, "show", f"{revision}:{filename}").stdout


def _state(repository, stem="all"):
    return {
        "head": _head(repository),
        "index": _git(repository, "ls-files", "--stage").stdout,
        "staged": _git(repository, "diff", "--cached", "--binary", "--no-ext-diff").stdout,
        "raw": {
            suffix: (repository / f"{stem}.{suffix}").read_bytes()
            if (repository / f"{stem}.{suffix}").exists() else None
            for suffix in ("py", "ipynb")
        },
    }


def _commit(repository, message, expected=None):
    return _git(
        repository, "-c", f"core.hooksPath={repository / '.f2' / 'hooks'}",
        "commit", "-m", message, expected=expected,
    )


@pytest.fixture
def tool():
    return _load_tool()


@pytest.fixture
def repository(tmp_path, tool, record_property):
    repository = tmp_path / "Репозиторий с пробелами"
    repository.mkdir()
    (repository / "tools").mkdir()
    original_body = TOOL_PATH.read_bytes()
    tree = ast.parse(original_body)
    insertion_line = 0
    for node in tree.body:
        is_docstring = isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        is_future = isinstance(node, ast.ImportFrom) and node.module == "__future__"
        if is_docstring or is_future:
            insertion_line = node.end_lineno
        else:
            break
    bootstrap = offline_guard().fixture_bootstrap_source(PROJECT_ROOT, lane='f2', role='f2-notebook').encode('utf-8')
    original_lines = original_body.splitlines(keepends=True)
    instrumented = b"".join(original_lines[:insertion_line]) + bootstrap + b"".join(original_lines[insertion_line:])
    assert instrumented.replace(bootstrap, b"", 1) == original_body
    (repository / "tools" / "notebook_sync.py").write_bytes(instrumented)
    record_property("fixture_tool_body_sha256", hashlib.sha256(original_body).hexdigest())
    record_property("fixture_instrumentation", "test-only early child guard; production body unchanged")
    shutil.copyfile(PROJECT_ROOT / ".pre-commit-config.yaml", repository / ".pre-commit-config.yaml")
    shutil.copyfile(PROJECT_ROOT / "pyproject.toml", repository / "pyproject.toml")
    (repository / ".gitignore").write_text(".f2/\n", encoding="utf-8")
    (repository / "unrelated.txt").write_bytes(b"original unrelated content\n")
    for stem in PAIR_NAMES:
        _write_notebook(repository / f"{stem}.ipynb")
        (repository / f"{stem}.py").write_text(_script(), encoding="utf-8", newline="")
    _git(repository, "init", "--initial-branch=codex/fixture")
    for key, value in (
        ("user.name", "F2 disposable fixture"), ("user.email", "fixture@example.invalid"),
        ("commit.gpgsign", "false"), ("core.autocrlf", "false"),
    ):
        _git(repository, "config", "--local", key, value)
    _git(repository, "add", "--", ".")
    _git(repository, "-c", f"core.hooksPath={repository / '.f2' / 'empty-hooks'}", "commit", "-m", "N0 P0 baseline")
    hooks = repository / ".f2" / "hooks"
    hooks.mkdir(parents=True)
    hook = hooks / "pre-commit"
    hook.write_text(tool.hook_template(sys.executable, root=repository), encoding="utf-8", newline="\n")
    hook.chmod(0o700)
    return repository


def test_real_commit_requires_restage_and_refreshes_head_for_second_update(repository, record_property):
    baseline = _head(repository)
    unrelated = b"unstaged unrelated content survives both cycles\n"
    (repository / "unrelated.txt").write_bytes(unrelated)
    observed_heads = [baseline]
    actual_sync_bases = []
    committed_pair_blobs = []
    export_index_oracles = []

    for value in (1, 2):
        previous_head = _head(repository)
        _write_notebook(repository / "all.ipynb", value)
        _git(repository, "add", "--", "all.ipynb")
        attempted = _commit(repository, f"N{value} P{value} without restaging")
        assert attempted.returncode != 0
        assert "RESTAGE_REQUIRED" in _output(attempted)
        assert previous_head in _output(attempted)
        actual_sync_bases.extend(re.findall(r"SYNC_BASE_SHA=([0-9a-f]{40})", _output(attempted)))
        assert _head(repository) == previous_head
        assert _notebook_value(_blob(repository, "", "all.ipynb")) == value
        assert _value(_blob(repository, "", "all.py").decode("utf-8")) == value - 1
        assert _value((repository / "all.py").read_text(encoding="utf-8")) == value
        assert (repository / "unrelated.txt").read_bytes() == unrelated
        export_index_oracles.append({
            "head": _head(repository), "notebook_value": _notebook_value(_blob(repository, "", "all.ipynb")),
            "index_script_value": _value(_blob(repository, "", "all.py").decode("utf-8")),
            "worktree_script_value": _value((repository / "all.py").read_text(encoding="utf-8")),
            "notebook_blob": _git(repository, "rev-parse", ":all.ipynb").stdout.decode("ascii").strip(),
            "script_blob": _git(repository, "rev-parse", ":all.py").stdout.decode("ascii").strip(),
        })

        _git(repository, "add", "--", "all.ipynb", "all.py")
        committed = _commit(repository, f"N{value} P{value} after explicit restaging", expected=0)
        actual_sync_bases.extend(re.findall(r"SYNC_BASE_SHA=([0-9a-f]{40})", _output(committed)))
        current_head = _head(repository)
        assert current_head != previous_head
        assert _notebook_value(_blob(repository, current_head, "all.ipynb")) == value
        assert _value(_blob(repository, current_head, "all.py").decode("utf-8")) == value
        assert (repository / "unrelated.txt").read_bytes() == unrelated
        observed_heads.append(current_head)
        committed_pair_blobs.append({
            "commit": current_head, "oracle_value": value,
            "notebook_blob": _git(repository, "rev-parse", f"{current_head}:all.ipynb").stdout.decode("ascii").strip(),
            "script_blob": _git(repository, "rev-parse", f"{current_head}:all.py").stdout.decode("ascii").strip(),
        })

    assert len(set(observed_heads)) == 3
    assert actual_sync_bases == [observed_heads[0], observed_heads[0], observed_heads[1], observed_heads[1]]
    record_property("actual_git_heads", observed_heads)
    record_property("actual_sync_base_shas", actual_sync_bases)
    record_property("export_index_oracles", export_index_oracles)
    record_property("committed_pair_blobs", committed_pair_blobs)
    record_property("unrelated_unstaged_sha256", hashlib.sha256(unrelated).hexdigest())
    _cli(repository, "check", "--index", expected=0)
    _cli(repository, "check", "--ref", observed_heads[-1], expected=0)


@pytest.mark.parametrize("unstaged_member", ["ipynb", "py"])
def test_partial_pair_refused_before_precommit_stash_or_cache(repository, unstaged_member):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    if unstaged_member == "ipynb":
        _write_notebook(repository / "all.ipynb", 2)
    else:
        (repository / "all.py").write_text(_script(9), encoding="utf-8")
    before = _state(repository)
    attempted = _commit(repository, "ambiguous partially staged pair")
    assert attempted.returncode != 0
    assert "PARTIAL_PAIR" in _output(attempted)
    assert _state(repository) == before
    assert not (repository / ".f2" / "cache" / "precommit").exists()
    assert "Stashing unstaged files" not in _output(attempted)


@pytest.mark.parametrize("notebook_changed", [False, True])
def test_divergent_python_change_never_overwrites_sources(repository, notebook_changed):
    if notebook_changed:
        _write_notebook(repository / "all.ipynb", 1)
    (repository / "all.py").write_text(_script(7), encoding="utf-8")
    _git(repository, "add", "--", "all.py", "all.ipynb")
    before = _state(repository)
    attempted = _commit(repository, "divergent Python change")
    assert attempted.returncode != 0
    assert "SOURCE_CONFLICT" in _output(attempted)
    assert _state(repository) == before


@pytest.mark.parametrize("member_state", ["deleted", "missing", "untracked", "intent", "renamed", "unmerged"])
def test_invalid_pair_members_refuse_without_writes(repository, member_state):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    if member_state == "deleted":
        _git(repository, "rm", "--", "all.py")
    elif member_state == "missing":
        (repository / "all.py").unlink()
    elif member_state in {"untracked", "intent"}:
        _git(repository, "rm", "--cached", "--", "all.py")
        if member_state == "intent":
            _git(repository, "add", "--intent-to-add", "--", "all.py")
    elif member_state == "renamed":
        _git(repository, "mv", "--", "all.py", "renamed.py")
    else:
        blob = _git(repository, "rev-parse", "HEAD:all.py").stdout.decode("ascii").strip()
        instructions = "0 " + "0" * 40 + "\tall.py\n"
        instructions += "".join(f"100644 {blob} {stage}\tall.py\n" for stage in (1, 2, 3))
        _git(repository, "update-index", "--index-info", input_bytes=instructions.encode("ascii"))
    before = _state(repository)
    attempted = _commit(repository, f"invalid {member_state} pair")
    assert attempted.returncode != 0
    if member_state == "unmerged":
        # Git сам запрещает такой commit до запуска pre-commit hook.
        assert "unmerged files" in _output(attempted)
        assert "SYNC_BASE_SHA=" not in _output(attempted)
        direct_hook = _cli(repository, "hook")
        assert direct_hook.returncode != 0
        assert "PAIR_MEMBER_STATE" in _output(direct_hook)
    else:
        assert "PAIR_MEMBER_STATE" in _output(attempted)
    assert _state(repository) == before
    assert not (repository / ".f2" / "cache" / "precommit").exists()


def test_crlf_only_unstaged_difference_is_allowed_and_preserved(repository):
    _write_notebook(repository / "all.ipynb", execution_count=17)
    _git(repository, "add", "--", "all.ipynb")
    script = repository / "all.py"
    crlf_bytes = script.read_bytes().replace(b"\n", b"\r\n")
    script.write_bytes(crlf_bytes)
    _commit(repository, "non-source metadata with CRLF working tree", expected=0)
    assert script.read_bytes() == crlf_bytes
    assert _value(_blob(repository, "HEAD", "all.py").decode("utf-8")) == 0


def test_empty_hook_cache_never_installs_dependencies(repository):
    cache = repository / ".f2" / "cache" / "precommit"
    assert not cache.exists()
    _write_notebook(repository / "all.ipynb", execution_count=11)
    _git(repository, "add", "--", "all.ipynb")
    result = _commit(repository, "first local system hook", expected=0)
    assert cache.is_dir()
    assert "Installing environment" not in _output(result)
    assert "Initializing environment" not in _output(result)
    assert not list(cache.glob("repo*"))


def test_skip_bypass_refused_without_source_or_cache_writes(repository):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    environment = _environment(repository)
    environment["SKIP"] = "notebook-sync"
    before = _state(repository)
    result = _run(repository, [sys.executable, "-I", "-B", str(repository / "tools" / "notebook_sync.py"), "hook"], environment=environment)
    assert result.returncode != 0
    assert "BYPASS_REJECTED" in _output(result)
    assert _state(repository) == before
    assert not (repository / ".f2" / "cache" / "precommit").exists()


def test_fixed_hook_refuses_foreign_sibling_repository_without_writes(repository, tmp_path):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    foreign = tmp_path / "Чужой соседний fixture"
    foreign.mkdir()
    (foreign / ".gitignore").write_text(".f2/\n", encoding="utf-8")
    canary = foreign / "sentinel.txt"
    canary.write_bytes(b"foreign repository must not be changed\n")
    _git(foreign, "init", "--initial-branch=codex/foreign-fixture")
    _git(foreign, "config", "--local", "user.name", "F2 foreign fixture")
    _git(foreign, "config", "--local", "user.email", "foreign-fixture@example.invalid")
    _git(foreign, "config", "--local", "commit.gpgsign", "false")
    _git(foreign, "add", "--", ".gitignore", "sentinel.txt")
    _git(foreign, "-c", f"core.hooksPath={foreign / '.f2' / 'empty-hooks'}", "commit", "-m", "foreign baseline")
    before = _state(repository)
    foreign_before = (_head(foreign), _git(foreign, "ls-files", "--stage").stdout, canary.read_bytes())
    attempted = _cli(repository, "hook", cwd=foreign)
    assert attempted.returncode != 0
    assert "HOOK_IDENTITY: invocation root" in _output(attempted)
    assert _state(repository) == before
    assert (_head(foreign), _git(foreign, "ls-files", "--stage").stdout, canary.read_bytes()) == foreign_before
    assert not (repository / ".f2" / "cache" / "precommit").exists()
    assert not (foreign / ".f2" / "cache").exists()


def test_hook_refuses_explicit_foreign_index_without_writes(repository, tmp_path):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    foreign_index = tmp_path / "foreign-index-canary"
    shutil.copyfile(repository / ".git" / "index", foreign_index)
    index_bytes = foreign_index.read_bytes()
    before = _state(repository)
    environment = _environment(repository)
    environment["GIT_INDEX_FILE"] = str(foreign_index)
    attempted = _run(
        repository,
        [sys.executable, "-I", "-B", str(repository / "tools" / "notebook_sync.py"), "hook"],
        environment=environment,
    )
    assert attempted.returncode != 0
    assert "HOOK_IDENTITY: alternate index" in _output(attempted)
    assert _state(repository) == before
    assert foreign_index.read_bytes() == index_bytes
    assert not (repository / ".f2" / "cache" / "precommit").exists()


def test_exact_ref_and_index_are_not_replaced_by_dirty_worktree(repository, tmp_path):
    baseline = _head(repository)
    _write_notebook(repository / "all.ipynb", 4)
    foreign = tmp_path / "Другой CWD"
    foreign.mkdir()
    _cli(repository, "check", "--ref", baseline, cwd=foreign, expected=0)
    _cli(repository, "check", "--index", cwd=foreign, expected=0)
    dirty = _cli(repository, "check", "--worktree", cwd=foreign)
    assert dirty.returncode != 0
    assert "PAIR_MISMATCH" in _output(dirty)
    _git(repository, "add", "--", "all.ipynb")
    staged = _cli(repository, "check", "--index", cwd=foreign)
    assert staged.returncode != 0
    assert "PAIR_MISMATCH" in _output(staged)
    _cli(repository, "check", "--ref", baseline, cwd=foreign, expected=0)


def test_manual_sync_exports_without_execution_then_is_noop(repository):
    baseline = _head(repository)
    source = '# Не исполнять fixture\nraise RuntimeError("MUST_NOT_EXECUTE")\n'
    _write_notebook(repository / "all.ipynb", source=source)
    notebook_bytes = (repository / "all.ipynb").read_bytes()
    exported = _cli(repository, "sync", "--base", baseline, "--pairs", "all")
    assert exported.returncode != 0
    assert "RESTAGE_REQUIRED" in _output(exported)
    script = (repository / "all.py").read_text(encoding="utf-8")
    assert ast.dump(ast.parse(script), include_attributes=False) == ast.dump(ast.parse(source), include_attributes=False)
    assert "# Не исполнять fixture" in script
    assert (repository / "all.ipynb").read_bytes() == notebook_bytes
    _cli(repository, "sync", "--base", baseline, "--pairs", "all", expected=0)


def test_cold_converter_import_does_not_launch_optional_probe_and_restores_popen(repository):
    launcher = repository / ".f2" / "tmp" / "cold_converter_import.py"
    launcher.write_text(
        offline_guard().fixture_bootstrap_source(PROJECT_ROOT, lane='f2', role='f2-script') +
        "import importlib.util\n"
        "import subprocess\n"
        "import sys\n"
        "spec = importlib.util.spec_from_file_location('cold_f2_sync', sys.argv[1])\n"
        "tool = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(tool)\n"
        "attempts = []\n"
        "original_class = subprocess.Popen\n"
        "def forbid_real_launch(self, *args, **kwargs):\n"
        "    attempts.append('external child attempted')\n"
        "    raise RuntimeError('REAL_EXTERNAL_PROCESS_MUST_NOT_START')\n"
        "subprocess.Popen.__init__ = forbid_real_launch\n"
        "converter = tool.get_jupytext()\n"
        "assert callable(converter.reads)\n"
        "assert not attempts\n"
        "assert subprocess.Popen is original_class\n"
        "assert subprocess.Popen.__init__ is forbid_real_launch\n"
        "print('CONVERTER_IMPORT_NO_EXTERNAL_PROCESS_POPEN_RESTORED')\n",
        encoding="utf-8",
    )
    offline_guard().register_fixture_script(
        launcher, [str(repository / "tools" / "notebook_sync.py")])
    result = _run(
        repository,
        [sys.executable, "-I", "-B", str(launcher), str(repository / "tools" / "notebook_sync.py")],
        expected=0,
    )
    assert "CONVERTER_IMPORT_NO_EXTERNAL_PROCESS_POPEN_RESTORED" in _output(result)


def test_converter_failure_validates_whole_batch_before_any_write(repository, tool, monkeypatch):
    for stem in ("all", "tests"):
        _write_notebook(repository / f"{stem}.ipynb", 5)
    original = {
        filename: (repository / filename).read_bytes()
        for stem in ("all", "tests") for filename in (f"{stem}.py", f"{stem}.ipynb")
    }
    real_export = tool.notebook_export
    candidate_attempts = []

    def fail_second_candidate(notebook_text):
        if _notebook_value(notebook_text) == 5:
            candidate_attempts.append(notebook_text)
            if len(candidate_attempts) == 2:
                raise RuntimeError("bounded injected converter failure")
        return real_export(notebook_text)

    monkeypatch.setattr(tool, "notebook_export", fail_second_candidate)
    with pytest.raises((ValueError, RuntimeError)):
        tool.sync(_head(repository), ["all", "tests"], root=repository)
    assert len(candidate_attempts) == 2
    assert {filename: (repository / filename).read_bytes() for filename in original} == original


@pytest.mark.parametrize("failure", ["missing", "version"])
def test_missing_or_wrong_converter_fails_without_writes(repository, tool, monkeypatch, failure):
    _write_notebook(repository / "all.ipynb", 3)
    before = _state(repository)

    def wrong_version(distribution):
        if distribution == "jupytext":
            if failure == "missing":
                raise tool.metadata.PackageNotFoundError("jupytext")
            return "0.0.0"
        return "4.3.0"

    monkeypatch.setattr(tool.metadata, "version", wrong_version)
    with pytest.raises(ValueError, match="MISSING_TOOL|TOOL_VERSION"):
        tool.sync(_head(repository), ["all"], root=repository)
    assert _state(repository) == before


@pytest.mark.parametrize("bad_notebook", ["not json", '{"nbformat": 4, "cells": false}', "invalid_python"])
def test_malformed_json_or_ast_preserves_source_before_export(repository, bad_notebook):
    if bad_notebook == "invalid_python":
        _write_notebook(repository / "all.ipynb", source="if incomplete:\n")
    else:
        (repository / "all.ipynb").write_text(bad_notebook, encoding="utf-8")
    before = _state(repository)
    result = _cli(repository, "sync", "--base", _head(repository), "--pairs", "all")
    assert result.returncode != 0
    assert any(code in _output(result) for code in ("CONVERTER_FAILED", "UNSUPPORTED_PAIR"))
    assert _state(repository) == before


def test_source_comparison_accepts_formatting_but_detects_lost_comment_and_cell_order(tool):
    original = json.dumps(_notebook(source="# Контроль смысла\nvalue = 1 + 2\n"), ensure_ascii=False)
    formatting = "# %%\n# Контроль смысла\nvalue=1+2\n"
    assert tool.source_signature(original, "ipynb") == tool.source_signature(formatting, "py:percent")
    without_comment = "# %%\nvalue = 1 + 2\n"
    assert tool.source_signature(original, "ipynb") != tool.source_signature(without_comment, "py:percent")
    ordered = "# %%\nvalue = 1\n# %%\nother = 2\n"
    reordered = "# %%\nother = 2\n# %%\nvalue = 1\n"
    merged = "# %%\nvalue = 1\nother = 2\n"
    assert tool.source_signature(ordered, "py:percent") != tool.source_signature(reordered, "py:percent")
    assert tool.source_signature(ordered, "py:percent") != tool.source_signature(merged, "py:percent")


def test_source_metadata_contract_keeps_semantics_and_ignores_execution_format(tool):
    plain = _notebook()
    non_source = _notebook(metadata={"tags": [], "lines_to_next_cell": 3}, execution_count=42)
    non_source["cells"][0]["id"] = "another-execution-cell-id"
    non_source["cells"][0]["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "old output"}]
    assert tool.source_signature(json.dumps(plain), "ipynb") == tool.source_signature(json.dumps(non_source), "ipynb")
    for metadata in ({"tags": ["approved-tag"]}, {"title": "Title"}, {"name": "named-cell"}):
        semantic = _notebook(metadata=metadata)
        assert tool.source_signature(json.dumps(plain), "ipynb") != tool.source_signature(json.dumps(semantic), "ipynb")
    other_semantic = _notebook(metadata={"unrecognized_contract": {"execute": "different"}})
    assert tool.source_signature(json.dumps(plain), "ipynb") != tool.source_signature(json.dumps(other_semantic), "ipynb")
    # Иные metadata должны либо сохраняться при roundtrip, либо блокировать
    # export; молча добавлять их в ignore-list нельзя.
    try:
        exported = tool.notebook_export(json.dumps(other_semantic))
    except ValueError:
        pass
    else:
        assert tool.source_signature(json.dumps(other_semantic), "ipynb") == tool.source_signature(exported, "py:percent")


def test_partial_hook_control_source_refused_before_cache(repository):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    control = repository / ".pre-commit-config.yaml"
    control.write_bytes(control.read_bytes() + b"\n# unstaged controller change\n")
    before = _state(repository)
    raw_control = control.read_bytes()
    attempted = _commit(repository, "partially staged hook controller")
    assert attempted.returncode != 0
    assert "CONTROL_SOURCE_UNSTAGED" in _output(attempted)
    assert _state(repository) == before
    assert control.read_bytes() == raw_control
    assert not (repository / ".f2" / "cache" / "precommit").exists()


def test_native_precommit_nonzero_does_not_reach_success_marker(repository, tool, monkeypatch):
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    before = _state(repository)
    monkeypatch.chdir(repository)
    marker = repository / ".f2" / "next-stage-marker"
    real_run = tool.subprocess.run
    real_check = tool.check

    def controlled_run(arguments, **kwargs):
        if "pre_commit" in arguments:
            return subprocess.CompletedProcess(arguments, 19)
        return real_run(arguments, **kwargs)

    def observed_check(snapshot="worktree", ref=None, root=None):
        if snapshot == "index":
            marker.write_text("next stage reached after failure", encoding="utf-8")
        return real_check(snapshot, ref, root)

    monkeypatch.setattr(tool.subprocess, "run", controlled_run)
    monkeypatch.setattr(tool, "check", observed_check)
    assert tool.hook(root=repository) == 19
    assert not marker.exists()
    assert _state(repository) == before


def test_source_hardlink_alias_refuses_and_preserves_external_sentinel(repository, tmp_path):
    external = tmp_path / "outside-source-canary.py"
    external.write_bytes((repository / "all.py").read_bytes())
    (repository / "all.py").unlink()
    os.link(external, repository / "all.py")
    before = external.read_bytes()
    result = _cli(repository, "check", "--worktree")
    assert result.returncode != 0
    assert "UNSAFE_PATH" in _output(result)
    assert external.read_bytes() == before
    assert (repository / "all.py").stat().st_nlink == 2


def test_cache_directory_alias_refuses_before_writes(repository, tmp_path):
    external = tmp_path / "outside-cache-canary"
    external.mkdir()
    sentinel = external / "sentinel.txt"
    sentinel.write_bytes(b"must survive unsafe cache refusal\n")
    cache_parent = repository / ".f2" / "cache"
    try:
        cache_parent.symlink_to(external, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"Directory symlink unavailable in this fixture profile: {error.winerror}")
    _write_notebook(repository / "all.ipynb", 1)
    _git(repository, "add", "--", "all.ipynb")
    before = _state(repository)
    result = _commit(repository, "unsafe cache alias")
    assert result.returncode != 0
    assert "UNSAFE_PATH" in _output(result)
    assert _state(repository) == before
    assert sentinel.read_bytes() == b"must survive unsafe cache refusal\n"
    assert list(external.iterdir()) == [sentinel]
    assert cache_parent.is_symlink()
