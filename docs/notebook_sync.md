# Explicit notebook synchronization — F2-01

Production uses reviewed Python scripts. Importing or running an entrypoint does
not convert or execute a notebook. The source-pair safeguard belongs to the
feature-branch commit workflow and the check-only CI job.

## Supported profile and ownership

Use the verified F1 dev profile: Windows x64, ordinary GIL CPython 3.14.8,
Jupytext 1.19.2, pre-commit 4.3.0 and the existing
`requirements/windows-cp314-dev.lock`. F2 does not update dependencies or install
packages during regression checks. An unavailable or incompatible interpreter
requires a separately authorized setup operation.

The coordinator owns activation and CI setup. The implementation supplies a
tested hook template; it does not install shared hooks or change persistent Git
configuration. A linked worktree can share hooks/configuration with other chats.
Do not run `pre-commit install`, change `core.hooksPath` persistently, or enable
`extensions.worktreeConfig` as part of this slice.

Use one writer and the approved F2 worktree. Its `.f2/` contains task-specific
hooks, cache, temporary repositories and evidence. Existing symlinks, junctions,
hardlinked files and other aliases in service paths must be rejected before
writing or cleaning up. Do not copy credentials into the worktree or fixtures.

## Pair contract and commands

The manifest contains ten pairs: `1year`, `all`, `count_check`, `data_gathering`,
`dividends`, `dohodru_data`, `main_tests`, `tech`, `tests` and `upload`.
Each `.py`/`.ipynb` pair is one ownership zone. F2-01 changes only the five
diagnostic pairs; the five library pairs are checked without rewriting them.

Equivalence compares cell order/type, Python AST, comments, markdown/raw cells
and semantic cell metadata. Code formatting may differ. Empty `tags` are
equivalent to absent tags; `lines_to_next_cell` is formatting. Outputs, execution
counts, cell IDs and notebook tool metadata are not source authority. Other cell
metadata must survive export or cause a failure. The whole exported script must
also parse as Python. No cell is executed.

Run these commands using the exact approved interpreter, from the F2 root:

```powershell
& $taskPython -I -B tools/notebook_sync.py check --worktree
if ($LASTEXITCODE -ne 0) { throw "Worktree pair check failed" }

& $taskPython -I -B tools/notebook_sync.py check --index
if ($LASTEXITCODE -ne 0) { throw "Index pair check failed" }

& $taskPython -I -B tools/notebook_sync.py check --ref $taskHeadSha
if ($LASTEXITCODE -ne 0) { throw "Exact-HEAD pair check failed" }
```

`--ref` requires a full commit SHA and reads committed blobs. It does not silently
substitute the dirty worktree or index. The target commit must be available in
the local object database.

Manual notebook-to-script export is explicit and restricted to a `codex/`
feature branch:

```powershell
$taskSyncBase = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot resolve sync base" }
& $taskPython -I -B tools/notebook_sync.py sync --base $taskSyncBase --pairs all
if ($LASTEXITCODE -ne 0) { throw "Review export result and restage before continuing" }
```

An export returns `RESTAGE_REQUIRED`/nonzero even when the generated script was
written successfully. Review the diff and explicitly stage both members. A
coherent pair is a no-op. A divergent Python source changed since the coherent
base produces `SOURCE_CONFLICT`; neither side is overwritten. For a Python-first
fix, compare both members and deliberately transfer the intended change into
the notebook before exporting. There is no mtime authority or force overwrite.

All candidates are validated before writes. Inputs are checked again before
replacement. Replacement is atomic per script, not across the batch. A partial
filesystem failure remains blocked until the author checks and restores the
pairs; it must not be treated as a successful sync.

## Coordinator activation and commit/restaging

Before setup, inspect the active hook directory and any configured hooks path.
An existing foreign hook requires an agreed preservation/chain decision before
using an override. Do not silently bypass it.

The coordinator prepares `.f2/hooks/pre-commit` from
`tools.notebook_sync.hook_template(exact_interpreter, root=exact_f2_root)`. This
function returns the template and writes nothing. Save it as UTF-8 with LF
endings only after validating the F2 root, hook directory and destination;
existing aliases or an existing hook must not be replaced automatically. Record
the actual interpreter, template hash, hook root and setup owner in the handoff.
The template invokes the exact interpreter with `-I -B`, with no PATH fallback.

Activate it for each authorized commit command:

```powershell
git -c "core.hooksPath=$taskHookRoot" commit -m "Describe the approved slice"
if ($LASTEXITCODE -ne 0) { throw "Commit gate failed; inspect diagnostics and restage" }
```

`$taskHookRoot` is the validated absolute F2 `.f2/hooks` path. This command makes
Git invoke the hook automatically. A plain `git commit` is not claimed to be
protected by F2. Persistent merge/deployment enforcement remains a coordinator/F3
hookup.

Use an ordinary staged commit from the owning worktree. The hook rejects a
different invocation root or alternate index, including `--only`/`--include`
commit profiles. Manual `check`/`sync` can still run from another CWD because they
read the explicit tool root; that capability does not relax hook identity.

The external `hook` command runs preflight before pre-commit can stash anything:

1. Capture the current coherent `HEAD` as `SYNC_BASE_SHA`. This is refreshed for
   every commit attempt and differs from the original branch `BASE_SHA`.
2. Expand staged notebook/script changes to both members. Reject missing,
   deleted, renamed, unmerged, untracked or intent-to-add members.
3. Reject unstaged changes in either selected member or a hook-control source.
   Only CRLF/LF normalization is allowed; raw worktree bytes are preserved.
4. Reject bypass settings, including nonempty `SKIP`. Validate the interpreter,
   configuration and `.f2/cache/precommit` before cache writes.
5. Run pinned pre-commit with the local `system` hook. It installs no hook
   environment. The internal `hook-check` receives the captured HEAD/index;
   a changed HEAD or index fails instead of exporting from a different snapshot.
6. Accept coherent pairs first. Export admissible notebook-only changes and
   return `RESTAGE_REQUIRED`; the index and HEAD are not changed by the tool.
7. After explicit staging of both members, retry the same authorized commit.
   The coherent pair passes and all ten index pairs are checked.

The first commit attempt with `N1/P0` therefore fails with worktree `N1/P1` and
index `N1/P0`. Restaging creates a commit containing `N1/P1`. A second notebook
update uses that new commit as its base; it does not compare against the original
fork revision. No automatic staging or committing occurs.

## Offline verification and CI

The approved regression runner uses only `tests/f2`, task-specific cache/temp
roots and disabled third-party pytest autoload. It does not install dependencies
or permit network, production writers or credential reads. Real disposable Git
repositories use local fixture identity/signing settings and the same
per-command hook activation. They do not mutate shared configuration.

```powershell
$taskBaseTemp = ".f2/tmp/pytest-" + [guid]::NewGuid().ToString("N")
& $taskPython -I -B -m pytest -c pyproject.toml --confcutdir tests/f2 tests/f2 -q --basetemp $taskBaseTemp -o cache_dir=.f2/cache/pytest
if ($LASTEXITCODE -ne 0) { throw "F2 regression failed" }
```

Use a fresh basetemp for each run. Pytest can leave its own `*current` directory
links in a previous run; do not broaden cleanup through those aliases or reuse
the old tree as the next run's disposable boundary.

Some restricted Windows launchers can refuse Git Bash/MSYS named-object access
before a hook starts. That nonzero is an execution limitation, not evidence that
the intended hook rejected a pair. Use the separately authorized test profile
and require the expected diagnostic. Converter dependency imports can also emit
Jupyter's `platformdirs` deprecation warning; record it rather than updating
locks solely to suppress it. Neither warning proves notebook execution.

Regression evidence includes actual commit/index blobs, raw unstaged bytes,
per-hook base SHAs, exit codes and negative controls. A sentinel/exit code alone
does not establish source preservation. Tests include two successive notebook
commits, restaging, partial-staging refusal before stash/cache, conflicts,
invalid members, converter failure, exact refs and alias canaries.

Disposable tool copies receive a test-only guard bootstrap before their imports;
the original production body is preserved and its hash recorded. Audit hooks do
not inherit into child processes automatically. The separate pre-commit
dispatcher is constrained by the validated local/system/no-install
configuration; fixture evidence does not claim inherited process guards there.

The narrow CI job uses `windows-2025` x64 and the same F1 dev lock. Setup prepares
CPython 3.14.8 and dependencies separately from the offline phase. The checkout
is the actual PR head or push SHA; it is checked against `rev-parse HEAD`.
CI then runs the committed pair check and F2 regressions. Every native nonzero
stops the runner. CI never syncs or commits the checked-out source tree.

The job activates only when the coordinator has separately approved CI setup
and set the repository variable `MDS_F2_CI_SETUP_APPROVED` to `true`. F2 does not
set that variable. A PR push therefore does not silently authorize new setup
downloads. A skipped job is `NOT VERIFIED`, not a passed acceptance check. The
coordinator/F3 must enforce the gate when activated; adding this conditional job
does not itself establish required merge or deployment checks.

F2-01 does not complete the runtime-root contract, #4 incremental-path fix, #5
cold-start signature, shared logging or deployment enforcement. Those remain
F2-02, A/#5, F-LOG and coordinator/F3 work. Safe imports do not authorize live
execution of diagnostic wrappers. PRs use `Refs`; full issue closure and
production readiness require separate acceptance.
