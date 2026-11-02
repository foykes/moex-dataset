# Development workflow

Repository: [foykes/moex-dataset](https://github.com/foykes/moex-dataset). Default branch: `main`.
Run commands from the repository root. Examples below use PowerShell.

Publication credentials and local fill-in files are described in [CREDENTIALS.md](CREDENTIALS.md).

## Prepared checkout

This checkout contains the full Git history, tags and all remote branches. `main`
tracks `origin/main`; `origin/HEAD` points to `origin/main`.

The local preparation branch includes the existing instructions from
`codex/add-agents-md` ([PR #1](https://github.com/foykes/moex-dataset/pull/1)).
The audit is available separately on `origin/codex/research-export-audit`
([PR #3](https://github.com/foykes/moex-dataset/pull/3)). These open PRs are not
part of upstream `main`; check their current status before relying on them.

Repository-local Git settings:

- `fetch.prune=true` removes obsolete remote-tracking branches on fetch.
- `pull.ff=only` stops a pull when the histories have diverged.
- `push.default=simple` and `push.autoSetupRemote=true` set up tracking for new branches.
- The GitHub credential helper uses the existing `gh` login. No token is stored in project files.

These settings live in `.git/config` and are not versioned. A new clone needs its
own settings. Inspect this checkout with:

```powershell
git status --short --branch
git remote -v
git branch -avv
gh auth status
```

## Start a fix

Check for uncommitted work first. Finish or preserve it before switching branches.

```powershell
git status --short --branch
git fetch --prune origin
git switch main
git pull --ff-only
git switch -c codex/fix-topic
```

Until the preparation instructions are merged into upstream `main`, create a new
working branch from `codex/prepare-workspace` to keep both guides in that branch:

```powershell
git fetch --prune origin
git switch -c codex/fix-topic codex/prepare-workspace
git merge origin/main
```

If upstream has advanced, review any merge conflicts before editing code.
Use a unique name for each task. For simultaneous chats, use a separate worktree
per chat so that checking out a branch in one chat cannot change another chat's
files. If using the Codex worktree tool before the guides are merged, specify
`ref: codex/prepare-workspace` as the starting revision.

## Commit and publish

Review changes and run checks appropriate to the fix. Stage explicit paths so
generated data or unrelated work cannot enter the commit accidentally.

```powershell
git diff --check
git diff --stat
git add path/to/changed-file.py path/to/changed-file.ipynb
git diff --cached
git commit -m "Fix a concrete problem"
git push -u origin HEAD
gh pr create --draft --base main --fill
```

For a detailed PR description, save the exact text in a temporary UTF-8 file and
pass it with `gh pr create --body-file <file>`. Describe the resulting behavior,
validation and any changes to exported columns or files.

Read-only publication check, which does not create or update a remote branch:

```powershell
git push --dry-run origin HEAD
```

If network access fails inside a restricted execution environment, retry the
same command with the environment's authorized network access before changing
credentials. `gh auth status` can report an invalid login when it cannot reach
GitHub. Reauthenticate only after confirming a real authentication failure.

## Python and notebook workflow

`pyproject.toml` configures Jupytext pairing as `ipynb,py:percent`.
`.pre-commit-config.yaml` pins Jupytext to `1.19.2`. Preserve `# %%` cell markers
and the existing Russian code comments and messages.

Install tools in a local virtual environment when starting Python work. Use a
real Python executable, not the Windows Store alias. In a Codex chat the
workspace dependency tool can provide a bundled Python path; otherwise use an
installed Python version.

```powershell
# Replace the path below with the actual Python executable.
& 'C:\path\to\python.exe' -m venv .venv
.\.venv\Scripts\python.exe -m pip install jupytext==1.19.2 pre-commit
.\.venv\Scripts\pre-commit.exe install
```

The repository has no runtime dependency manifest or lockfile. The source imports
`pandas`, `requests`, `beautifulsoup4`, `TA-Lib` and `pygsheets`; HTML/Excel and
notebook conversion also need packages such as `lxml`, `openpyxl` and `nbconvert`.
Select and validate dependency versions for the task before declaring the full
application runnable. Installing packages alone does not supply publication credentials.

Compare the notebook and script before syncing. After editing a Python pair,
explicitly update the notebook from that script:

```powershell
.\.venv\Scripts\jupytext.exe --update --to ipynb data_gathering.py
git diff -- data_gathering.py data_gathering.ipynb
.\.venv\Scripts\pre-commit.exe run --files data_gathering.ipynb
```

The configured hook matches `.ipynb` files; a Python-only edit will not trigger it.
For notebook edits, sync only after confirming which member contains the intended
change. Avoid bulk sync while the authority of any pair is uncertain.

## Safe baseline checks

Syntax compilation does not execute imports, collect data or upload files:

```powershell
.\.venv\Scripts\python.exe -m compileall -q -x '\.venv' .
git diff --check
```

This is a syntax check, not a behavioral test. Existing `main_tests.py` uses
`unittest` and the live MOEX API, and also makes a live call at import time.
`tests.py` is an exploratory script. Add isolated tests with mocked responses for
future bug fixes; do not run broad test discovery against these scripts as an
offline smoke check.

`main.py` converts notebooks into scripts at top level. Running it performs the
collection, indicator, dividend and publication pipeline. Use bounded, task-specific
checks instead of running or importing it for setup validation.

Keep generated datasets and publication credentials out of unrelated commits.
Existing dataset files retain their stored contents; downloading the newest code
does not refresh market data. Production data collection and uploads need their own task.
