# Offline tests — F3-01

Use a verified immutable Windows CPython 3.14.8 dev environment from the existing
hash-locked manifests. This runner never installs dependencies. `python` and the
console `pytest` must refer to that same interpreter. Set
`PYTHONDONTWRITEBYTECODE=1`, `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`; remove inherited
`PYTEST_ADDOPTS` and `PYTEST_PLUGINS`. Run from the owning source worktree.

## Fixed lanes and actual commands

```text
python -I -B -X utf8 tools/offline_tests.py --lane f1
python -I -B -X utf8 tools/offline_tests.py --lane f2
python -I -B -X utf8 tools/offline_tests.py --lane flog
python -I -B -X utf8 tools/offline_tests.py --lane f3
```

Each lane has a separate process and native collection boundary. `--collect-only`
is available on each lane. It records inventory, not successful runtime tests.
`--legacy-collection` is F3-only and already collection-only; combining it with
`--collect-only` is invalid. Targets and arbitrary pytest options are not forwarded.
F1 requires the environment to belong to this worktree's `.f1/python`; a foreign
immutable environment can exercise other lanes but does not satisfy that assertion.

| Actual command in the prepared environment | Contract |
|---|---|
| `pytest -q`, `python -m pytest -q` | Substantive deterministic shared tests |
| `python -m unittest discover` | Root adapter runs those substantive assertions; failure propagates |
| `pytest --collect-only -q tests.py main_tests.py` and module equivalent | Two original `main_tests` node IDs; no live runtime |
| `pytest -q tests.py main_tests.py` and module equivalent | Two explained skips; no live readiness claim |
| `pytest [--collect-only] -q tests.py` and module equivalent | Zero tests, exit 5; not PASS |
| `pytest [--collect-only] -q main_tests.py` and module equivalent | Two nodes at collection, two skips at runtime |
| `python -m unittest main_tests` | Two skips; never invokes `main()` |
| `python -I -B -X utf8 tools/offline_tests.py --lane f3 --legacy-collection` | Separate legacy inventory evidence |
| `python -I -B -X utf8 tools/offline_tests.py --notebook-check --ref <HEAD>` | Read-only all-pair check on the current clean full SHA |

`offline` means deterministic fixtures; `live_read` classifies the CLI spelling
`live-read`; `staging` classifies staging diagnostics. All are registered with
strict markers. Unknown decorators fail collection. Unknown `-m` names confer no
permission; empty selection is exit 5. Imported live unittest classes always
remain skipped, regardless of flags, environment or marker selection.

These actual console/module commands are checked independently of `pytest.main`
inside the canonical worker. The canonical launcher cleans inherited options and
plugin settings. Bare pytest cannot guard arbitrary plugins executed before
conftest. `--noconftest`, escaping `--confcutdir` and mixed native targets are not
supported safe entrypoints.

## Workload policy and compatibility

The early F3 guard refuses network/DNS, workload sleep, credential payload reads,
production data outside issued fixture roots, writes outside issued disposable
roots and unregistered children. It journals a violation before raising; catching
the exception cannot erase it. Source, settings and interpreter reads remain
available. Credential documentation is not a credential payload. Synthetic
datasets inside issued roots are valid fixtures.

Positive tests perform real Path and owned-file-descriptor writes/reads of exactly
`b"F3_TMP_ROUNDTRIP\x00\xff\n"`. A blanket write ban does not pass. Negative
secret/data controls use artificial disposable canaries. No actual secret or
production dataset is read to test the guard. Existing alias/reparse/hardlink
checks and external canary preservation remain part of the boundary.

F1/F2/F-LOG retain their native guards and assertions. F3's workload sleep policy
does not overlay them. F-LOG's real 1.2-second late-ACK fault, race delays, joins,
bounded waits, stdout/Pipe protocols and existing fault exits are retained.
Expected violations are scoped to a specific node/role, operation and exact count;
there is no blanket child/fault bypass.

The actual F2 chain is pytest → disposable Git → fixture shell hook → copied
notebook-sync `hook` → real installed pre-commit → Python `hook-check`.
The disposable tool copy receives only an early test bootstrap; removing the
insertion restores the original bytes. Production notebook-sync and logger stay
unchanged. Native Git/shell use parent admission, fixed command/environment,
observed exits and fixture evidence; they do not contain Python audit hooks.
Windows spawn targets remain fixed. Owned Windows jobs bound cleanup; unrelated
processes are never killed. The guard prevents accidental side effects, not
hostile Python; it is not an OS sandbox or general process framework.

## Reports and preservation

Each run owns a new UUID root:

| Lane | Temp/cache | Evidence |
|---|---|---|
| F1 | `.f1/tmp/pytest-dev`; `.f1/cache/runs/R/pytest` | `.f1/evidence/runs/R` |
| F2 | `.f2/tmp/pytest-R`; `.f2/cache/runs/R/pytest` | `.f2/evidence/runs/R` |
| F-LOG | `.f-log/tmp/pytest-R`; `.f-log/cache/runs/R/pytest` | `.f-log/evidence/runs/R` |
| F3 | `.f3/tmp/pytest-R`; `.f3/cache/runs/R/pytest` | `.f3/evidence/runs/R` |

F1's fixed basetemp whitelist is unchanged. It is used sequentially by one
writer. F2/F-LOG/F3 use `MDS_OFFLINE_REPORT_ROOT`; F1 uses its existing
`MDS_ENVIRONMENT_OUTPUT_ROOT`. Override roots are validated and cannot accept an
old report. Native `pytest.json` and `overhead.json` go directly to the current
root; creation is exclusive. Previous originals are never overwritten then copied.
Collection gets a distinct mode/root; preflight failures cannot reuse old success.

Tickets bind run/lane/mode/source SHA and digest, role, parent, exact argv,
bootstrap/policy digests, roots and narrowly expected outcomes. Python roles emit
start after guard installation, synchronous violation records, and a terminal
report. Native application results remain separate. Missing, damaged, duplicate,
stale or foreign evidence, unexpected counters/children, and report-write failure
produce an outward nonzero verdict. Exit 0 alone is insufficient. The enclosing
negative test expects rejection of its specific payload, not success of that run.

Working-tree runs identify their source manifest and are not clean-commit
evidence. Final checks must be rerun on the exact clean tested commit. Retain
initial failure reports and previous hashes. Reports contain safe identities,
inventory, counters and outcomes, not full inherited environments or raw secrets.

## Fixtures and notebook inventory

Shared smoke calls real `data_gathering.get_next_header`, `moex_query`, legacy
`tests.get_ticker_dates`, and `run_logging.make_error`. Literal expected rows,
columns/order, natural keys, retries/header rotation, errors and redaction are in
the shared tests. Recorded request/response/JSON/sleep/header/settings doubles
replace external work; unexpected requests and writers fail. Malformed date
structure is different from calendar validity, which remains a known domain gap.
No calculation, RSI, schema, filename, publication or temporal policy is changed.

The exact pair names, with both `.py` and `.ipynb` members, are:

```text
1year, all, count_check, data_gathering, dividends,
dohodru_data, main_tests, tech, tests, upload
```

Check set equality and all 20 members, not just count 10. Only `tests` and
`main_tests` are edited/synchronized in F3; the other eight remain unchanged.
`main.py` is standalone; there is no `main.ipynb`.

## CI and acceptance boundary

Five prepared Windows checks are `offline-f1`, `offline-f2`, `offline-f-log`,
`offline-f3` and `notebook-sync`. Their jobs remain gated by the separate
`MDS_F3_CI_SETUP_APPROVED` activation grant. Network SETUP uses the existing
Python archive hash and hash-locked dev wheels, then ends. OFFLINE tests use the
immutable fingerprinted environment. Setup failure is failure, not a test skip.
F3 SETUP also fetches only the exact reviewed baseline commit, without tags, so
original fixed-report callbacks are available to offline red-before fixtures.
It verifies that object and leaves the exact tested checkout HEAD unchanged.
Pinned checkout uses exact head with credentials disabled and contents:read;
event/merge-ref SHA and PR-head/tested SHA remain distinct. Maximum parallelism
is two standard Windows hosted jobs, each bounded to 20 minutes. No production
secrets, self-hosted runner, paid resources or raw-log publication are enabled.

Workflow activation, hosted network SETUP and the triggering push require their
separate grant. Actual successful exact-head run/check URLs are required after
activation. Skipped/unavailable/old checks are NOT VERIFIED. Required-check
enforcement requires later owner permission; branch rules are not changed here.

`python main_tests.py --allow-live-diagnostics --profile live-read` and
`python tests.py --allow-live-diagnostics --profile staging --output-root <fresh>`
fail closed without a separately approved bounded target/profile. Classification
and flags do not authorize a real run. See [coordination](coordination.md) for
the A/B/C/D/F/R residual acceptance and the distinction between F-ready,
final #47 and production.

## PR #83 guard regressions

Native Git admission checks complete argument forms, not just the command name.
Only the notebook fixtures' and installed pre-commit's read queries are admitted;
`diff --output`, ref-changing `symbolic-ref`, arbitrary object/input forms and
unlisted configuration values fail before launch. Fixture writes retain their
existing exact commands and disposable-root restrictions. Admission rejection
updates the attached native counter and common journal together.

F3 handles the `os.truncate` audit event for both path and FD operations.
Truncation requires an issued path or a registered writable regular-file FD
whose current file identity still matches the issued path. Read-only, closed,
foreign and aliased descriptors fail before the OS action. Ordinary owned path
and duplicated writable-FD truncation remain real operations in positive tests.

Ticketless source fingerprinting inventories metadata before reading candidates
or running Git status. Recognized credential payloads and source aliases cause
preflight failure; their contents are not hashed. Ordinary tracked and relevant
untracked source still contributes to the manifest and source-mode distinction.

Shared regressions use fixed fresh-process controls and synthetic Git/file
replicas. They check actual F2 admission, caught violations, unchanged bytes,
lengths, hashes and refs, zero forbidden leaf calls, and current run-bound
reports. Expected payload rejection is distinct from enclosing test success.
Windows red-before/green evidence is retained separately for all three findings.
New exact-head checks require repeat independent review; earlier successful runs
do not establish acceptance of the revised guard boundary.

## D2-01 fixed offline lane

The separate owner H and isolated SETUP grants are recorded in coordination.
Use this worktree's verified `.f1/venv-dev` interpreter and unchanged hash-locked
dependencies. Fixed commands:

```text
python -I -B -X utf8 tools/offline_tests.py --lane d2 --d2-scope previous-failures
python -I -B -X utf8 tools/offline_tests.py --lane d2 --d2-scope r31a-contract
python -I -B -X utf8 tools/offline_tests.py --lane d2 --collect-only
python -I -B -X utf8 tools/offline_tests.py --lane d2
```

The two named scopes expand only to literal registered D2 node IDs and verify
the exact collection set/count, including duplicate refusal. The first repeats
the two original failures; the second covers the closed R3.1a contract inventory.
They cannot combine with collection-only, legacy collection, `--ref` or another
lane. Arbitrary pytest arguments, targets and timeout overrides are not accepted.
The default full D2 inventory remains required.

D2 targets only `tests/d2`. Its roots are fresh `.f3/d2` temp/cache/evidence and
issued `.f-log/d2` slots. Controller, publication-worker, baseline-worker and
lock-worker roles have fixed targets, arguments, source identity, one-use tickets
and registry case IDs. Unknown cases/targets/options confer no permission. The
existing F1/F2/F-LOG/F3/D1 policies and the 900-second outer lane watchdog remain.

Closed D2 cases receive work budget 15 seconds, poll 0.1 seconds, shared cleanup
10 seconds and existing logging shutdown 10 seconds. The case watchdog is at
most 75 seconds. There are no arbitrary timeout CLI flags. Hang acceptance
requires the actual D2 parent deadline, cleanup requests and independent retained
PID/OS settlement before any controller Job action. A negative cleanup case
observes a live worker and preserves nonzero/UNKNOWN before test-only Job
containment. External termination cannot upgrade D2 evidence. Legacy hang uses
its own bounded baseline containment to demonstrate the old defect.

Red-before validates original `upload.py` blob
`dc1f12bc63eadebe589a666ffcb6a0a2bb908747` and its full literal bytes/hash. Four
swallowed failures, ignored exit 23 and a real hang are tested using original
baseline functions; baseline has no new receipt/helper requirements. Current
cases cover both starts, partial/UNKNOWN outcomes, abnormal/bootstrap exits,
checkpoint corruption and size boundary+1, listener/final ACK faults, same-run
logging, native standard handles, foreign object effects and negative canaries.
Checkpoint-reader cases independently prove actual reader closure before the
positive replacement/new-bytes oracle. An open-destination conflict remains an
expected refusal. READY readers close before GO; terminal reads require actual OS
settlement. Live sibling leaf access is refused before metadata effects. Slot-local
writer/reservation checks, leftover pending refusal and the final full inventory
retain alias/reparse/hardlink/corruption checks. Exact native arguments,
conversion/read failure cleanup and foreign caller/descriptor/path refusal remain.
The two parent-finalization faults require both children to succeed and the
intended pre-seal boundary to be observed. Raised-finalization fixture cleanup is
separate from the application CLI/sink; the INCOMPLETE scenario uses the unchanged
logger's actual report rather than a fabricated successful seal.
Raw canary captures stay in harness memory; exported evidence contains safe
observations. Literal Google columns/order/arguments, FTP destinations and
fixture bytes/hashes remain independently checked.

After changes, all five existing lanes and D2 plus the ten-pair read-only notebook
inventory must run on the exact clean tested HEAD. Hosted D2 coverage is
`NOT EXECUTED`: CI is unchanged. Windows native fixtures do not establish Ubuntu
VPS/Linux/macOS operation, production publication/readback or issue closure.
See [D2 contracts and remaining adoption](d2_supervision.md).

Historical pre-amendment D2 stop: the fifth working-tree run was 174 PASS /
2 FAIL (176 collected, exit 86). Live-reader replacement failed and the intended
parent-finalization boundary was not reached. All five historical test sets and
the BLOCKED handoff remain preserved; these results are not upgraded to PASS.
The owner accepted R3.1a and resumed under the existing H/SETUP grants.
`previous-failures` subsequently passed 2 tests (3.15 s, exit 0, run
`98a26eefbb4b48e394d40bccdae004b4`); `r31a-contract` passed 57 tests (85.79 s,
exit 0, run `fd336401949a4faf9e92336c60ad9530`). Those working-tree runs bind
source digest `6e6cf094b1ba2e8aab555eb0946a9c25de97c711b15eaeadb3d1255bd54b7119`
and precede subsequent guard/document refinements. Scoped repeat, full D2, prior
lanes and exact-clean-HEAD checks are still required; no PR readiness is claimed.
