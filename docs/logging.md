# F-LOG-01 local logging foundation

This slice adds an explicit logger and five shared `main` boundaries. It does
not validate datasets or verify publication. A stage's normal return is
`RETURNED`; a run's normal return is `COMPLETED`. Neither means
`CURRENT_VERIFIED`. Unknown dataset counts, snapshot/release IDs and release
results remain null with reasons under the [F0 contracts](dataset_contracts.md).

The evidence profile is Windows x64, CPython 3.14.8 with the GIL, using the
existing verified F1 dev environment read-only. Linux/macOS are NOT VERIFIED.
No dependencies, CI activation, live data reload or remote writes are added.

## Entry points and configuration

Importing `run_logging` opens no files, queues, processes or handlers. Importing
`main` does not require the helper. `main()` keeps its return value, console
messages, five stage calls, arguments and target. The explicit
`main(logging_context=session)` branch adds observations and health gates.

The production CLI accepts `--log-config JSON`, `--log-root RELATIVE`,
`--log-environment ALIAS`, `--log-console-level` and `--log-file-level`.
Precedence is defaults, then JSON, then explicit CLI options. There are no
logging environment overrides. Invalid configuration fails before stage
imports/calls. Errors never echo argv or invalid configuration values.
Credentials must not be supplied in configuration or CLI options.

The default root is `<owning checkout>/.f-log/logs/<run_id>`, independent of
the current working directory. An override must stay inside `.f-log/` in the
same checkout, use forward slashes and contain no `..`, drive or absolute path.
Existing symlink, junction, reparse point and hardlink aliases are rejected
before writing, rotation, retention or bundle capture. This assumes one owner
of the checkout; it is not an adversarial filesystem race security boundary.
Use a clean, pinned checkout for reproducible execution. CLI `producer_sha`
reads Git HEAD metadata; it does not certify a clean worktree or deployment.

| Setting | Default |
|---|---|
| console_level / file_level | INFO / DEBUG |
| queue_size / max_record_bytes / max_workers | 1024 / 65536 / 16 |
| critical_deadline_s / shutdown_deadline_s | 1 / 10 |
| rotation_bytes / rotation_segments | 10 MiB / 5 (active plus four retained) |
| protected_bytes | 10 MiB |
| fallback_bytes_per_producer | 1 MiB |
| context_bytes / summary_bytes | 1 MiB / 1 MiB |
| retention_days / retained_runs | 7 / 10 |
| root_budget_bytes | 1 GiB |

Only the declared keys and DEBUG/INFO/WARNING/ERROR levels are accepted. Counts
and sizes are positive integers, excluding booleans; deadlines are finite
positive numbers. Record size, worker count and segment count cannot exceed
the declared defaults. The configured full-run reserve, including every
producer fallback and the 65-byte summary seal, must fit retained-run/root
budgets. An exclusive allocation lock serializes reservation/retention. Stale
locks require explicit diagnosis; the logger does not remove them.

Retention removes only validated, sealed, finished runs with COMPLETE delivery
and `evidence_incomplete=false` in this configured root. Application FAILED
with complete diagnostics remains eligible. Active, missing/corrupt-summary and
INCOMPLETE evidence is preserved, including sealed INCOMPLETE runs. Accounting
uses the larger of actual bytes, the current full reservation and the validated
previous full reservation. A smaller new context cap does not hide the previous
configuration. Unavailable previous reservation or insufficient remaining budget
refuses a new run.
Changing roots is an explicit caller choice; archives have separate per-export
limits. Paths and operational artifacts never enter the public dataset manifest.

## Procedural API

Sessions, bootstraps and producers are opaque dictionaries. Keep transport
handles local; do not serialize the entire dictionary or mutate its private
members. The only runtime dependency is the standard library.

```python
validate_logging_config(config=None)
configure_logging(checkout_root, *, run_id, environment, producer_sha,
                  config=None, sensitive_values=(), session=None)
prepare_worker(session, worker_id)
configure_worker(bootstrap, *, sensitive_values=())
emit_event(context, level, event, message, fields)
flush_logging(context)
finish_worker(producer, *, execution_outcome, exit_status)
record_worker_outcome(session, worker_id, *, started, exit_status, timed_out=False)
check_logging_health(session)
finalize_logging(session, *, execution_outcome, exit_status,
                 application_error=None, release_result=None)
export_diagnostic_bundle(run_root, destination)
```

Pass envelope additions such as `stage`, `instrument`, `interval`, `page`,
`file`, `counts`, `retries` and a full F0 `error` in `fields`. Typed F0 additions
use `{"fields": {"capacity": capacity_record}}`; ordinary `event.fields` is
empty. A capacity assessment is caller-supplied evidence; this helper does not
calculate preflight or call exporters. CAPACITY and CAPACITY_NOT_VERIFIED remain
distinct. Pending capacity exit can be null; a final blocking summary is nonzero.
Previous quality/freshness and NOT_TOUCHED are preserved without a readback claim.

The startup context has `kind=LOGGING_CONTEXT`, version 1, run/environment,
package, producer SHA, UTC start, checkout alias, Python/platform/library versions,
safe configuration and null reasons. Library versions use distribution metadata
without importing dependencies; unavailable versions remain null/NOT_VERIFIED.
It is distinct from the canonical F0
dataset run context. Identifiers conflicting with registered secrets or
structural names are refused; they are not transported or persisted.

Repeated identical configuration with the active explicit session returns that
session. Changed configuration is refused. Finalizers return their cached
report; lifecycle final events are reserved for these finalizers. Finalization
freezes application events, public barriers, registrations, outcomes and stage
observations. Simultaneous finalizers are refused rather than emitting two finals.
Admission and freeze share a local condition in each producer. An admitted
operation owns its permit through sanitization, first LogRecord, IPC, fallback
and fallback-handle close. Parent and worker finalizers wait for these operations
within their single shutdown deadline before final events, close and seal.
Admission timeout returns a detached, cached INCOMPLETE/nonzero report with
LOWER_BOUND counters and `ADMISSION_TIMEOUT`; it publishes neither summary nor
summary seal. Cleanup waits for the last operation and, in the parent, listener
handle closure. New operations after freeze are refused. Cached health checks
sticky failures and completeness rather than trusting a cached delivery string.
Listener timeout, unclosed output or unsettled children likewise leaves only an
in-memory unsealed report; live owners cannot invalidate a published inventory.

## Delivery and failure semantics

An explicit spawn context supplies a bounded multiprocessing queue. One parent
listener thread owns primary JSONL, protected JSONL and console delivery.
Workers register before `Process.start`; each has its own one-way ACK pipe.
The private wrapper contains protocol version, producer token, monotonic
sequence, kind and a sanitized F0 event. Tokens/sequences never extend F0 JSONL.

ERROR, stage boundaries and lifecycle finals bypass ordinary level filters.
ACK is one fixed 14-byte frame after primary/protected/console writes and flushes.
A barrier ACK confirms earlier accepted events from that producer. One shared
deadline starts at public operation entry and covers producer-lock acquisition,
queue put and ACK wait. Finalization
clips this to the remaining shutdown deadline. There is one critical request
in flight per producer; no new requests follow an unconfirmed ACK.

Queue overflow, stale/cross/lost ACK or listener failure is sticky. A shared
one-way byte lets the parent observe child delivery failure independently of
its OS exit status, without a shared semaphore that a dead child could strand.
Sanitized stderr and a bounded per-producer fallback are attempted without
re-enqueue. Fallback records declare `unconfirmed` and `may_duplicate`, with
producer/sequence identity (or an unsent unique identity after lock timeout).
Late ACK cannot restore success. Exactly-once physical storage is not promised.
Logical severity counters are incremented once before sink fanout/filtering;
protected/fallback copies do not multiply them or duplicate the same F0 error.

The caller owns worker launch/join/timeout/termination and reports observed
facts through `record_worker_outcome`. Worker final ACK does not establish child
success. Missing final, missing outcome, nonzero actual exit, timeout or a failed
worker outcome makes the run incomplete/nonzero, including death before the
first event. Unknown worker counters are a lower bound. No arbitrary process
is killed by the helper.

`main` checks health before entering each stage, confirms its start event, then
records actual call entry. It records return/failure/interruption before trying
terminal delivery and stops before the next stage on known diagnostics failure.
It cannot cancel a stage that is already executing an external operation.

Programmatic application exceptions are re-raised unchanged; diagnostics errors
remain secondary. CLI catches raw tracebacks and returns 1 for ordinary failures,
130 for KeyboardInterrupt, the integer SystemExit code, or 1 for string exits.
SystemExit(None/0) permits zero only with complete diagnostics. No-argument
library calls do not configure a logger or write a stage ledger.

## Sealing and summary v1

Finalization freezes observations, verifies expected producers/outcomes, drains
through the parent barrier and emits one execution final. Then the listener
flushes/closes its files and confirms stop; owned queue/pipe endpoints close.
Closed files are hashed with reader handles closed. The run duration is measured
before summary serialization. The expected summary hash is written and closed
as `summary.sha256`; the temporary summary is flushed/closed and atomically
replaced as `run_summary.json`. Replacement is the last fallible logging step.
There is no post-summary event, flush, close, hash or bundle operation.

The summary contains version/run/UTC times, `run_duration_ms`, `observed_stages`,
execution and delivery outcomes, exit status, caller-reported release result,
null reasons, transport counters, logical severity counts, deduplicated errors,
capacity reports, expected producer final evidence, actual worker outcomes,
terminal reference, retained file hashes/sizes, rotation count, storage seal and
evidence completeness. No release result is inferred from a successful return.

For main, exactly five ordered stage rows contain `order`, `stage`,
`execution_state`, `elapsed_ms`, `duration_ms`, `error_ref` and `state_reason`.
These observations are parent-local facts independent of JSONL delivery.
NOT_STARTED rows have null timing and NOT_REACHED/PRIOR_STAGE_FAILED/
DIAGNOSTICS_FAILURE/INTERRUPTION reasons. RETURNED is only an observed normal
return; FAILED/INTERRUPTED references the corresponding F0 error. A call lacking
an observed terminal becomes UNKNOWN with null duration/TERMINAL_NOT_OBSERVED.
Helper-only runs have an empty ledger with NO_MAIN_BOUNDARIES. Run duration
includes setup, delivery shutdown and closed-file inventory; it excludes summary
serialization/replacement and later bundle export.

Mandatory rows/errors are never silently truncated to fit summary limits.
Summary write/close/replace failure leaves no authoritative zero. A reader
checks full summary keys/version/run, summary seal, retained inventory, terminal
run/outcome and expected successful producer facts. Missing/truncated/inconsistent
evidence is incomplete. `storage_sealed` requires settled expected children and
closed owned writers/endpoints. Deadlines do not guarantee termination of blocked
OS I/O or survival of hard process termination; those runs remain unsealed.

## Privacy and diagnostic bundles

Accepted values are bounded JSON primitives: depth 8, 256 items and at most
64 KiB per encoded event. Unsupported objects, cycles, nonfinite values and
oversized strings are rejected without evaluating repr/str. Sensitive key names
are normalized for case, underscores, hyphens and spaces. Register sensitive
literals locally in each producer; they are never put in bootstrap/config/IPC.

Redaction precedes the first owned LogRecord and serialization. Records have
empty args and no exc_info/exc_text/stack_info. Exception messages use bounded
primitive args and traceback filenames/functions/line numbers; no locals or
source lines are inspected. Credential-bearing text lines, URL userinfo/query/
fragment, private targets and absolute paths are removed. Only the known public
MOEX/Dohod URL hosts retain paths; Google API/Docs/Drive and other targets use
aliases. Registered structural collisions are refused. Foreign handlers and
legacy prints are outside this slice's capture guarantee.

Bundle export is explicit and separate from finalization. It accepts sealed
finished evidence, including sealed failed/incomplete runs with their flags.
The destination must be a new file under `.f-log/bundles/`. It copies only
context, summary/seal, retained JSONL/protected/fallback files into its own
snapshot, checks source hashes again and binds the captured summary bytes to
the seal. Each immutable byte payload is checked before entering ZIP and listed
in `bundle_manifest.json` with its actual hash and size. Disposable staging is
removed in `finally`. Snapshot/archive limits are 96/128 MiB. Environment files,
credentials, local absolute paths and datasets are not collected. These hashes
detect accidental mutation; they are not a signature against an attacker able
to rewrite the entire run and seal. Review the archive manually before sharing.

## Offline verification and overhead

Use the existing pinned F1 dev interpreter with `-I -B -X utf8`, disabled pytest
plugin autoload/options, task-local TEMP/TMP and fresh basetemp/cache roots.
Do not install into or mutate that environment. Do not collect root `tests.py`
or `main_tests.py`, import live stages as a smoke test, or invoke the real CLI.
Both suites are separate processes because their private `_probe` modules and
safety profiles differ.

```text
python -I -B -X utf8 -m pytest -c pyproject.toml --confcutdir tests/logging tests/logging -q --basetemp .f-log/tmp/pytest-UNIQUE -o cache_dir=.f-log/cache/pytest
python -I -B -X utf8 -m pytest -c pyproject.toml --confcutdir tests/f2 tests/f2 -q --basetemp .f2/tmp/pytest-UNIQUE -o cache_dir=.f2/cache/pytest
python -I -B -X utf8 tools/notebook_sync.py check --worktree
python -I -B -X utf8 tools/notebook_sync.py check --ref EXACT_HEAD
git diff --check
```

F-LOG guards install before collection and inside each fresh fixture child.
They forbid network/DNS, secrets/.env, datasets and writes outside `.f-log`,
and allow only the exact fixture interpreter/script or worker target. Actual
Windows spawn, hardlink and junction fixtures may need the host sandbox
exception; the Python guards remain enabled. Sentinel contents are checked.

The source-bound red-before test runs baseline `main` with all five dependencies
stubbed and finds a canary in its raw traceback. New-helper absence is not used
as a bug reproduction. Independent literal F0 fixtures and timing oracles cover
success (225 ms), third-stage failure/interruption (115 ms), diagnostics failure
after stage two (75 ms), and one logical WARNING. No live acceptance is inferred.

The benchmark delivers 2,000 INFO events with 1 KiB messages in three runs,
barriers every 250 events, INFO console and DEBUG JSONL. It checks all 2,000
ordinary records, zero drops and separate lifecycle events, measuring through
delivery shutdown/closed inventory before summary. It records the median and
payload-loop baseline without an arbitrary pass threshold. Console uses pytest
capture; these synthetic measurements do not predict ETL or terminal latency.
Actual samples, source HEAD, environment before/after hashes and narrow test
results belong in the ignored review handoff, not a production PASS claim.

## Integration and rollback

F-LOG-02 and owners A/B/C/D still need internal stage progress, ticker/interval/
page/file correlation, retry/schema/revision/quarantine events and exporter/
transfer/rollback outcomes. Caller-owned capacity/release fixtures are not real
exporter/preflight acceptance. F3/coordinator owns CI activation and required
merge gates; E3/R3 owns credential-isolated staging and production evidence.
Issue #78 stays open until those hooks are implemented and validated.

CSV/XLSX names, schemas, column order, dates, RSI and Google targets are
unchanged. Revert this slice to remove the helper/shared boundaries/ignore rule
and documentation; existing datasets and remote targets require no rollback.
Preserve diagnostic evidence separately before any explicitly authorized cleanup.
