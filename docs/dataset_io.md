# D1: local single-file writes

`dataset_io.py` provides two procedural helpers for cooperative Windows callers.
It has no import-time filesystem, logging or Windows DLL operations. Other
platforms fail closed before target I/O. Production writers remain **NOT
CONNECTED**; adding this helper does not complete issues #25, #26, #47 or #78.

## Public API

```python
resource_lock(
    relative_path,
    *,
    resource_root,
    validate_target,
    logging_session,
    receipt,
    timeout_s=0,
)  # context manager -> opaque lease

atomic_write_file(lease, prepare_candidate, validate_candidate) -> d1_receipt
```

`resource_root` is an existing absolute local Windows directory. The target's
parent must already exist. Paths with traversal, ADS, reserved device names,
ambiguous trailing dots/spaces, DOS short-name forms, links, reparse points,
network drives or a target with multiple hard links are refused. No directories
are created by the helper. Drive-letter case, slash spelling and a root trailing
separator do not change the resource identity.

Keep the entire current read, update computation and write inside the context:

```python
receipt = {}
with resource_lock(
    'candidate.csv', resource_root=candidate_root,
    validate_target=validate_current, logging_session=session,
    receipt=receipt, timeout_s=5,
) as lease:
    # Read current and compute the update here, while the lease is owned.
    snapshot = atomic_write_file(lease, prepare_csv, validate_csv)
# receipt now also reports release and its confirmed diagnostics.
```

The caller creates and finalizes `session` through the existing `run_logging`
API. A live parent-owned session in this process is required; registered worker
producers are unsupported. Both JSONL and console thresholds must include INFO.
The helper does not create or finalize a logging run.

`receipt` must initially be an empty plain `dict`. During the context the helper
owns that sink and replaces it with a closed typed snapshot. Caller-added fields
are discarded. Receipts never authorize admission, replace or recovery. An
opaque lease is tied to its PID/thread, cannot be forged or used after release,
and permits one atomic-write attempt. Recursive acquisition in the same thread
for the same physical directory is refused.

## Lock and admission

All siblings in a physical parent directory serialize on one named mutex.
Different physical directories are independent. The exact name is:

```text
Global\MDS-D1-v1-<64 lowercase SHA-256 hex digits>
```

The only identity source is successful
`GetFileInformationByHandleEx(FileIdInfo)` on a checked non-inheritable parent
directory HANDLE, opened with `FILE_READ_ATTRIBUTES`, share READ/WRITE/DELETE,
`OPEN_EXISTING`, and `BACKUP_SEMANTICS | OPEN_REPARSE_POINT`. The digest preimage is:

```python
b'MDS-D1-v1\0' + volume_serial.to_bytes(8, 'little') + file_id_bytes
```

The serial has all 64 bits; the file ID has all 16 opaque bytes. Paths, target
names, sessions, PIDs, threads, run IDs and random salts are excluded. Identity,
namespace, access, create/open and wait failures refuse acquisition. There is no
`Local\`, alternate name, stat/legacy identity, ACL, elevation or unlocked
fallback. Existing Windows access permissions still apply to the global object.

`timeout_s` is a finite number from 0 to 60. The default 0 refuses a busy resource
immediately. Positive values use a monotonic deadline and native waits of at most
100 ms. FIFO and bounded callback/filesystem execution time are not promised.

Every successful native acquisition validates the specifically requested target,
including NORMAL, a newly created mutex and ABANDONED. The validator receives a
borrowed read-only binary stream at cursor zero, or `None` only after confirmed
absence. It must return exactly `True`. False, None or another value yields
`D1_RECOVERY_REQUIRED`; a callback exception is re-raised unchanged. The helper
measures the target before and after validation and refuses changes.

The caller's policy decides whether ABSENT or structurally usable but unverified
current data is acceptable. Admission does not assert data quality, freshness,
provenance or F0 `VERIFIED`. Neither an abandoned mutex nor admission of another
sibling authorizes this target. Every retry uses a new receipt and fresh checks.

## Candidate and commit

The helper captures this attempt's baseline, creates an exclusive
`.d1-<generated-id>.tmp` sibling and calls `prepare_candidate` with a borrowed
writable binary stream. It flushes, fsyncs and closes that writer, then calls
`validate_candidate` with a separate borrowed read-only stream. The candidate
validator must return exactly `True`. Callbacks must not close borrowed streams.

After confirmed precommit diagnostics, it rechecks parent identity, target
baseline, candidate identity/hash and logging health. One real `os.replace`
changes the final pathname. Final readback must match the candidate. Ordinary
cleanup only deletes the known owned temp after identity checks; foreign temps
are never scanned, removed or promoted. A killed process may leave an orphan.

Commit facts are separate from observed bytes and diagnostics:

| State | Meaning |
|---|---|
| `NOT_ATTEMPTED` | Replace was not called. Preservation needs an actual final observation matching this attempt's baseline. |
| `UNKNOWN` | Replace was entered but did not return successfully. Readback relation does not prove syscall success. |
| `REPLACED` | Replace returned successfully. This fact remains true after readback, logging or release failure. |

There is no automatic restore. A new caller after a crash observes the current
file as its own lease entry; that file is not relabeled as an earlier historical
baseline. Original application exceptions and tracebacks remain primary;
diagnostic and cleanup errors are secondary typed codes.

## Closed receipt

| Fields | Values |
|---|---|
| `receipt_version`, `operation_id`, `file_ref` | 1 and generated logical IDs; no raw path |
| `lock_observation` | `NOT_ACQUIRED`, `NORMAL`, `ABANDONED` |
| `lock_state` | `NOT_ACQUIRED`, `OWNED`, `RELEASED`, `UNKNOWN` |
| `target_state`, `target_validation` | `ABSENT`, `PRESENT`, `CHANGED`, `UNKNOWN`; `NOT_RUN`, `ADMITTED`, `REJECTED`, `ERROR` |
| `lease_entry_observation`, `attempt_baseline`, `final_observation` | `{exists, sha256, bytes}` or null with a fixed reason |
| `prepared_candidate` | `{sha256, bytes}` or null |
| `commit_state`, `final_relation` | States above; `MATCHES_BASELINE`, `MATCHES_CANDIDATE`, `OTHER`, `UNKNOWN` |
| `write_status` | `NOT_RUN`, `SUCCEEDED`, `FAILED` |
| `diagnostics_state` | `NOT_CONFIRMED`, `PRECOMMIT_CONFIRMED`, `POSTCOMMIT_CONFIRMED`, `COMPLETE`, `INCOMPLETE` |
| `mandatory_sequences`, `barrier_sequence` | Integer sequences only; null barrier before confirmation |
| `errors`, `null_reasons` | Bounded `{stage, code}` entries and fixed reasons; primary error first |

Digests are lowercase SHA-256; sizes are nonnegative integers. Confirmed absence
has `exists=False`, `sha256=None`, `bytes=None`; unreadable current is not absence.
`atomic_write_file` returns an independent snapshot while the lease is still
owned. The supplied sink receives release results when the context exits.
`COMPLETE` requires confirmed release diagnostics; it does not finalize the run.

## Logging and offline evidence

The existing envelope uses stage `runtime`, a generated file reference, fixed
Russian messages and outcomes. `event.fields` is always `{}`. Hashes, bytes,
recovery facts and commit state stay in the receipt. `make_error` performs public
F-LOG redaction; raw exceptions and transport dictionaries are never in receipts.
`elapsed_ms` measures monotonic elapsed time from this acquisition attempt's
start, including its lock wait. Unmeasured per-phase `duration_ms` stays null;
this is not a logging-overhead benchmark.

| Event | Level / outcome | Fixed message |
|---|---|---|
| `d1_lock_acquired` | INFO / ACQUIRED | Получена блокировка ресурса |
| `d1_lock_denied` | WARNING / DENIED | Ресурс занят; lease не выдан |
| `d1_lock_abandoned` | INFO / ABANDONED | Обнаружено завершение предыдущего владельца |
| `d1_target_admitted` | INFO / ADMITTED | Текущий target допущен к локальной операции |
| `d1_target_rejected` | ERROR / REJECTED | Текущий target не допущен |
| `d1_temp_prepared` | INFO / PREPARED | Временный файл подготовлен |
| `d1_temp_validated` | INFO / VALIDATED | Временный файл проверен |
| `d1_replace_precommit` | INFO / READY_TO_REPLACE | Подготовлена замена отдельного файла |
| `d1_file_replaced` | INFO / REPLACED | Отдельный файл заменён |
| `d1_write_failed` | ERROR / FAILED | Локальная запись завершилась ошибкой |
| `d1_lock_released` | INFO / RELEASED | Блокировка ресурса освобождена |

The logger supplies run ID, producer SHA, process/worker and timestamp. Unknown
dataset/snapshot/release/instrument/interval/page/artifact/target IDs retain its
existing null reasons. Error records use public `make_error`; no bytes in
`counts`, hashes in `capacity`, or receipt/transport objects in `fields`.

Required INFO sequences cover lock acquisition, optional abandoned observation,
target admission, preparation, candidate validation, precommit and replaced/release
events. Accepted events alone are insufficient: same-producer `flush_logging`
must confirm their sequence and public `check_logging_health` must be healthy.
Precommit diagnostic failure forbids replace. Postcommit failure leaves
`REPLACED` and fails the operation with incomplete diagnostics. The final health
check and data replace have a race; there is no data/log transaction guarantee.

Use the guarded `tools/offline_tests.py --lane d1` entrypoint, including a separate
`--collect-only` invocation. D1 uses `.f3/d1` temporary/cache/evidence roots and
issued `.f-log/d1/<guard-run>/<case>/<slot>/logs` sessions. The fixed controller
starts only `tests/d1/d1_worker.py` with registry case/slot arguments, establishes
real overlap, enforces finite bounds and owns process Jobs and termination
evidence. Each process owns its parent logging run. A killed run remains
incomplete; expected-death evidence does not produce a fake successful final.

Independent literal CSV/JSON fixtures, parent file reads/hashes and actual OS
exit reports are the byte-preservation oracles. Same-session process tests prove
overlap and naming, not an executed cross-session deployment. Cross-session
permissions/integration require separate R1 evidence.

The reader regression closes its complete h0 read before the writer commits and
opens a fresh read after replacement. Its evidence covers complete per-read
h0/h1 snapshots and overlapping processes. It does not establish that commit
succeeds while a reader keeps an old native HANDLE open: that arrangement caused
a Windows sharing/access refusal in the disposable regression. Sharing failures
remain explicit failures with measured final bytes and UNKNOWN commit state if
replace was called; the helper does not change sharing policy or restore files.

## Fixed process protocol

The controller accepts a registered case, never arbitrary executable, path,
mode, timeout or expected exit. Gate commands use the isolated
`.f1/venv-dev/Scripts/python.exe`. Workload processes use the owner-approved,
hash-checked official Windows x64 CPython 3.14.8 GIL interpreter directly at
`.f1/python/python.exe`, followed by `-I -B -X utf8`, the absolute
`tests/d1/d1_worker.py`, `--case <registered-case> --slot <registered-slot>`.
The Windows venv executable is a redirector that creates an extra process; using
the same verified base directly makes Popen PID equal the worker PID. No launcher
environment, argv/prefix spoofing or unverified interpreter is accepted.
Guard installation precedes helper/backend/logger imports. Workers cannot launch
descendants. A consumed private launch token fixes argv/cwd/env/PIPE stdio;
suspended CreateProcess, owned Job assignment and resume belong to the controller.

| Case | Slots and fixed modes | Checkpoint / independent oracle |
|---|---|---|
| `deny_second` | holder: hold_update; contender: deny | TARGET_READY holder remains owned; timeout 0 contender denies; RELEASE holder |
| `bounded_wait` | holder: hold_update; contender: wait_500ms | Holder lives through contender's bounded denial |
| `serialize_updates` | holder: hold_update; contender: retry_after_denial | Genuine contention; h0 → h1 (A) → h2 (A+B) |
| `independent_directories` | a/b: hold_update | Both TARGET_READY simultaneously |
| `same_directory_siblings` | holder: hold_update_a; contender: deny_b | One directory mutex; sibling B denied |
| `abandoned_before_commit` | holder: hold_partial; recovery: wait_recover | CONTENDED before kill at CANDIDATE_READY; ABANDONED still validates target |
| `late_restart_before_commit` | holder: hold_partial; restart: validate_only | No old handles/mutex; NORMAL still validates h0 |
| `late_restart_invalid_before_commit` | holder: hold_partial; restart: validate_reject_only | After confirmed mutex absence, a NORMAL acquire rejects changed invalid current |
| `crash_after_commit` | holder: hold_after_replace; recovery: wait_validate_only | Kill at POSTCOMMIT_READY; current h1, not preserved historical h0 |
| `late_restart_after_commit` | holder: hold_after_replace; restart: validate_only | New NORMAL acquire validates h1 |
| `directory_target_switch` | holder: hold_partial_a; next: wait_reject_b | B validator refuses independently of A/ABANDONED |
| `retry_false` | worker: retry_false | Two fresh False refusals and two target callbacks |
| `retry_unverified` | worker: retry_unverified | Two fresh None refusals and two target callbacks |
| `retry_exception` | worker: retry_exception | Two fresh exception refusals and two target callbacks |
| `reader_visibility` | writer: write_gated; reader: read_final | Successful reads equal complete h0 or h1 |

Frames authenticate ticket/run/node/slot, nonce, source/policy/worker digests and
actual Popen PID. GUARD_READY confirms guard/start identity; ARMED confirms
initialization before GO. For writer modes, TARGET_READY confirms the concrete
target read/admission under the owned lease. The `read_final` mode instead
confirms a complete native read whose handle is already closed, without a lease.
CANDIDATE_READY confirms flushed partial/temp and retained ownership; CONTENDED
confirms real native wait timeout; POSTCOMMIT_READY confirms returned replace and
already-published REPLACED before readback; RESULT is a closed typed normal result.
Only GO/RELEASE commands are accepted in defined states. The postcommit pause is
a test wrapper; production API has no crash hooks.

For `late_restart_invalid_before_commit`, the holder reaches TARGET_READY and
CANDIDATE_READY with a real flushed partial temp. The controller kills and
collects it, closes former handles, confirms the mutex is absent, and writes the
independent literal `D1_INVALID_CURRENT\n` to current. A fresh timeout-0 restart
must observe NORMAL, call its target validator once and return REJECTED with no
candidate and NOT_ATTEMPTED. Invalid current remains unchanged and the orphan
partial temp is retained. The dead holder still has only the declared raw death
errors, unknown final counters and no successful logging final.

Bounds are bootstrap/state 10 s, settle/cleanup 5 s and whole case 40 s, with at
most two live workloads. Registry lock waits are 0, 0.5 or 5 s. Frames are at most
4 KiB and 32 per worker; stdout is at most 64 KiB and stderr 128 KiB, drained
concurrently. Workload sleeps are forbidden. An undeclared timeout control gets
0.5 s after READY.

Expected death is limited to the exact crash case/node/slot after authenticated
CANDIDATE_READY or POSTCOMMIT_READY. Before `TerminateJobObject(..., 124)`, an
exclusive `.d1-termination.json` declaration records ticket/PID/phase/READY
witness. The ticket is unchanged. Valid start, no violation journals or extra
descendants/reports, exit 124 and `timed_out=False` allow only the full raw error
set `{MISSING_FINAL_REPORT, UNEXPECTED_EXIT}`. Unknown final counters remain
UNKNOWN. The killed logging run is INCOMPLETE.

| Negative control | Exact raw result |
|---|---|
| Wrong ticket/node | Refusal before issuance/Popen; launched false; no death exemption |
| exit_before_ready_7 | D1_READY_NOT_REACHED, MISSING_FINAL_REPORT, UNEXPECTED_EXIT; exit 7, timeout false |
| omit_guard_final | MISSING_FINAL_REPORT only; exit 0 |
| corrupt_guard_final | CORRUPT_FINAL_REPORT only; exit 0 |
| park_without_release | D1_UNDECLARED_TIMEOUT, MISSING_FINAL_REPORT, TIMEOUT, UNEXPECTED_EXIT; exit 124, timeout true |
| attempt_extra_child | UNEXPECTED_COUNTERS; exactly children:1 and matching journal; launch blocked |
| Foreign/duplicate/replayed report or malformed READY | Exact identity/order/report refusal; no termination declaration |

Controls compare full raw error sets and exact counters/node/exit/timeout;
additional errors fail. A successful harness assertion never changes raw
`validated=False` into true. Control/termination JSON has separate strict schema;
there is no generic missing-report or JSON waiver.

## Required verification gates

Use the owner-verified interpreter, full clean D1 HEAD and authorized base.
These commands describe required checks, not completed results:

```powershell
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane d1 --collect-only
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane d1
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane f2
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane flog
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane f3
& $d1Python -I -B -X utf8 tools/offline_tests.py --lane f1
& $d1Python -I -B -X utf8 tools/offline_tests.py --notebook-check --ref $d1Head
git diff --check
git diff --check $d1Base $d1Head
```

`$d1Python` is the verified `.f1/venv-dev/Scripts/python.exe` from the isolated
SETUP handoff. Fixed D1 workers use the verified `.f1/python/python.exe` base
directly; this does not change the interpreter used for the gates. `$d1Head` is a full
40-character lowercase SHA of the clean source commit; `$d1Base` is the exact
authorized base. Repeat required checks after the last source commit. The
notebook invocation is separate, with no lane or collect-only arguments; it
checks pairs without execution or synchronization. Pending-diff whitespace
checks do not replace the full PR range check. D1/F2/FLOG/F3 do not replace F1,
and existing F1/F2/FLOG/F3 assertions and A-admission behavior remain mandatory.

## Remaining integration work

All A/B/C/D public writers and orchestrators need separate owner PRs that start
the lock before current reads. F0 `write_dataset`, registered-worker authorization,
other platform backends, CSV/XLSX pair consistency, snapshot transactions and
remote publication remain separate tasks. Candidate fsync plus one-file replace
does not promise metadata survival under power loss or protection from hostile
noncooperative filesystem mutation. Rollback reverts source in a reviewed PR;
it never deletes outputs, foreign temps or worktrees.
