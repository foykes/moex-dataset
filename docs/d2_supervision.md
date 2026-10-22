# D2 Linux publication supervision

This package supervises the mandatory Google and FTP children. Successful local
supervision is not a verified release. There is no remote readback protocol in
D2, no D4 `release_result`, no current-generation switch and no evidence that a
previous publication was preserved. See the exact-HEAD review evidence for the
actual acceptance status; this document does not certify an unexecuted platform.

## Entrypoints and ownership

`upload` has an inert, standard-library-only import. On unsupported platforms,
direct `upload.main` refuses before credentials, sources, children or remote
effects. The full pipeline obtains runtime admission before its first modifying
stage. Windows imports, static tools and closed offline diagnostics remain usable;
there is no legacy uploader fallback or public bypass flag. Frozen Windows D2 is
unchanged: 221 collected, 216 passed / five failed on its historical working tree.
The three bootstrap export failures remain failures in that evidence.

`main.main(logging_context=..., result_sink=...)` preserves the five-stage order,
original targets and successful `None` return. Its existing `run_stage` closure
hands the exact context, sink and admitted runtime to `upload.main`. CLI owns one
runtime and one F-LOG session, with one run ID and two registered worker IDs. A
borrowed logging session belongs to its caller and is not finalized by `main`.

The upload notebook is paired with `upload.py`; production never converts it.
`run_logging.py`, `dataset_io.py`, settings, locks, all other notebook pairs and
published datasets are outside this package's write scope.

## Source and destination bindings

The legacy uploader bound dataset sources to the parent's `sys.path[0]` and
credentials to `current_path.rsplit('/', 1)[0] + '/secrets/'`. Normal Linux script
launch therefore uses checkout `datasets/` and sibling `../secrets/`. D2 freezes
these effective bindings before isolated child launch. It does not recompute
them from a child's `-I` search path, ambient CWD or decoy files. Ambiguous
programmatic bindings fail before mutation rather than silently relocating data.

Google reads `10years_data_1d_interval.csv`. Projection is drop-only and preserves
the input order of the allowed open/close/high/low/value/volume/begin/end/ticker/
RSI14 columns. The existing first worksheet is cleared, then receives `A1` with
`copy_index=False`, `copy_head=True`, `fit=False`, `escape_formulae=False` and
`nan='NaN'`. This package does not authorize a Google capacity or overflow policy.

FTP inventories exactly three shallow directories: regular files in dataset
root, `dividends` and `ticker_lists`, retaining their previous relative `STOR`
destinations. A root README remains an output. Nested directories, including
dohod.ru content, are not added recursively. Required missing/unreadable
directories, aliases, symlinks, hardlinks, special files and inventory races fail
before GO. Empty existing directories are valid. Discovery is not an approved
release universe or a D4 manifest.

## Native launch, deadlines and settlement

The supported evidence profile is ordinary GIL CPython 3.14.8 on Ubuntu 24.04
x86-64. Native launch uses explicit standard input/output/error DEVNULL handles,
closed foreign FDs, a fixed worker entrypoint and a minimal inherited FD list.
The real F-LOG spawn transport is serialized before birth; third-party imports
occur only inside the protected worker wrapper. Linux bootstrap protection does
not imply Windows or macOS protection.

The adapter reuses the foundation's read-only CPython source attestation. That
does not install its test process/network guard in production. Closed D2 fixture
admission and OS network isolation are test protection; production publication
uses its own fixed native launch and ownership, without a fixture capability.

Birth intent, returned process owner, PID/creation identity, pidfd binding,
registration and actual exit are distinct observations. A constructor exception
after a possible native birth is not recorded as `started=False`. An unknown
birth remains incomplete; the parent never invents an exit or worker final.
Popen remains the reaper; observing a pidfd must not consume its wait status.
Only owned processes and owned transport/tracker resources may be cleaned up.

The production work budget is 3600 monotonic seconds immediately before the first
start. It includes both starts, bootstrap, READY/GO, operations, checkpoint/final
ACK and actual OS exit. Source preflight and logger setup precede that budget.
Polling is 0.1 seconds. Cleanup has one shared ten-second budget: TERM for up to
five seconds, then KILL and settlement using the remainder. F-LOG shutdown has
its separate ten-second budget. Blocking native construction or OS I/O prevents
an absolute 3600-second wall-clock guarantee. A live or unsettled worker excludes
success even when a cleanup request was issued.

The overall deadline and each worker's timeout are separate observations. An
actual terminal poll completed before the deadline proves that worker timely.
An actual live poll begun at/after expiry proves its missed deadline. A first
late or crossing terminal observation proves neither: `timed_out` remains null,
the worker cannot succeed and its unknown boolean F-LOG outcome is not invented.
TERM/KILL settlement, including a later exit zero, never upgrades timing proof.
The healthy sibling keeps `timed_out=false` in both D2 and F-LOG and is not a
failed channel merely because its peer exhausted the common budget.

Closed native D2 fixtures use a validated 15-second work profile and a 75-second
outer containment watchdog. The outer lane remains bounded to 900 seconds.
There is no arbitrary CLI deadline override. A hang passes its negative oracle
only if the D2 parent itself observes expiry, requests cleanup and records actual
settlement or an honest unsettled nonzero outcome. Watchdog termination alone is
not that oracle.

## Checkpoint phases and receipts

| Phase | Admitted checkpoint activity |
|---|---|
| Before GO | Each worker validates/writes only its own slot, publishes READY, then waits. Parent validates both READY records and closes all of its reader/inventory handles. |
| GO | READY identities/sequences, reader closure, source recheck, logging health/flush and remaining deadline must pass. |
| Running | Parent observes process identities/exits, deadline, cancellation and F-LOG health. It does not reopen a live worker's mutable checkpoint; no worker inspects a live sibling leaf. |
| Individual OS settlement | Parent may read that worker's terminal checkpoint. Exit zero alone does not supply a missing worker final or ACK. |
| All started workers settled | Full inventory checks allowed names, phase-appropriate existence, aliases/security and absence of leftover pending files. Allowed pending names are not required to exist after successful replacement. |

Writes serialize completely, flush and close before standard `os.replace`.
Readers use bounded regular-file reads with no-follow and inode/owner/link
checks. Replacement failure is nonzero; it does not authorize a rename adapter,
foreign-reader bypass or silent retry. Missing, corrupt, partial, oversize,
stale-run/attempt/PID/sequence/token and leftover evidence fail closed.

Parent `execution_state` retains the last proved phase: `STARTED` after an
actual process owner, `READY` after checkpoint validation, and `GO_RELEASED`
only after that worker's successful GO pipe write. GO_RELEASED proves delivery
to the pipe, not worker consumption or remote mutation. Missing/invalid or
nonterminal final evidence retains that phase with unknown terminal outcome,
never NOT_STARTED or invented RETURNED. Unknown constructor birth is UNKNOWN;
only proved non-start is NOT_STARTED. A valid terminal checkpoint is required
for RETURNED/FAILED/INTERRUPTED and normal-return evidence.

| Observation | Meaning |
|---|---|
| NOT_ATTEMPTED | No mutation was started for that output. |
| UNKNOWN | Mutation was started or cannot be ruled out, without verified remote readback. |
| ACKNOWLEDGED | The local API call returned; remote committed state remains UNKNOWN. |
| `committed_outputs` | Empty without actual verified remote commit/readback evidence. |
| PREFINAL | Persisted preliminary parent diagnostic result before F-LOG finalization. Never a sealed final success. |
| FINAL in-memory sink | Combines mandatory worker results, actual process/transport settlement and the observed F-LOG finalization report. |

Before a remote call, the checkpoint records UNKNOWN. A Google clear followed by
a failed write and a partial FTP transfer are not rolled back or called preserved.
The parent never substitutes process supervision for VERIFIED RELEASE.

## F-LOG and completion

Register Google and FTP before launch. Record truthful observed `started` and
actual exit status after bounded cleanup; unknown births are not false non-starts.
Only workers emit their real final messages. Listener/ACK failure, missing final,
mandatory failed upload or unconfirmed settlement makes overall success false.

PREFINAL may have been written successfully when finalization later fails. The
persisted record retains its preliminary scope; CLI and final sink remain
nonzero/INCOMPLETE. A pre-seal fault test must independently witness both worker
successes and its injection before successful seal. A separate incomplete seal
scenario is required. Cleanup performed by a fixture is not an application seal.

If cleanup fails after a genuine F-LOG seal, the actual report/history is kept;
the overall result separately becomes incomplete. Do not finalize twice or
rewrite an observed report. The legacy Russian success banners and elapsed-time
format are rendered only after required upload, runtime cleanup, actual F-LOG
finalization and zero final outcome. A caller-owned session leaves final
completion rendering to its owner. Required-stage `SystemExit(None/0)` is a
nonzero incomplete pipeline; explicit 7 stays 7, string status maps to 1 and
KeyboardInterrupt to 130. The original application exception is preserved.

## Diagnostic bounds and privacy

| Proposed conservative diagnostic maximum | Value |
|---|---:|
| FTP output tokens | 512 |
| Google output tokens | 1 |
| Full worker checkpoint | 65,536 bytes |
| Full parent receipt | 131,072 bytes |
| All owned run files, including pending files | 1,048,576 bytes |

Only positive integer reductions of the four public diagnostic limits are
accepted; unknown keys, bools, increases and timeout overrides fail. More than
512 FTP files refuses before mutation; no output list is truncated. This is not
an existing owner capacity policy or measured production inventory. A broader
capacity decision needs explicit product ownership.

Bounds measure the complete UTF-8 serialization with ASCII escapes, sorted keys,
compact separators, `allow_nan=False` and a trailing newline. Tests exercise each
boundary and boundary+1 with complete payloads. Reservation checks account for
both pending and current files; exceeding a limit is an error, never truncation.
The D2 run-file bound is separate from F-LOG's unchanged logging configuration
and storage reservations. F-LOG lives outside the mutable checkpoint inventory;
excluding it from that inventory does not certify its delivery or settlement.

Console, receipts, F-LOG and exported evidence contain only closed diagnostic
codes and approved fields. Raw exceptions, tracebacks, credentials, secret argv,
URLs and private paths must not escape. Pre-wrapper import/unpickle failures have
explicit NOT_ENTERED/NOT_OBSERVED evidence where no slot observation exists; zero
invented counters are not substituted. An independent positive canary control
must reject contaminated exports, not merely pass because the target never ran.

## Regression and acceptance ledger

The four fixed D2 targets are `test_supervision.py`, `test_targets.py`,
`test_logging_contract.py` and `test_guard_admission.py` under `tests/d2/`.

| Contract / AC | Independent oracle |
|---|---|
| Original defects / #39, #41 | Literal legacy upload blob dc1f12bc63eadebe589a666ffcb6a0a2bb908747, swallowed Google/three FTP errors, genuine exit23 and safely contained real hang; no new legacy helper assumed. |
| Mandatory outcome | Two actual worker PIDs; success, exit23, ordinary/abnormal exit, missing final, both start faults and partial/unknown construction. |
| Bounded lifetime | Parent monotonic-expiry witness, TERM/KILL sequence, actual pidfd/process settlement, cancellation and owned tracker/FD closure. |
| Checkpoints | READY/read-close/GO ordering, live sibling refusal, terminal read only after OS exit, full inventory only after all settlement, security/corruption/replace negatives. |
| Remote truth | Partial Google/FTP and transfer ACK remain UNKNOWN; no invented committed outputs. |
| #78 logging | One run ID/session, two registered workers, exact context/sink handoff, actual worker final and listener/ACK failures. |
| Completion | Reached pre-seal injection, separate INCOMPLETE, PREFINAL persistence and post-seal cleanup preserve true report history. |
| Privacy and limits | Negative/positive canaries; actual bootstrap fault boundaries; complete serialized boundary+1 and output 512/513. |
| Targets | Literal CSV bytes/hash, drop-only projection/order, existing sheet/write arguments, shallow FTP destinations, bindings and source races. |
| #47 / old policies | Source-bound compatibility fixtures, unchanged Windows runner/guard policies and all ten notebook pairs. |
| #67 platform | Actual OS/kernel/architecture/Python/start mode/CWD/Unicode/encoding and native observations. Linux VPS, fork/forkserver, macOS and Windows adoption stay open. |

Every historical Windows node is retained in the local comparison ledger with
its previous result and either a semantic Linux counterpart or explicit
Windows-only hookup. Linux acceptance never changes those historical outcomes.

FD evidence preserves the raw before/after descriptor comparison. A process-wide
CPython allocator cache is distinguishable from D2-owned resources only through
an independent, source-pinned birth/custody witness: the same actual global heap,
fully free Arena indexes, the original Arena/mmap objects, unchanged baseline,
exact descriptor births and no observed close/dup/rebinding of that pair. Equal
file metadata or a matching descriptor count alone cannot establish custody.
The native fixture also verifies that every D2-owned lease is closed and disjoint
from the cache. Unknown provenance fails closed. Destructive substitution controls
use their own Heap/Arena and the same custody predicate; they never close or
mutate the borrowed global allocator. Historical raw mismatches remain recorded.
The fixture enforces five literal Python source hashes plus the issued upload
source hash. Its mmap extension hash is an observation, not an independently
enforced pin. Runtime identity trusts the existing toolcache; it does not
revalidate CPython source archive bytes.

Portable alias/special-file tests dispose only the leaves they created, after
their refusal and preservation assertions. They restore the original target's
bytes/link count before the unchanged strict outer cleanup. This is fixture
teardown, not native application cleanup or an upgrade of a failed outcome.

The serialization fault fixture keeps the original exception alive through real
teardown and late collection. Cleanup relinquishes only the completed, direct
source-attested `reduction.dump` invocation whose caller, object and payload
buffer belong to D2. Nested/foreign frames and exception chains are preserved;
unproven resource closure remains INCOMPLETE. A post-context bind fault separately
checks that the actual owned F-LOG session is finalized even when the caller's
local context alias was never assigned.

## Integration and rollback

This is one stacked Draft PR based on independently code-accepted #117 HEAD
25dee502a68128aea752f24ff67dfcb16b615a36. #117 is not merged. Own exact-HEAD checks
and preliminary review do not imply current-main integration acceptance or merge
permission. No D1/F3 waiver applies to Stage B. Eventual merge requires accepted
prerequisite, current integration tuple, required gates and owner authorization.

After any eventual helper merge, separately granted real D2, legacy LNX-03 and
legacy LNX-02 hosted runs must verify the actual post-merge SHA. Foundation inside
D2 is not a substitute for standalone legacy acceptance. D1 production callers,
D4/D5/D6, A8/B3, full #47/#78 and #66 staging remain external hookups. Production,
staging, MOEX/Google/FTP writes, cron and full reload are not authorized here.

Rollback is a separately authorized thematic revert while publication is stopped.
It restores previous uploader defects and is not production recovery. Keep the
accepted compatibility fixtures, frozen worktrees, environments and all evidence.
