# D2-01: bounded publication supervision

Approved R3: `10aac6c50f5b30eb8a1b65e102184efd3a6aa3bdd5e1c1e3aa892389e3c3fa99`
(owner-supplied plan digest). Implementation base:
`42830d91b4c701b899cbadb5ebdf8829eec6b5cf`, tree
`0ffb53a271a64da0eeac2b3779a508aee1817636`. Legacy red-before is bound to
`upload.py` blob `dc1f12bc63eadebe589a666ffcb6a0a2bb908747` at `3320b44df3d825e9c4626377943bdfaf03c019d1`.
The four intervening C2 paths do not overlap D2. C2 failure still stops later stages.
Owner-accepted R3.1 and R3.1a supplement R3 with phase-separated checkpoints and
pre-seal finalization faults. The base, receipt contract and fifteen-path scope
remain unchanged.

## Outcome contract

Both Google and FTP are required children. A normal function return, actual exit
zero and a confirmed real F-LOG worker final are separate observations. Missing
final, abnormal exit, start fault, deadline expiry, uncertain settlement or
diagnostic failure excludes completion. Parent observations never manufacture a
worker final. Parent-owned F-LOG registration records both children, including
not-started and failed-start outcomes.

An output starts `NOT_ATTEMPTED`. Before a remote mutation, its full checkpoint
must persist `UNKNOWN`; after the operation returns it may persist
`ACKNOWLEDGED`. ACK is local operation acknowledgement, without verified remote
commit/readback. Attempted outputs keep commit state `UNKNOWN` and belong in
`unknown_outputs`, including acknowledged transfers. `committed_outputs` is
empty. Known unattempted outputs belong in `uncommitted_outputs`. Missing or
invalid worker evidence cannot prove that a remote mutation did not occur.

These receipts are D2 diagnostics. They do not supply a D4 `release_result`,
release universe, current generation, verified previous preservation or recovery.
No FTP or Google readback is added.

## Call and logging ownership

`main.main(*, logging_context=None, result_sink=None)` passes both keyword
arguments through `run_stage(..., **kwargs)` to `upload.main`. The legacy call
without context or sink retains the two positional upload arguments and successful
`None` return. The upload API adds keyword-only logging context, result sink and
reduction-only diagnostic limits. Dataset schemas and publication arguments stay
unchanged.

The pipeline CLI creates one run ID and one parent session, passes its sink to
`main`, then finalizes that session exactly once. Upload registers `google` and
`ftp`; each child configures its own producer and sends its actual final.
Inherited-context upload never finalizes the parent. Standalone upload owns its
own parent session. Logging summary schema and `run_logging.py` stay unchanged.

The persisted parent receipt is written before F-LOG finalization:

```text
receipt_scope=PREFINAL_SUPERVISION
finalization_state=NOT_OBSERVED
diagnostic_success_allowed=false
```

It remains immutable afterwards. A successful receipt write followed by failed
finalization leaves the final in-memory sink and CLI nonzero/`INCOMPLETE`, with
sealed state `UNKNOWN` if no actual report exists. Completion requires the real
same-run F-LOG report: exit zero, `COMPLETE`, `storage_sealed=true` and
`evidence_incomplete=false`, together with both successful child observations.
Completion confirms local operations and diagnostics; it never announces a
verified release.

The two finalization fixtures first require two distinct, actually settled
exit-zero children, valid normal-return checkpoints, real COMPLETED/zero worker
finals and a persisted PREFINAL receipt. `parent_finalization_raises` injects at
freeze entry, before a successful summary or seal. The application CLI/sink stays
nonzero/INCOMPLETE with sealing UNKNOWN. Independent fixture cleanup then invokes
genuine FAILED/nonzero finalization; its report is labelled TEST_CLEANUP and never
replaces the application sink. `parent_finalization_incomplete` injects at seal
inventory after transport closure but before successful seal, obtaining the
unchanged logger's actual INCOMPLETE, unsealed report. Injection counts, pre-seal
state and unchanged persisted bytes/hash are separate oracles. `run_logging.py`
is unchanged.

## Read-only source preflight

The existing module-global `datasets_path` remains the legacy source. Before
child launch, enumerate only regular files directly in its root, `dividends` and
`ticker_lists`. Keep destinations: root basename, `dividends/name` and
`ticker_lists/name`. There is no extension filter or recursive scan. Root README
files remain included; nested `dohod.ru` remains outside this scan.

All three directories must exist and be readable; empty directories are valid.
The existing Google CSV must exist and be readable. Missing directories, stat,
enumeration or open failures, links/reparse aliases and normalized case collisions
refuse launch. Stable logical output IDs enter diagnostics; private source paths
and remote destinations stay in the execution plan.

Read-only opens compare file identity, size and modification metadata. Freeze the
plan and recheck it before each start and before releasing both READY children.
Workers use frozen entries and check source identity before/after reads, without
discovering additional files. Late source changes fail with honest partial/UNKNOWN
outputs. Metadata checks do not eliminate TOCTOU or same-metadata replacement;
canonical source-contract acceptance remains A8/B3/D4 work. Discovered files are
not an approved complete release universe.

## Budgets and persistence

The default work budget is 3600 monotonic seconds from immediately before the
first start, shared by both starts, bootstrap/import, readiness, work, checkpoints,
worker final ACK and OS settlement. Polling is 0.1 seconds. Failure requests peer
cleanup using one shared ten-second budget: termination, then kill/settlement
observation (five seconds for termination, then the remaining five for a second
request and OS settlement). Kill requests are not settlement proof. A remaining live child
excludes success. Windows kill and terminate use the same native operation.

Preflight and logger configuration precede the work budget. Cleanup and existing
F-LOG shutdown are separate. This is not an absolute 3600-second wall-clock
runtime guarantee: `Process.start()`, serialization and OS I/O may block.

Diagnostic maxima are a proposed conservative profile, not an existing owner
capacity policy or verified production inventory:

| Limit | Maximum |
|---|---:|
| FTP output tokens | 512 |
| Google output tokens | 1 |
| Full worker checkpoint | 65,536 bytes |
| Full parent result | 131,072 bytes |
| Owned diagnostic run files | 1,048,576 bytes |

Only positive integer reductions of these four public diagnostic limits are
accepted; booleans, unknown keys and expanded limits fail. More than 512 FTP files
refuses before mutation. No output list is truncated. This profile is unrelated
to Excel/Google product capacity decisions; a broader inventory/capacity policy
needs an owner decision.

JSON is fully serialized as UTF-8, ASCII escapes, sorted keys, compact separators,
no NaN and a trailing newline before measuring or writing. The six owned files
are two worker checkpoints and their pending files, plus parent receipt and its
pending file. Writes flush/close before replacement. This is not a power-loss
durable dataset writer. A one-way shared byte releases work only after both READY
and healthy logging; structured results travel through bounded checkpoints.

| Checkpoint phase | Admitted activity |
|---|---|
| Before GO | Each worker publishes immutable READY and waits. Parent validates both READY records and closes all owned readers before the stable inventory and remaining gates. |
| GO release | Both READY records, closed reader lifetimes, source identity, logging flush/health and monotonic deadline must pass. |
| After GO, worker unsettled | Parent observes retained process handles, exit status, deadline and logging health; it does not open or inspect that worker's mutable checkpoint leaf. |
| Worker actually settled | Parent may read that worker's terminal checkpoint; sibling leaves remain inaccessible while that sibling is unsettled. |
| All started workers settled | Parent performs full inventory and may publish the PREFINAL receipt. Unsettled cleanup withholds aggregate persistence. |

A READY publication attempt freezes the worker's pre-GO checkpoint even if that
attempt raises. Its actual worker final/exit still reports failure; parent never
fabricates a terminal record. After GO, missing/corrupt/stale/partial evidence
fails closed with applicable UNKNOWN outputs. An actual kill request is not
settlement. Standard `os.replace` remains; no native rename adapter or retry is
introduced, and an arbitrary foreign reader is not bypassed.

Live workers validate a frozen, recomputable reservation and inspect only their
own checkpoint/pending leaf metadata and handles. Root filename enumeration uses
a closed directory iterator without inspecting sibling leaves. Exact identities,
output tokens and reduction-only limits regenerate full serialized bounds:
`2 * (Google bound + FTP bound + parent bound) <= max_run_bytes`. Each own file
must fit both its reservation and the existing per-file cap. Forged bounds,
aliases/reparse/hardlinks, an oversized existing own file or an existing pending
file refuse publication. Independent full inventory retains all physical-byte and
security checks in stable phases. Allowed filenames are separate from expected
existence: successful READY has two worker JSON files; successful parent
publication has three JSON files; pending files must be absent. Failure evidence
is preserved rather than deleted or silently reused.

On the accepted Windows profile, checkpoint reads use `CreateFile` with read
access, `OPEN_EXISTING`, `FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE`,
`FILE_FLAG_OPEN_REPARSE_POINT | FILE_ATTRIBUTE_NORMAL` and null security/template
arguments. The non-inheritable handle transfers once to a non-inheritable binary
CRT descriptor. A bounded read closes its stream and descriptor before parsing;
conversion failure closes the still-owned native handle. Phase separation avoids
parent read/replace overlap: READY readers close before GO and terminal readers
open only after actual OS settlement. Legacy Windows `os.replace` can fail with
an open destination despite this share mask; that conflict remains a negative
oracle. Exact builtin
bindings, path/identity admission, all seven native arguments and ownership
transitions have separate guarded oracles. No retry, lasting descriptor grant or
foreign-handle cleanup is added.

Independent byte-boundary and boundary+1 oracles cover complete serialization,
512 rows, every failure transition and preservation of prior checkpoint bytes.
Schema, identity and sequence oracles are separate from byte admission.

## Windows launch boundary

The supported adapter profile is official Windows x64 CPython 3.14.8 GIL with
verified executable/stdlib source and compatible bindings. Unsupported or changed
private API shapes safely refuse; there is no unrestricted inheritance fallback.

`-I -S -B -X utf8` is startup policy; flags do not redirect standard handles.
The adapter opens private non-inheritable read/write NUL bases and duplicates
exactly two inheritable handles. A copied `STARTUPINFO` sets
`STARTF_USESTDHANDLES`: stdin is read-NUL; stdout/stderr are write-NUL. Its explicit
`handle_list` contains only those two duplicates and `bInheritHandles=true`.
Parent bases and duplicates close immediately after CreateProcess return/failure;
the child owns inherited copies. Caller, logger, bootstrap-pipe, Job and foreign
handles are excluded from this inheritance list. Multiprocessing later transfers
its required pipe/Queue/SemLock resources through the pinned preparation/reduction
protocol; offline admission binds those transfers to the issued bootstrap.
Python stream/FD redirection occurs before spawn imports.

Thread/request-scoped hooks preserve and delegate to the current admitted
CreateProcess wrapper, including the offline suspended-create/Job wrapper.
The adapter retains a non-inheritable actual process-handle witness before
serialization. Partial start failures use that witness; matching vendor finalizer
ownership is reconciled after settlement. Unknown correspondence is failure.
Cleanup never reopens a PID and never runs unrelated finalizers.

Spawn preparation restores parent `sys.path` despite `-S`; it does not execute
`.pth` startup effects. Actual dependency compatibility, bootstrap/import/unpickle
failure canaries, NUL lifetime and foreign-handle object effects require guarded
native acceptance. A concurrent unrelated inherit-all process creator could
inherit temporary NUL duplicates. D2 does not claim global handle isolation or an
OS sandbox. Test-only Windows Jobs contain tests and do not protect production.

## Platform and adoption matrix

| Platform / caller | Acceptance boundary |
|---|---|
| Pinned Windows 3.14.8 x64 GIL | D2 offline native profile; exact-head evidence required |
| Other Windows/Python/frozen/service profiles | Unsupported until separately accepted |
| Known production Ubuntu VPS | OPEN R1; Windows adapter evidence does not establish Linux operation |
| Linux/macOS other callers | OPEN R1 platform hookup and adoption |
| D1 production callers | NOT CONNECTED by D2; fixture lock evidence is narrower |

## Regression and residual acceptance

Red-before loads the original baseline blob, with real controlled legacy targets.
It proves four swallowed remote errors, ignored actual exit 23 and a genuinely
blocked child in a safely bounded external Job. It does not expect a new helper
or receipt on the baseline.

Green cases cover both start faults before/after native creation, abnormal exit,
SystemExit zero/string, real 15-second D2 parent expiry, peer cleanup and actual
retained-PID settlement, negative unsettled cleanup, partial Google/FTP UNKNOWN,
corrupt/missing/oversize/stale checkpoints, listener/final ACK failures, one run ID,
real worker finals, immutable prefinal receipt, canary redaction and native handle
inheritance. Literal projection/order/write arguments and original fixture
bytes/hashes remain independent oracles.
R3.1a adds reader closure before GO, settled-only terminal reads, live sibling
refusal before metadata access, slot-local writer effects, forged/reduced
reservation refusal, pending-file preservation and final full inventory. Native
phase witnesses correlate ticket/PID/actual exit with retained-handle settlement;
worker-final counters require zero peer-slot attempts. Full inventory separately
records closed readers, regular single-link files and graph quiescence. Pure unit
models are distinct from these native observations.

| Requirement | Scoped evidence |
|---|---|
| #39/#41 required child failures propagate | Native baseline red and current nonzero outcomes |
| Bounded supervision, no false success | Parent deadline and independent OS/PID settlement oracles |
| F-LOG handoff | Same-session/two-worker finals and finalization-fault checks |
| Publication interfaces | Literal Google projection/arguments and FTP shallow destinations |
| Diagnostic privacy and budgets | Negative canaries, full-schema and byte boundary+1 checks |
| #78 integration subset | These offline observations only; full pipeline remains OPEN |

D4 release/current/previous/readback, D5 capacity/readback/recovery, D6 protected FTP
transport/readback, A8/B3 source contracts, D1 production caller adoption, #47/#78
hosted/global integration and #66 staging stay open. No production, staging, cron
or live publication is authorized by this PR. Rollback requires a separately
controlled revert with publication stopped; it restores the former D2 defects
and is not production recovery. Stop after PR and independent CODE_REVIEW handoff.

## Historical pre-amendment BLOCKED evidence

The fifth working-tree D2 run collected 176 tests: 174 passed, two failed,
exit 86. The live-reader replacement oracle failed. The parent-finalization-raises
case also failed before its intended boundary: FTP checkpoint replacement had
`errno=13`, `winerror=5`, followed by an existing pending-file failure. Reader
lifetimes completed; these observations do not prove the exact temporal overlap
at that FTP call. CPython 3.14.8 uses legacy `MoveFileExW` for `os.replace`.
[Microsoft documents separate POSIX rename semantics for open-target replacement](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information).

At that stop the reader-only change was insufficient, the intended finalization
boundary was not reached, and implementation/tests were stopped. The five
historical test sets, BLOCKED handoff, code, fixtures, raw local logs, immutable
environment and reservations remain preserved. The then-passing ten-pair
working-tree notebook check was narrower than final exact-head acceptance. These
old failures are not reclassified as success.

The owner subsequently accepted R3.1a and explicitly resumed implementation
under the existing H/SETUP grants. Two working-tree checkpoints passed:

| Closed scope | Actual tests / exit | Duration | Run ID |
|---|---|---|---|
| `previous-failures` | 2 passed / 0 | 3.15 s | `98a26eefbb4b48e394d40bccdae004b4` |
| `r31a-contract` | 57 passed / 0 | 85.79 s | `fd336401949a4faf9e92336c60ad9530` |

Both bind tested base `42830d91b4c701b899cbadb5ebdf8829eec6b5cf` and working-tree
source digest `6e6cf094b1ba2e8aab555eb0946a9c25de97c711b15eaeadb3d1255bd54b7119`.
They precede subsequent guard/document refinements and do not establish final
HEAD acceptance. The initial resumed launcher-format error before collection is
preserved separately and is not a behavioral test PASS. Scoped repeat, full D2,
all five existing lanes and exact-clean-HEAD notebook/environment checks remain
required. No final exact-HEAD acceptance or PR readiness is claimed here.
