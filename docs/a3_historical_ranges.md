# A3-FINAL historical acquisition

Author implementation for independent review: one final A3 PR, no planned
A3-04. A3-01 and A3-02 remain accepted prerequisites. Implementation acceptance,
source verification, issue #6 full acceptance and production verification are
separate statuses. This document grants neither live access nor publication.

## Calendar planner and union

`moex` freezes one local calendar anchor and constructs N logical annual windows
with A3-02 anniversaries, leap recovery, portable `YYYY-MM-DD` and upper halo.
The complete schedule is built before GET. Legacy zero/negative/bool/no-index
behavior remains; calendar underflow still refuses before GET.

| Interval | Physical cores per full logical year | Calls for N logical years |
|---|---|---|
| 24 (1d) | annual | N |
| 60 (1h) | annual | N |
| 10 (10m) | 12 anchored calendar months | 12N |
| 1 (1m) | successive spans of at most 7 calendar days | 53N |

Monthly anniversaries use the original upper day, clamped independently to the
target month's last day. Weekly cores move back exactly seven days. Each core
`[L,U]` requests `from=L, till=min(logical_request_upper,U+1 calendar day)`.
The newest leaf cannot extend the outer upper bound. Halo is applied once,
including at annual joins; it is not recursively extended. Closed/closed
date models can repeat two dates at a join. Models do not establish actual ISS
inclusivity, timestamp selection, availability or finality.

Explicit caller spans first become annual cores anchored at their upper date;
the oldest core is clipped to the actual lower bound. Equal bounds remain one
literal request for all four intervals. Invalid/reversed bounds refuse before
GET. Every step decreases the remaining date ordinal, so planning terminates;
8,192 planned leaves is the finite job admission cap. All planned empty leaves
are executed: an empty recent period never means older history is absent.
No failed native query is replayed with smaller dates.

All original nonempty physical frames remain flat until `_a3_assemble` audits
every numeric scalar, including equal repeats. First-row locators avoid a second
copy of nine cells per key. The key remains request-bound:
`ISS_CANDLES_BOARDLESS / stock / market / security / interval / original begin`.
Global board identity is not inferred. Compare all nine values, using exact
integer/float ratios and signed zero. A late conflict refuses against the first
representative. Only confirmed equal repeats are removed, then stable begin
sorting and RangeIndex are applied. Unsafe int64 `9007199254740993` plus float
promotion refuses; no casts, rounding, keep-first conflict repair or filtering
of out-of-bound returned membership is introduced.

## Callers and refusal

`moex`, `full_reload`, existing/new `data_update` acquisitions, and missing-file
nested reload share the mechanism. Reload preserves listing/cutoff/stopped
precedence, 10/30-year history and years=0 sentinel. Update preserves stored-end
incremental requests, current 30-day filter and one-year new-ticker bootstrap.
Returned ticker values still determine delta in catalogue order.

Reload collects nonempty ticker frames and concatenates once without resetting
their indices. Update retains previous, existing results and bootstrap results
in order, including empty dtype participation, then concatenates once and keeps
its existing sort/full-row dedup/reset. Both concats audit every original number
before writer admission. Inputs are released when their audit is complete.

Acquisition, terminal, conflict, numeric, resource and diagnostic checks must
finish before the affected dataset's first existing writer. Failure returns no
prefix, starts no next leaf/ticker/dataset and preserves previous bytes. Cold
start also preserves an existing XLSX when CSV is missing. This is not a
transaction across datasets, formats or remote targets; writer-stage rollback
and release remain D/R1. A6 revision/update policy is unchanged.

Raw OHLC/timestamps, nine columns/order, RSI14/indicators, filenames, Excel row
policy, Google/FTP/SFTP and publication interfaces remain unchanged. More
complete acquisition can add previously omitted source rows to a future
authorized output. No historical dataset is rewritten by implementation tests.

## Hierarchical finite resources

Counters cover original admitted rows, actual GET attempts/retries and returned
read bytes, including terminal reads. They are not wire-byte estimates. Previous
rows count once; concat copies do not count as new intake. Run counters survive
datasets and nested cold starts; job counters survive all logical windows.

| Limit | Ticker job | Dataset | Run |
|---|---:|---:|---:|
| Physical ranges | 8,192 | 1,048,576 | 4,194,304 |
| GET attempts | 262,144 | 16,777,216 | 67,108,864 |
| Returned read bytes | 8 GiB | 128 GiB | 512 GiB |
| Original rows | 24M | 128M | 512M |
| Final frame payload | 8 GiB | 16 GiB | dataset cap reused |
| Accounted simultaneous frames | 16 GiB | 32 GiB | 32 GiB |
| Accounted key metadata | 8 GiB | 8 GiB | 8 GiB |

Run also admits at most 64 datasets and 4,096 ticker jobs. Cooperative budgets
are `Tjob=min(96h,180s*planned_ranges+6h)`,
`Tdataset=min(96d,sum(Tjob)+24h)`, `Trun=256d`.
Private execution evidence has a 32 GiB quota with 1 MiB reserved for a failure
receipt. Acquisition creates no spool or new dataset writer. These are maxima,
not permission to run a reload or a promise about actual duration.

Each native `moex_query` keeps 180 seconds, 1,000 pages including terminal,
64 MiB/query, 8 MiB/body, retry/header/Retry-After/cleanup and the complete
3-second success sleep. Direct calls without a private ledger keep their native
contract. Native checks run before aggregate checks at shared checkpoints. An
existing primary object/type/args survives later budget, evidence or logging
failure. Without a primary, aggregate refusals use
`A3_ISS_RANGE_CAPACITY_EXCEEDED` or `A3_ISS_RANGE_DEADLINE_EXCEEDED`.

## Native memory admission and development profile

Deep frame payload measures retained objects; it does not prove peak RSS.
Windows x64 admission reads physical availability, process private commit/working
set, system commit pages multiplied once by PageSize, and process headroom.
Reserve `max(8GiB,25% total physical)` and `max(8GiB,10% commit limit)`.
Require `2*G+1GiB <= min(physical,commit,verified process/job headroom)`.
Probe age must be at most one second. Parsing, Python cells/keys, evidence and
assembly copies contribute to G; large previous CSV reads are admitted before
reading. Recheck before leaves and material allocations. Snapshot is not a
reservation; native MemoryError still refuses without replacing its identity.

`QueryInformationJobObject(NULL)` exposes only the immediate job. Neither a
DataFrame measurement nor system/application available commit proves all nested
ancestor quotas. If the process belongs to an unverified job chain, large
admission fails closed. Unavailable/stale/malformed probes also refuse. No
claim that 32 GiB frames plus 8 GiB metadata fits an arbitrary machine is made.
See [Microsoft's job query contract](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-queryinformationjobobject)
and [native memory fields](https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex).

The explicit existing guarded offline report envelope selects a separate small
profile: 100,000 total rows, 32 MiB returned bytes, 64 MiB final frame,
128 MiB accounted live/growth and 64 MiB metadata. This is not production memory
admission; markers grant no source access and never disable the guard. Native
test commands retain 20-minute maximum, stricter inner timeout, 32 processes and
8 GiB aggregate committed-memory hard cap; missing host admission selects the
1 GiB scaled Job. Benchmarks use disposable data only (60s/100k rows). Full
A/F1/F2/F-LOG/F3 inventories remain mandatory; unavailable lanes are not PASS.

## Feasibility, not source promises

The independent fixture models 10/30-year horizons, four intervals and
1/10/50/200 instruments. Dense means a synthetic candle every interval over
every calendar date; sparse means one per active date; empty means no candles;
inactive90 keeps only the last tenth of the horizon. Unknown availability never
permits skipping inactive leaves. A 500-row synthetic page model includes all
empty terminals and the full sleep. These are sizing models, not ISS contracts.

| 200-instrument empty workload | 10 years | 30 years |
|---|---:|---:|
| 24/60 ranges = GETs; minimum sleep | 2,000; 1.67h | 6,000; 5h |
| 10 ranges = GETs; minimum sleep | 24,000; 20h | 72,000; 60h |
| 1 ranges = GETs; minimum sleep | 106,000; 88.33h | 318,000; 265h |

Sparse/empty sizing passes count/time/payload caps for all four instrument
counts, subject to actual native admission/latency. Dense 200x30-year minute
would retain 3,155,904,000 unique rows, 4,071,168,000 original rows with overlap,
about 767.12 GiB at the model's 261 B/row, and about 470.27 GiB CSV at 160 B/row.
Dense 200x10-year minute would retain about 255.73 GiB. They must refuse capacity
under the current whole-frame contract. Spooling does not eliminate the final
allocation. Successful dense acquisition at that scale requires a separate
D-owned streaming/export interface or a different admitted runtime; this PR
neither grants nor implements that cross-lane change. Earlier memory/time
refusal remains possible even for models below final row/payload caps.

## Evidence, logging and F07

Range conflicts add private `a3_range_evidence`: two typed rows, 64 KiB cells and
16 KiB metadata, with logical/physical ordinals, bounds, dtypes and request
identity. Annual evidence compatibility remains. This is parsed evidence, not
raw CSV/page/codec/SHA provenance. Native raw evidence and privacy caps remain.
Secondary evidence failure preserves the primary; no production evidence writer
or public diagnostic bundle is added.

Closed F-LOG typed fields remain unchanged; `fields={}`. Bounded messages record
planned/completed ranges, GET/retries/bytes/rows. Caller correlation wins when
typed and bounded. Native query success follows terminal and native assembly;
`a3_ranges_returned` follows the entire plan, lossless union and final checks,
with outcome `CLIENT_PLAN_COMPLETE_SOURCE_UNVERIFIED`. Range capacity/conflict
errors are final. Existing native errors retain native logging precedence.

F07 is a ready protocol/offline harness, **not executed live**. After a separate
live grant, compare candidate to independent reference captures for both SBER
and MOEX: 24/60=`2023-02-27..2024-03-02`,
10=`2024-01-30..2024-03-02`, 1=`2024-02-23..2024-03-02`.
Whole diagnostic limits: 45 minutes, 512 GETs, 128 MiB returned bytes,
250,000 rows, 128 MiB frame and 256 MiB evidence. Record actual bounds/offsets,
retry/sleeps, raw bodies/codec/SHA, counts/keys/numbers and capture times. The
private accounting scope enforces candidate limits; the reference harness must
count both candidate and reference traffic against the same outer limits.
Only approved candles/candleborders endpoints are eligible. No reload/update or
publication. Changed reference/candidate captures yield SOURCE_CHANGED /
NOT_VERIFIED, not a repaired history. A limited successful diagnostic proves
only its observed ranges; borders do not prove internal membership or finality.

## Acceptance and rollback

| Requirement | This PR | Remaining acceptance |
|---|---|---|
| F01 / #21 | Calendar logical bounds, physical coverage/halo, termination | Actual ISS date/inclusivity semantics remain source validation |
| F02 / #6 | Hierarchical caps, native admission, explicit exhaustion | No universal successful dense workload claim |
| F03 | Flat lossless audit, exact conflicts/dedup, bounded evidence | Global source/board identity is A5 |
| F04/F05 | All four callers, one concat, native primary and affected previous | D/R1 writer/release/remote transaction remains separate |
| F06 / #47 | Independent fixtures, native gates, notebook pair, exact-HEAD CI | Independent CODE_REVIEW required; Linux syntax is not Linux A runtime |
| F07 / #6 | Eight-case protocol, limits and offline/reference checks | Separate live grant and A5 identity/availability/finality evidence |
| #78 | Closed local F-LOG events and delivery refusal | Global F/D/R1 staging/release integration |
| #76 | One final implementation PR; no planned A3-04 | Source/production and other package lanes remain open |

Before merge, rollback is a reviewed revert of the exact ten-path feature diff,
including both notebook-pair members. Retain worktrees, previous datasets,
original RED, all failures/recovery/warnings and sealed evidence. No reset,
force-push, main merge/revert, issue closure or production action is authorized.
