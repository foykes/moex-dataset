# A3-01: bounded ISS candle pagination

Status: author implementation for independent CODE_REVIEW. PLAN_ACCEPT and the
fresh owner-direct grant authorize this slice; neither is CODE_ACCEPT or release
acceptance. Ownership source: `REPOSITORY_OWNER_DIRECT_GRANT`; the coordinator did
not issue the grant. [Scoped receipt](https://github.com/foykes/moex-dataset/issues/47#issuecomment-6223897056).

## Contract and scope

`moex_query` keeps its public signature, routing, argument order, raw pandas
values, and nine output columns in this order:

```text
open close high low value volume begin end ticker
```

The first structurally valid empty page returns the legacy zero-column empty
frame when no data preceded it. Nonempty results have a fresh `RangeIndex` and
unchanged string timestamps. No timezone, from/till inclusivity, trading calendar,
price adjustment, quality, source availability or global coverage is inferred.
`from=till` remains a literal request, including intervals `24/60/10/1`.

Only these nine paths belong to the slice:

```text
data_gathering.py
data_gathering.ipynb
tests/A/test_pagination.py
tests/fixtures/a3/pagination_cases.json
tests/A/test_iss_transport.py
tests/A/test_iss_validation.py
tests/A/test_iss_retry_policy.py
tests/shared/test_preparation.py
docs/a3_pagination.md
```

The shared grant covers only
`test_candle_retry_503_then_200_has_literal_rows_and_keys`. Other shared code is
unchanged. The notebook pair is one ownership unit; Python is the edit authority.
`moex`, annual windows/leap handling, B2/RSI14/STOCH/TRIX, historical files,
Excel/Google policy, logger, F0, settings, guards, runners, CI and dependencies
remain outside the implementation.

## Pagination, identity and assembly

Start at `start=0`. A successful nonempty page advances by its raw validated row
count. A short page also requires another request. Only a structurally valid empty
page is terminal. Parameters and selected market/security stay fixed across every
page and retry, including terminal retries. Each page keeps A2's finite retry,
header rotation, original-header admission, HTTPS, identity encoding, 8 MiB body
limit, close precedence and mandatory complete 3-second success sleep.

Strictly increasing `begin` is required within each page and between admitted
pages. Any repeated payload, repeated request-bound key or overlap refuses the
whole query, including identical duplicates. No sorting, deduplication, keep-last
or successful-prefix return repairs a refusal. The key is exactly:

```text
("ISS_CANDLES_BOARDLESS", "stock", selected_market,
 frozen_request_security_token, frozen_request_interval_token, raw_begin)
```

This is a local projection for one boardless request. Board is absent from the
request (`NOT_PRESENT_IN_REQUEST_A5_UNVERIFIED`). Full board/source identity and
availability remain A5/source acceptance work.

Keep original page frames and concatenate once, after empty terminal. Homogeneous
numeric dtypes must remain unchanged. A common numeric dtype for heterogeneous
pages is permitted only when every original scalar survives exactly: integer
identity is `(int(value), 1, False)`; finite float identity is its native exact
integer ratio plus the negative-zero bit. Compare each Series independently,
before and after concatenation. Object fallback, unsupported scalar types or any
loss raises `A3_ISS_NUMERIC_ASSEMBLY_LOSS`; no round, downcast or cast-back repair.
This preserves A2's parsed values, not arbitrary decimal lexeme precision.

Literal regressions admit `2000 + 2000.5`, scientific notation, exact even integers
above the consecutive-float boundary, and signed zero. Homogeneous int64 pages
retain `9007199254740993`; that integer combined with a float page refuses after
terminal, with no partial return.

## Finite resources and completeness limits

The cooperative 180-second budget belongs to the entire public call, including
all page attempts, diagnostics, parsing, cleanup, assembly and sleeps. It is not
reset by a page or retry. A3 additionally limits one call to 1000 logical pages
(including empty terminal) and 64 MiB returned `raw.read` bytes. Bytes from failed
attempts and terminal count. The reader admits at most one overflow-probe byte;
this is not a wire-byte or total-RSS guarantee. A nonempty page 1000 refuses
before request 1001. Every refusal discards accumulated results.

Even with zero other overhead, the existing 3-second success sleep and strict
budget admission permit at most 59 completed successful pages, including the
required empty terminal. A multiyear minute request cannot generally complete in
one call. A3-03 owns the inactive follow-up hookup for bounded time subranges or a
separately approved resource strategy. A3-01 does not alter A2 pacing or activate
that hookup. A3-02 separately owns annual-window correctness. Exhaustion after
already staged pages is an explicit offline regression.

## Private bounded evidence

Evidence is a local `primary.a3_evidence` attribute, never a log field, production
writer, public bundle or automatic publication. Runtime persistence is
`NOT_REQUESTED`. It contains at most two records of eight original CSV cell
strings (`RAW_CSV_CELLS_V1`), at most 16 MiB UTF-8 cell payload, and at most 16 KiB
canonical JSON metadata. This limits the attribute; exception tracebacks and
ordinary parsing/frame memory have their normal lifetime.

Preliminary body capture records bytes/hash before codec/parser. Admitted bodies
share the 64 MiB query budget; the key index keeps page/record pointers and
recovers the first version with the existing parser only on a conflict. Both raw
versions survive cross-page overlap. The existing A2 duplicate branch constructs
its original `A2_ISS_RESPONSE_SCHEMA_INVALID` ValueError, then attempts a diagnostic
hook for the first/current record; it does not widen the source profile.

Metadata includes format/reason/state, exactness/omissions, request ID and frozen
request values, endpoint/engine, board/source uncertainty, columns and caps, byte
sizes, capture UTC, and per-record page ordinal/start/attempt/data-record ordinal,
body SHA-256, codec and admission state. Data-record ordinals are one-based among
nonempty CSV data records. Run/snapshot/dataset correlation is recorded when
available. SHA is null with `NOT_VERIFIED` unless independently verified; runtime
does not infer it from arbitrary context. Strings up to 128 encoded bytes remain
exact; larger or unencodable metadata uses incremental UTF8_SURROGATEPASS
hash/length projection with explicit omissions. Unknown objects are not repr'd.

A preliminary capture failure with no source primary is itself terminal, even
when its native type resembles a transient transport error. No retry, partial
return or hidden success follows. Once a source primary exists, evidence capture
or attribute-write failure preserves that same exception object, type and args,
adding only `A3_EVIDENCE_FAILED_UNVERIFIED`. Cleanup and logging retain native
primary precedence. No fallback evidence target is written. Query-owned caches
and indexes are cleared in `finally`.

## Closed F-LOG mapping

No field/schema change is required. Ordinary `fields` stays `{}`. Typed caller
correlation wins, including `page` when it is a string or nonnegative exact int;
the caller context is never mutated. Without that page, page events use numeric
offset and aggregate events use null with `AGGREGATE_QUERY`.

Safe messages contain fixed aliases/codes and internal counters: one-based page
ordinal `p`, zero-based offset `start`, fully validated rows `got`, staged rows
`add`, accumulated staged rows `sum`, actual GET count `get`, actual retries
`retry`, and terminal/outcome token `term`. Raw bounds, bodies, headers and native
exception text stay out of diagnostics. No new typed offset/evidence/completeness
field is introduced.

| Event | Level | Meaning |
|---|---|---|
| `a2_query_started` | INFO | One aggregate operation start |
| `a3_page_started` | DEBUG | Logical page admission starts |
| `a2_query_attempt_started` | DEBUG | Existing admission event, before GET |
| `a2_query_retry` | WARNING | Existing pending retry policy |
| `a3_page_returned` | INFO | Validated/closed/slept page passed cross-page staging |
| `a3_page_terminal` | INFO | Validated/closed/slept empty page, `OFFSET_TERMINAL_EMPTY` |
| `a2_query_returned` | INFO | Once, only after terminal, final assembly/audit and final deadline check |
| `a2_query_failed` | ERROR | Once, terminal aggregate refusal when delivery remains available |

Page retries retain already-called attempts and per-page limit. Aggregate retries
use total actual GET attempts and the bound `1000 * max_retries`; message retry
total is the sum of `max(page_attempts - 1, 0)`. Counts include fully A2-validated
responses, even a later rejected overlap, and valid empty terminal. Invalid-page
rows are unknown; page events give a null reason until validation. Accumulated
staged rows remain distinct in messages. Accepted/quarantined/instruments stay
null with `NOT_ASSESSED`.

No domain check follows successful returned-event delivery. A delivery/health
failure still refuses; an already accepted INFO record cannot be erased.
Diagnostic failures remain outside transport retry handling and cannot trigger a
second request. Native success-sleep interruption retains the A2 policy: preserve
its own object/args without fabricating a terminal logging sequence. Real-writer
level, redaction/canary and native-primary regressions
remain covered.

## Regression and AC mapping

| AC / risk | Offline evidence | Remaining acceptance |
|---|---|---|
| #6 page membership | Independent 1001-row fixture; 0/1/500/501/1000/1001 and 250+250 pages; literal rows/dtypes/keys/index; short-page continuation and terminal | Live/source coverage remains open |
| #6 refusal | Repeated/overlapping/reversed pages; malformed/failed terminal; retry exhaustion; page/byte/time exhaustion after data | Source availability is not proven |
| #6 numeric compatibility | Exact large integer and mixed numeric literal oracles; signed zero; unsafe promotion refusal | No decimal lexical precision claim |
| #21 request bounds | `from=till`, shares/bonds × 24/60/10/1; all parameters fixed on pages and retries | Annual/leap/window ordering remains A3-02 |
| Evidence / native errors | Both raw conflict versions; local A2 duplicate; preliminary capture faults with/without prefix; secondary metadata/cap/attribute faults; same native primary | Private local evidence only |
| #78 logging | Page order/counts/retries/caller precedence; delivery faults; real F-LOG canaries/redaction and primary precedence | Global logging integration remains F |
| #47 caller integration | Actual full_reload/data_update late-page faults; no new writer; affected dataset's previous bytes/hashes preserved | Broader integration remains open |
| D/R1 | Local affected-dataset fixture preservation only | NOT_CONNECTED: no multi-dataset/all-format/remote transaction or release claim |
| Notebook / guard | Indivisible pair sync; unchanged native A/F2/F-LOG/F3 boundaries; clean exact-HEAD validation | Local borrowed runtime does not satisfy owning-worktree F1; hosted CI is separate |

Fixture origin is synthetic and independent of production helpers. SHA-256:
`5c7951bfecd46fe54c484876eb78c4c614cfae89d4f7f6b56d13d524453a63f0`.
Tests never call MOEX or production writers. Guarded working-tree RED/GREEN is
retained separately from final clean-commit evidence. Final handoff supplies
actual BASE/HEAD/tree, nine-path diff, command exits/counts, native run identities,
source/runtime fingerprints, hosted exact-HEAD CI and self-review; collection is
not runtime PASS, and skipped CI is NOT VERIFIED.

Publication risk: callers now receive all admitted pages rather than a successful
prefix, or a terminal refusal. Successful raw CSV/XLSX dataset contents can
therefore contain additional source rows. Filenames, formats, column schema and
publication policy are unchanged. Local fault tests protect previous only for
the affected dataset before its writer is reached; they do not promise atomicity
across datasets, formats or remote targets.

Rollback is a separate reviewed revert of this one slice's nine-path diff,
including both pair members. No dataset rollback/publication is executed here.
Stop at one draft PR and independent code-review handoff (`Refs #6, #21, #78`);
do not merge, close issues, or activate A3-02/A3-03/A5/F/D/R1 hookups.
