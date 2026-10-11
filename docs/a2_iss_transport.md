# A2-01 HTTPS and observable ISS refusal

This slice implements accepted A2-01 R2 for issues
[#20](https://github.com/foykes/moex-dataset/issues/20),
[#22](https://github.com/foykes/moex-dataset/issues/22),
[#24](https://github.com/foykes/moex-dataset/issues/24) and the local domain
contribution to [#78](https://github.com/foykes/moex-dataset/issues/78).
The reviewed source base was `2120fb53e1acd0bf0e30620bda2a3faca7a2bdeb`.
The owner-approved implementation base is
`3320b44df3d825e9c4626377943bdfaf03c019d1`, on `codex/mds-a2-01`.
Continue on that pinned base when independent changes reach main; report drift
and integration risk without rebasing, merging or resetting automatically.
Actual ownership, interface, mandatory-contract, guard-compatibility,
security or functional conflicts stop the affected work for coordination.

## Ownership and compatibility

The exact six-path allowlist is:

```text
data_gathering.py
data_gathering.ipynb
tests/A/test_iss_transport.py
tests/A/test_cold_start.py
tests/shared/test_preparation.py
docs/a2_iss_transport.md
```

The script/notebook pair is one A ownership zone. The
[coordinator receipt](https://github.com/foykes/moex-dataset/issues/47#issuecomment-6101523501)
releases only
`test_candle_retry_503_then_200_has_literal_rows_and_keys` inside the shared
file for request-double and transport-expectation adaptation. It does not
transfer the file, shared fixtures, guard, logger, schema, registry or CI.
Its existing literal rows, values, natural keys, index and delays are retained.
Local previous-file bytes/hash assertions are retained. A cold-start mock now
accepts keyword-only `logging_context`; its domain assertions remain intact.

Market routing, date windows, intervals, candle values, string timestamps,
the nine legacy columns and their order, filenames and successful return/index
semantics remain unchanged. Raw OHLC/history, RSI, metadata/schema, Excel and
Google policies are outside this slice. No release or remote-preservation
guarantee follows from a local previous-file check.

## Transport and caller refusal

`moex_query` requests the existing candle endpoint through HTTPS with
`verify=True`, `allow_redirects=False` and `timeout=30`. Every 300-399 response
raises native `TooManyRedirects` with `A2_ISS_REDIRECT_REJECTED`; its Location
is never requested. Native `SSLError` is raised immediately. Neither case
rotates headers, sleeps, retries or falls back to HTTP.

Other failed attempts retain the legacy policy in this slice. Five attempts
are the default; retry sleeps are 3, 6, 12 and 24 seconds, with the existing
header rotation. A successful request still sleeps three seconds before its
returned event and DataFrame return. An interrupted/failed success sleep emits
no returned event. Exhaustion
raises the final attempt's native primary exception instead of returning an
empty frame. A non-200 final response supplies native `HTTPError` with that
response; a preceding timeout does not replace it, or vice versa. `moex`
records the failed ticker and re-raises, without returning accumulated partial
history.

The existing positional interfaces are retained. Only keyword-only
`logging_context=None` is added to `moex_query` and `moex`; context is forwarded
through all four candle call sites inside A. `main.py` is untouched.

Before any request, `max_retries` must be a positive exact integer, excluding
bool. `retry_sleep_start` must be a finite, nonnegative exact int/float,
excluding bool; invalid arguments raise `ValueError('A2_RETRY_ARGUMENTS')`.
User-supplied `max_retries` has **no general resource cap**. Finite attempts
under the default do not establish a bounded resource policy. The existing
non-streaming response has no body cap, and the request timeout is not a total
download deadline. Decoder/body limits, deadline and resource-cap policy remain
A2-03.

## Closed diagnostics and failure precedence

New diagnostics are assembled from fixed codes, source `MOEX_ISS`, endpoint
alias `ISS_CANDLES`, the known market `shares`/`bonds` and bounded primitives.
Console lines are at most 256 characters. They never interpolate raw
exception text/args, instrument input, URLs, headers, response bodies or old/new
User-Agent values. Header changes use the fixed `HEADER_ROTATED` diagnostic.
Displayed numeric values are exact int/float primitives, finite and within
0..1,000,000,000, otherwise `UNKNOWN`. Displayed dates are exact ten-character
calendar-valid `YYYY-MM-DD` strings reconstructed from parsed components,
otherwise `UNKNOWN`. These display rules do not modify request inputs or
dataset identity.

Every event has `stage='data_gathering'`, `fields={}` and existing nullable
F0 correlation fields with explicit null reasons. There are no new metadata
fields or `CollectionStatus` values. The query outcomes below are observations
of the legacy parse, not validated collection status.

| Event | Level | Outcome | retries / error |
| --- | --- | --- | --- |
| `a2_query_started` | INFO | `QUERY_STARTED` | `{attempt: 0, limit: N}`; error null |
| `a2_query_attempt_started` | DEBUG | `REQUEST_STARTED` | `{attempt: i, limit: N}`; error null |
| `a2_query_retry` | WARNING | `RETRY_PENDING` | `{attempt: i, limit: N}`; last-attempt error, final false, retryable true |
| `a2_query_returned` | INFO | `QUERY_EMPTY_UNVALIDATED` or `QUERY_RETURNED_UNVALIDATED` | `{attempt: i, limit: N}`; error null |
| `a2_query_failed` | ERROR | `TLS_FAILED` | `A2_ISS_TLS_FAILED` / SOURCE; final true, retryable false |
| `a2_query_failed` | ERROR | `REDIRECT_REJECTED` | `A2_ISS_REDIRECT_REJECTED` / SOURCE; final true, retryable false |
| `a2_query_failed` | ERROR | `RETRY_EXHAUSTED` | Last-attempt code/category; final true, retryable false |
| `a2_query_failed` | ERROR | `CONFIG_REJECTED` | `A2_RETRY_ARGUMENTS` / CONFIG; attempt 0, retries null with `INVALID_CONFIGURATION`; final true, retryable false |

The other attempt codes/categories are `A2_ISS_TIMEOUT` / TIMEOUT,
`A2_ISS_HTTP_STATUS` / SOURCE, `A2_ISS_REQUEST_ERROR` / SOURCE and
`A2_ISS_PARSE_ERROR` / SOURCE. For terminal transport failures, retries records
the actual `i,N`. Here retryable means another legacy attempt is scheduled;
it does not claim the A2-03 eligibility classification. `final` means this
attempt chain has ended.

Full error records use exactly the common error keys: category, code, stage,
retryable, final, message, run_id, snapshot_id, dataset_id, instrument, interval,
page, file, artifact_id, target_id, endpoint, attempt, exception_type and
null_reasons. Messages use safe bounded components, never the primary's raw
args/str/repr. Attempts and known native exception type use their existing
fields. Unknown correlation has explicit reasons rather than invented IDs.

Returned counts contain measured `rows_received=len(parsed_frame)`;
`rows_accepted`, `rows_quarantined` and `instruments` are null with
`NOT_ASSESSED`; `pages` is null with `NOT_PAGINATION_EVIDENCE`. Failure does
not fabricate zero-row completeness. `duration_ms` is measured nonnegative
finite query elapsed time through completion of the mandatory success sleep,
including earlier retry sleeps when present, before returned-event delivery.

HTTP status, requested range and decoded-body byte length may appear in the
bounded human message and local fixture evidence. They are **NOT_CONNECTED**
as typed machine-readable F-LOG fields: the coordinator must own any contract
change and its integration regression. `len(response.content)` measures
decoded response bytes, not wire bytes. It proves neither a cap nor a deadline.

With no logging context, the safe console fallback prints INFO/WARNING/ERROR;
attempt DEBUG is silent. With context, F-LOG exclusively owns normal console
delivery and applies its console_level; A2 adds no direct console duplicate.
INFO/DEBUG/WARNING require accepted receipt and healthy logger; ERROR also
requires confirmed receipt. Logging and health checks sit outside the
request/parser catch, so a logger failure never triggers an HTTP retry.
If a primary failure already exists, preserve the same exception object/args
and stop; secondary diagnosis attempts only the fixed stderr message and
exception note `A2_LOGGING_FAILURE: LOGGING_INCOMPLETE`. Without a primary,
raise `RuntimeError('LOGGING_INCOMPLETE') from None` before another request or
writer. Rejected/unconfirmed ERROR is not delivery evidence. Local recording
receipts test the real pure envelope/redaction. The code-review regression also
uses a real disposable F-LOG queue/writer with DEBUG/INFO/WARNING/ERROR console
levels, confirmed flush and persisted records: it checks filtering, duplicates,
canaries and cleanup. This narrow local sink evidence does not prove common
runtime/production delivery or complete end-to-end logging integration.

## Acceptance contribution and evidence limits

| Issue AC | A2-01 contribution / regression | Remaining owner/package |
| --- | --- | --- |
| #20 MIME, required columns, numeric/date types, instrument identity | Deferred; legacy parse outputs retained | A2-02 and source identity evidence |
| #20 Valid empty distinct from missing block/HTML/invalid schema | Unvalidated empty return is labeled diagnostically only | A2-02 |
| #20 Failed middle page cannot publish partial history | Raised second-window/ticker failure stops local callers/writers | Actual pagination A3; coherent release D1/D4/R1 |
| #22 Observable failure and nonzero ETL for incomplete mandatory scope | Native final failure, caller propagation and local writer refusal | Common caller/F-LOG/R1 nonzero and upload exclusion |
| #22 Retryable vs permanent 4xx/schema; Retry-After/jitter/backoff | Immediate TLS/redirect refusal; default legacy backoff retained | Full classifier, Retry-After, jitter, deadline/body/resource cap A2-03 |
| #22 Last correct dataset preserved after retries | No writer and unchanged bytes/hash for the affected local candle dataset | Release/remote preservation D1/D4/R1 |
| #24 HTTPS, certificate verification and no downgrade | Exact HTTPS/verify/no-redirect request kwargs; redirect destinations never called | Separate authorized live evidence |
| #24 TLS failure observable rather than empty success | Same native primary, one request, no sleep/fallback; local diagnostics | F-LOG/R1 end-to-end delivery |
| #24 Scheme and unsafe-fallback regression | Literal transport assertions and TLS/redirect matrix | Maintain exact-head regression |
| #78 Domain ERROR/redaction and safe diagnostics | Console+logger canaries for None/context, receipts and primary precedence | Common caller context, physical delivery, shared summary F-LOG/R1 |

Typed HTTP evidence, common `main.py` context forwarding, physical delivery and
hosted/default domain A coverage remain **NOT_CONNECTED**. Caller/F-LOG handoff
requests context forwarding plus an integration test: ISS failure, confirmed
domain ERROR, FAILED/nonzero and upload not invoked. Infrastructure CI does not
replace explicit A domain evidence. Issues remain open; A2-02/A2-03 are separate
approved slices, not automatic follow-up implementation.

Offline regression uses literal candle CSV, native exception identity, fake
clock/sleep, parser/request doubles, writer spies and previous-file hashes.
Compatibility checks retain successful rows/keys/schema/values/index and
503-then-200 / timeout-then-200 delays. New cases cover TLS, all redirect
variants, exhaustion/final-error precedence, partial-window refusal, argument
validation, console/logger canaries and logger-failure precedence.
Transport doubles must support baseline kwargs; a mock TypeError is not RED.

The initial domain RED was four substantive failures, exit 1, against the
unchanged production pair; no mock-TypeError or guard failure was counted.
The first working GREEN collected all 164 A tests and passed 164, exit 0;
collection also exited 0. Script-to-notebook source-only synchronization exited
0. These are candidate-worktree results, not validation of a clean committed
HEAD. The PR handoff records exact-HEAD A/native-lane results, all-pair notebook
verification, actual CI and retained original/failed-run receipts. Required
final checks remain pending until those receipts exist; this document does not
upgrade the first GREEN into final admission or production readiness.
Inherited overrides and credential variables are cleared without printing
values; plugin autoload is disabled. Owning scratch/evidence stays under `.f3/`.
Guards apply before collection and to children. Guard, receipt and capability
failures remain infrastructure stops, not domain RED.

Compare both pair members before script-to-notebook source-only sync. Check all
ten pairs read-only without executing cells; only the A pair may change. No
dependency installation, guard weakening, live API probe, production run,
dataset refresh or publication belongs to this slice.

Rollback is a separately reviewed revert of only these A2-01 changes, with
dependent caller coordination if needed. Preserve previous files, secrets,
environments, worktrees and evidence. No auto-merge, automatic issue closure or
promotion to the next slice is authorized.
