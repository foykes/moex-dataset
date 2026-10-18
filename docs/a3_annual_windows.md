# A3-02 annual candle windows

`moex(ticker_in, ticker_type, years, interval, *, logging_context=None)` uses
calendar windows and an exact union of successful candle queries. This is a
client policy. Synthetic fixtures do not establish the real ISS inclusivity,
date-only expansion, timestamp selection, membership, availability or finality.

## Calendar requests

Capture the legacy local calendar date `A` once. For positive integer `years=N`,
send exactly N requests, newest first. Each anniversary `B_i` is calculated
directly from A with year `A.year-i`. February 29 becomes February 28 only in a
non-leap target year; a later leap anniversary recovers February 29. All generated
dates use `date.isoformat()` and exactly `YYYY-MM-DD`, including years 0001/0999.
Prepare the entire schedule before the first query: year underflow refuses the
call before any GET. No artificial maximum years is introduced.

| Window | `from` | `till` |
| --- | --- | --- |
| 1 | `B_1` | `A` |
| i > 1 | `B_i` | `min(A, B_(i-1) + 1 calendar day)` |

For A=`2024-03-02`, years=3 requests are exactly:

```text
2023-03-02 .. 2024-03-02
2022-03-02 .. 2023-03-03
2021-03-02 .. 2022-03-03
```

The one-day upper-bound extension of the older window is a halo. Under a
synthetic closed/closed whole-date model, **two calendar dates** can occur in
both responses: the anniversary and its following day. No separate halo query
or recursive expansion occurs. An empty window does not stop older queries.
Do not filter, shift or fabricate returned rows to fit nominal bounds. All
empty responses retain the zero-column empty DataFrame.

Legacy zero/negative integer and bool inputs retain their no-query empty
behavior; non-indexable inputs retain native pre-query TypeError. This slice
adds no A4 input validation/coercion policy.

## Exact annual union

The natural key is request-bound:

```text
(ISS_CANDLES_BOARDLESS, stock, selected_market,
 frozen_request_security_token, frozen_request_interval_token, raw_begin)
```

Request bounds, end and pandas index are not key fields. Board is absent from
this endpoint request; global board/source identity remains an A5 question.
Keep market/security/interval and the same caller logging-context object for
each window.

Compare all nine original parsed values before assembly can set the ticker.
For numbers, use exact integer/float ratios and signed zero. A same-key mismatch
in any value, including end or original ticker, refuses the whole annual call
with `A3_ISS_ANNUAL_KEY_CONFLICT`. Retain the first original representative:
W1=X, W2=X, W3=Y must compare versions 1/3 and refuse at W3, without querying W4.
Such three-window repeated membership is an injected source fault, not a claim
of valid ISS membership.

After all queries succeed, every original nonempty frame, including confirmed
repeats, enters the unchanged `_a3_assemble` once. Homogeneous numeric dtypes
remain; mixed numeric promotion is accepted only when every scalar survives
exactly. Unsafe int64 `9007199254740993` plus a float frame refuses with
`A3_ISS_NUMERIC_ASSEMBLY_LOSS`. Removing repeats first must not hide precision
loss or discard a contributing float dtype.

Only then remove confirmed equal annual repeats, stable-sort ascending by begin
and assign a fresh RangeIndex. Inputs remain unchanged. Strict duplicate/order
refusal inside a single paginated `moex_query` remains unchanged.

## Local diagnostic evidence

Annual conflict evidence is `primary.a3_annual_evidence`, profile
`A3_ANNUAL_TYPED_ROWS_V1`: at most two conflicting rows, 64 KiB of serialized
typed source cells plus 16 KiB of metadata. Eight source cells retain parsed
scalar type/dtype, integer decimal or float hex/exact ratio/signed zero, and
original timestamp strings. Ticker/request/correlation identifiers use bounded
privacy projections or digests.

Metadata records window ordinal, requested bounds, original row position/index,
dtypes and request identity/correlation. This is **typed parsed evidence**; it
does not claim raw CSV lexemes, body SHA, codec, page or HTTP attempt provenance.
Successful `moex_query` calls clear their private raw captures, so annual
evidence cannot claim that raw provenance. Existing native raw `a3_evidence`
remains untouched.

Evidence is a bounded local exception attribute, not a journal, production
writer or public bundle. Evidence/cap/attachment/exception-list failures cannot
replace an existing primary object/type/args; incomplete evidence is noted when
possible. Preliminary capture failure before any primary is itself terminal.
No terminal annual failure permits a later window, replay, hidden success or a
returned prefix. Existing transport retries inside each query remain unchanged.

## Caller and resource boundaries

`full_reload` changes only its annual cutoff to a calendar anniversary and its
generated current-date token. Keep years=0 all-history sentinel, listing-date
maximum, stopped-date precedence, direct query routing and writers.

`data_update` changes only the new-ticker cutoff to one calendar year and its
generated current-date token, including for a 30-year dataset. Stored-end
incremental requests, the separate legacy 30-day clock/filter, ticker order,
full-row deduplication and writers remain unchanged. Neither caller is routed
through annual chunks.

Calendar corrections can change requested CSV/XLSX row membership near leap
spans. Annual result ordering/index/duplicate multiplicity intentionally change.
Raw OHLC, timestamp values, nine public columns/order, dtype policy, filenames
and RSI14 formula remain unchanged. No historical dataset rewrite or publication
is part of this slice.

Each `moex_query` retains A2/A3-01 transport/parser/retry/pagination, 1,000-page
and 64 MiB returned-read-byte caps, including failed-attempt and terminal reads,
the 180-second cooperative budget and full 3-second success sleep. A halo can
require extra physical pages and exhaust
these existing limits; there is no universal extra-row/GET bound. A fixture
pins actual offsets/retries/sleeps, including late exhaustion. N annual calls
do not establish a global resource budget or make multi-year minute acquisition
complete. A3-03 owns bounded time/resource subdivision and production hookup.

Acquisition-fault caller fixtures assert no new writer and retained previous
bytes/hashes **for the affected dataset**. They establish no transaction across
datasets, formats or remote targets, nor writer-stage rollback. D/R1 remains
`NOT_CONNECTED`.

## Offline acceptance and open work

Independent literals in `tests/fixtures/a3/annual_windows.json` and
`tests/A/test_annual_windows.py` cover the following. Expected dates, keys and
values are not calculated with production helpers.

| Regression | Independent oracle |
| --- | --- |
| R1/R2 | N=1/2/3/4/10/30, one annual clock, four intervals, low-year/century/leap boundaries, full-schedule underflow, legacy argument profile |
| R3 | Four synthetic endpoint models; six source rows with two repeated dates become four keys; literal extra-page/retry/terminal effects |
| R4/R5 | All-nine-cell conflicts, late third-window conflict/no fourth query, exact-equal mixed-dtype triple, unsafe numeric promotion, signed zero, immutable inputs |
| R6/R8 | First/middle/last source faults, None/context identity, original primary, preliminary/secondary evidence failures, bounded private metadata |
| R7 | Real mocked CSV pagination/retry/terminal and resource exhaustion; existing from=till and A3-01 findings A/B regressions remain |
| R9 | Two narrow caller cutoffs, zero-year sentinel, listing/stopped precedence, one-year bootstrap, unchanged 30-day behavior and affected previous preservation |

Run through the existing guarded A entrypoint and F1/F2/F-LOG/F3 lanes, then
check all ten notebook pairs on the clean exact local HEAD. Native reports,
inventories, process exits and source/environment fingerprints are evidence;
this document is not a test receipt. Linux syntax is not Linux A runtime
verification. Existing CRLF and F2 warning observations must not be declared
fixed without new evidence.

| Issue | Scoped contribution | Still open |
| --- | --- | --- |
| #21 | Exact N client calendar windows, portable dates, explicit halo, request-bound sort/exact union, deterministic pytest | Real source inclusivity/date expansion/membership/finality; source-semantic AC2 remains NOT_VERIFIED |
| #6 | A3-01 pagination plus A3-02 calendar join/refusal policies | A3-03 resource/caller hookup; A5 identity/availability and separately authorized live coverage/last completed candle; later A8/R1 propagation |
| #47 | Offline annual/caller regressions | Global integration, source coverage, indicators and release/remote AC |
| #78 | Preserve closed F-LOG and per-query diagnostics | Global F/D/R1 delivery/FAILED/nonzero/stop/bundle/staging; per-window return is not whole-horizon coverage |
| #76 | A3-01 scoped complete; A3-02 scoped implementation | A3-03/A4/A5 and other downstream slices remain separate |

The separate full `90_source/MOEX_DATASET_FIX_PLAN_v2_1.md` copy was not located;
full reading remains `NOT_VERIFIED`. The reviewed newer owner decisions remain
effective, including B2 CONTINUE across quarantine; no B2 computation is changed.
Issues remain open. Live MOEX, full reload, cron, Google/Excel/FTP/SFTP publication
policy, production and merge require their separate authority.

Before merge, rollback is a reviewed feature revert of the paired/scoped change,
with evidence and worktrees retained. No reset, force push, automatic main revert
or publication rollback is authorized by this slice.
