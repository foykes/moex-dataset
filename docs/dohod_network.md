# dohod.ru cooperative acquisition (C2-02B)

This transport-only slice starts from `3b6e78f11f7681cdcd9c813fff4236a8c63ab677`.
The owner accepted PLAN R2 and granted condition H exclusive ownership of
`dohodru_data.py`, `dohodru_data.ipynb`,
`tests/shared/c2/test_dohod_network.py`,
`tests/shared/c2/test_dohod_compatibility.py`, and this document.
The historical F/C reservation is released for these paths only. The local
ignored handoff retains the approval, competing-writer snapshot, actual base,
final HEAD/tree, guard reports and hosted CI results. A snapshot is not a lock;
recheck ownership and actual main differences before a later mutation.

## Public calls and transport

`get_main(url)` returns a DataFrame and conditionally writes overview XLSX.
`get_page_info(url)` returns a tuple of three DataFrames and writes nothing.
`main(url)` returns None and preserves the nine historical writer calls.
Each also accepts optional keyword-only `logging_context=None`. Direct calls
do not bootstrap logging. The current parent `main.py` does not forward this
context to dohod; its existing stage failure propagates the original exception
and records UNKNOWN. Parent/child diagnostics integration remains D/F work.

Each direct entrypoint starts one run-local monotonic deadline at +600 seconds.
Private acquisition helpers share that deadline in `main`; it is never paused,
reset or extended. Each overview, discovery or detail operation starts a fresh
35-second deadline, bounded by the source deadline, before its request/events.
The operation includes retries, final-body consumption, closing, cached text
decoding, the historical parser/transforms, progress diagnostics and barriers.

Every outer attempt uses `requests.get(url, headers=headers, stream=True,
timeout=(t, t))`, where `t=min(10, positive remaining operation budget)`.
Status is checked before consuming the final response body. Final 2xx means
transport acceptance only. Final 4xx/5xx preserves Requests' HTTPError;
terminal 1xx/3xx raises a fixed-message HTTPError. Requests' existing TLS,
proxy, authentication and redirect defaults and the historical User-Agent
are unchanged. There is no persistent Session or adapter retry policy.

`.content` supplies the historical raw bytes to pandas. Discovery consumes the
same cached body and uses Requests' `.text` encoding behavior after closing.
Every obtained response is closed, including falsey HTTP-error responses and
exception-attached responses; identity deduplication prevents double close.
Partial bytes from a failed read never reach the parser. A secondary cleanup
failure cannot replace the primary OPEN/READ error. CLOSE-only failures are IO
and do not repeat the HTTP request. DECODE/parser/assembly failures do not retry.

## Retry and error matrix

| Current typed failure | Category | Outer retry |
| --- | --- | --- |
| HTTP 401 / 403 | ACCESS | Never; no access bypass |
| HTTP 429 | QUOTA_429 | Up to three total attempts |
| HTTP 500 / 502 / 503 / 504 | SOURCE | Up to three total attempts |
| Other terminal non-2xx | SOURCE | Never |
| Requests/socket/urllib3 actual timeout | TIMEOUT | Up to three total attempts |
| Typed reset/abort, including Windows 10054/10053 | SOURCE | Up to three total attempts |
| Typed DNS EAI_AGAIN | SOURCE | Up to three total attempts |
| Other DNS, refusal, unknown connection | SOURCE | Never |
| TLS / ProxyError | SOURCE | Never, including nested transient causes |
| Current decoding/header/config error | SOURCE | Never |
| CLOSE-only or export failure | IO | Never |
| Cooperative deadline exhaustion | TIMEOUT | No further request/admission |
| Required diagnostic receipt/barrier failure | IO | No further admission |

Policy uses typed exceptions, not strings. A cycle-safe walk of permitted
exception-valued Requests/urllib3 arguments, explicit causes, MaxRetryError.reason,
NameResolutionError._reason and ProxyError.original_error is bounded to depth 8
and 32 nodes. Current typed failures outrank historical causes; unrelated
implicit context cannot authorize a retry. NewConnectionError's inherited
ConnectTimeoutError base does not itself prove TIMEOUT. State is fresh for every
attempt/operation. Attempt exhaustion rethrows the last original exception,
preserving its identity and args.

Backoff is 1/2 seconds, with no wait after attempt three. For the currently
classified retryable HTTP response only, Retry-After accepts ASCII nonnegative
integer seconds or an HTTP date using a separate wall clock. Invalid/non-string,
past or zero values fall back to backoff. The chosen delay is the greater of
backoff and valid server delay. A delay above 10 seconds is refused, preserving
the HTTP error, rather than clamped or retried early; huge integers are compared
before integer conversion. Delay must be strictly below remaining budget before
the warning and again after diagnostics. Budget is checked after wait before
another GET. A refused/infeasible retry creates no wait or extra request; an
infeasible retry before diagnostics creates no retry warning. A budget error
uses fixed `DOHOD_BUDGET_EXHAUSTED` and retains the interrupted network cause.

## Acquisition, admission and export

| Entry/boundary | Acquisition checks | Admitted export |
| --- | --- | --- |
| Direct `get_main` | Overview fetch/parser/barrier and one source admission | Conditional overview XLSX; no check after writer |
| `main` early group | Same preparation and one source admission | Conditional overview XLSX, main-page XLSX, main-page CSV |
| Transition to discovery | Check the original source deadline after early writes | No rollback of early group |
| `get_page_info` | Discovery and each detail operation, assembly/barriers and final source check | None |
| `main` final group | All frames prepared, barriers confirmed and one source admission | All six remaining XLSX/CSV calls, in original order |

There are no acquisition checks between or after writers in an admitted group.
An XLSX writer crossing the deadline therefore cannot cause the group's CSV or
next pair to be skipped solely by acquisition budget. Once all pages/frames
have been collected and the final group admitted, acquisition budget introduces
no new partial export. Original writer failures still stop subsequent calls,
preserve exception identity/args and are classified separately as IO, including
a writer TimeoutError. Export has no new timeout or writer framework.

Early write time counts against the original source deadline. If early export
takes 36 seconds, discovery gets a new 35-second operation while source budget
remains positive. If it takes over 600 seconds, the whole admitted early group
finishes, then discovery fails before GET. Late detail network failure likewise
leaves the three early outputs updated and the later six untouched. This is
historical partial output, not atomic multi-file publication or whole-release
previous preservation.

| Order | Historical target under `datasets/dividends/dohodru/` |
| --- | --- |
| 1 (conditional) | obzor_vsex_divs_tickerov.xlsx |
| 2 / 3 | main_page.xlsx / main_page.csv |
| 4 / 5 | rate_for_each_ticker.xlsx / rate_for_each_ticker.csv |
| 6 / 7 | sum_each_year_divs.xlsx / sum_each_year_divs.csv |
| 8 / 9 | all_payments.xlsx / all_payments.csv |

All original kwargs remain omitted, including pandas' default index=True.
Empty overview omits only call 1; the other eight retain their behavior.
The parser retains `df_list[-1]`, historical columns/drop/reset/concat behavior,
2/3-table branches, duplicates, indices and existing bare detail `read_html`
exception handling. Budget/diagnostics stay outside that bare except, including
its skip path. Filtering/prefix concatenation and `list(set(data))` remain
unchanged; order across distinct detail URLs remains set-dependent.

## Diagnostics and offline evidence

Existing F0/F-LOG full error envelopes are used with stage `dohodru_data`,
attempt limit 3, fixed reason tokens and OPEN/READ/CLOSE/DECODE phase markers.
The phase is cleared before parser/barrier work. `dohod_request_received` says
only that the body was received, never that data is complete or published.
`dohod_request_retry`, `dohod_acquisition_failed` and `dohod_export_failed`
record distinct outcomes. Error codes are DOHOD_SOURCE_REQUEST_FAILED,
DOHOD_BUDGET_EXHAUSTED, LOGGING_INCOMPLETE and DOHOD_EXPORT_FAILED.
Ordinary INFO/WARNING requires accepted delivery and healthy logging;
ERROR requires confirmation. Acquisition barriers require confirmed flush
and healthy logging before admission. Error messages contain fixed tokens,
not raw exception args, server reasons/bodies/headers or custom tracebacks.
Endpoint sanitization uses the existing logger. Secondary diagnostic failure
preserves the primary error; retry-warning refusal stops retries.

The two approved pytest files use actual offline Requests Response objects,
typed urllib3 errors and real `.content` wrapper conversions, literal parser
output doubles, actual BeautifulSoup discovery, fake monotonic/wall clocks,
fake waits and writer spies/disposable byte files. They do not run a production
writer, live HTTP or real sleep. Literal PASS criteria include:

- Healthy positional calls retain return types, schemas, values, duplicates,
  ordering, indices, full paths, kwargs and all nine calls (eight for empty overview).
- Permanent failures make one request; transient exhaustion makes three with
  waits exactly [1, 2]; status errors never read body; resources close once.
- Retry-After refusal/infeasibility, expiry after read/close/parser/barrier/wait,
  failed diagnostics and partial body prevent further acquisition/admission.
- Source deadline remains fixed across multiple individually short operations;
  successful final admission survives deadline crossing during any final XLSX.
- Direct overview and early-group writers finish after crossing their deadline;
  discovery respects the original source deadline; writer errors remain IO.
- A failure on the actual second/last detail records three early and zero later
  writes; later-six disposable previous bytes and SHA-256 hashes remain identical.
- B messages do not leak registered or unregistered synthetic body/exception
  markers. A stubbed AST-extracted parent function propagates the dohod failure,
  records stage FAILED and cannot reach upload/final success.

RED on unchanged production pair: 76 failed / 3 passed. New logging/helper API
absence is distinguished from positional timeout/retry/cleanup/parser-budget
failures; it is not the sole RED oracle. Final clean-HEAD counts, source identity,
guard reconciliation, notebook parity and applicable hosted Windows/Linux results
belong to the exact-HEAD handoff/PR. Any skipped hosted check is NOT VERIFIED.

Required local commands use the preverified immutable dev runtime in a clean,
credential-free child from the owning worktree:

```text
python -I -B -X utf8 -m pytest -c pyproject.toml -q tests/shared/c2/test_dohod_network.py tests/shared/c2/test_dohod_compatibility.py
python -I -B -X utf8 tools/offline_tests.py --lane f3
python -I -B -X utf8 tools/offline_tests.py --lane f2
python -I -B -X utf8 tools/offline_tests.py --lane flog
python -I -B -X utf8 tools/offline_tests.py --notebook-check --ref <full-clean-HEAD>
```

Local F1 is not required by this approval; the foreign immutable runtime cannot
prove owning-worktree F1 placement. Hosted F1 is applicable. No dependencies,
permissions or CI activation are added by B.

## Limits and rollback

35/600 seconds are cooperative acquisition budgets, not strict wall-clock
termination. DNS/multiple addresses/redirects/slow-drip reads, parser work,
close and logging may overrun before a checkpoint; body size/memory and actual
process supervision remain unbounded here. No live/VPS/source-quality or real
production-writer evidence is claimed.

C3 #31 raw-bytes pandas compatibility, #32 canonical/allowed URLs and #33
table semantics/completeness remain separate. Literal parser-output doubles
do not prove today's real HTML works with pandas. C2-03 quality and C2-04/D
publication/previous integrity, D2/F child supervision and parent diagnostic
hookup remain open integration AC. HTTP 200, an empty/skipped detail or a
successful writer does not establish complete data or publication success.
Legacy parser URL prints and standalone uncaught traceback privacy remain
residuals outside the new B diagnostics. No main/logger/CI/guard/coordination,
dividend pair, D helper, OHLC/history/RSI or Excel/Google policy is changed.

Hard prerequisites are the accepted R2/implementation approval, exact ownership,
compatible actual base/shared interfaces and passing offline/pair/hosted gates.
Compatible foreign-zone main movement may be safely merged into a clean feature
branch; revalidate and review the final integrated HEAD. No automatic rebase,
reset, force-push or PR merge is permitted. Deliver one draft PR for independent
CODE_REVIEW, with NO_AUTO_MERGE=true and no issue closure or next-slice activation.
Rollback is a reviewed revert of the five-path commit/pair; it cannot undo
historical partial output or provide D-owned data recovery.
