# dohod.ru acquisition, HTML compatibility and URL admission

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

Each outer attempt owns a fresh `requests.Session` for its entire approved
redirect chain. Preparation uses `Session.prepare_request(Request(...))` and
`merge_environment_settings`; sending uses the public `HTTPAdapter.send` with
`stream=True, timeout=(t, t)`, where `t=min(10, positive remaining operation
budget)` is recomputed immediately before each send. Prepared URL equality is
checked before sending. The default adapter disables urllib3 redirects and
preloading. `Session.get/send` is deliberately avoided: even `allow_redirects=False`
can consume a 3xx body while Requests prepares `_next`. See C3-02 below.

Status is checked before consuming the terminal body. Final 2xx means transport
acceptance only. Final 4xx/5xx preserves Requests' HTTPError; unsupported terminal
1xx/3xx and missing Location use a fixed-message HTTPError. TLS/proxy/CA/netrc,
Session default headers and the explicit historical User-Agent remain Requests
policy. Cookie extraction uses its real cookie jar/domain/path/expiry rules
within an attempt; cookies and Sessions do not persist across attempts or pages.
No adapter retry policy is added. Response history/elapsed/_next and external
hooks are not recreated; private Responses are never returned to callers.

`.content` supplies the cached body bytes to the C3 HTML adapter described below.
Discovery consumes the same cached body and uses Requests' `.text` encoding
behavior after closing.
Every obtained response and owned Session is closed, including falsey HTTP-error responses and
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
its skip path. C3-02 replaces prefix concatenation/set iteration with approved
canonical URLs in first DOM order. Source-row duplicates remain unchanged.

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

C2-02B RED on unchanged production pair: 76 failed / 3 passed. New logging/helper API
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

C2-02B's literal parser-output doubles do not prove genuine HTML compatibility.
C3-01 below addresses #31 with a byte-reader adapter and synthetic real-parser
regressions; today's live dohod HTML remains unverified. C3-02 below implements
the selected #32 consumer URL policy; live URL grammar remains unverified.
#33 table semantics/completeness remain separate. C2-03 quality and C2-04/D
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
The original C2-02B rollback is a reviewed five-path revert; the C3-01 rollback
is described below. Neither can undo historical partial output or provide
D-owned data recovery.

## C3-01 HTML/pandas compatibility

C3-01 starts from `2b222d16782f986ecbaf4e3efb04a81eefa1a0ba`, after a new
explicit owner/coordinator transfer. Its exact six paths are the dohod Python/
notebook pair, the two existing C2 pytest files, this document and
`tests/shared/c3/test_dohod_html.py`. Shared runner, guard, logger, CI and
coordination files are not part of this transfer.

The private adapter owns a fresh `BytesIO` in a context manager and passes a
generic reader exposing only bound `read`, `seek`, `seekable` and `tell` to
`pd.read_html(..., encoding=None)`. The reader has no mode, encoding, path
protocol or IOBase inheritance. Pandas' default lxml-to-bs4/html5lib fallback
remains enabled. Every backend receives unchanged cached bytes; the adapter
selects no charset, decodes no text and adds no HTTP request or parser retry.
The owned buffer closes on success or failure, and pandas can rewind it before
fallback. Discovery's existing Requests text policy is unchanged.

This boundary is deliberate for the approved Windows CPython 3.14.8 profile:
pandas 2.3.3, lxml 6.0.2, beautifulsoup4 4.14.2 and html5lib 1.1. Direct
`BytesIO` with `encoding=None` reaches pandas' UTF-8/strict text wrapper on the
second backend; the generic reader instead preserves the byte input and each
backend's own document/meta/BOM inference and recovery. See versioned
[pandas HTML parsing](https://github.com/pandas-dev/pandas/blob/v2.3.3/pandas/io/html.py),
[pandas IO handling](https://github.com/pandas-dev/pandas/blob/v2.3.3/pandas/io/common.py)
and [lxml file-like parsing](https://github.com/lxml/lxml/blob/lxml-6.0.2/src/lxml/parser.pxi).
Other dependency versions require separate verification.
Preserving `encoding=None` and the default flavor alone does not prove parity
with historical raw-byte decoding; the byte handoff and real fallback must be
verified. Changing the IO object can change upstream parser exception messages
or their input representation. C3 introduces no new diagnostic category/code or
transport-error reclassification; existing fixed C2 diagnostics/redaction stay.

C3 uses synthetic overview/detail HTML with hand-written literal rows, columns,
dtypes, indices, names and duplicates. Real UTF-8 and declared windows-1251
Cyrillic cases cover normal lxml parsing and a controlled first-backend failure
after prefix/full consumption; the second backend and DataFrames remain real.
The windows-1251 fixture diagnoses fallback compatibility; it is not a confirmed
live dohod charset, and these cases provide no guarantee for every charset.
Malformed/no-table inputs, native encoding recovery, path-shaped HTTP bodies,
closed resources, external budget checks and redacted logging are exercised.
Repeated calls retain returned A frames and original global placeholder objects,
advance the same fake clock, use fresh response/log contexts and require only B
rows after failure/skip. Repeated `main` checks preserve both nine-writer success
groups and literal later-six previous bytes/hashes after a second detail fails.
The existing C2 doubles adapt only their reader input; their literal frames,
transport-policy assertions, writer order/kwargs and exception identities stay
unchanged.

Overview parser errors still propagate before writer admission. Detail parsing
retains its historical bare `except` and skip behavior; 2/3-table transforms and
unrecognized counts are unchanged. Stream compatibility does not establish page
completeness, atomic publication, URL safety, strict deadlines or a live encoding
contract. #32, #33, C2-03 quality, D publication and D/F diagnostics remain separate.
Final exact-HEAD regression, notebook, F1/F2/F-LOG/F3 and Linux syntax results
belong to the handoff/PR; skipped checks are NOT VERIFIED. No live HTTP, real
sleep, production writers, credentials, cron, Google or FTP is used for C3 tests.

C3 rollback is a reviewed revert of these six paths, including both notebook
pair members and the C2 boundary adaptations. It does not restore an already
published release or repair historical partial output.

## C3-02 URL and redirect policy

C3-02 PLAN R2 starts from `945c424ecbf7bdd4459de50711929e21e6a3aeb4`
(tree `8259a6c46fc691325d81a9443b8c15137e768c9c`). The owner/coordinator explicitly
released C3-01's reservation to C3-02, transferred the six existing pair/test/doc
paths and admitted `tests/shared/c3/test_dohod_urls.py`. The independent local
ignored HANDOFF_GRANTED records that direct grant and the ownership snapshot.
A retained inactive worktree is not itself a competing writer.

The consumer admission profile accepts HTTPS and exact ASCII `www.dohod.ru`
(DNS case insensitive), no userinfo, and absent or literal `:443` only. It strips
fragment/default port and preserves path case. Overview has two distinct routes:
`/ik/analytics/dividend` (O) and `/ik/analytics/dividend/` (S). Detail has exactly
one nonempty ASCII-unreserved slug below that subtree, without trailing slash.
After normalization, a slug containing `.pdf` in any case is rejected.
No host aliases, query parameters, alternate ports, Unicode hosts/paths, nested
slugs or trailing-slash detail aliases are inferred from synthetic fixtures.

Raw whitespace/C0/DEL/backslash/non-ASCII, malformed percent escapes and raw or
decoded dot segments are rejected before URL joining. Path percent escapes are
decoded once only for ASCII unreserved characters; encoded reserved/control/percent
bytes are rejected. A nonempty query is rejected; an empty delimiter is removed.
Relative, root-relative, absolute and scheme-relative hrefs resolve against the
approved final overview URL using RFC joining, then undergo the full admission
check. HTML `<base>` is ignored. Empty/local-fragment and ordinary same-host
navigation outside the detail subtree create no detail operation.

Private `_request_page(..., page_kind=..., text=False)` returns
`(body, approved_final_url)` only after a successful terminal 2xx body, Response/
Session cleanup and existing decode/diagnostic/budget checks. The URL is the
controller's approved PreparedRequest URL, never untrusted `response.url` or a
value recovered from mutable diagnostic state. `_main_frame` uses only body;
`_page_frames` retains the discovery result URL locally while detail operations
replace diagnostic state. No global base/cache/seen set is added. Public function
signatures and DataFrame/tuple/None returns stay unchanged.

Thus after O -> S, href `aaa` resolves to detail A. Without the slash redirect,
that href resolves outside the detail subtree and is ignored; `dividend/aaa`
resolves to A from O. The controller never adds a slash to invent equivalence.
First DOM occurrence of each canonical discovered detail URL wins. Equivalent
hrefs share one logical operation; retries may repeat its GET. Separate overview
acquisitions in `main` and new public invocations remain separate requests.
Ordering, admitted URL counts and page_url values can intentionally change;
columns, dtypes, indices, source values/duplicates and writer kwargs do not.
Ticker/provenance derives from the canonical discovered detail URL.

Redirect statuses are 301/302/303/307/308. Raw
`response.raw.headers.getlist('Location')` must yield exactly one nonempty string.
More than one field is rejected even if identical or differently cased; merged
`response.headers` is not an authority and there is no comma splitting. Missing
raw headers/getlist or an accessor exception fails closed without a merged-header
fallback or network retry. Invalid targets never reach a new adapter send.

For every hop, resolve/admit the target, require the original page role, preserve
exact canonical detail identity, and check visited URLs/30-transition ceiling
before closing the unread redirect Response and emitting the accepted-hop INFO.
Only then prepare/revalidate/send the next request. O <-> S is allowed once;
O -> S -> O fails before the final O. Overview -> detail and detail -> overview
fail before sending. Detail A -> B (including case-only slug changes) fails with
`REDIRECT_DETAIL_IDENTITY_CHANGED`; uppercase ticker equality proves no alias.
An equivalent detail target becomes the already visited A and fails as a loop.
With this finite profile, 30 valid transitions are unreachable: the ceiling is
defensive, not evidence of a supported long redirect chain.

URL/redirect/cookie/preparation/cleanup/diagnostic failures are terminal acquisition
failures outside the legacy detail parser bare except. Retryable OPEN/READ failure
at any hop restarts the entire chain from its original URL with fresh Session,
visited set and the same 35-second operation/600-second source deadlines. For
O -> S -> 503 exhaustion, requests are `[O,S,O,S,O,S]` with waits `[1,2]`; the last
original network exception and args survive. An attempt-local identity ledger
prevents re-closing an earlier redirect also attached to a later exception.
Session cleanup never masks a primary failure; CLOSE-only errors never retry.

Discovery emits at most one `dohod_url_rejected` WARNING per distinct fixed reason
per invocation. An accepted overview hop emits `dohod_redirect_followed` after
close, before another send. New rejection messages/fields/exception chains do not
include raw href, Location or header/body exception canaries. Logging refusal or
budget expiry stops further sends/waits. Existing received/failed envelopes,
redaction, barriers and writer-group admissions remain in place.

The new offline suite starts with public behavioral RED on the unchanged pair,
then checks real Requests preparation/cookie/Response/raw-header semantics,
forbidden sends, effective O/S bases, repeated same-import calls, literal real-HTML
frames, discovery vs retry counts, cleanup, bounded warnings and late writer/hash
boundaries. Existing C2 transport/compatibility assertions and C3-01 real
UTF-8/windows-1251 normal/fallback parsers remain required. No HTTP socket, real
sleep, production writer or credential is used. Final exact-HEAD evidence requires
all four C2/C3 files, F2/F-LOG/F3, all ten notebook pairs, hosted F1 and Linux syntax;
working-tree GREEN is not a substitute for those clean-commit gates.

The live source grammar remains UNKNOWN outside this explicit consumer profile.
No cross-slug identity, semantic table selection, completeness, required-page,
partial-output, previous-snapshot or publication guarantee is added. A late URL
failure can still leave the three early outputs updated and six later targets
untouched. #33/C3-03 and D integration remain separate. Rollback is a reviewed
seven-path revert with the notebook pair; it does not restore published data.
No main/logger/runner/guard/CI/coordination/dependency changes, issue closure,
auto-merge, production or next-slice activation are authorized by C3-02.
