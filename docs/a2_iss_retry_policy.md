# A2-03 finite ISS candle retry and resource policy

This slice implements accepted A2-03 R2 for
[#22](https://github.com/foykes/moex-dataset/issues/22), retaining the scoped
A2-01/A2-02 contributions to [#20](https://github.com/foykes/moex-dataset/issues/20),
[#24](https://github.com/foykes/moex-dataset/issues/24) and
[#78](https://github.com/foykes/moex-dataset/issues/78).
Pinned execution base: `dab1ad3b6328ccdce1da7d636c36720f755d227a`.
Exact source HEAD, runtime results and independent main delta belong in the
reviewer handoff. Limits below are accepted **consumer policy**, not measured
MOEX service limits or a claim that the provider emits only accepted forms.

## Ownership and scope

The exact nine-path allowlist is:

```text
data_gathering.py
data_gathering.ipynb
tests/A/test_iss_retry_policy.py
tests/A/test_iss_transport.py
tests/A/test_iss_validation.py
tests/shared/test_preparation.py
docs/a2_iss_retry_policy.md
docs/a2_iss_transport.md
docs/a2_iss_validation.md
```

The script/notebook pair is one owning zone. The new
[A2-03 owner direct grant](https://github.com/foykes/moex-dataset/issues/47#issuecomment-6199271244)
releases only `test_candle_retry_503_then_200_has_literal_rows_and_keys` in the
shared file for stream/timeout/raw-header/close/request-local-header/clock
double adaptation. Its nodeid, 503-to-200 sequence, A/B User-Agent rotation,
HTTPS/verify/no-redirect assertions, default sleeps `[3, 3]`, literal
rows/columns/values/dtypes/keys/index, fixture/previous hashes and forbidden
writer assertions remain. No shared imports/helpers or other function is
transferred. Earlier grants are not reused.

Pinned development base, pinned integration snapshot and non-blocking
independent main drift remain separate controls. Independent changes do not
trigger automatic rebase/reset; real ownership, interface, dependency, guard
or functional conflicts require analysis. No logger/F0/metadata, guard/runner,
workflow, dependency, release or other domain implementation belongs here.

## Finite policy and compatibility

| Parameter | Accepted policy |
| --- | --- |
| Total attempts, `max_retries` | Exact integer 1..5, default 5; bool rejected |
| Retry base, `retry_sleep_start` | Finite exact int/float 0..30 seconds, default 3; bool rejected |
| Keyword-only `retry_jitter` | Finite exact int/float 0..1 seconds, default 0; bool rejected |
| Local selected backoff | `min(30, base * 2**(i-1) + U[0,jitter])` after failed attempt i |
| Query budget | 180 seconds, cooperative monotonic |
| Connect/read timeout at admission | `(min(5,R), min(30,R))`, with positive remaining budget R |
| Maximum selected retry wait | 30 seconds; no server-delay clamp |
| Accepted body | At most 8 MiB identity entity bytes |
| Individual raw read | At most 64 KiB, with bounded overflow probe |
| Retry-After input | At most 128 ASCII characters |

Invalid parameters fail before network with
`ValueError('A2_RETRY_ARGUMENTS')`; no silent clamp. Default jitter is zero,
retaining default retry sleeps `3, 6, 12, 24` and mandatory success sleep 3.
There is no sleep after the last failed attempt.

HTTPS, certificate verification, no redirects, market routing, query/date/
interval inputs, positional parameters and four existing A caller hookups
remain. `retry_jitter` adds only an optional keyword. Successful pandas
values/dtypes, public nine columns/order, timestamps/index and structural-empty
zero-column return retain the accepted `A2_CANDLES_CSV_V1` profile. No sorting,
deduplication, economic repair or partial frame acceptance is added. Callers
still stop before affected local writers after source failure.

## SOURCE / ERROR_POLICY_TABLE

| Current failure | Action | Safe code / category |
| --- | --- | --- |
| Invalid policy argument | Before network, terminal | `A2_RETRY_ARGUMENTS` / CONFIG |
| Native TLS failure | Immediate, no rotation/wait/retry | `A2_ISS_TLS_FAILED` / SOURCE |
| Native redirect or any 300-399 | Immediate; do not follow Location or read body | `A2_ISS_REDIRECT_REJECTED` / SOURCE |
| HTTP 408/429/500/502/503/504 | Retry candidate subject to count/header/deadline admission | `A2_ISS_HTTP_STATUS` / QUOTA_429 for 429, otherwise SOURCE |
| Any other non-200 | Terminal, no speculative retry | `A2_ISS_HTTP_STATUS` / SOURCE |
| Typed native connect/read timeout | Retry candidate | `A2_ISS_TIMEOUT` / TIMEOUT |
| Typed transient reset/abort/temporary DNS | Retry candidate | `A2_ISS_REQUEST_ERROR` / SOURCE |
| Permanent/unknown connection, proxy/config failure | Terminal | `A2_ISS_REQUEST_ERROR` / SOURCE or CONFIG under closed typed classification |
| MIME/envelope/schema/frame or native CSV refusal | Terminal; next prepared valid response is not consumed | `A2_ISS_RESPONSE_SCHEMA_INVALID` / SOURCE |
| Strict decode failure | Terminal, native UnicodeDecodeError retained | `A2_ISS_RESPONSE_DECODE_ERROR` / SOURCE |
| Existing pandas parsing failure | Terminal, native parser exception retained | `A2_ISS_PARSE_ERROR` / SOURCE |
| Unsupported/ambiguous Content-Encoding | Terminal before body read | `A2_ISS_CONTENT_ENCODING_UNSUPPORTED` / SOURCE |
| Body cap exceeded | Refuse entire response | `A2_ISS_BODY_LIMIT_EXCEEDED` / CAPACITY |
| Cleanup-only failure | Terminal, native exception retained | `A2_ISS_CLOSE_ERROR` / IO |
| Native MemoryError | Terminal, native exception retained | `A2_ISS_MEMORY_ERROR` / MEMORY_ERROR |
| Unknown implementation exception | Terminal, no broad retry | `A2_ISS_REQUEST_ERROR` / UNKNOWN |
| Deadline with no current primary | Native Requests Timeout with fixed args | `A2_ISS_DEADLINE_EXCEEDED` / TIMEOUT |

Existing native primary objects/args are preserved; generated HTTPError uses
the current response, never a stale earlier exception. Typed classification
does not parse exception text. Traversal of current known wrappers is bounded
to depth 8 / 32 nodes; arbitrary historical `__context__` is not retry
evidence. TLS/config precedence overrides nested timeout/transient evidence.
Raw streaming can expose native urllib3 exceptions; their identity is retained.
Retry eligibility is the selected client policy, not a universal source fact.

## Closed Retry-After and Content-Encoding parsing

Read original raw header occurrences through the pinned urllib3 header API.
Flattened `response.headers` and comma splitting cannot establish duplicates.
Apply Retry-After only to the current retry-eligible HTTP response:

| Header classification, in order | Decision |
| --- | --- |
| Proven absent | Local bounded backoff |
| Multiple occurrences, even identical; unavailable occurrence provenance | No retry; retain current HTTPError |
| Single input longer than 128 characters | No retry before numeric/date parsing |
| Complete valid ASCII decimal seconds or HTTP-date | Evaluate delay |
| Remaining comma-bearing input | Ambiguous, no retry |
| Other ordinary-invalid input, including empty/negative/fractional/short non-ASCII | Local backoff plus safe diagnostic |

Accept only complete grammar/calendar/timezone-checked IMF-fixdate, RFC850 and
asctime HTTP-date forms. An internal comma in a single valid HTTP-date is not
a duplicate. `5, 5`, `date, date`, `date, 5` and `Wed, junk` are ambiguous.
RFC850's two-digit year follows the rule that a date appearing more than 50
years in the future is the most recent past year with those two digits.
Use one UTC wall sample for conversion; subsequent scheduling is monotonic.
Bound length/magnitude before decimal conversion.

Zero/past delay uses local backoff. A valid positive delay over 30 seconds
refuses retry without clamp. Otherwise select
`max(local_backoff_with_jitter, server_delay)`. A wait that leaves no positive
remaining query budget is infeasible. Check again after mandatory diagnostics
and after sleep; an emitted RETRY_PENDING is a plan, not proof of another GET.
Refusal retains the current HTTPError/args/code/category, with terminal
`RETRY_NOT_ADMITTED` or `DEADLINE_EXCEEDED`; no new GET/wait is started.
Ordinary-invalid raw values are never copied into diagnostics.

For Content-Encoding, accept only proven absence or exactly one `identity`
token, case-insensitive with surrounding SP/HTAB OWS. Reject present-empty,
duplicate identity, lists, parameters, unknown/conflicting coding, controls,
substring matches and unavailable provenance, all before body read. Thus
` Identity ` is accepted, while `xidentity` and `identity, identity` are not.

## Bounded streaming and response cleanup

Set `Accept-Encoding: identity` only in a request-local copy of the rotating
header; leave global headers unchanged. Request with `stream=True`,
`verify=True`, `allow_redirects=False` and the admitted timeout tuple.
Classify HTTP status before touching body. Never read rejected non-200 bodies
or use eager `.content`, `.text` or `.json` on the streaming response.

Read `response.raw.read(amt=..., decode_content=False)` with a bounded amount.
Accept no more than 8 MiB; a bounded boundary probe distinguishes exact-cap
EOF from overflow. A prefix is never complete response evidence. A checked
Content-Length exceeding the cap may refuse early; absent, malformed or
false-small declarations never disable the actual counter. Decode/validate
only the complete bounded bytes, using the unchanged A2-02 profile.

Close every returned or exception-attached response on success, failure or
interruption, including falsey responses. Do not drain rejected bodies. A
cleanup failure stops further requests. With an existing primary it is
secondary; without one it becomes the native primary. A later secondary
deadline or logger failure does not replace it.

The body cap is not a hard process RSS limit. TLS/native buffers, decoded
text, CSV objects, StringIO and pandas allocations can coexist. Identity
entity bytes are not total wire bytes. A hard process supervisor/RSS cap and
strict wall-clock termination are outside this slice.

## Deadline and primary precedence

Start the monotonic budget at query entry. Check before/after admission
diagnostics, before requests, around bounded reads and decode/CSV/pandas/frame
phases, around retry wait, before/after success sleep, and before the returned
diagnostic. Deadline is expired at `now >= deadline`. Admit success sleep only
with more than 3 seconds remaining; never shorten the legacy 3-second sleep.

An existing current native primary keeps identity/args/code/category. After a
new GET is admitted, an earlier attempt's failure is no longer current. In
the absence of primary, expiry creates
`requests.exceptions.Timeout('A2_ISS_DEADLINE_EXCEEDED')` with exactly that one
arg, code `A2_ISS_DEADLINE_EXCEEDED`, category TIMEOUT. It does not turn an
existing mandatory logging failure into Timeout.

| Deadline checkpoint | Actual GET count / primary | Terminal behavior |
| --- | --- | --- |
| Before started/first GET | 0; new deadline Timeout | ERROR failed / DEADLINE_EXCEEDED; no started, GET, sleep or returned |
| After started or successful admission diagnostics | 0; new deadline Timeout | failed, attempt 0; no GET/sleep/returned |
| After failed attempt, before next GET | n; same current native primary | failed / DEADLINE_EXCEEDED; no new GET/wait/returned |
| After successful admitted 200/read/validation | n; new deadline Timeout | close response, discard frame; no retry/success sleep/returned |
| Before success sleep with R<=3 | n; new deadline Timeout | no sleep/returned |
| During sleep, discovered after full sleep | n; new Timeout unless sleep itself failed | retain completed sleep evidence; no returned/new wait/GET |
| Current primary plus cleanup fault and expiry | n; original primary | cleanup/deadline secondary; no retry/returned |
| Cleanup fault first, then expiry | n; native cleanup exception / IO | REQUEST_FAILED; no retry/returned |
| Timeout first, then cleanup/logger fault | n; same deadline Timeout | secondary bounded signal; no retry/returned |
| Mandatory logging failure with no primary plus expiry | 0 or n; LOGGING_INCOMPLETE RuntimeError | not Timeout; no fake confirmed terminal delivery, retry or returned |
| Mandatory logging failure with primary plus expiry | n; same primary | bounded secondary fallback; no retry/returned |

Fix the current operation's exception before processing a secondary deadline.
Sleep interruption preserves its own native object/args; KeyboardInterrupt and
SystemExit do not acquire a fabricated logging sequence. An already delivered
retry warning remains a planned retry when a later checkpoint refuses it.

The deadline is cooperative: DNS/multi-address connect, blocking reads,
parsers, logger, cleanup and oversleep cannot be preempted here. The timeout
tuple is not a mathematical total timeout. Returned `duration_ms` includes
processing, cleanup and full success sleep, measured before returned-event
delivery; it excludes subsequent logger latency. Do not add a new deadline
refusal after an already emitted returned event.

## Closed diagnostics

Keep existing F-LOG envelope, `stage='data_gathering'`, ordinary `fields={}`,
correlation/null reasons and event names. Actual attempt count increments only
at the `requests.get` call, after admission diagnostics/checks.

| Event | Level / outcome | Retry/error semantics |
| --- | --- | --- |
| `a2_query_started` | INFO / QUERY_STARTED | attempt 0, actual validated limit; no error |
| `a2_query_attempt_started` | DEBUG / REQUEST_ADMISSION_STARTED | number of already called GETs; no fictional attempt |
| `a2_query_retry` | WARNING / RETRY_PENDING | feasible planned retry; current primary, final false, retryable true |
| `a2_query_returned` | INFO / existing QUERY_*_UNVALIDATED | after full required success sequence; actual attempts/counts |
| `a2_query_failed` | ERROR / terminal outcome | current primary, final true, retryable false |

Terminal outcomes are CONFIG_REJECTED, TLS_FAILED, REDIRECT_REJECTED,
HTTP_NONRETRYABLE, RESPONSE_REJECTED, BODY_LIMIT_EXCEEDED, RETRY_NOT_ADMITTED,
RETRY_EXHAUSTED, DEADLINE_EXCEEDED and REQUEST_FAILED. Budget refusal after a
failed attempt retains that primary's safe code/category. QUERY_*_UNVALIDATED
remain diagnostics, not F0 CollectionStatus or metadata values.

With context, F-LOG owns normal console delivery/levels; without context,
fallback is bounded and non-DEBUG. INFO/DEBUG/WARNING require accepted+healthy;
ERROR additionally requires confirmed. Logger failure never triggers HTTP
retry. Preserve existing primary object/args on diagnostic failure and emit
only the fixed bounded secondary fallback/note. Without primary, preserve
`RuntimeError('LOGGING_INCOMPLETE') from None`; no retry or invented delivery.

Actual attempts/limit, elapsed time, existing counts/outcome/error fields are
typed evidence. HTTP status, wait/source, remaining budget, timeout tuple and
body byte facts are bounded human-message/regression evidence only. Typed
HTTP linkage remains **NOT_CONNECTED**; a coordinator-owned contract proposal
is required before adding typed fields. No hidden JSON/dynamic codes/metadata
or raw header/body/URL/exception text is added. No recording fixture proves
production delivery, source identity, wire bytes or complete release.

## Regression and execution gates

Substantive RED precedes implementation on the unchanged approved production
pair. Baseline-compatible doubles prevent missing helpers, mock TypeError or
guard failure from being mistaken for domain RED. RED cases cover permanent
404 and invalid-schema recovery, million-attempt arguments, Retry-After 31,
body cap+1 and successful-200 clock overrun. Initial sandbox infrastructure
failure and all earlier receipts remain evidence, not substituted domain RED.

Fresh regressions additionally cover the full HTTP/typed error matrix, last
primary/args, header occurrences and all HTTP-date forms/boundaries, jitter
min/max/default, cap-1/cap/cap+1 and misleading lengths, eager-body traps,
cleanup/interruption, every deadline row, actual GET counts, None/context
canaries, real F-LOG levels/no duplicates and affected-local-writer refusal.
Preserve all existing literal output/dtype/key/index/previous-hash assertions.
Historical million-count acceptance and invalid-schema-to-valid retry oracles
change explicitly to policy refusal; their original commits/receipts remain.

Mandatory execution is CI-first:

| Gate | Required context |
| --- | --- |
| Substantive RED; fresh full A collection/runtime | Proven compatible guarded local context, fresh exact source evidence |
| F1 | Exact-HEAD Windows hosted CI with its own worktree-local `.f1/python` |
| F2/F-LOG/F3, current shared/C2/C3 inventory | Exact-HEAD native CI; local execution only in proven compatible context |
| All ten notebook pairs | Exact-HEAD source-only check, no executed cells |
| Linux syntax | Exact-HEAD hosted check |

No mandatory local `--lane f1`: an interpreter from a different worktree does
not satisfy owning F1 environment tests. No automatic F1 installs/host
wrappers, guard changes, skips/xfails or reused one-shot approvals/locks.
Fresh A is explicit because foundation CI does not run tests/A. Historical
A381/F1 or aggregate green CI cannot replace actual new-HEAD inventory,
native exits, source/environment preservation and receipt readback. Final
test counts/results/CI links are recorded in the reviewer handoff after they
exist; this document does not declare pending gates PASS.

## AC contribution, residuals and rollback

| AC | Local contribution | Remaining owner/package |
| --- | --- | --- |
| #22 observable failure/nonzero | Native failure and local caller propagation | Common FAILED/nonzero/upload-stop F/R1 |
| #22 retry/permanent/schema, Retry-After/jitter/backoff | Closed finite client policy and exact scheduling/error oracles | Strict process termination separate |
| #22 previous dataset | No affected writer, unchanged local previous bytes/hash | Coherent release/remote D/R1 |
| #20 accepted HTTP200 profile | A2-02 grammar and successful outputs retained | Genuine empty/independent identity A/source evidence |
| #20 failed middle page | No local partial return after raised failure | Pagination/coverage A3; release D/R1 |
| #24 HTTPS/TLS/no downgrade | A2-01 transport assertions retained | Separately authorized live acceptance |
| #78 attempts/reasons/redaction | Closed safe actual-call/admission/resource diagnostics | Typed/physical/bundle/final outcome F/R1 |

No source issue is automatically complete. Main context forwarding, physical
delivery, pagination, source availability/empty scope, independent identity,
coherent previous release and remote preservation remain integration work.
No raw/history/RSI, quarantine, Excel/Google or capacity/previous policy change,
live probe, dataset rebuild/publication, cron or production execution belongs
to this slice.

Rollback needs a separately authorized scoped A2-03 revert, preserving
A2-01/A2-02, foreign commits, datasets, environments, worktrees, locks and
evidence. It restores the old unbounded/permissive retry risk. No automatic
rollback, merge, issue closure or next-slice implementation is authorized.
