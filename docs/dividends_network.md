# C2-02A1: cooperative MOEX dividend network budgets

Reviewed base: `42830d91b4c701b899cbadb5ebdf8829eec6b5cf`.
The owner accepted R2 and exclusively transferred the dividend pair and the
five test/document paths in this PR to C2-02A1. Historical C1-01/C2-01 copies
remain evidence; C1-02 requires a subsequent transfer. Shared coordination,
controller, guard, runner, logging infrastructure and dohod files are outside
this reservation.

## Timing and retries

`main(*, logging_context=None)` starts one monotonic 600-second acquisition
budget before scope preparation. Direct `div_loader(isin, ticker, *,
logging_context=None)` starts its own 600-second budget. The private loader
reuses the collection deadline for every instrument; neither retries nor the
next instrument renew it.

Each search/detail operation starts a 35-second budget before its first domain
event. Its deadline is the earlier of that deadline and the source deadline.
The same operation budget covers requests, read, close, JSON decoding, existing
parser and logging checkpoints. Each `urlopen` receives
`timeout=min(10, positive_remaining_seconds)`.

Only permitted OPEN/READ failures repeat, with at most three total attempts
per request and backoffs of 1 and 2 seconds. Search and detail have separate
retry chains. No wait follows the final attempt. A new request cannot start
after a failed remaining-budget check; a retry delay must leave positive
budget for another attempt. Checks also follow waiting, decoding, parsing and
the response logging barrier, and precede collection exports.

For HTTP 429 and 500/502/503/504, `Retry-After` supports nonnegative integer
seconds and HTTP dates. Dates use a separate wall clock; budgets remain
monotonic. Missing/invalid/past/zero values use normal backoff. The delay is
the larger of backoff and valid server delay. A valid delay above 10 seconds
refuses the retry chain with the original exception, rather than clamping it
and retrying early. A delay that cannot fit the remaining budget produces a
final budget failure. Header parsing failures cannot replace the primary error.

**These are cooperative checks, not strict 35/600-second wall-clock limits.**
Socket timeouts do not bound overall response duration. A1 cannot stop hung
DNS, slow drip, read, scope preparation, close, decoding, parsing or parent
logging. A call that returns late fails at its next checkpoint. There is no
process termination or five-second cleanup guarantee in A1. Writers remain
outside acquisition and do not become an atomic publication transaction.

## Error policy

Classification uses types and symbolic errno/DNS constants. An exception-valued
`URLError.reason`, explicit cause and unsuppressed context are examined with
cycle/depth limits. HTTPError is tested before its URLError/OSError bases.
The nearest typed failure takes precedence over historical causes; a current
timeout does not acquire a historical HTTP Retry-After. String reasons are
never used to guess transient errors.

| Failure | Category | Retry |
| --- | --- | --- |
| HTTP 401/403 | ACCESS | No |
| HTTP 429 | QUOTA_429 | Bounded, respects Retry-After |
| HTTP 500/502/503/504 | SOURCE | Bounded |
| Other HTTP statuses, including 400/404/501/505 | SOURCE | No |
| Direct/wrapped TimeoutError or socket.timeout | TIMEOUT | Bounded |
| Typed/symbolic connection reset or abort | SOURCE | Bounded |
| DNS EAI_AGAIN | SOURCE | Bounded |
| Other DNS failures | SOURCE | No |
| Certificate verification and other SSL errors | SOURCE | No |
| Unknown OSError/string reason | SOURCE | No |
| JSON/Unicode decoding, schema or identity failure | Existing SOURCE/QUALITY code | No |
| Close-only failure | IO | No |
| Logging barrier failure | IO | No |
| Acquisition budget exhaustion | TIMEOUT | No further requests/exports |

Attempt exhaustion re-raises the last original network exception, preserving
its identity and args within the existing process. Actual cooperative budget
exhaustion raises `TimeoutError` with `DIVIDEND_BUDGET_EXHAUSTED`; the preceding
network error remains its cause when exhaustion interrupts retry. HTTPError
bodies and opened responses are closed on failure. Secondary close/diagnostic
failures do not replace the primary error. A close-only failure never repeats
the request. TLS verification and redirect/source trust policy are unchanged;
there is no workaround for access refusal.

Retry warnings use the existing envelope: instrument, safe endpoint, duration,
`retries={attempt, limit}`, fixed reason and nonfinal retryable error. Final
errors are nonretryable/final; the caller retains the exception so existing
stage orchestration can fail and exit nonzero. Ordinary `fields` stay empty.
Existing F-LOG redaction handles endpoints/messages; no raw body, response
headers, cookies, environment or new diagnostics payload is logged. Retry
diagnostic refusal stops retries and preserves the primary failure.

## Preserved contracts

Public signatures and successful `None` returns, exact ISIN selection, parser,
five dividend fields, decoded values, order, duplicate payouts and existing
CSV/XLSX filenames/arguments (`index=False`) are preserved. A response is
appended only after structural validation, confirmed logger barrier and its
budget check. Failed responses do not append partial rows. Previously accepted
global rows remain. Repeat-main isolation and per-run request reuse remain
C1-02; natural event key/lossless event dedup remain C1-03/F0. Date, amount,
currency, precision and provenance quality remain C2-03.
`VALID_EMPTY`, missing-block `KeyError('dividends')` and the collection
`BLOCKED_KEEP_PREVIOUS` empty-release gate remain. Previous-file preservation
tests are fixture evidence, not a production recovery proof.

## Offline evidence and review gates

Tests use literal responses, separately exercise search/detail and retain all
91 C1 and 63 C2 cases. Existing doubles only accept `timeout`; HTTP 500 gains
its exact three detail calls and fake waits `[1, 2]`; HTTP 403 expects ACCESS.
Literal rows, primary exceptions, previous bytes/hashes, accumulator and writer
oracles are not weakened. R1 added 114 transport/budget cases, including
nested conflicting failures and the gap before the next attempt. The
corrective revision adds 14 more cases.
Private clocks/wait are replaced without changing logger clocks. No live
MOEX/dohod requests, real retry sleep, production datasets or inherited
credentials are used; the existing F3 admission is unchanged.

On the unchanged production pair at the reviewed base, the expanded RED
command was:

```text
python -I -B -X utf8 -m pytest -c pyproject.toml -q tests/shared/c1 tests/shared/c2
103 failed, 158 passed; exit 1
```

Seven review regressions subsequently failed before their fixes (exit 1).
The same full narrow command after R1 implementation passed 268 cases (exit 0).
These historical runs describe working-tree evidence. Exact-clean-HEAD F3,
F2, F-LOG, ten notebook-pair checks and hosted CI are required and must be
recorded separately in the PR handoff; earlier PR results do not substitute
for them. No production readiness or strict process deadline is claimed by
these unit results.

## Acceptance contribution and residual owners

| Issue | A1 contribution | Remaining work |
| --- | --- | --- |
| #30 | Socket args, bounded attempts/backoff, error mapping, cooperative budget refusal and no exports on acquisition failure | A2: D2/F strict source envelope and platform proof; B: dohod transport; D: publication/recovery |
| #29 | VALID_EMPTY and BLOCKED_KEEP_PREVIOUS compatibility | C2-04/D: freshness, manifest, generation/readback |
| #33 | No dohod semantic/completeness claim | C3 #31 parsing, #32 canonical/allowed URLs, #33 completeness/partial policy; B/D integration |
| #62 | Offline access/malformed/missing-block fixtures | Live endpoint, VPS and current responses NOT VERIFIED |
| #78 | Request phase, retry reason/attempts, duration, final outcome, redaction and primary precedence | F/D: whole-pipeline/process/bundle/overhead acceptance |

C3 is not an A1 prerequisite. Future B must separately preserve direct
`get_main`, `get_page_info` and `main` contracts and address existing early
writes through C3/D ownership. C2-02A2 remains `BLOCKED_D2_F_DEPENDENCY`; current
D2 publication preparation does not establish the accepted source envelope.
Controller/API, typed IPC, actual kill/wait, platform containment, direct
entrypoint integration and any shared hookup require separate owner PRs.

Rollback is a revert of the thematic A1 commit with both pair members and its
owned tests/docs. It restores the prior network behavior, including its
unbounded limitations; it does not roll back datasets. Other accepted work,
historical worktrees, runtime and evidence are retained. This slice authorizes
offline implementation/review and one draft PR only: no auto-merge, issue
closure, production reload, live probes, cron or FTP/Google publication.

## PR #90 corrective review: R1 to R2

Independent review of `22cdd4285f7e8e4993d8d55c5299fe1ab777d180` requested
two diagnostics corrections. Retry/final failure messages now include the
fixed marker `request_phase=OPEN|READ|CLOSE|DECODE` in the existing F-LOG
message envelope. Only these four literal tokens are admitted; no endpoint,
query, headers, credentials or payload is added. A successful decode checkpoint
clears the marker before parser/barrier work. Secondary cleanup cannot relabel
a primary READ failure as CLOSE. Exception identity/args, codes, categories and
healthy events stay unchanged.

A retry delay must fit the remaining operation budget before a warning is
emitted. The warning explicitly means a planned retry; its attempt number is
the failed actual attempt, not a prematurely incremented next attempt. Budget
is checked again after logging and after waiting. If diagnostic delivery uses
the remaining budget, the planned warning can exist but no subsequent wait or
request begins; terminal TIMEOUT retains the primary network error as cause.
An already infeasible delay produces no retry warning, wait or additional
request. Equality between delay and remaining budget is also refused.

Fourteen corrective cases cover both search/detail failure phases, original
exception/envelope/redaction, infeasible/equal delays, budget expiry during
diagnostics and absence of stale DECODE markers for parser/barrier failures.
On the unchanged reviewed production pair, the targeted RED returned
`12 failed, 2 passed, 114 deselected` (exit 1). New GREEN and exact-clean-HEAD
gate/hosted receipts belong in the corrective PR handoff, alongside the
preserved R1 receipts. HTTP/Retry-After/attempt/backoff/budget policies and all
C1/C2 domain contracts and residual owners remain unchanged.
