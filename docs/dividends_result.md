# C2-01: dividend response structure and empty release gate

This slice implements the approved C2-01 / R2 against base
`2120fb53e1acd0bf0e30620bda2a3faca7a2bdeb`. It has four owned paths:
`dividends.py`, `dividends.ipynb`, `tests/shared/c2/test_source_result.py`,
and this document. The paired dividend files have one writer. C1 tests,
coordination docs, D1, F3 bootstrap/guard/runner, shared logging, CI and
dependencies are outside this change.

## Empty release decision

`DIVIDEND_EMPTY_RELEASE_POLICY=BLOCKED_KEEP_PREVIOUS` comes from the owner's
answer in the C2 planning conversation, before R1:

> Сохранить previous (Recommended)

The selected description says that an empty candidate does not become current;
the release is BLOCKED/nonzero and previous keeps its date, with evidence-based
STALE or UNKNOWN status. This is a dividend decision, independently of the
candle `REQUIRE_NONEMPTY` contract. An explicitly empty agreed snapshot is an
alternative requiring a separate owner decision and C2-04/D publication checks.
If the primary decision is unavailable at a future handoff, record
`DIVIDEND_EMPTY_RELEASE_POLICY=NEEDS_DECISION`; parser review remains independent
of that policy gate.

C2-01 refuses before writers when the selected scope is empty or every successful
response in the current traversal is empty. It measures the accumulator delta,
so old global rows cannot turn an all-empty traversal into a release. It does
not persist candidates, update previous timestamps, create manifests, label
freshness or perform publication readback/recovery. Those belong to C2-04/D.

## Response contract and diagnostics

The private procedural parser requires a JSON object with a `dividends` object,
list `columns` and list `data`. Column names must be unique, nonblank strings;
`registryclosedate`, `value` and `currencyid` are required. Extra columns and
reordered columns are supported. Every row is a list of exactly the source
width. A missing `dividends` block retains `KeyError('dividends')`; other
structural violations raise `ValueError` prefixed by
`DIVIDEND_SOURCE_SCHEMA_INVALID`.

All rows of one response are prepared locally by column name. Accumulator
mutation follows event acceptance, confirmed flush and healthy logger checks
when a logging context is supplied. A late malformed row or failed logging
barrier contributes no rows from that response. Contributions of earlier
successful responses remain; this is not whole-run rollback.

| Response/collection state | Meaning |
| --- | --- |
| Nonempty, structurally valid | Existing `dividend_loader_returned`, `RETURNED`; counts describe this SECID response only. |
| `VALID_EMPTY` | Explicit `data=[]` with a valid required schema; the same event has `VALID_EMPTY`, received/accepted counts zero. |
| `FAILED` | Request, JSON, schema or diagnostics failure; no successful loader return. |
| Selected SECIDs traversed | Local iteration completed; source coverage, semantic quality and live contract are unverified. |
| Previous STALE/UNKNOWN | Separate snapshot freshness evidence, outside the response state. |

Healthy nonempty C1 event names, sequence, counts and `RETURNED` are retained.
`COMPLETE` is not a response outcome. Shared logger `delivery_outcome=COMPLETE`
describes diagnostics delivery only. `event.fields` remains empty; endpoint is
in the existing error envelope only. `rows_quarantined` stays null with
`NOT_EVALUATED`, without claiming zero quarantined rows.

| Failure code | Category | Retry policy |
| --- | --- | --- |
| `DIVIDEND_SOURCE_SCHEMA_INVALID` | SOURCE | final, nonretryable |
| `DIVIDEND_SCOPE_EMPTY_UNVERIFIED` | QUALITY | final, nonretryable |
| `DIVIDEND_EMPTY_RELEASE_BLOCKED` | QUALITY | final, nonretryable |

Collection gates emit ERROR `dividend_collection_failed`, outcome `BLOCKED`.
Loader errors retain `dividend_loader_failed`, outcome `FAILED`. A secondary
failure in error construction, emission, confirmation or fallback stderr cannot
replace the primary exception. The existing required pipeline stage propagates
that exception and finishes FAILED/nonzero. BLOCKED is its domain reason and
does not mean a successful run. The shared logger and `main.py` are unchanged.

## Compatibility and evidence limits

Public `div_loader`/`main` signatures and successful `None` returns are retained.
Output columns remain `ISIN, TRADE_CODE, dt, value, currency`; output paths remain
`datasets/dividends/all.xlsx` and `datasets/dividends/all.csv`, with `index=False`
and unchanged writer arguments. Decoded values, response order and repeated
payout events are preserved. CSV literal/quoting/NaN/precision policies are not
redefined. Date 2111, zero/small amounts and currencies are not repaired by
guessing; semantic validation belongs to C2-03.

[C1-02](https://github.com/foykes/moex-dataset/issues/27#issuecomment-6094880460)
still owns repeat-main independence: a new nonempty response can still cause old
global rows to be exported. Success-to-success and partial-failure-to-success
independence are unresolved. There is no accumulator reset, cache or dedup here.

C1 source fixtures are fixed examples, not proof of the current official
endpoint or VPS contract. Neither HTTP 200 nor correct SECID proves a dividend
block exists. [#62](https://github.com/foykes/moex-dataset/issues/62#issuecomment-6094883998)
remains NOT VERIFIED. Tests use fixtures; no live probe or access-restriction
bypass is part of C2-01.

OHLC/raw/history, all time, RSI, Google/Excel structure, required-export capacity
policy and production targets are unchanged. D1 PR #87 remains separate.

## Regression oracle and execution preflight

The 63 C2 cases use public loaders/main and independently literal expected rows,
counts and output schema. The 91 existing C1 cases remain unchanged. Coverage:

- Named/reordered/extra columns, decoded values, duplicates and literal output
  parity, filenames, column order and writer kwargs.
- Malformed top-level/block/data/columns types, duplicate/blank/non-string or
  missing columns, wrong row width/types and late malformed rows.
- Explicit empty responses, mixed empty/nonempty SECIDs in both orders, empty
  scope, all-empty fresh/preseeded accumulators and partial failure after success.
- Missing block, HTML/malformed JSON and HTTP failures, logging barrier faults,
  secondary collection diagnostics failures, redaction canary and required-stage
  FAILED/nonzero propagation.

Main refusal scenarios assert zero writer calls; all-empty and existing-source-
failure scenarios additionally verify unchanged bytes/hashes of disposable
previous files. Loader refusals verify per-response no-mutation and diagnostics.
This proves local refusal paths, not production preservation or recovery.
Per-response accumulator assertions do not claim per-run reset.

RED uses new tests with unchanged production blobs at the exact approved base;
expected values do not call the production parser. It produced 51 failed / 103
passed, exit 1. The first GREEN produced 154 passed, exit 0. Both direct runs
have run-bound start/final reports, working-tree source digests and zero guard
violations; these results do not replace clean-HEAD checks or hosted CI.

Before direct pytest or the fixed offline entrypoint:

1. Verify owning root, base, fixed targets and unchanged bootstrap/guard/runner.
2. Start a separate process with an environment allowlist. Remove inherited
   `PYTEST_*`, `PYTHON*`, `GIT_*`, `MDS_OFFLINE_*`, `MDS_ENVIRONMENT_*` and live/
   diagnostic overrides. Do not inherit credentials, `.env` or setup secrets.
3. Set `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`, `PYTHONDONTWRITEBYTECODE=1` and UTF-8.
   `-I` alone does not provide this isolation. Do not use `--noconftest`, escaping
   `--confcutdir`, arbitrary plugins or guard relaxations.
4. Check Windows CPython 3.14.8 dev provenance, locks and immutable fingerprints
   before/after. Install nothing. A borrowed interpreter is not owning-root F1
   environment evidence.
5. Allocate fresh owned disposable TEMP/TMP, cache, reports and fixture roots;
   reject linked, foreign or reused evidence roots.
6. Confirm root conftest bootstrap installs guards before collection. Reconcile
   start/terminal records, run/SHA/source digest, bootstrap/policy identities and
   violation counters. Preflight/guard failure stops the slice.

Commands, run in that prepared child environment:

```powershell
& $taskPython -I -B -X utf8 -m pytest -c pyproject.toml -q tests/shared/c2/test_source_result.py tests/shared/c1
# Python source is authoritative for this pair; inspect both members first.
& $taskPython -I -B -X utf8 -m jupytext --update --to ipynb dividends.py
& $taskPython -I -B -X utf8 tools/notebook_sync.py check --worktree
# After committing, use the exact clean HEAD:
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane f3
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane f2
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane flog
& $taskPython -I -B -X utf8 tools/offline_tests.py --notebook-check --ref $taskHeadSha
git diff --check
```

The handoff records actual base/HEAD, commands/counts/exitcodes, interpreter
fingerprints, guard evidence, pair sync, redacted domain logs and exact-head CI
separately. `NO_AUTO_MERGE=true`; review does not authorize merge, production
operations or complete issue acceptance.

## AC contribution matrix

These are bounded fixture contributions; #29/#30/#57/#62/#78 remain open.

| Issue / AC | C2-01 contribution | Remaining owner/slice/evidence |
| --- | --- | --- |
| #29: distinct empty-success, source-failure, stale statuses | Response outcomes and collection refusal | Previous freshness/manifest: C2-04/D |
| #29: agreed empty release versus previous with stale | Cited owner decision and BLOCKED gate | Actual STALE/UNKNOWN evidence: D |
| #29: consistent CSV/XLSX/manifest for each state | Zero writers on refusal | One-generation publication: C2-04/D |
| #30: connect/read/stage deadlines and bounded retries | Outside this slice | C2-02 for both loaders; strict deadline needs a stopping mechanism |
| #30: observable timeout, no partial publication | Outside this slice | C2-02 and D |
| #30: tests avoid real sleep/external network by default | New fixture tests under existing guard | Full loader timeout/retry acceptance: C2-02 |
| #57: source-confirmed 2111 correction or unknown | Value preserved; no invented date | C2-03; live source evidence unresolved |
| #57: small/zero amounts, precision, currency, natural key | Existing decoded/output representation preserved | C2-03; key/dedup: C1-03/F0 |
| #57: observable implausible-value quality status | Structural schema errors only | Semantic quality: C2-03 |
| #62: official endpoint/access/schema on VPS without 403 bypass | Outside this slice | NOT VERIFIED; separate live-read profile |
| #62: real SBER/MOEX responses validated by block/column names | Fixture parser contribution | Current real responses/VPS: NOT VERIFIED |
| #62: missing block/HTML/auth/unverified empty fails and keeps last snapshot | Offline errors, zero writers, disposable previous hashes | Live source and end-to-end preservation: C2-04/D |
| #78: stage lifecycle, run_id, separated worktree logs | Dividend events using existing context | Whole pipeline: F/integration |
| #78: identify unprocessed instrument/page/file, distinct empty/failure | SECID response/empty/schema diagnostics, null inapplicable fields | Other stages; C2-02/03 |
| #78: status/exitcode matches mandatory stages, child/publish cannot succeed falsely | Dividend refusal propagates FAILED/nonzero | Child/publication: D/F |
| #78: bounded DEBUG/readable console without output/schema changes | Bounded domain messages, existing output contract | Full operator workflow: F |
| #78: retry/schema/page/overlap/quarantine/overflow/child/transfer/rollback fixtures | Schema/barrier/empty faults | Retry: C2-02; quality: C2-03; remaining A/B/D/F |
| #78: canaries in messages/URLs/exceptions/nested fields/bundle | Domain canary protection | Full envelope/bundle: F/integration |
| #78: real Windows spawn/queue/listener/shutdown evidence | Outside this slice | F-LOG/integration |
| #78: rotation/retention/path/disk faults without false success | Primary-error preservation and refusal | Rotation/disk/retention: F |
| #78: redacted diagnostic bundle and manual independent review | Domain handoff evidence | Full bundle: F/integration |
| #78: measured logging overhead and operator instructions | Outside this slice | F/integration |
| #78: staging/failure injection without production credentials | Credential-free offline fixtures | End-to-end staging: C2-04/D/F, separate authorization |

## Rollback

Revert the thematic commit including both dividend pair members; preserve the
worktree and evidence. This restores the previous parser and empty-release
defects. Code rollback is separate from dataset rollback and does not establish
future previous/readback/recovery behavior.
