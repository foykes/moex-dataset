# Dividend identity: C1-01

Refs #28, #27, #75, #62, #78.

Approved plan: `PLAN (15).md`, SHA-256
`c0799d03821c94ef0f7cc30df2c3cf6eb8d599e199481355c12b92bcf5226fec`.
Accepted base: `ebc66fc376a2077ce027cf33689aae31728709c2`.

`div_loader(isin, ticker)` reads the search table's `secid` and `isin` by
column name. It accepts only rows whose ISIN is exactly the requested string.
Repeated identical SECIDs within this search result are one candidate. More
than one distinct exact SECID is an ambiguity error before any dividend request;
the historical multiple-SECID fixture in #75 does not establish an alias contract.

Input ISIN must be a string matching `[A-Z]{2}[A-Z0-9]{9}[0-9]`. This structural
check rejects missing/NaN values, casing changes and whitespace; it does not
perform an ISIN checksum validation. Search columns must contain exactly one
`secid` and one `isin`, and every data row must match the column width. SECID is
an opaque nonempty string: whitespace, controls, `/`, `\`, `?`, `#`, `.` and
`..` are rejected; other safe characters are percent-encoded as one URL segment.
Nonexact ISIN rows are excluded before candidate validation.

Observable `ValueError` prefixes distinguish `DIVIDEND_ISIN_INVALID`,
`DIVIDEND_SEARCH_SCHEMA_INVALID`, `DIVIDEND_IDENTITY_NOT_FOUND`,
`DIVIDEND_IDENTITY_AMBIGUOUS` and `DIVIDEND_SECID_INVALID`. A correctly selected
SECID does not prove the downstream source contract: missing `dividends` remains
the original `KeyError('dividends')`. This slice does not guess a live endpoint,
access policy, response contract or empty-snapshot policy.

## Domain diagnostics

Both dividend entrypoints accept an optional keyword-only `logging_context`.
The pipeline forwards its exact context only to the dividends stage. Old callers
retain their positional arguments and `None` return. No logging session is
configured by importing or calling dividends without a context.

Domain events cover collection start, identity search/selection, dividend request,
loader return, export input and collection return. Selection counts measure
search rows and exact matching rows; the event instrument identifies the selected
SECID. Loader counts measure the received payout rows, without event deduplication.
Export counts measure the existing accumulated input frame, not independent
per-run output. Unmeasured values have canonical null reasons. Ordinary
`event.fields` stays empty, and endpoints appear only in `error.endpoint`, with
the existing logger's query/userinfo redaction. Logger/schema code is unchanged.

Invalid identity with a real disposable context fails before source requests.
Source errors preserve their original exception, even if error construction,
delivery or the fallback diagnostic fails. Rejected INFO, unhealthy diagnostics
or an unconfirmed loader barrier cannot produce a successful loader return.
INFO receipts need acceptance; ERROR and the completion barrier need confirmation.

## Offline evidence and limits

The original public two-argument loader is the RED oracle. Literal SBER/MOEX
fixtures with Cyrillic display names reproduce the old wrong URL mapping;
column permutations prove name-based selection. Fixtures also cover fuzzy
matches, missing identities, malformed tables, candidate validity, ambiguity,
safe URL encoding and a separate missing downstream block.

Real disposable logging tests exercise canonical envelopes/counts, redaction,
primary exception preservation and exact pipeline context forwarding. Export
spies compare literal five-column frames for both old and logged callers. They
do not serialize files or prove atomic publication, previous-file integrity,
capacity readiness or live access. Same-date different-currency/value payouts
remain separate rows in these fixtures; no event key is approved here.

Use the accepted immutable Windows development interpreter with `-I -B`; do not
install dependencies or execute notebooks. Canonical validation commands:

```text
python -I -B -X utf8 tools/offline_tests.py --lane f3
python -I -B -X utf8 tools/offline_tests.py --lane f2
python -I -B -X utf8 tools/offline_tests.py --lane flog
python -I -B -X utf8 tools/notebook_sync.py sync --base BASE_SHA --pairs dividends
python -I -B -X utf8 tools/offline_tests.py --notebook-check --ref HEAD_SHA
git diff --check
```

Sync/check is conversion and comparison only. Both dividend pair members must
be reviewed and staged together; the commit uses the existing guarded hook.
Local tests and hosted CI are separate evidence in the review handoff.

## Acceptance coverage

| Issue / criterion | C1-01 evidence or remaining work |
| --- | --- |
| #28 named SECID + exact ISIN + reordered columns | Public loader literal SBER/MOEX tests |
| #28 invalid/NaN, ambiguity, invalid SECID | Refusal tests, including real context and zero requests for invalid caller identity |
| #28 correct URL, separate downstream schema failure | Public URL assertions and original missing-block exception with domain error endpoint |
| #27 all repeat-main independence criteria | Deferred to C1-02; global `divs_all` remains unchanged |
| #75 unique SECID requests per run | Only repeated identical candidates within one loader call are covered; run-level cache deferred to C1-02 |
| #75 approved event key and lossless deduplication | Deferred to C1-03 after the owner/source contract decision |
| #62 official/live/VPS schema, access, empty snapshot and previous integrity | Unverified; missing-block refusal is narrow offline evidence only |
| #78 domain logging | Existing envelope, typed errors, counts, timing, redaction and exact context propagation; publication/capacity claims remain unverified |

The global accumulator, association preparation, payout positional extraction,
five output columns (`ISIN`, `TRADE_CODE`, `dt`, `value`, `currency`), order,
filenames, `index=False` and conditional writers remain unchanged. Correcting
identity can change which source is requested and therefore future export
content; it does not change the public schema. No timeout/retry, publication
writer, source pagination or repeat-main/event-dedup fix is included.

Rollback is reverting this single slice, including both dividend pair members.
The previous mapping bug then returns. No dataset files, settings, dependencies,
logger, guards, CI or other notebook pairs are changed.
