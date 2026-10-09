# A1-01 cold start and catalogue boundary

This implements accepted A1 R3 on bootstrap base
`2d3afeff3a8bbd012aa18e601f56ea6e39ffb9ac`. The historical plan/reviewer base is
`ebc66fc376a2077ce027cf33689aae31728709c2`. Scope is #5/#10 and local A1
instrumentation for #78. These issues remain open pending independent review
and their remaining acceptance criteria.

## Snapshot and catalogue contracts

`main` builds metadata once and forwards that same DataFrame object to every
reload/update. `data_update` and `full_reload` require `tickers_dates`; there is
no default, rebuild or disk fallback. Forwarding a valid fixture snapshot does
not establish real metadata validity. The existing builder, including its
uppercase/type policy, remains unchanged; lowercase catalogue/metadata
compatibility and broader metadata validation remain A5 work.

The parsed source catalogue, processing copy and disk-derived lookup copy have
separate roles. The helper changes only the copies. Nonempty source raw values,
columns, row order and index semantics survive the existing full XLSX/full CSV
and filtered stocks CSV exports. Export order, filenames, filters and writer
index defaults are preserved. An empty/structure failure stops source writers;
it does not repair a catalogue or establish A4 CSV/XLSX parity.

Processing uses stripped TRADE_CODE as its deduplication key, in first source
occurrence order. Blank strings and scalar pandas nulls are excluded. Every
other non-null TRADE_CODE must be a string, otherwise
`A1_TICKER_CATALOGUE_STRUCTURE` is raised. No `astype(str)`, uppercase, casefold
or parser option change is introduced here. Literal text `NaN`, `nan` and `None`
remains text; parser-dependent CSV literal policy remains separate work.
TRADE_CODE and SUPERTYPE headers must each occur exactly once. An empty
processing catalogue raises `A1_EMPTY_TICKER_CATALOGUE`.

An exact raw-code duplicate retains the legacy first row. A new collision
between different raw codes after strip is checked across its entire group
before choosing a row. Missing/non-string/blank or conflicting SUPERTYPE raises
`A1_AMBIGUOUS_TICKER_ROUTING`. Conflicting available scalar ISIN or INSTRUMENT_ID
raises `A1_AMBIGUOUS_TICKER_IDENTITY`; those values are compared without coercion.
Missing hints alone neither prove nor disprove identity. NAME, CURRENCY and
other differences do not create an invented source contract. Consistent alias
groups retain the first row; ambiguous groups stop for A5 resolution without
mapping, source repair or a validation framework.

## Stored candle identity and previous files

Immediately after reading an existing dataset, before dropping technical
columns, filtering 30 days, querying candles or writing that dataset, update
checks all stored ticker values against processing and lookup keys. A string
that changes under strip and whose stripped form is a current key raises
`A1_STORED_TICKER_INCOMPATIBLE`. Both padded-only and padded-plus-clean forms
refuse. No candle normalization, alias substitution or hidden annual backfill
is performed. Unknown/null/non-string stored values are not given a new broad
validation policy by this guard.

This refusal preserves the affected dataset's previous CSV/XLSX and prevents
its candle calls/writers. It does not prove that the source catalogue producer,
metadata builder or an earlier dataset wrote nothing. D owns the transactional
gate. A5 owns migration/identity decisions (#11/#12); A4/#58 owns broader
catalogue/export consistency. The early guards are partial links to those
packages, not completion claims.

History bounds, raw candle schema/order, RSI recalculation flow, existing
30-day filter and 365-day new-ticker policy are preserved. Tests use a literal
clock of `2024-03-02 12:00:00`; the inclusive cutoff is
`2024-02-01 12:00:00`. Processing order determines new-ticker query order;
the existing final ticker/begin sort still determines output row order.

Capacity/previous policy v2.1 remains mandatory. Preserving the Excel/Google
interface does not approve legacy CSV-only success when required XLSX is
omitted. Existing row-cap code is unchanged; capacity assessment, transactional
preservation and end-to-end release checks belong to D and the corresponding
publication packages. A1 `RETURNED` means the local function returned, not
publication or release success.

## Local diagnostics and caller integration

All four owned public entrypoints accept keyword-only `logging_context=None`.
Without context they import/create no logging resources. With context, A1 uses
the existing logger and closed event/error schema. Ordinary INFO requires
accepted plus current healthy status; unconfirmed INFO is allowed. ERROR
requires accepted, confirmed and current healthy status. Logical file names,
known dataset/interval/instrument, local monotonic duration and available counts
are emitted. Unavailable fields have null reasons. Catalogue cleanup does not
claim quarantine: rows_quarantined is null/NOT_ASSESSED.

Lookup read/preparation failures identify the known logical file
`ticker_lists/moex_full.csv`. Metadata builder start/failure has file=null with
the existing NOT_AVAILABLE_OR_NOT_APPLICABLE reason: the unchanged builder
writes XLSX then CSV, and its failed writer cannot be determined by this caller.

Source/domain exceptions are re-raised as the same primary object. If diagnostics
then fail, two independently protected signals are attempted: fixed stderr
`A1_LOGGING_FAILURE: LOGGING_INCOMPLETE` and the same exact exception note.
A failed fallback cannot replace primary. Once noted, propagation through owned
callers avoids duplicate failure diagnostics. With no primary, diagnostics
failure raises exact `RuntimeError("LOGGING_INCOMPLETE")` before further domain
calls. No false local success event follows either refusal.

The future main.py/F-LOG caller must recognize BOTH the exact note and the exact
RuntimeError type/one-argument signal; checking notes alone loses the no-primary
case. Apply signal handling to the data_gathering caller stage only.
Transient failure reconciliation into shared summary/delivery_outcome is
an independent integration AC, not proven by local recording doubles. A1 does
not modify main.py, run_logging.py or their schemas/resources. The owned
missing-file stdout is bounded Russian text without dataset_path; other foreign
diagnostic paths are not rewritten.

## Regression targets and isolated commands

The four targets are real main/data_update with a full_reload recorder; real
full_reload with candle/writer doubles; real existing/new update with exact
catalogue/dataset reads; and real moex_tickerlists with fixture HTTP bytes and
writer spies. Assertions use independent literal dates, arguments, ticker
sequences, keys, counts, row values, dtypes and schema. The catalogue read double
matches only `datasets/ticker_lists/moex_full.csv` with `index_col=0`, before the
first dataset existence check. Declared dataset reads have no parser options;
other data reads are rejected at the fixture boundary. There is no blanket isfile or
read_csv patch. Trailing fixture separators explicitly isolate #4; runtime-path
acceptance without a separator remains F2-02.

H reservation/ownership and tests/A admission are prerequisites and were
provided for this slice. Admission does not replace process preparation or add
hosted CI coverage. Before direct pytest, use the existing prepared immutable
F3 dev profile, disabled plugin autoload and cleared inherited pytest/Python/
ticket overrides. Do not install dependencies or create another guard.

From the owning checkout, after process-only preparation, use the exact
prepared interpreter represented by `$taskPython`:

```powershell
& $taskPython -I -B -X utf8 -m pytest -c pyproject.toml --collect-only -q tests/A
& $taskPython -I -B -X utf8 -m pytest -c pyproject.toml -q tests/A
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane f3
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane f2
& $taskPython -I -B -X utf8 tools/offline_tests.py --lane flog
& $taskPython -I -B -X utf8 tools/offline_tests.py --notebook-check --ref $taskHeadSha
```

The final source must be clean exact HEAD. The notebook command checks all ten
pairs without executing cells. Synchronize only the A1 pair after examining
both source members; self-review, environment preservation and exact six-path
diff checks are part of the handoff. Keep original RED/GREEN and failed runs.
Missing-receipt/guard failures are infrastructure evidence, not domain RED;
preserve and diagnose them rather than retrying until green.

## Acceptance evidence and remaining hookups

| Local AC | Regression evidence |
| --- | --- |
| Required same snapshot, one metadata build, eight cold/missing/force branches | test_cold_start orchestration tests |
| Clean, blank, scalar null, text sentinels, non-string and sparse source index | test_ticker_catalog helper matrix |
| Raw exports and first-writer source/structure/ambiguity refusal | real HTTP parser/export matrix |
| Date/query bounds and raw candle rows/dtypes/schema/writer options | real reload/update tests |
| Fresh/old/both-form stored refusal and previous bytes/checksums | stored identity fixtures |
| Fixed cutoff, ordered delta and unchanged history rules | cutoff/order controls |
| No context, INFO acceptance, primary precedence, two fallback signals | diagnostics fixtures |
| Path/URL/exception/nested canaries and bounded stdout | logger envelope/redaction fixtures and sanitized receipt audit |
| Bootstrap remains usable, foreign source unchanged | explicit A smoke plus exact changed-path comparison |

Default pytest and hosted domain A1 runtime remain **NOT_CONNECTED**. Existing
hosted bootstrap smoke is infrastructure coverage only. Caller context
forwarding and both-signal shared-summary handling remain **NOT_CONNECTED**,
owned by caller/F-LOG integration. Shared paths remain outside A1 ownership;
documented requests are handoff requirements, not completed transfers. Local
fixtures do not await complete A5/A8/B3/D4 implementation.

The previously unexplained local F2 receipt incident remains **NOT DIAGNOSED**;
a subsequent successful run/CI does not resolve its cause. Reports for this
slice must disclose any new tooling/infrastructure failures separately.

## Rollback

Before caller hookup, revert only this A1 pair, its three new test files and
this document as a coordinated reviewed change. Preserve bootstrap smoke,
shared F3/logger, environments, other writers' changes and evidence.

After caller hookup, reverting the domain signature requires coordinated
caller keyword/snapshot and diagnostics-signal disabling or compatibility.
Do not revert whole shared main/logger packages. Rollback does not include
dataset regeneration, cron or remote recovery writes.
