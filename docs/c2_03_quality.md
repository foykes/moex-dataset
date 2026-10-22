# C2 — Dividend characterization and response-local guard

## Historical C2-03A record

C2-03A freezes observations of the existing dividend parser and exports using
offline fixtures. GREEN means that measured behavior matches the reviewed
baseline; it does not mean semantic quality PASS, a verified MOEX contract,
complete source coverage, or publication readiness. Runtime quality policy
remains **NEEDS_DECISION**.

### C2-03A scope and baseline

The accepted R2 preparation slice owned exactly four new paths:

- `tests/shared/c2_quality/test_characterization.py`
- `tests/fixtures/c2_quality/source_cases.json`
- `tests/fixtures/c2_quality/export_oracles.json`
- `docs/c2_03_quality.md`

Its grant and exclusive writer applied only to these paths. The dividend pair
was read-only; C2-03A neither reserved it nor blocked C1 workers. Formal H for
the pair remained a prerequisite of a separately approved runtime change.
Shared guards, logger, configuration, CI and other owners' files were unchanged.

| Baseline item | Exact identity |
|---|---|
| Source base | `b423a7817e9a508fc167cdd47704d0b788252a8c` |
| `dividends.py` blob | `7e63237528dbf43d2fe9dd3fba7ac277f477dc35` |
| `dividends.ipynb` blob | `2b297e85944ad7445889ec85c5ce4931271adc3f` |

Integration base advanced compatibly to 2b222d16782f986ecbaf4e3efb04a81eefa1a0ba
(PR #97, Linux environment files). Ancestry was verified and the dividend pair,
Windows dev lock, collection configuration, guards and serializers were unchanged.
The table identifies the original characterized source; exact candidate HEAD
and integration evidence are recorded separately in the handoff.

Fixture identity checks retain both blobs. Fresh module imports isolate tests;
they do not repair the production global accumulator or repeated-run behavior.
No live HTTP, production file, credential, publication target or real writer
destination is used.

### C2-03A fixture and observation contracts

The source fixture inventory contains 41 independently named cases. Outer
fixture JSON contains no NaN/Infinity constants. Source bodies are literal
UTF-8 JSON strings, while runtime-only values use controlled descriptors.

| `input_kind` | Meaning |
|---|---|
| `STANDARD_JSON` | Literal body follows JSON syntax; a finite numeric token can supply an independent decimal reference. |
| `NONSTANDARD_JSON_EXTENSION` | Synthetic body explicitly exercises decoder acceptance of `NaN`, `Infinity` or `-Infinity`; this is not an official-source claim. |
| `PYTHON_RUNTIME` | Already-decoded synthetic rows use `runtime_float` descriptors `nan`, `inf`, `-inf`; no source body or source numeric token is invented. |

`raw_json_utf8` is null for runtime cases; `runtime_rows` is null for body cases.
Each `value_inputs` entry binds a zero-based `row_ordinal` to its exact
`raw_token`, independently stated `decimal_reference`, and `comparison_reason`.
Only a finite source numeric token has a decimal reference. String, boolean,
null, container and nonstandard nonfinite inputs retain explicit reasons.
Runtime entries have null token/reference and `NOT_APPLICABLE`, because there
was no source token. A syntactically valid `1e309` is a finite decimal reference
even when binary decoding produces infinity.

Provenance is `SYNTHETIC` or `HISTORICAL_AUDIT_RECONSTRUCTION`. The healthy case
is synthetic. The `2111-01-01`, 17.35 RUB example reconstructs historical #57
evidence; it is neither a captured current response nor proof of a source error.
No corrected year, official source provenance or future-date cutoff is invented.

Oracles record actual stages `decode`, `parse`, `loader`, `accumulator`, `frame`,
`xlsx`, `csv`, and `main` as `RETURNED`, `RAISED` or `NOT_REACHED`. They retain:

- Exact tagged Python/numpy types, scalar values, finite float hex and explicit
  nonfinite tags; decoded/prepared/accumulator row order and duplicates.
- Frame column order, dtypes, cells and null mask; writer destinations, order,
  arguments and artifact presence.
- Literal UTF-8 CSV bytes represented as text, including line endings, and
  parsed fields; XLSX XML cell types/literals and actual openpyxl readback
  cell types/values. XLSX ZIP bytes and timestamps are not golden evidence.

Only successfully returned serializers receive content/readback observations.
Expected exceptions retain class and fixed safe code, without raw exception
repr or payload. Later unreached stages stay `NOT_REACHED`; an XLSX exception
does not fabricate a subsequent CSV call. A partially created artifact is not
classified as a completed export. Guard/assertion/infrastructure failures remain
test failures and cannot be accepted as baseline domain outcomes.

### C2-03A exact precision oracle

Let `reference = Fraction(Decimal(literal_source_token))`. The recorded signed
delta is **observed minus reference**, using reduced rational numerator and
positive denominator strings. Decimal context precision cannot round the
acceptance arithmetic.

For integers, observed value is an exact integer fraction. For finite binary
floats it is `Fraction.from_float(value)`. For numeric CSV/XLSX XML tokens it is
`Fraction(Decimal(serialized_token))`. Binary values and decimal serialized
tokens are separate observations; a float's display is not a source oracle.
The literal reference is checked against the source token independently of
production `json.loads` and pandas conversion.

Nonfinite, boolean, nonnumeric, absent and unreached observations have null
delta with a reason. Missing source references never become delta zero. No
tolerance, rounding, coercion, discarded tiny amount or production Decimal
conversion is introduced. Nonzero deltas remain findings, even when the
characterization test is GREEN.

Expected observations are fixed from the unchanged pair in the pinned profile:
Windows CPython 3.14.8, pandas 2.3.3, numpy 2.3.4, openpyxl 3.1.5 and pytest
8.4.2. Real original pandas serializers run only in fresh guarded disposable
fixture paths. Source reads, network calls and clock/wait behavior are doubles.
There is no automatic golden update; changed expectations require independently
checked arithmetic, an explained source/environment change and review.

### C2-03A findings and remaining decisions

The guarded baseline capture measured all 41 cases on the unchanged pair:
35 standard JSON, three nonstandard decoder inputs and three runtime injections.
Thirty-eight pipeline calls returned and three raised the expected failures.
Before freezing the oracles, a separate arithmetic review recomputed 172 finite
comparisons from literal decimal references and the recorded typed values.
The working-tree targeted suite passed all 44 tests. These are characterization
results, not clean-HEAD, hosted, source-quality or production acceptance.

| Case | Observed finding |
|---|---|
| healthy | Source 17.35 becomes a binary float with delta 1/703687441776640; CSV and XLSX numeric XML retain decimal 17.35 with delta zero. |
| long_decimal | Source 0.10000000000000001 becomes float 0.1; binary delta is -122290016319/27487790694400000000000000000. CSV/XML decimal delta is -1/100000000000000000; readback returns the binary float. |
| underflow_positive | Standard JSON token 1e-400 becomes float zero; every reached numeric stage has exact delta -1/10^400. CSV contains 0.0; XLSX readback is integer zero. |
| large_integer | Integer 9007199254740993 survives decoding, accumulator, DataFrame and CSV, but XLSX numeric XML/readback contain 9007199254740992, delta -1. |
| large_integer_and_null | Adding a null row induces a float column; delta -1 begins at DataFrame construction and persists through both exports. |
| zero_and_null | Accumulator retains integer zero and None; frame contains float zero and NaN. Blank export cells do not preserve the upstream null type. |
| overflow_number | Standard JSON token 1e309 has a finite decimal reference but decodes to infinity; CSV and XLSX contain string inf. No finite delta is invented. |
| serializer_illegal_character | Synthetic currency contains U+0001. Real XLSX serialization raises IllegalCharacterError; a disposable XLSX artifact exists, CSV is not reached, and no completed XLSX readback is claimed. |
| late_malformed_row / valid_empty | Respectively, schema refusal before response append and existing empty-release refusal occur before both writers. |

The same source token can have a nonzero binary delta and zero decimal
serialization delta. Neither observation can replace the other. Exception,
partial artifact, type/lexeme change and zero delta have separate meanings;
none establishes dataset losslessness or quality PASS.

Exact-HEAD local and hosted acceptance must be verified using the handoff
receipts and PR checks for the actual candidate SHA. This document does not
substitute an earlier working-tree run for those gates.

Dates, zero/tiny/negative amounts, malformed values, unknown currency, same-date
payouts and identical rows characterize preservation and representation; they
do not establish mandatory quality rules. Calendar validity alone cannot verify
`registryclosedate` semantics, and RUB vocabulary alone cannot establish units.
Official source semantics, amount units, currency vocabulary and the 2111 event
remain #62/#57 evidence work. Equal date/value/currency does not prove identity.

C1-02 owns repeat-run accumulator independence; C1-03/#75 owns the natural event
key and lossless dedup. C2-03A does not reset or deduplicate rows. Runtime BLOCK,
quarantine and preserve-with-NOT_VERIFIED alternatives require an owner policy
decision. F0 quality/sidecar structures and C2-04/D publication/evidence hookups
remain proposed integration work, not implemented interfaces in this slice.

The public five-column order remains `ISIN, TRADE_CODE, dt, value, currency`;
`all.xlsx` then `all.csv`, `index=False`, direct return behavior, identity,
logger barrier, VALID_EMPTY/empty-release gates and network policy are unchanged.
Existing C1/C2 regressions verify those boundaries. Zero dividend writers does
not prove whole-pipeline atomicity or previous preservation for every output:
`main.py` runs `data_gathering` before dividends. This PR cannot close #57.

### C2-03A acceptance, drift and rollback

Use the verified owning environment and existing [offline guards](testing.md).
Before handoff, record a clean exact candidate HEAD and the following evidence:

1. Guarded `--lane f3 --collect-only` reports contain every expected new case
   node ID. Collection is inventory evidence, not successful test execution.
2. Run the new characterization tests and local F3, F2, F-LOG and all ten
   notebook-pair checks on that same clean HEAD; retain execution/guard reports.
3. Require hosted exact-HEAD Windows F1/F2/F-LOG/F3/notebook checks and Linux AST
   syntax evidence. Skipped or failed mandatory checks are NOT VERIFIED and
   block handoff; legacy quarantine is not C2-03A quality evidence.
4. Handoff includes actual base/HEAD, unchanged pair blobs, fixture/test and
   environment digests, baseline observations, findings, scope/regressions,
   self-review and rollback. No blanket xfail or fabricated RED result is used.

Compatible main movement requires ancestry/path/shared-contract review and
remeasurement on the integrated candidate HEAD. Oracle changes require a
justified behavior change and independent review; movement alone is not a new
owner approval gate. A competing writer or incompatible source/fixture contract
stops only the affected integration. No automatic blessing, rebase or merge is
part of this slice.

Rollback is a separately authorized thematic revert of this four-path change.
Preserve evidence, environments and other worktrees. No dataset reload, source
correction, production file rollback or publication recovery is implied.

## C2-03B — Response-local runtime profile

C2-03B adds a deliberately limited consumer profile after full structural
parsing of one dividend response. It does not establish the official MOEX
source contract or a whole-release quality policy. The implementation base is
`c98664b7da20375cf0bfed8125a94d64008d0d63`. The owner/coordinator explicitly
granted the ten paths below and satisfied Condition H for both active dividend
members; the ignored handoff receipt records the grant and base evidence.

```text
dividends.py
dividends.ipynb
tests/shared/c2/test_source_result.py
tests/shared/c2_quality/test_characterization.py
tests/shared/c2_quality/test_runtime_quality.py
tests/fixtures/c2_quality/baseline_v1/dividends.py
tests/fixtures/c2_quality/baseline_v1/dividends.ipynb
tests/fixtures/c2_quality/source_cases_v2.json
tests/fixtures/c2_quality/export_oracles_v2.json
docs/c2_03_quality.md
```

Other tracked paths, including the original source fixtures/oracles, logger,
F0 contracts, guards, runner, collection configuration, workflows and dependency
locks, remain outside this write scope. The grant does not reserve unrelated
C1 work; only actual changes to the shared dividend pair require coordination.

### Mandatory checks and preserved uncertainty

Rows are checked in source order, using zero-based response ordinals. Each row
is checked in `dt`, `value`, `currency` order; the first violation terminates the
response. Type checks use exact built-in types, so subclasses do not bypass the
profile.

| Field | Accepted local profile | Failure rule |
|---|---|---|
| `dt` | Exact `str`, ASCII `YYYY-MM-DD`, valid Gregorian date in years 0001–9999 | `DATE_TYPE`, `DATE_SHAPE`, `DATE_CALENDAR` |
| `value` | Exact `int`, or exact finite `float`; `bool` is excluded | `VALUE_TYPE`, `VALUE_NONFINITE` |
| `currency` | Exact nonblank `str`, without U+0000–0008, U+000B–000C or U+000E–001F | `CURRENCY_TYPE`, `CURRENCY_BLANK`, `CURRENCY_CONTROL` |

Calendar-valid `2111-01-01` and other future dates remain unchanged. There is no
clock-relative cutoff. The 2111 case remains historical audit reconstruction,
**NOT source-verified**. Zero, tiny and negative finite amounts remain unchanged;
there is no sign rule or economic threshold. Unknown and historical currency
strings remain unchanged: no whitelist, uppercasing, trimming or substitution
is introduced. `strip()` is only the blank predicate. HT, LF and CR are allowed
inside a nonblank currency string; this narrow C0 check does not promise full
XML representability or official currency validity.

The decoder and pandas/XLSX/CSV representation chain remain unchanged. There is
no runtime Decimal conversion, rounding, tolerance or correction. In particular,
decoded `1e-400` is already zero and passes this profile; its precision loss
remains a finding. Finite `0.10000000000000001` and `9007199254740993` can still
lose precision in later stages. Standard JSON `1e309` decodes to infinity and is
rejected as `VALUE_NONFINITE`; it is distinct from synthetic nonstandard JSON
constants and runtime nonfinite injections.

### Failure, logging and append boundaries

The ordered boundary is structural parse, budget check, semantic guard, budget
check, existing returned event, logger barrier, budget check, then one
`divs_all.extend(rows)`. A semantic failure raises `ValueError` with this closed
message shape:

```text
DIVIDEND_SOURCE_SEMANTIC_INVALID: rule=<fixed rule>; field=<fixed field>; row=<ordinal>
```

No source value/body, arbitrary `repr`, or source string is included. The fixed
code uses the existing error envelope with category `QUALITY`, `final=True`
and `retryable=False`; it is not a DECODE failure and causes no HTTP retry.
Secondary failures in error construction, event emission, health checks or
fallback diagnostics must not replace the original exception. Existing logger
redaction still applies to the existing instrument/endpoint envelope fields.
No logger fields or events are added.

The failing response contributes zero rows. Earlier successful responses remain
in the global accumulator. An exception stops the current dividend invocation
before its writers, but does not create a whole-release transaction or prove
production preservation. The successful event text states that local checks
ran while `source_semantics=NOT_VERIFIED`, `precision=NOT_VERIFIED` and source
completeness remains unchecked. Without `logging_context`, no separate durable
consumer status is promised.

The public five-column order, signatures, successful `None` returns,
`datasets/dividends/all.xlsx` then `all.csv` with `index=False`, transport
budgets/retries and logger barrier are retained. Duplicates and source order
remain intact. No global reset, dedup, quarantine, release gate or publication
logic is added. F0 `quality_result` remains PROPOSED.

### Frozen V1 and independent current V2 characterization

The accepted V1 record is preserved, rather than regenerated against the guard:

- `source_cases.json` stays byte-identical at Git blob
  `524943fa9f886aff9fdb751a5e21c9aad33738d3`.
- `export_oracles.json` stays byte-identical at Git blob
  `4f285c1184a8224db3df164d838c35935517cc0b`.
- `baseline_v1/dividends.py` and `.ipynb` preserve the original source blobs
  `7e63237528dbf43d2fe9dd3fba7ac277f477dc35` and
  `2b297e85944ad7445889ec85c5ce4931271adc3f`.

The historical harness uses only the archived root, verifies both archived
fingerprints before **every** historical import, and keeps the same 44 nodeids,
41 inputs and 172 finite comparisons. The notebook is identity evidence and is
not executed or registered as an eleventh active notebook pair. The archive
Python file does enter the tracked Linux syntax inventory.

Current V2 uses only the active root and its separately recorded source
fingerprints. `source.base_sha` identifies the integration parent, not the
commit containing those new blobs; exact tested candidate SHA is recorded in
external handoff evidence to avoid a self-reference. There is no fallback to V1.
Its manifest maps all 41 historical
cases and adds eight rule-boundary cases. The authored expected inventory is
49 cases: 32 semantic refusals, 15 successful exports, one structural refusal
and one empty-release refusal. V2 adds the prepared-row comparison before the
guard; its finite comparison inventory is 162. Independent static review checked
all 49 outcomes and recomputed all 162 comparisons: 112 nonzero deltas and 50
zero deltas, plus 216 explicitly noncomparable stages. A targeted guarded
working-tree run passed 273 runtime/source/characterization tests. This is
working-tree evidence, separate from final clean-HEAD and hosted gates.

For changed outcomes, decoded and prepared observations remain available;
semantic refusal is recorded separately and downstream append/export stages
are `NOT_REACHED`, not precision-preserved. The historical U+0001 currency case
still records its real serializer exception in V1; V2 rejects it before writers.
The null-companion cases that caused DataFrame inference loss in V1 are also
rejected locally in V2. Their historical losses are not erased or described as
repaired. Remaining serializer failures are distinct from lossless success.

Both versions retain the frozen Windows profile: CPython 3.14.8, pandas 2.3.3,
numpy 2.3.4, openpyxl 3.1.5 and pytest 8.4.2. Literal source tokens supply the
independent `Decimal`/rational references; observed floats never construct the
source reference. Finite deltas remain exact reduced signed fractions. Changes
to source identities or outcomes need an explained behavior change, fresh
measurement and independent oracle review. Tests are not disabled, golden
updates are not automatic, and an expected exception is not quality PASS.

### Evidence, residual work and rollback

Meaningful RED evidence concerns rejection assertions on the unchanged source,
not intentionally broken fingerprints. Before review handoff, all expected V1,
V2 and runtime nodes must be collected and executed through the existing guarded
F3 path. Real serializers may operate only in fresh, owned, guard-admitted
disposable paths. The same clean candidate SHA must pass local F3 collection
and execution, F2, F-LOG and all ten active notebook pairs / 20 members, then
hosted Windows F1/F2/F-LOG/F3/notebook checks and Linux syntax preflight. Linux
file counts come from the actual candidate inventory. Failed, skipped,
cancelled, unavailable or incorrectly bound mandatory checks do not satisfy
handoff. This document does not predeclare those outcomes.

Handoff records actual base/HEAD/tree, ownership evidence, source and fixture
fingerprints, frozen owning environment, historical/current collected and
executed inventories, RED/GREEN reports and exit codes, exact tested-SHA hosted
links, precision findings, changed paths, regression evidence, self-review and
rollback. New oracles require independent review, followed by independent
CODE_ACCEPT and separate merge permission; author evidence is not acceptance.

No runtime raw evidence/provenance store is added. The current decoded pipeline
cannot recover original numeric tokens or durable row-to-response provenance.
An independent evidence-store design needs owner decisions on immutable raw
bytes, manifest mapping, budgets, retention, access/redaction and capture
failure policy. Official source semantics and limited live-read permission
remain separate #62 work.

Whole-release FAIL/NOT_VERIFIED policy remains **NEEDS_DECISION**. #57 remains
OPEN/PARTIAL and no acceptance criterion is completed automatically. C1-02/#27
owns repeated-run state, C1-03/#75 owns event identity and separately approved
lossless dedup, and C2-04/D owns candidate/previous generation, freshness,
preservation, readback and publication. #29/#30 and #76/#78 retain their existing
release, transport and observability boundaries.

Compatible main movement calls for ancestry/path/contract review and updated
evidence. Incompatible dividend behavior requires current recharacterization
and independent oracle review; a concrete conflict stops only the affected
integration. There is no automatic rebase, force push or reset of another
worktree. Rollback is a separately authorized thematic revert of the entire
C2-03B change, restoring the pair and harness together while preserving prior
evidence. Dataset rollback is not needed for this offline-tested source change.
## C1-02 R0: independent main invocations and V3

R0 fixes the repeat-run accumulator defect in #27. Each `dividends.main()` call
owns a new row list. Only that call's accepted responses enter its export frame;
an earlier successful run, a failed run, or a standalone loader cannot seed it.
The private loader receives the destination explicitly. Response validation,
logging confirmation and the final budget check still precede its single append.
An exception stops the current invocation before its remaining writers; it does
not introduce a multi-file transaction or prove production preservation.

The public signatures and successful `None` returns are retained. Standalone
`div_loader(isin, ticker, *, logging_context=None)` keeps appending to the same
legacy `divs_all` object. `main(*, logging_context=None)` neither reads nor resets,
rebinds or updates that object. Reading `divs_all` after `main()` therefore no
longer exposes main's export rows. This observable state correction is intentional;
the standalone accumulator is not a release result API.

The five columns, source values, row order, identical payouts, filenames,
XLSX-before-CSV order and `index=False` remain unchanged. Only contamination from
other invocations disappears. Empty-current-run refusal, retries, cooperative
budgets, semantic rules, logging envelope/barrier and original exceptions remain.
SECID request reuse and natural-event deduplication are excluded. O6's approved
association policy belongs to the later reuse slice; no guessed key is introduced.

The historical C1-01 report in `docs/dividends_identity.md` remains unchanged in
its owner's custody. This R0 section supersedes its historical main/global-state
description only. Its identity, source-evidence and dedup residuals are not closed.
R0 is the separately authorized run-state subset, not completion of the full R,
P6, C1 or C2 release campaign.

### Frozen V2 before the first pair edit

Before either active pair member changed, exact Git bytes were retained at:

```text
tests/fixtures/c2_quality/baseline_v2/dividends.py
tests/fixtures/c2_quality/baseline_v2/dividends.ipynb
```

Their blobs are `e66e1effa4d113518d1ef99180176562f42e65d8` and
`affcc5efb1aad3b3b18fad3ba1c4c3c223781579`. The V2 JSON blobs remain
`f1b4cd8bff65ceecb4dd848a89d8e6383deab888` (`source_cases_v2.json`) and
`0a817cf03fdbb63993ffc76c48804847df87e742` (`export_oracles_v2.json`).

All existing V1/V2 test nodeids are retained: V1 41 cases / 44 nodes / 172 finite
comparisons, V2 49 cases / 53 nodes / 162 finite comparisons. The existing
`test_current_*` names now identify archived V2 for nodeid continuity; they do not
claim to exercise the active pair. Both historical source fingerprints are checked
before every historical import. The archived notebook is identity evidence, never
an additional active notebook pair; its Python member belongs to syntax inventory.

V3 uses only the active root pair and its separately pinned fingerprints in
`export_oracles_v3.json`. No current-to-archive fallback exists. Its 49 inputs and
all numeric/serialization expectations are preserved from independently reviewed
V2 before the first V3 measurement. The only added expectations are
`accumulator_scope=RUN_LOCAL` and `legacy_accumulator_rows=[]`. The harness observes
the actual list passed to the private loader, including accepted rows before an
exception, rather than substituting DataFrame output for accumulator evidence.
V3 retains 162 finite comparisons and the frozen Windows runtime profile.

This preserves the known binary/CSV/XLSX precision findings, underflow and
noncomparable outcomes. R0 changes no decoder, rounding, serializer or source
semantics. GREEN characterization still does not mean quality PASS or losslessness.
No historical oracle is updated automatically. A later source-adapter change must
archive V3 and introduce independently reviewed V4 in its own granted scope.

### Regression and verification boundaries

`tests/shared/c1/test_run_state.py` imports the module once per test, then calls
main twice. Literal independent expectations cover identical runs, changed second
inputs, schema/semantic failure followed by recovery, empty second collection,
standalone accumulator ownership and writer failure with primary-error identity.
The identical-run serializer case writes only to a fresh guard-issued disposable
root: CSV literal bytes and SHA-256 match; XLSX cell coordinates/types/values match
after actual openpyxl readback. ZIP timestamps are not a checksum oracle.

Existing named C2 tests continue to prove accepted responses before a later
failure, deadline/request accounting, no writers on source failure, preserved
disposable previous files, logging/counts, schema and writer arguments. Those
accepted rows now live in the observed run-owned sink, while the standalone list
stays unchanged. Their nodeids and parameter axes are retained. The held P6 test,
shared helpers/imports, other test zones and C1 identity document are untouched.

Development RED/GREEN, final clean-HEAD checks, hosted jobs and independent review
are distinct receipts. Required F3 collection/execution must include all 97
historical nodes, the V3 nodes and repeat-run regressions. Existing F1/F2/F-LOG,
all ten active pairs, source/environment integrity and exact-checkout hosted gates
remain required; this document does not declare pending checks passed.

Rollback requires a separately authorized revert of the complete R0 pair/harness/
test/doc changes while preserving evidence. The prior repeat-run defect would
return. Dataset restoration, issue closure, production activation, publication
integrity, live-source evidence and full C2 acceptance are not authorized here.
