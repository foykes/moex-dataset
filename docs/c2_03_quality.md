# C2-03A — Dividend characterization

C2-03A freezes observations of the existing dividend parser and exports using
offline fixtures. GREEN means that measured behavior matches the reviewed
baseline; it does not mean semantic quality PASS, a verified MOEX contract,
complete source coverage, or publication readiness. Runtime quality policy
remains **NEEDS_DECISION**.

## Scope and baseline

The approved R2 preparation slice owns exactly four new paths:

- `tests/shared/c2_quality/test_characterization.py`
- `tests/fixtures/c2_quality/source_cases.json`
- `tests/fixtures/c2_quality/export_oracles.json`
- `docs/c2_03_quality.md`

Grant and exclusive writer apply only to these paths. The dividend pair is
read-only; this slice neither reserves it nor blocks C1 workers. Formal H for
the pair is a prerequisite of a separately approved future runtime change.
Shared guards, logger, configuration, CI and other owners' files are unchanged.

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

## Fixture and observation contracts

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

## Exact precision oracle

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

## Findings and remaining decisions

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

## Acceptance, drift and rollback

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
