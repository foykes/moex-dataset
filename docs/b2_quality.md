# B2 candle assessment and offline computation

The B2-01 sections below record the accepted R2 plan and R3 regression
clarification for the isolated assessment helper. The B2-02 section adds an
offline in-memory consumer. Legacy file callers are not activated through that
consumer; production calculation, retention and publication remain separate.
The runtime defect in [#16](https://github.com/foykes/moex-dataset/issues/16)
therefore remains open.

## Authority, ownership and compatibility

Implementation base: `59f2f57c8972fd4dd40f1843a24503aa906f6300`.
Branch: `codex/mds-b2-01`. The explicit scoped receipts are
`F-B2-01-20261016074307Z-943b3e2d` and
`F0-COORD-B2-01-20261016T074535Z-01a107af`.
Their six-path allowance is:

```text
data_quality.py
docs/b2_quality.md
tests/shared/b2/test_quality.py
tests/fixtures/b2/cases.json
tests/fixtures/b2/archived_observations.json
tests/fixtures/b2/evidence_manifest.json
```

The accepted F3 head `2417193260dfc4033f71db0de2ab3fcadd712a35` and its
merge are ancestors of this base. B1/TRIX/H89 is a read-only prerequisite.
The `tech.py`/`tech.ipynb` pair, B1 reference tests, shared F0 documents,
F-LOG, F3 runner/guard, CI, settings and datasets remain outside the grant.
The new standalone helper has no notebook pair. Later main drift requires a
fresh compatibility check, not an automatic rebase or wider reservation.

Public function names, CSV/XLSX columns/order/types/filenames, Google projection,
indicator formulae, horizon handling and export-capacity/previous policies do
not change. Quality metadata remains internal: no new public columns, sidecar
writer, float normalization profile or export/NaN convention is introduced.

## Internal interface and three populations

```python
assess_candles(frame, *, spec, metadata, retained_evidence,
               logging_context=None) -> assessment
```

The caller supplies existing [F0 concepts](dataset_contracts.md): spec, context,
scoped identities, retained raw locators, row bindings, independent producer
selection and, when available, independent source membership. The helper
checks the B2 subset; it is not a complete F0 dataset validator. It allocates
no run/snapshot identity and does not mutate inputs or create a replacement
DataFrame.

| Population | Meaning and counts |
| --- | --- |
| Scoped retained raw corpus | All retained variants for the declared snapshot and request scope, including unselected variants. Raw findings and counts refer to this corpus. |
| Original candidate | The producer's original selection, before B2 sorting/quarantine. `quality_result.rows_checked` is its measured row count. |
| Effective projection | A sorted view expressed as original candidate ordinals, grouped by full raw identity and interval. Accepted/quarantined candidate counts belong to this population's assessment. |

An unselected raw row is not automatically quarantined. In F0 E19, four raw
rows support a three-row candidate with A/A/B identity membership and no
quarantine. Consequently `rows_received == len(frame)` and
`received == accepted + quarantined` are not general invariants. Unknown or
unmeasured counts remain null with a reason; they do not become zero.

Every original candidate ordinal, including a subsequently quarantined row,
must have exactly one checked binding. The binding resolves
`identity_record_id`, `provenance_ref` and `raw_row_ordinal`; a natural key
alone cannot identify competing raw variants. Its raw key is
`engine/market/source_scope/board/raw_instrument_id/interval/begin`.
`end`, display ticker and pandas index are not key fields. Explicit AGGREGATE
scope with a reason for null board differs from an unknown board.

The ordered frame and binding fingerprints, snapshot/dataset/schema, complete
ordinal set, raw content and provenance are checked against independently
retained `producer_selection`. Tests freeze this authority before constructing
negative inputs. Recomputing candidate hashes cannot authorize changed lineage:
E19 A/A/B becoming A/B/B fails `IDENTITY_BINDING_MISMATCH` even with unchanged
public values/counts, valid raw locators and internally consistent new hashes.
Sorting never rewrites the input bindings or the expected selection.

Candidate field names must be unique and include every raw8 field and `ticker`
exactly once. A producer-declared schema and matching fingerprints do not make
duplicate names or missing required raw fields valid: those mappings fail before
values are assembled by field name. Additional unambiguous columns remain
unchanged.

Independent selection proves provenance, not candle uniqueness. Selecting one
retained raw locator twice cannot create two valid bars: duplicate candidate
natural keys block the affected effective group even when its raw corpus has
only one occurrence. Repeated raw locators are themselves invalid evidence;
they cannot overwrite each other in the source lookup. Evidence references
reject absolute/private path forms and `.`/`..` path components. Snapshot,
dataset and schema binding identities must be explicit nonempty strings:
matching missing/unsupported identities remain NOT_VERIFIED. Record IDs cannot
be empty. Known identity contradictions remain FAIL.

Only the existing miniature profile `ILLUSTRATIVE_JSON_ARRAYS_V1` is supported
for this evidence check. It is not a production MOEX float serialization
policy. Missing/unsupported normalization or independent authority yields
`NOT_VERIFIED`; contradictory known lineage or foreign snapshot yields `FAIL`.
All scoped raw variants are assessed, so an unselected conflicting variant can
make a selected row ineligible.

### Required context preconditions

The internal subset also checks the following existing F0 preconditions. No
consumer-generated context or new fields in `producer_selection`/`row_bindings`
are introduced. A supplied string is retained unchanged; checking that it is
nonblank does not trim or replace its value.

| Context | Supported input | Missing/unsupported evidence | Known contradiction |
| --- | --- | --- | --- |
| Metadata `run_id`; existing snapshot/dataset/schema binding IDs | Nonblank strings; existing spec/authority equality rules still apply. | Absent, null, empty/blank or unsupported type is NOT_VERIFIED when consistently unknown. | Different known binding/spec identities remain FAIL. There is no invented independent run-ID comparator. |
| Spec and metadata `contract_version` | Strict Python integer `1`; booleans are not integers for this check. | Missing/null/unsupported type, or matching unsupported integer versions, is NOT_VERIFIED. | Different explicit integer versions are FAIL. |
| Metadata `source` | Exact literal `MOEX_ISS_RAW`. The existing F0 spec has no source field. | Missing/null/blank or unsupported type is NOT_VERIFIED. | A different explicit nonblank string is FAIL. The approved label does not authenticate provider bytes. |
| Metadata `as_of` | ISO 8601 date/time with a `T` separator and explicit UTC zero offset; `Z`, `+00:00` and fractional seconds are supported without rewriting. | Missing/null/blank, unsupported type/representation, malformed calendar/time or a timezone-naive value is NOT_VERIFIED. | A parseable aware nonzero UTC offset is FAIL; no implicit timezone conversion. |

These are context checks only: no comparison with the current clock, market
session, candle end, acquisition time or publication time is added. The UTC
check also examines the literal offset: parser truncation of a nonzero offset
below microsecond precision cannot establish UTC. Fractional time and exactly
zero offset representations remain unchanged. The existing producer-selection
structure does not independently carry `run_id` or `as_of`;
its fingerprints cannot establish those missing correlations. Required context
uncertainty blocks local eligibility, and `FULL_DATASET_VALIDATION` remains
NOT_VERIFIED even when the supported context preconditions pass.

## Checks, selection and separate status axes

Numeric/finite checks cover all six raw numeric fields: `open`, `close`,
`high`, `low`, `value`, `volume`. The supported scalar representations are
Python int/float and NumPy integer/floating scalars, excluding booleans.
Required null, NaN and positive/negative infinity fail their required-value or
finite check. Bool, complex, numeric-looking strings, containers and other
unsupported representations are non-eligible and separately `NOT_VERIFIED`.
There is no `float()`/`to_numeric()` conversion, CSV parsing, assignment or
rounding; finite large integers are not corrupted by a conversion-overflow
test. Zero, finite negative values and fractional volume receive no universal
positivity/integrality restriction.

After valid scalar checks, OHLC must satisfy
`low <= min(open, close) <= max(open, close) <= high`.
`begin`/`end` require the declared timestamp representation and calendar
validity; `end < begin` is a contradiction. No timezone conversion or universal
`end - begin == 86399` rule is applied. Extra columns, source strings, values,
dtypes, index and original order remain untouched.

Each of the two archived ABIO multiyear observations is excluded as an ordinary
daily candle by its own exact scoped identity/interval/raw8 signature and
evidence reference: `CONFIRMED_ABIO_MULTIYEAR_OBSERVATION`. A singleton does
not need the other variant to receive this finding. Together they also form
`SOURCE_KEY_CONFLICT`; both temporal reasons and both locators survive. The
fixture contains reconstructed parsed observations, not original ISS bytes.
No guessed general duration threshold follows from these cases.

Exact duplicates, evidenced old/new revisions and source conflicts are distinct
groups. There is no automatic duplicate representative, keep-last or application
of a revision. An exact duplicate or unresolved revision-selection requirement
blocks verified sequence eligibility; acquisition order alone is not revision
evidence. Source conflicts quarantine every variant of the affected scoped key,
including a selected variant whose competitor is outside the candidate.
The local evidence rule recognizes only an independently linked two-row old/new
pair. A link covering two rows does not establish a revision chain for a third
unproven variant; that group remains a conflict.

The owner-approved [CONTINUE_ACROSS_QUARANTINE policy](https://github.com/foykes/moex-dataset/issues/76#issuecomment-6133109560)
keeps calculation state across excluded conflicts on the remaining verified
sequence. Ordinary non-trading gaps and partial candles do not themselves
trigger quarantine, reset, delisting or a completed-bar claim. Unresolved
required temporal evidence still prevents verified admission.

| Result component | Interpretation |
| --- | --- |
| `raw_assessment` | Checks/findings/groups and measured counts for the entire declared raw corpus; quarantine cannot erase its failures. |
| `quality_result` | Existing F0 shape for the original candidate, with caller context and no invented evidence/artifact refs. It always contains `FULL_DATASET_VALIDATION=NOT_VERIFIED`, reason `B2_INTERNAL_SUBSET_ONLY`: known required failure gives FAIL, otherwise the full result remains NOT_VERIFIED. |
| `eligibility` | Local status, blockers and ordered original candidate ordinals per identity/interval. Passing local checks is not production/TA-Lib/release permission. |
| `coverage_assessment` | Independent expected source-key membership, never inferred from accepted counts, MIN/MAX or a clean projection. Unknown membership is NOT_VERIFIED; a known missing expected key is INCOMPLETE. |
| Source verification | Fixture/archive-fragment integrity is separate from original source-byte authenticity, which remains NOT_VERIFIED where those bytes are absent. |

Coverage also reconciles the existing acquisition `rows_received` declarations:

| Declaration | Required comparison |
| --- | --- |
| Each identity record's `counts.rows_received` | Matches the measured retained raw occurrences for that scoped identity/interval and the independently evidenced expected membership count. |
| Dataset `metadata.counts.rows_received` | Matches the measured total scoped raw corpus and the record-level acquisition counts; an aggregate match cannot hide compensating record-level errors. |

Counts use strict integers. Missing/null/unsupported count values or containers
are NOT_VERIFIED; a known integer mismatch or negative count is INCOMPLETE.
Known independent membership or count contradictions take precedence over
unknown evidence: INCOMPLETE wins over NOT_VERIFIED. No early absence of one
proof may hide a separately established contradiction.

The helper does not fill, infer or repair these declarations from candidate
length, accepted rows, quarantine counts or a clean projection. It does not
infer pages, instrument completeness or missing source membership. In E19,
received raw rows remain four while the candidate has three; after quarantine,
acquisition counts still refer to all received variants. Regressions vary
record/aggregate declarations independently of retained rows, keys and values,
including compensating record errors, unknown counts and contradiction priority.

`accepted_rows` measures verified eligible candidate rows; `quarantined_rows`
measures candidate rows with established quarantine reasons. These counts do
not silently include all unselected raw occurrences. Missing durable quarantine
or validation artifacts leave `quarantine_ref`/`evidence_ref` null. Detailed
in-memory findings are not falsely presented as saved artifacts. Raw failure,
local eligibility, acquisition coverage and source authenticity are independent.
Each F0-shaped quality check has `reason=null` on PASS and a nonblank reason on
FAIL/NOT_VERIFIED; this does not manufacture an evidence artifact reference.

## Fixtures, regression oracles and diagnostics

The logical archive source is `AUDIT_019fdd9e_20260905`, SHA-256
`d35bc3b49f27e827855077730e4a8995fa6a7c4125ad37546543263aa4056ee3`.
The manifest records decoding, selectors, byte ranges and fragment digests;
byte ranges are zero-based UTF-8 and end-exclusive after decoding.

| Evidence | Retained locator chain |
| --- | --- |
| Ten OHLC samples | JSONL line 1370, `JSON.parse($.payload.output[2].text).output`, bytes `[2459,4909)`, then `$.samples[i]`. |
| ASSB corroboration | Line 1472, `JSON.parse($.payload.output[1].text).output`, bytes `[13,420)`, `$.candles.data[0]`. |
| Both ABIO versions | Same line/decoding, bytes `[433,1086)`, `$.candles.data[0]` and `[1]`. |
| HTTP metadata | Line 1482, `JSON.parse($.payload.output[1].text).output`, bytes `[0,1868)`, ASSB/ABIO entries. |

Fragment digests prove retained parsed text, not original HTTP bytes or numeric
lexemes. The ten samples are ASSB 2013-12-05; BISVP 2013-08-22; BRZL 2013-05-06;
CHGZ 2012-07-17/2014-02-12; CNTLP 2012-08-10/2013-08-06/2013-12-16; DZRD
2012-08-14/2012-10-08. ASSB corroboration is not an eleventh sample. Both ABIO
versions are separate #56 evidence.

Repository JSON integrity digests use explicitly labeled `UTF8_TEXT_LF`, so
Git's Windows checkout line endings do not change the fixture identity. Archive
bytes and decoded fragment digests retain their separately stated bases; this
does not normalize source numeric values or supply missing HTTP bytes.

The ledger preserves the historical total of 79 OHLC observations, ten available
parsed samples and 69 unenumerated observations. It does not invent original
observation IDs, exported CSV ordinals, acquisition times, numeric lexemes or
full scope from display ticker. Archive event time is not acquisition time;
79 is not expected membership for the full source dataset. Full archive,
private paths and credentials are not copied into repository fixtures.

Regression scenarios cover E19 positive/negative/missing authority; bindings of
later-quarantined rows; each numeric field and unsupported representation;
timestamp validity; ASSB and both isolated/paired ABIO observations; raw versus
candidate counts; duplicate/revision/conflict classification and unresolved
selection; scoped sort/interval/board isolation; preserved inputs; unknown and
missing coverage; partial/gap controls; and logging failure/redaction.

The accepted R3 duplicate/revision matrix is explicit:

| Input | Classification and retained evidence | Effective-sequence outcome |
| --- | --- | --- |
| Identical full key/all raw values, two raw locators | `DUPLICATE_EXACT`; preserve both occurrences and locators. | Entire affected eligibility group blocked; no representative. |
| Same key/OHLC, different end | `SOURCE_KEY_CONFLICT`, `END_VARIANT`; end is not part of the key; quarantine both. | Both excluded; no first/last winner. |
| Same key, different OHLC, no proven revision | `SOURCE_KEY_CONFLICT`, `OHLC_VARIANT`; quarantine both. | Both excluded; no first/last winner. |
| Independently evidenced old/new versions | `EVIDENCED_REVISION`, `REVISION_SELECTION_NOT_APPLIED`; preserve both; revision is not itself an invalid-price finding. | Selection remains blocked; no automatic revision application. |
| Different variants with only later acquisition time or metadata claim | `SOURCE_KEY_CONFLICT`, `REVISION_NOT_PROVEN`; quarantine both. | Both excluded; chronology/metadata cannot select a winner. |
| Same begin, distinct full scoped identities/boards | Two distinct keys, one variant per key; no false conflict. | Separate eligible groups when other gates pass; no identity mixing. |
| Candidate selects one variant; conflicting raw variant is unselected | `SOURCE_KEY_CONFLICT`, `UNSELECTED_RAW_VARIANT_CONFLICT`; two raw rows quarantined, one candidate row excluded. | Selected occurrence excluded before projection. |

Every row checks reasons, counts, complete locators, unchanged raw content and
eligibility under forward and reversed input order. No test accepts arbitrary
first/last selection. The source-evidence boundary remains independent from
candidate metadata.

Permutation tests compare full keys/values and within-group order after rebinding
ordinals, rather than demanding equal ordinal lists across permutations.

Each fresh synthetic corpus, producer selection or ordered candidate owns an
explicit new fixture snapshot identity. The positive fixture authority is
captured before assessment; it is not reconstructed from the assessor's result.
Permutation snapshots therefore retain their own independently frozen selection,
frame/binding fingerprints and original ordinal bindings. A negative mutation
keeps that authority fixed, even when its damaged metadata carries newly
calculated hashes. Reusing an existing snapshot ID for changed content or
refreshing expected selection from the negative input cannot be evidence of
lineage correctness. Mutating order/bindings within an existing immutable
snapshot fails even when the negative's own hashes are recomputed. A new snapshot
ID alone is insufficient without its independently captured authority; missing
lineage remains NOT_VERIFIED. E19 remains a separate required authority test.
Source-backed snapshot allocation remains a later hookup.

The synthetic CONTINUE oracle uses closes `100..114`, competing `115/180`, then
`110`: 18 raw/candidate rows, two quarantined and 16 effective rows. Direct
TA-Lib and independent Wilder give 14 initial NaNs, then `100` and
`76.47058823529412`; shape/index/NaN mask are checked separately from finite
values (`rtol=atol=1e-12`). The expected arithmetic does not use the helper.
This does not satisfy the source-backed SBER/MOEX reference acceptance.

An explicit optional existing F-LOG context carries start/progress/finish/failure,
standard counts, timing, outcome and the full shared error shape. There is no
logger initialization at import and no new quality addition in `event.fields`.
Raw and candidate count populations use separate scoped phases/events. Detailed
keys/reasons remain in assessment/evidence; diagnostics do not dump raw rows.
Canaries and primary-error preservation are checked. Logger failure cannot
replace a pre-existing assessment exception or create a false successful result.

## Verification, acceptance and remaining work

The following are intended commands, **not execution results in this document**.
The caller resolves `$B2_PYTHON` to the verified immutable dev interpreter;
no installation or foreign-environment writes are allowed. F3 owns fresh UUID
temp/cache/evidence roots in this worktree and guards collection/runtime/children.

```powershell
& $B2_PYTHON -I -B -X utf8 tools/offline_tests.py --lane f3 --collect-only
& $B2_PYTHON -I -B -X utf8 tools/offline_tests.py --lane f3
& $B2_PYTHON -I -B -X utf8 tools/offline_tests.py --notebook-check --ref <exact-clean-HEAD>
git diff --check
```

Specification RED for a new helper is distinguished from a production bug
reproduction; missing-module collection alone is not a substantive regression
proof. Runtime results, exact commands/counts/exits, guard reports and exact-HEAD
CI belong in the completed handoff. Until recorded, they are NOT VERIFIED.
No live MOEX, production writer or publication run is needed for these checks.

The PR #108 review repairs retain separate RED/GREEN evidence: malformed captured
candidate schemas (duplicate names or missing raw fields), F0 check reason
nullability, and required-context uncertainty/contradiction cases. Their RED
reference is the independently reviewed implementation; revised clean-HEAD
commands, counts, exits and evidence roots belong in the updated handoff. Earlier
successful runs do not validate the repaired commit.

The following original issue acceptance criteria are copied without shortening.
In-slice entries describe the intended local contribution, not full completion.

| Original issue AC | B2-01 contribution/check | Remaining owner/package |
| --- | --- | --- |
| #16: Вход валидируется по ticker/interval/begin и сортируется либо отклоняется с явной ошибкой до расчёта. | Partial: helper key/interval/order/lineage checks. | Pre-TA-Lib integration: B2-02. |
| #16: Результаты по ключу не зависят от начального порядка строк и не смешивают тикеры. | Partial: fixture permutations with ordinal rebinding. | Calculator integration: B2-02. |
| #16: Повторные ключи и смешанные интервалы выявляются, а не просто сортируются и принимаются. | Helper duplicate/conflict/interval checks. | Producer/runtime integration: A/B2. |
| #16: RSI(14) сравнен с TA-Lib и независимой формулой Wilder на SBER/MOEX, с проверкой warm-up и индекса. | Not completed; synthetic policy oracle only. | Source-backed reference: B2-03. |
| #55: Исходные данные сохранены неизменными с признаком качества и сведениями о происхождении. | Partial: candidate immutability, retained refs and parsed fixtures. | Original bytes/durable retention: A/B2. |
| #55: Определена политика исключения, карантина или трактовки источникового null для расчётов, без выдуманных цен. | Accepted local ASSB/quarantine/CONTINUE subset, no invented prices. | Runtime integration: B2-02. |
| #55: Все 79 наблюдений разобраны и учтены; регрессионный fixture ASSB не проходит как обычная валидная свеча. | Partial: ASSB plus ten parsed/69 unenumerated ledger. | Full inventory: B2-03/A/R1. |
| #56: Утверждена политика обработки многолетних и конфликтных строк источника. | Accepted local ABIO singleton exclusion/conflict subset. | General temporal/export semantics: F0/B2. |
| #56: Обе исходные строки ABIO сохраняются со сведениями о происхождении, но не маскируются под две обычные дневные свечи. | Partial: both parsed variants, individual reasons and provenance. | Original bytes/runtime retention: A/B2. |
| #56: Проверки учитывают частичный end внутри дня и неторговые периоды, не создавая ложных срабатываний на каждую короткую свечу. | Local partial/gap controls. | Runtime acceptance: B2-02. |

All three issues remain open. The PR uses `Refs #16, #55, #56, #63, #78`.
The #63 contribution is the internal subset and explicit unknowns; final public
dictionary/export/NaN conformance is not completed. The #78 contribution is
scoped helper telemetry: common pipeline delivery, final nonzero/upload-stop,
capacity, child supervision, bundles, overhead and staging remain F/D/R1 work.

| Dependency/hookup | Remaining work |
| --- | --- |
| A raw capture | Preserve real raw variants, including responses A2 rejects before returning; B2 does not weaken A2 validation. |
| A/F0/B2 source authority | Production immutable selection, expected membership and reviewed float normalization. |
| F0/B2/B3/R export contract | Quarantined public-row/key representation and CSV/XLSX/Google NaN semantics. |
| B2-02/B4/D | Calculator admission, paired notebook integration and error propagation. |
| B2-03/A/R1 | Source-backed SBER/MOEX reference and all 79 original observations. |
| A8/B3/D4/R | Canonical windows, output parity, release/publication/previous acceptance. |

Fixture work does not wait circularly on A8/B3/D4. These later slices are not
activated by B2-01. Production normalization/authority and missing source bytes
remain material limitations; `tech` retains its existing behavior until its
separately approved integration.

READY_FOR_INDEPENDENT_CODE_REVIEW requires the exact six-path diff, approved base/receipts,
substantive guarded offline evidence on the clean actual HEAD, input preservation,
honest archive ledger, self-review and one PR. It is not full issue acceptance.
BLOCKED means a required scoped prerequisite/evidence is absent; NEEDS_DECISION
means a new blocking semantic choice, not reopening the accepted CONTINUE policy.

Independent source review identified and corrected candidate-key duplication
despite valid provenance, overbroad revision inference from one pair, unsafe
reference path components and matching absent binding identities. Their narrow
regressions remain part of the in-slice verification. This review is not an
independent code-acceptance decision. The observed immutable development
environment passed a read-only comparison of all 16,692 sealed-manifest files;
that environment check is not a behavioral test or exact-HEAD CI evidence.
Focused/full F3 execution and final clean-HEAD results must be read from the
completed handoff; they are not implied by the intended commands above.

The final handoff records REPO/PACKAGE/CURRENT_SLICE, PR/BRANCH/BASE_SHA/HEAD_SHA,
approved R2+R3 and both receipt IDs, actual changed paths, tests/counts/exits,
exact-HEAD CI or NOT VERIFIED, notebook inventory, redacted log evidence,
schema impact, this AC matrix, open hookups, permissions and self-review.
`NO_AUTO_MERGE=true`. Rollback is a separately reviewed revert of the B2-01
commit; this slice changes no dataset or remote target. Stop after independent
review handoff: no automatic merge, issue closure, next slice or production.

## B2-02: offline computation under Full Completion R2

The owner-direct grant `B2-P1P2-OWNER-DIRECT-20261017-01` releases only the
ten P1/P2 paths in the accepted Full Completion R2 plan. This P1 changes only
`tech.py`, `tech.ipynb`, this document, `tests/shared/b2/test_calculator.py` and
`tests/fixtures/b2/calculator_cases.json`. The existing assessment implementation,
quality tests, E19/cases and archival assets are read-only. The tech pair is one
ownership zone. P0/B1, P3 normalization and P4 runtime integration are excluded.

```python
calculate_candles(frame, *, spec, metadata, retained_evidence,
                  logging_context=None) -> computation
```

`frame` is the candidate population; retained evidence includes selected and
unselected raw variants. The function snapshots its inputs and calls
`assess_candles` itself. There is no public mode, skip-assessment flag or fallback
to the legacy file calculator.

Before the first arithmetic call, the whole request must have independently
verified bindings, COMPLETE local coverage, PASS request/group eligibility,
nonempty required groups and a valid identity/interval/ordinal projection.
All six numeric computational columns are prepared for every group before any
group is calculated. Missing authority, unsupported normalization, unresolved
counts/membership, a blocked/empty required group or unrepresentable binary64
input blocks the whole request with zero TA-Lib calls. The overall quality
result is not the admission switch: full F0 remains NOT_VERIFIED, and known raw
quarantine FAIL can coexist with PASS effective eligibility.

The computational copy preserves exact integers when representable, checks
overflow/precision loss and keeps negative zero. It does not mutate source
values, impose positive/integer-volume rules or provide a production float
normalization profile. A conversion failure is a computational blocker, not a
new raw-data corruption verdict.

The single private `_calculate_frame(..., group_ordinals=...)` kernel retains
the existing 66 TA-Lib calls, parameters, 80 indicator names/order and RSI14
implementation. The legacy wrapper supplies its existing display-ticker groups.
The admitted path supplies one explicitly scoped, chronological dense-index
group at a time and returns original candidate ordinals. The kernel never
reconstructs admitted identity from ticker. Native dtype and NaN masks remain
per-column; HT_TRENDMODE retains its integer/zero warm-up semantics. H89 is the
legacy raw-nine plus 80-column schema, not a row-trimming policy.

| Internal result | Meaning |
| --- | --- |
| `status` | CALCULATED_OFFLINE or BLOCKED; never successful empty/partial required computation. |
| `assessment` | Unmodified raw, quality, coverage, eligibility and assessment diagnostics. |
| `candidate_rows` | Original-value copy, including column order, extra columns and original index labels. |
| `candidate_row_map` | Original ordinal/index, verified key/ref/identity when available, disposition and reasons. Unverified bindings are not invented. |
| `effective_groups` | Identity/interval, ordered original ordinals, sorted original-value rows and separate 80-column indicator frames on matching dense indices. Empty when blocked. |
| `quarantine` | Verified candidate quarantine ordinals and retained raw findings/groups. Missing effective ordinals alone never imply quarantine. |
| `blockers`, `diagnostics` | Internal reasons and safe diagnostic outcome; no raw payload logging. |
| `production_admission` | NOT_ENABLED. |

The function does not insert public NaN rows, remove public keys or serialize a
candidate. `calculator(filename)` and `tech.main` retain legacy behavior and are
UNVERIFIED admission paths. P4 will require an accepted producer handoff and
separate B4/D error/release gates. In particular, current producer writes occur
before tech; metadata alone cannot establish preservation of the previous
release. Quarantine representation in CSV/XLSX remains an owner decision.

P1 regressions retain the frozen legacy-order counterexample and compare the new
path by full key/value across independently captured permutation snapshots.
They cover all-group admission before TA calls, raw/candidate preservation,
extra columns and duplicate/custom indices, the exact CONTINUE 18-to-16 oracle,
all native parameter/output bindings, dtype/masks, conversion and diagnostic
faults. A 320-bar fixture exercises long lookbacks; it is not a universal minimum
history/admission rule or evidence of historical completeness.

The P1 legacy parity fixture retains the original campaign-anchor tech Git
blob as compressed bytes in `calculator_cases.json`. A literal length, SHA256
and Git blob check precedes import, with strict decoding and no runtime Git,
network or candidate-source fallback. This makes the same historical reference
available in shallow CI. It is a code reference, not candle/source evidence,
and does not implement or replace the separately owned P0/B1 companion.

P0 is a separate B1/F3 reference-preparation companion. The existing B1 source
guard rejects substantial tech changes before running its accepted-reference
comparison. Until P0 is independently accepted and included in the actual base,
full P1 GREEN and READY_FOR_INDEPENDENT_CODE_REVIEW are not claimed. Its tests,
pins, tolerances, runner and guard are not weakened in this PR. Focused results
and this prerequisite must both be included in the handoff.

The milestones remain separate: B2_COMPUTE_OFFLINE_ACCEPTED requires independent
P1 acceptance; B2_REFERENCE_EVIDENCE_ACCEPTED or PARTIAL reports only its actual
scope; B2_RUNTIME_INTEGRATION_ACCEPTED requires the real caller hookup;
B2_FULL_AC_ACCEPTED requires all original #16/#55/#56 evidence. None is implied
by a successful local calculation. P1/P2 grant no source acquisition,
production/live, merge, issue closure, P3 or P4 permission.
