# C2-03C-01 — Offline dividend release decision contract

This slice models an owner-selected target policy using synthetic observations.
It adds no runtime validator, store, adapter, sidecar, release gate or production
API. Candidate quality is evaluated before writers; it does not depend on a
future publication result. A simulated PASS is not official source evidence,
production readiness or permission to publish a dividend dataset.

## Accepted policy and provenance

The owner accepted PLAN R2 and explicitly granted implementation/exclusive
ownership of exactly these three new paths, including the two F3 paths:

```text
docs/c2_03_release_policy.md
tests/fixtures/c2_quality/release_policy_cases_v1.json
tests/shared/c2_quality/test_release_policy_contract.py
```

The dividend pair remains a read-only dependency owned by C2-03B. No pair
handoff is required or granted. Coordination, F0/logger contracts, existing
fixtures/tests, guards, configuration, CI and dependencies are outside scope.

| Owner-selected target rule | Meaning |
|---|---|
| Mandatory FAIL | BLOCK_NEW_RELEASE; evaluate previous independently. |
| Mandatory NOT_VERIFIED | BLOCK_NEW_RELEASE; absence of evidence is not PASS. |
| Previous eligibility | Separately verified content/readback, quality and freshness. |
| Precision | Exact raw bytes/tokens; measure/disclose finite losses without a lossless claim. |
| Mandatory source basis | Date/amount/currency semantics, provenance and declared coverage. |
| Future raw store | Private immutable evidence; mandatory capture failure blocks release. |

Original decision receipts identify session
`01a13747-e9ed-7382-90a9-c94a37dc27c2`, turn
`01a146a9-77bd-7a60-953b-1d83ae100a26`. Receipt A selected
`mandatory_fail`, `not_verified`, `previous_contract`; receipt B selected
`precision_policy`, `source_mandatory`, `raw_store_policy`.

| Receipt | Safe original identity | UTC / SHA-256 of exact UTF-8 output, no final newline |
|---|---|---|
| A | `call_5123bcaa5e2b47288489d6efaf1b22c1`; `fco_01a146be-dcb9-7f92-8b33-21f94216f1be` | `2026-10-16T22:04:35.897Z`; `97f4c0944a511fb9295b236c990ef6e31fbedddf83ae6f4aba0208edb8194e25` |
| B | `call_3f83e04d1ab54ac7b6abb81e11e77123`; `fco_01a146c0-8598-78a3-a339-b1c8a1778f2b` | `2026-10-16T22:06:24.664Z`; `72a6890a2181df9ab998052e1d650c379d06e90e7b9b3cab91f0810ca8482fa4` |

The later implementation grant confirms these six choices. These receipts
establish policy provenance, not an official MOEX contract. The existing
[mandatory capacity decision](https://github.com/foykes/moex-dataset/issues/76#issuecomment-5981425474)
is retained: no truncation, split release or optional downgrade after mandatory
Excel/Google overflow. Candle quarantine continuation does not govern dividends.

## Reviewed base and frozen characterization

Implementation starts from main `6b7e97148227400841b90ad614d50d94f139f665`,
tree `3d891d4980fd22f57012ed76c7cbe35ed545fc32`. Accepted C2-03B merge
`59f2f57c8972fd4dd40f1843a24503aa906f6300` is an ancestor. Subsequent
Linux-admission work does not establish production containment or source quality.
Exact candidate/tested identities belong in the implementation handoff; an old
merge CI result is not a check of this slice.

The review-fix preflight observed main
`e8fb38c7151fe53ef58d8baaaaaf14ef6be938f2`: PR #108 adds six disjoint B2 paths.
The source and characterization dependencies are unchanged. The existing
feature branch retains its original base; compatible drift does not require a
rebase. Actual main, ancestry and path compatibility are rechecked before
integration.

| Source | Frozen blob |
|---|---|
| Active `dividends.py` | `e66e1effa4d113518d1ef99180176562f42e65d8` |
| Active `dividends.ipynb` | `affcc5efb1aad3b3b18fad3ba1c4c3c223781579` |
| Archived V1 Python | `7e63237528dbf43d2fe9dd3fba7ac277f477dc35` |
| Archived V1 notebook | `2b297e85944ad7445889ec85c5ce4931271adc3f` |

Accepted [V1](https://github.com/foykes/moex-dataset/pull/99#issuecomment-6187824783)
retains 41 cases / 44 nodes / 172 finite comparisons;
[V2](https://github.com/foykes/moex-dataset/pull/105#issuecomment-6210331544)
retains 49 cases / 53 nodes / 162 finite comparisons. The Windows profile stays
CPython 3.14.8, pandas 2.3.3, numpy 2.3.4, openpyxl 3.1.5, pytest 8.4.2.
Both source fingerprints remain checked before the corresponding import.
No old input/oracle changes, skipped characterization or archive fallback are
allowed. This three-file slice needs no V3 recharacterization.

Known findings remain: binary differences for 17.35; loss for
`0.10000000000000001`; underflow of `1e-400`; XLSX loss of one for
`9007199254740993`. Historical null-induced inference loss and U+0001 serializer
failure remain V1 evidence; V2 rejects those inputs before writers. See
[the full characterization report](c2_03_quality.md). GREEN reproduces these
observations; it does not repair their source values or prove losslessness.

## Source evidence and uncertainty

Evidence labels are VERIFIED, HISTORICAL_ONLY, SYNTHETIC_ONLY, NOT_VERIFIED and
CONFLICTED. The last requires incompatible evidence; missing official evidence
alone is NOT_VERIFIED. Code behavior and official source semantics are distinct.

| Topic | Observed basis / evidence | Remaining boundary and policy effect |
|---|---|---|
| `registryclosedate` | [Mapping and guard](https://github.com/foykes/moex-dataset/blob/6b7e97148227400841b90ad614d50d94f139f665/dividends.py#L275): VERIFIED implementation. | Official field meaning is NOT_VERIFIED; need definition, applicability/version and linked source record. |
| MOEX 17.35 RUB / 2111 | [#57](https://github.com/foykes/moex-dataset/issues/57): HISTORICAL_ONLY public snapshot; fixture is reconstruction. | Do not correct the year. Original/archived response or authoritative event provenance is missing. |
| Amount units/gross-net/sign | Finite zero/tiny/negative are preserved; unsupported/nonfinite are rejected. Synthetic tests verify the local profile. | Official units and economic semantics are NOT_VERIFIED; local acceptance is not a source-backed PASS. |
| `currencyid` | Exact nonblank string and selected C0 bans are VERIFIED local rules. No whitelist/normalization. | Official vocabulary/history and empty/missing meaning are NOT_VERIFIED. Unknown codes stay unchanged. |
| Precision | [V1/V2](c2_03_quality.md) distinguish raw token through decoder/frame/CSV/XLSX XML/readback. | SYNTHETIC_ONLY inputs do not establish production fidelity. Original bytes/tokens and bindings are needed. |
| Required `dividends` block | Parser requires it; [#62](https://github.com/foykes/moex-dataset/issues/62) records historical HTTP 200 without it for SBER/MOEX. | Current endpoint/VPS is NOT_VERIFIED. Missing block is failure, never valid empty. |
| Event identity | Identical rows, order and multiple same-date payouts remain intact; [#75](https://github.com/foykes/moex-dataset/issues/75) owns natural identity. | A row locator is not an event key. Do not infer identity from date/value/currency or deduplicate. |
| Declared completeness | Scope comes from `moex_full.xlsx`; [#27](https://github.com/foykes/moex-dataset/issues/27) owns repeat-run isolation. | Official historical universe/bounds are NOT_VERIFIED. Require independent expected membership and source/run bindings. |

Therefore the selected mandatory NOT_VERIFIED-to-BLOCK policy currently cannot
justify a source-backed new dividend release. Offline contract work remains
independent. No live MOEX/VPS probe is part of this slice.

## Stages and status boundaries

[F0 structures and helpers](dataset_contracts.md) remain PROPOSED.
[F-LOG RETURNED/COMPLETED](logging.md) is not release verification.

| Stage | Inputs / result |
|---|---|
| Candidate quality | Collection, semantics, provenance, coverage, isolation, canonical content/bindings and nonempty rule. PASS permits preflight only. |
| Required preflight | Declared mandatory exports, target/capability bindings and applicable capacity, before any current-target mutation. |
| Disposable preparation | Actual writer/preparation plus independently verified artifacts; WRITTEN is not VERIFIED. |
| Publication/readback | Actual mutation, ACK and measured target generation; ACK is not readback. |
| Terminal aggregation | All required stages, target facts and retained errors; no mandatory failure becomes SUCCESS. |

SOURCE_FAILURE and BLOCKED_KEEP_PREVIOUS are descriptive labels, not new deployed
enum values. Logging/supervision are release obligations, not source checks.

The test-only schema uses F0 spellings for collection, coverage, aggregate
quality, preflight, artifact, preparation, publication and readback. Individual
checks have only PASS/FAIL/NOT_VERIFIED; NOT_RUN and NEEDS_DECISION belong to
aggregate observations. Artifact has no invented NOT_VERIFIED enum: a nominal
VERIFIED without proof projects to WRITTEN with an unsatisfied verification gate.
Unsupported readback claims project to UNKNOWN, preserving nominal observations.
Failed observations remain failures, rather than being weakened to uncertainty.

## Closed fixture contract and evaluator

The standard JSON envelope identifies `mds.c2.release_policy_cases`, version 1,
policy `C2-03C-01/BLOCK_V1`, origin SYNTHETIC_ONLY, mandatory checks, closed export
profiles, `selection_encoding=MDS_SYNTHETIC_JSON_V1`, independently authored
`retained_selections` and explicit cases. Each
case has an ID, scenario mode, `retained_selection_id`, complete input and
independently authored `expected: {contract_error, result}`. Contract-error
cases require `result=null`; evaluated cases have `contract_error=null`.
Unknown/missing keys, duplicate JSON
keys/IDs, NaN/Infinity constants, invalid types/enums and dangling non-null refs
are contract errors. A boolean is not an integer. Null counts are not zero.

Every current observation/evidence record carries the caller's exact `run_id`,
`snapshot_id`, `dataset_id`, `as_of`; canonical UTC `as_of` is separate from
acquisition/evaluation time. The evaluator never creates identities, reads the
clock or merges contexts. Previous has a current `captured_for` context and its
own previous release/snapshot/as_of identity. Context drift is a contract error.

The mandatory candidate inventory is fixed and nonempty:

```text
IDENTITY_SCHEMA       LOCAL_PROFILE       SOURCE_SEMANTICS
RAW_PROVENANCE        COVERAGE            RUN_ISOLATION
CANONICAL_CONTENT     CANONICAL_BINDINGS  REQUIRED_NONEMPTY
```

SOURCE_SEMANTICS requires three separately proved `basis_refs` obligations:
DATE_SEMANTICS, AMOUNT_SEMANTICS and CURRENCY_SEMANTICS. One cannot stand for all
three. Each evidence record is bound to its specific check or export-stage
obligation, so a valid reference to unrelated evidence cannot prove that check.
Evidence also carries nullable `artifact_id` and `target_id`: candidate-wide
proofs have both null; export-stage proofs match that exact artifact and target.
Previous proofs match the target and logical export, with null artifact because
the previous artifact identity is separate. Cross-artifact/target proof reuse
cannot justify a verified result.
An executed assessment requires exactly one record of each check. Individual
records contain `check_id`, `status`, nullable `evidence_ref` and nullable closed
`reason`. PASS requires null reason; FAIL/NOT_VERIFIED require a safe reason.
Null evidence is valid uncertainty but cannot justify effective PASS. Missing a
required field/check is a ContractError, not an ordinary quality refusal.

Inputs separately retain collection/instrument scope, explicit coverage
`expected_members`/`received_members` and independent `membership_evidence_ref`.
Membership proof is distinct from the ordinary coverage/source status evidence;
matching received and expected lists without independent proof is insufficient.
Quality retains `rows_checked`, `accepted_rows`, `quarantined_rows` and nominal
assessment/decision facts. Inputs also retain evidence, canonical ordered schema,
literal five-cell rows and row bindings, required export records,
logging/supervision and full safe
errors with an explicit primary reference. Expected decisions are not inputs to
the reducer. This is a pure test-local evaluator, not an importable production
contract or a replacement implementation of F0 helpers.

### Independent retained selection

Each retained selection declares its context and candidate release identity,
ordered source locators and literal rows in the five public column positions.
The three selections contain zero, one and two rows. Cells are standard JSON
primitives, with finite numbers and UTF-8-encodable strings, not values recovered
from a digest. The
manifest loader freezes this independently authored basis before any scenario
input is copied or mutated. The case-level `retained_selection_id` remains
outside mutable `input`; the evaluator receives the selected frozen basis
separately. The immutable registry uses a read-only mapping of tuples. It must
not reconstruct trusted rows or expected lineage from the candidate being checked.

Canonical checks compare actual candidate rows and both binding arrays with
that retained basis. They also recompute deterministic, domain-tagged JSON-v1
SHA-256 digests for each row, ordered content and ordered bindings, using the
following encoding. For each digest, serialize `[tag, payload]` with Python
`json.dumps(..., ensure_ascii=False, allow_nan=False, separators=(',', ':'))`,
encode the result as UTF-8 and take its SHA-256 hexadecimal digest.

| Digest | Tag | Payload |
|---|---|---|
| Row | `mds.c2.synthetic-row/v1` | `[COLUMNS, row]` |
| Ordered content | `mds.c2.synthetic-content/v1` | `[COLUMNS, rows]` |
| Ordered bindings | `mds.c2.synthetic-bindings/v1` | `[[response_id, row_ordinal, row_fingerprint], ...]` |

`COLUMNS` is the fixed ordered five-column list. Declared digest strings alone do not
prove content. A common-mode mutation of both caller binding arrays, changed
row content, forged digest or stale digest cannot justify PASS. Context and
candidate identity must also match the selection. Identical rows keep distinct
source ordinals and positions; this comparison performs no deduplication.

The original placeholder digests made from repeated `1`, `2` and `4` characters
are replaced with independently calculated synthetic row/content/binding
digests. The retained rows, digest inputs and expected decisions are authored
independently of the evaluator; no golden generator updates its outputs. These
literal synthetic rows neither add a production column nor establish real MOEX
lineage, official semantics or capture of raw source evidence.

`DIVIDEND_V1` requires CSV and XLSX. `SYNTHETIC_GOOGLE_CAPACITY_V1` adds an injected
hypothetical Google obligation solely to test capacity policy. Required export
membership must exactly match the profile: no omission, optional downgrade or
duplicate/conflicting binding. No dividend Google target is declared or created.
Export IDs are `CSV`, `XLSX`, and injected hypothetical-only `GOOGLE`. CSV/XLSX
may share the `PUBLIC` target alias; each artifact/readback remains separate,
and mixed generations must still be detected within that common target.
CSV has no spreadsheet row ceiling; only applicable evidenced resource limits
can block it. The existing candle Google view is unchanged.

HYPOTHETICAL cases explicitly assume sufficient evidence to exercise branches.
EVIDENCE_BOUNDARY cases do not: historical/synthetic/missing/conflicted evidence
cannot prove an official source requirement. The assumption is separate from
actual evidence status. Every successful evaluation retains:

```text
evaluation_scope: SYNTHETIC_ONLY
official_source_status: NOT_VERIFIED
```

The result copies caller context and separates candidate quality/admission from
release status, stage/per-target outcomes, failed required exports, retained
errors/primary ID and exit status. `release.policy_violations` is always present,
including an empty list, with closed MUTATION_BEFORE_ADMISSION,
MUTATION_BEFORE_PREFLIGHT and MUTATION_BEFORE_ARTIFACT_VERIFICATION codes.
Contract errors return no candidate decision.
The test-only `per_target_outcomes` is a limited explicit projection for these
cases, not the full F0 per-target shape or a deployed interface.
Expected outputs are fixed independently: no automatic golden update or expected
generation from the reducer. The fixture and executable validator define exact
field spellings; this prose does not introduce a second runtime schema.
The original 52 scenario IDs and 106 test nodes are retained. Seven additional
fixture scenarios bring the authored scenario inventory to 59. Targeted tests
cover the review counterexamples, row/content/digest drift and retry history,
alongside obligation-specific proof, count consistency, artifact contradictions,
independent previous quality/freshness and mixed generations. Exact collected
and executed inventories belong in the clean-HEAD handoff. This inventory is
separate from the 49 V2 source characterization cases.

## Reduction rules and counterexamples

Within an executed quality assessment: FAIL precedes explicit NEEDS_DECISION,
which precedes NOT_VERIFIED; PASS requires every mandatory check and its proof.
Nominal PASS alone cannot override missing evidence. An unexecuted assessment
retains NOT_RUN, empty checks and null measured counts. Collection failure,
incomplete/unavailable source, valid-empty/nonempty refusal or non-PASS quality
blocks admission. Successful COMPLETE plus effective PASS allows preflight.
VALID_EMPTY denotes successful empty collection; coverage remains independent.

| Scenario | Required outcome |
|---|---|
| Quality PASS, downstream NOT_RUN, nonterminal | ALLOW_PREFLIGHT; CANDIDATE; exit null, never CURRENT_VERIFIED. |
| Nominal PASS without mandatory source proof | Effective NOT_VERIFIED; BLOCK; terminal exit 1. |
| FAIL and NOT_VERIFIED | FAIL, retaining all observations. |
| Missing context/check/export, PASS with reason | ContractError; no fabricated quality result. |
| Valid empty versus source/missing-block failure | Separate collection facts; neither permits the required nonempty release. |
| 2111/zero/tiny/negative/unknown currency | Preserve values; source uncertainty is not a correction rule. |
| Same-date/identical rows or different currencies | Preserve ordered row bindings and duplicates. |
| Both mutable binding arrays agree after unapproved lineage drift | CANONICAL_BINDINGS FAIL against the independent frozen selection. |
| Changed row content or forged/stale canonical fingerprint | No canonical PASS from a string-shaped digest; compare retained rows and recomputed digests. |
| Nonfinal retry history followed by fully proved successful stages | Retain retry history and per-export references; terminal primary null; CURRENT_VERIFIED remains possible. |
| Unresolved final mandatory failure | BLOCKED / 1; retained final failure and terminal primary ID. |
| Previous matching readback, quality UNVERIFIED, freshness UNKNOWN | PRESERVED_VERIFIED content, unchanged quality/freshness; not eligible verified fallback. |
| Mutation timeout, later conclusive previous readback | BLOCKED / 1; previous content known; timeout and primary error retained. |
| Conclusively different required artifact/target identities | PARTIAL_REMOTE / 1, even without mutation in this attempt; compare release/snapshot/content identity, not labels alone. |
| Possible mutation, unidentified required content | REMOTE_UNKNOWN / 1. |
| Proven absent previous | ABSENT with evidence; no invented published release, current manifest or preservation. |
| Synthetic all-PASS | Evaluator-only result; official source still NOT_VERIFIED. |

With `terminal=false`, a started mutation gives PUBLISHING with exit null;
otherwise a pending candidate is CANDIDATE. At terminal completion unresolved
mutated content takes precedence, then known mixed generations independently of
whether this attempt mutated anything, then mandatory
blockers/missing stages/final failed attempts. CURRENT_VERIFIED/0 requires every
mandatory gate, verified artifact and candidate readback at every required target.
Known candidate content after a final failed mandatory attempt does not erase
failure. A recovered nonfinal retry is diagnostic history, not a failed terminal
mandatory stage.
Mutation contrary to a blocked candidate remains a recorded policy violation.
Aggregate artifact status remains NOT_RUN if any required artifact was not run,
unless a demonstrated FAILED artifact takes precedence. Per-export observations
are retained. Missing verification proof projects nominal VERIFIED to WRITTEN;
known canonical-value/binding contradiction with proof projects to FAILED.
Independently proved actual readback remains VERIFIED even if the artifact gate
failed: remote content facts are not erased to hide a policy violation.

Matching previous bytes/content establishes preservation only. It does not
verify previous data quality, refresh its cutoff or make it fresh. NOT_TOUCHED
cannot follow a mutating attempt. Later conclusive readback resolves content
uncertainty without deleting TIMEOUT or upgrading prior quality/freshness.
Previous content proof, `quality_evidence_ref` and `freshness_evidence_ref` are
separate. Claimed VERIFIED quality without its own proof becomes UNVERIFIED or
UNKNOWN; claimed CURRENT freshness without its proof becomes UNKNOWN. Only
proved preserved content plus verified quality and CURRENT freshness permits
`fallback_eligible=true`.

## Integration boundaries, evidence handling and acceptance

C2-03B remains response-local: rejected response adds zero rows before append,
but earlier responses/global state remain. Its exception stops that invocation's
dividend writers, not all prior pipeline mutations. The public five columns
`ISIN, TRADE_CODE, dt, value, currency`, `all.xlsx` then `all.csv`, `index=False`,
signatures/returns, retries/budgets, logger barrier, order and duplicate rows stay
unchanged. Raw/history/all-time and RSI are outside scope.

Runtime integration is NEEDS_DECISION: #62 source contract/provenance/coverage,
C1-02/#27 isolation, future private evidence store, previous freshness, F0/D
representation mapping and C2-04/D publication/readback remain separate.
C1-03/#75 identity/dedup is parallel independent work. D2/F process containment
and F-LOG integration are full-release acceptance dependencies, not blockers of
this bounded offline PR. Research #2/PR #3 is separate from public datasets.

Future raw evidence needs immutable original bytes, digest/length, acquisition
UTC, profile and response-to-SECID/row/token/canonical bindings, plus distinct
original/private and redacted evidence. A digest proves byte identity, not
source truth. Store root/owner, budgets, retention/access, source terms and
capture-failure handling require a separate approval and implementation slice.

Safe errors retain the full common shape, closed codes, explicit primary ID and
secondary diagnostics. They must not echo raw source strings/bodies, arbitrary
repr, credentials, private paths or environment dumps. Diagnostic failure cannot
replace the primary exception or manufacture successful terminal status.

Error history and terminal failure are separate. A `retryable=true, final=false`
attempt may remain in ordered input history, retained error IDs and the relevant
export's references after successful recovery. It does not itself block a
fully evidenced terminal success. `primary_error_id` is null when no terminal
failure remains; a non-null primary must name a final error. Any unresolved
final mandatory error still blocks success. A secondary DIAGNOSTIC_FAILURE may
not replace an existing substantive final failure as the primary error. The
primary is the first substantive final error in ordered history, or the first
final diagnostic error when no substantive final error exists. No error record
is deleted to make a release succeed.

The independent CODE_REVIEW_CHANGES against
`edffb173210725c5399e73ba03e9ca64ba9d1083` found common-mode binding drift and
recovered-retry rejection. Meaningful assertions on that evaluator produced
two failures and one passing unresolved-final counterexample before the fixes.
The revised evaluator must pass focused regressions and then all clean-HEAD
gates below; prior-HEAD evidence is retained separately. PR #109 stays draft
for a second independent CODE_REVIEW. Runtime integration remains NEEDS_DECISION.

Acceptance evidence is recorded separately for the actual clean candidate SHA:
guarded F3 collection/execution and new node inventory, F2, F-LOG, all ten active
notebook pairs/20 members, then hosted Windows F1/F2/F-LOG/F3/notebook and Linux
syntax checks of that same tested SHA. Failed/skipped/cancelled/unavailable or
other-SHA mandatory checks do not satisfy handoff. Linux count uses actual
tracked Python inventory; adding this test does not expand the closed LNX-03
consumer workload. No author test result substitutes for independent CODE_REVIEW.

Future pair/serializer changes require preserving V1/V2 evidence and fingerprints,
separate V3 cases/oracles, independently recomputed exact rational deltas and
review. `Decimal(literal_source_token)` is the reference; `str(actual_float)` is
not. Artifact-byte equality is distinct from canonical numeric equality; F0
verification is not weakened to accept an unexplained roundtrip mismatch.

Compatible main drift calls for ancestry/path/contract review and updated exact
evidence. A real writer conflict stops only affected integration. No automatic
rebase, force push, ownership release or other-worktree reset is authorized.
Rollback is a separately authorized revert of these three files, preserving
evidence. No dataset rollback/publication recovery is implied. Issues remain
OPEN/PARTIAL; no acceptance criterion is closed automatically.
