# C2 Q — pure candidate quality assessment

Q is a four-file package stacked on accepted U commit
`ef5b9611620712417366f0710743cd892ac0ae53`. It adds candidate assessment, without
activating any application entrypoint or authorizing a release. Runtime source,
E custody, consumers and D/F integration remain separate acceptance gates.

## Interface and authority boundary

```python
assess(raw, *, schema_bytes, trusted, basis) -> assessment_record
```

`raw`, `schema_bytes` and `trusted` are passed directly to the unchanged U
`decode` interface. U identity is `mds.c2.runtime/1`; the exact semantic schema
digest is `a0daced78e661713d75a5f768df5a354f5caf3c82a9f0a76a7ca53f10ad19619`.
Missing/unknown fields, duplicate JSON keys, duplicate evidence/source/check IDs,
context/version/digest drift and caller-retained registry substitutions retain
U's closed failures. This module does not import the independent #109 evaluator.

The additional `basis` is an explicit in-process caller argument, **not** a new
field in the U envelope. Coordinator/E must independently retain it before the
candidate can be changed. Q cannot authenticate that custody or reconstruct it
from candidate rows/hashes. The source worker must not control this argument or
U's `trusted` registry. Producer/assessor aliases are logical bindings, not OS
process separation or credentials. No physical store, ticket or registry is
created by Q; a caller that permits common ownership defeats the boundary.

The function performs no file/network I/O, current-clock lookup, capture,
serialization, publication, normalization, deduplication or source corrections.
It does not emit `ALLOW_PREFLIGHT`, a release decision, a current generation,
an exit status, or a ticket. Publication, preflight, readback, supervision and
previous eligibility are outside this assessment.

## Closed independently retained basis

Exactly these keys are accepted:

| Field | Meaning |
|---|---|
| `basis_version` | Exact integer `1`; bool is invalid. |
| `context` | Exact U run/snapshot/dataset/as_of context. |
| `candidate_release_id` | Exact U candidate identity; not `release_id`. |
| `retained_rows` | Ordered five-cell U typed rows, or null if unmeasured. |
| `retained_bindings` | Ordered U canonical/source ordinals, or null if unmeasured. |
| `source_counts` | Exactly one `{source_id, row_count}` per trusted source; count is nonnegative integer or null. |
| `expected_source_ids` | Independent required membership, or null if unknown. Unique safe aliases. |
| `proofs` | Closed obligation-to-evidence associations described below. |

Present-null means missing measurement, never zero, absence or proof. Missing
keys are contract errors. Retained rows and bindings use U's exact typed cells
and hash profile; #109's tagged test-only fingerprints are not substituted.
In-process context must be an ordinary dict of four ordinary strings, and the
candidate ID an ordinary safe string. Custom equality objects/subclasses are
refused before identity comparison, using the same closed error boundary.
Order and multiplicity are compared literally, including repeated identical
rows. Source ordinals are lineage, not a natural event key.

Each proof has exactly `obligation`, `source_id`, `acquisition_id`,
`evidence_ref`, `subject_sha256`, `assessor_id`, `profile_sha256`. The source and
acquisition are null for global obligations. The reference/subject/assessor/
profile may be null as explicit unavailable proof. A non-null reference must
exist in U's independently retained evidence registry. Duplicate obligations or
reusing one evidence ID for different external obligations is rejected.
One **input anchor** can support several Q-computed checks; it is not an upstream
LOCAL_PROFILE/PASS certificate.

Required global obligations are `CANDIDATE_INPUT`, `COVERAGE_MEMBERSHIP` and
`RUN_ISOLATION`. Every declared source additionally has exactly:

```text
SOURCE_SEMANTICS  DATE_SEMANTICS  AMOUNT_UNITS  AMOUNT_GROSS_NET
AMOUNT_SIGN      CURRENCY_SEMANTICS  RAW_PROVENANCE  COVERAGE
```

Proof IDs and expected subjects/profile/assessor bindings must be selected from
independent E custody. VERIFIED alone is insufficient: the evidence must match
source, subject, E_VERIFIER role, assessor and profile, with non-null validity.
U checks all record contexts, candidate identities, acquisition bounds and
explicit evaluation time/validity. Q adds no TTL. Unknown validity remains
NOT_VERIFIED. Synthetic/historical/conflicted/unverified records cannot support
effective PASS; a SYNTHETIC_ONLY source cannot satisfy official source checks
even if someone supplies nominal VERIFIED records.

`CANDIDATE_INPUT` subject is the SHA-256 of canonical compact sorted UTF-8 JSON:

```text
["mds.c2.quality.input/1", context, candidate_release_id,
 ["ISIN","TRADE_CODE","dt","value","currency"],
 retained_rows, retained_bindings, trusted.source_bindings]
```

`COVERAGE_MEMBERSHIP` subject uses
`["mds.c2.quality.membership/1", context, candidate_release_id, expected_source_ids]`.
RAW_PROVENANCE subject must equal the trusted source raw SHA-256. Semantics and
coverage references must match the corresponding U source-binding references;
all five semantics subsidiary obligations are independently required. Other
subject/profile assertions are retained provider facts, not inferred by Q.
Evidence status is never rewritten. No new evidence records are fabricated.

## Nine checks and result semantics

| Check | Q computation / required basis |
|---|---|
| IDENTITY_SCHEMA | U-validated exact context, columns and typed cells; canonical present and input anchor. No inferred ISIN/source event semantics. |
| LOCAL_PROFILE | All rows: Gregorian date/ASCII shape, INT or finite FLOAT64 amount, nonblank TEXT currency without the accepted C0 set; input anchor. |
| SOURCE_SEMANTICS | Every source's summary and five semantic obligations; no guessed units, gross/net, sign or currency vocabulary. |
| RAW_PROVENANCE | Every source's raw digest and bound E proof. Q never reads raw bytes. |
| COVERAGE | Every source's coverage proof and independently proved expected membership; matching unknown lists is not proof. |
| RUN_ISOLATION | Independently retained run-isolation proof. Q does not repair global accumulation. |
| CANONICAL_CONTENT | Exact retained typed rows, measured total and each source count against its binding multiplicity; input anchor. |
| CANONICAL_BINDINGS | Exact ordered row-to-source association against retained rows/bindings; input anchor. |
| REQUIRED_NONEMPTY | Measured canonical count is positive; input anchor. Empty is FAIL. |

All effective PASS checks require a real retained input anchor plus any external
obligations. `assessment` preserves U's exact `status`/`checks` shape; individual
checks have `check_id`, `status`, `evidence_ref`, `reason`. Its PASS reference is
the actual supporting input-anchor ID; `basis_evidence_refs` separately lists
all consulted source/membership/isolation references. No fake aggregate evidence
record is introduced. `computed_checks` retains observed computations even when
missing proof downgrades effective PASS. Safe local findings contain only row
ordinal, fixed field and closed rule, never source values.

An incoming NOT_RUN/empty assessment is the normal pre-Q input. Any already-run
assessment requires exactly all nine check records. Incoming FAIL and explicit
uncertainty are preserved; nominal PASS cannot override missing proof. Aggregate
precedence is FAIL, NEEDS_DECISION, NOT_VERIFIED, PASS. `observed_assessment` is
retained separately; Q running now does not pretend that an earlier assessment
ran. Missing/duplicate check inventory is a contract error.

Counts are outside U `assessment`: `rows_checked`, `accepted_rows`,
`quarantined_rows`, `locally_valid_rows`, `locally_invalid_rows`, `source_rows`.
Here `accepted_rows` measures the rows **already present in the supplied
canonical candidate**, not Q approval. Q inspects all of them, keeps every row,
and does no quarantine; measured quarantine count is zero only with a canonical
candidate. An absent canonical yields null local counts. Unknown source totals
remain null; local invalidity does not silently discard rows.

`source_counts` measures rows supplied by each selected response. Equal totals
cannot hide a per-source mismatch. A known mismatch remains FAIL even alongside
another unknown count. Q does not infer source-ordinal uniqueness, contiguity or
a natural event key; ordered lineage is checked against the independently
retained selection without deduplication.

Calendar-valid 2111/future dates, zero/tiny/negative finite amounts, unknown
currencies and duplicate rows remain unchanged. Finite serialization losses are
not repaired or measured by Q; `precision_status=NOT_VERIFIED` remains explicit.
This profile is not an official source contract or complete XML safety check.

## Errors, regression evidence and remaining gates

U errors retain `C2_RUNTIME_CONTRACT_INVALID:<code>`. New basis failures are
`QualityInputError(ValueError)` with `C2_QUALITY_INPUT_INVALID:<closed-code>`:
`U_INTERFACE`, `BASIS_VERSION`, `BASIS_SHAPE`, `BASIS_CONTEXT`, `BASIS_SOURCE`,
`BASIS_INVENTORY`, `BASIS_REFERENCE`, `BASIS_SELECTION`, `CHECK_INVENTORY`.
No caller values, raw payload, paths or arbitrary exception text are appended.

The fixed fixture file was authored before implementation. Every case is
SYNTHETIC_ONLY: positive cases exercise the conditional branch under hypothetical
independently retained VERIFIED records; they are not captured MOEX evidence.
Explicit synthetic-record/source cases demonstrate refusal to promote such
records. Expected decisions are manually fixed, not generated from Q outputs.
Tests preserve original objects and cover common-mode row/hash/lineage mutation,
proof substitution, missing/null/duplicate inventory, source/identity mismatch,
all local boundaries, precedence and pure no-I/O behavior.

Validation uses existing guarded shared-test entrypoints and the approved pinned
Windows environment. Historical U/main F3 failures remain historical failures;
new focused or hosted PASS cannot establish their cause or waive integration.
Actual run SHA/counts/exits and independent review are recorded in the PR receipt.
No workflow, guard, dependency, public schema, pair, V1/V2 or #109 change is part
of Q. Rollback is a separately authorized thematic revert of these four additions;
there is no dataset rollback. C2_FULL_ACCEPT and FULL_RELEASE_ACCEPT are not claimed.
