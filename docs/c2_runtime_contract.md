# C2 runtime wire contract — U

This contract-only package defines `mds.c2.runtime/1`. It changes no application
entrypoint, dataset writer, publication policy or public CSV/XLSX schema.
Runtime integration and official source acceptance remain **NEEDS_DECISION**.

## Identity and codec

The owner is `F0-D4/C2-U-CONTRACT-OWNER`. The normative closed schema is
[`contracts/c2_runtime_v1.json`](../contracts/c2_runtime_v1.json); the procedural
codec is [`dividend_runtime_contract.py`](../dividend_runtime_contract.py).

Schema identity is exactly:

```text
schema_id: mds.c2.runtime
schema_version: 1 (integer; bool is invalid)
schema_sha256: 2b1ac3102d695d6d8f86a4ff3146af015d71a210f352295f4a30da976400d73b
```

Digest profile `SHA256_CANONICAL_JSON_UTF8_SORTED_COMPACT_V1` means strict UTF-8
JSON, unique object keys, `ensure_ascii=False`, recursively sorted keys,
`separators=(',', ':')`, `allow_nan=False`, no trailing newline. The digest is of
the entire decoded schema, including descriptions; the schema does not embed its
own digest. Whitespace, key order and CRLF checkout do not change this identity.
Git blob/mode and raw-byte fingerprints are separate source evidence. An altered
schema is refused even when a payload repeats its new hash.

The standard-library-only codec accepts explicit inputs:

```text
schema_digest(schema_bytes) -> frozen SHA-256
decode(raw_bytes, *, schema_bytes, trusted) -> fresh decoded record
encode(record, *, schema_bytes, trusted) -> canonical UTF-8 bytes
pack_value(already_decoded_scalar) -> typed cell
unpack_value(typed_cell) -> Python scalar
encode_cell(typed_cell) -> canonical UTF-8 bytes
```

There is no default file lookup, clock, network, persistence or application/test
evaluator import. JSON duplicate keys, bare NaN/Infinity, numeric overflow to a
nonfinite float, invalid UTF-8/BOM and lone surrogate strings are refused. Keys
and types are closed at every object. Missing and null remain distinct; integer
fields reject bool. Caller objects and retained registries are not mutated.

## Correlation and independent caller basis

`context` has exactly `run_id`, `snapshot_id`, `dataset_id`, `as_of`.
`candidate_release_id` is a separate required coordinator allocation, stable
across retry/recovery. IDs are safe logical aliases, never paths or credentials.
`as_of` is a real canonical UTC `YYYY-MM-DDTHH:MM:SSZ` cutoff, not acquisition or
evaluation time. Acquisition bounds, evaluation time and evidence validity are
separate explicit caller inputs. The codec never substitutes current time.

| Wire field | F0 / independent PR #109 alignment |
|---|---|
| `candidate_release_id` | F0 `release_result.candidate_release_id`; #109 `input.candidate_release_id` |
| `previous.previous_release_id` | F0 per-target `previous_release_id`; #109 export previous `release_id` |
| `previous.previous_snapshot_id`, `previous.previous_as_of` | Previous generation facts, independent of current snapshot/cutoff |
| `context` | Four-field C2 correlation; no mandatory `context.release_id` |
| `release_id` | Reserved for an observed generation or explicitly selected F0 event; not an alias for candidate ID and not a U envelope field |

Previous null IDs do not establish ABSENT. An ABSENT claim requires an evidence
reference; PRESENT requires all previous identity fields and an evidence reference.
Such claims still need physical verification downstream. Null reasons are closed
`NOT_AVAILABLE` / `NOT_APPLICABLE`, with exactly one reason per nullable null field.

`trusted` is a separate caller-retained input, not a payload member. It supplies
the context/candidate, bound producer role/profile/version, canonical descriptor,
ordered source bindings, evidence registry, previous facts, evaluation time and
source acquisition window. All relevant identities are compared against it.
Changing rows and recomputing both payload hashes, or replacing evidence together
with all payload references, cannot replace the independently retained basis.

The future D4 coordinator must create this basis from trusted custody, allocate
context/candidate IDs, register producers and revoke/consume one-use tickets.
This codec does **none** of those operations. Source workers, E verifiers, Q
assessors and K identity workers have explicit distinct role bindings; accepting
a role label is not process separation or authentication. A caller that lets the
same producer rewrite `trusted` defeats that boundary. OS/process custody and
the physical evidence store are separate implementation and acceptance gates.

## Typed values and provenance

The canonical descriptor binds the context, five ordered columns
`ISIN, TRADE_CODE, dt, value, currency`, measured row count, content hash and
binding hash. Content hashes canonical `[columns, typed_rows]`; bindings hash
the ordered `{canonical_ordinal, source_id, source_ordinal}` records. Both are
checked internally and against the caller-retained descriptor. Repeats and order
are preserved. The codec does not infer a natural key or deduplicate.

Typed cells distinguish NULL, BOOL, canonical decimal-string INT, finite
canonical FLOAT64 hex, TEXT and runtime NONFINITE tags `NAN/POS_INF/NEG_INF`.
This is a wire representation, not a change to the five public columns. It does
not invent a literal source numeric token or a Decimal reference from a float.
Finite float bits, negative zero, large integers and literal strings roundtrip.
Dates, zero/tiny/negative amounts, 2111 and currency strings receive no semantic
correction. NONFINITE descriptors do not bypass the existing runtime guard.

Source bindings carry acquisition identity, declared source kind, raw hash/length,
acquisition time and separate nullable semantics/coverage evidence references.
Evidence retains context/candidate, source/subject identity, assessor/profile,
status, creation and nullable validity bound. Duplicate IDs, dangling references,
foreign context and caller-binding substitutions are refused. Observed source
times must fit the independently supplied acquisition window and evaluation time.
Provided validity bounds are enforced. An absent validity bound remains unknown;
it is not a freshness PASS. This layer does not read bytes or prove coverage.

Assessment status/check records are **claims being transported**, not an effective
quality result. Even a well-formed nominal PASS with synthetic evidence creates no
official proof or admission. Individual check status retains F0 PASS/FAIL/
NOT_VERIFIED spelling; PASS has null reason, other statuses require a closed reason.
The codec does not run the #109 reducer, mandatory-check inventory or release gate.
`DIVIDEND_V1` and frozen V1/V2 remain independent and unchanged.

## Safe failures, compatibility and review

Failures are `ContractError(ValueError)` with exactly
`C2_RUNTIME_CONTRACT_INVALID:<closed-code>`, raised without interpolated payload,
paths, raw source values, repr or decoder exception text. This is a codec error,
not a deployed F0 error envelope; the future caller must preserve F0 primary/
secondary error semantics when integrating it. Invalid version, digest, shape,
context, producer/source/evidence/previous/canonical binding, freshness, null
reason and reference failures are distinct regression cases.

Fixtures are explicitly synthetic. Independent expected codes, source/content/
binding digests, typed scalar examples and common-mode substitution controls are
checked through the existing guarded F3 collection. Runtime consumers must use
these compatibility/negative fixtures and independently freeze source/evidence
custody before integration. A future accepted schema change needs an explicit
version/digest review; no automatic golden update or unknown-version fallback.

Rollback is a thematic revert of this exact five-file package after separate
authorization. Dataset rollback is unnecessary because this package performs no
dataset writes. Native containment, production readiness, C2_FULL_ACCEPT and
global FULL_RELEASE_ACCEPT are not established by these offline tests.
