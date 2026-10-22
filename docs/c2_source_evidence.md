# C2 E — offline raw and source evidence verification

This five-file package is stacked on accepted U commit
`ef5b9611620712417366f0710743cd892ac0ae53`. It verifies explicit retained inputs.
It does not collect a response, deploy a private store, authenticate an issuer,
create a registry/ticket, import Q/#109, activate an entrypoint or admit a release.

## Interface and independent custody boundary

```python
verify_capture(raw_bytes, *, capture, retained, profile_bytes, schema_bytes)
```

All inputs are explicit. There is no default root, path discovery, file/network
I/O, current-clock lookup or source retry. The caller must supply original bytes
captured before decode. `capture` contains the claimed metadata; `retained` is a
separate parent-owned input frozen before the source/canonical worker can change
its output. Giving both arguments to the same untrusted worker defeats custody.
The helper cannot establish OS/process isolation or immutable physical storage.
Every successful result therefore has `custody_status=NOT_VERIFIED` and
`release_admission=NOT_ASSESSED`.

U remains `mds.c2.runtime/1`, exact digest:
`a0daced78e661713d75a5f768df5a354f5caf3c82a9f0a76a7ca53f10ad19619`.
E calls the frozen U schema verifier and typed-cell codec, without modifying U.
The E-specific retained document records below are **not U wire evidence** and
are not directly installable into Q's registry or proof map. A future reviewed
coordinator adapter must bind actual custody, retained selection and applicable
evidence to U/Q; this package creates no such authority.

`capture` has exactly: `capture_id`, U four-field `context`, separate
`candidate_release_id`, U-shaped `source_binding`, `request_id`, `instrument`
(`isin`, `secid`), positive integer `attempt`, boolean `complete`, and
`profile_sha256`. `source_binding` retains acquisition/source identity, declared
kind, raw digest/length/acquisition UTC, nullable semantics/coverage references
and explicit null reasons. A declared `MOEX_ISS` label is not origin proof.

`retained` has exactly: independently frozen `capture`, `profile_sha256`,
`evaluation_as_of`, independent `acquisition_window` (`start`, `end`),
`requirements`, and `evidence`. Context, IDs, actual raw bytes/hash/length,
completeness, request/instrument/attempt/acquisition and profile commitments are
checked **before source decoding**. Changing raw plus claimed manifest together
cannot replace the retained commitment. `as_of` remains a business cutoff;
acquisition time and evaluation time are distinct, explicit caller values.

Missing fields differ from present-null. Unknown keys, wrong primitive types,
bool-as-count, duplicate identities and custom equality objects are refused.
No natural event key, event deduplication, SECID reuse or association inference
is performed. The retained request association is a caller responsibility.

## Supported mapping and exact token evidence

The tracked profile is a closed, versioned, **offline mapping** profile. Its
canonical JSON digest is pinned independently in retained metadata. It contains
no production/private physical path, credentials, TTL, official assertion or
redistribution permission. `source_fact_status` remains `NOT_VERIFIED`.

The only supported mapping is `MOEX_DIVIDENDS_JSON_V1`, over strict standard
UTF-8 JSON with a `dividends.columns`/`dividends.data` block and named
`registryclosedate`, `value`, `currencyid` columns. Column order is discovered
from those names; duplicate names, malformed rows or absent columns are refused.
Extra named columns and other response blocks do not create coverage proof.
The three selected source cells must be JSON scalars. A valid empty data array
is a successfully verified empty selection, not quality or release PASS.

The mapping accepts declared `MOEX_ISS` or `SYNTHETIC_ONLY` fixtures. DOHOD/HTML
and other mappings are unsupported; a source-kind label does not invent a
parser. Future dohod selection needs its own reviewed adapter/evidence mapping.
Bare `NaN`/`Infinity`, duplicate JSON keys, malformed UTF-8/BOM and lone surrogates
are refused. This source decoder differs deliberately from strict U wire JSON:
standard numeric token `1e309` can decode to infinity, recorded using U's
NONFINITE descriptor. E does not silently correct it or admit it through Q.

E independently derives ordered typed rows and source-row bindings, row count,
per-source count, content hash of `[columns, typed_rows]` and ordered bindings
hash using U's canonical JSON digest profile. Repeated source rows remain
repeated. The public five-column order is unchanged. `ISIN` and `TRADE_CODE`
come from retained request association, explicitly tagged `CALLER_BINDING`;
they are not reported as literal response tokens.

Each amount observation includes source row/column, exact literal token and
UTF-8 byte interval, decoded typed value, independent decimal reference and
numeric delta. The locator traversal follows JSON row/column structure, so an
identical number in another field cannot supply the token. Numeric reference is
`Decimal(literal_token)`. Delta uses exact rational arithmetic, with signed
numerator and positive denominator strings. No Decimal context rounding or
`str(actual_float)` reference is used. Finite long-decimal/underflow losses are
reported. For `1e309`, the numeric reference remains available but delta is null
with `NONFINITE`; string/null/bool amounts have no invented numeric reference.
E does not measure downstream pandas/CSV/XLSX losses or claim losslessness.

Exact measurement has an explicit implementation capacity: at most 1024
coefficient digits (including zeros) and absolute literal exponent at most 1024.
Both limits are checked before Decimal/Fraction construction or exponentiation;
exponent text is length-checked before integer conversion. Within this capacity,
the exact decimal denominator has at most 2048 powers of ten; combining it with
a binary64 fraction stays below the pinned runtime's 4300-digit integer-string
limit. Over-capacity measurement returns closed `PRECISION_CAPACITY`, with no
rounded value, zero delta or preservation claim. This is a computational bound,
not an official MOEX rule or an economic amount threshold. Conversion limits
and Decimal failures also retain this safe boundary; locator recursion produces
`RAW_JSON`. The original 56 fixture objects remain unchanged.

## Source semantics and evidence status

Retained requirements contain exactly one association for each obligation:

```text
ORIGIN SOURCE_SEMANTICS DATE_SEMANTICS AMOUNT_UNITS AMOUNT_GROSS_NET
AMOUNT_SIGN CURRENCY_SEMANTICS COVERAGE
```

Each association has `obligation`, nullable `evidence_ref`, `subject_sha256`,
`assessor_id`, `profile_sha256`. Its four nullable proof values are either all
present or all unavailable. A null requirement is uncertainty, not an omission
of the obligation. Non-null references are unique and resolve exactly once.

The separate retained document record has `evidence_id`, U context/candidate,
source/acquisition identity, `subject_text`, status, assessor role/identity,
profile digest, creation/valid-until times and explicit business-as-of
applicability bounds. Its text is hashed against the independently retained
requirement; arbitrary self-reported VERIFIED/hash values are insufficient.
Context, source, acquisition, expected assessor/profile/subject and source
semantics/coverage references must all match. The E role label is a logical
binding, not authentication or an OS capability.

Observed evidence status is retained. Missing, synthetic, historical,
conflicted, unverified, expired, not-yet-valid or unknown-validity evidence
cannot support an effective VERIFIED result. A synthetic source cannot become
official by receiving nominal VERIFIED records. Semantics needs the summary
plus all five subsidiary obligations; coverage is independent of received row
counts. A successful hash/selection comparison proves retained byte identity,
not official origin, economic meaning, universe completeness or legal use.

Conditional positive fixture branches assume independently retained evidence;
all fixtures, including their document texts, are explicitly synthetic. These
tests prove validation logic, not current MOEX facts. `2111-01-01`, zero/tiny/
negative values, unknown currencies and duplicates are preserved. Natural-key
proof, source/VPS acquisition, dohod redistribution rights, private storage and
run-isolation proof remain separate acceptance requirements.

## Errors, validation and rollback

E failures are `EvidenceError(ValueError)` with only
`C2_EVIDENCE_INVALID:<closed-code>`: `SHAPE`, `PROFILE`, `U_INTERFACE`,
`CAPTURE_BINDING`, `RAW_IDENTITY`, `INCOMPLETE_CAPTURE`, `ACQUISITION`,
`UNSUPPORTED_MAPPING`, `RAW_JSON`, `SOURCE_SHAPE`, `EVIDENCE_INVENTORY`,
`EVIDENCE_BINDING`, `PRECISION_CAPACITY`. U schema errors retain U's safe contract boundary. No raw
value, document text, request string, path or arbitrary exception/repr is echoed.
Capture failure is terminal for dependent admission; E initiates no HTTP retry.

Fixed independent fixtures precede the implementation and include exact tokens,
binary deltas, UTF-8 offsets, reordered columns/decoy tokens, empty selection,
raw/manifest spoofing and authority/freshness failures. Guarded scoped and final
checks record actual source SHA/mode/counts; historical U/main/Q failures remain
preserved. No test-only #109 evaluator, production writer, frozen V1/V2 fixture,
guard, workflow, dependency or existing schema is changed.

Rollback is a separately authorized revert of these five additions. Dataset
rollback is unnecessary. Runtime integration, C2_FULL_ACCEPT and global
FULL_RELEASE_ACCEPT are not established by this offline helper.
