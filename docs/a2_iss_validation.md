# A2-02 candle HTTP 200 structural validation

This slice implements the accepted `A2_CANDLES_CSV_V1` consumer profile for
[issue #20](https://github.com/foykes/moex-dataset/issues/20), with scoped
failure/diagnostic contributions to [#22](https://github.com/foykes/moex-dataset/issues/22)
and [#78](https://github.com/foykes/moex-dataset/issues/78).
Pinned development base: `3b6e78f11f7681cdcd9c813fff4236a8c63ab677`.
Source HEAD, actual results and independent main delta belong in the PR handoff.

## Ownership and limits

The exact allowlist is `data_gathering.py`, `data_gathering.ipynb`,
`tests/A/test_iss_validation.py`, `tests/A/test_iss_transport.py`,
`tests/shared/test_preparation.py`, and this document. The pair is one zone.
The [repository-owner direct grant](https://github.com/foykes/moex-dataset/issues/47#issuecomment-6162081162)
permits only Content-Type/charset metadata in the local successful response
double of `test_candle_retry_503_then_200_has_literal_rows_and_keys`.
Its body/bytes, nodeid, request tuples, rotation, delays, literal rows/values,
columns/order, keys/index and fixture/previous hashes are retained.
The remainder of the shared file, fixtures, logger, contracts, guards, runner,
workflows and dependencies are excluded. The A2-01 handoff is not this grant.

No pagination, economic-quality repair, deduplication, sorting, quarantine
writer, metadata/CollectionStatus, export or release implementation is added.
Raw OHLC/history, RSI, public schema/filenames, Excel/Google and capacity/previous
policy remain unchanged. Local refusal does not prove coherent release or
remote preservation. Independent main advances do not rewrite the pinned
development branch; actual interface/ownership/guard/security conflicts require
analysis and a separately recorded pinned integration snapshot.

## Evidence classes

`DOCUMENTED_SOURCE_FACT` means an official source statement.
`RETAINED_ENDPOINT_OBSERVATION` means a real capture with declared scope/format.
`PROPOSED_CONSUMER_POLICY` denotes the accepted policy of this bounded consumer,
not a claim that the provider emits only these forms. `NOT_VERIFIED` denotes
missing source evidence. All regression payloads here are synthetic.

The [official ISS documentation page](https://www.moex.com/ru/documents/6523)
links the [Developer Manual v1.4](https://www.moex.com/files/4gspnsx9er8s7wg65ve3ccg1ch),
dated 2016. It documents `text/csv` and windows-1251 for CSV, configurable
representations, multiple blocks and a general empty-block pagination endpoint.
It does not specify every current boardless shares/bonds CSV byte form.
The historical [audit](https://github.com/foykes/moex-dataset/pull/3) has real
JSON observations, not proof of this CSV envelope/charset/null semantics.
Those originals were not revalidated by A2-02. No new live probe ran.
Board-scoped ALGOPACK metadata is not substituted for this endpoint's contract.

## SOURCE_PROFILE_TABLE

ACCEPT means eligible after every check succeeds. REJECT and
UNSUPPORTED_OR_UNVERIFIED both refuse the entire response. O0 means unchanged
successful pandas values/dtypes/order/index; F means failure before return/writers;
E0 means the existing zero-row, zero-column return; R means an explicit residue.

| Representation | Decision / algorithm | Evidence | Fixture / output impact |
| --- | --- | --- | --- |
| `text/csv`, ASCII case variants | ACCEPT; full media-type comparison | DOCUMENTED_SOURCE_FACT; consumer case handling | Literal valid rows, O0 |
| Missing/HTML/malformed MIME | REJECT; no body sniffing | PROPOSED_CONSUMER_POLICY | MIME fixtures, F |
| Other MIME or unknown parameter | UNSUPPORTED_OR_UNVERIFIED; only one charset parameter allowed | PROPOSED_CONSUMER_POLICY | MIME/parameter fixtures, F |
| `windows-1251`/`cp1251` | ACCEPT; strict cp1251 decode | Documented encoding; aliases are consumer policy | Literal rows, O0 |
| No charset | ACCEPT; explicit cp1251 default | PROPOSED_CONSUMER_POLICY based on documented CSV encoding | Default fixture, O0 |
| `utf-8`/`utf8`, including quoted value | ACCEPT; strict UTF-8 | PROPOSED_CONSUMER_POLICY, not live observation | UTF-8/quoted fixtures, O0 |
| Unknown/malformed/duplicate charset, non-ASCII parameter name | Refuse; closed ASCII allowset | PROPOSED_CONSUMER_POLICY | Charset fixtures, F |
| One leading BOM with explicit UTF-8 | ACCEPT for envelope validation only | PROPOSED_CONSUMER_POLICY | BOM fixture, O0 |
| BOM with default/cp1251, repeated or embedded BOM | REJECT before decoding | PROPOSED_CONSUMER_POLICY | BOM fixtures, F |
| Invalid encoded bytes | REJECT; native UnicodeDecodeError, no replacement | PROPOSED_CONSUMER_POLICY | Strict-decode fixtures, F |
| LF/CRLF; final newline optional | ACCEPT; preserve original decoded text | PROPOSED_CONSUMER_POLICY | Line-ending/EOF fixtures, O0 |
| Standalone CR | UNSUPPORTED_OR_UNVERIFIED; no implicit dialect widening | PROPOSED_CONSUMER_POLICY | Bare-CR fixture, F |
| Empty physical CSV records `[]` | ACCEPT and ignore; whitespace/quoted-empty/8-empty-field records are not blank | PROPOSED_CONSUMER_POLICY | Blank/null fixtures, O0/F |
| Exactly one `candles` marker and ordered raw8 header | ACCEPT; whole record sequence checked before pandas | Existing literal oracle; consumer grammar | Block/header fixtures, O0 |
| Missing/wrong/duplicate block, additional block/trailer | Refuse; never truncate or skip it | PROPOSED_CONSUMER_POLICY | Envelope fixtures, F |
| `;`, standard quoted/unquoted cells and doubled quotes | ACCEPT; strict csv.reader | PROPOSED_CONSUMER_POLICY | Quoted/semicolon fixtures, O0 |
| Other separator or malformed quotes | Refuse; no sniff/repair/skip | PROPOSED_CONSUMER_POLICY | Syntax fixtures, F |
| Exact raw8 `open,close,high,low,value,volume,begin,end` | ACCEPT; names unique and ordered | Existing literal oracle; consumer policy | Literal/header fixtures, O0 |
| Missing/duplicate/renamed/extra/reordered field | Refuse; no pandas header mangling or schema repair | PROPOSED_CONSUMER_POLICY | Header fixtures, F |
| Every row has exactly eight cells | ACCEPT; check the entire response | PROPOSED_CONSUMER_POLICY | Width/prefix-tail fixtures, O0/F |
| Signed integer/decimal/exponent finite numeric cells | ACCEPT; lexical check then inferred numeric/finite frame check, without assignment | PROPOSED_CONSUMER_POLICY | Literal dtype/scientific fixtures, O0 |
| Zero, finite negative values and fractional volume | ACCEPT structurally; no positivity/integer-volume/price cap | PROPOSED_CONSUMER_POLICY | Quality-boundary fixtures, O0 |
| Null/blank/NA/NaN/Inf/overflow/text | REJECT; required finite consumer cells | PROPOSED_CONSUMER_POLICY; provider null meaning NOT_VERIFIED | Numeric fixtures, F |
| Decimal comma/thousands separator | UNSUPPORTED_OR_UNVERIFIED; no conversion | PROPOSED_CONSUMER_POLICY | Numeric fixtures, F |
| Exact ASCII `YYYY-MM-DD HH:MM:SS` and valid calendar | ACCEPT; explicit padded component roundtrip, preserve input strings | Existing literal oracle; consumer policy | Date/low-year fixtures, O0 |
| Invalid/null or alternative timestamp representation | Refuse; no coercion or timezone conversion | PROPOSED_CONSUMER_POLICY | Date fixtures, F |
| Repeated begin in this one requested scope/interval | REJECT identical and conflicting copies; no dedup/version choice | PROPOSED_CONSUMER_POLICY; not full source-key proof | Collision fixtures, F |
| OHLC inequalities, end-before-begin, session/bar completion | NOT_VERIFIED as quality claims; no semantic repair/rejection added here | Separate B2/temporal evidence | Well-typed anomaly fixtures, R |
| Valid marker/header with zero data rows | ACCEPT structurally; preserve legacy empty shape | Consumer grammar; empty concept documented, scope meaning NOT_VERIFIED | Empty fixtures, E0 |
| Added ticker | Existing request binding only | Independent payload identity NOT_VERIFIED | Public9 literal oracle, O0/R |

Numeric grammar is `[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?`.
Only surrounding ASCII space/tab is ignored in a temporary numeric view.
The provider's genuine null forms may be legitimate but unsupported by this
required-finite profile: refusal is not a claim that such a source row is fake.

## Whole-response algorithm and compatibility

The accepted [A2-03 finite retry/resource policy](a2_iss_retry_policy.md)
replaces eager response loading and speculative validation retries. It leaves
every accepted representation and output check in this profile unchanged.

Validate MIME/parameters and BOM; strict-decode complete bounded bytes; parse the
complete CSV; validate envelope/raw8/width/numeric/calendar/local collisions.
Only then run the original pandas read_csv with `sep=';'`, `header=1` on the
original decoded text. Reconcile exact columns, record count, RangeIndex and
timestamp strings; inspect inferred numeric/finite values without converting
or assigning to the returned frame. No Decimal-versus-float equality or new
rounding policy is introduced. Zero-row numeric dtype checking is skipped
because an empty pandas CSV has object columns.

After every check succeeds, retain the original ticker injection/concat,
mandatory success sleep3, returned event and return. A valid prefix followed
by damaged data cannot return. Empty still returns a zero-column frame.
Routing, query parameters, positional arguments, caller hookups and successful
schema/values/dtypes/order/index/timestamps are retained.

## Error behavior and closed diagnostics

| Source phase | Native primary / code / category |
| --- | --- |
| Profile/schema/frame refusal | ValueError with fixed args `A2_ISS_RESPONSE_SCHEMA_INVALID`; SOURCE |
| CSV syntax | Same native csv.Error object/args; `A2_ISS_RESPONSE_SCHEMA_INVALID`; SOURCE |
| Strict decode | Same native UnicodeDecodeError object/args; `A2_ISS_RESPONSE_DECODE_ERROR`; SOURCE |
| Existing pandas parsing | Same native parser object/args; `A2_ISS_PARSE_ERROR`; SOURCE |

The original A2-02 permitted invalid-to-valid retry recovery. A2-03 deliberately
makes MIME/decode/envelope/schema/CSV/pandas/frame failures terminal: preserve
the first current native primary and do not consume a prepared later valid
response. This changes retry scheduling, not the accepted profile. Default
eligible-transient sleeps remain 3/6/12/24, with header rotation and the final
attempt's native primary. TLS/redirect refusal remains immediate. A2-03 owns
the finite classification, Retry-After, optional jitter, cooperative deadline,
identity streaming/body cap and retry-argument caps; no hard deadline/RSS
claim follows.

Existing F-LOG event names/levels, stage/fields={}, correlation/null reasons,
accepted/confirmed/health gates and primary-versus-logging precedence remain.
No raw header/body/URL/exception text is copied into messages. Known native
exception type names are selected from a fixed list. Context owns normal
console delivery; no-context fallback remains bounded and non-DEBUG.
QUERY_*_UNVALIDATED are retained diagnostic outcomes because identity, economic
quality, coverage and release remain unevidenced. Counts still measure only
rows_received after whole-response acceptance; accepted/quarantined/instrument
counts remain NOT_ASSESSED and pages NOT_PAGINATION_EVIDENCE. No F0 enum or
metadata is added. Duration includes decode/validation/pandas/retries and the
completed success sleep; interrupted sleep never emits returned.

## Fresh regressions and execution

The A2-02 closed fixtures cover the accepted profile categories, exact dtypes/values,
whole-response refusal, invalid-to-valid/exhaustion/backoff, local previous-file
hashes and zero writers after a later response failure, native primary identity,
None/context canaries and logging-failure precedence. Existing A2-01 assertions
remain; MIME/valid-marker adaptations preserve real F-LOG level/no-duplicate
oracles and move body canaries to a failure/header carrier. A2-03 updates only
the explicitly superseded invalid-to-valid scheduling and transport-double
oracles; literal profile/output/canary/previous-hash assertions remain.

First demonstrate substantive RED on the unchanged pinned production pair,
then candidate A, source-only notebook sync and a clean commit. Use existing
push-triggered Windows foundation CI first for native F1/F2/F-LOG/F3 with
current shared/C2 membership and all ten notebook pairs. Run fresh explicit
full tests/A collection/execution on that same final source HEAD. The foundation
workflow does not execute A, and historical A185 is not fresh A2-02 evidence.
The draft PR also requires exact-HEAD Linux syntax. Do not modify infrastructure
to hide admission or capability failures. Any required non-elevated host A
execution needs separate fixed-command approval; previous one-shot host grants
and locks are not reused. Source/F1 preservation and native receipts must be
read back, rather than treating process exit alone as delivery/inventory proof.

## AC and integration residues

| AC | This slice | Remaining owner/package |
| --- | --- | --- |
| #20 MIME/columns/numeric/date/identity | Selected format checks; request binding only | Independent identity/provenance A/F0 integration |
| #20 empty versus missing/HTML/schema | Structural empty distinguished | Scope availability/empty membership A/R1 |
| #20 failed middle page | Local invalid-response caller stop | A3 pagination; D/R1 release |
| #22 failure/nonzero/previous | Last primary and affected local no-writer/hash evidence | Common FAILED/nonzero/upload-stop F/R1; coherent release/remote D/R1 |
| #22 retry classification/budget | A2-02 profile unchanged; former retry scheduling superseded | A2-03 finite consumer policy and separate evidence |
| #24 transport | A2-01 regressions retained | Separate live acceptance |
| #78 diagnostics/canaries | Closed scoped errors/events/fixture evidence | Typed HTTP NOT_CONNECTED; common delivery/bundle F/R1 |

Full natural keys include engine, market, source_scope, board, raw instrument,
interval and begin; ticker/index do not prove them. Structurally empty CSV
does not establish empty scope, coverage COMPLETE or mandatory empty release.
REQUIRE_NONEMPTY and previous/capacity policy remain existing release contracts.
B2 CONTINUE_ACROSS_QUARANTINE is settled but not implemented here. Coordination,
F0 and agent-rule documentation updates are separate follow-up, not hard
dependencies of structural validation.

Rollback requires an explicitly agreed scoped revert of A2-02 only, retaining
A2-01, foreign commits, datasets, environments and all historical evidence.
Rollback restores the unvalidated-HTTP200 risk and is never automatic. This
slice does not authorize merge, issue closure, live diagnostics or production.
