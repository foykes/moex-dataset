# A4-01 catalogue candidate

This slice prepares a detached candidate from caller-supplied bytes. It does not
download a catalogue, write current targets, activate a release, or change the
legacy `moex_tickerlists` caller. A ready candidate is neither A1 admission nor a
verified source or publication. Refs [#19](https://github.com/foykes/moex-dataset/issues/19)
and [#58](https://github.com/foykes/moex-dataset/issues/58).

Accepted plan: A4-01-R2. Implementation base:
`e7800add6ba700f37656cba3517b4e6db83ca413`, tree
`95a0e09b19be7350f2af6cb229d7486a242ae16b`. The four tracked paths are
`catalogue_snapshot.py`, `tests/A/test_catalogue_snapshot.py`,
`tests/fixtures/a4/catalogue_cases.json`, and this document. Existing A/F/F07 tests,
notebook pairs, D1, shared logger/guard/launcher/CI and F0 are read-only.

## Authority and source qualification

The full Fix Issues v2.1 document dated 2026-10-04 was read, including all 464
lines. Its exact archived locator is
`MOEX_DATASET_CODEX_PROMPT_KIT_v1.zip :: moex-dataset-codex-kit/90_source/MOEX_DATASET_FIX_PLAN_v2_1.md`.
The owner-provided archive is available in the local Downloads folder; the
document is read directly from the ZIP without extraction.

| Identity | SHA-256 |
|---|---|
| ZIP | `82f9e2d57164d9e1fc3a4a1f84640d9b60f181b63f96fbe919c155765de6fbd4` |
| Document entry, 64,712 bytes | `e56bfc78adefd3c30583d3f56c8011c219cf808e9b25a4c53947058ecc54a3a2` |

The document assigns A4 status/MIME/schema validation, one CSV/XLSX snapshot and
preservation on refusal. It permits small PRs and separates D1 single-file
replacement from pair/release transactions. It does not define a catalogue index
or serializer contract. The later
[A3 closeout](https://github.com/foykes/moex-dataset/issues/76#issuecomment-6381573817)
accepts the scoped A3 dependency and calls for separate A4 plan review. The later
[CONTINUE_ACROSS_QUARANTINE decision](https://github.com/foykes/moex-dataset/issues/76#issuecomment-6133109560)
supersedes the older document's unresolved reset proposal; this slice does not
change indicator semantics.

Consumer acceptance is explicit and is not official MOEX source qualification:

| Required `consumer_profile` | Encoding rule |
|---|---|
| `A4_CATALOGUE_EXPLICIT_ENCODING_V1` | A supported declared charset is required. |
| `A4_CATALOGUE_LEGACY_CP1251_V1` | Missing charset uses CP1251; this compatibility rule is explicit. |

`SOURCE_CONTRACT_QUALIFICATION` and `SOURCE_FEASIBILITY` remain `NOT_VERIFIED`.
Successful parsing does not establish the current official full schema,
encoding authority, origin/redirect chain or target-VPS feasibility. The 403
reported on 2026-09-05 remains historical evidence. No fallback source, access
bypass or live probe is part of this helper.

## Interface and payload contract

```python
prepare_catalogue_snapshot(
    content, *, consumer_profile, status, content_type, source_ref,
    snapshot_id, captured_at_utc, logging_context=None, artifact_ids=None,
)
```

The result is `{'artifacts': {basename: bytes}, 'receipt': ...}`. The four
basenames are `moex_full.csv`, `moex_full.xlsx`, `moex_stocks.csv` and
`moex_stocks.xlsx`. There is no target-path or activation argument. Missing
profile selection refuses rather than choosing a source policy implicitly.

Validate supplied status, MIME, encoding, CSV structure, required headers,
nonempty universe and limits before rendering. Missing required headers,
blank/duplicate headers and ragged rows refuse. A valid source-added column is
retained in source order in both representations. Keep source cells as text, raw
row order and duplicates; do not
infer numeric/date types, trim identity values, normalize tokens or route unknown
SUPERTYPE values to shares. Stocks contain exactly `Акции` and
`Депозитарные расписки`. Full and stocks payloads derive from the same accepted
source bytes and their respective single canonical frames.

CSV uses UTF-8, comma, minimal quoting, CRLF and the index. Full index values are
`1..N`; the filtered stocks index is `0..M-1`. XLSX has only `Sheet1`, a blank A1,
source headers from B1, and the corresponding index column. Source text,
including formula-looking headers/cells and `#N/A`, must read back as literal
text. Empty cells are checked using the documented logical empty-string
normalization of Excel blanks. Independent readback checks ordered schema,
index, identity, cell types and values; counts alone do not establish parity.

Raw CSV text roundtrip is not safe spreadsheet auto-open and does not implement
an approved CSV escaping policy. [#35](https://github.com/foykes/moex-dataset/issues/35)
/D3 remains open. No new public dataset columns, release manifest, market routing
or historical-universe policy is introduced.

The local receipt version is `A4_CANDIDATE_RECEIPT_V1`. It records the profile,
snapshot ID, capture UTC, source reference and SHA-256, canonical codec, ordered
schema, full/stocks row counts and each payload's byte count/SHA-256 plus render
and readback outcomes. Structural parsing/rendering/readback `PASS` is distinct
from source qualification: its status, encoding, full-schema, origin/redirect and
target-live entries remain `NOT_VERIFIED`; source feasibility is `NOT_VERIFIED`
and activation authority is `NOT_GRANTED`. The receipt is evidence about this
candidate, not an independent correctness oracle or a release manifest.

## Limits, diagnostics and refusal

| Boundary | Maximum |
|---|---|
| Supplied body | 16 MiB |
| Source columns | 256 |
| Source data rows | 100,000 |
| Parsed source cells, headers plus rows, excluding index | 1,000,000 |
| Aggregate payload high-water bytes | 64 MiB |
| Each cell | 32,767 UTF-16 code units |

These are A4 consumer limits, not MOEX guarantees or hard process/RSS/storage
admission. The 42,810,563-byte tracked CSV is not the HTTP-body size oracle.
XLSX serialization can use temporary storage; execution therefore needs the
separate admitted process/storage profile and its aggregate accounting. The
helper does not configure shared logging, launch workers or admit its own run.

Diagnostics use the existing closed F-LOG envelope with `fields={}` and bounded
fixed codes/messages. No body, cell dump, raw header values, arbitrary exception
text or private source/path is logged. With a context, accepted event receipts
must share one active parent producer (`worker_id=None`), INFO delivery to the
console/file, and a later confirmed flush barrier; health/ACTIVE state is checked
again before return. Failure ERROR delivery is confirmed when available. The
receipt records producer, barrier sequence and event sequences with logging
`CONFIRMED`; without context it records `NOT_CONNECTED`. It must not invent
delivery confirmation. Secondary logger/cleanup failures preserve a native
primary exception with only bounded `a4_failure` diagnostics.

Any validation, resource, serialization, readback or required diagnostic failure
refuses the complete helper result. No partial artifacts/ready receipt are
returned, and no current target is opened. This proves no activation by the
helper; it does not prove current/previous preservation across legacy callers,
crash recovery or remote publication. Rollback is a reviewed revert of these
four paths; preserve unrelated worktrees, outputs, environments and evidence.

## Regression evidence and acceptance matrix

Execution disposition: **BLOCKED_EXECUTION**. Discovery is retained separately at
`.f3/evidence/a4-execution-admission-20261026T072337Z/ADMISSION_DISCOVERY.json`.
This is discovery evidence, not an admission or test receipt. The demonstrated
reason is `EXISTING_MECHANISMS_DO_NOT_QUALIFY_AGGREGATE_ORIGINAL_STORAGE_AND_FINAL_SETTLEMENT`.
Historical A3 storage observations are sampled, rather than enforced cumulative
bounds, and final descendant settlement is not independently confirmed. The
fixed canonical F3/R3 observer retains its original-storage refusal; the
operational R4 parent/storage admission implementation is absent in the checked
worktree inventory at the discovery timestamp. Shared infrastructure redesign
is outside this four-path slice.

A4 still needs aggregate original stdout/stderr, TMP/cache/evidence accounting
and checked termination/settlement in an applicable execution profile. Project
imports, pytest collection/runs, environment setup, live probes and ticket
issuance for A4 are all **NOT_RUN**. Runtime is **NOT_PREPARED**; a qualified
outer invocation/admission is **NOT_AVAILABLE**. Legacy RED, helper GREEN,
full A-runtime, common gates and exact-HEAD notebook validation have no execution
receipts. Implementation is not `READY_FOR_REVIEW`. Static review and Git/data
metadata checks do not substitute for these gates.

Two separate legacy RED receipts must expose the missing stocks XLSX and HTTP
403 CSV-shaped acceptance. Helper GREEN uses independent literal fixtures and
CSV/openpyxl readback oracles. Cases include explicit versus compatibility
encoding, 403/HTML, malformed CSV, empty universe, missing/duplicate/new headers,
raw text/duplicates/order/index, source-added-column parity, SUPERTYPE classes,
every resource boundary, serializer/readback failures, no partial return,
logging receipts/barriers, health failure, canaries and native-primary
precedence. Full A-runtime remains distinct from F3 smoke. Store actual command,
source/runtime identities, counts, native exit and retained receipts only after
admitted execution; original failures retain their original status.

The following issue AC are quoted verbatim. “Planned subset” describes code
scope, not completion; all corresponding execution evidence is currently
`NOT_RUN`.

| Issue AC | In A4-01 | Verification | Remaining owner/package |
|---|---|---|---|
| #19: Оба обещанных формата создаются из одного DataFrame и одного release snapshot либо неподдерживаемый формат явно снят с публикации. | Planned candidate subset; no format removed | Four payloads from one input and respective frames | A4-02/D/F: release snapshot and activation |
| #19: Согласованность проверяется по schema, типам, identity и значениям, а не только числу строк. | Planned candidate subset | Independent CSV/XLSX readback of schema/order/index/types/identity/values | D/R2: release and public readback |
| #19: Релиз содержит timestamp и checksum обоих форматов. | Local receipt only | Supplied capture UTC and independent byte hashes | D4/F: release manifest/provider |
| #19: Тест schema addition выявляет расхождение CSV/XLSX. | Planned fixture regression | Added-column parity plus negative divergence oracle | Execution remains NOT_RUN |
| #19: Восстановлены согласованные CSV/XLSX обоих публичных каталогов, с единым snapshot/checksum contract; неизвестная production причина выяснена отдельно. | No | No live/publication claim | R2/D: public restoration/readback; separate production-cause investigation |
| #58: Владелец подтвердил разрешённый и работающий источник каталога на VPS. | No | SOURCE_CONTRACT_QUALIFICATION and SOURCE_FEASIBILITY NOT_VERIFIED | A/source owner: separately authorized target-VPS evidence |
| #58: HTTP-ошибка и HTML не принимаются за CSV; валидируются кодировка, обязательные колонки и непустой состав инструментов. | Planned supplied-input subset | Status/MIME/encoding/schema/nonempty fixtures and refusal | A/source: authority and current-source contract qualification |
| #58: Каталог публикуется только после проверок; при ошибке сохраняется предыдущий снимок. | No publication; helper returns no partial candidate | Candidate refusal and zero target/activation interface | A4-02/D/F: current/previous/crash/readback and pipeline gate |
| #58: После устранения блокера детерминированно проверено по одному инструменту каждого SUPERTYPE и market. | Offline class representatives only | Literal SUPERTYPE fixtures; no unknown-to-shares routing | A5/source: actual SUPERTYPE/market matrix and live evidence |

## OPEN_HOOKUPS

| Hookup | Owner/package | Required acceptance |
|---|---|---|
| Source qualification | A/source, separate approved profile | Owner-approved current official source, redirect/status/MIME/encoding/full-schema evidence and target-VPS feasibility; no access bypass |
| Caller, transport and context | A4-02 with F/coordinator | Explicit source/session profile, supplied bytes/provenance and common logging context through real caller; A1 admission stays separate |
| Candidate provider and release | A4-02/D/F, D4 | Agreed provider/receipt-to-manifest adapter; all required payloads/capacity before current activation; no duplicate writer |
| Preservation and recovery | D/F with A4-02 | Failure before/after commit, current/previous hashes, crash settlement and independent readback; D1 single-file replacement does not supply pair transaction |
| F07 source pin | A/F07 owner, separate reviewed update | Explicit whole-file pin/forward-regression maintenance before editing the existing gathering pair; existing A/F/F07 files unchanged here |
| Literal CSV policy | D3/#35 | Approved escaping and consumer behavior; raw CSV auto-open safety is not claimed |
| Class/market routing | A5/source | Verified actual SUPERTYPE/market representatives and explicit unknown policy |
| Public catalogues | R2/D | Both full/stocks CSV/XLSX URLs restored and independently read back with snapshot/schema/type/identity/value/checksum parity; investigate historical production cause separately |
| End-to-end observability | F/R1 | Common delivery/outcome/nonzero/upload-stop, required-export failures and bounded redacted bundle; local helper receipts are insufficient |

No hook above is silently completed by `candidate_ready`. Issues remain open;
this slice uses `Refs`, with no merge, issue closure, live probe, upload, cron or
production operation.

## Pending execution commands

These are inner payloads for a future qualified outer profile, not standalone
execution authority. Valid exact-source tickets and an aggregate process/storage
profile are required before any project import or collection.

| Purpose | Inner payload after the verified interpreter's `-I -B -X utf8` |
|---|---|
| Focused collection | `-m pytest -c pyproject.toml --collect-only -q tests/A/test_catalogue_snapshot.py` |
| Legacy RED: HTTP refusal | `-m pytest -c pyproject.toml -q tests/A/test_catalogue_snapshot.py::test_legacy_red_403_csv_reaches_current_writers` |
| Legacy RED: four formats | `-m pytest -c pyproject.toml -q tests/A/test_catalogue_snapshot.py::test_legacy_red_valid_csv_has_only_three_current_writers` |
| Focused helper/characterization | `-m pytest -c pyproject.toml -q tests/A/test_catalogue_snapshot.py` |
| Full A collection | `-m pytest -c pyproject.toml --collect-only -q tests/A` |
| Full A runtime | `-m pytest -c pyproject.toml -q tests/A` |
| Existing common gates | `tools/offline_tests.py --lane <f1/f2/flog/f3>` as separate admitted commands |
| Notebook check | `tools/offline_tests.py --notebook-check --ref <exact-final-HEAD>` |

Only the two separate RED invocations use child-only `MDS_A4_LEGACY_RED=1`;
the focused/full ordinary runs leave it unset. No guard or live capability is
enabled by this test-only expectation selector. Default legacy assertions
characterize the still-unconnected caller; they do not claim that it is fixed.
Retain each original failing RED receipt separately from helper GREEN.

Static Git whitespace/scope review and JSON fixture parsing are metadata checks.
They do not establish Python syntax, test collection, execution, real F-LOG
session delivery, cleanup or runtime capacity. No test counts are inferred from
the number of functions or parameters in the source.
