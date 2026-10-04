# Coordination and acceptance — F0-01

## Approved slice

Repository: `foykes/moex-dataset`. Package: `F0`. Current slice: `F0-01`. Approved review version: `R3`.

Approved plan SHA-256: `e6635da5a65f47f4b2fbd051caa5a7040347eb17e90a24f4d055b2d42d2ab92b`.

Implementation branch: `codex/mds-f0-01`. Source base: `5e4eb07fa24e9ecf1c77c2fefb0adfde453ff1c4`.

The exact write allowlist is:

- `docs/dataset_contracts.md`
- `docs/coordination.md`
- `README.md`, for links to these documents only

F0 specifies the minimum contracts needed by later packages. It does not implement helpers, change datasets or runtime configuration, install dependencies, edit workflows, merge PRs, close issues, publish data, or establish production readiness.

References: [schema contract #63](https://github.com/foykes/moex-dataset/issues/63), [all time #77](https://github.com/foykes/moex-dataset/issues/77), [logging #78](https://github.com/foykes/moex-dataset/issues/78), and [Fix Plan v2.1 owner update](https://github.com/foykes/moex-dataset/issues/76#issuecomment-5981425474). The detailed data, time, quality, export, logging and release contracts are in [dataset_contracts.md](dataset_contracts.md).

## Current observations

The [README](../README.md) describes eight legacy 10-/30-year outputs. The [current dataset configuration](../settings/datasets_config.json) and existing filenames remain the observed baseline; the four all-time outputs are additions proposed by the contract.

[pyproject.toml](../pyproject.toml) declares notebook/script pairing. The [pre-commit configuration](../.pre-commit-config.yaml) matches notebooks, so a Python-only edit does not itself establish successful synchronization. Each pair remains one ownership zone.

The current C pairs are [dividends.py](../dividends.py) / [dividends.ipynb](../dividends.ipynb) and [dohodru_data.py](../dohodru_data.py) / [dohodru_data.ipynb](../dohodru_data.ipynb).

[AGENTS PR #1](https://github.com/foykes/moex-dataset/pull/1) is open and non-draft in the reviewed metadata snapshot. It is not treated as merged into this source base. [Research PR #3](https://github.com/foykes/moex-dataset/pull/3) and `docs/research_export_audit.md` remain reserved for the separate research workflow.

These observations describe the inspected source and metadata, not a successful producing run or refreshed market data.

## Ownership and handoff

**H — common transfer condition:** the necessary bootstrap is accepted on an exact HEAD and is included in the base used by the next package; the coordinator explicitly frees the reservation; the next package is separately approved. Its handoff records paths, base/HEAD, interface, evidence and open hookups.

“Current owner” below describes reservation custody. It does not expand the current F0 write allowlist. Every new module, test, fixture, tool or workflow path is explicitly **PROPOSED**.

| Path / glob | Current owner | Next owner | Transfer condition | Tests / fixtures | Consumers |
|---|---|---|---|---|---|
| **PROPOSED:** `docs/dataset_contracts.md`, `docs/coordination.md`; existing `README.md` | F0 | Coordinator | F0 accepted; later changes require a separate PR | Inline examples; no tracked fixtures | All |
| `data_gathering.py`, `data_gathering.ipynb` | F bootstrap reservation | A | H after F3; transfer both pair members | **PROPOSED:** `tests/test_data_gathering*.py`, `tests/fixtures/data_gathering/**` | B/D |
| `settings/datasets_config.json`, `settings/user_agents.json` | Coordinator | A | F0 contract fixed; H | A tests; shared fixtures read-only | A/B/D |
| `all.py`, `all.ipynb`, `settings/datasets_config_1_year.json` | Coordinator reservation | A | Separately approved corresponding diagnostic slice | **PROPOSED:** `tests/test_diagnostic_export*.py` | A/R |
| `tech.py`, `tech.ipynb`; **PROPOSED:** `data_quality.py` | F reservation | B | H; helper only in separately approved B2 | **PROPOSED:** `tests/test_tech*.py`, `tests/test_data_quality*.py`, `tests/fixtures/tech/**`, `tests/fixtures/data_quality/**` | A/D/R |
| `dividends.py`, `dividends.ipynb` | F reservation | C | H; transfer both pair members | **PROPOSED:** `tests/test_dividends*.py`, `tests/fixtures/dividends/**` | D/R |
| `dohodru_data.py`, `dohodru_data.ipynb` | F reservation | C | H; C2/C3 edits sequential | **PROPOSED:** `tests/test_dohodru*.py`, `tests/fixtures/dohodru/**` | D/R |
| `upload.py`, `upload.ipynb` | F reservation | D | H; transfer both pair members | **PROPOSED:** `tests/test_upload*.py`, `tests/fixtures/upload/**` | D/R |
| `main.py` | F2 → F-LOG → F3 | D | Safe import, dev/CI sync, run/log context and offline runner verified; F explicitly releases the file after H | **PROPOSED:** `tests/test_pipeline_gate*.py`; shared import/log tests stay coordinator-owned | A/B/C stage interfaces; D/R |
| **PROPOSED:** `dataset_io.py` | Coordinator reservation | D1 | Separately approved minimal writer slice | **PROPOSED:** `tests/test_dataset_io*.py`, `tests/fixtures/dataset_io/**` | A/B/C/D |
| **PROPOSED:** `release_manifest.py` | Coordinator reservation | D4 | Separately approved manifest/outcomes slice | **PROPOSED:** `tests/test_release_manifest*.py`, `tests/fixtures/release/**` | D/R |
| **PROPOSED:** `run_logging.py` | F-LOG | Coordinator | After F-LOG acceptance; shared maintenance stays coordinator-owned | **PROPOSED:** `tests/test_logging*.py`, `tests/fixtures/logging/**` | All |
| **PROPOSED:** `tests/conftest.py`, `tests/test_contracts*.py`, `tests/fixtures/contracts/**` | F3 | Coordinator | Never transferred to an individual lane | Shared producer/consumer examples | A8/B3/D4 |
| `tests/data_gathering_moex_query_YNDX.csv` | Coordinator reservation | A | H; historical sample, not a universal oracle | A tests | A |
| `pyproject.toml`, `.pre-commit-config.yaml`, `.gitignore`; **PROPOSED:** `tools/notebook_sync.py`, `.github/workflows/ci.yml` | F1/F2/F3 | Coordinator | Shared config/sync/CI stays coordinator-owned | **PROPOSED:** `tests/test_import_safety*.py`, `tests/test_notebook_sync*.py` | All |
| `tests.py`, `tests.ipynb`, `main_tests.py`, `main_tests.ipynb`, `1year.py`, `1year.ipynb`, `count_check.py`, `count_check.ipynb` | Coordinator reservation | Unassigned | Separate scoped handoff required | Automatic collection forbidden | Separate package |
| `docs/research_export_audit.md` | Research / PR #3 | Research | Outside F0 | Research evidence | Research reviewer |

The proposed helpers remain small procedural functions: `data_quality.py` for B2 validation, `dataset_io.py` for D1 writing, `release_manifest.py` for D4 manifests/outcomes, and `run_logging.py` for the shared logging contract. This reservation is not permission to create them in F0 or introduce a framework.

There is one active writer per worktree, branch and file zone. Notebook/script pairs are indivisible. Shared schema/contract interfaces, fixtures, `conftest`, sync and CI remain coordinator-owned.

A change to another owner's interface requires a request containing the path, reason, dependency and integration test. The caller's owner performs its hookup. A duplicate helper is not an ownership workaround.

F0 has no code prerequisites. The next preparation packages are sequential: **F1 → F2 → F-LOG → F3**. A/B/C/D start after the required preparation has been accepted and H is satisfied; each package still needs its own approval.

A8, B3 and D4 develop producer/consumer behavior independently against shared contract examples and future fixtures. Final acceptance later connects real call sites and verifies all 12 outputs, history/seed, keys, schema and parity. F0 does not create a circular requirement for all three implementations to exist before fixture development.

## Acceptance matrix

The following three criteria are copied from [issue #63](https://github.com/foykes/moex-dataset/issues/63). F0 completes only the initial specification contribution; final implementation conformance remains open.

| Issue AC | In F0-01 | Check | Remaining owner/package |
|---|---|---|---|
| Описаны типы, порядок, единицы, NaN и естественные ключи; служебный индекс CSV отделён от данных. | Initial dictionary, legacy ordering/index distinction and natural-key contract; unknown semantics marked explicitly | Source/config/sample evidence and inline examples | Complete export reconciliation: module owners/R1 |
| Зафиксированы timezone, begin/end, границы окна, интервалы 24/60/10/1, незавершённый бар и raw/adjusted prices; неизвестное помечено. | Initial time/window/interval/raw-price contract and explicit unknowns | Owner decisions/source evidence | A5/A8/B2; external evidence |
| Определены версия схемы и политика изменений. Полная жизнь инструмента и seed RSI остаются отдельными задачами. | Initial schema version/change agreement; lifecycle and RSI seed kept separate | Tables/examples | Metadata emission and conformance: R1 |

The PR uses `Refs #63, #77, #78`. It does not use `Closes` or claim full issue acceptance. These issues remain open after F0's initial specification contribution.

### Approved regression matrix

These are future offline oracles, not results of runtime tests performed by F0.

| Scenario | Required oracle | Owner |
|---|---|---|
| XLSX: **1,048,575 data rows**, one header, other parameters admissible | Capacity-preflight **PASS** only; other release gates remain unproved | D3 |
| XLSX: **1,048,576 data rows** plus one header | Capacity **FAIL**, ERROR/summary/nonzero; no current target changes | D3/D4/F-LOG |
| XLSX: admissible/exceeded column limit | Separate PASS/FAIL cases with the same rules | D3 |
| Sheets: confirmed applicable limit `L`; the whole post-export workbook grid equals `L`; other limits admissible | Capacity-preflight **PASS** only; actual export/publication remain unconfirmed | D5 |
| Sheets: same grid/evidence, allocated cells total `L+1` | Capacity **FAIL**, ERROR/summary/nonzero; no clear/write/current changes | D5/D4/F-LOG |
| Sheets: target limit, grid or applicability evidence unknown | `NOT_VERIFIED`; release blocked, with neither false PASS nor proven overflow | D5 |
| Small injected capacities | Same boundary/boundary+1 oracles without giant files; fixture evidence is not live-target evidence | D3/D5 |
| 12 datasets, legacy schema and Google projection | Eight old names/columns preserved; Google remains 10y daily, ten columns without row index | A8/D5 |
| Same key, changed OHLC/end | Revision or quarantine according to evidence; `end` is not part of the key | A6/B2 |
| History longer than 30 years; overlapping windows | Same OHLC/RSI under one snapshot and canonical calculation | A8/B3 |
| Empty / failed / missing | Distinct collection outcomes and reasons | A/C |
| Required child failure; partial/unknown remote | No false success/current; recovery requires readback | D2/D4/F-LOG |

Excel has 1,048,576 rows and 16,384 columns per sheet; see [Microsoft specifications](https://support.microsoft.com/en-us/excel/excel-specifications-and-limits). Google publishes a limit of up to 20 million cells and applicable size constraints; see [Google file limits](https://support.google.com/drive/answer/37603?hl=en). Public Google information is not proof of the actual target's applicable capability.

Sheets preflight includes the entire allocated workbook grid after the operation, retained target-sheet dimensions and every other retained sheet. Effective limits and other constraints of the concrete Google target remain **NOT VERIFIED** until the separately approved profile confirms them.

Predictable required-export capacity failure must be detected before mutation of **any** current target. Unknown capabilities block required promotion without being reported as proven overflow. Small injected capacities are sufficient for offline boundary checks; they do not confirm a real target's capabilities.

## Open decision and separate changes

**DECISION REQUIRED — B2 indicator behavior after quarantine.** Neither continuation nor reset/warm-up has been approved.

Example: clean closes `100, 101, …, 114` produce seed RSI14 = 100. The next observed key has conflicting closes `115` and `180`; both raw variants are retained in quarantine. The next clean close is `110`.

- Bridging the remaining clean bars gives RSI14 approximately `76.47`.
- Restarting a clean calculation segment at `110` gives NaN until 14 new clean price changes are available.

B2 must obtain the explicit mathematical choice before implementing it. Ordinary non-trading gaps are not quarantined observed candles. Independent F0 contracts can proceed while this decision remains pending.

The following AGENTS amendments are proposals for a separate agreed update to existing [PR #1](https://github.com/foykes/moex-dataset/pull/1):

- Move shared logging preparation before parallel A/B/C/D and require instrumentation in each later fix.
- Remove wording that permits skipping mandatory XLSX because CSV is sufficient.
- Replace the 30-year-only slicing suggestion with canonical calculation on maximum approved available history, followed by window selection.
- Require capacity ERROR/summary/nonzero reporting and explicit previous/partial/unknown outcomes.

F0 does not edit `AGENTS.md`, create a competing AGENTS PR, or merge PR #1. Research PR #3 remains separate.

## Definition of done and review handoff

F0-01 is ready for independent review when:

- All changes are inside the exact three-path allowlist.
- Current observations, target contracts, proposals and unknowns are distinguishable.
- The 3×4 dataset matrix, legacy names/order/index behavior, Google 10-year daily interface, ownership/H table and all 12 approved regression scenarios are represented consistently.
- The #63 matrix retains all three original AC and names the remaining integration owners.
- Every main structure, new input, intent and assessment has a field table with types, required/conditional/optional presence, enums and missing/null reasons.
- Every required result field has an explicit source. Pure preflight creates no run identity; the caller constructs the complete export result; the writer returns one artifact outcome for one chosen format.
- ID correlation, canonical schema/order, raw identity/provenance and quarantine reasons are described. Collection/validation/export/publication and preflight/export/readback remain distinct.
- Content-preservation verification does not upgrade previous data-quality verification. Unconfirmed required capabilities never receive PASS.
- Capacity fields extend the general event envelope; privacy covers URL fields, nested fields, exceptions/traceback and bundles. Startup failure requires no ready artifact.
- Inline examples cover all 11 approved families below, including two run/snapshot identities, separate CSV/XLSX artifacts and UNVERIFIED previous with verified preserved bytes.
- Relative Markdown links, tables, code fences and rendered readability are checked; inline JSON parses and satisfies required fields/types/enums, conditional-null rules, coherent IDs and state invariants.
- The exact allowlist, absence of private values and `git diff --check` are verified. Actual commands, exit codes and results are recorded separately from proposed checks.
- No runtime, notebook, configuration, dataset, dependency, CI, tracked fixture, helper or secret change is included. The quarantine decision remains B2's open decision.
- Self-review addresses the exact diff/base/HEAD.

### Required inline example families

These are documentation examples, not executed writers or runtime tests.

| Family | Required distinction |
|---|---|
| 1. Successful canonical frame/metadata handoff | Prepared artifact is separate from verified publication |
| 2. `VALID_EMPTY`, failed request, missing block | Explicitly different collection outcomes |
| 3. Overflow with previous `VERIFIED`, `ABSENT`, `UNVERIFIED` | Previous state is reported honestly |
| 4. `NOT_TOUCHED` | No false readback-verified preservation claim |
| 5. `PARTIAL_REMOTE`, `REMOTE_UNKNOWN` | Honest per-target outcomes |
| 6. Preflight PASS; actual export `NOT_RUN` or `FAILED` | Preflight does not imply export success |
| 7. Unconfirmed capabilities | `NOT_VERIFIED` |
| 8. Same spec/shape/capabilities in two run/snapshot contexts | Equal pure assessments; caller-built results use their own IDs; cross-snapshot artifact rejected |
| 9. CSV/XLSX of one dataset | Different artifact IDs/formats/paths, shared snapshot, separate outcomes; required XLSX failure blocks overall success even with ready CSV |
| 10. UNVERIFIED previous with known baseline; mutating timeout and conclusive readback | `PRESERVED_VERIFIED` content, unchanged UNVERIFIED quality, attempt error retained |
| 11. Ordinary stage/error event | General event envelope |

An untracked validator checks inline JSON parsing, required fields/types/enums, conditional-null rules, coherent IDs and state invariants. The handoff records the actual commands, tools, exit codes and results. Runtime imports, pipeline execution, `tests.py`, live writes and production checks are unnecessary for this documentation validation.

`READY_FOR_REVIEW` means this documentation slice is complete and reviewable. It does not mean pipeline or production readiness. `BLOCKED` identifies required missing evidence/action with an owner. `NEEDS_DECISION` identifies an unresolved choice that prevents the current slice from proceeding; the independent pending B2 decision alone does not block completed F0 documentation.

Runtime red-before/green-after tests, live probes and publication tests are not claimed for this slice. Notebook synchronization is not applicable because no pair is changed; the allowlist/diff check substantiates that scope.

Rollback is a revert of the F0 documentation commit. No current dataset or remote target is changed by this slice.

Fill the following handoff with actual values after implementation. Use `NOT AVAILABLE`, `NOT CREATED` or `NOT APPLICABLE` with a reason instead of inventing evidence.

```text
REPO: foykes/moex-dataset
PACKAGE: F0
CURRENT_SLICE: F0-01
PR: actual URL or NOT CREATED
BRANCH: codex/mds-f0-01
BASE_SHA: actual checked base
HEAD_SHA: actual reviewed HEAD
APPROVED_PLAN: F0-01 R3 and chosen contract decisions
APPROVED_PLAN_SHA256: e6635da5a65f47f4b2fbd051caa5a7040347eb17e90a24f4d055b2d42d2ab92b
CHANGED_PATHS: actual paths
TESTS: actual documentation checks, commands and exit codes
CI: actual checks on HEAD_SHA or NOT AVAILABLE
NOTEBOOK_SYNC: NOT APPLICABLE; no notebook/script pair changed
LOG_EVIDENCE: NOT APPLICABLE; no runtime logging implemented/executed
SCHEMA_IMPACT: documentation only; target changes remain future work
AC_MATRIX: initial specification covered; remaining owners/packages recorded
OPEN_HOOKUPS: F1/F2/F-LOG/F3 preparation; A/B/C/D implementation; R integration; B2 decision
PERMISSIONS_USED: actual approved scope
SELF_REVIEW: findings, fixes and remaining risks
STATUS: READY_FOR_REVIEW / BLOCKED / NEEDS_DECISION
NO_AUTO_MERGE: true
```

Stop after the current slice and its handoff. The next package needs separate approval and a freshly checked base. Findings publication, merge, issue closure, production reload and cron activation are outside this slice.
