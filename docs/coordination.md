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
- Inline examples cover all 11 approved families below, including two run/snapshot identities, separate CSV/XLSX artifacts and UNVERIFIED previous with verified preserved canonical content using declared same-basis normalization (not serialized-byte equality).
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

## F2-01 reservation and integration handoff

F2-01 R2 was explicitly approved on 2026-10-05 from `PLAN (6).md`, SHA-256
`93f525cdfd3ca54b7acd0f6d8113fb3a88fe1a9abd750d50f534a29b536ee1a3`,
independent plan review `PLAN_ACCEPT`. Its branch is `codex/mds-f2-01`; source
base is `683a25ece7a6abe17407aed5eb642bf918e9914a` (F1 PR80 merged).

This approval temporarily transfers the five diagnostic pairs (`1year`, `all`,
`tests`, `count_check`, `main_tests`) to F2 as indivisible ownership zones.
F2 also owns this slice of `main.py`, `tools/notebook_sync.py`,
`.pre-commit-config.yaml`, `.github/workflows/notebook-sync.yml`, `tests/f2/`,
`docs/notebook_sync.md`, this F2 handoff section and the `.f2/` ignore rule.
Other changes to these shared files require a separate handoff. `pyproject.toml`,
settings, datasets, credentials, F1 locks/guards and research PR3 are read-only.
F retains the ten pair reservations until explicit release under condition H;
this PR does not start A/B/C/D or transfer their files automatically.

The safeguard checks all ten pairs on worktree/index/exact source revisions;
only the five approved diagnostic pairs are edited. Development synchronization
does not execute notebooks and never auto-stages or auto-commits. Each commit
attempt captures its current HEAD as `SYNC_BASE_SHA`, distinct from the branch
base. A notebook-only export fails with `RESTAGE_REQUIRED`, preserving the old
index/HEAD until the author explicitly stages both members. See
[notebook sync protocol](notebook_sync.md) for the per-command profile.

Actual hook activation, shared setup and the CI setup approval variable remain
coordinator-owned. Disposable real Git/pre-commit tests do not activate a hook
in the application worktree. A skipped/unavailable CI job is NOT VERIFIED;
required merge/deployment enforcement remains coordinator/F3. Exact tested
source HEAD, actual commands/results, AC evidence and activation status belong
in the review handoff, not inferred from this reservation document.

Open hookups: F2-02 owns #4 and the full public runtime path contract of #45
(including the existing machine root in `1year`). A owns the separate #5
cold-start signature defect. F-LOG owns shared logger integration; F3/coordinator
owns actual activation and required ETL/merge/deployment enforcement. Other OS
evidence remains #67/R1. A8/B3/D4 can use fixtures without circular dependencies.
The eleven guarded module-import probes use the declared launcher root for
legacy library settings reads; their offline profile disables urllib3's optional
IPv6 loopback capability probe. They do not prove foreign runtime-root behavior
or authorize live diagnostic wrappers. Public functions, schemas, column order,
filenames, RSI calculations, Google targets and publication policy are preserved.

This slice references #44/#45/#46 and audit #76; it does not close these issues,
implement F2-02/#5/F-LOG/general F3, merge, deploy, enable cron or write remote
datasets. PR1 is not duplicated. Stop after the independent F2-01 code review
handoff; later slices need new prompts and verified bases.

## F-LOG-01 reservation and integration handoff

F-LOG-01 R3 and its independent plan review were approved in this chat; the
human explicitly authorized implementation on 2026-10-06. The 2026-10-07
review-fix request binds APPROVED_PLAN to F-LOG-01 R3 with SHA-256
`866499c4c289346929cf3938e7d3acd2df71b4941a0a864a42ea9723352ae776`.
This hash is the human-supplied reviewer reference; original R3 byte-artifact
hash recomputation is NOT VERIFIED. R1/R2 attachments remain source evidence.
Branch: `codex/mds-f-log-01`. Checked source base:
`c4f2210e83447d2c72f32a2adb1a3834349390c2` (F2 PR81 merged). F0 contract blob:
`19c4d9d5786ea5bf66a0d42f1f88ff37ee5d7adc`.

This approval is the condition-H transfer of only these shared zones:
`main.py` logger bootstrap/boundaries/observations/safe CLI, this appended F-LOG
handoff section and the `.f-log/` ignore rule. Parent is the sole writer in this
isolated worktree; delegated reviewers are read-only. Existing F0/F2 reservations,
the ten notebook pairs, settings, credentials, datasets, F1 environment/guards,
CI/setup approval controls and unrelated PRs remain outside the slice.

The exact 15-path allowlist is:

```text
run_logging.py
main.py
tests/logging/conftest.py
tests/logging/_probe.py
tests/logging/test_contract.py
tests/logging/test_redaction.py
tests/logging/test_multiprocessing.py
tests/logging/test_failures.py
tests/logging/test_entrypoint.py
tests/logging/test_bundle.py
tests/logging/test_overhead.py
tests/logging/fixtures/capacity.json
docs/logging.md
docs/coordination.md
.gitignore
```

F-LOG supplies the procedural logger, bounded single parent writer/producer ACKs,
redaction before IPC, health gates, parent-local stage timings, summary-last
sealing and explicit immutable bundle export. See [logging protocol](logging.md).
Stage return, execution result, delivery result and publication verification
remain distinct. No-argument `main()` preserves its existing contract.

Issue #78 acceptance is tracked as a foundation plus remaining hooks, never
closed automatically:

| AC family | This slice's evidence | Remaining owner/hook |
|---|---|---|
| Stage lifecycle/progress/correlation | Five main boundaries and exact timing ledger | F-LOG-02/A/B/C/D internal progress and snapshot links |
| Ticker/interval/page/file and empty/failure cause | F0 envelope/null/retry/error fixture checks | A/B/C actual collection/quality paths |
| Exit, children and publication truth | Actual Windows spawn/exit/ACK faults; no inferred release | D/R coordinator publication aggregation |
| DEBUG detail without output schema change | Independent levels, console/file/protected checks | Internal producer adoption |
| Retry/schema/page/revision/quarantine/capacity/transfer/rollback | Synthetic shared error/capacity tests | A/B/C/D domain fixtures and runtime hooks |
| Privacy in all owned evidence | LogRecord/IPC/console/JSONL/error/ZIP canaries | Adopt producers; manually review sharing |
| Multiprocessing safety | Two real spawn workers; missing/lost/stale/cross/duplicate/late finals | Real ETL worker wiring |
| Bounded resources and failures | Queue/disk/flush/close/rotation/path/budget/summary fixtures | Deployment filesystem/load evidence |
| Diagnostic bundle | Sealed snapshot, byte hashes, privacy/mutation checks | Manual review and support handoff |
| Measured overhead | Delivered synthetic 2,000-event median/baseline | Representative ETL overhead |
| Separate staging acceptance | Offline fixtures only | E3/R3 credential-isolated staging; no production inference |

Actual exact tested HEAD, draft PR URL, changed-path verification, test commands/
exit codes, CI checks on that HEAD, environment hashes, notebook checks and
independent review evidence are recorded in `.f-log/evidence/HANDOFF.md` and
the PR body. This ignored evidence is local and does not enter the public
dataset manifest. Missing/skipped CI is NOT VERIFIED, not PASS.
The PR82 review fixes target reviewed HEAD
`011a9d8bf87a77a9b8b99bb94221127019c7448f`: common credential families,
admitted-operation/freeze ordering in parent and worker, and protected INCOMPLETE
retention. The repeat-review handoff includes a portable redacted evidence ZIP
with raw report structure, before/after hashes, regression results, overhead,
actual fixture logs/summaries and a sealed diagnostic bundle. Export manifests
distinguish original local bytes from redacted sharing copies.

This slice leaves #78 open. It does not merge, deploy, run the production CLI,
activate hooks/cron/CI, import the real pipeline for smoke tests, contact MOEX/
FTP/Google or enable paid services. Rollback is a revert of the F-LOG slice;
datasets and remote targets are unchanged. Later integration requires its own
prompt, freshly verified base and condition-H ownership transfer.

## F3-01 reservation and review handoff

The human authorized implementation of F3-01 R3 in this chat. Source base:
`07945ed4ded747145b40d7a7c25e8ce97abeaa0d`, tree
`381f1f83ab74ac549c39f3a77d0570714c350c19`; branch `codex/mds-f3-01`.
The owning source root is a new isolated worktree. Absolute local paths belong
only in ignored evidence, not this repository. R3 plan artifact SHA-256:
`7d443da6bed980604685a24f0f4433186ef978e260b58590394ac40b8a563484`.

The exact 30-path write reservation is:

```text
.gitignore
pyproject.toml
conftest.py
test_offline.py
tools/offline_tests.py
tools/offline_guard.py
tests/conftest.py
tests/shared/test_collection.py
tests/shared/test_guard.py
tests/shared/test_children.py
tests/shared/test_preparation.py
tests/shared/test_logging_contract.py
tests/fixtures/shared/candles.csv
tests/fixtures/shared/ticker_dates.json
tests/environment/_safety.py
tests/f2/_probe.py
tests/f2/test_notebook_sync.py
tests/logging/_probe.py
tests.py
tests.ipynb
main_tests.py
main_tests.ipynb
.github/workflows/offline-tests.yml
.github/workflows/notebook-sync.yml
docs/testing.md
docs/coordination.md
README.md
tests/f2/conftest.py
tests/logging/conftest.py
tests/logging/test_overhead.py
```

The three additions to R1 are exactly the two native conftests and logging
overhead test. Their changes only route/validate current-run evidence; native
workloads/assertions remain intact. Production notebook-sync, logger,
calculations, settings, dependency locks and dataset interfaces remain read-only.
The indivisible mutable pairs are `tests` and `main_tests`; all ten pair names
and all 20 members are checked, including `upload`. The eight other pairs remain
unchanged. Existing worktrees, environments, secrets and evidence are preserved.

F3 implements test-only guards, fixed independent native lanes, legacy quarantine,
run-bound fail-closed descendant reports, exclusive destinations and shared
literal smoke. See [offline testing](testing.md). Expected faults are scoped;
native F-LOG timing and F1/F2 guards are not replaced by the F3 workload policy.
Initial local failures remain evidence, not reclassified success. The handoff
records actual clean tested SHA, manifest digest, commands/counts/exits/skips,
guard graph/counters, previous hashes, exact pair inventory and check URLs.
Unavailable execution evidence is NOT AVAILABLE or NOT VERIFIED.

Implementation/local offline validation does not grant workflow activation or
hosted network SETUP. Those two gated workflows require separate approval before
the first triggering push. The setup scope is fresh standard Windows hosted
runner, exact checkout, the existing CPython 3.14.8 archive hash/pip 26.2.1 and
hash-locked dev wheels, then immutable OFFLINE. No production secrets or paid
resources. Required-check enforcement needs later owner permission after actual
successful exact-head checks; branch protection/rulesets are unchanged.

### Literal AC #47 and residual work

Issues [#47](https://github.com/foykes/moex-dataset/issues/47) and
[#78](https://github.com/foykes/moex-dataset/issues/78) remain **OPEN**.

| Literal AC #47 | F3 contribution | Residual acceptance |
|---|---|---|
| Default pytest/unittest запускает содержательные deterministic tests без сети и productionwrites. | Actual default commands, guards, positive disposable I/O and reports | Successful local/exact-head execution evidence |
| Live/staging probes отдельно помечены и требуют явного разрешения. | Import quarantine, strict markers and fail-closed CLI gates tested with doubles | Separate approved bounded profiles/targets |
| CI выполняет coverage/pagination/update/RSI/publication fault regressions и notebook sync check на точном SHA. | Four lane checks and exact-set notebook check prepared | Hosted activation plus reviewed A/B/C/D integration and executed domain matrix |

F connects real reviewed A/B/C/D regression paths in a separately scoped integration
change: namespace/guard compatibility, default collection and CI targets, actual
local and exact-head checks. Unconnected regressions are NOT CONNECTED / NOT RUN;
placeholders do not count as coverage. A8/B3/D4 final acceptance does not create a
circular prerequisite for this bootstrap.

Open hookups: A — source/history/universe/coverage/pagination/update and A8;
B — indicators/warm-up/window/RSI and B3; C — dividends/catalog/source;
D — publication/capacity/fault/recovery/previous/readback and D4;
F — hookups/config/check enforcement/domain logging; R — independent review and
final end-to-end acceptance. Known domain gaps (#6/#20/#21/#22/#24, metadata #11)
are not fixed by smoke. Malformed response structure does not establish calendar
date validation. #78 foundation/redaction smoke does not establish every domain
failure's logging context, capacity ERROR/nonzero or production observability.

F-ready requires the implemented slice, unchanged native assertions, local
evidence and successful current exact-head CI/notebook checks. Final #47 also
requires all domain AC regressions. Production requires separately approved
targets/diagnostics/deployment/readback/recovery evidence. No merge, issue closure,
production invocation, next slice or automatic reservation transfer occurs here.
Rollback is a separately authorized thematic revert; retained evidence,
environments, worktrees and secrets are not deleted.

PR #83 independent review of `439874348f2fbb33a2c8e3bde84935982f463929`
requested F3-CODE-01/02/03 changes: exact native Git admission, truncate/FD
mutation controls and source-inventory checks before fingerprint reads.
Corrections stay in F3-01 with preserved commits, environments and evidence.
The handoff records Windows red/green regressions and new-head local/hosted
checks. Its next gate is READY_FOR_REPEAT_REVIEW; guard acceptance requires the
new independent code review. The residual AC matrix and issue states above
remain unchanged.

## F3-ADMISSION-01 reservation and A1 handoff

The repository owner assigned this narrow F bootstrap to the current executor.
Branch: `codex/mds-f3-admission-01`. Actual source base:
`ebc66fc376a2077ce027cf33689aae31728709c2`. The separate worktree preserves the
existing preparation checkout, reservations, environments and evidence.

The exact write reservation is `tools/offline_tests.py`,
`tests/shared/test_collection.py`, `tests/A/test_admission_smoke.py` and this
appended section. Existing coordination sections and owners remain unchanged.
The runner admits explicit A directories and regular `test_*.py` files/node
selectors through the existing precollection guard and ReportPlugin. Parent
traversal and A aliases are refused. Shared/legacy fixture allowances remain;
the fixed lanes, default discovery, CI, guard and domain code are unchanged.

Use the existing verified immutable dev environment and child-only F3 profile:
disable plugin autoload, clear inherited pytest/Python/ticket/Git overrides and
credential variables without printing them, and keep TEMP/TMP/evidence in the
owning worktree. No dependency installation or credential copy is required.

```powershell
& $F3Python -I -B -X utf8 -m pytest -c pyproject.toml --collect-only -q tests/A
& $F3Python -I -B -X utf8 -m pytest -c pyproject.toml -q tests/A/test_admission_smoke.py
& $F3Python -I -B -X utf8 -m pytest -c pyproject.toml -q tests/A/test_admission_smoke.py::test_admission_smoke
```

The smoke verifies infrastructure admission only. It establishes no A1 domain
AC, metadata validity or production readiness. Local handoff evidence records
original refusal, source mode/digest, actual commands/counts/exits, preserved
negative/default/legacy controls, guard reports, environment fingerprints,
exact-HEAD F3/F2/F-LOG and ten-pair notebook checks, self-review and rollback.

A1 R3 and its six-path allowlist remain unchanged. A1 starts only after independent
bootstrap acceptance, separately authorized merge and verification of its actual
base/drift. Explicit admission does not add A tests to default or hosted CI.
Rollback reverts only this bootstrap before downstream adoption; after adoption,
coordinate its removal with dependent explicit A test callers. Preserve evidence
and environments. Refs #47, #78; no merge, issue closure or production operation.
