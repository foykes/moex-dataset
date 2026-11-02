# MOEX Dataset: start here

Handoff date: 2026-11-02. Repository: [https://github.com/foykes/moex-dataset](https://github.com/foykes/moex-dataset). Default branch: `main`.
Transfer status: **PARTIAL_HANDOFF**. The public source checkpoints and documentation have been published and verified at exact refs. Private evidence still requires a separate secure transfer; full migration remains partial until the recipient receives it and the old host-state boundaries are resolved. Existing public source branches remain available. No product readiness or F3 completion is claimed.

This Python scripts/notebooks project acquires MOEX ISS raw OHLCV, catalogues and dividends, computes TA-Lib indicators and prepares CSV/XLSX plus legacy Google/FTP publication. It is an existing application under incremental repair, not a new architecture project. The historical datasets in Git are observations, not a newly produced release.

The verified public main is `e7800add6ba700f37656cba3517b4e6db83ca413`, tree `95a0e09b19be7350f2af6cb229d7486a242ae16b`. The original local branch named `main` is deliberately still `5e4eb07fa24e9ecf1c77c2fefb0adfde453ff1c4`. Do not confuse them. PR #120 is already merged into public main. PR #1 with AGENTS remains OPEN and non-draft; it was not silently adopted.

## Read in this order

1. [Project agent instructions](references/AGENTS.md), then [architecture and owner decisions](ARCHITECTURE_AND_DECISIONS.md).
2. [Development status](DEVELOPMENT_STATUS.md) and [blockers](BLOCKERS_AND_RISKS.md).
3. [Repository inventory](REPOSITORY_INVENTORY.md), [manifest](TRANSFER_MANIFEST.json) and [GitHub snapshot](GITHUB_SNAPSHOT.json).
4. [Restore instructions](RESTORE_ON_NEW_MACHINE.md), [validation](VALIDATION_REPORT.md) and [private transfer dependencies](LOCAL_ONLY_MATERIALS.md).
5. [Coordination](../../coordination.md), [dataset contracts](../../dataset_contracts.md), [environment](../../environment.md), [testing](../../testing.md). Read their dated amendments, not just their opening historical sections.
6. [Audit/backlog #76](https://github.com/foykes/moex-dataset/issues/76), including the owner decisions of 4 and 13 October and scoped A3 closeout of 24 October. These are accessible without old chats.

## Repository map

| Area | Main files | Current boundary |
|---|---|---|
| Acquisition / A | `data_gathering.py` and paired notebook; `settings/` | A1/A2/A3 integrated; A4 detached draft; source coverage still qualified separately |
| Indicators / B | `tech.py`, `data_quality.py`, paired notebook | STOCH/TRIX fixes and B2 subsets integrated; B2-03 reference ledger remains draft |
| Dividends / C | `dividends.py`, `dohodru_data.py`, paired notebooks | Identity/network/parser subsets integrated; U/Q/E/R0 runtime slices are draft |
| Publication / D | `upload.py`, `dataset_io.py`, `main.py` | Atomic single-file helper integrated; Windows D2 is frozen WIP; Linux D2 draft is stacked on #117 |
| Foundation / F | `run_logging.py`, `tools/offline*`, requirements, CI | Initial F0/F1/F2/F-LOG/F3 integrated; later F3 timing/storage remediation remains incomplete |
| Release / R | contracts, source evidence, public dataset restoration | Caller/coverage/staging/release acceptance still separate |

## What was preserved

The inventory covers 68 original registered worktrees, 6 additional independent checkouts and 67 original branches in the shared repository. No original worktree was removed. There were 13 modified tracked source files plus 11 untracked source files outside the primary checkout's mixed evidence area. They are preserved in three exact-byte WIP commits:

| Branch | Commit | Meaning |
|---|---|---|
| `handoff/d2-windows-wip-20261102` | `c55768dc0e3d8d84c4cd86f4e25d29b8a4964e55` | All 15 frozen D2 files, including both shared tools and the upload pair |
| `handoff/d2-fixtures-wip-20261102` | `042c606a97987e135653df61f54d04e3cb3e65c8` | Six earlier uncommitted prerequisite fixtures; distinct from reviewed #117 |
| `handoff/a4-earlier-wip-20261102` | `af5669ba47dba301dcbe413d5b63f4b75bbd677b` | Three earlier orphaned A4 files; distinct from authoritative #126 |

The source directories, original HEADs and original indexes were preserved. These commits establish custody, not accepted implementation or runtime proof. Historical F3 and C1 previews, B2 preparation and five unreachable F3 revisions have separate exact anchors; see the inventory. Do not merge checkpoints into one combined implementation.

## First safe actions

Verify Git refs and the manifest, inspect PR/issue status, obtain any required private evidence and choose a small owner-approved next slice. Do not replay archived commands. No new F3 qualification, A4 runtime, source capture or publication was performed for this handoff. Canonical test execution requires its owned environment and admission contract; a new computer is not automatically admitted.

No merge, issue closure, force push, rebase, reset/clean, branch/worktree deletion, environment upgrade, production pipeline, cron, live MOEX test, Google/FTP write or automatic next-slice resume is authorized. Do not increase old F3 resource budgets or treat an old owner grant as transferable. D2's `offline_tests.py` and `offline_guard.py` form one custody unit. Notebook/script pairs remain indivisible.

The project budget is zero. Credentials stay outside Git and must be transferred separately by the owner using an encrypted channel. Original host-specific FileIds, processes, locks and storage settlement cannot be restored from a clone. Missing proof remains UNKNOWN/NOT_VERIFIED. Use [NEW_AI_BOOTSTRAP_PROMPT](NEW_AI_BOOTSTRAP_PROMPT.md) to begin on the next computer.
