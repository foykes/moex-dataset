# Blockers and risks

## Transfer blockers

1. **Original public-push approval blocker resolved.** After the automatic rejection, the owner explicitly approved the concrete set. Normal explicit-ref push and exact remote readback were completed; see the manifest/publication receipt. No workaround or force push occurred. Private evidence/host-state limitations below remain.
2. **Private evidence is not on GitHub.** Raw evidence, owner receipts, machine/process metadata and historical transfer archives require encrypted owner-to-owner transfer. Their absence blocks exact receipt revalidation and some continuation work. Source checkpoints and public summaries do not replace those originals.
3. **Original host identities are nonportable.** Old NTFS identities, accounts/ledgers, storage claims, OS process lifetimes and outstanding custody cannot be recreated by copying files. Any new host needs new admission and explicit continuation decisions.

## F3: preserve the actual unfinished state

Initial F3-01/PR #83 was merged. The later unfinished scope is timing/native completion/provenance/retention/storage recovery. Committed U, provenance, aggregate and retention branches are separate historical stages. They are not an applied, qualified main-bound remediation.

The retained F3 transfer report of 2 November says: R8 read-only observations and narrow source-only controls passed in their stated scope; they do not settle the older failed console/runtime/storage attempt. Original missing caller/driver exits remain UNKNOWN. A recorded `P2_ELEVATED_CONSOLE_READY_TIMEOUT` occurred before the sole parent ACK; the downstream authorized DiskPart/storage worker route did not execute for that attempt. This negative fact does not establish all current storage noninterference or a mandatory reboot. These are **HISTORICAL_REPORT** facts read from the retained handoff; primary old-host execution was not independently replayed here.

The private six-path composition is NOT_READY_TO_APPLY. `RECOVERY_BUSINESS_LITERAL_EXECUTOR_UNBOUND_R3.ps1` is source-only/UNBOUND; its original 28 assertions have no runtime qualification. The main-bound passive-producer patch and parameterized original-storage patch are UNAPPLIED. BIND01-05 and QUAL01-03 remain unresolved. Original operational and engineering caps/ledgers are not changed by migration. Do not resume consumed R7/R8, PrepareOnce, RUN_CURRENT, RUN_CONTINUE, OWNER_LAUNCHER, PREPARE or provisioning scripts.

The paired `offline_tests.py` / `offline_guard.py` custody receipt was read locally: HANDBACK_ACCEPTED, source editing FROZEN_NOT_AUTHORIZED, correction H PROPOSED_NOT_GRANTED, no automatic adoption. A new snapshot branch is preservation, not a new implementation grant. The current F3 operational consumer release remains incomplete.

### Hosted failed attempts, still failed

All rows below were refreshed from the Actions API. Their continued existence is confirmed; only two failed job texts and current-main F3 text were re-read in full during migration. Raw leaf artifacts for the selected failures are absent from the API.

| Run | Exact source SHA | Date UTC | Current result |
|---|---|---|---|
| [37736345267](https://github.com/foykes/moex-dataset/actions/runs/37736345267) | `caa729fb4de1d380e7bdf9c488a68d6ee099cb6d` | 2026-10-08T06:12:28Z | FAILED |
| [39066449556](https://github.com/foykes/moex-dataset/actions/runs/39066449556) | `ee5f59d8d58db3dce918594b723a9320ea55283d` | 2026-10-17T20:22:42Z | FAILED |
| [39066780910](https://github.com/foykes/moex-dataset/actions/runs/39066780910) | `e4258f7148cb15aa202a52f5fa549674c6b9c598` | 2026-10-17T20:27:24Z | FAILED |
| [39068234814](https://github.com/foykes/moex-dataset/actions/runs/39068234814) | `44e810e9097c22faabafb097cf099eded3e5febb` | 2026-10-17T20:47:55Z | FAILED |
| [39068338338](https://github.com/foykes/moex-dataset/actions/runs/39068338338) | `60f3569b54f005127bfcbf729e9032ac587a10af` | 2026-10-17T20:49:26Z | FAILED |
| [39069567571](https://github.com/foykes/moex-dataset/actions/runs/39069567571) | `44e810e9097c22faabafb097cf099eded3e5febb` | 2026-10-17T21:06:58Z | FAILED |
| [39069597809](https://github.com/foykes/moex-dataset/actions/runs/39069597809) | `60f3569b54f005127bfcbf729e9032ac587a10af` | 2026-10-17T21:07:24Z | FAILED |
| [39328322291](https://github.com/foykes/moex-dataset/actions/runs/39328322291) | `a689225e4773986b509576b8ee8ec5392198764f` | 2026-10-19T21:32:43Z | FAILED |
| [39583884979](https://github.com/foykes/moex-dataset/actions/runs/39583884979) | `ae8466f84dd6b4d950207e2ffbea4b2f7f39caf2` | 2026-10-21T10:20:35Z | FAILED |
| [39584675507](https://github.com/foykes/moex-dataset/actions/runs/39584675507) | `e3d4d2af23d217af54683ca7a1141ce77276ea12` | 2026-10-21T10:27:06Z | FAILED |
| [39677296604](https://github.com/foykes/moex-dataset/actions/runs/39677296604) | `925b0b8ad0e4c740c493b8a1abd59451462876fc` | 2026-10-21T21:15:30Z | FAILED |
| [39856307283](https://github.com/foykes/moex-dataset/actions/runs/39856307283) | `577d46fe13be24934e0de4a7b6a662fded69d285` | 2026-10-22T21:19:28Z | FAILED |
| [39916598692](https://github.com/foykes/moex-dataset/actions/runs/39916598692) | `1d7f78823ec74e11422b67e2505b4ee07ef5f6dd` | 2026-10-23T07:15:38Z | FAILED |
| [40291813670](https://github.com/foykes/moex-dataset/actions/runs/40291813670) | `bf63a6469678b253f6a4a647f6222fff98c017ce` | 2026-10-26T08:22:28Z | FAILED |
| [40386859657](https://github.com/foykes/moex-dataset/actions/runs/40386859657) | `9026d8715abe7c86cb5cdfaf6cff0e184bd1169a` | 2026-10-26T19:27:09Z | FAILED |

### Raw text independently read during migration

| Run/job | Native evidence in job text | What it does not prove |
|---|---|---|
| [40291813670 / 121469052402](https://github.com/foykes/moex-dataset/actions/runs/40291813670/job/121469052402) | At A4 HEAD bf63a646: 2 failed, 1612 passed, 2 warnings, 761.07s; guard 86. Failed default-command console-pytest and unittest-discover. Reconciliation MISSING_FINAL_REPORT,TIMEOUT,UNEXPECTED_COUNTERS,UNEXPECTED_EXIT. Post-run environment/source step success. Artifact API empty. | No authenticated leaf OS exit/counter type/stuck phase. TIMING_CAUSE_NOT_VERIFIED; no direct A4-helper fault established. |
| [39583884979 / 119167882148](https://github.com/foykes/moex-dataset/actions/runs/39583884979/job/119167882148) | Synthetic integration ae8466f8: 1 failed, 1613 passed, 2 warnings, 755.06s; guard 86. unittest-discover failed with the same reconciliation set. Artifact API empty. | Not a #117 current-main combined PASS; no primary leaf settlement/cause. |
| [39857336792 / 120098856087](https://github.com/foykes/moex-dataset/actions/runs/39857336792/job/120098856087) | Actual current main e7800add: 1614 passed, 2 warnings, 455.36s; guard 0. Other Windows lanes and notebook jobs success in API. | Does not repair earlier failing runs or qualify later F3 recovery drafts. |

### What the next executor needs

An owner-approved continuation route and exact source/H, original failed-attempt primary receipts, actual old/current host settlement/storage disposition, qualified runtime/process/original-output/storage profile and field-level source bindings. Any remediation must preserve original assertions, negative oracles and historical failures. Do not poll for a profile, retry CI blindly, increase timeouts, weaken checks, manufacture a ticket/pin or apply a draft to escape admission.

## D2 and D1

The retained Windows D2 full regression at working-tree digest `31a2ac72d17be3eb0f34c8ef5226eb7cd73c47f900e1261da03b8f19c5139bbe` collected 221 and had 216 PASS / 5 FAIL, runner 86. Earlier 174/2, 2/2 and 57/77 scoped successes are separate source-bound observations. The full run is not green. Native errno, winerror and NTSTATUS remain separate fields. The original receipt explicitly records errno=13 and winerror=5 for its own replacements; this cannot be generalized to another run with errno alone. Missing physical-unsettlement proof stays NOT_ESTABLISHED.

D1 is a single-file primitive. It is not a CSV/XLSX snapshot transaction or remote release, and no production caller hookup is accepted. D1_STAGE_B=NOT_MET. #117 source-level acceptance does not release Windows D2, #122 combined runtime or Stage B.

## A4, B2 and C lanes

A4 local helper RED/GREEN/full-A execution remains NOT_RUN and source feasibility/qualification NOT_VERIFIED. Its hosted shared F3 failure is real. Earlier A4 orphan files are preserved as historical work; do not overwrite #126 with them.

B2 old draft #111/#112 and post-P0 draft #116 coexist. Their head identities and acceptance are different. Historical RSI14 correctness on selected original input is not complete current source-backed reference acceptance.

C2 U/Q/E/R0 and I1/I2/I3 are distinct source/evidence/runtime contracts. R0 has FAILED F3. Merged C3 parser coverage does not prove a complete live source snapshot, D/F connectivity, publication atomicity or #33 AC3.

## Remaining uncertainty

No fresh live MOEX, Google, FTP/VPS or public dataset probe was performed. Full Linux/macOS application acceptance, source rights, historical Git dataset secret audit (#69), large workbook visual QA and staging remain separate work. Existing committed dataset history was not re-audited for secrets or freshness; new commits add no generated datasets.


## Private census boundary

The additional metadata-only scan found 84 retained junction fixture directories, with targets under the original worktree tree. It also retained 269 missing and 37 denied directory observations in sandbox/profile evidence. No access-control repair or path recreation was attempted. These descendants cannot be described as completely transferable; the raw private scan and existing original F3 delivery remain available to the owner.


## Two new historical-workflow failure records

The transfer's approved publication produced two completed LNX workflow failure events with zero jobs/check runs: [41409851740](https://github.com/foykes/moex-dataset/actions/runs/41409851740) and [41409874319](https://github.com/foykes/moex-dataset/actions/runs/41409874319). Their precise reason remains NOT_VERIFIED. No canonical/qualification test, workflow repair or rerun was performed. Original remote codex/lnx-02 and main still match their prior SHA. See ACTIONS_SIDE_EFFECTS.json for exact identity and the correction to the pre-push static estimate.
