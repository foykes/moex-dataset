# LNX-03 Linux application admission

LNX-03 is Stage A of `LINUX_FIRST_FEASIBILITY / PLAN_V1.1`. It adds one
closed application-admission consumer to the accepted LNX-02 environment
foundation. It does not implement D2 publication or establish Linux production
readiness. Windows D2 remains frozen and BLOCKED with its original environment,
221-node ledger, red-before artifacts and handoffs intact.

## Source and ownership

The owner directly transferred exactly these 12 paths for Stage A:

```text
.github/lnx02/verify_env.py
.github/workflows/d2-linux-offline.yml
tools/offline_linux.py
tools/offline_guard_linux.py
tests/linux_bootstrap/conftest.py
tests/linux_bootstrap/native_worker.py
tests/linux_bootstrap/test_environment_contract.py
tests/linux_bootstrap/test_admission.py
tests/linux_bootstrap/test_spawn_contract.py
docs/d2_linux_ci.md
docs/testing.md
docs/coordination.md
```

The original candidate `15a7b6974b521e62d414a956f7f751108a24742a` and its base
`e411a154b47446997fd4697c5cb4d7cc604519bf` remain preserved in history and
evidence. The owner authorized a controlled merge of compatible main
`c98664b7da20375cf0bfed8125a94d64008d0d63`, producing integration checkpoint
`70a6dbd502be500eb908ee1905ac383d53088d3b`. Implementation stays in the separate
`codex/lnx-03-d2-bootstrap` worktree. Each subsequent tested HEAD has its own
exact source/base grants and evidence; the integration checkpoint is not a
claim that later source was tested.

The 2026-10-15 owner night grant supersedes the earlier first-push STOP only
for this Stage A. It permits bounded branch pushes, the existing Linux workflow,
public hash-locked SETUP, conditional source-attested supervised namespace and
existing Windows side-effect checks. The window ends at 2026-10-16 09:00 MSK.
Limits are six Linux executions, four repair cycles, three base integrations,
one draft PR and two additional Windows checks. Every discovery and rerun attempt
counts as a Linux execution. No merge, issue closure, production or Stage B is
authorized.

LNX/coordinator transfers only the new consumer in the shared helper; F3/
coordinator transfers only Linux admission. Existing LNX-02 modes/grants/schema,
Linux dependency locks, Windows tools/workflows, F-LOG APIs and historical
coordination entries retain their contracts. `run_logging.py`, `dataset_io.py`,
upload code, production settings and datasets are outside this slice.

## Closed command and grants

The external consumer command is fixed:

```text
<approved-python> -I -S -B -X utf8 .github/lnx02/verify_env.py linux-bootstrap
  --repo-root <exact-checkout> --output <fresh-job-owned-output>
  --expected-sha <40-lowercase-hex> --expected-base <40-lowercase-hex>
```

It is not a module/script/command/callback launcher. Unknown modes and extra
arguments are rejected. The only application lane is `linux-bootstrap`, with
these three fixed pytest targets and no forwarded pytest arguments:

```text
tests/linux_bootstrap/test_environment_contract.py
tests/linux_bootstrap/test_admission.py
tests/linux_bootstrap/test_spawn_contract.py
```

Only the registered roles in `tests/linux_bootstrap/native_worker.py` may run
as native fixture children, with exact argument and source-bound tickets.

Three distinct non-secret repository grants are required where applicable:

| Grant | Authority |
|---|---|
| `MDS_LNX03_ADMISSION_GRANT` | Exact source and closed consumer authorization |
| `MDS_LNX03_SETUP_GRANT` | Public hash-locked package SETUP for that identity |
| `MDS_LNX03_SUPERVISED_NETNS_GRANT` | Separately approved hosted-only namespace fallback |

The admission JSON has exactly `approved`, `consumer`, `repository`, `sha`,
`base`, `event`, `ref`, `actor`, `triggering_actor`, `workflow`, `job`,
`run_number`, `run_id`, `attempt` and `ticket_id`. These are exactly 15 keys.
Approval is JSON `true`; run number is a positive JSON integer, run ID is a
positive decimal string, attempt is integer `2`, ticket ID is 64 lower-case
hexadecimal characters, and the remaining values are strings. SETUP adds only
`capability: "network-setup"`; supervised authority adds only
`capability: "supervised-netns"`. All shared identity fields must match.
Only `foykes/moex-dataset`, owner actors,
the new workflow and its `linux-bootstrap` job are admitted. Push is restricted
to `refs/heads/codex/lnx-03-d2-bootstrap`; a later separately granted dispatch
uses `main`, with base equal to SHA. Only exact attempt two is admitted.

A run number is never predicted or reserved. Each new exact source first has
an identity-discovery attempt with no active LNX-03 grants. Attempt one always
refuses before checkout and SETUP, including when stale grants are present.
That actual failed run is not Linux application evidence. The external owner-
authorized controller reads its actual ID, number, attempt, source, branch,
workflow and actors from the Actions API, then writes and reads back all
required matching grants for exactly attempt two of the same run. It requests
only that rerun and verifies the actual API identity again. Attempts one and
three or later, foreign runs and any mismatch remain fail-closed. There is no
mid-job grant refresh, implicit retry or empty commit to obtain another number.
The consumer's exclusive job-owned claim survives success and failure until job
teardown; changing an output directory does not make a consumed ticket reusable.

Only the externally verified owner identity may use the delegated repository
Variables write capability. The CI job remains `contents: read`; it cannot
write or reissue grants. Missing Variables write authority blocks hosted work.
The controller revokes the three LNX-03 grants in `finally` and verifies
readback. Revocation failure requires a separate security BLOCKED handoff.

The workflow gates before checkout or package traffic. Checkout/artifact
transport are identified control-plane traffic; admission is not permission
for package SETUP. Missing SETUP grant causes refusal before DNS/download.
Rootless namespace denial cannot activate supervised mode without its separate
grant. LNX-03 grants do not authorize old LNX-02 modes, and LNX-02 grants do not
authorize this consumer. The job cannot create or extend grants.

## Immutable base and compatible main drift

Source admission requires a clean exact HEAD, an existing immutable approved
base in its ancestry and a candidate diff containing only recorded A/M changes
in the exact 12 paths. Other statuses, including D/R/C/T, are refused; disabling
rename detection exposes a rename's removed source as D. The workflow performs
independent literal Git checks; the helper
performs its separate LNX-03 source check. Legacy LNX-02 rules stay unchanged.

Before SETUP the workflow fetches and records one observed main snapshot. The
approved base must be its ancestor, and drift from base to that snapshot must
be disjoint from the closed 22-path dependency set: the 12 Stage A paths,
`run_logging.py`, the three Linux locks, the three Windows locks and
`requirements/runtime.in`, `requirements/test.in`, `requirements/dev.in`.
Shared-interface drift refuses admission. Disjoint later movement of main does
not change the tested source SHA, approved base or completed exact-SHA evidence.
Changing tested source requires a new discovery/grant/test cycle. A later
integration conflict still blocks readiness and requires its own disposition.

## Setup and immutable offline phase

Target: standard GitHub-hosted `ubuntu-24.04`, ordinary GIL CPython **3.14.8**
x86-64/glibc, pip **26.2.1**, unchanged Linux dev closure of 82 packages.
The three existing Linux locks remain read-only. SETUP uses only their
hash-locked wheels from the existing public PyPI sources and the pinned pip
wheel. No source/editable/VCS installs, dependency updates, global setup or
borrowed mutable environment are admitted.

Before `actions/setup-python`, the workflow requires the exact complete
interpreter in the standard hosted toolcache. Cache miss refuses; no implicit
installer or pip upgrade is allowed. Interpreter/build/executable identity,
locks, wheels, installed inventory and environment fingerprint are recorded.
A locally computed hash identifies bytes; it is not independent provenance.

The consumer creates its own unique mode-0700 temporary root and fresh replay
environment. Offline installation uses the validated wheelhouse, `--no-index`,
`--require-hashes` and `pip check`. No application workload begins before OS
network isolation is proven. Package configuration, proxy/plugin/Python
overrides and unrelated credentials are excluded from subprocess environments.

Rootless isolation and the separately granted supervised chain retain the
reviewed LNX-02 primitive, privilege-drop and kernel-network requirements.
Root Python, host firewall/AppArmor/sysctl changes and silent privilege fallback
are forbidden. Audit hooks, mocks and `--no-index` complement network namespace
evidence; they do not replace it. Native fixture evidence must establish
actual F-LOG transport, inherited-FD ownership, process identity, cleanup and
settlement, including any owned resource tracker.

Before/after source and environment manifests must agree. Cleanup runs in
`finally` only after actual settlement and only for revalidated owned paths.
Unsettled processes or cleanup failure exclude PASS; unknown scratch is retained
for controlled investigation rather than recursively removed.

## Results and acceptance

The exported bundle is exactly `result.json` and `manifest.json`, audited before
upload. The closed `lnx03-linux-bootstrap-v1` schema records source/base and
helper/lock identities, GitHub/ticket identity, environment/setup/replay outcomes,
native admission witnesses and cleanup. It contains no raw stream, traceback,
environment dump, secret, URL or private path. Missing observations are not zero
counters or invented worker finals. Manifest entries bind the complete exported
bytes; unknown/extra files or fields, aliases and stale evidence fail closed.
The fixed read-only `audit-linux-bootstrap --output <owned-output>
--expected-sha <sha> --expected-base <base>` helper command validates the bundle
before upload; it grants no execution or SETUP authority.
Stage export uses an explicit LNX-03 projection. It retains typed exits,
identity, namespace, guard and application observations; internal readiness and
topology records remain private. Audit rejects unknown nested fields in both
success and failure bundles. Failed application counts and partial native
observations remain non-PASS evidence rather than disappearing at an exception.
Its successful exit means bundle validation only, with a separate
`consumer_status`. A valid failure bundle may be preserved, but its actual
consumer exit remains nonzero and the workflow fails. The workflow also checks
that consumer exit zero agrees with result `PASS`; export success cannot turn
failed or missing application evidence into successful CI.

`PASS` means **APPLICATION_ADMISSION** only. It requires actual exits,
settlement, immutable fingerprints, native F-LOG/FD/process/network evidence and
clean completion. Local Windows static/refusal/schema checks cannot establish
Linux native acceptance. Skipped, unavailable, cancelled or other-SHA runs are
NOT VERIFIED. The workflow has `contents: read`, pinned actions, no persisted
checkout credentials, secrets, PR targets, self-hosted runner, cron or deployment.

Stage A acceptance also requires legacy LNX-02 source-bound authorization,
schema/output, self-test and artifact-negative regressions before merge. The
modified helper has a new hash; old source-bound artifacts remain tied to the
old helper. A real separately authorized LNX-02 `verify` on exact post-merge
`main` must pass and be independently accepted before transfer to Stage B.

| Boundary | Required independent oracle |
|---|---|
| Authority | Wrong SHA/base/ref/actors/run ID/number, foreign/reused ticket, attempt other than 2, unsupported mode and missing SETUP grant refuse |
| Source | Missing ancestry, changes outside 12, disallowed change types and drift inside closed 22 refuse; compatible disjoint drift preserves exact-SHA evidence |
| Fixed launch | Unknown targets, arguments, module/script/callback and unregistered children refuse before launch |
| Environment | Hash/closure/interpreter mismatch, changed source/environment and failed cleanup exclude PASS |
| F-LOG/native | Real Queue/Pipe/RawValue transport, actual process exits, FD inheritance and settlement witnesses |
| Isolation | Kernel namespace/network proof and separate supervised authority; forbidden DNS/network remains blocked |
| Export | Missing/corrupt/stale/extra evidence and canary-positive control fail rather than export false success |
| Legacy | Existing LNX-02 routing, grants, source-bound outputs and refusal oracles remain accepted separately |

## Hosted execution and integration gates

Before each bounded branch push, record actual base/clean HEAD/tree, the exact
12-path diff, workflow blob identity, environment fingerprints, completed local
checks and remaining execution budgets. Preserve failing evidence through each
minimal repair. Read back the branch and actual Actions identity after push;
run only the discovery/exact-attempt-two sequence above. The night grant does
not make setup, native observations or independent acceptance PASS in advance.
A new workflow must exist on the default branch before manual dispatch.

A push to this branch also matches the existing `offline-tests.yml` `codex/**`
trigger. If `MDS_F3_CI_SETUP_APPROVED=true`, Windows F1/F2/F-LOG/F3 and dependent
notebook checks may execute; the night grant authorizes those side effects
without changing that variable. The existing
Linux syntax preflight runs on PR to main/main push, not this branch push.
`linux-environment.yml` still accepts only its old branch/main modes and gains
no authority from LNX-03. Existing LNX-02 and F3 grants must not be changed.

Stage B is NOT AUTHORIZED. It needs independent review on the accepted Stage A
foundation, a fresh H and owner decision for early unsupported-platform refusal
at both `upload.main` and full `main.main`. Production adoption, other Linux start
methods/macOS, Windows D2, D1 production callers, D4/D5/D6, A8/B3, complete
#47/#78 integration and #66 staging remain open hookups. No release result,
remote commit, current generation or previous preservation is asserted here.

After accepted hosted gates and self-review, at most one draft Stage A PR is
authorized, with `Refs #39, #41, #47, #67, #78` and no closing keywords. Record
exact Linux and Windows commands, counts/exits, actual identities, redacted
evidence, AC mapping and remaining acceptance. No automatic merge is allowed.

Rollback is a separately controlled thematic revert of LNX-03 consumer/CI
changes. Preserve the original LNX-02 foundation, Windows worktree, environments
and all evidence. Do not reset, clean, force-push, rebase, close issues or run
publication. Only the night grant's bounded compatible main integration is
authorized; it is not production recovery.

## Native evidence limits

The fixed runner and fixture workers use `-I -S -B`; the runner adds only its
owned replay environment's package directories after installing its guard.
It does not execute `.pth` or site customization files. The real export gate
is exercised by the native canary positive control; redirected native handles
provide the separate pre-wrapper console containment witness.

Emergency PID-registry cleanup covers registered fixtures only. Cancellation
before registry publication can leave an unregistered birth uncertain. Missing
application proof therefore retains scratch and reports `INCOMPLETE`; it never
asserts complete descendant containment. Abrupt controller cancellation still
requires hosted runner teardown. None of these Linux mechanisms is accepted
until separately authorized exact-SHA hosted execution and independent review.
