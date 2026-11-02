# Restore on a new machine

## Public source and handoff

Git operations below are safe and platform-independent unless noted. The new handoff refs were published and read back at exact SHAs. Check the final publication receipt before relying on the handoff fetch. A missing ref is a transfer blocker; do not replace it with main or an older branch.

```powershell
git clone https://github.com/foykes/moex-dataset.git moex-dataset
cd moex-dataset
git status --short --branch
git remote -v
git ls-remote --heads origin
git fetch --no-prune --no-tags origin
git switch --create handoff/moex-migration-20261102 --track origin/handoff/moex-migration-20261102
git rev-parse HEAD
```

Read `docs/handoff/2026-11-02/README_START_HERE.md`. Inspect `TRANSFER_MANIFEST.json`, compare every required ref/SHA against `git ls-remote --heads origin` and check the payload-commit binding. The manifest uses a prior immutable payload commit to avoid an impossible self-referential Git SHA; the final containing commit is bound by the external publication receipt and `git rev-parse HEAD`.

Explicit immutable worktree examples (after the refs are available):

```powershell
git -c core.autocrlf=false worktree add --detach ../mds-main e7800add6ba700f37656cba3517b4e6db83ca413
git -c core.autocrlf=false worktree add --detach ../mds-a4 origin/codex/mds-a4-01
git -c core.autocrlf=false worktree add --detach ../mds-d2-wip origin/handoff/d2-windows-wip-20261102
git -c core.autocrlf=false worktree add --detach ../mds-f3-history origin/handoff/f3-retention-20261102
```

Verify exact HEAD in each new worktree. Detached review is intentional; create a fresh `codex/<topic>` implementation branch only for an approved next slice. Do not check out another active owner's branch or merge checkpoints. Existing original machine paths and its worktree administration are not restoration targets.

## Private offline bundle fallback

If publication remains blocked, the owner must securely transfer the private local Git bundle and this documentation. Obtain its exact SHA-256 receipt outside public Git. From an existing Git repository, verify the bundle; then change to its parent and clone into a new destination:

```powershell
git bundle verify PATH_TO_PRIVATE_BUNDLE
cd ..
git clone PATH_TO_PRIVATE_BUNDLE moex-offline
cd moex-offline
git remote rename origin transfer-bundle
git remote add origin https://github.com/foykes/moex-dataset.git
git fetch --no-prune --no-tags transfer-bundle 'refs/heads/*:refs/remotes/transfer-bundle/*'
git switch --create handoff/moex-migration-20261102 --track transfer-bundle/handoff/moex-migration-20261102
```

Replace `PATH_TO_PRIVATE_BUNDLE` with the actual securely received file; it is a placeholder, not a command to execute unchanged. A local-bundle restoration is **not** proof that new branches are on GitHub. Bundles contain historical committed datasets and potentially private handoff metadata; do not publish the private bundle.

Transfer `PRIVATE_LOCAL_EVIDENCE_20261102.zip`, its content manifest and original F3 delivery archives by encrypted channel. Verify SHA-256 before extraction, then each entry's manifest digest. Extract into an isolated archive directory, not over source worktrees or `.git`. Do not execute any archived script. Original physical identities, locks, claims, processes and installed environments are not recreated. See [local-only materials](LOCAL_ONLY_MATERIALS.md).

## Environment: Windows

Current target is Windows x64 ordinary-GIL CPython 3.14.8. Runtime/test/dev locks are in `requirements/windows-cp314-*.lock`; current recipes and official archive hash are in [environment](../../environment.md). Do not use historical audit versions or the Windows Store Python alias, install globally, regenerate locks, upgrade pip/dependencies or copy an installed environment from another worktree.

After a separately approved owned SETUP, follow the existing recipe to verify the official full Python ZIP, create a fresh worktree-local environment and install unchanged hash-locked wheels. No environment was reinstalled for migration. Archive availability and compatible download/setup on the next computer remain NOT_VERIFIED.

Before running even an offline project suite, obtain the applicable source/runtime/storage/process admission and exact lane contract. F3 remediation and A4 execution remain blocked. Do not run bare broad discovery, main.py, upload.py or legacy diagnostics as setup checks.

The following existing canonical spelling is a **reference only, not authority to execute**:

```text
python -I -B -X utf8 tools/offline_tests.py --lane f1
```

Use the owned verified interpreter and fixed lane targets from testing docs only after admission. Do not use this handoff to bypass source or child-process checks. No F3 qualification rerun is needed to validate transfer integrity.

## Linux / macOS

The Git clone/ref/worktree commands above also work in a POSIX shell. For SHA-256 use `sha256sum` (Linux) or `shasum -a 256` (macOS), corresponding to PowerShell `Get-FileHash -Algorithm SHA256`. Linux has separate committed locks/contracts in [linux_environment](../../linux_environment.md) and [D2 Linux CI](../../d2_linux_ci.md); Windows locks are not Linux/macOS locks. Existing Linux syntax/SETUP/native results do not establish all application lanes on a new host. macOS application runtime remains NOT_VERIFIED. Do not create a new dependency set during restoration.

## What requires the owner

Choose the next small scope after reading blockers; obtain original private receipts needed for that scope, fresh ownership/H and execution authority. F3 old-host recovery, A4 runtime, Windows D2 adoption, #117/#122 combined acceptance, D1 Stage B, live/source diagnostics, staging/publication and production cannot resume automatically. Credentials require a separate secure owner transfer; validate only presence/format/missing field names, never print their values. Budget remains zero.


## Verified Windows byte restoration

The default test-clone policy `core.autocrlf=true` physically converted seven of the fifteen frozen D2 files. This is a Git checkout transformation, not changed committed source. The per-command `-c core.autocrlf=false worktree add --detach` spelling above was actually tested in a fresh second worktree and reproduced all fifteen original file SHA-256 values exactly. All twenty-four snapshot files were also read from the restored Git blobs and materialized with exact original bytes. Do not bulk-normalize or sync notebooks to make an old seal fit.

The private bundle at the first payload commit was independently cloned: 22 required preservation refs, 24 exact source files and 18 documents verified. A final successor bundle/receipt also records the containing validation commit; use the final external delivery receipt to select that archive. Git-admin ZIP is forensic custody only: never overlay its original indexes/paths onto the new clone's `.git`.

The private evidence delivery includes a separate LONG_PATHS ZIP: all 10,191 initial ordinary-path read failures were recovered and verified through Windows extended-length paths. Keep both evidence manifests. Five reserved-name fixture entries in the base ZIP should be retained/extracted by content hash under neutral filenames, with their original names kept only in the manifest; never recreate NUL/CON-style names or junctions on the new host. An archive is evidence, not a live fixture/OS-state restoration.
