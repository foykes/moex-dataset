# Validation report

Status: PARTIAL_HANDOFF. This report distinguishes completed local custody checks from pending public transfer verification.

| Check | Actual result | Evidence / limitation |
|---|---|---|
| Initial status and remotes | PASS | Primary codex/prepare-workspace; origin foykes/moex-dataset |
| Registered worktrees / independent clones | PASS | 68 original registered + 6 independent; one separate migration worktree created |
| Branch/stash/reflog/fsck inventory | PASS | 67 original shared local branches; no stashes; 17 initially origin-unreachable local commits + 5 unreachable historical F3 commits |
| Origin refresh | PASS | fetch --no-prune --no-tags; no branch deletion; remote main e7800add |
| GitHub metadata | PASS | Public visibility, all 49 PRs / 77 issues / 289 runs paginated; 13 open PRs, 12 draft; selected raw job logs read |
| Source checkpoint byte preservation | PASS | All 24 source files hashed before commit and checked again; raw exact blobs stored; paired D2 tools/upload pair retained together |
| Original dirty indexes / HEADs | PASS at checkpoint creation | Original source HEAD and raw index SHA-256 unchanged; no original stage/reset/checkout |
| Historical unreachable commits | PASS | Five explicit recovered anchors, two extra C1 historical previews imported; no new merge/rebase/amend |
| Secret/privacy scan | PASS for proposed new public source blobs | Private-key/token patterns, credential paths, sizes and credential-valued assignments checked; literal synthetic URL canaries reviewed. Private original F3 handoff withheld. Historical already-public dataset secret audit is NOT_VERIFIED (#69). |
| Push side-effect review | PASS static scope | Distinct candidate workflow trigger blobs inspected; handoff/** matches none. No new PR or workflow/grant mutation. |
| Public push | BLOCKED_AUTO_REVIEW | Automatic approval rejected the public multi-branch payload. No fallback public mutation used. |
| Independent GitHub clone / handoff refs | PENDING | Local commits cannot establish remote availability. |
| Private bundle / evidence archive restore | PENDING | Separate receipts will record actual checks, counts and missing materials. |
| Documentation JSON / links / scope / whitespace | PENDING | Checked before payload commit and repeated after restoration. |
| Application/runtime tests | NOT_RUN | No imports/main.py/production/live requests, no F3 qualification, no environment/dependency changes for transfer. |
| Another OS / real new computer | NOT_VERIFIED | Local clean clone can verify Git payload only, not another machine's runtime/host identities. |

No claim is made that a successful transport repairs failed F3 runs, qualifies an unbound draft, releases ownership or satisfies production acceptance. Final containing commit is reported outside the self-referential manifest; the manifest's payload-commit field names an immutable earlier documentation snapshot.
