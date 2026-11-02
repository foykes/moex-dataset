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
| Original dirty indexes / HEADs | PASS at checkpoint creation and independent final audit | All 68 original worktree HEADs/raw index hashes/status states and 67 original local branches match initial inventory; six additional original checkouts match; all recorded dirty/untracked bytes unchanged. |
| Historical unreachable commits | PASS | Five explicit recovered anchors, two extra C1 historical previews imported; no new merge/rebase/amend |
| Secret/privacy scan | PASS for proposed new public source blobs | Private-key/token patterns, credential paths, sizes and credential-valued assignments checked; literal synthetic URL canaries reviewed. Private original F3 handoff withheld. Historical already-public dataset secret audit is NOT_VERIFIED (#69). |
| Push side-effect review | PASS static scope | Distinct candidate workflow trigger blobs inspected; handoff/** matches none. No new PR or workflow/grant mutation. |
| Public push | PASS_AFTER_EXPLICIT_USER_GRANT | Normal explicit-ref pushes; all 22 preservation refs matched exact remote SHAs. Original owner refs/main/PR/issues unchanged. No force or fallback mechanism. |
| Independent GitHub clone | PASS for published payload | Fresh HTTPS clone fetched all 22 preservation refs and the documentation payload from GitHub, independently checked 24 source files, 18 documents/31 links, clean status and fsck exit 0. Final successor readback is bound by the external publication receipt. |
| Private bundle restoration | PASS | Complete self-contained bundle: 224 recorded refs/HEAD roles; 22 required preservation refs, 24 source file Git/raw byte hashes and 18 documents verified from a fresh clone. |
| Private evidence archive | PASS_FULL_ENTRY_SHA256_AND_ZIP_CRC_READBACK | 444812 entries verified by full SHA-256 and ZIP CRC; 726367867 archive bytes. Original files retained. |
| Private Git administration archive | PASS_FULL_ENTRY_SHA256_AND_ZIP_CRC_READBACK | All 6002 entries verified; includes unreachable object files/reflogs/original indexes. Config/locks/credentials excluded; forensic, never overlay restore. |
| Documentation JSON / links / scope / whitespace | PASS at payload | 18 documentation files, 3 JSON files and 31 internal links; explicit path staging and cached diff --check passed. Same documents validated in independent bundle clone. Repeated for validation successor. |
| Application/runtime tests | NOT_RUN | No imports/main.py/production/live requests, no F3 qualification, no environment/dependency changes for transfer. |
| Another OS / real new computer | NOT_VERIFIED | Local clean clone can verify Git payload only, not another machine's runtime/host identities. |

No claim is made that a successful transport repairs failed F3 runs, qualifies an unbound draft, releases ownership or satisfies production acceptance. Final containing commit is reported outside the self-referential manifest; the manifest's payload-commit field names an immutable earlier documentation snapshot.


## Actual restoration details and limits

Default Windows checkout CRLF transformations were detected, recorded and separated from Git blob hashes. A second fresh D2 worktree created with per-command `core.autocrlf=false` matched all 15 frozen file hashes exactly. No global or original repository config changed. The initial naive documentation physical-byte comparison was corrected to verify the exact observed LF-to-CRLF policy; no source acceptance oracle was relaxed.

Git count-objects reported four old temporary object files totalling 807 bytes; they were preserved in the private administration archive, not removed. Shared/extra repository fsck checks had no object corruption verdict. The secondary private directory/reparse census found 84 junction fixture roots (all targets within the original worktree tree), 269 FileNotFoundError and 37 PermissionError directory observations, primarily retained sandbox/profile trees. Their metadata is private; inaccessible descendants and old host identities are not claimed copied. The archive does not recreate live links, process lifetimes, installed environments or original filesystem identities.

GitHub main protection was read: protected=false, classic protection HTTP404, rulesets=[] and effective branch rules=[]. Required-by-project gates in testing/coordination remain obligations; server enforcement was not enabled or changed.

The runtime automatic approval rejection of the 21-ref public push is retained as a transfer event. A subsequent explicit grant was requested against the concrete 22-ref preservation set plus payload documentation SHA. No alternate public-write mechanism was used to evade that rejection. Final remote state is reported in TRANSFER_MANIFEST.json and the external delivery/publication receipt.

All 10,191 initial file read errors in the private base archive were independently recovered in a supplemental ZIP using Windows extended-length source paths; 10,191/10,191 SHA-256/CRC checks passed and no source permission/name/content changed. The base and supplemental archives contain 455,003 verified entries in total. These resolved file reads are distinct from the older metadata-only directory census limits. The base ZIP retains five synthetic reserved-name fixture entries as ordinary stored bytes; their original names must not be blindly recreated during extraction.


The owner explicitly replied "Разрешаю этот push и фиксацию результатов проверки" to the exact payload request. That resolved the automatic approval blocker before any public write. The original 61 public refs and main were not targeted; no new PR, issue status change, workflow activation was observed. Two completed workflow failure events with zero jobs are recorded below.


## Actual Actions side effects: correction to static trigger estimate

The explicit push filters exclude handoff/**, but GitHub nevertheless created two completed failure events for preserved historical LNX source: [41409851740](https://github.com/foykes/moex-dataset/actions/runs/41409851740) at bacd494c44c04ce0ae9320e715a6cebb34dbc0c6 and [41409874319](https://github.com/foykes/moex-dataset/actions/runs/41409874319) at 57d83501f4a1a4964876bb510055fadb51c33aa9. Both report 0 jobs, 0 check runs, check-suite before=zero, rerequestable=false. No runner execution is evidenced; the exact rejection reason is NOT_VERIFIED. The second API head_branch label is codex/lnx-02; direct remote readback still proves that original ref is e312c55b1c013dd1ff4b44858ce1139537319614 and was not advanced by this transfer.

The pre-push filter review did not cover this GitHub validation-event behavior; the earlier zero-side-effect expectation was too broad. No rerun, workflow/grant change, source fix or production action followed. These new failure records remain failures and are included in ACTIONS_SIDE_EFFECTS.json; they do not invalidate the byte/ref transfer or replace old qualification evidence.
