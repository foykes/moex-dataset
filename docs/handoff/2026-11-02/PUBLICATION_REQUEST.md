# Concrete public publication request

The owner already authorized ordinary safe branch pushes to existing origin in the migration request. Automatic approval review nevertheless rejected the multi-branch push; no public mutation had occurred at that initial rejection. This document makes the exact requested payload reviewable.

Destination: **public** `https://github.com/foykes/moex-dataset.git`. Only new `refs/heads/handoff/**` refs listed below and the documentation branch are proposed. No existing owner ref, main, PR or issue is changed. No force. No new PR.

All new historical/source blobs were scanned for private-key/token/credential patterns, credential paths and large payloads; flagged URL credentials were inspected and are literal synthetic negative-test canaries. Original private F3 transfer commit is excluded. No new generated datasets or credentials are in the new commits. Existing public ancestry is reused rather than re-audited as a historical secret scan.

Workflow YAML at each distinct historical candidate version was inspected: push filters select main/codex/** or the exact LNX branch; notebook-sync is workflow_call only. `handoff/**` does not match. No workflow/grant/Actions mutation is requested. Readback must still verify no unexpected handoff run appeared.

| Direction | New remote ref | Exact SHA |
|---|---|---|
| D2_WINDOWS | `refs/heads/handoff/d2-windows-wip-20261102` | `c55768dc0e3d8d84c4cd86f4e25d29b8a4964e55` |
| D2_FIXTURES | `refs/heads/handoff/d2-fixtures-wip-20261102` | `042c606a97987e135653df61f54d04e3cb3e65c8` |
| A4_EARLIER | `refs/heads/handoff/a4-earlier-wip-20261102` | `af5669ba47dba301dcbe413d5b63f4b75bbd677b` |
| F3-U | `refs/heads/handoff/f3-u-20261102` | `a1071014c536c27c5cc1311e003d08a21346db0d` |
| F3-provenance | `refs/heads/handoff/f3-provenance-20261102` | `f8717a1ac122b3c65881e22e079905f0894fde98` |
| F3-aggregate | `refs/heads/handoff/f3-aggregate-20261102` | `7f9e63d8a43372f4c66788d803ecfad023b0c053` |
| F3-retention | `refs/heads/handoff/f3-retention-20261102` | `b84d2034b70396c22237975c7b6bb19c6e2a4411` |
| LNX-preserved | `refs/heads/handoff/lnx-02-preserved-20261102` | `bacd494c44c04ce0ae9320e715a6cebb34dbc0c6` |
| B2-prep | `refs/heads/handoff/b2-prep-20261102` | `9470d04e064e4c8b7c3b700d039116ed06c49802` |
| C2-integration | `refs/heads/handoff/c2-integration-20261102` | `1ed9ff6f6ea2788447c4a0c81cc1b8e91c8d58d8` |
| Workspace-guides | `refs/heads/handoff/workspace-guides-20261102` | `ddb6906e3ec6511302f1b85ae8e20b03b5d7965a` |
| D2-frozen-base | `refs/heads/handoff/d2-base-20261102` | `42830d91b4c701b899cbadb5ebdf8829eec6b5cf` |
| LNX-access-anchor | `refs/heads/handoff/lnx-access-anchor-20261102` | `6323e89380dea6fc37d92efd0b94ef5f81b483f4` |
| LNX-h0prime | `refs/heads/handoff/lnx-h0prime-20261102` | `57d83501f4a1a4964876bb510055fadb51c33aa9` |
| F3-recovered-WIP | `refs/heads/handoff/recovered-f3-034c560c-20261102` | `034c560cc122306067416acefaa980428d7ec4df` |
| F3-recovered-WIP | `refs/heads/handoff/recovered-f3-01e7de77-20261102` | `01e7de77addf7e995505788bd0a7f4b15a6ac50b` |
| F3-recovered-WIP | `refs/heads/handoff/recovered-f3-e42ed09f-20261102` | `e42ed09f4d4b555e86e5333aa385755213c2957f` |
| F3-recovered-WIP | `refs/heads/handoff/recovered-f3-f8743d79-20261102` | `f8743d79843ececa1656a8a5d4cf6f2a6e854294` |
| F3-recovered-WIP | `refs/heads/handoff/recovered-f3-8cf750a1-20261102` | `8cf750a15cdba036c4c08b8a1daac108ec66c168` |
| C1-preview-9dc | `refs/heads/handoff/c1-preview-9dc-20261102` | `96047a56a2540ea46cde412c3bcdf07ba941267f` |
| C1-preview-2d3 | `refs/heads/handoff/c1-preview-2d3-20261102` | `f60859006b1b431365e65d5ca2a76cb3e433a48e` |
| A2-detached-preview | `refs/heads/handoff/a2-merge-preview-20261102` | `dacfed4b969908b517453ecdee43ddda211c887e` |

The documentation branch `handoff/moex-migration-20261102` will use the final exact local SHA in the signed-by-Git commit/readback receipt supplied with the approval request. Documentation validation receipts may be committed normally on that isolated branch after restoration verification; owner-reviewed implementation refs remain fixed.

Authorization request: approve these new public refs/SHA payloads plus the final reviewed documentation commit and its validation-only follow-up, then normal push and independent clean GitHub clone verification. Private bundles/evidence/credentials are excluded from public publication.


## Resolution

The owner explicitly approved the listed 22 refs, documentation payload db8418fcb4efa0f60d7c2cea7010f420d3e6a8fc and validation-only follow-up. All 22 refs plus that payload were then published by normal explicit-ref push and checked from an independent HTTPS clone. The final containing validation commit and its readback are identified by the external publication receipt. Private archives remain excluded.


## Actual Actions side effects: correction to static trigger estimate

The explicit push filters exclude handoff/**, but GitHub nevertheless created two completed failure events for preserved historical LNX source: [41409851740](https://github.com/foykes/moex-dataset/actions/runs/41409851740) at bacd494c44c04ce0ae9320e715a6cebb34dbc0c6 and [41409874319](https://github.com/foykes/moex-dataset/actions/runs/41409874319) at 57d83501f4a1a4964876bb510055fadb51c33aa9. Both report 0 jobs, 0 check runs, check-suite before=zero, rerequestable=false. No runner execution is evidenced; the exact rejection reason is NOT_VERIFIED. The second API head_branch label is codex/lnx-02; direct remote readback still proves that original ref is e312c55b1c013dd1ff4b44858ce1139537319614 and was not advanced by this transfer.

The pre-push filter review did not cover this GitHub validation-event behavior; the earlier zero-side-effect expectation was too broad. No rerun, workflow/grant change, source fix or production action followed. These new failure records remain failures and are included in ACTIONS_SIDE_EFFECTS.json; they do not invalidate the byte/ref transfer or replace old qualification evidence.
