# Release workspace declaration v1

`contracts/release_workspace_v1.json` defines the closed, contract-only
`mds.release.workspace/1` preparation interface. The D/R1 workspace contract owner
owns this schema; A/B/C2 and the D/F providers must review consumer integration.
No existing entrypoint, writer, public dataset schema or publication policy changes.

The canonical schema SHA-256 is
`adb45e0d3253e2787658af8de9041da51ffe33b01c174921bb76c37711e3a1c8`.
Digest input is strict-decoded JSON encoded as UTF-8 with sorted keys, compact
separators, `ensure_ascii=false` and `allow_nan=false`. Checkout LF/CRLF therefore
does not change the contract identity. Git blob/mode identities separately bind
the reviewed source bytes. Any schema change requires a reviewed digest change;
unknown versions, duplicate JSON keys and nonstandard NaN/Infinity are rejected.

## Inputs and trust boundary

The workspace context is exactly `run_id`, `snapshot_id`, `dataset_id`, `as_of`.
`candidate_release_id` is a separate required ID. There is no `context.release_id`.
IDs are safe aliases; `as_of` is a calendar-valid canonical UTC string. A producer
must not derive this business cutoff from acquisition or evaluation time.

The preparation proposal references an independently retained parent registry by
`registry_ref`. The registry supplies the same context, candidate ID, platform,
fixed root inventory and separately identified basis records. An application
worker's own dictionary is not a trusted registry. A future parent/provider must
authenticate registry custody and resolve references outside worker write access.
This PR supplies no registry service, ticket issuer, runtime validator or authority.

The synthetic reference interpreter lives only in
`tests/shared/d4/test_workspace_contract.py`. It accepts schema, proposal and retained
registry; it never receives expected decisions, imports application modules or the
#109 evaluator, reads actual target roots, changes CWD/sys.path, or calls writers.

## Fixed root and role policy

All nine roots are mandatory, unique and pairwise disjoint, including ancestor/
descendant overlap. Paths are explicit; neither CWD nor `sys.path[0]` is a workspace.

| Kind | Owner | Preparation writes |
| --- | --- | --- |
| SOURCE / CONFIGURATION | INPUT_OWNER | Forbidden |
| PREVIOUS | INPUT_OWNER | Forbidden |
| CANDIDATE_A | A | PREPARER_A only |
| CANDIDATE_B | B | PREPARER_B only |
| CANDIDATE_C | C2 | PREPARER_C only |
| CAPTURE_SPOOL | F_SOURCE | SOURCE_WORKER only |
| SEALED_EVIDENCE | E | E_VERIFIER only |
| CURRENT | D_PUBLISH | Forbidden, including D4 preparation calls |

The exact read-role inventory is in `x-contract.root_policy`. In particular,
PREPARER_B/C may consume the declared A catalogue candidate, Q reads sealed
evidence, and source workers cannot write sealed evidence. CURRENT cannot be used
as a synonym for PREVIOUS. Later D publication requires a separate admitted
provider interface; it cannot obtain authority through this preparation contract.

Operations bind an exact role, root, relative path, context and candidate ID.
Candidate operations require the candidate release/snapshot/as_of tuple. Previous
operations require explicit PRESENT previous and its independent tuple. Other
roots require `generation=null`, `generation_reason=NOT_APPLICABLE`. Missing or
unknown generation is not NOT_APPLICABLE. A null previous identity is NOT_VERIFIED,
not ABSENT; ABSENT requires its own retained absence basis. This contract does not
establish previous quality, freshness or eligibility as fallback.

## Lexical path and evidence boundaries

LINUX declarations use canonical absolute POSIX paths. WINDOWS declarations use
canonical drive-absolute forward-slash paths and case-insensitive comparison.
Relative paths cannot contain parent/dot/empty components, backslash aliases,
encoded escapes, trailing dots/spaces or absolute/device/ADS syntax. Windows also
rejects UNC roots and reserved device names. Different drive roots are distinct.
The profile deliberately accepts a narrow path syntax; it does not normalize an
untrusted spelling into acceptance. Native support remains Linux-first and separate.

`PLAIN_DECLARATION` is only a supplied lexical observation. Declared symlink,
reparse, shared-hardlink or unknown alias states are rejected. Synthetic fixtures
cannot prove filesystem permissions, physical root identity, race-free traversal,
stop/reap containment or absence of late CURRENT writes. Actual providers must
establish those properties independently before runtime admission.

Business as_of, source acquisition and independent expected coverage each have a
different basis kind/reference and exact context. Acquisition has its own UTC;
coverage has its own declared scope and as_of. A received row list or the caller's
request list does not establish source completeness. `NOT_VERIFIED` uses explicit
null values and a reason; `SYNTHETIC_ONLY` uses retained synthetic references. No
official VERIFIED source claim is accepted or manufactured here.

LOCAL/PUBLIC artifact or target bindings belong to the later versioned release
profile, not these physical roots. This contract neither approves the proposed
11-artifact/22-binding composite profile nor invents a Google dividend target.

## Outcomes and acceptance

Valid declarations return only `VALID_DECLARATION`, `PREPARED`,
`publication_status=NOT_ATTEMPTED`, `authority=DECLARATION_ONLY`,
`evaluation_scope=SYNTHETIC_ONLY`, and native/source `NOT_VERIFIED`. There is no
route to PUBLISHED, CURRENT_VERIFIED or release success. Contract errors contain
only closed safe codes; untrusted paths/values are not interpolated into messages.

The fixture specifies 83 manual scenario outcomes, including root overlap/escape,
role violations, version/context/generation substitution, unknown/null evidence
and attempts to relabel preparation as publication. Six strict-JSON decoder cases
and independent fixture/schema, full-value pattern, expected-output and redaction regressions accompany
them. Expected outcomes are not computed by the reference interpreter.

Run existing guarded F3 collection/execution, F2, F-LOG and the exact-ref check of
all ten notebook pairs on a clean committed HEAD. Owning environment and source
integrity receipts are separate from test exit codes. No skips, dependency changes,
CI changes or golden updates are introduced.

`C2_FULL_ACCEPT` and global A/B/C/D `FULL_RELEASE_ACCEPT` remain distinct. This
contract does not redirect existing A/B/C writers, establish global previous
preservation, satisfy source/use/native/staging evidence, or grant production
permissions. Rollback is a reviewed revert of this four-file contract-only PR,
with evidence retained; no dataset restoration is performed.
