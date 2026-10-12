# LNX-02 Linux dependency environment

Target: standard GitHub-hosted `ubuntu-24.04`, Linux x86-64/glibc, ordinary GIL
CPython **3.14.8**, pip **26.2.1**. This slice verifies dependency environments,
not application execution or production. LNX-03 owns Linux application regression
acceptance. Existing [Windows environment](environment.md), [testing](testing.md)
and notebook workflows retain their requirements and checks.

## Files and ownership

The [Issue #92 owner reservation](https://github.com/foykes/moex-dataset/issues/92#issuecomment-6120075488)
assigns one executor exactly six new paths:

| Profile | Existing direct inputs, read-only | New complete Linux lock |
|---|---|---|
| runtime | `requirements/runtime.in` | `requirements/linux-cp314-runtime.lock` |
| test | `requirements/test.in` | `requirements/linux-cp314-test.lock` |
| dev | `requirements/dev.in` | `requirements/linux-cp314-dev.lock` |

The other three paths are this document, `.github/lnx02/verify_env.py` and
`.github/workflows/linux-environment.yml`. No shared-file custody is transferred.
H0 contains the finished helper, workflow and documentation. It contains no
invented locks. H0 to H1 may add only the three verified locks; changing the
helper or workflow requires renewed independent review.

## Authorization and activation

The new workflow has a branch-specific `push` trigger for `codex/lnx-02` and a
post-merge `workflow_dispatch`. A trigger is not a SETUP grant. Missing or invalid
authorization fails before checkout, Python setup or package traffic. A skipped,
unavailable or failed job never establishes successful verification.

Only the repository owner `foykes` sets these non-secret repository variables,
after reviewing the complete exact commit and separately authorizing its push
and network SETUP. The job has no permission to set them.

| Repository variable | Exact permitted value |
|---|---|
| `MDS_LNX02_SETUP_APPROVED` | `true`; absent, empty or any other value refuses SETUP |
| `MDS_LNX02_APPROVED_SHA` | Full lower-case 40-hex reviewed commit SHA |
| `MDS_LNX02_APPROVED_BASE` | Full lower-case 40-hex freshly checked `main` SHA |
| `MDS_LNX02_OPERATION` | `generate` or `verify` |

Order for each new approval: set `MDS_LNX02_SETUP_APPROVED=false` first; set SHA,
base and operation; check their readback; set the flag to `true` last; perform
only the separately authorized triggering action. Revoke with `false` after
that run. A stale grant cannot approve a new commit. The owner must revoke a
grant before changing any of its fields. Do not rerun an approved SHA without a
new explicit run authorization; these variables are a revocable exact-SHA gate,
not a cryptographic single-use ticket.

For a pre-merge push, the repository must be `foykes/moex-dataset`, the ref must
be `refs/heads/codex/lnx-02`, and actor/triggering actor must both be `foykes`.
`github.sha`, the approved SHA and actual checkout HEAD must agree. Live `main`
must equal approved base and be an ancestor of HEAD. Recheck drift before SETUP;
drift stops the run and requires review of a new base/HEAD. There are no PR,
`pull_request_target`, fork or self-hosted execution paths.

After merge, manual dispatch requires ref `main`, mode `verify`, explicit
`allow_network_setup=true` and the full approved SHA. For that dispatch,
approved base and approved SHA both equal the current exact merge SHA on main.
If main advances before dispatch, stop and obtain a new owner decision; do not
silently check a different revision. Manual inputs never override repository
grants. The new workflow must first exist on the default branch for dispatch.

Permissions are `contents: read`; no secrets or write permissions. Actions are
pinned to full commit SHAs. Checkout does not persist credentials. GitHub
checkout/fetch and sanitized artifact transport are separately identified
control-plane traffic. Package SETUP uses only public PyPI over HTTPS.

## Interpreter prerequisites and phase boundaries

Before `actions/setup-python`, require an existing complete ordinary GIL
CPython 3.14.8/x64 entry in the standard runner toolcache. After selection,
verify version, implementation, architecture and GIL again. Record runner image
identity, interpreter/build identity and executable digest. The trust root is
the standard GitHub-hosted image/toolcache; a freshly computed executable hash
is evidence identity, not independent interpreter provenance verification.

Cache miss stops the run. The action's cold-cache installer can upgrade pip
without a fixed version; later installing fixed pip would not undo that earlier
operation. No implicit download/build, WSL provisioning or privileged fallback
is permitted. A cache miss is NOT VERIFIED, not a dependency-version change.

Rootless user/network namespace creation is independently tested on each
runner before package SETUP. Ubuntu 24.04 policy may deny it. The replay must
prove a different network namespace, no external interfaces/routes and refusal
of an external-address probe. No AppArmor, sysctl or privilege changes are
allowed. `--no-index` alone is not evidence of network isolation.
Netlink must report zero addresses and routes with only loopback present and
down. Proc cross-checks accept an empty IPv4 table or its exact header, and
only exact IPv6 kernel null/unreachable sentinel rows; usable routes fail.

SETUP may fetch only public PyPI HTTPS metadata and binary wheels. It never
uses source distributions, editable/VCS installs, extra indexes or project
installation. Pip bootstrap is the universal `pip==26.2.1` wheel with SHA-256
`71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e`.
Inherited pip configuration, proxies and Python/plugin overrides are excluded
from package subprocess environments. No persistent configuration is changed.

## Generation and independent replay

The helper runs only under an approved hosted context:

```bash
umask 077
task_run_root="$(mktemp -d "$RUNNER_TEMP/lnx02-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT.XXXXXXXX")"
python -I -S -B .github/lnx02/verify_env.py run \
  --mode generate --repo-root "$GITHUB_WORKSPACE" \
  --output "$task_run_root/artifact" \
  --expected-sha "$MDS_LNX02_APPROVED_SHA" \
  --expected-base "$MDS_LNX02_APPROVED_BASE"
```

`verify` uses the same command with `--mode verify`; it requires the three
committed Linux locks and never rewrites them. Every output destination is
fresh, owned and separate from the checkout. Existing destinations, linked
components, shared files or unsafe artifact names are refused.

Resolve runtime first, preserving the Windows profile's agreed versions using
version-only constraints. Verify its selected wheel records, then derive
version-only runtime constraints for test and dev. Preserve the versions of
common Windows dependencies; Linux markers determine active dependencies.
Windows-only `colorama` and `pywin32` are omitted only after their public
dependency markers have been checked. The initial accepted Linux closure is
the Windows closure minus those inactive packages. An unexpected Linux-only
dependency is a blocker requiring review of a narrow remedy; it is not silently
added. Missing wheels or version conflicts also stop; the helper never
substitutes an agreed version.

Each lock is a sorted complete list of exact versions and SHA-256 hashes for
selected Linux-compatible wheels. Universal wheels are valid when compatible
with this interpreter. Validate public wheel origins, native compatibility,
downloaded bytes and manifests; Windows native wheel hashes are not reused.

Each profile is replayed in a new venv from the validated wheelhouse within
the disconnected namespace, with `--no-index`, `--only-binary=:all:` and
`--require-hashes`, followed by `pip check`, exact installed inventory and
bounded synthetic TA-Lib/NumPy/pandas and temporary CSV/XLSX checks. No
application imports, notebook conversion, production queries or real datasets
are involved. Pip and verification failures retain their actual exit codes.

Every managed Python process enters through the helper. OFFLINE hooks are
installed before third-party imports; pip executes through `runpy` in that
same process. Venv creation omits ensurepip. OFFLINE does not allow arbitrary
children or ambient `.pth`/`sitecustomize` code. Process-local fixed pip user
agent and disabled IPv6 capability probing avoid incidental subprocess/socket
probes; installed pip files and dependency resolution semantics are unchanged.

Scoped negative DNS and child-launch probes run in the actual pip/verification
PID. Expected denials are distinct from sticky unexpected violations, which
fail even if pip returns zero. Python audit hooks are not a sandbox: native
code can bypass events. They complement the independently proven namespace.

## Evidence, local review and H0 to H1

The artifact output is closed: `report.json`, `manifest.json` and, on success,
the three `linux-cp314-*.lock` files. Only these sanitized outputs may be
uploaded. Raw subprocess output, venvs, wheelhouse, credentials and private
paths stay out of artifacts. Reports identify source/base/helper, interpreter,
OS/architecture, actual wheels/digests, operation identifiers/exits, guarded processes,
namespace evidence and check outcomes. Manifests bind the exported file bytes.
Failure produces sanitized evidence and a nonzero exit; it does not export
partially verified candidate locks. Cleanup removes only the helper's fresh
scratch root after revalidating ownership and links; the evidence is retained.

Local H0 checks require no installation or online SETUP:

```bash
python -I -S -B .github/lnx02/verify_env.py --help
python -I -S -B .github/lnx02/verify_env.py self-test
git diff --check
```

Use an already trusted interpreter for stdlib syntax/self-tests. These checks
do not require that interpreter to match the native target and cannot prove
Linux wheels or replay. Use actionlint if already available; otherwise record
that gate as NOT VERIFIED and perform independent workflow structure checks.

1. Independently review local H0's exact base, HEAD and full diff. No SETUP is
   needed to inspect code, authorization, self-tests or the fail-closed path.
2. Obtain separate exact-H0 push/SETUP authorization; the owner sets grants in
   the order above. H0's `generate` run must pass native prerequisites, all
   profiles and independent replay before artifact transfer is authorized.
3. After separate transfer authorization, validate run/H0 identity, GitHub
   artifact digest and closed manifest. Extract only the three expected lock
   files with bounded path/size/link checks; never execute artifact content.
4. Add only those locks in the same branch to form H1 and one bounded PR.
   Any helper/workflow change returns to review instead of being hidden in H1.
5. Obtain a new exact-H1 `verify` grant and run. Review the final exact HEAD;
   successful H0 evidence does not establish H1. Preserve independent Windows
   checks. Recheck live base/reservations and obtain separate merge approval.
6. After merge, obtain separate manual dispatch authorization for the exact
   merge SHA. Merge-SHA Linux environment CI remains NOT VERIFIED until that
   hosted run succeeds. Production remains outside this slice.

If a prerequisite or dependency is unavailable, retain the blocker and propose
a narrow separately approved remedy. Strategy A (private WSL provisioning) is
only a reserve owner decision, never an automatic fallback. Rollback before
publication preserves the local branch/evidence; after publication use a
separately reviewed thematic revert. Never delete foreign worktrees or alter
production, branch protection or existing workflows during rollback.
