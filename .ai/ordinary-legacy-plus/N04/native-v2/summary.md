# N04 native V2 Git implementation

Worker: `worker-400ad386874a-8131394e8a`, attempt 1.
Worktree: `/home/jordan/gh/from-nibly/pi-dag-workflow/.git/ordinary-legacy-plus-git`.
Base: `98a0abb`; implementation: `15770e18f57a876340f1d9a7c4f2d288db594caa`.
Tested implementation tree: `032ef43a39fe27b69dbad0b1c7b8df2a0ba89e39`.

**Implemented and verified for the documented supported profile.** This supersedes
N04's earlier implementation-blocker conclusion, not its valid unsafe-merge
characterizations. Those assertions remain in `scripts/dag-v2-git-test.mjs`.
No model authority, historical real DAG, installed overlay, installation,
publication, push, or restricted external effect was changed/performed.

## Implementation and N05 handoff

- `GitDriverV2`, exported by `runtime-v2/index.ts`, is the concrete adapter:
  `new GitDriverV2(runtime, absoluteBoundRoot).integrate(mutation, itemId,
  generation, candidate, signal?)`. It does actual composition, validation,
  guarded landing, reconciliation and internal lifecycle acceptance. Do not call
  `RuntimeV2.integrate` with a permissive callback in N05.
- One durable operation stores the natural request, native root/common/admin
  path/dev/ino binding, source/candidate/expected/proposal native OIDs, actual
  prefix/final requests, dispatch state and observations. Snapshot audit checks
  joins, prefix/evidence applicability and accepted landing. Fresh services use
  persisted context, not closure-cached observations or lifecycle hash aliases.
- Explicit-base `merge-tree`, deterministic one-parent `commit-tree`, pinned
  composition settings/UTF-8 metadata, and exact-CAS private refs preserve exact
  composition. No raw checked-branch update-ref, reset, stash or forced worktree
  cleanup is used. Unsupported required attributes/filters/default-driver
  capabilities fail closed; unrelated safe settings and unused drivers work.
- Every prefix and final argv executes on an isolated exact-proposal checkout
  using N03's subreaper. Native workspace identity and clean index/files are
  checked before/after execution and before ordinary cleanup. Long validation
  runs outside the snapshot lock. Actual nonPASS or unresolved execution blocks.
- Landing uses ordinary `merge --ff-only --no-autostash --no-overwrite-ignore`
  with only the operation-owned reference hook. Its prepared-phase expected-old,
  proposal and direct-branch/HEAD checks execute under Git's ref locks. ORIG_HEAD
  guards the captured starting OID early; supported ancillary refs/phases are
  narrowly handled. Python isolated mode prevents user startup injection.
- Common-directory flock exclusion spans validation through acceptance. The
  supervisor inherits the lock description until subtree settlement. A durable
  common-directory claim also blocks other stores and participating V1 writers
  behind unresolved V2 work; unresolved V1 lock directories block V2. Historical
  V1 records and interpretation are unchanged.
- Lease/generation/authority/pause/cancellation fences remain internal. New-clean
  is observed once, never merged again; old-clean permits bounded redispatch only
  with current authority and exact stored checks. Dirty/third/missing/identity
  ambiguity retains all files, index state and foreign locks and blocks successor
  work. `closeOperation` requires observed settlement and does not clean bytes or
  accept a cancelled node. Accepted dependencies remain ready after reload.

## Operational profile, not a filesystem-atomicity promise

Supported here: Linux/local filesystem, util-linux flock, N03's Python subreaper
capabilities, **Git 2.54.0**, files ref backend, SHA-1/SHA-256, and a quiescent bound
session checkout during landing. Other Git versions/backends or required unsafe
capabilities fail closed. A copied linked-worktree gitfile cannot impersonate the
sole registered target checkout.

Ordinary merge can leave partial **own** index/files on failure. The driver
retains those effects and never treats nonzero exit as proof of no effect. It does
not isolate arbitrary same-UID editors, checkout/index writers, direct ref-store
rewrites, lock deletion, or adversarial namespace replacement at every syscall.
Local verification argv are trusted repository-local procedures, not an OS or
network sandbox and not publication authority. Uncooperative older installed V1
conductors must not run alongside this profile. No separate approval ceremony is
needed for this engineering interpretation.

## Verification

All commands below exited **0**, with **3600 seconds per command**. Exact commands,
durations and retained log paths are in `verification.json`; aggregate command
execution time was **1954.841 seconds**, with adequate process-level aggregate
budgets. The native suite was rerun after the final Git-specific hardening; the
unchanged lifecycle/store/V1 surfaces passed their complete listed suites.

| Command / selection | Seconds | Result |
| --- | ---: | --- |
| `node scripts/dag-v2-git-acceptance-test.mjs` | 380.520 | 43/43 |
| `node scripts/dag-v2-lifecycle-test.mjs` | 175.898 | 38/38 |
| `node scripts/dag-v2-context-test.mjs` | 48.975 | 6/6 |
| `node scripts/dag-v2-state-test.mjs` | 373.051 | 23/23 |
| `node scripts/git-integration-test.mjs` | 156.463 | PASS |
| `node scripts/dag-runtime-test.mjs` | 373.894 | PASS |
| `node scripts/dag-v2-git-test.mjs` | 1.100 | 4 unsafe characterizations retained |
| dogfood `--scenario semanticIntegrationRecovery` | 231.739 | PASS |
| dogfood `--scenario semanticIntegrationFailure` | 213.201 | PASS |

The native suite exercises real positive SHA-1/SHA-256 and linked-worktree landing,
wrong old/new/ref and malformed hook contexts, backward and during-command ref
races, partial effects and later user-byte retention, dirty/index/untracked/ignored/
directory/symlink collisions, foreign locks, conflicts, exact private-ref replay,
identity replacement and fresh reload, actual validation failure/stale evidence,
lease/cancellation fencing, command-success/no-op acknowledgement, real Git kills
before checkout/after checkout/after ref commit, owner kills before/after landing,
and real two-node F0–F8 plus prefix/final integration across fresh services.
This is bounded coverage, not exhaustive enumeration of every syscall schedule.

## Development failures retained, not relabeled PASS

`development/` retains the initial native runs and the adversarial configuration
failure that led to the last fix:

- Direct-ref inspection needed `symbolic-ref --quiet` to distinguish a direct ref
  by status 1. Git 2.54 also invokes the supported `preparing` hook phase before
  `prepared`; rejecting every unfamiliar-to-the-prototype phase broke the positive
  path. These were fixed and real positive/negative cases rerun.
- A stale-evidence corruption was correctly rejected by the executor/intent join;
  the test originally expected a different diagnostic. The assertion now names
  the actual rejection and still checks unchanged durable bytes.
- Crucially, Git permits `merge.text.driver` to shadow the supposedly built-in
  `text` name. Pinning `merge.default=text` alone was insufficient. The failing
  real fixture exposed that execution. Composition now rejects this required
  capability before merge-tree; separate regressions verify an unrelated default
  driver is disabled, the text override fails closed, and neither command runs.
  The focused fix passed, followed by the full final 43-case native run.
- An early broad development run overlapped source/schema editing and was not
  used as verification: parent/child schemas differed. The final complete V2
  suites were rerun on the settled schema. V1's original lock diagnostic was also
  preserved when adding shared exclusion; its full Git/runtime suites pass.

## Remaining work

No known N04 implementation blocker remains in the supported profile. N05 still
owns product/tool wiring, joined end-to-end release checks, uncached full release
readiness and extracted-package certification. This evidence is not certification
of an installed overlay or permission to publish. Preserve the conservative
blocked states when settlement evidence is missing or foreign locks remain.
