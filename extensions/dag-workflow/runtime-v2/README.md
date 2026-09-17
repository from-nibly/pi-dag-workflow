# V2 state, lifecycle evidence and native Git integration (N02–N04)

This is a working local state service, not the product cutover. Import from
`runtime-v2/index.ts`. N05 must wire this service instead of the V1 writer. The
only V1 writer adjustment here is common-directory integration exclusion;
historical V1 data and interpretation remain unchanged. Do not start V1 selection
through this service.

## Storage and consistency

`StoreV2(repositoryDirectory)` writes only `.ai/dag-workflow-v2/state.json`.
One snapshot contains retained plan revisions, frozen run selectors, start
requests, session bindings, leases, node generations, reservations and integration
identities. A start request, run and binding are published atomically; no separate
start receipt/index recovery protocol is necessary. Views do not create files.

The initial supported profile is **Linux, local filesystem, util-linux flock,
/proc process identities**. Unsupported/missing lock facilities fail closed.
The flock subprocess locks the parent's inherited open-file description; the
parent keeps that descriptor until the transaction finishes. The lock inode is
never unlinked, and process death releases the OS lock. This avoids unsafe stale
lock timeout/deletion races. Snapshot writes use unique temp + file fsync + rename
+ directory fsync. Transactions also sync the surviving publication and parent
directories before acknowledging replay after a rename/fsync interruption.
Unreferenced temp files left by process death are harmless, not authoritative.

Every ancestor from `/` through the repository, `.ai` and store is opened with
`O_DIRECTORY | O_NOFOLLOW` and retained for the operation. Reads, lock creation,
temp creation, rename, cleanup and directory fsync use those descriptors
(`/proc/self/fd`), not a newly resolved repository path. State/lock files must be
regular, non-symlink, single-link files; nonblocking opens reject FIFOs without
hanging. Directory, lock and state identities are rechecked before publication
and before acknowledgement. A detected replacement fails closed; views never
create or repair directories. Symlinked repository ancestors are unsupported.

Identity checks alone are not atomic namespace guards. Descriptor anchoring is
what prevents a concurrent ancestor swap from redirecting I/O into its replacement.
A swap after the final check can still leave a publication in the originally
opened (now detached) directory and produce an uncertain result; do not interpret
that error as proof no write occurred. This is not a sandbox against a same-UID
actor rewriting store inodes in place, manipulating mounts, or repeatedly moving
entries between checks. Such actors require filesystem/OS isolation. Cooperative
writers must never replace directories or unlink/replace the lock inode.

Plan saves and starts compare the store integer revision. Run mutations compare
run integer revision and the current process-bound lease. Lease acquisition
increments a generation; same-manager acquisition fences old in-memory callers.
Another process can acquire only after absence, PID reuse or zombie state proves
the old process cannot act. There is no heartbeat-expiry takeover. Public inputs
are closed schemas, including all ID-keyed records. Plan inputs reject generated
envelope fields (`kind`, `schemaVersion`, `revision`, `planHash`) instead of
silently overwriting them. Semantic plan validation happens on ingress, and
transition guards enforce local invariants. Both reload and every publication
audit the complete snapshot semantically; rejected validation leaves existing
snapshot bytes and durable revisions unchanged. There are no
nested hashes or snapshot history chains. The only content hash is the plan hash.

## APIs and downstream boundaries

- `save`, `show`, `renderPlanV2`: immutable retained content revisions and inert
  inspection. No approval fields, status, revision, receipt or transition.
- `start({ intent: "run", sessionId, selection, authority }, storeRevision)`:
  selects exact **current** saved content; checks independently hydrated current
  repository/source equality and closed scope; atomically starts or resumes.
  Scope is dependency-closed and serial integration scopes are prefixes. Effects
  are currently restricted to `repository_local`; publication cannot be granted.
- `FreshnessV2.current(plan)` is the trusted N05 repository/model hydration adapter.
  It must recompute governing closure/source equality and inspect actual baseline
  and repository eligibility, never echo unvalidated caller input.
- `reserve` checks dependencies **after integration**, gates, resources, mutexes
  and sticky node concurrency. Serial plans admit only the next unfinished prefix
  node, so a later independent node cannot monopolize the lane needed by its
  prefix. Serial work-item order must also be topological. Its natural identity
  is `run/item/generation`.
  An identical reservation request resumes; changed request fails.
- `dispatch` persists `dispatching` before calling `WorkersV2.ensure`, and binds
  its worker ID afterwards. The adapter MUST durably create-or-get by natural
  identity with exact request comparison (identity excludes mutable dispatch
  state/workerId). Lost acknowledgement repeats ensure, not unkeyed spawn. Hold
  the transaction lock across this short operation. A raw spawn-only adapter is
  invalid. N05 must connect this to the durable generic worker manager.
- `integrate` first enforces **internal computed lifecycle readiness**, including
  exact current candidate and F0–F8 evidence. Even a permissive integration adapter
  cannot bypass this guard. `IntegrationsV2.verify` must additionally verify the
  reconciled native Git landing (N04). The dependent-node fixture now executes
  actual lifecycle commands before fast-forward and persisted successor reload;
  it does not claim N04 transactional Git certification.
- `replace` retains the sticky lane, requires all lifecycle executions reconciled
  and the prior worker verified settled, consumes the replacement retry dimension,
  and invalidates lifecycle evidence before exposing a new generation. It never
  resets repair counters, finding dispositions, retained results or a retry stop.
- `pause`, `needs_replan`, and explicit disposition-based resume stop dispatch.
  Pausing a `needs_replan` run cannot bypass its required disposition.
- `cancel` durably fences unfinished generations before callers signal workers.
  `reconcileCancellation` requires verified worker/effect settlement before making
  the binding terminal. Cancelling runs cannot admit a successor. Complete or
  reconciled-cancelled bindings can be atomically replaced by an exact successor
  plan with predecessor selector; old runs remain unchanged and addressable.
- `releaseGate` accepts only declared runtime gates and a trusted verification
  callback. Model/semantic gates must instead require a successor plan.

## Direct lifecycle API

Each V2 work item now has a required `lifecycle` containing a grounded oracle
(statement, independent source references and F2 check IDs) and concrete checks.
The old human-readable `checks` strings remain review text, **not PASS evidence**.
Oracle/check grounding and non-applicability evidence must resolve to the plan's
hash-bound source references (hydrated by the N05 freshness adapter).
Every check binds its fixed F1–F7 stage, expectation, source references, required
or positively evidenced `not_applicable` applicability, command argv or registered
producer ID, environment profile and replay class. Every checkpoint has at least
one applicable check; every oracle reference names an applicable F2 check.
Unknown fields, custom stages and required non-repeatable procedures reject.
This intentionally small profile has no conditional-expression language, waivers,
non-replayable validity windows or executable-byte consent. Missing tools are
BLOCKED, never NOT_APPLICABLE. Unsupported cases need a plan/profile extension,
not a fabricated result. Existing pre-lifecycle experimental N02 V2 snapshots
without these required plan fields fail closed; they are not silently migrated.

1. `reserve` computes F0 **before implementation dispatch**, binding the frozen
   oracle, independently hydrated start baseline, risk, applicability and authority.
   Dispatch requires this frame. `setCandidate(mutation, item, generation, candidate,
   inspector)` then binds the natively inspected worker-authored F1 candidate;
   checks cannot execute until it is bound. The initial candidate is not a retry.
   The implementation worker stays generic; its completion claim advances nothing.
   `CommandRunnerV2` implements the candidate inspector. F1 checks observe the
   committed candidate; this service does not edit code on the worker's behalf.
2. `prepareCheck` persists an exact natural stage/check slot and execution request:
   whole-plan selector, run/item, worker reservation, generation, stage attempt,
   round, candidate commit/tree, procedure, environment and local effect scope.
   Exact prepare replay returns the same request, never a new invocation.
3. Outside the runtime transaction, `ResultsV2.ensure(request, signal?)` invokes
   the actual command/producer. `recordResult(..., executionId, results)` hydrates
   its durable result, checks the complete request, and records it once. There is
   no raw worker-report-to-PASS operation. Paused runs may ingest current results;
   obsolete results are retained as quarantined, never promoted or landed.
4. `advanceLifecycle(..., stage)` derives passage from every applicable individual
   result; missing/failing checks name their command and diagnostic. F2 and F5
   require fresh independent context identities, distinct from implementation and
   all prior contexts, with no predecessor reasoning lineage. F3 codification can
   use the implementation thread; any new candidate conservatively returns to F1
   and fresh F2 (no unproven evidence-only-delta optimization). F4 is no-edit.
   F5/F6 producers can report typed findings. F7 replays **every** applicable check
   on newly materialized clean exact-candidate worktrees. F8 is computed only after
   F0–F7, no unresolved blocking findings and no ambiguous execution remain.
5. `retryCheck` names an actual failed execution and derives the dimension/back-edge:
   product defects → F1; test gaps → F3; architecture issues → F5; infrastructure
   → same checkpoint; other hardening failures → F1. Unchanged earlier evidence
   survives a local typed back-edge; affected/later evidence is quarantined.
   Candidate changes invalidate all F1–F8 evidence. `dispositionFinding` retains an
   explicit immutable disposition. Plan-affecting findings atomically hold the
   whole run in `needs_replan`; resume requires disposition, not just a pause toggle.

Retry ceilings are product/test/review/hardening/integration 3, infrastructure 1,
replacement 2. Detailed keys retain stage/procedure/fingerprint, with a conservative
per-item/dimension ceiling as well, so renaming a check or cycling fingerprints
cannot reset budgets. Same-tree retries, recurring trees, persistent fingerprints
across repair trees and failure oscillation stop earlier. A stop is durable and
cannot be cleared by candidate changes or worker replacement. No lifecycle-wide
wall-time, token, launch or compute budget is introduced. N04 may use the exported
integration retry dimension; its Git-specific operations remain N04's work.

### Concrete execution and crash reconciliation

`new CommandRunnerV2(store, repository, producers?, environment = "node-local")`
provides argv-only `spawn` (no implicit shell), private detached Git worktrees,
native commit/tree and before/after cleanliness verification, fresh process
contexts for commands, bounded stdout/stderr (16,384 characters each), exact argv,
exit/signal/disposition, monotonic duration, wall timestamps, measured platform/
Node runtime and configured environment profile. The plan profile must match.
This is a local trusted execution profile, not an OS sandbox: approved procedures
must honor no-edit/local-effect contracts. It neither grants publication authority
nor installs dependencies or credentials. Commands must provision any required
local test dependencies explicitly; there is no hidden setup executable attestation.
Dirty/ambiguous worktrees are retained with actionable paths, never force-cleaned.

Command execution additionally requires Python 3.9+ on `PATH` with stdlib `ctypes`,
`os.pidfd_open` and `signal.pidfd_send_signal`, Linux subreaping/pidfd support,
and readable same-namespace `/proc`. Capability failure prevents launch. The
package-owned `command-supervisor.py` is resolved via `import.meta.url` and is
included by the package's `extensions/` files rule; no dependency installation or
candidate-relative helper lookup is used. The detached supervisor enables
`PR_SET_CHILD_SUBREAPER` before announcing readiness. Its launch is gated by a
synced request/boot/start/nonce journal and the current-intent launch lock.

The command's exit is separate from descendant extinction. Only kernel
`waitpid(-1)` returning `ECHILD` allows the supervisor to atomically publish a
synced nonce/identity/workspace-bound extinction receipt. The receipt/journal
remain until the lifecycle result is durably published, including a crash after
clean candidate removal but before publication. Fork/exit handoffs stay waitable
through adoption; no negative process-table scan proves settlement.
Abort, expiry or owner pipe loss initiates TERM/KILL escalation independently of
argv leader/owner survival. Positive `/proc` observations target same-session
processes through pidfds, never recycled PIDs; missed handoffs remain waitable.
This is not a sandbox against session/namespace escape or same-UID tampering.

Missing/corrupt extinction evidence after launch leaves the job running without
a lifecycle result and retains its workspace. A live supervisor without a receipt
must complete reaping. A dead supervisor without a receipt remains ambiguous even
if `/proc` looks empty: only the mandatory independent process/effect settlement
adapter can resolve that ambiguity. Receipt recovery after owner/acknowledgement
loss still requires that adapter and yields infrastructure BLOCKED, not invented
PASS or automatic cleanup/redispatch. Legacy journals have no reaping receipt and
likewise require independent settlement.

Registered `TrustedProducerV2.run` implementations are actually invoked in the
isolated workspace and must return a concrete observation and typed findings;
unused registry entries produce no evidence. F2/F5 worker-backed producers must
return the **actual independently hydrated context ID/lineage** from their durable
worker manager, not a generated label for this callback. Missing context or an
invalid protocol result settles BLOCKED. Generic worker identity stays DAG-agnostic;
N05 supplies its durable manager adapter and authenticates/hydrates its own local
producer results. The concrete command path already works without that adapter.

The same protected snapshot stores executor jobs separately from lifecycle result
application. A job is published before invocation; a result is published before
acknowledgement. The exact workspace is persisted and the generation rechecked
under the launch lock immediately before actual invocation, so cancellation
between job creation and start cannot launch an obsolete producer. Repeated ensure of an existing job never blindly spawns again:
settled jobs return stored results, unresolved jobs remain ambiguous. Result
publication retries only transient store-lock contention, not the command. If
publication fails without a durable result, do not infer success or retry the
procedure: after executor death, establish process-tree/effect settlement and call
`reconcileInterrupted`. Its settlement adapter receives the exact durable job,
including owner identity and workspace, and must establish process/effect settlement.
It records BLOCKED infrastructure evidence, enabling an explicit bounded retry. Parent death alone is not child/effect settlement.
`reconcileUnlaunched` proves under the launch lock that an obsolete intent never
had a job; it records non-execution so cancellation can terminate. Cancellation
fences first, then the caller signals workers/commands (an AbortSignal can terminate
the subreaper's descendant termination/reaping protocol), then ingests/quarantines
results and reconciles settlement.
A still-current host with an unresolved job is deliberately not presumed dead.

Trusted callbacks receive cloned inputs. These interfaces are local trusted
components, not human-consent proofs or security capabilities. They deliberately
have no permissive production default. N03/N04 may extend versioned snapshot
fields for direct evidence and Git operation state; do not smuggle PASS through
an unverified callback. Product tools should derive revision/lease internals and
call these closed operations, never expose `StoreV2.transaction` as a state-patch
tool.

## Native Git integration (N04)

N05 can call `new GitDriverV2(runtime, boundRoot).integrate(mutation, itemId,
generation, candidate, signal?)`. This is a concrete adapter, not a verification
callback placeholder. It persists native root/common/admin path/dev/ino bindings
and operation requests in the V2 snapshot, composes with explicit-base
`merge-tree`, creates a deterministic single-parent native commit, protects the
objects with exact-CAS private refs, and runs every prefix and final argv on
isolated exact-proposal worktrees through N03's subreaping command executor.
Requests/results survive fresh services; failure or ambiguity never fabricates
PASS. Internal lifecycle readiness remains mandatory. Long checks run outside
the snapshot lock. `verify` only accepts an independently hydrated native landing.

The supported landing profile is a **quiescent bound worktree**, Linux/local
filesystem, Git 2.54.0, files refs and SHA-1/SHA-256. It is not arbitrary same-UID
filesystem/editor isolation. Other worktrees may be used; another checkout,
reset, index writer or editor must not modify these exact session files during
landing. Unsupported required capabilities fail closed: sparse/case-insensitive
checkout, attributes (including ignored/untracked working attributes), gitlinks,
partial/shallow/alternate objects, overrides of the required built-in text merge
driver, and unsupported backend/version. UTF-8 commit encoding and the default
merge driver are pinned. Unrelated safe configuration and unused drivers/hooks are
allowed; composition semantics, user hooks, fsmonitor and automatic maintenance
are pinned/disabled rather than silently inherited. No lazy fetch is permitted.
No restricted external effects are supported. Local test argv are trusted local
procedures, not a network/credential sandbox or a grant to publish.

Git 2.54 `hook.<name>.command/event/enabled` hooks are disabled separately from
`core.hooksPath`. Each native invocation inventories hook names using Git's
config parser, including nested and dormant conditional includes (which can
activate in worktree-add children). Per-name command-scope overrides disable
those hooks without changing repository config. Validation/landing descendants
inherit the same per-name disabling; only landing's operation-owned hooksPath
is permitted. Names installed by validation are rediscovered before subsequent
observation, cleanup, recovery and closure; cached base options are insufficient.
Disabled hooks and unused events such as pre-push remain allowed. Referenced
config must be readable/parseable for discovery. Existing worktree-config profile
restrictions remain unchanged. Trusted argv can deliberately override its Git
environment or install and invoke a new hook within argv itself; this is not a
sandbox against the approved local procedure or concurrent same-UID config edits.

Landing is ordinary `merge --ff-only --no-autostash --no-overwrite-ignore`, with
an operation-owned reference-transaction hook instead of user hooks. At Git's
**prepared** phase, under its native ref locks, the hook requires the exact direct
branch/HEAD and expected-old/proposal tuple. Only supported ORIG_HEAD and
AUTO_MERGE ancillary operations are allowed. The ORIG_HEAD new-OID guard rejects
backward captured-HEAD drift before checkout on this supported profile; the final
branch guard independently rejects stale expected old. No raw target update-ref,
reset, stash, forced cleanup or automatic recomposition is used.

Ordinary merge is **not a filesystem transaction**. Failed/killed commands can
leave their own partial index/files. Reconciliation distinguishes old-clean,
new-clean, old/new-dirty, third and identity drift. New-clean is observed once
without another merge; acceptance still requires the current lease/generation,
authority and lifecycle. Old-clean allows at most two dispatches with exact stored
validation and current authority. Dirty/third/identity ambiguity is retained and
blocked, including after user edits or fresh reload. Foreign locks are never
removed. No exit code alone proves landing or absence of effects.

Lock order is common-dir then snapshot. V1 and V2 in this source share the same
non-unlinked flock inode. Before publishing intent, V2 durably claims the common
directory; it refuses unresolved V1 locks. The claim blocks V1 integration and
cross-store V2 successors until the named operation is accepted or explicitly
closed after observed settlement. Old installed/uncooperative V1 conductors must
not run alongside this profile. A landing/validation supervisor inherits the
common flock description so owner death does not release it while children act.
Missing subtree-extinction evidence blocks recovery, even if the owner died.
Hook/store locking cannot invert the lock order: the trusted hook never opens the
runtime store. All ambiguous protocol directories and verification worktrees are
retained, not force-removed.

`closeOperation(mutation, operationId)` can close a proven clean old operation
(consuming integration retry budget), or a clean old/new operation during
cancellation. It never releases consumers or cleans up bytes; running checks,
unsettled subprocesses, dirty/third targets and native drift still block. Normal
replacement/cancellation/successor APIs refuse unresolved Git operations.
Closure verifies the persisted native binding before opening/creating the common
lock, and rechecks binding and held-lock identity before claiming. An already
replaced common directory receives no lock, claim or other operation metadata.

`node scripts/dag-v2-git-test.mjs` retains the original unsafe-merge
characterizations. `node scripts/dag-v2-git-acceptance-test.mjs` exercises the
native adapter, real command validation, guarded races, process death and
fresh-service dependent-node integration. `node scripts/dag-v2-git-hooks-test.mjs`
adds real configured-hook markers across effective config, recovery, private refs,
worktree children and validation descendants. None is an OS-isolation claim.

## Limitations and verification

One project snapshot and project-wide short lock simplify cross-record atomicity;
large histories may eventually need compaction/partitioning. Dispatch adapters
must return promptly; long worker execution must happen outside the lock. Native
Git effects use N04's separate repository lock/CAS and reconciliation; N05 must
wire the concrete driver rather than a permissive verification callback.
No installed package, historical real run, project model or V1 writer is changed.

Run `node scripts/dag-v2-state-test.mjs` and
`node scripts/dag-v2-lifecycle-test.mjs`. The latter exercises actual commands and
invoked producers, complete F0–F8, missing/mismatched/stale results, failed argv,
independence, clean F7, candidate invalidation, typed findings/back-edges, retry
stops, cancellation, non-execution reconciliation and process death on either
side of durable result publication. State tests cover direct acceptance, inert
save/show/revision, scope/effects, real dependent Git landing and reload,
resource/mutex/gate/concurrency, process locks and owner death, integer CAS,
stale generations, acknowledgement loss, process death at publication boundaries,
terminal successor binding and V1 byte/path-preserving inspection. Regressions
also cover closed records/envelopes, rejected-write byte/revision preservation,
semantic output audit, serial lane deadlock/order, static symlinks, nonregular
files, and repository/.ai/store/lock/state replacement during transactions.
