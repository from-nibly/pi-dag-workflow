# V2 product, state, lifecycle evidence and native Git integration

The extension registers `registerProductV2`, backed by `ProductV2`,
`PlanningFreshnessV2`, `ProductWorkersV2`, the concrete command runner and native
Git driver. Import library APIs from `runtime-v2/index.ts`. Historical V1 readers
remain version-separated; no V1 selection starts through this service.

## Registered product boundary

`dag_plan_save` derives native repository identity and source digests itself. The
frozen `source.selector` stores `workstream_scope_v2` and explicit workstream IDs.
Saving and planning require no saved focus or brainstorm session. Historical
`model_scope_v2` selectors remain readable unchanged; new revisions use explicit
scope rather than inheriting a saved discussion selection.
Save/run hydrate tracked authoritative model bytes, recompute all applicable
non-superseded accepted governing objects, and compare requested generated specs
with `SpecProjector.render`. Repository-wide objects always seed the closure;
empty workstreams add no other scope seeds. Accepted governing relationships
close transitively in both directions, excluding context-only `related_to` and
historical `supersedes` links. Unaccepted governing dependencies block saving,
but are advisory at run assessment. Context refs and drafts do not become model
authority. The optional selector in the library schema preserves primitive
fixtures; the product requires it. New runs retain a separate `acceptance`
observation: timestamp, current repository baseline, observed source when it can
be hydrated, and findings. Absent observed source means hydration was semantically
unavailable; saved provenance is never relabeled as fresh. Dispatch uses the
captured baseline plus accepted native Git prefix, not frozen saved source
checks. Old runs without an acceptance snapshot keep their original baseline
and checks; no automatic migration. Native landing recovery still owns
old/new/third reconciliation. Candidate inspection rejects model/requested-spec
changes, including clean committed edits.

`dag_plan_assess` accepts an explicit plan ID/revision and concrete bounded
scope/concurrency/local-effect configuration. Hash input is optional and
never required from the user. It reports content review, non-head selection,
source/baseline differences, unresolved model questions/directions (`REVIEW_REQUIRED`), scope subset, concurrency exceeding
the recommendation. The agent judges these against
the conversation; ask only about material unresolved concerns, not for another
approval. Configuration is not tool-verifiable evidence of consent.
`dag_run_start` and explicit `/dag run` JSON recompute and retain those findings
without requiring an assessment receipt/token. `/dag run` with no payload reopens
a binding or prompts discovery → inspect → assess → judgment → start/ask.
Saving/revising/showing/assessing never executes work. The agent must review relevant
unresolved decisions and pending reviews before dispatch, recognizing already-settled
channel-neutral conversation without requesting tokens. Lavish is optional.

Necessary scoped dependency restoration/feature dependency edits are allowed in
implementation workers; network, install scripts, credentials and external effects
must be considered separately. Verification retains its no-edit contract.

Model operations and migration now use explicit scope rather than durable focus.
Independent reviews in `project.reviews` share model atomic persistence; pending
review IDs are also surfaced as advisory `REVIEW_REQUIRED` findings. Semantic hashes
and receipts are not consent gates. Actual governing model/spec reconciliation is
pending parent review; these code changes do not rewrite governing content or history.

Hard boundaries: malformed plans/models, unknown revisions/items, unsafe paths,
unsupported versions/effects, non-closed dependency/serial scopes, concurrency
outside 1–512, native identity/dirty-state/target races, CAS, process leases,
generation fencing, worker settlement and actual command evidence. Resource and
mutex capacities remain execution invariants; the plan concurrency recommendation
is not a cap on supplied configuration. Execution authority has no wall-clock deadline. Existing bound scope cannot be
silently expanded, and paused work is not resumed. Reopening an identical bound
run returns its original acceptance history, not a fabricated new observation;
use `dag_plan_assess` for a new read-only assessment.

Historical V2 authority metadata is accepted only through an isolated, lossless
shape-validation view. Stored runs, reserved worker envelopes, command requests,
results and their hashes retain their original bytes/meaning; loading never
rewrites them. Historical time bounds do not block dispatch, recovery, checks or
landing. Public inputs and newly generated authority/check frames have no time
bound. Existing reservations still replay exactly; fresh worker direction and
replacement remain explicit, and paused runs still require explicit resume.
Commands and native landing retain independent one-hour per-invocation timeouts
for hung processes, plus cancellation, owner loss and descendant settlement.

The shipped product profile accepts actual `node-local` argv checks, not arbitrary
producer IDs. F2/F5 command processes perform declared deterministic oracle and
architecture checks independently; F7 replays every applicable check. This is not
a claim that a generated label represents a human/model reviewer. Custom trusted
producers remain a library extension point. Runtime gates require an actual
current PASS check with the same ID as the declared gate.

`ProductWorkersV2` uses the generic manager's durable keyed `launchOwnedAttempt`
and `launch`. Requests freeze task, native repository and base; reserved normalized
tool/model configuration survives changed host presentation. The generic
`explicitDispatchRecovery` policy suppresses scan-time launch of reserved/planned
work and generic retry: only an explicit caller holding current generation/CAS
may recover dispatch. The registered V2 host sets `autoRecoverOwned: false` so
legacy-owned reservations cannot auto-launch or generically retry either.
Planned cancellation is recorded as non-execution, never
PASS; replacement uses a fresh generation/key. V2 retains exact
storage/session/worker/attempt/nonce/config identity and ingested completion.
Candidate inspection checks owned worktree identity, raw clean detached native
commit/tree and base ancestry. Bounded replacement freezes the inspected prior
candidate and actual failure observations into the next generation's immutable
repair request. Old evidence is retained/quarantined and affected checks run again.
Each product node owns one durable `workspace` binding: stable `runId/itemId`, cwd,
and native root/common/admin directory identities. First dispatch lazily allocates
`v2-hash(runId/itemId)`, publishes the shared ownership receipt, then persists that
same immutable receipt on the node **before** launching. Interrupted allocation
replays the same path; existing adopted paths never move. The journal alone holds
mutable launch/execution ownership and epoch; the node does not mirror those claims.
New `workspaceProtocol: "node-owned-v1"` requests borrow the binding under the exact
launch key/request hash/current epoch. They create no disposable-root approvals,
and their normalized `borrowed_node` roots carry no manager cleanup authority.
Checks, repair and native integration consume this same binding. Setup belongs to
the node, not to an attempt. An explicit settled attempt handoff is still required.
`WorkerManager.relinquishNodeWorkspace(binding,nodeId)` inspects the exact settled
binding, including other manager stores and archives, and durably relinquishes
launch/retry/disposable-cleanup rights before checks may use it. Existing exact
legacy roots are adopted in place through this API; configs/results are not edited.
A repository-local overlap-aware registry flock serializes manager dispatch/cleanup and handoff;
its durable ownership record has no expiry. A dead owner does not clear an active
execution. Outstanding cleanup intents or unsettled attempts prevent adoption.
A new generation gets a new immutable base/request and launch key, not the old
launch's base check. Generic retention limits can require operator maintenance. Reports cannot PASS lifecycle checks. Generic
working-root ownership tokens remain process safety, not plan approval.

### Revising worker direction and retaining failed work

`dag_set_worker_direction({runId,itemId,generation,direction:{task,provenance}})`
records a complete current user instruction snapshot for that item. `task` is bounded
at 12,000 characters and `provenance` at 2,000 (a user-message/channel reference,
not a consent token). Pending first launches use it. Existing reservations do not
change: keyed replay always uses their exact old envelope. Direction is item-local;
set it explicitly for each pending item whose instructions changed.

`dag_start_work` and `dag_replace_worker` also accept optional `direction` with the
same shape. A supplied fresh task replaces the saved item's narrative rather than
appending a contradictory policy beneath it; immutable item identity, dependencies,
outcomes, oracle/checks, frozen paths and effect bounds remain. The caller interprets
current user intent and must supply the complete bounded task, not merely a delta.
Direction cannot waive verification, change governing sources, or expand native
effects. Without an explicit snapshot, first launch retains the saved item narrative.

Replacement archives the prior reservation unchanged and renders a new envelope.
Pre-renderer/legacy envelopes require an explicit fresh direction (passed to replace,
or previously set through the setter); there is no prompt-string migration. Neither
setting direction nor replacing resumes or launches paused work. Explicit conflicting
direction on replay fails; omit it to replay the reserved bytes.

Repair independently inspects the exact settled worktree even for `needs_attention`.
A clean eligible committed descendant can seed repair without `candidateReady` or
PASS. Dirty, attached, frozen-source-changing or otherwise ineligible work stays on
disk and blocks replacement without consuming a generation. There is no fallback
clone, reset, unknown-artifact deletion, or admission of dirty bytes. Arbitrary hashes in reports are never
selected. The packet includes exact binding/completion/result references, bounded
report/artifact excerpts, separate command stdout/stderr/diagnostic excerpts and Git
observations. These are observations, not instructions. Full lifecycle results remain
addressable by execution ID; full worker reports by exact binding/result path.
Candidate intake rejection is persisted and `dag_next_action` offers replacement,
not an unchanged completion-ingestion loop. Every replacement invalidates verification.

Tools derive leases/CAS internally but retain exact generation, stage-attempt,
completion and candidate selectors. `dag_recover_dispatch` only binds an already
attempted exact manager attempt after acknowledgement loss; it never launches.
A `dispatching` reservation with no manager attempt (for example, a prelaunch
approval error) instead resumes via `dag_start_work` with the **same generation**
while active. Resume a paused run first. Neither route consumes replacement
budget, changes the frozen request, or allocates a replacement root.

Historical frozen requests without `workspaceProtocol` retain their original
launch/config semantics; they are not reinterpreted as borrowed requests. Only
this compatibility path, after a same-session/storage process restart, may reuse its
original disposable approval through a durable, operation/request/owner-bound
launch handoff. This requires the exact settled prior binding, current node
ownership/epoch, unchanged path/device/inode, approved disposable parent and a
proven-dead original approving process. The original token, approver and old
configs/results remain unchanged; the handoff does not grant token reuse or
cleanup rights. Revoked, foreign, stale and incompatible approvals remain errors.
The adopted node binding persists independently of that historical provenance;
subsequent newly rendered generations borrow it without further approval handoffs.
Normal generic disposable-worker creation and cleanup are unchanged.
`dag_recover_execution` requires kernel subtree extinction and
native workspace settlement and records BLOCKED, never invented PASS.
`dag_close_git_operation` uses native settlement closure. Missing proof stays
blocked. Read tools and passive headless-safe widgets never call these writers.

`dag_history_v1` uses original plan/run/worker/Git/evaluation validators without
attachment, migration or repair. There is no registered V1 continuation writer.
A session retaining any V1 binding must use a new unbound session for V2;
terminal V1 bindings are not implicitly adopted either.
Do not run an older installed conductor concurrently with V2. Source changes do
not update the separately installed runtime overlay.

`node scripts/dag-v2-product-test.mjs` crosses actual extension registration,
generic manager/supervisor/results, real lifecycle commands and native Git.
Fractory successor and Operant safe-prefix tests are V2 product scenarios,
not relabeled V1 tests. Focused success is not joined release certification.

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
nested runtime proof hashes or snapshot history chains. The plan hash covers plan
content; source/model and generic worker config digests identify independently
stored inputs rather than granting consent.

## APIs and downstream boundaries

- `save`, `show`, `renderPlanV2`: immutable retained content revisions and inert
  inspection. No approval fields, status, revision, receipt or transition.
- `start({ intent: "run", sessionId, selection, authority }, storeRevision)`:
  selects exact retained saved content; computes advisory assessment and captures
  the current baseline independently; atomically starts or reopens (never resumes).
  Scope is dependency-closed and serial integration scopes are prefixes. Effects
  are currently restricted to `repository_local`; publication cannot be granted.
- `FreshnessV2.current(plan)` recomputes governing closure from actual sources.
  `observe(plan)` separates semantic hydration failures from native eligibility
  failures and returns a current baseline plus honest source observations. The
  product checks the target before and after hydration; a racing target is not
  advisory. Primitive adapters without `observe` use `current` as their observation.
  Neither adapter may echo unvalidated caller input.
- `executionRepositoryV2(run, plan)` supplies the acceptance baseline to F0/F8,
  worker bases, Git composition and persisted Git audits. `runPlanV2` still
  returns the unmodified historical plan; its hash never claims refreshed content.
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
  invalid. `ProductWorkersV2` connects the durable generic worker manager.
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
   through new independent invocations in the exclusively owned exact node worktree. F8 is computed only after
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
provides argv-only `spawn` (no implicit shell), exclusively owned node worktrees
for all product lifecycle and native composed-proposal checks (historical
requests without node-workspace identity retain their original sandbox semantics),
native commit/tree and before/after cleanliness verification, fresh process
contexts for commands, bounded stdout/stderr (16,384 characters each), exact argv,
exit/signal/disposition, monotonic duration, wall timestamps, measured platform/
Node runtime and configured environment profile. The plan profile must match.
This is a local trusted execution profile, not an OS sandbox: approved procedures
must honor no-edit/local-effect contracts. It neither grants publication authority
nor installs dependencies or credentials. Node lifecycle checks consume the
worker's existing ignored dependencies/build caches in the same cwd, retaining
them across checks and repairs. Raw tracked bytes/modes/index and nonignored
untracked paths must match the candidate before and after every invocation.
Native prefix/final checks consume that same ignored setup, but validate the exact
combined proposal, not the raw candidate. There is no extra check worktree and no
hidden setup executable attestation.
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
live in a per-execution protocol directory persisted in the job, never inferred
from a node cwd's parent. Node protocol evidence is retained; legacy absent fields
still resolve the original sandbox journal without rewriting historical identity.
Sandbox receipts remain until the lifecycle result is durably published, including
a crash after clean candidate removal but before publication. Node roots are never
removed, on success, failure, cancellation, recovery, or repair. Fork/exit handoffs stay waitable
through adoption; no negative process-table scan proves settlement.
Abort, per-command timeout or owner pipe loss initiates TERM/KILL escalation independently of
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
exclusively owned candidate workspace and must return a concrete observation and typed findings;
their promise must settle all owned descendants before resolving or rejecting.
unused registry entries produce no evidence. F2/F5 worker-backed producers must
return the **actual independently hydrated context ID/lineage** from their durable
worker manager, not a generated label for this callback. Missing context or an
invalid protocol result settles BLOCKED. Generic worker identity stays DAG-agnostic;
A custom worker-backed producer must supply and authenticate its own durable
manager results. The shipped product uses the concrete independent command path
and rejects arbitrary producer IDs rather than installing a placeholder adapter.

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

The registered product calls `new GitDriverV2(runtime, boundRoot).integrate(mutation, itemId,
generation, candidate, signal?, nodeWorkspace?)`. Product passes the exact settled
worker workspace for every new operation. This is a concrete adapter, not a verification
callback placeholder. It persists native root/common/admin path/dev/ino bindings
and operation requests in the V2 snapshot, composes with explicit-base
`merge-tree`, creates a deterministic single-parent native commit, protects the
objects with exact-CAS private refs, and runs every prefix and final argv in the
same retained node root through N03's subreaping command executor.
Requests/results survive fresh services; failure or ambiguity never fabricates
PASS. Internal lifecycle readiness remains mandatory. Long checks run outside
the snapshot lock. `verify` only accepts an independently hydrated native landing.

New operations use **ordinary-ff-v2-2** and an `accepted-prefix-v1` composition
receipt. The receipt binds the authorized execution baseline and the exact ordered
accepted operation IDs/proposals through `expected`. State audit rejects missing,
extra or unaccepted entries and unauthorized source bases. Native composition checks
commit/tree identities, the single-parent accepted chain and its expected endpoint,
then selects the latest accepted proposal proven ancestral to the candidate, or the
baseline if none is incorporated. It revalidates this selection on replay and acceptance.
A retained unlanded repair starting commit is **not** accepted base authority. A
parallel candidate need not contain the latest expected target: composition retains
that target and uses only the incorporated accepted prefix. Arbitrary worker parents
and automatic merge-base selection are never authority.

Historical **ordinary-ff-v2-1** requests retain their explicit execution-baseline
semantics, original proposal identity and receipt-free representation. They are not
upgraded on read/replay. Blocked and closed operations cannot be reopened: supported
closure, bounded generation replacement and fresh readiness produce a new operation.
Native command failures retain bounded stdout/stderr, status, signal and argv;
merge-tree exit 1 reports conflict stages/paths even with empty stderr. Its emitted
conflict tree is never a proposal.

The node journal distinguishes original, switching, composed, restoring and
restored states. An operation-scoped ownership claim excludes manager launches,
repairs, nested Git actors and cleanup throughout composition, checkout, checks
and restoration. Check launch gates and closure fencing share the snapshot lock.
Normal detached checkout uses `--no-overwrite-ignore`, disabled hooks/attributes,
preflight collision detection (including ignored paths), and exact pre/post raw
tracked bytes/index/modes. Commands get fresh execution IDs and distinct protocol
directories, not distinct working directories. Ignored noncolliding setup survives.

Before target CAS, restore the original candidate HEAD/tree and verify it again;
old lifecycle evidence remains evidence for that original candidate. Transition
supervisors inherit the native lock, and recovery requires positive extinction,
never owner death alone. Lost acknowledgements observe exact endpoints. Explicit
`dag_close_git_operation` can restore partial own checkout effects only after
checking every index entry and source path against the two journaled trees; third
bytes, foreign locks, identity drift and opaque directory-shape changes are retained
and block. Resolve reported user collisions/drift without editing the store, then
repeat closure. A missing extinction receipt remains unresolved.

Historical operations without a node journal keep their original requests/results
and supported closure route. Close a failed historical operation, then use bounded
worker replacement and fresh F1–F8 evidence; the new integration uses the retained
node root. Never rewrite old failed results to claim a same-directory PASS.

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
cancellation. It restores a journaled node candidate before releasing ownership,
without deleting unrelated files or releasing consumers; running checks,
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
Git effects use the separate repository lock/CAS and reconciliation through the
concrete driver rather than a permissive verification callback.
No installed package, historical real run or project model is changed.

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
