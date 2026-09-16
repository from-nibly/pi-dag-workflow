# V2 state and admission (N02)

This is a working local state service, not the product cutover. Import from
`runtime-v2/index.ts`. N05 must wire this service instead of the V1 writer; no V1
runtime module is modified here. Do not start V1 selection through this service.

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
- `integrate` accepts only the current bound attempt and native commit/tree
  identities. `IntegrationsV2.verify` is the mandatory N03/N04 trusted adapter:
  hydrate applicable actual F0–F8 evidence and exact reconciled Git landing, and
  reject missing/mismatched evidence. A worker's completion report is insufficient.
  The N02 real-Git test exercises dependency release after actual fast-forward and
  fresh persisted reload, not full N03 lifecycle or N04 transaction certification.
- `replace` retains the sticky lane and increments the generation only after a
  settlement verifier succeeds. N03 must enforce finite retry/no-progress policy
  and invalidate lifecycle evidence before exposing replacement in product tools.
- `pause`, `needs_replan`, and explicit disposition-based resume stop dispatch.
  Pausing a `needs_replan` run cannot bypass its required disposition.
- `cancel` durably fences unfinished generations before callers signal workers.
  `reconcileCancellation` requires verified worker/effect settlement before making
  the binding terminal. Cancelling runs cannot admit a successor. Complete or
  reconciled-cancelled bindings can be atomically replaced by an exact successor
  plan with predecessor selector; old runs remain unchanged and addressable.
- `releaseGate` accepts only declared runtime gates and a trusted verification
  callback. Model/semantic gates must instead require a successor plan.

Trusted callbacks receive cloned inputs. These interfaces are local trusted
components, not human-consent proofs or security capabilities. They deliberately
have no permissive production default. N03/N04 may extend versioned snapshot
fields for direct evidence and Git operation state; do not smuggle PASS through
an unverified callback. Product tools should derive revision/lease internals and
call these closed operations, never expose `StoreV2.transaction` as a state-patch
tool.

## Limitations and verification

One project snapshot and project-wide short lock simplify cross-record atomicity;
large histories may eventually need compaction/partitioning. Dispatch adapters
must return promptly; long worker execution must happen outside the lock. Native
Git effects still need N04's separate repository lock/CAS and reconciliation.
No installed package, historical real run, project model or V1 writer is changed.

Run `node scripts/dag-v2-state-test.mjs`. Tests cover direct acceptance, inert
save/show/revision, scope/effects, real dependent Git landing and reload,
resource/mutex/gate/concurrency, process locks and owner death, integer CAS,
stale generations, acknowledgement loss, process death at publication boundaries,
terminal successor binding and V1 byte/path-preserving inspection. Regressions
also cover closed records/envelopes, rejected-write byte/revision preservation,
semantic output audit, serial lane deadlock/order, static symlinks, nonregular
files, and repository/.ai/store/lock/state replacement during transactions.
