# Simplified DAG contract and V1 coexistence

This hand-authored implementation contract refines the accepted direction in
[the generated runtime spec](spec.md) and
[the generated project-model spec](../mixed-initiative-project-model/spec.md).
It does not modify model authority or redefine persisted V1 schemas. It describes
new writers and their acceptance tests, not a claim that the cutover is shipped.

Governing decisions: `DEC-closed-content-hashed-plan-envelope-v1`,
`DEC-direct-user-commit-derived-review`, `DEC-one-artifact-two-phase-dag-planning`,
`DEC-run-pause-cancel-late-result-v1`, and
`DEC-tool-driven-dag-preserves-canonical-run-history`.

## Retained invariants

- Plans preserve outcomes, non-goals, architecture, grounded checks, semantic
  dependencies, repository baselines, effect limits, and integration obligations.
  Passing tests or a worker's completion claim cannot substitute for those facts.
- Exact selection and current governing-source/baseline validation precede start.
  Ambiguity returns bounded selectors; neither mtime nor an implicit latest plan
  chooses what the user meant. Changed selected content requires renewed intent.
- Only explicit run intent starts work. Scope/effect authorization remains an
  independent enforced boundary, even when the same instruction supplies both.
  Production, publication, new credentials, irreversible effects, and materially
  expanded scope require fresh explicit authority. Concurrency must be explicit
  and positive within authorized and validated maxima.
- Mutable writes use process-shared locking and expected integer revisions;
  durable snapshots use temp-write, fsync, rename, and directory fsync. Recovery
  never invents completion or retries an ambiguous effect without reconciliation.
- Attempts and cancellation/replacement generations fence stale results before
  signaling. Worker results advance only their exact current run/item/attempt.
  Persisted natural operation identities and request comparison prevent duplicate
  launches, completion application, and external effects.
- Semantic precedence, gates, capacity/mutex limits, retry/no-progress stops,
  exact candidate evidence, and guarded Git integration remain enforced by the
  existing execution substrate. A causal consumer waits for accepted integration,
  not merely authoring completion. Native target OIDs are checked before landing.
- A blocking plan-affecting finding atomically sets the whole run to
  `needs_replan`, preventing new dispatch and integration. Existing workers may
  settle. A confirmed semantic revision creates a distinct run; an explicitly
  dismissed or downgraded finding may permit resuming the unchanged run.

## New plan record: reviewable, without approval state

A versioned new record contains a stable plan ID, monotonic content revision,
optional predecessor reference, one canonical whole-plan hash, repository
baseline, concise governing source references, outcomes, non-goals, architecture
notes/risks, work items, checks, dependencies, optional concurrency constraints,
and integration profile. The version must distinguish it from V1; it must not
reuse a V1 discriminator with different semantics.

There is **no approval status, approval receipt/artifact, approval revision, or
approval transition** in the new plan contract. Do not encode approval under
`ready`, `accepted`, or a renamed equivalent. Review is an interaction and a
readable projection, not an authority state machine. A semantic edit writes a
new retained content revision; viewing, accepting, or starting an unchanged plan
does not increment its content revision. Execution status belongs to the run,
not to a mutable approval field on the plan.

`/dag plan` saves the revision and previews deterministic Markdown and graph.
`/dag show` is read-only for exact plan, node, lineage, and live-run views.
Markdown, graphs, exports, and contextual discussion are not editable authority.

### Direct run-intent boundary

1. Resolve one explicit selector or the one unambiguous contextually selected
   saved plan. Bind its ID, revision, and whole-plan hash. A historical selector
   is for inspection, not permission to execute a superseded revision.
2. Interpret explicit unambiguous user direction to run that exact current plan
   as acceptance of its semantic payload. Do not request an intervening approval
   operation, fabricate an interaction proof, or treat silence as acceptance.
3. Validate current plan/source/baseline equality, scope/effect authorization,
   repository eligibility, and concurrency. Missing authority blocks start;
   acceptance alone cannot widen authority. A changed or ambiguous plan requires
   clarification rather than applying stale intent to new content.
4. Under the appropriate consistency guards, persist one recoverable start
   intent and create or resume its exact canonical run. Derive low-level context
   and legacy substrate facts internally. Replayed identical intent resumes;
   conflicting intent or an active conflicting session binding fails closed.

Review-only requests, requests to revise, preview/export, and granting future
scope authorization without run intent cannot create a run, reserve work, or
launch a worker. “Revise this, then let me review it” is not run intent.

## Practical identities and evidence

- Stable IDs identify plan lineage, runs, attempts, findings, and effects.
  Monotonic revisions detect concurrent mutable writes; generations reject
  obsolete attempts. These are correlation/reliability markers, not capabilities.
- Keep one canonical whole-plan hash for portable exact content equality; keep
  semantic project-model object/selected governing-closure digests. Hash only
  independently hydrated artifacts and stored projection bundles where byte
  equality is needed. Locators are not artifact identity; verify size/type/digest
  and access restrictions before hydration.
- Use native Git commit/tree OIDs directly and structured repository/worktree
  identity. Keep useful config/result/environment and no-progress fingerprints.
  Do not wrap Git OIDs or inode identities in redundant hashes.
- Evidence retains exact plan/run/item/attempt/generation and candidate Git
  identity, applicable check and disposition, procedure/environment, authorization
  scope, bounded findings, and artifact references. Missing or stale required
  evidence blocks advancement. Retained candidates from cancelled or predecessor
  runs are source material only: revalidate adopted changes against the successor
  baseline and record fresh evidence for that run, never reuse old stage PASS.
  An immutable independently stored fact needs one
  content-address key, not a duplicate embedded self-hash or receipt chain.
- Remove blanket nested entity/schema hashes, parallel mutable snapshot hash-CAS,
  run hash chains, repeated tuple hashes, presentation/interaction receipt chains,
  mandatory compilation manifests, and phase-fact forests from new writers
  unless a concrete independently stored equality failure justifies them.
- Internal compatibility facts required by the shipped runtime remain internal.
  They must not be described as proof of human consent or reintroduced as public
  approval fields. Hashes do not authenticate intent against a trusted local agent.

## Immutable read-only V1 coexistence

Historical V1 plan, run, worker, Git, and evaluation artifacts retain their exact
bytes, identities, hashes, and original interpretation. Dispatch readers by
explicit version. Unsupported versions, corruption, or mismatched joins produce
bounded errors, never repair-on-read or fallback reinterpretation.

The new workflow may inspect V1 plans, nodes, lineage, and run projections, but
must not start new work from historical V1 selection, mutate its approval fields,
normalize its JSON, backfill fields, regenerate receipts, reseal hashes, or
silently migrate a store. Inspection must not create locks, indexes, snapshots,
or migration files. Derived in-memory projections are disposable.

Active V1 runs are not automatically converted: any continuing execution stays
under the existing V1 runtime and original rules, not the simplified writer.
Historical terminal runs remain addressable. Explicitly revising historical
content into the new contract creates a distinct record with a predecessor
reference, validates current scope and baseline, and requires fresh run intent;
it leaves the source untouched and transfers no execution evidence implicitly.

## Validation matrix

The commands below name existing regression suites; target assertions identify
what the cutover must prove. A named suite is not evidence that every target
assertion is already implemented. This contract-only change adds the V1 planning
read-byte regression; new-writer/start assertions belong to the cutover work.

| Boundary | Concrete failure prevented | Required assertion / verification home |
| --- | --- | --- |
| Model/spec consistency | Hand-edited generated output drifts from accepted authority | `npm run test:model`; `ProjectModelDomain.specs({ action: "check" })` at repository root returns no drift; leave model and generated specs untouched. |
| No approval ceremony | Hidden `ready`/approval gate prevents direct run intent | New schema rejects approval status/receipt/revision fields; review does not write a decision revision. Extend `scripts/dag-planning-test.mjs`. |
| Run intent | Review or revision launches work, or acceptance is applied to a changed plan | Exercise save → review → revise → show with zero runs/reservations/launches; explicit run of current selection starts without approval; ambiguous/stale selection blocks. Extend `scripts/dag-planning-command-test.mjs` and `scripts/dag-prepared-start-test.mjs`. |
| Scope/effects | Run intent silently authorizes publication or expanded scope | Missing/expired/narrow authority and invalid concurrency reject before start; fresh authority cannot retroactively authorize effects. `scripts/dag-prepared-start-test.mjs`, `scripts/dag-runtime-test.mjs`. |
| Historical reads | Viewing rewrites history or changes old hash semantics | Snapshot all stored paths and raw bytes; read/list/select/render historical draft and authorized revisions; compare complete snapshots. `scripts/dag-planning-test.mjs` test `historical V1 inspection preserves every stored byte and path`. Extend equivalent coverage to V1 run/worker/Git/evaluation readers during cutover. |
| Version separation | V1 is silently upgraded or executed with new semantics | New workflow rejects historical start/mutation and unknown versions without writes; old active runs remain on V1. Extend planning command/runtime suites. |
| CAS and durability | Concurrent revision loses updates; crash publishes torn state | Competing process lock/revision tests, failpoints at durable publication, restart/reconcile with no duplicated operation. `scripts/dag-planning-test.mjs`, `scripts/dag-runtime-test.mjs`. |
| Exact evidence | Old worker result or candidate advances current work | Reject mismatched attempt/generation/candidate and missing required checks; cancellation fences before signals. `scripts/dag-runtime-test.mjs`, `scripts/worker-runtime-test.mjs`. |
| Idempotency/recovery | Retried start/launch/completion repeats an external effect | Same operation and same request is a no-op/resume; conflicting request rejects; ambiguous effects block pending reconciliation. `scripts/dag-prepared-start-test.mjs`, `scripts/dag-planning-runtime-test.mjs`. |
| Scheduling/integration | Consumer starts too soon or drifted target lands | Producer integration required, capacity/gates honored, exact candidate and target OIDs checked, conflicts never auto-edited. `scripts/dag-runtime-test.mjs`, `scripts/git-integration-test.mjs`. |
| Replanning | Blocking architecture discovery permits more effects | Atomic whole-run `needs_replan` prevents dispatch/landing; dismissal needs explicit disposition; semantic revision creates distinct run. `scripts/dag-planning-runtime-test.mjs`. |
| Artifact safety | Wrong bytes or restricted content launches a worker | Verify digest/size/type and locator restrictions before hydration; bounded evidence only. Planning/runtime and worker suites. |

Failures in any retained protection block release; less ceremony is not a waiver
of correctness. Test results must distinguish exercised protections from pending
cutover assertions, especially beyond the V1 planning reader covered here.
