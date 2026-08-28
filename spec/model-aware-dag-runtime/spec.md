<!-- generated-by: pi-dag-workflow/project-model; view: SPEC-model-aware-dag-runtime; contract: 1; input: sha256:a5ba0f612e52da78b0876e792ba501903f893924f91d1f1cbcc7003d87a65a67 -->

# Model-aware DAG planning and execution

Current accepted planning, decomposition, fixed-node lifecycle, safe parallel scheduling, integration, recovery, live projection, and dogfood evaluation behavior.

## Accepted outcomes and values

_No accepted current content._

## Accepted planning and execution direction

<a id="obj-dec-one-artifact-two-phase-dag-planning"></a>

### Ship one focus-linked plan/show/run workflow over the existing runtime

`/dag plan` is one seamless focus-linked workflow: it establishes architecture and outcomes, internally decomposes them into executable work items, and saves one revisioned inspectable plan record. The record contains repository baseline, concise governing source references, outcomes, non-goals, architecture notes and risks, work items, checks, dependencies, optional concurrency constraints, and integration profile. `/dag plan` atomically writes the record and automatically previews deterministic human-readable Markdown and a static graph. `/dag show` renders exact plan, node, lineage, and live-run views without mtime-based selection. Explicit `/dag run` resolves the saved plan, validates its whole-plan identity and current baseline, derives low-level genesis/context internally, and creates or resumes one canonical run. Keep the shipped worker, scheduler, lifecycle, and Git runtime as the hidden execution substrate. Build the first vertical slice through ordinary reviewed coding and tests, then dogfood it on remaining release work. Defer mandatory compilation manifests, phase-fact forests, new receipt chains, and bootstrap-authority ceremony until field use demonstrates a concrete need.

**Rationale.** The missing value is a usable planning and inspection workflow, not more internal authority machinery.

<a id="obj-dec-fixed-evidence-producing-dag-node-lifecycle"></a>

### Use a fixed evidence-producing lifecycle for every DAG node

Every node uses the same guarded evidence lifecycle, while worker boundaries are chosen by evidence and context needs rather than one launch per state. F0 frame/preflight binds the independent acceptance oracle, risk, baseline, capabilities, and applicable checks. F1 build candidate runs one implementation worker with an internal edit → parse/build/typecheck/lint/static/security/architecture-fitness loop. F2 behavioral evaluation uses a fresh evaluator against the oracle. F3 codifies required behavior and repaired failures. F4 performs accumulated verification. F5 a fresh implementation and architecture review records typed findings. F6 performs risk-selected hardening: production defects return to F1 and then must pass F3/F4; test-sensitivity gaps may return directly to F3; material architecture issues return to F5/replan; equivalent findings and tool limits require disposition. F7 cleanly reruns mandatory checks. F8 emits integration-ready evidence. Failures follow typed back-edges with finite budgets and honest escalation. Node-specific data may add bounded context, acceptance scenarios, risk, and check applicability, but cannot add, remove, reorder, or replace checkpoints. The DAG integration train then rebases or composes parallel branches and verifies exact future combined states before completion. Advancement is a coalesced service-owned pump rather than a one-command pass: session attachment and asynchronous worker-state/completion changes wake reconciliation so durable procedure evidence is sealed and subsequent ready scheduler work is dispatched without another user command.

**Rationale.** This operationalizes the requested opinionated loop without paying top-level worker/context overhead for the high-frequency implementation/static feedback cycle, while preserving independent evaluation, review, bounded autonomy, architecture quality, and integrated-state correctness.

<a id="obj-dec-typed-incompatibility-first-dag-scheduling"></a>

### Use typed incompatibility-first scheduling without hard file boundaries

Assume implementation nodes may run concurrently unless evidence establishes a constraint. Encode producer/consumer and feedback dependencies as directed precedence; unresolved architecture and contract choices as prerequisite gates; non-directional semantic-write conflicts and shared mutable resources as mutex/capacity constraints; and merge/deployment order as a separate integration train. Use isolated worktrees and namespaced runtime resources, dynamic constraint discovery, risk-bounded speculation, combined-state validation, and adaptive fanout. Predicted and observed paths inform review/risk but never act as hard write allowlists or proof of independence.

**Rationale.** This separates true semantic dependencies from scheduling exclusions and integration order, preserving useful parallelism without distorting architecture.

<a id="obj-dec-canonical-live-dag-worker-widget"></a>

### Project live DAG and worker activity from canonical run state

Establish the direction to prototype and iterate a persistent width-aware ASCII DAG/subagent widget across several review turns before implementation. The target widget appears only while a DAG run is active and renders a topology slice with character/dot status marks, typed blockers/constraints, readiness, fixed node phase/status, retries, integration position, and linked owned-worker attempt state through a versioned DAG execution projection. It pairs the graph with a compact running-work list. Large graphs focus on active nodes plus immediate predecessors, successors, blockers, and ready frontier. Explicit commands/tools provide the full diagram, inspect, bounded tail, cancel, retry, and run controls. The generic worker runtime remains DAG-agnostic and never infers node state. Acceptance authorizes the exploration target, not immediate shipment; implementation follows prototype review and acceptance of the exact projection/layout contract.

**Rationale.** The redesign now supplies the canonical adapter boundary that was intentionally missing when the interim worker widget was deferred, but concrete monospaced topology prototypes must be reviewed before implementation.

<a id="obj-com-create-disposable-dag-widget-prototypes"></a>

### Create disposable ASCII DAG widget prototypes before accepting the layout contract

Continue the live DAG projection frontier by creating and presenting non-runtime, non-authoritative ASCII widget prototypes at narrow, normal, and wide terminal sizes. Use them to review the exact projection and layout contract before runtime implementation.

<a id="obj-dec-ten-state-ascii-dag-widget-glyphs-v1"></a>

### Use ten ASCII primary marks while keeping execution dimensions orthogonal

The v1 persistent DAG widget uses this ASCII primary-mark set: `.` pending/not active or ready; `>` correctness-ready for a free lane; `:` owns an active-node lane with no operation in flight; `*` has an active procedure, worker, or integration effect in flight; `+` is F8 integration-ready and waiting for its train; `@` is actively integrating; `#` is fully landed and receipted complete; `?` requires human/operator direction or has authority/runtime ambiguity; `!` has failure, retry exhaustion, deadlock, or integrity attention; `x` is cancelled or superseded. Primary precedence is cancelled/superseded, complete, decision/ambiguity, failure/exhaustion, integrating, integration-ready, in-flight, active-wait, ready, pending. The mark is a disposable scan projection only. Phase, retry, attempt, blocker, finding, worker, and train facts remain separate annotations, and color never carries unique meaning.

**Rationale.** The reviewed ten-state set keeps sticky waiting, active work, integration readiness, integration, terminal success, and attention distinguishable without collapsing canonical lifecycle dimensions.

<a id="obj-dec-horizontal-wide-adjacency-narrow-dag-widget-v1"></a>

### Use horizontal topology at normal and wide widths with adjacency fallback when narrow

The persistent widget uses a horizontal topology slice at widths of at least 96 columns, compact horizontal chain rows at 60–95 columns, and exact per-anchor adjacency rows of the form `<- predecessors -> successors` below 60 columns. Every representation uses the same deterministic active/attention/frontier slice, preserves causal edge direction, reports omitted upstream/downstream regions explicitly, and never represents mutex, capacity, gate, provider, or integration constraints as causal edges. A full topology-gutter or boxed DAG remains an explicit inspection projection rather than the persistent default.

**Rationale.** The reviewed horizontal family preserves useful edge shape in fewer rows, while adjacency rows remain legible when columns are too scarce for connector routing.

<a id="obj-dec-one-third-max-twelve-dag-widget-height-v1"></a>

### Cap the persistent DAG widget at one-third of terminal rows and twelve rows

The persistent widget row budget is `clamp(4, 12, floor(terminalRows / 3))`. The renderer allocates within that bound to a run summary, topology slice, active/attention rows, omission counts, and a read-only hint. When mandatory active/attention content exceeds the budget, it remains represented through deterministic counts and compact rows rather than increasing widget height or silently disappearing.

**Rationale.** A one-third adaptive budget provides more context on tall terminals while keeping the widget bounded and usable on small screens.

<a id="obj-dec-readonly-widget-conductor-agent-controls-v1"></a>

### Keep the widget read-only and route every control through the conductor agent

The persistent DAG widget is a passive read-only status surface. It never captures focus, handles input, opens an interactive control overlay, or dispatches a reducer command. Users control a run only by communicating intent to the conductor agent. The conductor may then invoke exact closed tools and guarded reducer operations under existing authorization, owner epoch, revision, idempotency, cancellation, retry, and reconciliation rules. Read-only full diagrams, inspection, blocker explanation, and bounded tails may be surfaced by the conductor through commands/tools, but the widget itself exposes no controls.

**Rationale.** The widget should preserve ambient visibility without creating a second mutation or interaction plane alongside the conductor agent.

<a id="obj-dec-attention-active-context-widget-overflow-v1"></a>

### Prioritize attention, then active lanes, then topology context under widget overflow

When the bounded persistent widget cannot show every relevant node, deterministic selection orders mandatory decision/failure/integrity/deadlock attention first, then sticky active lanes by admission sequence and canonical work-item identity, then current integration-train head, accepted ready-frontier ordering, and one-hop causal predecessors, successors, and exact blocker owners. Every omitted region is represented by stable direction/state counts; omitted active lanes are explicitly counted and named by stable alias where space permits. The widget never rotates, time-ages, or silently hides omitted work.

**Rationale.** Operational attention must not disappear behind future scheduler priority, while admission-stable active ordering preserves a comprehensible sticky-lane set.

<a id="obj-dec-stable-canonical-widget-node-aliases-v1"></a>

### Use stable canonical node aliases in the bounded widget

Every planned work item receives a stable ASCII alias `N` plus a zero-padded ordinal derived from immutable canonical plan order and sized to the plan node count. The alias never changes with focus, width, state, or run revision. The widget shows the alias plus a width-bounded cosmetic short title; titles truncate before identity fields. Users ask the conductor to inspect an alias to obtain the full work-item ID, content hash, oracle, effects, evidence, and bindings. Aliases are projection conveniences and never authority or reducer identity.

**Rationale.** Stable aliases keep long IDs unambiguous across narrow and changing focus slices without spending the entire widget width on identity text.

<a id="obj-dec-last-good-stale-readonly-dag-widget-v1"></a>

### Show only hash-validated last-good topology during transient projection churn

If an exact plan/run/bound-worker projection join temporarily cannot stabilize but a prior projection was fully hash-validated, the widget may retain that last-good graph only with a prominent `STALE READ-ONLY` header naming its source revision/hash, the newer observed revision when known, and a coarse age. It cannot be used as mutation precondition or current authority. Corruption, identity mismatch, ambiguous ownership without an exact read binding, or absence of a validated last-good projection suppresses the graph and shows one fail-closed diagnostic summary. Partial or generic-worker-derived graphs are never rendered.

**Rationale.** A clearly fenced last-good view preserves useful context during normal atomic churn while suppressing plausible but unvalidated partial state.

<a id="obj-dec-minute-bucket-dag-widget-elapsed-time-v1"></a>

### Render informational elapsed time in one-minute buckets

Persistent-widget elapsed durations use an explicit observation-time input rounded down to one-minute buckets. Time is a disposable layout input, not semantic projection identity, lifecycle authority, liveness proof, timeout, retry trigger, or budget. Minute-boundary refresh may update elapsed labels without changing topology ordering or node selection. Exact timestamps remain available through conductor inspection.

**Rationale.** Minute buckets provide useful duration context without second-by-second terminal churn or accidental timeout semantics.

<a id="obj-dec-exact-session-bound-single-dag-widget-run-v1"></a>

### Bind the widget to one exact session run and never infer latest

A persistent widget projects exactly one run named by an exact durable Pi-session/run binding receipt. Reload and resume validate and reconstruct that same binding. An exact direct fork may transfer it only under the accepted lineage and prior-owner safety rules. Other sessions never attach implicitly. Multiple eligible runs, missing binding, live ownership mismatch, or ambiguous ownership produces a read-only ambiguity summary until the user communicates selection/reattachment intent to the conductor. Timestamp, filesystem order, chat history, and “latest active run” never select authority.

**Rationale.** An explicit single binding prevents the widget from silently switching project/run context while preserving accepted direct-fork continuity.

<a id="obj-dec-nonterminal-visible-terminal-cleared-dag-widget-v1"></a>

### Keep every authoritative nonterminal run visible and clear only after durable terminal reconciliation

The bound widget remains visible for initializing, active, integration, paused, blocked, needs-decision, cancelling, recovery, and partial-landing conditions even when no physical worker is running. Sticky active-lane ownership, blockers, generation fences, effects, and integration reconciliation remain visible. It clears only after authoritative completion, cancellation, or supersession is terminal and all required effect/landing reconciliation is durable. The conductor emits a bounded completion message; historical status remains available by inspection. Terminal display never requires manual widget dismissal.

**Rationale.** Widget lifecycle must follow canonical run completion rather than physical-worker activity or transient UI state.

<a id="obj-dec-transition-signal-safety-rescan-stable-widget-join-v1"></a>

### Refresh from transitions plus a safety rescan and three-attempt stable join

Accepted reducer transitions and exact bound-worker/repository/fact ingestion signal widget refresh; bursts coalesce and low-level heartbeats do not directly rerender topology. A one-second safety rescan repairs missed event/watch delivery after reload or crash. Projection joins validate the plan, read/hash run snapshot `R1`, read only inputs named by `R1`, re-read as `R2`, and emit only when revision/hash match; they retry at most three times before applying the accepted last-good-stale or fail-closed policy. A separate minute-boundary tick updates elapsed labels only. Watchers, timers, and subscriptions stop idempotently on session shutdown without cancelling detached work.

**Rationale.** Signals provide low-latency updates, a bounded rescan repairs missed delivery, and the stable join prevents torn plan/run/worker projections.

<a id="obj-dec-nine-separate-typed-dag-conductor-tools-v1"></a>

### Give the conductor nine separate typed DAG run tools

The conductor agent receives exactly five read-only tools—`dag_run_status`, `dag_run_diagram`, `dag_run_inspect`, `dag_run_tail`, and `dag_run_explain`—and four guarded mutation tools—`dag_run_start`, `dag_run_control`, `dag_run_retry`, and `dag_run_reattach`. Read tools bind exact run/projection/subject/attempt/page identities and bounded sensitivity. Mutation tools compile to closed reducer commands and carry run nonce, owner epoch, expected revision, idempotency identity, payload hash, and explicit time; control has only pause/resume/cancel, retry names exact dimension/fingerprint/generation/effect state, and reattach requires exact lineage and ownership proof. There is no generic state patch, direct generic-worker retry shortcut, or widget control. Users communicate intent to the conductor rather than invoking these through the widget.

**Rationale.** Separate small tools preserve category and authority boundaries while keeping conductor intent inspectable and schemas narrow.

<a id="obj-dec-tui-only-persistent-dag-widget-v1"></a>

### Install the persistent DAG widget only in interactive TUI mode

Only Pi interactive TUI mode installs the width- and height-aware persistent DAG component. RPC, JSON, and print modes do not emit or imitate the widget. They expose the same semantic run information and transitions through the accepted typed tools/events, without a second fixed-width visual summary contract or unsolicited stream output. The TUI widget remains a disposable projection and its absence never affects execution.

**Rationale.** One exact visual contract avoids false parity and output pollution in headless modes while preserving semantic access.

<a id="obj-dec-contextual-human-usage-diagnostics-separate-prompt-eval-v1"></a>

### Keep human attention and model usage contextual and prompt evaluation separate

V1 records exact operator-blocker count/interval/coverage, human decisions, authorization changes, waivers, cancellations, successor plans, recovery dispositions, and active operator minutes only when explicitly observed. Authority wait and unattended/away time are distinct and never treated as agent performance. Dogfood runs may request a fixed optional three-item 1–7 post-run pulse—confidence in final state, cognitive effort, and interruption/flow burden—and retain each raw item without a composite. Exact bound worker results contribute model/provider, input/output/cache tokens, inference/request count when available, reported cost, repairs, and runtime errors with coverage; views report per started run, per completed item, and per accepted outcome and compare only the same model/profile/risk cohort. Missing usage is not estimated, pricing is not normalized, and no time/token/cost field is a scheduler input or budget. Prompts, transcripts, source, paths, and prose are excluded. Agent-prompt quality, LLM judges, and semantic transcript thresholds remain deferred to a separately authorized field-grounded corpus/rubric exercise.

**Rationale.** This follows multidimensional DevEx/SPACE guidance while preventing user availability, provider pricing, or speculative prompt judges from becoming misleading workflow scores.

<a id="obj-dec-quality-conditioned-parallelism-metrics-v1"></a>

### Measure useful parallelism as opportunity captured by evidence-creditable overlap

For valid same-clock intervals, an operation is creditable only when it contributes to current accepted evidence/integration lineage or produces a distinct actionable finding that receives a current disposition. Let `C(t)` be concurrent creditable operations and `R(t)` safely correctness-ready eligible operations. Report useful work `sum(creditable durations)`, useful average concurrency `sum(creditable durations)/autonomous elapsed`, useful overlap area `integral(max(0,C(t)-1))`, parallel opportunity area `integral(max(0,R(t)-1))`, opportunity capture `useful overlap area/parallel opportunity area`, work efficiency `creditable operation time/all operation time`, typed ready-to-admit/dispatch waits, false-independence incident count and wasted operation time, integration conflict/invalidation work, and serial/parallel paired deltas. No eligible opportunity, serial policy, unsupported clocks, partial coverage, or censoring reports an explicit non-measured state rather than zero. Peak workers, launch count, and sticky lane occupancy are diagnostics only. Higher concurrency never changes scheduler policy automatically.

**Rationale.** This conditions overlap on accepted evidence and actionable learning, distinguishes available opportunity from utilization, and exposes wasted false independence rather than rewarding raw fanout.

<a id="obj-dec-local-latest-fifty-evaluation-retention-v1"></a>

### Keep DAG telemetry local and retain latest fifty envelopes per project

V1 telemetry is repository-local and has no upload/export path. A `RunObservationAccumulatorV1` is deleted with expected identity after its terminal envelope has remained validated for seven days. Each project retains at most the latest fifty bounded final or explicitly censored `RunEvaluationEnvelopeV1` artifacts, ordered deterministically by terminal/cutoff identity; cleanup failure affects telemetry storage only. Cross-run reports are deterministic disposable projections over an explicit cohort of compatible evaluation-profile hashes. Before an envelope ages out, selected conclusions may be explicitly promoted into project-model evidence with exact source hashes. Telemetry never extends source, worktree, worker diagnostic, transcript, or restricted artifact retention.

**Rationale.** Fifty local envelopes are enough for single-user trend and paired analysis while bounding private telemetry growth and preserving intentional promotion of durable findings.

<a id="obj-dec-nonauthoritative-evaluation-envelope-accumulator-v1"></a>

### Use a reproducible evaluation envelope and a lossy non-authoritative interval accumulator

One strict canonical content-addressed `RunEvaluationEnvelopeV1` is generated per terminal run or explicit right-censored checkpoint and binds exact evaluation-profile version/hash, plan/run/source snapshot and receipt hashes, terminal/censor class, coverage, clock quality, uncompensated invariant results, outcomes, attempts, retries/back-edges, findings, integration, timing, useful parallelism, human attention, model usage, reported cost, and instrumentation status. Every metric carries measured, zero_exposure, unsupported_policy, not_observed, partial_coverage, or censored status plus numerator/denominator/missing/censored counts. A separate atomic `RunObservationAccumulatorV1` stores only currently open readiness/active/human/recovery intervals, fixed counters/sums/histograms, source revision/hash, and coverage gaps; it contains no transition list or scheduler decision bodies and folds closed intervals. Observation happens after authoritative commits, may drop on overload, never backpressures or affects execution, and missing data is never imputed. Envelope generation failure changes evaluation coverage only. Corrections explicitly supersede one prior envelope rather than form an event history.

**Rationale.** The envelope preserves reproducible bounded evidence; the accumulator captures queue/concurrency intervals that current snapshots cannot recover, without event sourcing or authority inversion.

<a id="obj-dec-three-strata-six-pair-dag-dogfood-v1"></a>

### Use a three-strata scorecard and six-pair prospective dogfood

DAG evaluation separates (1) uncompensated conformance/safety invariants, (2) quality-conditioned outcomes, and (3) explanatory diagnostics; no composite score allows throughput/cost to offset correctness. Deterministic property, failpoint, worker, scheduler, and real-Git matrices plus predeclared scripted scenarios are release evidence. Comparative local direction uses six valid pairs/twelve executions from identical committed baselines with fixed plan/oracle/model/profile/environment and counterbalanced serial `maxActiveNodes=1` versus selected parallel settings: two independent fan-out/fan-in, two hidden-constraint/architecture-pressure, one integration-train, and one recovery-sensitive pair. Separate provider/worker-loss, conductor-crash/resume, and target-drift/conflict drills test recovery but do not enter speed comparisons. Every pair is published with raw differences/ratios, median/range, and wins/ties/losses; no statistical or population claim. Any integrity breach, duplicate mutation, stale-result advancement, unreconciled effect, unauthorized scope, false completion, or architecture escape is an immediate stop. Otherwise review after six pairs; re-exercise every blocking fix and continue until three consecutive valid pairs add no new blocking failure class, with mandatory review by four weeks or twelve pairs.

**Rationale.** A small paired portfolio can establish local directional usefulness and expose regressions without fabricating precision from incompatible historical runs or averaging away safety failures.

<a id="obj-dec-separate-canonical-plan-from-operational-records"></a>

### Separate the immutable canonical plan from authorization, run, evidence, and worker records

`CanonicalDagPlanV1` is the sole immutable, content-hashed architecture/decomposition authority. A detached `PlanReviewReceipt` binds the exact plan and deterministic reviewed projection; a detached `PlanAuthorizationReceipt` binds the exact plan hash plus mechanically closed work-item, phase, effect, repository, budget, and validity scope. One atomic mutable `DagRunStateV1` references those hashes and owns readiness, phases, attempts, leases, integration, staleness, and dispositions. Immutable bounded evidence envelopes own observations; generic owned-worker results remain DAG-agnostic claims/runtime facts. The plan has lineage and per-entity hashes but never status, approval, attempts, leases, evidence bodies, worker state, or mutable capacity.

**Rationale.** This preserves one planning authority without recreating the corrupt shared-file pattern or making the plan a second project model, approval ledger, run database, evidence warehouse, or worker transcript.

<a id="obj-dec-selector-closure-plan-staleness"></a>

### Use versioned semantic selector closure and typed staleness

Every plan binds the full project-model revision/hash for audit plus a versioned selector and deterministic closure over relevant workstreams, explicit seeds, transitive governing relationships, effective authority states, and object semantic hashes. It also binds generated-spec projection input/content hashes, immutable repository commit/tree identities, schema/canonicalization/lifecycle/check profile hashes, and bounded context artifact digests. Freshness classifies `valid_exact`, `valid_revalidated`, `stale_model`, `stale_code`, `stale_schema`, `integration_drift`, or `unknown_impact`. A global model revision change may revalidate only when the selected closure and exact execution baseline are mechanically unchanged; a new or changed in-scope governing object stales the plan; unknown impact blocks. Target-branch movement is integration drift and invalidates combined-state evidence without silently retargeting execution.

**Rationale.** Global-hash-only invalidation needlessly discards plans, while explicit-object-only comparison misses newly added governing authority. A reproducible closure and typed drift preserve useful work without guessing.

<a id="obj-dec-multi-repository-plan-immutable-tree-baselines"></a>

### Represent repositories explicitly and execute only immutable tree baselines

Schema v1 contains `repositories[]` with stable root-independent identity, immutable commit/tree baseline, authority/input role, and per-repository integration train. Each executable work item writes at most one repository; verification may consume exact commits from several; cross-repository work is represented as coordinated per-repository trains and gates, never claimed atomic. The initiating repository model remains primary semantic authority, while external model/contract inputs are separately hash-bound. Planning may inspect dirty user state, but execution starts only from a user-approved immutable snapshot commit/tree created without cleaning, resetting, stashing, or overwriting the user worktree. If the executor cannot safely support a declared repository, validation rejects the plan rather than permitting undeclared absolute-path edits.

**Rationale.** Historical runs crossed repositories and lost ignored context, while detached worktrees cannot recover from a mere dirty-file digest. Explicit repository scope and immutable materialized trees make launch, recovery, integration, and authorization honest.

<a id="obj-dec-separate-work-items-gates-static-constraint-intent"></a>

### Separate executable work items, gates, and static constraint intent

The immutable plan stores executable change work items separately from non-executable architecture, contract, human-authorization, environment, external-precondition, and integration gates. Only work items use fixed F0–F8. One directed precedence graph represents producer/consumer and feedback causality; separate phase-scoped semantic mutex groups represent unordered exclusion; resource demand classes and semantic safety maxima are static intent while actual capacities/leases are run facts; per-repository integration trains order landing and exact-prefix validation without serializing authoring. Gates state whether release is bound semantic authority requiring plan revision or runtime evidence that may release in the run. Predicted and observed paths remain advisory. Run overlays may only narrow operations; discovering a missing semantic edge, effect, gate, outcome, repository write, oracle, or migration stage pauses affected work and requires a successor plan with fresh authorization.

**Rationale.** This gives validators, scheduler, integration train, and widget one unambiguous topology while preventing fake causal edges, frozen machine capacity, custom gate lifecycles, and silent dynamic widening of authority.

<a id="obj-dec-closed-content-hashed-plan-envelope-v1"></a>

### Use one canonical plan hash and practical identities elsewhere

Historical `CanonicalDagPlanV1` artifacts remain readable and immutable. New planning records use stable IDs and monotonic revisions for lineage and occurrence identity, plus one canonical whole-plan hash when exact portable plan bytes must be reviewed, cached, or joined across stores. Do not require blanket nested entity hashes, schema hashes, self-hashed receipts, or hash chains unless an entity is independently stored, compared, cached, or reused and a concrete content-equality failure requires it. Use process-shared locks with expected integer revisions for mutable CAS; generations for stale result fencing; durable temp-write/fsync/rename/fsync for snapshots; semantic hashes for project-model objects and selected governing closure; digests for independently hydrated artifacts and stored projection bundles; native Git OIDs without wrapper hashes; and stable natural operation IDs with persisted request comparison for external-effect idempotency. Hashes provide checksums, content addressing, fingerprints, and privacy-safe equality—not authentication or proof of human intent. Introduce versioned simplified writers only where needed and never reinterpret active V1 artifacts.

**Rationale.** Every retained identity mechanism must prevent a concrete local reliability failure; redundant hash layers add migration and validation risk without creating a security boundary.

<a id="obj-dec-versioned-governing-selector-closure-v1"></a>

### Use a versioned governing selector and fixed-point closure

`modelBinding` stores the full project ID/schema/revision/model hash for audit plus an independently hashed selector and closure. Selector roots are all effective, receipt-valid, non-superseded accepted intents, concepts, scenarios, decisions, and commitments whose repository scope or workstream scope intersects selected workstreams, plus explicit accepted seeds. The closure repeatedly includes receipt-valid current governing objects connected by inbound or outbound typed relationships, regardless of workstream, until fixed point. Entries sort by collection and ID and bind effective state, semantic hash, and acceptance content hash. Explicit non-governing evidence, question, proposal, or discovery inputs are separately hash-bound `contextRefs` and do not cause unrelated new research to stale the plan. Every consumed generated spec binds projection ID, projection contract, model-input hash, and content hash. Revalidation reruns the exact selector version: new or changed in-scope/connected governing authority changes the closure, while unrelated model metadata or unselected non-governing content may produce `valid_revalidated`.

**Rationale.** This catches newly added or cross-workstream governing authority without making every repository-model revision or research note invalidate useful planning.

<a id="obj-dec-structured-change-work-item-oracle-effect-v1"></a>

### Use one structured change-work-item, oracle, and semantic-effect contract

Schema v1 has one executable `change` work-item kind, and only change work items run fixed F0–F8. Architecture declares outcomes, non-goals, components, contracts, risks, and assumptions; global semantic subjects are typed behavior, invariant, contract, schema, decision, data, generated-source, or external-resource identities. Every work item binds stable ID/content hash, title/objective, exactly one write repository, outcome/non-goal/model/contract/oracle refs, bounded `extraContext` and content-addressed context refs, typed semantic reads/writes, risk tier/reasons/hardening profiles, required capabilities by phase, fixed-catalog check applicability with reasons, advisory path/symbol evidence with basis/confidence, and integration obligations. Read modes are observe/consume/validate; write modes are create/extend/replace/migrate/delete with compatible/breaking/unknown classification and a mandatory migration-protocol reference for migrate/delete or breaking replace. Every pre-F1 oracle binds accepted or independently grounded source refs and typed assertions containing subject, observation method/procedure ref, pass condition, failure signals, tolerance, environment profile, and required evidence class. Every outcome and work item has an oracle; implementation output, path prediction, test-passing alone, or a worker completion claim is insufficient. Work items cannot define phases, flows, worker boundaries, file permissions, or arbitrary procedures outside bound catalogs.

**Rationale.** This gives every node bounded task-specific context and independently grounded success criteria without letting node data customize the lifecycle or distort architecture for parallel execution.

<a id="obj-dec-typed-gate-migration-constraint-unions-v1"></a>

### Use disjoint phase-scoped gate, migration, and constraint unions

Schema v1 gates are non-executable records with kind model-authority, contract, human-authorization, environment-capability, external-precondition, or integration; subject/authority/evidence refs; release mode `plan_revision` or `run_evidence`; typed predicate; and phase-scoped work-item blockers. Semantic/contract resolution requires a successor plan; operational evidence may release inside the run. Precedence represents only producer-consumer or feedback causality with subject, evidence, and release disposition. Semantic mutex groups are unordered phase-scoped memberships with subject, reason, confidence, and evidence. Resource classes declare units, namespacing, and semantic safety maxima; work items declare phase-scoped demand; actual capacities and leases remain run state. Per-repository integration trains declare members, partial integration precedence, strategy, and prefix/final validation profiles without implying authoring precedence. Migration protocols bind subject/from/to, atomic or expand-contract strategy, typed expand/dual-support/backfill/switch/contract stages, compatibility matrix, rollback, and work-item/gate/oracle refs. Breaking persisted/external migrate/delete requires expand-contract unless explicit accepted atomic-risk disposition proves mixed-version safety unnecessary. Every record carries a content hash and category validation forbids fake causal edges, opposite-edge mutexes, frozen machine capacity, or hidden landing order.

**Rationale.** These disjoint unions encode why work cannot proceed concurrently and how it safely integrates without conflating causality, exclusion, capacity, migration, and landing.

<a id="obj-dec-content-addressed-bounded-plan-artifact-policy-v1"></a>

### Use bounded content-addressed planning and artifact storage

Store mutable planning transaction snapshots under ignored repository-local `.ai/dag-plan-transactions-v1` and immutable canonical plan bytes, phase facts, receipts, projection bundles, and lineage indexes under ignored `.ai/dag-plans-v1`. Publish exact candidate bytes without regeneration. Every context/evidence reference uses `ArtifactRefV1` with SHA-256 digest, byte size, media type, optional schema, sensitivity, retention, and optional validated locator; locator is never identity. Hydration verifies digest, size, and type before launch; missing or restricted required content blocks. Plans and durable artifacts contain no secrets, worktrees, dependency trees, arbitrary directories, or source copies. Human-readable Markdown and diagrams are deterministic on-demand or exportable projections and never editable authority; a later explicit projection policy may track generated plan Markdown, while width-specific layouts remain disposable.

**Rationale.** Repository-local ignored content-addressed storage provides crash recovery, exact publication lineage, bounded hydration, and readable exports without making transient planning state or projections authoritative.

<a id="obj-dec-broad-coordination-plan-authorization-v1"></a>

### Default to broad whole-plan coordination authorization with independent approval

The default detached authorization receipt binds exact plan, review receipt, and reviewed-projection hashes and authorizes all dependency/effect/integration-closed work within the plan maximum, including F0–F8 activity, architecture-preserving repository edits, worktree/commit operations, exact verification and landing, and reviewed reversible non-production evaluation scopes within budgets and retry policy. The final `/dag plan` Lavish turn may offer exact plan approval and run authorization together, but they remain independent decisions with separate content-bound receipts; the user may approve without authorizing. Authorization binds exact scope, effects, repositories, targets, retry ceilings, validity, and max concurrency. Neither approval nor authorization starts execution; explicit `/dag run` does. Fresh authorization remains required for materially new semantics, undeclared repositories/targets, production deployment, irreversible effects, new credentials/effect scope, or budget escalation. Optional partial authorization must be mechanically closed and can only narrow the plan maximum.

**Rationale.** One review minimizes ceremony while preserving the distinction between accepting architecture and authorizing external effects.

<a id="obj-dec-deterministic-plan-projections-v1"></a>

### Derive all plan and execution views through `/dag show`

`projectionContract` binds versioned pure projections for architecture/outcome review Markdown, decomposition review, static typed graph, bounded node execution packet, normalized scheduler indexes, optional explicitly non-executable legacy inspection, and `DagExecutionProjectionV1`. Every projection identifies plan hash, projection kind/version, and joined run/worker input hashes; ordering and focus tie-breaks are deterministic. Node packets contain the work item and complete referenced outcomes, contracts, oracles, context, artifacts, and policy facts without broadening authority. Human-readable Markdown remains a first-class generated projection, not authority. Use `/dag show` as the canonical read-only namespace for static plans, exact nodes, and exact session-bound live overlays; automatically preview the human plan and static graph after each valid `/dag plan` revision. Ambiguity produces a bounded selector list rather than choosing latest or mtime. Keep legacy `dag_diagram` explicitly legacy and keep Lavish/exports non-authoritative.

**Rationale.** One exact inspection namespace preserves readable planning and live execution without creating editable projections, implicit selection, or a second status store.

<a id="obj-dec-stage-evidence-disposition-finding-contract-v1"></a>

### Use conductor-sealed stage evidence and small orthogonal lifecycle enums

Every F0–F8 stage attempt is identified by immutable `StageAttemptInputV1` and may advance only through conductor-validated `StageEvidenceV1`; generic worker `completed` or `succeeded` is only a runtime fact. Check dispositions are exactly PASS, FAIL, BLOCKED, WAIVED, NOT_APPLICABLE, and BUDGET_EXHAUSTED. Current stage state is separately pending, active, passed, failed, blocked, budget_exhausted, cancelled, or invalidated. WAIVED and NOT_APPLICABLE apply only to plan-bound catalog checks; every stage still runs and emits an envelope, and stage outcome is mechanically derived. Finding kinds are product_defect, test_evidence_gap, architecture_issue, oracle_contract_issue, infrastructure_failure, capability_absent, external_precondition_failure, and equivalent_nonactionable, with separate severity advisory/blocking, materiality local/plan_affecting, domain, normalized fingerprint, and semantic subject. Evidence binds exact plan, work item, oracle, model closure, candidate generation, worker attempt/result, commit/tree, procedure, environment, authorization, findings, side effects, budgets, and bounded artifacts. Corrections, invalidations, finding resolutions, waivers, and adoption are immutable hash-linked records rather than edits.

**Rationale.** Small orthogonal enums prevent worker/process status, check disposition, semantic finding, and node state from collapsing into one ambiguous status while still supporting exact legal routing and recovery.

<a id="obj-dec-lifecycle-worker-freshness-invalidation-v1"></a>

### Use one F1 thread, fresh F2/F5, and conservative evidence invalidation

F0 and F8 are conductor computations. F1 is one logical long-lived implementation/static thread across product-repair back-edges; physical replacement records a handoff and never resets lineage or budget. F2 is a fresh read-only evaluator for each behavior-bearing candidate. F3 normally reuses F1 for codification. F4 is a deterministic no-edit runner. F5 is a fresh read-only reviewer with context lineage distinct from F1 and F2. F6 uses deterministic tools and a fresh adversarial evaluator only when generation or judgment requires one. F7 uses a newly materialized clean no-edit environment. Fresh F2/F5 packets include exact plan/oracle/candidate/evidence inputs but exclude hidden predecessor reasoning. F3 may preserve predecessor F2 evidence only when a hash-bound repository procedure mechanically attests an evidence-only delta; unknown or behavior-bearing changes route through F1 and fresh F2. Any candidate change after F5 invalidates F5 onward. F7 replays mandatory automatable behavior on the exact final tree; non-replayable evidence needs an exact validity window or visible waiver.

**Rationale.** This preserves useful implementation continuity without turning each phase into a worker, while protecting independent behavioral and architecture evidence and handling F3's intentional test changes without pretending every stage observes one tree.

<a id="obj-dec-lifecycle-applicability-waiver-effect-retry-v1"></a>

### Use plan-bound applicability, narrow waivers, and replay-classed effects

Every catalog check is plan-declared required, conditional under a deterministic predicate, or not_applicable with positive semantic evidence; F0 confirms applicability. Tool, credential, capability, budget, or environment absence and any observed failure are never NOT_APPLICABLE. Waivers are detached exact-scope human or accepted-policy receipts, never worker/conductor self-authority, apply only to checks, name residual risk and compensating evidence, remain visible, and by default expire on relevant plan, oracle, candidate tree, environment, successor-plan, rebase, or target change. Plan/review/authorization identity, immutable baseline, evidence/artifact integrity, existence of a candidate, independent F2/F5, clean exact-tree F7, deterministic F8, unresolved side-effect reconciliation, and authorization for new/production/irreversible scope are non-waivable. Every external procedure declares replay class pure, idempotent, compensatable, non_repeatable, or unknown. Durable effect intent and observed-operation reconciliation precede retry; unknown and non_repeatable effects never auto-retry.

**Rationale.** This prevents unavailable tools and uncertain effects from being relabeled as success while keeping risk acceptance explicit, narrow, visible, and authoritative.

<a id="obj-dec-lifecycle-retry-only-limits-v1"></a>

### Limit lifecycle retries without global execution budgets

Lifecycle limits apply only to retries. Product repair, test/review/hardening rework, infrastructure retry, worker replacement, and integration retry use separate counters keyed by work item, stage, procedure, failure class, and normalized stable fingerprint. Default v1 allows three product-repair retries per fingerprint/class, one controlled infrastructure retry per procedure/fingerprint, and two replacement-worker retries per stage. Autonomy stops earlier when the same fingerprint survives two materially different repair commits, two consecutive retries make no material progress, a prior candidate tree recurs, or an A→B→A failure oscillation repeats. Infrastructure failures never consume product-repair counts; cancellation, loss, renamed checks, or replacement workers never reset counters; and retries involving effects remain constrained by replay class and reconciliation. V1 imposes no lifecycle-wide wall-time, token, cost, stage-entry, worker-launch, compute, or artifact ceilings; bounded artifact retention remains governed by the separate artifact policy. Increasing retry ceilings beyond the authorization receipt requires fresh authorization.

**Rationale.** The user wants finite protection against retry loops without imposing broader execution budgets. Separate retry counters and mechanical no-progress detection address historical looping while avoiding arbitrary time, cost, launch, and total-work ceilings.

<a id="obj-dec-integration-ready-completion-predicate-v1"></a>

### Separate local completion and release readiness from explicit publication

F8 remains a deterministic no-edit integration-readiness seal, and canonical plan completion means changes are locally composed, landed, and verified in required combined state. Release readiness is deterministic but impact-aware: determine changed files from an explicit release base, classify every relevant path through a fail-closed impact map, run affected focused suites and selected canonical dogfood groups, portfolio templates, and recovery drills, then run one broad smoke pass against the extracted npm artifact. Exact content-addressed scenario receipts may be reused only when all relevant source, fixture, policy, toolchain, and environment inputs are unchanged. Unknown paths and broad canonical runtime changes escalate to the full gate, and periodic full certification remains independent of ordinary low-risk releases. A passing check means the repository is ready for an explicit human-operated push, tag, or package publication. `/dag run` never publishes remotely. Defer `/dag release`, remote push state, credential schemas, and release receipts until explicitly requested or justified by field failure.

**Rationale.** Release readiness is required now; remote release orchestration is separate. Impact selection and exact evidence reuse keep most releases below one hour without allowing path heuristics or throughput to weaken runtime-critical certification.

<a id="obj-dec-closed-atomic-dag-run-state-reducer-v1"></a>

### Use one closed atomic DagRunStateV1 and pure guarded reducer

`DagRunStateV1` is the sole mutable DAG execution authority and is a strict canonical snapshot, not an event log. It contains immutable plan/review/authorization identity; owner, desired, current, repository, work-item, gate, precedence, resource, mutex, lease, attempt, launch, worker-binding, evidence-index, finding, retry, blocker, effect, cancellation, quarantine, freshness, integration-train, and completion projections. It stores immutable fact references and current pointers only—never plan/evidence bodies, worker transcripts, secrets, source copies, or mutable generic worker state. `reduceDagRunV1` is pure and accepts only a closed command/observation union carrying run nonce, owner epoch, expected revision, command/idempotency identity, payload hash, and explicit time. Natural entity slots make exact replay a no-op and conflicting replay an error; otherwise stale revision/epoch rejects. The reducer derives readiness and notices, validates invariants, and advances revision/previous-hash/snapshot-hash once. Every launch, cancellation, repository, procedure, external, and integration operation requires a persisted effect intent before dispatch and a later observation/reconciliation. Transition notices and widgets are disposable projections, never authority.

**Rationale.** One small deterministic mutation boundary prevents the historical pattern of plausible but invalid state mutations, while avoiding event sourcing and keeping immutable evidence and generic process facts outside the mutable snapshot.

<a id="obj-dec-process-shared-dag-conductor-ownership-v1"></a>

### Use process-shared locking, owner epochs, and direct-fork-only automatic transfer

Exactly one conductor may mutate a run. The atomic snapshot store acquires a process-shared OS/filesystem exclusive lock, validates snapshot schema/hash, and compares expected revision, previous hash, owner epoch/token, and PID/start identity before invoking the reducer. Successful writes fsync temporary content, atomically rename, and fsync the parent directory before effects become dispatchable. Run conductor ownership is independent from generic worker-session/completion-delivery ownership. An exact direct Pi-session fork may automatically transfer the run only with hash-bound lineage and proof that the source owner is absent/dead or is the same manager; all other attach/takeover requires explicit reattachment plus proven-dead prior ownership. Live, mismatched, concurrent, or ambiguous ownership blocks mutation and exposes read-only diagnostics; lock expiry or stale heartbeat alone never proves death. Operational liveness must not be inferred solely from a long-lived Pi session-wrapper PID: each attached conductor service holds an in-memory exact owner generation, and a fresh service in the same exact session/process must CAS-transfer to a new same-manager epoch before mutation. That epoch transfer fences any prior command-scoped pump even while the host wrapper remains alive. A different process still requires the existing proven-dead/direct-lineage rules. A conductor error terminates the current pump immediately: every caller sharing that pump receives the same rejection, and no dirty or background wake may silently create or adopt a successor retry. The error must bubble to the top-level agent for diagnosis and explicit handling; only a later explicit top-level retry may clear the latched fault and start a new attempt.

**Rationale.** Process-local promise queues and atomic rename cannot prevent two Pi processes from losing updates. Direct-fork continuity remains convenient without allowing ambiguous or arbitrary ownership transfer. Terminal error propagation and explicit fault clearing prevent deterministic authority failures from becoming hidden retry loops that wedge the top-level agent and shutdown.

<a id="obj-dec-run-pause-cancel-late-result-v1"></a>

### Pause plan-invalid runs simply and fence cancellation generations

Ordinary pause stops new worker, procedure, effect, and integration dispatch while already-running current-generation attempts may settle and ingest normally. Cancellation and replacement fence affected generations before external signals; old-generation results remain quarantined and cannot advance or land. When a blocking plan-affecting finding is recorded, the same atomic run update sets the whole run to `needs_replan` and blocks all new dispatch and integration. Running workers may settle and their branches/results are retained. Agent/user review classifies the finding: a confirmed semantic plan change creates a revised plan and distinct run, while a dismissed or misclassified finding may be downgraded and the existing run resumed with an explicit disposition. Defer affected-closure continuation, mandatory successor on dismissal, automatic candidate-adoption receipts, and cross-plan evidence transfer. Prior output may be bounded context and must be revalidated according to actual changed scope.

**Rationale.** A simple whole-run pause closes the concrete unsafe-dispatch gap without prematurely building a complex successor protocol.

<a id="obj-dec-run-authorization-successor-candidate-adoption-v1"></a>

### Compose hash-bound authorization sets and limit v1 successor reuse to candidates

A canonical immutable `AuthorizationSetV1` contains sorted currently valid receipt hashes and mechanically derives the effective work-item, stage, repository, effect, integration, credential, retry, and validity scope. Multiple partial receipts may union only within the immutable plan maximum and only when the resulting scope is dependency/effect/integration closed. Every stage attempt and external effect binds the exact authorization-set hash; later authorization never retroactively authorizes earlier work, and expiry/revocation creates a new set that fences only affected continuing effects and integration. A successor plan always creates a distinct run ID/nonce linked to the old run and never changes the old run's plan hash. V1 permits only exact candidate commit/tree reuse through a detached equivalence/adoption receipt; all F0–F8 lifecycle evidence reruns under the successor plan. Direct cross-plan stage-evidence adoption is deferred until dogfood demonstrates sufficient benefit to justify its equivalence and freshness complexity.

**Rationale.** Canonical receipt composition supports useful partial authorization without boolean drift. Candidate-only successor reuse is conservative, understandable, and avoids making evidence-adoption correctness a v1 release blocker.

<a id="obj-dec-balanced-retry-dimension-defaults-v1"></a>

### Complete retry-only defaults with balanced per-dimension ceilings

V1 retry ceilings are product repair=3, test-evidence rework=3, review rework=3, hardening rework=3, infrastructure=1, worker replacement=2, and integration=3 per canonical work-item/stage/procedure/failure-class/fingerprint key. No-progress, repeated-fingerprint, repeated-tree, oscillation, unreconciled-effect, or authorization stops may terminate earlier. Counters remain separate and cancellation/loss/renaming never resets lineage. A plan-bound authorization profile may narrow or explicitly raise these retry ceilings; exceeding the current receipt requires fresh authorization. These are the only lifecycle limits and introduce no non-retry budgets.

**Rationale.** Uniform semantic retry ceilings are simpler to remember and configure, accepting more repeated review, hardening, and integration work.

<a id="obj-dec-session-worktree-conductor-topology-v1"></a>

### Bind each brainstorm/DAG session to its current worktree and branch

Each brainstorm and DAG session runs from a Git worktree directory. The conductor/top-level agent is bound to the branch checked out by that current working directory, including `main` when the user chooses to launch there. Every DAG node executes in its own separate worktree rather than sharing the conductor worktree.

**Rationale.** The session should use the branch context deliberately chosen by the user while isolating node implementation work from the conductor and from other nodes.

<a id="obj-dec-session-worktree-automatic-fast-forward-v1"></a>

### Automatically fast-forward the bound clean session worktree

After exact proposal verification, the conductor automatically lands through ordinary worktree-aware fast-forward Git in the session's bound current working directory. Before dispatch it persists landing intent and revalidates exact common-dir/worktree-admin identity, the originally bound direct branch, expected-old HEAD/tree, clean index and files, owner epoch, proposal commit/tree, and that the target branch is not checked out in another worktree. Git updates branch, index, and files together. Dirty state, branch change, duplicate checkout, target drift, identity mismatch, non-fast-forward state, or another live session blocks without stash, reset, clean, forced checkout, raw checked-branch `update-ref`, or silent branch switching.

**Rationale.** The current worktree is explicitly the conductor's session worktree, so a guarded ordinary fast-forward is both safe and more useful than an extra handoff while avoiding the stale-index hazard of raw ref movement.

<a id="obj-dec-explicit-base-merge-tree-synthetic-train-v1"></a>

### Use explicit-base merge-tree, synthetic first-parent commits, and no conductor conflict edits

V1 supports exactly one composition strategy: fixed-profile `merge-tree --write-tree` with the plan-bound source base supplied explicitly, followed by a deterministic one-parent `commit-tree` integration commit. Source candidate commit/tree and fixed Pi-DAG attribution are retained in immutable receipts and trailers; worker commit topology is not replayed. Git patch IDs and range diagnostics are advisory only. Any textual, rename/delete, custom-driver, semantic, or ambiguous conflict produces no accepted composed tree, fences the attempt, and routes implementation to a new candidate generation through F1 and fresh affected evidence. The conductor never edits conflict content or owns sequencer continuation.

**Rationale.** This minimizes mutable Git recovery state and avoids inventing a second implementation workspace inside integration. Exact combined-tree checks, not clean composition, prove behavior.

<a id="obj-dec-exact-prefix-every-landing-long-lock-v1"></a>

### Verify every future prefix fully and hold one repository lock through receipt acceptance

Every train entry runs all plan-required entry/prefix and repository-final profiles on its exact future synthetic commit before target movement. Speculative evidence is adoptable only under exact prefix commit/tree, ordered predecessor candidate/receipt-set hash, profiles, procedures, environment, cross-repository tuple, and authorization equality. A process-shared per-common-dir integration lock is held from authoritative composition through landing reconciliation and receipt acceptance, while Git CAS still guards nonparticipating users. Target drift fences the attempt, invalidates F8 integration readiness and affected combined evidence, and requires an authorized new train generation with fresh composition and verification.

**Rationale.** Repeated final checks cost compute but prevent the train from landing a prefix that already violates a required final invariant. Long lock ownership makes compliant crash recovery and prefix identity simpler.

<a id="obj-dec-user-supplied-committed-baseline-v1"></a>

### Require a user-supplied committed baseline

V1 executes only from an existing user-created immutable commit/tree. The conductor does not snapshot dirty, untracked, or ignored source; users must first commit or create an appropriate private branch/ref themselves.

**Rationale.** Minimizes baseline-capture and sensitivity logic at the cost of usability for in-progress work.

<a id="obj-dec-core-git-only-capability-profile-v1"></a>

### Support only core self-contained Git repositories in v1

V1 rejects repositories requiring custom merge drivers, filters, submodules, LFS, generated-source hydration, shallow/partial history, alternates, or replace refs. Only tested self-contained local Git/object/filesystem profiles may integrate.

**Rationale.** Makes exact composition and verification easiest to specify and test while deliberately excluding common advanced repository mechanics.

<a id="obj-dec-private-ref-retention-quarantine-v1"></a>

### Protect durable Git objects with private refs and exact cleanup

Every durable baseline, candidate, accepted prefix/composed result, proposal, and quarantine object is protected by an immutable expected-absent private ref. Baseline, candidate, accepted, and unresolved proposal anchors survive through project retention or explicit content-preserving archival; temporary attempt refs may expire only after durable receipts and no active recovery, audit, or remediation dependency. Cleanup is intent/effect/reconciliation driven, deletes only by expected OID, removes only exact clean dead-owned disposable worktrees, never performs broad prune/force cleanup, and quarantines dirty, conflicted, live, or identity-ambiguous resources. Cleanup failure is correctness-neutral.

**Rationale.** Worktree registrations and reflogs do not reliably preserve objects. Exact refs make recovery inspectable, while conservative cleanup prevents a locator collision from becoming deletion authority.

<a id="obj-dec-preverified-visible-partial-multirepo-landing-v1"></a>

### Preverify all repositories before visible non-atomic landing

V1 permits plan-ordered multi-repository landing only after every repository has an exact protected proposal and all required cross-repository tuple checks pass on those proposal commits/trees. The plan declares landing order, compatibility of each possible partial prefix, and forward remediation/compensation; detached authorization explicitly acknowledges non-atomic partial effects. Each repository lands and receipts independently without simultaneously held repository locks. If a later target drifts or fails, prior landings remain immutable and visible, the coordinator becomes partial-landing-blocked, and only authorized forward completion or compensation may proceed. V1 never claims atomicity or automatically rolls back.

**Rationale.** Git cannot atomically update multiple repositories. Preverification reduces avoidable partial states while explicit compatibility and remediation make unavoidable partial landing honest and recoverable.

<a id="obj-dec-causal-consumer-waits-for-integrated-producer-v1"></a>

### Release causal consumer work only after producer integration completes

For every true producer/consumer precedence edge `A → B`, no phase of B—including F0—may become correctness-ready until A has an accepted current `IntegrationReceiptV1`, its exact conductor-branch fast-forward is reconciled, and A is complete. A worker result, candidate seal, F1 completion, behavioral pass, or F8 integration-ready receipt cannot release B. Earlier phases of A may run only A's own validation lifecycle. Independent work items without a causal edge may run concurrently, and integration-train ordering alone does not create authoring precedence. Future-prefix speculation never starts a causally dependent consumer early.

**Rationale.** A consumer must build on the exact integrated producer state rather than an unvalidated or not-yet-landed candidate; this keeps true dependencies honest while preserving parallelism among independent work.

<a id="obj-dec-critical-rank-bypass-list-scheduler-v1"></a>

### Use deterministic vector first-fit with critical rank and bounded bypass fairness

V1 uses deterministic greedy full-stream vector first-fit rather than optimal packing or per-run DRF. Mandatory safety reconciliation runs before scheduling. Admission priority is: an already-triggered fairness reservation; head integration and F2/F4/F5/F6/F7/F8 drain work; typed repairs; initial F0/F1 authoring; exact-prefix speculation. Within a class green precedes amber, then greater unit-weight remaining milestone height, later lifecycle progress, greater genuine admission debt, earlier train position, and stable repository/work-item/phase/generation IDs. Debt increments only when an actual competing admission consumed a needed compatible resource or mutex. At eight bypasses, new competitors for the missing classes are withheld until the candidate fits; no preemption or time aging occurs.

**Rationale.** The policy is deterministic and explainable, drains evidence/integration bottlenecks, favors structural unlocks, and avoids both strict-FIFO idling and starvation without solver complexity or guessed durations.

<a id="obj-dec-sticky-active-node-concurrency-lanes-v1"></a>

### Count concurrency as sticky active-node lanes through integration

The scheduler's primary concurrency limit is `maxActiveNodes`, counting DAG work items rather than phase operations or physical workers. When a correctness-ready work item is admitted, it holds one active-node lane from F0 through every F1–F8 forward phase, typed repair/back-edge, worker-context replacement, exact-prefix integration, conductor-worktree landing, and accepted integration receipt. A new work item cannot enter that lane merely because the incumbent moves between phases, has no physical worker running at an instant, waits for a phase resource, or is blocked on a human decision, external capability, provider hold, or environment. The fresh F2/F5 workers and other phase procedures are internal execution of the same active node. A lane releases only when the node is fully integrated/complete or receives an explicit terminal cancellation or successor-plan/replan disposition; there is no automatic or implicit parking in v1. True causal consumers remain blocked until their producer is integrated; only independent correctness-ready work competes for a newly free lane.

**Rationale.** Concurrency should mean a stable bounded set of nodes carried to completion, not an expanding set of partially processed nodes created by phase-level slot reuse or automatic parking.

<a id="obj-dec-five-layer-monotone-scheduler-v1"></a>

### Use five explicit scheduler layers and monotone runtime narrowing

V1 separates static validity, correctness readiness, speculative safety, capacity/mutex admission, and dispatch safety. Static validation expands gate evidence-producer arcs with precedence and rejects closed causal SCCs, impossible demands, duplicate architecture authority, and category smuggling before authorization. Readiness is a pure exact-generation predicate and excludes capacity/fairness. Admission uses phase-scoped mutexes and vector leases. Dispatch requires an atomically reserved natural slot plus persisted idempotent effect intent. Runtime observations may only narrow or restore prior operational narrowing within plan/authorization maxima; any missing semantic dependency, effect, gate, repository, oracle, outcome, or migration requires a successor plan.

**Rationale.** This prevents heuristic capacity waits from masquerading as semantic blockers and prevents an optimistic ready loop from launching work before recovery-safe reservation.

<a id="obj-dec-no-future-prefix-speculation-v1"></a>

### Disable future-prefix speculation in v1

V1 composes and verifies only the current accepted train head. Later entries wait until every predecessor is landed and receipted; independent authoring remains parallel.

**Rationale.** Eliminates speculative suffix invalidation at the cost of serialized integration preparation.

<a id="obj-dec-fixed-fanout-manual-provider-recovery-v1"></a>

### Use fixed implementation fanout and manual provider recovery

V1 keeps implementation fanout at configured capacity. Provider failure observations create a shared dispatch hold that requires explicit user/operator release or approved rerouting.

**Rationale.** Maximizes policy predictability while requiring more intervention and providing less automatic outage recovery.

<a id="obj-dec-explicit-max-active-nodes-required-v1"></a>

### Require explicit maxActiveNodes for every run

V1 has no default active-node concurrency. Every run authorization must supply a positive `maxActiveNodes` within validated operational and semantic maxima before any node can be admitted.

**Rationale.** Makes every active-node concurrency choice explicit at the cost of mandatory configuration.

<a id="obj-dec-sticky-lane-affected-scope-conflict-v1"></a>

### Contain conflicts within sticky active-node lanes

When exact evidence discovers a new runtime-narrowing incompatibility between active nodes, the earlier admitted node wins and the later node's conflicting generation/attempt is fenced before cancellation signaling. The later work item retains its sticky active-node lane while waiting for exact conflict release, lifecycle repair, terminal cancellation, or successor-plan/replan disposition; no inactive node automatically replaces it. A finding that implies missing causality or semantic scope requires a successor plan. Capacity reduction records overcommit and denies new leases without preemption unless continued overlap is explicitly unsafe, then newest conflicting attempts are fenced first. Structural deadlock is an indexed wait-for SCC with no active, external, human, capacity-observation, retry, or back-edge release path; only the mechanically affected active-node closure is held, while ambiguous scope blocks the run. No timeout releases leases or constraints.

**Rationale.** Sticky lane ownership preserves the selected active-node set while deterministic attempt fencing contains newly observed hazards without guessing effort or globally pausing unrelated active nodes.

<a id="obj-dec-policy-bound-reservations-disposable-scheduler-explanations-v1"></a>

### Bind one scheduler policy hash but keep routine batch explanations disposable

Executable plans and runs bind an exact `schedulerPolicyVersion` and canonical policy hash through the lifecycle/projection catalog, normalized scheduler index, reservations, and operation intents. The atomic snapshot persists exact current reservations, leases, decision sequence, adaptive/circuit/fairness counters, and launch/effect identities needed for guards and recovery. Deterministic projections expose the correctness frontier, admission reasons, priority components, wait-for graph, and current policy inputs. Routine non-authoritative batch explanations are not immutable artifacts or event sourcing. Only human/authority overrides, cross-session policy/capacity facts, deadlock/starvation/replan attention, or scheduler facts promoted as evidence cross the immutable artifact boundary.

**Rationale.** Correctness comes from atomic reservations and intents; disposable deterministic explanations satisfy observability without violating the accepted bounded-artifact policy or accumulating a scheduler event log.

<a id="obj-dec-activity-centered-topology-widget-v2"></a>

### Center the persistent DAG widget on live activity and immediate dependents

The bounded passive DAG widget renders an activity-centered topology spotlight rather than the whole graph: mandatory attention nodes first, then sticky active lanes, then their immediate causal dependents and required one-hop context. It uses horizontal space for parallel activity lanes and summarizes every omitted region explicitly. Each selected node may show compact stage and activity detail. Spinner motion appears only while an exact joined worker observation proves that node's bound process is live and fresh; otherwise the active mark is static or frozen. The persistent renderer is purpose-built and responsive; general graph renderers are reserved for explicit full-DAG inspection.

**Rationale.** This keeps topology legible, makes current work continuously visible, and prevents animation from claiming activity that canonical worker evidence does not prove.

<a id="obj-dec-canonical-stage-progress-widget-v1"></a>

### Measure node progress by canonical F0–F8 stage completion

A node progress bar has nine canonical segments for F0 through F8. Passed stages in the current valid candidate generation render complete, the current stage renders active, and later stages render pending. When repair, retry, or a successor candidate invalidates later stage results, the bar may regress. The UI does not infer percentages from tool events, elapsed time, tokens, or worker output.

**Rationale.** Stage completion is already authoritative, understandable, and intentionally non-monotonic under iteration.

<a id="obj-dec-responsive-nine-stage-progress-encoding-v1"></a>

### Render nine-stage progress with deterministic width fallbacks

At wide and medium widths, each selected node lane renders nine ordered F0–F8 cells with passed, current, and pending states distinguishable without color-only meaning. Titles and dependent labels truncate before semantic cells are removed. At narrow widths, the full bar becomes the current stage plus `passed/9`. At subminimum widths, the renderer keeps only alias, primary mark, and current stage and makes no progress-bar claim. Rework regression redraws fewer passed cells without animation that implies failure.

**Rationale.** This keeps canonical stage progress visually useful while preserving exact-width and topology obligations at every breakpoint.

<a id="obj-dec-responsive-horizontal-graph-branches-v1"></a>

### Render selected activity anchors as responsive horizontal graph branches

Each selected activity or attention anchor renders its activity cell, stable primary glyph, alias, responsive F0–F8 progress encoding, and truncated title on one semantic row. A connector rail below that row routes to the anchor's visible immediate dependents on one shared dependent row. Each dependent edge uses a right-pointing arrow followed directly by the stable dependent alias, without a redundant `>` prefix. The same graph topology grammar applies at 50-, 80-, and 120-column layouts; only the already accepted progress encoding changes at narrow width. When all immediate dependents cannot fit, the renderer preserves deterministic priority and states the omitted count rather than silently dropping edges or switching to a different topology grammar.

**Rationale.** The iterated prototype made causal topology substantially easier to read than a flat list while retaining horizontal efficiency and predictable responsive behavior.

<a id="obj-dec-dag-widget-nonblocking-incident-reproduction-v1"></a>

### Do not gate widget replacement on reproducing the exact historical incident

Proceed with Projection V2, the serialized disposable controller, exact-width Graph branches renderer, and regression harness while the exact historical editor-scrambling transition remains open diagnostic research. Capture PI_TUI_WRITE_LOG, terminal dimensions, projection revisions, and controller generation if the incident recurs; add any newly proven transition to the regression suite. The implementation must already cover exact-width resize, overlapping refresh, variable-height breakpoint changes, stale liveness, and publication after disposal.

**Rationale.** A definite width-contract violation is already actionable and the accepted safety architecture prevents every currently plausible corruption class.

<a id="obj-dec-static-deduplicated-dag-widget-v1"></a>

### Use static worker activity and deduplicate DAG widget renders

DagExecutionProjectionV2 preserves the exact plan/run/scheduler/worker join and exposes each bound worker terminal status without process-disposition or retry-safety fields. The session-scoped controller serializes and coalesces status reads, retains last-good fail-closed behavior, and requests a TUI render only when the projection hash or visible diagnostic changes. Active workers use a static activity mark. The widget has no liveness window, animation frame, or animation timer, and unchanged periodic refreshes produce no terminal output.

**Rationale.** A 120 ms animation timer repeatedly redrew the full Pi TUI and amplified into thousands of PTY writes per second. Motion adds no lifecycle authority; a static mark and change-driven rendering preserve useful state while making idle output quiescent.

<a id="obj-dec-agent-driven-canonical-dag-dispatch"></a>

### Make canonical DAG worker dispatch agent-driven through a dedicated tool

The top-level conversational agent is the DAG orchestrator. For each canonically ready and admissible work item or stage, the agent invokes `dag_run_dispatch` over one exact ready packet. The tool atomically revalidates current run authority, readiness, admission, gates, concurrency, and generations; persists or consumes the exact reservation and launch intent; invokes the owned worker manager; and records the canonical binding or durable launch ambiguity. Generic `subagent` remains DAG-agnostic and cannot consume canonical DAG work. Each dispatch may include a bounded agent-authored tactical directive, while an immutable canonical envelope preserves objective, stage role, repository/worktree and edit boundaries, required checks, and completion identity. The directive and final prompt hash are recorded. Timers, session wakes, `agent_end`, and background conductor pumps may reconcile durable observations and surface attention but must not autonomously launch new DAG workers. The agent continues only independent orchestration work and settles at worker dependency barriers so completion follow-ups resume orchestration.

**Rationale.** Visible agent tool calls should own launch timing and tactical worker guidance, while a dedicated canonical dispatch mutation preserves exact reducer and worker-runtime authority without coupling generic subagents to DAG internals.

<a id="obj-dec-tool-driven-dag-public-action-surface"></a>

### Use explicit semantic tools for top-level DAG orchestration

Expose `dag_next_action` and named semantic mutation tools for starting work, recording completion, integration, retry, cancellation, and finalization. Tools derive internal revisions, epochs, hashes, locks, and idempotency identities rather than requiring routine agent transport of those fields.

**Rationale.** Named operations preserve a visible, understandable orchestration history while code retains safety invariants.

<a id="obj-dec-dag-next-action-returns-independent-frontier"></a>

### Return the full independent semantic frontier

`dag_next_action` returns all currently admissible independent semantic actions with explanations and any mutex or concurrency constraints. The top-level agent chooses which to perform and in what order.

**Rationale.** The agent remains the orchestrator without forcing artificial serialization.

<a id="obj-dec-canonical-completion-recorded-by-agent-tool"></a>

### Record canonical worker completion through an explicit agent tool call

Owned-worker completion delivery starts a follow-up turn containing the exact run and completion identities. The top-level agent calls `dag_record_completion`, which validates the durable worker binding and records the result before returning next-action guidance. Completion notification itself does not mutate canonical DAG state.

**Rationale.** The callback keeps work moving while canonical mutation remains visible and agent-driven.

<a id="obj-dec-tool-driven-dag-preserves-canonical-run-history"></a>

### Preserve canonical run history while removing conductor ceremony

The tool-driven cutover preserves current canonical run snapshots, worker bindings, lifecycle evidence, and historical readers. It removes timer pumps and process-local conductor-generation requirements from normal operation. Tools derive operation-scoped consistency guards and perform safe same-session resume or proven-dead-owner recovery without requiring the agent to transport internal lock and hash fields.

**Rationale.** Existing evidence remains usable while routine operation becomes simple.

## Accepted representative failure scenarios

_No accepted current content._
