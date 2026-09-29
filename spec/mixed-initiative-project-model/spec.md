<!-- generated-by: pi-dag-workflow/project-model; view: SPEC-mixed-initiative; contract: 1; input: sha256:b36319641c487e0667e2531743d741702f9603e9ff6c527224524384f57f4364 -->

# Mixed-initiative project model

Shared-model authority, turn-loop, projection, migration, and Pi integration contracts.

## Candidate intent

<a id="obj-int-scalable-oversight"></a>

### Scale human oversight through a shared project model

Concentrate human attention on desired outcomes, priorities, material model changes, misunderstandings, consequences, and representative scenarios while the agent owns research and model-maintenance labor.

<a id="obj-int-human-authority"></a>

### Preserve explicit human authority over project direction

Humans own outcomes, priorities, values, non-goals, and acceptance or replacement of governing direction; silence and agent-derived implications never commit.

<a id="obj-int-brainstorm-first-dogfood"></a>

### Dogfood model brainstorming before redesigning execution

Switch and evaluate the complete brainstorming slice first, with model-unaware planning and execution disabled until field experience informs their model-aware replacements.

<a id="obj-int-dag-migrate-existing-repository"></a>

### Add /dag migrate for existing repositories

Provide a guided `/dag migrate` workflow that converts an existing repository into a project-model candidate, exposes what was inferred, and requires explicit user approval before authority changes.

## Candidate concepts

<a id="obj-con-focus-session"></a>

### Focus session

A named resumable ignored attention workspace containing selected workstreams, presentation preferences, one active review turn, and one previous-review snapshot over the single repository model. It contains no unique project meaning.

<a id="obj-con-review-turn"></a>

### Review turn

A user-facing oversight boundary that separates For awareness from exact Decisions needed. It is not a Git commit or authority transition.

## Candidate accepted direction

<a id="obj-dec-canonical-mixed-initiative-project-model"></a>

### Adopt one durable project model as authority

Adopt one durable tracked model per repository as authority for intended outcomes and explicitly accepted decisions and commitments, while storing uncertainty and agent-authored objects with clearly non-authoritative state. Humans retain authority over outcomes, priorities, values, and acceptance. Code and tests remain evidence of implemented reality; differences are explicit divergence to reconcile. Detailed model-aware planning and execution reconciliation remains deferred.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-durable-statused-snapshot"></a>

### Persist model meaning and independent review outcomes

Persist one current tracked snapshot containing materially useful typed objects, uncertainty, discoveries, and independent scoped review records with durable sparse outcomes. Objects carry stable identity, type-specific state, scope, source references and relationships. Git supplies history; the model is not an event log. Review records and model outcomes publish atomically in the model snapshot. Presentation artifacts and scratch research are derived or ephemeral; no durable focus owns unique meaning.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-model-delta-oversight-loop"></a>

### Center oversight on model deltas and consequences

Use a repeating Explore, Consolidate, Stress-test, Commit, Project loop. Consolidation presents Added, Changed, Still unresolved, Possible misunderstanding, and consequences; stress-testing adds boundary/failure cases, contradictions, and alternatives. Commit explicitly disposes exact model objects according to their type-specific lifecycle; silence commits nothing. Oversight projects Current understanding, Model delta, Decision ledger, Unresolved frontier, Consequence, Divergence, and Representative slices.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-single-repository-model"></a>

### One repository model with workstreams/focuses

One durable project model is authoritative per repository; initiatives are scoped model objects, not separate authority files.

**Rationale.** This preserves one source of truth while allowing independent initiative views and context-efficient execution slices.

<a id="obj-dec-tracked-project-model-json"></a>

### project-model/model.json

v1 persists one tracked project-model/model.json current snapshot; Git provides history and tools own atomic mutations and narrow reads.

**Rationale.** It makes authority visually distinct from disposable .ai runtime data and minimizes the first durable storage implementation.

<a id="obj-dec-semantic-current-state-import"></a>

### Import current meaning plus a migration report

Migration preserves current authority and unresolved direction, not every legacy workflow object; the audit report explains mappings, omissions, and representative-view comparison.

**Rationale.** This avoids both context loss and historical-object bloat while making the one-time transformation reviewable.

<a id="obj-dec-lean-typed-v1-model"></a>

### Lean typed v1 core

v1 uses a small explicit type union and shared metadata; subtype-specific fields are required only where they enforce material authority or validation behavior.

**Rationale.** This supports the adopted loop and migration while leaving room to split types after real usage.

<a id="obj-dec-direct-user-commit-derived-review"></a>

### Use contextual user direction without approval ceremony

Interpret actual user intent from chat, Lavish or another supported user-originated channel. Clear direction may be recorded once through the direction tools; questions, silence, quoted text and agent-derived implications are not blanket consent. Review relevant unresolved decisions and pending review outcomes before dispatch; already-settled conversational direction does not require repeated confirmation. Do not require semantic consent hashes, acceptance receipts, current-turn markers, a trusted presentation adapter, or formal plan approval. Saved plan content, scope and semantic freshness are assessed and reported for agent judgment. Start the explicit plan revision when the conversation resolves material concerns and establishes execution intent. New material scope or restricted external effects require contextual authorization; internal CAS, real evidence and native Git integrity remain enforced.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-model-prose-deterministic-projections"></a>

### Model-owned prose with deterministic structural projection

Tracked specs are deterministic structural projections of model-owned prose; regeneration is drift-checkable and hand edits do not mutate authority.

**Rationale.** This yields readable specs, stable diffs, and one semantic source without requiring an LLM for routine regeneration.

<a id="obj-dec-audited-candidate-atomic-cutover"></a>

### Audited candidate followed by atomic cutover

Pre-cutover candidate comparison is allowed, but after acceptance only v1 is authoritative; rollback uses Git and the migration report rather than continued dual writes.

**Rationale.** This protects nuance while respecting the accepted no-dual-authority architecture.

<a id="obj-dec-brainstorm-authority-cutover-disable-downstream"></a>

### Switch brainstorming authority; disable old downstream commands

Authority may switch after the complete brainstorming vertical slice—model, tools, turn loop, projections, migration audit, and production command—is validated. Legacy plan/chunk/run are disabled at that switch; execution integration remains deferred rather than using compatibility documents.

**Rationale.** This enables real dogfooding without dual authority or allowing downstream workflows to follow obsolete contracts.

<a id="obj-dec-discoveries-throughout-project-loop"></a>

### Use discoveries throughout brainstorming and future execution

Discovery is an active v1 type during brainstorming, not a dormant execution-only placeholder. Research, repository inspection, prototypes, user observations, and later implementation may produce discoveries. A discovery records a new finding and its possible model impact until the agent integrates it into evidence, assumptions, questions, tensions, proposals, scenarios, decisions, or commitments, dismisses it, or defers it. Future execution reconciliation reuses this same type for worker findings but does not define its existence.

**Rationale.** Prototypes and research are primary evidence-generating activities during mixed-initiative exploration. Restricting discovery to future DAG execution would lose the finding-to-model-change step that brainstorming already needs.

<a id="obj-dec-separate-tension-type"></a>

### Keep tension as a separate durable type

Keep tension as a separate durable type for enduring opposing forces, with active, resolved, deferred, and retired states. Questions remain concrete uncertainties, contradictions, tradeoffs, or reconsideration prompts and may arise from a tension.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-type-state-separate-authority"></a>

### Use explicit object states rather than receipt proof

The current model derives governing meaning from object type, explicit accepted state and supersession, interpreted under the user conversation. New accepted objects do not require semantic-hash acceptance receipts. Historical receipts remain readable provenance, not proof of current agreement. Keep structural schema/reference validation and mutable revision CAS separate from semantic consent.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-distinct-intent-decision-commitment"></a>

### Distinct intent, decision, and commitment

Direct requirements may become accepted commitments without synthetic decisions. Derived commitments begin not_reviewed and govern only after explicit acceptance. Decisions link selected proposals and derived commitments without duplicating their prose.

**Rationale.** This is the smallest split that preserves why, chosen tradeoff, and enforceable contract for different views.

<a id="obj-dec-content-bound-acceptance-receipt"></a>

### Content-bound semantic acceptance receipt

Tools never store raw user wording but mechanically prevent accepted semantic prose/scope/rationale from changing without renewed authority. Explicit batch acceptance identifies exact object hashes.

**Rationale.** This operationalizes direct-user commitment once while protecting it from later agent drift.

<a id="obj-dec-reconsideration-question-preserves-effect"></a>

### Keep governing effect until explicit suspension or replacement

Discussion state is derived from linked open questions; governing effect changes only through an explicit authoritative transition.

**Rationale.** This avoids a second review-state axis and prevents casual exploration from destabilizing current contracts.

<a id="obj-dec-explicit-scope-typed-edges"></a>

### Explicit repository/workstream scope plus typed edges

Cross-workstream links are legal and visible; validators enforce endpoint existence, allowed endpoint types, and acyclic supersession.

**Rationale.** This supports bounded context without duplicating objects or maintaining reverse-link arrays.

<a id="obj-dec-authority-aware-deletion"></a>

### Hard-delete only non-authoritative mistakes

No accepted object is silently hard-deleted; replacing authority marks dependent objects for review rather than rewriting them automatically.

**Rationale.** This preserves stable semantic references without turning the current snapshot into an exhaustive history archive.

<a id="obj-dec-immediate-nonauthoritative-model-write"></a>

### Keep shared meaning in the model, not session modes

Record useful research and proposals promptly as non-governing model material. The durable model contains shared project meaning and independent pending reviews. Conversation context selects the current task without a saved-focus requirement; presentation files and scratch notes do not create a parallel source of governing meaning.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-multiple-ephemeral-sessions-one-model"></a>

### Multiple resumable sessions; one active at a time

Sessions are disposable attention workspaces over one authority and may coexist without separate project snapshots.

**Rationale.** This preserves useful context switching and resume UX without duplicating semantic authority.

<a id="obj-dec-tracked-nonauthoritative-synthesis"></a>

### One non-authoritative synthesis in project metadata

Current understanding is durable shared-agent context but never carries a human acceptance receipt or governing authority; only its latest value is retained.

**Rationale.** This preserves continuity and comprehension evidence without pretending causal prose is deterministic or human-approved.

<a id="obj-dec-point-level-hash-resolution"></a>

### Apply clear unchanged points; preserve stale or omitted points

Content-bound authority is checked per point/object rather than through global optimistic revision rejection.

**Rationale.** This is safe under content-bound receipts while preserving sparse-response efficiency.

<a id="obj-dec-awareness-plus-exact-decisions"></a>

### For awareness plus exact Decisions needed

A checkpoint is an attention boundary, not a whole-delta authority transition.

**Rationale.** This supports broad high-bandwidth reviews without accidentally authorizing agent-authored findings.

<a id="obj-dec-materiality-based-review-turns"></a>

### Use materiality-based review turns

The agent initiates a user-facing review turn when a coherent cluster needs product judgment, evidence materially changes framing, a user preference blocks progress, a contradiction affects governing direction, a material prototype result appears, continuing risks directional drift, the user changes focus or pauses, or the user requests review. A review turn is an attention/presentation boundary—not a Git commit or authority transition—and uses no fixed time, turn, object-count, or token quota.

**Rationale.** Materiality-based review turns preserve autonomous research and concentrated oversight without checkpoint/commit terminology that implies repository or authority mutation.

<a id="obj-dec-ephemeral-session-baseline"></a>

### Ephemeral per-session baseline token/projection

Uncommitted checkpoint deltas are computed by snapshot comparison without writing review metadata into the authoritative model.

**Rationale.** This supports accurate resume and deltas while preserving the model/presentation boundary.

<a id="obj-dec-single-canonical-placement-links"></a>

### One canonical body placement; generated links elsewhere

The projector validates unique canonical placement and visible cross-scope dependencies; duplicated normative body prose is an error.

**Rationale.** This preserves discoverability and cross-domain awareness without duplication drift or noisy repeated contracts.

<a id="obj-dec-tracked-specs-ephemeral-oversight"></a>

### Track generated specs and prototype index only

The default tracked projection surface is the current specification tree; oversight views remain on-demand unless a later accepted use case adds one explicitly.

**Rationale.** This preserves familiar repository discovery without turning attention/session surfaces into a noisy second documentation system.

<a id="obj-dec-explicit-accepted-spec-eligible-types"></a>

### Accepted intent, concept, scenario, decision, and commitment are spec-eligible

Normative spec inclusion never inherits authority through relationships; every emitted semantic body is explicitly accepted and currently effective.

**Rationale.** This supports stable vocabulary and representative behavioral examples without smuggling agent interpretation into specs.

<a id="obj-dec-model-metadata-projection-declarations"></a>

### Projection declarations inside project metadata

The one model contains project-specific projection structure without making it governing semantic prose; no second hand-maintained routing file exists.

**Rationale.** This supports functional domains and supporting documents that do not map one-to-one to workstreams while retaining one project-description source.

<a id="obj-dec-minimal-projection-safety-no-manifest"></a>

### Use minimal one-way projection safety without a required manifest

V1 marks generated files, renders to a temporary location, supports regenerate-and-compare validation, refuses to overwrite unknown targets, protects hand-authored prototype directories, and reports stale generated files conservatively. It does not require a separate generated-file ownership manifest, manual editable regions, reverse synchronization, or automatic deletion machinery; add stronger ownership metadata only after a concrete cleanup or scale need is observed.

**Rationale.** The one-way model-to-spec boundary needs basic consistency and file-safety checks, but a manifest and elaborate stale-cleanup protocol are premature defensive architecture before dogfooding demonstrates the problem.

<a id="obj-dec-effective-under-review-visible"></a>

### Include effective content with an Under review marker

Generated specs distinguish governing effect from active reconsideration without mixing historical/non-current content into current contracts.

**Rationale.** This matches the accepted rule that reconsideration does not automatically revoke authority while warning readers about potential change.

<a id="obj-dec-six-explicit-model-tools"></a>

### Use six action-shaped project-model tools

Keep model context, update, direct direction, review, review resolution and spec tools always available with explicit scope or exact review IDs; optional presentation is separate. Context exposes bounded model slices and pending reviews. Update records research without silently promoting governing meaning; record_direction records actual contextual user direction without a receipt/current-turn token. Independent review records contain explicit points/options and sparse outcomes; resolution uses exact IDs and revision CAS, not semantic-hash consent. Relevant unresolved decisions require agent review before dispatch. Specs preview, check and regenerate derived views through supported operations.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-transactional-automatic-spec-sync"></a>

### Synchronize affected specs after model mutations

Routine successful semantic model mutations synchronize any affected tracked current specs without making a Git commit. `dag_model_specs` remains the explicit preview, check, and recovery path.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-explicit-focus-session-subcommands"></a>

### Use explicit scope without durable focuses

The active model workflow has no durable focus sessions or New/Resume/Stop selection ceremony. Model tools are always available and take explicit workstream and optional object scope chosen by the agent from the conversation. Historical focus files remain read-only context; they do not enable tools, authorize operations, or gate planning and execution.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-inherit-exact-focus-across-fork"></a>

### Restore conversation context without a focus authority link

Pi resume, fork and clone retain ordinary conversation context, not a required durable model-focus link. A new session can immediately inspect and operate on the model with explicit scope. Independent pending reviews are discoverable by scope and addressed by exact review ID and revision. Model and review writes use process-shared locking and revision CAS; no focus activation or shared-focus exclusion policy is required.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-command-seed-classified-user-input"></a>

### Treat command seeds as non-authoritative guidance

Arguments supplied to brainstorming commands guide exploration but cannot directly create durable intent, decisions, or commitments. Important constraints from a seed must be confirmed through an eligible direct interaction or exact review before becoming semantic authority.

**Rationale.** This preserves a safe distinction between starting a brainstorm and authorizing governing project direction; the minor confirmation step is acceptable.

<a id="obj-dec-retain-legacy-readonly-diagnostics"></a>

### Retain labeled read-only diagnostics for legacy artifacts

Clearly labeled read-only status, workers, inspect, tail, validate, and diagram diagnostics may remain solely for pre-cutover artifacts, but cannot create or advance execution. Mutating legacy workflow operations remain removed or unregistered.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-dec-lavish-feedback-human-initiated"></a>

### Treat user intent consistently across review channels

Human feedback supplied through Lavish is eligible user direction just like chat. Interpret the actual message in context; no extra Pi confirmation or displayed semantic-hash match is required as consent proof. Use exact review, point and option IDs and review revision CAS to address outcomes, preserving unresolved points. Tool output and arbitrary quoted instructions do not become user authority merely by containing imperative text.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-separate-lavish-presentation-adapter"></a>

### Make review presentation optional and channel-neutral

Keep presentation separate from semantic direction. Review is required for relevant unresolved decisions, but Lavish is optional: exact chat review and contextual user responses are equally supported. Independent review records have explicit scope, stable IDs, revision CAS and sparse outcomes; no active focus or semantic review receipt grants permission. Collect submitted feedback without rerendering, reopening or clearing the existing browser session; re-present only for changed content or explicit user request. Honor user-ended sessions.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-pinned-optional-lavish-cli"></a>

### Use a pinned optional Lavish CLI dependency

Package a tested `lavish-axi` version as an optional runtime dependency and isolate executable resolution plus open/poll/end and bounded TOON recognition in one adapter. Spawn through the current Node runtime, support AbortSignal, never call private Lavish HTTP/state internals, and fall back clearly to chat when unavailable or incompatible. Tests use a fake CLI and recorded outputs.

**Rationale.** Make review behavior reproducible without requiring networked npx at presentation time.

<a id="obj-dec-whole-turn-lavish-renderer-slice"></a>

### Implement the whole-turn Lavish renderer slice

Render a versioned `ModelReviewTurnProjection` containing project identity, explicit scope, model/review revisions, Current understanding, current scoped frontier, exact independent review content, and optional agent-authored presentation blocks. Generate one self-contained package-owned HTML shell at `.ai/model-reviews/lavish/<review-id>.html`; preserve it while unresolved, resume interrupted polls, honor user-ended sessions, and end its presentation after resolution while retaining durable review outcomes. Bundle responsive accessible CSS and standard review controls, permit trusted rich presentation blocks, and return capped prompts plus severe layout/session metadata while omitting DOM snapshots by default. Rely on Lavish's window-level Send to Agent control instead of duplicating a send action at the top of the page. Make option cards self-contained with all prose needed for an informed choice and do not expose serialized authority-payload expanders. Support any number of awareness and decision points in one turn, with independently queued sparse outcomes. Always include an explicit Other radio option so the human can move away from suggested choices. Keep a separate response text box available regardless of which radio option is selected, so the human can add context or modification to a suggested option as well as describe Other direction. Proceed with direct production implementation rather than creating a planning DAG for this bounded slice.

**Rationale.** The renderer's product frontier and vertical prototype are complete; the remaining work is bounded production integration and end-to-end dogfood, which does not justify a separate planning DAG.

<a id="obj-dec-dag-migrate-staged-candidate-review-cutover"></a>

### Migrate with explicit scope and channel-neutral adoption

`/dag migrate` inventories repository sources and bootstraps or resumes a non-authoritative candidate model without a dedicated focus. Always-available model tools use explicit scope to build source-traceable objects, mappings, omissions, proposed projections and per-artifact dispositions. Review unresolved semantic decisions with the user in chat or optional Lavish. Contextual user intent may adopt the candidate through an explicit cutover operation without a manifest consent token or semantic approval receipt. Keep structural readiness and actual file-replacement/inventory safety; candidate edits do not silently overwrite authoritative unmanaged artifacts. Existing authoritative models are not silently remigrated.

**Rationale.** Updated from the user’s prompt-audit feedback: focus-free option C, contextual direction, required unresolved-decision review and no semantic consent ceremony.

<a id="obj-dec-dag-migrate-preserve-files-single-authority"></a>

### Preserve required artifacts without preserving dual authority

`/dag migrate` assigns every relevant existing artifact an explicit disposition. Cutover replaces only exact generated-projection collisions approved in the reviewed manifest; all other required files and directories may remain in place as linked manual references or evidence. If a retained artifact must still govern project semantics, cutover remains blocked until the authority conflict is resolved.

**Rationale.** Physical coexistence satisfies repository retention needs while preserving one explicit semantic authority after cutover.

## Candidate representative scenarios

<a id="obj-scn-fork-correction-continues-focus"></a>

### Forking to correct an earlier response preserves focus

When the user forks or clones a Pi conversation to edit an earlier response, the exact linked focus session is validated and remains active without requiring `/dag brainstorm` reentry.

<a id="obj-scn-stale-sparse-review"></a>

### Sparse review resolution preserves stale and omitted points

When the user answers only part of a review and unrelated model content changed, each fresh hash-matching point resolves independently while omitted, ambiguous, contradictory, or stale points remain unresolved.
