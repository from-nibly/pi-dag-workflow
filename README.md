# pi-dag-workflow

Pi extension for mixed-initiative project-model brainstorming, architecture-first DAG planning, exact session-bound execution, and extension-owned asynchronous Pi workers. The production workflow covers research, intent clarification, semantic review, deterministic generated specifications, inspectable plans, and durable process-isolated subagents.

## Authority

One repository-wide tracked snapshot owns project meaning:

```text
project-model/model.json
```

It may contain both governing and non-authoritative objects:

- workstreams;
- intent and concepts;
- evidence and assumptions;
- questions and enduring tensions;
- scenarios and proposals;
- decisions and commitments;
- discoveries from research, prototypes, repository inspection, or later execution.

Human authority is explicit. Accepted intent, concepts, scenarios, decisions, and commitments carry content-bound receipts. Agent findings and derived implications remain non-authoritative until accepted.

Tracked Markdown under `spec/` is a deterministic readable projection of accepted model-owned prose, not another source of truth. Rendered review turns, frontiers, deltas, ledgers, and consequence views remain ephemeral by default.

## Install

```nu
pi install git:git@github.com:from-nibly/pi-dag-workflow@v0.1.4
```

## Model brainstorming commands

```text
/dag brainstorm                         # interactive New/Resume selector
/dag brainstorm new <name>              # create a resumable focus session
/dag brainstorm resume <focus-id>       # resume an exact focus
/dag brainstorm list                    # list focus sessions
/dag brainstorm stop                    # suspend model mode
```

A focus session is ignored presentation state under `.ai/model-sessions/`. It contains selected workstreams, one active review turn, and one replaceable previous-review snapshot. Optional Lavish HTML and adjacent lifecycle metadata live under `<focus-id>/lavish/`. None of these files owns unique project meaning.

Reloading, resuming, forking, or cloning a linked Pi conversation restores the exact focus. A new unlinked Pi session starts inactive. Model and focus snapshots use process-shared locking, expected integer revisions, and durable atomic replacement; conflicting concurrent mutations fail rather than losing an update.

## Planning, inspection, and execution

One product workflow carries accepted project meaning into the shipped canonical runtime:

```text
/dag plan [--new | --plan <plan-id>] [goal]       # architecture, then internal decomposition
/dag plan approve --plan <plan-id>@<revision>      # approve the exact current head
/dag plan authorize --plan <plan-id>@<revision>    # authorize without starting
/dag show [--plan <selector>] [--view plan|graph|lineage]
/dag show [--plan <selector>] --node <id|Nxx>
/dag show --run [<exact-current-session-run-id>]
/dag run [--plan <plan-id>@<revision>]              # explicit start, or advance bound run
/dag run --resume                                   # require an existing current-session binding
```

`/dag plan` keeps the active project-model focus. It asks the agent to establish outcomes, architecture, boundaries, risks, integration checks, and shell-free prefix/final validation argv before internally producing the smallest causal work-item graph. `dag_plan_save` derives Git and source identities from a clean tracked `HEAD`; callers never supply Git object IDs, hashes, artifact paths, or runtime receipts. Every successful revision returns deterministic Markdown and static-graph previews.

Approval and authorization are independent retained revisions. They may be decided together after one exact review, but neither starts work. V1 authorization is deliberately whole-plan: its scope must equal the complete exact work-item ID set, while maximum concurrency remains independently bounded. Only an explicit `/dag run` starts execution. `/dag chunk` is intentionally absent because decomposition is an internal phase of `/dag plan`.

Thin plans live under ignored `.ai/dag-plans-v1/`. Each head has immutable retained revisions, an exact active-focus binding, typed project-model/spec sources, one static semantic hash, and ordinary decision fields. Exact selectors are a plan ID for its head or `planId@revision`; prefix, modification-time, and inferred-latest selection are never used.

`/dag show` is read-only. A current-session live run wins by default; otherwise one session-bound or unambiguous active-focus static plan is required. Static Markdown, graph, node, and lineage views are deterministic projections. Live views resolve only the exact current-session run binding.

`/dag run` revalidates the clean Git baseline, target branch, authoritative model objects, generated specification bytes, approval, and authorization before creating run authority. It internally compiles the thin plan into the existing canonical F0–F8 contracts, generates UUID occurrence identities, durably records a recoverable start intent, binds the run to the current session, and only then permits scheduling. A repeated command advances the bound run without restarting or implicitly unpausing it. A process crash during start is recovered from the exact unfinished intent.

## Model tools

The seven tools register once and are activated only after Pi's extension runtime has initialized and a model brainstorming focus is active:

- `dag_model_context` — read narrow orientation, entity, frontier, delta, review, or governing projections.
- `dag_model_update` — record non-authoritative findings, relationships, routing metadata, or Current understanding. It cannot grant authority or rewrite accepted semantics.
- `dag_model_record_direction` — record unambiguous direct user authority with content-bound receipts.
- `dag_model_review` — create an exact hash-bound review turn with **For awareness** and **Decisions needed**; its exact visible tool result records successful presentation.
- `dag_model_present_review` — optionally render and `present`, `resume`, or `end` the active review through Lavish while returning bounded feedback for agent interpretation.
- `dag_model_resolve_review` — apply independent fresh outcomes while preserving stale, omitted, or ambiguous points.
- `dag_model_specs` — preview, check, or explicitly recover deterministic generated specs.

Routine successful semantic mutations automatically synchronize affected current specs without making a Git commit. Accepted objects explicitly superseded by another receipt-valid accepted object stop rendering while retaining stable historical model identity. A direct direction that exactly matches an active review disposition reconciles that disposable review point without requiring a second authority receipt.

Lavish presentation uses the pinned optional dependency `lavish-axi@0.1.43`; it never falls back to ambient `npx`. The generated shell supports multiple independent decision points, complete visible option prose, an explicit **Other** radio, and a separate response box. The renderer does not resolve semantic state automatically: the agent validates returned review/point/option hashes and invokes `dag_model_resolve_review` from a bound human turn.

## Mixed-initiative loop

```text
Orient
  → Explore and record coherent non-authoritative findings
  → Consolidate material model changes
  → Stress-test with representative/boundary/failure cases
  → Present a materiality-based review turn
  → Apply exact explicit outcomes
  → Regenerate affected specs
  → Continue, change focus, or stop
```

Direct, unambiguous user direction commits once. Silence, generic praise, ambiguity, and agent-derived consequences never commit. Reconsidering accepted content does not revoke it automatically; generated specs retain still-governing content with an **Under review** marker until it is explicitly suspended, retired, or superseded.

New behavioral prototypes require explicit user request. Hand-authored prototype evidence lives under `spec/prototypes/<slug>/` and is protected from spec generation.

## Generated specifications

Project-specific non-semantic routing metadata in the model declares output paths, sections, short summaries, and object order. Every accepted object's full body has one canonical generated placement; other specs link to it.

V1 deliberately uses minimal one-way safety:

- generated files are marked;
- rendering occurs in a temporary location;
- `dag_model_specs check` regenerates and compares output;
- unknown target collisions fail;
- prototype directories are never overwritten;
- stale generated paths are reported conservatively.

There is no generated-file ownership manifest, editable generated region, reverse synchronization, or automatic deletion framework in V1.

## Asynchronous workers

The extension owns a generic worker runtime; it does not depend on `pi-subagents`. Every launch returns immediately while a detached supervisor runs the exact installed Pi CLI in RPC mode. Launch output explicitly tells the parent agent not to poll status or sleep: it should continue independent work or end its turn because completion arrives automatically through the serial queue. Workers survive top-level Pi reload or exit, report through a terminating `subagent_report` tool, and deliver bounded completions serially when the owning session reconnects.

Top-level agent tools:

- `subagent` — launch an asynchronous worker;
- `subagent_status` — list or summarize workers;
- `subagent_inspect` — read a bounded immutable terminal result;
- `subagent_tail` — read selected bounded diagnostics;
- `subagent_cancel` — cancel only after PID/start-identity and attempt verification;
- `subagent_retry` — explicitly start a new attempt for a terminal worker.

Equivalent user commands are `/workers list|inspect|tail|cancel|retry`.

Runtime state lives under `.ai/worker-sessions/`. One atomic `worker-session.json` belongs to each top-level Pi session; detached supervisors write bounded mailboxes, a diagnostic log capped at 50 MiB, and immutable terminal results. Child processes inherit ordinary active tools but omit `subagent*`, `dag_*`, and `dag_model_*` orchestration surfaces except for `subagent_report`. Full transcripts and cumulative `message_update` events are never persisted.

Direct forks and clones transfer the complete worker session and completion queue when source ownership can be proven. Ambiguous, corrupt, stale-live, PID-reused, or conflicting ownership fails closed rather than signaling or relaunching an unproven process.

Obsolete `pi-subagents` artifacts are not adopted or deleted automatically. Historical sibling directories named `*-dag-subagents` and temporary `/tmp/pi-subagents-*` trees may be removed manually only after confirming that no legacy worker process still owns them.

## Canonical DAG execution

The product commands above use the existing guarded canonical runtime as a hidden execution substrate. The conductor binds one exact run to the current Pi session and branch; it never selects a “latest” run. Low-level tools remain available for exact diagnostics and compatibility.

Read-only tools:

- `dag_run_status`
- `dag_run_diagram`
- `dag_run_inspect`
- `dag_run_tail`
- `dag_run_explain`

Guarded mutation tools:

- `dag_run_start`
- `dag_run_control` (`pause`, `resume`, or `cancel`)
- `dag_run_retry`
- `dag_run_reattach`

Every post-start mutation carries the exact run nonce, owner epoch, revision, snapshot hash, command/idempotency identity, and explicit timestamp; start itself binds immutable plan/genesis/context artifacts and an explicit run identity. Interactive TUI sessions show a passive bounded DAG widget; headless modes expose the same semantic projection without rendering a widget.

The deterministic scheduler separates correctness readiness from lane/resource/mutex admission. `maxActiveNodes` lanes remain sticky through phase waits, repairs, blocking, and integration. Generic worker status affects DAG projection only through an exact run-state worker binding.

Real-Git integration uses core-only repository preflight, immutable private refs, explicit-base `merge-tree`, deterministic one-parent `commit-tree`, plan-hashed executable prefix/final verification profiles, and a guarded ordinary fast-forward in the clean session-bound worktree. Target old/new/third reconciliation and immutable receipts make every failpoint recoverable without reset, stash, force update, or conductor conflict edits.

The reducer-driven F0–F8 lifecycle publishes immutable attempt, launch, worker-result, candidate, check, environment, finding, integration, and cleanup facts before they can advance authority. Session attachment and exact terminal-worker ingestion wake one coalesced service-owned conductor pump, so procedure reconciliation seals stages and dispatches newly ready work without another user command. A fresh conductor service in the same exact session/process CAS-transfers to a new owner epoch before mutation instead of treating a long-lived Pi wrapper PID as proof that the prior pump is still operational; old commands are fenced by epoch/token/revision checks. Different-process takeover still requires exact proven-dead reattachment. Owner takeover and prior-session worker reconciliation remain process-identity fenced. Detached evaluation observes committed snapshot identities asynchronously, retains privacy-safe bounded accumulators and envelopes, and cannot affect execution. The dogfood portfolio runs six counterbalanced serial/parallel pairs (twelve canonical executions) plus separate recovery drills.

Separate `/dag review`, `/dag retro`, and `/dag archive` product workflows are not implemented. `/dag chunk` is intentionally folded into `/dag plan`. GrillMe, promotion, legacy prompt workflows, the dormant `dag_subagent` adapter, the `pi-subagents` dependency, and model-unaware mutating DAG tools are removed.

Clearly labeled read-only diagnostics remain for pre-cutover artifacts:

```text
/dag validate
/dag status
/dag workers
/dag inspect
/dag tail
dag_validate
dag_diagram
dag_status
```

They cannot create or advance execution.

## Candidate migration

The repository includes a one-time importer for the previous structured-brainstorm snapshot:

```nu
node scripts/migrate-brainstorm-to-project-model.mjs
```

It writes:

```text
project-model/model.json                              # candidate mode
project-model/migrations/brainstorm-v2-candidate.md  # mapping/omission audit
.ai/model-migration/candidate/spec/**                 # ignored generated preview
```

The candidate does not become authoritative until its semantic mappings, omissions, generated specs, and exact manifest hash receive explicit human approval. Cutover is an isolated `dag_model_record_direction` operation with `{ "cutover": { "candidateManifestHash": "sha256:…" } }`; it creates `migration_cutover` receipts, changes model mode, and replaces the declared hand-maintained projection targets as one recoverable transaction. The importer refuses to replace an authoritative model, even with `--force`.

## Source-checkout validation

These repository release and test commands use tracked model/spec fixtures and Git history; they are contributor checks, not installed-package runtime commands.

```nu
npm run smoke
npm run test:model
npm run test:dag-planning
npm run test:dag-planning-runtime
npm run test:dag-planning-command
npm run test:dag-prepared-start
npm run test:dag-runtime
npm run test:dag-evaluation
npm run test:dag-dogfood
npm run test:dag-dogfood-portfolio
npm run test:git-integration
npm run test:workers
npm run release:ready              # full deterministic matrix plus specs/package/clean-tree gates
# Only while project-model/model.json is still a non-authoritative candidate:
node scripts/migrate-brainstorm-to-project-model.mjs --force
```

The production tests cover model validation, acceptance boundaries, concurrent model/focus CAS, sparse/stale review resolution, deterministic plan projections and lineage, exact command selection, real-Git source/baseline validation, crash-recoverable prepared start, canonical runtime compilation, whole-run replanning, Pi activation and fork restoration, legacy read-only compatibility, and migration candidate generation.
