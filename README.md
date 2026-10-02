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

Human direction is interpreted explicitly in conversation context, whether in chat or Lavish. Governing object state records settled direction without semantic hashes or consent receipts. Questions, silence and agent-derived implications do not grant blanket authorization. Historical receipts remain readable, not required or treated as proof of new agreement.

Tracked Markdown under `spec/` is a deterministic readable projection of accepted model-owned prose, not another source of truth. Rendered review turns, frontiers, deltas, ledgers, and consequence views remain ephemeral by default.

## Install

```nu
pi install git:git@github.com:from-nibly/pi-dag-workflow@v0.6.0
```

## Project-model migration

```text
/dag migrate                            # create or resume a reviewed candidate migration
```

Run `/dag migrate` in a repository that does not yet have an authoritative project model. The command inventories relevant repository orientation, specifications, decision records, and plans; uses a supported legacy snapshot as a deterministic fast path when available; creates `project-model/model.json` in candidate mode; and supplies contextual migration guidance without creating a focus.

Migration never overwrites existing specifications while building the candidate. The agent records source mappings and omissions, generated projection previews, blockers, and an explicit disposition for every relevant artifact. It then reviews, in chat or optionally Lavish, inferred project meaning, unresolved questions, source coverage, generated-spec consequences, and the exact cutover/coexistence choice.

Cutover follows context-informed explicit direction (`cutover: true`), not a semantic hash ceremony. Internal file digests still protect exact source inventory and generated-file replacements; structural readiness and blockers remain enforced. It replaces only designated generated-projection collisions. Existing spec directories and required documents may remain side by side as linked references or evidence; if a retained document must remain governing semantic authority, cutover stays blocked. Re-running `/dag migrate` resumes the candidate, while an already-authoritative model fails closed.

## Model brainstorming commands

```text
/dag brainstorm [topic]                 # contextual exploration with available tools
```

There is no active durable focus, New/Resume ceremony, or restored focus link. Model calls use explicit `scope: { workstreamIds, objectIds? }`; `[]` selects repository scope for model objects. Reviews are independent records in `project.reviews`, addressed by exact review ID and revision, with scope, pending points and durable outcomes. They share the model's process-shared lock and atomic replacement so outcomes and semantic changes cannot be split across files. Multiple pending reviews are discoverable through `dag_model_context` with `view: "reviews"`; an empty workstream filter discovers all reviews. Review CAS conflicts fail rather than lose an update.

Historical `.ai/model-sessions/` files are read-only. Pending legacy reviews are discoverable and readable with exact `legacyFocusId` plus `reviewId`; they are never activated or rewritten. Optional derived Lavish artifacts live in `.ai/model-reviews/lavish/`. Their digests identify presentation cache versions, not user consent.

## Planning, inspection, and execution

The registered extension writes V2 plans and runs in ignored
`.ai/dag-workflow-v2/state.json`. Saving, previewing and revising are inert. An
explicit request to run the exact current saved content accepts that content;
there is no separate plan approval tool, field, command or hidden receipt.
Model agreement likewise uses context-informed explicit direction, not consent tokens.

```text
/dag plan [goal]
/dag show --plan <plan-id>@<revision> [--view plan|graph|lineage]
/dag show --plan <plan-id>@<revision> --view node --node <exact-item-id>
/dag show --run <exact-run-id>
/dag run <JSON object containing selection and authority>
/dag run                              # reopen only the existing session binding
```

`dag_plan_save` requires explicit `workstreamIds` and expected content revision
(`0` for creation), not a brainstorm session or saved focus. The agent chooses
scope from user intent and model context; an explicit empty array means repository
scope only, never an unknown-scope fallback. It independently reads tracked model/spec bytes and a clean
native Git baseline. Sources use `model:<collection>/<objectId>` and
`spec:<repository-relative-path>`. The persisted `workstream_scope_v2` selector drives
a fresh walk of **all** applicable, accepted-state, non-superseded governing
objects, including newly accepted objects—not just the caller's submitted refs.
Repository-wide authority always applies; an empty workstream set starts from
repository-wide objects. The closure also follows accepted governing relationships
conservatively in both directions (excluding context-only `related_to` and
historical `supersedes` links); drafts and contextual objects do not gain authority.
An unaccepted governing dependency blocks saving; at run assessment it is an
advisory observation. No submitted digest grants consent. New runs capture their
current native baseline separately from saved provenance. Worker/check/integration
dispatch checks that baseline plus the accepted Git prefix, not outdated saved
source equality. Worker candidates cannot alter model/requested-spec paths.

`dag_plan_list` enumerates exact current-head selectors without choosing a plan.
It lists repository heads, optionally filtered by explicit `workstreamIds` (an empty list is normal).
`/dag run` without a bound run asks the agent to discover and inspect a saved V2
plan, call `dag_plan_assess`, judge observations using the conversation, and start
or ask only about material unresolved concerns—or report “No active DAG to run”.
Content, non-head revision, source/baseline freshness, unresolved model directions, recommended
concurrency are observations, not consent gates. No extra
approval receipt, acknowledgement token or user-supplied hash is required.
`dag_plan_show` and `/dag show` expose plan, graph, exact node and lineage views.
Implicit plan selection requires an exact session plan binding or one unique
repository head. No timestamps, prefixes, or inferred latest selection. Reads
never acquire a lease, ingest results, repair bindings or create the V2 store.

Before dispatch, the agent must review relevant unresolved questions/directions and
pending reviews, using existing conversation for already-settled meaning. Assessment
surfaces unresolved model objects as `REVIEW_REQUIRED` findings, not token gates.
User intent is channel-neutral; Lavish is optional. Necessary scoped dependency
setup/feature edits and low-risk local prototypes are allowed; network, scripts,
credentials and external effects still need separate consideration.

Model operations, reviews, migration and V2 planning are focus-free. Historical
focus-bearing plans remain readable with original hashes; no history is rewritten.

Use `dag_run_start` (or the identical `/dag run` JSON payload):

```json
{
  "selection": {"planId": "delivery", "revision": 1},
  "authority": {
    "scope": ["first", "second"],
    "maxConcurrency": 1,
    "effects": ["repository_local"]
  }
}
```

Use the same payload with `dag_plan_assess` before start. Authority is concrete
agent-supplied execution configuration, not tool-verifiable consent evidence.
Use existing conversation to resolve findings; ask only for material unresolved
concerns. Start recomputes and retains observations in `run.acceptance`, including
the current execution baseline; historical plans are never rewritten. Execution authority has no wall-clock deadline. Concurrency is bounded to 1–512; exceeding
the plan recommendation is advisory, while resource/mutex limits remain enforced. Dependency scopes must be closed; serial integration scopes must
be prefixes. Excluded nodes cannot occupy lanes or dispatch. Restricted effects
(publication, credentials, deployment) are unsupported, never implied. Reopening
the same frozen run cannot expand authority or implicitly resume paused work.
Complete/reconciled-cancelled runs allow an exact terminal successor whose plan
names the predecessor selector and freshly observed landed baseline.

The visible agent orchestrates via read-only `dag_next_action` and semantic
`dag_start_work`, `dag_record_completion`, `dag_run_checks`, `dag_integrate`,
`dag_retry`, `dag_replace_worker`, `dag_pause`, `dag_resume`, `dag_cancel` and
`dag_finalize`. Tools derive leases/revision CAS internally while preserving
exact run/item/generation/stage-attempt/completion selectors. Generic workers
are durably keyed create-or-get operations with exact storage/attempt identities;
acknowledgement loss never means a second unkeyed launch. V2-owned requests
persist `explicitDispatchRecovery`: generic scans cannot launch reserved/planned
work after an external generation fence, and generic retry cannot create another
attempt under the same operation. The V2 host also disables scan-time launch and
generic retry of legacy-owned reservations; reopening an old session is not V1
execution continuation. Recovery dispatch goes through the V2 writer;
replacement uses a fresh exact generation/key. After dispatch, end the
turn when remaining work depends on completion; the durable completion follow-up
resumes it. Do not poll or use generic subagent launch for DAG work.

F0 precedes implementation; actual committed candidate inspection and individual
command results govern F1–F8. The shipped product profile uses shell-free
`node-local` argv checks, with independently created F2/F5 process contexts and
fresh clean F7 replay. Supply meaningful deterministic oracle/review checks,
not success-only commands. Worker reports do not confer PASS. Arbitrary producer
IDs are rejected by product save: custom semantic worker-backed evaluators are a
library adapter capability, not a shipped product implementation. Runtime gates
require a current durable PASS check with the same check ID as the declared gate.

Native `GitDriverV2.integrate` performs explicit-base composition, real prefix/final
checks in the same node worktree, guarded target CAS, and old/new/third-target recovery.
`dag_recover_dispatch`, `dag_recover_execution`, and `dag_close_git_operation`
provide exact recovery operations; missing extinction or ambiguous Git/workspace
settlement remains blocked. Cancellation fences first, then signals workers and
waits for actual settlement. Retry budgets and failure evidence survive reload.
Worker replacement is explicit and has no replacement retry budget. Its retained
count/archive is history only; settlement, generation fences, and unresolved native
operation guards still apply. Other lifecycle/integration retry ceilings remain.
Worker repair uses a new keyed generation based on the inspected prior candidate,
with frozen actual failure observations; all affected checks must run again.
Implementation, node lifecycle checks, and clean committed repairs reuse one
persistent node worktree, preserving ignored dependencies and build caches.
Exact settled legacy roots are adopted in place; durable handoff fences old
manager launch/retry/cleanup rights. Dirty or ambiguous roots block repair
without cloning, resetting, or deleting artifacts. Per-execution journals and
fresh contexts provide independent evidence, not different cwd paths. Native
integration temporarily checks out the exact combined proposal in that same root,
including accepted predecessor changes, under continuous durable ownership.
Ignored/untracked collisions block without overwriting user bytes. The original
candidate is restored and verified before target CAS or ownership release.
Historical sandbox requests/results remain readable and closable unchanged;
new product attempts use the node root. Generic worker/storage retention limits
can require operator maintenance.
The passive TUI widget shows stable current state and remounts for a successor;
headless modes never mount it. It has no fabricated V1 hash fields.

## Verification command timeout

Set `verificationCommandTimeoutMs` in the existing DAG config, for example in
`<repository>/.ai/dag.config.json`:

```json
{
  "verificationCommandTimeoutMs": 14400000
}
```

This four-hour limit applies **to each verification argv**, both lifecycle
`dag_run_checks` (including F7) and native integration prefix/final checks.
The default is `3600000` (one hour). Values must be integer milliseconds from
`1` through `2147483647`; zero, null, strings and unbounded values are rejected.
Precedence is package default → `~/.pi/agent/extensions/dag-workflow/config.json`
→ repository `.ai/dag.config.json` (library inline config overrides last).
Invalid timeout values in any layer fail rather than silently falling back.

The product rereads config at the beginning of each checks/integrate call, before
execution. New lifecycle requests freeze that call's value; native integration
freezes it at operation intent for every prefix/final request, including recovery.
Results retain the exact request's `commandTimeoutMs`. Changed config therefore
applies to **future new checks/retries of existing authorized runs**, not in-flight
requests, exact replay or already-created native operations. Legacy absent fields
mean one hour without rewriting stored JSON or hashes. No saved plan/store edit
or authority renewal is needed. Installing new extension code still requires the
normal package/HM deployment; use a fresh Pi process after updating the installed
version, rather than relying on old module caches or source-checkout overlays.
Config-only changes need no process restart once this version is loaded.

This does not introduce a whole-run/authority expiry or change worker budgets,
native Git checkout/landing time limits, retry ceilings or TERM/KILL grace.
Timeout initiates cancellation; descendant extinction and clean workspace/native
CAS proofs are still required before settlement. Library producer callbacks are
not subprocesses and retain their own settlement contract.

## Model tools

The seven tools register once and remain enabled after Pi's extension runtime initializes, without any model-mode prerequisite:

- `dag_model_context` — read scoped orientation, migration, entities, frontier, governing objects, pending review IDs, or an exact current/historical review.
- `dag_model_update` — record non-authoritative findings, relationships, routing metadata, migration source/artifact dispositions, or Current understanding. It cannot grant authority or rewrite accepted semantics.
- `dag_model_record_direction` — record explicit user direction understood from available context; no ephemeral current-turn marker or semantic receipt.
- `dag_model_review` — create an independently addressed review with **For awareness** and **Decisions needed** without replacing other pending reviews.
- `dag_model_present_review` — optionally `present`, `collect`, `resume`, or `end` an exact `reviewId` through Lavish.
- `dag_model_resolve_review` — apply explicit per-point outcomes with exact `reviewId` and `expectedRevision`; omitted and ambiguous points remain pending. Inspect changed context before choosing an outcome.
- `dag_model_specs` — preview, check, or explicitly recover deterministic generated specs.

Routine successful model mutations synchronize current specs without making a Git commit. Accepted objects superseded by another accepted object stop rendering while retaining stable identity. Direct direction does not silently resolve independent reviews: record their explicit contextual outcomes by ID. Current understanding references object IDs without requiring matching semantic hashes.

Lavish presentation uses the pinned optional dependency `lavish-axi@0.1.43`; it never falls back to ambient `npx`. The generated shell supports multiple independent decision points, complete visible option prose, an explicit **Other** radio, and a separate response box. The renderer does not resolve semantic state automatically. The agent interprets feedback in context, verifies the exact review/point/option and current revision, and resolves only settled decisions. Presentation cache digests never authorize model changes.

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

Low-risk local prototypes within the user's scope are allowed; assess destructive, network, credential and external effects separately. Hand-authored prototype evidence lives under `spec/prototypes/<slug>/` and is protected from spec generation.

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

The extension owns a generic worker runtime; it does not depend on `pi-subagents`. Every launch returns immediately while a detached supervisor runs the exact installed Pi CLI in RPC mode. Launch output gives the parent an explicit dependency-barrier rule: continue only independent work, then keep the parent task in progress and end the turn immediately when remaining work depends on the worker. The completion follow-up starts the next turn automatically without user action; status, inspection, result lookup, and diagnostic tails must not be used for completion waiting. Workers survive top-level Pi reload or exit, report through a terminating `subagent_report` tool, and deliver bounded completions serially when the owning session reconnects.

Top-level generic-worker tools (canonical DAG work uses `dag_start_work` and `dag_record_completion` instead):

- `subagent` — launch a DAG-unbound asynchronous worker;
- `subagent_status` — diagnostically list or summarize workers, never wait for completion;
- `subagent_inspect` — read a bounded immutable result for diagnosis or recovery;
- `subagent_tail` — read selected bounded diagnostics, never wait for completion;
- `subagent_cancel` — cancel only after PID/start-identity and attempt verification;
- `subagent_retry` — explicitly start a new attempt for a terminal worker.

Equivalent user commands are `/workers list|inspect|tail|cancel|retry`.

TUI status integrations can subscribe to the generic
[`pi-dag-workflow:worker-activity` snapshot bus](spec/worker-activity.md). It reports
current attempts and pending completion delivery even when the parent is idle,
with an owner/epoch-safe request handshake for extension load order and reload.
It adds no scanner and never publishes task text or headless terminal ownership.

Runtime state lives under `.ai/worker-sessions/`. One atomic `worker-session.json` belongs to each top-level Pi session; detached supervisors write bounded mailboxes, a diagnostic log capped at 50 MiB, and immutable terminal results. Child processes inherit ordinary active tools but omit `subagent*`, `dag_*`, and `dag_model_*` orchestration surfaces except for `subagent_report`. Full transcripts and cumulative `message_update` events are never persisted.

A worker becomes terminal only after its supervisor observes the exact Pi child exit and publishes the bound immutable result. Report delivery initiates shutdown but is not itself completion. Cancellation escalates from RPC abort to `SIGTERM` and `SIGKILL` against the exact child identity; failure is reported to the parent and blocks automatic retry. Retry and owned-worktree cleanup require the exact terminal result, not machine-wide proof that no unrelated process can edit the repository. The manager checks known attempt artifacts only while workers are active and never discovers workers by scanning process cwd or environments.

Direct forks and clones transfer the complete worker session and completion queue when source ownership can be proven. Ambiguous, corrupt, stale-live, PID-reused, or conflicting ownership fails closed rather than signaling or relaunching an unproven process.

Obsolete `pi-subagents` artifacts are not adopted or deleted automatically. Historical sibling directories named `*-dag-subagents` and temporary `/tmp/pi-subagents-*` trees may be removed manually only after confirming that no legacy worker process still owns them.

## Historical compatibility and supported profile

`dag_history_v1` reads an explicit V1 plan/revision, canonical run, bound workers,
Git integration state or evaluation envelope through the original validators.
`dag_history_worker` also accepts an exact generic worker storage/attempt binding.
Neither reader attaches a conductor, acquires ownership, repairs, migrates,
rewrites bytes or starts work. V1 files remain at their original paths. Existing V1 libraries and
regressions remain for compatibility; **this extension does not continue V1
runs or convert V1 evidence into V2 execution**. Do not run an old installed V1
conductor alongside V2 integration. Installed-package overlays are not replaced
by source-checkout changes.

The older `/dag validate|status|workers|inspect|tail` and
`dag_validate|dag_diagram|dag_status` diagnostics remain clearly labeled legacy
read-only views. Their old latest-selection behavior is never used for V2.
`/dag review`, `/dag retro`, `/dag archive` and separate `/dag chunk` are not
implemented (`/dag plan` includes decomposition).

The supported V2 profile is Linux/local filesystem with util-linux flock,
readable same-namespace `/proc`, Python 3.9+ and **Git 2.54.0, files refs,
SHA-1/SHA-256**. Unsupported capabilities fail closed. Native landing requires a
quiescent session worktree. Sparse/partial/shallow repositories, attributes,
gitlinks and unsafe composition overrides are unsupported. User hooks and
configured Git 2.54 hooks are disabled without rewriting repository config.
No reset, stash, force landing or forced ambiguous-workspace cleanup occurs.
The repository must ignore `.ai/` and have a clean committed baseline; the
extension does not edit ignore rules automatically. A session retaining any V1
binding must use a new unbound Pi session for V2 (no implicit cross-version
adoption, even for a terminal V1 run).
See [the V2 API and safety profile](extensions/dag-workflow/runtime-v2/README.md).

Local-effect authority is not an arbitrary-code/network sandbox: trusted argv
and generic worker tools must honor their bounded task and no-edit contracts.
No dependencies or credentials are installed automatically. OS isolation is
required for hostile same-UID code, namespace tampering or escaped effects.

## Runtime-v2 local command capability

Worker immutable artifacts also require Linux `renameat2(RENAME_NOREPLACE)` exported by libc and `python3` on `PATH` (stdlib `ctypes`). The packaged `worker-runtime/rename-no-replace.py` helper installs a synced temporary file atomically without overwriting an existing result or exposing a transient hard-link alias. Directory fsync precedes acknowledgement, including identical-content replay after interruption. Unsupported kernels/filesystems or missing helpers fail closed; trusted readers still reject foreign hard links. The install syscall is the atomic publication point: before it, only the uncommitted temporary exists, but a surviving helper can still complete an interrupted publisher's install. Installation exposes the complete, single-link inode; exact replay finishes directory durability if acknowledgement was interrupted.

The runtime-v2 `CommandRunnerV2` requires Linux in the same PID namespace, readable `/proc`, and `python3` on `PATH` (Python 3.9+ stdlib `ctypes`, `os.pidfd_open`, `signal.pidfd_send_signal`; Linux `PR_SET_CHILD_SUBREAPER` and pidfds). Missing Python, denied kernel capabilities, or an unavailable packaged helper fail closed before command launch. No Python package or installation is performed. The helper is shipped under `extensions/dag-workflow/runtime-v2/command-supervisor.py` and resolved relative to the importing module, not the candidate or current directory.

A detached, gated subreaper announces its boot/start identity only after enabling subreaping. The runner syncs a request/identity/nonce launch journal and rechecks the current intent under the store lock before sending argv launch permission. The supervisor records the actual argv exit independently, adopts/reaps orphan descendants, and publishes a synced nonce/identity/workspace-bound extinction receipt **only after `waitpid(-1)` returns `ECHILD`**. A finite fork/exit handoff cannot disappear between observations. `/proc` enumeration is used only for positive cancellation signaling via pidfds, never as an extinction proof. Abort, per-command timeout, and owner pipe loss start TERM then KILL escalation; the supervisor continues after argv/owner exit and does not kill itself during escalation.

No durable extinction receipt after launch means no lifecycle result, PASS, retry, or workspace cleanup. The job and workspace remain unresolved, including when a supervisor dies and `/proc` appears empty. Recovery never relaunches the natural request. `reconcileInterrupted` still requires an independent process-tree/**effect** settlement callback, even with a receipt; without one, supervisor death is ambiguity, not settlement. A live supervisor without a receipt must finish reaping first. Successful reconciliation emits infrastructure BLOCKED, retains the workspace, and requires an explicit bounded retry. This is trusted local command process management, **not a sandbox**: hostile same-UID receipt tampering, session/namespace escape, and effects delegated to unrelated services are outside containment. Trusted producer callbacks retain their own effect protocols.

## Legacy migration adapter

`/dag migrate` automatically recognizes the previous `.ai/brainstorm/structured-brainstorming.json` snapshot and uses its deterministic mapper as a fast path. The repository-only `node scripts/migrate-brainstorm-to-project-model.mjs` command remains available for reproducing that adapter directly. It emits the candidate model, mapping/omission report, and ignored generated preview, but never bypasses the same semantic audit, artifact dispositions, Lavish review, freshness checks, or exact cutover required by the product command.

## Source-checkout validation

These repository release and test commands use tracked model/spec fixtures and Git history; they are contributor checks, not installed-package runtime commands.

```nu
npm run smoke
npm run test:model
npm run test:dag-planning
npm run test:dag-planning-runtime
npm run test:dag-planning-command
npm run test:dag-prepared-start
node scripts/dag-v2-product-test.mjs
npm run test:dag-runtime
npm run test:dag-evaluation
npm run test:dag-dogfood -- --group lifecycle
npm run test:dag-dogfood-portfolio -- --template recovery-sensitive --drill conductor_crash_resume
npm run test:git-integration
npm run test:workers
npm run test:release-impact
npm run release:impact             # explain changed paths and selected release gates
npm run release:ready              # impact-aware gates plus one packed-artifact smoke pass
npm run release:full               # uncached full dogfood/portfolio certification
# Only while project-model/model.json is still a non-authoritative candidate:
node scripts/migrate-brainstorm-to-project-model.mjs --force
```

`release:ready` compares `HEAD` with the latest prior semantic release tag (or `--base <ref>` / `PI_RELEASE_BASE`), classifies every changed path through a fail-closed impact map, and runs only affected focused suites, dogfood groups, portfolio templates, and recovery drills. It then runs one package-mode smoke pass against the extracted npm artifact to verify contents, entrypoint loading, release-impact policy, and direct package helpers without repeating the focused process/Git matrices. Unknown paths and broad canonical primitives escalate to the full gate. Successful expensive gates are reused only through hash-validated local receipts under `$XDG_CACHE_HOME/pi-dag-workflow/release-evidence-v1` (or `~/.cache/...`) bound to the exact relevant Git tree, command, executable hashes, Node/Git toolchain, kernel/platform, locale, and timezone; use `--no-cache` to bypass them. Broad smoke runs once against the extracted npm artifact. `release:full` remains the periodic uncached certification path. Allow at least 28,800 seconds for the outer full-release command. The full V2 product suite has a 14,400-second nested budget in both release readiness and source smoke; other suite budgets remain 3,600 seconds.

`dag-v2-product-test.mjs` is registered V2 end-to-end coverage. The older V1
planning/runtime/dogfood/evaluation suites remain compatibility coverage, not
substitutes for V2 product certification. Full uncached release readiness and
extracted-package smoke must run on the joined clean candidate.

The test suites cover structural model validation, explicit-direction boundaries, concurrent model/review CAS, independent sparse review resolution, no-focus CRUD and migration, historical read-only focus records, deterministic plan projections and lineage, exact command selection, real-Git source/baseline validation, crash-recoverable prepared start, canonical runtime compilation, whole-run replanning, always-enabled Pi tools across forks, legacy read-only compatibility, generic migration bootstrap/resume, no-overwrite staging, source and file-transaction freshness, preserved side-by-side specs, approved projection collisions, legacy-adapter dispatch, and authoritative-model refusal.
