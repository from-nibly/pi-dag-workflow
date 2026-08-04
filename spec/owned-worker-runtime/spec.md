<!-- generated-by: pi-dag-workflow/project-model; view: SPEC-owned-worker-runtime; contract: 1; input: sha256:7c53dbb936e1a318df8f355969b484e5a72b8104b6ec043796843217b20c8ec7 -->

# Owned asynchronous DAG worker runtime

Accepted lifecycle, execution, completion, resource-safety, and DAG UI behavior for the extension-owned worker runtime.

## Accepted runtime direction

<a id="obj-dec-owned-async-worker-runtime"></a>

### Own an always-asynchronous worker runtime

This extension must implement and own its DAG worker runtime instead of relying on `pi-subagents`. Every worker launch is asynchronous relative to the main agent. Each terminal completion, failure, cancellation, or timeout is durably ingested and reported back to the main agent through a bounded completion message; the main agent never has to block on or manually relay a worker tool result.

**Rationale.** The prior dependency has produced unacceptable event-log/resource behavior and cannot support the desired DAG-native lifecycle and UI contract.

<a id="obj-dec-subprocess-rpc-worker-kernel"></a>

### Use process-isolated Pi RPC workers

V1 workers run as child processes through the exact installed Pi CLI in RPC mode, using ephemeral sessions by default. The runtime incrementally parses strict LF-framed protocol records, persists no raw event stream, treats `agent_settled` as terminal completion, sends RPC abort before process-group termination, and supplies explicit model, thinking, trust, context, tool, and extension profiles.

**Rationale.** Process isolation contains resource failures and preserves installed Pi/provider compatibility while relying on a public protocol rather than another extension's private internals.

<a id="obj-dec-workers-survive-pi-reload-exit"></a>

### Workers survive Pi reload and exit

Asynchronous workers must continue running when the owning Pi extension reloads or the top-level Pi process closes, so the top-level agent can be restarted without losing active work. Completion state is retained durably and reported after a compatible top-level Pi session reconnects; this requirement does not by itself require a centralized daemon.

**Rationale.** The top-level agent may need to restart independently while delegated work remains useful and in progress.

<a id="obj-dec-serial-worker-completion-queue"></a>

### Drain worker completions through a serial follow-up queue

Each terminal subagent completion is durably enqueued. When no completion follow-up is in flight, the runtime delivers exactly one completion message to the top-level agent. After the agent finishes reacting to that completion and settles, the runtime delivers the next queued completion, continuing one by one until the queue is drained. Stable completion IDs prevent duplicate enqueue or delivery across reload.

**Rationale.** Serial delivery lets the top-level agent fully process each worker outcome without simultaneous completion turns, while preserving every completion independently rather than collapsing a burst into one summary.

<a id="obj-dec-detached-per-run-worker-supervisors"></a>

### Use detached per-run worker supervisors

Workers survive top-level Pi reload and exit through detached per-run supervisor processes, not a centralized daemon. Each supervisor runs in an independent process group and persists only bounded state: repository/run/worker/attempt IDs, an unguessable ownership token, PID plus process-start identity, heartbeat, compact atomic snapshot, structured result envelope, and completion receipt. On reconnect, the extension verifies process identity, restores the DAG and live UI projection, reconciles completed, missing, stale, and orphaned workers, and queues undelivered completions. Raw Pi message, reasoning, tool-delta, and event streams are never persisted.

**Rationale.** This retains independent worker lifetime with less infrastructure than a daemon while directly correcting the resource, identity, recovery, and UI weaknesses observed in pi-subagents.

<a id="obj-dec-minimal-message-update-hotfix"></a>

### Apply only the message-update logging hotfix

V1's mandatory safety baseline only drops `message_update`, retains selected final message, tool, lifecycle, and control diagnostics, and caps child diagnostics at 50 MiB per run. Atomic conductor behavior, explicit profiles, structured terminal results, process identity, orphan recovery, idempotent completion, and broader budgets remain separate future decisions.

**Rationale.** The first implementation should address only the measured logging amplification and avoid bundling broader runtime correctness or resource policy.

<a id="obj-dec-explore-runtime-contracts-first"></a>

### Explore recovery, RPC, and completion contracts first

The next focused exploration must jointly define: the detached worker recovery and reconciliation contract; the exact Pi RPC worker launch, context, and capability contract; and the serial one-at-a-time completion queue contract. Defer live DAG UI detail, model-aware planning/execution restoration, and dependency removal until this combined runtime-contract frontier is coherent.

**Rationale.** These three contracts share one state machine and establish the canonical runtime projection consumed by later scheduling and UI work.

<a id="obj-dec-single-writer-fail-closed-worker-recovery"></a>

### Use single-writer state and fail-closed detached recovery

One attached top-level worker supervisor is the sole writer of a schema-versioned atomic worker-session snapshot through one serialized mutation queue. Detached workers write only bounded atomic mailboxes and immutable terminal results. Attempts bind top-level session, worker, and attempt IDs, a random attempt nonce used for stale-file correlation, PID plus OS process-start identity, config hash, heartbeat, and cwd. Reconnect scans results before mailboxes, ingests each valid result once, restores only matching live workers, and marks proven-dead attempts lost. Launch-ambiguous, stale-live, identity-mismatch, corrupt-artifact, concurrent-owner, and cwd-conflict states fail closed to needs-decision; the runtime never auto-relaunches or signals a process whose ownership is not proven. The nonce is a correctness generation marker, not a security boundary against malicious local code.

**Rationale.** Fail-closed recovery prevents duplicate workers and PID-reuse mistakes across an unavoidable non-transactional OS spawn boundary without pretending local worker artifacts are a security boundary.

<a id="obj-dec-ambient-rpc-worker-inheritance"></a>

### Inherit the top-level Pi environment and tools in RPC workers

RPC workers load the normal top-level/global Pi extensions, skills, prompt templates, provider registrations, project context, and tools so custom providers and established project capabilities work without a separate mapping layer. The runtime does not treat child tools as a security boundary and does not maintain a tool denylist merely to restrict authority. Role prompts and task instructions define correct worker behavior, including that workers must not recursively delegate, mutate project-model authority, or perform top-level orchestration unless explicitly assigned.

**Rationale.** This local runtime is trusted, and clear worker instructions preserve compatibility more simply than reconstructing or denying the top-level Pi capability environment.

<a id="obj-dec-report-tool-with-automatic-repair-prompt"></a>

### Require worker report tool use with deterministic repair prompting

RPC workers receive a `dag_worker_report` tool and explicit instructions to call it with their bounded terminal report. The supervisor deterministically tracks whether a valid report tool result was captured. If Pi reaches `agent_settled` without one, the supervisor automatically sends a report-only follow-up instructing the worker to call the tool, then waits for the next settled state. The retry count is bounded by configuration; exhaustion produces a report-missing terminal result with bounded final text rather than inferred success.

**Rationale.** A hybrid tool-plus-repair flow keeps structured results reliable even when the initial worker turn omits the required report call.

<a id="obj-dec-subagent-runtime-dag-agnostic"></a>

### Keep subagent mechanics independent of DAG orchestration

The owned subagent runtime manages asynchronous worker launch, detached recovery, result reporting, and per-top-level-session completion delivery only. It does not mutate DAG state, compute DAG readiness, gate dependent nodes, launch DAG work, merge worktrees, or otherwise embed current DAG semantics. Completion messages return worker outcomes to the top-level agent, which alone decides how to interpret them and interact with the current or future DAG workflow. DAG integration will be redesigned separately.

**Rationale.** The DAG workflow needs independent rework, and coupling generic subagent mechanics to its current state model would prematurely constrain both systems.

<a id="obj-dec-one-completion-queue-per-top-level-session"></a>

### Use one completion queue per top-level Pi session

Each top-level Pi session owns exactly one durable serial worker-completion queue and one in-flight completion slot, regardless of how tasks are later related to DAGs or other workflows. Every worker launched for that top-level session enqueues into the same queue. The dispatcher deterministically drains one completion-triggered turn at a time. This is a permanent runtime boundary, not a v1 restriction to one mutable DAG run.

**Rationale.** The top-level agent is the unit that receives and handles completions; queue scope should follow that agent session rather than DAG runs.

<a id="obj-dec-auto-ack-worker-completion-on-settle"></a>

### Automatically acknowledge worker completion on settle

The worker manager treats the first `agent_settled` event after a completion-triggered turn as acknowledgement of that in-flight completion and schedules the next top-level-session queue item without requiring an explicit disposition tool call.

**Rationale.** Lower completion-turn overhead is more important than a durable semantic acknowledgement.

<a id="obj-dec-automatically-transfer-workers-to-direct-fork"></a>

### Automatically transfer workers to a direct fork or clone

When a new Pi session header directly names the launching session as `parentSession`, and no source manager remains attached, the runtime durably and atomically transfers that worker-session and completion queue to the descendant UUID so correction forks and clones preserve live work. Later results and delivery follow the descendant. Unrelated new sessions and ordinary switches do not adopt workers. Transfer receipts and crash reconciliation prevent dual ownership. General transfer to an unrelated session, including correction of a mistaken automatic transfer, is deferred until field use demonstrates the need.

**Rationale.** Correction forks and clones are common enough that live workers should follow the active conversational branch automatically; arbitrary cross-session transfer is unlikely and does not justify v1 complexity.

<a id="obj-dec-generic-subagent-tool-namespace"></a>

### Use a generic subagent tool namespace and /workers commands

Expose an always-asynchronous `subagent` launch tool plus narrow `subagent_status`, `subagent_inspect`, `subagent_tail`, `subagent_cancel`, and `subagent_retry` management tools for the top-level agent. Expose `subagent_report` as the structured child report tool. Equivalent user controls use `/workers list|inspect|tail|cancel|retry`. Remove the dormant `dag_subagent` adapter and do not use `/dag` for generic worker control. This renames the earlier proposed `dag_worker_report` surface without changing the accepted structured-report behavior.

**Rationale.** The public API should describe the generic owned subagent runtime rather than the deferred DAG adapter.

<a id="obj-dec-child-inherits-tools-minus-parent-orchestration"></a>

### Inherit normal child tools while omitting parent orchestration surfaces

Ordinary RPC workers inherit the top-level Pi providers, normal extensions, project context, skills, prompt templates, trust, and every unrelated active tool. At launch, the runtime snapshots the parent active tool names and passes an explicit child set that adds `subagent_report` but omits `subagent`, `subagent_status`, `subagent_inspect`, `subagent_tail`, `subagent_cancel`, `subagent_retry`, all `dag_*` tools, and all `dag_model_*` tools. Worker-role metadata causes this extension's parent entrypoint to return before attaching its worker-session writer, parent result watcher, completion dispatcher, or management UI; only child reporting initializes. The prompt forbids recursive delegation. The runtime does not guess at or deny arbitrary third-party tools, and headless interactive UI requests are answered with cancellation rather than allowed to hang. Nested fanout is deferred; if later needed, it requires explicit opt-in with child-safe routing and depth/cycle limits. This narrows `DEC-ambient-rpc-worker-inheritance` only for parent-owned orchestration and authority surfaces, not as a general security boundary.

**Rationale.** This uses the established Pi child-role pattern to prevent recursion and duplicate parent services without broadly reducing worker capability.

<a id="obj-dec-two-default-report-repair-turns"></a>

### Default to two bounded automatic report-repair turns

`reportRepairAttempts` is configurable from 0 through 2 and defaults to 2. The supervisor sends up to two deterministic report-only prompts before finalizing report-missing and needs-attention.

**Rationale.** Structured report recovery is worth up to two additional model turns by default.

<a id="obj-dec-compact-subagent-completion-message"></a>

### Inject compact worker completions and inspect details on demand

Each serial worker completion follow-up is capped at 16 KiB and includes stable completion ID, worker ID/label and attempt, canonical terminal status, report status, bounded worker summary, artifact references, optional next steps, and concise `subagent_inspect`/`subagent_tail` hints. Truncation is explicit. Report details, raw transcript, tool history, diagnostic logs, and artifact contents are never injected automatically; the immutable terminal result remains available through inspection. The first top-level `agent_settled` after this follow-up automatically acknowledges it under the accepted queue policy.

**Rationale.** Compact identity, status, summary, and references let the top-level agent react without duplicating worker history or consuming unbounded context.

<a id="obj-dec-minimal-subagent-terminal-result-contract"></a>

### Use a minimal terminating report and supervisor-derived terminal envelope

`subagent_report` is a terminating typed tool. Its worker-authored schema requires `outcome: completed | needs_attention` and `summary`; optional fields are bounded `details`, artifact references, and next steps. The serialized report is capped at 64 KiB, with summary at 8 KiB, details at 32 KiB, at most 32 artifact references, and at most 16 next steps. Artifact paths are references only and are never automatically read or injected. The tool returns the validated report in RPC `tool_execution_end` details with `terminate: true`. The supervisor then waits for `agent_settled` or process termination and writes one immutable schema-versioned result containing stable completion/worker/attempt/config identity, canonical terminal status, report status and optional report, timestamps, process exit/signal, Pi/model stop reason, model/usage, bounded fallback text, diagnostic and artifact references, and result hash. Canonical status precedence is: verified cancellation → cancelled; lost or ambiguous process identity → lost or needs_attention; nonzero process exit, RPC/protocol failure, or model error → failed; settled valid needs-attention report → needs_attention; settled valid completed report → succeeded; settled missing report after repair → needs_attention. A captured completed claim is preserved but never overrides observed failure.

**Rationale.** A small generic worker claim plus an independently observed runtime envelope is reliable across task types and preserves provenance without DAG coupling.

<a id="obj-dec-generic-worker-idempotent-consumer-contract-v1"></a>

### Put idempotent launch and retry-safe reconciliation in the generic worker runtime

The generic owned-worker runtime—not a DAG-only compatibility layer—owns process-shared worker-session lock/CAS; opaque caller `launchKey` plus full normalized request hash; atomic reserve-or-launch returning an existing exact attempt on replay and rejecting conflicting requests; exact storage, launch-owner-session, worker, attempt, nonce, config, supervisor, and child identities; durable result enumeration/read-by-launch-key independent of completion delivery or auto-ack; process disposition and `retrySafe` proof distinct from terminal status; preservation/quarantine of late, corrupt, conflicting, and recovery artifacts; owner-managed retry tokens that prevent unbound manual retries; and verified approved disposable working roots. These are generic delegated-process guarantees and expose no plan, DAG node, phase, gate, worktree, merge, or integration semantics. The DAG adapter binds opaque identities, validates lifecycle evidence, and remains the only interpreter of DAG success.

**Rationale.** Launch idempotency and proof that an old process can no longer mutate its cwd are process-service responsibilities. Reimplementing them in the DAG adapter would leave other consumers unsafe and create two competing recovery protocols.
