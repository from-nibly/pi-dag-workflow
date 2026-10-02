import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { StrictObject, canonicalHash } from "../dag-runtime/common.ts";
import { PlanInputV2Schema, PlanSelectorV2Schema, IdV2, TextV2, CountV2, requireV2, sameV2, selectorV2, renderPlanV2, validateShapeV2, type PlanV2 } from "../planning/v2.ts";
import { PlanningFreshnessV2 } from "../planning/freshness-v2.ts";
import { AuthorityV2Schema, WorkerBindingV2Schema, WorkerDirectionV2Schema } from "./state.ts";
import { CandidateV2Schema } from "./lifecycle-schema.ts";
import { ProductV2 } from "./product.ts";
import { ProductWidgetV2 } from "./widget.ts";
import { historicalV1 } from "./historical.ts";
import { currentExecutionV2 } from "./lifecycle.ts";

const bindingEntry = "dag-planning-session-binding-v2";
const { repository: _repository, source: _source, ...content } = PlanInputV2Schema.properties;
export const ProductSaveV2Schema = StrictObject({ ...content, expectedPlanRevision: Type.Integer({ minimum: 0 }),
  workstreamIds: Type.Array(IdV2, { maxItems: 512, uniqueItems: true, description: "Choose scope from user intent and model context. An explicit empty array selects repository-wide meaning only." }),
  sourceRefs: Type.Array(TextV2, { maxItems: 512, uniqueItems: true }), scopeSummary: TextV2 });
const ProductSelectionV2Schema = StrictObject({ planId: IdV2, revision: CountV2, planHash: Type.Optional(PlanSelectorV2Schema.properties.planHash) });
export const ProductRunV2Schema = StrictObject({ selection: ProductSelectionV2Schema, authority: AuthorityV2Schema });
const selectedNode = { runId: IdV2, itemId: IdV2, generation: CountV2 };
const result = (value: unknown, text = JSON.stringify(value, null, 2)) => ({ content: [{ type: "text" as const, text: text.length > 48000 ? `${text.slice(0, 48000)}\n[truncated; select a node or execution]` : text }], details: value });

export function registerProductV2(pi: ExtensionAPI, options: { workerManager: any }) {
  const products = new Map<string, ProductV2>();
  const product = (ctx: any) => {
    const root = resolve(ctx.cwd), sessionId = String(ctx.sessionManager.getSessionId()), key = `${root}\0${sessionId}`;
    let value = products.get(key); if (!value) { value = new ProductV2(root, sessionId, options.workerManager); products.set(key, value); } return value;
  };
  const widget = new ProductWidgetV2(product);
  const observations = async (_ctx: any, plan: PlanV2, selection: any) => {
    const findings: string[] = [];
    if (selection.planHash && selection.planHash !== plan.planHash) findings.push("CONTENT_HASH_MISMATCH: supplied content identity differs from the selected saved revision; inspect actual content");
    return findings;
  };
  const explicitPlan = async (ctx: any, selection: any) => {
    const snapshot = await product(ctx).runtime.store.read();
    const plan = snapshot.plans[selection.planId]?.find(p => p.revision === selection.revision);
    requireV2(plan, "PLAN_NOT_FOUND_V2"); return plan;
  };
  const bind = (ctx: any, plan: PlanV2) => pi.appendEntry(bindingEntry, { root: resolve(ctx.cwd), sessionId: String(ctx.sessionManager.getSessionId()), selection: selectorV2(plan) });
  const select = async (ctx: any, exact?: string) => {
    const service = product(ctx), snapshot = await service.runtime.store.read();
    if (exact) {
      const match = /^([A-Za-z0-9][A-Za-z0-9._-]*)@([1-9][0-9]*)$/.exec(exact); requireV2(match, "EXACT_PLAN_ID_REVISION_REQUIRED");
      const plan = snapshot.plans[match[1]]?.find(p => p.revision === Number(match[2])); requireV2(plan, "PLAN_NOT_FOUND_V2"); return plan;
    }
    const entries = ctx.sessionManager.getBranch?.() ?? [];
    const binding = [...entries].reverse().find((e: any) => e.type === "custom" && e.customType === bindingEntry && e.data?.root === service.root && e.data?.sessionId === service.sessionId);
    if (binding) return service.runtime.show(binding.data.selection);
    const heads = Object.values(snapshot.plans).map(p => p.at(-1)!);
    requireV2(heads.length === 1, `PLAN_SELECTION_REQUIRED: ${heads.slice(0, 8).map(p => `${p.planId}@${p.revision}`).join(", ")}`); return heads[0];
  };
  const register = (name: string, description: string, parameters: any, execute: (params: any, ctx: any, signal?: AbortSignal) => Promise<any>, readOnly = false) => {
    pi.registerTool({ name, label: name, description, parameters,
      async execute(_id, params, signal, _onUpdate, ctx) {
        validateShapeV2(parameters, params);
        const value = await execute(params, ctx, signal); if (!readOnly) await widget.refresh(); return result(value);
      } });
  };
  register("dag_plan_save", "Save/revise inert V2 plan content from explicit workstream scope chosen using conversation and model context. No saved focus or brainstorm session is required. Sources and native baseline are independently hydrated. expectedPlanRevision=0 creates a new plan. Saving never executes work.", ProductSaveV2Schema, async (params, ctx) => {
    const service = product(ctx), snapshot = await service.runtime.store.read(), head = snapshot.plans[params.planId]?.at(-1);
    requireV2((head?.revision ?? 0) === params.expectedPlanRevision, `STALE_PLAN_REVISION: current ${head ? JSON.stringify(selectorV2(head)) : "absent"}`);
    const selector = { kind: "workstream_scope_v2" as const, workstreamIds: [...params.workstreamIds].sort() };
    const { expectedPlanRevision: _, workstreamIds: _scope, sourceRefs, scopeSummary, ...input } = params;
    requireV2(input.workItems.every((n: any) => n.lifecycle.checks.every((c: any) => c.procedure.kind === "command" && c.environment === "node-local")), "PRODUCT_PROFILE_REQUIRES_ACTUAL_NODE_LOCAL_COMMANDS");
    const hydrated = await new PlanningFreshnessV2(service.root).hydrate(selector, sourceRefs, scopeSummary);
    const plan = await service.runtime.save({ ...input, ...hydrated }, snapshot.revision); bind(ctx, plan); return plan;
  });
  register("dag_plan_list", "List exact retained V2 current-head selectors, sorted by plan ID, without selecting or starting any plan. Lists repository current heads, optionally filtered by explicit workstream IDs. total=0 means no saved V2 plans in that scope; follow pagination otherwise. No latest-by-time heuristic.", StrictObject({ workstreamIds: Type.Optional(Type.Array(IdV2, { maxItems: 512, uniqueItems: true })), offset: Type.Optional(Type.Integer({ minimum: 0 })), limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 64 })) }), async (p, c) => {
    const snapshot = await product(c).runtime.store.read();
    const heads = Object.values(snapshot.plans).map(revisions => revisions.at(-1)!).filter(plan => !p.workstreamIds || sameV2([...(plan.source.selector?.workstreamIds ?? [])].sort(), [...p.workstreamIds].sort())).sort((a, b) => a.planId.localeCompare(b.planId));
    const offset = p.offset ?? 0, limit = p.limit ?? 32;
    return { total: heads.length, offset, plans: heads.slice(offset, offset + limit).map(plan => ({ ...selectorV2(plan), title: plan.title.slice(0, 256), sourceSelector: plan.source.selector })) };
  }, true);
  const planView = (plan: PlanV2, view = "plan", itemId?: string) => {
    if (view === "graph") return { selection: selectorV2(plan), nodes: plan.workItems.map(n => ({ id: n.id, title: n.title })), edges: plan.workItems.flatMap(n => n.dependsOn.map(from => ({ from, to: n.id }))) };
    if (view === "lineage") return { selection: selectorV2(plan), predecessor: plan.predecessor ?? null };
    if (view === "node") { const item = plan.workItems.find(n => n.id === itemId); requireV2(item, "EXACT_ITEM_ID_REQUIRED"); return item; }
    requireV2(["plan", "all"].includes(view), "UNKNOWN_PLAN_VIEW"); return plan;
  };
  register("dag_plan_show", "Read one exact retained V2 plan, graph, node or lineage. No save, session binding, lease, worker ingestion or execution.", StrictObject({ selection: ProductSelectionV2Schema, view: Type.Optional(Type.Union(["plan", "all", "graph", "lineage", "node"].map(x => Type.Literal(x)))), itemId: Type.Optional(IdV2) }), async (p, c) => planView(await explicitPlan(c, p.selection), p.view, p.itemId), true);
  register("dag_plan_assess", "Read content, scope, authority configuration and freshness observations for an explicit V2 plan ID/revision (hash not required). Inspect dag_plan_show first. Judge findings using existing conversation; ask only about material unresolved concerns. This is not consent verification and issues no mandatory acknowledgement token. No mutation. Output bounded to 48K characters.", ProductRunV2Schema, async (params, ctx) => {
    const plan = await explicitPlan(ctx, params.selection);
    const assessment = await product(ctx).runtime.assess(selectorV2(plan), params.authority, await observations(ctx, plan, params.selection));
    return { selection: selectorV2(plan), assessment, authority: params.authority, savedRepository: plan.repository, savedSource: plan.source };
  }, true);
  register("dag_plan_findings", "Read paginated full current advisory findings omitted/shortened by dag_plan_assess or run.acceptance. Supply the same selection and authority; offset defaults to 0, limit to 32 (maximum 64). Follow nextOffset until null. Reobserves current repository, not historical acceptance; compare returned repository baseline across pages. No mutation or consent gate.", StrictObject({ ...ProductRunV2Schema.properties, offset: Type.Optional(Type.Integer({ minimum: 0 })), limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 64 })) }), async (params, ctx) => {
    const plan = await explicitPlan(ctx, params.selection);
    return product(ctx).runtime.findingDetails(selectorV2(plan), params.authority, params.offset, params.limit, await observations(ctx, plan, params.selection));
  }, true);
  const start = async (params: any, ctx: any) => {
    const service = product(ctx), plan = await explicitPlan(ctx, params.selection);
    const run = await service.start(selectorV2(plan), params.authority, await observations(ctx, plan, params.selection)); bind(ctx, plan); widget.mount(ctx); return run;
  };
  register("dag_run_start", "Start the explicit saved V2 plan ID/revision (no content hash or brainstorming focus required) after conversation-informed judgment. Supply concrete bounded authority configuration, not consent evidence. Recomputes and records advisory content/scope/freshness findings in run.acceptance, capturing the current execution baseline without rewriting the plan. No acknowledgement token, V1 start or automatic resume. Malformed structure, native races and execution fences remain hard.", ProductRunV2Schema, start);
  register("dag_next_action", "Read exact V2 selectors and frontier (up to 64 actions; filter by exact itemId if omittedActions is nonzero); never leases, dispatches, ingests or repairs. After a worker launch end the turn; completion notification resumes orchestration.", StrictObject({ runId: Type.Optional(IdV2), itemId: Type.Optional(IdV2) }), (p, c) => product(c).next(p.runId, p.itemId), true);
  for (const name of ["dag_run_status", "dag_run_diagram", "dag_run_inspect", "dag_run_tail", "dag_run_explain"]) register(name, "Read V2 current state/graph/inspection/actual-result tail/diagnostics without mutation. Output is bounded to 48K characters; use exact itemId or executionId for narrow evidence. runId defaults only to the current session binding.", StrictObject({ runId: Type.Optional(IdV2), itemId: Type.Optional(IdV2), executionId: Type.Optional(IdV2) }), async (p, c) => {
    const { snapshot, run, plan } = await product(c).read(p.runId);
    if (!run || !plan) return { run: null };
    requireV2(!p.itemId || run.nodes[p.itemId], "ITEM_NOT_FOUND");
    const items = plan.workItems.filter(n => !p.itemId || n.id === p.itemId);
    if (p.executionId) {
      const execution = items.flatMap(n => run.nodes[n.id].lifecycle?.executions ?? []).find(e => e.request.id === p.executionId);
      const git = run.gitOperations?.filter(o => !p.itemId || o.itemId === p.itemId).flatMap(o => o.checks).find(r => r.id === p.executionId);
      requireV2(execution || git, "EXECUTION_NOT_FOUND"); return execution ?? snapshot.executions?.[p.executionId] ?? { request: git, result: null };
    }
    if (name === "dag_run_inspect") return p.itemId ? { item: items[0], node: run.nodes[p.itemId] } : run;
    const summary = { runId: run.runId, revision: run.revision, status: run.status, selection: run.start.selection };
    if (name === "dag_run_tail") return { run: summary, executions: items.flatMap(n => run.nodes[n.id].lifecycle?.executions ?? []).slice(-10).map(e => ({ id: e.request.id, itemId: e.request.itemId, generation: e.request.generation, attempt: e.request.attempt, stage: e.request.stage, candidate: e.request.candidate, procedure: e.request.check.procedure, status: e.status, disposition: e.result?.disposition, exitCode: e.result?.exitCode, signal: e.result?.signal, durationMs: e.result?.durationMs, diagnostic: e.result?.diagnostic.slice(-1500), stdout: e.result?.stdout.slice(-1500), stderr: e.result?.stderr.slice(-1500) })) };
    const nodes = items.map(item => { const n = run.nodes[item.id]; return { id: item.id, title: item.title, status: n.status, generation: n.generation, stage: n.lifecycle?.stage, passed: n.lifecycle?.passed, ready: n.lifecycle?.ready, stop: n.lifecycle?.stop, workerId: n.reservation?.workerId, completion: n.reservation?.completion,
      ...(name === "dag_run_explain" ? { waitingDependencies: item.dependsOn.filter(id => run.nodes[id].status !== "complete"), waitingGates: item.gates.filter(g => !run.releasedGates.includes(g)), retries: n.retries ?? [], findings: n.lifecycle?.findings ?? [] } : {}) }; });
    return { run: summary, nodes, ...(name === "dag_run_diagram" ? { edges: items.flatMap(n => n.dependsOn.map(from => ({ from, to: n.id }))) } : {}), git: run.gitOperations?.map(o => ({ operationId: o.operationId, itemId: o.itemId, phase: o.phase, workspace: o.workspace ? { cwd: o.workspace.node.cwd, phase: o.workspace.phase, closing: o.workspace.closing ?? false } : undefined, observation: o.observation, diagnostic: o.diagnostic })) ?? [] };
  }, true);
  register("dag_start_work", "Reserve F0 and create-or-get one exact generic implementation worker. Optional direction is a complete current task plus user-message provenance, replacing obsolete item narrative without changing scope/oracle/tests/effects. Pending direction otherwise applies. Replay keeps the exact envelope; conflicting explicit direction is fenced. Continue independent work, otherwise end the turn.", StrictObject({ ...selectedNode, direction: Type.Optional(WorkerDirectionV2Schema) }), (p, c) => product(c).startWork(p.runId, p.itemId, p.generation, p.direction));
  register("dag_set_worker_direction", "Set a complete current contextual user task snapshot and user-message provenance for this item. Pending first launches and future replacements use it; existing reserved/dispatched envelopes replay unchanged. Replaces prior task narrative, not scope/oracle/tests/effect authority. No launch or resume.", StrictObject({ ...selectedNode, direction: WorkerDirectionV2Schema }), async (p, c) => { const s = product(c); return s.runtime.setWorkerDirection(await s.mutation(p.runId), p.itemId, p.generation, p.direction); });
  register("dag_recover_dispatch", "Bind an already-created exact generic worker after dispatch acknowledgement loss, including a fenced cancelling generation. Performs no launch.", StrictObject(selectedNode), async (p, c) => { const s = product(c); return s.runtime.recoverWorkerBinding(await s.mutation(p.runId), p.itemId, p.generation, (r, workspace) => s.workers.recoverBinding(r, workspace)); });
  register("dag_record_completion", "Ingest the exact durable worker completion and inspect its committed candidate. A completion claim alone cannot pass a lifecycle check.", StrictObject({ ...selectedNode, completionId: TextV2 }), (p, c) => product(c).completion(p.runId, p.itemId, p.generation, p.completionId));
  register("dag_run_checks", "Execute applicable planned command checks with exclusive use of the persistent exact-candidate node worktree and derive one F1-F8 checkpoint. Exact stageAttemptId fences old rounds. F2/F5 use independent real command contexts; F7 replays all applicable checks.", StrictObject({ ...selectedNode, stageAttemptId: TextV2 }), (p, c, signal) => product(c).checks(p.runId, p.itemId, p.generation, p.stageAttemptId, signal));
  register("dag_integrate", "Compose the exact native proposal, validate prefix/final commands in the same exclusively owned node worktree, and restore its original candidate before target CAS and reconciled landing. No extra check clone or publish.", StrictObject({ ...selectedNode, candidate: CandidateV2Schema }), (p, c, signal) => product(c).integrate(p.runId, p.itemId, p.generation, p.candidate, signal));
  register("dag_retry", "Bounded typed retry of one actual failed current lifecycle execution; no counter reset.", StrictObject({ ...selectedNode, executionId: IdV2 }), async (p, c) => { const s = product(c); return s.runtime.retryCheck(await s.mutation(p.runId), p.itemId, p.generation, p.executionId); });
  register("dag_replace_worker", "Explicitly create a new repair generation after exact settlement. Worker replacement has no retry budget; its count is history only. Optional direction is a complete fresh task plus user-message provenance, superseding prior user stop/setup narrative but not scope, mandatory tests or effects. Required for legacy envelopes unless current direction was set explicitly. Archives the old request unchanged; inspects retained committed work independently of report success; resets verification. No dispatch or resume.", StrictObject({ ...selectedNode, completionId: TextV2, direction: Type.Optional(WorkerDirectionV2Schema) }), (p, c) => product(c).replaceWorker(p.runId, p.itemId, p.generation, p.completionId, p.direction));
  for (const action of ["pause", "resume"] as const) register(`dag_${action}`, `Explicit V2 ${action}; resume never enlarges authority or bypasses needs_replan.`, StrictObject({ runId: IdV2, disposition: Type.Optional(TextV2) }), async (p, c) => { const s = product(c); return s.runtime.control(await s.mutation(p.runId), action, p.disposition); });
  register("dag_disposition_finding", "Retain an explicit finding disposition; does not execute checks or grant effects.", StrictObject({ runId: IdV2, itemId: IdV2, findingId: IdV2, disposition: TextV2 }), async (p, c) => { const s = product(c); return s.runtime.dispositionFinding(await s.mutation(p.runId), p.itemId, p.findingId, p.disposition); });
  register("dag_release_gate", "Release a declared runtime gate only from an actual current PASS check whose ID equals that gate. This cannot grant scope, effects or model acceptance.", StrictObject({ runId: IdV2, gate: IdV2, itemId: IdV2, executionId: IdV2 }), async (p, c) => {
    const s = product(c), observed = (await s.bound(p.runId)).run;
    const execution = observed.nodes[p.itemId]?.lifecycle?.executions.find(e => e.request.id === p.executionId);
    requireV2(execution?.status === "observed" && execution.result?.disposition === "PASS" && execution.request.check.id === p.gate && currentExecutionV2(observed, execution.request), "CURRENT_GATE_COMMAND_PASS_REQUIRED");
    return s.runtime.releaseGate(await s.mutation(p.runId), p.gate, async () => {
      const current = (await s.bound(p.runId)).run;
      requireV2(currentExecutionV2(current, execution.request) && current.nodes[p.itemId].lifecycle?.executions.some(e => e.request.id === p.executionId && e.status === "observed"), "STALE_GATE_RESULT");
      const result = await s.runner.read(execution.request); requireV2(result && sameV2(result, execution.result), "DURABLE_GATE_RESULT_REQUIRED");
    });
  });
  register("dag_recover_execution", "Reconcile one exact interrupted command only after kernel subtree extinction and native workspace settlement. Records infrastructure BLOCKED, never invented PASS. Missing proof stays blocked.", StrictObject({ runId: IdV2, itemId: IdV2, executionId: IdV2 }), async (p, c) => {
    const s = product(c), { run } = await s.bound(p.runId), execution = run.nodes[p.itemId]?.lifecycle?.executions.find(e => e.request.id === p.executionId);
    const git = run.gitOperations?.filter(op => op.itemId === p.itemId).flatMap(op => op.checks).find(request => request.id === p.executionId);
    const request = execution?.request ?? git; requireV2(request, "EXACT_EXECUTION_REQUIRED");
    await s.mutation(p.runId);
    await s.runner.reconcileExtinctCommand(request);
    if (execution) return s.runtime.recordResult(await s.mutation(p.runId), p.itemId, p.executionId, s.runner);
    return { run: (await s.read(p.runId)).run, execution: await s.runner.read(request) };
  });
  register("dag_cancel", "Fence unfinished generations before signaling exact workers. Cancellation remains nonterminal until real worker/command/Git settlement.", StrictObject({ runId: IdV2 }), (p, c) => product(c).cancel(p.runId));
  register("dag_finalize", "Reconcile a cancelling V2 run only after actual worker, execution and Git settlement; never force-clean ambiguous effects.", StrictObject({ runId: IdV2 }), (p, c) => product(c).finalize(p.runId));
  register("dag_close_git_operation", "Close an exact native Git operation after command extinction and safe original-node restoration. Retains collisions/unknown edits and blocks on ambiguity; retry closure after resolving reported drift, never edit the store. Historical sandbox operations remain closable. Consumes bounded integration retry.", StrictObject({ runId: IdV2, operationId: TextV2 }), async (p, c) => { const s = product(c); await s.git.closeOperation(await s.mutation(p.runId), p.operationId); return (await s.read(p.runId)).run; });
  register("dag_history_worker", "Read the exact generic historical worker attempt/config without scans, ingestion, ownership transfer or repair. Worker storage keeps its original version and paths.", StrictObject({ binding: WorkerBindingV2Schema }), (p) => options.workerManager.inspectBindingReadOnly(p.binding), true);
  register("dag_history_v1", "Read explicit historical V1 plan/run/worker/Git/evaluation records through original validators. Never start, attach, lease, migrate, repair or ingest.", StrictObject({ kind: Type.Union(["plan", "run", "workers", "git", "evaluation"].map(x => Type.Literal(x))), id: TextV2, revision: Type.Optional(CountV2) }), (p, c) => historicalV1(resolve(c.cwd), p, options.workerManager), true);
  pi.on("session_start", async (_e, ctx) => { widget.mount(ctx); });
  pi.on("session_shutdown", async () => { widget.dispose(); for (const service of products.values()) service.dispose(); products.clear(); });
  pi.on("before_agent_start", async (_e, ctx) => {
    const { run } = await product(ctx).read(); if (!run || ["complete", "cancelled"].includes(run.status)) return;
    return { message: { customType: "dag-v2-guidance", content: "V2 execution is bound. Use dag_next_action for exact run/item/generation/stage/completion selectors. Mutations derive lease/CAS internally. Never substitute worker reports for checks, never expand scope/effects, never use generic subagent for DAG dispatch. After asynchronous launch, end the turn when remaining work depends on completion; do not poll.", display: false } };
  });
  options.workerManager.onTerminalResult(async (event: any) => {
    const ctx = options.workerManager.context; if (!ctx) return;
    const { run } = await product(ctx).read(); if (!run) return;
    const match = Object.entries(run.nodes).find(([, n]) => n.reservation?.workerId === event.workerId || n.reservation && `v2-${canonicalHash(n.reservation.operationId).slice(7)}` === event.workerId);
    // Pre-bind completions are still delivered by the generic durable queue;
    // dag_next_action offers keyed dispatch recovery, never an unkeyed launch.
    if (match) pi.sendMessage({ customType: "dag-v2-worker-completion", content: `V2 worker settled for ${run.runId}/${match[0]}. Read dag_next_action and ingest the exact completion; this notice changes no run state.`, details: event, display: true }, { triggerTurn: true, deliverAs: "followUp" });
    await widget.refresh();
  });
  return { product, widget, async handleCommand(command: string, input: { rest: string; options: Record<string, any>; raw?: string }, ctx: any) {
    if (!["plan", "show", "run"].includes(command)) return false;
    try {
      const allowed: Record<string, string[]> = { plan: ["new", "plan"], show: ["plan", "run", "view", "node"], run: [] };
      requireV2(Object.keys(input.options).every(key => allowed[command].includes(key)), "UNSUPPORTED_DAG_OPTION: select the exact supported V2 view or use dag_history_v1");
      if (command === "plan") {
        requireV2(!/^(approve|authorize)(\s|$)/.test(input.rest), "UNSUPPORTED_PLAN_COMMAND");
        requireV2(!input.options.new || !input.options.plan, "AMBIGUOUS_PLAN_SELECTION");
        const prompt = await readFile(new URL("../command-prompts/plan.md", import.meta.url), "utf8");
        const selected = input.options.plan ? await select(ctx, String(input.options.plan)) : undefined;
        const goal = `${typeof input.options.new === "string" ? `${input.options.new} ` : ""}${input.rest}`;
        pi.sendMessage({ customType: "dag-plan-v2", content: `${prompt}\nChoose explicit workstream scope from conversation and model context; no saved focus is required.\n${selected ? renderPlanV2(selected) : "No plan selected. Create a new plan or ask for an exact existing selection; do not infer latest."}\nGoal: ${goal}`, display: true }, { triggerTurn: true, deliverAs: "followUp" });
      } else if (command === "show") {
        requireV2(!input.options.run || !input.options.plan, "AMBIGUOUS_SHOW_SELECTION");
        requireV2(!input.options.run || typeof input.options.run === "string", "EXACT_RUN_ID_REQUIRED");
        const current = !input.options.plan ? await product(ctx).read(input.options.run) : undefined;
        if (input.options.run || current?.run) {
          const { run, plan } = current!; requireV2(run && plan, "RUN_NOT_FOUND");
          const view = input.options.view ?? (input.options.node ? "node" : "status");
          const projection = ["all", "status"].includes(view) ? run : view === "node" ? { item: planView(plan, "node", input.options.node), node: run.nodes[input.options.node] }
            : { plan: planView(plan, view), runId: run.runId, status: run.status, predecessorRunId: run.predecessorRunId ?? null };
          pi.sendMessage({ customType: "dag-show-v2", content: JSON.stringify(projection, null, 2).slice(0, 48000), display: true });
        }
        else { const plan = await select(ctx, input.options.plan), view = input.options.view ?? (input.options.node ? "node" : "plan");
          const projection = planView(plan, view, input.options.node);
          pi.sendMessage({ customType: "dag-show-v2", content: (["plan", "all"].includes(view) ? renderPlanV2(plan) : JSON.stringify(projection, null, 2)).slice(0, 48000), display: true }); }
      } else {
        const raw = input.raw ?? input.rest;
        let run;
        if (!raw.trim()) {
          run = (await product(ctx).read()).run;
          if (!run) {
            const prompt = await readFile(new URL("../command-prompts/run.md", import.meta.url), "utf8");
            pi.sendMessage({ customType: "dag-run-v2", content: prompt, display: true }, { triggerTurn: true, deliverAs: "followUp" });
            return true;
          }
        }
        else { const payload = JSON.parse(raw); validateShapeV2(ProductRunV2Schema, payload); run = await start(payload, ctx); }
        pi.sendMessage({ customType: "dag-run-v2", content: `Exact V2 run ${run.runId} is ${run.status}. Acceptance observations: ${JSON.stringify(run.acceptance ?? null)}. Use conversation judgment; ask only for material unresolved concerns. Call dag_next_action when resolved. No automatic resume or scope expansion.`, display: true }, { triggerTurn: true, deliverAs: "followUp" });
      }
    } catch (error) { if (ctx.hasUI) ctx.ui.notify(String(error), "error"); else throw error; }
    return true;
  } };
}
