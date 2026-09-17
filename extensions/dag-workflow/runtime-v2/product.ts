import { resolve, join } from "node:path";
import { lstat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { canonicalStringify, canonicalHash } from "../dag-runtime/common.ts";
import { PlanningFreshnessV2 } from "../planning/freshness-v2.ts";
import { requireV2, sameV2, selectorV2, type PlanV2, type PlanSelectorV2 } from "../planning/v2.ts";
import { RuntimeV2, type MutationV2 } from "./service.ts";
import { StoreV2 } from "./store.ts";
import { admissibleV2, runPlanV2, type LeaseV2, type RunV2, type AuthorityV2 } from "./state.ts";
import { CommandRunnerV2 } from "./command-runner.ts";
import { GitDriverV2 } from "./git-driver.ts";
import { gitOptionsV2, bindGitV2 } from "./git-native.ts";
import { stageChecksV2, currentExecutionV2 } from "./lifecycle.ts";
import { ProductWorkersV2 } from "./worker-adapter.ts";
import type { CandidateV2 } from "./lifecycle-schema.ts";

export class ProductV2 {
  readonly runtime: RuntimeV2;
  readonly workers: ProductWorkersV2;
  readonly runner: CommandRunnerV2;
  readonly git: GitDriverV2;
  private leases = new Map<string, LeaseV2>();
  private aborts = new Map<string, AbortController>();
  private disposed = false;
  readonly root: string;
  readonly sessionId: string;
  constructor(root: string, sessionId: string, manager: any) {
    this.root = root; this.sessionId = sessionId;
    requireV2(resolve(root) === root, "EXACT_PRODUCT_ROOT_REQUIRED");
    this.runtime = new RuntimeV2(new StoreV2(root), new PlanningFreshnessV2(root));
    this.runner = new CommandRunnerV2(this.runtime.store, root, new Map(), "node-local", gitOptionsV2);
    this.workers = new ProductWorkersV2(manager, root, this.runner);
    this.git = new GitDriverV2(this.runtime, root);
  }
  async read(runId?: string) {
    const snapshot = await this.runtime.store.read(), id = runId ?? snapshot.bindings[this.sessionId];
    const run = id ? snapshot.runs[id] : undefined;
    requireV2(!id || run, "RUN_NOT_FOUND");
    return { snapshot, run, plan: run ? runPlanV2(snapshot, run) : undefined };
  }
  async bound(runId: string) {
    const value = await this.read(runId);
    requireV2(value.run && value.snapshot.bindings[this.sessionId] === runId && value.run.start.sessionId === this.sessionId, "EXACT_SESSION_RUN_REQUIRED");
    return { ...value, run: value.run, plan: value.plan! };
  }
  dispose() { this.disposed = true; for (const controller of this.aborts.values()) controller.abort(); }
  async mutation(runId: string): Promise<MutationV2> {
    requireV2(!this.disposed, "PRODUCT_SESSION_DISPOSED");
    let { run } = await this.bound(runId);
    let lease = this.leases.get(runId);
    if (!lease) { run = await this.runtime.acquireLease(runId, this.sessionId, run.revision); lease = run.lease!; this.leases.set(runId, lease); }
    requireV2(sameV2(run.lease, lease), "STALE_PRODUCT_LEASE");
    return { runId, expectedRevision: run.revision, lease };
  }
  async start(selection: PlanSelectorV2, authority: AuthorityV2) {
    requireV2(!this.disposed, "PRODUCT_SESSION_DISPOSED");
    const historicalBinding = join(this.root, ".ai/dag-session-bindings-v1", `${createHash("sha256").update(this.sessionId).digest("hex")}.json`);
    let historical = false;
    try { await lstat(historicalBinding); historical = true; } catch (error: any) { if (error.code !== "ENOENT") throw error; }
    requireV2(!historical, "V1_SESSION_BINDING: use a new unbound session for V2; historical authority is not adopted or continued");
    const { snapshot } = await this.read(), previous = snapshot.runs[snapshot.bindings[this.sessionId]];
    // Reopening exact frozen work is an observation, not a new start, resume or
    // freshness check against the now-landed baseline. Never expand authority.
    const head = snapshot.plans[selection.planId]?.at(-1);
    requireV2(head && sameV2(selectorV2(head), selection), "PLAN_SELECTION_STALE_OR_MISSING");
    if (previous && sameV2(previous.start.selection, selection)) {
      requireV2(sameV2(previous.start.authority, authority), "BOUND_AUTHORITY_CONFLICT"); return previous;
    }
    return this.runtime.start({ intent: "run", sessionId: this.sessionId, selection, authority }, snapshot.revision);
  }
  private async repository(plan: PlanV2, run?: RunV2) {
    requireV2(`repo-${canonicalHash(await bindGitV2(this.root)).slice(7)}` === plan.repository.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    if (run) {
      const prefix = run.gitOperations?.filter(op => op.phase === "accepted").at(-1)?.proposal;
      const current = await this.runtime.freshness.current(plan);
      requireV2(sameV2(current.source, plan.source) && sameV2(current.repository, { ...plan.repository, ...(prefix ? { baselineCommit: prefix.commit, baselineTree: prefix.tree } : {}) }), "RUN_STALE_SOURCE_OR_BASELINE");
    }
  }
  private node(run: RunV2, itemId: string, generation: number) {
    const node = run.nodes[itemId]; requireV2(node?.generation === generation, "STALE_GENERATION"); return node;
  }
  async startWork(runId: string, itemId: string, generation: number) {
    let { run, plan } = await this.bound(runId), node = this.node(run, itemId, generation);
    await this.repository(plan, run);
    if (!node.reservation) {
      requireV2(admissibleV2(plan, run).includes(itemId), "ITEM_NOT_ADMISSIBLE");
      const item = plan.workItems.find(n => n.id === itemId)!;
      const baseCommit = run.gitOperations?.filter(o => o.phase === "accepted").at(-1)?.proposal?.commit ?? plan.repository.baselineCommit;
      const task = ["Implement only this bounded work item in the supplied detached worktree. Commit the candidate; do not merge into the target branch.",
        "Repository-local effects only. Do not publish, push, deploy, install dependencies, use credentials, or alter runtime/model authority. Do not delegate or orchestrate. End with subagent_report; completion is not verification PASS.",
        `Plan: ${canonicalStringify(selectorV2(plan))}`, `Source: ${canonicalStringify(plan.source)}`, `Item: ${canonicalStringify(item)}`].join("\n");
      const sourcePaths = ["project-model/model.json", ...plan.source.refs.filter(r => r.ref.startsWith("spec:")).map(r => r.ref.slice(5))];
      const request = canonicalStringify({ kind: "product_worker_v2", explicitDispatchRecovery: true, repositoryId: plan.repository.repositoryId, baseCommit, sourcePaths, task });
      run = await this.runtime.reserve(await this.mutation(runId), itemId, generation, request); node = run.nodes[itemId];
    }
    return this.runtime.dispatch(await this.mutation(runId), itemId, generation, this.workers);
  }
  async completion(runId: string, itemId: string, generation: number, completionId: string) {
    let { run } = await this.bound(runId); const node = this.node(run, itemId, generation);
    requireV2(!node.reservation || node.reservation.generation === generation, "STALE_WORKER_GENERATION");
    if (node.lifecycle?.candidateReady && node.reservation?.completion?.completionId === completionId) return run;
    run = await this.runtime.recordWorkerCompletion(await this.mutation(runId), itemId, generation, completionId, b => this.workers.terminal(b, true));
    if (run.nodes[itemId].reservation!.completion!.terminalStatus !== "succeeded" || run.status !== "active") return run;
    const candidate = await this.workers.candidate(run.nodes[itemId].reservation!, completionId);
    return this.runtime.setCandidate(await this.mutation(runId), itemId, generation, candidate, this.runner);
  }
  async checks(runId: string, itemId: string, generation: number, stageAttemptId: string, signal?: AbortSignal) {
    let { run, plan } = await this.bound(runId); const node = this.node(run, itemId, generation), lifecycle = node.lifecycle;
    requireV2(lifecycle && node.reservation && stageAttemptId === `${node.reservation.operationId}/F${lifecycle.stage}/${lifecycle.round}`, "STALE_STAGE_ATTEMPT");
    await this.repository(plan, run);
    const stage = lifecycle.stage, round = lifecycle.round;
    const controller = new AbortController(), abort = () => controller.abort(signal?.reason);
    requireV2(!this.aborts.has(runId), "PRODUCT_OPERATION_IN_PROGRESS");
    this.aborts.set(runId, controller); signal?.addEventListener("abort", abort, { once: true }); if (signal?.aborted) abort();
    try {
      for (const check of stageChecksV2(plan, itemId, stage)) {
        run = await this.runtime.prepareCheck(await this.mutation(runId), itemId, generation, check.id, { stage, round });
        const execution = run.nodes[itemId].lifecycle!.executions.find(e => e.request.stage === stage && e.request.check.id === check.id && e.status !== "quarantined" && currentExecutionV2(run, e.request))!;
        await this.runner.ensure(execution.request, controller.signal);
        await this.runtime.recordResult(await this.mutation(runId), itemId, execution.request.id, this.runner);
      }
      return this.runtime.advanceLifecycle(await this.mutation(runId), itemId, generation, stage, round);
    } finally { signal?.removeEventListener("abort", abort); this.aborts.delete(runId); }
  }
  async integrate(runId: string, itemId: string, generation: number, candidate: CandidateV2, signal?: AbortSignal) {
    const { run, plan } = await this.bound(runId); const node = this.node(run, itemId, generation);
    await this.repository(plan);
    requireV2(node.lifecycle?.candidateReady && sameV2(node.lifecycle.candidate, candidate), "STALE_CANDIDATE");
    if (node.integration) { requireV2(sameV2(node.integration.candidate, candidate), "INTEGRATION_REQUEST_CONFLICT"); return run; }
    const prior = run.gitOperations?.find(op => op.operationId === `${runId}/${itemId}/${generation}/integration`);
    // Once a landing was attempted, native old/new/third reconciliation owns the
    // baseline observation. Before that boundary, new governing changes block.
    if (!prior?.landing) await this.repository(plan, run);
    const controller = new AbortController(); requireV2(!this.aborts.has(runId), "PRODUCT_OPERATION_IN_PROGRESS");
    this.aborts.set(runId, controller);
    try { return await this.git.integrate(await this.mutation(runId), itemId, generation, candidate, signal ? AbortSignal.any([signal, controller.signal]) : controller.signal); }
    finally { this.aborts.delete(runId); }
  }
  async cancel(runId: string) {
    const existing = (await this.bound(runId)).run; if (existing.status === "cancelled") return existing;
    const run = await this.runtime.cancel(await this.mutation(runId)); this.aborts.get(runId)?.abort();
    const attempts = Object.values(run.nodes).filter(n => n.status === "cancelled" && n.reservation);
    const signals = await Promise.allSettled(attempts.map(n => this.workers.cancel(n.reservation!)));
    const failures = signals.flatMap((r, i) => r.status === "rejected" ? [`${attempts[i].reservation!.operationId}: ${String(r.reason).slice(0, 1000)}`] : []);
    requireV2(!failures.length, `CANCELLATION_RECONCILIATION_REQUIRED: ${failures.slice(0, 8).join("; ")}`);
    return run;
  }
  async finalize(runId: string) {
    let { run } = await this.bound(runId);
    if (run.status === "cancelled") return run;
    requireV2(run.status === "cancelling", "CANCELLATION_REQUIRED");
    await this.mutation(runId);
    for (const [id, node] of Object.entries(run.nodes)) for (const e of node.lifecycle?.executions ?? []) if (!e.result) {
      const job = (await this.runtime.store.read()).executions?.[e.request.id];
      if (!job) await this.runner.reconcileUnlaunched(e.request);
      else requireV2(job.result, "EXECUTION_SETTLEMENT_REQUIRED: use dag_recover_execution after owner exit and proven command extinction");
      await this.runtime.recordResult(await this.mutation(runId), id, e.request.id, this.runner);
    }
    run = (await this.bound(runId)).run;
    for (const op of run.gitOperations ?? []) if (!["accepted", "closed"].includes(op.phase)) await this.git.closeOperation(await this.mutation(runId), op.operationId);
    return this.runtime.reconcileCancellation(await this.mutation(runId), async current => {
      for (const node of Object.values(current.nodes)) if (node.reservation) await this.workers.settled(node.reservation);
    });
  }
  async next(runId?: string, itemId?: string) {
    const { snapshot, run, plan } = await this.read(runId); if (!run || !plan) return { run: null, actions: [] };
    requireV2(!itemId || run.nodes[itemId], "ITEM_NOT_FOUND");
    const summary = { runId: run.runId, revision: run.revision, status: run.status, selection: run.start.selection };
    if (run.start.sessionId !== this.sessionId || snapshot.bindings[this.sessionId] !== run.runId) return { run: summary, actions: [], diagnostic: "READ_ONLY_UNBOUND_RUN" };
    const actions: any[] = [];
    if (run.status === "active" && run.start.authority.expiresAt <= Date.now()) return { run: summary, actions, diagnostic: "AUTHORITY_EXPIRED: no new dispatch; cancellation remains available" };
    if (run.status === "active") {
      for (const itemId of admissibleV2(plan, run)) actions.push({ tool: "dag_start_work", runId: run.runId, itemId, generation: run.nodes[itemId].generation });
      for (const [itemId, node] of Object.entries(run.nodes)) if (node.status === "active") {
        const selectors = { runId: run.runId, itemId, generation: node.generation };
        if (node.reservation?.state !== "bound") actions.push({ tool: "dag_start_work", ...selectors });
        else if (!node.lifecycle?.candidateReady) {
          const terminal = node.reservation.binding ? await this.workers.terminal(node.reservation.binding) : null;
          if (terminal && terminal.terminalStatus === "succeeded") actions.push({ tool: "dag_record_completion", ...selectors, completionId: terminal.completionId });
          else if (terminal && (node.retries ?? []).filter(r => r.dimension === "replacement").reduce((sum, r) => sum + r.count, 0) < 2) actions.push({ tool: "dag_replace_worker", ...selectors, completionId: terminal.completionId });
        } else if (node.lifecycle.ready) {
          const op = run.gitOperations?.find(o => o.operationId === `${node.reservation.operationId}/integration`);
          const unresolved = op?.checks.find(request => snapshot.executions?.[request.id] && !snapshot.executions[request.id].result);
          if (unresolved) actions.push({ tool: "dag_recover_execution", runId: run.runId, itemId, executionId: unresolved.id });
          else if (op?.phase === "closed") {
            if (node.reservation.completion && (node.retries ?? []).filter(r => r.dimension === "replacement").reduce((sum, r) => sum + r.count, 0) < 2) actions.push({ tool: "dag_replace_worker", ...selectors, completionId: node.reservation.completion.completionId });
          } else if (op && (op.phase === "blocked" || op.checks.some(request => snapshot.executions?.[request.id]?.result && snapshot.executions[request.id].result!.disposition !== "PASS"))) actions.push({ tool: "dag_close_git_operation", runId: run.runId, operationId: op.operationId });
          else actions.push({ tool: "dag_integrate", ...selectors, candidate: node.lifecycle.candidate });
        }
        else if (!node.lifecycle.stop) {
          const unresolved = node.lifecycle.executions.find(e => e.status !== "quarantined" && currentExecutionV2(run, e.request) && snapshot.executions?.[e.request.id] && !snapshot.executions[e.request.id].result);
          const failed = node.lifecycle.executions.find(e => e.status === "observed" && currentExecutionV2(run, e.request) && e.result?.disposition !== "PASS");
          if (unresolved) actions.push({ tool: "dag_recover_execution", runId: run.runId, itemId, executionId: unresolved.request.id });
          else if (failed) actions.push({ tool: "dag_retry", ...selectors, executionId: failed.request.id });
          else if (!node.lifecycle.findings.some(f => f.finding.severity === "blocking" && !f.disposition)) actions.push({ tool: "dag_run_checks", ...selectors, stageAttemptId: `${node.reservation.operationId}/F${node.lifecycle.stage}/${node.lifecycle.round}` });
        }
      }
    } else if (run.status === "paused") {
      for (const [itemId, node] of Object.entries(run.nodes)) if (node.status === "active" && node.reservation?.binding && !node.reservation.completion) {
        const terminal = await this.workers.terminal(node.reservation.binding);
        if (terminal) actions.push({ tool: "dag_record_completion", runId: run.runId, itemId, generation: node.generation, completionId: terminal.completionId });
      }
    } else if (run.status === "cancelling") actions.push({ tool: "dag_cancel", runId: run.runId }, { tool: "dag_finalize", runId: run.runId });
    const selected = itemId ? actions.filter(a => !a.itemId || a.itemId === itemId) : actions;
    const attention = Object.entries(run.nodes).filter(([id]) => !itemId || id === itemId).flatMap(([id, node]) => (node.lifecycle?.findings ?? []).filter(f => f.finding.severity === "blocking" && !f.disposition).map(f => ({ itemId: id, findingId: f.finding.id, kind: f.finding.kind, materiality: f.finding.materiality, detail: f.finding.detail.slice(0, 1000), requires: "dag_disposition_finding with an explicit disposition" })));
    return { run: summary, actions: selected.slice(0, 64), omittedActions: Math.max(0, selected.length - 64), attention: attention.slice(0, 32), omittedAttention: Math.max(0, attention.length - 32) };
  }
}
