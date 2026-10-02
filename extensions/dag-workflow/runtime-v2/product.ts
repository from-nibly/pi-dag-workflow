import { resolve, join } from "node:path";
import { loadWorkflowConfig } from "../config.ts";
import { verificationCommandTimeoutMs } from "../command-timeout.ts";
import { lstat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { canonicalHash, parseStrictJson } from "../dag-runtime/common.ts";
import { PlanningFreshnessV2 } from "../planning/freshness-v2.ts";
import { requireV2, sameV2, type PlanV2, type PlanSelectorV2 } from "../planning/v2.ts";
import { RuntimeV2, type MutationV2 } from "./service.ts";
import { StoreV2 } from "./store.ts";
import { admissibleV2, executionRepositoryV2, runPlanV2, type LeaseV2, type RunV2, type AuthorityV2, type WorkerDirectionV2 } from "./state.ts";
import { CommandRunnerV2 } from "./command-runner.ts";
import { GitDriverV2 } from "./git-driver.ts";
import { gitOptionsV2, bindGitV2, assertTargetV2 } from "./git-native.ts";
import { stageChecksV2, currentExecutionV2 } from "./lifecycle.ts";
import { ProductWorkersV2 } from "./worker-adapter.ts";
import { workerRequestV2, evidenceExcerptV2 } from "./worker-direction.ts";
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
  async start(selection: PlanSelectorV2, authority: AuthorityV2, contextFindings: string[] = []) {
    requireV2(!this.disposed, "PRODUCT_SESSION_DISPOSED");
    const historicalBinding = join(this.root, ".ai/dag-session-bindings-v1", `${createHash("sha256").update(this.sessionId).digest("hex")}.json`);
    let historical = false;
    try { await lstat(historicalBinding); historical = true; } catch (error: any) { if (error.code !== "ENOENT") throw error; }
    requireV2(!historical, "V1_SESSION_BINDING: use a new unbound session for V2; historical authority is not adopted or continued");
    const { snapshot } = await this.read(), previous = snapshot.runs[snapshot.bindings[this.sessionId]];
    // Reopening exact frozen work is an observation, not a new start, resume or
    // freshness check against the now-landed baseline. Never expand authority.
    await this.runtime.show(selection);
    if (previous && sameV2(previous.start.selection, selection)) {
      requireV2(sameV2(previous.start.authority, authority), "BOUND_AUTHORITY_CONFLICT"); return previous;
    }
    return this.runtime.start({ intent: "run", sessionId: this.sessionId, selection, authority }, snapshot.revision, contextFindings);
  }
  private async repository(plan: PlanV2, run?: RunV2) {
    requireV2(`repo-${canonicalHash(await bindGitV2(this.root)).slice(7)}` === plan.repository.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    if (run) {
      const prefix = run.gitOperations?.filter(op => op.phase === "accepted").at(-1)?.proposal;
      if (run.acceptance) {
        const repository = executionRepositoryV2(run, plan);
        await assertTargetV2({ binding: await bindGitV2(this.root), targetRef: repository.targetBranch },
          prefix ?? { commit: repository.baselineCommit, tree: repository.baselineTree });
        return;
      }
      const current = await this.runtime.freshness.current(plan);
      requireV2(sameV2(current.source, plan.source) && sameV2(current.repository, { ...plan.repository, ...(prefix ? { baselineCommit: prefix.commit, baselineTree: prefix.tree } : {}) }), "RUN_STALE_SOURCE_OR_BASELINE");
    }
  }
  private node(run: RunV2, itemId: string, generation: number) {
    const node = run.nodes[itemId]; requireV2(node?.generation === generation, "STALE_GENERATION"); return node;
  }
  async startWork(runId: string, itemId: string, generation: number, direction?: WorkerDirectionV2) {
    let { run, plan } = await this.bound(runId), node = this.node(run, itemId, generation);
    await this.repository(plan, run);
    const mutation = await this.mutation(runId);
    ({ run, plan } = await this.bound(runId)); node = this.node(run, itemId, generation);
    requireV2(mutation.expectedRevision === run.revision, "STALE_DIRECTION_REVISION");
    if (node.reservation && direction) requireV2(sameV2((parseStrictJson(node.reservation.request) as any).direction, direction), "RESERVATION_REQUEST_CONFLICT: update direction for a new generation, not replay");
    if (!node.reservation) {
      requireV2(admissibleV2(plan, run).includes(itemId), "ITEM_NOT_ADMISSIBLE");
      const baseCommit = run.gitOperations?.filter(o => o.phase === "accepted").at(-1)?.proposal?.commit ?? executionRepositoryV2(run, plan).baselineCommit;
      const request = workerRequestV2(plan, run, itemId, baseCommit, direction ?? node.direction);
      run = await this.runtime.reserve(mutation, itemId, generation, request, direction); node = run.nodes[itemId];
    }
    return this.runtime.dispatch(await this.mutation(runId), itemId, generation, this.workers);
  }
  async completion(runId: string, itemId: string, generation: number, completionId: string) {
    let { run } = await this.bound(runId); const node = this.node(run, itemId, generation);
    requireV2(!node.reservation || node.reservation.generation === generation, "STALE_WORKER_GENERATION");
    if (node.lifecycle?.candidateReady && node.reservation?.completion?.completionId === completionId) return run;
    run = await this.runtime.recordWorkerCompletion(await this.mutation(runId), itemId, generation, completionId, b => this.workers.terminal(b, true));
    if (run.nodes[itemId].reservation!.completion!.terminalStatus !== "succeeded" || run.status !== "active") return run;
    const mutation = await this.mutation(runId);
    try {
      const candidate = await this.workers.candidate(run.nodes[itemId].reservation!, completionId);
      return await this.runtime.setCandidate(mutation, itemId, generation, candidate, this.runner);
    } catch (error) {
      await this.runtime.recordIntakeRejection(mutation, itemId, generation, completionId, String(error));
      throw error;
    }
  }
  async replaceWorker(runId: string, itemId: string, generation: number, completionId: string, direction?: WorkerDirectionV2) {
    let mutation = await this.mutation(runId);
    const { run, plan, snapshot } = await this.bound(runId), node = this.node(run, itemId, generation), reservation = node.reservation;
    requireV2(reservation?.binding, "EXACT_WORKER_BINDING_REQUIRED");
    requireV2(!node.lifecycle?.executions.some(e => !e.result), "EXECUTION_RECONCILIATION_REQUIRED");
    const frozen = parseStrictJson(reservation.request) as any;
    const currentDirection = direction ?? node.direction;
    requireV2(frozen.rendererVersion === 1 || currentDirection, "FRESH_WORKER_DIRECTION_REQUIRED: legacy envelope is archived unchanged; supply a complete current task and user-message provenance");
    const terminal = await this.workers.terminal(reservation.binding, true);
    requireV2(terminal?.completionId === completionId, "EXACT_WORKER_COMPLETION_REQUIRED");
    const settled = { ...reservation, completion: terminal };
    const evidence = await this.workers.evidence(settled);
    requireV2(evidence?.completionId === completionId, "EXACT_WORKER_COMPLETION_REQUIRED");
    // Identity and settlement failures cannot authorize fallback or consume a generation.
    const identity = await this.workers.candidateIdentity(settled, completionId, true);
    let retained = null, rejection = null, workspaceObservation: unknown = null;
    try { retained = await this.workers.candidateArtifact(identity, observed => { workspaceObservation = observed; }); }
    catch (error) { rejection = String(error).slice(0, 4000); }
    const operations = (run.gitOperations ?? []).filter(op => op.itemId === itemId && op.generation === generation);
    const results = [...(node.lifecycle?.executions ?? []), ...operations.flatMap(op => op.checks.map(request => ({ request, status: snapshot.executions?.[request.id]?.status, result: snapshot.executions?.[request.id]?.result })))].filter(e => e.request.generation === generation && e.result && e.result.disposition !== "PASS");
    const repair = { priorGeneration: generation, binding: reservation.binding, completion: terminal,
      worker: { resultPath: evidence.resultPath, resultHash: evidence.resultHash, reportStatus: evidence.reportStatus, report: evidenceExcerptV2(evidence.report, 9000), artifacts: evidenceExcerptV2(evidence.artifacts, 2000), diagnostics: evidence.diagnostics ?? null, runtime: evidenceExcerptV2(evidence.runtime, 1500) },
      intakeRejection: reservation.intakeRejection ?? null,
      work: { selectedBase: retained?.commit ?? frozen.baseCommit, inspectedCandidate: retained, rejection, workspaceObservation, preservedWorkspace: (await this.workers.manager.inspectBindingReadOnly(reservation.binding)).worker.cwd,
        disposition: retained ? "Clean eligible committed work reused for repair only; not PASS or candidateReady" : "Ineligible workspace retained untouched; repair blocked" },
      observations: results.slice(-4).map(e => ({ executionId: e.request.id, generation: e.request.generation, stage: e.request.stage, current: e.status !== "quarantined" && e.request.round === node.lifecycle?.round && currentExecutionV2(run, e.request), resultHash: canonicalHash(e.result), candidate: e.request.candidate, check: e.request.check.id, procedure: evidenceExcerptV2(e.request.check.procedure, 1000), result: { disposition: e.result!.disposition, exitCode: e.result!.exitCode, diagnostic: evidenceExcerptV2(e.result!.diagnostic, 1000), stdout: evidenceExcerptV2(e.result!.stdout, 1000), stderr: evidenceExcerptV2(e.result!.stderr, 1000), findings: evidenceExcerptV2(e.result!.findings, 500) } })), omittedResults: Math.max(0, results.length - 4),
      git: operations.slice(-2).map(op => ({ operationId: op.operationId, phase: op.phase, diagnostic: evidenceExcerptV2(op.diagnostic, 2000), observation: evidenceExcerptV2(op.observation, 1500) })),
      findings: evidenceExcerptV2(node.lifecycle?.findings, 2000) };
    requireV2(retained, `NODE_WORKSPACE_REPAIR_BLOCKED: ${rejection}; workspace retained untouched`);
    const nodeWorkspace = await this.workers.workspace(settled, node.workspace, async binding => {
      if (!node.workspace) {
        const adopted = await this.runtime.bindNodeWorkspace(mutation, itemId, generation, binding);
        mutation = { ...mutation, expectedRevision: adopted.revision };
      }
    });
    const request = JSON.stringify({ ...parseStrictJson(workerRequestV2(plan, run, itemId, retained.commit, currentDirection, repair)) as object, nodeWorkspace });
    return this.runtime.replace(mutation, itemId, generation, async r => {
      await this.workers.settled(r);
      await this.runner.inspectCleanWorkspace(retained!, nodeWorkspace.cwd);
    }, request, currentDirection);
  }
  async checks(runId: string, itemId: string, generation: number, stageAttemptId: string, signal?: AbortSignal) {
    const commandTimeoutMs = verificationCommandTimeoutMs((await loadWorkflowConfig(this.root)).verificationCommandTimeoutMs);
    let { run, plan } = await this.bound(runId); const node = this.node(run, itemId, generation), lifecycle = node.lifecycle;
    requireV2(lifecycle && node.reservation && stageAttemptId === `${node.reservation.operationId}/F${lifecycle.stage}/${lifecycle.round}`, "STALE_STAGE_ATTEMPT");
    await this.repository(plan, run);
    const stage = lifecycle.stage, round = lifecycle.round;
    const nodeWorkspace = await this.nodeWorkspace(node.reservation, node);
    const controller = new AbortController(), abort = () => controller.abort(signal?.reason);
    requireV2(!this.aborts.has(runId), "PRODUCT_OPERATION_IN_PROGRESS");
    this.aborts.set(runId, controller); signal?.addEventListener("abort", abort, { once: true }); if (signal?.aborted) abort();
    try {
      for (const check of stageChecksV2(plan, itemId, stage)) {
        run = await this.runtime.prepareCheck(await this.mutation(runId), itemId, generation, check.id, { stage, round }, nodeWorkspace, commandTimeoutMs);
        const execution = run.nodes[itemId].lifecycle!.executions.find(e => e.request.stage === stage && e.request.check.id === check.id && e.status !== "quarantined" && currentExecutionV2(run, e.request))!;
        await this.runner.ensure(execution.request, controller.signal);
        await this.runtime.recordResult(await this.mutation(runId), itemId, execution.request.id, this.runner);
      }
      return this.runtime.advanceLifecycle(await this.mutation(runId), itemId, generation, stage, round);
    } finally { signal?.removeEventListener("abort", abort); this.aborts.delete(runId); }
  }
  async integrate(runId: string, itemId: string, generation: number, candidate: CandidateV2, signal?: AbortSignal) {
    const commandTimeoutMs = verificationCommandTimeoutMs((await loadWorkflowConfig(this.root)).verificationCommandTimeoutMs);
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
    try {
      const workspace = prior ? undefined : await this.nodeWorkspace(node.reservation!, node);
      return await this.git.integrate(await this.mutation(runId), itemId, generation, candidate, signal ? AbortSignal.any([signal, controller.signal]) : controller.signal, workspace, commandTimeoutMs);
    }
    finally { this.aborts.delete(runId); }
  }
  private async nodeWorkspace(reservation: import("./state.ts").ReservationV2, node: RunV2["nodes"][string]) {
    return this.workers.workspace(reservation, node.workspace, async binding => {
      if (!node.workspace) await this.runtime.bindNodeWorkspace(await this.mutation(reservation.runId), reservation.itemId, reservation.generation, binding);
    });
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
    if (run.status === "active") {
      for (const itemId of admissibleV2(plan, run)) actions.push({ tool: "dag_start_work", runId: run.runId, itemId, generation: run.nodes[itemId].generation });
      for (const [itemId, node] of Object.entries(run.nodes)) if (node.status === "active") {
        const selectors = { runId: run.runId, itemId, generation: node.generation };
        if (node.reservation?.state !== "bound") actions.push({ tool: "dag_start_work", ...selectors });
        else if (!node.lifecycle?.candidateReady) {
          const terminal = node.reservation.binding ? await this.workers.terminal(node.reservation.binding) : null;
          if (terminal && terminal.terminalStatus === "succeeded" && !node.reservation.intakeRejection) actions.push({ tool: "dag_record_completion", ...selectors, completionId: terminal.completionId });
          else if (terminal) actions.push({ tool: "dag_replace_worker", ...selectors, completionId: terminal.completionId });
        } else if (node.lifecycle.ready) {
          const op = run.gitOperations?.find(o => o.operationId === `${node.reservation.operationId}/integration`);
          const unresolved = op?.checks.find(request => snapshot.executions?.[request.id] && !snapshot.executions[request.id].result);
          if (unresolved) actions.push({ tool: "dag_recover_execution", runId: run.runId, itemId, executionId: unresolved.id });
          else if (op?.phase === "closed") {
            if (node.reservation.completion) actions.push({ tool: "dag_replace_worker", ...selectors, completionId: node.reservation.completion.completionId });
          } else if (op && (op.phase === "blocked" || op.workspace && (op.workspace.closing || ["original", "switching", "restoring", "restored"].includes(op.workspace.phase)) && !op.landing || op.checks.some(request => snapshot.executions?.[request.id]?.result && snapshot.executions[request.id].result!.disposition !== "PASS"))) actions.push({ tool: "dag_close_git_operation", runId: run.runId, operationId: op.operationId });
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
      for (const op of run.gitOperations ?? []) if (!["accepted", "closed"].includes(op.phase)) {
        const request = op.checks.find(c => snapshot.executions?.[c.id]?.status === "running");
        if (request) actions.push({ tool: "dag_recover_execution", runId: run.runId, itemId: op.itemId, executionId: request.id });
        else actions.push({ tool: "dag_close_git_operation", runId: run.runId, operationId: op.operationId });
      }
      for (const [itemId, node] of Object.entries(run.nodes)) if (node.status === "active" && node.reservation?.binding) {
        const terminal = await this.workers.terminal(node.reservation.binding);
        if (terminal && !node.reservation.completion) actions.push({ tool: "dag_record_completion", runId: run.runId, itemId, generation: node.generation, completionId: terminal.completionId });
        else if (terminal && (terminal.terminalStatus !== "succeeded" || node.reservation.intakeRejection)) actions.push({ tool: "dag_replace_worker", runId: run.runId, itemId, generation: node.generation, completionId: terminal.completionId });
      }
    } else if (run.status === "cancelling") actions.push({ tool: "dag_cancel", runId: run.runId }, { tool: "dag_finalize", runId: run.runId });
    const selected = itemId ? actions.filter(a => !a.itemId || a.itemId === itemId) : actions;
    const attention = Object.entries(run.nodes).filter(([id]) => !itemId || id === itemId).flatMap(([id, node]) => (node.lifecycle?.findings ?? []).filter(f => f.finding.severity === "blocking" && !f.disposition).map(f => ({ itemId: id, findingId: f.finding.id, kind: f.finding.kind, materiality: f.finding.materiality, detail: f.finding.detail.slice(0, 1000), requires: "dag_disposition_finding with an explicit disposition" })));
    const intakeRejections = Object.entries(run.nodes).filter(([id, n]) => (!itemId || id === itemId) && n.reservation?.intakeRejection).map(([id, n]) => ({ itemId: id, generation: n.generation, diagnostic: n.reservation!.intakeRejection, requires: "After correcting the intake cause, explicitly retry dag_record_completion with the same exact completion while active; or dag_replace_worker with current direction. Retained work is inspected independently, never PASS" }));
    return { run: summary, intakeRejections, actions: selected.slice(0, 64), omittedActions: Math.max(0, selected.length - 64), attention: attention.slice(0, 32), omittedAttention: Math.max(0, attention.length - 32) };
  }
}
