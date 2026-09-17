import { randomUUID } from "node:crypto";
import { createPlanV2, parsePlanV2, PlanInputV2Schema, PlanSelectorV2Schema, requireV2, sameV2, validateShapeV2, type PlanInputV2, type PlanSelectorV2, type PlanV2 } from "../planning/v2.ts";
import { admissibleV2, assertScopeV2, IntegrationV2Schema, StartV2Schema, runPlanV2, type IntegrationV2, type LeaseV2, type ReservationV2, type RunV2, type SnapshotV2, type StartV2, type WorkerBindingV2 } from "./state.ts";
import { processIdentityV2, StoreV2 } from "./store.ts";
import { CandidateV2Schema, ExecutionResultV2Schema, type CandidateV2, type RetryDimensionV2 } from "./lifecycle-schema.ts";
import { assertReadyV2, assertStageV2, auditResultV2, consumeRetryV2, contextReusedV2, rejectedContextResultV2, currentExecutionV2, executionRequestV2, frameV2, invalidateLifecycleV2, stageChecksV2 } from "./lifecycle.ts";
import type { CandidateInspectorV2, ResultsV2 } from "./command-runner.ts";
import { unresolvedGitV2 } from "./git-state.ts";

/** Hydrate from the repository/model, not from the submitted plan. Called inside
 * the start consistency guard; N05 owns concrete product adapters. */
export interface FreshnessV2 { current(plan: Readonly<PlanV2>): Promise<Pick<PlanV2, "repository" | "source">> }
/** The generic worker manager must durably key creation by operationId and compare
 * the entire request. Repeated ensure after acknowledgement loss must return the
 * same worker, never create a second process. No caller-generated action ID. */
export interface WorkersV2 { ensure(reservation: Readonly<ReservationV2>): Promise<{ workerId: string; binding?: WorkerBindingV2 }> }
/** Native Git boundary (N04): independently hydrate the reconciled landing.
 * Internal lifecycle checks run first; a worker claim is not landing evidence. */
export interface IntegrationsV2 { verify(run: Readonly<RunV2>, plan: Readonly<PlanV2>, integration: Readonly<IntegrationV2>): Promise<void> }
export interface MutationV2 { runId: string; expectedRevision: number; lease: LeaseV2 }
export class RuntimeV2 {
  readonly store: StoreV2;
  readonly freshness: FreshnessV2;
  readonly now: () => number;
  constructor(store: StoreV2, freshness: FreshnessV2, now: () => number = Date.now) { this.store = store; this.freshness = freshness; this.now = now; }

  async save(input: PlanInputV2, expectedStoreRevision: number): Promise<PlanV2> {
    validateShapeV2(PlanInputV2Schema, input);
    return this.store.transaction(async (s, publish) => {
      this.cas(s.revision, expectedStoreRevision);
      const previous = s.plans[input.planId]?.at(-1);
      if (previous) {
        const { kind: _, schemaVersion: __, revision: ___, planHash: ____, ...content } = previous;
        if (sameV2(content, input)) return previous;
      }
      const plan = createPlanV2(input, (previous?.revision ?? 0) + 1);
      if (plan.predecessor) this.select(s, plan.predecessor, false);
      (s.plans[plan.planId] ??= []).push(plan); await publish(); return plan;
    });
  }
  async show(selection?: PlanSelectorV2): Promise<PlanV2> {
    const s = await this.store.read();
    if (selection) return this.select(s, selection, false);
    const plans = Object.values(s.plans).map(p => p.at(-1)!);
    requireV2(plans.length === 1, `PLAN_SELECTION_REQUIRED: ${plans.slice(0, 8).map(p => `${p.planId}@${p.revision}`).join(", ")}`);
    return plans[0];
  }
  async start(request: StartV2, expectedStoreRevision: number): Promise<RunV2> {
    validateShapeV2(StartV2Schema, request);
    return this.store.transaction(async (s, publish) => {
      this.cas(s.revision, expectedStoreRevision);
      const plan = this.select(s, request.selection, true);
      assertScopeV2(plan, request.authority, this.now());
      requireV2(sameV2(await this.freshness.current(structuredClone(plan)), { repository: plan.repository, source: plan.source }), "PLAN_STALE_SOURCE_OR_BASELINE");
      const previous = s.runs[s.bindings[request.sessionId]];
      if (previous && sameV2(previous.start, request)) return previous;
      if (previous) {
        requireV2(["complete", "cancelled"].includes(previous.status), "ACTIVE_BINDING_CONFLICT");
        requireV2(plan.predecessor && sameV2(plan.predecessor, previous.start.selection), "SUCCESSOR_PREDECESSOR_REQUIRED");
      }
      const run: RunV2 = { kind: "dag_run_v2", schemaVersion: 2, runId: `run-${randomUUID()}`, revision: 0, start: structuredClone(request), status: "active", releasedGates: [],
        nodes: Object.fromEntries(plan.workItems.map(n => [n.id, { generation: 1, status: request.authority.scope.includes(n.id) ? "pending" : "excluded" }])) };
      if (previous) run.predecessorRunId = previous.runId;
      s.runs[run.runId] = run; s.bindings[request.sessionId] = run.runId;
      // Start intent, run creation and binding are one durable publication.
      await publish(); return run;
    });
  }
  async acquireLease(runId: string, sessionId: string, expectedRevision: number): Promise<RunV2> {
    return this.store.transaction(async (s, publish) => {
      const run = this.run(s, runId); this.cas(run.revision, expectedRevision);
      requireV2(!["complete", "cancelled"].includes(run.status) && s.bindings[sessionId] === runId && run.start.sessionId === sessionId, "LEASE_BINDING_CONFLICT");
      const identity = await processIdentityV2(); requireV2(identity, "PROCESS_IDENTITY_UNAVAILABLE");
      if (run.lease) {
        const observed = await processIdentityV2(run.lease.pid);
        const sameManager = run.lease.pid === process.pid && run.lease.processStart === identity && run.lease.sessionId === sessionId;
        requireV2(sameManager || observed !== run.lease.processStart, "LIVE_LEASE_CONFLICT");
      }
      run.lease = { sessionId, pid: process.pid, processStart: identity, generation: (run.lease?.generation ?? 0) + 1 };
      run.revision++; await publish(); return run;
    });
  }
  async frontier(runId: string): Promise<string[]> {
    const s = await this.store.read(), r = this.run(s, runId);
    assertScopeV2(runPlanV2(s, r), r.start.authority, this.now()); return admissibleV2(runPlanV2(s, r), r);
  }
  async reserve(mutation: MutationV2, itemId: string, generation: number, request: string): Promise<RunV2> {
    requireV2(typeof request === "string" && request.length > 0 && request.length <= 65536, "INVALID_WORKER_REQUEST");
    return this.change(mutation, async (run, plan) => {
      this.dispatchGuard(run, plan);
      const node = this.node(run, itemId, generation);
      if (node.reservation) { requireV2(node.reservation.request === request, "RESERVATION_REQUEST_CONFLICT"); return false; }
      requireV2(admissibleV2(plan, run).includes(itemId), "ITEM_NOT_ADMISSIBLE");
      node.status = "active";
      node.reservation = { operationId: `${run.runId}/${itemId}/${generation}`, runId: run.runId, itemId, generation, request, state: "reserved" };
      // F0 precedes implementation dispatch. Start already hydrated the immutable
      // baseline/source; a worker may author only after this computed frame exists.
      node.lifecycle = frameV2(run, plan, itemId, { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree }, this.now(), false);
      return true;
    });
  }
  async dispatch(m: MutationV2, itemId: string, generation: number, workers: WorkersV2): Promise<RunV2> {
    // Hold the OS lock through launch acknowledgement. The durable natural slot
    // permits keyed recovery if the parent dies after launch but before binding.
    return this.store.transaction(async (s, publish) => {
      const run = await this.guard(s, m), plan = runPlanV2(s, run); this.dispatchGuard(run, plan);
      const node = this.node(run, itemId, generation), reservation = node.reservation;
      requireV2(node.status === "active" && reservation, "RESERVATION_REQUIRED");
      requireV2(node.lifecycle?.passed.includes(0) && !node.lifecycle.stop, "F0_PREFLIGHT_REQUIRED");
      if (reservation.state === "bound") return run;
      if (reservation.state === "reserved") { reservation.state = "dispatching"; run.revision++; await publish(); }
      const worker = await workers.ensure(structuredClone(reservation));
      requireV2(typeof worker.workerId === "string" && worker.workerId.length > 0 && worker.workerId.length <= 65536, "INVALID_WORKER_BINDING");
      reservation.workerId = worker.workerId; reservation.state = "bound";
      if (worker.binding) reservation.binding = structuredClone(worker.binding);
      run.revision++; await publish(); return run;
    });
  }
  /** Recover a lost dispatch acknowledgement by read-only exact manager lookup.
   * Unlike dispatch, this may bind an already-fenced generation but cannot launch. */
  async recoverWorkerBinding(m: MutationV2, itemId: string, generation: number,
    read: (reservation: Readonly<ReservationV2>) => Promise<WorkerBindingV2>): Promise<RunV2> {
    return this.change(m, async run => {
      const node = run.nodes[itemId], reservation = node?.reservation;
      requireV2(reservation && reservation.generation === generation && ["active", "cancelled"].includes(node.status), "EXACT_RESERVATION_REQUIRED");
      if (reservation.state === "bound") return false;
      requireV2(reservation.state === "dispatching", "DISPATCH_INTENT_REQUIRED");
      const binding = await read(structuredClone(reservation));
      reservation.workerId = binding.workerId; reservation.binding = structuredClone(binding); reservation.state = "bound"; return true;
    });
  }
  async recordWorkerCompletion(m: MutationV2, itemId: string, generation: number, completionId: string,
    read: (binding: Readonly<WorkerBindingV2>) => Promise<{ completionId: string; terminalStatus: string } | null>): Promise<RunV2> {
    return this.change(m, async run => {
      const node = this.node(run, itemId, generation), reservation = node.reservation;
      requireV2(node.status === "active" && reservation?.binding, "EXACT_WORKER_BINDING_REQUIRED");
      const terminal = await read(structuredClone(reservation.binding));
      requireV2(terminal && terminal.completionId === completionId, "EXACT_WORKER_COMPLETION_REQUIRED");
      if (reservation.completion) { requireV2(sameV2(reservation.completion, terminal), "WORKER_COMPLETION_CONFLICT"); return false; }
      reservation.completion = structuredClone(terminal); return true;
    });
  }
  async integrate(m: MutationV2, evidence: IntegrationV2, integrations: IntegrationsV2): Promise<RunV2> {
    validateShapeV2(IntegrationV2Schema, evidence);
    return this.change(m, async (run, plan) => {
      requireV2(evidence.runId === run.runId, "INTEGRATION_RUN_MISMATCH");
      const node = this.node(run, evidence.itemId, evidence.generation);
      if (node.integration) { requireV2(sameV2(node.integration, evidence), "INTEGRATION_REQUEST_CONFLICT"); return false; }
      this.dispatchGuard(run, plan);
      requireV2(node.status === "active" && node.reservation?.state === "bound", "BOUND_WORKER_REQUIRED");
      requireV2(evidence.operationId === `${node.reservation.operationId}/integration`, "INTEGRATION_OPERATION_MISMATCH");
      if (plan.integration.strategy === "serial") {
        const index = plan.workItems.findIndex(n => n.id === evidence.itemId);
        requireV2(plan.workItems.slice(0, index).every(n => run.nodes[n.id].status === "complete"), "INTEGRATION_PREFIX_NOT_COMPLETE");
      }
      assertReadyV2(run, plan, evidence.itemId, evidence.candidate);
      await integrations.verify(structuredClone(run), structuredClone(plan), structuredClone(evidence));
      const operation = run.gitOperations?.find(op => op.operationId === evidence.operationId);
      if (operation) { requireV2(operation.phase === "landed" && sameV2(operation.proposal, evidence.target), "GIT_LANDING_NOT_RECONCILED"); operation.phase = "accepted"; }
      node.integration = structuredClone(evidence); node.status = "complete";
      if (Object.values(run.nodes).every(n => ["complete", "excluded"].includes(n.status))) run.status = "complete";
      // No initializing fallback: a completed producer immediately releases its
      // consumer after persisted reload, even with zero currently active nodes.
      return true;
    });
  }
  async control(m: MutationV2, action: "pause" | "resume" | "needs_replan", disposition?: string): Promise<RunV2> {
    requireV2(["pause", "resume", "needs_replan"].includes(action), "INVALID_CONTROL");
    return this.change(m, async run => {
      requireV2(!["complete", "cancelling", "cancelled"].includes(run.status), "RUN_TERMINAL_OR_CANCELLING");
      if (run.status === "needs_replan" && action === "pause") return false;
      if (run.status === "needs_replan" && action === "resume") {
        requireV2(disposition?.trim(), "REPLAN_DISPOSITION_REQUIRED");
        requireV2(Object.values(run.nodes).every(n => !n.lifecycle?.findings.some(f => f.finding.severity === "blocking" && f.finding.materiality === "plan_affecting" && !f.disposition)), "PLAN_FINDING_DISPOSITION_REQUIRED");
      }
      const status = action === "pause" ? "paused" : action === "resume" ? "active" : "needs_replan";
      if (status === run.status) return false;
      if (run.status === "needs_replan" && action === "resume") run.replanDisposition = disposition!;
      run.status = status; return true;
    });
  }
  /** Generation replacement only after the worker adapter proves the old natural
   * operation settled, within retained retry limits and evidence invalidation. */
  async replace(m: MutationV2, itemId: string, generation: number, settled: (reservation: Readonly<ReservationV2>) => Promise<void>, request?: string): Promise<RunV2> {
    if (request !== undefined) requireV2(typeof request === "string" && request.length > 0 && request.length <= 65536, "INVALID_WORKER_REQUEST");
    return this.change(m, async run => {
      requireV2(!["complete", "cancelling", "cancelled"].includes(run.status), "RUN_TERMINAL_OR_CANCELLING");
      const node = this.node(run, itemId, generation);
      requireV2(node.status === "active" && node.reservation, "ACTIVE_RESERVATION_REQUIRED");
      requireV2(!unresolvedGitV2(run, itemId), "UNRESOLVED_GIT_OPERATION");
      requireV2(!node.lifecycle?.executions.some(e => !e.result), "EXECUTION_RECONCILIATION_REQUIRED");
      await settled(structuredClone(node.reservation));
      consumeRetryV2(run, itemId, "replacement", 0, "worker", "replacement");
      invalidateLifecycleV2(run, itemId, "worker replacement");
      if (node.lifecycle) node.lifecycle.candidateReady = false;
      node.generation++;
      node.reservation = { ...node.reservation, ...(request === undefined ? {} : { request }), operationId: `${run.runId}/${itemId}/${node.generation}`, generation: node.generation, state: "reserved" };
      delete node.reservation.workerId; delete node.reservation.binding; delete node.reservation.completion;
      return true;
    });
  }
  /** Fence every unfinished generation durably BEFORE the caller signals workers.
   * A cancelling binding is still active until all worker/effect ambiguity settles. */
  async cancel(m: MutationV2): Promise<RunV2> {
    return this.change(m, async run => {
      if (["cancelling", "cancelled"].includes(run.status)) return false;
      requireV2(run.status !== "complete", "RUN_TERMINAL");
      for (const [itemId, node] of Object.entries(run.nodes)) if (["pending", "active"].includes(node.status)) {
        invalidateLifecycleV2(run, itemId, "cancelled generation"); node.generation++; node.status = "cancelled";
      }
      run.status = "cancelling"; return true;
    });
  }
  async reconcileCancellation(m: MutationV2, settled: (run: Readonly<RunV2>) => Promise<void>): Promise<RunV2> {
    return this.change(m, async run => {
      if (run.status === "cancelled") return false;
      requireV2(run.status === "cancelling", "CANCELLATION_REQUIRED");
      requireV2(Object.values(run.nodes).every(n => !n.lifecycle?.executions.some(e => !e.result)), "EXECUTION_RECONCILIATION_REQUIRED");
      requireV2(!unresolvedGitV2(run), "UNRESOLVED_GIT_OPERATION");
      await settled(structuredClone(run)); run.status = "cancelled"; return true;
    });
  }
  async releaseGate(m: MutationV2, gate: string, verify: () => Promise<void>): Promise<RunV2> {
    return this.change(m, async (run, plan) => {
      this.dispatchGuard(run, plan); requireV2(plan.constraints.gates.includes(gate), "UNKNOWN_GATE");
      if (run.releasedGates.includes(gate)) return false;
      await verify(); run.releasedGates.push(gate); return true;
    });
  }
  /** Bind the actual F1 candidate after the pre-dispatch F0 frame. Subsequent
   * candidate changes require fresh affected evidence, never a worker PASS. */
  async setCandidate(m: MutationV2, itemId: string, generation: number, candidate: CandidateV2, inspector: CandidateInspectorV2): Promise<RunV2> {
    validateShapeV2(CandidateV2Schema, candidate);
    return this.change(m, async (run, plan) => {
      this.dispatchGuard(run, plan); const node = this.node(run, itemId, generation);
      requireV2(node.status === "active" && node.reservation?.state === "bound", "BOUND_WORKER_REQUIRED");
      requireV2(!node.lifecycle?.stop, "LIFECYCLE_RETRY_STOP");
      if (node.lifecycle?.candidateReady && sameV2(node.lifecycle.candidate, candidate)) return false;
      requireV2(!unresolvedGitV2(run, itemId), "UNRESOLVED_GIT_OPERATION");
      await inspector.inspect(structuredClone(candidate));
      if (node.lifecycle && candidate.tree !== node.lifecycle.candidate.tree && node.lifecycle.candidates.some(c => c.tree === candidate.tree)) {
        node.lifecycle.stop = "NO_PROGRESS: recurring candidate tree";
        node.lifecycle.ready = false; node.lifecycle.passed = node.lifecycle.passed.filter(s => s < 8); return true;
      }
      if (node.lifecycle?.candidateReady) {
        consumeRetryV2(run, itemId, "product", 1, "candidate", "candidate-change");
        invalidateLifecycleV2(run, itemId, "candidate changed");
      }
      node.lifecycle = frameV2(run, plan, itemId, candidate, this.now()); return true;
    });
  }
  async prepareCheck(m: MutationV2, itemId: string, generation: number, checkId: string): Promise<RunV2> {
    return this.change(m, async (run, plan) => {
      this.dispatchGuard(run, plan); const node = this.node(run, itemId, generation), l = node.lifecycle;
      requireV2(node.status === "active" && l?.candidateReady && !l.stop, "LIFECYCLE_NOT_EXECUTABLE");
      const check = stageChecksV2(plan, itemId, l.stage).find(c => c.id === checkId);
      requireV2(check, "CHECK_NOT_APPLICABLE_TO_STAGE");
      if (l.executions.some(e => e.request.stage === l.stage && e.request.check.id === checkId && e.status !== "quarantined" && currentExecutionV2(run, e.request))) return false;
      l.executions.push({ request: executionRequestV2(run, itemId, check), status: "intent" }); return true;
    });
  }
  /** Hydrate only a durable execution result from a trusted executor. There is no
   * public method accepting a worker completion string or aggregate PASS. */
  async recordResult(m: MutationV2, itemId: string, executionId: string, results: ResultsV2): Promise<RunV2> {
    return this.change(m, async (run, _plan, snapshot) => {
      const l = run.nodes[itemId]?.lifecycle, execution = l?.executions.find(e => e.request.id === executionId);
      requireV2(l && execution, "EXECUTION_NOT_FOUND");
      const result = await results.read(structuredClone(execution.request));
      requireV2(result, "EXECUTION_RESULT_NOT_DURABLE"); validateShapeV2(ExecutionResultV2Schema, result); auditResultV2(execution.request, result);
      if (execution.result) { requireV2(sameV2(execution.contextRejection?.observed ?? execution.result, result), "EXECUTION_RESULT_CONFLICT"); return false; }
      const reused = contextReusedV2(run, result), job = snapshot.executions?.[executionId];
      // A reader cannot replace the concrete executor's durable observation.
      requireV2(!job || (job.status === "settled" && sameV2(job.result, result)), `EXECUTOR_RESULT_MISMATCH${reused ? ": EVALUATOR_CONTEXT_REUSED_ACROSS_ITEMS" : ""}`);
      // Cross-execution acceptance is atomic with publication, not the earlier
      // executor settlement. Preserve its exact observation without accepting
      // an invalid identity or wedging cancellation on a physically settled job.
      if (reused) {
        execution.contextRejection = { reason: "EVALUATOR_CONTEXT_REUSED", observed: structuredClone(result) };
        execution.result = rejectedContextResultV2(result);
      } else execution.result = structuredClone(result);
      if (!currentExecutionV2(run, execution.request) || execution.status === "quarantined") {
        execution.status = "quarantined"; execution.quarantineReason ??= "stale generation, candidate or attempt"; return true;
      }
      execution.status = "observed";
      for (const [index, finding] of execution.result.findings.entries()) {
        // The original producer ID remains in the result; a bounded natural slot
        // avoids truncation collisions when promoting it into the finding ledger.
        const retained = { ...finding, id: `${execution.request.id}.${index}` };
        l.findings.push({ finding: retained });
        if (finding.severity === "blocking" && finding.materiality === "plan_affecting") run.status = "needs_replan";
      }
      return true;
    });
  }
  async advanceLifecycle(m: MutationV2, itemId: string, generation: number, stage: number): Promise<RunV2> {
    return this.change(m, async (run, plan) => {
      this.dispatchGuard(run, plan); const l = this.node(run, itemId, generation).lifecycle;
      requireV2(l && !l.stop, "LIFECYCLE_NOT_EXECUTABLE");
      if (l.passed.includes(stage)) return false;
      requireV2(l.stage === stage, "STAGE_OUT_OF_ORDER");
      requireV2(!l.findings.some(f => f.finding.severity === "blocking" && !f.disposition), "UNRESOLVED_FINDINGS");
      if (stage < 8) { assertStageV2(run, plan, itemId, stage); l.passed.push(stage); l.stage++; }
      else { l.ready = true; l.passed.push(8); assertReadyV2(run, plan, itemId, l.candidate); }
      return true;
    });
  }
  async dispositionFinding(m: MutationV2, itemId: string, findingId: string, disposition: string): Promise<RunV2> {
    requireV2(typeof disposition === "string" && disposition.trim().length > 0 && disposition.length <= 65536, "FINDING_DISPOSITION_REQUIRED");
    return this.change(m, async run => {
      requireV2(!["complete", "cancelled"].includes(run.status), "RUN_TERMINAL");
      const finding = run.nodes[itemId]?.lifecycle?.findings.find(f => f.finding.id === findingId);
      requireV2(finding, "FINDING_NOT_FOUND");
      if (finding.disposition) { requireV2(finding.disposition === disposition, "FINDING_DISPOSITION_CONFLICT"); return false; }
      finding.disposition = disposition; return true;
    });
  }
  async retryCheck(m: MutationV2, itemId: string, generation: number, executionId: string): Promise<RunV2> {
    return this.change(m, async (run, plan) => {
      this.dispatchGuard(run, plan); const node = this.node(run, itemId, generation), l = node.lifecycle;
      const e = l?.executions.find(e => e.request.id === executionId);
      requireV2(l && e?.result && e.status === "observed" && currentExecutionV2(run, e.request) && e.result.disposition !== "PASS", "CURRENT_FAILED_EXECUTION_REQUIRED");
      requireV2(l.executions.every(e => e.result), "EXECUTION_RECONCILIATION_REQUIRED");
      requireV2(!unresolvedGitV2(run, itemId), "UNRESOLVED_GIT_OPERATION");
      const finding = e.result.findings.find(f => f.severity === "blocking");
      const kind = finding?.kind;
      const dimension: RetryDimensionV2 = e.contextRejection || kind === "infrastructure_failure" || kind === "capability_absent" || kind === "external_precondition_failure" ? "infrastructure"
        : kind === "product_defect" ? "product" : kind === "test_evidence_gap" ? "test" : kind === "architecture_issue" ? "review"
        : e.request.stage === 3 ? "test" : e.request.stage === 5 ? "review" : e.request.stage === 6 ? "hardening" : "product";
      const target = dimension === "test" ? 3 : dimension === "review" ? 5 : dimension === "infrastructure" ? e.request.stage : 1;
      const procedure = JSON.stringify(e.request.check.procedure);
      const fingerprint = e.contextRejection?.reason ?? finding?.fingerprint ?? `${procedure}:exit=${e.result.exitCode}:signal=${e.result.signal}`;
      try { consumeRetryV2(run, itemId, dimension, e.request.check.stage, procedure, fingerprint); }
      catch (error) { l.stop = String(error); return true; }
      l.ready = false; l.round++; l.stage = Math.min(target, l.stage); l.passed = l.passed.filter(s => s < l.stage);
      for (const previous of l.executions) if (previous.request.stage >= l.stage && previous.status !== "quarantined") {
        previous.status = "quarantined"; previous.quarantineReason = `typed ${dimension} retry`;
      }
      return true;
    });
  }
  private async change(m: MutationV2, update: (run: RunV2, plan: PlanV2, snapshot: SnapshotV2) => Promise<boolean>): Promise<RunV2> {
    return this.store.transaction(async (s, publish) => {
      const run = await this.guard(s, m);
      if (await update(run, runPlanV2(s, run), s)) { run.revision++; await publish(); }
      return run;
    });
  }
  private async guard(s: SnapshotV2, m: MutationV2): Promise<RunV2> {
    const run = this.run(s, m.runId); this.cas(run.revision, m.expectedRevision);
    requireV2(run.lease && sameV2(run.lease, m.lease) && run.lease.pid === process.pid && run.lease.processStart === await processIdentityV2(), "STALE_LEASE"); return run;
  }
  private dispatchGuard(run: RunV2, plan: PlanV2): void { requireV2(run.status === "active", "RUN_NOT_ACTIVE"); assertScopeV2(plan, run.start.authority, this.now()); }
  private node(run: RunV2, id: string, generation: number) { const node = run.nodes[id]; requireV2(node && node.generation === generation, "STALE_GENERATION"); return node; }
  private run(s: SnapshotV2, id: string): RunV2 { const run = s.runs[id]; requireV2(run, "RUN_NOT_FOUND"); return run; }
  private cas(actual: number, expected: number): void { requireV2(Number.isSafeInteger(expected) && actual === expected, `STALE_REVISION: expected ${expected}, current ${actual}`); }
  private select(s: SnapshotV2, selection: PlanSelectorV2, current: boolean): PlanV2 {
    validateShapeV2(PlanSelectorV2Schema, selection);
    const versions = s.plans[selection.planId];
    const plan = versions?.find(p => p.revision === selection.revision && p.planHash === selection.planHash);
    requireV2(plan && (!current || plan === versions.at(-1)), "PLAN_SELECTION_STALE_OR_MISSING"); return parsePlanV2(plan);
  }
}
