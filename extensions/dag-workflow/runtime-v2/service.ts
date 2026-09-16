import { randomUUID } from "node:crypto";
import { createPlanV2, parsePlanV2, PlanSelectorV2Schema, requireV2, sameV2, validateShapeV2, type PlanInputV2, type PlanSelectorV2, type PlanV2 } from "../planning/v2.ts";
import { admissibleV2, assertScopeV2, IntegrationV2Schema, StartV2Schema, runPlanV2, type IntegrationV2, type LeaseV2, type ReservationV2, type RunV2, type SnapshotV2, type StartV2 } from "./state.ts";
import { processIdentityV2, StoreV2 } from "./store.ts";

/** Hydrate from the repository/model, not from the submitted plan. Called inside
 * the start consistency guard; N05 owns concrete product adapters. */
export interface FreshnessV2 { current(plan: Readonly<PlanV2>): Promise<Pick<PlanV2, "repository" | "source">> }
/** The generic worker manager must durably key creation by operationId and compare
 * the entire request. Repeated ensure after acknowledgement loss must return the
 * same worker, never create a second process. No caller-generated action ID. */
export interface WorkersV2 { ensure(reservation: Readonly<ReservationV2>): Promise<{ workerId: string }> }
/** N03/N04 boundary: independently hydrate actual lifecycle and reconciled landing
 * evidence. A worker's completion assertion is NOT an implementation of this. */
export interface IntegrationsV2 { verify(run: Readonly<RunV2>, plan: Readonly<PlanV2>, integration: Readonly<IntegrationV2>): Promise<void> }
export interface MutationV2 { runId: string; expectedRevision: number; lease: LeaseV2 }
export class RuntimeV2 {
  readonly store: StoreV2;
  readonly freshness: FreshnessV2;
  readonly now: () => number;
  constructor(store: StoreV2, freshness: FreshnessV2, now: () => number = Date.now) { this.store = store; this.freshness = freshness; this.now = now; }

  async save(input: PlanInputV2, expectedStoreRevision: number): Promise<PlanV2> {
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
      if (reservation.state === "bound") return run;
      if (reservation.state === "reserved") { reservation.state = "dispatching"; run.revision++; await publish(); }
      const worker = await workers.ensure(structuredClone(reservation));
      requireV2(typeof worker.workerId === "string" && worker.workerId.length > 0 && worker.workerId.length <= 65536, "INVALID_WORKER_BINDING");
      reservation.workerId = worker.workerId; reservation.state = "bound";
      run.revision++; await publish(); return run;
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
      await integrations.verify(structuredClone(run), structuredClone(plan), structuredClone(evidence));
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
      if (run.status === "needs_replan" && action === "resume") requireV2(disposition?.trim(), "REPLAN_DISPOSITION_REQUIRED");
      const status = action === "pause" ? "paused" : action === "resume" ? "active" : "needs_replan";
      if (status === run.status) return false;
      if (run.status === "needs_replan" && action === "resume") run.replanDisposition = disposition!;
      run.status = status; return true;
    });
  }
  /** Generation replacement only after the worker adapter proves the old natural
   * operation settled; N03 owns retry budgets and evidence invalidation. */
  async replace(m: MutationV2, itemId: string, generation: number, settled: (reservation: Readonly<ReservationV2>) => Promise<void>): Promise<RunV2> {
    return this.change(m, async run => {
      requireV2(!["complete", "cancelling", "cancelled"].includes(run.status), "RUN_TERMINAL_OR_CANCELLING");
      const node = this.node(run, itemId, generation);
      requireV2(node.status === "active" && node.reservation, "ACTIVE_RESERVATION_REQUIRED");
      await settled(structuredClone(node.reservation));
      node.generation++;
      node.reservation = { ...node.reservation, operationId: `${run.runId}/${itemId}/${node.generation}`, generation: node.generation, state: "reserved" };
      delete node.reservation.workerId;
      return true;
    });
  }
  /** Fence every unfinished generation durably BEFORE the caller signals workers.
   * A cancelling binding is still active until all worker/effect ambiguity settles. */
  async cancel(m: MutationV2): Promise<RunV2> {
    return this.change(m, async run => {
      if (["cancelling", "cancelled"].includes(run.status)) return false;
      requireV2(run.status !== "complete", "RUN_TERMINAL");
      for (const node of Object.values(run.nodes)) if (["pending", "active"].includes(node.status)) { node.generation++; node.status = "cancelled"; }
      run.status = "cancelling"; return true;
    });
  }
  async reconcileCancellation(m: MutationV2, settled: (run: Readonly<RunV2>) => Promise<void>): Promise<RunV2> {
    return this.change(m, async run => {
      if (run.status === "cancelled") return false;
      requireV2(run.status === "cancelling", "CANCELLATION_REQUIRED");
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
  private async change(m: MutationV2, update: (run: RunV2, plan: PlanV2) => Promise<boolean>): Promise<RunV2> {
    return this.store.transaction(async (s, publish) => {
      const run = await this.guard(s, m);
      if (await update(run, runPlanV2(s, run))) { run.revision++; await publish(); }
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
