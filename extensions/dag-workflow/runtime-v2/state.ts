import { Type, type Static } from "typebox";
import { StrictObject } from "../dag-runtime/common.ts";
import { CountV2, IdV2, TextV2, sameV2, PlanV2Schema, PlanSelectorV2Schema, parsePlanV2, requireV2, validateShapeV2, type PlanV2 } from "../planning/v2.ts";

import { CandidateV2Schema, LifecycleV2Schema, RetryV2Schema, RetryDimensionV2Schema, CommandJobV2Schema } from "./lifecycle-schema.ts";
import { auditLifecycleV2, assertReadyV2, auditResultV2, contextReusedV2, retryLimitsV2 } from "./lifecycle.ts";
export { CandidateV2Schema } from "./lifecycle-schema.ts";

const nonnegative = Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
export const AuthorityV2Schema = StrictObject({ scope: Type.Array(IdV2, { minItems: 1, maxItems: 512, uniqueItems: true }), maxConcurrency: CountV2,
  // This slice cannot grant restricted effects. N04 supplies separate effect-specific checks.
  effects: Type.Array(Type.Literal("repository_local"), { minItems: 1, maxItems: 1 }), expiresAt: Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }) });
export const StartV2Schema = StrictObject({ intent: Type.Literal("run"), sessionId: IdV2, selection: PlanSelectorV2Schema, authority: AuthorityV2Schema });
export const LeaseV2Schema = StrictObject({ sessionId: IdV2, pid: Type.Integer({ minimum: 1, maximum: 2147483647 }), processStart: TextV2, generation: CountV2 });
export const ReservationV2Schema = StrictObject({ operationId: TextV2, runId: IdV2, itemId: IdV2, generation: CountV2, request: TextV2,
  state: Type.Union([Type.Literal("reserved"), Type.Literal("dispatching"), Type.Literal("bound")]), workerId: Type.Optional(TextV2) });
export const IntegrationV2Schema = StrictObject({ operationId: TextV2, runId: IdV2, itemId: IdV2, generation: CountV2, candidate: CandidateV2Schema, target: CandidateV2Schema });
const node = StrictObject({ generation: CountV2, status: Type.Union([Type.Literal("excluded"), Type.Literal("pending"), Type.Literal("active"), Type.Literal("complete"), Type.Literal("cancelled")]),
  reservation: Type.Optional(ReservationV2Schema), integration: Type.Optional(IntegrationV2Schema), lifecycle: Type.Optional(LifecycleV2Schema), retries: Type.Optional(Type.Array(RetryV2Schema)), retryHistory: Type.Optional(Type.Array(StrictObject({ dimension: RetryDimensionV2Schema, fingerprint: TextV2, tree: Type.Optional(CandidateV2Schema.properties.tree) }))) });
export const RunV2Schema = StrictObject({ kind: Type.Literal("dag_run_v2"), schemaVersion: Type.Literal(2), runId: IdV2, revision: nonnegative,
  start: StartV2Schema, predecessorRunId: Type.Optional(IdV2), lease: Type.Optional(LeaseV2Schema),
  status: Type.Union([Type.Literal("active"), Type.Literal("paused"), Type.Literal("needs_replan"), Type.Literal("complete"), Type.Literal("cancelling"), Type.Literal("cancelled")]),
  replanDisposition: Type.Optional(TextV2),
  nodes: Type.Record(IdV2, node, { additionalProperties: false }), releasedGates: Type.Array(IdV2, { uniqueItems: true, maxItems: 512 }),
});
export const SnapshotV2Schema = StrictObject({ kind: Type.Literal("dag_store_v2"), schemaVersion: Type.Literal(2), revision: nonnegative,
  executions: Type.Optional(Type.Record(IdV2, CommandJobV2Schema, { additionalProperties: false })),
  plans: Type.Record(IdV2, Type.Array(PlanV2Schema, { minItems: 1 }), { additionalProperties: false }), runs: Type.Record(IdV2, RunV2Schema, { additionalProperties: false }), bindings: Type.Record(IdV2, IdV2, { additionalProperties: false }) });
export type AuthorityV2 = Static<typeof AuthorityV2Schema>;
export type StartV2 = Static<typeof StartV2Schema>;
export type LeaseV2 = Static<typeof LeaseV2Schema>;
export type ReservationV2 = Static<typeof ReservationV2Schema>;
export type IntegrationV2 = Static<typeof IntegrationV2Schema>;
export type RunV2 = Static<typeof RunV2Schema>;
export type SnapshotV2 = Static<typeof SnapshotV2Schema>;
export const emptySnapshotV2 = (): SnapshotV2 => ({ kind: "dag_store_v2", schemaVersion: 2, revision: 0, plans: Object.create(null), runs: Object.create(null), bindings: Object.create(null) });
export function assertScopeV2(plan: PlanV2, authority: AuthorityV2, now: number): void {
  validateShapeV2(AuthorityV2Schema, authority);
  requireV2(authority.expiresAt > now, "AUTHORITY_EXPIRED");
  requireV2(authority.maxConcurrency <= plan.constraints.maxConcurrency, "CONCURRENCY_EXCEEDS_PLAN");
  const scope = new Set(authority.scope);
  for (const id of scope) {
    const item = plan.workItems.find(n => n.id === id);
    requireV2(item && item.dependsOn.every(dep => scope.has(dep)), "SCOPE_NOT_DEPENDENCY_CLOSED");
  }
  if (plan.integration.strategy === "serial") {
    const last = Math.max(...authority.scope.map(id => plan.workItems.findIndex(n => n.id === id)));
    requireV2(plan.workItems.slice(0, last + 1).every(n => scope.has(n.id)), "SCOPE_NOT_INTEGRATION_CLOSED");
  }
}
export function runPlanV2(snapshot: SnapshotV2, run: RunV2): PlanV2 {
  const plan = snapshot.plans[run.start.selection.planId]?.find(p => p.revision === run.start.selection.revision);
  requireV2(plan && plan.planHash === run.start.selection.planHash, "RUN_PLAN_MISMATCH"); return plan;
}
/** Audit ingress, reload and every publication before any authoritative bytes change. */
export function auditSnapshotV2(value: unknown): SnapshotV2 {
  validateShapeV2(SnapshotV2Schema, value); const s = value as SnapshotV2;
  for (const [id, job] of Object.entries(s.executions ?? {})) {
    requireV2(id === job.request.id && (job.status === "settled") === Boolean(job.result), "EXECUTION_JOB_MISMATCH");
    const execution = s.runs[job.request.runId]?.nodes[job.request.itemId]?.lifecycle?.executions.find(e => e.request.id === id);
    requireV2(execution && sameV2(execution.request, job.request), "EXECUTOR_WITHOUT_LIFECYCLE_INTENT");
    if (job.result) auditResultV2(job.request, job.result);
    if (execution.contextRejection) requireV2(sameV2(job.result, execution.contextRejection.observed), "REJECTED_EXECUTOR_RESULT_MISMATCH");
  }
  for (const [id, revisions] of Object.entries(s.plans)) revisions.forEach((p, i) => { parsePlanV2(p); requireV2(p.planId === id && p.revision === i + 1, "PLAN_REVISION_GAP"); });
  for (const [id, r] of Object.entries(s.runs)) {
    requireV2(r.runId === id, "RUN_ID_MISMATCH"); const p = runPlanV2(s, r); assertScopeV2(p, r.start.authority, 0);
    requireV2(Object.keys(r.nodes).length === p.workItems.length, "NODE_SET_MISMATCH");
    requireV2(r.releasedGates.every(g => p.constraints.gates.includes(g)), "UNKNOWN_GATE");
    for (const n of p.workItems) {
      const state = r.nodes[n.id]; requireV2(state && ((state.status === "excluded") === !r.start.authority.scope.includes(n.id)), "NODE_SCOPE_MISMATCH");
      if (state.status === "active" || state.status === "complete") requireV2(n.dependsOn.every(d => r.nodes[d].status === "complete"), "DEPENDENCY_NOT_INTEGRATED");
      if (state.reservation) requireV2(state.reservation.runId === id && state.reservation.itemId === n.id && state.reservation.generation === state.generation - (state.status === "cancelled" ? 1 : 0) && ["active", "complete", "cancelled"].includes(state.status), "RESERVATION_MISMATCH");
      if (state.reservation) {
        requireV2(state.reservation.operationId === `${id}/${n.id}/${state.reservation.generation}`, "RESERVATION_ID_MISMATCH");
        requireV2((state.reservation.state === "bound") === Boolean(state.reservation.workerId), "WORKER_BINDING_MISMATCH");
      }
      if (state.integration) requireV2(state.status === "complete" && state.integration.operationId === `${id}/${n.id}/${state.generation}/integration`, "INTEGRATION_ID_MISMATCH");
      for (const [dimension, limit] of Object.entries(retryLimitsV2)) {
        const retries = (state.retries ?? []).filter(r => r.dimension === dimension);
        const count = retries.reduce((sum, r) => sum + r.count, 0);
        requireV2(count <= limit && count === (state.retryHistory ?? []).filter(r => r.dimension === dimension).length
          && retries.every(r => r.failures.length === r.count && r.trees.length <= r.count), "RETRY_LEDGER_MISMATCH");
      }
      if (state.lifecycle) auditLifecycleV2(r, p, n.id);
      if (state.integration) assertReadyV2(r, p, n.id, state.integration.candidate);
      if (state.status === "active") requireV2(state.reservation, "ACTIVE_WITHOUT_RESERVATION");
      if (state.status === "complete") requireV2(state.integration && state.integration.runId === id && state.integration.itemId === n.id && state.integration.generation === state.generation, "COMPLETION_WITHOUT_INTEGRATION");
    }
    // Rejected later observations do not retroactively invalidate accepted
    // identities. New acceptance still consults the full retained history.
    for (const n of Object.values(r.nodes)) for (const e of n.lifecycle?.executions ?? []) {
      if (e.result && !e.contextRejection) requireV2(!contextReusedV2(r, e.result, false), "EVALUATOR_CONTEXT_REUSED_ACROSS_ITEMS");
    }
    requireV2((r.status === "complete") === Object.values(r.nodes).every(n => ["excluded", "complete"].includes(n.status)), "TERMINAL_MISMATCH");
    requireV2(Object.values(r.nodes).every(n => n.status !== "cancelled" || ["cancelling", "cancelled"].includes(r.status)), "CANCELLATION_MISMATCH");
    if (["cancelling", "cancelled"].includes(r.status)) requireV2(Object.values(r.nodes).every(n => ["excluded", "complete", "cancelled"].includes(n.status)), "CANCELLATION_NOT_FENCED");
    if (p.integration.strategy === "serial") {
      let prefixComplete = true;
      for (const n of p.workItems) {
        const status = r.nodes[n.id].status;
        if (status === "active" || status === "complete") requireV2(prefixComplete, "SERIAL_PREFIX_NOT_COMPLETE");
        prefixComplete &&= status === "complete";
      }
    }
    const active = p.workItems.filter(n => r.nodes[n.id].status === "active");
    requireV2(active.length <= r.start.authority.maxConcurrency, "LANE_OVERCOMMIT");
    for (const [resource, capacity] of Object.entries(p.constraints.resources)) requireV2(active.reduce((sum, n) => sum + (n.resources[resource] ?? 0), 0) <= capacity, "RESOURCE_OVERCOMMIT");
    for (const mutex of p.constraints.mutexGroups) requireV2(active.filter(n => mutex.workItemIds.includes(n.id)).length <= 1, "MUTEX_OVERCOMMIT");
    if (r.lease) requireV2(r.lease.sessionId === r.start.sessionId, "LEASE_SESSION_MISMATCH");
  }
  for (const [session, id] of Object.entries(s.bindings)) requireV2(s.runs[id]?.start.sessionId === session, "BINDING_MISMATCH");
  return s;
}
export function admissibleV2(plan: PlanV2, run: RunV2): string[] {
  if (run.status !== "active") return [];
  const active = plan.workItems.filter(n => run.nodes[n.id].status === "active");
  if (active.length >= run.start.authority.maxConcurrency) return [];
  // A later serial item cannot release its sticky lane until the prefix lands.
  // Admit only the next unfinished item, even when spare lanes are available.
  const nextSerial = plan.integration.strategy === "serial"
    ? plan.workItems.find(n => !["complete", "excluded"].includes(run.nodes[n.id].status))?.id : undefined;
  return plan.workItems.filter(n => run.nodes[n.id].status === "pending"
    && (plan.integration.strategy !== "serial" || n.id === nextSerial)
    && n.dependsOn.every(d => run.nodes[d].status === "complete") && n.gates.every(g => run.releasedGates.includes(g))
    && !plan.constraints.mutexGroups.some(m => m.workItemIds.includes(n.id) && active.some(a => m.workItemIds.includes(a.id)))
    && Object.entries(n.resources).every(([id, demand]) => demand + active.reduce((sum, a) => sum + (a.resources[id] ?? 0), 0) <= plan.constraints.resources[id])
  ).map(n => n.id);
}
