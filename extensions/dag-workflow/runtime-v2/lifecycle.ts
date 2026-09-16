import { randomUUID } from "node:crypto";
import { requireV2, sameV2, selectorV2, type PlanV2, type LifecycleCheckV2 } from "../planning/v2.ts";
import type { RunV2 } from "./state.ts";
import type { CandidateV2, ExecutionRequestV2, ExecutionResultV2, LifecycleV2, RetryDimensionV2 } from "./lifecycle-schema.ts";

export const retryLimitsV2 = { product: 3, test: 3, review: 3, hardening: 3, infrastructure: 1, replacement: 2, integration: 3 } as const;
export function stageChecksV2(plan: PlanV2, itemId: string, stage: number): LifecycleCheckV2[] {
  return plan.workItems.find(n => n.id === itemId)!.lifecycle.checks.filter(c => c.applicability.kind === "required" && (stage === 7 || c.stage === stage));
}
export function frameV2(run: RunV2, plan: PlanV2, itemId: string, candidate: CandidateV2, now: number, candidateReady = true): LifecycleV2 {
  const node = run.nodes[itemId], previous = node.lifecycle, item = plan.workItems.find(n => n.id === itemId)!;
  const candidates = [...(previous?.candidates ?? [])];
  if (candidateReady && !sameV2(candidates.at(-1) ?? null, candidate)) candidates.push(structuredClone(candidate));
  return { candidate: structuredClone(candidate), candidateReady, candidates, round: (previous?.round ?? 0) + 1, stage: 1, passed: [0], ready: false,
    frame: { plan: selectorV2(plan), baseline: { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree }, oracle: item.lifecycle.oracle.statement,
      risk: item.risk, checks: item.lifecycle.checks.filter(c => c.applicability.kind === "required").map(c => c.id), at: now },
    executions: previous?.executions ?? [], findings: previous?.findings ?? [] };
}
export function executionRequestV2(run: RunV2, itemId: string, check: LifecycleCheckV2): ExecutionRequestV2 {
  const n = run.nodes[itemId], l = n.lifecycle!;
  requireV2(l.candidateReady && l.stage < 8 && n.reservation?.workerId, "LIFECYCLE_NOT_EXECUTABLE");
  return { id: `execution-${randomUUID()}`, plan: structuredClone(run.start.selection), runId: run.runId, itemId, generation: n.generation,
    attempt: `${n.reservation.operationId}/F${l.stage}/${l.round}`, round: l.round, stage: l.stage as ExecutionRequestV2["stage"], candidate: structuredClone(l.candidate),
    implementationWorkerId: n.reservation.workerId, check: structuredClone(check), authority: { effect: "repository_local", expiresAt: run.start.authority.expiresAt } };
}
export function currentExecutionV2(run: RunV2, request: ExecutionRequestV2): boolean {
  const n = run.nodes[request.itemId], l = n?.lifecycle;
  return Boolean(n && l && n.generation === request.generation && request.round <= l.round && sameV2(l.candidate, request.candidate)
    && n.reservation?.workerId === request.implementationWorkerId && !["cancelled", "excluded"].includes(n.status)
    && run.runId === request.runId && sameV2(run.start.selection, request.plan));
}
export function auditResultV2(request: ExecutionRequestV2, result: ExecutionResultV2): void {
  requireV2(sameV2(request, result.request), "RESULT_REQUEST_MISMATCH");
  requireV2(result.findings.every(f => f.kind !== "oracle_contract_issue" || f.materiality === "plan_affecting"), "ORACLE_FINDING_MUST_REPLAN");
  requireV2(result.endedAt >= result.startedAt && result.durationMs >= 0, "RESULT_TIMING_MISMATCH");
  requireV2(result.executor.kind === request.check.procedure.kind, "RESULT_PRODUCER_MISMATCH");
  requireV2(result.executor.identity === (request.check.procedure.kind === "command" ? request.check.procedure.argv[0] : request.check.procedure.producerId), "RESULT_PRODUCER_MISMATCH");
  if (result.disposition === "PASS") {
    requireV2(result.environment.profile === request.check.environment, "RESULT_ENVIRONMENT_MISMATCH");
    requireV2(result.executor.invoked && result.startedAt < request.authority.expiresAt, "UNEXECUTED_OR_UNAUTHORIZED_RESULT");
    requireV2(sameV2(result.workspace.candidate, request.candidate) && result.workspace.cleanBefore && result.workspace.cleanAfter && result.workspace.isolated, "RESULT_CANDIDATE_OR_CLEAN_MISMATCH");
    requireV2(result.signal === null && (request.check.procedure.kind !== "command" || result.exitCode === 0), "RESULT_EXIT_MISMATCH");
    requireV2(!result.findings.some(f => f.severity === "blocking"), "PASS_WITH_BLOCKING_FINDING");
  }
  if (request.stage === 2 || request.stage === 5 || request.stage === 7) {
    requireV2(result.executor.contextId !== request.implementationWorkerId && result.executor.lineage.length === 0, "INDEPENDENT_CONTEXT_REQUIRED");
  }
}
export function auditLifecycleV2(run: RunV2, plan: PlanV2, itemId: string): void {
  const n = run.nodes[itemId], l = n.lifecycle!, item = plan.workItems.find(n => n.id === itemId)!;
  requireV2(sameV2(l.frame.plan, run.start.selection) && l.frame.oracle === item.lifecycle.oracle.statement && l.frame.risk === item.risk
    && sameV2(l.frame.baseline, { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree })
    && sameV2(l.frame.checks, item.lifecycle.checks.filter(c => c.applicability.kind === "required").map(c => c.id)), "FRAME_MISMATCH");
  requireV2(!l.candidateReady || sameV2(l.candidates.at(-1), l.candidate), "CANDIDATE_HISTORY_MISMATCH");
  requireV2(l.passed.includes(0) && l.passed.every((s, i) => s === i) && l.passed.length === l.stage + (l.ready ? 1 : 0), "STAGE_SEQUENCE_MISMATCH");
  const ids = new Set<string>(), contexts = new Set<string>();
  for (const e of l.executions) {
    requireV2(!ids.has(e.request.id), "DUPLICATE_EXECUTION"); ids.add(e.request.id);
    requireV2(e.request.runId === run.runId && e.request.itemId === itemId && sameV2(e.request.plan, run.start.selection), "EXECUTION_BINDING_MISMATCH");
    requireV2(item.lifecycle.checks.some(c => sameV2(c, e.request.check)) && (e.request.stage === 7 || e.request.stage === e.request.check.stage), "EXECUTION_CHECK_MISMATCH");
    requireV2(e.request.attempt === `${run.runId}/${itemId}/${e.request.generation}/F${e.request.stage}/${e.request.round}`, "EXECUTION_ATTEMPT_MISMATCH");
    requireV2(e.status !== "observed" || e.result, "OBSERVATION_MISSING");
    if (e.result) {
      auditResultV2(e.request, e.result);
      if (e.request.stage === 2 || e.request.stage === 5 || e.request.stage === 7) {
        requireV2(!contexts.has(e.result.executor.contextId), "EVALUATOR_CONTEXT_REUSED");
      }
      contexts.add(e.result.executor.contextId);
    }
  }
  for (const stage of l.passed.filter(s => s > 0 && s < 8)) assertStageV2(run, plan, itemId, stage);
  if (l.ready) assertReadyV2(run, plan, itemId, l.candidate);
}
export function assertStageV2(run: RunV2, plan: PlanV2, itemId: string, stage: number): void {
  const l = run.nodes[itemId].lifecycle!;
  for (const check of stageChecksV2(plan, itemId, stage)) {
    const matches = l.executions.filter(e => e.request.stage === stage && e.request.check.id === check.id && e.status === "observed" && currentExecutionV2(run, e.request));
    requireV2(matches.length === 1 && matches[0].result?.disposition === "PASS", `CHECK_NOT_PASSED: F${stage} ${check.id} ${matches[0]?.result?.diagnostic ?? "missing current execution"}`);
  }
}
export function assertReadyV2(run: RunV2, plan: PlanV2, itemId: string, candidate: CandidateV2): void {
  const l = run.nodes[itemId]?.lifecycle;
  requireV2(l && l.candidateReady && l.ready && l.stage === 8 && sameV2(l.candidate, candidate) && l.passed.length === 9 && !l.stop, "LIFECYCLE_NOT_READY");
  requireV2(!l.findings.some(f => f.finding.severity === "blocking" && !f.disposition), "UNRESOLVED_FINDINGS");
  requireV2(l.executions.every(e => e.result), "EXECUTION_RECONCILIATION_REQUIRED");
  for (let stage = 1; stage <= 7; stage++) assertStageV2(run, plan, itemId, stage);
}
/** Counters survive candidates, physical worker replacement and stage back-edges.
 * Fingerprints use procedure/failure identity, never check display names. */
export function consumeRetryV2(run: RunV2, itemId: string, dimension: RetryDimensionV2, stage: number, procedure: string, fingerprint: string): void {
  const n = run.nodes[itemId], tree = n.lifecycle?.candidate.tree;
  const counters = n.retries ??= [], history = n.retryHistory ??= [];
  // A conservative item/dimension ceiling also prevents renamed procedures or
  // changing fingerprints from manufacturing unlimited fresh retry buckets.
  requireV2(counters.filter(r => r.dimension === dimension).reduce((sum, r) => sum + r.count, 0) < retryLimitsV2[dimension], `RETRY_EXHAUSTED: ${dimension}`);
  const previous = history.filter(r => r.dimension === dimension);
  if (dimension !== "replacement" && tree) {
    requireV2(!(previous.length >= 2 && previous.slice(-2).every(r => r.tree === tree)), "NO_PROGRESS: two consecutive retries without a new tree");
    requireV2(!(previous.some(r => r.tree === tree) && previous.at(-1)?.tree !== tree), "NO_PROGRESS: recurring candidate tree");
    requireV2(!(previous.length >= 2 && previous.at(-2)?.fingerprint === fingerprint && previous.at(-1)?.fingerprint !== fingerprint), "NO_PROGRESS: failure oscillation");
  }
  let retry = counters.find(r => r.dimension === dimension && r.stage === stage && r.procedure === procedure && r.fingerprint === fingerprint);
  if (!retry) { retry = { dimension, stage, procedure, fingerprint, count: 0, trees: [], failures: [] }; counters.push(retry); }
  requireV2(retry.count < retryLimitsV2[dimension], `RETRY_EXHAUSTED: ${dimension} ${fingerprint}`);
  if (tree && dimension !== "replacement") {
    requireV2(!(retry.trees.length >= 2 && retry.trees.slice(-2).every(t => t === tree)), "NO_PROGRESS: two retries on the same tree");
    requireV2(!(retry.trees.includes(tree) && retry.trees.at(-1) !== tree), "NO_PROGRESS: recurring candidate tree");
    requireV2(new Set(retry.trees).size < 2, "NO_PROGRESS: fingerprint survived two repair trees");
    retry.trees.push(tree);
  }
  retry.count++; retry.failures.push(fingerprint); history.push({ dimension, fingerprint, ...(tree ? { tree } : {}) });
}
export function invalidateLifecycleV2(run: RunV2, itemId: string, reason: string): void {
  const l = run.nodes[itemId].lifecycle;
  if (!l) return;
  l.ready = false; l.passed = [0]; l.stage = 1; l.round++;
  for (const e of l.executions) if (e.status !== "quarantined") { e.status = "quarantined"; e.quarantineReason = reason; }
}
