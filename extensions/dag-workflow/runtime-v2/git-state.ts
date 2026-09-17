import { requireV2, sameV2, type PlanV2 } from "../planning/v2.ts";
import type { SnapshotV2, RunV2 } from "./state.ts";
import type { ExecutionRequestV2 } from "./lifecycle-schema.ts";
import { assertReadyV2, auditResultV2 } from "./lifecycle.ts";
import type { GitOperationV2 } from "./git-schema.ts";

export function gitExecutionV2(s: SnapshotV2, id: string) {
  for (const run of Object.values(s.runs)) for (const op of run.gitOperations ?? []) {
    const request = op.checks.find(r => r.id === id); if (request) return { run, op, request };
  }
}
export function currentGitExecutionV2(s: SnapshotV2, request: ExecutionRequestV2): boolean {
  const found = gitExecutionV2(s, request.id);
  if (!found) return false;
  const { run, op } = found, node = run.nodes[op.itemId];
  return sameV2(found.request, request) && op.phase === "composed" && run.status === "active"
    && sameV2(run.lease, op.lease) && node.generation === op.generation && Boolean(node.lifecycle?.ready)
    && sameV2(node.lifecycle?.candidate, op.candidate) && Date.now() < run.start.authority.expiresAt;
}
export function unresolvedGitV2(run: RunV2, itemId?: string): boolean {
  return (run.gitOperations ?? []).some(op => (!itemId || op.itemId === itemId) && !["accepted", "closed"].includes(op.phase));
}
export function assertGitChecksV2(s: SnapshotV2, op: GitOperationV2): void {
  requireV2(op.checks.length > 0 && op.proposal, "GIT_CHECKS_MISSING");
  for (const request of op.checks) {
    const result = s.executions?.[request.id]?.result;
    requireV2(result && sameV2(result.request, request) && sameV2(request.candidate, op.proposal), "GIT_CHECK_MISSING_OR_STALE");
    auditResultV2(request, result);
    requireV2(result.disposition === "PASS", `GIT_CHECK_NONPASS ${request.check.id}: ${result.diagnostic}`);
  }
}
export function auditGitV2(s: SnapshotV2, run: RunV2, plan: PlanV2): void {
  const ids = new Set<string>(), checks = new Set<string>();
  let prefix = { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree };
  for (const op of run.gitOperations ?? []) {
    const node = run.nodes[op.itemId];
    requireV2(node && !ids.has(op.operationId) && op.operationId === `${run.runId}/${op.itemId}/${op.generation}/integration`
      && op.reservation === `${run.runId}/${op.itemId}/${op.generation}` && sameV2(run.gitBinding, op.binding), "GIT_OPERATION_JOIN_MISMATCH");
    ids.add(op.operationId);
    requireV2(sameV2(op.expected, prefix), "GIT_EXPECTED_PREFIX_MISMATCH");
    requireV2(op.targetRef === plan.repository.targetBranch && sameV2(op.sourceBase, { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree }), "GIT_REQUEST_PLAN_MISMATCH");
    if (!["intent", "blocked", "closed"].includes(op.phase)) requireV2(op.proposal, "GIT_PROPOSAL_REQUIRED");
    if (op.checks.length) {
      const commands = [...plan.integration.prefixCommands, ...plan.integration.finalCommands];
      requireV2(op.checks.length === commands.length, "GIT_CHECK_SET_MISMATCH");
      op.checks.forEach((r, index) => {
        requireV2(!checks.has(r.id) && r.stage === 7 && r.round === 1 && r.authority.expiresAt === run.start.authority.expiresAt
          && r.check.environment === "node-local" && r.check.applicability.kind === "required" && r.check.replay === "idempotent"
          && r.runId === run.runId && r.itemId === op.itemId && r.generation === op.generation
          && r.attempt === op.operationId && sameV2(r.plan, run.start.selection) && sameV2(r.candidate, op.proposal)
          && r.check.procedure.kind === "command" && sameV2(r.check.procedure.argv, commands[index].argv), "GIT_CHECK_REQUEST_MISMATCH");
        checks.add(r.id);
      });
    }
    if (["validated", "landing", "landed", "accepted"].includes(op.phase)) assertGitChecksV2(s, op);
    if (["landed", "accepted"].includes(op.phase)) requireV2(op.landing?.settled && op.observation === "new-clean", "GIT_LANDING_NOT_RECONCILED");
    if (op.phase === "closed") requireV2(["old-clean", "new-clean"].includes(op.observation ?? "") && (!op.landing || op.landing.settled)
      && op.checks.every(c => !s.executions?.[c.id] || s.executions[c.id].status === "settled"), "GIT_CLOSURE_UNRESOLVED");
    if (op.phase === "accepted") {
      assertReadyV2(run, plan, op.itemId, op.candidate);
      requireV2(node.integration?.operationId === op.operationId && sameV2(node.integration.target, op.proposal), "GIT_ACCEPTANCE_MISMATCH");
      prefix = op.proposal!;
    }
    if (node.integration?.operationId === op.operationId) requireV2(op.phase === "accepted", "INTEGRATION_WITHOUT_GIT_ACCEPTANCE");
  }
  if (["complete", "cancelled"].includes(run.status)) requireV2(!unresolvedGitV2(run), "UNRESOLVED_GIT_OPERATION");
}
