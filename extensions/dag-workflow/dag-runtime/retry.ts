import { canonicalHash } from "./common.ts";
import type { DagRunStateV1, DagRunValidationContextV1 } from "./run-state.ts";

// This is the policy already bound by retained thin-plan authorizations, not a
// migration or permission to interpret arbitrary retry-policy hashes.
const THIN_PLAN_RETRY_POLICY_HASH = canonicalHash({ policy: "thin-plan-bounded-retry-v1" });

export function isRecoveredWorkerRetryV1(entry: DagRunStateV1["retryLedger"][string]): boolean {
  return entry.fingerprint === canonicalHash({ policy: "thin-plan-worker-replacement-v1", workItemId: entry.workItemId, stage: entry.stage });
}

export function recoverableWorkerRetriesV1(state: DagRunStateV1, context: DagRunValidationContextV1): DagRunStateV1["retryLedger"] {
  const entries: DagRunStateV1["retryLedger"] = {};
  if (state.desired.run !== "running" || !["active", "integration"].includes(state.current.run) || state.completion.state !== "open" || Object.values(state.cancellations).some((cancellation) => cancellation.state !== "closed")) return entries;
  if (context.plan.lifecycleBinding.retryPolicyHash !== THIN_PLAN_RETRY_POLICY_HASH || context.authorization.retryCeilingsHash !== THIN_PLAN_RETRY_POLICY_HASH) return entries;
  for (const item of Object.values(state.workItems)) {
    const stage = item.currentStage ? item.stages[item.currentStage] : null;
    const attempt = stage?.currentAttemptId ? state.stageAttempts[stage.currentAttemptId] : null;
    const result = attempt?.workerResult ? context.facts[attempt.workerResult.hash] : null;
    if (!stage || !attempt || !["failed", "blocked"].includes(stage.state) || attempt.state !== "sealed" || !attempt.terminalAt || attempt.producerKind !== "owned_worker" || !result || result.kind !== "worker_result" || !["needs_attention", "failed", "lost"].includes(result.terminalStatus)) continue;
    if ((attempt.reservedOutputGeneration ?? attempt.inputGeneration) !== item.candidateGeneration || attempt.authorizationSetHash !== state.identity.authorizationSet.hash || item.desired !== "run" || !item.authorizedStages.includes(attempt.stage) || item.openFindingIds.length || item.blockerIds.length || stage.blockerIds.length) continue;
    if (Object.values(state.scheduler.reservations).some((reservation) => reservation.workItemId === item.workItemId && !["released", "fenced"].includes(reservation.state))) continue;
    if (Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && (["unknown", "non_repeatable"].includes(effect.procedureClass) || !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation)))) continue;
    if (!Object.values(state.effects).some((effect) => effect.kind === "cleanup_worktree" && effect.boundStageAttemptId === attempt.stageAttemptId && effect.boundWorkerResultHash === attempt.workerResult?.hash && effect.state === "reconciled" && ["applied_exact", "proven_absent"].includes(effect.reconciliation))) continue;
    const fingerprint = canonicalHash({ policy: "thin-plan-worker-replacement-v1", workItemId: item.workItemId, stage: attempt.stage });
    const retryKey = canonicalHash({ workItemId: item.workItemId, stage: attempt.stage, dimension: "worker_replacement", failureClass: "worker_runtime", fingerprint });
    // Count retained attempts conservatively across candidates and physical worker
    // identities. An empty ledger must not reset already spent replacement budget.
    const count = Math.max(0, stage.attemptIds.length - 1);
    const existing = state.retryLedger[retryKey];
    if (Object.values(state.retryLedger).some((entry) => entry.retryKey !== retryKey && entry.workItemId === item.workItemId && entry.stage === attempt.stage) || count >= 2) continue;
    if (existing) {
      if (existing.count === count && existing.ceiling <= 2 && existing.count < existing.ceiling && existing.stop === "none" && existing.authorizationSetHash === state.identity.authorizationSet.hash) entries[retryKey] = existing;
      continue;
    }
    entries[retryKey] = { retryKey, workItemId: item.workItemId, stage: attempt.stage, dimension: "worker_replacement", procedureId: null, failureClass: "worker_runtime", fingerprint, count, ceiling: 2, authorizationSetHash: state.identity.authorizationSet.hash, candidateTrees: [], repairCommitTrees: [], progressHashes: [], failureSequence: [], stop: "none", lastRetryCommandId: null };
  }
  return entries;
}
