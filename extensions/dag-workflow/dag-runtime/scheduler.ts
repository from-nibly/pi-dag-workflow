import { canonicalHash } from "./common.ts";
import { PLAN_STAGE_IDS, type CanonicalDagPlanV1 } from "./plan.ts";
import { dagRunNeedsReplanV1, dagRunSnapshotHash, type DagRunStateV1 } from "./run-state.ts";

export const DAG_SCHEDULER_POLICY_VERSION_V1 = "sticky-lanes-v1";
export const DAG_SCHEDULER_POLICY_V1 = {
  version: DAG_SCHEDULER_POLICY_VERSION_V1,
  fairnessReservationBypasses: 8,
  starvationReviewBypasses: 32,
  speculation: "disabled",
  ordering: ["fairness", "launch_class", "critical_height", "phase_progress", "bypass", "train_position", "repository", "work_item", "generation"],
} as const;
export const DAG_SCHEDULER_POLICY_HASH_V1 = canonicalHash(DAG_SCHEDULER_POLICY_V1);

export type DagSchedulerBlockerCodeV1 =
  | "OWNER_UNATTACHED" | "RUN_NOT_RUNNING" | "NEEDS_REPLAN" | "FRESHNESS_BLOCK" | "ITEM_NOT_RUNNABLE" | "AUTHORIZATION"
  | "PRECEDENCE" | "GATE" | "ITEM_BLOCKER" | "STAGE_BLOCKER" | "ATTEMPT_ACTIVE"
  | "EFFECT_UNRECONCILED" | "PROVIDER_HOLD" | "INTEGRATION_NOT_HEAD" | "INTEGRATION_DRIFT";
export type DagSchedulerAdmissionCodeV1 = "LANE_CAPACITY" | "EXISTING_RESERVATION" | "MUTEX" | "RESOURCE_CAPACITY" | "OPERATIONAL_CAPACITY" | "DYNAMIC_EXCLUSION" | "FAIRNESS_RESERVATION";

export interface DagSchedulerSlotV1 {
  slotId: string;
  workItemId: string;
  repositoryId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  operationKind: "conductor" | "implementation" | "evaluation" | "codification" | "verification" | "review" | "hardening" | "integration";
  itemGeneration: number;
  attemptOrdinal: number;
  correctnessReady: boolean;
  blockerCodes: DagSchedulerBlockerCodeV1[];
  blockerIds: string[];
  admissible: boolean;
  admissionCodes: DagSchedulerAdmissionCodeV1[];
  mutexGroupIds: string[];
  resourceUnits: Record<string, number>;
  operationalUnits: Record<string, number>;
  priority: {
    fairnessReserved: boolean;
    launchClass: number;
    riskOrdinal: number;
    criticalHeight: number;
    phaseProgress: number;
    bypassCount: number;
    trainPosition: number | null;
  };
}

export interface DagSchedulerReservationProposalV1 {
  reservationId: string;
  reservationSequence: number;
  slotId: string;
  workItemId: string;
  repositoryId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  operationKind: DagSchedulerSlotV1["operationKind"];
  itemGeneration: number;
  attemptOrdinal: number;
  normalizedRequestHash: string;
  mutexGroupIds: string[];
  resourceUnits: Record<string, number>;
  operationalUnits: Record<string, number>;
  workerRole: "none" | "implementation" | "evaluator" | "reviewer";
}

export interface DagSchedulerDecisionV1 {
  schemaVersion: 1;
  kind: "DagSchedulerDecisionV1";
  policyVersion: typeof DAG_SCHEDULER_POLICY_VERSION_V1;
  policyHash: string;
  normalizedIndexHash: string;
  planHash: string;
  runId: string;
  runNonceHash: string;
  inputRevision: number;
  inputSnapshotHash: string;
  ownerEpoch: number;
  decisionSequence: number;
  maxActiveNodes: number;
  activeLaneWorkItemIds: string[];
  frontier: DagSchedulerSlotV1[];
  selected: DagSchedulerReservationProposalV1[];
  bypassIncrements: string[];
  notice: "RESERVATIONS_PROPOSED" | "PAUSED" | "NEEDS_REPLAN" | "AWAITING_ACTIVE" | "WAITING_EXTERNAL" | "NEEDS_HUMAN" | "CAPACITY_OR_EXCLUSION_BLOCKED" | "INTEGRATION_DRIFT" | "RUN_BLOCKED" | "RUN_COMPLETED";
  decisionHash: string;
}

export interface DagWorkerProjectionInputV1 {
  projectionHash: string;
  workers: Array<{
    storageId: string;
    launchOwnerSessionId: string;
    workerId: string;
    attemptNumber: number;
    attemptNonce: string;
    configHash: string;
    terminalStatus: string | null;
    resultHash: string | null;
  }>;
}

export interface DagExecutionNodeV1 {
  alias: string;
  workItemId: string;
  title: string;
  repositoryId: string;
  current: string;
  stage: string | null;
  glyph: "." | ">" | ":" | "*" | "+" | "@" | "#" | "?" | "!" | "x";
  correctnessReady: boolean;
  admissible: boolean;
  schedulerOrder: number | null;
  blockerCodes: string[];
  blockerIds: string[];
  activeLane: boolean;
  laneAdmissionSequence: number | null;
  admittedAt: string | null;
  retryCount: number;
  findingCount: number;
  integrationPosition: number | null;
  worker: null | { workerId: string; attemptNumber: number; terminalStatus: string | null };
}

export interface DagExecutionProjectionV1 {
  schemaVersion: 1;
  kind: "DagExecutionProjectionV1";
  projectionVersion: "1";
  planHash: string;
  runId: string;
  runRevision: number;
  runSnapshotHash: string;
  workerProjectionHash: string | null;
  repositoryProjectionHash: string;
  schedulerDecisionHash: string;
  desired: string;
  current: string;
  completion: string;
  staleReadOnly: boolean;
  nodes: DagExecutionNodeV1[];
  precedence: Array<{ precedenceId: string; from: string; to: string; state: string }>;
  trainHeads: Array<{ repositoryId: string; entryId: string | null; workItemId: string | null; acceptedPrefixOrdinal: number }>;
  summary: { ready: number; activeLanes: number; attention: number; integrationReady: number; complete: number; omittedWorkers: number };
  projectionHash: string;
}

export type DagExecutionStageStateV2 = DagRunStateV1["workItems"][string]["stages"][typeof PLAN_STAGE_IDS[number]]["state"];

export interface DagExecutionStageV2 {
  stage: typeof PLAN_STAGE_IDS[number];
  state: DagExecutionStageStateV2;
}

export interface DagExecutionNodeV2 extends Omit<DagExecutionNodeV1, "worker"> {
  candidateGeneration: number;
  stages: DagExecutionStageV2[];
  worker: null | { workerId: string; attemptNumber: number; terminalStatus: string | null };
}

export interface DagExecutionProjectionV2 extends Omit<DagExecutionProjectionV1, "schemaVersion" | "kind" | "projectionVersion" | "nodes" | "projectionHash"> {
  schemaVersion: 2;
  kind: "DagExecutionProjectionV2";
  projectionVersion: "2";
  nodes: DagExecutionNodeV2[];
  projectionHash: string;
}

export function buildSchedulerPlanIndexV1(plan: CanonicalDagPlanV1): { schemaVersion: 1; kind: "SchedulerPlanIndexV1"; planHash: string; policyHash: string; workItems: unknown[]; precedence: unknown[]; trains: unknown[]; indexHash: string } {
  const core = {
    schemaVersion: 1 as const, kind: "SchedulerPlanIndexV1" as const, planHash: plan.planHash, policyHash: DAG_SCHEDULER_POLICY_HASH_V1,
    workItems: plan.workItems.map((item) => ({
      workItemId: item.workItemId, repositoryId: item.writeRepositoryId, risk: item.risk.tier,
      resourceDemands: item.resourceDemands.map((demand) => ({ ...demand, phases: [...demand.phases].sort() })).sort((a, b) => a.resourceClassId.localeCompare(b.resourceClassId)),
      mutexes: plan.constraints.semanticMutexes.filter((mutex) => mutex.members.some((member) => member.workItemId === item.workItemId)).map(({ mutexGroupId, members }) => ({ mutexGroupId, phases: members.find((member) => member.workItemId === item.workItemId)!.phases })).sort((a, b) => a.mutexGroupId.localeCompare(b.mutexGroupId)),
    })).sort((a, b) => a.workItemId.localeCompare(b.workItemId)),
    precedence: plan.constraints.precedence.map(({ precedenceId, predecessorWorkItemId, successorWorkItemId, releaseDisposition }) => ({ precedenceId, predecessorWorkItemId, successorWorkItemId, releaseDisposition })).sort((a, b) => a.precedenceId.localeCompare(b.precedenceId)),
    trains: plan.constraints.integrationTrains.map(({ trainId, repositoryId, members }) => ({ trainId, repositoryId, members: [...members].sort((a, b) => a.ordinal - b.ordinal || a.workItemId.localeCompare(b.workItemId)) })).sort((a, b) => a.repositoryId.localeCompare(b.repositoryId)),
  };
  return { ...core, indexHash: canonicalHash(core) };
}

export function scheduleDagRunV1(plan: CanonicalDagPlanV1, state: DagRunStateV1): DagSchedulerDecisionV1 {
  if (state.identity.planHash !== plan.planHash) throw new Error("Scheduler plan/run identity mismatch");
  if (dagRunSnapshotHash(state) !== state.snapshotHash) throw new Error("Scheduler requires an exact hash-valid run snapshot");
  const index = buildSchedulerPlanIndexV1(plan);
  if (state.scheduler.policyVersion !== DAG_SCHEDULER_POLICY_VERSION_V1 || state.scheduler.policyHash !== DAG_SCHEDULER_POLICY_HASH_V1) throw new Error("Run scheduler policy binding differs from sticky-lanes-v1");
  if (state.scheduler.normalizedIndexHash !== index.indexHash) throw new Error("Run normalized scheduler index binding is stale");

  const activeLaneWorkItemIds = Object.values(state.scheduler.activeNodeLanes).filter(({ releaseDisposition }) => releaseDisposition === null).sort((a, b) => a.admissionSequence - b.admissionSequence || a.workItemId.localeCompare(b.workItemId)).map(({ workItemId }) => workItemId);
  const slots = plan.workItems.map((item) => deriveSlot(plan, state, item.workItemId)).filter((slot): slot is DagSchedulerSlotV1 => Boolean(slot));
  const tentativeResources = Object.fromEntries(Object.entries(state.resourcePools).map(([id, pool]) => [id, pool.allocatedUnits]));
  const tentativeMutexes = new Set(Object.values(state.scheduler.reservations).filter((reservation) => !["released", "fenced"].includes(reservation.state)).flatMap(({ mutexGroupIds }) => mutexGroupIds));
  const tentativeOperational = Object.fromEntries(Object.entries(state.scheduler.operationalCapacities).map(([id, pool]) => [id, pool.allocatedUnits]));
  const selected: DagSchedulerReservationProposalV1[] = [];
  const lanes = new Set(activeLaneWorkItemIds);
  const remaining = slots.filter(({ correctnessReady }) => correctnessReady);
  while (true) {
    let evaluated = remaining.filter((slot) => !selected.some(({ slotId }) => slotId === slot.slotId)).map((slot) => withAdmission(state, slot, selected, lanes, tentativeResources, tentativeOperational, tentativeMutexes));
    const fairnessWaiter = evaluated.filter(({ priority, admissible }) => priority.fairnessReserved && !admissible).sort(compareSlots)[0];
    if (fairnessWaiter) evaluated = evaluated.map((candidate) => candidate.slotId === fairnessWaiter.slotId || activeLaneWorkItemIds.includes(candidate.workItemId) || !competesForMissingCapacity(fairnessWaiter, candidate) ? candidate : { ...candidate, admissible: false, admissionCodes: [...new Set([...candidate.admissionCodes, "FAIRNESS_RESERVATION"])] as DagSchedulerAdmissionCodeV1[] });
    const candidates = evaluated.sort(compareSlots);
    const chosen = candidates.find(({ admissible }) => admissible);
    if (!chosen) { for (const candidate of evaluated) replaceSlot(slots, candidate); break; }
    replaceSlot(slots, chosen);
    if (!lanes.has(chosen.workItemId)) lanes.add(chosen.workItemId);
    for (const [id, units] of Object.entries(chosen.resourceUnits)) tentativeResources[id] = (tentativeResources[id] ?? 0) + units;
    for (const [id, units] of Object.entries(chosen.operationalUnits)) tentativeOperational[id] = (tentativeOperational[id] ?? 0) + units;
    chosen.mutexGroupIds.forEach((id) => tentativeMutexes.add(id));
    const reservationSequence = state.scheduler.nextReservationSequence + selected.length;
    const normalizedRequestHash = canonicalHash({ runId: state.runId, runNonce: state.runNonce, ownerEpoch: state.owner.ownerEpoch, revision: state.revision, workItemId: chosen.workItemId, stage: chosen.stage, itemGeneration: chosen.itemGeneration, attemptOrdinal: chosen.attemptOrdinal, operationKind: chosen.operationKind });
    selected.push({
      reservationId: `reservation-${canonicalHash({ runNonce: state.runNonce, decisionSequence: state.scheduler.decisionSequence + 1, reservationSequence, slotId: chosen.slotId }).slice(7, 31)}`,
      reservationSequence, slotId: chosen.slotId, workItemId: chosen.workItemId, repositoryId: chosen.repositoryId,
      stage: chosen.stage, operationKind: chosen.operationKind, itemGeneration: chosen.itemGeneration, attemptOrdinal: chosen.attemptOrdinal,
      normalizedRequestHash, mutexGroupIds: chosen.mutexGroupIds, resourceUnits: chosen.resourceUnits, operationalUnits: chosen.operationalUnits, workerRole: workerRole(chosen.operationKind),
    });
  }
  const selectedSlots = selected.map(({ slotId }) => slots.find((slot) => slot.slotId === slotId)!).filter(Boolean);
  const bypassIncrements = slots.filter((slot) => slot.correctnessReady && !selected.some(({ slotId }) => slotId === slot.slotId) && selectedSlots.some((selectedSlot) => competingAdmission(slot, selectedSlot))).map(({ slotId }) => slotId).sort();
  const attention = slots.some(({ blockerCodes }) => blockerCodes.some((code) => ["OWNER_UNATTACHED", "FRESHNESS_BLOCK", "INTEGRATION_DRIFT", "ATTEMPT_ACTIVE"].includes(code)));
  const core = {
    schemaVersion: 1 as const, kind: "DagSchedulerDecisionV1" as const, policyVersion: DAG_SCHEDULER_POLICY_VERSION_V1,
    policyHash: DAG_SCHEDULER_POLICY_HASH_V1, normalizedIndexHash: index.indexHash, planHash: plan.planHash,
    runId: state.runId, runNonceHash: canonicalHash(state.runNonce), inputRevision: state.revision, inputSnapshotHash: state.snapshotHash,
    ownerEpoch: state.owner.ownerEpoch, decisionSequence: state.scheduler.decisionSequence + 1, maxActiveNodes: state.scheduler.maxActiveNodes,
    activeLaneWorkItemIds, frontier: slots.slice().sort(compareSlots), selected,
    bypassIncrements,
    notice: noticeFor(state, slots, selected.length, attention),
  };
  return { ...core, decisionHash: canonicalHash(core) };
}

export function requireSchedulerDispatchIntentV1(state: DagRunStateV1, request: { effectId: string; kind: string; requestHash: string }): DagRunStateV1["scheduler"]["reservations"][string] {
  if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") throw new Error("needs_replan blocks scheduler dispatch");
  const reservation = state.scheduler.reservations[request.effectId];
  if (!request.kind.startsWith("scheduler_") || !reservation || request.kind !== `scheduler_${reservation.operationKind}` || reservation.normalizedRequestHash !== request.requestHash || reservation.state !== "dispatch_intent") throw new Error("Scheduler dispatch does not bind one exact durable dispatch intent");
  return reservation;
}

export function projectDagExecutionV1(plan: CanonicalDagPlanV1, state: DagRunStateV1, decision: DagSchedulerDecisionV1, workerInput: DagWorkerProjectionInputV1 | null = null): DagExecutionProjectionV1 {
  if (decision.inputSnapshotHash !== state.snapshotHash || decision.planHash !== plan.planHash) throw new Error("Execution projection requires one exact plan/run/scheduler join");
  const slots = new Map(decision.frontier.map((slot) => [slot.workItemId, slot])); const schedulerOrder = new Map(decision.frontier.map((slot, index) => [slot.workItemId, index]));
  const width = Math.max(2, String(plan.workItems.length).length);
  let omittedWorkers = 0; const joinedWorkerFacts: Array<{ storageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; terminalStatus: string | null; resultHash: string | null }> = [];
  const nodes = plan.workItems.map((planItem, ordinal) => {
    const item = state.workItems[planItem.workItemId]; const slot = slots.get(planItem.workItemId);
    const currentAttempt = item.currentStage ? state.stageAttempts[item.stages[item.currentStage].currentAttemptId ?? ""] : undefined;
    const attemptInFlight = Boolean(currentAttempt && ["launching", "running", "settling", "cancelling"].includes(currentAttempt.state));
    const attemptTerminal = Boolean(currentAttempt && ["result_observed", "evidence_pending", "sealed", "cancelled", "failed", "lost", "ambiguous", "quarantined"].includes(currentAttempt.state));
    const reservationInFlight = Object.values(state.scheduler.reservations).some((reservation) => reservation.workItemId === item.workItemId && reservation.stage === item.currentStage && ["dispatch_intent", "active"].includes(reservation.state));
    const effectInFlight = Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && effect.state === "dispatching");
    const attemptNeedsAttention = Boolean(currentAttempt && ["failed", "lost", "ambiguous", "quarantined"].includes(currentAttempt.state)) || Boolean(item.currentStage && ["failed", "budget_exhausted"].includes(item.stages[item.currentStage].state));
    const binding = currentAttempt ? state.workerBindings[currentAttempt.stageAttemptId] : undefined;
    let worker: DagExecutionNodeV1["worker"] = null;
    if (binding && workerInput) {
      const exact = workerInput.workers.find((candidate) => candidate.storageId === binding.workerStorageId && candidate.launchOwnerSessionId === binding.launchOwnerSessionId && candidate.workerId === binding.workerId && candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash && candidate.resultHash === binding.resultHash);
      if (exact) { worker = { workerId: exact.workerId, attemptNumber: exact.attemptNumber, terminalStatus: exact.terminalStatus }; joinedWorkerFacts.push(exact); } else omittedWorkers += 1;
    }
    const train = state.integrationTrains[item.writeRepositoryId]; const entryId = item.integrationEntryId; const position = entryId ? train?.entryOrder.indexOf(entryId) ?? -1 : -1;
    const retryCount = Object.values(state.retryLedger).filter((entry) => entry.workItemId === item.workItemId).reduce((sum, entry) => sum + entry.count, 0);
    const node: DagExecutionNodeV1 = {
      alias: `N${String(ordinal + 1).padStart(width, "0")}`, workItemId: item.workItemId, title: planItem.title,
      repositoryId: item.writeRepositoryId, current: item.current, stage: item.currentStage, glyph: glyphFor(item, slot, attemptInFlight || (!attemptTerminal && (reservationInFlight || effectInFlight)), attemptNeedsAttention),
      correctnessReady: slot?.correctnessReady ?? false, admissible: slot?.admissible ?? false, schedulerOrder: schedulerOrder.get(item.workItemId) ?? null,
      blockerCodes: slot?.blockerCodes ?? [], blockerIds: [...new Set([...(slot?.blockerIds ?? []), ...item.blockerIds])].sort(),
      activeLane: Boolean(state.scheduler.activeNodeLanes[item.workItemId]?.releaseDisposition === null), laneAdmissionSequence: item.laneAdmissionSequence, admittedAt: item.admittedAt,
      retryCount, findingCount: item.openFindingIds.length, integrationPosition: position >= 0 ? position : null, worker,
    };
    return node;
  });
  const repositoryProjectionHash = canonicalHash(Object.values(state.repositories).map(({ repositoryId, observedTarget, observationReceipt, workspace }) => ({ repositoryId, observedTarget, observationReceipt, workspace })).sort((a, b) => a.repositoryId.localeCompare(b.repositoryId)));
  const core = {
    schemaVersion: 1 as const, kind: "DagExecutionProjectionV1" as const, projectionVersion: "1" as const,
    planHash: plan.planHash, runId: state.runId, runRevision: state.revision, runSnapshotHash: state.snapshotHash,
    workerProjectionHash: canonicalHash({ bindings: Object.values(state.workerBindings).map(({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash }) => ({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash })).sort((a, b) => a.workerStorageId.localeCompare(b.workerStorageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber), workers: joinedWorkerFacts.sort((a, b) => a.storageId.localeCompare(b.storageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber) }), repositoryProjectionHash, schedulerDecisionHash: decision.decisionHash,
    desired: state.desired.run, current: state.current.run, completion: state.completion.state,
    staleReadOnly: !["valid_exact", "valid_revalidated"].includes(state.freshness.class), nodes,
    precedence: plan.constraints.precedence.map((edge) => ({ precedenceId: edge.precedenceId, from: edge.predecessorWorkItemId, to: edge.successorWorkItemId, state: state.precedence[edge.precedenceId]?.state ?? "waiting" })).sort((a, b) => a.precedenceId.localeCompare(b.precedenceId)),
    trainHeads: Object.values(state.integrationTrains).map((train) => { const entryId = train.entryOrder.find((id) => train.entries[id]?.state !== "integrated") ?? null; return { repositoryId: train.repositoryId, entryId, workItemId: entryId ? train.entries[entryId]?.workItemId ?? null : null, acceptedPrefixOrdinal: train.acceptedPrefixOrdinal }; }).sort((a, b) => a.repositoryId.localeCompare(b.repositoryId)),
    summary: { ready: nodes.filter(({ correctnessReady }) => correctnessReady).length, activeLanes: nodes.filter(({ activeLane }) => activeLane).length, attention: nodes.filter(({ glyph }) => glyph === "!" || glyph === "?").length, integrationReady: nodes.filter(({ glyph }) => glyph === "+").length, complete: nodes.filter(({ glyph }) => glyph === "#").length, omittedWorkers },
  };
  return { ...core, projectionHash: canonicalHash(core) };
}

export function projectDagExecutionV2(plan: CanonicalDagPlanV1, state: DagRunStateV1, decision: DagSchedulerDecisionV1, workerInput: DagWorkerProjectionInputV1 | null = null): DagExecutionProjectionV2 {
  const v1 = projectDagExecutionV1(plan, state, decision, workerInput);
  const nodes = v1.nodes.map((node): DagExecutionNodeV2 => {
    const item = state.workItems[node.workItemId];
    const currentAttempt = item.currentStage ? state.stageAttempts[item.stages[item.currentStage].currentAttemptId ?? ""] : undefined;
    return {
      ...node,
      candidateGeneration: item.candidateGeneration,
      stages: PLAN_STAGE_IDS.map((stage) => ({ stage, state: item.stages[stage].state })),
    };
  });
  const core = {
    schemaVersion: 2 as const,
    kind: "DagExecutionProjectionV2" as const,
    projectionVersion: "2" as const,
    planHash: v1.planHash,
    runId: v1.runId,
    runRevision: v1.runRevision,
    runSnapshotHash: v1.runSnapshotHash,
    workerProjectionHash: v1.workerProjectionHash,
    repositoryProjectionHash: v1.repositoryProjectionHash,
    schedulerDecisionHash: v1.schedulerDecisionHash,
    desired: v1.desired,
    current: v1.current,
    completion: v1.completion,
    staleReadOnly: v1.staleReadOnly,
    nodes,
    precedence: v1.precedence,
    trainHeads: v1.trainHeads,
    summary: v1.summary,
  };
  return { ...core, projectionHash: canonicalHash(core) };
}

function deriveSlot(plan: CanonicalDagPlanV1, state: DagRunStateV1, workItemId: string): DagSchedulerSlotV1 | null {
  const item = state.workItems[workItemId]; const planItem = plan.workItems.find((candidate) => candidate.workItemId === workItemId)!;
  if (["complete", "cancelled", "superseded"].includes(item.current)) return null;
  let stage = PLAN_STAGE_IDS.find((id) => item.stages[id].state !== "passed") ?? "F8";
  let operationKind = operationFor(stage);
  if (item.current === "integration_ready" || item.current === "integrating") { stage = "F8"; operationKind = "integration"; }
  const blockers: Array<{ code: DagSchedulerBlockerCodeV1; id: string }> = [];
  if (!state.owner.sessionId || !state.owner.lockIdentity) blockers.push({ code: "OWNER_UNATTACHED", id: "run-owner" });
  const needsReplan = dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan";
  if (needsReplan) blockers.push({ code: "NEEDS_REPLAN", id: "run-needs-replan" });
  else if (state.desired.run !== "running" || !["active", "integration"].includes(state.current.run)) blockers.push({ code: "RUN_NOT_RUNNING", id: state.current.run });
  if (state.freshness.blocksNewLaunches || !["valid_exact", "valid_revalidated"].includes(state.freshness.class)) blockers.push({ code: state.freshness.class === "integration_drift" ? "INTEGRATION_DRIFT" : "FRESHNESS_BLOCK", id: state.freshness.receipt.hash });
  if (item.desired !== "run" || ["cancelled", "superseded"].includes(item.current)) blockers.push({ code: "ITEM_NOT_RUNNABLE", id: workItemId });
  if (!item.authorizedStages.includes(stage)) blockers.push({ code: "AUTHORIZATION", id: stage });
  for (const id of item.precedenceIds) if (state.precedence[id]?.state !== "satisfied") blockers.push({ code: "PRECEDENCE", id });
  for (const id of item.gateIds) {
    const gate = state.gates[id]; const planGate = plan.gates.find((candidate) => candidate.gateId === id);
    if (gate?.state !== "released" && planGate?.blocks.some((block) => block.workItemId === workItemId && block.stages.includes(stage))) blockers.push({ code: "GATE", id });
  }
  item.blockerIds.forEach((id) => blockers.push({ code: "ITEM_BLOCKER", id }));
  item.stages[stage].blockerIds.forEach((id) => blockers.push({ code: "STAGE_BLOCKER", id }));
  const attempt = item.stages[stage].currentAttemptId ? state.stageAttempts[item.stages[stage].currentAttemptId!] : null;
  if (attempt && !["sealed", "cancelled", "failed", "lost", "quarantined"].includes(attempt.state)) blockers.push({ code: "ATTEMPT_ACTIVE", id: attempt.stageAttemptId });
  for (const effect of Object.values(state.effects)) if (effect.subject.kind === "work_item" && effect.subject.id === workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation)) blockers.push({ code: "EFFECT_UNRECONCILED", id: effect.effectId });
  state.scheduler.providerHoldIds.forEach((id) => blockers.push({ code: "PROVIDER_HOLD", id }));
  if (operationKind === "integration") {
    const train = state.integrationTrains[item.writeRepositoryId]; const entryId = item.integrationEntryId;
    const head = train?.entryOrder.find((id) => train.entries[id]?.state !== "integrated");
    if (!entryId || head !== entryId) blockers.push({ code: "INTEGRATION_NOT_HEAD", id: head ?? item.writeRepositoryId });
    if (state.freshness.blocksIntegration) blockers.push({ code: "INTEGRATION_DRIFT", id: state.freshness.receipt.hash });
  }
  const mutexGroupIds = plan.constraints.semanticMutexes.filter((mutex) => mutex.members.some((member) => member.workItemId === workItemId && member.phases.includes(stage))).map(({ mutexGroupId }) => mutexGroupId).sort();
  const resourceUnits = Object.fromEntries(planItem.resourceDemands.filter(({ phases }) => phases.includes(stage)).map(({ resourceClassId, units }) => [resourceClassId, units]).sort(([left], [right]) => left.localeCompare(right)));
  const stageOrdinal = PLAN_STAGE_IDS.indexOf(stage);
  const train = plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === item.writeRepositoryId); const trainPosition = train?.members.find(({ workItemId: id }) => id === workItemId)?.ordinal ?? null;
  const bypassCount = state.scheduler.bypassCounters[slotKey(workItemId, item.candidateGeneration, stage, operationKind)] ?? 0;
  const slot: DagSchedulerSlotV1 = {
    slotId: slotKey(workItemId, item.candidateGeneration, stage, operationKind), workItemId, repositoryId: item.writeRepositoryId,
    stage, operationKind, itemGeneration: item.candidateGeneration, attemptOrdinal: item.stages[stage].attemptIds.length + 1,
    correctnessReady: blockers.length === 0, blockerCodes: [...new Set(blockers.map(({ code }) => code))].sort() as DagSchedulerBlockerCodeV1[], blockerIds: [...new Set(blockers.map(({ id }) => id))].sort(),
    admissible: false, admissionCodes: [], mutexGroupIds, resourceUnits, operationalUnits: operationalDemand(operationKind, item.writeRepositoryId),
    priority: { fairnessReserved: bypassCount >= DAG_SCHEDULER_POLICY_V1.fairnessReservationBypasses, launchClass: launchClass(operationKind, stage, item.stages[stage].state), riskOrdinal: 0, criticalHeight: criticalHeight(plan, state, workItemId, stageOrdinal), phaseProgress: stageOrdinal, bypassCount, trainPosition },
  };
  return slot;
}

function withAdmission(state: DagRunStateV1, slot: DagSchedulerSlotV1, selected: DagSchedulerReservationProposalV1[], lanes: Set<string>, resources: Record<string, number>, operational: Record<string, number>, mutexes: Set<string>): DagSchedulerSlotV1 {
  const codes: DagSchedulerAdmissionCodeV1[] = [];
  if (!lanes.has(slot.workItemId) && lanes.size >= state.scheduler.maxActiveNodes) codes.push("LANE_CAPACITY");
  if (Object.values(state.scheduler.reservations).some((reservation) => reservation.workItemId === slot.workItemId && !["released", "fenced"].includes(reservation.state))) codes.push("EXISTING_RESERVATION");
  if (slot.mutexGroupIds.some((id) => mutexes.has(id))) codes.push("MUTEX");
  for (const [id, units] of Object.entries(slot.resourceUnits)) { const pool = state.resourcePools[id]; if (!pool || (resources[id] ?? 0) + units > Math.min(pool.observedCapacity, pool.semanticMaximum)) codes.push("RESOURCE_CAPACITY"); }
  for (const [id, units] of Object.entries(slot.operationalUnits)) { const pool = state.scheduler.operationalCapacities[id]; if (!pool || (operational[id] ?? 0) + units > pool.observedCapacity) codes.push("OPERATIONAL_CAPACITY"); }
  if (Object.values(state.scheduler.dynamicExclusions).some((exclusion) => exclusion.state === "active" && exclusion.workItemIds.includes(slot.workItemId) && exclusion.phases.includes(slot.stage) && exclusion.workItemIds.some((id) => (id !== slot.workItemId && state.scheduler.activeNodeLanes[id]?.releaseDisposition === null && state.workItems[id]?.currentStage !== null && exclusion.phases.includes(state.workItems[id].currentStage!) && lanePrecedes(state, id, slot.workItemId)) || Object.values(state.scheduler.reservations).some((reservation) => reservation.workItemId === id && exclusion.phases.includes(reservation.stage) && !["released", "fenced"].includes(reservation.state)) || selected.some((reservation) => reservation.workItemId === id && exclusion.phases.includes(reservation.stage))))) codes.push("DYNAMIC_EXCLUSION");
  return { ...slot, admissible: codes.length === 0, admissionCodes: [...new Set(codes)].sort() as DagSchedulerAdmissionCodeV1[] };
}

function compareSlots(left: DagSchedulerSlotV1, right: DagSchedulerSlotV1): number {
  const a = left.priority; const b = right.priority;
  return Number(b.fairnessReserved) - Number(a.fairnessReserved) || a.launchClass - b.launchClass || b.criticalHeight - a.criticalHeight || b.phaseProgress - a.phaseProgress || b.bypassCount - a.bypassCount || (a.trainPosition ?? Number.MAX_SAFE_INTEGER) - (b.trainPosition ?? Number.MAX_SAFE_INTEGER) || left.repositoryId.localeCompare(right.repositoryId) || left.workItemId.localeCompare(right.workItemId) || left.itemGeneration - right.itemGeneration || left.slotId.localeCompare(right.slotId);
}

function noticeFor(state: DagRunStateV1, slots: DagSchedulerSlotV1[], selected: number, attention: boolean): DagSchedulerDecisionV1["notice"] {
  if (state.completion.state !== "open") return "RUN_COMPLETED";
  if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return "NEEDS_REPLAN";
  if (state.desired.run === "paused" || state.current.run === "paused") return "PAUSED";
  if (state.freshness.class === "integration_drift") return "INTEGRATION_DRIFT";
  if (selected) return "RESERVATIONS_PROPOSED";
  if (Object.values(state.scheduler.reservations).some(({ state: reservationState }) => !["released", "fenced"].includes(reservationState))) return "AWAITING_ACTIVE";
  if (attention) return "NEEDS_HUMAN";
  if (slots.some(({ correctnessReady }) => correctnessReady)) return "CAPACITY_OR_EXCLUSION_BLOCKED";
  if (slots.some(({ blockerCodes }) => blockerCodes.includes("GATE"))) return "WAITING_EXTERNAL";
  return "RUN_BLOCKED";
}

function glyphFor(item: DagRunStateV1["workItems"][string], slot: DagSchedulerSlotV1 | undefined, operationActive: boolean, attemptNeedsAttention: boolean): DagExecutionNodeV1["glyph"] {
  if (["cancelled", "superseded"].includes(item.current)) return "x";
  if (item.current === "complete") return "#";
  if (attemptNeedsAttention || item.blockerIds.length || item.openFindingIds.length || slot?.blockerCodes.some((code) => ["OWNER_UNATTACHED", "NEEDS_REPLAN", "FRESHNESS_BLOCK", "INTEGRATION_DRIFT"].includes(code))) return "!";
  if (slot?.blockerCodes.includes("GATE")) return "?";
  if (item.current === "integrating") return "@";
  if (item.current === "integration_ready") return "+";
  if (operationActive) return "*";
  if (item.laneAdmissionSequence !== null) return ":";
  if (slot?.correctnessReady) return ">";
  return ".";
}

function criticalHeight(plan: CanonicalDagPlanV1, state: DagRunStateV1, workItemId: string, stageOrdinal: number, seen = new Set<string>()): number {
  if (seen.has(workItemId)) return 0; seen.add(workItemId);
  const remaining = PLAN_STAGE_IDS.length - stageOrdinal + 1;
  const successors = plan.constraints.precedence.filter(({ predecessorWorkItemId }) => predecessorWorkItemId === workItemId).map(({ successorWorkItemId }) => successorWorkItemId).filter((id) => state.workItems[id]?.current !== "complete").sort();
  return remaining + Math.max(0, ...successors.map((id) => criticalHeight(plan, state, id, 0, new Set(seen))));
}

function operationalDemand(operation: DagSchedulerSlotV1["operationKind"], repositoryId: string): Record<string, number> {
  if (operation === "conductor") return {};
  if (operation === "integration") return { [`repository-integration:${repositoryId}`]: 1 };
  const role = operation === "implementation" || operation === "codification" || operation === "hardening" ? "implementation" : operation === "evaluation" ? "evaluation" : operation === "review" ? "review" : "check";
  return { "worker.process": 1, [`role:${role}`]: 1, [`repository-worktree:${repositoryId}`]: 1 };
}
function lanePrecedes(state: DagRunStateV1, leftId: string, rightId: string): boolean { const left = state.scheduler.activeNodeLanes[leftId]; const right = state.scheduler.activeNodeLanes[rightId]; if (!left || left.releaseDisposition !== null) return false; if (!right || right.releaseDisposition !== null) return true; return left.admissionSequence < right.admissionSequence || (left.admissionSequence === right.admissionSequence && leftId.localeCompare(rightId) < 0); }
function competingAdmission(waiter: DagSchedulerSlotV1, selected: DagSchedulerSlotV1): boolean {
  if (waiter.mutexGroupIds.some((id) => selected.mutexGroupIds.includes(id))) return true;
  if (Object.keys(waiter.resourceUnits).some((id) => (selected.resourceUnits[id] ?? 0) > 0)) return true;
  if (Object.keys(waiter.operationalUnits).some((id) => (selected.operationalUnits[id] ?? 0) > 0)) return true;
  return false;
}
function competesForMissingCapacity(waiter: DagSchedulerSlotV1, candidate: DagSchedulerSlotV1): boolean { return competingAdmission(waiter, candidate); }

function launchClass(operation: DagSchedulerSlotV1["operationKind"], stage: string, stageState: string): number { if (operation === "integration") return 0; if (["F2", "F4", "F5", "F6", "F7", "F8"].includes(stage)) return 1; if (stageState === "failed" || stageState === "invalidated") return 2; return 3; }
function operationFor(stage: typeof PLAN_STAGE_IDS[number]): DagSchedulerSlotV1["operationKind"] { return ({ F0: "conductor", F1: "implementation", F2: "evaluation", F3: "codification", F4: "verification", F5: "review", F6: "hardening", F7: "verification", F8: "conductor" } as const)[stage]; }
function workerRole(operation: DagSchedulerSlotV1["operationKind"]): DagSchedulerReservationProposalV1["workerRole"] { return operation === "implementation" || operation === "codification" || operation === "hardening" ? "implementation" : operation === "evaluation" ? "evaluator" : operation === "review" ? "reviewer" : "none"; }
function slotKey(workItemId: string, generation: number, stage: string, operationKind: string): string { return `${workItemId}:${generation}:${stage}:${operationKind}`; }
function replaceSlot(slots: DagSchedulerSlotV1[], replacement: DagSchedulerSlotV1): void { const index = slots.findIndex(({ slotId }) => slotId === replacement.slotId); if (index >= 0) slots[index] = replacement; }
