import { Type, type Static } from "typebox";
import {
  GitTreeRefV1Schema,
  HashSchema,
  IdSchema,
  NonNegativeIntegerSchema,
  PositiveIntegerSchema,
  StrictObject,
  TimestampSchema,
  canonicalHash,
  canonicalStringify,
  parseStrictJson,
  schemaIssues,
  utcTimestampOrderValue,
  validateTimestampFields,
  type ValidationIssue,
} from "./common.ts";
import { PLAN_STAGE_IDS } from "./plan.ts";
import { scheduleDagRunV1 } from "./scheduler.ts";
import {
  EffectProjectionV1Schema,
  HashRefV1Schema,
  QuarantineProjectionV1Schema,
  assertDagRunStateV1,
  dagRunSnapshotHash,
  type DagRunStateV1,
  type DagRunValidationContextV1,
} from "./run-state.ts";

const RunDesiredSchema = Type.Enum(["running", "paused"]);
const PlanStageSchema = Type.Enum(["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"]);
const SchedulerOperationSchema = Type.Enum(["conductor", "implementation", "evaluation", "codification", "verification", "review", "hardening", "integration"]);

const AttachOwnerPayloadSchema = StrictObject({
  ownerTokenHash: HashSchema, sessionId: IdSchema, pid: PositiveIntegerSchema,
  processStartIdentity: Type.String({ minLength: 1, maxLength: 256 }), lockIdentity: HashSchema,
  ownershipReceipt: HashSchema, priorOwnerDisposition: Type.Enum(["absent", "dead", "same_manager"]),
});
const ReleaseOwnerPayloadSchema = StrictObject({ reason: Type.String({ minLength: 1, maxLength: 4096 }) });
const SetDesiredPayloadSchema = StrictObject({
  desired: RunDesiredSchema, reason: Type.Union([Type.String({ minLength: 1, maxLength: 4096 }), Type.Null()]),
  requestedBy: Type.Enum(["user", "conductor", "successor_plan"]),
});
const PutEffectPayloadSchema = StrictObject({ effect: EffectProjectionV1Schema });
const MarkEffectDispatchingPayloadSchema = StrictObject({ effectId: IdSchema, expectedDispatchCount: NonNegativeIntegerSchema });
const RetryEffectDispatchPayloadSchema = StrictObject({ effectId: IdSchema, expectedDispatchCount: PositiveIntegerSchema, reason: Type.Literal("uncertain_acknowledgement") });
const RecordEffectObservationPayloadSchema = StrictObject({
  effectId: IdSchema, observationHash: HashSchema,
  reconciliation: Type.Enum(["applied_exact", "compensated", "proven_absent", "conflict", "unknown"]),
  terminalState: Type.Enum(["observed", "reconciled", "failed", "ambiguous"]),
});
const RequestCancellationPayloadSchema = StrictObject({
  cancellationId: IdSchema, scope: Type.Enum(["run", "work_item"]), subjectId: IdSchema,
  reason: Type.String({ minLength: 1, maxLength: 4096 }), workItemIds: Type.Array(IdSchema, { minItems: 1 }),
  effects: Type.Array(EffectProjectionV1Schema),
});
const RecordCancellationPayloadSchema = StrictObject({
  cancellationId: IdSchema, resultHash: HashSchema,
  effectObservations: Type.Array(StrictObject({ effectId: IdSchema, observationHash: HashSchema })),
  workerResults: Type.Array(StrictObject({ stageAttemptId: IdSchema, result: HashRefV1Schema })),
});
const QuarantineFactPayloadSchema = StrictObject({ quarantine: QuarantineProjectionV1Schema });
const AdoptQuarantinePayloadSchema = StrictObject({ quarantineId: IdSchema, adoptionReceipt: HashSchema });
const RejectQuarantinePayloadSchema = StrictObject({ quarantineId: IdSchema, reason: Type.String({ minLength: 1, maxLength: 4096 }) });
const SchedulerReservationPayloadSchema = StrictObject({
  reservationId: IdSchema, reservationSequence: PositiveIntegerSchema, slotId: IdSchema, workItemId: IdSchema,
  repositoryId: IdSchema, stage: PlanStageSchema, operationKind: SchedulerOperationSchema,
  itemGeneration: NonNegativeIntegerSchema, attemptOrdinal: PositiveIntegerSchema, normalizedRequestHash: HashSchema,
  mutexGroupIds: Type.Array(IdSchema), resourceUnits: Type.Record(IdSchema, NonNegativeIntegerSchema), operationalUnits: Type.Record(IdSchema, NonNegativeIntegerSchema),
  workerRole: Type.Enum(["none", "implementation", "evaluator", "reviewer"]),
});
const ReserveSchedulerBatchPayloadSchema = StrictObject({
  decisionHash: HashSchema, decisionSequence: PositiveIntegerSchema, policyHash: HashSchema, normalizedIndexHash: HashSchema,
  inputSnapshotHash: HashSchema, reservations: Type.Array(SchedulerReservationPayloadSchema, { minItems: 1 }), bypassSlotIds: Type.Array(IdSchema),
});
const MarkSchedulerReservationDispatchPayloadSchema = StrictObject({ reservationId: IdSchema, normalizedRequestHash: HashSchema });
const RecordSchedulerReservationDispatchPayloadSchema = StrictObject({ reservationId: IdSchema, normalizedRequestHash: HashSchema, disposition: Type.Enum(["active", "launch_ambiguous"]) });
const ReleaseSchedulerReservationPayloadSchema = StrictObject({ reservationId: IdSchema, disposition: Type.Enum(["released", "fenced"]), reason: Type.String({ minLength: 1, maxLength: 4096 }) });
const AuthorizeRetryPayloadSchema = StrictObject({ retryKey: HashSchema, expectedCount: NonNegativeIntegerSchema, workItemId: IdSchema, stage: PlanStageSchema, dimension: Type.Enum(["product_repair", "test_rework", "review_rework", "hardening_rework", "infrastructure", "worker_replacement", "integration"]), fingerprint: HashSchema, candidateGeneration: NonNegativeIntegerSchema });
const ReserveIntegrationAttemptPayloadSchema = StrictObject({
  integrationAttemptId: IdSchema, entryId: IdSchema, repositoryId: IdSchema, workItemId: IdSchema, retryOrdinal: NonNegativeIntegerSchema, retryAuthorizationKey: Type.Union([HashSchema, Type.Null()]),
  sourceCandidateHash: HashSchema, sourceBase: GitTreeRefV1Schema, sourceCandidate: GitTreeRefV1Schema,
  expectedPrefix: GitTreeRefV1Schema, expectedTarget: GitTreeRefV1Schema, temporaryRef: Type.String({ minLength: 1, maxLength: 512 }),
  repositoryBindingFactHash: HashSchema, lockLeaseId: IdSchema, compositionEffect: EffectProjectionV1Schema,
});
const RecordGitCompositionPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, compositionFactHash: HashSchema, composedTree: GitTreeRefV1Schema, syntheticParentCommit: Type.String({ pattern: "^(?:[0-9a-f]{40}|[0-9a-f]{64})$" }), sourceToIntegratedLineageHash: HashSchema, conflictClass: Type.Enum(["none", "mechanical", "semantic", "ambiguous"]), privateRefFactHashes: Type.Array(HashSchema, { minItems: 1 }) });
const RecordGitCompositionConflictPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, compositionFactHash: HashSchema, conflictClass: Type.Enum(["mechanical", "semantic", "ambiguous"]) });
const RecordProposalVerificationPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, proposalVerificationFactHash: HashSchema, prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema });
const PrepareGitLandingPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, landingEffect: EffectProjectionV1Schema, intendedLandedTree: GitTreeRefV1Schema });
const RecordGitLandingPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, landingObservationFactHash: HashSchema, reconciliation: Type.Enum(["applied_exact", "proven_absent", "conflict", "unknown"]) });
const AcceptIntegrationReceiptPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, integrationReceiptHash: HashSchema, transactionReceiptHash: HashSchema, transactionReceiptFactHash: HashSchema });

const variant = <T extends string, P extends Record<string, any>>(type: T, kind: "command" | "observation", payload: P) => StrictObject({
  schemaVersion: Type.Literal(1), kind: Type.Literal(kind), type: Type.Literal(type),
  commandId: IdSchema, idempotencyKey: Type.String({ minLength: 1, maxLength: 256 }), payloadHash: HashSchema,
  runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }), expectedRevision: NonNegativeIntegerSchema,
  expectedSnapshotHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema, occurredAt: TimestampSchema, payload,
});

export const DagRunInputV1Schema = Type.Union([
  variant("attach_owner", "observation", AttachOwnerPayloadSchema),
  variant("transfer_owner", "command", AttachOwnerPayloadSchema),
  variant("release_owner", "command", ReleaseOwnerPayloadSchema),
  variant("set_desired_run", "command", SetDesiredPayloadSchema),
  variant("put_effect_intent", "command", PutEffectPayloadSchema),
  variant("mark_effect_dispatching", "command", MarkEffectDispatchingPayloadSchema),
  variant("retry_effect_dispatch", "command", RetryEffectDispatchPayloadSchema),
  variant("record_effect_observation", "observation", RecordEffectObservationPayloadSchema),
  variant("request_cancellation", "command", RequestCancellationPayloadSchema),
  variant("record_cancellation", "observation", RecordCancellationPayloadSchema),
  variant("quarantine_fact", "observation", QuarantineFactPayloadSchema),
  variant("adopt_quarantined_fact", "observation", AdoptQuarantinePayloadSchema),
  variant("reject_quarantined_fact", "command", RejectQuarantinePayloadSchema),
  variant("reserve_scheduler_batch", "command", ReserveSchedulerBatchPayloadSchema),
  variant("mark_scheduler_reservation_dispatch", "command", MarkSchedulerReservationDispatchPayloadSchema),
  variant("record_scheduler_reservation_dispatch", "observation", RecordSchedulerReservationDispatchPayloadSchema),
  variant("release_scheduler_reservation", "observation", ReleaseSchedulerReservationPayloadSchema),
  variant("authorize_retry", "command", AuthorizeRetryPayloadSchema),
  variant("reserve_integration_attempt", "command", ReserveIntegrationAttemptPayloadSchema),
  variant("record_git_composition", "observation", RecordGitCompositionPayloadSchema),
  variant("record_git_composition_conflict", "observation", RecordGitCompositionConflictPayloadSchema),
  variant("record_proposal_verification", "observation", RecordProposalVerificationPayloadSchema),
  variant("prepare_git_landing", "command", PrepareGitLandingPayloadSchema),
  variant("record_git_landing_reconciliation", "observation", RecordGitLandingPayloadSchema),
  variant("accept_integration_receipt", "observation", AcceptIntegrationReceiptPayloadSchema),
]);
export type DagRunInputV1 = Static<typeof DagRunInputV1Schema>;
export const DAG_RUN_INPUT_SCHEMA_HASH = canonicalHash(JSON.parse(JSON.stringify(DagRunInputV1Schema)));

export type DagRunRejectCodeV1 =
  | "INVALID_INPUT" | "IDENTITY_MISMATCH" | "IDEMPOTENCY_CONFLICT" | "STALE_REVISION"
  | "STALE_SNAPSHOT" | "STALE_OWNER" | "PRECONDITION_FAILED" | "INVALID_TRANSITION";
export interface DagRunEffectRequestV1 { effectId: string; kind: string; requestHash: string; }
export interface DagRunTransitionNoticeV1 { runId: string; revision: number; type: string; subjectId: string; hash: string | null; }
export type DagRunReducerResultV1 =
  | { accepted: true; duplicate: boolean; state: DagRunStateV1; effects: DagRunEffectRequestV1[]; notices: DagRunTransitionNoticeV1[] }
  | { accepted: false; code: DagRunRejectCodeV1; message: string; currentRevision: number; blockerIds: string[]; issues?: ValidationIssue[] };

export function parseDagRunInputV1(text: string): DagRunInputV1 {
  const value = parseStrictJson(text);
  const issues = validateDagRunInputV1(value);
  if (issues.length) throw new Error(`Invalid DagRunInputV1:\n${issues.map(({ path, message }) => `- ${path}: ${message}`).join("\n")}`);
  return value as DagRunInputV1;
}

export function validateDagRunInputV1(value: unknown): ValidationIssue[] {
  const issues = schemaIssues(DagRunInputV1Schema, value);
  if (issues.length || !value || typeof value !== "object") return issues;
  validateTimestampFields(value, issues);
  const input = value as any;
  if (input.payloadHash !== canonicalHash(input.payload)) issues.push({ path: "/payloadHash", message: "must equal canonical payload content" });
  return issues;
}

export function reduceDagRunV1(state: DagRunStateV1, inputValue: unknown, context: DagRunValidationContextV1): DagRunReducerResultV1 {
  try { assertDagRunStateV1(state, context); }
  catch (error) { return reject(state, "INVALID_TRANSITION", `current snapshot is invalid: ${error instanceof Error ? error.message : String(error)}`); }
  const inputIssues = validateDagRunInputV1(inputValue);
  if (inputIssues.length) return reject(state, "INVALID_INPUT", "input failed closed validation", inputIssues);
  const input = inputValue as DagRunInputV1;
  if (input.runId !== state.runId || input.runNonce !== state.runNonce) return reject(state, "IDENTITY_MISMATCH", "run identity does not match");
  const slotId = naturalSlotId(input);
  const duplicate = idempotencyDisposition(state, input, slotId);
  if (duplicate === "same") return { accepted: true, duplicate: true, state, effects: [], notices: [] };
  if (duplicate === "conflict") return reject(state, "IDEMPOTENCY_CONFLICT", "natural idempotency slot contains conflicting content");
  if (input.expectedRevision !== state.revision) return reject(state, "STALE_REVISION", "expected revision is stale");
  if (input.expectedSnapshotHash !== state.snapshotHash) return reject(state, "STALE_SNAPSHOT", "expected snapshot hash is stale");
  if (utcTimestampOrderValue(input.occurredAt) < utcTimestampOrderValue(state.updatedAt)) return reject(state, "PRECONDITION_FAILED", "input occurrence time cannot move snapshot time backward");
  if (input.type !== "attach_owner" && input.ownerEpoch !== state.owner.ownerEpoch) return reject(state, "STALE_OWNER", "owner epoch is stale");
  const factPrecondition = validateObservationFacts(state, input, context);
  if (factPrecondition) return reject(state, "PRECONDITION_FAILED", factPrecondition);

  const next = structuredClone(state) as DagRunStateV1;
  const effects: DagRunEffectRequestV1[] = [];
  const notices: DagRunTransitionNoticeV1[] = [];
  const precondition = applyInput(next, input, context, effects, notices);
  if (precondition) return reject(state, precondition.code, precondition.message);
  next.idempotencySlots[slotId] = { slotId, inputType: input.type, commandId: input.commandId, idempotencyKey: input.idempotencyKey, payloadHash: input.payloadHash, inputHash: canonicalHash(input), appliedRevision: state.revision + 1 };
  next.previousSnapshotHash = state.snapshotHash;
  next.revision = state.revision + 1;
  next.updatedAt = input.occurredAt;
  rederiveCurrent(next, input.commandId);
  next.snapshotHash = dagRunSnapshotHash(next);
  try {
    assertDagRunStateV1(next, context);
  } catch (error) {
    return reject(state, "INVALID_TRANSITION", error instanceof Error ? error.message : String(error));
  }
  for (const notice of notices) notice.revision = next.revision;
  return { accepted: true, duplicate: false, state: next, effects, notices };
}

function validateObservationFacts(state: DagRunStateV1, input: DagRunInputV1, context: DagRunValidationContextV1): string | null {
  const payload: any = input.payload;
  if (input.type === "attach_owner" || input.type === "transfer_owner") {
    const fact = context.facts[payload.ownershipReceipt];
    const owner = state.owner;
    const priorAbsent = owner.sessionId === null && owner.pid === 0 && owner.processStartIdentity === null && owner.lockIdentity === null;
    if (fact?.kind !== "ownership" || fact.hash !== payload.ownershipReceipt || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.priorSessionId !== owner.sessionId || fact.priorOwnerTokenHash !== owner.ownerTokenHash || fact.priorPid !== owner.pid || fact.priorProcessStartIdentity !== owner.processStartIdentity || fact.priorLockIdentity !== owner.lockIdentity || fact.priorAttachedAt !== owner.attachedAt || fact.disposition !== payload.priorOwnerDisposition || fact.successorSessionId !== payload.sessionId || fact.successorPid !== payload.pid || fact.successorProcessStartIdentity !== payload.processStartIdentity || fact.successorLockIdentity !== payload.lockIdentity) return "owner attach must resolve an exact canonical ownership fact";
    if (input.type === "attach_owner" && payload.priorOwnerDisposition === "same_manager") return "same-manager lineage requires the explicit current-owner transfer command";
    if (input.type === "transfer_owner" && payload.priorOwnerDisposition !== "same_manager") return "owner transfer requires exact same-manager lineage";
    if ((payload.priorOwnerDisposition === "absent") !== priorAbsent) return "absent-owner proof must bind an actually detached owner";
    if (payload.priorOwnerDisposition === "dead" && (priorAbsent || fact.priorObservationHash === null)) return "dead-owner proof requires an attached prior owner and exact process-death observation";
    if (payload.priorOwnerDisposition !== "dead" && fact.priorObservationHash !== null) return "only dead-owner recovery may carry a prior process observation";
    const expectedLineageHash = canonicalHash({ kind: "direct_owner_transfer", runId: state.runId, runNonce: state.runNonce, priorSessionId: owner.sessionId, priorOwnerTokenHash: owner.ownerTokenHash, priorPid: owner.pid, priorProcessStartIdentity: owner.processStartIdentity, priorLockIdentity: owner.lockIdentity, successorSessionId: payload.sessionId, successorPid: payload.pid, successorProcessStartIdentity: payload.processStartIdentity, successorLockIdentity: payload.lockIdentity });
    if (payload.priorOwnerDisposition === "same_manager" && (priorAbsent || fact.lineageHash !== expectedLineageHash)) return "same-manager transfer requires an attached prior owner and exact canonical direct lineage";
    if (payload.priorOwnerDisposition !== "same_manager" && fact.lineageHash !== null) return "only same-manager transfer may carry direct lineage";
  }
  if (input.type === "adopt_quarantined_fact") {
    const entry = state.quarantine[payload.quarantineId];
    const fact = context.facts[payload.adoptionReceipt];
    const entryHash = entry ? canonicalHash({ quarantineId: entry.quarantineId, fact: entry.fact, reason: entry.reason, observedBindingHash: entry.observedBindingHash, expectedBindingHash: entry.expectedBindingHash, observedAt: entry.observedAt }) : null;
    const authority = fact?.kind === "quarantine_resolution" ? context.authorityReceipts?.[fact.authorityReceiptHash] : undefined;
    if (!entry || fact?.kind !== "quarantine_resolution" || fact.hash !== payload.adoptionReceipt || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.quarantineId !== entry.quarantineId || fact.factHash !== entry.fact.hash || fact.quarantineEntryHash !== entryHash || fact.disposition !== "adopted" || authority?.kind !== "quarantine_authority" || authority.hash !== fact.authorityReceiptHash || authority.hash !== canonicalHash(Object.fromEntries(Object.entries(authority).filter(([key]) => key !== "hash"))) || authority.planHash !== state.identity.planHash || authority.runId !== state.runId || authority.runNonce !== state.runNonce || authority.quarantineId !== entry.quarantineId || authority.factHash !== entry.fact.hash || authority.quarantineEntryHash !== entryHash || authority.decision !== "adopt" || authority.issuedBy !== "user" || !Number.isFinite(utcTimestampOrderValue(authority.issuedAt)) || utcTimestampOrderValue(entry.observedAt) > utcTimestampOrderValue(authority.issuedAt) || utcTimestampOrderValue(authority.issuedAt) > utcTimestampOrderValue(input.occurredAt)) return "quarantine adoption must resolve exact external user authority and canonical immutable resolution facts";
  }
  if (input.type === "record_effect_observation") {
    const effect = state.effects[payload.effectId];
    const fact = context.facts[payload.observationHash];
    if (!effect || fact?.kind !== "effect_reconciliation" || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.reconciliation !== payload.reconciliation) return "effect observation must be an exact canonical reconciliation fact";
  }
  if (input.type === "record_cancellation") {
    const cancellation = state.cancellations[payload.cancellationId];
    if (!cancellation) return "cancellation observation references an unknown cancellation";
    const targetIds = cancellation.scope === "run" ? Object.keys(cancellation.fencedGenerations) : [cancellation.subjectId];
    const expectedWorkerAttemptIds = Object.values(state.stageAttempts).filter((attempt) => targetIds.includes(attempt.workItemId) && attempt.producerKind === "owned_worker" && !attempt.terminalAt).map(({ stageAttemptId }) => stageAttemptId).sort();
    if (JSON.stringify(payload.workerResults.map(({ stageAttemptId }: any) => stageAttemptId).sort()) !== JSON.stringify(expectedWorkerAttemptIds)) return "cancellation observation must cover every exact active worker attempt";
    for (const workerResult of payload.workerResults) {
      const binding = state.workerBindings[workerResult.stageAttemptId];
      const fact = context.facts[workerResult.result.hash];
      if (!binding || workerResult.result.kind !== "worker_result" || workerResult.result.bytes !== Buffer.byteLength(canonicalStringify(fact)) || fact?.kind !== "worker_result" || fact.hash !== workerResult.result.hash || fact.workerStorageId !== binding.workerStorageId || fact.launchOwnerSessionId !== binding.launchOwnerSessionId || fact.workerId !== binding.workerId || fact.attemptNumber !== binding.attemptNumber || fact.attemptNonce !== binding.attemptNonce || fact.configHash !== binding.configHash || !["succeeded", "needs_attention", "failed", "cancelled", "lost"].includes(fact.terminalStatus) || fact.processDisposition !== "dead" || !fact.retrySafe) return "cancellation worker result must prove exact terminal identity and retry-safe process death";
    }
    for (const observation of payload.effectObservations) {
      const effect = state.effects[observation.effectId];
      const fact = context.facts[observation.observationHash];
      if (!effect || fact?.kind !== "effect_reconciliation" || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || !["applied_exact", "compensated", "proven_absent"].includes(fact.reconciliation)) return "cancellation observation must use exact terminal reconciliation facts";
      if (effect.kind === "cancel_worker" && !["applied_exact", "proven_absent"].includes(fact.reconciliation)) return "cancel-worker observation must be applied exactly or proven absent";
    }
  }
  return null;
}

function applyInput(
  state: DagRunStateV1,
  input: DagRunInputV1,
  context: DagRunValidationContextV1,
  effects: DagRunEffectRequestV1[],
  notices: DagRunTransitionNoticeV1[],
): { code: DagRunRejectCodeV1; message: string } | null {
  const payload: any = input.payload;
  switch (input.type) {
    case "attach_owner":
    case "transfer_owner": {
      if (input.ownerEpoch !== state.owner.ownerEpoch || !["absent", "dead", "same_manager"].includes(payload.priorOwnerDisposition)) return precondition("owner takeover is not proven safe");
      state.owner = {
        ownerEpoch: state.owner.ownerEpoch + 1, ownerTokenHash: payload.ownerTokenHash, sessionId: payload.sessionId,
        pid: payload.pid, processStartIdentity: payload.processStartIdentity, lockIdentity: payload.lockIdentity,
        attachedAt: input.occurredAt, lastHeartbeatAt: input.occurredAt, ownershipReceipt: payload.ownershipReceipt,
        lastReleaseCommandId: null, lastReleasePayloadHash: null,
      };
      notices.push(notice(state, "owner_changed", state.runId, payload.ownershipReceipt));
      return null;
    }
    case "release_owner": {
      if (!state.owner.sessionId) return precondition("run has no attached owner");
      state.owner.sessionId = null; state.owner.pid = 0; state.owner.processStartIdentity = null;
      state.owner.lockIdentity = null; state.owner.attachedAt = null; state.owner.lastHeartbeatAt = null; state.owner.ownershipReceipt = null;
      state.owner.lastReleaseCommandId = input.commandId; state.owner.lastReleasePayloadHash = input.payloadHash;
      notices.push(notice(state, "owner_changed", state.runId, null));
      return null;
    }
    case "set_desired_run": {
      if (payload.desired === "running" && state.desired.run !== "paused") return precondition("only a paused run may resume");
      if (payload.desired === "paused" && (!["running", "paused"].includes(state.desired.run) || state.completion.state === "plan_complete" || ["completed", "cancelled", "superseded"].includes(state.current.run))) return precondition("terminal/cancelling run cannot pause");
      state.desired = { run: payload.desired, reason: payload.reason, requestedAt: input.occurredAt, requestedBy: payload.requestedBy };
      notices.push(notice(state, "desired_changed", state.runId, input.payloadHash));
      return null;
    }
    case "reserve_scheduler_batch": {
      const exactDecision = scheduleDagRunV1(context.plan, state);
      if (payload.decisionHash !== exactDecision.decisionHash || payload.decisionSequence !== exactDecision.decisionSequence || payload.policyHash !== exactDecision.policyHash || payload.normalizedIndexHash !== exactDecision.normalizedIndexHash || payload.inputSnapshotHash !== exactDecision.inputSnapshotHash || canonicalHash(payload.reservations) !== canonicalHash(exactDecision.selected) || canonicalHash([...payload.bypassSlotIds].sort()) !== canonicalHash([...exactDecision.bypassIncrements].sort())) return precondition("scheduler reservation must exactly reproduce the deterministic bound plan/run/policy decision");
      if (!state.owner.sessionId || !state.owner.lockIdentity) return precondition("scheduler reservation requires one exact attached owner");
      if (state.desired.run !== "running" || !["active", "integration"].includes(state.current.run)) return precondition("only an active running run may reserve scheduler work");
      if (payload.policyHash !== state.scheduler.policyHash || payload.normalizedIndexHash !== state.scheduler.normalizedIndexHash || payload.inputSnapshotHash !== input.expectedSnapshotHash) return precondition("scheduler decision bindings differ from the current run snapshot");
      if (payload.decisionSequence !== state.scheduler.decisionSequence + 1) return precondition("scheduler decision sequence is not the exact successor");
      const activeLanes = Object.values(state.scheduler.activeNodeLanes).filter(({ releaseDisposition }) => releaseDisposition === null).length;
      const newLaneIds = [...new Set(payload.reservations.map((reservation: any) => reservation.workItemId).filter((id: string) => !state.scheduler.activeNodeLanes[id] || state.scheduler.activeNodeLanes[id].releaseDisposition !== null))];
      if (activeLanes + newLaneIds.length > state.scheduler.maxActiveNodes) return precondition("scheduler batch exceeds sticky active-node lanes");
      const batchMutexes = new Set<string>();
      const batchResourceUnits: Record<string, number> = {};
      const batchOperationalUnits: Record<string, number> = {};
      for (const reservation of payload.reservations) {
        const item = state.workItems[reservation.workItemId];
        if (!item || item.writeRepositoryId !== reservation.repositoryId || item.candidateGeneration !== reservation.itemGeneration || !item.authorizedStages.includes(reservation.stage) || ["complete", "cancelled", "superseded"].includes(item.current)) return precondition("scheduler reservation does not bind one exact runnable work item generation");
        if (state.scheduler.reservations[reservation.reservationId] || Object.values(state.scheduler.reservations).some((current) => current.workItemId === reservation.workItemId && !["released", "fenced"].includes(current.state))) return precondition("scheduler natural reservation slot is already occupied");
        if (reservation.reservationSequence !== state.scheduler.nextReservationSequence + payload.reservations.indexOf(reservation)) return precondition("scheduler reservation sequence is not contiguous");
        const expectedOperation: Record<string, string[]> = { F0: ["conductor"], F1: ["implementation"], F2: ["evaluation"], F3: ["codification"], F4: ["verification"], F5: ["review"], F6: ["hardening"], F7: ["verification"], F8: item.current === "integration_ready" || item.current === "integrating" ? ["integration"] : ["conductor"] };
        if (!expectedOperation[reservation.stage].includes(reservation.operationKind)) return precondition("scheduler reservation violates the fixed lifecycle operation class");
        for (const mutexId of reservation.mutexGroupIds) {
          if (!state.mutexes[mutexId] || state.mutexes[mutexId].activeLeaseId || batchMutexes.has(mutexId)) return precondition("scheduler reservation conflicts with an active semantic mutex");
          batchMutexes.add(mutexId);
        }
        for (const [resourceId, units] of Object.entries(reservation.resourceUnits) as Array<[string, number]>) {
          const pool = state.resourcePools[resourceId];
          if (!pool || pool.allocatedUnits + (batchResourceUnits[resourceId] ?? 0) + units > Math.min(pool.observedCapacity, pool.semanticMaximum)) return precondition("scheduler reservation exceeds exact vector resource capacity");
          batchResourceUnits[resourceId] = (batchResourceUnits[resourceId] ?? 0) + units;
        }
        for (const [namespace, units] of Object.entries(reservation.operationalUnits) as Array<[string, number]>) {
          const pool = state.scheduler.operationalCapacities[namespace];
          if (!pool || pool.allocatedUnits + (batchOperationalUnits[namespace] ?? 0) + units > pool.observedCapacity) return precondition("scheduler reservation exceeds exact operational capacity");
          batchOperationalUnits[namespace] = (batchOperationalUnits[namespace] ?? 0) + units;
        }
        const conflictingExclusion = Object.values(state.scheduler.dynamicExclusions).find((exclusion) => exclusion.state === "active" && exclusion.workItemIds.includes(reservation.workItemId) && exclusion.phases.includes(reservation.stage) && exclusion.workItemIds.some((id) => payload.reservations.some((candidate: any) => candidate.workItemId === id && candidate.workItemId !== reservation.workItemId) || Object.values(state.scheduler.reservations).some((candidate) => candidate.workItemId === id && !["released", "fenced"].includes(candidate.state))));
        if (conflictingExclusion) return precondition(`scheduler reservation conflicts with dynamic exclusion ${conflictingExclusion.exclusionId}`);
      }
      for (const reservation of payload.reservations) {
        const item = state.workItems[reservation.workItemId];
        if (!state.scheduler.activeNodeLanes[item.workItemId] || state.scheduler.activeNodeLanes[item.workItemId].releaseDisposition !== null) {
          state.scheduler.activeNodeLanes[item.workItemId] = { workItemId: item.workItemId, admissionSequence: reservation.reservationSequence, admittedAt: input.occurredAt, releaseDisposition: null, releasedAt: null };
          item.laneAdmissionSequence = reservation.reservationSequence; item.admittedAt = input.occurredAt;
        }
        item.current = reservation.operationKind === "integration" ? "integrating" : "active"; item.currentStage = reservation.stage;
        const leaseIds: string[] = [];
        const stageLeaseId = `${reservation.reservationId}-stage`;
        state.leases[stageLeaseId] = { leaseId: stageLeaseId, kind: "stage_claim", subject: { kind: "work_item", id: item.workItemId }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: item.candidateGeneration, units: 1, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
        leaseIds.push(stageLeaseId);
        for (const mutexId of reservation.mutexGroupIds) {
          const leaseId = `${reservation.reservationId}-mutex-${canonicalHash(mutexId).slice(7, 15)}`;
          state.leases[leaseId] = { leaseId, kind: "semantic_mutex", subject: { kind: "mutex", id: mutexId }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: item.candidateGeneration, units: 1, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
          state.mutexes[mutexId].activeLeaseId = leaseId; leaseIds.push(leaseId);
        }
        for (const [resourceId, units] of Object.entries(reservation.resourceUnits) as Array<[string, number]>) {
          const leaseId = `${reservation.reservationId}-resource-${canonicalHash(resourceId).slice(7, 15)}`;
          state.leases[leaseId] = { leaseId, kind: "resource", subject: { kind: "resource", id: resourceId }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: item.candidateGeneration, units, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
          state.resourcePools[resourceId].leaseIds = [...state.resourcePools[resourceId].leaseIds, leaseId].sort(); state.resourcePools[resourceId].allocatedUnits += units; leaseIds.push(leaseId);
        }
        for (const [namespace, units] of Object.entries(reservation.operationalUnits) as Array<[string, number]>) {
          const leaseId = `${reservation.reservationId}-operational-${canonicalHash(namespace).slice(7, 15)}`;
          state.leases[leaseId] = { leaseId, kind: "resource", subject: { kind: "resource", id: namespace }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: item.candidateGeneration, units, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
          const pool = state.scheduler.operationalCapacities[namespace]; pool.allocatedUnits += units; pool.reservationIds = [...pool.reservationIds, reservation.reservationId].sort(); leaseIds.push(leaseId);
        }
        item.activeLeaseIds = [...new Set([...item.activeLeaseIds, ...leaseIds])].sort();
        state.scheduler.reservations[reservation.reservationId] = { reservationId: reservation.reservationId, reservationSequence: reservation.reservationSequence, workItemId: item.workItemId, stage: reservation.stage, attemptOrdinal: reservation.attemptOrdinal, operationKind: reservation.operationKind, state: "reserved", candidateGeneration: reservation.itemGeneration, ownerEpoch: state.owner.ownerEpoch, authorizationSetHash: state.identity.authorizationSet.hash, normalizedRequestHash: reservation.normalizedRequestHash, leaseIds: leaseIds.sort(), mutexGroupIds: [...reservation.mutexGroupIds].sort(), resourceUnits: Object.fromEntries(Object.entries(reservation.resourceUnits).sort(([a], [b]) => a.localeCompare(b))), operationalUnits: Object.fromEntries(Object.entries(reservation.operationalUnits).sort(([a], [b]) => a.localeCompare(b))), workerRole: reservation.workerRole, repositoryId: reservation.repositoryId, createdAt: input.occurredAt, releasedAt: null };
        state.scheduler.bypassCounters[reservation.slotId] = 0;
      }
      for (const slotId of payload.bypassSlotIds) state.scheduler.bypassCounters[slotId] = (state.scheduler.bypassCounters[slotId] ?? 0) + 1;
      state.scheduler.decisionSequence = payload.decisionSequence; state.scheduler.nextReservationSequence += payload.reservations.length; state.scheduler.lastDecisionCommandId = input.commandId;
      notices.push(notice(state, "scheduler_reserved", state.runId, payload.decisionHash));
      return null;
    }
    case "mark_scheduler_reservation_dispatch": {
      const reservation = state.scheduler.reservations[payload.reservationId]; if (!reservation || reservation.state !== "reserved" || reservation.normalizedRequestHash !== payload.normalizedRequestHash) return precondition("only an exact durable reserved scheduler operation may enter dispatch intent"); reservation.state = "dispatch_intent"; effects.push({ effectId: reservation.reservationId, kind: `scheduler_${reservation.operationKind}`, requestHash: reservation.normalizedRequestHash }); notices.push(notice(state, "scheduler_dispatch_intended", reservation.reservationId, reservation.normalizedRequestHash)); return null;
    }
    case "record_scheduler_reservation_dispatch": {
      const reservation = state.scheduler.reservations[payload.reservationId]; if (!reservation || reservation.state !== "dispatch_intent" || reservation.normalizedRequestHash !== payload.normalizedRequestHash) return precondition("scheduler dispatch observation must bind the exact persisted dispatch intent"); reservation.state = payload.disposition; if (payload.disposition === "launch_ambiguous") { const blockerId = `launch-ambiguous-${reservation.reservationId}`; state.blockers[blockerId] = { blockerId, kind: "launch_ambiguous", subject: { kind: "work_item", id: reservation.workItemId }, stage: reservation.stage, sourceId: reservation.reservationId, sourceHash: reservation.normalizedRequestHash, release: "immutable_fact", active: true, createdAt: input.occurredAt, releasedAt: null, releaseReceipt: null }; if (!state.workItems[reservation.workItemId].blockerIds.includes(blockerId)) state.workItems[reservation.workItemId].blockerIds.push(blockerId); } notices.push(notice(state, "scheduler_dispatch_observed", reservation.reservationId, payload.normalizedRequestHash)); return null;
    }
    case "release_scheduler_reservation": {
      const reservation = state.scheduler.reservations[payload.reservationId];
      if (!reservation || ["released", "fenced"].includes(reservation.state)) return precondition("scheduler reservation is not active");
      reservation.state = payload.disposition; reservation.releasedAt = input.occurredAt;
      const item = state.workItems[reservation.workItemId];
      for (const leaseId of reservation.leaseIds) {
        const lease = state.leases[leaseId]; if (!lease || !["active", "release_requested"].includes(lease.state)) return precondition("reservation lease is missing or already terminal");
        lease.state = payload.disposition === "fenced" ? "fenced" : "released"; lease.releasedAt = input.occurredAt; lease.releaseReason = payload.reason;
        item.activeLeaseIds = item.activeLeaseIds.filter((id) => id !== leaseId);
        if (lease.kind === "resource" && state.resourcePools[lease.subject.id]) { const pool = state.resourcePools[lease.subject.id]; pool.allocatedUnits -= lease.units; pool.leaseIds = pool.leaseIds.filter((id) => id !== leaseId); }
        if (lease.kind === "resource" && state.scheduler.operationalCapacities[lease.subject.id]) { const pool = state.scheduler.operationalCapacities[lease.subject.id]; pool.allocatedUnits -= lease.units; pool.reservationIds = pool.reservationIds.filter((id) => id !== reservation.reservationId); }
        if (lease.kind === "semantic_mutex" && state.mutexes[lease.subject.id]?.activeLeaseId === leaseId) state.mutexes[lease.subject.id].activeLeaseId = null;
      }
      notices.push(notice(state, "scheduler_released", reservation.reservationId, input.payloadHash));
      return null;
    }
    case "authorize_retry": {
      const entry = state.retryLedger[payload.retryKey]; const item = state.workItems[payload.workItemId];
      if (!entry || !item || entry.workItemId !== payload.workItemId || entry.stage !== payload.stage || entry.dimension !== payload.dimension || entry.fingerprint !== payload.fingerprint || item.candidateGeneration !== payload.candidateGeneration) return precondition("retry request does not bind an existing exact retry ledger slot");
      if (entry.count !== payload.expectedCount || entry.count >= entry.ceiling || entry.stop !== "none") return precondition("retry count, ceiling, or stop disposition rejects authorization");
      if (Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation))) return precondition("retry requires every prior effect reconciled");
      entry.count += 1; entry.lastRetryCommandId = input.commandId;
      notices.push(notice(state, "retry_authorized", payload.workItemId, payload.retryKey));
      return null;
    }
    case "reserve_integration_attempt": {
      if (state.desired.run !== "running" || state.freshness.blocksIntegration) return precondition("integration reservation requires a running integration-fresh run");
      const item = state.workItems[payload.workItemId]; const repository = state.repositories[payload.repositoryId]; const train = state.integrationTrains[payload.repositoryId];
      const planTrain = context.plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === payload.repositoryId);
      const member = planTrain?.members.find(({ workItemId }) => workItemId === payload.workItemId);
      if (!item || !repository || !train || !planTrain || !member || item.writeRepositoryId !== payload.repositoryId || item.current !== "integration_ready" || !item.integrationReadyReceipt || !item.candidate || item.candidate.candidateHash !== payload.sourceCandidateHash) return precondition("integration attempt does not bind one exact integration-ready train member and candidate");
      const headOrdinal = train.entryOrder.filter((entryId) => train.entries[entryId]?.state === "integrated").length;
      if (member.ordinal !== headOrdinal || train.activeIntegrationAttemptId || state.integrationAttempts[payload.integrationAttemptId]) return precondition("only the exact current train head may reserve one integration attempt");
      const retryAuthorization = payload.retryAuthorizationKey ? state.retryLedger[payload.retryAuthorizationKey] : null;
      if ((payload.retryOrdinal === 0) !== (payload.retryAuthorizationKey === null) || (payload.retryOrdinal > 0 && (!retryAuthorization || retryAuthorization.workItemId !== item.workItemId || retryAuthorization.dimension !== "integration" || retryAuthorization.count !== payload.retryOrdinal || retryAuthorization.lastRetryCommandId === null))) return precondition("integration retry must bind an exact previously authorized integration retry ledger slot");
      if (canonicalHash(payload.sourceBase) !== canonicalHash(item.candidate.base) || canonicalHash(payload.sourceCandidate) !== canonicalHash(item.candidate.git) || canonicalHash(payload.expectedPrefix) !== canonicalHash(train.acceptedPrefix) || canonicalHash(payload.expectedTarget) !== canonicalHash(train.expectedTarget)) return precondition("integration source, prefix, or target identity differs from current authority");
      const readyFact = context.facts[item.integrationReadyReceipt] as any;
      if (readyFact?.kind !== "integration_ready" || readyFact.hash !== item.integrationReadyReceipt || readyFact.workItemId !== item.workItemId || readyFact.candidateGeneration !== item.candidateGeneration || readyFact.candidateHash !== item.candidate.candidateHash) return precondition("integration readiness fact is missing or stale");
      const bindingFact = context.facts[payload.repositoryBindingFactHash] as any;
      if (!exactGitFact(bindingFact, state, "repository_binding", payload.repositoryId, null) || bindingFact.reconciliation !== "applied_exact" || bindingFact.ownerEpoch !== state.owner.ownerEpoch || bindingFact.targetRef !== repository.targetRef || bindingFact.commit !== payload.expectedTarget.commit || bindingFact.tree !== payload.expectedTarget.tree || bindingFact.objectFormat !== (payload.expectedTarget.commit.length === 40 ? "sha1" : "sha256")) return precondition("repository binding fact does not prove the exact current target/common-dir identity");
      if (repository.workspace.gitCommonDirIdentityHash && repository.workspace.gitCommonDirIdentityHash !== bindingFact.commonDirIdentityHash) return precondition("repository common-dir identity conflicts with prior session binding");
      if (Object.values(state.repositories).some((candidate) => candidate.repositoryId !== repository.repositoryId && candidate.integrationLockLeaseId !== null && candidate.workspace.gitCommonDirIdentityHash === bindingFact.commonDirIdentityHash && state.leases[candidate.integrationLockLeaseId]?.state !== "released")) return precondition("another repository identity already holds the exact same Git common-directory integration lock");
      const effect = payload.compositionEffect;
      if (effect.kind !== "compose_candidate" || effect.effectId !== `${payload.integrationAttemptId}-compose` || effect.subject.kind !== "train" || effect.subject.id !== planTrain.trainId || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.boundOwnerEpoch !== state.owner.ownerEpoch || effect.boundAuthorizationSetHash !== state.identity.authorizationSet.hash || effect.boundFreshnessReceiptHash !== state.freshness.receipt.hash) return precondition("integration composition effect must be one pristine current-authority intent");
      const lockLease = { leaseId: payload.lockLeaseId, kind: "integration_lock" as const, subject: { kind: "repository" as const, id: payload.repositoryId }, holderStageAttemptId: null, holderIntegrationAttemptId: payload.integrationAttemptId, candidateGeneration: item.candidateGeneration, units: 1, ownerEpoch: state.owner.ownerEpoch, state: "active" as const, acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
      if (state.leases[payload.lockLeaseId] || repository.integrationLockLeaseId || train.lockLeaseId) return precondition("repository integration lock lease slot is already occupied");
      state.leases[payload.lockLeaseId] = lockLease; repository.integrationLockLeaseId = payload.lockLeaseId; train.lockLeaseId = payload.lockLeaseId;
      repository.workspace.gitCommonDirIdentityHash = bindingFact.commonDirIdentityHash; repository.workspace.gitWorktreeIdentityHash = bindingFact.worktreeIdentityHash; repository.workspace.observationReceipt = bindingFact.hash; repository.observationReceipt = bindingFact.hash; repository.observedTarget = payload.expectedTarget; repository.observedTargetAt = bindingFact.observedAt; state.freshness.repositoryObservationHashes[payload.repositoryId] = bindingFact.hash;
      const priorEntry = train.entries[payload.entryId];
      if (priorEntry && (priorEntry.workItemId !== item.workItemId || priorEntry.ordinal !== member.ordinal || priorEntry.state !== "invalidated" || state.integrationAttempts[priorEntry.currentAttemptId ?? ""]?.conflictClass === "none")) return precondition("integration entry natural slot is not an exact retryable conflict");
      if (priorEntry) { const priorAttempt = state.integrationAttempts[priorEntry.currentAttemptId!]; const conflictFact = priorAttempt?.compositionFactHash ? context.facts[priorAttempt.compositionFactHash] as any : null; const candidateFact = context.facts[item.candidate.candidateHash] as any; const producerAttempt = state.stageAttempts[item.candidate.producedByStageAttemptId]; if (item.candidate.generation !== priorEntry.sourceCandidate.generation + 1 || candidateFact?.kind !== "candidate" || candidateFact.producedByStageAttemptId !== item.candidate.producedByStageAttemptId || producerAttempt?.stage !== "F1" || producerAttempt.state !== "sealed" || producerAttempt.reservedOutputGeneration !== item.candidate.generation || !conflictFact || utcTimestampOrderValue(producerAttempt.createdAt) < utcTimestampOrderValue(conflictFact.observedAt)) return precondition("integration retry requires a fresh exact post-conflict F1 candidate generation"); }
      if (Object.values(state.integrationAttempts).some((candidate) => candidate.entryId === payload.entryId && candidate.retryOrdinal === payload.retryOrdinal)) return precondition("integration retry ordinal/authorization has already been consumed by an immutable attempt");
      if (priorEntry) { priorEntry.state = "reserved"; priorEntry.attemptIds = [...priorEntry.attemptIds, payload.integrationAttemptId]; priorEntry.currentAttemptId = payload.integrationAttemptId; priorEntry.integrationReadyHash = item.integrationReadyReceipt; priorEntry.sourceCandidate = structuredClone(item.candidate); }
      else { train.entries[payload.entryId] = { entryId: payload.entryId, workItemId: item.workItemId, ordinal: member.ordinal, state: "reserved", integrationReadyHash: item.integrationReadyReceipt, sourceCandidate: structuredClone(item.candidate), attemptIds: [payload.integrationAttemptId], currentAttemptId: payload.integrationAttemptId, integrationReceipt: null, blockerIds: [] }; train.entryOrder = [...train.entryOrder, payload.entryId]; }
      train.activeIntegrationAttemptId = payload.integrationAttemptId;
      state.effects[effect.effectId] = structuredClone(effect);
      state.integrationAttempts[payload.integrationAttemptId] = { integrationAttemptId: payload.integrationAttemptId, entryId: payload.entryId, retryOrdinal: payload.retryOrdinal, sourceCandidateHash: payload.sourceCandidateHash, strategy: "merge_tree_one_parent", compositionProfileHash: planTrain.compositionProfileHash, prefixValidationProfileHash: planTrain.prefixValidationProfileHash, finalValidationProfileHash: planTrain.finalValidationProfileHash, sourceBase: structuredClone(payload.sourceBase), sourceCandidate: structuredClone(payload.sourceCandidate), expectedPrefix: structuredClone(payload.expectedPrefix), expectedTarget: structuredClone(payload.expectedTarget), temporaryRef: payload.temporaryRef, temporaryWorkspaceReceipt: null, compositionEffectId: effect.effectId, composedTree: null, syntheticParentCommit: null, sourceToIntegratedLineageHash: null, conflictClass: "none", prefixEvidenceHashes: [], finalEvidenceHashes: [], environmentClosureHash: null, landingEffectId: null, landingState: "none", intendedLandedTree: null, integrationReceipt: null, repositoryBindingFactHash: bindingFact.hash, privateRefFactHashes: [], compositionFactHash: null, proposalVerificationFactHash: null, landingObservationFactHash: null };
      item.integrationEntryId = payload.entryId; item.current = "integrating"; train.entries[payload.entryId].state = "composing";
      notices.push(notice(state, "integration_reserved", payload.integrationAttemptId, bindingFact.hash));
      return null;
    }
    case "record_git_composition": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId];
      if (!attempt || !train || !entry || entry.currentAttemptId !== attempt.integrationAttemptId || entry.state !== "composing" || payload.conflictClass !== "none") return precondition("composition observation must bind the active clean train attempt");
      const fact = context.facts[payload.compositionFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any; const effect = state.effects[attempt.compositionEffectId];
      if (!sameGitRepositoryBinding(fact, bindingFact) || effect?.state !== "dispatching" || effect.dispatchCount < 1 || !exactGitFact(fact, state, "composition", train.repositoryId, attempt.integrationAttemptId) || fact.effectId !== effect?.effectId || fact.requestHash !== effect?.requestHash || fact.ownerEpoch !== effect?.boundOwnerEpoch || fact.reconciliation !== "applied_exact" || fact.commit !== payload.composedTree.commit || fact.tree !== payload.composedTree.tree || fact.parentCommit !== payload.syntheticParentCommit || payload.syntheticParentCommit !== attempt.expectedPrefix.commit) return precondition("composition fact does not prove exact one-parent composed commit/tree");
      for (const hash of payload.privateRefFactHashes) { const anchor = context.facts[hash] as any; if (!exactGitFact(anchor, state, "private_ref", train.repositoryId, attempt.integrationAttemptId) || anchor.ownerEpoch !== effect.boundOwnerEpoch || anchor.commonDirIdentityHash !== fact.commonDirIdentityHash || anchor.worktreeIdentityHash !== fact.worktreeIdentityHash || anchor.gitConfigHash !== fact.gitConfigHash || anchor.gitVersionHash !== fact.gitVersionHash || anchor.objectFormat !== fact.objectFormat || anchor.reconciliation !== "applied_exact") return precondition("composition private-ref fact is missing or conflicting"); }
      effect.state = "reconciled"; effect.reconciliation = "applied_exact";
      attempt.composedTree = structuredClone(payload.composedTree); attempt.syntheticParentCommit = payload.syntheticParentCommit; attempt.sourceToIntegratedLineageHash = payload.sourceToIntegratedLineageHash; attempt.conflictClass = "none"; attempt.compositionFactHash = fact.hash; attempt.privateRefFactHashes = [...payload.privateRefFactHashes].sort();
      entry.state = "verifying_prefix"; notices.push(notice(state, "integration_composed", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "record_git_composition_conflict": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId]; const item = entry ? state.workItems[entry.workItemId] : null; const effect = attempt ? state.effects[attempt.compositionEffectId] : null;
      if (!attempt || !train || !entry || !item || entry.currentAttemptId !== attempt.integrationAttemptId || entry.state !== "composing" || effect?.state !== "dispatching" || effect.dispatchCount < 1) return precondition("composition conflict must bind the exact active dispatched train attempt");
      const fact = context.facts[payload.compositionFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any;
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "composition", train.repositoryId, attempt.integrationAttemptId) || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.ownerEpoch !== effect.boundOwnerEpoch || fact.reconciliation !== "conflict" || fact.commit !== null || fact.tree !== null) return precondition("composition conflict fact must prove an exact no-result conflict observation");
      attempt.conflictClass = payload.conflictClass; attempt.compositionFactHash = fact.hash; effect.state = "failed"; effect.reconciliation = "conflict"; entry.state = "invalidated"; train.activeIntegrationAttemptId = null;
      item.candidateGeneration += 1; item.candidate = null; item.integrationReadyReceipt = null; item.integrationEntryId = null; item.current = "active"; item.currentStage = "F1";
      for (const stageId of PLAN_STAGE_IDS.slice(1)) { const stage = item.stages[stageId]; stage.state = "pending"; stage.currentAttemptId = null; stage.currentEvidence = null; stage.adoptionReceipt = null; stage.lastDisposition = null; stage.blockerIds = []; }
      releaseIntegrationLock(state, train.repositoryId, attempt.integrationAttemptId, input.occurredAt, "exact composition conflict");
      notices.push(notice(state, "integration_conflict", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "record_proposal_verification": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId];
      if (!attempt || !train || !entry || entry.state !== "verifying_prefix" || !attempt.composedTree) return precondition("proposal verification requires one exact composed train attempt");
      const fact = context.facts[payload.proposalVerificationFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any;
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "proposal_verification", train.repositoryId, attempt.integrationAttemptId) || fact.ownerEpoch !== state.effects[attempt.compositionEffectId]?.boundOwnerEpoch || fact.reconciliation !== "applied_exact" || fact.commit !== attempt.composedTree.commit || fact.tree !== attempt.composedTree.tree || fact.detailsHash !== canonicalHash({ prefixEvidenceHashes: [...payload.prefixEvidenceHashes].sort(), finalEvidenceHashes: [...payload.finalEvidenceHashes].sort(), environmentClosureHash: payload.environmentClosureHash }) || payload.prefixEvidenceHashes.length === 0 || payload.finalEvidenceHashes.length === 0) return precondition("proposal verification fact does not bind exact composed prefix/final evidence");
      for (const hash of [...payload.prefixEvidenceHashes, ...payload.finalEvidenceHashes]) { const evidence = context.facts[hash] as any; if (evidence?.kind !== "verification" || evidence.hash !== hash || evidence.planHash !== state.identity.planHash || evidence.runId !== state.runId || evidence.runNonce !== state.runNonce || evidence.integrationAttemptId !== attempt.integrationAttemptId || canonicalHash(evidence.tree) !== canonicalHash(attempt.composedTree) || evidence.disposition !== "PASS") return precondition("proposal verification evidence is absent, stale, or non-PASS"); }
      for (const hash of [...payload.prefixEvidenceHashes, ...payload.finalEvidenceHashes]) { const evidence = context.facts[hash] as any; state.evidenceIndex.verifications[hash] = { kind: "verification", schemaVersion: 1, id: evidence.id ?? `verification-${hash.slice(7, 19)}`, hash, bytes: Buffer.byteLength(canonicalStringify(evidence)), mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }; }
      attempt.prefixEvidenceHashes = [...payload.prefixEvidenceHashes].sort(); attempt.finalEvidenceHashes = [...payload.finalEvidenceHashes].sort(); attempt.environmentClosureHash = payload.environmentClosureHash; attempt.proposalVerificationFactHash = fact.hash; entry.state = "landing";
      notices.push(notice(state, "integration_verified", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "prepare_git_landing": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId]; const effect = payload.landingEffect;
      if (!attempt || !train || !entry || entry.state !== "landing" || !attempt.composedTree || !attempt.proposalVerificationFactHash || attempt.prefixEvidenceHashes.length === 0 || attempt.finalEvidenceHashes.length === 0 || canonicalHash(payload.intendedLandedTree) !== canonicalHash(attempt.composedTree)) return precondition("landing intent requires exact current composed and fully verified proposal");
      if (effect.kind !== "land_target" || effect.effectId !== `${attempt.integrationAttemptId}-land` || effect.subject.kind !== "repository" || effect.subject.id !== train.repositoryId || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.boundOwnerEpoch !== state.owner.ownerEpoch || effect.boundAuthorizationSetHash !== state.identity.authorizationSet.hash || effect.boundFreshnessReceiptHash !== state.freshness.receipt.hash) return precondition("landing effect must be one pristine current-authority intent");
      state.effects[effect.effectId] = structuredClone(effect); attempt.landingEffectId = effect.effectId; attempt.landingState = "intended"; attempt.intendedLandedTree = structuredClone(payload.intendedLandedTree);
      notices.push(notice(state, "landing_intended", attempt.integrationAttemptId, effect.requestHash)); return null;
    }
    case "record_git_landing_reconciliation": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const effect = attempt?.landingEffectId ? state.effects[attempt.landingEffectId] : null;
      if (!attempt || !train || !effect || effect.state !== "dispatching" || effect.dispatchCount < 1 || !attempt.intendedLandedTree || !["intended", "dispatching", "observed", "ambiguous"].includes(attempt.landingState)) return precondition("landing observation has no exact current landing intent");
      const fact = context.facts[payload.landingObservationFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any;
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "landing", train.repositoryId, attempt.integrationAttemptId) || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.ownerEpoch !== effect.boundOwnerEpoch || fact.reconciliation !== payload.reconciliation) return precondition("landing observation fact does not bind exact effect and reconciliation");
      attempt.landingObservationFactHash = fact.hash; effect.reconciliation = payload.reconciliation;
      if (payload.reconciliation === "applied_exact") {
        if (fact.commit !== attempt.intendedLandedTree.commit || fact.tree !== attempt.intendedLandedTree.tree) return precondition("applied landing observation differs from exact intended commit/tree");
        attempt.landingState = "reconciled"; effect.state = "reconciled";
      } else if (payload.reconciliation === "proven_absent") {
        if (fact.commit !== attempt.expectedTarget.commit || fact.tree !== attempt.expectedTarget.tree) return precondition("proven-absent landing must observe the exact expected old target identity");
        attempt.landingState = "observed"; effect.state = "reconciled";
      } else {
        if (payload.reconciliation === "conflict" && (fact.commit === null || fact.tree === null || (fact.commit === attempt.expectedTarget.commit && fact.tree === attempt.expectedTarget.tree) || (fact.commit === attempt.intendedLandedTree.commit && fact.tree === attempt.intendedLandedTree.tree))) return precondition("landing conflict must observe a concrete third target identity");
        attempt.landingState = "ambiguous";
        if (payload.reconciliation === "conflict") {
          effect.state = "failed"; const entry = train.entries[attempt.entryId]; const item = entry ? state.workItems[entry.workItemId] : null; const blockerId = `integration-target-third-${attempt.integrationAttemptId}`;
          if (entry && item) { entry.state = "blocked"; train.activeIntegrationAttemptId = null; item.current = "integration_ready"; if (!item.blockerIds.includes(blockerId)) item.blockerIds.push(blockerId); state.blockers[blockerId] = { blockerId, kind: "integration_drift", subject: { kind: "work_item", id: item.workItemId }, stage: "F8", sourceId: attempt.integrationAttemptId, sourceHash: fact.hash, release: "successor_plan", active: true, createdAt: input.occurredAt, releasedAt: null, releaseReceipt: null }; releaseIntegrationLock(state, train.repositoryId, attempt.integrationAttemptId, input.occurredAt, "exact third-target conflict"); }
        } else effect.state = "ambiguous";
      }
      notices.push(notice(state, "landing_observed", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "accept_integration_receipt": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId]; const item = entry ? state.workItems[entry.workItemId] : null;
      if (!attempt || !train || !entry || !item || attempt.landingState !== "reconciled" || !attempt.landingObservationFactHash || !attempt.intendedLandedTree) return precondition("integration receipt acceptance requires exact applied landing reconciliation");
      const fact = context.facts[payload.integrationReceiptHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any; const transactionFact = context.facts[payload.transactionReceiptFactHash] as any; const receipt = transactionFact?.receipt as any;
      const receiptHashExact = receipt?.receiptHash === payload.transactionReceiptHash && receipt?.receiptHash === canonicalHash(Object.fromEntries(Object.entries(receipt ?? {}).filter(([key]) => key !== "receiptHash")));
      const transactionBindingExact = transactionFact?.kind === "git_integration_receipt" && transactionFact.hash === payload.transactionReceiptFactHash && transactionFact.hash === canonicalHash(Object.fromEntries(Object.entries(transactionFact).filter(([key]) => key !== "hash"))) && transactionFact.planHash === state.identity.planHash && transactionFact.runId === state.runId && transactionFact.runNonce === state.runNonce && transactionFact.authorizationSetHash === state.identity.authorizationSet.hash && transactionFact.repositoryId === train.repositoryId && transactionFact.integrationAttemptId === attempt.integrationAttemptId && transactionFact.transactionReceiptHash === payload.transactionReceiptHash;
      const receiptExact = receiptHashExact && receipt?.transactionId === attempt.integrationAttemptId && receipt?.runId === state.runId && receipt?.runNonce === state.runNonce && receipt?.planHash === state.identity.planHash && receipt?.authorizationSetHash === state.identity.authorizationSet.hash && receipt?.ownerEpoch === state.owner.ownerEpoch && receipt?.repositoryId === train.repositoryId && receipt?.commonDirIdentityHash === bindingFact?.commonDirIdentityHash && receipt?.worktreeIdentityHash === bindingFact?.worktreeIdentityHash && receipt?.configHash === bindingFact?.gitConfigHash && canonicalHash(receipt?.gitVersion) === bindingFact?.gitVersionHash && receipt?.objectFormat === bindingFact?.objectFormat && receipt?.targetRef === state.repositories[train.repositoryId].targetRef && receipt?.workItemId === item.workItemId && receipt?.candidateGeneration === item.candidateGeneration && canonicalHash(receipt?.sourceBase) === canonicalHash(attempt.sourceBase) && canonicalHash(receipt?.candidate) === canonicalHash(attempt.sourceCandidate) && canonicalHash(receipt?.expectedPrefix) === canonicalHash(attempt.expectedPrefix) && canonicalHash(receipt?.composed) === canonicalHash(attempt.intendedLandedTree) && receipt?.compositionProfileHash === attempt.compositionProfileHash && receipt?.prefixValidationProfileHash === attempt.prefixValidationProfileHash && receipt?.finalValidationProfileHash === attempt.finalValidationProfileHash && receipt?.environmentClosureHash === attempt.environmentClosureHash && canonicalHash([...(receipt?.prefixEvidenceHashes ?? [])].sort()) === canonicalHash(attempt.prefixEvidenceHashes) && canonicalHash([...(receipt?.finalEvidenceHashes ?? [])].sort()) === canonicalHash(attempt.finalEvidenceHashes) && receipt?.landing?.expectedOldOid === attempt.expectedTarget.commit && receipt?.landing?.newOid === attempt.intendedLandedTree.commit && receipt?.landing?.reconciliation === "applied_exact" && receipt?.landing?.targetObservationHash === (context.facts[attempt.landingObservationFactHash] as any)?.detailsHash && canonicalHash(Object.keys(receipt?.privateRefs ?? {}).sort()) === canonicalHash(["baseline", "candidate", "composed", "prefix", "proposal"]) && canonicalHash(Object.values(receipt?.privateRefs ?? {}).sort()) === canonicalHash(attempt.privateRefFactHashes.map((hash) => (context.facts[hash] as any)?.targetRef).sort());
      if (!transactionBindingExact || !receiptExact) return precondition("transaction receipt fact does not resolve the exact immutable real-Git transaction receipt/content authority");
      if (fact?.kind !== "integration" || fact.hash !== payload.integrationReceiptHash || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.workItemId !== item.workItemId || fact.repositoryId !== train.repositoryId || fact.integrationAttemptId !== attempt.integrationAttemptId || fact.candidateHash !== attempt.sourceCandidateHash || fact.strategy !== attempt.strategy || fact.compositionProfileHash !== attempt.compositionProfileHash || canonicalHash(fact.expectedPrefix) !== canonicalHash(attempt.expectedPrefix) || canonicalHash(fact.expectedTarget) !== canonicalHash(attempt.expectedTarget) || canonicalHash(fact.landed) !== canonicalHash(attempt.intendedLandedTree) || fact.syntheticParentCommit !== attempt.expectedPrefix.commit || fact.sourceToIntegratedLineageHash !== attempt.sourceToIntegratedLineageHash || fact.environmentClosureHash !== attempt.environmentClosureHash || !fact.combinedStateVerified || !fact.reconciled || fact.acceptingOwnerEpoch !== state.owner.ownerEpoch || fact.commonDirIdentityHash !== bindingFact?.commonDirIdentityHash || fact.worktreeIdentityHash !== bindingFact?.worktreeIdentityHash || fact.gitConfigHash !== bindingFact?.gitConfigHash || fact.gitVersionHash !== bindingFact?.gitVersionHash || fact.objectFormat !== bindingFact?.objectFormat || fact.transactionReceiptHash !== payload.transactionReceiptHash || fact.transactionReceiptFactHash !== payload.transactionReceiptFactHash || fact.landingObservationHash !== attempt.landingObservationFactHash) return precondition("integration receipt does not duplicate the exact current source/composition/verification/landing transaction");
      attempt.integrationReceipt = fact.hash; attempt.landingState = "landed"; entry.integrationReceipt = fact.hash; entry.state = "integrated"; entry.currentAttemptId = null;
      item.integrationReceipt = fact.hash; item.current = "complete"; item.completedAt = input.occurredAt; item.currentStage = "F8";
      state.evidenceIndex.integrationReceipts[attempt.integrationAttemptId] = { kind: "integration", schemaVersion: 1, id: attempt.integrationAttemptId, hash: fact.hash, bytes: Buffer.byteLength(canonicalStringify(fact)), mediaType: "application/json", sensitivity: "internal", retention: "project", locator: null };
      train.acceptedPrefix = structuredClone(fact.landed); train.expectedTarget = structuredClone(fact.landed); train.acceptedPrefixOrdinal = entry.ordinal + 1; train.acceptedPrefixReceipt = fact.hash; train.activeIntegrationAttemptId = null;
      const repository = state.repositories[train.repositoryId]; repository.observedTarget = structuredClone(fact.landed); repository.observedTargetAt = input.occurredAt; repository.observationReceipt = attempt.landingObservationFactHash; state.freshness.repositoryObservationHashes[train.repositoryId] = attempt.landingObservationFactHash;
      const lockLease = train.lockLeaseId ? state.leases[train.lockLeaseId] : null; if (lockLease) { lockLease.state = "released"; lockLease.releasedAt = input.occurredAt; lockLease.releaseReason = "integration receipt accepted"; }
      repository.integrationLockLeaseId = null; train.lockLeaseId = null;
      const lane = state.scheduler.activeNodeLanes[item.workItemId]; if (lane?.releaseDisposition === null) { lane.releaseDisposition = "integrated"; lane.releasedAt = input.occurredAt; }
      for (const reservation of Object.values(state.scheduler.reservations).filter((candidate) => candidate.workItemId === item.workItemId && !["released", "fenced"].includes(candidate.state))) { reservation.state = "released"; reservation.releasedAt = input.occurredAt; for (const leaseId of reservation.leaseIds) { const lease = state.leases[leaseId]; if (lease && !["released", "fenced"].includes(lease.state)) { lease.state = "released"; lease.releasedAt = input.occurredAt; lease.releaseReason = "integration receipt accepted"; } } }
      item.activeLeaseIds = [];
      for (const edge of Object.values(state.precedence).filter(({ predecessorWorkItemId }) => predecessorWorkItemId === item.workItemId)) { edge.state = "satisfied"; edge.satisfyingReceipt = fact.hash; }
      state.completion.completeWorkItemIds = [...new Set([...state.completion.completeWorkItemIds, item.workItemId])].sort(); state.completion.remainingAuthorizedWorkItemIds = state.completion.remainingAuthorizedWorkItemIds.filter((id) => id !== item.workItemId);
      if (train.entryOrder.every((entryId) => train.entries[entryId]?.state === "integrated")) state.completion.completedRepositoryIds = [...new Set([...state.completion.completedRepositoryIds, train.repositoryId])].sort();
      if (!state.completion.remainingAuthorizedWorkItemIds.length) { state.completion.state = state.completion.unauthorizedWorkItemIds.length ? "authorized_scope_complete" : "plan_complete"; state.completion.completedAt = input.occurredAt; }
      notices.push(notice(state, "integration_accepted", item.workItemId, fact.hash)); return null;
    }
    case "put_effect_intent": {
      const effect = payload.effect;
      if (state.effects[effect.effectId]) return precondition("effect ID collides with an existing immutable effect slot");
      if (state.desired.run !== "running" && effect.kind !== "reconcile_external_effect") return precondition("non-running run may create only reconciliation intents");
      if (effect.subject.kind === "work_item" && ["cancel", "supersede"].includes(state.workItems[effect.subject.id]?.desired) && effect.kind !== "reconcile_external_effect") return precondition("fenced work item cannot receive new execution effects");
      if (effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.state !== "intended" || effect.dispatchCount !== 0) return precondition("new effect must be an undispatched intent at the next revision");
      state.effects[effect.effectId] = structuredClone(effect);
      notices.push(notice(state, "effect_intended", effect.effectId, effect.requestHash));
      return null;
    }
    case "mark_effect_dispatching": {
      const effect = state.effects[payload.effectId];
      if (!effect || effect.state !== "intended" || effect.dispatchCount !== payload.expectedDispatchCount) return precondition("effect is not dispatchable at the expected count");
      if (state.desired.run !== "running" && !["cancel_worker", "reconcile_external_effect"].includes(effect.kind)) return precondition("paused/cancelling/terminal run blocks new non-recovery dispatch");
      if (effect.subject.kind === "work_item" && ["cancel", "supersede"].includes(state.workItems[effect.subject.id]?.desired) && !["cancel_worker", "reconcile_external_effect"].includes(effect.kind)) return precondition("fenced work item blocks new non-recovery dispatch");
      effect.state = "dispatching"; effect.dispatchCount += 1; effect.lastDispatchAt = input.occurredAt;
      effects.push({ effectId: effect.effectId, kind: effect.kind, requestHash: effect.requestHash });
      notices.push(notice(state, "effect_dispatching", effect.effectId, effect.requestHash));
      return null;
    }
    case "retry_effect_dispatch": {
      const effect = state.effects[payload.effectId];
      const replayableProvenAbsentLanding = effect?.kind === "land_target" && effect.state === "reconciled" && effect.reconciliation === "proven_absent";
      if (!effect || (!replayableProvenAbsentLanding && !["dispatching", "ambiguous"].includes(effect.state)) || effect.dispatchCount !== payload.expectedDispatchCount) return precondition("uncertain/proven-absent landing effect is not retryable at the expected dispatch count");
      if (!["pure", "idempotent"].includes(effect.procedureClass)) return precondition("compensatable/non-repeatable/unknown effect requires exact reconciliation rather than redispatch");
      if (state.desired.run === "paused" && !["cancel_worker", "reconcile_external_effect"].includes(effect.kind)) return precondition("pause blocks uncertain effect redispatch");
      effect.state = "dispatching"; effect.dispatchCount += 1; effect.lastDispatchAt = input.occurredAt;
      effects.push({ effectId: effect.effectId, kind: effect.kind, requestHash: effect.requestHash });
      notices.push(notice(state, "effect_redispatching", effect.effectId, effect.requestHash));
      return null;
    }
    case "record_effect_observation": {
      const effect = state.effects[payload.effectId];
      if (!effect || !["dispatching", "observed", "failed", "ambiguous"].includes(effect.state)) return precondition("effect has no matching dispatch to observe");
      const reconciled = ["applied_exact", "compensated", "proven_absent"].includes(payload.reconciliation);
      if ((payload.terminalState === "reconciled") !== reconciled || (["ambiguous", "failed"].includes(payload.terminalState) && !["conflict", "unknown"].includes(payload.reconciliation))) return precondition("effect terminal state must match exact reconciliation disposition");
      effect.observationHash = payload.observationHash; effect.reconciliation = payload.reconciliation; effect.state = payload.terminalState;
      notices.push(notice(state, "effect_observed", effect.effectId, payload.observationHash));
      return null;
    }
    case "request_cancellation": {
      if (state.cancellations[payload.cancellationId]) return precondition("cancellation ID collides with an existing immutable cancellation slot");
      const targetIds = [...payload.workItemIds].sort();
      if (new Set(targetIds).size !== targetIds.length || targetIds.some((id: string) => !state.workItems[id] || ["complete", "cancelled", "superseded"].includes(state.workItems[id].current))) return precondition("cancellation work items must be exact, unique, and nonterminal");
      const overlappingCancellation = Object.values(state.cancellations).find((candidate) => candidate.state !== "closed" && Object.keys(candidate.fencedGenerations).some((id) => targetIds.includes(id)));
      if (overlappingCancellation) return precondition(`cancellation overlaps open fence ${overlappingCancellation.cancellationId}`);
      const nonterminalIds = Object.keys(state.workItems).filter((id) => !["complete", "cancelled", "superseded"].includes(state.workItems[id].current)).sort();
      if (payload.scope === "run" && (payload.subjectId !== state.runId || JSON.stringify(targetIds) !== JSON.stringify(nonterminalIds))) return precondition("run cancellation must fence every nonterminal work item exactly");
      if (payload.scope === "work_item" && (targetIds.length !== 1 || targetIds[0] !== payload.subjectId)) return precondition("work-item cancellation must fence exactly its subject");
      for (const workItemId of targetIds) fenceWorkItemForCancellation(state, workItemId, input.occurredAt);
      if (payload.scope === "run") for (const effect of Object.values(state.effects).filter((candidate) => candidate.subject.kind !== "work_item")) fenceEffectForCancellation(state, effect, input.occurredAt);
      const expectedCancelRequests = Object.values(state.stageAttempts).filter((attempt) => targetIds.includes(attempt.workItemId) && attempt.producerKind === "owned_worker" && !attempt.terminalAt && Boolean(state.workerBindings[attempt.stageAttemptId])).map((attempt) => {
        const binding = state.workerBindings[attempt.stageAttemptId];
        return canonicalHash({ kind: "cancel_worker", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, fencedGeneration: state.workItems[attempt.workItemId].candidateGeneration });
      }).sort();
      if (JSON.stringify(payload.effects.map(({ requestHash }: any) => requestHash).sort()) !== JSON.stringify(expectedCancelRequests)) return precondition("cancellation effects must cover every exact bound active worker and no other operation");
      for (const effect of payload.effects) {
        if (state.effects[effect.effectId]) return precondition("cancellation effect ID collides with an existing immutable effect slot");
        if (effect.kind !== "cancel_worker" || effect.subject.kind !== "work_item" || !targetIds.includes(effect.subject.id) || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.lastDispatchAt !== null || effect.observationHash !== null || effect.reconciliation !== "not_started" || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt) return precondition("cancellation effects must be pristine exact persisted cancel-worker intents");
        state.effects[effect.effectId] = structuredClone(effect);
      }
      state.cancellations[payload.cancellationId] = {
        cancellationId: payload.cancellationId, scope: payload.scope, subjectId: payload.subjectId,
        fencedGenerations: Object.fromEntries(targetIds.map((id: string) => [id, state.workItems[id].candidateGeneration])), state: "requested",
        reason: payload.reason, requestedAt: input.occurredAt, effectIds: payload.effects.map(({ effectId }: any) => effectId).sort(), resultHash: null,
      };
      if (payload.scope === "run") state.desired = { run: "cancelled", reason: payload.reason, requestedAt: input.occurredAt, requestedBy: "user" };
      notices.push(notice(state, "cancellation_requested", payload.cancellationId, input.payloadHash));
      return null;
    }
    case "record_cancellation": {
      const cancellation = state.cancellations[payload.cancellationId];
      if (!cancellation || !["requested", "dispatching", "observed"].includes(cancellation.state)) return precondition("cancellation is not awaiting observation");
      const expectedResultHash = canonicalHash({ cancellationId: payload.cancellationId, effectObservations: [...payload.effectObservations].sort((left: any, right: any) => left.effectId.localeCompare(right.effectId)), workerResults: payload.workerResults.map(({ stageAttemptId, result }: any) => ({ stageAttemptId, resultHash: result.hash })).sort((left: any, right: any) => left.stageAttemptId.localeCompare(right.stageAttemptId)) });
      if (payload.resultHash !== expectedResultHash) return precondition("cancellation result hash must bind every exact terminal observation");
      cancellation.state = "closed"; cancellation.resultHash = payload.resultHash;
      for (const workerResult of payload.workerResults) {
        const binding = state.workerBindings[workerResult.stageAttemptId];
        const fact = context.facts[workerResult.result.hash];
        const attempt = state.stageAttempts[workerResult.stageAttemptId];
        if (binding && attempt && fact?.kind === "worker_result") {
          binding.processDisposition = "dead"; binding.retrySafe = true;
          if (fact.terminalStatus === "cancelled") { state.evidenceIndex.workerResults[fact.hash] = workerResult.result; attempt.workerResult = workerResult.result; binding.resultHash = fact.hash; binding.completionId = fact.completionId; }
          else {
            const quarantineId = `cancelled-${workerResult.stageAttemptId}-${fact.hash.slice("sha256:".length, "sha256:".length + 12)}`;
            if (state.quarantine[quarantineId]) return precondition("late terminal worker result collides with existing quarantine identity");
            state.quarantine[quarantineId] = { quarantineId, fact: workerResult.result, reason: "cancelled_generation", observedBindingHash: canonicalHash({ workerStorageId: fact.workerStorageId, launchOwnerSessionId: fact.launchOwnerSessionId, workerId: fact.workerId, attemptNumber: fact.attemptNumber, attemptNonce: fact.attemptNonce, configHash: fact.configHash, completionId: fact.completionId }), expectedBindingHash: canonicalHash({ workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, fencedGeneration: state.cancellations[payload.cancellationId].fencedGenerations[attempt.workItemId] }), state: "held", observedAt: input.occurredAt, adoptionReceipt: null, rejectionReason: null };
          }
        }
      }
      const targetIds = cancellation.scope === "run" ? Object.keys(cancellation.fencedGenerations) : [cancellation.subjectId];
      for (const workItemId of targetIds) closeCancelledWorkItem(state, workItemId, input.occurredAt);
      const observedEffectIds = payload.effectObservations.map(({ effectId }: any) => effectId).sort();
      const targetIdsForEffects = Object.keys(cancellation.fencedGenerations);
      const unresolvedPriorEffectIds = Object.values(state.effects).filter((effect) => (cancellation.scope === "run" || (effect.subject.kind === "work_item" && targetIdsForEffects.includes(effect.subject.id))) && !cancellation.effectIds.includes(effect.effectId) && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation)).map(({ effectId }) => effectId);
      const expectedObservedEffectIds = [...new Set([...cancellation.effectIds, ...unresolvedPriorEffectIds])].sort();
      if (JSON.stringify(observedEffectIds) !== JSON.stringify(expectedObservedEffectIds)) return precondition("cancellation observation must cover every exact cancellation and unresolved prior effect");
      for (const observation of payload.effectObservations) {
        const effect = state.effects[observation.effectId];
        const fact = context.facts[observation.observationHash];
        const isCancelEffect = cancellation.effectIds.includes(observation.effectId);
        if (!effect || !["intended", "dispatching", "observed", "failed", "ambiguous", "reconciled"].includes(effect.state) || !["applied_exact", "compensated", "proven_absent"].includes(fact?.reconciliation ?? "") || (isCancelEffect && !["applied_exact", "proven_absent"].includes(fact.reconciliation)) || (effect.state === "intended" && fact?.reconciliation !== "proven_absent") || (effect.state === "reconciled" && (effect.observationHash !== observation.observationHash || effect.reconciliation !== fact.reconciliation))) return precondition("cancellation observation requires exact terminal effect reconciliation");
        effect.state = "reconciled"; effect.reconciliation = fact.reconciliation; effect.observationHash = observation.observationHash;
        if (effect.blockerId && state.blockers[effect.blockerId]?.active) { state.blockers[effect.blockerId].active = false; state.blockers[effect.blockerId].releasedAt = input.occurredAt; state.blockers[effect.blockerId].releaseReceipt = observation.observationHash; }
      }
      notices.push(notice(state, "cancellation_closed", payload.cancellationId, payload.resultHash));
      return null;
    }
    case "quarantine_fact": {
      if (state.quarantine[payload.quarantine.quarantineId]) return precondition("quarantine ID collides with an existing immutable quarantine slot");
      if (payload.quarantine.state !== "held" || payload.quarantine.observedAt !== input.occurredAt || payload.quarantine.adoptionReceipt !== null || payload.quarantine.rejectionReason !== null) return precondition("new quarantine entry must begin held with no disposition");
      state.quarantine[payload.quarantine.quarantineId] = structuredClone(payload.quarantine);
      notices.push(notice(state, "fact_quarantined", payload.quarantine.quarantineId, payload.quarantine.fact.hash));
      return null;
    }
    case "adopt_quarantined_fact": {
      const entry = state.quarantine[payload.quarantineId];
      if (!entry || entry.state !== "held") return precondition("only a held quarantine fact may be adopted");
      entry.state = "adopted"; entry.adoptionReceipt = payload.adoptionReceipt;
      notices.push(notice(state, "quarantine_adopted", payload.quarantineId, payload.adoptionReceipt));
      return null;
    }
    case "reject_quarantined_fact": {
      const entry = state.quarantine[payload.quarantineId];
      if (!entry || entry.state !== "held") return precondition("only a held quarantine fact may be rejected");
      entry.state = "rejected"; entry.rejectionReason = payload.reason;
      notices.push(notice(state, "quarantine_rejected", payload.quarantineId, entry.fact.hash));
      return null;
    }
  }
}

function exactGitFact(fact: any, state: DagRunStateV1, factType: string, repositoryId: string, integrationAttemptId: string | null): boolean {
  if (!fact || fact.kind !== "git_transaction" || fact.factType !== factType || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash")))) return false;
  return fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.authorizationSetHash === state.identity.authorizationSet.hash && fact.repositoryId === repositoryId && fact.integrationAttemptId === integrationAttemptId;
}

function sameGitRepositoryBinding(fact: any, binding: any): boolean {
  return Boolean(fact && binding && fact.commonDirIdentityHash === binding.commonDirIdentityHash && fact.worktreeIdentityHash === binding.worktreeIdentityHash && fact.gitConfigHash === binding.gitConfigHash && fact.gitVersionHash === binding.gitVersionHash && fact.objectFormat === binding.objectFormat);
}

function naturalSlotId(input: DagRunInputV1): string {
  const payload: any = input.payload;
  const naturalIdentity = (() => {
    switch (input.type) {
      case "attach_owner": return payload.ownershipReceipt;
      case "transfer_owner": return payload.ownershipReceipt;
      case "release_owner": return input.ownerEpoch;
      case "set_desired_run": return input.commandId;
      case "put_effect_intent": return payload.effect.effectId;
      case "mark_effect_dispatching": return `${payload.effectId}/${payload.expectedDispatchCount}`;
      case "retry_effect_dispatch": return `${payload.effectId}/${payload.expectedDispatchCount}`;
      case "record_effect_observation": return `${payload.effectId}/${payload.observationHash}`;
      case "request_cancellation": return payload.cancellationId;
      case "record_cancellation": return `${payload.cancellationId}/${payload.resultHash}`;
      case "quarantine_fact": return payload.quarantine.quarantineId;
      case "adopt_quarantined_fact": return payload.quarantineId;
      case "reject_quarantined_fact": return payload.quarantineId;
      case "reserve_scheduler_batch": return `${payload.decisionSequence}/${payload.decisionHash}`;
      case "mark_scheduler_reservation_dispatch": return payload.reservationId;
      case "record_scheduler_reservation_dispatch": return `${payload.reservationId}/${payload.disposition}`;
      case "release_scheduler_reservation": return `${payload.reservationId}/${payload.disposition}`;
      case "authorize_retry": return `${payload.retryKey}/${payload.expectedCount}`;
      case "reserve_integration_attempt": return payload.integrationAttemptId;
      case "record_git_composition": return `${payload.integrationAttemptId}/${payload.compositionFactHash}`;
      case "record_git_composition_conflict": return `${payload.integrationAttemptId}/${payload.compositionFactHash}`;
      case "record_proposal_verification": return `${payload.integrationAttemptId}/${payload.proposalVerificationFactHash}`;
      case "prepare_git_landing": return `${payload.integrationAttemptId}/${payload.landingEffect.effectId}`;
      case "record_git_landing_reconciliation": return `${payload.integrationAttemptId}/${payload.landingObservationFactHash}`;
      case "accept_integration_receipt": return `${payload.integrationAttemptId}/${payload.integrationReceiptHash}`;
    }
  })();
  return canonicalHash({ type: input.type, naturalIdentity });
}

function idempotencyDisposition(state: DagRunStateV1, input: DagRunInputV1, slotId: string): "none" | "same" | "conflict" {
  const slot = state.idempotencySlots[slotId];
  const inputHash = canonicalHash(input);
  if (slot) return slot.inputHash === inputHash && slot.commandId === input.commandId && slot.idempotencyKey === input.idempotencyKey && slot.payloadHash === input.payloadHash ? "same" : "conflict";
  const identityConflict = Object.values(state.idempotencySlots).some((candidate) => candidate.commandId === input.commandId || candidate.idempotencyKey === input.idempotencyKey);
  return identityConflict ? "conflict" : "none";
}

function fenceEffectForCancellation(state: DagRunStateV1, effect: any, at: string): void {
  if (effect.state === "intended") { effect.state = "cancelled"; effect.reconciliation = "proven_absent"; return; }
  if (["dispatching", "observed", "failed", "ambiguous"].includes(effect.state) && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation)) {
    effect.state = "ambiguous";
    if (!["conflict", "unknown"].includes(effect.reconciliation)) effect.reconciliation = "unknown";
    const blockerId = `cancellation-effect-${effect.effectId}`;
    effect.blockerId = blockerId;
    state.blockers[blockerId] = { blockerId, kind: "side_effect_unreconciled", subject: effect.subject, stage: null, sourceId: effect.effectId, sourceHash: effect.requestHash, release: "immutable_fact", active: true, createdAt: at, releasedAt: null, releaseReceipt: null };
    if (effect.subject.kind === "work_item" && state.workItems[effect.subject.id] && !state.workItems[effect.subject.id].blockerIds.includes(blockerId)) state.workItems[effect.subject.id].blockerIds.push(blockerId);
  }
}

function fenceWorkItemForCancellation(state: DagRunStateV1, workItemId: string, at: string): void {
  const item = state.workItems[workItemId];
  item.desired = "cancel"; item.candidateGeneration += 1; item.candidate = null; item.current = "blocked"; item.currentStage = null;
  item.integrationReadyReceipt = null; item.integrationReceipt = null; item.completedAt = null;
  for (const stage of Object.values(item.stages)) {
    if (["active", "passed", "failed", "blocked", "budget_exhausted"].includes(stage.state)) stage.state = "cancelled";
    stage.currentAttemptId = null; stage.currentEvidence = null; stage.adoptionReceipt = null; stage.lastDisposition = null;
  }
  for (const attempt of Object.values(state.stageAttempts).filter((candidate) => candidate.workItemId === workItemId && !candidate.terminalAt)) {
    attempt.state = "cancelling"; attempt.updatedAt = at;
    if (attempt.launchIntentId && state.launchIntents[attempt.launchIntentId] && !["closed", "not_started"].includes(state.launchIntents[attempt.launchIntentId].state)) state.launchIntents[attempt.launchIntentId].state = "cancel_requested";
  }
  for (const reservation of Object.values(state.scheduler.reservations).filter((candidate) => candidate.workItemId === workItemId && !["released", "fenced"].includes(candidate.state))) { reservation.state = "fenced"; reservation.releasedAt = at; }
  for (const leaseId of item.activeLeaseIds) if (state.leases[leaseId]?.state === "active") state.leases[leaseId].state = "release_requested";
  for (const effect of Object.values(state.effects).filter((candidate) => candidate.subject.kind === "work_item" && candidate.subject.id === workItemId)) {
    fenceEffectForCancellation(state, effect, at);
  }
  for (const train of Object.values(state.integrationTrains)) for (const entry of Object.values(train.entries).filter((candidate) => candidate.workItemId === workItemId && candidate.state !== "integrated")) {
    entry.state = "invalidated"; entry.currentAttemptId = null; train.activeIntegrationAttemptId = null;
  }
}

function closeCancelledWorkItem(state: DagRunStateV1, workItemId: string, at: string): void {
  const item = state.workItems[workItemId];
  item.current = "cancelled";
  for (const attempt of Object.values(state.stageAttempts).filter((candidate) => candidate.workItemId === workItemId && !candidate.terminalAt)) {
    attempt.state = "cancelled"; attempt.updatedAt = at; attempt.terminalAt = at;
  }
  const lane = state.scheduler.activeNodeLanes[workItemId];
  if (lane && lane.releaseDisposition === null) { lane.releaseDisposition = "terminal_cancelled"; lane.releasedAt = at; }
  for (const leaseId of item.activeLeaseIds) {
    const lease = state.leases[leaseId];
    if (lease && lease.state !== "released") { lease.state = "released"; lease.releasedAt = at; lease.releaseReason = "terminal cancellation"; }
  }
  item.activeLeaseIds = [];
  for (const mutex of Object.values(state.mutexes)) if (mutex.activeLeaseId && state.leases[mutex.activeLeaseId]?.state === "released") mutex.activeLeaseId = null;
  for (const resource of Object.values(state.resourcePools)) resource.allocatedUnits = resource.leaseIds.reduce((sum, leaseId) => sum + (["active", "release_requested", "expired", "fenced"].includes(state.leases[leaseId]?.state ?? "") ? state.leases[leaseId].units : 0), 0);
  for (const [namespace, capacity] of Object.entries(state.scheduler.operationalCapacities)) {
    const activeLeases = Object.values(state.leases).filter((lease) => lease.kind === "resource" && lease.subject.id === namespace && ["active", "release_requested", "expired"].includes(lease.state));
    capacity.allocatedUnits = activeLeases.reduce((sum, lease) => sum + lease.units, 0);
    capacity.reservationIds = [...new Set(activeLeases.flatMap((lease) => Object.values(state.scheduler.reservations).filter((reservation) => reservation.leaseIds.includes(lease.leaseId)).map(({ reservationId }) => reservationId)))].sort();
  }
}

function releaseIntegrationLock(state: DagRunStateV1, repositoryId: string, integrationAttemptId: string, at: string, reason: string): void {
  const repository = state.repositories[repositoryId]; const train = state.integrationTrains[repositoryId]; const leaseId = repository?.integrationLockLeaseId;
  if (!repository || !train || !leaseId || train.lockLeaseId !== leaseId) return;
  const lease = state.leases[leaseId];
  if (lease?.kind === "integration_lock" && lease.holderIntegrationAttemptId === integrationAttemptId && lease.state !== "released") { lease.state = "released"; lease.releasedAt = at; lease.releaseReason = reason; }
  repository.integrationLockLeaseId = null; train.lockLeaseId = null;
}

function rederiveCurrent(state: DagRunStateV1, commandId: string): void {
  const ids = Object.keys(state.workItems).sort();
  state.current.readyWorkItemIds = ids.filter((id) => state.workItems[id].current === "ready");
  state.current.activeWorkItemIds = Object.values(state.scheduler.activeNodeLanes).filter(({ releaseDisposition }) => releaseDisposition === null).map(({ workItemId }) => workItemId).sort();
  state.current.blockedWorkItemIds = ids.filter((id) => state.workItems[id].current === "blocked");
  state.current.integrationReadyWorkItemIds = ids.filter((id) => state.workItems[id].current === "integration_ready");
  if (state.completion.state === "plan_complete") state.current.run = "completed";
  else if (state.desired.run === "paused") state.current.run = "paused";
  else if (state.desired.run === "cancelled") {
    state.current.run = ids.every((id) => ["complete", "cancelled", "superseded"].includes(state.workItems[id].current)) ? "cancelled" : "cancelling";
  } else if (state.current.integrationReadyWorkItemIds.length || ids.some((id) => state.workItems[id].current === "integrating")) state.current.run = "integration";
  else if (state.current.activeWorkItemIds.length || state.current.readyWorkItemIds.length) state.current.run = "active";
  else if (state.current.blockedWorkItemIds.length) state.current.run = "blocked";
  else state.current.run = "initializing";
  state.current.updatedByCommandId = commandId;
}

function notice(state: DagRunStateV1, type: string, subjectId: string, hash: string | null): DagRunTransitionNoticeV1 {
  return { runId: state.runId, revision: state.revision + 1, type, subjectId, hash };
}
function precondition(message: string): { code: "PRECONDITION_FAILED"; message: string } { return { code: "PRECONDITION_FAILED", message }; }
function reject(state: DagRunStateV1, code: DagRunRejectCodeV1, message: string, issues?: ValidationIssue[]): DagRunReducerResultV1 {
  return { accepted: false, code, message, currentRevision: state.revision, blockerIds: Object.values(state.blockers).filter(({ active }) => active).map(({ blockerId }) => blockerId).sort(), ...(issues ? { issues } : {}) };
}
