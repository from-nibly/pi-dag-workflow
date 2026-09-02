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
  dagRunNeedsReplanV1,
  dagRunSnapshotHash,
  deriveStageAggregateDispositionV1,
  exactLifecycleProcedureCatalogBindingV1,
  integrationValidationEffectRequestV1,
  lifecycleProcedureEffectRequestV1,
  ownershipChainHashV1,
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
const MarkEffectDispatchingPayloadSchema = StrictObject({
  effectId: IdSchema,
  expectedDispatchCount: NonNegativeIntegerSchema,
  ownedWorkerDispatch: Type.Optional(StrictObject({ readyPacketHash: HashSchema, normalizedDirective: Type.Union([Type.String({ maxLength: 2_000 }), Type.Null()]), directiveHash: HashSchema, promptHash: HashSchema, dispatchConfigRequestHash: HashSchema })),
});
const RetryEffectDispatchPayloadSchema = StrictObject({ effectId: IdSchema, expectedDispatchCount: PositiveIntegerSchema, reason: Type.Literal("uncertain_acknowledgement") });
const RecordEffectExecutionPayloadSchema = StrictObject({ effectId: IdSchema, executionObservationHash: HashSchema });
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
const RecordProposalVerificationPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, proposalVerificationFactHash: HashSchema, prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), prefixEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), finalEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema });
const PrepareGitLandingPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, landingEffect: EffectProjectionV1Schema, intendedLandedTree: GitTreeRefV1Schema });
const RecordGitLandingPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, landingObservationFactHash: HashSchema, reconciliation: Type.Enum(["applied_exact", "proven_absent", "conflict", "unknown"]) });
const AcceptIntegrationReceiptPayloadSchema = StrictObject({ integrationAttemptId: IdSchema, integrationReceiptHash: HashSchema, transactionReceiptHash: HashSchema, transactionReceiptFactHash: HashSchema });
const LaunchIntentInputSchema = StrictObject({
  launchIntentId: IdSchema, effectId: IdSchema, state: Type.Literal("reserved"), adapter: Type.Literal("owned-worker-v1"),
  launchKey: IdSchema, workerId: IdSchema, expectedAttemptNumber: PositiveIntegerSchema, taskPacketHash: HashSchema,
  cwdRepositoryId: IdSchema, configRequestHash: HashSchema, dispatchProtocolVersion: Type.Optional(Type.Literal(1)), dispatchCount: Type.Literal(0), lastDispatchAt: Type.Null(),
  boundAt: Type.Null(), ambiguityReason: Type.Null(),
});
const WorkerBindingInputSchema = StrictObject({
  stageAttemptId: IdSchema, launchIntentId: IdSchema, workerStorageId: IdSchema, launchOwnerSessionId: IdSchema,
  workerId: IdSchema, attemptNumber: PositiveIntegerSchema, attemptNonce: Type.String({ minLength: 16, maxLength: 256 }),
  configHash: HashSchema, configRef: HashRefV1Schema, supervisorPid: NonNegativeIntegerSchema,
  supervisorStartIdentity: Type.Union([Type.String({ minLength: 1, maxLength: 256 }), Type.Null()]),
  childPid: Type.Union([NonNegativeIntegerSchema, Type.Null()]), childStartIdentity: Type.Union([Type.String({ minLength: 1, maxLength: 256 }), Type.Null()]),
  mailboxHash: Type.Union([HashSchema, Type.Null()]), heartbeatAt: Type.Union([TimestampSchema, Type.Null()]),
  completionId: Type.Null(), resultHash: Type.Null(),
});
const BeginStageAttemptPayloadSchema = StrictObject({
  reservationId: IdSchema, stageAttemptId: IdSchema, attemptInput: HashRefV1Schema,
  launchIntent: Type.Union([LaunchIntentInputSchema, Type.Null()]), launchEffect: Type.Union([EffectProjectionV1Schema, Type.Null()]),
});
const BindWorkerAttemptPayloadSchema = StrictObject({ stageAttemptId: IdSchema, binding: WorkerBindingInputSchema, launchObservation: HashRefV1Schema });
const RecordWorkerResultPayloadSchema = StrictObject({ stageAttemptId: IdSchema, result: HashRefV1Schema });
const RecordCandidatePayloadSchema = StrictObject({ stageAttemptId: IdSchema, candidate: HashRefV1Schema, f2Transition: Type.Optional(HashRefV1Schema), procedureExecution: Type.Optional(HashRefV1Schema) });
const RecordFindingPayloadSchema = StrictObject({ finding: HashRefV1Schema });
const RecordFindingResolutionPayloadSchema = StrictObject({ resolution: HashRefV1Schema });
const SealStageAttemptPayloadSchema = StrictObject({
  stageAttemptId: IdSchema, evidence: HashRefV1Schema, checkAggregate: HashRefV1Schema,
  oracleAssertions: Type.Array(HashRefV1Schema), checkDispositions: Type.Array(HashRefV1Schema),
  checkExecutions: Type.Optional(Type.Array(HashRefV1Schema)), checkAuthorities: Type.Optional(Type.Array(HashRefV1Schema)),
  effectReconciliations: Type.Optional(Type.Array(HashRefV1Schema)),
  environmentObservation: Type.Optional(HashRefV1Schema), workspaceMaterialization: Type.Optional(HashRefV1Schema),
});
const SealF8IntegrationReadyPayloadSchema = StrictObject({
  stageAttemptId: IdSchema, evidence: HashRefV1Schema, checkAggregate: HashRefV1Schema, integrationReady: HashRefV1Schema,
  oracleAssertions: Type.Array(HashRefV1Schema), checkDispositions: Type.Array(HashRefV1Schema),
  checkExecutions: Type.Optional(Type.Array(HashRefV1Schema)), checkAuthorities: Type.Optional(Type.Array(HashRefV1Schema)),
  effectReconciliations: Type.Optional(Type.Array(HashRefV1Schema)),
  environmentObservation: Type.Optional(HashRefV1Schema), workspaceMaterialization: Type.Optional(HashRefV1Schema),
});

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
  variant("record_effect_execution", "observation", RecordEffectExecutionPayloadSchema),
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
  variant("begin_stage_attempt", "command", BeginStageAttemptPayloadSchema),
  variant("bind_worker_attempt", "observation", BindWorkerAttemptPayloadSchema),
  variant("record_worker_result", "observation", RecordWorkerResultPayloadSchema),
  variant("record_candidate", "observation", RecordCandidatePayloadSchema),
  variant("record_finding", "observation", RecordFindingPayloadSchema),
  variant("record_finding_resolution", "observation", RecordFindingResolutionPayloadSchema),
  variant("seal_stage_attempt", "observation", SealStageAttemptPayloadSchema),
  variant("seal_f8_integration_ready", "observation", SealF8IntegrationReadyPayloadSchema),
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
  let transitionPrecondition: { code: DagRunRejectCodeV1; message: string } | null | undefined;
  try { transitionPrecondition = applyInput(next, input, context, effects, notices); }
  catch (error) { return reject(state, "PRECONDITION_FAILED", `reducer transition helper rejected without escape: ${error instanceof Error ? error.message : String(error)}`); }
  if (transitionPrecondition) return reject(state, transitionPrecondition.code, transitionPrecondition.message);
  const reconciliationBindings = input.type === "record_effect_observation"
    ? [{ effectId: (input.payload as any).effectId, observationHash: (input.payload as any).observationHash }]
    : input.type === "record_cancellation"
      ? (input.payload as any).effectObservations.filter(({ effectId, observationHash }: any) => {
        const prior = state.effects[effectId];
        return prior?.observationHash !== observationHash || prior.state !== "reconciled";
      }).map(({ effectId, observationHash }: any) => ({ effectId, observationHash })).sort((left: any, right: any) => left.effectId.localeCompare(right.effectId))
      : [];
  const landingObservationBinding = input.type === "record_git_landing_reconciliation" ? (() => {
    const payload = input.payload as any;
    const attempt = state.integrationAttempts[payload.integrationAttemptId];
    const effect = attempt?.landingEffectId ? state.effects[attempt.landingEffectId] : undefined;
    const fact = context.facts[payload.landingObservationFactHash] as any;
    const expectedDispatchCount = effect!.dispatchCount - 1;
    const dispatchInputType = expectedDispatchCount === 0 ? "mark_effect_dispatching" : "retry_effect_dispatch";
    const dispatchSlotId = canonicalHash({ type: dispatchInputType, naturalIdentity: `${effect!.effectId}/${expectedDispatchCount}` });
    return {
      integrationAttemptId: attempt!.integrationAttemptId, effectId: effect!.effectId, observationHash: fact.hash,
      requestHash: effect!.requestHash, authorizationSetHash: fact.authorizationSetHash, ownerEpoch: fact.ownerEpoch,
      dispatchCount: effect!.dispatchCount, dispatchRevision: state.idempotencySlots[dispatchSlotId].appliedRevision,
      dispatchAt: effect!.lastDispatchAt!,
    };
  })() : null;
  next.idempotencySlots[slotId] = {
    slotId, inputType: input.type, commandId: input.commandId, idempotencyKey: input.idempotencyKey,
    payloadHash: input.payloadHash, inputHash: canonicalHash(input), appliedRevision: state.revision + 1,
    ...(reconciliationBindings.length ? { reconciliationBindings } : {}),
    ...(input.type === "record_cancellation" ? { reconciliationCancellationId: (input.payload as any).cancellationId } : {}),
    ...(landingObservationBinding ? { landingObservationBinding } : {}),
  };
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
    const priorReceipt = owner.ownershipReceipt ? context.facts[owner.ownershipReceipt] as any : null;
    if (fact?.kind !== "ownership" || fact.hash !== payload.ownershipReceipt || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.ownerEpoch !== owner.ownerEpoch + 1 || fact.priorOwnershipReceiptHash !== owner.ownershipReceipt || fact.chainHash !== ownershipChainHashV1(fact as any, priorReceipt?.kind === "ownership" ? priorReceipt.chainHash : null) || fact.priorSessionId !== owner.sessionId || fact.priorOwnerTokenHash !== owner.ownerTokenHash || fact.priorPid !== owner.pid || fact.priorProcessStartIdentity !== owner.processStartIdentity || fact.priorLockIdentity !== owner.lockIdentity || fact.priorAttachedAt !== owner.attachedAt || fact.disposition !== payload.priorOwnerDisposition || fact.successorSessionId !== payload.sessionId || fact.successorPid !== payload.pid || fact.successorProcessStartIdentity !== payload.processStartIdentity || fact.successorLockIdentity !== payload.lockIdentity) return "owner attach must resolve an exact canonical chained ownership fact";
    if ((fact.priorOwnershipReceiptHash === null) !== (fact.ownerEpoch === 1)) return "only the epoch-one genesis owner receipt may have a null predecessor";
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
  if (input.type === "bind_worker_attempt") {
    const attempt = state.stageAttempts[payload.stageAttemptId];
    const launch = attempt?.launchIntentId ? state.launchIntents[attempt.launchIntentId] : undefined;
    const effect = launch ? state.effects[launch.effectId] : undefined;
    const configFact = context.facts[payload.binding.configRef.hash] as any;
    const observation = context.facts[payload.launchObservation.hash] as any;
    if (!exactFactRef(payload.binding.configRef, configFact, "worker_config") || configFact.configHash !== payload.binding.configHash || canonicalHash(configFact.config) !== payload.binding.configHash) return "worker binding config must be a readable exact immutable worker-config artifact";
    if (!launch || !effect || !exactFactRef(payload.launchObservation, observation, "worker_launch_observation") || observation.planHash !== state.identity.planHash || observation.runId !== state.runId || observation.runNonce !== state.runNonce || observation.authorizationSetHash !== state.identity.authorizationSet.hash || observation.ownerEpoch !== state.owner.ownerEpoch || observation.effectId !== effect.effectId || observation.requestHash !== effect.requestHash || observation.launchIntentId !== launch.launchIntentId || observation.launchKey !== launch.launchKey || observation.reconciliation !== "applied_exact") return "worker binding requires an exact immutable current-authority launch/effect reconciliation observation";
  }
  if (input.type === "record_effect_execution") {
    const effect = state.effects[payload.effectId] as any;
    const fact = context.facts[payload.executionObservationHash] as any;
    const exactCommon = effect && fact?.hash === payload.executionObservationHash && fact.hash === canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.authorizationSetHash === effect.boundAuthorizationSetHash && fact.freshnessReceiptHash === effect.boundFreshnessReceiptHash && fact.ownerEpoch === effect.boundOwnerEpoch && fact.effectId === effect.effectId && fact.requestHash === effect.requestHash && fact.requestIdentityHash === effect.requestHash;
    const exactProcedure = effect?.kind === "run_procedure" && effect.boundStageAttemptId != null && fact?.kind === "effect_execution_observation" && fact.operationKind === "lifecycle_procedure" && fact.resultIdentityHash === canonicalHash(fact.result) && fact.resultBytes === Buffer.byteLength(canonicalStringify(fact.result)) && fact.resultBytes <= 4 * 1024 * 1024 && fact.disposition === fact.result?.checkAggregate?.disposition;
    const exactValidation = effect?.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null && fact?.kind === "verification" && fact.integrationAttemptId === effect.boundIntegrationAttemptId && fact.phase === effect.executionRequest?.phase;
    if (!exactCommon || (!exactProcedure && !exactValidation) || utcTimestampOrderValue(fact.startedAt) < utcTimestampOrderValue(effect.lastDispatchAt) || utcTimestampOrderValue(fact.completedAt) < utcTimestampOrderValue(fact.startedAt) || utcTimestampOrderValue(fact.completedAt) > utcTimestampOrderValue(input.occurredAt)) return "effect execution must be one exact canonical non-future post-dispatch bounded observation for the persisted request identity";
  }
  if (input.type === "record_effect_observation") {
    const effect = state.effects[payload.effectId] as any;
    const fact = context.facts[payload.observationHash] as any;
    if (!effect || fact?.kind !== "effect_reconciliation" || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.reconciliation !== payload.reconciliation || !effectReconciliationTimeIsBounded(effect, fact, input.occurredAt, context)) return "effect observation must be an exact canonical temporally ordered reconciliation fact";
    const exactExecution = (effect.kind === "run_procedure" && effect.boundStageAttemptId != null) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null);
    if (exactExecution) {
      const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
      const resultIdentityHash = effect.kind === "run_procedure" ? execution?.resultIdentityHash : execution?.hash;
      if (!execution || fact.executionObservationHash !== effect.executionObservationHash || fact.resultIdentityHash !== resultIdentityHash || fact.closedAt !== execution.completedAt || fact.reconciliation !== "applied_exact") return "execution reconciliation must close the exact immutable execution observation/result identity";
    }
    if (effect.kind === "cleanup_worktree" && !["applied_exact", "proven_absent"].includes(fact.reconciliation)) return "cleanup_worktree observation must be applied exactly or prove the exact bound worktree absent";
  }
  if (input.type === "record_cancellation") {
    const cancellation = state.cancellations[payload.cancellationId];
    if (!cancellation) return "cancellation observation references an unknown cancellation";
    const targetIds = cancellation.scope === "run" ? Object.keys(cancellation.fencedGenerations) : [cancellation.subjectId];
    const expectedWorkerAttemptIds = Object.values(state.stageAttempts).filter((attempt) => targetIds.includes(attempt.workItemId) && attempt.producerKind === "owned_worker" && !attempt.terminalAt && Boolean(state.workerBindings[attempt.stageAttemptId])).map(({ stageAttemptId }) => stageAttemptId).sort();
    if (JSON.stringify(payload.workerResults.map(({ stageAttemptId }: any) => stageAttemptId).sort()) !== JSON.stringify(expectedWorkerAttemptIds)) return "cancellation observation must cover every exact bound active worker attempt and must not invent a pre-bind result";
    for (const workerResult of payload.workerResults) {
      const binding = state.workerBindings[workerResult.stageAttemptId];
      const fact = context.facts[workerResult.result.hash];
      const attempt = state.stageAttempts[workerResult.stageAttemptId];
      if (!binding || !attempt || workerResult.result.kind !== "worker_result" || workerResult.result.bytes !== Buffer.byteLength(canonicalStringify(fact)) || fact?.kind !== "worker_result" || fact.hash !== workerResult.result.hash || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.workItemId !== attempt.workItemId || fact.stage !== attempt.stage || fact.stageAttemptId !== binding.stageAttemptId || fact.launchIntentId !== binding.launchIntentId || fact.workerStorageId !== binding.workerStorageId || fact.launchOwnerSessionId !== binding.launchOwnerSessionId || fact.workerId !== binding.workerId || fact.attemptNumber !== binding.attemptNumber || fact.attemptNonce !== binding.attemptNonce || fact.configHash !== binding.configHash || !["succeeded", "needs_attention", "failed", "cancelled", "lost"].includes(fact.terminalStatus) || !validWorkerGitOutputIdentity(fact, attempt, state, context)) return "cancellation worker result must prove exact attempt-bound terminal identity and Git output shape";
    }
    for (const observation of payload.effectObservations) {
      const effect = state.effects[observation.effectId];
      const fact = context.facts[observation.observationHash];
      if (!effect || fact?.kind !== "effect_reconciliation" || fact.hash !== observation.observationHash || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || !["applied_exact", "compensated", "proven_absent"].includes(fact.reconciliation) || !effectReconciliationTimeIsBounded(effect, fact, input.occurredAt, context)) return "cancellation observation must use exact canonical terminal reconciliation facts within the effect and input timeline";
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
      rebindOwnerAuthority(state, state.owner.ownerEpoch + 1);
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
      state.owner.lockIdentity = null; state.owner.attachedAt = null; state.owner.lastHeartbeatAt = null;
      state.owner.lastReleaseCommandId = input.commandId; state.owner.lastReleasePayloadHash = input.payloadHash;
      notices.push(notice(state, "owner_changed", state.runId, null));
      return null;
    }
    case "set_desired_run": {
      if (payload.desired === "running" && (state.desired.run !== "paused" || state.completion.state !== "open" || ["completed", "cancelled", "superseded"].includes(state.current.run) || Object.values(state.cancellations).some((candidate) => candidate.state !== "closed"))) return precondition("only a nonterminal, noncancelling paused run may resume");
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
          state.leases[leaseId] = { leaseId, kind: "operational", subject: { kind: "resource", id: namespace }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: item.candidateGeneration, units, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
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
      if (dagRunNeedsReplanV1(state) || state.desired.run !== "running" || !["active", "integration"].includes(state.current.run) || state.completion.state !== "open" || Object.values(state.cancellations).some((candidate) => candidate.state !== "closed")) return precondition("only an active, running, noncancelling run may dispatch scheduler work");
      const reservation = state.scheduler.reservations[payload.reservationId]; if (!reservation || reservation.state !== "reserved" || reservation.normalizedRequestHash !== payload.normalizedRequestHash) return precondition("only an exact durable reserved scheduler operation may enter dispatch intent"); reservation.state = "dispatch_intent"; effects.push({ effectId: reservation.reservationId, kind: `scheduler_${reservation.operationKind}`, requestHash: reservation.normalizedRequestHash }); notices.push(notice(state, "scheduler_dispatch_intended", reservation.reservationId, reservation.normalizedRequestHash)); return null;
    }
    case "record_scheduler_reservation_dispatch": {
      if (state.desired.run !== "running" || !["active", "integration"].includes(state.current.run) || state.completion.state !== "open" || Object.values(state.cancellations).some((candidate) => candidate.state !== "closed")) return precondition("only an active, running, noncancelling run may activate scheduler work");
      const reservation = state.scheduler.reservations[payload.reservationId]; if (!reservation || reservation.state !== "dispatch_intent" || reservation.normalizedRequestHash !== payload.normalizedRequestHash) return precondition("scheduler dispatch observation must bind the exact persisted dispatch intent"); reservation.state = payload.disposition; if (payload.disposition === "launch_ambiguous") { const blockerId = `launch-ambiguous-${reservation.reservationId}`; state.blockers[blockerId] = { blockerId, kind: "launch_ambiguous", subject: { kind: "work_item", id: reservation.workItemId }, stage: reservation.stage, sourceId: reservation.reservationId, sourceHash: reservation.normalizedRequestHash, release: "immutable_fact", active: true, createdAt: input.occurredAt, releasedAt: null, releaseReceipt: null }; if (!state.workItems[reservation.workItemId].blockerIds.includes(blockerId)) state.workItems[reservation.workItemId].blockerIds.push(blockerId); } notices.push(notice(state, "scheduler_dispatch_observed", reservation.reservationId, payload.normalizedRequestHash)); return null;
    }
    case "release_scheduler_reservation": {
      const reservation = state.scheduler.reservations[payload.reservationId];
      if (!reservation || ["released", "fenced"].includes(reservation.state)) return precondition("scheduler reservation is not active");
      const releaseError = releaseSchedulerReservationAccounting(state, reservation, payload.disposition, input.occurredAt, payload.reason);
      if (releaseError) return precondition(releaseError);
      notices.push(notice(state, "scheduler_released", reservation.reservationId, input.payloadHash));
      return null;
    }
    case "authorize_retry": {
      const entry = state.retryLedger[payload.retryKey]; const item = state.workItems[payload.workItemId];
      if (state.desired.run !== "running" || !["active", "integration"].includes(state.current.run) || state.completion.state !== "open" || Object.values(state.cancellations).some((candidate) => candidate.state !== "closed")) return precondition("terminal, paused, or cancelling runs cannot authorize retries");
      if (!entry || !item || entry.workItemId !== payload.workItemId || entry.stage !== payload.stage || entry.dimension !== payload.dimension || entry.fingerprint !== payload.fingerprint || item.candidateGeneration !== payload.candidateGeneration || ["complete", "cancelled", "superseded"].includes(item.current)) return precondition("retry request does not bind an existing exact nonterminal retry ledger slot");
      if (entry.count !== payload.expectedCount || entry.count >= entry.ceiling || entry.stop !== "none" || retryFailureCount(state, entry) <= entry.count) return precondition("retry count, observed failure authority, ceiling, or stop disposition rejects authorization");
      if (Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation))) return precondition("retry requires every prior effect reconciled");
      entry.count += 1; entry.lastRetryCommandId = input.commandId;
      notices.push(notice(state, "retry_authorized", payload.workItemId, payload.retryKey));
      return null;
    }
    case "begin_stage_attempt": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks new stage attempts");
      const reservation = state.scheduler.reservations[payload.reservationId];
      const item = reservation ? state.workItems[reservation.workItemId] : null;
      const stage = item && reservation ? item.stages[reservation.stage] : null;
      const inputFact = context.facts[payload.attemptInput.hash] as any;
      if (!reservation || !item || !stage || reservation.state !== "active" || reservation.ownerEpoch !== state.owner.ownerEpoch || reservation.authorizationSetHash !== state.identity.authorizationSet.hash || reservation.candidateGeneration !== item.candidateGeneration) return precondition("stage attempt must consume one exact active current-generation scheduler reservation");
      if (state.stageAttempts[payload.stageAttemptId] || stage.currentAttemptId !== null || stage.state === "passed" || reservation.attemptOrdinal !== stage.attemptIds.length + 1) return precondition("stage attempt natural identity or ordinal is already occupied");
      if (!exactFactRef(payload.attemptInput, inputFact, "stage_attempt_input") || inputFact.planHash !== state.identity.planHash || inputFact.runId !== state.runId || inputFact.runNonce !== state.runNonce || inputFact.workItemId !== item.workItemId || inputFact.stage !== reservation.stage || inputFact.stageAttemptId !== payload.stageAttemptId || inputFact.candidateGeneration !== item.candidateGeneration || inputFact.authorizationSetHash !== state.identity.authorizationSet.hash || inputFact.implementationLineageHash !== (["F1", "F3"].includes(reservation.stage) ? item.implementationLineageHash : null)) return precondition("attempt input must be an exact canonical current plan/run/item/stage/generation binding");
      const producerKind = fixedProducerForStage(reservation.stage, reservation.operationKind);
      if (!producerKind || inputFact.producerKind !== producerKind) return precondition("attempt input producer does not match the fixed scheduler operation");
      const expectedCandidateHash = ["F0", "F1"].includes(reservation.stage) ? null : item.candidate?.candidateHash ?? null;
      if (inputFact.candidateHash !== expectedCandidateHash || (!["F0", "F1"].includes(reservation.stage) && !item.candidate)) return precondition("attempt input candidate does not bind the exact current stage input");
      if (producerKind === "owned_worker") {
        const launch = payload.launchIntent; const effect = payload.launchEffect;
        if (!launch || !effect || state.launchIntents[launch.launchIntentId] || state.effects[effect.effectId] || launch.effectId !== effect.effectId || launch.workerId.length === 0 || launch.cwdRepositoryId !== item.writeRepositoryId) return precondition("owned-worker begin requires one pristine launch identity and effect");
        if (effect.kind !== "launch_worker" || effect.subject.kind !== "work_item" || effect.subject.id !== item.workItemId || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.boundOwnerEpoch !== state.owner.ownerEpoch || effect.boundAuthorizationSetHash !== state.identity.authorizationSet.hash || effect.boundFreshnessReceiptHash !== state.freshness.receipt.hash || effect.boundCandidateGeneration !== item.candidateGeneration || effect.observationHash !== null || effect.reconciliation !== "not_started") return precondition("owned-worker launch effect must be a pristine current-authority intent");
        state.effects[effect.effectId] = structuredClone(effect);
        state.launchIntents[launch.launchIntentId] = { ...structuredClone(launch), stageAttemptId: payload.stageAttemptId, ...(launch.dispatchProtocolVersion === 1 ? { state: "dispatchable" } : {}) };
      } else if (payload.launchIntent !== null || payload.launchEffect !== null) return precondition("non-worker stage attempt cannot create worker launch authority");
      for (const leaseId of reservation.leaseIds) {
        const lease = state.leases[leaseId];
        if (!lease || lease.state !== "active" || lease.holderStageAttemptId !== null || lease.holderIntegrationAttemptId !== null) return precondition("stage attempt reservation leases are not pristine and active");
        lease.holderStageAttemptId = payload.stageAttemptId;
      }
      const launchIntentId = payload.launchIntent?.launchIntentId ?? null;
      state.stageAttempts[payload.stageAttemptId] = { stageAttemptId: payload.stageAttemptId, workItemId: item.workItemId, stage: reservation.stage, ordinal: reservation.attemptOrdinal, producerKind, implementationLineageHash: inputFact.implementationLineageHash, inputGeneration: inputFact.candidateGeneration, reservedOutputGeneration: null, attemptInput: structuredClone(payload.attemptInput), authorizationSetHash: state.identity.authorizationSet.hash, state: producerKind === "owned_worker" ? payload.launchIntent?.dispatchProtocolVersion === 1 ? "dispatchable" : "launching" : "running", launchIntentId, leaseIds: [...reservation.leaseIds].sort(), workerResult: null, evidence: null, failure: null, createdAt: input.occurredAt, updatedAt: input.occurredAt, terminalAt: null };
      state.evidenceIndex.stageAttemptInputs[payload.stageAttemptId] = structuredClone(payload.attemptInput);
      stage.state = "active"; stage.attemptIds = [...stage.attemptIds, payload.stageAttemptId].sort(); stage.currentAttemptId = payload.stageAttemptId; stage.currentEvidence = null; stage.lastDisposition = null;
      item.current = "active"; item.currentStage = reservation.stage;
      notices.push(notice(state, "stage_attempt_begun", payload.stageAttemptId, inputFact.hash));
      return null;
    }
    case "bind_worker_attempt": {
      const attempt = state.stageAttempts[payload.stageAttemptId]; const binding = payload.binding; const launch = attempt?.launchIntentId ? state.launchIntents[attempt.launchIntentId] : null;
      const effect = launch ? state.effects[launch.effectId] : null; const configFact = context.facts[binding.configRef.hash] as any; const observation = context.facts[payload.launchObservation.hash] as any;
      const item = attempt ? state.workItems[attempt.workItemId] : null;
      const cancellation = attempt ? Object.values(state.cancellations).find((candidate) => candidate.state !== "closed" && candidate.fencedGenerations[attempt.workItemId] === item?.candidateGeneration) : undefined;
      const ordinaryLaunch = attempt?.state === "launching" && launch?.state === "dispatching" && effect?.state === "dispatching";
      const cancellationRecovery = attempt?.state === "cancelling" && launch?.state === "cancel_requested" && effect?.state === "ambiguous" && effect.reconciliation === "unknown" && Boolean(cancellation);
      if (!attempt || attempt.producerKind !== "owned_worker" || !launch || !effect || (!ordinaryLaunch && !cancellationRecovery) || effect.dispatchCount <= 0 || launch.dispatchCount !== effect.dispatchCount || state.workerBindings[payload.stageAttemptId]) return precondition("worker binding requires exact positive durable launch/effect dispatch or cancellation-recovery authority");
      if (effect.boundOwnerEpoch !== state.owner.ownerEpoch || effect.boundAuthorizationSetHash !== state.identity.authorizationSet.hash || (ordinaryLaunch && (effect.boundFreshnessReceiptHash !== state.freshness.receipt.hash || state.freshness.blocksNewLaunches))) return precondition("worker binding launch dispatch authority is stale");
      if (cancellationRecovery && (!item || item.desired !== "cancel" || attempt.inputGeneration + 1 !== item.candidateGeneration)) return precondition("worker binding cancellation recovery does not match the exact fenced attempt generation");
      const launchOwnerProven = exactOwnerLineageIncludes(state, context, configFact?.config?.launchOwner, binding.launchOwnerSessionId);
      if (binding.stageAttemptId !== attempt.stageAttemptId || binding.launchIntentId !== launch.launchIntentId || !launchOwnerProven || binding.launchOwnerSessionId !== observation?.launchOwnerSessionId || binding.workerId !== launch.workerId || binding.attemptNumber !== launch.expectedAttemptNumber || binding.heartbeatAt !== input.occurredAt || binding.supervisorPid <= 0 || binding.supervisorStartIdentity === null) return precondition("worker binding does not match the exact current or proven prior launch owner/config/attempt identity");
      const config = configFact?.config;
      if (!config || configFact.configHash !== binding.configHash || canonicalHash(config) !== binding.configHash || config.storageId !== binding.workerStorageId || config.ownerSessionId !== binding.launchOwnerSessionId || config.workerId !== binding.workerId || config.attemptNumber !== binding.attemptNumber || config.attemptNonce !== binding.attemptNonce || config.launchKey !== launch.launchKey || config.requestHash !== (launch.dispatchConfigRequestHash ?? launch.configRequestHash) || (launch.dispatchProtocolVersion === 1 && (canonicalHash(config.task) !== launch.promptHash || canonicalHash({ protocolVersion: 1, launchKey: launch.launchKey, workerId: launch.workerId, taskPacketHash: launch.taskPacketHash, directiveHash: launch.directiveHash, promptHash: launch.promptHash }) !== launch.dispatchConfigRequestHash)) || config.launchOwner?.sessionId !== binding.launchOwnerSessionId) return precondition("immutable worker config does not bind the exact launch owner, request, process, and attempt");
      const observedBinding = { workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, supervisorPid: binding.supervisorPid, supervisorStartIdentity: binding.supervisorStartIdentity };
      if (!observation || observation.ownerEpoch !== state.owner.ownerEpoch || observation.authorizationSetHash !== state.identity.authorizationSet.hash || observation.effectId !== effect.effectId || observation.requestHash !== effect.requestHash || observation.launchIntentId !== launch.launchIntentId || observation.launchKey !== launch.launchKey || Object.entries(observedBinding).some(([key, value]) => observation[key] !== value) || observation.observedAt !== input.occurredAt) return precondition("launch observation does not bind the exact dispatched worker identity");
      if (!item || (ordinaryLaunch ? item.candidateGeneration !== attempt.inputGeneration : cancellation!.fencedGenerations[attempt.workItemId] !== item.candidateGeneration) || attempt.authorizationSetHash !== state.identity.authorizationSet.hash) return precondition("worker binding is stale for the current or exactly fenced item generation/authorization");
      const genericIdentity = canonicalHash({ workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash });
      if (Object.entries(state.workerBindings).some(([otherId, other]) => otherId !== attempt.stageAttemptId && canonicalHash({ workerStorageId: other.workerStorageId, launchOwnerSessionId: other.launchOwnerSessionId, workerId: other.workerId, attemptNumber: other.attemptNumber, attemptNonce: other.attemptNonce, configHash: other.configHash }) === genericIdentity)) return precondition("generic worker/config/attempt identity is already bound to another lifecycle attempt");
      if (["F2", "F5"].includes(attempt.stage)) {
        const predecessorContexts = Object.entries(state.workerBindings).filter(([otherId]) => { const other = state.stageAttempts[otherId]; return other?.workItemId === attempt.workItemId && ["F1", "F2", "F5"].includes(other.stage) && (PLAN_STAGE_IDS.indexOf(other.stage) < PLAN_STAGE_IDS.indexOf(attempt.stage) || (other.stage === attempt.stage && other.ordinal < attempt.ordinal)); }).map(([otherId, otherBinding]) => ({ attempt: state.stageAttempts[otherId], launch: state.launchIntents[otherBinding.launchIntentId], binding: otherBinding }));
        if (predecessorContexts.some(({ attempt: otherAttempt, launch: otherLaunch, binding: other }) => other.workerId === binding.workerId || other.attemptNonce === binding.attemptNonce || other.configHash === binding.configHash || other.launchIntentId === binding.launchIntentId || otherLaunch?.launchKey === launch.launchKey || otherAttempt?.stageAttemptId === attempt.stageAttemptId)) return precondition(`${attempt.stage} requires a fresh exact worker/config/nonce and launch/intent/attempt identity distinct from predecessor implementation/evaluation/review contexts; manager storage may be shared`);
      }
      let recoveredCancellationEffect: any = null;
      if (cancellationRecovery) {
        const effectId = `cancel-${canonicalHash({ cancellationId: cancellation!.cancellationId, stageAttemptId: attempt.stageAttemptId }).slice("sha256:".length, "sha256:".length + 24)}`;
        if (state.effects[effectId]) return precondition("recovered worker cancellation effect identity collides with an existing effect");
        recoveredCancellationEffect = {
          effectId, kind: "cancel_worker", subject: { kind: "work_item", id: attempt.workItemId }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent",
          requestHash: canonicalHash({ kind: "cancel_worker", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, fencedGeneration: item.candidateGeneration }),
          boundOwnerEpoch: state.owner.ownerEpoch, boundAuthorizationSetHash: state.identity.authorizationSet.hash, boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: item.candidateGeneration,
          boundGateEpochHash: canonicalHash(item.gateIds.map((gateId) => state.gates[gateId])), boundStageAttemptId: attempt.stageAttemptId, boundWorkerResultHash: null,
          state: "intended", dispatchCount: 0, createdRevision: state.revision + 1, createdAt: input.occurredAt, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null,
        };
      }
      state.workerBindings[payload.stageAttemptId] = structuredClone(binding); launch.state = cancellationRecovery ? "cancel_requested" : "bound"; launch.boundAt = input.occurredAt; attempt.state = cancellationRecovery ? "cancelling" : "running"; attempt.updatedAt = input.occurredAt;
      effect.state = "reconciled"; effect.reconciliation = "applied_exact"; effect.observationHash = observation.hash;
      if (effect.blockerId && state.blockers[effect.blockerId]?.active) { state.blockers[effect.blockerId].active = false; state.blockers[effect.blockerId].releasedAt = input.occurredAt; state.blockers[effect.blockerId].releaseReceipt = observation.hash; }
      if (recoveredCancellationEffect) { state.effects[recoveredCancellationEffect.effectId] = recoveredCancellationEffect; cancellation!.effectIds = [...cancellation!.effectIds, recoveredCancellationEffect.effectId].sort(); }
      notices.push(notice(state, cancellationRecovery ? "worker_attempt_recovered_for_cancellation" : "worker_attempt_bound", payload.stageAttemptId, observation.hash));
      return null;
    }
    case "record_worker_result": {
      const attempt = state.stageAttempts[payload.stageAttemptId]; const binding = state.workerBindings[payload.stageAttemptId]; const fact = context.facts[payload.result.hash] as any;
      if (!attempt || !binding || attempt.producerKind !== "owned_worker" || !["running", "settling"].includes(attempt.state) || attempt.workerResult || binding.resultHash) return precondition("worker result requires one exact active bound worker attempt");
      const item = state.workItems[attempt.workItemId];
      if (!item || item.candidateGeneration !== attempt.inputGeneration || item.stages[attempt.stage].currentAttemptId !== attempt.stageAttemptId || attempt.authorizationSetHash !== state.identity.authorizationSet.hash) return precondition("worker result is stale for the current attempt generation");
      if (!exactFactRef(payload.result, fact, "worker_result") || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.workItemId !== attempt.workItemId || fact.stage !== attempt.stage || fact.stageAttemptId !== attempt.stageAttemptId || fact.launchIntentId !== binding.launchIntentId || fact.workerStorageId !== binding.workerStorageId || fact.launchOwnerSessionId !== binding.launchOwnerSessionId || fact.workerId !== binding.workerId || fact.attemptNumber !== binding.attemptNumber || fact.attemptNonce !== binding.attemptNonce || fact.configHash !== binding.configHash || !validWorkerGitOutputIdentity(fact, attempt, state, context) || (fact.candidateObservedAt !== null && (utcTimestampOrderValue(fact.candidateObservedAt) < utcTimestampOrderValue(attempt.createdAt) || utcTimestampOrderValue(fact.candidateObservedAt) > utcTimestampOrderValue(input.occurredAt)))) return precondition("worker result does not prove the exact plan/run/item/stage/attempt/launch, terminal identity, and nullable Git output identity");
      attempt.workerResult = structuredClone(payload.result); attempt.state = "result_observed"; attempt.updatedAt = input.occurredAt;
      binding.completionId = fact.completionId; binding.resultHash = fact.hash;
      if (attempt.launchIntentId) state.launchIntents[attempt.launchIntentId].state = "closed";
      state.evidenceIndex.workerResults[fact.hash] = structuredClone(payload.result);
      notices.push(notice(state, "worker_result_recorded", payload.stageAttemptId, fact.hash));
      return null;
    }
    case "record_candidate": {
      const attempt = state.stageAttempts[payload.stageAttemptId]; const fact = context.facts[payload.candidate.hash] as any;
      const item = attempt ? state.workItems[attempt.workItemId] : null; const resultFact = attempt?.workerResult ? context.facts[attempt.workerResult.hash] as any : null;
      if (!attempt || !item || !["F1", "F3"].includes(attempt.stage) || attempt.reservedOutputGeneration !== null || item.stages[attempt.stage].currentAttemptId !== attempt.stageAttemptId) return precondition("candidate observation requires the current unconsumed F1/F3 output slot");
      if (attempt.producerKind === "owned_worker" && (attempt.state !== "result_observed" || resultFact?.terminalStatus !== "succeeded")) return precondition("candidate observation requires the exact successful owned-worker result");
      const priorCandidate = item.candidate ? structuredClone(item.candidate) : null;
      const expectedGeneration = item.candidate === null && item.candidateGeneration > 0 ? item.candidateGeneration : item.candidateGeneration + 1;
      if (!exactFactRef(payload.candidate, fact, "candidate") || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.workItemId !== item.workItemId || fact.generation !== expectedGeneration || fact.producedByStageAttemptId !== attempt.stageAttemptId || fact.lineageHash !== item.implementationLineageHash || fact.git?.repositoryId !== item.writeRepositoryId || fact.base?.repositoryId !== item.writeRepositoryId) return precondition("candidate fact is stale or does not bind the exact reserved output identity");
      if (!resultFact || resultFact.terminalStatus !== "succeeded" || resultFact.outputRepositoryId !== item.writeRepositoryId || resultFact.outputSourceBase === null || canonicalHash(resultFact.outputSourceBase) !== canonicalHash(fact.base) || resultFact.outputCommit !== fact.git.commit || resultFact.outputTree !== fact.git.tree || resultFact.outputObjectFormat !== (fact.git.commit.length === 40 ? "sha1" : "sha256") || resultFact.candidateObservedAt === null || utcTimestampOrderValue(resultFact.candidateObservedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("F1/F3 candidate must exactly join the successful worker result repository/source-base/commit/tree/object-format observation");
      const transition = payload.f2Transition ? context.facts[payload.f2Transition.hash] as any : null;
      const procedureExecution = payload.procedureExecution ? context.facts[payload.procedureExecution.hash] as any : null;
      if (attempt.stage === "F1" && (payload.f2Transition || payload.procedureExecution)) return precondition("F1 candidate generation cannot carry an F2 transition or delta execution");
      if (attempt.stage === "F3") {
        const f2 = item.stages.F2; const f2Evidence = f2.currentEvidence ? context.facts[f2.currentEvidence] as any : null;
        if (!priorCandidate || f2.state !== "passed" || !f2Evidence || !payload.f2Transition || !exactFactRef(payload.f2Transition, transition, transition?.kind)) return precondition("F3 candidate generation must atomically carry exact prior-F2 adoption or invalidation authority");
        const common = transition.planHash === state.identity.planHash && transition.runId === state.runId && transition.runNonce === state.runNonce && transition.workItemId === item.workItemId && transition.stage === "F2" && transition.fromCandidateGeneration === priorCandidate.generation && transition.fromCandidateHash === priorCandidate.candidateHash && transition.toCandidateGeneration === expectedGeneration && transition.toCandidateHash === fact.hash && transition.f3StageAttemptId === attempt.stageAttemptId;
        if (transition.kind === "adoption") {
          const deltaProcedure = context.catalog.procedures[transition.deltaAttestationProcedureHash]; const executable = deltaProcedure?.executable;
          const exactExecution = exactFactRef(payload.procedureExecution, procedureExecution, "procedure_execution") && procedureExecution.planHash === state.identity.planHash && procedureExecution.runId === state.runId && procedureExecution.runNonce === state.runNonce && procedureExecution.authorizationSetHash === state.identity.authorizationSet.hash && procedureExecution.workItemId === item.workItemId && procedureExecution.stage === "F3" && procedureExecution.stageAttemptId === attempt.stageAttemptId && procedureExecution.attemptInputHash === attempt.attemptInput.hash && procedureExecution.fromCandidateGeneration === priorCandidate.generation && procedureExecution.fromCandidateHash === priorCandidate.candidateHash && procedureExecution.toCandidateGeneration === expectedGeneration && procedureExecution.toCandidateHash === fact.hash && procedureExecution.procedureHash === transition.deltaAttestationProcedureHash && procedureExecution.environmentProfileHash === transition.environmentProfileHash && procedureExecution.executableArtifactHash === executable?.executableArtifactHash && procedureExecution.environmentHash === executable?.environmentHash && procedureExecution.disposition === "PASS" && utcTimestampOrderValue(procedureExecution.startedAt) >= utcTimestampOrderValue(attempt.createdAt) && utcTimestampOrderValue(procedureExecution.completedAt) >= utcTimestampOrderValue(procedureExecution.startedAt) && utcTimestampOrderValue(procedureExecution.occurredAt) >= utcTimestampOrderValue(procedureExecution.completedAt) && utcTimestampOrderValue(procedureExecution.occurredAt) <= utcTimestampOrderValue(input.occurredAt);
          if (!common || transition.evidenceHash !== f2.currentEvidence || transition.sourceEvidenceProcedureHash !== f2Evidence.procedureHash || transition.environmentProfileHash !== f2Evidence.environmentProfileHash || transition.deltaAttestationExecutionHash !== procedureExecution?.hash || transition.occurredAt !== procedureExecution?.occurredAt || utcTimestampOrderValue(transition.occurredAt) > utcTimestampOrderValue(input.occurredAt) || transition.evidenceOnlyDelta !== true || !deltaProcedure || deltaProcedure.purpose !== "evidence_only_delta_attestation" || !deltaProcedure.stages.includes("F3") || !deltaProcedure.readOnly || deltaProcedure.environmentProfileHash !== transition.environmentProfileHash || !executable || executable.readOnly !== true || executable.noEdit !== true || !exactExecution) return precondition("F3 evidence-only generation requires exact immutable executable PASS delta attestation bound to prior F2 evidence and both candidate trees");
        } else if (transition.kind === "invalidation") {
          if (payload.procedureExecution || !common || transition.authorizationSetHash !== state.identity.authorizationSet.hash || transition.priorEvidenceHash !== f2.currentEvidence || !["behavior_bearing", "unknown_impact"].includes(transition.reason) || transition.rerouteStage !== "F2") return precondition("behavior-bearing/unknown F3 generation requires exact immutable F2 invalidation and reroute authority without unrelated procedure execution");
        } else return precondition("F3 transition fact must be adoption or invalidation");
      }
      attempt.reservedOutputGeneration = expectedGeneration; attempt.updatedAt = input.occurredAt; item.candidateGeneration = expectedGeneration;
      item.candidate = { generation: fact.generation, candidateId: fact.candidateId, candidateHash: fact.hash, base: structuredClone(fact.base), git: structuredClone(fact.git), patchIdentityHash: fact.patchIdentityHash, producedByStageAttemptId: fact.producedByStageAttemptId, lineageHash: fact.lineageHash };
      state.evidenceIndex.candidates[fact.hash] = structuredClone(payload.candidate);
      if (attempt.stage === "F3" && transition?.kind === "adoption") {
        (state.evidenceIndex as any).procedureExecutions ??= {};
        (state.evidenceIndex as any).procedureExecutions[payload.procedureExecution.hash] = structuredClone(payload.procedureExecution);
        (state.evidenceIndex.adoptions as any)[payload.f2Transition.id] = structuredClone(payload.f2Transition);
        item.stages.F2.adoptionReceipt = transition.hash;
      } else if (attempt.stage === "F3" && transition?.kind === "invalidation") {
        (state.evidenceIndex.invalidations as any)[payload.f2Transition.id] = structuredClone(payload.f2Transition);
        item.stages.F2.state = "invalidated"; item.stages.F2.currentAttemptId = null; item.stages.F2.currentEvidence = null; item.stages.F2.adoptionReceipt = null;
        item.stages.F2.lastDisposition = null; item.stages.F2.invalidationIds = [...new Set([...item.stages.F2.invalidationIds, payload.f2Transition.id])].sort();
        item.stages.F3.state = "invalidated"; item.stages.F3.currentAttemptId = null; item.stages.F3.currentEvidence = null; item.stages.F3.lastDisposition = null;
        attempt.state = "failed"; attempt.updatedAt = input.occurredAt; attempt.terminalAt = input.occurredAt;
        const reservation = Object.values(state.scheduler.reservations).find((candidate) => candidate.leaseIds.length === attempt.leaseIds.length && candidate.leaseIds.every((leaseId) => attempt.leaseIds.includes(leaseId)) && !["released", "fenced"].includes(candidate.state));
        if (!reservation) return precondition("F3 invalidation reroute cannot resolve its exact scheduler reservation");
        const releaseError = releaseSchedulerReservationAccounting(state, reservation, "released", input.occurredAt, "F3 candidate invalidated and rerouted F2");
        if (releaseError) return precondition(releaseError);
        item.current = "active"; item.currentStage = "F2";
      }
      notices.push(notice(state, "candidate_recorded", payload.stageAttemptId, fact.hash));
      return null;
    }
    case "record_finding": {
      const fact = context.facts[payload.finding.hash] as any;
      const attempt = fact?.stageAttemptId ? state.stageAttempts[fact.stageAttemptId] : undefined;
      const item = attempt ? state.workItems[attempt.workItemId] : undefined;
      const evidenceFact = fact?.evidenceHash ? context.facts[fact.evidenceHash] as any : undefined;
      if (!exactFactRef(payload.finding, fact, "finding") || payload.finding.id !== fact.findingId || !attempt || !item || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.workItemId !== attempt.workItemId || fact.stage !== attempt.stage || fact.attemptInputHash !== attempt.attemptInput.hash || evidenceFact?.hash !== fact.evidenceHash || evidenceFact.hash !== canonicalHash(Object.fromEntries(Object.entries(evidenceFact).filter(([key]) => key !== "hash"))) || evidenceFact.stageAttemptId !== attempt.stageAttemptId || utcTimestampOrderValue(fact.observedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("finding must bind exact immutable plan/run/authorization/work-item/stage/attempt/input/evidence identity");
      if (state.findingClosures[fact.findingId] || state.evidenceIndex.findings[fact.findingId]) return precondition("finding immutable ID slot is already occupied");
      const duplicateCurrent = Object.values(state.findingClosures).some((finding) => finding.workItemId === fact.workItemId && finding.semanticSubjectId === fact.semanticSubjectId && finding.fingerprint === fact.fingerprint && ["open", "successor_plan_required"].includes(finding.state));
      if (duplicateCurrent) return precondition("only one current finding may own an exact work-item/semantic-subject/fingerprint identity");
      state.evidenceIndex.findings[fact.findingId] = structuredClone(payload.finding);
      state.findingClosures[fact.findingId] = { findingId: fact.findingId, findingHash: fact.hash, workItemId: fact.workItemId, stage: fact.stage, stageAttemptId: fact.stageAttemptId, attemptInputHash: fact.attemptInputHash, introducedByEvidenceHash: fact.evidenceHash, kind: fact.findingKind, severity: fact.severity, materiality: fact.materiality, fingerprint: fact.fingerprint, semanticSubjectId: fact.semanticSubjectId, state: "open", resolutionHash: null, supersedingEvidenceHash: null };
      item.openFindingIds = [...item.openFindingIds, fact.findingId].sort();
      if (fact.severity === "blocking" && fact.materiality === "plan_affecting") {
        state.desired = { run: "needs_replan", reason: `Blocking plan-affecting finding ${fact.findingId}`, requestedAt: input.occurredAt, requestedBy: "conductor" };
        notices.push(notice(state, "run_needs_replan", state.runId, fact.hash));
      }
      notices.push(notice(state, "finding_recorded", fact.findingId, fact.hash));
      return null;
    }
    case "record_finding_resolution": {
      const fact = context.facts[payload.resolution.hash] as any;
      const finding = fact?.findingId ? state.findingClosures[fact.findingId] : undefined;
      const attempt = finding ? state.stageAttempts[finding.stageAttemptId] : undefined;
      const item = finding ? state.workItems[finding.workItemId] : undefined;
      const findingFact = finding ? context.facts[finding.findingHash] as any : undefined;
      if (!exactFactRef(payload.resolution, fact, "finding_resolution") || payload.resolution.id !== fact.findingId || !finding || !attempt || !item || finding.state !== "open" || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.findingHash !== finding.findingHash || fact.workItemId !== finding.workItemId || fact.stage !== finding.stage || fact.stageAttemptId !== finding.stageAttemptId || fact.attemptInputHash !== finding.attemptInputHash || utcTimestampOrderValue(fact.resolvedAt) < utcTimestampOrderValue(findingFact?.observedAt) || utcTimestampOrderValue(fact.resolvedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("finding resolution must bind the sole exact current finding and immutable stage/attempt/input identity at a non-future time");
      if (finding.materiality === "plan_affecting") {
        if (!["successor_plan_required", "dismissed", "misclassified"].includes(fact.disposition) || fact.supersedingEvidenceHash !== null) return precondition("plan-affecting finding requires an explicit successor-required, dismissed, or misclassified disposition without local superseding evidence");
      } else if (!["corrected", "equivalent_accepted", "invalidated"].includes(fact.disposition) || !exactFindingResolutionEvidenceAtIngestion(state, context, finding, findingFact, fact)) return precondition("local finding resolution requires exact same-item canonical stage or typed correction evidence at a valid candidate generation");
      const superseding = fact.supersedingEvidenceHash ? context.facts[fact.supersedingEvidenceHash] as any : null;
      if (superseding?.kind === "finding_correction") { (state.evidenceIndex as any).findingCorrections ??= {}; (state.evidenceIndex as any).findingCorrections[superseding.hash] = { kind: "finding_correction", schemaVersion: 1, id: superseding.hash.slice(7, 31), hash: superseding.hash, bytes: Buffer.byteLength(canonicalStringify(superseding)), mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }; }
      state.evidenceIndex.findingResolutions[fact.findingId] = structuredClone(payload.resolution);
      finding.state = fact.disposition; finding.resolutionHash = fact.hash; finding.supersedingEvidenceHash = fact.supersedingEvidenceHash;
      item.openFindingIds = item.openFindingIds.filter((id) => id !== fact.findingId);
      if (fact.disposition === "successor_plan_required") item.openFindingIds = [...item.openFindingIds, fact.findingId].sort();
      if (state.desired.run === "needs_replan" && !dagRunNeedsReplanV1(state)) {
        state.desired = { run: "running", reason: null, requestedAt: input.occurredAt, requestedBy: "conductor" };
        notices.push(notice(state, "run_replan_hold_cleared", state.runId, fact.hash));
      }
      notices.push(notice(state, "finding_resolved", fact.findingId, fact.hash));
      return null;
    }
    case "seal_stage_attempt":
      return sealStageAttempt(state, input, payload, context, notices, false);
    case "seal_f8_integration_ready":
      return sealStageAttempt(state, input, payload, context, notices, true);
    case "reserve_integration_attempt": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks new integration attempts");
      if (state.desired.run !== "running" || state.freshness.blocksIntegration) return precondition("integration reservation requires a running integration-fresh run");
      const item = state.workItems[payload.workItemId]; const repository = state.repositories[payload.repositoryId]; const train = state.integrationTrains[payload.repositoryId];
      const planTrain = context.plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === payload.repositoryId);
      const member = planTrain?.members.find(({ workItemId }) => workItemId === payload.workItemId);
      const integrationReservation = Object.values(state.scheduler.reservations).find((reservation) => reservation.workItemId === payload.workItemId && reservation.stage === "F8" && reservation.operationKind === "integration" && reservation.state === "active");
      if (!item || !repository || !train || !planTrain || !member || item.writeRepositoryId !== payload.repositoryId || item.current !== "integrating" || !integrationReservation || integrationReservation.candidateGeneration !== item.candidateGeneration || !item.integrationReadyReceipt || !item.candidate || item.candidate.candidateHash !== payload.sourceCandidateHash) return precondition("integration attempt does not bind one exact scheduler-active integration-ready train member and candidate");
      const headOrdinal = train.entryOrder.filter((entryId) => train.entries[entryId]?.state === "integrated").length;
      if (member.ordinal !== headOrdinal || train.activeIntegrationAttemptId || state.integrationAttempts[payload.integrationAttemptId]) return precondition("only the exact current train head may reserve one integration attempt");
      const retryAuthorization = payload.retryAuthorizationKey ? state.retryLedger[payload.retryAuthorizationKey] : null;
      if ((payload.retryOrdinal === 0) !== (payload.retryAuthorizationKey === null) || (payload.retryOrdinal > 0 && (!retryAuthorization || retryAuthorization.workItemId !== item.workItemId || retryAuthorization.dimension !== "integration" || retryAuthorization.count !== payload.retryOrdinal || retryAuthorization.lastRetryCommandId === null))) return precondition("integration retry must bind an exact previously authorized integration retry ledger slot");
      if (canonicalHash(payload.sourceBase) !== canonicalHash(item.candidate.base) || canonicalHash(payload.sourceCandidate) !== canonicalHash(item.candidate.git) || canonicalHash(payload.expectedPrefix) !== canonicalHash(train.acceptedPrefix) || canonicalHash(payload.expectedTarget) !== canonicalHash(train.expectedTarget)) return precondition("integration source, prefix, or target identity differs from current authority");
      const readyFact = context.facts[item.integrationReadyReceipt] as any;
      if (readyFact?.kind !== "integration_ready" || readyFact.hash !== item.integrationReadyReceipt || readyFact.workItemId !== item.workItemId || readyFact.candidateGeneration !== item.candidateGeneration || readyFact.candidateHash !== item.candidate.candidateHash) return precondition("integration readiness fact is missing or stale");
      const bindingFact = context.facts[payload.repositoryBindingFactHash] as any;
      if (!exactGitFact(bindingFact, state, "repository_binding", payload.repositoryId, null) || !gitFactTimeWithinInput(bindingFact, state, input) || bindingFact.reconciliation !== "applied_exact" || bindingFact.ownerEpoch !== state.owner.ownerEpoch || bindingFact.targetRef !== repository.targetRef || bindingFact.commit !== payload.expectedTarget.commit || bindingFact.tree !== payload.expectedTarget.tree || bindingFact.objectFormat !== (payload.expectedTarget.commit.length === 40 ? "sha1" : "sha256")) return precondition("repository binding fact does not prove the exact current non-future target/common-dir identity");
      if (repository.workspace.gitCommonDirIdentityHash && repository.workspace.gitCommonDirIdentityHash !== bindingFact.commonDirIdentityHash) return precondition("repository common-dir identity conflicts with prior session binding");
      if (Object.values(state.repositories).some((candidate) => candidate.repositoryId !== repository.repositoryId && candidate.integrationLockLeaseId !== null && candidate.workspace.gitCommonDirIdentityHash === bindingFact.commonDirIdentityHash && state.leases[candidate.integrationLockLeaseId]?.state !== "released")) return precondition("another repository identity already holds the exact same Git common-directory integration lock");
      const effect = payload.compositionEffect;
      if (effect.kind !== "compose_candidate" || effect.effectId !== `${payload.integrationAttemptId}-compose` || effect.subject.kind !== "train" || effect.subject.id !== planTrain.trainId || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.boundOwnerEpoch !== state.owner.ownerEpoch || effect.boundAuthorizationSetHash !== state.identity.authorizationSet.hash || effect.boundFreshnessReceiptHash !== state.freshness.receipt.hash) return precondition("integration composition effect must be one pristine current-authority intent");
      const lockLease = { leaseId: payload.lockLeaseId, kind: "integration_lock" as const, subject: { kind: "repository" as const, id: payload.repositoryId }, holderStageAttemptId: null, holderIntegrationAttemptId: payload.integrationAttemptId, candidateGeneration: item.candidateGeneration, units: 1, ownerEpoch: state.owner.ownerEpoch, state: "active" as const, acquiredAt: input.occurredAt, expiresAt: null, releasedAt: null, releaseReason: null };
      if (state.leases[payload.lockLeaseId] || repository.integrationLockLeaseId || train.lockLeaseId) return precondition("repository integration lock lease slot is already occupied");
      state.leases[payload.lockLeaseId] = lockLease; repository.integrationLockLeaseId = payload.lockLeaseId; train.lockLeaseId = payload.lockLeaseId;
      repository.workspace.gitCommonDirIdentityHash = bindingFact.commonDirIdentityHash; repository.workspace.gitWorktreeIdentityHash = bindingFact.worktreeIdentityHash; repository.workspace.observationReceipt = bindingFact.hash; repository.observationReceipt = bindingFact.hash; repository.observedTarget = payload.expectedTarget; repository.observedTargetAt = bindingFact.observedAt; state.freshness.repositoryObservationHashes[payload.repositoryId] = bindingFact.hash;
      const priorEntry = train.entries[payload.entryId];
      const pristineEntry = priorEntry && priorEntry.workItemId === item.workItemId && priorEntry.ordinal === member.ordinal && ["eligible", "waiting"].includes(priorEntry.state) && priorEntry.attemptIds.length === 0 && priorEntry.currentAttemptId === null && priorEntry.integrationReceipt === null && priorEntry.integrationReadyHash === item.integrationReadyReceipt && priorEntry.sourceCandidate.candidateHash === item.candidate.candidateHash;
      const retryEntry = priorEntry && priorEntry.workItemId === item.workItemId && priorEntry.ordinal === member.ordinal && priorEntry.state === "invalidated" && state.integrationAttempts[priorEntry.currentAttemptId ?? ""]?.conflictClass !== "none";
      if (!priorEntry || (!pristineEntry && !retryEntry)) return precondition("integration entry must be the exact pristine F8 enqueue or an exact retryable conflict");
      if (retryEntry) { const priorAttempt = state.integrationAttempts[priorEntry.currentAttemptId!]; const conflictFact = priorAttempt?.compositionFactHash ? context.facts[priorAttempt.compositionFactHash] as any : null; const candidateFact = context.facts[item.candidate.candidateHash] as any; const producerAttempt = state.stageAttempts[item.candidate.producedByStageAttemptId]; if (item.candidate.generation !== priorEntry.sourceCandidate.generation + 1 || candidateFact?.kind !== "candidate" || candidateFact.producedByStageAttemptId !== item.candidate.producedByStageAttemptId || producerAttempt?.stage !== "F1" || producerAttempt.state !== "sealed" || producerAttempt.reservedOutputGeneration !== item.candidate.generation || !conflictFact || utcTimestampOrderValue(producerAttempt.createdAt) < utcTimestampOrderValue(conflictFact.observedAt)) return precondition("integration retry requires a fresh exact post-conflict F1 candidate generation"); }
      if (Object.values(state.integrationAttempts).some((candidate) => candidate.entryId === payload.entryId && candidate.retryOrdinal === payload.retryOrdinal)) return precondition("integration retry ordinal/authorization has already been consumed by an immutable attempt");
      for (const leaseId of integrationReservation.leaseIds) {
        const lease = state.leases[leaseId];
        if (!lease || lease.state !== "active" || lease.holderStageAttemptId !== null || lease.holderIntegrationAttemptId !== null || lease.candidateGeneration !== item.candidateGeneration) return precondition("integration reservation requires every exact active F8 lease to be pristine and generation-bound");
      }
      for (const leaseId of integrationReservation.leaseIds) state.leases[leaseId].holderIntegrationAttemptId = payload.integrationAttemptId;
      priorEntry.state = "reserved"; priorEntry.attemptIds = [...priorEntry.attemptIds, payload.integrationAttemptId]; priorEntry.currentAttemptId = payload.integrationAttemptId; priorEntry.integrationReadyHash = item.integrationReadyReceipt; priorEntry.sourceCandidate = structuredClone(item.candidate);
      train.activeIntegrationAttemptId = payload.integrationAttemptId;
      state.effects[effect.effectId] = structuredClone(effect);
      state.integrationAttempts[payload.integrationAttemptId] = { integrationAttemptId: payload.integrationAttemptId, entryId: payload.entryId, retryOrdinal: payload.retryOrdinal, sourceCandidateHash: payload.sourceCandidateHash, strategy: "merge_tree_one_parent", compositionProfileHash: planTrain.compositionProfileHash, prefixValidationProfileHash: planTrain.prefixValidationProfileHash, finalValidationProfileHash: planTrain.finalValidationProfileHash, sourceBase: structuredClone(payload.sourceBase), sourceCandidate: structuredClone(payload.sourceCandidate), expectedPrefix: structuredClone(payload.expectedPrefix), expectedTarget: structuredClone(payload.expectedTarget), temporaryRef: payload.temporaryRef, temporaryWorkspaceReceipt: null, compositionEffectId: effect.effectId, composedTree: null, syntheticParentCommit: null, sourceToIntegratedLineageHash: null, conflictClass: "none", prefixEvidenceHashes: [], finalEvidenceHashes: [], prefixEffectReconciliationHashes: [], finalEffectReconciliationHashes: [], environmentClosureHash: null, landingEffectId: null, landingState: "none", intendedLandedTree: null, integrationReceipt: null, repositoryBindingFactHash: bindingFact.hash, privateRefFactHashes: [], compositionFactHash: null, proposalVerificationFactHash: null, landingObservationFactHash: null };
      item.integrationEntryId = payload.entryId; item.current = "integrating"; train.entries[payload.entryId].state = "composing";
      notices.push(notice(state, "integration_reserved", payload.integrationAttemptId, bindingFact.hash));
      return null;
    }
    case "record_git_composition": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId];
      if (!attempt || !train || !entry || entry.currentAttemptId !== attempt.integrationAttemptId || entry.state !== "composing" || payload.conflictClass !== "none") return precondition("composition observation must bind the active clean train attempt");
      const fact = context.facts[payload.compositionFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any; const effect = state.effects[attempt.compositionEffectId];
      if (!sameGitRepositoryBinding(fact, bindingFact) || effect?.state !== "dispatching" || effect.dispatchCount < 1 || !exactGitFact(fact, state, "composition", train.repositoryId, attempt.integrationAttemptId) || !gitFactTimeWithinInput(fact, state, input, bindingFact.observedAt, effect.createdAt, effect.lastDispatchAt) || fact.effectId !== effect?.effectId || fact.requestHash !== effect?.requestHash || fact.ownerEpoch !== effect?.boundOwnerEpoch || fact.reconciliation !== "applied_exact" || fact.commit !== payload.composedTree.commit || fact.tree !== payload.composedTree.tree || fact.parentCommit !== payload.syntheticParentCommit || payload.syntheticParentCommit !== attempt.expectedPrefix.commit) return precondition("composition fact does not prove exact non-future one-parent composed commit/tree after dispatch");
      const privateRefFacts = payload.privateRefFactHashes.map((hash: string) => context.facts[hash] as any);
      for (const anchor of privateRefFacts) { if (!exactGitFact(anchor, state, "private_ref", train.repositoryId, attempt.integrationAttemptId) || !gitFactTimeWithinInput(anchor, state, input, bindingFact.observedAt, effect.createdAt, effect.lastDispatchAt) || utcTimestampOrderValue(anchor.observedAt) > utcTimestampOrderValue(fact.observedAt) || anchor.ownerEpoch !== effect.boundOwnerEpoch || anchor.commonDirIdentityHash !== fact.commonDirIdentityHash || anchor.worktreeIdentityHash !== fact.worktreeIdentityHash || anchor.gitConfigHash !== fact.gitConfigHash || anchor.gitVersionHash !== fact.gitVersionHash || anchor.objectFormat !== fact.objectFormat || anchor.reconciliation !== "applied_exact") return precondition("composition private-ref fact is missing, conflicting, future, or later than its composition observation"); }
      effect.state = "reconciled"; effect.reconciliation = "applied_exact";
      attempt.composedTree = structuredClone(payload.composedTree); attempt.syntheticParentCommit = payload.syntheticParentCommit; attempt.sourceToIntegratedLineageHash = payload.sourceToIntegratedLineageHash; attempt.conflictClass = "none"; attempt.compositionFactHash = fact.hash; attempt.privateRefFactHashes = [...payload.privateRefFactHashes].sort();
      entry.state = "verifying_prefix"; notices.push(notice(state, "integration_composed", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "record_git_composition_conflict": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId]; const item = entry ? state.workItems[entry.workItemId] : null; const effect = attempt ? state.effects[attempt.compositionEffectId] : null;
      if (!attempt || !train || !entry || !item || entry.currentAttemptId !== attempt.integrationAttemptId || entry.state !== "composing" || effect?.state !== "dispatching" || effect.dispatchCount < 1) return precondition("composition conflict must bind the exact active dispatched train attempt");
      const fact = context.facts[payload.compositionFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any;
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "composition", train.repositoryId, attempt.integrationAttemptId) || !gitFactTimeWithinInput(fact, state, input, bindingFact.observedAt, effect.createdAt, effect.lastDispatchAt) || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.ownerEpoch !== effect.boundOwnerEpoch || fact.reconciliation !== "conflict" || fact.commit !== null || fact.tree !== null) return precondition("composition conflict fact must prove an exact non-future post-dispatch no-result conflict observation");
      attempt.conflictClass = payload.conflictClass; attempt.compositionFactHash = fact.hash; effect.state = "failed"; effect.reconciliation = "conflict"; entry.state = "invalidated"; train.activeIntegrationAttemptId = null;
      item.candidateGeneration += 1; item.candidate = null; item.integrationReadyReceipt = null; item.integrationEntryId = null; item.current = "active"; item.currentStage = "F1";
      for (const stageId of PLAN_STAGE_IDS.slice(1)) { const stage = item.stages[stageId]; stage.state = "pending"; stage.currentAttemptId = null; stage.currentEvidence = null; stage.adoptionReceipt = null; stage.lastDisposition = null; stage.blockerIds = []; }
      const lockReleaseError = releaseIntegrationLock(state, train.repositoryId, attempt.integrationAttemptId, input.occurredAt, "exact composition conflict");
      if (lockReleaseError) return precondition(lockReleaseError);
      const reservationReleaseError = releaseWorkItemReservations(state, item.workItemId, "released", input.occurredAt, "exact composition conflict");
      if (reservationReleaseError) return precondition(reservationReleaseError);
      notices.push(notice(state, "integration_conflict", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "record_proposal_verification": {
      const attempt = state.integrationAttempts[payload.integrationAttemptId]; const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId])); const entry = train?.entries[attempt?.entryId];
      if (!attempt || !train || !entry || entry.state !== "verifying_prefix" || !attempt.composedTree) return precondition("proposal verification requires one exact composed train attempt");
      const fact = context.facts[payload.proposalVerificationFactHash] as any; const bindingFact = context.facts[attempt.repositoryBindingFactHash] as any;
      const verificationClosure = { prefixEvidenceHashes: [...payload.prefixEvidenceHashes].sort(), finalEvidenceHashes: [...payload.finalEvidenceHashes].sort(), prefixEffectReconciliationHashes: [...payload.prefixEffectReconciliationHashes].sort(), finalEffectReconciliationHashes: [...payload.finalEffectReconciliationHashes].sort(), environmentClosureHash: payload.environmentClosureHash };
      const verificationRequestHash = canonicalHash({ kind: "proposal_verification", integrationAttemptId: attempt.integrationAttemptId, closure: verificationClosure });
      const compositionFact = attempt.compositionFactHash ? context.facts[attempt.compositionFactHash] as any : null;
      const privateRefFacts = attempt.privateRefFactHashes.map((hash) => context.facts[hash] as any);
      const validationFacts = [...payload.prefixEvidenceHashes, ...payload.finalEvidenceHashes].map((hash) => context.facts[hash] as any);
      const reconciliationFacts = [...payload.prefixEffectReconciliationHashes, ...payload.finalEffectReconciliationHashes].map((hash) => context.facts[hash] as any);
      const proposalPrerequisiteTimes = [bindingFact?.observedAt, compositionFact?.observedAt, ...privateRefFacts.map((candidate) => candidate?.observedAt), ...validationFacts.map((candidate) => candidate?.completedAt), ...reconciliationFacts.map((candidate) => candidate?.closedAt)];
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "proposal_verification", train.repositoryId, attempt.integrationAttemptId) || !gitFactTimeWithinInput(fact, state, input, ...proposalPrerequisiteTimes) || fact.effectId !== null || fact.requestHash !== verificationRequestHash || fact.ownerEpoch !== state.owner.ownerEpoch || fact.reconciliation !== "applied_exact" || fact.commit !== attempt.composedTree.commit || fact.tree !== attempt.composedTree.tree || fact.detailsHash !== canonicalHash(verificationClosure) || payload.prefixEvidenceHashes.length === 0 || payload.finalEvidenceHashes.length === 0 || payload.prefixEffectReconciliationHashes.length !== payload.prefixEvidenceHashes.length || payload.finalEffectReconciliationHashes.length !== payload.finalEvidenceHashes.length) return precondition("proposal verification fact does not bind exact current-owner non-future composed prefix/final execution and effect closure evidence");
      for (const [phase, evidenceHashes, reconciliationHashes] of [["prefix", payload.prefixEvidenceHashes, payload.prefixEffectReconciliationHashes], ["final", payload.finalEvidenceHashes, payload.finalEffectReconciliationHashes]] as const) {
        for (let index = 0; index < evidenceHashes.length; index += 1) {
          const evidenceHash = evidenceHashes[index]; const reconciliationHash = reconciliationHashes[index];
          const effect = Object.values(state.effects).find((candidate: any) => candidate.kind === "verify_prefix" && candidate.boundIntegrationAttemptId === attempt.integrationAttemptId && candidate.executionRequest?.phase === phase && candidate.executionObservationHash === evidenceHash);
          const reconciliationFact = context.facts[reconciliationHash] as any;
          if (!effect || effect.state !== "reconciled" || effect.reconciliation !== "applied_exact" || effect.observationHash !== reconciliationHash || reconciliationFact?.kind !== "effect_reconciliation" || reconciliationFact.hash !== reconciliationHash || reconciliationFact.hash !== canonicalHash(Object.fromEntries(Object.entries(reconciliationFact).filter(([key]) => key !== "hash"))) || reconciliationFact.effectId !== effect.effectId || reconciliationFact.requestHash !== effect.requestHash || reconciliationFact.executionObservationHash !== evidenceHash || reconciliationFact.resultIdentityHash !== evidenceHash || reconciliationFact.reconciliation !== "applied_exact") return precondition("proposal verification requires exact terminal effect reconciliation for every prefix/final execution observation");
        }
      }
      for (const hash of [...payload.prefixEvidenceHashes, ...payload.finalEvidenceHashes]) {
        const evidence = context.facts[hash] as any;
        const profile = context.integrationValidationProfiles?.[evidence?.profileHash] as any;
        const exactProfile = profile && canonicalHash(profile) === evidence.profileHash && profile.profileId === evidence.profileId
          && evidence.executableArtifactHash === profile.executableArtifactHash && evidence.argvHash === canonicalHash(profile.argv)
          && evidence.cwdMode === profile.cwdMode && evidence.environmentProfileId === profile.environmentProfileId
          && evidence.environmentProfileHash === profile.environmentProfileHash && evidence.environmentHash === profile.environmentHash
          && evidence.timeoutMs === profile.timeoutMs && evidence.readOnly === true && evidence.noEdit === true;
        if (evidence?.kind !== "verification" || evidence.hash !== hash || evidence.hash !== canonicalHash(Object.fromEntries(Object.entries(evidence).filter(([key]) => key !== "hash"))) || evidence.planHash !== state.identity.planHash || evidence.runId !== state.runId || evidence.runNonce !== state.runNonce || evidence.authorizationSetHash !== state.identity.authorizationSet.hash || evidence.integrationAttemptId !== attempt.integrationAttemptId || canonicalHash(evidence.tree) !== canonicalHash(attempt.composedTree) || !exactProfile || evidence.exitCode !== 0 || evidence.signal !== null || evidence.parser !== "strict-json-disposition-v1" || evidence.parserDisposition !== "PASS" || evidence.parsedResultHash === null || evidence.disposition !== "PASS" || utcTimestampOrderValue(evidence.completedAt) < utcTimestampOrderValue(evidence.startedAt)) return precondition("proposal verification evidence is absent, mapping-inexact, execution-failed, parser-inexact, stale, or non-PASS");
      }
      for (const hash of [...payload.prefixEvidenceHashes, ...payload.finalEvidenceHashes]) { const evidence = context.facts[hash] as any; state.evidenceIndex.verifications[hash] = { kind: "verification", schemaVersion: 1, id: evidence.id ?? `verification-${hash.slice(7, 19)}`, hash, bytes: Buffer.byteLength(canonicalStringify(evidence)), mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }; }
      for (const hash of [...payload.prefixEffectReconciliationHashes, ...payload.finalEffectReconciliationHashes]) { const reconciliation = context.facts[hash] as any; state.evidenceIndex.effectReconciliations[reconciliation.effectId] = { kind: "effect_reconciliation", schemaVersion: 1, id: reconciliation.effectId, hash, bytes: Buffer.byteLength(canonicalStringify(reconciliation)), mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }; }
      attempt.prefixEvidenceHashes = [...payload.prefixEvidenceHashes].sort(); attempt.finalEvidenceHashes = [...payload.finalEvidenceHashes].sort(); attempt.prefixEffectReconciliationHashes = [...payload.prefixEffectReconciliationHashes].sort(); attempt.finalEffectReconciliationHashes = [...payload.finalEffectReconciliationHashes].sort(); attempt.environmentClosureHash = payload.environmentClosureHash; attempt.proposalVerificationFactHash = fact.hash; entry.state = "landing";
      notices.push(notice(state, "integration_verified", attempt.integrationAttemptId, fact.hash)); return null;
    }
    case "prepare_git_landing": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks new integration landing effects");
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
      const proposalFact = attempt.proposalVerificationFactHash ? context.facts[attempt.proposalVerificationFactHash] as any : null;
      if (!sameGitRepositoryBinding(fact, bindingFact) || !exactGitFact(fact, state, "landing", train.repositoryId, attempt.integrationAttemptId) || !gitFactTimeWithinInput(fact, state, input, bindingFact?.observedAt, proposalFact?.observedAt, effect.createdAt, effect.lastDispatchAt) || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.ownerEpoch !== effect.boundOwnerEpoch || fact.reconciliation !== payload.reconciliation) return precondition("landing observation fact does not bind exact non-future post-proposal dispatched effect and reconciliation");
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
          if (entry && item) { entry.state = "blocked"; train.activeIntegrationAttemptId = null; item.current = "integration_ready"; if (!item.blockerIds.includes(blockerId)) item.blockerIds.push(blockerId); state.blockers[blockerId] = { blockerId, kind: "integration_drift", subject: { kind: "work_item", id: item.workItemId }, stage: "F8", sourceId: attempt.integrationAttemptId, sourceHash: fact.hash, release: "successor_plan", active: true, createdAt: input.occurredAt, releasedAt: null, releaseReceipt: null }; const lockError = releaseIntegrationLock(state, train.repositoryId, attempt.integrationAttemptId, input.occurredAt, "exact third-target conflict"); if (lockError) return precondition(lockError); const reservationError = releaseWorkItemReservations(state, item.workItemId, "released", input.occurredAt, "exact third-target conflict"); if (reservationError) return precondition(reservationError); }
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
      const landingFact = context.facts[attempt.landingObservationFactHash] as any;
      const transactionSealedAt = utcTimestampOrderValue(receipt?.sealedAt);
      const integrationSealedAt = utcTimestampOrderValue(fact?.sealedAt);
      const receiptTimesExact = Number.isFinite(transactionSealedAt) && Number.isFinite(integrationSealedAt) && transactionSealedAt >= utcTimestampOrderValue(state.updatedAt) && transactionSealedAt >= utcTimestampOrderValue(landingFact?.observedAt) && integrationSealedAt >= transactionSealedAt && integrationSealedAt <= utcTimestampOrderValue(input.occurredAt);
      const receiptExact = receiptHashExact && receiptTimesExact && receipt?.transactionId === attempt.integrationAttemptId && receipt?.runId === state.runId && receipt?.runNonce === state.runNonce && receipt?.planHash === state.identity.planHash && receipt?.authorizationSetHash === state.identity.authorizationSet.hash && receipt?.ownerEpoch === state.owner.ownerEpoch && receipt?.repositoryId === train.repositoryId && receipt?.commonDirIdentityHash === bindingFact?.commonDirIdentityHash && receipt?.worktreeIdentityHash === bindingFact?.worktreeIdentityHash && receipt?.configHash === bindingFact?.gitConfigHash && canonicalHash(receipt?.gitVersion) === bindingFact?.gitVersionHash && receipt?.objectFormat === bindingFact?.objectFormat && receipt?.targetRef === state.repositories[train.repositoryId].targetRef && receipt?.workItemId === item.workItemId && receipt?.candidateGeneration === item.candidateGeneration && canonicalHash(receipt?.sourceBase) === canonicalHash(attempt.sourceBase) && canonicalHash(receipt?.candidate) === canonicalHash(attempt.sourceCandidate) && canonicalHash(receipt?.expectedPrefix) === canonicalHash(attempt.expectedPrefix) && canonicalHash(receipt?.composed) === canonicalHash(attempt.intendedLandedTree) && receipt?.compositionProfileHash === attempt.compositionProfileHash && receipt?.prefixValidationProfileHash === attempt.prefixValidationProfileHash && receipt?.finalValidationProfileHash === attempt.finalValidationProfileHash && receipt?.environmentClosureHash === attempt.environmentClosureHash && canonicalHash([...(receipt?.prefixEvidenceHashes ?? [])].sort()) === canonicalHash(attempt.prefixEvidenceHashes) && canonicalHash([...(receipt?.finalEvidenceHashes ?? [])].sort()) === canonicalHash(attempt.finalEvidenceHashes) && canonicalHash([...(receipt?.prefixEffectReconciliationHashes ?? [])].sort()) === canonicalHash(attempt.prefixEffectReconciliationHashes) && canonicalHash([...(receipt?.finalEffectReconciliationHashes ?? [])].sort()) === canonicalHash(attempt.finalEffectReconciliationHashes) && receipt?.landing?.expectedOldOid === attempt.expectedTarget.commit && receipt?.landing?.newOid === attempt.intendedLandedTree.commit && receipt?.landing?.reconciliation === "applied_exact" && receipt?.landing?.targetObservationHash === (context.facts[attempt.landingObservationFactHash] as any)?.detailsHash && canonicalHash(Object.keys(receipt?.privateRefs ?? {}).sort()) === canonicalHash(["baseline", "candidate", "composed", "prefix", "proposal"]) && canonicalHash(Object.values(receipt?.privateRefs ?? {}).sort()) === canonicalHash(attempt.privateRefFactHashes.map((hash) => (context.facts[hash] as any)?.targetRef).sort());
      if (!transactionBindingExact || !receiptExact) return precondition("transaction receipt fact does not resolve the exact immutable real-Git transaction receipt/content authority");
      if (fact?.kind !== "integration" || fact.hash !== payload.integrationReceiptHash || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.workItemId !== item.workItemId || fact.repositoryId !== train.repositoryId || fact.integrationAttemptId !== attempt.integrationAttemptId || fact.candidateHash !== attempt.sourceCandidateHash || fact.strategy !== attempt.strategy || fact.compositionProfileHash !== attempt.compositionProfileHash || canonicalHash(fact.expectedPrefix) !== canonicalHash(attempt.expectedPrefix) || canonicalHash(fact.expectedTarget) !== canonicalHash(attempt.expectedTarget) || canonicalHash(fact.landed) !== canonicalHash(attempt.intendedLandedTree) || fact.syntheticParentCommit !== attempt.expectedPrefix.commit || fact.sourceToIntegratedLineageHash !== attempt.sourceToIntegratedLineageHash || fact.environmentClosureHash !== attempt.environmentClosureHash || canonicalHash([...(fact.prefixEffectReconciliationHashes ?? [])].sort()) !== canonicalHash(attempt.prefixEffectReconciliationHashes) || canonicalHash([...(fact.finalEffectReconciliationHashes ?? [])].sort()) !== canonicalHash(attempt.finalEffectReconciliationHashes) || !fact.combinedStateVerified || !fact.reconciled || fact.acceptingOwnerEpoch !== state.owner.ownerEpoch || fact.commonDirIdentityHash !== bindingFact?.commonDirIdentityHash || fact.worktreeIdentityHash !== bindingFact?.worktreeIdentityHash || fact.gitConfigHash !== bindingFact?.gitConfigHash || fact.gitVersionHash !== bindingFact?.gitVersionHash || fact.objectFormat !== bindingFact?.objectFormat || fact.transactionReceiptHash !== payload.transactionReceiptHash || fact.transactionReceiptFactHash !== payload.transactionReceiptFactHash || fact.landingObservationHash !== attempt.landingObservationFactHash) return precondition("integration receipt does not duplicate the exact current temporally sealed source/composition/verification/landing transaction");
      attempt.integrationReceipt = fact.hash; attempt.landingState = "landed"; entry.integrationReceipt = fact.hash; entry.state = "integrated"; entry.currentAttemptId = null;
      item.integrationReceipt = fact.hash; item.current = "complete"; item.completedAt = input.occurredAt; item.currentStage = "F8";
      state.evidenceIndex.integrationReceipts[attempt.integrationAttemptId] = { kind: "integration", schemaVersion: 1, id: attempt.integrationAttemptId, hash: fact.hash, bytes: Buffer.byteLength(canonicalStringify(fact)), mediaType: "application/json", sensitivity: "internal", retention: "project", locator: null };
      train.acceptedPrefix = structuredClone(fact.landed); train.expectedTarget = structuredClone(fact.landed); train.acceptedPrefixOrdinal = entry.ordinal + 1; train.acceptedPrefixReceipt = fact.hash; train.activeIntegrationAttemptId = null;
      const repository = state.repositories[train.repositoryId]; repository.observedTarget = structuredClone(fact.landed); repository.observedTargetAt = input.occurredAt; repository.observationReceipt = attempt.landingObservationFactHash; state.freshness.repositoryObservationHashes[train.repositoryId] = attempt.landingObservationFactHash;
      const lockReleaseError = releaseIntegrationLock(state, train.repositoryId, attempt.integrationAttemptId, input.occurredAt, "integration receipt accepted");
      if (lockReleaseError) return precondition(lockReleaseError);
      const lane = state.scheduler.activeNodeLanes[item.workItemId]; if (lane?.releaseDisposition === null) { lane.releaseDisposition = "integrated"; lane.releasedAt = input.occurredAt; }
      const releaseError = releaseWorkItemReservations(state, item.workItemId, "released", input.occurredAt, "integration receipt accepted");
      if (releaseError) return precondition(releaseError);
      for (const edge of Object.values(state.precedence).filter(({ predecessorWorkItemId }) => predecessorWorkItemId === item.workItemId)) { edge.state = "satisfied"; edge.satisfyingReceipt = fact.hash; }
      state.completion.completeWorkItemIds = [...new Set([...state.completion.completeWorkItemIds, item.workItemId])].sort(); state.completion.remainingAuthorizedWorkItemIds = state.completion.remainingAuthorizedWorkItemIds.filter((id) => id !== item.workItemId);
      if (train.entryOrder.every((entryId) => train.entries[entryId]?.state === "integrated")) state.completion.completedRepositoryIds = [...new Set([...state.completion.completedRepositoryIds, train.repositoryId])].sort();
      if (!state.completion.remainingAuthorizedWorkItemIds.length) { state.completion.state = state.completion.unauthorizedWorkItemIds.length ? "authorized_scope_complete" : "plan_complete"; state.completion.completedAt = input.occurredAt; }
      notices.push(notice(state, "integration_accepted", item.workItemId, fact.hash)); return null;
    }
    case "put_effect_intent": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks new effect intents");
      const effect = payload.effect;
      if (state.effects[effect.effectId]) return precondition("effect ID collides with an existing immutable effect slot");
      if (state.desired.run !== "running" && !["reconcile_external_effect", "cleanup_worktree"].includes(effect.kind)) return precondition("non-running run may create only reconciliation or terminal worktree-cleanup intents");
      if (effect.subject.kind === "work_item" && ["cancel", "supersede"].includes(state.workItems[effect.subject.id]?.desired) && !["reconcile_external_effect", "cleanup_worktree"].includes(effect.kind)) return precondition("fenced work item cannot receive new execution effects");
      if (effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.reconciliationRevision != null) return precondition("new effect must be an undispatched intent at the next revision with no reconciliation attribution");
      if (effect.kind === "cleanup_worktree") {
        const stageAttemptId = effect.boundStageAttemptId ?? null; const workerResultHash = effect.boundWorkerResultHash ?? null;
        const attempt = stageAttemptId ? state.stageAttempts[stageAttemptId] : undefined; const binding = stageAttemptId ? state.workerBindings[stageAttemptId] : undefined;
        const result = workerResultHash ? context.facts[workerResultHash] as any : undefined; const repositoryId = attempt ? state.workItems[attempt.workItemId]?.writeRepositoryId : undefined;
        const requestHash = attempt && binding && result ? canonicalHash({ kind: "cleanup_worktree", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, launchIntentId: binding.launchIntentId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, workerResultHash: result.hash, repositoryId, commonDirIdentityHash: result.outputCommonDirIdentityHash, worktreeIdentityHash: result.outputWorktreeIdentityHash }) : null;
        if (!attempt?.terminalAt || !binding || binding.resultHash !== workerResultHash || result?.kind !== "worker_result" || result.outputRepositoryId !== repositoryId || result.outputCommonDirIdentityHash === null || result.outputWorktreeIdentityHash === null || effect.subject.kind !== "work_item" || effect.subject.id !== attempt.workItemId || effect.procedureClass !== "idempotent" || effect.effectScopeId !== null || effect.requestHash !== requestHash) return precondition("cleanup_worktree requires the exact terminal worker result and repository/worktree request identity");
      } else {
        if (effect.boundWorkerResultHash != null && effect.kind !== "run_procedure") return precondition("only cleanup_worktree or exact lifecycle procedure execution may bind a worker-result identity");
        if (effect.boundStageAttemptId != null) {
          const attempt = state.stageAttempts[effect.boundStageAttemptId];
          if (!attempt || effect.subject.kind !== "work_item" || effect.subject.id !== attempt.workItemId || effect.boundAuthorizationSetHash !== attempt.authorizationSetHash) return precondition("stage-bound effect must bind exact attempt/work-item/authorization identity");
          if (!isPostTerminalClosureEffect(effect.kind) && !isExactOpenStageEffectBoundary(state, attempt)) return precondition("stage-bound execution effect intent requires the exact current active unsealed attempt before evidence seal");
          if (effect.kind === "run_procedure") {
            const procedures = Object.values(context.catalog.procedures).filter((procedure) => procedure.purpose === "lifecycle" && procedure.stages.includes(attempt.stage) && procedure.producerKinds.includes(attempt.producerKind));
            let expectedRequest: Record<string, unknown> | null = null;
            try { if (procedures.length === 1) expectedRequest = lifecycleProcedureEffectRequestV1(state, context, attempt, procedures[0]); } catch {}
            if (!expectedRequest || !effect.executionRequest || canonicalHash(effect.executionRequest) !== canonicalHash(expectedRequest) || effect.requestHash !== canonicalHash(expectedRequest) || effect.boundWorkerResultHash !== (attempt.workerResult?.hash ?? null) || effect.executionObservationHash !== null) return precondition("run_procedure intent must bind the exact attempt/input/catalog/executable/argv/environment/candidate/worker/authority request");
          }
        }
        if (effect.kind === "verify_prefix") {
          let expectedRequest: Record<string, unknown> | null = null;
          try { if (effect.boundIntegrationAttemptId) expectedRequest = integrationValidationEffectRequestV1(state, context, effect.boundIntegrationAttemptId, effect.executionRequest?.phase); } catch {}
          if (!expectedRequest || !effect.executionRequest || canonicalHash(effect.executionRequest) !== canonicalHash(expectedRequest) || effect.requestHash !== canonicalHash(expectedRequest) || effect.executionObservationHash !== null) return precondition("verify_prefix intent must bind the exact integration/profile/executable/argv/environment/tree/repository/authority request");
        }
      }
      state.effects[effect.effectId] = structuredClone(effect);
      notices.push(notice(state, "effect_intended", effect.effectId, effect.requestHash));
      return null;
    }
    case "mark_effect_dispatching": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks new effect dispatch");
      const effect = state.effects[payload.effectId];
      const stageAttempt = effect?.boundStageAttemptId ? state.stageAttempts[effect.boundStageAttemptId] : undefined;
      if (effect && stageAttempt && !isPostTerminalClosureEffect(effect.kind) && !isExactOpenStageEffectBoundary(state, stageAttempt)) return precondition("stage-bound execution effect cannot dispatch after its exact attempt evidence seal");
      const exactPersistedExecution = effect && ((effect.kind === "run_procedure" && effect.boundStageAttemptId != null) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null));
      if (!effect || effect.state !== "intended" || effect.dispatchCount !== payload.expectedDispatchCount || (!exactPersistedExecution && effect.boundOwnerEpoch !== state.owner.ownerEpoch) || effect.executionObservationHash != null) return precondition("effect is not dispatchable at the expected count and current owner authority");
      const launch = Object.values(state.launchIntents).find((candidate) => candidate.effectId === effect.effectId);
      const launchAttempt = launch ? state.stageAttempts[launch.stageAttemptId] : undefined;
      if (launch?.dispatchProtocolVersion === 1) {
        const attempt = launchAttempt;
        const dispatch = payload.ownedWorkerDispatch;
        if (!dispatch || attempt?.state !== "dispatchable" || launch.state !== "dispatchable" || effect.dispatchCount !== 0 || launch.readyPacketHash || Object.prototype.hasOwnProperty.call(launch, "normalizedDirective") || launch.directiveHash || launch.promptHash || launch.dispatchConfigRequestHash) return precondition("modern owned-worker launch requires one pristine exact agent dispatch envelope");
        launch.readyPacketHash = dispatch.readyPacketHash; launch.normalizedDirective = dispatch.normalizedDirective; launch.directiveHash = dispatch.directiveHash; launch.promptHash = dispatch.promptHash; launch.dispatchConfigRequestHash = dispatch.dispatchConfigRequestHash;
        attempt.state = "launching"; attempt.updatedAt = input.occurredAt;
      } else if (payload.ownedWorkerDispatch) return precondition("legacy/non-worker effects cannot acquire a modern owned-worker dispatch envelope");
      else if (launch && launchAttempt?.producerKind === "owned_worker" && ["preparing", "launching"].includes(launchAttempt.state) && launch.state === "reserved") { launchAttempt.state = "launching"; launchAttempt.updatedAt = input.occurredAt; }
      if (state.desired.run !== "running" && !["cancel_worker", "cleanup_worktree", "reconcile_external_effect"].includes(effect.kind)) return precondition("paused/cancelling/terminal run blocks new non-recovery dispatch");
      if (effect.subject.kind === "work_item" && ["cancel", "supersede"].includes(state.workItems[effect.subject.id]?.desired) && !["cancel_worker", "cleanup_worktree", "reconcile_external_effect"].includes(effect.kind)) return precondition("fenced work item blocks new non-recovery dispatch");
      effect.state = "dispatching"; effect.dispatchCount += 1; effect.lastDispatchAt = input.occurredAt;
      if (launch) { launch.state = "dispatching"; launch.dispatchCount = effect.dispatchCount; launch.lastDispatchAt = input.occurredAt; }
      effects.push({ effectId: effect.effectId, kind: effect.kind, requestHash: effect.requestHash });
      notices.push(notice(state, "effect_dispatching", effect.effectId, effect.requestHash));
      return null;
    }
    case "retry_effect_dispatch": {
      if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan" || state.current.run === "needs_replan") return precondition("needs_replan blocks effect redispatch");
      const effect = state.effects[payload.effectId];
      const stageAttempt = effect?.boundStageAttemptId ? state.stageAttempts[effect.boundStageAttemptId] : undefined;
      if (effect && stageAttempt && !isPostTerminalClosureEffect(effect.kind) && !isExactOpenStageEffectBoundary(state, stageAttempt)) return precondition("stage-bound execution effect cannot redispatch after its exact attempt evidence seal");
      const replayableProvenAbsentLanding = effect?.kind === "land_target" && effect.state === "reconciled" && effect.reconciliation === "proven_absent";
      const exactExecution = effect && ((effect.kind === "run_procedure" && effect.boundStageAttemptId != null) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null));
      if (exactExecution) return precondition("exact procedure/validation execution recovers the same dispatched operation and cannot acquire duplicate redispatch authority");
      if (!effect || (!replayableProvenAbsentLanding && !["dispatching", "ambiguous"].includes(effect.state)) || effect.dispatchCount !== payload.expectedDispatchCount || effect.boundOwnerEpoch !== state.owner.ownerEpoch) return precondition("uncertain/proven-absent landing effect is not retryable at the expected dispatch count and current owner authority");
      if (!["pure", "idempotent"].includes(effect.procedureClass)) return precondition("compensatable/non-repeatable/unknown effect requires exact reconciliation rather than redispatch");
      if (state.desired.run === "paused" && !["cancel_worker", "reconcile_external_effect"].includes(effect.kind)) return precondition("pause blocks uncertain effect redispatch");
      effect.state = "dispatching"; effect.dispatchCount += 1; effect.lastDispatchAt = input.occurredAt;
      const launch = Object.values(state.launchIntents).find((candidate) => candidate.effectId === effect.effectId);
      if (launch) { launch.state = "dispatching"; launch.dispatchCount = effect.dispatchCount; launch.lastDispatchAt = input.occurredAt; }
      effects.push({ effectId: effect.effectId, kind: effect.kind, requestHash: effect.requestHash });
      notices.push(notice(state, "effect_redispatching", effect.effectId, effect.requestHash));
      return null;
    }
    case "record_effect_execution": {
      const effect = state.effects[payload.effectId] as any;
      if (!effect || effect.state !== "dispatching" || effect.dispatchCount <= 0 || effect.executionObservationHash != null) return precondition("effect execution observation requires one exact prior durable dispatch and an empty immutable result slot");
      if (!((effect.kind === "run_procedure" && effect.boundStageAttemptId != null) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null))) return precondition("only exact lifecycle or integration validation execution effects admit a separate execution observation");
      effect.executionObservationHash = payload.executionObservationHash; effect.state = "observed";
      notices.push(notice(state, "effect_execution_observed", effect.effectId, payload.executionObservationHash));
      return null;
    }
    case "record_effect_observation": {
      const effect = state.effects[payload.effectId] as any;
      const exactExecution = effect && ((effect.kind === "run_procedure" && effect.boundStageAttemptId != null) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null));
      if (!effect || !["dispatching", "observed", "failed", "ambiguous"].includes(effect.state) || (exactExecution && effect.state !== "observed")) return precondition("effect has no matching dispatched execution observation to reconcile");
      const reconciled = ["applied_exact", "compensated", "proven_absent"].includes(payload.reconciliation);
      if ((payload.terminalState === "reconciled") !== reconciled || (["ambiguous", "failed"].includes(payload.terminalState) && !["conflict", "unknown"].includes(payload.reconciliation))) return precondition("effect terminal state must match exact reconciliation disposition");
      if (effect.kind === "cleanup_worktree" && (payload.terminalState !== "reconciled" || !["applied_exact", "proven_absent"].includes(payload.reconciliation))) return precondition("cleanup_worktree observation must be applied exactly or prove the exact worktree absent");
      effect.observationHash = payload.observationHash; effect.reconciliation = payload.reconciliation; effect.state = payload.terminalState; effect.reconciliationRevision = state.revision + 1;
      notices.push(notice(state, "effect_observed", effect.effectId, payload.observationHash));
      return null;
    }
    case "request_cancellation": {
      if (state.completion.state !== "open" || ["completed", "cancelled", "superseded"].includes(state.current.run) || state.desired.run === "cancelled") return precondition("terminal or already-cancelling runs cannot request cancellation");
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
        if (effect.kind !== "cancel_worker" || effect.subject.kind !== "work_item" || !targetIds.includes(effect.subject.id) || effect.state !== "intended" || effect.dispatchCount !== 0 || effect.lastDispatchAt !== null || effect.observationHash !== null || effect.reconciliation !== "not_started" || effect.reconciliationRevision != null || effect.createdRevision !== state.revision + 1 || effect.createdAt !== input.occurredAt) return precondition("cancellation effects must be pristine exact persisted cancel-worker intents");
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
      const targetIdsForEffects = Object.keys(cancellation.fencedGenerations);
      const observedEffectIds = payload.effectObservations.map(({ effectId }: any) => effectId).sort();
      if (new Set(observedEffectIds).size !== observedEffectIds.length) return precondition("cancellation effect observations must be unique");
      const unresolvedPriorEffectIds = Object.values(state.effects).filter((effect) => cancellationAffectsEffect(state, cancellation.scope, targetIdsForEffects, effect) && !cancellation.effectIds.includes(effect.effectId) && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation)).map(({ effectId }) => effectId);
      const expectedObservedEffectIds = [...new Set([...cancellation.effectIds, ...unresolvedPriorEffectIds])].sort();
      if (JSON.stringify(observedEffectIds) !== JSON.stringify(expectedObservedEffectIds)) return precondition("cancellation observation must cover every exact cancellation and unresolved work-item/train/repository effect");
      for (const workerResult of payload.workerResults) {
        const binding = state.workerBindings[workerResult.stageAttemptId];
        const fact = context.facts[workerResult.result.hash];
        const attempt = state.stageAttempts[workerResult.stageAttemptId];
        if (binding && attempt && fact?.kind === "worker_result") {
          if (fact.terminalStatus === "cancelled") { state.evidenceIndex.workerResults[fact.hash] = workerResult.result; attempt.workerResult = workerResult.result; binding.resultHash = fact.hash; binding.completionId = fact.completionId; }
          else {
            const quarantineId = `cancelled-${workerResult.stageAttemptId}-${fact.hash.slice("sha256:".length, "sha256:".length + 12)}`;
            if (state.quarantine[quarantineId]) return precondition("late terminal worker result collides with existing quarantine identity");
            state.quarantine[quarantineId] = { quarantineId, fact: workerResult.result, reason: "cancelled_generation", observedBindingHash: canonicalHash({ workerStorageId: fact.workerStorageId, launchOwnerSessionId: fact.launchOwnerSessionId, workerId: fact.workerId, attemptNumber: fact.attemptNumber, attemptNonce: fact.attemptNonce, configHash: fact.configHash, completionId: fact.completionId }), expectedBindingHash: canonicalHash({ workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, fencedGeneration: state.cancellations[payload.cancellationId].fencedGenerations[attempt.workItemId] }), state: "held", observedAt: input.occurredAt, adoptionReceipt: null, rejectionReason: null };
          }
        }
      }
      const targetIds = cancellation.scope === "run" ? Object.keys(cancellation.fencedGenerations) : [cancellation.subjectId];
      for (const observation of payload.effectObservations) {
        const effect = state.effects[observation.effectId];
        const fact = context.facts[observation.observationHash];
        const isCancelEffect = cancellation.effectIds.includes(observation.effectId);
        const launch = effect ? Object.values(state.launchIntents).find((candidate) => candidate.effectId === effect.effectId) : undefined;
        const unboundLaunch = launch ? state.stageAttempts[launch.stageAttemptId] : undefined;
        if (!effect || !["intended", "dispatching", "observed", "failed", "ambiguous", "reconciled"].includes(effect.state) || !["applied_exact", "compensated", "proven_absent"].includes(fact?.reconciliation ?? "") || (isCancelEffect && !["applied_exact", "proven_absent"].includes(fact.reconciliation)) || (effect.state === "intended" && fact?.reconciliation !== "proven_absent") || (effect.state === "reconciled" && (effect.observationHash !== observation.observationHash || effect.reconciliation !== fact.reconciliation))) return precondition("cancellation observation requires exact terminal effect reconciliation");
        if (effect.kind === "launch_worker" && launch && !state.workerBindings[launch.stageAttemptId]) {
          if (effect.dispatchCount > 0) return precondition("dispatched pre-bind launch must recover and bind the exact opaque operation before cancellation can close");
          if (fact.reconciliation !== "proven_absent" || !unboundLaunch || unboundLaunch.workerResult !== null || unboundLaunch.state !== "cancelling") return precondition("undispatched pre-bind cancellation requires exact proven-absent launch authority and cannot invent worker binding/result authority");
          launch.state = "not_started"; launch.ambiguityReason = null;
        }
        const alreadyReconciled = effect.state === "reconciled" && effect.observationHash === observation.observationHash && effect.reconciliation === fact.reconciliation;
        effect.state = "reconciled"; effect.reconciliation = fact.reconciliation; effect.observationHash = observation.observationHash;
        if (!alreadyReconciled) effect.reconciliationRevision = state.revision + 1;
        if (effect.blockerId && state.blockers[effect.blockerId]?.active) { state.blockers[effect.blockerId].active = false; state.blockers[effect.blockerId].releasedAt = input.occurredAt; state.blockers[effect.blockerId].releaseReceipt = observation.observationHash; }
      }
      for (const workItemId of targetIds) {
        const closeError = closeCancelledWorkItem(state, workItemId, input.occurredAt);
        if (closeError) return precondition(closeError);
      }
      const orphanIntegrationAuthority = Object.values(state.integrationTrains).some((train) => {
        const affectedEntry = Object.values(train.entries).some((entry) => targetIdsForEffects.includes(entry.workItemId) && entry.state !== "integrated");
        return affectedEntry && (train.activeIntegrationAttemptId !== null || train.lockLeaseId !== null || state.repositories[train.repositoryId]?.integrationLockLeaseId !== null);
      });
      const orphanReservationAuthority = Object.values(state.scheduler.reservations).some((reservation) => targetIdsForEffects.includes(reservation.workItemId) && !["released", "fenced"].includes(reservation.state));
      const orphanEffectAuthority = Object.values(state.effects).some((effect) => cancellationAffectsEffect(state, cancellation.scope, targetIdsForEffects, effect) && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation));
      if (orphanIntegrationAuthority || orphanReservationAuthority || orphanEffectAuthority) return precondition("cancellation cannot close with orphan integration, reservation, or effect authority");
      cancellation.state = "closed"; cancellation.resultHash = payload.resultHash;
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

function retryFailureCount(state: DagRunStateV1, entry: DagRunStateV1["retryLedger"][string]): number {
  const stage = state.workItems[entry.workItemId]?.stages[entry.stage];
  const failedAttempts = stage?.state === "blocked" ? stage.attemptIds.length : 0;
  const integrationConflicts = Object.values(state.integrationAttempts).filter((attempt) => attempt.conflictClass !== "none" && Object.values(state.integrationTrains).some((train) => train.entries[attempt.entryId]?.workItemId === entry.workItemId)).length;
  return Math.max(entry.failureSequence.length, failedAttempts, entry.dimension === "integration" ? integrationConflicts : 0);
}

function exactFindingResolutionEvidenceAtIngestion(state: DagRunStateV1, context: DagRunValidationContextV1, finding: any, findingFact: any, resolution: any): boolean {
  const hash = resolution.supersedingEvidenceHash;
  if (typeof hash !== "string" || hash === finding.introducedByEvidenceHash) return false;
  const evidence = context.facts[hash] as any;
  if (!evidence || evidence.hash !== hash || evidence.hash !== canonicalHash(Object.fromEntries(Object.entries(evidence).filter(([key]) => key !== "hash"))) || evidence.planHash !== state.identity.planHash || evidence.runId !== state.runId || evidence.runNonce !== state.runNonce || evidence.workItemId !== finding.workItemId) return false;
  const evidenceAttempt = state.stageAttempts[evidence.stageAttemptId];
  if (!evidenceAttempt || evidenceAttempt.workItemId !== finding.workItemId || evidenceAttempt.stage !== evidence.stage || evidenceAttempt.attemptInput.hash !== evidence.attemptInputHash) return false;
  const observedAt = evidence.kind === "stage_evidence" ? evidence.producedAt : evidence.kind === "finding_correction" ? evidence.observedAt : null;
  if (typeof observedAt !== "string" || utcTimestampOrderValue(observedAt) < utcTimestampOrderValue(findingFact?.observedAt) || utcTimestampOrderValue(observedAt) > utcTimestampOrderValue(resolution.resolvedAt)) return false;
  const candidateHash = evidence.candidateHash ?? null; const generation = evidence.candidateGeneration;
  const candidate = candidateHash ? context.facts[candidateHash] as any : null;
  const validCandidate = generation === 0 ? candidateHash === null : candidate?.kind === "candidate" && candidate.hash === candidateHash && candidate.planHash === state.identity.planHash && candidate.runId === state.runId && candidate.runNonce === state.runNonce && candidate.workItemId === finding.workItemId && candidate.generation === generation && state.evidenceIndex.candidates[candidateHash]?.hash === candidateHash;
  if (!validCandidate) return false;
  if (evidence.kind === "stage_evidence") return evidence.authorizationSetHash === state.identity.authorizationSet.hash && state.evidenceIndex.stageEvidence[hash]?.hash === hash;
  return resolution.disposition === "corrected" && evidence.kind === "finding_correction" && evidence.authorizationSetHash === state.identity.authorizationSet.hash && evidence.findingId === finding.findingId && evidence.findingHash === finding.findingHash;
}

function exactFactRef(ref: any, fact: any, kind: string): boolean {
  return Boolean(ref && fact && ref.kind === kind && fact.kind === kind && ref.hash === fact.hash && ref.bytes === Buffer.byteLength(canonicalStringify(fact)) && fact.hash === canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))));
}

function effectReconciliationTimeIsBounded(effect: any, fact: any, upperBound: string, context: DagRunValidationContextV1): boolean {
  if (typeof fact?.closedAt !== "string" || schemaIssues(TimestampSchema, fact.closedAt).length) return false;
  const closedAt = utcTimestampOrderValue(fact.closedAt);
  const execution = effect?.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
  const executionCompletedAt = execution?.completedAt === undefined ? null : utcTimestampOrderValue(execution.completedAt);
  return Number.isFinite(closedAt)
    && closedAt >= utcTimestampOrderValue(effect.createdAt)
    && (effect.lastDispatchAt === null || closedAt >= utcTimestampOrderValue(effect.lastDispatchAt))
    && (executionCompletedAt === null || Number.isFinite(executionCompletedAt) && closedAt >= executionCompletedAt)
    && closedAt <= utcTimestampOrderValue(upperBound);
}

function validWorkerGitOutputIdentity(fact: any, attempt: any, state: DagRunStateV1, context: DagRunValidationContextV1): boolean {
  const values = [fact?.outputRepositoryId, fact?.outputCommonDirIdentityHash, fact?.outputWorktreeIdentityHash, fact?.outputSourceBase, fact?.outputCommit, fact?.outputTree, fact?.outputObjectFormat, fact?.candidateObservedAt];
  const absent = values.every((value) => value === null);
  const present = values.every((value) => value !== null && value !== undefined);
  if (!absent && !present) return false;
  if (absent) return !(fact?.terminalStatus === "succeeded" && ["F1", "F3"].includes(attempt?.stage));
  const repositoryId = state.workItems[attempt.workItemId]?.writeRepositoryId;
  const oidLength = fact.outputObjectFormat === "sha1" ? 40 : fact.outputObjectFormat === "sha256" ? 64 : 0;
  const structurallyValid = fact.outputRepositoryId === repositoryId && fact.outputSourceBase?.repositoryId === repositoryId
    && typeof fact.outputCommit === "string" && fact.outputCommit.length === oidLength
    && typeof fact.outputTree === "string" && fact.outputTree.length === oidLength
    && typeof fact.outputSourceBase?.commit === "string" && fact.outputSourceBase.commit.length === oidLength
    && typeof fact.outputSourceBase?.tree === "string" && fact.outputSourceBase.tree.length === oidLength
    && Number.isFinite(utcTimestampOrderValue(fact.candidateObservedAt));
  if (!structurallyValid || !["F2", "F5"].includes(attempt?.stage)) return structurallyValid;
  const input = context.facts[attempt.attemptInput.hash] as any;
  const candidate = input?.kind === "stage_attempt_input" && typeof input.candidateHash === "string" ? context.facts[input.candidateHash] as any : null;
  const expectedObjectFormat = candidate?.git?.commit?.length === 40 ? "sha1" : candidate?.git?.commit?.length === 64 ? "sha256" : null;
  return candidate?.kind === "candidate"
    && candidate.hash === input.candidateHash
    && candidate.hash === canonicalHash(Object.fromEntries(Object.entries(candidate).filter(([key]) => key !== "hash")))
    && candidate.planHash === state.identity.planHash
    && candidate.runId === state.runId
    && candidate.runNonce === state.runNonce
    && candidate.generation === attempt.inputGeneration
    && candidate.workItemId === attempt.workItemId
    && candidate.git.repositoryId === repositoryId
    && fact.outputRepositoryId === candidate.git.repositoryId
    && canonicalHash(fact.outputSourceBase) === canonicalHash(candidate.git)
    && fact.outputCommit === candidate.git.commit
    && fact.outputTree === candidate.git.tree
    && fact.outputObjectFormat === expectedObjectFormat;
}

function isPostTerminalClosureEffect(kind: string): boolean {
  return ["cancel_worker", "cleanup_worktree", "reconcile_external_effect"].includes(kind);
}

function isExactOpenStageEffectBoundary(state: DagRunStateV1, attempt: any): boolean {
  const item = state.workItems[attempt.workItemId];
  const stage = item?.stages[attempt.stage];
  return Boolean(stage?.state === "active" && stage.currentAttemptId === attempt.stageAttemptId && attempt.evidence === null && attempt.terminalAt === null && !["sealed", "cancelled", "failed", "lost", "ambiguous", "quarantined"].includes(attempt.state));
}

function fixedProducerForStage(stage: string, operationKind: string): "conductor" | "owned_worker" | "deterministic_runner" | null {
  const expected: Record<string, Record<string, "conductor" | "owned_worker" | "deterministic_runner">> = {
    F0: { conductor: "conductor" }, F1: { implementation: "owned_worker" }, F2: { evaluation: "owned_worker" },
    F3: { codification: "owned_worker" }, F4: { verification: "deterministic_runner" }, F5: { review: "owned_worker" },
    F6: { hardening: "owned_worker" }, F7: { verification: "deterministic_runner" }, F8: { conductor: "conductor" },
  };
  return expected[stage]?.[operationKind] ?? null;
}

function sealStageAttempt(state: DagRunStateV1, input: DagRunInputV1, payload: any, context: DagRunValidationContextV1, notices: DagRunTransitionNoticeV1[], f8: boolean): { code: DagRunRejectCodeV1; message: string } | null {
  const attempt = state.stageAttempts[payload.stageAttemptId]; const item = attempt ? state.workItems[attempt.workItemId] : null;
  const stage = attempt && item ? item.stages[attempt.stage] : null;
  const evidence = context.facts[payload.evidence.hash] as any; const aggregate = context.facts[payload.checkAggregate.hash] as any;
  if (!attempt || !item || !stage || stage.currentAttemptId !== attempt.stageAttemptId || stage.state !== "active" || attempt.evidence || attempt.terminalAt) return precondition("stage seal requires the exact current active unsealed attempt");
  if (f8 !== (attempt.stage === "F8")) return precondition("F8 integration readiness uses only the distinct atomic F8 seal variant");
  if (attempt.inputGeneration > item.candidateGeneration || attempt.authorizationSetHash !== state.identity.authorizationSet.hash) return precondition("stage seal is stale for the current generation or authorization");
  const resultFact = attempt.workerResult ? context.facts[attempt.workerResult.hash] as any : null;
  if (attempt.producerKind === "owned_worker") {
    if (attempt.state !== "result_observed" || !["succeeded", "needs_attention", "failed", "cancelled", "lost"].includes(resultFact?.terminalStatus)) return precondition("owned-worker seal requires one exact recorded terminal result; canonical aggregate derivation determines PASS/non-PASS");
  }
  if (attempt.producerKind !== "owned_worker" && !["running", "evidence_pending"].includes(attempt.state)) return precondition("non-worker stage seal requires the exact active local attempt boundary");
  if (!exactFactRef(payload.checkAggregate, aggregate, "check_aggregate") || aggregate.planHash !== state.identity.planHash || aggregate.runId !== state.runId || aggregate.runNonce !== state.runNonce || aggregate.authorizationSetHash !== state.identity.authorizationSet.hash || aggregate.workItemId !== item.workItemId || aggregate.stage !== attempt.stage || aggregate.stageAttemptId !== attempt.stageAttemptId || aggregate.attemptInputHash !== attempt.attemptInput.hash) return precondition("check aggregate must be an exact canonical immutable attempt binding");
  if (!exactFactRef(payload.evidence, evidence, "stage_evidence") || evidence.planHash !== state.identity.planHash || evidence.runId !== state.runId || evidence.runNonce !== state.runNonce || evidence.authorizationSetHash !== state.identity.authorizationSet.hash || evidence.workItemId !== item.workItemId || evidence.stage !== attempt.stage || evidence.stageAttemptId !== attempt.stageAttemptId || evidence.attemptInputHash !== attempt.attemptInput.hash || evidence.checkAggregateHash !== aggregate.hash || evidence.procedureHash !== aggregate.procedureHash || evidence.environmentProfileHash !== aggregate.environmentProfileHash || evidence.disposition !== aggregate.disposition || evidence.producerKind !== attempt.producerKind || evidence.producerResultHash !== (attempt.workerResult?.hash ?? null) || utcTimestampOrderValue(evidence.producedAt) < utcTimestampOrderValue(attempt.createdAt) || utcTimestampOrderValue(evidence.producedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("stage evidence must duplicate the exact aggregate, attempt, producer, result, and non-future production authority");
  if (!exactLifecycleProcedureCatalogBindingV1(context, evidence, attempt.stage, attempt.producerKind)) return precondition("stage evidence procedure is outside the exact canonical lifecycle catalog producer/environment/executable contract");
  const planItem = context.plan.workItems.find(({ workItemId }) => workItemId === item.workItemId);
  if (!planItem) return precondition("stage evidence work item is absent from the exact plan");
  const applicableChecks = planItem.checks.filter(({ phases }) => phases.includes(attempt.stage));
  const requiredAssertions = attempt.stage === "F2" ? planItem.oracleIds.flatMap((oracleId) => (context.plan.acceptanceOracles.find((oracle) => oracle.oracleId === oracleId)?.assertions ?? []).map((assertion) => ({ oracleId, assertion }))) : [];
  if (canonicalHash([...aggregate.oracleIds].sort()) !== canonicalHash([...planItem.oracleIds].sort()) || canonicalHash(aggregate.assertions.map(({ oracleId, assertionId }: any) => `${oracleId}/${assertionId}`).sort()) !== canonicalHash(requiredAssertions.map(({ oracleId, assertion }) => `${oracleId}/${assertion.assertionId}`).sort()) || canonicalHash(aggregate.checks.map(({ checkId }: any) => checkId).sort()) !== canonicalHash(applicableChecks.map(({ checkId }) => checkId).sort())) return precondition("check aggregate must cover the exact plan item stage checks and oracle assertions");
  const checkExecutionRefs = payload.checkExecutions ?? [];
  const checkAuthorityRefs = payload.checkAuthorities ?? [];
  const effectReconciliationRefs = payload.effectReconciliations ?? [];
  const supportRefs = [...payload.oracleAssertions, ...payload.checkDispositions, ...checkExecutionRefs, ...checkAuthorityRefs, ...effectReconciliationRefs];
  if (new Set(supportRefs.map((reference: any) => reference.hash)).size !== supportRefs.length) return precondition("stage support references must be unique");
  const expectedOracleHashes = aggregate.assertions.map(({ evidenceHash }: any) => evidenceHash).sort();
  const expectedCheckHashes = aggregate.checks.flatMap(({ applicabilityEvidenceHashes }: any) => applicabilityEvidenceHashes).sort();
  const expectedExecutionHashes = aggregate.checks.map(({ executionEvidenceHash }: any) => executionEvidenceHash).filter((hash: any): hash is string => typeof hash === "string").sort();
  const expectedAuthorityHashes = aggregate.checks.flatMap(({ applicabilityEvidenceHashes }: any) => applicabilityEvidenceHashes).flatMap((hash: string) => ((context.facts[hash] as any)?.evidenceHashes ?? [])).sort();
  if (canonicalHash(payload.oracleAssertions.map(({ hash }: any) => hash).sort()) !== canonicalHash(expectedOracleHashes) || canonicalHash(payload.checkDispositions.map(({ hash }: any) => hash).sort()) !== canonicalHash(expectedCheckHashes) || canonicalHash(checkExecutionRefs.map(({ hash }: any) => hash).sort()) !== canonicalHash(expectedExecutionHashes) || canonicalHash(checkAuthorityRefs.map(({ hash }: any) => hash).sort()) !== canonicalHash(expectedAuthorityHashes)) return precondition("aggregate support and authority references must be exact, complete, and contain no extras");
  for (const reference of payload.oracleAssertions) {
    const fact = context.facts[reference.hash] as any; const aggregateAssertion = aggregate.assertions.find(({ evidenceHash }: any) => evidenceHash === reference.hash); const expected = requiredAssertions.find(({ oracleId, assertion }) => oracleId === aggregateAssertion?.oracleId && assertion.assertionId === aggregateAssertion?.assertionId)?.assertion;
    if (!exactFactRef(reference, fact, "oracle_assertion") || !aggregateAssertion || !expected || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.workItemId !== item.workItemId || fact.stage !== "F2" || fact.stageAttemptId !== attempt.stageAttemptId || fact.attemptInputHash !== attempt.attemptInput.hash || fact.oracleId !== aggregateAssertion.oracleId || fact.assertionId !== aggregateAssertion.assertionId || fact.procedureId !== expected.procedureId || fact.environmentProfileId !== expected.environmentProfileId || fact.observationMethod !== expected.observationMethod || fact.requiredEvidenceClass !== expected.requiredEvidenceClass) return precondition("oracle assertion must bind exact plan/run/item/stage/attempt/oracle semantics");
  }
  for (const aggregateCheck of aggregate.checks) {
    const expected = applicableChecks.find(({ checkId }) => checkId === aggregateCheck.checkId); const predicateHash = expected?.condition?.contentHash ?? null;
    if (!expected) return precondition("aggregate check is absent from the exact plan stage");
    const isExecutionDisposition = ["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"].includes(aggregateCheck.disposition);
    if (expected.applicability === "required" && (!isExecutionDisposition || aggregateCheck.applicabilityEvidenceHashes.length !== 0)) return precondition("required check requires only exact execution evidence and cannot carry waiver/applicability support");
    if (isExecutionDisposition) {
      const executionHash = aggregateCheck.executionEvidenceHash;
      const reference = checkExecutionRefs.find((candidate: any) => candidate.hash === executionHash); const execution = executionHash ? context.facts[executionHash] as any : null;
      if (!exactFactRef(reference, execution, "check_execution") || execution.planHash !== state.identity.planHash || execution.runId !== state.runId || execution.runNonce !== state.runNonce || execution.authorizationSetHash !== state.identity.authorizationSet.hash || execution.workItemId !== item.workItemId || execution.stage !== attempt.stage || execution.stageAttemptId !== attempt.stageAttemptId || execution.attemptInputHash !== attempt.attemptInput.hash || execution.candidateGeneration !== (attempt.reservedOutputGeneration ?? attempt.inputGeneration) || execution.candidateHash !== (attempt.stage === "F0" ? null : item.candidate?.candidateHash ?? null) || execution.checkId !== expected.checkId || execution.procedureHash !== evidence.procedureHash || execution.environmentProfileHash !== evidence.environmentProfileHash || execution.environmentObservationHash !== (evidence.environmentObservationHash ?? null) || execution.disposition !== aggregateCheck.disposition || utcTimestampOrderValue(execution.startedAt) < utcTimestampOrderValue(attempt.createdAt) || utcTimestampOrderValue(execution.completedAt) < utcTimestampOrderValue(execution.startedAt) || utcTimestampOrderValue(execution.completedAt) > utcTimestampOrderValue(evidence.producedAt) || utcTimestampOrderValue(execution.completedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("check result must cite exact immutable non-future stage/attempt/input/check/procedure/environment execution evidence");
    } else if ((aggregateCheck.executionEvidenceHash ?? null) !== null || aggregateCheck.applicabilityEvidenceHashes.length === 0) return precondition("waiver/not-applicable check disposition requires exact authority and cannot cite execution");
    for (const hash of aggregateCheck.applicabilityEvidenceHashes) {
      const reference = payload.checkDispositions.find((candidate: any) => candidate.hash === hash); const fact = context.facts[hash] as any;
      if (!exactFactRef(reference, fact, "check_disposition") || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.workItemId !== item.workItemId || fact.stage !== attempt.stage || fact.stageAttemptId !== attempt.stageAttemptId || fact.attemptInputHash !== attempt.attemptInput.hash || fact.checkId !== expected.checkId || fact.disposition !== aggregateCheck.disposition || fact.predicateHash !== predicateHash || fact.evidenceHashes.some((hash: string) => !context.facts[hash])) return precondition("check disposition must bind exact plan/run/item/stage/attempt/check semantics and readable evidence");
      for (const authorityHash of fact.evidenceHashes) {
        const authorityRef = checkAuthorityRefs.find((candidate: any) => candidate.hash === authorityHash); const authority = context.facts[authorityHash] as any;
        const commonAuthority = authority?.hash === authorityHash && authority.hash === canonicalHash(Object.fromEntries(Object.entries(authority).filter(([key]) => key !== "hash"))) && authority.planHash === state.identity.planHash && authority.runId === state.runId && authority.runNonce === state.runNonce && authority.authorizationSetHash === state.identity.authorizationSet.hash && authority.workItemId === item.workItemId && authority.stage === attempt.stage && authority.stageAttemptId === attempt.stageAttemptId && authority.attemptInputHash === attempt.attemptInput.hash && authority.checkId === expected.checkId && authority.predicateHash === predicateHash;
        if (aggregateCheck.disposition === "WAIVED") { if (!exactFactRef(authorityRef, authority, "waiver") || !commonAuthority || authority.issuedBy !== "user") return precondition("WAIVED check requires exact attempt-bound immutable user waiver authority"); }
        else { if (!exactFactRef(authorityRef, authority, "check_applicability") || !commonAuthority || authority.applicable !== (aggregateCheck.disposition !== "NOT_APPLICABLE")) return precondition("conditional/not-applicable check requires exact predicate-bound applicability authority"); }
      }
    }
  }
  const supportDispositions = [...payload.oracleAssertions, ...payload.checkDispositions, ...checkExecutionRefs].map(({ hash }: any) => (context.facts[hash] as any)?.disposition);
  const workerTerminalStatus = attempt.producerKind === "owned_worker" ? resultFact?.terminalStatus ?? null : null;
  const environment = typeof evidence.environmentObservationHash === "string" ? context.facts[evidence.environmentObservationHash] as any : null;
  const environmentDisposition = environment?.kind === "environment_observation" ? environment.cleanliness === "clean" ? "PASS" : "FAIL" : null;
  const derivedDisposition = deriveStageAggregateDispositionV1(workerTerminalStatus, aggregate.checks.map(({ disposition }: any) => disposition), aggregate.assertions.map(({ evidenceHash }: any) => (context.facts[evidenceHash] as any)?.disposition), environmentDisposition);
  if (aggregate.disposition !== derivedDisposition) return precondition("aggregate disposition contradicts the sole canonical precedence BUDGET_EXHAUSTED > BLOCKED > FAIL > PASS over exact worker/check/assertion/environment terminals");
  if (resultFact?.terminalStatus === "cancelled") {
    const binding = state.workerBindings[attempt.stageAttemptId];
    const activeCancellation = Object.values(state.cancellations).some((cancellation) => cancellation.state !== "closed" && Object.prototype.hasOwnProperty.call(cancellation.fencedGenerations, attempt.workItemId));
    if (!binding || binding.resultHash !== resultFact.hash || derivedDisposition !== "BLOCKED") return precondition("cancelled worker seal requires the exact bound terminal result and canonical BLOCKED derivation");
    if (activeCancellation) return precondition("active conductor cancellation must close through record_cancellation rather than stage sealing");
  }
  const expectedFindingHashes = Object.values(state.findingClosures).filter((finding) => finding.workItemId === item.workItemId && finding.stageAttemptId === attempt.stageAttemptId).map(({ findingHash }) => findingHash).sort();
  if (canonicalHash([...evidence.findingHashes].sort()) !== canonicalHash(expectedFindingHashes) || new Set(evidence.findingHashes).size !== evidence.findingHashes.length) return precondition("stage evidence finding hashes must exactly equal applicable current attempt findings for PASS and non-PASS");
  const applicableEffects = Object.values(state.effects).filter((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && effect.boundStageAttemptId === attempt.stageAttemptId && !["launch_worker", "cancel_worker", "materialize_workspace", "cleanup_worktree"].includes(effect.kind));
  const expectedEffectHashes = applicableEffects.map(({ observationHash }) => observationHash).filter((hash): hash is string => typeof hash === "string").sort();
  if (applicableEffects.some((effect) => effect.state !== "reconciled" || !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation) || effect.observationHash === null) || canonicalHash([...evidence.effectReconciliationHashes].sort()) !== canonicalHash(expectedEffectHashes) || canonicalHash(effectReconciliationRefs.map(({ hash }: any) => hash).sort()) !== canonicalHash(expectedEffectHashes)) return precondition("stage evidence effect hashes must exactly equal every applicable terminally reconciled current attempt effect");
  for (const reference of effectReconciliationRefs) {
    const fact = context.facts[reference.hash] as any;
    const effect = applicableEffects.find((candidate) => candidate.observationHash === reference.hash);
    if (!effect || reference.id !== effect.effectId || !exactFactRef(reference, fact, "effect_reconciliation") || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.effectId !== effect.effectId || fact.requestHash !== effect.requestHash || fact.reconciliation !== effect.reconciliation) return precondition("stage effect closure must cite exact immutable effect-ID-keyed reconciliation facts");
    if (effect.kind === "run_procedure") {
      const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
      if (!execution || fact.executionObservationHash !== effect.executionObservationHash || fact.resultIdentityHash !== execution.resultIdentityHash || fact.closedAt !== execution.completedAt || effect.state !== "reconciled" || effect.reconciliation !== "applied_exact") return precondition("stage procedure closure must bind the exact immutable execution observation and result identity");
    }
  }
  const expectedGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
  if (evidence.candidateGeneration !== expectedGeneration || evidence.candidateHash !== (attempt.stage === "F0" ? null : item.candidate?.candidateHash ?? null)) return precondition("stage evidence candidate generation/hash is stale");
  if (aggregate.disposition === "PASS" && attempt.stage === "F1" && (!item.candidate || item.candidate.producedByStageAttemptId !== attempt.stageAttemptId || attempt.reservedOutputGeneration !== item.candidateGeneration)) return precondition("F1 cannot pass without its exact recorded candidate output");
  if (aggregate.disposition === "PASS" && attempt.stage !== "F0" && !item.candidate) return precondition("non-F0 stage cannot pass without a current candidate");
  if (["F2", "F5", "F7"].includes(attempt.stage)) {
    const observationRef = payload.environmentObservation; const observation = observationRef ? context.facts[observationRef.hash] as any : null;
    const materializationRef = payload.workspaceMaterialization; const materialization = materializationRef ? context.facts[materializationRef.hash] as any : null;
    const repository = state.repositories[item.writeRepositoryId]; const candidate = item.candidate ? context.facts[item.candidate.candidateHash] as any : null;
    if (evidence.environmentObservationHash !== observationRef?.hash || !exactFactRef(observationRef, observation, "environment_observation") || observation.planHash !== state.identity.planHash || observation.runId !== state.runId || observation.runNonce !== state.runNonce || observation.workItemId !== item.workItemId || observation.stage !== attempt.stage || observation.stageAttemptId !== attempt.stageAttemptId || observation.attemptInputHash !== attempt.attemptInput.hash || observation.repositoryId !== item.writeRepositoryId || observation.candidateGeneration !== expectedGeneration || observation.candidateHash !== item.candidate?.candidateHash || canonicalHash(observation.candidateTree) !== canonicalHash(candidate?.git) || observation.environmentProfileHash !== evidence.environmentProfileHash || observation.workspaceMaterializationHash !== materializationRef?.hash) return precondition(`${attempt.stage} requires exact immutable candidate-tree/profile/worktree/materialization/attempt environment observation`);
    if (!exactFactRef(materializationRef, materialization, "workspace_materialization") || materialization.planHash !== state.identity.planHash || materialization.runId !== state.runId || materialization.runNonce !== state.runNonce || materialization.workItemId !== item.workItemId || materialization.stageAttemptId !== attempt.stageAttemptId || materialization.repositoryId !== item.writeRepositoryId || materialization.candidateGeneration !== expectedGeneration || materialization.candidateHash !== item.candidate?.candidateHash || canonicalHash(materialization.candidateTree) !== canonicalHash(candidate?.git) || materialization.commonDirIdentityHash !== observation.commonDirIdentityHash || materialization.worktreeIdentityHash !== observation.worktreeIdentityHash || utcTimestampOrderValue(materialization.materializedAt) < utcTimestampOrderValue(attempt.createdAt) || utcTimestampOrderValue(observation.observedAt) < utcTimestampOrderValue(materialization.materializedAt) || utcTimestampOrderValue(observation.observedAt) > utcTimestampOrderValue(evidence.producedAt) || utcTimestampOrderValue(observation.observedAt) > utcTimestampOrderValue(input.occurredAt)) return precondition("environment observation requires a fresh exact immutable non-future workspace materialization");
    if (["F2", "F5"].includes(attempt.stage) && resultFact?.outputRepositoryId !== null && (resultFact?.outputCommonDirIdentityHash !== materialization.commonDirIdentityHash || resultFact?.outputCommonDirIdentityHash !== observation.commonDirIdentityHash || resultFact?.outputWorktreeIdentityHash !== materialization.worktreeIdentityHash || resultFact?.outputWorktreeIdentityHash !== observation.worktreeIdentityHash)) return precondition(`${attempt.stage} worker Git output must preserve the exact materialized common-dir/worktree identities`);
    if (attempt.stage === "F7" && aggregate.disposition === "PASS" && observation.cleanliness !== "clean") return precondition("passing F7 requires fresh clean exact-tree evidence");
    repository.workspace.state = observation.cleanliness === "clean" ? "clean" : observation.cleanliness === "dirty" ? "dirty" : "quarantined";
    repository.workspace.gitCommonDirIdentityHash = observation.commonDirIdentityHash; repository.workspace.gitWorktreeIdentityHash = observation.worktreeIdentityHash;
    repository.workspace.expectedHead = structuredClone(candidate.git); repository.workspace.observationReceipt = materialization.hash;
    (state.evidenceIndex as any).environmentObservations ??= {}; (state.evidenceIndex as any).workspaceMaterializations ??= {};
    (state.evidenceIndex as any).environmentObservations[observation.hash] = structuredClone(observationRef); (state.evidenceIndex as any).workspaceMaterializations[materialization.hash] = structuredClone(materializationRef);
  } else if (evidence.environmentObservationHash != null || payload.environmentObservation || payload.workspaceMaterialization) return precondition("only F2/F5/F7 may attach lifecycle environment observation authority");
  for (const reference of payload.oracleAssertions) state.evidenceIndex.oracleAssertions[reference.hash] = structuredClone(reference);
  for (const reference of payload.checkDispositions) state.evidenceIndex.checkDispositions[reference.hash] = structuredClone(reference);
  (state.evidenceIndex as any).checkExecutions ??= {}; (state.evidenceIndex as any).checkApplicabilities ??= {};
  for (const reference of checkExecutionRefs) (state.evidenceIndex as any).checkExecutions[reference.hash] = structuredClone(reference);
  for (const reference of checkAuthorityRefs) {
    if (reference.kind === "waiver") state.evidenceIndex.waivers[reference.id] = structuredClone(reference);
    else if (reference.kind === "check_applicability") (state.evidenceIndex as any).checkApplicabilities[reference.hash] = structuredClone(reference);
  }
  for (const reference of effectReconciliationRefs) state.evidenceIndex.effectReconciliations[reference.id] = structuredClone(reference);
  state.evidenceIndex.checkAggregates[aggregate.hash] = structuredClone(payload.checkAggregate); state.evidenceIndex.stageEvidence[evidence.hash] = structuredClone(payload.evidence);
  attempt.evidence = structuredClone(payload.evidence); attempt.state = "sealed"; attempt.updatedAt = input.occurredAt; attempt.terminalAt = input.occurredAt;
  stage.currentEvidence = evidence.hash; stage.lastDisposition = aggregate.disposition;
  if (aggregate.disposition === "PASS") stage.state = "passed";
  else { stage.state = aggregate.disposition === "BUDGET_EXHAUSTED" ? "budget_exhausted" : aggregate.disposition === "BLOCKED" ? "blocked" : "failed"; item.current = "blocked"; }
  const reservation = Object.values(state.scheduler.reservations).find((candidate) => candidate.workItemId === item.workItemId && candidate.stage === attempt.stage && candidate.leaseIds.length === attempt.leaseIds.length && candidate.leaseIds.every((leaseId) => attempt.leaseIds.includes(leaseId)) && !["released", "fenced"].includes(candidate.state));
  if (!reservation) return precondition("stage seal cannot resolve the exact active scheduler reservation");
  if (aggregate.disposition === "PASS" && f8) {
    if (PLAN_STAGE_IDS.slice(0, 8).some((stageId) => item.stages[stageId].state !== "passed") || !item.candidate) return precondition("F8 cannot seal without the exact complete F0-F7 chain and current candidate");
    const ready = context.facts[payload.integrationReady.hash] as any;
    if (!exactFactRef(payload.integrationReady, ready, "integration_ready") || ready.planHash !== state.identity.planHash || ready.runId !== state.runId || ready.runNonce !== state.runNonce || ready.workItemId !== item.workItemId || ready.candidateGeneration !== item.candidateGeneration || ready.candidateHash !== item.candidate.candidateHash || ready.f8EvidenceHash !== evidence.hash || !ready.allRequiredChecksPassed || !ready.effectsReconciled || !ready.findingsClosed) return precondition("F8 readiness must be one exact canonical closure fact");
    if (item.openFindingIds.length || Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation))) return precondition("F8 readiness cannot retain findings or unreconciled effects");
    const planTrain = context.plan.constraints.integrationTrains.find((train) => train.repositoryId === item.writeRepositoryId); const train = state.integrationTrains[item.writeRepositoryId]; const member = planTrain?.members.find((candidate) => candidate.workItemId === item.workItemId);
    if (!planTrain || !train || !member) return precondition("F8 enqueue must resolve the exact plan train member");
    const entryId = `entry-${String(member.ordinal).padStart(3, "0")}-${item.workItemId}`; const existingEntry = train.entries[entryId];
    const pristineInitialSlot = !existingEntry && member.ordinal === train.entryOrder.length;
    const exactConflictRetrySlot = existingEntry?.workItemId === item.workItemId && existingEntry.ordinal === member.ordinal && existingEntry.state === "invalidated" && existingEntry.currentAttemptId !== null && state.integrationAttempts[existingEntry.currentAttemptId]?.conflictClass !== "none";
    if ((!pristineInitialSlot && !exactConflictRetrySlot) || item.integrationEntryId || item.integrationReadyReceipt) return precondition("F8 integration-ready/train slot is neither pristine plan order nor an exact invalidated conflict retry");
    state.evidenceIndex.integrationReady[item.workItemId] = structuredClone(payload.integrationReady);
    item.integrationReadyReceipt = ready.hash; item.integrationEntryId = entryId; item.current = "integration_ready"; item.currentStage = "F8";
    if (pristineInitialSlot) {
      train.entries[entryId] = { entryId, workItemId: item.workItemId, ordinal: member.ordinal, state: member.ordinal === train.acceptedPrefixOrdinal ? "eligible" : "waiting", integrationReadyHash: ready.hash, sourceCandidate: structuredClone(item.candidate), attemptIds: [], currentAttemptId: null, integrationReceipt: null, blockerIds: [] };
      train.entryOrder = [...train.entryOrder, entryId];
    } else existingEntry.integrationReadyHash = ready.hash;
  } else if (aggregate.disposition === "PASS") {
    const nextStage = PLAN_STAGE_IDS[PLAN_STAGE_IDS.indexOf(attempt.stage) + 1]; item.current = "active"; item.currentStage = nextStage ?? attempt.stage;
  }
  const releaseError = releaseSchedulerReservationAccounting(state, reservation, "released", input.occurredAt, f8 ? "F8 integration-ready seal" : "stage attempt sealed");
  if (releaseError) return precondition(releaseError);
  notices.push(notice(state, f8 ? "f8_integration_ready" : "stage_attempt_sealed", attempt.stageAttemptId, f8 ? payload.integrationReady.hash : evidence.hash));
  return null;
}

function releaseSchedulerReservationAccounting(state: DagRunStateV1, reservation: any, disposition: "released" | "fenced", at: string, reason: string, terminalCancellation = false): string | null {
  const item = state.workItems[reservation.workItemId];
  if (!item) return "scheduler reservation work item is missing";
  const holderAttemptIds = [...new Set(reservation.leaseIds.map((leaseId: string) => state.leases[leaseId]?.holderStageAttemptId).filter((id: unknown): id is string => typeof id === "string"))];
  const attempt = holderAttemptIds.length === 1 ? state.stageAttempts[holderAttemptIds[0]] : undefined;
  const integrationAttempt = Object.values(state.integrationAttempts).find((candidate) => reservation.leaseIds.some((leaseId: string) => state.leases[leaseId]?.holderIntegrationAttemptId === candidate.integrationAttemptId));
  const attemptTerminal = Boolean(attempt?.terminalAt && ["sealed", "cancelled", "failed", "lost", "quarantined"].includes(attempt.state));
  const integrationConflictTerminal = Boolean(integrationAttempt && ([integrationAttempt.compositionEffectId, integrationAttempt.landingEffectId].filter(Boolean) as string[]).some((effectId) => state.effects[effectId]?.state === "failed" && state.effects[effectId]?.reconciliation === "conflict"));
  const integrationTerminal = Boolean(integrationAttempt && (integrationAttempt.integrationReceipt !== null || integrationAttempt.conflictClass !== "none" || integrationConflictTerminal || terminalCancellation));
  if (!terminalCancellation && !attemptTerminal && !integrationTerminal) return "generic reservation release requires an exact terminal stage/integration attempt";
  const associatedEffectIds = new Set<string>();
  if (attempt?.launchIntentId) { const launch = state.launchIntents[attempt.launchIntentId]; if (launch) associatedEffectIds.add(launch.effectId); }
  if (integrationAttempt) { associatedEffectIds.add(integrationAttempt.compositionEffectId); if (integrationAttempt.landingEffectId) associatedEffectIds.add(integrationAttempt.landingEffectId); }
  for (const effect of Object.values(state.effects)) if (effect.subject.kind === "work_item" && effect.subject.id === reservation.workItemId && effect.boundCandidateGeneration === reservation.candidateGeneration) associatedEffectIds.add(effect.effectId);
  if ([...associatedEffectIds].some((effectId) => {
    const effect = state.effects[effectId];
    const exactIntegrationConflict = Boolean(integrationAttempt && [integrationAttempt.compositionEffectId, integrationAttempt.landingEffectId].includes(effectId) && effect?.state === "failed" && effect.reconciliation === "conflict");
    return effect && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation) && !exactIntegrationConflict;
  })) return "reservation release requires every associated effect to be exactly reconciled, except its own exact terminal integration conflict";
  for (const leaseId of reservation.leaseIds) {
    const lease = state.leases[leaseId];
    if (!lease) return "reservation lease is missing";
    if (["released", "fenced"].includes(lease.state)) continue;
    if (!["active", "release_requested"].includes(lease.state)) return "reservation lease is not releasable";
    const resource = lease.kind === "resource" ? state.resourcePools[lease.subject.id] : null;
    const operational = lease.kind === "operational" ? state.scheduler.operationalCapacities[lease.subject.id] : null;
    if (resource && resource.allocatedUnits < lease.units) return "resource release would underflow exact allocation";
    if (operational && operational.allocatedUnits < lease.units) return "operational release would underflow exact allocation";
  }
  for (const leaseId of reservation.leaseIds) {
    const lease = state.leases[leaseId];
    if (["released", "fenced"].includes(lease.state)) { item.activeLeaseIds = item.activeLeaseIds.filter((id) => id !== leaseId); continue; }
    lease.state = disposition; lease.releasedAt = at; lease.releaseReason = reason; item.activeLeaseIds = item.activeLeaseIds.filter((id) => id !== leaseId);
    if (lease.kind === "resource" && state.resourcePools[lease.subject.id]) { const pool = state.resourcePools[lease.subject.id]; pool.allocatedUnits -= lease.units; pool.leaseIds = pool.leaseIds.filter((id) => id !== leaseId); }
    if (lease.kind === "operational" && state.scheduler.operationalCapacities[lease.subject.id]) { const pool = state.scheduler.operationalCapacities[lease.subject.id]; pool.allocatedUnits -= lease.units; pool.reservationIds = pool.reservationIds.filter((id) => id !== reservation.reservationId); }
    if (lease.kind === "semantic_mutex" && state.mutexes[lease.subject.id]?.activeLeaseId === leaseId) state.mutexes[lease.subject.id].activeLeaseId = null;
  }
  reservation.state = disposition; reservation.releasedAt = at;
  return null;
}

function releaseWorkItemReservations(state: DagRunStateV1, workItemId: string, disposition: "released" | "fenced", at: string, reason: string, terminalCancellation = false): string | null {
  for (const reservation of Object.values(state.scheduler.reservations).filter((candidate) => candidate.workItemId === workItemId && (!(["released", "fenced"].includes(candidate.state)) || candidate.leaseIds.some((leaseId) => !["released", "fenced"].includes(state.leases[leaseId]?.state ?? ""))))) {
    const error = releaseSchedulerReservationAccounting(state, reservation, disposition, at, reason, terminalCancellation);
    if (error) return error;
  }
  return null;
}

function requestWorkItemReservationRelease(state: DagRunStateV1, workItemId: string, reason: string): void {
  for (const reservation of Object.values(state.scheduler.reservations).filter((candidate) => candidate.workItemId === workItemId && !["released", "fenced"].includes(candidate.state))) {
    reservation.state = "release_requested";
    for (const leaseId of reservation.leaseIds) { const lease = state.leases[leaseId]; if (lease && !["released", "fenced"].includes(lease.state)) { lease.state = "release_requested"; lease.releaseReason = reason; } }
  }
}

function exactGitFact(fact: any, state: DagRunStateV1, factType: string, repositoryId: string, integrationAttemptId: string | null): boolean {
  if (!fact || fact.kind !== "git_transaction" || fact.factType !== factType || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash")))) return false;
  return fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.authorizationSetHash === state.identity.authorizationSet.hash && fact.repositoryId === repositoryId && fact.integrationAttemptId === integrationAttemptId;
}

function gitFactTimeWithinInput(fact: any, state: DagRunStateV1, input: DagRunInputV1, ...prerequisiteTimes: Array<string | null | undefined>): boolean {
  const observedAt = utcTimestampOrderValue(fact?.observedAt);
  return Number.isFinite(observedAt)
    && observedAt >= utcTimestampOrderValue(state.updatedAt)
    && prerequisiteTimes.every((value) => typeof value === "string" && Number.isFinite(utcTimestampOrderValue(value)) && observedAt >= utcTimestampOrderValue(value))
    && observedAt <= utcTimestampOrderValue(input.occurredAt);
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
      case "record_effect_execution": return `${payload.effectId}/${payload.executionObservationHash}`;
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
      case "begin_stage_attempt": return payload.stageAttemptId;
      case "bind_worker_attempt": return payload.stageAttemptId;
      case "record_worker_result": return `${payload.stageAttemptId}/${payload.result.hash}`;
      case "record_candidate": return `${payload.stageAttemptId}/${payload.candidate.hash}`;
      case "record_finding": return payload.finding.id;
      case "record_finding_resolution": return payload.resolution.id;
      case "seal_stage_attempt": return `${payload.stageAttemptId}/${payload.evidence.hash}`;
      case "seal_f8_integration_ready": return `${payload.stageAttemptId}/${payload.integrationReady.hash}`;
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

function exactOwnerLineageIncludes(state: DagRunStateV1, context: DagRunValidationContextV1, launchOwner: any, launchOwnerSessionId: string): boolean {
  if (!launchOwner || launchOwner.sessionId !== launchOwnerSessionId || !state.owner.ownershipReceipt) return false;
  let cursor = { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity, lockIdentity: state.owner.lockIdentity };
  let receiptHash: string | null = state.owner.ownershipReceipt; let expectedEpoch = state.owner.ownerEpoch;
  const target = canonicalHash({ sessionId: launchOwner.sessionId, pid: launchOwner.pid, processStartIdentity: launchOwner.processStartIdentity });
  const visited = new Set<string>();
  for (let depth = 0; depth < 64 && receiptHash; depth += 1) {
    if (canonicalHash({ sessionId: cursor.sessionId, pid: cursor.pid, processStartIdentity: cursor.processStartIdentity }) === target) return true;
    const fact = context.facts[receiptHash] as any;
    if (visited.has(receiptHash) || fact?.kind !== "ownership" || fact.hash !== receiptHash || fact.hash !== canonicalHash(Object.fromEntries(Object.entries(fact).filter(([key]) => key !== "hash"))) || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.ownerEpoch !== expectedEpoch || fact.successorSessionId !== cursor.sessionId || fact.successorPid !== cursor.pid || fact.successorProcessStartIdentity !== cursor.processStartIdentity || fact.successorLockIdentity !== cursor.lockIdentity) return false;
    const predecessor = fact.priorOwnershipReceiptHash ? context.facts[fact.priorOwnershipReceiptHash] as any : null;
    if (fact.chainHash !== ownershipChainHashV1(fact, predecessor?.kind === "ownership" ? predecessor.chainHash : null) || ((fact.priorOwnershipReceiptHash === null) !== (fact.ownerEpoch === 1))) return false;
    visited.add(receiptHash); receiptHash = fact.priorOwnershipReceiptHash; expectedEpoch -= 1;
    cursor = { sessionId: fact.priorSessionId, pid: fact.priorPid, processStartIdentity: fact.priorProcessStartIdentity, lockIdentity: fact.priorLockIdentity };
  }
  return false;
}

function rebindOwnerAuthority(state: DagRunStateV1, successorEpoch: number): void {
  for (const reservation of Object.values(state.scheduler.reservations)) if (!["released", "fenced"].includes(reservation.state)) reservation.ownerEpoch = successorEpoch;
  for (const lease of Object.values(state.leases)) if (!["released", "fenced"].includes(lease.state)) lease.ownerEpoch = successorEpoch;
  // Only opaque-key idempotent worker/worktree operations are safe to reconcile under a
  // proven successor owner. Preserve dispatch count and dispatch/ambiguity state exactly.
  for (const effect of Object.values(state.effects)) {
    const opaqueReconciliationSafe = ["launch_worker", "cancel_worker", "materialize_workspace", "cleanup_worktree", "compose_candidate", "land_target"].includes(effect.kind) && ["pure", "idempotent"].includes(effect.procedureClass);
    const replayableProvenAbsentLanding = effect.kind === "land_target" && effect.state === "reconciled" && effect.reconciliation === "proven_absent" && effect.procedureClass === "idempotent";
    if (opaqueReconciliationSafe && (["intended", "dispatching", "ambiguous"].includes(effect.state) || replayableProvenAbsentLanding)) {
      effect.boundOwnerEpoch = successorEpoch;
      effect.boundAuthorizationSetHash = state.identity.authorizationSet.hash;
      effect.boundFreshnessReceiptHash = state.freshness.receipt.hash;
    }
  }
}

function fenceEffectForCancellation(state: DagRunStateV1, effect: any, at: string): void {
  if (effect.state === "intended" && effect.dispatchCount === 0 && effect.lastDispatchAt === null) { effect.state = "cancelled"; effect.reconciliation = "proven_absent"; return; }
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
  for (const effect of Object.values(state.effects).filter((candidate) => candidate.subject.kind === "work_item" && candidate.subject.id === workItemId)) fenceEffectForCancellation(state, effect, at);
  for (const train of Object.values(state.integrationTrains)) for (const entry of Object.values(train.entries).filter((candidate) => candidate.workItemId === workItemId && candidate.state !== "integrated")) {
    const integrationAttempt = entry.currentAttemptId ? state.integrationAttempts[entry.currentAttemptId] : undefined;
    if (integrationAttempt) {
      const integrationEffectIds = [integrationAttempt.compositionEffectId, integrationAttempt.landingEffectId, ...Object.values(state.effects).filter((effect) => effect.boundIntegrationAttemptId === integrationAttempt.integrationAttemptId).map(({ effectId }) => effectId)].filter((id): id is string => Boolean(id));
      for (const effectId of [...new Set(integrationEffectIds)]) {
        const effect = state.effects[effectId]; if (effect) fenceEffectForCancellation(state, effect, at);
      }
      requestIntegrationLockRelease(state, train.repositoryId, integrationAttempt.integrationAttemptId, "work-item cancellation requested integration release");
    }
    entry.state = "invalidated";
  }
  requestWorkItemReservationRelease(state, workItemId, "work-item cancellation release requested");
}

function cancellationAffectsEffect(state: DagRunStateV1, scope: string, workItemIds: string[], effect: any): boolean {
  if (scope === "run" || (effect.subject.kind === "work_item" && workItemIds.includes(effect.subject.id))) return true;
  return Object.values(state.integrationAttempts).some((attempt) => {
    if (![attempt.compositionEffectId, attempt.landingEffectId].includes(effect.effectId) && effect.boundIntegrationAttemptId !== attempt.integrationAttemptId) return false;
    return Object.values(state.integrationTrains).some((train) => {
      const entry = train.entries[attempt.entryId]; return Boolean(entry && workItemIds.includes(entry.workItemId));
    });
  });
}

function closeCancelledWorkItem(state: DagRunStateV1, workItemId: string, at: string): string | null {
  const item = state.workItems[workItemId];
  item.current = "cancelled";
  for (const attempt of Object.values(state.stageAttempts).filter((candidate) => candidate.workItemId === workItemId && !candidate.terminalAt)) {
    attempt.state = "cancelled"; attempt.updatedAt = at; attempt.terminalAt = at;
    if (attempt.launchIntentId) {
      const launch = state.launchIntents[attempt.launchIntentId];
      if (launch && !state.workerBindings[attempt.stageAttemptId]) launch.state = "not_started";
      else if (launch) launch.state = "closed";
    }
  }
  const lane = state.scheduler.activeNodeLanes[workItemId];
  if (lane && lane.releaseDisposition === null) { lane.releaseDisposition = "terminal_cancelled"; lane.releasedAt = at; }
  for (const train of Object.values(state.integrationTrains)) for (const entry of Object.values(train.entries).filter((candidate) => candidate.workItemId === workItemId && candidate.state !== "integrated")) {
    const integrationAttempt = entry.currentAttemptId ? state.integrationAttempts[entry.currentAttemptId] : undefined;
    if (integrationAttempt) {
      const lockError = releaseIntegrationLock(state, train.repositoryId, integrationAttempt.integrationAttemptId, at, "terminal cancellation effects reconciled");
      if (lockError) return lockError;
    }
  }
  return releaseWorkItemReservations(state, workItemId, "released", at, "terminal cancellation", true);
}

function requestIntegrationLockRelease(state: DagRunStateV1, repositoryId: string, integrationAttemptId: string, reason: string): void {
  const repository = state.repositories[repositoryId]; const train = state.integrationTrains[repositoryId]; const leaseId = repository?.integrationLockLeaseId;
  if (!repository || !train || !leaseId || train.lockLeaseId !== leaseId) return;
  const lease = state.leases[leaseId];
  if (lease?.kind === "integration_lock" && lease.holderIntegrationAttemptId === integrationAttemptId && !["released", "fenced"].includes(lease.state)) { lease.state = "release_requested"; lease.releaseReason = reason; }
}

function releaseIntegrationLock(state: DagRunStateV1, repositoryId: string, integrationAttemptId: string, at: string, reason: string): string | null {
  const repository = state.repositories[repositoryId]; const train = state.integrationTrains[repositoryId];
  if (!repository || !train) return "integration lock release references an unknown repository/train";
  if (repository.integrationLockLeaseId === null && train.lockLeaseId === null) return null;
  const leaseId = repository.integrationLockLeaseId;
  if (!leaseId || train.lockLeaseId !== leaseId) return "integration lock repository/train identity is inconsistent";
  const lease = state.leases[leaseId];
  if (!lease || lease.kind !== "integration_lock" || lease.holderIntegrationAttemptId !== integrationAttemptId) return "integration lock does not bind the exact terminal integration attempt";
  if (!['released', 'fenced'].includes(lease.state)) { lease.state = "released"; lease.releasedAt = at; lease.releaseReason = reason; }
  repository.integrationLockLeaseId = null; train.lockLeaseId = null;
  if (train.activeIntegrationAttemptId === integrationAttemptId) train.activeIntegrationAttemptId = null;
  return null;
}

function rederiveCurrent(state: DagRunStateV1, commandId: string): void {
  const ids = Object.keys(state.workItems).sort();
  state.current.readyWorkItemIds = ids.filter((id) => state.workItems[id].current === "ready");
  state.current.activeWorkItemIds = Object.values(state.scheduler.activeNodeLanes).filter(({ releaseDisposition }) => releaseDisposition === null).map(({ workItemId }) => workItemId).sort();
  state.current.blockedWorkItemIds = ids.filter((id) => state.workItems[id].current === "blocked");
  state.current.integrationReadyWorkItemIds = ids.filter((id) => state.workItems[id].current === "integration_ready");
  if (dagRunNeedsReplanV1(state) || state.desired.run === "needs_replan") state.current.run = "needs_replan";
  else if (state.completion.state === "plan_complete") state.current.run = "completed";
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
