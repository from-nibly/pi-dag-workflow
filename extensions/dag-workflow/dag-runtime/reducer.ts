import { Type, type Static } from "typebox";
import {
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
      if (!effect || !["dispatching", "ambiguous"].includes(effect.state) || effect.dispatchCount !== payload.expectedDispatchCount) return precondition("uncertain effect is not retryable at the expected dispatch count");
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
