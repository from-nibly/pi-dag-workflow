import { readdir } from "node:fs/promises";
import { canonicalHash, canonicalStringify } from "./common.ts";
import { DagReducerGitIntegrationDriverV1 } from "./integration-driver.ts";
import type { CanonicalDagPlanV1 } from "./plan.ts";
import type { DagRunInputV1 } from "./reducer.ts";
import { lifecycleProcedureEffectRequestV1, type DagRunStateV1, type DagRunValidationContextV1, type ProcedureCatalogBindingV1 } from "./run-state.ts";
import { DagRunSnapshotStoreV1, type DagRunStoreLockIdentityV1 } from "./store.ts";

const OWNED_STAGES = new Set(["F1", "F2", "F3", "F5", "F6"]);
const FRESH_READ_ONLY_STAGES = new Set(["F2", "F5"]);
const DETERMINISTIC_STAGES = new Set(["F4", "F7"]);

type ReservationV1 = DagRunStateV1["scheduler"]["reservations"][string];
type AttemptV1 = DagRunStateV1["stageAttempts"][string];
type WorkerBindingV1 = DagRunStateV1["workerBindings"][string];

export interface DagOwnedWorkerLaunchRequestV1 {
  launchKey: string;
  workerId: string;
  expectedAttemptNumber: number;
  taskPacketHash: string;
  configRequestHash: string;
  repositoryId: string;
  baseCommit: string;
  worktreeKey: string;
  label: string;
  task: string;
  readOnly: boolean;
  freshIndependent: boolean;
  implementationLineageHash: string | null;
}

export interface DagOwnedWorkerLaunchObservationV1 {
  workerStorageId: string;
  launchOwnerSessionId: string;
  workerId: string;
  attemptNumber: number;
  attemptNonce: string;
  configHash: string;
  configFact: unknown;
  supervisorPid: number;
  supervisorStartIdentity: string | null;
  childPid: number | null;
  childStartIdentity: string | null;
  mailboxHash: string | null;
  heartbeatAt: string;
}

export interface DagOwnedWorkerTerminalObservationV1 {
  completionId: string;
  terminalStatus: "succeeded" | "needs_attention" | "failed" | "cancelled" | "lost";
  workerOutput?: DagCandidateSealingResultV1["workerOutput"];
}

export interface DagOwnedWorkerAdapterV1 {
  launchExact(request: DagOwnedWorkerLaunchRequestV1, state: DagRunStateV1): Promise<DagOwnedWorkerLaunchObservationV1>;
  readTerminalExact(binding: WorkerBindingV1, state: DagRunStateV1): Promise<DagOwnedWorkerTerminalObservationV1 | null>;
  cancelExact?(binding: WorkerBindingV1, input: { effectId: string; requestHash: string }, state: DagRunStateV1): Promise<"applied_exact" | "proven_absent" | null>;
  cleanupExact?(binding: WorkerBindingV1, input: { effectId: string; requestHash: string; launchKey: string }, state: DagRunStateV1): Promise<"applied_exact" | "proven_absent" | null>;
}

export interface DagCandidateSealingResultV1 {
  candidate: Record<string, unknown>;
  workerOutput?: {
    outputRepositoryId: string;
    outputCommonDirIdentityHash: string;
    outputWorktreeIdentityHash: string;
    outputSourceBase: Record<string, unknown>;
    outputCommit: string;
    outputTree: string;
    outputObjectFormat: "sha1" | "sha256";
    candidateObservedAt: string;
  };
  /** Only an exact immutable evidence-only adoption fact may avoid conservative F2 invalidation for an F3 delta. */
  f2Transition?: Record<string, unknown>;
  /** Exact executable delta-attestation execution required when f2Transition is an adoption. */
  procedureExecution?: Record<string, unknown>;
}

export interface DagCandidateSealingAdapterV1 {
  inspectAndSealCandidate(input: { plan: CanonicalDagPlanV1; state: DagRunStateV1; attempt: AttemptV1; binding: WorkerBindingV1; repositoryId: string }): Promise<Record<string, unknown> | DagCandidateSealingResultV1 | null>;
}

export interface DagProcedureExecutionResultV1 {
  checkAggregate: Record<string, unknown>;
  evidence: Record<string, unknown>;
  oracleAssertions?: readonly Record<string, unknown>[];
  checkDispositions?: readonly Record<string, unknown>[];
  checkExecutions?: readonly Record<string, unknown>[];
  checkAuthorities?: readonly Record<string, unknown>[];
  environmentObservation?: Record<string, unknown>;
  workspaceMaterialization?: Record<string, unknown>;
  integrationReady?: Record<string, unknown>;
}

export interface DagProcedureExecutionAdapterV1 {
  readonly adapterKind: "immutable-catalog-command-v1";
  readonly allowlistedProcedureHashes?: readonly string[];
  readonly allowlistHash?: string;
  allowsProcedure?(procedure: ProcedureCatalogBindingV1): boolean;
  executeExact(input: { plan: CanonicalDagPlanV1; state: DagRunStateV1; attempt: AttemptV1; procedure: ProcedureCatalogBindingV1; effectId: string; requestHash: string; executionRequest: Readonly<Record<string, unknown>> }): Promise<DagProcedureExecutionResultV1 | null>;
}

export interface DagIntegrationReconciliationAdapterV1 {
  reconcileExact(input: { plan: CanonicalDagPlanV1; state: DagRunStateV1; reservation: ReservationV1; repositoryRoot: string }): Promise<void>;
}

export type DagLifecycleRuntimeFailpointV1 = "after_procedure_intent" | "after_procedure_dispatch" | "after_procedure_result" | "after_procedure_execution_commit" | "after_procedure_reconcile";

export interface DagLifecycleRuntimeOptionsV1 {
  worker?: DagOwnedWorkerAdapterV1;
  candidate?: DagCandidateSealingAdapterV1;
  procedure?: DagProcedureExecutionAdapterV1;
  integration?: DagIntegrationReconciliationAdapterV1;
  failpoint?: (point: DagLifecycleRuntimeFailpointV1, context: Readonly<Record<string, unknown>>) => Promise<void> | void;
}

export interface DagLifecycleReconcileResultV1 {
  state: DagRunStateV1;
  progressed: boolean;
  waiting: boolean;
  reason: string | null;
}

export class DagLifecycleRuntimeV1 {
  readonly store: DagRunSnapshotStoreV1;
  readonly plan: CanonicalDagPlanV1;
  readonly context: DagRunValidationContextV1;
  readonly lock: DagRunStoreLockIdentityV1;
  readonly repositoryRoot: string;
  readonly options: DagLifecycleRuntimeOptionsV1;

  constructor(store: DagRunSnapshotStoreV1, plan: CanonicalDagPlanV1, context: DagRunValidationContextV1, lock: DagRunStoreLockIdentityV1, repositoryRoot: string, options: DagLifecycleRuntimeOptionsV1 = {}) {
    this.store = store;
    this.plan = plan;
    this.context = context;
    this.lock = lock;
    this.repositoryRoot = repositoryRoot;
    this.options = options;
  }

  async reconcileOne(occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    const state = await this.store.read(this.context);

    // Recovery work always precedes fresh scheduling. Both cancellation and retirement are
    // opaque-key, idempotent operations whose durable DAG effects survive dispatch/result crashes.
    const cancellation = await this.reconcileCancellation(state, occurredAt);
    if (cancellation) return cancellation;
    const cleanup = await this.reconcileCleanup(state, occurredAt);
    if (cleanup) return cleanup;

    const outstanding = Object.values(state.scheduler.reservations)
      .filter((reservation) => !["released", "fenced", "launch_ambiguous"].includes(reservation.state))
      .sort((left, right) => left.reservationSequence - right.reservationSequence);
    if (!outstanding.length) return { state, progressed: false, waiting: false, reason: null };

    const intent = outstanding.find((reservation) => reservation.state === "reserved" || reservation.state === "dispatch_intent");
    if (intent?.state === "reserved") {
      return this.mutate(state, "mark_scheduler_reservation_dispatch", "command", {
        reservationId: intent.reservationId,
        normalizedRequestHash: intent.normalizedRequestHash,
      }, occurredAt, `activate-intent-${intent.reservationId}`);
    }
    if (intent?.state === "dispatch_intent") {
      return this.mutate(state, "record_scheduler_reservation_dispatch", "observation", {
        reservationId: intent.reservationId,
        normalizedRequestHash: intent.normalizedRequestHash,
        disposition: "active",
      }, occurredAt, `activate-observe-${intent.reservationId}`);
    }

    const waitingReasons: string[] = [];
    for (const reservation of outstanding) {
      if (reservation.state !== "active") { waitingReasons.push(`reservation ${reservation.reservationId} is ${reservation.state}`); continue; }
      const reconciled = await this.reconcileActive(state, reservation, occurredAt);
      if (reconciled.progressed) return reconciled;
      if (reconciled.reason) waitingReasons.push(reconciled.reason);
    }
    const fresh = await this.store.read(this.context);
    if (fresh.revision !== state.revision || fresh.snapshotHash !== state.snapshotHash) return { state: fresh, progressed: true, waiting: false, reason: null };
    return { state: fresh, progressed: false, waiting: true, reason: waitingReasons.join("; ") || "active reservations are waiting" };
  }

  private async reconcileCancellation(state: DagRunStateV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1 | null> {
    const cancellation = Object.values(state.cancellations).filter((candidate) => candidate.state !== "closed").sort((left, right) => left.requestedAt.localeCompare(right.requestedAt))[0];
    if (!cancellation) return null;
    const cancelEffects = cancellation.effectIds.map((effectId) => state.effects[effectId]).filter(Boolean);
    const intended = cancelEffects.find((effect) => effect.state === "intended");
    if (intended) return this.mutate(state, "mark_effect_dispatching", "command", { effectId: intended.effectId, expectedDispatchCount: intended.dispatchCount }, occurredAt, `cancel-dispatch-${intended.effectId}-${intended.dispatchCount}`);

    const targetIds = Object.keys(cancellation.fencedGenerations);
    const activeAttempts = Object.values(state.stageAttempts).filter((attempt) => targetIds.includes(attempt.workItemId) && attempt.producerKind === "owned_worker" && !attempt.terminalAt).sort((left, right) => left.stageAttemptId.localeCompare(right.stageAttemptId));
    const workerResults: Array<{ stageAttemptId: string; result: any }> = [];
    const observations = new Map<string, any>();
    for (const attempt of activeAttempts) {
      const binding = state.workerBindings[attempt.stageAttemptId];
      if (!binding) {
        const launch = attempt.launchIntentId ? state.launchIntents[attempt.launchIntentId] : null;
        const effect = launch ? state.effects[launch.effectId] : null;
        if (!launch || !effect) return { state, progressed: false, waiting: true, reason: `pre-bind cancellation lacks exact launch authority for ${attempt.stageAttemptId}` };
        if (effect.dispatchCount === 0 && effect.state === "cancelled" && effect.reconciliation === "proven_absent") continue;
        if (effect.dispatchCount <= 0 || launch.state !== "cancel_requested" || effect.state !== "ambiguous" || effect.reconciliation !== "unknown") return { state, progressed: false, waiting: true, reason: `pre-bind cancellation launch boundary is inconsistent for ${attempt.stageAttemptId}` };
        if (!this.options.worker) return { state, progressed: false, waiting: true, reason: "owned-worker cancellation recovery adapter unavailable" };
        const observation = await this.options.worker.launchExact(await this.exactLaunchRequest(state, attempt), state);
        return this.bindLaunchObservation(attempt.stageAttemptId, observation, occurredAt);
      }
      if (!this.options.worker) return { state, progressed: false, waiting: true, reason: "owned-worker cancellation adapter unavailable" };
      const effect = cancelEffects.find((candidate) => candidate.subject.kind === "work_item" && candidate.subject.id === attempt.workItemId && candidate.requestHash === cancelRequestHash(state, attempt, binding));
      if (!effect || !["dispatching", "reconciled"].includes(effect.state)) return { state, progressed: false, waiting: true, reason: `exact cancellation effect is not dispatching or reconciled for ${attempt.stageAttemptId}` };
      if (!this.options.worker.cancelExact) return { state, progressed: false, waiting: true, reason: "owned-worker adapter has no exact cancellation operation" };
      const disposition = effect.state === "reconciled" ? effect.reconciliation as "applied_exact" | "proven_absent" : await this.options.worker.cancelExact(binding, { effectId: effect.effectId, requestHash: effect.requestHash }, state);
      if (!disposition || !["applied_exact", "proven_absent"].includes(disposition)) return { state, progressed: false, waiting: true, reason: `worker cancellation is pending for ${attempt.stageAttemptId}` };
      const terminal = await this.options.worker.readTerminalExact(binding, state);
      if (!terminal) return { state, progressed: false, waiting: true, reason: `worker cancellation awaits the exact terminal result for ${attempt.stageAttemptId}` };
      const result = withHash({
        kind: "worker_result", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
        workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, launchIntentId: binding.launchIntentId,
        workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId,
        attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash,
        completionId: terminal.completionId, terminalStatus: terminal.terminalStatus,
        ...(terminal.workerOutput ?? { outputRepositoryId: null, outputCommonDirIdentityHash: null, outputWorktreeIdentityHash: null, outputSourceBase: null, outputCommit: null, outputTree: null, outputObjectFormat: null, candidateObservedAt: null }),
      });
      workerResults.push({ stageAttemptId: attempt.stageAttemptId, result: await this.publishRef("worker_result", terminal.completionId, result) });
      observations.set(effect.effectId, withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId: effect.effectId, requestHash: effect.requestHash, reconciliation: disposition, closedAt: occurredAt }));
    }

    const unresolved = Object.values(state.effects).filter((effect) => cancellationAffects(state, cancellation.scope, targetIds, effect) && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation));
    for (const effect of unresolved) {
      if (observations.has(effect.effectId)) continue;
      if (["intended", "cancelled"].includes(effect.state) && effect.dispatchCount === 0 && effect.lastDispatchAt === null) observations.set(effect.effectId, withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId: effect.effectId, requestHash: effect.requestHash, reconciliation: "proven_absent", closedAt: occurredAt }));
      else if (effect.executionObservationHash && ((effect.kind === "run_procedure" && effect.boundStageAttemptId) || (effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId))) {
        const execution = await this.store.readImmutableFact(effect.executionObservationHash) as any;
        observations.set(effect.effectId, withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId: effect.effectId, requestHash: effect.requestHash, reconciliation: "applied_exact", executionObservationHash: execution.hash, resultIdentityHash: effect.kind === "run_procedure" ? execution.resultIdentityHash : execution.hash, closedAt: execution.completedAt }));
      } else return { state, progressed: false, waiting: true, reason: `cancellation awaits exact reconciliation of ${effect.kind} effect ${effect.effectId}` };
    }
    const effectObservations = [];
    for (const fact of [...observations.values()].sort((left, right) => left.effectId.localeCompare(right.effectId))) effectObservations.push({ effectId: fact.effectId, observationHash: (await this.publishRef("effect_reconciliation", fact.effectId, fact)).hash });
    const fresh = await this.store.read(this.context);
    const payloadCore = { cancellationId: cancellation.cancellationId, effectObservations, workerResults };
    const resultHash = canonicalHash({ cancellationId: cancellation.cancellationId, effectObservations, workerResults: workerResults.map(({ stageAttemptId, result }) => ({ stageAttemptId, resultHash: result.hash })).sort((left, right) => left.stageAttemptId.localeCompare(right.stageAttemptId)) });
    return this.mutate(fresh, "record_cancellation", "observation", { ...payloadCore, resultHash }, occurredAt, `cancel-result-${cancellation.cancellationId}`);
  }

  private async reconcileCleanup(state: DagRunStateV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1 | null> {
    if (!this.options.worker?.cleanupExact) return null;
    const terminal = Object.values(state.stageAttempts).filter((attempt) => attempt.producerKind === "owned_worker" && attempt.terminalAt && state.workerBindings[attempt.stageAttemptId]).sort((left, right) => left.stageAttemptId.localeCompare(right.stageAttemptId));
    for (const attempt of terminal) {
      const binding = state.workerBindings[attempt.stageAttemptId];
      const launch = state.launchIntents[binding.launchIntentId];
      if (!launch) continue;
      const effectId = opaqueId("cleanupfx", { runNonce: state.runNonce, stageAttemptId: attempt.stageAttemptId, launchKey: launch.launchKey });
      let effect = state.effects[effectId];
      if (!effect) {
        const result = await this.store.readImmutableFact(attempt.workerResult!.hash) as any;
        if (!result.outputRepositoryId || !result.outputCommonDirIdentityHash || !result.outputWorktreeIdentityHash) return { state, progressed: false, waiting: true, reason: `terminal worker ${attempt.stageAttemptId} lacks exact worktree cleanup identity` };
        const requestHash = canonicalHash({ kind: "cleanup_worktree", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, launchIntentId: binding.launchIntentId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, workerResultHash: result.hash, repositoryId: state.workItems[attempt.workItemId].writeRepositoryId, commonDirIdentityHash: result.outputCommonDirIdentityHash, worktreeIdentityHash: result.outputWorktreeIdentityHash });
        const intent = { effectId, kind: "cleanup_worktree", subject: { kind: "work_item", id: attempt.workItemId }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash, boundOwnerEpoch: state.owner.ownerEpoch, boundAuthorizationSetHash: state.identity.authorizationSet.hash, boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: state.workItems[attempt.workItemId].candidateGeneration, boundGateEpochHash: canonicalHash(state.workItems[attempt.workItemId].gateIds.map((gateId) => state.gates[gateId])), boundStageAttemptId: attempt.stageAttemptId, boundWorkerResultHash: result.hash, state: "intended", dispatchCount: 0, createdRevision: state.revision + 1, createdAt: occurredAt, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
        return this.mutate(state, "put_effect_intent", "command", { effect: intent }, occurredAt, `cleanup-intent-${attempt.stageAttemptId}`);
      }
      if (effect.state === "intended") return this.mutate(state, "mark_effect_dispatching", "command", { effectId, expectedDispatchCount: effect.dispatchCount }, occurredAt, `cleanup-dispatch-${effectId}-${effect.dispatchCount}`);
      if (["dispatching", "ambiguous"].includes(effect.state)) {
        const reconciliation = await this.options.worker.cleanupExact(binding, { effectId, requestHash: effect.requestHash, launchKey: launch.launchKey }, state);
        if (!reconciliation) return { state, progressed: false, waiting: true, reason: `worker cleanup is pending for ${attempt.stageAttemptId}` };
        const fact = withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId, requestHash: effect.requestHash, reconciliation, closedAt: occurredAt });
        const reference = await this.publishRef("effect_reconciliation", effectId, fact);
        const fresh = await this.store.read(this.context);
        return this.mutate(fresh, "record_effect_observation", "observation", { effectId, observationHash: reference.hash, reconciliation, terminalState: "reconciled" }, occurredAt, `cleanup-observe-${effectId}`);
      }
    }
    return null;
  }

  private async reconcileActive(state: DagRunStateV1, reservation: ReservationV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    if (reservation.operationKind === "integration") {
      const integration = this.options.integration ?? new DagReducerGitIntegrationDriverV1({ store: this.store, context: this.context, lock: this.lock });
      await integration.reconcileExact({ plan: this.plan, state, reservation, repositoryRoot: this.repositoryRoot });
      const fresh = await this.store.read(this.context);
      return { state: fresh, progressed: fresh.revision !== state.revision || fresh.snapshotHash !== state.snapshotHash, waiting: true, reason: `integration ${reservation.reservationId} delegated` };
    }
    const item = state.workItems[reservation.workItemId];
    const currentAttempt = item?.stages[reservation.stage].currentAttemptId;
    const attempt = currentAttempt ? state.stageAttempts[currentAttempt] : null;
    if (!attempt) return this.beginAttempt(state, reservation, occurredAt);
    if (attempt.producerKind === "owned_worker") return this.reconcileWorker(state, attempt, occurredAt);
    return this.reconcileEvidence(state, attempt, occurredAt);
  }

  private async beginAttempt(state: DagRunStateV1, reservation: ReservationV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    const item = state.workItems[reservation.workItemId];
    const producerKind = producerFor(reservation.stage);
    const stageAttemptId = opaqueId("attempt", { runNonce: state.runNonce, reservationId: reservation.reservationId });
    const inputCore = {
      kind: "stage_attempt_input",
      planHash: state.identity.planHash,
      runId: state.runId,
      runNonce: state.runNonce,
      workItemId: item.workItemId,
      stage: reservation.stage,
      stageAttemptId,
      candidateGeneration: item.candidateGeneration,
      candidateHash: ["F0", "F1"].includes(reservation.stage) ? null : item.candidate?.candidateHash ?? null,
      authorizationSetHash: state.identity.authorizationSet.hash,
      producerKind,
      implementationLineageHash: ["F1", "F3"].includes(reservation.stage) ? item.implementationLineageHash : null,
    };
    const inputFact = withHash(inputCore);
    const attemptInput = await this.publishRef("stage_attempt_input", stageAttemptId, inputFact);

    let launchIntent: any = null;
    let launchEffect: any = null;
    if (producerKind === "owned_worker") {
      const launchIntentId = opaqueId("launch", { runNonce: state.runNonce, reservationId: reservation.reservationId });
      const effectId = opaqueId("launchfx", { runNonce: state.runNonce, reservationId: reservation.reservationId });
      const launchKey = opaqueId("dag", { runId: state.runId, runNonce: state.runNonce, reservationId: reservation.reservationId });
      const workerId = opaqueId("worker", { runNonce: state.runNonce, reservationId: reservation.reservationId });
      const packet = taskPacket(this.plan, state, reservation, stageAttemptId);
      const taskPacketHash = canonicalHash(packet);
      launchEffect = {
        effectId,
        kind: "launch_worker",
        subject: { kind: "work_item", id: item.workItemId },
        effectScopeId: null,
        effectScopeKind: null,
        provider: null,
        procedureClass: "idempotent",
        requestHash: canonicalHash({ launchKey, workerId, expectedAttemptNumber: 1, taskPacketHash, cwdRepositoryId: item.writeRepositoryId }),
        boundOwnerEpoch: state.owner.ownerEpoch,
        boundAuthorizationSetHash: state.identity.authorizationSet.hash,
        boundFreshnessReceiptHash: state.freshness.receipt.hash,
        boundCandidateGeneration: item.candidateGeneration,
        boundGateEpochHash: canonicalHash(item.gateIds.map((gateId) => state.gates[gateId])),
        state: "intended",
        dispatchCount: 0,
        createdRevision: state.revision + 1,
        createdAt: occurredAt,
        lastDispatchAt: null,
        observationHash: null,
        reconciliation: "not_started",
        blockerId: null,
      };
      launchIntent = {
        launchIntentId,
        effectId,
        state: "reserved",
        adapter: "owned-worker-v1",
        launchKey,
        workerId,
        expectedAttemptNumber: 1,
        taskPacketHash,
        cwdRepositoryId: item.writeRepositoryId,
        configRequestHash: canonicalHash({ packet, launchKey, workerId }),
        dispatchCount: 0,
        lastDispatchAt: null,
        boundAt: null,
        ambiguityReason: null,
      };
    }

    return this.mutate(state, "begin_stage_attempt", "command", { reservationId: reservation.reservationId, stageAttemptId, attemptInput, launchIntent, launchEffect }, occurredAt, `begin-${stageAttemptId}`);
  }

  private async exactLaunchRequest(state: DagRunStateV1, attempt: AttemptV1): Promise<DagOwnedWorkerLaunchRequestV1> {
    if (!attempt.launchIntentId) throw new Error("Owned attempt lacks launch intent");
    const launch = state.launchIntents[attempt.launchIntentId];
    const effect = launch ? state.effects[launch.effectId] : null;
    const item = state.workItems[attempt.workItemId];
    const repository = item ? state.repositories[item.writeRepositoryId] : null;
    if (!launch || !effect || !item || !repository) throw new Error("Owned launch authority is incomplete");
    const attemptInput = await this.store.readImmutableFact(attempt.attemptInput.hash) as any;
    if (attemptInput?.kind !== "stage_attempt_input" || attemptInput.hash !== attempt.attemptInput.hash || attemptInput.stageAttemptId !== attempt.stageAttemptId || attemptInput.candidateGeneration !== attempt.inputGeneration || attemptInput.authorizationSetHash !== attempt.authorizationSetHash) throw new Error("Owned launch recovery lacks the exact immutable attempt input");
    const reservation = reservationForAttempt(state, attempt);
    const packet = taskPacket(this.plan, state, reservation, attempt.stageAttemptId, { candidateGeneration: attemptInput.candidateGeneration, candidateHash: attemptInput.candidateHash, authorizationSetHash: attemptInput.authorizationSetHash });
    const taskPacketHash = canonicalHash(packet);
    const configRequestHash = canonicalHash({ packet, launchKey: launch.launchKey, workerId: launch.workerId });
    const requestHash = canonicalHash({ launchKey: launch.launchKey, workerId: launch.workerId, expectedAttemptNumber: launch.expectedAttemptNumber, taskPacketHash, cwdRepositoryId: launch.cwdRepositoryId });
    if (taskPacketHash !== launch.taskPacketHash || configRequestHash !== launch.configRequestHash || requestHash !== effect.requestHash) throw new Error("Owned launch recovery would change the durable opaque operation identity");
    let baseCommit = repository.baseline.commit;
    if (attempt.stage !== "F1") {
      const candidate = typeof attemptInput.candidateHash === "string" ? await this.store.readImmutableFact(attemptInput.candidateHash) as any : null;
      if (candidate?.kind !== "candidate" || candidate.hash !== attemptInput.candidateHash || candidate.planHash !== state.identity.planHash || candidate.runId !== state.runId || candidate.runNonce !== state.runNonce || candidate.workItemId !== attempt.workItemId || candidate.generation !== attempt.inputGeneration || candidate.git?.repositoryId !== launch.cwdRepositoryId) throw new Error("Owned launch recovery lacks the exact immutable candidate input");
      baseCommit = candidate.git.commit;
    }
    return {
      launchKey: launch.launchKey, workerId: launch.workerId, expectedAttemptNumber: launch.expectedAttemptNumber,
      taskPacketHash: launch.taskPacketHash, configRequestHash: launch.configRequestHash, repositoryId: launch.cwdRepositoryId,
      baseCommit, worktreeKey: launch.launchKey, label: `${attempt.workItemId}/${attempt.stage}`, task: canonicalStringify(packet),
      readOnly: FRESH_READ_ONLY_STAGES.has(attempt.stage), freshIndependent: FRESH_READ_ONLY_STAGES.has(attempt.stage), implementationLineageHash: attempt.implementationLineageHash,
    };
  }

  private async bindLaunchObservation(stageAttemptId: string, observation: DagOwnedWorkerLaunchObservationV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    const fresh = await this.store.read(this.context);
    if (fresh.workerBindings[stageAttemptId]) return { state: fresh, progressed: true, waiting: false, reason: null };
    const attempt = fresh.stageAttempts[stageAttemptId];
    const launch = attempt?.launchIntentId ? fresh.launchIntents[attempt.launchIntentId] : null;
    const effect = launch ? fresh.effects[launch.effectId] : null;
    if (!attempt || !launch || !effect) return { state: fresh, progressed: false, waiting: true, reason: "owned launch authority changed before exact binding" };
    const configStored = await this.store.putImmutableFact(observation.configFact);
    if ((observation.configFact as any)?.kind !== "worker_config" || (observation.configFact as any)?.configHash !== observation.configHash || (observation.configFact as any)?.hash !== configStored.hash) throw new Error("Owned-worker config fact does not bind the exact reported config hash");
    if (!observation.supervisorStartIdentity || observation.supervisorPid <= 0) return { state: fresh, progressed: false, waiting: true, reason: "worker launch identity is ambiguous" };
    const configRef = artifactRef("worker_config", `config-${attempt.stageAttemptId}`, configStored.hash, configStored.bytes);
    const binding = {
      stageAttemptId: attempt.stageAttemptId, launchIntentId: launch.launchIntentId, workerStorageId: observation.workerStorageId, launchOwnerSessionId: observation.launchOwnerSessionId,
      workerId: observation.workerId, attemptNumber: observation.attemptNumber, attemptNonce: observation.attemptNonce, configHash: observation.configHash, configRef,
      supervisorPid: observation.supervisorPid, supervisorStartIdentity: observation.supervisorStartIdentity, childPid: observation.childPid, childStartIdentity: observation.childStartIdentity,
      mailboxHash: observation.mailboxHash, heartbeatAt: occurredAt, completionId: null, resultHash: null,
    };
    const launchObservationCore = {
      kind: "worker_launch_observation", planHash: fresh.identity.planHash, runId: fresh.runId, runNonce: fresh.runNonce, authorizationSetHash: fresh.identity.authorizationSet.hash,
      ownerEpoch: fresh.owner.ownerEpoch, effectId: effect.effectId, requestHash: effect.requestHash, launchIntentId: launch.launchIntentId, launchKey: launch.launchKey,
      workerStorageId: observation.workerStorageId, launchOwnerSessionId: observation.launchOwnerSessionId, workerId: observation.workerId, attemptNumber: observation.attemptNumber,
      attemptNonce: observation.attemptNonce, configHash: observation.configHash, supervisorPid: observation.supervisorPid, supervisorStartIdentity: observation.supervisorStartIdentity,
      reconciliation: "applied_exact", observedAt: occurredAt,
    };
    const launchObservation = await this.publishRef("worker_launch_observation", `launch-${attempt.stageAttemptId}`, withHash(launchObservationCore));
    return this.mutate(fresh, "bind_worker_attempt", "observation", { stageAttemptId: attempt.stageAttemptId, binding, launchObservation }, occurredAt, `bind-${attempt.stageAttemptId}`);
  }

  private async reconcileWorker(state: DagRunStateV1, attempt: AttemptV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    if (!attempt.launchIntentId) return { state, progressed: false, waiting: true, reason: "owned attempt lacks launch intent" };
    const launch = state.launchIntents[attempt.launchIntentId];
    const effect = launch ? state.effects[launch.effectId] : null;
    if (!launch || !effect) return { state, progressed: false, waiting: true, reason: "owned launch authority is incomplete" };

    if (attempt.state === "launching" && effect.state === "intended") {
      return this.mutate(state, "mark_effect_dispatching", "command", { effectId: effect.effectId, expectedDispatchCount: effect.dispatchCount }, occurredAt, `dispatch-${effect.effectId}-${effect.dispatchCount}`);
    }
    if (attempt.state === "launching") {
      if (!this.options.worker) return { state, progressed: false, waiting: true, reason: "owned-worker adapter unavailable" };
      const observation = await this.options.worker.launchExact(await this.exactLaunchRequest(state, attempt), state);
      return this.bindLaunchObservation(attempt.stageAttemptId, observation, occurredAt);
    }

    if (["running", "settling"].includes(attempt.state)) {
      if (!this.options.worker) return { state, progressed: false, waiting: true, reason: "owned-worker adapter unavailable" };
      const binding = state.workerBindings[attempt.stageAttemptId];
      if (!binding) return { state, progressed: false, waiting: true, reason: "owned-worker binding unavailable" };
      const terminal = await this.options.worker.readTerminalExact(binding, state);
      if (!terminal) return { state, progressed: false, waiting: true, reason: "owned worker still active" };
      let workerOutput: DagCandidateSealingResultV1["workerOutput"] | null = terminal.workerOutput ?? null;
      if (terminal.terminalStatus === "succeeded" && ["F1", "F3"].includes(attempt.stage)) {
        if (!this.options.candidate) return { state, progressed: false, waiting: true, reason: `${attempt.stage} candidate output observation adapter unavailable` };
        const sealed = await this.options.candidate.inspectAndSealCandidate({ plan: this.plan, state, attempt, binding, repositoryId: state.workItems[attempt.workItemId].writeRepositoryId });
        const candidateOutput = (sealed as DagCandidateSealingResultV1 | null)?.workerOutput ?? null;
        if (workerOutput && candidateOutput && canonicalHash(workerOutput) !== canonicalHash(candidateOutput)) throw new Error(`${attempt.stage} terminal and candidate adapters disagree on exact Git output identity`);
        workerOutput = candidateOutput ?? workerOutput;
        if (!workerOutput) return { state, progressed: false, waiting: true, reason: `${attempt.stage} worker result lacks exact Git output observation` };
      }
      const nullOutput = { outputRepositoryId: null, outputCommonDirIdentityHash: null, outputWorktreeIdentityHash: null, outputSourceBase: null, outputCommit: null, outputTree: null, outputObjectFormat: null, candidateObservedAt: null };
      const resultCore = {
        kind: "worker_result",
        planHash: state.identity.planHash,
        runId: state.runId,
        runNonce: state.runNonce,
        workItemId: attempt.workItemId,
        stage: attempt.stage,
        stageAttemptId: attempt.stageAttemptId,
        launchIntentId: binding.launchIntentId,
        workerStorageId: binding.workerStorageId,
        launchOwnerSessionId: binding.launchOwnerSessionId,
        workerId: binding.workerId,
        attemptNumber: binding.attemptNumber,
        attemptNonce: binding.attemptNonce,
        configHash: binding.configHash,
        completionId: terminal.completionId,
        terminalStatus: terminal.terminalStatus,
        ...(workerOutput ?? nullOutput),
      };
      const fact = withHash(resultCore);
      const result = await this.publishRef("worker_result", terminal.completionId, fact);
      const fresh = await this.store.read(this.context);
      if (fresh.stageAttempts[attempt.stageAttemptId]?.workerResult) return { state: fresh, progressed: true, waiting: false, reason: null };
      return this.mutate(fresh, "record_worker_result", "observation", { stageAttemptId: attempt.stageAttemptId, result }, occurredAt, `result-${attempt.stageAttemptId}`);
    }

    if (attempt.state === "result_observed") {
      const resultFact = await this.store.readImmutableFact(attempt.workerResult!.hash) as any;
      if (resultFact.terminalStatus === "succeeded" && ["F1", "F3"].includes(attempt.stage) && state.workItems[attempt.workItemId].candidate?.producedByStageAttemptId !== attempt.stageAttemptId) {
        if (!this.options.candidate) return { state, progressed: false, waiting: true, reason: `${attempt.stage} candidate sealing adapter unavailable` };
        const binding = state.workerBindings[attempt.stageAttemptId];
        const sealed = await this.options.candidate.inspectAndSealCandidate({ plan: this.plan, state, attempt, binding, repositoryId: state.workItems[attempt.workItemId].writeRepositoryId });
        if (!sealed) return { state, progressed: false, waiting: true, reason: "candidate is not sealed" };
        const candidate = (sealed as DagCandidateSealingResultV1).candidate ?? sealed as Record<string, unknown>;
        const transitionFromAdapter = (sealed as DagCandidateSealingResultV1).candidate ? (sealed as DagCandidateSealingResultV1).f2Transition : undefined;
        const procedureExecutionFromAdapter = (sealed as DagCandidateSealingResultV1).candidate ? (sealed as DagCandidateSealingResultV1).procedureExecution : undefined;
        const prior = state.workItems[attempt.workItemId].candidate;
        const candidateGit = (candidate as any).git;
        // F3 is codification, not a second implementation lineage. Exact no-delta output
        // reuses the F1 candidate and proceeds without consuming a generation.
        if (attempt.stage === "F3" && prior && canonicalHash(candidateGit) === canonicalHash(prior.git)) return this.reconcileEvidence(state, attempt, occurredAt);
        const reference = await this.publishRef("candidate", String((candidate as any).candidateId ?? attempt.stageAttemptId), candidate);
        let f2Transition: any = undefined;
        if (attempt.stage === "F3") {
          if (!prior) throw new Error("F3 delta cannot be sealed without its exact prior candidate");
          const transition = transitionFromAdapter ?? withHash({
            kind: "invalidation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
            authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: "F2",
            fromCandidateGeneration: prior.generation, fromCandidateHash: prior.candidateHash,
            toCandidateGeneration: (candidate as any).generation, toCandidateHash: reference.hash,
            f3StageAttemptId: attempt.stageAttemptId, priorEvidenceHash: state.workItems[attempt.workItemId].stages.F2.currentEvidence,
            reason: "unknown_impact", rerouteStage: "F2",
          });
          f2Transition = await this.publishRef((transition as any).kind, `f2-transition-${attempt.stageAttemptId}`, transition);
        }
        const procedureExecution = procedureExecutionFromAdapter ? await this.publishRef("procedure_execution", `delta-execution-${attempt.stageAttemptId}`, procedureExecutionFromAdapter) : undefined;
        const fresh = await this.store.read(this.context);
        if (fresh.workItems[attempt.workItemId].candidate?.producedByStageAttemptId === attempt.stageAttemptId) return { state: fresh, progressed: true, waiting: false, reason: null };
        return this.mutate(fresh, "record_candidate", "observation", { stageAttemptId: attempt.stageAttemptId, candidate: reference, ...(f2Transition ? { f2Transition } : {}), ...(procedureExecution ? { procedureExecution } : {}) }, occurredAt, `candidate-${attempt.stageAttemptId}`);
      }
      // Genuine retry-safe failed/cancelled/lost results still execute the exact catalog
      // evidence adapter and must seal a non-PASS disposition instead of waiting forever.
      return this.reconcileEvidence(state, attempt, occurredAt);
    }
    return { state, progressed: false, waiting: true, reason: `attempt ${attempt.stageAttemptId} is ${attempt.state}` };
  }

  private async reconcileEvidence(state: DagRunStateV1, attempt: AttemptV1, occurredAt: string): Promise<DagLifecycleReconcileResultV1> {
    if (!this.options.procedure) return { state, progressed: false, waiting: true, reason: "immutable catalog command mapping is absent; lifecycle procedure execution is blocked" };
    const adapter = this.options.procedure;
    const allowlist = [...(adapter.allowlistedProcedureHashes ?? [])].sort();
    const staticAllowlistValid = allowlist.length > 0 && new Set(allowlist).size === allowlist.length && adapter.allowlistHash === canonicalHash(allowlist);
    if (adapter.adapterKind !== "immutable-catalog-command-v1" || (!staticAllowlistValid && !adapter.allowsProcedure)) return { state, progressed: false, waiting: true, reason: "lifecycle procedure adapter allowlist identity is invalid" };
    const procedures = Object.values(this.context.catalog.procedures).filter((procedure) => procedure.purpose === "lifecycle" && procedure.stages.includes(attempt.stage as any) && procedure.producerKinds.includes(attempt.producerKind)).sort((left, right) => left.hash.localeCompare(right.hash));
    if (procedures.length !== 1) return { state, progressed: false, waiting: true, reason: `exact lifecycle procedure unavailable or ambiguous for ${attempt.stage}/${attempt.producerKind}` };
    const procedure = procedures[0];
    if (!(staticAllowlistValid ? allowlist.includes(procedure.hash) : adapter.allowsProcedure!(procedure))) return { state, progressed: false, waiting: true, reason: `immutable command mapping absent for catalog procedure ${procedure.procedureId}/${procedure.hash}` };
    if ((FRESH_READ_ONLY_STAGES.has(attempt.stage) || DETERMINISTIC_STAGES.has(attempt.stage)) && !procedure.readOnly) return { state, progressed: false, waiting: true, reason: `${attempt.stage} requires an exact read-only procedure` };

    let executionRequest = lifecycleProcedureEffectRequestV1(state, this.context, attempt, procedure);
    const effectId = opaqueId("procedure", { runNonce: state.runNonce, stageAttemptId: attempt.stageAttemptId, procedureHash: procedure.hash });
    const matching = Object.values(state.effects).filter((effect: any) => effect.kind === "run_procedure" && effect.boundStageAttemptId === attempt.stageAttemptId);
    if (matching.length > 1 || (matching.length === 1 && matching[0].effectId !== effectId)) throw new Error("Lifecycle attempt has conflicting procedure effect authority");
    let effect: any = matching[0];
    if (effect) executionRequest = { ...executionRequest, ownerEpoch: effect.boundOwnerEpoch, authorizationSetHash: effect.boundAuthorizationSetHash, freshnessReceiptHash: effect.boundFreshnessReceiptHash };
    const requestHash = canonicalHash(executionRequest);
    if (!effect) {
      effect = {
        effectId, kind: "run_procedure", subject: { kind: "work_item", id: attempt.workItemId }, boundStageAttemptId: attempt.stageAttemptId,
        boundIntegrationAttemptId: null, boundWorkerResultHash: attempt.workerResult?.hash ?? null, executionRequest, executionObservationHash: null,
        effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash,
        boundOwnerEpoch: state.owner.ownerEpoch, boundAuthorizationSetHash: state.identity.authorizationSet.hash,
        boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: attempt.reservedOutputGeneration ?? attempt.inputGeneration,
        boundGateEpochHash: canonicalHash(state.workItems[attempt.workItemId].gateIds.map((gateId) => state.gates[gateId])),
        state: "intended", dispatchCount: 0, createdRevision: state.revision + 1, createdAt: occurredAt, lastDispatchAt: null,
        observationHash: null, reconciliation: "not_started", blockerId: null,
      };
      const result = await this.mutate(state, "put_effect_intent", "command", { effect }, occurredAt, `procedure-intent-${effectId}`);
      await this.options.failpoint?.("after_procedure_intent", { stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, effectId, requestHash });
      return result;
    }
    if (canonicalHash(effect.executionRequest) !== requestHash || effect.requestHash !== requestHash) throw new Error("Lifecycle procedure recovery would change the persisted canonical request identity");
    if (effect.state === "intended") {
      const result = await this.mutate(state, "mark_effect_dispatching", "command", { effectId, expectedDispatchCount: effect.dispatchCount }, occurredAt, `procedure-dispatch-${effectId}-${effect.dispatchCount}`);
      await this.options.failpoint?.("after_procedure_dispatch", { stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, effectId, requestHash });
      return result;
    }
    if (effect.state === "dispatching") {
      let observation = await this.findDurableProcedureObservation(effect);
      if (!observation) {
        const output = await adapter.executeExact({ plan: this.plan, state, attempt, procedure, effectId, requestHash, executionRequest });
        if (!output) return { state, progressed: false, waiting: true, reason: "lifecycle procedure is blocked" };
        assertProcedureOutput(output, state, attempt, procedure);
        if ((output.evidence as any).effectReconciliationHashes?.length !== 0) throw new Error("Procedure adapter cannot pre-authorize its own effect reconciliation closure");
        await this.publishRawProcedureOutput(output, attempt);
        const resultIdentityHash = canonicalHash(output);
        const resultBytes = Buffer.byteLength(canonicalStringify(output));
        if (resultBytes > 4 * 1024 * 1024) throw new Error("Lifecycle procedure result exceeds the closed 4 MiB observation bound");
        observation = withHash({
          kind: "effect_execution_observation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
          authorizationSetHash: effect.boundAuthorizationSetHash, freshnessReceiptHash: effect.boundFreshnessReceiptHash, ownerEpoch: effect.boundOwnerEpoch,
          effectId, requestHash, requestIdentityHash: requestHash, operationKind: "lifecycle_procedure",
          executionId: opaqueId("procedure-execution", { effectId, requestHash }), resultIdentityHash, result: output,
          disposition: (output.checkAggregate as any).disposition, resultBytes, startedAt: occurredAt, completedAt: occurredAt,
        });
        const stored = await this.store.putImmutableFact(observation);
        if (stored.hash !== observation.hash) throw new Error("Procedure execution observation does not bind its exact immutable hash");
        await this.options.failpoint?.("after_procedure_result", { stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, effectId, requestHash, executionObservationHash: observation.hash });
      }
      const fresh = await this.store.read(this.context);
      const result = await this.mutate(fresh, "record_effect_execution", "observation", { effectId, executionObservationHash: observation.hash }, occurredAt, `procedure-result-${effectId}-${observation.hash.slice(7, 19)}`);
      await this.options.failpoint?.("after_procedure_execution_commit", { stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, effectId, requestHash, executionObservationHash: observation.hash });
      return result;
    }
    if (effect.state === "observed") {
      const execution = await this.store.readImmutableFact(effect.executionObservationHash) as any;
      const reconciliation = withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId, requestHash, reconciliation: "applied_exact", executionObservationHash: execution.hash, resultIdentityHash: execution.resultIdentityHash, closedAt: execution.completedAt });
      const reference = await this.publishRef("effect_reconciliation", effectId, reconciliation);
      const fresh = await this.store.read(this.context);
      const result = await this.mutate(fresh, "record_effect_observation", "observation", { effectId, observationHash: reference.hash, reconciliation: "applied_exact", terminalState: "reconciled" }, occurredAt, `procedure-reconcile-${effectId}`);
      await this.options.failpoint?.("after_procedure_reconcile", { stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, effectId, requestHash, reconciliationHash: reference.hash });
      return result;
    }
    if (effect.state !== "reconciled" || effect.reconciliation !== "applied_exact" || !effect.executionObservationHash || !effect.observationHash) return { state, progressed: false, waiting: true, reason: `procedure effect ${effectId} is terminally ${effect.state}/${effect.reconciliation}` };

    const execution = await this.store.readImmutableFact(effect.executionObservationHash) as any;
    const reconciliation = await this.store.readImmutableFact(effect.observationHash) as any;
    const output = closeProcedureOutput(execution.result as DagProcedureExecutionResultV1, reconciliation.hash, reconciliation.closedAt);
    const aggregate = await this.publishRef("check_aggregate", `aggregate-${attempt.stageAttemptId}`, output.checkAggregate);
    const evidence = await this.publishRef("stage_evidence", `evidence-${attempt.stageAttemptId}`, output.evidence);
    const oracleAssertions = await Promise.all((output.oracleAssertions ?? []).map((fact: any) => this.publishRef("oracle_assertion", String(fact.assertionId), fact)));
    const checkDispositions = await Promise.all((output.checkDispositions ?? []).map((fact: any) => this.publishRef("check_disposition", String(fact.checkId), fact)));
    const checkExecutions = await Promise.all((output.checkExecutions ?? []).map((fact: any) => this.publishRef("check_execution", String(fact.executionId), fact)));
    const checkAuthorities = await Promise.all((output.checkAuthorities ?? []).map((fact: any) => this.publishRef(String(fact.kind), String(fact.checkId), fact)));
    const effectReconciliations = [artifactRef("effect_reconciliation", effectId, reconciliation.hash, Buffer.byteLength(canonicalStringify(reconciliation)))];
    const payload: any = { stageAttemptId: attempt.stageAttemptId, evidence, checkAggregate: aggregate, oracleAssertions, checkDispositions, checkExecutions, checkAuthorities, effectReconciliations };
    if (output.environmentObservation) payload.environmentObservation = await this.publishRef("environment_observation", attempt.stageAttemptId, output.environmentObservation);
    if (output.workspaceMaterialization) payload.workspaceMaterialization = await this.publishRef("workspace_materialization", attempt.stageAttemptId, output.workspaceMaterialization);
    let type = "seal_stage_attempt";
    if (attempt.stage === "F8") {
      if (!output.integrationReady) return { state, progressed: false, waiting: true, reason: "F8 procedure did not produce atomic integration-ready input" };
      payload.integrationReady = await this.publishRef("integration_ready", attempt.workItemId, output.integrationReady);
      type = "seal_f8_integration_ready";
    }
    const fresh = await this.store.read(this.context);
    if (fresh.stageAttempts[attempt.stageAttemptId]?.state === "sealed") return { state: fresh, progressed: true, waiting: false, reason: null };
    return this.mutate(fresh, type, "observation", payload, occurredAt, `seal-${attempt.stageAttemptId}`);
  }

  private async publishRawProcedureOutput(output: DagProcedureExecutionResultV1, attempt: AttemptV1): Promise<void> {
    const facts = [output.checkAggregate, output.evidence, ...(output.oracleAssertions ?? []), ...(output.checkDispositions ?? []), ...(output.checkExecutions ?? []), ...(output.checkAuthorities ?? []), output.environmentObservation, output.workspaceMaterialization, output.integrationReady].filter(Boolean) as Record<string, unknown>[];
    for (const fact of facts) {
      const stored = await this.store.putImmutableFact(fact);
      if ((fact as any).hash !== stored.hash) throw new Error(`Procedure result fact ${(fact as any).kind ?? "unknown"} does not have an exact canonical self-hash for ${attempt.stageAttemptId}`);
    }
  }

  private async findDurableProcedureObservation(effect: any): Promise<any | null> {
    const entries = (await readdir(this.store.factsDirectory)).filter((name) => /^[0-9a-f]{64}\.json$/.test(name)).sort();
    if (entries.length > 20_000) throw new Error("Immutable fact scan exceeds the bounded procedure-recovery limit");
    const matches: any[] = [];
    for (const name of entries) {
      const fact = await this.store.readImmutableFact(`sha256:${name.slice(0, 64)}`) as any;
      if (fact?.kind === "effect_execution_observation" && fact.effectId === effect.effectId && fact.requestHash === effect.requestHash) matches.push(fact);
    }
    if (matches.length > 1) throw new Error(`Conflicting immutable procedure observations exist for ${effect.effectId}/${effect.requestHash}`);
    return matches[0] ?? null;
  }

  private async publishRef(kind: string, id: string, fact: any): Promise<any> {
    const stored = await this.store.putImmutableFact(fact);
    if (fact?.hash !== stored.hash) throw new Error(`${kind} adapter fact does not have an exact canonical self-hash`);
    return artifactRef(kind, id, stored.hash, stored.bytes);
  }

  private async mutate(state: DagRunStateV1, type: string, kind: "command" | "observation", payload: any, occurredAt: string, slot: string): Promise<DagLifecycleReconcileResultV1> {
    const input = reducerInput(state, type, kind, payload, occurredAt, slot);
    const result = await this.store.mutate({ input, context: this.context, lock: this.lock });
    if (!result.accepted) {
      const fresh = await this.store.read(this.context);
      if (fresh.revision !== state.revision || fresh.snapshotHash !== state.snapshotHash) return { state: fresh, progressed: true, waiting: false, reason: null };
      throw new Error(`Lifecycle ${type} rejected: ${result.code}: ${result.message}`);
    }
    return { state: result.state, progressed: true, waiting: false, reason: null };
  }
}

function producerFor(stage: string): "conductor" | "owned_worker" | "deterministic_runner" {
  if (OWNED_STAGES.has(stage)) return "owned_worker";
  if (DETERMINISTIC_STAGES.has(stage)) return "deterministic_runner";
  return "conductor";
}

function reservationForAttempt(state: DagRunStateV1, attempt: AttemptV1): ReservationV1 {
  const matches = Object.values(state.scheduler.reservations).filter((reservation) => reservation.workItemId === attempt.workItemId && reservation.stage === attempt.stage && !["released", "fenced"].includes(reservation.state) && reservation.leaseIds.length === attempt.leaseIds.length && reservation.leaseIds.every((leaseId) => attempt.leaseIds.includes(leaseId)));
  if (matches.length !== 1) throw new Error("Owned attempt does not resolve one exact active reservation");
  return matches[0];
}

function taskPacket(plan: CanonicalDagPlanV1, state: DagRunStateV1, reservation: ReservationV1, stageAttemptId: string, exactInput?: { candidateGeneration: number; candidateHash: string | null; authorizationSetHash: string }): Record<string, unknown> {
  const item = state.workItems[reservation.workItemId];
  const planItem = plan.workItems.find((candidate) => candidate.workItemId === item.workItemId);
  if (!planItem) throw new Error(`Plan work item unavailable: ${item.workItemId}`);
  return {
    schemaVersion: 1,
    kind: "dag_owned_stage_packet",
    planHash: plan.planHash,
    runId: state.runId,
    runNonce: state.runNonce,
    authorizationSetHash: exactInput?.authorizationSetHash ?? state.identity.authorizationSet.hash,
    workItemId: item.workItemId,
    stage: reservation.stage,
    stageAttemptId,
    candidateGeneration: exactInput?.candidateGeneration ?? item.candidateGeneration,
    candidateHash: ["F0", "F1"].includes(reservation.stage) ? null : exactInput?.candidateHash ?? item.candidate?.candidateHash ?? null,
    implementationLineageHash: ["F1", "F3"].includes(reservation.stage) ? item.implementationLineageHash : null,
    role: reservation.workerRole,
    readOnly: FRESH_READ_ONLY_STAGES.has(reservation.stage),
    freshIndependent: FRESH_READ_ONLY_STAGES.has(reservation.stage),
    deterministicNoEdit: DETERMINISTIC_STAGES.has(reservation.stage),
    cleanEnvironment: reservation.stage === "F7",
    executionContract: reservation.stage === "F1"
      ? "Implement only the exact objective in this disposable worktree, commit the candidate, leave it clean, and report bounded diagnostics; never advance lifecycle or integrate."
      : reservation.stage === "F3"
        ? "Codify only exact evidence/contract deltas on the existing F1 lineage. If any file changes, commit them and leave the worktree clean; never discard output, advance lifecycle, or integrate."
      : FRESH_READ_ONLY_STAGES.has(reservation.stage)
        ? "Observe the exact candidate independently without edits, commits, lifecycle advancement, or integration; report bounded evidence only."
        : "Execute only this fixed role in the disposable worktree; never infer lifecycle authority, integrate, or land changes.",
    objective: planItem.objective,
    checks: planItem.checks,
    oracleIds: planItem.oracleIds,
  };
}

function assertProcedureOutput(output: DagProcedureExecutionResultV1, state: DagRunStateV1, attempt: AttemptV1, procedure: ProcedureCatalogBindingV1): void {
  const aggregate: any = output.checkAggregate;
  const evidence: any = output.evidence;
  const exact = aggregate?.kind === "check_aggregate" && evidence?.kind === "stage_evidence" && aggregate.hash === canonicalHash(Object.fromEntries(Object.entries(aggregate).filter(([key]) => key !== "hash"))) && evidence.hash === canonicalHash(Object.fromEntries(Object.entries(evidence).filter(([key]) => key !== "hash"))) && aggregate.procedureHash === procedure.hash && evidence.procedureHash === procedure.hash && aggregate.environmentProfileHash === procedure.environmentProfileHash && evidence.environmentProfileHash === procedure.environmentProfileHash && aggregate.planHash === state.identity.planHash && evidence.planHash === state.identity.planHash && aggregate.runId === state.runId && evidence.runId === state.runId && aggregate.runNonce === state.runNonce && evidence.runNonce === state.runNonce && aggregate.authorizationSetHash === state.identity.authorizationSet.hash && evidence.authorizationSetHash === state.identity.authorizationSet.hash && typeof evidence.producedAt === "string" && evidence.producedAt === state.updatedAt && aggregate.workItemId === attempt.workItemId && evidence.workItemId === attempt.workItemId && aggregate.stage === attempt.stage && evidence.stage === attempt.stage && aggregate.stageAttemptId === attempt.stageAttemptId && evidence.stageAttemptId === attempt.stageAttemptId && aggregate.attemptInputHash === attempt.attemptInput.hash && evidence.attemptInputHash === attempt.attemptInput.hash && evidence.checkAggregateHash === aggregate.hash && evidence.readOnly === procedure.readOnly && evidence.producerKind === attempt.producerKind && evidence.producerResultHash === (attempt.workerResult?.hash ?? null) && evidence.disposition === aggregate.disposition;
  if (!exact) throw new Error("Procedure adapter output does not bind the exact catalog/attempt authority");
  if (["F2", "F5", "F7"].includes(attempt.stage)) {
    const observation: any = output.environmentObservation; const materialization: any = output.workspaceMaterialization;
    if (!observation || !materialization || evidence.environmentObservationHash !== observation.hash || observation.workspaceMaterializationHash !== materialization.hash) throw new Error(`${attempt.stage} output must carry exact workspace-materialization and environment-observation authority`);
  } else if (evidence.environmentObservationHash != null || output.environmentObservation || output.workspaceMaterialization) throw new Error("Only F2/F5/F7 may carry environment authority");
  if (DETERMINISTIC_STAGES.has(attempt.stage) && (attempt.producerKind !== "deterministic_runner" || !evidence.readOnly)) throw new Error(`${attempt.stage} must be deterministic and no-edit`);
}

function closeProcedureOutput(outputValue: DagProcedureExecutionResultV1, reconciliationHash: string, reconciliationClosedAt: string): DagProcedureExecutionResultV1 {
  const output = structuredClone(outputValue) as any;
  const evidenceCore = Object.fromEntries(Object.entries(output.evidence ?? {}).filter(([key]) => key !== "hash"));
  output.evidence = withHash({ ...evidenceCore, effectReconciliationHashes: [reconciliationHash], producedAt: reconciliationClosedAt });
  if (output.integrationReady) {
    const readyCore = Object.fromEntries(Object.entries(output.integrationReady).filter(([key]) => key !== "hash"));
    output.integrationReady = withHash({ ...readyCore, f8EvidenceHash: output.evidence.hash, effectsReconciled: true });
  }
  return output;
}

function reducerInput(state: DagRunStateV1, type: string, kind: "command" | "observation", payload: any, occurredAt: string, slot: string): DagRunInputV1 {
  return { schemaVersion: 1, kind, type, commandId: slot, idempotencyKey: `${state.runNonce}:${slot}`, payloadHash: canonicalHash(payload), runId: state.runId, runNonce: state.runNonce, expectedRevision: state.revision, expectedSnapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch, occurredAt, payload } as DagRunInputV1;
}

function artifactRef(kind: string, id: string, hash: string, bytes: number): any {
  return { kind, schemaVersion: 1, id: id.slice(0, 128), hash, bytes, mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null };
}

function withHash<T extends Record<string, unknown>>(core: T): T & { hash: string } {
  return { ...core, hash: canonicalHash(core) };
}

function cancelRequestHash(state: DagRunStateV1, attempt: AttemptV1, binding: WorkerBindingV1): string {
  return canonicalHash({ kind: "cancel_worker", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, fencedGeneration: state.workItems[attempt.workItemId].candidateGeneration });
}

function cancellationAffects(state: DagRunStateV1, scope: string, workItemIds: string[], effect: DagRunStateV1["effects"][string]): boolean {
  if (scope === "run" || (effect.subject.kind === "work_item" && workItemIds.includes(effect.subject.id))) return true;
  return Object.values(state.integrationAttempts).some((attempt) => ([attempt.compositionEffectId, attempt.landingEffectId].includes(effect.effectId) || effect.boundIntegrationAttemptId === attempt.integrationAttemptId) && Object.values(state.integrationTrains).some((train) => workItemIds.includes(train.entries[attempt.entryId]?.workItemId)));
}

function opaqueId(prefix: string, value: unknown): string {
  return `${prefix}-${canonicalHash(value).slice("sha256:".length, "sha256:".length + 32)}`;
}
