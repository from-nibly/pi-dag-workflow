import { createHash, randomUUID } from "node:crypto";
import { link, mkdir, open, readFile, readdir, realpath, rename, rm, stat } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { canonicalHash, canonicalStringify, parseStrictJson } from "./common.ts";
import { parseCanonicalDagPlanV1, type CanonicalDagPlanV1 } from "./plan.ts";
import { type DagRunInputV1 } from "./reducer.ts";
import { parseDagRunStateV1, type DagRunStateV1, type DagRunValidationContextV1 } from "./run-state.ts";
import { buildSchedulerPlanIndexV1, DAG_SCHEDULER_POLICY_HASH_V1, projectDagExecutionV1, scheduleDagRunV1, type DagExecutionProjectionV1, type DagSchedulerDecisionV1, type DagWorkerProjectionInputV1 } from "./scheduler.ts";
import { DagRunSnapshotStoreV1, createDagRunStoreDeadOwnerProofV1, dagRunStoreLockIdentityFromOwner, type DagRunStoreLockIdentityV1 } from "./store.ts";

const RUN_ROOT = ".ai/dag-runs-v1";
const BINDING_ROOT = ".ai/dag-session-bindings-v1";
const HASH_RE = /^sha256:[0-9a-f]{64}$/;
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export interface DagConductorContextV1 {
  cwd: string;
  sessionManager: { getSessionId(): string; getSessionFile?(): string | null; getHeader?(): any };
}

export interface DagSessionRunBindingV1 {
  schemaVersion: 1;
  kind: "DagSessionRunBindingV1";
  sessionId: string;
  sessionFileHash: string | null;
  repositoryRootHash: string;
  commonDirIdentityHash: string;
  branchRef: string;
  runId: string;
  runNonceHash: string;
  planHash: string;
  ownerEpoch: number;
  ownershipReceiptHash: string;
  lineage: { kind: "start" | "direct_fork" | "explicit_reattach"; priorBindingHash: string | null; priorSessionId: string | null; proofHash: string | null };
  storeRoot: typeof RUN_ROOT;
  boundAt: string;
  bindingHash: string;
}

export interface DagRunStartInputV1 {
  runId: string;
  runNonce: string;
  planHash: string;
  planPath: string;
  genesisPath: string;
  contextPath: string;
  maxActiveNodes: number;
  occurredAt: string;
}

export interface DagMutationGuardV1 {
  runId: string;
  runNonce: string;
  expectedRevision: number;
  expectedSnapshotHash: string;
  ownerEpoch: number;
  commandId: string;
  idempotencyKey: string;
  occurredAt: string;
}

interface LoadedRunV1 {
  binding: DagSessionRunBindingV1;
  plan: CanonicalDagPlanV1;
  context: DagRunValidationContextV1;
  store: DagRunSnapshotStoreV1;
  state: DagRunStateV1;
}

export class DagConductorServiceV1 {
  readonly workerProjection?: (bindings: Array<{ workerStorageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; resultHash: string | null; processDisposition: string }>) => Promise<DagWorkerProjectionInputV1 | null>;
  readonly dispatchEffect?: (effect: { effectId: string; kind: string; requestHash: string }, state: DagRunStateV1) => Promise<void>;
  #currentLock = new Map<string, DagRunStoreLockIdentityV1>();
  #lastGood = new Map<string, { state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV1; cachedAt: string }>();
  constructor(options: { workerProjection?: (bindings: Array<{ workerStorageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; resultHash: string | null; processDisposition: string }>) => Promise<DagWorkerProjectionInputV1 | null>; dispatchEffect?: (effect: { effectId: string; kind: string; requestHash: string }, state: DagRunStateV1) => Promise<void> } = {}) { this.workerProjection = options.workerProjection; this.dispatchEffect = options.dispatchEffect; }

  async start(ctx: DagConductorContextV1, input: DagRunStartInputV1): Promise<{ binding: DagSessionRunBindingV1; state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    if (!ID_RE.test(input.runId) || input.runNonce.length < 16 || !HASH_RE.test(input.planHash) || !Number.isInteger(input.maxActiveNodes) || input.maxActiveNodes < 1) throw new Error("Invalid exact DAG run start identity or maxActiveNodes");
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId());
    const planPath = resolveBoundPath(repositoryRoot, input.planPath); const genesisPath = resolveBoundPath(repositoryRoot, input.genesisPath); const contextPath = resolveBoundPath(repositoryRoot, input.contextPath);
    const plan = parseCanonicalDagPlanV1(await readFile(planPath, "utf8"));
    if (plan.planHash !== input.planHash) throw new Error("Plan artifact does not match requested plan hash");
    const contextArtifact = parseStrictJson(await readFile(contextPath, "utf8")) as any;
    const seedFacts = Array.isArray(contextArtifact.seedFacts) ? contextArtifact.seedFacts : Object.values(contextArtifact.facts ?? {});
    delete contextArtifact.seedFacts;
    const context = contextArtifact as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan);
    const genesis = parseDagRunStateV1(await readFile(genesisPath, "utf8"), { ...context, plan, normalizedSchedulerIndexHash: index.indexHash });
    if (genesis.runId !== input.runId || genesis.runNonce !== input.runNonce || genesis.identity.planHash !== plan.planHash || genesis.scheduler.maxActiveNodes !== input.maxActiveNodes || genesis.scheduler.policyHash !== DAG_SCHEDULER_POLICY_HASH_V1 || genesis.scheduler.normalizedIndexHash !== index.indexHash) throw new Error("Genesis does not bind exact start authorization, scheduler policy, or explicit maxActiveNodes");
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), input.runId);
    for (const fact of seedFacts) await store.putImmutableFact(fact);
    await persistRunAuthority(store, plan, { ...context, plan, normalizedSchedulerIndexHash: index.indexHash });
    const initializationLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-initialize", sessionId, runId: input.runId }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId: input.runId, nonce: randomUUID() }), input.occurredAt);
    await store.initialize(genesis, { ...context, plan, normalizedSchedulerIndexHash: index.indexHash }, initializationLock);

    const existingBinding = await this.#readBinding(repositoryRoot, sessionId);
    if (existingBinding && (existingBinding.runId !== input.runId || existingBinding.runNonceHash !== canonicalHash(input.runNonce) || existingBinding.planHash !== plan.planHash)) throw new Error("Session already has a different exact DAG run binding");
    let state = await store.read({ ...context, plan, normalizedSchedulerIndexHash: index.indexHash });
    let effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    if (state.owner.sessionId === null) {
      const ownershipCore = {
        kind: "ownership" as const, runId: state.runId, runNonce: state.runNonce,
        priorSessionId: null, priorOwnerTokenHash: null, priorPid: 0, priorProcessStartIdentity: null, priorLockIdentity: null, priorAttachedAt: null,
        disposition: "absent" as const, priorObservationHash: null, successorSessionId: sessionId, successorPid: process.pid,
        successorProcessStartIdentity: initializationLock.processStartIdentity, successorLockIdentity: initializationLock.lockIdentity, lineageHash: null,
      };
      const ownership = { ...ownershipCore, hash: canonicalHash(ownershipCore) };
      await store.putImmutableFact(ownership);
      effectiveContext = { ...effectiveContext, facts: { ...effectiveContext.facts, [ownership.hash]: ownership } };
      const payload = { ownerTokenHash: initializationLock.ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: initializationLock.processStartIdentity, lockIdentity: initializationLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "absent" };
      const result = await store.mutate({ input: reducerInput(state, "attach_owner", "observation", payload, input.occurredAt, { commandId: `attach-${input.runId}`, idempotencyKey: `attach:${input.runId}:0` }), context: effectiveContext, lock: initializationLock });
      if (!result.accepted) throw new Error(`DAG owner attach rejected: ${result.code}: ${result.message}`);
      state = result.state;
      this.#currentLock.set(input.runId, initializationLock);
    } else if (state.owner.sessionId === sessionId && state.owner.pid === process.pid && state.owner.processStartIdentity === initializationLock.processStartIdentity) {
      const lock = dagRunStoreLockIdentityFromOwner(state.owner); this.#currentLock.set(input.runId, lock);
    } else throw new Error("Existing DAG run owner requires explicit reattachment");

    const binding = existingBinding ?? await this.#createBinding(ctx, repositoryRoot, plan, state, input.occurredAt);
    const advanced = await this.advance(ctx, state.runId, input.occurredAt);
    return { binding, state: advanced.state, decision: advanced.decision };
  }

  async advance(ctx: DagConductorContextV1, runId: string, occurredAt = new Date().toISOString()): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    const loaded = await this.#loadBound(ctx, runId); const lock = this.#currentLock.get(runId) ?? dagRunStoreLockIdentityFromOwner(loaded.state.owner); let state = await loaded.store.read(loaded.context);
    const dispatchOutstanding = async () => { for (const current of Object.values(state.scheduler.reservations).filter(({ state: reservationState }) => ["reserved", "dispatch_intent"].includes(reservationState)).sort((a, b) => a.reservationSequence - b.reservationSequence)) {
      let reservation = current;
      if (reservation.state === "reserved") { const marked = await loaded.store.mutate({ input: reducerInput(state, "mark_scheduler_reservation_dispatch", "command", { reservationId: reservation.reservationId, normalizedRequestHash: reservation.normalizedRequestHash }, occurredAt, { commandId: `dispatch-${reservation.reservationId}`, idempotencyKey: `dispatch:${state.runNonce}:${reservation.reservationId}` }), context: loaded.context, lock }); if (!marked.accepted) throw new Error(`Scheduler dispatch intent rejected: ${marked.code}: ${marked.message}`); state = marked.state; reservation = state.scheduler.reservations[reservation.reservationId]; }
      let disposition: "active" | "launch_ambiguous" = "active"; try { await this.dispatchEffect?.({ effectId: reservation.reservationId, kind: `scheduler_${reservation.operationKind}`, requestHash: reservation.normalizedRequestHash }, state); } catch { disposition = "launch_ambiguous"; }
      const observed = await loaded.store.mutate({ input: reducerInput(state, "record_scheduler_reservation_dispatch", "observation", { reservationId: reservation.reservationId, normalizedRequestHash: reservation.normalizedRequestHash, disposition }, occurredAt, { commandId: `dispatch-observe-${reservation.reservationId}-${disposition}`, idempotencyKey: `dispatch-observe:${state.runNonce}:${reservation.reservationId}` }), context: loaded.context, lock }); if (!observed.accepted) throw new Error(`Scheduler dispatch observation rejected: ${observed.code}: ${observed.message}`); state = observed.state;
    } };
    await dispatchOutstanding(); let decision = scheduleDagRunV1(loaded.plan, state); if (!decision.selected.length) return { state, decision };
    const payload = { decisionHash: decision.decisionHash, decisionSequence: decision.decisionSequence, policyHash: decision.policyHash, normalizedIndexHash: decision.normalizedIndexHash, inputSnapshotHash: state.snapshotHash, reservations: decision.selected, bypassSlotIds: decision.bypassIncrements };
    const result = await loaded.store.mutate({ input: reducerInput(state, "reserve_scheduler_batch", "command", payload, occurredAt, { commandId: `scheduler-${decision.decisionSequence}-${decision.decisionHash.slice(7, 19)}`, idempotencyKey: `scheduler:${state.runNonce}:${decision.decisionSequence}` }), context: loaded.context, lock });
    if (!result.accepted) throw new Error(`Scheduler reservation rejected: ${result.code}: ${result.message}`); state = result.state; await dispatchOutstanding(); decision = scheduleDagRunV1(loaded.plan, state); return { state, decision };
  }

  async status(ctx: DagConductorContextV1, runId: string): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV1; stale: null | { sourceRevision: number; sourceSnapshotHash: string; newerObservedRevision: number; cachedAt: string } }> {
    const loaded = await this.#loadBound(ctx, runId); const cacheKey = canonicalHash({ repositoryRootHash: loaded.binding.repositoryRootHash, sessionId: loaded.binding.sessionId, bindingHash: loaded.binding.bindingHash, runId: loaded.state.runId, runNonceHash: loaded.binding.runNonceHash, planHash: loaded.plan.planHash }); let newerObservedRevision = loaded.state.revision;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const first = await loaded.store.read(loaded.context);
      const exactBindings = Object.values(first.workerBindings).map(({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash, processDisposition }) => ({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash, processDisposition })).sort((a, b) => a.workerStorageId.localeCompare(b.workerStorageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber);
      const workers = await this.workerProjection?.(exactBindings) ?? null;
      const decision = scheduleDagRunV1(loaded.plan, first);
      const projection = projectDagExecutionV1(loaded.plan, first, decision, workers);
      const second = await loaded.store.read(loaded.context); newerObservedRevision = Math.max(newerObservedRevision, first.revision, second.revision);
      if (first.revision === second.revision && first.snapshotHash === second.snapshotHash) { const cachedAt = new Date().toISOString(); this.#lastGood.set(cacheKey, { state: first, decision, projection, cachedAt }); return { state: first, decision, projection, stale: null }; }
    }
    const lastGood = this.#lastGood.get(cacheKey);
    if (lastGood) return { state: lastGood.state, decision: lastGood.decision, projection: lastGood.projection, stale: { sourceRevision: lastGood.state.revision, sourceSnapshotHash: lastGood.state.snapshotHash, newerObservedRevision, cachedAt: lastGood.cachedAt } };
    throw new Error("DAG execution projection did not stabilize after three exact joins");
  }

  async inspect(ctx: DagConductorContextV1, runId: string, subjectId: string | null): Promise<unknown> {
    const { state, decision, projection } = await this.status(ctx, runId);
    if (!subjectId) return { state, decision, projection };
    const node = projection.nodes.find(({ alias, workItemId }) => alias === subjectId || workItemId === subjectId);
    if (node) return { node, workItem: state.workItems[node.workItemId], planWorkItem: (await this.#loadBound(ctx, runId)).plan.workItems.find(({ workItemId }) => workItemId === node.workItemId) };
    return state.stageAttempts[subjectId] ?? state.integrationAttempts[subjectId] ?? state.scheduler.reservations[subjectId] ?? state.effects[subjectId] ?? state.blockers[subjectId] ?? state.quarantine[subjectId] ?? null;
  }

  async tail(ctx: DagConductorContextV1, runId: string, limit: number, beforeRevision: number | null): Promise<{ snapshots: Array<{ revision: number; snapshotHash: string; updatedAt: string; current: string }>; nextBeforeRevision: number | null }> {
    const loaded = await this.#loadBound(ctx, runId); const names = await readdir(loaded.store.snapshotsDirectory);
    const snapshots: DagRunStateV1[] = [];
    for (const name of names.filter((value) => /^[0-9a-f]{64}\.json$/.test(value))) {
      const value = parseStrictJson(await readFile(join(loaded.store.snapshotsDirectory, name), "utf8")) as unknown as DagRunStateV1;
      if (value.runId === runId && (beforeRevision === null || value.revision < beforeRevision)) snapshots.push(value);
    }
    const page = snapshots.sort((a, b) => b.revision - a.revision || a.snapshotHash.localeCompare(b.snapshotHash)).slice(0, Math.max(1, Math.min(100, limit)));
    return { snapshots: page.map(({ revision, snapshotHash, updatedAt, current }) => ({ revision, snapshotHash, updatedAt, current: current.run })), nextBeforeRevision: page.length === Math.max(1, Math.min(100, limit)) ? page.at(-1)!.revision : null };
  }

  async control(ctx: DagConductorContextV1, guard: DagMutationGuardV1, action: "pause" | "resume" | "cancel", reason: string): Promise<DagRunStateV1> {
    const loaded = await this.#loadGuarded(ctx, guard); const lock = await this.#currentOwnerLock(loaded);
    let input: DagRunInputV1;
    if (action === "cancel") {
      const workItemIds = Object.values(loaded.state.workItems).filter(({ current }) => !["complete", "cancelled", "superseded"].includes(current)).map(({ workItemId }) => workItemId).sort();
      const effects = Object.values(loaded.state.stageAttempts).filter((attempt) => workItemIds.includes(attempt.workItemId) && attempt.producerKind === "owned_worker" && !attempt.terminalAt && Boolean(loaded.state.workerBindings[attempt.stageAttemptId])).map((attempt) => {
        const binding = loaded.state.workerBindings[attempt.stageAttemptId]; const fencedGeneration = loaded.state.workItems[attempt.workItemId].candidateGeneration + 1;
        const requestHash = canonicalHash({ kind: "cancel_worker", runId: loaded.state.runId, runNonce: loaded.state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, fencedGeneration });
        return { effectId: `cancel-${canonicalHash({ commandId: guard.commandId, stageAttemptId: attempt.stageAttemptId }).slice(7, 31)}`, kind: "cancel_worker", subject: { kind: "work_item", id: attempt.workItemId }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash, boundOwnerEpoch: loaded.state.owner.ownerEpoch, boundAuthorizationSetHash: loaded.state.identity.authorizationSet.hash, boundFreshnessReceiptHash: loaded.state.freshness.receipt.hash, boundCandidateGeneration: fencedGeneration, boundGateEpochHash: canonicalHash(loaded.state.workItems[attempt.workItemId].gateIds.map((id) => loaded.state.gates[id])), state: "intended", dispatchCount: 0, createdRevision: loaded.state.revision + 1, createdAt: guard.occurredAt, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
      }).sort((a, b) => a.effectId.localeCompare(b.effectId));
      const payload = { cancellationId: `cancel-${guard.commandId}`, scope: "run", subjectId: loaded.state.runId, reason, workItemIds, effects };
      input = reducerInput(loaded.state, "request_cancellation", "command", payload, guard.occurredAt, guard);
    } else {
      const payload = { desired: action === "pause" ? "paused" : "running", reason, requestedBy: "user" };
      input = reducerInput(loaded.state, "set_desired_run", "command", payload, guard.occurredAt, guard);
    }
    const result = await loaded.store.mutate({ input, context: loaded.context, lock });
    if (!result.accepted) throw new Error(`DAG control rejected: ${result.code}: ${result.message}`);
    let state = result.state;
    if (action === "cancel" && this.dispatchEffect) for (const effectId of result.state.cancellations[`cancel-${guard.commandId}`].effectIds) {
      const effect = state.effects[effectId];
      const markPayload = { effectId, expectedDispatchCount: effect.dispatchCount };
      const marked = await loaded.store.mutate({ input: reducerInput(state, "mark_effect_dispatching", "command", markPayload, guard.occurredAt, { commandId: `${guard.commandId}-dispatch-${effectId}`, idempotencyKey: `${guard.idempotencyKey}:dispatch:${effectId}` }), context: loaded.context, lock });
      if (!marked.accepted) throw new Error(`DAG cancellation dispatch rejected: ${marked.code}: ${marked.message}`);
      state = marked.state; await this.dispatchEffect({ effectId, kind: effect.kind, requestHash: effect.requestHash }, state);
    }
    return state;
  }

  async retry(ctx: DagConductorContextV1, guard: DagMutationGuardV1, payload: { retryKey: string; expectedCount: number; workItemId: string; stage: string; dimension: string; fingerprint: string; candidateGeneration: number }): Promise<DagRunStateV1> {
    const loaded = await this.#loadGuarded(ctx, guard); const lock = await this.#currentOwnerLock(loaded);
    const input = reducerInput(loaded.state, "authorize_retry", "command", payload, guard.occurredAt, guard);
    const result = await loaded.store.mutate({ input, context: loaded.context, lock });
    if (!result.accepted) throw new Error(`DAG retry rejected: ${result.code}: ${result.message}`);
    return result.state;
  }

  async reattach(ctx: DagConductorContextV1, guard: DagMutationGuardV1): Promise<DagRunStateV1> {
    const loaded = await this.#loadForReattach(ctx, guard.runId);
    if (loaded.state.runNonce !== guard.runNonce || loaded.state.revision !== guard.expectedRevision || loaded.state.snapshotHash !== guard.expectedSnapshotHash || loaded.state.owner.ownerEpoch !== guard.ownerEpoch) throw new Error("DAG reattach guard is stale");
    if (!loaded.state.owner.sessionId || !loaded.state.owner.lockIdentity) throw new Error("Detached epoch-zero run uses start/attach, not dead-owner reattach");
    const priorLock = dagRunStoreLockIdentityFromOwner(loaded.state.owner); const proof = await createDagRunStoreDeadOwnerProofV1(priorLock, guard.occurredAt);
    const sessionId = String(ctx.sessionManager.getSessionId());
    const newLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-reattach", sessionId, runId: guard.runId, ownerEpoch: guard.ownerEpoch + 1 }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId: guard.runId, nonce: randomUUID() }), guard.occurredAt);
    const ownershipCore = {
      kind: "ownership" as const, runId: loaded.state.runId, runNonce: loaded.state.runNonce,
      priorSessionId: loaded.state.owner.sessionId, priorOwnerTokenHash: loaded.state.owner.ownerTokenHash, priorPid: loaded.state.owner.pid,
      priorProcessStartIdentity: loaded.state.owner.processStartIdentity, priorLockIdentity: loaded.state.owner.lockIdentity, priorAttachedAt: loaded.state.owner.attachedAt,
      disposition: "dead" as const, priorObservationHash: proof.observationHash, successorSessionId: sessionId, successorPid: process.pid,
      successorProcessStartIdentity: newLock.processStartIdentity, successorLockIdentity: newLock.lockIdentity, lineageHash: null,
    };
    const ownership = { ...ownershipCore, hash: canonicalHash(ownershipCore) }; await loaded.store.putImmutableFact(ownership);
    const context = { ...loaded.context, facts: { ...loaded.context.facts, [ownership.hash]: ownership } };
    const payload = { ownerTokenHash: newLock.ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: newLock.processStartIdentity, lockIdentity: newLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "dead" };
    const input = reducerInput(loaded.state, "attach_owner", "observation", payload, guard.occurredAt, guard);
    const recovery = await loaded.store.reattachAfterDeadOwner(proof, input, context, newLock, async (candidate, lock) => candidate.expectedLockMetadataHash === canonicalHash(lock) && candidate.observationHash === proof.observationHash);
    if (!recovery.result.accepted) throw new Error(`DAG reattach rejected: ${recovery.result.code}: ${recovery.result.message}`);
    this.#currentLock.set(guard.runId, newLock);
    if (!await this.binding(ctx)) { const repositoryRoot = await realpath(ctx.cwd); const priorBinding = await this.#readBinding(repositoryRoot, loaded.state.owner.sessionId); await this.#createBinding(ctx, repositoryRoot, loaded.plan, recovery.result.state, guard.occurredAt, { kind: "explicit_reattach", priorBindingHash: priorBinding?.bindingHash ?? null, priorSessionId: loaded.state.owner.sessionId, proofHash: ownership.hash }); }
    return recovery.result.state;
  }

  async binding(ctx: DagConductorContextV1): Promise<DagSessionRunBindingV1 | null> { return this.#readBinding(await realpath(ctx.cwd), String(ctx.sessionManager.getSessionId())); }

  async #loadForReattach(ctx: DagConductorContextV1, runId: string): Promise<LoadedRunV1> {
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId());
    const binding = await this.#readBinding(repositoryRoot, sessionId);
    if (binding) return this.#loadBound(ctx, runId);
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), runId);
    const plan = parseCanonicalDagPlanV1(await readFile(join(store.runDirectory, "authority", "plan.json"), "utf8"));
    const context = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")) as unknown as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan); const effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    const state = await store.read(effectiveContext); const git = await repositoryGitBinding(repositoryRoot);
    const placeholderCore = { schemaVersion: 1 as const, kind: "DagSessionRunBindingV1" as const, sessionId, sessionFileHash: null, repositoryRootHash: canonicalHash(repositoryRoot), commonDirIdentityHash: git.commonDirIdentityHash, branchRef: git.branchRef, runId, runNonceHash: canonicalHash(state.runNonce), planHash: plan.planHash, storeRoot: RUN_ROOT, boundAt: state.createdAt };
    const placeholder = { ...placeholderCore, bindingHash: canonicalHash(placeholderCore) };
    return { binding: placeholder, plan, context: effectiveContext, store, state };
  }

  async #loadGuarded(ctx: DagConductorContextV1, guard: DagMutationGuardV1): Promise<LoadedRunV1> {
    const loaded = await this.#loadBound(ctx, guard.runId);
    if (loaded.state.runNonce !== guard.runNonce || loaded.state.revision !== guard.expectedRevision || loaded.state.snapshotHash !== guard.expectedSnapshotHash || loaded.state.owner.ownerEpoch !== guard.ownerEpoch) throw new Error("DAG mutation guard is stale");
    return loaded;
  }

  async #loadBound(ctx: DagConductorContextV1, runId: string): Promise<LoadedRunV1> {
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId()); const binding = await this.#readBinding(repositoryRoot, sessionId);
    if (!binding || binding.runId !== runId) throw new Error("No exact current-session binding exists for the requested DAG run");
    await validateBindingRepository(binding, repositoryRoot);
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, binding.storeRoot), runId);
    const plan = parseCanonicalDagPlanV1(await readFile(join(store.runDirectory, "authority", "plan.json"), "utf8"));
    const context = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")) as unknown as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan); const effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    const state = await store.read(effectiveContext);
    if ((state.runNonce && canonicalHash(state.runNonce) !== binding.runNonceHash) || state.identity.planHash !== binding.planHash || state.owner.ownerEpoch !== binding.ownerEpoch || state.owner.ownershipReceipt !== binding.ownershipReceiptHash || state.owner.sessionId !== binding.sessionId) throw new Error("Session binding conflicts with run authority");
    return { binding, plan, context: effectiveContext, store, state };
  }

  async #currentOwnerLock(loaded: LoadedRunV1): Promise<DagRunStoreLockIdentityV1> {
    const current = this.#currentLock.get(loaded.state.runId) ?? dagRunStoreLockIdentityFromOwner(loaded.state.owner);
    const start = await currentProcessStartIdentity();
    if (current.pid !== process.pid || current.processStartIdentity !== start) throw new Error("Current process does not own the exact DAG conductor epoch; use dag_run_reattach");
    this.#currentLock.set(loaded.state.runId, current); return current;
  }

  async #createBinding(ctx: DagConductorContextV1, repositoryRoot: string, plan: CanonicalDagPlanV1, state: DagRunStateV1, at: string, lineage: DagSessionRunBindingV1["lineage"] = { kind: "start", priorBindingHash: null, priorSessionId: null, proofHash: null }): Promise<DagSessionRunBindingV1> {
    const sessionId = String(ctx.sessionManager.getSessionId()); const sessionFile = ctx.sessionManager.getSessionFile?.() ?? null; const git = await repositoryGitBinding(repositoryRoot);
    const core = { schemaVersion: 1 as const, kind: "DagSessionRunBindingV1" as const, sessionId, sessionFileHash: sessionFile ? canonicalHash(await realpath(sessionFile).catch(() => sessionFile)) : null, repositoryRootHash: canonicalHash(repositoryRoot), commonDirIdentityHash: git.commonDirIdentityHash, branchRef: git.branchRef, runId: state.runId, runNonceHash: canonicalHash(state.runNonce), planHash: plan.planHash, ownerEpoch: state.owner.ownerEpoch, ownershipReceiptHash: state.owner.ownershipReceipt!, lineage, storeRoot: RUN_ROOT, boundAt: at };
    const binding = { ...core, bindingHash: canonicalHash(core) };
    await publishImmutableJson(bindingPath(repositoryRoot, sessionId), binding); await new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), state.runId).putImmutableFact(binding); return binding;
  }

  async #readBinding(repositoryRoot: string, sessionId: string): Promise<DagSessionRunBindingV1 | null> {
    try {
      const value = parseStrictJson(await readFile(bindingPath(repositoryRoot, sessionId), "utf8")) as unknown as DagSessionRunBindingV1;
      const core = { ...value } as any; delete core.bindingHash;
      if (value.schemaVersion !== 1 || value.kind !== "DagSessionRunBindingV1" || value.sessionId !== sessionId || !Number.isInteger(value.ownerEpoch) || !HASH_RE.test(value.ownershipReceiptHash) || !value.lineage || value.bindingHash !== canonicalHash(core)) throw new Error("DAG session binding is corrupt");
      return value;
    } catch (error: any) { if (error?.code === "ENOENT") return null; throw error; }
  }
}

function reducerInput(state: DagRunStateV1, type: any, kind: "command" | "observation", payload: any, occurredAt: string, overrides: Partial<DagMutationGuardV1>): DagRunInputV1 {
  return {
    schemaVersion: 1, kind, type, commandId: overrides.commandId ?? `command-${type}`, idempotencyKey: overrides.idempotencyKey ?? `${type}:${state.revision}`,
    payloadHash: canonicalHash(payload), runId: state.runId, runNonce: state.runNonce,
    expectedRevision: overrides.expectedRevision ?? state.revision, expectedSnapshotHash: overrides.expectedSnapshotHash ?? state.snapshotHash,
    ownerEpoch: overrides.ownerEpoch ?? state.owner.ownerEpoch, occurredAt, payload,
  } as DagRunInputV1;
}

async function persistRunAuthority(store: DagRunSnapshotStoreV1, plan: CanonicalDagPlanV1, context: DagRunValidationContextV1): Promise<void> {
  const directory = join(store.runDirectory, "authority"); await mkdir(directory, { recursive: true });
  await publishOrReplaceCanonical(join(directory, "plan.json"), plan, true);
  await publishOrReplaceCanonical(join(directory, "context.json"), context, true);
}

async function validateBindingRepository(binding: DagSessionRunBindingV1, repositoryRoot: string): Promise<void> {
  if (binding.repositoryRootHash !== canonicalHash(repositoryRoot)) throw new Error("DAG session binding repository root changed");
  const git = await repositoryGitBinding(repositoryRoot);
  if (git.commonDirIdentityHash !== binding.commonDirIdentityHash || git.branchRef !== binding.branchRef) throw new Error("DAG session binding common-dir or branch identity changed");
}

async function repositoryGitBinding(repositoryRoot: string): Promise<{ commonDirIdentityHash: string; branchRef: string }> {
  const { execFile } = await import("node:child_process"); const { promisify } = await import("node:util"); const run = promisify(execFile);
  const env = { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
  const commonRaw = (await run("git", ["rev-parse", "--path-format=absolute", "--git-common-dir"], { cwd: repositoryRoot, env, encoding: "utf8" })).stdout.trim();
  const common = await realpath(commonRaw); const info = await stat(common);
  const branchRef = (await run("git", ["symbolic-ref", "-q", "HEAD"], { cwd: repositoryRoot, env, encoding: "utf8" })).stdout.trim();
  if (!branchRef.startsWith("refs/heads/")) throw new Error("DAG session must be bound to one direct branch worktree");
  return { commonDirIdentityHash: canonicalHash({ pathHash: canonicalHash(common), dev: String(info.dev), ino: String(info.ino) }), branchRef };
}

async function processLockIdentity(sessionId: string, lockIdentity: string, ownerTokenHash: string, acquiredAt: string): Promise<DagRunStoreLockIdentityV1> { return { lockIdentity, ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: await currentProcessStartIdentity(), acquiredAt }; }
async function currentProcessStartIdentity(): Promise<string> { if (process.platform !== "linux") return `process:${process.pid}:${process.uptime().toFixed(3)}`; const text = await readFile(`/proc/${process.pid}/stat`, "utf8"); return `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`; }
function bindingPath(root: string, sessionId: string): string { return join(root, BINDING_ROOT, `${createHash("sha256").update(sessionId).digest("hex")}.json`); }
function resolveBoundPath(root: string, value: string): string { if (!value || value.startsWith("/") || value.includes("\\") || value.split("/").includes("..")) throw new Error("DAG artifact path must be root-relative without traversal"); const path = resolve(root, value); if (path === root || !path.startsWith(`${root}/`)) throw new Error("DAG artifact path escapes repository root"); return path; }

async function publishImmutableJson(path: string, value: unknown): Promise<void> {
  const text = canonicalStringify(value); await mkdir(dirname(path), { recursive: true });
  try { if (await readFile(path, "utf8") === text) return; throw new Error("Immutable DAG binding conflicts with existing bytes"); } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  const temp = `${path}.${process.pid}.${randomUUID()}.tmp`; const handle = await open(temp, "wx"); try { await handle.writeFile(text); await handle.sync(); } finally { await handle.close(); }
  try { await link(temp, path); } catch (error: any) { if (error?.code !== "EEXIST" || await readFile(path, "utf8") !== text) throw error; } finally { await rm(temp, { force: true }); }
  await syncDirectory(dirname(path));
}

async function publishOrReplaceCanonical(path: string, value: unknown, immutable: boolean): Promise<void> {
  const text = canonicalStringify(value); await mkdir(dirname(path), { recursive: true });
  if (immutable) return publishImmutableJson(path, value);
  const temp = `${path}.${process.pid}.${randomUUID()}.tmp`; const handle = await open(temp, "wx"); try { await handle.writeFile(text); await handle.sync(); } finally { await handle.close(); }
  await rename(temp, path); await syncDirectory(dirname(path));
}
async function syncDirectory(path: string): Promise<void> { const handle = await open(path, "r"); try { await handle.sync(); } finally { await handle.close(); } }
