import { createHash, randomUUID } from "node:crypto";
import { closeSync, fsyncSync, linkSync, mkdirSync, openSync, readFileSync, realpathSync, unlinkSync, writeFileSync } from "node:fs";
import { link, mkdir, open, readFile, readdir, realpath, rename, rm, stat } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { canonicalHash, canonicalStringify, parseStrictJson } from "./common.ts";
import { assertBoundedDagReadyPacketV1, DagLifecycleRuntimeV1, normalizeDagTacticalDirectiveV1, type DagIntegrationReconciliationAdapterV1, type DagLifecycleRuntimeOptionsV1, type DagOwnedWorkerDispatchResultV1, type DagOwnedWorkerReadyPacketV1 } from "./lifecycle-runtime.ts";
import { parseCanonicalDagPlanV1, type CanonicalDagPlanV1 } from "./plan.ts";
import { type DagRunInputV1 } from "./reducer.ts";
import { ownershipChainHashV1, parseDagRunStateV1, type DagRunStateV1, type DagRunValidationContextV1 } from "./run-state.ts";
import { buildSchedulerPlanIndexV1, DAG_SCHEDULER_POLICY_HASH_V1, projectDagExecutionV2, scheduleDagRunV1, type DagExecutionProjectionV2, type DagSchedulerDecisionV1, type DagWorkerProjectionInputV1 } from "./scheduler.ts";
import { DagRunSnapshotStoreV1, createDagRunStoreDeadOwnerProofV1, dagRunStoreLockIdentityFromOwner, type DagRunStoreLockIdentityV1 } from "./store.ts";

const RUN_ROOT = ".ai/dag-runs-v1";
const BINDING_ROOT = ".ai/dag-session-bindings-v1";
const START_INTENT_ROOT = ".ai/dag-start-intents-v1";
const CONDUCTOR_FAULT_ROOT = ".ai/dag-conductor-faults-v1";
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
  lineage: { kind: "start" | "direct_fork" | "explicit_reattach" | "same_manager_resume"; priorBindingHash: string | null; priorSessionId: string | null; proofHash: string | null };
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

export interface DagPreparedRunStartInputV1 {
  runId: string;
  runNonce: string;
  planHash: string;
  maxActiveNodes: number;
  occurredAt: string;
  plan: CanonicalDagPlanV1;
  genesis: DagRunStateV1;
  context: DagRunValidationContextV1;
  seedFacts: unknown[];
  sourcePlanningPlanId?: string | null;
  sourcePlanningPlanHash?: string | null;
}

export type DagPreparedStartFailpointV1 = "after_start_intent" | "after_run_authority" | "before_genesis_initialize" | "after_genesis_initialize" | "before_owner_attach" | "after_owner_attach" | "before_final_binding" | "after_final_binding" | "after_start_active" | "before_response";
export type DagOwnerResumeFailpointV1 = "after_owner_transfer" | "after_owner_binding" | "after_owner_start_identity";
export type DagConductorPumpFailpointV1 = "after_quiescent_check";

interface DagRunStartIntentV1 {
  schemaVersion: 1;
  kind: "DagRunStartIntentV1";
  startId: string;
  state: "starting" | "active";
  revision: number;
  sessionId: string;
  runId: string;
  runNonce: string;
  planHash: string;
  preparedHash: string;
  sourcePlanningPlanId: string | null;
  sourcePlanningPlanHash: string | null;
  startedAt: string;
  bindingHash: string | null;
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

export type DagSemanticOperationV1 = "start_work" | "run_checks" | "record_completion" | "integrate" | "retry" | "pause" | "resume" | "cancel" | "finalize";
export interface DagSemanticActionV1 {
  operation: DagSemanticOperationV1;
  actionId: string;
  runId: string;
  revision: number;
  snapshotHash: string;
  ownerEpoch: number;
  decisionHash: string;
  workItemId: string | null;
  stage: string | null;
  candidateGeneration: number | null;
  reservationId: string | null;
  reservationSequence: number | null;
  reservationState: string | null;
  stageAttemptId: string | null;
  completionId: string | null;
  retryKey: string | null;
  retryCount: number | null;
  finalizationKind: "worker_result" | "cleanup" | "cancellation" | null;
  explanation: string;
  mutexGroupIds: string[];
  concurrency: { activeLanes: number; maxActiveNodes: number; requiresRefreshAfterMutation: true };
}
export interface DagNextActionResultV1 {
  schemaVersion: 1;
  kind: "DagNextActionResultV1";
  runId: string;
  revision: number;
  snapshotHash: string;
  frontier: DagSemanticActionV1[];
  controls: DagSemanticActionV1[];
  waiting: boolean;
  notice: string;
}

interface LoadedRunV1 {
  binding: DagSessionRunBindingV1;
  plan: CanonicalDagPlanV1;
  context: DagRunValidationContextV1;
  store: DagRunSnapshotStoreV1;
  state: DagRunStateV1;
}

export class DagConductorServiceV1 {
  readonly workerProjection?: (bindings: Array<{ workerStorageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; resultHash: string | null }>) => Promise<DagWorkerProjectionInputV1 | null>;
  readonly dispatchEffect?: (effect: { effectId: string; kind: string; requestHash: string }, state: DagRunStateV1) => Promise<void>;
  readonly lifecycle: DagLifecycleRuntimeOptionsV1;
  readonly integrationFactory?: (input: { store: DagRunSnapshotStoreV1; context: DagRunValidationContextV1; lock: DagRunStoreLockIdentityV1 }) => DagIntegrationReconciliationAdapterV1;
  readonly startFailpoint?: (point: DagPreparedStartFailpointV1) => Promise<void> | void;
  readonly ownerResumeFailpoint?: (point: DagOwnerResumeFailpointV1) => Promise<void> | void;
  readonly pumpFailpoint?: (point: DagConductorPumpFailpointV1, detail?: { occurredAt: string; wakeGeneration: number }) => Promise<void> | void;
  readonly onPumpError?: (input: { runId: string; error: Error }) => Promise<void> | void;
  #currentLock = new Map<string, DagRunStoreLockIdentityV1>();
  #activeContexts = new Map<string, DagConductorContextV1>();
  #wakeGenerations = new Map<string, number>();
  #wakeTimes = new Map<string, string>();
  #pumps = new Map<string, Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }>>();
  #dispatches = new Map<string, { requestHash: string; promise: Promise<DagOwnedWorkerDispatchResultV1> }>();
  #faults = new Map<string, Error>();
  #semanticActionClaims = new Set<string>();
  #detaching = false;
  #detachPromise: Promise<void> | null = null;
  #lastGood = new Map<string, { state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV2; readyPackets: DagOwnedWorkerReadyPacketV1[]; cachedAt: string }>();
  constructor(options: { workerProjection?: (bindings: Array<{ workerStorageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; resultHash: string | null }>) => Promise<DagWorkerProjectionInputV1 | null>; dispatchEffect?: (effect: { effectId: string; kind: string; requestHash: string }, state: DagRunStateV1) => Promise<void>; lifecycle?: DagLifecycleRuntimeOptionsV1; integrationFactory?: (input: { store: DagRunSnapshotStoreV1; context: DagRunValidationContextV1; lock: DagRunStoreLockIdentityV1 }) => DagIntegrationReconciliationAdapterV1; startFailpoint?: (point: DagPreparedStartFailpointV1) => Promise<void> | void; ownerResumeFailpoint?: (point: DagOwnerResumeFailpointV1) => Promise<void> | void; pumpFailpoint?: (point: DagConductorPumpFailpointV1, detail?: { occurredAt: string; wakeGeneration: number }) => Promise<void> | void; onPumpError?: (input: { runId: string; error: Error }) => Promise<void> | void } = {}) { this.workerProjection = options.workerProjection; this.dispatchEffect = options.dispatchEffect; this.lifecycle = options.lifecycle ?? {}; this.integrationFactory = options.integrationFactory; this.startFailpoint = options.startFailpoint; this.ownerResumeFailpoint = options.ownerResumeFailpoint; this.pumpFailpoint = options.pumpFailpoint; this.onPumpError = options.onPumpError; }

  /** Drain any in-flight legacy pump or owned dispatch before releasing local operation caches. */
  detach(): Promise<void> {
    if (this.#detachPromise) return this.#detachPromise;
    this.#detaching = true;
    this.#activeContexts.clear();
    this.#detachPromise = (async () => {
      await Promise.allSettled([...this.#pumps.values(), ...[...this.#dispatches.values()].map(({ promise }) => promise)]);
      this.#wakeGenerations.clear(); this.#wakeTimes.clear(); this.#currentLock.clear(); this.#dispatches.clear(); this.#faults.clear(); this.#semanticActionClaims.clear();
    })();
    return this.#detachPromise;
  }

  /** Legacy explicit recovery API; normal orchestration uses named semantic operations instead. */
  async activate(ctx: DagConductorContextV1, runId: string, occurredAt = new Date().toISOString()): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    if (this.#detaching) throw new Error("DAG conductor service is detaching and cannot accept another wake");
    const fault = this.#faults.get(runId); if (fault) throw fault;
    if (!this.#pumps.has(runId) && this.#hasDurableFault(ctx, runId)) throw new Error(`Canonical DAG conductor ${runId} has a durable surfaced fault; diagnose it before explicit retry`);
    this.#activeContexts.set(runId, ctx);
    this.#wakeGenerations.set(runId, (this.#wakeGenerations.get(runId) ?? 0) + 1);
    this.#wakeTimes.set(runId, occurredAt);
    return this.#startPump(ctx, runId);
  }

  #startPump(ctx: DagConductorContextV1, runId: string): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    const current = this.#pumps.get(runId);
    if (current) return current;
    let observedWakeGeneration = this.#wakeGenerations.get(runId)!;
    let observedWakeTime = this.#wakeTimes.get(runId)!;
    const body = (async () => {
      observedWakeGeneration = this.#wakeGenerations.get(runId)!;
      observedWakeTime = this.#wakeTimes.get(runId)!;
      await this.#ensureOperationalOwner(ctx, runId, observedWakeTime);
      const result = await this.advance(ctx, runId, observedWakeTime);
      // Give terminal-result microtasks queued at the waiting boundary a chance
      // to mark this run dirty before this bounded pass settles. A newer wake
      // starts a successor pump but never extends this caller's promise.
      await Promise.resolve();
      if (this.#wakeGenerations.get(runId) === observedWakeGeneration) await this.pumpFailpoint?.("after_quiescent_check", { occurredAt: observedWakeTime, wakeGeneration: observedWakeGeneration });
      return { result, settledWakeGeneration: observedWakeGeneration };
    })();
    let pump!: Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }>;
    pump = body.then(
      ({ result, settledWakeGeneration }) => {
        if (this.#pumps.get(runId) === pump) this.#pumps.delete(runId);
        if (["completed", "cancelled", "superseded"].includes(result.state.current.run)) this.#activeContexts.delete(runId);
        if (!this.#detaching && this.#activeContexts.has(runId) && this.#wakeGenerations.get(runId) !== settledWakeGeneration) void this.#startPump(ctx, runId).catch(() => undefined);
        return result;
      },
      async (error) => {
        if (this.#pumps.get(runId) === pump) this.#pumps.delete(runId);
        const exact = error instanceof Error ? error : new Error(String(error));
        this.#activeContexts.delete(runId); this.#faults.set(runId, exact);
        try { this.#persistDurableFault(ctx, runId, exact, observedWakeTime); } catch { /* the first conductor error remains exact even if fault-marker I/O fails */ }
        try { void Promise.resolve(this.onPumpError?.({ runId, error: exact })).catch(() => undefined); } catch { /* error propagation must not be replaced by reporting failure */ }
        throw exact;
      },
    );
    this.#pumps.set(runId, pump);
    return pump;
  }

  #hasDurableFault(ctx: DagConductorContextV1, runId: string): boolean {
    try { readDurableConductorFault(conductorFaultPath(realpathSync(ctx.cwd), runId), runId); return true; }
    catch (error: any) { if (error?.code === "ENOENT") return false; throw error; }
  }

  #persistDurableFault(ctx: DagConductorContextV1, runId: string, error: Error, faultedAt: string): void {
    const core = { schemaVersion: 1, kind: "DagConductorFaultV1", runId, errorMessage: error.message, faultedAt };
    publishImmutableJsonSync(conductorFaultPath(realpathSync(ctx.cwd), runId), { ...core, hash: canonicalHash(core) });
  }

  #clearDurableFault(ctx: DagConductorContextV1, runId: string): void {
    const path = conductorFaultPath(realpathSync(ctx.cwd), runId);
    try { readDurableConductorFault(path, runId); unlinkSync(path); syncDirectorySync(dirname(path)); }
    catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  }

  /** Explicit top-level retry keeps the durable fault visible until the retry succeeds. */
  async retryActivation(ctx: DagConductorContextV1, runId: string, occurredAt = new Date().toISOString()): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    if (this.#detaching) throw new Error("DAG conductor service is detaching and cannot start an explicit retry");
    if (!this.#hasDurableFault(ctx, runId)) { this.#faults.delete(runId); return this.activate(ctx, runId, occurredAt); }
    this.#faults.delete(runId);
    this.#activeContexts.set(runId, ctx); this.#wakeGenerations.set(runId, (this.#wakeGenerations.get(runId) ?? 0) + 1); this.#wakeTimes.set(runId, occurredAt);
    const result = await this.#startPump(ctx, runId);
    this.#clearDurableFault(ctx, runId);
    return result;
  }

  /** Resume the exact bound run when a session/agent lifecycle event wakes the extension. */
  async resumeBound(ctx: DagConductorContextV1, occurredAt = new Date().toISOString()): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 } | null> {
    const binding = await this.binding(ctx);
    return binding && !this.#faults.has(binding.runId) && !this.#hasDurableFault(ctx, binding.runId) ? this.activate(ctx, binding.runId, occurredAt) : null;
  }

  /** Wake all runs attached to this exact service instance after asynchronous worker state changes. */
  async wakeActive(occurredAt = new Date().toISOString()): Promise<void> {
    await Promise.all([...this.#activeContexts.entries()].map(([runId, ctx]) => this.activate(ctx, runId, occurredAt).then(() => undefined)));
  }

  async start(ctx: DagConductorContextV1, input: DagRunStartInputV1): Promise<{ binding: DagSessionRunBindingV1; state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    if (!ID_RE.test(input.runId) || input.runNonce.length < 16 || !HASH_RE.test(input.planHash) || !Number.isInteger(input.maxActiveNodes) || input.maxActiveNodes < 1) throw new Error("Invalid exact DAG run start identity or maxActiveNodes");
    const repositoryRoot = await realpath(ctx.cwd);
    const planPath = resolveBoundPath(repositoryRoot, input.planPath); const genesisPath = resolveBoundPath(repositoryRoot, input.genesisPath); const contextPath = resolveBoundPath(repositoryRoot, input.contextPath);
    const plan = parseCanonicalDagPlanV1(await readFile(planPath, "utf8"));
    if (plan.planHash !== input.planHash) throw new Error("Plan artifact does not match requested plan hash");
    const contextArtifact = parseStrictJson(await readFile(contextPath, "utf8")) as any;
    const seedFacts = Array.isArray(contextArtifact.seedFacts) ? contextArtifact.seedFacts : Object.values(contextArtifact.facts ?? {});
    delete contextArtifact.seedFacts;
    const context = contextArtifact as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan);
    const genesis = parseDagRunStateV1(await readFile(genesisPath, "utf8"), { ...context, plan, normalizedSchedulerIndexHash: index.indexHash });
    return this.startPrepared(ctx, { runId: input.runId, runNonce: input.runNonce, planHash: input.planHash, maxActiveNodes: input.maxActiveNodes, occurredAt: input.occurredAt, plan, genesis, context, seedFacts });
  }

  async startPrepared(ctx: DagConductorContextV1, input: DagPreparedRunStartInputV1): Promise<{ binding: DagSessionRunBindingV1; state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    if (!ID_RE.test(input.runId) || input.runNonce.length < 16 || !HASH_RE.test(input.planHash) || !Number.isInteger(input.maxActiveNodes) || input.maxActiveNodes < 1) throw new Error("Invalid exact prepared DAG run start identity or maxActiveNodes");
    const sourcePlanningPlanId = input.sourcePlanningPlanId ?? null; const sourcePlanningPlanHash = input.sourcePlanningPlanHash ?? null;
    if ((sourcePlanningPlanId === null) !== (sourcePlanningPlanHash === null) || (sourcePlanningPlanId !== null && (!ID_RE.test(sourcePlanningPlanId) || !HASH_RE.test(sourcePlanningPlanHash!)))) throw new Error("Prepared DAG start source planning identity must provide one exact plan ID/hash pair");
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId());
    const plan = parseCanonicalDagPlanV1(canonicalStringify(input.plan));
    if (plan.planHash !== input.planHash) throw new Error("Prepared plan does not match requested plan hash");
    const context = parseStrictJson(canonicalStringify(input.context)) as unknown as DagRunValidationContextV1;
    const seedFacts = parseStrictJson(canonicalStringify(input.seedFacts)) as unknown[];
    if (!Array.isArray(seedFacts)) throw new Error("Prepared DAG start seedFacts must be an exact array");
    const index = buildSchedulerPlanIndexV1(plan); const effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    const genesis = parseDagRunStateV1(canonicalStringify(input.genesis), effectiveContext);
    if (genesis.runId !== input.runId || genesis.runNonce !== input.runNonce || genesis.identity.planHash !== plan.planHash || genesis.scheduler.maxActiveNodes !== input.maxActiveNodes || genesis.scheduler.policyHash !== DAG_SCHEDULER_POLICY_HASH_V1 || genesis.scheduler.normalizedIndexHash !== index.indexHash) throw new Error("Genesis does not bind exact prepared start authorization, scheduler policy, or explicit maxActiveNodes");

    let existingBinding = await this.#readBinding(repositoryRoot, sessionId);
    if (existingBinding && (existingBinding.runId !== input.runId || existingBinding.runNonceHash !== canonicalHash(input.runNonce) || existingBinding.planHash !== plan.planHash)) throw new Error("Session already has a different exact DAG run binding");
    const unfinished = await this.pendingStart(ctx);
    if (unfinished && unfinished.runId !== input.runId) throw new Error(`Session has unfinished DAG start ${unfinished.runId}; recover it before starting another run`);

    const preparedHash = canonicalHash({ runId: input.runId, runNonce: input.runNonce, planHash: input.planHash, maxActiveNodes: input.maxActiveNodes, occurredAt: input.occurredAt, plan, genesis, context, seedFacts, sourcePlanningPlanId, sourcePlanningPlanHash });
    const intentPath = startIntentPath(repositoryRoot, sessionId, input.runId);
    let intent = await readStartIntent(intentPath, sessionId, input.runId);
    if (!intent) {
      const candidate: DagRunStartIntentV1 = { schemaVersion: 1, kind: "DagRunStartIntentV1", startId: randomUUID(), state: "starting", revision: 0, sessionId, runId: input.runId, runNonce: input.runNonce, planHash: plan.planHash, preparedHash, sourcePlanningPlanId, sourcePlanningPlanHash, startedAt: input.occurredAt, bindingHash: null };
      try { await publishImmutableJson(intentPath, candidate); intent = candidate; }
      catch (error) { intent = await readStartIntent(intentPath, sessionId, input.runId); if (!intent) throw error; }
    }
    if (!intent) throw new Error("Prepared DAG start intent publication did not become durable");
    assertExactStartReplay(intent, { runNonce: input.runNonce, planHash: plan.planHash, preparedHash, sourcePlanningPlanId, sourcePlanningPlanHash });
    await this.startFailpoint?.("after_start_intent");

    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), input.runId);
    for (const fact of seedFacts) await store.putImmutableFact(fact);
    await persistRunAuthority(store, plan, effectiveContext);
    await this.startFailpoint?.("after_run_authority");
    const initializationLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-initialize", sessionId, runId: input.runId, startId: intent.startId }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId: input.runId, startId: intent.startId, nonce: randomUUID() }), input.occurredAt);
    await this.startFailpoint?.("before_genesis_initialize");
    let priorState: DagRunStateV1 | null = null;
    try { priorState = await store.read(effectiveContext); }
    catch (error: any) { if (!(error?.causeValue?.code === "ENOENT" || error?.cause?.code === "ENOENT" || error?.code === "ENOENT")) throw error; }
    if (!priorState || priorState.revision === 0) await store.initialize(genesis, effectiveContext, initializationLock);
    else if (priorState.runId !== genesis.runId || priorState.runNonce !== genesis.runNonce || priorState.identity.planHash !== genesis.identity.planHash) throw new Error("Durable prepared start intent conflicts with existing run authority");
    await this.startFailpoint?.("after_genesis_initialize");

    let state = priorState?.revision ? priorState : await store.read(effectiveContext); let runtimeContext = effectiveContext;
    await this.startFailpoint?.("before_owner_attach");
    if (state.owner.sessionId === null) {
      if (existingBinding) throw new Error("Session binding exists before prepared run owner authority");
      const ownershipCore = {
        kind: "ownership" as const, runId: state.runId, runNonce: state.runNonce,
        priorSessionId: null, priorOwnerTokenHash: null, priorPid: 0, priorProcessStartIdentity: null, priorLockIdentity: null, priorAttachedAt: null,
        disposition: "absent" as const, priorObservationHash: null, priorOwnershipReceiptHash: null, ownerEpoch: 1,
        successorSessionId: sessionId, successorPid: process.pid,
        successorProcessStartIdentity: initializationLock.processStartIdentity, successorLockIdentity: initializationLock.lockIdentity, lineageHash: null,
      };
      const ownershipWithChain = { ...ownershipCore, chainHash: ownershipChainHashV1(ownershipCore, null) };
      const ownership = { ...ownershipWithChain, hash: canonicalHash(ownershipWithChain) };
      await store.putImmutableFact(ownership);
      runtimeContext = { ...runtimeContext, facts: { ...runtimeContext.facts, [ownership.hash]: ownership } };
      const payload = { ownerTokenHash: initializationLock.ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: initializationLock.processStartIdentity, lockIdentity: initializationLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "absent" };
      const result = await store.mutate({ input: reducerInput(state, "attach_owner", "observation", payload, input.occurredAt, { commandId: `attach-${intent.startId}`, idempotencyKey: `attach:${intent.startId}:0` }), context: runtimeContext, lock: initializationLock });
      if (!result.accepted) throw new Error(`DAG owner attach rejected: ${result.code}: ${result.message}`);
      state = result.state; this.#currentLock.set(input.runId, initializationLock);
    } else if (state.owner.sessionId === sessionId && state.owner.pid === process.pid && state.owner.processStartIdentity === initializationLock.processStartIdentity) {
      if (existingBinding) await validateBindingAuthority(existingBinding, repositoryRoot, plan, state);
      const held = this.#currentLock.get(input.runId);
      if (held?.lockIdentity === state.owner.lockIdentity && held.ownerTokenHash === state.owner.ownerTokenHash) this.#currentLock.set(input.runId, held);
      else if (existingBinding) {
        await this.#ensureOperationalOwner(ctx, input.runId, input.occurredAt);
        state = await store.read(effectiveContext);
        existingBinding = await this.#readBinding(repositoryRoot, sessionId);
        intent = await readStartIntent(intentPath, sessionId, input.runId) ?? intent;
      } else throw new Error("Prepared-start recovery found same-process owner authority without its exact session binding");
    } else if (state.owner.sessionId === sessionId) {
      if (existingBinding) await validateBindingAuthority(existingBinding, repositoryRoot, plan, state);
      const priorLock = dagRunStoreLockIdentityFromOwner(state.owner); const proof = await createDagRunStoreDeadOwnerProofV1(priorLock, input.occurredAt);
      const newLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-start-recover", sessionId, runId: input.runId, startId: intent.startId, ownerEpoch: state.owner.ownerEpoch + 1 }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId: input.runId, startId: intent.startId, nonce: randomUUID() }), input.occurredAt);
      const priorOwnership = state.owner.ownershipReceipt ? await store.readImmutableFact(state.owner.ownershipReceipt) as any : null;
      const ownershipCore = {
        kind: "ownership" as const, runId: state.runId, runNonce: state.runNonce,
        priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, priorAttachedAt: state.owner.attachedAt,
        disposition: "dead" as const, priorObservationHash: proof.observationHash, priorOwnershipReceiptHash: state.owner.ownershipReceipt, ownerEpoch: state.owner.ownerEpoch + 1,
        successorSessionId: sessionId, successorPid: process.pid, successorProcessStartIdentity: newLock.processStartIdentity, successorLockIdentity: newLock.lockIdentity, lineageHash: null,
      };
      const ownershipWithChain = { ...ownershipCore, chainHash: ownershipChainHashV1(ownershipCore, priorOwnership?.kind === "ownership" ? priorOwnership.chainHash : null) };
      const ownership = { ...ownershipWithChain, hash: canonicalHash(ownershipWithChain) }; await store.putImmutableFact(ownership);
      runtimeContext = { ...runtimeContext, facts: { ...runtimeContext.facts, [ownership.hash]: ownership } };
      const payload = { ownerTokenHash: newLock.ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: newLock.processStartIdentity, lockIdentity: newLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "dead" };
      const recovery = await store.reattachAfterDeadOwner(proof, reducerInput(state, "attach_owner", "observation", payload, input.occurredAt, { commandId: `recover-${intent.startId}-${state.owner.ownerEpoch + 1}`, idempotencyKey: `recover:${intent.startId}:${state.owner.ownerEpoch + 1}` }), runtimeContext, newLock, async (candidate, lock) => candidate.expectedLockMetadataHash === canonicalHash(lock) && candidate.observationHash === proof.observationHash);
      if (!recovery.result.accepted) throw new Error(`DAG prepared-start recovery rejected: ${recovery.result.code}: ${recovery.result.message}`);
      state = recovery.result.state; this.#currentLock.set(input.runId, newLock);
      if (existingBinding) existingBinding = await this.#createBinding(ctx, repositoryRoot, plan, state, input.occurredAt, { kind: "explicit_reattach", priorBindingHash: existingBinding.bindingHash, priorSessionId: existingBinding.sessionId, proofHash: ownership.hash }, existingBinding);
    } else throw new Error("Existing DAG run owner conflicts with prepared start intent");
    await this.startFailpoint?.("after_owner_attach");

    await this.startFailpoint?.("before_final_binding");
    const binding = existingBinding ?? await this.#createBinding(ctx, repositoryRoot, plan, state, input.occurredAt);
    await this.startFailpoint?.("after_final_binding");
    if (intent.state === "starting" || intent.bindingHash !== binding.bindingHash) {
      const active: DagRunStartIntentV1 = { ...intent, state: "active", revision: intent.revision + 1, bindingHash: binding.bindingHash };
      await replaceCanonicalJson(intentPath, intent, active); intent = active;
    }
    await this.startFailpoint?.("after_start_active");
    await this.#currentOwnerLock({ binding, plan, context: runtimeContext, store, state });
    await this.startFailpoint?.("before_response");
    return { binding, state, decision: scheduleDagRunV1(plan, state) };
  }

  async advance(ctx: DagConductorContextV1, runId: string, occurredAt = new Date().toISOString()): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }> {
    const loaded = await this.#loadBound(ctx, runId);
    const lock = await this.#currentOwnerLock(loaded);
    const lifecycleOptions = this.integrationFactory && !this.lifecycle.integration
      ? { ...this.lifecycle, integration: this.integrationFactory({ store: loaded.store, context: loaded.context, lock }) }
      : this.lifecycle;
    const lifecycle = new DagLifecycleRuntimeV1(loaded.store, loaded.plan, loaded.context, lock, await realpath(ctx.cwd), lifecycleOptions);
    for (let step = 0; step < 256; step += 1) {
      const reconciled = await lifecycle.reconcileOne(occurredAt);
      if (reconciled.progressed) continue;
      if (reconciled.waiting) return { state: reconciled.state, decision: scheduleDagRunV1(loaded.plan, reconciled.state) };
      const state = await loaded.store.read(loaded.context);
      const decision = scheduleDagRunV1(loaded.plan, state);
      if (!decision.selected.length) return { state, decision };
      const payload = { decisionHash: decision.decisionHash, decisionSequence: decision.decisionSequence, policyHash: decision.policyHash, normalizedIndexHash: decision.normalizedIndexHash, inputSnapshotHash: state.snapshotHash, reservations: decision.selected, bypassSlotIds: decision.bypassIncrements };
      const result = await loaded.store.mutate({ input: reducerInput(state, "reserve_scheduler_batch", "command", payload, occurredAt, { commandId: `scheduler-${decision.decisionSequence}-${decision.decisionHash.slice(7, 19)}`, idempotencyKey: `scheduler:${state.runNonce}:${decision.decisionSequence}` }), context: loaded.context, lock });
      if (!result.accepted) throw new Error(`Scheduler reservation rejected: ${result.code}: ${result.message}`);
    }
    const state = await loaded.store.read(loaded.context);
    return { state, decision: scheduleDagRunV1(loaded.plan, state) };
  }

  async status(ctx: DagConductorContextV1, runId: string): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV2; readyPackets: DagOwnedWorkerReadyPacketV1[]; stale: null | { sourceRevision: number; sourceSnapshotHash: string; newerObservedRevision: number; cachedAt: string } }> {
    const loaded = await this.#loadBound(ctx, runId); const lock = dagRunStoreLockIdentityFromOwner(loaded.state.owner); const lifecycle = new DagLifecycleRuntimeV1(loaded.store, loaded.plan, loaded.context, lock, await realpath(ctx.cwd), this.lifecycle); const cacheKey = canonicalHash({ repositoryRootHash: loaded.binding.repositoryRootHash, sessionId: loaded.binding.sessionId, bindingHash: loaded.binding.bindingHash, runId: loaded.state.runId, runNonceHash: loaded.binding.runNonceHash, planHash: loaded.plan.planHash }); let newerObservedRevision = loaded.state.revision;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const first = await loaded.store.read(loaded.context);
      const exactBindings = Object.values(first.workerBindings).map(({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash }) => ({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash })).sort((a, b) => a.workerStorageId.localeCompare(b.workerStorageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber);
      const workers = await this.workerProjection?.(exactBindings) ?? null;
      const decision = scheduleDagRunV1(loaded.plan, first);
      const projection = projectDagExecutionV2(loaded.plan, first, decision, workers);
      const second = await loaded.store.read(loaded.context); newerObservedRevision = Math.max(newerObservedRevision, first.revision, second.revision);
      if (first.revision === second.revision && first.snapshotHash === second.snapshotHash) { const readyPackets = await lifecycle.readyPackets(first); const third = await loaded.store.read(loaded.context); newerObservedRevision = Math.max(newerObservedRevision, third.revision); if (third.revision !== first.revision || third.snapshotHash !== first.snapshotHash) continue; const cachedAt = new Date().toISOString(); this.#lastGood.set(cacheKey, { state: first, decision, projection, readyPackets, cachedAt }); return { state: first, decision, projection, readyPackets, stale: null }; }
    }
    const lastGood = this.#lastGood.get(cacheKey);
    if (lastGood) return { state: lastGood.state, decision: lastGood.decision, projection: lastGood.projection, readyPackets: [], stale: { sourceRevision: lastGood.state.revision, sourceSnapshotHash: lastGood.state.snapshotHash, newerObservedRevision, cachedAt: lastGood.cachedAt } };
    throw new Error("DAG execution projection did not stabilize after three exact joins");
  }

  async dispatch(ctx: DagConductorContextV1, packet: DagOwnedWorkerReadyPacketV1, tacticalDirective: string | null | undefined, occurredAt = new Date().toISOString(), signal?: AbortSignal): Promise<DagOwnedWorkerDispatchResultV1> {
    throwIfAborted(signal);
    assertBoundedDagReadyPacketV1(packet);
    const normalizedDirective = tacticalDirective === undefined ? undefined : normalizeDagTacticalDirectiveV1(tacticalDirective);
    const dispatchKey = `${packet.runId}\u0000${packet.stageAttemptId}\u0000${packet.launchIntentId}`;
    const requestHash = canonicalHash({ packet, tacticalDirective: normalizedDirective ?? null });
    const current = this.#dispatches.get(dispatchKey);
    if (current) {
      if (current.requestHash !== requestHash) throw new Error("Concurrent owned-worker dispatch conflicts with the exact packet or tactical directive already in flight");
      return current.promise;
    }
    let operation!: Promise<DagOwnedWorkerDispatchResultV1>;
    operation = (async () => {
      const loaded = await this.#loadBound(ctx, packet.runId);
      const lock = await this.#currentOwnerLock(loaded);
      const lifecycle = new DagLifecycleRuntimeV1(loaded.store, loaded.plan, loaded.context, lock, await realpath(ctx.cwd), this.lifecycle);
      return lifecycle.dispatch(packet, normalizedDirective, occurredAt, signal);
    })().finally(() => { if (this.#dispatches.get(dispatchKey)?.promise === operation) this.#dispatches.delete(dispatchKey); });
    this.#dispatches.set(dispatchKey, { requestHash, promise: operation });
    return operation;
  }

  /** Read the complete currently admissible semantic frontier without mutating run or worker state. */
  async nextAction(ctx: DagConductorContextV1, runId: string, signal?: AbortSignal): Promise<DagNextActionResultV1> {
    throwIfAborted(signal);
    const status = await this.status(ctx, runId);
    if (status.stale) throw new Error("Semantic frontier requires one fresh exact run projection");
    const { state, decision } = status;
    const loaded = await this.#loadBound(ctx, runId);
    const lifecycle = new DagLifecycleRuntimeV1(loaded.store, loaded.plan, loaded.context, dagRunStoreLockIdentityFromOwner(state.owner), await realpath(ctx.cwd), this.lifecycle);
    const terminal = new Map((await lifecycle.terminalCompletions(state, signal)).map((item) => [item.stageAttemptId, item]));
    throwIfAborted(signal);
    const actions: DagSemanticActionV1[] = [];
    const controls: DagSemanticActionV1[] = [];
    const action = (target: DagSemanticActionV1[], operation: DagSemanticOperationV1, input: { workItemId?: string | null; stage?: string | null; reservation?: DagRunStateV1["scheduler"]["reservations"][string] | null; stageAttemptId?: string | null; completionId?: string | null; retryKey?: string | null; retryCount?: number | null; finalizationKind?: DagSemanticActionV1["finalizationKind"]; explanation: string; mutexGroupIds?: string[] }) => {
      const workItemId = input.workItemId ?? input.reservation?.workItemId ?? null;
      const core = {
        operation, runId, revision: state.revision, snapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch, decisionHash: decision.decisionHash,
        workItemId, stage: input.stage ?? input.reservation?.stage ?? null, candidateGeneration: input.reservation?.candidateGeneration ?? (workItemId ? state.workItems[workItemId]?.candidateGeneration ?? null : null),
        reservationId: input.reservation?.reservationId ?? null, reservationSequence: input.reservation?.reservationSequence ?? null, reservationState: input.reservation?.state ?? null,
        stageAttemptId: input.stageAttemptId ?? null, completionId: input.completionId ?? null, retryKey: input.retryKey ?? null, retryCount: input.retryCount ?? null,
        finalizationKind: input.finalizationKind ?? null,
      };
      target.push({ ...core, actionId: `action-${canonicalHash({ schemaVersion: 1, ...core }).slice(7)}`, explanation: input.explanation, mutexGroupIds: [...(input.mutexGroupIds ?? input.reservation?.mutexGroupIds ?? [])].sort(), concurrency: { activeLanes: decision.activeLaneWorkItemIds.length, maxActiveNodes: decision.maxActiveNodes, requiresRefreshAfterMutation: true } });
    };

    const cleanupAttempts = this.lifecycle.worker?.cleanupExact ? Object.values(state.stageAttempts).filter((attempt) => attempt.producerKind === "owned_worker" && Boolean(attempt.terminalAt) && Boolean(attempt.workerResult) && Boolean(state.workerBindings[attempt.stageAttemptId]) && !Object.values(state.effects).some((effect: any) => effect.kind === "cleanup_worktree" && effect.boundStageAttemptId === attempt.stageAttemptId && effect.state === "reconciled" && ["applied_exact", "proven_absent"].includes(effect.reconciliation))).sort((left, right) => left.stageAttemptId.localeCompare(right.stageAttemptId)) : [];
    const cancellation = Object.values(state.cancellations).find((candidate) => candidate.state !== "closed");
    const runTerminal = state.completion.state !== "open" || ["cancelled", "completed", "superseded"].includes(state.current.run);
    const outstanding = Object.values(state.scheduler.reservations).filter((reservation) => !["released", "fenced", "launch_ambiguous"].includes(reservation.state)).sort((left, right) => left.reservationSequence - right.reservationSequence);

    if (cancellation) {
      for (const packet of await lifecycle.readyPackets(state)) {
        const reservation = state.scheduler.reservations[packet.reservationId];
        const attempt = state.stageAttempts[packet.stageAttemptId];
        if (reservation && attempt && !state.workerBindings[attempt.stageAttemptId]) action(actions, "start_work", { reservation, stageAttemptId: attempt.stageAttemptId, explanation: `Exactly replay and bind the already-authorized ${reservation.stage} launch for ${reservation.workItemId} so cancellation can reconcile it.` });
      }
      action(actions, "finalize", { finalizationKind: "cancellation", explanation: `Finish exact cancellation ${cancellation.cancellationId} and reconcile its durable effects.` });
    } else {
      for (const attempt of cleanupAttempts) action(actions, "finalize", { workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, finalizationKind: "cleanup", explanation: `Reconcile exact owned-worktree cleanup for ${attempt.stageAttemptId}.` });
      const mayContinueAdmittedWork = !runTerminal && state.desired.run === "running" && ["active", "integration"].includes(state.current.run);
      if (mayContinueAdmittedWork) for (const reservation of outstanding) {
        const item = state.workItems[reservation.workItemId];
        const attemptId = item?.stages[reservation.stage]?.currentAttemptId ?? null;
        const attempt = attemptId ? state.stageAttempts[attemptId] : null;
        if (reservation.operationKind === "integration") action(actions, "integrate", { reservation, explanation: `Continue the already-admitted exact integration for ${reservation.workItemId}.` });
        else if (!attempt || (attempt.producerKind === "owned_worker" && !state.workerBindings[attempt.stageAttemptId])) {
          const operation = ["implementation", "evaluation", "codification", "review", "hardening"].includes(reservation.operationKind) ? "start_work" : "run_checks";
          action(actions, operation, { reservation, stageAttemptId: attempt?.stageAttemptId ?? null, explanation: operation === "start_work" ? `Start or exactly recover already-admitted owned ${reservation.stage} work for ${reservation.workItemId}.` : `Continue already-admitted synchronous ${reservation.stage} checks for ${reservation.workItemId}.` });
        } else if (attempt.producerKind === "owned_worker" && ["running", "settling"].includes(attempt.state)) {
          const completion = terminal.get(attempt.stageAttemptId);
          if (completion) action(actions, "record_completion", { reservation, stageAttemptId: attempt.stageAttemptId, completionId: completion.completionId, explanation: `Record and explicitly reconcile durable worker completion ${completion.completionId} for ${attempt.stageAttemptId}.` });
        } else if (attempt.producerKind === "owned_worker" && attempt.state === "result_observed" && ["F1", "F3"].includes(attempt.stage) && state.workItems[attempt.workItemId].candidate?.producedByStageAttemptId !== attempt.stageAttemptId) action(actions, "finalize", { reservation, stageAttemptId: attempt.stageAttemptId, finalizationKind: "worker_result", explanation: `Finalize the already-admitted exact Git candidate output from ${attempt.stageAttemptId}.` });
        else action(actions, "run_checks", { reservation, stageAttemptId: attempt?.stageAttemptId ?? null, explanation: `Run or close already-admitted synchronous ${reservation.stage} checks for ${reservation.workItemId}.` });
      }
      if (!runTerminal && state.desired.run === "paused") for (const reservation of outstanding) {
        const item = state.workItems[reservation.workItemId];
        const attemptId = item?.stages[reservation.stage]?.currentAttemptId ?? null;
        const attempt = attemptId ? state.stageAttempts[attemptId] : null;
        if (!attempt || attempt.producerKind !== "owned_worker" || !state.workerBindings[attempt.stageAttemptId]) continue;
        if (["running", "settling"].includes(attempt.state)) {
          const completion = terminal.get(attempt.stageAttemptId);
          if (completion) action(actions, "record_completion", { reservation, stageAttemptId: attempt.stageAttemptId, completionId: completion.completionId, explanation: `Observe durable completion ${completion.completionId} for already-started paused work ${attempt.stageAttemptId}.` });
        } else if (attempt.state === "result_observed" && ["F1", "F3"].includes(attempt.stage) && state.workItems[attempt.workItemId].candidate?.producedByStageAttemptId !== attempt.stageAttemptId) action(actions, "finalize", { reservation, stageAttemptId: attempt.stageAttemptId, finalizationKind: "worker_result", explanation: `Finalize already-observed paused work ${attempt.stageAttemptId} without starting another attempt.` });
      }

      if (!runTerminal && state.desired.run === "running" && ["active", "integration"].includes(state.current.run)) {
        const outstandingKeys = new Set(outstanding.map(({ workItemId, stage }) => `${workItemId}\u0000${stage}`));
        for (const proposal of decision.selected) {
          if (outstandingKeys.has(`${proposal.workItemId}\u0000${proposal.stage}`)) continue;
          const operation = proposal.operationKind === "integration" ? "integrate" : ["implementation", "evaluation", "codification", "review", "hardening"].includes(proposal.operationKind) ? "start_work" : "run_checks";
          action(actions, operation, { workItemId: proposal.workItemId, stage: proposal.stage, explanation: operation === "integrate" ? `Admit exact integration for ${proposal.workItemId}.` : operation === "start_work" ? `Admit and start exact owned ${proposal.stage} work for ${proposal.workItemId}.` : `Admit and run synchronous ${proposal.stage} checks for ${proposal.workItemId}.`, mutexGroupIds: proposal.mutexGroupIds });
        }
        for (const retry of Object.values(state.retryLedger).sort((left, right) => left.retryKey.localeCompare(right.retryKey))) if (retryIsAdmissible(state, retry)) action(actions, "retry", { workItemId: retry.workItemId, stage: retry.stage, retryKey: retry.retryKey, retryCount: retry.count, explanation: `Authorize the next exact ${retry.dimension} retry (${retry.count + 1}/${retry.ceiling}).` });
        action(controls, "pause", { explanation: "Pause only new canonical admission; already-admitted work remains observable and finalizable." });
        action(controls, "cancel", { explanation: "Cancel the bound run and reconcile every exact effect." });
      } else if (!runTerminal && state.desired.run === "paused") {
        action(controls, "resume", { explanation: "Resume new admission for this exact paused canonical run." });
        action(controls, "cancel", { explanation: "Cancel the bound paused run and reconcile every exact effect." });
      }
    }
    actions.sort(compareSemanticActions); controls.sort(compareSemanticActions);
    return { schemaVersion: 1, kind: "DagNextActionResultV1", runId, revision: state.revision, snapshotHash: state.snapshotHash, frontier: actions, controls, waiting: actions.length === 0 && !runTerminal && !cancellation, notice: `${decision.notice} Choices are revision-bound; invoke one mutation, then refresh dag_next_action before selecting another.` };
  }

  /** Resolve a callback to one canonical completion identity without changing DAG state. */
  async completionNotice(ctx: DagConductorContextV1, event: { workerId: string; attemptNumber: number; completionId: string; terminalStatus: string }): Promise<{ runId: string; stageAttemptId: string; workItemId: string; stage: string; completionId: string; terminalStatus: string; requiresBinding: boolean } | null> {
    const binding = await this.binding(ctx);
    if (!binding) return null;
    const loaded = await this.#loadBound(ctx, binding.runId);
    const bindingMatches = Object.values(loaded.state.workerBindings).filter((candidate) => candidate.workerId === event.workerId && candidate.attemptNumber === event.attemptNumber);
    const attemptMatches = Object.values(loaded.state.stageAttempts).filter((candidate) => {
      const launch = candidate.launchIntentId ? loaded.state.launchIntents[candidate.launchIntentId] : null;
      return candidate.producerKind === "owned_worker" && launch?.workerId === event.workerId && launch.expectedAttemptNumber === event.attemptNumber;
    });
    if (bindingMatches.length > 1 || attemptMatches.length !== 1 || (bindingMatches.length === 1 && bindingMatches[0].stageAttemptId !== attemptMatches[0].stageAttemptId)) throw new Error("Worker completion callback conflicts with canonical launch authority");
    const attempt = attemptMatches[0];
    if (attempt.workerResult) return null;
    if (!event.completionId || !event.terminalStatus) throw new Error("Worker completion callback lacks exact terminal identities");
    return { runId: loaded.state.runId, stageAttemptId: attempt.stageAttemptId, workItemId: attempt.workItemId, stage: attempt.stage, completionId: event.completionId, terminalStatus: event.terminalStatus, requiresBinding: bindingMatches.length === 0 };
  }

  async startWork(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage: string, tacticalDirective?: string | null, signal?: AbortSignal): Promise<{ state: DagRunStateV1; binding: DagOwnedWorkerDispatchResultV1["binding"]; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#startWork(ctx, runId, actionId, workItemId, stage, tacticalDirective, signal)); }
  async #startWork(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage: string, tacticalDirective?: string | null, signal?: AbortSignal): Promise<{ state: DagRunStateV1; binding: DagOwnedWorkerDispatchResultV1["binding"]; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "start_work", { workItemId, stage }, signal);
    const occurredAt = new Date().toISOString();
    const reservation = await this.#ensureSemanticReservation(ctx, selected, occurredAt, signal);
    for (let transition = 0; transition < 4; transition += 1) {
      throwIfAborted(signal);
      const { lifecycle, loaded } = await this.#semanticRuntime(ctx, runId, occurredAt, undefined, signal);
      const packets = await lifecycle.readyPackets(loaded.state);
      const packet = packets.find((candidate) => candidate.reservationId === reservation.reservationId);
      if (packet) {
        const dispatched = await this.dispatch(ctx, packet, tacticalDirective, occurredAt, signal);
        return { state: dispatched.state, binding: dispatched.binding, next: await this.nextAction(ctx, runId) };
      }
      const current = loaded.state.scheduler.reservations[reservation.reservationId];
      if (!current || ["released", "fenced", "launch_ambiguous"].includes(current.state)) throw new Error("Selected owned-work reservation is no longer startable");
      const result = await lifecycle.reconcileSemanticOne(reservation.reservationId, occurredAt, signal);
      if (!result.progressed) throw new Error(result.reason ?? "Selected owned work did not reach exact dispatch authority");
    }
    throw new Error("Selected owned work exceeded its closed canonical start transition set");
  }

  async runChecks(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#runChecks(ctx, runId, actionId, workItemId, stage, signal)); }
  async #runChecks(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "run_checks", { workItemId, stage }, signal);
    const occurredAt = new Date().toISOString();
    const reservation = await this.#ensureSemanticReservation(ctx, selected, occurredAt, signal);
    let state: DagRunStateV1 | null = null;
    let waitingReason: string | null = null;
    let madeLifecycleProgress = false;
    for (let transition = 0; transition < 32; transition += 1) {
      throwIfAborted(signal);
      const { lifecycle, loaded } = await this.#semanticRuntime(ctx, runId, occurredAt, undefined, signal); state = loaded.state;
      const current = state.scheduler.reservations[reservation.reservationId];
      if (!current || ["released", "fenced", "launch_ambiguous"].includes(current.state)) break;
      const result = await lifecycle.reconcileSemanticOne(reservation.reservationId, occurredAt, signal); state = result.state;
      if (!result.progressed) { waitingReason = result.reason; break; }
      madeLifecycleProgress = true;
    }
    if (!state) throw new Error("Synchronous checks did not read canonical state");
    const stillActionable = state.scheduler.reservations[reservation.reservationId] && !["released", "fenced", "launch_ambiguous"].includes(state.scheduler.reservations[reservation.reservationId].state);
    if (stillActionable && !madeLifecycleProgress) throw new Error(waitingReason ?? "Synchronous checks made no canonical progress");
    return { state, next: await this.nextAction(ctx, runId) };
  }

  async recordCompletion(ctx: DagConductorContextV1, runId: string, actionId: string, stageAttemptId: string, completionId: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#recordCompletion(ctx, runId, actionId, stageAttemptId, completionId, signal)); }
  async #recordCompletion(ctx: DagConductorContextV1, runId: string, actionId: string, stageAttemptId: string, completionId: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "record_completion", { stageAttemptId, completionId }, signal);
    const occurredAt = new Date().toISOString();
    const { lifecycle } = await this.#semanticRuntime(ctx, runId, occurredAt, selected, signal);
    const result = await lifecycle.recordCompletion(stageAttemptId, completionId, occurredAt, signal);
    return { state: result.state, next: await this.nextAction(ctx, runId) };
  }

  async integrateSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage = "F8", signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#integrateSemantic(ctx, runId, actionId, workItemId, stage, signal)); }
  async #integrateSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, workItemId: string, stage = "F8", signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "integrate", { workItemId, stage }, signal);
    const occurredAt = new Date().toISOString();
    const reservation = await this.#ensureSemanticReservation(ctx, selected, occurredAt, signal);
    let state: DagRunStateV1 | null = null;
    for (let transition = 0; transition < 32; transition += 1) {
      throwIfAborted(signal);
      const { lifecycle, loaded } = await this.#semanticRuntime(ctx, runId, occurredAt, undefined, signal); state = loaded.state;
      const current = state.scheduler.reservations[reservation.reservationId];
      if (!current || ["released", "fenced", "launch_ambiguous"].includes(current.state)) break;
      const result = await lifecycle.reconcileSemanticOne(reservation.reservationId, occurredAt, signal); state = result.state;
      if (!result.progressed) break;
    }
    if (!state) throw new Error("Integration did not read canonical state");
    return { state, next: await this.nextAction(ctx, runId) };
  }

  async finalizeSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, stageAttemptId?: string | null, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#finalizeSemantic(ctx, runId, actionId, stageAttemptId, signal)); }
  async #finalizeSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, stageAttemptId?: string | null, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "finalize", { stageAttemptId: stageAttemptId ?? null }, signal);
    const occurredAt = new Date().toISOString();
    const { lifecycle, loaded } = await this.#semanticRuntime(ctx, runId, occurredAt, selected, signal);
    let result = selected.finalizationKind === "worker_result" ? await lifecycle.finalizeWorkerResult(selected.stageAttemptId!, occurredAt, signal)
      : selected.finalizationKind === "cleanup" ? await lifecycle.reconcileCleanupAttempt(selected.stageAttemptId!, occurredAt, signal)
      : selected.finalizationKind === "cancellation" ? await lifecycle.reconcileCancellationOne(occurredAt, signal) : null;
    if (!result) result = { state: loaded.state, progressed: false, waiting: true, reason: "No exact finalization is currently pending" };
    return { state: result.state, next: await this.nextAction(ctx, runId) };
  }

  async retrySemantic(ctx: DagConductorContextV1, runId: string, actionId: string, retryKey: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#retrySemantic(ctx, runId, actionId, retryKey, signal)); }
  async #retrySemantic(ctx: DagConductorContextV1, runId: string, actionId: string, retryKey: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, "retry", { retryKey }, signal);
    const occurredAt = new Date().toISOString();
    const loaded = await this.#loadOperational(ctx, runId, occurredAt, signal); this.#assertActionState(loaded.state, selected); const entry = loaded.state.retryLedger[retryKey];
    if (!entry || !retryIsAdmissible(loaded.state, entry)) throw new Error("Exact retry is no longer admissible");
    const guard = semanticGuard(loaded.state, "retry", actionId, occurredAt);
    const state = await this.retry(ctx, guard, { retryKey, expectedCount: entry.count, workItemId: entry.workItemId, stage: entry.stage, dimension: entry.dimension, fingerprint: entry.fingerprint, candidateGeneration: loaded.state.workItems[entry.workItemId].candidateGeneration }, signal);
    return { state, next: await this.nextAction(ctx, runId) };
  }

  async controlSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, operation: "pause" | "resume" | "cancel", reason: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.#withSemanticActionClaim(actionId, () => this.#controlSemantic(ctx, runId, actionId, operation, reason, signal)); }
  async #controlSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, operation: "pause" | "resume" | "cancel", reason: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> {
    const selected = await this.#requireSemanticAction(ctx, runId, actionId, operation, {}, signal);
    const occurredAt = new Date().toISOString(); const loaded = await this.#loadOperational(ctx, runId, occurredAt, signal); this.#assertActionState(loaded.state, selected); throwIfAborted(signal);
    const guard = semanticGuard(loaded.state, operation, actionId, occurredAt);
    const state = await this.control(ctx, guard, operation, reason, signal);
    return { state, next: await this.nextAction(ctx, runId) };
  }

  async cancelSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, reason: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.controlSemantic(ctx, runId, actionId, "cancel", reason, signal); }
  async pauseSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, reason: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.controlSemantic(ctx, runId, actionId, "pause", reason, signal); }
  async resumeSemantic(ctx: DagConductorContextV1, runId: string, actionId: string, reason: string, signal?: AbortSignal): Promise<{ state: DagRunStateV1; next: DagNextActionResultV1 }> { return this.controlSemantic(ctx, runId, actionId, "resume", reason, signal); }

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

  async control(ctx: DagConductorContextV1, guard: DagMutationGuardV1, action: "pause" | "resume" | "cancel", reason: string, signal?: AbortSignal): Promise<DagRunStateV1> {
    throwIfAborted(signal);
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
    throwIfAborted(signal);
    const result = await loaded.store.mutate({ input, context: loaded.context, lock, signal });
    if (!result.accepted) throw new Error(`DAG control rejected: ${result.code}: ${result.message}`);
    let state = result.state;
    if (action === "cancel" && this.dispatchEffect) for (const effectId of result.state.cancellations[`cancel-${guard.commandId}`].effectIds) {
      throwIfAborted(signal);
      const effect = state.effects[effectId];
      const markPayload = { effectId, expectedDispatchCount: effect.dispatchCount };
      const marked = await loaded.store.mutate({ input: reducerInput(state, "mark_effect_dispatching", "command", markPayload, guard.occurredAt, { commandId: `${guard.commandId}-dispatch-${effectId}`, idempotencyKey: `${guard.idempotencyKey}:dispatch:${effectId}` }), context: loaded.context, lock, signal });
      if (!marked.accepted) throw new Error(`DAG cancellation dispatch rejected: ${marked.code}: ${marked.message}`);
      state = marked.state; await this.dispatchEffect({ effectId, kind: effect.kind, requestHash: effect.requestHash }, state);
    }
    return state;
  }

  async retry(ctx: DagConductorContextV1, guard: DagMutationGuardV1, payload: { retryKey: string; expectedCount: number; workItemId: string; stage: string; dimension: string; fingerprint: string; candidateGeneration: number }, signal?: AbortSignal): Promise<DagRunStateV1> {
    throwIfAborted(signal);
    const loaded = await this.#loadGuarded(ctx, guard); const lock = await this.#currentOwnerLock(loaded);
    const input = reducerInput(loaded.state, "authorize_retry", "command", payload, guard.occurredAt, guard);
    const result = await loaded.store.mutate({ input, context: loaded.context, lock, signal });
    if (!result.accepted) throw new Error(`DAG retry rejected: ${result.code}: ${result.message}`);
    return result.state;
  }

  async reattach(ctx: DagConductorContextV1, guard: DagMutationGuardV1): Promise<DagRunStateV1> {
    const loaded = await this.#loadForReattach(ctx, guard.runId);
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId()); const processStartIdentity = await currentProcessStartIdentity();
    if (loaded.binding.ownerEpoch === loaded.state.owner.ownerEpoch && loaded.binding.ownershipReceiptHash === loaded.state.owner.ownershipReceipt) {
      await this.#refreshStartIntentBinding(repositoryRoot, loaded.binding.lineage?.priorSessionId ?? loaded.state.owner.sessionId, sessionId, guard.runId, loaded.binding.bindingHash);
    }
    if (loaded.state.runNonce === guard.runNonce && loaded.state.owner.ownerEpoch === guard.ownerEpoch + 1 && loaded.state.owner.sessionId === sessionId && loaded.state.owner.pid === process.pid && loaded.state.owner.processStartIdentity === processStartIdentity) {
      const ownership = loaded.state.owner.ownershipReceipt ? await loaded.store.readImmutableFact(loaded.state.owner.ownershipReceipt) as any : null;
      const previousPath = loaded.state.previousSnapshotHash ? join(loaded.store.snapshotsDirectory, `${loaded.state.previousSnapshotHash.slice("sha256:".length)}.json`) : null;
      const previous = previousPath ? parseStrictJson(await readFile(previousPath, "utf8")) as DagRunStateV1 : null;
      if (ownership?.kind === "ownership" && ownership.ownerEpoch === loaded.state.owner.ownerEpoch && ownership.priorSessionId !== null && previous?.revision === guard.expectedRevision && previous.snapshotHash === guard.expectedSnapshotHash && previous.owner.ownerEpoch === guard.ownerEpoch) {
        const lock = dagRunStoreLockIdentityFromOwner(loaded.state.owner); this.#currentLock.set(guard.runId, lock); return loaded.state;
      }
    }
    if (loaded.state.runNonce !== guard.runNonce || loaded.state.revision !== guard.expectedRevision || loaded.state.snapshotHash !== guard.expectedSnapshotHash || loaded.state.owner.ownerEpoch !== guard.ownerEpoch) throw new Error("DAG reattach guard is stale");
    if (!loaded.state.owner.sessionId || !loaded.state.owner.lockIdentity) throw new Error("Detached epoch-zero run uses start/attach, not dead-owner reattach");
    const priorBinding = await this.#readBinding(repositoryRoot, loaded.state.owner.sessionId);
    if (!priorBinding) throw new Error("DAG reattach requires the exact prior owner session binding");
    await validateBindingAuthority(priorBinding, repositoryRoot, loaded.plan, loaded.state);
    const successorExistingBinding = await this.#readBinding(repositoryRoot, sessionId);
    if (successorExistingBinding && successorExistingBinding.bindingHash !== priorBinding.bindingHash) throw new Error("Successor session binding conflicts with pre-transfer authority");
    const priorLock = dagRunStoreLockIdentityFromOwner(loaded.state.owner); const proof = await createDagRunStoreDeadOwnerProofV1(priorLock, guard.occurredAt);
    const newLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-reattach", sessionId, runId: guard.runId, ownerEpoch: guard.ownerEpoch + 1 }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId: guard.runId, nonce: randomUUID() }), guard.occurredAt);
    const priorOwnership = loaded.state.owner.ownershipReceipt ? await loaded.store.readImmutableFact(loaded.state.owner.ownershipReceipt) as any : null;
    const ownershipCore = {
      kind: "ownership" as const, runId: loaded.state.runId, runNonce: loaded.state.runNonce,
      priorSessionId: loaded.state.owner.sessionId, priorOwnerTokenHash: loaded.state.owner.ownerTokenHash, priorPid: loaded.state.owner.pid,
      priorProcessStartIdentity: loaded.state.owner.processStartIdentity, priorLockIdentity: loaded.state.owner.lockIdentity, priorAttachedAt: loaded.state.owner.attachedAt,
      disposition: "dead" as const, priorObservationHash: proof.observationHash, priorOwnershipReceiptHash: loaded.state.owner.ownershipReceipt,
      ownerEpoch: loaded.state.owner.ownerEpoch + 1, successorSessionId: sessionId, successorPid: process.pid,
      successorProcessStartIdentity: newLock.processStartIdentity, successorLockIdentity: newLock.lockIdentity, lineageHash: null,
    };
    const ownershipWithChain = { ...ownershipCore, chainHash: ownershipChainHashV1(ownershipCore, priorOwnership?.kind === "ownership" ? priorOwnership.chainHash : null) };
    const ownership = { ...ownershipWithChain, hash: canonicalHash(ownershipWithChain) }; await loaded.store.putImmutableFact(ownership);
    const context = { ...loaded.context, facts: { ...loaded.context.facts, [ownership.hash]: ownership } };
    const payload = { ownerTokenHash: newLock.ownerTokenHash, sessionId, pid: process.pid, processStartIdentity: newLock.processStartIdentity, lockIdentity: newLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "dead" };
    const input = reducerInput(loaded.state, "attach_owner", "observation", payload, guard.occurredAt, guard);
    const recovery = await loaded.store.reattachAfterDeadOwner(proof, input, context, newLock, async (candidate, lock) => candidate.expectedLockMetadataHash === canonicalHash(lock) && candidate.observationHash === proof.observationHash);
    if (!recovery.result.accepted) throw new Error(`DAG reattach rejected: ${recovery.result.code}: ${recovery.result.message}`);
    await this.ownerResumeFailpoint?.("after_owner_transfer");
    this.#currentLock.set(guard.runId, newLock);
    const binding = await this.#createBinding(ctx, repositoryRoot, loaded.plan, recovery.result.state, guard.occurredAt, { kind: "explicit_reattach", priorBindingHash: priorBinding.bindingHash, priorSessionId: priorBinding.sessionId, proofHash: ownership.hash }, successorExistingBinding);
    await this.ownerResumeFailpoint?.("after_owner_binding");
    await this.#refreshStartIntentBinding(repositoryRoot, priorBinding.sessionId, sessionId, guard.runId, binding.bindingHash);
    await this.ownerResumeFailpoint?.("after_owner_start_identity");
    return recovery.result.state;
  }

  async binding(ctx: DagConductorContextV1): Promise<DagSessionRunBindingV1 | null> { return this.#readBinding(await realpath(ctx.cwd), String(ctx.sessionManager.getSessionId())); }

  /** Return the sole unfinished start for this exact session, if one exists. */
  async pendingStart(ctx: DagConductorContextV1, sourcePlanningPlanId?: string, sourcePlanningPlanHash?: string): Promise<Pick<DagRunStartIntentV1, "runId" | "runNonce" | "planHash" | "sourcePlanningPlanId" | "sourcePlanningPlanHash" | "startedAt"> | null> {
    const repositoryRoot = await realpath(ctx.cwd);
    const sessionId = String(ctx.sessionManager.getSessionId());
    const directory = dirname(startIntentPath(repositoryRoot, sessionId, "placeholder"));
    let names: string[];
    try { names = await readdir(directory); }
    catch (error: any) { if (error?.code === "ENOENT") return null; throw error; }
    const matches: DagRunStartIntentV1[] = [];
    for (const name of names.filter((value) => ID_RE.test(value.slice(0, -5)) && value.endsWith(".json")).sort()) {
      const runId = name.slice(0, -5);
      const intent = await readStartIntent(join(directory, name), sessionId, runId);
      if (intent?.state === "starting") matches.push(intent);
    }
    if (matches.length > 1) throw new Error("Multiple unfinished DAG starts exist for the exact current session");
    if (matches.length === 0) return null;
    if ((sourcePlanningPlanId !== undefined && matches[0].sourcePlanningPlanId !== sourcePlanningPlanId)
      || (sourcePlanningPlanHash !== undefined && matches[0].sourcePlanningPlanHash !== sourcePlanningPlanHash)) {
      throw new Error(`Unfinished DAG start ${matches[0].runId} belongs to a different planning plan and must be recovered first`);
    }
    const { runId, runNonce, planHash: canonicalPlanHash, sourcePlanningPlanId: planId, sourcePlanningPlanHash: sourcePlanHash, startedAt } = matches[0];
    return { runId, runNonce, planHash: canonicalPlanHash, sourcePlanningPlanId: planId, sourcePlanningPlanHash: sourcePlanHash, startedAt };
  }

  async startIdentity(ctx: DagConductorContextV1, runId: string): Promise<Pick<DagRunStartIntentV1, "runId" | "planHash" | "sourcePlanningPlanId" | "sourcePlanningPlanHash">> {
    const repositoryRoot = await realpath(ctx.cwd);
    const sessionId = String(ctx.sessionManager.getSessionId());
    const binding = await this.#readBinding(repositoryRoot, sessionId);
    if (!binding || binding.runId !== runId) throw new Error("No exact current-session binding exists for the requested DAG run");
    const intent = await readStartIntent(startIntentPath(repositoryRoot, sessionId, runId), sessionId, runId);
    if (!intent) {
      if (await requiresPreparedStartIntent(repositoryRoot, binding.storeRoot, runId)) throw new Error("Current product run is missing its exact prepared-start identity");
      return { runId: binding.runId, planHash: binding.planHash, sourcePlanningPlanId: null, sourcePlanningPlanHash: null };
    }
    if (intent.state !== "active" || intent.bindingHash !== binding.bindingHash || intent.planHash !== binding.planHash) throw new Error("Current-session binding does not resolve its exact active start identity");
    return { runId: intent.runId, planHash: intent.planHash, sourcePlanningPlanId: intent.sourcePlanningPlanId, sourcePlanningPlanHash: intent.sourcePlanningPlanHash };
  }

  async #loadForReattach(ctx: DagConductorContextV1, runId: string): Promise<LoadedRunV1> {
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId());
    const binding = await this.#readBinding(repositoryRoot, sessionId);
    if (binding) return this.#loadBound(ctx, runId, true);
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), runId);
    const plan = parseCanonicalDagPlanV1(await readFile(join(store.runDirectory, "authority", "plan.json"), "utf8"));
    const context = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")) as unknown as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan); const effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    const state = await store.read(effectiveContext);
    if (state.owner.sessionId === sessionId && state.owner.ownershipReceipt) {
      const ownership = await store.readImmutableFact(state.owner.ownershipReceipt) as any;
      const sourceSessionId = ownership?.priorSessionId as string | null;
      const sourceBinding = sourceSessionId ? await this.#readBinding(repositoryRoot, sourceSessionId) : null;
      const repairable = sourceBinding && ownership?.kind === "ownership" && ownership.hash === state.owner.ownershipReceipt
        && ownership.ownerEpoch === state.owner.ownerEpoch && ownership.successorSessionId === sessionId
        && ownership.successorPid === state.owner.pid && ownership.successorProcessStartIdentity === state.owner.processStartIdentity
        && ownership.successorLockIdentity === state.owner.lockIdentity && sourceBinding.ownerEpoch === state.owner.ownerEpoch - 1
        && sourceBinding.ownershipReceiptHash === ownership.priorOwnershipReceiptHash;
      if (!repairable) throw new Error("DAG owner recovery has no exact predecessor binding for successor repair");
      await validateBindingRepository(sourceBinding, repositoryRoot);
      const lineageKind = ownership.disposition === "same_manager" ? "same_manager_resume" : "explicit_reattach";
      const repaired = await this.#createBinding(ctx, repositoryRoot, plan, state, state.updatedAt, { kind: lineageKind, priorBindingHash: sourceBinding.bindingHash, priorSessionId: sourceSessionId, proofHash: ownership.hash });
      await this.#refreshStartIntentBinding(repositoryRoot, sourceSessionId!, sessionId, runId, repaired.bindingHash);
      return this.#loadBound(ctx, runId);
    }
    const git = await repositoryGitBinding(repositoryRoot);
    const placeholderCore = { schemaVersion: 1 as const, kind: "DagSessionRunBindingV1" as const, sessionId, sessionFileHash: null, repositoryRootHash: canonicalHash(repositoryRoot), commonDirIdentityHash: git.commonDirIdentityHash, branchRef: git.branchRef, runId, runNonceHash: canonicalHash(state.runNonce), planHash: plan.planHash, storeRoot: RUN_ROOT, boundAt: state.createdAt };
    const placeholder = { ...placeholderCore, bindingHash: canonicalHash(placeholderCore) };
    return { binding: placeholder, plan, context: effectiveContext, store, state };
  }

  async #loadOperational(ctx: DagConductorContextV1, runId: string, occurredAt: string, signal?: AbortSignal): Promise<LoadedRunV1> {
    throwIfAborted(signal);
    let loaded = await this.#loadBound(ctx, runId, true);
    try { await this.#currentOwnerLock(loaded); return loaded; }
    catch (ownerError) {
      if (!loaded.state.owner.sessionId || !loaded.state.owner.lockIdentity) throw ownerError;
      const guard = semanticGuard(loaded.state, "recover", loaded.binding.bindingHash, occurredAt);
      throwIfAborted(signal);
      await this.reattach(ctx, guard);
      loaded = await this.#loadBound(ctx, runId, true);
      await this.#currentOwnerLock(loaded);
      return loaded;
    }
  }

  async #withSemanticActionClaim<T>(actionId: string, operation: () => Promise<T>): Promise<T> {
    if (this.#semanticActionClaims.has(actionId)) throw new Error("Semantic action is already executing concurrently");
    this.#semanticActionClaims.add(actionId);
    try { return await operation(); }
    finally { this.#semanticActionClaims.delete(actionId); }
  }

  async #requireSemanticAction(ctx: DagConductorContextV1, runId: string, actionId: string, operation: DagSemanticOperationV1, selector: Partial<Pick<DagSemanticActionV1, "workItemId" | "stage" | "stageAttemptId" | "completionId" | "retryKey">>, signal?: AbortSignal): Promise<DagSemanticActionV1> {
    if (!actionId) throw new Error("Semantic mutation requires the exact actionId returned by dag_next_action");
    const frontier = await this.nextAction(ctx, runId, signal);
    const selected = [...frontier.frontier, ...frontier.controls].find((candidate) => candidate.actionId === actionId);
    if (!selected) throw new Error("Semantic action is stale, consumed, or from a different run snapshot");
    if (selected.operation !== operation) throw new Error(`Semantic action ${actionId} requires ${selected.operation}, not ${operation}`);
    for (const [key, value] of Object.entries(selector)) if ((selected as any)[key] !== value) throw new Error(`Semantic action ${actionId} does not bind the requested ${key}`);
    return selected;
  }

  #assertActionState(state: DagRunStateV1, action: DagSemanticActionV1): void {
    if (state.runId !== action.runId || state.revision !== action.revision || state.snapshotHash !== action.snapshotHash || state.owner.ownerEpoch !== action.ownerEpoch) throw new Error("Semantic action became stale before its exact mutation boundary");
    if (action.workItemId && !action.reservationId && state.workItems[action.workItemId]?.candidateGeneration !== action.candidateGeneration) throw new Error("Semantic action candidate generation is stale");
    if (action.reservationId) {
      const reservation = state.scheduler.reservations[action.reservationId];
      if (!reservation || reservation.workItemId !== action.workItemId || reservation.stage !== action.stage || reservation.reservationSequence !== action.reservationSequence || reservation.state !== action.reservationState || reservation.candidateGeneration !== action.candidateGeneration) throw new Error("Semantic action reservation identity is stale or replaced");
    }
  }

  async #semanticRuntime(ctx: DagConductorContextV1, runId: string, occurredAt: string, selected?: DagSemanticActionV1, signal?: AbortSignal): Promise<{ loaded: LoadedRunV1; lifecycle: DagLifecycleRuntimeV1 }> {
    const loaded = await this.#loadOperational(ctx, runId, occurredAt, signal);
    if (selected) this.#assertActionState(loaded.state, selected);
    const lock = await this.#currentOwnerLock(loaded);
    const lifecycleOptions = this.integrationFactory && !this.lifecycle.integration ? { ...this.lifecycle, integration: this.integrationFactory({ store: loaded.store, context: loaded.context, lock }) } : this.lifecycle;
    return { loaded, lifecycle: new DagLifecycleRuntimeV1(loaded.store, loaded.plan, loaded.context, lock, await realpath(ctx.cwd), lifecycleOptions) };
  }

  async #ensureSemanticReservation(ctx: DagConductorContextV1, selected: DagSemanticActionV1, occurredAt: string, signal?: AbortSignal): Promise<DagRunStateV1["scheduler"]["reservations"][string]> {
    throwIfAborted(signal);
    let loaded = await this.#loadOperational(ctx, selected.runId, occurredAt, signal);
    this.#assertActionState(loaded.state, selected);
    let reservation = selected.reservationId ? loaded.state.scheduler.reservations[selected.reservationId] : undefined;
    if (!reservation) {
      const decision = scheduleDagRunV1(loaded.plan, loaded.state);
      if (decision.decisionHash !== selected.decisionHash) throw new Error("Semantic action scheduler decision is stale");
      const proposal = decision.selected.find((candidate) => candidate.workItemId === selected.workItemId && candidate.stage === selected.stage && candidate.itemGeneration === selected.candidateGeneration);
      if (!proposal) throw new Error(`No exact selected canonical ${selected.stage} action exists for ${selected.workItemId}`);
      const expected = proposal.operationKind === "integration" ? "integrate" : ["implementation", "evaluation", "codification", "review", "hardening"].includes(proposal.operationKind) ? "start_work" : "run_checks";
      if (expected !== selected.operation) throw new Error(`Selected action requires dag_${expected}, not dag_${selected.operation}`);
      const payload = { decisionHash: decision.decisionHash, decisionSequence: decision.decisionSequence, policyHash: decision.policyHash, normalizedIndexHash: decision.normalizedIndexHash, inputSnapshotHash: loaded.state.snapshotHash, reservations: decision.selected, bypassSlotIds: decision.bypassIncrements };
      const lock = await this.#currentOwnerLock(loaded);
      throwIfAborted(signal);
      const result = await loaded.store.mutate({ input: reducerInput(loaded.state, "reserve_scheduler_batch", "command", payload, occurredAt, { commandId: `semantic-reserve-${decision.decisionSequence}-${selected.actionId.slice(7, 19)}`, idempotencyKey: `semantic-reserve:${loaded.state.runNonce}:${decision.decisionSequence}:${selected.actionId}` }), context: loaded.context, lock, signal });
      if (!result.accepted) throw new Error(`Semantic scheduler reservation rejected: ${result.code}: ${result.message}`);
      loaded = await this.#loadBound(ctx, selected.runId, true);
      reservation = loaded.state.scheduler.reservations[proposal.reservationId];
    }
    if (!reservation || reservation.workItemId !== selected.workItemId || reservation.stage !== selected.stage || reservation.candidateGeneration !== selected.candidateGeneration) throw new Error("Semantic action did not resolve its exact durable reservation");
    const expected = reservation.operationKind === "integration" ? "integrate" : ["implementation", "evaluation", "codification", "review", "hardening"].includes(reservation.operationKind) ? "start_work" : "run_checks";
    if (selected.operation !== expected && !(selected.operation === "run_checks" && Boolean(loaded.state.workItems[selected.workItemId!]?.stages[selected.stage!]?.currentAttemptId))) throw new Error(`Durable reservation requires dag_${expected}, not dag_${selected.operation}`);
    return reservation;
  }

  async #loadGuarded(ctx: DagConductorContextV1, guard: DagMutationGuardV1): Promise<LoadedRunV1> {
    const loaded = await this.#loadBound(ctx, guard.runId);
    if (loaded.state.runNonce !== guard.runNonce || loaded.state.revision !== guard.expectedRevision || loaded.state.snapshotHash !== guard.expectedSnapshotHash || loaded.state.owner.ownerEpoch !== guard.ownerEpoch) throw new Error("DAG mutation guard is stale");
    return loaded;
  }

  async #loadBound(ctx: DagConductorContextV1, runId: string, repairBinding = false): Promise<LoadedRunV1> {
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId()); let binding = await this.#readBinding(repositoryRoot, sessionId);
    if (!binding || binding.runId !== runId) throw new Error("No exact current-session binding exists for the requested DAG run");
    await validateBindingRepository(binding, repositoryRoot);
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, binding.storeRoot), runId);
    const plan = parseCanonicalDagPlanV1(await readFile(join(store.runDirectory, "authority", "plan.json"), "utf8"));
    const context = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")) as unknown as DagRunValidationContextV1;
    const index = buildSchedulerPlanIndexV1(plan); const effectiveContext = { ...context, plan, normalizedSchedulerIndexHash: index.indexHash };
    const state = await store.read(effectiveContext);
    const staticIdentityExact = (!state.runNonce || canonicalHash(state.runNonce) === binding.runNonceHash) && state.identity.planHash === binding.planHash && state.owner.sessionId === binding.sessionId;
    if (!staticIdentityExact) throw new Error("Session binding conflicts with run authority");
    if (state.owner.ownerEpoch !== binding.ownerEpoch || state.owner.ownershipReceipt !== binding.ownershipReceiptHash) {
      if (!repairBinding) throw new Error("Session binding conflicts with run authority");
      const previousPath = state.previousSnapshotHash ? join(store.snapshotsDirectory, `${state.previousSnapshotHash.slice("sha256:".length)}.json`) : null;
      const previous = previousPath ? parseStrictJson(await readFile(previousPath, "utf8")) as DagRunStateV1 : null;
      const ownership = state.owner.ownershipReceipt ? await store.readImmutableFact(state.owner.ownershipReceipt) as any : null;
      const repairable = previous && state.owner.ownerEpoch === binding.ownerEpoch + 1 && previous.owner.ownerEpoch === binding.ownerEpoch
        && previous.owner.ownershipReceipt === binding.ownershipReceiptHash && previous.owner.sessionId === binding.sessionId
        && ownership?.kind === "ownership" && ["same_manager", "dead"].includes(ownership.disposition) && ownership.priorOwnershipReceiptHash === previous.owner.ownershipReceipt
        && ownership.successorSessionId === state.owner.sessionId && ownership.successorPid === state.owner.pid
        && ownership.successorProcessStartIdentity === state.owner.processStartIdentity && ownership.successorLockIdentity === state.owner.lockIdentity;
      if (!repairable) throw new Error("Session binding conflicts with run authority");
      const lineageKind = ownership.disposition === "same_manager" ? "same_manager_resume" : "explicit_reattach";
      binding = await this.#createBinding(ctx, repositoryRoot, plan, state, state.updatedAt, { kind: lineageKind, priorBindingHash: binding.bindingHash, priorSessionId: binding.sessionId, proofHash: ownership.hash }, binding);
      await this.#refreshStartIntentBinding(repositoryRoot, binding.lineage.priorSessionId ?? sessionId, sessionId, runId, binding.bindingHash);
    }
    return { binding, plan, context: effectiveContext, store, state };
  }

  async #currentOwnerLock(loaded: LoadedRunV1): Promise<DagRunStoreLockIdentityV1> {
    const start = await currentProcessStartIdentity();
    const owner = loaded.state.owner;
    if (owner.sessionId !== loaded.binding.sessionId || owner.pid !== process.pid || owner.processStartIdentity !== start || !owner.lockIdentity || !owner.ownerTokenHash) throw new Error("Current conductor service does not own the exact DAG epoch; proven-dead recovery is required");
    const current = this.#currentLock.get(loaded.state.runId);
    if (current && current.lockIdentity === owner.lockIdentity && current.ownerTokenHash === owner.ownerTokenHash && current.sessionId === owner.sessionId && current.pid === process.pid && current.processStartIdentity === start) return current;
    const derived = dagRunStoreLockIdentityFromOwner(owner);
    this.#currentLock.set(loaded.state.runId, derived);
    return derived;
  }

  async #ensureOperationalOwner(ctx: DagConductorContextV1, runId: string, occurredAt: string): Promise<void> {
    const loaded = await this.#loadBound(ctx, runId, true);
    const repositoryRoot = await realpath(ctx.cwd);
    const sessionId = String(ctx.sessionManager.getSessionId());
    await this.#refreshStartIntentBinding(repositoryRoot, loaded.binding.lineage.priorSessionId ?? sessionId, sessionId, runId, loaded.binding.bindingHash);
    await this.#currentOwnerLock(loaded);
  }

  async #refreshStartIntentBinding(repositoryRoot: string, sourceSessionId: string, targetSessionId: string, runId: string, bindingHash: string): Promise<void> {
    const sourcePath = startIntentPath(repositoryRoot, sourceSessionId, runId);
    const intent = await readStartIntent(sourcePath, sourceSessionId, runId);
    if (!intent) {
      if (await requiresPreparedStartIntent(repositoryRoot, RUN_ROOT, runId)) throw new Error("Current product run is missing its exact prepared-start identity");
      return; // Historical low-level canonical runs predate prepared-start intents.
    }
    if (intent.state !== "active") throw new Error("DAG owner recovery requires the exact active prepared-start intent");
    if (sourceSessionId === targetSessionId) {
      if (intent.bindingHash === bindingHash) return;
      await replaceCanonicalJson(sourcePath, intent, { ...intent, revision: intent.revision + 1, bindingHash });
      return;
    }
    const targetPath = startIntentPath(repositoryRoot, targetSessionId, runId);
    const successor = { ...intent, sessionId: targetSessionId, revision: intent.revision + 1, bindingHash };
    await publishImmutableJson(targetPath, successor);
    const exact = await readStartIntent(targetPath, targetSessionId, runId);
    if (!exact || canonicalStringify(exact) !== canonicalStringify(successor)) throw new Error("DAG successor start identity did not publish exact bytes");
  }

  async #createBinding(ctx: DagConductorContextV1, repositoryRoot: string, plan: CanonicalDagPlanV1, state: DagRunStateV1, at: string, lineage: DagSessionRunBindingV1["lineage"] = { kind: "start", priorBindingHash: null, priorSessionId: null, proofHash: null }, expectedExisting: DagSessionRunBindingV1 | null = null): Promise<DagSessionRunBindingV1> {
    const sessionId = String(ctx.sessionManager.getSessionId()); const sessionFile = ctx.sessionManager.getSessionFile?.() ?? null; const git = await repositoryGitBinding(repositoryRoot);
    const core = { schemaVersion: 1 as const, kind: "DagSessionRunBindingV1" as const, sessionId, sessionFileHash: sessionFile ? canonicalHash(await realpath(sessionFile).catch(() => sessionFile)) : null, repositoryRootHash: canonicalHash(repositoryRoot), commonDirIdentityHash: git.commonDirIdentityHash, branchRef: git.branchRef, runId: state.runId, runNonceHash: canonicalHash(state.runNonce), planHash: plan.planHash, ownerEpoch: state.owner.ownerEpoch, ownershipReceiptHash: state.owner.ownershipReceipt!, lineage, storeRoot: RUN_ROOT, boundAt: at };
    const binding = { ...core, bindingHash: canonicalHash(core) };
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), state.runId);
    await store.putImmutableFact(binding);
    if (expectedExisting) await replaceCanonicalJson(bindingPath(repositoryRoot, sessionId), expectedExisting, binding);
    else await publishImmutableJson(bindingPath(repositoryRoot, sessionId), binding);
    return binding;
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

function compareSemanticActions(left: DagSemanticActionV1, right: DagSemanticActionV1): number { return left.operation.localeCompare(right.operation) || (left.workItemId ?? "").localeCompare(right.workItemId ?? "") || (left.stage ?? "").localeCompare(right.stage ?? "") || left.actionId.localeCompare(right.actionId); }

function retryIsAdmissible(state: DagRunStateV1, retry: DagRunStateV1["retryLedger"][string]): boolean {
  const item = state.workItems[retry.workItemId];
  return state.desired.run === "running" && ["active", "integration"].includes(state.current.run) && state.completion.state === "open"
    && !Object.values(state.cancellations).some((candidate) => candidate.state !== "closed")
    && Boolean(item) && !["complete", "cancelled", "superseded"].includes(item.current)
    && retry.stop === "none" && retry.count < retry.ceiling && semanticRetryFailureCount(state, retry) > retry.count
    && !Object.values(state.effects).some((effect) => effect.subject.kind === "work_item" && effect.subject.id === retry.workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation));
}

function semanticRetryFailureCount(state: DagRunStateV1, retry: DagRunStateV1["retryLedger"][string]): number {
  const stage = state.workItems[retry.workItemId]?.stages[retry.stage];
  const failedAttempts = stage?.state === "blocked" ? stage.attemptIds.length : 0;
  const integrationConflicts = Object.values(state.integrationAttempts).filter((attempt) => attempt.conflictClass !== "none" && Object.values(state.integrationTrains).some((train) => train.entries[attempt.entryId]?.workItemId === retry.workItemId)).length;
  return Math.max(retry.failureSequence.length, failedAttempts, retry.dimension === "integration" ? integrationConflicts : 0);
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  if (typeof signal.throwIfAborted === "function") signal.throwIfAborted();
  throw signal.reason instanceof Error ? signal.reason : new DOMException("The operation was aborted", "AbortError");
}

function semanticGuard(state: DagRunStateV1, operation: string, subject: string, occurredAt: string): DagMutationGuardV1 {
  const identity = canonicalHash({ runId: state.runId, runNonce: state.runNonce, operation, subject, revision: state.revision, snapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch });
  return { runId: state.runId, runNonce: state.runNonce, expectedRevision: state.revision, expectedSnapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch, commandId: `semantic-${operation}-${identity.slice(7, 31)}`, idempotencyKey: `semantic:${operation}:${identity}`, occurredAt };
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

async function validateBindingAuthority(binding: DagSessionRunBindingV1, repositoryRoot: string, plan: CanonicalDagPlanV1, state: DagRunStateV1): Promise<void> {
  await validateBindingRepository(binding, repositoryRoot);
  if (binding.sessionId !== state.owner.sessionId || binding.runId !== state.runId || binding.runNonceHash !== canonicalHash(state.runNonce) || binding.planHash !== plan.planHash || binding.ownerEpoch !== state.owner.ownerEpoch || binding.ownershipReceiptHash !== state.owner.ownershipReceipt) throw new Error("Prior session binding conflicts with pre-transfer authority");
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
async function requiresPreparedStartIntent(root: string, storeRoot: string, runId: string): Promise<boolean> {
  const plan = parseCanonicalDagPlanV1(await readFile(join(root, storeRoot, runId, "authority", "plan.json"), "utf8"));
  return plan.generator.name === "thin-plan-runtime-adapter";
}
function bindingPath(root: string, sessionId: string): string { return join(root, BINDING_ROOT, `${createHash("sha256").update(sessionId).digest("hex")}.json`); }
function conductorFaultPath(root: string, runId: string): string { if (!ID_RE.test(runId)) throw new Error("Invalid conductor fault run identity"); return join(root, CONDUCTOR_FAULT_ROOT, `${runId}.json`); }
function readDurableConductorFault(path: string, runId: string): any {
  const text = readFileSync(path, "utf8"); const value = parseStrictJson(text) as any;
  if (canonicalStringify(value) !== text || value?.schemaVersion !== 1 || value?.kind !== "DagConductorFaultV1" || value?.runId !== runId || !HASH_RE.test(value?.hash ?? "")) throw new Error(`Invalid durable conductor fault marker for ${runId}`);
  const core = Object.fromEntries(Object.entries(value).filter(([key]) => key !== "hash"));
  if (canonicalHash(core) !== value.hash) throw new Error(`Durable conductor fault marker hash mismatch for ${runId}`);
  return value;
}
function publishImmutableJsonSync(path: string, value: unknown): void {
  const text = canonicalStringify(value); mkdirSync(dirname(path), { recursive: true });
  try { readDurableConductorFault(path, (value as any).runId); return; } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  const temporary = `${path}.tmp-${process.pid}-${randomUUID()}`; let handle: number | null = null;
  try {
    handle = openSync(temporary, "wx", 0o600); writeFileSync(handle, text); fsyncSync(handle); closeSync(handle); handle = null;
    try { linkSync(temporary, path); } catch (error: any) { if (error?.code !== "EEXIST") throw error; readDurableConductorFault(path, (value as any).runId); }
  } finally {
    if (handle !== null) closeSync(handle);
    try { unlinkSync(temporary); } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  }
  syncDirectorySync(dirname(path));
}
function syncDirectorySync(path: string): void { const handle = openSync(path, "r"); try { fsyncSync(handle); } finally { closeSync(handle); } }
function startIntentPath(root: string, sessionId: string, runId: string): string { return join(root, START_INTENT_ROOT, createHash("sha256").update(sessionId).digest("hex"), `${runId}.json`); }

async function readStartIntent(path: string, sessionId: string, runId: string): Promise<DagRunStartIntentV1 | null> {
  try {
    const text = await readFile(path, "utf8"); const value = parseStrictJson(text) as unknown as DagRunStartIntentV1;
    if (canonicalStringify(value) !== text || value.schemaVersion !== 1 || value.kind !== "DagRunStartIntentV1" || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.startId) || !["starting", "active"].includes(value.state) || !Number.isInteger(value.revision) || value.revision < 0 || value.sessionId !== sessionId || value.runId !== runId || !ID_RE.test(value.runId) || value.runNonce.length < 16 || !HASH_RE.test(value.planHash) || !HASH_RE.test(value.preparedHash) || (value.sourcePlanningPlanId === null) !== (value.sourcePlanningPlanHash === null) || (value.sourcePlanningPlanId !== null && (!ID_RE.test(value.sourcePlanningPlanId) || !HASH_RE.test(value.sourcePlanningPlanHash!))) || (value.bindingHash !== null && !HASH_RE.test(value.bindingHash)) || (value.state === "starting" ? value.revision !== 0 || value.bindingHash !== null : value.revision < 1 || value.bindingHash === null)) throw new Error("DAG prepared start intent is corrupt");
    return value;
  } catch (error: any) { if (error?.code === "ENOENT") return null; throw error; }
}

function assertExactStartReplay(intent: DagRunStartIntentV1, expected: Pick<DagRunStartIntentV1, "runNonce" | "planHash" | "preparedHash" | "sourcePlanningPlanId" | "sourcePlanningPlanHash">): void {
  if (intent.runNonce !== expected.runNonce || intent.planHash !== expected.planHash || intent.preparedHash !== expected.preparedHash || intent.sourcePlanningPlanId !== expected.sourcePlanningPlanId || intent.sourcePlanningPlanHash !== expected.sourcePlanningPlanHash) throw new Error("Prepared DAG start replay does not match the exact durable start intent");
}

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

async function replaceCanonicalJson(path: string, expected: unknown, value: unknown): Promise<void> {
  const expectedText = canonicalStringify(expected); const text = canonicalStringify(value); const directory = dirname(path);
  await mkdir(directory, { recursive: true });
  const claim = `${path}.${process.pid}.${randomUUID()}.expected`; const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await link(path, claim);
    const [currentInfo, claimInfo, currentText, claimText] = await Promise.all([stat(path), stat(claim), readFile(path, "utf8"), readFile(claim, "utf8")]);
    if (currentInfo.dev !== claimInfo.dev || currentInfo.ino !== claimInfo.ino || currentText !== expectedText || claimText !== expectedText) throw new Error("DAG session binding changed before successor publication");
    const handle = await open(temp, "wx"); try { await handle.writeFile(text); await handle.sync(); } finally { await handle.close(); }
    const [beforeInfo, beforeText] = await Promise.all([stat(path), readFile(path, "utf8")]);
    if (beforeInfo.dev !== claimInfo.dev || beforeInfo.ino !== claimInfo.ino || beforeText !== expectedText) throw new Error("DAG session binding changed during successor publication");
    await rename(temp, path); await syncDirectory(directory);
  } finally { await rm(temp, { force: true }); await rm(claim, { force: true }); }
}
async function syncDirectory(path: string): Promise<void> { const handle = await open(path, "r"); try { await handle.sync(); } finally { await handle.close(); } }
