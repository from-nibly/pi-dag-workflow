import { createHash, randomUUID } from "node:crypto";
import { closeSync, fsyncSync, linkSync, mkdirSync, openSync, readFileSync, realpathSync, unlinkSync, writeFileSync } from "node:fs";
import { link, mkdir, open, readFile, readdir, realpath, rename, rm, stat } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { canonicalHash, canonicalStringify, parseStrictJson } from "./common.ts";
import { DagLifecycleRuntimeV1, type DagIntegrationReconciliationAdapterV1, type DagLifecycleRuntimeOptionsV1 } from "./lifecycle-runtime.ts";
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
const SERVICE_REGISTRY_KEY = Symbol.for("pi-dag-workflow.canonical-conductor-services-v1");
const conductorServiceRegistry: Map<string, { serviceId: string; lockIdentity: string }> = (globalThis as any)[SERVICE_REGISTRY_KEY] ??= new Map();

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
  readonly lifecycle: DagLifecycleRuntimeOptionsV1;
  readonly integrationFactory?: (input: { store: DagRunSnapshotStoreV1; context: DagRunValidationContextV1; lock: DagRunStoreLockIdentityV1 }) => DagIntegrationReconciliationAdapterV1;
  readonly startFailpoint?: (point: DagPreparedStartFailpointV1) => Promise<void> | void;
  readonly ownerResumeFailpoint?: (point: DagOwnerResumeFailpointV1) => Promise<void> | void;
  readonly pumpFailpoint?: (point: DagConductorPumpFailpointV1, detail?: { occurredAt: string; wakeGeneration: number }) => Promise<void> | void;
  readonly onPumpError?: (input: { runId: string; error: Error }) => Promise<void> | void;
  #serviceId = randomUUID();
  #currentLock = new Map<string, DagRunStoreLockIdentityV1>();
  #ownedServiceKeys = new Set<string>();
  #activeContexts = new Map<string, DagConductorContextV1>();
  #wakeGenerations = new Map<string, number>();
  #wakeTimes = new Map<string, string>();
  #pumps = new Map<string, Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }>>();
  #faults = new Map<string, Error>();
  #detaching = false;
  #detachPromise: Promise<void> | null = null;
  #lastGood = new Map<string, { state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV2; cachedAt: string }>();
  constructor(options: { workerProjection?: (bindings: Array<{ workerStorageId: string; launchOwnerSessionId: string; workerId: string; attemptNumber: number; attemptNonce: string; configHash: string; resultHash: string | null; processDisposition: string }>) => Promise<DagWorkerProjectionInputV1 | null>; dispatchEffect?: (effect: { effectId: string; kind: string; requestHash: string }, state: DagRunStateV1) => Promise<void>; lifecycle?: DagLifecycleRuntimeOptionsV1; integrationFactory?: (input: { store: DagRunSnapshotStoreV1; context: DagRunValidationContextV1; lock: DagRunStoreLockIdentityV1 }) => DagIntegrationReconciliationAdapterV1; startFailpoint?: (point: DagPreparedStartFailpointV1) => Promise<void> | void; ownerResumeFailpoint?: (point: DagOwnerResumeFailpointV1) => Promise<void> | void; pumpFailpoint?: (point: DagConductorPumpFailpointV1, detail?: { occurredAt: string; wakeGeneration: number }) => Promise<void> | void; onPumpError?: (input: { runId: string; error: Error }) => Promise<void> | void } = {}) { this.workerProjection = options.workerProjection; this.dispatchEffect = options.dispatchEffect; this.lifecycle = options.lifecycle ?? {}; this.integrationFactory = options.integrationFactory; this.startFailpoint = options.startFailpoint; this.ownerResumeFailpoint = options.ownerResumeFailpoint; this.pumpFailpoint = options.pumpFailpoint; this.onPumpError = options.onPumpError; }

  /** Drain this exact service generation before releasing its process-local ownership fence. */
  detach(): Promise<void> {
    if (this.#detachPromise) return this.#detachPromise;
    this.#detaching = true;
    this.#activeContexts.clear();
    this.#detachPromise = (async () => {
      await Promise.allSettled([...this.#pumps.values()]);
      for (const key of this.#ownedServiceKeys) if (conductorServiceRegistry.get(key)?.serviceId === this.#serviceId) conductorServiceRegistry.delete(key);
      this.#ownedServiceKeys.clear(); this.#wakeGenerations.clear(); this.#wakeTimes.clear(); this.#currentLock.clear(); this.#faults.clear();
    })();
    return this.#detachPromise;
  }

  /** Coalesce one service-owned advancement pump and fence stale same-process service generations first. */
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
      let result: { state: DagRunStateV1; decision: DagSchedulerDecisionV1 };
      for (;;) {
        observedWakeGeneration = this.#wakeGenerations.get(runId)!;
        observedWakeTime = this.#wakeTimes.get(runId)!;
        await this.#ensureOperationalOwner(ctx, runId, observedWakeTime);
        result = await this.advance(ctx, runId, observedWakeTime);
        // Give terminal-result microtasks queued at the waiting boundary a chance
        // to mark this run dirty before this pass settles.
        await Promise.resolve();
        if (this.#wakeGenerations.get(runId) === observedWakeGeneration) {
          await this.pumpFailpoint?.("after_quiescent_check", { occurredAt: observedWakeTime, wakeGeneration: observedWakeGeneration });
          return { result, settledWakeGeneration: observedWakeGeneration };
        }
      }
    })();
    let pump!: Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1 }>;
    pump = body.then(
      ({ result, settledWakeGeneration }) => {
        if (this.#pumps.get(runId) === pump) this.#pumps.delete(runId);
        if (["completed", "cancelled", "superseded"].includes(result.state.current.run)) this.#activeContexts.delete(runId);
        if (!this.#detaching && this.#activeContexts.has(runId) && this.#wakeGenerations.get(runId) !== settledWakeGeneration) return this.#startPump(ctx, runId);
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
    const prepared = await this.startPrepared(ctx, { runId: input.runId, runNonce: input.runNonce, planHash: input.planHash, maxActiveNodes: input.maxActiveNodes, occurredAt: input.occurredAt, plan, genesis, context, seedFacts });
    const advanced = await this.activate(ctx, prepared.state.runId, input.occurredAt);
    return { binding: prepared.binding, state: advanced.state, decision: advanced.decision };
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
    this.#claimService(repositoryRoot, input.runId, await this.#currentOwnerLock({ binding, plan, context: runtimeContext, store, state }));
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

  async status(ctx: DagConductorContextV1, runId: string): Promise<{ state: DagRunStateV1; decision: DagSchedulerDecisionV1; projection: DagExecutionProjectionV2; stale: null | { sourceRevision: number; sourceSnapshotHash: string; newerObservedRevision: number; cachedAt: string } }> {
    const loaded = await this.#loadBound(ctx, runId); const cacheKey = canonicalHash({ repositoryRootHash: loaded.binding.repositoryRootHash, sessionId: loaded.binding.sessionId, bindingHash: loaded.binding.bindingHash, runId: loaded.state.runId, runNonceHash: loaded.binding.runNonceHash, planHash: loaded.plan.planHash }); let newerObservedRevision = loaded.state.revision;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const first = await loaded.store.read(loaded.context);
      const exactBindings = Object.values(first.workerBindings).map(({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash, processDisposition }) => ({ workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash, resultHash, processDisposition })).sort((a, b) => a.workerStorageId.localeCompare(b.workerStorageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber);
      const workers = await this.workerProjection?.(exactBindings) ?? null;
      const decision = scheduleDagRunV1(loaded.plan, first);
      const projection = projectDagExecutionV2(loaded.plan, first, decision, workers);
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
    const repositoryRoot = await realpath(ctx.cwd); const sessionId = String(ctx.sessionManager.getSessionId()); const processStartIdentity = await currentProcessStartIdentity();
    if (loaded.binding.ownerEpoch === loaded.state.owner.ownerEpoch && loaded.binding.ownershipReceiptHash === loaded.state.owner.ownershipReceipt) {
      await this.#refreshStartIntentBinding(repositoryRoot, loaded.binding.lineage?.priorSessionId ?? loaded.state.owner.sessionId, sessionId, guard.runId, loaded.binding.bindingHash);
    }
    if (loaded.state.runNonce === guard.runNonce && loaded.state.owner.ownerEpoch === guard.ownerEpoch + 1 && loaded.state.owner.sessionId === sessionId && loaded.state.owner.pid === process.pid && loaded.state.owner.processStartIdentity === processStartIdentity) {
      const ownership = loaded.state.owner.ownershipReceipt ? await loaded.store.readImmutableFact(loaded.state.owner.ownershipReceipt) as any : null;
      const previousPath = loaded.state.previousSnapshotHash ? join(loaded.store.snapshotsDirectory, `${loaded.state.previousSnapshotHash.slice("sha256:".length)}.json`) : null;
      const previous = previousPath ? parseStrictJson(await readFile(previousPath, "utf8")) as DagRunStateV1 : null;
      if (ownership?.kind === "ownership" && ownership.ownerEpoch === loaded.state.owner.ownerEpoch && ownership.priorSessionId !== null && previous?.revision === guard.expectedRevision && previous.snapshotHash === guard.expectedSnapshotHash && previous.owner.ownerEpoch === guard.ownerEpoch) {
        const lock = dagRunStoreLockIdentityFromOwner(loaded.state.owner); this.#currentLock.set(guard.runId, lock); this.#claimService(repositoryRoot, guard.runId, lock); return loaded.state;
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
    this.#claimService(repositoryRoot, guard.runId, newLock);
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

  #serviceKey(repositoryRoot: string, runId: string): string { return canonicalHash({ repositoryRoot, runId }); }

  #claimService(repositoryRoot: string, runId: string, lock: DagRunStoreLockIdentityV1): void {
    const key = this.#serviceKey(repositoryRoot, runId);
    const current = conductorServiceRegistry.get(key);
    if (current && current.serviceId !== this.#serviceId && current.lockIdentity === lock.lockIdentity) throw new Error("Another conductor service generation is still operational for this exact run");
    conductorServiceRegistry.set(key, { serviceId: this.#serviceId, lockIdentity: lock.lockIdentity });
    this.#ownedServiceKeys.add(key);
  }

  async #currentOwnerLock(loaded: LoadedRunV1): Promise<DagRunStoreLockIdentityV1> {
    const current = this.#currentLock.get(loaded.state.runId);
    const start = await currentProcessStartIdentity();
    if (!current || current.lockIdentity !== loaded.state.owner.lockIdentity || current.ownerTokenHash !== loaded.state.owner.ownerTokenHash || current.sessionId !== loaded.state.owner.sessionId || current.pid !== process.pid || current.processStartIdentity !== start) throw new Error("Current conductor service does not own the exact DAG epoch; activate or reattach the run");
    return current;
  }

  async #ensureOperationalOwner(ctx: DagConductorContextV1, runId: string, occurredAt: string): Promise<void> {
    let loaded = await this.#loadBound(ctx, runId, true);
    const repositoryRoot = await realpath(ctx.cwd);
    const sessionId = String(ctx.sessionManager.getSessionId());
    await this.#refreshStartIntentBinding(repositoryRoot, loaded.binding.lineage.priorSessionId ?? sessionId, sessionId, runId, loaded.binding.bindingHash);
    const serviceKey = this.#serviceKey(repositoryRoot, runId);
    const registered = conductorServiceRegistry.get(serviceKey);
    const cached = this.#currentLock.get(runId);
    if (cached && cached.lockIdentity === loaded.state.owner.lockIdentity && cached.ownerTokenHash === loaded.state.owner.ownerTokenHash) {
      if (registered && registered.serviceId !== this.#serviceId && registered.lockIdentity === cached.lockIdentity) throw new Error("Another conductor service generation is still operational for this exact run");
      this.#claimService(repositoryRoot, runId, cached);
      await this.#currentOwnerLock(loaded);
      return;
    }
    if (registered && registered.serviceId !== this.#serviceId && registered.lockIdentity === loaded.state.owner.lockIdentity) throw new Error("Another conductor service generation is still operational for this exact run");
    const processStartIdentity = await currentProcessStartIdentity();
    if (loaded.state.owner.sessionId !== sessionId || loaded.state.owner.pid !== process.pid || loaded.state.owner.processStartIdentity !== processStartIdentity) throw new Error("DAG owner belongs to another live process or session; use exact proven-dead reattachment");

    const prior = loaded.state.owner;
    const priorLock = dagRunStoreLockIdentityFromOwner(prior);
    const newLock = await processLockIdentity(sessionId, canonicalHash({ purpose: "dag-run-service-resume", sessionId, runId, ownerEpoch: prior.ownerEpoch + 1, nonce: randomUUID() }), canonicalHash({ purpose: "dag-run-owner-token", sessionId, runId, ownerEpoch: prior.ownerEpoch + 1, nonce: randomUUID() }), occurredAt);
    const lineageHash = canonicalHash({
      kind: "direct_owner_transfer", runId: loaded.state.runId, runNonce: loaded.state.runNonce,
      priorSessionId: prior.sessionId, priorOwnerTokenHash: prior.ownerTokenHash, priorPid: prior.pid,
      priorProcessStartIdentity: prior.processStartIdentity, priorLockIdentity: prior.lockIdentity,
      successorSessionId: sessionId, successorPid: newLock.pid, successorProcessStartIdentity: newLock.processStartIdentity,
      successorLockIdentity: newLock.lockIdentity,
    });
    const priorOwnership = prior.ownershipReceipt ? await loaded.store.readImmutableFact(prior.ownershipReceipt) as any : null;
    const ownershipCore = {
      kind: "ownership" as const, runId: loaded.state.runId, runNonce: loaded.state.runNonce,
      priorSessionId: prior.sessionId, priorOwnerTokenHash: prior.ownerTokenHash, priorPid: prior.pid,
      priorProcessStartIdentity: prior.processStartIdentity, priorLockIdentity: prior.lockIdentity, priorAttachedAt: prior.attachedAt,
      disposition: "same_manager" as const, priorObservationHash: null, priorOwnershipReceiptHash: prior.ownershipReceipt,
      ownerEpoch: prior.ownerEpoch + 1, successorSessionId: sessionId, successorPid: newLock.pid,
      successorProcessStartIdentity: newLock.processStartIdentity, successorLockIdentity: newLock.lockIdentity, lineageHash,
    };
    const ownershipWithChain = { ...ownershipCore, chainHash: ownershipChainHashV1(ownershipCore, priorOwnership?.kind === "ownership" ? priorOwnership.chainHash : null) };
    const ownership = { ...ownershipWithChain, hash: canonicalHash(ownershipWithChain) };
    await loaded.store.putImmutableFact(ownership);
    const context = { ...loaded.context, facts: { ...loaded.context.facts, [ownership.hash]: ownership } };
    const payload = { ownerTokenHash: newLock.ownerTokenHash, sessionId, pid: newLock.pid, processStartIdentity: newLock.processStartIdentity, lockIdentity: newLock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "same_manager" };
    const transferred = await loaded.store.mutate({ input: reducerInput(loaded.state, "transfer_owner", "command", payload, occurredAt, { commandId: `service-resume-${prior.ownerEpoch + 1}-${ownership.hash.slice(7, 19)}`, idempotencyKey: `service-resume:${loaded.state.runNonce}:${prior.ownerEpoch + 1}` }), context, lock: priorLock });
    if (!transferred.accepted) throw new Error(`DAG same-manager service recovery rejected: ${transferred.code}: ${transferred.message}`);
    await this.ownerResumeFailpoint?.("after_owner_transfer");
    this.#currentLock.set(runId, newLock);
    this.#claimService(repositoryRoot, runId, newLock);
    const binding = await this.#createBinding(ctx, repositoryRoot, loaded.plan, transferred.state, occurredAt, { kind: "same_manager_resume", priorBindingHash: loaded.binding.bindingHash, priorSessionId: sessionId, proofHash: ownership.hash }, loaded.binding);
    await this.ownerResumeFailpoint?.("after_owner_binding");
    await this.#refreshStartIntentBinding(repositoryRoot, sessionId, sessionId, runId, binding.bindingHash);
    await this.ownerResumeFailpoint?.("after_owner_start_identity");
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
