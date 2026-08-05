import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, realpath, rename, stat } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  MAX_COMPLETION_MESSAGE_BYTES,
  MAX_MAILBOX_BYTES,
  MAX_RESULT_BYTES,
  MAX_STATE_BYTES,
  WorkerSessionStore,
  assertAttemptConfig,
  assertTerminalResult,
  attemptPaths,
  createWorkerSession,
  listDirectories,
  newNonce,
  normalizeRuntimeId,
  nowIso,
  pathExists,
  processGroupStatus,
  processIdentityStatus,
  processesUsingWorkingRoot,
  processesWithEnvironmentBinding,
  processStartIdentity,
  uninspectableSameUidProcesses,
  readFileHead,
  readFileTail,
  readJson,
  sha256,
  withConfigHash,
  withResultHash,
  workerArchivePath,
  workerSessionPath,
  workerSessionsRoot,
  writeImmutableJson,
} from "./core.mjs";

const TERMINAL_STATUSES = new Set(["succeeded", "needs_attention", "failed", "cancelled", "lost"]);
const extensionPath = resolve(dirname(fileURLToPath(import.meta.url)), "..", "index.ts");
const defaultSupervisorPath = resolve(dirname(fileURLToPath(import.meta.url)), "supervisor.mjs");

export class WorkerManager {
  constructor(pi, options = {}) {
    this.pi = pi;
    this.options = options;
    this.store = null;
    this.context = null;
    this.timer = null;
    this.timerScanPending = false;
    this.scanning = false;
    this.scanQueue = Promise.resolve();
    this.attached = false;
    this.processStartIdentity = null;
    this.cancellationTimers = new Map();
    this.dispatchingAttempts = new Set();
  }

  async attach(ctx) {
    await this.detach();
    this.context = ctx;
    const repositoryRoot = resolve(ctx.cwd);
    const sessionId = String(ctx.sessionManager.getSessionId());
    const sessionFile = ctx.sessionManager.getSessionFile?.() ?? null;
    const header = ctx.sessionManager.getHeader?.() ?? {};
    const parentSessionFile = header?.parentSession ?? null;
    const parentSessionId = parentSessionFile ? await sessionIdFromFile(parentSessionFile) : null;
    this.processStartIdentity = await processStartIdentity();
    if (!this.processStartIdentity) throw new Error("Cannot prove top-level process start identity");

    const owned = await findOwnedStores(repositoryRoot, sessionId);
    if (owned.length > 1) throw new Error(`Multiple worker sessions claim top-level session ${sessionId}`);
    let store = owned[0] ?? null;
    if (!store && parentSessionId) store = await this.#transferDirectParent(repositoryRoot, parentSessionId, sessionId, sessionFile);
    if (!store) {
      store = new WorkerSessionStore(repositoryRoot, sessionId);
      const owner = this.#owner(sessionId);
      await store.initialize(createWorkerSession({ sessionId, repositoryRoot, sessionFile, owner }));
    }
    this.store = store;
    await this.#claimOwnership(sessionId, sessionFile);
    await this.#migrateLegacyLaunchBindings(ctx);
    this.attached = true;
    await this.scan();
    await this.dispatchNext();
    this.timer = setInterval(() => {
      if (this.timerScanPending) return;
      this.timerScanPending = true;
      this.options.onTimerScanRequested?.();
      void this.scan().catch((error) => { console.error(`Worker scan failed: ${error.message}`); }).finally(() => { this.timerScanPending = false; });
    }, this.options.watchIntervalMs ?? 1000);
    this.timer.unref?.();
    return this.summary();
  }

  async detach() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.timerScanPending = false;
    for (const timer of this.cancellationTimers.values()) clearTimeout(timer);
    this.cancellationTimers.clear();
    this.attached = false;
    const store = this.store;
    await this.scanQueue.catch(() => {});
    if (store?.queue) await store.queue;
    this.context = null;
    this.store = null;
  }

  async launch(input, ctx = this.context) {
    this.#assertAttached();
    const state = await this.store.load();
    const request = await this.#normalizeLaunchRequest(input, ctx, state);
    const requestHash = sha256(request);
    const launchKey = normalizeLaunchKey(input.launchKey ?? `manual-${newNonce()}`);
    const candidateWorkerId = normalizeRuntimeId(input.workerId ?? `worker-${requestHash.slice(7, 19)}-${newNonce(5)}`, "workerId");
    const reserved = await this.store.mutate((draft) => {
      draft.launchRecords ??= [];
      const existing = draft.launchRecords.find((record) => record.launchKey === launchKey);
      if (existing) {
        if (existing.requestHash !== requestHash) throw new Error(`Launch key conflict: ${launchKey}`);
        const worker = draft.workers[existing.workerId];
        if (!worker) {
          if (existing.archivedWorkerPath) return { workerId: existing.workerId, existing: true, archived: true, attemptNumber: existing.archivedCurrentAttempt, status: existing.archivedStatus };
          throw new Error(`Launch reservation ${launchKey} references a missing worker`);
        }
        return { workerId: existing.workerId, existing: true };
      }
      assertWorkingRootApprovalState(draft, request);
      if (draft.workers[candidateWorkerId]) throw new Error(`Worker already exists: ${candidateWorkerId}`);
      const maxLaunchRecords = Math.max(1, Number(this.options.maxLaunchRecords ?? 4096));
      if (draft.launchRecords.length >= maxLaunchRecords) throw new Error(`Worker session reached the retained launch-record limit of ${maxLaunchRecords}`);
      const activeWorkers = Object.values(draft.workers).filter((worker) => !TERMINAL_STATUSES.has(worker.status)).length;
      if (activeWorkers >= Math.max(1, Number(this.options.maxActiveWorkers ?? 64))) throw new Error("Worker session reached its active-worker retention bound");
      if (Buffer.byteLength(JSON.stringify(draft)) + Buffer.byteLength(JSON.stringify(request)) * 3 > MAX_STATE_BYTES * 0.75) throw new Error("Worker session retention budget cannot reserve another launch safely");
      const createdAt = nowIso();
      const label = String(input.label ?? candidateWorkerId).trim().slice(0, 256) || candidateWorkerId;
      draft.workers[candidateWorkerId] = {
        id: candidateWorkerId,
        label,
        task: request.task,
        cwd: request.cwd,
        status: "launching",
        createdAt,
        updatedAt: createdAt,
        currentAttempt: 0,
        attempts: [],
        launchKey,
        requestHash,
        normalizedRequest: request,
        launchOptions: {
          ...(request.provider ? { provider: request.provider } : {}),
          ...(request.model ? { model: request.model } : {}),
          ...(request.thinking ? { thinking: request.thinking } : {}),
          reportRepairAttempts: request.reportRepairAttempts,
        },
      };
      draft.launchRecords.push({ launchKey, requestHash, workerId: candidateWorkerId, reservedAt: createdAt });
      return { workerId: candidateWorkerId, existing: false };
    });
    const { workerId, existing } = reserved.result;
    if (reserved.result.archived) return { workerId, launchKey, attemptNumber: reserved.result.attemptNumber, status: reserved.result.status, asynchronous: true, idempotentReplay: true, archived: true };
    if (!existing) await this.#hitFailpoint("after_launch_reservation", { workerId, launchKey });
    const current = await this.store.load();
    const worker = current.workers[workerId];
    if (existing && worker.currentAttempt > 0) return { workerId, launchKey, attemptNumber: worker.currentAttempt, status: worker.status, asynchronous: true, idempotentReplay: true };
    return this.#launchAttempt(workerId, ctx, { initialOnly: true, launchKey, idempotentReplay: existing });
  }

  async approveDisposableWorkingRoot(cwd) {
    this.#assertAttached();
    const state = await this.store.load();
    const resolved = resolve(cwd);
    assertCwdWithin(state.repositoryRoot, resolved);
    const canonicalRoot = await realpath(resolved);
    if (canonicalRoot === await realpath(state.repositoryRoot)) throw new Error("Repository root cannot be approved as disposable");
    const approvedParents = this.options.approvedDisposableRootParents ?? [join(state.repositoryRoot, ".ai", "worker-roots")];
    let underApprovedParent = false;
    const canonicalRepositoryRoot = await realpath(state.repositoryRoot);
    for (const parent of approvedParents) {
      try {
        const canonicalParent = await realpath(resolve(parent));
        if ((canonicalParent === canonicalRepositoryRoot || isStrictDescendant(canonicalRepositoryRoot, canonicalParent)) && isStrictDescendant(canonicalParent, canonicalRoot)) underApprovedParent = true;
      }
      catch (error) { if (error?.code !== "ENOENT") throw error; }
    }
    if (!underApprovedParent) throw new Error("Disposable working root must be below an explicitly approved disposable-root parent");
    const info = await stat(canonicalRoot);
    if (!info.isDirectory()) throw new Error("Disposable working root must be a directory");
    const token = newNonce();
    const tokenHash = sha256(token);
    const approvalId = `disposable-root-${tokenHash.slice(7, 23)}`;
    await this.store.mutate((draft) => {
      draft.approvedDisposableRoots ??= [];
      const conflict = draft.approvedDisposableRoots.find((approval) => !approval.retiredAt && approval.realPath === canonicalRoot);
      if (conflict) throw new Error(`Disposable working root is already approved as ${conflict.approvalId}`);
      if (draft.approvedDisposableRoots.filter((approval) => !approval.retiredAt).length >= Math.max(1, Number(this.options.maxActiveDisposableRoots ?? 256))) throw new Error("Worker session reached its active disposable-root approval bound");
      draft.approvedDisposableRoots.push({ approvalId, tokenHash, path: resolved, realPath: canonicalRoot, dev: String(info.dev), ino: String(info.ino), ownerSessionId: draft.ownerSessionId, approvedByOwner: structuredClone(draft.owner), approvedAt: nowIso(), retiredAt: null });
    });
    return { approvalId, disposableRootToken: token, realPath: canonicalRoot };
  }

  async retireDisposableWorkingRoot(disposableRootToken) {
    this.#assertAttached();
    const tokenHash = sha256(String(disposableRootToken ?? ""));
    const retired = await this.store.mutate((draft) => {
      const approval = (draft.approvedDisposableRoots ?? []).find((candidate) => candidate.tokenHash === tokenHash && !candidate.retiredAt);
      if (!approval || approval.ownerSessionId !== draft.ownerSessionId || canonicalOwnerIdentity(approval.approvedByOwner) !== canonicalOwnerIdentity(draft.owner)) throw new Error("Disposable working-root approval is missing or belongs to another owner");
      const active = Object.values(draft.workers).some((worker) => worker.normalizedRequest?.workingRoot?.approvalId === approval.approvalId && !TERMINAL_STATUSES.has(worker.status));
      if (active) throw new Error("Disposable working root still has an active worker");
      approval.retiredAt = nowIso();
      const retiredApprovals = draft.approvedDisposableRoots.filter((candidate) => candidate.retiredAt).sort((left, right) => right.retiredAt.localeCompare(left.retiredAt));
      const retainedRetiredIds = new Set(retiredApprovals.slice(0, 256).map((candidate) => candidate.approvalId));
      draft.approvedDisposableRoots = draft.approvedDisposableRoots.filter((candidate) => !candidate.retiredAt || retainedRetiredIds.has(candidate.approvalId));
      return approval;
    });
    return { approvalId: retired.result.approvalId, retiredAt: retired.result.retiredAt };
  }

  async authorizeRetry(workerId) {
    this.#assertAttached();
    await this.#refreshProcessDisposition(workerId);
    const token = newNonce();
    const tokenHash = sha256(token);
    const authorization = await this.store.mutate(async (draft) => {
      const worker = draft.workers[workerId];
      if (!worker) throw new Error(`Unknown worker: ${workerId}`);
      const attempt = worker.attempts.find((candidate) => candidate.attemptNumber === worker.currentAttempt);
      if (!attempt || !TERMINAL_STATUSES.has(worker.status)) throw new Error(`Worker ${workerId} is not terminal`);
      const maxAttempts = Math.max(1, Number(this.options.maxAttemptsPerWorker ?? 32));
      if (worker.attempts.length >= maxAttempts) throw new Error(`Worker ${workerId} reached the retained attempt limit of ${maxAttempts}`);
      if (attempt.retrySafe !== true || attempt.processDisposition !== "dead") throw new Error(`Worker ${workerId} is not retry-safe`);
      await assertRetrySafeProcessFact(draft, worker, attempt);
      draft.retryAuthorizations ??= [];
      const existing = draft.retryAuthorizations.find((candidate) => candidate.workerId === workerId && candidate.attemptNumber === attempt.attemptNumber && !candidate.consumedAt);
      if (existing) throw new Error(`Worker ${workerId} already has an open retry authorization`);
      const record = { workerId, attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, tokenHash, authorizedBySessionId: draft.ownerSessionId, authorizedByOwner: structuredClone(draft.owner), authorizedAt: nowIso() };
      draft.retryAuthorizations.push(record);
      return record;
    });
    return { workerId, attemptNumber: authorization.result.attemptNumber, retryToken: token };
  }

  async retry(workerId, ctx = this.context, retryToken = null) {
    this.#assertAttached();
    const authorization = retryToken ? { retryToken } : await this.authorizeRetry(workerId);
    return this.#launchAttempt(workerId, ctx, { retryToken: authorization.retryToken });
  }

  async #launchAttempt(workerId, ctx, options = {}) {
    const before = await this.store.load();
    const workerBefore = before.workers[workerId];
    if (!workerBefore) throw new Error(`Unknown worker: ${workerId}`);
    if (!options.initialOnly && options.retryToken) {
      const replayAuthorization = (before.retryAuthorizations ?? []).find((candidate) => candidate.workerId === workerId && candidate.tokenHash === sha256(options.retryToken));
      if (replayAuthorization?.consumedAt && replayAuthorization.launchedAttemptNumber) {
        if (canonicalOwnerIdentity(replayAuthorization.authorizedByOwner) !== canonicalOwnerIdentity(before.owner)) throw new Error("Consumed retry authorization belongs to another owner");
        const replayAttempt = workerBefore.attempts.find((candidate) => candidate.attemptNumber === replayAuthorization.launchedAttemptNumber);
        if (!replayAttempt) throw new Error("Consumed retry authorization references a missing attempt");
        return { workerId, launchKey: workerBefore.launchKey, attemptNumber: replayAttempt.attemptNumber, status: workerBefore.currentAttempt === replayAttempt.attemptNumber ? workerBefore.status : replayAttempt.status, asynchronous: true, idempotentReplay: true };
      }
    }
    if (options.initialOnly && workerBefore.currentAttempt > 0) return { workerId, launchKey: workerBefore.launchKey, attemptNumber: workerBefore.currentAttempt, status: workerBefore.status, asynchronous: true, idempotentReplay: true };
    const attemptNumber = workerBefore.currentAttempt + 1;
    const attemptNonce = newNonce();
    const request = workerBefore.normalizedRequest ?? await this.#normalizeLaunchRequest({ task: workerBefore.task, cwd: workerBefore.cwd, ...workerBefore.launchOptions }, ctx, before);
    if (!Array.isArray(request.activeTools) || !request.workingRoot) throw new Error(`Worker ${workerId} has no verified reusable launch request`);
    await this.#assertWorkingRootCurrent(before, request);
    const uninspectableBaseline = await this.#observeUninspectableProcesses();
    if (uninspectableBaseline.status !== "observed") throw new Error("Cannot establish the pre-launch uninspectable-process baseline");
    const config = withConfigHash({
      schemaVersion: 1,
      storageId: before.storageId,
      ownerSessionId: before.ownerSessionId,
      launchKey: workerBefore.launchKey,
      requestHash: workerBefore.requestHash,
      launchOwner: this.#owner(before.ownerSessionId),
      uninspectableProcessBaseline: uninspectableBaseline.processes,
      workerId,
      label: workerBefore.label,
      attemptNumber,
      attemptNonce,
      repositoryRoot: before.repositoryRoot,
      cwd: workerBefore.cwd,
      task: workerBefore.task,
      activeTools: request.activeTools,
      reportRepairAttempts: workerBefore.launchOptions.reportRepairAttempts,
      piCliPath: request.piCliPath,
      extensionPath: request.extensionPath,
      ...(request.provider ? { provider: request.provider } : {}),
      ...(request.model ? { model: request.model } : {}),
      ...(request.thinking ? { thinking: request.thinking } : {}),
      createdAt: nowIso(),
    });
    assertAttemptConfig(config);
    const paths = attemptPaths(before.repositoryRoot, before.storageId, workerId, attemptNumber);
    const reservation = await this.store.mutate((draft) => {
      const current = draft.workers[workerId];
      if (!current) throw new Error(`Unknown worker: ${workerId}`);
      assertWorkingRootApprovalState(draft, request);
      if (options.initialOnly && current.currentAttempt > 0) return { attemptNumber: current.currentAttempt, existing: true };
      if (!options.initialOnly) {
        draft.retryAuthorizations ??= [];
        const tokenHash = sha256(options.retryToken);
        const tokenAuthorization = draft.retryAuthorizations.find((candidate) => candidate.workerId === workerId && candidate.tokenHash === tokenHash);
        if (tokenAuthorization?.consumedAt && tokenAuthorization.launchedAttemptNumber) {
          if (canonicalOwnerIdentity(tokenAuthorization.authorizedByOwner) !== canonicalOwnerIdentity(draft.owner)) throw new Error("Consumed retry authorization belongs to another owner");
          const launchedAttempt = current.attempts.find((candidate) => candidate.attemptNumber === tokenAuthorization.launchedAttemptNumber);
          if (!launchedAttempt) throw new Error("Consumed retry authorization references a missing attempt");
          return { attemptNumber: launchedAttempt.attemptNumber, existing: true };
        }
        const auth = draft.retryAuthorizations.find((candidate) => candidate.workerId === workerId && candidate.attemptNumber === current.currentAttempt && !candidate.consumedAt && candidate.tokenHash === tokenHash);
        if (!auth || auth.attemptNonce !== current.attempts.find((candidate) => candidate.attemptNumber === current.currentAttempt)?.attemptNonce || auth.authorizedBySessionId !== draft.ownerSessionId || canonicalOwnerIdentity(auth.authorizedByOwner) !== canonicalOwnerIdentity(draft.owner)) throw new Error("Retry authorization is missing, stale, or bound to another owner");
        auth.consumedAt = nowIso();
        auth.launchedAttemptNumber = attemptNumber;
      }
      if (current.currentAttempt !== attemptNumber - 1) throw new Error(`Worker ${workerId} attempt changed concurrently`);
      current.currentAttempt = attemptNumber;
      current.status = "launching";
      current.updatedAt = nowIso();
      current.attempts.push({
        attemptNumber,
        attemptNonce,
        configHash: config.configHash,
        configPath: relative(before.repositoryRoot, paths.config),
        launchKey: workerBefore.launchKey,
        requestHash: workerBefore.requestHash,
        launchOwner: config.launchOwner,
        uninspectableProcessBaseline: config.uninspectableProcessBaseline,
        config,
        status: "planned",
        processDisposition: "not_observed",
        retrySafe: false,
        launchSessionId: before.ownerSessionId,
        createdAt: config.createdAt,
      });
      const launchRecord = (draft.launchRecords ?? []).find((record) => record.workerId === workerId);
      if (launchRecord && !launchRecord.attemptNumber) launchRecord.attemptNumber = attemptNumber;
      return { attemptNumber, existing: false };
    });
    if (!reservation.result.existing) await this.#hitFailpoint("after_attempt_reservation", { workerId, attemptNumber });
    if (reservation.result.existing) {
      const state = await this.store.load();
      const current = state.workers[workerId];
      return { workerId, launchKey: current.launchKey, attemptNumber: current.currentAttempt, status: current.status, asynchronous: true, idempotentReplay: true };
    }
    await writeImmutableJson(paths.config, config);
    await this.#hitFailpoint("after_config_publication", { workerId, attemptNumber });
    return this.#dispatchReservedAttempt(workerId, attemptNumber);
  }

  async #dispatchReservedAttempt(workerId, attemptNumber) {
    const dispatchKey = `${workerId}:${attemptNumber}`;
    if (this.dispatchingAttempts.has(dispatchKey)) {
      const state = await this.store.load();
      const worker = state.workers[workerId];
      const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      return { workerId, attemptNumber, status: worker?.currentAttempt === attemptNumber ? worker.status : attempt?.status ?? "dispatching", asynchronous: true, idempotentReplay: true };
    }
    this.dispatchingAttempts.add(dispatchKey);
    try { return await this.#performReservedAttemptDispatch(workerId, attemptNumber); }
    finally { this.dispatchingAttempts.delete(dispatchKey); }
  }

  async #performReservedAttemptDispatch(workerId, attemptNumber) {
    const claimed = await this.store.mutate((draft) => {
      const worker = draft.workers[workerId];
      const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!worker || !attempt) throw new Error(`Unknown worker attempt: ${workerId}/${attemptNumber}`);
      if (attempt.status !== "planned") return { dispatch: false, status: worker.currentAttempt === attemptNumber ? worker.status : attempt.status };
      attempt.status = "dispatching";
      delete attempt.config;
      attempt.configPublishedAt = nowIso();
      attempt.dispatchOwner = this.#owner(draft.ownerSessionId);
      attempt.dispatchClaimedAt = nowIso();
      return { dispatch: true };
    });
    if (!claimed.result.dispatch) return { workerId, attemptNumber, status: claimed.result.status, asynchronous: true, idempotentReplay: true };
    await this.#hitFailpoint("after_dispatch_claim", { workerId, attemptNumber });
    const state = claimed.state;
    const worker = state.workers[workerId];
    const attempt = worker.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    try { await this.#assertWorkingRootCurrent(state, worker.normalizedRequest); }
    catch (error) {
      await this.#writeRecoveryResult(workerId, attemptNumber, "failed", `Working-root validation failed before dispatch: ${error.message}`);
      if (await pathExists(paths.recoveryResult)) await this.#ingestResult(workerId, attemptNumber, paths.recoveryResult, true);
      return { workerId, attemptNumber, status: "failed", asynchronous: true };
    }
    let processHandle;
    try {
      const launch = this.options.spawnSupervisor ?? spawnDetachedSupervisor;
      processHandle = await launch(this.options.supervisorPath ?? defaultSupervisorPath, paths.config, worker.cwd);
    } catch (error) {
      await this.#writeRecoveryResult(workerId, attemptNumber, "failed", `Supervisor launch failed: ${error.message}`);
      if (await pathExists(paths.recoveryResult)) await this.#ingestResult(workerId, attemptNumber, paths.recoveryResult, true);
      throw error;
    }
    const supervisorStartIdentity = await waitForIdentity(processHandle.pid);
    let launchReceipt = null;
    if (supervisorStartIdentity) {
      const launchReceiptPayload = { schemaVersion: 1, kind: "worker_supervisor_launch", storageId: state.storageId, ownerSessionId: attempt.launchSessionId, workerId, attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, supervisorPid: processHandle.pid, supervisorStartIdentity, observedAt: attempt.createdAt };
      launchReceipt = { ...launchReceiptPayload, receiptHash: sha256(launchReceiptPayload) };
      await writeImmutableJson(paths.launchReceipt, launchReceipt, { maxBytes: 64 * 1024 });
    }
    let launchStatus;
    await this.store.mutate((draft) => {
      const current = draft.workers[workerId];
      const currentAttempt = current?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!currentAttempt || currentAttempt.attemptNonce !== attempt.attemptNonce) throw new Error(`Worker ${workerId} attempt generation changed during launch`);
      if (currentAttempt.supervisorPid && (currentAttempt.supervisorPid !== processHandle.pid || currentAttempt.supervisorStartIdentity !== supervisorStartIdentity)) throw new Error("Spawn-bound supervisor identity conflicts with persisted attempt authority");
      currentAttempt.supervisorPid = processHandle.pid;
      currentAttempt.supervisorStartIdentity = supervisorStartIdentity;
      if (launchReceipt) {
        currentAttempt.launchReceiptPath = relative(state.repositoryRoot, paths.launchReceipt);
        currentAttempt.launchReceiptHash = launchReceipt.receiptHash;
      }
      const isCurrent = current.currentAttempt === attemptNumber;
      const isTerminal = Boolean(currentAttempt.ingestedAt) || TERMINAL_STATUSES.has(currentAttempt.status);
      if (isCurrent && !isTerminal && current.status !== "cancelling") {
        currentAttempt.status = supervisorStartIdentity ? "running" : "launch_ambiguous";
        currentAttempt.processDisposition = supervisorStartIdentity ? "live" : "ambiguous";
        current.status = supervisorStartIdentity ? "running" : "needs_attention";
        current.updatedAt = nowIso();
      }
      launchStatus = isCurrent ? current.status : currentAttempt.status;
    });
    await this.#hitFailpoint("after_supervisor_spawn", { workerId, attemptNumber, supervisorPid: processHandle.pid });
    return { workerId, launchKey: worker.launchKey, attemptNumber, status: launchStatus, asynchronous: true };
  }

  async #normalizeLaunchRequest(input, ctx, state) {
    const task = String(input.task ?? "").trim();
    if (!task) throw new Error("subagent task is required");
    if (Buffer.byteLength(task) > 64 * 1024) throw new Error("subagent task exceeds 64 KiB");
    const cwd = resolve(input.cwd ?? state.repositoryRoot);
    assertCwdWithin(state.repositoryRoot, cwd);
    const canonicalRepositoryRoot = await realpath(state.repositoryRoot);
    const canonicalCwd = await realpath(cwd);
    if (canonicalCwd !== canonicalRepositoryRoot && !isStrictDescendant(canonicalRepositoryRoot, canonicalCwd)) throw new Error("Worker cwd resolves outside the repository root");
    let workingRoot = { kind: "repository", path: cwd, realPath: canonicalCwd };
    if (input.disposableRootToken !== undefined) {
      const tokenHash = sha256(String(input.disposableRootToken));
      const approval = (state.approvedDisposableRoots ?? []).find((candidate) => candidate.tokenHash === tokenHash && !candidate.retiredAt);
      if (!approval || approval.ownerSessionId !== state.ownerSessionId || canonicalOwnerIdentity(approval.approvedByOwner) !== canonicalOwnerIdentity(state.owner) || approval.path !== cwd) throw new Error("Disposable working-root approval is missing, stale, or bound to another owner/path");
      const canonicalRoot = await realpath(cwd);
      const info = await stat(canonicalRoot);
      if (canonicalRoot !== approval.realPath || String(info.dev) !== approval.dev || String(info.ino) !== approval.ino || !info.isDirectory()) throw new Error("Disposable working-root identity changed after approval");
      workingRoot = { kind: "approved_disposable", path: cwd, realPath: canonicalRoot, approvalId: approval.approvalId, dev: approval.dev, ino: approval.ino };
    }
    return {
      schemaVersion: 1,
      task,
      cwd,
      workingRoot,
      requestedLabel: input.label === undefined ? null : String(input.label).trim().slice(0, 256),
      requestedWorkerId: input.workerId === undefined ? null : normalizeRuntimeId(input.workerId, "workerId"),
      activeTools: [...new Set((this.pi.getActiveTools?.() ?? []).map(String))].sort(),
      reportRepairAttempts: normalizeRepairAttempts(input.reportRepairAttempts),
      piCliPath: await resolvePiCliPath(this.options.piCliPath),
      extensionPath: resolve(this.options.extensionPath ?? extensionPath),
      provider: input.provider ? String(input.provider) : ctx?.model?.provider ? String(ctx.model.provider) : null,
      model: input.model ? String(input.model) : ctx?.model?.id ? String(ctx.model.id) : null,
      thinking: input.thinking ? String(input.thinking) : ctx?.thinkingLevel ? String(ctx.thinkingLevel) : null,
    };
  }

  async #observeUninspectableProcesses() {
    if (this.options.observeUninspectableProcesses) return this.options.observeUninspectableProcesses();
    return uninspectableSameUidProcesses([process.pid]);
  }

  async #assertWorkingRootCurrent(state, request) {
    const root = request?.workingRoot;
    if (!root || root.path !== request.cwd) throw new Error("Launch request lacks an exact working-root binding");
    const canonicalRoot = await realpath(request.cwd);
    const info = await stat(canonicalRoot);
    if (!info.isDirectory() || canonicalRoot !== root.realPath) throw new Error("Launch working-root identity changed");
    if (root.kind === "approved_disposable") {
      const approval = (state.approvedDisposableRoots ?? []).find((candidate) => candidate.approvalId === root.approvalId && !candidate.retiredAt);
      if (!approval || approval.path !== request.cwd || approval.realPath !== canonicalRoot || approval.dev !== String(info.dev) || approval.ino !== String(info.ino) || root.dev !== approval.dev || root.ino !== approval.ino) throw new Error("Disposable working-root approval is retired, stale, or inode-conflicting");
    } else if (root.kind === "repository") {
      const repositoryRoot = await realpath(state.repositoryRoot);
      if (canonicalRoot !== repositoryRoot && !isStrictDescendant(repositoryRoot, canonicalRoot)) throw new Error("Repository working root escaped its canonical repository");
    } else throw new Error("Launch working-root kind is invalid");
  }

  async scan() {
    const operation = this.scanQueue.then(async () => {
      if (!this.attached) return;
      this.scanning = true;
      try {
        const state = await this.store.load();
      for (const worker of Object.values(state.workers)) {
        if (worker.currentAttempt === 0 && worker.launchKey && worker.normalizedRequest) {
          await this.#launchAttempt(worker.id, this.context, { initialOnly: true, launchKey: worker.launchKey, idempotentReplay: true });
          continue;
        }
        for (const attempt of worker.attempts ?? []) {
          if (attempt.ingestedAt) {
            const terminalPaths = attemptPaths(state.repositoryRoot, state.storageId, worker.id, attempt.attemptNumber);
            const ingestedPath = attempt.resultPath ? resolve(state.repositoryRoot, attempt.resultPath) : null;
            if (ingestedPath === terminalPaths.recoveryResult && await pathExists(terminalPaths.result)) await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, terminalPaths.result, "late-primary-result", "Primary result appeared after recovery result adoption");
            if (ingestedPath === terminalPaths.result && await pathExists(terminalPaths.recoveryResult)) await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, terminalPaths.recoveryResult, "late-recovery-result", "Recovery result appeared after primary result adoption");
            await this.#refreshProcessDisposition(worker.id, attempt.attemptNumber);
            continue;
          }
          const paths = attemptPaths(state.repositoryRoot, state.storageId, worker.id, attempt.attemptNumber);
          if (await pathExists(paths.launchReceipt)) await this.#bindLaunchReceipt(state, worker, attempt, paths);
          if (attempt.status === "planned") {
            await writeImmutableJson(paths.config, attempt.config);
            await this.#dispatchReservedAttempt(worker.id, attempt.attemptNumber);
            continue;
          }
          if (await pathExists(paths.recoveryResult)) {
            await this.#ingestResult(worker.id, attempt.attemptNumber, paths.recoveryResult, true);
            if (await pathExists(paths.result)) await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.result, "late-primary-result", "Primary result conflicts with an already-published recovery result");
            continue;
          }
          if (!attempt.supervisorPid || !attempt.supervisorStartIdentity) {
            if (this.dispatchingAttempts.has(`${worker.id}:${attempt.attemptNumber}`)) continue;
            await this.#reconcileMissingAttempt(state, worker, attempt);
            continue;
          }
          if (await pathExists(paths.result)) {
            await this.#ingestResult(worker.id, attempt.attemptNumber, paths.result, false);
            continue;
          }
          if (await pathExists(paths.mailbox)) {
            try {
              const mailbox = await readJson(paths.mailbox, { maxBytes: MAX_MAILBOX_BYTES });
              if (!mailboxMatches(mailbox, state, worker, attempt)) throw new Error("mailbox identity mismatch");
              await this.store.mutate((draft) => {
                const currentWorker = draft.workers[worker.id];
                const currentAttempt = currentWorker?.attempts.find((candidate) => candidate.attemptNumber === attempt.attemptNumber);
                if (!currentAttempt || currentAttempt.ingestedAt) return;
                currentAttempt.supervisorPid = mailbox.supervisorPid;
                currentAttempt.supervisorStartIdentity = mailbox.supervisorStartIdentity;
                currentAttempt.childPid = mailbox.childPid;
                currentAttempt.childStartIdentity = mailbox.childStartIdentity;
                currentAttempt.heartbeatAt = mailbox.heartbeatAt;
                currentAttempt.mailboxStatus = mailbox.status;
                if (currentWorker.currentAttempt === attempt.attemptNumber && !TERMINAL_STATUSES.has(currentWorker.status) && currentWorker.status !== "cancelling") {
                  currentWorker.status = mailbox.status === "settling" ? "settling" : "running";
                  currentWorker.updatedAt = nowIso();
                }
              });
              if (worker.status === "cancelling") this.#scheduleCancellationEscalation(worker.id, attempt, mailbox.supervisorPid, mailbox.supervisorStartIdentity);
            } catch (error) {
              await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.mailbox, "conflicting-or-corrupt-mailbox", error.message);
              await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", `Mailbox recovery failed closed: ${error.message}`);
            }
            continue;
          }
          await this.#reconcileMissingAttempt(state, worker, attempt);
        }
      }
        await this.#compactTerminalWorkers();
        await this.dispatchNext();
      } finally { this.scanning = false; }
    });
    this.scanQueue = operation.then(() => undefined, () => undefined);
    return operation;
  }

  async #bindLaunchReceipt(state, worker, attempt, paths) {
    const receipt = await readJson(paths.launchReceipt, { maxBytes: 64 * 1024 });
    if (!attempt.supervisorPid || !attempt.supervisorStartIdentity) return null;
    const { receiptHash, ...payload } = receipt ?? {};
    const valid = receiptHash === sha256(payload) && receipt.kind === "worker_supervisor_launch" && receipt.storageId === state.storageId && receipt.ownerSessionId === attempt.launchSessionId && receipt.workerId === worker.id && receipt.attemptNumber === attempt.attemptNumber && receipt.attemptNonce === attempt.attemptNonce && receipt.configHash === attempt.configHash && Number.isInteger(receipt.supervisorPid) && typeof receipt.supervisorStartIdentity === "string";
    const conflict = attempt.supervisorPid && (attempt.supervisorPid !== receipt.supervisorPid || attempt.supervisorStartIdentity !== receipt.supervisorStartIdentity);
    if (!valid || conflict) {
      await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.launchReceipt, "conflicting-or-corrupt-launch-receipt", valid ? "Launch receipt conflicts with spawn-bound supervisor identity" : "Launch receipt identity or hash is invalid");
      throw new Error("Supervisor launch receipt is invalid or conflicting");
    }
    if (attempt.launchReceiptHash === receipt.receiptHash && attempt.supervisorPid === receipt.supervisorPid && attempt.supervisorStartIdentity === receipt.supervisorStartIdentity) return receipt;
    await this.store.mutate((draft) => {
      const current = draft.workers[worker.id]?.attempts.find((candidate) => candidate.attemptNumber === attempt.attemptNumber);
      if (!current || current.attemptNonce !== attempt.attemptNonce) return;
      if (current.supervisorPid && (current.supervisorPid !== receipt.supervisorPid || current.supervisorStartIdentity !== receipt.supervisorStartIdentity)) throw new Error("Supervisor launch receipt conflicts after CAS recheck");
      current.supervisorPid ??= receipt.supervisorPid;
      current.supervisorStartIdentity ??= receipt.supervisorStartIdentity;
      current.launchReceiptPath = relative(state.repositoryRoot, paths.launchReceipt);
      current.launchReceiptHash = receipt.receiptHash;
    });
    attempt.supervisorPid ??= receipt.supervisorPid;
    attempt.supervisorStartIdentity ??= receipt.supervisorStartIdentity;
    return receipt;
  }

  async #refreshProcessDisposition(workerId, attemptNumber = null) {
    const state = await this.store.load();
    const worker = state.workers[workerId];
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === (attemptNumber ?? worker.currentAttempt));
    if (!worker || !attempt) return null;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attempt.attemptNumber);
    if (await pathExists(paths.launchReceipt)) await this.#bindLaunchReceipt(state, worker, attempt, paths);
    let supervisorPid = attempt.supervisorPid;
    let supervisorStartIdentity = attempt.supervisorStartIdentity;
    let childPid = attempt.childPid;
    let childStartIdentity = attempt.childStartIdentity;
    if (await pathExists(paths.mailbox)) {
      try {
        const mailbox = await readJson(paths.mailbox, { maxBytes: MAX_MAILBOX_BYTES });
        if (!mailboxMatches(mailbox, state, worker, attempt)) throw new Error("mailbox identity mismatch");
        supervisorPid = mailbox.supervisorPid ?? supervisorPid;
        supervisorStartIdentity = mailbox.supervisorStartIdentity ?? supervisorStartIdentity;
        childPid = mailbox.childPid ?? childPid;
        childStartIdentity = mailbox.childStartIdentity ?? childStartIdentity;
      } catch (error) {
        await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.mailbox, "conflicting-or-corrupt-mailbox", error.message);
      }
    }
    const supervisor = await processIdentityStatus(supervisorPid, supervisorStartIdentity);
    const child = childPid && childStartIdentity ? await processIdentityStatus(childPid, childStartIdentity) : "ambiguous";
    const processGroup = processGroupStatus(supervisorPid);
    const workingRootUsers = await processesUsingWorkingRoot(worker.cwd, [process.pid]);
    const boundProcesses = await processesWithEnvironmentBinding("PI_DAG_WORKER_ATTEMPT_NONCE", attempt.attemptNonce, [process.pid]);
    const currentUninspectable = await this.#observeUninspectableProcesses();
    const baselineIdentities = new Set((attempt.uninspectableProcessBaseline ?? []).map((candidate) => `${candidate.pid}:${candidate.processStartIdentity}`));
    const newUninspectable = currentUninspectable.processes.filter((candidate) => !baselineIdentities.has(`${candidate.pid}:${candidate.processStartIdentity}`));
    let disposition = "ambiguous";
    if (supervisor === "live" || child === "live" || processGroup === "present" || workingRootUsers.users.length || boundProcesses.users.length || newUninspectable.length) disposition = "live";
    else if (["dead", "mismatch"].includes(supervisor) && ["dead", "mismatch"].includes(child) && processGroup === "absent" && workingRootUsers.status === "observed" && boundProcesses.status === "observed" && currentUninspectable.status === "observed") disposition = "dead";
    const retrySafe = disposition === "dead" && workingRootUsers.status === "observed" && workingRootUsers.users.length === 0 && boundProcesses.status === "observed" && boundProcesses.users.length === 0 && currentUninspectable.status === "observed" && newUninspectable.length === 0;
    if (attempt.processDisposition === disposition && attempt.retrySafe === retrySafe && attempt.supervisorPid === supervisorPid && attempt.supervisorStartIdentity === supervisorStartIdentity && attempt.childPid === childPid && attempt.childStartIdentity === childStartIdentity) return { disposition, retrySafe };
    const observedAt = nowIso();
    const factPayload = { schemaVersion: 1, kind: "worker_process_disposition", storageId: state.storageId, workerId, attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, supervisor: { pid: supervisorPid ?? null, processStartIdentity: supervisorStartIdentity ?? null, status: supervisor }, child: { pid: childPid ?? null, processStartIdentity: childStartIdentity ?? null, status: child }, processGroup: { leaderPid: supervisorPid ?? null, status: processGroup }, workingRoot: { path: worker.cwd, observationStatus: workingRootUsers.status, users: workingRootUsers.users }, boundProcesses: { environmentName: "PI_DAG_WORKER_ATTEMPT_NONCE", observationStatus: boundProcesses.status, users: boundProcesses.users }, uninspectableProcesses: { observationStatus: currentUninspectable.status, baseline: attempt.uninspectableProcessBaseline ?? [], newProcesses: newUninspectable }, disposition, retrySafe, observedByOwner: structuredClone(state.owner), observedAt };
    const processFact = { ...factPayload, factHash: sha256(factPayload) };
    const processFactPath = join(paths.processFacts, `${processFact.factHash.slice(7)}.json`);
    await writeImmutableJson(processFactPath, processFact, { maxBytes: 64 * 1024 });
    await this.store.mutate((draft) => {
      const current = draft.workers[workerId]?.attempts.find((candidate) => candidate.attemptNumber === attempt.attemptNumber);
      if (!current || current.attemptNonce !== attempt.attemptNonce) return;
      current.supervisorPid ??= supervisorPid;
      current.supervisorStartIdentity ??= supervisorStartIdentity;
      current.childPid ??= childPid;
      current.childStartIdentity ??= childStartIdentity;
      current.processDisposition = disposition;
      current.retrySafe = retrySafe;
      current.processObservedAt = observedAt;
      current.processObservation = { supervisor, child, processGroup, workingRootUsers, boundProcesses, currentUninspectable, newUninspectable };
      current.processDispositionFactPath = relative(state.repositoryRoot, processFactPath);
      current.processDispositionFactHash = processFact.factHash;
    });
    return { disposition, retrySafe };
  }

  async #reconcileMissingAttempt(state, worker, attempt) {
    const age = Date.now() - Date.parse(attempt.createdAt);
    if (age < (this.options.launchGraceMs ?? 10_000)) return;
    if (!attempt.supervisorPid || !attempt.supervisorStartIdentity) {
      await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", "Launch ownership remained ambiguous after the recovery grace period", false, { requireUnbound: true, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash });
      return;
    }
    const status = await processIdentityStatus(attempt.supervisorPid, attempt.supervisorStartIdentity);
    if (status === "live") return;
    if (status === "dead") {
      await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", "Supervisor process died without an immutable result");
      await this.#refreshProcessDisposition(worker.id, attempt.attemptNumber);
      return;
    }
    await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", `Supervisor ownership is ${status}; refusing to signal or relaunch`);
  }

  async #quarantineAttemptArtifact(workerId, attemptNumber, sourcePath, kind, reason) {
    let observed;
    let factHash;
    try {
      observed = await stat(sourcePath);
      factHash = await sha256File(sourcePath);
      const confirmed = await stat(sourcePath);
      if (observed.dev !== confirmed.dev || observed.ino !== confirmed.ino || observed.size !== confirmed.size || observed.mtimeMs !== confirmed.mtimeMs) throw new Error("Worker artifact changed while being quarantined");
    } catch (error) { if (error?.code === "ENOENT") return null; throw error; }
    const state = await this.store.load();
    const worker = state.workers[workerId];
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
    if (!attempt) return null;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    const retainedPath = join(paths.quarantine, `${normalizeRuntimeId(kind, "quarantine kind")}-${factHash.slice(7)}.bin`);
    await mkdir(paths.quarantine, { recursive: true });
    try { await rename(sourcePath, retainedPath); }
    catch (error) {
      if (error?.code !== "EEXIST" || await sha256File(retainedPath) !== factHash) throw error;
    }
    await syncDirectory(paths.quarantine);
    await syncDirectory(paths.root);
    const envelopePath = join(paths.quarantine, `${normalizeRuntimeId(kind, "quarantine kind")}-${factHash.slice(7)}.envelope.json`);
    const record = { quarantineId: `worker-artifact-${factHash.slice(7, 31)}`, workerId, attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, kind, reason, sourcePath: relative(state.repositoryRoot, sourcePath), retainedPath: relative(state.repositoryRoot, retainedPath), envelopePath: relative(state.repositoryRoot, envelopePath), factHash, byteLength: observed.size, retainedComplete: true };
    const envelopePayload = { schemaVersion: 1, kind: "quarantined_worker_artifact", storageId: state.storageId, record };
    await writeImmutableJson(envelopePath, { ...envelopePayload, envelopeHash: sha256(envelopePayload) }, { maxBytes: 64 * 1024 });
    await this.store.mutate((draft) => {
      draft.quarantinedArtifacts ??= [];
      const existing = draft.quarantinedArtifacts.find((candidate) => candidate.workerId === workerId && candidate.attemptNumber === attemptNumber && candidate.kind === kind && candidate.factHash === factHash);
      if (!existing) draft.quarantinedArtifacts.push(record);
      if (draft.quarantinedArtifacts.length > 256) draft.quarantinedArtifacts.splice(0, draft.quarantinedArtifacts.length - 256);
    });
    return record;
  }

  async #writeRecoveryResult(workerId, attemptNumber, terminalStatus, summary, allowPrimaryResult = false, guard = null) {
    const publication = await this.store.mutate(async (draft) => {
      const worker = draft.workers[workerId];
      const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!attempt || attempt.ingestedAt) return { published: false };
      if (guard?.requireUnbound && (attempt.attemptNonce !== guard.attemptNonce || attempt.configHash !== guard.configHash || attempt.supervisorPid || attempt.supervisorStartIdentity)) return { published: false };
      const paths = attemptPaths(draft.repositoryRoot, draft.storageId, workerId, attemptNumber);
      if ((!allowPrimaryResult && await pathExists(paths.result)) || await pathExists(paths.recoveryResult)) return { published: false };
      const seed = sha256({ storageId: draft.storageId, workerId, attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash });
      const result = withResultHash({
        schemaVersion: 1,
        completionId: `completion-${workerId}-${attemptNumber}-${seed.slice(7, 19)}`,
        storageId: draft.storageId,
        ownerSessionId: attempt.launchSessionId,
        workerId,
        attemptNumber,
        attemptNonce: attempt.attemptNonce,
        configHash: attempt.configHash,
        terminalStatus,
        reportStatus: "missing",
        startedAt: attempt.createdAt,
        endedAt: nowIso(),
        fallbackFinalText: summary,
        runtime: { recovery: true },
        diagnostics: { path: relative(draft.repositoryRoot, paths.diagnostics), cappedAtBytes: 50 * 1024 * 1024, truncated: false, eventCounts: {} },
        artifacts: [],
      });
      await writeImmutableJson(paths.recoveryResult, result, { maxBytes: MAX_RESULT_BYTES });
      return { published: true };
    });
    return publication.result.published;
  }

  async #ingestResult(workerId, attemptNumber, path, recovery) {
    let result;
    try {
      result = await readJson(path, { maxBytes: MAX_RESULT_BYTES });
      assertTerminalResult(result, { recovery });
    } catch (error) {
      await this.#quarantineAttemptArtifact(workerId, attemptNumber, path, recovery ? "corrupt-recovery-result" : "corrupt-primary-result", error.message);
      if (!recovery) {
        await this.#writeRecoveryResult(workerId, attemptNumber, "lost", `Terminal result is corrupt: ${error.message}`, true);
        const state = await this.store.load();
        const recoveryPath = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber).recoveryResult;
        if (await pathExists(recoveryPath)) await this.#ingestResult(workerId, attemptNumber, recoveryPath, true);
      } else {
        await this.store.mutate((draft) => {
          const worker = draft.workers[workerId];
          const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
          if (!attempt || attempt.ingestedAt) return;
          attempt.status = "recovery_corrupt";
          attempt.processDisposition = "ambiguous";
          attempt.retrySafe = false;
          if (worker.currentAttempt === attemptNumber) { worker.status = "needs_attention"; worker.updatedAt = nowIso(); }
        });
      }
      return;
    }
    const state = await this.store.load();
    const worker = state.workers[workerId];
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
    if (!attempt) return;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    const processIdentityConflict = !recovery && ((!attempt.supervisorPid || !attempt.supervisorStartIdentity) || result.process.supervisorPid !== attempt.supervisorPid || result.process.supervisorStartIdentity !== attempt.supervisorStartIdentity || (attempt.childPid && result.process.childPid !== attempt.childPid) || (attempt.childStartIdentity && result.process.childStartIdentity !== attempt.childStartIdentity));
    const temporalConflict = !recovery && (Date.parse(result.startedAt) < Date.parse(attempt.createdAt) || Date.parse(result.endedAt) > Date.now() + 5 * 60_000);
    if (result.storageId !== state.storageId || result.workerId !== workerId || result.attemptNumber !== attemptNumber || result.attemptNonce !== attempt.attemptNonce || result.configHash !== attempt.configHash || result.ownerSessionId !== attempt.launchSessionId || processIdentityConflict || temporalConflict) {
      await this.#quarantineAttemptArtifact(workerId, attemptNumber, path, recovery ? "conflicting-recovery-result" : "conflicting-primary-result", "Terminal result identity mismatch");
      if (!recovery) {
        await this.#writeRecoveryResult(workerId, attemptNumber, "lost", "Terminal result identity mismatch", true);
        const recoveryPath = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber).recoveryResult;
        if (await pathExists(recoveryPath)) await this.#ingestResult(workerId, attemptNumber, recoveryPath, true);
      }
      return;
    }
    const ingestion = await this.store.mutate(async (draft) => {
      const currentWorker = draft.workers[workerId];
      const currentAttempt = currentWorker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!currentAttempt || currentAttempt.ingestedAt) return { adopted: false };
      if (!recovery && result.terminalStatus !== "cancelled" && await pathExists(paths.cancel)) {
        const cancellation = await readJson(paths.cancel, { maxBytes: 16 * 1024 });
        const cancellationMatches = cancellation.storageId === draft.storageId && cancellation.workerId === workerId && cancellation.attemptNumber === attemptNumber && cancellation.attemptNonce === currentAttempt.attemptNonce && cancellation.configHash === currentAttempt.configHash;
        return { adopted: false, rejectedByCancellation: true, cancellationIdentityValid: cancellationMatches };
      }
      currentAttempt.ingestedAt = nowIso();
      currentAttempt.status = result.terminalStatus;
      currentAttempt.resultPath = relative(state.repositoryRoot, path);
      currentAttempt.resultHash = result.resultHash;
      currentAttempt.completionId = result.completionId;
      if (!recovery) {
        currentAttempt.childPid ??= result.process.childPid;
        currentAttempt.childStartIdentity ??= result.process.childStartIdentity;
      }
      if (currentWorker.currentAttempt === attemptNumber) {
        currentWorker.status = result.terminalStatus;
        currentWorker.updatedAt = nowIso();
        currentWorker.completionId = result.completionId;
      }
      const known = draft.completedCompletionIds.includes(result.completionId) || draft.completionQueue.includes(result.completionId) || draft.inFlightCompletionId === result.completionId;
      if (!known && currentWorker.currentAttempt === attemptNumber) draft.completionQueue.push(result.completionId);
      return { adopted: true };
    });
    if (ingestion.result?.rejectedByCancellation) {
      await this.#quarantineAttemptArtifact(workerId, attemptNumber, path, "late-post-cancellation-result", "Non-cancelled terminal result did not precede the serialized cancellation intent");
      await this.#writeRecoveryResult(workerId, attemptNumber, "lost", "Non-cancelled terminal result conflicted with serialized cancellation authority", true);
      if (await pathExists(paths.recoveryResult)) await this.#ingestResult(workerId, attemptNumber, paths.recoveryResult, true);
    }
  }

  async dispatchNext() {
    if (!this.attached) return false;
    let completionId = null;
    await this.store.mutate((state) => {
      if (state.inFlightCompletionId || !state.completionQueue.length) return;
      completionId = state.completionQueue.shift();
      state.inFlightCompletionId = completionId;
    });
    if (!completionId) return false;
    try {
      const message = await this.#completionMessage(completionId);
      this.pi.sendMessage({ customType: "subagent-completion", content: message, display: true, details: { completionId } }, { deliverAs: "followUp", triggerTurn: true });
      return true;
    } catch (error) {
      await this.store.mutate((state) => {
        if (state.inFlightCompletionId !== completionId) return;
        state.inFlightCompletionId = null;
        if (!state.completionQueue.includes(completionId)) state.completionQueue.unshift(completionId);
      });
      throw error;
    }
  }

  async onAgentSettled() {
    if (!this.attached) return;
    let acknowledged = null;
    await this.store.mutate((state) => {
      if (!state.inFlightCompletionId) return;
      acknowledged = state.inFlightCompletionId;
      state.inFlightCompletionId = null;
      if (!state.completedCompletionIds.includes(acknowledged)) state.completedCompletionIds.push(acknowledged);
      if (state.completedCompletionIds.length > 2000) state.completedCompletionIds.splice(0, state.completedCompletionIds.length - 2000);
    });
    if (acknowledged) {
      await this.#compactTerminalWorkers();
      await this.dispatchNext();
    }
  }

  async #compactTerminalWorkers() {
    const state = await this.store.load();
    const limit = Math.max(1, Number(this.options.maxRetainedTerminalWorkers ?? state.retentionPolicy?.maxRetainedTerminalWorkers ?? 50));
    const eligible = Object.values(state.workers)
      .filter((worker) => TERMINAL_STATUSES.has(worker.status))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
    for (const worker of eligible.slice(limit)) {
      const archivedAt = worker.updatedAt;
      const payload = { schemaVersion: 1, kind: "archived_worker", storageId: state.storageId, worker: structuredClone(worker), archivedAt };
      const archive = { ...payload, archiveHash: sha256(payload) };
      const path = workerArchivePath(state.repositoryRoot, state.storageId, worker.id);
      await writeImmutableJson(path, archive, { maxBytes: MAX_STATE_BYTES });
      await this.store.mutate((draft) => {
        const current = draft.workers[worker.id];
        if (!current || current.completionId !== worker.completionId || current.updatedAt !== worker.updatedAt || !TERMINAL_STATUSES.has(current.status)) return;
        const record = (draft.launchRecords ?? []).find((candidate) => candidate.workerId === worker.id);
        if (!record) throw new Error(`Cannot archive worker ${worker.id} without its launch record`);
        record.archivedWorkerPath = relative(draft.repositoryRoot, path);
        record.archivedWorkerHash = archive.archiveHash;
        record.archivedCurrentAttempt = worker.currentAttempt;
        record.archivedStatus = worker.status;
        record.archivedAt = archivedAt;
        delete draft.workers[worker.id];
        draft.retryAuthorizations = (draft.retryAuthorizations ?? []).filter((authorization) => authorization.workerId !== worker.id);
        draft.completedCompletionIds = draft.completedCompletionIds.filter((completionId) => completionId !== worker.completionId);
      });
    }
  }

  async #completionMessage(completionId) {
    const inspected = await this.inspect(completionId);
    const { worker, result } = inspected;
    const lines = [
      "[Asynchronous subagent completion]",
      `completionId: ${completionId}`,
      `worker: ${worker.id}${worker.label !== worker.id ? ` (${worker.label})` : ""}`,
      `attempt: ${result.attemptNumber}`,
      `terminalStatus: ${result.terminalStatus}`,
      `reportStatus: ${result.reportStatus}`,
      "",
      result.report?.summary ?? result.fallbackFinalText ?? "No summary was reported.",
    ];
    if (result.artifacts?.length) lines.push("", "Artifacts:", ...result.artifacts.map((artifact) => `- ${artifact.path}${artifact.label ? ` — ${artifact.label}` : ""}`));
    if (result.report?.nextSteps?.length) lines.push("", "Next steps:", ...result.report.nextSteps.map((step) => `- ${step}`));
    lines.push("", `Use subagent_inspect({ workerId: \"${worker.id}\" }) for the full bounded result or subagent_tail for diagnostics.`);
    return truncateUtf8(lines.join("\n"), MAX_COMPLETION_MESSAGE_BYTES);
  }

  async cancel(workerId, reason = "requested") {
    this.#assertAttached();
    const state = await this.store.load();
    const worker = state.workers[workerId];
    if (!worker) throw new Error(`Unknown worker: ${workerId}`);
    if (TERMINAL_STATUSES.has(worker.status)) return { workerId, status: worker.status, alreadyTerminal: true };
    const attempt = worker.attempts.find((candidate) => candidate.attemptNumber === worker.currentAttempt);
    if (!attempt) throw new Error(`Worker ${workerId} has no current attempt`);
    let pid = attempt.supervisorPid;
    let identity = attempt.supervisorStartIdentity;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attempt.attemptNumber);
    const processStatus = pid && identity ? await processIdentityStatus(pid, identity) : "unbound";
    if (processStatus !== "live" && processStatus !== "unbound") throw new Error(`Cancellation refused: supervisor ownership is ${processStatus}`);
    const cancellationCommit = await this.store.mutate(async (draft) => {
      const current = draft.workers[workerId];
      if (!current) throw new Error(`Unknown worker: ${workerId}`);
      if (TERMINAL_STATUSES.has(current.status)) return { alreadyTerminal: true, status: current.status };
      const currentAttempt = current.attempts.find((candidate) => candidate.attemptNumber === current.currentAttempt);
      if (!currentAttempt || currentAttempt.attemptNumber !== attempt.attemptNumber || currentAttempt.attemptNonce !== attempt.attemptNonce || currentAttempt.configHash !== attempt.configHash) throw new Error("Cancellation attempt identity changed before serialized intent publication");
      const cancellation = { schemaVersion: 1, storageId: draft.storageId, workerId, attemptNumber: currentAttempt.attemptNumber, attemptNonce: currentAttempt.attemptNonce, configHash: currentAttempt.configHash, requestedByOwner: structuredClone(draft.owner), requestedAt: nowIso(), reason: String(reason).slice(0, 1024) };
      if (await pathExists(paths.cancel)) {
        const existing = await readJson(paths.cancel, { maxBytes: 16 * 1024 });
        if (!cancellationIntentMatches(existing, cancellation)) throw new Error("Cancellation intent conflicts with the current attempt identity or reason");
      } else {
        try { await writeImmutableJson(paths.cancel, cancellation, { maxBytes: 16 * 1024 }); }
        catch (error) {
          const existing = await readJson(paths.cancel, { maxBytes: 16 * 1024 }).catch(() => null);
          if (!existing || !cancellationIntentMatches(existing, cancellation)) throw error;
        }
      }
      current.status = "cancelling";
      current.updatedAt = nowIso();
      return { alreadyTerminal: false, status: "cancelling", cancellation };
    });
    if (cancellationCommit.result.alreadyTerminal) return { workerId, status: cancellationCommit.result.status, alreadyTerminal: true };
    if (processStatus === "live") this.#scheduleCancellationEscalation(workerId, attempt, pid, identity);
    return { workerId, attemptNumber: attempt.attemptNumber, status: "cancelling" };
  }

  #scheduleCancellationEscalation(workerId, attempt, pid, identity) {
    const key = `${workerId}:${attempt.attemptNumber}:${attempt.attemptNonce}`;
    if (this.cancellationTimers.has(key)) return;
    const timer = setTimeout(async () => {
      this.cancellationTimers.delete(key);
      if (!this.attached || !this.store) return;
      try {
        const state = await this.store.load();
        const worker = state.workers[workerId];
        const current = worker?.attempts.find((candidate) => candidate.attemptNumber === attempt.attemptNumber);
        if (!worker || worker.status !== "cancelling" || worker.currentAttempt !== attempt.attemptNumber || current?.attemptNonce !== attempt.attemptNonce) return;
        if (await processIdentityStatus(pid, identity) !== "live") return;
        process.kill(-pid, "SIGTERM");
      } catch (error) {
        if (error?.code !== "ESRCH") console.error(`Worker cancellation escalation failed for ${workerId}: ${error.message}`);
      }
    }, this.options.cancelEscalationMs ?? 8000);
    timer.unref?.();
    this.cancellationTimers.set(key, timer);
  }

  async listResults(options = {}) {
    this.#assertAttached();
    const state = await this.store.load();
    const launchRecord = options.launchKey === undefined ? null : (state.launchRecords ?? []).find((record) => record.launchKey === normalizeLaunchKey(options.launchKey));
    if (options.launchKey !== undefined && !launchRecord) return [];
    const records = launchRecord ? [launchRecord] : (state.launchRecords ?? []);
    const workers = launchRecord ? [state.workers[launchRecord.workerId]].filter(Boolean) : Object.values(state.workers);
    for (const record of records) if (record.archivedWorkerPath && !workers.some((worker) => worker.id === record.workerId)) workers.push(await readArchivedWorker(state, record));
    const results = [];
    for (const worker of workers) {
      for (const attempt of worker.attempts ?? []) {
        if (!attempt.resultPath) continue;
        const result = await readJson(resolve(state.repositoryRoot, attempt.resultPath), { maxBytes: MAX_RESULT_BYTES });
        assertTerminalResult(result, { recovery: resolve(state.repositoryRoot, attempt.resultPath) === attemptPaths(state.repositoryRoot, state.storageId, worker.id, attempt.attemptNumber).recoveryResult });
        if (result.storageId !== state.storageId || result.workerId !== worker.id || result.attemptNumber !== attempt.attemptNumber || result.attemptNonce !== attempt.attemptNonce || result.configHash !== attempt.configHash) throw new Error(`Stored result identity mismatch for ${worker.id}/${attempt.attemptNumber}`);
        if (attempt.retrySafe === true) await assertRetrySafeProcessFact(state, worker, attempt);
        results.push({ launchKey: worker.launchKey ?? null, requestHash: worker.requestHash ?? null, workerId: worker.id, attemptNumber: attempt.attemptNumber, completionId: result.completionId, resultHash: result.resultHash, terminalStatus: result.terminalStatus, processDisposition: attempt.processDisposition ?? "ambiguous", retrySafe: attempt.retrySafe === true, processDispositionFactHash: attempt.processDispositionFactHash ?? null, resultPath: attempt.resultPath });
      }
    }
    return results.sort((left, right) => left.workerId.localeCompare(right.workerId) || left.attemptNumber - right.attemptNumber);
  }

  async resultByLaunchKey(launchKey) {
    const summaries = await this.listResults({ launchKey });
    if (!summaries.length) return null;
    const selected = summaries.at(-1);
    return this.inspect(selected.completionId);
  }

  async readBoundAttempts(bindings) {
    this.#assertAttached(); const repositoryRoot = resolve(this.context.cwd); const outputs = [];
    for (const binding of [...bindings].sort((a, b) => a.workerStorageId.localeCompare(b.workerStorageId) || a.workerId.localeCompare(b.workerId) || a.attemptNumber - b.attemptNumber)) {
      const store = new WorkerSessionStore(repositoryRoot, binding.workerStorageId); const state = await store.load(); if (state.storageId !== binding.workerStorageId || state.ownerSessionId !== binding.launchOwnerSessionId) continue;
      let worker = state.workers[binding.workerId]; if (!worker) { const record = (state.launchRecords ?? []).find(({ workerId }) => workerId === binding.workerId); if (record?.archivedWorkerPath) worker = await readArchivedWorker(state, record); }
      const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash); if (!attempt) continue; const config = await readJson(resolve(repositoryRoot, attempt.configPath)); if (config.ownerSessionId !== binding.launchOwnerSessionId) continue;
      outputs.push({ storageId: state.storageId, launchOwnerSessionId: config.ownerSessionId, workerId: worker.id, attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, terminalStatus: attempt.ingestedAt ? attempt.status : null, processDisposition: attempt.processDisposition ?? "ambiguous", retrySafe: Boolean(attempt.retrySafe), resultHash: attempt.resultHash ?? null });
    }
    return outputs;
  }

  async status(workerId) {
    this.#assertAttached();
    const state = await this.store.load();
    if (workerId) {
      const worker = state.workers[workerId];
      if (!worker) throw new Error(`Unknown worker: ${workerId}`);
      return verifiedWorkerSummary(state, worker);
    }
    return Promise.all(Object.values(state.workers).sort((a, b) => a.createdAt.localeCompare(b.createdAt)).map((worker) => verifiedWorkerSummary(state, worker)));
  }

  async inspect(id) {
    this.#assertAttached();
    const state = await this.store.load();
    const directWorker = state.workers[id];
    let worker = directWorker ?? Object.values(state.workers).find((candidate) => candidate.attempts.some((attempt) => attempt.completionId === id));
    let direct = Boolean(directWorker);
    if (!worker) {
      for (const record of state.launchRecords ?? []) {
        if (!record.archivedWorkerPath) continue;
        const archived = await readArchivedWorker(state, record);
        if (archived.id === id || archived.attempts.some((attempt) => attempt.completionId === id)) { worker = archived; direct = archived.id === id; break; }
      }
    }
    if (!worker) throw new Error(`Unknown worker or completion: ${id}`);
    const attempt = direct
      ? worker.attempts.find((candidate) => candidate.attemptNumber === worker.currentAttempt) ?? worker.attempts.at(-1)
      : worker.attempts.find((candidate) => candidate.completionId === id);
    if (!attempt) throw new Error(`Completion ${id} has no matching attempt`);
    let result = null;
    if (attempt.resultPath) {
      result = await readJson(resolve(state.repositoryRoot, attempt.resultPath), { maxBytes: MAX_RESULT_BYTES });
      assertTerminalResult(result, { recovery: resolve(state.repositoryRoot, attempt.resultPath) === attemptPaths(state.repositoryRoot, state.storageId, worker.id, attempt.attemptNumber).recoveryResult });
      if (result.storageId !== state.storageId || result.workerId !== worker.id || result.attemptNumber !== attempt.attemptNumber || result.attemptNonce !== attempt.attemptNonce || result.configHash !== attempt.configHash) throw new Error("Stored terminal result identity mismatch");
    }
    return { worker, attempt, result };
  }

  async tail(workerId, options = {}) {
    this.#assertAttached();
    const state = await this.store.load();
    const worker = state.workers[workerId];
    if (!worker) throw new Error(`Unknown worker: ${workerId}`);
    const attemptNumber = options.attemptNumber ?? worker.currentAttempt;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    if (!(await pathExists(paths.diagnostics))) return "";
    const text = await readFileTail(paths.diagnostics, Math.min(Math.max(Number(options.maxBytes ?? 64 * 1024), 1024), 256 * 1024));
    const lines = text.split(/\r?\n/);
    return lines.slice(-Math.min(Math.max(Number(options.lines ?? 80), 1), 1000)).join("\n");
  }

  async summary() {
    if (!this.store) return { attached: false };
    const state = await this.store.load();
    return { attached: true, storageId: state.storageId, ownerSessionId: state.ownerSessionId, workerCount: Object.keys(state.workers).length, queuedCompletions: state.completionQueue.length, inFlightCompletionId: state.inFlightCompletionId };
  }

  async #migrateLegacyLaunchBindings(ctx) {
    const state = await this.store.load();
    const migrations = [];
    for (const worker of Object.values(state.workers)) {
      if (worker.launchKey && worker.requestHash && worker.normalizedRequest) continue;
      let normalizedRequest;
      try { normalizedRequest = await this.#normalizeLaunchRequest({ task: worker.task, label: worker.label, cwd: worker.cwd, ...worker.launchOptions }, ctx, state); }
      catch (error) { normalizedRequest = { schemaVersion: 1, kind: "legacy_unverified", task: String(worker.task ?? ""), cwd: String(worker.cwd ?? ""), migrationError: error.message }; }
      const launchKey = worker.launchKey ?? `legacy:${state.storageId}:${worker.id}`;
      migrations.push({ workerId: worker.id, launchKey, requestHash: worker.requestHash ?? sha256(normalizedRequest), normalizedRequest });
    }
    if (!migrations.length && state.launchRecords !== undefined && state.retryAuthorizations !== undefined && state.approvedDisposableRoots !== undefined && state.quarantinedArtifacts !== undefined && state.retentionPolicy !== undefined) return;
    await this.store.mutate((draft) => {
      draft.launchRecords ??= [];
      draft.retryAuthorizations ??= [];
      draft.approvedDisposableRoots ??= [];
      draft.quarantinedArtifacts ??= [];
      draft.retentionPolicy ??= { terminalResults: "preserve", quarantine: "preserve", maxAcknowledgedCompletionIds: 2000, maxRetainedTerminalWorkers: 50 };
      for (const migration of migrations) {
        const worker = draft.workers[migration.workerId];
        if (!worker) continue;
        worker.launchKey ??= migration.launchKey;
        worker.requestHash ??= migration.requestHash;
        worker.normalizedRequest ??= migration.normalizedRequest;
        if (!draft.launchRecords.some((record) => record.launchKey === worker.launchKey)) draft.launchRecords.push({ launchKey: worker.launchKey, requestHash: worker.requestHash, workerId: worker.id, attemptNumber: worker.currentAttempt || undefined, reservedAt: worker.createdAt, legacyMigratedAt: nowIso() });
      }
    });
  }

  async #claimOwnership(sessionId, sessionFile) {
    const currentPid = process.pid;
    const currentStart = this.processStartIdentity;
    await this.store.mutate(async (state) => {
      if (resolve(state.repositoryRoot) !== resolve(this.context.cwd)) throw new Error("Worker session cwd conflicts with the attached repository");
      if (state.ownerSessionId !== sessionId) throw new Error(`Worker session belongs to ${state.ownerSessionId}, not ${sessionId}`);
      if (state.owner && (state.owner.pid !== currentPid || state.owner.processStartIdentity !== currentStart)) {
        const ownerStatus = await processIdentityStatus(state.owner.pid, state.owner.processStartIdentity);
        if (ownerStatus === "live") throw new Error(`Worker session already has a live owner process ${state.owner.pid}`);
        if (!processIdentityIsGone(ownerStatus)) throw new Error(`Worker session owner identity is ${ownerStatus}; refusing attachment`);
      }
      if (state.inFlightCompletionId) {
        if (!state.completionQueue.includes(state.inFlightCompletionId)) state.completionQueue.unshift(state.inFlightCompletionId);
        state.inFlightCompletionId = null;
      }
      state.sessionFile = sessionFile;
      state.owner = this.#owner(sessionId);
    });
  }

  async #transferDirectParent(repositoryRoot, parentSessionId, sessionId, sessionFile) {
    const parents = await findOwnedStores(repositoryRoot, parentSessionId);
    if (parents.length > 1) throw new Error(`Multiple worker sessions claim parent session ${parentSessionId}`);
    const store = parents[0];
    if (!store) return null;
    const state = await store.load();
    const observedOwner = structuredClone(state.owner);
    const sameManager = observedOwner.pid === process.pid && observedOwner.processStartIdentity === this.processStartIdentity;
    if (!sameManager) {
      const status = await processIdentityStatus(observedOwner.pid, observedOwner.processStartIdentity);
      if (status === "live") return null;
      if (!processIdentityIsGone(status)) throw new Error(`Direct-fork transfer refused: source owner identity is ${status}`);
    }
    await store.mutate(async (draft) => {
      if (draft.ownerSessionId !== parentSessionId || canonicalOwnerIdentity(draft.owner) !== canonicalOwnerIdentity(observedOwner)) throw new Error("Direct-fork source changed during transfer");
      if (!sameManager) {
        const status = await processIdentityStatus(draft.owner.pid, draft.owner.processStartIdentity);
        if (!processIdentityIsGone(status)) throw new Error(`Direct-fork transfer refused after CAS recheck: source owner identity is ${status}`);
      }
      draft.lineage.push({ fromSessionId: parentSessionId, toSessionId: sessionId, transferredAt: nowIso(), transferId: `transfer-${newNonce(8)}` });
      draft.ownerSessionId = sessionId;
      draft.sessionFile = sessionFile;
      draft.owner = this.#owner(sessionId);
      if (draft.inFlightCompletionId) {
        if (!draft.completionQueue.includes(draft.inFlightCompletionId)) draft.completionQueue.unshift(draft.inFlightCompletionId);
        draft.inFlightCompletionId = null;
      }
    });
    return store;
  }

  async #hitFailpoint(name, context) { if (this.options.failpoint) await this.options.failpoint(name, context); }
  #owner(sessionId) { return { sessionId, pid: process.pid, processStartIdentity: this.processStartIdentity, attachedAt: nowIso() }; }
  #assertAttached() { if (!this.attached || !this.store) throw new Error("Worker manager is not attached to a top-level Pi session"); }
}

async function findOwnedStores(repositoryRoot, sessionId) {
  const root = workerSessionsRoot(repositoryRoot);
  const stores = [];
  for (const storageId of await listDirectories(root)) {
    const path = workerSessionPath(repositoryRoot, storageId);
    if (!(await pathExists(path))) continue;
    let state;
    try { state = await readJson(path); } catch { continue; }
    if (state.ownerSessionId === sessionId) stores.push(new WorkerSessionStore(repositoryRoot, storageId));
  }
  return stores;
}

function spawnDetachedSupervisor(supervisorPath, configPath, cwd) {
  const child = spawn(process.execPath, [supervisorPath, configPath], { cwd, detached: true, stdio: "ignore", env: { ...process.env } });
  if (!child.pid) throw new Error("Detached supervisor did not return a PID");
  child.unref();
  return child;
}

async function resolvePiCliPath(explicit) {
  if (explicit) return realpath(resolve(explicit));
  if (process.env.PI_DAG_PI_CLI_PATH) return realpath(resolve(process.env.PI_DAG_PI_CLI_PATH));
  const argvCandidate = process.argv[1];
  if (argvCandidate && ["cli.js", "pi"].includes(basename(argvCandidate))) {
    try { return await realpath(argvCandidate); } catch {}
  }
  for (const directory of String(process.env.PATH ?? "").split(":")) {
    if (!directory) continue;
    try { return await realpath(join(directory, "pi")); } catch {}
  }
  throw new Error("Unable to resolve the exact installed Pi CLI path");
}

async function waitForIdentity(pid) {
  for (let attempt = 0; attempt < 50; attempt++) {
    const identity = await processStartIdentity(pid);
    if (identity) return identity;
    await new Promise((resolveWait) => setTimeout(resolveWait, 10));
  }
  return null;
}

async function sessionIdFromFile(path) {
  if (!isAbsolute(path)) return null;
  try {
    const text = await readFileHead(path, 16 * 1024);
    const first = text.split(/\r?\n/).find((line) => line.trim());
    const header = first ? JSON.parse(first) : null;
    return header?.type === "session" && typeof header.id === "string" ? header.id : null;
  } catch { return null; }
}

function mailboxMatches(mailbox, state, worker, attempt) {
  return mailbox?.schemaVersion === 1
    && mailbox.storageId === state.storageId
    && mailbox.workerId === worker.id
    && mailbox.attemptNumber === attempt.attemptNumber
    && mailbox.attemptNonce === attempt.attemptNonce
    && mailbox.configHash === attempt.configHash
    && mailbox.ownerSessionId === attempt.launchSessionId
    && Number.isInteger(attempt.supervisorPid)
    && typeof attempt.supervisorStartIdentity === "string"
    && mailbox.supervisorPid === attempt.supervisorPid
    && mailbox.supervisorStartIdentity === attempt.supervisorStartIdentity
    && (!attempt.childPid || mailbox.childPid === attempt.childPid)
    && (!attempt.childStartIdentity || mailbox.childStartIdentity === attempt.childStartIdentity);
}

async function readArchivedWorker(state, record) {
  const path = resolve(state.repositoryRoot, record.archivedWorkerPath);
  assertCwdWithin(state.repositoryRoot, path);
  const archive = await readJson(path, { maxBytes: MAX_STATE_BYTES });
  const { archiveHash, ...payload } = archive ?? {};
  if (archiveHash !== record.archivedWorkerHash || sha256(payload) !== archiveHash || archive.kind !== "archived_worker" || archive.storageId !== state.storageId || archive.worker?.id !== record.workerId || archive.worker.launchKey !== record.launchKey || archive.worker.requestHash !== record.requestHash) throw new Error(`Archived worker binding is corrupt: ${record.workerId}`);
  return archive.worker;
}

async function sha256File(path) {
  const hash = createHash("sha256");
  await new Promise((resolveHash, reject) => {
    const stream = createReadStream(path);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.once("error", reject);
    stream.once("end", resolveHash);
  });
  return `sha256:${hash.digest("hex")}`;
}

async function syncDirectory(path) {
  const handle = await open(path, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}

async function assertRetrySafeProcessFact(state, worker, attempt) {
  if (typeof attempt.processDispositionFactPath !== "string" || typeof attempt.processDispositionFactHash !== "string") throw new Error("Retry-safe process disposition is missing its immutable proof");
  const path = resolve(state.repositoryRoot, attempt.processDispositionFactPath);
  assertCwdWithin(state.repositoryRoot, path);
  const fact = await readJson(path, { maxBytes: 64 * 1024 });
  const { factHash, ...payload } = fact ?? {};
  if (factHash !== attempt.processDispositionFactHash || sha256(payload) !== factHash || fact.kind !== "worker_process_disposition" || fact.storageId !== state.storageId || fact.workerId !== worker.id || fact.attemptNumber !== attempt.attemptNumber || fact.attemptNonce !== attempt.attemptNonce || fact.configHash !== attempt.configHash || fact.disposition !== "dead" || fact.retrySafe !== true) throw new Error("Retry-safe process disposition proof is corrupt or conflicts with the exact attempt");
}

function assertWorkingRootApprovalState(state, request) {
  const root = request?.workingRoot;
  if (root?.kind !== "approved_disposable") return;
  const approval = (state.approvedDisposableRoots ?? []).find((candidate) => candidate.approvalId === root.approvalId && !candidate.retiredAt);
  if (!approval || approval.path !== request.cwd || approval.realPath !== root.realPath || approval.dev !== root.dev || approval.ino !== root.ino) throw new Error("Disposable working-root approval changed before attempt reservation");
}

function processIdentityIsGone(disposition) { return disposition === "dead" || disposition === "mismatch"; }

function cancellationIntentMatches(left, right) {
  return left?.storageId === right.storageId && left?.workerId === right.workerId && left?.attemptNumber === right.attemptNumber && left?.attemptNonce === right.attemptNonce && left?.configHash === right.configHash && left?.reason === right.reason;
}

function canonicalOwnerIdentity(owner) {
  return JSON.stringify({ sessionId: owner?.sessionId ?? null, pid: owner?.pid ?? null, processStartIdentity: owner?.processStartIdentity ?? null });
}

function normalizeLaunchKey(value) {
  const launchKey = String(value ?? "");
  if (!launchKey || launchKey.length > 512 || Buffer.byteLength(launchKey) > 2048 || /[\u0000-\u001f\u007f]/.test(launchKey)) throw new Error("launchKey must be a non-empty opaque string without control characters");
  return launchKey;
}

function normalizeRepairAttempts(value) {
  const number = value === undefined ? 2 : Number(value);
  if (!Number.isInteger(number) || number < 0 || number > 2) throw new Error("reportRepairAttempts must be an integer from 0 through 2");
  return number;
}

function isStrictDescendant(parent, candidate) {
  const rel = relative(resolve(parent), resolve(candidate));
  return Boolean(rel) && !rel.startsWith("..") && !isAbsolute(rel);
}

function assertCwdWithin(repositoryRoot, cwd) {
  const rel = relative(resolve(repositoryRoot), resolve(cwd));
  if (rel.startsWith("..") || isAbsolute(rel)) throw new Error("Worker cwd must stay within the repository root");
}

async function verifiedWorkerSummary(state, worker) {
  const attempt = worker.attempts?.find((candidate) => candidate.attemptNumber === worker.currentAttempt);
  if (attempt?.retrySafe === true) await assertRetrySafeProcessFact(state, worker, attempt);
  return { id: worker.id, label: worker.label, launchKey: worker.launchKey ?? null, status: worker.status, currentAttempt: worker.currentAttempt, completionId: worker.completionId ?? null, processDisposition: attempt?.processDisposition ?? null, retrySafe: attempt?.retrySafe === true, processDispositionFactHash: attempt?.processDispositionFactHash ?? null, createdAt: worker.createdAt, updatedAt: worker.updatedAt };
}

function truncateUtf8(value, maxBytes) {
  const buffer = Buffer.from(String(value ?? ""));
  if (buffer.length <= maxBytes) return buffer.toString();
  return `${buffer.subarray(0, Math.max(0, maxBytes - 32)).toString()}\n…[truncated]`;
}
