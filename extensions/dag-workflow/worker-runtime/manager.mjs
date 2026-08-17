import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, mkdir, open, readFile, readdir, realpath, rename, stat } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  MAX_COMPLETION_MESSAGE_BYTES,
  MAX_MAILBOX_BYTES,
  MAX_RESULT_BYTES,
  MAX_STATE_BYTES,
  WorkerSessionStore,
  assertAttemptConfig,
  assertWorkerSession,
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

const execFileAsync = promisify(execFile);
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
    this.bindingOperationQueue = Promise.resolve();
    this.attached = false;
    this.processStartIdentity = null;
    this.cancellationTimers = new Map();
    this.dispatchingAttempts = new Set();
    this.terminalResultListeners = new Set();
  }

  onTerminalResult(listener) {
    if (typeof listener !== "function") throw new Error("Terminal-result listener must be a function");
    this.terminalResultListeners.add(listener);
    return () => this.terminalResultListeners.delete(listener);
  }

  #notifyTerminalResult(event) {
    for (const listener of this.terminalResultListeners) queueMicrotask(() => Promise.resolve(listener(event)).catch((error) => console.error(`Worker terminal-result listener failed: ${error.message}`)));
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

  async launchOwnedAttempt(input, ctx = this.context) {
    this.#assertAttached();
    const state = await this.store.load();
    const launchKey = normalizeLaunchKey(input.launchKey);
    const workerId = normalizeRuntimeId(input.workerId, "workerId");
    const baseCommit = String(input.baseCommit ?? "");
    if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(baseCommit)) throw new Error("Owned-worker launch requires one exact Git base commit");
    if (!/^sha256:[0-9a-f]{64}$/.test(String(input.configRequestHash ?? "")) || !Number.isInteger(Number(input.expectedAttemptNumber)) || Number(input.expectedAttemptNumber) < 1) throw new Error("Owned-worker launch requires exact config-request and attempt identities");
    const existingRecord = (state.launchRecords ?? []).find((candidate) => candidate.launchKey === launchKey);
    if (existingRecord) {
      const existingWorker = state.workers[existingRecord.workerId];
      if (!existingWorker || existingWorker.id !== workerId || existingWorker.normalizedRequest?.ownedWorktree?.baseCommit !== baseCommit || existingWorker.normalizedRequest?.boundConfigRequestHash !== input.configRequestHash) throw new Error("Owned-worker launch replay conflicts with its exact durable base/request identity");
      if (existingWorker.currentAttempt > 0) {
        const replay = await this.attemptIdentityByLaunchKey(launchKey);
        if (!replay || replay.workerId !== workerId || replay.attemptNumber !== Number(input.expectedAttemptNumber)) throw new Error("Owned-worker launch replay lacks the exact reserved attempt identity");
        return replay;
      }
    } else {
      const recovered = await recoverUnboundOwnedAttempt(state.repositoryRoot, {
        launchKey, workerId, expectedAttemptNumber: Number(input.expectedAttemptNumber), configRequestHash: String(input.configRequestHash), baseCommit,
        worktreeName: normalizeRuntimeId(input.worktreeKey ?? launchKey, "worktreeKey"), label: String(input.label ?? workerId).trim().slice(0, 256) || workerId, task: String(input.task ?? "").trim(),
      });
      if (recovered) return recovered;
    }
    const worktreeName = normalizeRuntimeId(input.worktreeKey ?? launchKey, "worktreeKey");
    const worktreeRoot = join(state.repositoryRoot, ".ai", "worker-roots", worktreeName);
    await mkdir(dirname(worktreeRoot), { recursive: true });
    let worktreeExists = true;
    try { await stat(worktreeRoot); } catch (error) { if (error?.code === "ENOENT") worktreeExists = false; else throw error; }
    if (!worktreeExists) await execFileAsync("git", ["worktree", "add", "--detach", worktreeRoot, baseCommit], { cwd: state.repositoryRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 });
    const exactWorktree = await inspectOwnedWorktreeExact(state.repositoryRoot, worktreeRoot, baseCommit);
    const canonicalRoot = exactWorktree.realPath;
    const refreshed = await this.store.load();
    let approval = (refreshed.approvedDisposableRoots ?? []).find((candidate) => !candidate.retiredAt && candidate.realPath === canonicalRoot && candidate.ownerSessionId === refreshed.ownerSessionId && canonicalOwnerIdentity(candidate.approvedByOwner) === canonicalOwnerIdentity(refreshed.owner));
    let disposableRootToken;
    if (!approval) {
      const created = await this.approveDisposableWorkingRoot(worktreeRoot);
      disposableRootToken = created.disposableRootToken;
      approval = (await this.store.load()).approvedDisposableRoots.find((candidate) => candidate.approvalId === created.approvalId);
    }
    if (!approval || approval.realPath !== canonicalRoot || approval.dev !== exactWorktree.dev || approval.ino !== exactWorktree.ino) throw new Error("Owned-worker worktree approval does not bind the exact path/device/inode");
    await this.launch({ launchKey, workerId, label: input.label, task: input.task, cwd: worktreeRoot, boundConfigRequestHash: input.configRequestHash, ownedWorktreeBaseCommit: baseCommit, ownedWorktreeCommonDir: exactWorktree.commonDir, ownedWorktreeObjectFormat: exactWorktree.objectFormat, ...(disposableRootToken ? { disposableRootToken } : { disposableApprovalId: approval.approvalId }) }, ctx);
    const exact = await this.attemptIdentityByLaunchKey(launchKey);
    if (!exact || exact.workerId !== workerId || exact.attemptNumber !== Number(input.expectedAttemptNumber)) throw new Error("Owned-worker launch did not bind the exact requested worker/attempt identity");
    return exact;
  }

  async attemptIdentityByLaunchKey(launchKey) {
    this.#assertAttached();
    const state = await this.store.load();
    const normalized = normalizeLaunchKey(launchKey);
    const record = (state.launchRecords ?? []).find((candidate) => candidate.launchKey === normalized);
    if (!record) return null;
    let worker = state.workers[record.workerId];
    if (!worker && record.archivedWorkerPath) worker = await readArchivedWorker(state, record);
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === (record.attemptNumber ?? worker.currentAttempt));
    if (!worker || !attempt) return null;
    const config = await readJson(resolve(state.repositoryRoot, attempt.configPath));
    assertAttemptConfig(config);
    const { configHash, ...configPayload } = config;
    const configFactCore = { kind: "worker_config", configHash, config: configPayload };
    return {
      workerStorageId: state.storageId,
      launchOwnerSessionId: config.ownerSessionId,
      workerId: worker.id,
      attemptNumber: attempt.attemptNumber,
      attemptNonce: attempt.attemptNonce,
      configHash,
      configFact: { ...configFactCore, hash: sha256(configFactCore) },
      supervisorPid: attempt.supervisorPid ?? 0,
      supervisorStartIdentity: attempt.supervisorStartIdentity ?? null,
      childPid: attempt.childPid ?? null,
      childStartIdentity: attempt.childStartIdentity ?? null,
      mailboxHash: attempt.mailboxHash ?? null,
      heartbeatAt: attempt.updatedAt ?? attempt.createdAt,
    };
  }

  async cleanupOwnedWorktreeForBinding(binding, input = {}) {
    return this.#withBindingStore(binding, () => this.#cleanupOwnedWorktreeForCurrentStore(binding, input));
  }

  async #cleanupOwnedWorktreeForCurrentStore(binding, input = {}) {
    this.#assertAttached();
    const effectId = String(input.effectId ?? "");
    const requestHash = String(input.requestHash ?? "");
    const launchKey = normalizeLaunchKey(input.launchKey);
    if (!effectId || !requestHash.startsWith("sha256:")) throw new Error("Owned-worktree cleanup requires an exact durable DAG effect identity");
    await this.scan();
    let state = await this.store.load();
    if (state.storageId !== binding.workerStorageId) throw new Error("Owned-worktree cleanup binding belongs to another manager storage identity");
    let worker = state.workers[binding.workerId];
    if (!worker) { const record = (state.launchRecords ?? []).find((candidate) => candidate.workerId === binding.workerId); if (record?.archivedWorkerPath) worker = await readArchivedWorker(state, record); }
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash);
    if (!worker || worker.launchKey !== launchKey || !attempt || !attempt.ingestedAt || attempt.processDisposition !== "dead" || attempt.retrySafe !== true) return null;
    await assertRetrySafeProcessFact(state, worker, attempt);
    const root = worker.normalizedRequest?.workingRoot;
    if (root?.kind !== "approved_disposable") throw new Error("Owned-worktree cleanup is not bound to an approved disposable root");
    const repositoryCommonDir = await gitCommonDir(state.repositoryRoot);
    await this.store.mutate((draft) => {
      draft.worktreeCleanupIntents ??= [];
      const existing = draft.worktreeCleanupIntents.find((candidate) => candidate.effectId === effectId);
      const identity = { effectId, requestHash, launchKey, workerStorageId: binding.workerStorageId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, path: root.path, realPath: root.realPath, dev: root.dev, ino: root.ino, approvalId: root.approvalId, commonDir: repositoryCommonDir };
      if (existing) {
        const existingIdentity = Object.fromEntries(Object.keys(identity).map((key) => [key, existing[key]]));
        if (sha256(existingIdentity) !== sha256(identity)) throw new Error("Owned-worktree cleanup effect conflicts with its durable manager intent");
        return;
      }
      draft.worktreeCleanupIntents.push({ ...identity, state: "intended", intendedAt: nowIso(), retiredAt: null, removedAt: null });
    });
    await this.#hitFailpoint("after_worktree_cleanup_intent", { effectId, workerId: binding.workerId });
    state = await this.store.load();
    const cleanup = state.worktreeCleanupIntents.find((candidate) => candidate.effectId === effectId);
    if (!cleanup || cleanup.requestHash !== requestHash) throw new Error("Durable owned-worktree cleanup intent is missing or conflicting");
    if (!cleanup.retiredAt) {
      await this.store.mutate((draft) => {
        const current = draft.worktreeCleanupIntents.find((candidate) => candidate.effectId === effectId);
        const approval = (draft.approvedDisposableRoots ?? []).find((candidate) => candidate.approvalId === cleanup.approvalId);
        if (!current || !approval || approval.realPath !== cleanup.realPath || approval.dev !== cleanup.dev || approval.ino !== cleanup.ino) throw new Error("Owned-worktree cleanup approval identity changed");
        if (!approval.retiredAt) approval.retiredAt = nowIso();
        current.retiredAt = approval.retiredAt; current.state = "retired";
      });
      await this.#hitFailpoint("after_worktree_cleanup_retirement", { effectId, workerId: binding.workerId });
    }
    let exists = true;
    try { const info = await stat(cleanup.realPath); if (!info.isDirectory() || String(info.dev) !== cleanup.dev || String(info.ino) !== cleanup.ino) throw new Error("Owned-worktree identity changed before removal"); }
    catch (error) { if (error?.code === "ENOENT") exists = false; else throw error; }
    if (exists) {
      const exact = await inspectOwnedWorktreeExact(state.repositoryRoot, cleanup.realPath, null, { allowDirty: true });
      if (exact.commonDir !== cleanup.commonDir) throw new Error("Owned-worktree common-dir identity changed before cleanup");
      await execFileAsync("git", ["worktree", "remove", "--force", cleanup.realPath], { cwd: state.repositoryRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 });
      await this.#hitFailpoint("after_worktree_cleanup_remove", { effectId, workerId: binding.workerId });
    } else if (await worktreeListed(state.repositoryRoot, cleanup.realPath)) throw new Error("Missing owned-worktree path remains registered; cleanup is ambiguous");
    await this.store.mutate((draft) => {
      const current = draft.worktreeCleanupIntents.find((candidate) => candidate.effectId === effectId);
      if (!current || current.requestHash !== requestHash) throw new Error("Owned-worktree cleanup authority changed before result commit");
      current.state = "removed"; current.removedAt ??= nowIso();
    });
    return "applied_exact";
  }

  async terminalResultForBinding(binding) {
    return this.#withBindingStore(binding, async () => {
      this.#assertAttached();
      await this.scan();
      const repositoryRoot = resolve(this.context.cwd);
      const store = new WorkerSessionStore(repositoryRoot, binding.workerStorageId);
      const state = await store.load();
    if (state.storageId !== binding.workerStorageId) return null;
    let worker = state.workers[binding.workerId];
    if (!worker) { const record = (state.launchRecords ?? []).find((candidate) => candidate.workerId === binding.workerId); if (record?.archivedWorkerPath) worker = await readArchivedWorker(state, record); }
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash);
    if (!attempt?.ingestedAt || !attempt.resultPath) return null;
    const result = await readJson(resolve(repositoryRoot, attempt.resultPath), { maxBytes: MAX_RESULT_BYTES });
    assertTerminalResult(result, { recovery: resolve(repositoryRoot, attempt.resultPath) === attemptPaths(repositoryRoot, state.storageId, worker.id, attempt.attemptNumber).recoveryResult });
    if (result.storageId !== binding.workerStorageId || result.ownerSessionId !== binding.launchOwnerSessionId || result.workerId !== binding.workerId || result.attemptNumber !== binding.attemptNumber || result.attemptNonce !== binding.attemptNonce || result.configHash !== binding.configHash) throw new Error("Terminal worker result conflicts with exact DAG binding");
      if (attempt.retrySafe === true) await assertRetrySafeProcessFact(state, worker, attempt);
      return { completionId: result.completionId, terminalStatus: result.terminalStatus, processDisposition: attempt.processDisposition ?? "ambiguous", retrySafe: attempt.retrySafe === true };
    });
  }

  async inspectBinding(binding) {
    return this.#withBindingStore(binding, async () => {
      await this.scan();
      const exact = await this.inspect(binding.workerId);
      if (exact?.attempt?.attemptNumber !== binding.attemptNumber || exact.attempt.attemptNonce !== binding.attemptNonce || exact.attempt.configHash !== binding.configHash) throw new Error("Worker inspection did not resolve the exact DAG-bound prior attempt");
      return exact;
    });
  }

  async cancelBinding(binding, reason) {
    return this.#withBindingStore(binding, async () => {
      await this.scan();
      const exact = await this.inspect(binding.workerId);
      if (exact?.worker?.currentAttempt !== binding.attemptNumber || exact.attempt?.attemptNumber !== binding.attemptNumber || exact.attempt.attemptNonce !== binding.attemptNonce || exact.attempt.configHash !== binding.configHash) throw new Error("Worker cancellation refused to target a different current attempt identity");
      return this.cancel(binding.workerId, reason);
    });
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
      requestHash: request.boundConfigRequestHash ?? workerBefore.requestHash,
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
      try { await writeImmutableJson(paths.launchReceipt, launchReceipt, { maxBytes: 64 * 1024 }); }
      catch (error) {
        await this.#quarantineAttemptArtifact(workerId, attemptNumber, paths.launchReceipt, "conflicting-or-corrupt-launch-receipt", `Published launch receipt conflicts with the exact spawned supervisor: ${error.message}`);
        await this.store.mutate((draft) => {
          const currentWorker = draft.workers[workerId];
          const currentAttempt = currentWorker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
          if (!currentAttempt || currentAttempt.attemptNonce !== attempt.attemptNonce || currentAttempt.configHash !== attempt.configHash) return;
          delete currentAttempt.supervisorPid;
          delete currentAttempt.supervisorStartIdentity;
          delete currentAttempt.launchReceiptPath;
          delete currentAttempt.launchReceiptHash;
          currentAttempt.status = "launch_ambiguous";
          currentAttempt.processDisposition = "ambiguous";
          currentAttempt.retrySafe = false;
          if (currentWorker.currentAttempt === attemptNumber) { currentWorker.status = "needs_attention"; currentWorker.updatedAt = nowIso(); }
        });
        throw new Error(`Supervisor launch receipt publication conflicted: ${error.message}`);
      }
      await this.#hitFailpoint("after_launch_receipt_publication", { workerId, attemptNumber, supervisorPid: processHandle.pid, supervisorStartIdentity, launchReceiptHash: launchReceipt.receiptHash });
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
    if (input.disposableRootToken !== undefined || input.disposableApprovalId !== undefined) {
      const tokenHash = input.disposableRootToken === undefined ? null : sha256(String(input.disposableRootToken));
      const approval = (state.approvedDisposableRoots ?? []).find((candidate) => (tokenHash ? candidate.tokenHash === tokenHash : candidate.approvalId === input.disposableApprovalId) && !candidate.retiredAt);
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
      boundConfigRequestHash: input.boundConfigRequestHash === undefined ? null : String(input.boundConfigRequestHash),
      ownedWorktree: input.ownedWorktreeBaseCommit === undefined ? null : { baseCommit: String(input.ownedWorktreeBaseCommit), commonDir: String(input.ownedWorktreeCommonDir), objectFormat: String(input.ownedWorktreeObjectFormat) },
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
      if (request.ownedWorktree) {
        const exact = await inspectOwnedWorktreeExact(state.repositoryRoot, request.cwd, request.ownedWorktree.baseCommit);
        if (exact.commonDir !== request.ownedWorktree.commonDir || exact.objectFormat !== request.ownedWorktree.objectFormat) throw new Error("Owned-worker worktree Git identity changed after reservation");
      }
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
    const { receiptHash, ...payload } = receipt ?? {};
    const canonical = receiptHash === sha256(payload)
      && receipt?.schemaVersion === 1
      && receipt?.kind === "worker_supervisor_launch"
      && Number.isInteger(receipt?.supervisorPid) && receipt.supervisorPid > 0
      && typeof receipt?.supervisorStartIdentity === "string" && receipt.supervisorStartIdentity.length > 0;
    if (!canonical) {
      await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.launchReceipt, "conflicting-or-corrupt-launch-receipt", "Launch receipt canonical identity or hash is invalid");
      throw new Error("Supervisor launch receipt is invalid or conflicting");
    }
    const receiptPath = relative(state.repositoryRoot, paths.launchReceipt);
    const capturedIdentityExact = receipt.storageId === state.storageId && receipt.ownerSessionId === attempt.launchSessionId && receipt.workerId === worker.id && receipt.attemptNumber === attempt.attemptNumber && receipt.attemptNonce === attempt.attemptNonce && receipt.configHash === attempt.configHash;
    if (capturedIdentityExact && attempt.supervisorPid === receipt.supervisorPid && attempt.supervisorStartIdentity === receipt.supervisorStartIdentity && attempt.launchReceiptHash === receipt.receiptHash && attempt.launchReceiptPath === receiptPath) return receipt;
    const hydrated = await this.store.mutate((draft) => {
      const currentWorker = draft.workers[worker.id];
      const current = currentWorker?.attempts.find((candidate) => candidate.attemptNumber === attempt.attemptNumber);
      const identityExact = draft.storageId === receipt.storageId
        && currentWorker?.id === receipt.workerId
        && current?.attemptNumber === receipt.attemptNumber
        && current?.launchSessionId === receipt.ownerSessionId
        && current?.attemptNonce === receipt.attemptNonce
        && current?.configHash === receipt.configHash
        && current?.attemptNonce === attempt.attemptNonce
        && current?.configHash === attempt.configHash;
      if (!identityExact) return { disposition: "conflict", reason: "Launch receipt conflicts with the fresh store worker/config/attempt identity" };
      const processConflict = (current.supervisorPid !== undefined && current.supervisorPid !== receipt.supervisorPid)
        || (current.supervisorStartIdentity !== undefined && current.supervisorStartIdentity !== receipt.supervisorStartIdentity);
      const receiptConflict = (current.launchReceiptHash !== undefined && current.launchReceiptHash !== receipt.receiptHash)
        || (current.launchReceiptPath !== undefined && current.launchReceiptPath !== receiptPath);
      if (processConflict || receiptConflict) return { disposition: "conflict", reason: "Launch receipt conflicts with fresh spawn-bound supervisor authority" };
      if (current.supervisorPid === receipt.supervisorPid && current.supervisorStartIdentity === receipt.supervisorStartIdentity && current.launchReceiptHash === receipt.receiptHash && current.launchReceiptPath === receiptPath) return { disposition: "replay" };
      current.supervisorPid = receipt.supervisorPid;
      current.supervisorStartIdentity = receipt.supervisorStartIdentity;
      current.launchReceiptPath = receiptPath;
      current.launchReceiptHash = receipt.receiptHash;
      if (!current.ingestedAt && current.status === "dispatching") {
        current.status = "running";
        current.processDisposition = "live";
        if (currentWorker.currentAttempt === current.attemptNumber && currentWorker.status !== "cancelling") { currentWorker.status = "running"; currentWorker.updatedAt = nowIso(); }
      }
      return { disposition: "hydrated" };
    });
    if (hydrated.result.disposition === "conflict") {
      await this.#quarantineAttemptArtifact(worker.id, attempt.attemptNumber, paths.launchReceipt, "conflicting-or-corrupt-launch-receipt", hydrated.result.reason);
      throw new Error("Supervisor launch receipt is invalid or conflicting");
    }
    attempt.supervisorPid = receipt.supervisorPid;
    attempt.supervisorStartIdentity = receipt.supervisorStartIdentity;
    attempt.launchReceiptPath = receiptPath;
    attempt.launchReceiptHash = receipt.receiptHash;
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
    } else if (ingestion.result?.adopted) this.#notifyTerminalResult({ workerId, attemptNumber, completionId: result.completionId, terminalStatus: result.terminalStatus });
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
      const output = await this.#withBindingStore(binding, async () => {
        await this.scan();
        const state = await this.store.load(); if (state.storageId !== binding.workerStorageId) return null;
        let worker = state.workers[binding.workerId]; if (!worker) { const record = (state.launchRecords ?? []).find(({ workerId }) => workerId === binding.workerId); if (record?.archivedWorkerPath) worker = await readArchivedWorker(state, record); }
        const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash); if (!attempt) return null; const config = await readJson(resolve(repositoryRoot, attempt.configPath)); if (config.ownerSessionId !== binding.launchOwnerSessionId) return null;
        return { storageId: state.storageId, launchOwnerSessionId: config.ownerSessionId, workerId: worker.id, attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, terminalStatus: attempt.ingestedAt ? attempt.status : null, processDisposition: attempt.processDisposition ?? "ambiguous", retrySafe: Boolean(attempt.retrySafe), resultHash: attempt.resultHash ?? null };
      });
      if (output) outputs.push(output);
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

  async #withBindingStore(binding, operation) {
    this.#assertAttached();
    const run = this.bindingOperationQueue.then(async () => {
      await this.scanQueue;
      const repositoryRoot = resolve(this.context.cwd);
      const canonicalRepositoryRoot = await realpath(repositoryRoot); const repositoryInfo = await lstat(repositoryRoot);
      if (canonicalRepositoryRoot !== repositoryRoot || !repositoryInfo.isDirectory() || repositoryInfo.isSymbolicLink()) throw new Error("DAG worker binding repository root is not canonical trusted storage authority");
      const sessionsRoot = workerSessionsRoot(repositoryRoot); const sessionsInfo = await assertTrustedDirectory(sessionsRoot, sessionsRoot, repositoryInfo.dev, "worker storage root");
      const boundStorageRoot = join(sessionsRoot, normalizeRuntimeId(binding.workerStorageId, "workerStorageId")); await assertTrustedDirectory(boundStorageRoot, boundStorageRoot, sessionsInfo.dev, "bound worker storage root");
      const originalStore = this.store;
      const originalState = await originalStore.load();
      const exactStore = originalState.storageId === binding.workerStorageId ? originalStore : new WorkerSessionStore(repositoryRoot, binding.workerStorageId);
      let exactState = await exactStore.load();
      if (exactState.storageId !== binding.workerStorageId || resolve(exactState.repositoryRoot) !== repositoryRoot) throw new Error("DAG worker binding storage identity does not resolve in the attached repository");
      let worker = exactState.workers[binding.workerId];
      if (!worker) { const record = (exactState.launchRecords ?? []).find((candidate) => candidate.workerId === binding.workerId); if (record?.archivedWorkerPath) worker = await readArchivedWorker(exactState, record); }
      const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === binding.attemptNumber && candidate.attemptNonce === binding.attemptNonce && candidate.configHash === binding.configHash);
      if (!worker || !attempt) throw new Error("DAG worker binding does not resolve the exact immutable prior attempt");
      const paths = attemptPaths(repositoryRoot, binding.workerStorageId, binding.workerId, binding.attemptNumber);
      if (attempt.configPath !== relative(repositoryRoot, paths.config)) throw new Error("DAG worker binding config path is not the canonical attempt storage path");
      await assertTrustedDirectory(paths.root, paths.root, sessionsInfo.dev, "bound worker attempt root");
      const config = await readTrustedJson(paths.config, paths.root, sessionsInfo.dev, 256 * 1024, "bound worker config");
      assertAttemptConfig(config);
      if (config.storageId !== binding.workerStorageId || config.ownerSessionId !== binding.launchOwnerSessionId || config.workerId !== binding.workerId || config.attemptNumber !== binding.attemptNumber || config.attemptNonce !== binding.attemptNonce || config.configHash !== binding.configHash) throw new Error("DAG worker binding conflicts with its immutable attempt config");
      let lineageSessionId = binding.launchOwnerSessionId;
      for (const transfer of exactState.lineage ?? []) {
        if (transfer.fromSessionId !== lineageSessionId) throw new Error("Bound worker storage contains a discontinuous immutable session lineage");
        lineageSessionId = transfer.toSessionId;
      }
      if (lineageSessionId !== exactState.ownerSessionId) throw new Error("Bound worker storage owner is not the immutable successor of the exact launch owner");
      if (exactStore !== originalStore) {
        const successorSessionId = originalState.ownerSessionId;
        const successorOwner = this.#owner(successorSessionId);
        const observedOwner = structuredClone(exactState.owner);
        const status = observedOwner?.pid === process.pid && observedOwner?.processStartIdentity === this.processStartIdentity ? "same_manager" : await processIdentityStatus(observedOwner?.pid, observedOwner?.processStartIdentity);
        if (status !== "same_manager" && !processIdentityIsGone(status)) throw new Error(`Bound worker storage has a non-dead owner identity (${status}); refusing a second manager identity`);
        await exactStore.mutate(async (draft) => {
          if (canonicalOwnerIdentity(draft.owner) !== canonicalOwnerIdentity(observedOwner) || draft.ownerSessionId !== exactState.ownerSessionId) throw new Error("Bound worker storage owner changed during proven-dead transfer");
          if (status !== "same_manager") {
            const rechecked = await processIdentityStatus(draft.owner.pid, draft.owner.processStartIdentity);
            if (!processIdentityIsGone(rechecked)) throw new Error(`Bound worker storage owner revived or is ambiguous (${rechecked})`);
          }
          draft.lineage ??= [];
          draft.lineage.push({
            fromSessionId: draft.ownerSessionId, toSessionId: successorSessionId, transferredAt: nowIso(), transferId: `binding-transfer-${newNonce(8)}`,
            disposition: status === "same_manager" ? "same_manager" : "proven_dead", priorOwner: structuredClone(draft.owner), successorOwner: structuredClone(successorOwner),
            immutableBindingHash: sha256({ workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash }),
          });
          draft.ownerSessionId = successorSessionId; draft.sessionFile = originalState.sessionFile; draft.owner = successorOwner;
          if (draft.inFlightCompletionId) { if (!draft.completionQueue.includes(draft.inFlightCompletionId)) draft.completionQueue.unshift(draft.inFlightCompletionId); draft.inFlightCompletionId = null; }
        });
        exactState = await exactStore.load();
      }
      this.store = exactStore;
      try { return await operation(exactState); }
      finally { this.store = originalStore; }
    });
    this.bindingOperationQueue = run.then(() => undefined, () => undefined);
    return run;
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

async function recoverUnboundOwnedAttempt(repositoryRootValue, expected) {
  const repositoryRoot = resolve(repositoryRootValue);
  const canonicalRepositoryRoot = await realpath(repositoryRoot);
  if (canonicalRepositoryRoot !== repositoryRoot) throw new Error("Unbound launch recovery requires a canonical repository root without symlink indirection");
  const repositoryInfo = await lstat(repositoryRoot);
  if (!repositoryInfo.isDirectory() || repositoryInfo.isSymbolicLink()) throw new Error("Unbound launch recovery repository root is not a trusted directory");
  const sessionsRoot = workerSessionsRoot(repositoryRoot);
  let rootInfo;
  try { rootInfo = await assertTrustedDirectory(sessionsRoot, sessionsRoot, repositoryInfo.dev, "worker storage root"); }
  catch (error) { if (error?.code === "ENOENT") return null; throw error; }
  const entries = await readdir(sessionsRoot, { withFileTypes: true });
  const matches = []; const conflicts = [];
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    if (!entry.isDirectory() || entry.isSymbolicLink() || normalizeRuntimeId(entry.name, "storageId") !== entry.name) throw new Error(`Unbound launch recovery found an untrusted worker storage entry: ${entry.name}`);
    const storageRoot = join(sessionsRoot, entry.name);
    await assertTrustedDirectory(storageRoot, storageRoot, rootInfo.dev, `worker storage ${entry.name}`);
    const sessionPath = workerSessionPath(repositoryRoot, entry.name);
    const state = await readTrustedJson(sessionPath, storageRoot, rootInfo.dev, MAX_STATE_BYTES, `worker session ${entry.name}`);
    assertWorkerSession(state);
    if (state.storageId !== entry.name || resolve(state.repositoryRoot) !== repositoryRoot) throw new Error(`Unbound launch recovery worker session ${entry.name} has conflicting repository/storage identity`);
    const records = (state.launchRecords ?? []).filter((record) => record.launchKey === expected.launchKey);
    const workerById = state.workers[expected.workerId];
    if (!records.length && !workerById) continue;
    try {
      const storeOwnerStatus = state.owner?.pid === process.pid && state.owner?.processStartIdentity === await processStartIdentity() ? "same_manager" : await processIdentityStatus(state.owner?.pid, state.owner?.processStartIdentity);
      if (storeOwnerStatus !== "same_manager" && !processIdentityIsGone(storeOwnerStatus)) throw new Error(`worker session has a non-dead owner identity (${storeOwnerStatus})`);
      if (records.length !== 1 || records[0].workerId !== expected.workerId || !workerById || records[0].archivedWorkerPath) throw new Error("launch key/worker slot is not one exact active record");
      const record = records[0]; const worker = workerById;
      if (worker.launchKey !== expected.launchKey || worker.id !== expected.workerId || worker.currentAttempt !== expected.expectedAttemptNumber || record.attemptNumber !== expected.expectedAttemptNumber) throw new Error("launch record and worker attempt slot differ");
      if (!worker.normalizedRequest || worker.requestHash !== sha256(worker.normalizedRequest) || record.requestHash !== worker.requestHash) throw new Error("launch record does not bind the immutable normalized request hash");
      const request = worker.normalizedRequest;
      const expectedWorktree = join(repositoryRoot, ".ai", "worker-roots", expected.worktreeName);
      const exactRequest = request.schemaVersion === 1 && request.task === expected.task && worker.task === expected.task && worker.label === expected.label
        && request.requestedWorkerId === expected.workerId && request.requestedLabel === expected.label && request.boundConfigRequestHash === expected.configRequestHash
        && request.cwd === expectedWorktree && worker.cwd === expectedWorktree && request.ownedWorktree?.baseCommit === expected.baseCommit
        && request.workingRoot?.kind === "approved_disposable" && request.workingRoot.path === expectedWorktree && request.workingRoot.realPath === expectedWorktree;
      if (!exactRequest) throw new Error("normalized owned-worker request differs from the exact DAG launch request");
      const approval = (state.approvedDisposableRoots ?? []).find((candidate) => candidate.approvalId === request.workingRoot.approvalId);
      const worktreeInfo = await assertTrustedDirectory(expectedWorktree, expectedWorktree, null, "owned worker worktree");
      if (!approval || approval.retiredAt || approval.path !== expectedWorktree || approval.realPath !== expectedWorktree || approval.dev !== String(worktreeInfo.dev) || approval.ino !== String(worktreeInfo.ino) || request.workingRoot.dev !== approval.dev || request.workingRoot.ino !== approval.ino) throw new Error("owned worktree approval does not bind the exact path/device/inode");
      if (!isStrictDescendant(join(repositoryRoot, ".ai", "worker-roots"), expectedWorktree) || !await worktreeListed(repositoryRoot, expectedWorktree) || await gitCommonDir(expectedWorktree) !== await gitCommonDir(repositoryRoot) || await gitObjectFormat(expectedWorktree) !== request.ownedWorktree.objectFormat || request.ownedWorktree.commonDir !== await gitCommonDir(repositoryRoot)) throw new Error("owned worktree escaped or changed its exact Git common-dir/object identity");
      const attempt = worker.attempts.find((candidate) => candidate.attemptNumber === expected.expectedAttemptNumber);
      if (!attempt || attempt.attemptNonce.length < 16 || attempt.launchKey !== expected.launchKey || attempt.requestHash !== worker.requestHash || attempt.launchSessionId === undefined || approval.ownerSessionId !== attempt.launchSessionId || canonicalOwnerIdentity(approval.approvedByOwner) !== canonicalOwnerIdentity(attempt.launchOwner) || canonicalOwnerIdentity(attempt.dispatchOwner) !== canonicalOwnerIdentity(attempt.launchOwner)) throw new Error("worker attempt does not bind the exact immutable launch/approval/dispatch generation");
      if ((state.quarantinedArtifacts ?? []).some((artifact) => artifact.workerId === worker.id && artifact.attemptNumber === attempt.attemptNumber)) throw new Error("matching attempt has quarantined conflicting artifacts");
      const paths = attemptPaths(repositoryRoot, state.storageId, worker.id, attempt.attemptNumber);
      await assertTrustedDirectory(paths.root, paths.root, rootInfo.dev, "worker attempt root");
      let quarantineEntries = [];
      try { await assertTrustedDirectory(paths.quarantine, paths.quarantine, rootInfo.dev, "worker attempt quarantine"); quarantineEntries = await readdir(paths.quarantine); }
      catch (error) { if (error?.code !== "ENOENT") throw error; }
      if (quarantineEntries.length) throw new Error("matching attempt has an unindexed quarantine conflict");
      if (attempt.configPath !== relative(repositoryRoot, paths.config) || attempt.launchReceiptPath !== relative(repositoryRoot, paths.launchReceipt)) throw new Error("attempt artifact paths are not the canonical storage paths");
      const config = await readTrustedJson(paths.config, paths.root, rootInfo.dev, 256 * 1024, "worker attempt config");
      assertAttemptConfig(config);
      const configExact = config.storageId === state.storageId && config.ownerSessionId === attempt.launchSessionId && config.workerId === worker.id && config.attemptNumber === attempt.attemptNumber && config.attemptNonce === attempt.attemptNonce && config.configHash === attempt.configHash
        && config.launchKey === expected.launchKey && config.requestHash === expected.configRequestHash && config.repositoryRoot === repositoryRoot && config.cwd === expectedWorktree && config.task === expected.task
        && canonicalOwnerIdentity(config.launchOwner) === canonicalOwnerIdentity(attempt.launchOwner);
      if (!configExact) throw new Error("immutable worker config differs from the exact session/launch/request identity");
      const receipt = await readTrustedJson(paths.launchReceipt, paths.root, rootInfo.dev, 64 * 1024, "worker launch receipt");
      const { receiptHash, ...receiptPayload } = receipt ?? {};
      const receiptExact = receiptHash === sha256(receiptPayload) && receipt.schemaVersion === 1 && receipt.kind === "worker_supervisor_launch" && receipt.storageId === state.storageId && receipt.ownerSessionId === config.ownerSessionId
        && receipt.workerId === worker.id && receipt.attemptNumber === attempt.attemptNumber && receipt.attemptNonce === attempt.attemptNonce && receipt.configHash === attempt.configHash && receipt.observedAt === attempt.createdAt
        && Number.isInteger(receipt.supervisorPid) && receipt.supervisorPid > 0 && typeof receipt.supervisorStartIdentity === "string" && receipt.supervisorStartIdentity.length > 0
        && attempt.supervisorPid === receipt.supervisorPid && attempt.supervisorStartIdentity === receipt.supervisorStartIdentity && attempt.launchReceiptHash === receipt.receiptHash;
      if (!receiptExact) throw new Error("immutable launch receipt differs from the exact config/attempt/process identity");
      const processStatus = await processIdentityStatus(receipt.supervisorPid, receipt.supervisorStartIdentity);
      if (processStatus !== "live") await assertRecoveredTerminalProcessIdentity(state, worker, attempt, paths, receipt, rootInfo.dev, processStatus);
      let lineageSessionId = config.ownerSessionId;
      for (const transfer of state.lineage ?? []) { if (transfer.fromSessionId !== lineageSessionId || typeof transfer.toSessionId !== "string" || !transfer.toSessionId) throw new Error("worker session lineage is discontinuous"); lineageSessionId = transfer.toSessionId; }
      if (lineageSessionId !== state.ownerSessionId) throw new Error("worker session owner is not the exact launch-owner lineage successor");
      const { configHash, ...configPayload } = config; const configFactCore = { kind: "worker_config", configHash, config: configPayload };
      matches.push({ workerStorageId: state.storageId, launchOwnerSessionId: config.ownerSessionId, workerId: worker.id, attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash, configFact: { ...configFactCore, hash: sha256(configFactCore) }, supervisorPid: receipt.supervisorPid, supervisorStartIdentity: receipt.supervisorStartIdentity, childPid: attempt.childPid ?? null, childStartIdentity: attempt.childStartIdentity ?? null, mailboxHash: attempt.mailboxHash ?? null, heartbeatAt: attempt.updatedAt ?? attempt.createdAt });
    } catch (error) { conflicts.push(`${entry.name}: ${error.message}`); }
  }
  if (conflicts.length) throw new Error(`Unbound owned-worker launch recovery failed closed: ${conflicts.join("; ")}`);
  if (matches.length > 1) throw new Error("Unbound owned-worker launch recovery found multiple exact manager-store matches");
  return matches[0] ?? null;
}

async function assertRecoveredTerminalProcessIdentity(state, worker, attempt, paths, receipt, device, processStatus) {
  if (processStatus !== "dead") throw new Error(`launch receipt process identity is ${processStatus}`);
  const existing = [];
  for (const [path, recovery] of [[paths.result, false], [paths.recoveryResult, true]]) {
    try { existing.push({ path, recovery, result: await readTrustedJson(path, paths.root, device, MAX_RESULT_BYTES, recovery ? "worker recovery result" : "worker primary result") }); }
    catch (error) { if (error?.code !== "ENOENT") throw error; }
  }
  if (existing.length !== 1) throw new Error("dead recovered supervisor lacks one unique exact terminal result");
  const terminal = existing[0]; assertTerminalResult(terminal.result, { recovery: terminal.recovery });
  const exact = terminal.result.storageId === state.storageId && terminal.result.ownerSessionId === attempt.launchSessionId && terminal.result.workerId === worker.id && terminal.result.attemptNumber === attempt.attemptNumber && terminal.result.attemptNonce === attempt.attemptNonce && terminal.result.configHash === attempt.configHash;
  const processExact = terminal.recovery || (terminal.result.process?.supervisorPid === receipt.supervisorPid && terminal.result.process?.supervisorStartIdentity === receipt.supervisorStartIdentity);
  if (!exact || !processExact) throw new Error("terminal result conflicts with recovered supervisor/config identity");
}

async function assertTrustedDirectory(path, expectedRealPath, device, label) {
  const info = await lstat(path);
  if (!info.isDirectory() || info.isSymbolicLink() || (device !== null && info.dev !== device) || await realpath(path) !== expectedRealPath) throw new Error(`${label} is a symlink, device escape, or noncanonical path`);
  return info;
}

async function readTrustedJson(path, trustedRoot, device, maxBytes, label) {
  if (!isStrictDescendant(trustedRoot, path)) throw new Error(`${label} escapes its trusted storage root`);
  const before = await lstat(path);
  if (!before.isFile() || before.isSymbolicLink() || before.dev !== device || before.nlink !== 1 || before.size > maxBytes || await realpath(path) !== path) throw new Error(`${label} is a symlink, device escape, hard-link alias, oversized artifact, or noncanonical path`);
  const bytes = await readFile(path);
  const after = await lstat(path);
  if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size || before.mtimeMs !== after.mtimeMs || !after.isFile() || after.isSymbolicLink()) throw new Error(`${label} changed during stable read`);
  return JSON.parse(bytes.toString("utf8"));
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

async function gitCommonDir(cwd) {
  const raw = (await execFileAsync("git", ["rev-parse", "--git-common-dir"], { cwd, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 })).stdout.trim();
  return realpath(resolve(cwd, raw));
}

async function gitObjectFormat(cwd) {
  const result = await execFileAsync("git", ["rev-parse", "--show-object-format"], { cwd, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 });
  const format = result.stdout.trim();
  if (!["sha1", "sha256"].includes(format)) throw new Error(`Unsupported Git object format: ${format}`);
  return format;
}

async function inspectOwnedWorktreeExact(repositoryRoot, worktreeRoot, expectedBaseCommit, options = {}) {
  const canonicalRoot = await realpath(worktreeRoot);
  const info = await stat(canonicalRoot);
  if (!info.isDirectory()) throw new Error("Owned-worker disposable worktree is not a directory");
  const top = (await execFileAsync("git", ["rev-parse", "--show-toplevel"], { cwd: canonicalRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 })).stdout.trim();
  if (await realpath(top) !== canonicalRoot) throw new Error("Owned-worker disposable worktree identity is not exact");
  const commonDir = await gitCommonDir(canonicalRoot);
  const repositoryCommonDir = await gitCommonDir(repositoryRoot);
  if (commonDir !== repositoryCommonDir) throw new Error("Owned-worker worktree does not share the exact repository Git common-dir");
  const objectFormat = await gitObjectFormat(canonicalRoot);
  if (objectFormat !== await gitObjectFormat(repositoryRoot)) throw new Error("Owned-worker worktree object format conflicts with the repository");
  const head = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd: canonicalRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 })).stdout.trim();
  if (expectedBaseCommit && head !== expectedBaseCommit) throw new Error(`Existing owned-worker worktree HEAD ${head} does not match exact base ${expectedBaseCommit}`);
  const symbolic = await execFileAsync("git", ["symbolic-ref", "-q", "HEAD"], { cwd: canonicalRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 }).then(() => true, (error) => { if (error?.code === 1) return false; throw error; });
  if (symbolic) throw new Error("Owned-worker disposable worktree must remain detached");
  const status = (await execFileAsync("git", ["status", "--porcelain=v2", "--untracked-files=all"], { cwd: canonicalRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 })).stdout;
  if (!options.allowDirty && status.trim()) throw new Error("Owned-worker disposable worktree must be clean before launch or cleanup");
  return { realPath: canonicalRoot, dev: String(info.dev), ino: String(info.ino), commonDir, objectFormat, head };
}

async function worktreeListed(repositoryRoot, path) {
  const output = (await execFileAsync("git", ["worktree", "list", "--porcelain"], { cwd: repositoryRoot, env: gitWorktreeEnvironment(), maxBuffer: 1024 * 1024 })).stdout;
  const target = resolve(path);
  return output.split(/\r?\n/).some((line) => line.startsWith("worktree ") && resolve(line.slice("worktree ".length)) === target);
}

function gitWorktreeEnvironment() {
  return { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
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
