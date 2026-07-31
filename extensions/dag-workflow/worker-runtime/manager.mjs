import { spawn } from "node:child_process";
import { realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  MAX_COMPLETION_MESSAGE_BYTES,
  MAX_MAILBOX_BYTES,
  MAX_RESULT_BYTES,
  WorkerSessionStore,
  assertAttemptConfig,
  assertTerminalResult,
  atomicWriteJson,
  attemptPaths,
  createWorkerSession,
  listDirectories,
  newNonce,
  normalizeRuntimeId,
  nowIso,
  pathExists,
  processIdentityStatus,
  processStartIdentity,
  readFileHead,
  readFileTail,
  readJson,
  sha256,
  withConfigHash,
  withResultHash,
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
    this.scanning = false;
    this.attached = false;
    this.processStartIdentity = null;
    this.cancellationTimers = new Map();
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
    this.attached = true;
    await this.scan();
    await this.dispatchNext();
    this.timer = setInterval(() => { void this.scan(); }, this.options.watchIntervalMs ?? 1000);
    this.timer.unref?.();
    return this.summary();
  }

  async detach() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const timer of this.cancellationTimers.values()) clearTimeout(timer);
    this.cancellationTimers.clear();
    this.attached = false;
    const store = this.store;
    while (this.scanning) await new Promise((resolveWait) => setTimeout(resolveWait, 10));
    if (store?.queue) await store.queue;
    this.context = null;
    this.store = null;
  }

  async launch(input, ctx = this.context) {
    this.#assertAttached();
    const task = String(input.task ?? "").trim();
    if (!task) throw new Error("subagent task is required");
    if (Buffer.byteLength(task) > 64 * 1024) throw new Error("subagent task exceeds 64 KiB");
    const state = await this.store.load();
    const workerId = normalizeRuntimeId(input.workerId ?? `worker-${Date.now().toString(36)}-${newNonce(5)}`, "workerId");
    if (state.workers[workerId]) throw new Error(`Worker already exists: ${workerId}`);
    const label = String(input.label ?? workerId).trim().slice(0, 256) || workerId;
    const cwd = resolve(input.cwd ?? state.repositoryRoot);
    assertCwdWithin(state.repositoryRoot, cwd);
    const worker = {
      id: workerId,
      label,
      task,
      cwd,
      status: "launching",
      createdAt: nowIso(),
      updatedAt: nowIso(),
      currentAttempt: 0,
      attempts: [],
      launchOptions: {
        ...(input.provider ? { provider: String(input.provider) } : {}),
        ...(input.model ? { model: String(input.model) } : {}),
        ...(input.thinking ? { thinking: String(input.thinking) } : {}),
        reportRepairAttempts: normalizeRepairAttempts(input.reportRepairAttempts),
      },
    };
    await this.store.mutate((draft) => { draft.workers[workerId] = worker; });
    return this.#launchAttempt(workerId, ctx);
  }

  async retry(workerId, ctx = this.context) {
    this.#assertAttached();
    const state = await this.store.load();
    const worker = state.workers[workerId];
    if (!worker) throw new Error(`Unknown worker: ${workerId}`);
    if (!TERMINAL_STATUSES.has(worker.status)) throw new Error(`Worker ${workerId} is not terminal`);
    return this.#launchAttempt(workerId, ctx);
  }

  async #launchAttempt(workerId, ctx) {
    const state = await this.store.load();
    const worker = state.workers[workerId];
    if (!worker) throw new Error(`Unknown worker: ${workerId}`);
    const attemptNumber = worker.currentAttempt + 1;
    const attemptNonce = newNonce();
    const piCliPath = await resolvePiCliPath(this.options.piCliPath);
    const activeTools = [...new Set((this.pi.getActiveTools?.() ?? []).map(String))].sort();
    const config = withConfigHash({
      schemaVersion: 1,
      storageId: state.storageId,
      ownerSessionId: state.ownerSessionId,
      workerId,
      label: worker.label,
      attemptNumber,
      attemptNonce,
      repositoryRoot: state.repositoryRoot,
      cwd: worker.cwd,
      task: worker.task,
      activeTools,
      reportRepairAttempts: worker.launchOptions.reportRepairAttempts,
      piCliPath,
      extensionPath: this.options.extensionPath ?? extensionPath,
      ...(worker.launchOptions.provider ?? ctx?.model?.provider ? { provider: worker.launchOptions.provider ?? ctx.model.provider } : {}),
      ...(worker.launchOptions.model ?? ctx?.model?.id ? { model: worker.launchOptions.model ?? ctx.model.id } : {}),
      ...(worker.launchOptions.thinking ?? ctx?.thinkingLevel ? { thinking: worker.launchOptions.thinking ?? ctx.thinkingLevel } : {}),
      createdAt: nowIso(),
    });
    assertAttemptConfig(config);
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    await writeImmutableJson(paths.config, config);
    await this.store.mutate((draft) => {
      const current = draft.workers[workerId];
      if (!current || current.currentAttempt !== attemptNumber - 1) throw new Error(`Worker ${workerId} attempt changed concurrently`);
      current.currentAttempt = attemptNumber;
      current.status = "launching";
      current.updatedAt = nowIso();
      current.attempts.push({
        attemptNumber,
        attemptNonce,
        configHash: config.configHash,
        configPath: relative(state.repositoryRoot, paths.config),
        status: "planned",
        launchSessionId: state.ownerSessionId,
        createdAt: nowIso(),
      });
    });

    let processHandle;
    try {
      const launch = this.options.spawnSupervisor ?? spawnDetachedSupervisor;
      processHandle = await launch(this.options.supervisorPath ?? defaultSupervisorPath, paths.config, worker.cwd);
    } catch (error) {
      await this.#writeRecoveryResult(workerId, attemptNumber, "failed", `Supervisor launch failed: ${error.message}`);
      await this.scan();
      throw error;
    }
    const supervisorStartIdentity = await waitForIdentity(processHandle.pid);
    await this.store.mutate((draft) => {
      const current = draft.workers[workerId];
      const attempt = current?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!attempt || attempt.attemptNonce !== attemptNonce) throw new Error(`Worker ${workerId} attempt generation changed during launch`);
      attempt.supervisorPid = processHandle.pid;
      attempt.supervisorStartIdentity = supervisorStartIdentity;
      attempt.status = supervisorStartIdentity ? "running" : "launch_ambiguous";
      current.status = supervisorStartIdentity ? "running" : "needs_attention";
      current.updatedAt = nowIso();
    });
    return { workerId, attemptNumber, status: supervisorStartIdentity ? "running" : "needs_attention", asynchronous: true };
  }

  async scan() {
    if (!this.attached || this.scanning) return;
    this.scanning = true;
    try {
      const state = await this.store.load();
      for (const worker of Object.values(state.workers)) {
        for (const attempt of worker.attempts ?? []) {
          if (attempt.ingestedAt) continue;
          const paths = attemptPaths(state.repositoryRoot, state.storageId, worker.id, attempt.attemptNumber);
          if (await pathExists(paths.result)) {
            await this.#ingestResult(worker.id, attempt.attemptNumber, paths.result, false);
            continue;
          }
          if (await pathExists(paths.recoveryResult)) {
            await this.#ingestResult(worker.id, attempt.attemptNumber, paths.recoveryResult, true);
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
              await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", `Mailbox recovery failed closed: ${error.message}`);
            }
            continue;
          }
          await this.#reconcileMissingAttempt(state, worker, attempt);
        }
      }
      await this.dispatchNext();
    } finally { this.scanning = false; }
  }

  async #reconcileMissingAttempt(state, worker, attempt) {
    const age = Date.now() - Date.parse(attempt.createdAt);
    if (age < (this.options.launchGraceMs ?? 10_000)) return;
    if (!attempt.supervisorPid || !attempt.supervisorStartIdentity) {
      await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", "Launch ownership remained ambiguous after the recovery grace period");
      return;
    }
    const status = await processIdentityStatus(attempt.supervisorPid, attempt.supervisorStartIdentity);
    if (status === "live") return;
    if (status === "dead") {
      await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", "Supervisor process died without an immutable result");
      return;
    }
    await this.#writeRecoveryResult(worker.id, attempt.attemptNumber, "lost", `Supervisor ownership is ${status}; refusing to signal or relaunch`);
  }

  async #writeRecoveryResult(workerId, attemptNumber, terminalStatus, summary, allowPrimaryResult = false) {
    const state = await this.store.load();
    const worker = state.workers[workerId];
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
    if (!attempt) return;
    const paths = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber);
    if ((!allowPrimaryResult && await pathExists(paths.result)) || await pathExists(paths.recoveryResult)) return;
    const seed = sha256({ storageId: state.storageId, workerId, attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash });
    const result = withResultHash({
      schemaVersion: 1,
      completionId: `completion-${workerId}-${attemptNumber}-${seed.slice(7, 19)}`,
      storageId: state.storageId,
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
      diagnostics: { path: relative(state.repositoryRoot, paths.diagnostics), cappedAtBytes: 50 * 1024 * 1024, truncated: false, eventCounts: {} },
      artifacts: [],
    });
    await writeImmutableJson(paths.recoveryResult, result, { maxBytes: MAX_RESULT_BYTES });
  }

  async #ingestResult(workerId, attemptNumber, path, recovery) {
    let result;
    try {
      result = await readJson(path, { maxBytes: MAX_RESULT_BYTES });
      assertTerminalResult(result);
    } catch (error) {
      if (!recovery) {
        await this.#writeRecoveryResult(workerId, attemptNumber, "lost", `Terminal result is corrupt: ${error.message}`, true);
        const state = await this.store.load();
        const recoveryPath = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber).recoveryResult;
        if (await pathExists(recoveryPath)) await this.#ingestResult(workerId, attemptNumber, recoveryPath, true);
      }
      return;
    }
    const state = await this.store.load();
    const worker = state.workers[workerId];
    const attempt = worker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
    if (!attempt) return;
    if (result.storageId !== state.storageId || result.workerId !== workerId || result.attemptNumber !== attemptNumber || result.attemptNonce !== attempt.attemptNonce || result.configHash !== attempt.configHash || result.ownerSessionId !== attempt.launchSessionId) {
      if (!recovery) {
        await this.#writeRecoveryResult(workerId, attemptNumber, "lost", "Terminal result identity mismatch", true);
        const recoveryPath = attemptPaths(state.repositoryRoot, state.storageId, workerId, attemptNumber).recoveryResult;
        if (await pathExists(recoveryPath)) await this.#ingestResult(workerId, attemptNumber, recoveryPath, true);
      }
      return;
    }
    await this.store.mutate((draft) => {
      const currentWorker = draft.workers[workerId];
      const currentAttempt = currentWorker?.attempts.find((candidate) => candidate.attemptNumber === attemptNumber);
      if (!currentAttempt || currentAttempt.ingestedAt) return;
      currentAttempt.ingestedAt = nowIso();
      currentAttempt.status = result.terminalStatus;
      currentAttempt.resultPath = relative(state.repositoryRoot, path);
      currentAttempt.resultHash = result.resultHash;
      currentAttempt.completionId = result.completionId;
      if (currentWorker.currentAttempt === attemptNumber) {
        currentWorker.status = result.terminalStatus;
        currentWorker.updatedAt = nowIso();
        currentWorker.completionId = result.completionId;
      }
      const known = draft.completedCompletionIds.includes(result.completionId) || draft.completionQueue.includes(result.completionId) || draft.inFlightCompletionId === result.completionId;
      if (!known && currentWorker.currentAttempt === attemptNumber) draft.completionQueue.push(result.completionId);
    });
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
    if (acknowledged) setTimeout(() => { void this.dispatchNext(); }, 0);
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
    if ((!pid || !identity) && await pathExists(paths.mailbox)) {
      const mailbox = await readJson(paths.mailbox, { maxBytes: MAX_MAILBOX_BYTES });
      if (!mailboxMatches(mailbox, state, worker, attempt)) throw new Error("Cancellation refused: mailbox identity mismatch");
      pid = mailbox.supervisorPid;
      identity = mailbox.supervisorStartIdentity;
    }
    const processStatus = await processIdentityStatus(pid, identity);
    if (processStatus !== "live") throw new Error(`Cancellation refused: supervisor ownership is ${processStatus}`);
    await atomicWriteJson(paths.cancel, { attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, requestedAt: nowIso(), reason: String(reason).slice(0, 1024) }, { maxBytes: 16 * 1024 });
    await this.store.mutate((draft) => {
      const current = draft.workers[workerId];
      if (current && !TERMINAL_STATUSES.has(current.status)) { current.status = "cancelling"; current.updatedAt = nowIso(); }
    });
    this.#scheduleCancellationEscalation(workerId, attempt, pid, identity);
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

  async status(workerId) {
    this.#assertAttached();
    const state = await this.store.load();
    if (workerId) {
      const worker = state.workers[workerId];
      if (!worker) throw new Error(`Unknown worker: ${workerId}`);
      return summarizeWorker(worker);
    }
    return Object.values(state.workers).sort((a, b) => a.createdAt.localeCompare(b.createdAt)).map(summarizeWorker);
  }

  async inspect(id) {
    this.#assertAttached();
    const state = await this.store.load();
    const directWorker = state.workers[id];
    const worker = directWorker ?? Object.values(state.workers).find((candidate) => candidate.attempts.some((attempt) => attempt.completionId === id));
    if (!worker) throw new Error(`Unknown worker or completion: ${id}`);
    const attempt = directWorker
      ? worker.attempts.find((candidate) => candidate.attemptNumber === worker.currentAttempt) ?? worker.attempts.at(-1)
      : worker.attempts.find((candidate) => candidate.completionId === id);
    if (!attempt) throw new Error(`Completion ${id} has no matching attempt`);
    let result = null;
    if (attempt.resultPath) result = await readJson(resolve(state.repositoryRoot, attempt.resultPath), { maxBytes: MAX_RESULT_BYTES });
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

  async #claimOwnership(sessionId, sessionFile) {
    const currentPid = process.pid;
    const currentStart = this.processStartIdentity;
    await this.store.mutate(async (state) => {
      if (resolve(state.repositoryRoot) !== resolve(this.context.cwd)) throw new Error("Worker session cwd conflicts with the attached repository");
      if (state.ownerSessionId !== sessionId) throw new Error(`Worker session belongs to ${state.ownerSessionId}, not ${sessionId}`);
      if (state.owner && (state.owner.pid !== currentPid || state.owner.processStartIdentity !== currentStart)) {
        const ownerStatus = await processIdentityStatus(state.owner.pid, state.owner.processStartIdentity);
        if (ownerStatus === "live") throw new Error(`Worker session already has a live owner process ${state.owner.pid}`);
        if (ownerStatus !== "dead") throw new Error(`Worker session owner identity is ${ownerStatus}; refusing attachment`);
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
    const sameManager = state.owner.pid === process.pid && state.owner.processStartIdentity === this.processStartIdentity;
    if (!sameManager) {
      const status = await processIdentityStatus(state.owner.pid, state.owner.processStartIdentity);
      if (status === "live") return null;
      if (status !== "dead") throw new Error(`Direct-fork transfer refused: source owner identity is ${status}`);
    }
    await store.mutate((draft) => {
      if (draft.ownerSessionId !== parentSessionId) throw new Error("Direct-fork source changed during transfer");
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
    && mailbox.ownerSessionId === attempt.launchSessionId;
}

function normalizeRepairAttempts(value) {
  const number = value === undefined ? 2 : Number(value);
  if (!Number.isInteger(number) || number < 0 || number > 2) throw new Error("reportRepairAttempts must be an integer from 0 through 2");
  return number;
}

function assertCwdWithin(repositoryRoot, cwd) {
  const rel = relative(resolve(repositoryRoot), resolve(cwd));
  if (rel.startsWith("..") || isAbsolute(rel)) throw new Error("Worker cwd must stay within the repository root");
}

function summarizeWorker(worker) {
  return { id: worker.id, label: worker.label, status: worker.status, currentAttempt: worker.currentAttempt, completionId: worker.completionId ?? null, createdAt: worker.createdAt, updatedAt: worker.updatedAt };
}

function truncateUtf8(value, maxBytes) {
  const buffer = Buffer.from(String(value ?? ""));
  if (buffer.length <= maxBytes) return buffer.toString();
  return `${buffer.subarray(0, Math.max(0, maxBytes - 32)).toString()}\n…[truncated]`;
}
