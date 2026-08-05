import { createHash, randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, link, mkdir, open, readFile, readdir, readlink, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import { StringDecoder } from "node:string_decoder";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";

export const WORKER_SESSION_SCHEMA_VERSION = 1;
export const WORKER_CONFIG_SCHEMA_VERSION = 1;
export const WORKER_MAILBOX_SCHEMA_VERSION = 1;
export const WORKER_RESULT_SCHEMA_VERSION = 1;
export const MAX_DIAGNOSTIC_BYTES = 50 * 1024 * 1024;
export const MAX_REPORT_BYTES = 64 * 1024;
export const MAX_FALLBACK_TEXT_BYTES = 8 * 1024;
export const MAX_COMPLETION_MESSAGE_BYTES = 16 * 1024;
export const MAX_STATE_BYTES = 4 * 1024 * 1024;
export const MAX_MAILBOX_BYTES = 256 * 1024;
export const MAX_RESULT_BYTES = 256 * 1024;

export function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    const result = Object.create(null);
    for (const key of Object.keys(value).sort()) if (value[key] !== undefined) result[key] = canonicalize(value[key]);
    return result;
  }
  return value;
}

export function canonicalStringify(value) { return JSON.stringify(canonicalize(value)); }
export function sha256(value) { return `sha256:${createHash("sha256").update(typeof value === "string" ? value : canonicalStringify(value)).digest("hex")}`; }
export function nowIso() { return new Date().toISOString(); }
export function newNonce(bytes = 24) { return randomBytes(bytes).toString("hex"); }

export function normalizeRuntimeId(value, label = "identifier") {
  const normalized = String(value ?? "").trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 128);
  if (!normalized || normalized === "." || normalized === ".." || ["__proto__", "prototype", "constructor"].includes(normalized)) throw new Error(`${label} is invalid`);
  return normalized;
}

export function workerSessionsRoot(repositoryRoot) { return join(resolve(repositoryRoot), ".ai", "worker-sessions"); }
export function workerStorageRoot(repositoryRoot, storageId) { return join(workerSessionsRoot(repositoryRoot), normalizeRuntimeId(storageId, "storageId")); }
export function workerSessionPath(repositoryRoot, storageId) { return join(workerStorageRoot(repositoryRoot, storageId), "worker-session.json"); }
export function workerArchivePath(repositoryRoot, storageId, workerId) { return join(workerStorageRoot(repositoryRoot, storageId), "archive", "workers", `${normalizeRuntimeId(workerId, "workerId")}.json`); }
export function attemptRoot(repositoryRoot, storageId, workerId, attemptNumber) {
  if (!Number.isInteger(attemptNumber) || attemptNumber < 1) throw new Error("attemptNumber must be a positive integer");
  return join(workerStorageRoot(repositoryRoot, storageId), "attempts", normalizeRuntimeId(workerId, "workerId"), String(attemptNumber));
}
export function attemptPaths(repositoryRoot, storageId, workerId, attemptNumber) {
  const root = attemptRoot(repositoryRoot, storageId, workerId, attemptNumber);
  return {
    root,
    config: join(root, "config.json"),
    mailbox: join(root, "mailbox.json"),
    result: join(root, "result.json"),
    recoveryResult: join(root, "recovery-result.json"),
    launchReceipt: join(root, "launch-receipt.json"),
    diagnostics: join(root, "diagnostics.jsonl"),
    cancel: join(root, "cancel.json"),
    quarantine: join(root, "quarantine"),
    processFacts: join(root, "process-facts"),
  };
}

export function assertWithin(root, path, label = "path") {
  const base = resolve(root);
  const target = resolve(path);
  const rel = relative(base, target);
  if (!rel || rel === "." || rel.startsWith("..") || resolve(base, rel) !== target) throw new Error(`${label} must stay below ${base}`);
  return target;
}

export async function atomicWriteJson(path, value, options = {}) {
  const text = `${JSON.stringify(value, null, 2)}\n`;
  const maxBytes = options.maxBytes ?? MAX_STATE_BYTES;
  if (Buffer.byteLength(text) > maxBytes) throw new Error(`JSON artifact exceeds ${maxBytes} bytes: ${path}`);
  await ensureDirectory(dirname(path));
  const temporary = `${path}.${process.pid}.${Date.now()}.${newNonce(6)}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(text, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
  await syncDirectory(dirname(path));
}

export async function writeImmutableJson(path, value, options = {}) {
  const text = `${JSON.stringify(value, null, 2)}\n`;
  const maxBytes = options.maxBytes ?? MAX_RESULT_BYTES;
  if (Buffer.byteLength(text) > maxBytes) throw new Error(`Immutable JSON artifact exceeds ${maxBytes} bytes: ${path}`);
  await ensureDirectory(dirname(path));
  try {
    const existing = await readFile(path, "utf8");
    if (existing !== text) throw new Error(`Immutable artifact already exists with different content: ${path}`);
    return false;
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const temporary = `${path}.${process.pid}.${Date.now()}.${newNonce(6)}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(text, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await link(temporary, path);
    await syncDirectory(dirname(path));
    return true;
  } catch (error) {
    if (error?.code !== "EEXIST") throw error;
    const existing = await readFile(path, "utf8");
    if (existing !== text) throw new Error(`Immutable artifact race produced different content: ${path}`);
    return false;
  } finally {
    await unlink(temporary).catch(() => {});
  }
}

export async function writeImmutableBytes(path, bytes, options = {}) {
  const buffer = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
  const maxBytes = options.maxBytes ?? MAX_RESULT_BYTES;
  if (buffer.length > maxBytes) throw new Error(`Immutable artifact exceeds ${maxBytes} bytes: ${path}`);
  await ensureDirectory(dirname(path));
  try {
    const existing = await readFile(path);
    if (!existing.equals(buffer)) throw new Error(`Immutable artifact already exists with different bytes: ${path}`);
    return false;
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const temporary = `${path}.${process.pid}.${Date.now()}.${newNonce(6)}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try { await handle.writeFile(buffer); await handle.sync(); } finally { await handle.close(); }
  try {
    await link(temporary, path);
    await syncDirectory(dirname(path));
    return true;
  } catch (error) {
    if (error?.code !== "EEXIST") throw error;
    const existing = await readFile(path);
    if (!existing.equals(buffer)) throw new Error(`Immutable artifact race produced different bytes: ${path}`);
    return false;
  } finally { await unlink(temporary).catch(() => {}); }
}

export async function readJson(path, options = {}) {
  const maxBytes = options.maxBytes ?? MAX_STATE_BYTES;
  const info = await stat(path);
  if (info.size > maxBytes) throw new Error(`JSON artifact exceeds ${maxBytes} bytes: ${path}`);
  return JSON.parse(await readFile(path, "utf8"));
}

export async function pathExists(path) {
  try { await access(path, fsConstants.F_OK); return true; } catch { return false; }
}

export async function listDirectories(path) {
  try { return (await readdir(path, { withFileTypes: true })).filter((entry) => entry.isDirectory()).map(({ name }) => name).sort(); }
  catch (error) { if (error?.code === "ENOENT") return []; throw error; }
}

export async function readFileHead(path, maxBytes = 16 * 1024) {
  const info = await stat(path);
  const length = Math.min(info.size, maxBytes);
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, 0);
    return buffer.toString("utf8");
  } finally { await handle.close(); }
}

export async function readFileTail(path, maxBytes = 64 * 1024) {
  const info = await stat(path);
  const length = Math.min(info.size, maxBytes);
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, Math.max(0, info.size - length));
    return buffer.toString("utf8");
  } finally { await handle.close(); }
}

export async function processStartIdentity(pid = process.pid) {
  if (!Number.isInteger(pid) || pid < 1) return null;
  try {
    const text = await readFile(`/proc/${pid}/stat`, "utf8");
    const close = text.lastIndexOf(")");
    if (close < 0) return null;
    const fields = text.slice(close + 2).trim().split(/\s+/);
    const startTicks = fields[19];
    return startTicks ? `linux-proc:${startTicks}` : null;
  } catch {
    return null;
  }
}

export async function processIdentityStatus(pid, expectedStartIdentity) {
  if (!Number.isInteger(pid) || pid < 1 || typeof expectedStartIdentity !== "string" || !expectedStartIdentity) return "ambiguous";
  try { process.kill(pid, 0); } catch (error) { return error?.code === "ESRCH" ? "dead" : "ambiguous"; }
  const actual = await processStartIdentity(pid);
  if (!actual) return "ambiguous";
  return actual === expectedStartIdentity ? "live" : "mismatch";
}

export async function processesUsingWorkingRoot(workingRoot, excludedPids = []) {
  const root = resolve(workingRoot);
  const excluded = new Set(excludedPids.filter((pid) => Number.isInteger(pid)));
  const users = [];
  let entries;
  try { entries = await readdir("/proc", { withFileTypes: true }); }
  catch { return { status: "ambiguous", users: [] }; }
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    const pid = Number(entry.name);
    if (excluded.has(pid)) continue;
    try {
      const processInfo = await stat(`/proc/${pid}`);
      if (typeof process.getuid === "function" && processInfo.uid !== process.getuid()) continue;
      const cwd = (await readlink(`/proc/${pid}/cwd`)).replace(/ \(deleted\)$/, "");
      const rel = relative(root, resolve(cwd));
      if (cwd === root || (rel && !rel.startsWith("..") && !isAbsolute(rel))) {
        const identity = await processStartIdentity(pid);
        if (!identity) return { status: "ambiguous", users };
        users.push({ pid, processStartIdentity: identity, cwd });
      }
    } catch {
      // Processes that disappear or deny inspection without matching cwd evidence are unrelated.
    }
  }
  users.sort((left, right) => left.pid - right.pid);
  return { status: "observed", users };
}

export async function processesWithEnvironmentBinding(name, value, excludedPids = []) {
  const expected = Buffer.from(`${name}=${value}`);
  const excluded = new Set(excludedPids.filter((pid) => Number.isInteger(pid)));
  const users = [];
  let entries;
  try { entries = await readdir("/proc", { withFileTypes: true }); }
  catch { return { status: "ambiguous", users: [] }; }
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    const pid = Number(entry.name);
    if (excluded.has(pid)) continue;
    try {
      const processInfo = await stat(`/proc/${pid}`);
      if (typeof process.getuid === "function" && processInfo.uid !== process.getuid()) continue;
      const environment = await readFile(`/proc/${pid}/environ`);
      if (environment.toString("utf8").split("\0").some((entryValue) => Buffer.from(entryValue).equals(expected))) {
        const identity = await processStartIdentity(pid);
        if (!identity) return { status: "ambiguous", users };
        users.push({ pid, processStartIdentity: identity });
      }
    } catch {
      // Processes that disappear or deny inspection without matching evidence are unrelated.
    }
  }
  users.sort((left, right) => left.pid - right.pid);
  return { status: "observed", users };
}

export async function uninspectableSameUidProcesses(excludedPids = []) {
  const excluded = new Set(excludedPids.filter((pid) => Number.isInteger(pid)));
  const processes = [];
  let entries;
  try { entries = await readdir("/proc", { withFileTypes: true }); }
  catch { return { status: "ambiguous", processes: [] }; }
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    const pid = Number(entry.name);
    if (excluded.has(pid)) continue;
    try {
      const processInfo = await stat(`/proc/${pid}`);
      if (typeof process.getuid === "function" && processInfo.uid !== process.getuid()) continue;
      let denied = false;
      try { await readlink(`/proc/${pid}/cwd`); } catch (error) { if (["EACCES", "EPERM"].includes(error?.code)) denied = true; else if (error?.code !== "ENOENT") throw error; }
      try { await readFile(`/proc/${pid}/environ`); } catch (error) { if (["EACCES", "EPERM"].includes(error?.code)) denied = true; else if (error?.code !== "ENOENT") throw error; }
      if (denied) {
        const processStart = await processStartIdentity(pid);
        if (!processStart) continue;
        processes.push({ pid, processStartIdentity: processStart });
      }
    } catch (error) {
      if (error?.code !== "ENOENT") return { status: "ambiguous", processes };
    }
  }
  processes.sort((left, right) => left.pid - right.pid);
  return { status: "observed", processes };
}

export function processGroupStatus(groupLeaderPid) {
  if (!Number.isInteger(groupLeaderPid) || groupLeaderPid < 1) return "ambiguous";
  try { process.kill(-groupLeaderPid, 0); return "present"; }
  catch (error) {
    if (error?.code === "ESRCH") return "absent";
    if (error?.code === "EPERM") return "present";
    return "ambiguous";
  }
}

export class StrictJsonlParser {
  constructor(onRecord, onError) {
    this.onRecord = onRecord;
    this.onError = onError;
    this.decoder = new StringDecoder("utf8");
    this.buffer = "";
  }
  push(chunk) {
    this.buffer += typeof chunk === "string" ? chunk : this.decoder.write(chunk);
    this.#drain(false);
  }
  end() {
    this.buffer += this.decoder.end();
    this.#drain(true);
  }
  #drain(final) {
    while (true) {
      const index = this.buffer.indexOf("\n");
      if (index < 0) break;
      let line = this.buffer.slice(0, index);
      this.buffer = this.buffer.slice(index + 1);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      this.#parse(line);
    }
    if (final && this.buffer) {
      let line = this.buffer;
      this.buffer = "";
      if (line.endsWith("\r")) line = line.slice(0, -1);
      this.#parse(line);
    }
  }
  #parse(line) {
    if (!line.trim()) return;
    try { this.onRecord(JSON.parse(line)); }
    catch (error) { this.onError?.(error, line); }
  }
}

export class BoundedDiagnosticLog {
  constructor(path, maxBytes = MAX_DIAGNOSTIC_BYTES) {
    this.path = path;
    this.maxBytes = maxBytes;
    this.bytes = 0;
    this.truncated = false;
    this.queue = Promise.resolve();
  }
  append(record) {
    this.queue = this.queue.then(async () => {
      if (this.truncated) return;
      const line = `${JSON.stringify(record)}\n`;
      const bytes = Buffer.byteLength(line);
      if (this.bytes + bytes > this.maxBytes) {
        const marker = `${JSON.stringify({ type: "diagnostic_log_truncated", atBytes: this.bytes, capBytes: this.maxBytes, timestamp: nowIso() })}\n`;
        if (this.bytes + Buffer.byteLength(marker) <= this.maxBytes) {
          await mkdir(dirname(this.path), { recursive: true });
          await writeFile(this.path, marker, { encoding: "utf8", mode: 0o600, flag: "a" });
          this.bytes += Buffer.byteLength(marker);
        }
        this.truncated = true;
        return;
      }
      await mkdir(dirname(this.path), { recursive: true });
      await writeFile(this.path, line, { encoding: "utf8", mode: 0o600, flag: "a" });
      this.bytes += bytes;
    });
    return this.queue;
  }
  async flush() { await this.queue; }
}

export function createWorkerSession({ sessionId, storageId = sessionId, repositoryRoot, sessionFile = null, owner }) {
  const now = nowIso();
  const state = {
    schemaVersion: WORKER_SESSION_SCHEMA_VERSION,
    storageId: normalizeRuntimeId(storageId, "storageId"),
    ownerSessionId: String(sessionId),
    repositoryRoot: resolve(repositoryRoot),
    sessionFile,
    revision: 0,
    createdAt: now,
    updatedAt: now,
    owner,
    lineage: [],
    workers: {},
    launchRecords: [],
    retryAuthorizations: [],
    approvedDisposableRoots: [],
    quarantinedArtifacts: [],
    retentionPolicy: { terminalResults: "preserve", quarantine: "preserve", maxAcknowledgedCompletionIds: 2000, maxRetainedTerminalWorkers: 50 },
    completionQueue: [],
    inFlightCompletionId: null,
    completedCompletionIds: [],
  };
  validateWorkerSession(state);
  return state;
}

export function validateWorkerSession(state) {
  const errors = [];
  if (!state || typeof state !== "object") return ["worker session must be an object"];
  if (state.schemaVersion !== WORKER_SESSION_SCHEMA_VERSION) errors.push(`schemaVersion must be ${WORKER_SESSION_SCHEMA_VERSION}`);
  for (const field of ["storageId", "ownerSessionId", "repositoryRoot", "createdAt", "updatedAt"]) if (typeof state[field] !== "string" || !state[field]) errors.push(`${field} is required`);
  if (!Number.isInteger(state.revision) || state.revision < 0) errors.push("revision must be a non-negative integer");
  if (!state.owner || !Number.isInteger(state.owner.pid) || typeof state.owner.processStartIdentity !== "string") errors.push("owner process identity is required");
  if (!state.workers || typeof state.workers !== "object" || Array.isArray(state.workers)) errors.push("workers must be an object");
  else {
    if (Object.keys(state.workers).some((key) => ["__proto__", "prototype", "constructor"].includes(key))) errors.push("workers contains a dangerous key");
    for (const [workerId, worker] of Object.entries(state.workers)) {
      if (!worker || typeof worker !== "object" || worker.id !== workerId) { errors.push(`worker identity is invalid: ${workerId}`); continue; }
      const hasLaunchBinding = worker.launchKey !== undefined || worker.requestHash !== undefined || worker.normalizedRequest !== undefined;
      if (hasLaunchBinding && (typeof worker.launchKey !== "string" || !worker.launchKey || typeof worker.requestHash !== "string" || !worker.normalizedRequest || sha256(worker.normalizedRequest) !== worker.requestHash)) errors.push(`worker launch binding is invalid: ${workerId}`);
      if (worker.attempts !== undefined) {
        if (!Array.isArray(worker.attempts)) errors.push(`worker attempts must be an array: ${workerId}`);
        else {
          const attempts = new Set();
          for (const attempt of worker.attempts) {
            if (!Number.isInteger(attempt?.attemptNumber) || attempt.attemptNumber < 1 || attempts.has(attempt.attemptNumber) || typeof attempt.attemptNonce !== "string" || typeof attempt.configHash !== "string") errors.push(`worker attempt identity is invalid: ${workerId}`);
            else attempts.add(attempt.attemptNumber);
            if (attempt?.config) {
              try { assertAttemptConfig(attempt.config); }
              catch { errors.push(`worker attempt config is invalid: ${workerId}/${attempt.attemptNumber}`); }
              if (attempt.config.storageId !== state.storageId || attempt.config.workerId !== workerId || attempt.config.attemptNumber !== attempt.attemptNumber || attempt.config.attemptNonce !== attempt.attemptNonce || attempt.config.configHash !== attempt.configHash) errors.push(`worker attempt config binding is invalid: ${workerId}/${attempt.attemptNumber}`);
            }
            if (attempt?.retrySafe === true && (attempt.processDisposition !== "dead" || typeof attempt.processDispositionFactPath !== "string" || typeof attempt.processDispositionFactHash !== "string")) errors.push(`retry-safe attempt lacks process proof binding: ${workerId}/${attempt.attemptNumber}`);
          }
          if (worker.currentAttempt !== undefined && (!Number.isInteger(worker.currentAttempt) || worker.currentAttempt < 0 || (worker.currentAttempt > 0 && !attempts.has(worker.currentAttempt)))) errors.push(`worker currentAttempt is invalid: ${workerId}`);
        }
      }
    }
  }
  if (state.launchRecords !== undefined && !Array.isArray(state.launchRecords)) errors.push("launchRecords must be an array");
  if (Array.isArray(state.launchRecords)) {
    const keys = new Set();
    for (const record of state.launchRecords) {
      if (!record || typeof record.launchKey !== "string" || !record.launchKey || typeof record.requestHash !== "string" || !record.requestHash.startsWith("sha256:") || typeof record.workerId !== "string" || (!state.workers[record.workerId] && typeof record.archivedWorkerPath !== "string")) errors.push("launch record identity is invalid");
      else if (keys.has(record.launchKey)) errors.push(`launchKey is duplicated: ${record.launchKey}`);
      else keys.add(record.launchKey);
    }
  }
  if (state.retryAuthorizations !== undefined && !Array.isArray(state.retryAuthorizations)) errors.push("retryAuthorizations must be an array");
  if (Array.isArray(state.retryAuthorizations)) {
    const openSlots = new Set();
    for (const authorization of state.retryAuthorizations) {
      const worker = state.workers[authorization?.workerId];
      const attempt = worker?.attempts?.find((candidate) => candidate.attemptNumber === authorization?.attemptNumber);
      if (!attempt || authorization.attemptNonce !== attempt.attemptNonce || typeof authorization.tokenHash !== "string" || !authorization.tokenHash.startsWith("sha256:") || authorization.authorizedBySessionId !== authorization.authorizedByOwner?.sessionId) errors.push("retry authorization binding is invalid");
      if (!authorization?.consumedAt) {
        const slot = `${authorization?.workerId}:${authorization?.attemptNumber}`;
        if (openSlots.has(slot)) errors.push(`retry authorization slot is duplicated: ${slot}`);
        openSlots.add(slot);
      }
    }
  }
  if (state.approvedDisposableRoots !== undefined && !Array.isArray(state.approvedDisposableRoots)) errors.push("approvedDisposableRoots must be an array");
  if (Array.isArray(state.approvedDisposableRoots) && state.approvedDisposableRoots.some((approval) => typeof approval?.approvalId !== "string" || typeof approval?.tokenHash !== "string" || typeof approval?.realPath !== "string" || approval.ownerSessionId !== approval.approvedByOwner?.sessionId)) errors.push("disposable working-root approval binding is invalid");
  if (state.quarantinedArtifacts !== undefined && !Array.isArray(state.quarantinedArtifacts)) errors.push("quarantinedArtifacts must be an array");
  if (!Array.isArray(state.completionQueue) || state.completionQueue.some((id) => typeof id !== "string")) errors.push("completionQueue must be a string array");
  if (state.inFlightCompletionId !== null && typeof state.inFlightCompletionId !== "string") errors.push("inFlightCompletionId must be null or a string");
  if (!Array.isArray(state.completedCompletionIds) || state.completedCompletionIds.some((id) => typeof id !== "string")) errors.push("completedCompletionIds must be a string array");
  if (!Array.isArray(state.lineage)) errors.push("lineage must be an array");
  return errors;
}

export function assertWorkerSession(state) {
  const errors = validateWorkerSession(state);
  if (errors.length) throw new Error(`Invalid worker session:\n- ${errors.join("\n- ")}`);
}

export function validateAttemptConfig(config) {
  const errors = [];
  if (!config || typeof config !== "object") return ["attempt config must be an object"];
  if (config.schemaVersion !== WORKER_CONFIG_SCHEMA_VERSION) errors.push(`schemaVersion must be ${WORKER_CONFIG_SCHEMA_VERSION}`);
  for (const field of ["storageId", "ownerSessionId", "workerId", "attemptNonce", "repositoryRoot", "cwd", "task", "configHash", "piCliPath"]) if (typeof config[field] !== "string" || !config[field]) errors.push(`${field} is required`);
  if (!Number.isInteger(config.attemptNumber) || config.attemptNumber < 1) errors.push("attemptNumber must be positive");
  if (!Array.isArray(config.activeTools) || config.activeTools.some((name) => typeof name !== "string")) errors.push("activeTools must be a string array");
  if (config.uninspectableProcessBaseline !== undefined && (!Array.isArray(config.uninspectableProcessBaseline) || config.uninspectableProcessBaseline.some((processEntry) => !Number.isInteger(processEntry?.pid) || typeof processEntry?.processStartIdentity !== "string"))) errors.push("uninspectableProcessBaseline must contain exact process identities");
  const hasLaunchBinding = config.launchKey !== undefined || config.requestHash !== undefined || config.launchOwner !== undefined;
  if (hasLaunchBinding) {
    if (typeof config.launchKey !== "string" || !config.launchKey) errors.push("launchKey is required when launch identity is present");
    if (typeof config.requestHash !== "string" || !config.requestHash.startsWith("sha256:")) errors.push("requestHash is required when launch identity is present");
    if (!config.launchOwner || config.launchOwner.sessionId !== config.ownerSessionId || !Number.isInteger(config.launchOwner.pid) || typeof config.launchOwner.processStartIdentity !== "string") errors.push("launchOwner must bind the owner session and process identity");
  }
  if (!Number.isInteger(config.reportRepairAttempts) || config.reportRepairAttempts < 0 || config.reportRepairAttempts > 2) errors.push("reportRepairAttempts must be between 0 and 2");
  return errors;
}

export function assertAttemptConfig(config) {
  const errors = validateAttemptConfig(config);
  if (errors.length) throw new Error(`Invalid attempt config:\n- ${errors.join("\n- ")}`);
  const { configHash: _hash, ...payload } = config;
  if (sha256(payload) !== config.configHash) throw new Error("Attempt config hash mismatch");
}

export function withConfigHash(config) {
  const payload = { ...config };
  delete payload.configHash;
  return { ...payload, configHash: sha256(payload) };
}

export function validateTerminalResult(result, options = {}) {
  const errors = [];
  if (!result || typeof result !== "object") return ["terminal result must be an object"];
  if (result.schemaVersion !== WORKER_RESULT_SCHEMA_VERSION) errors.push(`schemaVersion must be ${WORKER_RESULT_SCHEMA_VERSION}`);
  for (const field of ["completionId", "storageId", "ownerSessionId", "workerId", "attemptNonce", "configHash", "terminalStatus", "reportStatus", "startedAt", "endedAt"]) if (typeof result[field] !== "string" || !result[field]) errors.push(`${field} is required`);
  if (!isCanonicalIsoTimestamp(result.startedAt) || !isCanonicalIsoTimestamp(result.endedAt) || Date.parse(result.endedAt) < Date.parse(result.startedAt)) errors.push("result timestamps must be ordered canonical ISO timestamps");
  if (!Number.isInteger(result.attemptNumber) || result.attemptNumber < 1) errors.push("attemptNumber must be positive");
  if (!new Set(["succeeded", "needs_attention", "failed", "cancelled", "lost"]).has(result.terminalStatus)) errors.push("terminalStatus is invalid");
  if (!new Set(["valid", "repaired", "missing"]).has(result.reportStatus)) errors.push("reportStatus is invalid");
  if (["valid", "repaired"].includes(result.reportStatus) && (!result.report || !new Set(["completed", "needs_attention"]).has(result.report.outcome) || typeof result.report.summary !== "string" || !result.report.summary)) errors.push("valid/repaired reportStatus requires a valid report");
  if (result.reportStatus === "missing" && result.report !== undefined) errors.push("missing reportStatus cannot include a report");
  const recovery = result.runtime?.recovery === true;
  if (options.recovery === true && !recovery) errors.push("recovery result path requires runtime.recovery");
  if (options.recovery !== true && recovery) errors.push("primary result path cannot claim recovery authority");
  if (!recovery) {
    const processEnvelope = result.process;
    if (!processEnvelope || !Number.isInteger(processEnvelope.supervisorPid) || typeof processEnvelope.supervisorStartIdentity !== "string" || !processEnvelope.supervisorStartIdentity) errors.push("primary result requires supervisor process identity");
    if (!processEnvelope || !((processEnvelope.childPid === null && processEnvelope.childStartIdentity === null) || (Number.isInteger(processEnvelope.childPid) && processEnvelope.childPid > 0 && typeof processEnvelope.childStartIdentity === "string" && processEnvelope.childStartIdentity))) errors.push("primary result child identity must be exact or explicitly absent");
    if (!processEnvelope || !((processEnvelope.exitCode === null || Number.isInteger(processEnvelope.exitCode)) && (processEnvelope.signal === null || typeof processEnvelope.signal === "string") && typeof processEnvelope.teardownForced === "boolean")) errors.push("primary result process disposition is invalid");
    if (["succeeded", "needs_attention"].includes(result.terminalStatus) && (processEnvelope?.exitCode !== 0 || processEnvelope?.signal !== null)) errors.push("successful/needs-attention primary result requires a clean child exit");
  }
  if (result.terminalStatus === "succeeded" && result.report?.outcome !== "completed") errors.push("succeeded result requires a completed report outcome");
  if (result.terminalStatus === "needs_attention" && result.reportStatus !== "missing" && result.report?.outcome !== "needs_attention") errors.push("needs_attention result requires a needs_attention report outcome or a missing report");
  if (typeof result.resultHash !== "string") errors.push("resultHash is required");
  return errors;
}

function isCanonicalIsoTimestamp(value) {
  if (typeof value !== "string") return false;
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) && new Date(milliseconds).toISOString() === value;
}

export function withResultHash(result) {
  const payload = { ...result };
  delete payload.resultHash;
  return { ...payload, resultHash: sha256(payload) };
}

export function assertTerminalResult(result, options = {}) {
  const errors = validateTerminalResult(result, options);
  if (errors.length) throw new Error(`Invalid terminal result:\n- ${errors.join("\n- ")}`);
  const { resultHash: _hash, ...payload } = result;
  if (sha256(payload) !== result.resultHash) throw new Error("Terminal result hash mismatch");
}

export class WorkerSessionLockedError extends Error {
  constructor(details) {
    super(`WORKER_SESSION_LOCKED: ${JSON.stringify(details)}`);
    this.name = "WorkerSessionLockedError";
    this.code = "WORKER_SESSION_LOCKED";
    this.details = details;
  }
}

export class WorkerSessionCasError extends Error {
  constructor(details) {
    super(`WORKER_SESSION_CAS_MISMATCH: ${JSON.stringify(details)}`);
    this.name = "WorkerSessionCasError";
    this.code = "WORKER_SESSION_CAS_MISMATCH";
    this.details = details;
  }
}

export class WorkerSessionStore {
  constructor(repositoryRoot, storageId) {
    this.repositoryRoot = resolve(repositoryRoot);
    this.storageId = normalizeRuntimeId(storageId, "storageId");
    this.root = workerStorageRoot(this.repositoryRoot, this.storageId);
    this.path = workerSessionPath(this.repositoryRoot, this.storageId);
    this.lockPath = join(this.root, ".worker-session-lock");
    this.recoveryLockPath = join(this.root, ".worker-session-lock-recovery");
    this.lockQuarantineRoot = join(this.root, "quarantine", "locks");
    this.queue = Promise.resolve();
  }
  async exists() { return pathExists(this.path); }
  async load() { return (await this.#loadSnapshot()).state; }
  async initialize(state) {
    assertWorkerSession(state);
    const operation = this.queue.then(async () => this.#withLock(async () => {
      if (await this.exists()) {
        const existing = await this.load();
        if (existing.storageId === state.storageId && resolve(existing.repositoryRoot) === resolve(state.repositoryRoot) && existing.ownerSessionId === state.ownerSessionId) return existing;
        throw new Error(`Worker session already exists with conflicting identity: ${this.path}`);
      }
      await atomicWriteJson(this.path, state, { maxBytes: MAX_STATE_BYTES });
      return state;
    }));
    this.queue = operation.then(() => undefined, () => undefined);
    return operation;
  }
  mutate(mutator, options = {}) {
    const operation = this.queue.then(async () => this.#withLock(async () => {
      const snapshot = await this.#loadSnapshot();
      if (options.expectedRevision !== undefined && snapshot.state.revision !== options.expectedRevision) {
        throw new WorkerSessionCasError({ expectedRevision: options.expectedRevision, actualRevision: snapshot.state.revision });
      }
      if (options.expectedSnapshotHash !== undefined && snapshot.hash !== options.expectedSnapshotHash) {
        throw new WorkerSessionCasError({ expectedSnapshotHash: options.expectedSnapshotHash, actualSnapshotHash: snapshot.hash });
      }
      const state = structuredClone(snapshot.state);
      const result = await mutator(state, { revision: snapshot.state.revision, snapshotHash: snapshot.hash });
      state.revision += 1;
      state.updatedAt = nowIso();
      assertWorkerSession(state);
      const current = await this.#loadSnapshot();
      if (current.state.revision !== snapshot.state.revision || current.hash !== snapshot.hash) {
        throw new WorkerSessionCasError({ expectedRevision: snapshot.state.revision, actualRevision: current.state.revision, expectedSnapshotHash: snapshot.hash, actualSnapshotHash: current.hash });
      }
      await atomicWriteJson(this.path, state, { maxBytes: MAX_STATE_BYTES });
      return { state, result, previousRevision: snapshot.state.revision, previousSnapshotHash: snapshot.hash };
    }));
    this.queue = operation.then(() => undefined, () => undefined);
    return operation;
  }
  async #loadSnapshot() {
    for (let pass = 0; pass < 5; pass++) {
      let infoBefore;
      let bytes;
      let infoAfter;
      try {
        infoBefore = await stat(this.path);
        if (infoBefore.size > MAX_STATE_BYTES) throw new Error(`JSON artifact exceeds ${MAX_STATE_BYTES} bytes: ${this.path}`);
        bytes = await readFile(this.path);
        infoAfter = await stat(this.path);
      } catch (error) {
        if (pass < 4 && error?.code === "ENOENT") continue;
        throw error;
      }
      if (infoBefore.dev !== infoAfter.dev || infoBefore.ino !== infoAfter.ino || infoBefore.size !== infoAfter.size || infoBefore.mtimeMs !== infoAfter.mtimeMs) {
        if (pass < 4) continue;
        throw new Error("Worker session changed during stable read");
      }
      const state = JSON.parse(bytes.toString("utf8"));
      assertWorkerSession(state);
      if (resolve(state.repositoryRoot) !== this.repositoryRoot || state.storageId !== this.storageId) throw new Error("Worker session repository/storage identity mismatch");
      return { state, hash: sha256(bytes.toString("utf8")) };
    }
    throw new Error("Worker session could not be read stably");
  }
  async #withLock(operation) {
    const lock = await acquireProcessLock({ root: this.root, lockPath: this.lockPath, recoveryLockPath: this.recoveryLockPath, quarantineRoot: this.lockQuarantineRoot, storageId: this.storageId });
    try { return await operation(); }
    finally { await releaseProcessLock(lock); }
  }
}

async function acquireProcessLock(context) {
  await ensureDirectory(context.root);
  for (let pass = 0; pass < 3; pass++) {
    if (await pathExists(context.recoveryLockPath)) await recoverDeadRecoveryLock(context);
    const claim = await createLockClaim(context.root, context.storageId, "writer");
    try {
      await rename(claim.path, context.lockPath);
      await syncDirectory(context.root);
      return { ...context, metadata: claim.metadata };
    } catch (error) {
      await rm(claim.path, { recursive: true, force: true });
      if (!["EEXIST", "ENOTEMPTY", "EPERM"].includes(error?.code)) throw error;
    }
    await recoverDeadProcessLock(context);
  }
  throw new WorkerSessionLockedError({ reason: "could not acquire process-shared lock", storageId: context.storageId });
}

async function recoverDeadRecoveryLock(context) {
  let observed;
  try { observed = await readLockMetadata(context.recoveryLockPath); }
  catch (error) { if (error?.code === "ENOENT") return; throw error; }
  const disposition = await processIdentityStatus(observed.pid, observed.processStartIdentity);
  if (!processIdentityIsGone(disposition)) throw new WorkerSessionLockedError({ reason: "stale-lock recovery is active", disposition, pid: observed.pid, processStartIdentity: observed.processStartIdentity, storageId: context.storageId });
  const current = await readLockMetadata(context.recoveryLockPath);
  if (canonicalStringify(current) !== canonicalStringify(observed) || !processIdentityIsGone(await processIdentityStatus(current.pid, current.processStartIdentity))) throw new WorkerSessionLockedError({ reason: "recovery lock changed while proving owner death", storageId: context.storageId });
  await ensureDirectory(context.quarantineRoot);
  const retired = join(context.quarantineRoot, `dead-recovery-${normalizeRuntimeId(current.token, "lock token")}`);
  try { await rename(context.recoveryLockPath, retired); await syncDirectory(context.root); }
  catch (error) { if (error?.code !== "ENOENT") throw error; }
}

async function recoverDeadProcessLock(context) {
  let observed;
  try { observed = await readLockMetadata(context.lockPath); }
  catch (error) { if (error?.code === "ENOENT") return; throw error; }
  const disposition = await processIdentityStatus(observed.pid, observed.processStartIdentity);
  if (!processIdentityIsGone(disposition)) throw new WorkerSessionLockedError({ reason: "existing lock owner is not proven gone", disposition, pid: observed.pid, processStartIdentity: observed.processStartIdentity, storageId: context.storageId });
  const recoveryClaim = await createLockClaim(context.root, context.storageId, "recovery");
  try {
    await rename(recoveryClaim.path, context.recoveryLockPath);
    await syncDirectory(context.root);
  } catch (error) {
    await rm(recoveryClaim.path, { recursive: true, force: true });
    if (["EEXIST", "ENOTEMPTY", "EPERM"].includes(error?.code)) throw new WorkerSessionLockedError({ reason: "another process is recovering the stale lock", storageId: context.storageId });
    throw error;
  }
  const recovery = { ...context, lockPath: context.recoveryLockPath, metadata: recoveryClaim.metadata };
  try {
    const current = await readLockMetadata(context.lockPath);
    if (canonicalStringify(current) !== canonicalStringify(observed)) throw new WorkerSessionLockedError({ reason: "stale lock changed during recovery", storageId: context.storageId });
    const confirmed = await processIdentityStatus(current.pid, current.processStartIdentity);
    if (!processIdentityIsGone(confirmed)) throw new WorkerSessionLockedError({ reason: "stale lock owner absence proof changed", disposition: confirmed, storageId: context.storageId });
    await ensureDirectory(context.quarantineRoot);
    const retired = join(context.quarantineRoot, `dead-${current.pid}-${normalizeRuntimeId(current.token, "lock token")}`);
    await rename(context.lockPath, retired);
    await syncDirectory(context.root);
  } finally {
    await releaseProcessLock(recovery);
  }
}

async function createLockClaim(root, storageId, purpose) {
  const processStart = await processStartIdentity();
  if (!processStart) throw new Error("Cannot prove executing process start identity for worker-session lock");
  const token = newNonce();
  const path = join(root, `.worker-session-${purpose}-claim-${token}`);
  const metadata = { schemaVersion: 1, storageId, purpose, token, pid: process.pid, processStartIdentity: processStart, acquiredAt: nowIso() };
  await mkdir(path, { recursive: false, mode: 0o700 });
  await writeImmutableJson(join(path, "metadata.json"), metadata, { maxBytes: 16 * 1024 });
  await syncDirectory(path);
  return { path, metadata };
}

async function readLockMetadata(lockPath) {
  const metadata = await readJson(join(lockPath, "metadata.json"), { maxBytes: 16 * 1024 });
  if (metadata?.schemaVersion !== 1 || !Number.isInteger(metadata.pid) || typeof metadata.processStartIdentity !== "string" || typeof metadata.token !== "string") throw new WorkerSessionLockedError({ reason: "lock metadata is corrupt or incomplete", lockPath });
  return metadata;
}

async function releaseProcessLock(lock) {
  const current = await readLockMetadata(lock.lockPath);
  if (canonicalStringify(current) !== canonicalStringify(lock.metadata) || current.pid !== process.pid || current.processStartIdentity !== await processStartIdentity()) throw new WorkerSessionLockedError({ reason: "lock release identity mismatch", storageId: lock.storageId });
  const retired = `${lock.lockPath}.released-${lock.metadata.token}`;
  await rename(lock.lockPath, retired);
  await syncDirectory(lock.root);
  await rm(retired, { recursive: true, force: true });
}

function processIdentityIsGone(disposition) { return disposition === "dead" || disposition === "mismatch"; }

async function ensureDirectory(path) {
  try { await mkdir(path); await syncDirectory(dirname(path)); }
  catch (error) {
    if (error?.code === "ENOENT") { await ensureDirectory(dirname(path)); return ensureDirectory(path); }
    if (error?.code !== "EEXIST") throw error;
  }
}

async function syncDirectory(path) {
  const handle = await open(path, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}
