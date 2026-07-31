import { createHash, randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, link, mkdir, open, readFile, readdir, rename, stat, unlink, writeFile } from "node:fs/promises";
import { StringDecoder } from "node:string_decoder";
import { dirname, join, relative, resolve } from "node:path";

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
    const result = {};
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
  if (!normalized || normalized === "." || normalized === "..") throw new Error(`${label} is invalid`);
  return normalized;
}

export function workerSessionsRoot(repositoryRoot) { return join(resolve(repositoryRoot), ".ai", "worker-sessions"); }
export function workerStorageRoot(repositoryRoot, storageId) { return join(workerSessionsRoot(repositoryRoot), normalizeRuntimeId(storageId, "storageId")); }
export function workerSessionPath(repositoryRoot, storageId) { return join(workerStorageRoot(repositoryRoot, storageId), "worker-session.json"); }
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
    diagnostics: join(root, "diagnostics.jsonl"),
    cancel: join(root, "cancel.json"),
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
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.${newNonce(6)}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(text, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
}

export async function writeImmutableJson(path, value, options = {}) {
  const text = `${JSON.stringify(value, null, 2)}\n`;
  const maxBytes = options.maxBytes ?? MAX_RESULT_BYTES;
  if (Buffer.byteLength(text) > maxBytes) throw new Error(`Immutable JSON artifact exceeds ${maxBytes} bytes: ${path}`);
  await mkdir(dirname(path), { recursive: true });
  try {
    const existing = await readFile(path, "utf8");
    if (existing !== text) throw new Error(`Immutable artifact already exists with different content: ${path}`);
    return false;
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const temporary = `${path}.${process.pid}.${Date.now()}.${newNonce(6)}.tmp`;
  await writeFile(temporary, text, { encoding: "utf8", mode: 0o600, flag: "wx" });
  try {
    await link(temporary, path);
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

export function validateTerminalResult(result) {
  const errors = [];
  if (!result || typeof result !== "object") return ["terminal result must be an object"];
  if (result.schemaVersion !== WORKER_RESULT_SCHEMA_VERSION) errors.push(`schemaVersion must be ${WORKER_RESULT_SCHEMA_VERSION}`);
  for (const field of ["completionId", "storageId", "ownerSessionId", "workerId", "attemptNonce", "configHash", "terminalStatus", "reportStatus", "startedAt", "endedAt"]) if (typeof result[field] !== "string" || !result[field]) errors.push(`${field} is required`);
  if (!Number.isInteger(result.attemptNumber) || result.attemptNumber < 1) errors.push("attemptNumber must be positive");
  if (!new Set(["succeeded", "needs_attention", "failed", "cancelled", "lost"]).has(result.terminalStatus)) errors.push("terminalStatus is invalid");
  if (!new Set(["valid", "repaired", "missing"]).has(result.reportStatus)) errors.push("reportStatus is invalid");
  if (typeof result.resultHash !== "string") errors.push("resultHash is required");
  return errors;
}

export function withResultHash(result) {
  const payload = { ...result };
  delete payload.resultHash;
  return { ...payload, resultHash: sha256(payload) };
}

export function assertTerminalResult(result) {
  const errors = validateTerminalResult(result);
  if (errors.length) throw new Error(`Invalid terminal result:\n- ${errors.join("\n- ")}`);
  const { resultHash: _hash, ...payload } = result;
  if (sha256(payload) !== result.resultHash) throw new Error("Terminal result hash mismatch");
}

export class WorkerSessionStore {
  constructor(repositoryRoot, storageId) {
    this.repositoryRoot = resolve(repositoryRoot);
    this.storageId = normalizeRuntimeId(storageId, "storageId");
    this.path = workerSessionPath(this.repositoryRoot, this.storageId);
    this.queue = Promise.resolve();
  }
  async exists() { return pathExists(this.path); }
  async load() {
    const state = await readJson(this.path, { maxBytes: MAX_STATE_BYTES });
    assertWorkerSession(state);
    if (resolve(state.repositoryRoot) !== this.repositoryRoot || state.storageId !== this.storageId) throw new Error("Worker session repository/storage identity mismatch");
    return state;
  }
  async initialize(state) {
    assertWorkerSession(state);
    if (await this.exists()) throw new Error(`Worker session already exists: ${this.path}`);
    await atomicWriteJson(this.path, state, { maxBytes: MAX_STATE_BYTES });
    return state;
  }
  mutate(mutator) {
    const operation = this.queue.then(async () => {
      const state = await this.load();
      const result = await mutator(state);
      state.revision += 1;
      state.updatedAt = nowIso();
      assertWorkerSession(state);
      await atomicWriteJson(this.path, state, { maxBytes: MAX_STATE_BYTES });
      return { state, result };
    });
    this.queue = operation.then(() => undefined, () => undefined);
    return operation;
  }
}
