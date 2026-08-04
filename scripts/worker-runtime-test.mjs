import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import {
  BoundedDiagnosticLog,
  StrictJsonlParser,
  WorkerSessionStore,
  attemptPaths,
  assertAttemptConfig,
  assertTerminalResult,
  createWorkerSession,
  newNonce,
  atomicWriteJson,
  processIdentityStatus,
  processStartIdentity,
  readJson,
  withConfigHash,
  withResultHash,
  writeImmutableJson,
} from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { WorkerManager } from "../extensions/dag-workflow/worker-runtime/manager.mjs";

const root = await mkdtemp(join(tmpdir(), "pi-worker-core-"));
try {
  const owner = { pid: process.pid, processStartIdentity: await processStartIdentity(), attachedAt: new Date().toISOString() };
  const state = createWorkerSession({ sessionId: "session-one", repositoryRoot: root, owner });
  const store = new WorkerSessionStore(root, state.storageId);
  await store.initialize(state);
  await Promise.all(Array.from({ length: 20 }, (_, index) => store.mutate(async (draft) => {
    await new Promise((resolve) => setTimeout(resolve, index % 3));
    draft.workers[`worker-${index}`] = { id: `worker-${index}` };
  })));
  const serialized = await store.load();
  assert(serialized.revision === 20 && Object.keys(serialized.workers).length === 20, "worker-session mutations serialize without lost updates");

  const paths = attemptPaths(root, state.storageId, "worker-1", 1);
  await writeImmutableJson(paths.result, { value: 1 });
  assert((await readJson(paths.result)).value === 1, "immutable JSON is readable");
  let immutableRejected = false;
  try { await writeImmutableJson(paths.result, { value: 2 }); } catch { immutableRejected = true; }
  assert(immutableRejected, "immutable JSON refuses conflicting replacement");

  const records = [];
  const errors = [];
  const parser = new StrictJsonlParser((record) => records.push(record), (error) => errors.push(error));
  parser.push(Buffer.from('{"text":"a\u2028b"}\r\n{"value":'));
  parser.push(Buffer.from("2}\n"));
  parser.end();
  assert(records.length === 2 && records[0].text.includes("\u2028") && records[1].value === 2 && errors.length === 0, "strict parser splits only on LF and accepts CRLF");

  const log = new BoundedDiagnosticLog(paths.diagnostics, 220);
  for (let index = 0; index < 20; index++) await log.append({ type: "event", index, text: "x".repeat(30) });
  await log.flush();
  const logText = await readFile(paths.diagnostics, "utf8");
  assert(Buffer.byteLength(logText) <= 220 && log.truncated, "diagnostic log stops at its byte cap");

  const config = withConfigHash({
    schemaVersion: 1,
    storageId: state.storageId,
    ownerSessionId: "session-one",
    workerId: "worker-1",
    attemptNumber: 1,
    attemptNonce: newNonce(),
    repositoryRoot: root,
    cwd: root,
    task: "Test",
    activeTools: ["read", "subagent_report"],
    reportRepairAttempts: 2,
    piCliPath: "/tmp/pi",
  });
  assertAttemptConfig(config);
  await writeImmutableJson(paths.config, config);
  process.env.PI_DAG_WORKER_CONFIG = paths.config;
  process.env.PI_DAG_WORKER_ROLE = "child";
  const { registerWorkerChild } = await import("../extensions/dag-workflow/worker-runtime/child-report.ts");
  const childPi = createFakeChildPi(["read", "bash", "subagent", "dag_status", "dag_model_context"]);
  registerWorkerChild(childPi);
  await childPi.emit("session_start");
  assert(childPi.activeTools.has("read") && childPi.activeTools.has("subagent_report") && !childPi.activeTools.has("subagent") && !childPi.activeTools.has("dag_status"), "child role intersects inherited tools and omits orchestration surfaces");
  const reportResult = await childPi.tools.get("subagent_report").execute("call", { outcome: "completed", summary: "Done", artifacts: [{ path: "artifact.txt" }] });
  assert(reportResult.terminate === true && reportResult.details.report.summary === "Done", "subagent_report returns a terminating structured result");
  delete process.env.PI_DAG_WORKER_CONFIG;
  delete process.env.PI_DAG_WORKER_ROLE;
  let tamperedConfigRejected = false;
  try { assertAttemptConfig({ ...config, task: "Changed" }); } catch { tamperedConfigRejected = true; }
  assert(tamperedConfigRejected, "attempt config hash detects mutation");

  const result = withResultHash({
    schemaVersion: 1,
    completionId: "completion-1",
    storageId: state.storageId,
    ownerSessionId: "session-one",
    workerId: "worker-1",
    attemptNumber: 1,
    attemptNonce: config.attemptNonce,
    configHash: config.configHash,
    terminalStatus: "succeeded",
    reportStatus: "valid",
    startedAt: new Date().toISOString(),
    endedAt: new Date().toISOString(),
  });
  assertTerminalResult(result);
  let tamperedResultRejected = false;
  try { assertTerminalResult({ ...result, terminalStatus: "failed" }); } catch { tamperedResultRejected = true; }
  assert(tamperedResultRejected, "terminal result hash detects mutation");

  const identity = await processStartIdentity();
  assert(identity && await processIdentityStatus(process.pid, identity) === "live", "process identity proves the current process");
  assert(await processIdentityStatus(process.pid, `${identity}-wrong`) === "mismatch", "process identity detects PID reuse/mismatch");

  const fastSessionFile = join(root, "fast-session.jsonl");
  await writeFile(fastSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "fast-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  const fastPi = createFakeParentPi();
  let fastManager;
  fastManager = new WorkerManager(fastPi, {
    piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"),
    watchIntervalMs: 1000,
    spawnSupervisor: async (_supervisorPath, configPath) => {
      const fastConfig = await readJson(configPath);
      const fastPaths = attemptPaths(root, fastConfig.storageId, fastConfig.workerId, fastConfig.attemptNumber);
      await writeImmutableJson(fastPaths.result, withResultHash({
        schemaVersion: 1,
        completionId: `completion-${fastConfig.workerId}-${fastConfig.attemptNumber}-fast`,
        storageId: fastConfig.storageId,
        ownerSessionId: fastConfig.ownerSessionId,
        workerId: fastConfig.workerId,
        attemptNumber: fastConfig.attemptNumber,
        attemptNonce: fastConfig.attemptNonce,
        configHash: fastConfig.configHash,
        terminalStatus: "succeeded",
        reportStatus: "valid",
        startedAt: fastConfig.createdAt,
        endedAt: new Date().toISOString(),
      }));
      await fastManager.scan();
      return { pid: process.pid, unref() {} };
    },
  });
  await fastManager.attach(managerContext(root, "fast-parent", fastSessionFile));
  const fastLaunch = await fastManager.launch({ task: "Finish before supervisor identity binding." });
  const fastStatus = await fastManager.status(fastLaunch.workerId);
  const fastState = await fastManager.store.load();
  const fastAttempt = fastState.workers[fastLaunch.workerId].attempts[0];
  assert(fastLaunch.status === "succeeded" && fastStatus.status === "succeeded", "late supervisor identity binding preserves an already ingested terminal status");
  assert(fastAttempt.ingestedAt && fastAttempt.supervisorPid === process.pid && fastAttempt.supervisorStartIdentity === identity, "late supervisor identity facts attach without regressing terminal state");
  await fastManager.onAgentSettled();
  await fastManager.detach();

  const valid = await runSupervisor(root, "valid", 2);
  assert(valid.terminalStatus === "succeeded" && valid.reportStatus === "valid", "supervisor captures a valid terminating report");
  const repaired = await runSupervisor(root, "repair", 2);
  assert(repaired.terminalStatus === "succeeded" && repaired.reportStatus === "repaired" && repaired.runtime.repairsUsed === 2, "supervisor performs the configured report-only repairs");
  const missing = await runSupervisor(root, "missing", 2);
  assert(missing.terminalStatus === "needs_attention" && missing.reportStatus === "missing" && missing.fallbackFinalText === "Fallback 3", "supervisor finalizes bounded fallback after repair exhaustion");
  const failed = await runSupervisor(root, "fail", 2);
  assert(failed.terminalStatus === "failed" && failed.reportStatus === "valid", "observed process failure overrides a captured completed claim");
  const cancelled = await runSupervisor(root, "hang", 2, true);
  assert(cancelled.terminalStatus === "cancelled", "supervisor handles a nonce-bound cancellation mailbox");
  const validDiagnostics = await readFile(resolve(root, valid.diagnostics.path), "utf8");
  assert(!validDiagnostics.includes('"type":"message_update"'), "supervisor drops cumulative message_update diagnostics");

  const sourceSessionFile = join(root, "source-session.jsonl");
  const descendantSessionFile = join(root, "descendant-session.jsonl");
  await writeFile(sourceSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "manager-source", timestamp: new Date().toISOString(), cwd: root })}\n`);
  await writeFile(descendantSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "manager-descendant", timestamp: new Date().toISOString(), cwd: root, parentSession: sourceSessionFile })}\n`);
  const parentPi = createFakeParentPi();
  const manager = new WorkerManager(parentPi, { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 20, launchGraceMs: 500 });
  process.env.FAKE_WORKER_RPC_MODE = "valid";
  await manager.attach(managerContext(root, "manager-source", sourceSessionFile));
  const launched = await manager.launch({ task: "Return a fake report.", label: "valid manager worker" });
  assert(launched.asynchronous && launched.status === "running", "manager launch returns immediately with running attempt identity");
  await waitFor(async () => { await manager.scan(); return parentPi.messages.length === 1; });
  const firstMessage = parentPi.messages[0];
  assert(firstMessage.message.content.includes("Fake worker completed.") && firstMessage.options.deliverAs === "followUp" && firstMessage.options.triggerTurn, "manager delivers one compact triggered follow-up");
  await manager.attach(managerContext(root, "manager-source", sourceSessionFile));
  await waitFor(async () => parentPi.messages.length === 2);
  assert(parentPi.messages[1].message.details.completionId === firstMessage.message.details.completionId, "reload redelivers an unacknowledged completion with the same stable ID");
  await manager.onAgentSettled();
  assert((await manager.summary()).inFlightCompletionId === null, "first settle automatically acknowledges the in-flight completion");

  process.env.FAKE_WORKER_RPC_MODE = "hang";
  const live = await manager.launch({ task: "Wait for cancellation.", label: "transfer worker" });
  await manager.attach(managerContext(root, "manager-descendant", descendantSessionFile, sourceSessionFile));
  const transferredSummary = await manager.summary();
  assert(transferredSummary.storageId === "manager-source" && transferredSummary.ownerSessionId === "manager-descendant", "direct fork durably transfers the complete stable worker session");
  await manager.cancel(live.workerId, "test direct-fork cancellation");
  await waitFor(async () => { await manager.scan(); return (await manager.status(live.workerId)).status === "cancelled"; });
  const cancelledInspection = await manager.inspect(live.workerId);
  const cancelledCompletionId = cancelledInspection.result.completionId;
  assert(cancelledInspection.result.attemptNumber === 1 && cancelledInspection.result.terminalStatus === "cancelled", "cancelled attempt has an immutable terminal result");
  await waitFor(async () => parentPi.messages.length >= 3);
  await manager.onAgentSettled();

  process.env.FAKE_WORKER_RPC_MODE = "valid";
  const retried = await manager.retry(live.workerId);
  assert(retried.attemptNumber === 2, "explicit retry creates a new attempt generation");
  await waitFor(async () => { await manager.scan(); return (await manager.status(live.workerId)).status === "succeeded"; });
  await waitFor(async () => parentPi.messages.length >= 4);
  const inspection = await manager.inspect(live.workerId);
  assert(inspection.result.attemptNumber === 2 && inspection.result.terminalStatus === "succeeded", "inspection returns the immutable current-attempt result");
  const historicalInspection = await manager.inspect(cancelledCompletionId);
  assert(historicalInspection.result.completionId === cancelledCompletionId && historicalInspection.result.attemptNumber === 1 && historicalInspection.result.terminalStatus === "cancelled", "completion inspection remains bound to its historical attempt after retry");
  await manager.onAgentSettled();
  await manager.detach();
  delete process.env.FAKE_WORKER_RPC_MODE;

  const corruptSessionFile = join(root, "corrupt-session.jsonl");
  await writeFile(corruptSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "corrupt-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  const corruptPi = createFakeParentPi();
  const corruptManager = new WorkerManager(corruptPi, {
    watchIntervalMs: 1000,
    launchGraceMs: 5000,
    spawnSupervisor: async () => ({ pid: process.pid, unref() {} }),
    piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"),
  });
  await corruptManager.attach(managerContext(root, "corrupt-parent", corruptSessionFile));
  const corruptLaunch = await corruptManager.launch({ task: "Simulate a corrupt detached result." });
  const corruptState = await corruptManager.store.load();
  const corruptPaths = attemptPaths(root, corruptState.storageId, corruptLaunch.workerId, 1);
  await writeFile(corruptPaths.result, "{\"bad\":true}\n");
  await corruptManager.scan();
  assert((await corruptManager.status(corruptLaunch.workerId)).status === "lost", "manager fails closed and ingests a recovery result for corrupt terminal output");
  assert((await corruptManager.inspect(corruptLaunch.workerId)).result.runtime.recovery, "corrupt terminal output preserves an inspectable recovery envelope");
  await corruptManager.onAgentSettled();
  await corruptManager.detach();

  console.log("Owned worker core, supervisor, and manager tests OK");
} finally {
  await rm(root, { recursive: true, force: true });
}

function createFakeParentPi() {
  return {
    messages: [],
    getActiveTools: () => ["read", "bash", "subagent", "dag_status"],
    sendMessage(message, options) { this.messages.push({ message, options }); },
  };
}

function managerContext(cwd, sessionId, sessionFile, parentSession = null) {
  return {
    cwd,
    model: { provider: "fake-provider", id: "fake-model" },
    thinkingLevel: "off",
    sessionManager: {
      getSessionId: () => sessionId,
      getSessionFile: () => sessionFile,
      getHeader: () => ({ type: "session", id: sessionId, cwd, ...(parentSession ? { parentSession } : {}) }),
    },
  };
}

function createFakeChildPi(ambientTools) {
  return {
    tools: new Map(),
    handlers: new Map(),
    activeTools: new Set(ambientTools),
    registerTool(tool) { this.tools.set(tool.name, tool); this.activeTools.add(tool.name); },
    on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); },
    getAllTools() { return [...ambientTools, ...this.tools.keys()].map((name) => ({ name })); },
    setActiveTools(names) { this.activeTools = new Set(names); },
    async emit(event, payload = {}) { for (const handler of this.handlers.get(event) ?? []) await handler(payload); },
  };
}

async function runSupervisor(root, mode, reportRepairAttempts, cancel = false) {
  const workerId = `supervisor-${mode}`;
  const attemptNumber = 1;
  const nonce = newNonce();
  const paths = attemptPaths(root, "session-one", workerId, attemptNumber);
  const config = withConfigHash({
    schemaVersion: 1,
    storageId: "session-one",
    ownerSessionId: "session-one",
    workerId,
    label: workerId,
    attemptNumber,
    attemptNonce: nonce,
    repositoryRoot: root,
    cwd: root,
    task: "Exercise the fake RPC worker.",
    activeTools: ["read", "subagent", "dag_status"],
    reportRepairAttempts,
    piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"),
  });
  await writeImmutableJson(paths.config, config);
  const child = spawn(process.execPath, [resolve("extensions/dag-workflow/worker-runtime/supervisor.mjs"), paths.config], {
    cwd: root,
    env: { ...process.env, FAKE_WORKER_RPC_MODE: mode },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let supervisorOutput = "";
  child.stdout.on("data", (chunk) => { supervisorOutput += String(chunk); });
  child.stderr.on("data", (chunk) => { supervisorOutput += String(chunk); });
  if (cancel) {
    await waitFor(async () => {
      if (child.exitCode !== null) throw new Error(`supervisor ${mode} exited before mailbox: ${supervisorOutput}`);
      try { await readJson(paths.mailbox); return true; } catch { return false; }
    });
    await atomicWriteJson(paths.cancel, { attemptNonce: nonce, configHash: config.configHash, reason: "test" });
  }
  await new Promise((resolveExit, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => code === 0 ? resolveExit() : reject(new Error(`supervisor ${mode} exited ${code}: ${supervisorOutput}`)));
  });
  return readJson(paths.result);
}

async function waitFor(predicate, timeoutMs = 5000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await predicate()) return;
    await new Promise((resolveWait) => setTimeout(resolveWait, 20));
  }
  throw new Error("Timed out waiting for supervisor artifact");
}

function assert(value, message) { if (!value) throw new Error(`Worker runtime test failed: ${message}`); }
