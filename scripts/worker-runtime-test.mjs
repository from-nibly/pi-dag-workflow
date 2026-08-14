import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
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
  pathExists,
  processIdentityStatus,
  processStartIdentity,
  readJson,
  sha256,
  withConfigHash,
  withResultHash,
  workerStorageRoot,
  writeImmutableJson,
} from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { ASYNC_COMPLETION_GUIDANCE, registerWorkerRuntime } from "../extensions/dag-workflow/worker-runtime/integration.ts";
import { WorkerManager } from "../extensions/dag-workflow/worker-runtime/manager.mjs";

const root = await mkdtemp(join(tmpdir(), "pi-worker-core-"));
try {
  const registeredPi = createRegistrationPi();
  registerWorkerRuntime(registeredPi);
  for (const toolName of ["subagent", "subagent_approve_disposable_root", "subagent_retire_disposable_root", "subagent_results", "subagent_result_by_launch_key"]) assert(registeredPi.tools.has(toolName), `integration exposes ${toolName} to real tool consumers`);
  assert(registeredPi.tools.get("subagent").description.includes("Do not poll status or sleep while waiting"), "subagent tool description discourages polling before launch");
  assert(ASYNC_COMPLETION_GUIDANCE.includes("delivered automatically") && ASYNC_COMPLETION_GUIDANCE.includes("do not poll subagent_status or sleep"), "launch output guidance tells the parent to await automatic completion without polling or sleeping");

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

  await Promise.all(Array.from({ length: 8 }, (_, index) => runStoreChild("mutate", root, state.storageId, String(index))));
  const processShared = await store.load();
  assert(processShared.revision === 28 && Array.from({ length: 8 }, (_, index) => processShared.workers[`child-${index}`]?.id).every(Boolean), "process-shared worker-session locking prevents cross-process lost updates");
  const crashedLock = await runStoreChild("crash-lock", root, state.storageId, "crash", 47);
  assert(crashedLock === 47, "child exits while holding the durable worker-session lock");
  await store.mutate((draft) => { draft.workers["after-dead-lock"] = { id: "after-dead-lock" }; });
  assert((await store.load()).workers["after-dead-lock"], "proven-dead process lock is quarantined and mutation resumes");
  await runStoreChild("orphan-recovery-lock", root, state.storageId, "recovery", 48);
  await store.mutate((draft) => { draft.workers["after-dead-recovery-lock"] = { id: "after-dead-recovery-lock" }; });
  assert((await store.load()).workers["after-dead-recovery-lock"], "proven-dead recovery lock is quarantined without wedging future mutation");
  for (const lockName of [".worker-session-lock", ".worker-session-lock-recovery"]) {
    const mismatchLock = join(workerStorageRoot(root, state.storageId), lockName);
    await mkdir(mismatchLock);
    await writeImmutableJson(join(mismatchLock, "metadata.json"), { schemaVersion: 1, storageId: state.storageId, purpose: lockName.endsWith("recovery") ? "recovery" : "writer", token: newNonce(), pid: process.pid, processStartIdentity: `${owner.processStartIdentity}-reused`, acquiredAt: new Date().toISOString() });
    await store.mutate((draft) => { draft.workers[`after-mismatch-${lockName}`] = { id: `after-mismatch-${lockName}` }; });
  }
  assert((await store.load()).workers["after-mismatch-.worker-session-lock-recovery"], "PID reuse mismatch proves stale writer and recovery lock owners are gone");

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
    report: { outcome: "completed", summary: "Synthetic result" },
    startedAt: new Date().toISOString(),
    endedAt: new Date().toISOString(),
    process: { supervisorPid: process.pid, supervisorStartIdentity: owner.processStartIdentity, childPid: process.pid, childStartIdentity: owner.processStartIdentity, exitCode: 0, signal: null, teardownForced: false },
  });
  assertTerminalResult(result);
  let tamperedResultRejected = false;
  try { assertTerminalResult({ ...result, terminalStatus: "failed" }); } catch { tamperedResultRejected = true; }
  assert(tamperedResultRejected, "terminal result hash detects mutation");
  let invalidTimestampRejected = false;
  try { assertTerminalResult(withResultHash({ ...result, endedAt: "not-a-time" })); } catch { invalidTimestampRejected = true; }
  assert(invalidTimestampRejected, "terminal result rejects non-canonical timestamps that could bypass cancellation ordering");
  const { process: _omittedProcess, ...processlessPayload } = result;
  let processlessResultRejected = false;
  try { assertTerminalResult(withResultHash(processlessPayload)); } catch { processlessResultRejected = true; }
  assert(processlessResultRejected, "primary terminal result requires an exact process envelope");
  let conflictingOutcomeRejected = false;
  try { assertTerminalResult(withResultHash({ ...result, report: { outcome: "needs_attention", summary: "Not completed" } })); } catch { conflictingOutcomeRejected = true; }
  assert(conflictingOutcomeRejected, "succeeded terminal result requires a completed report outcome");
  let completedNeedsAttentionRejected = false;
  try { assertTerminalResult(withResultHash({ ...result, terminalStatus: "needs_attention", report: { outcome: "completed", summary: "Conflicting attention" } })); } catch { completedNeedsAttentionRejected = true; }
  assert(completedNeedsAttentionRejected, "needs_attention terminal result cannot carry a completed report outcome");
  let primaryRecoveryMarkerRejected = false;
  try { assertTerminalResult(withResultHash({ ...processlessPayload, runtime: { recovery: true } })); } catch { primaryRecoveryMarkerRejected = true; }
  assert(primaryRecoveryMarkerRejected, "payload-controlled recovery marker cannot bypass primary result process validation");

  const identity = await processStartIdentity();
  assert(identity && await processIdentityStatus(process.pid, identity) === "live", "process identity proves the current process");
  assert(await processIdentityStatus(process.pid, `${identity}-wrong`) === "mismatch", "process identity detects PID reuse/mismatch");

  const mismatchOwnerRoot = join(root, "mismatch-owner");
  await mkdir(mismatchOwnerRoot, { recursive: true });
  const mismatchOwnerFile = join(mismatchOwnerRoot, "session.jsonl");
  await writeFile(mismatchOwnerFile, `${JSON.stringify({ type: "session", version: 3, id: "mismatch-owner", timestamp: new Date().toISOString(), cwd: mismatchOwnerRoot })}\n`);
  const mismatchOwnerState = createWorkerSession({ sessionId: "mismatch-owner", repositoryRoot: mismatchOwnerRoot, owner: { pid: process.pid, processStartIdentity: `${identity}-reused`, attachedAt: new Date().toISOString() } });
  await new WorkerSessionStore(mismatchOwnerRoot, mismatchOwnerState.storageId).initialize(mismatchOwnerState);
  const mismatchOwnerManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000 });
  await mismatchOwnerManager.attach(managerContext(mismatchOwnerRoot, "mismatch-owner", mismatchOwnerFile));
  assert((await mismatchOwnerManager.summary()).attached, "PID reuse mismatch permits exact stale session-owner takeover");
  await mismatchOwnerManager.detach();

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
        report: { outcome: "completed", summary: "Fast synthetic result" },
        startedAt: fastConfig.createdAt,
        endedAt: new Date().toISOString(),
        process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false },
      }));
      return { pid: process.pid, unref() {} };
    },
  });
  await fastManager.attach(managerContext(root, "fast-parent", fastSessionFile));
  const fastLaunch = await fastManager.launch({ task: "Publish a result before the manager's next scan." });
  await fastManager.scan();
  const fastStatus = await fastManager.status(fastLaunch.workerId);
  const fastState = await fastManager.store.load();
  const fastAttempt = fastState.workers[fastLaunch.workerId].attempts[0];
  assert(fastLaunch.status === "running" && fastStatus.status === "succeeded", "result ingestion preserves the spawn-bound supervisor identity");
  assert(fastAttempt.ingestedAt && fastAttempt.supervisorPid === process.pid && fastAttempt.supervisorStartIdentity === identity, "late supervisor identity facts attach without regressing terminal state");
  const fastAttemptPaths = attemptPaths(root, fastState.storageId, fastLaunch.workerId, 1);
  const lateRecoveryResult = withResultHash({ schemaVersion: 1, completionId: "completion-late-recovery", storageId: fastState.storageId, ownerSessionId: fastAttempt.launchSessionId, workerId: fastLaunch.workerId, attemptNumber: 1, attemptNonce: fastAttempt.attemptNonce, configHash: fastAttempt.configHash, terminalStatus: "lost", reportStatus: "missing", startedAt: fastAttempt.createdAt, endedAt: new Date().toISOString(), runtime: { recovery: true } });
  await writeImmutableJson(fastAttemptPaths.recoveryResult, lateRecoveryResult);
  await fastManager.scan();
  assert((await fastManager.store.load()).quarantinedArtifacts.some((artifact) => artifact.workerId === fastLaunch.workerId && artifact.kind === "late-recovery-result"), "recovery result published after primary adoption is quarantined durably");
  await writeImmutableJson(fastAttemptPaths.recoveryResult, lateRecoveryResult);
  await fastManager.scan();
  assert(!(await pathExists(fastAttemptPaths.recoveryResult)), "identical late recovery replay is removed from canonical result authority again");
  await fastManager.onAgentSettled();
  await fastManager.detach();

  const cancellingSessionFile = join(root, "cancelling-session.jsonl");
  await writeFile(cancellingSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "cancelling-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  const cancellingPi = createFakeParentPi();
  let releaseDelayedSpawn;
  let delayedConfigReady;
  const delayedSpawnGate = new Promise((resolveGate) => { releaseDelayedSpawn = resolveGate; });
  const delayedConfig = new Promise((resolveConfig) => { delayedConfigReady = resolveConfig; });
  const cancellingManager = new WorkerManager(cancellingPi, {
    piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"),
    watchIntervalMs: 1000,
    cancelEscalationMs: 60_000,
    spawnSupervisor: async (_supervisorPath, configPath) => {
      const attemptConfig = await readJson(configPath);
      const attemptMailbox = attemptPaths(root, attemptConfig.storageId, attemptConfig.workerId, attemptConfig.attemptNumber).mailbox;
      await atomicWriteJson(attemptMailbox, {
        schemaVersion: 1,
        storageId: attemptConfig.storageId,
        ownerSessionId: attemptConfig.ownerSessionId,
        workerId: attemptConfig.workerId,
        attemptNumber: attemptConfig.attemptNumber,
        attemptNonce: attemptConfig.attemptNonce,
        configHash: attemptConfig.configHash,
        supervisorPid: process.pid,
        supervisorStartIdentity: identity,
        heartbeatAt: new Date().toISOString(),
        status: "running",
      });
      delayedConfigReady(attemptConfig);
      await delayedSpawnGate;
      return { pid: process.pid, unref() {} };
    },
  });
  await cancellingManager.attach(managerContext(root, "cancelling-parent", cancellingSessionFile));
  const delayedLaunchPromise = cancellingManager.launch({ task: "Cancel while supervisor identity binding is delayed." });
  const cancellingConfig = await delayedConfig;
  const cancelDuringLaunch = await cancellingManager.cancel(cancellingConfig.workerId, "race fixture");
  assert(cancelDuringLaunch.status === "cancelling", "cancellation is recorded while launch identity binding is delayed");
  releaseDelayedSpawn();
  const delayedLaunch = await delayedLaunchPromise;
  const cancellingStatus = await cancellingManager.status(cancellingConfig.workerId);
  assert(delayedLaunch.status === "cancelling" && cancellingStatus.status === "cancelling", "late supervisor identity binding preserves concurrent cancellation state");
  await cancellingManager.detach();

  const valid = await runSupervisor(root, "valid", 2);
  assert(valid.terminalStatus === "succeeded" && valid.reportStatus === "valid", "supervisor captures a valid terminating report");
  const repaired = await runSupervisor(root, "repair", 2);
  assert(repaired.terminalStatus === "succeeded" && repaired.reportStatus === "repaired" && repaired.runtime.repairsUsed === 2, "supervisor performs the configured report-only repairs");
  const missing = await runSupervisor(root, "missing", 2);
  assert(missing.terminalStatus === "needs_attention" && missing.reportStatus === "missing" && missing.fallbackFinalText === "Fallback 3", "supervisor finalizes bounded fallback after repair exhaustion");
  const failed = await runSupervisor(root, "fail", 2);
  assert(failed.terminalStatus === "failed" && failed.reportStatus === "valid", "observed process failure overrides a captured completed claim");
  const forced = await runSupervisor(root, "forced-after-report", 2);
  assert(forced.terminalStatus === "failed" && forced.process.teardownForced && (forced.process.signal === "SIGTERM" || forced.process.exitCode !== 0), `forced teardown signal overrides a completed report claim: ${JSON.stringify(forced.process)}`);
  const cancelled = await runSupervisor(root, "hang", 2, true);
  assert(cancelled.terminalStatus === "cancelled", "supervisor handles a nonce-bound cancellation mailbox");
  const validDiagnostics = await readFile(resolve(root, valid.diagnostics.path), "utf8");
  assert(!validDiagnostics.includes('"type":"message_update"'), "supervisor drops cumulative message_update diagnostics");

  const managerRoot = join(root, "manager-root");
  await mkdir(managerRoot, { recursive: true });
  const sourceSessionFile = join(managerRoot, "source-session.jsonl");
  const descendantSessionFile = join(managerRoot, "descendant-session.jsonl");
  await writeFile(sourceSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "manager-source", timestamp: new Date().toISOString(), cwd: managerRoot })}\n`);
  await writeFile(descendantSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "manager-descendant", timestamp: new Date().toISOString(), cwd: managerRoot, parentSession: sourceSessionFile })}\n`);
  const parentPi = createFakeParentPi();
  const manager = new WorkerManager(parentPi, { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 20, launchGraceMs: 500, observeUninspectableProcesses: async () => ({ status: "observed", processes: [] }) });
  process.env.FAKE_WORKER_RPC_MODE = "valid";
  await manager.attach(managerContext(managerRoot, "manager-source", sourceSessionFile));
  const unsafeDisposableRoot = join(root, "src");
  await mkdir(unsafeDisposableRoot, { recursive: true });
  let unsafeDisposableRejected = false;
  try { await manager.approveDisposableWorkingRoot(unsafeDisposableRoot); } catch { unsafeDisposableRejected = true; }
  assert(unsafeDisposableRejected, "ordinary repository directories cannot be mislabeled as disposable working roots");
  const disposableRoot = join(managerRoot, ".ai", "worker-roots", "run-1-f1");
  await mkdir(disposableRoot, { recursive: true });
  const disposableApproval = await manager.approveDisposableWorkingRoot(disposableRoot);
  let wrongDisposableApprovalRejected = false;
  try { await manager.launch({ task: "Do not launch", cwd: disposableRoot, disposableRootToken: `${disposableApproval.disposableRootToken}-wrong`, launchKey: "wrong-disposable" }); } catch { wrongDisposableApprovalRejected = true; }
  assert(wrongDisposableApprovalRejected, "disposable working root requires the exact owner-issued path and inode approval");
  let dangerousWorkerIdRejected = false;
  try { await manager.launch({ task: "Do not launch", workerId: "__proto__", launchKey: "dangerous-worker-id" }); } catch { dangerousWorkerIdRejected = true; }
  assert(dangerousWorkerIdRejected, "worker identifiers cannot address dangerous object keys");
  const launchRequest = { task: "Return a fake report.", label: "valid manager worker", cwd: disposableRoot, disposableRootToken: disposableApproval.disposableRootToken, launchKey: "dag:run-1:stage-f1:generation-1" };
  const launched = await manager.launch(launchRequest);
  assert(launched.asynchronous && launched.status === "running", "manager launch returns immediately with running attempt identity");
  const replayedLaunch = await manager.launch(launchRequest);
  assert(replayedLaunch.workerId === launched.workerId && replayedLaunch.attemptNumber === 1 && replayedLaunch.idempotentReplay, "exact launch-key replay returns the reserved attempt without another spawn");
  let conflictingLaunchRejected = false;
  try { await manager.launch({ ...launchRequest, task: "Conflicting request" }); } catch (error) { conflictingLaunchRejected = error.message.includes("Launch key conflict"); }
  assert(conflictingLaunchRejected, "same launch key with a different normalized request fails closed");
  await waitFor(async () => { await manager.scan(); return parentPi.messages.length === 1; });
  const firstMessage = parentPi.messages[0];
  assert(firstMessage.message.content.includes("Fake worker completed.") && firstMessage.options.deliverAs === "followUp" && firstMessage.options.triggerTurn, "manager delivers one compact triggered follow-up");
  await manager.attach(managerContext(managerRoot, "manager-source", sourceSessionFile));
  await waitFor(async () => parentPi.messages.length === 2);
  assert(parentPi.messages[1].message.details.completionId === firstMessage.message.details.completionId, "reload redelivers an unacknowledged completion with the same stable ID");
  await manager.onAgentSettled();
  assert((await manager.summary()).inFlightCompletionId === null, "first settle automatically acknowledges the in-flight completion");
  const retiredApproval = await manager.retireDisposableWorkingRoot(disposableApproval.disposableRootToken);
  assert(retiredApproval.approvalId === disposableApproval.approvalId, "owner can retire an approved disposable root only after its worker becomes terminal");
  await waitFor(async () => { await manager.scan(); return (await manager.status(launched.workerId)).retrySafe; }, 15_000);

  process.env.FAKE_WORKER_RPC_MODE = "hang";
  const live = await manager.launch({ task: "Wait for cancellation.", label: "transfer worker" });
  await manager.attach(managerContext(managerRoot, "manager-descendant", descendantSessionFile, sourceSessionFile));
  const transferredSummary = await manager.summary();
  assert(transferredSummary.storageId === "manager-source" && transferredSummary.ownerSessionId === "manager-descendant", "direct fork durably transfers the complete stable worker session");
  await manager.cancel(live.workerId, "test direct-fork cancellation");
  await waitFor(async () => { await manager.scan(); return (await manager.status(live.workerId)).status === "cancelled"; });
  const cancelledInspection = await manager.inspect(live.workerId);
  const cancelledCompletionId = cancelledInspection.result.completionId;
  assert(cancelledInspection.result.attemptNumber === 1 && cancelledInspection.result.terminalStatus === "cancelled", "cancelled attempt has an immutable terminal result");
  await waitFor(async () => parentPi.messages.length >= 3);
  await manager.onAgentSettled();

  await waitFor(async () => { await manager.scan(); return (await manager.status(live.workerId)).retrySafe; }, 15_000);
  const retrySafeState = await manager.store.load();
  const retrySafeAttempt = retrySafeState.workers[live.workerId].attempts.find((attempt) => attempt.attemptNumber === retrySafeState.workers[live.workerId].currentAttempt);
  const retrySafeFactPath = resolve(retrySafeState.repositoryRoot, retrySafeAttempt.processDispositionFactPath);
  const retrySafeFact = await readJson(retrySafeFactPath);
  await rm(retrySafeFactPath);
  let missingRetryProofRejected = false;
  try { await manager.authorizeRetry(live.workerId); } catch { missingRetryProofRejected = true; }
  assert(missingRetryProofRejected, "retry authorization requires immutable process-death proof liveness");
  await writeImmutableJson(retrySafeFactPath, retrySafeFact);
  const retryAuthorization = await manager.authorizeRetry(live.workerId);
  let unboundRetryRejected = false;
  try { await manager.retry(live.workerId, undefined, `${retryAuthorization.retryToken}-wrong`); } catch { unboundRetryRejected = true; }
  assert(unboundRetryRejected, "retry rejects a token not bound to the exact terminal attempt and owner");
  process.env.FAKE_WORKER_RPC_MODE = "valid";
  const [retried, replayedRetry] = await Promise.all([
    manager.retry(live.workerId, undefined, retryAuthorization.retryToken),
    manager.retry(live.workerId, undefined, retryAuthorization.retryToken),
  ]);
  assert(retried.attemptNumber === 2 && replayedRetry.attemptNumber === 2, "concurrent retry-token replay creates exactly one attempt generation");
  assert(retried.idempotentReplay || replayedRetry.idempotentReplay, "concurrent retry loser returns the exact launched attempt idempotently");
  await waitFor(async () => { await manager.scan(); return (await manager.status(live.workerId)).status === "succeeded"; });
  await waitFor(async () => parentPi.messages.length >= 4);
  const inspection = await manager.inspect(live.workerId);
  assert(inspection.result.attemptNumber === 2 && inspection.result.terminalStatus === "succeeded", "inspection returns the immutable current-attempt result");
  const durableResults = await manager.listResults({ launchKey: live.launchKey });
  assert(durableResults.length === 2 && durableResults.every((entry) => entry.workerId === live.workerId), "durable result enumeration is independent of completion acknowledgement");
  const byLaunchKey = await manager.resultByLaunchKey(live.launchKey);
  assert(byLaunchKey.result.resultHash === inspection.result.resultHash, "latest immutable result is readable by opaque launch key");
  const historicalInspection = await manager.inspect(cancelledCompletionId);
  assert(historicalInspection.result.completionId === cancelledCompletionId && historicalInspection.result.attemptNumber === 1 && historicalInspection.result.terminalStatus === "cancelled", "completion inspection remains bound to its historical attempt after retry");
  await manager.onAgentSettled();
  await manager.detach();
  delete process.env.FAKE_WORKER_RPC_MODE;

  const mailboxSessionFile = join(root, "mailbox-session.jsonl");
  await writeFile(mailboxSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "mailbox-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  let releaseMailboxSpawn;
  let mailboxConfigReady;
  const mailboxSpawnGate = new Promise((resolveGate) => { releaseMailboxSpawn = resolveGate; });
  const mailboxConfigPromise = new Promise((resolveConfig) => { mailboxConfigReady = resolveConfig; });
  const mailboxManager = new WorkerManager(createFakeParentPi(), { watchIntervalMs: 1000, spawnSupervisor: async (_supervisorPath, configPath) => { mailboxConfigReady(await readJson(configPath)); await mailboxSpawnGate; return { pid: process.pid, unref() {} }; }, piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs") });
  await mailboxManager.attach(managerContext(root, "mailbox-parent", mailboxSessionFile));
  const mailboxLaunchPromise = mailboxManager.launch({ task: "Reject conflicting mailbox identity.", launchKey: "conflicting-mailbox" });
  const mailboxConfig = await mailboxConfigPromise;
  const mailboxState = await mailboxManager.store.load();
  const mailboxAttempt = mailboxState.workers[mailboxConfig.workerId].attempts[0];
  const unrelated = spawn(process.execPath, ["-e", "setTimeout(()=>{},10000)"], { stdio: "ignore" });
  const unrelatedIdentity = await waitForIdentityForTest(unrelated.pid);
  const mailboxPaths = attemptPaths(root, mailboxState.storageId, mailboxConfig.workerId, 1);
  await atomicWriteJson(mailboxPaths.mailbox, { schemaVersion: 1, storageId: mailboxState.storageId, ownerSessionId: mailboxAttempt.launchSessionId, workerId: mailboxConfig.workerId, attemptNumber: 1, attemptNonce: mailboxAttempt.attemptNonce, configHash: mailboxAttempt.configHash, supervisorPid: unrelated.pid, supervisorStartIdentity: unrelatedIdentity, childPid: unrelated.pid, childStartIdentity: unrelatedIdentity, heartbeatAt: new Date().toISOString(), status: "running" });
  await mailboxManager.scan();
  releaseMailboxSpawn();
  const mailboxLaunch = await mailboxLaunchPromise;
  await mailboxManager.scan();
  const mailboxAfter = await mailboxManager.store.load();
  assert(mailboxAfter.workers[mailboxLaunch.workerId].attempts[0].supervisorPid === process.pid && mailboxAfter.workers[mailboxLaunch.workerId].attempts[0].supervisorPid !== unrelated.pid, "pre-binding mailbox cannot replace spawn-bound supervisor identity");
  assert(mailboxAfter.quarantinedArtifacts.some((artifact) => artifact.kind === "conflicting-or-corrupt-mailbox"), "conflicting mailbox bytes are quarantined");
  assert(unrelated.exitCode === null, "conflicting mailbox never redirects cancellation or recovery signals to an unrelated process");
  unrelated.kill("SIGTERM");
  await mailboxManager.detach();

  const forgedResultSessionFile = join(root, "forged-result-session.jsonl");
  await writeFile(forgedResultSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "forged-result-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  let releaseForgedResultSpawn;
  let forgedResultConfigReady;
  const forgedResultSpawnGate = new Promise((resolveGate) => { releaseForgedResultSpawn = resolveGate; });
  const forgedResultConfigPromise = new Promise((resolveConfig) => { forgedResultConfigReady = resolveConfig; });
  const forgedResultManager = new WorkerManager(createFakeParentPi(), { watchIntervalMs: 1000, launchGraceMs: 10, spawnSupervisor: async (_supervisorPath, configPath) => { forgedResultConfigReady(await readJson(configPath)); await forgedResultSpawnGate; return { pid: process.pid, unref() {} }; }, piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs") });
  await forgedResultManager.attach(managerContext(root, "forged-result-parent", forgedResultSessionFile));
  const forgedResultLaunchPromise = forgedResultManager.launch({ task: "Reject pre-binding primary process authority.", launchKey: "forged-primary-process" });
  const forgedResultConfig = await forgedResultConfigPromise;
  const forgedResultState = await forgedResultManager.store.load();
  const forgedResultAttempt = forgedResultState.workers[forgedResultConfig.workerId].attempts[0];
  const forgedProcess = spawn(process.execPath, ["-e", "setTimeout(()=>{},10000)"], { detached: true, stdio: "ignore" });
  forgedProcess.unref();
  const forgedProcessIdentity = await waitForIdentityForTest(forgedProcess.pid);
  const forgedResultPaths = attemptPaths(root, forgedResultState.storageId, forgedResultConfig.workerId, 1);
  await writeImmutableJson(forgedResultPaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-forged-process", storageId: forgedResultState.storageId, ownerSessionId: forgedResultAttempt.launchSessionId, workerId: forgedResultConfig.workerId, attemptNumber: 1, attemptNonce: forgedResultAttempt.attemptNonce, configHash: forgedResultAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Forged process" }, startedAt: forgedResultAttempt.createdAt, endedAt: new Date().toISOString(), process: { supervisorPid: forgedProcess.pid, supervisorStartIdentity: forgedProcessIdentity, childPid: forgedProcess.pid, childStartIdentity: forgedProcessIdentity, exitCode: 0, signal: null, teardownForced: false } }));
  await new Promise((resolveWait) => setTimeout(resolveWait, 30));
  const originalForgedResultLoad = forgedResultManager.store.load.bind(forgedResultManager.store);
  let staleDispatchSnapshotCaptured;
  let releaseStaleDispatchSnapshot;
  let interceptStaleDispatchSnapshot = true;
  const staleDispatchSnapshotReady = new Promise((resolveReady) => { staleDispatchSnapshotCaptured = resolveReady; });
  const staleDispatchSnapshotGate = new Promise((resolveGate) => { releaseStaleDispatchSnapshot = resolveGate; });
  forgedResultManager.store.load = async () => {
    const snapshot = await originalForgedResultLoad();
    if (interceptStaleDispatchSnapshot) { interceptStaleDispatchSnapshot = false; staleDispatchSnapshotCaptured(); await staleDispatchSnapshotGate; }
    return snapshot;
  };
  const staleDispatchScan = forgedResultManager.scan();
  await staleDispatchSnapshotReady;
  releaseForgedResultSpawn();
  const forgedResultLaunch = await forgedResultLaunchPromise;
  releaseStaleDispatchSnapshot();
  await staleDispatchScan;
  const staleReceiptBoundState = await forgedResultManager.store.load();
  const staleReceiptBoundAttempt = staleReceiptBoundState.workers[forgedResultConfig.workerId].attempts[0];
  assert(staleReceiptBoundAttempt.supervisorPid === process.pid && staleReceiptBoundAttempt.supervisorStartIdentity === identity, "stale unbound scan hydrates the canonical launch receipt before any missing-launch recovery");
  if (staleReceiptBoundState.workers[forgedResultConfig.workerId].status === "lost") assert(staleReceiptBoundState.quarantinedArtifacts.some((artifact) => artifact.kind === "conflicting-primary-result"), "same-pass terminal reconciliation may fail closed only for the forged primary process envelope, never as an unbound launch");
  await forgedResultManager.scan();
  const forgedResultAfter = await forgedResultManager.store.load();
  assert(forgedResultAfter.workers[forgedResultLaunch.workerId].attempts[0].supervisorPid === process.pid && forgedResultAfter.workers[forgedResultLaunch.workerId].attempts[0].supervisorPid !== forgedProcess.pid, "primary result cannot bootstrap supervisor authority before actual spawn binding");
  assert(forgedResultAfter.quarantinedArtifacts.some((artifact) => artifact.kind === "conflicting-primary-result"), "pre-binding primary process envelope is quarantined");
  assert(forgedProcess.exitCode === null, "forged primary process identity is never signalled");
  try { process.kill(-forgedProcess.pid, "SIGTERM"); } catch {}
  await forgedResultManager.detach();

  const staleDispatchRoot = join(root, "stale-dispatch-root");
  await mkdir(staleDispatchRoot, { recursive: true });
  const staleDispatchSessionFile = join(staleDispatchRoot, "session.jsonl");
  await writeFile(staleDispatchSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "stale-dispatch-parent", timestamp: new Date().toISOString(), cwd: staleDispatchRoot })}\n`);
  let releaseStaleSpawn;
  let staleSpawnConfigReady;
  const staleSpawnGate = new Promise((resolveGate) => { releaseStaleSpawn = resolveGate; });
  const staleSpawnConfig = new Promise((resolveReady) => { staleSpawnConfigReady = resolveReady; });
  const staleDispatchManager = new WorkerManager(createFakeParentPi(), { watchIntervalMs: 60_000, launchGraceMs: 10, spawnSupervisor: async (_supervisorPath, configPath) => { staleSpawnConfigReady(await readJson(configPath)); await staleSpawnGate; return { pid: process.pid, unref() {} }; }, piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs") });
  await staleDispatchManager.attach(managerContext(staleDispatchRoot, "stale-dispatch-parent", staleDispatchSessionFile));
  const staleDispatchLaunchPromise = staleDispatchManager.launch({ task: "Adopt a valid result after stale dispatch scan.", launchKey: "stale-dispatch-snapshot" });
  const staleConfig = await staleSpawnConfig;
  await new Promise((resolveWait) => setTimeout(resolveWait, 30));
  const originalStaleLoad = staleDispatchManager.store.load.bind(staleDispatchManager.store);
  let staleSnapshotCaptured;
  let releaseStaleSnapshot;
  let interceptStaleSnapshot = true;
  const staleSnapshotReady = new Promise((resolveReady) => { staleSnapshotCaptured = resolveReady; });
  const staleSnapshotGate = new Promise((resolveGate) => { releaseStaleSnapshot = resolveGate; });
  staleDispatchManager.store.load = async () => {
    const snapshot = await originalStaleLoad();
    if (interceptStaleSnapshot) { interceptStaleSnapshot = false; staleSnapshotCaptured(); await staleSnapshotGate; }
    return snapshot;
  };
  const staleSnapshotScan = staleDispatchManager.scan();
  await staleSnapshotReady;
  releaseStaleSpawn();
  const staleDispatchLaunch = await staleDispatchLaunchPromise;
  releaseStaleSnapshot();
  await staleSnapshotScan;
  const staleBoundState = await staleDispatchManager.store.load();
  const staleBoundAttempt = staleBoundState.workers[staleDispatchLaunch.workerId].attempts[0];
  const staleBoundPaths = attemptPaths(staleDispatchRoot, staleBoundState.storageId, staleDispatchLaunch.workerId, 1);
  assert(!(await pathExists(staleBoundPaths.recoveryResult)) && staleBoundAttempt.supervisorPid === process.pid, "recovery publication atomically rechecks stale missing-launch observations against persisted spawn binding");
  await writeImmutableJson(staleBoundPaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-stale-dispatch-valid", storageId: staleBoundState.storageId, ownerSessionId: staleBoundAttempt.launchSessionId, workerId: staleDispatchLaunch.workerId, attemptNumber: 1, attemptNonce: staleBoundAttempt.attemptNonce, configHash: staleBoundAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Valid post-binding result" }, startedAt: staleBoundAttempt.createdAt, endedAt: new Date().toISOString(), process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false } }));
  await staleDispatchManager.scan();
  const staleAdoptedState = await staleDispatchManager.store.load();
  assert(staleAdoptedState.workers[staleDispatchLaunch.workerId].status === "succeeded" && !staleAdoptedState.quarantinedArtifacts.some((artifact) => artifact.workerId === staleDispatchLaunch.workerId), "valid primary result is adopted after stale dispatch snapshot without quarantine");
  await staleDispatchManager.detach();

  const forgedReceiptSessionFile = join(root, "forged-receipt-session.jsonl");
  await writeFile(forgedReceiptSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "forged-receipt-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  let releaseForgedReceiptSpawn;
  let forgedReceiptConfigReady;
  const forgedReceiptSpawnGate = new Promise((resolveGate) => { releaseForgedReceiptSpawn = resolveGate; });
  const forgedReceiptConfigPromise = new Promise((resolveConfig) => { forgedReceiptConfigReady = resolveConfig; });
  const forgedReceiptManager = new WorkerManager(createFakeParentPi(), { watchIntervalMs: 1000, spawnSupervisor: async (_supervisorPath, configPath) => { forgedReceiptConfigReady(await readJson(configPath)); await forgedReceiptSpawnGate; return { pid: process.pid, unref() {} }; }, piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs") });
  await forgedReceiptManager.attach(managerContext(root, "forged-receipt-parent", forgedReceiptSessionFile));
  const forgedReceiptLaunchPromise = forgedReceiptManager.launch({ task: "Reject forgeable launch receipt authority.", launchKey: "forged-launch-receipt" });
  const forgedReceiptConfig = await forgedReceiptConfigPromise;
  const forgedReceiptState = await forgedReceiptManager.store.load();
  const forgedReceiptAttempt = forgedReceiptState.workers[forgedReceiptConfig.workerId].attempts[0];
  const forgedReceiptProcess = spawn(process.execPath, ["-e", "setTimeout(()=>{},10000)"], { detached: true, stdio: "ignore" });
  forgedReceiptProcess.unref();
  const forgedReceiptProcessIdentity = await waitForIdentityForTest(forgedReceiptProcess.pid);
  const forgedReceiptPaths = attemptPaths(root, forgedReceiptState.storageId, forgedReceiptConfig.workerId, 1);
  const forgedReceiptPayload = { schemaVersion: 1, kind: "worker_supervisor_launch", storageId: forgedReceiptState.storageId, ownerSessionId: forgedReceiptAttempt.launchSessionId, workerId: forgedReceiptConfig.workerId, attemptNumber: 1, attemptNonce: forgedReceiptAttempt.attemptNonce, configHash: forgedReceiptAttempt.configHash, supervisorPid: forgedReceiptProcess.pid, supervisorStartIdentity: forgedReceiptProcessIdentity, observedAt: forgedReceiptAttempt.createdAt };
  await writeImmutableJson(forgedReceiptPaths.launchReceipt, { ...forgedReceiptPayload, receiptHash: sha256(forgedReceiptPayload) });
  await forgedReceiptManager.scan();
  releaseForgedReceiptSpawn();
  let forgedReceiptLaunchRejected = false;
  try { await forgedReceiptLaunchPromise; } catch { forgedReceiptLaunchRejected = true; }
  assert(forgedReceiptLaunchRejected, "forged pre-binding receipt conflicts with actual spawn and fails closed");
  await forgedReceiptManager.cancel(forgedReceiptConfig.workerId, "forged receipt must not redirect cancellation");
  assert((await forgedReceiptManager.store.load()).workers[forgedReceiptConfig.workerId].attempts[0].supervisorPid === undefined, "forgeable receipt never establishes persisted supervisor authority");
  assert(forgedReceiptProcess.exitCode === null, "unbound forged receipt process is never signalled");
  try { process.kill(-forgedReceiptProcess.pid, "SIGTERM"); } catch {}
  await forgedReceiptManager.detach();

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
  const quarantinedState = await corruptManager.store.load();
  const quarantinedCorrupt = quarantinedState.quarantinedArtifacts.find((artifact) => artifact.workerId === corruptLaunch.workerId && artifact.kind === "corrupt-primary-result");
  assert(quarantinedCorrupt && await readFile(resolve(root, quarantinedCorrupt.retainedPath), "utf8") === "{\"bad\":true}\n", "corrupt primary bytes are durably retained outside semantic result authority");
  const forgedRecoveryLaunch = await corruptManager.launch({ task: "Reject payload-controlled recovery authority.", launchKey: "forged-primary-recovery" });
  const forgedRecoveryState = await corruptManager.store.load();
  const forgedRecoveryAttempt = forgedRecoveryState.workers[forgedRecoveryLaunch.workerId].attempts[0];
  const forgedRecoveryPaths = attemptPaths(root, forgedRecoveryState.storageId, forgedRecoveryLaunch.workerId, 1);
  await writeImmutableJson(forgedRecoveryPaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-forged-recovery", storageId: forgedRecoveryState.storageId, ownerSessionId: forgedRecoveryAttempt.launchSessionId, workerId: forgedRecoveryLaunch.workerId, attemptNumber: 1, attemptNonce: forgedRecoveryAttempt.attemptNonce, configHash: forgedRecoveryAttempt.configHash, terminalStatus: "lost", reportStatus: "missing", startedAt: forgedRecoveryAttempt.createdAt, endedAt: new Date().toISOString(), runtime: { recovery: true } }));
  await corruptManager.scan();
  assert((await corruptManager.status(forgedRecoveryLaunch.workerId)).status === "lost" && (await corruptManager.inspect(forgedRecoveryLaunch.workerId)).result.runtime.recovery, "primary-path recovery marker is quarantined and replaced by manager-owned recovery authority without wedging scan");
  await corruptManager.onAgentSettled();
  await corruptManager.detach();

  const lateSessionFile = join(root, "late-session.jsonl");
  await writeFile(lateSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "late-parent", timestamp: new Date().toISOString(), cwd: root })}\n`);
  const lateManager = new WorkerManager(createFakeParentPi(), { watchIntervalMs: 1000, cancelEscalationMs: 60_000, spawnSupervisor: async () => ({ pid: process.pid, unref() {} }), piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs") });
  await lateManager.attach(managerContext(root, "late-parent", lateSessionFile));
  const lateLaunch = await lateManager.launch({ task: "Produce a result after cancellation.", launchKey: "late-after-cancel" });
  const lateState = await lateManager.store.load();
  const lateAttempt = lateState.workers[lateLaunch.workerId].attempts[0];
  await lateManager.cancel(lateLaunch.workerId, "late result fixture");
  await new Promise((resolveWait) => setTimeout(resolveWait, 2));
  const latePaths = attemptPaths(root, lateState.storageId, lateLaunch.workerId, 1);
  await writeImmutableJson(latePaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-late-success", storageId: lateState.storageId, ownerSessionId: lateAttempt.launchSessionId, workerId: lateLaunch.workerId, attemptNumber: 1, attemptNonce: lateAttempt.attemptNonce, configHash: lateAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Late success" }, startedAt: lateAttempt.createdAt, endedAt: new Date().toISOString(), process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false } }));
  await lateManager.scan();
  assert((await lateManager.status(lateLaunch.workerId)).status === "lost", "post-cancellation non-cancelled result is excluded from terminal authority");
  assert((await lateManager.store.load()).quarantinedArtifacts.some((artifact) => artifact.workerId === lateLaunch.workerId && artifact.kind === "late-post-cancellation-result"), "late post-cancellation result bytes are quarantined");
  const impossibleTimeLaunch = await lateManager.launch({ task: "Reject impossible pre-attempt timestamps.", launchKey: "impossible-time" });
  const impossibleTimeState = await lateManager.store.load();
  const impossibleTimeAttempt = impossibleTimeState.workers[impossibleTimeLaunch.workerId].attempts[0];
  await lateManager.cancel(impossibleTimeLaunch.workerId, "timestamp authority fixture");
  const impossibleTime = new Date(Date.parse(impossibleTimeAttempt.createdAt) - 24 * 60 * 60 * 1000).toISOString();
  const impossibleTimePaths = attemptPaths(root, impossibleTimeState.storageId, impossibleTimeLaunch.workerId, 1);
  await writeImmutableJson(impossibleTimePaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-impossible-time", storageId: impossibleTimeState.storageId, ownerSessionId: impossibleTimeAttempt.launchSessionId, workerId: impossibleTimeLaunch.workerId, attemptNumber: 1, attemptNonce: impossibleTimeAttempt.attemptNonce, configHash: impossibleTimeAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Impossible time" }, startedAt: impossibleTime, endedAt: impossibleTime, process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false } }));
  await lateManager.scan();
  assert((await lateManager.status(impossibleTimeLaunch.workerId)).status === "lost" && (await lateManager.store.load()).quarantinedArtifacts.some((artifact) => artifact.workerId === impossibleTimeLaunch.workerId && artifact.kind === "conflicting-primary-result"), "payload timestamps before attempt creation cannot bypass cancellation authority");
  const equalCancelLaunch = await lateManager.launch({ task: "Reject equal cancellation/result timestamps.", launchKey: "equal-cancel-time" });
  const equalCancelState = await lateManager.store.load();
  const equalCancelAttempt = equalCancelState.workers[equalCancelLaunch.workerId].attempts[0];
  const equalCancelPaths = attemptPaths(root, equalCancelState.storageId, equalCancelLaunch.workerId, 1);
  await lateManager.cancel(equalCancelLaunch.workerId, "equal timestamp fixture");
  const equalCancellation = await readJson(equalCancelPaths.cancel);
  await writeImmutableJson(equalCancelPaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-equal-cancel", storageId: equalCancelState.storageId, ownerSessionId: equalCancelAttempt.launchSessionId, workerId: equalCancelLaunch.workerId, attemptNumber: 1, attemptNonce: equalCancelAttempt.attemptNonce, configHash: equalCancelAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Equal cancellation" }, startedAt: equalCancelAttempt.createdAt, endedAt: equalCancellation.requestedAt, process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false } }));
  await lateManager.scan();
  assert((await lateManager.status(equalCancelLaunch.workerId)).status === "lost", "result timestamp equal to serialized cancellation intent fails closed");
  await lateManager.detach();

  const cancellationRaceRoot = join(root, "cancellation-race-root");
  await mkdir(cancellationRaceRoot, { recursive: true });
  const cancellationRaceSessionFile = join(cancellationRaceRoot, "session.jsonl");
  await writeFile(cancellationRaceSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "cancellation-race-parent", timestamp: new Date().toISOString(), cwd: cancellationRaceRoot })}\n`);
  const cancellationRaceManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 60_000, spawnSupervisor: async () => ({ pid: process.pid, unref() {} }) });
  await cancellationRaceManager.attach(managerContext(cancellationRaceRoot, "cancellation-race-parent", cancellationRaceSessionFile));
  const cancellationRaceLaunch = await cancellationRaceManager.launch({ task: "Serialize cancellation against result ingestion.", launchKey: "cancellation-ingestion-race" });
  const cancellationRaceState = await cancellationRaceManager.store.load();
  const cancellationRaceAttempt = cancellationRaceState.workers[cancellationRaceLaunch.workerId].attempts[0];
  const cancellationRacePaths = attemptPaths(cancellationRaceRoot, cancellationRaceState.storageId, cancellationRaceLaunch.workerId, 1);
  await writeImmutableJson(cancellationRacePaths.result, withResultHash({ schemaVersion: 1, completionId: "completion-cancellation-race", storageId: cancellationRaceState.storageId, ownerSessionId: cancellationRaceAttempt.launchSessionId, workerId: cancellationRaceLaunch.workerId, attemptNumber: 1, attemptNonce: cancellationRaceAttempt.attemptNonce, configHash: cancellationRaceAttempt.configHash, terminalStatus: "succeeded", reportStatus: "valid", report: { outcome: "completed", summary: "Race success" }, startedAt: cancellationRaceAttempt.createdAt, endedAt: new Date().toISOString(), process: { supervisorPid: process.pid, supervisorStartIdentity: identity, childPid: process.pid, childStartIdentity: identity, exitCode: 0, signal: null, teardownForced: false } }));
  const originalRaceMutate = cancellationRaceManager.store.mutate.bind(cancellationRaceManager.store);
  let releaseIngestionMutate;
  let ingestionMutateStarted;
  let gateIngestionMutate = true;
  const ingestionMutateGate = new Promise((resolveGate) => { releaseIngestionMutate = resolveGate; });
  const ingestionMutateStart = new Promise((resolveStarted) => { ingestionMutateStarted = resolveStarted; });
  cancellationRaceManager.store.mutate = async (...args) => {
    if (gateIngestionMutate) { gateIngestionMutate = false; ingestionMutateStarted(); await ingestionMutateGate; }
    return originalRaceMutate(...args);
  };
  const racedIngestion = cancellationRaceManager.scan();
  await ingestionMutateStart;
  await cancellationRaceManager.cancel(cancellationRaceLaunch.workerId, "serialized race fixture");
  releaseIngestionMutate();
  await racedIngestion;
  assert((await cancellationRaceManager.status(cancellationRaceLaunch.workerId)).status === "lost", "cancellation intent published before ingestion commit wins through serialized state authority");
  await cancellationRaceManager.detach();

  const rootIdentityCase = join(root, "root-identity-case");
  await mkdir(join(rootIdentityCase, ".ai", "worker-roots"), { recursive: true });
  const rootIdentitySessionFile = join(rootIdentityCase, "session.jsonl");
  await writeFile(rootIdentitySessionFile, `${JSON.stringify({ type: "session", version: 3, id: "root-identity-parent", timestamp: new Date().toISOString(), cwd: rootIdentityCase })}\n`);
  const externalRoot = await mkdtemp(join(tmpdir(), "pi-worker-external-"));
  const escapedRoot = join(rootIdentityCase, ".ai", "worker-roots", "escaped");
  await symlink(externalRoot, escapedRoot);
  const escapedParent = join(rootIdentityCase, ".ai", "external-worker-roots");
  await symlink(externalRoot, escapedParent);
  const escapedThroughParent = join(escapedParent, "child");
  await mkdir(escapedThroughParent);
  let rootToReplace;
  let replacementTarget;
  let replacedRoot = false;
  let identitySpawnCount = 0;
  const rootIdentityManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000, spawnSupervisor: async () => { identitySpawnCount += 1; return { pid: process.pid, unref() {} }; }, maxActiveDisposableRoots: 2, approvedDisposableRootParents: [join(rootIdentityCase, ".ai", "worker-roots"), escapedParent], failpoint: async (name) => { if (name === "after_attempt_reservation" && rootToReplace && !replacedRoot) { replacedRoot = true; await rm(rootToReplace, { recursive: true, force: true }); await mkdir(replacementTarget); await symlink(replacementTarget, rootToReplace); } } });
  await rootIdentityManager.attach(managerContext(rootIdentityCase, "root-identity-parent", rootIdentitySessionFile));
  let symlinkEscapeRejected = false;
  try { await rootIdentityManager.approveDisposableWorkingRoot(escapedRoot); } catch { symlinkEscapeRejected = true; }
  assert(symlinkEscapeRejected, "disposable-root approval rejects symlink escape outside the canonical repository");
  let parentSymlinkEscapeRejected = false;
  try { await rootIdentityManager.approveDisposableWorkingRoot(escapedThroughParent); } catch { parentSymlinkEscapeRejected = true; }
  assert(parentSymlinkEscapeRejected, "disposable-root approval rejects an approved-parent symlink escaping the canonical repository");
  rootToReplace = join(rootIdentityCase, ".ai", "worker-roots", "replace-before-dispatch");
  replacementTarget = join(rootIdentityCase, ".ai", "worker-roots", "replacement-target");
  await mkdir(rootToReplace);
  const replacementApproval = await rootIdentityManager.approveDisposableWorkingRoot(rootToReplace);
  const replacedLaunch = await rootIdentityManager.launch({ task: "Do not dispatch into a replacement inode.", cwd: rootToReplace, disposableRootToken: replacementApproval.disposableRootToken, launchKey: "replaced-root" });
  assert(replacedLaunch.status === "failed" && identitySpawnCount === 0, "inode replacement after reservation is detected before supervisor dispatch");
  const retirementRaceRoot = join(rootIdentityCase, ".ai", "worker-roots", "retirement-race");
  await mkdir(retirementRaceRoot);
  const retirementRaceApproval = await rootIdentityManager.approveDisposableWorkingRoot(retirementRaceRoot);
  const excessApprovalRoot = join(rootIdentityCase, ".ai", "worker-roots", "excess-approval");
  await mkdir(excessApprovalRoot);
  let excessApprovalRejected = false;
  try { await rootIdentityManager.approveDisposableWorkingRoot(excessApprovalRoot); } catch { excessApprovalRejected = true; }
  assert(excessApprovalRejected, "active disposable-root approvals are explicitly bounded before snapshot pressure");
  const retirementRace = await Promise.allSettled([
    rootIdentityManager.launch({ task: "Race root retirement against launch.", cwd: retirementRaceRoot, disposableRootToken: retirementRaceApproval.disposableRootToken, launchKey: "retirement-race" }),
    rootIdentityManager.retireDisposableWorkingRoot(retirementRaceApproval.disposableRootToken),
  ]);
  assert(retirementRace.some((outcome) => outcome.status === "rejected"), "concurrent disposable-root retirement and launch cannot both commit");
  await rootIdentityManager.detach();
  await rm(externalRoot, { recursive: true, force: true });

  const descendantRoot = join(root, "descendant-root");
  await mkdir(descendantRoot, { recursive: true });
  const detachedDescendantSessionFile = join(descendantRoot, "session.jsonl");
  await writeFile(detachedDescendantSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "descendant-parent", timestamp: new Date().toISOString(), cwd: descendantRoot })}\n`);
  const descendantManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000 });
  process.env.FAKE_WORKER_RPC_MODE = "detached-grandchild";
  await descendantManager.attach(managerContext(descendantRoot, "descendant-parent", detachedDescendantSessionFile));
  const descendantLaunch = await descendantManager.launch({ task: "Spawn a detached descendant.", launchKey: "detached-descendant" });
  await waitFor(async () => { try { return (await readFile(join(descendantRoot, "detached-grandchild-writes.txt"))).length > 0; } catch { return false; } });
  await descendantManager.scan();
  const descendantStatus = await descendantManager.status(descendantLaunch.workerId);
  assert(descendantStatus.status === "succeeded" && !descendantStatus.retrySafe, "detached descendant retaining the cwd blocks retry safety after direct-child success");
  const firstDescendantWrites = (await readFile(join(descendantRoot, "detached-grandchild-writes.txt"))).length;
  await new Promise((resolveWait) => setTimeout(resolveWait, 150));
  const secondDescendantWrites = (await readFile(join(descendantRoot, "detached-grandchild-writes.txt"))).length;
  assert(secondDescendantWrites > firstDescendantWrites, "detached descendant remains able to mutate the cwd while retry is fenced");
  await waitFor(async () => { await descendantManager.scan(); return (await descendantManager.status(descendantLaunch.workerId)).retrySafe; }, 5000);
  process.env.FAKE_WORKER_RPC_MODE = "detached-uninspectable";
  const uninspectableLaunch = await descendantManager.launch({ task: "Spawn an inspectability-denying descendant.", launchKey: "uninspectable-descendant" });
  await waitFor(async () => { try { return (await readFile(join(descendantRoot, "uninspectable-descendant-writes.txt"))).length > 0; } catch { return false; } });
  await descendantManager.scan();
  const uninspectableStatus = await descendantManager.status(uninspectableLaunch.workerId);
  assert(uninspectableStatus.status === "succeeded" && !uninspectableStatus.retrySafe, "new same-UID process denying cwd/environment inspection blocks retry safety");
  const firstUninspectableWrites = (await readFile(join(descendantRoot, "uninspectable-descendant-writes.txt"))).length;
  await waitFor(async () => (await readFile(join(descendantRoot, "uninspectable-descendant-writes.txt"))).length > firstUninspectableWrites, 2000);
  assert((await readFile(join(descendantRoot, "uninspectable-descendant-writes.txt"))).length > firstUninspectableWrites, "inspectability-denying descendant remains a live cwd writer while retry is fenced");
  await waitFor(async () => { await descendantManager.scan(); return (await descendantManager.status(uninspectableLaunch.workerId)).retrySafe; }, 10_000);
  await descendantManager.detach();
  delete process.env.FAKE_WORKER_RPC_MODE;

  const retentionRoot = join(root, "retention-root");
  await mkdir(retentionRoot, { recursive: true });
  const retentionSessionFile = join(retentionRoot, "session.jsonl");
  await writeFile(retentionSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "retention-parent", timestamp: new Date().toISOString(), cwd: retentionRoot })}\n`);
  const retentionManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000, maxRetainedTerminalWorkers: 2 });
  process.env.FAKE_WORKER_RPC_MODE = "valid";
  await retentionManager.attach(managerContext(retentionRoot, "retention-parent", retentionSessionFile));
  const retentionCompletions = [];
  for (let index = 0; index < 5; index++) {
    const launchedForRetention = await retentionManager.launch({ task: `Retention worker ${index}`, launchKey: `retention:${index}` });
    await waitFor(async () => { await retentionManager.scan(); return (await retentionManager.status(launchedForRetention.workerId)).status === "succeeded"; });
    retentionCompletions.push((await retentionManager.inspect(launchedForRetention.workerId)).result.completionId);
  }
  const retentionState = await retentionManager.store.load();
  assert(Object.keys(retentionState.workers).length <= 2 && retentionState.launchRecords.filter((record) => record.archivedWorkerPath).length >= 3, "bounded retention archives terminal workers even while completion acknowledgement is stalled");
  assert((await retentionManager.listResults()).length === 5, "archived immutable results remain durably enumerable after state compaction");
  assert((await retentionManager.inspect(retentionCompletions[0])).result.completionId === retentionCompletions[0], "archived completion remains inspectable by immutable completion identity");
  const archivedReplay = await retentionManager.launch({ task: "Retention worker 0", launchKey: "retention:0" });
  assert(archivedReplay.archived && archivedReplay.idempotentReplay && archivedReplay.attemptNumber === 1, "launch-key replay returns an archived exact attempt without relaunch");
  assert((await retentionManager.resultByLaunchKey("retention:0")).result.completionId === retentionCompletions[0], "result-by-launch-key traverses bounded worker archives");
  await retentionManager.detach();
  delete process.env.FAKE_WORKER_RPC_MODE;

  const corruptRetentionRoot = join(root, "corrupt-retention-root");
  await mkdir(corruptRetentionRoot, { recursive: true });
  const corruptRetentionSessionFile = join(corruptRetentionRoot, "session.jsonl");
  await writeFile(corruptRetentionSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "corrupt-retention-parent", timestamp: new Date().toISOString(), cwd: corruptRetentionRoot })}\n`);
  const corruptRetentionManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000, maxRetainedTerminalWorkers: 2, spawnSupervisor: async () => ({ pid: process.pid, unref() {} }) });
  await corruptRetentionManager.attach(managerContext(corruptRetentionRoot, "corrupt-retention-parent", corruptRetentionSessionFile));
  for (let index = 0; index < 4; index++) {
    const corruptTerminalLaunch = await corruptRetentionManager.launch({ task: `Corrupt recovery terminal ${index}`, launchKey: `corrupt-retention:${index}` });
    const corruptTerminalState = await corruptRetentionManager.store.load();
    const corruptTerminalPaths = attemptPaths(corruptRetentionRoot, corruptTerminalState.storageId, corruptTerminalLaunch.workerId, 1);
    await writeFile(corruptTerminalPaths.recoveryResult, "{\"corrupt\":true}\n");
    await corruptRetentionManager.scan();
  }
  const corruptRetentionState = await corruptRetentionManager.store.load();
  assert(Object.keys(corruptRetentionState.workers).length <= 2 && corruptRetentionState.launchRecords.filter((record) => record.archivedWorkerPath).length >= 2, "terminal workers without completion IDs are bounded by immutable worker archives");
  await corruptRetentionManager.detach();

  const scanQueueRoot = join(root, "scan-queue-root");
  await mkdir(scanQueueRoot, { recursive: true });
  const scanQueueSessionFile = join(scanQueueRoot, "session.jsonl");
  await writeFile(scanQueueSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "scan-queue-parent", timestamp: new Date().toISOString(), cwd: scanQueueRoot })}\n`);
  let timerScanRequests = 0;
  const scanQueueManager = new WorkerManager(createFakeParentPi(), { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 5, onTimerScanRequested: () => { timerScanRequests += 1; } });
  await scanQueueManager.attach(managerContext(scanQueueRoot, "scan-queue-parent", scanQueueSessionFile));
  const originalScanLoad = scanQueueManager.store.load.bind(scanQueueManager.store);
  let scanLoadCount = 0;
  let releaseFirstScanLoad;
  let firstScanLoadStarted;
  const firstScanLoadGate = new Promise((resolveGate) => { releaseFirstScanLoad = resolveGate; });
  const firstScanStarted = new Promise((resolveStarted) => { firstScanLoadStarted = resolveStarted; });
  scanQueueManager.store.load = async () => {
    scanLoadCount += 1;
    if (scanLoadCount === 1) { firstScanLoadStarted(); await firstScanLoadGate; }
    await new Promise((resolveWait) => setTimeout(resolveWait, 5));
    return originalScanLoad();
  };
  timerScanRequests = 0;
  const firstQueuedScan = scanQueueManager.scan();
  await firstScanStarted;
  const explicitScanBacklog = Array.from({ length: 20 }, () => scanQueueManager.scan());
  await waitFor(() => timerScanRequests >= 1 && scanQueueManager.timerScanPending, 2000);
  assert(timerScanRequests >= 1 && scanQueueManager.timerScanPending, "timer scan is retained and coalesced while explicit scan traffic is backlogged");
  releaseFirstScanLoad();
  await Promise.all([firstQueuedScan, ...explicitScanBacklog]);
  assert(scanLoadCount >= 21, "every overlapping explicit scan waits for a distinct serialized pass beginning after its request");
  await scanQueueManager.detach();

  for (const failpoint of ["after_launch_reservation", "after_attempt_reservation", "after_config_publication", "after_dispatch_claim", "after_launch_receipt_publication", "after_supervisor_spawn"]) {
    const crashRoot = join(root, `manager-crash-${failpoint}`);
    await mkdir(crashRoot, { recursive: true });
    const crashSessionFile = join(crashRoot, "session.jsonl");
    await writeFile(crashSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "manager-crash-session", timestamp: new Date().toISOString(), cwd: crashRoot })}\n`);
    await runManagerCrashChild(crashRoot, crashSessionFile, failpoint);
    const recoveredPi = createFakeParentPi();
    const recoveredManager = new WorkerManager(recoveredPi, { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 1000, launchGraceMs: 2000 });
    process.env.FAKE_WORKER_RPC_MODE = "valid";
    await recoveredManager.attach(managerContext(crashRoot, "manager-crash-session", crashSessionFile));
    await waitFor(async () => {
      await recoveredManager.scan();
      const workers = await recoveredManager.status();
      return workers.length === 1 && ["succeeded", "lost"].includes(workers[0].status);
    });
    const recoveredStatus = (await recoveredManager.status())[0];
    if (failpoint === "after_dispatch_claim") assert(recoveredStatus.status === "lost" && !recoveredStatus.retrySafe, "ambiguous dispatch crash fails closed without authorizing retry");
    else assert(recoveredStatus.status === "succeeded", `manager resumes durable launch after ${failpoint}`);
    if (failpoint === "after_launch_receipt_publication") {
      const recoveredState = await recoveredManager.store.load();
      const recoveredWorker = Object.values(recoveredState.workers)[0];
      const recoveredAttempt = recoveredWorker.attempts[0];
      const recoveredPaths = attemptPaths(crashRoot, recoveredState.storageId, recoveredWorker.id, recoveredAttempt.attemptNumber);
      const originalReceipt = await readJson(recoveredPaths.launchReceipt);
      assert(recoveredWorker.attempts.length === 1 && recoveredWorker.currentAttempt === 1, "receipt-before-state recovery never duplicates the original attempt generation");
      assert(recoveredAttempt.attemptNonce === originalReceipt.attemptNonce && recoveredAttempt.configHash === originalReceipt.configHash && recoveredAttempt.supervisorPid === originalReceipt.supervisorPid && recoveredAttempt.supervisorStartIdentity === originalReceipt.supervisorStartIdentity && recoveredAttempt.launchReceiptHash === originalReceipt.receiptHash, "restart hydrates the exact original attempt and supervisor identity from its canonical immutable receipt");
      await recoveredManager.scan();
      const replayedState = await recoveredManager.store.load();
      const replayedAttempt = replayedState.workers[recoveredWorker.id].attempts[0];
      assert(replayedState.workers[recoveredWorker.id].attempts.length === 1 && replayedAttempt.launchReceiptHash === originalReceipt.receiptHash && replayedAttempt.supervisorPid === originalReceipt.supervisorPid && replayedAttempt.supervisorStartIdentity === originalReceipt.supervisorStartIdentity, "exact launch-receipt replay preserves one unchanged attempt/process binding while unrelated terminal bookkeeping may advance");
    }
    await recoveredManager.detach();
    delete process.env.FAKE_WORKER_RPC_MODE;
  }

  console.log("Owned worker core, supervisor, and manager tests OK");
} finally {
  await rm(root, { recursive: true, force: true });
}

function createRegistrationPi() {
  return {
    tools: new Map(),
    commands: new Map(),
    handlers: new Map(),
    registerTool(tool) { this.tools.set(tool.name, tool); },
    registerCommand(name, command) { this.commands.set(name, command); },
    on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); },
  };
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

async function waitForIdentityForTest(pid) {
  for (let pass = 0; pass < 100; pass++) {
    const identity = await processStartIdentity(pid);
    if (identity) return identity;
    await new Promise((resolveWait) => setTimeout(resolveWait, 5));
  }
  throw new Error(`Could not observe process identity for ${pid}`);
}

async function runManagerCrashChild(repositoryRoot, sessionFile, failpoint) {
  const child = spawn(process.execPath, [resolve("scripts/fixtures/worker-manager-crash-child.mjs"), repositoryRoot, sessionFile, failpoint], { env: { ...process.env, FAKE_WORKER_RPC_MODE: "valid" }, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  child.stdout.on("data", (chunk) => { output += String(chunk); });
  child.stderr.on("data", (chunk) => { output += String(chunk); });
  const code = await new Promise((resolveExit, reject) => { child.once("error", reject); child.once("exit", resolveExit); });
  if (code !== 61) throw new Error(`worker-manager crash child ${failpoint} exited ${code}: ${output}`);
}

async function runStoreChild(mode, repositoryRoot, storageId, key, expectedCode = 0) {
  const child = spawn(process.execPath, [resolve("scripts/fixtures/worker-store-child.mjs"), mode, repositoryRoot, storageId, key], { stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  child.stdout.on("data", (chunk) => { output += String(chunk); });
  child.stderr.on("data", (chunk) => { output += String(chunk); });
  const code = await new Promise((resolveExit, reject) => {
    child.once("error", reject);
    child.once("exit", resolveExit);
  });
  if (code !== expectedCode) throw new Error(`worker-store child ${mode} exited ${code}: ${output}`);
  return code;
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
