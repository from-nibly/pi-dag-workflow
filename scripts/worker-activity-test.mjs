import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { WorkerManager } from "../extensions/dag-workflow/worker-runtime/manager.mjs";
import { connectWorkerActivityBus, WORKER_ACTIVITY_EVENT, WORKER_ACTIVITY_REQUEST_EVENT } from "../extensions/dag-workflow/worker-runtime/activity.mjs";
import { attemptPaths, withResultHash, writeImmutableJson } from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { registerWorkerRuntime } from "../extensions/dag-workflow/worker-runtime/integration.ts";

const root = await mkdtemp(join(tmpdir(), "pi-worker-activity-"));
const managers = [];
const options = { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 60_000, spawnSupervisor: async () => ({ pid: process.pid }) };
function bus() {
  const emitter = new EventEmitter();
  return { emit: (name, data) => emitter.emit(name, data), on(name, listener) { emitter.on(name, listener); return () => emitter.off(name, listener); }, emitter };
}
function context(cwd = root, id = "parent", mode = "tui", parentSession) {
  return { cwd, mode, sessionManager: { getSessionId: () => id, getSessionFile: () => null, getHeader: () => ({ parentSession }) }, ui: { notify() {} } };
}
function manager(pi = {}, extra = {}) {
  const value = new WorkerManager({ getActiveTools: () => ["read"], sendMessage() {}, ...pi }, { ...options, ...extra });
  managers.push(value);
  return value;
}
async function finish(value, workerId, status = "succeeded") {
  const state = await value.store.load();
  const worker = state.workers[workerId];
  const attempt = worker.attempts.find((entry) => entry.attemptNumber === worker.currentAttempt);
  const paths = attemptPaths(root, state.storageId, workerId, attempt.attemptNumber);
  await writeImmutableJson(paths.recoveryResult, withResultHash({
    schemaVersion: 1, completionId: `completion-${workerId}-${attempt.attemptNumber}`, storageId: state.storageId,
    ownerSessionId: attempt.launchSessionId, workerId, attemptNumber: attempt.attemptNumber,
    attemptNonce: attempt.attemptNonce, configHash: attempt.configHash, terminalStatus: status,
    reportStatus: ["succeeded", "needs_attention"].includes(status) ? "valid" : "missing",
    ...(["succeeded", "needs_attention"].includes(status) ? { report: { outcome: status === "succeeded" ? "completed" : "needs_attention", summary: "fixture" } } : {}),
    startedAt: attempt.createdAt, endedAt: new Date().toISOString(), runtime: { recovery: true },
  }));
}
function active(value) { return value.activitySnapshot().stores.flatMap((store) => store.activeAttempts); }

try {
  const events = bus();
  const seen = [];
  const value = manager();
  const disconnect = connectWorkerActivityBus(events, value);
  events.on(WORKER_ACTIVITY_EVENT, (event) => seen.push(event));
  // Consumer first: its initial request can precede attachment.
  events.emit(WORKER_ACTIVITY_REQUEST_EVENT, { schemaVersion: 1, ownerSessionId: "parent", requestId: "early" });
  assert.equal(seen.length, 0);
  await value.attach(context());
  assert.equal(seen.at(-1).phase, "attached");
  assert.equal(seen.at(-1).working, false);
  assert.equal(seen.at(-1).ownerSessionId, "parent");
  const epoch = seen.at(-1).epoch;
  const starts = await Promise.all([value.launch({ workerId: "one", task: "SECRET TASK ONE" }), value.launch({ workerId: "two", task: "SECRET TASK TWO" })]);
  assert.equal(active(value).length, 2);
  assert(seen.some((event) => event.stores.some((store) => store.activeAttempts.some((attempt) => attempt.attemptNumber === 0))), "launch reservation is observable before dispatch");
  assert(!JSON.stringify(seen).includes("SECRET"), "activity never carries task text");
  for (let i = 1; i < seen.length; i++) assert(seen[i].revision > seen[i - 1].revision);

  // Producer first / consumer reload: handshake is synchronous and does no IO.
  const originalLoad = value.store.load;
  value.store.load = () => { throw new Error("handshake must not scan or load"); };
  events.emit(WORKER_ACTIVITY_REQUEST_EVENT, { schemaVersion: 1, ownerSessionId: "parent", requestId: "late" });
  assert.equal(seen.at(-1).requestId, "late");
  assert.equal(seen.at(-1).stores[0].activeAttempts.length, 2);
  const beforeIgnored = seen.length;
  events.emit(WORKER_ACTIVITY_REQUEST_EVENT, { schemaVersion: 2, ownerSessionId: "parent", requestId: "wrong-version" });
  events.emit(WORKER_ACTIVITY_REQUEST_EVENT, { schemaVersion: 1, ownerSessionId: "other", requestId: "wrong-owner" });
  assert.equal(seen.length, beforeIgnored);
  value.store.load = originalLoad;

  await value.cancel(starts[0].workerId);
  assert.equal(active(value).find((attempt) => attempt.workerId === "one").status, "cancelling");
  await finish(value, "one", "cancelled");
  await value.scan();
  assert.equal(active(value).length, 1);
  assert(value.activitySnapshot().working);
  await finish(value, "two");
  const terminalStart = seen.length;
  await value.scan();
  assert.equal(active(value).length, 0);
  assert(seen.slice(terminalStart).every((event) => event.working), "terminal ingestion and delivery have no idle gap");
  assert.equal(value.activitySnapshot().stores[0].queuedCompletionIds.length, 1);
  const retried = await value.retry("one");
  assert.equal(retried.attemptNumber, 2);
  const retryNonce = active(value)[0].attemptNonce;
  await value.onAgentSettled();
  await value.onAgentSettled();
  assert.equal(active(value)[0].attemptNonce, retryNonce, "old completions cannot clear a newer attempt");
  assert(value.activitySnapshot().working);

  await value.detach();
  assert.equal(seen.at(-1).phase, "detached");
  assert.equal(seen.at(-1).working, false);
  assert.deepEqual(seen.at(-1).stores, []);
  disconnect();
  const reloaded = manager();
  const disconnectReload = connectWorkerActivityBus(events, reloaded);
  await reloaded.attach(context());
  assert(reloaded.activitySnapshot().epoch > epoch);
  assert.equal(active(reloaded)[0].attemptNonce, retryNonce, "reload recovers exact current attempt from the existing manager scan");
  await finish(reloaded, "one", "failed");
  await reloaded.scan();
  await reloaded.onAgentSettled();
  assert.equal(reloaded.activitySnapshot().working, false);

  for (const status of ["needs_attention", "lost"]) {
    await reloaded.launch({ workerId: status, task: "terminal fixture" });
    await finish(reloaded, status, status);
    await reloaded.scan();
    assert.equal(active(reloaded).length, 0);
    assert(reloaded.activitySnapshot().working, "terminal result stays busy through its completion follow-up");
    await reloaded.onAgentSettled();
    assert.equal(reloaded.activitySnapshot().working, false);
  }

  // A fork changes the owner but preserves storage and attempt identity.
  await reloaded.launch({ workerId: "transfer", task: "transfer fixture" });
  const storageId = reloaded.activitySnapshot().stores[0].storageId;
  await reloaded.detach();
  disconnectReload();
  const parentFile = join(root, "parent.jsonl");
  await writeFile(parentFile, JSON.stringify({ type: "session", id: "parent" }) + "\n");
  const successor = manager();
  await successor.attach(context(root, "successor", "tui", parentFile));
  assert.equal(successor.activitySnapshot().ownerSessionId, "successor");
  assert.equal(successor.activitySnapshot().stores[0].storageId, storageId);
  assert.equal(active(successor)[0].workerId, "transfer");
  await successor.attach(context(root, "unrelated"));
  assert.equal(successor.activitySnapshot().working, false, "unrelated session cannot inherit old activity");
  assert.equal(successor.activitySnapshot().ownerSessionId, "unrelated");

  // In-flight delivery from an old epoch may not send to a replacement session.
  const delivery = manager();
  await delivery.attach(context(root, "delivery"));
  await delivery.launch({ workerId: "delivery", task: "delivery fixture" });
  await finish(delivery, "delivery");
  let sent = 0;
  delivery.pi.sendMessage = () => { throw new Error("delivery unavailable"); };
  await assert.rejects(delivery.scan(), /delivery unavailable/);
  assert.equal(delivery.activitySnapshot().phase, "error");
  assert(delivery.activitySnapshot().working, "send errors preserve queued completion activity");
  delivery.pi.sendMessage = () => { sent++; };
  let releaseInspect;
  let inspected;
  const gate = new Promise((resolveGate) => { releaseInspect = resolveGate; });
  const inspectEntered = new Promise((done) => { inspected = done; });
  const originalInspect = delivery.inspect.bind(delivery);
  delivery.inspect = async (...args) => { const result = await originalInspect(...args); inspected(); await gate; return result; };
  const pendingDelivery = delivery.dispatchNext();
  await inspectEntered;
  const detachingDelivery = delivery.detach();
  releaseInspect();
  await detachingDelivery;
  await delivery.attach(context(root, "delivery-other"));
  await pendingDelivery;
  assert.equal(sent, 0);
  assert.equal(delivery.activitySnapshot().working, false);

  // Binding operations restore the primary pointer before delayed escalation and
  // settlement. Both operations must retain the exact secondary store instead.
  const realKill = process.kill;
  const signals = [];
  process.kill = (pid, signal) => {
    if (signal === 0) return realKill(pid, signal);
    signals.push({ pid, signal });
    return true;
  };
  try {
    const source = manager();
    await source.attach(context(root, "secondary-source"));
    await source.launch({ workerId: "secondary", task: "secondary fixture" });
    await source.store.mutate((state) => {
      const attempt = state.workers.secondary.attempts[0];
      attempt.childPid = process.pid;
      attempt.childStartIdentity = source.processStartIdentity;
    });
    const secondaryStore = source.store;
    const secondaryState = await secondaryStore.load();
    const attempt = secondaryState.workers.secondary.attempts[0];
    const binding = { workerStorageId: secondaryState.storageId, launchOwnerSessionId: attempt.launchSessionId, workerId: "secondary", attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash };
    await source.detach();
    const delivered = [];
    const multi = manager({ sendMessage: (message) => delivered.push(message.details.completionId) }, { cancelEscalationMs: 20, cancelKillEscalationMs: 20 });
    await multi.attach(context(root, "multi-owner"));
    const primaryStore = multi.store;
    await multi.cancelBinding(binding, "regression");
    assert.equal(multi.store, primaryStore);
    for (let i = 0; i < 100 && signals.length < 2; i++) await new Promise((done) => setTimeout(done, 10));
    assert.deepEqual(signals, [{ pid: process.pid, signal: "SIGTERM" }, { pid: process.pid, signal: "SIGKILL" }]);
    await finish({ store: secondaryStore }, "secondary", "cancelled");
    await multi.inspectBinding(binding);
    assert.deepEqual(delivered, ["completion-secondary-1"]);
    for (const workerId of ["primary-a", "primary-b"]) {
      await multi.launch({ workerId, task: "primary completion fixture" });
      await finish(multi, workerId);
    }
    await Promise.all([multi.scan(), multi.dispatchNext()]);
    assert.equal(delivered.length, 1, "all stores share one in-flight completion slot");
    assert.equal((await secondaryStore.load()).inFlightCompletionId, "completion-secondary-1");
    await multi.onAgentSettled();
    assert.equal((await secondaryStore.load()).inFlightCompletionId, null);
    assert((await secondaryStore.load()).completedCompletionIds.includes("completion-secondary-1"));
    assert.equal(delivered.length, 2);
    assert(multi.activitySnapshot().working);
    await multi.onAgentSettled();
    assert.equal(delivered.length, 3);
    await multi.onAgentSettled();
    assert.equal(multi.activitySnapshot().working, false, "secondary settlement reaches final idle");
    assert.equal(multi.store, primaryStore);
    await multi.detach();

    // Stale generation, changed ownership, and replaced process identity may
    // never signal a child, even when a secondary cancellation timer exists.
    for (const change of ["owner", "process", "attempt", "detach"]) {
      const prior = manager();
      await prior.attach(context(root, `guard-source-${change}`));
      await prior.launch({ workerId: "guard", task: "guard fixture" });
      await prior.store.mutate((state) => {
        state.workers.guard.attempts[0].childPid = process.pid;
        state.workers.guard.attempts[0].childStartIdentity = prior.processStartIdentity;
      });
      const store = prior.store;
      const state = await store.load();
      const attempt = state.workers.guard.attempts[0];
      const binding = { workerStorageId: state.storageId, launchOwnerSessionId: attempt.launchSessionId, workerId: "guard", attemptNumber: attempt.attemptNumber, attemptNonce: attempt.attemptNonce, configHash: attempt.configHash };
      await prior.detach();
      const guarded = manager({}, { cancelEscalationMs: 80, cancelKillEscalationMs: 20 });
      await guarded.attach(context(root, `guard-owner-${change}`));
      await guarded.cancelBinding(binding, "guard");
      const signalCount = signals.length;
      if (change === "detach") await guarded.detach();
      else await store.mutate((draft) => {
        if (change === "owner") { draft.ownerSessionId = "foreign-owner"; draft.owner.sessionId = "foreign-owner"; }
        if (change === "process") draft.workers.guard.attempts[0].childStartIdentity = "replacement-process";
        if (change === "attempt") draft.workers.guard.attempts[0].configHash = `sha256:${"0".repeat(64)}`;
      });
      await new Promise((done) => setTimeout(done, 150));
      assert.equal(signals.length, signalCount, `${change} prevents cross-owner or stale-attempt signals`);
      assert.equal((await store.load()).workers.guard.attempts[0].terminationError, undefined);
      await guarded.detach();
    }

    // Settlement is not authority to acknowledge another owner's delivery.
    const ackSource = manager();
    await ackSource.attach(context(root, "ack-source"));
    await ackSource.launch({ workerId: "ack", task: "ack fixture" });
    const ackStore = ackSource.store;
    const ackState = await ackStore.load();
    const ackAttempt = ackState.workers.ack.attempts[0];
    await finish(ackSource, "ack");
    await ackSource.detach();
    const ackDeliveries = [];
    const ackOwner = manager({ sendMessage: (message) => ackDeliveries.push(message.details.completionId) });
    await ackOwner.attach(context(root, "ack-owner"));
    const ackBinding = { workerStorageId: ackState.storageId, launchOwnerSessionId: ackAttempt.launchSessionId, workerId: "ack", attemptNumber: ackAttempt.attemptNumber, attemptNonce: ackAttempt.attemptNonce, configHash: ackAttempt.configHash };
    await ackOwner.inspectBinding(ackBinding);
    assert.deepEqual(ackDeliveries, ["completion-ack-1"]);
    await ackStore.mutate((state) => { state.ownerSessionId = "ack-foreign"; state.owner.sessionId = "ack-foreign"; });
    await assert.rejects(ackOwner.onAgentSettled(), /ownership changed/);
    assert.equal((await ackStore.load()).inFlightCompletionId, "completion-ack-1");
    assert(!(await ackStore.load()).completedCompletionIds.includes("completion-ack-1"));
    await ackOwner.detach();
  } finally {
    process.kill = realKill;
  }

  // Headless managers can execute work, but never claim terminal reporting.
  for (const mode of ["json", "print", "rpc"]) {
    const headless = manager();
    const headlessBus = bus();
    const emitted = [];
    headlessBus.on(WORKER_ACTIVITY_EVENT, (event) => emitted.push(event));
    const stop = connectWorkerActivityBus(headlessBus, headless);
    await headless.attach(context(root, `headless-${mode}`, mode));
    headlessBus.emit(WORKER_ACTIVITY_REQUEST_EVENT, { schemaVersion: 1, ownerSessionId: `headless-${mode}`, requestId: "headless" });
    await headless.detach();
    assert.equal(emitted.length, 0);
    stop();
  }

  const failedLaunch = manager({}, { spawnSupervisor: async () => { throw new Error("spawn fixture failure"); } });
  await failedLaunch.attach(context(root, "failed-launch"));
  await assert.rejects(failedLaunch.launch({ workerId: "failed-launch", task: "failure fixture" }), /spawn fixture failure/);
  assert.equal(active(failedLaunch).length, 0);
  assert(failedLaunch.activitySnapshot().working, "failed spawn retains its queued completion");
  await failedLaunch.dispatchNext();
  await failedLaunch.onAgentSettled();
  assert.equal(failedLaunch.activitySnapshot().working, false);

  // An old live manager must retract its view when storage changes owner.
  const oldOwner = manager();
  await oldOwner.attach(context(root, "old-owner"));
  await oldOwner.launch({ workerId: "old-owner-worker", task: "ownership fixture" });
  const oldOwnerFile = join(root, "old-owner.jsonl");
  await writeFile(oldOwnerFile, JSON.stringify({ type: "session", id: "old-owner" }) + "\n");
  const newOwner = manager();
  await newOwner.attach(context(root, "new-owner", "tui", oldOwnerFile));
  assert(newOwner.activitySnapshot().working);
  await assert.rejects(oldOwner.scan(), /ownership changed/);
  assert.equal(oldOwner.activitySnapshot().working, false);
  assert.deepEqual(oldOwner.activitySnapshot().stores, []);

  // A dispatch resolving after detach publishes its immutable receipt only,
  // never new-owner activity or mutable state. Recovery binds that receipt.
  let releaseSpawn;
  let spawnEntered;
  const spawnGate = new Promise((done) => { releaseSpawn = done; });
  const entered = new Promise((done) => { spawnEntered = done; });
  const staleLaunch = manager({}, { spawnSupervisor: async () => { spawnEntered(); await spawnGate; return { pid: process.pid }; } });
  await staleLaunch.attach(context(root, "stale-launch"));
  const launching = staleLaunch.launch({ workerId: "stale-launch-worker", task: "late dispatch fixture" });
  await entered;
  await staleLaunch.detach();
  const detachedRevision = staleLaunch.activitySnapshot().revision;
  releaseSpawn();
  await assert.rejects(launching, /epoch changed/);
  assert.equal(staleLaunch.activitySnapshot().revision, detachedRevision);
  await staleLaunch.attach(context(root, "stale-launch"));
  assert.equal(active(staleLaunch)[0].workerId, "stale-launch-worker");
  assert.equal(active(staleLaunch)[0].status, "running");

  const unavailable = manager();
  await assert.rejects(unavailable.attach(context(parentFile, "unavailable")));
  assert.equal(unavailable.activitySnapshot().phase, "error");
  assert.equal(unavailable.activitySnapshot().working, false);

  // Real registration installs and removes both subscriptions with the session.
  const hooks = new Map();
  const integrationBus = bus();
  const pi = { events: integrationBus, on: (name, handler) => hooks.set(name, handler), registerTool() {}, registerCommand() {}, getActiveTools: () => [], sendMessage() {} };
  const integrated = registerWorkerRuntime(pi, options);
  managers.push(integrated);
  await hooks.get("session_start")({}, context(root, "integrated"));
  assert.equal(integrationBus.emitter.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 1);
  await hooks.get("session_shutdown")();
  assert.equal(integrationBus.emitter.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 0);
  assert.equal(integrated.activityListeners.size, 0);
  console.log("Worker activity snapshots and bus lifecycle tests OK");
} finally {
  await Promise.all(managers.map((value) => value.detach()));
  await rm(root, { recursive: true, force: true });
}
