import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RuntimeV2, StoreV2, CommandRunnerV2, selectorV2, stageChecksV2, auditSnapshotV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";
import { fixtureLifecycleV2, fixtureSourceV2, fixtureGitV2, mutationV2 as m } from "./fixtures/dag-v2-lifecycle.mjs";

const fresh = { current: async p => ({ repository: p.repository, source: p.source }) };
const input = candidate => ({ planId: "context", title: "Context protocol", repository: { repositoryId: "repo", baselineCommit: candidate.commit, baselineTree: candidate.tree, targetBranch: "refs/heads/main" },
  source: { governingClosure: `sha256:${"1".repeat(64)}`, refs: [fixtureSourceV2], scopeSummary: "fixture verification" },
  architecture: { outcomes: [{ id: "outcome", description: "record contract" }], nonGoals: ["external effects"], notes: [], risks: [] },
  workItems: [{ id: "item", title: "Record", objective: "Verify fixture", outcomeIds: ["outcome"], context: [], checks: ["record contract"], dependsOn: [], risk: "low", riskNotes: [], resources: {}, gates: [], lifecycle: fixtureLifecycleV2() }],
  constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] }, integration: { strategy: "serial", checks: ["land exact candidate"], finalChecks: ["record contract"], prefixCommands: [{ id: "prefix", argv: ["git", "status", "--porcelain"] }], finalCommands: [{ id: "final", argv: ["git", "status", "--porcelain"] }] } });
async function fixture(context = () => "same-real-evaluator", change = () => {}) {
  const root = await mkdtemp(join(tmpdir(), "dag-context-test-"));
  try {
    const actual = await fixtureGitV2(root), store = new StoreV2(root), rt = new RuntimeV2(store, fresh), calls = [];
    const producer = { run: async ({ cwd, request }) => {
      const text = await readFile(join(cwd, "file"), "utf8"); assert.equal(text, "baseline\n");
      calls.push(request.id);
      return { disposition: "PASS", observation: `Actually read ${text}`, findings: [], context: { id: context(request), lineage: [] } };
    } };
    const producers = new Map([["evaluator", producer]]), data = input(actual.candidate);
    for (const i of [1, 4]) data.workItems[0].lifecycle.checks[i].procedure = { kind: "producer", producerId: "evaluator" };
    change(data);
    const plan = await rt.save(data, 0);
    let run = await rt.start({ intent: "run", sessionId: "session", selection: selectorV2(plan), authority: { scope: plan.workItems.map(n => n.id), maxConcurrency: plan.constraints.maxConcurrency, effects: ["repository_local"], expiresAt: Date.now() + 86400000 } }, 1);
    run = await rt.acquireLease(run.runId, "session", run.revision);
    const f = { root, ...actual, store, rt, plan, run, calls, producers, runner: new CommandRunnerV2(store, actual.repository, producers), cleanup: () => rm(root, { recursive: true, force: true }) };
    for (const item of plan.workItems) {
      f.run = await rt.reserve(m(f.run), item.id, 1, "Implement record");
      f.run = await rt.dispatch(m(f.run), item.id, 1, { ensure: async () => ({ workerId: `implementation-${item.id}` }) });
      f.run = await rt.setCandidate(m(f.run), item.id, 1, f.candidate, f.runner);
    }
    return f;
  } catch (error) { await rm(root, { recursive: true, force: true }); throw error; }
}
async function reload(f) {
  f.store = new StoreV2(f.root); f.rt = new RuntimeV2(f.store, fresh);
  f.runner = new CommandRunnerV2(f.store, f.repository, f.producers);
  f.run = (await f.store.read()).runs[f.run.runId];
}
test("F7 producer replay without actual independent context is durably blocked", async () => {
  const f = await fixture(request => `actual-${request.id}`);
  try {
    await through(f, 6);
    const original = f.producers.get("evaluator");
    f.producers.set("evaluator", { run: async input => {
      const observation = await original.run(input);
      delete observation.context;
      return observation;
    } });
    const request = await prepare(f, "review");
    await f.runner.ensure(request);
    const result = await f.runner.read(request);
    assert.equal(result.executor.invoked, true);
    assert.equal(result.disposition, "BLOCKED");
    assert.match(result.diagnostic, /PRODUCER_INDEPENDENT_CONTEXT_REQUIRED/);
    await reload(f);
    await record(f, request);
    assert.equal(last(f).result.disposition, "BLOCKED");
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 7));
    f.run = await f.rt.cancel(m(f.run));
    f.run = await f.rt.reconcileCancellation(m(f.run), async () => {});
    assert.equal(f.run.status, "cancelled");
  } finally { await f.cleanup(); }
});

const last = (f, item = "item") => f.run.nodes[item].lifecycle.executions.at(-1);
async function prepare(f, check, item = "item") {
  f.run = await f.rt.prepareCheck(m(f.run), item, 1, check); return last(f, item).request;
}
async function record(f, request) { f.run = await f.rt.recordResult(m(f.run), request.itemId, request.id, f.runner); }
async function through(f, stage, item = "item") {
  for (let s = f.run.nodes[item].lifecycle.stage; s <= stage; s++) {
    for (const c of stageChecksV2(f.plan, item, s)) { const req = await prepare(f, c.id, item); await f.runner.ensure(req); await record(f, req); }
    f.run = await f.rt.advanceLifecycle(m(f.run), item, 1, s);
  }
}
async function reusedReview(f) {
  await through(f, 4); const req = await prepare(f, "review"); await f.runner.ensure(req);
  const raw = await f.runner.read(req); assert.equal(raw.disposition, "PASS"); assert.equal(raw.executor.invoked, true);
  return { req, raw };
}
function assertRejection(execution, raw) {
  assert.equal(execution.result.disposition, "BLOCKED");
  assert.equal(execution.contextRejection.reason, "EVALUATOR_CONTEXT_REUSED");
  assert.deepEqual(structuredClone(execution.contextRejection.observed), structuredClone(raw));
  assert.deepEqual(structuredClone(execution.result.executor), structuredClone(raw.executor)); // No invented fresh identity.
  assert.equal(execution.result.stdout, raw.stdout);
  assert.match(execution.result.diagnostic, /EVALUATOR_CONTEXT_REUSED/);
}

test("concrete reused F2/F5 observation survives reload, cancels and unblocks a terminal successor", async () => {
  const f = await fixture();
  try {
    const { req, raw } = await reusedReview(f);
    await reload(f); await f.runner.ensure(req); assert.equal(f.calls.length, 2);
    f.run = await f.rt.cancel(m(f.run));
    await assert.rejects(f.rt.reconcileCancellation(m(f.run), async () => {}), /EXECUTION_RECONCILIATION_REQUIRED/);
    const next = await f.rt.save({ ...input(f.candidate), planId: "successor", predecessor: selectorV2(f.plan) }, (await f.store.read()).revision);
    const start = { ...f.run.start, selection: selectorV2(next) };
    await assert.rejects(f.rt.start(start, (await f.store.read()).revision), /ACTIVE_BINDING_CONFLICT/);
    await reload(f); await record(f, req);
    assert.equal(last(f).status, "quarantined"); assertRejection(last(f), raw);
    assert.deepEqual(await f.runner.read(req), raw);
    await reload(f);
    const revision = f.run.revision; await record(f, req); assert.equal(f.run.revision, revision);
    const changed = structuredClone(raw); changed.stdout += "changed";
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, { read: async () => changed }), /EXECUTION_RESULT_CONFLICT/);
    await assert.rejects(f.rt.reconcileCancellation(m(f.run), async () => { throw Error("effects not settled"); }), /effects not settled/);
    assert.equal((await f.store.read()).runs[f.run.runId].status, "cancelling");
    let reconciled = 0;
    f.run = await f.rt.reconcileCancellation(m(f.run), async run => { assertRejection(run.nodes.item.lifecycle.executions.at(-1), raw); reconciled++; });
    assert.equal(reconciled, 1); assert.equal(f.run.status, "cancelled");
    const successor = await f.rt.start(start, (await f.store.read()).revision);
    assert.equal(successor.predecessorRunId, f.run.runId);
    assertRejection((await f.store.read()).runs[f.run.runId].nodes.item.lifecycle.executions.at(-1), raw);
    assert.equal(f.calls.length, 2);
  } finally { await f.cleanup(); }
});

test("ordinary rejection blocks passage, preserves raw evidence and supports one fresh infrastructure retry", async () => {
  let corrected = false;
  const f = await fixture(req => corrected ? `fresh-${req.id}` : "same-real-evaluator");
  try {
    const { req, raw } = await reusedReview(f); await record(f, req);
    assert.equal(last(f).status, "observed"); assertRejection(last(f), raw);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 5), /CHECK_NOT_PASSED.*EVALUATOR_CONTEXT_REUSED/);
    await assert.rejects(f.rt.integrate(m(f.run), { operationId: `${f.run.nodes.item.reservation.operationId}/integration`, runId: f.run.runId, itemId: "item", generation: 1, candidate: f.candidate, target: f.candidate }, { verify: async () => {} }), /LIFECYCLE_NOT_READY/);
    const snapshot = await f.store.read(), tampered = structuredClone(snapshot);
    const execution = tampered.runs[f.run.runId].nodes.item.lifecycle.executions.at(-1);
    execution.result = structuredClone(raw);
    assert.throws(() => auditSnapshotV2(tampered), /CONTEXT_REJECTION_RESULT_MISMATCH/);
    delete execution.contextRejection;
    assert.throws(() => auditSnapshotV2(tampered), /CONTEXT_REUSED/);
    await reload(f); const revision = f.run.revision; await record(f, req); assert.equal(f.run.revision, revision);
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
    assert.equal(f.run.nodes.item.lifecycle.stage, 5); assert.equal(f.run.nodes.item.retries[0].dimension, "infrastructure");
    assert.equal(f.run.nodes.item.retries[0].count, 1); assert.equal(last(f).status, "quarantined");
    corrected = true; await through(f, 7);
    f.run = await f.rt.advanceLifecycle(m(f.run), "item", 1, 8);
    assert.equal(f.run.nodes.item.lifecycle.ready, true);
    const retained = f.run.nodes.item.lifecycle.executions.find(e => e.request.id === req.id); assertRejection(retained, raw);
    assert.equal(f.calls.filter(id => id === req.id).length, 1);
    await reload(f); assert.equal(f.run.nodes.item.lifecycle.ready, true);
  } finally { await f.cleanup(); }
});

test("quarantined context history cannot be reused on retry or reset the infrastructure ceiling", async () => {
  const f = await fixture();
  try {
    const { req } = await reusedReview(f); await record(f, req);
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
    const retry = await prepare(f, "review"); assert.notEqual(retry.id, req.id);
    await f.runner.ensure(retry); const raw = await f.runner.read(retry); await record(f, retry);
    assertRejection(last(f), raw);
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, retry.id);
    assert.match(f.run.nodes.item.lifecycle.stop, /RETRY_EXHAUSTED: infrastructure/);
    assert.equal(f.run.nodes.item.retries[0].count, 1);
    await reload(f); f.run = await f.rt.cancel(m(f.run));
    f.run = await f.rt.reconcileCancellation(m(f.run), async () => {});
    assert.equal(f.run.status, "cancelled"); assert.equal(f.calls.length, 3);
  } finally { await f.cleanup(); }
});

for (const reverse of [false, true]) test(`two physically settled cross-item results are accepted atomically (${reverse ? "reverse" : "forward"} ingestion)`, async () => {
  const f = await fixture(() => "cross-item-evaluator", data => {
    data.workItems.push({ ...structuredClone(data.workItems[0]), id: "other" });
    data.constraints.maxConcurrency = 2; data.integration.strategy = "dependency_order";
  });
  try {
    const requests = [];
    for (const item of ["item", "other"]) { await through(f, 1, item); const req = await prepare(f, "behavior", item); await f.runner.ensure(req); requests.push(req); }
    if (reverse) requests.reverse();
    await reload(f);
    await record(f, requests[0]); assert.equal(last(f, requests[0].itemId).result.disposition, "PASS");
    await reload(f);
    await record(f, requests[1]); assertRejection(last(f, requests[1].itemId), await f.runner.read(requests[1]));
    assert.equal(last(f, requests[0].itemId).result.disposition, "PASS");
    f.run = await f.rt.cancel(m(f.run)); f.run = await f.rt.reconcileCancellation(m(f.run), async () => {});
    assert.equal(f.run.status, "cancelled"); assert.equal(f.calls.length, 2);
  } finally { await f.cleanup(); }
});
