import assert from "node:assert/strict";
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { RuntimeV2, StoreV2, CommandRunnerV2, createPlanV2, selectorV2, auditSnapshotV2, stageChecksV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";
import { fixtureLifecycleV2, fixtureSourceV2, fixtureGitV2, finishLifecycleV2, mutationV2 as m } from "./fixtures/dag-v2-lifecycle.mjs";
const tests = [], test = (name, fn) => tests.push([name, fn]);
const fresh = { current: async p => ({ repository: p.repository, source: p.source }) };
const input = candidate => ({ planId: "lifecycle", title: "Actual lifecycle", repository: { repositoryId: "repo", baselineCommit: candidate.commit, baselineTree: candidate.tree, targetBranch: "refs/heads/main" },
  source: { governingClosure: `sha256:${"1".repeat(64)}`, refs: [fixtureSourceV2], scopeSummary: "fixture verification" },
  architecture: { outcomes: [{ id: "outcome", description: "record contract" }], nonGoals: ["external effects"], notes: [], risks: [] },
  workItems: [{ id: "item", title: "Record", objective: "Verify fixture", outcomeIds: ["outcome"], context: [], checks: ["record contract"], dependsOn: [], risk: "low", riskNotes: [], resources: {}, gates: [], lifecycle: fixtureLifecycleV2() }],
  constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] }, integration: { strategy: "serial", checks: ["land exact candidate"], finalChecks: ["record contract"], prefixCommands: [{ id: "prefix", argv: ["git", "status", "--porcelain"] }], finalCommands: [{ id: "final", argv: ["git", "status", "--porcelain"] }] } });
async function fixture(change = () => {}, producers = new Map()) {
  const root = await mkdtemp(join(tmpdir(), "dag-lifecycle-test-")), actual = await fixtureGitV2(root), store = new StoreV2(root), rt = new RuntimeV2(store, fresh);
  const data = input(actual.candidate); change(data.workItems[0].lifecycle, data);
  const plan = await rt.save(data, 0);
  let run = await rt.start({ intent: "run", sessionId: "session", selection: selectorV2(plan), authority: { scope: plan.workItems.map(n => n.id), maxConcurrency: plan.constraints.maxConcurrency, effects: ["repository_local"], expiresAt: Date.now() + 86400000 } }, 1);
  run = await rt.acquireLease(run.runId, "session", run.revision); run = await rt.reserve(m(run), "item", 1, "Implement fixture");
  run = await rt.dispatch(m(run), "item", 1, { ensure: async () => {
    const beforeLaunch = (await store.read()).runs[run.runId].nodes.item.lifecycle;
    assert.deepEqual([...beforeLaunch.passed], [0]); assert.equal(beforeLaunch.candidateReady, false);
    return { workerId: "generic-implementation-worker" };
  } });
  const runner = new CommandRunnerV2(store, actual.repository, producers);
  return { root, ...actual, store, rt, plan, run, runner, cleanup: () => rm(root, { recursive: true, force: true }) };
}
async function busyRetry(fn) { for (let i = 0; ; i++) { try { return await fn(); } catch (e) { if (e.message !== "STORE_BUSY" || i > 100) throw e; await delay(10); } } }
async function frame(f) { f.run = await f.rt.setCandidate(m(f.run), "item", f.run.nodes.item.generation, f.candidate, f.runner); }
async function prepare(f, id) { f.run = await f.rt.prepareCheck(m(f.run), "item", f.run.nodes.item.generation, id); return f.run.nodes.item.lifecycle.executions.at(-1).request; }
async function execute(f, id) { const request = await prepare(f, id); await f.runner.ensure(request); f.run = await f.rt.recordResult(m(f.run), "item", request.id, f.runner); return request; }
async function through(f, stage) { for (let s = f.run.nodes.item.lifecycle.stage; s <= stage; s++) { for (const c of stageChecksV2(f.plan, "item", s)) await execute(f, c.id); f.run = await f.rt.advanceLifecycle(m(f.run), "item", f.run.nodes.item.generation, s); } }
const integration = f => ({ operationId: `${f.run.nodes.item.reservation.operationId}/integration`, runId: f.run.runId, itemId: "item", generation: f.run.nodes.item.generation, candidate: f.candidate, target: f.candidate });
const failCommand = check => { check.procedure = { kind: "command", argv: [process.execPath, "-e", "console.error('actionable failure'); process.exit(7)"] }; };
const observation = (detail, kind = "test_evidence_gap", materiality = "local") => ({ disposition: "FAIL", observation: detail, findings: [{ id: "finding", kind, severity: "blocking", materiality, subject: "record", fingerprint: "record-gap", detail }] });
const reader = result => ({ ensure: async () => {}, read: async () => result });

test("closed applicability and oracle schema cannot skip checkpoints or add attestation fields", async () => {
  const candidate = { commit: "1".repeat(40), tree: "2".repeat(40) };
  for (const mutate of [p => p.workItems[0].lifecycle.checks.pop(), p => p.workItems[0].lifecycle.checks[1].applicability = { kind: "not_applicable", reason: "tool absent", evidence: ["not executed"] }, p => p.workItems[0].lifecycle.oracle.checkIds = ["unknown"], p => p.workItems[0].lifecycle.checks[0].attestation = "PASS", p => p.workItems[0].lifecycle.checks[0].replay = "non_repeatable"]) {
    const p = input(candidate); mutate(p); assert.throws(() => createPlanV2(p, 1), /INVALID_V2|MISSING_LIFECYCLE_STAGE|ORACLE_NOT|NON_REPLAYABLE|UNGROUNDED/);
  }
});
test("worker claim and permissive integration adapter cannot advance; complete actual F0-F8 can", async () => {
  const f = await fixture(); try {
    await assert.rejects(f.rt.integrate(m(f.run), integration(f), { verify: async () => {} }), /LIFECYCLE_NOT_READY/);
    assert.deepEqual([...f.run.nodes.item.lifecycle.passed], [0]);
    await assert.rejects(f.rt.prepareCheck(m(f.run), "item", 1, "static"), /LIFECYCLE_NOT_EXECUTABLE/);
    await frame(f); assert.deepEqual(f.run.nodes.item.lifecycle.passed, [0]);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED.*static/);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 8), /STAGE_OUT_OF_ORDER/);
    await through(f, 7); assert.equal(f.run.nodes.item.lifecycle.ready, false);
    f.run = await f.rt.advanceLifecycle(m(f.run), "item", 1, 8);
    const lifecycle = f.run.nodes.item.lifecycle;
    assert.deepEqual(lifecycle.passed, [0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert.equal(lifecycle.executions.length, 13); // F1-F6 plus all seven at F7
    for (const e of lifecycle.executions) { assert(e.result.executor.invoked); assert.equal(e.result.exitCode, 0); assert(e.result.workspace.cleanAfter); assert(e.result.stdout.includes("verified record")); assert(e.result.durationMs >= 0); }
    assert.notEqual(lifecycle.executions.find(e => e.request.stage === 2).result.executor.contextId, lifecycle.executions.find(e => e.request.stage === 5).result.executor.contextId);
    f.run = await f.rt.integrate(m(f.run), integration(f), { verify: async (_r, _p, exact) => { assert.equal(f.git("rev-parse", "HEAD"), exact.target.commit); } });
    assert.equal(f.run.status, "complete"); auditSnapshotV2(await f.store.read());
  } finally { await f.cleanup(); }
});
test("missing, wrong-plan, attempt, generation and candidate results never advance", async () => {
  const f = await fixture(); try {
    await frame(f); const req = await prepare(f, "static");
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, f.runner), /NOT_DURABLE/);
    await f.runner.ensure(req); const result = await f.runner.read(req), before = await readFile(f.store.statePath, "utf8");
    for (const mutate of [r => r.request.plan.revision++, r => r.request.runId = "other", r => r.request.itemId = "other", r => r.request.attempt += "x", r => r.request.generation++, r => r.request.candidate.tree = "9".repeat(40), r => r.executor.invoked = false, r => r.exitCode = 4]) {
      const wrong = structuredClone(result); mutate(wrong);
      await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, reader(wrong)), /MISMATCH|UNEXECUTED/);
      assert.equal(await readFile(f.store.statePath, "utf8"), before);
    }
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    f.run = await f.rt.advanceLifecycle(m(f.run), "item", 1, 1); assert.equal(f.run.nodes.item.lifecycle.stage, 2);
  } finally { await f.cleanup(); }
});
test("failed argv retains exact exit, timing, bounded output and actionable diagnostics", async () => {
  const f = await fixture(l => { l.checks[0].procedure.argv = [process.execPath, "-e", "console.log('x'.repeat(50000)); console.error('specific broken assertion'); process.exit(23)"]; }); try {
    await frame(f); const req = await execute(f, "static"), result = await f.runner.read(req);
    assert.equal(result.exitCode, 23); assert.equal(result.disposition, "FAIL"); assert.equal(result.stdout.length, 16384); assert(result.truncated);
    assert.match(result.stderr, /specific broken assertion/); assert.match(result.diagnostic, /argv=.*exit=23/); assert(result.endedAt >= result.startedAt);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /static.*exit=23/);
  } finally { await f.cleanup(); }
});
test("only actually invoked producers are evidence; missing producers and executables block", async () => {
  let unused = 0, calls = 0;
  const f = await fixture(l => { l.checks[0].procedure = { kind: "producer", producerId: "record-reader" }; }, new Map([
    ["unused", { run: async () => { unused++; throw Error("never run"); } }],
    ["record-reader", { run: async ({ cwd }) => { calls++; assert.equal(await readFile(join(cwd, "file"), "utf8"), "baseline\n"); return { disposition: "PASS", observation: "Read actual record: baseline newline", findings: [] }; } }],
  ])); try {
    await frame(f); const req = await execute(f, "static"); assert.equal(calls, 1); assert.equal(unused, 0);
    assert.equal((await f.runner.read(req)).executor.identity, "record-reader"); await f.runner.ensure(req); assert.equal(calls, 1);
  } finally { await f.cleanup(); }
  for (const procedure of [{ kind: "producer", producerId: "unregistered" }, { kind: "command", argv: ["/does-not-exist/dag-check"] }]) {
    const g = await fixture(l => l.checks[0].procedure = procedure); try {
      await frame(g); const req = await execute(g, "static"), r = await g.runner.read(req);
      assert.equal(r.disposition, "BLOCKED"); assert.equal(r.executor.invoked, false);
      await assert.rejects(g.rt.advanceLifecycle(m(g.run), "item", 1, 1), /CHECK_NOT_PASSED|UNRESOLVED_FINDINGS/);
    } finally { await g.cleanup(); }
  }
});
test("command acknowledgement loss and service reload read one durable execution", async () => {
  const f = await fixture(); try {
    await frame(f); const req = await prepare(f, "static");
    await assert.rejects((async () => { await f.runner.ensure(req); throw Error("ack lost"); })(), /ack lost/);
    const bytes = JSON.stringify((await f.store.read()).executions[req.id]);
    const newRunner = new CommandRunnerV2(new StoreV2(f.root), f.repository);
    await newRunner.ensure(req); assert.equal(JSON.stringify((await f.store.read()).executions[req.id]), bytes);
    f.run = await new RuntimeV2(new StoreV2(f.root), fresh).recordResult(m(f.run), "item", req.id, newRunner);
    assert.equal(f.run.nodes.item.lifecycle.executions[0].status, "observed");
    const revision = f.run.revision; f.run = await f.rt.recordResult(m(f.run), "item", req.id, newRunner); assert.equal(f.run.revision, revision);
  } finally { await f.cleanup(); }
});
test("F2/F5 must be fresh independent contexts even for a trusted result adapter", async () => {
  const f = await fixture(); try {
    await frame(f); await through(f, 4);
    const req = await prepare(f, "review"); await f.runner.ensure(req);
    const actual = await f.runner.read(req), bad = structuredClone(actual);
    bad.executor.contextId = f.run.nodes.item.lifecycle.executions.find(e => e.request.stage === 2).result.executor.contextId;
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, reader(bad)), /CONTEXT_REUSED/);
    bad.executor.contextId = "generic-implementation-worker";
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, reader(bad)), /INDEPENDENT_CONTEXT/);
    bad.executor.contextId = "new"; bad.executor.lineage = ["prior-reasoning"];
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, reader(bad)), /INDEPENDENT_CONTEXT/);
  } finally { await f.cleanup(); }
});
test("candidate changes invalidate F2/F5/F7/readiness, preserve prior results and quarantine late outcomes", async () => {
  const f = await fixture(); try {
    f.run = await finishLifecycleV2(f.rt, f.plan, f.run, "item", f.candidate, f.repository);
    const previous = f.run.nodes.item.lifecycle.executions.length;
    await writeFile(join(f.repository, "file"), "integrated a\n"); f.git("add", "."); f.git("commit", "-m", "new candidate");
    const candidate = { commit: f.git("rev-parse", "HEAD"), tree: f.git("rev-parse", "HEAD^{tree}") };
    f.run = await f.rt.setCandidate(m(f.run), "item", 1, candidate, f.runner);
    assert.deepEqual(f.run.nodes.item.lifecycle.passed, [0]); assert.equal(f.run.nodes.item.lifecycle.executions.length, previous);
    assert(f.run.nodes.item.lifecycle.executions.every(e => e.status === "quarantined"));
    await assert.rejects(f.rt.integrate(m(f.run), { ...integration(f), candidate, target: candidate }, { verify: async () => {} }), /LIFECYCLE_NOT_READY/);
    const req = await prepare(f, "static"); await f.runner.ensure(req);
    await writeFile(join(f.repository, "file"), "integrated b\n"); f.git("add", "."); f.git("commit", "-m", "newer");
    f.run = await f.rt.setCandidate(m(f.run), "item", 1, { commit: f.git("rev-parse", "HEAD"), tree: f.git("rev-parse", "HEAD^{tree}") }, f.runner);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    assert.equal(f.run.nodes.item.lifecycle.executions.at(-1).status, "quarantined"); assert.equal(f.run.nodes.item.lifecycle.stage, 1);
  } finally { await f.cleanup(); }
});
test("F7 checks run on clean materializations of exact final candidate; editing checks fail", async () => {
  const f = await fixture(l => { l.checks[6].procedure.argv = [process.execPath, "-e", "require('fs').appendFileSync('file','changed\\n')"]; }); try {
    await frame(f); await through(f, 6);
    const req = await execute(f, "final"), result = await f.runner.read(req);
    assert(result.workspace.cleanBefore); assert.equal(result.workspace.cleanAfter, false); assert.equal(result.disposition, "FAIL");
    assert.match(result.diagnostic, /changed during no-edit/); assert.equal(await readFile(join(f.repository, "file"), "utf8"), "baseline\n");
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 7), /CHECK_NOT_PASSED/);
  } finally { await f.cleanup(); }
});
test("pause permits settlement but not dispatch; cancellation fences before signal and quarantines late results", async () => {
  const f = await fixture(l => { l.checks[0].procedure.argv = [process.execPath, "-e", "setTimeout(()=>console.log('finished'),500)"]; }); try {
    await frame(f); const req = await prepare(f, "static"), controller = new AbortController();
    const running = f.runner.ensure(req, controller.signal);
    for (let i = 0; i < 200; i++) { try { if ((await f.store.read()).executions?.[req.id]) break; } catch {} await delay(10); }
    f.run = await busyRetry(() => f.rt.cancel(m(f.run))); controller.abort();
    assert.equal(f.run.nodes.item.generation, 2);
    await assert.rejects(busyRetry(() => f.rt.reconcileCancellation(m(f.run), async () => {})), /RECONCILIATION_REQUIRED/);
    await running; f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    assert.equal(f.run.nodes.item.lifecycle.executions[0].status, "quarantined");
    f.run = await f.rt.reconcileCancellation(m(f.run), async () => {}); assert.equal(f.run.status, "cancelled");
  } finally { await f.cleanup(); }
  const g = await fixture(); try {
    await frame(g); const req = await prepare(g, "static"); await g.runner.ensure(req);
    g.run = await g.rt.control(m(g.run), "pause"); g.run = await g.rt.recordResult(m(g.run), "item", req.id, g.runner);
    assert.equal(g.run.nodes.item.lifecycle.executions[0].status, "observed");
    await assert.rejects(g.rt.advanceLifecycle(m(g.run), "item", 1, 1), /RUN_NOT_ACTIVE/);
  } finally { await g.cleanup(); }
});
test("unlaunched cancelled intent cannot spawn and needs explicit durable non-execution reconciliation", async () => {
  const f = await fixture(); try {
    await frame(f); const req = await prepare(f, "static"); f.run = await f.rt.cancel(m(f.run));
    await assert.rejects(f.runner.ensure(req), /CURRENT_EXECUTION_INTENT_REQUIRED/);
    assert.equal((await f.store.read()).executions?.[req.id], undefined);
    await f.runner.reconcileUnlaunched(req);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    assert.equal(f.run.nodes.item.lifecycle.executions[0].result.executor.invoked, false);
    f.run = await f.rt.reconcileCancellation(m(f.run), async () => {}); assert.equal(f.run.status, "cancelled");
  } finally { await f.cleanup(); }
});
test("typed plan-affecting findings stop the whole run; explicit disposition required", async () => {
  const f = await fixture(l => { l.checks[0].procedure = { kind: "producer", producerId: "architecture" }; }, new Map([["architecture", { run: async ({ cwd }) => { assert.equal(await readFile(join(cwd, "file"), "utf8"), "baseline\n"); return observation("oracle needs a new semantic dependency", "oracle_contract_issue", "plan_affecting"); } }]])); try {
    await frame(f); await execute(f, "static"); assert.equal(f.run.status, "needs_replan");
    await assert.rejects(f.rt.prepareCheck(m(f.run), "item", 1, "static"), /RUN_NOT_ACTIVE/);
    await assert.rejects(f.rt.control(m(f.run), "resume", "ignored"), /PLAN_FINDING_DISPOSITION_REQUIRED/);
    const id = f.run.nodes.item.lifecycle.findings[0].finding.id;
    f.run = await f.rt.dispositionFinding(m(f.run), "item", id, "Dismissed after independent review: existing dependency covers this");
    f.run = await f.rt.control(m(f.run), "resume", "Finding dismissed, unchanged plan"); assert.equal(f.run.status, "active");
  } finally { await f.cleanup(); }
});
test("test-evidence findings route F6 to F3 without discarding unchanged F2, production defects route F1", async () => {
  for (const kind of ["test_evidence_gap", "product_defect", "architecture_issue"]) {
    const f = await fixture(l => { l.checks[5].procedure = { kind: "producer", producerId: "hardener" }; }, new Map([["hardener", { run: async ({ cwd }) => { await readFile(join(cwd, "file")); return observation("discovered gap", kind); } }]])); try {
      await frame(f); await through(f, 5); const req = await execute(f, "hardening");
      f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
      assert.equal(f.run.nodes.item.lifecycle.stage, kind === "test_evidence_gap" ? 3 : kind === "architecture_issue" ? 5 : 1);
      assert.equal(f.run.nodes.item.retries[0].dimension, kind === "test_evidence_gap" ? "test" : kind === "architecture_issue" ? "review" : "product");
      if (kind === "test_evidence_gap") assert(f.run.nodes.item.lifecycle.passed.includes(2));
    } finally { await f.cleanup(); }
  }
});
test("per-dimension retries and no-progress stop; replacements cannot reset counters", async () => {
  const f = await fixture(l => failCommand(l.checks[0])); try {
    await frame(f);
    for (let i = 0; i < 4 && !f.run.nodes.item.lifecycle.stop; i++) {
      const req = await execute(f, "static"); f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
    }
    assert.match(f.run.nodes.item.lifecycle.stop, /NO_PROGRESS|RETRY_EXHAUSTED/);
    const counts = structuredClone(f.run.nodes.item.retries);
    f.run = await f.rt.replace(m(f.run), "item", 1, async () => {});
    assert.deepEqual(structuredClone(f.run.nodes.item.retries.filter(r => r.dimension !== "replacement")), counts);
    f.run = await f.rt.replace(m(f.run), "item", 2, async () => {});
    await assert.rejects(f.rt.replace(m(f.run), "item", 3, async () => {}), /RETRY_EXHAUSTED: replacement/);
    await assert.rejects(f.rt.prepareCheck(m(f.run), "item", 3, "static"), /LIFECYCLE_NOT_EXECUTABLE/);
  } finally { await f.cleanup(); }
});
test("infrastructure retry has its own one-retry ceiling", async () => {
  const f = await fixture(l => l.checks[0].procedure = { kind: "producer", producerId: "absent" }); try {
    await frame(f);
    for (let i = 0; i < 2; i++) {
      const req = await execute(f, "static"); f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
    }
    assert.match(f.run.nodes.item.lifecycle.stop, /RETRY_EXHAUSTED: infrastructure/);
    assert.equal(f.run.nodes.item.retries[0].count, 1);
  } finally { await f.cleanup(); }
});
test("executor process death leaves ambiguity, not replay; verified settlement permits bounded retry", async () => {
  const f = await fixture(l => { l.checks[0].procedure = { kind: "producer", producerId: "die" }; }); try {
    await frame(f); const req = await prepare(f, "static");
    const url = pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href;
    const p = spawn(process.execPath, ["--input-type=module", "-e", `import {CommandRunnerV2,StoreV2} from ${JSON.stringify(url)}; const runner=new CommandRunnerV2(new StoreV2(process.argv[1]),process.argv[2],new Map([['die',{run:async()=>process.exit(88)}]])); await runner.ensure(JSON.parse(process.argv[3]));`, f.root, f.repository, JSON.stringify(req)], { stdio: ["ignore", "pipe", "pipe"] });
    let errors = ""; p.stderr.on("data", b => errors += b); const code = await new Promise(resolve => p.on("close", resolve)); assert.equal(code, 88, errors);
    assert.equal((await f.store.read()).executions[req.id].status, "running");
    await f.runner.ensure(req); assert.equal(await f.runner.read(req), null);
    await assert.rejects(f.runner.reconcileInterrupted(req, async () => { throw Error("not yet settled"); }), /not yet settled/);
    await f.runner.reconcileInterrupted(req, async () => {});
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    assert.equal(f.run.nodes.item.lifecycle.executions[0].result.disposition, "BLOCKED");
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id); assert.equal(f.run.nodes.item.retries[0].dimension, "infrastructure");
  } finally { await f.cleanup(); }
});
test("not-applicable check has positive plan evidence and is never invoked or attested", async () => {
  const f = await fixture(l => l.checks.push({ ...structuredClone(l.checks[0]), id: "irrelevant", applicability: { kind: "not_applicable", reason: "fixture has no database schema", evidence: ["fixture:independent-record-contract"] }, procedure: { kind: "command", argv: ["/must-not-run"] } })); try {
    await frame(f); await assert.rejects(f.rt.prepareCheck(m(f.run), "item", 1, "irrelevant"), /NOT_APPLICABLE/);
    f.run = await finishLifecycleV2(f.rt, f.plan, f.run, "item", f.candidate, f.repository);
    assert(f.run.nodes.item.lifecycle.executions.every(e => e.request.check.id !== "irrelevant"));
  } finally { await f.cleanup(); }
});
test("producer callback alone cannot claim a fresh F5 context; malformed output settles as protocol failure", async () => {
  for (const observed of [{ disposition: "PASS", observation: "callback claim", findings: [] }, { disposition: "made-up", observation: "bad protocol", findings: [], context: { id: "reviewer", lineage: [] } }]) {
    const f = await fixture(l => l.checks[4].procedure = { kind: "producer", producerId: "reviewer" }, new Map([["reviewer", { run: async ({ cwd }) => { await readFile(join(cwd, "file")); return observed; } }]])); try {
      await frame(f); await through(f, 4); const req = await execute(f, "review"), result = await f.runner.read(req);
      assert.equal(result.disposition, "BLOCKED"); assert(result.executor.invoked);
      assert.match(result.diagnostic, /INDEPENDENT_CONTEXT|Invalid execution response/);
      assert.equal((await f.store.read()).executions[req.id].status, "settled");
    } finally { await f.cleanup(); }
  }
});
test("process death after result rename reconciles exact durable outcome without reexecution", async () => {
  const f = await fixture(); let req; try {
    await frame(f); req = await prepare(f, "static"); const url = pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href;
    const code = `import {CommandRunnerV2,StoreV2} from ${JSON.stringify(url)}; import {readFileSync} from 'node:fs';
      const req=JSON.parse(process.argv[3]); let store; store=new StoreV2(process.argv[1],{failpoint:point=>{if(point==='renamed' && JSON.parse(readFileSync(store.statePath,'utf8')).executions?.[req.id]?.result)process.exit(88)}});
      await new CommandRunnerV2(store,process.argv[2]).ensure(req);`;
    const p = spawn(process.execPath, ["--input-type=module", "-e", code, f.root, f.repository, JSON.stringify(req)], { stdio: ["ignore", "pipe", "pipe"] });
    let errors = ""; p.stderr.on("data", b => errors += b); assert.equal(await new Promise(resolve => p.on("close", resolve)), 88, errors);
    const original = JSON.stringify((await f.store.read()).executions[req.id]); await f.runner.ensure(req);
    assert.equal(JSON.stringify((await f.store.read()).executions[req.id]), original);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner); assert.equal(f.run.nodes.item.lifecycle.executions[0].result.disposition, "PASS");
  } finally { if (req) await retainedCleanup(f, req); await f.cleanup(); }
});
test("extinction receipt survives crash after candidate cleanup before lifecycle publication", async () => {
  const f = await fixture(); let req;
  try {
    await frame(f); req = await prepare(f, "static");
    const url = new URL("../extensions/dag-workflow/runtime-v2/index.ts", import.meta.url).href;
    const code = `import {CommandRunnerV2,StoreV2} from ${JSON.stringify(url)};
      const runner=new CommandRunnerV2(new StoreV2(process.argv[1]),process.argv[2]);
      const execute=runner.execute.bind(runner); runner.execute=async(...args)=>{await execute(...args);process.exit(88)};
      await runner.ensure(JSON.parse(process.argv[3]));`;
    const owner = spawn(process.execPath, ["--input-type=module", "-e", code, f.root, f.repository, JSON.stringify(req)], { stdio: ["ignore", "pipe", "pipe"] });
    let errors = ""; owner.stderr.on("data", b => errors += b); owner.stdout.resume();
    assert.equal(await new Promise(resolve => owner.once("close", resolve)), 88, errors);
    const job = await commandJob(f, req), receipt = JSON.parse(await readFile(outcomeFile(job), "utf8"));
    assert.equal(receipt.extinct, true); assert.equal(receipt.exitCode, 0); assert.equal(receipt.invoked, true);
    assert.equal(job.status, "running"); assert.equal(job.result, undefined);
    await assert.rejects(readFile(join(job.workspace, "file")), /ENOENT/);
    await f.runner.ensure(req); assert.equal(await f.runner.read(req), null);
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, f.runner), /NOT_DURABLE/);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED/);
    await assert.rejects(f.runner.reconcileInterrupted(req, async () => { throw Error("independent effect settlement required"); }), /independent effect/);
    await f.runner.reconcileInterrupted(req, async () => {});
    const result = await f.runner.read(req);
    assert.equal(result.disposition, "BLOCKED"); assert.equal(result.exitCode, 0); assert.equal(result.executor.invoked, true);
  } finally { if (req) await retainedCleanup(f, req); await f.cleanup(); }
});
test("a real behavioral oracle fails on an invalid committed candidate despite static success", async () => {
  const f = await fixture(); try {
    await writeFile(join(f.repository, "file"), "wrong record\n"); f.git("add", "."); f.git("commit", "-m", "invalid behavior");
    f.candidate = { commit: f.git("rev-parse", "HEAD"), tree: f.git("rev-parse", "HEAD^{tree}") };
    await frame(f); await through(f, 1); const req = await execute(f, "behavior");
    const result = await f.runner.read(req); assert.equal(result.disposition, "FAIL"); assert.match(result.stderr, /AssertionError/);
    assert.equal(f.run.nodes.item.lifecycle.stage, 2);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 2), /behavior.*exit=1/s);
  } finally { await f.cleanup(); }
});
test("cancellation between durable job intent and actual invocation cannot start a producer", async () => {
  let invoked = 0;
  const f = await fixture(l => l.checks[0].procedure = { kind: "producer", producerId: "observer" }, new Map([["observer", { run: async () => { invoked++; return { disposition: "PASS", observation: "not allowed", findings: [] }; } }]])); try {
    await frame(f); const req = await prepare(f, "static");
    const inspect = f.runner.inspect.bind(f.runner); let release, entered;
    const paused = new Promise(resolve => release = resolve), ready = new Promise(resolve => entered = resolve);
    f.runner.inspect = async candidate => { await inspect(candidate); entered(); await paused; };
    const execution = f.runner.ensure(req); await ready;
    f.run = await f.rt.cancel(m(f.run)); release(); await execution;
    assert.equal(invoked, 0); const result = await f.runner.read(req); assert.equal(result.executor.invoked, false);
    assert.match(result.diagnostic, /FENCED_BEFORE_INVOCATION/);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner); assert.equal(f.run.nodes.item.lifecycle.executions[0].status, "quarantined");
  } finally { await f.cleanup(); }
});
test("recurring candidate tree stops at adoption and retained history survives replacement", async () => {
  const f = await fixture(); try {
    await frame(f); const original = f.candidate;
    await writeFile(join(f.repository, "file"), "integrated a\n"); f.git("add", "."); f.git("commit", "-m", "candidate a");
    const next = { commit: f.git("rev-parse", "HEAD"), tree: f.git("rev-parse", "HEAD^{tree}") };
    f.run = await f.rt.setCandidate(m(f.run), "item", 1, next, f.runner);
    f.run = await f.rt.setCandidate(m(f.run), "item", 1, original, f.runner);
    assert.match(f.run.nodes.item.lifecycle.stop, /recurring candidate tree/);
    assert.deepEqual(structuredClone(f.run.nodes.item.lifecycle.candidates), [original, next]);
    f.run = await f.rt.replace(m(f.run), "item", 1, async () => {});
    assert.match(f.run.nodes.item.lifecycle.stop, /NO_PROGRESS/);
    await assert.rejects(f.rt.dispatch(m(f.run), "item", 2, { ensure: async () => { throw Error("must not launch"); } }), /F0_PREFLIGHT_REQUIRED/);
  } finally { await f.cleanup(); }
});
test("independent evaluator contexts cannot be reused across work items", async () => {
  const f = await fixture((_l, data) => { data.workItems.push({ ...structuredClone(data.workItems[0]), id: "other" }); data.constraints.maxConcurrency = 2; data.integration.strategy = "dependency_order"; }); try {
    await frame(f); await through(f, 2);
    const context = f.run.nodes.item.lifecycle.executions.find(e => e.request.stage === 2).result.executor.contextId;
    f.run = await f.rt.reserve(m(f.run), "other", 1, "other implementation");
    f.run = await f.rt.dispatch(m(f.run), "other", 1, { ensure: async () => ({ workerId: "other-implementation-worker" }) });
    f.run = await f.rt.setCandidate(m(f.run), "other", 1, f.candidate, f.runner);
    for (const id of ["static", "behavior"]) {
      f.run = await f.rt.prepareCheck(m(f.run), "other", 1, id); const req = f.run.nodes.other.lifecycle.executions.at(-1).request;
      await f.runner.ensure(req);
      if (id === "static") {
        f.run = await f.rt.recordResult(m(f.run), "other", req.id, f.runner); f.run = await f.rt.advanceLifecycle(m(f.run), "other", 1, 1);
      } else {
        const bad = structuredClone(await f.runner.read(req)); bad.executor.contextId = context;
        await assert.rejects(f.rt.recordResult(m(f.run), "other", req.id, reader(bad)), /CONTEXT_REUSED_ACROSS_ITEMS/);
      }
    }
  } finally { await f.cleanup(); }
});
const processState = async pid => {
  try { const text = await readFile(`/proc/${pid}/stat`, "utf8"); return text.slice(text.lastIndexOf(")") + 2).split(" ")[0]; }
  catch (e) { if (["ENOENT", "ESRCH"].includes(e.code)) return "absent"; throw e; }
};
const liveProcess = async pid => !["Z", "X", "absent"].includes(await processState(pid));
async function waitForFile(path) {
  for (let i = 0; i < 500; i++) { try { return await readFile(path, "utf8"); } catch (e) { if (e.code !== "ENOENT") throw e; } await delay(10); }
  throw Error(`Timed out waiting for ${path}`);
}
async function retainedCleanup(f, req) {
  const job = (await f.store.read()).executions[req.id];
  if (job?.workspace) {
    try { f.git("worktree", "remove", "--force", job.workspace); } catch {}
    await rm(resolve(job.workspace, ".."), { recursive: true, force: true });
  }
}

test("inherited Git selectors/config cannot redirect source inspection, checks or no-edit validation", async () => {
  const f = await fixture(l => l.checks[0].procedure = { kind: "command", argv: [process.execPath, "-e", `
    const fs=require('node:fs'),{execFileSync}=require('node:child_process'),assert=require('node:assert/strict');
    assert.equal(process.env.GIT_WORK_TREE,undefined); assert.equal(process.env.GIT_DIR,undefined);
    assert.equal(process.env.GIT_INDEX_FILE,undefined); assert.equal(process.env.GIT_CONFIG_COUNT,undefined);
    assert.equal(process.env.GIT_CONFIG_PARAMETERS,undefined);
    assert.equal(execFileSync('git',['rev-parse','--show-toplevel'],{encoding:'utf8'}).trim(),process.cwd());
    fs.appendFileSync('file','MUTATION\\n'); console.log('changed actual candidate');
  `] });
  const keys = { GIT_WORK_TREE: f.repository, GIT_DIR: join(f.root, "not-a-git-dir"), GIT_COMMON_DIR: join(f.root, "bad-common"),
    GIT_INDEX_FILE: join(f.root, "bad-index"), GIT_OBJECT_DIRECTORY: join(f.root, "bad-objects"), GIT_ALTERNATE_OBJECT_DIRECTORIES: join(f.root, "bad-alternates"),
    GIT_CONFIG_COUNT: "1", GIT_CONFIG_KEY_0: "core.worktree", GIT_CONFIG_VALUE_0: f.repository,
    GIT_CONFIG_PARAMETERS: "'core.worktree'='" + f.repository + "'", GIT_CONFIG: join(f.root, "bad-config"),
    GIT_CONFIG_GLOBAL: join(f.root, "bad-global"), GIT_NAMESPACE: "other", GIT_CEILING_DIRECTORIES: "/tmp" };
  const saved = Object.fromEntries(Object.keys(keys).map(key => [key, process.env[key]])); let req;
  try {
    Object.assign(process.env, keys);
    await frame(f); req = await execute(f, "static"); const result = await f.runner.read(req);
    assert.equal(result.exitCode, 0, result.stderr); assert.equal(result.disposition, "FAIL");
    assert(result.workspace.cleanBefore); assert.equal(result.workspace.cleanAfter, false);
    const job = (await f.store.read()).executions[req.id];
    assert.match(await readFile(join(job.workspace, "file"), "utf8"), /MUTATION/);
    assert.equal(await readFile(join(f.repository, "file"), "utf8"), "baseline\n");
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED/);
    assert.equal(f.run.nodes.item.lifecycle.stage, 1);
  } finally {
    for (const [key, value] of Object.entries(saved)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    if (req) await retainedCleanup(f, req); await f.cleanup();
  }
});

test("contradictory dirty cleanup cannot retain a PASS even if the cleanliness observer claimed clean", async () => {
  const f = await fixture(l => l.checks[0].procedure.argv = [process.execPath, "-e", "require('fs').appendFileSync('file','mutation\\n')"]); let req;
  try {
    await frame(f);
    // Fault injection at the independent observer recreates the reviewer's
    // contradictory cleanAfter/dirty-cleanup path without weakening real Git.
    f.runner.clean = () => true;
    req = await execute(f, "static"); const result = await f.runner.read(req);
    assert.equal(result.disposition, "BLOCKED"); assert.equal(result.workspace.cleanAfter, false);
    assert.match(result.diagnostic, /cleanup retained/);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED/);
  } finally { if (req) await retainedCleanup(f, req); await f.cleanup(); }
});

for (const aborted of [false, true]) test(`same-session unref descendant prevents premature settlement/cleanup (${aborted ? "abort with TERM resistance" : "normal leader exit"})`, async () => {
  const probes = await mkdtemp(join(tmpdir(), "n03-descendant-")), pidfile = join(probes, "pid");
  const descendant = `process.on('SIGTERM',()=>{}); require('fs').writeFileSync(${JSON.stringify(pidfile)},String(process.pid)); setTimeout(()=>process.exit(0),${aborted ? 20000 : 1800}); setInterval(()=>{},1000);`;
  const parent = `const c=require('child_process').spawn(process.execPath,['-e',${JSON.stringify(descendant)}],{stdio:'ignore'}); c.unref(); ${aborted ? "setInterval(()=>{},1000);" : ""}`;
  const f = await fixture(l => l.checks[0].procedure.argv = [process.execPath, "-e", parent]);
  let pid, req, execution;
  const controller = new AbortController();
  try {
    await frame(f); req = await prepare(f, "static"); execution = f.runner.ensure(req, controller.signal);
    pid = Number(await waitForFile(pidfile)); assert(await liveProcess(pid));
    let job = (await f.store.read()).executions[req.id];
    const journal = JSON.parse(await readFile(join(resolve(job.workspace, ".."), "command-process.json"), "utf8"));
    assert.equal(journal.requestId, req.id); assert(journal.identity.pid > 0); assert.match(journal.identity.processStart, /:/);
    if (aborted) { f.run = await busyRetry(() => f.rt.cancel(m(f.run))); controller.abort(); }
    await delay(300);
    assert(await liveProcess(pid), "descendant should still be running before normal exit / KILL escalation");
    job = (await f.store.read()).executions[req.id]; assert.equal(job.status, "running"); assert.equal(job.result, undefined);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "baseline\n");
    assert.equal(await f.runner.read(req), null);
    await f.runner.ensure(req); // Natural identity is not permission to relaunch.
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, f.runner), /NOT_DURABLE/);
    if (aborted) await assert.rejects(f.rt.reconcileCancellation(m(f.run), async () => {}), /RECONCILIATION_REQUIRED/);
    else {
      await assert.rejects(f.rt.retryCheck(m(f.run), "item", 1, req.id), /RETRY|RESULT|FAIL|SETTLED/);
      await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED/);
    }
    await execution;
    assert.equal(await liveProcess(pid), false);
    job = (await f.store.read()).executions[req.id]; assert.equal(job.status, "settled");
    assert.equal(job.result.disposition, aborted ? "FAIL" : "PASS");
    assert(job.result.durationMs >= (aborted ? 1000 : 1800));
    await assert.rejects(readFile(join(job.workspace, "file")), /ENOENT/);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    if (aborted) { f.run = await f.rt.reconcileCancellation(m(f.run), async () => {}); assert.equal(f.run.status, "cancelled"); }
  } finally {
    controller.abort(); if (pid && await liveProcess(pid)) try { process.kill(pid, "SIGKILL"); } catch {}
    await execution; if (req) await retainedCleanup(f, req); await f.cleanup(); await rm(probes, { recursive: true, force: true });
  }
});

test("timeout escalation survives argv leader close and kills TERM-resistant descendants", async () => {
  const root = await mkdtemp(join(tmpdir(), "n03-timeout-")), pidfile = join(root, "pid"); let pid;
  try {
    const descendant = `process.on('SIGTERM',()=>{});require('fs').writeFileSync(${JSON.stringify(pidfile)},String(process.pid));setInterval(()=>{},1000)`;
    const { runArgvV2 } = await import("../extensions/dag-workflow/runtime-v2/command-runner.ts");
    const execution = runArgvV2([process.execPath, "-e", `require('child_process').spawn(process.execPath,['-e',${JSON.stringify(descendant)}],{stdio:'ignore'}).unref()`], root, undefined, { timeoutMs: 500 });
    pid = Number(await waitForFile(pidfile)); const result = await execution;
    assert(result.settled); assert(result.interrupted); assert.equal(await liveProcess(pid), false);
    assert.match(result.stderr, /deadline expired/);
  } finally { if (pid && await liveProcess(pid)) try { process.kill(pid, "SIGKILL"); } catch {} await rm(root, { recursive: true, force: true }); }
});

test("uncertain settlement retains a BLOCKED journal and workspace, not a settled retryable outcome", async () => {
  const f = await fixture(); let req;
  try {
    await frame(f); req = await prepare(f, "static");
    const invoke = f.runner.invoke.bind(f.runner);
    // Fault injection after the real process settled models a lost ECHILD receipt.
    // The executor must not turn this into a lifecycle-settled BLOCKED result.
    f.runner.invoke = async (...args) => {
      const value = await invoke(...args);
      if (value && typeof value === "object" && "settled" in value) return { ...value, settled: false, stderr: "injected unavailable extinction receipt" };
      return value;
    };
    await assert.rejects(f.runner.ensure(req), /COMMAND_PROCESS_SETTLEMENT_REQUIRED/);
    const job = (await f.store.read()).executions[req.id]; assert.equal(job.status, "running"); assert.equal(job.result, undefined);
    const journal = JSON.parse(await readFile(join(resolve(job.workspace, ".."), "command-process.json"), "utf8"));
    assert.equal(journal.state, "BLOCKED"); assert.match(journal.diagnostic, /unavailable extinction receipt/);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "baseline\n");
    await f.runner.ensure(req); assert.equal(await f.runner.read(req), null);
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, f.runner), /NOT_DURABLE/);
    await assert.rejects(f.rt.retryCheck(m(f.run), "item", 1, req.id), /RETRY|RESULT|FAIL|SETTLED/);
    const receiptPath = join(resolve(job.workspace, ".."), `command-outcome-${journal.identity.token}.json`);
    const receiptBytes = await readFile(receiptPath, "utf8");
    for (const change of [r => r.token = "0".repeat(36), r => r.identity.processStart += "0", r => r.cwd += "-other", r => r.extinct = false]) {
      const receipt = JSON.parse(receiptBytes); change(receipt); await writeFile(receiptPath, JSON.stringify(receipt));
      await assert.rejects(f.runner.reconcileInterrupted(req, async () => {}), /INVALID_COMMAND_PROCESS_OUTCOME/);
      assert.equal(await f.runner.read(req), null);
    }
    await writeFile(receiptPath, receiptBytes);
    await f.runner.reconcileInterrupted(req, async () => {});
    assert.equal((await f.runner.read(req)).disposition, "BLOCKED");
  } finally { if (req) await retainedCleanup(f, req); await f.cleanup(); }
});

test("executor crash preserves a durable session identity; live descendants forbid reconciliation and retry", async () => {
  const root = await mkdtemp(join(tmpdir(), "n03-crash-")), pidfile = join(root, "pid");
  const descendant = `process.on('SIGTERM',()=>{});require('fs').writeFileSync(${JSON.stringify(pidfile)},String(process.pid));setInterval(()=>{},1000)`;
  const f = await fixture(l => l.checks[0].procedure.argv = [process.execPath, "-e", `require('child_process').spawn(process.execPath,['-e',${JSON.stringify(descendant)}],{stdio:'ignore'}).unref()`]);
  let pid, req, journal, owner, completion;
  try {
    await frame(f); req = await prepare(f, "static");
    const url = pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href;
    owner = spawn(process.execPath, ["--input-type=module", "-e", `import {CommandRunnerV2,StoreV2} from ${JSON.stringify(url)};await new CommandRunnerV2(new StoreV2(process.argv[1]),process.argv[2]).ensure(JSON.parse(process.argv[3]));`, f.root, f.repository, JSON.stringify(req)], { stdio: ["ignore", "pipe", "pipe"] });
    let errors = ""; owner.stderr.on("data", b => errors += b); owner.stdout.resume();
    completion = new Promise(resolve => owner.once("close", (code, signal) => resolve({ code, signal })));
    pid = Number(await waitForFile(pidfile));
    let job = (await f.store.read()).executions[req.id];
    journal = JSON.parse(await readFile(join(resolve(job.workspace, ".."), "command-process.json"), "utf8"));
    owner.kill("SIGKILL"); assert.equal((await completion).signal, "SIGKILL", errors);
    assert(await liveProcess(pid));
    const freshRunner = new CommandRunnerV2(new StoreV2(f.root), f.repository);
    await freshRunner.ensure(req); assert.equal(await freshRunner.read(req), null);
    await assert.rejects(freshRunner.reconcileInterrupted(req, async () => {}), /COMMAND_PROCESS_SETTLEMENT_REQUIRED/);
    await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, freshRunner), /NOT_DURABLE/);
    job = (await f.store.read()).executions[req.id]; assert.equal(job.status, "running");
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "baseline\n");
    const outcome = JSON.parse(await waitForFile(join(resolve(job.workspace, ".."), `command-outcome-${journal.identity.token}.json`)));
    assert.equal(outcome.extinct, true); assert.equal(outcome.exitCode, 0); assert.equal(outcome.interrupted, true);
    assert.equal(await liveProcess(pid), false);
    await freshRunner.reconcileInterrupted(req, async () => {});
    assert.equal((await freshRunner.read(req)).disposition, "BLOCKED");
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, freshRunner);
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id); assert.equal(f.run.nodes.item.retries[0].dimension, "infrastructure");
  } finally {
    owner?.kill("SIGKILL"); if (journal) try { process.kill(-journal.identity.pid, "SIGKILL"); } catch {}
    if (pid && await liveProcess(pid)) try { process.kill(pid, "SIGKILL"); } catch {}
    await completion; if (req) await retainedCleanup(f, req); await f.cleanup(); await rm(root, { recursive: true, force: true });
  }
});

const handoffArgv = root => ["python3", "-I", "-B", fileURLToPath(new URL("./fixtures/command-fork-handoff.py", import.meta.url)), root];
const outcomeFile = job => join(resolve(job.workspace, ".."), `command-outcome-${job.journal.identity.token}.json`);
async function commandJob(f, req) {
  const job = (await f.store.read()).executions[req.id];
  return { ...job, journal: JSON.parse(await readFile(join(resolve(job.workspace, ".."), "command-process.json"), "utf8")) };
}
async function denyUnsettled(f, req) {
  const job = (await f.store.read()).executions[req.id];
  assert.equal(job.status, "running"); assert.equal(job.result, undefined);
  assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "baseline\n");
  await new CommandRunnerV2(new StoreV2(f.root), f.repository).ensure(req);
  assert.equal(await f.runner.read(req), null);
  await assert.rejects(f.rt.recordResult(m(f.run), "item", req.id, f.runner), /NOT_DURABLE/);
  if (f.run.status === "active") {
    await assert.rejects(f.rt.retryCheck(m(f.run), "item", 1, req.id), /RETRY|RESULT|FAIL|SETTLED/);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED/);
  } else await assert.rejects(f.rt.reconcileCancellation(m(f.run), async () => {}), /RECONCILIATION_REQUIRED/);
}
for (const aborted of [false, true]) test(`finite fork/exit handoff requires kernel extinction before durable result (${aborted ? "cancelled" : "normal"})`, async () => {
  const probes = await mkdtemp(join(tmpdir(), "n03-handoff-"));
  const f = await fixture(l => l.checks[0].procedure.argv = handoffArgv(probes));
  const controller = new AbortController(); let req, execution, group;
  try {
    await frame(f); req = await prepare(f, "static"); execution = f.runner.ensure(req, controller.signal);
    ({ group } = JSON.parse(await waitForFile(join(probes, "first"))));
    await waitForFile(join(probes, "middle"));
    if (aborted) { f.run = await busyRetry(() => f.rt.cancel(m(f.run))); controller.abort(); }
    await denyUnsettled(f, req);
    const pid = Number(await waitForFile(join(probes, "last"))); assert(await liveProcess(pid));
    await denyUnsettled(f, req);
    if (!aborted) await writeFile(join(probes, "release"), "release");
    await execution;
    const job = (await f.store.read()).executions[req.id];
    assert.equal(await liveProcess(pid), false); assert.equal(job.status, "settled");
    assert.equal(job.result.disposition, aborted ? "FAIL" : "PASS");
    assert.equal(job.result.exitCode, 0, "retain actual command exit separately from descendant extinction/cancellation");
    assert.equal(job.result.signal, null); assert(job.result.durationMs >= 1200);
    await assert.rejects(readFile(join(job.workspace, "file")), /ENOENT/);
    if (aborted) await assert.rejects(readFile(join(probes, "done")), /ENOENT/);
    else assert.equal(Number(await readFile(join(probes, "done"), "utf8")), pid);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    if (aborted) { f.run = await f.rt.reconcileCancellation(m(f.run), async () => {}); assert.equal(f.run.status, "cancelled"); }
    else { f.run = await f.rt.advanceLifecycle(m(f.run), "item", 1, 1); assert.equal(f.run.nodes.item.lifecycle.stage, 2); }
  } finally {
    controller.abort(); if (group) try { process.kill(-group, "SIGKILL"); } catch {}
    await execution; if (req) await retainedCleanup(f, req); await f.cleanup(); await rm(probes, { recursive: true, force: true });
  }
});

for (const lost of ["owner", "outcome acknowledgement", "supervisor"]) test(`finite handoff crash reconciliation: lost ${lost}`, async () => {
  const probes = await mkdtemp(join(tmpdir(), "n03-handoff-crash-"));
  const f = await fixture(l => l.checks[0].procedure.argv = handoffArgv(probes));
  let req, owner, ownerPid, completion, job, group;
  const killOwner = signal => { if (ownerPid) try { process.kill(ownerPid, signal); } catch (e) { if (e.code !== "ESRCH") throw e; } };
  const ownerExit = async () => JSON.parse(await waitForFile(join(probes, "owner-exit")));
  try {
    await frame(f); req = await prepare(f, "static");
    const url = new URL("../extensions/dag-workflow/runtime-v2/index.ts", import.meta.url).href;
    owner = spawn("python3", ["-I", "-B", fileURLToPath(new URL("./fixtures/command-owner-subreaper.py", import.meta.url)), probes,
      process.execPath, "--input-type=module", "-e", `import {CommandRunnerV2,StoreV2} from ${JSON.stringify(url)};await new CommandRunnerV2(new StoreV2(process.argv[1]),process.argv[2]).ensure(JSON.parse(process.argv[3]));`, f.root, f.repository, JSON.stringify(req)], { stdio: ["ignore", "pipe", "pipe"] });
    let errors = ""; owner.stderr.on("data", b => errors += b); owner.stdout.resume();
    completion = new Promise(resolve => owner.once("close", (code, signal) => resolve({ code, signal })));
    ownerPid = Number(await waitForFile(join(probes, "owner-pid")));
    ({ group } = JSON.parse(await waitForFile(join(probes, "first"))));
    job = await commandJob(f, req);
    assert.equal(job.journal.version, 2);
    if (lost === "outcome acknowledgement") {
      // The owner cannot consume the helper exit or publish/clean while stopped.
      killOwner("SIGSTOP");
      const pid = Number(await waitForFile(join(probes, "last"))); assert(await liveProcess(pid));
      await denyUnsettled(f, req);
      await writeFile(join(probes, "release"), "release");
      const receipt = JSON.parse(await waitForFile(outcomeFile(job)));
      assert.equal(receipt.exitCode, 0); assert.equal(receipt.extinct, true); assert.equal(receipt.interrupted, false);
      assert.equal(await liveProcess(pid), false);
      killOwner("SIGKILL"); assert.equal((await ownerExit()).signal, "SIGKILL", errors);
    } else if (lost === "owner") {
      await waitForFile(join(probes, "middle")); // Crash during ordinary churn.
      killOwner("SIGKILL"); assert.equal((await ownerExit()).signal, "SIGKILL", errors);
      await denyUnsettled(f, req);
      await assert.rejects(f.runner.reconcileInterrupted(req, async () => {}), /COMMAND_PROCESS_SETTLEMENT_REQUIRED/);
      const receipt = JSON.parse(await waitForFile(outcomeFile(job)));
      assert.equal(receipt.exitCode, 0); assert.equal(receipt.extinct, true); assert.equal(receipt.interrupted, true);
      const pid = Number(await waitForFile(join(probes, "last"))); assert.equal(await liveProcess(pid), false);
    } else {
      process.kill(job.journal.identity.pid, "SIGKILL");
      assert.notEqual((await ownerExit()).code, 0, errors);
      const pid = Number(await waitForFile(join(probes, "last"))); assert(await liveProcess(pid));
      await denyUnsettled(f, req);
      await assert.rejects(readFile(outcomeFile(job)), /ENOENT/);
      await assert.rejects(f.runner.reconcileInterrupted(req, async () => { throw Error("independent descendant settlement unavailable"); }), /independent descendant/);
      // Even after the last descendant stops, missing receipt is NOT proof.
      await writeFile(join(probes, "release"), "release"); await waitForFile(join(probes, "done"));
      for (let i = 0; i < 500 && await liveProcess(pid); i++) await delay(10);
      assert.equal(await liveProcess(pid), false);
      await assert.rejects(f.runner.reconcileInterrupted(req, async () => { throw Error("independent effect settlement still required"); }), /independent effect/);
      await denyUnsettled(f, req);
    }
    let independentlySettled = 0;
    await f.runner.reconcileInterrupted(req, async () => {
      // The outer test reaper independently waits to ECHILD, including when the
      // production supervisor was killed. No negative /proc scan is the proof.
      assert.equal((await completion).code, 0, errors); independentlySettled++;
    });
    assert.equal(independentlySettled, 1);
    const recovered = await f.runner.read(req); assert.equal(recovered.disposition, "BLOCKED");
    if (lost !== "supervisor") { assert.equal(recovered.exitCode, 0); assert.equal(recovered.executor.invoked, true); }
    else assert.equal(recovered.exitCode, null);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "baseline\n", "reconciliation does not auto-clean or invent PASS");
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    await assert.rejects(f.rt.advanceLifecycle(m(f.run), "item", 1, 1), /CHECK_NOT_PASSED|UNRESOLVED_FINDINGS/);
    f.run = await f.rt.retryCheck(m(f.run), "item", 1, req.id);
    assert.equal(f.run.nodes.item.retries[0].dimension, "infrastructure");
  } finally {
    killOwner("SIGKILL"); if (group) try { process.kill(-group, "SIGKILL"); } catch {}
    await completion; if (req) await retainedCleanup(f, req); await f.cleanup(); await rm(probes, { recursive: true, force: true });
  }
});

test("current-intent fencing at the subreaper gate prevents actual command launch", async () => {
  const probes = await mkdtemp(join(tmpdir(), "n03-gate-")), marker = join(probes, "invoked");
  const f = await fixture(l => l.checks[0].procedure.argv = [process.execPath, "-e", `require('fs').writeFileSync(${JSON.stringify(marker)},'unsafe')`]);
  let release, reached; const gated = new Promise(resolve => reached = resolve), resume = new Promise(resolve => release = resolve);
  let execution, req;
  try {
    await frame(f); req = await prepare(f, "static");
    const invoke = f.runner.invoke.bind(f.runner);
    f.runner.invoke = async (...args) => { if (args[3]) { reached(); await resume; } return invoke(...args); };
    execution = f.runner.ensure(req); await gated;
    f.run = await busyRetry(() => f.rt.cancel(m(f.run))); release(); await execution;
    const result = await f.runner.read(req);
    assert.equal(result.executor.invoked, false); assert.equal(result.disposition, "BLOCKED");
    assert.match(result.diagnostic, /FENCED_BEFORE_INVOCATION/);
    await assert.rejects(readFile(marker), /ENOENT/);
    f.run = await f.rt.recordResult(m(f.run), "item", req.id, f.runner);
    assert.equal(f.run.nodes.item.lifecycle.executions.at(-1).status, "quarantined");
  } finally { release(); await execution; if (req) await retainedCleanup(f, req); await f.cleanup(); await rm(probes, { recursive: true, force: true }); }
});

test("missing Python capability fails closed before argv launch", async () => {
  const { runArgvV2 } = await import("../extensions/dag-workflow/runtime-v2/command-runner.ts");
  const root = await mkdtemp(join(tmpdir(), "n03-no-python-")), previous = process.env.PATH;
  try {
    process.env.PATH = root;
    const result = await runArgvV2([process.execPath, "-e", "throw Error('must not launch')"], root);
    assert.equal(result.invoked, false); assert.equal(result.settled, true); assert.equal(result.exitCode, null);
    assert.match(result.stderr, /ENOENT/);
  } finally { process.env.PATH = previous; await rm(root, { recursive: true, force: true }); }
});

let failed = 0;
for (const [name, fn] of tests) { const at = performance.now(); try { await fn(); console.log(`PASS ${name} (${((performance.now() - at) / 1000).toFixed(2)}s)`); } catch (error) { failed++; console.error(`FAIL ${name}`, error); } }
console.log(`${tests.length - failed}/${tests.length} passed`); process.exitCode = failed ? 1 : 0;
