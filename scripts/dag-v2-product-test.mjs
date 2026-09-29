import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { WORKER_ACTIVITY_EVENT, WORKER_ACTIVITY_REQUEST_EVENT } from "../extensions/dag-workflow/worker-runtime/activity.mjs";
import { FocusSessionStore } from "../extensions/dag-workflow/project-model/sessions.ts";
import { SpecProjector } from "../extensions/dag-workflow/project-model/projector.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { selectorV2 } from "../extensions/dag-workflow/planning/v2.ts";
import { PlanningFreshnessV2 } from "../extensions/dag-workflow/planning/freshness-v2.ts";
import { ProductWidgetV2 } from "../extensions/dag-workflow/runtime-v2/widget.ts";
import { gitEnvironmentV2 } from "../extensions/dag-workflow/runtime-v2/command-runner.ts";
import { planFixture as historicalPlanFixture, runFixture as historicalRunFixture } from "./dag-dogfood-test.mjs";
import { DagRunSnapshotStoreV1 } from "../extensions/dag-workflow/dag-runtime/store.ts";
import { DagPlanningStoreV1 } from "../extensions/dag-workflow/planning/store.ts";
import { createDagPlanningPlanV1 } from "../extensions/dag-workflow/planning/artifact.ts";
import { canonicalHash, canonicalStringify } from "../extensions/dag-workflow/dag-runtime/common.ts";

const tests = [], test = (name, run) => tests.push([name, run]);
const AT = "2026-01-01T00:00:00.000Z", source = "model:decisions/DEC-delivery";
class Pi {
  bus = new EventEmitter();
  events = {
    emit: (name, data) => { this.bus.emit(name, data); },
    on: (name, listener) => {
      const handler = (data) => listener(data);
      this.bus.on(name, handler);
      return () => { this.bus.off(name, handler); };
    },
  };
  tools = new Map(); commands = new Map(); handlers = new Map(); entries = []; messages = []; active = ["read", "bash", "write", "edit"];
  registerTool(t) { assert(!this.tools.has(t.name), `duplicate tool ${t.name}`); this.tools.set(t.name, t); this.active.push(t.name); }
  registerCommand(n, c) { this.commands.set(n, c); }
  on(n, fn) { this.handlers.set(n, [...(this.handlers.get(n) ?? []), fn]); }
  getActiveTools() { return this.active; } setActiveTools(names) { this.active = names; }
  getAllTools() { return [...this.tools.values()]; }
  appendEntry(customType, data) { this.entries.push({ type: "custom", customType, data }); }
  sendMessage(message, options) { this.messages.push({ message, options }); }
  async emit(name, ctx) { for (const fn of this.handlers.get(name) ?? []) await fn({ systemPrompt: "fixture" }, ctx); }
}
async function fixture(name, options = {}) {
  const root = await mkdtemp(join(tmpdir(), `dag-v2-product-${name}-`));
  const git = (...args) => execFileSync("git", args, { cwd: root, env: gitEnvironmentV2(), encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  git("init", "-b", "main"); git("config", "user.name", "Product fixture"); git("config", "user.email", "product@example.invalid");
  const decision = { id: "DEC-delivery", title: "Deliver verified local files", body: "Each item writes its named file containing its own ID. Causal items require the previous file. Architecture is one local file per item; never publish.", state: "accepted", scope: { kind: "repository" }, introducedBy: "user", sourceRefs: ["fixture"], relationships: [], createdAt: AT, updatedAt: AT, rationale: "Independent deterministic oracle observes actual committed bytes." };
  decision.acceptance = { mode: "direct_direction", actor: "user", acceptedAt: AT, contentHash: semanticHash("decisions", decision), interactionRef: "fixture-user-direction" };
  const model = { schemaVersion: 1, project: { id: "product-fixture", title: "Product", revision: 1, mode: "authoritative", createdAt: AT, updatedAt: AT,
    projections: { specs: [{ id: "SPEC-delivery", kind: "spec", path: "spec/delivery/spec.md", title: "Delivery", sections: [{ id: "direction", title: "Direction", objectIds: [decision.id] }] }] } },
    workstreams: [], intents: [], concepts: [], evidence: [], assumptions: [], questions: [], tensions: [], scenarios: [], proposals: [], decisions: [decision], commitments: [], discoveries: [] };
  await mkdir(join(root, "project-model")); await mkdir(join(root, "spec/delivery"), { recursive: true });
  await writeFile(join(root, "project-model/model.json"), JSON.stringify(model));
  for (const p of new SpecProjector(root).render(model)) await writeFile(join(root, p.path), p.content);
  await writeFile(join(root, ".gitignore"), ".ai/\n");
  await writeFile(join(root, "implement.mjs"), `import {writeFileSync,existsSync,readFileSync} from 'node:fs'; import {execFileSync} from 'node:child_process'; const id=process.argv[2]; if(id==='broken')process.exit(47); if(id==='b'&&!existsSync('a.txt'))process.exit(41); if(id==='c'&&!existsSync('b.txt'))process.exit(42); writeFileSync(id+'.txt',id==='repairable'&&process.argv[3]!=='repair'?'wrong\\n':id+'\\n'); if(id==='model-change'){const path='project-model/model.json',model=JSON.parse(readFileSync(path));model.decisions[0].body+=' unauthorized';writeFileSync(path,JSON.stringify(model));execFileSync('git',['add',path]);} execFileSync('git',['add',id+'.txt']); try{execFileSync('git',['diff','--cached','--quiet']);}catch(error){if(error.status!==1)throw error;execFileSync('git',['-c','user.name=fixture','-c','user.email=fixture@example.invalid','-c','core.hooksPath=/dev/null','commit','-m','implement '+id]);}`);
  await writeFile(join(root, "verify.mjs"), `import assert from 'node:assert/strict'; import {readFileSync,existsSync,readdirSync} from 'node:fs'; const id=process.argv[2]; if(id==='combined'){assert(existsSync('a.txt')); for(const f of readdirSync('.').filter(f=>/^[abc]\\.txt$/.test(f)))assert.equal(readFileSync(f,'utf8'),f[0]+'\\n');}else {assert.equal(readFileSync(id+'.txt','utf8'),id+'\\n'); if(id==='b')assert.equal(readFileSync('a.txt','utf8'),'a\\n'); if(id==='c')assert.equal(readFileSync('b.txt','utf8'),'b\\n');} console.log('observed exact bytes',id,process.argv[3]);`);
  git("add", "."); git("commit", "-m", "test: native product baseline");
  await new FocusSessionStore(root).create({ id: "focus-product", title: "Product", workstreamIds: [] });
  const entries = [{ type: "custom", customType: "dag-model-focus-link", data: { repositoryRoot: root, focusSessionId: "focus-product", mode: "active" } }];
  const widgets = [], sessionId = `session-${name}`;
  let pi, ctx, handles;
  const load = async () => {
    pi = new Pi(); pi.entries = entries;
    ctx = { cwd: root, hasUI: options.tui ?? false, mode: options.tui ? "tui" : "print", model: { id: "deterministic", provider: "local" }, thinkingLevel: "off", ui: { notify(text, type) { if (type === "error") throw Error(text); }, setWidget(name, factory) { widgets.push({ name, factory }); } },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null, getHeader: () => ({ id: sessionId, cwd: root }), getBranch: () => entries, getEntries: () => entries } };
    const role = process.env.PI_DAG_WORKER_ROLE;
    try { delete process.env.PI_DAG_WORKER_ROLE; handles = dagWorkflow(pi, { workerRuntime: { piCliPath: resolve("scripts/fixtures/product-worker-rpc.mjs"), watchIntervalMs: 20, ...options.workerRuntime } }); }
    finally { if (role !== undefined) process.env.PI_DAG_WORKER_ROLE = role; }
    await pi.emit("session_start", ctx);
  };
  await load();
  return { root, git, model, widgets, get pi() { return pi; }, get ctx() { return ctx; }, get handles() { return handles; },
    service: () => handles.planningIntegration.product(ctx),
    async call(name, params, signal) { const t = pi.tools.get(name); assert(t, `missing ${name}`); return (await t.execute("fixture", params, signal, undefined, ctx)).details; },
    async command(text) { await pi.commands.get("dag").handler(text, ctx); },
    async reload() { await pi.emit("session_shutdown", ctx); assert.equal(pi.bus.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 0); await load(); },
    async cleanup() { try { await pi.emit("session_shutdown", ctx); assert.equal(pi.bus.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 0); } finally { if (process.env.DAG_V2_PRODUCT_KEEP) console.log(`Retained fixture ${root}`); else await rm(root, { recursive: true, force: true }); } } };
}
function planInput(planId = "delivery", ids = ["a", "b"]) {
  return { planId, expectedPlanRevision: 0, title: planId, sourceRefs: [source, "spec:spec/delivery/spec.md"], scopeSummary: "Repository-wide accepted product delivery",
    architecture: { outcomes: [{ id: "out", description: "Verified causal local files" }], nonGoals: ["Publication"], notes: ["One file per item"], risks: ["Do not bypass independent checks"] },
    workItems: ids.map((id, index) => ({ id, title: `Implement ${id}`, objective: `Commit ${id}.txt containing ${id} and newline`, outcomeIds: ["out"], context: [], checks: ["Observe exact bytes"], dependsOn: index ? [ids[index - 1]] : [], risk: "low", riskNotes: [], resources: {}, gates: [],
      lifecycle: { oracle: { statement: "Committed named file has the exact ID and newline; dependencies exist", sourceRefs: [source], checkIds: ["f2"] },
        checks: Array.from({ length: 7 }, (_, j) => ({ id: `f${j + 1}`, stage: j + 1, expectation: j === 4 ? "Independent architecture invariant: one correct file per item and causal prefix" : "Observe exact committed bytes and causal dependencies", sourceRefs: [source], applicability: { kind: "required" }, procedure: { kind: "command", argv: [process.execPath, "verify.mjs", id, `F${j + 1}`] }, environment: "node-local", replay: "pure" })) } })),
    constraints: { maxConcurrency: 2, resources: {}, mutexGroups: [], gates: [] }, integration: { strategy: "serial", checks: ["Real combined prefix"], finalChecks: ["Real combined final"], prefixCommands: [{ id: "prefix", argv: [process.execPath, "verify.mjs", "combined", "prefix"] }], finalCommands: [{ id: "final", argv: [process.execPath, "verify.mjs", "combined", "final"] }] } };
}
const authority = scope => ({ scope, maxConcurrency: 1, effects: ["repository_local"], expiresAt: Date.now() + 3600000 });
async function waitTerminal(f, runId, itemId) {
  const end = Date.now() + 30000;
  while (Date.now() < end) {
    const { run } = await f.service().read(runId), binding = run.nodes[itemId].reservation?.binding;
    if (binding) { try { const terminal = await f.handles.workerManager.terminalResultForBinding(binding); if (terminal) return terminal; } catch (error) { if (!String(error).includes("hard-link alias")) throw error; } }
    await delay(30);
  }
  throw Error("fixture worker did not settle");
}
async function finish(f, runId, itemId, generation = 1, land = true) {
  await f.call("dag_start_work", { runId, itemId, generation });
  const terminal = await waitTerminal(f, runId, itemId); assert.equal(terminal.terminalStatus, "succeeded");
  let run = await f.call("dag_record_completion", { runId, itemId, generation, completionId: terminal.completionId });
  for (let stage = 1; stage <= 8; stage++) {
    const action = (await f.call("dag_next_action", { runId })).actions.find(a => a.itemId === itemId && a.tool === "dag_run_checks"); assert(action, `missing F${stage}`);
    const { tool, ...params } = action; run = await f.call(tool, params);
  }
  assert.deepEqual(run.nodes[itemId].lifecycle.passed, [0, 1, 2, 3, 4, 5, 6, 7, 8]);
  const results = run.nodes[itemId].lifecycle.executions.filter(e => e.status === "observed" && e.request.generation === generation).map(e => e.result);
  assert.equal(results.length, 13); assert(results.every(r => r.disposition === "PASS" && r.executor.invoked && r.exitCode === 0 && r.stdout.includes("observed exact bytes")));
  assert.equal(new Set(results.filter(r => [2, 5, 7].includes(r.request.stage)).map(r => r.executor.contextId)).size, 9);
  if (!land) return run;
  const action = (await f.call("dag_next_action", { runId })).actions.find(a => a.tool === "dag_integrate" && a.itemId === itemId);
  const { tool, ...params } = action; return f.call(tool, params);
}
async function tree(path) {
  const result = {};
  async function visit(dir, prefix = "") { for (const e of await readdir(dir, { withFileTypes: true }).catch(e => e.code === "ENOENT" ? [] : Promise.reject(e))) { const key = `${prefix}${e.name}`; if (e.isDirectory()) { result[`${key}/`] = "directory"; await visit(join(dir, e.name), `${key}/`); } else result[key] = (await readFile(join(dir, e.name))).toString("base64"); } }
  await visit(path); return result;
}

function loseCommandResultPublication(root, request) {
      const code = `import {StoreV2,CommandRunnerV2} from ${JSON.stringify(pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href)};
        import {gitOptionsV2} from ${JSON.stringify(pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/git-native.ts")).href)};
        const root=process.argv[1], request=JSON.parse(process.argv[2]), store=new StoreV2(root), transaction=store.transaction.bind(store);
        store.transaction=update=>transaction((s,publish)=>update(s,async()=>{if(s.executions?.[request.id]?.result){process.stdout.write('actual-result-publication-boundary\\n');process.kill(process.pid,'SIGKILL');}await publish();}));
        await new CommandRunnerV2(store,root,new Map(),'node-local',gitOptionsV2).ensure(request);`;
      const result = spawnSync(process.execPath, ["--input-type=module", "-e", code, root, JSON.stringify(request)], { encoding: "utf8", timeout: 120000 });
      assert.equal(result.signal, "SIGKILL", result.stderr); assert.match(result.stdout, /actual-result-publication-boundary/);
}

test("registered worker activity bus replays snapshots and removes retired session listeners", async () => {
  const f = await fixture("activity-bus", { tui: true });
  const retired = f.pi, snapshots = [];
  const unsubscribe = retired.events.on(WORKER_ACTIVITY_EVENT, (snapshot) => snapshots.push(snapshot));
  const request = { schemaVersion: 1, ownerSessionId: f.ctx.sessionManager.getSessionId(), requestId: "product-replay" };
  try {
    assert.equal(retired.bus.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 1);
    retired.events.emit(WORKER_ACTIVITY_REQUEST_EVENT, request);
    assert.equal(snapshots.at(-1).requestId, request.requestId);
    assert.equal(snapshots.at(-1).phase, "attached");
    await f.reload();
    assert.equal(snapshots.at(-1).phase, "detached");
    const count = snapshots.length;
    retired.events.emit(WORKER_ACTIVITY_REQUEST_EVENT, request);
    assert.equal(snapshots.length, count, "retired producer must not answer requests");
    assert.equal(f.pi.bus.listenerCount(WORKER_ACTIVITY_REQUEST_EVENT), 1);
    unsubscribe(); unsubscribe();
    retired.events.emit(WORKER_ACTIVITY_EVENT, { phase: "test" });
    assert.equal(snapshots.length, count, "consumer unsubscribe is effective and idempotent");
  } finally { unsubscribe(); await f.cleanup(); }
});

test("registered V2 writer: inert save/show/revise, exact explicit run JSON, no approval or action-ID prerequisite", async () => {
  const f = await fixture("explicit"); try {
    assert(!f.pi.tools.has("dag_plan_decide"));
    for (const [name, tool] of f.pi.tools) if (name.startsWith("dag_") && !name.startsWith("dag_model")) assert(!JSON.stringify(tool.parameters).includes('"actionId"'), name);
    await f.command("plan --new exact product goal"); assert.match(f.pi.messages.at(-1).message.content, /Goal: exact product goal/);
    let plan = await f.call("dag_plan_save", planInput());
    assert.equal(plan.schemaVersion, 2); assert(!("approval" in plan)); assert(!("authorization" in plan));
    assert.equal(Object.keys((await f.service().runtime.store.read()).runs).length, 0);
    const before = await tree(join(f.root, ".ai/dag-workflow-v2")); await f.command("show --plan delivery@1");
    await f.command("show --plan delivery@1 --node a"); assert.equal(JSON.parse(f.pi.messages.at(-1).message.content).id, "a");
    await assert.rejects(f.command("show --version 1"), /UNSUPPORTED_DAG_OPTION/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    plan = await f.call("dag_plan_save", { ...planInput(), expectedPlanRevision: 1, title: "Revised" }); assert.equal(plan.revision, 2);
    assert.equal(Object.keys((await f.service().runtime.store.read()).runs).length, 0);
    const payload = { selection: selectorV2(plan), authority: authority(["a", "b"]) };
    await f.command(`run ${JSON.stringify(payload)}`); const run = (await f.service().read()).run;
    assert(run); assert.equal(run.start.selection.revision, 2); assert.equal(run.nodes.a.status, "pending");
    const saved = await tree(join(f.root, ".ai/dag-workflow-v2")); await f.call("dag_next_action", {}); await f.call("dag_run_status", {}); assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), saved);
    await f.call("dag_pause", { runId: run.runId }); await f.call("dag_run_start", payload); assert.equal((await f.service().read()).run.status, "paused");
    await assert.rejects(f.call("dag_run_start", { ...payload, authority: { ...payload.authority, maxConcurrency: 2 } }), /BOUND_AUTHORITY_CONFLICT/);
    assert.equal(f.widgets.length, 0, "headless must not mount a widget");
  } finally { await f.cleanup(); }
});
test("two causal nodes cross actual WorkerManager/results, F0-F8 commands and native Git with persisted service recovery", async () => {
  const f = await fixture("two-nodes"); try {
    const plan = await f.call("dag_plan_save", planInput()), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a", "b"]) });
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "b", generation: 1 }), /ITEM_NOT_ADMISSIBLE/);
    let current = await finish(f, run.runId, "a"); assert.equal(current.status, "active"); assert.equal(current.nodes.b.status, "pending");
    const first = current.gitOperations[0]; assert.equal(first.phase, "accepted"); assert.equal(first.binding.gitVersion, "git version 2.54.0");
    await f.reload(); assert((await f.call("dag_next_action", {})).actions.some(a => a.itemId === "b"));
    current = await finish(f, run.runId, "b"); assert.equal(current.status, "complete");
    assert.equal(await readFile(join(f.root, "a.txt"), "utf8"), "a\n"); assert.equal(await readFile(join(f.root, "b.txt"), "utf8"), "b\n");
    assert.equal(current.gitOperations.length, 2); assert.equal(current.gitOperations[1].expected.commit, first.proposal.commit);
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 2);
    const state = await f.service().runtime.store.read(); assert(current.gitOperations.every(op => op.checks.every(c => state.executions[c.id].result.disposition === "PASS")));
  } finally { await f.cleanup(); }
});
test("Fractory terminal successor rebinds exact predecessor and remounts passive widget without changing predecessor", async () => {
  const f = await fixture("fractory", { tui: true }); try {
    const plan = await f.call("dag_plan_save", planInput("fractory", ["a"]));
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) }); await finish(f, run.runId, "a");
    const before = JSON.stringify((await f.service().read(run.runId)).run), widgetCount = f.widgets.length;
    const input = planInput("fractory-successor", ["b"]); input.predecessor = selectorV2(plan);
    const successor = await f.call("dag_plan_save", input), next = await f.call("dag_run_start", { selection: selectorV2(successor), authority: authority(["b"]) });
    assert.equal(next.predecessorRunId, run.runId); assert.equal(JSON.stringify((await f.service().read(run.runId)).run), before); assert(f.widgets.length > widgetCount);
    assert.equal((await finish(f, next.runId, "b")).status, "complete");
    assert.equal(JSON.stringify((await f.service().read(run.runId)).run), before);
  } finally { await f.cleanup(); }
});
test("Operant closed safe prefix excludes remainder, recovers between landings and denies restricted effects", async () => {
  const f = await fixture("operant"); try {
    const plan = await f.call("dag_plan_save", planInput("operant", ["a", "b", "c"]));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["b"]) }), /SCOPE_NOT_DEPENDENCY_CLOSED/);
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: { ...authority(["a", "b"]), effects: ["publish"] } }), /INVALID_V2/);
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a", "b"]) });
    assert.equal(run.nodes.c.status, "excluded"); await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "c", generation: 1 }), /ITEM_NOT_ADMISSIBLE/);
    await finish(f, run.runId, "a"); await f.reload(); const current = await finish(f, run.runId, "b");
    assert.equal(current.status, "complete"); assert.equal(current.nodes.c.status, "excluded"); assert.equal(current.nodes.c.reservation, undefined);
    await assert.rejects(readFile(join(f.root, "c.txt")), { code: "ENOENT" }); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 2);
  } finally { await f.cleanup(); }
});
test("exact current revision, focus/session and ambiguous selection never fall through to a latest plan", async () => {
  const f = await fixture("selection"); try {
    const one = await f.call("dag_plan_save", planInput("one", ["a"])), two = await f.call("dag_plan_save", planInput("two", ["a"]));
    for (let i = f.pi.entries.length - 1; i >= 0; i--) if (f.pi.entries[i].customType === "dag-planning-session-binding-v2") f.pi.entries.splice(i, 1);
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.command("show"), /PLAN_SELECTION_REQUIRED/); await assert.rejects(f.command("run"), /RUN_PAYLOAD_REQUIRED/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    await f.call("dag_plan_save", { ...planInput("one", ["a"]), expectedPlanRevision: 1, title: "new current content" });
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(one), authority: authority(["a"]) }), /PLAN_SELECTION_STALE_OR_MISSING/);
    const run = await f.call("dag_run_start", { selection: selectorV2(two), authority: authority(["a"]) });
    const otherContext = { ...f.ctx, sessionManager: { ...f.ctx.sessionManager, getSessionId: () => "other-session" } };
    await assert.rejects(f.pi.tools.get("dag_start_work").execute("test", { runId: run.runId, itemId: "a", generation: 1 }, undefined, undefined, otherContext), /EXACT_SESSION_RUN_REQUIRED/);
    await new FocusSessionStore(f.root).create({ id: "focus-other", title: "Other", workstreamIds: [] });
    f.pi.entries.push({ type: "custom", customType: "dag-model-focus-link", data: { repositoryRoot: f.root, focusSessionId: "focus-other", mode: "active" } }); await f.reload();
    const frozen = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(two), authority: run.start.authority }), /PLAN_FOCUS_MISMATCH/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), frozen);
  } finally { await f.cleanup(); }
});
test("serial non-prefix scope and stale stage selectors cannot dispatch excluded or replacement work", async () => {
  const f = await fixture("nonprefix"); try {
    const input = planInput("nonprefix", ["a", "b", "c"]); for (const item of input.workItems) item.dependsOn = [];
    const plan = await f.call("dag_plan_save", input), before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a", "c"]) }), /SCOPE_NOT_INTEGRATION_CLOSED/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await assert.rejects(f.call("dag_run_checks", { runId: run.runId, itemId: "b", generation: 1, stageAttemptId: `${run.runId}/b/1/F1/1` }), /STALE_STAGE_ATTEMPT/);
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 2 }), /STALE_GENERATION/);
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
  } finally { await f.cleanup(); }
});
test("independent frozen governing closure includes newly applicable accepted objects and rejects forged sources", async () => {
  const f = await fixture("freshness"); try {
    const input = planInput(); await assert.rejects(f.call("dag_plan_save", { ...input, source: { governingClosure: "forged" } }), /INVALID_V2/);
    const plan = await f.call("dag_plan_save", input), before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    const next = structuredClone(f.model.decisions[0]); next.id = "DEC-new"; next.title = "New governing constraint";
    next.acceptance.contentHash = semanticHash("decisions", next); f.model.decisions.push(next);
    f.model.project.projections.specs[0].sections[0].objectIds.push(next.id);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "new applicable authority");
    const observed = await new PlanningFreshnessV2(f.root).current(plan); assert.notEqual(observed.source.governingClosure, plan.source.governingClosure); assert(observed.source.refs.some(r => r.ref === "model:decisions/DEC-new"));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a", "b"]) }), /PLAN_STALE_SOURCE_OR_BASELINE/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
  } finally { await f.cleanup(); }
});
test("frozen governing selector recomputes transitive accepted links without promoting unrelated context", async () => {
  const f = await fixture("closure-links"); try {
    f.model.workstreams.push({ id: "WS-other", title: "Other", body: "Other scope", state: "active", scope: { kind: "repository" }, introducedBy: "user", sourceRefs: [], relationships: [], createdAt: AT, updatedAt: AT });
    for (const [id, kind, targetId] of [["DEC-linked", "affects", "DEC-delivery"], ["DEC-transitive", "depends_on", "DEC-linked"], ["DEC-context", "related_to", "DEC-delivery"]]) {
      const decision = { ...structuredClone(f.model.decisions[0]), id, title: id, scope: { kind: "workstreams", workstreamIds: ["WS-other"] }, relationships: [{ kind, targetId }] };
      decision.acceptance.contentHash = semanticHash("decisions", decision); f.model.decisions.push(decision); f.model.project.projections.specs[0].sections[0].objectIds.push(id);
    }
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "accepted transitive governing links");
    const plan = await f.call("dag_plan_save", planInput("closure-links", ["a"])); assert.deepEqual(plan.source.selector.workstreamIds, []);
    assert(plan.source.refs.some(r => r.ref === "model:decisions/DEC-linked")); assert(plan.source.refs.some(r => r.ref === "model:decisions/DEC-transitive")); assert(!plan.source.refs.some(r => r.ref === "model:decisions/DEC-context"));
    const changed = f.model.decisions.find(d => d.id === "DEC-transitive"); changed.body = "New applicable accepted transitive constraint"; changed.acceptance.contentHash = semanticHash("decisions", changed);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "change transitive governing content");
    const current = await new PlanningFreshnessV2(f.root).current(plan); assert.notEqual(current.source.governingClosure, plan.source.governingClosure);
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) }), /PLAN_STALE_SOURCE_OR_BASELINE/);
  } finally { await f.cleanup(); }
});
test("worker reservation acknowledgement loss and completion delivery loss reuse exact durable attempt after reload", async () => {
  let crash = true;
  const f = await fixture("ack-loss", { workerRuntime: { failpoint: async point => { if (point === "after_launch_reservation" && crash) { crash = false; throw Error("simulated acknowledgement loss"); } } } });
  try {
    const plan = await f.call("dag_plan_save", planInput("ack", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /acknowledgement loss/);
    assert.equal((await f.service().read()).run.nodes.a.reservation.state, "dispatching");
    await f.reload(); await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    const binding = (await f.service().read()).run.nodes.a.reservation.binding;
    await waitTerminal(f, run.runId, "a"); await f.reload();
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    assert.deepEqual((await f.service().read()).run.nodes.a.reservation.binding, binding); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 1);
    const terminal = await waitTerminal(f, run.runId, "a"); await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const before = (await f.service().read()).run.revision;
    await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId }); assert.equal((await f.service().read()).run.revision, before);
    await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 2, completionId: terminal.completionId }), /STALE_GENERATION/);
    await f.call("dag_cancel", { runId: run.runId });
    await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 2, completionId: terminal.completionId }), /STALE_WORKER_GENERATION/);
    await f.call("dag_finalize", { runId: run.runId });
  } finally { await f.cleanup(); }
});
test("cancel fences generation before worker signaling and rejects late completion before actual reconciliation", async () => {
  const f = await fixture("cancel"); try {
    const input = planInput("cancel", ["a"]); input.workItems[0].context = ["wait-for-cancel"];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    const cancelled = await f.call("dag_cancel", { runId: run.runId }); assert.equal(cancelled.nodes.a.generation, 2); assert.equal(cancelled.status, "cancelling");
    const terminal = await waitTerminal(f, run.runId, "a");
    await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId }), /STALE_GENERATION/);
    assert.equal((await f.call("dag_finalize", { runId: run.runId })).status, "cancelled"); assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});

test("actual worker creation before lost dispatch ack recovers read-only identity, cancellation never relaunches", async () => {
  const f = await fixture("created-ack"); try {
    const input = planInput("created-ack", ["a"]), plan = await f.call("dag_plan_save", input);
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const workers = f.service().workers, ensure = workers.ensure.bind(workers); let binding;
    workers.ensure = async reservation => { const value = await ensure(reservation); binding = value.binding; throw Error("lost concrete worker acknowledgement"); };
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /lost concrete/);
    assert(binding); assert.equal((await f.service().read()).run.nodes.a.reservation.state, "dispatching");
    await f.reload(); f.ctx.model = { id: "changed-parent-model", provider: "changed-parent" };
    const recovered = await f.call("dag_recover_dispatch", { runId: run.runId, itemId: "a", generation: 1 }); assert.deepEqual(recovered.nodes.a.reservation.binding, binding);
    await waitTerminal(f, run.runId, "a"); await f.handles.workerManager.scan({ includeTerminal: true });
    await f.handles.workerManager.scanQueue; await f.handles.workerManager.store.queue;
    const before = await tree(join(f.root, ".ai/worker-sessions"));
    const historical = await f.call("dag_history_worker", { binding }); assert.equal(historical.attempt.attemptNumber, 1);
    assert.deepEqual(await tree(join(f.root, ".ai/worker-sessions")), before);
    await f.call("dag_cancel", { runId: run.runId }); assert.equal((await f.call("dag_finalize", { runId: run.runId })).status, "cancelled");
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 1); assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});
test("pre-attempt cancellation durably fences the generic reservation without inventing completion or dispatch", async () => {
  let armed = true;
  const f = await fixture("unlaunched-cancel", { workerRuntime: { failpoint: async point => { if (point === "after_launch_reservation" && armed) { armed = false; throw Error("reserved before attempt"); } } } });
  try {
    const plan = await f.call("dag_plan_save", planInput("unlaunched", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /reserved before attempt/);
    const reservation = (await f.service().read()).run.nodes.a.reservation;
    await f.call("dag_cancel", { runId: run.runId }); assert.equal((await f.call("dag_finalize", { runId: run.runId })).status, "cancelled");
    const state = await f.handles.workerManager.store.load(), worker = Object.values(state.workers)[0];
    assert.equal(worker.currentAttempt, 0); assert.equal(worker.status, "cancelled"); assert.equal(worker.attempts.length, 0);
    await assert.rejects(f.service().workers.ensure(reservation), /cancelled before/);
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 1); assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});
for (const boundary of ["after_launch_reservation", "after_attempt_reservation", "after_config_publication"]) test(`cancel/owner reload before ${boundary} recovery never launches a fenced generic intent`, async () => {
  let armed = true;
  const f = await fixture(`fence-${boundary}`, { workerRuntime: { failpoint: async point => { if (point === boundary && armed) { armed = false; throw Error(`crash ${boundary}`); } } } });
  try {
    const plan = await f.call("dag_plan_save", planInput("fenced", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /crash/);
    const s = f.service(); await s.runtime.cancel(await s.mutation(run.runId)); // crash after durable fence, before any manager signal
    await f.reload(); const state = await f.handles.workerManager.store.load(), worker = Object.values(state.workers)[0];
    assert.equal(state.launchRecords.length, 1); assert(worker.attempts.every(a => !a.dispatchClaimedAt && !a.supervisorPid));
    await assert.rejects(readFile(join(worker.cwd, "a.txt")), { code: "ENOENT" });
    await f.call("dag_cancel", { runId: run.runId }); assert.equal((await f.call("dag_finalize", { runId: run.runId })).status, "cancelled");
    await assert.rejects(f.handles.workerManager.retry(worker.id), /Externally managed launch/);
    await f.reload(); assert.equal((await f.service().read()).run.status, "cancelled");
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 1); assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});
test("registered V2 host never auto-dispatches or generically retries retained legacy-owned reservations", async () => {
  let armed = true;
  const f = await fixture("legacy-owned", { workerRuntime: { failpoint: async point => { if (point === "after_launch_reservation" && armed) { armed = false; throw Error("retained legacy reservation"); } } } });
  try {
    const baseCommit = f.git("rev-parse", "HEAD"), task = 'Item: {"id":"legacy","context":[]}';
    await assert.rejects(f.handles.workerManager.launchOwnedAttempt({ workerId: "legacy-owned", launchKey: "legacy-owned-key", expectedAttemptNumber: 1, configRequestHash: canonicalHash({ baseCommit, task }), baseCommit, worktreeKey: "legacy-owned", label: "legacy-owned", task }), /retained legacy/);
    await f.reload(); await f.handles.workerManager.scan({ includeTerminal: true });
    const worker = (await f.handles.workerManager.store.load()).workers["legacy-owned"];
    assert.equal(worker.normalizedRequest.explicitDispatchRecovery, undefined); assert.equal(worker.currentAttempt, 0); assert.equal(worker.attempts.length, 0);
    await assert.rejects(f.handles.workerManager.retry(worker.id), /Externally managed launch/);
    await assert.rejects(readFile(join(worker.cwd, "legacy.txt")), { code: "ENOENT" });
  } finally { await f.cleanup(); }
});
test("retained V1 session bindings cannot silently acquire V2 run authority", async () => {
  const f = await fixture("v1-binding"); try {
    const plan = await f.call("dag_plan_save", planInput("new-content", ["a"]));
    const directory = join(f.root, ".ai/dag-session-bindings-v1"); await mkdir(directory);
    const path = join(directory, `${createHash("sha256").update(f.ctx.sessionManager.getSessionId()).digest("hex")}.json`);
    await writeFile(path, '{"kind":"historical-or-corrupt-binding-is-not-new-authority"}\n');
    const before = await tree(join(f.root, ".ai"));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) }), /V1_SESSION_BINDING/);
    assert.deepEqual(await tree(join(f.root, ".ai")), before);
  } finally { await f.cleanup(); }
});
test("owned candidate hidden-dirty bytes and nested attributes fail closed before inspection filters", async () => {
  const f = await fixture("hidden-dirty"); try {
    const plan = await f.call("dag_plan_save", planInput("hidden-dirty", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    const binding = (await f.service().read()).run.nodes.a.reservation.binding, exact = await f.handles.workerManager.inspectBindingReadOnly(binding), cwd = exact.worker.cwd;
    execFileSync("git", ["update-index", "--assume-unchanged", "a.txt"], { cwd }); await writeFile(join(cwd, "a.txt"), "hidden dirty\n");
    await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId }), /UNCLEAN_CANDIDATE_WORKSPACE/);
    assert.equal((await f.service().read()).run.nodes.a.lifecycle.candidateReady, false);
    await writeFile(join(cwd, ".gitattributes"), "*.txt filter=unsafe\n");
    await assert.rejects(new PlanningFreshnessV2(f.root).current(plan), /attributes/);
    assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "hidden dirty\n");
  } finally { await f.cleanup(); }
});
test("actual failing F2 command preserves argv/exit/diagnostic and bounded retry; reports cannot PASS or land", async () => {
  const f = await fixture("failing-oracle"); try {
    const input = planInput("failing-oracle", ["a"]);
    input.workItems[0].lifecycle.checks[1].procedure.argv = [process.execPath, "-e", "process.stderr.write('observed oracle failure');process.exit(17)"];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: "invented" }), /EXACT_WORKER_COMPLETION_REQUIRED/);
    let current = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    await assert.rejects(f.call("dag_integrate", { runId: run.runId, itemId: "a", generation: 1, candidate: current.nodes.a.lifecycle.candidate }), /LIFECYCLE_NOT_READY/);
    let action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await f.call(tool, params); await assert.rejects(f.call(tool, params), /STALE_STAGE_ATTEMPT/);
    action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"); ({ tool, ...params } = action);
    await assert.rejects(f.call(tool, params), /CHECK_NOT_PASSED/);
    current = (await f.service().read()).run; const failed = current.nodes.a.lifecycle.executions.find(e => e.result?.exitCode === 17);
    assert(failed); assert.equal(failed.result.disposition, "FAIL"); assert.match(failed.result.stderr, /observed oracle failure/); assert(failed.result.executor.invoked);
    assert.deepEqual(failed.result.request.check.procedure.argv, input.workItems[0].lifecycle.checks[1].procedure.argv); assert(failed.result.durationMs >= 0);
    current = await f.call("dag_retry", { runId: run.runId, itemId: "a", generation: 1, executionId: failed.request.id });
    assert.equal(current.nodes.a.lifecycle.stage, 1); assert.equal(current.nodes.a.retries[0].count, 1);
    await assert.rejects(f.call("dag_retry", { runId: run.runId, itemId: "a", generation: 1, executionId: failed.request.id }), /CURRENT_FAILED_EXECUTION_REQUIRED/);
    assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});
for (const boundary of ["prepare", "advance"]) test(`stage attempt fencing rejects concurrent retry at ${boundary} after a real failing command`, async () => {
  const f = await fixture(`round-race-${boundary}`); try {
    const input = planInput(`round-race-${boundary}`, ["a"]);
    input.workItems[0].lifecycle.checks[0].procedure.argv = [process.execPath, "-e", "process.exit(17)"];
    if (boundary === "prepare") {
      const extra = structuredClone(input.workItems[0].lifecycle.checks[0]); extra.id = "extra-f1";
      extra.procedure.argv = [process.execPath, "verify.mjs", "a", "extra-F1"]; input.workItems[0].lifecycle.checks.push(extra);
    }
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    const ready = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const round = ready.nodes.a.lifecycle.round, runtime = f.service().runtime, record = runtime.recordResult.bind(runtime);
    const launches = [], ensure = f.service().runner.ensure.bind(f.service().runner);
    f.service().runner.ensure = async (...args) => { launches.push(args[0]); return ensure(...args); };
    let retried = false;
    runtime.recordResult = async (...args) => {
      const saved = await record(...args);
      if (!retried) {
        retried = true; const failed = saved.nodes.a.lifecycle.executions.find(e => e.result?.exitCode === 17);
        assert(failed.result.executor.invoked);
        const retry = await f.call("dag_retry", { runId: run.runId, itemId: "a", generation: 1, executionId: failed.request.id });
        assert.equal(retry.nodes.a.lifecycle.round, round + 1);
      }
      return saved;
    };
    const action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await assert.rejects(f.call(tool, params), /STALE_STAGE_ATTEMPT/);
    const current = (await f.service().read()).run;
    assert(retried); assert.equal(launches.length, 1); assert(launches.every(request => request.round === round));
    assert.equal(current.nodes.a.lifecycle.executions.length, 1); assert.equal(current.nodes.a.lifecycle.executions[0].status, "quarantined");
    assert.deepEqual(current.nodes.a.lifecycle.passed, [0]);
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call(tool, params), /STALE_STAGE_ATTEMPT/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before); assert.equal(launches.length, 1);
  } finally { await f.cleanup(); }
});
test("stage attempt fencing rejects a stage change inside revision refresh and preserves exact replay", async () => {
  const f = await fixture("stage-race"); try {
    const plan = await f.call("dag_plan_save", planInput("stage-race", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    const ready = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const product = f.service(), runtime = product.runtime, mutation = product.mutation.bind(product), { stage, round } = ready.nodes.a.lifecycle;
    const launches = [], ensure = product.runner.ensure.bind(product.runner);
    product.runner.ensure = async (...args) => { launches.push(args[0]); return ensure(...args); };
    let advanced;
    product.mutation = async runId => {
      if (!advanced) {
        const prepared = await runtime.prepareCheck(await mutation(runId), "a", 1, "f1", { stage, round });
        const replay = await runtime.prepareCheck(await mutation(runId), "a", 1, "f1", { stage, round });
        assert.equal(canonicalStringify(replay), canonicalStringify(prepared));
        const execution = prepared.nodes.a.lifecycle.executions[0];
        await product.runner.ensure(execution.request); await runtime.recordResult(await mutation(runId), "a", execution.request.id, product.runner);
        advanced = await runtime.advanceLifecycle(await mutation(runId), "a", 1, stage, round);
        assert.equal(advanced.nodes.a.lifecycle.stage, stage + 1);
        assert.equal(canonicalStringify(await runtime.advanceLifecycle(await mutation(runId), "a", 1, stage, round)), canonicalStringify(advanced));
      }
      return mutation(runId);
    };
    const action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await assert.rejects(f.call(tool, params), /STALE_STAGE_ATTEMPT/);
    assert.equal(canonicalStringify((await product.read()).run), canonicalStringify(advanced)); assert.equal(launches.length, 1); assert.equal(launches[0].stage, stage);
    await assert.rejects(runtime.prepareCheck(await mutation(run.runId), "a", 1, "f2", { stage, round }), /STALE_STAGE_ATTEMPT/);
    await assert.rejects(runtime.advanceLifecycle(await mutation(run.runId), "a", 1, stage, round - 1), /STALE_STAGE_ATTEMPT/);
    assert.equal(canonicalStringify((await product.read()).run), canonicalStringify(advanced));
  } finally { await f.cleanup(); }
});
test("actual command owner death after clean workspace removal offers read-only recovery frontier and bounded retry", async () => {
  const f = await fixture("command-owner"); try {
    const plan = await f.call("dag_plan_save", planInput("command-owner", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const runner = f.service().runner, ensure = runner.ensure.bind(runner);
    runner.ensure = async request => {
      loseCommandResultPublication(f.root, request);
      throw Error("actual executor owner died before result publication");
    };
    const action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await assert.rejects(f.call(tool, params), /actual executor owner died/); runner.ensure = ensure;
    let current = (await f.service().read()).run; const execution = current.nodes.a.lifecycle.executions[0], job = (await f.service().runtime.store.read()).executions[execution.request.id];
    assert.equal(job.status, "running"); assert.equal(job.result, undefined); await assert.rejects(readFile(join(job.workspace, "a.txt")), { code: "ENOENT" });
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    const frontier = await f.call("dag_next_action", {});
    assert.deepEqual(frontier.actions, [{ tool: "dag_recover_execution", runId: run.runId, itemId: "a", executionId: execution.request.id }]);
    assert.deepEqual(await f.call("dag_next_action", { runId: run.runId, itemId: "a" }), frontier);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before, "frontier must not acquire a lease, ingest or settle results");
    const { tool: recoveryTool, ...recoveryParams } = frontier.actions[0];
    current = await f.call(recoveryTool, recoveryParams);
    const recovered = current.nodes.a.lifecycle.executions[0].result; assert.equal(recovered.disposition, "BLOCKED"); assert.equal(recovered.exitCode, 0); assert.equal(recovered.executor.invoked, true); assert.deepEqual(current.nodes.a.lifecycle.passed, [0]);
    current = await f.call("dag_retry", { runId: run.runId, itemId: "a", generation: 1, executionId: execution.request.id }); assert.equal(current.nodes.a.retries[0].dimension, "infrastructure");
    for (const finding of current.nodes.a.lifecycle.findings) await f.call("dag_disposition_finding", { runId: run.runId, itemId: "a", findingId: finding.finding.id, disposition: "Kernel extinction and native deregistration were observed; explicit bounded retry will obtain new evidence." });
    const next = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"); const { tool: nextTool, ...nextParams } = next;
    current = await f.call(nextTool, nextParams); assert.deepEqual(current.nodes.a.lifecycle.passed, [0, 1]);
    assert.equal(current.nodes.a.lifecycle.executions[0].status, "quarantined"); assert.equal(current.nodes.a.lifecycle.executions[1].result.disposition, "PASS");
  } finally { await f.cleanup(); }
});
test("active-run source drift blocks dispatch, and worker-authored model changes never become candidates", async () => {
  const f = await fixture("running-source"); try {
    const plan = await f.call("dag_plan_save", planInput("running-source", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const decision = { ...structuredClone(f.model.decisions[0]), id: "DEC-new", title: "New constraint" }; decision.acceptance.contentHash = semanticHash("decisions", decision); f.model.decisions.push(decision); f.model.project.projections.specs[0].sections[0].objectIds.push(decision.id);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content); f.git("add", "."); f.git("commit", "-m", "accepted source change during run");
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /RUN_STALE_SOURCE_OR_BASELINE/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
  } finally { await f.cleanup(); }
  const g = await fixture("candidate-source"); try {
    const plan = await g.call("dag_plan_save", planInput("candidate-source", ["model-change"])), run = await g.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["model-change"]) });
    await g.call("dag_start_work", { runId: run.runId, itemId: "model-change", generation: 1 }); const terminal = await waitTerminal(g, run.runId, "model-change"); assert.equal(terminal.terminalStatus, "succeeded");
    await assert.rejects(g.call("dag_record_completion", { runId: run.runId, itemId: "model-change", generation: 1, completionId: terminal.completionId }), /WORKER_CHANGED_FROZEN_MODEL_OR_SPEC/);
    assert.equal((await g.service().read()).run.nodes["model-change"].lifecycle.candidateReady, false); assert.equal(g.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await g.cleanup(); }
});
test("real product-defect repair freezes observed failure context, edits the prior candidate and obtains fresh F0-F8 evidence", async () => {
  const f = await fixture("repair"); try {
    const input = planInput("repair", ["repairable"]); for (const check of [...input.integration.prefixCommands, ...input.integration.finalCommands]) check.argv = [process.execPath, "verify.mjs", "repairable", check.id];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["repairable"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "repairable", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "repairable");
    let current = await f.call("dag_record_completion", { runId: run.runId, itemId: "repairable", generation: 1, completionId: terminal.completionId }); const firstCandidate = current.nodes.repairable.lifecycle.candidate;
    const action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await assert.rejects(f.call(tool, params), /CHECK_NOT_PASSED/);
    const failed = (await f.service().read()).run.nodes.repairable.lifecycle.executions[0]; assert.equal(failed.result.disposition, "FAIL");
    await f.call("dag_retry", { runId: run.runId, itemId: "repairable", generation: 1, executionId: failed.request.id });
    current = await f.call("dag_replace_worker", { runId: run.runId, itemId: "repairable", generation: 1, completionId: terminal.completionId });
    const request = JSON.parse(current.nodes.repairable.reservation.request); assert.equal(request.baseCommit, firstCandidate.commit); assert.match(request.task, /Repair observations:/); assert.match(request.task, /FAIL/);
    current = await finish(f, run.runId, "repairable", 2);
    assert.equal(current.status, "complete"); assert.equal(current.nodes.repairable.retries.find(r => r.dimension === "product").count, 1); assert.equal(current.nodes.repairable.retries.find(r => r.dimension === "replacement").count, 1);
    assert.equal(await readFile(join(f.root, "repairable.txt"), "utf8"), "repairable\n"); assert.notEqual(current.nodes.repairable.lifecycle.candidate.tree, firstCandidate.tree);
    assert.equal(current.nodes.repairable.lifecycle.executions[0].status, "quarantined");
  } finally { await f.cleanup(); }
});
test("registered Git validation recovery uses real extinct command evidence, closes only a clean prefix and freezes retry context", async () => {
  const f = await fixture("git-command-owner"); try {
    const plan = await f.call("dag_plan_save", planInput("git-command-owner", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const ready = await finish(f, run.runId, "a", 1, false), candidate = ready.nodes.a.lifecycle.candidate;
    f.service().git.options.failpoint = async point => { if (point === "composed") throw Error("pause at real composed validation intent"); };
    await assert.rejects(f.call("dag_integrate", { runId: run.runId, itemId: "a", generation: 1, candidate }), /real composed/);
    const op = (await f.service().read()).run.gitOperations[0]; assert.equal(op.phase, "composed");
    loseCommandResultPublication(f.root, op.checks[0]);
    let action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_recover_execution"); assert.equal(action.executionId, op.checks[0].id);
    let { tool, ...params } = action; const observed = await f.call(tool, params); assert.equal(observed.execution.disposition, "BLOCKED"); assert.equal(observed.execution.exitCode, 0);
    action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_close_git_operation"); ({ tool, ...params } = action); const closed = await f.call(tool, params);
    assert.equal(closed.gitOperations[0].phase, "closed"); assert.equal(closed.nodes.a.retries.find(r => r.dimension === "integration").count, 1);
    action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_replace_worker"); ({ tool, ...params } = action); const replaced = await f.call(tool, params);
    assert.equal(replaced.nodes.a.generation, 2); const request = JSON.parse(replaced.nodes.a.reservation.request); assert.equal(request.baseCommit, candidate.commit); assert.match(request.task, /Executor died without a durable lifecycle result/);
    assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 1, "replacement is inert until explicit guarded dispatch");
  } finally { await f.cleanup(); }
});
test("failed implementation replacement is bounded, uses fresh keyed generation and rejects old completion", async () => {
  const f = await fixture("replacement"); try {
    const plan = await f.call("dag_plan_save", planInput("replacement", ["broken"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["broken"]) });
    let oldCompletion;
    for (let generation = 1; generation <= 3; generation++) {
      await f.call("dag_start_work", { runId: run.runId, itemId: "broken", generation }); const terminal = await waitTerminal(f, run.runId, "broken");
      assert.equal(terminal.terminalStatus, "needs_attention");
      if (oldCompletion) await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "broken", generation: generation - 1, completionId: oldCompletion }), /STALE_GENERATION/);
      const params = { runId: run.runId, itemId: "broken", generation, completionId: terminal.completionId };
      if (generation < 3) { const replaced = await f.call("dag_replace_worker", params); assert.equal(replaced.nodes.broken.generation, generation + 1); }
      else await assert.rejects(f.call("dag_replace_worker", params), /RETRY_EXHAUSTED/);
      oldCompletion = terminal.completionId;
    }
    const state = await f.handles.workerManager.store.load(); assert.equal(state.launchRecords.length, 3); assert(Object.values(state.workers).every(w => w.currentAttempt === 1));
    assert.equal((await f.service().read()).run.nodes.broken.lifecycle.candidateReady, false);
    assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
  } finally { await f.cleanup(); }
});
test("native Git landing acknowledgement loss reloads exact operation and accepts one landing without redispatch", async () => {
  const f = await fixture("git-ack"); try {
    const plan = await f.call("dag_plan_save", planInput("git-ack", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const ready = await finish(f, run.runId, "a", 1, false), candidate = ready.nodes.a.lifecycle.candidate;
    let once = true; f.service().git.options.failpoint = async point => { if (point === "git-exited" && once) { once = false; throw Error("lost native landing acknowledgement"); } };
    await assert.rejects(f.call("dag_integrate", { runId: run.runId, itemId: "a", generation: 1, candidate }), /lost native landing/);
    const landed = f.git("rev-parse", "HEAD"), reflog = f.git("reflog", "show", "--format=%H", "main");
    await f.reload(); const current = await f.call("dag_integrate", { runId: run.runId, itemId: "a", generation: 1, candidate });
    assert.equal(current.status, "complete"); assert.equal(current.gitOperations[0].dispatches, 1); assert.equal(current.gitOperations[0].proposal.commit, landed);
    assert.equal(f.git("reflog", "show", "--format=%H", "main"), reflog);
  } finally { await f.cleanup(); }
});
test("registered historical V1 plan/run/Git/worker readers preserve byte/path identity and never create V2 authority", async () => {
  const f = await fixture("history"); try {
    const input = planInput("history", ["a"]), legacy = createDagPlanningPlanV1({ planId: input.planId, status: "draft", title: input.title, focusId: "focus-product",
      repository: { repositoryId: "repo-main", baselineCommit: f.git("rev-parse", "HEAD"), baselineTree: f.git("rev-parse", "HEAD^{tree}"), targetBranch: "main" },
      source: { refs: [{ kind: "project_model_object", collection: "decisions", objectId: "DEC-delivery", semanticHash: semanticHash("decisions", f.model.decisions[0]) }], scopeSummary: input.scopeSummary }, architecture: input.architecture,
      workItems: input.workItems.map(({ lifecycle, resources, gates, ...n }) => n), constraints: { maxConcurrency: 1, mutexGroups: [] }, integration: input.integration,
      approval: { status: "pending", by: null, at: null, note: null }, authorization: { status: "not_authorized", by: null, at: null, scope: [], maxConcurrency: null, note: null } }, AT);
    const plans = new DagPlanningStoreV1(f.root); await plans.create(legacy); await writeFile(plans.pathFor("history"), `${JSON.stringify(legacy, null, 3)}\n`);
    const plan = historicalPlanFixture({ commit: f.git("rev-parse", "HEAD"), tree: f.git("rev-parse", "HEAD^{tree}") }, 1), { genesis, context, seedFacts } = historicalRunFixture(plan, 1);
    const store = new DagRunSnapshotStoreV1(join(f.root, ".ai/dag-runs-v1"), genesis.runId);
    for (const fact of seedFacts) await store.putImmutableFact(fact);
    const proc = await readFile(`/proc/${process.pid}/stat`, "utf8");
    await store.initialize(genesis, context, { lockIdentity: canonicalHash("history-lock"), ownerTokenHash: canonicalHash("history-owner"), sessionId: "history-fixture", pid: process.pid, processStartIdentity: `linux-proc:${proc.slice(proc.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`, acquiredAt: AT });
    await mkdir(join(store.runDirectory, "authority"), { recursive: true }); await writeFile(join(store.runDirectory, "authority/plan.json"), canonicalStringify(plan)); await writeFile(join(store.runDirectory, "authority/context.json"), canonicalStringify(context));
    const before = await tree(join(f.root, ".ai"));
    assert.equal((await f.call("dag_history_v1", { kind: "plan", id: "history", revision: 1 })).planHash, legacy.planHash);
    for (const kind of ["run", "git", "workers"]) await f.call("dag_history_v1", { kind, id: genesis.runId });
    await assert.rejects(f.call("dag_history_v1", { kind: "run", id: genesis.runId, revision: 1 }), /REVISION_SELECTOR_ONLY_SUPPORTED_FOR_V1_PLANS/);
    await assert.rejects(f.call("dag_history_v1", { kind: "evaluation", id: canonicalHash("absent") }));
    await assert.rejects(f.call("dag_run_start", { selection: { planId: legacy.planId, revision: legacy.revision, planHash: legacy.planHash }, authority: authority(["a"]) }), /PLAN_SELECTION_STALE_OR_MISSING/);
    await f.call("dag_run_status", {}); await f.call("dag_next_action", {});
    assert.deepEqual(await tree(join(f.root, ".ai")), before);
    await assert.rejects(readFile(join(f.root, ".ai/dag-workflow-v2/state.json")), { code: "ENOENT" });
    await writeFile(plans.pathFor("history"), JSON.stringify({ ...legacy, schemaVersion: 99 })); const unknown = await tree(join(f.root, ".ai"));
    await assert.rejects(f.call("dag_history_v1", { kind: "plan", id: "history" })); assert.deepEqual(await tree(join(f.root, ".ai")), unknown);
  } finally { await f.cleanup(); }
});
test("passive widget bounds width, deduplicates reads, retains labeled last-good and fences late previous-session reads", async () => {
  const updates = [], ctx = { mode: "tui", hasUI: true, ui: { setWidget: (_name, value) => updates.push(value) } };
  let fail = false, release;
  const run = { runId: "run-one", revision: 1, status: "active", nodes: { a: { status: "active", generation: 1 } } }, plan = { title: "長いタイトル".repeat(20), workItems: [{ id: "a", dependsOn: [] }] };
  const widget = new ProductWidgetV2(() => ({ read: async () => { if (release === "pending") await new Promise(r => { release = r; }); if (fail) throw Error("observation unavailable"); return { run, plan }; } }));
  widget.mount(ctx); await widget.refresh(); const count = updates.length; await widget.refresh(); assert.equal(updates.length, count);
  const { visibleWidth } = await import("@earendil-works/pi-tui"); assert(updates.at(-1)().render(20).every(line => visibleWidth(line) <= 20));
  fail = true; await widget.refresh(); assert(updates.at(-1)().render(100).some(line => line.includes("stale"))); fail = false;
  release = "pending"; const pending = widget.refresh(); while (release === "pending") await delay(1); widget.dispose(); const disposed = updates.length; release(); await pending; assert.equal(updates.length, disposed);
  widget.mount({ ...ctx, hasUI: false, mode: "print" }); assert.equal(updates.length, disposed); widget.dispose();
});

const selected = tests.filter(([name]) => !process.env.DAG_V2_PRODUCT_FILTER || name.includes(process.env.DAG_V2_PRODUCT_FILTER));
assert(selected.length > 0, "No V2 product tests matched the requested filter");
for (const [name, run] of selected) {
  const start = performance.now(); await run(); console.log(`PASS ${name} (${((performance.now() - start) / 1000).toFixed(2)}s)`);
}
console.log(`${selected.length}/${tests.length} V2 registered product tests passed${process.env.DAG_V2_PRODUCT_FILTER ? " (filtered)" : ""}`);
