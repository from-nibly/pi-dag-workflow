import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm, symlink, lstat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { WORKER_ACTIVITY_EVENT, WORKER_ACTIVITY_REQUEST_EVENT } from "../extensions/dag-workflow/worker-runtime/activity.mjs";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { FocusSessionStore } from "../extensions/dag-workflow/project-model/sessions.ts";
import { SpecProjector } from "../extensions/dag-workflow/project-model/projector.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { selectorV2, parsePlanV2, planHashV2 } from "../extensions/dag-workflow/planning/v2.ts";
import { PlanningFreshnessV2 } from "../extensions/dag-workflow/planning/freshness-v2.ts";
import { ProductWidgetV2 } from "../extensions/dag-workflow/runtime-v2/widget.ts";
import { auditSnapshotV2 } from "../extensions/dag-workflow/runtime-v2/state.ts";
import { withResultHash, assertTerminalResult } from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { WorkerManager } from "../extensions/dag-workflow/worker-runtime/manager.mjs";
import { gitEnvironmentV2 } from "../extensions/dag-workflow/runtime-v2/command-runner.ts";
import { planFixture as historicalPlanFixture, runFixture as historicalRunFixture } from "./dag-dogfood-test.mjs";
import { DagRunSnapshotStoreV1 } from "../extensions/dag-workflow/dag-runtime/store.ts";
import { DagPlanningStoreV1 } from "../extensions/dag-workflow/planning/store.ts";
import { createDagPlanningPlanV1 } from "../extensions/dag-workflow/planning/artifact.ts";
import { canonicalHash, canonicalStringify } from "../extensions/dag-workflow/dag-runtime/common.ts";

import { publicationBarrier } from "./fixtures/immutable-publication-barrier.mjs";
import { historicalStartV2, historicalExecutionRequestV2, historicalDeadlineV2, removedAuthorityFieldV2 } from "./fixtures/historical-authority-v2.mjs";

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
async function historicalFocus(root, input) {
  await mkdir(join(root, ".ai/model-sessions"), { recursive: true });
  await writeFile(new FocusSessionStore(root).path(input.id), JSON.stringify({ schemaVersion: 1, ...input, createdAt: AT, updatedAt: AT, status: "active" }));
}
async function fixture(name, options = {}) {
  const root = options.root ?? await mkdtemp(join(tmpdir(), `dag-v2-product-${name}-`));
  const git = (...args) => execFileSync("git", args, { cwd: root, env: gitEnvironmentV2(), encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  if (!options.root) { git("init", "-b", "main"); git("config", "user.name", "Product fixture"); git("config", "user.email", "product@example.invalid"); }
  const decision = { id: "DEC-delivery", title: "Deliver verified local files", body: "Each item writes its named file containing its own ID. Causal items require the previous file. Architecture is one local file per item; never publish.", state: "accepted", scope: { kind: "repository" }, introducedBy: "user", sourceRefs: ["fixture"], relationships: [], createdAt: AT, updatedAt: AT, rationale: "Independent deterministic oracle observes actual committed bytes." };
  const model = { schemaVersion: 1, project: { id: "product-fixture", title: "Product", revision: 1, mode: "authoritative", createdAt: AT, updatedAt: AT,
    projections: { specs: [{ id: "SPEC-delivery", kind: "spec", path: "spec/delivery/spec.md", title: "Delivery", sections: [{ id: "direction", title: "Direction", objectIds: [decision.id] }] }] } },
    workstreams: [], intents: [], concepts: [], evidence: [], assumptions: [], questions: [], tensions: [], scenarios: [], proposals: [], decisions: [decision], commitments: [], discoveries: [] };
  if (!options.root) {
  await mkdir(join(root, "project-model")); await mkdir(join(root, "spec/delivery"), { recursive: true });
  await writeFile(join(root, "project-model/model.json"), JSON.stringify(model));
  for (const p of new SpecProjector(root).render(model)) await writeFile(join(root, p.path), p.content);
  await writeFile(join(root, ".gitignore"), ".ai/\n");
  await writeFile(join(root, "implement.mjs"), `import {writeFileSync,existsSync,readFileSync} from 'node:fs'; import {execFileSync} from 'node:child_process'; const id=process.argv[2]; if(id==='broken')process.exit(47); if(id==='b'&&!existsSync('a.txt'))process.exit(41); if(id==='c'&&!existsSync('b.txt'))process.exit(42); writeFileSync(id+'.txt',id==='repairable'&&process.argv[3]!=='repair'?'wrong\\n':id+'\\n'); if(id==='model-change'){const path='project-model/model.json',model=JSON.parse(readFileSync(path));model.decisions[0].body+=' unauthorized';writeFileSync(path,JSON.stringify(model));execFileSync('git',['add',path]);} execFileSync('git',['add',id+'.txt']); try{execFileSync('git',['diff','--cached','--quiet']);}catch(error){if(error.status!==1)throw error;execFileSync('git',['-c','user.name=fixture','-c','user.email=fixture@example.invalid','-c','core.hooksPath=/dev/null','commit','-m','implement '+id]);}`);
  await writeFile(join(root, "verify.mjs"), `import assert from 'node:assert/strict'; import {readFileSync,existsSync,readdirSync} from 'node:fs'; const id=process.argv[2]; if(id==='combined'){assert(existsSync('a.txt')); for(const f of readdirSync('.').filter(f=>/^[abc]\\.txt$/.test(f)))assert.equal(readFileSync(f,'utf8'),f[0]+'\\n');}else {assert.equal(readFileSync(id+'.txt','utf8'),id+'\\n'); if(id==='b')assert.equal(readFileSync('a.txt','utf8'),'a\\n'); if(id==='c')assert.equal(readFileSync('b.txt','utf8'),'b\\n');} console.log('observed exact bytes',id,process.argv[3]);`);
  git("add", "."); git("commit", "-m", "test: native product baseline");
  await historicalFocus(root, { id: "focus-product", title: "Product", workstreamIds: [] });
  }
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
  return { planId, expectedPlanRevision: 0, workstreamIds: [], title: planId, sourceRefs: [source, "spec:spec/delivery/spec.md"], scopeSummary: "Repository-wide accepted product delivery",
    architecture: { outcomes: [{ id: "out", description: "Verified causal local files" }], nonGoals: ["Publication"], notes: ["One file per item"], risks: ["Do not bypass independent checks"] },
    workItems: ids.map((id, index) => ({ id, title: `Implement ${id}`, objective: `Commit ${id}.txt containing ${id} and newline`, outcomeIds: ["out"], context: [], checks: ["Observe exact bytes"], dependsOn: index ? [ids[index - 1]] : [], risk: "low", riskNotes: [], resources: {}, gates: [],
      lifecycle: { oracle: { statement: "Committed named file has the exact ID and newline; dependencies exist", sourceRefs: [source], checkIds: ["f2"] },
        checks: Array.from({ length: 7 }, (_, j) => ({ id: `f${j + 1}`, stage: j + 1, expectation: j === 4 ? "Independent architecture invariant: one correct file per item and causal prefix" : "Observe exact committed bytes and causal dependencies", sourceRefs: [source], applicability: { kind: "required" }, procedure: { kind: "command", argv: [process.execPath, "verify.mjs", id, `F${j + 1}`] }, environment: "node-local", replay: "pure" })) } })),
    constraints: { maxConcurrency: 2, resources: {}, mutexGroups: [], gates: [] }, integration: { strategy: "serial", checks: ["Real combined prefix"], finalChecks: ["Real combined final"], prefixCommands: [{ id: "prefix", argv: [process.execPath, "verify.mjs", "combined", "prefix"] }], finalCommands: [{ id: "final", argv: [process.execPath, "verify.mjs", "combined", "final"] }] } };
}
const authority = scope => ({ scope, maxConcurrency: 1, effects: ["repository_local"] });
async function waitTerminal(f, runId, itemId) {
  const end = Date.now() + 30000;
  while (Date.now() < end) {
    const { run } = await f.service().read(runId), binding = run.nodes[itemId].reservation?.binding;
    if (binding) { const terminal = await f.handles.workerManager.terminalResultForBinding(binding); if (terminal) return terminal; }
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

function loseCommandResultPublication(root, request, afterPublication = false) {
      const code = `import {StoreV2,CommandRunnerV2} from ${JSON.stringify(pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href)};
        import {gitOptionsV2} from ${JSON.stringify(pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/git-native.ts")).href)};
        const root=process.argv[1], request=JSON.parse(process.argv[2]), store=new StoreV2(root), transaction=store.transaction.bind(store);
        store.transaction=update=>transaction((s,publish)=>update(s,async()=>{if(s.executions?.[request.id]?.result){if(${afterPublication})await publish();process.stdout.write('actual-result-publication-boundary\\n');process.kill(process.pid,'SIGKILL');}await publish();}));
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
test("planning ignores historical focus corruption without reading or rewriting it", async () => {
  const f = await fixture("corrupt-focus"); try {
    await writeFile(new FocusSessionStore(f.root).path("focus-product"), "not-json");
    assert.equal((await f.call("dag_plan_list", {})).total, 0);
    const plan = await f.call("dag_plan_save", planInput());
    assert.deepEqual(plan.source.selector, { kind: "workstream_scope_v2", workstreamIds: [] });
    assert.equal(await readFile(new FocusSessionStore(f.root).path("focus-product"), "utf8"), "not-json");
  } finally { await f.cleanup(); }
});
test("focus-free save requires explicit scope and retains legacy selector readability", async () => {
  const f = await fixture("focus-free-save"); try {
    f.pi.entries.splice(0); await rm(join(f.root, ".ai/model-sessions"), { recursive: true }); await f.reload();
    const input = planInput("scoped", ["a"]), missing = { ...input }; delete missing.workstreamIds;
    await assert.rejects(f.call("dag_plan_save", missing), /INVALID_V2/);
    await assert.rejects(f.call("dag_plan_save", { ...input, workstreamIds: ["WS-missing"] }), /SOURCE_WORKSTREAM_MISSING/);
    await f.command("plan --new no focus needed");
    const plan = await f.call("dag_plan_save", input);
    assert.deepEqual(plan.source.selector, { kind: "workstream_scope_v2", workstreamIds: [] });
    assert.equal((await f.call("dag_plan_list", { workstreamIds: [] })).total, 1);
    assert.equal((await f.call("dag_plan_list", { workstreamIds: ["WS-other"] })).total, 0);
    await assert.rejects(readdir(join(f.root, ".ai/model-sessions")), { code: "ENOENT" });
    const legacy = structuredClone(plan);
    legacy.source.selector = { kind: "model_scope_v2", focusId: "historical-focus", workstreamIds: [] };
    legacy.planHash = planHashV2(legacy);
    assert.deepEqual(parsePlanV2(legacy), legacy);
    assert.deepEqual((await new PlanningFreshnessV2(f.root).current(legacy)).source.selector, legacy.source.selector);
    assert.deepEqual(parsePlanV2(plan), plan);
  } finally { await f.cleanup(); }
});
test("large advisory backlog starts with bounded findings and discoverable details", async () => {
  const f = await fixture("large-review-backlog"); try {
    await new ProjectModelDomain(f.root).update({ workstreamIds: [] }, { add: Array.from({ length: 520 }, (_, i) => ({ collection: "questions", key: `backlog-${i}`, value: { title: `Question ${i}`, body: "Unresolved context", kind: "uncertainty" } })) });
    f.git("add", "."); f.git("commit", "-m", "fixture advisory backlog");
    const plan = await f.call("dag_plan_save", planInput("backlog", ["a"]));
    const payload = { selection: selectorV2(plan), authority: authority(["a"]) };
    const assessed = (await f.call("dag_plan_assess", payload)).assessment;
    assert(assessed.findings.length <= 512);
    assert.match(assessed.findings[0], /FINDINGS_TRUNCATED: omitted \d+.*dag_plan_findings/);
    const run = await f.call("dag_run_start", payload);
    assert.deepEqual(run.acceptance.findings, assessed.findings);
    assert.equal(run.nodes.a.status, "pending");
    const details = [];
    let offset = 0;
    do {
      const page = await f.call("dag_plan_findings", { ...payload, offset, limit: 64 });
      details.push(...page.findings); offset = page.nextOffset;
      assert.equal(page.total, 521);
    } while (offset !== null);
    assert.equal(details.length, 521);
    assert(details.some(x => x.includes("questions/Q-backlog-519")));
    assert(details.some(x => x.startsWith("CONTENT_REVIEW:")));
    assert.match(assessed.findings[0], new RegExp(`omitted ${details.length - (assessed.findings.length - 1)}`));
  } finally { await f.cleanup(); }
});
test("unresolved scoped directions are review findings, not consent or focus gates", async () => {
  const f = await fixture("review-guidance"); try {
    const plan = await f.call("dag_plan_save", planInput("review", ["a"]));
    const draft = structuredClone(f.model.decisions[0]);
    draft.id = "DEC-open"; draft.state = "candidate"; draft.title = "Choose output convention"; delete draft.acceptance;
    f.model.decisions.push(draft);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    const modelDomain = new ProjectModelDomain(f.root);
    for (const title of ["First pending", "Second pending"]) await modelDomain.createReview({ workstreamIds: [] }, { title, points: [{ title: "Choose", context: "An unresolved relevant choice", purpose: "decision", question: "Which behavior?", options: [{ label: "Keep", description: "Keep current behavior" }] }] });
    f.git("add", "."); f.git("commit", "-m", "fixture unresolved direction and independent reviews");
    const payload = { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) };
    const assessment = await f.call("dag_plan_assess", payload);
    assert(assessment.assessment.findings.some(x => x.startsWith("REVIEW_REQUIRED: decisions/DEC-open")));
    for (const id of ["review-first-pending", "review-second-pending"]) assert(assessment.assessment.findings.some(x => x.startsWith(`REVIEW_REQUIRED: ${id} revision 0`)));
    const run = await f.call("dag_run_start", payload);
    assert(run.acceptance.findings.some(x => x.startsWith("REVIEW_REQUIRED: decisions/DEC-open")));
    for (const id of ["review-first-pending", "review-second-pending"]) assert(run.acceptance.findings.some(x => x.startsWith(`REVIEW_REQUIRED: ${id} revision 0`)));
    assert.equal(run.nodes.a.status, "pending");
  } finally { await f.cleanup(); }
});
test("fresh no-focus session discovers empty repository and gets agent-guided entry without mutation", async () => {
  const f = await fixture("empty-discovery"); try {
    f.pi.entries.splice(0); await f.reload();
    // No model or focus artifacts are needed for discovery.
    await rm(join(f.root, "project-model"), { recursive: true });
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    assert.deepEqual(await f.call("dag_plan_list", {}), { total: 0, offset: 0, plans: [] });
    await f.command("run");
    const { message, options } = f.pi.messages.at(-1);
    for (const pattern of [/No active DAG to run/, /dag_plan_list/, /dag_plan_show/, /dag_run_start/, /authority/, /latest timestamp/, /V1/, /pagination/]) assert.match(message.content, pattern);
    assert.equal(options.triggerTurn, true);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    await assert.rejects(f.call("dag_plan_save", planInput()), /TARGET_DIRTY/);
  } finally { await f.cleanup(); }
});
test("fresh no-focus session discovers and starts an explicit revision without hashes after advisory assessment", async () => {
  const f = await fixture("saved-discovery"); try {
    const old = await f.call("dag_plan_save", planInput("saved", ["a"]));
    const plan = await f.call("dag_plan_save", { ...planInput("saved", ["a"]), expectedPlanRevision: 1, title: "Revised saved plan" });
    f.pi.entries.splice(0); await f.reload();
    const listed = await f.call("dag_plan_list", {});
    assert.equal(listed.total, 1); assert.equal(listed.plans[0].revision, 2);
    assert.equal(canonicalStringify(await f.call("dag_plan_show", { selection: selectorV2(plan) })), canonicalStringify(plan));
    await f.command("show");
    assert.match(f.pi.messages.at(-1).message.content, /Revised saved plan/);
    assert((await f.call("dag_plan_assess", { selection: selectorV2(old), authority: authority(["a"]) })).assessment.findings.some(x => x.startsWith("PLAN_NOT_CURRENT_HEAD")));
    await assert.rejects(f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["missing"]) }), /SCOPE/);
    await assert.rejects(f.call("dag_plan_assess", { selection: selectorV2(plan), authority: { ...authority(["a"]), [removedAuthorityFieldV2]: 1 } }), /INVALID_V2/);
    const payload = { selection: { planId: plan.planId, revision: plan.revision }, authority: authority(["a"]) };
    await writeFile(join(f.root, "baseline-change.txt"), "changed baseline");
    f.git("add", "baseline-change.txt"); f.git("commit", "-m", "advance baseline");
    const beforeAssessment = await tree(join(f.root, ".ai/dag-workflow-v2"));
    const assessment = await f.call("dag_plan_assess", payload);
    assert(assessment.assessment.findings.some(x => x.startsWith("BASELINE_CHANGED")));
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), beforeAssessment);
    await f.command(`run ${JSON.stringify(payload)}`);
    const run = (await f.service().read()).run;
    assert.equal(canonicalStringify(run.start.selection), canonicalStringify(selectorV2(plan)));
    assert.equal(run.acceptance.repository.baselineCommit, f.git("rev-parse", "HEAD"));
    assert(run.acceptance.findings.some(x => x.startsWith("BASELINE_CHANGED")));
    assert.equal(canonicalStringify(run.start.authority), canonicalStringify(payload.authority));
    assert.equal(canonicalStringify(run.start.selection), canonicalStringify(listed.plans.map(({ title, sourceSelector, ...s }) => s)[0]));
    await assert.rejects(f.call("dag_run_start", { ...payload, authority: { ...payload.authority, maxConcurrency: 2 } }), /BOUND_AUTHORITY_CONFLICT/);
  } finally { await f.cleanup(); }
});
test("advisory content, old revision, focus and concurrency start without a mandatory assessment receipt", async () => {
  const f = await fixture("advisory-content"); try {
    const plan = await f.call("dag_plan_save", planInput("advisory", ["a"]));
    await f.call("dag_plan_save", { ...planInput("advisory", ["a"]), expectedPlanRevision: 1, title: "New recommendation" });
    await historicalFocus(f.root, { id: "focus-other", title: "Other", workstreamIds: [] });
    f.pi.entries.push({ type: "custom", customType: "dag-model-focus-link", data: { repositoryRoot: f.root, focusSessionId: "focus-other", mode: "active" } }); await f.reload();
    const run = await f.call("dag_run_start", { selection: { ...selectorV2(plan), planHash: `sha256:${"0".repeat(64)}` }, authority: { ...authority(["a"]), maxConcurrency: plan.constraints.maxConcurrency + 1 } });
    for (const code of ["CONTENT_REVIEW", "CONTENT_HASH_MISMATCH", "PLAN_NOT_CURRENT_HEAD", "CONCURRENCY_EXCEEDS_PLAN"]) assert(run.acceptance.findings.some(x => x.startsWith(code)), code);
    assert.equal(run.start.selection.revision, 1);
    assert.equal((await finish(f, run.runId, "a")).status, "complete");
  } finally { await f.cleanup(); }
});
test("stale projections are observations, not fabricated refreshed provenance", async () => {
  const f = await fixture("stale-projection"); try {
    const plan = await f.call("dag_plan_save", planInput("projection", ["a"]));
    const spec = plan.source.refs.find(r => r.ref.startsWith("spec:")); assert(spec);
    await writeFile(join(f.root, spec.ref.slice(5)), "Stale generated projection\n");
    f.git("add", "."); f.git("commit", "-m", "stale projection fixture");
    const run = await f.call("dag_run_start", { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) });
    assert(run.acceptance.findings.some(x => x.includes("SPEC_PROJECTION_STALE")));
    assert.equal(run.acceptance.source, undefined);
    assert.equal((await finish(f, run.runId, "a")).status, "complete");
    assert.equal(canonicalStringify((await f.service().runtime.store.read()).plans[plan.planId][0]), canonicalStringify(plan));
  } finally { await f.cleanup(); }
});
test("missing committed source is advisory while malformed current model stays hard", async () => {
  const f = await fixture("missing-source"); try {
    const plan = await f.call("dag_plan_save", planInput("missing", ["a"]));
    const spec = plan.source.refs.find(r => r.ref.startsWith("spec:")); assert(spec);
    await rm(join(f.root, spec.ref.slice(5))); f.git("add", "."); f.git("commit", "-m", "delete source fixture");
    const payload = { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) };
    const assessment = await f.call("dag_plan_assess", payload);
    assert(assessment.assessment.findings.some(x => x.includes("SOURCE_NOT_FOUND")));
    const run = await f.call("dag_run_start", payload);
    assert(run.acceptance.findings.some(x => x.includes("SOURCE_NOT_FOUND")));
    assert.equal(run.acceptance.source, undefined);
    assert.equal((await finish(f, run.runId, "a")).status, "complete");
    await writeFile(join(f.root, "project-model/model.json"), "not-json"); f.git("add", "."); f.git("commit", "-m", "malformed model fixture");
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_plan_assess", payload), /JSON/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
  } finally { await f.cleanup(); }
});
for (const unavailable of ["candidate-model", "missing-model", "missing-spec", "stale-spec", "removed-projection"]) test(`${unavailable} cannot mask a later unsafe retained source`, async () => {
  const f = await fixture(`source-safety-${unavailable}`); try {
    const later = "spec/later/spec.md";
    const decision = { ...structuredClone(f.model.decisions[0]), id: "DEC-later" };
    f.model.decisions.push(decision);
    const projection = { ...structuredClone(f.model.project.projections.specs[0]), id: "SPEC-later", path: later };
    projection.sections[0].objectIds = [decision.id]; f.model.project.projections.specs.push(projection);
    await mkdir(join(f.root, "spec/later"));
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "second retained source fixture");
    const input = planInput("source-safety", ["a"]); input.sourceRefs.push(`spec:${later}`);
    const plan = await f.call("dag_plan_save", input);
    if (unavailable === "candidate-model") f.model.project.mode = "candidate";
    if (unavailable === "removed-projection") f.model.project.projections.specs.at(-1).path = "spec/renamed/spec.md";
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    if (unavailable === "missing-model") await rm(join(f.root, "project-model/model.json"));
    if (unavailable === "missing-spec") await rm(join(f.root, "spec/delivery/spec.md"));
    if (unavailable === "stale-spec") await writeFile(join(f.root, "spec/delivery/spec.md"), "Stale projection\n");
    f.git("add", "."); f.git("commit", "-m", "unavailable provenance fixture");
    const payload = { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) };
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    const assessment = await f.call("dag_plan_assess", payload);
    assert(assessment.assessment.findings.some(x => x.startsWith("SOURCE_ASSESSMENT:")));
    assert.equal(assessment.assessment.source, undefined);
    // The missing earlier spec must not hide a symlink in a later path's ancestor.
    if (unavailable === "missing-spec") {
      await rm(join(f.root, "spec/later"), { recursive: true });
      await symlink("delivery", join(f.root, "spec/later"));
    } else {
      await rm(join(f.root, later)); await symlink("../../verify.mjs", join(f.root, later));
    }
    f.git("add", "."); f.git("commit", "-m", "unsafe retained source fixture");
    for (const tool of ["dag_plan_assess", "dag_run_start"]) await assert.rejects(f.call(tool, payload), /SYMLINK_SOURCE_UNSUPPORTED/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
  } finally { await f.cleanup(); }
});
test("semantic unavailability does not mask malformed paths or unexpected source I/O", async () => {
  const f = await fixture("source-safety-errors"); try {
    const plan = await f.call("dag_plan_save", planInput("source-errors", ["a"]));
    f.model.project.mode = "candidate";
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    f.git("add", "."); f.git("commit", "-m", "unavailable model fixture");
    const freshness = new PlanningFreshnessV2(f.root);
    for (const [path, error] of [["../verify.mjs", /UNSAFE_SOURCE_PATH/], ["spec/delivery/spec.md/child", /ENOTDIR/], ["spec/delivery", /REGULAR_SOURCE_REQUIRED/]]) {
      const retained = structuredClone(plan);
      retained.source.refs.push({ ref: `spec:${path}`, digest: plan.source.refs[0].digest });
      await assert.rejects(freshness.observe(retained), error);
    }
    const run = await f.call("dag_run_start", { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) });
    assert(run.acceptance.findings.some(x => x.includes("AUTHORITATIVE_MODEL_REQUIRED")));
    assert.equal((await finish(f, run.runId, "a")).status, "complete");
  } finally { await f.cleanup(); }
});
test("native target races during acceptance stay hard and publish no run", async () => {
  const f = await fixture("acceptance-race"); try {
    const plan = await f.call("dag_plan_save", planInput("race", ["a"]));
    const freshness = f.service().runtime.freshness, current = freshness.current.bind(freshness);
    freshness.current = async p => {
      const observed = await current(p);
      await writeFile(join(f.root, "race.txt"), "concurrent native change\n");
      f.git("add", "."); f.git("commit", "-m", "target race fixture");
      return observed;
    };
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_run_start", { selection: { planId: plan.planId, revision: 1 }, authority: authority(["a"]) }), /TARGET_DRIFT/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
  } finally { await f.cleanup(); }
});
test("verification timeout config controls future authorized checks and native integration without plan edits", async () => {
  const f = await fixture("verification-timeout");
  const config = join(f.root, ".ai/dag.config.json"), marker = join(f.root, ".ai/timeout-started");
  const configure = value => writeFile(config, JSON.stringify({ verificationCommandTimeoutMs: value }));
  try {
    const input = planInput("verification-timeout", ["a"]);
    input.workItems[0].lifecycle.checks[0].procedure.argv = [process.execPath, "-e", `require('fs').writeFileSync(${JSON.stringify(marker)},'started');setTimeout(()=>console.log('observed exact bytes'),8000)`];
    const plan = await f.call("dag_plan_save", input), planBytes = canonicalStringify(plan);
    let run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const startBytes = canonicalStringify(run.start);
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    const terminal = await waitTerminal(f, run.runId, "a");
    await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const nextCheck = async () => {
      const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks");
      return { tool, params };
    };
    let action = await nextCheck();
    for (const invalid of [0, null, "7200000", 1.5, 2147483648]) {
      await configure(invalid); const bytes = await readFile(f.service().runtime.store.statePath, "utf8");
      await assert.rejects(f.call(action.tool, action.params), /INVALID_VERIFICATION_COMMAND_TIMEOUT/);
      assert.equal(await readFile(f.service().runtime.store.statePath, "utf8"), bytes, "invalid config cannot adopt workspace, lease or prepare a request");
    }
    await configure(5000);
    await assert.rejects(f.call(action.tool, action.params), /CHECK_NOT_PASSED/);
    run = (await f.service().read()).run;
    const failed = run.nodes.a.lifecycle.executions.at(-1); assert.equal(failed.result.disposition, "FAIL", failed.result.diagnostic);
    assert.equal(failed.request.commandTimeoutMs, 5000); assert(failed.result.executor.invoked);
    const failedBytes = JSON.stringify(failed);
    const retry = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_retry");
    const { tool: retryTool, ...retryParams } = retry; await f.call(retryTool, retryParams);
    await configure(30000); await rm(marker, { force: true }); action = await nextCheck();
    const executing = f.call(action.tool, action.params);
    // Wait only on this test-owned subprocess's positive start marker.
    for (let i = 0; ; i++) { try { await readFile(marker); break; } catch (e) { if (e.code !== "ENOENT" || i > 500) throw e; await delay(20); } }
    await configure(1); run = await executing;
    const passed = run.nodes.a.lifecycle.executions.at(-1);
    assert.equal(passed.request.commandTimeoutMs, 30000); assert.equal(passed.result.disposition, "PASS");
    assert.equal(passed.result.request.commandTimeoutMs, 30000);
    assert.equal(JSON.stringify(run.nodes.a.lifecycle.executions.find(e => e.request.id === failed.request.id).result), JSON.stringify(JSON.parse(failedBytes).result));
    await configure(14400000);
    for (let stage = 2; stage <= 8; stage++) { action = await nextCheck(); run = await f.call(action.tool, action.params); }
    assert(run.nodes.a.lifecycle.executions.filter(e => e.status === "observed" && e.request.stage >= 2).every(e => e.request.commandTimeoutMs === 14400000));
    const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_integrate");
    await configure(null); const bytes = await readFile(f.service().runtime.store.statePath, "utf8");
    await assert.rejects(f.call(tool, params), /INVALID_VERIFICATION_COMMAND_TIMEOUT/);
    assert.equal(await readFile(f.service().runtime.store.statePath, "utf8"), bytes);
    await configure(14400000); run = await f.call(tool, params); assert.equal(run.status, "complete");
    const snapshot = await f.service().runtime.store.read();
    assert(run.gitOperations[0].checks.every(c => c.commandTimeoutMs === 14400000 && snapshot.executions[c.id].result.request.commandTimeoutMs === 14400000));
    assert.equal(canonicalStringify(run.start), startBytes); assert.equal(canonicalStringify(await f.service().runtime.show(selectorV2(plan))), planBytes);
    await f.reload(); assert.equal((await f.service().read()).run.status, "complete");
  } finally { await f.cleanup(); }
});

test("new public inputs and generated plan/run/check frames have no authority deadline", async () => {
  const f = await fixture("no-deadline"); try {
    const plan = await f.call("dag_plan_save", planInput("no-deadline", ["a"]));
    const selection = { planId: plan.planId, revision: plan.revision };
    await assert.rejects(f.call("dag_run_start", { selection: { ...selection, revision: 999 }, authority: authority(["a"]) }), /PLAN_NOT_FOUND/);
    await assert.rejects(f.call("dag_run_start", { selection, authority: { ...authority(["a"]), maxConcurrency: 0 } }), /INVALID_V2/);
    await assert.rejects(f.call("dag_run_start", { selection, authority: { ...authority(["a"]), [removedAuthorityFieldV2]: 1 } }), /INVALID_V2/);
    for (const tool of f.pi.tools.values()) assert(!JSON.stringify(tool.parameters).includes(removedAuthorityFieldV2));
    const run = await f.call("dag_run_start", { selection, authority: authority(["a"]) });
    const completed = await finish(f, run.runId, "a");
    assert.equal(completed.status, "complete");
    assert(!JSON.stringify({ plan, run: completed }).includes(removedAuthorityFieldV2));
    assert(!(await readFile(join(f.root, ".ai/dag-workflow-v2/state.json"), "utf8")).includes(removedAuthorityFieldV2));
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
test("accepted-prefix registered dependent nodes overlap code with real same-node lifecycle and composed checks", async () => {
  const f = await fixture("overlapping-code"); try {
    await writeFile(join(f.root, "shared.mjs"), 'export const ids = [];\n');
    const implement = await readFile(join(f.root, "implement.mjs"), "utf8");
    await writeFile(join(f.root, "implement.mjs"), implement.replace("execFileSync('git',['add',id+'.txt']);", "writeFileSync('shared.mjs','export const ids = '+JSON.stringify(id==='a'?['a']:['a','b'])+';\\n'); execFileSync('git',['add',id+'.txt','shared.mjs']);"));
    await writeFile(join(f.root, "verify.mjs"), (await readFile(join(f.root, "verify.mjs"), "utf8")) + "\nassert.deepEqual((await import('./shared.mjs')).ids,existsSync('b.txt')?['a','b']:['a']); console.log('shared code checked');\n");
    f.git("add", "."); f.git("commit", "-m", "overlapping code oracle baseline");
    const plan = await f.call("dag_plan_save", planInput()), start = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a", "b"]) });
    let run = await finish(f, start.runId, "a"); const prefix = run.gitOperations[0].proposal, first = JSON.stringify(run.gitOperations[0]);
    await f.reload(); run = await finish(f, start.runId, "b"); assert.equal(run.status, "complete");
    const second = run.gitOperations[1]; assert.equal(second.profile, "ordinary-ff-v2-2"); assert.equal(second.sourceBase.commit, prefix.commit);
    assert.equal(JSON.parse(run.nodes.b.reservation.request).baseCommit, prefix.commit);
    assert.equal(second.proposal.tree, second.candidate.tree); assert.equal(JSON.stringify(run.gitOperations[0]), first);
    assert.equal(f.git("show", "HEAD:shared.mjs"), 'export const ids = ["a","b"];');
    const snapshot = await f.service().runtime.store.read();
    for (const op of run.gitOperations) {
      assert.equal(op.workspace.phase, "restored"); assert.equal(op.workspace.node.cwd, run.nodes[op.itemId].workspace.cwd);
      for (const check of op.checks) {
        const job = snapshot.executions[check.id]; assert.equal(job.workspace, op.workspace.node.cwd);
        assert.equal(job.result.disposition, "PASS"); assert.equal(job.result.exitCode, 0); assert.equal(job.result.executor.invoked, true);
        assert.match(job.result.stdout, /shared code checked/); assert.equal(check.candidate.commit, op.proposal.commit);
      }
    }
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
    await assert.rejects(f.command("show"), /PLAN_SELECTION_REQUIRED/); await f.command("run");
    assert.match(f.pi.messages.at(-1).message.content, /Find a saved V2 DAG plan/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    await f.call("dag_plan_save", { ...planInput("one", ["a"]), expectedPlanRevision: 1, title: "new current content" });
    assert((await f.call("dag_plan_assess", { selection: selectorV2(one), authority: authority(["a"]) })).assessment.findings.some(x => x.startsWith("PLAN_NOT_CURRENT_HEAD")));
    const run = await f.call("dag_run_start", { selection: selectorV2(two), authority: authority(["a"]) });
    const otherContext = { ...f.ctx, sessionManager: { ...f.ctx.sessionManager, getSessionId: () => "other-session" } };
    await assert.rejects(f.pi.tools.get("dag_start_work").execute("test", { runId: run.runId, itemId: "a", generation: 1 }, undefined, undefined, otherContext), /EXACT_SESSION_RUN_REQUIRED/);
    await historicalFocus(f.root, { id: "focus-other", title: "Other", workstreamIds: [] });
    f.pi.entries.push({ type: "custom", customType: "dag-model-focus-link", data: { repositoryRoot: f.root, focusSessionId: "focus-other", mode: "active" } }); await f.reload();
    const frozen = await tree(join(f.root, ".ai/dag-workflow-v2"));
    assert(!(await f.call("dag_plan_assess", { selection: selectorV2(two), authority: run.start.authority })).assessment.findings.some(x => x.startsWith("PLAN_FOCUS_MISMATCH")));
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
    f.pi.entries.splice(0); await f.reload();
    const next = structuredClone(f.model.decisions[0]); next.id = "DEC-new"; next.title = "New governing constraint";
    f.model.decisions.push(next);
    f.model.project.projections.specs[0].sections[0].objectIds.push(next.id);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model));
    for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "new applicable authority");
    const observed = await new PlanningFreshnessV2(f.root).current(plan); assert.notEqual(observed.source.governingClosure, plan.source.governingClosure); assert(observed.source.refs.some(r => r.ref === "model:decisions/DEC-new"));
    const assessment = await f.call("dag_plan_assess", { selection: selectorV2(plan), authority: authority(["a", "b"]) });
    assert(assessment.assessment.findings.some(x => x.startsWith("SOURCE_CHANGED")));
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before);
    const run = await f.call("dag_run_start", { selection: { planId: plan.planId, revision: plan.revision }, authority: authority(["a", "b"]) });
    assert.equal(run.acceptance.source.governingClosure, observed.source.governingClosure);
    assert.equal(canonicalStringify(await f.call("dag_plan_show", { selection: selectorV2(plan) })), canonicalStringify(plan));
    await finish(f, run.runId, "a"); await f.reload();
    assert.equal((await finish(f, run.runId, "b")).status, "complete");
  } finally { await f.cleanup(); }
});
test("frozen governing selector recomputes transitive accepted links without promoting unrelated context", async () => {
  const f = await fixture("closure-links"); try {
    f.model.workstreams.push({ id: "WS-other", title: "Other", body: "Other scope", state: "active", scope: { kind: "repository" }, introducedBy: "user", sourceRefs: [], relationships: [], createdAt: AT, updatedAt: AT });
    for (const [id, kind, targetId] of [["DEC-linked", "affects", "DEC-delivery"], ["DEC-transitive", "depends_on", "DEC-linked"], ["DEC-context", "related_to", "DEC-delivery"]]) {
      const decision = { ...structuredClone(f.model.decisions[0]), id, title: id, scope: { kind: "workstreams", workstreamIds: ["WS-other"] }, relationships: [{ kind, targetId }] };
      f.model.decisions.push(decision); f.model.project.projections.specs[0].sections[0].objectIds.push(id);
    }
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "accepted transitive governing links");
    const plan = await f.call("dag_plan_save", planInput("closure-links", ["a"])); assert.deepEqual(plan.source.selector.workstreamIds, []);
    assert(plan.source.refs.some(r => r.ref === "model:decisions/DEC-linked")); assert(plan.source.refs.some(r => r.ref === "model:decisions/DEC-transitive")); assert(!plan.source.refs.some(r => r.ref === "model:decisions/DEC-context"));
    const changed = f.model.decisions.find(d => d.id === "DEC-transitive"); changed.body = "New applicable accepted transitive constraint";
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content);
    f.git("add", "."); f.git("commit", "-m", "change transitive governing content");
    const current = await new PlanningFreshnessV2(f.root).current(plan); assert.notEqual(current.source.governingClosure, plan.source.governingClosure);
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    assert(run.acceptance.findings.some(x => x.startsWith("SOURCE_CHANGED")));
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
    workers.ensure = async (...args) => { const value = await ensure(...args); binding = value.binding; throw Error("lost concrete worker acknowledgement"); };
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
    await assert.rejects(f.service().workers.ensure(reservation, (await f.service().read()).run.nodes.a.workspace), /cancelled before/);
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
test("actual command owner death retains node workspace and offers read-only recovery frontier and bounded retry", async () => {
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
    assert.equal(job.status, "running"); assert.equal(job.result, undefined); assert.equal(await readFile(join(job.workspace, "a.txt"), "utf8"), "a\n");
    assert(job.protocolDirectory && job.protocolDirectory !== resolve(job.workspace, ".."));
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    const frontier = await f.call("dag_next_action", {});
    assert.deepEqual(frontier.actions, [{ tool: "dag_recover_execution", runId: run.runId, itemId: "a", executionId: execution.request.id }]);
    assert.deepEqual(await f.call("dag_next_action", { runId: run.runId, itemId: "a" }), frontier);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before, "frontier must not acquire a lease, ingest or settle results");
    const { tool: recoveryTool, ...recoveryParams } = frontier.actions[0];
    current = await f.call(recoveryTool, recoveryParams);
    const recovered = current.nodes.a.lifecycle.executions[0].result; assert.equal(recovered.disposition, "BLOCKED"); assert.equal(recovered.exitCode, 0); assert.equal(recovered.executor.invoked, true); assert.deepEqual(current.nodes.a.lifecycle.passed, [0]);
    current = await f.call("dag_retry", { runId: run.runId, itemId: "a", generation: 1, executionId: execution.request.id }); assert.equal(current.nodes.a.retries[0].dimension, "infrastructure");
    for (const finding of current.nodes.a.lifecycle.findings) await f.call("dag_disposition_finding", { runId: run.runId, itemId: "a", findingId: finding.finding.id, disposition: "Kernel extinction and exact retained source integrity were observed; explicit bounded retry will obtain new evidence." });
    const next = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"); const { tool: nextTool, ...nextParams } = next;
    current = await f.call(nextTool, nextParams); assert.deepEqual(current.nodes.a.lifecycle.passed, [0, 1]);
    assert.equal(current.nodes.a.lifecycle.executions[0].status, "quarantined"); assert.equal(current.nodes.a.lifecycle.executions[1].result.disposition, "PASS");
  } finally { await f.cleanup(); }
});
test("published check result reconciles ownership after actual owner death before release", async () => {
  const f = await fixture("published-node-result"); try {
    const plan = await f.call("dag_plan_save", planInput("published-node-result", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }); const terminal = await waitTerminal(f, run.runId, "a");
    const current = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const product = f.service(), workspace = await product.workers.workspace(current.nodes.a.reservation);
    const prepared = await product.runtime.prepareCheck(await product.mutation(run.runId), "a", 1, "f1", undefined, workspace);
    const request = prepared.nodes.a.lifecycle.executions[0].request;
    loseCommandResultPublication(f.root, request, true);
    assert.equal((await product.runner.read(request)).disposition, "PASS");
    const { withWorkspaceOwnership } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
    await withWorkspaceOwnership(f.root, workspace.cwd, async state => { assert.equal(state.execution, request.id); });
    // A different observer may apply the immutable result before its original
    // publisher has acknowledged ownership release. The next stage must recover.
    await product.runtime.recordResult(await product.mutation(run.runId), "a", request.id, product.runner);
    await product.runtime.advanceLifecycle(await product.mutation(run.runId), "a", 1, 1);
    await f.reload();
    const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks");
    const checked = await f.call(tool, params); assert.deepEqual(checked.nodes.a.lifecycle.passed, [0, 1, 2]);
    assert.equal(checked.nodes.a.lifecycle.executions[1].request.nodeWorkspace.cwd, workspace.cwd);
    await withWorkspaceOwnership(f.root, workspace.cwd, async state => { assert.equal(state.execution, null); });
  } finally { await f.cleanup(); }
});
test("active-run source drift blocks dispatch, and worker-authored model changes never become candidates", async () => {
  const f = await fixture("running-source"); try {
    const plan = await f.call("dag_plan_save", planInput("running-source", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const decision = { ...structuredClone(f.model.decisions[0]), id: "DEC-new", title: "New constraint" }; f.model.decisions.push(decision); f.model.project.projections.specs[0].sections[0].objectIds.push(decision.id);
    await writeFile(join(f.root, "project-model/model.json"), JSON.stringify(f.model)); for (const p of new SpecProjector(f.root).render(f.model)) await writeFile(join(f.root, p.path), p.content); f.git("add", "."); f.git("commit", "-m", "accepted source change during run");
    const before = await tree(join(f.root, ".ai/dag-workflow-v2"));
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 }), /TARGET_|HEAD_|DIRTY|DRIFT/);
    assert.deepEqual(await tree(join(f.root, ".ai/dag-workflow-v2")), before); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
  } finally { await f.cleanup(); }
  const g = await fixture("candidate-source"); try {
    const plan = await g.call("dag_plan_save", planInput("candidate-source", ["model-change"])), run = await g.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["model-change"]) });
    await g.call("dag_start_work", { runId: run.runId, itemId: "model-change", generation: 1 }); const terminal = await waitTerminal(g, run.runId, "model-change"); assert.equal(terminal.terminalStatus, "succeeded");
    await assert.rejects(g.call("dag_record_completion", { runId: run.runId, itemId: "model-change", generation: 1, completionId: terminal.completionId }), /WORKER_CHANGED_FROZEN_MODEL_OR_SPEC/);
    assert.equal((await g.service().read()).run.nodes["model-change"].lifecycle.candidateReady, false); assert.equal(g.git("rev-parse", "HEAD"), plan.repository.baselineCommit);
    const frontier = await g.call("dag_next_action", {}); assert.equal(frontier.actions[0].tool, "dag_replace_worker"); assert.match(frontier.intakeRejections[0].diagnostic, /FROZEN/);
    const before = canonicalStringify((await g.service().read()).run);
    const { tool, ...params } = frontier.actions[0];
    await assert.rejects(g.call(tool, params), /NODE_WORKSPACE_REPAIR_BLOCKED.*WORKER_CHANGED_FROZEN_MODEL_OR_SPEC/s);
    assert.equal(canonicalStringify((await g.service().read()).run), before, "unsafe source cannot authorize a fresh clone or consume a generation");
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
test("borrowed node activity survives recovery without duplicate delivery or read-only ownership effects", async () => {
  const f = await fixture("node-activity", { tui: true, workerRuntime: { watchIntervalMs: 60_000 } });
  const snapshots = [], terminalEvents = [];
  const unsubscribe = f.pi.events.on(WORKER_ACTIVITY_EVENT, snapshot => snapshots.push(snapshot));
  const manager = f.handles.workerManager;
  const stopTerminal = manager.onTerminalResult(event => terminalEvents.push(event));
  try {
    const plan = await f.call("dag_plan_save", planInput("node-activity", ["a"]));
    let run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    run = await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    const binding = run.nodes.a.reservation.binding;
    const exact = await manager.inspectBindingReadOnly(binding);
    assert.equal(exact.worker.normalizedRequest.workingRoot.kind, "borrowed_node");
    assert(snapshots.some(snapshot => snapshot.stores.some(store => store.activeAttempts.some(attempt => attempt.workerId === binding.workerId && attempt.attemptNonce === binding.attemptNonce))));
    const terminal = await waitTerminal(f, run.runId, "a");
    assert.equal(terminal.terminalStatus, "succeeded");
    await Promise.all([manager.scan(), manager.inspectBinding(binding), manager.dispatchNext()]);
    const delivered = () => f.pi.messages.filter(({ message }) => message.customType === "subagent-completion");
    assert.equal(terminalEvents.length, 1, "reconciliation publishes the exact terminal event once");
    assert.equal(delivered().length, 1, "concurrent reconciliation shares one completion delivery");
    const before = manager.activitySnapshot();
    await manager.inspectBindingReadOnly(binding);
    await manager.terminalResultForBinding(binding);
    assert.deepEqual(manager.activitySnapshot(), before, "read-only binding evidence does not publish activity or acquire ownership");
    assert.equal(before.working, true, "terminal work remains active through completion delivery");
    await manager.onAgentSettled();
    assert.equal(manager.activitySnapshot().working, false);
    const workspace = canonicalStringify(run.nodes.a.workspace);
    await f.reload();
    assert.equal(snapshots.at(-1).phase, "detached");
    assert.equal(f.handles.workerManager.activitySnapshot().working, false);
    assert.equal(delivered().length, 0, "acknowledged completion is not redelivered after reload");
    run = (await f.service().read()).run;
    assert.equal(canonicalStringify(run.nodes.a.workspace), workspace);
    const state = await f.handles.workerManager.store.load();
    assert.equal(state.approvedDisposableRoots.length, 0);
    assert.equal((state.worktreeCleanupIntents ?? []).length, 0);
    assert.equal(state.launchRecords.length, 1);
    assert.equal(state.workers[binding.workerId].currentAttempt, binding.attemptNumber);
  } finally { unsubscribe(); stopTerminal(); await f.cleanup(); }
});

test("node-owned binding survives interrupted allocation and three worker generations across actual host restarts without approvals", async () => {
  const phase = process.env.DAG_V2_NODE_PHASE, infoPath = process.env.DAG_V2_NODE_INFO;
  const protect = f => {
    f.handles.workerManager.approveDisposableWorkingRoot = async () => { throw Error("node allocation must not approve disposable directories"); };
  };
  const remember = async (f, info, run) => {
    const reservation = structuredClone(run.nodes.repairable.reservation);
    const exact = await f.handles.workerManager.inspectBindingReadOnly(reservation.binding);
    const evidence = await f.handles.workerManager.terminalResultForBinding(reservation.binding, { evidence: true });
    assert.equal(exact.worker.normalizedRequest.workingRoot.kind, "borrowed_node");
    assert.equal(exact.worker.normalizedRequest.workingRoot.approvalId, undefined);
    assert.equal(canonicalStringify(exact.worker.normalizedRequest.workingRoot.workspace), canonicalStringify(run.nodes.repairable.workspace));
    info.history.push({ reservation, configPath: join(f.root, exact.attempt.configPath), configBytes: await readFile(join(f.root, exact.attempt.configPath), "utf8"), resultPath: evidence.resultPath, resultBytes: await readFile(evidence.resultPath, "utf8") });
  };
  if (phase === "allocate") {
    const f = await fixture("node-owned-restart", { workerRuntime: { failpoint(name) { if (name === "after_node_workspace_materialization") throw Error("allocation interrupted after native creation"); } } });
    protect(f);
    try {
      await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\n");
      await writeFile(join(f.root, "implement.mjs"), await readFile(join(f.root, "implement.mjs"), "utf8") + `
        const fs=await import('node:fs'); if(!fs.existsSync('node_modules/identity')) { fs.mkdirSync('node_modules/local',{recursive:true}); fs.writeFileSync('node_modules/identity',process.cwd()); fs.writeFileSync('node_modules/local/index.mjs','export default 73;'); }
        if(fs.readFileSync('node_modules/identity','utf8')!==process.cwd())throw Error('lost node setup');
      `);
      await writeFile(join(f.root, "verify.mjs"), `import dep from './node_modules/local/index.mjs';if(dep!==73)throw Error('dependency');console.log('node-cwd='+process.cwd());\n` + await readFile(join(f.root, "verify.mjs"), "utf8"));
      f.git("add", "."); f.git("commit", "-m", "test: node-owned setup");
      const input = planInput("node-owned-restart", ["repairable"]);
      for (const c of [...input.integration.prefixCommands, ...input.integration.finalCommands]) c.argv = [process.execPath, "verify.mjs", "repairable", c.id];
      const plan = await f.call("dag_plan_save", input);
      let run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["repairable"]) });
      const selectors = { runId: run.runId, itemId: "repairable", generation: 1 };
      await assert.rejects(f.call("dag_start_work", selectors), /allocation interrupted/);
      run = (await f.service().read()).run;
      assert.equal(run.nodes.repairable.workspace, undefined);
      assert.equal(run.nodes.repairable.reservation.state, "dispatching");
      const cwd = join(f.root, ".ai/worker-roots", `v2-${canonicalHash(`${run.runId}/repairable`).slice(7)}`);
      const { inspectNodeWorkspaceBinding } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
      const workspace = await inspectNodeWorkspaceBinding(cwd, `${run.runId}/repairable`);
      assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
      await writeFile(infoPath, JSON.stringify({ root: f.root, runId: run.runId, workspace, history: [], request: run.nodes.repairable.reservation.request }));
    } finally { await f.pi.emit("session_shutdown", f.ctx); }
    return;
  }
  if (phase === "first" || phase === "second") {
    const info = JSON.parse(await readFile(infoPath, "utf8"));
    const f = await fixture("node-owned-restart", { root: info.root }); protect(f);
    try {
      const generation = phase === "first" ? 1 : 2, selectors = { runId: info.runId, itemId: "repairable", generation };
      if (phase === "first") {
        f.handles.workerManager.options.failpoint = name => { if (name === "after_node_workspace_publication") throw Error("allocation receipt acknowledgement interrupted"); };
        await assert.rejects(f.call("dag_start_work", selectors), /receipt acknowledgement interrupted/);
        assert.equal((await f.service().read()).run.nodes.repairable.workspace, undefined);
        assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 0);
        await f.reload(); protect(f);
        assert.equal((await f.service().read()).run.nodes.repairable.reservation.request, info.request);
      }
      const workers = f.service().workers, ensure = workers.ensure.bind(workers);
      workers.ensure = async (reservation, workspace) => {
        // Read the durable file from another store, not the in-transaction draft.
        const { StoreV2 } = await import("../extensions/dag-workflow/runtime-v2/store.ts");
        const durable = (await new StoreV2(f.root).read()).runs[info.runId].nodes.repairable.workspace;
        assert.equal(canonicalStringify(durable), canonicalStringify(info.workspace));
        assert.equal(canonicalStringify(workspace), canonicalStringify(info.workspace));
        const drifted = structuredClone(workspace); drifted.identity.admin.ino = "1";
        await assert.rejects(workers.prepareWorkspace(reservation, drifted), /NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT/);
        const manager = f.handles.workerManager;
        await assert.rejects(manager.launch({ cwd: workspace.cwd, launchKey: reservation.operationId, task: "no directory capability" }, f.ctx), /NODE_WORKSPACE_CAPABILITY_MISMATCH/);
        const { withWorkspaceOwnership } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
        const epoch = await withWorkspaceOwnership(f.root, workspace.cwd, owner => owner.epoch);
        const hash = canonicalHash(JSON.parse(reservation.request));
        const borrowed = { kind: "borrowed_node", path: workspace.cwd, realPath: workspace.cwd, dev: workspace.identity.root.dev, ino: workspace.identity.root.ino, workspace, nodeId: workspace.nodeId, epoch: epoch + 1, requestHash: hash };
        await assert.rejects(manager.launch({ cwd: workspace.cwd, launchKey: reservation.operationId, task: "stale epoch", boundConfigRequestHash: hash, borrowedNodeWorkspace: borrowed }, f.ctx), /NODE_WORKSPACE_CAPABILITY_MISMATCH/);
        assert.equal((await manager.store.load()).launchRecords.length, generation - 1);
        return ensure(reservation, workspace);
      };
      let run = await f.call("dag_start_work", selectors), terminal = await waitTerminal(f, info.runId, "repairable");
      run = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
      await remember(f, info, run);
      const { tool, ...checkParams } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks");
      if (phase === "first") await assert.rejects(f.call(tool, checkParams), /CHECK_NOT_PASSED/);
      else await f.call(tool, checkParams);
      run = (await f.service().read()).run;
      assert.equal(run.nodes.repairable.lifecycle.executions.at(-1).result.disposition, phase === "first" ? "FAIL" : "PASS");
      run = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
      assert.equal(canonicalStringify(run.nodes.repairable.workspace), canonicalStringify(info.workspace));
      const state = await f.handles.workerManager.store.load();
      assert.equal(state.approvedDisposableRoots.length, 0); assert.equal((state.worktreeCleanupIntents ?? []).length, 0);
      await writeFile(infoPath, JSON.stringify(info));
    } finally { await f.pi.emit("session_shutdown", f.ctx); }
    return;
  }
  const directory = await mkdtemp(join(tmpdir(), "node-owned-restart-protocol-")), infoFile = join(directory, "info.json");
  let f;
  try {
    for (const step of ["allocate", "first", "second"]) {
      const child = spawnSync(process.execPath, [resolve("scripts/dag-v2-product-test.mjs")], { encoding: "utf8", timeout: 3600000,
        env: { ...process.env, DAG_V2_PRODUCT_FILTER: "node-owned binding survives", DAG_V2_NODE_PHASE: step, DAG_V2_NODE_INFO: infoFile } });
      assert.equal(child.status, 0, `${step}: ${child.stdout}\n${child.stderr}`);
    }
    const info = JSON.parse(await readFile(infoFile, "utf8"));
    f = await fixture("node-owned-restart", { root: info.root }); protect(f);
    const selectors = { runId: info.runId, itemId: "repairable", generation: 3 };
    const before = (await f.service().read()).run;
    assert.equal(canonicalStringify(before.nodes.repairable.workspace), canonicalStringify(info.workspace));
    const invalid = structuredClone((await f.service().runtime.store.read()));
    invalid.runs[info.runId].nodes.repairable.workspace.identity.admin.ino = "1";
    assert.throws(() => auditSnapshotV2(invalid), /NODE_EXECUTION_NATIVE_IDENTITY_MISMATCH/);
    const { withWorkspaceOwnership } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
    await withWorkspaceOwnership(f.root, info.workspace.cwd, async (owner, publish) => {
      assert.equal(canonicalStringify(owner.workspace), canonicalStringify(info.workspace));
      owner.workspace.identity.root.ino = "1"; owner.root.ino = "1";
      await assert.rejects(publish(owner), /NODE_WORKSPACE_BINDING_IMMUTABLE/);
    });
    await f.call("dag_start_work", selectors);
    const run = await finish(f, info.runId, "repairable", 3);
    assert.equal(run.status, "complete"); assert.equal(canonicalStringify(run.nodes.repairable.workspace), canonicalStringify(info.workspace));
    assert.equal(canonicalStringify(run.nodes.repairable.retries), canonicalStringify(before.nodes.repairable.retries));
    const state = await f.handles.workerManager.store.load();
    assert.equal(state.approvedDisposableRoots.length, 0); assert.equal(state.launchRecords.length, 3);
    assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 1);
    for (const worker of Object.values(state.workers)) { assert.equal(worker.cwd, info.workspace.cwd); assert.equal(worker.attempts.length, 1); }
    for (const old of info.history) {
      assert.equal(await readFile(old.configPath, "utf8"), old.configBytes); assert.equal(await readFile(old.resultPath, "utf8"), old.resultBytes);
      await assert.rejects(f.handles.workerManager.cleanupOwnedWorktreeForBinding(old.reservation.binding), /CLEANUP_RELINQUISHED/);
    }
    assert.equal(((await f.handles.workerManager.store.load()).worktreeCleanupIntents ?? []).length, 0);
    for (const job of Object.values((await f.service().runtime.store.read()).executions)) {
      assert.equal(job.workspace, info.workspace.cwd); assert.equal(canonicalStringify(job.workspaceIdentity), canonicalStringify(info.workspace.identity));
      assert(job.result.stdout.includes(`node-cwd=${info.workspace.cwd}`));
    }
    assert.equal(run.gitOperations.at(-1).workspace.phase, "restored");
  } finally { if (f) await f.cleanup(); await rm(directory, { recursive: true, force: true }); }
});

test("registered retained approval survives real owner process restarts and prelaunch dispatch recovery", async () => {
  const phase = process.env.DAG_V2_APPROVAL_PHASE, infoPath = process.env.DAG_V2_APPROVAL_INFO;
  if (phase === "prepare") {
    const f = await fixture("approval-restart");
    // Freeze genuinely old envelopes at creation, never reinterpret/rewrite a
    // persisted request or config. This fixture must keep exercising the shim.
    const runtime = f.service().runtime;
    const legacy = text => { const request = JSON.parse(text); delete request.workspaceProtocol; return canonicalStringify(request); };
    const reserve = runtime.reserve.bind(runtime), replace = runtime.replace.bind(runtime);
    runtime.reserve = (m, item, generation, request, ...args) => reserve(m, item, generation, legacy(request), ...args);
    runtime.replace = (m, item, generation, settled, request, ...args) => replace(m, item, generation, settled, legacy(request), ...args);
    let originalToken;
    const approve = f.handles.workerManager.approveDisposableWorkingRoot.bind(f.handles.workerManager);
    f.handles.workerManager.approveDisposableWorkingRoot = async cwd => { const result = await approve(cwd); originalToken = result.disposableRootToken; return result; };
    try {
      await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\n");
      await writeFile(join(f.root, "implement.mjs"), await readFile(join(f.root, "implement.mjs"), "utf8") + `
        const fs=await import('node:fs'); if(!fs.existsSync('node_modules/identity')) { fs.mkdirSync('node_modules/local-dependency',{recursive:true}); fs.writeFileSync('node_modules/identity',process.cwd()); fs.writeFileSync('node_modules/local-dependency/index.mjs','export default 73;'); }
        if(fs.readFileSync('node_modules/identity','utf8')!==process.cwd())throw Error('lost cwd');
      `);
      await writeFile(join(f.root, "verify.mjs"), `import value from './node_modules/local-dependency/index.mjs'; if(value!==73)throw Error('lost dependency'); console.log('node-cwd='+process.cwd());\n` + await readFile(join(f.root, "verify.mjs"), "utf8"));
      f.git("add", "."); f.git("commit", "-m", "test: retained approval dependency");
      const input = planInput("approval-restart", ["repairable"]);
      for (const c of [...input.integration.prefixCommands, ...input.integration.finalCommands]) c.argv = [process.execPath, "verify.mjs", "repairable", c.id];
      const plan = await f.call("dag_plan_save", input);
      let run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["repairable"]) });
      const history = [];
      for (const generation of [1, 2]) {
        const selectors = { runId: run.runId, itemId: "repairable", generation };
        await f.call("dag_start_work", selectors); const terminal = await waitTerminal(f, run.runId, "repairable");
        run = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
        const reservation = structuredClone(run.nodes.repairable.reservation), exact = await f.handles.workerManager.inspectBindingReadOnly(reservation.binding);
        const evidence = await f.handles.workerManager.terminalResultForBinding(reservation.binding, { evidence: true });
        history.push({ reservation, cwd: exact.worker.cwd, configPath: join(f.root, exact.attempt.configPath), configBytes: await readFile(join(f.root, exact.attempt.configPath), "utf8"), resultPath: evidence.resultPath, resultBytes: await readFile(evidence.resultPath, "utf8") });
        run = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
      }
      await f.call("dag_pause", { runId: run.runId });
      const state = await f.handles.workerManager.store.load();
      await writeFile(infoPath, JSON.stringify({ root: f.root, runId: run.runId, history, originalToken, reservation: run.nodes.repairable.reservation, counters: run.nodes.repairable.retries, retryHistory: run.nodes.repairable.retryHistory, approval: state.approvedDisposableRoots[0], owner: state.owner, storageId: state.storageId }));
    } finally { await f.pi.emit("session_shutdown", f.ctx); }
    return;
  }
  if (phase === "interrupt") {
    const info = JSON.parse(await readFile(infoPath, "utf8"));
    const f = await fixture("approval-restart", { root: info.root, workerRuntime: { failpoint(name) { if (name === "after_node_approval_handoff") throw Error("approval handoff interruption"); } } });
    try {
      const selectors = { runId: info.runId, itemId: "repairable", generation: 3 };
      await f.call("dag_resume", { runId: info.runId });
      await assert.rejects(f.call("dag_start_work", selectors), /approval handoff interruption/);
      const run = (await f.service().read()).run;
      assert.equal(run.nodes.repairable.reservation.state, "dispatching"); assert(!run.nodes.repairable.reservation.binding);
      assert.equal(await f.handles.workerManager.attemptIdentityByLaunchKey(info.reservation.operationId), null);
      assert.equal(Object.keys((await f.handles.workerManager.store.load()).workers).length, 2);
      await f.reload();
      await assert.rejects(f.call("dag_start_work", selectors), /approval handoff interruption/, "same-process reload reuses the immutable handoff receipt");
      await f.call("dag_pause", { runId: info.runId });
    } finally { await f.pi.emit("session_shutdown", f.ctx); }
    return;
  }
  const directory = await mkdtemp(join(tmpdir(), "approval-restart-protocol-")), infoFile = join(directory, "info.json");
  let f;
  try {
    for (const step of ["prepare", "interrupt"]) {
      const child = spawnSync(process.execPath, [resolve("scripts/dag-v2-product-test.mjs")], { encoding: "utf8", timeout: 3600000,
        env: { ...process.env, DAG_V2_PRODUCT_FILTER: "registered retained approval", DAG_V2_APPROVAL_PHASE: step, DAG_V2_APPROVAL_INFO: infoFile } });
      assert.equal(child.status, 0, `${step}: ${child.stdout}\n${child.stderr}`);
    }
    const info = JSON.parse(await readFile(infoFile, "utf8"));
    f = await fixture("approval-restart", { root: info.root });
    const selectors = { runId: info.runId, itemId: "repairable", generation: 3 }, cwd = info.history[1].cwd;
    let run = (await f.service().read()).run;
    assert.equal(run.status, "paused"); assert.equal(run.nodes.repairable.reservation.state, "dispatching");
    assert.equal(run.nodes.repairable.reservation.request, info.reservation.request);
    await assert.rejects(f.call("dag_recover_dispatch", selectors), /EXACT_EXISTING_ATTEMPT_REQUIRED/);
    await assert.rejects(f.call("dag_start_work", selectors), /RUN_NOT_ACTIVE/);
    const manager = f.handles.workerManager, state = await manager.store.load();
    assert.notEqual(state.owner.pid, info.owner.pid); assert.equal(state.storageId, info.storageId);
    const launchKey = info.reservation.operationId, requestHash = canonicalHash(JSON.parse(info.reservation.request));
    const tokenInput = { cwd, launchKey, task: "must not start", disposableApprovalId: info.approval.approvalId, boundConfigRequestHash: requestHash };
    for (const disposableRootToken of ["arbitrary-token", info.originalToken]) await assert.rejects(manager.launch({ ...tokenInput, disposableRootToken }, f.ctx), /approval is missing/);
    await assert.rejects(manager.launch({ ...tokenInput, launchKey: `${info.runId}/foreign/3` }, f.ctx), /approval is missing/);
    await assert.rejects(manager.launch(tokenInput, f.ctx), /approval is missing/, "an earlier process receipt does not authorize this process");
    await f.call("dag_resume", { runId: info.runId });
    const approvalBefore = structuredClone((await manager.store.load()).approvedDisposableRoots[0]);
    for (const [field, value, pattern] of [["ownerSessionId", "foreign", /another owner session/], ["ino", "1", /exact path\/device\/inode/], ["retiredAt", new Date().toISOString(), /missing or retired/]]) {
      await manager.store.mutate(s => { const a = s.approvedDisposableRoots[0]; a[field] = value; if (field === "ownerSessionId") { a.approvedByOwner.sessionId = value; a.nodeLaunchHandoffs = []; } });
      await assert.rejects(f.call("dag_start_work", selectors), pattern);
      await manager.store.mutate(s => { s.approvedDisposableRoots[0] = structuredClone(approvalBefore); });
    }
    const originalParents = manager.options.approvedDisposableRootParents;
    manager.options.approvedDisposableRootParents = [join(f.root, "spec")];
    await assert.rejects(f.call("dag_start_work", selectors), /approved disposable-root parent/);
    manager.options.approvedDisposableRootParents = originalParents;
    const { withWorkspaceOwnership } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
    let originalOwnership;
    await withWorkspaceOwnership(f.root, cwd, async (state, publish) => { originalOwnership = structuredClone(state); state.epoch++; await publish(state); });
    await assert.rejects(f.call("dag_start_work", selectors), /NODE_WORKSPACE_HANDOFF_CONFLICT/);
    await withWorkspaceOwnership(f.root, cwd, async (state, publish) => { await publish(originalOwnership); });
    await manager.store.mutate(s => { s.approvedDisposableRoots[0].approvedByOwner.processStartIdentity += "-foreign"; });
    await assert.rejects(f.call("dag_start_work", selectors), /exact settled approval-bound attempt/);
    await manager.store.mutate(s => { s.approvedDisposableRoots[0] = structuredClone(approvalBefore); });
    const workers = f.service().workers, ensure = workers.ensure.bind(workers); let bound;
    workers.ensure = async (...args) => { const result = await ensure(...args); bound = result.binding; throw Error("lost recovered launch acknowledgement"); };
    await assert.rejects(f.call("dag_start_work", selectors), /lost recovered launch acknowledgement/);
    assert(bound); assert.equal((await f.service().read()).run.nodes.repairable.reservation.state, "dispatching");
    workers.ensure = async () => { throw Error("read-only acknowledgement recovery must not launch"); };
    await f.call("dag_pause", { runId: info.runId });
    await f.call("dag_recover_dispatch", selectors);
    workers.ensure = ensure;
    await f.call("dag_resume", { runId: info.runId });
    await f.call("dag_start_work", selectors); await f.call("dag_recover_dispatch", selectors);
    assert.equal(canonicalStringify((await f.service().read()).run.nodes.repairable.reservation.binding), canonicalStringify(bound));
    run = await finish(f, info.runId, "repairable", 3);
    assert.equal(run.status, "complete"); assert.equal(canonicalStringify(run.nodes.repairable.retries), canonicalStringify(info.counters)); assert.equal(canonicalStringify(run.nodes.repairable.retryHistory), canonicalStringify(info.retryHistory));
    assert.equal(info.counters.filter(c => c.dimension === "replacement").reduce((sum, c) => sum + c.count, 0), 2);
    assert.equal(run.nodes.repairable.workspace.cwd, cwd); assert.equal(run.nodes.repairable.workspace.nodeId, `${info.runId}/repairable`);
    await withWorkspaceOwnership(f.root, cwd, owner => { assert.equal(canonicalStringify(owner.workspace), canonicalStringify(run.nodes.repairable.workspace)); });
    const finalState = await manager.store.load();
    assert.equal(Object.keys(finalState.workers).length, 3); assert.equal(finalState.launchRecords.length, 3);
    assert.equal(finalState.workers[bound.workerId].attempts.length, 1);
    const { nodeLaunchHandoffs, ...originalApproval } = finalState.approvedDisposableRoots[0];
    assert.deepEqual(originalApproval, info.approval); assert.equal(nodeLaunchHandoffs.length, 2);
    assert.equal(finalState.approvedDisposableRoots.length, 1); assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 1);
    for (const old of info.history) { assert.equal(await readFile(old.configPath, "utf8"), old.configBytes); assert.equal(await readFile(old.resultPath, "utf8"), old.resultBytes); }
    assert.equal(await readFile(join(cwd, "node_modules/identity"), "utf8"), cwd);
    for (const job of Object.values((await f.service().runtime.store.read()).executions)) {
      assert.equal(job.workspace, cwd); assert(job.result.stdout.includes(`node-cwd=${cwd}`)); assert.equal(job.result.disposition, "PASS");
    }
    assert.equal(run.gitOperations.at(-1).workspace.phase, "restored");
  } finally { if (f) await f.cleanup(); await rm(directory, { recursive: true, force: true }); }
});

test("persistent node dependency is consumed by real checks, failed retry and repaired F1-F8 in the same cwd", async () => {
  const f = await fixture("persistent-dependency"); try {
    await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\ndist/\n");
    await writeFile(join(f.root, "implement.mjs"), await readFile(join(f.root, "implement.mjs"), "utf8") + `
      const fs=await import('node:fs'); const path='node_modules/local-dependency/index.mjs';
      if(process.argv[3]==='repair') {
        if(fs.readFileSync('node_modules/identity','utf8')!==process.cwd())throw Error('repair lost node cwd/cache');
        if(fs.readFileSync(path,'utf8')!=="export default 73;\\n")throw Error('dependency lost');
      } else {fs.mkdirSync('node_modules/local-dependency',{recursive:true});fs.writeFileSync(path,"export default 73;\\n");fs.writeFileSync('node_modules/identity',process.cwd());}
    `);
    await writeFile(join(f.root, "verify.mjs"), `import value from './node_modules/local-dependency/index.mjs'; if(value!==73)throw Error('dependency not consumed'); console.log('node-cwd='+process.cwd());\n` + await readFile(join(f.root, "verify.mjs"), "utf8"));
    f.git("add", "."); f.git("commit", "-m", "test: actual node-local dependency consumer");
    const input = planInput("persistent-dependency", ["repairable"]);
    for (const check of [...input.integration.prefixCommands, ...input.integration.finalCommands]) check.argv = [process.execPath, "verify.mjs", "repairable", check.id];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["repairable"]) });
    const selectors = { runId: run.runId, itemId: "repairable", generation: 1 };
    await f.call("dag_start_work", selectors); const terminal = await waitTerminal(f, run.runId, "repairable");
    let current = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    const reservation = structuredClone(current.nodes.repairable.reservation), manager = f.handles.workerManager;
    const exact = await manager.inspectBindingReadOnly(reservation.binding), cwd = exact.worker.cwd;
    const configBytes = await readFile(join(f.root, exact.attempt.configPath), "utf8"), evidence = await manager.terminalResultForBinding(reservation.binding, { evidence: true }), resultBytes = await readFile(evidence.resultPath, "utf8");
    const check = async () => { const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"); return f.call(tool, params); };
    await assert.rejects(check(), /CHECK_NOT_PASSED/);
    const failed = (await f.service().read()).run.nodes.repairable.lifecycle.executions.at(-1);
    assert.match(failed.result.stdout, new RegExp('node-cwd=')); assert(failed.result.stdout.includes(cwd));
    await f.call("dag_retry", { ...selectors, executionId: failed.request.id });
    await assert.rejects(check(), /CHECK_NOT_PASSED/);
    const retry = (await f.service().read()).run.nodes.repairable.lifecycle.executions.at(-1);
    assert.notEqual(retry.request.id, failed.request.id); assert.equal(retry.request.nodeWorkspace.cwd, cwd);
    const stored = canonicalStringify(await f.service().runner.read(failed.request));
    await f.service().runner.ensure(failed.request); assert.equal(canonicalStringify(await f.service().runner.read(failed.request)), stored);
    await assert.rejects(manager.cleanupOwnedWorktreeForBinding(reservation.binding, { launchKey: reservation.operationId, effectId: "old-cleanup", requestHash: canonicalHash("old-cleanup") }), /NODE_WORKSPACE_CLEANUP_RELINQUISHED/);
    await assert.rejects(manager.retry(reservation.workerId, f.ctx), /Externally managed|relinquished/i);
    const alias = join(f.root, ".ai", "node-alias"); await symlink(cwd, alias);
    for (const path of [cwd, alias, join(cwd, "node_modules/local-dependency")]) {
      await assert.rejects(manager.launch({ task: "must not start", cwd: path, launchKey: `stale-${canonicalHash(path)}` }, f.ctx), /NODE_WORKSPACE_LAUNCH_RELINQUISHED/);
    }
    assert.equal(Object.keys((await manager.store.load()).workers).length, 1, "rejected launches cannot poison the retained workspace with reservations");
    await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
    await f.reload(); current = await finish(f, run.runId, "repairable", 2);
    assert.equal(current.status, "complete");
    const repaired = await f.handles.workerManager.inspectBindingReadOnly(current.nodes.repairable.reservation.binding); assert.equal(repaired.worker.cwd, cwd);
    assert.equal(await readFile(join(cwd, "node_modules/identity"), "utf8"), cwd);
    assert.equal(await readFile(join(cwd, "node_modules/local-dependency/index.mjs"), "utf8"), "export default 73;\n");
    assert.equal(await readFile(join(f.root, exact.attempt.configPath), "utf8"), configBytes); assert.equal(await readFile(evidence.resultPath, "utf8"), resultBytes);
    await assert.rejects(f.service().workers.candidateIdentity(reservation, terminal.completionId), /NODE_WORKSPACE_STALE_ATTEMPT/);
    const jobs = Object.values((await f.service().runtime.store.read()).executions).filter(j => j.request.nodeWorkspace);
    assert(jobs.every(j => j.workspace === cwd && j.result.workspace.node.cwd === cwd && !j.result.workspace.isolated));
    assert.equal(new Set(jobs.map(j => j.protocolDirectory)).size, jobs.length);
    assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 1);
    const operation = current.gitOperations.at(-1);
    assert.equal(operation.workspace.phase, "restored");
    for (const request of operation.checks) {
      const job = (await f.service().runtime.store.read()).executions[request.id];
      assert.equal(job.workspace, cwd); assert(job.result.stdout.includes('node-cwd=' + cwd));
      assert.deepEqual(job.request.candidate, operation.proposal);
    }
    assert.equal(execFileSync('git', ['rev-parse', 'HEAD'], { cwd, encoding: 'utf8' }).trim(), current.nodes.repairable.lifecycle.candidate.commit);
    await f.service().runner.ensure(failed.request); assert.equal(canonicalStringify(await f.service().runner.read(failed.request)), stored, "historical replay after repair is read-only evidence");
  } finally { await f.cleanup(); }
});
test("all node steps compose predecessors, preserve ignored collisions, close failures and recover partial transitions in one root", async () => {
  const f = await fixture("all-node-steps"); try {
    await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\n");
    await writeFile(join(f.root, "implement.mjs"), await readFile(join(f.root, "implement.mjs"), "utf8") + `
      const fs=await import('node:fs');fs.mkdirSync('node_modules/local',{recursive:true});
      if(!fs.existsSync('node_modules/local/index.mjs'))fs.writeFileSync('node_modules/local/index.mjs','export default 73;');
      if(!fs.existsSync('node_modules/cwd'))fs.writeFileSync('node_modules/cwd',process.cwd());
      if(fs.readFileSync('node_modules/cwd','utf8')!==process.cwd())throw Error('lost cwd');
    `);
    await writeFile(join(f.root, "verify.mjs"), `import dep from './node_modules/local/index.mjs';if(dep!==73)throw Error('dependency');console.log('node-cwd='+process.cwd());\n` + await readFile(join(f.root, "verify.mjs"), "utf8"));
    await writeFile(join(f.root, "integrate.mjs"), `import dep from './node_modules/local/index.mjs';import assert from 'node:assert/strict';import fs from 'node:fs';assert.equal(dep,73);assert.equal(fs.readFileSync('node_modules/cwd','utf8'),process.cwd());assert.equal(fs.readFileSync('a.txt','utf8'),'a\\n');console.log('node-cwd='+process.cwd());if(fs.existsSync('x.txt')){assert.equal(fs.readFileSync('x.txt','utf8'),'x\\n');if(!fs.existsSync('node_modules/prefix-failed')){fs.writeFileSync('node_modules/prefix-failed','retained');process.exit(19);}}`);
    f.git("add", "."); f.git("commit", "-m", "test: all node commands consume worker dependency");
    const input = planInput("all-node-steps", ["a", "x"]); input.workItems[1].dependsOn = []; input.integration.strategy = "dependency_order";
    for (const check of [...input.integration.prefixCommands, ...input.integration.finalCommands]) check.argv = [process.execPath, "integrate.mjs", check.id];
    const plan = await f.call("dag_plan_save", input), initial = await f.call("dag_run_start", { selection: selectorV2(plan), authority: { ...authority(["a", "x"]), maxConcurrency: 2 } });
    const runId = initial.runId;
    await finish(f, runId, "a", 1, false); let run = await finish(f, runId, "x", 1, false);
    const original = run.nodes.x.lifecycle.candidate, workspace = await f.service().workers.workspace(run.nodes.x.reservation), cwd = workspace.cwd;
    const git = (...args) => execFileSync("git", args, { cwd, env: gitEnvironmentV2(), encoding: "utf8" }).trim();
    assert.equal(git("ls-tree", "--name-only", original.tree, "a.txt"), "", "candidate intentionally lacks predecessor");
    const roots = f.git("worktree", "list", "--porcelain");
    await f.call("dag_integrate", { runId, itemId: "a", generation: 1, candidate: run.nodes.a.lifecycle.candidate });
    // Same common excludes apply to target and node; tracked predecessor remains tracked.
    await writeFile(join(f.root, ".git/info/exclude"), "a.txt\n");
    await writeFile(join(cwd, "a.txt"), "IGNORED USER BYTES\n");
    const integrate = generation => f.call("dag_integrate", { runId, itemId: "x", generation, candidate: original });
    await assert.rejects(integrate(1), /GIT_NODE_PATH_COLLISION/);
    assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "IGNORED USER BYTES\n"); assert.equal(git("rev-parse", "HEAD"), original.commit);
    await (await import('node:fs/promises')).rename(join(cwd, "a.txt"), join(cwd, "node_modules/user-a.txt"));
    await assert.rejects(integrate(1), /GIT_CHECK_NONPASS/);
    run = (await f.service().read()).run; const first = run.gitOperations.at(-1), failed = await f.service().runner.read(first.checks[0]);
    assert.equal(failed.exitCode, 19); assert(failed.stdout.includes(cwd)); assert.notEqual(first.proposal.tree, original.tree);
    assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "a\n");
    await f.call("dag_close_git_operation", { runId, operationId: first.operationId });
    assert.equal(git("rev-parse", "HEAD"), original.commit); await assert.rejects(readFile(join(cwd, "a.txt")), { code: "ENOENT" });
    await f.call("dag_replace_worker", { runId, itemId: "x", generation: 1, completionId: run.nodes.x.reservation.completion.completionId });
    run = await finish(f, runId, "x", 2, false);
    // Emulate a checkout interrupted after only some own effects: original HEAD,
    // proposal index, and missing proposal-added path, after real Git extinction.
    f.service().git.options.failpoint = async (point, op) => { if (point === "node-switching-exited") {
      const nested = join(cwd, "node_modules/local"); execFileSync("git", ["init"], { cwd: nested, stdio: "ignore" });
      await assert.rejects(f.handles.workerManager.launch({ task: "must not mutate", cwd: nested, launchKey: "late-integration-worker" }, f.ctx), /NODE_WORKSPACE_OVERLAP/);
      await assert.rejects(f.service().runner.ensure(op.checks[0]), /CURRENT_EXECUTION_INTENT_REQUIRED/);
      await assert.rejects(f.call("dag_replace_worker", { runId, itemId: "x", generation: 2, completionId: run.nodes.x.reservation.completion.completionId }), /NODE_WORKSPACE_REPAIR_BLOCKED/);
      await assert.rejects(f.handles.workerManager.cleanupOwnedWorktreeForBinding(run.nodes.x.reservation.binding, { launchKey: run.nodes.x.reservation.operationId, effectId: "late-integration-cleanup", requestHash: canonicalHash("late-cleanup") }), /NODE_WORKSPACE_CLEANUP_RELINQUISHED/);
      git("update-ref", "--no-deref", "HEAD", original.commit); await rm(join(cwd, "a.txt")); throw Error("partial transition boundary");
    } };
    await assert.rejects(integrate(2), /partial transition boundary/); await f.reload();
    let action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_close_git_operation"); assert(action);
    // Unknown tracked tampering is never attributed to our partial checkout.
    await writeFile(join(cwd, "x.txt"), "USER SOURCE EDIT\n");
    await assert.rejects(f.call(action.tool, { runId, operationId: action.operationId }), /PARTIAL_SOURCE_DRIFT/);
    assert.equal(await readFile(join(cwd, "x.txt"), "utf8"), "USER SOURCE EDIT\n");
    await writeFile(join(cwd, "x.txt"), "x\n");
    await f.call(action.tool, { runId, operationId: action.operationId });
    assert.equal(git("rev-parse", "HEAD"), original.commit); assert.equal(git("write-tree"), original.tree);
    run = (await f.service().read()).run;
    await f.call("dag_replace_worker", { runId, itemId: "x", generation: 2, completionId: run.nodes.x.reservation.completion.completionId });
    run = await finish(f, runId, "x", 3, false);
    f.service().git.options.failpoint = async point => { if (point === "node-restoring-exited") throw Error("restoration acknowledgement lost"); };
    await assert.rejects(integrate(3), /restoration acknowledgement lost/);
    assert.equal(f.git("rev-parse", "HEAD"), run.nodes.a.integration.target.commit, "no target update before settled restoration");
    await f.reload(); run = await integrate(3); assert.equal(run.status, "complete");
    assert.deepEqual(await f.service().runner.read(first.checks[0]), failed, "failure history unchanged");
    const snapshot = await f.service().runtime.store.read();
    for (const op of run.gitOperations) for (const request of op.checks) {
      const job = snapshot.executions[request.id]; if (!job) continue;
      assert.equal(job.workspace, op.workspace.node.cwd); assert.deepEqual(job.request.candidate, op.proposal);
      assert(job.result.stdout.includes('node-cwd=' + job.workspace));
    }
    for (const op of run.gitOperations) assert.equal(op.workspace.phase, "restored");
    assert.equal(await readFile(join(cwd, "node_modules/user-a.txt"), "utf8"), "IGNORED USER BYTES\n");
    assert.equal(git("rev-parse", "HEAD"), original.commit); assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 2);
    assert.equal(f.git("worktree", "list", "--porcelain").split('\n').filter(l => l.startsWith('worktree ')).length, roots.split('\n').filter(l => l.startsWith('worktree ')).length);
  } finally { await f.cleanup(); }
});
test("nested Git dependency launches are fenced during a real node check through aliases without mutation", async () => {
  const f = await fixture("nested-check-fence"); let pending;
  const release = join(f.root, ".ai/nested-release"), started = join(f.root, ".ai/nested-started");
  try {
    const input = planInput("nested-check-fence", ["a"]);
    input.workItems[0].lifecycle.checks[0].procedure.argv = [process.execPath, "-e", `const fs=require('node:fs');fs.writeFileSync(${JSON.stringify(started)},'started');const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)}))clearInterval(timer)},20)`];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 1 });
    const terminal = await waitTerminal(f, run.runId, "a");
    const current = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 1, completionId: terminal.completionId });
    const product = f.service(), manager = f.handles.workerManager, workspace = await product.workers.workspace(current.nodes.a.reservation);
    const nested = join(workspace.cwd, ".ai/dependency"), alias = join(f.root, ".ai/nested-alias"), rootAlias = join(f.root, ".ai/owner-alias");
    await mkdir(join(nested, "subdir"), { recursive: true }); execFileSync("git", ["init"], { cwd: nested, stdio: "ignore" });
    await writeFile(join(nested, "implement.mjs"), "import {writeFileSync} from 'node:fs';writeFileSync('cache','concurrent mutation');");
    await symlink(nested, alias); await symlink(workspace.cwd, rootAlias);
    const prepared = await product.runtime.prepareCheck(await product.mutation(run.runId), "a", 1, "f1", undefined, workspace);
    const request = prepared.nodes.a.lifecycle.executions[0].request;
    pending = product.runner.ensure(request); pending.catch(() => {});
    for (let i = 0; ; i++) { try { await readFile(started); break; } catch (e) { assert(i < 500); await delay(20); } }
    const before = await manager.store.load(), bytes = await tree(nested);
    for (const [i, cwd] of [join(workspace.cwd, ".ai"), nested, join(nested, "subdir"), alias, join(alias, "subdir"), join(rootAlias, ".ai/dependency"), f.root].entries()) {
      await assert.rejects(manager.launch({ task: 'Item: {"id":"nested","context":[]}', cwd, launchKey: `nested-rejected-${i}` }, f.ctx), /NODE_WORKSPACE_(LAUNCH_RELINQUISHED|OVERLAP)/);
    }
    assert.deepEqual((await manager.store.load()).launchRecords, before.launchRecords);
    assert.deepEqual(await tree(nested), bytes, "rejected nested workers must not write ignored dependency bytes");
    assert.equal((await product.runtime.store.read()).executions[request.id].status, "running");
    await writeFile(release, "release"); await pending;
    assert.equal((await product.runner.read(request)).disposition, "PASS", "ordinary nested Git dependencies remain allowed");
  } finally { await writeFile(release, "release"); await pending?.catch(() => {}); await f.cleanup(); }
});
for (const owner of ["ancestor", "nested"]) test(`nested Git live worker fences adoption and cleanup; ${owner} ownership excludes overlaps`, async () => {
  const f = await fixture(`nested-adoption-${owner}`); let other, nestedWorkerId, barrier;
  try {
    const manager = f.handles.workerManager, launchKey = "legacy/a/1";
    const identity = await manager.launchOwnedAttempt({ workerId: "legacy-a", launchKey, expectedAttemptNumber: 1, configRequestHash: canonicalHash("legacy"), explicitDispatchRecovery: true,
      baseCommit: f.git("rev-parse", "HEAD"), worktreeKey: "legacy-a", task: 'Item: {"id":"a","context":[]}' }, f.ctx);
    const bindingKeys = ["workerStorageId", "launchOwnerSessionId", "workerId", "attemptNumber", "attemptNonce", "configHash"];
    const binding = Object.fromEntries(bindingKeys.map(k => [k, identity[k]]));
    for (let i = 0; !await manager.terminalResultForBinding(binding, { reconcile: true }); i++) { assert(i < 1000); await delay(20); }
    const exact = await manager.inspectBindingReadOnly(binding), cwd = exact.worker.cwd, nested = join(cwd, ".ai/dependency");
    const config = await readFile(join(f.root, exact.attempt.configPath), "utf8"), evidence = await manager.terminalResultForBinding(binding, { evidence: true }), result = await readFile(evidence.resultPath, "utf8");
    await mkdir(nested, { recursive: true }); execFileSync("git", ["init"], { cwd: nested, stdio: "ignore" });
    other = new WorkerManager(f.pi, { piCliPath: resolve("scripts/fixtures/product-worker-rpc.mjs"), autoRecoverOwned: false, watchIntervalMs: 20 });
    await other.attach({ ...f.ctx, sessionManager: { ...f.ctx.sessionManager, getSessionId: () => "nested-binding-store" } });
    const approval = await other.approveDisposableWorkingRoot(nested);
    barrier = await publicationBarrier(f.root);
    const saved = Object.fromEntries(Object.keys(barrier.env).map(key => [key, process.env[key]]));
    let launched;
    try {
      Object.assign(process.env, barrier.env);
      launched = await other.launch({ cwd: nested, launchKey: "nested/a/1", disposableRootToken: approval.disposableRootToken, task: 'Item: {"id":"nested","context":["wait-for-cancel"]}' }, other.context);
    } finally { for (const [key, value] of Object.entries(saved)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } }
    nestedWorkerId = launched.workerId;
    const nestedIdentity = await other.attemptIdentityByLaunchKey("nested/a/1");
    const nestedBinding = Object.fromEntries(bindingKeys.map(k => [k, nestedIdentity[k]]));
    const cleanup = { launchKey, effectId: "ancestor-cleanup", requestHash: canonicalHash("ancestor-cleanup") };
    await assert.rejects(manager.relinquishNodeWorkspace(binding, "legacy/a"), /WORKSPACE_WORKER_SETTLEMENT_REQUIRED/);
    await assert.rejects(manager.cleanupOwnedWorktreeForBinding(binding, cleanup), /WORKSPACE_WORKER_SETTLEMENT_REQUIRED/);
    assert.equal((await manager.store.load()).worktreeCleanupIntents?.length ?? 0, 0);
    await other.cancel(launched.workerId, "settle nested worker");
    const publication = await barrier.reached;
    console.log("nested terminal publication barrier", JSON.stringify(publication));
    assert.equal(publication.stat.isFile, true); assert.equal(publication.stat.isSymbolicLink, false);
    assert.equal(publication.stat.dev, (await lstat(join(publication.path, ".."))).dev);
    assert.equal(publication.canonical, publication.path); assert(publication.stat.size < 256 * 1024);
    // Exercise the original direct reconciliation while the actual supervisor is
    // stopped at directory fsync, not a polling helper that swallows errors.
    const nestedTerminal = await other.terminalResultForBinding(nestedBinding, { reconcile: true });
    assert.equal(nestedTerminal.terminalStatus, "cancelled");
    assert.equal(publication.stat.nlink, 1);
    barrier.release();
    if (owner === "ancestor") {
      assert.equal((await manager.relinquishNodeWorkspace(binding, "legacy/a")).cwd, cwd);
      await assert.rejects(other.cleanupOwnedWorktreeForBinding(nestedBinding, { launchKey: "nested/a/1", effectId: "nested-cleanup", requestHash: canonicalHash("nested-cleanup") }), /NODE_WORKSPACE_(CLEANUP_RELINQUISHED|OVERLAP)/);
      await assert.rejects(other.relinquishNodeWorkspace(nestedBinding, "nested/a"), /NODE_WORKSPACE_OVERLAP/);
    } else {
      assert.equal((await other.relinquishNodeWorkspace(nestedBinding, "nested/a")).cwd, nested);
      await assert.rejects(manager.relinquishNodeWorkspace(binding, "legacy/a"), /NODE_WORKSPACE_OVERLAP/);
      await assert.rejects(manager.cleanupOwnedWorktreeForBinding(binding, cleanup), /NODE_WORKSPACE_(CLEANUP_RELINQUISHED|OVERLAP)/);
    }
    const alias = join(f.root, ".ai/adoption-alias"); await symlink(nested, alias);
    await assert.rejects(manager.launch({ cwd: alias, launchKey: "alias-overlap", task: "must not start" }, f.ctx), /NODE_WORKSPACE_(LAUNCH_RELINQUISHED|OVERLAP)/);
    assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "a\n");
    assert.equal(await readFile(join(f.root, exact.attempt.configPath), "utf8"), config);
    assert.equal(await readFile(evidence.resultPath, "utf8"), result, "handoffs must preserve historical result bytes");
    assert.equal((await manager.store.load()).worktreeCleanupIntents?.length ?? 0, 0);
    assert.equal((await other.store.load()).worktreeCleanupIntents?.length ?? 0, 0);
  } finally { await barrier?.close(); if (nestedWorkerId) await other.cancel(nestedWorkerId, "fixture cleanup").catch(() => {}); await other?.detach(); await f.cleanup(); }
});
for (const point of ["after_launch_reservation", "after_attempt_reservation", "after_config_publication"]) test(`nested Git ${point} atomically prevents ancestor adoption`, async () => {
  const f = await fixture(`nested-race-${point}`); let other, pending, release;
  try {
    const manager = f.handles.workerManager;
    const identity = await manager.launchOwnedAttempt({ workerId: "legacy-race", launchKey: "legacy/race/1", expectedAttemptNumber: 1, configRequestHash: canonicalHash("legacy-race"), explicitDispatchRecovery: true,
      baseCommit: f.git("rev-parse", "HEAD"), worktreeKey: "legacy-race", task: 'Item: {"id":"a","context":[]}' }, f.ctx);
    const binding = Object.fromEntries(["workerStorageId", "launchOwnerSessionId", "workerId", "attemptNumber", "attemptNonce", "configHash"].map(k => [k, identity[k]]));
    for (let i = 0; !await manager.terminalResultForBinding(binding, { reconcile: true }); i++) { assert(i < 1000); await delay(20); }
    const cwd = (await manager.inspectBindingReadOnly(binding)).worker.cwd, nested = join(cwd, ".ai/dependency"), alias = join(f.root, ".ai/race-alias");
    await mkdir(nested, { recursive: true }); execFileSync("git", ["init"], { cwd: nested, stdio: "ignore" }); await symlink(nested, alias);
    let entered = false;
    const barrier = new Promise(resolve => { release = resolve; });
    other = new WorkerManager(f.pi, { piCliPath: resolve("scripts/fixtures/product-worker-rpc.mjs"), autoRecoverOwned: false, watchIntervalMs: 20,
      failpoint: async name => { if (name === point) { entered = true; await barrier; } } });
    await other.attach({ ...f.ctx, sessionManager: { ...f.ctx.sessionManager, getSessionId: () => "nested-race-store" } });
    pending = other.launch({ workerId: "nested-race", cwd: alias, launchKey: "nested/race/1", task: 'Item: {"id":"nested","context":["wait-for-cancel"]}' }, other.context); pending.catch(() => {});
    for (let i = 0; !entered; i++) { assert(i < 1000); await delay(20); }
    await assert.rejects(manager.relinquishNodeWorkspace(binding, "legacy/race"), /WORKSPACE_WORKER_SETTLEMENT_REQUIRED/);
    release(); await pending;
    await assert.rejects(manager.relinquishNodeWorkspace(binding, "legacy/race"), /WORKSPACE_WORKER_SETTLEMENT_REQUIRED/);
    await other.cancel("nested-race", "settle nested race");
    const nestedBinding = await other.attemptIdentityByLaunchKey("nested/race/1");
    for (let i = 0; !await other.terminalResultForBinding(nestedBinding, { reconcile: true }); i++) { assert(i < 1000); await delay(20); }
    assert.equal((await manager.relinquishNodeWorkspace(binding, "legacy/race")).cwd, cwd);
    await assert.rejects(other.launch({ cwd: alias, launchKey: "nested/race/2", task: "must not start" }, other.context), /NODE_WORKSPACE_OVERLAP/);
  } finally { release?.(); await pending?.catch(() => {}); if (other) await other.cancel("nested-race", "fixture cleanup").catch(() => {}); await other?.detach(); await f.cleanup(); }
});
test("legacy exact generation-two root adopts after interrupted handoff; other binding stores cannot clean it", async () => {
  const f = await fixture("legacy-node-adoption"); let other;
  try {
    const plan = await f.call("dag_plan_save", planInput("legacy-node-adoption", ["a"]));
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const product = f.service(), manager = f.handles.workerManager;
    const request = canonicalStringify({ kind: "product_worker_v2", explicitDispatchRecovery: true, repositoryId: plan.repository.repositoryId,
      baseCommit: plan.repository.baselineCommit, sourcePaths: ["project-model/model.json", "spec/delivery/spec.md"], task: `Item: ${JSON.stringify(plan.workItems[0])}` });
    const legacy = { ensure: async reservation => {
      const workerId = `v2-${canonicalHash(reservation.operationId).slice(7)}`;
      const exact = await manager.launchOwnedAttempt({ workerId, launchKey: reservation.operationId, expectedAttemptNumber: 1, configRequestHash: canonicalHash(JSON.parse(request)),
        explicitDispatchRecovery: true, baseCommit: plan.repository.baselineCommit, worktreeKey: workerId, label: workerId, task: JSON.parse(request).task }, f.ctx);
      const { workerStorageId, launchOwnerSessionId, attemptNumber, attemptNonce, configHash } = exact;
      return { workerId, binding: { workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash } };
    } };
    await product.runtime.reserve(await product.mutation(run.runId), "a", 1, request);
    await product.runtime.dispatch(await product.mutation(run.runId), "a", 1, legacy); await waitTerminal(f, run.runId, "a");
    await product.runtime.replace(await product.mutation(run.runId), "a", 1, r => product.workers.settled(r), request);
    await product.runtime.dispatch(await product.mutation(run.runId), "a", 2, legacy);
    const terminal = await waitTerminal(f, run.runId, "a");
    const current = await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation: 2, completionId: terminal.completionId });
    const reservation = current.nodes.a.reservation, exact = await manager.inspectBindingReadOnly(reservation.binding), cwd = exact.worker.cwd;
    const config = await readFile(join(f.root, exact.attempt.configPath), "utf8");
    let once = true; manager.options.failpoint = async point => { if (point === "after_node_workspace_relinquishment" && once) { once = false; throw Error("lost node handoff acknowledgement"); } };
    const action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks"), { tool, ...params } = action;
    await assert.rejects(f.call(tool, params), /lost node handoff acknowledgement/);
    await f.reload(); const checked = await f.call(tool, params);
    assert.equal(checked.nodes.a.lifecycle.executions[0].result.workspace.node.cwd, cwd);
    assert.equal(await readFile(join(f.root, exact.attempt.configPath), "utf8"), config);
    assert.equal(checked.nodes.a.reservation.request, request);
    const adopted = (await f.service().read()).run.nodes.a.workspace;
    assert.equal(adopted.cwd, cwd);
    const approvals = structuredClone((await f.handles.workerManager.store.load()).approvedDisposableRoots);
    let claimObserved = false;
    f.handles.workerManager.options.failpoint = async (point, claim) => {
      if (point !== "after_node_workspace_launch_claim") return;
      claimObserved = true;
      const approval = approvals.find(a => a.path === cwd);
      const { withWorkspaceOwnership, assertWorkspaceLaunch } = await import("../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs");
      await withWorkspaceOwnership(f.root, cwd, owner => {
        assert.equal(owner.borrowedRequestHash, claim.requestHash, "loan type and request must publish atomically with the new launch key");
        assert.throws(() => assertWorkspaceLaunch(owner, claim.operationId, { kind: "approved_disposable" }), /NODE_WORKSPACE_CAPABILITY_MISMATCH/);
      });
      await assert.rejects(f.handles.workerManager.launch({ cwd, launchKey: claim.operationId, task: "old approval cannot borrow a new claim",
        boundConfigRequestHash: claim.requestHash, disposableApprovalId: approval.approvalId }, f.ctx), /NODE_WORKSPACE_CAPABILITY_MISMATCH|approval is missing/);
      throw Error("borrowed claim acknowledgement interrupted");
    };
    await f.call("dag_replace_worker", { runId: run.runId, itemId: "a", generation: 2, completionId: terminal.completionId,
      direction: { task: "Retain the committed correct a.txt and node setup; verify the exact planned oracle. Do not change model or spec.", provenance: "fixture current user direction" } });
    await assert.rejects(f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 3 }), /borrowed claim acknowledgement interrupted/);
    assert(claimObserved);
    f.handles.workerManager.options.failpoint = undefined;
    await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation: 3 });
    const finished = await finish(f, run.runId, "a", 3);
    assert.equal(finished.status, "complete");
    assert.equal(canonicalStringify(finished.nodes.a.workspace), canonicalStringify(adopted));
    const newExact = await f.handles.workerManager.inspectBindingReadOnly(finished.nodes.a.reservation.binding);
    assert.equal(newExact.worker.cwd, cwd); assert.equal(newExact.worker.normalizedRequest.workingRoot.kind, "borrowed_node");
    assert.deepEqual((await f.handles.workerManager.store.load()).approvedDisposableRoots, approvals, "new generation borrows an adopted legacy root without another approval handoff");
    assert.equal(await readFile(join(f.root, exact.attempt.configPath), "utf8"), config);
    // Exact binding access transfers storage ownership. Exercise the secondary
    // owner only after the original owner's writes, then prove it is fenced.
    other = new WorkerManager(f.pi, { piCliPath: resolve("scripts/fixtures/product-worker-rpc.mjs"), autoRecoverOwned: false });
    await other.attach({ ...f.ctx, sessionManager: { ...f.ctx.sessionManager, getSessionId: () => "separate-binding-store" } });
    assert.notEqual(other.store.storageId, reservation.binding.workerStorageId);
    await assert.rejects(other.cleanupOwnedWorktreeForBinding(reservation.binding, { launchKey: reservation.operationId, effectId: "old-store-cleanup", requestHash: canonicalHash("old-store-cleanup") }), /NODE_WORKSPACE_CLEANUP_RELINQUISHED/);
    assert.equal((await other.relinquishNodeWorkspace(finished.nodes.a.reservation.binding, `${run.runId}/a`)).cwd, cwd);
    assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "a\n");
    const retired = f.handles.workerManager;
    await assert.rejects(retired.scan(), /ownership changed/);
    assert.deepEqual(retired.activitySnapshot().stores, [], "transferred node store retracts the retired owner's activity");
    assert(retired.deliveredCompletion, "fixture retains the old owner's unacknowledged delivery");
    await assert.rejects(retired.onAgentSettled(), /ownership changed/);
    assert.equal((await other.store.load()).worktreeCleanupIntents?.length ?? 0, 0);
  } finally { await other?.detach(); await f.cleanup(); }
});
test("two persistent node checks keep separate journals and exclude cleanup/repair until orphan descendants settle", async () => {
  const f = await fixture("node-descendants"); const pending = []; const gates = [];
  try {
    const input = planInput("node-descendants", ["a", "x"]); input.workItems[1].dependsOn = []; input.integration.strategy = "dependency_order";
    for (const item of input.workItems) {
      const started = join(f.root, ".ai", `${item.id}-started`), release = join(f.root, ".ai", `${item.id}-release`); gates.push(release);
      const child = `const fs=require('node:fs');fs.writeFileSync(${JSON.stringify(started)},process.cwd());const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(t);process.exit(0)}},20);`;
      item.lifecycle.checks[0].procedure.argv = [process.execPath, "-e", `require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(child)}],{stdio:'ignore',detached:true}).unref()`];
    }
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: { ...authority(["a", "x"]), maxConcurrency: 2 } });
    const product = f.service(), requests = [], bindings = [];
    for (const id of ["a", "x"]) {
      await f.call("dag_start_work", { runId: run.runId, itemId: id, generation: 1 }); const terminal = await waitTerminal(f, run.runId, id);
      const current = await f.call("dag_record_completion", { runId: run.runId, itemId: id, generation: 1, completionId: terminal.completionId });
      const reservation = current.nodes[id].reservation; bindings.push(reservation);
      const workspace = await product.workers.workspace(reservation);
      const prepared = await product.runtime.prepareCheck(await product.mutation(run.runId), id, 1, "f1", undefined, workspace);
      const request = prepared.nodes[id].lifecycle.executions[0].request; requests.push(request);
      const execution = product.runner.ensure(request); pending.push(execution); execution.catch(() => {});
      for (let pass = 0; ; pass++) {
        try { assert.equal(await readFile(join(f.root, ".ai", `${id}-started`), "utf8"), workspace.cwd); break; }
        catch (error) { if (error.code !== "ENOENT" || pass > 500) throw error; await delay(20); }
      }
    }
    const snapshot = await product.runtime.store.read(), jobs = requests.map(r => snapshot.executions[r.id]);
    assert.notEqual(jobs[0].workspace, jobs[1].workspace); assert.notEqual(jobs[0].protocolDirectory, jobs[1].protocolDirectory);
    for (const [i, job] of jobs.entries()) {
      assert.equal(job.status, "running"); assert.equal(job.result, undefined, "argv leader exit is not subtree extinction");
      assert.equal(JSON.parse(await readFile(join(job.protocolDirectory, "command-process.json"), "utf8")).requestId, requests[i].id);
      const reservation = bindings[i];
      await assert.rejects(f.handles.workerManager.cleanupOwnedWorktreeForBinding(reservation.binding, { launchKey: reservation.operationId, effectId: `race-${i}`, requestHash: canonicalHash(`race-${i}`) }), /NODE_WORKSPACE_CLEANUP_RELINQUISHED/);
      await assert.rejects(product.replaceWorker(run.runId, reservation.itemId, 1, reservation.completion.completionId), /EXECUTION_RECONCILIATION_REQUIRED/);
    }
    for (const gate of gates) await writeFile(gate, "release");
    await Promise.all(pending);
    for (const request of requests) {
      assert.equal((await product.runner.read(request)).disposition, "PASS");
      await product.runtime.recordResult(await product.mutation(run.runId), request.itemId, request.id, product.runner);
    }
    assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 2);
  } finally { for (const gate of gates) await writeFile(gate, "release").catch(() => {}); await Promise.allSettled(pending); await f.cleanup(); }
});
for (const [name, body] of [
  ["bytes", "fs.writeFileSync('a.txt','changed\\n')"],
  ["index", "require('node:child_process').execFileSync('git',['update-index','--assume-unchanged','a.txt'])"],
  ["mode", "fs.chmodSync('a.txt',0o755)"],
  ["untracked", "fs.writeFileSync('unknown.txt','preserve unknown')"],
  ["ignored", "fs.mkdirSync('node_modules',{recursive:true});fs.writeFileSync('node_modules/cache','preserve cache')"],
]) test(`persistent node source integrity retains ${name} without cleaning`, async () => {
  const f = await fixture(`node-integrity-${name}`); try {
    await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\n"); f.git("add", ".gitignore"); f.git("commit", "-m", "test: ignored cache policy");
    const input = planInput(`node-integrity-${name}`, ["a"]); input.workItems[0].lifecycle.checks[0].procedure.argv = [process.execPath, "-e", `const fs=require('node:fs');${body}`];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const selectors = { runId: run.runId, itemId: "a", generation: 1 };
    await f.call("dag_start_work", selectors); const terminal = await waitTerminal(f, run.runId, "a");
    const ready = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    const cwd = (await f.handles.workerManager.inspectBindingReadOnly(ready.nodes.a.reservation.binding)).worker.cwd;
    const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks");
    if (name === "ignored") await f.call(tool, params); else await assert.rejects(f.call(tool, params), /CHECK_NOT_PASSED/);
    const result = (await f.service().read()).run.nodes.a.lifecycle.executions[0].result;
    assert.equal(result.exitCode, 0); assert.equal(result.workspace.node.cwd, cwd); assert.equal(result.workspace.cleanBefore, true);
    assert.equal(result.workspace.cleanAfter, name === "ignored"); assert.equal(result.disposition, name === "ignored" ? "PASS" : "FAIL");
    assert.equal((await readdir(join(f.root, ".ai/worker-roots"))).length, 1);
    if (name === "ignored") assert.equal(await readFile(join(cwd, "node_modules/cache"), "utf8"), "preserve cache");
    else {
      await assert.rejects(f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId }), /NODE_WORKSPACE_REPAIR_BLOCKED/);
      if (name === "untracked") assert.equal(await readFile(join(cwd, "unknown.txt"), "utf8"), "preserve unknown");
      if (name === "bytes") assert.equal(await readFile(join(cwd, "a.txt"), "utf8"), "changed\n");
    }
  } finally { await f.cleanup(); }
});
test("node handoff refuses a live worker and an interrupted pre-adoption cleanup intent", async () => {
  const f = await fixture("handoff-exclusion"); try {
    const manager = f.handles.workerManager, baseCommit = f.git("rev-parse", "HEAD");
    const launch = await manager.launchOwnedAttempt({ workerId: "legacy-handoff", launchKey: "run/a/1", expectedAttemptNumber: 1,
      configRequestHash: canonicalHash("legacy-handoff"), explicitDispatchRecovery: true, baseCommit, worktreeKey: "legacy-handoff",
      task: `Item: ${JSON.stringify({ id: "a", context: ["wait-for-cancel"] })}` }, f.ctx);
    const { workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash } = launch;
    const binding = { workerStorageId, launchOwnerSessionId, workerId, attemptNumber, attemptNonce, configHash };
    await assert.rejects(manager.relinquishNodeWorkspace(binding, "run/a"), /WORKER_SETTLEMENT_REQUIRED/);
    await manager.cancelBinding(binding, "fixture settlement");
    for (let i = 0; ; i++) {
      if (await manager.terminalResultForBinding(binding, { reconcile: true })) break;
      assert(i < 500); await delay(20);
    }
    manager.options.failpoint = async point => { if (point === "after_worktree_cleanup_intent") throw Error("interrupted legacy cleanup intent"); };
    await assert.rejects(manager.cleanupOwnedWorktreeForBinding(binding, { launchKey: "run/a/1", effectId: "legacy-cleanup", requestHash: canonicalHash("legacy-cleanup") }), /interrupted legacy cleanup intent/);
    await assert.rejects(manager.relinquishNodeWorkspace(binding, "run/a"), /WORKSPACE_CLEANUP_INTENT_EXISTS/);
    const exact = await manager.inspectBindingReadOnly(binding); assert.equal(await readFile(join(exact.worker.cwd, "verify.mjs"), "utf8"), await readFile(join(f.root, "verify.mjs"), "utf8"));
    assert.equal((await manager.store.load()).worktreeCleanupIntents[0].state, "intended");
  } finally { await f.cleanup(); }
});
test("registered Git validation recovery permits explicit generation four after native closure with full fresh checks", async () => {
  const f = await fixture("git-command-owner"); try {
    const plan = await f.call("dag_plan_save", planInput("git-command-owner", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    for (let generation = 1; generation <= 2; generation++) {
      await f.call("dag_start_work", { runId: run.runId, itemId: "a", generation });
      const terminal = await waitTerminal(f, run.runId, "a");
      await f.call("dag_record_completion", { runId: run.runId, itemId: "a", generation, completionId: terminal.completionId });
      await f.call("dag_replace_worker", { runId: run.runId, itemId: "a", generation, completionId: terminal.completionId });
    }
    const atCap = await readFile(f.service().runtime.store.statePath, "utf8");
    await f.reload(); assert.equal(await readFile(f.service().runtime.store.statePath, "utf8"), atCap);
    const ready = await finish(f, run.runId, "a", 3, false), candidate = ready.nodes.a.lifecycle.candidate;
    const workspace = canonicalStringify(ready.nodes.a.workspace), oldArchives = ready.nodes.a.archivedReservations.map(canonicalStringify);
    assert.equal(ready.nodes.a.retries.find(r => r.dimension === "replacement").count, 2);
    f.service().git.options.failpoint = async point => { if (point === "composed") throw Error("pause at real composed validation intent"); };
    await assert.rejects(f.call("dag_integrate", { runId: run.runId, itemId: "a", generation: 3, candidate }), /real composed/);
    const pending = (await f.service().read()).run, op = pending.gitOperations[0]; assert.equal(op.phase, "composed");
    const replaceParams = { runId: run.runId, itemId: "a", generation: 3, completionId: pending.nodes.a.reservation.completion.completionId };
    await assert.rejects(f.call("dag_replace_worker", replaceParams), /NODE_WORKSPACE_EXECUTION_BUSY|UNRESOLVED_GIT_OPERATION/);
    loseCommandResultPublication(f.root, op.checks[0]);
    let action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_recover_execution"); assert.equal(action.executionId, op.checks[0].id);
    let { tool, ...params } = action; const observed = await f.call(tool, params); assert.equal(observed.execution.disposition, "BLOCKED"); assert.equal(observed.execution.exitCode, 0);
    action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_close_git_operation"); ({ tool, ...params } = action); const closed = await f.call(tool, params);
    assert.equal(closed.gitOperations[0].phase, "closed"); assert.equal(closed.nodes.a.retries.find(r => r.dimension === "integration").count, 1);
    action = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_replace_worker"); ({ tool, ...params } = action); const replaced = await f.call(tool, params);
    assert.equal(replaced.nodes.a.generation, 4); const request = JSON.parse(replaced.nodes.a.reservation.request); assert.equal(request.baseCommit, candidate.commit); assert.match(request.task, /Executor died without a durable lifecycle result/);
    assert.equal(replaced.nodes.a.retries.find(r => r.dimension === "replacement").count, 3);
    assert.equal(canonicalStringify(replaced.nodes.a.workspace), workspace);
    assert.deepEqual(replaced.nodes.a.archivedReservations.slice(0, 2).map(canonicalStringify), oldArchives);
    assert.equal(f.git("rev-parse", "HEAD"), plan.repository.baselineCommit); assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 3, "replacement is inert until explicit guarded dispatch");
    const oldResults = replaced.nodes.a.lifecycle.executions.map(e => canonicalStringify(e.result));
    const closedOperation = canonicalStringify(replaced.gitOperations[0]);
    await f.reload(); const completed = await finish(f, run.runId, "a", 4);
    assert.equal(completed.status, "complete"); assert.equal(canonicalStringify(completed.nodes.a.workspace), workspace);
    assert.equal(canonicalStringify(completed.gitOperations[0]), closedOperation);
    assert.deepEqual(completed.nodes.a.lifecycle.executions.slice(0, oldResults.length).map(e => canonicalStringify(e.result)), oldResults);
    assert.equal(completed.nodes.a.lifecycle.executions.filter(e => e.status === "observed" && e.request.generation === 4).length, 13);
    assert.equal(completed.nodes.a.retries.find(r => r.dimension === "integration").count, 1);
  } finally { await f.cleanup(); }
});
test("historical expired V2 reservations and evidence survive reload, direction, replacement and native landing", async () => {
  const f = await fixture("historical-deadline"); try {
    const input = planInput("historical-deadline", ["repairable"]);
    for (const check of [...input.integration.prefixCommands, ...input.integration.finalCommands]) check.argv = [process.execPath, "verify.mjs", "repairable", check.id];
    const plan = await f.call("dag_plan_save", input);
    const started = await historicalStartV2(f.service().runtime, plan, f.ctx.sessionManager.getSessionId(), Date.now() + 60000);
    const selectors = { runId: started.runId, itemId: "repairable", generation: 1 };
    const oldRequest = canonicalStringify({ kind: "product_worker_v2", explicitDispatchRecovery: true, repositoryId: plan.repository.repositoryId, baseCommit: plan.repository.baselineCommit,
      sourcePaths: ["project-model/model.json", "spec/delivery/spec.md"], task: `OBSOLETE: stop at first failure\nItem: ${JSON.stringify(plan.workItems[0])}` });
    await f.service().runtime.reserve(await f.service().mutation(started.runId), "repairable", 1, oldRequest);
    await f.call("dag_start_work", selectors);
    const terminal = await waitTerminal(f, started.runId, "repairable");
    await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    let oldCheck;
    await f.service().runtime.store.transaction(async (s, publish) => {
      const run = s.runs[started.runId];
      oldCheck = historicalExecutionRequestV2(run, "repairable", plan.workItems[0].lifecycle.checks[0]);
      run.nodes.repairable.lifecycle.executions.push({ request: oldCheck, status: "intent" });
      run.revision++; await publish();
    });
    const { tool, ...params } = (await f.call("dag_next_action", {})).actions.find(a => a.tool === "dag_run_checks");
    await assert.rejects(f.call(tool, params), /CHECK_NOT_PASSED/);
    await f.call("dag_pause", { runId: started.runId });
    const statePath = join(f.root, ".ai/dag-workflow-v2/state.json"), bytes = await readFile(statePath, "utf8");
    const prior = JSON.parse(bytes), run = prior.runs[started.runId], reservation = run.nodes.repairable.reservation;
    const oldEvidence = prior.executions[oldCheck.id], evidenceHash = canonicalHash(oldEvidence);
    assert.deepEqual(auditSnapshotV2(prior), prior);
    const malformed = structuredClone(prior);
    malformed.runs[started.runId].start.authority[removedAuthorityFieldV2] = null;
    assert.throws(() => auditSnapshotV2(malformed), /INVALID_HISTORICAL_AUTHORITY/);
    const unknown = structuredClone(prior);
    unknown.runs[started.runId].start.authority.unrecognized = true;
    assert.throws(() => auditSnapshotV2(unknown), /INVALID_V2/);
    const forged = structuredClone(prior);
    delete forged.executions[oldCheck.id].result.request.authority[removedAuthorityFieldV2];
    assert.throws(() => auditSnapshotV2(forged), /RESULT_REQUEST_MISMATCH/);
    assert(oldEvidence.result.startedAt < historicalDeadlineV2(run), "actual old command ran before its historical deadline");
    assert.equal(oldEvidence.result.disposition, "FAIL");
    const exact = await f.handles.workerManager.inspectBindingReadOnly(reservation.binding), configHash = exact.worker.normalizedRequest.boundConfigRequestHash;
    assert.equal(configHash, canonicalHash(JSON.parse(oldRequest)));
    const candidate = structuredClone(run.nodes.repairable.lifecycle.candidate);
    await delay(Math.max(0, historicalDeadlineV2(run) - Date.now() + 5));
    await f.reload();
    assert.equal(await readFile(statePath, "utf8"), bytes, "expired historical load is byte-for-byte read-only");
    assert.equal((await f.call("dag_next_action", {})).run.status, "paused");
    const direction = { task: "CURRENT: repair the retained repairable.txt to its exact ID plus newline and run all required checks. Ordinary scoped setup is allowed.", provenance: "user-message:no-run-deadline" };
    await f.call("dag_set_worker_direction", { ...selectors, direction });
    const replacement = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
    assert.equal(replacement.status, "paused");
    assert.equal(JSON.parse(replacement.nodes.repairable.reservation.request).baseCommit, candidate.commit);
    assert.deepEqual(replacement.nodes.repairable.archivedReservations[0], reservation);
    await f.call("dag_resume", { runId: started.runId });
    const completed = await finish(f, started.runId, "repairable", 2);
    assert.equal(completed.status, "complete");
    const after = await f.service().runtime.store.read();
    assert.equal(canonicalStringify(after.executions[oldCheck.id]), canonicalStringify(oldEvidence));
    assert.equal(canonicalHash(after.executions[oldCheck.id]), evidenceHash);
    assert.equal(canonicalStringify(completed.start), canonicalStringify(run.start));
    assert.equal(canonicalStringify(completed.nodes.repairable.archivedReservations[0]), canonicalStringify(reservation));
    assert.equal((await f.handles.workerManager.inspectBindingReadOnly(reservation.binding)).worker.normalizedRequest.boundConfigRequestHash, configHash);
    const fresh = completed.nodes.repairable.lifecycle.executions.filter(e => e.request.generation === 2);
    assert(fresh.length && fresh.every(e => e.result?.disposition === "PASS"));
    assert(!JSON.stringify(fresh).includes(removedAuthorityFieldV2));
    assert(!JSON.stringify(completed.gitOperations).includes(removedAuthorityFieldV2));
    await f.reload(); assert.equal((await f.service().read()).run.status, "complete");
  } finally { await f.cleanup(); }
});
for (const diagnosticsMode of ["present", "null", "omitted"]) test(`registered recovery retains needs_attention work, refreshes legacy direction, and lands only after real checks (${diagnosticsMode} diagnostics)`, async () => {
  const f = await fixture("setup-recovery"); try {
    const implement = await readFile(join(f.root, "implement.mjs"), "utf8");
    await writeFile(join(f.root, "implement.mjs"), implement + `\nif(id==='setup-repair'){if(process.argv[3]==='repair'){if(!existsSync('setup-repair.txt'))throw Error('retained work missing');writeFileSync('setup-dependency.mjs','export default true;');execFileSync('git',['add','setup-dependency.mjs']);execFileSync('git',['-c','user.name=fixture','-c','user.email=fixture@example.invalid','commit','-m','restore setup']);}await import('./setup-dependency.mjs');}`);
    f.git("add", "."); f.git("commit", "-m", "fixture: actual missing setup module");
    const input = planInput("setup-recovery", ["setup-repair"]);
    input.workItems[0].context = ["OBSOLETE: stop at first failure; never install"];
    for (const check of [...input.integration.prefixCommands, ...input.integration.finalCommands]) check.argv = [process.execPath, "verify.mjs", "setup-repair", check.id];
    const plan = await f.call("dag_plan_save", input), started = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["setup-repair"]) });
    const selectors = { runId: started.runId, itemId: "setup-repair", generation: 1 };
    // Materialize an authentic pre-renderer reservation without rewriting history.
    const oldRequest = canonicalStringify({ kind: "product_worker_v2", explicitDispatchRecovery: true, repositoryId: plan.repository.repositoryId, baseCommit: plan.repository.baselineCommit,
      sourcePaths: ["project-model/model.json", "spec/delivery/spec.md"], task: `OBSOLETE: stop at first failure; never install\nItem: ${JSON.stringify(plan.workItems[0])}` });
    await f.service().runtime.reserve(await f.service().mutation(started.runId), "setup-repair", 1, oldRequest);
    await f.call("dag_start_work", selectors);
    const terminal = await waitTerminal(f, started.runId, "setup-repair"); assert.equal(terminal.terminalStatus, "needs_attention");
    const oldBinding = (await f.service().read()).run.nodes["setup-repair"].reservation.binding;
    const manager = f.handles.workerManager, readTerminal = manager.terminalResultForBinding.bind(manager);
    const oldEvidence = await readTerminal(oldBinding, { evidence: true });
    // Only this disposable fixture's report shape changes; keep a valid durable
    // hash and the real supervisor, request, attempt and completion identities.
    if (diagnosticsMode !== "present") {
      const result = JSON.parse(await readFile(oldEvidence.resultPath, "utf8"));
      if (diagnosticsMode === "omitted") delete result.diagnostics; else result.diagnostics = null;
      const historical = withResultHash(result); assertTerminalResult(historical);
      await writeFile(oldEvidence.resultPath, JSON.stringify(historical));
    }
    let run = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    const binding = run.nodes["setup-repair"].reservation.binding;
    const exact = await f.handles.workerManager.inspectBindingReadOnly(binding);
    const kept = execFileSync("git", ["rev-parse", "HEAD"], { cwd: exact.worker.cwd, encoding: "utf8" }).trim();
    assert.notEqual(kept, plan.repository.baselineCommit); assert.equal(run.nodes["setup-repair"].lifecycle.candidateReady, false);
    await f.call("dag_pause", { runId: run.runId });
    await assert.rejects(f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId }), /FRESH_WORKER_DIRECTION_REQUIRED/);
    const direction = { task: "CURRENT: restore ordinary dependencies and repair build failures. Complete setup-repair.txt with its exact ID and newline. Needed feature dependencies are allowed within scope. Run actual checks; stop only for machinery blockers or genuine user decisions.", provenance: "user-message:setup-recovery-current" };
    const before = await readFile(join(f.root, ".ai/dag-workflow-v2/state.json"), "utf8");
    // Reproduce the pre-evidence manager: async exact lookup ignores the new
    // option and returns only completionId/terminalStatus, even for a valid report.
    manager.terminalResultForBinding = async (binding, options = {}) => readTerminal(binding, { reconcile: options.reconcile });
    await assert.rejects(f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId, direction }), /WORKER_EVIDENCE_PROJECTION_REQUIRED/);
    assert.equal(await readFile(join(f.root, ".ai/dag-workflow-v2/state.json"), "utf8"), before);
    let evidenceReads = 0;
    manager.terminalResultForBinding = async (binding, options) => {
      const result = await readTerminal(binding, options);
      if (options?.evidence) {
        evidenceReads++;
        assert.equal(result.reportStatus, "valid"); assert.equal(result.terminalStatus, "needs_attention");
        assert.match(result.report.details, /ERR_MODULE_NOT_FOUND/);
        if (diagnosticsMode !== "present") assert.equal(result.diagnostics, null);
        // Also accept an omitted optional field in an otherwise full projection.
        if (diagnosticsMode === "omitted") delete result.diagnostics;
      }
      return result;
    };
    run = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId, direction });
    assert.equal(evidenceReads, 1); manager.terminalResultForBinding = readTerminal;
    const request = JSON.parse(run.nodes["setup-repair"].reservation.request);
    const repair = JSON.parse(request.task.match(/^Repair observations: (.+)$/m)[1]);
    assert.equal(repair.worker.resultHash, (await readTerminal(binding, { evidence: true })).resultHash);
    if (diagnosticsMode !== "present") assert.equal(repair.worker.diagnostics, null);
    assert.notEqual(request.baseCommit, "f".repeat(40), "reported arbitrary candidate is never trusted");
    assert.equal(request.baseCommit, kept); assert.deepEqual(request.direction, direction);
    assert(!request.task.includes("OBSOLETE:")); assert.match(request.task, /ERR_MODULE_NOT_FOUND/); assert.match(request.task, /useful committed implementation/);
    assert.equal(run.nodes["setup-repair"].archivedReservations[0].request, oldRequest);
    assert.equal(run.nodes["setup-repair"].lifecycle.candidateReady, false); assert.equal(run.nodes["setup-repair"].lifecycle.ready, false);
    await assert.rejects(f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId }), /STALE_GENERATION/);
    await f.reload(); await f.call("dag_resume", { runId: run.runId });
    await assert.rejects(f.call("dag_start_work", { ...selectors, generation: 2, direction: { ...direction, task: "conflicting replay" } }), /RESERVATION_REQUEST_CONFLICT/);
    run = await finish(f, run.runId, "setup-repair", 2);
    assert.equal(run.status, "complete"); assert.equal(await readFile(join(f.root, "setup-dependency.mjs"), "utf8"), "export default true;");
    assert.equal((await f.handles.workerManager.store.load()).launchRecords.length, 2);
    assert.equal(run.nodes["setup-repair"].archivedReservations[0].request, oldRequest);
  } finally { await f.cleanup(); }
});
test("registered recovery quarantined previous-round observations are not current", async () => {
const f=await fixture("review-old-round");
try {
 const input=planInput("review-old-round",["a"]);
 input.workItems[0].lifecycle.checks[0].procedure.argv=[process.execPath,"-e","process.stderr.write('old failure');process.exit(17)"];
 const plan=await f.call("dag_plan_save",input);
 const run=await f.call("dag_run_start",{selection:selectorV2(plan),authority:authority(["a"])});
 const selectors={runId:run.runId,itemId:"a",generation:1};
 await f.call("dag_start_work",selectors);const terminal=await waitTerminal(f,run.runId,"a");
 await f.call("dag_record_completion",{...selectors,completionId:terminal.completionId});
 const {tool,...params}=(await f.call("dag_next_action",{})).actions.find(a=>a.tool==="dag_run_checks");
 await assert.rejects(f.call(tool,params),/CHECK_NOT_PASSED/);
 const failed=(await f.service().read()).run.nodes.a.lifecycle.executions.find(e=>e.result?.exitCode===17);
 const retried=await f.call("dag_retry",{...selectors,executionId:failed.request.id});
 assert.equal(retried.nodes.a.lifecycle.executions.find(e=>e.request.id===failed.request.id).status,"quarantined");
 const action=(await f.call("dag_next_action",{})).actions.find(a=>a.tool==="dag_run_checks");
 const {tool:retryTool,...retryParams}=action;
 await assert.rejects(f.call(retryTool,retryParams),/CHECK_NOT_PASSED/);
 const currentFailed=(await f.service().read()).run.nodes.a.lifecycle.executions.find(e=>e.status!=="quarantined" && e.result?.exitCode===17);
 const next=await f.call("dag_replace_worker",{...selectors,completionId:terminal.completionId});
 const task=JSON.parse(next.nodes.a.reservation.request).task;
 const repair=JSON.parse(task.match(/^Repair observations: (.+)$/m)[1]);
 const observation=repair.observations.find(e=>e.executionId===failed.request.id);
 assert.equal(observation.current,false);
 assert.equal(observation.result.exitCode,17);
 assert.equal(repair.observations.find(e=>e.executionId===currentFailed.request.id).current,true);
}finally{await f.cleanup();}

});
test("registered recovery identity mismatch blocks replacement without snapshot or history mutation", async () => {
  const f = await fixture("review-request-identity");
  const manager = f.handles.workerManager, original = manager.inspectBindingReadOnly.bind(manager);
  try {
    const plan = await f.call("dag_plan_save", planInput("review-request-identity", ["a"]));
    const run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const selectors = { runId: run.runId, itemId: "a", generation: 1 };
    await f.call("dag_start_work", selectors);
    const terminal = await waitTerminal(f, run.runId, "a");
    const before = await readFile(join(f.root, ".ai/dag-workflow-v2/state.json"), "utf8");
    // Only the inspection view is faulted; terminal evidence remains authentic.
    manager.inspectBindingReadOnly = async (...args) => {
      const exact = await original(...args);
      return { ...exact, worker: { ...exact.worker, normalizedRequest: { ...exact.worker.normalizedRequest, boundConfigRequestHash: canonicalHash("different request") } } };
    };
    await assert.rejects(f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId }), /IMMUTABLE_WORKER_REQUEST_MISMATCH/);
    assert.equal(await readFile(join(f.root, ".ai/dag-workflow-v2/state.json"), "utf8"), before, "no generation, archive, revision, or replacement history mutation");
    manager.inspectBindingReadOnly = original;
    const next = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
    assert.equal(next.nodes.a.generation, 2);
  } finally { manager.inspectBindingReadOnly = original; await f.cleanup(); }
});
test("ignored build artifacts admit the same rejected completion after resume without replacement and land", async () => {
  const f = await fixture("ignored-intake");
  try {
    await writeFile(join(f.root, ".gitignore"), ".ai/\nnode_modules/\ndist/\n.svelte-kit/\ncodedb.snapshot\n");
    await writeFile(join(f.root, "implement.mjs"), await readFile(join(f.root, "implement.mjs"), "utf8") + `\nconst fs=await import('node:fs'); for(const path of ['node_modules/pkg/index.js','dist/index.html','.svelte-kit/output/client.js','codedb.snapshot']){fs.mkdirSync((await import('node:path')).dirname(path),{recursive:true});fs.writeFileSync(path,'ignored build artifact\\n');}`);
    f.git("add", ".gitignore", "implement.mjs"); f.git("commit", "-m", "test: worker build artifacts");
    const plan = await f.call("dag_plan_save", planInput("ignored-intake", ["a"]));
    let run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const selectors = { runId: run.runId, itemId: "a", generation: 1 };
    await f.call("dag_start_work", selectors);
    const terminal = await waitTerminal(f, run.runId, "a"); assert.equal(terminal.terminalStatus, "succeeded");
    const runner = f.service().runner, inspect = runner.inspectCleanWorkspace.bind(runner);
    // Reproduce the prior ignored-inclusive policy with actual worker artifacts,
    // recording rejection through the real product/service transition.
    runner.inspectCleanWorkspace = async (candidate, cwd) => {
      await inspect(candidate, cwd);
      if (!await runner.clean(cwd, candidate)) throw Error("UNCLEAN_CANDIDATE_WORKSPACE: prior ignored-inclusive policy");
    };
    await assert.rejects(f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId }), /UNCLEAN_CANDIDATE_WORKSPACE/);
    runner.inspectCleanWorkspace = inspect;
    run = (await f.service().read()).run;
    const reservation = structuredClone(run.nodes.a.reservation), retries = structuredClone(run.nodes.a.retries ?? []);
    const exact = await f.handles.workerManager.inspectBindingReadOnly(reservation.binding), cwd = exact.worker.cwd;
    const candidateCommit = execFileSync("git", ["rev-parse", "HEAD"], { cwd, encoding: "utf8" }).trim();
    await f.call("dag_pause", { runId: run.runId }); await f.reload();
    run = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    assert.equal(run.status, "paused"); assert.equal(run.nodes.a.lifecycle.candidateReady, false);
    assert.match(run.nodes.a.reservation.intakeRejection, /UNCLEAN_CANDIDATE_WORKSPACE/);
    await f.call("dag_resume", { runId: run.runId });
    await assert.rejects(f.call("dag_record_completion", { ...selectors, completionId: "not-the-exact-completion" }), /EXACT_WORKER_COMPLETION_REQUIRED/);
    run = await f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId });
    assert.equal(run.nodes.a.lifecycle.candidateReady, true); assert.equal(run.nodes.a.lifecycle.candidate.commit, candidateCommit);
    assert.equal(run.nodes.a.reservation.intakeRejection, undefined);
    const { intakeRejection, ...original } = reservation;
    assert.deepEqual(structuredClone(run.nodes.a.reservation), original); assert.deepEqual(structuredClone(run.nodes.a.retries ?? []), retries);
    assert.equal(run.nodes.a.generation, 1); assert.deepEqual((await f.call("dag_next_action", { runId: run.runId })).intakeRejections, []);
    run = await finish(f, run.runId, "a"); assert.equal(run.status, "complete");
    assert.equal(await readFile(join(f.root, "a.txt"), "utf8"), "a\n");
    for (const path of ["node_modules/pkg/index.js", "dist/index.html", ".svelte-kit/output/client.js", "codedb.snapshot"]) {
      assert.equal(await readFile(join(cwd, path), "utf8"), "ignored build artifact\n");
    }
  } finally { await f.cleanup(); }
});

test("dirty retained work is diagnostic only and intake frontier avoids completion loops", async () => {
  const f = await fixture("dirty-recovery"); try {
    const script = await readFile(join(f.root, "implement.mjs"), "utf8"); await writeFile(join(f.root, "implement.mjs"), script + `\nwriteFileSync('debug-output.txt','preserve this diagnostic');`);
    f.git("add", "."); f.git("commit", "-m", "fixture dirty completion");
    const plan = await f.call("dag_plan_save", planInput("dirty-recovery", ["a"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const selectors = { runId: run.runId, itemId: "a", generation: 1 }; await f.call("dag_start_work", selectors);
    const terminal = await waitTerminal(f, run.runId, "a"); await assert.rejects(f.call("dag_record_completion", { ...selectors, completionId: terminal.completionId }), /WORKER_CANDIDATE_DIRTY/);
    const old = (await f.service().read()).run.nodes.a.reservation, exact = await f.handles.workerManager.inspectBindingReadOnly(old.binding);
    const frontier = await f.call("dag_next_action", {}); assert.equal(frontier.actions[0].tool, "dag_replace_worker"); assert.match(frontier.intakeRejections[0].diagnostic, /DIRTY/);
    const before = canonicalStringify((await f.service().read()).run);
    await assert.rejects(f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId }), /NODE_WORKSPACE_REPAIR_BLOCKED.*WORKER_CANDIDATE_DIRTY/s);
    assert.equal(await readFile(join(exact.worker.cwd, "debug-output.txt"), "utf8"), "preserve this diagnostic");
    assert.equal(canonicalStringify((await f.service().read()).run), before, "dirty retained work blocks without a replacement clone or generation");
  } finally { await f.cleanup(); }
});
test("current pending direction and reserved replay are separate snapshots", async () => {
  const f = await fixture("pending-direction"); try {
    const input = planInput("pending-direction", ["a"]); input.workItems[0].context = ["OBSOLETE: stop at first failure; never install"];
    const plan = await f.call("dag_plan_save", input), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["a"]) });
    const selectors = { runId: run.runId, itemId: "a", generation: 1 }, direction = { task: "Implement a.txt; restore normal setup and repair tests.", provenance: "user-message:pending" };
    await f.call("dag_set_worker_direction", { ...selectors, direction });
    let current = await f.call("dag_start_work", selectors); const frozen = current.nodes.a.reservation.request;
    assert(!JSON.parse(frozen).task.includes("OBSOLETE:")); assert.deepEqual(JSON.parse(frozen).direction, direction);
    await f.call("dag_start_work", { ...selectors, direction });
    const newer = { ...direction, task: "Implement a.txt and fix ordinary build failures." };
    await f.call("dag_set_worker_direction", { ...selectors, direction: newer });
    current = await f.call("dag_start_work", selectors); assert.equal(current.nodes.a.reservation.request, frozen);
    await assert.rejects(f.call("dag_start_work", { ...selectors, direction: newer }), /RESERVATION_REQUEST_CONFLICT/);
    const terminal = await waitTerminal(f, run.runId, "a");
    current = await f.call("dag_replace_worker", { ...selectors, completionId: terminal.completionId });
    assert.deepEqual(JSON.parse(current.nodes.a.reservation.request).direction, newer);
    assert.equal(current.nodes.a.archivedReservations[0].request, frozen);
  } finally { await f.cleanup(); }
});
test("explicit failed worker replacement has no budget, retains history and one node root across reloads", async () => {
  const f = await fixture("replacement"); try {
    const plan = await f.call("dag_plan_save", planInput("replacement", ["broken"])), run = await f.call("dag_run_start", { selection: selectorV2(plan), authority: authority(["broken"]) });
    let oldCompletion, workspace; const history = [], artifacts = [];
    for (let generation = 1; generation <= 5; generation++) {
      await f.call("dag_start_work", { runId: run.runId, itemId: "broken", generation }); const terminal = await waitTerminal(f, run.runId, "broken");
      assert.equal(terminal.terminalStatus, "needs_attention");
      if (oldCompletion) await assert.rejects(f.call("dag_record_completion", { runId: run.runId, itemId: "broken", generation: generation - 1, completionId: oldCompletion }), /STALE_GENERATION/);
      await f.call("dag_record_completion", { runId: run.runId, itemId: "broken", generation, completionId: terminal.completionId });
      const node = (await f.service().read()).run.nodes.broken;
      workspace ??= canonicalStringify(node.workspace); assert.equal(canonicalStringify(node.workspace), workspace);
      history.push(canonicalStringify(node.reservation));
      const exact = await f.handles.workerManager.inspectBindingReadOnly(node.reservation.binding);
      const path = join(f.root, exact.attempt.configPath); artifacts.push({ path, bytes: await readFile(path, "utf8") });
      const params = { runId: run.runId, itemId: "broken", generation, completionId: terminal.completionId };
      const before = await readFile(f.service().runtime.store.statePath, "utf8");
      await assert.rejects(f.call("dag_replace_worker", { ...params, completionId: "foreign-completion" }), /EXACT_WORKER_COMPLETION_REQUIRED/);
      assert.equal(await readFile(f.service().runtime.store.statePath, "utf8"), before);
      if (generation === 4) await f.call("dag_pause", { runId: run.runId });
      assert((await f.call("dag_next_action", {})).actions.some(a => a.tool === "dag_replace_worker" && a.generation === generation), "frontier never exhausts replacement count");
      const replaced = await f.call("dag_replace_worker", params); assert.equal(replaced.nodes.broken.generation, generation + 1);
      assert.equal(replaced.nodes.broken.retries.find(r => r.dimension === "replacement").count, generation);
      assert.deepEqual(replaced.nodes.broken.archivedReservations.map(canonicalStringify), history);
      await assert.rejects(f.call("dag_replace_worker", params), /STALE_GENERATION/);
      const statePath = f.service().runtime.store.statePath, saved = await readFile(statePath, "utf8");
      await f.reload(); await f.service().read(); assert.equal(await readFile(statePath, "utf8"), saved, "reload never rewrites replacement history");
      if (generation === 4) await f.call("dag_resume", { runId: run.runId });
      oldCompletion = terminal.completionId;
    }
    for (const artifact of artifacts) assert.equal(await readFile(artifact.path, "utf8"), artifact.bytes);
    const state = await f.handles.workerManager.store.load(); assert.equal(state.launchRecords.length, 5); assert(Object.values(state.workers).every(w => w.currentAttempt === 1));
    const node = (await f.service().read()).run.nodes.broken;
    assert.equal(node.generation, 6); assert.equal(node.lifecycle.candidateReady, false); assert.equal(canonicalStringify(node.workspace), workspace);
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
    await assert.rejects(f.call("dag_run_start", { selection: { planId: legacy.planId, revision: legacy.revision, planHash: legacy.planHash }, authority: authority(["a"]) }), /PLAN_NOT_FOUND_V2/);
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
