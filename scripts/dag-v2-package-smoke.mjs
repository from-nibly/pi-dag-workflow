// Bounded packed-product check: actual registration and durable start, no worker
// launch or full lifecycle matrix. All mutable state lives in a disposable repo.
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { FocusSessionStore } from "../extensions/dag-workflow/project-model/sessions.ts";
import { SpecProjector } from "../extensions/dag-workflow/project-model/projector.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { selectorV2 } from "../extensions/dag-workflow/planning/v2.ts";
import { gitEnvironmentV2 } from "../extensions/dag-workflow/runtime-v2/command-runner.ts";

const root = await mkdtemp(join(tmpdir(), "dag-v2-package-smoke-"));
const tools = new Map(), commands = new Map(), handlers = new Map(), entries = [], messages = [];
let active = ["read", "bash", "write", "edit"], attached = false;
const bus = new EventEmitter();
const pi = {
  events: {
    emit(name, data) { bus.emit(name, data); },
    on(name, listener) {
      const handler = (data) => listener(data);
      bus.on(name, handler);
      return () => { bus.off(name, handler); };
    },
  },
  registerTool(tool) { assert(!tools.has(tool.name)); tools.set(tool.name, tool); active.push(tool.name); },
  registerCommand(name, command) { commands.set(name, command); },
  on(name, handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
  getActiveTools() { return active; }, setActiveTools(names) { active = names; },
  getAllTools() { return [...tools.values()]; },
  appendEntry(customType, data) { entries.push({ type: "custom", customType, data }); },
  sendMessage(message) { messages.push(message); },
};
const ctx = { cwd: root, hasUI: false, mode: "print", ui: { notify(text, kind) { if (kind === "error") throw Error(text); }, setWidget() { assert.fail("headless smoke mounted a widget"); } },
  sessionManager: { getSessionId: () => "package-smoke", getSessionFile: () => null, getHeader: () => ({ id: "package-smoke", cwd: root }), getBranch: () => entries, getEntries: () => entries } };
const emit = async name => { for (const handler of handlers.get(name) ?? []) await handler({ systemPrompt: "package smoke" }, ctx); };
const call = async (name, input) => {
  assert(tools.has(name), `missing shipped tool ${name}`);
  return (await tools.get(name).execute("package-smoke", input, undefined, undefined, ctx)).details;
};
try {
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd: root, env: gitEnvironmentV2(), stdio: ["ignore", "pipe", "pipe"] });
  git("init", "-b", "main"); git("config", "user.name", "Package smoke"); git("config", "user.email", "package@example.invalid");
  const at = "2026-01-01T00:00:00.000Z", source = "model:decisions/DEC-local";
  const decision = { id: "DEC-local", title: "Verify local bytes", body: "The local fixture must contain baseline and newline. No publication.", state: "accepted", scope: { kind: "repository" }, introducedBy: "user", sourceRefs: ["fixture"], relationships: [], createdAt: at, updatedAt: at, rationale: "Observe exact local bytes." };
  decision.acceptance = { mode: "direct_direction", actor: "user", acceptedAt: at, contentHash: semanticHash("decisions", decision), interactionRef: "smoke-fixture" };
  const model = { schemaVersion: 1, project: { id: "package-smoke", title: "Package smoke", revision: 1, mode: "authoritative", createdAt: at, updatedAt: at,
    projections: { specs: [{ id: "SPEC-local", kind: "spec", path: "spec/local/spec.md", title: "Local", sections: [{ id: "direction", title: "Direction", objectIds: [decision.id] }] }] } },
    workstreams: [], intents: [], concepts: [], evidence: [], assumptions: [], questions: [], tensions: [], scenarios: [], proposals: [], decisions: [decision], commitments: [], discoveries: [] };
  await mkdir(join(root, "project-model")); await mkdir(join(root, "spec/local"), { recursive: true });
  await writeFile(join(root, "project-model/model.json"), JSON.stringify(model));
  for (const projection of new SpecProjector(root).render(model)) await writeFile(join(root, projection.path), projection.content);
  await writeFile(join(root, ".gitignore"), ".ai/\n"); await writeFile(join(root, "file"), "baseline\n");
  await writeFile(join(root, "verify.mjs"), "import assert from 'node:assert/strict'; import {readFileSync} from 'node:fs'; assert.equal(readFileSync('file','utf8'),'baseline\\n');\n");
  git("add", "."); git("commit", "-m", "test: package fixture");
  await new FocusSessionStore(root).create({ id: "focus-smoke", title: "Smoke", workstreamIds: [] });
  entries.push({ type: "custom", customType: "dag-model-focus-link", data: { repositoryRoot: root, focusSessionId: "focus-smoke", mode: "active" } });
  const role = process.env.PI_DAG_WORKER_ROLE;
  let handles;
  try { delete process.env.PI_DAG_WORKER_ROLE; handles = dagWorkflow(pi); }
  finally { if (role !== undefined) process.env.PI_DAG_WORKER_ROLE = role; }
  attached = true; await emit("session_start");
  assert(!tools.has("dag_plan_decide"), "approval writer is still registered");
  for (const [name, tool] of tools) if (name.startsWith("dag_") && !name.startsWith("dag_model")) assert(!JSON.stringify(tool.parameters).includes('"actionId"'), name);
  const input = { planId: "smoke", expectedPlanRevision: 0, title: "Local verification", sourceRefs: [source, "spec:spec/local/spec.md"], scopeSummary: "Verify fixture locally",
    architecture: { outcomes: [{ id: "bytes", description: "Exact baseline bytes" }], nonGoals: ["Publication"], notes: [], risks: [] },
    workItems: [{ id: "local", title: "Verify local bytes", objective: "Keep baseline and newline", outcomeIds: ["bytes"], context: [], checks: ["Exact bytes"], dependsOn: [], risk: "low", riskNotes: [], resources: {}, gates: [],
      lifecycle: { oracle: { statement: "file contains baseline and newline", sourceRefs: [source], checkIds: ["f2"] },
        checks: Array.from({ length: 7 }, (_, i) => ({ id: `f${i + 1}`, stage: i + 1, expectation: "Observe exact fixture bytes", sourceRefs: [source], applicability: { kind: "required" }, procedure: { kind: "command", argv: [process.execPath, "verify.mjs"] }, environment: "node-local", replay: "pure" })) } }],
    constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] }, integration: { strategy: "serial", checks: ["Exact prefix"], finalChecks: ["Exact final"], prefixCommands: [{ id: "prefix", argv: [process.execPath, "verify.mjs"] }], finalCommands: [{ id: "final", argv: [process.execPath, "verify.mjs"] }] } };
  const product = handles.planningIntegration.product(ctx), statePath = join(root, ".ai/dag-workflow-v2/state.json");
  let plan = await call("dag_plan_save", input);
  assert.equal(plan.schemaVersion, 2); assert(!("approval" in plan)); assert(!("authorization" in plan));
  assert.equal(Object.keys((await product.runtime.store.read()).runs).length, 0);
  const beforeShow = await readFile(statePath);
  await commands.get("dag").handler("show --plan smoke@1", ctx);
  assert(messages.length > 0); assert.deepEqual(await readFile(statePath), beforeShow);
  plan = await call("dag_plan_save", { ...input, expectedPlanRevision: 1, title: "Revised local verification" });
  assert.equal(plan.revision, 2); assert.equal(Object.keys((await product.runtime.store.read()).runs).length, 0);
  const start = { selection: selectorV2(plan), authority: { scope: ["local"], maxConcurrency: 1, effects: ["repository_local"], expiresAt: Date.now() + 3600000 } };
  await commands.get("dag").handler(`run ${JSON.stringify(start)}`, ctx);
  const { run } = await product.read(); assert(run); assert.equal(run.start.selection.revision, 2);
  assert.equal(run.nodes.local.status, "pending"); assert.equal(run.nodes.local.reservation, undefined);
  const beforeReads = await readFile(statePath); await call("dag_run_status", {}); await call("dag_next_action", {});
  assert.deepEqual(await readFile(statePath), beforeReads);
  assert.equal(Object.keys((await product.runtime.store.read()).runs).length, 1);
  console.log("Packed V2 product smoke OK: actual default registration, inert save/show/revision, explicit durable run; no worker launched.");
} finally {
  try { if (attached) await emit("session_shutdown"); assert.deepEqual(bus.eventNames(), [], "session shutdown leaked bus subscriptions"); }
  finally { await rm(root, { recursive: true, force: true }); }
}
