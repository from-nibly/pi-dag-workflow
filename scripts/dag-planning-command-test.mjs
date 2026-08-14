import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { DagConductorServiceV1 } from "../extensions/dag-workflow/dag-runtime/conductor.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { registerDagPlanningIntegrationV1 } from "../extensions/dag-workflow/planning/integration.ts";
import { createBuiltInLifecycleProcedureAdapterV1 } from "../extensions/dag-workflow/planning/runtime-adapter.ts";

const run = promisify(execFile);
const AT = "2026-08-14T12:00:00.000Z";
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

async function git(cwd, args) {
  const result = await run("git", args, { cwd, encoding: "utf8", env: {
    ...process.env, LC_ALL: "C", LANG: "C", GIT_AUTHOR_NAME: "Planning Command Test", GIT_AUTHOR_EMAIL: "planning@example.invalid",
    GIT_COMMITTER_NAME: "Planning Command Test", GIT_COMMITTER_EMAIL: "planning@example.invalid", GIT_AUTHOR_DATE: AT, GIT_COMMITTER_DATE: AT,
  } });
  return result.stdout.trim();
}

class FakeUi {
  messages = [];
  notify(text, level) { this.messages.push({ text, level }); }
}

class FakePi {
  tools = new Map();
  messages = [];
  entries = [];
  registerTool(tool) { this.tools.set(tool.name, tool); }
  appendEntry(customType, data) { this.entries.push({ type: "custom", customType, data }); }
  sendMessage(message, options) { this.messages.push({ message, options }); }
  async call(name, params, ctx) { return this.tools.get(name).execute("tool-call", params, undefined, undefined, ctx); }
}

class FakeConductor {
  current = null;
  pending = null;
  failNextStart = false;
  startInputs = [];
  starts = 0;
  advances = 0;
  async binding() { return this.current?.binding ?? null; }
  async startIdentity(_ctx, runId) {
    if (!this.current || this.current.binding.runId !== runId) throw new Error("no fake binding");
    const input = this.current.input;
    return { runId, planHash: input.planHash, sourcePlanningPlanId: input.sourcePlanningPlanId, sourcePlanningPlanHash: input.sourcePlanningPlanHash };
  }
  async pendingStart(_ctx, planId, planHash) {
    return this.pending?.sourcePlanningPlanId === planId && this.pending?.sourcePlanningPlanHash === planHash ? this.pending : null;
  }
  async startPrepared(_ctx, input) {
    this.starts += 1;
    this.startInputs.push(input);
    if (this.failNextStart) {
      this.failNextStart = false;
      this.pending = { runId: input.runId, runNonce: input.runNonce, planHash: input.planHash, sourcePlanningPlanId: input.sourcePlanningPlanId, sourcePlanningPlanHash: input.sourcePlanningPlanHash, startedAt: input.occurredAt };
      throw new Error("simulated crash before binding");
    }
    const binding = { runId: input.runId, planHash: input.planHash };
    this.current = { binding, state: structuredClone(input.genesis), plan: input.plan, input };
    this.pending = null;
    return { binding, state: this.current.state, decision: { selected: [] } };
  }
  async advance(_ctx, runId) {
    this.advances += 1;
    if (!this.current || this.current.binding.runId !== runId) throw new Error("no fake binding");
    return { state: this.current.state, decision: { selected: [] } };
  }
  async status(_ctx, runId) {
    if (!this.current || this.current.binding.runId !== runId) throw new Error("no fake binding");
    return { state: this.current.state, decision: { selected: [] }, projection: { nodes: Object.keys(this.current.state.workItems).map((workItemId) => ({ workItemId })) }, stale: null };
  }
  async inspect(_ctx, runId, node) {
    const live = await this.status(_ctx, runId);
    return { node, state: live.state.workItems[node] ?? null };
  }
}

async function fixture(label) {
  const root = await mkdtemp(join(tmpdir(), `dag-planning-command-${label}-`));
  await git(root, ["init", "-b", "main"]);
  const decision = {
    id: "DEC-delivery", title: "Deliver the product workflow", body: "Ship exact product-facing planning, inspection, and execution commands.", state: "accepted",
    scope: { kind: "repository" }, introducedBy: "user", sourceRefs: ["test"], relationships: [], createdAt: AT, updatedAt: AT,
    rationale: "Exercise the product integration.",
  };
  decision.acceptance = { mode: "direct_direction", actor: "user", acceptedAt: AT, contentHash: semanticHash("decisions", decision), interactionRef: "test-direction" };
  const projection = { id: "SPEC-delivery", kind: "spec", path: "spec/delivery/spec.md", title: "Delivery", sections: [{ id: "direction", title: "Direction", objectIds: [decision.id] }] };
  const model = {
    schemaVersion: 1,
    project: { id: `planning-${label}`, title: "Planning fixture", revision: 1, mode: "authoritative", createdAt: AT, updatedAt: AT, projections: { specs: [projection] } },
    workstreams: [], intents: [], concepts: [], evidence: [], assumptions: [], questions: [], tensions: [], scenarios: [], proposals: [], decisions: [decision], commitments: [], discoveries: [],
  };
  await mkdir(join(root, "project-model"), { recursive: true });
  await mkdir(join(root, "spec/delivery"), { recursive: true });
  await writeFile(join(root, ".gitignore"), ".ai/\n");
  await writeFile(join(root, "project-model/model.json"), `${JSON.stringify(model, null, 2)}\n`);
  await writeFile(join(root, "spec/delivery/spec.md"), "# Delivery\n\nExact generated specification.\n");
  await writeFile(join(root, "baseline.txt"), "baseline\n");
  await git(root, ["add", "."]);
  await git(root, ["commit", "-m", "test: establish planning baseline"]);

  const pi = new FakePi();
  const conductor = new FakeConductor();
  const ui = new FakeUi();
  const sessionId = `session-${label}`;
  const ctx = { cwd: root, hasUI: false, ui, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null, getEntries: () => pi.entries } };
  const focus = { id: "focus-delivery", repositoryRoot: root, orientation: { focus: { id: "focus-delivery", title: "Delivery" }, project: { id: model.project.id, mode: "authoritative" } } };
  const integration = registerDagPlanningIntegrationV1(pi, { getActiveFocus: () => focus, conductor });
  return { root, pi, conductor, ui, ctx, integration };
}

function saveInput(planId = "delivery") {
  return {
    planId,
    title: `Deliver ${planId}`,
    source: { refs: [
      { kind: "project_model_object", collection: "decisions", objectId: "DEC-delivery", summary: "Accepted product direction." },
      { kind: "generated_spec", path: "spec/delivery/spec.md", summary: "Current generated specification." },
    ], scopeSummary: "Deliver the exact product workflow from accepted model direction." },
    architecture: { outcomes: [{ id: "out-delivery", description: "The product workflow is usable through plan, show, and run commands." }], nonGoals: ["Do not expose low-level execution artifacts."], notes: ["Keep architecture review ahead of decomposition."], risks: ["Repository drift must fail closed."] },
    workItems: [
      { id: "commands", title: "Implement product commands", objective: "Implement deterministic product command behavior.", outcomeIds: ["out-delivery"], context: ["Use the exact active focus."], checks: ["Run product command tests."], dependsOn: [], risk: "high", riskNotes: ["Selection ambiguity is authority-sensitive."] },
      { id: "verify", title: "Verify product workflow", objective: "Verify exact selection and start behavior.", outcomeIds: ["out-delivery"], context: ["Use a real clean Git fixture."], checks: ["Verify stale and dirty failures."], dependsOn: ["commands"], risk: "medium", riskNotes: [] },
    ],
    constraints: { maxConcurrency: 2, mutexGroups: [] },
    integration: {
      strategy: "dependency_order",
      checks: ["Run focused command integration tests."],
      finalChecks: ["Confirm the exact combined state."],
      prefixCommands: [{ id: "prefix-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
      finalCommands: [{ id: "final-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
    },
  };
}

async function approveAndAuthorize(fx, planId, revision = 1) {
  return fx.pi.call("dag_plan_decide", { planId, expectedRevision: revision, approve: true, approvalNote: "Explicit approval.", authorize: { scope: ["commands", "verify"], maxConcurrency: 2, note: "Explicit authorization." } }, fx.ctx);
}

test("parses planning command, sends one follow-up, and saves deterministic preview with derived identities", async () => {
  const fx = await fixture("save");
  try {
    assert.deepEqual([...fx.pi.tools.keys()].sort(), ["dag_plan_decide", "dag_plan_save"]);
    const schemaText = JSON.stringify([...fx.pi.tools.values()].map(({ parameters }) => parameters));
    assert(!/baselineCommit|baselineTree|planHash|semanticHash|contentHash|gitOid|receipt/i.test(schemaText));
    await fx.integration.handleCommand("plan", "--new exact delivery goal", fx.ctx);
    assert.equal(fx.pi.messages.length, 1);
    assert.equal(fx.pi.messages[0].options.triggerTurn, true);
    assert.match(fx.pi.messages[0].message.content, /Start with architecture/);
    assert.match(fx.pi.messages[0].message.content, /exact delivery goal/);

    const saved = await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    assert.match(saved.content[0].text, /# Deliver delivery/);
    assert.match(saved.content[0].text, /N01 \[high\]/);
    assert.match(saved.details.planHash, /^sha256:/);
    assert.equal(saved.details.revision, 1);
    const stored = JSON.parse(await (await import("node:fs/promises")).readFile(join(fx.root, ".ai/dag-plans-v1/plans/delivery.json"), "utf8"));
    assert.equal(stored.focusId, "focus-delivery");
    assert.equal(stored.repository.baselineCommit, await git(fx.root, ["rev-parse", "HEAD"]));
    assert.equal(stored.source.refs[0].semanticHash, semanticHash("decisions", JSON.parse(await (await import("node:fs/promises")).readFile(join(fx.root, "project-model/model.json"), "utf8")).decisions[0]));
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("show is exact and no-agent; approval and authorization are separate retained decisions and never start", async () => {
  const fx = await fixture("decide-show");
  try {
    const saved = await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    const staticHash = saved.details.planHash;
    const decided = await approveAndAuthorize(fx, "delivery");
    assert.equal(decided.details.revision, 3, "approve then authorize retains two separate revisions");
    assert.equal(decided.details.planHash, staticHash);
    assert.equal(fx.conductor.starts, 0);
    assert.equal(fx.pi.messages.length, 0);

    await fx.integration.handleCommand("show", { rest: "graph", options: { plan: "delivery@3" } }, fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /Exact static plan delivery@3/);
    assert.match(fx.ui.messages.at(-1).text, /N01 -> N02/);
    await fx.integration.handleCommand("show", "node N02 --plan delivery@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /"id": "verify"/);
    assert.equal(fx.pi.messages.length, 0, "show never triggers the agent");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("run starts once and duplicate invocation advances the exact binding without restart", async () => {
  const fx = await fixture("start-once");
  try {
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.equal(fx.conductor.starts, 1);
    assert.equal(fx.conductor.advances, 1);
    assert.match(fx.ui.messages.at(-1).text, /DAG run run-/);
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.equal(fx.conductor.starts, 1, "bound duplicate does not call startPrepared");
    assert.equal(fx.conductor.advances, 2);
    await fx.pi.call("dag_plan_save", saveInput("other-plan"), fx.ctx);
    await approveAndAuthorize(fx, "other-plan");
    await fx.integration.handleCommand("run", "--plan other-plan@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /does not match the exact current-session run source/);
    assert.equal(fx.conductor.advances, 2, "a mismatched explicit selector cannot advance the bound run");
    await fx.integration.handleCommand("show", "--run", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /Exact live run run-/);
    await fx.integration.handleCommand("show", "--run wrong-run", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /not the exact current-session binding/);
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("a restarted run command reuses the exact unfinished prepared-start identity", async () => {
  const fx = await fixture("recover-start");
  try {
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    fx.conductor.failNextStart = true;
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /simulated crash before binding/);
    assert.equal(fx.conductor.starts, 1);
    const first = fx.conductor.startInputs[0];
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.equal(fx.conductor.starts, 2);
    const recovered = fx.conductor.startInputs[1];
    assert.equal(recovered.runId, first.runId);
    assert.equal(recovered.runNonce, first.runNonce);
    assert.equal(recovered.occurredAt, first.occurredAt);
    assert.equal(recovered.planHash, first.planHash);
    assert(fx.conductor.current, "the retried exact start becomes session-bound");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("plan, show, and run reach the real canonical runtime through the prepared boundary", async () => {
  const fx = await fixture("real-runtime");
  try {
    const conductor = new DagConductorServiceV1({ lifecycle: { procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }) } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("show", "--plan delivery@3 --view graph", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /N01 -> N02/);
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    const binding = await conductor.binding(fx.ctx);
    assert(binding, "the product run command publishes a real current-session binding");
    const status = await conductor.status(fx.ctx, binding.runId);
    assert.equal(status.state.identity.planId, "delivery");
    assert.equal(status.state.workItems.commands.stages.F0.state, "passed", "the real hidden lifecycle completes F0");
    assert.equal(status.state.workItems.commands.currentStage, "F1", "the real runtime reaches the owned implementation boundary");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("resume without a current-session binding and runnable-plan ambiguity fail deterministically", async () => {
  const resume = await fixture("resume-error");
  try {
    await resume.integration.handleCommand("run", "--resume", resume.ctx);
    assert.match(resume.ui.messages.at(-1).text, /No exact current-session DAG run binding/);
    assert.equal(resume.conductor.starts, 0);
  } finally { await rm(resume.root, { recursive: true, force: true }); }

  const ambiguous = await fixture("ambiguity");
  try {
    await ambiguous.pi.call("dag_plan_save", saveInput("delivery-a"), ambiguous.ctx);
    await approveAndAuthorize(ambiguous, "delivery-a");
    await ambiguous.pi.call("dag_plan_save", saveInput("delivery-b"), ambiguous.ctx);
    await approveAndAuthorize(ambiguous, "delivery-b");
    await ambiguous.integration.handleCommand("run", "", ambiguous.ctx);
    assert.match(ambiguous.ui.messages.at(-1).text, /Multiple active-focus plan heads are runnable/);
    assert.equal(ambiguous.conductor.starts, 0);
  } finally { await rm(ambiguous.root, { recursive: true, force: true }); }
});

test("dirty save and stale baseline run fail before persistence or start", async () => {
  const dirty = await fixture("dirty");
  try {
    await writeFile(join(dirty.root, "baseline.txt"), "dirty\n");
    await assert.rejects(() => dirty.pi.call("dag_plan_save", saveInput(), dirty.ctx), /must be clean/);
  } finally { await rm(dirty.root, { recursive: true, force: true }); }

  const stale = await fixture("stale");
  try {
    await stale.pi.call("dag_plan_save", saveInput(), stale.ctx);
    await approveAndAuthorize(stale, "delivery");
    await writeFile(join(stale.root, "baseline.txt"), "new baseline\n");
    await git(stale.root, ["add", "baseline.txt"]);
    await git(stale.root, ["commit", "-m", "test: move baseline"]);
    await stale.integration.handleCommand("run", "--plan delivery@3", stale.ctx);
    assert.match(stale.ui.messages.at(-1).text, /Stale planned Git target|HEAD no longer equals/);
    assert.equal(stale.conductor.starts, 0);
  } finally { await rm(stale.root, { recursive: true, force: true }); }
});

let failures = 0;
for (const [name, fn] of tests) {
  try { await fn(); console.log(`ok - ${name}`); }
  catch (error) { failures += 1; console.error(`not ok - ${name}`); console.error(error?.stack ?? error); }
}
if (failures) process.exitCode = 1;
else console.log(`ok - ${tests.length} DAG planning command integration tests passed`);
