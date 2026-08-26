import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { canonicalHash } from "../extensions/dag-workflow/dag-runtime/common.ts";
import { DagConductorServiceV1 } from "../extensions/dag-workflow/dag-runtime/conductor.ts";
import { DagRunSnapshotStoreV1 } from "../extensions/dag-workflow/dag-runtime/store.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { registerDagPlanningIntegrationV1 } from "../extensions/dag-workflow/planning/integration.ts";
import { createBuiltInLifecycleProcedureAdapterV1 } from "../extensions/dag-workflow/planning/runtime-adapter.ts";

const run = promisify(execFile);
const AT = "2026-08-14T12:00:00.000Z";
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

function waitingWorkerAdapter(launches, onTerminalRead = null) {
  let terminalReads = 0;
  return {
    async launchExact(request, state) {
      const attemptNonce = `nonce-${request.workerId}-0123456789`;
      const config = {
        storageId: `command-test-storage-${state.runId}`, ownerSessionId: state.owner.sessionId,
        workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce,
        launchKey: request.launchKey, requestHash: request.configRequestHash, task: request.task,
        launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity },
      };
      const configHash = canonicalHash(config);
      const configCore = { kind: "worker_config", configHash, config };
      const observation = {
        workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId,
        attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash,
        configFact: { ...configCore, hash: canonicalHash(configCore) }, supervisorPid: process.pid,
        supervisorStartIdentity: state.owner.processStartIdentity, childPid: null, childStartIdentity: null,
        mailboxHash: null, heartbeatAt: state.updatedAt,
      };
      launches.push({ request: structuredClone(request), observation: structuredClone(observation) });
      return observation;
    },
    async readTerminalExact() { terminalReads += 1; return (await onTerminalRead?.(terminalReads)) ?? null; },
    async cancelExact() { return "proven_absent"; },
    async cleanupExact() { return "proven_absent"; },
  };
}

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
  async activate(_ctx, runId) {
    this.advances += 1;
    if (!this.current || this.current.binding.runId !== runId) throw new Error("no fake binding");
    return { state: this.current.state, decision: { selected: [] } };
  }
  async retryActivation(ctx, runId) { return this.activate(ctx, runId); }
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
    assert.equal(fx.pi.messages.at(-1).message.customType, "dag-run-orchestration-kickoff");
    assert.equal(fx.pi.messages.at(-1).options.triggerTurn, true);
    assert.match(fx.pi.messages.at(-1).message.content, /dag_run_status[\s\S]*dag_run_dispatch[\s\S]*Never use generic subagent/);
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
    const launches = [];
    let terminalReads = 0;
    let boundaryWake = null;
    let conductor;
    const worker = waitingWorkerAdapter(launches, (count) => { terminalReads = count; });
    let publishedSettlementWake = false;
    const settlementWakeAt = new Date(Date.now() + 60_000).toISOString(); const observedPumpTimes = [];
    conductor = new DagConductorServiceV1({
      lifecycle: { procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }), worker },
      pumpFailpoint(point, detail) {
        if (point === "after_quiescent_check") observedPumpTimes.push(detail?.occurredAt);
        if (point === "after_quiescent_check" && !publishedSettlementWake) {
          publishedSettlementWake = true;
          boundaryWake = conductor.wakeActive(settlementWakeAt);
        }
      },
    });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("show", "--plan delivery@3 --view graph", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /N01 -> N02/);
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    if (boundaryWake) await boundaryWake;
    const binding = await conductor.binding(fx.ctx);
    assert(binding, "the product run command publishes a real current-session binding");
    let status = await conductor.status(fx.ctx, binding.runId);
    assert.equal(status.state.identity.planId, "delivery");
    assert.equal(status.state.workItems.commands.stages.F0.state, "passed", "the real hidden lifecycle completes F0");
    assert.equal(status.state.workItems.commands.currentStage, "F1", "the real runtime reaches the owned implementation boundary");
    assert.equal(launches.length, 0, "post-F0 command and background wakes never launch the ready F1 worker");
    assert.equal(status.readyPackets.length, 1, "the exact F1 packet is agent-visible and actionable");

    const actualAdvance = conductor.advance.bind(conductor);
    let releaseSlowPass; let releaseSuccessorPass; let enterSlowPass; let enterSuccessorPass;
    const slowPassEntered = new Promise((resolve) => { enterSlowPass = resolve; });
    const successorPassEntered = new Promise((resolve) => { enterSuccessorPass = resolve; });
    const slowPassGate = new Promise((resolve) => { releaseSlowPass = resolve; });
    const successorPassGate = new Promise((resolve) => { releaseSuccessorPass = resolve; });
    let boundedPasses = 0;
    conductor.advance = async (...args) => {
      boundedPasses += 1;
      if (boundedPasses === 1) { enterSlowPass(); await slowPassGate; }
      else if (boundedPasses === 2) { enterSuccessorPass(); await successorPassGate; }
      return actualAdvance(...args);
    };
    const slowWake = conductor.wakeActive(new Date(Date.now() + 61_000).toISOString());
    await slowPassEntered;
    const overlappingWake = conductor.wakeActive(new Date(Date.now() + 62_000).toISOString());
    let slowWakeSettled = false; slowWake.then(() => { slowWakeSettled = true; });
    releaseSlowPass();
    await successorPassEntered;
    await Promise.resolve(); await Promise.resolve();
    const settledBeforeSuccessor = slowWakeSettled;
    releaseSuccessorPass();
    const finalBoundedWakeAt = new Date(Date.now() + 63_000).toISOString();
    await Promise.all([slowWake, overlappingWake, conductor.wakeActive(finalBoundedWakeAt)]);
    conductor.advance = actualAdvance;
    await conductor.wakeActive(finalBoundedWakeAt);
    assert.equal(settledBeforeSuccessor, true, "a newer timer wake starts a successor pump without extending callers of the completed pass");

    await conductor.dispatch(fx.ctx, status.readyPackets[0], null, new Date().toISOString());
    await conductor.activate(fx.ctx, binding.runId, new Date().toISOString());
    status = await conductor.status(fx.ctx, binding.runId);
    assert.equal(launches.length, 1, "only dedicated agent dispatch launches the F1 worker");
    assert(terminalReads >= 1, "post-dispatch reconciliation reads the exact bound worker without launching another");
    assert.equal(observedPumpTimes.includes(finalBoundedWakeAt), true, "the final bounded successor preserves the latest dirty wake occurrence time");
    assert(status.state.workerBindings[Object.keys(status.state.stageAttempts).find((id) => status.state.stageAttempts[id].stage === "F1")], "the explicitly dispatched F1 attempt has an exact worker binding");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("delayed procedure recovery closes evidence at durable reconciliation time", async () => {
  const fx = await fixture("delayed-procedure-recovery");
  try {
    const launches = [];
    let interrupted = false;
    const first = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches),
      failpoint(point) { if (!interrupted && point === "after_procedure_dispatch") { interrupted = true; throw new Error("simulated delayed procedure execution"); } },
    } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor: first });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /simulated delayed procedure execution/);
    const binding = await first.binding(fx.ctx);
    const interruptedState = (await first.status(fx.ctx, binding.runId)).state;
    assert(Object.values(interruptedState.effects).some((effect) => effect.kind === "run_procedure" && effect.state === "dispatching"));
    await first.detach();

    const resumed = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches),
    } });
    assert.equal(await resumed.resumeBound(fx.ctx), null, "a restarted service preserves the surfaced fault against background session wakes");
    const recovered = await resumed.retryActivation(fx.ctx, binding.runId, new Date(Date.parse(interruptedState.updatedAt) + 1_000).toISOString());
    const sealedF0 = Object.values(recovered.state.stageAttempts).find((attempt) => attempt.stage === "F0" && attempt.state === "sealed");
    assert(sealedF0, "delayed execution/reconciliation seals F0 instead of retaining pre-closure evidence time");
    const evidence = await new DagRunSnapshotStoreV1(join(fx.root, ".ai", "dag-runs-v1"), binding.runId).readImmutableFact(sealedF0.evidence.hash);
    const effect = Object.values(recovered.state.effects).find((candidate) => candidate.boundStageAttemptId === sealedF0.stageAttemptId);
    const reconciliation = await new DagRunSnapshotStoreV1(join(fx.root, ".ai", "dag-runs-v1"), binding.runId).readImmutableFact(effect.observationHash);
    assert.equal(evidence.producedAt, reconciliation.closedAt, "closed evidence uses the durable reconciliation closure time");
    assert.equal(launches.length, 0, "delayed F0 recovery prepares F1 without a background launch");
    const ready = await resumed.status(fx.ctx, binding.runId); assert.equal(ready.readyPackets.length, 1);
    await resumed.dispatch(fx.ctx, ready.readyPackets[0], null, new Date(Date.parse(recovered.state.updatedAt) + 2_000).toISOString());
    assert.equal(launches.length, 1, "dedicated dispatch launches the exact delayed-recovery F1 packet");
    await resumed.detach();
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("an Operant-r24-shaped reconciled F0 path seals after owner fencing and prepares agent dispatch", async () => {
  const fx = await fixture("service-resume");
  try {
    const launches = [];
    let interrupted = false;
    const first = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches),
      failpoint(point) { if (!interrupted && point === "after_procedure_reconcile") { interrupted = true; throw new Error("simulated command-scoped pump exit"); } },
    } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor: first });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /simulated command-scoped pump exit/);
    const binding = await first.binding(fx.ctx);
    const interruptedState = (await first.status(fx.ctx, binding.runId)).state;
    assert(Object.values(interruptedState.effects).some((effect) => effect.kind === "run_procedure" && effect.state === "reconciled"), "interruption preserves applied_exact F0 effect authority");
    assert(Object.values(interruptedState.stageAttempts).some((attempt) => attempt.stage === "F0" && attempt.state === "running"), "interruption reproduces an unsealed running F0 attempt");
    assert.equal(launches.length, 0, "interrupted pump has not dispatched F1");

    const lifecycle = () => ({
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches),
    });
    let activeService = new DagConductorServiceV1({ lifecycle: lifecycle() });
    let recoveryAt = new Date(Date.parse(interruptedState.updatedAt) + 1).toISOString();
    await assert.rejects(() => activeService.retryActivation(fx.ctx, binding.runId, recoveryAt), /Another conductor service generation is still operational/, "an explicit retry still cannot steal a concurrently live wrapper-bound epoch");
    await first.detach();
    let recovered = await activeService.retryActivation(fx.ctx, binding.runId, recoveryAt);
    assert.equal(recovered.state.owner.ownerEpoch, interruptedState.owner.ownerEpoch + 1, "fresh same-session service CAS-transfers to a new fencing epoch despite the live wrapper PID");
    assert.equal(launches.length, 0, "service recovery seals F0 but remains reconciliation-only at ready F1");
    assert.equal(recovered.state.workItems.commands.stages.F0.state, "passed");
    const ready = await activeService.status(fx.ctx, binding.runId); assert.equal(ready.readyPackets.length, 1);
    const dispatched = await activeService.dispatch(fx.ctx, ready.readyPackets[0], null, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString()); recovered = { ...recovered, state: dispatched.state };
    assert.equal(launches.length, 1, "dedicated dispatch launches and durably binds recovered F1");
    assert(Object.values(recovered.state.workerBindings).some((workerBinding) => workerBinding.stageAttemptId === Object.values(recovered.state.stageAttempts).find((attempt) => attempt.stage === "F1")?.stageAttemptId), "recovered F1 launch is durably bound");
    let rebound = await activeService.binding(fx.ctx);
    assert.equal(rebound.ownerEpoch, recovered.state.owner.ownerEpoch, "session binding advances atomically to the recovered owner epoch");
    assert.equal((await activeService.startIdentity(fx.ctx, binding.runId)).runId, binding.runId, "active start identity follows the recovered binding");

    for (const boundary of ["after_owner_transfer", "after_owner_binding", "after_owner_start_identity"]) {
      await activeService.detach();
      const crashing = new DagConductorServiceV1({ lifecycle: lifecycle(), ownerResumeFailpoint(point) { if (point === boundary) throw new Error(`owner-resume-crash:${boundary}`); } });
      recoveryAt = new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString();
      await assert.rejects(() => crashing.activate(fx.ctx, binding.runId, recoveryAt), new RegExp(`owner-resume-crash:${boundary}`));
      await crashing.detach();
      activeService = new DagConductorServiceV1({ lifecycle: lifecycle() });
      recovered = await activeService.retryActivation(fx.ctx, binding.runId, new Date(Date.parse(recoveryAt) + 1).toISOString());
      rebound = await activeService.binding(fx.ctx);
      assert.equal(rebound.ownerEpoch, recovered.state.owner.ownerEpoch, `${boundary} replay repairs the exact binding`);
      assert.equal((await activeService.startIdentity(fx.ctx, binding.runId)).runId, binding.runId, `${boundary} replay repairs the exact start identity`);
    }

    for (let generation = 0; generation < 66; generation += 1) {
      await activeService.detach();
      activeService = new DagConductorServiceV1({ lifecycle: lifecycle() });
      recovered = await activeService.activate(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString());
    }
    assert(recovered.state.owner.ownerEpoch > 64, "bounded ownership hydration supports more than 64 ordinary service generations");
    assert.equal((await activeService.status(fx.ctx, binding.runId)).state.snapshotHash, recovered.state.snapshotHash);

    await activeService.detach();
    let enterTerminalRead; let releaseTerminalRead;
    const terminalReadEntered = new Promise((resolve) => { enterTerminalRead = resolve; });
    const terminalReadGate = new Promise((resolve) => { releaseTerminalRead = resolve; });
    const drainingService = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches, async (count) => { if (count === 1) { enterTerminalRead(); await terminalReadGate; } }),
    } });
    const drainingPump = drainingService.activate(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString());
    await terminalReadEntered;
    const drainingDetach = drainingService.detach();
    const fencedReplacement = new DagConductorServiceV1({ lifecycle: lifecycle() });
    await assert.rejects(() => fencedReplacement.activate(fx.ctx, binding.runId), /Another conductor service generation is still operational/, "detach retains the registry fence until its in-flight lifecycle pass drains");
    releaseTerminalRead();
    recovered = await drainingPump;
    await drainingDetach;
    activeService = fencedReplacement;
    recovered = await activeService.retryActivation(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString());

    await activeService.detach();
    let injectedPumpFailure = false; let sharedWakeResult = null; let failingService; const reportedPumpErrors = [];
    failingService = new DagConductorServiceV1({
      lifecycle: lifecycle(),
      onPumpError(input) { reportedPumpErrors.push(input); },
      pumpFailpoint(point) {
        if (point === "after_quiescent_check" && !injectedPumpFailure) {
          injectedPumpFailure = true;
          sharedWakeResult = failingService.wakeActive().catch((error) => error);
          throw new Error("terminal-pump-failure");
        }
      },
    });
    await assert.rejects(() => failingService.activate(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString()), /terminal-pump-failure/, "a dirty wake cannot convert deterministic pump rejection into a successor success");
    assert.match(String((await sharedWakeResult).message), /terminal-pump-failure/, "all callers sharing the failed pump observe its exact terminal error");
    assert.equal(reportedPumpErrors.length, 1, "the exact pump error is reported once to the top-level integration");
    assert.equal(await failingService.resumeBound(fx.ctx), null, "background wakes remain stopped after a surfaced conductor fault");
    await failingService.detach();
    let enterExplicitRetry; let releaseExplicitRetry;
    const explicitRetryEntered = new Promise((resolve) => { enterExplicitRetry = resolve; });
    const explicitRetryGate = new Promise((resolve) => { releaseExplicitRetry = resolve; });
    const replacementService = new DagConductorServiceV1({ lifecycle: lifecycle(), async pumpFailpoint(point) { if (point === "after_quiescent_check") { enterExplicitRetry(); await explicitRetryGate; } } });
    assert.equal(await replacementService.resumeBound(fx.ctx), null, "the durable fault latch survives conductor service replacement");
    await assert.rejects(() => replacementService.activate(fx.ctx, binding.runId), /durable surfaced fault/, "ordinary activation cannot bypass a durable surfaced fault");
    const retryPromise = replacementService.retryActivation(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 2).toISOString());
    await explicitRetryEntered;
    const competingRetry = new DagConductorServiceV1({ lifecycle: lifecycle() });
    assert.equal(await competingRetry.resumeBound(fx.ctx), null, "the durable fault remains visible throughout an explicit retry");
    await assert.rejects(() => competingRetry.retryActivation(fx.ctx, binding.runId), /Another conductor service generation is still operational/, "canonical owner fencing still rejects a concurrent explicit retry");
    releaseExplicitRetry(); recovered = await retryPromise;
    assert.equal(reportedPumpErrors.length, 1, "an explicit successful retry clears the fault without replaying its report");
    await competingRetry.detach(); await replacementService.detach();

    await rm(join(fx.root, ".ai", "dag-start-intents-v1"), { recursive: true, force: true });
    activeService = new DagConductorServiceV1({ lifecycle: lifecycle() });
    await assert.rejects(() => activeService.activate(fx.ctx, binding.runId, new Date(Date.parse(recovered.state.updatedAt) + 1).toISOString()), /missing its exact prepared-start identity/, "current thin-plan runs fail closed if their prepared-start identity is removed");
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
