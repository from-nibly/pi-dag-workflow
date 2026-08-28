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

test("run starts once and duplicate invocation reopens the visible agent loop without mutation", async () => {
  const fx = await fixture("start-once");
  try {
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.equal(fx.conductor.starts, 1);
    assert.equal(fx.conductor.advances, 0);
    assert.match(fx.ui.messages.at(-1).text, /DAG run run-[\s\S]*dag_next_action/);
    assert.equal(fx.pi.messages.at(-1).message.customType, "dag-run-orchestration-kickoff");
    assert.equal(fx.pi.messages.at(-1).options.triggerTurn, true);
    assert.match(fx.pi.messages.at(-1).message.content, /dag_next_action[\s\S]*dag_start_work[\s\S]*dag_record_completion[\s\S]*dag_pause[\s\S]*dag_resume[\s\S]*Never use generic subagent/);
    await fx.integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    assert.equal(fx.conductor.starts, 1, "bound duplicate does not call startPrepared");
    assert.equal(fx.conductor.advances, 0);
    await fx.pi.call("dag_plan_save", saveInput("other-plan"), fx.ctx);
    await approveAndAuthorize(fx, "other-plan");
    await fx.integration.handleCommand("run", "--plan other-plan@3", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /does not match the exact current-session run source/);
    assert.equal(fx.conductor.advances, 0, "a mismatched explicit selector cannot mutate the bound run");
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
    let terminalReads = 0; let terminalObservation = null;
    const worker = waitingWorkerAdapter(launches, (count) => { terminalReads = count; return terminalObservation; });
    const conductor = new DagConductorServiceV1({ lifecycle: { procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }), worker } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("show", "--plan delivery@3 --view graph", fx.ctx);
    assert.match(fx.ui.messages.at(-1).text, /N01 -> N02/);
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    const binding = await conductor.binding(fx.ctx);
    assert(binding, "the product run command publishes a real current-session binding");
    let status = await conductor.status(fx.ctx, binding.runId);
    assert.equal(status.state.identity.planId, "delivery");
    assert.equal(status.state.workItems.commands.stages.F0.state, "pending", "run creation does not autonomously pump F0");
    assert.equal(status.state.revision, 1, "prepared start preserves the exact canonical owner-attach history");
    assert.equal(launches.length, 0, "run/session lifecycle never launches canonical work");

    const initial = await conductor.nextAction(fx.ctx, binding.runId);
    const f0 = initial.frontier.find((candidate) => candidate.operation === "run_checks" && candidate.workItemId === "commands" && candidate.stage === "F0");
    assert(f0, "read-only dag_next_action exposes the admissible F0 semantic action");
    const afterRead = await conductor.status(fx.ctx, binding.runId);
    assert.equal(afterRead.state.revision, status.state.revision, "dag_next_action does not mutate canonical DAG state");
    const checked = await conductor.runChecks(fx.ctx, binding.runId, f0.actionId, "commands", "F0");
    assert.equal(checked.state.workItems.commands.stages.F0.state, "passed", "explicit dag_run_checks closes F0 through existing lifecycle machinery");
    const owned = checked.next.frontier.find((candidate) => candidate.operation === "start_work" && candidate.workItemId === "commands" && candidate.stage === "F1");
    assert(owned, "post-check frontier exposes exact owned implementation work");
    const started = await conductor.startWork(fx.ctx, binding.runId, owned.actionId, "commands", "F1", null);
    status = await conductor.status(fx.ctx, binding.runId);
    assert.equal(launches.length, 1, "only explicit dag_start_work launches the F1 worker");
    assert(started.binding && status.state.workerBindings[started.binding.stageAttemptId], "semantic start derives the packet/CAS and retains the exact durable worker binding");
    assert(terminalReads >= 0, "owned work may now wait without a timer or arbitrary timeout");

    terminalObservation = { completionId: "completion-semantic-f1", terminalStatus: "failed" };
    const beforeNoticeRevision = status.state.revision;
    const notice = await conductor.completionNotice(fx.ctx, { workerId: started.binding.workerId, attemptNumber: started.binding.attemptNumber, completionId: terminalObservation.completionId, terminalStatus: terminalObservation.terminalStatus });
    assert.deepEqual({ runId: notice.runId, stageAttemptId: notice.stageAttemptId, completionId: notice.completionId }, { runId: binding.runId, stageAttemptId: started.binding.stageAttemptId, completionId: terminalObservation.completionId }, "completion callback resolves exact canonical identities");
    assert.equal((await conductor.status(fx.ctx, binding.runId)).state.revision, beforeNoticeRevision, "completion notification itself does not mutate canonical DAG state");
    const completionAction = (await conductor.nextAction(fx.ctx, binding.runId)).frontier.find((candidate) => candidate.operation === "record_completion" && candidate.stageAttemptId === notice.stageAttemptId);
    assert(completionAction, "terminal pure read exposes one exact record-completion action");
    const recorded = await conductor.recordCompletion(fx.ctx, binding.runId, completionAction.actionId, notice.stageAttemptId, notice.completionId);
    assert.equal(recorded.state.stageAttempts[notice.stageAttemptId].workerResult.id, notice.completionId, "dag_record_completion records exactly the notified completion");
    await assert.rejects(() => conductor.recordCompletion(fx.ctx, binding.runId, completionAction.actionId, notice.stageAttemptId, notice.completionId), /stale, consumed/, "consumed action identities cannot replay against replacement state");
    await assert.rejects(() => conductor.recordCompletion(fx.ctx, binding.runId, completionAction.actionId, notice.stageAttemptId, "completion-conflict"), /stale, consumed/, "a different completion identity fails before mutation");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("semantic frontier binds pause, resume, interruption, and concurrent selections exactly", async () => {
  const fx = await fixture("semantic-frontier-guards");
  try {
    const conductor = new DagConductorServiceV1({ lifecycle: { procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }), worker: waitingWorkerAdapter([]) } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx); await approveAndAuthorize(fx, "delivery"); await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    const binding = await conductor.binding(fx.ctx); const initial = await conductor.nextAction(fx.ctx, binding.runId);
    const f0 = initial.frontier.find((candidate) => candidate.operation === "run_checks" && candidate.stage === "F0");
    const pause = initial.controls.find((candidate) => candidate.operation === "pause");
    assert(f0 && pause && initial.controls.some((candidate) => candidate.operation === "cancel"));
    assert.equal(f0.revision, initial.revision); assert.equal(f0.snapshotHash, initial.snapshotHash); assert.equal(f0.candidateGeneration, 0, "action identity exposes its exact revision/snapshot/generation binding");
    const paused = await conductor.pauseSemantic(fx.ctx, binding.runId, pause.actionId, "focused pause test");
    assert.equal(paused.state.desired.run, "paused"); assert.equal(paused.next.frontier.some((candidate) => ["start_work", "run_checks", "retry", "integrate"].includes(candidate.operation)), false, "paused frontier suppresses work and retries");
    const resume = paused.next.controls.find((candidate) => candidate.operation === "resume"); assert(resume, "paused runs always expose exact semantic resume compatibility");
    await assert.rejects(() => conductor.runChecks(fx.ctx, binding.runId, f0.actionId, "commands", "F0"), /stale, consumed/, "pre-pause work selection cannot ABA onto resumed replacement work");
    const resumed = await conductor.resumeSemantic(fx.ctx, binding.runId, resume.actionId, "focused resume test"); assert.equal(resumed.state.desired.run, "running");
    const current = resumed.next.frontier.find((candidate) => candidate.operation === "run_checks" && candidate.stage === "F0"); assert(current);
    const aborted = new AbortController(); aborted.abort(new Error("focused interruption")); const beforeAbort = (await conductor.status(fx.ctx, binding.runId)).state;
    await assert.rejects(() => conductor.runChecks(fx.ctx, binding.runId, current.actionId, "commands", "F0", aborted.signal), /focused interruption/);
    assert.equal((await conductor.status(fx.ctx, binding.runId)).state.snapshotHash, beforeAbort.snapshotHash, "pre-boundary abort performs no hidden canonical mutation");
    let release; const gate = new Promise((resolveGate) => { release = resolveGate; });
    const blocking = new DagConductorServiceV1({ lifecycle: { procedure: { adapterKind: "immutable-catalog-command-v1", allowsProcedure: () => true, async executeExact(input) { await gate; return createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }).executeExact(input); } }, worker: waitingWorkerAdapter([]) } });
    const selected = (await blocking.nextAction(fx.ctx, binding.runId)).frontier.find((candidate) => candidate.operation === "run_checks" && candidate.stage === "F0");
    const first = blocking.runChecks(fx.ctx, binding.runId, selected.actionId, "commands", "F0");
    await new Promise((resolveWait) => setImmediate(resolveWait));
    await assert.rejects(() => blocking.runChecks(fx.ctx, binding.runId, selected.actionId, "commands", "F0"), /already executing concurrently/, "one action identity cannot execute concurrently twice");
    release(); await first; await blocking.detach();
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("semantic check recovery is explicit and does not rotate same-process owner epochs", async () => {
  const fx = await fixture("semantic-recovery");
  try {
    const launches = [];
    let interrupted = false;
    const first = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: waitingWorkerAdapter(launches),
      failpoint(point) { if (!interrupted && point === "after_procedure_reconcile") { interrupted = true; throw new Error("simulated semantic check interruption"); } },
    } });
    const integration = registerDagPlanningIntegrationV1(fx.pi, { getActiveFocus: () => ({ id: "focus-delivery", repositoryRoot: fx.root }), conductor: first });
    await fx.pi.call("dag_plan_save", saveInput(), fx.ctx);
    await approveAndAuthorize(fx, "delivery");
    await integration.handleCommand("run", "--plan delivery@3", fx.ctx);
    const binding = await first.binding(fx.ctx);
    const initial = await first.status(fx.ctx, binding.runId);
    assert.equal(initial.state.revision, 1, "run kickoff does not enter procedure execution");
    const firstAction = (await first.nextAction(fx.ctx, binding.runId)).frontier.find((candidate) => candidate.operation === "run_checks" && candidate.workItemId === "commands" && candidate.stage === "F0");
    await assert.rejects(() => first.runChecks(fx.ctx, binding.runId, firstAction.actionId, "commands", "F0"), /simulated semantic check interruption/);
    const interruptedState = (await first.status(fx.ctx, binding.runId)).state;
    assert(Object.values(interruptedState.effects).some((effect) => effect.kind === "run_procedure" && effect.state === "reconciled"), "explicit tool interruption preserves exact applied effect authority");
    assert(Object.values(interruptedState.stageAttempts).some((attempt) => attempt.stage === "F0" && attempt.state === "running"), "the unsealed attempt remains safely recoverable");
    assert.equal(launches.length, 0, "check recovery never launches owned work");

    await first.detach();
    const resumed = new DagConductorServiceV1({ lifecycle: { procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }), worker: waitingWorkerAdapter(launches) } });
    const recoveryAction = (await resumed.nextAction(fx.ctx, binding.runId)).frontier.find((candidate) => candidate.operation === "run_checks" && candidate.workItemId === "commands" && candidate.stage === "F0");
    const recovered = await resumed.runChecks(fx.ctx, binding.runId, recoveryAction.actionId, "commands", "F0");
    assert.equal(recovered.state.workItems.commands.stages.F0.state, "passed");
    assert.equal(recovered.state.owner.ownerEpoch, interruptedState.owner.ownerEpoch, "same-process semantic tools derive the durable lock without service-generation transfer");
    const start = recovered.next.frontier.find((candidate) => candidate.operation === "start_work" && candidate.workItemId === "commands" && candidate.stage === "F1");
    assert(start, "recovery returns the next exact semantic frontier");
    await resumed.startWork(fx.ctx, binding.runId, start.actionId, "commands", "F1", null);
    assert.equal(launches.length, 1, "only explicit semantic start crosses the worker launch boundary");
    await resumed.detach();
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
