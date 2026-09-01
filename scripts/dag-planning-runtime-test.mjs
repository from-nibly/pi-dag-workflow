import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { validationExecutableIdentityMatchesV1 } from "../extensions/dag-workflow/dag-runtime/integration-driver.ts";
import {
  canonicalHash,
  canonicalStringify,
  buildDagOwnedWorkerPromptV1,
  normalizeDagTacticalDirectiveV1,
  DagConductorServiceV1,
  DagRunSnapshotStoreV1,
  parseStrictJson,
  validateCanonicalDagPlanV1,
  validateDagRunStateShapeV1,
  validateDagRunStateV1,
} from "../extensions/dag-workflow/dag-runtime/index.ts";
import { semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { createDagPlanningPlanV1 } from "../extensions/dag-workflow/planning/artifact.ts";
import {
  createBuiltInLifecycleProcedureAdapterV1,
  prepareDagRunV1,
} from "../extensions/dag-workflow/planning/runtime-adapter.ts";

const run = promisify(execFile);
const AT = "2026-08-14T12:00:00.000Z";
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

async function git(cwd, args) {
  const result = await run("git", args, { cwd, encoding: "utf8", env: { ...process.env, LC_ALL: "C", LANG: "C", GIT_AUTHOR_NAME: "Runtime Adapter Test", GIT_AUTHOR_EMAIL: "runtime-adapter@example.invalid", GIT_COMMITTER_NAME: "Runtime Adapter Test", GIT_COMMITTER_EMAIL: "runtime-adapter@example.invalid", GIT_AUTHOR_DATE: AT, GIT_COMMITTER_DATE: AT } });
  return result.stdout.trim();
}

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "dag-planning-runtime-"));
  await git(root, ["init", "-b", "main"]);
  const decision = {
    id: "DEC-runtime-adapter", title: "Ship the runtime adapter", body: "Compile approved thin plans into the exact shipped DAG runtime contracts.", state: "accepted",
    scope: { kind: "repository" }, introducedBy: "user", sourceRefs: ["runtime-adapter-test"], relationships: [], createdAt: AT, updatedAt: AT,
    rationale: "Exercise a production compatibility boundary.",
  };
  decision.acceptance = { mode: "direct_direction", actor: "user", acceptedAt: AT, contentHash: semanticHash("decisions", decision), interactionRef: "test:runtime-adapter-approval" };
  const projection = { id: "SPEC-runtime", kind: "spec", path: "spec/runtime/spec.md", title: "Runtime adapter", sections: [{ id: "direction", title: "Direction", objectIds: [decision.id] }] };
  const model = {
    schemaVersion: 1,
    project: { id: "runtime-adapter-project", title: "Runtime adapter fixture", revision: 7, mode: "authoritative", createdAt: AT, updatedAt: AT, projections: { specs: [projection] } },
    workstreams: [], intents: [], concepts: [], evidence: [], assumptions: [], questions: [], tensions: [], scenarios: [], proposals: [], decisions: [decision], commitments: [], discoveries: [],
  };
  await mkdir(join(root, "project-model"), { recursive: true });
  await mkdir(join(root, "spec/runtime"), { recursive: true });
  const specBytes = "# Runtime adapter\n\nExact generated specification bytes.\n";
  await writeFile(join(root, "project-model/model.json"), `${JSON.stringify(model, null, 2)}\n`);
  await writeFile(join(root, "spec/runtime/spec.md"), specBytes);
  await writeFile(join(root, "tracked.txt"), "baseline\n");
  await git(root, ["add", "."]);
  await git(root, ["commit", "-m", "test: establish runtime adapter baseline"]);
  const baselineCommit = await git(root, ["rev-parse", "HEAD"]);
  const baselineTree = await git(root, ["rev-parse", "HEAD^{tree}"]);
  const common = await realpath(await git(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"]));
  const objectFormat = await git(root, ["rev-parse", "--show-object-format"]);
  const repositoryId = `repo-${createHash("sha256").update(`${common}\0${objectFormat}`).digest("hex").slice(0, 32)}`;
  return { root, baselineCommit, baselineTree, repositoryId, modelSemanticHash: semanticHash("decisions", decision), specContentHash: `sha256:${createHash("sha256").update(specBytes).digest("hex")}` };
}

function thinPlan({ baselineCommit, baselineTree, repositoryId, modelSemanticHash, specContentHash, sourceRefs, prefixCommands, finalCommands } = {}) {
  return createDagPlanningPlanV1({
    planId: "thin-runtime", status: "ready", title: "Prepare the runtime compatibility adapter", focusId: null,
    repository: { repositoryId, baselineCommit, baselineTree, targetBranch: "main" },
    source: { refs: sourceRefs ?? [{ kind: "project_model_object", collection: "decisions", objectId: "DEC-runtime-adapter", semanticHash: modelSemanticHash, summary: "Accepted model direction." }, { kind: "generated_spec", path: "spec/runtime/spec.md", contentHash: specContentHash, summary: "Generated specification bytes." }], scopeSummary: "Compile the approved thin plan without treating compatibility shims as security receipts." },
    architecture: { outcomes: [{ id: "out-adapter", description: "A valid canonical plan and genesis run are prepared." }], nonGoals: ["Do not create receipt chains."], notes: ["Use the shipped runtime validators."], risks: ["Git or model drift must block preparation."] },
    workItems: [
      { id: "compile", title: "Compile canonical inputs", objective: "Join model and spec sources and compile canonical runtime inputs.", outcomeIds: ["out-adapter"], context: ["Use exact source bytes."], checks: ["The canonical validator accepts the plan."], dependsOn: [], risk: "high", riskNotes: ["Source drift is authority-sensitive."] },
      { id: "verify", title: "Verify genesis", objective: "Validate the exact genesis state and runtime context.", outcomeIds: ["out-adapter"], context: ["Use the normalized scheduler index."], checks: ["The run-state validator accepts genesis."], dependsOn: ["compile"], risk: "medium", riskNotes: [] },
    ],
    constraints: { maxConcurrency: 2, mutexGroups: [{ id: "shared-runtime", workItemIds: ["compile", "verify"], reason: "The dependency already provides the stronger serialization boundary." }] },
    integration: {
      strategy: "dependency_order",
      checks: ["Validate the exact composed proposal."],
      finalChecks: ["Validate the exact final proposal."],
      prefixCommands: prefixCommands ?? [{ id: "prefix-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
      finalCommands: finalCommands ?? [{ id: "final-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
    },
    approval: { status: "approved", by: "user", at: AT, note: "Approved for adapter compilation." },
    authorization: { status: "authorized", by: "user", at: AT, scope: ["compile", "verify"], maxConcurrency: 2, note: "Compatibility run authorized." },
  }, AT);
}

function prepareInput(fx, planningPlan = thinPlan(fx)) {
  return { planningPlan, repositoryRoot: fx.root, runId: "run-runtime-adapter", runNonce: "runtime-adapter-nonce-0001", createdAt: AT };
}

function assertSelfHashed(fact) {
  const core = { ...fact };
  delete core.hash;
  assert.equal(fact.hash, canonicalHash(core), `${fact.kind} carries an exact canonical self-hash`);
}

function attempt(stage, workerResult = null) {
  return {
    stageAttemptId: `attempt-${stage.toLowerCase()}`, workItemId: "compile", stage, ordinal: 1,
    producerKind: ({ F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" })[stage],
    implementationLineageHash: null, inputGeneration: stage === "F0" ? 0 : 1, reservedOutputGeneration: null,
    attemptInput: { kind: "stage_attempt_input", schemaVersion: 1, id: `input-${stage.toLowerCase()}`, hash: canonicalHash({ input: stage }), bytes: 1, mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null },
    authorizationSetHash: canonicalHash({ authorization: true }), state: "evidence_pending", launchIntentId: null, leaseIds: [], workerResult,
    evidence: null, failure: null, createdAt: AT, updatedAt: AT, terminalAt: AT,
  };
}

test("agent dispatch is the sole fresh-launch boundary with exact CAS, replay, prompt, completion, and legacy compatibility", async () => {
  const fx = await fixture();
  try {
    const prepared = await prepareDagRunV1(prepareInput(fx));
    const ctx = { cwd: fx.root, sessionManager: { getSessionId: () => "dispatch-session", getSessionFile: () => null, getHeader: () => ({ type: "session", id: "dispatch-session", cwd: fx.root }) } };
    let launches = 0;
    let terminal = null;
    let capturedRequest = null;
    let crashAfterMark = false;
    const procedure = createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root });
    const lifecycle = {
      procedure,
      candidate: {
        async inspectAndSealCandidate({ plan, state, attempt, repositoryId }) {
          const item = state.workItems[attempt.workItemId]; const base = plan.repositories.find((repository) => repository.repositoryId === repositoryId).baseline;
          const git = { repositoryId, commit: fx.baselineCommit, tree: fx.baselineTree };
          const core = { kind: "candidate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: item.workItemId, generation: item.candidateGeneration + 1, candidateId: `candidate-${attempt.stageAttemptId}`, base, git, patchIdentityHash: canonicalHash({ base, git }), producedByStageAttemptId: attempt.stageAttemptId, lineageHash: item.implementationLineageHash };
          return { candidate: { ...core, hash: canonicalHash(core) }, workerOutput: terminal?.workerOutput };
        },
      },
      worker: {
        async launchExact(request, state) {
          launches += 1; capturedRequest = structuredClone(request);
          const attemptNonce = `dispatch-attempt-nonce-${request.workerId}`;
          const config = { storageId: "dispatch-store", ownerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, launchKey: request.launchKey, requestHash: request.configRequestHash, task: request.task, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity } };
          const configHash = canonicalHash(config); const configCore = { kind: "worker_config", configHash, config };
          return { workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash, configFact: { ...configCore, hash: canonicalHash(configCore) }, supervisorPid: process.pid, supervisorStartIdentity: `test-process-${process.pid}`, childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: AT };
        },
        async readTerminalExact() { return terminal; },
      },
      failpoint(point) { if (crashAfterMark && point === "after_owned_dispatch_mark") { crashAfterMark = false; throw new Error("simulated crash after agent dispatch mark"); } },
    };
    let service = new DagConductorServiceV1({ lifecycle });
    await service.startPrepared(ctx, { runId: prepared.genesis.runId, runNonce: prepared.genesis.runNonce, planHash: prepared.canonicalPlan.planHash, maxActiveNodes: prepared.genesis.scheduler.maxActiveNodes, occurredAt: AT, plan: prepared.canonicalPlan, genesis: prepared.genesis, context: prepared.context, seedFacts: [...prepared.seedFacts], sourcePlanningPlanId: "thin-runtime", sourcePlanningPlanHash: canonicalHash({ source: "thin-runtime" }) });
    await service.activate(ctx, prepared.genesis.runId, AT);
    const ready = await service.status(ctx, prepared.genesis.runId);
    assert.equal(ready.readyPackets.length, 1, "F0 reconciles and leaves one exact agent-visible F1 ready packet");
    const packet = ready.readyPackets[0];
    assert.equal(launches, 0, "start/session-style conductor activation never launches a fresh owned worker");
    await service.activate(ctx, prepared.genesis.runId, AT);
    assert.equal(launches, 0, "repeated background-style wakes remain reconciliation-only");

    await assert.rejects(() => service.dispatch(ctx, { ...packet, expectedRevision: packet.expectedRevision - 1 }, null, AT), /stale|does not match/, "stale ready packet revision is rejected before launch");
    await assert.rejects(() => service.dispatch(ctx, { ...packet, expectedSnapshotHash: canonicalHash({ stale: true }) }, null, AT), /stale|does not match/, "stale ready packet snapshot CAS is rejected before launch");
    await assert.rejects(() => service.dispatch(ctx, { ...packet, ownerEpoch: packet.ownerEpoch + 1 }, null, AT), /owner authority|stale/, "stale ready packet owner CAS is rejected before launch");
    assert.equal(launches, 0);

    const directive = "  Prefer the existing module boundary.\r\nKeep diagnostics bounded.  ";
    crashAfterMark = true;
    await assert.rejects(() => service.dispatch(ctx, packet, directive, AT), /simulated crash after agent dispatch mark/);
    assert.equal(launches, 0, "a crash after durable mark never launches in a background recovery path");
    await service.detach();
    service = new DagConductorServiceV1({ lifecycle });
    await service.resumeBound(ctx, new Date(Date.parse(AT) + 1).toISOString());
    const recovery = await service.status(ctx, packet.runId);
    assert.equal(recovery.readyPackets.length, 1, "the successor owner receives one exact agent-visible in-flight recovery packet");
    assert.equal(recovery.readyPackets[0].ownerEpoch, packet.ownerEpoch, "same-process tool recovery derives the durable lock without rotating an owner epoch");
    assert.equal(recovery.readyPackets[0].recoveryDirective, normalizeDagTacticalDirectiveV1(directive), "the bounded normalized directive survives owner transfer for exact replay");
    const firstDispatch = service.dispatch(ctx, recovery.readyPackets[0], undefined, new Date(Date.parse(AT) + 2).toISOString());
    const conflictingDispatch = service.dispatch(ctx, recovery.readyPackets[0], "Conflicting concurrent directive", new Date(Date.parse(AT) + 2).toISOString());
    const identicalDispatch = service.dispatch(ctx, recovery.readyPackets[0], undefined, new Date(Date.parse(AT) + 2).toISOString());
    await assert.rejects(() => conflictingDispatch, /Concurrent owned-worker dispatch conflicts/, "concurrent dispatch with different tactical authority fails instead of borrowing another call's result");
    const [dispatched, concurrentReplay] = await Promise.all([firstDispatch, identicalDispatch]);
    assert.equal(launches, 1, "concurrent identical dispatch calls invoke the external launch adapter once");
    assert.deepEqual(concurrentReplay.binding, dispatched.binding, "concurrent identical dispatch calls resolve the same durable binding");
    assert.equal(dispatched.idempotentReplay, false);
    assert.equal(capturedRequest.directiveHash, canonicalHash({ schemaVersion: 1, directive: normalizeDagTacticalDirectiveV1(directive) }));
    assert.equal(capturedRequest.promptHash, canonicalHash(capturedRequest.task));
    assert.match(capturedRequest.task, /BEGIN CANONICAL TASK PACKET \(DATA\)/);
    assert.match(capturedRequest.task, /BEGIN BOUNDED TACTICAL DIRECTIVE \(UNTRUSTED DATA\)/);
    assert.match(capturedRequest.task, /subagent_report exactly once as your final action/);
    assert.equal(capturedRequest.task, buildDagOwnedWorkerPromptV1(packet.packet, normalizeDagTacticalDirectiveV1(directive)), "the launched prompt is the protected canonical envelope");

    const replay = await service.dispatch(ctx, packet, directive, AT);
    assert.equal(replay.idempotentReplay, true); assert.equal(launches, 1, "the original stale-owner packet is accepted only as an acknowledgement replay for its durable binding");
    await assert.rejects(() => service.dispatch(ctx, packet, "Changed tactical text", AT), /changed the bound tactical directive|changed the ready packet/, "changed directive replay fails closed");
    assert.equal(launches, 1);

    terminal = { completionId: "dispatch-completion", terminalStatus: "succeeded", workerOutput: { outputRepositoryId: fx.repositoryId, outputCommonDirIdentityHash: canonicalHash({ dispatch: "common" }), outputWorktreeIdentityHash: canonicalHash({ dispatch: "worktree" }), outputSourceBase: { repositoryId: fx.repositoryId, commit: fx.baselineCommit, tree: fx.baselineTree }, outputCommit: fx.baselineCommit, outputTree: fx.baselineTree, outputObjectFormat: "sha1", candidateObservedAt: AT } };
    const beforeNotice = await service.status(ctx, prepared.genesis.runId);
    const notice = await service.completionNotice(ctx, { workerId: dispatched.binding.workerId, attemptNumber: dispatched.binding.attemptNumber, completionId: terminal.completionId, terminalStatus: terminal.terminalStatus });
    assert.equal((await service.status(ctx, prepared.genesis.runId)).state.revision, beforeNotice.state.revision, "owned-worker completion notification is read-only");
    const completionAction = (await service.nextAction(ctx, prepared.genesis.runId)).frontier.find((candidate) => candidate.operation === "record_completion" && candidate.stageAttemptId === notice.stageAttemptId);
    assert(completionAction, "terminal observation exposes the exact semantic completion action");
    const reconciled = await service.recordCompletion(ctx, prepared.genesis.runId, completionAction.actionId, notice.stageAttemptId, notice.completionId);
    const attempt = reconciled.state.stageAttempts[packet.stageAttemptId];
    assert(attempt.workerResult, "explicit semantic completion tool records the exact terminal result after agent dispatch");
    assert.equal(reconciled.state.workerBindings[packet.stageAttemptId].resultHash, attempt.workerResult.hash);
    const acknowledgementAfterRelease = await service.dispatch(ctx, packet, directive, AT);
    assert.equal(acknowledgementAfterRelease.idempotentReplay, true);
    assert.equal(canonicalHash(acknowledgementAfterRelease.binding), canonicalHash(reconciled.state.workerBindings[packet.stageAttemptId]), "acknowledgement-loss replay returns the durable binding after explicit terminal recording");
    const oversized = structuredClone(packet); oversized.packet.checks = [{ nested: { value: "x".repeat(17 * 1024) } }];
    await assert.rejects(() => service.dispatch(ctx, oversized, directive, AT), /oversized string|bounded canonical/, "nested packet data is bounded before dispatch hashing");
    const tooDeep = structuredClone(packet); let nested = {}; tooDeep.packet.checks = [nested];
    for (let depth = 0; depth < 20; depth += 1) { nested.child = {}; nested = nested.child; }
    await assert.rejects(() => service.dispatch(ctx, tooDeep, directive, AT), /bounded nested schema complexity/, "deep packet data is rejected by iterative bounds before dispatch hashing");
    await assert.rejects(() => service.dispatch(ctx, packet, "x".repeat(100_000), AT), /Tactical directive exceeds the bounded canonical dispatch limit/, "an unbounded directive is normalized and rejected before conductor request hashing");

    const legacy = structuredClone(ready.state); const legacyAttempt = legacy.stageAttempts[packet.stageAttemptId]; const legacyLaunch = legacy.launchIntents[legacyAttempt.launchIntentId];
    delete legacyLaunch.dispatchProtocolVersion; delete legacyLaunch.readyPacketHash; delete legacyLaunch.normalizedDirective; delete legacyLaunch.directiveHash; delete legacyLaunch.promptHash; delete legacyLaunch.dispatchConfigRequestHash;
    legacyAttempt.state = "launching"; legacyLaunch.state = "reserved"; legacy.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(legacy).filter(([key]) => key !== "snapshotHash")));
    const legacyValidation = validateDagRunStateShapeV1(legacy);
    assert.equal(legacyValidation.ok, true, JSON.stringify(legacyValidation.issues));
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("launch-before-bind owner transfer exposes exact agent recovery without autonomous relaunch", async () => {
  const fx = await fixture();
  try {
    const prepared = await prepareDagRunV1(prepareInput(fx));
    const ctx = { cwd: fx.root, sessionManager: { getSessionId: () => "launch-bind-recovery", getSessionFile: () => null, getHeader: () => ({ type: "session", id: "launch-bind-recovery", cwd: fx.root }) } };
    let crashAfterLaunch = true; let adapterCalls = 0; let physicalLaunches = 0; let durableObservation = null; const requests = [];
    const lifecycle = {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: {
        async launchExact(request, state) {
          adapterCalls += 1; requests.push(structuredClone(request));
          if (durableObservation) return durableObservation;
          physicalLaunches += 1;
          const attemptNonce = `launch-bind-nonce-${request.workerId}`;
          const config = { storageId: "launch-bind-store", ownerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, launchKey: request.launchKey, requestHash: request.configRequestHash, task: request.task, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity } };
          const configHash = canonicalHash(config); const core = { kind: "worker_config", configHash, config };
          durableObservation = { workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash, configFact: { ...core, hash: canonicalHash(core) }, supervisorPid: process.pid, supervisorStartIdentity: `launch-bind-process-${process.pid}`, childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: AT };
          return durableObservation;
        },
        async readTerminalExact() { return null; },
      },
      failpoint(point) { if (crashAfterLaunch && point === "after_owned_worker_launch") { crashAfterLaunch = false; throw new Error("simulated crash after launch before bind"); } },
    };
    let service = new DagConductorServiceV1({ lifecycle });
    await service.startPrepared(ctx, { runId: prepared.genesis.runId, runNonce: prepared.genesis.runNonce, planHash: prepared.canonicalPlan.planHash, maxActiveNodes: prepared.genesis.scheduler.maxActiveNodes, occurredAt: AT, plan: prepared.canonicalPlan, genesis: prepared.genesis, context: prepared.context, seedFacts: [...prepared.seedFacts], sourcePlanningPlanId: "thin-runtime", sourcePlanningPlanHash: canonicalHash({ source: "thin-runtime" }) });
    await service.activate(ctx, prepared.genesis.runId, AT); const initial = (await service.status(ctx, prepared.genesis.runId)).readyPackets[0];
    await assert.rejects(() => service.dispatch(ctx, initial, "Retain this exact directive.", AT), /simulated crash after launch before bind/);
    assert.equal(physicalLaunches, 1); assert.equal(Object.keys((await service.status(ctx, initial.runId)).state.workerBindings).length, 0);
    await service.activate(ctx, initial.runId, AT); assert.equal(adapterCalls, 1, "background reconciliation never invokes the launch adapter at the pre-bind boundary");
    await service.detach(); service = new DagConductorServiceV1({ lifecycle });
    await service.resumeBound(ctx, new Date(Date.parse(AT) + 1).toISOString());
    const recovery = await service.status(ctx, initial.runId);
    assert.equal(recovery.readyPackets.length, 1); assert.equal(recovery.readyPackets[0].recoveryDirective, "Retain this exact directive.");
    assert.equal(physicalLaunches, 1, "owner transfer only prepares recovery authority");
    const rebound = await service.dispatch(ctx, recovery.readyPackets[0], undefined, new Date(Date.parse(AT) + 2).toISOString());
    assert(rebound.state.workerBindings[initial.stageAttemptId], "explicit successor-epoch dispatch binds the exact already-launched worker");
    assert.equal(adapterCalls, 2); assert.equal(physicalLaunches, 1, "opaque adapter replay recovers rather than physically relaunching");
    assert.equal(requests[1].configRequestHash, requests[0].configRequestHash); assert.equal(requests[1].task, requests[0].task, "prompt and directive identity remain exact across launch-before-bind owner recovery");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("legacy pre-bind snapshots require explicit agent dispatch and preserve the old request identity", async () => {
  const fx = await fixture();
  try {
    const prepared = await prepareDagRunV1(prepareInput(fx));
    const ctx = { cwd: fx.root, sessionManager: { getSessionId: () => "legacy-dispatch-session", getSessionFile: () => null, getHeader: () => ({ type: "session", id: "legacy-dispatch-session", cwd: fx.root }) } };
    let launches = 0; let captured = null;
    const service = new DagConductorServiceV1({ lifecycle: {
      procedure: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root }),
      worker: {
        async launchExact(request, state) {
          launches += 1; captured = structuredClone(request);
          const attemptNonce = `legacy-attempt-nonce-${request.workerId}`;
          const config = { storageId: "legacy-dispatch-store", ownerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, launchKey: request.launchKey, requestHash: request.configRequestHash, task: request.task, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity } };
          const configHash = canonicalHash(config); const core = { kind: "worker_config", configHash, config };
          return { workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash, configFact: { ...core, hash: canonicalHash(core) }, supervisorPid: process.pid, supervisorStartIdentity: `legacy-process-${process.pid}`, childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: AT };
        },
        async readTerminalExact() { return null; },
      },
    } });
    await service.startPrepared(ctx, { runId: prepared.genesis.runId, runNonce: prepared.genesis.runNonce, planHash: prepared.canonicalPlan.planHash, maxActiveNodes: prepared.genesis.scheduler.maxActiveNodes, occurredAt: AT, plan: prepared.canonicalPlan, genesis: prepared.genesis, context: prepared.context, seedFacts: [...prepared.seedFacts], sourcePlanningPlanId: "thin-runtime", sourcePlanningPlanHash: canonicalHash({ source: "thin-runtime" }) });
    await service.activate(ctx, prepared.genesis.runId, AT);
    const modern = await service.status(ctx, prepared.genesis.runId); const modernPacket = modern.readyPackets[0];
    const legacy = structuredClone(modern.state); const legacyAttempt = legacy.stageAttempts[modernPacket.stageAttemptId]; const legacyLaunch = legacy.launchIntents[legacyAttempt.launchIntentId];
    delete legacyLaunch.dispatchProtocolVersion; delete legacyLaunch.readyPacketHash; delete legacyLaunch.normalizedDirective; delete legacyLaunch.directiveHash; delete legacyLaunch.promptHash; delete legacyLaunch.dispatchConfigRequestHash;
    legacyAttempt.state = "launching"; legacyLaunch.state = "reserved";
    legacy.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(legacy).filter(([key]) => key !== "snapshotHash")));
    const store = new DagRunSnapshotStoreV1(join(fx.root, ".ai", "dag-runs-v1"), legacy.runId); const bytes = canonicalStringify(legacy);
    await writeFile(join(store.snapshotsDirectory, `${legacy.snapshotHash.slice(7)}.json`), bytes);
    await writeFile(store.statePath, bytes);

    await service.activate(ctx, legacy.runId, AT);
    assert.equal(launches, 0, "legacy recovery wakes never launch autonomously");
    const status = await service.status(ctx, legacy.runId);
    assert.equal(status.readyPackets.length, 1, "legacy launching/reserved authority is upgraded to one explicit agent-visible recovery packet");
    assert.equal(status.readyPackets[0].dispatchProtocolVersion, 0);
    const dispatched = await service.dispatch(ctx, status.readyPackets[0], null, AT);
    assert.equal(launches, 1, "only explicit dag_run_dispatch crosses the legacy fresh-launch boundary");
    assert.equal(captured.configRequestHash, legacyLaunch.configRequestHash, "legacy dispatch preserves the old config request identity");
    assert.equal(captured.task, canonicalStringify(status.readyPackets[0].packet), "legacy dispatch preserves the original canonical packet task rather than upgrading its prompt identity");
    assert(dispatched.state.workerBindings[modernPacket.stageAttemptId], "legacy explicit dispatch executes through durable binding");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("prepares a valid canonical plan, scheduler context, genesis, and strict validation command", async () => {
  const fx = await fixture();
  try {
    const prepared = await prepareDagRunV1(prepareInput(fx));
    assert.equal(validateCanonicalDagPlanV1(prepared.canonicalPlan).ok, true);
    const runValidation = validateDagRunStateV1(prepared.genesis, prepared.context);
    assert.equal(runValidation.ok, true, JSON.stringify(runValidation.issues));
    assert.equal(prepared.genesis.scheduler.normalizedIndexHash, prepared.schedulerIndex.indexHash);
    assert.equal(prepared.context.authorization.hash, prepared.genesis.identity.authorizationSet.hash);
    assert.equal(prepared.canonicalPlan.modelBinding.closure.entries[0].id, "DEC-runtime-adapter");
    assert.match(prepared.canonicalPlan.modelBinding.specs[0].contentHash, /^sha256:/);
    assert.equal(prepared.canonicalPlan.constraints.semanticMutexes.length, 0, "a dependency-normalized redundant mutex is not duplicated into the canonical constraint graph");
    assert.equal(prepared.seedFacts.every((fact) => fact.compatibility === "internal_runtime_adapter_v1" || fact.planHash === prepared.canonicalPlan.planHash), true);

    const profile = Object.values(prepared.context.integrationValidationProfiles)[0];
    const result = await run(profile.argv[0], profile.argv.slice(1), { cwd: fx.root, encoding: "utf8", env: profile.environment });
    const parsed = parseStrictJson(result.stdout);
    assert.equal(parsed.disposition, "PASS");
    assert.deepEqual(Object.keys(parsed), ["disposition"]);
    assert.equal(result.stderr, "");
    const nodeHash = `sha256:${createHash("sha256").update(await readFile(profile.argv[0])).digest("hex")}`;
    const helperHash = `sha256:${createHash("sha256").update(await readFile(profile.argv[1])).digest("hex")}`;
    assert.equal(profile.executableArtifactHash, canonicalHash({ executableHash: nodeHash, argvArtifacts: [{ index: 1, hash: helperHash }] }), "profile identity binds both executable and absolute helper bytes");

    const failingPlan = thinPlan({ ...fx, prefixCommands: [{ id: "prefix-fails", argv: [process.execPath, "-e", "process.exit(7)"] }] });
    const failingPrepared = await prepareDagRunV1(prepareInput(fx, failingPlan));
    const failingProfile = Object.values(failingPrepared.context.integrationValidationProfiles).find(({ profileId }) => profileId.includes("prefix"));
    const failed = await run(failingProfile.argv[0], failingProfile.argv.slice(1), { cwd: fx.root, encoding: "utf8", env: failingProfile.environment });
    assert.equal(parseStrictJson(failed.stdout).disposition, "FAIL", "a plan-bound failing command cannot be promoted to integration PASS");
    assert.equal(await validationExecutableIdentityMatchesV1(profile), true, "the integration driver accepts the exact combined executable/helper identity");
    const alteredHelper = join(fx.root, "altered-validation-helper.mjs");
    await writeFile(alteredHelper, "process.stdout.write('{}')\n");
    assert.equal(await validationExecutableIdentityMatchesV1({ ...profile, argv: [profile.argv[0], alteredHelper, ...profile.argv.slice(2)] }), false, "changed helper bytes cannot retain plan-bound executable authority");
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("mapping is deterministic for exact thin plan, Git baseline, IDs, and time", async () => {
  const fx = await fixture();
  try {
    const input = prepareInput(fx);
    const first = await prepareDagRunV1(input);
    const second = await prepareDagRunV1(structuredClone(input));
    assert.deepEqual(second.canonicalPlan, first.canonicalPlan);
    assert.deepEqual(second.genesis, first.genesis);
    assert.deepEqual(second.context, first.context);
    assert.deepEqual(second.seedFacts, first.seedFacts);
    assert.deepEqual(second.schedulerIndex, first.schedulerIndex);
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

test("stale target baseline, dirty root, and unresolved source joins block before preparation", async () => {
  const stale = await fixture();
  try {
    const input = prepareInput(stale);
    await writeFile(join(stale.root, "tracked.txt"), "target drift\n");
    await git(stale.root, ["add", "tracked.txt"]);
    await git(stale.root, ["commit", "-m", "test: drift target"]);
    await assert.rejects(() => prepareDagRunV1(input), /Stale planned Git target|HEAD no longer equals/);
  } finally { await rm(stale.root, { recursive: true, force: true }); }

  const dirty = await fixture();
  try {
    await writeFile(join(dirty.root, "untracked.txt"), "dirty\n");
    await assert.rejects(() => prepareDagRunV1(prepareInput(dirty)), /must be clean/);
  } finally { await rm(dirty.root, { recursive: true, force: true }); }

  const branch = await fixture();
  try {
    await git(branch.root, ["branch", "same-commit-other"]);
    await git(branch.root, ["checkout", "same-commit-other"]);
    await assert.rejects(() => prepareDagRunV1(prepareInput(branch)), /Checked-out branch differs from the planned target branch/);
  } finally { await rm(branch.root, { recursive: true, force: true }); }

  const cloneSource = await fixture();
  const cloneParent = await mkdtemp(join(tmpdir(), "dag-planning-clone-"));
  const cloneRoot = join(cloneParent, "copy");
  try {
    await git(cloneParent, ["clone", "--no-hardlinks", cloneSource.root, cloneRoot]);
    await assert.rejects(
      () => prepareDagRunV1({ ...prepareInput(cloneSource), repositoryRoot: cloneRoot }),
      /common-dir\/object-format identity differs from the planned repository/,
    );
  } finally {
    await rm(cloneSource.root, { recursive: true, force: true });
    await rm(cloneParent, { recursive: true, force: true });
  }

  const mismatch = await fixture();
  try {
    const plan = thinPlan({ ...mismatch, sourceRefs: [{ kind: "project_model_object", collection: "decisions", objectId: "DEC-runtime-adapter", semanticHash: `sha256:${"0".repeat(64)}`, summary: "A stale typed source binding." }] });
    await assert.rejects(() => prepareDagRunV1(prepareInput(mismatch, plan)), /Source mismatch/);
    const wrongSpec = thinPlan({ ...mismatch, sourceRefs: [
      { kind: "project_model_object", collection: "decisions", objectId: "DEC-runtime-adapter", semanticHash: mismatch.modelSemanticHash },
      { kind: "generated_spec", path: "spec/runtime/spec.md", contentHash: `sha256:${"f".repeat(64)}` },
    ] });
    await assert.rejects(() => prepareDagRunV1(prepareInput(mismatch, wrongSpec)), /Source mismatch/);
  } finally { await rm(mismatch.root, { recursive: true, force: true }); }
});

test("built-in lifecycle adapter emits exact F0, F2, and F8 fact bundles with pragmatic Git checks", async () => {
  const fx = await fixture();
  try {
    const prepared = await prepareDagRunV1(prepareInput(fx));
    const adapter = createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: fx.root });
    const procedureFor = (stage) => Object.values(prepared.context.catalog.procedures).find((procedure) => procedure.stages[0] === stage);
    const historicalF0 = structuredClone(procedureFor("F0"));
    historicalF0.executable.argv[0] = "/nix/store/historical-node/bin/node";
    historicalF0.executable.executableArtifactHash = `sha256:${"1".repeat(64)}`;
    const { hash: _historicalHash, ...historicalF0Core } = historicalF0;
    historicalF0.hash = canonicalHash(historicalF0Core);
    assert.equal(adapter.allowsProcedure(historicalF0), true, "built-in compatibility recognizes an exact prior Node artifact mapping");
    const f0Attempt = attempt("F0");
    const f0 = await adapter.executeExact({ plan: prepared.canonicalPlan, state: prepared.genesis, attempt: f0Attempt, procedure: historicalF0, effectId: "effect-f0", requestHash: canonicalHash({ effect: "F0" }), executionRequest: {} });
    assert.equal(f0.checkAggregate.disposition, "PASS");
    assert.equal(f0.evidence.candidateHash, null);
    assertSelfHashed(f0.checkAggregate); assertSelfHashed(f0.evidence);

    await writeFile(join(fx.root, "candidate.txt"), "candidate\n");
    await git(fx.root, ["add", "candidate.txt"]);
    await git(fx.root, ["commit", "-m", "test: candidate"]);
    const commit = await git(fx.root, ["rev-parse", "HEAD"]);
    const tree = await git(fx.root, ["rev-parse", "HEAD^{tree}"]);
    const candidateCore = { kind: "candidate", commit, tree };
    const state = structuredClone(prepared.genesis);
    state.workItems.compile.candidateGeneration = 1;
    state.workItems.compile.candidate = { generation: 1, candidateId: "candidate-compile", candidateHash: canonicalHash(candidateCore), base: prepared.canonicalPlan.repositories[0].baseline, git: { repositoryId: fx.repositoryId, commit, tree }, patchIdentityHash: canonicalHash({ commit, tree }), producedByStageAttemptId: "attempt-f1", lineageHash: state.workItems.compile.implementationLineageHash };
    const workerResultCore = {
      kind: "worker_result", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
      workItemId: "compile", stage: "F2", stageAttemptId: "attempt-f2", terminalStatus: "succeeded",
      outputRepositoryId: fx.repositoryId, outputCommit: commit, outputTree: tree,
      outputSourceBase: { repositoryId: fx.repositoryId, commit, tree },
      outputCommonDirIdentityHash: canonicalHash({ common: fx.root }), outputWorktreeIdentityHash: canonicalHash({ worktree: fx.root }),
    };
    const workerResultFact = { ...workerResultCore, hash: canonicalHash(workerResultCore) };
    const runStore = new DagRunSnapshotStoreV1(join(fx.root, ".ai/dag-runs-v1"), state.runId);
    const storedWorkerResult = await runStore.putImmutableFact(workerResultFact);
    const workerResult = { kind: "worker_result", schemaVersion: 1, id: "worker-result-f2", hash: storedWorkerResult.hash, bytes: storedWorkerResult.bytes, mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null };
    const f2Attempt = attempt("F2", workerResult);
    const f2 = await adapter.executeExact({ plan: prepared.canonicalPlan, state, attempt: f2Attempt, procedure: procedureFor("F2"), effectId: "effect-f2", requestHash: canonicalHash({ effect: "F2" }), executionRequest: {} });
    assert.equal(f2.checkAggregate.disposition, "PASS");
    assert.equal(f2.oracleAssertions.length, 1);
    assert.equal(f2.oracleAssertions[0].observationHash, workerResult.hash);
    assert(f2.workspaceMaterialization && f2.environmentObservation);
    for (const fact of [f2.checkAggregate, f2.evidence, ...f2.oracleAssertions, ...f2.checkExecutions, f2.workspaceMaterialization, f2.environmentObservation]) assertSelfHashed(fact);

    const needsAttentionCore = { ...workerResultCore, terminalStatus: "needs_attention" };
    const needsAttentionFact = { ...needsAttentionCore, hash: canonicalHash(needsAttentionCore) };
    const storedNeedsAttention = await runStore.putImmutableFact(needsAttentionFact);
    const needsAttentionRef = { ...workerResult, hash: storedNeedsAttention.hash, bytes: storedNeedsAttention.bytes };
    const f2Failed = await adapter.executeExact({ plan: prepared.canonicalPlan, state, attempt: attempt("F2", needsAttentionRef), procedure: procedureFor("F2"), effectId: "effect-f2-fail", requestHash: canonicalHash({ effect: "F2-fail" }), executionRequest: {} });
    assert.equal(f2Failed.checkAggregate.disposition, "FAIL", "a needs-attention evaluator result cannot mint check or oracle PASS evidence");
    assert(f2Failed.oracleAssertions.every(({ disposition }) => disposition === "FAIL"));

    assert.equal(procedureFor("F6").readOnly, true, "thin-plan F6 is review/hardening evidence and cannot silently discard edits");
    const f6ChangedCore = {
      ...workerResultCore, stage: "F6", stageAttemptId: "attempt-f6",
      outputCommit: fx.baselineCommit, outputTree: fx.baselineTree,
    };
    const f6ChangedFact = { ...f6ChangedCore, hash: canonicalHash(f6ChangedCore) };
    const storedF6Changed = await runStore.putImmutableFact(f6ChangedFact);
    const f6ChangedRef = { ...workerResult, id: "worker-result-f6", hash: storedF6Changed.hash, bytes: storedF6Changed.bytes };
    const f6Changed = await adapter.executeExact({ plan: prepared.canonicalPlan, state, attempt: attempt("F6", f6ChangedRef), procedure: procedureFor("F6"), effectId: "effect-f6", requestHash: canonicalHash({ effect: "F6" }), executionRequest: {} });
    assert.equal(f6Changed.checkAggregate.disposition, "FAIL", "F6 output that differs from the exact current candidate cannot pass and be discarded");

    const f8Attempt = attempt("F8");
    const f8 = await adapter.executeExact({ plan: prepared.canonicalPlan, state, attempt: f8Attempt, procedure: procedureFor("F8"), effectId: "effect-f8", requestHash: canonicalHash({ effect: "F8" }), executionRequest: {} });
    assert.equal(f8.checkAggregate.disposition, "PASS");
    assert.equal(f8.integrationReady.candidateHash, state.workItems.compile.candidate.candidateHash);
    assert.equal(f8.integrationReady.f8EvidenceHash, f8.evidence.hash);
    assertSelfHashed(f8.checkAggregate); assertSelfHashed(f8.evidence); assertSelfHashed(f8.integrationReady);
  } finally { await rm(fx.root, { recursive: true, force: true }); }
});

let failures = 0;
for (const [name, fn] of tests) {
  try { await fn(); console.log(`ok - ${name}`); }
  catch (error) { failures += 1; console.error(`not ok - ${name}`); console.error(error?.stack ?? error); }
}
if (failures) process.exitCode = 1;
else console.log(`ok - ${tests.length} DAG planning runtime adapter tests passed`);
