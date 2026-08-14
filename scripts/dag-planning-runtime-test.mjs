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
  DagRunSnapshotStoreV1,
  parseStrictJson,
  validateCanonicalDagPlanV1,
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
    const f0Attempt = attempt("F0");
    const f0 = await adapter.executeExact({ plan: prepared.canonicalPlan, state: prepared.genesis, attempt: f0Attempt, procedure: procedureFor("F0"), effectId: "effect-f0", requestHash: canonicalHash({ effect: "F0" }), executionRequest: {} });
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
