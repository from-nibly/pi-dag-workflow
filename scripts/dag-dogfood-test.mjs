import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_SCHEDULER_POLICY_HASH_V1,
  DagConductorServiceV1,
  DagLifecycleRuntimeV1,
  DagReducerGitIntegrationDriverV1,
  DagRunSnapshotStoreV1,
  PLAN_STAGE_IDS,
  buildSchedulerPlanIndexV1,
  canonicalHash,
  canonicalStringify,
  dagRunStoreLockIdentityFromOwner,
  readRepositoryBindingIdentityV1,
  ownershipChainHashV1,
  scheduleDagRunV1,
  sealCanonicalDagPlanV1,
  sealDagRunStateV1,
} from "../extensions/dag-workflow/dag-runtime/index.ts";

const execFileAsync = promisify(execFile);
const realGitExecutable = (await execFileAsync("sh", ["-c", "command -v git"], { encoding: "utf8" })).stdout.trim();
export const DOGFOOD_AT = "2026-08-11T12:00:00.000Z";
const AT = DOGFOOD_AT;
const H = (char) => `sha256:${char.repeat(64)}`;
const procStat = await readFile(`/proc/${process.pid}/stat`, "utf8");
const processStartIdentity = `linux-proc:${procStat.slice(procStat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
const nodeExecutable = await realpath(process.execPath);
const nodeExecutableArtifactHash = `sha256:${(await import("node:crypto")).createHash("sha256").update(await readFile(nodeExecutable)).digest("hex")}`;
const validationEnvironment = Object.freeze({ LC_ALL: "C", LANG: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" });
const validationEnvironmentProfileId = "dogfood-closed-env-v1";
const validationEnvironmentProfileHash = canonicalHash({ profileId: validationEnvironmentProfileId, environment: validationEnvironment });

function integrationProfilesFixture(options = {}) {
  return Object.fromEntries(["prefix", "final"].map((phase) => {
    const environment = options.validationCounterPath ? { ...validationEnvironment, PI_DAG_VALIDATION_COUNTER: options.validationCounterPath } : validationEnvironment;
    const environmentProfileHash = canonicalHash({ profileId: validationEnvironmentProfileId, environment });
    const disposition = options.validationFailurePhase === phase ? "FAIL" : "PASS";
    const script = options.validationCounterPath ? `require("node:fs").appendFileSync(process.env.PI_DAG_VALIDATION_COUNTER, ${JSON.stringify(`${phase}\n`)}); process.stdout.write(${JSON.stringify(JSON.stringify({ disposition }))})` : `process.stdout.write(${JSON.stringify(JSON.stringify({ disposition }))})`;
    const profile = { profileId: `checks-${phase}`, executableArtifactHash: nodeExecutableArtifactHash, argv: [nodeExecutable, "-e", script], cwdMode: "detached_proposal_worktree", environmentProfileId: validationEnvironmentProfileId, environmentProfileHash, environment, environmentHash: canonicalHash(environment), timeoutMs: 5000, readOnly: true, noEdit: true };
    return [canonicalHash(profile), profile];
  }));
}

const lifecycleCrashPoints = ["after_procedure_intent", "after_procedure_dispatch", "after_procedure_result", "after_procedure_reconcile"];
export const DOGFOOD_SCENARIOS = Object.freeze([
  { id: "happy", group: "baseline", name: "happy", options: () => ({ items: 1 }) },
  { id: "f4PassCrashMatrix", group: "lifecycle", name: "f4-pass-crash-matrix", options: () => ({ items: 1, lifecycleCrashAt: lifecycleCrashPoints, lifecycleCrashStage: "F4" }) },
  { id: "f7FailCrashMatrix", group: "lifecycle", name: "f7-fail-crash-matrix", options: () => ({ items: 1, lifecycleCrashAt: lifecycleCrashPoints, lifecycleCrashStage: "F7", procedureFailureStage: "F7" }) },
  { id: "compositionBeforeDispatchReplay", group: "composition", name: "composition-before-dispatch", options: () => ({ items: 1, crashAt: "after_integration_reserve" }) },
  { id: "compositionAfterDispatchReplay", group: "composition", name: "composition-after-dispatch", options: () => ({ items: 1, crashAt: "after_composition_dispatch" }) },
  { id: "compositionAfterGitReplay", group: "composition", name: "composition-after-git", options: () => ({ items: 1, crashAt: "after_composition_git" }) },
  { id: "compositionAfterCommitReplay", group: "composition", name: "composition-after-commit", options: () => ({ items: 1, crashAt: "after_composition_commit" }) },
  { id: "validationCrashMatrix", group: "validation", name: "validation-crash-matrix", options: () => ({ items: 1, validationCrashPoints: ["after_prefix_validation_intent", "after_prefix_validation_dispatch", "after_prefix_validation_result", "after_prefix_validation_reconcile", "after_final_validation_intent", "after_final_validation_dispatch", "after_final_validation_result", "after_final_validation_reconcile"] }) },
  { id: "validationObservationConflict", group: "validation", name: "validation-observation-conflict", options: () => ({ items: 1, crashAt: "after_prefix_validation_result", validationObservationConflict: true }) },
  { id: "finalValidationFailClosed", group: "validation", name: "final-validation-fail", options: () => ({ items: 1, validationFailurePhase: "final" }) },
  { id: "landingBeforeDispatchReplay", group: "landing", name: "landing-before-dispatch", options: () => ({ items: 1, crashAt: "after_landing_intent" }) },
  { id: "landingAfterDispatchReplay", group: "landing", name: "landing-after-dispatch", options: () => ({ items: 1, crashAt: "after_landing_dispatch" }) },
  { id: "restartReplay", group: "landing", name: "restart", options: () => ({ items: 1, crashAt: "after_landing_git" }) },
  { id: "provenAbsentLandingReplay", group: "landing", name: "landing-proven-absent", options: () => ({ items: 1, provenAbsentLanding: true, crashAt: "after_landing_proven_absent_commit" }) },
  { id: "cleanupReplay", group: "cleanup", name: "cleanup-replay", options: () => ({ items: 1, crashCleanup: true }) },
  { id: "compositionConflict", group: "baseline", name: "conflict", options: () => ({ items: 2, conflictingCandidates: true }) },
  { id: "thirdTargetDrift", group: "baseline", name: "third-drift", options: () => ({ items: 1, thirdTargetDrift: true }) },
]);
export const DOGFOOD_SCENARIO_GROUPS = Object.freeze([...new Set(DOGFOOD_SCENARIOS.map(({ group }) => group))]);

export async function runCurrentDogfoodManifest({ groups = [], scenarios = [] } = {}) {
  const selected = new Set(scenarios);
  for (const group of groups) for (const definition of DOGFOOD_SCENARIOS) if (definition.group === group) selected.add(definition.id);
  const definitions = selected.size ? DOGFOOD_SCENARIOS.filter(({ id }) => selected.has(id)) : DOGFOOD_SCENARIOS;
  for (const id of selected) if (!DOGFOOD_SCENARIOS.some((definition) => definition.id === id)) throw new Error(`Unknown dogfood scenario: ${id}`);
  for (const group of groups) if (!DOGFOOD_SCENARIO_GROUPS.includes(group)) throw new Error(`Unknown dogfood group: ${group}`);
  const root = await mkdtemp(join(tmpdir(), "pi-dag-dogfood-v1-"));
  try {
    const results = {};
    for (const definition of definitions) results[definition.id] = await scenario(root, definition.name, definition.options());
    const hashResults = Object.fromEntries(Object.entries(results).map(([name, result]) => [name, canonicalHash(result)]));
    const full = definitions.length === DOGFOOD_SCENARIOS.length;
    const core = full ? { schemaVersion: 1, kind: "DagDogfoodHashManifestV1", results: hashResults } : { schemaVersion: 1, kind: "DagDogfoodSelectionManifestV1", selectedScenarioIds: definitions.map(({ id }) => id), results: hashResults };
    return { ...core, manifestHash: canonicalHash(core) };
  } finally { await rm(root, { recursive: true, force: true }); }
}

export async function scenario(parent, name, options) {
  const repo = join(parent, name);
  if (options.crashAt?.includes("_validation_") || options.validationCrashPoints?.length || options.validationFailurePhase) options.validationCounterPath = join(parent, `${name}-validation-count.log`);
  const originalPath = process.env.PATH;
  const gitWrapperRoot = join(parent, `${name}-git-wrapper`); const gitNoopMarker = join(gitWrapperRoot, "noop-next-merge");
  if (options.provenAbsentLanding) {
    await mkdir(gitWrapperRoot, { recursive: true });
    await writeFile(join(gitWrapperRoot, "git"), `#!/bin/sh\nif [ -f ${JSON.stringify(gitNoopMarker)} ]; then for arg in "$@"; do if [ "$arg" = "merge" ]; then rm -f ${JSON.stringify(gitNoopMarker)}; exit 0; fi; done; fi\nexec ${JSON.stringify(realGitExecutable)} "$@"\n`);
    await chmod(join(gitWrapperRoot, "git"), 0o755);
  }
  await mkdir(repo, { recursive: true });
  await git(repo, ["init", "-b", "main"]);
  await git(repo, ["config", "user.name", "Scripted DAG Fixture"]);
  await git(repo, ["config", "user.email", "dag-fixture@example.invalid"]);
  await writeFile(join(repo, ".gitignore"), ".ai/\n");
  await writeFile(join(repo, "shared.txt"), `baseline${options.templateId ? `-${options.templateId}` : ""}\n`);
  await git(repo, ["add", ".gitignore", "shared.txt"]);
  await git(repo, ["commit", "-m", "chore: establish dogfood baseline"], commitEnvironment());
  const baseline = await gitTree(repo, "HEAD");
  const targetRoots = { "repo-main": repo, "repo-1": repo };
  if (options.multipleRepositories) for (let index = 1; index < options.items; index += 1) {
    const branch = `target-${index + 1}`;
    const workspace = join(parent, `${name}-${branch}`);
    await git(repo, ["branch", branch, baseline.commit]);
    await git(repo, ["worktree", "add", workspace, branch]);
    targetRoots[`repo-${index + 1}`] = workspace;
  }
  const plan = planFixture(baseline, options.items, options);
  const { genesis, context, seedFacts } = runFixture(plan, options.items, options);
  const artifacts = join(repo, ".ai", "dogfood-input");
  await mkdir(artifacts, { recursive: true });
  await writeFile(join(artifacts, "plan.json"), canonicalStringify(plan));
  await writeFile(join(artifacts, "genesis.json"), canonicalStringify(genesis));
  await writeFile(join(artifacts, "context.json"), canonicalStringify({ ...context, seedFacts }));

  const runStoreRoot = join(repo, ".ai", "dag-runs-v1");
  const runStatePath = join(runStoreRoot, genesis.runId, "run-state.json");
  const committedProjections = new Map();
  const recordCommitted = (committed) => { const projection = committedSnapshotProjection(committed); committedProjections.set(projection.revision, projection); };
  const store = new DagRunSnapshotStoreV1(runStoreRoot, genesis.runId, {
    failpoint: async (point) => {
      if (point !== "after_snapshot_rename" || !options.onCommittedSnapshot) return;
      recordCommitted(JSON.parse(await readFile(runStatePath, "utf8")));
    },
  });
  if (options.onCommittedSnapshot) recordCommitted(genesis);
  const scripted = scriptedLifecycle(repo, options);
  let fired = false;
  const firedPoints = new Set();
  let landingNoopArmed = false;
  let driverGeneration = 0;
  let integrationEnabled = false;
  const integration = {
    async reconcileExact(input) {
      if (!integrationEnabled) return;
      const driver = new DagReducerGitIntegrationDriverV1({
        store,
        context,
        lock: dagRunStoreLockIdentityFromOwner(input.state.owner),
        now: () => AT,
        failpoint: async (point) => {
          if (options.provenAbsentLanding && point === "before_landing" && !landingNoopArmed) {
            landingNoopArmed = true; await writeFile(gitNoopMarker, "one exact no-op landing acknowledgement\n"); process.env.PATH = `${gitWrapperRoot}:${originalPath}`;
          }
          if (options.thirdTargetDrift && point === "before_landing" && !fired) {
            fired = true;
            await writeFile(join(repo, "third-target.txt"), "scripted third-target drift\n");
            await git(repo, ["add", "third-target.txt"]);
            await git(repo, ["commit", "-m", "chore: scripted third-target drift"], commitEnvironment());
          }
          const crashRequested = options.crashAt === point || options.validationCrashPoints?.includes(point);
          if (crashRequested && !firedPoints.has(point) && (options.validationCrashPoints?.length || !fired)) {
            if (point.includes("_validation_")) {
              const phase = point.includes("prefix") ? "prefix" : "final";
              const lines = (await readFile(options.validationCounterPath, "utf8").catch((error) => error?.code === "ENOENT" ? "" : Promise.reject(error))).trim().split("\n").filter(Boolean);
              const phaseCount = lines.filter((line) => line === phase).length;
              if (point.endsWith("_intent") || point.endsWith("_dispatch")) assert.equal(phaseCount, 0, `${phase} external command cannot run before its durable dispatch boundary`);
              else assert.equal(phaseCount, 1, `${phase} durable result/reconciliation boundary follows exactly one external command`);
              if (options.validationObservationConflict && point === "after_prefix_validation_result") {
                const original = Object.values(context.facts).find((fact) => fact.kind === "verification" && fact.phase === "prefix");
                const conflictCore = { ...original, stderrHash: H("0") }; delete conflictCore.hash;
                const conflict = { ...conflictCore, hash: canonicalHash(conflictCore) };
                await store.putImmutableFact(conflict);
              }
            }
            firedPoints.add(point); if (!options.validationCrashPoints?.length) fired = true;
            const error = new Error(`scripted-restart:${point}`);
            error.code = "SCRIPTED_RESTART";
            throw error;
          }
        },
      });
      driverGeneration += 1;
      return driver.reconcileExact({ ...input, repositoryRoot: targetRoots[input.reservation.repositoryId] ?? input.repositoryRoot });
    },
  };
  const lifecycleFailpoint = async (point, detail) => {
    const requested = Array.isArray(options.lifecycleCrashAt) ? options.lifecycleCrashAt.includes(point) : options.lifecycleCrashAt === point;
    if (!requested || options.lifecycleCrashStage !== detail.stage || firedPoints.has(point)) return;
    const count = scripted.procedureInvocationCounts[detail.stage];
    if (["after_procedure_intent", "after_procedure_dispatch"].includes(point)) assert.equal(count, 0, `${detail.stage} external procedure cannot run before dispatch`);
    else assert.equal(count, 1, `${detail.stage} result/reconciliation follows exactly one external procedure invocation`);
    firedPoints.add(point); const error = new Error(`scripted-restart:${point}`); error.code = "SCRIPTED_RESTART"; throw error;
  };
  const lifecycleOptions = { ...scripted.adapters, integration, failpoint: lifecycleFailpoint };
  const conductor = new DagConductorServiceV1({ lifecycle: lifecycleOptions });
  const ctx = { cwd: repo, sessionManager: { getSessionId: () => `session-${name}`, getSessionFile: () => null, getHeader: () => ({ type: "session", id: `session-${name}`, cwd: repo }) } };
  let state;
  let restartObserved = false;
  let restartCount = 0;
  let validationConflictObserved = false;
  let validationFailureObserved = false;
  try {
    state = (await conductor.start(ctx, { runId: genesis.runId, runNonce: genesis.runNonce, planHash: plan.planHash, planPath: ".ai/dogfood-input/plan.json", genesisPath: ".ai/dogfood-input/genesis.json", contextPath: ".ai/dogfood-input/context.json", maxActiveNodes: options.maxActiveNodes ?? 1, occurredAt: AT })).state;
    if (options.onCommittedSnapshot) recordCommitted(state);
  } catch (error) {
    if (error?.code !== "SCRIPTED_RESTART") throw error;
    restartObserved = true; restartCount += 1;
    state = await rebindScenarioOwner(store, context, await store.read(context), `${name}-start-${restartCount}`);
  }

  scripted.enabled = true;
  integrationEnabled = true;
  state = await store.read(context);
  for (let step = 0; step < 1024; step += 1) {
    if (terminalForScenario(state, options)) break;
    const before = state;
    try {
      const lifecycle = new DagLifecycleRuntimeV1(store, plan, context, dagRunStoreLockIdentityFromOwner(state.owner), repo, lifecycleOptions);
      const reconciled = await lifecycle.reconcileOne(AT);
      state = await store.read(context);
      if (state.revision !== before.revision || state.snapshotHash !== before.snapshotHash || reconciled.progressed) continue;
      if (reconciled.waiting) throw new Error(`Dogfood lifecycle stalled: ${reconciled.reason}`);
      const decision = scheduleDagRunV1(plan, state);
      if (!decision.selected.length) throw new Error(`Dogfood scheduler stalled: ${decision.notice}`);
      const payload = { decisionHash: decision.decisionHash, decisionSequence: decision.decisionSequence, policyHash: decision.policyHash, normalizedIndexHash: decision.normalizedIndexHash, inputSnapshotHash: state.snapshotHash, reservations: decision.selected, bypassSlotIds: decision.bypassIncrements };
      const reduced = await store.mutate({ input: reducerInput(state, "reserve_scheduler_batch", "command", payload, `scheduler-${decision.decisionSequence}-${decision.decisionHash.slice(7, 19)}`), context, lock: dagRunStoreLockIdentityFromOwner(state.owner) });
      if (!reduced.accepted) throw new Error(`Dogfood scheduler reducer rejection: ${reduced.code}: ${reduced.message}`);
      state = reduced.state;
    } catch (error) {
      if (options.validationObservationConflict && error?.code === "VALIDATION_OBSERVATION_CONFLICT") { validationConflictObserved = true; state = await store.read(context); break; }
      if (options.validationFailurePhase && error?.code === "VALIDATION_FAILED") { validationFailureObserved = true; state = await store.read(context); break; }
      if (error?.code !== "SCRIPTED_RESTART") throw error;
      restartObserved = true; restartCount += 1;
      state = await rebindScenarioOwner(store, context, await store.read(context), `${name}-loop-${restartCount}`);
    }
  }

  if (options.onCommittedSnapshot) {
    const snapshotsDirectory = join(runStoreRoot, genesis.runId, "snapshots");
    for (const name of await readdir(snapshotsDirectory)) if (/^[0-9a-f]{64}\.json$/.test(name)) recordCommitted(JSON.parse(await readFile(join(snapshotsDirectory, name), "utf8")));
    recordCommitted(state);
    for (const projection of [...committedProjections.values()].sort((left, right) => left.revision - right.revision)) options.onCommittedSnapshot(projection);
  }
  const target = await gitTree(repo, "HEAD");
  const itemStates = Object.values(state.workItems).map(({ workItemId, current, candidateGeneration, integrationReceipt, blockerIds }) => ({ workItemId, current, candidateGeneration, integrationReceipt, blockerIds })).sort((a, b) => a.workItemId.localeCompare(b.workItemId));
  if (options.validationFailurePhase) {
    assert.equal(validationFailureObserved, true, `${options.validationFailurePhase} non-PASS validation fails closed only after durable effect reconciliation`);
    assert.equal(itemStates[0].current, "integrating");
    const effect = Object.values(state.effects).find((candidate) => candidate.kind === "verify_prefix" && candidate.executionRequest?.phase === options.validationFailurePhase);
    assert.equal(effect.state, "reconciled"); assert.equal(effect.reconciliation, "applied_exact"); assert.equal(context.facts[effect.executionObservationHash].disposition, "FAIL");
  } else if (options.validationObservationConflict) {
    assert.equal(validationConflictObserved, true, "conflicting durable validation observations fail closed before proposal verification or landing");
    assert.equal(itemStates[0].current, "integrating");
  } else if (options.procedureFailureStage) {
    assert(itemStates.every(({ current }) => current === "blocked"), `${options.procedureFailureStage} non-PASS closes the procedure effect and blocks the item`);
    assert(Object.values(state.workItems).every((item) => item.stages[options.procedureFailureStage].state === "failed"), `${options.procedureFailureStage} exact FAIL disposition is sealed without synthesized PASS`);
  } else if (options.conflictingCandidates) {
    assert.equal(itemStates[0].current, "complete", "first train member lands through real Git");
    assert.equal(itemStates[1].current, "active", "real merge-tree conflict returns the second member to F1");
    assert.equal(itemStates[1].candidateGeneration, 2, "composition conflict fences the conflicting candidate generation");
    assert(Object.values(state.integrationAttempts).some(({ conflictClass }) => conflictClass === "mechanical"), "real conflict is reducer-recorded");
  } else if (options.thirdTargetDrift) {
    assert.equal(itemStates[0].current, "integration_ready", "third-target drift blocks without overwrite");
    assert(itemStates[0].blockerIds.some((id) => id.startsWith("integration-target-third-")), "third target creates an exact successor-plan blocker");
    assert.equal(await readFile(join(repo, "third-target.txt"), "utf8"), "scripted third-target drift\n");
  } else {
    assert.equal(state.completion.state, "plan_complete", "real F0-F8 and Git landing complete the plan");
    assert(itemStates.every(({ current }) => current === "complete"), "every authorized work item completes");
    for (const train of Object.values(state.integrationTrains)) {
      const landed = await gitTree(targetRoots[train.repositoryId] ?? repo, "HEAD");
      assert.equal(landed.commit, train.acceptedPrefix.commit, `exact ${train.repositoryId} integration target lands`);
      assert.equal(landed.tree, train.acceptedPrefix.tree, `exact ${train.repositoryId} integration tree lands`);
    }
    if (options.crashAt || options.crashCleanup) { assert.equal(restartObserved, true, "post-side-effect restart is observed and replayed"); assert(state.owner.ownerEpoch > 1, "restart changes owner epoch before exact effect reconciliation"); }
  }
  for (const targetRoot of new Set(Object.values(targetRoots))) assert.equal(await git(targetRoot, ["status", "--porcelain=v2", "--untracked-files=all"]), "", "bound target worktree remains exact and clean");
  const onlyAttempt = Object.values(state.integrationAttempts)[0];
  if (options.lifecycleCrashAt) {
    const stage = options.lifecycleCrashStage;
    for (const point of Array.isArray(options.lifecycleCrashAt) ? options.lifecycleCrashAt : [options.lifecycleCrashAt]) assert(firedPoints.has(point), `${stage} crosses ${point}`);
    assert.equal(scripted.procedureInvocationCounts[stage], 1, `${stage} procedure executes exactly once across ${options.lifecycleCrashAt}`);
    const attempt = Object.values(state.stageAttempts).find((candidate) => candidate.stage === stage);
    const effect = Object.values(state.effects).find((candidate) => candidate.kind === "run_procedure" && candidate.boundStageAttemptId === attempt.stageAttemptId);
    assert(effect); assert.equal(effect.dispatchCount, 1); assert.equal(effect.requestHash, canonicalHash(effect.executionRequest)); assert.equal(effect.state, "reconciled");
    const execution = await store.readImmutableFact(effect.executionObservationHash); const reconciliation = await store.readImmutableFact(effect.observationHash);
    assert.equal(execution.effectId, effect.effectId); assert.equal(reconciliation.executionObservationHash, execution.hash); assert.equal(reconciliation.resultIdentityHash, execution.resultIdentityHash);
  }
  if (options.crashAt === "after_composition_commit") {
    const compositionFacts = Object.values(context.facts).filter((fact) => fact.kind === "git_transaction" && fact.factType === "composition" && fact.integrationAttemptId === onlyAttempt.integrationAttemptId && fact.reconciliation === "applied_exact");
    const verificationFact = context.facts[onlyAttempt.proposalVerificationFactHash];
    assert.equal(compositionFacts.length, 1, "post-composition restart reuses one immutable composition fact without recomposition");
    assert.equal(state.effects[onlyAttempt.compositionEffectId].dispatchCount, 1, "post-composition verification retains the historical one-dispatch composition owner");
    assert(compositionFacts[0].ownerEpoch < verificationFact.ownerEpoch && verificationFact.ownerEpoch === state.owner.ownerEpoch, "post-composition verification is authorized by the exact chained successor owner rather than rewriting historical composition authority");
  }
  if ((options.crashAt?.includes("_validation_") || options.validationCrashPoints?.length) && !options.validationObservationConflict) {
    for (const point of options.validationCrashPoints ?? [options.crashAt]) assert(firedPoints.has(point), `validation crosses ${point}`);
    const lines = (await readFile(options.validationCounterPath, "utf8")).trim().split("\n").filter(Boolean);
    assert.equal(lines.filter((line) => line === "prefix").length, 1, "prefix validation executes exactly once across crash recovery");
    assert.equal(lines.filter((line) => line === "final").length, 1, "final validation executes exactly once across crash recovery");
    for (const phase of ["prefix", "final"]) {
      const effect = Object.values(state.effects).find((candidate) => candidate.kind === "verify_prefix" && candidate.boundIntegrationAttemptId === onlyAttempt.integrationAttemptId && candidate.executionRequest?.phase === phase);
      assert(effect, `${phase} validation retains one exact effect`); assert.equal(effect.dispatchCount, 1); assert.equal(effect.requestHash, canonicalHash(effect.executionRequest), `${phase} request identity remains unchanged`);
      const execution = context.facts[effect.executionObservationHash]; const reconciliation = context.facts[effect.observationHash];
      assert.equal(execution.effectId, effect.effectId); assert.equal(execution.requestHash, effect.requestHash); assert.equal(reconciliation.executionObservationHash, execution.hash); assert.equal(reconciliation.resultIdentityHash, execution.hash);
    }
  }
  if (options.provenAbsentLanding) {
    const landingFacts = Object.values(context.facts).filter((fact) => fact.kind === "git_transaction" && fact.factType === "landing" && fact.integrationAttemptId === onlyAttempt.integrationAttemptId);
    const absentFacts = landingFacts.filter(({ reconciliation }) => reconciliation === "proven_absent"); const appliedFacts = landingFacts.filter(({ reconciliation }) => reconciliation === "applied_exact");
    const landingEffect = state.effects[onlyAttempt.landingEffectId];
    assert.equal(absentFacts.length, 1, "restart retains one exact proven-absent landing observation"); assert.equal(appliedFacts.length, 1, "successor redispatch lands once without duplicate landing observations");
    assert.equal(landingEffect.dispatchCount, 2, "proven-absent landing receives one current-owner redispatch while preserving immutable dispatch count");
    assert(absentFacts[0].ownerEpoch < appliedFacts[0].ownerEpoch && appliedFacts[0].ownerEpoch === state.owner.ownerEpoch, "landing redispatch fact binds the exact chained successor owner");
    const absentSlotId = canonicalHash({ type: "record_git_landing_reconciliation", naturalIdentity: `${onlyAttempt.integrationAttemptId}/${absentFacts[0].hash}` });
    const appliedSlotId = canonicalHash({ type: "record_git_landing_reconciliation", naturalIdentity: `${onlyAttempt.integrationAttemptId}/${appliedFacts[0].hash}` });
    assert.deepEqual({ ownerEpoch: state.idempotencySlots[absentSlotId].landingObservationBinding.ownerEpoch, dispatchCount: state.idempotencySlots[absentSlotId].landingObservationBinding.dispatchCount }, { ownerEpoch: absentFacts[0].ownerEpoch, dispatchCount: 1 }, "old landing fact retains exact first-dispatch owner history after effect rebinding");
    assert.deepEqual({ ownerEpoch: state.idempotencySlots[appliedSlotId].landingObservationBinding.ownerEpoch, dispatchCount: state.idempotencySlots[appliedSlotId].landingObservationBinding.dispatchCount }, { ownerEpoch: appliedFacts[0].ownerEpoch, dispatchCount: 2 }, "successor landing fact binds only the explicit redispatch authority");
    const targetOccurrences = (await git(repo, ["reflog", "show", "--format=%H", "refs/heads/main"])).split("\n").filter((oid) => oid === target.commit).length;
    assert.equal(targetOccurrences, 1, "real Git target reaches the intended commit exactly once without overwrite");
  }
  assert(scripted.commandCount > 0, "scripted fixture procedures execute real deterministic commands");
  if (options.provenAbsentLanding) process.env.PATH = originalPath;
  return {
    stateHash: state.snapshotHash,
    planHash: plan.planHash,
    targetHash: canonicalHash(target),
    itemProjectionHash: canonicalHash(itemStates),
    scriptedEvidenceHash: canonicalHash({ identity: "scripted-fixture-evidence-only", productionGenerality: false, commandCount: scripted.commandCount }),
    restartObservedHash: canonicalHash({ restartObserved }),
    driverGenerationHash: canonicalHash({ driverGeneration }),
    ...(options.returnArtifacts ? { repo, baseline, plan, genesis, context, state, target, commandCount: scripted.commandCount } : {}),
  };
}

async function rebindScenarioOwner(store, context, state, label) {
  const prior = state.owner.ownershipReceipt ? await store.readImmutableFact(state.owner.ownershipReceipt) : null;
  const successor = { ownerTokenHash: canonicalHash({ label, kind: "dogfood-successor-token" }), sessionId: `session-${label}-successor`, pid: process.pid, processStartIdentity, lockIdentity: canonicalHash({ label, kind: "dogfood-successor-lock" }) };
  const lineageHash = canonicalHash({ kind: "direct_owner_transfer", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity });
  const core = { kind: "ownership", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, priorAttachedAt: state.owner.attachedAt, disposition: "same_manager", priorObservationHash: null, priorOwnershipReceiptHash: state.owner.ownershipReceipt, ownerEpoch: state.owner.ownerEpoch + 1, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity, lineageHash };
  const withChain = { ...core, chainHash: ownershipChainHashV1(core, prior?.kind === "ownership" ? prior.chainHash : null) };
  const ownership = withHash(withChain); await store.putImmutableFact(ownership); context.facts[ownership.hash] = ownership;
  const payload = { ...successor, ownershipReceipt: ownership.hash, priorOwnerDisposition: "same_manager" };
  const result = await store.mutate({ input: reducerInput(state, "transfer_owner", "command", payload, `owner-rebind-${label}`), context, lock: dagRunStoreLockIdentityFromOwner(state.owner) });
  if (!result.accepted) throw new Error(`Dogfood owner rebind rejected: ${result.code}: ${result.message}`);
  return result.state;
}

function committedSnapshotProjection(state) {
  const reservations = Object.values(state.scheduler.reservations);
  const operationHash = ({ reservationId }) => canonicalHash({ runId: state.runId, reservationId });
  return {
    runIdentityHash: canonicalHash({ runId: state.runId }),
    revision: state.revision,
    snapshotHash: state.snapshotHash,
    readyOperationHashes: [],
    reservedOperationHashes: reservations.filter(({ state: reservationState }) => ["reserved", "dispatch_intent"].includes(reservationState)).map(operationHash).sort(),
    activeOperationHashes: reservations.filter(({ state: reservationState }) => reservationState === "active").map(operationHash).sort(),
  };
}

function terminalForScenario(state, options) {
  if (options.procedureFailureStage) return Object.values(state.workItems).every((item) => item.stages[options.procedureFailureStage].state === "failed" && item.current === "blocked");
  if (options.conflictingCandidates) return Object.values(state.integrationAttempts).some(({ conflictClass }) => conflictClass !== "none") && state.workItems["item-2"]?.current === "active";
  if (options.thirdTargetDrift) return state.workItems["item-1"].current === "integration_ready" && state.workItems["item-1"].blockerIds.some((id) => id.startsWith("integration-target-third-"));
  return state.completion.state === "plan_complete";
}

function scriptedLifecycle(repo, options) {
  const launches = new Map();
  let cleanupCrashFired = false;
  const fixtureIdentity = { kind: "scripted_fixture_evidence", productionGenerality: false, contract: "actual deterministic Git commands only" };
  const result = {
    enabled: false,
    commandCount: 0,
    procedureInvocationCounts: Object.fromEntries(PLAN_STAGE_IDS.map((stage) => [stage, 0])),
    adapters: {
      worker: {
        async launchExact(request, state) {
          if (launches.has(request.launchKey)) return launches.get(request.launchKey).observation;
          const workspace = join(repo, ".ai", "scripted-workers", request.worktreeKey);
          await mkdir(join(repo, ".ai", "scripted-workers"), { recursive: true });
          await git(repo, ["worktree", "add", "--detach", workspace, request.baseCommit]);
          result.commandCount += 1;
          let candidate = null;
          if (request.label.endsWith("/F1")) {
            const itemId = request.label.split("/")[0];
            const body = options.conflictingCandidates ? `candidate-${itemId}\n` : `dogfood-${options.templateId ? `${options.templateId}-` : ""}${itemId}\n`;
            const candidatePath = options.independentCandidatePaths ? `candidate-${itemId}.txt` : "shared.txt";
            await writeFile(join(workspace, candidatePath), body);
            await git(workspace, ["add", candidatePath]);
            await git(workspace, ["commit", "-m", `feat: scripted candidate ${itemId}`], commitEnvironment());
            candidate = await gitTree(workspace, "HEAD");
            result.commandCount += 3;
          } else {
            await git(workspace, ["status", "--porcelain=v2", "--untracked-files=all"]);
            result.commandCount += 1;
          }
          if (request.label.endsWith("/F3")) candidate = await gitTree(workspace, "HEAD");
          const attemptNonce = `nonce-${request.workerId}-0123456789`;
          const config = { storageId: `scripted-storage-${state.runId}`, ownerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, launchKey: request.launchKey, requestHash: request.configRequestHash, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity }, fixtureIdentity };
          const configHash = canonicalHash(config);
          const configCore = { kind: "worker_config", configHash, config };
          const observation = { workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash, configFact: withHash(configCore), supervisorPid: process.pid, supervisorStartIdentity: processStartIdentity, childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: AT };
          launches.set(request.launchKey, { observation, candidate, workspace });
          return observation;
        },
        async readTerminalExact(binding, state) {
          const launch = [...launches.values()].find(({ observation }) => observation.workerId === binding.workerId);
          const attempt = state.stageAttempts[binding.stageAttemptId]; if (!launch || !attempt) return null;
          const output = launch.candidate ?? await gitTree(launch.workspace, "HEAD"); const item = state.workItems[attempt.workItemId]; const sourceBase = attempt.stage === "F1" ? state.repositories[item.writeRepositoryId].baseline : item.candidate?.git ?? state.repositories[item.writeRepositoryId].baseline;
          return { completionId: `completion-${binding.workerId}`, terminalStatus: "succeeded", workerOutput: { outputRepositoryId: item.writeRepositoryId, outputCommonDirIdentityHash: canonicalHash({ repo, common: await git(repo, ["rev-parse", "--git-common-dir"]) }), outputWorktreeIdentityHash: canonicalHash({ workspace: launch.workspace }), outputSourceBase: sourceBase, outputCommit: output.commit, outputTree: output.tree, outputObjectFormat: output.commit.length === 40 ? "sha1" : "sha256", candidateObservedAt: AT } };
        },
        async cleanupExact(binding) {
          const launch = [...launches.values()].find(({ observation }) => observation.workerId === binding.workerId);
          if (!launch) return "proven_absent";
          if (!launch.cleaned) {
            await git(repo, ["worktree", "remove", "--force", launch.workspace]); launch.cleaned = true; result.commandCount += 1;
            if (options.crashCleanup && !cleanupCrashFired) { cleanupCrashFired = true; const error = new Error("scripted-restart:cleanup"); error.code = "SCRIPTED_RESTART"; throw error; }
            return "applied_exact";
          }
          return "proven_absent";
        },
      },
      candidate: {
        async inspectAndSealCandidate({ state, attempt, binding, repositoryId }) {
          const launch = [...launches.values()].find(({ observation }) => observation.workerId === binding.workerId);
          if (!launch?.candidate) return null;
          await git(launch.workspace, ["diff-tree", "--check", "HEAD^!"]);
          result.commandCount += 1;
          const item = state.workItems[attempt.workItemId];
          const base = attempt.stage === "F1" ? state.repositories[repositoryId].baseline : item.candidate.git;
          const gitIdentity = { repositoryId, ...launch.candidate };
          const candidate = withHash({ kind: "candidate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: item.workItemId, generation: item.candidateGeneration + 1, candidateId: `candidate-${attempt.stageAttemptId}`, base, git: gitIdentity, patchIdentityHash: canonicalHash({ base, git: gitIdentity }), producedByStageAttemptId: attempt.stageAttemptId, lineageHash: item.implementationLineageHash });
          return { candidate, workerOutput: { outputRepositoryId: repositoryId, outputCommonDirIdentityHash: canonicalHash({ repo, common: await git(repo, ["rev-parse", "--git-common-dir"]) }), outputWorktreeIdentityHash: canonicalHash({ workspace: launch.workspace }), outputSourceBase: base, outputCommit: gitIdentity.commit, outputTree: gitIdentity.tree, outputObjectFormat: gitIdentity.commit.length === 40 ? "sha1" : "sha256", candidateObservedAt: AT } };
        },
      },
      procedure: {
        adapterKind: "immutable-catalog-command-v1",
        allowlistedProcedureHashes: Object.keys(procedureCatalogFixture()).sort(),
        allowlistHash: canonicalHash(Object.keys(procedureCatalogFixture()).sort()),
        async executeExact({ plan, state, attempt, procedure }) {
          if (!result.enabled) return null;
          result.procedureInvocationCounts[attempt.stage] += 1;
          const item = state.workItems[attempt.workItemId];
          const commit = item.candidate?.git.commit ?? state.repositories[item.writeRepositoryId].baseline.commit;
          await git(repo, ["cat-file", "-e", `${commit}^{commit}`]);
          await git(repo, ["diff-tree", "--check", `${commit}^!`]);
          result.commandCount += 2;
          const oracleAssertions = [];
          if (attempt.stage === "F2") {
            const oracle = plan.acceptanceOracles[0]; const expected = oracle.assertions[0];
            oracleAssertions.push(withHash({ kind: "oracle_assertion", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: "F2", stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, oracleId: oracle.oracleId, assertionId: expected.assertionId, procedureId: expected.procedureId, environmentProfileId: expected.environmentProfileId, observationMethod: expected.observationMethod, requiredEvidenceClass: expected.requiredEvidenceClass, disposition: "PASS", observationHash: attempt.workerResult.hash }));
          }
          const candidateGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
          let workspaceMaterialization; let environmentObservation;
          if (["F2", "F5", "F7"].includes(attempt.stage)) {
            const binding = state.workerBindings[attempt.stageAttemptId];
            const launch = binding ? [...launches.values()].find(({ observation }) => observation.workerId === binding.workerId) : null;
            const repositoryBinding = launch ? null : await readRepositoryBindingIdentityV1(repo);
            const commonDirIdentityHash = launch
              ? canonicalHash({ repo, common: await git(repo, ["rev-parse", "--git-common-dir"]) })
              : repositoryBinding.commonDirIdentityHash;
            const worktreeIdentityHash = launch
              ? canonicalHash({ workspace: launch.workspace })
              : repositoryBinding.worktreeIdentityHash;
            workspaceMaterialization = withHash({ kind: "workspace_materialization", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, commonDirIdentityHash, worktreeIdentityHash, materializedAt: AT });
            environmentObservation = withHash({ kind: "environment_observation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: workspaceMaterialization.hash, commonDirIdentityHash, worktreeIdentityHash, cleanliness: "clean", observedAt: AT });
          }
          const procedureDisposition = options.procedureFailureStage === attempt.stage ? "FAIL" : "PASS";
          const applicableChecks = plan.workItems.find(({ workItemId }) => workItemId === item.workItemId).checks.filter(({ phases }) => phases.includes(attempt.stage));
          const checkExecutions = applicableChecks.map((check) => withHash({ kind: "check_execution", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, candidateGeneration, candidateHash: attempt.stage === "F0" ? null : item.candidate.candidateHash, checkId: check.checkId, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation?.hash ?? null, executionId: `execution-${attempt.stageAttemptId}-${check.checkId}`, disposition: procedureDisposition, startedAt: AT, completedAt: AT }));
          const aggregateCore = { kind: "check_aggregate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: procedureDisposition, oracleIds: plan.workItems.find(({ workItemId }) => workItemId === item.workItemId).oracleIds, assertions: oracleAssertions.map((fact) => ({ oracleId: fact.oracleId, assertionId: fact.assertionId, evidenceHash: fact.hash })), checks: applicableChecks.map((check, index) => ({ checkId: check.checkId, disposition: procedureDisposition, executionEvidenceHash: checkExecutions[index].hash, applicabilityEvidenceHashes: [] })) };
          const checkAggregate = withHash(aggregateCore);
          const evidence = withHash({ kind: "stage_evidence", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: checkAggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration, candidateHash: attempt.stage === "F0" ? null : item.candidate.candidateHash, producerKind: attempt.producerKind, producerResultHash: attempt.workerResult?.hash ?? null, disposition: procedureDisposition, environmentObservationHash: environmentObservation?.hash ?? null, producedAt: state.updatedAt, readOnly: procedure.readOnly });
          const output = { checkAggregate, evidence, oracleAssertions, checkDispositions: [], checkExecutions, checkAuthorities: [], ...(workspaceMaterialization ? { workspaceMaterialization, environmentObservation } : {}) };
          if (attempt.stage === "F8") output.integrationReady = withHash({ kind: "integration_ready", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, candidateGeneration: item.candidateGeneration, candidateHash: item.candidate.candidateHash, f8EvidenceHash: evidence.hash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true });
          return output;
        },
      },
    },
  };
  return result;
}

export function planFixture(baseline, itemCount, options = {}) {
  const decisionRef = { collection: "decisions", id: "DEC-dogfood", semanticHash: H("2") };
  const subjects = options.independentSubjects
    ? Array.from({ length: itemCount }, (_, index) => content({ subjectId: `subject-api-${index + 1}`, kind: "contract", title: `Dogfood contract ${index + 1}`, description: "Dogfood behavior is exact." }))
    : [content({ subjectId: "subject-api", kind: "contract", title: "Dogfood contract", description: "Dogfood behavior is exact." })];
  const subject = subjects[0];
  const assertion = content({ assertionId: "assert-api", subjectId: subject.subjectId, observationMethod: "automated_check", procedureId: "check-api", passCondition: "Deterministic Git checks pass.", failureSignals: ["Command fails."], tolerance: "Exact.", environmentProfileId: "env-scripted-fixture", requiredEvidenceClass: "independent" });
  const oracle = content({ oracleId: "oracle-api", title: "Dogfood acceptance", sourceRefs: [decisionRef], assertions: [assertion] });
  const outcome = content({ outcomeId: "outcome-api", title: "Dogfood works", description: "The exact candidate lands.", oracleIds: [oracle.oracleId] });
  const contract = content({ contractId: "contract-api", title: "Dogfood contract", description: "The exact test contract.", subjectIds: subjects.map(({ subjectId }) => subjectId), compatibility: "compatible" });
  const component = content({ componentId: "component-api", title: "Dogfood", responsibilities: ["Exercise the runtime."], subjectIds: subjects.map(({ subjectId }) => subjectId), contractIds: [contract.contractId] });
  const workItems = Array.from({ length: itemCount }, (_, index) => content({ workItemId: `item-${index + 1}`, kind: "change", title: `Dogfood item ${index + 1}`, objective: "Execute the scripted fixture's actual deterministic Git workflow.", writeRepositoryId: options.multipleRepositories ? `repo-${index + 1}` : "repo-main", outcomeIds: [outcome.outcomeId], nonGoalIds: [], modelRefs: [decisionRef], contractIds: [contract.contractId], oracleIds: [oracle.oracleId], extraContext: "This is scripted fixture evidence and does not claim production generality.", contextRefs: [], semanticReads: [], semanticWrites: [{ subjectId: subjects[index]?.subjectId ?? subject.subjectId, mode: "extend", compatibility: "compatible", migrationProtocolId: null }], risk: { tier: options.riskTier ?? "medium", reasons: ["Exercises real Git landing."], hardeningProfileIds: ["hardening-default"] }, capabilities: [{ kind: "capability", capabilityId: "node", purpose: "Run deterministic fixture commands.", phases: ["F1"], environmentProfileId: "env-scripted-fixture" }], checks: [{ checkId: "check-api", phases: ["F2"], applicability: "required", reason: "Independent deterministic command evidence is required.", condition: null }, ...(options.procedureFailureStage ? [{ checkId: `check-${options.procedureFailureStage.toLowerCase()}`, phases: [options.procedureFailureStage], applicability: "required", reason: "Scripted exact non-PASS procedure coverage.", condition: null }] : [])], pathEvidence: [{ path: options.independentCandidatePaths ? `candidate-item-${index + 1}.txt` : "shared.txt", symbol: null, basis: "Committed fixture baseline.", confidence: "high" }], resourceDemands: [], integration: { trainIds: [options.multipleRepositories ? `train-${index + 1}` : "train-main"], effectScopeIds: [], migrationProtocolIds: [] } }));
  const repositories = options.multipleRepositories
    ? workItems.map((_, index) => content({ repositoryId: `repo-${index + 1}`, role: "write", locator: null, baseline: { repositoryId: `repo-${index + 1}`, ...baseline }, targetRef: index === 0 ? "refs/heads/main" : `refs/heads/target-${index + 1}` }))
    : [content({ repositoryId: "repo-main", role: "write", locator: null, baseline: { repositoryId: "repo-main", ...baseline }, targetRef: "refs/heads/main" })];
  const integrationProfiles = integrationProfilesFixture(options);
  const profileHashById = Object.fromEntries(Object.entries(integrationProfiles).map(([hash, profile]) => [profile.profileId, hash]));
  const trains = options.multipleRepositories
    ? workItems.map(({ workItemId }, index) => content({ trainId: `train-${index + 1}`, repositoryId: `repo-${index + 1}`, strategy: "merge_tree_one_parent", members: [{ workItemId, ordinal: 0 }], partialIntegrationPrecedenceIds: [], compositionProfileHash: H("d"), prefixValidationProfileId: "checks-prefix", prefixValidationProfileHash: profileHashById["checks-prefix"], finalValidationProfileId: "checks-final", finalValidationProfileHash: profileHashById["checks-final"] }))
    : [content({ trainId: "train-main", repositoryId: "repo-main", strategy: "merge_tree_one_parent", members: workItems.map(({ workItemId }, ordinal) => ({ workItemId, ordinal })), partialIntegrationPrecedenceIds: [], compositionProfileHash: H("d"), prefixValidationProfileId: "checks-prefix", prefixValidationProfileHash: profileHashById["checks-prefix"], finalValidationProfileId: "checks-final", finalValidationProfileHash: profileHashById["checks-final"] })];
  const procedureEntries = procedureCatalogFixture();
  const lifecycleProfileHash = canonicalHash(Object.values(procedureEntries).sort((a, b) => a.procedureId.localeCompare(b.procedureId)));
  const checkCatalogHash = canonicalHash(workItems.map(({ workItemId, checks }) => ({ workItemId, checks })));
  const selectorInput = { version: "governing-v1", selectedWorkstreamIds: ["WS-dogfood"], explicitSeedIds: [] };
  const closureEntries = [{ collection: "decisions", id: "DEC-dogfood", effectiveState: "accepted", semanticHash: H("2"), acceptanceContentHash: H("3") }];
  const modelInput = { projectId: "project-dogfood", schemaVersion: 1, revision: 1, modelHash: H("5"), selector: { ...selectorInput, selectorHash: canonicalHash(selectorInput) }, closure: { entries: closureEntries, closureHash: canonicalHash(closureEntries) }, contextRefs: [], specs: [{ projectionId: "SPEC-dogfood", projectionContract: "1", modelInputHash: H("6"), contentHash: H("7") }] };
  return sealCanonicalDagPlanV1({ schemaVersion: 1, kind: "CanonicalDagPlanV1", canonicalization: "jcs-v1", planId: `plan-dogfood-${options.templateId ?? itemCount}`, revision: 1, createdAt: AT, generator: { name: "scripted-dogfood-fixture", version: "1", profileHash: canonicalHash({ fixtureIdentity: "scripted_fixture_evidence", productionGenerality: false, templateId: options.templateId ?? null }) }, modelBinding: { ...modelInput, bindingHash: canonicalHash(modelInput) }, repositories, architecture: content({ outcomes: [outcome], nonGoals: [], components: [component], contracts: [contract], risks: [], assumptions: [], effectScopes: [] }), semanticSubjects: subjects, acceptanceOracles: [oracle], workItems, gates: [], constraints: { precedence: options.precedence ?? [], semanticMutexes: (options.semanticMutex ?? itemCount > 1) ? [content({ mutexGroupId: "mutex-shared", subjectIds: [subject.subjectId], members: workItems.map(({ workItemId }) => ({ workItemId, phases: ["F1"] })), reason: "Scripted candidates share a semantic contract.", confidence: "high", evidenceRefs: [] })] : [], resourceClasses: [], integrationTrains: trains, migrationProtocols: [] }, lifecycleBinding: { profileId: "scripted-fixture-lifecycle-v1", profileHash: lifecycleProfileHash, checkCatalogHash, retryPolicyHash: H("b"), schedulerPolicyVersion: "sticky-lanes-v1", schedulerPolicyHash: DAG_SCHEDULER_POLICY_HASH_V1, stages: [...PLAN_STAGE_IDS] }, artifactPolicy: { profileId: "artifact-v1", profileHash: H("c"), maxInlineBytes: 4096, maxArtifactBytes: 65536, maxArtifactsPerWorkItem: 32, allowedRoots: [".ai/dag-artifacts"], allowedMediaTypes: ["application/json"], defaultRetention: "run", redactRestrictedLocators: true }, projectionContract: { version: "1", projections: [content({ kind: "dag_execution", version: "1", executable: false })] } });
}

export function runFixture(plan, itemCount, options = {}) {
  const reviewFact = simpleFact("plan_review", "review-dogfood");
  const authorizationFact = simpleFact("plan_authorization", "authorization-dogfood");
  const freshnessFact = simpleFact("staleness", "freshness-dogfood");
  const repositoryObservation = simpleFact("repository_observation", "repo-main-observation");
  const authorization = authorizationBinding(plan, reviewFact.hash, [authorizationFact.hash], options.maxActiveNodes ?? 1);
  const authSet = ref("authorization_set", "authorization-set", authorization.hash);
  const repositories = Object.fromEntries(plan.repositories.map((planned) => [planned.repositoryId, { repositoryId: planned.repositoryId, planEntityHash: planned.contentHash, role: "write", baseline: planned.baseline, targetRef: planned.targetRef, observedTarget: planned.baseline, observedTargetAt: AT, observationReceipt: repositoryObservation.hash, workspace: { state: "unmaterialized", locator: null, gitCommonDirIdentityHash: null, gitWorktreeIdentityHash: null, branchRef: null, base: null, expectedHead: null, ownerLeaseId: null, processDisposition: "not_applicable", observationReceipt: null }, integrationLockLeaseId: null, blockerIds: [] }]));
  const workItems = Object.fromEntries(plan.workItems.map((item) => [item.workItemId, { workItemId: item.workItemId, planEntityHash: item.contentHash, writeRepositoryId: item.writeRepositoryId, desired: "run", current: "ready", authorizedStages: [...PLAN_STAGE_IDS], currentStage: null, implementationLineageHash: canonicalHash({ item: item.workItemId, fixture: true }), candidateGeneration: 0, candidate: null, stages: Object.fromEntries(PLAN_STAGE_IDS.map((stage) => [stage, { stage, state: "pending", attemptIds: [], currentAttemptId: null, currentEvidence: null, adoptionReceipt: null, invalidationIds: [], lastDisposition: null, blockerIds: [] }])), precedenceIds: [], gateIds: [], laneAdmissionSequence: null, admittedAt: null, activeLeaseIds: [], blockerIds: [], openFindingIds: [], integrationReadyReceipt: null, integrationEntryId: null, integrationReceipt: null, completedAt: null }]));
  const index = buildSchedulerPlanIndexV1(plan);
  const catalog = { lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, procedures: procedureCatalogFixture(), checkAggregates: {} };
  const context = { plan, authorization, historicalAuthorizations: {}, catalog, normalizedSchedulerIndexHash: index.indexHash, facts: {}, integrationValidationProfiles: integrationProfilesFixture(options) };
  const runLabel = options.runLabel ?? String(itemCount);
  const genesis = sealDagRunStateV1({ schemaVersion: 1, kind: "DagRunStateV1", canonicalization: "jcs-v1", runId: `run-dogfood-${runLabel}`, runNonce: `dogfood-nonce-${runLabel}-0123456789`, revision: 0, previousSnapshotHash: null, createdAt: AT, updatedAt: AT, identity: { projectId: "project-dogfood", planId: plan.planId, planRevision: plan.revision, planHash: plan.planHash, planSchemaHash: CANONICAL_DAG_PLAN_SCHEMA_HASH, lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, artifactPolicyHash: plan.artifactPolicy.profileHash, reviewReceipt: ref("plan_review", "review-dogfood", reviewFact.hash), authorizationReceipts: [ref("plan_authorization", "authorization-dogfood", authorizationFact.hash)], authorizationSet: authSet, previousRunId: null, supersededByRunId: null }, owner: { ownerEpoch: 0, ownerTokenHash: null, sessionId: null, pid: 0, processStartIdentity: null, lockIdentity: null, attachedAt: null, lastHeartbeatAt: null, ownershipReceipt: null, lastReleaseCommandId: null, lastReleasePayloadHash: null }, desired: { run: "running", reason: null, requestedAt: AT, requestedBy: "user" }, current: { run: "active", readyWorkItemIds: Object.keys(workItems).sort(), activeWorkItemIds: [], blockedWorkItemIds: [], integrationReadyWorkItemIds: [], updatedByCommandId: "create-run" }, repositories, workItems, gates: {}, precedence: {}, resourcePools: {}, mutexes: Object.fromEntries(plan.constraints.semanticMutexes.map((mutex) => [mutex.mutexGroupId, { mutexGroupId: mutex.mutexGroupId, planEntityHash: mutex.contentHash, activeLeaseId: null, waitingStageAttemptIds: [] }])), leases: {}, stageAttempts: {}, launchIntents: {}, workerBindings: {}, evidenceIndex: { stageAttemptInputs: {}, workerResults: {}, candidates: {}, stageEvidence: {}, checkAggregates: {}, checkDispositions: {}, verifications: {}, oracleAssertions: {}, findings: {}, findingResolutions: {}, waivers: {}, invalidations: {}, adoptions: {}, effectReconciliations: {}, integrationReady: {}, integrationReceipts: {}, stalenessReceipts: { [freshnessFact.hash]: ref("staleness", "freshness-dogfood", freshnessFact.hash) }, gateReceipts: {} }, findingClosures: {}, retryLedger: {}, blockers: {}, effects: {}, cancellations: {}, quarantine: {}, idempotencySlots: {}, integrationTrains: Object.fromEntries(plan.constraints.integrationTrains.map((train) => { const planned = plan.repositories.find(({ repositoryId }) => repositoryId === train.repositoryId); return [train.repositoryId, { repositoryId: train.repositoryId, planTrainHash: train.contentHash, strategy: "merge_tree_one_parent", targetRef: planned.targetRef, expectedTarget: planned.baseline, acceptedPrefix: planned.baseline, acceptedPrefixOrdinal: 0, acceptedPrefixReceipt: null, entryOrder: [], entries: {}, activeIntegrationAttemptId: null, lockLeaseId: null, blockerIds: [] }]; })),
   integrationAttempts: {}, scheduler: { policyVersion: "sticky-lanes-v1", policyHash: DAG_SCHEDULER_POLICY_HASH_V1, normalizedIndexHash: index.indexHash, maxActiveNodes: options.maxActiveNodes ?? 1, decisionSequence: 0, nextReservationSequence: 1, lastDecisionCommandId: null, activeNodeLanes: {}, reservations: {}, bypassCounters: {}, fairnessCounters: {}, dynamicExclusions: {}, providerHoldIds: [], operationalCapacities: Object.fromEntries(["worker.process", "role:implementation", "role:evaluation", "role:review", "role:check", ...plan.repositories.flatMap(({ repositoryId }) => [`repository-worktree:${repositoryId}`, `repository-integration:${repositoryId}`])].map((namespace) => [namespace, { namespace, observedCapacity: namespace.startsWith("repository-integration") ? 1 : 4, allocatedUnits: 0, reservationIds: [], observationHash: H("4") }])) }, freshness: { class: "valid_exact", receipt: ref("staleness", "freshness-dogfood", freshnessFact.hash), evaluatedPlanHash: plan.planHash, modelClosureHash: plan.modelBinding.closure.closureHash, repositoryObservationHashes: Object.fromEntries(plan.repositories.map(({ repositoryId }) => [repositoryId, repositoryObservation.hash])), affectedWorkItemIds: [], blocksNewLaunches: false, blocksIntegration: false, evaluatedAt: AT }, completion: { state: "open", authorizedScopeHash: authSet.hash, completeWorkItemIds: [], remainingAuthorizedWorkItemIds: Object.keys(workItems).sort(), unauthorizedWorkItemIds: [], completedRepositoryIds: [], completedAt: null } }, context);
  return { genesis, context, seedFacts: [reviewFact, authorizationFact, freshnessFact, repositoryObservation, authorization] };
}

function procedureCatalogFixture() {
  const fixtureEnvironment = canonicalHash({ kind: "scripted_fixture_evidence", productionGenerality: false, commands: ["git cat-file -e", "git diff-tree --check"] });
  return Object.fromEntries(PLAN_STAGE_IDS.map((stage) => { const readOnly = !["F1", "F3", "F6"].includes(stage); const executable = { executableArtifactHash: canonicalHash({ fixture: "/usr/bin/true" }), argv: ["/usr/bin/true"], cwdMode: "repository_root", environmentProfileId: "scripted-fixture", environmentProfileHash: fixtureEnvironment, environmentHash: canonicalHash({ LC_ALL: "C" }), timeoutMs: 1000, readOnly, noEdit: readOnly }; const core = { procedureId: `scripted-fixture-${stage.toLowerCase()}`, purpose: "lifecycle", stages: [stage], producerKinds: [({ F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" })[stage]], readOnly, environmentProfileHash: fixtureEnvironment, executable }; const hash = canonicalHash(core); return [hash, { ...core, hash }]; }));
}
function authorizationBinding(plan, reviewReceiptHash, receiptHashes, maxActiveNodes = 1) { const core = { planHash: plan.planHash, reviewReceiptHash, receiptHashes: [...receiptHashes].sort(), workItemIds: plan.workItems.map(({ workItemId }) => workItemId).sort(), stageScopes: Object.fromEntries(plan.workItems.map(({ workItemId }) => [workItemId, [...PLAN_STAGE_IDS]])), repositoryIds: plan.repositories.map(({ repositoryId }) => repositoryId).sort(), effectScopeIds: [], integrationTrainIds: plan.constraints.integrationTrains.map(({ trainId }) => trainId).sort(), retryCeilingsHash: plan.lifecycleBinding.retryPolicyHash, maxActiveNodes, validFrom: AT, validUntil: null }; return { ...core, hash: canonicalHash(core) }; }
function content(value) { return { ...value, contentHash: canonicalHash(value) }; }
function simpleFact(kind, id) { return withHash({ kind, id, schemaVersion: 1, issuedAt: AT }); }
function ref(kind, id, hash) { return { kind, schemaVersion: 1, id, hash, bytes: 1, mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }; }
function withHash(core) { return { ...core, hash: canonicalHash(core) }; }
function reducerInput(state, type, kind, payload, slot) { return { schemaVersion: 1, kind, type, commandId: slot, idempotencyKey: `${state.runNonce}:${slot}`, payloadHash: canonicalHash(payload), runId: state.runId, runNonce: state.runNonce, expectedRevision: state.revision, expectedSnapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch, occurredAt: AT, payload }; }
async function gitTree(repo, refName) { return { commit: await git(repo, ["rev-parse", refName]), tree: await git(repo, ["rev-parse", `${refName}^{tree}`]) }; }
async function git(cwd, args, env) { const result = await execFileAsync("git", args, { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, env: env ? { ...process.env, ...env } : process.env }); return result.stdout.trim(); }
export function commitEnvironment() { return { GIT_AUTHOR_NAME: "Scripted DAG Fixture", GIT_AUTHOR_EMAIL: "dag-fixture@example.invalid", GIT_COMMITTER_NAME: "Scripted DAG Fixture", GIT_COMMITTER_EMAIL: "dag-fixture@example.invalid", GIT_AUTHOR_DATE: AT, GIT_COMMITTER_DATE: AT, TZ: "UTC", LC_ALL: "C", LANG: "C" }; }

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2); assertKnownArgs(args, new Set(["--group", "--scenario"]), new Set(["--list"])); const groups = valuesFor(args, "--group"); const scenarios = valuesFor(args, "--scenario");
  if (args.includes("--list")) process.stdout.write(`${canonicalStringify({ groups: DOGFOOD_SCENARIO_GROUPS, scenarios: DOGFOOD_SCENARIOS.map(({ id, group }) => ({ id, group })) })}\n`);
  else process.stdout.write(`${canonicalStringify(await runCurrentDogfoodManifest({ groups, scenarios }))}\n`);
}
function assertKnownArgs(args, valued, boolean) { for (let index = 0; index < args.length; index += 1) { const arg = args[index]; if (valued.has(arg)) { if (!args[++index]) throw new Error(`${arg} requires a value`); } else if (!boolean.has(arg)) throw new Error(`Unknown argument: ${arg}`); } }
function valuesFor(args, flag) { const values = []; for (let index = 0; index < args.length; index += 1) if (args[index] === flag) { if (!args[index + 1]) throw new Error(`${flag} requires a value`); values.push(...args[++index].split(",").filter(Boolean)); } return values; }
