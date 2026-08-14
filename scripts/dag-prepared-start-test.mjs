import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { canonicalHash, canonicalStringify, parseStrictJson } from "../extensions/dag-workflow/dag-runtime/common.ts";
import { DagConductorServiceV1 } from "../extensions/dag-workflow/dag-runtime/conductor.ts";
import { sealDagRunStateV1 } from "../extensions/dag-workflow/dag-runtime/run-state.ts";
import { planFixture, runFixture } from "./dag-dogfood-test.mjs";

const execFileAsync = promisify(execFile);
const AT = "2026-08-14T12:00:00.000Z";
const START_BOUNDARIES = [
  "after_start_intent",
  "after_run_authority",
  "before_genesis_initialize",
  "after_genesis_initialize",
  "before_owner_attach",
  "after_owner_attach",
  "before_final_binding",
  "after_final_binding",
  "after_start_active",
  "before_response",
];

if (process.argv[2] === "__crash_at_start_boundary") {
  const [, , , root, sessionId, inputPath, boundary] = process.argv;
  const input = parseStrictJson(await readFile(inputPath, "utf8"));
  const ctx = { cwd: root, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null } };
  await new DagConductorServiceV1({ startFailpoint(point) { if (point === boundary) process.exit(86); } }).startPrepared(ctx, input);
  process.exit(87);
}

async function fixture(label, sessionId = `session-${label}`) {
  const root = await mkdtemp(join(tmpdir(), `pi-dag-prepared-${label}-`));
  await execFileAsync("git", ["init", "-b", "main"], { cwd: root });
  await writeFile(join(root, "baseline.txt"), `${label}\n`);
  await execFileAsync("git", ["add", "baseline.txt"], { cwd: root });
  await execFileAsync("git", ["-c", "user.name=DAG Test", "-c", "user.email=dag@example.invalid", "commit", "-m", "baseline"], { cwd: root });
  const commit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd: root })).stdout.trim();
  const tree = (await execFileAsync("git", ["rev-parse", "HEAD^{tree}"], { cwd: root })).stdout.trim();
  const plan = planFixture({ commit, tree }, 1, { templateId: label });
  const { genesis, context, seedFacts } = runFixture(plan, 1, { runLabel: label });
  const ctx = { cwd: root, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null } };
  const input = {
    runId: genesis.runId,
    runNonce: genesis.runNonce,
    planHash: plan.planHash,
    maxActiveNodes: genesis.scheduler.maxActiveNodes,
    occurredAt: AT,
    plan,
    genesis,
    context,
    seedFacts,
    sourcePlanningPlanId: `planning-${label}`,
    sourcePlanningPlanHash: canonicalHash({ planning: label }),
  };
  return { root, ctx, input };
}

async function intentFor(root) {
  const sessionDirectories = await readdir(join(root, ".ai", "dag-start-intents-v1"));
  assert.equal(sessionDirectories.length, 1);
  const names = await readdir(join(root, ".ai", "dag-start-intents-v1", sessionDirectories[0]));
  assert.equal(names.length, 1);
  return parseStrictJson(await readFile(join(root, ".ai", "dag-start-intents-v1", sessionDirectories[0], names[0]), "utf8"));
}

for (const boundary of START_BOUNDARIES) {
  const exact = await fixture(boundary);
  try {
    const inputPath = join(exact.root, ".prepared-start-input.json"); await writeFile(inputPath, canonicalStringify(exact.input));
    await assert.rejects(
      () => execFileAsync(process.execPath, [process.argv[1], "__crash_at_start_boundary", exact.root, exact.ctx.sessionManager.getSessionId(), inputPath, boundary], { cwd: process.cwd() }),
      (error) => error?.code === 86,
      `${boundary} terminates a real starter process`,
    );
    const recovered = await new DagConductorServiceV1().startPrepared(exact.ctx, exact.input);
    assert.equal(recovered.binding.runId, exact.input.runId, `${boundary} retry converges to the intended run`);
    assert.equal(recovered.binding.planHash, exact.input.planHash, `${boundary} retry preserves exact plan authority`);
    assert.equal(recovered.state.owner.sessionId, exact.ctx.sessionManager.getSessionId(), `${boundary} retry has active session ownership`);
    assert.equal(recovered.state.scheduler.decisionSequence, 0, `${boundary} cannot schedule before the active binding response`);
    assert.deepEqual(recovered.state.scheduler.reservations, {}, `${boundary} cannot dispatch work inside the prepared-start boundary`);
    assert.equal((await new DagConductorServiceV1().status(exact.ctx, exact.input.runId)).state.snapshotHash, recovered.state.snapshotHash, `${boundary} leaves existing binding readers compatible`);
    const intent = await intentFor(exact.root);
    assert.equal(intent.state, "active", `${boundary} retry finalizes the durable intent`);
    assert.equal(await new DagConductorServiceV1().pendingStart(exact.ctx, exact.input.sourcePlanningPlanId, exact.input.sourcePlanningPlanHash), null, `${boundary} has no unfinished start after recovery`);
    assert.equal(intent.runId, exact.input.runId);
    assert.equal(intent.planHash, exact.input.planHash);
    assert.equal(intent.sourcePlanningPlanId, exact.input.sourcePlanningPlanId);
    assert.equal(intent.sourcePlanningPlanHash, exact.input.sourcePlanningPlanHash);
  } finally { await rm(exact.root, { recursive: true, force: true }); }
}

{
  const exact = await fixture("duplicate");
  try {
    const conductor = new DagConductorServiceV1();
    const first = await conductor.startPrepared(exact.ctx, exact.input);
    const duplicate = await conductor.startPrepared(exact.ctx, exact.input);
    assert.equal(duplicate.binding.bindingHash, first.binding.bindingHash, "exact duplicate start returns the same binding");
    assert.equal(duplicate.state.snapshotHash, first.state.snapshotHash, "exact duplicate start does not mutate run authority");
    assert.equal((await intentFor(exact.root)).revision, 1, "exact duplicate start does not revise an already active intent");
  } finally { await rm(exact.root, { recursive: true, force: true }); }
}

{
  const exact = await fixture("path-wrapper");
  try {
    const unsealed = structuredClone(exact.input.genesis); delete unsealed.snapshotHash;
    unsealed.desired = { ...unsealed.desired, run: "paused" }; unsealed.current = { ...unsealed.current, run: "paused" };
    const genesis = sealDagRunStateV1(unsealed, exact.input.context);
    const artifactDirectory = join(exact.root, ".ai", "prepared-wrapper"); await mkdir(artifactDirectory, { recursive: true });
    await writeFile(join(artifactDirectory, "plan.json"), canonicalStringify(exact.input.plan));
    await writeFile(join(artifactDirectory, "genesis.json"), canonicalStringify(genesis));
    await writeFile(join(artifactDirectory, "context.json"), canonicalStringify({ ...exact.input.context, seedFacts: exact.input.seedFacts }));
    const result = await new DagConductorServiceV1().start(exact.ctx, { runId: genesis.runId, runNonce: genesis.runNonce, planHash: exact.input.planHash, planPath: ".ai/prepared-wrapper/plan.json", genesisPath: ".ai/prepared-wrapper/genesis.json", contextPath: ".ai/prepared-wrapper/context.json", maxActiveNodes: 1, occurredAt: AT });
    assert.equal(result.binding.runId, genesis.runId, "path-based start remains a compatibility wrapper");
    assert.equal(result.state.current.run, "paused");
    assert.equal((await intentFor(exact.root)).state, "active", "path wrapper uses the prepared durable boundary");
  } finally { await rm(exact.root, { recursive: true, force: true }); }
}

{
  const exact = await fixture("mismatch");
  try {
    const crashing = new DagConductorServiceV1({ startFailpoint(point) { if (point === "after_start_intent") throw new Error("crash:mismatch"); } });
    await assert.rejects(() => crashing.startPrepared(exact.ctx, exact.input), /crash:mismatch/);
    const pending = await new DagConductorServiceV1().pendingStart(exact.ctx, exact.input.sourcePlanningPlanId, exact.input.sourcePlanningPlanHash);
    assert.deepEqual(pending, {
      runId: exact.input.runId, runNonce: exact.input.runNonce, planHash: exact.input.planHash,
      sourcePlanningPlanId: exact.input.sourcePlanningPlanId, sourcePlanningPlanHash: exact.input.sourcePlanningPlanHash, startedAt: exact.input.occurredAt,
    }, "a restarted command can recover the exact unfinished start identity");
    await assert.rejects(
      () => new DagConductorServiceV1().pendingStart(exact.ctx, "other-plan", canonicalHash({ planning: "other" })),
      /belongs to a different planning plan/,
      "an unfinished start blocks a cross-plan second run",
    );
    const otherPlan = planFixture(exact.input.plan.repositories[0].baseline, 1, { templateId: "unfinished-other" });
    const otherPrepared = runFixture(otherPlan, 1, { runLabel: "unfinished-other" });
    const otherInput = {
      ...exact.input, runId: otherPrepared.genesis.runId, runNonce: otherPrepared.genesis.runNonce, planHash: otherPlan.planHash,
      plan: otherPlan, genesis: otherPrepared.genesis, context: otherPrepared.context, seedFacts: otherPrepared.seedFacts,
      sourcePlanningPlanId: "planning-unfinished-other", sourcePlanningPlanHash: canonicalHash({ planning: "unfinished-other" }),
    };
    await assert.rejects(() => new DagConductorServiceV1().startPrepared(exact.ctx, otherInput), /recover it before starting another run/, "the authoritative start boundary blocks a cross-plan second run");
    await assert.rejects(() => stat(join(exact.root, ".ai", "dag-runs-v1", otherInput.runId)), /ENOENT/, "blocked cross-plan start cannot publish run authority");
    const mismatched = { ...exact.input, seedFacts: [...exact.input.seedFacts].reverse() };
    await assert.rejects(() => new DagConductorServiceV1().startPrepared(exact.ctx, mismatched), /does not match the exact durable start intent/, "mismatched replay is rejected from the durable intent");
    await assert.rejects(() => stat(join(exact.root, ".ai", "dag-runs-v1", exact.input.runId, "run-state.json")), /ENOENT/, "mismatched replay cannot create run authority after an intent-only crash");
  } finally { await rm(exact.root, { recursive: true, force: true }); }
}

{
  const first = await fixture("bound-first", "session-conflict");
  try {
    await new DagConductorServiceV1().startPrepared(first.ctx, first.input);
    const plan = planFixture({ commit: first.input.plan.repositories[0].baseline.commit, tree: first.input.plan.repositories[0].baseline.tree }, 1, { templateId: "bound-second" });
    const prepared = runFixture(plan, 1, { runLabel: "bound-second" });
    const conflicting = { ...first.input, runId: prepared.genesis.runId, runNonce: prepared.genesis.runNonce, planHash: plan.planHash, plan, genesis: prepared.genesis, context: prepared.context, seedFacts: prepared.seedFacts, sourcePlanningPlanId: "planning-bound-second", sourcePlanningPlanHash: canonicalHash({ planning: "bound-second" }) };
    await assert.rejects(() => new DagConductorServiceV1().startPrepared(first.ctx, conflicting), /Session already has a different exact DAG run binding/, "conflicting current-session binding is preflighted");
    await assert.rejects(() => stat(join(first.root, ".ai", "dag-runs-v1", conflicting.runId)), /ENOENT/, "binding conflict is rejected before any conflicting run write");
  } finally { await rm(first.root, { recursive: true, force: true }); }
}

console.log("dag prepared start tests passed");
