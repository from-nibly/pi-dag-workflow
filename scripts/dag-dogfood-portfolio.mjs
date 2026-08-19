import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import {
  RUN_EVALUATION_CLOCK_POLICY_HASH_V1, RUN_EVALUATION_PROFILE_HASH_V1, RUN_EVALUATION_PROFILE_V1,
  accumulatorClockV1, accumulatorDerivedMetricsV1, buildPairedEvaluationReportV1, buildRunEvaluationEnvelopeV1,
  cutoffIdentityHashV1, createRunObservationAccumulatorV1, dagEvaluationCohortHashV1, executionIdentityHashV1,
  foldAccumulatorObservationV1, measuredMetric, pairIdentityHashV1, parseDagEvaluationPortfolioV1,
  portfolioIdentityHashV1, unavailableMetric, validateDagEvaluationPortfolioV1, validatePairedEvaluationReportV1,
  validateRunEvaluationEnvelopeV1,
} from "../extensions/dag-workflow/dag-runtime/evaluation.ts";
import { canonicalHash, canonicalStringify } from "../extensions/dag-workflow/dag-runtime/common.ts";
import { RunEvaluationStoreV1 } from "../extensions/dag-workflow/dag-runtime/evaluation-store.ts";
import { DOGFOOD_AT, commitEnvironment, planFixture, scenario } from "./dag-dogfood-test.mjs";

const execFileAsync = promisify(execFile);
const FIXTURE_URL = new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url);
const CLOCK_EPOCH_HASH = canonicalHash({ clock: "deterministic-dogfood-monotonic-v1" });
const FIXED_ENVIRONMENT = Object.freeze({ author: "Scripted DAG Fixture <dag-fixture@example.invalid>", clock: DOGFOOD_AT, locale: "C", timezone: "UTC", objectFormat: "sha1", lifecycle: "scripted-fixture-lifecycle-v1" });
const FIXED_PROVIDER = Object.freeze({ kind: "owned-disposable-git-worker", version: 1, productionGenerality: false });
const SCRIPT_CONTRACT = Object.freeze({ version: 1, commands: ["git worktree add --detach", "git add", "git commit", "git cat-file -e", "git diff-tree --check", "git worktree remove --force"], lifecycleStages: ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"] });

export const CANONICAL_DOGFOOD_PAIR_TEMPLATES_V1 = Object.freeze([
  { templateId: "fanout-alpha", scenarioClass: "independent_fanout", order: ["serial", "parallel"], independentSubjects: true, semanticMutex: false, riskTier: "low" },
  { templateId: "fanout-beta", scenarioClass: "independent_fanout", order: ["parallel", "serial"], independentSubjects: true, semanticMutex: false, riskTier: "medium" },
  { templateId: "constraint-contract", scenarioClass: "hidden_constraint", order: ["serial", "parallel"], independentSubjects: false, semanticMutex: true, riskTier: "medium" },
  { templateId: "constraint-architecture", scenarioClass: "hidden_constraint", order: ["parallel", "serial"], independentSubjects: false, semanticMutex: true, riskTier: "high" },
  { templateId: "integration-train", scenarioClass: "integration_train", order: ["serial", "parallel"], independentSubjects: true, semanticMutex: false, multipleRepositories: false, riskTier: "high" },
  { templateId: "recovery-sensitive", scenarioClass: "recovery_sensitive", order: ["parallel", "serial"], independentSubjects: false, semanticMutex: true, riskTier: "high" },
]);

function scenarioOptions(template) {
  return { items: 2, templateId: template.templateId, multipleRepositories: template.multipleRepositories ?? true, independentCandidatePaths: true, independentSubjects: template.independentSubjects, semanticMutex: template.semanticMutex, riskTier: template.riskTier };
}

export async function buildCanonicalDogfoodPortfolioV1() {
  const root = await mkdtemp(join(tmpdir(), "pi-dag-portfolio-identities-v1-"));
  try {
    const pairs = [];
    for (const template of CANONICAL_DOGFOOD_PAIR_TEMPLATES_V1) {
      const baseline = await createBaseline(root, template.templateId);
      const plan = planFixture(baseline, 2, scenarioOptions(template));
      const pair = {
        scenarioClass: template.scenarioClass,
        baselineCommitHash: canonicalHash({ objectFormat: "sha1", oid: baseline.commit }),
        baselineTreeHash: canonicalHash({ objectFormat: "sha1", oid: baseline.tree }),
        planHash: plan.planHash,
        oracleHash: canonicalHash(plan.acceptanceOracles),
        scriptHash: canonicalHash({ ...SCRIPT_CONTRACT, templateId: template.templateId }),
        environmentHash: canonicalHash(FIXED_ENVIRONMENT), evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1,
        modelHash: canonicalHash(plan.modelBinding), providerHash: canonicalHash(FIXED_PROVIDER),
        riskCohortHash: canonicalHash(plan.workItems.map(({ risk }) => risk)),
        executions: template.order.map((mode, index) => ({ mode, order: index + 1, maxActiveNodes: mode === "serial" ? 1 : 2 })),
      };
      for (const execution of pair.executions) execution.executionIdentityHash = executionIdentityHashV1(pair, execution);
      pair.pairIdentityHash = pairIdentityHashV1(pair);
      pairs.push(pair);
    }
    pairs.sort((a, b) => a.pairIdentityHash.localeCompare(b.pairIdentityHash));
    const portfolio = { schemaVersion: 1, kind: "dag_evaluation_portfolio", pairs, excludedRecoveryDrills: ["provider_worker_loss", "conductor_crash_resume", "target_drift_conflict"] };
    portfolio.portfolioIdentityHash = portfolioIdentityHashV1(portfolio);
    const validation = validateDagEvaluationPortfolioV1(portfolio);
    assert.equal(validation.ok, true, issues(validation));
    return portfolio;
  } finally { await rm(root, { recursive: true, force: true }); }
}

export async function runCanonicalDogfoodPortfolioV1({ outputRoot = null, templateIds = [], drillNames = [] } = {}) {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(FIXTURE_URL, "utf8"));
  assert.equal(canonicalStringify(portfolio), canonicalStringify(await buildCanonicalDogfoodPortfolioV1()), "checked-in portfolio must equal exact regenerated content");
  assert.equal(portfolio.pairs.length, 6);
  assert.equal(new Set(portfolio.pairs.flatMap((pair) => pair.executions.map((execution) => execution.executionIdentityHash))).size, 12);
  const templates = new Map(CANONICAL_DOGFOOD_PAIR_TEMPLATES_V1.map((template) => [canonicalHash({ ...SCRIPT_CONTRACT, templateId: template.templateId }), template]));
  const knownTemplateIds = CANONICAL_DOGFOOD_PAIR_TEMPLATES_V1.map(({ templateId }) => templateId); const knownDrillNames = [...portfolio.excludedRecoveryDrills];
  const selectionRequested = templateIds.length > 0 || drillNames.length > 0;
  const selectedTemplateIds = templateIds.length ? [...new Set(templateIds)] : selectionRequested ? [] : knownTemplateIds; const selectedDrillNames = drillNames.length ? [...new Set(drillNames)] : selectionRequested ? [] : knownDrillNames;
  for (const id of selectedTemplateIds) if (!knownTemplateIds.includes(id)) throw new Error(`Unknown dogfood portfolio template: ${id}`);
  for (const name of selectedDrillNames) if (!knownDrillNames.includes(name)) throw new Error(`Unknown dogfood recovery drill: ${name}`);
  const root = await mkdtemp(join(tmpdir(), "pi-dag-canonical-portfolio-v1-"));
  try {
    const cohortHash = dagEvaluationCohortHashV1(portfolio);
    const results = [], envelopeHashes = [];
    for (const pair of portfolio.pairs) {
      const template = templates.get(pair.scriptHash); assert(template);
      if (!selectedTemplateIds.includes(template.templateId)) continue;
      for (const execution of [...pair.executions].sort((a, b) => a.order - b.order)) {
        const executionRoot = join(root, execution.executionIdentityHash.slice(7, 23)); await mkdir(executionRoot);
        const projections = [];
        let result;
        try { result = await scenario(executionRoot, "target", { ...scenarioOptions(template), maxActiveNodes: execution.maxActiveNodes, runLabel: execution.executionIdentityHash.slice(7, 31), returnArtifacts: true, onCommittedSnapshot: (value) => projections.push(value) }); }
        catch (error) { throw new Error(`Dogfood portfolio ${template.templateId}/${execution.mode} failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error }); }
        assert.equal(result.state.completion.state, "plan_complete");
        assert(Object.values(result.state.workItems).every(({ current, integrationReceipt }) => current === "complete" && integrationReceipt));
        assert.equal(canonicalHash({ objectFormat: "sha1", oid: result.baseline.commit }), pair.baselineCommitHash);
        assert.equal(canonicalHash({ objectFormat: "sha1", oid: result.baseline.tree }), pair.baselineTreeHash);
        assert.equal(result.plan.planHash, pair.planHash);
        const envelope = await evaluateTerminalRun(executionRoot, result, execution, projections);
        assert.equal(validateRunEvaluationEnvelopeV1(envelope).ok, true);
        assertPrivacySafe(envelope);
        envelopeHashes.push(envelope.envelopeHash);
        results.push({ executionIdentityHash: execution.executionIdentityHash, envelopeHash: envelope.envelopeHash, cohortHash, valid: true, uncompensatedInvariantsPass: Object.values(envelope.invariants).every((status) => status === "pass"), elapsedMs: envelope.metrics.timing.autonomousElapsed.numerator ?? 0, usefulWorkMs: envelope.metrics.usefulParallelism.usefulWork.numerator ?? 0, reportedCost: envelope.metrics.modelUsage.reportedCost.numerator });
      }
    }
    const expectedExecutions = selectedTemplateIds.length * 2;
    assert.equal(results.length, expectedExecutions); assert.equal(new Set(results.map((result) => result.executionIdentityHash)).size, expectedExecutions);
    assert.equal(new Set(envelopeHashes).size, expectedExecutions);
    const drills = await runSeparateRecoveryDrills(root, selectedDrillNames);
    assert.deepEqual(Object.keys(drills).sort(), [...selectedDrillNames].sort());
    assert(Object.values(drills).every(({ comparativeExecutionCount }) => comparativeExecutionCount === 0));
    const full = selectedTemplateIds.length === knownTemplateIds.length && selectedDrillNames.length === knownDrillNames.length;
    let manifest;
    if (full) {
      const report = buildPairedEvaluationReportV1(portfolio, results);
      assert.equal(validatePairedEvaluationReportV1(report, portfolio).ok, true);
      const semanticCore = { schemaVersion: 1, kind: "DagDogfoodPortfolioSemanticManifestV1", portfolioIdentityHash: portfolio.portfolioIdentityHash, cohortHash, pairCountHash: canonicalHash({ pairCount: 6 }), executionCountHash: canonicalHash({ executionCount: 12 }), recoveryDrillCountHash: canonicalHash({ recoveryDrillCount: 3 }), reportHash: canonicalHash(report), runtimeIdentityStatus: "runtime_bound_exact_excluded_from_semantic_manifest_hash" };
      const manifestCore = { ...semanticCore, semanticManifestHash: canonicalHash(semanticCore), executionEnvelopeHashes: envelopeHashes.sort(), executionEnvelopeHashesStatus: "runtime_bound_exact", recoveryDrillHashes: Object.fromEntries(Object.entries(drills).map(([name, drill]) => [name, drill.drillHash])), recoveryDrillHashesStatus: "runtime_bound_exact" };
      manifest = { ...manifestCore, manifestHash: canonicalHash(manifestCore) };
      assert.equal(manifest.executionEnvelopeHashes.length, 12);
    } else {
      const core = { schemaVersion: 1, kind: "DagDogfoodPortfolioSelectionManifestV1", portfolioIdentityHash: portfolio.portfolioIdentityHash, cohortHash, selectedTemplateIds: [...selectedTemplateIds].sort(), selectedDrillNames: [...selectedDrillNames].sort(), executionResultsHash: canonicalHash(results), executionEnvelopeHashes: envelopeHashes.sort(), recoveryDrillHashes: Object.fromEntries(Object.entries(drills).map(([name, drill]) => [name, drill.drillHash])) };
      manifest = { ...core, manifestHash: canonicalHash(core) };
    }
    if (outputRoot) { await mkdir(outputRoot, { recursive: true }); await writeFile(join(outputRoot, "dag-dogfood-portfolio-manifest-v1.json"), canonicalStringify(manifest), { mode: 0o600 }); }
    return manifest;
  } finally { await rm(root, { recursive: true, force: true }); }
}

export async function evaluateTerminalRun(projectRoot, result, execution, projections) {
  const { state, genesis, plan } = result;
  assert(projections.length > 1);
  assert.equal(new Set(projections.map(({ revision }) => revision)).size, projections.length);
  assert(projections.every((projection, index) => index === 0 || projections[index - 1].revision < projection.revision));
  assert.deepEqual({ revision: projections[0].revision, snapshotHash: projections[0].snapshotHash }, { revision: genesis.revision, snapshotHash: genesis.snapshotHash });
  assert.deepEqual({ revision: projections.at(-1).revision, snapshotHash: projections.at(-1).snapshotHash }, { revision: state.revision, snapshotHash: state.snapshotHash });
  const acceptedIntegrationLineages = Object.values(state.workItems).map((item) => ({ acceptedEvidenceHash: item.stages.F8.currentEvidence, integrationReceiptHash: item.integrationReceipt })).sort((a, b) => `${a.acceptedEvidenceHash}:${a.integrationReceiptHash}`.localeCompare(`${b.acceptedEvidenceHash}:${b.integrationReceiptHash}`));
  const creditContext = { acceptedIntegrationLineages, actionableFindingDispositions: [] }, basis = acceptedIntegrationLineages[0];
  const accumulatorIdentity = { projectIdentityHash: canonicalHash({ projectId: state.identity.projectId }), runIdentityHash: canonicalHash({ runId: state.runId }), runNonceHash: canonicalHash({ runNonce: state.runNonce }), planHash: plan.planHash, evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1, clockPolicyHash: RUN_EVALUATION_CLOCK_POLICY_HASH_V1 };
  let accumulator = createRunObservationAccumulatorV1({ identity: accumulatorIdentity, source: { revision: genesis.revision, snapshotHash: genesis.snapshotHash, observedAt: DOGFOOD_AT, clockEpochHash: CLOCK_EPOCH_HASH, monotonicTickMs: 0 }, creditContext });
  for (const projection of projections.slice(1)) accumulator = foldAccumulatorObservationV1(accumulator, { revision: projection.revision, snapshotHash: projection.snapshotHash, observedAt: DOGFOOD_AT, clockEpochHash: CLOCK_EPOCH_HASH, monotonicTickMs: projection.revision * 10, readyOperationHashes: projection.readyOperationHashes, reservedOperationHashes: projection.reservedOperationHashes, activeOperations: projection.activeOperationHashes.map((operationHash) => ({ operationHash, creditBasis: { kind: "accepted_integration_lineage", ...basis } })), humanActiveHashes: [], authorityWaitHashes: [], recoveryHashes: [], falseIndependenceIncidents: [], creditContext, droppedBefore: 0, observerFailuresBefore: 0 });
  assert.equal(accumulator.source.snapshotHash, state.snapshotHash); assert.equal(accumulator.coverage.observedRevisionCount, projections.length - 1);
  assert.equal(accumulator.coverage.droppedRevisionCount + accumulator.coverage.observerFailureCount, 0);
  const store = new RunEvaluationStoreV1(projectRoot); await store.initialize(); await store.writeAccumulator(accumulator, null);
  const sourceHashes = evaluationSourceHashes(state, accumulator, projections);
  const derived = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: execution.mode === "serial", rightCensored: false });
  const count = (value) => measuredMetric("count", value), attempts = Object.keys(state.stageAttempts).length;
  const metrics = {
    ...derived,
    outcomes: { accepted: count(Object.keys(state.workItems).length), integrated: count(Object.values(state.workItems).filter((item) => item.integrationReceipt).length) },
    attempts: { attempts: count(attempts), retries: count(Math.max(0, attempts - Object.keys(state.workItems).length * 9)), backEdges: count(Object.keys(state.evidenceIndex.invalidations).length) },
    findings: { total: count(Object.keys(state.evidenceIndex.findings).length), disposed: count(Object.keys(state.evidenceIndex.findingResolutions).length), ...derived.findings },
    integration: { conflicts: count(Object.values(state.integrationAttempts).filter(({ conflictClass }) => conflictClass !== "none").length), invalidations: count(Object.keys(state.evidenceIndex.invalidations).length), reconciledEffects: count(Object.values(state.effects).filter(({ reconciliation }) => ["applied_exact", "proven_absent", "compensated"].includes(reconciliation)).length) },
    humanAttention: { ...derived.humanAttention, decisions: count(0) },
    modelUsage: Object.fromEntries([["inputTokens", "token"], ["outputTokens", "token"], ["cacheReadTokens", "token"], ["cacheWriteTokens", "token"], ["inferenceRequests", "count"], ["reportedCost", "provider_reported_cost"]].map(([name, unit]) => [name, unavailableMetric("not_observed", unit)])),
  };
  const identity = { projectIdentityHash: accumulatorIdentity.projectIdentityHash, runIdentityHash: accumulatorIdentity.runIdentityHash, runNonceHash: accumulatorIdentity.runNonceHash, planHash: accumulatorIdentity.planHash };
  const source = { revision: state.revision, snapshotHash: state.snapshotHash, accumulatorHash: accumulator.accumulatorHash, reviewReceiptHash: state.identity.reviewReceipt.hash, authorizationReceiptHash: state.identity.authorizationReceipts[0].hash, freshnessReceiptHash: state.freshness.receipt.hash };
  const cutoff = { kind: "terminal", class: "plan_complete", cutoffAt: state.completion.completedAt, checkpointIdentityHash: null, cutoffIdentityHash: "" }; cutoff.cutoffIdentityHash = cutoffIdentityHashV1({ identity, source, cutoff });
  const creditedOperations = [...new Set(projections.flatMap((projection) => projection.activeOperationHashes))].sort().map((operationHash) => ({ operationHash, basis: { kind: "accepted_integration_lineage", ...basis } }));
  const envelope = buildRunEvaluationEnvelopeV1({ schemaVersion: 1, kind: "run_evaluation_envelope", canonicalization: "jcs-v1", evaluationProfile: { ...RUN_EVALUATION_PROFILE_V1 }, identity, source, sourceHashes, creditContext, attribution: { creditedOperations, falseIndependenceIncidents: [] }, cutoff, supersedesEnvelopeHash: null, serialPolicy: execution.mode === "serial", clock: accumulatorClockV1(accumulator), coverage: { status: "measured", sourceRevisionCount: state.revision + 1, observedRevisionCount: accumulator.coverage.observedRevisionCount, missingRevisionCount: accumulator.coverage.missingRevisionCount, droppedRevisionCount: 0, censoredIntervalCount: accumulator.coverage.censoredIntervalCount, observerFailureCount: 0 }, invariants: { snapshotAndHashes: "pass", planSourceJoins: "pass", authorizationAndScope: "pass", idempotencyAndStaleAdvancement: "pass", effectsReconciled: "pass", integrationExact: "pass", completionExact: "pass" }, metrics, postRunPulse: { confidenceFinalState: { status: "not_observed", value: null }, cognitiveEffort: { status: "not_observed", value: null }, interruptionBurden: { status: "not_observed", value: null } } });
  const validation = validateRunEvaluationEnvelopeV1(envelope); assert.equal(validation.ok, true, issues(validation));
  await store.publishEnvelope(envelope, DOGFOOD_AT); assert.equal((await store.readEnvelope(envelope.envelopeHash)).envelopeHash, envelope.envelopeHash);
  return envelope;
}

function evaluationSourceHashes(state, accumulator, projections) {
  const hashes = (record) => Object.values(record).map((value) => typeof value === "string" ? value : value.hash).filter(Boolean);
  const exact = (...values) => [...new Set(values.flat().filter(Boolean))].sort();
  const operationHashes = projections.flatMap((projection) => [...projection.readyOperationHashes, ...projection.reservedOperationHashes, ...projection.activeOperationHashes]);
  return {
    authorization: exact(state.identity.authorizationSet.hash, ...state.identity.authorizationReceipts.map(({ hash }) => hash)),
    stageEvidence: exact(...hashes(state.evidenceIndex.stageEvidence)), workerResults: exact(...hashes(state.evidenceIndex.workerResults)),
    findingsAndResolutions: exact(...hashes(state.evidenceIndex.findings), ...hashes(state.evidenceIndex.findingResolutions), canonicalHash({ runIdentityHash: canonicalHash({ runId: state.runId }), findingClosure: "none" })),
    effectReconciliation: exact(...hashes(state.evidenceIndex.effectReconciliations), ...Object.values(state.effects).map(({ observationHash }) => observationHash)),
    verification: exact(state.identity.reviewReceipt.hash, ...hashes(state.evidenceIndex.verifications), ...Object.values(state.integrationAttempts).flatMap(({ proposalVerificationFactHash, prefixEvidenceHashes, finalEvidenceHashes }) => [proposalVerificationFactHash, ...prefixEvidenceHashes, ...finalEvidenceHashes])),
    integration: exact(...hashes(state.evidenceIndex.integrationReceipts), ...Object.values(state.workItems).map(({ integrationReceipt }) => integrationReceipt)),
    otherRequired: exact(state.freshness.receipt.hash, accumulator.accumulatorHash, state.snapshotHash, ...operationHashes),
  };
}

async function runSeparateRecoveryDrills(root, selectedNames) {
  const drills = {}, definitions = [["provider_worker_loss", { crashCleanup: true }], ["conductor_crash_resume", { crashAt: "after_landing_git" }], ["target_drift_conflict", { thirdTargetDrift: true }]].filter(([name]) => selectedNames.includes(name));
  for (const [name, option] of definitions) { const result = await scenario(join(root, "recovery"), name, { items: 1, templateId: `drill-${name}`, runLabel: `drill-${name}`, ...option }); drills[name] = { comparativeExecutionCount: 0, drillHash: canonicalHash(result) }; }
  return drills;
}

async function createBaseline(parent, templateId) {
  const repo = join(parent, templateId); await mkdir(repo, { recursive: true }); await git(repo, ["init", "-b", "main"]); await git(repo, ["config", "user.name", "Scripted DAG Fixture"]); await git(repo, ["config", "user.email", "dag-fixture@example.invalid"]); await writeFile(join(repo, ".gitignore"), ".ai/\n"); await writeFile(join(repo, "shared.txt"), `baseline-${templateId}\n`); await git(repo, ["add", ".gitignore", "shared.txt"]); await git(repo, ["commit", "-m", "chore: establish dogfood baseline"], commitEnvironment()); return { commit: await git(repo, ["rev-parse", "HEAD"]), tree: await git(repo, ["rev-parse", "HEAD^{tree}"]) };
}
async function git(cwd, args, env) { return (await execFileAsync("git", args, { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, env: env ? { ...process.env, ...env } : process.env })).stdout.trim(); }
function issues(validation) { return validation.issues.map(({ path, message }) => `${path}: ${message}`).join("\n"); }
function assertPrivacySafe(value) { const bytes = canonicalStringify(value).toLowerCase(); for (const forbidden of ["/", "prose", "prompt", "transcript", "diagnostic", "locator", "sourceText"]) assert.equal(bytes.includes(forbidden.toLowerCase()), false); }

function assertKnownArgs(args, valued, boolean) { for (let index = 0; index < args.length; index += 1) { const arg = args[index]; if (valued.has(arg)) { if (!args[++index]) throw new Error(`${arg} requires a value`); } else if (!boolean.has(arg)) throw new Error(`Unknown argument: ${arg}`); } }
function valuesFor(args, flag) { const values = []; for (let index = 0; index < args.length; index += 1) if (args[index] === flag) { if (!args[index + 1]) throw new Error(`${flag} requires a value`); values.push(...args[++index].split(",").filter(Boolean)); } return values; }

async function main() {
  const args = process.argv.slice(2); assertKnownArgs(args, new Set(["--output-root", "--template", "--drill"]), new Set(["--write-fixture", "--portfolio-only"]));
  if ((args.includes("--write-fixture") || args.includes("--portfolio-only")) && args.some((arg) => ["--output-root", "--template", "--drill"].includes(arg))) throw new Error("Fixture-only portfolio modes cannot be combined with execution selectors");
  if (args.includes("--write-fixture")) { const portfolio = await buildCanonicalDogfoodPortfolioV1(); await writeFile(FIXTURE_URL, `${JSON.stringify(portfolio, null, 2)}\n`); process.stdout.write(`${canonicalStringify({ kind: "DagDogfoodPortfolioFixtureReceiptV1", portfolioIdentityHash: portfolio.portfolioIdentityHash })}\n`); return; }
  if (args.includes("--portfolio-only")) { const portfolio = await buildCanonicalDogfoodPortfolioV1(); process.stdout.write(`${canonicalStringify({ kind: "DagDogfoodPortfolioFixtureReceiptV1", portfolioIdentityHash: portfolio.portfolioIdentityHash })}\n`); return; }
  const index = args.indexOf("--output-root"), outputRoot = index < 0 ? null : args[index + 1]; if (index >= 0 && !outputRoot) throw new Error("--output-root requires a directory");
  const templateIds = valuesFor(args, "--template"), drillNames = valuesFor(args, "--drill");
  process.stdout.write(`${canonicalStringify(await runCanonicalDogfoodPortfolioV1({ outputRoot, templateIds, drillNames }))}\n`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
