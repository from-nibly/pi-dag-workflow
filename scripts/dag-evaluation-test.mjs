import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, rename, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  RUN_EVALUATION_CLOCK_POLICY_HASH_V1,
  RUN_EVALUATION_PROFILE_HASH_V1,
  RUN_EVALUATION_PROFILE_V1,
  accumulatorClockV1,
  accumulatorDerivedMetricsV1,
  buildPairedEvaluationReportV1,
  buildRunEvaluationEnvelopeV1,
  canonicalStringify,
  createRunObservationAccumulatorV1,
  cutoffIdentityHashV1,
  dagEvaluationCohortHashV1,
  deriveCreditBasisV1,
  executionIdentityHashV1,
  foldAccumulatorObservationV1,
  measuredMetric,
  pairIdentityHashV1,
  parseDagEvaluationPortfolioV1,
  parseRfc3339UtcNanosecondsV1,
  portfolioIdentityHashV1,
  sourceClosureV1,
  sweepQualityConditionedIntervalsV1,
  unavailableMetric,
  usefulParallelismMetricsV1,
  validateDagEvaluationPortfolioV1,
  validateEvaluationMetricV1,
  validateRunEvaluationEnvelopeV1,
  validateRunObservationAccumulatorV1,
} from "../extensions/dag-workflow/dag-runtime/evaluation.ts";
import {
  RunEvaluationObserverV1,
  RunEvaluationStoreBusyError,
  RunEvaluationStoreConflictError,
  RunEvaluationStoreV1,
} from "../extensions/dag-workflow/dag-runtime/evaluation-store.ts";
import { canonicalHash } from "../extensions/dag-workflow/dag-runtime/common.ts";

const H = (char) => `sha256:${char.repeat(64)}`;
const I = (label) => canonicalHash({ identity: label });
const NOW = "2026-08-05T00:00:00.000Z";
const clone = (value) => structuredClone(value);
const ACCEPTED_BASIS = Object.freeze({ kind: "accepted_integration_lineage", acceptedEvidenceHash: H("4"), integrationReceiptHash: H("9") });
const CREDIT_CONTEXT = Object.freeze({ acceptedIntegrationLineages: [{ acceptedEvidenceHash: H("4"), integrationReceiptHash: H("9") }], actionableFindingDispositions: [{ findingHash: H("5"), currentDispositionHash: H("6") }] });
const TRANSITION_BASIS = Object.freeze({ kind: "accepted_integration_lineage", acceptedEvidenceHash: H("e"), integrationReceiptHash: H("f") });
const TRANSITION_CREDIT_CONTEXT = Object.freeze({ acceptedIntegrationLineages: [...CREDIT_CONTEXT.acceptedIntegrationLineages, { acceptedEvidenceHash: H("e"), integrationReceiptHash: H("f") }], actionableFindingDispositions: [...CREDIT_CONTEXT.actionableFindingDispositions] });
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

function countMetric(value, unit = "count") { return measuredMetric(unit, value, null, 1); }
function metricSections(parallelism = null) {
  const count = (value) => countMetric(value);
  return {
    accumulatorTelemetry: {
      counters: { readinessIntervals: 0, dispatchWaitIntervals: 0, activeIntervals: 2, humanActiveIntervals: 0, authorityWaitIntervals: 0, recoveryIntervals: 0, falseIndependenceIncidents: 0 },
      sums: { readinessWaitMs: 0, dispatchWaitMs: 0, humanActiveMs: 0, authorityWaitMs: 0, recoveryMs: 0, falseIndependenceWasteMs: 0 },
      histograms: { readinessWait: [0, 0, 0, 0, 0, 0], dispatchWait: [0, 0, 0, 0, 0, 0], recovery: [0, 0, 0, 0, 0, 0] },
      integrals: { autonomousElapsedMs: 3_000, allOperationWorkMs: 5_000, creditableWorkMs: 5_000, usefulOverlapMs: 2_000, parallelOpportunityMs: 3_000 },
      coverage: { observedRevisionCount: 3, droppedRevisionCount: 0, missingRevisionCount: 0, censoredIntervalCount: 0, observerFailureCount: 0, revisionGaps: [], revisionGapOverflowCount: 0 },
    },
    outcomes: { accepted: count(2), integrated: count(2) },
    attempts: { attempts: count(9), retries: count(1), backEdges: count(1) },
    findings: { total: count(1), disposed: count(1), falseIndependenceIncidents: count(0), falseIndependenceWaste: measuredMetric("milliseconds", 0) },
    integration: { conflicts: count(0), invalidations: count(0), reconciledEffects: count(2) },
    timing: {
      autonomousElapsed: measuredMetric("milliseconds", 3_000),
      readinessWait: measuredMetric("milliseconds", 0),
      readinessWaitIntervals: count(0),
      dispatchWait: measuredMetric("milliseconds", 0),
      dispatchWaitIntervals: count(0),
      recovery: measuredMetric("milliseconds", 0),
      recoveryIntervals: count(0),
    },
    waitHistograms: {
      readinessWait: Array.from({ length: 6 }, () => count(0)),
      dispatchWait: Array.from({ length: 6 }, () => count(0)),
      recovery: Array.from({ length: 6 }, () => count(0)),
    },
    usefulParallelism: parallelism ?? {
      usefulWork: measuredMetric("milliseconds", 5_000),
      usefulAverageConcurrency: measuredMetric("ratio", 5_000, 3_000),
      usefulOverlapArea: measuredMetric("milliseconds", 2_000),
      parallelOpportunityArea: measuredMetric("milliseconds", 3_000),
      opportunityCapture: measuredMetric("ratio", 2_000, 3_000),
      allOperationTime: measuredMetric("milliseconds", 5_000),
      workEfficiency: measuredMetric("ratio", 5_000, 5_000),
    },
    humanAttention: { activeMinutes: measuredMetric("milliseconds", 0), activeIntervals: count(0), authorityWait: measuredMetric("milliseconds", 0), authorityWaitIntervals: count(0), decisions: count(0) },
    modelUsage: { inputTokens: countMetric(100, "token"), outputTokens: countMetric(30, "token"), cacheReadTokens: countMetric(20, "token"), cacheWriteTokens: countMetric(0, "token"), inferenceRequests: unavailableMetric("not_observed", "count"), reportedCost: measuredMetric("provider_reported_cost", 0.25) },
    instrumentation: { observedRevisions: count(3), droppedRevisions: count(0), missingRevisions: count(0), censoredIntervals: count(0) },
  };
}
function envelopeCore({ runIdentityHash = I("run-1"), projectIdentityHash = I("project-1"), sourceRevision = 3, sourceSnapshotHash = H("d"), sourceAccumulatorHash = H("e"), cutoffAt = "2026-08-05T00:05:00.000Z", cutoffKind = "terminal", cutoffClass = "plan_complete", checkpointIdentityHash = null, supersedesEnvelopeHash = null, serialPolicy = false, clock = null, metrics = metricSections(), coverage = null, creditContext = CREDIT_CONTEXT } = {}) {
  const identity = { projectIdentityHash, runIdentityHash, runNonceHash: H("a"), planHash: H("b") };
  const source = { revision: sourceRevision, snapshotHash: sourceSnapshotHash, accumulatorHash: sourceAccumulatorHash, reviewReceiptHash: H("1"), authorizationReceiptHash: H("2"), freshnessReceiptHash: H("3") };
  const cutoff = { kind: cutoffKind, class: cutoffClass, cutoffAt, checkpointIdentityHash, cutoffIdentityHash: "" };
  cutoff.cutoffIdentityHash = cutoffIdentityHashV1({ identity, source, cutoff });
  return {
    schemaVersion: 1,
    kind: "run_evaluation_envelope",
    canonicalization: "jcs-v1",
    evaluationProfile: { ...RUN_EVALUATION_PROFILE_V1 },
    identity,
    source,
    creditContext: clone(creditContext),
    sourceHashes: {
      authorization: [H("2")], stageEvidence: [H("4"), H("5")], workerResults: [H("6")], findingsAndResolutions: [H("5"), H("6")],
      effectReconciliation: [H("7")], verification: [H("1"), H("8")].sort(), integration: [H("9")], otherRequired: [H("0"), H("3"), sourceAccumulatorHash].sort(),
    },
    attribution: {
      creditedOperations: [{ operationHash: H("0"), basis: clone(ACCEPTED_BASIS) }],
      falseIndependenceIncidents: [],
    },
    cutoff,
    supersedesEnvelopeHash,
    serialPolicy,
    clock: clock ?? { quality: "same_epoch", clockEpochHash: H("c") },
    coverage: coverage ?? { status: cutoffKind === "right_censored" ? "censored" : "measured", sourceRevisionCount: sourceRevision + 1, observedRevisionCount: sourceRevision, missingRevisionCount: 0, droppedRevisionCount: 0, censoredIntervalCount: cutoffKind === "right_censored" ? 1 : 0, observerFailureCount: 0 },
    invariants: { snapshotAndHashes: "pass", planSourceJoins: "pass", authorizationAndScope: "pass", idempotencyAndStaleAdvancement: "pass", effectsReconciled: "pass", integrationExact: "pass", completionExact: cutoffKind === "terminal" ? "pass" : "not_observed" },
    metrics,
    postRunPulse: { confidenceFinalState: { status: "not_observed", value: null }, cognitiveEffort: { status: "not_observed", value: null }, interruptionBurden: { status: "not_observed", value: null } },
  };
}
function observation(revision, tick, { ready = [], reserved = [], active = [], human = [], authority = [], recovery = [], incidents = [], creditContext = CREDIT_CONTEXT, epoch = H("c"), droppedBefore = 0, observerFailuresBefore = 0, snapshotHash = H(String(revision % 10)) } = {}) {
  return { revision, snapshotHash, observedAt: `2026-08-05T00:00:0${Math.min(revision, 9)}.000Z`, clockEpochHash: epoch, monotonicTickMs: tick, readyOperationHashes: [...ready].sort(), reservedOperationHashes: [...reserved].sort(), activeOperations: [...active].sort((a, b) => a.operationHash.localeCompare(b.operationHash)), humanActiveHashes: [...human].sort(), authorityWaitHashes: [...authority].sort(), recoveryHashes: [...recovery].sort(), falseIndependenceIncidents: [...incidents].sort((a, b) => `${a.findingHash}:${a.currentDispositionHash}`.localeCompare(`${b.findingHash}:${b.currentDispositionHash}`)), creditContext: clone(creditContext), droppedBefore, observerFailuresBefore };
}
function initialAccumulator(runIdentityHash = I("run-1"), projectIdentityHash = I("project-1"), source = {}) {
  return createRunObservationAccumulatorV1({
    identity: { projectIdentityHash, runIdentityHash, runNonceHash: H("a"), planHash: H("b"), evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1, clockPolicyHash: RUN_EVALUATION_CLOCK_POLICY_HASH_V1 },
    source: { revision: source.revision ?? 0, snapshotHash: source.snapshotHash ?? H("0"), observedAt: source.observedAt ?? NOW, clockEpochHash: source.clockEpochHash ?? H("c"), monotonicTickMs: source.monotonicTickMs ?? 0 },
    creditContext: clone(CREDIT_CONTEXT),
  });
}
function observerIdentity(accumulator) { return Object.freeze({ ...accumulator.identity, creditContextHash: accumulator.creditContextHash }); }
function committed(identity, revision, snapshotHash = H(String(revision % 10))) { return { ...identity, revision, snapshotHash }; }
function resealEnvelope(envelope) {
  delete envelope.envelopeHash;
  envelope.envelopeHash = canonicalHash(envelope);
  return envelope;
}
function resealPortfolio(portfolio) {
  for (const pair of portfolio.pairs) {
    for (const execution of pair.executions) execution.executionIdentityHash = executionIdentityHashV1(pair, execution);
    pair.pairIdentityHash = pairIdentityHashV1(pair);
  }
  portfolio.pairs.sort((left, right) => left.pairIdentityHash.localeCompare(right.pairIdentityHash));
  portfolio.portfolioIdentityHash = portfolioIdentityHashV1(portfolio);
  return portfolio;
}
function pairedResults(portfolio) {
  const cohortHash = dagEvaluationCohortHashV1(portfolio);
  return portfolio.pairs.flatMap((pair, pairIndex) => pair.executions.map((execution) => ({
    executionIdentityHash: execution.executionIdentityHash,
    envelopeHash: canonicalHash({ execution: execution.executionIdentityHash }),
    cohortHash,
    valid: true,
    uncompensatedInvariantsPass: true,
    elapsedMs: execution.mode === "serial" ? 1_000 + pairIndex * 10 : 900 + pairIndex * 30,
    usefulWorkMs: 800 + pairIndex,
    reportedCost: pairIndex === 5 ? null : 1 + pairIndex / 10,
  })));
}
async function buildBoundEnvelope(store, inputCore) {
  const core = clone(inputCore);
  let accumulator = await store.readAccumulator(core.identity.runIdentityHash);
  if (!accumulator) {
    accumulator = createRunObservationAccumulatorV1({
      identity: { ...core.identity, evaluationProfileHash: core.evaluationProfile.profileHash, clockPolicyHash: RUN_EVALUATION_CLOCK_POLICY_HASH_V1 },
      source: { revision: core.source.revision, snapshotHash: core.source.snapshotHash, observedAt: NOW, clockEpochHash: core.clock.clockEpochHash ?? H("c"), monotonicTickMs: 3_000 },
      creditContext: clone(core.creditContext),
    });
    accumulator.integrals = {
      autonomousElapsedMs: core.metrics.timing.autonomousElapsed.numerator ?? 0,
      allOperationWorkMs: core.metrics.usefulParallelism.allOperationTime.numerator ?? 0,
      creditableWorkMs: core.metrics.usefulParallelism.usefulWork.numerator ?? 0,
      usefulOverlapMs: core.metrics.usefulParallelism.usefulOverlapArea.numerator ?? 0,
      parallelOpportunityMs: core.metrics.usefulParallelism.parallelOpportunityArea.numerator ?? core.metrics.usefulParallelism.usefulOverlapArea.numerator ?? 0,
    };
    accumulator.coverage.observedRevisionCount = core.coverage.observedRevisionCount;
    accumulator.coverage.missingRevisionCount = core.coverage.missingRevisionCount;
    accumulator.coverage.droppedRevisionCount = core.coverage.droppedRevisionCount;
    accumulator.coverage.censoredIntervalCount = core.coverage.censoredIntervalCount;
    accumulator.coverage.observerFailureCount = core.coverage.observerFailureCount;
    delete accumulator.accumulatorHash;
    accumulator.accumulatorHash = canonicalHash(accumulator);
    assert.equal(validateRunObservationAccumulatorV1(accumulator).ok, true);
    await store.writeAccumulator(accumulator, null);
  }
  const oldAccumulatorHash = core.source.accumulatorHash;
  core.source.accumulatorHash = accumulator.accumulatorHash;
  core.sourceHashes.otherRequired = core.sourceHashes.otherRequired.filter((hash) => hash !== oldAccumulatorHash);
  if (!core.sourceHashes.otherRequired.includes(accumulator.accumulatorHash)) core.sourceHashes.otherRequired.push(accumulator.accumulatorHash);
  core.sourceHashes.otherRequired.sort();
  core.coverage = {
    ...core.coverage,
    sourceRevisionCount: accumulator.source.revision + 1,
    observedRevisionCount: accumulator.coverage.observedRevisionCount,
    missingRevisionCount: accumulator.coverage.missingRevisionCount,
    droppedRevisionCount: accumulator.coverage.droppedRevisionCount,
    censoredIntervalCount: accumulator.coverage.censoredIntervalCount,
    observerFailureCount: accumulator.coverage.observerFailureCount,
  };
  core.clock = accumulatorClockV1(accumulator);
  const exactMetrics = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: core.serialPolicy, rightCensored: core.cutoff.kind === "right_censored" });
  core.metrics.accumulatorTelemetry = exactMetrics.accumulatorTelemetry;
  core.metrics.timing = exactMetrics.timing;
  core.metrics.waitHistograms = exactMetrics.waitHistograms;
  core.metrics.usefulParallelism = exactMetrics.usefulParallelism;
  core.metrics.humanAttention = { ...core.metrics.humanAttention, ...exactMetrics.humanAttention };
  core.metrics.findings = { ...core.metrics.findings, ...exactMetrics.findings };
  core.metrics.instrumentation = exactMetrics.instrumentation;
  core.cutoff.cutoffIdentityHash = cutoffIdentityHashV1({ identity: core.identity, source: core.source, cutoff: core.cutoff });
  return buildRunEvaluationEnvelopeV1(core);
}

if (process.env.DAG_EVALUATION_TEST_CHILD_MODE === "crash-lock") {
  const accumulator = JSON.parse(await readFile(process.env.DAG_EVALUATION_TEST_ACCUMULATOR_PATH, "utf8"));
  const childStore = new RunEvaluationStoreV1(process.env.DAG_EVALUATION_TEST_PROJECT_ROOT, {
    failpoint: async (point) => {
      if (point !== "after_accumulator_temp_sync") return;
      await writeFile(process.env.DAG_EVALUATION_TEST_READY_PATH, String(process.pid), { flag: "wx", mode: 0o600 });
      setInterval(() => undefined, 60_000);
      await new Promise(() => undefined);
    },
  });
  await childStore.initialize();
  await childStore.writeAccumulator(accumulator, null);
  process.exit(0);
}

if (process.env.DAG_EVALUATION_TEST_CHILD_MODE === "publish-envelope") {
  const envelope = JSON.parse(await readFile(process.env.DAG_EVALUATION_TEST_ENVELOPE_PATH, "utf8"));
  const childStore = new RunEvaluationStoreV1(process.env.DAG_EVALUATION_TEST_PROJECT_ROOT, {
    failpoint: async (point) => {
      if (point !== "before_envelope_index_commit") return;
      await writeFile(process.env.DAG_EVALUATION_TEST_READY_PATH, String(process.pid), { flag: "wx", mode: 0o600 });
      for (;;) {
        try { await readFile(process.env.DAG_EVALUATION_TEST_RELEASE_PATH); return; }
        catch (error) { if (error.code !== "ENOENT") throw error; }
        await delay(5);
      }
    },
  });
  await childStore.publishEnvelope(envelope, NOW);
  process.exit(0);
}

// Strict schemas, semantic status rules, deterministic closures, and hash-free-of-hash construction.
test("schemas reject unknown fields and contradictory metric states", () => {
  assert.equal(validateEvaluationMetricV1(measuredMetric("count", 0)).ok, true);
  assert.equal(validateEvaluationMetricV1({ ...measuredMetric("ratio", 1, 2), missingCount: 1 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "unknown", unit: "count", numerator: null, denominator: null, observedCount: 0, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ ...measuredMetric("count", 1), extra: 1 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "zero_exposure", unit: "ratio", numerator: 0, denominator: 1, observedCount: 0, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "partial_coverage", unit: "count", numerator: 1, denominator: null, observedCount: 1, missingCount: 1, censoredCount: 0 }).ok, true);
  assert.equal(validateEvaluationMetricV1({ status: "censored", unit: "count", numerator: 1, denominator: null, observedCount: 1, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "zero_exposure", unit: "ratio", numerator: 0, denominator: 0, observedCount: 999, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "zero_exposure", unit: "count", numerator: 0, denominator: 0, observedCount: 1, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "partial_coverage", unit: "ratio", numerator: 10, denominator: 0, observedCount: 1, missingCount: 1, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "censored", unit: "ratio", numerator: null, denominator: 999, observedCount: 0, missingCount: 0, censoredCount: 1 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "not_observed", unit: "count", numerator: null, denominator: null, observedCount: 0, missingCount: 0, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1({ status: "unsupported_policy", unit: "count", numerator: null, denominator: null, observedCount: 0, missingCount: 1, censoredCount: 0 }).ok, false);
  assert.equal(validateEvaluationMetricV1(measuredMetric("count", 1.5)).ok, false);
});

test("source closures and envelopes are deterministic and content addressed without generated time", () => {
  assert.deepEqual(sourceClosureV1([H("2"), H("1")]), sourceClosureV1([H("1"), H("2")]));
  assert.throws(() => sourceClosureV1([H("1"), H("1")]));
  const first = buildRunEvaluationEnvelopeV1(envelopeCore());
  const second = buildRunEvaluationEnvelopeV1(clone(envelopeCore()));
  assert.equal(canonicalStringify(first), canonicalStringify(second));
  assert.equal(first.envelopeHash, second.envelopeHash);
  assert.equal("generatedAt" in first, false);
  const changed = buildRunEvaluationEnvelopeV1(envelopeCore({ sourceSnapshotHash: H("e") }));
  assert.notEqual(first.envelopeHash, changed.envelopeHash);
  assert.equal(validateRunEvaluationEnvelopeV1(first).ok, true);
  assert.equal(validateRunEvaluationEnvelopeV1({ ...first, unknown: true }).ok, false);
  assert.equal(validateRunEvaluationEnvelopeV1({ ...first, envelopeHash: H("f") }).ok, false);
  const forgedCommitment = buildRunEvaluationEnvelopeV1({ ...envelopeCore(), sourceClosures: { authorization: sourceClosureV1([]) } });
  assert.deepEqual(forgedCommitment.sourceClosures.authorization, sourceClosureV1([H("2")]));
  const forgedMembership = envelopeCore();
  forgedMembership.attribution.creditedOperations[0].basis.acceptedEvidenceHash = H("f");
  assert.throws(() => buildRunEvaluationEnvelopeV1(forgedMembership), /exact stageEvidence source set/);
  const incidentCore = envelopeCore();
  incidentCore.sourceHashes.findingsAndResolutions = [H("5"), H("6")];
  incidentCore.attribution.falseIndependenceIncidents = [{ findingHash: H("5"), currentDispositionHash: H("6"), operationHashes: [H("0")], wastedOperationMs: 75 }];
  incidentCore.metrics.findings.falseIndependenceIncidents = countMetric(1);
  incidentCore.metrics.findings.falseIndependenceWaste = measuredMetric("milliseconds", 75);
  assert.equal(buildRunEvaluationEnvelopeV1(incidentCore).metrics.findings.falseIndependenceWaste.numerator, 75);
  incidentCore.sourceHashes.findingsAndResolutions = [H("5")];
  assert.throws(() => buildRunEvaluationEnvelopeV1(incidentCore), /exact findingsAndResolutions source set/);
});

test("builder and validator enforce timing statuses from clock, coverage, cutoff, and serial policy", () => {
  const timingStatuses = (envelope) => [envelope.metrics.timing.autonomousElapsed, ...Object.values(envelope.metrics.usefulParallelism)].map(({ status }) => status);
  const unsupported = buildRunEvaluationEnvelopeV1(envelopeCore({ clock: { quality: "unsupported", clockEpochHash: null } }));
  assert.deepEqual(new Set(timingStatuses(unsupported)), new Set(["unsupported_policy"]));

  const partial = buildRunEvaluationEnvelopeV1(envelopeCore({ coverage: { status: "measured", sourceRevisionCount: 4, observedRevisionCount: 3, missingRevisionCount: 1, droppedRevisionCount: 1, censoredIntervalCount: 0, observerFailureCount: 1 } }));
  assert.equal(partial.coverage.status, "partial_coverage");
  assert.deepEqual(new Set(timingStatuses(partial)), new Set(["partial_coverage"]));
  assert.equal(partial.metrics.timing.autonomousElapsed.missingCount, 3);

  const unobserved = buildRunEvaluationEnvelopeV1(envelopeCore({ sourceRevision: 0, coverage: { status: "measured", sourceRevisionCount: 1, observedRevisionCount: 0, missingRevisionCount: 1, droppedRevisionCount: 0, censoredIntervalCount: 0, observerFailureCount: 0 } }));
  assert.deepEqual(new Set(timingStatuses(unobserved)), new Set(["not_observed"]));
  const censored = buildRunEvaluationEnvelopeV1(envelopeCore({ cutoffKind: "right_censored", cutoffClass: "checkpoint", checkpointIdentityHash: I("status-checkpoint") }));
  assert.deepEqual(new Set(timingStatuses(censored)), new Set(["censored"]));

  const serial = buildRunEvaluationEnvelopeV1(envelopeCore({ serialPolicy: true }));
  assert.equal(serial.metrics.usefulParallelism.parallelOpportunityArea.status, "unsupported_policy");
  assert.equal(serial.metrics.usefulParallelism.opportunityCapture.status, "unsupported_policy");
  assert.equal(serial.metrics.usefulParallelism.usefulWork.status, "measured");
  const zeroMetrics = metricSections();
  zeroMetrics.usefulParallelism.usefulWork = measuredMetric("milliseconds", 3_000);
  zeroMetrics.usefulParallelism.usefulAverageConcurrency = measuredMetric("ratio", 3_000, 3_000);
  zeroMetrics.usefulParallelism.usefulOverlapArea = measuredMetric("milliseconds", 0);
  zeroMetrics.usefulParallelism.parallelOpportunityArea = measuredMetric("milliseconds", 0);
  zeroMetrics.usefulParallelism.opportunityCapture = measuredMetric("ratio", 0, 0);
  zeroMetrics.usefulParallelism.workEfficiency = measuredMetric("ratio", 3_000, 5_000);
  const zero = buildRunEvaluationEnvelopeV1(envelopeCore({ metrics: zeroMetrics }));
  assert.equal(zero.metrics.usefulParallelism.opportunityCapture.status, "zero_exposure");

  const forged = clone(partial);
  forged.metrics.timing.autonomousElapsed = measuredMetric("milliseconds", 3_000, null, 3);
  delete forged.envelopeHash;
  forged.envelopeHash = canonicalHash(forged);
  assert.equal(validateRunEvaluationEnvelopeV1(forged).ok, false);
});

test("creditability derives only from exact canonical lineage or current finding disposition facts", () => {
  const context = clone(CREDIT_CONTEXT);
  assert.deepEqual(deriveCreditBasisV1(clone(ACCEPTED_BASIS), context), ACCEPTED_BASIS);
  assert.equal(deriveCreditBasisV1({ ...clone(ACCEPTED_BASIS), acceptedEvidenceHash: H("f") }, context), null);
  assert.deepEqual(deriveCreditBasisV1({ kind: "actionable_finding_disposition", findingHash: H("5"), currentDispositionHash: H("6") }, context), { kind: "actionable_finding_disposition", findingHash: H("5"), currentDispositionHash: H("6") });
  assert.equal(deriveCreditBasisV1({ kind: "actionable_finding_disposition", findingHash: H("5"), currentDispositionHash: H("7") }, context), null);
  assert.throws(() => foldAccumulatorObservationV1(initialAccumulator(I("forged-credit")), observation(1, 1, { active: [{ operationHash: H("a"), creditBasis: { ...clone(ACCEPTED_BASIS), acceptedEvidenceHash: H("f") } }] })), /exact accepted evidence\/integration lineage/);
});

test("credit context binds lineage tuples, unique current dispositions, and persisted open bases", () => {
  const crossContext = { acceptedIntegrationLineages: [
    { acceptedEvidenceHash: H("1"), integrationReceiptHash: H("3") },
    { acceptedEvidenceHash: H("2"), integrationReceiptHash: H("4") },
  ], actionableFindingDispositions: [] };
  assert.equal(deriveCreditBasisV1({ kind: "accepted_integration_lineage", acceptedEvidenceHash: H("1"), integrationReceiptHash: H("4") }, crossContext), null);
  const duplicateCurrent = { acceptedIntegrationLineages: [], actionableFindingDispositions: [
    { findingHash: H("5"), currentDispositionHash: H("6") },
    { findingHash: H("5"), currentDispositionHash: H("7") },
  ] };
  assert.throws(() => deriveCreditBasisV1({ kind: "actionable_finding_disposition", findingHash: H("5"), currentDispositionHash: H("6") }, duplicateCurrent), /one current disposition/);
  const forged = clone(initialAccumulator(I("forged-open-credit")));
  forged.open.active = [{ intervalHash: H("8"), class: "operation", sourceRevision: 0, sourceSnapshotHash: H("0"), clockEpochHash: H("c"), openedAtMonotonicMs: 0, creditBasis: { kind: "accepted_integration_lineage", acceptedEvidenceHash: H("e"), integrationReceiptHash: H("f") } }];
  delete forged.accumulatorHash;
  forged.accumulatorHash = canonicalHash(forged);
  assert.equal(validateRunObservationAccumulatorV1(forged).ok, false);
  assert.throws(() => foldAccumulatorObservationV1(forged, observation(1, 100)), /committed canonical credit context/);
});

test("completed-run v1 replay fixes the final committed credit context and direct folds cannot self-authorize transitions", () => {
  const runIdentityHash = I("completed-final-credit-context");
  const replay = () => {
    let accumulator = initialAccumulator(runIdentityHash);
    accumulator = foldAccumulatorObservationV1(accumulator, observation(1, 0, { active: [{ operationHash: H("1"), creditBasis: clone(ACCEPTED_BASIS) }] }));
    return foldAccumulatorObservationV1(accumulator, observation(2, 10));
  };
  const completed = replay();
  const replayed = replay();
  assert.equal(canonicalStringify(replayed), canonicalStringify(completed));
  assert.equal(canonicalStringify(completed.creditContext), canonicalStringify(CREDIT_CONTEXT));
  assert.equal(completed.creditContextHash, canonicalHash(CREDIT_CONTEXT));
  assert.equal(completed.integrals.creditableWorkMs, 10);

  const before = canonicalStringify(completed);
  const transition = observation(3, 20, { active: [{ operationHash: H("e"), creditBasis: clone(TRANSITION_BASIS) }], creditContext: TRANSITION_CREDIT_CONTEXT });
  assert.throws(() => foldAccumulatorObservationV1(completed, transition), /immutable v1 credit context/);
  assert.equal(canonicalStringify(completed), before);
  assert.equal(completed.integrals.creditableWorkMs, 10);
});

// Hand-computed half-open interval sweep: close/open at one sample never overlaps spuriously.
test("lossy accumulator folds intervals and computes accepted formulas", () => {
  const opA = H("1"), opB = H("2"), opC = H("3");
  let acc = initialAccumulator();
  acc = foldAccumulatorObservationV1(acc, observation(1, 0, { ready: [opA], active: [{ operationHash: opB, creditBasis: clone(ACCEPTED_BASIS) }] }));
  acc = foldAccumulatorObservationV1(acc, observation(2, 1_000, { active: [{ operationHash: opB, creditBasis: clone(ACCEPTED_BASIS) }, { operationHash: opC, creditBasis: clone(ACCEPTED_BASIS) }] }));
  acc = foldAccumulatorObservationV1(acc, observation(3, 3_000));
  assert.deepEqual(acc.integrals, { autonomousElapsedMs: 3_000, allOperationWorkMs: 5_000, creditableWorkMs: 5_000, usefulOverlapMs: 2_000, parallelOpportunityMs: 3_000 });
  assert.equal(acc.sums.readinessWaitMs, 1_000);
  assert.deepEqual(acc.histograms.readinessWait, [0, 1, 0, 0, 0, 0]);
  assert.deepEqual(acc.open, { readiness: [], active: [], human: [], recovery: [] });
  assert.equal("events" in acc, false);
  assert.equal("history" in acc, false);
  const metrics = usefulParallelismMetricsV1(acc, { serialPolicy: false, rightCensored: false });
  assert.deepEqual([metrics.opportunityCapture.numerator, metrics.opportunityCapture.denominator], [2_000, 3_000]);
  assert.deepEqual([metrics.usefulAverageConcurrency.numerator, metrics.usefulAverageConcurrency.denominator], [5_000, 3_000]);
  assert.deepEqual([metrics.workEfficiency.numerator, metrics.workEfficiency.denominator], [5_000, 5_000]);
  assert.equal(usefulParallelismMetricsV1(acc, { serialPolicy: true, rightCensored: false }).opportunityCapture.status, "unsupported_policy");
  assert.equal(validateRunObservationAccumulatorV1(acc).ok, true);
});

test("human and authority intervals close then reopen when the class changes", () => {
  const subject = H("9");
  let acc = initialAccumulator(I("human-transition"));
  acc = foldAccumulatorObservationV1(acc, observation(1, 0, { human: [subject] }));
  acc = foldAccumulatorObservationV1(acc, observation(2, 10, { authority: [subject] }));
  assert.equal(acc.open.human[0].class, "authority_wait");
  assert.equal(acc.open.human[0].openedAtMonotonicMs, 10);
  acc = foldAccumulatorObservationV1(acc, observation(3, 20));
  assert.deepEqual({ human: acc.sums.humanActiveMs, authority: acc.sums.authorityWaitMs }, { human: 10, authority: 10 });
  assert.deepEqual({ human: acc.counters.humanActiveIntervals, authority: acc.counters.authorityWaitIntervals }, { human: 1, authority: 1 });
});

test("pure interval sweep credits accepted work and applies half-open equal endpoints", () => {
  const totals = sweepQualityConditionedIntervalsV1([
    { intervalHash: H("1"), startMonotonicMs: 0, endMonotonicMs: 10, creditBasis: clone(ACCEPTED_BASIS) },
    { intervalHash: H("2"), startMonotonicMs: 5, endMonotonicMs: 15, creditBasis: clone(ACCEPTED_BASIS) },
    { intervalHash: H("3"), startMonotonicMs: 0, endMonotonicMs: 4, creditBasis: null },
  ], [
    { intervalHash: H("4"), startMonotonicMs: 0, endMonotonicMs: 5 },
    { intervalHash: H("5"), startMonotonicMs: 5, endMonotonicMs: 15 },
  ], CREDIT_CONTEXT);
  assert.deepEqual(totals, { autonomousElapsedMs: 15, allOperationWorkMs: 24, creditableWorkMs: 20, usefulOverlapMs: 5, parallelOpportunityMs: 24 });
});

test("coverage gaps, clock changes, censoring, and zero exposure remain explicit", () => {
  const op = H("1");
  let acc = initialAccumulator();
  acc = foldAccumulatorObservationV1(acc, observation(1, 1, { active: [{ operationHash: op, creditBasis: clone(ACCEPTED_BASIS) }] }));
  acc = foldAccumulatorObservationV1(acc, observation(3, 2, { active: [{ operationHash: op, creditBasis: clone(ACCEPTED_BASIS) }], droppedBefore: 1 }));
  assert.equal(acc.coverage.missingRevisionCount, 1);
  assert.equal(acc.coverage.droppedRevisionCount, 1);
  assert.equal(usefulParallelismMetricsV1(acc, { serialPolicy: false, rightCensored: false }).usefulWork.status, "censored");
  assert.equal(acc.open.active[0].openedAtMonotonicMs, 2);
  acc = foldAccumulatorObservationV1(acc, observation(4, 0, { epoch: H("d") }));
  assert.equal(acc.coverage.censoredIntervalCount, 2);
  assert.equal(acc.open.active.length, 0);
  assert.equal(usefulParallelismMetricsV1(acc, { serialPolicy: false, rightCensored: false }).usefulWork.status, "censored");
  const empty = initialAccumulator(I("empty"));
  assert.equal(usefulParallelismMetricsV1(empty, { serialPolicy: false, rightCensored: false }).opportunityCapture.status, "not_observed");
});

test("revision gaps censor at the last boundary and reopen every current state without imputing gap time", () => {
  const ready = H("1"), reserved = H("2"), active = H("3"), human = H("4"), authority = H("7"), recovery = H("8");
  const state = { ready: [ready], reserved: [reserved], active: [{ operationHash: active, creditBasis: clone(ACCEPTED_BASIS) }], human: [human], authority: [authority], recovery: [recovery] };
  let acc = initialAccumulator(I("exact-gap-reopen"));
  acc = foldAccumulatorObservationV1(acc, observation(1, 100, state));
  acc = foldAccumulatorObservationV1(acc, observation(2, 200, state));
  acc = foldAccumulatorObservationV1(acc, observation(4, 1_000, state));
  assert.deepEqual(acc.integrals, { autonomousElapsedMs: 200, allOperationWorkMs: 100, creditableWorkMs: 100, usefulOverlapMs: 0, parallelOpportunityMs: 200 });
  assert.deepEqual(acc.coverage.revisionGaps, [{ firstRevision: 3, lastRevision: 3 }]);
  assert.equal(acc.coverage.censoredIntervalCount, 6);
  assert.deepEqual(Object.fromEntries(Object.entries(acc.open).map(([name, intervals]) => [name, intervals.map(({ openedAtMonotonicMs }) => openedAtMonotonicMs)])), { readiness: [1_000], active: [1_000, 1_000], human: [1_000, 1_000], recovery: [1_000] });
  assert.deepEqual({ readiness: acc.sums.readinessWaitMs, dispatch: acc.sums.dispatchWaitMs, human: acc.sums.humanActiveMs, authority: acc.sums.authorityWaitMs, recovery: acc.sums.recoveryMs }, { readiness: 100, dispatch: 100, human: 100, authority: 100, recovery: 100 });
  acc = foldAccumulatorObservationV1(acc, observation(5, 1_100));
  assert.deepEqual(acc.integrals, { autonomousElapsedMs: 300, allOperationWorkMs: 200, creditableWorkMs: 200, usefulOverlapMs: 0, parallelOpportunityMs: 400 });
  assert.deepEqual({ readiness: acc.sums.readinessWaitMs, dispatch: acc.sums.dispatchWaitMs, human: acc.sums.humanActiveMs, authority: acc.sums.authorityWaitMs, recovery: acc.sums.recoveryMs }, { readiness: 200, dispatch: 200, human: 200, authority: 200, recovery: 200 });
});

test("clock epoch changes censor and immediately reopen exact current state for future epoch time", () => {
  const operationHash = H("1"), recoveryHash = H("2");
  let acc = initialAccumulator(I("epoch-reopen"));
  acc = foldAccumulatorObservationV1(acc, observation(1, 10, { active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }], recovery: [recoveryHash] }));
  acc = foldAccumulatorObservationV1(acc, observation(2, 20, { epoch: H("d"), active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }], recovery: [recoveryHash] }));
  assert.equal(acc.coverage.missingRevisionCount, 0);
  assert.equal(acc.coverage.censoredIntervalCount, 2);
  assert.deepEqual(acc.open.active.map(({ clockEpochHash, openedAtMonotonicMs }) => [clockEpochHash, openedAtMonotonicMs]), [[H("d"), 20]]);
  acc = foldAccumulatorObservationV1(acc, observation(3, 30, { epoch: H("d") }));
  assert.deepEqual(acc.integrals, { autonomousElapsedMs: 20, allOperationWorkMs: 10, creditableWorkMs: 10, usefulOverlapMs: 0, parallelOpportunityMs: 0 });
  assert.equal(acc.sums.recoveryMs, 10);
});

test("gap and epoch discontinuities reject incident waste that cannot be integrated exactly", () => {
  const operationHash = H("1");
  let gap = foldAccumulatorObservationV1(initialAccumulator(I("gap-incident")), observation(1, 10, { active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }] }));
  const incident = { findingHash: H("5"), currentDispositionHash: H("6"), operationHashes: [operationHash], wasteStartMonotonicMs: 10 };
  assert.throws(() => foldAccumulatorObservationV1(gap, observation(3, 30, { active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }], incidents: [incident] })), /exact contiguous same-clock fold/);
  assert.throws(() => foldAccumulatorObservationV1(gap, observation(2, 0, { epoch: H("d"), active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }], incidents: [incident] })), /exact contiguous same-clock fold/);
});

test("envelope validator enforces exact field units and status domains", () => {
  const unitForgery = clone(buildRunEvaluationEnvelopeV1(envelopeCore()));
  unitForgery.metrics.outcomes.accepted.unit = "token";
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(unitForgery)).ok, false);
  const statusForgery = clone(buildRunEvaluationEnvelopeV1(envelopeCore()));
  statusForgery.metrics.outcomes.accepted = unavailableMetric("unsupported_policy", "count");
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(statusForgery)).ok, false);
});

test("envelope validator enforces count bounds and every useful-parallelism formula", () => {
  const mutations = [
    (value) => { value.metrics.outcomes.accepted.numerator = 10; },
    (value) => { value.metrics.outcomes.integrated.numerator = 3; },
    (value) => { value.metrics.attempts.retries.numerator = 10; },
    (value) => { value.metrics.findings.disposed.numerator = 2; },
    (value) => { value.metrics.usefulParallelism.usefulWork.numerator = 5_001; },
    (value) => { value.metrics.usefulParallelism.usefulAverageConcurrency.numerator = 4_999; },
    (value) => { value.metrics.usefulParallelism.usefulOverlapArea.numerator = 3_001; },
    (value) => { value.metrics.usefulParallelism.opportunityCapture.numerator = 3_001; },
    (value) => { value.metrics.usefulParallelism.workEfficiency.denominator = 4_999; },
  ];
  const valid = buildRunEvaluationEnvelopeV1(envelopeCore());
  for (const mutate of mutations) {
    const forged = clone(valid);
    mutate(forged);
    assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(forged)).ok, false);
  }
});

test("coverage validation closes source revisions and exact instrumentation counts", () => {
  const valid = buildRunEvaluationEnvelopeV1(envelopeCore());
  const sourceClosureForgery = clone(valid);
  sourceClosureForgery.coverage.sourceRevisionCount += 1;
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(sourceClosureForgery)).ok, false);
  const arithmeticForgery = clone(valid);
  arithmeticForgery.coverage.observedRevisionCount = 5;
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(arithmeticForgery)).ok, false);
  const instrumentationForgery = clone(valid);
  instrumentationForgery.metrics.instrumentation.observedRevisions.numerator = 2;
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(instrumentationForgery)).ok, false);
});

test("accumulator validator rejects impossible coverage and integral bounds even when self-hashed", () => {
  const observedForgery = clone(foldAccumulatorObservationV1(initialAccumulator(I("coverage-bound")), observation(1, 1)));
  observedForgery.coverage.observedRevisionCount = 2;
  delete observedForgery.accumulatorHash;
  observedForgery.accumulatorHash = canonicalHash(observedForgery);
  assert.equal(validateRunObservationAccumulatorV1(observedForgery).ok, false);
  const integralForgery = clone(initialAccumulator(I("integral-bound")));
  integralForgery.integrals.autonomousElapsedMs = 1;
  integralForgery.integrals.allOperationWorkMs = 1;
  integralForgery.integrals.creditableWorkMs = 2;
  delete integralForgery.accumulatorHash;
  integralForgery.accumulatorHash = canonicalHash(integralForgery);
  assert.equal(validateRunObservationAccumulatorV1(integralForgery).ok, false);
});

test("combined observation maxima fit bounded open maps and false-independence waste uses exact fold inputs", () => {
  const hashes = (kind, count) => Array.from({ length: count }, (_, index) => canonicalHash({ kind, index })).sort();
  const reserved = hashes("reserved", 512);
  const activeHashes = hashes("active", 512);
  const human = hashes("human", 64);
  const authority = hashes("authority", 64);
  let acc = initialAccumulator(I("bounded-run"));
  acc = foldAccumulatorObservationV1(acc, observation(1, 0, {
    reserved,
    active: activeHashes.map((operationHash) => ({ operationHash, creditBasis: null })),
    human,
    authority,
  }));
  assert.equal(acc.open.active.length, 1024);
  assert.equal(acc.open.human.length, 128);
  const operationHash = activeHashes[0];
  acc = foldAccumulatorObservationV1(acc, observation(2, 100, {
    active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }],
    incidents: [{ findingHash: H("5"), currentDispositionHash: H("6"), operationHashes: [operationHash], wasteStartMonotonicMs: 25 }],
  }));
  assert.equal(acc.counters.falseIndependenceIncidents, 1);
  assert.equal(acc.sums.falseIndependenceWasteMs, 75);
  const forged = observation(3, 200, { incidents: [{ findingHash: H("5"), currentDispositionHash: H("6"), operationHashes: [H("f")], wasteStartMonotonicMs: 100 }] });
  assert.throws(() => foldAccumulatorObservationV1(acc, forged), /exact open operation fold input/);
});

test("strict RFC3339 UTC civil timestamps preserve exact nanosecond ordering", () => {
  const whole = parseRfc3339UtcNanosecondsV1("2026-01-01T00:00:00Z");
  const oneNanosecond = parseRfc3339UtcNanosecondsV1("2026-01-01T00:00:00.000000001Z");
  assert.equal(oneNanosecond - whole, 1n);
  assert.equal(parseRfc3339UtcNanosecondsV1("2024-02-29T23:59:59.123456789Z") !== null, true);
  for (const invalid of ["2026-02-29T00:00:00Z", "2026-02-30T00:00:00Z", "2026-04-31T00:00:00Z", "2026-01-01T24:00:00Z", "2026-01-01T00:00:60Z", "2026-01-01T00:00:00.0000000000Z", "2026-01-01t00:00:00z"]) assert.equal(parseRfc3339UtcNanosecondsV1(invalid), null, invalid);
  const impossible = envelopeCore({ cutoffAt: "2026-02-30T00:00:00Z" });
  assert.throws(() => buildRunEvaluationEnvelopeV1(impossible), /real UTC RFC 3339 civil timestamp/);
});

test("telemetry schemas exclude paths, prose, transcripts, source, and unrestricted payloads", () => {
  const marker = "SECRET_PROMPT_TRANSCRIPT_/absolute/private/path";
  const envelope = buildRunEvaluationEnvelopeV1(envelopeCore());
  const bytes = canonicalStringify(envelope);
  for (const forbidden of ["path", "prose", "prompt", "transcript", "diagnostic", "locator", "artifact", "sourceText", marker]) assert.equal(bytes.toLowerCase().includes(forbidden.toLowerCase()), false, forbidden);
  const polluted = { ...envelope, reportText: marker };
  assert.equal(validateRunEvaluationEnvelopeV1(polluted).ok, false);
  const accumulator = initialAccumulator();
  assert.equal(canonicalStringify(accumulator).includes(marker), false);
  assert.throws(() => initialAccumulator(marker), /runIdentityHash/);
  assert.throws(() => buildRunEvaluationEnvelopeV1(envelopeCore({ projectIdentityHash: marker })), /projectIdentityHash/);
  const pollutedPlan = envelopeCore();
  pollutedPlan.identity.planHash = marker;
  assert.throws(() => buildRunEvaluationEnvelopeV1(pollutedPlan), /planHash/);
  const pollutedOperation = envelopeCore();
  pollutedOperation.attribution.creditedOperations[0].operationHash = marker;
  assert.throws(() => buildRunEvaluationEnvelopeV1(pollutedOperation), /operationHash/);
  const pollutedFinding = envelopeCore();
  pollutedFinding.creditContext.actionableFindingDispositions[0].findingHash = marker;
  assert.throws(() => buildRunEvaluationEnvelopeV1(pollutedFinding), /findingHash/);
  const pollutedAllowedField = envelopeCore();
  pollutedAllowedField.cutoff.checkpointIdentityHash = marker;
  pollutedAllowedField.cutoff.kind = "right_censored";
  pollutedAllowedField.cutoff.class = "checkpoint";
  assert.throws(() => buildRunEvaluationEnvelopeV1(pollutedAllowedField), /checkpointIdentityHash/);
});

test("metric-family source bindings are exact, non-empty, sorted, closed, and model usage is worker-result bound", () => {
  const envelope = buildRunEvaluationEnvelopeV1(envelopeCore());
  assert.deepEqual(envelope.metricSourceBindings.modelUsage.sourceHashes, envelope.sourceHashes.workerResults);
  assert.deepEqual(envelope.metricSourceBindings.attempts.sourceHashes, envelope.sourceHashes.workerResults);
  assert.deepEqual(envelope.metricSourceBindings.findings.sourceHashes, [...new Set([...envelope.sourceHashes.findingsAndResolutions, envelope.source.accumulatorHash])].sort());
  for (const binding of Object.values(envelope.metricSourceBindings)) {
    assert.ok(binding.sourceHashes.length > 0);
    assert.deepEqual(binding.sourceHashes, [...new Set(binding.sourceHashes)].sort());
    assert.deepEqual(binding.sourceClosure, sourceClosureV1(binding.sourceHashes));
  }
  const forged = clone(envelope);
  forged.metricSourceBindings.modelUsage = { sourceHashes: [H("f")], sourceClosure: sourceClosureV1([H("f")]) };
  assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(forged)).ok, false);
  assert.throws(() => buildRunEvaluationEnvelopeV1({ ...envelopeCore(), metricSourceBindings: forged.metricSourceBindings }), /Caller-supplied metric source binding claims are unsupported/);
  const missingWorkerSources = envelopeCore();
  missingWorkerSources.sourceHashes.workerResults = [];
  assert.throws(() => buildRunEvaluationEnvelopeV1(missingWorkerSources), /cannot be empty/);
});

test("clock history records no-open-interval epoch changes and forces mixed-epoch quality", () => {
  let accumulator = initialAccumulator(I("no-open-clock-change"));
  accumulator = foldAccumulatorObservationV1(accumulator, observation(1, 10));
  accumulator = foldAccumulatorObservationV1(accumulator, observation(2, 20, { epoch: H("d") }));
  assert.deepEqual(accumulator.clockHistory, { initialEpochHash: H("c"), currentEpochHash: H("d"), epochChangeCount: 1, monotonicResetCount: 0, unsupportedObservationCount: 0 });
  assert.deepEqual(accumulatorClockV1(accumulator), { quality: "mixed_epoch", clockEpochHash: null });
  assert.equal(accumulator.coverage.censoredIntervalCount, 0);
  const metrics = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: false, rightCensored: false });
  assert.equal(metrics.timing.autonomousElapsed.status, "censored");
});

test("clock history persists monotonic resets and unsupported observations independently", () => {
  let accumulator = foldAccumulatorObservationV1(initialAccumulator(I("clock-reset")), observation(1, 100));
  accumulator = foldAccumulatorObservationV1(accumulator, observation(2, 50));
  assert.equal(accumulator.clockHistory.monotonicResetCount, 1);
  assert.equal(accumulatorClockV1(accumulator).quality, "mixed_epoch");
  accumulator = foldAccumulatorObservationV1(accumulator, { ...observation(3, 60), clockStatus: "unsupported" });
  assert.equal(accumulator.clockHistory.unsupportedObservationCount, 1);
  assert.deepEqual(accumulatorClockV1(accumulator), { quality: "unsupported", clockEpochHash: null });
  const projection = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: false, rightCensored: false });
  assert.equal(projection.timing.autonomousElapsed.status, "unsupported_policy");
  assert.deepEqual(projection.accumulatorTelemetry.integrals, accumulator.integrals);
  assert.deepEqual(projection.accumulatorTelemetry.coverage, accumulator.coverage);
  assert.equal(validateRunObservationAccumulatorV1(accumulator).ok, true);
});

test("accumulator projection publishes every wait, recovery, human, incident, integral, histogram, and coverage value", () => {
  const ready = H("1"), reserved = H("2"), operationHash = H("3"), human = H("7"), authority = H("8"), recovery = H("a");
  let accumulator = initialAccumulator(I("complete-projection"));
  accumulator = foldAccumulatorObservationV1(accumulator, observation(1, 0, { ready: [ready], reserved: [reserved], active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }], human: [human], authority: [authority], recovery: [recovery] }));
  accumulator = foldAccumulatorObservationV1(accumulator, observation(2, 1_500, {
    active: [{ operationHash, creditBasis: clone(ACCEPTED_BASIS) }],
    incidents: [{ findingHash: H("5"), currentDispositionHash: H("6"), operationHashes: [operationHash], wasteStartMonotonicMs: 1_000 }],
  }));
  const projection = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: false, rightCensored: false });
  assert.deepEqual([projection.timing.readinessWait.numerator, projection.timing.readinessWaitIntervals.numerator], [1_500, 1]);
  assert.deepEqual([projection.timing.dispatchWait.numerator, projection.timing.dispatchWaitIntervals.numerator], [1_500, 1]);
  assert.deepEqual([projection.timing.recovery.numerator, projection.timing.recoveryIntervals.numerator], [1_500, 1]);
  assert.deepEqual(projection.waitHistograms.readinessWait.map(({ numerator }) => numerator), [0, 1, 0, 0, 0, 0]);
  assert.deepEqual([projection.humanAttention.activeMinutes.numerator, projection.humanAttention.activeIntervals.numerator, projection.humanAttention.authorityWait.numerator, projection.humanAttention.authorityWaitIntervals.numerator], [1_500, 1, 1_500, 1]);
  assert.deepEqual([projection.findings.falseIndependenceIncidents.numerator, projection.findings.falseIndependenceWaste.numerator], [1, 500]);
  assert.equal(projection.usefulParallelism.usefulWork.numerator, accumulator.integrals.creditableWorkMs);
  assert.equal(projection.instrumentation.observedRevisions.numerator, accumulator.coverage.observedRevisionCount);
  assert.deepEqual(projection.accumulatorTelemetry, {
    counters: accumulator.counters,
    sums: accumulator.sums,
    histograms: accumulator.histograms,
    integrals: accumulator.integrals,
    coverage: accumulator.coverage,
  });
});

test("store publication rejects forged non-parallel accumulator telemetry", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-forged-wait-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const envelope = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash: I("forged-wait") }));
    const forged = clone(envelope);
    forged.metrics.timing.readinessWait.numerator = 123;
    assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(forged)).ok, true);
    await assert.rejects(store.publishEnvelope(forged, NOW), /accumulator-derived metrics/);
    const forgedRaw = clone(envelope);
    forgedRaw.metrics.accumulatorTelemetry.sums.readinessWaitMs = 456;
    assert.equal(validateRunEvaluationEnvelopeV1(resealEnvelope(forgedRaw)).ok, true);
    await assert.rejects(store.publishEnvelope(forgedRaw, NOW), /accumulator-derived metrics/);
    assert.equal((await store.readIndex()).entries.length, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("store derives mixed clock quality from history even when no interval was open", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-forged-clock-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("forged-clock-history");
    let accumulator = initialAccumulator(runIdentityHash, I("project-1"));
    accumulator = foldAccumulatorObservationV1(accumulator, observation(1, 10));
    accumulator = foldAccumulatorObservationV1(accumulator, observation(2, 20, { epoch: H("d") }));
    await store.writeAccumulator(accumulator, null);
    const valid = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash, sourceRevision: 2, sourceSnapshotHash: H("2"), sourceAccumulatorHash: accumulator.accumulatorHash }));
    assert.equal(valid.clock.quality, "mixed_epoch");
    const forgedCore = clone(valid);
    delete forgedCore.envelopeHash;
    delete forgedCore.sourceClosures;
    delete forgedCore.metricSourceBindings;
    delete forgedCore.creditContextHash;
    forgedCore.clock = { quality: "same_epoch", clockEpochHash: H("d") };
    forgedCore.metrics.usefulParallelism.opportunityCapture = measuredMetric("ratio", 0, 0);
    forgedCore.metrics.usefulParallelism.workEfficiency = measuredMetric("ratio", 0, 0);
    const forged = buildRunEvaluationEnvelopeV1(forgedCore);
    assert.equal(validateRunEvaluationEnvelopeV1(forged).ok, true);
    await assert.rejects(store.publishEnvelope(forged, NOW), /clock history/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("observer rejects secret/path payloads before queueing and forwards only a fresh closed identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-closed-offer-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("closed-offer");
    await store.writeAccumulator(initialAccumulator(runIdentityHash), null);
    let loaded = null;
    const observer = new RunEvaluationObserverV1(store, async (offered) => {
      loaded = offered;
      return observation(offered.revision, offered.revision, { snapshotHash: offered.snapshotHash });
    });
    const secret = { runIdentityHash, revision: 1, snapshotHash: H("1"), secretPromptTranscript: "SECRET_/absolute/private/path" };
    assert.throws(() => observer.offerCommittedSnapshot(secret), /Invalid committed snapshot offer/);
    assert.equal(loaded, null);
    const inherited = Object.create({ secretPromptTranscript: "SECRET_/absolute/private/path" });
    Object.assign(inherited, { runIdentityHash, revision: 1, snapshotHash: H("1") });
    observer.offerCommittedSnapshot(inherited);
    await observer.flush();
    assert.deepEqual(Object.keys(loaded).sort(), ["revision", "runIdentityHash", "snapshotHash"]);
    assert.equal(Object.getPrototypeOf(loaded), Object.prototype);
    assert.equal("secretPromptTranscript" in loaded, false);
  } finally { await rm(root, { recursive: true, force: true }); }
});

// Atomic local storage, expected identity/CAS, failpoints, observer loss, corrections, and retention.
test("atomic accumulator store survives pre/post rename failures and rejects stale/concurrent writers", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-store-"));
  let fail = null;
  const store = new RunEvaluationStoreV1(root, { failpoint: (point) => { if (point === fail) throw new Error(`FAILPOINT:${point}`); } });
  try {
    await store.initialize();
    const initial = initialAccumulator();
    await store.writeAccumulator(initial, null);
    const next = foldAccumulatorObservationV1(initial, observation(1, 1));
    fail = "after_accumulator_temp_sync";
    await assert.rejects(store.writeAccumulator(next, initial.accumulatorHash), /FAILPOINT/);
    assert.equal((await store.readAccumulator(I("run-1"))).accumulatorHash, initial.accumulatorHash);
    fail = "after_accumulator_rename";
    await assert.rejects(store.writeAccumulator(next, initial.accumulatorHash), /FAILPOINT/);
    assert.equal((await store.readAccumulator(I("run-1"))).accumulatorHash, next.accumulatorHash);
    fail = null;
    await assert.rejects(store.writeAccumulator(initial, initial.accumulatorHash), RunEvaluationStoreConflictError);

    const newerA = foldAccumulatorObservationV1(next, observation(2, 2));
    const newerB = foldAccumulatorObservationV1(next, observation(3, 3));
    const otherStore = new RunEvaluationStoreV1(root);
    const results = await Promise.allSettled([store.writeAccumulator(newerA, next.accumulatorHash), otherStore.writeAccumulator(newerB, next.accumulatorHash)]);
    assert.equal(results.filter(({ status }) => status === "fulfilled").length, 1);
    assert.equal(results.filter(({ status, reason }) => status === "rejected" && (reason instanceof RunEvaluationStoreBusyError || reason instanceof RunEvaluationStoreConflictError)).length, 1);
    assert.ok([newerA.accumulatorHash, newerB.accumulatorHash].includes((await store.readAccumulator(I("run-1"))).accumulatorHash));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("accumulator CAS binds the complete immutable schema/profile/project/run/nonce/plan/clock-policy/credit-context tuple", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-identity-cas-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const initial = initialAccumulator(I("identity-run"), I("project-a"));
    await store.writeAccumulator(initial, null);
    const forged = clone(initial);
    forged.identity.projectIdentityHash = I("project-b");
    forged.identity.planHash = H("e");
    delete forged.accumulatorHash;
    forged.accumulatorHash = canonicalHash(forged);
    assert.equal(validateRunObservationAccumulatorV1(forged).ok, true);
    await assert.rejects(store.writeAccumulator(forged, initial.accumulatorHash), RunEvaluationStoreConflictError);
    const forgedCreditContext = clone(initial);
    forgedCreditContext.creditContext = { acceptedIntegrationLineages: [{ acceptedEvidenceHash: H("e"), integrationReceiptHash: H("f") }], actionableFindingDispositions: [] };
    forgedCreditContext.creditContextHash = canonicalHash(forgedCreditContext.creditContext);
    forgedCreditContext.open.active = [{ intervalHash: H("8"), class: "operation", sourceRevision: 0, sourceSnapshotHash: H("0"), clockEpochHash: H("c"), openedAtMonotonicMs: 0, creditBasis: { kind: "accepted_integration_lineage", acceptedEvidenceHash: H("e"), integrationReceiptHash: H("f") } }];
    delete forgedCreditContext.accumulatorHash;
    forgedCreditContext.accumulatorHash = canonicalHash(forgedCreditContext);
    assert.equal(validateRunObservationAccumulatorV1(forgedCreditContext).ok, true);
    await assert.rejects(store.writeAccumulator(forgedCreditContext, initial.accumulatorHash), RunEvaluationStoreConflictError);
    assert.equal((await store.readAccumulator(I("identity-run"))).identity.projectIdentityHash, I("project-a"));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("store fold rejects a future credit-context transition before useful credit or accumulator writes", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-store-credit-transition-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("store-credit-transition");
    const initial = initialAccumulator(runIdentityHash);
    await store.writeAccumulator(initial, null);
    await store.foldObservation(runIdentityHash, observation(1, 0, { active: [{ operationHash: H("1"), creditBasis: clone(ACCEPTED_BASIS) }] }));
    const before = await store.readAccumulator(runIdentityHash);
    const transition = observation(2, 100, { active: [{ operationHash: H("e"), creditBasis: clone(TRANSITION_BASIS) }], creditContext: TRANSITION_CREDIT_CONTEXT });
    await assert.rejects(store.foldObservation(runIdentityHash, transition), /immutable v1 credit context/);
    const after = await store.readAccumulator(runIdentityHash);
    assert.equal(after.accumulatorHash, before.accumulatorHash);
    assert.equal(canonicalStringify(after.creditContext), canonicalStringify(CREDIT_CONTEXT));
    assert.equal(after.creditContextHash, canonicalHash(CREDIT_CONTEXT));
    assert.equal(after.integrals.creditableWorkMs, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("observer is synchronous, bounded, lossy, identity-exact, and never propagates failures", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const initial = initialAccumulator(I("observer-run"));
    const identity = observerIdentity(initial);
    await store.writeAccumulator(initial, null);
    let releaseFirst;
    const firstGate = new Promise((resolve) => { releaseFirst = resolve; });
    const observer = new RunEvaluationObserverV1(store, identity, async (offered) => {
      if (offered.revision === 1) await firstGate;
      return observation(offered.revision, offered.revision, { snapshotHash: offered.snapshotHash });
    });
    assert.equal(observer.offerCommittedSnapshot(committed(identity, 1)), undefined);
    observer.offerCommittedSnapshot(committed(identity, 2));
    observer.offerCommittedSnapshot(committed(identity, 3));
    releaseFirst();
    await observer.flush();
    const accumulated = await store.readAccumulator(I("observer-run"));
    assert.equal(accumulated.source.revision, 3);
    assert.equal(accumulated.coverage.droppedRevisionCount, 1);
    assert.equal(accumulated.coverage.missingRevisionCount, 1);

    const failing = new RunEvaluationObserverV1(store, identity, async (offered) => {
      if (offered.revision === 4) throw new Error("observer read failed");
      return observation(offered.revision, offered.revision, { snapshotHash: offered.snapshotHash });
    });
    assert.doesNotThrow(() => failing.offerCommittedSnapshot(committed(identity, 4)));
    await failing.flush();
    assert.equal((await store.readAccumulator(I("observer-run"))).source.revision, 3);
    failing.offerCommittedSnapshot(committed(identity, 5));
    await failing.flush();
    assert.equal((await store.readAccumulator(I("observer-run"))).coverage.observerFailureCount, 1);
    assert.throws(() => failing.offerCommittedSnapshot({ ...committed(identity, 6), runIdentityHash: I("wrong-run") }), /immutable run\/evaluation identity/);
    assert.throws(() => failing.offerCommittedSnapshot({ ...committed(identity, 6), projectIdentityHash: I("wrong-project") }), /immutable run\/evaluation identity/);
    assert.throws(() => failing.offerCommittedSnapshot({ runIdentityHash: "../../escape", revision: 6, snapshotHash: H("6") }), /runIdentityHash/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("bound observer rejects forged-lineage contexts per run without poisoning its credit-context binding", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-forged-lineage-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("observer-forged-lineage"), otherRunIdentityHash = I("observer-forged-lineage-other");
    const initial = initialAccumulator(runIdentityHash);
    const otherInitial = initialAccumulator(otherRunIdentityHash);
    const identity = observerIdentity(initial);
    await store.writeAccumulator(initial, null);
    await store.writeAccumulator(otherInitial, null);
    const observer = new RunEvaluationObserverV1(store, identity, async (offered) => observation(offered.revision, offered.revision * 10, {
      snapshotHash: offered.snapshotHash,
      active: offered.revision === 1 ? [{ operationHash: H("e"), creditBasis: clone(TRANSITION_BASIS) }] : [],
      creditContext: offered.revision === 1 ? TRANSITION_CREDIT_CONTEXT : CREDIT_CONTEXT,
    }));
    assert.throws(() => observer.offerCommittedSnapshot({ ...committed(identity, 1), creditContextHash: canonicalHash(TRANSITION_CREDIT_CONTEXT) }), /immutable run\/evaluation identity/);
    observer.offerCommittedSnapshot(committed(identity, 1));
    await observer.flush();
    assert.equal((await store.readAccumulator(runIdentityHash)).accumulatorHash, initial.accumulatorHash);
    assert.equal((await store.readAccumulator(otherRunIdentityHash)).coverage.observerFailureCount, 0);

    observer.offerCommittedSnapshot(committed(identity, 2));
    await observer.flush();
    const recovered = await store.readAccumulator(runIdentityHash);
    assert.equal(recovered.source.revision, 2);
    assert.equal(recovered.coverage.observerFailureCount, 1);
    assert.equal(recovered.integrals.creditableWorkMs, 0);
    assert.equal(canonicalStringify(recovered.creditContext), canonicalStringify(CREDIT_CONTEXT));
    assert.equal(recovered.creditContextHash, identity.creditContextHash);
    assert.equal((await store.readAccumulator(otherRunIdentityHash)).coverage.observerFailureCount, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("observer defers a 150ms synchronous loader prefix and preserves one-running one-pending accounting", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-constant-time-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("observer-constant-time");
    await store.writeAccumulator(initialAccumulator(runIdentityHash), null);
    const loaded = [];
    const observer = new RunEvaluationObserverV1(store, (offered) => {
      loaded.push(offered.revision);
      const until = performance.now() + 150;
      while (performance.now() < until) { /* deliberately synchronous loader prefix */ }
      return Promise.resolve(observation(offered.revision, offered.revision, { snapshotHash: offered.snapshotHash }));
    });
    const started = performance.now();
    observer.offerCommittedSnapshot({ runIdentityHash, revision: 1, snapshotHash: H("1") });
    const elapsed = performance.now() - started;
    assert.ok(elapsed < 75, `offer blocked for ${elapsed}ms`);
    assert.deepEqual(loaded, []);
    observer.offerCommittedSnapshot({ runIdentityHash, revision: 2, snapshotHash: H("2") });
    await observer.flush();
    assert.deepEqual(loaded, [1, 2]);
    assert.equal((await store.readAccumulator(runIdentityHash)).source.revision, 2);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("observer loss counters survive stale concurrent folds until a later durable fold", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-race-"));
  const runIdentityHash = I("observer-race");
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const initial = initialAccumulator(runIdentityHash);
    const identity = observerIdentity(initial);
    await store.writeAccumulator(initial, null);
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const observer = new RunEvaluationObserverV1(store, identity, async (offered) => {
      if (offered.revision === 1) await gate;
      return observation(offered.revision, offered.revision, { snapshotHash: offered.snapshotHash });
    });
    observer.offerCommittedSnapshot(committed(identity, 1));
    observer.offerCommittedSnapshot(committed(identity, 2));
    observer.offerCommittedSnapshot(committed(identity, 3));
    assert.equal((await store.foldObservation(runIdentityHash, observation(1, 1, { snapshotHash: H("1") }))).folded, true);
    release();
    await observer.flush();
    let accumulated = await store.readAccumulator(runIdentityHash);
    assert.equal(accumulated.coverage.droppedRevisionCount, 1);
    assert.equal(accumulated.coverage.missingRevisionCount, 1);
    observer.offerCommittedSnapshot(committed(identity, 4));
    await observer.flush();
    accumulated = await store.readAccumulator(runIdentityHash);
    assert.equal(accumulated.coverage.droppedRevisionCount, 1);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("mixed-run observer replacement charges drops only to the replaced run", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-mixed-drop-"));
  const runA = I("observer-mixed-a"), runB = I("observer-mixed-b"), runC = I("observer-mixed-c");
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    for (const runIdentityHash of [runA, runB, runC]) await store.writeAccumulator(initialAccumulator(runIdentityHash), null);
    let releaseA;
    const gateA = new Promise((resolve) => { releaseA = resolve; });
    const observer = new RunEvaluationObserverV1(store, async (identity) => {
      if (identity.runIdentityHash === runA && identity.revision === 1) await gateA;
      return observation(identity.revision, identity.revision, { snapshotHash: identity.snapshotHash });
    });
    observer.offerCommittedSnapshot({ runIdentityHash: runA, revision: 1, snapshotHash: H("1") });
    observer.offerCommittedSnapshot({ runIdentityHash: runB, revision: 1, snapshotHash: H("1") });
    observer.offerCommittedSnapshot({ runIdentityHash: runC, revision: 1, snapshotHash: H("1") });
    releaseA();
    await observer.flush();
    assert.equal((await store.readAccumulator(runA)).coverage.droppedRevisionCount, 0);
    assert.equal((await store.readAccumulator(runC)).coverage.droppedRevisionCount, 0);
    assert.equal((await store.readAccumulator(runB)).source.revision, 0);
    observer.offerCommittedSnapshot({ runIdentityHash: runB, revision: 1, snapshotHash: H("1") });
    await observer.flush();
    assert.equal((await store.readAccumulator(runB)).coverage.droppedRevisionCount, 1);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("mixed-run observer failures remain with the failing run until its durable fold", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-observer-mixed-failure-"));
  const runA = I("observer-failure-a"), runC = I("observer-failure-c");
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    await store.writeAccumulator(initialAccumulator(runA), null);
    await store.writeAccumulator(initialAccumulator(runC), null);
    let failA = true;
    const observer = new RunEvaluationObserverV1(store, async (identity) => {
      if (identity.runIdentityHash === runA && failA) { failA = false; throw new Error("A observation failed"); }
      return observation(identity.revision, identity.revision, { snapshotHash: identity.snapshotHash });
    });
    observer.offerCommittedSnapshot({ runIdentityHash: runA, revision: 1, snapshotHash: H("1") });
    await observer.flush();
    observer.offerCommittedSnapshot({ runIdentityHash: runC, revision: 1, snapshotHash: H("1") });
    await observer.flush();
    assert.equal((await store.readAccumulator(runC)).coverage.observerFailureCount, 0);
    observer.offerCommittedSnapshot({ runIdentityHash: runA, revision: 2, snapshotHash: H("2") });
    await observer.flush();
    assert.equal((await store.readAccumulator(runA)).coverage.observerFailureCount, 1);
    assert.equal((await store.readAccumulator(runC)).coverage.observerFailureCount, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("envelope publication rejects a missing source accumulator", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-missing-source-accumulator-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const envelope = buildRunEvaluationEnvelopeV1(envelopeCore({ runIdentityHash: I("missing-source-accumulator") }));
    await assert.rejects(store.publishEnvelope(envelope, NOW), /exact source accumulator/);
    assert.equal((await store.readIndex()).entries.length, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("envelope publication rejects forged self-hashed useful metrics against the exact accumulator", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-forged-parallelism-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const envelope = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash: I("forged-parallelism") }));
    const forged = clone(envelope);
    const parallel = forged.metrics.usefulParallelism;
    parallel.usefulWork.numerator = 4_000;
    parallel.usefulAverageConcurrency.numerator = 4_000;
    parallel.usefulOverlapArea.numerator = 1_000;
    parallel.parallelOpportunityArea.numerator = 2_000;
    parallel.opportunityCapture.numerator = 1_000;
    parallel.opportunityCapture.denominator = 2_000;
    parallel.allOperationTime.numerator = 4_500;
    parallel.workEfficiency.numerator = 4_000;
    parallel.workEfficiency.denominator = 4_500;
    resealEnvelope(forged);
    assert.equal(validateRunEvaluationEnvelopeV1(forged).ok, true);
    await assert.rejects(store.publishEnvelope(forged, NOW), /exact source accumulator derivation/);
    assert.equal((await store.readIndex()).entries.length, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("envelope publication rejects pure-valid coverage not derived from the bound accumulator", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-forged-coverage-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("forged-coverage");
    const bound = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash }));
    const forged = buildRunEvaluationEnvelopeV1(envelopeCore({
      runIdentityHash,
      sourceAccumulatorHash: bound.source.accumulatorHash,
      coverage: { status: "partial_coverage", sourceRevisionCount: 4, observedRevisionCount: 3, missingRevisionCount: 0, droppedRevisionCount: 1, censoredIntervalCount: 0, observerFailureCount: 0 },
    }));
    assert.equal(validateRunEvaluationEnvelopeV1(forged).ok, true);
    await assert.rejects(store.publishEnvelope(forged, NOW), /coverage droppedRevisionCount/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("envelope/index crash points preserve one valid head and maintenance removes unindexed artifacts", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-envelope-crash-"));
  let fail = null;
  const store = new RunEvaluationStoreV1(root, { failpoint: (point) => { if (point === fail) throw new Error(`FAILPOINT:${point}`); } });
  try {
    await store.initialize();
    const original = await buildBoundEnvelope(store, envelopeCore({ projectIdentityHash: I("crash-project"), runIdentityHash: I("crash-run") }));
    fail = "after_envelope_temp_sync";
    await assert.rejects(store.publishEnvelope(original, NOW), /FAILPOINT/);
    assert.equal((await store.readIndex()).entries.length, 0);
    await assert.rejects(store.readEnvelope(original.envelopeHash));

    fail = "after_index_temp_sync";
    await assert.rejects(store.publishEnvelope(original, NOW), /FAILPOINT/);
    assert.equal((await store.readIndex()).entries.length, 0);
    assert.equal((await store.readEnvelope(original.envelopeHash)).envelopeHash, original.envelopeHash);
    fail = null;
    await store.publishEnvelope(original, NOW);

    const changedMetrics = clone(original.metrics);
    changedMetrics.attempts.attempts = countMetric(10);
    const correction = await buildBoundEnvelope(store, { ...envelopeCore({ projectIdentityHash: I("crash-project"), runIdentityHash: I("crash-run"), supersedesEnvelopeHash: original.envelopeHash, metrics: changedMetrics }), cutoff: clone(original.cutoff) });
    fail = "after_index_rename";
    await assert.rejects(store.publishEnvelope(correction, "2026-08-05T00:01:00.000Z", original.envelopeHash), /FAILPOINT/);
    assert.equal((await store.readIndex()).entries[0].envelopeHash, correction.envelopeHash);
    assert.equal((await store.readEnvelope(original.envelopeHash)).envelopeHash, original.envelopeHash);
    fail = null;
    const maintenance = await store.maintain("2026-08-05T00:02:00.000Z");
    assert.equal(maintenance.prunedEnvelopeCount, 1);
    await assert.rejects(store.readEnvelope(original.envelopeHash));
    assert.equal((await store.readEnvelope(correction.envelopeHash)).envelopeHash, correction.envelopeHash);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("publish and maintenance serialize across a child-held pre-index failpoint", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-publish-maintain-"));
  const envelopePath = join(root, "child-envelope.json");
  const readyPath = join(root, "child-publish-ready");
  const releasePath = join(root, "child-publish-release");
  const parentStore = new RunEvaluationStoreV1(root);
  await parentStore.initialize();
  const envelope = await buildBoundEnvelope(parentStore, envelopeCore({ projectIdentityHash: I("publish-maintain-project"), runIdentityHash: I("publish-maintain-run") }));
  await writeFile(envelopePath, canonicalStringify(envelope), { mode: 0o600 });
  const child = spawn(process.execPath, [new URL(import.meta.url).pathname], {
    env: {
      ...process.env,
      DAG_EVALUATION_TEST_CHILD_MODE: "publish-envelope",
      DAG_EVALUATION_TEST_PROJECT_ROOT: root,
      DAG_EVALUATION_TEST_ENVELOPE_PATH: envelopePath,
      DAG_EVALUATION_TEST_READY_PATH: readyPath,
      DAG_EVALUATION_TEST_RELEASE_PATH: releasePath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  try {
    for (let attempt = 0; attempt < 400; attempt += 1) {
      try { await readFile(readyPath); break; }
      catch (error) { if (error.code !== "ENOENT") throw error; }
      if (attempt === 399) throw new Error(`child did not reach envelope/index failpoint: ${stderr}`);
      await delay(5);
    }
    assert.equal((await parentStore.readIndex()).entries.length, 0);
    assert.equal((await parentStore.readEnvelope(envelope.envelopeHash)).envelopeHash, envelope.envelopeHash);
    await assert.rejects(parentStore.maintain("2026-08-05T00:01:00.000Z"), RunEvaluationStoreBusyError);
    assert.equal((await parentStore.readEnvelope(envelope.envelopeHash)).envelopeHash, envelope.envelopeHash);
    const exited = new Promise((resolve) => child.once("exit", (code, signal) => resolve({ code, signal })));
    await writeFile(releasePath, "release", { flag: "wx", mode: 0o600 });
    assert.deepEqual(await exited, { code: 0, signal: null }, stderr);
    const index = await parentStore.readIndex();
    assert.equal(index.entries[0].envelopeHash, envelope.envelopeHash);
    assert.equal(canonicalStringify(await parentStore.readEnvelope(envelope.envelopeHash)), canonicalStringify(envelope));
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGKILL");
      await exited;
    }
    await rm(root, { recursive: true, force: true });
  }
});

test("concurrent envelope writers cannot commit an index before exact bytes are published", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-envelope-writers-"));
  let releaseFirst, reachedFirst;
  const gate = new Promise((resolve) => { releaseFirst = resolve; });
  const reached = new Promise((resolve) => { reachedFirst = resolve; });
  const firstStore = new RunEvaluationStoreV1(root, { failpoint: async (point) => {
    if (point !== "before_envelope_index_commit") return;
    reachedFirst();
    await gate;
  } });
  const secondStore = new RunEvaluationStoreV1(root);
  let first, second;
  try {
    await firstStore.initialize();
    first = await buildBoundEnvelope(firstStore, envelopeCore({ projectIdentityHash: I("writer-project"), runIdentityHash: I("writer-a") }));
    second = await buildBoundEnvelope(firstStore, envelopeCore({ projectIdentityHash: I("writer-project"), runIdentityHash: I("writer-b"), sourceSnapshotHash: H("e") }));
    const firstPublication = firstStore.publishEnvelope(first, NOW);
    await reached;
    await assert.rejects(secondStore.publishEnvelope(second, NOW), RunEvaluationStoreBusyError);
    await assert.rejects(secondStore.readEnvelope(second.envelopeHash));
    releaseFirst();
    await firstPublication;
    await secondStore.publishEnvelope(second, NOW);
    const index = await firstStore.readIndex();
    assert.deepEqual(new Set(index.entries.map(({ envelopeHash }) => envelopeHash)), new Set([first.envelopeHash, second.envelopeHash]));
    for (const envelope of [first, second]) assert.equal(canonicalStringify(await firstStore.readEnvelope(envelope.envelopeHash)), canonicalStringify(envelope));
  } finally { releaseFirst?.(); await rm(root, { recursive: true, force: true }); }
});

test("envelope publication is immutable, corrections replace heads, and latest-50 retention is deterministic per project", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-retention-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const published = [];
    for (let index = 0; index < 51; index += 1) {
      const suffix = String(index).padStart(2, "0");
      const envelope = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash: I(`run-${suffix}`), sourceRevision: index + 1, sourceSnapshotHash: canonicalHash({ index }), cutoffAt: `2026-08-${String((index % 28) + 1).padStart(2, "0")}T00:${String(Math.floor(index / 28)).padStart(2, "0")}:00.000Z` }));
      published.push(envelope);
      await store.publishEnvelope(envelope, "2026-09-01T00:00:00.000Z");
    }
    const index = await store.readIndex();
    assert.equal(index.entries.filter(({ projectIdentityHash }) => projectIdentityHash === I("project-1")).length, 50);
    const sorted = [...published].sort((a, b) => {
      const aTime = parseRfc3339UtcNanosecondsV1(a.cutoff.cutoffAt), bTime = parseRfc3339UtcNanosecondsV1(b.cutoff.cutoffAt);
      return (aTime < bTime ? -1 : aTime > bTime ? 1 : 0) || a.source.revision - b.source.revision || a.identity.runIdentityHash.localeCompare(b.identity.runIdentityHash) || a.identity.runNonceHash.localeCompare(b.identity.runNonceHash) || a.cutoff.cutoffIdentityHash.localeCompare(b.cutoff.cutoffIdentityHash) || a.envelopeHash.localeCompare(b.envelopeHash);
    });
    assert.equal(index.entries.some(({ envelopeHash }) => envelopeHash === sorted[0].envelopeHash), false);
    await assert.rejects(store.readEnvelope(sorted[0].envelopeHash));

    const head = published.find((item) => index.entries.some(({ envelopeHash }) => envelopeHash === item.envelopeHash));
    const correctedMetrics = clone(head.metrics);
    correctedMetrics.attempts.attempts = countMetric(10);
    const correction = await buildBoundEnvelope(store, { ...envelopeCore({ runIdentityHash: head.identity.runIdentityHash, sourceRevision: head.source.revision, sourceSnapshotHash: head.source.snapshotHash, cutoffAt: head.cutoff.cutoffAt, supersedesEnvelopeHash: head.envelopeHash, metrics: correctedMetrics }), cutoff: clone(head.cutoff) });
    await store.publishEnvelope(correction, "2026-09-02T00:00:00.000Z", head.envelopeHash);
    assert.equal((await store.readIndex()).entries.some(({ envelopeHash }) => envelopeHash === correction.envelopeHash), true);
    await assert.rejects(store.readEnvelope(head.envelopeHash));
    const staleCore = clone(correction);
    delete staleCore.envelopeHash;
    delete staleCore.metricSourceBindings;
    staleCore.metrics.attempts.attempts = countMetric(11);
    await assert.rejects(store.publishEnvelope(buildRunEvaluationEnvelopeV1(staleCore), "2026-09-03T00:00:00.000Z"), RunEvaluationStoreConflictError);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("seven-day cleanup requires exact terminal identity and preserves censored/mismatched sidecars", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-seven-day-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const terminalAcc = initialAccumulator(I("terminal-run"), I("retention-project"), { revision: 3, snapshotHash: H("d") });
    await store.writeAccumulator(terminalAcc, null);
    const terminal = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash: I("terminal-run"), projectIdentityHash: I("retention-project"), sourceAccumulatorHash: terminalAcc.accumulatorHash }));
    await store.publishEnvelope(terminal, "2026-08-01T00:00:00.000Z");

    const censoredAcc = initialAccumulator(I("censored-run"), I("retention-project"), { revision: 3, snapshotHash: H("d") });
    await store.writeAccumulator(censoredAcc, null);
    const censoredMetrics = metricSections();
    const censor = (metric) => ({ ...metric, status: "censored", censoredCount: 1 });
    for (const section of Object.values(censoredMetrics)) for (const key of Object.keys(section)) section[key] = censor(section[key]);
    const censored = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash: I("censored-run"), projectIdentityHash: I("retention-project"), sourceAccumulatorHash: censoredAcc.accumulatorHash, cutoffKind: "right_censored", cutoffClass: "checkpoint", checkpointIdentityHash: I("week-1"), metrics: censoredMetrics }));
    await store.publishEnvelope(censored, "2026-08-01T00:00:00.000Z");

    assert.equal((await store.maintain("2026-08-07T23:59:59.999Z")).deletedAccumulatorCount, 0);
    assert.equal((await store.maintain("2026-08-08T00:00:00.000Z")).deletedAccumulatorCount, 1);
    assert.equal(await store.readAccumulator(I("terminal-run")), null);
    assert.notEqual(await store.readAccumulator(I("censored-run")), null);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("seven-day current-head cleanup preserves accumulators when the exact indexed envelope is missing", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-missing-envelope-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("missing-envelope-run"), projectIdentityHash = I("missing-envelope-project");
    const accumulator = initialAccumulator(runIdentityHash, projectIdentityHash, { revision: 3, snapshotHash: H("d") });
    await store.writeAccumulator(accumulator, null);
    const envelope = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash, projectIdentityHash, sourceAccumulatorHash: accumulator.accumulatorHash }));
    await store.publishEnvelope(envelope, "2026-08-01T00:00:00.000Z");
    await unlink(join(root, ".ai", "dag-evaluations-v1", "envelopes", `${envelope.envelopeHash.slice("sha256:".length)}.json`));
    const result = await store.maintain("2026-08-08T00:00:00.000Z");
    assert.deepEqual({ deleted: result.deletedAccumulatorCount, errors: result.cleanupErrorCount }, { deleted: 0, errors: 1 });
    assert.equal((await store.readAccumulator(runIdentityHash)).accumulatorHash, accumulator.accumulatorHash);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("publication requires the envelope credit context to match the exact accumulator", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-credit-mismatch-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("credit-mismatch-run"), projectIdentityHash = I("credit-mismatch-project");
    const accumulator = initialAccumulator(runIdentityHash, projectIdentityHash, { revision: 3, snapshotHash: H("d") });
    await store.writeAccumulator(accumulator, null);
    const core = envelopeCore({ runIdentityHash, projectIdentityHash, sourceAccumulatorHash: accumulator.accumulatorHash, creditContext: { acceptedIntegrationLineages: [], actionableFindingDispositions: [] } });
    core.attribution.creditedOperations = [];
    core.coverage = { status: "not_observed", sourceRevisionCount: 4, observedRevisionCount: 0, missingRevisionCount: 0, droppedRevisionCount: 0, censoredIntervalCount: 0, observerFailureCount: 0 };
    core.metrics.usefulParallelism = usefulParallelismMetricsV1(accumulator, { serialPolicy: false, rightCensored: false, clockQuality: "same_epoch" });
    const envelope = buildRunEvaluationEnvelopeV1(core);
    await assert.rejects(store.publishEnvelope(envelope, "2026-08-01T00:00:00.000Z"), /exact current source accumulator/);
    assert.equal((await store.readIndex()).entries.length, 0);
    assert.equal((await store.readAccumulator(runIdentityHash)).accumulatorHash, accumulator.accumulatorHash);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("retention thresholds and latest-50 ordering use exact nanoseconds", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-retention-ns-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("retention-ns");
    const projectIdentityHash = I("retention-ns-project");
    const accumulator = initialAccumulator(runIdentityHash, projectIdentityHash, { revision: 3, snapshotHash: H("d") });
    await store.writeAccumulator(accumulator, null);
    const terminal = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash, projectIdentityHash, sourceAccumulatorHash: accumulator.accumulatorHash, cutoffAt: "2026-01-01T00:00:00Z" }));
    await store.publishEnvelope(terminal, "2026-01-01T00:00:00.000000001Z");
    assert.equal((await store.maintain("2026-01-08T00:00:00Z")).deletedAccumulatorCount, 0);
    assert.equal((await store.maintain("2026-01-08T00:00:00.000000001Z")).deletedAccumulatorCount, 1);
    await assert.rejects(store.maintain("2026-02-30T00:00:00Z"), /real UTC RFC 3339/);

    const orderingProject = I("latest-50-ns-project");
    const published = [];
    for (let index = 0; index < 51; index += 1) {
      const cutoffAt = index === 0 ? "2026-03-01T00:00:00Z" : `2026-03-01T00:00:00.${String(index).padStart(9, "0")}Z`;
      const envelope = await buildBoundEnvelope(store, envelopeCore({ projectIdentityHash: orderingProject, runIdentityHash: I(`latest-ns-${index}`), sourceRevision: index + 1, sourceSnapshotHash: canonicalHash({ latest: index }), cutoffAt }));
      published.push(envelope);
      await store.publishEnvelope(envelope, "2026-03-02T00:00:00Z");
    }
    const index = await store.readIndex();
    assert.equal(index.entries.filter(({ projectIdentityHash: project }) => project === orderingProject).length, 50);
    assert.equal(index.entries.some(({ envelopeHash }) => envelopeHash === published[0].envelopeHash), false);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("equal accumulator revisions are exact no-ops and cannot fork source snapshots", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-equal-revision-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const initial = initialAccumulator(I("equal-revision"));
    await store.writeAccumulator(initial, null);
    assert.equal((await store.writeAccumulator(clone(initial), initial.accumulatorHash)).accumulatorHash, initial.accumulatorHash);
    const sourceFork = clone(initial);
    sourceFork.source.snapshotHash = H("e");
    delete sourceFork.accumulatorHash;
    sourceFork.accumulatorHash = canonicalHash(sourceFork);
    await assert.rejects(store.writeAccumulator(sourceFork, initial.accumulatorHash), /exact source snapshot/);
    const contentFork = clone(initial);
    contentFork.coverage.observerFailureCount = 1;
    delete contentFork.accumulatorHash;
    contentFork.accumulatorHash = canonicalHash(contentFork);
    await assert.rejects(store.writeAccumulator(contentFork, initial.accumulatorHash), /exact no-op/);
    assert.equal((await store.readAccumulator(I("equal-revision"))).accumulatorHash, initial.accumulatorHash);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("equal observation revisions reject a different committed snapshot hash", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-equal-fold-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const initial = initialAccumulator(I("equal-fold"));
    await store.writeAccumulator(initial, null);
    assert.equal((await store.foldObservation(I("equal-fold"), observation(0, 0, { snapshotHash: H("0") }))).folded, false);
    await assert.rejects(store.foldObservation(I("equal-fold"), observation(0, 0, { snapshotHash: H("e") })), /exact source snapshot/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("stale and equal fold observations are schema and privacy validated before no-op handling", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-stale-secret-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("stale-secret");
    const initial = initialAccumulator(runIdentityHash, I("stale-secret-project"), { revision: 3, snapshotHash: H("3") });
    await store.writeAccumulator(initial, null);
    const staleSecret = { ...observation(2, 2, { snapshotHash: H("2") }), secretPromptTranscript: "SECRET_/absolute/private/path" };
    await assert.rejects(store.foldObservation(runIdentityHash, staleSecret), /Invalid observation/);
    const equalSecret = { ...observation(3, 3, { snapshotHash: H("3") }), unrestrictedPayload: { secret: true } };
    await assert.rejects(store.foldObservation(runIdentityHash, equalSecret), /Invalid observation/);
    assert.equal((await store.readAccumulator(runIdentityHash)).accumulatorHash, initial.accumulatorHash);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("stale and equal store folds reject credit-context mutations before no-op handling", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-stale-credit-context-"));
  const store = new RunEvaluationStoreV1(root);
  try {
    await store.initialize();
    const runIdentityHash = I("stale-credit-context");
    const initial = initialAccumulator(runIdentityHash, I("stale-credit-context-project"), { revision: 3, snapshotHash: H("3") });
    await store.writeAccumulator(initial, null);
    const stale = observation(2, 2, { active: [{ operationHash: H("e"), creditBasis: clone(TRANSITION_BASIS) }], creditContext: TRANSITION_CREDIT_CONTEXT, snapshotHash: H("2") });
    const equal = observation(3, 3, { active: [{ operationHash: H("e"), creditBasis: clone(TRANSITION_BASIS) }], creditContext: TRANSITION_CREDIT_CONTEXT, snapshotHash: H("3") });
    await assert.rejects(store.foldObservation(runIdentityHash, stale), /credit context canonical bytes and hash/);
    await assert.rejects(store.foldObservation(runIdentityHash, equal), /credit context canonical bytes and hash/);
    const unchanged = await store.readAccumulator(runIdentityHash);
    assert.equal(unchanged.accumulatorHash, initial.accumulatorHash);
    assert.equal(unchanged.integrals.creditableWorkMs, 0);
    assert.equal(canonicalStringify(unchanged.creditContext), canonicalStringify(CREDIT_CONTEXT));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("latest-50 eviction leaves bounded hash-bound receipts and cleans all 51 terminals at exactly seven days", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-pending-cleanup-"));
  const store = new RunEvaluationStoreV1(root);
  const projectIdentityHash = I("pending-cleanup-project");
  const runs = [];
  try {
    await store.initialize();
    for (let index = 0; index < 51; index += 1) {
      const runIdentityHash = I(`pending-cleanup-${index}`);
      runs.push(runIdentityHash);
      const accumulator = initialAccumulator(runIdentityHash, projectIdentityHash, { revision: 3, snapshotHash: H("d") });
      await store.writeAccumulator(accumulator, null);
      const envelope = await buildBoundEnvelope(store, envelopeCore({ runIdentityHash, projectIdentityHash, sourceAccumulatorHash: accumulator.accumulatorHash, cutoffAt: `2026-08-05T00:00:${String(index).padStart(2, "0")}.000Z` }));
      await store.publishEnvelope(envelope, "2026-08-05T01:00:00.000000001Z");
    }
    const index = await store.readIndex();
    assert.equal(index.entries.length, 50);
    assert.equal(index.pendingAccumulatorCleanup.length, 1);
    assert.match(index.pendingAccumulatorCleanup[0].receiptHash, /^sha256:[0-9a-f]{64}$/);
    assert.equal(canonicalStringify(index).includes("pending-cleanup-"), false);
    assert.equal((await store.maintain("2026-08-12T01:00:00Z")).deletedAccumulatorCount, 0);
    assert.equal((await store.maintain("2026-08-12T01:00:00.000000001Z")).deletedAccumulatorCount, 51);
    assert.equal((await store.readIndex()).pendingAccumulatorCleanup.length, 0);
    for (const runIdentityHash of runs) assert.equal(await store.readAccumulator(runIdentityHash), null);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("canonical root binding rejects preexisting symlinks and post-check directory substitution", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-path-root-"));
  const outside = await mkdtemp(join(tmpdir(), "dag-evaluation-path-outside-"));
  try {
    await mkdir(join(root, ".ai"), { mode: 0o700 });
    await symlink(outside, join(root, ".ai", "dag-evaluations-v1"), "dir");
    await assert.rejects(new RunEvaluationStoreV1(root).initialize(), /non-symlink directory/);
    await unlink(join(root, ".ai", "dag-evaluations-v1"));
    let swapped = false;
    const accumulatorDirectory = join(root, ".ai", "dag-evaluations-v1", "accumulators");
    const displaced = `${accumulatorDirectory}.displaced`;
    const store = new RunEvaluationStoreV1(root, { failpoint: async (point) => {
      if (point !== "after_accumulator_temp_sync" || swapped) return;
      swapped = true;
      await rename(accumulatorDirectory, displaced);
      await symlink(outside, accumulatorDirectory, "dir");
    } });
    await store.initialize();
    await assert.rejects(store.writeAccumulator(initialAccumulator(I("path-swap")), null), /non-symlink directory|identity changed/);
    assert.deepEqual(await readdir(outside), []);
    await unlink(accumulatorDirectory);
    await rename(displaced, accumulatorDirectory);
  } finally { await rm(root, { recursive: true, force: true }); await rm(outside, { recursive: true, force: true }); }
});

test("process-owned locks reject live takeover and recover exact SIGKILL residue", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-evaluation-lock-crash-"));
  const accumulatorPath = join(root, "child-accumulator.json");
  const readyPath = join(root, "child-ready");
  const accumulator = initialAccumulator(I("crash-lock"));
  await writeFile(accumulatorPath, canonicalStringify(accumulator), { mode: 0o600 });
  const child = spawn(process.execPath, [new URL(import.meta.url).pathname], {
    env: {
      ...process.env,
      DAG_EVALUATION_TEST_CHILD_MODE: "crash-lock",
      DAG_EVALUATION_TEST_PROJECT_ROOT: root,
      DAG_EVALUATION_TEST_ACCUMULATOR_PATH: accumulatorPath,
      DAG_EVALUATION_TEST_READY_PATH: readyPath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  try {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      try { await readFile(readyPath); break; } catch (error) { if (error.code !== "ENOENT") throw error; }
      if (attempt === 199) throw new Error(`child did not acquire evaluation lock: ${stderr}`);
      await delay(10);
    }
    const ownerRaw = await readFile(join(root, ".ai", "dag-evaluations-v1", "telemetry.lock", "owner.json"), "utf8");
    const owner = JSON.parse(ownerRaw);
    assert.equal(owner.pid, child.pid);
    assert.match(owner.processStartIdentity, /^linux-proc:[0-9]+$/);
    assert.equal(canonicalStringify(owner).includes(root), false);
    const contender = new RunEvaluationStoreV1(root);
    await assert.rejects(contender.writeAccumulator(accumulator, null), RunEvaluationStoreBusyError);
    const crashed = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGKILL");
    await crashed;
    const recovered = new RunEvaluationStoreV1(root);
    await recovered.writeAccumulator(accumulator, null);
    assert.equal((await recovered.readAccumulator(I("crash-lock"))).accumulatorHash, accumulator.accumulatorHash);
    await assert.rejects(readFile(join(root, ".ai", "dag-evaluations-v1", "telemetry.lock", "owner.json")), { code: "ENOENT" });
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGKILL");
      await exited;
    }
    await rm(root, { recursive: true, force: true });
  }
});

// Prospective six-pair portfolio and disposable pure report.
test("portfolio fixture identities are exact deterministic bottom-up content hashes", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  for (const pair of portfolio.pairs) {
    for (const execution of pair.executions) assert.equal(execution.executionIdentityHash, executionIdentityHashV1(pair, execution));
    assert.equal(pair.pairIdentityHash, pairIdentityHashV1(pair));
  }
  assert.equal(portfolio.portfolioIdentityHash, portfolioIdentityHashV1(portfolio));
  assert.equal(dagEvaluationCohortHashV1(portfolio), dagEvaluationCohortHashV1(clone(portfolio)));
});

test("baseline commit and tree changes cannot reuse nested or portfolio identities", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  const originalCohortHash = dagEvaluationCohortHashV1(portfolio);
  for (const field of ["baselineCommitHash", "baselineTreeHash"]) {
    const modified = clone(portfolio);
    modified.pairs[0][field] = H("f");
    const validation = validateDagEvaluationPortfolioV1(modified);
    assert.equal(validation.ok, false);
    assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/executions/0/executionIdentityHash"));
    assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/pairIdentityHash"));
    assert.ok(validation.issues.some(({ path }) => path === "/portfolioIdentityHash"));
    assert.throws(() => buildPairedEvaluationReportV1(modified, pairedResults(portfolio)), /Invalid portfolio/);
    const resealed = resealPortfolio(modified);
    assert.equal(validateDagEvaluationPortfolioV1(resealed).ok, true);
    assert.notEqual(resealed.portfolioIdentityHash, portfolio.portfolioIdentityHash);
    assert.notEqual(dagEvaluationCohortHashV1(resealed), originalCohortHash);
  }
});

test("execution order changes cannot retain execution, pair, or portfolio identities", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  portfolio.pairs[0].executions[0].order = portfolio.pairs[0].executions[0].order === 1 ? 2 : 1;
  const validation = validateDagEvaluationPortfolioV1(portfolio);
  assert.equal(validation.ok, false);
  assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/executions/0/executionIdentityHash"));
  assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/pairIdentityHash"));
  assert.ok(validation.issues.some(({ path }) => path === "/portfolioIdentityHash"));
});

test("scenario substitutions cannot retain execution, pair, or portfolio identities", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  portfolio.pairs[0].scenarioClass = portfolio.pairs[0].scenarioClass === "integration_train" ? "recovery_sensitive" : "integration_train";
  const validation = validateDagEvaluationPortfolioV1(portfolio);
  assert.equal(validation.ok, false);
  assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/executions/0/executionIdentityHash"));
  assert.ok(validation.issues.some(({ path }) => path === "/pairs/0/pairIdentityHash"));
  assert.ok(validation.issues.some(({ path }) => path === "/portfolioIdentityHash"));
});

test("execution, pair, and portfolio ID substitutions are rejected independently", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  const substitutions = [
    { mutate: (value) => { value.pairs[0].executions[0].executionIdentityHash = H("f"); }, path: "/pairs/0/executions/0/executionIdentityHash" },
    { mutate: (value) => { value.pairs[0].pairIdentityHash = H("f"); }, path: "/pairs/0/pairIdentityHash" },
    { mutate: (value) => { value.portfolioIdentityHash = H("f"); }, path: "/portfolioIdentityHash" },
  ];
  for (const { mutate, path } of substitutions) {
    const modified = clone(portfolio);
    mutate(modified);
    const validation = validateDagEvaluationPortfolioV1(modified);
    assert.equal(validation.ok, false);
    assert.ok(validation.issues.some((issue) => issue.path === path));
  }
});

test("portfolio is exactly six counterbalanced pairs and paired reports enforce cohorts/invariants", async () => {
  const portfolio = parseDagEvaluationPortfolioV1(await readFile(new URL("./fixtures/dag-evaluation-portfolio-v1.json", import.meta.url), "utf8"));
  assert.equal(validateDagEvaluationPortfolioV1(portfolio).ok, true);
  assert.equal(portfolio.pairs.length, 6);
  assert.equal(portfolio.pairs.flatMap(({ executions }) => executions).length, 12);
  const results = pairedResults(portfolio);
  const report = buildPairedEvaluationReportV1(portfolio, results);
  assert.equal(report.pairDeltas.length, 6);
  assert.equal(report.elapsedSummary.wins + report.elapsedSummary.ties + report.elapsedSummary.losses, 6);
  const bytes = canonicalStringify(report);
  for (const forbidden of ["pValue", "confidence", "significance", "population", "composite"]) assert.equal(bytes.includes(forbidden), false);
  const mismatch = clone(results); mismatch[0].cohortHash = H("f");
  assert.throws(() => buildPairedEvaluationReportV1(portfolio, mismatch), /Cohort mismatch/);
  const failed = clone(results); failed[0].uncompensatedInvariantsPass = false;
  assert.throws(() => buildPairedEvaluationReportV1(portfolio, failed), /not a valid uncompensated comparison/);
  const malformed = clone(portfolio); malformed.pairs[0].executions.find(({ mode }) => mode === "serial").maxActiveNodes = 2;
  assert.equal(validateDagEvaluationPortfolioV1(malformed).ok, false);
});

let passed = 0;
for (const [name, fn] of tests) {
  try { await fn(); passed += 1; console.log(`ok ${passed} - ${name}`); }
  catch (error) { console.error(`not ok ${passed + 1} - ${name}`); throw error; }
}
console.log(`dag evaluation tests passed: ${passed}`);
