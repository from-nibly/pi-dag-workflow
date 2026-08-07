import { Type, type Static } from "typebox";
import {
  HashSchema,
  NonNegativeIntegerSchema,
  StrictObject,
  TimestampSchema,
  canonicalHash,
  canonicalStringify,
  hashWithoutField,
  isSortedUnique,
  parseStrictJson,
  pushIssue,
  schemaIssues,
  type ValidationIssue,
  type ValidationResult,
} from "./common.ts";

const NonNegativeNumberSchema = Type.Number({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
const NullableNumberSchema = Type.Union([NonNegativeNumberSchema, Type.Null()]);
const NullableHashSchema = Type.Union([HashSchema, Type.Null()]);
type Mutable<T> = T extends readonly (infer U)[] ? Mutable<U>[] : T extends object ? { -readonly [K in keyof T]: Mutable<T[K]> } : T;

export const EvaluationMetricStatusSchema = Type.Enum([
  "measured",
  "zero_exposure",
  "unsupported_policy",
  "not_observed",
  "partial_coverage",
  "censored",
]);
export const EvaluationMetricUnitSchema = Type.Enum([
  "count",
  "milliseconds",
  "token",
  "provider_reported_cost",
  "ratio",
]);
export const EvaluationMetricV1Schema = StrictObject({
  status: EvaluationMetricStatusSchema,
  unit: EvaluationMetricUnitSchema,
  numerator: NullableNumberSchema,
  denominator: NullableNumberSchema,
  observedCount: NonNegativeIntegerSchema,
  missingCount: NonNegativeIntegerSchema,
  censoredCount: NonNegativeIntegerSchema,
});
export type EvaluationMetricV1 = Static<typeof EvaluationMetricV1Schema>;

const RUN_EVALUATION_PROFILE_CORE_V1 = {
  profileId: "dag-run-evaluation-v1",
  version: 1,
  canonicalization: "jcs-v1",
  metricStatuses: ["censored", "measured", "not_observed", "partial_coverage", "unsupported_policy", "zero_exposure"],
  intervalPolicy: "half-open-close-before-open-v1",
  clockDiscontinuityPolicy: "censor-open-intervals-v1",
  creditPolicy: "exact-bound-accepted-evidence-integration-receipt-or-unique-current-finding-disposition-v2",
  sourceClosurePolicy: "exact-sorted-hash-sets-with-derived-closures-and-accumulator-credit-context-v2",
  metricSemanticPolicy: "closed-status-unit-count-denominator-and-quality-prerequisite-matrix-v2",
  timingStatusPolicy: "clock-coverage-cutoff-and-serial-derived-v1",
  terminalAccumulatorBindingPolicy: "exact-source-accumulator-hash-in-source-closure-v1",
  observationBoundPolicy: "combined-open-map-maxima-v1",
  readinessPolicy: "correctness-ready-before-admission-v1",
  retentionPolicy: "local-latest-50-exact-rfc3339-nanosecond-terminal-sidecar-7d-v2",
  privacyPolicy: "canonical-identity-hashes-only-v2",
} as const;
export const RUN_EVALUATION_PROFILE_HASH_V1 = canonicalHash(RUN_EVALUATION_PROFILE_CORE_V1);
export const RUN_EVALUATION_CLOCK_POLICY_HASH_V1 = canonicalHash({
  intervalPolicy: RUN_EVALUATION_PROFILE_CORE_V1.intervalPolicy,
  clockDiscontinuityPolicy: RUN_EVALUATION_PROFILE_CORE_V1.clockDiscontinuityPolicy,
});
export const RUN_EVALUATION_PROFILE_V1 = Object.freeze({
  profileId: RUN_EVALUATION_PROFILE_CORE_V1.profileId,
  version: RUN_EVALUATION_PROFILE_CORE_V1.version,
  profileHash: RUN_EVALUATION_PROFILE_HASH_V1,
});

export const SourceClosureV1Schema = StrictObject({
  count: NonNegativeIntegerSchema,
  hashOfSortedHashes: HashSchema,
});
export type SourceClosureV1 = Static<typeof SourceClosureV1Schema>;

export const CreditBasisV1Schema = Type.Union([
  StrictObject({
    kind: Type.Literal("accepted_integration_lineage"),
    acceptedEvidenceHash: HashSchema,
    integrationReceiptHash: HashSchema,
  }),
  StrictObject({
    kind: Type.Literal("actionable_finding_disposition"),
    findingHash: HashSchema,
    currentDispositionHash: HashSchema,
  }),
]);
export type CreditBasisV1 = Static<typeof CreditBasisV1Schema>;
const NullableCreditBasisV1Schema = Type.Union([CreditBasisV1Schema, Type.Null()]);

/**
 * The bounded final committed credit facts used for a v1 evaluation replay.
 * Once an accumulator is created, these canonical bytes and their hash are
 * immutable. Live credit-context transitions require a future schema; an
 * observation cannot authorize credit by replacing this context.
 */
export const CanonicalCreditContextV1Schema = StrictObject({
  acceptedIntegrationLineages: Type.Array(StrictObject({ acceptedEvidenceHash: HashSchema, integrationReceiptHash: HashSchema }), { maxItems: 2048 }),
  actionableFindingDispositions: Type.Array(StrictObject({ findingHash: HashSchema, currentDispositionHash: HashSchema }), { maxItems: 2048 }),
});
export type CanonicalCreditContextV1 = Static<typeof CanonicalCreditContextV1Schema>;

const OpenIntervalV1Schema = StrictObject({
  intervalHash: HashSchema,
  class: Type.Enum([
    "readiness_lane_admit",
    "reserved_dispatch",
    "operation",
    "human_active",
    "authority_wait",
    "recovery",
  ]),
  sourceRevision: NonNegativeIntegerSchema,
  sourceSnapshotHash: HashSchema,
  clockEpochHash: HashSchema,
  openedAtMonotonicMs: NonNegativeNumberSchema,
  creditBasis: NullableCreditBasisV1Schema,
});
const RevisionGapV1Schema = StrictObject({
  firstRevision: NonNegativeIntegerSchema,
  lastRevision: NonNegativeIntegerSchema,
});
const SixBucketHistogramV1Schema = Type.Array(NonNegativeIntegerSchema, { minItems: 6, maxItems: 6 });

export const RunObservationAccumulatorV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("run_observation_accumulator"),
  canonicalization: Type.Literal("jcs-v1"),
  accumulatorHash: HashSchema,
  identity: StrictObject({
    projectIdentityHash: HashSchema,
    runIdentityHash: HashSchema,
    runNonceHash: HashSchema,
    planHash: HashSchema,
    evaluationProfileHash: HashSchema,
    clockPolicyHash: HashSchema,
  }),
  creditContext: CanonicalCreditContextV1Schema,
  creditContextHash: HashSchema,
  source: StrictObject({
    revision: NonNegativeIntegerSchema,
    snapshotHash: HashSchema,
    observedAt: TimestampSchema,
    clockEpochHash: HashSchema,
    monotonicTickMs: NonNegativeNumberSchema,
  }),
  clockHistory: StrictObject({
    initialEpochHash: HashSchema,
    currentEpochHash: HashSchema,
    epochChangeCount: NonNegativeIntegerSchema,
    monotonicResetCount: NonNegativeIntegerSchema,
    unsupportedObservationCount: NonNegativeIntegerSchema,
  }),
  open: StrictObject({
    readiness: Type.Array(OpenIntervalV1Schema, { maxItems: 512 }),
    active: Type.Array(OpenIntervalV1Schema, { maxItems: 1024 }),
    human: Type.Array(OpenIntervalV1Schema, { maxItems: 128 }),
    recovery: Type.Array(OpenIntervalV1Schema, { maxItems: 64 }),
  }),
  counters: StrictObject({
    readinessIntervals: NonNegativeIntegerSchema,
    dispatchWaitIntervals: NonNegativeIntegerSchema,
    activeIntervals: NonNegativeIntegerSchema,
    humanActiveIntervals: NonNegativeIntegerSchema,
    authorityWaitIntervals: NonNegativeIntegerSchema,
    recoveryIntervals: NonNegativeIntegerSchema,
    falseIndependenceIncidents: NonNegativeIntegerSchema,
  }),
  sums: StrictObject({
    readinessWaitMs: NonNegativeNumberSchema,
    dispatchWaitMs: NonNegativeNumberSchema,
    humanActiveMs: NonNegativeNumberSchema,
    authorityWaitMs: NonNegativeNumberSchema,
    recoveryMs: NonNegativeNumberSchema,
    falseIndependenceWasteMs: NonNegativeNumberSchema,
  }),
  histograms: StrictObject({
    readinessWait: SixBucketHistogramV1Schema,
    dispatchWait: SixBucketHistogramV1Schema,
    recovery: SixBucketHistogramV1Schema,
  }),
  integrals: StrictObject({
    autonomousElapsedMs: NonNegativeNumberSchema,
    allOperationWorkMs: NonNegativeNumberSchema,
    creditableWorkMs: NonNegativeNumberSchema,
    usefulOverlapMs: NonNegativeNumberSchema,
    parallelOpportunityMs: NonNegativeNumberSchema,
  }),
  coverage: StrictObject({
    observedRevisionCount: NonNegativeIntegerSchema,
    droppedRevisionCount: NonNegativeIntegerSchema,
    missingRevisionCount: NonNegativeIntegerSchema,
    censoredIntervalCount: NonNegativeIntegerSchema,
    observerFailureCount: NonNegativeIntegerSchema,
    revisionGaps: Type.Array(RevisionGapV1Schema, { maxItems: 64 }),
    revisionGapOverflowCount: NonNegativeIntegerSchema,
  }),
});
export type RunObservationAccumulatorV1 = Static<typeof RunObservationAccumulatorV1Schema>;

export const AccumulatorObservationV1Schema = StrictObject({
  revision: NonNegativeIntegerSchema,
  snapshotHash: HashSchema,
  observedAt: TimestampSchema,
  clockEpochHash: HashSchema,
  monotonicTickMs: NonNegativeNumberSchema,
  readyOperationHashes: Type.Array(HashSchema, { maxItems: 512 }),
  reservedOperationHashes: Type.Array(HashSchema, { maxItems: 512 }),
  activeOperations: Type.Array(StrictObject({ operationHash: HashSchema, creditBasis: NullableCreditBasisV1Schema }), { maxItems: 512 }),
  humanActiveHashes: Type.Array(HashSchema, { maxItems: 64 }),
  authorityWaitHashes: Type.Array(HashSchema, { maxItems: 64 }),
  recoveryHashes: Type.Array(HashSchema, { maxItems: 64 }),
  falseIndependenceIncidents: Type.Array(StrictObject({
    findingHash: HashSchema,
    currentDispositionHash: HashSchema,
    operationHashes: Type.Array(HashSchema, { minItems: 1, maxItems: 512 }),
    wasteStartMonotonicMs: NonNegativeNumberSchema,
  }), { maxItems: 512 }),
  creditContext: CanonicalCreditContextV1Schema,
  droppedBefore: NonNegativeIntegerSchema,
  observerFailuresBefore: NonNegativeIntegerSchema,
  clockStatus: Type.Optional(Type.Enum(["supported", "unsupported"])),
});
export type AccumulatorObservationV1 = Static<typeof AccumulatorObservationV1Schema>;

export const QualityConditionedIntervalV1Schema = StrictObject({
  intervalHash: HashSchema,
  startMonotonicMs: NonNegativeNumberSchema,
  endMonotonicMs: NonNegativeNumberSchema,
  creditBasis: NullableCreditBasisV1Schema,
});
export type QualityConditionedIntervalV1 = Static<typeof QualityConditionedIntervalV1Schema>;
export interface QualityConditionedIntervalTotalsV1 {
  autonomousElapsedMs: number;
  allOperationWorkMs: number;
  creditableWorkMs: number;
  usefulOverlapMs: number;
  parallelOpportunityMs: number;
}

const AccumulatorTelemetryV1Schema = StrictObject({
  counters: StrictObject({
    readinessIntervals: NonNegativeIntegerSchema,
    dispatchWaitIntervals: NonNegativeIntegerSchema,
    activeIntervals: NonNegativeIntegerSchema,
    humanActiveIntervals: NonNegativeIntegerSchema,
    authorityWaitIntervals: NonNegativeIntegerSchema,
    recoveryIntervals: NonNegativeIntegerSchema,
    falseIndependenceIncidents: NonNegativeIntegerSchema,
  }),
  sums: StrictObject({
    readinessWaitMs: NonNegativeNumberSchema,
    dispatchWaitMs: NonNegativeNumberSchema,
    humanActiveMs: NonNegativeNumberSchema,
    authorityWaitMs: NonNegativeNumberSchema,
    recoveryMs: NonNegativeNumberSchema,
    falseIndependenceWasteMs: NonNegativeNumberSchema,
  }),
  histograms: StrictObject({
    readinessWait: SixBucketHistogramV1Schema,
    dispatchWait: SixBucketHistogramV1Schema,
    recovery: SixBucketHistogramV1Schema,
  }),
  integrals: StrictObject({
    autonomousElapsedMs: NonNegativeNumberSchema,
    allOperationWorkMs: NonNegativeNumberSchema,
    creditableWorkMs: NonNegativeNumberSchema,
    usefulOverlapMs: NonNegativeNumberSchema,
    parallelOpportunityMs: NonNegativeNumberSchema,
  }),
  coverage: StrictObject({
    observedRevisionCount: NonNegativeIntegerSchema,
    droppedRevisionCount: NonNegativeIntegerSchema,
    missingRevisionCount: NonNegativeIntegerSchema,
    censoredIntervalCount: NonNegativeIntegerSchema,
    observerFailureCount: NonNegativeIntegerSchema,
    revisionGaps: Type.Array(RevisionGapV1Schema, { maxItems: 64 }),
    revisionGapOverflowCount: NonNegativeIntegerSchema,
  }),
});
const MetricHistogramV1Schema = Type.Array(EvaluationMetricV1Schema, { minItems: 6, maxItems: 6 });
const MetricSectionsV1Schema = StrictObject({
  accumulatorTelemetry: AccumulatorTelemetryV1Schema,
  outcomes: StrictObject({ accepted: EvaluationMetricV1Schema, integrated: EvaluationMetricV1Schema }),
  attempts: StrictObject({ attempts: EvaluationMetricV1Schema, retries: EvaluationMetricV1Schema, backEdges: EvaluationMetricV1Schema }),
  findings: StrictObject({ total: EvaluationMetricV1Schema, disposed: EvaluationMetricV1Schema, falseIndependenceIncidents: EvaluationMetricV1Schema, falseIndependenceWaste: EvaluationMetricV1Schema }),
  integration: StrictObject({ conflicts: EvaluationMetricV1Schema, invalidations: EvaluationMetricV1Schema, reconciledEffects: EvaluationMetricV1Schema }),
  timing: StrictObject({
    autonomousElapsed: EvaluationMetricV1Schema,
    readinessWait: EvaluationMetricV1Schema,
    readinessWaitIntervals: EvaluationMetricV1Schema,
    dispatchWait: EvaluationMetricV1Schema,
    dispatchWaitIntervals: EvaluationMetricV1Schema,
    recovery: EvaluationMetricV1Schema,
    recoveryIntervals: EvaluationMetricV1Schema,
  }),
  waitHistograms: StrictObject({ readinessWait: MetricHistogramV1Schema, dispatchWait: MetricHistogramV1Schema, recovery: MetricHistogramV1Schema }),
  usefulParallelism: StrictObject({ usefulWork: EvaluationMetricV1Schema, usefulAverageConcurrency: EvaluationMetricV1Schema, usefulOverlapArea: EvaluationMetricV1Schema, parallelOpportunityArea: EvaluationMetricV1Schema, opportunityCapture: EvaluationMetricV1Schema, allOperationTime: EvaluationMetricV1Schema, workEfficiency: EvaluationMetricV1Schema }),
  humanAttention: StrictObject({ activeMinutes: EvaluationMetricV1Schema, activeIntervals: EvaluationMetricV1Schema, authorityWait: EvaluationMetricV1Schema, authorityWaitIntervals: EvaluationMetricV1Schema, decisions: EvaluationMetricV1Schema }),
  modelUsage: StrictObject({ inputTokens: EvaluationMetricV1Schema, outputTokens: EvaluationMetricV1Schema, cacheReadTokens: EvaluationMetricV1Schema, cacheWriteTokens: EvaluationMetricV1Schema, inferenceRequests: EvaluationMetricV1Schema, reportedCost: EvaluationMetricV1Schema }),
  instrumentation: StrictObject({ observedRevisions: EvaluationMetricV1Schema, droppedRevisions: EvaluationMetricV1Schema, missingRevisions: EvaluationMetricV1Schema, censoredIntervals: EvaluationMetricV1Schema }),
});

const InvariantStatusSchema = Type.Enum(["pass", "fail", "not_observed"]);
const InvariantsV1Schema = StrictObject({
  snapshotAndHashes: InvariantStatusSchema,
  planSourceJoins: InvariantStatusSchema,
  authorizationAndScope: InvariantStatusSchema,
  idempotencyAndStaleAdvancement: InvariantStatusSchema,
  effectsReconciled: InvariantStatusSchema,
  integrationExact: InvariantStatusSchema,
  completionExact: InvariantStatusSchema,
});
const SourceHashSetV1Schema = Type.Array(HashSchema, { maxItems: 2048 });
export const EvaluationSourceHashSetsV1Schema = StrictObject({
  authorization: SourceHashSetV1Schema,
  stageEvidence: SourceHashSetV1Schema,
  workerResults: SourceHashSetV1Schema,
  findingsAndResolutions: SourceHashSetV1Schema,
  effectReconciliation: SourceHashSetV1Schema,
  verification: SourceHashSetV1Schema,
  integration: SourceHashSetV1Schema,
  otherRequired: SourceHashSetV1Schema,
});
export type EvaluationSourceHashSetsV1 = Static<typeof EvaluationSourceHashSetsV1Schema>;
const SourceClosuresV1Schema = StrictObject({
  authorization: SourceClosureV1Schema,
  stageEvidence: SourceClosureV1Schema,
  workerResults: SourceClosureV1Schema,
  findingsAndResolutions: SourceClosureV1Schema,
  effectReconciliation: SourceClosureV1Schema,
  verification: SourceClosureV1Schema,
  integration: SourceClosureV1Schema,
  otherRequired: SourceClosureV1Schema,
});
const MetricFamilySourceBindingV1Schema = StrictObject({
  sourceHashes: Type.Array(HashSchema, { minItems: 1, maxItems: 8192 }),
  sourceClosure: SourceClosureV1Schema,
});
const MetricFamilySourceBindingsV1Schema = StrictObject({
  outcomes: MetricFamilySourceBindingV1Schema,
  attempts: MetricFamilySourceBindingV1Schema,
  findings: MetricFamilySourceBindingV1Schema,
  integration: MetricFamilySourceBindingV1Schema,
  humanAttention: MetricFamilySourceBindingV1Schema,
  modelUsage: MetricFamilySourceBindingV1Schema,
});
const CreditedOperationLineageV1Schema = StrictObject({
  operationHash: HashSchema,
  basis: CreditBasisV1Schema,
});
const FalseIndependenceAttributionV1Schema = StrictObject({
  findingHash: HashSchema,
  currentDispositionHash: HashSchema,
  operationHashes: Type.Array(HashSchema, { minItems: 1, maxItems: 512 }),
  wastedOperationMs: NonNegativeNumberSchema,
});
const EvaluationAttributionV1Schema = StrictObject({
  creditedOperations: Type.Array(CreditedOperationLineageV1Schema, { maxItems: 1024 }),
  falseIndependenceIncidents: Type.Array(FalseIndependenceAttributionV1Schema, { maxItems: 512 }),
});
const PulseItemV1Schema = StrictObject({
  status: Type.Enum(["measured", "not_observed"]),
  value: Type.Union([Type.Integer({ minimum: 1, maximum: 7 }), Type.Null()]),
});

export const RunEvaluationEnvelopeV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("run_evaluation_envelope"),
  canonicalization: Type.Literal("jcs-v1"),
  envelopeHash: HashSchema,
  evaluationProfile: StrictObject({ profileId: Type.Literal("dag-run-evaluation-v1"), version: Type.Literal(1), profileHash: HashSchema }),
  identity: StrictObject({ projectIdentityHash: HashSchema, runIdentityHash: HashSchema, runNonceHash: HashSchema, planHash: HashSchema }),
  source: StrictObject({ revision: NonNegativeIntegerSchema, snapshotHash: HashSchema, accumulatorHash: HashSchema, reviewReceiptHash: NullableHashSchema, authorizationReceiptHash: NullableHashSchema, freshnessReceiptHash: NullableHashSchema }),
  sourceHashes: EvaluationSourceHashSetsV1Schema,
  sourceClosures: SourceClosuresV1Schema,
  metricSourceBindings: MetricFamilySourceBindingsV1Schema,
  creditContext: CanonicalCreditContextV1Schema,
  creditContextHash: HashSchema,
  attribution: EvaluationAttributionV1Schema,
  cutoff: StrictObject({
    kind: Type.Enum(["terminal", "right_censored"]),
    class: Type.Enum(["plan_complete", "authorized_scope_complete", "cancelled", "superseded", "checkpoint"]),
    cutoffAt: TimestampSchema,
    checkpointIdentityHash: NullableHashSchema,
    cutoffIdentityHash: HashSchema,
  }),
  supersedesEnvelopeHash: NullableHashSchema,
  serialPolicy: Type.Boolean(),
  clock: StrictObject({ quality: Type.Enum(["same_epoch", "mixed_epoch", "unsupported"]), clockEpochHash: NullableHashSchema }),
  coverage: StrictObject({ status: EvaluationMetricStatusSchema, sourceRevisionCount: NonNegativeIntegerSchema, observedRevisionCount: NonNegativeIntegerSchema, missingRevisionCount: NonNegativeIntegerSchema, droppedRevisionCount: NonNegativeIntegerSchema, censoredIntervalCount: NonNegativeIntegerSchema, observerFailureCount: NonNegativeIntegerSchema }),
  invariants: InvariantsV1Schema,
  metrics: MetricSectionsV1Schema,
  postRunPulse: StrictObject({ confidenceFinalState: PulseItemV1Schema, cognitiveEffort: PulseItemV1Schema, interruptionBurden: PulseItemV1Schema }),
});
export type RunEvaluationEnvelopeV1 = Static<typeof RunEvaluationEnvelopeV1Schema>;
export type RunEvaluationEnvelopeCoreV1 = Omit<RunEvaluationEnvelopeV1, "envelopeHash" | "sourceClosures" | "metricSourceBindings" | "creditContextHash">;

export const DogfoodScenarioClassV1Schema = Type.Enum(["independent_fanout", "hidden_constraint", "integration_train", "recovery_sensitive"]);
export const DogfoodExecutionV1Schema = StrictObject({
  executionIdentityHash: HashSchema,
  mode: Type.Enum(["serial", "parallel"]),
  order: Type.Integer({ minimum: 1, maximum: 2 }),
  maxActiveNodes: Type.Integer({ minimum: 1, maximum: 64 }),
});
export type DogfoodExecutionV1 = Static<typeof DogfoodExecutionV1Schema>;
export type DogfoodExecutionCoreV1 = Omit<DogfoodExecutionV1, "executionIdentityHash">;
export const DogfoodPairV1Schema = StrictObject({
  pairIdentityHash: HashSchema,
  scenarioClass: DogfoodScenarioClassV1Schema,
  baselineCommitHash: HashSchema,
  baselineTreeHash: HashSchema,
  planHash: HashSchema,
  oracleHash: HashSchema,
  scriptHash: HashSchema,
  environmentHash: HashSchema,
  evaluationProfileHash: HashSchema,
  modelHash: HashSchema,
  providerHash: HashSchema,
  riskCohortHash: HashSchema,
  executions: Type.Array(DogfoodExecutionV1Schema, { minItems: 2, maxItems: 2 }),
});
export type DogfoodPairV1 = Static<typeof DogfoodPairV1Schema>;
export type DogfoodPairCoreV1 = Omit<DogfoodPairV1, "pairIdentityHash">;
export const DagEvaluationPortfolioV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("dag_evaluation_portfolio"),
  portfolioIdentityHash: HashSchema,
  pairs: Type.Array(DogfoodPairV1Schema, { minItems: 6, maxItems: 6 }),
  excludedRecoveryDrills: Type.Array(Type.Enum(["provider_worker_loss", "conductor_crash_resume", "target_drift_conflict"]), { minItems: 3, maxItems: 3 }),
});
export type DagEvaluationPortfolioV1 = Static<typeof DagEvaluationPortfolioV1Schema>;
export type DagEvaluationPortfolioCoreV1 = Omit<DagEvaluationPortfolioV1, "portfolioIdentityHash">;

export const PairedExecutionResultV1Schema = StrictObject({
  executionIdentityHash: HashSchema,
  envelopeHash: HashSchema,
  cohortHash: HashSchema,
  valid: Type.Boolean(),
  uncompensatedInvariantsPass: Type.Boolean(),
  elapsedMs: NonNegativeNumberSchema,
  usefulWorkMs: NonNegativeNumberSchema,
  reportedCost: Type.Union([NonNegativeNumberSchema, Type.Null()]),
});
export type PairedExecutionResultV1 = Static<typeof PairedExecutionResultV1Schema>;
const PairDeltaV1Schema = StrictObject({
  pairIdentityHash: HashSchema,
  elapsedDifferenceMs: Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }),
  elapsedRatio: NullableNumberSchema,
  usefulWorkDifferenceMs: Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }),
  reportedCostDifference: Type.Union([Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }), Type.Null()]),
  elapsedOutcome: Type.Enum(["win", "tie", "loss"]),
});
export const PairedEvaluationReportV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("paired_evaluation_report"),
  portfolioIdentityHash: HashSchema,
  evaluationProfileHash: HashSchema,
  pairDeltas: Type.Array(PairDeltaV1Schema, { minItems: 6, maxItems: 6 }),
  elapsedSummary: StrictObject({ medianDifferenceMs: Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }), minimumDifferenceMs: Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }), maximumDifferenceMs: Type.Number({ minimum: -Number.MAX_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }), wins: NonNegativeIntegerSchema, ties: NonNegativeIntegerSchema, losses: NonNegativeIntegerSchema }),
});
export type PairedEvaluationReportV1 = Static<typeof PairedEvaluationReportV1Schema>;

export function parseRfc3339UtcNanosecondsV1(value: string): bigint | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
  if (!match) return null;
  const year = Number(match[1]), month = Number(match[2]), day = Number(match[3]);
  const hour = Number(match[4]), minute = Number(match[5]), second = Number(match[6]);
  if (year === 0 || month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59) return null;
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const monthDays = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (day < 1 || day > monthDays[month - 1]) return null;
  const adjustedYear = BigInt(year - (month <= 2 ? 1 : 0));
  const era = adjustedYear / 400n;
  const yearOfEra = adjustedYear - era * 400n;
  const adjustedMonth = BigInt(month + (month > 2 ? -3 : 9));
  const dayOfYear = (153n * adjustedMonth + 2n) / 5n + BigInt(day - 1);
  const dayOfEra = yearOfEra * 365n + yearOfEra / 4n - yearOfEra / 100n + dayOfYear;
  const daysSinceEpoch = era * 146097n + dayOfEra - 719468n;
  const secondsSinceEpoch = daysSinceEpoch * 86400n + BigInt(hour * 3600 + minute * 60 + second);
  const fractionalNanoseconds = BigInt((match[7] ?? "").padEnd(9, "0") || "0");
  return secondsSinceEpoch * 1_000_000_000n + fractionalNanoseconds;
}

export function sourceClosureV1(hashes: readonly string[]): SourceClosureV1 {
  const sorted = [...hashes].sort();
  if (!isSortedUnique(sorted)) throw new Error("Source closure hashes must be unique");
  if (sorted.some((hash) => !/^sha256:[0-9a-f]{64}$/.test(hash))) throw new Error("Source closure contains a malformed hash");
  return { count: sorted.length, hashOfSortedHashes: canonicalHash(sorted) };
}

export function deriveCreditBasisV1(claim: CreditBasisV1 | null, context: CanonicalCreditContextV1): CreditBasisV1 | null {
  const contextIssues = validateCanonicalCreditContext(context);
  if (contextIssues.length) throw new Error(formatIssues("Invalid canonical credit context", contextIssues));
  if (claim === null || schemaIssues(CreditBasisV1Schema, claim).length || !creditBasisAccepted(claim, context)) return null;
  return structuredClone(claim);
}

export function measuredMetric(unit: EvaluationMetricV1["unit"], numerator: number, denominator: number | null = null, observedCount = 1): EvaluationMetricV1 {
  return { status: "measured", unit, numerator, denominator, observedCount, missingCount: 0, censoredCount: 0 };
}
export function unavailableMetric(status: "unsupported_policy" | "not_observed", unit: EvaluationMetricV1["unit"], missingCount = status === "not_observed" ? 1 : 0): EvaluationMetricV1 {
  return { status, unit, numerator: null, denominator: null, observedCount: 0, missingCount, censoredCount: 0 };
}
export function zeroExposureMetric(unit: "ratio"): EvaluationMetricV1 {
  return { status: "zero_exposure", unit, numerator: 0, denominator: 0, observedCount: 0, missingCount: 0, censoredCount: 0 };
}

export function validateEvaluationMetricV1(value: unknown): ValidationResult<EvaluationMetricV1> {
  const issues = schemaIssues(EvaluationMetricV1Schema, value);
  if (!issues.length) validateMetricSemantics(value as EvaluationMetricV1, issues, "");
  return { ok: issues.length === 0, value: issues.length ? undefined : value as EvaluationMetricV1, issues };
}

/**
 * Creates the bounded accumulator used to evaluate or replay a completed run.
 * V1 callers supply that run's final committed credit context at creation; all
 * replayed observations must carry the exact same canonical context and hash.
 */
export function createRunObservationAccumulatorV1(input: Pick<RunObservationAccumulatorV1, "identity" | "source" | "creditContext">): RunObservationAccumulatorV1 {
  const creditContextIssues = validateCanonicalCreditContext(input.creditContext);
  if (creditContextIssues.length) throw new Error(formatIssues("Invalid canonical credit context", creditContextIssues));
  if (input.identity.evaluationProfileHash !== RUN_EVALUATION_PROFILE_HASH_V1) throw new Error("Accumulator evaluation profile hash is not the fixed v1 profile");
  if (input.identity.clockPolicyHash !== RUN_EVALUATION_CLOCK_POLICY_HASH_V1) throw new Error("Accumulator clock policy hash is not the fixed v1 policy");
  const core = {
    schemaVersion: 1 as const,
    kind: "run_observation_accumulator" as const,
    canonicalization: "jcs-v1" as const,
    identity: input.identity,
    source: input.source,
    clockHistory: { initialEpochHash: input.source.clockEpochHash, currentEpochHash: input.source.clockEpochHash, epochChangeCount: 0, monotonicResetCount: 0, unsupportedObservationCount: 0 },
    creditContext: structuredClone(input.creditContext),
    creditContextHash: canonicalHash(input.creditContext),
    open: { readiness: [], active: [], human: [], recovery: [] },
    counters: { readinessIntervals: 0, dispatchWaitIntervals: 0, activeIntervals: 0, humanActiveIntervals: 0, authorityWaitIntervals: 0, recoveryIntervals: 0, falseIndependenceIncidents: 0 },
    sums: { readinessWaitMs: 0, dispatchWaitMs: 0, humanActiveMs: 0, authorityWaitMs: 0, recoveryMs: 0, falseIndependenceWasteMs: 0 },
    histograms: { readinessWait: [0, 0, 0, 0, 0, 0], dispatchWait: [0, 0, 0, 0, 0, 0], recovery: [0, 0, 0, 0, 0, 0] },
    integrals: { autonomousElapsedMs: 0, allOperationWorkMs: 0, creditableWorkMs: 0, usefulOverlapMs: 0, parallelOpportunityMs: 0 },
    coverage: { observedRevisionCount: 0, droppedRevisionCount: 0, missingRevisionCount: 0, censoredIntervalCount: 0, observerFailureCount: 0, revisionGaps: [], revisionGapOverflowCount: 0 },
  };
  return sealAccumulator(core);
}

export function assertAccumulatorObservationV1(value: unknown): asserts value is AccumulatorObservationV1 {
  const observationIssues = schemaIssues(AccumulatorObservationV1Schema, value);
  if (!observationIssues.length) {
    const observation = value as AccumulatorObservationV1;
    validateEvaluationTimestampFields(observation, observationIssues);
    assertSortedUniqueHashes(observation.readyOperationHashes, observationIssues, "/readyOperationHashes");
    assertSortedUniqueHashes(observation.reservedOperationHashes, observationIssues, "/reservedOperationHashes");
    const activeHashes = observation.activeOperations.map(({ operationHash }) => operationHash);
    assertSortedUniqueHashes(activeHashes, observationIssues, "/activeOperations");
    assertDisjointHashes([observation.readyOperationHashes, observation.reservedOperationHashes, activeHashes], observationIssues, "/operations");
    assertSortedUniqueHashes(observation.humanActiveHashes, observationIssues, "/humanActiveHashes");
    assertSortedUniqueHashes(observation.authorityWaitHashes, observationIssues, "/authorityWaitHashes");
    assertDisjointHashes([observation.humanActiveHashes, observation.authorityWaitHashes], observationIssues, "/human");
    assertSortedUniqueHashes(observation.recoveryHashes, observationIssues, "/recoveryHashes");
    const incidentKeys = observation.falseIndependenceIncidents.map(({ findingHash, currentDispositionHash }) => `${findingHash}:${currentDispositionHash}`);
    pushIssue(observationIssues, "/falseIndependenceIncidents", isSortedUnique(incidentKeys), "must be sorted and unique by exact finding/disposition identity");
    const incidentOperationHashes: string[] = [];
    for (const [index, incident] of observation.falseIndependenceIncidents.entries()) {
      assertSortedUniqueHashes(incident.operationHashes, observationIssues, `/falseIndependenceIncidents/${index}/operationHashes`);
      incidentOperationHashes.push(...incident.operationHashes);
    }
    pushIssue(observationIssues, "/falseIndependenceIncidents", new Set(incidentOperationHashes).size === incidentOperationHashes.length, "an operation interval can contribute waste to at most one incident per fold");
    const creditContextIssues = validateCanonicalCreditContext(observation.creditContext);
    observationIssues.push(...creditContextIssues.map((issue) => ({ ...issue, path: `/creditContext${issue.path}` })));
    if (!creditContextIssues.length) {
      for (const [index, active] of observation.activeOperations.entries()) if (active.creditBasis !== null) pushIssue(observationIssues, `/activeOperations/${index}/creditBasis`, creditBasisAccepted(active.creditBasis, observation.creditContext), "must reference exact accepted evidence/integration lineage or an exact actionable finding with its current disposition");
      for (const [index, incident] of observation.falseIndependenceIncidents.entries()) pushIssue(observationIssues, `/falseIndependenceIncidents/${index}`, observation.creditContext.actionableFindingDispositions.some(({ findingHash, currentDispositionHash }) => findingHash === incident.findingHash && currentDispositionHash === incident.currentDispositionHash), "must reference an exact actionable finding and its current disposition from canonical context");
    }
  }
  if (observationIssues.length) throw new Error(formatIssues("Invalid observation", observationIssues));
}

export function foldAccumulatorObservationV1(accumulator: RunObservationAccumulatorV1, observation: AccumulatorObservationV1): RunObservationAccumulatorV1 {
  const validAccumulator = validateRunObservationAccumulatorV1(accumulator);
  if (!validAccumulator.ok) throw new Error(formatIssues("Invalid accumulator", validAccumulator.issues));
  assertAccumulatorObservationV1(observation);
  const observationCreditContextHash = canonicalHash(observation.creditContext);
  if (canonicalStringify(observation.creditContext) !== canonicalStringify(accumulator.creditContext) || observationCreditContextHash !== accumulator.creditContextHash) {
    throw new Error("Invalid observation:\n- /creditContext: canonical bytes and hash must equal the accumulator's immutable v1 credit context");
  }
  if (observation.revision <= accumulator.source.revision) return accumulator;

  const next = structuredClone(accumulator) as Mutable<RunObservationAccumulatorV1>;
  delete (next as Partial<Mutable<RunObservationAccumulatorV1>>).accumulatorHash;
  const epochChanged = observation.clockEpochHash !== accumulator.source.clockEpochHash;
  const monotonicReset = observation.monotonicTickMs < accumulator.source.monotonicTickMs;
  const unsupportedClock = observation.clockStatus === "unsupported";
  const sameClock = !unsupportedClock && !epochChanged && !monotonicReset;
  const skipped = observation.revision - accumulator.source.revision - 1;
  const contiguousClockInterval = sameClock && skipped === 0;
  if (epochChanged) next.clockHistory.epochChangeCount += 1;
  if (monotonicReset) next.clockHistory.monotonicResetCount += 1;
  if (unsupportedClock) next.clockHistory.unsupportedObservationCount += 1;
  next.clockHistory.currentEpochHash = observation.clockEpochHash;
  next.coverage.droppedRevisionCount += observation.droppedBefore;
  next.coverage.observerFailureCount += observation.observerFailuresBefore;
  if (skipped > 0) {
    next.coverage.missingRevisionCount += skipped;
    addRevisionGap(next.coverage, accumulator.source.revision + 1, observation.revision - 1);
  }
  if (contiguousClockInterval) {
    const elapsed = observation.monotonicTickMs - accumulator.source.monotonicTickMs;
    const operations = accumulator.open.active.filter(({ class: intervalClass }) => intervalClass === "operation");
    const all = operations.length;
    const creditable = operations.filter(({ creditBasis }) => creditBasis !== null && creditBasisAccepted(creditBasis, accumulator.creditContext)).length;
    const priorOperationHashes = new Set(operations.map(({ intervalHash }) => intervalHash));
    for (const [index, incident] of observation.falseIndependenceIncidents.entries()) {
      if (incident.wasteStartMonotonicMs < accumulator.source.monotonicTickMs || incident.wasteStartMonotonicMs > observation.monotonicTickMs) throw new Error(`Invalid observation:\n- /falseIndependenceIncidents/${index}/wasteStartMonotonicMs: must fall within the exact folded clock interval`);
      if (incident.operationHashes.some((hash) => !priorOperationHashes.has(hash))) throw new Error(`Invalid observation:\n- /falseIndependenceIncidents/${index}/operationHashes: every wasted operation must be an exact open operation fold input`);
      next.counters.falseIndependenceIncidents += 1;
      next.sums.falseIndependenceWasteMs += (observation.monotonicTickMs - incident.wasteStartMonotonicMs) * incident.operationHashes.length;
    }
    const ready = accumulator.open.readiness.length + accumulator.open.active.filter(({ class: intervalClass }) => intervalClass === "reserved_dispatch").length;
    next.integrals.autonomousElapsedMs += elapsed;
    next.integrals.allOperationWorkMs += elapsed * all;
    next.integrals.creditableWorkMs += elapsed * creditable;
    next.integrals.usefulOverlapMs += elapsed * Math.max(0, creditable - 1);
    next.integrals.parallelOpportunityMs += elapsed * Math.max(0, all + ready - 1);
    next.open.readiness = reconcileIntervals(next.open.readiness, observation.readyOperationHashes, "readiness_lane_admit", observation, next, "readinessWaitMs", "readinessWait", "readinessIntervals");
    next.open.active = reconcileActiveIntervals(next.open.active, observation.reservedOperationHashes, observation.activeOperations, observation, next);
    next.open.human = reconcileIntervals(next.open.human, [...observation.humanActiveHashes, ...observation.authorityWaitHashes].sort(), "human_active", observation, next, null, null, null, new Set(observation.authorityWaitHashes));
    next.open.recovery = reconcileIntervals(next.open.recovery, observation.recoveryHashes, "recovery", observation, next, "recoveryMs", "recovery", "recoveryIntervals");
  } else {
    if (observation.falseIndependenceIncidents.length > 0) throw new Error("Invalid observation:\n- /falseIndependenceIncidents: incident waste requires an exact contiguous same-clock fold");
    censorOpenIntervalsAtLastObservedBoundary(accumulator, next);
    reopenObservationState(observation, next);
  }
  next.coverage.observedRevisionCount += 1;
  next.source = { revision: observation.revision, snapshotHash: observation.snapshotHash, observedAt: observation.observedAt, clockEpochHash: observation.clockEpochHash, monotonicTickMs: observation.monotonicTickMs };
  return sealAccumulator(next as Omit<RunObservationAccumulatorV1, "accumulatorHash">);
}

export function sweepQualityConditionedIntervalsV1(operationIntervals: readonly QualityConditionedIntervalV1[], readinessIntervals: readonly Omit<QualityConditionedIntervalV1, "creditBasis">[], creditContext: CanonicalCreditContextV1): QualityConditionedIntervalTotalsV1 {
  const operationIssues = operationIntervals.flatMap((interval, index) => schemaIssues(QualityConditionedIntervalV1Schema, interval).map((issue) => ({ ...issue, path: `/operationIntervals/${index}${issue.path}` })));
  const readinessSchema = StrictObject({ intervalHash: HashSchema, startMonotonicMs: NonNegativeNumberSchema, endMonotonicMs: NonNegativeNumberSchema });
  const readinessIssues = readinessIntervals.flatMap((interval, index) => schemaIssues(readinessSchema, interval).map((issue) => ({ ...issue, path: `/readinessIntervals/${index}${issue.path}` })));
  const creditContextIssues = validateCanonicalCreditContext(creditContext);
  const issues = [...operationIssues, ...readinessIssues, ...creditContextIssues.map((issue) => ({ ...issue, path: `/creditContext${issue.path}` }))];
  if (!creditContextIssues.length) operationIntervals.forEach((interval, index) => {
    if (interval.creditBasis !== null) pushIssue(issues, `/operationIntervals/${index}/creditBasis`, creditBasisAccepted(interval.creditBasis, creditContext), "must be derived from exact canonical credit context");
  });
  const allIntervals = [...operationIntervals, ...readinessIntervals];
  pushIssue(issues, "/intervals", new Set(allIntervals.map(({ intervalHash }) => intervalHash)).size === allIntervals.length, "interval hashes must be unique across operation and readiness inputs");
  allIntervals.forEach((interval, index) => pushIssue(issues, `/intervals/${index}`, interval.startMonotonicMs <= interval.endMonotonicMs, "interval start must not follow its end"));
  if (issues.length) throw new Error(formatIssues("Invalid quality-conditioned intervals", issues));
  if (allIntervals.length === 0) return { autonomousElapsedMs: 0, allOperationWorkMs: 0, creditableWorkMs: 0, usefulOverlapMs: 0, parallelOpportunityMs: 0 };
  const events: { time: number; priority: number; all: number; creditable: number; ready: number }[] = [];
  for (const interval of operationIntervals) {
    events.push({ time: interval.startMonotonicMs, priority: 1, all: 1, creditable: interval.creditBasis === null ? 0 : 1, ready: 0 });
    events.push({ time: interval.endMonotonicMs, priority: 0, all: -1, creditable: interval.creditBasis === null ? 0 : -1, ready: 0 });
  }
  for (const interval of readinessIntervals) {
    events.push({ time: interval.startMonotonicMs, priority: 1, all: 0, creditable: 0, ready: 1 });
    events.push({ time: interval.endMonotonicMs, priority: 0, all: 0, creditable: 0, ready: -1 });
  }
  events.sort((a, b) => a.time - b.time || a.priority - b.priority);
  let previous = events[0].time, all = 0, creditable = 0, ready = 0;
  const totals = { autonomousElapsedMs: events.at(-1)!.time - events[0].time, allOperationWorkMs: 0, creditableWorkMs: 0, usefulOverlapMs: 0, parallelOpportunityMs: 0 };
  for (let index = 0; index < events.length;) {
    const time = events[index].time;
    const elapsed = time - previous;
    totals.allOperationWorkMs += elapsed * all;
    totals.creditableWorkMs += elapsed * creditable;
    totals.usefulOverlapMs += elapsed * Math.max(0, creditable - 1);
    totals.parallelOpportunityMs += elapsed * Math.max(0, all + ready - 1);
    while (index < events.length && events[index].time === time) {
      all += events[index].all; creditable += events[index].creditable; ready += events[index].ready; index += 1;
    }
    previous = time;
  }
  return totals;
}

export function accumulatorClockV1(accumulator: RunObservationAccumulatorV1): RunEvaluationEnvelopeV1["clock"] {
  if (accumulator.clockHistory.unsupportedObservationCount > 0) return { quality: "unsupported", clockEpochHash: null };
  if (accumulator.clockHistory.epochChangeCount > 0 || accumulator.clockHistory.monotonicResetCount > 0) return { quality: "mixed_epoch", clockEpochHash: null };
  return { quality: "same_epoch", clockEpochHash: accumulator.clockHistory.currentEpochHash };
}

export function usefulParallelismMetricsV1(accumulator: RunObservationAccumulatorV1, options: { serialPolicy: boolean; rightCensored: boolean; clockQuality?: RunEvaluationEnvelopeV1["clock"]["quality"] }): RunEvaluationEnvelopeV1["metrics"]["usefulParallelism"] {
  const coverage = {
    observedRevisionCount: accumulator.coverage.observedRevisionCount,
    missingRevisionCount: accumulator.coverage.missingRevisionCount,
    droppedRevisionCount: accumulator.coverage.droppedRevisionCount,
    observerFailureCount: accumulator.coverage.observerFailureCount,
    censoredIntervalCount: accumulator.coverage.censoredIntervalCount,
  };
  const condition = (metric: EvaluationMetricV1, serialUnsupported = false): EvaluationMetricV1 => conditionTimingMetricV1(metric, {
    clockQuality: options.clockQuality ?? "same_epoch",
    rightCensored: options.rightCensored,
    serialUnsupported: options.serialPolicy && serialUnsupported,
    coverage,
  });
  return {
    usefulWork: condition(measuredMetric("milliseconds", accumulator.integrals.creditableWorkMs)),
    usefulAverageConcurrency: condition(measuredMetric("ratio", accumulator.integrals.creditableWorkMs, accumulator.integrals.autonomousElapsedMs)),
    usefulOverlapArea: condition(measuredMetric("milliseconds", accumulator.integrals.usefulOverlapMs)),
    parallelOpportunityArea: condition(measuredMetric("milliseconds", accumulator.integrals.parallelOpportunityMs), true),
    opportunityCapture: condition(measuredMetric("ratio", accumulator.integrals.usefulOverlapMs, accumulator.integrals.parallelOpportunityMs), true),
    allOperationTime: condition(measuredMetric("milliseconds", accumulator.integrals.allOperationWorkMs)),
    workEfficiency: condition(measuredMetric("ratio", accumulator.integrals.creditableWorkMs, accumulator.integrals.allOperationWorkMs)),
  };
}

export function accumulatorTelemetryV1(accumulator: RunObservationAccumulatorV1): RunEvaluationEnvelopeV1["metrics"]["accumulatorTelemetry"] {
  return {
    counters: structuredClone(accumulator.counters),
    sums: structuredClone(accumulator.sums),
    histograms: structuredClone(accumulator.histograms),
    integrals: structuredClone(accumulator.integrals),
    coverage: structuredClone(accumulator.coverage),
  };
}

export function accumulatorDerivedMetricsV1(
  accumulator: RunObservationAccumulatorV1,
  options: { serialPolicy: boolean; rightCensored: boolean },
): Pick<RunEvaluationEnvelopeV1["metrics"], "accumulatorTelemetry" | "timing" | "waitHistograms" | "usefulParallelism" | "instrumentation"> & {
  humanAttention: Pick<RunEvaluationEnvelopeV1["metrics"]["humanAttention"], "activeMinutes" | "activeIntervals" | "authorityWait" | "authorityWaitIntervals">;
  findings: Pick<RunEvaluationEnvelopeV1["metrics"]["findings"], "falseIndependenceIncidents" | "falseIndependenceWaste">;
} {
  const clockQuality = accumulatorClockV1(accumulator).quality;
  const coverage = {
    observedRevisionCount: accumulator.coverage.observedRevisionCount,
    missingRevisionCount: accumulator.coverage.missingRevisionCount,
    droppedRevisionCount: accumulator.coverage.droppedRevisionCount,
    observerFailureCount: accumulator.coverage.observerFailureCount,
    censoredIntervalCount: accumulator.coverage.censoredIntervalCount,
  };
  const condition = (metric: EvaluationMetricV1, serialUnsupported = false): EvaluationMetricV1 => conditionTimingMetricV1(metric, {
    clockQuality,
    rightCensored: options.rightCensored,
    serialUnsupported: options.serialPolicy && serialUnsupported,
    coverage,
  });
  const histogram = (buckets: readonly number[]): EvaluationMetricV1[] => buckets.map((value) => condition(measuredMetric("count", value)));
  return {
    accumulatorTelemetry: accumulatorTelemetryV1(accumulator),
    timing: {
      autonomousElapsed: condition(measuredMetric("milliseconds", accumulator.integrals.autonomousElapsedMs)),
      readinessWait: condition(measuredMetric("milliseconds", accumulator.sums.readinessWaitMs)),
      readinessWaitIntervals: condition(measuredMetric("count", accumulator.counters.readinessIntervals)),
      dispatchWait: condition(measuredMetric("milliseconds", accumulator.sums.dispatchWaitMs)),
      dispatchWaitIntervals: condition(measuredMetric("count", accumulator.counters.dispatchWaitIntervals)),
      recovery: condition(measuredMetric("milliseconds", accumulator.sums.recoveryMs)),
      recoveryIntervals: condition(measuredMetric("count", accumulator.counters.recoveryIntervals)),
    },
    waitHistograms: {
      readinessWait: histogram(accumulator.histograms.readinessWait),
      dispatchWait: histogram(accumulator.histograms.dispatchWait),
      recovery: histogram(accumulator.histograms.recovery),
    },
    usefulParallelism: usefulParallelismMetricsV1(accumulator, { ...options, clockQuality }),
    humanAttention: {
      activeMinutes: condition(measuredMetric("milliseconds", accumulator.sums.humanActiveMs)),
      activeIntervals: condition(measuredMetric("count", accumulator.counters.humanActiveIntervals)),
      authorityWait: condition(measuredMetric("milliseconds", accumulator.sums.authorityWaitMs)),
      authorityWaitIntervals: condition(measuredMetric("count", accumulator.counters.authorityWaitIntervals)),
    },
    findings: {
      falseIndependenceIncidents: condition(measuredMetric("count", accumulator.counters.falseIndependenceIncidents)),
      falseIndependenceWaste: condition(measuredMetric("milliseconds", accumulator.sums.falseIndependenceWasteMs)),
    },
    instrumentation: {
      observedRevisions: measuredMetric("count", accumulator.coverage.observedRevisionCount),
      droppedRevisions: measuredMetric("count", accumulator.coverage.droppedRevisionCount),
      missingRevisions: measuredMetric("count", accumulator.coverage.missingRevisionCount),
      censoredIntervals: measuredMetric("count", accumulator.coverage.censoredIntervalCount),
    },
  };
}

interface TimingMetricConditionV1 {
  clockQuality: RunEvaluationEnvelopeV1["clock"]["quality"];
  rightCensored: boolean;
  serialUnsupported: boolean;
  coverage: Pick<RunEvaluationEnvelopeV1["coverage"], "observedRevisionCount" | "missingRevisionCount" | "droppedRevisionCount" | "observerFailureCount" | "censoredIntervalCount">;
}

function conditionTimingMetricV1(metric: EvaluationMetricV1, condition: TimingMetricConditionV1): EvaluationMetricV1 {
  const missingCount = condition.coverage.missingRevisionCount + condition.coverage.droppedRevisionCount + condition.coverage.observerFailureCount;
  const observedCount = condition.coverage.observedRevisionCount;
  const censoredCount = Math.max(condition.coverage.censoredIntervalCount, condition.rightCensored || condition.clockQuality === "mixed_epoch" ? 1 : 0);
  if (condition.clockQuality === "unsupported" || condition.serialUnsupported) return unavailableMetric("unsupported_policy", metric.unit);
  if (censoredCount > 0) {
    const hasUsableValue = observedCount > 0 && (metric.unit !== "ratio" || (metric.numerator !== null && metric.denominator !== null && metric.denominator > 0));
    return { status: "censored", unit: metric.unit, numerator: hasUsableValue ? metric.numerator : null, denominator: hasUsableValue ? metric.denominator : null, observedCount, missingCount, censoredCount };
  }
  if (observedCount === 0) return unavailableMetric("not_observed", metric.unit, Math.max(1, missingCount));
  if (missingCount > 0) {
    const hasUsableValue = metric.unit !== "ratio" || (metric.numerator !== null && metric.denominator !== null && metric.denominator > 0);
    return { status: "partial_coverage", unit: metric.unit, numerator: hasUsableValue ? metric.numerator : null, denominator: hasUsableValue ? metric.denominator : null, observedCount, missingCount, censoredCount: 0 };
  }
  if (metric.unit === "ratio" && metric.numerator === 0 && metric.denominator === 0) return zeroExposureMetric("ratio");
  return { status: "measured", unit: metric.unit, numerator: metric.numerator, denominator: metric.denominator, observedCount, missingCount: 0, censoredCount: 0 };
}

function expectedEnvelopeCoverageStatusV1(envelope: Pick<RunEvaluationEnvelopeV1, "clock" | "coverage" | "cutoff">): RunEvaluationEnvelopeV1["coverage"]["status"] {
  if (envelope.coverage.censoredIntervalCount > 0 || envelope.cutoff.kind === "right_censored" || envelope.clock.quality === "mixed_epoch") return "censored";
  const missing = envelope.coverage.missingRevisionCount + envelope.coverage.droppedRevisionCount + envelope.coverage.observerFailureCount;
  if (envelope.coverage.observedRevisionCount === 0) return "not_observed";
  return missing > 0 ? "partial_coverage" : "measured";
}

function deriveEnvelopeTimingMetricsV1(envelope: Mutable<RunEvaluationEnvelopeV1>): void {
  envelope.coverage.status = expectedEnvelopeCoverageStatusV1(envelope);
  const condition = (metric: EvaluationMetricV1, serialUnsupported = false): EvaluationMetricV1 => conditionTimingMetricV1(metric, {
    clockQuality: envelope.clock.quality,
    rightCensored: envelope.cutoff.kind === "right_censored",
    serialUnsupported: envelope.serialPolicy && serialUnsupported,
    coverage: envelope.coverage,
  });
  for (const name of Object.keys(envelope.metrics.timing) as Array<keyof RunEvaluationEnvelopeV1["metrics"]["timing"]>) envelope.metrics.timing[name] = condition(envelope.metrics.timing[name]);
  for (const histogram of Object.values(envelope.metrics.waitHistograms)) {
    for (let index = 0; index < histogram.length; index += 1) histogram[index] = condition(histogram[index]);
  }
  envelope.metrics.humanAttention.activeMinutes = condition(envelope.metrics.humanAttention.activeMinutes);
  envelope.metrics.humanAttention.activeIntervals = condition(envelope.metrics.humanAttention.activeIntervals);
  envelope.metrics.humanAttention.authorityWait = condition(envelope.metrics.humanAttention.authorityWait);
  envelope.metrics.humanAttention.authorityWaitIntervals = condition(envelope.metrics.humanAttention.authorityWaitIntervals);
  envelope.metrics.findings.falseIndependenceIncidents = condition(envelope.metrics.findings.falseIndependenceIncidents);
  envelope.metrics.findings.falseIndependenceWaste = condition(envelope.metrics.findings.falseIndependenceWaste);
  envelope.metrics.usefulParallelism.usefulWork = condition(envelope.metrics.usefulParallelism.usefulWork);
  envelope.metrics.usefulParallelism.usefulAverageConcurrency = condition(envelope.metrics.usefulParallelism.usefulAverageConcurrency);
  envelope.metrics.usefulParallelism.usefulOverlapArea = condition(envelope.metrics.usefulParallelism.usefulOverlapArea);
  envelope.metrics.usefulParallelism.parallelOpportunityArea = condition(envelope.metrics.usefulParallelism.parallelOpportunityArea, true);
  envelope.metrics.usefulParallelism.opportunityCapture = condition(envelope.metrics.usefulParallelism.opportunityCapture, true);
  envelope.metrics.usefulParallelism.allOperationTime = condition(envelope.metrics.usefulParallelism.allOperationTime);
  envelope.metrics.usefulParallelism.workEfficiency = condition(envelope.metrics.usefulParallelism.workEfficiency);
  envelope.metrics.instrumentation.observedRevisions = measuredMetric("count", envelope.coverage.observedRevisionCount);
  envelope.metrics.instrumentation.droppedRevisions = measuredMetric("count", envelope.coverage.droppedRevisionCount);
  envelope.metrics.instrumentation.missingRevisions = measuredMetric("count", envelope.coverage.missingRevisionCount);
  envelope.metrics.instrumentation.censoredIntervals = measuredMetric("count", envelope.coverage.censoredIntervalCount);
}

export function buildRunEvaluationEnvelopeV1(core: RunEvaluationEnvelopeCoreV1): RunEvaluationEnvelopeV1 {
  if (Object.prototype.hasOwnProperty.call(core, "metricSourceBindings")) throw new Error("Caller-supplied metric source binding claims are unsupported; bindings are derived from immutable source hash sets");
  const cloned = structuredClone(core);
  const sourceClosures = sourceClosuresFromHashSetsV1(cloned.sourceHashes);
  const metricSourceBindings = metricSourceBindingsFromEnvelopeV1(cloned);
  const envelope = { ...cloned, sourceClosures, metricSourceBindings, creditContextHash: canonicalHash(cloned.creditContext), envelopeHash: "" } as Mutable<RunEvaluationEnvelopeV1>;
  deriveEnvelopeTimingMetricsV1(envelope);
  envelope.envelopeHash = hashWithoutField(envelope as unknown as Record<string, unknown>, "envelopeHash");
  const result = validateRunEvaluationEnvelopeV1(envelope);
  if (!result.ok) throw new Error(formatIssues("Invalid run evaluation envelope", result.issues));
  return envelope;
}
export function runEvaluationEnvelopeHashV1(value: RunEvaluationEnvelopeV1): string {
  return hashWithoutField(value as unknown as Record<string, unknown>, "envelopeHash");
}

export function parseRunObservationAccumulatorV1(text: string): RunObservationAccumulatorV1 {
  const value = parseStrictJson(text);
  assertRunObservationAccumulatorV1(value);
  return value;
}
export function assertRunObservationAccumulatorV1(value: unknown): asserts value is RunObservationAccumulatorV1 {
  const result = validateRunObservationAccumulatorV1(value);
  if (!result.ok) throw new Error(formatIssues("Invalid RunObservationAccumulatorV1", result.issues));
}
export function validateRunObservationAccumulatorV1(value: unknown): ValidationResult<RunObservationAccumulatorV1> {
  const issues = schemaIssues(RunObservationAccumulatorV1Schema, value);
  if (!issues.length) {
    const accumulator = value as RunObservationAccumulatorV1;
    validateEvaluationTimestampFields(accumulator, issues);
    pushIssue(issues, "/identity/evaluationProfileHash", accumulator.identity.evaluationProfileHash === RUN_EVALUATION_PROFILE_HASH_V1, "must equal the fixed v1 evaluation profile hash");
    pushIssue(issues, "/identity/clockPolicyHash", accumulator.identity.clockPolicyHash === RUN_EVALUATION_CLOCK_POLICY_HASH_V1, "must equal the fixed v1 clock policy hash");
    pushIssue(issues, "/accumulatorHash", accumulator.accumulatorHash === hashWithoutField(accumulator as unknown as Record<string, unknown>, "accumulatorHash"), "must match canonical content excluding accumulatorHash");
    const creditContextIssues = validateCanonicalCreditContext(accumulator.creditContext);
    issues.push(...creditContextIssues.map((issue) => ({ ...issue, path: `/creditContext${issue.path}` })));
    pushIssue(issues, "/creditContextHash", accumulator.creditContextHash === canonicalHash(accumulator.creditContext), "must commit the exact bounded canonical credit context");
    pushIssue(issues, "/clockHistory/currentEpochHash", accumulator.clockHistory.currentEpochHash === accumulator.source.clockEpochHash, "must independently track the current source clock epoch");
    pushIssue(issues, "/clockHistory/epochChangeCount", accumulator.clockHistory.epochChangeCount <= accumulator.coverage.observedRevisionCount, "cannot exceed observed clock-bearing revisions");
    pushIssue(issues, "/clockHistory/monotonicResetCount", accumulator.clockHistory.monotonicResetCount <= accumulator.coverage.observedRevisionCount, "cannot exceed observed clock-bearing revisions");
    pushIssue(issues, "/clockHistory/unsupportedObservationCount", accumulator.clockHistory.unsupportedObservationCount <= accumulator.coverage.observedRevisionCount, "cannot exceed observed clock-bearing revisions");
    if (accumulator.clockHistory.epochChangeCount === 0) pushIssue(issues, "/clockHistory/initialEpochHash", accumulator.clockHistory.initialEpochHash === accumulator.clockHistory.currentEpochHash, "must equal currentEpochHash when no epoch change has been observed");
    for (const [name, intervals] of Object.entries(accumulator.open)) {
      const hashes = intervals.map(({ intervalHash }) => intervalHash);
      pushIssue(issues, `/open/${name}`, isSortedUnique(hashes), "must be sorted and unique by intervalHash");
      for (const [index, interval] of intervals.entries()) {
        pushIssue(issues, `/open/${name}/${index}`, interval.clockEpochHash === accumulator.source.clockEpochHash && interval.openedAtMonotonicMs <= accumulator.source.monotonicTickMs, "open interval must share the current clock epoch and not start in the future");
        if (interval.creditBasis !== null) pushIssue(issues, `/open/${name}/${index}/creditBasis`, creditContextIssues.length === 0 && creditBasisAccepted(interval.creditBasis, accumulator.creditContext), "must be bound to the accumulator's exact committed canonical credit context");
      }
    }
    let previousEnd = -1;
    let representedMissingRevisions = 0;
    accumulator.coverage.revisionGaps.forEach((gap, index) => {
      const validGap = gap.firstRevision <= gap.lastRevision && gap.firstRevision > previousEnd + 1 && gap.lastRevision <= accumulator.source.revision;
      pushIssue(issues, `/coverage/revisionGaps/${index}`, validGap, "revision gaps must be sorted, non-overlapping, merged, and closed by the current source revision");
      if (validGap) representedMissingRevisions += gap.lastRevision - gap.firstRevision + 1;
      previousEnd = gap.lastRevision;
    });
    pushIssue(issues, "/coverage/observedRevisionCount", accumulator.coverage.observedRevisionCount <= accumulator.source.revision, "cannot exceed the exact source revision closure");
    pushIssue(issues, "/coverage/missingRevisionCount", accumulator.coverage.observedRevisionCount + accumulator.coverage.missingRevisionCount <= accumulator.source.revision, "observed and missing revisions cannot exceed the exact source revision closure");
    pushIssue(issues, "/coverage/revisionGaps", representedMissingRevisions <= accumulator.coverage.missingRevisionCount, "represented revision gaps cannot exceed the missing revision count");
    pushIssue(issues, "/coverage/revisionGapOverflowCount", accumulator.coverage.revisionGapOverflowCount <= accumulator.coverage.missingRevisionCount - representedMissingRevisions, "overflowed gap coverage cannot exceed unrepresented missing revisions");
    pushIssue(issues, "/integrals/creditableWorkMs", accumulator.integrals.creditableWorkMs <= accumulator.integrals.allOperationWorkMs, "creditable work cannot exceed all operation work");
    pushIssue(issues, "/integrals/usefulOverlapMs", accumulator.integrals.usefulOverlapMs <= accumulator.integrals.creditableWorkMs && accumulator.integrals.usefulOverlapMs <= accumulator.integrals.parallelOpportunityMs && accumulator.integrals.creditableWorkMs - accumulator.integrals.usefulOverlapMs <= accumulator.integrals.autonomousElapsedMs, "useful overlap must remain within useful work and opportunity while non-overlapped useful work remains within elapsed exposure");
    if (accumulator.integrals.autonomousElapsedMs === 0) pushIssue(issues, "/integrals", accumulator.integrals.allOperationWorkMs === 0 && accumulator.integrals.creditableWorkMs === 0 && accumulator.integrals.usefulOverlapMs === 0 && accumulator.integrals.parallelOpportunityMs === 0, "zero elapsed exposure requires zero operation, useful, overlap, and opportunity areas");
  }
  return { ok: issues.length === 0, value: issues.length ? undefined : value as RunObservationAccumulatorV1, issues };
}

export function parseRunEvaluationEnvelopeV1(text: string): RunEvaluationEnvelopeV1 {
  const value = parseStrictJson(text);
  assertRunEvaluationEnvelopeV1(value);
  return value;
}
export function assertRunEvaluationEnvelopeV1(value: unknown): asserts value is RunEvaluationEnvelopeV1 {
  const result = validateRunEvaluationEnvelopeV1(value);
  if (!result.ok) throw new Error(formatIssues("Invalid RunEvaluationEnvelopeV1", result.issues));
}
export function validateRunEvaluationEnvelopeV1(value: unknown): ValidationResult<RunEvaluationEnvelopeV1> {
  const issues = schemaIssues(RunEvaluationEnvelopeV1Schema, value);
  if (!issues.length) {
    const envelope = value as RunEvaluationEnvelopeV1;
    validateEvaluationTimestampFields(envelope, issues);
    pushIssue(issues, "/evaluationProfile/profileHash", envelope.evaluationProfile.profileHash === RUN_EVALUATION_PROFILE_HASH_V1, "must equal the fixed v1 evaluation profile hash");
    pushIssue(issues, "/envelopeHash", envelope.envelopeHash === runEvaluationEnvelopeHashV1(envelope), "must match canonical content excluding envelopeHash");
    const creditContextIssues = validateCanonicalCreditContext(envelope.creditContext);
    issues.push(...creditContextIssues.map((issue) => ({ ...issue, path: `/creditContext${issue.path}` })));
    pushIssue(issues, "/creditContextHash", envelope.creditContextHash === canonicalHash(envelope.creditContext), "must commit the exact bounded canonical credit context");
    for (const [name, hashes] of Object.entries(envelope.sourceHashes)) {
      const exactSet = isSortedUnique(hashes);
      pushIssue(issues, `/sourceHashes/${name}`, exactSet, "must be an exact sorted unique source hash set");
      if (exactSet) pushIssue(issues, `/sourceClosures/${name}`, canonicalStringify(envelope.sourceClosures[name as keyof RunEvaluationEnvelopeV1["sourceClosures"]]) === canonicalStringify(sourceClosureV1(hashes)), "must be computed from the exact source hash set");
    }
    try {
      const exactMetricBindings = metricSourceBindingsFromEnvelopeV1(envelope);
      for (const name of Object.keys(exactMetricBindings) as Array<keyof typeof exactMetricBindings>) {
        const binding = envelope.metricSourceBindings[name];
        pushIssue(issues, `/metricSourceBindings/${name}/sourceHashes`, isSortedUnique(binding.sourceHashes), "must be a sorted unique, non-empty, family-specific source set");
        pushIssue(issues, `/metricSourceBindings/${name}`, canonicalStringify(binding) === canonicalStringify(exactMetricBindings[name]), "must be the exact complete family-specific source binding and closure");
      }
    } catch (error) {
      pushIssue(issues, "/metricSourceBindings", false, error instanceof Error ? error.message : "cannot derive exact metric-family source bindings");
    }
    validateEnvelopeSourceLineage(envelope, issues);
    if (!creditContextIssues.length) {
      envelope.attribution.creditedOperations.forEach(({ basis }, index) => pushIssue(issues, `/attribution/creditedOperations/${index}/basis`, creditBasisAccepted(basis, envelope.creditContext), "must be an exact tuple in the canonical credit context"));
      envelope.attribution.falseIndependenceIncidents.forEach((incident, index) => pushIssue(issues, `/attribution/falseIndependenceIncidents/${index}`, creditBasisAccepted({ kind: "actionable_finding_disposition", findingHash: incident.findingHash, currentDispositionHash: incident.currentDispositionHash }, envelope.creditContext), "must use the unique current finding disposition in canonical credit context"));
    }
    const expectedCutoffHash = canonicalHash({ identity: envelope.identity, source: { revision: envelope.source.revision, snapshotHash: envelope.source.snapshotHash, accumulatorHash: envelope.source.accumulatorHash }, cutoffAt: envelope.cutoff.cutoffAt, kind: envelope.cutoff.kind, class: envelope.cutoff.class, checkpointIdentityHash: envelope.cutoff.checkpointIdentityHash });
    pushIssue(issues, "/cutoff/cutoffIdentityHash", envelope.cutoff.cutoffIdentityHash === expectedCutoffHash, "must bind exact run, source accumulator, and cutoff identity");
    pushIssue(issues, "/cutoff/class", envelope.cutoff.kind === "terminal" ? envelope.cutoff.class !== "checkpoint" : envelope.cutoff.class === "checkpoint", "terminal cutoffs cannot be checkpoints and right-censored cutoffs must be checkpoints");
    pushIssue(issues, "/cutoff/checkpointIdentityHash", envelope.cutoff.kind === "terminal" ? envelope.cutoff.checkpointIdentityHash === null : envelope.cutoff.checkpointIdentityHash !== null, "checkpointIdentityHash presence must match cutoff kind");
    pushIssue(issues, "/clock/clockEpochHash", envelope.clock.quality === "same_epoch" ? envelope.clock.clockEpochHash !== null : envelope.clock.clockEpochHash === null, "clock epoch presence must exactly match same-epoch quality");
    const expectedCoverageStatus = expectedEnvelopeCoverageStatusV1(envelope);
    pushIssue(issues, "/coverage/status", envelope.coverage.status === expectedCoverageStatus, "must match clock quality, source coverage counts, and cutoff state");
    pushIssue(issues, "/coverage/sourceRevisionCount", envelope.coverage.sourceRevisionCount === envelope.source.revision + 1, "must close the exact zero-based source revision range");
    pushIssue(issues, "/coverage/observedRevisionCount", envelope.coverage.observedRevisionCount <= envelope.coverage.sourceRevisionCount, "cannot exceed source revision coverage");
    pushIssue(issues, "/coverage/missingRevisionCount", envelope.coverage.observedRevisionCount + envelope.coverage.missingRevisionCount <= envelope.coverage.sourceRevisionCount, "observed and missing revisions cannot exceed source revision coverage");
    const conditioned = structuredClone(envelope) as Mutable<RunEvaluationEnvelopeV1>;
    deriveEnvelopeTimingMetricsV1(conditioned);
    pushIssue(issues, "/metrics/timing", canonicalStringify(envelope.metrics.timing) === canonicalStringify(conditioned.metrics.timing), "timing statuses, values, and counts must be derived from exact clock, coverage, and cutoff prerequisites");
    pushIssue(issues, "/metrics/waitHistograms", canonicalStringify(envelope.metrics.waitHistograms) === canonicalStringify(conditioned.metrics.waitHistograms), "wait histograms must be conditioned by exact clock, coverage, and cutoff prerequisites");
    const conditionedHuman = { activeMinutes: conditioned.metrics.humanAttention.activeMinutes, activeIntervals: conditioned.metrics.humanAttention.activeIntervals, authorityWait: conditioned.metrics.humanAttention.authorityWait, authorityWaitIntervals: conditioned.metrics.humanAttention.authorityWaitIntervals };
    const envelopeHuman = { activeMinutes: envelope.metrics.humanAttention.activeMinutes, activeIntervals: envelope.metrics.humanAttention.activeIntervals, authorityWait: envelope.metrics.humanAttention.authorityWait, authorityWaitIntervals: envelope.metrics.humanAttention.authorityWaitIntervals };
    pushIssue(issues, "/metrics/humanAttention", canonicalStringify(envelopeHuman) === canonicalStringify(conditionedHuman), "accumulator-derived human metrics must be conditioned by exact clock, coverage, and cutoff prerequisites");
    const conditionedFindings = { falseIndependenceIncidents: conditioned.metrics.findings.falseIndependenceIncidents, falseIndependenceWaste: conditioned.metrics.findings.falseIndependenceWaste };
    const envelopeFindings = { falseIndependenceIncidents: envelope.metrics.findings.falseIndependenceIncidents, falseIndependenceWaste: envelope.metrics.findings.falseIndependenceWaste };
    pushIssue(issues, "/metrics/findings", canonicalStringify(envelopeFindings) === canonicalStringify(conditionedFindings), "accumulator-derived false-independence metrics must be conditioned by exact clock, coverage, and cutoff prerequisites");
    pushIssue(issues, "/metrics/usefulParallelism", canonicalStringify(envelope.metrics.usefulParallelism) === canonicalStringify(conditioned.metrics.usefulParallelism), "useful-parallelism statuses, values, and counts must be derived from exact clock, coverage, cutoff, and serial-policy prerequisites");
    validateEnvelopeMetricSemanticsV1(envelope, issues);
    for (const [key, pulse] of Object.entries(envelope.postRunPulse)) pushIssue(issues, `/postRunPulse/${key}`, pulse.status === "measured" ? pulse.value !== null : pulse.value === null, "pulse value presence must match status");
  }
  return { ok: issues.length === 0, value: issues.length ? undefined : value as RunEvaluationEnvelopeV1, issues };
}

export function executionIdentityHashV1(
  pair: Omit<DogfoodPairV1, "pairIdentityHash" | "executions">,
  execution: DogfoodExecutionV1 | DogfoodExecutionCoreV1,
): string {
  const { executionIdentityHash: _identity, ...executionContent } = execution as DogfoodExecutionV1;
  return canonicalHash({
    scenarioClass: pair.scenarioClass,
    baselineCommitHash: pair.baselineCommitHash,
    baselineTreeHash: pair.baselineTreeHash,
    planHash: pair.planHash,
    oracleHash: pair.oracleHash,
    scriptHash: pair.scriptHash,
    environmentHash: pair.environmentHash,
    evaluationProfileHash: pair.evaluationProfileHash,
    modelHash: pair.modelHash,
    providerHash: pair.providerHash,
    riskCohortHash: pair.riskCohortHash,
    execution: executionContent,
  });
}

export function pairIdentityHashV1(pair: DogfoodPairV1 | DogfoodPairCoreV1): string {
  return hashWithoutField(pair as unknown as Record<string, unknown>, "pairIdentityHash");
}

export function portfolioIdentityHashV1(portfolio: DagEvaluationPortfolioV1 | DagEvaluationPortfolioCoreV1): string {
  return hashWithoutField(portfolio as unknown as Record<string, unknown>, "portfolioIdentityHash");
}

export function dagEvaluationCohortHashV1(portfolio: DagEvaluationPortfolioV1): string {
  const validation = validateDagEvaluationPortfolioV1(portfolio);
  if (!validation.ok) throw new Error(formatIssues("Invalid portfolio for cohort identity", validation.issues));
  return cohortHashForValidatedPortfolioV1(portfolio);
}

export function parseDagEvaluationPortfolioV1(text: string): DagEvaluationPortfolioV1 {
  const value = parseStrictJson(text);
  assertDagEvaluationPortfolioV1(value);
  return value;
}
export function assertDagEvaluationPortfolioV1(value: unknown): asserts value is DagEvaluationPortfolioV1 {
  const result = validateDagEvaluationPortfolioV1(value);
  if (!result.ok) throw new Error(formatIssues("Invalid DagEvaluationPortfolioV1", result.issues));
}
export function validateDagEvaluationPortfolioV1(value: unknown): ValidationResult<DagEvaluationPortfolioV1> {
  const issues = schemaIssues(DagEvaluationPortfolioV1Schema, value);
  if (!issues.length) {
    const portfolio = value as DagEvaluationPortfolioV1;
    const pairIdentityHashes = portfolio.pairs.map(({ pairIdentityHash }) => pairIdentityHash);
    pushIssue(issues, "/pairs", isSortedUnique(pairIdentityHashes), "pair IDs must be sorted and unique");
    const counts = new Map<string, number>();
    const executionIdentityHashes: string[] = [];
    let serialFirst = 0;
    let parallelFirst = 0;
    portfolio.pairs.forEach((pair, index) => {
      pair.executions.forEach((execution, executionIndex) => {
        pushIssue(issues, `/pairs/${index}/executions/${executionIndex}/executionIdentityHash`, execution.executionIdentityHash === executionIdentityHashV1(pair, execution), "must match canonical execution content excluding executionIdentityHash");
      });
      pushIssue(issues, `/pairs/${index}/pairIdentityHash`, pair.pairIdentityHash === pairIdentityHashV1(pair), "must match canonical pair content excluding pairIdentityHash");
      counts.set(pair.scenarioClass, (counts.get(pair.scenarioClass) ?? 0) + 1);
      pushIssue(issues, `/pairs/${index}/evaluationProfileHash`, pair.evaluationProfileHash === RUN_EVALUATION_PROFILE_HASH_V1, "must use the fixed v1 evaluation profile");
      const modes = pair.executions.map(({ mode }) => mode).sort();
      pushIssue(issues, `/pairs/${index}/executions`, modes.join(",") === "parallel,serial", "must contain one serial and one parallel execution");
      const order = [...pair.executions].sort((a, b) => a.order - b.order);
      pushIssue(issues, `/pairs/${index}/executions`, order[0]?.order === 1 && order[1]?.order === 2, "must use execution orders 1 and 2");
      pushIssue(issues, `/pairs/${index}/executions`, pair.executions.find(({ mode }) => mode === "serial")?.maxActiveNodes === 1, "serial execution must use maxActiveNodes=1");
      pushIssue(issues, `/pairs/${index}/executions`, (pair.executions.find(({ mode }) => mode === "parallel")?.maxActiveNodes ?? 0) > 1, "parallel execution must use maxActiveNodes greater than one");
      if (order[0]?.mode === "serial") serialFirst += 1; else parallelFirst += 1;
      executionIdentityHashes.push(...pair.executions.map(({ executionIdentityHash }) => executionIdentityHash));
    });
    pushIssue(issues, "/pairs", counts.get("independent_fanout") === 2 && counts.get("hidden_constraint") === 2 && counts.get("integration_train") === 1 && counts.get("recovery_sensitive") === 1, "must contain the accepted 2+2+1+1 scenario strata");
    pushIssue(issues, "/pairs", serialFirst === 3 && parallelFirst === 3, "must counterbalance serial-first and parallel-first 3/3");
    pushIssue(issues, "/pairs/executions", new Set(executionIdentityHashes).size === 12, "must contain 12 unique execution IDs");
    pushIssue(issues, "/excludedRecoveryDrills", [...portfolio.excludedRecoveryDrills].sort().join(",") === "conductor_crash_resume,provider_worker_loss,target_drift_conflict", "must declare all three separate recovery drills exactly once");
    pushIssue(issues, "/portfolioIdentityHash", portfolio.portfolioIdentityHash === portfolioIdentityHashV1(portfolio), "must match canonical portfolio content excluding portfolioIdentityHash");
  }
  return { ok: issues.length === 0, value: issues.length ? undefined : value as DagEvaluationPortfolioV1, issues };
}

export function buildPairedEvaluationReportV1(portfolio: DagEvaluationPortfolioV1, results: readonly PairedExecutionResultV1[]): PairedEvaluationReportV1 {
  const portfolioValidation = validateDagEvaluationPortfolioV1(portfolio);
  if (!portfolioValidation.ok) throw new Error(formatIssues("Invalid portfolio", portfolioValidation.issues));
  if (results.length !== 12) throw new Error("Paired report requires exactly 12 execution results");
  const resultIssues = results.flatMap((result, index) => schemaIssues(PairedExecutionResultV1Schema, result).map((issue) => ({ ...issue, path: `/results/${index}${issue.path}` })));
  if (resultIssues.length) throw new Error(formatIssues("Invalid paired results", resultIssues));
  const byId = new Map(results.map((result) => [result.executionIdentityHash, result]));
  if (byId.size !== results.length) throw new Error("Paired report execution results must be unique");
  const expectedCohort = cohortHashForValidatedPortfolioV1(portfolio);
  if (results.some(({ cohortHash }) => cohortHash !== expectedCohort)) throw new Error("Cohort mismatch: every result must use the internally derived accepted portfolio cohort");
  const pairDeltas = portfolio.pairs.map((pair) => {
    const serialExecution = pair.executions.find(({ mode }) => mode === "serial")!;
    const parallelExecution = pair.executions.find(({ mode }) => mode === "parallel")!;
    const serial = byId.get(serialExecution.executionIdentityHash);
    const parallel = byId.get(parallelExecution.executionIdentityHash);
    if (!serial || !parallel) throw new Error(`Missing result for pair ${pair.pairIdentityHash}`);
    if (!serial.valid || !parallel.valid || !serial.uncompensatedInvariantsPass || !parallel.uncompensatedInvariantsPass) throw new Error(`Pair ${pair.pairIdentityHash} is not a valid uncompensated comparison`);
    const elapsedDifferenceMs = parallel.elapsedMs - serial.elapsedMs;
    return {
      pairIdentityHash: pair.pairIdentityHash,
      elapsedDifferenceMs,
      elapsedRatio: serial.elapsedMs === 0 ? null : parallel.elapsedMs / serial.elapsedMs,
      usefulWorkDifferenceMs: parallel.usefulWorkMs - serial.usefulWorkMs,
      reportedCostDifference: parallel.reportedCost === null || serial.reportedCost === null ? null : parallel.reportedCost - serial.reportedCost,
      elapsedOutcome: elapsedDifferenceMs < 0 ? "win" as const : elapsedDifferenceMs > 0 ? "loss" as const : "tie" as const,
    };
  });
  if (byId.size !== portfolio.pairs.flatMap(({ executions }) => executions).length || [...byId.keys()].some((id) => !portfolio.pairs.some(({ executions }) => executions.some(({ executionIdentityHash }) => executionIdentityHash === id)))) throw new Error("Paired report contains an undeclared execution result");
  const differences = pairDeltas.map(({ elapsedDifferenceMs }) => elapsedDifferenceMs).sort((a, b) => a - b);
  const report: PairedEvaluationReportV1 = {
    schemaVersion: 1,
    kind: "paired_evaluation_report",
    portfolioIdentityHash: portfolio.portfolioIdentityHash,
    evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1,
    pairDeltas,
    elapsedSummary: {
      medianDifferenceMs: (differences[2] + differences[3]) / 2,
      minimumDifferenceMs: differences[0],
      maximumDifferenceMs: differences[5],
      wins: pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "win").length,
      ties: pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "tie").length,
      losses: pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "loss").length,
    },
  };
  const validation = validatePairedEvaluationReportV1(report, portfolio);
  if (!validation.ok) throw new Error(formatIssues("Invalid paired report", validation.issues));
  return report;
}

export function validatePairedEvaluationReportV1(value: unknown, portfolio?: DagEvaluationPortfolioV1): ValidationResult<PairedEvaluationReportV1> {
  const issues = schemaIssues(PairedEvaluationReportV1Schema, value);
  if (!issues.length) {
    const report = value as PairedEvaluationReportV1;
    pushIssue(issues, "/evaluationProfileHash", report.evaluationProfileHash === RUN_EVALUATION_PROFILE_HASH_V1, "must use the fixed v1 evaluation profile");
    const ids = report.pairDeltas.map(({ pairIdentityHash }) => pairIdentityHash);
    pushIssue(issues, "/pairDeltas", isSortedUnique(ids), "pair IDs must be sorted and unique");
    for (const [index, delta] of report.pairDeltas.entries()) {
      const expected = delta.elapsedDifferenceMs < 0 ? "win" : delta.elapsedDifferenceMs > 0 ? "loss" : "tie";
      pushIssue(issues, `/pairDeltas/${index}/elapsedOutcome`, delta.elapsedOutcome === expected, "must match the sign of the raw elapsed difference");
    }
    const differences = report.pairDeltas.map(({ elapsedDifferenceMs }) => elapsedDifferenceMs).sort((a, b) => a - b);
    const expectedSummary = {
      medianDifferenceMs: (differences[2] + differences[3]) / 2,
      minimumDifferenceMs: differences[0], maximumDifferenceMs: differences[5],
      wins: report.pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "win").length,
      ties: report.pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "tie").length,
      losses: report.pairDeltas.filter(({ elapsedOutcome }) => elapsedOutcome === "loss").length,
    };
    pushIssue(issues, "/elapsedSummary", canonicalStringify(report.elapsedSummary) === canonicalStringify(expectedSummary), "must be the exact median/range/win-tie-loss projection of pair deltas");
    if (portfolio) {
      const portfolioValidation = validateDagEvaluationPortfolioV1(portfolio);
      if (!portfolioValidation.ok) issues.push(...portfolioValidation.issues.map((issue) => ({ ...issue, path: `/portfolio${issue.path}` })));
      else {
        pushIssue(issues, "/portfolioIdentityHash", report.portfolioIdentityHash === portfolio.portfolioIdentityHash, "must match the portfolio identity");
        pushIssue(issues, "/pairDeltas", canonicalStringify(ids) === canonicalStringify(portfolio.pairs.map(({ pairIdentityHash }) => pairIdentityHash)), "must publish every declared pair exactly once");
      }
    }
  }
  return { ok: issues.length === 0, value: issues.length ? undefined : value as PairedEvaluationReportV1, issues };
}

function cohortHashForValidatedPortfolioV1(portfolio: DagEvaluationPortfolioV1): string {
  const pairs = portfolio.pairs.map((pair) => ({
    pairIdentityHash: pair.pairIdentityHash,
    scenarioClass: pair.scenarioClass,
    baselineCommitHash: pair.baselineCommitHash,
    baselineTreeHash: pair.baselineTreeHash,
    executions: pair.executions.map(({ executionIdentityHash, mode, order }) => ({ executionIdentityHash, mode, order }))
      .sort((left, right) => left.executionIdentityHash.localeCompare(right.executionIdentityHash)),
  })).sort((left, right) => left.pairIdentityHash.localeCompare(right.pairIdentityHash));
  return canonicalHash({
    portfolioIdentityHash: portfolio.portfolioIdentityHash,
    evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1,
    pairs,
  });
}

export function cutoffIdentityHashV1(input: Pick<RunEvaluationEnvelopeV1, "identity" | "source" | "cutoff">): string {
  return canonicalHash({ identity: input.identity, source: { revision: input.source.revision, snapshotHash: input.source.snapshotHash, accumulatorHash: input.source.accumulatorHash }, cutoffAt: input.cutoff.cutoffAt, kind: input.cutoff.kind, class: input.cutoff.class, checkpointIdentityHash: input.cutoff.checkpointIdentityHash });
}

export function sourceClosuresFromHashSetsV1(sourceHashes: EvaluationSourceHashSetsV1): RunEvaluationEnvelopeV1["sourceClosures"] {
  const issues = schemaIssues(EvaluationSourceHashSetsV1Schema, sourceHashes);
  for (const [name, hashes] of Object.entries(sourceHashes)) assertSortedUniqueHashes(hashes, issues, `/${name}`);
  if (issues.length) throw new Error(formatIssues("Invalid evaluation source hash sets", issues));
  return {
    authorization: sourceClosureV1(sourceHashes.authorization),
    stageEvidence: sourceClosureV1(sourceHashes.stageEvidence),
    workerResults: sourceClosureV1(sourceHashes.workerResults),
    findingsAndResolutions: sourceClosureV1(sourceHashes.findingsAndResolutions),
    effectReconciliation: sourceClosureV1(sourceHashes.effectReconciliation),
    verification: sourceClosureV1(sourceHashes.verification),
    integration: sourceClosureV1(sourceHashes.integration),
    otherRequired: sourceClosureV1(sourceHashes.otherRequired),
  };
}

export function metricSourceBindingsFromEnvelopeV1(
  envelope: Pick<RunEvaluationEnvelopeV1, "source" | "sourceHashes">,
): RunEvaluationEnvelopeV1["metricSourceBindings"] {
  const sourceIssues = schemaIssues(EvaluationSourceHashSetsV1Schema, envelope.sourceHashes);
  for (const [name, hashes] of Object.entries(envelope.sourceHashes)) assertSortedUniqueHashes(hashes, sourceIssues, `/${name}`);
  if (sourceIssues.length) throw new Error(formatIssues("Invalid metric-family source hash sets", sourceIssues));
  const exact = (family: string, ...sets: readonly (readonly string[])[]): RunEvaluationEnvelopeV1["metricSourceBindings"]["outcomes"] => {
    if (sets.some((hashes) => hashes.length === 0)) throw new Error(`Metric-family source bindings cannot be empty or incomplete for ${family}`);
    const sourceHashes = [...new Set(sets.flat())].sort();
    return { sourceHashes, sourceClosure: sourceClosureV1(sourceHashes) };
  };
  return {
    outcomes: exact("outcomes", envelope.sourceHashes.stageEvidence, envelope.sourceHashes.integration),
    attempts: exact("attempts", envelope.sourceHashes.workerResults),
    findings: exact("findings", envelope.sourceHashes.findingsAndResolutions, [envelope.source.accumulatorHash]),
    integration: exact("integration", envelope.sourceHashes.integration, envelope.sourceHashes.effectReconciliation, envelope.sourceHashes.verification),
    humanAttention: exact("humanAttention", envelope.sourceHashes.authorization, [envelope.source.accumulatorHash]),
    modelUsage: exact("modelUsage", envelope.sourceHashes.workerResults),
  };
}

function validateEnvelopeSourceLineage(envelope: RunEvaluationEnvelopeV1, issues: ValidationIssue[]): void {
  const contains = (category: keyof RunEvaluationEnvelopeV1["sourceHashes"], hash: string | null, path: string): void => {
    if (hash !== null) pushIssue(issues, path, envelope.sourceHashes[category].includes(hash), `must be a member of the exact ${category} source set`);
  };
  contains("authorization", envelope.source.authorizationReceiptHash, "/source/authorizationReceiptHash");
  contains("verification", envelope.source.reviewReceiptHash, "/source/reviewReceiptHash");
  contains("otherRequired", envelope.source.freshnessReceiptHash, "/source/freshnessReceiptHash");
  contains("otherRequired", envelope.source.accumulatorHash, "/source/accumulatorHash");
  envelope.creditContext.acceptedIntegrationLineages.forEach((lineage, index) => {
    contains("stageEvidence", lineage.acceptedEvidenceHash, `/creditContext/acceptedIntegrationLineages/${index}/acceptedEvidenceHash`);
    contains("integration", lineage.integrationReceiptHash, `/creditContext/acceptedIntegrationLineages/${index}/integrationReceiptHash`);
  });
  envelope.creditContext.actionableFindingDispositions.forEach((disposition, index) => {
    contains("findingsAndResolutions", disposition.findingHash, `/creditContext/actionableFindingDispositions/${index}/findingHash`);
    contains("findingsAndResolutions", disposition.currentDispositionHash, `/creditContext/actionableFindingDispositions/${index}/currentDispositionHash`);
  });
  const creditedOperationHashes = envelope.attribution.creditedOperations.map(({ operationHash }) => operationHash);
  pushIssue(issues, "/attribution/creditedOperations", isSortedUnique(creditedOperationHashes), "must be sorted and unique by operationHash");
  for (const [index, lineage] of envelope.attribution.creditedOperations.entries()) {
    contains("otherRequired", lineage.operationHash, `/attribution/creditedOperations/${index}/operationHash`);
    if (lineage.basis.kind === "accepted_integration_lineage") {
      contains("stageEvidence", lineage.basis.acceptedEvidenceHash, `/attribution/creditedOperations/${index}/basis/acceptedEvidenceHash`);
      contains("integration", lineage.basis.integrationReceiptHash, `/attribution/creditedOperations/${index}/basis/integrationReceiptHash`);
    } else {
      contains("findingsAndResolutions", lineage.basis.findingHash, `/attribution/creditedOperations/${index}/basis/findingHash`);
      contains("findingsAndResolutions", lineage.basis.currentDispositionHash, `/attribution/creditedOperations/${index}/basis/currentDispositionHash`);
    }
  }
  const incidentKeys = envelope.attribution.falseIndependenceIncidents.map(({ findingHash, currentDispositionHash }) => `${findingHash}:${currentDispositionHash}`);
  pushIssue(issues, "/attribution/falseIndependenceIncidents", isSortedUnique(incidentKeys), "must be sorted and unique by exact finding/disposition identity");
  let exactWaste = 0;
  for (const [index, incident] of envelope.attribution.falseIndependenceIncidents.entries()) {
    contains("findingsAndResolutions", incident.findingHash, `/attribution/falseIndependenceIncidents/${index}/findingHash`);
    contains("findingsAndResolutions", incident.currentDispositionHash, `/attribution/falseIndependenceIncidents/${index}/currentDispositionHash`);
    pushIssue(issues, `/attribution/falseIndependenceIncidents/${index}/operationHashes`, isSortedUnique(incident.operationHashes), "must be sorted and unique");
    incident.operationHashes.forEach((hash, operationIndex) => contains("otherRequired", hash, `/attribution/falseIndependenceIncidents/${index}/operationHashes/${operationIndex}`));
    exactWaste += incident.wastedOperationMs;
  }
  const incidentMetric = envelope.metrics.findings.falseIndependenceIncidents;
  const wasteMetric = envelope.metrics.findings.falseIndependenceWaste;
  if (incidentMetric.numerator !== null) pushIssue(issues, "/metrics/findings/falseIndependenceIncidents/numerator", incidentMetric.numerator === envelope.attribution.falseIndependenceIncidents.length, "must equal the exact attributed incident count");
  if (wasteMetric.numerator !== null) pushIssue(issues, "/metrics/findings/falseIndependenceWaste/numerator", wasteMetric.numerator === exactWaste, "must equal exact attributed wasted operation time");
  if ((envelope.metrics.usefulParallelism.usefulWork.numerator ?? 0) > 0) pushIssue(issues, "/attribution/creditedOperations", envelope.attribution.creditedOperations.length > 0, "positive useful work requires at least one exact credited lineage");
}

function sealAccumulator(core: Omit<RunObservationAccumulatorV1, "accumulatorHash">): RunObservationAccumulatorV1 {
  const accumulator = { ...core, accumulatorHash: canonicalHash(core) } as RunObservationAccumulatorV1;
  const validation = validateRunObservationAccumulatorV1(accumulator);
  if (!validation.ok) throw new Error(formatIssues("Invalid accumulator", validation.issues));
  return accumulator;
}

function reconcileIntervals(
  prior: RunObservationAccumulatorV1["open"]["readiness"], hashes: string[], intervalClass: RunObservationAccumulatorV1["open"]["readiness"][number]["class"], observation: AccumulatorObservationV1, next: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">>,
  sumKey: keyof RunObservationAccumulatorV1["sums"] | null, histogramKey: keyof RunObservationAccumulatorV1["histograms"] | null, counterKey: keyof RunObservationAccumulatorV1["counters"] | null, authorityWait = new Set<string>(),
): Mutable<RunObservationAccumulatorV1["open"]["readiness"]> {
  const wanted = new Set(hashes);
  const wantedClass = (hash: string): RunObservationAccumulatorV1["open"]["readiness"][number]["class"] => authorityWait.has(hash) ? "authority_wait" : intervalClass;
  for (const interval of prior) if (!wanted.has(interval.intervalHash) || interval.class !== wantedClass(interval.intervalHash)) closeInterval(interval, observation.monotonicTickMs, next, sumKey, histogramKey, counterKey);
  const existing = new Map(prior.filter(({ intervalHash, class: priorClass }) => wanted.has(intervalHash) && priorClass === wantedClass(intervalHash)).map((item) => [item.intervalHash, item]));
  for (const hash of hashes) if (!existing.has(hash)) existing.set(hash, { intervalHash: hash, class: wantedClass(hash), sourceRevision: observation.revision, sourceSnapshotHash: observation.snapshotHash, clockEpochHash: observation.clockEpochHash, openedAtMonotonicMs: observation.monotonicTickMs, creditBasis: null });
  return [...existing.values()].sort((a, b) => a.intervalHash.localeCompare(b.intervalHash));
}
function reconcileActiveIntervals(prior: RunObservationAccumulatorV1["open"]["active"], reserved: string[], active: AccumulatorObservationV1["activeOperations"], observation: AccumulatorObservationV1, next: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">>): Mutable<RunObservationAccumulatorV1["open"]["active"]> {
  const wantedReserved = new Set(reserved);
  const wantedActive = new Map(active.map((item) => [item.operationHash, item.creditBasis]));
  const output = new Map<string, RunObservationAccumulatorV1["open"]["active"][number]>();
  for (const interval of prior) {
    if (interval.class === "reserved_dispatch") {
      if (wantedReserved.has(interval.intervalHash)) output.set(interval.intervalHash, interval);
      else closeInterval(interval, observation.monotonicTickMs, next, "dispatchWaitMs", "dispatchWait", "dispatchWaitIntervals");
    } else if (wantedActive.has(interval.intervalHash)) output.set(interval.intervalHash, { ...interval, creditBasis: wantedActive.get(interval.intervalHash)! });
    else closeInterval(interval, observation.monotonicTickMs, next, null, null, "activeIntervals");
  }
  for (const hash of wantedReserved) if (!output.has(hash)) output.set(hash, { intervalHash: hash, class: "reserved_dispatch", sourceRevision: observation.revision, sourceSnapshotHash: observation.snapshotHash, clockEpochHash: observation.clockEpochHash, openedAtMonotonicMs: observation.monotonicTickMs, creditBasis: null });
  for (const [hash, creditBasis] of wantedActive) if (!output.has(hash) || output.get(hash)!.class !== "operation") output.set(hash, { intervalHash: hash, class: "operation", sourceRevision: observation.revision, sourceSnapshotHash: observation.snapshotHash, clockEpochHash: observation.clockEpochHash, openedAtMonotonicMs: observation.monotonicTickMs, creditBasis });
  return [...output.values()].sort((a, b) => a.intervalHash.localeCompare(b.intervalHash));
}
function censorOpenIntervalsAtLastObservedBoundary(accumulator: RunObservationAccumulatorV1, next: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">>): void {
  const boundary = accumulator.source.monotonicTickMs;
  const censoredCount = accumulator.open.readiness.length + accumulator.open.active.length + accumulator.open.human.length + accumulator.open.recovery.length;
  for (const interval of accumulator.open.readiness) closeInterval(interval, boundary, next, "readinessWaitMs", "readinessWait", "readinessIntervals");
  for (const interval of accumulator.open.active) {
    if (interval.class === "reserved_dispatch") closeInterval(interval, boundary, next, "dispatchWaitMs", "dispatchWait", "dispatchWaitIntervals");
    else closeInterval(interval, boundary, next, null, null, "activeIntervals");
  }
  for (const interval of accumulator.open.human) closeInterval(interval, boundary, next, null, null, null);
  for (const interval of accumulator.open.recovery) closeInterval(interval, boundary, next, "recoveryMs", "recovery", "recoveryIntervals");
  next.coverage.censoredIntervalCount += censoredCount;
  next.open = { readiness: [], active: [], human: [], recovery: [] };
}
function reopenObservationState(observation: AccumulatorObservationV1, next: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">>): void {
  next.open.readiness = reconcileIntervals([], observation.readyOperationHashes, "readiness_lane_admit", observation, next, "readinessWaitMs", "readinessWait", "readinessIntervals");
  next.open.active = reconcileActiveIntervals([], observation.reservedOperationHashes, observation.activeOperations, observation, next);
  next.open.human = reconcileIntervals([], [...observation.humanActiveHashes, ...observation.authorityWaitHashes].sort(), "human_active", observation, next, null, null, null, new Set(observation.authorityWaitHashes));
  next.open.recovery = reconcileIntervals([], observation.recoveryHashes, "recovery", observation, next, "recoveryMs", "recovery", "recoveryIntervals");
}
function closeInterval(interval: RunObservationAccumulatorV1["open"]["active"][number], now: number, next: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">>, sumKey: keyof RunObservationAccumulatorV1["sums"] | null, histogramKey: keyof RunObservationAccumulatorV1["histograms"] | null, counterKey: keyof RunObservationAccumulatorV1["counters"] | null): void {
  const duration = now - interval.openedAtMonotonicMs;
  if (sumKey) next.sums[sumKey] += duration;
  if (histogramKey) next.histograms[histogramKey][bucketFor(duration)] += 1;
  if (counterKey) next.counters[counterKey] += 1;
  if (interval.class === "human_active") { next.sums.humanActiveMs += duration; next.counters.humanActiveIntervals += 1; }
  if (interval.class === "authority_wait") { next.sums.authorityWaitMs += duration; next.counters.authorityWaitIntervals += 1; }
}
function bucketFor(durationMs: number): number {
  if (durationMs < 1_000) return 0;
  if (durationMs < 10_000) return 1;
  if (durationMs < 60_000) return 2;
  if (durationMs < 300_000) return 3;
  if (durationMs < 1_800_000) return 4;
  return 5;
}
function addRevisionGap(coverage: Mutable<Omit<RunObservationAccumulatorV1, "accumulatorHash">["coverage"]>, firstRevision: number, lastRevision: number): void {
  const last = coverage.revisionGaps.at(-1);
  if (last && firstRevision <= last.lastRevision + 1) { last.lastRevision = Math.max(last.lastRevision, lastRevision); return; }
  if (coverage.revisionGaps.length < 64) coverage.revisionGaps.push({ firstRevision, lastRevision });
  else coverage.revisionGapOverflowCount += 1;
}
function validateMetricSemantics(metric: EvaluationMetricV1, issues: ValidationIssue[], path: string): void {
  const numeratorPresent = metric.numerator !== null;
  const denominatorPresent = metric.denominator !== null;
  if (metric.unit === "ratio") {
    pushIssue(issues, path, numeratorPresent === denominatorPresent, "ratio numerator and denominator must be present or absent together");
  } else {
    pushIssue(issues, `${path}/denominator`, !denominatorPresent, "only ratio metrics may have a denominator");
  }
  if ((metric.unit === "count" || metric.unit === "token") && numeratorPresent) pushIssue(issues, `${path}/numerator`, Number.isSafeInteger(metric.numerator), `${metric.unit} values must be safe integers`);
  const ratioValueValid = metric.unit !== "ratio" || !denominatorPresent || metric.denominator! > 0;
  if (metric.status === "measured") {
    pushIssue(issues, path, numeratorPresent && ratioValueValid && metric.observedCount > 0 && metric.missingCount === 0 && metric.censoredCount === 0, "measured metrics require a valid value, positive observed count, and no missing or censored observations");
  } else if (metric.status === "zero_exposure") {
    pushIssue(issues, path, metric.unit === "ratio" && metric.numerator === 0 && metric.denominator === 0 && metric.observedCount === 0 && metric.missingCount === 0 && metric.censoredCount === 0, "zero exposure is a ratio-only 0/0 state with no observation, gap, or censor counts");
  } else if (metric.status === "unsupported_policy") {
    pushIssue(issues, path, !numeratorPresent && !denominatorPresent && metric.observedCount === 0 && metric.missingCount === 0 && metric.censoredCount === 0, "unsupported policy metrics cannot contain values or observation counts");
  } else if (metric.status === "not_observed") {
    pushIssue(issues, path, !numeratorPresent && !denominatorPresent && metric.observedCount === 0 && metric.missingCount > 0 && metric.censoredCount === 0, "not-observed metrics require a positive missing count and no values, observations, or censoring");
  } else if (metric.status === "partial_coverage") {
    const valueShape = metric.unit === "ratio" ? (!numeratorPresent || ratioValueValid) : numeratorPresent;
    pushIssue(issues, path, valueShape && metric.observedCount > 0 && metric.missingCount > 0 && metric.censoredCount === 0, "partial coverage requires observations and missingness, no censoring, and a valid value shape");
  } else if (metric.status === "censored") {
    const valueShape = metric.unit === "ratio" ? (!numeratorPresent || ratioValueValid) : (metric.observedCount === 0 ? !numeratorPresent : numeratorPresent);
    pushIssue(issues, path, valueShape && metric.censoredCount > 0, "censored metrics require positive censoring and a value shape consistent with observed data");
  }
}
function validateEnvelopeMetricSemanticsV1(envelope: RunEvaluationEnvelopeV1, issues: ValidationIssue[]): void {
  const fields: Array<[string, EvaluationMetricV1, EvaluationMetricV1["unit"]]> = [
    ["/outcomes/accepted", envelope.metrics.outcomes.accepted, "count"],
    ["/outcomes/integrated", envelope.metrics.outcomes.integrated, "count"],
    ["/attempts/attempts", envelope.metrics.attempts.attempts, "count"],
    ["/attempts/retries", envelope.metrics.attempts.retries, "count"],
    ["/attempts/backEdges", envelope.metrics.attempts.backEdges, "count"],
    ["/findings/total", envelope.metrics.findings.total, "count"],
    ["/findings/disposed", envelope.metrics.findings.disposed, "count"],
    ["/findings/falseIndependenceIncidents", envelope.metrics.findings.falseIndependenceIncidents, "count"],
    ["/findings/falseIndependenceWaste", envelope.metrics.findings.falseIndependenceWaste, "milliseconds"],
    ["/integration/conflicts", envelope.metrics.integration.conflicts, "count"],
    ["/integration/invalidations", envelope.metrics.integration.invalidations, "count"],
    ["/integration/reconciledEffects", envelope.metrics.integration.reconciledEffects, "count"],
    ["/timing/autonomousElapsed", envelope.metrics.timing.autonomousElapsed, "milliseconds"],
    ["/timing/readinessWait", envelope.metrics.timing.readinessWait, "milliseconds"],
    ["/timing/readinessWaitIntervals", envelope.metrics.timing.readinessWaitIntervals, "count"],
    ["/timing/dispatchWait", envelope.metrics.timing.dispatchWait, "milliseconds"],
    ["/timing/dispatchWaitIntervals", envelope.metrics.timing.dispatchWaitIntervals, "count"],
    ["/timing/recovery", envelope.metrics.timing.recovery, "milliseconds"],
    ["/timing/recoveryIntervals", envelope.metrics.timing.recoveryIntervals, "count"],
    ...envelope.metrics.waitHistograms.readinessWait.map((metric, index) => [`/waitHistograms/readinessWait/${index}`, metric, "count"] as [string, EvaluationMetricV1, EvaluationMetricV1["unit"]]),
    ...envelope.metrics.waitHistograms.dispatchWait.map((metric, index) => [`/waitHistograms/dispatchWait/${index}`, metric, "count"] as [string, EvaluationMetricV1, EvaluationMetricV1["unit"]]),
    ...envelope.metrics.waitHistograms.recovery.map((metric, index) => [`/waitHistograms/recovery/${index}`, metric, "count"] as [string, EvaluationMetricV1, EvaluationMetricV1["unit"]]),
    ["/usefulParallelism/usefulWork", envelope.metrics.usefulParallelism.usefulWork, "milliseconds"],
    ["/usefulParallelism/usefulAverageConcurrency", envelope.metrics.usefulParallelism.usefulAverageConcurrency, "ratio"],
    ["/usefulParallelism/usefulOverlapArea", envelope.metrics.usefulParallelism.usefulOverlapArea, "milliseconds"],
    ["/usefulParallelism/parallelOpportunityArea", envelope.metrics.usefulParallelism.parallelOpportunityArea, "milliseconds"],
    ["/usefulParallelism/opportunityCapture", envelope.metrics.usefulParallelism.opportunityCapture, "ratio"],
    ["/usefulParallelism/allOperationTime", envelope.metrics.usefulParallelism.allOperationTime, "milliseconds"],
    ["/usefulParallelism/workEfficiency", envelope.metrics.usefulParallelism.workEfficiency, "ratio"],
    ["/humanAttention/activeMinutes", envelope.metrics.humanAttention.activeMinutes, "milliseconds"],
    ["/humanAttention/activeIntervals", envelope.metrics.humanAttention.activeIntervals, "count"],
    ["/humanAttention/authorityWait", envelope.metrics.humanAttention.authorityWait, "milliseconds"],
    ["/humanAttention/authorityWaitIntervals", envelope.metrics.humanAttention.authorityWaitIntervals, "count"],
    ["/humanAttention/decisions", envelope.metrics.humanAttention.decisions, "count"],
    ["/modelUsage/inputTokens", envelope.metrics.modelUsage.inputTokens, "token"],
    ["/modelUsage/outputTokens", envelope.metrics.modelUsage.outputTokens, "token"],
    ["/modelUsage/cacheReadTokens", envelope.metrics.modelUsage.cacheReadTokens, "token"],
    ["/modelUsage/cacheWriteTokens", envelope.metrics.modelUsage.cacheWriteTokens, "token"],
    ["/modelUsage/inferenceRequests", envelope.metrics.modelUsage.inferenceRequests, "count"],
    ["/modelUsage/reportedCost", envelope.metrics.modelUsage.reportedCost, "provider_reported_cost"],
    ["/instrumentation/observedRevisions", envelope.metrics.instrumentation.observedRevisions, "count"],
    ["/instrumentation/droppedRevisions", envelope.metrics.instrumentation.droppedRevisions, "count"],
    ["/instrumentation/missingRevisions", envelope.metrics.instrumentation.missingRevisions, "count"],
    ["/instrumentation/censoredIntervals", envelope.metrics.instrumentation.censoredIntervals, "count"],
  ];
  for (const [path, metric, unit] of fields) {
    validateMetricSemantics(metric, issues, `/metrics${path}`);
    pushIssue(issues, `/metrics${path}/unit`, metric.unit === unit, `must use exact ${unit} units`);
    if (!path.startsWith("/timing/") && !path.startsWith("/waitHistograms/") && !path.startsWith("/usefulParallelism/") && !path.startsWith("/instrumentation/") && !["/humanAttention/activeMinutes", "/humanAttention/activeIntervals", "/humanAttention/authorityWait", "/humanAttention/authorityWaitIntervals", "/findings/falseIndependenceIncidents", "/findings/falseIndependenceWaste"].includes(path)) {
      pushIssue(issues, `/metrics${path}/status`, ["measured", "not_observed", "partial_coverage", "censored"].includes(metric.status), "status is not supported for this exact metric field");
    }
  }

  const numeric = (metric: EvaluationMetricV1): number | null => metric.numerator;
  const atMost = (left: EvaluationMetricV1, right: EvaluationMetricV1, path: string, message: string): void => {
    if (numeric(left) !== null && numeric(right) !== null) pushIssue(issues, path, numeric(left)! <= numeric(right)!, message);
  };
  atMost(envelope.metrics.outcomes.integrated, envelope.metrics.outcomes.accepted, "/metrics/outcomes/integrated/numerator", "integrated outcomes cannot exceed accepted outcomes");
  atMost(envelope.metrics.outcomes.accepted, envelope.metrics.attempts.attempts, "/metrics/outcomes/accepted/numerator", "accepted outcomes cannot exceed attempts");
  atMost(envelope.metrics.attempts.retries, envelope.metrics.attempts.attempts, "/metrics/attempts/retries/numerator", "retries cannot exceed attempts");
  atMost(envelope.metrics.attempts.backEdges, envelope.metrics.attempts.attempts, "/metrics/attempts/backEdges/numerator", "back-edges cannot exceed attempts");
  atMost(envelope.metrics.findings.disposed, envelope.metrics.findings.total, "/metrics/findings/disposed/numerator", "disposed findings cannot exceed total findings");

  const parallel = envelope.metrics.usefulParallelism;
  atMost(parallel.usefulWork, parallel.allOperationTime, "/metrics/usefulParallelism/usefulWork/numerator", "useful work cannot exceed all operation time");
  atMost(parallel.usefulOverlapArea, parallel.usefulWork, "/metrics/usefulParallelism/usefulOverlapArea/numerator", "useful overlap cannot exceed useful work");
  atMost(parallel.usefulOverlapArea, parallel.parallelOpportunityArea, "/metrics/usefulParallelism/usefulOverlapArea/numerator", "useful overlap cannot exceed parallel opportunity");
  if (parallel.usefulWork.numerator !== null && parallel.usefulOverlapArea.numerator !== null && envelope.metrics.timing.autonomousElapsed.numerator !== null) {
    pushIssue(issues, "/metrics/usefulParallelism/usefulOverlapArea/numerator", parallel.usefulWork.numerator - parallel.usefulOverlapArea.numerator <= envelope.metrics.timing.autonomousElapsed.numerator, "useful work outside overlap cannot exceed autonomous elapsed exposure");
  }

  const checkRatio = (ratio: EvaluationMetricV1, numerator: EvaluationMetricV1, denominator: EvaluationMetricV1, path: string, bounded: boolean): void => {
    if (ratio.numerator !== null) {
      pushIssue(issues, `${path}/numerator`, numerator.numerator !== null && ratio.numerator === numerator.numerator, "must equal its exact source numerator");
      pushIssue(issues, `${path}/denominator`, denominator.numerator !== null && ratio.denominator === denominator.numerator, "must equal its exact source exposure denominator");
      if (bounded) pushIssue(issues, path, ratio.denominator !== null && ratio.numerator <= ratio.denominator, "ratio value must be in [0,1]");
    }
    if (ratio.status === "zero_exposure") pushIssue(issues, path, numerator.numerator === 0 && denominator.numerator === 0, "zero exposure requires exact zero source numerator and denominator");
    if (numerator.numerator === 0 && denominator.numerator === 0 && !["unsupported_policy", "not_observed"].includes(ratio.status)) pushIssue(issues, path, ratio.status === "zero_exposure" || (ratio.status === "censored" && ratio.numerator === null), "zero source exposure must not be published as an ordinary measured ratio");
  };
  checkRatio(parallel.usefulAverageConcurrency, parallel.usefulWork, envelope.metrics.timing.autonomousElapsed, "/metrics/usefulParallelism/usefulAverageConcurrency", false);
  checkRatio(parallel.opportunityCapture, parallel.usefulOverlapArea, parallel.parallelOpportunityArea, "/metrics/usefulParallelism/opportunityCapture", true);
  checkRatio(parallel.workEfficiency, parallel.usefulWork, parallel.allOperationTime, "/metrics/usefulParallelism/workEfficiency", true);

  const instrumentation: Array<[keyof RunEvaluationEnvelopeV1["metrics"]["instrumentation"], number]> = [
    ["observedRevisions", envelope.coverage.observedRevisionCount],
    ["droppedRevisions", envelope.coverage.droppedRevisionCount],
    ["missingRevisions", envelope.coverage.missingRevisionCount],
    ["censoredIntervals", envelope.coverage.censoredIntervalCount],
  ];
  for (const [name, exact] of instrumentation) {
    const metric = envelope.metrics.instrumentation[name];
    pushIssue(issues, `/metrics/instrumentation/${name}`, metric.status === "measured" && metric.numerator === exact && metric.denominator === null && metric.observedCount === 1 && metric.missingCount === 0 && metric.censoredCount === 0, "must be the exact measured envelope coverage count");
  }
}
function validateCanonicalCreditContext(context: CanonicalCreditContextV1): ValidationIssue[] {
  const issues = schemaIssues(CanonicalCreditContextV1Schema, context);
  if (!issues.length) {
    const lineageKeys = context.acceptedIntegrationLineages.map(({ acceptedEvidenceHash, integrationReceiptHash }) => `${acceptedEvidenceHash}:${integrationReceiptHash}`);
    pushIssue(issues, "/acceptedIntegrationLineages", isSortedUnique(lineageKeys), "must be sorted and unique by exact evidence/integration-receipt tuple");
    const findingHashes = context.actionableFindingDispositions.map(({ findingHash }) => findingHash);
    pushIssue(issues, "/actionableFindingDispositions", isSortedUnique(findingHashes), "must be sorted with exactly one current disposition per finding");
  }
  return issues;
}
function creditBasisAccepted(claim: CreditBasisV1, context: CanonicalCreditContextV1): boolean {
  if (claim.kind === "accepted_integration_lineage") return context.acceptedIntegrationLineages.some(({ acceptedEvidenceHash, integrationReceiptHash }) => acceptedEvidenceHash === claim.acceptedEvidenceHash && integrationReceiptHash === claim.integrationReceiptHash);
  return context.actionableFindingDispositions.some(({ findingHash, currentDispositionHash }) => findingHash === claim.findingHash && currentDispositionHash === claim.currentDispositionHash);
}
function validateEvaluationTimestampFields(value: unknown, issues: ValidationIssue[], path = ""): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateEvaluationTimestampFields(item, issues, `${path}/${index}`));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    const childPath = `${path}/${key}`;
    if ((key.endsWith("At") || key === "validFrom" || key === "validUntil" || key === "expiresAt") && item !== null) {
      pushIssue(issues, childPath, typeof item === "string" && parseRfc3339UtcNanosecondsV1(item) !== null, "must be a real UTC RFC 3339 civil timestamp with 0 or 1-9 fractional digits");
    } else validateEvaluationTimestampFields(item, issues, childPath);
  }
}
function assertSortedUniqueHashes(values: string[], issues: ValidationIssue[], path: string): void {
  pushIssue(issues, path, isSortedUnique(values), "must be sorted and unique");
}
function assertDisjointHashes(groups: string[][], issues: ValidationIssue[], path: string): void {
  const values = groups.flat();
  pushIssue(issues, path, new Set(values).size === values.length, "interval identity sets must be disjoint");
}
function formatIssues(label: string, issues: ValidationIssue[]): string {
  return `${label}:\n${issues.map(({ path, message }) => `- ${path || "/"}: ${message}`).join("\n")}`;
}

export const RUN_EVALUATION_ENVELOPE_SCHEMA_HASH_V1 = canonicalHash(JSON.parse(JSON.stringify(RunEvaluationEnvelopeV1Schema)));
export const RUN_OBSERVATION_ACCUMULATOR_SCHEMA_HASH_V1 = canonicalHash(JSON.parse(JSON.stringify(RunObservationAccumulatorV1Schema)));
export const DAG_EVALUATION_PORTFOLIO_SCHEMA_HASH_V1 = canonicalHash(JSON.parse(JSON.stringify(DagEvaluationPortfolioV1Schema)));
export { canonicalStringify };
