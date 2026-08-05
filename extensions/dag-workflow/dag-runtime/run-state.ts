import { Type, type Static } from "typebox";
import {
  BoundedTextSchema,
  GitOidSchema,
  GitTreeRefV1Schema,
  HashMap,
  HashSchema,
  IdMap,
  IdSchema,
  NonNegativeIntegerSchema,
  Nullable,
  PositiveIntegerSchema,
  RootRelativePathSchema,
  StrictObject,
  StringSet,
  TimestampSchema,
  canonicalHash,
  hashWithoutField,
  isSortedUnique,
  parseStrictJson,
  pushIssue,
  schemaIssues,
  utcTimestampOrderValue,
  validateTimestampFields,
  type GitTreeRefV1,
  type ValidationIssue,
  type ValidationResult,
} from "./common.ts";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  PLAN_STAGE_IDS,
  PlanStageIdSchema,
  validateCanonicalDagPlanV1,
  type CanonicalDagPlanV1,
} from "./plan.ts";

export interface StageEvidenceFactBindingV1 {
  kind: "stage_evidence";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  authorizationSetHash: string;
  procedureHash: string;
  environmentProfileHash: string;
  checkAggregateHash: string;
  findingHashes: readonly string[];
  effectReconciliationHashes: readonly string[];
  candidateGeneration: number;
  candidateHash: string | null;
  producerKind: "conductor" | "owned_worker" | "deterministic_runner";
  producerResultHash: string | null;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  freshIndependent: boolean;
  readOnly: boolean;
  cleanEnvironment: boolean;
}

export interface StageAttemptInputFactBindingV1 {
  kind: "stage_attempt_input";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  candidateGeneration: number;
  candidateHash: string | null;
  authorizationSetHash: string;
  producerKind: "conductor" | "owned_worker" | "deterministic_runner";
  implementationLineageHash: string | null;
}

export interface WorkerResultFactBindingV1 {
  kind: "worker_result";
  hash: string;
  workerStorageId: string;
  launchOwnerSessionId: string;
  workerId: string;
  attemptNumber: number;
  attemptNonce: string;
  configHash: string;
  completionId: string;
  terminalStatus: "succeeded" | "needs_attention" | "failed" | "cancelled" | "lost";
  processDisposition: "dead" | "ambiguous";
  retrySafe: boolean;
}

export interface CheckDispositionFactBindingV1 {
  kind: "check_disposition";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  checkId: string;
  disposition: "PASS" | "WAIVED" | "NOT_APPLICABLE";
  predicateHash: string | null;
  authorizationSetHash: string;
  evidenceHashes: readonly string[];
}

export interface VerificationFactBindingV1 {
  kind: "verification";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  repositoryId: string;
  trainId: string;
  integrationAttemptId: string;
  phase: "prefix" | "final";
  profileId: string;
  profileHash: string;
  tree: GitTreeRefV1;
  disposition: "PASS";
}

export interface OracleAssertionFactBindingV1 {
  kind: "oracle_assertion";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: "F2";
  oracleId: string;
  assertionId: string;
  procedureId: string;
  environmentProfileId: string;
  observationMethod: "static_analysis" | "automated_check" | "manual_observation" | "external_observation" | "combined";
  requiredEvidenceClass: "deterministic" | "independent" | "manual" | "external";
  disposition: "PASS";
  observationHash: string;
}

export interface EffectReconciliationFactBindingV1 {
  kind: "effect_reconciliation";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  effectId: string;
  requestHash: string;
  reconciliation: "applied_exact" | "compensated" | "proven_absent" | "conflict" | "unknown";
}

export interface ProcessIdentityObservationBindingV1 {
  kind: "process_identity_observation";
  hash: string;
  lockMetadataHash: string;
  pid: number;
  processStartIdentity: string;
  disposition: "dead_missing" | "dead_reused";
  observedAt: string;
}

export interface CorruptFactEnvelopeBindingV1 {
  kind: "corrupt_fact";
  hash: string;
  claimedHash: string;
  rawBytesHash: string;
  rawBytes: number;
  quarantinedAt: string;
  rawPathIdentityHash: string;
}

export interface QuarantineAuthorityBindingV1 {
  kind: "quarantine_authority";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  quarantineId: string;
  factHash: string;
  quarantineEntryHash: string;
  decision: "adopt";
  issuedBy: "user";
  issuedAt: string;
}

export interface QuarantineResolutionFactBindingV1 {
  kind: "quarantine_resolution";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  quarantineId: string;
  factHash: string;
  quarantineEntryHash: string;
  authorityReceiptHash: string;
  disposition: "adopted";
  rationaleHash: string;
}

export interface OwnershipFactBindingV1 {
  kind: "ownership";
  hash: string;
  runId: string;
  runNonce: string;
  priorSessionId: string | null;
  priorOwnerTokenHash: string | null;
  priorPid: number;
  priorProcessStartIdentity: string | null;
  priorLockIdentity: string | null;
  priorAttachedAt: string | null;
  disposition: "absent" | "dead" | "same_manager";
  priorObservationHash: string | null;
  successorSessionId: string;
  successorPid: number;
  successorProcessStartIdentity: string;
  successorLockIdentity: string;
  lineageHash: string | null;
}

export interface CandidateFactBindingV1 {
  kind: "candidate";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  generation: number;
  candidateId: string;
  base: GitTreeRefV1;
  git: GitTreeRefV1;
  patchIdentityHash: string;
  producedByStageAttemptId: string;
  lineageHash: string;
}

export interface EvidenceAdoptionFactBindingV1 {
  kind: "adoption";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: "F2";
  fromCandidateGeneration: number;
  fromCandidateHash: string;
  toCandidateGeneration: number;
  toCandidateHash: string;
  f3StageAttemptId: string;
  evidenceHash: string;
  sourceEvidenceProcedureHash: string;
  deltaAttestationProcedureHash: string;
  environmentProfileHash: string;
  evidenceOnlyDelta: true;
}

export interface IntegrationReadyFactBindingV1 {
  kind: "integration_ready";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  candidateGeneration: number;
  candidateHash: string;
  f8EvidenceHash: string;
  allRequiredChecksPassed: boolean;
  effectsReconciled: boolean;
  findingsClosed: boolean;
}

export interface GitTransactionFactBindingV1 {
  kind: "git_transaction";
  hash: string;
  factType: "repository_binding" | "private_ref" | "composition" | "proposal_verification" | "landing" | "quarantine";
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  repositoryId: string;
  integrationAttemptId: string | null;
  effectId: string | null;
  requestHash: string;
  ownerEpoch: number;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  gitConfigHash: string;
  gitVersionHash: string;
  objectFormat: "sha1" | "sha256";
  targetRef: string | null;
  commit: string | null;
  tree: string | null;
  parentCommit: string | null;
  reconciliation: "not_started" | "applied_exact" | "proven_absent" | "conflict" | "unknown";
  detailsHash: string;
  observedAt: string;
}

export interface GitIntegrationReceiptFactBindingV1 {
  kind: "git_integration_receipt";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  repositoryId: string;
  integrationAttemptId: string;
  transactionReceiptHash: string;
  receipt: Record<string, unknown>;
}

export interface IntegrationFactBindingV1 {
  kind: "integration";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  repositoryId: string;
  integrationAttemptId: string;
  candidateHash: string;
  strategy: "merge_tree_one_parent";
  compositionProfileHash: string;
  expectedPrefix: GitTreeRefV1;
  expectedTarget: GitTreeRefV1;
  prefixEvidenceHashes: readonly string[];
  finalEvidenceHashes: readonly string[];
  environmentClosureHash: string;
  sourceBase: GitTreeRefV1;
  sourceCandidate: GitTreeRefV1;
  syntheticParentCommit: string;
  sourceToIntegratedLineageHash: string;
  landed: GitTreeRefV1;
  combinedStateVerified: boolean;
  reconciled: boolean;
  acceptingOwnerEpoch: number;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  gitConfigHash: string;
  gitVersionHash: string;
  objectFormat: "sha1" | "sha256";
  transactionReceiptHash: string;
  transactionReceiptFactHash: string;
  landingObservationHash: string;
}

export type DagRunFactBindingV1 = GitTransactionFactBindingV1 | GitIntegrationReceiptFactBindingV1 | ProcessIdentityObservationBindingV1 | CorruptFactEnvelopeBindingV1 | StageAttemptInputFactBindingV1 | WorkerResultFactBindingV1 | StageEvidenceFactBindingV1 | CheckDispositionFactBindingV1 | VerificationFactBindingV1 | OracleAssertionFactBindingV1 | EffectReconciliationFactBindingV1 | QuarantineResolutionFactBindingV1 | OwnershipFactBindingV1 | CandidateFactBindingV1 | EvidenceAdoptionFactBindingV1 | IntegrationReadyFactBindingV1 | IntegrationFactBindingV1;
export interface DagRunAuthorizationBindingV1 {
  hash: string;
  planHash: string;
  reviewReceiptHash: string;
  receiptHashes: readonly string[];
  workItemIds: readonly string[];
  stageScopes: Readonly<Record<string, readonly typeof PLAN_STAGE_IDS[number][]>>;
  repositoryIds: readonly string[];
  effectScopeIds: readonly string[];
  integrationTrainIds: readonly string[];
  retryCeilingsHash: string;
  maxActiveNodes: number;
  validFrom: string;
  validUntil: string | null;
}
export interface ProcedureCatalogBindingV1 {
  hash: string;
  procedureId: string;
  purpose: "lifecycle" | "evidence_only_delta_attestation";
  stages: readonly typeof PLAN_STAGE_IDS[number][];
  producerKinds: readonly ("conductor" | "owned_worker" | "deterministic_runner")[];
  readOnly: boolean;
  environmentProfileHash: string;
}
export interface CheckAggregateBindingV1 {
  hash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  procedureHash: string;
  environmentProfileHash: string;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  oracleIds: readonly string[];
  assertions: readonly { oracleId: string; assertionId: string; evidenceHash: string }[];
  checks: readonly { checkId: string; disposition: "PASS" | "WAIVED" | "NOT_APPLICABLE"; applicabilityEvidenceHashes: readonly string[] }[];
}
export interface DagExecutionCatalogBindingV1 {
  lifecycleProfileHash: string;
  checkCatalogHash: string;
  procedures: Readonly<Record<string, ProcedureCatalogBindingV1>>;
  checkAggregates: Readonly<Record<string, CheckAggregateBindingV1>>;
}
export interface DagRunValidationContextV1 {
  plan: CanonicalDagPlanV1;
  authorization: DagRunAuthorizationBindingV1;
  historicalAuthorizations: Readonly<Record<string, DagRunAuthorizationBindingV1>>;
  catalog: DagExecutionCatalogBindingV1;
  normalizedSchedulerIndexHash: string;
  facts: Readonly<Record<string, DagRunFactBindingV1>>;
  authorityReceipts?: Readonly<Record<string, QuarantineAuthorityBindingV1>>;
}

const RunDesiredSchema = Type.Enum(["running", "paused", "cancelled", "superseded"]);
const RunCurrentSchema = Type.Enum(["initializing", "active", "paused", "cancelling", "blocked", "integration", "needs_decision", "completed", "cancelled", "superseded"]);
const RunCompletionSchema = Type.Enum(["open", "authorized_scope_complete", "plan_complete"]);
const FreshnessClassSchema = Type.Enum(["valid_exact", "valid_revalidated", "stale_model", "stale_code", "stale_schema", "integration_drift", "unknown_impact"]);
const StageStateSchema = Type.Enum(["pending", "active", "passed", "failed", "blocked", "budget_exhausted", "cancelled", "invalidated"]);
const CheckDispositionSchema = Type.Enum(["PASS", "FAIL", "BLOCKED", "WAIVED", "NOT_APPLICABLE", "BUDGET_EXHAUSTED"]);
const ProducerKindSchema = Type.Enum(["conductor", "owned_worker", "deterministic_runner"]);
const AttemptStateSchema = Type.Enum(["preparing", "dispatchable", "launching", "running", "settling", "result_observed", "evidence_pending", "sealed", "cancelling", "cancelled", "failed", "lost", "ambiguous", "quarantined"]);
const LaunchStateSchema = Type.Enum(["reserved", "dispatchable", "dispatching", "bound", "not_started", "cancel_requested", "closed", "ambiguous"]);
const ProcessDispositionSchema = Type.Enum(["live", "dead", "ambiguous", "not_applicable"]);
const FindingKindSchema = Type.Enum(["product_defect", "test_evidence_gap", "architecture_issue", "oracle_contract_issue", "infrastructure_failure", "capability_absent", "external_precondition_failure", "equivalent_nonactionable"]);
const FindingSeveritySchema = Type.Enum(["advisory", "blocking"]);
const FindingMaterialitySchema = Type.Enum(["local", "plan_affecting"]);
const FindingClosureSchema = Type.Enum(["open", "corrected", "equivalent_accepted", "successor_plan_required", "invalidated"]);
const FailureClassSchema = Type.Enum(["product", "evidence", "architecture", "oracle_contract", "infrastructure", "capability", "external_precondition", "worker_runtime", "cancellation", "integration", "integrity"]);
const ProcedureClassSchema = Type.Enum(["pure", "idempotent", "compensatable", "non_repeatable", "unknown"]);
const EffectStateSchema = Type.Enum(["intended", "dispatching", "observed", "reconciled", "failed", "ambiguous", "cancelled"]);
const ReconciliationSchema = Type.Enum(["not_started", "applied_exact", "compensated", "proven_absent", "conflict", "unknown"]);
const RetryDimensionSchema = Type.Enum(["product_repair", "test_rework", "review_rework", "hardening_rework", "infrastructure", "worker_replacement", "integration"]);
const RetryStopSchema = Type.Enum(["none", "ceiling_reached", "repeated_fingerprint", "no_material_progress", "repeated_tree", "oscillation", "unreconciled_effect", "authorization_required"]);
const BlockerKindSchema = Type.Enum(["precedence", "gate", "authorization", "plan_staleness", "integration_drift", "resource_capacity", "semantic_mutex", "repository_lease", "side_effect_unreconciled", "retry_exhausted", "no_progress", "finding", "cancellation", "launch_ambiguous", "worker_lost", "capability", "external_precondition", "successor_plan_required", "operator_decision", "corrupt_fact", "concurrent_owner"]);
const BlockerReleaseSchema = Type.Enum(["automatic", "immutable_fact", "operator", "successor_plan"]);
const LeaseKindSchema = Type.Enum(["stage_claim", "resource", "semantic_mutex", "repository_workspace", "integration_lock"]);
const LeaseStateSchema = Type.Enum(["active", "release_requested", "released", "expired", "fenced"]);
const GateStateSchema = Type.Enum(["closed", "released", "invalidated"]);
const PrecedenceStateSchema = Type.Enum(["waiting", "satisfied", "invalidated"]);
const WorkItemCurrentSchema = Type.Enum(["pending", "ready", "active", "blocked", "integration_ready", "integrating", "complete", "cancelled", "superseded"]);
const QuarantineReasonSchema = Type.Enum(["identity_mismatch", "stale_generation", "stale_plan", "cancelled_generation", "superseded_attempt", "corrupt_fact", "duplicate_conflict", "unauthorized_scope", "ambiguous_runtime", "unsolicited"]);
const QuarantineStateSchema = Type.Enum(["held", "adopted", "rejected"]);
const TrainEntryStateSchema = Type.Enum(["waiting", "eligible", "reserved", "composing", "verifying_prefix", "landing", "integrated", "blocked", "invalidated", "quarantined"]);
const ConflictClassSchema = Type.Enum(["none", "mechanical", "semantic", "ambiguous"]);
const LandingStateSchema = Type.Enum(["none", "intended", "dispatching", "observed", "reconciled", "landed", "ambiguous"]);
const CancellationScopeSchema = Type.Enum(["run", "work_item", "stage_attempt", "integration_attempt"]);
const CancellationStateSchema = Type.Enum(["requested", "dispatching", "observed", "ambiguous", "closed"]);
const FactKindSchema = Type.Enum(["plan_review", "plan_authorization", "authorization_set", "staleness", "stage_attempt_input", "worker_result", "candidate", "stage_evidence", "check_disposition", "oracle_assertion", "finding", "finding_resolution", "waiver", "invalidation", "adoption", "effect_intent", "effect_reconciliation", "corrupt_fact", "process_identity_observation", "quarantine_resolution", "quarantine_authority", "integration_ready", "integration", "ownership", "gate_release", "repository_observation", "verification", "git_transaction", "git_integration_receipt"]);
const StageAttemptInputFactBindingV1Schema = StrictObject({
  kind: Type.Literal("stage_attempt_input"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema),
  authorizationSetHash: HashSchema, producerKind: ProducerKindSchema, implementationLineageHash: Nullable(HashSchema),
});
const WorkerResultFactBindingV1Schema = StrictObject({
  kind: Type.Literal("worker_result"), hash: HashSchema, workerStorageId: IdSchema,
  launchOwnerSessionId: IdSchema, workerId: IdSchema, attemptNumber: PositiveIntegerSchema,
  attemptNonce: Type.String({ minLength: 16, maxLength: 256 }), configHash: HashSchema, completionId: IdSchema,
  terminalStatus: Type.Enum(["succeeded", "needs_attention", "failed", "cancelled", "lost"]),
  processDisposition: Type.Enum(["dead", "ambiguous"]), retrySafe: Type.Boolean(),
});
const CheckDispositionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("check_disposition"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  checkId: IdSchema, disposition: Type.Enum(["PASS", "WAIVED", "NOT_APPLICABLE"]),
  predicateHash: Nullable(HashSchema), authorizationSetHash: HashSchema, evidenceHashes: Type.Array(HashSchema, { minItems: 1 }),
});
const VerificationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("verification"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), repositoryId: IdSchema, trainId: IdSchema,
  integrationAttemptId: IdSchema, phase: Type.Enum(["prefix", "final"]), profileId: IdSchema,
  profileHash: HashSchema, tree: GitTreeRefV1Schema, disposition: Type.Literal("PASS"),
});
const OracleAssertionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("oracle_assertion"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: Type.Literal("F2"),
  oracleId: IdSchema, assertionId: IdSchema, procedureId: IdSchema, environmentProfileId: IdSchema,
  observationMethod: Type.Enum(["static_analysis", "automated_check", "manual_observation", "external_observation", "combined"]),
  requiredEvidenceClass: Type.Enum(["deterministic", "independent", "manual", "external"]),
  disposition: Type.Literal("PASS"), observationHash: HashSchema,
});
const EffectReconciliationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("effect_reconciliation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), effectId: IdSchema, requestHash: HashSchema,
  reconciliation: Type.Enum(["applied_exact", "compensated", "proven_absent", "conflict", "unknown"]),
});
const ProcessIdentityObservationBindingV1Schema = StrictObject({
  kind: Type.Literal("process_identity_observation"), hash: HashSchema, lockMetadataHash: HashSchema,
  pid: PositiveIntegerSchema, processStartIdentity: Type.String({ minLength: 1, maxLength: 256 }),
  disposition: Type.Enum(["dead_missing", "dead_reused"]), observedAt: TimestampSchema,
});
const CorruptFactEnvelopeBindingV1Schema = StrictObject({
  kind: Type.Literal("corrupt_fact"), hash: HashSchema, claimedHash: HashSchema, rawBytesHash: HashSchema,
  rawBytes: NonNegativeIntegerSchema, quarantinedAt: TimestampSchema, rawPathIdentityHash: HashSchema,
});
const QuarantineAuthorityBindingV1Schema = StrictObject({
  kind: Type.Literal("quarantine_authority"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), quarantineId: IdSchema, factHash: HashSchema, quarantineEntryHash: HashSchema,
  decision: Type.Literal("adopt"), issuedBy: Type.Literal("user"), issuedAt: TimestampSchema,
});
const QuarantineResolutionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("quarantine_resolution"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), quarantineId: IdSchema, factHash: HashSchema, quarantineEntryHash: HashSchema, authorityReceiptHash: HashSchema,
  disposition: Type.Literal("adopted"), rationaleHash: HashSchema,
});
const OwnershipFactBindingV1Schema = StrictObject({
  kind: Type.Literal("ownership"), hash: HashSchema, runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }),
  priorSessionId: Nullable(IdSchema), priorOwnerTokenHash: Nullable(HashSchema), priorPid: NonNegativeIntegerSchema, priorProcessStartIdentity: Nullable(Type.String({ minLength: 1, maxLength: 256 })), priorLockIdentity: Nullable(HashSchema), priorAttachedAt: Nullable(TimestampSchema),
  disposition: Type.Enum(["absent", "dead", "same_manager"]), priorObservationHash: Nullable(HashSchema), successorSessionId: IdSchema, successorPid: PositiveIntegerSchema,
  successorProcessStartIdentity: Type.String({ minLength: 1, maxLength: 256 }), successorLockIdentity: HashSchema, lineageHash: Nullable(HashSchema),
});
const StageEvidenceFactBindingV1Schema = StrictObject({
  kind: Type.Literal("stage_evidence"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, authorizationSetHash: HashSchema,
  procedureHash: HashSchema, environmentProfileHash: HashSchema, checkAggregateHash: HashSchema,
  findingHashes: Type.Array(HashSchema), effectReconciliationHashes: Type.Array(HashSchema),
  candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema),
  producerKind: ProducerKindSchema, producerResultHash: Nullable(HashSchema), disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  freshIndependent: Type.Boolean(), readOnly: Type.Boolean(), cleanEnvironment: Type.Boolean(),
});
const CandidateFactBindingV1Schema = StrictObject({
  kind: Type.Literal("candidate"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema,
  generation: PositiveIntegerSchema, candidateId: IdSchema, base: GitTreeRefV1Schema, git: GitTreeRefV1Schema,
  patchIdentityHash: HashSchema, producedByStageAttemptId: IdSchema, lineageHash: HashSchema,
});
const EvidenceAdoptionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("adoption"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: Type.Literal("F2"),
  fromCandidateGeneration: PositiveIntegerSchema, fromCandidateHash: HashSchema,
  toCandidateGeneration: PositiveIntegerSchema, toCandidateHash: HashSchema, f3StageAttemptId: IdSchema, evidenceHash: HashSchema,
  sourceEvidenceProcedureHash: HashSchema, deltaAttestationProcedureHash: HashSchema, environmentProfileHash: HashSchema, evidenceOnlyDelta: Type.Literal(true),
});
const IntegrationReadyFactBindingV1Schema = StrictObject({
  kind: Type.Literal("integration_ready"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema,
  candidateGeneration: NonNegativeIntegerSchema, candidateHash: HashSchema, f8EvidenceHash: HashSchema,
  allRequiredChecksPassed: Type.Boolean(), effectsReconciled: Type.Boolean(), findingsClosed: Type.Boolean(),
});
const GitTransactionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("git_transaction"), hash: HashSchema,
  factType: Type.Enum(["repository_binding", "private_ref", "composition", "proposal_verification", "landing", "quarantine"]),
  planHash: HashSchema, runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema, repositoryId: IdSchema,
  integrationAttemptId: Nullable(IdSchema), effectId: Nullable(IdSchema), requestHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema,
  commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema, gitConfigHash: HashSchema, gitVersionHash: HashSchema, objectFormat: Type.Enum(["sha1", "sha256"]),
  targetRef: Nullable(Type.String({ minLength: 1, maxLength: 512 })), commit: Nullable(GitOidSchema), tree: Nullable(GitOidSchema), parentCommit: Nullable(GitOidSchema),
  reconciliation: Type.Enum(["not_started", "applied_exact", "proven_absent", "conflict", "unknown"]), detailsHash: HashSchema, observedAt: TimestampSchema,
});
const GitIntegrationReceiptPayloadV1Schema = StrictObject({
  schemaVersion: Type.Literal(1), kind: Type.Literal("IntegrationReceiptV1"), transactionId: IdSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), planHash: HashSchema, authorizationSetHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema,
  repositoryId: IdSchema, commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema, gitVersion: Type.String({ minLength: 1, maxLength: 256 }), configHash: HashSchema, objectFormat: Type.Enum(["sha1", "sha256"]), targetRef: Type.String({ minLength: 1, maxLength: 512 }),
  sourceBase: GitTreeRefV1Schema, candidate: GitTreeRefV1Schema, expectedPrefix: GitTreeRefV1Schema, composed: GitTreeRefV1Schema,
  workItemId: IdSchema, candidateGeneration: PositiveIntegerSchema, compositionProfileHash: HashSchema, prefixValidationProfileHash: HashSchema, finalValidationProfileHash: HashSchema,
  prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema,
  privateRefs: Type.Record(Type.String({ minLength: 1, maxLength: 128 }), Type.String({ minLength: 1, maxLength: 512 })),
  landing: StrictObject({ expectedOldOid: GitOidSchema, newOid: GitOidSchema, reconciliation: Type.Literal("applied_exact"), targetObservationHash: HashSchema }),
  sealedAt: TimestampSchema, receiptHash: HashSchema,
});
const GitIntegrationReceiptFactBindingV1Schema = StrictObject({
  kind: Type.Literal("git_integration_receipt"), hash: HashSchema, planHash: HashSchema, runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  repositoryId: IdSchema, integrationAttemptId: IdSchema, transactionReceiptHash: HashSchema, receipt: GitIntegrationReceiptPayloadV1Schema,
});
const IntegrationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("integration"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema, workItemId: IdSchema, repositoryId: IdSchema, integrationAttemptId: IdSchema,
  candidateHash: HashSchema, strategy: Type.Literal("merge_tree_one_parent"), compositionProfileHash: HashSchema,
  expectedPrefix: GitTreeRefV1Schema, expectedTarget: GitTreeRefV1Schema,
  prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema,
  sourceBase: GitTreeRefV1Schema, sourceCandidate: GitTreeRefV1Schema, syntheticParentCommit: GitOidSchema,
  sourceToIntegratedLineageHash: HashSchema, landed: GitTreeRefV1Schema, combinedStateVerified: Type.Boolean(), reconciled: Type.Boolean(), acceptingOwnerEpoch: NonNegativeIntegerSchema,
  commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema, gitConfigHash: HashSchema, gitVersionHash: HashSchema, objectFormat: Type.Enum(["sha1", "sha256"]),
  transactionReceiptHash: HashSchema, transactionReceiptFactHash: HashSchema, landingObservationHash: HashSchema,
});
const DagRunFactBindingV1Schema = Type.Union([GitTransactionFactBindingV1Schema, GitIntegrationReceiptFactBindingV1Schema, ProcessIdentityObservationBindingV1Schema, CorruptFactEnvelopeBindingV1Schema, StageAttemptInputFactBindingV1Schema, WorkerResultFactBindingV1Schema, StageEvidenceFactBindingV1Schema, CheckDispositionFactBindingV1Schema, VerificationFactBindingV1Schema, OracleAssertionFactBindingV1Schema, EffectReconciliationFactBindingV1Schema, QuarantineResolutionFactBindingV1Schema, OwnershipFactBindingV1Schema, CandidateFactBindingV1Schema, EvidenceAdoptionFactBindingV1Schema, IntegrationReadyFactBindingV1Schema, IntegrationFactBindingV1Schema]);
const ProcedureCatalogBindingV1Schema = StrictObject({
  hash: HashSchema, procedureId: IdSchema, purpose: Type.Enum(["lifecycle", "evidence_only_delta_attestation"]), stages: Type.Array(PlanStageIdSchema, { minItems: 1 }),
  producerKinds: Type.Array(ProducerKindSchema, { minItems: 1 }), readOnly: Type.Boolean(), environmentProfileHash: HashSchema,
});
const CheckResultBindingV1Schema = StrictObject({
  checkId: IdSchema, disposition: Type.Enum(["PASS", "WAIVED", "NOT_APPLICABLE"]), applicabilityEvidenceHashes: Type.Array(HashSchema),
});
const OracleAssertionResultBindingV1Schema = StrictObject({ oracleId: IdSchema, assertionId: IdSchema, evidenceHash: HashSchema });
const CheckAggregateBindingV1Schema = StrictObject({
  hash: HashSchema, workItemId: IdSchema, stage: PlanStageIdSchema, procedureHash: HashSchema,
  environmentProfileHash: HashSchema, disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  oracleIds: StringSet(), assertions: Type.Array(OracleAssertionResultBindingV1Schema), checks: Type.Array(CheckResultBindingV1Schema),
});
const DagExecutionCatalogBindingV1Schema = StrictObject({
  lifecycleProfileHash: HashSchema, checkCatalogHash: HashSchema,
  procedures: HashMap(ProcedureCatalogBindingV1Schema), checkAggregates: HashMap(CheckAggregateBindingV1Schema),
});
const DagRunAuthorizationBindingV1Schema = StrictObject({
  hash: HashSchema, planHash: HashSchema, reviewReceiptHash: HashSchema, receiptHashes: Type.Array(HashSchema),
  workItemIds: StringSet({ minItems: 1 }), stageScopes: Type.Record(IdSchema, Type.Array(PlanStageIdSchema)), repositoryIds: StringSet({ minItems: 1 }),
  effectScopeIds: StringSet(), integrationTrainIds: StringSet({ minItems: 1 }), retryCeilingsHash: HashSchema,
  maxActiveNodes: PositiveIntegerSchema, validFrom: TimestampSchema, validUntil: Nullable(TimestampSchema),
});

export const HashRefV1Schema = StrictObject({
  kind: FactKindSchema,
  schemaVersion: Type.Literal(1),
  id: IdSchema,
  hash: HashSchema,
  bytes: NonNegativeIntegerSchema,
  mediaType: Type.Literal("application/json"),
  sensitivity: Type.Enum(["public", "internal", "restricted"]),
  retention: Type.Enum(["ephemeral", "run", "project"]),
  locator: Nullable(RootRelativePathSchema),
});
const SubjectRefV1Schema = StrictObject({
  kind: Type.Enum(["work_item", "gate", "precedence", "repository", "resource", "mutex", "train", "run"]),
  id: IdSchema,
});
const FailureKeyV1Schema = StrictObject({
  failureClass: FailureClassSchema,
  fingerprint: HashSchema,
  procedureId: Nullable(IdSchema),
  checkId: Nullable(IdSchema),
});
const RunIdentityV1Schema = StrictObject({
  projectId: IdSchema,
  planId: IdSchema,
  planRevision: PositiveIntegerSchema,
  planHash: HashSchema,
  planSchemaHash: HashSchema,
  lifecycleProfileHash: HashSchema,
  checkCatalogHash: HashSchema,
  artifactPolicyHash: HashSchema,
  reviewReceipt: HashRefV1Schema,
  authorizationReceipts: Type.Array(HashRefV1Schema),
  authorizationSet: HashRefV1Schema,
  previousRunId: Nullable(IdSchema),
  supersededByRunId: Nullable(IdSchema),
});
const OwnerProjectionV1Schema = StrictObject({
  ownerEpoch: NonNegativeIntegerSchema,
  ownerTokenHash: Nullable(HashSchema),
  sessionId: Nullable(IdSchema),
  pid: NonNegativeIntegerSchema,
  processStartIdentity: Nullable(Type.String({ minLength: 1, maxLength: 256 })),
  lockIdentity: Nullable(Type.String({ minLength: 1, maxLength: 512 })),
  attachedAt: Nullable(TimestampSchema),
  lastHeartbeatAt: Nullable(TimestampSchema),
  ownershipReceipt: Nullable(HashSchema),
  lastReleaseCommandId: Nullable(IdSchema),
  lastReleasePayloadHash: Nullable(HashSchema),
});
const DesiredProjectionV1Schema = StrictObject({
  run: RunDesiredSchema,
  reason: Nullable(BoundedTextSchema),
  requestedAt: TimestampSchema,
  requestedBy: Type.Enum(["user", "conductor", "successor_plan"]),
});
const CurrentProjectionV1Schema = StrictObject({
  run: RunCurrentSchema,
  readyWorkItemIds: StringSet(),
  activeWorkItemIds: StringSet(),
  blockedWorkItemIds: StringSet(),
  integrationReadyWorkItemIds: StringSet(),
  updatedByCommandId: IdSchema,
});
const WorkspaceProjectionV1Schema = StrictObject({
  state: Type.Enum(["unmaterialized", "materializing", "clean", "dirty", "missing", "quarantined"]),
  locator: Nullable(RootRelativePathSchema),
  gitCommonDirIdentityHash: Nullable(HashSchema),
  gitWorktreeIdentityHash: Nullable(HashSchema),
  branchRef: Nullable(Type.String({ minLength: 1, maxLength: 512 })),
  base: Nullable(GitTreeRefV1Schema),
  expectedHead: Nullable(GitTreeRefV1Schema),
  ownerLeaseId: Nullable(IdSchema),
  processDisposition: ProcessDispositionSchema,
  observationReceipt: Nullable(HashSchema),
});
const RepositoryProjectionV1Schema = StrictObject({
  repositoryId: IdSchema,
  planEntityHash: HashSchema,
  role: Type.Enum(["authority", "write", "input", "verification"]),
  baseline: GitTreeRefV1Schema,
  targetRef: Type.String({ minLength: 1, maxLength: 512 }),
  observedTarget: GitTreeRefV1Schema,
  observedTargetAt: TimestampSchema,
  observationReceipt: HashSchema,
  workspace: WorkspaceProjectionV1Schema,
  integrationLockLeaseId: Nullable(IdSchema),
  blockerIds: StringSet(),
});
const CandidatePointerV1Schema = StrictObject({
  generation: PositiveIntegerSchema,
  candidateId: IdSchema,
  candidateHash: HashSchema,
  base: GitTreeRefV1Schema,
  git: GitTreeRefV1Schema,
  patchIdentityHash: HashSchema,
  producedByStageAttemptId: IdSchema,
  lineageHash: HashSchema,
});
const StageProjectionV1Schema = StrictObject({
  stage: PlanStageIdSchema,
  state: StageStateSchema,
  attemptIds: StringSet(),
  currentAttemptId: Nullable(IdSchema),
  currentEvidence: Nullable(HashSchema),
  adoptionReceipt: Nullable(HashSchema),
  invalidationIds: StringSet(),
  lastDisposition: Nullable(CheckDispositionSchema),
  blockerIds: StringSet(),
});
const StageMapV1Schema = StrictObject(Object.fromEntries(PLAN_STAGE_IDS.map((stage) => [stage, StageProjectionV1Schema])) as any);
const WorkItemProjectionV1Schema = StrictObject({
  workItemId: IdSchema,
  planEntityHash: HashSchema,
  writeRepositoryId: IdSchema,
  desired: Type.Enum(["run", "pause", "cancel", "supersede"]),
  current: WorkItemCurrentSchema,
  authorizedStages: Type.Array(PlanStageIdSchema),
  currentStage: Nullable(PlanStageIdSchema),
  implementationLineageHash: HashSchema,
  candidateGeneration: NonNegativeIntegerSchema,
  candidate: Nullable(CandidatePointerV1Schema),
  stages: StageMapV1Schema,
  precedenceIds: StringSet(),
  gateIds: StringSet(),
  laneAdmissionSequence: Nullable(PositiveIntegerSchema),
  admittedAt: Nullable(TimestampSchema),
  activeLeaseIds: StringSet(),
  blockerIds: StringSet(),
  openFindingIds: StringSet(),
  integrationReadyReceipt: Nullable(HashSchema),
  integrationEntryId: Nullable(IdSchema),
  integrationReceipt: Nullable(HashSchema),
  completedAt: Nullable(TimestampSchema),
});
const GateProjectionV1Schema = StrictObject({
  gateId: IdSchema,
  planEntityHash: HashSchema,
  kind: Type.Enum(["model_authority", "contract", "human_authorization", "environment_capability", "external_precondition", "integration"]),
  releaseMode: Type.Enum(["plan_revision", "run_evidence"]),
  state: GateStateSchema,
  releaseReceipt: Nullable(HashSchema),
  invalidationReceipt: Nullable(HashSchema),
  blockedWorkItemStages: Type.Array(StrictObject({ workItemId: IdSchema, stages: Type.Array(PlanStageIdSchema) })),
});
const PrecedenceProjectionV1Schema = StrictObject({
  precedenceId: IdSchema,
  planEntityHash: HashSchema,
  predecessorWorkItemId: IdSchema,
  successorWorkItemId: IdSchema,
  releaseDisposition: Type.Literal("integrated"),
  state: PrecedenceStateSchema,
  satisfyingReceipt: Nullable(HashSchema),
});
const ResourcePoolProjectionV1Schema = StrictObject({
  resourceClassId: IdSchema,
  planEntityHash: HashSchema,
  capacityRevision: NonNegativeIntegerSchema,
  observedCapacity: NonNegativeIntegerSchema,
  semanticMaximum: NonNegativeIntegerSchema,
  allocatedUnits: NonNegativeIntegerSchema,
  leaseIds: StringSet(),
  observationReceipt: HashSchema,
});
const MutexProjectionV1Schema = StrictObject({
  mutexGroupId: IdSchema,
  planEntityHash: HashSchema,
  activeLeaseId: Nullable(IdSchema),
  waitingStageAttemptIds: StringSet(),
});
const LeaseProjectionV1Schema = StrictObject({
  leaseId: IdSchema,
  kind: LeaseKindSchema,
  subject: SubjectRefV1Schema,
  holderStageAttemptId: Nullable(IdSchema),
  holderIntegrationAttemptId: Nullable(IdSchema),
  candidateGeneration: NonNegativeIntegerSchema,
  units: NonNegativeIntegerSchema,
  ownerEpoch: NonNegativeIntegerSchema,
  state: LeaseStateSchema,
  acquiredAt: TimestampSchema,
  expiresAt: Nullable(TimestampSchema),
  releasedAt: Nullable(TimestampSchema),
  releaseReason: Nullable(BoundedTextSchema),
});
const StageAttemptProjectionV1Schema = StrictObject({
  stageAttemptId: IdSchema,
  workItemId: IdSchema,
  stage: PlanStageIdSchema,
  ordinal: PositiveIntegerSchema,
  producerKind: ProducerKindSchema,
  implementationLineageHash: Nullable(HashSchema),
  inputGeneration: NonNegativeIntegerSchema,
  reservedOutputGeneration: Nullable(NonNegativeIntegerSchema),
  attemptInput: HashRefV1Schema,
  authorizationSetHash: HashSchema,
  state: AttemptStateSchema,
  launchIntentId: Nullable(IdSchema),
  leaseIds: StringSet(),
  workerResult: Nullable(HashRefV1Schema),
  evidence: Nullable(HashRefV1Schema),
  failure: Nullable(FailureKeyV1Schema),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema,
  terminalAt: Nullable(TimestampSchema),
});
const LaunchIntentProjectionV1Schema = StrictObject({
  launchIntentId: IdSchema,
  effectId: IdSchema,
  stageAttemptId: IdSchema,
  state: LaunchStateSchema,
  adapter: Type.Literal("owned-worker-v1"),
  launchKey: IdSchema,
  workerId: IdSchema,
  expectedAttemptNumber: PositiveIntegerSchema,
  taskPacketHash: HashSchema,
  cwdRepositoryId: IdSchema,
  configRequestHash: HashSchema,
  dispatchCount: NonNegativeIntegerSchema,
  lastDispatchAt: Nullable(TimestampSchema),
  boundAt: Nullable(TimestampSchema),
  ambiguityReason: Nullable(BoundedTextSchema),
});
const WorkerBindingV1Schema = StrictObject({
  stageAttemptId: IdSchema,
  launchIntentId: IdSchema,
  workerStorageId: IdSchema,
  launchOwnerSessionId: IdSchema,
  workerId: IdSchema,
  attemptNumber: PositiveIntegerSchema,
  attemptNonce: Type.String({ minLength: 16, maxLength: 256 }),
  configHash: HashSchema,
  configRef: HashRefV1Schema,
  supervisorPid: NonNegativeIntegerSchema,
  supervisorStartIdentity: Nullable(Type.String({ minLength: 1, maxLength: 256 })),
  childPid: Nullable(NonNegativeIntegerSchema),
  childStartIdentity: Nullable(Type.String({ minLength: 1, maxLength: 256 })),
  mailboxHash: Nullable(HashSchema),
  heartbeatAt: Nullable(TimestampSchema),
  completionId: Nullable(IdSchema),
  resultHash: Nullable(HashSchema),
  processDisposition: ProcessDispositionSchema,
  retrySafe: Type.Boolean(),
});
const EvidenceIndexV1Schema = StrictObject({
  stageAttemptInputs: IdMap(HashRefV1Schema),
  workerResults: HashMap(HashRefV1Schema),
  candidates: HashMap(HashRefV1Schema),
  stageEvidence: HashMap(HashRefV1Schema),
  checkDispositions: HashMap(HashRefV1Schema),
  verifications: HashMap(HashRefV1Schema),
  oracleAssertions: HashMap(HashRefV1Schema),
  findings: IdMap(HashRefV1Schema),
  findingResolutions: IdMap(HashRefV1Schema),
  waivers: IdMap(HashRefV1Schema),
  invalidations: IdMap(HashRefV1Schema),
  adoptions: IdMap(HashRefV1Schema),
  effectReconciliations: IdMap(HashRefV1Schema),
  integrationReady: IdMap(HashRefV1Schema),
  integrationReceipts: IdMap(HashRefV1Schema),
  stalenessReceipts: HashMap(HashRefV1Schema),
  gateReceipts: IdMap(HashRefV1Schema),
});
const FindingClosureProjectionV1Schema = StrictObject({
  findingId: IdSchema,
  findingHash: HashSchema,
  workItemId: IdSchema,
  introducedByEvidenceHash: HashSchema,
  kind: FindingKindSchema,
  severity: FindingSeveritySchema,
  materiality: FindingMaterialitySchema,
  fingerprint: HashSchema,
  semanticSubjectId: IdSchema,
  state: FindingClosureSchema,
  resolutionHash: Nullable(HashSchema),
  supersedingEvidenceHash: Nullable(HashSchema),
});
const RetryLedgerEntryV1Schema = StrictObject({
  retryKey: HashSchema,
  workItemId: IdSchema,
  stage: PlanStageIdSchema,
  dimension: RetryDimensionSchema,
  procedureId: Nullable(IdSchema),
  failureClass: FailureClassSchema,
  fingerprint: HashSchema,
  count: NonNegativeIntegerSchema,
  ceiling: NonNegativeIntegerSchema,
  authorizationSetHash: HashSchema,
  candidateTrees: Type.Array(HashSchema),
  repairCommitTrees: Type.Array(HashSchema),
  progressHashes: Type.Array(HashSchema),
  failureSequence: Type.Array(HashSchema),
  stop: RetryStopSchema,
  lastRetryCommandId: Nullable(IdSchema),
});
const BlockerProjectionV1Schema = StrictObject({
  blockerId: IdSchema,
  kind: BlockerKindSchema,
  subject: SubjectRefV1Schema,
  stage: Nullable(PlanStageIdSchema),
  sourceId: IdSchema,
  sourceHash: Nullable(HashSchema),
  release: BlockerReleaseSchema,
  active: Type.Boolean(),
  createdAt: TimestampSchema,
  releasedAt: Nullable(TimestampSchema),
  releaseReceipt: Nullable(HashSchema),
});
export const EffectProjectionV1Schema = StrictObject({
  effectId: IdSchema,
  kind: Type.Enum(["put_immutable_fact", "launch_worker", "cancel_worker", "materialize_workspace", "run_procedure", "reconcile_external_effect", "compose_candidate", "verify_prefix", "land_target"]),
  subject: SubjectRefV1Schema,
  effectScopeId: Nullable(IdSchema),
  effectScopeKind: Nullable(Type.Enum(["reversible_evaluation", "external_write", "production", "irreversible"])),
  provider: Nullable(IdSchema),
  procedureClass: ProcedureClassSchema,
  requestHash: HashSchema,
  boundOwnerEpoch: NonNegativeIntegerSchema,
  boundAuthorizationSetHash: HashSchema,
  boundFreshnessReceiptHash: HashSchema,
  boundCandidateGeneration: NonNegativeIntegerSchema,
  boundGateEpochHash: HashSchema,
  state: EffectStateSchema,
  dispatchCount: NonNegativeIntegerSchema,
  createdRevision: NonNegativeIntegerSchema,
  createdAt: TimestampSchema,
  lastDispatchAt: Nullable(TimestampSchema),
  observationHash: Nullable(HashSchema),
  reconciliation: ReconciliationSchema,
  blockerId: Nullable(IdSchema),
});
const CancellationProjectionV1Schema = StrictObject({
  cancellationId: IdSchema,
  scope: CancellationScopeSchema,
  subjectId: IdSchema,
  fencedGenerations: Type.Record(IdSchema, NonNegativeIntegerSchema),
  state: CancellationStateSchema,
  reason: BoundedTextSchema,
  requestedAt: TimestampSchema,
  effectIds: StringSet(),
  resultHash: Nullable(HashSchema),
});
export const QuarantineProjectionV1Schema = StrictObject({
  quarantineId: IdSchema,
  fact: HashRefV1Schema,
  reason: QuarantineReasonSchema,
  observedBindingHash: HashSchema,
  expectedBindingHash: Nullable(HashSchema),
  state: QuarantineStateSchema,
  observedAt: TimestampSchema,
  adoptionReceipt: Nullable(HashSchema),
  rejectionReason: Nullable(BoundedTextSchema),
});
const IntegrationEntryProjectionV1Schema = StrictObject({
  entryId: IdSchema,
  workItemId: IdSchema,
  ordinal: NonNegativeIntegerSchema,
  state: TrainEntryStateSchema,
  integrationReadyHash: HashSchema,
  sourceCandidate: CandidatePointerV1Schema,
  attemptIds: StringSet(),
  currentAttemptId: Nullable(IdSchema),
  integrationReceipt: Nullable(HashSchema),
  blockerIds: StringSet(),
});
const IntegrationTrainProjectionV1Schema = StrictObject({
  repositoryId: IdSchema,
  planTrainHash: HashSchema,
  strategy: Type.Literal("merge_tree_one_parent"),
  targetRef: Type.String({ minLength: 1, maxLength: 512 }),
  expectedTarget: GitTreeRefV1Schema,
  acceptedPrefix: GitTreeRefV1Schema,
  acceptedPrefixOrdinal: NonNegativeIntegerSchema,
  acceptedPrefixReceipt: Nullable(HashSchema),
  entryOrder: StringSet(),
  entries: IdMap(IntegrationEntryProjectionV1Schema),
  activeIntegrationAttemptId: Nullable(IdSchema),
  lockLeaseId: Nullable(IdSchema),
  blockerIds: StringSet(),
});
const IntegrationAttemptProjectionV1Schema = StrictObject({
  integrationAttemptId: IdSchema,
  entryId: IdSchema,
  retryOrdinal: NonNegativeIntegerSchema,
  sourceCandidateHash: HashSchema,
  strategy: Type.Literal("merge_tree_one_parent"),
  compositionProfileHash: HashSchema,
  prefixValidationProfileHash: HashSchema,
  finalValidationProfileHash: HashSchema,
  sourceBase: GitTreeRefV1Schema,
  sourceCandidate: GitTreeRefV1Schema,
  expectedPrefix: GitTreeRefV1Schema,
  expectedTarget: GitTreeRefV1Schema,
  temporaryRef: Type.String({ minLength: 1, maxLength: 512 }),
  temporaryWorkspaceReceipt: Nullable(HashSchema),
  compositionEffectId: IdSchema,
  composedTree: Nullable(GitTreeRefV1Schema),
  syntheticParentCommit: Nullable(GitOidSchema),
  sourceToIntegratedLineageHash: Nullable(HashSchema),
  conflictClass: ConflictClassSchema,
  prefixEvidenceHashes: Type.Array(HashSchema),
  finalEvidenceHashes: Type.Array(HashSchema),
  environmentClosureHash: Nullable(HashSchema),
  landingEffectId: Nullable(IdSchema),
  landingState: LandingStateSchema,
  intendedLandedTree: Nullable(GitTreeRefV1Schema),
  integrationReceipt: Nullable(HashSchema),
  repositoryBindingFactHash: Nullable(HashSchema),
  privateRefFactHashes: Type.Array(HashSchema),
  compositionFactHash: Nullable(HashSchema),
  proposalVerificationFactHash: Nullable(HashSchema),
  landingObservationFactHash: Nullable(HashSchema),
});
const FreshnessProjectionV1Schema = StrictObject({
  class: FreshnessClassSchema,
  receipt: HashRefV1Schema,
  evaluatedPlanHash: HashSchema,
  modelClosureHash: HashSchema,
  repositoryObservationHashes: Type.Record(IdSchema, HashSchema),
  affectedWorkItemIds: StringSet(),
  blocksNewLaunches: Type.Boolean(),
  blocksIntegration: Type.Boolean(),
  evaluatedAt: TimestampSchema,
});
const CompletionProjectionV1Schema = StrictObject({
  state: RunCompletionSchema,
  authorizedScopeHash: HashSchema,
  completeWorkItemIds: StringSet(),
  remainingAuthorizedWorkItemIds: StringSet(),
  unauthorizedWorkItemIds: StringSet(),
  completedRepositoryIds: StringSet(),
  completedAt: Nullable(TimestampSchema),
});
const OperationalCapacityV1Schema = StrictObject({
  namespace: IdSchema,
  observedCapacity: NonNegativeIntegerSchema,
  allocatedUnits: NonNegativeIntegerSchema,
  reservationIds: StringSet(),
  observationHash: HashSchema,
});
const SchedulerReservationV1Schema = StrictObject({
  reservationId: IdSchema,
  reservationSequence: PositiveIntegerSchema,
  workItemId: IdSchema,
  stage: PlanStageIdSchema,
  attemptOrdinal: PositiveIntegerSchema,
  operationKind: Type.Enum(["conductor", "implementation", "evaluation", "codification", "verification", "review", "hardening", "integration"]),
  state: Type.Enum(["reserved", "dispatch_intent", "launch_ambiguous", "active", "release_requested", "released", "fenced"]),
  candidateGeneration: NonNegativeIntegerSchema,
  ownerEpoch: NonNegativeIntegerSchema,
  authorizationSetHash: HashSchema,
  normalizedRequestHash: HashSchema,
  leaseIds: StringSet(),
  mutexGroupIds: StringSet(),
  resourceUnits: Type.Record(IdSchema, NonNegativeIntegerSchema),
  operationalUnits: Type.Record(IdSchema, NonNegativeIntegerSchema),
  workerRole: Type.Enum(["none", "implementation", "evaluator", "reviewer"]),
  repositoryId: IdSchema,
  createdAt: TimestampSchema,
  releasedAt: Nullable(TimestampSchema),
});
const ActiveNodeLaneV1Schema = StrictObject({
  workItemId: IdSchema,
  admissionSequence: PositiveIntegerSchema,
  admittedAt: TimestampSchema,
  releaseDisposition: Nullable(Type.Enum(["integrated", "terminal_cancelled", "successor_plan", "replan"])),
  releasedAt: Nullable(TimestampSchema),
});
const DynamicExclusionV1Schema = StrictObject({
  exclusionId: IdSchema,
  workItemIds: StringSet({ minItems: 2 }),
  phases: Type.Array(PlanStageIdSchema, { minItems: 1 }),
  repositoryIds: StringSet(),
  reason: Type.Enum(["observed_incompatibility", "provider_hold", "environment_hold", "operator_hold", "structural_deadlock"]),
  evidenceHash: HashSchema,
  creator: Type.Enum(["conductor", "accepted_policy", "human_receipt"]),
  releasePredicateHash: HashSchema,
  state: Type.Enum(["active", "released"]),
  createdAt: TimestampSchema,
  releasedAt: Nullable(TimestampSchema),
});
const IdempotencySlotV1Schema = StrictObject({
  slotId: HashSchema, inputType: IdSchema, commandId: IdSchema, idempotencyKey: Type.String({ minLength: 1, maxLength: 256 }),
  payloadHash: HashSchema, inputHash: HashSchema, appliedRevision: PositiveIntegerSchema,
});
const SchedulerProjectionV1Schema = StrictObject({
  policyVersion: Type.String({ minLength: 1, maxLength: 64 }),
  policyHash: HashSchema,
  normalizedIndexHash: HashSchema,
  maxActiveNodes: PositiveIntegerSchema,
  decisionSequence: NonNegativeIntegerSchema,
  nextReservationSequence: PositiveIntegerSchema,
  lastDecisionCommandId: Nullable(IdSchema),
  activeNodeLanes: IdMap(ActiveNodeLaneV1Schema),
  reservations: IdMap(SchedulerReservationV1Schema),
  bypassCounters: Type.Record(IdSchema, NonNegativeIntegerSchema),
  fairnessCounters: Type.Record(IdSchema, NonNegativeIntegerSchema),
  dynamicExclusions: IdMap(DynamicExclusionV1Schema),
  providerHoldIds: StringSet(),
  operationalCapacities: IdMap(OperationalCapacityV1Schema),
});

export const DagRunStateV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("DagRunStateV1"),
  canonicalization: Type.Literal("jcs-v1"),
  runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }),
  revision: NonNegativeIntegerSchema,
  snapshotHash: HashSchema,
  previousSnapshotHash: Nullable(HashSchema),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema,
  identity: RunIdentityV1Schema,
  owner: OwnerProjectionV1Schema,
  desired: DesiredProjectionV1Schema,
  current: CurrentProjectionV1Schema,
  repositories: IdMap(RepositoryProjectionV1Schema),
  workItems: IdMap(WorkItemProjectionV1Schema),
  gates: IdMap(GateProjectionV1Schema),
  precedence: IdMap(PrecedenceProjectionV1Schema),
  resourcePools: IdMap(ResourcePoolProjectionV1Schema),
  mutexes: IdMap(MutexProjectionV1Schema),
  leases: IdMap(LeaseProjectionV1Schema),
  stageAttempts: IdMap(StageAttemptProjectionV1Schema),
  launchIntents: IdMap(LaunchIntentProjectionV1Schema),
  workerBindings: IdMap(WorkerBindingV1Schema),
  evidenceIndex: EvidenceIndexV1Schema,
  findingClosures: IdMap(FindingClosureProjectionV1Schema),
  retryLedger: HashMap(RetryLedgerEntryV1Schema),
  blockers: IdMap(BlockerProjectionV1Schema),
  effects: IdMap(EffectProjectionV1Schema),
  cancellations: IdMap(CancellationProjectionV1Schema),
  quarantine: IdMap(QuarantineProjectionV1Schema),
  idempotencySlots: Type.Record(HashSchema, IdempotencySlotV1Schema),
  integrationTrains: IdMap(IntegrationTrainProjectionV1Schema),
  integrationAttempts: IdMap(IntegrationAttemptProjectionV1Schema),
  scheduler: SchedulerProjectionV1Schema,
  freshness: FreshnessProjectionV1Schema,
  completion: CompletionProjectionV1Schema,
});
export type DagRunStateV1 = Static<typeof DagRunStateV1Schema>;
export const DAG_RUN_STATE_SCHEMA_HASH = canonicalHash(JSON.parse(JSON.stringify(DagRunStateV1Schema)));

export function dagRunSnapshotHash(state: Omit<DagRunStateV1, "snapshotHash"> | DagRunStateV1): string {
  return hashWithoutField(state as unknown as Record<string, unknown>, "snapshotHash");
}

export function validateDagRunStateShapeV1(value: unknown): ValidationResult<DagRunStateV1> {
  const issues = schemaIssues(DagRunStateV1Schema, value);
  return issues.length ? { ok: false, issues } : { ok: true, value: value as DagRunStateV1, issues };
}

export function validateDagRunStateV1(value: unknown, context?: DagRunValidationContextV1): ValidationResult<DagRunStateV1> {
  const shape = validateDagRunStateShapeV1(value);
  if (!shape.ok) return shape;
  const issues: ValidationIssue[] = [];
  if (!context) issues.push({ path: "/", message: "exact DagRunStateV1 validation requires the bound canonical plan and validated immutable fact bindings" });
  else {
    const planResult = validateCanonicalDagPlanV1(context.plan);
    if (!planResult.ok) issues.push({ path: "/identity/planHash", message: "validation context contains an invalid canonical plan" });
    const authorizationIssues = schemaIssues(DagRunAuthorizationBindingV1Schema, context.authorization);
    for (const [hash, authorization] of Object.entries(context.historicalAuthorizations ?? {})) {
      const historicalIssues = schemaIssues(DagRunAuthorizationBindingV1Schema, authorization);
      authorizationIssues.push(...historicalIssues.map((issue) => ({ ...issue, path: `/historical/${hash}${issue.path === "/" ? "" : issue.path}` })));
      if (authorization?.hash !== hash) authorizationIssues.push({ path: `/historical/${hash}/hash`, message: "historical authorization key must equal its hash" });
    }
    const catalogIssues = schemaIssues(DagExecutionCatalogBindingV1Schema, context.catalog);
    pushIssue(issues, "/scheduler/normalizedIndexHash", /^sha256:[0-9a-f]{64}$/.test(context.normalizedSchedulerIndexHash ?? ""), "validation context requires a canonical scheduler-index hash");
    for (const issue of authorizationIssues) issues.push({ path: `/authorization${issue.path === "/" ? "" : issue.path}`, message: issue.message });
    for (const issue of catalogIssues) issues.push({ path: `/catalog${issue.path === "/" ? "" : issue.path}`, message: issue.message });
    let factShapeValid = true;
    for (const [hash, fact] of Object.entries(context.facts ?? {})) {
      const factIssues = schemaIssues(DagRunFactBindingV1Schema, fact);
      if (factIssues.length) factShapeValid = false;
      for (const issue of factIssues) issues.push({ path: `/facts/${hash}${issue.path === "/" ? "" : issue.path}`, message: issue.message });
      pushIssue(issues, `/facts/${hash}/hash`, Boolean(fact && typeof fact === "object" && (fact as any).hash === hash), "fact binding key must equal its canonical hash");
      if (fact && typeof fact === "object" && (fact as any).hash === hash) pushIssue(issues, `/facts/${hash}/hash`, hash === hashWithoutField(fact as unknown as Record<string, unknown>, "hash"), "fact hash must equal canonical immutable fact content");
    }
    if (planResult.ok && !authorizationIssues.length && !catalogIssues.length && factShapeValid) validateRunSemantics(shape.value!, context, issues);
  }
  return issues.length ? { ok: false, issues } : { ok: true, value: shape.value, issues };
}

export function parseDagRunStateV1(text: string, context: DagRunValidationContextV1): DagRunStateV1 {
  const value = parseStrictJson(text);
  assertDagRunStateV1(value, context);
  return value;
}

export function assertDagRunStateV1(value: unknown, context: DagRunValidationContextV1): asserts value is DagRunStateV1 {
  const result = validateDagRunStateV1(value, context);
  if (!result.ok) throw new Error(`Invalid DagRunStateV1:\n${result.issues.map(({ path, message }) => `- ${path}: ${message}`).join("\n")}`);
}

function validateRunSemantics(state: DagRunStateV1, context: DagRunValidationContextV1, issues: ValidationIssue[]): void {
  validateTimestampFields(state, issues);
  pushIssue(issues, "/snapshotHash", state.snapshotHash === dagRunSnapshotHash(state), "does not match canonical snapshot content");
  pushIssue(issues, "/previousSnapshotHash", state.revision === 0 ? state.previousSnapshotHash === null : state.previousSnapshotHash !== null, "must be null only at revision zero");
  pushIssue(issues, "/freshness/evaluatedPlanHash", state.freshness.evaluatedPlanHash === state.identity.planHash, "must bind the run planHash");
  pushIssue(issues, "/freshness/modelClosureHash", state.freshness.modelClosureHash === context.plan.modelBinding.closure.closureHash, "must bind the canonical plan model closure");
  validateRunPlanJoin(state, context.plan, issues);
  pushIssue(issues, "/scheduler/normalizedIndexHash", state.scheduler.normalizedIndexHash === context.normalizedSchedulerIndexHash, "must match the validated deterministic scheduler index");
  validateCatalogJoin(state, context, issues);
  validateRunAuthorizationJoin(state, context, issues);
  pushIssue(issues, "/identity/reviewReceipt/kind", state.identity.reviewReceipt.kind === "plan_review", "must be a plan_review fact");
  state.identity.authorizationReceipts.forEach((receipt, index) => pushIssue(issues, `/identity/authorizationReceipts/${index}/kind`, receipt.kind === "plan_authorization", "must be a plan_authorization fact"));
  pushIssue(issues, "/identity/authorizationSet/kind", state.identity.authorizationSet.kind === "authorization_set", "must be an authorization_set fact");
  pushIssue(issues, "/freshness/receipt/kind", state.freshness.receipt.kind === "staleness", "must be a staleness fact");
  const ownerAttached = state.owner.sessionId !== null;
  pushIssue(issues, "/owner", ownerAttached === (state.owner.ownerTokenHash !== null && state.owner.processStartIdentity !== null && state.owner.lockIdentity !== null && state.owner.attachedAt !== null && state.owner.ownershipReceipt !== null && state.owner.pid > 0), "attached owner requires complete token, process, lock, time, and receipt identity");
  pushIssue(issues, "/owner/ownerEpoch", !ownerAttached || state.owner.ownerEpoch > 0, "attached owner requires a positive fencing epoch; detached runs retain their last epoch");
  if (ownerAttached) {
    const ownership = context.facts[state.owner.ownershipReceipt!];
    pushIssue(issues, "/owner/ownershipReceipt", ownership?.kind === "ownership" && ownership.hash === state.owner.ownershipReceipt && ownership.hash === hashWithoutField(ownership as unknown as Record<string, unknown>, "hash") && ownership.runId === state.runId && ownership.runNonce === state.runNonce && ownership.successorSessionId === state.owner.sessionId && ownership.successorPid === state.owner.pid && ownership.successorProcessStartIdentity === state.owner.processStartIdentity && ownership.successorLockIdentity === state.owner.lockIdentity, "must resolve exact canonical ownership evidence for the attached process identity");
    if (ownership?.kind === "ownership" && ownership.disposition === "dead") {
      const observation = ownership.priorObservationHash ? context.facts[ownership.priorObservationHash] : undefined;
      const priorLock = ownership.priorSessionId && ownership.priorOwnerTokenHash && ownership.priorProcessStartIdentity && ownership.priorLockIdentity && ownership.priorAttachedAt ? { lockIdentity: ownership.priorLockIdentity, ownerTokenHash: ownership.priorOwnerTokenHash, sessionId: ownership.priorSessionId, pid: ownership.priorPid, processStartIdentity: ownership.priorProcessStartIdentity, acquiredAt: ownership.priorAttachedAt } : null;
      pushIssue(issues, "/owner/ownershipReceipt", observation?.kind === "process_identity_observation" && observation.hash === ownership.priorObservationHash && observation.hash === hashWithoutField(observation as unknown as Record<string, unknown>, "hash") && priorLock !== null && observation.lockMetadataHash === canonicalHash(priorLock) && observation.pid === ownership.priorPid && observation.processStartIdentity === ownership.priorProcessStartIdentity && ["dead_missing", "dead_reused"].includes(observation.disposition), "dead-owner attachment must retain exact durable prior process observation evidence");
    }
  }
  validateMapKeys(state.repositories, "repositoryId", "/repositories", issues);
  validateMapKeys(state.workItems, "workItemId", "/workItems", issues);
  validateMapKeys(state.gates, "gateId", "/gates", issues);
  validateMapKeys(state.precedence, "precedenceId", "/precedence", issues);
  validateMapKeys(state.resourcePools, "resourceClassId", "/resourcePools", issues);
  validateMapKeys(state.mutexes, "mutexGroupId", "/mutexes", issues);
  validateMapKeys(state.leases, "leaseId", "/leases", issues);
  validateMapKeys(state.stageAttempts, "stageAttemptId", "/stageAttempts", issues);
  validateMapKeys(state.launchIntents, "launchIntentId", "/launchIntents", issues);
  validateMapKeys(state.workerBindings, "stageAttemptId", "/workerBindings", issues);
  validateMapKeys(state.findingClosures, "findingId", "/findingClosures", issues);
  validateMapKeys(state.retryLedger, "retryKey", "/retryLedger", issues);
  validateMapKeys(state.blockers, "blockerId", "/blockers", issues);
  validateMapKeys(state.effects, "effectId", "/effects", issues);
  validateMapKeys(state.cancellations, "cancellationId", "/cancellations", issues);
  validateMapKeys(state.quarantine, "quarantineId", "/quarantine", issues);
  validateMapKeys(state.idempotencySlots, "slotId", "/idempotencySlots", issues);
  pushIssue(issues, "/idempotencySlots", new Set(Object.values(state.idempotencySlots).map(({ commandId }) => commandId)).size === Object.keys(state.idempotencySlots).length, "command IDs must be globally unique across natural idempotency slots");
  pushIssue(issues, "/idempotencySlots", new Set(Object.values(state.idempotencySlots).map(({ idempotencyKey }) => idempotencyKey)).size === Object.keys(state.idempotencySlots).length, "idempotency keys must be globally unique across natural slots");
  const appliedRevisions = Object.values(state.idempotencySlots).map(({ appliedRevision }) => appliedRevision).sort((left, right) => left - right);
  pushIssue(issues, "/idempotencySlots", appliedRevisions.length === state.revision && appliedRevisions.every((revision, index) => revision === index + 1), "must contain exactly one durable applied input slot for every committed revision and no phantom/future slots");
  if (state.revision > 0) {
    const latestSlot = Object.values(state.idempotencySlots).find(({ appliedRevision }) => appliedRevision === state.revision);
    pushIssue(issues, "/current/updatedByCommandId", latestSlot?.commandId === state.current.updatedByCommandId, "must match the exact command recorded for the current revision");
  }
  validateMapKeys(state.integrationTrains, "repositoryId", "/integrationTrains", issues);
  validateMapKeys(state.integrationAttempts, "integrationAttemptId", "/integrationAttempts", issues);
  validateMapKeys(state.scheduler.activeNodeLanes, "workItemId", "/scheduler/activeNodeLanes", issues);
  validateMapKeys(state.scheduler.reservations, "reservationId", "/scheduler/reservations", issues);
  validateMapKeys(state.scheduler.dynamicExclusions, "exclusionId", "/scheduler/dynamicExclusions", issues);
  const activeLaneIds = Object.values(state.scheduler.activeNodeLanes)
    .filter(({ releaseDisposition }) => releaseDisposition === null)
    .map(({ workItemId }) => workItemId).sort();
  pushIssue(issues, "/scheduler/activeNodeLanes", activeLaneIds.length <= state.scheduler.maxActiveNodes, "active sticky lanes cannot exceed maxActiveNodes");
  pushIssue(issues, "/scheduler/activeNodeLanes", new Set(Object.values(state.scheduler.activeNodeLanes).map(({ admissionSequence }) => admissionSequence)).size === Object.keys(state.scheduler.activeNodeLanes).length, "admissionSequence values must be unique");
  for (const [workItemId, lane] of Object.entries(state.scheduler.activeNodeLanes)) {
    pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}/workItemId`, Boolean(state.workItems[workItemId]), "references an unknown work item");
    pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}/releasedAt`, lane.releaseDisposition === null ? lane.releasedAt === null : lane.releasedAt !== null, "release disposition and releasedAt must change together");
    const current = state.workItems[workItemId]?.current;
    if (lane.releaseDisposition === null) pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}`, ["active", "blocked", "integration_ready", "integrating"].includes(current ?? ""), "unreleased lane requires an admitted nonterminal work item");
    if (lane.releaseDisposition === "integrated") pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}/releaseDisposition`, current === "complete", "integrated lane release requires a complete work item");
    if (lane.releaseDisposition === "terminal_cancelled") pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}/releaseDisposition`, current === "cancelled", "terminal cancellation lane release requires a cancelled work item");
    if (["successor_plan", "replan"].includes(lane.releaseDisposition ?? "")) pushIssue(issues, `/scheduler/activeNodeLanes/${workItemId}/releaseDisposition`, current === "superseded", "successor-plan/replan lane release requires a superseded work item");
  }
  for (const [reservationId, reservation] of Object.entries(state.scheduler.reservations)) {
    pushIssue(issues, `/scheduler/reservations/${reservationId}/workItemId`, Boolean(state.workItems[reservation.workItemId]), "references an unknown work item");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/repositoryId`, Boolean(state.repositories[reservation.repositoryId]), "references an unknown repository");
    const item = state.workItems[reservation.workItemId];
    const planItem = context.plan.workItems.find(({ workItemId }) => workItemId === reservation.workItemId);
    const reservationTerminal = ["released", "fenced"].includes(reservation.state);
    pushIssue(issues, `/scheduler/reservations/${reservationId}/candidateGeneration`, reservationTerminal ? reservation.candidateGeneration <= (item?.candidateGeneration ?? -1) : item?.candidateGeneration === reservation.candidateGeneration, "active reservation must bind current generation; terminal reservation may retain an older fenced generation");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/ownerEpoch`, reservationTerminal ? reservation.ownerEpoch <= state.owner.ownerEpoch : reservation.ownerEpoch === state.owner.ownerEpoch, "active reservation must bind current owner epoch");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/authorizationSetHash`, reservationTerminal || (reservation.authorizationSetHash === state.identity.authorizationSet.hash && item?.authorizedStages.includes(reservation.stage)), "active reservation must bind current authority for the reserved stage");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/repositoryId`, reservation.repositoryId === item?.writeRepositoryId, "must bind the work item's write repository");
    const expectedOperation: Record<string, string[]> = { F0: ["conductor"], F1: ["implementation"], F2: ["evaluation"], F3: ["codification"], F4: ["verification"], F5: ["review"], F6: ["hardening"], F7: ["verification"], F8: item?.current === "integration_ready" || item?.current === "integrating" ? ["integration"] : ["conductor"] };
    pushIssue(issues, `/scheduler/reservations/${reservationId}/operationKind`, expectedOperation[reservation.stage].includes(reservation.operationKind), "must use the fixed F0-F8/integration operation class");
    const expectedMutexes = context.plan.constraints.semanticMutexes.filter((mutex) => mutex.members.some((member) => member.workItemId === reservation.workItemId && member.phases.includes(reservation.stage))).map(({ mutexGroupId }) => mutexGroupId).sort();
    pushIssue(issues, `/scheduler/reservations/${reservationId}/mutexGroupIds`, expectedMutexes.every((id) => reservation.mutexGroupIds.includes(id)), "must reserve every applicable plan semantic mutex");
    for (const demand of planItem?.resourceDemands.filter(({ phases }) => phases.includes(reservation.stage)) ?? []) {
      const resource = context.plan.constraints.resourceClasses.find(({ resourceClassId }) => resourceClassId === demand.resourceClassId);
      const units = reservation.resourceUnits[demand.resourceClassId];
      pushIssue(issues, `/scheduler/reservations/${reservationId}/resourceUnits/${demand.resourceClassId}`, units >= demand.units && units <= (resource?.semanticMaximum ?? -1), "must cover declared demand without exceeding the semantic maximum");
    }
    reservation.leaseIds.forEach((leaseId) => pushIssue(issues, `/scheduler/reservations/${reservationId}/leaseIds`, Boolean(state.leases[leaseId]), "references an unknown lease"));
    reservation.mutexGroupIds.forEach((mutexId) => pushIssue(issues, `/scheduler/reservations/${reservationId}/mutexGroupIds`, Boolean(state.mutexes[mutexId]), "references an unknown mutex"));
    for (const resourceId of Object.keys(reservation.resourceUnits)) pushIssue(issues, `/scheduler/reservations/${reservationId}/resourceUnits/${resourceId}`, Boolean(state.resourcePools[resourceId]), "references an unknown resource pool");
    for (const namespace of Object.keys(reservation.operationalUnits)) pushIssue(issues, `/scheduler/reservations/${reservationId}/operationalUnits/${namespace}`, Boolean(state.scheduler.operationalCapacities[namespace]), "references an unknown operational capacity");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/reservationSequence`, reservation.reservationSequence < state.scheduler.nextReservationSequence, "must be lower than nextReservationSequence");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/releasedAt`, ["released", "fenced"].includes(reservation.state) ? reservation.releasedAt !== null : reservation.releasedAt === null, "terminal reservation state and releasedAt must change together");
    if (!["released", "fenced"].includes(reservation.state)) pushIssue(issues, `/scheduler/reservations/${reservationId}/workItemId`, activeLaneIds.includes(reservation.workItemId), "current reservation requires a sticky active-node lane");
  }
  for (const [exclusionId, exclusion] of Object.entries(state.scheduler.dynamicExclusions)) {
    exclusion.workItemIds.forEach((id) => pushIssue(issues, `/scheduler/dynamicExclusions/${exclusionId}/workItemIds`, Boolean(state.workItems[id]), "references an unknown work item"));
    exclusion.repositoryIds.forEach((id) => pushIssue(issues, `/scheduler/dynamicExclusions/${exclusionId}/repositoryIds`, Boolean(state.repositories[id]), "references an unknown repository"));
    pushIssue(issues, `/scheduler/dynamicExclusions/${exclusionId}/releasedAt`, exclusion.state === "released" ? exclusion.releasedAt !== null : exclusion.releasedAt === null, "release state and releasedAt must change together");
    if (exclusion.state === "active") {
      const conflictingReservations = Object.values(state.scheduler.reservations).filter((reservation) => !["released", "fenced"].includes(reservation.state) && exclusion.workItemIds.includes(reservation.workItemId) && exclusion.phases.includes(reservation.stage));
      pushIssue(issues, `/scheduler/dynamicExclusions/${exclusionId}`, new Set(conflictingReservations.map(({ workItemId }) => workItemId)).size <= 1, "active exclusion requires later conflicting reservations to be fenced before continued execution");
    }
  }
  state.scheduler.providerHoldIds.forEach((blockerId) => pushIssue(issues, "/scheduler/providerHoldIds", state.blockers[blockerId]?.active, "must reference an active blocker"));
  for (const [namespace, capacity] of Object.entries(state.scheduler.operationalCapacities)) {
    pushIssue(issues, `/scheduler/operationalCapacities/${namespace}/namespace`, capacity.namespace === namespace, "must equal map key");
    const activeLeases = Object.values(state.leases).filter((lease) => lease.kind === "resource" && lease.subject.id === namespace && ["active", "release_requested", "expired"].includes(lease.state));
    const activeIds = [...new Set(activeLeases.flatMap((lease) => Object.values(state.scheduler.reservations).filter((reservation) => reservation.leaseIds.includes(lease.leaseId)).map(({ reservationId }) => reservationId)))].sort();
    const allocated = activeLeases.reduce((sum, lease) => sum + lease.units, 0);
    pushIssue(issues, `/scheduler/operationalCapacities/${namespace}/reservationIds`, sameStrings(capacity.reservationIds, activeIds), "must list exact active operational reservations");
    pushIssue(issues, `/scheduler/operationalCapacities/${namespace}/allocatedUnits`, capacity.allocatedUnits === allocated, "must equal active operational reservation units");
  }

  validateEvidenceIndex(state, issues);

  pushIssue(issues, "/freshness/repositoryObservationHashes", sameStrings(Object.keys(state.freshness.repositoryObservationHashes).sort(), Object.keys(state.repositories).sort()), "must cover every plan repository exactly");
  for (const [repositoryId, repository] of Object.entries(state.repositories)) {
    pushIssue(issues, `/repositories/${repositoryId}/baseline/repositoryId`, repository.baseline.repositoryId === repositoryId, "must match repository map key");
    pushIssue(issues, `/freshness/repositoryObservationHashes/${repositoryId}`, state.freshness.repositoryObservationHashes[repositoryId] === repository.observationReceipt, "must match the current repository observation receipt");
    pushIssue(issues, `/repositories/${repositoryId}/observedTarget/repositoryId`, repository.observedTarget.repositoryId === repositoryId, "must match repository map key");
    if (repository.workspace.base) pushIssue(issues, `/repositories/${repositoryId}/workspace/base/repositoryId`, repository.workspace.base.repositoryId === repositoryId, "must match repository map key");
  }
  for (const [precedenceId, edge] of Object.entries(state.precedence)) {
    const predecessor = state.workItems[edge.predecessorWorkItemId];
    const successor = state.workItems[edge.successorWorkItemId];
    pushIssue(issues, `/precedence/${precedenceId}/predecessorWorkItemId`, Boolean(predecessor), "references an unknown predecessor");
    pushIssue(issues, `/precedence/${precedenceId}/successorWorkItemId`, Boolean(successor), "references an unknown successor");
    const integrated = predecessor?.current === "complete" && predecessor.integrationReceipt !== null;
    if (edge.state === "satisfied") pushIssue(issues, `/precedence/${precedenceId}/state`, integrated && edge.satisfyingReceipt === predecessor.integrationReceipt, "satisfaction requires the predecessor's accepted integration receipt");
    if (edge.state === "waiting") pushIssue(issues, `/precedence/${precedenceId}/state`, !integrated && edge.satisfyingReceipt === null, "waiting precedence cannot hide an integrated predecessor or carry a receipt");
  }
  for (const [gateId, gate] of Object.entries(state.gates)) {
    if (gate.state === "released") {
      pushIssue(issues, `/gates/${gateId}/state`, gate.releaseMode === "run_evidence" && gate.releaseReceipt !== null, "only run-evidence gates may release inside a run with an immutable receipt");
      pushIssue(issues, `/gates/${gateId}/releaseReceipt`, state.evidenceIndex.gateReceipts[gateId]?.hash === gate.releaseReceipt, "must match indexed gate-release evidence");
    }
    if (gate.state === "closed") pushIssue(issues, `/gates/${gateId}/releaseReceipt`, gate.releaseReceipt === null, "closed gate cannot carry a release receipt");
  }

  pushIssue(issues, "/workItems", new Set(Object.values(state.workItems).map(({ implementationLineageHash }) => implementationLineageHash)).size === Object.keys(state.workItems).length, "implementation lineage hashes must be unique per work item");
  for (const [workItemId, item] of Object.entries(state.workItems)) {
    const path = `/workItems/${workItemId}`; const integrationConflictFence = exactIntegrationConflictFence(state, context, item);
    pushIssue(issues, `${path}/writeRepositoryId`, Boolean(state.repositories[item.writeRepositoryId]), "references an unknown repository");
    pushIssue(issues, `${path}/authorizedStages`, isSortedUnique(item.authorizedStages), "must be sorted and deduplicated");
    if (item.currentStage) pushIssue(issues, `${path}/currentStage`, item.authorizedStages.includes(item.currentStage), "current stage must be within exact authorization scope");
    if (item.current === "ready") {
      pushIssue(issues, `${path}/current`, item.desired === "run" && ["running", "paused"].includes(state.desired.run), "ready work requires runnable item intent; a global pause may retain readiness while blocking dispatch");
      pushIssue(issues, `${path}/current`, item.authorizedStages.includes("F0"), "ready work requires F0 authorization");
      pushIssue(issues, `${path}/precedenceIds`, item.precedenceIds.every((id) => state.precedence[id]?.state === "satisfied"), "ready work requires every causal predecessor integrated and satisfied");
      pushIssue(issues, `${path}/gateIds`, item.gateIds.every((id) => state.gates[id]?.state === "released"), "ready work requires every gate released");
      pushIssue(issues, `${path}/blockerIds`, item.blockerIds.every((id) => !state.blockers[id]?.active), "ready work cannot have active blockers");
      pushIssue(issues, `${path}/current`, !state.freshness.blocksNewLaunches && ["valid_exact", "valid_revalidated"].includes(state.freshness.class), "ready work requires current validated plan freshness");
      pushIssue(issues, `${path}/current`, !activeLaneIds.includes(workItemId), "ready work has not yet acquired a sticky active-node lane");
    }
    const lane = state.scheduler.activeNodeLanes[workItemId];
    pushIssue(issues, `${path}/laneAdmissionSequence`, (item.laneAdmissionSequence === null) === (item.admittedAt === null), "lane admission sequence and timestamp must change together");
    if (lane) pushIssue(issues, `${path}/laneAdmissionSequence`, item.laneAdmissionSequence === lane.admissionSequence && item.admittedAt === lane.admittedAt, "lane must match the work item's durable admission identity");
    if (["active", "integration_ready", "integrating"].includes(item.current)) {
      pushIssue(issues, `${path}/current`, activeLaneIds.includes(workItemId) && item.laneAdmissionSequence !== null, "admitted active/integration work item requires an unreleased sticky lane and durable admission identity");
      pushIssue(issues, `${path}/precedenceIds`, item.precedenceIds.every((id) => state.precedence[id]?.state === "satisfied"), "active/integration work cannot bypass integrated causal predecessors");
      pushIssue(issues, `${path}/gateIds`, item.gateIds.every((id) => state.gates[id]?.state === "released"), "active/integration work cannot bypass closed or invalidated gates");
    }
    const hasStarted = item.laneAdmissionSequence !== null || item.candidate !== null || Object.values(item.stages).some(({ attemptIds, state: stageState }) => attemptIds.length > 0 || stageState === "passed");
    if (item.current === "blocked" && hasStarted) pushIssue(issues, `${path}/current`, activeLaneIds.includes(workItemId), "blocked admitted work retains its sticky active-node lane");
    if (item.current === "complete") pushIssue(issues, `${path}/current`, state.scheduler.activeNodeLanes[workItemId]?.releaseDisposition === "integrated", "complete work item requires its sticky lane's integrated release");
    for (const stage of PLAN_STAGE_IDS) {
      const projection = item.stages[stage];
      pushIssue(issues, `${path}/stages/${stage}/stage`, projection.stage === stage, "must match stage map key");
      projection.attemptIds.forEach((attemptId) => {
        const attempt = state.stageAttempts[attemptId];
        pushIssue(issues, `${path}/stages/${stage}/attemptIds`, attempt?.workItemId === workItemId && attempt?.stage === stage, "must reference a matching stage attempt");
      });
      if (projection.currentAttemptId) {
        pushIssue(issues, `${path}/stages/${stage}/currentAttemptId`, projection.attemptIds.includes(projection.currentAttemptId), "must occur in attemptIds");
        const currentAttempt = state.stageAttempts[projection.currentAttemptId];
        const generationMatches = !currentAttempt ? false
          : projection.state === "passed" && stage === "F0" ? currentAttempt.inputGeneration === 0
          : projection.state === "passed" && stage === "F1" ? currentAttempt.reservedOutputGeneration !== null && currentAttempt.reservedOutputGeneration <= item.candidateGeneration
          : projection.state === "passed" && stage === "F2" && projection.adoptionReceipt !== null ? currentAttempt.inputGeneration + 1 === item.candidateGeneration
          : ["F1", "F3"].includes(stage) && currentAttempt.reservedOutputGeneration !== null ? currentAttempt.reservedOutputGeneration === item.candidateGeneration
          : currentAttempt.inputGeneration === item.candidateGeneration;
        pushIssue(issues, `${path}/stages/${stage}/currentAttemptId`, generationMatches, "current attempt must bind the current input or reserved output generation");
      }
      if (projection.state === "active") {
        const activeAttempt = projection.currentAttemptId ? state.stageAttempts[projection.currentAttemptId] : undefined;
        pushIssue(issues, `${path}/stages/${stage}/currentAttemptId`, projection.currentAttemptId !== null, "active stage requires a current attempt");
        pushIssue(issues, `${path}/currentStage`, item.current === "active" && item.currentStage === stage, "active stage must be the work item's one current active stage");
        const stageIndex = PLAN_STAGE_IDS.indexOf(stage);
        for (const predecessor of PLAN_STAGE_IDS.slice(0, stageIndex)) pushIssue(issues, `${path}/stages/${stage}/state`, item.stages[predecessor].state === "passed", `cannot activate before ${predecessor}`);
        pushIssue(issues, `${path}/stages/${stage}/currentAttemptId`, Boolean(activeAttempt && fixedStageProducers(stage).includes(activeAttempt.producerKind)), "active attempt must use the fixed stage producer kind");
        pushIssue(issues, `${path}/precedenceIds`, item.precedenceIds.every((id) => state.precedence[id]?.state === "satisfied"), "active stage requires every causal predecessor integrated");
        pushIssue(issues, `${path}/gateIds`, item.gateIds.every((id) => state.gates[id]?.state === "released"), "active stage requires every gate released");
      }
      if (["active", "passed"].includes(projection.state)) pushIssue(issues, `${path}/stages/${stage}/state`, item.authorizedStages.includes(stage), "active or passed stage must be inside the exact authorized stage scope");
      if (projection.currentEvidence) pushIssue(issues, `${path}/stages/${stage}/currentEvidence`, Boolean(state.evidenceIndex.stageEvidence[projection.currentEvidence]), "must reference indexed stage evidence");
      if (projection.adoptionReceipt) pushIssue(issues, `${path}/stages/${stage}/adoptionReceipt`, stage === "F2" && projection.state === "passed" && Object.values(state.evidenceIndex.adoptions).some(({ hash }) => hash === projection.adoptionReceipt) && context.facts[projection.adoptionReceipt]?.kind === "adoption", "only passed F2 evidence may carry an exact indexed adoption receipt");
      if (projection.state === "passed") validatePassedStage(state, context, item, stage, projection, path, issues);
    }
    const derivedOpenFindingIds = Object.values(state.findingClosures).filter((finding) => finding.workItemId === workItemId && ["open", "successor_plan_required"].includes(finding.state)).map(({ findingId }) => findingId).sort();
    pushIssue(issues, `${path}/openFindingIds`, sameStrings(item.openFindingIds, derivedOpenFindingIds), "must equal current open immutable findings");
    if (item.candidate) {
      pushIssue(issues, `${path}/candidate/generation`, item.candidate.generation === item.candidateGeneration, "must equal candidateGeneration");
      pushIssue(issues, `${path}/candidate/git/repositoryId`, item.candidate.git.repositoryId === item.writeRepositoryId, "must use the write repository");
      const producer = state.stageAttempts[item.candidate.producedByStageAttemptId];
      pushIssue(issues, `${path}/candidate/producedByStageAttemptId`, producer?.workItemId === workItemId && ["F1", "F3"].includes(producer?.stage ?? ""), "candidate must be produced by this item's F1 or F3 attempt");
      pushIssue(issues, `${path}/candidate/candidateHash`, state.evidenceIndex.candidates[item.candidate.candidateHash]?.hash === item.candidate.candidateHash, "must reference indexed immutable candidate evidence");
      const candidateFact = context.facts[item.candidate.candidateHash];
      pushIssue(issues, `${path}/candidate/candidateHash`, candidateFact?.kind === "candidate", "requires a validated immutable candidate binding");
      if (candidateFact?.kind === "candidate") {
        pushIssue(issues, `${path}/candidate/candidateHash`, candidateFact.hash === item.candidate.candidateHash && candidateFact.planHash === state.identity.planHash && candidateFact.runId === state.runId && candidateFact.runNonce === state.runNonce && candidateFact.workItemId === workItemId, "candidate fact must bind exact plan, run, and work item identity");
        pushIssue(issues, `${path}/candidate/candidateHash`, candidateFact.generation === item.candidate.generation && candidateFact.candidateId === item.candidate.candidateId && candidateFact.patchIdentityHash === item.candidate.patchIdentityHash && candidateFact.producedByStageAttemptId === item.candidate.producedByStageAttemptId && candidateFact.lineageHash === item.candidate.lineageHash, "candidate fact must match every candidate identity field");
        const producingAttempt = state.stageAttempts[candidateFact.producedByStageAttemptId];
        pushIssue(issues, `${path}/candidate/producedByStageAttemptId`, Boolean(producingAttempt && ["F1", "F3"].includes(producingAttempt.stage) && producingAttempt.workItemId === workItemId && producingAttempt.reservedOutputGeneration === candidateFact.generation && producingAttempt.state === "sealed"), "candidate must be produced by an exact sealed F1/F3 output generation");
        pushIssue(issues, `${path}/candidate/git`, canonicalHash(candidateFact.base) === canonicalHash(item.candidate.base) && canonicalHash(candidateFact.git) === canonicalHash(item.candidate.git), "candidate fact must match the exact source base, commit, and tree");
      }
    } else pushIssue(issues, `${path}/candidateGeneration`, item.candidateGeneration === 0 || ["blocked", "cancelled", "superseded"].includes(item.current) || integrationConflictFence !== null, "candidate-less positive generation is allowed only after an explicit fence");
    if (["integration_ready", "integrating", "complete"].includes(item.current)) {
      const readyRef = state.evidenceIndex.integrationReady[workItemId];
      pushIssue(issues, `${path}/integrationReadyReceipt`, item.integrationReadyReceipt !== null && readyRef?.hash === item.integrationReadyReceipt && readyRef?.kind === "integration_ready", "requires the exact indexed integration-ready receipt");
      for (const stage of PLAN_STAGE_IDS) pushIssue(issues, `${path}/stages/${stage}/state`, item.stages[stage].state === "passed", "must be passed before integration readiness");
      pushIssue(issues, `${path}/candidate`, item.candidate !== null, "integration readiness requires a current candidate");
      const unresolvedMaterialFindings = Object.values(state.findingClosures).filter((finding) => finding.workItemId === workItemId && ["open", "successor_plan_required"].includes(finding.state) && (finding.severity === "blocking" || finding.materiality === "plan_affecting"));
      pushIssue(issues, `${path}/openFindingIds`, unresolvedMaterialFindings.length === 0, "integration readiness cannot retain blocking or plan-affecting findings");
      const unresolvedEffects = Object.values(state.effects).filter((effect) => effect.subject.kind === "work_item" && effect.subject.id === workItemId && !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation));
      pushIssue(issues, `${path}/integrationReadyReceipt`, unresolvedEffects.length === 0, "integration readiness requires every work-item effect reconciled");
      const readyFact = item.integrationReadyReceipt ? context.facts[item.integrationReadyReceipt] : undefined;
      pushIssue(issues, `${path}/integrationReadyReceipt`, readyFact?.kind === "integration_ready", "requires a validated immutable integration-ready binding");
      if (readyFact?.kind === "integration_ready" && item.candidate) {
        pushIssue(issues, `${path}/integrationReadyReceipt`, readyFact.hash === item.integrationReadyReceipt && readyFact.planHash === state.identity.planHash && readyFact.runId === state.runId && readyFact.runNonce === state.runNonce && readyFact.workItemId === workItemId, "receipt must bind exact plan, run, and work item identity");
        pushIssue(issues, `${path}/integrationReadyReceipt`, readyFact.candidateGeneration === item.candidateGeneration && readyFact.candidateHash === item.candidate.candidateHash, "receipt must bind the current candidate generation and hash");
        pushIssue(issues, `${path}/integrationReadyReceipt`, readyFact.f8EvidenceHash === item.stages.F8.currentEvidence, "receipt must bind current F8 evidence");
        pushIssue(issues, `${path}/integrationReadyReceipt`, readyFact.allRequiredChecksPassed && readyFact.effectsReconciled && readyFact.findingsClosed, "receipt must prove checks, effects, and findings are closed");
      }
    }
    if (item.current === "complete") {
      pushIssue(issues, `${path}/integrationReceipt`, item.integrationReceipt !== null && Object.values(state.evidenceIndex.integrationReceipts).some(({ hash }) => hash === item.integrationReceipt), "complete item requires an indexed integration receipt");
      const integrationFact = item.integrationReceipt ? context.facts[item.integrationReceipt] : undefined;
      pushIssue(issues, `${path}/integrationReceipt`, integrationFact?.kind === "integration", "complete item requires a validated immutable integration binding");
      if (integrationFact?.kind === "integration" && item.candidate) {
        pushIssue(issues, `${path}/integrationReceipt`, integrationFact.hash === item.integrationReceipt && integrationFact.planHash === state.identity.planHash && integrationFact.runId === state.runId && integrationFact.runNonce === state.runNonce && integrationFact.workItemId === workItemId && integrationFact.repositoryId === item.writeRepositoryId, "integration receipt must bind exact plan, run, item, and repository identity");
        const integrationAttempt = state.integrationAttempts[integrationFact.integrationAttemptId];
        const integrationEntry = Object.values(state.integrationTrains[item.writeRepositoryId]?.entries ?? {}).find(({ entryId }) => entryId === integrationAttempt?.entryId);
        pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt?.integrationReceipt === item.integrationReceipt && integrationAttempt?.landingState === "landed" && integrationEntry?.workItemId === workItemId && integrationEntry.state === "integrated" && integrationEntry.integrationReceipt === item.integrationReceipt, "integration receipt must be produced by the exact landed current train entry attempt");
        if (integrationAttempt) {
          pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt.strategy === "merge_tree_one_parent" && integrationFact.strategy === integrationAttempt.strategy && integrationFact.compositionProfileHash === integrationAttempt.compositionProfileHash, "integration must use the exact accepted merge-tree composition profile");
          pushIssue(issues, `${path}/integrationReceipt`, canonicalHash(integrationFact.expectedPrefix) === canonicalHash(integrationAttempt.expectedPrefix) && canonicalHash(integrationFact.expectedTarget) === canonicalHash(integrationAttempt.expectedTarget) && sameStrings([...integrationFact.prefixEvidenceHashes], [...integrationAttempt.prefixEvidenceHashes]) && sameStrings([...integrationFact.finalEvidenceHashes], [...integrationAttempt.finalEvidenceHashes]) && integrationFact.environmentClosureHash === integrationAttempt.environmentClosureHash, "integration receipt must bind exact source prefix, target, prefix checks, and final checks");
          pushIssue(issues, `${path}/integrationReceipt`, canonicalHash(integrationFact.sourceBase) === canonicalHash(integrationAttempt.sourceBase) && canonicalHash(integrationFact.sourceBase) === canonicalHash(item.candidate.base) && canonicalHash(integrationFact.sourceCandidate) === canonicalHash(integrationAttempt.sourceCandidate) && canonicalHash(integrationFact.sourceCandidate) === canonicalHash(item.candidate.git), "integration must bind exact candidate source base and current source candidate");
          pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt.syntheticParentCommit === integrationAttempt.expectedPrefix.commit && integrationFact.syntheticParentCommit === integrationAttempt.syntheticParentCommit, "synthetic commit must have exactly the accepted prefix as its one parent");
          pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt.sourceToIntegratedLineageHash === integrationFact.sourceToIntegratedLineageHash && integrationAttempt.composedTree !== null && canonicalHash(integrationAttempt.composedTree) === canonicalHash(integrationFact.landed), "receipt must bind exact source-to-integrated lineage and composed landed tree");
          const transactionFact = context.facts[integrationFact.transactionReceiptFactHash]; const receipt = transactionFact?.kind === "git_integration_receipt" ? transactionFact.receipt as any : null; const bindingFact = context.facts[integrationAttempt.repositoryBindingFactHash] as any;
          pushIssue(issues, `${path}/integrationReceipt`, transactionFact?.kind === "git_integration_receipt" && transactionFact.hash === integrationFact.transactionReceiptFactHash && transactionFact.hash === hashWithoutField(transactionFact as unknown as Record<string, unknown>, "hash") && transactionFact.transactionReceiptHash === integrationFact.transactionReceiptHash && transactionFact.planHash === state.identity.planHash && transactionFact.runId === state.runId && transactionFact.runNonce === state.runNonce && transactionFact.authorizationSetHash === state.identity.authorizationSet.hash && transactionFact.repositoryId === item.writeRepositoryId && transactionFact.integrationAttemptId === integrationAttempt.integrationAttemptId, "integration must resolve the exact immutable transaction-receipt binding");
          pushIssue(issues, `${path}/integrationReceipt`, receipt?.receiptHash === integrationFact.transactionReceiptHash && receipt?.receiptHash === hashWithoutField(receipt as Record<string, unknown>, "receiptHash") && receipt?.transactionId === integrationAttempt.integrationAttemptId && receipt?.ownerEpoch === integrationFact.acceptingOwnerEpoch && receipt?.commonDirIdentityHash === bindingFact?.commonDirIdentityHash && receipt?.worktreeIdentityHash === bindingFact?.worktreeIdentityHash && receipt?.configHash === bindingFact?.gitConfigHash && canonicalHash(receipt?.gitVersion) === bindingFact?.gitVersionHash && receipt?.objectFormat === bindingFact?.objectFormat && canonicalHash(receipt?.composed) === canonicalHash(integrationFact.landed) && receipt?.landing?.targetObservationHash === (context.facts[integrationFact.landingObservationHash] as any)?.detailsHash && canonicalHash(Object.keys(receipt?.privateRefs ?? {}).sort()) === canonicalHash(["baseline", "candidate", "composed", "prefix", "proposal"]) && canonicalHash(Object.values(receipt?.privateRefs ?? {}).sort()) === canonicalHash(integrationAttempt.privateRefFactHashes.map((hash) => (context.facts[hash] as any)?.targetRef).sort()), "transaction receipt content must bind exact owner, repository environment, composed target, and landing observation");
        }
        pushIssue(issues, `${path}/integrationReceipt`, integrationFact.candidateHash === item.candidate.candidateHash && integrationFact.combinedStateVerified && integrationFact.reconciled, "integration receipt must prove the current candidate, combined state, and reconciliation");
        pushIssue(issues, `${path}/integrationReceipt`, canonicalHash(state.repositories[item.writeRepositoryId].observedTarget) === canonicalHash(integrationFact.landed), "observed target must equal the reconciled landed tree");
      }
      pushIssue(issues, `${path}/completedAt`, item.completedAt !== null, "complete item requires completedAt");
    } else pushIssue(issues, `${path}/completedAt`, item.completedAt === null, "must be null unless complete");
  }

  for (const [attemptId, attempt] of Object.entries(state.stageAttempts)) {
    const path = `/stageAttempts/${attemptId}`;
    const item = state.workItems[attempt.workItemId];
    pushIssue(issues, `${path}/workItemId`, Boolean(item), "references an unknown work item");
    pushIssue(issues, `${path}/inputGeneration`, !item || attempt.inputGeneration <= item.candidateGeneration, "cannot exceed current candidate generation");
    pushIssue(issues, `${path}/implementationLineageHash`, ["F1", "F3"].includes(attempt.stage) ? attempt.implementationLineageHash === item?.implementationLineageHash : attempt.implementationLineageHash === null, "only F1/F3 attempts bind the work item's single implementation lineage");
    pushIssue(issues, `${path}/reservedOutputGeneration`, attempt.reservedOutputGeneration === null || ["F1", "F3"].includes(attempt.stage), "only F1/F3 may reserve candidate output generations");
    pushIssue(issues, `${path}/attemptInput`, state.evidenceIndex.stageAttemptInputs[attemptId]?.hash === attempt.attemptInput.hash, "must match indexed immutable attempt input");
    const inputFact = context.facts[attempt.attemptInput.hash];
    pushIssue(issues, `${path}/attemptInput`, inputFact?.kind === "stage_attempt_input", "requires a validated immutable stage-attempt input binding");
    if (inputFact?.kind === "stage_attempt_input") {
      pushIssue(issues, `${path}/attemptInput`, inputFact.hash === attempt.attemptInput.hash && inputFact.planHash === state.identity.planHash && inputFact.runId === state.runId && inputFact.runNonce === state.runNonce && inputFact.workItemId === attempt.workItemId && inputFact.stage === attempt.stage && inputFact.stageAttemptId === attemptId, "attempt input must bind exact plan, run, work item, stage, and attempt identity");
      pushIssue(issues, `${path}/attemptInput`, inputFact.candidateGeneration === attempt.inputGeneration && inputFact.authorizationSetHash === attempt.authorizationSetHash && inputFact.authorizationSetHash === state.identity.authorizationSet.hash, "attempt input must bind exact generation and authorization set");
      pushIssue(issues, `${path}/attemptInput`, inputFact.producerKind === attempt.producerKind && inputFact.implementationLineageHash === attempt.implementationLineageHash, "attempt input must bind exact producer and implementation lineage");
      if (!["F0", "F1", "F3"].includes(attempt.stage)) {
        const adoptedF2Input = attempt.stage === "F2" && item?.stages.F2.adoptionReceipt !== null && inputFact.candidateGeneration + 1 === item?.candidateGeneration;
        const isCurrentAttempt = item?.stages[attempt.stage].currentAttemptId === attemptId; const historicalCandidate = inputFact.candidateHash ? context.facts[inputFact.candidateHash] : undefined; const exactHistoricalInput = !isCurrentAttempt && historicalCandidate?.kind === "candidate" && historicalCandidate.planHash === state.identity.planHash && historicalCandidate.runId === state.runId && historicalCandidate.runNonce === state.runNonce && historicalCandidate.workItemId === attempt.workItemId && historicalCandidate.generation === inputFact.candidateGeneration && state.evidenceIndex.candidates[inputFact.candidateHash!]?.hash === inputFact.candidateHash;
        pushIssue(issues, `${path}/attemptInput`, (isCurrentAttempt && item?.candidate !== null && (inputFact.candidateHash === item?.candidate?.candidateHash || adoptedF2Input)) || exactHistoricalInput, "non-producing current input must bind the current candidate/adoption; historical input must resolve its exact immutable candidate generation");
      }
    }
    if (attempt.launchIntentId) pushIssue(issues, `${path}/launchIntentId`, state.launchIntents[attempt.launchIntentId]?.stageAttemptId === attemptId, "must reference a matching launch intent");
    if (attempt.producerKind === "owned_worker") pushIssue(issues, `${path}/producerKind`, attempt.launchIntentId !== null && Boolean(state.workerBindings[attemptId]), "owned-worker attempt requires exact launch intent and worker binding");
    else pushIssue(issues, `${path}/producerKind`, attempt.launchIntentId === null && !state.workerBindings[attemptId] && attempt.workerResult === null, "conductor/deterministic attempt cannot use generic worker state");
    if (attempt.workerResult) pushIssue(issues, `${path}/workerResult`, state.evidenceIndex.workerResults[attempt.workerResult.hash]?.hash === attempt.workerResult.hash, "must be indexed by exact result hash");
    if (attempt.evidence) pushIssue(issues, `${path}/evidence`, state.evidenceIndex.stageEvidence[attempt.evidence.hash]?.hash === attempt.evidence.hash, "must be indexed by exact evidence hash");
  }

  for (const [launchId, launch] of Object.entries(state.launchIntents)) {
    pushIssue(issues, `/launchIntents/${launchId}/stageAttemptId`, state.stageAttempts[launch.stageAttemptId]?.launchIntentId === launchId, "must backreference the launch intent");
    pushIssue(issues, `/launchIntents/${launchId}/effectId`, state.effects[launch.effectId]?.kind === "launch_worker", "must reference a launch_worker effect intent");
    pushIssue(issues, `/launchIntents/${launchId}/cwdRepositoryId`, Boolean(state.repositories[launch.cwdRepositoryId]), "references an unknown repository");
  }
  for (const [attemptId, binding] of Object.entries(state.workerBindings)) {
    pushIssue(issues, `/workerBindings/${attemptId}/stageAttemptId`, binding.stageAttemptId === attemptId && Boolean(state.stageAttempts[attemptId]), "must match an existing stage attempt");
    const launch = state.launchIntents[binding.launchIntentId];
    pushIssue(issues, `/workerBindings/${attemptId}/launchIntentId`, launch?.stageAttemptId === attemptId && launch?.workerId === binding.workerId, "must match the exact launch intent");
    pushIssue(issues, `/workerBindings/${attemptId}/retrySafe`, !binding.retrySafe || binding.processDisposition === "dead", "retrySafe requires proven dead process disposition");
    if (binding.resultHash) {
      const attempt = state.stageAttempts[attemptId];
      pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, binding.completionId !== null && attempt?.workerResult?.hash === binding.resultHash && state.evidenceIndex.workerResults[binding.resultHash]?.hash === binding.resultHash, "must match exact attempt, completion, and indexed worker result");
      const resultFact = context.facts[binding.resultHash];
      pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact?.kind === "worker_result", "requires a validated immutable generic worker-result binding");
      if (resultFact?.kind === "worker_result") {
        pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact.hash === binding.resultHash && resultFact.workerStorageId === binding.workerStorageId && resultFact.launchOwnerSessionId === binding.launchOwnerSessionId && resultFact.workerId === binding.workerId && resultFact.attemptNumber === binding.attemptNumber && resultFact.attemptNonce === binding.attemptNonce && resultFact.configHash === binding.configHash && resultFact.completionId === binding.completionId, "worker result must match the full exact generic attempt ingest key");
        pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact.processDisposition === binding.processDisposition && resultFact.retrySafe === binding.retrySafe, "worker result disposition must match the current immutable process facts");
      }
    } else pushIssue(issues, `/workerBindings/${attemptId}/completionId`, binding.completionId === null, "completionId requires a resultHash");
  }

  for (const [leaseId, lease] of Object.entries(state.leases)) {
    pushIssue(issues, `/leases/${leaseId}/ownerEpoch`, lease.ownerEpoch <= state.owner.ownerEpoch, "cannot exceed the current owner epoch");
    pushIssue(issues, `/leases/${leaseId}`, !(lease.holderStageAttemptId && lease.holderIntegrationAttemptId), "cannot have both holder kinds");
    if (lease.holderStageAttemptId) pushIssue(issues, `/leases/${leaseId}/holderStageAttemptId`, Boolean(state.stageAttempts[lease.holderStageAttemptId]), "references an unknown stage attempt");
    if (lease.holderIntegrationAttemptId) pushIssue(issues, `/leases/${leaseId}/holderIntegrationAttemptId`, Boolean(state.integrationAttempts[lease.holderIntegrationAttemptId]), "references an unknown integration attempt");
    pushIssue(issues, `/leases/${leaseId}/releasedAt`, ["released", "fenced"].includes(lease.state) ? lease.releasedAt !== null : true, "released/fenced lease requires releasedAt");
  }
  for (const [resourceId, resource] of Object.entries(state.resourcePools)) {
    const activeUnits = resource.leaseIds.reduce((sum, leaseId) => {
      const lease = state.leases[leaseId];
      return sum + (["active", "release_requested", "expired", "fenced"].includes(lease?.state ?? "") ? lease.units : 0);
    }, 0);
    pushIssue(issues, `/resourcePools/${resourceId}/allocatedUnits`, resource.allocatedUnits === activeUnits, "must equal active resource lease units");
    pushIssue(issues, `/resourcePools/${resourceId}/observedCapacity`, resource.observedCapacity <= resource.semanticMaximum, "cannot exceed the plan semantic maximum");
  }

  for (const [findingId, finding] of Object.entries(state.findingClosures)) {
    pushIssue(issues, `/findingClosures/${findingId}/findingHash`, state.evidenceIndex.findings[findingId]?.hash === finding.findingHash, "must match indexed immutable finding");
    pushIssue(issues, `/findingClosures/${findingId}/workItemId`, Boolean(state.workItems[finding.workItemId]), "references an unknown work item");
    pushIssue(issues, `/findingClosures/${findingId}`, !(finding.materiality === "plan_affecting" && finding.state === "corrected"), "plan-affecting finding cannot close as a local correction");
  }
  for (const [retryKey, retry] of Object.entries(state.retryLedger)) {
    pushIssue(issues, `/retryLedger/${retryKey}/count`, retry.count <= retry.ceiling, "cannot exceed retry ceiling");
    pushIssue(issues, `/retryLedger/${retryKey}/workItemId`, Boolean(state.workItems[retry.workItemId]), "references an unknown work item");
    pushIssue(issues, `/retryLedger/${retryKey}/stop`, retry.count < retry.ceiling || retry.stop !== "none", "ceiling exhaustion requires a stop disposition");
  }
  for (const [effectId, effect] of Object.entries(state.effects)) {
    pushIssue(issues, `/effects/${effectId}/boundOwnerEpoch`, effect.boundOwnerEpoch <= state.owner.ownerEpoch, "cannot exceed current owner epoch");
    pushIssue(issues, `/effects/${effectId}/createdRevision`, effect.createdRevision <= state.revision, "cannot be created after the current revision");
    const authorization = effect.boundAuthorizationSetHash === context.authorization.hash ? context.authorization : context.historicalAuthorizations[effect.boundAuthorizationSetHash];
    pushIssue(issues, `/effects/${effectId}/boundAuthorizationSetHash`, Boolean(authorization && authorization.planHash === state.identity.planHash && authorization.hash === hashWithoutField(authorization as unknown as Record<string, unknown>, "hash")), "must resolve to exact canonical current or historical authorization content for this plan");
    if (authorization) {
      const at = utcTimestampOrderValue(effect.createdAt);
      pushIssue(issues, `/effects/${effectId}/createdAt`, utcTimestampOrderValue(authorization.validFrom) <= at && (authorization.validUntil === null || at <= utcTimestampOrderValue(authorization.validUntil)), "effect intent must be created while its bound authorization is valid");
      if (effect.subject.kind === "work_item") pushIssue(issues, `/effects/${effectId}/subject`, authorization.workItemIds.includes(effect.subject.id), "effect subject work item must be authorized");
      if (effect.subject.kind === "repository") pushIssue(issues, `/effects/${effectId}/subject`, authorization.repositoryIds.includes(effect.subject.id), "effect subject repository must be authorized");
      if (effect.subject.kind === "train") pushIssue(issues, `/effects/${effectId}/subject`, authorization.integrationTrainIds.includes(effect.subject.id), "effect subject train must be authorized");
      if (effect.effectScopeId) {
        const scope = context.plan.architecture.effectScopes.find(({ effectScopeId }) => effectScopeId === effect.effectScopeId);
        const subjectItem = effect.subject.kind === "work_item" ? context.plan.workItems.find(({ workItemId }) => workItemId === effect.subject.id) : undefined;
        pushIssue(issues, `/effects/${effectId}/effectScopeId`, authorization.effectScopeIds.includes(effect.effectScopeId) && Boolean(scope) && scope?.kind === effect.effectScopeKind && scope?.provider === effect.provider && scope?.procedureClass === effect.procedureClass && Boolean(subjectItem) && subjectItem!.integration.effectScopeIds.includes(effect.effectScopeId), "effect scope must be authorized, assigned to the exact work item, and match the plan replay class");
      }
      if (!effect.effectScopeId) pushIssue(issues, `/effects/${effectId}/effectScopeId`, effect.effectScopeKind === null && effect.provider === null, "unscoped effects cannot claim a provider or effect-scope kind");
      const requiresExternalScope = effect.kind === "reconcile_external_effect" || (effect.kind === "run_procedure" && effect.procedureClass !== "pure");
      pushIssue(issues, `/effects/${effectId}/effectScopeId`, !requiresExternalScope || (effect.effectScopeId !== null && effect.subject.kind === "work_item"), "external-effect procedures require an explicit plan-assigned work-item effect scope");
    }
    pushIssue(issues, `/effects/${effectId}/lastDispatchAt`, effect.dispatchCount === 0 ? effect.lastDispatchAt === null : effect.lastDispatchAt !== null, "dispatch count and last dispatch time must change together");
    if (["intended", "dispatching"].includes(effect.state)) {
      pushIssue(issues, `/effects/${effectId}/boundAuthorizationSetHash`, effect.boundAuthorizationSetHash === context.authorization.hash, "pending dispatch must bind current authorization");
      pushIssue(issues, `/effects/${effectId}/boundFreshnessReceiptHash`, effect.boundFreshnessReceiptHash === state.freshness.receipt.hash && !state.freshness.blocksNewLaunches, "pending dispatch must bind current nonblocking freshness");
      pushIssue(issues, `/effects/${effectId}/boundOwnerEpoch`, effect.boundOwnerEpoch === state.owner.ownerEpoch, "pending dispatch must bind current owner epoch");
      if (effect.subject.kind === "work_item") pushIssue(issues, `/effects/${effectId}/boundCandidateGeneration`, effect.boundCandidateGeneration === state.workItems[effect.subject.id]?.candidateGeneration, "pending dispatch must bind current work-item generation");
    }
    if (effect.observationHash !== null) {
      const observation = context.facts[effect.observationHash];
      pushIssue(issues, `/effects/${effectId}/observationHash`, observation?.kind === "effect_reconciliation" && observation.hash === effect.observationHash && observation.hash === hashWithoutField(observation as unknown as Record<string, unknown>, "hash") && observation.planHash === state.identity.planHash && observation.runId === state.runId && observation.runNonce === state.runNonce && observation.effectId === effect.effectId && observation.requestHash === effect.requestHash && observation.reconciliation === effect.reconciliation, "must resolve exact canonical immutable effect reconciliation evidence");
    }
    if (["unknown", "non_repeatable"].includes(effect.procedureClass) && effect.state === "ambiguous") pushIssue(issues, `/effects/${effectId}/blockerId`, effect.blockerId !== null && state.blockers[effect.blockerId]?.active, "ambiguous unknown/non-repeatable effect must have an active blocker");
  }

  for (const [cancellationId, cancellation] of Object.entries(state.cancellations)) {
    pushIssue(issues, `/cancellations/${cancellationId}/cancellationId`, cancellation.cancellationId === cancellationId, "must match map key");
    pushIssue(issues, `/cancellations/${cancellationId}/effectIds`, cancellation.effectIds.every((effectId) => state.effects[effectId]?.kind === "cancel_worker"), "must reference only exact cancel-worker effects");
    pushIssue(issues, `/cancellations/${cancellationId}/resultHash`, cancellation.state === "closed" ? cancellation.resultHash !== null : cancellation.resultHash === null, "only a closed cancellation has a terminal result hash");
    if (cancellation.scope === "run") pushIssue(issues, `/cancellations/${cancellationId}/subjectId`, cancellation.subjectId === state.runId, "run cancellation subject must be this run");
    const fencedIds = Object.keys(cancellation.fencedGenerations).sort();
    pushIssue(issues, `/cancellations/${cancellationId}/fencedGenerations`, fencedIds.every((id) => Boolean(state.workItems[id]) && cancellation.fencedGenerations[id] <= state.workItems[id].candidateGeneration), "must bind exact existing fenced work-item generations");
    if (cancellation.scope === "work_item") pushIssue(issues, `/cancellations/${cancellationId}/subjectId`, fencedIds.length === 1 && fencedIds[0] === cancellation.subjectId, "work-item cancellation must bind exactly its subject generation");
  }

  for (const [quarantineId, entry] of Object.entries(state.quarantine)) {
    pushIssue(issues, `/quarantine/${quarantineId}/quarantineId`, entry.quarantineId === quarantineId, "must match map key");
    const quarantinedFact = context.facts[entry.fact.hash];
    pushIssue(issues, `/quarantine/${quarantineId}/fact`, quarantinedFact?.hash === entry.fact.hash && quarantinedFact?.kind === entry.fact.kind, "must resolve the exact immutable fact kind and hash retained in quarantine");
    pushIssue(issues, `/quarantine/${quarantineId}`, entry.state === "held" ? entry.adoptionReceipt === null && entry.rejectionReason === null : entry.state === "adopted" ? entry.adoptionReceipt !== null && entry.rejectionReason === null : entry.adoptionReceipt === null && entry.rejectionReason !== null, "quarantine disposition fields must exactly match held/adopted/rejected state");
    if (entry.state === "adopted") {
      const adoption = context.facts[entry.adoptionReceipt!];
      const entryHash = canonicalHash({ quarantineId: entry.quarantineId, fact: entry.fact, reason: entry.reason, observedBindingHash: entry.observedBindingHash, expectedBindingHash: entry.expectedBindingHash, observedAt: entry.observedAt });
      const authority = adoption?.kind === "quarantine_resolution" ? context.authorityReceipts?.[adoption.authorityReceiptHash] : undefined;
      const exactAdoption = adoption?.kind === "quarantine_resolution" && adoption.hash === entry.adoptionReceipt && adoption.hash === hashWithoutField(adoption as unknown as Record<string, unknown>, "hash") && adoption.planHash === state.identity.planHash && adoption.runId === state.runId && adoption.runNonce === state.runNonce && adoption.quarantineId === entry.quarantineId && adoption.factHash === entry.fact.hash && adoption.quarantineEntryHash === entryHash && adoption.disposition === "adopted";
      const exactAuthority = authority?.kind === "quarantine_authority" && authority.hash === adoption?.authorityReceiptHash && authority.hash === hashWithoutField(authority as unknown as Record<string, unknown>, "hash") && authority.planHash === state.identity.planHash && authority.runId === state.runId && authority.runNonce === state.runNonce && authority.quarantineId === entry.quarantineId && authority.factHash === entry.fact.hash && authority.quarantineEntryHash === entryHash && authority.decision === "adopt" && authority.issuedBy === "user" && Number.isFinite(utcTimestampOrderValue(authority.issuedAt)) && utcTimestampOrderValue(entry.observedAt) <= utcTimestampOrderValue(authority.issuedAt) && utcTimestampOrderValue(authority.issuedAt) <= utcTimestampOrderValue(state.updatedAt);
      pushIssue(issues, `/quarantine/${quarantineId}/adoptionReceipt`, exactAdoption && exactAuthority, "adoption must resolve an exact external user-authority receipt and canonical immutable quarantine-resolution fact");
    }
  }

  const activeCommonDirLocks = new Map<string, string>();
  for (const repository of Object.values(state.repositories)) if (repository.integrationLockLeaseId) {
    const lease = state.leases[repository.integrationLockLeaseId]; const commonDir = repository.workspace.gitCommonDirIdentityHash;
    if (lease && lease.state !== "released" && commonDir) { const prior = activeCommonDirLocks.get(commonDir); pushIssue(issues, `/repositories/${repository.repositoryId}/integrationLockLeaseId`, prior === undefined, "only one repository identity may hold an active integration lock for an exact Git common directory"); if (!prior) activeCommonDirLocks.set(commonDir, repository.repositoryId); }
  }

  for (const [repositoryId, train] of Object.entries(state.integrationTrains)) {
    pushIssue(issues, `/integrationTrains/${repositoryId}/expectedTarget/repositoryId`, train.expectedTarget.repositoryId === repositoryId && canonicalHash(train.expectedTarget) === canonicalHash(state.repositories[repositoryId].observedTarget), "must match repository key and exact current observed target");
    pushIssue(issues, `/integrationTrains/${repositoryId}/acceptedPrefix/repositoryId`, train.acceptedPrefix.repositoryId === repositoryId, "must match repository key");
    pushIssue(issues, `/integrationTrains/${repositoryId}/entryOrder`, train.entryOrder.length === Object.keys(train.entries).length && train.entryOrder.every((id) => Boolean(train.entries[id])), "must contain every entry exactly once");
    validateMapKeys(train.entries, "entryId", `/integrationTrains/${repositoryId}/entries`, issues);
    const planTrain = context.plan.constraints.integrationTrains.find((candidate) => candidate.repositoryId === repositoryId);
    const headOrdinal = train.entryOrder.findIndex((entryId) => train.entries[entryId]?.state !== "integrated");
    const integratedCount = headOrdinal < 0 ? train.entryOrder.length : headOrdinal;
    const lastIntegratedEntry = integratedCount > 0 ? train.entries[train.entryOrder[integratedCount - 1]] : undefined;
    const lastIntegratedFact = lastIntegratedEntry?.integrationReceipt ? context.facts[lastIntegratedEntry.integrationReceipt] : undefined;
    const expectedAcceptedPrefix = lastIntegratedFact?.kind === "integration" ? lastIntegratedFact.landed : state.repositories[repositoryId].baseline;
    pushIssue(issues, `/integrationTrains/${repositoryId}/acceptedPrefixOrdinal`, train.acceptedPrefixOrdinal === integratedCount, "must equal the exact integrated prefix length");
    pushIssue(issues, `/integrationTrains/${repositoryId}/acceptedPrefix`, canonicalHash(train.acceptedPrefix) === canonicalHash(expectedAcceptedPrefix), "must equal the last exact integrated landing or repository baseline");
    pushIssue(issues, `/integrationTrains/${repositoryId}/acceptedPrefixReceipt`, train.acceptedPrefixReceipt === (lastIntegratedEntry?.integrationReceipt ?? null), "must equal the exact last integrated receipt");
    train.entryOrder.forEach((entryId, ordinal) => {
      const entry = train.entries[entryId];
      pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/ordinal`, entry?.ordinal === ordinal, "must match entry order");
      pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/workItemId`, Boolean(entry && state.workItems[entry.workItemId]), "references an unknown work item");
      pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/workItemId`, entry?.workItemId === planTrain?.members[ordinal]?.workItemId, "must follow exact plan train order");
      const item = entry ? state.workItems[entry.workItemId] : undefined;
      if (entry && item) {
        const entryTerminallyInvalidated = ["invalidated", "quarantined"].includes(entry.state);
        pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/workItemId`, entryTerminallyInvalidated || ["integration_ready", "integrating", "complete"].includes(item.current), "active train entry requires an integration-ready or integrated work item");
        pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/integrationReadyHash`, entryTerminallyInvalidated || item.integrationReadyReceipt === entry.integrationReadyHash, "active entry must bind the work item's exact integration-ready receipt");
        pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/sourceCandidate`, entryTerminallyInvalidated || (item.candidate !== null && item.candidate.candidateHash === entry.sourceCandidate.candidateHash && item.candidateGeneration === entry.sourceCandidate.generation), "active entry must bind the work item's current candidate generation and hash");
        if (entry.state === "integrated") pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/integrationReceipt`, entry.integrationReceipt !== null && item.current === "complete" && item.integrationReceipt === entry.integrationReceipt, "integrated entry requires the work item's exact accepted integration receipt and completion");
      }
      if (headOrdinal >= 0 && ordinal > headOrdinal) pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/state`, entry?.state === "waiting", "future train entries must wait until every predecessor is landed and receipted");
      if (entry && entry.state !== "waiting") for (const predecessorId of train.entryOrder.slice(0, ordinal)) {
        const predecessor = train.entries[predecessorId];
        const predecessorItem = predecessor ? state.workItems[predecessor.workItemId] : undefined;
        const predecessorFact = predecessor?.integrationReceipt ? context.facts[predecessor.integrationReceipt] : undefined;
        const receiptIndexed = predecessor?.attemptIds.some((attemptId) => state.evidenceIndex.integrationReceipts[attemptId]?.hash === predecessor.integrationReceipt);
        pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/state`, predecessor?.state === "integrated" && predecessor.integrationReceipt !== null && predecessorItem?.current === "complete" && predecessorItem.integrationReceipt === predecessor.integrationReceipt && receiptIndexed && predecessorFact?.kind === "integration" && predecessorFact.hash === predecessor.integrationReceipt, "progress requires every predecessor landed, indexed, exactly receipted, and complete");
      }
      if (entry?.currentAttemptId) pushIssue(issues, `/integrationTrains/${repositoryId}/entries/${entryId}/currentAttemptId`, state.integrationAttempts[entry.currentAttemptId]?.entryId === entryId, "must reference a matching integration attempt");
    });
    if (train.activeIntegrationAttemptId) {
      const active = state.integrationAttempts[train.activeIntegrationAttemptId];
      const headEntryId = headOrdinal >= 0 ? train.entryOrder[headOrdinal] : null;
      pushIssue(issues, `/integrationTrains/${repositoryId}/activeIntegrationAttemptId`, active?.entryId === headEntryId, "only the current accepted train head may have an active integration attempt");
    }
  }
  for (const [attemptId, attempt] of Object.entries(state.integrationAttempts)) {
    const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt.entryId]));
    const trainEntry = train?.entries[attempt.entryId];
    const planTrain = context.plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === train?.repositoryId);
    const entryOrdinal = trainEntry ? train?.entryOrder.indexOf(trainEntry.entryId) ?? -1 : -1;
    const predecessorEntry = entryOrdinal > 0 ? train?.entries[train!.entryOrder[entryOrdinal - 1]] : undefined;
    const predecessorReceipt = predecessorEntry?.integrationReceipt ? context.facts[predecessorEntry.integrationReceipt] : undefined;
    const expectedSourcePrefix = entryOrdinal === 0 ? state.repositories[train!.repositoryId].baseline : predecessorReceipt?.kind === "integration" ? predecessorReceipt.landed : null;
    pushIssue(issues, `/integrationAttempts/${attemptId}/integrationAttemptId`, attempt.integrationAttemptId === attemptId, "must match map key");
    pushIssue(issues, `/integrationAttempts/${attemptId}/entryId`, Boolean(trainEntry) && trainEntry!.attemptIds.includes(attemptId) && (trainEntry!.currentAttemptId === attemptId || trainEntry!.integrationReceipt === attempt.integrationReceipt || attempt.conflictClass !== "none"), "must be listed by the exact train entry as current, conflicted, or receipted attempt");
    pushIssue(issues, `/integrationAttempts/${attemptId}/strategy`, attempt.strategy === "merge_tree_one_parent" && planTrain?.strategy === attempt.strategy, "must use accepted explicit-base merge-tree composition");
    pushIssue(issues, `/integrationAttempts/${attemptId}/compositionProfileHash`, attempt.compositionProfileHash === planTrain?.compositionProfileHash && attempt.prefixValidationProfileHash === planTrain?.prefixValidationProfileHash && attempt.finalValidationProfileHash === planTrain?.finalValidationProfileHash, "must bind exact plan-authorized composition, prefix, and final profiles");
    pushIssue(issues, `/integrationAttempts/${attemptId}/expectedPrefix`, expectedSourcePrefix !== null && canonicalHash(attempt.expectedPrefix) === canonicalHash(expectedSourcePrefix) && canonicalHash(attempt.expectedTarget) === canonicalHash(attempt.expectedPrefix), "must bind the exact integrated predecessor prefix and matching observed target");
    if (attempt.landingState !== "landed") pushIssue(issues, `/integrationAttempts/${attemptId}/expectedTarget`, canonicalHash(attempt.expectedTarget) === canonicalHash(train?.expectedTarget), "active integration attempt must bind the train's exact current expected target");
    const historicalConflictCandidate = attempt.conflictClass !== "none" ? context.facts[attempt.sourceCandidateHash] : undefined; const sourceMatchesEntry = attempt.sourceCandidateHash === trainEntry?.sourceCandidate.candidateHash && canonicalHash(attempt.sourceBase) === canonicalHash(trainEntry?.sourceCandidate.base) && canonicalHash(attempt.sourceCandidate) === canonicalHash(trainEntry?.sourceCandidate.git); const sourceMatchesHistoricalConflict = historicalConflictCandidate?.kind === "candidate" && historicalConflictCandidate.workItemId === trainEntry?.workItemId && canonicalHash(historicalConflictCandidate.base) === canonicalHash(attempt.sourceBase) && canonicalHash(historicalConflictCandidate.git) === canonicalHash(attempt.sourceCandidate);
    pushIssue(issues, `/integrationAttempts/${attemptId}/sourceCandidate`, sourceMatchesEntry || sourceMatchesHistoricalConflict, "must bind the current train-entry candidate or exact immutable historical conflict candidate");
    const bindingFact = attempt.repositoryBindingFactHash ? context.facts[attempt.repositoryBindingFactHash] : undefined;
    pushIssue(issues, `/integrationAttempts/${attemptId}/repositoryBindingFactHash`, isExactGitTransactionFact(bindingFact, state, "repository_binding", train?.repositoryId, null), "must resolve the exact repository/common-dir binding fact");
    if (attempt.compositionFactHash) pushIssue(issues, `/integrationAttempts/${attemptId}/compositionFactHash`, isExactGitTransactionFact(context.facts[attempt.compositionFactHash], state, "composition", train?.repositoryId, attemptId), "must resolve exact composition fact");
    for (const hash of attempt.privateRefFactHashes) pushIssue(issues, `/integrationAttempts/${attemptId}/privateRefFactHashes`, isExactGitTransactionFact(context.facts[hash], state, "private_ref", train?.repositoryId, attemptId), "must resolve exact immutable private-ref fact");
    if (attempt.proposalVerificationFactHash) pushIssue(issues, `/integrationAttempts/${attemptId}/proposalVerificationFactHash`, isExactGitTransactionFact(context.facts[attempt.proposalVerificationFactHash], state, "proposal_verification", train?.repositoryId, attemptId), "must resolve exact proposal-verification fact");
    if (attempt.landingObservationFactHash) pushIssue(issues, `/integrationAttempts/${attemptId}/landingObservationFactHash`, isExactGitTransactionFact(context.facts[attempt.landingObservationFactHash], state, "landing", train?.repositoryId, attemptId), "must resolve exact landing-observation fact");
    const integrationLock = train?.lockLeaseId ? state.leases[train.lockLeaseId] : undefined;
    const landingObservation = attempt.landingObservationFactHash ? context.facts[attempt.landingObservationFactHash] : undefined;
    const exactTargetConflict = landingObservation?.kind === "git_transaction" && landingObservation.factType === "landing" && landingObservation.reconciliation === "conflict";
    if (attempt.landingState !== "landed" && attempt.conflictClass === "none" && !exactTargetConflict) pushIssue(issues, `/integrationAttempts/${attemptId}`, integrationLock?.kind === "integration_lock" && integrationLock.state === "active" && integrationLock.holderIntegrationAttemptId === attemptId && integrationLock.ownerEpoch === state.owner.ownerEpoch && integrationLock.subject.kind === "repository" && integrationLock.subject.id === train?.repositoryId && state.repositories[train!.repositoryId].integrationLockLeaseId === integrationLock.leaseId, "active integration attempt requires one exact current-owner common-repository lock lease");
    if (attempt.conflictClass !== "none" || exactTargetConflict) pushIssue(issues, `/integrationAttempts/${attemptId}`, integrationLock?.holderIntegrationAttemptId !== attemptId, "exact composition/target-conflict attempt must release its integration lock");
    pushIssue(issues, `/integrationAttempts/${attemptId}/compositionEffectId`, state.effects[attempt.compositionEffectId]?.kind === "compose_candidate", "must reference a compose_candidate effect");
    if (attempt.landingEffectId) pushIssue(issues, `/integrationAttempts/${attemptId}/landingEffectId`, state.effects[attempt.landingEffectId]?.kind === "land_target", "must reference a land_target effect");
    pushIssue(issues, `/integrationAttempts/${attemptId}/integrationReceipt`, attempt.integrationReceipt === null || Boolean(state.evidenceIndex.integrationReceipts[attemptId]), "must match indexed integration receipt");
    const validateVerification = (hash: string, phase: "prefix" | "final", profileId: string | undefined, profileHash: string | undefined): boolean => {
      const fact = context.facts[hash];
      return state.evidenceIndex.verifications[hash]?.hash === hash && fact?.kind === "verification" && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.repositoryId === train?.repositoryId && fact.trainId === planTrain?.trainId && fact.integrationAttemptId === attemptId && fact.phase === phase && fact.profileId === profileId && fact.profileHash === profileHash && attempt.composedTree !== null && canonicalHash(fact.tree) === canonicalHash(attempt.composedTree) && fact.disposition === "PASS";
    };
    pushIssue(issues, `/integrationAttempts/${attemptId}/prefixEvidenceHashes`, isSortedUnique([...attempt.prefixEvidenceHashes]) && attempt.prefixEvidenceHashes.every((hash) => validateVerification(hash, "prefix", planTrain?.prefixValidationProfileId, planTrain?.prefixValidationProfileHash)), "prefix evidence must resolve to exact passing composed-tree profile facts");
    pushIssue(issues, `/integrationAttempts/${attemptId}/finalEvidenceHashes`, isSortedUnique([...attempt.finalEvidenceHashes]) && attempt.finalEvidenceHashes.every((hash) => validateVerification(hash, "final", planTrain?.finalValidationProfileId, planTrain?.finalValidationProfileHash)), "final evidence must resolve to exact passing composed-tree profile facts");
    if (attempt.landingState === "landed") {
      const receipt = attempt.integrationReceipt ? context.facts[attempt.integrationReceipt] : undefined;
      pushIssue(issues, `/integrationAttempts/${attemptId}`, attempt.composedTree !== null && attempt.intendedLandedTree !== null && canonicalHash(attempt.composedTree) === canonicalHash(attempt.intendedLandedTree) && attempt.syntheticParentCommit === attempt.expectedPrefix.commit && attempt.sourceToIntegratedLineageHash !== null && attempt.environmentClosureHash !== null && attempt.integrationReceipt !== null && attempt.prefixEvidenceHashes.length > 0 && attempt.finalEvidenceHashes.length > 0 && receipt?.kind === "integration" && receipt.transactionReceiptHash && receipt.landingObservationHash === attempt.landingObservationFactHash, "landed attempt requires exact composed/intended tree, one synthetic parent, source lineage, required profile evidence, transaction/landing receipts, and accepted integration fact");
    }
  }

  const expectedCurrent = {
    readyWorkItemIds: idsByCurrent(state, ["ready"]),
    activeWorkItemIds: activeLaneIds,
    blockedWorkItemIds: idsByCurrent(state, ["blocked"]),
    integrationReadyWorkItemIds: idsByCurrent(state, ["integration_ready"]),
  };
  for (const [field, expected] of Object.entries(expectedCurrent)) pushIssue(issues, `/current/${field}`, JSON.stringify((state.current as any)[field]) === JSON.stringify(expected), "must equal the derived work-item projection");
  const completeIds = idsByCurrent(state, ["complete"]);
  pushIssue(issues, "/completion/completeWorkItemIds", JSON.stringify(state.completion.completeWorkItemIds) === JSON.stringify(completeIds), "must equal complete work items");
  pushIssue(issues, "/completion/completedRepositoryIds", state.completion.completedRepositoryIds.every((id) => Boolean(state.repositories[id])), "contains an unknown repository");
  pushIssue(issues, "/completion/completedAt", state.completion.state === "open" ? state.completion.completedAt === null : state.completion.completedAt !== null, "completion timestamp must be present exactly for a closed authorized scope");
  if (state.completion.state === "plan_complete") {
    pushIssue(issues, "/completion/completeWorkItemIds", completeIds.length === Object.keys(state.workItems).length, "plan_complete requires every plan work item to be complete");
    pushIssue(issues, "/completion/remainingAuthorizedWorkItemIds", state.completion.remainingAuthorizedWorkItemIds.length === 0, "plan_complete cannot have remaining authorized work");
    pushIssue(issues, "/completion/unauthorizedWorkItemIds", state.completion.unauthorizedWorkItemIds.length === 0, "plan_complete cannot have unauthorized work");
    const requiredRepositories = [...new Set(Object.values(state.workItems).map(({ writeRepositoryId }) => writeRepositoryId))].sort();
    pushIssue(issues, "/completion/completedRepositoryIds", JSON.stringify(state.completion.completedRepositoryIds) === JSON.stringify(requiredRepositories), "plan_complete requires every write repository to be completed");
    pushIssue(issues, "/current/run", state.current.run === "completed", "plan_complete requires current completed state");
    pushIssue(issues, "/completion/completedAt", state.completion.completedAt !== null, "plan_complete requires completedAt");
  }
  validateSortedSets(state, issues);
}

function fixedStageProducers(stage: typeof PLAN_STAGE_IDS[number]): string[] {
  return ({ F0: ["conductor"], F1: ["owned_worker"], F2: ["owned_worker"], F3: ["owned_worker"], F4: ["deterministic_runner"], F5: ["owned_worker"], F6: ["owned_worker", "deterministic_runner"], F7: ["deterministic_runner"], F8: ["conductor"] } as Record<string, string[]>)[stage];
}
function validatePassedStage(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, stage: typeof PLAN_STAGE_IDS[number], projection: any, itemPath: string, issues: ValidationIssue[]): void {
  const path = `${itemPath}/stages/${stage}`;
  pushIssue(issues, `${path}/currentAttemptId`, projection.currentAttemptId !== null, "passed stage requires the exact sealed current attempt");
  pushIssue(issues, `${path}/lastDisposition`, projection.lastDisposition === "PASS", "stage passage requires PASS; check waivers or applicability never replace stage execution");
  const stageIndex = PLAN_STAGE_IDS.indexOf(stage);
  for (const predecessor of PLAN_STAGE_IDS.slice(0, stageIndex)) pushIssue(issues, `${path}/state`, item.stages[predecessor].state === "passed", `cannot pass before ${predecessor}`);
  if (!projection.currentAttemptId || !projection.currentEvidence) return;
  const attempt = state.stageAttempts[projection.currentAttemptId];
  pushIssue(issues, `${path}/currentAttemptId`, attempt?.state === "sealed", "passed stage requires a sealed attempt");
  pushIssue(issues, `${path}/currentEvidence`, attempt?.evidence?.hash === projection.currentEvidence, "must equal the sealed attempt evidence hash");
  const indexed = state.evidenceIndex.stageEvidence[projection.currentEvidence];
  pushIssue(issues, `${path}/currentEvidence`, indexed?.hash === projection.currentEvidence && indexed?.kind === "stage_evidence", "must reference exact indexed stage evidence");
  const fact = context.facts[projection.currentEvidence];
  pushIssue(issues, `${path}/currentEvidence`, fact?.kind === "stage_evidence", "requires a validated immutable stage-evidence binding");
  if (!attempt || fact?.kind !== "stage_evidence") return;
  pushIssue(issues, `${path}/currentEvidence`, fact.hash === projection.currentEvidence && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce, "fact must bind exact plan and run identity");
  pushIssue(issues, `${path}/currentEvidence`, fact.workItemId === item.workItemId && fact.stage === stage && fact.stageAttemptId === attempt.stageAttemptId, "fact must bind exact work item, stage, and attempt");
  pushIssue(issues, `${path}/currentEvidence`, fact.attemptInputHash === attempt.attemptInput.hash && fact.authorizationSetHash === attempt.authorizationSetHash && fact.authorizationSetHash === state.identity.authorizationSet.hash, "fact must bind exact attempt input and authorization set");
  const procedure = context.catalog.procedures[fact.procedureHash];
  const aggregate = context.catalog.checkAggregates[fact.checkAggregateHash];
  pushIssue(issues, `${path}/currentEvidence`, Boolean(procedure && procedure.purpose === "lifecycle" && procedure.stages.includes(stage) && procedure.producerKinds.includes(fact.producerKind) && procedure.environmentProfileHash === fact.environmentProfileHash && procedure.readOnly === fact.readOnly), "procedure and environment must resolve through the exact plan-bound lifecycle catalog");
  pushIssue(issues, `${path}/currentEvidence`, Boolean(aggregate && aggregate.workItemId === item.workItemId && aggregate.stage === stage && aggregate.procedureHash === fact.procedureHash && aggregate.environmentProfileHash === fact.environmentProfileHash && aggregate.disposition === "PASS"), "check aggregate must resolve through the exact plan-bound check catalog");
  const planItem = context.plan.workItems.find(({ workItemId }) => workItemId === item.workItemId);
  if (aggregate && planItem) {
    pushIssue(issues, `${path}/currentEvidence`, sameStrings([...aggregate.oracleIds], [...planItem.oracleIds]), "check aggregate must cover every work-item acceptance oracle exactly");
    const requiredAssertions = planItem.oracleIds.flatMap((oracleId) => (context.plan.acceptanceOracles.find((oracle) => oracle.oracleId === oracleId)?.assertions ?? []).map((assertion) => ({ oracleId, assertion })));
    const expectedAssertionKeys = stage === "F2" ? requiredAssertions.map(({ oracleId, assertion }) => `${oracleId}/${assertion.assertionId}`).sort() : [];
    pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.assertions.map(({ oracleId, assertionId }) => `${oracleId}/${assertionId}`).sort(), expectedAssertionKeys), "F2 must prove every exact oracle assertion; other stages cannot claim assertion execution");
    for (const result of aggregate.assertions) {
      const expected = requiredAssertions.find(({ oracleId, assertion }) => oracleId === result.oracleId && assertion.assertionId === result.assertionId);
      const assertionFact = context.facts[result.evidenceHash];
      pushIssue(issues, `${path}/currentEvidence`, stage === "F2" && Boolean(expected) && state.evidenceIndex.oracleAssertions[result.evidenceHash]?.hash === result.evidenceHash && assertionFact?.kind === "oracle_assertion" && assertionFact.planHash === state.identity.planHash && assertionFact.runId === state.runId && assertionFact.runNonce === state.runNonce && assertionFact.workItemId === item.workItemId && assertionFact.stage === "F2" && assertionFact.oracleId === result.oracleId && assertionFact.assertionId === result.assertionId && assertionFact.procedureId === expected?.assertion.procedureId && assertionFact.environmentProfileId === expected?.assertion.environmentProfileId && assertionFact.observationMethod === expected?.assertion.observationMethod && assertionFact.requiredEvidenceClass === expected?.assertion.requiredEvidenceClass && assertionFact.disposition === "PASS" && assertionFact.observationHash === fact.producerResultHash && Object.entries(state.evidenceIndex).some(([indexName, index]) => indexName !== "oracleAssertions" && Object.values(index as Record<string, { hash: string }>).some((ref) => ref.hash === assertionFact.observationHash)), `oracle assertion ${result.oracleId}/${result.assertionId} must resolve to exact procedure, environment, method, evidence class, and indexed observation`);
    }
    const applicableChecks = planItem.checks.filter(({ phases }) => phases.includes(stage));
    pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.checks.map(({ checkId }) => checkId).sort(), applicableChecks.map(({ checkId }) => checkId).sort()), "check aggregate must cover exactly the checks applicable to this stage");
    for (const check of applicableChecks) {
      const result = aggregate.checks.find(({ checkId }) => checkId === check.checkId);
      pushIssue(issues, `${path}/currentEvidence`, Boolean(result), `check aggregate is missing applicable check ${check.checkId}`);
      if (!result) continue;
      if (check.applicability === "required") pushIssue(issues, `${path}/currentEvidence`, result.disposition !== "NOT_APPLICABLE", `required check ${check.checkId} cannot be not applicable`);
      if (check.applicability === "not_applicable") pushIssue(issues, `${path}/currentEvidence`, result.disposition === "NOT_APPLICABLE", `statically not-applicable check ${check.checkId} requires an exact NOT_APPLICABLE disposition`);
      if (result.disposition !== "PASS" || check.applicability !== "required") {
        pushIssue(issues, `${path}/currentEvidence`, result.applicabilityEvidenceHashes.length > 0 && isSortedUnique([...result.applicabilityEvidenceHashes]), `waived/conditional/not-applicable check ${check.checkId} requires sorted immutable evidence`);
        for (const dispositionHash of result.applicabilityEvidenceHashes) {
          const dispositionFact = context.facts[dispositionHash];
          const predicateHash = check.condition?.contentHash ?? null;
          pushIssue(issues, `${path}/currentEvidence`, state.evidenceIndex.checkDispositions[dispositionHash]?.hash === dispositionHash && dispositionFact?.kind === "check_disposition" && dispositionFact.planHash === state.identity.planHash && dispositionFact.runId === state.runId && dispositionFact.runNonce === state.runNonce && dispositionFact.workItemId === item.workItemId && dispositionFact.stage === stage && dispositionFact.checkId === check.checkId && dispositionFact.disposition === result.disposition && dispositionFact.predicateHash === predicateHash && dispositionFact.authorizationSetHash === state.identity.authorizationSet.hash, `check disposition ${dispositionHash} must be an exact indexed, predicate-bound, authorized fact`);
          if (dispositionFact?.kind === "check_disposition") {
            pushIssue(issues, `${path}/currentEvidence`, isSortedUnique([...dispositionFact.evidenceHashes]) && dispositionFact.evidenceHashes.every((hash) => hash !== dispositionHash && Object.entries(state.evidenceIndex).some(([indexName, index]) => indexName !== "checkDispositions" && Object.values(index as Record<string, { hash: string }>).some((ref) => ref.hash === hash))), `check disposition ${dispositionHash} must cite only indexed immutable source evidence`);
            if (result.disposition === "WAIVED") pushIssue(issues, `${path}/currentEvidence`, dispositionFact.evidenceHashes.some((hash) => Object.values(state.evidenceIndex.waivers).some((ref) => ref.hash === hash)), `waived check ${check.checkId} requires an indexed waiver receipt`);
          }
        }
      }
    }
  }
  pushIssue(issues, `${path}/currentEvidence`, isSortedUnique([...fact.findingHashes]) && isSortedUnique([...fact.effectReconciliationHashes]), "finding and effect-reconciliation hashes must be sorted and deduplicated");
  fact.findingHashes.forEach((hash) => pushIssue(issues, `${path}/currentEvidence`, Object.values(state.evidenceIndex.findings).some((ref) => ref.hash === hash), `finding ${hash} must be indexed`));
  fact.effectReconciliationHashes.forEach((hash) => pushIssue(issues, `${path}/currentEvidence`, Object.values(state.evidenceIndex.effectReconciliations).some((ref) => ref.hash === hash), `effect reconciliation ${hash} must be indexed`));
  pushIssue(issues, `${path}/currentEvidence`, fact.disposition === "PASS", "fact disposition must be PASS");
  pushIssue(issues, `${path}/currentEvidence`, fixedStageProducers(stage).includes(fact.producerKind) && attempt.producerKind === fact.producerKind, "fact and attempt must use the fixed stage producer kind");
  if (fact.producerKind === "owned_worker") {
    const binding = state.workerBindings[attempt.stageAttemptId];
    const resultFact = fact.producerResultHash ? context.facts[fact.producerResultHash] : undefined;
    pushIssue(issues, `${path}/currentEvidence`, attempt.launchIntentId !== null && attempt.workerResult?.hash === fact.producerResultHash && binding?.resultHash === fact.producerResultHash && resultFact?.kind === "worker_result" && resultFact.terminalStatus === "succeeded", "owned-worker passage requires the exact successful generic worker result");
  } else pushIssue(issues, `${path}/currentEvidence`, fact.producerResultHash === null, "conductor/deterministic passage cannot claim a generic worker result");
  const expectedGeneration = ["F1", "F3"].includes(stage) && attempt.reservedOutputGeneration !== null ? attempt.reservedOutputGeneration : attempt.inputGeneration;
  pushIssue(issues, `${path}/currentEvidence`, fact.candidateGeneration === expectedGeneration, "fact must bind the exact attempt candidate generation");
  if (stage === "F0") pushIssue(issues, `${path}/currentEvidence`, fact.candidateHash === null, "F0 must not claim a candidate tree");
  else if (item.candidate !== null && fact.candidateHash !== item.candidate.candidateHash && stage === "F1") {
    const producedCandidate = fact.candidateHash ? context.facts[fact.candidateHash] : undefined;
    pushIssue(issues, `${path}/currentEvidence`, producedCandidate?.kind === "candidate" && producedCandidate.planHash === state.identity.planHash && producedCandidate.runId === state.runId && producedCandidate.runNonce === state.runNonce && producedCandidate.workItemId === item.workItemId && producedCandidate.generation === fact.candidateGeneration && producedCandidate.producedByStageAttemptId === attempt.stageAttemptId && state.evidenceIndex.candidates[fact.candidateHash]?.hash === fact.candidateHash, "historical F1 evidence must bind the exact candidate it produced before a later F3 transition");
  } else if (item.candidate !== null && fact.candidateHash !== item.candidate.candidateHash) {
    const adoption = projection.adoptionReceipt ? context.facts[projection.adoptionReceipt] : undefined;
    const f3Attempt = adoption?.kind === "adoption" ? state.stageAttempts[adoption.f3StageAttemptId] : undefined;
    const deltaProcedure = adoption?.kind === "adoption" ? context.catalog.procedures[adoption.deltaAttestationProcedureHash] : undefined;
    pushIssue(issues, `${path}/adoptionReceipt`, stage === "F2" && adoption?.kind === "adoption" && adoption.planHash === state.identity.planHash && adoption.runId === state.runId && adoption.runNonce === state.runNonce && adoption.workItemId === item.workItemId && adoption.stage === "F2" && adoption.fromCandidateGeneration === fact.candidateGeneration && adoption.fromCandidateHash === fact.candidateHash && adoption.toCandidateGeneration === item.candidate.generation && adoption.toCandidateGeneration === adoption.fromCandidateGeneration + 1 && adoption.toCandidateHash === item.candidate.candidateHash && adoption.f3StageAttemptId === item.candidate.producedByStageAttemptId && f3Attempt?.stage === "F3" && f3Attempt.state === "sealed" && f3Attempt.inputGeneration === adoption.fromCandidateGeneration && f3Attempt.reservedOutputGeneration === adoption.toCandidateGeneration && item.stages.F3.state === "passed" && adoption.evidenceHash === projection.currentEvidence && adoption.sourceEvidenceProcedureHash === fact.procedureHash && deltaProcedure?.purpose === "evidence_only_delta_attestation" && deltaProcedure.stages.includes("F3") && deltaProcedure.readOnly && deltaProcedure.environmentProfileHash === adoption.environmentProfileHash && adoption.environmentProfileHash === fact.environmentProfileHash && adoption.evidenceOnlyDelta === true, "candidate mismatch requires the sole post-F3 generation transition and exact read-only delta attestation");
  } else pushIssue(issues, `${path}/currentEvidence`, item.candidate !== null && fact.candidateHash === item.candidate.candidateHash, "fact must bind the current candidate hash");
  if (["F2", "F5"].includes(stage)) {
    pushIssue(issues, `${path}/currentEvidence`, fact.freshIndependent && fact.readOnly, `${stage} must prove fresh independent read-only evaluation`);
  }
  if (["F4", "F7"].includes(stage)) pushIssue(issues, `${path}/currentEvidence`, fact.readOnly, `${stage} must be read-only`);
  if (stage === "F7") pushIssue(issues, `${path}/currentEvidence`, fact.cleanEnvironment, "F7 must prove a newly materialized clean environment");
  if (attempt.producerKind === "owned_worker") {
    pushIssue(issues, `${path}/currentAttemptId`, attempt.workerResult !== null, "owned-worker stage requires an exact observed worker result");
    pushIssue(issues, `${path}/currentAttemptId`, Boolean(state.workerBindings[attempt.stageAttemptId]), "owned-worker stage requires an exact worker binding");
  }
}
function validateCatalogJoin(state: DagRunStateV1, context: DagRunValidationContextV1, issues: ValidationIssue[]): void {
  const catalog = context.catalog;
  pushIssue(issues, "/catalog/lifecycleProfileHash", catalog.lifecycleProfileHash === state.identity.lifecycleProfileHash && catalog.lifecycleProfileHash === canonicalHash(Object.values(catalog.procedures).sort((left, right) => left.procedureId.localeCompare(right.procedureId))), "must be the canonical hash of the exact run lifecycle procedure catalog");
  const expectedCheckCatalogHash = canonicalHash(context.plan.workItems.map(({ workItemId, checks }) => ({ workItemId, checks })));
  pushIssue(issues, "/catalog/checkCatalogHash", catalog.checkCatalogHash === state.identity.checkCatalogHash && catalog.checkCatalogHash === expectedCheckCatalogHash, "must be the canonical hash of the plan's exact check-applicability catalog");
  for (const [hash, procedure] of Object.entries(catalog.procedures)) {
    pushIssue(issues, `/catalog/procedures/${hash}/hash`, procedure.hash === hash && procedure.hash === hashWithoutField(procedure as unknown as Record<string, unknown>, "hash"), "procedure key and hash must equal canonical procedure content");
    pushIssue(issues, `/catalog/procedures/${hash}/stages`, isSortedUnique([...procedure.stages]), "procedure stages must be sorted and deduplicated");
  }
  for (const stage of PLAN_STAGE_IDS) pushIssue(issues, "/catalog/procedures", Object.values(catalog.procedures).some((procedure) => procedure.purpose === "lifecycle" && procedure.stages.includes(stage) && procedure.producerKinds.some((producer) => fixedStageProducers(stage).includes(producer))), `lifecycle catalog must define a fixed-producer procedure for ${stage}`);
  for (const [hash, aggregate] of Object.entries(catalog.checkAggregates)) {
    pushIssue(issues, `/catalog/checkAggregates/${hash}/hash`, aggregate.hash === hash && aggregate.hash === hashWithoutField(aggregate as unknown as Record<string, unknown>, "hash"), "check aggregate key and hash must equal canonical aggregate content");
    pushIssue(issues, `/catalog/checkAggregates/${hash}/oracleIds`, isSortedUnique([...aggregate.oracleIds]), "oracle IDs must be sorted and deduplicated");
    pushIssue(issues, `/catalog/checkAggregates/${hash}/assertions`, isSortedUnique(aggregate.assertions.map(({ oracleId, assertionId }) => `${oracleId}/${assertionId}`)), "oracle assertion results must be sorted and unique");
    pushIssue(issues, `/catalog/checkAggregates/${hash}/checks`, new Set(aggregate.checks.map(({ checkId }) => checkId)).size === aggregate.checks.length, "check IDs must be unique");
  }
}
function validateRunAuthorizationJoin(state: DagRunStateV1, context: DagRunValidationContextV1, issues: ValidationIssue[]): void {
  const authorization = context.authorization;
  validateTimestampFields(authorization, issues, "/authorization");
  pushIssue(issues, "/identity/authorizationSet/hash", authorization.hash === state.identity.authorizationSet.hash && authorization.hash === hashWithoutField(authorization as unknown as Record<string, unknown>, "hash"), "must match canonical validated authorization-set content");
  pushIssue(issues, "/identity/authorizationSet/hash", authorization.planHash === state.identity.planHash, "authorization set must bind the exact plan hash");
  pushIssue(issues, "/identity/reviewReceipt/hash", authorization.reviewReceiptHash === state.identity.reviewReceipt.hash, "authorization set must bind the exact reviewed projection receipt");
  pushIssue(issues, "/identity/authorizationReceipts", isSortedUnique([...authorization.receiptHashes]) && sameStrings([...authorization.receiptHashes], state.identity.authorizationReceipts.map(({ hash }) => hash).sort()), "must be sorted/deduplicated and match the receipts composing the authorization set");
  pushIssue(issues, "/identity/authorizationSet/hash", authorization.validUntil === null || utcTimestampOrderValue(authorization.validUntil) >= utcTimestampOrderValue(authorization.validFrom), "authorization validity cannot end before it begins");
  pushIssue(issues, "/identity/authorizationSet/hash", utcTimestampOrderValue(authorization.validFrom) <= utcTimestampOrderValue(state.createdAt) && (authorization.validUntil === null || utcTimestampOrderValue(state.createdAt) <= utcTimestampOrderValue(authorization.validUntil)), "current authorization must be valid when the run is created");
  pushIssue(issues, "/identity/authorizationSet/hash", authorization.validUntil === null || utcTimestampOrderValue(state.updatedAt) <= utcTimestampOrderValue(authorization.validUntil), "current authorization must remain valid through the snapshot's latest transition");
  pushIssue(issues, "/scheduler/maxActiveNodes", Number.isSafeInteger(authorization.maxActiveNodes) && authorization.maxActiveNodes > 0 && authorization.maxActiveNodes <= context.plan.workItems.length && state.scheduler.maxActiveNodes === authorization.maxActiveNodes, "must equal the explicit positive authorized active-node limit within the plan work-item maximum");
  pushIssue(issues, "/identity/authorizationSet/hash", authorization.retryCeilingsHash === context.plan.lifecycleBinding.retryPolicyHash, "authorization retry ceilings must bind the plan retry policy");
  pushIssue(issues, "/identity/authorizationSet/hash", isSortedUnique([...authorization.workItemIds]) && isSortedUnique([...authorization.repositoryIds]) && isSortedUnique([...authorization.effectScopeIds]) && isSortedUnique([...authorization.integrationTrainIds]), "authorization sets must be sorted and deduplicated");
  const planWorkItems = new Set(context.plan.workItems.map(({ workItemId }) => workItemId));
  const planRepositories = new Set(context.plan.repositories.map(({ repositoryId }) => repositoryId));
  const planTrains = new Set(context.plan.constraints.integrationTrains.map(({ trainId }) => trainId));
  authorization.workItemIds.forEach((id) => pushIssue(issues, "/identity/authorizationSet/hash", planWorkItems.has(id), `authorization references unknown work item ${id}`));
  authorization.repositoryIds.forEach((id) => pushIssue(issues, "/identity/authorizationSet/hash", planRepositories.has(id), `authorization references unknown repository ${id}`));
  authorization.integrationTrainIds.forEach((id) => pushIssue(issues, "/identity/authorizationSet/hash", planTrains.has(id), `authorization references unknown train ${id}`));
  const authorized = new Set(authorization.workItemIds);
  for (const [workItemId, item] of Object.entries(state.workItems)) {
    const stages = authorization.stageScopes[workItemId] ?? [];
    pushIssue(issues, `/workItems/${workItemId}/authorizedStages`, sameStrings(item.authorizedStages, [...stages]), "must match exact authorization stage scope");
    pushIssue(issues, `/workItems/${workItemId}/authorizedStages`, authorized.has(workItemId) === (item.authorizedStages.length > 0), "unauthorized work item cannot carry authorized stages");
  }
  for (const [workItemId, stages] of Object.entries(authorization.stageScopes)) {
    pushIssue(issues, "/identity/authorizationSet/hash", authorized.has(workItemId), `stage scope references unauthorized work item ${workItemId}`);
    pushIssue(issues, "/identity/authorizationSet/hash", isSortedUnique([...stages]), `stage scope for ${workItemId} must be sorted and deduplicated`);
  }
  const declaredEffectScopes = new Set(context.plan.workItems.flatMap(({ integration }) => integration.effectScopeIds));
  authorization.effectScopeIds.forEach((id) => pushIssue(issues, "/identity/authorizationSet/hash", declaredEffectScopes.has(id), `authorization references undeclared effect scope ${id}`));
  for (const item of context.plan.workItems.filter(({ workItemId }) => authorized.has(workItemId))) {
    pushIssue(issues, "/identity/authorizationSet/hash", authorization.repositoryIds.includes(item.writeRepositoryId), `authorization for ${item.workItemId} must include write repository ${item.writeRepositoryId}`);
    item.integration.trainIds.forEach((trainId) => {
      pushIssue(issues, "/identity/authorizationSet/hash", authorization.integrationTrainIds.includes(trainId), `authorization for ${item.workItemId} must include train ${trainId}`);
      const train = context.plan.constraints.integrationTrains.find(({ trainId: id }) => id === trainId);
      const ordinal = train?.members.findIndex(({ workItemId }) => workItemId === item.workItemId) ?? -1;
      for (const predecessor of train?.members.slice(0, Math.max(0, ordinal)) ?? []) pushIssue(issues, "/identity/authorizationSet/hash", authorized.has(predecessor.workItemId), `authorization for train suffix ${item.workItemId} must include prefix member ${predecessor.workItemId}`);
    });
    item.integration.effectScopeIds.forEach((scopeId) => pushIssue(issues, "/identity/authorizationSet/hash", authorization.effectScopeIds.includes(scopeId), `authorization for ${item.workItemId} must include effect scope ${scopeId}`));
  }
  for (const edge of context.plan.constraints.precedence) if (authorized.has(edge.successorWorkItemId)) pushIssue(issues, "/identity/authorizationSet/hash", authorized.has(edge.predecessorWorkItemId), `authorization must close causal predecessor ${edge.predecessorWorkItemId}`);
  const unauthorizedIds = [...planWorkItems].filter((id) => !authorized.has(id)).sort();
  pushIssue(issues, "/completion/unauthorizedWorkItemIds", sameStrings(state.completion.unauthorizedWorkItemIds, unauthorizedIds), "must equal the exact unauthorized plan work items");
  const completeAuthorized = state.completion.completeWorkItemIds.filter((id) => authorized.has(id));
  const remainingAuthorized = [...authorized].filter((id) => !completeAuthorized.includes(id)).sort();
  pushIssue(issues, "/completion/remainingAuthorizedWorkItemIds", sameStrings(state.completion.remainingAuthorizedWorkItemIds, remainingAuthorized), "must equal authorized work not yet complete");
  const expectedCompletion = remainingAuthorized.length ? "open" : unauthorizedIds.length ? "authorized_scope_complete" : "plan_complete";
  pushIssue(issues, "/completion/state", state.completion.state === expectedCompletion, "must be mechanically derived from exact authorized, unauthorized, and complete work items");
  pushIssue(issues, "/completion/authorizedScopeHash", state.completion.authorizedScopeHash === authorization.hash, "must match the authorization-set hash");
}
function validateRunPlanJoin(state: DagRunStateV1, plan: CanonicalDagPlanV1, issues: ValidationIssue[]): void {
  pushIssue(issues, "/identity/projectId", state.identity.projectId === plan.modelBinding.projectId, "must match the canonical plan project");
  pushIssue(issues, "/identity/planId", state.identity.planId === plan.planId, "must match the canonical plan ID");
  pushIssue(issues, "/identity/planRevision", state.identity.planRevision === plan.revision, "must match the canonical plan revision");
  pushIssue(issues, "/identity/planHash", state.identity.planHash === plan.planHash, "must match the canonical plan hash");
  pushIssue(issues, "/identity/planSchemaHash", state.identity.planSchemaHash === CANONICAL_DAG_PLAN_SCHEMA_HASH, "must match the implemented canonical plan schema hash");
  pushIssue(issues, "/identity/lifecycleProfileHash", state.identity.lifecycleProfileHash === plan.lifecycleBinding.profileHash, "must match the canonical lifecycle profile");
  pushIssue(issues, "/identity/checkCatalogHash", state.identity.checkCatalogHash === plan.lifecycleBinding.checkCatalogHash, "must match the canonical check catalog");
  pushIssue(issues, "/identity/artifactPolicyHash", state.identity.artifactPolicyHash === plan.artifactPolicy.profileHash, "must match the canonical artifact policy");
  pushIssue(issues, "/scheduler/policyVersion", state.scheduler.policyVersion === plan.lifecycleBinding.schedulerPolicyVersion, "must match the plan-bound scheduler policy version");
  pushIssue(issues, "/scheduler/policyHash", state.scheduler.policyHash === plan.lifecycleBinding.schedulerPolicyHash, "must match the plan-bound scheduler policy hash");

  const expectedRepositoryIds = plan.repositories.map(({ repositoryId }) => repositoryId).sort();
  pushIssue(issues, "/repositories", sameStrings(Object.keys(state.repositories).sort(), expectedRepositoryIds), "must contain exactly the plan repositories");
  for (const repository of plan.repositories) {
    const current = state.repositories[repository.repositoryId];
    if (!current) continue;
    pushIssue(issues, `/repositories/${repository.repositoryId}/planEntityHash`, current.planEntityHash === repository.contentHash, "must match the plan repository entity hash");
    pushIssue(issues, `/repositories/${repository.repositoryId}/role`, current.role === repository.role, "must match the plan repository role");
    pushIssue(issues, `/repositories/${repository.repositoryId}/baseline`, canonicalHash(current.baseline) === canonicalHash(repository.baseline), "must match the immutable plan baseline");
    pushIssue(issues, `/repositories/${repository.repositoryId}/targetRef`, current.targetRef === repository.targetRef, "must match the plan target ref");
  }

  const expectedWorkItemIds = plan.workItems.map(({ workItemId }) => workItemId).sort();
  pushIssue(issues, "/workItems", sameStrings(Object.keys(state.workItems).sort(), expectedWorkItemIds), "must contain exactly the plan work items");
  for (const item of plan.workItems) {
    const current = state.workItems[item.workItemId];
    if (!current) continue;
    pushIssue(issues, `/workItems/${item.workItemId}/planEntityHash`, current.planEntityHash === item.contentHash, "must match the plan work-item entity hash");
    pushIssue(issues, `/workItems/${item.workItemId}/writeRepositoryId`, current.writeRepositoryId === item.writeRepositoryId, "must match the plan write repository");
    const expectedPrecedence = plan.constraints.precedence.filter(({ successorWorkItemId }) => successorWorkItemId === item.workItemId).map(({ precedenceId }) => precedenceId).sort();
    const expectedGates = plan.gates.filter(({ blocks }) => blocks.some(({ workItemId }) => workItemId === item.workItemId)).map(({ gateId }) => gateId).sort();
    pushIssue(issues, `/workItems/${item.workItemId}/precedenceIds`, sameStrings(current.precedenceIds, expectedPrecedence), "must match plan causal predecessors");
    pushIssue(issues, `/workItems/${item.workItemId}/gateIds`, sameStrings(current.gateIds, expectedGates), "must match plan gate blockers");
  }

  joinEntityMap(state.gates, plan.gates, "gateId", "contentHash", "/gates", issues);
  for (const gate of plan.gates) if (state.gates[gate.gateId]) {
    pushIssue(issues, `/gates/${gate.gateId}/kind`, state.gates[gate.gateId].kind === gate.kind, "must match plan gate kind");
    pushIssue(issues, `/gates/${gate.gateId}/releaseMode`, state.gates[gate.gateId].releaseMode === gate.releaseMode, "must match plan gate release mode");
    pushIssue(issues, `/gates/${gate.gateId}/blockedWorkItemStages`, canonicalHash(state.gates[gate.gateId].blockedWorkItemStages) === canonicalHash(gate.blocks), "must match plan phase-scoped blockers");
  }
  joinEntityMap(state.precedence, plan.constraints.precedence, "precedenceId", "contentHash", "/precedence", issues);
  for (const edge of plan.constraints.precedence) if (state.precedence[edge.precedenceId]) {
    const current = state.precedence[edge.precedenceId];
    pushIssue(issues, `/precedence/${edge.precedenceId}/predecessorWorkItemId`, current.predecessorWorkItemId === edge.predecessorWorkItemId, "must match plan predecessor");
    pushIssue(issues, `/precedence/${edge.precedenceId}/successorWorkItemId`, current.successorWorkItemId === edge.successorWorkItemId, "must match plan successor");
    pushIssue(issues, `/precedence/${edge.precedenceId}/releaseDisposition`, current.releaseDisposition === "integrated", "causal release requires accepted integration");
  }
  joinEntityMap(state.resourcePools, plan.constraints.resourceClasses, "resourceClassId", "contentHash", "/resourcePools", issues);
  for (const resource of plan.constraints.resourceClasses) if (state.resourcePools[resource.resourceClassId]) pushIssue(issues, `/resourcePools/${resource.resourceClassId}/semanticMaximum`, state.resourcePools[resource.resourceClassId].semanticMaximum === resource.semanticMaximum, "must match plan semantic maximum");
  joinEntityMap(state.mutexes, plan.constraints.semanticMutexes, "mutexGroupId", "contentHash", "/mutexes", issues);
  const planTrainsByRepository = new Map(plan.constraints.integrationTrains.map((train) => [train.repositoryId, train]));
  pushIssue(issues, "/integrationTrains", sameStrings(Object.keys(state.integrationTrains).sort(), [...planTrainsByRepository.keys()].sort()), "must contain exactly one run train for every plan train repository");
  for (const [repositoryId, train] of planTrainsByRepository) {
    const current = state.integrationTrains[repositoryId];
    if (!current) continue;
    pushIssue(issues, `/integrationTrains/${repositoryId}/planTrainHash`, current.planTrainHash === train.contentHash, "must match the plan train entity hash");
    pushIssue(issues, `/integrationTrains/${repositoryId}/strategy`, current.strategy === train.strategy, "must match the plan integration strategy");
    pushIssue(issues, `/integrationTrains/${repositoryId}/targetRef`, current.targetRef === plan.repositories.find(({ repositoryId: id }) => id === repositoryId)?.targetRef, "must match the plan repository target");
  }
}
function joinEntityMap(stateValues: Record<string, any>, planValues: readonly any[], idField: string, hashField: string, path: string, issues: ValidationIssue[]): void {
  const expectedIds = planValues.map((value) => value[idField]).sort();
  pushIssue(issues, path, sameStrings(Object.keys(stateValues).sort(), expectedIds), "must contain exactly the corresponding plan entities");
  for (const value of planValues) if (stateValues[value[idField]]) pushIssue(issues, `${path}/${value[idField]}/planEntityHash`, stateValues[value[idField]].planEntityHash === value[hashField], "must match the plan entity hash");
}
function exactIntegrationConflictFence(state: DagRunStateV1, context: DagRunValidationContextV1, item: any): any | null {
  if (item.current !== "active" || item.currentStage !== "F1" || item.candidate !== null) return null;
  for (const train of Object.values(state.integrationTrains)) for (const entry of Object.values(train.entries)) {
    if (entry.workItemId !== item.workItemId || entry.state !== "invalidated" || !entry.currentAttemptId) continue;
    const attempt = state.integrationAttempts[entry.currentAttemptId]; const fact = attempt?.compositionFactHash ? context.facts[attempt.compositionFactHash] : undefined;
    if (attempt?.conflictClass !== "none" && fact?.kind === "git_transaction" && fact.factType === "composition" && fact.reconciliation === "conflict" && isExactGitTransactionFact(fact, state, "composition", train.repositoryId, attempt.integrationAttemptId) && item.candidateGeneration === entry.sourceCandidate.generation + 1) return entry;
  }
  return null;
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}
function isExactGitTransactionFact(fact: any, state: DagRunStateV1, factType: string, repositoryId: string | undefined, integrationAttemptId: string | null): boolean {
  return Boolean(fact && fact.kind === "git_transaction" && fact.factType === factType && fact.hash === hashWithoutField(fact as Record<string, unknown>, "hash") && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.authorizationSetHash === state.identity.authorizationSet.hash && fact.repositoryId === repositoryId && fact.integrationAttemptId === integrationAttemptId);
}

function validateEvidenceIndex(state: DagRunStateV1, issues: ValidationIssue[]): void {
  const expectedKinds: Record<string, string> = {
    stageAttemptInputs: "stage_attempt_input", workerResults: "worker_result", candidates: "candidate", stageEvidence: "stage_evidence",
    checkDispositions: "check_disposition", verifications: "verification", oracleAssertions: "oracle_assertion", findings: "finding", findingResolutions: "finding_resolution", waivers: "waiver", invalidations: "invalidation", adoptions: "adoption",
    effectReconciliations: "effect_reconciliation", integrationReady: "integration_ready", integrationReceipts: "integration",
    stalenessReceipts: "staleness", gateReceipts: "gate_release",
  };
  const hashKeyed = new Set(["workerResults", "candidates", "stageEvidence", "stalenessReceipts"]);
  for (const [collection, values] of Object.entries(state.evidenceIndex)) {
    for (const [key, ref] of Object.entries(values as Record<string, any>)) {
      pushIssue(issues, `/evidenceIndex/${collection}/${key}/kind`, ref.kind === expectedKinds[collection], `must be ${expectedKinds[collection]}`);
      if (hashKeyed.has(collection)) pushIssue(issues, `/evidenceIndex/${collection}/${key}/hash`, ref.hash === key, "must equal hash-index key");
    }
  }
}
function validateMapKeys(values: Record<string, any>, field: string, path: string, issues: ValidationIssue[]): void {
  for (const [key, value] of Object.entries(values)) pushIssue(issues, `${path}/${key}/${field}`, value[field] === key, `must equal map key ${key}`);
}
function idsByCurrent(state: DagRunStateV1, states: string[]): string[] {
  return Object.values(state.workItems).filter(({ current }) => states.includes(current)).map(({ workItemId }) => workItemId).sort();
}
function validateSortedSets(value: unknown, issues: ValidationIssue[], path = ""): void {
  if (Array.isArray(value)) return;
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    const childPath = `${path}/${key}`;
    if (Array.isArray(item) && (key.endsWith("Ids") || key.endsWith("Hashes"))) {
      const strings = item.filter((candidate): candidate is string => typeof candidate === "string");
      pushIssue(issues, childPath, strings.length === item.length && isSortedUnique(strings), "must be sorted and deduplicated");
    } else validateSortedSets(item, issues, childPath);
  }
}

export function sealDagRunStateV1(input: Omit<DagRunStateV1, "snapshotHash">, context: DagRunValidationContextV1): DagRunStateV1 {
  const state = { ...input, snapshotHash: canonicalHash(input) } as DagRunStateV1;
  assertDagRunStateV1(state, context);
  return state;
}
