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
  canonicalStringify,
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

export const MAX_OWNERSHIP_LINEAGE_DEPTH_V1 = 4096;

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
  environmentObservationHash?: string | null;
  producedAt: string;
  readOnly: boolean;
  /** @deprecated Ignored for independence; retained only for pre-kernel F0/F1 fact compatibility. */
  freshIndependent?: boolean;
  /** @deprecated Ignored for cleanliness; retained only for pre-kernel non-F7 fact compatibility. */
  cleanEnvironment?: boolean;
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

export interface WorkerConfigFactBindingV1 {
  kind: "worker_config";
  hash: string;
  configHash: string;
  config: Readonly<Record<string, unknown>>;
}

export interface WorkerLaunchObservationFactBindingV1 {
  kind: "worker_launch_observation";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  ownerEpoch: number;
  effectId: string;
  requestHash: string;
  launchIntentId: string;
  launchKey: string;
  workerStorageId: string;
  launchOwnerSessionId: string;
  workerId: string;
  attemptNumber: number;
  attemptNonce: string;
  configHash: string;
  supervisorPid: number;
  supervisorStartIdentity: string;
  reconciliation: "applied_exact";
  observedAt: string;
}

export interface WorkerResultFactBindingV1 {
  kind: "worker_result";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  launchIntentId: string;
  workerStorageId: string;
  launchOwnerSessionId: string;
  workerId: string;
  attemptNumber: number;
  attemptNonce: string;
  configHash: string;
  completionId: string;
  terminalStatus: "succeeded" | "needs_attention" | "failed" | "cancelled" | "lost";
  processDisposition?: "dead" | "ambiguous";
  retrySafe?: boolean;
  outputRepositoryId: string | null;
  outputCommonDirIdentityHash: string | null;
  outputWorktreeIdentityHash: string | null;
  outputSourceBase: GitTreeRefV1 | null;
  outputCommit: string | null;
  outputTree: string | null;
  outputObjectFormat: "sha1" | "sha256" | null;
  candidateObservedAt: string | null;
}

export interface FindingFactBindingV1 {
  kind: "finding";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  findingId: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  evidenceHash: string;
  findingKind: "product_defect" | "test_evidence_gap" | "architecture_issue" | "oracle_contract_issue" | "infrastructure_failure" | "capability_absent" | "external_precondition_failure" | "equivalent_nonactionable";
  severity: "advisory" | "blocking";
  materiality: "local" | "plan_affecting";
  fingerprint: string;
  semanticSubjectId: string;
  observedAt: string;
}

export interface FindingResolutionFactBindingV1 {
  kind: "finding_resolution";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  findingId: string;
  findingHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  disposition: "corrected" | "equivalent_accepted" | "successor_plan_required" | "invalidated" | "dismissed" | "misclassified";
  supersedingEvidenceHash: string | null;
  resolvedAt: string;
}

export interface FindingCorrectionFactBindingV1 {
  kind: "finding_correction";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  findingId: string;
  findingHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  candidateGeneration: number;
  candidateHash: string | null;
  observedAt: string;
}

export interface CheckAggregateFactBindingV1 {
  kind: "check_aggregate";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  procedureHash: string;
  environmentProfileHash: string;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  oracleIds: readonly string[];
  assertions: readonly { oracleId: string; assertionId: string; evidenceHash: string }[];
  checks: readonly { checkId: string; disposition: "PASS" | "FAIL" | "BLOCKED" | "WAIVED" | "NOT_APPLICABLE" | "BUDGET_EXHAUSTED"; executionEvidenceHash?: string | null; applicabilityEvidenceHashes: readonly string[] }[];
}

export interface WorkspaceMaterializationFactBindingV1 {
  kind: "workspace_materialization";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stageAttemptId: string;
  repositoryId: string;
  candidateGeneration: number;
  candidateHash: string;
  candidateTree: GitTreeRefV1;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  materializedAt: string;
}

export interface EnvironmentObservationFactBindingV1 {
  kind: "environment_observation";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash?: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  repositoryId: string;
  candidateGeneration: number;
  candidateHash: string;
  candidateTree: GitTreeRefV1;
  environmentProfileHash: string;
  workspaceMaterializationHash: string;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  cleanliness: "clean" | "dirty" | "unknown";
  observedAt: string;
}

export interface CheckExecutionFactBindingV1 {
  kind: "check_execution";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  candidateGeneration: number;
  candidateHash: string | null;
  checkId: string;
  procedureHash: string;
  environmentProfileHash: string;
  environmentObservationHash: string | null;
  executionId: string;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  startedAt: string;
  completedAt: string;
}

export interface ProcedureExecutionFactBindingV1 {
  kind: "procedure_execution";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  fromCandidateGeneration: number;
  fromCandidateHash: string;
  toCandidateGeneration: number;
  toCandidateHash: string;
  procedureHash: string;
  environmentProfileHash: string;
  executableArtifactHash: string;
  environmentHash: string;
  executionId: string;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  startedAt: string;
  completedAt: string;
  occurredAt: string;
}

export interface CheckWaiverFactBindingV1 {
  kind: "waiver";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  checkId: string;
  predicateHash: string | null;
  issuedBy: "user";
  issuedAt: string;
}

export interface CheckApplicabilityFactBindingV1 {
  kind: "check_applicability";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  checkId: string;
  predicateHash: string | null;
  applicable: boolean;
  observedAt: string;
}

export interface CheckDispositionFactBindingV1 {
  kind: "check_disposition";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: typeof PLAN_STAGE_IDS[number];
  stageAttemptId: string;
  attemptInputHash: string;
  checkId: string;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "WAIVED" | "NOT_APPLICABLE" | "BUDGET_EXHAUSTED";
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
  authorizationSetHash: string;
  ownerEpoch: number;
  freshnessReceiptHash: string;
  effectId: string;
  requestHash: string;
  requestIdentityHash: string;
  repositoryId: string;
  trainId: string;
  integrationAttemptId: string;
  phase: "prefix" | "final";
  profileId: string;
  profileHash: string;
  executableArtifactHash: string;
  argvHash: string;
  cwdMode: "detached_proposal_worktree";
  environmentProfileId: string;
  environmentProfileHash: string;
  environmentHash: string;
  timeoutMs: number;
  readOnly: true;
  noEdit: true;
  tree: GitTreeRefV1;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  objectFormat: "sha1" | "sha256";
  executionId: string;
  exitCode: number | null;
  signal: string | null;
  outputHash: string;
  stdoutHash: string;
  stderrHash: string;
  outputBytes: number;
  parser: "strict-json-disposition-v1";
  parserDisposition: "PASS" | "FAIL" | "BLOCKED";
  parsedResultHash: string | null;
  startedAt: string;
  completedAt: string;
  disposition: "PASS" | "FAIL" | "BLOCKED";
}

export interface IntegrationValidationProfileMappingV1 {
  profileId: string;
  executableArtifactHash: string;
  argv: readonly string[];
  cwdMode: "detached_proposal_worktree";
  environmentProfileId: string;
  environmentProfileHash: string;
  environment: Readonly<Record<string, string>>;
  environmentHash: string;
  timeoutMs: number;
  readOnly: true;
  noEdit: true;
}

export interface OracleAssertionFactBindingV1 {
  kind: "oracle_assertion";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  workItemId: string;
  stage: "F2";
  stageAttemptId: string;
  attemptInputHash: string;
  authorizationSetHash: string;
  oracleId: string;
  assertionId: string;
  procedureId: string;
  environmentProfileId: string;
  observationMethod: "static_analysis" | "automated_check" | "manual_observation" | "external_observation" | "combined";
  requiredEvidenceClass: "deterministic" | "independent" | "manual" | "external";
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  observationHash: string;
}

export interface EffectExecutionObservationFactBindingV1 {
  kind: "effect_execution_observation";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  freshnessReceiptHash: string;
  ownerEpoch: number;
  effectId: string;
  requestHash: string;
  requestIdentityHash: string;
  operationKind: "lifecycle_procedure";
  executionId: string;
  resultIdentityHash: string;
  result: Record<string, unknown>;
  disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED";
  resultBytes: number;
  startedAt: string;
  completedAt: string;
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
  executionObservationHash?: string;
  resultIdentityHash?: string;
  closedAt: string;
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
  priorOwnershipReceiptHash: string | null;
  ownerEpoch: number;
  successorSessionId: string;
  successorPid: number;
  successorProcessStartIdentity: string;
  successorLockIdentity: string;
  lineageHash: string | null;
  chainHash: string;
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
  deltaAttestationExecutionHash: string;
  occurredAt: string;
  evidenceOnlyDelta: true;
}

export interface F2InvalidationFactBindingV1 {
  kind: "invalidation";
  hash: string;
  planHash: string;
  runId: string;
  runNonce: string;
  authorizationSetHash: string;
  workItemId: string;
  stage: "F2";
  fromCandidateGeneration: number;
  fromCandidateHash: string;
  toCandidateGeneration: number;
  toCandidateHash: string;
  f3StageAttemptId: string;
  priorEvidenceHash: string;
  reason: "behavior_bearing" | "unknown_impact";
  rerouteStage: "F2";
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
  prefixEffectReconciliationHashes: readonly string[];
  finalEffectReconciliationHashes: readonly string[];
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
  sealedAt: string;
}

export type DagRunFactBindingV1 = GitTransactionFactBindingV1 | GitIntegrationReceiptFactBindingV1 | ProcessIdentityObservationBindingV1 | CorruptFactEnvelopeBindingV1 | StageAttemptInputFactBindingV1 | WorkerConfigFactBindingV1 | WorkerLaunchObservationFactBindingV1 | WorkerResultFactBindingV1 | FindingFactBindingV1 | FindingResolutionFactBindingV1 | FindingCorrectionFactBindingV1 | WorkspaceMaterializationFactBindingV1 | EnvironmentObservationFactBindingV1 | StageEvidenceFactBindingV1 | CheckAggregateFactBindingV1 | CheckExecutionFactBindingV1 | ProcedureExecutionFactBindingV1 | CheckWaiverFactBindingV1 | CheckApplicabilityFactBindingV1 | CheckDispositionFactBindingV1 | VerificationFactBindingV1 | OracleAssertionFactBindingV1 | EffectExecutionObservationFactBindingV1 | EffectReconciliationFactBindingV1 | QuarantineResolutionFactBindingV1 | OwnershipFactBindingV1 | CandidateFactBindingV1 | EvidenceAdoptionFactBindingV1 | F2InvalidationFactBindingV1 | IntegrationReadyFactBindingV1 | IntegrationFactBindingV1;
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
export interface ProcedureExecutableMappingV1 {
  executableArtifactHash: string;
  argv: readonly string[];
  cwdMode: "repository_root" | "attempt_worktree" | "run_root";
  environmentProfileId: string;
  environmentProfileHash: string;
  environmentHash: string;
  timeoutMs: number;
  readOnly: boolean;
  noEdit: boolean;
}
export interface ProcedureCatalogBindingV1 {
  hash: string;
  procedureId: string;
  purpose: "lifecycle" | "evidence_only_delta_attestation";
  stages: readonly typeof PLAN_STAGE_IDS[number][];
  producerKinds: readonly ("conductor" | "owned_worker" | "deterministic_runner")[];
  readOnly: boolean;
  environmentProfileHash: string;
  executable: ProcedureExecutableMappingV1;
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
  checks: readonly { checkId: string; disposition: "PASS" | "WAIVED" | "NOT_APPLICABLE"; executionEvidenceHash?: string | null; applicabilityEvidenceHashes: readonly string[] }[];
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
  /** Explicit immutable validation command mappings, keyed by their canonical content hash. */
  integrationValidationProfiles?: Readonly<Record<string, IntegrationValidationProfileMappingV1>>;
  authorityReceipts?: Readonly<Record<string, QuarantineAuthorityBindingV1>>;
}

const RunDesiredSchema = Type.Enum(["running", "paused", "needs_replan", "cancelled", "superseded"]);
const RunCurrentSchema = Type.Enum(["initializing", "active", "paused", "needs_replan", "cancelling", "blocked", "integration", "needs_decision", "completed", "cancelled", "superseded"]);
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
const FindingClosureSchema = Type.Enum(["open", "corrected", "equivalent_accepted", "successor_plan_required", "invalidated", "dismissed", "misclassified"]);
const FailureClassSchema = Type.Enum(["product", "evidence", "architecture", "oracle_contract", "infrastructure", "capability", "external_precondition", "worker_runtime", "cancellation", "integration", "integrity"]);
const ProcedureClassSchema = Type.Enum(["pure", "idempotent", "compensatable", "non_repeatable", "unknown"]);
const EffectStateSchema = Type.Enum(["intended", "dispatching", "observed", "reconciled", "failed", "ambiguous", "cancelled"]);
const ReconciliationSchema = Type.Enum(["not_started", "applied_exact", "compensated", "proven_absent", "conflict", "unknown"]);
const RetryDimensionSchema = Type.Enum(["product_repair", "test_rework", "review_rework", "hardening_rework", "infrastructure", "worker_replacement", "integration"]);
const RetryStopSchema = Type.Enum(["none", "ceiling_reached", "repeated_fingerprint", "no_material_progress", "repeated_tree", "oscillation", "unreconciled_effect", "authorization_required"]);
const BlockerKindSchema = Type.Enum(["precedence", "gate", "authorization", "plan_staleness", "integration_drift", "resource_capacity", "semantic_mutex", "repository_lease", "side_effect_unreconciled", "retry_exhausted", "no_progress", "finding", "cancellation", "launch_ambiguous", "worker_lost", "capability", "external_precondition", "successor_plan_required", "operator_decision", "corrupt_fact", "concurrent_owner"]);
const BlockerReleaseSchema = Type.Enum(["automatic", "immutable_fact", "operator", "successor_plan"]);
const LeaseKindSchema = Type.Enum(["stage_claim", "resource", "operational", "semantic_mutex", "repository_workspace", "integration_lock"]);
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
const FactKindSchema = Type.Enum(["plan_review", "plan_authorization", "authorization_set", "staleness", "stage_attempt_input", "worker_config", "worker_launch_observation", "worker_result", "workspace_materialization", "environment_observation", "candidate", "stage_evidence", "check_aggregate", "check_execution", "procedure_execution", "check_applicability", "check_disposition", "oracle_assertion", "finding", "finding_resolution", "finding_correction", "waiver", "invalidation", "adoption", "effect_intent", "effect_reconciliation", "corrupt_fact", "process_identity_observation", "quarantine_resolution", "quarantine_authority", "integration_ready", "integration", "ownership", "gate_release", "repository_observation", "verification", "git_transaction", "git_integration_receipt"]);
const StageAttemptInputFactBindingV1Schema = StrictObject({
  kind: Type.Literal("stage_attempt_input"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema),
  authorizationSetHash: HashSchema, producerKind: ProducerKindSchema, implementationLineageHash: Nullable(HashSchema),
});
const WorkerConfigFactBindingV1Schema = StrictObject({
  kind: Type.Literal("worker_config"), hash: HashSchema, configHash: HashSchema,
  config: Type.Record(Type.String({ minLength: 1, maxLength: 256 }), Type.Unknown()),
});
const WorkerLaunchObservationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("worker_launch_observation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema, ownerEpoch: PositiveIntegerSchema,
  effectId: IdSchema, requestHash: HashSchema, launchIntentId: IdSchema, launchKey: IdSchema,
  workerStorageId: IdSchema, launchOwnerSessionId: IdSchema, workerId: IdSchema, attemptNumber: PositiveIntegerSchema,
  attemptNonce: Type.String({ minLength: 16, maxLength: 256 }), configHash: HashSchema,
  supervisorPid: PositiveIntegerSchema, supervisorStartIdentity: Type.String({ minLength: 1, maxLength: 256 }),
  reconciliation: Type.Literal("applied_exact"), observedAt: TimestampSchema,
});
const WorkerResultFactBindingV1Schema = StrictObject({
  kind: Type.Literal("worker_result"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, launchIntentId: IdSchema, workerStorageId: IdSchema,
  launchOwnerSessionId: IdSchema, workerId: IdSchema, attemptNumber: PositiveIntegerSchema,
  attemptNonce: Type.String({ minLength: 16, maxLength: 256 }), configHash: HashSchema, completionId: IdSchema,
  terminalStatus: Type.Enum(["succeeded", "needs_attention", "failed", "cancelled", "lost"]),
  processDisposition: Type.Optional(Type.Enum(["dead", "ambiguous"])), retrySafe: Type.Optional(Type.Boolean()),
  outputRepositoryId: Nullable(IdSchema), outputCommonDirIdentityHash: Nullable(HashSchema), outputWorktreeIdentityHash: Nullable(HashSchema),
  outputSourceBase: Nullable(GitTreeRefV1Schema), outputCommit: Nullable(GitOidSchema), outputTree: Nullable(GitOidSchema),
  outputObjectFormat: Nullable(Type.Enum(["sha1", "sha256"])), candidateObservedAt: Nullable(TimestampSchema),
});
const LegacyQuarantinableWorkerResultFactBindingV1Schema = StrictObject({
  kind: Type.Literal("worker_result"), hash: HashSchema, workerStorageId: IdSchema,
  launchOwnerSessionId: IdSchema, workerId: IdSchema, attemptNumber: PositiveIntegerSchema,
  attemptNonce: Type.String({ minLength: 16, maxLength: 256 }), configHash: HashSchema, completionId: IdSchema,
  terminalStatus: Type.Enum(["succeeded", "needs_attention", "failed", "cancelled", "lost"]),
  processDisposition: Type.Optional(Type.Enum(["dead", "ambiguous"])), retrySafe: Type.Optional(Type.Boolean()),
});
const FindingFactBindingV1Schema = StrictObject({
  kind: Type.Literal("finding"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  findingId: IdSchema, workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema,
  attemptInputHash: HashSchema, evidenceHash: HashSchema, findingKind: FindingKindSchema,
  severity: FindingSeveritySchema, materiality: FindingMaterialitySchema, fingerprint: HashSchema,
  semanticSubjectId: IdSchema, observedAt: TimestampSchema,
});
const FindingResolutionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("finding_resolution"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  findingId: IdSchema, findingHash: HashSchema, workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  disposition: Type.Enum(["corrected", "equivalent_accepted", "successor_plan_required", "invalidated", "dismissed", "misclassified"]),
  supersedingEvidenceHash: Nullable(HashSchema), resolvedAt: TimestampSchema,
});
const FindingCorrectionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("finding_correction"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  findingId: IdSchema, findingHash: HashSchema, workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, candidateGeneration: NonNegativeIntegerSchema,
  candidateHash: Nullable(HashSchema), observedAt: TimestampSchema,
});
const WorkspaceMaterializationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("workspace_materialization"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stageAttemptId: IdSchema,
  repositoryId: IdSchema, candidateGeneration: PositiveIntegerSchema, candidateHash: HashSchema,
  candidateTree: GitTreeRefV1Schema, commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema,
  materializedAt: TimestampSchema,
});
const EnvironmentObservationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("environment_observation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: Type.Optional(HashSchema), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, repositoryId: IdSchema,
  candidateGeneration: PositiveIntegerSchema, candidateHash: HashSchema, candidateTree: GitTreeRefV1Schema,
  environmentProfileHash: HashSchema, workspaceMaterializationHash: HashSchema,
  commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema,
  cleanliness: Type.Enum(["clean", "dirty", "unknown"]), observedAt: TimestampSchema,
});
const CheckAggregateFactBindingV1Schema = StrictObject({
  kind: Type.Literal("check_aggregate"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  procedureHash: HashSchema, environmentProfileHash: HashSchema,
  disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]), oracleIds: StringSet(),
  assertions: Type.Array(StrictObject({ oracleId: IdSchema, assertionId: IdSchema, evidenceHash: HashSchema })),
  checks: Type.Array(StrictObject({ checkId: IdSchema, disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "WAIVED", "NOT_APPLICABLE", "BUDGET_EXHAUSTED"]), executionEvidenceHash: Type.Optional(Nullable(HashSchema)), applicabilityEvidenceHashes: Type.Array(HashSchema) })),
});
const CheckExecutionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("check_execution"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema), checkId: IdSchema,
  procedureHash: HashSchema, environmentProfileHash: HashSchema, environmentObservationHash: Nullable(HashSchema),
  executionId: IdSchema, disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  startedAt: TimestampSchema, completedAt: TimestampSchema,
});
const ProcedureExecutionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("procedure_execution"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  fromCandidateGeneration: PositiveIntegerSchema, fromCandidateHash: HashSchema,
  toCandidateGeneration: PositiveIntegerSchema, toCandidateHash: HashSchema,
  procedureHash: HashSchema, environmentProfileHash: HashSchema, executableArtifactHash: HashSchema,
  environmentHash: HashSchema, executionId: IdSchema,
  disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  startedAt: TimestampSchema, completedAt: TimestampSchema, occurredAt: TimestampSchema,
});
const CheckWaiverFactBindingV1Schema = StrictObject({
  kind: Type.Literal("waiver"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  checkId: IdSchema, predicateHash: Nullable(HashSchema), issuedBy: Type.Literal("user"), issuedAt: TimestampSchema,
});
const CheckApplicabilityFactBindingV1Schema = StrictObject({
  kind: Type.Literal("check_applicability"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema,
  checkId: IdSchema, predicateHash: Nullable(HashSchema), applicable: Type.Boolean(), observedAt: TimestampSchema,
});
const CheckDispositionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("check_disposition"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, checkId: IdSchema,
  disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "WAIVED", "NOT_APPLICABLE", "BUDGET_EXHAUSTED"]),
  predicateHash: Nullable(HashSchema), authorizationSetHash: HashSchema, evidenceHashes: Type.Array(HashSchema, { minItems: 1 }),
});
const VerificationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("verification"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  ownerEpoch: NonNegativeIntegerSchema, freshnessReceiptHash: HashSchema, effectId: IdSchema, requestHash: HashSchema, requestIdentityHash: HashSchema,
  repositoryId: IdSchema, trainId: IdSchema, integrationAttemptId: IdSchema,
  phase: Type.Enum(["prefix", "final"]), profileId: IdSchema, profileHash: HashSchema,
  executableArtifactHash: HashSchema, argvHash: HashSchema, cwdMode: Type.Literal("detached_proposal_worktree"),
  environmentProfileId: IdSchema, environmentProfileHash: HashSchema, environmentHash: HashSchema,
  timeoutMs: PositiveIntegerSchema, readOnly: Type.Literal(true), noEdit: Type.Literal(true), tree: GitTreeRefV1Schema,
  commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema, objectFormat: Type.Enum(["sha1", "sha256"]),
  executionId: IdSchema, exitCode: Nullable(NonNegativeIntegerSchema), signal: Nullable(Type.String({ minLength: 1, maxLength: 64 })),
  outputHash: HashSchema, stdoutHash: HashSchema, stderrHash: HashSchema, outputBytes: NonNegativeIntegerSchema,
  parser: Type.Literal("strict-json-disposition-v1"), parserDisposition: Type.Enum(["PASS", "FAIL", "BLOCKED"]),
  parsedResultHash: Nullable(HashSchema), startedAt: TimestampSchema, completedAt: TimestampSchema,
  disposition: Type.Enum(["PASS", "FAIL", "BLOCKED"]),
});
const OracleAssertionFactBindingV1Schema = StrictObject({
  kind: Type.Literal("oracle_assertion"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: Type.Literal("F2"),
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, authorizationSetHash: HashSchema,
  oracleId: IdSchema, assertionId: IdSchema, procedureId: IdSchema, environmentProfileId: IdSchema,
  observationMethod: Type.Enum(["static_analysis", "automated_check", "manual_observation", "external_observation", "combined"]),
  requiredEvidenceClass: Type.Enum(["deterministic", "independent", "manual", "external"]),
  disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]), observationHash: HashSchema,
});
const EffectExecutionObservationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("effect_execution_observation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema, freshnessReceiptHash: HashSchema,
  ownerEpoch: NonNegativeIntegerSchema, effectId: IdSchema, requestHash: HashSchema, requestIdentityHash: HashSchema,
  operationKind: Type.Literal("lifecycle_procedure"), executionId: IdSchema, resultIdentityHash: HashSchema,
  result: Type.Record(Type.String(), Type.Unknown()), disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  resultBytes: NonNegativeIntegerSchema, startedAt: TimestampSchema, completedAt: TimestampSchema,
});
const EffectReconciliationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("effect_reconciliation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), effectId: IdSchema, requestHash: HashSchema,
  reconciliation: Type.Enum(["applied_exact", "compensated", "proven_absent", "conflict", "unknown"]),
  executionObservationHash: Type.Optional(HashSchema), resultIdentityHash: Type.Optional(HashSchema), closedAt: TimestampSchema,
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
  disposition: Type.Enum(["absent", "dead", "same_manager"]), priorObservationHash: Nullable(HashSchema), priorOwnershipReceiptHash: Nullable(HashSchema), ownerEpoch: PositiveIntegerSchema, successorSessionId: IdSchema, successorPid: PositiveIntegerSchema,
  successorProcessStartIdentity: Type.String({ minLength: 1, maxLength: 256 }), successorLockIdentity: HashSchema, lineageHash: Nullable(HashSchema), chainHash: HashSchema,
});
const StageEvidenceFactBindingV1Schema = StrictObject({
  kind: Type.Literal("stage_evidence"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema, stage: PlanStageIdSchema,
  stageAttemptId: IdSchema, attemptInputHash: HashSchema, authorizationSetHash: HashSchema,
  procedureHash: HashSchema, environmentProfileHash: HashSchema, checkAggregateHash: HashSchema,
  findingHashes: Type.Array(HashSchema), effectReconciliationHashes: Type.Array(HashSchema),
  candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema),
  producerKind: ProducerKindSchema, producerResultHash: Nullable(HashSchema), disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]),
  environmentObservationHash: Type.Optional(Nullable(HashSchema)), producedAt: TimestampSchema, readOnly: Type.Boolean(),
  freshIndependent: Type.Optional(Type.Boolean()), cleanEnvironment: Type.Optional(Type.Boolean()),
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
  sourceEvidenceProcedureHash: HashSchema, deltaAttestationProcedureHash: HashSchema, environmentProfileHash: HashSchema,
  deltaAttestationExecutionHash: HashSchema, occurredAt: TimestampSchema, evidenceOnlyDelta: Type.Literal(true),
});
const F2InvalidationFactBindingV1Schema = StrictObject({
  kind: Type.Literal("invalidation"), hash: HashSchema, planHash: HashSchema, runId: IdSchema,
  runNonce: Type.String({ minLength: 16, maxLength: 256 }), authorizationSetHash: HashSchema,
  workItemId: IdSchema, stage: Type.Literal("F2"), fromCandidateGeneration: PositiveIntegerSchema,
  fromCandidateHash: HashSchema, toCandidateGeneration: PositiveIntegerSchema, toCandidateHash: HashSchema,
  f3StageAttemptId: IdSchema, priorEvidenceHash: HashSchema,
  reason: Type.Enum(["behavior_bearing", "unknown_impact"]), rerouteStage: Type.Literal("F2"),
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
  prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }),
  prefixEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), finalEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema,
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
  prefixEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }), finalEvidenceHashes: Type.Array(HashSchema, { minItems: 1 }),
  prefixEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), finalEffectReconciliationHashes: Type.Array(HashSchema, { minItems: 1 }), environmentClosureHash: HashSchema,
  sourceBase: GitTreeRefV1Schema, sourceCandidate: GitTreeRefV1Schema, syntheticParentCommit: GitOidSchema,
  sourceToIntegratedLineageHash: HashSchema, landed: GitTreeRefV1Schema, combinedStateVerified: Type.Boolean(), reconciled: Type.Boolean(), acceptingOwnerEpoch: NonNegativeIntegerSchema,
  commonDirIdentityHash: HashSchema, worktreeIdentityHash: HashSchema, gitConfigHash: HashSchema, gitVersionHash: HashSchema, objectFormat: Type.Enum(["sha1", "sha256"]),
  transactionReceiptHash: HashSchema, transactionReceiptFactHash: HashSchema, landingObservationHash: HashSchema, sealedAt: TimestampSchema,
});
const DagRunFactBindingV1Schema = Type.Union([GitTransactionFactBindingV1Schema, GitIntegrationReceiptFactBindingV1Schema, ProcessIdentityObservationBindingV1Schema, CorruptFactEnvelopeBindingV1Schema, StageAttemptInputFactBindingV1Schema, WorkerConfigFactBindingV1Schema, WorkerLaunchObservationFactBindingV1Schema, WorkerResultFactBindingV1Schema, LegacyQuarantinableWorkerResultFactBindingV1Schema, FindingFactBindingV1Schema, FindingResolutionFactBindingV1Schema, FindingCorrectionFactBindingV1Schema, WorkspaceMaterializationFactBindingV1Schema, EnvironmentObservationFactBindingV1Schema, StageEvidenceFactBindingV1Schema, CheckAggregateFactBindingV1Schema, CheckExecutionFactBindingV1Schema, ProcedureExecutionFactBindingV1Schema, CheckWaiverFactBindingV1Schema, CheckApplicabilityFactBindingV1Schema, CheckDispositionFactBindingV1Schema, VerificationFactBindingV1Schema, OracleAssertionFactBindingV1Schema, EffectExecutionObservationFactBindingV1Schema, EffectReconciliationFactBindingV1Schema, QuarantineResolutionFactBindingV1Schema, OwnershipFactBindingV1Schema, CandidateFactBindingV1Schema, EvidenceAdoptionFactBindingV1Schema, F2InvalidationFactBindingV1Schema, IntegrationReadyFactBindingV1Schema, IntegrationFactBindingV1Schema]);
const ProcedureExecutableMappingV1Schema = StrictObject({
  executableArtifactHash: HashSchema,
  argv: Type.Array(Type.String({ minLength: 1, maxLength: 4096 }), { minItems: 1, maxItems: 256 }),
  cwdMode: Type.Enum(["repository_root", "attempt_worktree", "run_root"]),
  environmentProfileId: IdSchema, environmentProfileHash: HashSchema, environmentHash: HashSchema,
  timeoutMs: PositiveIntegerSchema, readOnly: Type.Boolean(), noEdit: Type.Boolean(),
});
const IntegrationValidationProfileMappingV1Schema = StrictObject({
  profileId: IdSchema, executableArtifactHash: HashSchema,
  argv: Type.Array(Type.String({ minLength: 1, maxLength: 4096 }), { minItems: 1, maxItems: 256 }),
  cwdMode: Type.Literal("detached_proposal_worktree"), environmentProfileId: IdSchema,
  environmentProfileHash: HashSchema,
  environment: Type.Record(Type.String({ minLength: 1, maxLength: 256 }), Type.String({ maxLength: 8192 })),
  environmentHash: HashSchema, timeoutMs: PositiveIntegerSchema, readOnly: Type.Literal(true), noEdit: Type.Literal(true),
});
const ProcedureCatalogBindingV1Schema = StrictObject({
  hash: HashSchema, procedureId: IdSchema, purpose: Type.Enum(["lifecycle", "evidence_only_delta_attestation"]), stages: Type.Array(PlanStageIdSchema, { minItems: 1 }),
  producerKinds: Type.Array(ProducerKindSchema, { minItems: 1 }), readOnly: Type.Boolean(), environmentProfileHash: HashSchema,
  executable: ProcedureExecutableMappingV1Schema,
});
const CheckResultBindingV1Schema = StrictObject({
  checkId: IdSchema, disposition: Type.Enum(["PASS", "FAIL", "BLOCKED", "WAIVED", "NOT_APPLICABLE", "BUDGET_EXHAUSTED"]), executionEvidenceHash: Type.Optional(Nullable(HashSchema)), applicabilityEvidenceHashes: Type.Array(HashSchema),
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
  dispatchProtocolVersion: Type.Optional(Type.Literal(1)),
  readyPacketHash: Type.Optional(HashSchema),
  normalizedDirective: Type.Optional(Type.Union([Type.String({ maxLength: 2_000 }), Type.Null()])),
  directiveHash: Type.Optional(HashSchema),
  promptHash: Type.Optional(HashSchema),
  dispatchConfigRequestHash: Type.Optional(HashSchema),
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
  processDisposition: Type.Optional(ProcessDispositionSchema),
  retrySafe: Type.Optional(Type.Boolean()),
});
const EvidenceIndexV1Schema = StrictObject({
  stageAttemptInputs: IdMap(HashRefV1Schema),
  workerResults: HashMap(HashRefV1Schema),
  candidates: HashMap(HashRefV1Schema),
  stageEvidence: HashMap(HashRefV1Schema),
  checkAggregates: HashMap(HashRefV1Schema),
  checkExecutions: Type.Optional(HashMap(HashRefV1Schema)),
  procedureExecutions: Type.Optional(HashMap(HashRefV1Schema)),
  findingCorrections: Type.Optional(HashMap(HashRefV1Schema)),
  checkApplicabilities: Type.Optional(HashMap(HashRefV1Schema)),
  environmentObservations: Type.Optional(HashMap(HashRefV1Schema)),
  workspaceMaterializations: Type.Optional(HashMap(HashRefV1Schema)),
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
  stage: PlanStageIdSchema,
  stageAttemptId: IdSchema,
  attemptInputHash: HashSchema,
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
const LifecycleProcedureEffectRequestV1Schema = StrictObject({
  requestKind: Type.Literal("lifecycle_procedure_v1"),
  planHash: HashSchema, runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }), workItemId: IdSchema,
  stage: PlanStageIdSchema, stageAttemptId: IdSchema, attemptInputHash: HashSchema, producerKind: ProducerKindSchema,
  procedureHash: HashSchema, procedureCatalogHash: HashSchema, executableMappingHash: HashSchema,
  executableArtifactHash: HashSchema, argv: Type.Array(Type.String({ minLength: 1, maxLength: 4096 }), { minItems: 1, maxItems: 256 }), argvHash: HashSchema,
  argvEnvironmentHash: HashSchema, cwdMode: Type.Enum(["repository_root", "attempt_worktree", "run_root"]),
  environmentProfileId: IdSchema, environmentProfileHash: HashSchema, environmentHash: HashSchema,
  timeoutMs: PositiveIntegerSchema, readOnly: Type.Boolean(), noEdit: Type.Boolean(),
  candidateGeneration: NonNegativeIntegerSchema, candidateHash: Nullable(HashSchema), candidateTree: Nullable(GitTreeRefV1Schema), workerResultHash: Nullable(HashSchema),
  authorizationSetHash: HashSchema, freshnessReceiptHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema,
});
const IntegrationValidationEffectRequestV1Schema = StrictObject({
  requestKind: Type.Literal("integration_validation_v1"),
  planHash: HashSchema, runId: IdSchema, runNonce: Type.String({ minLength: 16, maxLength: 256 }), repositoryId: IdSchema, trainId: IdSchema,
  integrationAttemptId: IdSchema, phase: Type.Enum(["prefix", "final"]), profileId: IdSchema, profileHash: HashSchema,
  executableArtifactHash: HashSchema, argv: Type.Array(Type.String({ minLength: 1, maxLength: 4096 }), { minItems: 1, maxItems: 256 }), argvHash: HashSchema,
  argvEnvironmentHash: HashSchema, cwdMode: Type.Literal("detached_proposal_worktree"), environmentProfileId: IdSchema,
  environmentProfileHash: HashSchema, environmentHash: HashSchema, timeoutMs: PositiveIntegerSchema, readOnly: Type.Literal(true), noEdit: Type.Literal(true),
  tree: GitTreeRefV1Schema, commonDirIdentityHash: HashSchema, repositoryWorktreeIdentityHash: HashSchema, gitConfigHash: HashSchema, gitVersionHash: HashSchema,
  objectFormat: Type.Enum(["sha1", "sha256"]), candidateGeneration: NonNegativeIntegerSchema,
  authorizationSetHash: HashSchema, freshnessReceiptHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema,
});

export const EffectProjectionV1Schema = StrictObject({
  effectId: IdSchema,
  kind: Type.Enum(["put_immutable_fact", "launch_worker", "cancel_worker", "materialize_workspace", "cleanup_worktree", "run_procedure", "reconcile_external_effect", "compose_candidate", "verify_prefix", "land_target"]),
  subject: SubjectRefV1Schema,
  boundStageAttemptId: Type.Optional(Nullable(IdSchema)),
  boundIntegrationAttemptId: Type.Optional(Nullable(IdSchema)),
  boundWorkerResultHash: Type.Optional(Nullable(HashSchema)),
  executionRequest: Type.Optional(Type.Union([LifecycleProcedureEffectRequestV1Schema, IntegrationValidationEffectRequestV1Schema])),
  executionObservationHash: Type.Optional(Nullable(HashSchema)),
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
  reconciliationRevision: Type.Optional(Nullable(PositiveIntegerSchema)),
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
  prefixEffectReconciliationHashes: Type.Array(HashSchema),
  finalEffectReconciliationHashes: Type.Array(HashSchema),
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
const ReconciliationBindingV1Schema = StrictObject({ effectId: IdSchema, observationHash: HashSchema });
const LandingObservationBindingV1Schema = StrictObject({
  integrationAttemptId: IdSchema, effectId: IdSchema, observationHash: HashSchema, requestHash: HashSchema,
  authorizationSetHash: HashSchema, ownerEpoch: NonNegativeIntegerSchema, dispatchCount: PositiveIntegerSchema,
  dispatchRevision: PositiveIntegerSchema, dispatchAt: TimestampSchema,
});
const IdempotencySlotV1Schema = StrictObject({
  slotId: HashSchema, inputType: IdSchema, commandId: IdSchema, idempotencyKey: Type.String({ minLength: 1, maxLength: 256 }),
  payloadHash: HashSchema, inputHash: HashSchema, appliedRevision: PositiveIntegerSchema,
  reconciliationBindings: Type.Optional(Type.Array(ReconciliationBindingV1Schema, { minItems: 1 })),
  reconciliationCancellationId: Type.Optional(IdSchema),
  landingObservationBinding: Type.Optional(LandingObservationBindingV1Schema),
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

export function dagRunNeedsReplanV1(state: DagRunStateV1): boolean {
  return Object.values(state.findingClosures).some((finding) =>
    finding.severity === "blocking"
    && finding.materiality === "plan_affecting"
    && ["open", "successor_plan_required"].includes(finding.state));
}

export function ownershipChainHashV1(fact: Pick<OwnershipFactBindingV1, "runId" | "runNonce" | "ownerEpoch" | "priorOwnershipReceiptHash" | "successorSessionId" | "successorPid" | "successorProcessStartIdentity" | "successorLockIdentity">, priorChainHash: string | null): string {
  return canonicalHash({
    kind: "ownership_chain", runId: fact.runId, runNonce: fact.runNonce, ownerEpoch: fact.ownerEpoch,
    priorOwnershipReceiptHash: fact.priorOwnershipReceiptHash, priorChainHash,
    successorSessionId: fact.successorSessionId, successorPid: fact.successorPid,
    successorProcessStartIdentity: fact.successorProcessStartIdentity, successorLockIdentity: fact.successorLockIdentity,
  });
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
    for (const [profileHash, profile] of Object.entries(context.integrationValidationProfiles ?? {})) {
      const profileIssues = schemaIssues(IntegrationValidationProfileMappingV1Schema, profile);
      for (const issue of profileIssues) issues.push({ path: `/integrationValidationProfiles/${profileHash}${issue.path === "/" ? "" : issue.path}`, message: issue.message });
      pushIssue(issues, `/integrationValidationProfiles/${profileHash}`, profileHash === canonicalHash(profile), "validation profile key must equal exact canonical mapping content hash");
      pushIssue(issues, `/integrationValidationProfiles/${profileHash}/environmentHash`, profile.environmentHash === canonicalHash(profile.environment), "environment hash must bind the exact closed environment record");
      pushIssue(issues, `/integrationValidationProfiles/${profileHash}/environmentProfileHash`, profile.environmentProfileHash === canonicalHash({ profileId: profile.environmentProfileId, environment: profile.environment }), "environment profile hash must bind exact profile ID and closed environment");
      pushIssue(issues, `/integrationValidationProfiles/${profileHash}/argv/0`, Boolean(profile.argv[0]?.startsWith("/")), "validation executable argv[0] must be absolute and shell-free");
    }
    pushIssue(issues, "/scheduler/normalizedIndexHash", /^sha256:[0-9a-f]{64}$/.test(context.normalizedSchedulerIndexHash ?? ""), "validation context requires a canonical scheduler-index hash");
    for (const issue of authorizationIssues) issues.push({ path: `/authorization${issue.path === "/" ? "" : issue.path}`, message: issue.message });
    for (const issue of catalogIssues) issues.push({ path: `/catalog${issue.path === "/" ? "" : issue.path}`, message: issue.message });
    let factShapeValid = true;
    for (const [hash, fact] of Object.entries(context.facts ?? {})) {
      const factIssues = schemaIssues(DagRunFactBindingV1Schema, fact);
      validateTimestampFields(fact, factIssues);
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
  const needsReplan = dagRunNeedsReplanV1(state);
  if (state.desired.run === "needs_replan") pushIssue(issues, "/desired/run", needsReplan, "needs_replan intent requires an unresolved blocking plan-affecting finding");
  if (state.current.run === "needs_replan") pushIssue(issues, "/current/run", needsReplan, "needs_replan current state requires an unresolved blocking plan-affecting finding");
  if (state.desired.run === "needs_replan") pushIssue(issues, "/current/run", state.current.run === "needs_replan", "needs_replan intent must be scheduler-visible in current state");
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
  const ownerIdentityComplete = state.owner.ownerTokenHash !== null && state.owner.processStartIdentity !== null && state.owner.lockIdentity !== null && state.owner.attachedAt !== null && state.owner.pid > 0;
  pushIssue(issues, "/owner", ownerAttached === ownerIdentityComplete, "attached owner requires complete token, process, lock, and time identity");
  pushIssue(issues, "/owner/ownerEpoch", state.owner.ownerEpoch === 0 ? !ownerAttached && state.owner.ownershipReceipt === null : state.owner.ownershipReceipt !== null, "epoch zero is the sole receipt-free genesis; every later attached or released owner epoch retains its ownership receipt");
  if (state.owner.ownershipReceipt) {
    const ownership = context.facts[state.owner.ownershipReceipt] as any;
    pushIssue(issues, "/owner/ownershipReceipt", ownership?.kind === "ownership" && ownership.hash === state.owner.ownershipReceipt && ownership.hash === hashWithoutField(ownership as Record<string, unknown>, "hash") && ownership.runId === state.runId && ownership.runNonce === state.runNonce && ownership.ownerEpoch === state.owner.ownerEpoch && (!ownerAttached || (ownership.successorSessionId === state.owner.sessionId && ownership.successorPid === state.owner.pid && ownership.successorProcessStartIdentity === state.owner.processStartIdentity && ownership.successorLockIdentity === state.owner.lockIdentity)), "must resolve the exact canonical current-epoch ownership receipt");
    validateOwnershipReceiptChain(state, context, issues);
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
    const cancellationReleasePending = reservation.state === "release_requested" && Object.values(state.cancellations).some((cancellation) => cancellation.state !== "closed" && cancellation.fencedGenerations[reservation.workItemId] === item?.candidateGeneration && reservation.candidateGeneration < (item?.candidateGeneration ?? 0));
    const currentProducerAttempt = item ? state.stageAttempts[item.stages[reservation.stage].currentAttemptId ?? ""] : undefined;
    const preciseOutputBoundary = !reservationTerminal && ["F1", "F3"].includes(reservation.stage) && currentProducerAttempt?.reservedOutputGeneration === item?.candidateGeneration && currentProducerAttempt.inputGeneration === reservation.candidateGeneration && currentProducerAttempt.stageAttemptId === item?.candidate?.producedByStageAttemptId;
    pushIssue(issues, `/scheduler/reservations/${reservationId}/candidateGeneration`, reservationTerminal ? reservation.candidateGeneration <= (item?.candidateGeneration ?? -1) : cancellationReleasePending || item?.candidateGeneration === reservation.candidateGeneration || preciseOutputBoundary, "active reservation must bind current generation, an exact cancellation release request, or the precise current producer output boundary; terminal reservation may retain an older fenced generation");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/ownerEpoch`, reservationTerminal ? reservation.ownerEpoch <= state.owner.ownerEpoch : reservation.ownerEpoch === state.owner.ownerEpoch, "active reservation must bind current owner epoch");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/authorizationSetHash`, reservationTerminal || (reservation.authorizationSetHash === state.identity.authorizationSet.hash && item?.authorizedStages.includes(reservation.stage)), "active reservation must bind current authority for the reserved stage");
    pushIssue(issues, `/scheduler/reservations/${reservationId}/repositoryId`, reservation.repositoryId === item?.writeRepositoryId, "must bind the work item's write repository");
    const expectedOperation: Record<string, string[]> = { F0: ["conductor"], F1: ["implementation"], F2: ["evaluation"], F3: ["codification"], F4: ["verification"], F5: ["review"], F6: ["hardening"], F7: ["verification"], F8: reservationTerminal || cancellationReleasePending ? ["conductor", "integration"] : item?.current === "integration_ready" || item?.current === "integrating" ? ["integration"] : ["conductor"] };
    pushIssue(issues, `/scheduler/reservations/${reservationId}/operationKind`, expectedOperation[reservation.stage].includes(reservation.operationKind), "must use the fixed F0-F8/integration operation class");
    const expectedMutexes = context.plan.constraints.semanticMutexes.filter((mutex) => mutex.members.some((member) => member.workItemId === reservation.workItemId && member.phases.includes(reservation.stage))).map(({ mutexGroupId }) => mutexGroupId).sort();
    pushIssue(issues, `/scheduler/reservations/${reservationId}/mutexGroupIds`, expectedMutexes.every((id) => reservation.mutexGroupIds.includes(id)), "must reserve every applicable plan semantic mutex");
    for (const demand of planItem?.resourceDemands.filter(({ phases }) => phases.includes(reservation.stage)) ?? []) {
      const resource = context.plan.constraints.resourceClasses.find(({ resourceClassId }) => resourceClassId === demand.resourceClassId);
      const units = reservation.resourceUnits[demand.resourceClassId];
      pushIssue(issues, `/scheduler/reservations/${reservationId}/resourceUnits/${demand.resourceClassId}`, units >= demand.units && units <= (resource?.semanticMaximum ?? -1), "must cover declared demand without exceeding the semantic maximum");
    }
    reservation.leaseIds.forEach((leaseId) => pushIssue(issues, `/scheduler/reservations/${reservationId}/leaseIds`, Boolean(state.leases[leaseId]), "references an unknown lease"));
    if (!reservationTerminal && reservation.stage === "F8" && reservation.operationKind === "integration") {
      const train = state.integrationTrains[reservation.repositoryId];
      const integrationAttemptId = train?.activeIntegrationAttemptId ?? null;
      const integrationAttempt = integrationAttemptId ? state.integrationAttempts[integrationAttemptId] : undefined;
      const entry = integrationAttempt ? train?.entries[integrationAttempt.entryId] : undefined;
      if (integrationAttempt && entry?.workItemId === reservation.workItemId) {
        pushIssue(issues, `/scheduler/reservations/${reservationId}/leaseIds`, reservation.leaseIds.every((leaseId) => state.leases[leaseId]?.holderIntegrationAttemptId === integrationAttemptId && state.leases[leaseId]?.holderStageAttemptId === null), "active integration reservation leases must all bind the exact active integration attempt");
      } else pushIssue(issues, `/scheduler/reservations/${reservationId}/leaseIds`, reservation.leaseIds.every((leaseId) => state.leases[leaseId]?.holderIntegrationAttemptId === null), "pre-integration F8 reservation leases cannot claim integration authority");
    }
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
    const activeLeases = Object.values(state.leases).filter((lease) => lease.kind === "operational" && lease.subject.kind === "resource" && lease.subject.id === namespace && ["active", "release_requested", "expired"].includes(lease.state));
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
      pushIssue(issues, `${path}/current`, item.desired === "run" && ["running", "paused", "needs_replan"].includes(state.desired.run), "ready work requires runnable item intent; a global pause or replan hold may retain readiness while blocking dispatch");
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
      for (const invalidationId of projection.invalidationIds) {
        const reference = state.evidenceIndex.invalidations[invalidationId]; const invalidation = reference ? context.facts[reference.hash] as any : undefined;
        pushIssue(issues, `${path}/stages/${stage}/invalidationIds`, stage === "F2" && reference?.id === invalidationId && invalidation?.kind === "invalidation" && invalidation.hash === reference.hash && invalidation.hash === hashWithoutField(invalidation, "hash") && invalidation.planHash === state.identity.planHash && invalidation.runId === state.runId && invalidation.runNonce === state.runNonce && invalidation.authorizationSetHash === state.identity.authorizationSet.hash && invalidation.workItemId === item.workItemId && invalidation.stage === "F2" && invalidation.rerouteStage === "F2" && ["behavior_bearing", "unknown_impact"].includes(invalidation.reason), "F2 invalidation must resolve exact immutable plan/run/item/authority reroute evidence");
      }
      if (projection.state === "passed") validatePassedStage(state, context, item, stage, projection, path, issues);
      if (["failed", "blocked", "budget_exhausted"].includes(projection.state)) validateNonPassStage(state, context, item, stage, projection, path, issues);
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
        const preciseUnsealedOutput = producingAttempt && item.stages[producingAttempt.stage].currentAttemptId === producingAttempt.stageAttemptId && item.stages[producingAttempt.stage].state === "active" && ["result_observed", "evidence_pending"].includes(producingAttempt.state);
        const exactInvalidatedReroute = producingAttempt?.stage === "F3" && producingAttempt.state === "failed" && producingAttempt.terminalAt !== null && Object.values(state.evidenceIndex.invalidations).some((ref) => { const invalidation = context.facts[ref.hash] as any; return invalidation?.kind === "invalidation" && invalidation.toCandidateHash === candidateFact.hash && invalidation.toCandidateGeneration === candidateFact.generation && invalidation.f3StageAttemptId === producingAttempt.stageAttemptId && invalidation.rerouteStage === "F2"; });
        pushIssue(issues, `${path}/candidate/producedByStageAttemptId`, Boolean(producingAttempt && ["F1", "F3"].includes(producingAttempt.stage) && producingAttempt.workItemId === workItemId && producingAttempt.reservedOutputGeneration === candidateFact.generation && (producingAttempt.state === "sealed" || preciseUnsealedOutput || exactInvalidatedReroute)), "candidate must be produced by an exact sealed F1/F3 output, precise current pre-seal output, or exact invalidated F3-to-F2 reroute");
        pushIssue(issues, `${path}/candidate/git`, canonicalHash(candidateFact.base) === canonicalHash(item.candidate.base) && canonicalHash(candidateFact.git) === canonicalHash(item.candidate.git), "candidate fact must match the exact source base, commit, and tree");
        const producerResult = producingAttempt?.workerResult ? context.facts[producingAttempt.workerResult.hash] : undefined;
        pushIssue(issues, `${path}/candidate/candidateHash`, producerResult?.kind === "worker_result" && producerResult.terminalStatus === "succeeded" && producerResult.outputRepositoryId === item.writeRepositoryId && producerResult.outputSourceBase !== null && canonicalHash(producerResult.outputSourceBase) === canonicalHash(candidateFact.base) && producerResult.outputCommit === candidateFact.git.commit && producerResult.outputTree === candidateFact.git.tree && producerResult.outputObjectFormat === (candidateFact.git.commit.length === 40 ? "sha1" : "sha256") && producerResult.candidateObservedAt !== null, "F1/F3 candidate must exactly join its successful worker result Git output identity");
      }
    } else pushIssue(issues, `${path}/candidateGeneration`, item.candidateGeneration === 0 || ["blocked", "cancelled", "superseded"].includes(item.current) || integrationConflictFence !== null, "candidate-less positive generation is allowed only after an explicit fence");
    if (["integration_ready", "integrating", "complete"].includes(item.current)) {
      const readyRef = state.evidenceIndex.integrationReady[workItemId];
      pushIssue(issues, `${path}/integrationReadyReceipt`, item.integrationReadyReceipt !== null && readyRef?.hash === item.integrationReadyReceipt && readyRef?.kind === "integration_ready", "requires the exact indexed integration-ready receipt");
      for (const stage of PLAN_STAGE_IDS) pushIssue(issues, `${path}/stages/${stage}/state`, item.stages[stage].state === "passed", "must be passed before integration readiness");
      pushIssue(issues, `${path}/candidate`, item.candidate !== null, "integration readiness requires a current candidate");
      const unresolvedMaterialFindings = Object.values(state.findingClosures).filter((finding) => finding.workItemId === workItemId && ["open", "successor_plan_required"].includes(finding.state) && (finding.severity === "blocking" || finding.materiality === "plan_affecting"));
      const heldPlanFindings = unresolvedMaterialFindings.length > 0 && unresolvedMaterialFindings.every((finding) => finding.severity === "blocking" && finding.materiality === "plan_affecting") && needsReplan;
      pushIssue(issues, `${path}/openFindingIds`, unresolvedMaterialFindings.length === 0 || heldPlanFindings, "integration readiness may retain only blocking plan-affecting findings while the whole run is held for replanning");
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
          pushIssue(issues, `${path}/integrationReceipt`, canonicalHash(integrationFact.expectedPrefix) === canonicalHash(integrationAttempt.expectedPrefix) && canonicalHash(integrationFact.expectedTarget) === canonicalHash(integrationAttempt.expectedTarget) && sameStrings([...integrationFact.prefixEvidenceHashes], [...integrationAttempt.prefixEvidenceHashes]) && sameStrings([...integrationFact.finalEvidenceHashes], [...integrationAttempt.finalEvidenceHashes]) && sameStrings([...(integrationFact.prefixEffectReconciliationHashes ?? [])], [...integrationAttempt.prefixEffectReconciliationHashes]) && sameStrings([...(integrationFact.finalEffectReconciliationHashes ?? [])], [...integrationAttempt.finalEffectReconciliationHashes]) && integrationFact.environmentClosureHash === integrationAttempt.environmentClosureHash, "integration receipt must bind exact source prefix, target, prefix checks, and final checks");
          pushIssue(issues, `${path}/integrationReceipt`, canonicalHash(integrationFact.sourceBase) === canonicalHash(integrationAttempt.sourceBase) && canonicalHash(integrationFact.sourceBase) === canonicalHash(item.candidate.base) && canonicalHash(integrationFact.sourceCandidate) === canonicalHash(integrationAttempt.sourceCandidate) && canonicalHash(integrationFact.sourceCandidate) === canonicalHash(item.candidate.git), "integration must bind exact candidate source base and current source candidate");
          pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt.syntheticParentCommit === integrationAttempt.expectedPrefix.commit && integrationFact.syntheticParentCommit === integrationAttempt.syntheticParentCommit, "synthetic commit must have exactly the accepted prefix as its one parent");
          pushIssue(issues, `${path}/integrationReceipt`, integrationAttempt.sourceToIntegratedLineageHash === integrationFact.sourceToIntegratedLineageHash && integrationAttempt.composedTree !== null && canonicalHash(integrationAttempt.composedTree) === canonicalHash(integrationFact.landed), "receipt must bind exact source-to-integrated lineage and composed landed tree");
          const transactionFact = context.facts[integrationFact.transactionReceiptFactHash]; const receipt = transactionFact?.kind === "git_integration_receipt" ? transactionFact.receipt as any : null; const bindingFact = context.facts[integrationAttempt.repositoryBindingFactHash] as any;
          pushIssue(issues, `${path}/integrationReceipt`, transactionFact?.kind === "git_integration_receipt" && transactionFact.hash === integrationFact.transactionReceiptFactHash && transactionFact.hash === hashWithoutField(transactionFact as unknown as Record<string, unknown>, "hash") && transactionFact.transactionReceiptHash === integrationFact.transactionReceiptHash && transactionFact.planHash === state.identity.planHash && transactionFact.runId === state.runId && transactionFact.runNonce === state.runNonce && transactionFact.authorizationSetHash === state.identity.authorizationSet.hash && transactionFact.repositoryId === item.writeRepositoryId && transactionFact.integrationAttemptId === integrationAttempt.integrationAttemptId, "integration must resolve the exact immutable transaction-receipt binding");
          pushIssue(issues, `${path}/integrationReceipt`, receipt?.receiptHash === integrationFact.transactionReceiptHash && receipt?.receiptHash === hashWithoutField(receipt as Record<string, unknown>, "receiptHash") && receipt?.transactionId === integrationAttempt.integrationAttemptId && receipt?.ownerEpoch === integrationFact.acceptingOwnerEpoch && receipt?.commonDirIdentityHash === bindingFact?.commonDirIdentityHash && receipt?.worktreeIdentityHash === bindingFact?.worktreeIdentityHash && receipt?.configHash === bindingFact?.gitConfigHash && canonicalHash(receipt?.gitVersion) === bindingFact?.gitVersionHash && receipt?.objectFormat === bindingFact?.objectFormat && sameStrings([...(receipt?.prefixEffectReconciliationHashes ?? [])], [...integrationAttempt.prefixEffectReconciliationHashes]) && sameStrings([...(receipt?.finalEffectReconciliationHashes ?? [])], [...integrationAttempt.finalEffectReconciliationHashes]) && canonicalHash(receipt?.composed) === canonicalHash(integrationFact.landed) && receipt?.landing?.targetObservationHash === (context.facts[integrationFact.landingObservationHash] as any)?.detailsHash && canonicalHash(Object.keys(receipt?.privateRefs ?? {}).sort()) === canonicalHash(["baseline", "candidate", "composed", "prefix", "proposal"]) && canonicalHash(Object.values(receipt?.privateRefs ?? {}).sort()) === canonicalHash(integrationAttempt.privateRefFactHashes.map((hash) => (context.facts[hash] as any)?.targetRef).sort()), "transaction receipt content must bind exact owner, repository environment, composed target, and landing observation");
        }
        pushIssue(issues, `${path}/integrationReceipt`, integrationFact.candidateHash === item.candidate.candidateHash && integrationFact.combinedStateVerified && integrationFact.reconciled, "integration receipt must prove the current candidate, combined state, and reconciliation");
        const itemTrain = state.integrationTrains[item.writeRepositoryId];
        const itemEntry = item.integrationEntryId ? itemTrain?.entries[item.integrationEntryId] : undefined;
        const isCurrentAcceptedPrefix = itemEntry?.ordinal === (itemTrain?.acceptedPrefixOrdinal ?? 0) - 1;
        pushIssue(issues, `${path}/integrationReceipt`, !isCurrentAcceptedPrefix || canonicalHash(state.repositories[item.writeRepositoryId].observedTarget) === canonicalHash(integrationFact.landed), "the current accepted-prefix receipt must equal the observed target; earlier exact train receipts remain historical");
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
    if (attempt.producerKind === "owned_worker") {
      const launch = attempt.launchIntentId ? state.launchIntents[attempt.launchIntentId] : undefined;
      const exactPreBindBoundary = !state.workerBindings[attemptId] && ["preparing", "dispatchable", "launching"].includes(attempt.state) && Boolean(launch && ["reserved", "dispatchable", "dispatching"].includes(launch.state));
      const exactPreBindCancellation = !state.workerBindings[attemptId] && attempt.workerResult === null && ["cancelling", "cancelled"].includes(attempt.state) && Boolean(launch && (attempt.state === "cancelling" ? launch.state === "cancel_requested" : ["not_started", "closed"].includes(launch.state)));
      pushIssue(issues, `${path}/producerKind`, attempt.launchIntentId !== null && (Boolean(state.workerBindings[attemptId]) || exactPreBindBoundary || exactPreBindCancellation), "owned-worker attempt requires an exact binding, precise pre-bind launch boundary, or proven-never-started cancellation");
    }
    else pushIssue(issues, `${path}/producerKind`, attempt.launchIntentId === null && !state.workerBindings[attemptId] && attempt.workerResult === null, "conductor/deterministic attempt cannot use generic worker state");
    if (attempt.workerResult) pushIssue(issues, `${path}/workerResult`, state.evidenceIndex.workerResults[attempt.workerResult.hash]?.hash === attempt.workerResult.hash, "must be indexed by exact result hash");
    if (attempt.evidence) pushIssue(issues, `${path}/evidence`, state.evidenceIndex.stageEvidence[attempt.evidence.hash]?.hash === attempt.evidence.hash, "must be indexed by exact evidence hash");
  }

  for (const [launchId, launch] of Object.entries(state.launchIntents)) {
    pushIssue(issues, `/launchIntents/${launchId}/stageAttemptId`, state.stageAttempts[launch.stageAttemptId]?.launchIntentId === launchId, "must backreference the launch intent");
    const launchEffect = state.effects[launch.effectId];
    pushIssue(issues, `/launchIntents/${launchId}/effectId`, launchEffect?.kind === "launch_worker", "must reference a launch_worker effect intent");
    pushIssue(issues, `/launchIntents/${launchId}/dispatchCount`, launch.dispatchCount === (launchEffect?.dispatchCount ?? -1) && launch.lastDispatchAt === (launchEffect?.lastDispatchAt ?? null), "must mirror exact durable launch-effect dispatch authority");
    if (launch.dispatchProtocolVersion === 1) {
      const dispatchHashes = [launch.readyPacketHash, launch.directiveHash, launch.promptHash, launch.dispatchConfigRequestHash];
      const directiveBound = Object.prototype.hasOwnProperty.call(launch, "normalizedDirective") && (launch.normalizedDirective === null || typeof launch.normalizedDirective === "string");
      const envelopeAbsent = dispatchHashes.every((value) => value === undefined) && !Object.prototype.hasOwnProperty.call(launch, "normalizedDirective");
      const envelopeBound = dispatchHashes.every((value) => typeof value === "string") && directiveBound;
      pushIssue(issues, `/launchIntents/${launchId}/dispatchProtocolVersion`, envelopeAbsent || envelopeBound, "modern dispatch envelope identity must be wholly absent before dispatch or wholly bound afterwards");
      if (envelopeBound) {
        const normalized = launch.normalizedDirective;
        const directiveExact = normalized === null || typeof normalized === "string" && normalized.length <= 2_000 && Buffer.byteLength(normalized, "utf8") <= 8_192 && normalized === normalized.normalize("NFC").trim() && ![...normalized].some((character) => { const code = character.codePointAt(0)!; return (code < 32 && code !== 9 && code !== 10) || code === 127; });
        pushIssue(issues, `/launchIntents/${launchId}/normalizedDirective`, directiveExact && launch.directiveHash === canonicalHash({ schemaVersion: 1, directive: normalized }), "bound tactical directive must be exact normalized bounded data matching its durable hash");
      }
      pushIssue(issues, `/launchIntents/${launchId}/state`, launch.dispatchCount === 0 ? ["dispatchable", "not_started", "closed"].includes(launch.state) && envelopeAbsent : envelopeBound, "modern launch is dispatchable/proven-never-started before authority and identity-bound after dispatch");
    } else pushIssue(issues, `/launchIntents/${launchId}/dispatchProtocolVersion`, launch.readyPacketHash === undefined && launch.normalizedDirective === undefined && launch.directiveHash === undefined && launch.promptHash === undefined && launch.dispatchConfigRequestHash === undefined, "legacy launch intents cannot carry a modern dispatch envelope");
    if (["bound", "closed"].includes(launch.state)) pushIssue(issues, `/launchIntents/${launchId}/state`, launch.dispatchCount > 0 && launchEffect?.observationHash !== null && launchEffect?.reconciliation === "applied_exact", "bound/closed launch requires positive dispatch and immutable exact launch observation");
    if (launch.state === "not_started") pushIssue(issues, `/launchIntents/${launchId}/state`, launch.dispatchCount === 0 && ["cancelled", "reconciled"].includes(launchEffect?.state ?? "") && launchEffect?.reconciliation === "proven_absent" && !state.workerBindings[launch.stageAttemptId], "not-started launch requires exact never-dispatched proven-absent closure without a worker binding");
    pushIssue(issues, `/launchIntents/${launchId}/cwdRepositoryId`, Boolean(state.repositories[launch.cwdRepositoryId]), "references an unknown repository");
  }
  const workerIdentityOwners = new Map<string, string>();
  for (const [attemptId, binding] of Object.entries(state.workerBindings)) {
    pushIssue(issues, `/workerBindings/${attemptId}/stageAttemptId`, binding.stageAttemptId === attemptId && Boolean(state.stageAttempts[attemptId]), "must match an existing stage attempt");
    const genericAttemptIdentity = canonicalHash({ workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash });
    const priorIdentityOwner = workerIdentityOwners.get(genericAttemptIdentity);
    pushIssue(issues, `/workerBindings/${attemptId}`, priorIdentityOwner === undefined, `generic worker/config/attempt identity is already owned by ${priorIdentityOwner ?? "another lifecycle attempt"}`);
    if (priorIdentityOwner === undefined) workerIdentityOwners.set(genericAttemptIdentity, attemptId);
    const launch = state.launchIntents[binding.launchIntentId];
    pushIssue(issues, `/workerBindings/${attemptId}/launchIntentId`, launch?.stageAttemptId === attemptId && launch?.workerId === binding.workerId, "must match the exact launch intent");
    const configFact = context.facts[binding.configRef.hash] as any;
    const config = configFact?.kind === "worker_config" ? configFact.config : null;
    pushIssue(issues, `/workerBindings/${attemptId}/configRef`, binding.configRef.kind === "worker_config" && configFact?.hash === binding.configRef.hash && configFact?.hash === hashWithoutField(configFact as Record<string, unknown>, "hash") && configFact?.configHash === binding.configHash && canonicalHash(config) === binding.configHash, "must resolve a readable canonical immutable worker config artifact");
    pushIssue(issues, `/workerBindings/${attemptId}/configRef`, Boolean(config && config.storageId === binding.workerStorageId && config.ownerSessionId === binding.launchOwnerSessionId && config.workerId === binding.workerId && config.attemptNumber === binding.attemptNumber && config.attemptNonce === binding.attemptNonce && config.launchKey === launch?.launchKey && config.requestHash === (launch?.dispatchConfigRequestHash ?? launch?.configRequestHash) && (launch?.dispatchProtocolVersion !== 1 || canonicalHash(config.task) === launch.promptHash && canonicalHash({ protocolVersion: 1, launchKey: launch.launchKey, workerId: launch.workerId, taskPacketHash: launch.taskPacketHash, directiveHash: launch.directiveHash, promptHash: launch.promptHash }) === launch.dispatchConfigRequestHash)), "worker config must bind the exact launch and generic worker attempt identity");
    const attemptForIndependence = state.stageAttempts[attemptId];
    if (attemptForIndependence && ["F2", "F5"].includes(attemptForIndependence.stage)) {
      const predecessorContexts = Object.entries(state.workerBindings).filter(([otherId]) => {
        const other = state.stageAttempts[otherId];
        return otherId !== attemptId && other?.workItemId === attemptForIndependence.workItemId && ["F1", "F2", "F5"].includes(other.stage) && (PLAN_STAGE_IDS.indexOf(other.stage) < PLAN_STAGE_IDS.indexOf(attemptForIndependence.stage) || (other.stage === attemptForIndependence.stage && other.ordinal < attemptForIndependence.ordinal));
      }).map(([otherId, otherBinding]) => ({ attempt: state.stageAttempts[otherId], launch: state.launchIntents[otherBinding.launchIntentId], binding: otherBinding }));
      const currentLaunch = state.launchIntents[binding.launchIntentId];
      pushIssue(issues, `/workerBindings/${attemptId}`, predecessorContexts.every(({ attempt: otherAttempt, launch: otherLaunch, binding: other }) => other.workerId !== binding.workerId && other.attemptNonce !== binding.attemptNonce && other.configHash !== binding.configHash && other.launchIntentId !== binding.launchIntentId && otherLaunch?.launchKey !== currentLaunch?.launchKey && otherAttempt?.stageAttemptId !== attemptForIndependence.stageAttemptId), `${attemptForIndependence.stage} requires a fresh exact worker/config/nonce and launch/intent/attempt identity distinct from predecessor implementation/evaluation/review contexts; manager storage may be shared`);
    }
    if (binding.resultHash) {
      const attempt = state.stageAttempts[attemptId];
      pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, binding.completionId !== null && attempt?.workerResult?.hash === binding.resultHash && state.evidenceIndex.workerResults[binding.resultHash]?.hash === binding.resultHash, "must match exact attempt, completion, and indexed worker result");
      const resultFact = context.facts[binding.resultHash];
      pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact?.kind === "worker_result", "requires a validated immutable generic worker-result binding");
      if (resultFact?.kind === "worker_result") {
        pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact.hash === binding.resultHash && resultFact.planHash === state.identity.planHash && resultFact.runId === state.runId && resultFact.runNonce === state.runNonce && resultFact.workItemId === attempt?.workItemId && resultFact.stage === attempt?.stage && resultFact.stageAttemptId === attemptId && resultFact.launchIntentId === binding.launchIntentId && resultFact.workerStorageId === binding.workerStorageId && resultFact.launchOwnerSessionId === binding.launchOwnerSessionId && resultFact.workerId === binding.workerId && resultFact.attemptNumber === binding.attemptNumber && resultFact.attemptNonce === binding.attemptNonce && resultFact.configHash === binding.configHash && resultFact.completionId === binding.completionId, "worker result must match exact plan/run/item/stage/attempt/launch and full generic attempt ingest key");
        const outputValues = [resultFact.outputRepositoryId, resultFact.outputCommonDirIdentityHash, resultFact.outputWorktreeIdentityHash, resultFact.outputSourceBase, resultFact.outputCommit, resultFact.outputTree, resultFact.outputObjectFormat, resultFact.candidateObservedAt];
        const hasOutput = outputValues.every((value) => value !== null);
        pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, hasOutput || outputValues.every((value) => value === null), "worker Git output identity must be wholly present or wholly null");
        if (hasOutput) {
          pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact.outputRepositoryId === state.workItems[attempt!.workItemId]?.writeRepositoryId && resultFact.outputSourceBase.repositoryId === resultFact.outputRepositoryId, "worker Git output must bind the attempt write repository and source base");
          pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, resultFact.outputCommit.length === (resultFact.outputObjectFormat === "sha1" ? 40 : 64) && resultFact.outputTree.length === resultFact.outputCommit.length && resultFact.outputSourceBase.commit.length === resultFact.outputCommit.length, "worker Git OIDs must match the exact reported object format");
          pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, utcTimestampOrderValue(resultFact.candidateObservedAt) >= utcTimestampOrderValue(attempt!.createdAt) && utcTimestampOrderValue(resultFact.candidateObservedAt) <= utcTimestampOrderValue(state.updatedAt), "worker candidate observation time must fall within the immutable attempt history");
        }
        if (["F1", "F3"].includes(attempt?.stage ?? "") && resultFact.terminalStatus === "succeeded") pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, hasOutput, "successful F1/F3 worker result requires exact Git output identity");
        pushIssue(issues, `/workerBindings/${attemptId}/resultHash`, exactReadOnlyWorkerGitBoundary(state, context, attempt, resultFact), "F2/F5 worker Git output must be wholly null or preserve the exact immutable input candidate and attempt materialization identities");
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

  const currentFindingKeys = new Set<string>();
  for (const [findingId, finding] of Object.entries(state.findingClosures)) {
    const findingRef = state.evidenceIndex.findings[findingId];
    const findingFact = findingRef ? context.facts[findingRef.hash] as any : undefined;
    pushIssue(issues, `/findingClosures/${findingId}/findingHash`, findingRef?.hash === finding.findingHash && findingRef.id === findingId, "must match the finding-ID-indexed immutable finding");
    pushIssue(issues, `/findingClosures/${findingId}/workItemId`, Boolean(state.workItems[finding.workItemId]), "references an unknown work item");
    const attempt = state.stageAttempts[finding.stageAttemptId];
    const evidenceFact = context.facts[finding.introducedByEvidenceHash] as any;
    const exactFinding = findingFact?.kind === "finding" && findingFact.hash === finding.findingHash && findingFact.hash === hashWithoutField(findingFact, "hash") && findingFact.planHash === state.identity.planHash && findingFact.runId === state.runId && findingFact.runNonce === state.runNonce && findingFact.authorizationSetHash === state.identity.authorizationSet.hash && findingFact.findingId === findingId && findingFact.workItemId === finding.workItemId && findingFact.stage === finding.stage && findingFact.stageAttemptId === finding.stageAttemptId && findingFact.attemptInputHash === finding.attemptInputHash && findingFact.evidenceHash === finding.introducedByEvidenceHash && findingFact.findingKind === finding.kind && findingFact.severity === finding.severity && findingFact.materiality === finding.materiality && findingFact.fingerprint === finding.fingerprint && findingFact.semanticSubjectId === finding.semanticSubjectId;
    pushIssue(issues, `/findingClosures/${findingId}/findingHash`, Boolean(exactFinding && attempt?.workItemId === finding.workItemId && attempt.stage === finding.stage && attempt.attemptInput.hash === finding.attemptInputHash && evidenceFact?.hash === finding.introducedByEvidenceHash && evidenceFact.hash === hashWithoutField(evidenceFact, "hash") && evidenceFact.stageAttemptId === finding.stageAttemptId), "finding must resolve exact immutable plan/run/authorization/stage/attempt/input/evidence identity");
    const currentKey = canonicalHash({ workItemId: finding.workItemId, semanticSubjectId: finding.semanticSubjectId, fingerprint: finding.fingerprint });
    if (["open", "successor_plan_required"].includes(finding.state)) {
      pushIssue(issues, `/findingClosures/${findingId}`, !currentFindingKeys.has(currentKey), "only one current finding may own an exact work-item/semantic-subject/fingerprint identity");
      currentFindingKeys.add(currentKey);
    }
    if (finding.state === "open") pushIssue(issues, `/findingClosures/${findingId}/resolutionHash`, finding.resolutionHash === null && finding.supersedingEvidenceHash === null && state.evidenceIndex.findingResolutions[findingId] === undefined, "open finding cannot carry resolution authority");
    else {
      const resolutionRef = state.evidenceIndex.findingResolutions[findingId];
      const resolution = resolutionRef ? context.facts[resolutionRef.hash] as any : undefined;
      const exactResolution = resolutionRef?.id === findingId && resolutionRef.hash === finding.resolutionHash && resolution?.kind === "finding_resolution" && resolution.hash === finding.resolutionHash && resolution.hash === hashWithoutField(resolution, "hash") && resolution.planHash === state.identity.planHash && resolution.runId === state.runId && resolution.runNonce === state.runNonce && resolution.authorizationSetHash === state.identity.authorizationSet.hash && resolution.findingId === findingId && resolution.findingHash === finding.findingHash && resolution.workItemId === finding.workItemId && resolution.stage === finding.stage && resolution.stageAttemptId === finding.stageAttemptId && resolution.attemptInputHash === finding.attemptInputHash && resolution.disposition === finding.state && resolution.supersedingEvidenceHash === finding.supersedingEvidenceHash && utcTimestampOrderValue(resolution.resolvedAt) >= utcTimestampOrderValue(findingFact?.observedAt) && utcTimestampOrderValue(resolution.resolvedAt) <= utcTimestampOrderValue(state.updatedAt);
      pushIssue(issues, `/findingClosures/${findingId}/resolutionHash`, Boolean(exactResolution), "closed finding must resolve its sole exact immutable current resolution at a non-future time");
      const exactMateriality = finding.materiality === "plan_affecting"
        ? ["successor_plan_required", "dismissed", "misclassified"].includes(finding.state) && finding.supersedingEvidenceHash === null
        : ["corrected", "equivalent_accepted", "invalidated"].includes(finding.state) && exactFindingResolutionEvidence(state, context, finding, findingFact, resolution);
      pushIssue(issues, `/findingClosures/${findingId}/supersedingEvidenceHash`, exactMateriality, "finding closure disposition and superseding evidence must match materiality exactly");
    }
  }
  for (const [retryKey, retry] of Object.entries(state.retryLedger)) {
    pushIssue(issues, `/retryLedger/${retryKey}/count`, retry.count <= retry.ceiling, "cannot exceed retry ceiling");
    pushIssue(issues, `/retryLedger/${retryKey}/workItemId`, Boolean(state.workItems[retry.workItemId]), "references an unknown work item");
    pushIssue(issues, `/retryLedger/${retryKey}/stop`, retry.count < retry.ceiling || retry.stop !== "none", "ceiling exhaustion requires a stop disposition");
  }
  for (const [slotKey, slot] of Object.entries(state.idempotencySlots)) {
    const bindings = (slot as any).reconciliationBindings ?? [];
    const bindingKeys = bindings.map(({ effectId, observationHash }: any) => `${effectId}/${observationHash}`);
    pushIssue(issues, `/idempotencySlots/${slotKey}/slotId`, slot.slotId === slotKey, "must match the command-record map key");
    pushIssue(issues, `/idempotencySlots/${slotKey}/appliedRevision`, slot.appliedRevision <= state.revision, "cannot name a future accepted revision");
    pushIssue(issues, `/idempotencySlots/${slotKey}/reconciliationBindings`, isSortedUnique(bindingKeys), "reconciliation bindings must be a sorted unique exact effect/observation set");
    if (slot.inputType === "record_effect_observation") {
      pushIssue(issues, `/idempotencySlots/${slotKey}/reconciliationBindings`, bindings.length === 1 && (slot as any).reconciliationCancellationId === undefined && slot.slotId === canonicalHash({ type: slot.inputType, naturalIdentity: bindingKeys[0] }), "effect-observation command record must name its sole exact natural effect/observation identity");
    } else if (slot.inputType === "record_cancellation") {
      const cancellationId = (slot as any).reconciliationCancellationId;
      const cancellation = cancellationId ? state.cancellations[cancellationId] : null;
      pushIssue(issues, `/idempotencySlots/${slotKey}/reconciliationCancellationId`, Boolean(cancellation?.resultHash && cancellation.state === "closed" && slot.slotId === canonicalHash({ type: slot.inputType, naturalIdentity: `${cancellationId}/${cancellation.resultHash}` })), "cancellation command record must bind its exact closed cancellation natural identity");
    } else if (slot.inputType === "record_git_landing_reconciliation") {
      const binding = (slot as any).landingObservationBinding;
      pushIssue(issues, `/idempotencySlots/${slotKey}/landingObservationBinding`, bindings.length === 0 && (slot as any).reconciliationCancellationId === undefined && exactLandingObservationCommandBinding(state, context, slot, binding), "landing command record must retain only its exact immutable observation, owner epoch, and dispatch authority");
    } else pushIssue(issues, `/idempotencySlots/${slotKey}/reconciliationBindings`, bindings.length === 0 && (slot as any).reconciliationCancellationId === undefined && (slot as any).landingObservationBinding === undefined, "only reconciliation transitions may carry effect-observation attribution");
    if (slot.inputType !== "record_git_landing_reconciliation") pushIssue(issues, `/idempotencySlots/${slotKey}/landingObservationBinding`, (slot as any).landingObservationBinding === undefined, "only landing observations may carry historical landing dispatch attribution");
  }

  for (const [effectId, effect] of Object.entries(state.effects)) {
    pushIssue(issues, `/effects/${effectId}/boundOwnerEpoch`, effect.boundOwnerEpoch <= state.owner.ownerEpoch, "cannot exceed current owner epoch");
    pushIssue(issues, `/effects/${effectId}/createdRevision`, effect.createdRevision <= state.revision, "cannot be created after the current revision");
    pushIssue(issues, `/effects/${effectId}/createdAt`, utcTimestampOrderValue(effect.createdAt) >= utcTimestampOrderValue(state.createdAt) && utcTimestampOrderValue(effect.createdAt) <= utcTimestampOrderValue(state.updatedAt), "effect intent creation must lie within the durable snapshot timeline");
    const authorization = effect.boundAuthorizationSetHash === context.authorization.hash ? context.authorization : context.historicalAuthorizations[effect.boundAuthorizationSetHash];
    pushIssue(issues, `/effects/${effectId}/boundAuthorizationSetHash`, Boolean(authorization && authorization.planHash === state.identity.planHash && authorization.hash === hashWithoutField(authorization as unknown as Record<string, unknown>, "hash")), "must resolve to exact canonical current or historical authorization content for this plan");
    if (effect.kind === "cleanup_worktree") {
      const stageAttemptId = effect.boundStageAttemptId ?? null;
      const workerResultHash = effect.boundWorkerResultHash ?? null;
      const attempt = stageAttemptId ? state.stageAttempts[stageAttemptId] : undefined;
      const binding = stageAttemptId ? state.workerBindings[stageAttemptId] : undefined;
      const result = workerResultHash ? context.facts[workerResultHash] as any : undefined;
      const repositoryId = attempt ? state.workItems[attempt.workItemId]?.writeRepositoryId : undefined;
      const exactCleanupRequest = attempt && binding && result?.kind === "worker_result" ? canonicalHash({ kind: "cleanup_worktree", runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, launchIntentId: binding.launchIntentId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, workerResultHash: result.hash, repositoryId, commonDirIdentityHash: result.outputCommonDirIdentityHash, worktreeIdentityHash: result.outputWorktreeIdentityHash }) : null;
      pushIssue(issues, `/effects/${effectId}`, Boolean(attempt?.terminalAt && binding?.resultHash === workerResultHash && result?.outputRepositoryId === repositoryId && result.outputCommonDirIdentityHash !== null && result.outputWorktreeIdentityHash !== null && effect.subject.kind === "work_item" && effect.subject.id === attempt.workItemId && effect.procedureClass === "idempotent" && effect.requestHash === exactCleanupRequest), "cleanup_worktree must bind the exact terminal worker result and repository/worktree identity");
      if (effect.observationHash !== null) pushIssue(issues, `/effects/${effectId}/reconciliation`, ["applied_exact", "proven_absent"].includes(effect.reconciliation), "cleanup_worktree observation must be applied exactly or prove the worktree absent");
    } else {
      pushIssue(issues, `/effects/${effectId}/boundWorkerResultHash`, effect.boundWorkerResultHash == null || effect.kind === "run_procedure", "only cleanup_worktree or exact lifecycle procedure execution may bind a worker-result identity");
      if (effect.boundStageAttemptId != null) {
        const attempt = state.stageAttempts[effect.boundStageAttemptId];
        pushIssue(issues, `/effects/${effectId}/boundStageAttemptId`, Boolean(attempt && effect.subject.kind === "work_item" && effect.subject.id === attempt.workItemId && effect.boundAuthorizationSetHash === attempt.authorizationSetHash), "stage-bound effect must bind an exact attempt/work-item/authorization identity");
        if (attempt && !isPostTerminalClosureEffect(effect.kind) && attempt.terminalAt !== null) {
          const sealRevision = exactStageAttemptSealRevision(state, attempt);
          pushIssue(issues, `/effects/${effectId}/createdRevision`, sealRevision === null ? utcTimestampOrderValue(effect.createdAt) <= utcTimestampOrderValue(attempt.terminalAt) : effect.createdRevision < sealRevision, "stage-bound execution effect intent must have been first authorized before the exact attempt seal");
          pushIssue(issues, `/effects/${effectId}/state`, !["intended", "dispatching"].includes(effect.state), "sealed stage attempt cannot retain pending stage-bound execution dispatch authority");
          if (effect.dispatchCount > 0 && sealRevision !== null) {
            const dispatchRevisions = exactEffectDispatchRevisions(state, effect);
            pushIssue(issues, `/effects/${effectId}/lastDispatchAt`, dispatchRevisions.length === effect.dispatchCount && dispatchRevisions.every((revision) => revision < sealRevision), "every stage-bound execution dispatch must have been durably authorized before the exact attempt seal");
          }
        }
      }
    }
    const isLifecycleExecution = effect.kind === "run_procedure" && effect.boundStageAttemptId != null;
    const isValidationExecution = effect.kind === "verify_prefix" && effect.boundIntegrationAttemptId != null;
    const isExactExecution = isLifecycleExecution || isValidationExecution;
    if (isLifecycleExecution) {
      const attempt = state.stageAttempts[effect.boundStageAttemptId!];
      const procedure = attempt ? Object.values(context.catalog.procedures).find((candidate) => candidate.purpose === "lifecycle" && candidate.stages.includes(attempt.stage) && candidate.producerKinds.includes(attempt.producerKind)) : undefined;
      let expectedRequest: Record<string, unknown> | null = null;
      try { if (attempt && procedure) { expectedRequest = lifecycleProcedureEffectRequestV1(state, context, attempt, procedure); expectedRequest.ownerEpoch = effect.boundOwnerEpoch; expectedRequest.authorizationSetHash = effect.boundAuthorizationSetHash; expectedRequest.freshnessReceiptHash = effect.boundFreshnessReceiptHash; } } catch {}
      pushIssue(issues, `/effects/${effectId}/executionRequest`, Boolean(attempt && procedure && expectedRequest && effect.executionRequest && canonicalHash(effect.executionRequest) === canonicalHash(expectedRequest) && effect.requestHash === canonicalHash(expectedRequest) && effect.boundWorkerResultHash === (attempt.workerResult?.hash ?? null) && effect.boundCandidateGeneration === (attempt.reservedOutputGeneration ?? attempt.inputGeneration) && effect.procedureClass === "pure" && effect.effectScopeId === null), "lifecycle procedure effect must bind the exact attempt/input/catalog/executable/argv/environment/candidate/worker/authority request identity");
    }
    if (isValidationExecution) {
      let expectedRequest: Record<string, unknown> | null = null;
      try { expectedRequest = integrationValidationEffectRequestV1(state, context, effect.boundIntegrationAttemptId!, (effect.executionRequest as any)?.phase); expectedRequest.ownerEpoch = effect.boundOwnerEpoch; expectedRequest.authorizationSetHash = effect.boundAuthorizationSetHash; expectedRequest.freshnessReceiptHash = effect.boundFreshnessReceiptHash; } catch {}
      pushIssue(issues, `/effects/${effectId}/executionRequest`, Boolean(expectedRequest && effect.executionRequest && canonicalHash(effect.executionRequest) === canonicalHash(expectedRequest) && effect.requestHash === canonicalHash(expectedRequest) && effect.subject.kind === "train" && effect.subject.id === (expectedRequest as any).trainId && effect.boundCandidateGeneration === (expectedRequest as any).candidateGeneration && effect.procedureClass === "idempotent" && effect.effectScopeId === null), "integration validation effect must bind the exact attempt/profile/executable/argv/environment/tree/repository/authority request identity");
    }
    if (isExactExecution) {
      const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
      const exactProcedureObservation = isLifecycleExecution && execution?.kind === "effect_execution_observation" && execution.operationKind === "lifecycle_procedure" && execution.authorizationSetHash === effect.boundAuthorizationSetHash && execution.freshnessReceiptHash === effect.boundFreshnessReceiptHash && execution.ownerEpoch === effect.boundOwnerEpoch && execution.requestIdentityHash === effect.requestHash && execution.resultIdentityHash === canonicalHash(execution.result) && execution.resultBytes === Buffer.byteLength(canonicalStringify(execution.result)) && execution.resultBytes <= 4 * 1024 * 1024 && execution.disposition === execution.result?.checkAggregate?.disposition && exactLifecycleProcedureExecutionObservationV1(state, context, effect, execution);
      const request = effect.executionRequest as any;
      const exactValidationObservation = isValidationExecution && execution?.kind === "verification" && execution.authorizationSetHash === effect.boundAuthorizationSetHash && execution.freshnessReceiptHash === effect.boundFreshnessReceiptHash && execution.ownerEpoch === effect.boundOwnerEpoch && execution.requestIdentityHash === effect.requestHash && execution.integrationAttemptId === effect.boundIntegrationAttemptId && execution.phase === request?.phase && execution.repositoryId === request?.repositoryId && execution.trainId === request?.trainId && execution.profileId === request?.profileId && execution.profileHash === request?.profileHash && execution.executableArtifactHash === request?.executableArtifactHash && execution.argvHash === request?.argvHash && execution.cwdMode === request?.cwdMode && execution.environmentProfileId === request?.environmentProfileId && execution.environmentProfileHash === request?.environmentProfileHash && execution.environmentHash === request?.environmentHash && execution.timeoutMs === request?.timeoutMs && execution.readOnly === true && execution.noEdit === true && canonicalHash(execution.tree) === canonicalHash(request?.tree) && execution.commonDirIdentityHash === request?.commonDirIdentityHash && execution.objectFormat === request?.objectFormat && execution.outputBytes <= 1024 * 1024;

      if (effect.executionObservationHash !== null && effect.executionObservationHash !== undefined) pushIssue(issues, `/effects/${effectId}/executionObservationHash`, Boolean(execution?.hash === effect.executionObservationHash && execution.hash === hashWithoutField(execution, "hash") && execution.effectId === effect.effectId && execution.requestHash === effect.requestHash && (exactProcedureObservation || exactValidationObservation)), "execution observation must be one exact immutable bounded result for the persisted request");
      const exactNeverDispatched = effect.state === "cancelled" && effect.dispatchCount === 0 && effect.reconciliation === "proven_absent";
      pushIssue(issues, `/effects/${effectId}/state`, ["intended", "dispatching"].includes(effect.state) || exactNeverDispatched ? effect.executionObservationHash == null : effect.executionObservationHash != null, "exact execution observation may appear only after dispatch and is required before terminal reconciliation");
      if (effect.dispatchCount > 0) pushIssue(issues, `/effects/${effectId}/dispatchCount`, effect.dispatchCount === 1, "exact procedure/validation execution has one immutable dispatch authority and never redispatches after uncertainty");
      if (effect.executionObservationHash) {
        pushIssue(issues, `/effects/${effectId}/executionObservationHash`, utcTimestampOrderValue(execution?.startedAt) >= utcTimestampOrderValue(effect.lastDispatchAt) && utcTimestampOrderValue(execution?.completedAt) >= utcTimestampOrderValue(execution?.startedAt) && utcTimestampOrderValue(execution?.completedAt) <= utcTimestampOrderValue(state.updatedAt), "execution observation timestamps must follow dispatch and be closed by the current snapshot");
        const executionRevision = exactEffectExecutionRevision(state, effect);
        const dispatchRevisions = exactEffectDispatchRevisions(state, effect);
        pushIssue(issues, `/effects/${effectId}/executionObservationHash`, effect.dispatchCount > 0 && executionRevision !== null && dispatchRevisions.length === effect.dispatchCount && dispatchRevisions.every((revision) => revision < executionRevision), "execution observation commit must follow every durable dispatch authority transition");
      }
    }
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
    pushIssue(issues, `/effects/${effectId}/lastDispatchAt`, effect.dispatchCount === 0 ? effect.lastDispatchAt === null : effect.lastDispatchAt !== null && utcTimestampOrderValue(effect.lastDispatchAt) >= utcTimestampOrderValue(effect.createdAt) && utcTimestampOrderValue(effect.lastDispatchAt) <= utcTimestampOrderValue(state.updatedAt), "dispatch count and last dispatch time must change together in durable temporal order");
    if (effect.state === "cancelled") pushIssue(issues, `/effects/${effectId}`, effect.dispatchCount === 0 && effect.lastDispatchAt === null && effect.observationHash === null && effect.reconciliation === "proven_absent", "cancelled effect state is canonical only for a durable never-dispatched intent");
    if (["intended", "dispatching"].includes(effect.state)) {
      pushIssue(issues, `/effects/${effectId}/boundAuthorizationSetHash`, effect.boundAuthorizationSetHash === context.authorization.hash, "pending dispatch must bind current authorization");
      pushIssue(issues, `/effects/${effectId}/boundFreshnessReceiptHash`, effect.boundFreshnessReceiptHash === state.freshness.receipt.hash && !state.freshness.blocksNewLaunches, "pending dispatch must bind current nonblocking freshness");
      if (effect.state === "intended" && !isExactExecution) pushIssue(issues, `/effects/${effectId}/boundOwnerEpoch`, effect.boundOwnerEpoch === state.owner.ownerEpoch, "undispatched non-execution intent must bind current owner epoch");
      if (effect.subject.kind === "work_item" && effect.kind !== "cleanup_worktree") pushIssue(issues, `/effects/${effectId}/boundCandidateGeneration`, effect.boundCandidateGeneration === state.workItems[effect.subject.id]?.candidateGeneration, "pending dispatch must bind current work-item generation");
    }
    if (effect.observationHash !== null) {
      const observation = context.facts[effect.observationHash] as any;
      const exactCommon = observation?.hash === effect.observationHash && observation.hash === hashWithoutField(observation as unknown as Record<string, unknown>, "hash") && observation.planHash === state.identity.planHash && observation.runId === state.runId && observation.runNonce === state.runNonce && observation.effectId === effect.effectId && observation.requestHash === effect.requestHash && observation.reconciliation === effect.reconciliation;
      const exactLaunch = effect.kind === "launch_worker" && observation?.kind === "worker_launch_observation" && observation.ownerEpoch === effect.boundOwnerEpoch && observation.authorizationSetHash === effect.boundAuthorizationSetHash;
      pushIssue(issues, `/effects/${effectId}/observationHash`, Boolean(exactCommon && (observation?.kind === "effect_reconciliation" || exactLaunch)), "must resolve exact canonical immutable effect reconciliation evidence");
      if (observation?.kind === "effect_reconciliation") {
        const closedAt = utcTimestampOrderValue(observation.closedAt);
        const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
        const executionCompletedAt = execution?.completedAt === undefined ? null : utcTimestampOrderValue(execution.completedAt);
        const temporal = Number.isFinite(closedAt)
          && closedAt >= utcTimestampOrderValue(effect.createdAt)
          && (effect.lastDispatchAt === null || closedAt >= utcTimestampOrderValue(effect.lastDispatchAt))
          && (executionCompletedAt === null || Number.isFinite(executionCompletedAt) && closedAt >= executionCompletedAt)
          && closedAt <= utcTimestampOrderValue(state.updatedAt);
        pushIssue(issues, `/effects/${effectId}/observationHash`, temporal, "effect reconciliation closedAt must be a strict timestamp after intent, dispatch, and immutable execution completion and no later than the snapshot");
        pushIssue(issues, `/effects/${effectId}/reconciliationRevision`, exactEffectReconciliationRevision(state, effect) !== null, "effect reconciliation must have one unique exact accepted command record naming this effect observation");
      } else pushIssue(issues, `/effects/${effectId}/reconciliationRevision`, effect.reconciliationRevision == null, "non-reconciliation observations cannot claim reconciliation command attribution");
      if (isExactExecution) {
        const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
        const executionRevision = exactEffectExecutionRevision(state, effect); const reconciliationRevision = exactEffectReconciliationRevision(state, effect);
        pushIssue(issues, `/effects/${effectId}/observationHash`, observation?.kind === "effect_reconciliation" && observation.executionObservationHash === effect.executionObservationHash && observation.resultIdentityHash === (isLifecycleExecution ? execution?.resultIdentityHash : execution?.hash) && observation.closedAt === execution?.completedAt && effect.state === "reconciled" && effect.reconciliation === "applied_exact" && executionRevision !== null && reconciliationRevision !== null && executionRevision < reconciliationRevision, "terminal execution reconciliation must follow and bind the exact closed execution observation/result identity");
      }
    } else pushIssue(issues, `/effects/${effectId}/reconciliationRevision`, effect.reconciliationRevision == null, "an effect without reconciliation evidence cannot claim reconciliation command attribution");
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
    pushIssue(issues, `/integrationAttempts/${attemptId}/entryId`, Boolean(trainEntry) && trainEntry!.attemptIds.includes(attemptId) && (trainEntry!.currentAttemptId === attemptId || (attempt.integrationReceipt !== null && trainEntry!.integrationReceipt === attempt.integrationReceipt) || attempt.conflictClass !== "none" || trainEntry!.state === "invalidated"), "must be listed by the exact train entry as current, conflicted, cancelled, or exactly receipted attempt");
    pushIssue(issues, `/integrationAttempts/${attemptId}/strategy`, attempt.strategy === "merge_tree_one_parent" && planTrain?.strategy === attempt.strategy, "must use accepted explicit-base merge-tree composition");
    pushIssue(issues, `/integrationAttempts/${attemptId}/compositionProfileHash`, attempt.compositionProfileHash === planTrain?.compositionProfileHash && attempt.prefixValidationProfileHash === planTrain?.prefixValidationProfileHash && attempt.finalValidationProfileHash === planTrain?.finalValidationProfileHash, "must bind exact plan-authorized composition, prefix, and final profiles");
    pushIssue(issues, `/integrationAttempts/${attemptId}/expectedPrefix`, expectedSourcePrefix !== null && canonicalHash(attempt.expectedPrefix) === canonicalHash(expectedSourcePrefix) && canonicalHash(attempt.expectedTarget) === canonicalHash(attempt.expectedPrefix), "must bind the exact integrated predecessor prefix and matching observed target");
    if (attempt.landingState !== "landed") pushIssue(issues, `/integrationAttempts/${attemptId}/expectedTarget`, canonicalHash(attempt.expectedTarget) === canonicalHash(train?.expectedTarget), "active integration attempt must bind the train's exact current expected target");
    const historicalConflictCandidate = attempt.conflictClass !== "none" ? context.facts[attempt.sourceCandidateHash] : undefined; const sourceMatchesEntry = attempt.sourceCandidateHash === trainEntry?.sourceCandidate.candidateHash && canonicalHash(attempt.sourceBase) === canonicalHash(trainEntry?.sourceCandidate.base) && canonicalHash(attempt.sourceCandidate) === canonicalHash(trainEntry?.sourceCandidate.git); const sourceMatchesHistoricalConflict = historicalConflictCandidate?.kind === "candidate" && historicalConflictCandidate.workItemId === trainEntry?.workItemId && canonicalHash(historicalConflictCandidate.base) === canonicalHash(attempt.sourceBase) && canonicalHash(historicalConflictCandidate.git) === canonicalHash(attempt.sourceCandidate);
    pushIssue(issues, `/integrationAttempts/${attemptId}/sourceCandidate`, sourceMatchesEntry || sourceMatchesHistoricalConflict, "must bind the current train-entry candidate or exact immutable historical conflict candidate");
    const bindingFact = attempt.repositoryBindingFactHash ? context.facts[attempt.repositoryBindingFactHash] : undefined;
    const compositionEffect = state.effects[attempt.compositionEffectId];
    const compositionFact = attempt.compositionFactHash ? context.facts[attempt.compositionFactHash] as any : undefined;
    const privateRefFacts = attempt.privateRefFactHashes.map((hash) => context.facts[hash] as any);
    const sameBinding = (fact: any) => Boolean(fact && bindingFact && fact.commonDirIdentityHash === bindingFact.commonDirIdentityHash && fact.worktreeIdentityHash === bindingFact.worktreeIdentityHash && fact.gitConfigHash === bindingFact.gitConfigHash && fact.gitVersionHash === bindingFact.gitVersionHash && fact.objectFormat === bindingFact.objectFormat);
    const stateTime = utcTimestampOrderValue(state.updatedAt);
    const bindingTime = utcTimestampOrderValue((bindingFact as any)?.observedAt);
    const readyFact = Object.values(context.facts).find((fact: any) => fact?.kind === "integration_ready" && fact.workItemId === trainEntry?.workItemId && fact.candidateHash === attempt.sourceCandidateHash) as any;
    const f8Evidence = readyFact?.f8EvidenceHash ? context.facts[readyFact.f8EvidenceHash] as any : undefined;
    const readyTime = utcTimestampOrderValue(f8Evidence?.producedAt);
    pushIssue(issues, `/integrationAttempts/${attemptId}/repositoryBindingFactHash`, isExactGitTransactionFact(bindingFact, state, "repository_binding", train?.repositoryId, null) && Number.isFinite(bindingTime) && Number.isFinite(readyTime) && bindingTime >= readyTime && bindingTime <= utcTimestampOrderValue(compositionEffect?.createdAt) && bindingTime <= stateTime, "must resolve the exact non-future repository/common-dir binding fact after F8 readiness and before composition intent");
    if (attempt.compositionFactHash) {
      const compositionTime = utcTimestampOrderValue(compositionFact?.observedAt);
      const privateTimes = privateRefFacts.map((fact) => utcTimestampOrderValue(fact?.observedAt));
      const compositionTemporal = Number.isFinite(compositionTime) && compositionTime >= bindingTime && compositionTime >= utcTimestampOrderValue(compositionEffect?.createdAt) && compositionTime >= utcTimestampOrderValue(compositionEffect?.lastDispatchAt) && compositionTime <= stateTime
        && privateTimes.every((value) => Number.isFinite(value) && value >= bindingTime && value >= utcTimestampOrderValue(compositionEffect?.createdAt) && value >= utcTimestampOrderValue(compositionEffect?.lastDispatchAt) && value <= compositionTime);
      const expectedCompositionReconciliation = attempt.conflictClass === "none" ? "applied_exact" : "conflict";
      const compositionAuthority = sameBinding(compositionFact) && compositionFact?.effectId === compositionEffect?.effectId && compositionFact?.requestHash === compositionEffect?.requestHash && compositionFact?.ownerEpoch === compositionEffect?.boundOwnerEpoch && compositionFact?.reconciliation === expectedCompositionReconciliation
        && (expectedCompositionReconciliation === "applied_exact" ? attempt.composedTree !== null && compositionFact.commit === attempt.composedTree.commit && compositionFact.tree === attempt.composedTree.tree && compositionFact.parentCommit === attempt.syntheticParentCommit : compositionFact.commit === null && compositionFact.tree === null);
      pushIssue(issues, `/integrationAttempts/${attemptId}/compositionFactHash`, isExactGitTransactionFact(compositionFact, state, "composition", train?.repositoryId, attemptId) && compositionTemporal && compositionAuthority, "must resolve exact non-future composition fact after intent, dispatch, binding, and private-ref prerequisites");
    }
    for (const hash of attempt.privateRefFactHashes) {
      const anchor = context.facts[hash] as any;
      pushIssue(issues, `/integrationAttempts/${attemptId}/privateRefFactHashes`, isExactGitTransactionFact(anchor, state, "private_ref", train?.repositoryId, attemptId) && sameBinding(anchor) && anchor.effectId === compositionEffect?.effectId && anchor.requestHash === compositionEffect?.requestHash && anchor.ownerEpoch === compositionEffect?.boundOwnerEpoch && anchor.reconciliation === "applied_exact", "must resolve exact immutable composition-effect private-ref fact");
    }
    if (attempt.proposalVerificationFactHash) {
      const proposal = context.facts[attempt.proposalVerificationFactHash] as any;
      const closure = { prefixEvidenceHashes: [...attempt.prefixEvidenceHashes].sort(), finalEvidenceHashes: [...attempt.finalEvidenceHashes].sort(), prefixEffectReconciliationHashes: [...attempt.prefixEffectReconciliationHashes].sort(), finalEffectReconciliationHashes: [...attempt.finalEffectReconciliationHashes].sort(), environmentClosureHash: attempt.environmentClosureHash };
      const validationFacts = [...attempt.prefixEvidenceHashes, ...attempt.finalEvidenceHashes].map((hash) => context.facts[hash] as any);
      const reconciliationFacts = [...attempt.prefixEffectReconciliationHashes, ...attempt.finalEffectReconciliationHashes].map((hash) => context.facts[hash] as any);
      const proposalTime = utcTimestampOrderValue(proposal?.observedAt);
      const prerequisiteTimes = [compositionFact?.observedAt, ...privateRefFacts.map((fact) => fact?.observedAt), ...validationFacts.map((fact) => fact?.completedAt), ...reconciliationFacts.map((fact) => fact?.closedAt)].map((value) => utcTimestampOrderValue(value));
      const proposalTemporal = Number.isFinite(proposalTime) && prerequisiteTimes.every((value) => Number.isFinite(value) && value <= proposalTime) && proposalTime <= stateTime;
      pushIssue(issues, `/integrationAttempts/${attemptId}/proposalVerificationFactHash`, isExactGitTransactionFact(proposal, state, "proposal_verification", train?.repositoryId, attemptId) && sameBinding(proposal) && proposalTemporal && proposal.effectId === null && proposal.requestHash === canonicalHash({ kind: "proposal_verification", integrationAttemptId: attemptId, closure }) && proposal.detailsHash === canonicalHash(closure), "must resolve exact non-future proposal-verification fact after composition, private-ref, execution, and terminal validation-effect prerequisites without composition-effect authority");
    }
    if (attempt.landingObservationFactHash) {
      const landing = context.facts[attempt.landingObservationFactHash] as any;
      const landingEffect = attempt.landingEffectId ? state.effects[attempt.landingEffectId] : undefined;
      const proposal = attempt.proposalVerificationFactHash ? context.facts[attempt.proposalVerificationFactHash] as any : undefined;
      const landingSlotId = canonicalHash({ type: "record_git_landing_reconciliation", naturalIdentity: `${attemptId}/${attempt.landingObservationFactHash}` });
      const landingSlot = state.idempotencySlots[landingSlotId] as any;
      const historical = landingSlot?.landingObservationBinding;
      const landingTime = utcTimestampOrderValue(landing?.observedAt);
      const landingTemporal = Number.isFinite(landingTime) && landingTime >= utcTimestampOrderValue(proposal?.observedAt) && landingTime >= utcTimestampOrderValue(landingEffect?.createdAt) && landingTime >= utcTimestampOrderValue(historical?.dispatchAt) && landingTime <= stateTime;
      pushIssue(issues, `/integrationAttempts/${attemptId}/landingObservationFactHash`, isExactGitTransactionFact(landing, state, "landing", train?.repositoryId, attemptId) && sameBinding(landing) && landing.effectId === landingEffect?.effectId && landing.requestHash === landingEffect?.requestHash && landing.ownerEpoch === historical?.ownerEpoch && landing.reconciliation === landingEffect?.reconciliation && landingTemporal && exactLandingObservationCommandBinding(state, context, landingSlot, historical), "must resolve the exact accepted historical landing command, owner epoch, and dispatch without rebinding immutable evidence");
    }
    const integrationLock = train?.lockLeaseId ? state.leases[train.lockLeaseId] : undefined;
    const landingObservation = attempt.landingObservationFactHash ? context.facts[attempt.landingObservationFactHash] : undefined;
    const exactTargetConflict = landingObservation?.kind === "git_transaction" && landingObservation.factType === "landing" && landingObservation.reconciliation === "conflict";
    const invalidatedAttempt = trainEntry?.state === "invalidated" && train?.activeIntegrationAttemptId !== attemptId;
    if (attempt.landingState !== "landed" && attempt.conflictClass === "none" && !exactTargetConflict && !invalidatedAttempt) pushIssue(issues, `/integrationAttempts/${attemptId}`, integrationLock?.kind === "integration_lock" && integrationLock.state === "active" && integrationLock.holderIntegrationAttemptId === attemptId && integrationLock.ownerEpoch === state.owner.ownerEpoch && integrationLock.subject.kind === "repository" && integrationLock.subject.id === train?.repositoryId && state.repositories[train!.repositoryId].integrationLockLeaseId === integrationLock.leaseId, "active integration attempt requires one exact current-owner common-repository lock lease");
    if (attempt.conflictClass !== "none" || exactTargetConflict || invalidatedAttempt) pushIssue(issues, `/integrationAttempts/${attemptId}`, integrationLock?.holderIntegrationAttemptId !== attemptId, "conflicted, cancelled, or target-conflict attempt must release its integration lock");
    pushIssue(issues, `/integrationAttempts/${attemptId}/compositionEffectId`, state.effects[attempt.compositionEffectId]?.kind === "compose_candidate", "must reference a compose_candidate effect");
    if (attempt.landingEffectId) pushIssue(issues, `/integrationAttempts/${attemptId}/landingEffectId`, state.effects[attempt.landingEffectId]?.kind === "land_target", "must reference a land_target effect");
    const indexedIntegrationReceipt = state.evidenceIndex.integrationReceipts[attemptId];
    pushIssue(issues, `/integrationAttempts/${attemptId}/integrationReceipt`, attempt.integrationReceipt === null ? indexedIntegrationReceipt === undefined : indexedIntegrationReceipt?.hash === attempt.integrationReceipt, "null and non-null integration receipts must match the exact indexed receipt bidirectionally");
    const validateVerification = (hash: string, reconciliationHash: string | undefined, phase: "prefix" | "final", profileId: string | undefined, profileHash: string | undefined): boolean => {
      const fact = context.facts[hash];
      const profile = profileHash ? context.integrationValidationProfiles?.[profileHash] : undefined;
      const exactProfile = profile && canonicalHash(profile) === profileHash && profile.profileId === profileId
        && fact?.executableArtifactHash === profile.executableArtifactHash && fact.argvHash === canonicalHash(profile.argv)
        && fact.cwdMode === profile.cwdMode && fact.environmentProfileId === profile.environmentProfileId
        && fact.environmentProfileHash === profile.environmentProfileHash && fact.environmentHash === profile.environmentHash
        && fact.timeoutMs === profile.timeoutMs && fact.readOnly === profile.readOnly && fact.noEdit === profile.noEdit;
      const effect = Object.values(state.effects).find((candidate: any) => candidate.kind === "verify_prefix" && candidate.boundIntegrationAttemptId === attemptId && candidate.executionRequest?.phase === phase && candidate.executionObservationHash === hash);
      const reconciliation = reconciliationHash ? context.facts[reconciliationHash] as any : null;
      const proposal = attempt.proposalVerificationFactHash ? context.facts[attempt.proposalVerificationFactHash] as any : null;
      const effectTemporal = effect && utcTimestampOrderValue(effect.createdAt) >= utcTimestampOrderValue(compositionFact?.observedAt)
        && utcTimestampOrderValue(effect.lastDispatchAt) >= utcTimestampOrderValue(effect.createdAt)
        && utcTimestampOrderValue(fact?.startedAt) >= utcTimestampOrderValue(effect.lastDispatchAt)
        && utcTimestampOrderValue(fact?.completedAt) >= utcTimestampOrderValue(fact?.startedAt)
        && utcTimestampOrderValue(reconciliation?.closedAt) === utcTimestampOrderValue(fact?.completedAt)
        && (!proposal || utcTimestampOrderValue(fact?.completedAt) <= utcTimestampOrderValue(proposal.observedAt));
      const exactEffect = effect && effectTemporal && effect.state === "reconciled" && effect.reconciliation === "applied_exact" && effect.observationHash === reconciliationHash && reconciliation?.kind === "effect_reconciliation" && reconciliation.hash === reconciliationHash && reconciliation.hash === hashWithoutField(reconciliation, "hash") && reconciliation.effectId === effect.effectId && reconciliation.requestHash === effect.requestHash && reconciliation.executionObservationHash === hash && reconciliation.resultIdentityHash === hash && reconciliation.reconciliation === "applied_exact";
      return Boolean(exactEffect) && state.evidenceIndex.verifications[hash]?.hash === hash && fact?.kind === "verification" && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce && fact.authorizationSetHash === state.identity.authorizationSet.hash && fact.repositoryId === train?.repositoryId && fact.trainId === planTrain?.trainId && fact.integrationAttemptId === attemptId && fact.effectId === effect?.effectId && fact.requestHash === effect?.requestHash && fact.requestIdentityHash === effect?.requestHash && fact.ownerEpoch === effect?.boundOwnerEpoch && fact.freshnessReceiptHash === effect?.boundFreshnessReceiptHash && fact.phase === phase && fact.profileId === profileId && fact.profileHash === profileHash && exactProfile && attempt.composedTree !== null && canonicalHash(fact.tree) === canonicalHash(attempt.composedTree) && fact.objectFormat === (attempt.composedTree.commit.length === 40 ? "sha1" : "sha256") && fact.exitCode === 0 && fact.signal === null && fact.parser === "strict-json-disposition-v1" && fact.parserDisposition === "PASS" && fact.parsedResultHash !== null && fact.disposition === "PASS" && utcTimestampOrderValue(fact.completedAt) >= utcTimestampOrderValue(fact.startedAt);
    };
    pushIssue(issues, `/integrationAttempts/${attemptId}/prefixEvidenceHashes`, isSortedUnique([...attempt.prefixEvidenceHashes]) && isSortedUnique([...attempt.prefixEffectReconciliationHashes]) && attempt.prefixEvidenceHashes.length === attempt.prefixEffectReconciliationHashes.length && attempt.prefixEvidenceHashes.every((hash, index) => validateVerification(hash, attempt.prefixEffectReconciliationHashes[index], "prefix", planTrain?.prefixValidationProfileId, planTrain?.prefixValidationProfileHash)), "prefix evidence must resolve to exact passing composed-tree profile facts and terminal effect reconciliations");
    pushIssue(issues, `/integrationAttempts/${attemptId}/finalEvidenceHashes`, isSortedUnique([...attempt.finalEvidenceHashes]) && isSortedUnique([...attempt.finalEffectReconciliationHashes]) && attempt.finalEvidenceHashes.length === attempt.finalEffectReconciliationHashes.length && attempt.finalEvidenceHashes.every((hash, index) => validateVerification(hash, attempt.finalEffectReconciliationHashes[index], "final", planTrain?.finalValidationProfileId, planTrain?.finalValidationProfileHash)), "final evidence must resolve to exact passing composed-tree profile facts and terminal effect reconciliations");
    if (attempt.landingState === "landed") {
      const receipt = attempt.integrationReceipt ? context.facts[attempt.integrationReceipt] as any : undefined;
      const transactionFact = receipt?.kind === "integration" ? context.facts[receipt.transactionReceiptFactHash] as any : undefined;
      const transactionSealedAt = utcTimestampOrderValue(transactionFact?.receipt?.sealedAt);
      const integrationSealedAt = utcTimestampOrderValue(receipt?.sealedAt);
      const landingObservedAt = utcTimestampOrderValue((context.facts[attempt.landingObservationFactHash!] as any)?.observedAt);
      const completedAt = utcTimestampOrderValue(state.workItems[trainEntry!.workItemId]?.completedAt);
      const receiptTemporal = Number.isFinite(transactionSealedAt) && Number.isFinite(integrationSealedAt) && Number.isFinite(completedAt) && transactionSealedAt >= landingObservedAt && integrationSealedAt >= transactionSealedAt && integrationSealedAt <= completedAt && completedAt <= stateTime;
      pushIssue(issues, `/integrationAttempts/${attemptId}`, attempt.composedTree !== null && attempt.intendedLandedTree !== null && canonicalHash(attempt.composedTree) === canonicalHash(attempt.intendedLandedTree) && attempt.syntheticParentCommit === attempt.expectedPrefix.commit && attempt.sourceToIntegratedLineageHash !== null && attempt.environmentClosureHash !== null && attempt.integrationReceipt !== null && attempt.prefixEvidenceHashes.length > 0 && attempt.finalEvidenceHashes.length > 0 && attempt.prefixEffectReconciliationHashes.length === attempt.prefixEvidenceHashes.length && attempt.finalEffectReconciliationHashes.length === attempt.finalEvidenceHashes.length && receipt?.kind === "integration" && receipt.transactionReceiptHash && receipt.landingObservationHash === attempt.landingObservationFactHash && receiptTemporal, "landed attempt requires exact composed/intended tree, one synthetic parent, source lineage, required profile evidence, and temporally ordered transaction/landing/integration receipts");
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

function exactFindingResolutionEvidence(state: DagRunStateV1, context: DagRunValidationContextV1, finding: any, findingFact: any, resolution: any): boolean {
  const hash = resolution?.supersedingEvidenceHash;
  if (typeof hash !== "string" || hash === finding.introducedByEvidenceHash) return false;
  const evidence = context.facts[hash] as any;
  if (!evidence || evidence.hash !== hash || evidence.hash !== hashWithoutField(evidence, "hash") || evidence.planHash !== state.identity.planHash || evidence.runId !== state.runId || evidence.runNonce !== state.runNonce || evidence.workItemId !== finding.workItemId) return false;
  const attempt = state.stageAttempts[evidence.stageAttemptId];
  if (!attempt || attempt.workItemId !== finding.workItemId || attempt.stage !== evidence.stage || attempt.attemptInput.hash !== evidence.attemptInputHash) return false;
  const observedAt = evidence.kind === "stage_evidence" ? evidence.producedAt : evidence.kind === "finding_correction" ? evidence.observedAt : null;
  if (typeof observedAt !== "string" || utcTimestampOrderValue(observedAt) < utcTimestampOrderValue(findingFact?.observedAt) || utcTimestampOrderValue(observedAt) > utcTimestampOrderValue(resolution.resolvedAt)) return false;
  const candidateHash = evidence.candidateHash ?? null;
  const generation = evidence.candidateGeneration;
  const candidate = candidateHash ? context.facts[candidateHash] as any : null;
  const validCandidate = generation === 0
    ? candidateHash === null
    : candidate?.kind === "candidate" && candidate.hash === candidateHash && candidate.hash === hashWithoutField(candidate, "hash") && candidate.planHash === state.identity.planHash && candidate.runId === state.runId && candidate.runNonce === state.runNonce && candidate.workItemId === finding.workItemId && candidate.generation === generation && state.evidenceIndex.candidates[candidateHash]?.hash === candidateHash;
  if (!validCandidate) return false;
  if (evidence.kind === "stage_evidence") return state.evidenceIndex.stageEvidence[hash]?.hash === hash && evidence.authorizationSetHash === state.identity.authorizationSet.hash;
  const correctionIndex = (state.evidenceIndex as any).findingCorrections ?? {};
  return resolution.disposition === "corrected" && evidence.kind === "finding_correction" && correctionIndex[hash]?.hash === hash && evidence.authorizationSetHash === state.identity.authorizationSet.hash && evidence.findingId === finding.findingId && evidence.findingHash === finding.findingHash;
}

function validateOwnershipReceiptChain(state: DagRunStateV1, context: DagRunValidationContextV1, issues: ValidationIssue[]): void {
  let receiptHash: string | null = state.owner.ownershipReceipt;
  let expectedEpoch = state.owner.ownerEpoch;
  const visited = new Set<string>();
  for (let depth = 0; receiptHash !== null && depth < MAX_OWNERSHIP_LINEAGE_DEPTH_V1; depth += 1) {
    const fact = context.facts[receiptHash] as any;
    const path = `/owner/ownershipReceipt/chain/${expectedEpoch}`;
    if (!fact || fact.kind !== "ownership" || fact.hash !== receiptHash || fact.hash !== hashWithoutField(fact, "hash") || fact.runId !== state.runId || fact.runNonce !== state.runNonce) {
      pushIssue(issues, path, false, "ownership chain receipt is missing, noncanonical, or cross-run");
      return;
    }
    if (visited.has(receiptHash)) {
      pushIssue(issues, path, false, "ownership receipt chain contains a cycle");
      return;
    }
    visited.add(receiptHash);
    pushIssue(issues, `${path}/ownerEpoch`, fact.ownerEpoch === expectedEpoch, "ownership receipt epoch must descend exactly by one");
    const predecessorHash = fact.priorOwnershipReceiptHash as string | null;
    const predecessor = predecessorHash ? context.facts[predecessorHash] as any : null;
    pushIssue(issues, `${path}/priorOwnershipReceiptHash`, expectedEpoch === 1 ? predecessorHash === null : typeof predecessorHash === "string", "only epoch-one genesis may have a null ownership predecessor");
    pushIssue(issues, `${path}/chainHash`, fact.chainHash === ownershipChainHashV1(fact, predecessor?.kind === "ownership" ? predecessor.chainHash : null), "ownership chain hash must bind the exact predecessor chain and successor epoch");
    if (predecessorHash) {
      pushIssue(issues, `${path}/priorOwnershipReceiptHash`, predecessor?.kind === "ownership" && predecessor.hash === predecessorHash && predecessor.ownerEpoch === expectedEpoch - 1, "ownership predecessor must resolve the exact immediately preceding epoch");
      if (fact.disposition !== "absent") pushIssue(issues, `${path}/priorOwnershipReceiptHash`, predecessor?.successorSessionId === fact.priorSessionId && predecessor?.successorPid === fact.priorPid && predecessor?.successorProcessStartIdentity === fact.priorProcessStartIdentity && predecessor?.successorLockIdentity === fact.priorLockIdentity, "ownership predecessor successor identity must equal the claimed prior owner");
    }
    if (fact.disposition === "dead") {
      const observation = fact.priorObservationHash ? context.facts[fact.priorObservationHash] as any : undefined;
      const priorLock = fact.priorSessionId && fact.priorOwnerTokenHash && fact.priorProcessStartIdentity && fact.priorLockIdentity && fact.priorAttachedAt ? { lockIdentity: fact.priorLockIdentity, ownerTokenHash: fact.priorOwnerTokenHash, sessionId: fact.priorSessionId, pid: fact.priorPid, processStartIdentity: fact.priorProcessStartIdentity, acquiredAt: fact.priorAttachedAt } : null;
      pushIssue(issues, `${path}/priorObservationHash`, observation?.kind === "process_identity_observation" && observation.hash === fact.priorObservationHash && observation.hash === hashWithoutField(observation, "hash") && priorLock !== null && observation.lockMetadataHash === canonicalHash(priorLock) && observation.pid === fact.priorPid && observation.processStartIdentity === fact.priorProcessStartIdentity && ["dead_missing", "dead_reused"].includes(observation.disposition), "dead-owner receipt must retain exact process observation evidence");
    } else pushIssue(issues, `${path}/priorObservationHash`, fact.priorObservationHash === null, "only dead-owner recovery may cite process observation evidence");
    receiptHash = predecessorHash;
    expectedEpoch -= 1;
  }
  pushIssue(issues, "/owner/ownershipReceipt", receiptHash === null && expectedEpoch === 0, "ownership receipt lineage must terminate exactly at epoch one within the bounded lineage depth");
}

function isPostTerminalClosureEffect(kind: string): boolean {
  return ["cancel_worker", "cleanup_worktree", "reconcile_external_effect"].includes(kind);
}

function exactStageAttemptSealRevision(state: DagRunStateV1, attempt: any): number | null {
  if (!attempt?.evidence?.hash) return null;
  const item = state.workItems[attempt.workItemId];
  const inputType = attempt.stage === "F8" ? "seal_f8_integration_ready" : "seal_stage_attempt";
  const naturalIdentity = attempt.stage === "F8"
    ? item?.integrationReadyReceipt ? `${attempt.stageAttemptId}/${item.integrationReadyReceipt}` : null
    : `${attempt.stageAttemptId}/${attempt.evidence.hash}`;
  if (naturalIdentity === null) return null;
  const slotId = canonicalHash({ type: inputType, naturalIdentity });
  const slot = state.idempotencySlots[slotId];
  return slot?.inputType === inputType ? slot.appliedRevision : null;
}

function exactEffectDispatchRevisions(state: DagRunStateV1, effect: any): number[] {
  const revisions: number[] = [];
  for (let expectedDispatchCount = 0; expectedDispatchCount < effect.dispatchCount; expectedDispatchCount += 1) {
    const inputType = expectedDispatchCount === 0 ? "mark_effect_dispatching" : "retry_effect_dispatch";
    const slotId = canonicalHash({ type: inputType, naturalIdentity: `${effect.effectId}/${expectedDispatchCount}` });
    const slot = state.idempotencySlots[slotId];
    if (slot?.inputType === inputType) revisions.push(slot.appliedRevision);
  }
  return revisions;
}

function exactLandingObservationCommandBinding(state: DagRunStateV1, context: DagRunValidationContextV1, slot: any, binding: any): boolean {
  if (!slot || slot.inputType !== "record_git_landing_reconciliation" || !binding) return false;
  const effect = state.effects[binding.effectId];
  const attempt = state.integrationAttempts[binding.integrationAttemptId];
  const fact = context.facts[binding.observationHash] as any;
  const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId]));
  const repositoryBinding = attempt?.repositoryBindingFactHash ? context.facts[attempt.repositoryBindingFactHash] as any : null;
  const proposal = attempt?.proposalVerificationFactHash ? context.facts[attempt.proposalVerificationFactHash] as any : null;
  if (!effect || !attempt || !train || attempt.landingEffectId !== effect.effectId || effect.kind !== "land_target" || !Number.isInteger(binding.dispatchCount) || binding.dispatchCount < 1 || binding.dispatchCount > effect.dispatchCount) return false;
  if (slot.slotId !== canonicalHash({ type: slot.inputType, naturalIdentity: `${binding.integrationAttemptId}/${binding.observationHash}` }) || slot.appliedRevision <= binding.dispatchRevision) return false;
  if (fact?.kind !== "git_transaction" || fact.factType !== "landing" || fact.hash !== binding.observationHash || fact.hash !== hashWithoutField(fact, "hash") || fact.planHash !== state.identity.planHash || fact.runId !== state.runId || fact.runNonce !== state.runNonce || fact.repositoryId !== train.repositoryId || fact.integrationAttemptId !== binding.integrationAttemptId || fact.effectId !== binding.effectId || fact.requestHash !== binding.requestHash || fact.authorizationSetHash !== binding.authorizationSetHash || fact.authorizationSetHash !== state.identity.authorizationSet.hash || fact.ownerEpoch !== binding.ownerEpoch) return false;
  const sameBinding = repositoryBinding && fact.commonDirIdentityHash === repositoryBinding.commonDirIdentityHash && fact.worktreeIdentityHash === repositoryBinding.worktreeIdentityHash && fact.gitConfigHash === repositoryBinding.gitConfigHash && fact.gitVersionHash === repositoryBinding.gitVersionHash && fact.objectFormat === repositoryBinding.objectFormat;
  const observedAt = utcTimestampOrderValue(fact.observedAt);
  const temporal = Number.isFinite(observedAt) && observedAt >= utcTimestampOrderValue(repositoryBinding?.observedAt) && observedAt >= utcTimestampOrderValue(proposal?.observedAt) && observedAt >= utcTimestampOrderValue(effect.createdAt) && observedAt >= utcTimestampOrderValue(binding.dispatchAt) && observedAt <= utcTimestampOrderValue(state.updatedAt);
  const exactDisposition = fact.reconciliation === "applied_exact"
    ? attempt.intendedLandedTree !== null && fact.commit === attempt.intendedLandedTree.commit && fact.tree === attempt.intendedLandedTree.tree
    : fact.reconciliation === "proven_absent"
      ? fact.commit === attempt.expectedTarget.commit && fact.tree === attempt.expectedTarget.tree
      : fact.reconciliation === "conflict"
        ? fact.commit !== null && fact.tree !== null && !(fact.commit === attempt.expectedTarget.commit && fact.tree === attempt.expectedTarget.tree) && !(attempt.intendedLandedTree && fact.commit === attempt.intendedLandedTree.commit && fact.tree === attempt.intendedLandedTree.tree)
        : fact.reconciliation === "unknown";
  if (!sameBinding || !temporal || !exactDisposition || binding.requestHash !== effect.requestHash || typeof binding.dispatchAt !== "string") return false;
  const authorization = binding.authorizationSetHash === context.authorization.hash ? context.authorization : context.historicalAuthorizations?.[binding.authorizationSetHash];
  if (!authorization || authorization.planHash !== state.identity.planHash || authorization.hash !== hashWithoutField(authorization as unknown as Record<string, unknown>, "hash")) return false;
  let priorRevision = -1;
  for (let expectedDispatchCount = 0; expectedDispatchCount < binding.dispatchCount; expectedDispatchCount += 1) {
    const inputType = expectedDispatchCount === 0 ? "mark_effect_dispatching" : "retry_effect_dispatch";
    const dispatchSlotId = canonicalHash({ type: inputType, naturalIdentity: `${effect.effectId}/${expectedDispatchCount}` });
    const dispatchSlot = state.idempotencySlots[dispatchSlotId];
    if (!dispatchSlot || dispatchSlot.inputType !== inputType || dispatchSlot.appliedRevision <= priorRevision || dispatchSlot.appliedRevision >= slot.appliedRevision) return false;
    priorRevision = dispatchSlot.appliedRevision;
  }
  return priorRevision === binding.dispatchRevision;
}

function exactEffectExecutionRevision(state: DagRunStateV1, effect: any): number | null {
  if (!effect.executionObservationHash) return null;
  const slotId = canonicalHash({ type: "record_effect_execution", naturalIdentity: `${effect.effectId}/${effect.executionObservationHash}` });
  const slot = state.idempotencySlots[slotId];
  return slot?.inputType === "record_effect_execution" ? slot.appliedRevision : null;
}

function exactEffectReconciliationRevision(state: DagRunStateV1, effect: any): number | null {
  if (!effect.observationHash || !Number.isInteger(effect.reconciliationRevision) || effect.reconciliationRevision <= 0) return null;
  const matches = Object.values(state.idempotencySlots).filter((slot: any) => {
    if (slot.appliedRevision !== effect.reconciliationRevision || !["record_effect_observation", "record_cancellation"].includes(slot.inputType)) return false;
    const bindings = slot.reconciliationBindings ?? [];
    if (!bindings.some((binding: any) => binding.effectId === effect.effectId && binding.observationHash === effect.observationHash)) return false;
    if (slot.inputType === "record_effect_observation") {
      return bindings.length === 1
        && slot.reconciliationCancellationId === undefined
        && slot.slotId === canonicalHash({ type: slot.inputType, naturalIdentity: `${effect.effectId}/${effect.observationHash}` });
    }
    const cancellation = slot.reconciliationCancellationId ? state.cancellations[slot.reconciliationCancellationId] : null;
    return Boolean(cancellation?.resultHash
      && slot.slotId === canonicalHash({ type: slot.inputType, naturalIdentity: `${cancellation.cancellationId}/${cancellation.resultHash}` }));
  });
  return matches.length === 1 ? matches[0].appliedRevision : null;
}

function fixedStageProducers(stage: typeof PLAN_STAGE_IDS[number]): string[] {
  return ({ F0: ["conductor"], F1: ["owned_worker"], F2: ["owned_worker"], F3: ["owned_worker"], F4: ["deterministic_runner"], F5: ["owned_worker"], F6: ["owned_worker", "deterministic_runner"], F7: ["deterministic_runner"], F8: ["conductor"] } as Record<string, string[]>)[stage];
}

export function lifecycleProcedureEffectRequestV1(state: DagRunStateV1, context: DagRunValidationContextV1, attempt: any, procedure: ProcedureCatalogBindingV1): Record<string, unknown> {
  const item = state.workItems[attempt.workItemId];
  if (!item) throw new Error("Lifecycle procedure request requires an exact work item");
  const executable = procedure.executable;
  const candidateGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
  const inputFact = context.facts[attempt.attemptInput.hash] as any;
  const historicalCandidate = Object.values(context.facts).find((fact: any) => fact?.kind === "candidate" && fact.workItemId === attempt.workItemId && fact.generation === candidateGeneration && (attempt.stage === "F1" ? fact.producedByStageAttemptId === attempt.stageAttemptId : attempt.stage === "F3" ? fact.producedByStageAttemptId === attempt.stageAttemptId || fact.hash === inputFact?.candidateHash : fact.hash === inputFact?.candidateHash)) as any;
  const candidate = attempt.stage === "F0" ? null : item.candidate?.generation === candidateGeneration ? item.candidate : historicalCandidate ? { candidateHash: historicalCandidate.hash, git: historicalCandidate.git } : null;
  return {
    requestKind: "lifecycle_procedure_v1", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
    workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash,
    producerKind: attempt.producerKind, procedureHash: procedure.hash, procedureCatalogHash: canonicalHash(context.catalog), executableMappingHash: canonicalHash(executable),
    executableArtifactHash: executable.executableArtifactHash, argv: [...executable.argv], argvHash: canonicalHash(executable.argv),
    argvEnvironmentHash: canonicalHash({ argv: executable.argv, environmentHash: executable.environmentHash }), cwdMode: executable.cwdMode,
    environmentProfileId: executable.environmentProfileId, environmentProfileHash: executable.environmentProfileHash, environmentHash: executable.environmentHash,
    timeoutMs: executable.timeoutMs, readOnly: executable.readOnly, noEdit: executable.noEdit,
    candidateGeneration, candidateHash: candidate?.candidateHash ?? null, candidateTree: candidate ? structuredClone(candidate.git) : null,
    workerResultHash: attempt.workerResult?.hash ?? null, authorizationSetHash: attempt.authorizationSetHash,
    freshnessReceiptHash: state.freshness.receipt.hash, ownerEpoch: state.owner.ownerEpoch,
  };
}

export function integrationValidationEffectRequestV1(state: DagRunStateV1, context: DagRunValidationContextV1, integrationAttemptId: string, phase: "prefix" | "final"): Record<string, unknown> {
  const attempt = state.integrationAttempts[integrationAttemptId];
  const train = Object.values(state.integrationTrains).find(({ entries }) => Boolean(entries[attempt?.entryId]));
  const entry = train?.entries[attempt?.entryId];
  const item = entry ? state.workItems[entry.workItemId] : undefined;
  const planTrain = context.plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === train?.repositoryId);
  const profileHash = phase === "prefix" ? planTrain?.prefixValidationProfileHash : planTrain?.finalValidationProfileHash;
  const profileId = phase === "prefix" ? planTrain?.prefixValidationProfileId : planTrain?.finalValidationProfileId;
  const profile = profileHash ? context.integrationValidationProfiles?.[profileHash] : undefined;
  const binding = attempt?.repositoryBindingFactHash ? context.facts[attempt.repositoryBindingFactHash] as any : undefined;
  if (!attempt || !train || !entry || !item || !planTrain || !profile || !profileHash || !profileId || !attempt.composedTree || binding?.kind !== "git_transaction") throw new Error("Integration validation request requires exact attempt/profile/tree/repository authority");
  return {
    requestKind: "integration_validation_v1", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
    repositoryId: train.repositoryId, trainId: planTrain.trainId, integrationAttemptId, phase, profileId, profileHash,
    executableArtifactHash: profile.executableArtifactHash, argv: [...profile.argv], argvHash: canonicalHash(profile.argv),
    argvEnvironmentHash: canonicalHash({ argv: profile.argv, environmentHash: profile.environmentHash }), cwdMode: profile.cwdMode,
    environmentProfileId: profile.environmentProfileId, environmentProfileHash: profile.environmentProfileHash, environmentHash: profile.environmentHash,
    timeoutMs: profile.timeoutMs, readOnly: profile.readOnly, noEdit: profile.noEdit, tree: structuredClone(attempt.composedTree),
    commonDirIdentityHash: binding.commonDirIdentityHash, repositoryWorktreeIdentityHash: binding.worktreeIdentityHash,
    gitConfigHash: binding.gitConfigHash, gitVersionHash: binding.gitVersionHash, objectFormat: binding.objectFormat,
    candidateGeneration: item.candidateGeneration, authorizationSetHash: state.identity.authorizationSet.hash,
    freshnessReceiptHash: state.freshness.receipt.hash, ownerEpoch: state.owner.ownerEpoch,
  };
}

function exactLifecycleProcedureExecutionObservationV1(state: DagRunStateV1, context: DagRunValidationContextV1, effect: any, observation: any): boolean {
  const request = effect.executionRequest as any;
  const result = observation.result as any;
  const aggregate = result?.checkAggregate; const evidence = result?.evidence;
  const attempt = state.stageAttempts[effect.boundStageAttemptId];
  if (!request || !attempt || aggregate?.kind !== "check_aggregate" || evidence?.kind !== "stage_evidence") return false;
  const canonicalFact = (fact: any) => fact && typeof fact === "object" && fact.hash === hashWithoutField(fact, "hash");
  const common = canonicalFact(aggregate) && canonicalFact(evidence)
    && aggregate.planHash === state.identity.planHash && evidence.planHash === state.identity.planHash
    && aggregate.runId === state.runId && evidence.runId === state.runId && aggregate.runNonce === state.runNonce && evidence.runNonce === state.runNonce
    && aggregate.authorizationSetHash === effect.boundAuthorizationSetHash && evidence.authorizationSetHash === effect.boundAuthorizationSetHash
    && aggregate.workItemId === attempt.workItemId && evidence.workItemId === attempt.workItemId
    && aggregate.stage === attempt.stage && evidence.stage === attempt.stage && aggregate.stageAttemptId === attempt.stageAttemptId && evidence.stageAttemptId === attempt.stageAttemptId
    && aggregate.attemptInputHash === attempt.attemptInput.hash && evidence.attemptInputHash === attempt.attemptInput.hash
    && aggregate.procedureHash === request.procedureHash && evidence.procedureHash === request.procedureHash
    && aggregate.environmentProfileHash === request.environmentProfileHash && evidence.environmentProfileHash === request.environmentProfileHash
    && evidence.checkAggregateHash === aggregate.hash && evidence.disposition === aggregate.disposition && evidence.disposition === observation.disposition
    && evidence.candidateGeneration === request.candidateGeneration && evidence.candidateHash === request.candidateHash
    && evidence.producerKind === attempt.producerKind && evidence.producerResultHash === request.workerResultHash
    && Array.isArray(evidence.effectReconciliationHashes) && evidence.effectReconciliationHashes.length === 0 && evidence.readOnly === request.readOnly;
  if (!common) return false;
  const nestedFacts = [...(result.oracleAssertions ?? []), ...(result.checkDispositions ?? []), ...(result.checkExecutions ?? []), ...(result.checkAuthorities ?? [])];
  if (!nestedFacts.every(canonicalFact)) return false;
  const requiresEnvironment = ["F2", "F5", "F7"].includes(attempt.stage);
  if (requiresEnvironment) {
    const environment = result.environmentObservation; const materialization = result.workspaceMaterialization;
    if (!canonicalFact(environment) || !canonicalFact(materialization) || evidence.environmentObservationHash !== environment.hash || environment.workspaceMaterializationHash !== materialization.hash || environment.candidateHash !== request.candidateHash || canonicalHash(environment.candidateTree) !== canonicalHash(request.candidateTree) || environment.environmentProfileHash !== request.environmentProfileHash) return false;
  } else if (evidence.environmentObservationHash != null || result.environmentObservation || result.workspaceMaterialization) return false;
  if (attempt.stage === "F8" && aggregate.disposition === "PASS") {
    const ready = result.integrationReady;
    if (!canonicalFact(ready) || ready.workItemId !== attempt.workItemId || ready.candidateGeneration !== request.candidateGeneration || ready.candidateHash !== request.candidateHash || ready.f8EvidenceHash !== evidence.hash) return false;
  }
  return true;
}

export function exactLifecycleProcedureCatalogBindingV1(context: DagRunValidationContextV1, evidence: any, stage: typeof PLAN_STAGE_IDS[number], producerKind: string): boolean {
  const procedure = context.catalog.procedures[evidence?.procedureHash] as any;
  const executable = procedure?.executable;
  const exactHash = (value: unknown): value is string => typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
  const stageRequiresReadOnly = ["F2", "F4", "F5", "F7"].includes(stage);
  return Boolean(procedure
    && procedure.hash === evidence.procedureHash
    && procedure.hash === hashWithoutField(procedure as Record<string, unknown>, "hash")
    && procedure.purpose === "lifecycle"
    && procedure.stages.includes(stage)
    && fixedStageProducers(stage).includes(producerKind)
    && procedure.producerKinds.length === 1
    && procedure.producerKinds[0] === producerKind
    && procedure.environmentProfileHash === evidence.environmentProfileHash
    && procedure.readOnly === evidence.readOnly
    && (!stageRequiresReadOnly || procedure.readOnly)
    && executable
    && exactHash(executable.executableArtifactHash)
    && executable.argv.length > 0
    && executable.argv.every((argument: unknown) => typeof argument === "string" && argument.length > 0)
    && typeof executable.environmentProfileId === "string"
    && executable.environmentProfileId.length > 0
    && executable.environmentProfileHash === procedure.environmentProfileHash
    && exactHash(executable.environmentHash)
    && executable.timeoutMs > 0
    && executable.readOnly === procedure.readOnly
    && executable.noEdit === procedure.readOnly);
}

function exactReadOnlyWorkerGitBoundary(state: DagRunStateV1, context: DagRunValidationContextV1, attempt: any, result: any): boolean {
  if (!attempt || !["F2", "F5"].includes(attempt.stage)) return true;
  const values = [result?.outputRepositoryId, result?.outputCommonDirIdentityHash, result?.outputWorktreeIdentityHash, result?.outputSourceBase, result?.outputCommit, result?.outputTree, result?.outputObjectFormat, result?.candidateObservedAt];
  if (values.every((value) => value === null)) return true;
  if (!values.every((value) => value !== null && value !== undefined)) return false;
  const input = context.facts[attempt.attemptInput.hash] as any;
  const candidate = input?.kind === "stage_attempt_input" && typeof input.candidateHash === "string" ? context.facts[input.candidateHash] as any : undefined;
  if (candidate?.kind !== "candidate" || candidate.hash !== input.candidateHash || candidate.hash !== hashWithoutField(candidate as Record<string, unknown>, "hash") || candidate.planHash !== state.identity.planHash || candidate.runId !== state.runId || candidate.runNonce !== state.runNonce || candidate.generation !== attempt.inputGeneration || candidate.workItemId !== attempt.workItemId || candidate.git.repositoryId !== state.workItems[attempt.workItemId]?.writeRepositoryId) return false;
  const expectedObjectFormat = candidate.git.commit.length === 40 ? "sha1" : candidate.git.commit.length === 64 ? "sha256" : null;
  if (result.outputRepositoryId !== candidate.git.repositoryId
    || canonicalHash(result.outputSourceBase) !== canonicalHash(candidate.git)
    || result.outputCommit !== candidate.git.commit
    || result.outputTree !== candidate.git.tree
    || result.outputObjectFormat !== expectedObjectFormat) return false;
  const evidence = attempt.evidence ? context.facts[attempt.evidence.hash] as any : undefined;
  const observation = evidence?.kind === "stage_evidence" && typeof evidence.environmentObservationHash === "string" ? context.facts[evidence.environmentObservationHash] as any : undefined;
  const materialization = observation?.kind === "environment_observation" ? context.facts[observation.workspaceMaterializationHash] as any : undefined;
  if (observation?.kind === "environment_observation" && materialization?.kind === "workspace_materialization") {
    return result.outputCommonDirIdentityHash === observation.commonDirIdentityHash
      && result.outputCommonDirIdentityHash === materialization.commonDirIdentityHash
      && result.outputWorktreeIdentityHash === observation.worktreeIdentityHash
      && result.outputWorktreeIdentityHash === materialization.worktreeIdentityHash;
  }
  return true;
}

function exactEnvironmentObservation(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, attempt: any, evidence: any): boolean {
  const hash = evidence?.environmentObservationHash;
  if (typeof hash !== "string") return false;
  const observation = context.facts[hash] as any;
  const materialization = observation?.kind === "environment_observation" ? context.facts[observation.workspaceMaterializationHash] as any : undefined;
  const candidate = evidence.candidateHash ? context.facts[evidence.candidateHash] as any : undefined;
  const observationIndex = (state.evidenceIndex as any).environmentObservations ?? {};
  const materializationIndex = (state.evidenceIndex as any).workspaceMaterializations ?? {};
  const attemptObservationHashes = Object.values(observationIndex).map((reference: any) => reference.hash).filter((candidateHash) => (context.facts[candidateHash] as any)?.stageAttemptId === attempt.stageAttemptId).sort();
  if (observation?.kind !== "environment_observation" || observation.hash !== hash || observation.hash !== hashWithoutField(observation, "hash") || observationIndex[hash]?.hash !== hash || !sameStrings(attemptObservationHashes, [hash])) return false;
  if (observation.planHash !== state.identity.planHash || observation.runId !== state.runId || observation.runNonce !== state.runNonce || observation.workItemId !== item.workItemId || observation.stage !== attempt.stage || observation.stageAttemptId !== attempt.stageAttemptId || observation.attemptInputHash !== attempt.attemptInput.hash || observation.repositoryId !== item.writeRepositoryId || observation.candidateGeneration !== evidence.candidateGeneration || observation.candidateHash !== evidence.candidateHash || observation.environmentProfileHash !== evidence.environmentProfileHash) return false;
  if (candidate?.kind !== "candidate" || canonicalHash(observation.candidateTree) !== canonicalHash(candidate.git)) return false;
  const attemptMaterializationHashes = Object.values(materializationIndex).map((reference: any) => reference.hash).filter((candidateHash) => (context.facts[candidateHash] as any)?.stageAttemptId === attempt.stageAttemptId).sort();
  if (materialization?.kind !== "workspace_materialization" || materialization.hash !== observation.workspaceMaterializationHash || materialization.hash !== hashWithoutField(materialization, "hash") || materializationIndex[materialization.hash]?.hash !== materialization.hash || !sameStrings(attemptMaterializationHashes, [materialization.hash])) return false;
  if (materialization.planHash !== state.identity.planHash || materialization.runId !== state.runId || materialization.runNonce !== state.runNonce || materialization.workItemId !== item.workItemId || materialization.stageAttemptId !== attempt.stageAttemptId || materialization.repositoryId !== item.writeRepositoryId || materialization.candidateGeneration !== evidence.candidateGeneration || materialization.candidateHash !== evidence.candidateHash || canonicalHash(materialization.candidateTree) !== canonicalHash(candidate.git)) return false;
  if (materialization.commonDirIdentityHash !== observation.commonDirIdentityHash || materialization.worktreeIdentityHash !== observation.worktreeIdentityHash || utcTimestampOrderValue(materialization.materializedAt) < utcTimestampOrderValue(attempt.createdAt) || utcTimestampOrderValue(observation.observedAt) < utcTimestampOrderValue(materialization.materializedAt) || utcTimestampOrderValue(observation.observedAt) > utcTimestampOrderValue(evidence.producedAt) || utcTimestampOrderValue(evidence.producedAt) > utcTimestampOrderValue(attempt.terminalAt)) return false;
  // The repository workspace projection is only the latest operational observation. Historical
  // stage authority is the immutable attempt-bound materialization/observation pair above.
  return true;
}

function validateStageEnvironmentAuthority(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, stage: typeof PLAN_STAGE_IDS[number], attempt: any, evidence: any, path: string, issues: ValidationIssue[]): void {
  if (!["F2", "F5", "F7"].includes(stage)) return;
  const attemptInput = context.facts[attempt.attemptInput.hash] as any;
  pushIssue(issues, `${path}/currentEvidence`, attemptInput?.kind === "stage_attempt_input" && evidence.candidateGeneration === attempt.inputGeneration && evidence.candidateHash === attemptInput.candidateHash, `${stage} environment authority must bind the exact attempt input candidate generation and hash`);
  pushIssue(issues, `${path}/currentEvidence`, exactEnvironmentObservation(state, context, item, attempt, evidence), `${stage} requires exact immutable candidate-tree/profile/worktree/materialization/attempt environment evidence`);
  if (["F2", "F5"].includes(stage)) {
    pushIssue(issues, `${path}/currentEvidence`, evidence.readOnly, `${stage} must use a read-only procedure; disposition cannot bypass output closure`);
    const result = attempt.workerResult ? context.facts[attempt.workerResult.hash] as any : undefined;
    pushIssue(issues, `${path}/currentEvidence`, exactReadOnlyWorkerGitBoundary(state, context, attempt, result), `${stage} worker output must preserve the exact immutable candidate and materialized common-dir/worktree identities`);
  }
  if (stage === "F7") {
    const environment = typeof evidence.environmentObservationHash === "string" ? context.facts[evidence.environmentObservationHash] as any : undefined;
    pushIssue(issues, `${path}/currentEvidence`, evidence.readOnly && environment?.kind === "environment_observation" && environment.cleanliness === "clean", "F7 requires read-only fresh clean exact-tree environment authority");
  }
  if (["failed", "blocked", "budget_exhausted"].includes(item.stages[stage].state)) {
    const observation = typeof evidence.environmentObservationHash === "string" ? context.facts[evidence.environmentObservationHash] as any : undefined;
    const workspace = state.repositories[item.writeRepositoryId]?.workspace;
    pushIssue(issues, `${path}/currentEvidence`, observation?.kind === "environment_observation" && workspace?.gitCommonDirIdentityHash === observation.commonDirIdentityHash && workspace?.gitWorktreeIdentityHash === observation.worktreeIdentityHash && workspace?.expectedHead !== null && canonicalHash(workspace.expectedHead) === canonicalHash(observation.candidateTree) && workspace?.observationReceipt === observation.workspaceMaterializationHash, `${stage} terminal non-PASS projection must retain its exact latest materialization identity`);
  }
}

function exactStageClosureHashes(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, attempt: any): { findings: string[]; effects: string[]; effectsExact: boolean } {
  const sealRevision = exactStageAttemptSealRevision(state, attempt);
  const findings = Object.values(state.findingClosures)
    .filter((finding) => finding.workItemId === item.workItemId && finding.stageAttemptId === attempt.stageAttemptId)
    .filter((finding) => {
      if (sealRevision === null) return true;
      const slotId = canonicalHash({ type: "record_finding", naturalIdentity: finding.findingId });
      return (state.idempotencySlots[slotId]?.appliedRevision ?? Number.MAX_SAFE_INTEGER) < sealRevision;
    })
    .map(({ findingHash }) => findingHash).sort();
  const applicableEffects = Object.values(state.effects)
    .filter((effect) => effect.subject.kind === "work_item" && effect.subject.id === item.workItemId && effect.boundStageAttemptId === attempt.stageAttemptId && !["launch_worker", "cancel_worker", "materialize_workspace", "cleanup_worktree"].includes(effect.kind));
  const effects = applicableEffects.map(({ observationHash }) => observationHash).filter((hash): hash is string => typeof hash === "string").sort();
  const evidence = attempt.evidence ? context.facts[attempt.evidence.hash] as any : undefined;
  const effectsExact = applicableEffects.every((effect) => {
    if (effect.state !== "reconciled" || !["applied_exact", "compensated", "proven_absent"].includes(effect.reconciliation) || typeof effect.observationHash !== "string") return false;
    const fact = context.facts[effect.observationHash] as any;
    const reference = state.evidenceIndex.effectReconciliations[effect.effectId] as any;
    const closedAt = utcTimestampOrderValue(fact?.closedAt);
    const execution = effect.executionObservationHash ? context.facts[effect.executionObservationHash] as any : null;
    const executionCompletedAt = execution?.completedAt === undefined ? null : utcTimestampOrderValue(execution.completedAt);
    const temporal = Number.isFinite(closedAt) && closedAt >= utcTimestampOrderValue(effect.createdAt) && (effect.lastDispatchAt === null || closedAt >= utcTimestampOrderValue(effect.lastDispatchAt)) && (executionCompletedAt === null || Number.isFinite(executionCompletedAt) && closedAt >= executionCompletedAt) && closedAt <= utcTimestampOrderValue(state.updatedAt) && closedAt <= utcTimestampOrderValue(evidence?.producedAt) && closedAt <= utcTimestampOrderValue(attempt.terminalAt);
    return fact?.kind === "effect_reconciliation" && fact.hash === effect.observationHash && fact.hash === hashWithoutField(fact, "hash")
      && fact.planHash === state.identity.planHash && fact.runId === state.runId && fact.runNonce === state.runNonce
      && fact.effectId === effect.effectId && fact.requestHash === effect.requestHash && fact.reconciliation === effect.reconciliation
      && reference?.kind === "effect_reconciliation" && reference.id === effect.effectId && reference.hash === fact.hash && temporal;
  });
  return { findings, effects, effectsExact };
}

/** Canonical aggregate precedence: BUDGET_EXHAUSTED > BLOCKED > FAIL > PASS. */
export function deriveStageAggregateDispositionV1(workerTerminalStatus: string | null, checkDispositions: readonly string[], assertionDispositions: readonly string[]): "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED" {
  const components = [...checkDispositions, ...assertionDispositions].filter((value) => !["WAIVED", "NOT_APPLICABLE"].includes(value));
  if (workerTerminalStatus === "needs_attention" || workerTerminalStatus === "failed") components.push("FAIL");
  else if (workerTerminalStatus === "lost" || workerTerminalStatus === "cancelled") components.push("BLOCKED");
  else if (workerTerminalStatus !== null && workerTerminalStatus !== "succeeded") components.push("BLOCKED");
  if (components.includes("BUDGET_EXHAUSTED")) return "BUDGET_EXHAUSTED";
  if (components.includes("BLOCKED")) return "BLOCKED";
  if (components.includes("FAIL")) return "FAIL";
  return "PASS";
}

function exactCheckExecution(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, attempt: any, evidence: any, result: any): boolean {
  const hash = result?.executionEvidenceHash;
  if (typeof hash !== "string") return false;
  const execution = context.facts[hash] as any;
  const index = (state.evidenceIndex as any).checkExecutions ?? {};
  const executionDisposition = ["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"].includes(result.disposition);
  return Boolean(executionDisposition && index[hash]?.hash === hash && execution?.kind === "check_execution" && execution.hash === hash && execution.hash === hashWithoutField(execution, "hash") && execution.planHash === state.identity.planHash && execution.runId === state.runId && execution.runNonce === state.runNonce && execution.authorizationSetHash === state.identity.authorizationSet.hash && execution.workItemId === item.workItemId && execution.stage === attempt.stage && execution.stageAttemptId === attempt.stageAttemptId && execution.attemptInputHash === attempt.attemptInput.hash && execution.candidateGeneration === evidence.candidateGeneration && execution.candidateHash === evidence.candidateHash && execution.checkId === result.checkId && execution.procedureHash === evidence.procedureHash && execution.environmentProfileHash === evidence.environmentProfileHash && execution.environmentObservationHash === (evidence.environmentObservationHash ?? null) && execution.disposition === result.disposition && utcTimestampOrderValue(execution.startedAt) >= utcTimestampOrderValue(attempt.createdAt) && utcTimestampOrderValue(execution.completedAt) >= utcTimestampOrderValue(execution.startedAt) && utcTimestampOrderValue(execution.completedAt) <= utcTimestampOrderValue(evidence.producedAt) && utcTimestampOrderValue(evidence.producedAt) <= utcTimestampOrderValue(attempt.terminalAt));
}

function validateNonPassStage(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, stage: typeof PLAN_STAGE_IDS[number], projection: any, itemPath: string, issues: ValidationIssue[]): void {
  const path = `${itemPath}/stages/${stage}`;
  const attempt = projection.currentAttemptId ? state.stageAttempts[projection.currentAttemptId] : undefined;
  const evidence = projection.currentEvidence ? context.facts[projection.currentEvidence] as any : undefined;
  const aggregate = evidence?.kind === "stage_evidence" ? context.facts[evidence.checkAggregateHash] as any : undefined;
  const expectedDisposition = projection.state === "budget_exhausted" ? "BUDGET_EXHAUSTED" : projection.state === "blocked" ? "BLOCKED" : "FAIL";
  pushIssue(issues, `${path}/currentAttemptId`, Boolean(attempt && attempt.state === "sealed" && attempt.terminalAt !== null && attempt.evidence?.hash === projection.currentEvidence), "terminal non-PASS stage requires its exact sealed attempt and evidence");
  pushIssue(issues, `${path}/currentEvidence`, Boolean(evidence?.kind === "stage_evidence" && evidence.hash === projection.currentEvidence && evidence.hash === hashWithoutField(evidence as Record<string, unknown>, "hash") && evidence.planHash === state.identity.planHash && evidence.runId === state.runId && evidence.runNonce === state.runNonce && evidence.authorizationSetHash === state.identity.authorizationSet.hash && evidence.workItemId === item.workItemId && evidence.stage === stage && evidence.stageAttemptId === attempt?.stageAttemptId && evidence.attemptInputHash === attempt?.attemptInput.hash && evidence.disposition === expectedDisposition && evidence.producerKind === attempt?.producerKind && evidence.producerResultHash === (attempt?.workerResult?.hash ?? null) && utcTimestampOrderValue(evidence.producedAt) >= utcTimestampOrderValue(attempt?.createdAt) && utcTimestampOrderValue(evidence.producedAt) <= utcTimestampOrderValue(attempt?.terminalAt)), "terminal non-PASS evidence must bind exact plan/run/item/stage/attempt/producer disposition and production time");
  pushIssue(issues, `${path}/currentEvidence`, Boolean(aggregate?.kind === "check_aggregate" && aggregate.hash === evidence?.checkAggregateHash && aggregate.hash === hashWithoutField(aggregate as Record<string, unknown>, "hash") && aggregate.planHash === state.identity.planHash && aggregate.runId === state.runId && aggregate.runNonce === state.runNonce && aggregate.authorizationSetHash === state.identity.authorizationSet.hash && aggregate.workItemId === item.workItemId && aggregate.stage === stage && aggregate.stageAttemptId === attempt?.stageAttemptId && aggregate.attemptInputHash === attempt?.attemptInput.hash && aggregate.procedureHash === evidence?.procedureHash && aggregate.environmentProfileHash === evidence?.environmentProfileHash && aggregate.disposition === expectedDisposition && state.evidenceIndex.checkAggregates[aggregate.hash]?.hash === aggregate.hash), "terminal non-PASS aggregate must be the exact indexed immutable attempt-bound authority");
  pushIssue(issues, `${path}/currentEvidence`, Boolean(attempt && exactLifecycleProcedureCatalogBindingV1(context, evidence, stage, attempt.producerKind)), "terminal non-PASS procedure must resolve through the exact canonical lifecycle catalog producer/environment/executable contract");
  if (!attempt || !aggregate) return;
  validateStageEnvironmentAuthority(state, context, item, stage, attempt, evidence, path, issues);
  const resultFact = attempt.workerResult ? context.facts[attempt.workerResult.hash] as any : undefined;
  const derivedDisposition = deriveStageAggregateDispositionV1(attempt.producerKind === "owned_worker" ? resultFact?.terminalStatus ?? null : null, aggregate.checks.map(({ disposition }: any) => disposition), aggregate.assertions.map(({ evidenceHash }: any) => (context.facts[evidenceHash] as any)?.disposition));
  pushIssue(issues, `${path}/currentEvidence`, aggregate.disposition === derivedDisposition, "aggregate disposition must equal the sole canonical worker/check/assertion precedence derivation");
  const closure = exactStageClosureHashes(state, context, item, attempt);
  pushIssue(issues, `${path}/currentEvidence`, closure.effectsExact && sameStrings([...evidence.findingHashes], closure.findings) && sameStrings([...evidence.effectReconciliationHashes], closure.effects), "non-PASS stage evidence closure must exactly equal canonical terminal current finding/effect facts");
  if (attempt.producerKind === "owned_worker") {
    const result = attempt.workerResult ? context.facts[attempt.workerResult.hash] as any : undefined;
    pushIssue(issues, `${path}/currentEvidence`, result?.kind === "worker_result" && ["succeeded", "needs_attention", "failed", "cancelled", "lost"].includes(result.terminalStatus), "owned-worker non-PASS disposition requires one exact terminal result; canonical worker/check/assertion precedence determines the disposition");
    if (result?.terminalStatus === "cancelled") {
      const binding = state.workerBindings[attempt.stageAttemptId];
      const activeCancellation = Object.values(state.cancellations).some((cancellation) => cancellation.state !== "closed" && Object.prototype.hasOwnProperty.call(cancellation.fencedGenerations, attempt.workItemId));
      pushIssue(issues, `${path}/currentEvidence`, Boolean(binding && binding.resultHash === result.hash && derivedDisposition === "BLOCKED"), "cancelled non-PASS authority requires the exact bound terminal result and canonical BLOCKED derivation");
      pushIssue(issues, `${path}/currentEvidence`, !activeCancellation, "active conductor cancellation must close through record_cancellation rather than stage sealing");
    }
  }
  const planItem = context.plan.workItems.find(({ workItemId }) => workItemId === item.workItemId);
  const applicableChecks = planItem?.checks.filter(({ phases }) => phases.includes(stage)) ?? [];
  const requiredAssertions = stage === "F2" ? (planItem?.oracleIds ?? []).flatMap((oracleId) => (context.plan.acceptanceOracles.find((oracle) => oracle.oracleId === oracleId)?.assertions ?? []).map((assertion) => ({ oracleId, assertion }))) : [];
  pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.checks.map(({ checkId }: any) => checkId).sort(), applicableChecks.map(({ checkId }) => checkId).sort()), "non-PASS aggregate must cover exact applicable stage checks");
  pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.assertions.map(({ oracleId, assertionId }: any) => `${oracleId}/${assertionId}`).sort(), requiredAssertions.map(({ oracleId, assertion }) => `${oracleId}/${assertion.assertionId}`).sort()), "non-PASS F2 aggregate must cover exact oracle assertions");
  for (const result of aggregate.assertions) {
    const assertion = context.facts[result.evidenceHash] as any;
    pushIssue(issues, `${path}/currentEvidence`, assertion?.kind === "oracle_assertion" && assertion.planHash === state.identity.planHash && assertion.runId === state.runId && assertion.runNonce === state.runNonce && assertion.authorizationSetHash === state.identity.authorizationSet.hash && assertion.workItemId === item.workItemId && assertion.stageAttemptId === attempt.stageAttemptId && assertion.attemptInputHash === attempt.attemptInput.hash && assertion.oracleId === result.oracleId && assertion.assertionId === result.assertionId && state.evidenceIndex.oracleAssertions[result.evidenceHash]?.hash === result.evidenceHash, "non-PASS oracle support must bind the exact attempt and oracle assertion");
  }
  for (const result of aggregate.checks) {
    const check = applicableChecks.find(({ checkId }) => checkId === result.checkId);
    if (check?.applicability === "required") pushIssue(issues, `${path}/currentEvidence`, result.applicabilityEvidenceHashes.length === 0, `required check ${result.checkId} cannot use unrelated applicability support`);
    if (["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"].includes(result.disposition)) pushIssue(issues, `${path}/currentEvidence`, exactCheckExecution(state, context, item, attempt, evidence, result), `check ${result.checkId} must cite exact immutable stage/attempt/input/check/procedure/environment execution evidence`);
    else pushIssue(issues, `${path}/currentEvidence`, (result.executionEvidenceHash ?? null) === null, `authority-only check ${result.checkId} cannot cite execution evidence`);
    for (const hash of result.applicabilityEvidenceHashes) {
      const disposition = context.facts[hash] as any;
      pushIssue(issues, `${path}/currentEvidence`, disposition?.kind === "check_disposition" && disposition.planHash === state.identity.planHash && disposition.runId === state.runId && disposition.runNonce === state.runNonce && disposition.authorizationSetHash === state.identity.authorizationSet.hash && disposition.workItemId === item.workItemId && disposition.stage === stage && disposition.stageAttemptId === attempt.stageAttemptId && disposition.attemptInputHash === attempt.attemptInput.hash && disposition.checkId === result.checkId && disposition.disposition === result.disposition && disposition.predicateHash === (check?.condition?.contentHash ?? null) && state.evidenceIndex.checkDispositions[hash]?.hash === hash, "non-PASS check support must bind the exact attempt and check semantics");
    }
  }
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
  pushIssue(issues, `${path}/currentEvidence`, utcTimestampOrderValue(fact.producedAt) >= utcTimestampOrderValue(attempt.createdAt) && utcTimestampOrderValue(fact.producedAt) <= utcTimestampOrderValue(attempt.terminalAt), "stage evidence production time must lie within its exact attempt");
  pushIssue(issues, `${path}/currentEvidence`, fact.workItemId === item.workItemId && fact.stage === stage && fact.stageAttemptId === attempt.stageAttemptId, "fact must bind exact work item, stage, and attempt");
  pushIssue(issues, `${path}/currentEvidence`, fact.attemptInputHash === attempt.attemptInput.hash && fact.authorizationSetHash === attempt.authorizationSetHash && fact.authorizationSetHash === state.identity.authorizationSet.hash, "fact must bind exact attempt input and authorization set");
  const aggregate = context.facts[fact.checkAggregateHash];
  const aggregateRef = state.evidenceIndex.checkAggregates[fact.checkAggregateHash];
  pushIssue(issues, `${path}/currentEvidence`, exactLifecycleProcedureCatalogBindingV1(context, fact, stage, attempt.producerKind), "procedure and environment must resolve through the exact canonical lifecycle catalog producer/environment/executable contract");
  pushIssue(issues, `${path}/currentEvidence`, Boolean(aggregateRef?.kind === "check_aggregate" && aggregateRef.hash === fact.checkAggregateHash && aggregate?.kind === "check_aggregate" && aggregate.hash === fact.checkAggregateHash && aggregate.hash === hashWithoutField(aggregate as Record<string, unknown>, "hash") && aggregate.planHash === state.identity.planHash && aggregate.runId === state.runId && aggregate.runNonce === state.runNonce && aggregate.authorizationSetHash === state.identity.authorizationSet.hash && aggregate.workItemId === item.workItemId && aggregate.stage === stage && aggregate.stageAttemptId === attempt.stageAttemptId && aggregate.attemptInputHash === attempt.attemptInput.hash && aggregate.procedureHash === fact.procedureHash && aggregate.environmentProfileHash === fact.environmentProfileHash && aggregate.disposition === "PASS"), "check aggregate must resolve through the exact indexed immutable attempt-bound fact");
  const planItem = context.plan.workItems.find(({ workItemId }) => workItemId === item.workItemId);
  if (aggregate && planItem) {
    pushIssue(issues, `${path}/currentEvidence`, sameStrings([...aggregate.oracleIds], [...planItem.oracleIds]), "check aggregate must cover every work-item acceptance oracle exactly");
    const requiredAssertions = planItem.oracleIds.flatMap((oracleId) => (context.plan.acceptanceOracles.find((oracle) => oracle.oracleId === oracleId)?.assertions ?? []).map((assertion) => ({ oracleId, assertion })));
    const expectedAssertionKeys = stage === "F2" ? requiredAssertions.map(({ oracleId, assertion }) => `${oracleId}/${assertion.assertionId}`).sort() : [];
    pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.assertions.map(({ oracleId, assertionId }) => `${oracleId}/${assertionId}`).sort(), expectedAssertionKeys), "F2 must prove every exact oracle assertion; other stages cannot claim assertion execution");
    for (const result of aggregate.assertions) {
      const expected = requiredAssertions.find(({ oracleId, assertion }) => oracleId === result.oracleId && assertion.assertionId === result.assertionId);
      const assertionFact = context.facts[result.evidenceHash];
      pushIssue(issues, `${path}/currentEvidence`, stage === "F2" && Boolean(expected) && state.evidenceIndex.oracleAssertions[result.evidenceHash]?.hash === result.evidenceHash && assertionFact?.kind === "oracle_assertion" && assertionFact.planHash === state.identity.planHash && assertionFact.runId === state.runId && assertionFact.runNonce === state.runNonce && assertionFact.workItemId === item.workItemId && assertionFact.stage === "F2" && assertionFact.stageAttemptId === attempt.stageAttemptId && assertionFact.attemptInputHash === attempt.attemptInput.hash && assertionFact.authorizationSetHash === state.identity.authorizationSet.hash && assertionFact.oracleId === result.oracleId && assertionFact.assertionId === result.assertionId && assertionFact.procedureId === expected?.assertion.procedureId && assertionFact.environmentProfileId === expected?.assertion.environmentProfileId && assertionFact.observationMethod === expected?.assertion.observationMethod && assertionFact.requiredEvidenceClass === expected?.assertion.requiredEvidenceClass && assertionFact.disposition === "PASS" && assertionFact.observationHash === fact.producerResultHash && Object.entries(state.evidenceIndex).some(([indexName, index]) => indexName !== "oracleAssertions" && Object.values(index as Record<string, { hash: string }>).some((ref) => ref.hash === assertionFact.observationHash)), `oracle assertion ${result.oracleId}/${result.assertionId} must resolve to exact procedure, environment, method, evidence class, and indexed observation`);
    }
    const applicableChecks = planItem.checks.filter(({ phases }) => phases.includes(stage));
    pushIssue(issues, `${path}/currentEvidence`, sameStrings(aggregate.checks.map(({ checkId }) => checkId).sort(), applicableChecks.map(({ checkId }) => checkId).sort()), "check aggregate must cover exactly the checks applicable to this stage");
    for (const check of applicableChecks) {
      const result = aggregate.checks.find(({ checkId }) => checkId === check.checkId);
      pushIssue(issues, `${path}/currentEvidence`, Boolean(result), `check aggregate is missing applicable check ${check.checkId}`);
      if (!result) continue;
      if (check.applicability === "required") pushIssue(issues, `${path}/currentEvidence`, !["WAIVED", "NOT_APPLICABLE"].includes(result.disposition) && result.applicabilityEvidenceHashes.length === 0, `required check ${check.checkId} requires only exact execution evidence and cannot carry waiver/applicability support`);
      if (check.applicability === "not_applicable") pushIssue(issues, `${path}/currentEvidence`, result.disposition === "NOT_APPLICABLE", `statically not-applicable check ${check.checkId} requires an exact NOT_APPLICABLE disposition`);
      if (["PASS", "FAIL", "BLOCKED", "BUDGET_EXHAUSTED"].includes(result.disposition)) pushIssue(issues, `${path}/currentEvidence`, exactCheckExecution(state, context, item, attempt, fact, result), `check ${check.checkId} must cite exact immutable stage/attempt/input/check/procedure/environment execution evidence`);
      else pushIssue(issues, `${path}/currentEvidence`, (result.executionEvidenceHash ?? null) === null, `waiver/not-applicable authority for ${check.checkId} cannot masquerade as check execution`);
      if (result.disposition !== "PASS" || check.applicability !== "required") {
        pushIssue(issues, `${path}/currentEvidence`, result.applicabilityEvidenceHashes.length > 0 && isSortedUnique([...result.applicabilityEvidenceHashes]), `waived/conditional/not-applicable check ${check.checkId} requires sorted immutable evidence`);
        for (const dispositionHash of result.applicabilityEvidenceHashes) {
          const dispositionFact = context.facts[dispositionHash];
          const predicateHash = check.condition?.contentHash ?? null;
          pushIssue(issues, `${path}/currentEvidence`, state.evidenceIndex.checkDispositions[dispositionHash]?.hash === dispositionHash && dispositionFact?.kind === "check_disposition" && dispositionFact.planHash === state.identity.planHash && dispositionFact.runId === state.runId && dispositionFact.runNonce === state.runNonce && dispositionFact.workItemId === item.workItemId && dispositionFact.stage === stage && dispositionFact.stageAttemptId === attempt.stageAttemptId && dispositionFact.attemptInputHash === attempt.attemptInput.hash && dispositionFact.checkId === check.checkId && dispositionFact.disposition === result.disposition && dispositionFact.predicateHash === predicateHash && dispositionFact.authorizationSetHash === state.identity.authorizationSet.hash, `check disposition ${dispositionHash} must be an exact indexed, predicate-bound, authorized fact`);
          if (dispositionFact?.kind === "check_disposition") {
            const authoritiesExact = isSortedUnique([...dispositionFact.evidenceHashes]) && dispositionFact.evidenceHashes.length > 0 && dispositionFact.evidenceHashes.every((hash) => {
              const authority = context.facts[hash] as any;
              const common = authority?.hash === hash && authority.hash === hashWithoutField(authority, "hash") && authority.planHash === state.identity.planHash && authority.runId === state.runId && authority.runNonce === state.runNonce && authority.authorizationSetHash === state.identity.authorizationSet.hash && authority.workItemId === item.workItemId && authority.stage === stage && authority.stageAttemptId === attempt.stageAttemptId && authority.attemptInputHash === attempt.attemptInput.hash && authority.checkId === check.checkId && authority.predicateHash === predicateHash;
              if (result.disposition === "WAIVED") return common && authority.kind === "waiver" && authority.issuedBy === "user" && Object.values(state.evidenceIndex.waivers).some((ref) => ref.hash === hash);
              return common && authority.kind === "check_applicability" && authority.applicable === (result.disposition !== "NOT_APPLICABLE") && Object.values((state.evidenceIndex as any).checkApplicabilities ?? {}).some((ref: any) => ref.hash === hash);
            });
            pushIssue(issues, `${path}/currentEvidence`, authoritiesExact, `check disposition ${dispositionHash} must cite only exact attempt/predicate-bound waiver or applicability authority`);
          }
        }
      }
    }
  }
  const closure = exactStageClosureHashes(state, context, item, attempt);
  pushIssue(issues, `${path}/currentEvidence`, isSortedUnique([...fact.findingHashes]) && sameStrings([...fact.findingHashes], closure.findings), "finding hashes must exactly equal sorted applicable current attempt findings");
  pushIssue(issues, `${path}/currentEvidence`, closure.effectsExact && isSortedUnique([...fact.effectReconciliationHashes]) && sameStrings([...fact.effectReconciliationHashes], closure.effects), "effect-reconciliation hashes must exactly equal canonical terminal sorted applicable current attempt effects");
  fact.findingHashes.forEach((hash) => pushIssue(issues, `${path}/currentEvidence`, Object.values(state.evidenceIndex.findings).some((ref) => ref.hash === hash), `finding ${hash} must be indexed`));
  fact.effectReconciliationHashes.forEach((hash) => pushIssue(issues, `${path}/currentEvidence`, Object.values(state.evidenceIndex.effectReconciliations).some((ref) => ref.hash === hash), `effect reconciliation ${hash} must be indexed`));
  pushIssue(issues, `${path}/currentEvidence`, fact.disposition === "PASS", "fact disposition must be PASS");
  pushIssue(issues, `${path}/currentEvidence`, fixedStageProducers(stage).includes(fact.producerKind) && attempt.producerKind === fact.producerKind, "fact and attempt must use the fixed stage producer kind");
  const aggregateResult = aggregate as any;
  const aggregateWorkerResult = fact.producerResultHash ? context.facts[fact.producerResultHash] as any : undefined;
  pushIssue(issues, `${path}/currentEvidence`, deriveStageAggregateDispositionV1(fact.producerKind === "owned_worker" ? aggregateWorkerResult?.terminalStatus ?? null : null, aggregateResult?.checks?.map(({ disposition }: any) => disposition) ?? [], aggregateResult?.assertions?.map(({ evidenceHash }: any) => (context.facts[evidenceHash] as any)?.disposition) ?? []) === "PASS", "PASS must be the sole canonical worker/check/assertion precedence derivation");
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
    const exactF3TransitionBoundary = f3Attempt?.stage === "F3" && f3Attempt.inputGeneration === adoption?.fromCandidateGeneration && f3Attempt.reservedOutputGeneration === adoption?.toCandidateGeneration && ((f3Attempt.state === "sealed" && item.stages.F3.state === "passed") || (["result_observed", "evidence_pending"].includes(f3Attempt.state) && item.stages.F3.state === "active" && item.stages.F3.currentAttemptId === f3Attempt.stageAttemptId));
    pushIssue(issues, `${path}/adoptionReceipt`, stage === "F2" && adoption?.kind === "adoption" && adoption.planHash === state.identity.planHash && adoption.runId === state.runId && adoption.runNonce === state.runNonce && adoption.workItemId === item.workItemId && adoption.stage === "F2" && adoption.fromCandidateGeneration === fact.candidateGeneration && adoption.fromCandidateHash === fact.candidateHash && adoption.toCandidateGeneration === item.candidate.generation && adoption.toCandidateGeneration === adoption.fromCandidateGeneration + 1 && adoption.toCandidateHash === item.candidate.candidateHash && adoption.f3StageAttemptId === item.candidate.producedByStageAttemptId && exactF3TransitionBoundary && adoption.evidenceHash === projection.currentEvidence && adoption.sourceEvidenceProcedureHash === fact.procedureHash && deltaProcedure?.purpose === "evidence_only_delta_attestation" && deltaProcedure.stages.includes("F3") && deltaProcedure.readOnly && deltaProcedure.environmentProfileHash === adoption.environmentProfileHash && adoption.environmentProfileHash === fact.environmentProfileHash && adoption.evidenceOnlyDelta === true && exactAdoptionProcedureExecution(state, context, item, adoption, f3Attempt, deltaProcedure), "candidate mismatch requires the sole atomic post-F3 generation transition and exact executable PASS delta attestation");
  } else pushIssue(issues, `${path}/currentEvidence`, item.candidate !== null && fact.candidateHash === item.candidate.candidateHash, "fact must bind the current candidate hash");
  validateStageEnvironmentAuthority(state, context, item, stage, attempt, fact, path, issues);
  if (stage === "F4") pushIssue(issues, `${path}/currentEvidence`, fact.readOnly, "F4 must be read-only");
  if (attempt.producerKind === "owned_worker") {
    pushIssue(issues, `${path}/currentAttemptId`, attempt.workerResult !== null, "owned-worker stage requires an exact observed worker result");
    pushIssue(issues, `${path}/currentAttemptId`, Boolean(state.workerBindings[attempt.stageAttemptId]), "owned-worker stage requires an exact worker binding");
  }
}
function exactAdoptionProcedureExecution(state: DagRunStateV1, context: DagRunValidationContextV1, item: any, adoption: any, attempt: any, procedure: any): boolean {
  const hash = adoption?.deltaAttestationExecutionHash;
  const execution = typeof hash === "string" ? context.facts[hash] as any : null;
  const index = (state.evidenceIndex as any).procedureExecutions ?? {};
  const executable = procedure?.executable;
  const stageEvidence = attempt?.evidence ? context.facts[attempt.evidence.hash] as any : null;
  const executionUpperBound = stageEvidence?.kind === "stage_evidence" ? stageEvidence.producedAt : attempt?.terminalAt ?? attempt?.updatedAt;
  return Boolean(execution?.kind === "procedure_execution" && execution.hash === hash && execution.hash === hashWithoutField(execution, "hash") && index[hash]?.hash === hash && execution.planHash === state.identity.planHash && execution.runId === state.runId && execution.runNonce === state.runNonce && execution.authorizationSetHash === state.identity.authorizationSet.hash && execution.workItemId === item.workItemId && execution.stage === "F3" && execution.stageAttemptId === attempt?.stageAttemptId && execution.attemptInputHash === attempt?.attemptInput.hash && execution.fromCandidateGeneration === adoption.fromCandidateGeneration && execution.fromCandidateHash === adoption.fromCandidateHash && execution.toCandidateGeneration === adoption.toCandidateGeneration && execution.toCandidateHash === adoption.toCandidateHash && execution.procedureHash === adoption.deltaAttestationProcedureHash && execution.environmentProfileHash === adoption.environmentProfileHash && execution.executableArtifactHash === executable?.executableArtifactHash && execution.environmentHash === executable?.environmentHash && execution.disposition === "PASS" && utcTimestampOrderValue(execution.startedAt) >= utcTimestampOrderValue(attempt?.createdAt) && utcTimestampOrderValue(execution.completedAt) >= utcTimestampOrderValue(execution.startedAt) && utcTimestampOrderValue(execution.occurredAt) >= utcTimestampOrderValue(execution.completedAt) && execution.occurredAt === adoption.occurredAt && utcTimestampOrderValue(execution.occurredAt) <= utcTimestampOrderValue(executionUpperBound) && (!attempt?.terminalAt || utcTimestampOrderValue(executionUpperBound) <= utcTimestampOrderValue(attempt.terminalAt)));
}

function validateCatalogJoin(state: DagRunStateV1, context: DagRunValidationContextV1, issues: ValidationIssue[]): void {
  const catalog = context.catalog;
  pushIssue(issues, "/catalog/lifecycleProfileHash", catalog.lifecycleProfileHash === state.identity.lifecycleProfileHash && catalog.lifecycleProfileHash === canonicalHash(Object.values(catalog.procedures).sort((left, right) => left.procedureId.localeCompare(right.procedureId))), "must be the canonical hash of the exact run lifecycle procedure catalog");
  const expectedCheckCatalogHash = canonicalHash(context.plan.workItems.map(({ workItemId, checks }) => ({ workItemId, checks })));
  pushIssue(issues, "/catalog/checkCatalogHash", catalog.checkCatalogHash === state.identity.checkCatalogHash && catalog.checkCatalogHash === expectedCheckCatalogHash, "must be the canonical hash of the plan's exact check-applicability catalog");
  for (const [hash, procedure] of Object.entries(catalog.procedures)) {
    pushIssue(issues, `/catalog/procedures/${hash}/hash`, procedure.hash === hash && procedure.hash === hashWithoutField(procedure as unknown as Record<string, unknown>, "hash"), "procedure key and hash must equal canonical procedure content, including executable mapping");
    pushIssue(issues, `/catalog/procedures/${hash}/stages`, isSortedUnique([...procedure.stages]), "procedure stages must be sorted and deduplicated");
    const executable = procedure.executable;
    pushIssue(issues, `/catalog/procedures/${hash}/executable`, Boolean(executable && executable.argv.length > 0 && executable.environmentProfileHash === procedure.environmentProfileHash && executable.readOnly === procedure.readOnly && executable.noEdit === procedure.readOnly && executable.timeoutMs > 0), "every procedure must fail closed without exact artifact/argv/cwd/environment/timeout/read-only/no-edit mapping");
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
    stageAttemptInputs: "stage_attempt_input", workerResults: "worker_result", candidates: "candidate", stageEvidence: "stage_evidence", checkAggregates: "check_aggregate",
    checkExecutions: "check_execution", procedureExecutions: "procedure_execution", findingCorrections: "finding_correction", checkApplicabilities: "check_applicability", environmentObservations: "environment_observation", workspaceMaterializations: "workspace_materialization",
    checkDispositions: "check_disposition", verifications: "verification", oracleAssertions: "oracle_assertion", findings: "finding", findingResolutions: "finding_resolution", waivers: "waiver", invalidations: "invalidation", adoptions: "adoption",
    effectReconciliations: "effect_reconciliation", integrationReady: "integration_ready", integrationReceipts: "integration",
    stalenessReceipts: "staleness", gateReceipts: "gate_release",
  };
  const hashKeyed = new Set(["workerResults", "candidates", "stageEvidence", "checkAggregates", "checkExecutions", "procedureExecutions", "findingCorrections", "checkApplicabilities", "environmentObservations", "workspaceMaterializations", "stalenessReceipts"]);
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
