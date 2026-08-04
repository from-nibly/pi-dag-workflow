import { Type, type Static } from "typebox";
import {
  ArtifactRefV1Schema,
  BoundedTextSchema,
  GitTreeRefV1Schema,
  HashSchema,
  IdSchema,
  NonNegativeIntegerSchema,
  Nullable,
  PositiveIntegerSchema,
  RootRelativePathSchema,
  StrictObject,
  StringSet,
  TimestampSchema,
  canonicalHash,
  contentHashMatches,
  hashWithoutField,
  isSortedUnique,
  parseStrictJson,
  pushIssue,
  schemaIssues,
  validateTimestampFields,
  type ValidationIssue,
  type ValidationResult,
} from "./common.ts";

export const PLAN_STAGE_IDS = ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"] as const;
export const PlanStageIdSchema = Type.Enum(PLAN_STAGE_IDS);
const ContentHashSchema = HashSchema;
const ContentEntity = <const P extends Record<string, any>>(properties: P) => StrictObject({ ...properties, contentHash: ContentHashSchema });

const GeneratorSchema = StrictObject({
  name: IdSchema,
  version: Type.String({ minLength: 1, maxLength: 64 }),
  profileHash: HashSchema,
});

const GoverningModelRefSchema = StrictObject({
  collection: Type.Enum(["intents", "concepts", "scenarios", "decisions", "commitments"]),
  id: IdSchema,
  semanticHash: HashSchema,
});
const GroundingModelRefSchema = StrictObject({
  collection: Type.Enum(["intents", "concepts", "scenarios", "decisions", "commitments", "evidence"]),
  id: IdSchema,
  semanticHash: HashSchema,
});
const ModelSelectorSchema = StrictObject({
  version: Type.String({ minLength: 1, maxLength: 64 }),
  selectedWorkstreamIds: StringSet(),
  explicitSeedIds: StringSet(),
  selectorHash: HashSchema,
});

const GoverningObjectBindingSchema = StrictObject({
  collection: Type.Enum(["intents", "concepts", "scenarios", "decisions", "commitments"]),
  id: IdSchema,
  effectiveState: Type.Literal("accepted"),
  semanticHash: HashSchema,
  acceptanceContentHash: HashSchema,
});

const ContextRefSchema = StrictObject({
  collection: Type.Enum(["evidence", "questions", "proposals", "discoveries"]),
  id: IdSchema,
  semanticHash: HashSchema,
});

const SpecBindingSchema = StrictObject({
  projectionId: IdSchema,
  projectionContract: Type.String({ minLength: 1, maxLength: 64 }),
  modelInputHash: HashSchema,
  contentHash: HashSchema,
});

const ModelBindingSchema = StrictObject({
  projectId: IdSchema,
  schemaVersion: PositiveIntegerSchema,
  revision: NonNegativeIntegerSchema,
  modelHash: HashSchema,
  selector: ModelSelectorSchema,
  closure: StrictObject({ entries: Type.Array(GoverningObjectBindingSchema), closureHash: HashSchema }),
  contextRefs: Type.Array(ContextRefSchema),
  specs: Type.Array(SpecBindingSchema),
  bindingHash: HashSchema,
});

const RepositorySchema = ContentEntity({
  repositoryId: IdSchema,
  role: Type.Enum(["authority", "write", "input", "verification"]),
  locator: Nullable(RootRelativePathSchema),
  baseline: GitTreeRefV1Schema,
  targetRef: Type.String({ minLength: 1, maxLength: 512 }),
});

const OutcomeSchema = ContentEntity({
  outcomeId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  description: BoundedTextSchema,
  oracleIds: StringSet({ minItems: 1 }),
});
const NonGoalSchema = ContentEntity({
  nonGoalId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  description: BoundedTextSchema,
});
const ComponentSchema = ContentEntity({
  componentId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  responsibilities: Type.Array(BoundedTextSchema),
  subjectIds: StringSet(),
  contractIds: StringSet(),
});
const ContractSchema = ContentEntity({
  contractId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  description: BoundedTextSchema,
  subjectIds: StringSet({ minItems: 1 }),
  compatibility: Type.Enum(["compatible", "breaking", "unknown"]),
});
const RiskSchema = ContentEntity({
  riskId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  severity: Type.Enum(["low", "medium", "high", "critical"]),
  subjectIds: StringSet(),
  mitigation: BoundedTextSchema,
});
const AssumptionSchema = ContentEntity({
  assumptionId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  body: BoundedTextSchema,
  subjectIds: StringSet(),
});
const EffectScopeSchema = ContentEntity({
  effectScopeId: IdSchema,
  kind: Type.Enum(["reversible_evaluation", "external_write", "production", "irreversible"]),
  provider: IdSchema,
  purpose: BoundedTextSchema,
  procedureClass: Type.Enum(["pure", "idempotent", "compensatable", "non_repeatable", "unknown"]),
  subjectIds: StringSet(),
});

const ArchitectureSchema = ContentEntity({
  outcomes: Type.Array(OutcomeSchema, { minItems: 1 }),
  nonGoals: Type.Array(NonGoalSchema),
  components: Type.Array(ComponentSchema, { minItems: 1 }),
  contracts: Type.Array(ContractSchema),
  risks: Type.Array(RiskSchema),
  assumptions: Type.Array(AssumptionSchema),
  effectScopes: Type.Array(EffectScopeSchema),
});

const SemanticSubjectSchema = ContentEntity({
  subjectId: IdSchema,
  kind: Type.Enum(["behavior", "invariant", "contract", "schema", "decision", "data", "generated_source", "external_resource"]),
  title: Type.String({ minLength: 1, maxLength: 256 }),
  description: BoundedTextSchema,
});

const OracleAssertionSchema = ContentEntity({
  assertionId: IdSchema,
  subjectId: IdSchema,
  observationMethod: Type.Enum(["static_analysis", "automated_check", "manual_observation", "external_observation", "combined"]),
  procedureId: IdSchema,
  passCondition: BoundedTextSchema,
  failureSignals: Type.Array(BoundedTextSchema, { minItems: 1 }),
  tolerance: BoundedTextSchema,
  environmentProfileId: IdSchema,
  requiredEvidenceClass: Type.Enum(["deterministic", "independent", "manual", "external"]),
});
const AcceptanceOracleSchema = ContentEntity({
  oracleId: IdSchema,
  title: Type.String({ minLength: 1, maxLength: 256 }),
  sourceRefs: Type.Array(GroundingModelRefSchema, { minItems: 1 }),
  assertions: Type.Array(OracleAssertionSchema, { minItems: 1 }),
});

const SemanticReadSchema = StrictObject({
  subjectId: IdSchema,
  mode: Type.Enum(["observe", "consume", "validate"]),
});
const SemanticWriteSchema = StrictObject({
  subjectId: IdSchema,
  mode: Type.Enum(["create", "extend", "replace", "migrate", "delete"]),
  compatibility: Type.Enum(["compatible", "breaking", "unknown"]),
  migrationProtocolId: Nullable(IdSchema),
});
const CapabilityRequirementSchema = Type.Union([
  StrictObject({
    kind: Type.Literal("capability"), capabilityId: IdSchema, purpose: BoundedTextSchema,
    phases: Type.Array(PlanStageIdSchema, { minItems: 1 }), environmentProfileId: IdSchema,
  }),
  StrictObject({
    kind: Type.Literal("credential"), capabilityId: IdSchema, provider: IdSchema,
    scope: Type.String({ minLength: 1, maxLength: 512 }), purpose: BoundedTextSchema,
    phases: Type.Array(PlanStageIdSchema, { minItems: 1 }), environmentProfileId: IdSchema,
    semanticClass: Type.Enum(["auth_only", "semantic_versioned", "non_replayable"]),
  }),
]);
const CheckConditionSchema = ContentEntity({
  predicateId: IdSchema,
  subjectId: IdSchema,
  operator: Type.Enum(["exists", "equals", "compatible", "satisfies", "available"]),
  expected: Type.Union([Type.String({ maxLength: 4096 }), Type.Boolean(), NonNegativeIntegerSchema]),
  sourceRefs: Type.Array(GroundingModelRefSchema, { minItems: 1 }),
});
const CheckApplicabilitySchema = StrictObject({
  checkId: IdSchema,
  phases: Type.Array(PlanStageIdSchema, { minItems: 1 }),
  applicability: Type.Enum(["required", "conditional", "not_applicable"]),
  reason: BoundedTextSchema,
  condition: Nullable(CheckConditionSchema),
});
const PathEvidenceSchema = StrictObject({
  path: RootRelativePathSchema,
  symbol: Nullable(Type.String({ minLength: 1, maxLength: 512 })),
  basis: BoundedTextSchema,
  confidence: Type.Enum(["low", "medium", "high"]),
});
const ResourceDemandSchema = StrictObject({
  resourceClassId: IdSchema,
  phases: Type.Array(PlanStageIdSchema, { minItems: 1 }),
  units: PositiveIntegerSchema,
});
const RiskProfileSchema = StrictObject({
  tier: Type.Enum(["low", "medium", "high", "critical"]),
  reasons: Type.Array(BoundedTextSchema),
  hardeningProfileIds: StringSet(),
});
const IntegrationObligationsSchema = StrictObject({
  trainIds: StringSet({ minItems: 1 }),
  effectScopeIds: StringSet(),
  migrationProtocolIds: StringSet(),
});

const WorkItemSchema = ContentEntity({
  workItemId: IdSchema,
  kind: Type.Literal("change"),
  title: Type.String({ minLength: 1, maxLength: 256 }),
  objective: BoundedTextSchema,
  writeRepositoryId: IdSchema,
  outcomeIds: StringSet({ minItems: 1 }),
  nonGoalIds: StringSet(),
  modelRefs: Type.Array(GoverningModelRefSchema),
  contractIds: StringSet(),
  oracleIds: StringSet({ minItems: 1 }),
  extraContext: Type.String({ maxLength: 32_768 }),
  contextRefs: Type.Array(ArtifactRefV1Schema),
  semanticReads: Type.Array(SemanticReadSchema),
  semanticWrites: Type.Array(SemanticWriteSchema),
  risk: RiskProfileSchema,
  capabilities: Type.Array(CapabilityRequirementSchema),
  checks: Type.Array(CheckApplicabilitySchema),
  pathEvidence: Type.Array(PathEvidenceSchema),
  resourceDemands: Type.Array(ResourceDemandSchema),
  integration: IntegrationObligationsSchema,
});

const GatePredicateSchema = ContentEntity({
  predicateId: IdSchema,
  subjectId: IdSchema,
  operator: Type.Enum(["exists", "equals", "compatible", "satisfies", "authorized", "available"]),
  expected: Type.Union([Type.String({ maxLength: 4096 }), Type.Boolean(), NonNegativeIntegerSchema]),
  evidenceClass: Type.Enum(["model", "contract", "authorization", "capability", "external", "integration"]),
});
const BlockedStageSchema = StrictObject({ workItemId: IdSchema, stages: Type.Array(PlanStageIdSchema, { minItems: 1 }) });
const GateSchema = ContentEntity({
  gateId: IdSchema,
  kind: Type.Enum(["model_authority", "contract", "human_authorization", "environment_capability", "external_precondition", "integration"]),
  subjectIds: StringSet({ minItems: 1 }),
  authorityRefs: Type.Array(GoverningModelRefSchema),
  evidenceRefs: Type.Array(ArtifactRefV1Schema),
  evidenceProducerWorkItemIds: StringSet(),
  releaseMode: Type.Enum(["plan_revision", "run_evidence"]),
  predicate: GatePredicateSchema,
  blocks: Type.Array(BlockedStageSchema, { minItems: 1 }),
});

const PrecedenceSchema = ContentEntity({
  precedenceId: IdSchema,
  predecessorWorkItemId: IdSchema,
  successorWorkItemId: IdSchema,
  subjectIds: StringSet({ minItems: 1 }),
  reason: BoundedTextSchema,
  releaseDisposition: Type.Literal("integrated"),
  evidenceRefs: Type.Array(ArtifactRefV1Schema),
});
const MutexMemberSchema = StrictObject({ workItemId: IdSchema, phases: Type.Array(PlanStageIdSchema, { minItems: 1 }) });
const SemanticMutexSchema = ContentEntity({
  mutexGroupId: IdSchema,
  subjectIds: StringSet({ minItems: 1 }),
  members: Type.Array(MutexMemberSchema, { minItems: 2 }),
  reason: BoundedTextSchema,
  confidence: Type.Enum(["low", "medium", "high"]),
  evidenceRefs: Type.Array(ArtifactRefV1Schema),
});
const ResourceClassSchema = ContentEntity({
  resourceClassId: IdSchema,
  name: Type.String({ minLength: 1, maxLength: 256 }),
  namespace: IdSchema,
  unit: Type.String({ minLength: 1, maxLength: 64 }),
  semanticMaximum: PositiveIntegerSchema,
});
const TrainMemberSchema = StrictObject({ workItemId: IdSchema, ordinal: NonNegativeIntegerSchema });
const IntegrationTrainSchema = ContentEntity({
  trainId: IdSchema,
  repositoryId: IdSchema,
  strategy: Type.Literal("merge_tree_one_parent"),
  members: Type.Array(TrainMemberSchema, { minItems: 1 }),
  partialIntegrationPrecedenceIds: StringSet(),
  compositionProfileHash: HashSchema,
  prefixValidationProfileId: IdSchema,
  prefixValidationProfileHash: HashSchema,
  finalValidationProfileId: IdSchema,
  finalValidationProfileHash: HashSchema,
});
const CompatibilityEntrySchema = StrictObject({ from: IdSchema, to: IdSchema, compatible: Type.Boolean() });
const MigrationStageSchema = ContentEntity({
  stageId: IdSchema,
  kind: Type.Enum(["expand", "dual_support", "backfill", "switch", "contract"]),
  workItemIds: StringSet({ minItems: 1 }),
  gateIds: StringSet(),
  oracleIds: StringSet({ minItems: 1 }),
});
const MigrationProtocolSchema = ContentEntity({
  migrationProtocolId: IdSchema,
  subjectId: IdSchema,
  fromVersion: IdSchema,
  toVersion: IdSchema,
  strategy: Type.Enum(["atomic", "expand_contract"]),
  stages: Type.Array(MigrationStageSchema, { minItems: 1 }),
  compatibility: Type.Array(CompatibilityEntrySchema),
  rollback: BoundedTextSchema,
  atomicRiskDispositionRef: Nullable(GoverningModelRefSchema),
});
const ConstraintSetSchema = StrictObject({
  precedence: Type.Array(PrecedenceSchema),
  semanticMutexes: Type.Array(SemanticMutexSchema),
  resourceClasses: Type.Array(ResourceClassSchema),
  integrationTrains: Type.Array(IntegrationTrainSchema, { minItems: 1 }),
  migrationProtocols: Type.Array(MigrationProtocolSchema),
});

const LifecycleBindingSchema = StrictObject({
  profileId: IdSchema,
  profileHash: HashSchema,
  checkCatalogHash: HashSchema,
  retryPolicyHash: HashSchema,
  schedulerPolicyVersion: Type.String({ minLength: 1, maxLength: 64 }),
  schedulerPolicyHash: HashSchema,
  stages: Type.Tuple(PLAN_STAGE_IDS.map((stage) => Type.Literal(stage)) as any),
});
const ArtifactPolicySchema = StrictObject({
  profileId: IdSchema,
  profileHash: HashSchema,
  maxInlineBytes: NonNegativeIntegerSchema,
  maxArtifactBytes: PositiveIntegerSchema,
  maxArtifactsPerWorkItem: PositiveIntegerSchema,
  allowedRoots: Type.Array(RootRelativePathSchema),
  allowedMediaTypes: Type.Array(Type.String({ minLength: 1, maxLength: 128 })),
  defaultRetention: Type.Enum(["ephemeral", "run", "project"]),
  redactRestrictedLocators: Type.Boolean(),
});
const ProjectionDefinitionSchema = ContentEntity({
  kind: Type.Enum(["architecture_review", "decomposition_review", "static_graph", "node_packet", "scheduler_index", "legacy_inspection", "dag_execution"]),
  version: Type.String({ minLength: 1, maxLength: 64 }),
  executable: Type.Boolean(),
});
const ProjectionContractSchema = StrictObject({
  version: Type.String({ minLength: 1, maxLength: 64 }),
  projections: Type.Array(ProjectionDefinitionSchema, { minItems: 1 }),
});

export const CanonicalDagPlanV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("CanonicalDagPlanV1"),
  canonicalization: Type.Literal("jcs-v1"),
  planId: IdSchema,
  revision: PositiveIntegerSchema,
  supersedesPlanHash: Type.Optional(HashSchema),
  createdAt: TimestampSchema,
  generator: GeneratorSchema,
  modelBinding: ModelBindingSchema,
  repositories: Type.Array(RepositorySchema, { minItems: 1 }),
  architecture: ArchitectureSchema,
  semanticSubjects: Type.Array(SemanticSubjectSchema, { minItems: 1 }),
  acceptanceOracles: Type.Array(AcceptanceOracleSchema, { minItems: 1 }),
  workItems: Type.Array(WorkItemSchema, { minItems: 1 }),
  gates: Type.Array(GateSchema),
  constraints: ConstraintSetSchema,
  lifecycleBinding: LifecycleBindingSchema,
  artifactPolicy: ArtifactPolicySchema,
  projectionContract: ProjectionContractSchema,
  planHash: HashSchema,
});
export type CanonicalDagPlanV1 = Static<typeof CanonicalDagPlanV1Schema>;
export const CANONICAL_DAG_PLAN_SCHEMA_HASH = canonicalHash(JSON.parse(JSON.stringify(CanonicalDagPlanV1Schema)));

export function canonicalDagPlanHash(plan: Omit<CanonicalDagPlanV1, "planHash"> | CanonicalDagPlanV1): string {
  return hashWithoutField(plan as unknown as Record<string, unknown>, "planHash");
}

export function validateCanonicalDagPlanV1(value: unknown): ValidationResult<CanonicalDagPlanV1> {
  const issues = schemaIssues(CanonicalDagPlanV1Schema, value);
  if (issues.length) return { ok: false, issues };
  const plan = value as CanonicalDagPlanV1;
  validatePlanSemantics(plan, issues);
  return issues.length ? { ok: false, issues } : { ok: true, value: plan, issues };
}

export function parseCanonicalDagPlanV1(text: string): CanonicalDagPlanV1 {
  const value = parseStrictJson(text);
  assertCanonicalDagPlanV1(value);
  return value;
}

export function assertCanonicalDagPlanV1(value: unknown): asserts value is CanonicalDagPlanV1 {
  const result = validateCanonicalDagPlanV1(value);
  if (!result.ok) throw new Error(`Invalid CanonicalDagPlanV1:\n${result.issues.map(({ path, message }) => `- ${path}: ${message}`).join("\n")}`);
}

function validatePlanSemantics(plan: CanonicalDagPlanV1, issues: ValidationIssue[]): void {
  validateTimestampFields(plan, issues);
  pushIssue(issues, "/planHash", plan.planHash === canonicalDagPlanHash(plan), "does not match canonical content");
  pushIssue(issues, "/supersedesPlanHash", plan.revision === 1 ? plan.supersedesPlanHash === undefined : typeof plan.supersedesPlanHash === "string",
    "must be absent for revision 1 and present for successor revisions");
  pushIssue(issues, "/modelBinding/bindingHash", plan.modelBinding.bindingHash === hashWithoutField(plan.modelBinding as unknown as Record<string, unknown>, "bindingHash"), "does not match model binding content");
  pushIssue(issues, "/modelBinding/selector/selectorHash", plan.modelBinding.selector.selectorHash === hashWithoutField(plan.modelBinding.selector as unknown as Record<string, unknown>, "selectorHash"), "does not match selector content");
  pushIssue(issues, "/modelBinding/closure/closureHash", plan.modelBinding.closure.closureHash === canonicalHash(plan.modelBinding.closure.entries), "does not match closure entries");
  pushIssue(issues, "/modelBinding/selector/selectedWorkstreamIds", isSortedUnique(plan.modelBinding.selector.selectedWorkstreamIds), "must be sorted and deduplicated");
  pushIssue(issues, "/modelBinding/selector/explicitSeedIds", isSortedUnique(plan.modelBinding.selector.explicitSeedIds), "must be sorted and deduplicated");
  pushIssue(issues, "/modelBinding/closure/entries", isSortedUnique(plan.modelBinding.closure.entries.map(({ collection, id }) => `${collection}/${id}`)), "must be sorted by collection and id without duplicates");
  pushIssue(issues, "/modelBinding/contextRefs", isSortedUnique(plan.modelBinding.contextRefs.map(({ collection, id }) => `${collection}/${id}`)), "must be sorted by collection and id without duplicates");
  pushIssue(issues, "/modelBinding/specs", isSortedUnique(plan.modelBinding.specs.map(({ projectionId }) => projectionId)), "must be sorted by projectionId without duplicates");

  const governingModelRefs = new Map(plan.modelBinding.closure.entries.map(({ collection, id, semanticHash }) => [`${collection}/${id}`, semanticHash]));
  const groundingModelRefs = new Map([
    ...plan.modelBinding.closure.entries.map(({ collection, id, semanticHash }) => [`${collection}/${id}`, semanticHash] as const),
    ...plan.modelBinding.contextRefs.filter(({ collection }) => collection === "evidence").map(({ collection, id, semanticHash }) => [`${collection}/${id}`, semanticHash] as const),
  ]);
  const repositories = uniqueEntityIds(plan.repositories, "repositoryId", "/repositories", issues);
  const outcomes = uniqueEntityIds(plan.architecture.outcomes, "outcomeId", "/architecture/outcomes", issues);
  const nonGoals = uniqueEntityIds(plan.architecture.nonGoals, "nonGoalId", "/architecture/nonGoals", issues);
  const components = uniqueEntityIds(plan.architecture.components, "componentId", "/architecture/components", issues);
  const contracts = uniqueEntityIds(plan.architecture.contracts, "contractId", "/architecture/contracts", issues);
  uniqueEntityIds(plan.architecture.risks, "riskId", "/architecture/risks", issues);
  uniqueEntityIds(plan.architecture.assumptions, "assumptionId", "/architecture/assumptions", issues);
  const effectScopes = uniqueEntityIds(plan.architecture.effectScopes, "effectScopeId", "/architecture/effectScopes", issues);
  const subjects = uniqueEntityIds(plan.semanticSubjects, "subjectId", "/semanticSubjects", issues);
  const oracles = uniqueEntityIds(plan.acceptanceOracles, "oracleId", "/acceptanceOracles", issues);
  const workItems = uniqueEntityIds(plan.workItems, "workItemId", "/workItems", issues);
  const gates = uniqueEntityIds(plan.gates, "gateId", "/gates", issues);
  const precedence = uniqueEntityIds(plan.constraints.precedence, "precedenceId", "/constraints/precedence", issues);
  const mutexes = uniqueEntityIds(plan.constraints.semanticMutexes, "mutexGroupId", "/constraints/semanticMutexes", issues);
  const resources = uniqueEntityIds(plan.constraints.resourceClasses, "resourceClassId", "/constraints/resourceClasses", issues);
  const trains = uniqueEntityIds(plan.constraints.integrationTrains, "trainId", "/constraints/integrationTrains", issues);
  const migrations = uniqueEntityIds(plan.constraints.migrationProtocols, "migrationProtocolId", "/constraints/migrationProtocols", issues);
  const globallyAddressableIds = [
    ...repositories, ...outcomes, ...nonGoals, ...components, ...contracts, ...subjects, ...oracles, ...workItems,
    ...gates, ...precedence, ...mutexes, ...resources, ...trains, ...migrations, ...effectScopes,
    ...plan.architecture.risks.map(({ riskId }) => riskId), ...plan.architecture.assumptions.map(({ assumptionId }) => assumptionId),
  ];
  pushIssue(issues, "/", new Set(globallyAddressableIds).size === globallyAddressableIds.length, "globally addressable entity IDs must be unique across categories");

  const allContentEntities: Array<[string, any[]]> = [
    ["/repositories", plan.repositories], ["/architecture/outcomes", plan.architecture.outcomes], ["/architecture/nonGoals", plan.architecture.nonGoals],
    ["/architecture/components", plan.architecture.components], ["/architecture/contracts", plan.architecture.contracts], ["/architecture/risks", plan.architecture.risks],
    ["/architecture/assumptions", plan.architecture.assumptions], ["/architecture/effectScopes", plan.architecture.effectScopes], ["/semanticSubjects", plan.semanticSubjects], ["/acceptanceOracles", plan.acceptanceOracles],
    ["/workItems", plan.workItems], ["/gates", plan.gates], ["/constraints/precedence", plan.constraints.precedence],
    ["/constraints/semanticMutexes", plan.constraints.semanticMutexes], ["/constraints/resourceClasses", plan.constraints.resourceClasses],
    ["/constraints/integrationTrains", plan.constraints.integrationTrains], ["/constraints/migrationProtocols", plan.constraints.migrationProtocols],
    ["/projectionContract/projections", plan.projectionContract.projections],
  ];
  for (const [path, entities] of allContentEntities) entities.forEach((entity, index) => pushIssue(issues, `${path}/${index}/contentHash`, contentHashMatches(entity), "does not match canonical entity content"));
  pushIssue(issues, "/architecture/contentHash", contentHashMatches(plan.architecture), "does not match canonical architecture content");
  pushIssue(issues, "/projectionContract/projections", new Set(plan.projectionContract.projections.map(({ kind, version }) => `${kind}/${version}`)).size === plan.projectionContract.projections.length, "projection kind/version pairs must be unique");
  plan.projectionContract.projections.forEach((projection, index) => pushIssue(issues, `/projectionContract/projections/${index}/executable`, !["legacy_inspection", "dag_execution"].includes(projection.kind) || projection.executable === false, "legacy and execution-status projections are never executable authority"));
  plan.acceptanceOracles.forEach((oracle, oracleIndex) => {
    oracle.sourceRefs.forEach((modelRef) => validateModelRef(issues, `/acceptanceOracles/${oracleIndex}/sourceRefs`, groundingModelRefs, modelRef));
    oracle.assertions.forEach((assertion, assertionIndex) => {
      pushIssue(issues, `/acceptanceOracles/${oracleIndex}/assertions/${assertionIndex}/contentHash`, contentHashMatches(assertion), "does not match canonical assertion content");
      ref(issues, `/acceptanceOracles/${oracleIndex}/assertions/${assertionIndex}/subjectId`, subjects, assertion.subjectId);
    });
  });
  plan.gates.forEach((gate, gateIndex) => pushIssue(issues, `/gates/${gateIndex}/predicate/contentHash`, contentHashMatches(gate.predicate), "does not match canonical predicate content"));
  plan.constraints.migrationProtocols.forEach((migration, migrationIndex) => migration.stages.forEach((stage, stageIndex) => pushIssue(issues, `/constraints/migrationProtocols/${migrationIndex}/stages/${stageIndex}/contentHash`, contentHashMatches(stage), "does not match canonical migration-stage content")));

  for (const [index, repository] of plan.repositories.entries()) {
    pushIssue(issues, `/repositories/${index}/baseline/repositoryId`, repository.baseline.repositoryId === repository.repositoryId, "must match repositoryId");
  }
  for (const [index, outcome] of plan.architecture.outcomes.entries()) outcome.oracleIds.forEach((id) => ref(issues, `/architecture/outcomes/${index}/oracleIds`, oracles, id));
  for (const [index, component] of plan.architecture.components.entries()) {
    component.subjectIds.forEach((id) => ref(issues, `/architecture/components/${index}/subjectIds`, subjects, id));
    component.contractIds.forEach((id) => ref(issues, `/architecture/components/${index}/contractIds`, contracts, id));
  }
  for (const [index, contract] of plan.architecture.contracts.entries()) contract.subjectIds.forEach((id) => ref(issues, `/architecture/contracts/${index}/subjectIds`, subjects, id));
  for (const [index, effect] of plan.architecture.effectScopes.entries()) effect.subjectIds.forEach((id) => ref(issues, `/architecture/effectScopes/${index}/subjectIds`, subjects, id));

  for (const [index, item] of plan.workItems.entries()) {
    const path = `/workItems/${index}`;
    ref(issues, `${path}/writeRepositoryId`, repositories, item.writeRepositoryId);
    pushIssue(issues, `${path}/writeRepositoryId`, plan.repositories.find(({ repositoryId }) => repositoryId === item.writeRepositoryId)?.role === "write", "must reference a repository with role write");
    item.outcomeIds.forEach((id) => {
      ref(issues, `${path}/outcomeIds`, outcomes, id);
      const outcome = plan.architecture.outcomes.find(({ outcomeId }) => outcomeId === id);
      outcome?.oracleIds.forEach((oracleId) => pushIssue(issues, `${path}/oracleIds`, item.oracleIds.includes(oracleId), `must include outcome oracle ${oracleId}`));
    });
    item.nonGoalIds.forEach((id) => ref(issues, `${path}/nonGoalIds`, nonGoals, id));
    item.contractIds.forEach((id) => ref(issues, `${path}/contractIds`, contracts, id));
    item.oracleIds.forEach((id) => ref(issues, `${path}/oracleIds`, oracles, id));
    item.modelRefs.forEach((modelRef) => validateModelRef(issues, `${path}/modelRefs`, governingModelRefs, modelRef));
    item.semanticReads.forEach(({ subjectId }) => ref(issues, `${path}/semanticReads`, subjects, subjectId));
    item.semanticWrites.forEach((write, writeIndex) => {
      ref(issues, `${path}/semanticWrites/${writeIndex}/subjectId`, subjects, write.subjectId);
      const requiresMigration = write.mode === "migrate" || write.mode === "delete" || (write.mode === "replace" && write.compatibility !== "compatible");
      pushIssue(issues, `${path}/semanticWrites/${writeIndex}/migrationProtocolId`, requiresMigration ? write.migrationProtocolId !== null : true, "is required for migrate/delete or non-compatible replace");
      if (write.migrationProtocolId) ref(issues, `${path}/semanticWrites/${writeIndex}/migrationProtocolId`, migrations, write.migrationProtocolId);
    });
    item.checks.forEach((check, checkIndex) => {
      const requiresCondition = check.applicability !== "required";
      pushIssue(issues, `${path}/checks/${checkIndex}/condition`, requiresCondition ? check.condition !== null : check.condition === null, "conditional/not-applicable checks require a bound condition; required checks must not carry one");
      if (check.condition) {
        pushIssue(issues, `${path}/checks/${checkIndex}/condition/contentHash`, contentHashMatches(check.condition), "does not match canonical check-condition content");
        ref(issues, `${path}/checks/${checkIndex}/condition/subjectId`, subjects, check.condition.subjectId);
        check.condition.sourceRefs.forEach((modelRef) => validateModelRef(issues, `${path}/checks/${checkIndex}/condition/sourceRefs`, groundingModelRefs, modelRef));
      }
    });
    item.resourceDemands.forEach(({ resourceClassId, units }, demandIndex) => {
      ref(issues, `${path}/resourceDemands/${demandIndex}/resourceClassId`, resources, resourceClassId);
      const resource = plan.constraints.resourceClasses.find((candidate) => candidate.resourceClassId === resourceClassId);
      pushIssue(issues, `${path}/resourceDemands/${demandIndex}/units`, !resource || units <= resource.semanticMaximum, "cannot exceed the resource semantic maximum");
    });
    pushIssue(issues, `${path}/resourceDemands`, new Set(item.resourceDemands.map(({ resourceClassId }) => resourceClassId)).size === item.resourceDemands.length, "must contain at most one phase-scoped demand per resource class");
    item.integration.trainIds.forEach((id) => {
      ref(issues, `${path}/integration/trainIds`, trains, id);
      pushIssue(issues, `${path}/integration/trainIds`, plan.constraints.integrationTrains.find(({ trainId }) => trainId === id)?.members.some(({ workItemId }) => workItemId === item.workItemId), "declared train must contain this work item");
    });
    item.integration.effectScopeIds.forEach((id) => {
      ref(issues, `${path}/integration/effectScopeIds`, effectScopes, id);
      const scope = plan.architecture.effectScopes.find(({ effectScopeId }) => effectScopeId === id);
      const itemSubjectIds = new Set([...item.semanticReads.map(({ subjectId }) => subjectId), ...item.semanticWrites.map(({ subjectId }) => subjectId)]);
      pushIssue(issues, `${path}/integration/effectScopeIds`, !scope || scope.subjectIds.every((subjectId) => itemSubjectIds.has(subjectId)), "effect scope subjects must belong to the exact work item's semantic contract");
    });
    item.integration.migrationProtocolIds.forEach((id) => ref(issues, `${path}/integration/migrationProtocolIds`, migrations, id));
  }

  for (const outcome of plan.architecture.outcomes) pushIssue(issues, "/workItems", plan.workItems.some(({ outcomeIds }) => outcomeIds.includes(outcome.outcomeId)), `accepted outcome ${outcome.outcomeId} must be assigned to at least one work item`);
  for (const [index, gate] of plan.gates.entries()) {
    gate.subjectIds.forEach((id) => ref(issues, `/gates/${index}/subjectIds`, subjects, id));
    ref(issues, `/gates/${index}/predicate/subjectId`, subjects, gate.predicate.subjectId);
    gate.authorityRefs.forEach((modelRef) => validateModelRef(issues, `/gates/${index}/authorityRefs`, governingModelRefs, modelRef));
    gate.evidenceProducerWorkItemIds.forEach((id) => ref(issues, `/gates/${index}/evidenceProducerWorkItemIds`, workItems, id));
    gate.blocks.forEach(({ workItemId }) => ref(issues, `/gates/${index}/blocks`, workItems, workItemId));
    pushIssue(issues, `/gates/${index}/evidenceProducerWorkItemIds`, gate.releaseMode !== "plan_revision" || gate.evidenceProducerWorkItemIds.length === 0, "plan_revision gate cannot have in-run evidence producers");
    pushIssue(issues, `/gates/${index}/releaseMode`, ["model_authority", "contract"].includes(gate.kind) ? gate.releaseMode === "plan_revision" : true, "semantic/model gates require plan_revision release");
  }
  for (const [index, edge] of plan.constraints.precedence.entries()) {
    ref(issues, `/constraints/precedence/${index}/predecessorWorkItemId`, workItems, edge.predecessorWorkItemId);
    ref(issues, `/constraints/precedence/${index}/successorWorkItemId`, workItems, edge.successorWorkItemId);
    edge.subjectIds.forEach((id) => ref(issues, `/constraints/precedence/${index}/subjectIds`, subjects, id));
    pushIssue(issues, `/constraints/precedence/${index}`, edge.predecessorWorkItemId !== edge.successorWorkItemId, "cannot be a self-edge");
  }
  validateAcyclicCausalGraph(plan, precedence, issues);
  for (const [index, mutex] of plan.constraints.semanticMutexes.entries()) {
    mutex.subjectIds.forEach((id) => ref(issues, `/constraints/semanticMutexes/${index}/subjectIds`, subjects, id));
    const memberIds = mutex.members.map(({ workItemId }) => workItemId).sort();
    pushIssue(issues, `/constraints/semanticMutexes/${index}/members`, new Set(memberIds).size === memberIds.length, "must not repeat a work item");
    memberIds.forEach((id) => ref(issues, `/constraints/semanticMutexes/${index}/members`, workItems, id));
    for (const edge of plan.constraints.precedence) pushIssue(issues, `/constraints/semanticMutexes/${index}`, !(memberIds.includes(edge.predecessorWorkItemId) && memberIds.includes(edge.successorWorkItemId)), "must not duplicate a causal precedence edge");
  }
  validateSharedSemanticWrites(plan, issues);
  pushIssue(issues, "/constraints/integrationTrains", new Set(plan.constraints.integrationTrains.map(({ repositoryId }) => repositoryId)).size === plan.constraints.integrationTrains.length, "v1 permits exactly one integration train per repository");
  for (const repository of plan.repositories.filter(({ role }) => role === "write")) pushIssue(issues, "/constraints/integrationTrains", plan.constraints.integrationTrains.some(({ repositoryId }) => repositoryId === repository.repositoryId), `write repository ${repository.repositoryId} requires an integration train`);
  for (const [index, train] of plan.constraints.integrationTrains.entries()) {
    ref(issues, `/constraints/integrationTrains/${index}/repositoryId`, repositories, train.repositoryId);
    pushIssue(issues, `/constraints/integrationTrains/${index}/repositoryId`, plan.repositories.find(({ repositoryId }) => repositoryId === train.repositoryId)?.role === "write", "integration train requires a write repository");
    train.partialIntegrationPrecedenceIds.forEach((id) => ref(issues, `/constraints/integrationTrains/${index}/partialIntegrationPrecedenceIds`, precedence, id));
    pushIssue(issues, `/constraints/integrationTrains/${index}/members`, train.members.every(({ ordinal }, memberIndex) => ordinal === memberIndex), "ordinals must be contiguous from zero in declared order");
    pushIssue(issues, `/constraints/integrationTrains/${index}/members`, new Set(train.members.map(({ workItemId }) => workItemId)).size === train.members.length, "must not repeat a work item");
    train.members.forEach(({ workItemId }) => {
      ref(issues, `/constraints/integrationTrains/${index}/members`, workItems, workItemId);
      const item = plan.workItems.find((candidate) => candidate.workItemId === workItemId);
      pushIssue(issues, `/constraints/integrationTrains/${index}/members`, item?.writeRepositoryId === train.repositoryId, "member write repository must match train repository");
      pushIssue(issues, `/constraints/integrationTrains/${index}/members`, item?.integration.trainIds.includes(train.trainId), "member must declare this integration train");
    });
  }
  for (const [index, migration] of plan.constraints.migrationProtocols.entries()) {
    ref(issues, `/constraints/migrationProtocols/${index}/subjectId`, subjects, migration.subjectId);
    migration.stages.forEach((stage) => {
      stage.workItemIds.forEach((id) => ref(issues, `/constraints/migrationProtocols/${index}/stages`, workItems, id));
      stage.gateIds.forEach((id) => ref(issues, `/constraints/migrationProtocols/${index}/stages`, gates, id));
      stage.oracleIds.forEach((id) => ref(issues, `/constraints/migrationProtocols/${index}/stages`, oracles, id));
    });
    pushIssue(issues, `/constraints/migrationProtocols/${index}/atomicRiskDispositionRef`, migration.strategy !== "atomic" || migration.atomicRiskDispositionRef !== null, "atomic migration requires an accepted risk disposition reference");
    if (migration.atomicRiskDispositionRef) validateModelRef(issues, `/constraints/migrationProtocols/${index}/atomicRiskDispositionRef`, governingModelRefs, migration.atomicRiskDispositionRef);
  }
  validateSetFields(plan, "", issues);
}

function uniqueEntityIds(values: readonly any[], key: string, path: string, issues: ValidationIssue[]): Set<string> {
  const ids = values.map((value) => String(value[key]));
  pushIssue(issues, path, new Set(ids).size === ids.length, `contains duplicate ${key}`);
  return new Set(ids);
}
function ref(issues: ValidationIssue[], path: string, ids: Set<string>, id: string): void {
  pushIssue(issues, path, ids.has(id), `references unknown ID ${id}`);
}
function validateModelRef(issues: ValidationIssue[], path: string, refs: Map<string, string>, modelRef: { collection: string; id: string; semanticHash: string }): void {
  const key = `${modelRef.collection}/${modelRef.id}`;
  pushIssue(issues, path, refs.get(key) === modelRef.semanticHash, `reference ${key} must resolve with its exact bound collection and semantic hash`);
}
function validateSetFields(value: any, path: string, issues: ValidationIssue[]): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateSetFields(item, `${path}/${index}`, issues));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value)) {
    const childPath = `${path}/${key}`;
    if (Array.isArray(item) && (key.endsWith("Ids") || key === "phases" || key === "sourceRefs" || key === "authorityRefs" || key === "modelRefs" || key === "allowedRoots" || key === "allowedMediaTypes")) {
      const keys = item.map((candidate) => typeof candidate === "string" ? candidate : candidate && typeof candidate === "object" && "collection" in candidate && "id" in candidate ? `${String((candidate as any).collection)}/${String((candidate as any).id)}` : "");
      pushIssue(issues, childPath, keys.every(Boolean) && isSortedUnique(keys), "must be sorted and deduplicated");
    }
    validateSetFields(item, childPath, issues);
  }
}
function validateSharedSemanticWrites(plan: CanonicalDagPlanV1, issues: ValidationIssue[]): void {
  const bySubject = new Map<string, string[]>();
  for (const item of plan.workItems) for (const write of item.semanticWrites) bySubject.set(write.subjectId, [...(bySubject.get(write.subjectId) ?? []), item.workItemId]);
  for (const [subjectId, rawIds] of bySubject) {
    const ids = [...new Set(rawIds)].sort();
    for (let left = 0; left < ids.length; left += 1) for (let right = left + 1; right < ids.length; right += 1) {
      const first = ids[left];
      const second = ids[right];
      const hasPrecedence = plan.constraints.precedence.some(({ predecessorWorkItemId, successorWorkItemId }) =>
        (predecessorWorkItemId === first && successorWorkItemId === second) || (predecessorWorkItemId === second && successorWorkItemId === first));
      const hasMutex = plan.constraints.semanticMutexes.some((mutex) => mutex.subjectIds.includes(subjectId) &&
        mutex.members.some(({ workItemId }) => workItemId === first) && mutex.members.some(({ workItemId }) => workItemId === second));
      const hasMigration = plan.constraints.migrationProtocols.some((migration) => migration.subjectId === subjectId &&
        migration.stages.some(({ workItemIds }) => workItemIds.includes(first)) && migration.stages.some(({ workItemIds }) => workItemIds.includes(second)));
      pushIssue(issues, "/workItems", hasPrecedence || hasMutex || hasMigration, `shared semantic writes to ${subjectId} by ${first} and ${second} require precedence, a semantic mutex, or one migration protocol`);
    }
  }
}
function validateAcyclicCausalGraph(plan: CanonicalDagPlanV1, _ids: Set<string>, issues: ValidationIssue[]): void {
  const outgoing = new Map<string, string[]>();
  for (const edge of plan.constraints.precedence) outgoing.set(edge.predecessorWorkItemId, [...(outgoing.get(edge.predecessorWorkItemId) ?? []), edge.successorWorkItemId]);
  for (const gate of plan.gates) for (const producerId of gate.evidenceProducerWorkItemIds) for (const blocked of gate.blocks) {
    outgoing.set(producerId, [...(outgoing.get(producerId) ?? []), blocked.workItemId]);
  }
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (id: string): boolean => {
    if (visiting.has(id)) return false;
    if (visited.has(id)) return true;
    visiting.add(id);
    for (const next of outgoing.get(id) ?? []) if (!visit(next)) return false;
    visiting.delete(id); visited.add(id); return true;
  };
  pushIssue(issues, "/constraints/precedence", plan.workItems.every(({ workItemId }) => visit(workItemId)), "precedence plus gate-producer arcs must be acyclic");
}

export function sealCanonicalDagPlanV1(input: Omit<CanonicalDagPlanV1, "planHash">): CanonicalDagPlanV1 {
  const plan = { ...input, planHash: canonicalHash(input) } as CanonicalDagPlanV1;
  assertCanonicalDagPlanV1(plan);
  return plan;
}
