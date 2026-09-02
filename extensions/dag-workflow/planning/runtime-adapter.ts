import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { readFileSync, realpathSync } from "node:fs";
import { lstat, readFile, realpath } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_SCHEDULER_POLICY_HASH_V1,
  PLAN_STAGE_IDS,
  buildSchedulerPlanIndexV1,
  canonicalHash,
  canonicalStringify,
  parseStrictJson,
  sealCanonicalDagPlanV1,
  sealDagRunStateV1,
  DagRunSnapshotStoreV1,
  type CanonicalDagPlanV1,
  type DagExecutionCatalogBindingV1,
  type DagProcedureExecutionAdapterV1,
  type DagProcedureExecutionResultV1,
  type DagRunAuthorizationBindingV1,
  type DagRunStateV1,
  type DagRunValidationContextV1,
  type IntegrationValidationProfileMappingV1,
  type ProcedureCatalogBindingV1,
} from "../dag-runtime/index.ts";
import {
  allObjects,
  assertValidProjectModel,
  modelHash,
  semanticHash,
} from "../project-model/model.ts";
import type { ModelCollectionName, ModelObject, ProjectModel, SpecProjectionView } from "../project-model/types.ts";
import { assertDagPlanningPlanV1 } from "./artifact.ts";
import type { DagPlanningPlanV1 } from "./types.ts";

const run = promisify(execFile);
const PRODUCER_BY_STAGE = {
  F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner",
  F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor",
} as const;
const WRITING_STAGES = new Set(["F1", "F3"]);
const ENVIRONMENT_STAGES = new Set(["F2", "F5", "F7"]);
const GOVERNING_COLLECTIONS = new Set<ModelCollectionName>(["intents", "concepts", "scenarios", "decisions", "commitments"]);
const CONTEXT_COLLECTIONS = new Set<ModelCollectionName>(["evidence", "questions", "proposals", "discoveries"]);
const HELPER_PATH = fileURLToPath(new URL("./integration-validation-pass.mjs", import.meta.url));
const PROJECT_MODEL_PATH = "project-model/model.json";
const RUN_ROOT = ".ai/dag-runs-v1";

type PlanStage = typeof PLAN_STAGE_IDS[number];
type StageAttempt = DagRunStateV1["stageAttempts"][string];

export interface PrepareDagRunV1Input {
  planningPlan: DagPlanningPlanV1;
  repositoryRoot: string;
  runId: string;
  runNonce: string;
  createdAt: string;
}

export interface PreparedDagRunV1 {
  canonicalPlan: CanonicalDagPlanV1;
  genesis: DagRunStateV1;
  context: DagRunValidationContextV1;
  seedFacts: readonly Record<string, unknown>[];
  schedulerIndex: ReturnType<typeof buildSchedulerPlanIndexV1>;
  lifecycleAdapter: DagProcedureExecutionAdapterV1;
}

interface ResolvedSources {
  model: ProjectModel;
  governing: Array<{ collection: Exclude<ModelCollectionName, "workstreams" | "evidence" | "assumptions" | "questions" | "tensions" | "proposals" | "discoveries">; id: string; semanticHash: string; acceptanceContentHash: string; object: ModelObject }>;
  context: Array<{ collection: "evidence" | "questions" | "proposals" | "discoveries"; id: string; semanticHash: string }>;
  specs: Array<{ projectionId: string; projectionContract: string; modelInputHash: string; contentHash: string }>;
}

/**
 * Compiles an approved thin planning record into the shipped canonical runtime contracts.
 * The generated review/authorization/freshness facts are compatibility shims only; callers
 * must not present them as independent user security receipts.
 */
export async function prepareDagRunV1(input: PrepareDagRunV1Input): Promise<PreparedDagRunV1> {
  assertDagPlanningPlanV1(input.planningPlan);
  assertReadyForCompatibilityRun(input.planningPlan, input.createdAt);
  const repositoryRoot = await validateRepositoryBaseline(input.repositoryRoot, input.planningPlan);
  const sources = await resolveSources(repositoryRoot, input.planningPlan);
  const executable = await executableIdentity();
  const integrationValidationProfiles = integrationProfiles(executable, input.planningPlan);
  const procedures = procedureCatalog(executable);
  const canonicalPlan = compileCanonicalPlan(input.planningPlan, sources, procedures, integrationValidationProfiles, input.createdAt);
  const schedulerIndex = buildSchedulerPlanIndexV1(canonicalPlan);
  const prepared = buildGenesis(input, repositoryRoot, canonicalPlan, schedulerIndex, procedures, integrationValidationProfiles);
  return {
    canonicalPlan,
    genesis: prepared.genesis,
    context: prepared.context,
    seedFacts: prepared.seedFacts,
    schedulerIndex,
    lifecycleAdapter: createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot }),
  };
}

export function createBuiltInLifecycleProcedureAdapterV1(input: { repositoryRoot: string }): DagProcedureExecutionAdapterV1 {
  const repositoryRoot = resolve(input.repositoryRoot);
  const executable = { node: realpathSync(process.execPath), nodeHash: hashBytes(readFileSync(realpathSync(process.execPath))) };
  const allowsProcedure = (procedure: ProcedureCatalogBindingV1) => isBuiltInProcedure(procedure, executable);
  return {
    adapterKind: "immutable-catalog-command-v1",
    allowsProcedure,
    async executeExact({ plan, state, attempt, procedure, signal }) {
      if (!allowsProcedure(procedure)) throw new Error("Lifecycle procedure does not match the exact current built-in executable mapping");
      return executeLifecycleProcedure(repositoryRoot, plan, state, attempt, procedure, signal);
    },
  };
}

async function validateRepositoryBaseline(repositoryRootInput: string, plan: DagPlanningPlanV1): Promise<string> {
  const repositoryRoot = await realpath(repositoryRootInput);
  const topLevel = await realpath(await git(repositoryRoot, ["rev-parse", "--show-toplevel"]));
  if (topLevel !== repositoryRoot) throw new Error(`Repository root mismatch: expected ${repositoryRoot}, Git reports ${topLevel}`);

  const branch = plan.repository.targetBranch.startsWith("refs/heads/") ? plan.repository.targetBranch.slice("refs/heads/".length) : plan.repository.targetBranch;
  if (plan.repository.targetBranch.startsWith("refs/") && !plan.repository.targetBranch.startsWith("refs/heads/")) throw new Error("Target branch must name a local refs/heads branch");
  await git(repositoryRoot, ["check-ref-format", "--branch", branch]);
  const targetRef = `refs/heads/${branch}`;
  const currentBranchRef = await git(repositoryRoot, ["symbolic-ref", "-q", "HEAD"]);
  if (currentBranchRef !== targetRef) throw new Error(`Checked-out branch differs from the planned target branch: expected ${targetRef}, found ${currentBranchRef}`);
  const commonRaw = await git(repositoryRoot, ["rev-parse", "--path-format=absolute", "--git-common-dir"]);
  const common = await realpath(commonRaw);
  const objectFormat = await git(repositoryRoot, ["rev-parse", "--show-object-format"]);
  const repositoryId = `repo-${createHash("sha256").update(`${common}\0${objectFormat}`).digest("hex").slice(0, 32)}`;
  if (repositoryId !== plan.repository.repositoryId) throw new Error("Current Git common-dir/object-format identity differs from the planned repository");
  const baselineCommit = await git(repositoryRoot, ["rev-parse", `${plan.repository.baselineCommit}^{commit}`]);
  const baselineTree = await git(repositoryRoot, ["rev-parse", `${plan.repository.baselineCommit}^{tree}`]);
  const declaredTree = await git(repositoryRoot, ["rev-parse", `${plan.repository.baselineTree}^{tree}`]);
  const targetCommit = await git(repositoryRoot, ["rev-parse", `${targetRef}^{commit}`]);
  const targetTree = await git(repositoryRoot, ["rev-parse", `${targetRef}^{tree}`]);
  const headCommit = await git(repositoryRoot, ["rev-parse", "HEAD^{commit}"]);
  const status = await git(repositoryRoot, ["status", "--porcelain=v2", "--untracked-files=all"]);
  if (baselineCommit !== plan.repository.baselineCommit || baselineTree !== plan.repository.baselineTree || declaredTree !== plan.repository.baselineTree) throw new Error("Stale or invalid planned Git baseline commit/tree");
  if (targetCommit !== baselineCommit || targetTree !== baselineTree) throw new Error("Stale planned Git target: target branch no longer equals the declared baseline");
  if (headCommit !== baselineCommit) throw new Error("Repository root HEAD no longer equals the declared baseline");
  if (status !== "") throw new Error("Repository root must be clean before preparing a DAG run");
  return repositoryRoot;
}

async function resolveSources(repositoryRoot: string, plan: DagPlanningPlanV1): Promise<ResolvedSources> {
  await assertTrackedBaselineBytes(repositoryRoot, PROJECT_MODEL_PATH, plan.repository.baselineCommit);
  const modelBytes = await readSafeRegularFile(repositoryRoot, PROJECT_MODEL_PATH);
  const model = parseStrictJson(modelBytes.toString("utf8")) as ProjectModel;
  assertValidProjectModel(model);
  if (model.project.mode !== "authoritative") throw new Error("project-model/model.json must be authoritative before a runtime plan can be prepared");

  const objects = new Map(allObjects(model).map((entry) => [entry.object.id, entry]));
  const projectionsByPath = new Map<string, SpecProjectionView[]>();
  for (const projection of model.project.projections.specs) projectionsByPath.set(projection.path, [...(projectionsByPath.get(projection.path) ?? []), projection]);

  const governing: ResolvedSources["governing"] = [];
  const context: ResolvedSources["context"] = [];
  const specs: ResolvedSources["specs"] = [];
  for (const source of plan.source.refs) {
    if (source.kind === "external") throw new Error(`External planning source ${source.ref} cannot be promoted into canonical runtime authority`);
    if (source.kind === "project_model_object") {
      const objectEntry = objects.get(source.objectId);
      if (!objectEntry || objectEntry.collection !== source.collection) throw new Error(`Planning source ${source.collection}/${source.objectId} does not resolve exactly in project-model/model.json`);
      const { collection, object } = objectEntry;
      const exactSemanticHash = semanticHash(collection, object);
      if (source.semanticHash !== exactSemanticHash) throw new Error(`Source mismatch: ${collection}/${object.id} semantic hash differs from the thin plan`);
      if (GOVERNING_COLLECTIONS.has(collection)) {
        if (object.state !== "accepted" || object.acceptance?.contentHash !== exactSemanticHash) throw new Error(`Planning source ${object.id} is not exact accepted governing model authority`);
        governing.push({ collection: collection as ResolvedSources["governing"][number]["collection"], id: object.id, semanticHash: exactSemanticHash, acceptanceContentHash: object.acceptance.contentHash, object });
      } else if (CONTEXT_COLLECTIONS.has(collection)) {
        context.push({ collection: collection as ResolvedSources["context"][number]["collection"], id: object.id, semanticHash: exactSemanticHash });
      } else {
        throw new Error(`Planning source ${object.id} has unsupported runtime model collection ${collection}`);
      }
      continue;
    }
    const projections = projectionsByPath.get(source.path) ?? [];
    if (projections.length !== 1) throw new Error(`Planning source ${source.path} does not resolve exactly in project-model/model.json`);
    const projection = projections[0];
    await assertTrackedBaselineBytes(repositoryRoot, projection.path, plan.repository.baselineCommit);
    const bytes = await readSafeRegularFile(repositoryRoot, projection.path);
    const exactContentHash = hashBytes(bytes);
    if (source.contentHash !== exactContentHash) throw new Error(`Source mismatch: ${source.path} content hash differs from the thin plan`);
    specs.push({
      projectionId: projection.id,
      projectionContract: "1",
      modelInputHash: canonicalHash({ modelHash: modelHash(model), projection }),
      contentHash: exactContentHash,
    });
  }
  if (governing.length === 0) throw new Error("At least one exact accepted governing model source is required");
  governing.sort((left, right) => `${left.collection}/${left.id}`.localeCompare(`${right.collection}/${right.id}`));
  context.sort((left, right) => `${left.collection}/${left.id}`.localeCompare(`${right.collection}/${right.id}`));
  specs.sort((left, right) => left.projectionId.localeCompare(right.projectionId));
  assertUnique(governing.map(({ collection, id }) => `${collection}/${id}`), "governing model sources");
  assertUnique(context.map(({ collection, id }) => `${collection}/${id}`), "context model sources");
  assertUnique(specs.map(({ projectionId }) => projectionId), "spec projection sources");
  return { model, governing, context, specs };
}

function compileCanonicalPlan(
  thin: DagPlanningPlanV1,
  sources: ResolvedSources,
  procedures: Record<string, ProcedureCatalogBindingV1>,
  profiles: Record<string, IntegrationValidationProfileMappingV1>,
  createdAt: string,
): CanonicalDagPlanV1 {
  const content = <T extends Record<string, unknown>>(value: T) => ({ ...value, contentHash: canonicalHash(value) });
  const sourceRefs = sources.governing.map(({ collection, id, semanticHash }) => ({ collection, id, semanticHash }));
  const subjects = thin.workItems.map((item) => content({ subjectId: derivedId("subject", thin.planHash, item.id), kind: "behavior" as const, title: compactTitle(item.title), description: item.objective }));
  const subjectByItem = new Map(thin.workItems.map((item, index) => [item.id, subjects[index]]));
  const oracles = thin.architecture.outcomes.map((outcome) => {
    const owner = thin.workItems.find((item) => item.outcomeIds.includes(outcome.id))!;
    const subject = subjectByItem.get(owner.id)!;
    const assertion = content({
      assertionId: derivedId("assertion", thin.planHash, outcome.id), subjectId: subject.subjectId,
      observationMethod: "external_observation" as const, procedureId: derivedId("oracle-procedure", thin.planHash, outcome.id),
      passCondition: `The independent owned evaluator completes after assessing: ${outcome.description}`, failureSignals: ["The evaluator reports needs-attention/failure or the exact candidate boundary is violated."],
      tolerance: "Exact compatibility-adapter evidence is required.", environmentProfileId: "thin-plan-runtime-v1",
      requiredEvidenceClass: "independent" as const,
    });
    return content({ oracleId: derivedId("oracle", thin.planHash, outcome.id), title: compactTitle(outcome.description), sourceRefs, assertions: [assertion] });
  });
  const oracleByOutcome = new Map(thin.architecture.outcomes.map((outcome, index) => [outcome.id, oracles[index].oracleId]));
  const outcomes = thin.architecture.outcomes.map((outcome) => content({ outcomeId: outcome.id, title: compactTitle(outcome.description), description: outcome.description, oracleIds: [oracleByOutcome.get(outcome.id)!].sort() }));
  const nonGoals = thin.architecture.nonGoals.map((description, index) => content({ nonGoalId: derivedId("non-goal", thin.planHash, String(index)), title: compactTitle(description), description }));
  const component = content({
    componentId: derivedId("component", thin.planHash), title: compactTitle(thin.title),
    responsibilities: thin.architecture.notes.length ? thin.architecture.notes : [thin.source.scopeSummary],
    subjectIds: subjects.map(({ subjectId }) => subjectId).sort(), contractIds: [],
  });
  const architecture = content({
    outcomes, nonGoals, components: [component], contracts: [],
    risks: thin.architecture.risks.map((risk, index) => content({ riskId: derivedId("risk", thin.planHash, String(index)), title: compactTitle(risk), severity: "medium" as const, subjectIds: [], mitigation: "Address through the mapped work-item checks and lifecycle." })),
    assumptions: thin.architecture.notes.map((note, index) => content({ assumptionId: derivedId("assumption", thin.planHash, String(index)), title: compactTitle(note), body: note, subjectIds: [] })),
    effectScopes: [],
  });
  const trainId = derivedId("train", thin.planHash, thin.repository.repositoryId);
  const workItems = thin.workItems.map((item) => {
    const extraContext = [thin.source.scopeSummary, ...item.context, ...(item.constraints ?? [])].join("\n\n");
    if (extraContext.length > 32_768) throw new Error(`Work item ${item.id} context exceeds the canonical runtime bound`);
    return content({
      workItemId: item.id, kind: "change" as const, title: compactTitle(item.title), objective: item.objective,
      writeRepositoryId: thin.repository.repositoryId, outcomeIds: [...item.outcomeIds].sort(), nonGoalIds: [],
      modelRefs: sourceRefs, contractIds: [], oracleIds: item.outcomeIds.map((id) => oracleByOutcome.get(id)!).sort(),
      extraContext, contextRefs: [], semanticReads: [], semanticWrites: [{ subjectId: subjectByItem.get(item.id)!.subjectId, mode: "extend" as const, compatibility: "compatible" as const, migrationProtocolId: null }],
      risk: { tier: item.risk, reasons: [...item.riskNotes], hardeningProfileIds: [] }, capabilities: [],
      checks: item.checks.map((check, index) => ({ checkId: derivedId("check", thin.planHash, item.id, String(index)), phases: ["F2" as const], applicability: "required" as const, reason: check, condition: null })),
      pathEvidence: [], resourceDemands: [], integration: { trainIds: [trainId], effectScopeIds: [], migrationProtocolIds: [] },
    });
  });
  const precedence = thin.workItems.flatMap((item) => item.dependsOn.map((dependency) => content({
    precedenceId: derivedId("precedence", thin.planHash, dependency, item.id), predecessorWorkItemId: dependency,
    successorWorkItemId: item.id, subjectIds: [subjectByItem.get(item.id)!.subjectId], reason: `${item.id} depends on ${dependency}.`,
    releaseDisposition: "integrated" as const, evidenceRefs: [],
  }))).sort((left, right) => left.precedenceId.localeCompare(right.precedenceId));
  const dependencyPairs = new Set(precedence.map(({ predecessorWorkItemId, successorWorkItemId }) => [predecessorWorkItemId, successorWorkItemId].sort().join("\0")));
  const semanticMutexes = thin.constraints.mutexGroups.flatMap((group) => {
    const pairs: Array<[string, string]> = [];
    for (let left = 0; left < group.workItemIds.length; left += 1) for (let right = left + 1; right < group.workItemIds.length; right += 1) {
      const pair = [group.workItemIds[left], group.workItemIds[right]].sort() as [string, string];
      if (!dependencyPairs.has(pair.join("\0"))) pairs.push(pair);
    }
    return pairs.map((pair, index) => content({
      mutexGroupId: pairs.length === 1 ? group.id : derivedId("mutex", thin.planHash, group.id, String(index)),
      subjectIds: pair.map((id) => subjectByItem.get(id)!.subjectId).sort(),
      members: pair.map((workItemId) => ({ workItemId, phases: [...PLAN_STAGE_IDS] })), reason: group.reason,
      confidence: "high" as const, evidenceRefs: [],
    }));
  }).sort((left, right) => left.mutexGroupId.localeCompare(right.mutexGroupId));
  const orderedItems = topologicalWorkItems(thin);
  const prefixProfile = Object.entries(profiles).find(([, profile]) => profile.profileId === "thin-plan-prefix-v1")!;
  const finalProfile = Object.entries(profiles).find(([, profile]) => profile.profileId === "thin-plan-final-v1")!;
  const train = content({
    trainId, repositoryId: thin.repository.repositoryId, strategy: "merge_tree_one_parent" as const,
    members: orderedItems.map(({ id: workItemId }, ordinal) => ({ workItemId, ordinal })),
    partialIntegrationPrecedenceIds: precedence.map(({ precedenceId }) => precedenceId).sort(), compositionProfileHash: canonicalHash({ kind: "thin-plan-composition-v1" }),
    prefixValidationProfileId: prefixProfile[1].profileId, prefixValidationProfileHash: prefixProfile[0],
    finalValidationProfileId: finalProfile[1].profileId, finalValidationProfileHash: finalProfile[0],
  });
  const closureEntries = sources.governing.map(({ collection, id, semanticHash, acceptanceContentHash }) => ({ collection, id, effectiveState: "accepted" as const, semanticHash, acceptanceContentHash }));
  const selectedWorkstreamIds = [...new Set(sources.governing.flatMap(({ object }) => object.scope.kind === "workstreams" ? object.scope.workstreamIds : []))].sort();
  const selectorCore = { version: "thin-plan-source-join-v1", selectedWorkstreamIds, explicitSeedIds: sources.governing.map(({ id }) => id).sort() };
  const modelBindingCore = {
    projectId: sources.model.project.id, schemaVersion: sources.model.schemaVersion, revision: sources.model.project.revision,
    modelHash: modelHash(sources.model), selector: { ...selectorCore, selectorHash: canonicalHash(selectorCore) },
    closure: { entries: closureEntries, closureHash: canonicalHash(closureEntries) }, contextRefs: sources.context, specs: sources.specs,
  };
  const lifecycleProfileHash = canonicalHash(Object.values(procedures).sort((left, right) => left.procedureId.localeCompare(right.procedureId)));
  const checkCatalogHash = canonicalHash(workItems.map(({ workItemId, checks }) => ({ workItemId, checks })));
  const artifactPolicyCore = { maxInlineBytes: 4096, maxArtifactBytes: 4 * 1024 * 1024, maxArtifactsPerWorkItem: 64, allowedRoots: [".ai/dag-artifacts"], allowedMediaTypes: ["application/json"], defaultRetention: "run", redactRestrictedLocators: true } as const;
  return sealCanonicalDagPlanV1({
    schemaVersion: 1, kind: "CanonicalDagPlanV1", canonicalization: "jcs-v1", planId: thin.planId, revision: 1, createdAt,
    generator: { name: "thin-plan-runtime-adapter", version: "1", profileHash: canonicalHash({ adapter: "thin-plan-runtime-adapter-v1", thinPlanHash: thin.planHash, thinRevision: thin.revision }) },
    modelBinding: { ...modelBindingCore, bindingHash: canonicalHash(modelBindingCore) },
    repositories: [content({ repositoryId: thin.repository.repositoryId, role: "write" as const, locator: null, baseline: { repositoryId: thin.repository.repositoryId, commit: thin.repository.baselineCommit, tree: thin.repository.baselineTree }, targetRef: targetRef(thin.repository.targetBranch) })],
    architecture, semanticSubjects: subjects, acceptanceOracles: oracles, workItems, gates: [],
    constraints: { precedence, semanticMutexes, resourceClasses: [], integrationTrains: [train], migrationProtocols: [] },
    lifecycleBinding: { profileId: "thin-plan-lifecycle-v1", profileHash: lifecycleProfileHash, checkCatalogHash, retryPolicyHash: canonicalHash({ policy: "thin-plan-bounded-retry-v1" }), schedulerPolicyVersion: "sticky-lanes-v1", schedulerPolicyHash: DAG_SCHEDULER_POLICY_HASH_V1, stages: [...PLAN_STAGE_IDS] },
    artifactPolicy: { profileId: "thin-plan-artifacts-v1", profileHash: canonicalHash(artifactPolicyCore), ...artifactPolicyCore },
    projectionContract: { version: "1", projections: [content({ kind: "dag_execution" as const, version: "1", executable: false })] },
  });
}

function buildGenesis(
  input: PrepareDagRunV1Input,
  _repositoryRoot: string,
  plan: CanonicalDagPlanV1,
  schedulerIndex: ReturnType<typeof buildSchedulerPlanIndexV1>,
  procedures: Record<string, ProcedureCatalogBindingV1>,
  integrationValidationProfiles: Record<string, IntegrationValidationProfileMappingV1>,
): { genesis: DagRunStateV1; context: DagRunValidationContextV1; seedFacts: Record<string, unknown>[] } {
  const at = input.createdAt;
  const reviewFact = shimFact("plan_review", derivedId("review", plan.planHash), at, { thinPlanHash: input.planningPlan.planHash, approvedBy: input.planningPlan.approval.by });
  const authorizationFact = shimFact("plan_authorization", derivedId("authorization", plan.planHash), at, { thinPlanHash: input.planningPlan.planHash, authorizedBy: input.planningPlan.authorization.by, scope: input.planningPlan.authorization.scope });
  const freshnessFact = shimFact("staleness", derivedId("freshness", plan.planHash), at, { classification: "valid_exact", baseline: plan.repositories[0].baseline });
  const repositoryObservation = shimFact("repository_observation", derivedId("repository-observation", plan.planHash), at, { targetRef: plan.repositories[0].targetRef, observed: plan.repositories[0].baseline });
  const reviewRef = factRef("plan_review", String(reviewFact.id), reviewFact);
  const authorizationRef = factRef("plan_authorization", String(authorizationFact.id), authorizationFact);
  const freshnessRef = factRef("staleness", String(freshnessFact.id), freshnessFact);
  const maxActiveNodes = Math.min(input.planningPlan.authorization.maxConcurrency!, input.planningPlan.constraints.maxConcurrency ?? Number.MAX_SAFE_INTEGER, plan.workItems.length);
  const authorizationCore = {
    planHash: plan.planHash, reviewReceiptHash: reviewFact.hash, receiptHashes: [authorizationFact.hash],
    workItemIds: [...input.planningPlan.authorization.scope].sort(),
    stageScopes: Object.fromEntries(input.planningPlan.authorization.scope.map((workItemId) => [workItemId, [...PLAN_STAGE_IDS]])),
    repositoryIds: plan.repositories.map(({ repositoryId }) => repositoryId).sort(), effectScopeIds: [],
    integrationTrainIds: plan.constraints.integrationTrains.map(({ trainId }) => trainId).sort(), retryCeilingsHash: plan.lifecycleBinding.retryPolicyHash,
    maxActiveNodes, validFrom: input.planningPlan.authorization.at!, validUntil: null,
  };
  const authorization: DagRunAuthorizationBindingV1 = { ...authorizationCore, hash: canonicalHash(authorizationCore) };
  const authorizationSetRef = factRef("authorization_set", derivedId("authorization-set", plan.planHash), authorization);
  const repositories = Object.fromEntries(plan.repositories.map((repository) => [repository.repositoryId, {
    repositoryId: repository.repositoryId, planEntityHash: repository.contentHash, role: repository.role, baseline: repository.baseline,
    targetRef: repository.targetRef, observedTarget: repository.baseline, observedTargetAt: at, observationReceipt: repositoryObservation.hash,
    workspace: { state: "unmaterialized", locator: null, gitCommonDirIdentityHash: null, gitWorktreeIdentityHash: null, branchRef: null, base: null, expectedHead: null, ownerLeaseId: null, processDisposition: "not_applicable", observationReceipt: null },
    integrationLockLeaseId: null, blockerIds: [],
  }]));
  const workItems = Object.fromEntries(plan.workItems.map((item) => {
    const precedenceIds = plan.constraints.precedence.filter(({ successorWorkItemId }) => successorWorkItemId === item.workItemId).map(({ precedenceId }) => precedenceId).sort();
    return [item.workItemId, {
    workItemId: item.workItemId, planEntityHash: item.contentHash, writeRepositoryId: item.writeRepositoryId, desired: "run", current: precedenceIds.length ? "pending" : "ready",
    authorizedStages: [...PLAN_STAGE_IDS], currentStage: null, implementationLineageHash: canonicalHash({ planHash: plan.planHash, workItemId: item.workItemId, kind: "thin-plan-implementation-lineage-v1" }),
    candidateGeneration: 0, candidate: null,
    stages: Object.fromEntries(PLAN_STAGE_IDS.map((stage) => [stage, { stage, state: "pending", attemptIds: [], currentAttemptId: null, currentEvidence: null, adoptionReceipt: null, invalidationIds: [], lastDisposition: null, blockerIds: [] }])),
    precedenceIds, gateIds: [],
    laneAdmissionSequence: null, admittedAt: null, activeLeaseIds: [], blockerIds: [], openFindingIds: [], integrationReadyReceipt: null, integrationEntryId: null, integrationReceipt: null, completedAt: null,
  }];
  }));
  const catalog: DagExecutionCatalogBindingV1 = { lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, procedures, checkAggregates: {} };
  const context: DagRunValidationContextV1 = { plan, authorization, historicalAuthorizations: {}, catalog, normalizedSchedulerIndexHash: schedulerIndex.indexHash, facts: {}, integrationValidationProfiles };
  const operationalNamespaces = ["worker.process", "role:implementation", "role:evaluation", "role:review", "role:check", ...plan.repositories.flatMap(({ repositoryId }) => [`repository-worktree:${repositoryId}`, `repository-integration:${repositoryId}`])];
  const genesis = sealDagRunStateV1({
    schemaVersion: 1, kind: "DagRunStateV1", canonicalization: "jcs-v1", runId: input.runId, runNonce: input.runNonce,
    revision: 0, previousSnapshotHash: null, createdAt: at, updatedAt: at,
    identity: { projectId: plan.modelBinding.projectId, planId: plan.planId, planRevision: plan.revision, planHash: plan.planHash, planSchemaHash: CANONICAL_DAG_PLAN_SCHEMA_HASH, lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, artifactPolicyHash: plan.artifactPolicy.profileHash, reviewReceipt: reviewRef, authorizationReceipts: [authorizationRef], authorizationSet: authorizationSetRef, previousRunId: null, supersededByRunId: null },
    owner: { ownerEpoch: 0, ownerTokenHash: null, sessionId: null, pid: 0, processStartIdentity: null, lockIdentity: null, attachedAt: null, lastHeartbeatAt: null, ownershipReceipt: null, lastReleaseCommandId: null, lastReleasePayloadHash: null },
    desired: { run: "running", reason: null, requestedAt: at, requestedBy: "user" },
    current: { run: "active", readyWorkItemIds: Object.values(workItems).filter(({ current }) => current === "ready").map(({ workItemId }) => workItemId).sort(), activeWorkItemIds: [], blockedWorkItemIds: [], integrationReadyWorkItemIds: [], updatedByCommandId: "create-run" },
    repositories, workItems, gates: {},
    precedence: Object.fromEntries(plan.constraints.precedence.map((edge) => [edge.precedenceId, { precedenceId: edge.precedenceId, planEntityHash: edge.contentHash, predecessorWorkItemId: edge.predecessorWorkItemId, successorWorkItemId: edge.successorWorkItemId, releaseDisposition: "integrated", state: "waiting", satisfyingReceipt: null }])),
    resourcePools: {}, mutexes: Object.fromEntries(plan.constraints.semanticMutexes.map((mutex) => [mutex.mutexGroupId, { mutexGroupId: mutex.mutexGroupId, planEntityHash: mutex.contentHash, activeLeaseId: null, waitingStageAttemptIds: [] }])),
    leases: {}, stageAttempts: {}, launchIntents: {}, workerBindings: {},
    evidenceIndex: { stageAttemptInputs: {}, workerResults: {}, candidates: {}, stageEvidence: {}, checkAggregates: {}, checkExecutions: {}, procedureExecutions: {}, findingCorrections: {}, checkApplicabilities: {}, environmentObservations: {}, workspaceMaterializations: {}, checkDispositions: {}, verifications: {}, oracleAssertions: {}, findings: {}, findingResolutions: {}, waivers: {}, invalidations: {}, adoptions: {}, effectReconciliations: {}, integrationReady: {}, integrationReceipts: {}, stalenessReceipts: { [freshnessFact.hash]: freshnessRef }, gateReceipts: {} },
    findingClosures: {}, retryLedger: {}, blockers: {}, effects: {}, cancellations: {}, quarantine: {}, idempotencySlots: {},
    integrationTrains: Object.fromEntries(plan.constraints.integrationTrains.map((train) => {
      const repository = plan.repositories.find(({ repositoryId }) => repositoryId === train.repositoryId)!;
      return [train.repositoryId, { repositoryId: train.repositoryId, planTrainHash: train.contentHash, strategy: train.strategy, targetRef: repository.targetRef, expectedTarget: repository.baseline, acceptedPrefix: repository.baseline, acceptedPrefixOrdinal: 0, acceptedPrefixReceipt: null, entryOrder: [], entries: {}, activeIntegrationAttemptId: null, lockLeaseId: null, blockerIds: [] }];
    })),
    integrationAttempts: {},
    scheduler: { policyVersion: "sticky-lanes-v1", policyHash: DAG_SCHEDULER_POLICY_HASH_V1, normalizedIndexHash: schedulerIndex.indexHash, maxActiveNodes, decisionSequence: 0, nextReservationSequence: 1, lastDecisionCommandId: null, activeNodeLanes: {}, reservations: {}, bypassCounters: {}, fairnessCounters: {}, dynamicExclusions: {}, providerHoldIds: [], operationalCapacities: Object.fromEntries(operationalNamespaces.map((namespace) => [namespace, { namespace, observedCapacity: namespace.startsWith("repository-integration:") ? 1 : Math.max(1, maxActiveNodes), allocatedUnits: 0, reservationIds: [], observationHash: repositoryObservation.hash }])) },
    freshness: { class: "valid_exact", receipt: freshnessRef, evaluatedPlanHash: plan.planHash, modelClosureHash: plan.modelBinding.closure.closureHash, repositoryObservationHashes: Object.fromEntries(plan.repositories.map(({ repositoryId }) => [repositoryId, repositoryObservation.hash])), affectedWorkItemIds: [], blocksNewLaunches: false, blocksIntegration: false, evaluatedAt: at },
    completion: { state: "open", authorizedScopeHash: authorization.hash, completeWorkItemIds: [], remainingAuthorizedWorkItemIds: Object.keys(workItems).sort(), unauthorizedWorkItemIds: [], completedRepositoryIds: [], completedAt: null },
  }, context);
  return { genesis, context, seedFacts: [reviewFact, authorizationFact, freshnessFact, repositoryObservation, authorization] };
}

async function executeLifecycleProcedure(
  repositoryRoot: string,
  plan: CanonicalDagPlanV1,
  state: DagRunStateV1,
  attempt: StageAttempt,
  procedure: ProcedureCatalogBindingV1,
  signal?: AbortSignal,
): Promise<DagProcedureExecutionResultV1> {
  signal?.throwIfAborted?.();
  const item = state.workItems[attempt.workItemId];
  const planItem = plan.workItems.find(({ workItemId }) => workItemId === attempt.workItemId);
  const repository = item && plan.repositories.find(({ repositoryId }) => repositoryId === item.writeRepositoryId);
  if (!item || !planItem || !repository) throw new Error("Lifecycle procedure attempt does not resolve its exact plan item and repository");
  if (procedure.stages.length !== 1 || procedure.stages[0] !== attempt.stage || procedure.producerKinds.length !== 1 || procedure.producerKinds[0] !== attempt.producerKind) throw new Error("Lifecycle procedure does not exactly match the stage producer");
  const candidateGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
  const candidateHash = attempt.stage === "F0" ? null : item.candidate?.candidateHash ?? null;
  if (attempt.stage !== "F0" && (!candidateHash || !item.candidate)) throw new Error(`${attempt.stage} lifecycle evidence requires an exact current candidate`);
  const candidateTree = item.candidate?.git ?? repository.baseline;
  let workerResult: any = null;
  if (attempt.workerResult) {
    const store = new DagRunSnapshotStoreV1(join(repositoryRoot, RUN_ROOT), state.runId);
    workerResult = await store.readImmutableFact(attempt.workerResult.hash);
    if (workerResult?.kind !== "worker_result" || workerResult.hash !== attempt.workerResult.hash
      || workerResult.runId !== state.runId || workerResult.runNonce !== state.runNonce
      || workerResult.stageAttemptId !== attempt.stageAttemptId || workerResult.stage !== attempt.stage
      || workerResult.workItemId !== attempt.workItemId) {
      throw new Error("Lifecycle procedure worker result does not bind the exact stage attempt");
    }
  }
  let disposition: "PASS" | "FAIL" | "BLOCKED" | "BUDGET_EXHAUSTED" = attempt.producerKind === "owned_worker"
    ? workerResult?.terminalStatus === "succeeded" ? "PASS" : workerResult ? "FAIL" : "BLOCKED"
    : "PASS";
  let observationHash = workerResult?.hash ?? canonicalHash({ repository: repository.baseline, stage: attempt.stage });
  try {
    const commit = candidateTree.commit;
    const observedTree = await git(repositoryRoot, ["rev-parse", `${commit}^{tree}`], signal);
    if (observedTree !== candidateTree.tree) throw new Error("candidate tree mismatch");
    await git(repositoryRoot, ["cat-file", "-e", `${commit}^{commit}`], signal);
    await git(repositoryRoot, ["diff-tree", "--check", "--root", commit], signal);
    const ancestor = await gitResult(repositoryRoot, ["merge-base", "--is-ancestor", repository.baseline.commit, commit], signal);
    if (ancestor.exitCode !== 0) throw new Error("candidate is not descended from the exact baseline");
    if (attempt.stage === "F7" && await git(repositoryRoot, ["status", "--porcelain=v2", "--untracked-files=all"], signal) !== "") throw new Error("F7 requires a clean repository root");
    if (workerResult && ["F2", "F5", "F6"].includes(attempt.stage)) {
      if (workerResult.outputRepositoryId !== repository.repositoryId
        || workerResult.outputCommit !== candidateTree.commit || workerResult.outputTree !== candidateTree.tree
        || canonicalHash(workerResult.outputSourceBase) !== canonicalHash(candidateTree)
        || !workerResult.outputCommonDirIdentityHash || !workerResult.outputWorktreeIdentityHash) {
        throw new Error(`${attempt.stage} worker result does not bind the exact read-only candidate workspace`);
      }
    }
    observationHash = workerResult?.hash ?? canonicalHash({ repositoryId: repository.repositoryId, commit, tree: observedTree, stage: attempt.stage });
  } catch (error: any) {
    if (signal?.aborted || error?.name === "AbortError" || error?.code === "ABORT_ERR") throw error;
    disposition = "FAIL";
  }
  const common = {
    planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash,
    workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash,
  };
  let workspaceMaterialization: Record<string, unknown> | undefined;
  let environmentObservation: Record<string, unknown> | undefined;
  if (ENVIRONMENT_STAGES.has(attempt.stage)) {
    if (!candidateHash || !item.candidate) disposition = "BLOCKED";
    const identities = workerResult && ["F2", "F5"].includes(attempt.stage)
      ? { commonDirIdentityHash: workerResult.outputCommonDirIdentityHash, worktreeIdentityHash: workerResult.outputWorktreeIdentityHash }
      : await repositoryIdentities(repositoryRoot, signal);
    if (!identities.commonDirIdentityHash || !identities.worktreeIdentityHash) disposition = "FAIL";
    workspaceMaterialization = withHash({ kind: "workspace_materialization", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, repositoryId: repository.repositoryId, candidateGeneration, candidateHash: candidateHash!, candidateTree, ...identities, materializedAt: state.updatedAt });
    const { authorizationSetHash: _authorizationSetHash, ...environmentCommon } = common;
    environmentObservation = withHash({ kind: "environment_observation", ...environmentCommon, repositoryId: repository.repositoryId, candidateGeneration, candidateHash: candidateHash!, candidateTree, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: workspaceMaterialization.hash, ...identities, cleanliness: disposition === "PASS" ? "clean" : "unknown", observedAt: state.updatedAt });
  }
  const oracleAssertions = attempt.stage === "F2" ? planItem.oracleIds.flatMap((oracleId) => {
    const oracle = plan.acceptanceOracles.find((candidate) => candidate.oracleId === oracleId)!;
    return oracle.assertions.map((assertion) => withHash({ kind: "oracle_assertion", ...common, stage: "F2" as const, oracleId, assertionId: assertion.assertionId, procedureId: assertion.procedureId, environmentProfileId: assertion.environmentProfileId, observationMethod: assertion.observationMethod, requiredEvidenceClass: assertion.requiredEvidenceClass, disposition, observationHash: attempt.workerResult?.hash ?? observationHash }));
  }) : [];
  const applicableChecks = planItem.checks.filter(({ phases }) => phases.includes(attempt.stage));
  const checkExecutions = applicableChecks.map((check) => withHash({ kind: "check_execution", ...common, candidateGeneration, candidateHash, checkId: check.checkId, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation?.hash ?? null, executionId: derivedId("check-execution", state.runNonce, attempt.stageAttemptId, check.checkId), disposition, startedAt: state.updatedAt, completedAt: state.updatedAt }));
  const aggregate = withHash({ kind: "check_aggregate", ...common, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition, oracleIds: [...planItem.oracleIds], assertions: oracleAssertions.map((fact) => ({ oracleId: fact.oracleId, assertionId: fact.assertionId, evidenceHash: fact.hash })), checks: applicableChecks.map((check, index) => ({ checkId: check.checkId, disposition, executionEvidenceHash: checkExecutions[index].hash, applicabilityEvidenceHashes: [] })) });
  const evidence = withHash({ kind: "stage_evidence", ...common, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: aggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration, candidateHash, producerKind: attempt.producerKind, producerResultHash: attempt.workerResult?.hash ?? null, disposition, environmentObservationHash: environmentObservation?.hash ?? null, producedAt: state.updatedAt, readOnly: procedure.readOnly });
  const output: DagProcedureExecutionResultV1 = { checkAggregate: aggregate, evidence, oracleAssertions, checkDispositions: [], checkExecutions, checkAuthorities: [], ...(workspaceMaterialization && environmentObservation ? { workspaceMaterialization, environmentObservation } : {}) };
  if (attempt.stage === "F8") output.integrationReady = withHash({ kind: "integration_ready", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, candidateGeneration, candidateHash: candidateHash!, f8EvidenceHash: evidence.hash, allRequiredChecksPassed: disposition === "PASS", effectsReconciled: true, findingsClosed: true });
  return output;
}

async function executableIdentity(): Promise<{ node: string; nodeHash: string; helperHash: string }> {
  const node = await realpath(process.execPath);
  return { node, nodeHash: hashBytes(await readFile(node)), helperHash: hashBytes(await readFile(HELPER_PATH)) };
}

function procedureCatalog(executableIdentity: { node: string; nodeHash: string }): Record<string, ProcedureCatalogBindingV1> {
  const environment = { LC_ALL: "C", LANG: "C" };
  const environmentProfileId = "thin-plan-lifecycle-env-v1";
  const environmentProfileHash = canonicalHash({ profileId: environmentProfileId, environment });
  return Object.fromEntries(PLAN_STAGE_IDS.map((stage) => {
    const readOnly = !WRITING_STAGES.has(stage);
    const executable = { executableArtifactHash: executableIdentity.nodeHash, argv: [executableIdentity.node, HELPER_PATH, "--lifecycle", stage], cwdMode: "repository_root" as const, environmentProfileId, environmentProfileHash, environmentHash: canonicalHash(environment), timeoutMs: 30_000, readOnly, noEdit: readOnly };
    const core = { procedureId: `thin-plan-${stage.toLowerCase()}-v1`, purpose: "lifecycle" as const, stages: [stage], producerKinds: [PRODUCER_BY_STAGE[stage]], readOnly, environmentProfileHash, executable };
    const hash = canonicalHash(core);
    return [hash, { ...core, hash }];
  }));
}

function isBuiltInProcedure(procedure: ProcedureCatalogBindingV1, executableIdentity: { node: string; nodeHash: string }): boolean {
  const stage = procedure.stages.length === 1 ? procedure.stages[0] : null;
  if (!stage) return false;
  const readOnly = !WRITING_STAGES.has(stage);
  const environment = { LC_ALL: "C", LANG: "C" };
  const environmentProfileHash = canonicalHash({ profileId: "thin-plan-lifecycle-env-v1", environment });
  return procedure.hash === canonicalHash(withoutHash(procedure))
    && procedure.procedureId === `thin-plan-${stage.toLowerCase()}-v1`
    && procedure.purpose === "lifecycle"
    && procedure.producerKinds.length === 1
    && procedure.producerKinds[0] === PRODUCER_BY_STAGE[stage]
    && procedure.readOnly === readOnly
    && procedure.environmentProfileHash === environmentProfileHash
    && procedure.executable.argv.length === 4
    && procedure.executable.argv[0] === executableIdentity.node
    && procedure.executable.executableArtifactHash === executableIdentity.nodeHash
    && procedure.executable.argv[1] === HELPER_PATH
    && procedure.executable.argv[2] === "--lifecycle"
    && procedure.executable.argv[3] === stage
    && procedure.executable.cwdMode === "repository_root"
    && procedure.executable.environmentProfileId === "thin-plan-lifecycle-env-v1"
    && procedure.executable.environmentProfileHash === environmentProfileHash
    && procedure.executable.environmentHash === canonicalHash(environment)
    && procedure.executable.timeoutMs === 30_000
    && procedure.executable.readOnly === readOnly
    && procedure.executable.noEdit === readOnly;
}

function integrationProfiles(executableIdentity: { node: string; nodeHash: string; helperHash: string }, plan: DagPlanningPlanV1): Record<string, IntegrationValidationProfileMappingV1> {
  const environment = { LC_ALL: "C", LANG: "C", PATH: process.env.PATH ?? "/usr/bin:/bin" };
  const environmentProfileId = "thin-plan-integration-env-v1";
  const environmentProfileHash = canonicalHash({ profileId: environmentProfileId, environment });
  const executableArtifactHash = canonicalHash({
    executableHash: executableIdentity.nodeHash,
    argvArtifacts: [{ index: 1, hash: executableIdentity.helperHash }],
  });
  return Object.fromEntries((["prefix", "final"] as const).map((phase) => {
    const commands = phase === "prefix" ? plan.integration.prefixCommands : plan.integration.finalCommands;
    const encoded = JSON.stringify(commands);
    if (Buffer.byteLength(encoded) > 4_000) throw new Error(`${phase} integration validation command profile exceeds 4000 bytes`);
    const profile: IntegrationValidationProfileMappingV1 = { profileId: `thin-plan-${phase}-v1`, executableArtifactHash, argv: [executableIdentity.node, HELPER_PATH, "--commands", phase, encoded], cwdMode: "detached_proposal_worktree", environmentProfileId, environmentProfileHash, environment, environmentHash: canonicalHash(environment), timeoutMs: 120_000, readOnly: true, noEdit: true };
    return [canonicalHash(profile), profile];
  }));
}

function assertReadyForCompatibilityRun(plan: DagPlanningPlanV1, createdAt: string): void {
  if (plan.status !== "ready" || plan.approval.status !== "approved" || plan.authorization.status !== "authorized") throw new Error("DAG planning record must be ready, approved, and authorized");
  if (!plan.approval.by || !plan.approval.at || !plan.authorization.by || !plan.authorization.at || !plan.authorization.maxConcurrency || plan.authorization.scope.length === 0) throw new Error("Approved/authorized planning decisions are incomplete");
  const created = Date.parse(createdAt);
  if (!Number.isFinite(created) || new Date(created).toISOString() !== createdAt) throw new Error("createdAt must be an exact ISO UTC timestamp");
  if (Date.parse(plan.updatedAt) > created || Date.parse(plan.approval.at) > created || Date.parse(plan.authorization.at) > created) throw new Error("Run creation time predates the approved planning authority");
}

function topologicalWorkItems(plan: DagPlanningPlanV1): DagPlanningPlanV1["workItems"] {
  const rank = new Map(plan.workItems.map((item, index) => [item.id, index]));
  const byId = new Map(plan.workItems.map((item) => [item.id, item]));
  const remaining = new Map(plan.workItems.map((item) => [item.id, new Set(item.dependsOn)]));
  const output: DagPlanningPlanV1["workItems"] = [];
  while (remaining.size) {
    const ready = [...remaining.keys()].filter((id) => remaining.get(id)!.size === 0).sort((left, right) => rank.get(left)! - rank.get(right)! || left.localeCompare(right));
    if (!ready.length) throw new Error("Thin planning dependency graph is cyclic");
    for (const id of ready) {
      output.push(byId.get(id)!);
      remaining.delete(id);
      for (const dependencies of remaining.values()) dependencies.delete(id);
    }
  }
  return output;
}

async function assertTrackedBaselineBytes(repositoryRoot: string, path: string, baselineCommit: string): Promise<void> {
  await git(repositoryRoot, ["ls-files", "--error-unmatch", "--", path]);
  const current = await readSafeRegularFile(repositoryRoot, path);
  const committed = await gitBuffer(repositoryRoot, ["show", `${baselineCommit}:${path}`]);
  if (!current.equals(committed)) throw new Error(`Source mismatch: ${path} bytes differ from the declared baseline`);
}

async function readSafeRegularFile(repositoryRoot: string, path: string): Promise<Buffer> {
  const absolute = resolve(repositoryRoot, path);
  const rel = relative(repositoryRoot, absolute);
  if (!rel || rel.startsWith("..") || rel.includes(`..${sep}`)) throw new Error(`Unsafe repository source path: ${path}`);
  const metadata = await lstat(absolute);
  if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error(`Repository source must be a regular file: ${path}`);
  const canonical = await realpath(absolute);
  if (dirname(canonical) !== repositoryRoot && !canonical.startsWith(`${repositoryRoot}${sep}`)) throw new Error(`Repository source escapes the repository root: ${path}`);
  return readFile(canonical);
}

function targetRef(value: string): string {
  return value.startsWith("refs/heads/") ? value : `refs/heads/${value}`;
}

function compactTitle(value: string): string {
  const title = value.replace(/\s+/g, " ").trim();
  if (title.length <= 256) return title;
  return `${title.slice(0, 232)}…${canonicalHash(title).slice(7, 30)}`;
}

function derivedId(prefix: string, ...parts: string[]): string {
  return `${prefix}-${canonicalHash(parts).slice(7, 39)}`;
}

function shimFact(kind: string, id: string, issuedAt: string, binding: Record<string, unknown>): Record<string, unknown> & { hash: string; id: string } {
  return withHash({ kind, id, schemaVersion: 1, issuedAt, compatibility: "internal_runtime_adapter_v1", bindingHash: canonicalHash(binding) });
}

function factRef(kind: string, id: string, fact: Record<string, unknown>): any {
  return { kind, schemaVersion: 1, id, hash: fact.hash, bytes: Buffer.byteLength(canonicalStringify(fact)), mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null };
}

function withHash<T extends Record<string, any>>(core: T): T & { hash: string } {
  return { ...core, hash: canonicalHash(core) };
}

function withoutHash(value: Record<string, unknown>): Record<string, unknown> {
  const copy = { ...value };
  delete copy.hash;
  return copy;
}

function hashBytes(bytes: Uint8Array): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function assertUnique(values: string[], label: string): void {
  if (new Set(values).size !== values.length) throw new Error(`Duplicate ${label} are not compatible with CanonicalDagPlanV1`);
}

async function repositoryIdentities(repositoryRoot: string, signal?: AbortSignal): Promise<{ commonDirIdentityHash: string; worktreeIdentityHash: string }> {
  const common = resolve(repositoryRoot, await git(repositoryRoot, ["rev-parse", "--git-common-dir"], signal));
  return { commonDirIdentityHash: canonicalHash({ realPath: await realpath(common) }), worktreeIdentityHash: canonicalHash({ realPath: await realpath(repositoryRoot) }) };
}

async function git(cwd: string, args: string[], signal?: AbortSignal): Promise<string> {
  const result = await run("git", args, { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, signal, env: { ...process.env, LC_ALL: "C", LANG: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" } });
  return result.stdout.trim();
}

async function gitBuffer(cwd: string, args: string[]): Promise<Buffer> {
  const result = await run("git", args, { cwd, encoding: "buffer", maxBuffer: 16 * 1024 * 1024, env: { ...process.env, LC_ALL: "C", LANG: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" } });
  return result.stdout as Buffer;
}

async function gitResult(cwd: string, args: string[], signal?: AbortSignal): Promise<{ exitCode: number }> {
  try { await git(cwd, args, signal); return { exitCode: 0 }; }
  catch (error: any) { if (signal?.aborted || error?.name === "AbortError" || error?.code === "ABORT_ERR") throw error; return { exitCode: Number.isInteger(error?.code) ? error.code : 1 }; }
}
