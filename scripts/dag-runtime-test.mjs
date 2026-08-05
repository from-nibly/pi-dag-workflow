import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { visibleWidth } from "@earendil-works/pi-tui";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_RUN_INPUT_SCHEMA_HASH,
  DAG_SCHEDULER_POLICY_HASH_V1,
  DAG_RUN_STATE_SCHEMA_HASH,
  DagConductorServiceV1,
  DagRunSnapshotStoreV1,
  DagRunStoreCorruptError,
  DagRunStoreLockedError,
  PLAN_STAGE_IDS,
  canonicalHash,
  canonicalStringify,
  createDagRunStoreDeadOwnerProofV1,
  buildSchedulerPlanIndexV1,
  parseCanonicalDagPlanV1,
  parseDagRunStateV1,
  parseStrictJson,
  sealCanonicalDagPlanV1,
  reduceDagRunV1,
  renderDagWidgetV1,
  projectDagExecutionV1,
  registerCanonicalDagRuntime,
  requireSchedulerDispatchIntentV1,
  scheduleDagRunV1,
  sealDagRunStateV1,
  validateCanonicalDagPlanV1,
  validateDagRunStateV1,
} from "../extensions/dag-workflow/dag-runtime/index.ts";

const execFileAsync = promisify(execFile);
const H = (char) => `sha256:${char.repeat(64)}`;
const O = (char) => char.repeat(40);
const NOW = "2026-08-04T15:00:00.000Z";
const procStat = await readFile(`/proc/${process.pid}/stat`, "utf8");
const PROCESS_START_IDENTITY = `linux-proc:${procStat.slice(procStat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
const content = (value) => ({ ...value, contentHash: canonicalHash(value) });
const recontent = (value) => {
  const copy = { ...value };
  delete copy.contentHash;
  return content(copy);
};
const rehashPlan = (value) => { value.planHash = canonicalHash(Object.fromEntries(Object.entries(value).filter(([key]) => key !== "planHash"))); return value; };
const rehashRun = (value) => { value.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(value).filter(([key]) => key !== "snapshotHash"))); return value; };
const simpleFact = (kind, id) => { const core = { kind, id, schemaVersion: 1, issuedAt: NOW }; return { ...core, hash: canonicalHash(core) }; };
const ref = (kind, id, hash = H("a")) => ({
  kind, schemaVersion: 1, id, hash, bytes: 1, mediaType: "application/json",
  sensitivity: "internal", retention: "run", locator: null,
});
const clone = (value) => structuredClone(value);
const procedureCatalogFixture = () => Object.fromEntries(PLAN_STAGE_IDS.map((stage) => {
  const input = { procedureId: `procedure-${stage.toLowerCase()}`, purpose: "lifecycle", stages: [stage], producerKinds: [({ F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" })[stage]], readOnly: !["F1", "F3", "F6"].includes(stage), environmentProfileHash: H("b") };
  const hash = canonicalHash(input);
  return [hash, { ...input, hash }];
}));
const catalogBinding = (plan) => ({ lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, procedures: procedureCatalogFixture(), checkAggregates: {} });
const authorizationBinding = (plan, reviewReceiptHash, receiptHashes, _authorizationSetHash) => {
  const input = {
  planHash: plan.planHash,
  reviewReceiptHash,
  receiptHashes: [...receiptHashes].sort(),
  workItemIds: plan.workItems.map(({ workItemId }) => workItemId).sort(),
  stageScopes: Object.fromEntries(plan.workItems.map(({ workItemId }) => [workItemId, [...PLAN_STAGE_IDS]])),
  repositoryIds: plan.repositories.map(({ repositoryId }) => repositoryId).sort(),
  effectScopeIds: [],
  integrationTrainIds: plan.constraints.integrationTrains.map(({ trainId }) => trainId).sort(),
  retryCeilingsHash: plan.lifecycleBinding.retryPolicyHash,
  maxActiveNodes: 1,
  validFrom: NOW,
  validUntil: null,
  };
  return { ...input, hash: canonicalHash(input) };
};

function planFixture() {
  const decisionRef = { collection: "decisions", id: "DEC-api", semanticHash: H("2") };
  const subject = content({ subjectId: "subject-api", kind: "contract", title: "API contract", description: "Stable API behavior." });
  const assertion = content({
    assertionId: "assert-api", subjectId: subject.subjectId, observationMethod: "automated_check", procedureId: "check-api",
    passCondition: "The API returns the accepted response.", failureSignals: ["Response differs."], tolerance: "Exact match.",
    environmentProfileId: "env-test", requiredEvidenceClass: "independent",
  });
  const oracle = content({ oracleId: "oracle-api", title: "API acceptance", sourceRefs: [decisionRef], assertions: [assertion] });
  const outcome = content({ outcomeId: "outcome-api", title: "API works", description: "The API contract is implemented.", oracleIds: [oracle.oracleId] });
  const component = content({ componentId: "component-api", title: "API", responsibilities: ["Own API behavior."], subjectIds: [subject.subjectId], contractIds: ["contract-api"] });
  const contract = content({ contractId: "contract-api", title: "API contract", description: "The accepted API surface.", subjectIds: [subject.subjectId], compatibility: "compatible" });
  const architectureInput = { outcomes: [outcome], nonGoals: [], components: [component], contracts: [contract], risks: [], assumptions: [], effectScopes: [] };
  const architecture = content(architectureInput);
  const repository = content({
    repositoryId: "repo-main", role: "write", locator: null,
    baseline: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, targetRef: "refs/heads/main",
  });
  const train = content({
    trainId: "train-main", repositoryId: "repo-main", strategy: "merge_tree_one_parent",
    members: [{ workItemId: "item-api", ordinal: 0 }], partialIntegrationPrecedenceIds: [], compositionProfileHash: H("d"),
    prefixValidationProfileId: "checks-prefix", prefixValidationProfileHash: H("e"), finalValidationProfileId: "checks-final", finalValidationProfileHash: H("f"),
  });
  const workItem = content({
    workItemId: "item-api", kind: "change", title: "Implement API", objective: "Implement the accepted API contract.",
    writeRepositoryId: "repo-main", outcomeIds: [outcome.outcomeId], nonGoalIds: [], modelRefs: [decisionRef],
    contractIds: [contract.contractId], oracleIds: [oracle.oracleId], extraContext: "Preserve architecture.", contextRefs: [],
    semanticReads: [], semanticWrites: [{ subjectId: subject.subjectId, mode: "extend", compatibility: "compatible", migrationProtocolId: null }],
    risk: { tier: "medium", reasons: ["Public contract."], hardeningProfileIds: ["hardening-default"] },
    capabilities: [{ kind: "capability", capabilityId: "node", purpose: "Run implementation tools.", phases: ["F1"], environmentProfileId: "env-test" }],
    checks: [{ checkId: "check-api", phases: ["F2"], applicability: "required", reason: "The outcome requires independent evaluation.", condition: null }],
    pathEvidence: [{ path: "src/api.ts", symbol: null, basis: "Current API implementation.", confidence: "medium" }],
    resourceDemands: [], integration: { trainIds: [train.trainId], effectScopeIds: [], migrationProtocolIds: [] },
  });
  const projection = content({ kind: "dag_execution", version: "1", executable: false });
  const selectorInput = { version: "governing-v1", selectedWorkstreamIds: ["WS-agent-workflow"], explicitSeedIds: [] };
  const selector = { ...selectorInput, selectorHash: canonicalHash(selectorInput) };
  const closureEntries = [{ collection: "decisions", id: "DEC-api", effectiveState: "accepted", semanticHash: H("2"), acceptanceContentHash: H("3") }];
  const closure = { entries: closureEntries, closureHash: canonicalHash(closureEntries) };
  const modelBindingInput = {
    projectId: "project", schemaVersion: 1, revision: 1, modelHash: H("5"), selector, closure,
    contextRefs: [], specs: [{ projectionId: "SPEC-api", projectionContract: "1", modelInputHash: H("6"), contentHash: H("7") }],
  };
  const modelBinding = { ...modelBindingInput, bindingHash: canonicalHash(modelBindingInput) };
  const lifecycleProfileHash = canonicalHash(Object.values(procedureCatalogFixture()).sort((left, right) => left.procedureId.localeCompare(right.procedureId)));
  const checkCatalogHash = canonicalHash([{ workItemId: workItem.workItemId, checks: workItem.checks }]);
  return sealCanonicalDagPlanV1({
    schemaVersion: 1, kind: "CanonicalDagPlanV1", canonicalization: "jcs-v1", planId: "plan-api", revision: 1,
    createdAt: NOW, generator: { name: "pi-dag-workflow", version: "1", profileHash: H("8") },
    modelBinding, repositories: [repository], architecture, semanticSubjects: [subject], acceptanceOracles: [oracle], workItems: [workItem], gates: [],
    constraints: { precedence: [], semanticMutexes: [], resourceClasses: [], integrationTrains: [train], migrationProtocols: [] },
    lifecycleBinding: { profileId: "lifecycle-v1", profileHash: lifecycleProfileHash, checkCatalogHash, retryPolicyHash: H("b"), schedulerPolicyVersion: "sticky-lanes-v1", schedulerPolicyHash: DAG_SCHEDULER_POLICY_HASH_V1, stages: [...PLAN_STAGE_IDS] },
    artifactPolicy: { profileId: "artifact-v1", profileHash: H("c"), maxInlineBytes: 4096, maxArtifactBytes: 65536, maxArtifactsPerWorkItem: 32, allowedRoots: [".ai/dag-artifacts"], allowedMediaTypes: ["application/json"], defaultRetention: "run", redactRestrictedLocators: true },
    projectionContract: { version: "1", projections: [projection] },
  });
}

function runFixture(plan) {
  const emptyStages = Object.fromEntries(PLAN_STAGE_IDS.map((stage) => [stage, {
    stage, state: "pending", attemptIds: [], currentAttemptId: null, currentEvidence: null, adoptionReceipt: null,
    invalidationIds: [], lastDisposition: null, blockerIds: [],
  }]));
  const repository = {
    repositoryId: "repo-main", planEntityHash: plan.repositories[0].contentHash, role: "write", baseline: plan.repositories[0].baseline,
    targetRef: "refs/heads/main", observedTarget: plan.repositories[0].baseline, observedTargetAt: NOW, observationReceipt: simpleFact("repository_observation", "repo-main-observation").hash,
    workspace: { state: "unmaterialized", locator: null, gitCommonDirIdentityHash: null, gitWorktreeIdentityHash: null, branchRef: null, base: null, expectedHead: null, ownerLeaseId: null, processDisposition: "not_applicable", observationReceipt: null },
    integrationLockLeaseId: null, blockerIds: [],
  };
  const item = {
    workItemId: "item-api", planEntityHash: plan.workItems[0].contentHash, writeRepositoryId: "repo-main", desired: "run", current: "pending",
    authorizedStages: [...PLAN_STAGE_IDS], currentStage: null, implementationLineageHash: H("7"), candidateGeneration: 0, candidate: null, stages: emptyStages,
    precedenceIds: [], gateIds: [], laneAdmissionSequence: null, admittedAt: null, activeLeaseIds: [], blockerIds: [], openFindingIds: [], integrationReadyReceipt: null,
    integrationEntryId: null, integrationReceipt: null, completedAt: null,
  };
  const reviewFact = simpleFact("plan_review", "review-plan");
  const authorizationFact = simpleFact("plan_authorization", "authorization-plan");
  const freshnessFact = simpleFact("staleness", "freshness");
  const review = ref("plan_review", "review-plan", reviewFact.hash);
  const authorization = ref("plan_authorization", "authorization-plan", authorizationFact.hash);
  const freshness = ref("staleness", "freshness", freshnessFact.hash);
  const authorizationContext = authorizationBinding(plan, review.hash, [authorization.hash], null);
  const authorizationSet = ref("authorization_set", "authorization-set", authorizationContext.hash);
  return sealDagRunStateV1({
    schemaVersion: 1, kind: "DagRunStateV1", canonicalization: "jcs-v1", runId: "run-api", runNonce: "0123456789abcdef",
    revision: 0, previousSnapshotHash: null, createdAt: NOW, updatedAt: NOW,
    identity: { projectId: "project", planId: plan.planId, planRevision: plan.revision, planHash: plan.planHash, planSchemaHash: CANONICAL_DAG_PLAN_SCHEMA_HASH, lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, artifactPolicyHash: plan.artifactPolicy.profileHash, reviewReceipt: review, authorizationReceipts: [authorization], authorizationSet, previousRunId: null, supersededByRunId: null },
    owner: { ownerEpoch: 0, ownerTokenHash: null, sessionId: null, pid: 0, processStartIdentity: null, lockIdentity: null, attachedAt: null, lastHeartbeatAt: null, ownershipReceipt: null, lastReleaseCommandId: null, lastReleasePayloadHash: null },
    desired: { run: "running", reason: null, requestedAt: NOW, requestedBy: "user" },
    current: { run: "initializing", readyWorkItemIds: [], activeWorkItemIds: [], blockedWorkItemIds: [], integrationReadyWorkItemIds: [], updatedByCommandId: "create-run" },
    repositories: { "repo-main": repository }, workItems: { "item-api": item }, gates: {}, precedence: {}, resourcePools: {}, mutexes: {}, leases: {},
    stageAttempts: {}, launchIntents: {}, workerBindings: {},
    evidenceIndex: { stageAttemptInputs: {}, workerResults: {}, candidates: {}, stageEvidence: {}, checkDispositions: {}, verifications: {}, oracleAssertions: {}, findings: {}, findingResolutions: {}, waivers: {}, invalidations: {}, adoptions: {}, effectReconciliations: {}, integrationReady: {}, integrationReceipts: {}, stalenessReceipts: { [freshness.hash]: freshness }, gateReceipts: {} },
    findingClosures: {}, retryLedger: {}, blockers: {}, effects: {}, cancellations: {}, quarantine: {}, idempotencySlots: {},
    integrationTrains: { "repo-main": { repositoryId: "repo-main", planTrainHash: plan.constraints.integrationTrains[0].contentHash, strategy: "merge_tree_one_parent", targetRef: "refs/heads/main", expectedTarget: plan.repositories[0].baseline, acceptedPrefix: plan.repositories[0].baseline, acceptedPrefixOrdinal: 0, acceptedPrefixReceipt: null, entryOrder: [], entries: {}, activeIntegrationAttemptId: null, lockLeaseId: null, blockerIds: [] } },
    integrationAttempts: {},
    scheduler: { policyVersion: "sticky-lanes-v1", policyHash: DAG_SCHEDULER_POLICY_HASH_V1, normalizedIndexHash: H("6"), maxActiveNodes: 1, decisionSequence: 0, nextReservationSequence: 1, lastDecisionCommandId: null, activeNodeLanes: {}, reservations: {}, bypassCounters: {}, fairnessCounters: {}, dynamicExclusions: {}, providerHoldIds: [], operationalCapacities: Object.fromEntries(["worker.process", "role:implementation", "role:evaluation", "role:review", "role:check", "repository-worktree:repo-main", "repository-integration:repo-main"].map((namespace) => [namespace, { namespace, observedCapacity: namespace.startsWith("repository-integration") ? 1 : 4, allocatedUnits: 0, reservationIds: [], observationHash: H("4") }])) },
    freshness: { class: "valid_exact", receipt: freshness, evaluatedPlanHash: plan.planHash, modelClosureHash: plan.modelBinding.closure.closureHash, repositoryObservationHashes: { "repo-main": repository.observationReceipt }, affectedWorkItemIds: [], blocksNewLaunches: false, blocksIntegration: false, evaluatedAt: NOW },
    completion: { state: "open", authorizedScopeHash: authorizationSet.hash, completeWorkItemIds: [], remainingAuthorizedWorkItemIds: ["item-api"], unauthorizedWorkItemIds: [], completedRepositoryIds: [], completedAt: null },
  }, { plan, authorization: authorizationContext, historicalAuthorizations: {}, catalog: catalogBinding(plan), normalizedSchedulerIndexHash: H("6"), facts: {} });
}

function expectInvalid(validate, value, label) {
  const result = validate(value);
  assert.equal(result.ok, false, label);
  assert(result.issues.length > 0, `${label} returns issues`);
}

const plan = planFixture();
assert.match(CANONICAL_DAG_PLAN_SCHEMA_HASH, /^sha256:[0-9a-f]{64}$/, "canonical plan schema exposes a stable hash");
assert.match(DAG_RUN_STATE_SCHEMA_HASH, /^sha256:[0-9a-f]{64}$/, "run-state schema exposes a stable hash");
assert.equal(validateCanonicalDagPlanV1(plan).ok, true, "valid canonical plan passes");
assert.equal(parseCanonicalDagPlanV1(JSON.stringify(plan)).planHash, plan.planHash, "strict plan parser validates duplicate-safe canonical JSON");
assert.equal(plan.planHash, canonicalHash(Object.fromEntries(Object.entries(plan).filter(([key]) => key !== "planHash"))), "plan hash covers every field except itself");
assert.throws(() => parseStrictJson('{"a":1,"a":2}'), /duplicate object key/, "strict parser rejects duplicate keys");
assert.throws(() => parseStrictJson('{"n":9007199254740992}'), /unsafe integer/, "strict parser rejects unsafe integers");
assert.throws(() => canonicalHash({ value: Number.POSITIVE_INFINITY }), /non-finite/, "canonicalization rejects non-finite numbers");
assert.equal(JSON.stringify(parseStrictJson('{"b":2,"a":1}')), '{"b":2,"a":1}', "strict parser preserves parsed source order while hashing canonicalizes separately");
const dangerousJson = '{"safe":1,"__proto__":{"admin":true}}';
const parsedDangerous = parseStrictJson(dangerousJson);
assert(Object.prototype.hasOwnProperty.call(parsedDangerous, "__proto__"), "strict parser preserves dangerous JSON keys as own data properties");
assert.notEqual(canonicalHash(JSON.parse(dangerousJson)), canonicalHash({ safe: 1 }), "canonical hash covers __proto__ instead of colliding with a reduced object");

const parsedDangerousPlan = parseStrictJson(`${JSON.stringify(plan).slice(0, -1)},"__proto__":{"admin":true}}`);
expectInvalid(validateCanonicalDagPlanV1, parsedDangerousPlan, "closed plan validation sees and rejects dangerous unknown keys after strict parsing");
const unknownPlan = clone(plan); unknownPlan.mutableStatus = "running"; expectInvalid(validateCanonicalDagPlanV1, unknownPlan, "plan rejects unknown mutable fields");
const badPlanHash = clone(plan); badPlanHash.planHash = H("f"); expectInvalid(validateCanonicalDagPlanV1, badPlanHash, "plan rejects a stale root hash");
const badEntity = clone(plan); badEntity.workItems[0].objective = "Tampered"; badEntity.planHash = canonicalHash(Object.fromEntries(Object.entries(badEntity).filter(([key]) => key !== "planHash"))); expectInvalid(validateCanonicalDagPlanV1, badEntity, "plan rejects a stale entity hash");
const badOracle = clone(plan); badOracle.workItems[0].oracleIds = ["oracle-missing"]; badOracle.workItems[0].contentHash = canonicalHash(Object.fromEntries(Object.entries(badOracle.workItems[0]).filter(([key]) => key !== "contentHash"))); badOracle.planHash = canonicalHash(Object.fromEntries(Object.entries(badOracle).filter(([key]) => key !== "planHash"))); expectInvalid(validateCanonicalDagPlanV1, badOracle, "plan rejects missing oracle references");
const absolutePath = clone(plan); absolutePath.workItems[0].pathEvidence[0].path = "/tmp/source.ts"; expectInvalid(validateCanonicalDagPlanV1, absolutePath, "plan rejects absolute advisory paths");
const parentPath = clone(plan); parentPath.workItems[0].pathEvidence[0].path = "../source.ts"; expectInvalid(validateCanonicalDagPlanV1, parentPath, "plan rejects parent-traversing advisory paths");
const windowsPath = clone(plan); windowsPath.workItems[0].pathEvidence[0].path = "src\\api.ts"; expectInvalid(validateCanonicalDagPlanV1, windowsPath, "plan rejects non-portable backslash paths");
const windowsAbsolutePath = clone(plan); windowsAbsolutePath.workItems[0].pathEvidence[0].path = "C:/src/api.ts"; expectInvalid(validateCanonicalDagPlanV1, windowsAbsolutePath, "plan rejects Windows-absolute advisory paths");
const windowsDriveRelativePath = clone(plan); windowsDriveRelativePath.workItems[0].pathEvidence[0].path = "C:src/api.ts"; expectInvalid(validateCanonicalDagPlanV1, windowsDriveRelativePath, "plan rejects Windows drive-relative advisory paths");
const uriLikePath = clone(plan); uriLikePath.workItems[0].pathEvidence[0].path = "file:///tmp/source.ts"; expectInvalid(validateCanonicalDagPlanV1, uriLikePath, "plan rejects URI-like advisory paths");
const wrongLineage = clone(plan); wrongLineage.revision = 2; rehashPlan(wrongLineage); expectInvalid(validateCanonicalDagPlanV1, wrongLineage, "successor revision requires supersedesPlanHash");
const invalidTimestamp = clone(plan); invalidTimestamp.createdAt = "2026-99-99T15:00:00Z"; rehashPlan(invalidTimestamp); expectInvalid(validateCanonicalDagPlanV1, invalidTimestamp, "plan rejects impossible UTC timestamps");
const normalizedImpossibleTimestamp = clone(plan); normalizedImpossibleTimestamp.createdAt = "2026-02-30T15:00:00Z"; rehashPlan(normalizedImpossibleTimestamp); expectInvalid(validateCanonicalDagPlanV1, normalizedImpossibleTimestamp, "plan rejects normalized impossible calendar dates");
const leapSecondTimestamp = clone(plan); leapSecondTimestamp.createdAt = "2016-12-31T23:59:60Z"; rehashPlan(leapSecondTimestamp); assert.equal(validateCanonicalDagPlanV1(leapSecondTimestamp).ok, true, "plan accepts valid RFC 3339 leap-second timestamps");
const yearZeroLeapDay = clone(plan); yearZeroLeapDay.createdAt = "0000-02-29T00:00:00Z"; rehashPlan(yearZeroLeapDay); assert.equal(validateCanonicalDagPlanV1(yearZeroLeapDay).ok, true, "plan accepts RFC 3339 year-zero leap day");
const forbiddenStrategy = clone(plan); forbiddenStrategy.constraints.integrationTrains[0].strategy = "rebase_ff"; expectInvalid(validateCanonicalDagPlanV1, forbiddenStrategy, "plan permits only explicit-base merge-tree one-parent integration");
const earlyCausalRelease = clone(plan); earlyCausalRelease.constraints.precedence.push({ precedenceId: "edge-early", predecessorWorkItemId: "item-api", successorWorkItemId: "item-api", subjectIds: ["subject-api"], reason: "Invalid early release.", releaseDisposition: "integration_ready", evidenceRefs: [], contentHash: H("e") }); expectInvalid(validateCanonicalDagPlanV1, earlyCausalRelease, "causal consumers cannot release at integration-ready or evidence time");
const bogusModelRef = clone(plan); bogusModelRef.workItems[0].modelRefs = [{ collection: "decisions", id: "DEC-missing", semanticHash: H("2") }]; bogusModelRef.workItems[0] = recontent(bogusModelRef.workItems[0]); rehashPlan(bogusModelRef); expectInvalid(validateCanonicalDagPlanV1, bogusModelRef, "work-item model refs must resolve through governing closure category and hash");
const questionGroundedOracle = clone(plan); questionGroundedOracle.acceptanceOracles[0].sourceRefs = [{ collection: "questions", id: "Q-open", semanticHash: H("9") }]; questionGroundedOracle.acceptanceOracles[0] = recontent(questionGroundedOracle.acceptanceOracles[0]); rehashPlan(questionGroundedOracle); expectInvalid(validateCanonicalDagPlanV1, questionGroundedOracle, "open questions cannot masquerade as accepted oracle grounding");
const nonWriteRepository = clone(plan); nonWriteRepository.repositories[0].role = "input"; nonWriteRepository.repositories[0] = recontent(nonWriteRepository.repositories[0]); rehashPlan(nonWriteRepository); expectInvalid(validateCanonicalDagPlanV1, nonWriteRepository, "change work item requires a repository explicitly declared writable");
const conditionalWithoutPredicate = clone(plan); conditionalWithoutPredicate.workItems[0].checks[0].applicability = "conditional"; conditionalWithoutPredicate.workItems[0] = recontent(conditionalWithoutPredicate.workItems[0]); rehashPlan(conditionalWithoutPredicate); expectInvalid(validateCanonicalDagPlanV1, conditionalWithoutPredicate, "conditional check applicability requires a hash-bound predicate");
const customFlow = clone(plan); customFlow.workItems[0].phases = ["implement"]; expectInvalid(validateCanonicalDagPlanV1, customFlow, "work item cannot customize lifecycle phases");
const unconstrainedSharedWrite = clone(plan);
const secondItemInput = { ...unconstrainedSharedWrite.workItems[0], workItemId: "item-api-two" };
delete secondItemInput.contentHash;
unconstrainedSharedWrite.workItems.push(content(secondItemInput));
unconstrainedSharedWrite.constraints.integrationTrains[0].members.push({ workItemId: "item-api-two", ordinal: 1 });
const trainWithoutHash = { ...unconstrainedSharedWrite.constraints.integrationTrains[0] };
delete trainWithoutHash.contentHash;
unconstrainedSharedWrite.constraints.integrationTrains[0] = content(trainWithoutHash);
unconstrainedSharedWrite.planHash = canonicalHash(Object.fromEntries(Object.entries(unconstrainedSharedWrite).filter(([key]) => key !== "planHash")));
expectInvalid(validateCanonicalDagPlanV1, unconstrainedSharedWrite, "shared semantic writes require explicit incompatibility or causality evidence");
const twoItemPlan = clone(plan);
const secondSubject = content({ subjectId: "subject-worker", kind: "behavior", title: "Worker behavior", description: "Independent worker behavior." });
twoItemPlan.semanticSubjects.push(secondSubject);
const independentItemInput = { ...twoItemPlan.workItems[0], workItemId: "item-worker", title: "Implement worker", objective: "Implement independent worker behavior.", semanticWrites: [{ subjectId: secondSubject.subjectId, mode: "extend", compatibility: "compatible", migrationProtocolId: null }], pathEvidence: [{ path: "src/worker.ts", symbol: null, basis: "Current worker implementation.", confidence: "medium" }] };
delete independentItemInput.contentHash;
twoItemPlan.workItems.push(content(independentItemInput));
twoItemPlan.constraints.integrationTrains[0].members.push({ workItemId: "item-worker", ordinal: 1 });
twoItemPlan.constraints.integrationTrains[0] = recontent(twoItemPlan.constraints.integrationTrains[0]);
twoItemPlan.lifecycleBinding.checkCatalogHash = canonicalHash(twoItemPlan.workItems.map(({ workItemId, checks }) => ({ workItemId, checks })));
rehashPlan(twoItemPlan);
assert.equal(validateCanonicalDagPlanV1(twoItemPlan).ok, true, "valid two-item plan supports ordered integration after independent authoring");

const run = runFixture(plan);
const runContext = { plan, authorization: authorizationBinding(plan, run.identity.reviewReceipt.hash, run.identity.authorizationReceipts.map(({ hash }) => hash), run.identity.authorizationSet.hash), historicalAuthorizations: {}, catalog: catalogBinding(plan), normalizedSchedulerIndexHash: run.scheduler.normalizedIndexHash, facts: {} };
const baselineFactValues = [simpleFact("plan_review", "review-plan"), simpleFact("plan_authorization", "authorization-plan"), simpleFact("staleness", "freshness"), simpleFact("repository_observation", "repo-main-observation"), runContext.authorization];
const seedBaselineFacts = async (store) => { for (const fact of baselineFactValues) await store.putImmutableFact(fact); };
const validateRun = (value) => validateDagRunStateV1(value, runContext);
assert.equal(validateRun(run).ok, true, "valid canonical run snapshot passes");
assert.equal(parseDagRunStateV1(JSON.stringify(run), runContext).snapshotHash, run.snapshotHash, "strict run parser joins canonical plan and authorization context");
expectInvalid(validateDagRunStateV1, run, "exact run validation requires canonical plan and immutable fact context");
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, authorization: { ...runContext.authorization, maxActiveNodes: 2 } }), run, "run active-node limit must match exact detached authorization");
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, authorization: { ...runContext.authorization, repositoryIds: [] } }), run, "partial authorization must include dependency/effect/integration repository closure");
const blockedStickyLane = clone(run);
blockedStickyLane.workItems["item-api"].current = "blocked";
blockedStickyLane.current.activeWorkItemIds = ["item-api"];
blockedStickyLane.current.blockedWorkItemIds = ["item-api"];
blockedStickyLane.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null };
blockedStickyLane.workItems["item-api"].laneAdmissionSequence = 1;
blockedStickyLane.workItems["item-api"].admittedAt = NOW;
blockedStickyLane.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(blockedStickyLane).filter(([key]) => key !== "snapshotHash")));
assert.equal(validateRun(blockedStickyLane).ok, true, "blocked work item retains its sticky active-node lane");
const f0EvidenceHash = H("8");
const passedF0 = clone(run);
const f0Input = ref("stage_attempt_input", "attempt-f0", H("9"));
const f0Evidence = ref("stage_evidence", "evidence-f0", f0EvidenceHash);
passedF0.workItems["item-api"].current = "active";
passedF0.workItems["item-api"].currentStage = "F1";
passedF0.workItems["item-api"].stages.F0 = { stage: "F0", state: "passed", attemptIds: ["attempt-f0"], currentAttemptId: "attempt-f0", currentEvidence: f0EvidenceHash, adoptionReceipt: null, invalidationIds: [], lastDisposition: "PASS", blockerIds: [] };
passedF0.current.activeWorkItemIds = ["item-api"];
passedF0.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null };
passedF0.workItems["item-api"].laneAdmissionSequence = 1;
passedF0.workItems["item-api"].admittedAt = NOW;
passedF0.stageAttempts["attempt-f0"] = { stageAttemptId: "attempt-f0", workItemId: "item-api", stage: "F0", ordinal: 1, producerKind: "conductor", implementationLineageHash: null, inputGeneration: 0, reservedOutputGeneration: null, attemptInput: f0Input, authorizationSetHash: run.identity.authorizationSet.hash, state: "sealed", launchIntentId: null, leaseIds: [], workerResult: null, evidence: f0Evidence, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: NOW };
passedF0.evidenceIndex.stageAttemptInputs["attempt-f0"] = f0Input;
passedF0.evidenceIndex.stageEvidence[f0EvidenceHash] = f0Evidence;
rehashRun(passedF0);
const f0InputFact = { kind: "stage_attempt_input", hash: f0Input.hash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F0", stageAttemptId: "attempt-f0", candidateGeneration: 0, candidateHash: null, authorizationSetHash: run.identity.authorizationSet.hash, producerKind: "conductor", implementationLineageHash: null };
const f0Catalog = catalogBinding(plan);
const f0Procedure = Object.values(f0Catalog.procedures).find(({ stages }) => stages.includes("F0"));
const f0AggregateInput = { workItemId: "item-api", stage: "F0", procedureHash: f0Procedure.hash, environmentProfileHash: f0Procedure.environmentProfileHash, disposition: "PASS", oracleIds: ["oracle-api"], assertions: [], checks: [] };
const f0AggregateHash = canonicalHash(f0AggregateInput);
f0Catalog.checkAggregates[f0AggregateHash] = { ...f0AggregateInput, hash: f0AggregateHash };
const f0Fact = { kind: "stage_evidence", hash: f0EvidenceHash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F0", stageAttemptId: "attempt-f0", attemptInputHash: f0Input.hash, authorizationSetHash: run.identity.authorizationSet.hash, procedureHash: f0Procedure.hash, environmentProfileHash: f0Procedure.environmentProfileHash, checkAggregateHash: f0AggregateHash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration: 0, candidateHash: null, producerKind: "conductor", producerResultHash: null, disposition: "PASS", freshIndependent: false, readOnly: true, cleanEnvironment: false };
f0InputFact.hash = canonicalHash(Object.fromEntries(Object.entries(f0InputFact).filter(([key]) => key !== "hash")));
f0Input.hash = f0InputFact.hash;
f0Fact.attemptInputHash = f0Input.hash;
f0Fact.hash = canonicalHash(Object.fromEntries(Object.entries(f0Fact).filter(([key]) => key !== "hash")));
passedF0.stageAttempts["attempt-f0"].attemptInput.hash = f0Input.hash;
passedF0.stageAttempts["attempt-f0"].evidence.hash = f0Fact.hash;
passedF0.evidenceIndex.stageAttemptInputs["attempt-f0"].hash = f0Input.hash;
delete passedF0.evidenceIndex.stageEvidence[f0EvidenceHash];
passedF0.evidenceIndex.stageEvidence[f0Fact.hash] = { ...f0Evidence, hash: f0Fact.hash };
passedF0.workItems["item-api"].stages.F0.currentEvidence = f0Fact.hash;
rehashRun(passedF0);
assert.equal(validateDagRunStateV1(passedF0, { ...runContext, catalog: f0Catalog, facts: { [f0Input.hash]: f0InputFact, [f0Fact.hash]: f0Fact } }).ok, true, "exact sealed conductor evidence can pass F0 without generic worker authority");
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, facts: { [f0Input.hash]: f0InputFact, [f0Fact.hash]: f0Fact } }), passedF0, "stage evidence cannot use arbitrary unbound procedure, environment, or check hashes");
const blockedWithoutLane = clone(passedF0); blockedWithoutLane.workItems["item-api"].current = "blocked"; blockedWithoutLane.current.activeWorkItemIds = []; blockedWithoutLane.current.blockedWorkItemIds = ["item-api"]; delete blockedWithoutLane.scheduler.activeNodeLanes["item-api"]; rehashRun(blockedWithoutLane); expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, catalog: f0Catalog, facts: { [f0Input.hash]: f0InputFact, [f0Fact.hash]: f0Fact } }), blockedWithoutLane, "blocked admitted work cannot release its sticky lane");
const futureAuthorization = clone(runContext.authorization); futureAuthorization.validFrom = "2099-01-01T00:00:00Z"; expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, authorization: futureAuthorization }), run, "future authorization cannot authorize an earlier run");
const expiredAuthorizationInput = { ...runContext.authorization, validUntil: "2026-08-04T15:30:00.000Z" }; delete expiredAuthorizationInput.hash; const expiredAuthorization = { ...expiredAuthorizationInput, hash: canonicalHash(expiredAuthorizationInput) };
const expiredSnapshot = clone(run); expiredSnapshot.updatedAt = "2026-08-04T16:00:00.000Z"; expiredSnapshot.identity.authorizationSet.hash = expiredAuthorization.hash; expiredSnapshot.completion.authorizedScopeHash = expiredAuthorization.hash; rehashRun(expiredSnapshot); expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, authorization: expiredAuthorization }), expiredSnapshot, "authorization expired before the latest transition cannot remain current");
const unscopedExternalEffect = clone(run); unscopedExternalEffect.effects["effect-external"] = { effectId: "effect-external", kind: "reconcile_external_effect", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: H("4"), boundOwnerEpoch: 0, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("5"), state: "intended", dispatchCount: 0, createdRevision: 0, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null }; rehashRun(unscopedExternalEffect); expectInvalid(validateRun, unscopedExternalEffect, "external effect cannot dispatch without an item-local plan-authorized effect scope");
const unknownRun = clone(run); unknownRun.events = []; expectInvalid(validateRun, unknownRun, "run snapshot rejects event-log fields");
const phantomSlotRun = clone(run); phantomSlotRun.idempotencySlots[H("4")] = { slotId: H("4"), inputType: "set_desired_run", commandId: "phantom-command", idempotencyKey: "phantom-key", payloadHash: H("5"), inputHash: H("6"), appliedRevision: 1 }; rehashRun(phantomSlotRun); expectInvalid(validateRun, phantomSlotRun, "revision-zero snapshot rejects phantom future idempotency receipts");
const speculativeRun = clone(run); speculativeRun.scheduler.speculativeReservations = []; expectInvalid(validateRun, speculativeRun, "v1 scheduler rejects future-prefix speculation state");
const adaptiveRun = clone(run); adaptiveRun.scheduler.adaptiveFanout = 2; expectInvalid(validateRun, adaptiveRun, "v1 scheduler rejects adaptive fanout state");
const speculativeSuffix = clone(run);
speculativeSuffix.identity.planHash = twoItemPlan.planHash;
speculativeSuffix.freshness.evaluatedPlanHash = twoItemPlan.planHash;
speculativeSuffix.workItems["item-api"].planEntityHash = twoItemPlan.workItems[0].contentHash;
const secondProjection = clone(speculativeSuffix.workItems["item-api"]);
secondProjection.workItemId = "item-worker";
secondProjection.planEntityHash = twoItemPlan.workItems[1].contentHash;
secondProjection.implementationLineageHash = H("8");
speculativeSuffix.workItems["item-worker"] = secondProjection;
speculativeSuffix.completion.remainingAuthorizedWorkItemIds = ["item-api", "item-worker"];
speculativeSuffix.integrationTrains["repo-main"].planTrainHash = twoItemPlan.constraints.integrationTrains[0].contentHash;
const candidate = (id, hash, producedBy) => ({ generation: 1, candidateId: id, candidateHash: hash, base: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, git: { repositoryId: "repo-main", commit: O("c"), tree: O("d") }, patchIdentityHash: H("9"), producedByStageAttemptId: producedBy, lineageHash: H("a") });
speculativeSuffix.integrationTrains["repo-main"].entryOrder = ["entry-api", "entry-worker"];
speculativeSuffix.integrationTrains["repo-main"].entries = {
  "entry-api": { entryId: "entry-api", workItemId: "item-api", ordinal: 0, state: "waiting", integrationReadyHash: H("b"), sourceCandidate: candidate("candidate-api", H("c"), "attempt-api"), attemptIds: [], currentAttemptId: null, integrationReceipt: null, blockerIds: [] },
  "entry-worker": { entryId: "entry-worker", workItemId: "item-worker", ordinal: 1, state: "verifying_prefix", integrationReadyHash: H("d"), sourceCandidate: candidate("candidate-worker", H("e"), "attempt-worker"), attemptIds: [], currentAttemptId: null, integrationReceipt: null, blockerIds: [] },
};
rehashRun(speculativeSuffix);
expectInvalid((value) => validateDagRunStateV1(value, { plan: twoItemPlan, authorization: authorizationBinding(twoItemPlan, run.identity.reviewReceipt.hash, run.identity.authorizationReceipts.map(({ hash }) => hash), run.identity.authorizationSet.hash), historicalAuthorizations: {}, catalog: catalogBinding(twoItemPlan), normalizedSchedulerIndexHash: speculativeSuffix.scheduler.normalizedIndexHash, facts: {} }), speculativeSuffix, "later train entry cannot compose or verify while the accepted head still waits");
const badRunHash = clone(run); badRunHash.snapshotHash = H("f"); expectInvalid(validateRun, badRunHash, "run rejects stale snapshot hash");
const falsePass = clone(run); falsePass.workItems["item-api"].stages.F0.state = "passed"; falsePass.workItems["item-api"].stages.F0.lastDisposition = "PASS"; rehashRun(falsePass); expectInvalid(validateRun, falsePass, "generic state cannot claim stage pass without immutable evidence");
const forgedEvidenceHash = H("6");
const forgedIndexedPass = clone(run);
forgedIndexedPass.workItems["item-api"].stages.F8.state = "passed";
forgedIndexedPass.workItems["item-api"].stages.F8.currentEvidence = forgedEvidenceHash;
forgedIndexedPass.workItems["item-api"].stages.F8.lastDisposition = "PASS";
forgedIndexedPass.evidenceIndex.stageEvidence[forgedEvidenceHash] = ref("stage_evidence", "forged-f8", forgedEvidenceHash);
rehashRun(forgedIndexedPass);
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, facts: { [forgedEvidenceHash]: { kind: "stage_evidence", hash: forgedEvidenceHash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F8", stageAttemptId: "missing-attempt", attemptInputHash: H("1"), authorizationSetHash: run.identity.authorizationSet.hash, procedureHash: H("2"), environmentProfileHash: H("3"), checkAggregateHash: H("4"), findingHashes: [], effectReconciliationHashes: [], candidateGeneration: 0, candidateHash: null, producerKind: "conductor", producerResultHash: null, disposition: "PASS", freshIndependent: false, readOnly: true, cleanEnvironment: true } } }), forgedIndexedPass, "indexed hashes cannot forge out-of-order F8 passage without a sealed exact attempt");
const prematureReadyHash = H("7");
const prematureReady = clone(run);
prematureReady.workItems["item-api"].current = "integration_ready";
prematureReady.workItems["item-api"].integrationReadyReceipt = prematureReadyHash;
prematureReady.current.activeWorkItemIds = ["item-api"];
prematureReady.current.integrationReadyWorkItemIds = ["item-api"];
prematureReady.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null };
prematureReady.evidenceIndex.integrationReady["item-api"] = ref("integration_ready", "wrong-id", prematureReadyHash);
rehashRun(prematureReady);
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, facts: { [prematureReadyHash]: { kind: "integration_ready", hash: prematureReadyHash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", candidateGeneration: 0, candidateHash: H("8"), f8EvidenceHash: forgedEvidenceHash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true } } }), prematureReady, "integration readiness requires exact F0-F8 closure and a current candidate");
const wrongFactKinds = clone(run);
wrongFactKinds.identity.reviewReceipt.kind = "worker_result";
wrongFactKinds.identity.authorizationReceipts[0].kind = "candidate";
wrongFactKinds.identity.authorizationSet.kind = "finding";
wrongFactKinds.freshness.receipt.kind = "waiver";
rehashRun(wrongFactKinds);
expectInvalid(validateRun, wrongFactKinds, "run identity rejects detached facts of the wrong semantic kind");
const activeWithoutLane = clone(run);
activeWithoutLane.workItems["item-api"].current = "active";
rehashRun(activeWithoutLane);
expectInvalid(validateRun, activeWithoutLane, "active work item cannot exist without a sticky active-node lane");
const releasedNonterminalLane = clone(blockedStickyLane);
releasedNonterminalLane.scheduler.activeNodeLanes["item-api"].releaseDisposition = "terminal_cancelled";
releasedNonterminalLane.scheduler.activeNodeLanes["item-api"].releasedAt = NOW;
releasedNonterminalLane.current.activeWorkItemIds = [];
rehashRun(releasedNonterminalLane);
expectInvalid(validateRun, releasedNonterminalLane, "sticky lane cannot release while its work item remains nonterminal");
const unsafeRetry = clone(run); unsafeRetry.stageAttempts["attempt-f1"] = { stageAttemptId: "attempt-f1", workItemId: "item-api", stage: "F1", ordinal: 1, producerKind: "owned_worker", implementationLineageHash: run.workItems["item-api"].implementationLineageHash, inputGeneration: 0, reservedOutputGeneration: 1, attemptInput: ref("stage_attempt_input", "attempt-f1", H("3")), authorizationSetHash: run.identity.authorizationSet.hash, state: "lost", launchIntentId: null, leaseIds: [], workerResult: null, evidence: null, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: NOW }; unsafeRetry.evidenceIndex.stageAttemptInputs["attempt-f1"] = unsafeRetry.stageAttempts["attempt-f1"].attemptInput; unsafeRetry.workerBindings["attempt-f1"] = { stageAttemptId: "attempt-f1", launchIntentId: "launch-f1", workerStorageId: "storage", launchOwnerSessionId: "session", workerId: "worker", attemptNumber: 1, attemptNonce: "abcdef0123456789", configHash: H("4"), configRef: ref("worker_result", "config", H("4")), supervisorPid: 1, supervisorStartIdentity: "proc:1", childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: NOW, completionId: null, resultHash: null, processDisposition: "live", retrySafe: true }; rehashRun(unsafeRetry); expectInvalid(validateRun, unsafeRetry, "live worker ownership cannot be retry-safe");
const prematureComplete = clone(run); prematureComplete.completion.state = "plan_complete"; prematureComplete.current.run = "completed"; prematureComplete.completion.remainingAuthorizedWorkItemIds = []; prematureComplete.completion.completedAt = NOW; prematureComplete.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(prematureComplete).filter(([key]) => key !== "snapshotHash"))); expectInvalid(validateRun, prematureComplete, "run cannot complete without integrated work items");
const globalBudget = clone(run); globalBudget.tokenBudget = 1000; expectInvalid(validateRun, globalBudget, "run schema forbids global execution budgets");

assert.match(DAG_RUN_INPUT_SCHEMA_HASH, /^sha256:[0-9a-f]{64}$/, "reducer input schema exposes a stable hash");
const reducerInput = (state, type, payload, overrides = {}) => ({
  schemaVersion: 1, kind: "command", type, commandId: `command-${type}`, idempotencyKey: `key-${type}`,
  payloadHash: canonicalHash(payload), runId: state.runId, runNonce: state.runNonce,
  expectedRevision: state.revision, expectedSnapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch,
  occurredAt: NOW, payload, ...overrides,
});
const pausePayload = { desired: "paused", reason: "Operator pause", requestedBy: "user" };
const terminalPauseProjection = clone(run); terminalPauseProjection.completion.state = "plan_complete"; terminalPauseProjection.current.run = "completed"; rehashRun(terminalPauseProjection);
assert.equal(reduceDagRunV1(terminalPauseProjection, reducerInput(terminalPauseProjection, "set_desired_run", pausePayload, { commandId: "command-terminal-pause", idempotencyKey: "key-terminal-pause" }), runContext).accepted, false, "terminal completion projection cannot regress to paused even if desired state previously remained running");
const pauseInput = reducerInput(run, "set_desired_run", pausePayload);
const pausedResult = reduceDagRunV1(run, pauseInput, runContext);
assert.equal(pausedResult.accepted, true, "pure reducer accepts a guarded pause");
assert.equal(pausedResult.accepted && pausedResult.state.current.run, "paused", "pause stops new dispatch through derived current state");
assert.equal(pausedResult.accepted && pausedResult.state.previousSnapshotHash, run.snapshotHash, "accepted mutation chains previous snapshot hash");
assert.equal(run.revision, 0, "pure reducer does not mutate its input snapshot");
const readyRun = clone(run); readyRun.workItems["item-api"].current = "ready"; readyRun.current.run = "active"; readyRun.current.readyWorkItemIds = ["item-api"]; rehashRun(readyRun);
assert.equal(validateRun(readyRun).ok, true, "ready-run pause fixture is canonical before transition");
const readyPauseInput = reducerInput(readyRun, "set_desired_run", pausePayload, { commandId: "command-ready-pause", idempotencyKey: "key-ready-pause" });
const readyPause = reduceDagRunV1(readyRun, readyPauseInput, runContext);
assert.equal(readyPause.accepted && readyPause.state.current.run, "paused", "pause retains legal ready frontier while blocking dispatch");
const duplicatePause = reduceDagRunV1(pausedResult.accepted ? pausedResult.state : run, pauseInput, runContext);
assert.equal(duplicatePause.accepted && duplicatePause.duplicate, true, "exact natural-slot replay is a no-op before stale-CAS rejection");
const conflictingPausePayload = { ...pausePayload, reason: "Conflicting replay" };
const conflictingPause = reduceDagRunV1(pausedResult.accepted ? pausedResult.state : run, { ...pauseInput, payload: conflictingPausePayload, payloadHash: canonicalHash(conflictingPausePayload) }, runContext);
assert.equal(conflictingPause.accepted, false, "same command slot with different content conflicts");
const staleResume = reducerInput(run, "set_desired_run", { desired: "running", reason: null, requestedBy: "user" }, { commandId: "command-stale-resume" });
const staleResumeResult = reduceDagRunV1(pausedResult.accepted ? pausedResult.state : run, staleResume, runContext);
assert.equal(staleResumeResult.accepted, false, "new stale command cannot mutate a later revision");
const malformedReducerInput = { ...pauseInput, surprise: true };
assert.equal(reduceDagRunV1(run, malformedReducerInput, runContext).accepted, false, "closed reducer input rejects unknown fields");
const lateFactInput = { kind: "worker_result", workerStorageId: "storage-late", launchOwnerSessionId: "session-late", workerId: "worker-late", attemptNumber: 1, attemptNonce: "0123456789abcdef", configHash: H("6"), completionId: "completion-late", terminalStatus: "succeeded", processDisposition: "dead", retrySafe: true };
const lateFact = { ...lateFactInput, hash: canonicalHash(lateFactInput) };
const lateFactText = JSON.stringify(lateFact);
const quarantineEntry = { quarantineId: "quarantine-late", fact: { ...ref("worker_result", "late-result", lateFact.hash), bytes: Buffer.byteLength(lateFactText) }, reason: "stale_generation", observedBindingHash: H("8"), expectedBindingHash: H("9"), state: "held", observedAt: NOW, adoptionReceipt: null, rejectionReason: null };
const quarantineContext = { ...runContext, facts: { ...runContext.facts, [lateFact.hash]: lateFact } };
const quarantineInput = reducerInput(run, "quarantine_fact", { quarantine: quarantineEntry }, { kind: "observation", commandId: "command-quarantine", idempotencyKey: "key-quarantine" });
const quarantinedResult = reduceDagRunV1(run, quarantineInput, quarantineContext);
assert.equal(quarantinedResult.accepted && quarantinedResult.state.quarantine["quarantine-late"].state, "held", "new mismatched fact enters quarantine held without semantic advancement");
assert.equal(quarantinedResult.accepted && reduceDagRunV1(quarantinedResult.state, quarantineInput, quarantineContext).duplicate, true, "quarantine observation exact replay wins before stale snapshot CAS");
const forgedAdoptedQuarantine = { ...quarantineEntry, state: "adopted", adoptionReceipt: H("a") };
assert.equal(reduceDagRunV1(run, reducerInput(run, "quarantine_fact", { quarantine: forgedAdoptedQuarantine }, { kind: "observation", commandId: "command-forged-adoption", idempotencyKey: "key-forged-adoption" }), quarantineContext).accepted, false, "caller cannot inject adopted quarantine state");
if (quarantinedResult.accepted) {
  const quarantineEntryHash = canonicalHash({ quarantineId: quarantineEntry.quarantineId, fact: quarantineEntry.fact, reason: quarantineEntry.reason, observedBindingHash: quarantineEntry.observedBindingHash, expectedBindingHash: quarantineEntry.expectedBindingHash, observedAt: quarantineEntry.observedAt });
  const quarantineAuthorityCore = { kind: "quarantine_authority", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, quarantineId: "quarantine-late", factHash: lateFact.hash, quarantineEntryHash, decision: "adopt", issuedBy: "user", issuedAt: NOW };
  const quarantineAuthority = { ...quarantineAuthorityCore, hash: canonicalHash(quarantineAuthorityCore) };
  const quarantineAdoptionCore = { kind: "quarantine_resolution", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, quarantineId: "quarantine-late", factHash: lateFact.hash, quarantineEntryHash, authorityReceiptHash: quarantineAuthority.hash, disposition: "adopted", rationaleHash: H("5") };
  const quarantineAdoptionFact = { ...quarantineAdoptionCore, hash: canonicalHash(quarantineAdoptionCore) };
  const quarantineAdoptionContext = { ...quarantineContext, facts: { ...quarantineContext.facts, [quarantineAdoptionFact.hash]: quarantineAdoptionFact }, authorityReceipts: { [quarantineAuthority.hash]: quarantineAuthority } };
  const adoptQuarantinePayload = { quarantineId: "quarantine-late", adoptionReceipt: quarantineAdoptionFact.hash };
  const adoptQuarantineInput = reducerInput(quarantinedResult.state, "adopt_quarantined_fact", adoptQuarantinePayload, { kind: "observation", commandId: "command-adopt-quarantine", idempotencyKey: "key-adopt-quarantine" });
  assert.equal(reduceDagRunV1(quarantinedResult.state, adoptQuarantineInput, { ...quarantineAdoptionContext, authorityReceipts: {} }).accepted, false, "conductor-authored resolution cannot adopt quarantine without external user authority");
  const adoptedQuarantine = reduceDagRunV1(quarantinedResult.state, adoptQuarantineInput, quarantineAdoptionContext);
  assert.equal(adoptedQuarantine.accepted && adoptedQuarantine.state.quarantine["quarantine-late"].state, "adopted", "quarantined fact adoption requires an exact immutable resolution receipt");
  assert.equal(adoptedQuarantine.accepted && reduceDagRunV1(adoptedQuarantine.state, adoptQuarantineInput, quarantineAdoptionContext).duplicate, true, "quarantine adoption exact replay is idempotent before stale CAS");
  const rejectQuarantinePayload = { quarantineId: "quarantine-late", reason: "Wrong generation" };
  const rejectQuarantineInput = reducerInput(quarantinedResult.state, "reject_quarantined_fact", rejectQuarantinePayload, { commandId: "command-reject-quarantine", idempotencyKey: "key-reject-quarantine" });
  const rejectedQuarantine = reduceDagRunV1(quarantinedResult.state, rejectQuarantineInput, quarantineContext);
  assert.equal(rejectedQuarantine.accepted && rejectedQuarantine.state.quarantine["quarantine-late"].state, "rejected", "quarantine rejection is an explicit guarded disposition");
  assert.equal(rejectedQuarantine.accepted && reduceDagRunV1(rejectedQuarantine.state, rejectQuarantineInput, quarantineContext).duplicate, true, "quarantine rejection exact replay is idempotent before stale CAS");
}

const ownershipFactFor = (state, successor, disposition, lineageHash = null, priorObservationHash = null) => {
  const exactLineageHash = disposition === "same_manager" ? canonicalHash({ kind: "direct_owner_transfer", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity }) : lineageHash;
  const input = { kind: "ownership", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, priorAttachedAt: state.owner.attachedAt, disposition, priorObservationHash, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity, lineageHash: exactLineageHash };
  return { ...input, hash: canonicalHash(input) };
};
const attachSuccessor = { ownerTokenHash: H("d"), sessionId: "session-owner", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("e") };
const attachOwnershipFact = ownershipFactFor(run, attachSuccessor, "absent");
const attachPayload = { ...attachSuccessor, ownershipReceipt: attachOwnershipFact.hash, priorOwnerDisposition: "absent" };
const ownerContext = { ...runContext, facts: { ...runContext.facts, [attachOwnershipFact.hash]: attachOwnershipFact } };
const attachedResult = reduceDagRunV1(run, reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), ownerContext);
assert.equal(attachedResult.accepted, true, "owner attach advances a fenced owner epoch under explicit proof");
if (attachedResult.accepted) {
  assert.equal(attachedResult.state.owner.ownerEpoch, 1);
  const transferSuccessor = { ownerTokenHash: H("2"), sessionId: "session-direct-transfer", pid: 2345, processStartIdentity: "linux-proc:234", lockIdentity: H("3") };
  const transferOwnershipFact = ownershipFactFor(attachedResult.state, transferSuccessor, "same_manager", H("4"));
  const transferPayload = { ...transferSuccessor, ownershipReceipt: transferOwnershipFact.hash, priorOwnerDisposition: "same_manager" };
  const transferContext = { ...ownerContext, facts: { ...ownerContext.facts, [transferOwnershipFact.hash]: transferOwnershipFact } };
  const directTransfer = reduceDagRunV1(attachedResult.state, reducerInput(attachedResult.state, "transfer_owner", transferPayload, { commandId: "command-direct-transfer", idempotencyKey: "key-direct-transfer" }), transferContext);
  assert.equal(directTransfer.accepted && directTransfer.state.owner.ownerEpoch, 2, `hash-bound same-manager lineage permits direct owner transfer: ${JSON.stringify(directTransfer)}`);
  const releasePayload = { reason: "Detach owner" };
  const releaseInput = reducerInput(attachedResult.state, "release_owner", releasePayload, { commandId: "command-release-owner" });
  const releasedOwner = reduceDagRunV1(attachedResult.state, releaseInput, ownerContext);
  assert.equal(releasedOwner.accepted, true, `owner release accepted: ${JSON.stringify(releasedOwner)}`);
  assert.equal(releasedOwner.accepted ? releasedOwner.state.owner.sessionId : "rejected", null, "owner release clears process locator while retaining fencing epoch");
  const duplicateRelease = reduceDagRunV1(releasedOwner.accepted ? releasedOwner.state : attachedResult.state, releaseInput, ownerContext);
  assert.equal(duplicateRelease.accepted && duplicateRelease.duplicate, true, "exact owner-release replay is a no-op");
  const effect = { effectId: "effect-procedure", kind: "run_procedure", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("a"), boundOwnerEpoch: 1, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("b"), state: "intended", dispatchCount: 0, createdRevision: 2, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
  const effectIntentInput = reducerInput(attachedResult.state, "put_effect_intent", { effect });
  const intentResult = reduceDagRunV1(attachedResult.state, effectIntentInput, ownerContext);
  assert.equal(intentResult.accepted, true, "external operation is first persisted as an effect intent");
  if (intentResult.accepted) {
    assert.equal(reduceDagRunV1(intentResult.state, effectIntentInput, ownerContext).duplicate, true, "effect-intent exact replay is idempotent before stale CAS");
    const dispatchPayload = { effectId: effect.effectId, expectedDispatchCount: 0 };
    const effectDispatchInput = reducerInput(intentResult.state, "mark_effect_dispatching", dispatchPayload);
    const dispatchResult = reduceDagRunV1(intentResult.state, effectDispatchInput, ownerContext);
    assert.equal(dispatchResult.accepted, true, "dispatch transition persists before returning an external effect request");
    assert.equal(dispatchResult.accepted && dispatchResult.effects.length, 1, "only committed dispatch returns an external effect request");
    assert.equal(dispatchResult.accepted && reduceDagRunV1(dispatchResult.state, effectDispatchInput, ownerContext).duplicate, true, "effect dispatch exact replay never emits a second external request");
    if (dispatchResult.accepted) {
      const uncertainRetryPayload = { effectId: effect.effectId, expectedDispatchCount: 1, reason: "uncertain_acknowledgement" };
      const uncertainRetry = reduceDagRunV1(dispatchResult.state, reducerInput(dispatchResult.state, "retry_effect_dispatch", uncertainRetryPayload, { commandId: "command-retry-uncertain-effect", idempotencyKey: "key-retry-uncertain-effect" }), ownerContext);
      assert.equal(uncertainRetry.accepted && uncertainRetry.effects.length, 1, "durable dispatching state can explicitly redispatch only replay-safe work after uncertain acknowledgement");
    }
  }
}

const cancelEffect = { effectId: "effect-cancel-run", kind: "cancel_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: H("c"), boundOwnerEpoch: 0, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 1, boundGateEpochHash: H("d"), state: "intended", dispatchCount: 0, createdRevision: 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
const cancelPayload = { cancellationId: "cancel-run", scope: "run", subjectId: run.runId, reason: "Stop run", workItemIds: ["item-api"], effects: [] };
const cancellationInput = reducerInput(run, "request_cancellation", cancelPayload);
const cancellationResult = reduceDagRunV1(run, cancellationInput, runContext);
assert.equal(cancellationResult.accepted, true, "cancellation fences candidate generation before exposing cancellation intent");
assert.equal(cancellationResult.accepted && cancellationResult.state.workItems["item-api"].candidateGeneration, 1, "cancellation advances generation first");
assert.equal(cancellationResult.accepted && cancellationResult.effects.length, 0, "new cancellation intent is not externally dispatchable until a later dispatch CAS");
assert.equal(cancellationResult.accepted && reduceDagRunV1(cancellationResult.state, cancellationInput, runContext).duplicate, true, "cancellation request exact replay is idempotent before stale CAS");
if (cancellationResult.accepted) {
  const overlappingPayload = { ...cancelPayload, cancellationId: "cancel-overlap" };
  const overlappingCancellation = reduceDagRunV1(cancellationResult.state, reducerInput(cancellationResult.state, "request_cancellation", overlappingPayload, { commandId: "command-cancel-overlap", idempotencyKey: "key-cancel-overlap" }), runContext);
  assert.equal(overlappingCancellation.accepted, false, "overlapping open cancellation cannot advance generation or fence the first cancellation's effects");
}
const activeCancellationRun = clone(passedF0);
activeCancellationRun.scheduler.reservations["reservation-f1"] = { reservationId: "reservation-f1", reservationSequence: 1, workItemId: "item-api", stage: "F1", attemptOrdinal: 1, operationKind: "implementation", state: "active", candidateGeneration: 0, ownerEpoch: 0, authorizationSetHash: run.identity.authorizationSet.hash, normalizedRequestHash: H("1"), leaseIds: [], mutexGroupIds: [], resourceUnits: {}, operationalUnits: { "worker.process": 1, "role:implementation": 1, "repository-worktree:repo-main": 1 }, workerRole: "implementation", repositoryId: "repo-main", createdAt: NOW, releasedAt: null };
activeCancellationRun.scheduler.nextReservationSequence = 2;
const f1AttemptId = "attempt-f1-active";
const f1InputCore = { kind: "stage_attempt_input", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F1", stageAttemptId: f1AttemptId, candidateGeneration: 0, candidateHash: null, authorizationSetHash: run.identity.authorizationSet.hash, producerKind: "owned_worker", implementationLineageHash: activeCancellationRun.workItems["item-api"].implementationLineageHash };
const f1InputFact = { ...f1InputCore, hash: canonicalHash(f1InputCore) };
const f1InputRef = ref("stage_attempt_input", f1AttemptId, f1InputFact.hash);
const launchEffect = { effectId: "effect-launch-f1", kind: "launch_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: H("0"), boundOwnerEpoch: 0, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("1"), state: "dispatching", dispatchCount: 1, createdRevision: 0, createdAt: NOW, lastDispatchAt: NOW, observationHash: null, reconciliation: "not_started", blockerId: null };
activeCancellationRun.effects[launchEffect.effectId] = launchEffect;
activeCancellationRun.effects["effect-old-intent"] = { effectId: "effect-old-intent", kind: "run_procedure", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("2"), boundOwnerEpoch: 0, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("3"), state: "intended", dispatchCount: 0, createdRevision: 0, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
const launchIntentId = "launch-f1-active";
activeCancellationRun.launchIntents[launchIntentId] = { launchIntentId, effectId: launchEffect.effectId, stageAttemptId: f1AttemptId, state: "bound", adapter: "owned-worker-v1", launchKey: "launch-key-f1", workerId: "worker-f1", expectedAttemptNumber: 1, taskPacketHash: H("2"), cwdRepositoryId: "repo-main", configRequestHash: H("3"), dispatchCount: 1, lastDispatchAt: NOW, boundAt: NOW, ambiguityReason: null };
activeCancellationRun.stageAttempts[f1AttemptId] = { stageAttemptId: f1AttemptId, workItemId: "item-api", stage: "F1", ordinal: 1, producerKind: "owned_worker", implementationLineageHash: activeCancellationRun.workItems["item-api"].implementationLineageHash, inputGeneration: 0, reservedOutputGeneration: null, attemptInput: f1InputRef, authorizationSetHash: run.identity.authorizationSet.hash, state: "running", launchIntentId, leaseIds: ["lease-f1"], workerResult: null, evidence: null, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: null };
activeCancellationRun.evidenceIndex.stageAttemptInputs[f1AttemptId] = f1InputRef;
activeCancellationRun.workerBindings[f1AttemptId] = { stageAttemptId: f1AttemptId, launchIntentId, workerStorageId: "storage-f1", launchOwnerSessionId: "session-launch-f1", workerId: "worker-f1", attemptNumber: 1, attemptNonce: "0123456789abcdef", configHash: H("4"), configRef: ref("verification", "config-f1", H("5")), supervisorPid: 12345, supervisorStartIdentity: "linux-proc:12345", childPid: 12346, childStartIdentity: "linux-proc:12346", mailboxHash: H("6"), heartbeatAt: NOW, completionId: null, resultHash: null, processDisposition: "live", retrySafe: false };
activeCancellationRun.leases["lease-f1"] = { leaseId: "lease-f1", kind: "stage_claim", subject: { kind: "work_item", id: "item-api" }, holderStageAttemptId: f1AttemptId, holderIntegrationAttemptId: null, candidateGeneration: 0, units: 0, ownerEpoch: 0, state: "active", acquiredAt: NOW, expiresAt: null, releasedAt: null, releaseReason: null };
const operationalLeaseIds = [];
for (const namespace of ["worker.process", "role:implementation", "repository-worktree:repo-main"]) {
  const leaseId = `lease-operational-${namespace.replaceAll(/[^A-Za-z0-9]/g, "-")}`; operationalLeaseIds.push(leaseId);
  activeCancellationRun.leases[leaseId] = { leaseId, kind: "resource", subject: { kind: "resource", id: namespace }, holderStageAttemptId: f1AttemptId, holderIntegrationAttemptId: null, candidateGeneration: 0, units: 1, ownerEpoch: 0, state: "active", acquiredAt: NOW, expiresAt: null, releasedAt: null, releaseReason: null };
  activeCancellationRun.scheduler.operationalCapacities[namespace].allocatedUnits = 1; activeCancellationRun.scheduler.operationalCapacities[namespace].reservationIds = ["reservation-f1"];
}
activeCancellationRun.workItems["item-api"].activeLeaseIds = ["lease-f1", ...operationalLeaseIds].sort();
activeCancellationRun.workItems["item-api"].stages.F1 = { stage: "F1", state: "active", attemptIds: [f1AttemptId], currentAttemptId: f1AttemptId, currentEvidence: null, adoptionReceipt: null, invalidationIds: [], lastDisposition: null, blockerIds: [] };
activeCancellationRun.scheduler.reservations["reservation-f1"].leaseIds = ["lease-f1", ...operationalLeaseIds].sort();
rehashRun(activeCancellationRun);
const activeBinding = activeCancellationRun.workerBindings[f1AttemptId];
const activeCancelRequestHash = canonicalHash({ kind: "cancel_worker", runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stageAttemptId: f1AttemptId, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, fencedGeneration: 1 });
const activeCancelEffect = { ...cancelEffect, effectId: "effect-cancel-active", requestHash: activeCancelRequestHash };
const activeCancelPayload = { cancellationId: "cancel-active", scope: "run", subjectId: run.runId, reason: "Stop active run", workItemIds: ["item-api"], effects: [activeCancelEffect] };
const activeCancellationContext = { ...runContext, catalog: f0Catalog, facts: { [f0Input.hash]: f0InputFact, [f0Fact.hash]: f0Fact, [f1InputFact.hash]: f1InputFact } };
const activeCancellationFixtureValidation = validateDagRunStateV1(activeCancellationRun, activeCancellationContext);
assert.equal(activeCancellationFixtureValidation.ok, true, `active cancellation fixture is legal: ${JSON.stringify(activeCancellationFixtureValidation.issues)}`);
const activePause = reduceDagRunV1(activeCancellationRun, reducerInput(activeCancellationRun, "set_desired_run", pausePayload, { commandId: "command-pause-active-worker", idempotencyKey: "key-pause-active-worker" }), activeCancellationContext);
assert.equal(activePause.accepted && activePause.state.current.run, "paused", "pause preserves active worker/lease history while blocking new dispatch");
if (activePause.accepted) {
  const pausedDispatch = reduceDagRunV1(activePause.state, reducerInput(activePause.state, "mark_effect_dispatching", { effectId: "effect-old-intent", expectedDispatchCount: 0 }, { commandId: "command-paused-dispatch", idempotencyKey: "key-paused-dispatch" }), activeCancellationContext);
  assert.equal(pausedDispatch.accepted, false, "paused active run rejects new external dispatch without rewriting its frontier");
}
const activeCancellation = reduceDagRunV1(activeCancellationRun, reducerInput(activeCancellationRun, "request_cancellation", activeCancelPayload, { commandId: "command-cancel-active", idempotencyKey: "key-cancel-active" }), activeCancellationContext);
assert.equal(activeCancellation.accepted, true, "generation-first cancellation coherently fences an active reserved/effectful item");
if (activeCancellation.accepted) {
  assert.equal(activeCancellation.state.scheduler.reservations["reservation-f1"].state, "fenced", "active reservation is fenced at cancellation generation");
  assert.equal(activeCancellation.state.effects["effect-old-intent"].state, "cancelled", "undispatched old-generation effect becomes proven absent");
  assert.equal(activeCancellation.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, null, "sticky lane remains until cancellation terminal observation");
  assert.equal(activeCancellation.state.leases["lease-f1"].state, "release_requested", "active lease remains held until exact worker death is reconciled");
  const absentWorkerCore = { kind: "worker_result", workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, completionId: "completion-f1-already-dead", terminalStatus: "cancelled", processDisposition: "dead", retrySafe: true };
  const absentWorkerFact = { ...absentWorkerCore, hash: canonicalHash(absentWorkerCore) };
  const absentReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: activeCancelEffect.effectId, requestHash: activeCancelEffect.requestHash, reconciliation: "proven_absent" };
  const absentReconciliationFact = { ...absentReconciliationCore, hash: canonicalHash(absentReconciliationCore) };
  const launchReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: launchEffect.effectId, requestHash: launchEffect.requestHash, reconciliation: "applied_exact" };
  const launchReconciliationFact = { ...launchReconciliationCore, hash: canonicalHash(launchReconciliationCore) };
  const absentResultRef = { ...ref("worker_result", absentWorkerFact.completionId, absentWorkerFact.hash), bytes: Buffer.byteLength(canonicalStringify(absentWorkerFact)) };
  const absentObservationCore = { cancellationId: "cancel-active", effectObservations: [{ effectId: activeCancelEffect.effectId, observationHash: absentReconciliationFact.hash }, { effectId: launchEffect.effectId, observationHash: launchReconciliationFact.hash }], workerResults: [{ stageAttemptId: f1AttemptId, result: absentResultRef }] };
  const absentObservationPayload = { ...absentObservationCore, resultHash: canonicalHash({ cancellationId: absentObservationCore.cancellationId, effectObservations: absentObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: absentWorkerFact.hash }] }) };
  const absentObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [absentWorkerFact.hash]: absentWorkerFact, [absentReconciliationFact.hash]: absentReconciliationFact, [launchReconciliationFact.hash]: launchReconciliationFact } };
  const absentClosed = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "record_cancellation", absentObservationPayload, { commandId: "command-close-absent-cancel", idempotencyKey: "key-close-absent-cancel", kind: "observation" }), absentObservationContext);
  assert.equal(absentClosed.accepted && absentClosed.state.effects[activeCancelEffect.effectId].reconciliation, "proven_absent", "already-dead worker permits exact proven-absent cancellation-effect closure without dispatch");
  const racedWorkerCore = { ...absentWorkerCore, completionId: "completion-f1-raced-success", terminalStatus: "succeeded" };
  const racedWorkerFact = { ...racedWorkerCore, hash: canonicalHash(racedWorkerCore) };
  const racedWorkerText = canonicalStringify(racedWorkerFact);
  const racedWorkerRef = { ...ref("worker_result", racedWorkerFact.completionId, racedWorkerFact.hash), bytes: Buffer.byteLength(racedWorkerText) };
  const racedObservationCore = { cancellationId: "cancel-active", effectObservations: absentObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, result: racedWorkerRef }] };
  const racedObservationPayload = { ...racedObservationCore, resultHash: canonicalHash({ cancellationId: racedObservationCore.cancellationId, effectObservations: racedObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: racedWorkerFact.hash }] }) };
  const racedObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [racedWorkerFact.hash]: racedWorkerFact, [absentReconciliationFact.hash]: absentReconciliationFact, [launchReconciliationFact.hash]: launchReconciliationFact } };
  const racedClosed = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "record_cancellation", racedObservationPayload, { commandId: "command-close-raced-cancel", idempotencyKey: "key-close-raced-cancel", kind: "observation" }), racedObservationContext);
  const racedQuarantine = racedClosed.accepted ? Object.values(racedClosed.state.quarantine).find(({ fact }) => fact.hash === racedWorkerFact.hash) : null;
  assert.equal(racedClosed.accepted && racedClosed.state.current.run === "cancelled" && racedQuarantine?.state, "held", "late successful terminal result proves process death but remains quarantined from semantic adoption");
  assert.equal(racedClosed.accepted && racedClosed.state.stageAttempts[f1AttemptId].workerResult === null && racedClosed.state.workerBindings[f1AttemptId].resultHash === null && racedClosed.state.evidenceIndex.workerResults[racedWorkerFact.hash] === undefined, true, "quarantined late success never becomes current attempt, binding, or evidence authority");
  const activeCancelDispatch = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "mark_effect_dispatching", { effectId: activeCancelEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-dispatch-active-cancel", idempotencyKey: "key-dispatch-active-cancel" }), activeCancellationContext);
  assert.equal(activeCancelDispatch.accepted && activeCancelDispatch.effects.length, 1, "persisted active-worker cancellation intent dispatches only after generation fence");
  if (activeCancelDispatch.accepted) {
    const cancelledWorkerCore = { kind: "worker_result", workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, completionId: "completion-f1-cancelled", terminalStatus: "cancelled", processDisposition: "dead", retrySafe: true };
    const cancelledWorkerFact = { ...cancelledWorkerCore, hash: canonicalHash(cancelledWorkerCore) };
    const cancelReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: activeCancelEffect.effectId, requestHash: activeCancelEffect.requestHash, reconciliation: "applied_exact" };
    const cancelReconciliationFact = { ...cancelReconciliationCore, hash: canonicalHash(cancelReconciliationCore) };
    const workerResultRef = { ...ref("worker_result", cancelledWorkerFact.completionId, cancelledWorkerFact.hash), bytes: Buffer.byteLength(canonicalStringify(cancelledWorkerFact)) };
    const activeObservationCore = { cancellationId: "cancel-active", effectObservations: [{ effectId: activeCancelEffect.effectId, observationHash: cancelReconciliationFact.hash }, { effectId: launchEffect.effectId, observationHash: launchReconciliationFact.hash }], workerResults: [{ stageAttemptId: f1AttemptId, result: workerResultRef }] };
    const activeObservationPayload = { ...activeObservationCore, resultHash: canonicalHash({ cancellationId: activeObservationCore.cancellationId, effectObservations: activeObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: cancelledWorkerFact.hash }] }) };
    const activeObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [cancelledWorkerFact.hash]: cancelledWorkerFact, [cancelReconciliationFact.hash]: cancelReconciliationFact, [launchReconciliationFact.hash]: launchReconciliationFact } };
    const preReconciledCancelPayload = { effectId: activeCancelEffect.effectId, observationHash: cancelReconciliationFact.hash, reconciliation: "applied_exact", terminalState: "reconciled" };
    const preReconciledCancel = reduceDagRunV1(activeCancelDispatch.state, reducerInput(activeCancelDispatch.state, "record_effect_observation", preReconciledCancelPayload, { commandId: "command-pre-reconcile-cancel", idempotencyKey: "key-pre-reconcile-cancel", kind: "observation" }), activeObservationContext);
    assert.equal(preReconciledCancel.accepted, true, "cancel effect may legally reconcile through generic effect observation before cancellation closure");
    const activeClosed = reduceDagRunV1(preReconciledCancel.accepted ? preReconciledCancel.state : activeCancelDispatch.state, reducerInput(preReconciledCancel.accepted ? preReconciledCancel.state : activeCancelDispatch.state, "record_cancellation", activeObservationPayload, { commandId: "command-close-active-cancel", idempotencyKey: "key-close-active-cancel", kind: "observation" }), activeObservationContext);
    assert.equal(activeClosed.accepted && activeClosed.state.current.run, "cancelled", "active cancellation closes only after exact worker identity reports cancelled and dead");
    assert.equal(activeClosed.accepted && activeClosed.state.leases["lease-f1"].state, "released", "lease releases only after retry-safe worker death observation");
    assert.equal(activeClosed.accepted && activeClosed.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, "terminal_cancelled", "sticky lane releases only at terminal cancellation");
  }
}
const ambiguousCancellationRun = clone(activeCancellationRun);
ambiguousCancellationRun.effects[launchEffect.effectId].procedureClass = "non_repeatable";
rehashRun(ambiguousCancellationRun);
const ambiguousCancelEffect = { ...activeCancelEffect, effectId: "effect-cancel-ambiguous", requestHash: activeCancelRequestHash };
const ambiguousCancelPayload = { ...activeCancelPayload, cancellationId: "cancel-ambiguous", effects: [ambiguousCancelEffect] };
const ambiguousCancellation = reduceDagRunV1(ambiguousCancellationRun, reducerInput(ambiguousCancellationRun, "request_cancellation", ambiguousCancelPayload, { commandId: "command-cancel-ambiguous", idempotencyKey: "key-cancel-ambiguous" }), activeCancellationContext);
assert.equal(ambiguousCancellation.accepted && ambiguousCancellation.state.blockers[`cancellation-effect-${launchEffect.effectId}`].active, true, "ambiguous non-repeatable old effect creates an explicit durable cancellation blocker");

const failedEffectRun = clone(run);
const failedRunEffect = { effectId: "effect-failed-run", kind: "run_procedure", subject: { kind: "run", id: run.runId }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("3"), boundOwnerEpoch: 0, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("4"), state: "failed", dispatchCount: 1, createdRevision: 0, createdAt: NOW, lastDispatchAt: NOW, observationHash: null, reconciliation: "unknown", blockerId: null };
failedEffectRun.effects[failedRunEffect.effectId] = failedRunEffect;
rehashRun(failedEffectRun);
assert.equal(validateRun(failedEffectRun).ok, true, "failed unresolved run effect is a legal pre-cancellation snapshot");
const failedEffectCancelPayload = { cancellationId: "cancel-failed-effect", scope: "run", subjectId: run.runId, reason: "Stop failed effect run", workItemIds: ["item-api"], effects: [] };
const failedEffectCancellation = reduceDagRunV1(failedEffectRun, reducerInput(failedEffectRun, "request_cancellation", failedEffectCancelPayload, { commandId: "command-cancel-failed-effect", idempotencyKey: "key-cancel-failed-effect" }), runContext);
assert.equal(failedEffectCancellation.accepted && failedEffectCancellation.state.blockers[`cancellation-effect-${failedRunEffect.effectId}`].active, true, "run cancellation fences failed unresolved run/repository/train effects with explicit blockers");
if (failedEffectCancellation.accepted) {
  const prematureFailedEffectCore = { cancellationId: "cancel-failed-effect", effectObservations: [], workerResults: [] };
  const prematureFailedEffectObservation = { ...prematureFailedEffectCore, resultHash: canonicalHash(prematureFailedEffectCore) };
  assert.equal(reduceDagRunV1(failedEffectCancellation.state, reducerInput(failedEffectCancellation.state, "record_cancellation", prematureFailedEffectObservation, { commandId: "command-premature-failed-close", idempotencyKey: "key-premature-failed-close", kind: "observation" }), runContext).accepted, false, "cancellation cannot close while any affected failed effect remains unresolved");
  const failedEffectReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: failedRunEffect.effectId, requestHash: failedRunEffect.requestHash, reconciliation: "compensated" };
  const failedEffectReconciliation = { ...failedEffectReconciliationCore, hash: canonicalHash(failedEffectReconciliationCore) };
  const failedEffectObservationCore = { cancellationId: "cancel-failed-effect", effectObservations: [{ effectId: failedRunEffect.effectId, observationHash: failedEffectReconciliation.hash }], workerResults: [] };
  const failedEffectObservation = { ...failedEffectObservationCore, resultHash: canonicalHash(failedEffectObservationCore) };
  const failedEffectContext = { ...runContext, facts: { [failedEffectReconciliation.hash]: failedEffectReconciliation } };
  const failedEffectClosed = reduceDagRunV1(failedEffectCancellation.state, reducerInput(failedEffectCancellation.state, "record_cancellation", failedEffectObservation, { commandId: "command-close-failed-effect", idempotencyKey: "key-close-failed-effect", kind: "observation" }), failedEffectContext);
  assert.equal(failedEffectClosed.accepted && failedEffectClosed.state.current.run, "cancelled", "cancellation closes only after exact terminal reconciliation of every affected external effect");
}

const partiallyTerminalRun = clone(run);
const partiallyTerminalAuthorization = authorizationBinding(twoItemPlan, partiallyTerminalRun.identity.reviewReceipt.hash, partiallyTerminalRun.identity.authorizationReceipts.map(({ hash }) => hash), null);
partiallyTerminalRun.identity.planHash = twoItemPlan.planHash;
partiallyTerminalRun.identity.checkCatalogHash = twoItemPlan.lifecycleBinding.checkCatalogHash;
partiallyTerminalRun.identity.authorizationSet.hash = partiallyTerminalAuthorization.hash;
partiallyTerminalRun.freshness.evaluatedPlanHash = twoItemPlan.planHash;
partiallyTerminalRun.completion.authorizedScopeHash = partiallyTerminalAuthorization.hash;
partiallyTerminalRun.integrationTrains["repo-main"].planTrainHash = twoItemPlan.constraints.integrationTrains[0].contentHash;
partiallyTerminalRun.workItems["item-worker"] = clone(partiallyTerminalRun.workItems["item-api"]);
partiallyTerminalRun.workItems["item-worker"].workItemId = "item-worker";
partiallyTerminalRun.workItems["item-worker"].planEntityHash = twoItemPlan.workItems.find(({ workItemId }) => workItemId === "item-worker").contentHash;
partiallyTerminalRun.workItems["item-worker"].implementationLineageHash = H("6");
partiallyTerminalRun.workItems["item-worker"].desired = "cancel";
partiallyTerminalRun.workItems["item-worker"].current = "cancelled";
partiallyTerminalRun.workItems["item-worker"].candidateGeneration = 1;
partiallyTerminalRun.completion.remainingAuthorizedWorkItemIds = ["item-api", "item-worker"];
rehashRun(partiallyTerminalRun);
const partiallyTerminalContext = { plan: twoItemPlan, authorization: partiallyTerminalAuthorization, historicalAuthorizations: {}, catalog: catalogBinding(twoItemPlan), normalizedSchedulerIndexHash: partiallyTerminalRun.scheduler.normalizedIndexHash, facts: {} };
const partiallyTerminalValidation = validateDagRunStateV1(partiallyTerminalRun, partiallyTerminalContext);
assert.equal(partiallyTerminalValidation.ok, true, `partially terminal two-item cancellation fixture is legal: ${JSON.stringify(partiallyTerminalValidation.issues)}`);
const partialCancelPayload = { cancellationId: "cancel-partial", scope: "run", subjectId: partiallyTerminalRun.runId, reason: "Stop remaining work", workItemIds: ["item-api"], effects: [] };
const partialCancelRequested = reduceDagRunV1(partiallyTerminalRun, reducerInput(partiallyTerminalRun, "request_cancellation", partialCancelPayload, { commandId: "command-cancel-partial", idempotencyKey: "key-cancel-partial" }), partiallyTerminalContext);
assert.equal(partialCancelRequested.accepted, true, "run cancellation fences only exact nonterminal work while preserving terminal siblings");
if (partialCancelRequested.accepted) {
  const partialObservationCore = { cancellationId: "cancel-partial", effectObservations: [], workerResults: [] };
  const partialObservation = { ...partialObservationCore, resultHash: canonicalHash(partialObservationCore) };
  const partialClosed = reduceDagRunV1(partialCancelRequested.state, reducerInput(partialCancelRequested.state, "record_cancellation", partialObservation, { commandId: "command-close-partial", idempotencyKey: "key-close-partial", kind: "observation" }), partiallyTerminalContext);
  assert.equal(partialClosed.accepted && partialClosed.state.current.run, "cancelled", "partial run cancellation closes without rewriting an already terminal sibling");
}

if (cancellationResult.accepted) {
  const postFenceEffect = { ...cancelEffect, effectId: "effect-post-fence", kind: "run_procedure", procedureClass: "pure", requestHash: H("5"), createdRevision: cancellationResult.state.revision + 1 };
  const postFenceIntent = reduceDagRunV1(cancellationResult.state, reducerInput(cancellationResult.state, "put_effect_intent", { effect: postFenceEffect }, { commandId: "command-post-fence-effect", idempotencyKey: "key-post-fence-effect" }), runContext);
  assert.equal(postFenceIntent.accepted, false, "cancelled run rejects every new non-recovery execution effect");
  const cancelObservationCore = { cancellationId: "cancel-run", effectObservations: [], workerResults: [] };
  const cancelObservationPayload = { ...cancelObservationCore, resultHash: canonicalHash(cancelObservationCore) };
  const cancelObservationInput = reducerInput(cancellationResult.state, "record_cancellation", cancelObservationPayload, { kind: "observation" });
  const cancelObservation = reduceDagRunV1(cancellationResult.state, cancelObservationInput, runContext);
  assert.equal(cancelObservation.accepted && cancelObservation.state.current.run, "cancelled", "cancellation without live workers closes only under exact empty terminal observation");
  assert.equal(cancelObservation.accepted && reduceDagRunV1(cancelObservation.state, cancelObservationInput, runContext).duplicate, true, "cancellation observation exact replay is idempotent before stale CAS");
}

const storeRoot = await mkdtemp(join(tmpdir(), "pi-dag-run-store-"));
try {
  const initializationLock = { lockIdentity: H("8"), ownerTokenHash: H("9"), sessionId: "session-init", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, acquiredAt: NOW };
  assert.throws(() => new DagRunSnapshotStoreV1(storeRoot, "a/../../escaped"), /safe path segment/, "store run namespace rejects schema-valid traversal IDs");
  const mismatchedNamespaceStore = new DagRunSnapshotStoreV1(join(storeRoot, "namespace-mismatch"), run.runId);
  const mismatchedNamespaceRun = clone(run); mismatchedNamespaceRun.runId = "different-run"; rehashRun(mismatchedNamespaceRun);
  await seedBaselineFacts(mismatchedNamespaceStore);
  await assert.rejects(() => mismatchedNamespaceStore.initialize(mismatchedNamespaceRun, runContext, { lockIdentity: H("4"), ownerTokenHash: H("5"), sessionId: "session-mismatch", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, acquiredAt: NOW }), /does not match store namespace/, "store namespace is bound to exact snapshot run ID");
  for (const [index, initializationCrashPoint] of ["after_archive", "after_snapshot_temp_sync", "after_snapshot_rename", "after_lock_release_rename"].entries()) {
    const initializationCrashStore = new DagRunSnapshotStoreV1(join(storeRoot, `initialization-crash-${index}`), run.runId);
    await seedBaselineFacts(initializationCrashStore);
    const initializationContextPath = join(storeRoot, `initialization-context-${index}.json`); const initializationStatePath = join(storeRoot, `initialization-state-${index}.json`); const initializationLockPath = join(storeRoot, `initialization-lock-${index}.json`);
    const childInitializationLock = { lockIdentity: H(String((index + 3) % 10)), ownerTokenHash: H(String((index + 4) % 10)), sessionId: `session-initialization-crash-${index}`, pid: 1, processStartIdentity: "linux-proc:0", acquiredAt: NOW };
    await writeFile(initializationContextPath, JSON.stringify(runContext)); await writeFile(initializationStatePath, JSON.stringify(run)); await writeFile(initializationLockPath, JSON.stringify(childInitializationLock));
    await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", initializationCrashStore.rootDirectory, run.runId, initializationContextPath, initializationStatePath, initializationLockPath, initializationCrashPoint, "initialize-auto"], { cwd: process.cwd() }), (error) => error?.code === 86, `real child exits at initialization point ${initializationCrashPoint}`);
    await initializationCrashStore.initialize(run, runContext, initializationLock);
    assert.equal((await initializationCrashStore.read(runContext)).snapshotHash, run.snapshotHash, `initialization safely resumes after ${initializationCrashPoint}`);
  }
  const immutableCrashStore = new DagRunSnapshotStoreV1(join(storeRoot, "immutable-link-crash"), run.runId);
  const immutableCrashCore = { kind: "verification", id: "immutable-link-crash", value: "durable" }; const immutableCrashFact = { ...immutableCrashCore, hash: canonicalHash(immutableCrashCore) };
  const immutableCrashContextPath = join(storeRoot, "immutable-crash-context.json"); const immutableCrashFactPath = join(storeRoot, "immutable-crash-fact.json"); const immutableCrashLockPath = join(storeRoot, "immutable-crash-lock.json");
  await writeFile(immutableCrashContextPath, JSON.stringify(runContext)); await writeFile(immutableCrashFactPath, JSON.stringify(immutableCrashFact)); await writeFile(immutableCrashLockPath, JSON.stringify(initializationLock));
  await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", immutableCrashStore.rootDirectory, run.runId, immutableCrashContextPath, immutableCrashFactPath, immutableCrashLockPath, "after_immutable_link", "publish-fact"], { cwd: process.cwd() }), (error) => error?.code === 86, "real child exits after immutable link publication before parent-directory fsync");
  const immutableRetry = await immutableCrashStore.putImmutableFact(immutableCrashFact);
  assert.equal((await immutableCrashStore.readImmutableFact(immutableRetry.hash)).hash, immutableCrashFact.hash, "existing immutable publication retry verifies content and fsyncs its containing directory");

  const store = new DagRunSnapshotStoreV1(storeRoot, run.runId);
  await seedBaselineFacts(store);
  await store.initialize(run, runContext, initializationLock);
  await store.initialize(run, runContext, initializationLock);
  const differentGenesis = clone(run); differentGenesis.desired.reason = "different genesis"; rehashRun(differentGenesis);
  await assert.rejects(() => store.initialize(differentGenesis, runContext, initializationLock), /different genesis|already exists/, "initializer never reports success for a different revision-zero snapshot");
  await store.putImmutableFact(attachOwnershipFact);
  assert.equal((await store.read(runContext)).snapshotHash, run.snapshotHash, "atomic store initializes and validates exact canonical snapshot");
  const reviewFactPath = join(store.factsDirectory, `${run.identity.reviewReceipt.hash.slice("sha256:".length)}.json`); const reviewFactText = await readFile(reviewFactPath, "utf8");
  await rm(reviewFactPath);
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "missing review/authorization/freshness/repository immutable references invalidate snapshot liveness");
  await writeFile(reviewFactPath, reviewFactText);
  const ownerLock = { lockIdentity: attachPayload.lockIdentity, ownerTokenHash: attachPayload.ownerTokenHash, sessionId: attachPayload.sessionId, pid: attachPayload.pid, processStartIdentity: attachPayload.processStartIdentity, acquiredAt: NOW };
  const storedAttach = await store.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  assert.equal(storedAttach.accepted, true, "store commits owner attach under process-shared lock");
  const storedAttachReplay = await store.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  assert.equal(storedAttachReplay.accepted && storedAttachReplay.duplicate, true, "stored owner-attach replay resolves exact natural slot before stale owner/CAS checks");
  const attachedState = await store.read(runContext);
  assert.equal(attachedState.revision, 1, "store publishes exactly one revision after durable rename");
  const forgedLiveTakeoverPayload = { ...attachPayload, ownerTokenHash: H("4"), sessionId: "forged-takeover", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("5"), ownershipReceipt: H("6"), priorOwnerDisposition: "dead" };
  const forgedLiveTakeoverLock = { lockIdentity: forgedLiveTakeoverPayload.lockIdentity, ownerTokenHash: forgedLiveTakeoverPayload.ownerTokenHash, sessionId: forgedLiveTakeoverPayload.sessionId, pid: forgedLiveTakeoverPayload.pid, processStartIdentity: forgedLiveTakeoverPayload.processStartIdentity, acquiredAt: NOW };
  const forgedLiveTakeoverInput = reducerInput(attachedState, "attach_owner", forgedLiveTakeoverPayload, { commandId: "command-forged-takeover", idempotencyKey: "key-forged-takeover", kind: "observation" });
  const forgedTakeover = await store.mutate({ input: forgedLiveTakeoverInput, context: runContext, lock: forgedLiveTakeoverLock });
  assert.equal(forgedTakeover.accepted, false, "ordinary mutation API cannot replace an attached live conductor by claiming it dead");
  const mismatchedProcessLock = { ...ownerLock, pid: ownerLock.pid + 1 };
  await assert.rejects(() => store.mutate({ input: reducerInput(attachedState, "set_desired_run", pausePayload, { commandId: "command-mismatched-process", idempotencyKey: "key-mismatched-process" }), context: runContext, lock: mismatchedProcessLock }), DagRunStoreLockedError, "process-shared mutation requires the executing PID/start identity, not copied owner metadata");
  const storedPauseInput = reducerInput(attachedState, "set_desired_run", pausePayload, { commandId: "command-store-pause" });
  const storedPause = await store.mutate({ input: storedPauseInput, context: runContext, lock: ownerLock });
  assert.equal(storedPause.accepted, true, "store persists guarded reducer transition");
  const storedDuplicate = await store.mutate({ input: storedPauseInput, context: runContext, lock: ownerLock });
  assert.equal(storedDuplicate.accepted && storedDuplicate.duplicate, true, "store returns exact duplicate without another write");
  assert.equal((await store.read(runContext)).revision, 2, "duplicate replay does not advance stored revision");
  const staleStoreInput = reducerInput(attachedState, "set_desired_run", pausePayload, { commandId: "command-store-stale" });
  const staleStored = await store.mutate({ input: staleStoreInput, context: runContext, lock: ownerLock });
  assert.equal(staleStored.accepted, false, "snapshot CAS rejects stale store writer");

  const raceStore = new DagRunSnapshotStoreV1(join(storeRoot, "owner-race"), run.runId);
  await seedBaselineFacts(raceStore);
  await raceStore.initialize(run, runContext, initializationLock);
  await raceStore.putImmutableFact(attachOwnershipFact);
  await raceStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  const raceState = await raceStore.read(runContext);
  const raceContextPath = join(storeRoot, "race-context.json");
  await writeFile(raceContextPath, JSON.stringify(runContext));
  const copiedIdentityInputPath = join(storeRoot, "copied-identity-input.json"); const copiedIdentityLockPath = join(storeRoot, "copied-identity-lock.json");
  const copiedIdentityInput = reducerInput(await store.read(runContext), "set_desired_run", { desired: "running", reason: null, requestedBy: "user" }, { commandId: "command-copied-identity", idempotencyKey: "key-copied-identity" });
  await writeFile(copiedIdentityInputPath, JSON.stringify(copiedIdentityInput)); await writeFile(copiedIdentityLockPath, JSON.stringify(ownerLock));
  const { stdout: copiedIdentityStdout } = await execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", store.rootDirectory, run.runId, raceContextPath, copiedIdentityInputPath, copiedIdentityLockPath], { cwd: process.cwd() });
  assert.equal(JSON.parse(copiedIdentityStdout).code, "LOCKED", "different process cannot mutate by copying the attached owner's PID/start/token metadata");
  const resumePayload = { desired: "running", reason: null, requestedBy: "user" };
  const raceFixtures = await Promise.all(Array.from({ length: 6 }, async (_, index) => {
    const input = reducerInput(raceState, "set_desired_run", resumePayload, { commandId: `command-race-${index}`, idempotencyKey: `race-${index}` });
    const inputPath = join(storeRoot, `race-input-${index}.json`);
    const lockPath = join(storeRoot, `race-lock-${index}.json`);
    const lockTemplate = { lockIdentity: H(String((index + 1) % 10)), ownerTokenHash: H(String((index + 2) % 10)), sessionId: `session-race-${index}`, pid: 1, processStartIdentity: "linux-proc:0", acquiredAt: NOW };
    await writeFile(inputPath, JSON.stringify(input)); await writeFile(lockPath, JSON.stringify(lockTemplate));
    return { inputPath, lockPath };
  }));
  const raceResults = await Promise.all(raceFixtures.map(async ({ inputPath, lockPath }, index) => {
    const { stdout } = await execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", raceStore.rootDirectory, run.runId, raceContextPath, inputPath, lockPath, "", "transfer-cas-auto"], { cwd: process.cwd() });
    return { ...JSON.parse(stdout), index, lockPath };
  }));
  assert.equal(raceResults.filter(({ accepted }) => accepted).length, 0, "unrelated successor processes cannot self-author same-manager transfer authority");
  assert(raceResults.every(({ code }) => code === "LOCKED"), "every self-authored cross-process transfer is rejected at the current-owner boundary");
  assert.equal((await raceStore.read(runContext)).revision, 1, "rejected cross-process transfers do not advance the owner epoch or snapshot");

  const fact = { kind: "test_fact", value: "immutable" };
  const storedFact = await store.putImmutableFact(fact);
  assert.equal(canonicalHash(await store.readImmutableFact(storedFact.hash)), canonicalHash(fact), "immutable fact store round-trips canonical hash content");
  assert.equal((await store.putImmutableFact(fact)).hash, storedFact.hash, "immutable fact replay is idempotent");
  await writeFile(storedFact.path, "{corrupt-bytes");
  const corruptEnvelope = await store.quarantineCorruptImmutableFact(storedFact.hash, NOW);
  assert.match(corruptEnvelope.rawPath, /corrupt-facts/, "byte-corrupt immutable content is durably retained outside the authoritative fact namespace");
  const corruptQuarantineState = await store.read(runContext);
  const corruptQuarantine = { quarantineId: "quarantine-corrupt-bytes", fact: corruptEnvelope.ref, reason: "corrupt_fact", observedBindingHash: canonicalHash(corruptEnvelope.fact), expectedBindingHash: storedFact.hash, state: "held", observedAt: NOW, adoptionReceipt: null, rejectionReason: null };
  const corruptQuarantineInput = reducerInput(corruptQuarantineState, "quarantine_fact", { quarantine: corruptQuarantine }, { kind: "observation", commandId: "command-quarantine-corrupt-bytes", idempotencyKey: "key-quarantine-corrupt-bytes" });
  assert.equal((await store.mutate({ input: corruptQuarantineInput, context: runContext, lock: ownerLock })).accepted, true, "canonical corruption envelope enters held quarantine without treating corrupt bytes as authority");
  const storedLateFact = await store.putImmutableFact(lateFact);
  const storeQuarantineState = await store.read(runContext);
  const storeQuarantineRef = { ...ref("worker_result", "stored-corrupt-fact", storedLateFact.hash), bytes: storedLateFact.bytes };
  const storeQuarantine = { quarantineId: "quarantine-stored", fact: storeQuarantineRef, reason: "identity_mismatch", observedBindingHash: H("8"), expectedBindingHash: null, state: "held", observedAt: NOW, adoptionReceipt: null, rejectionReason: null };
  const storeQuarantineInput = reducerInput(storeQuarantineState, "quarantine_fact", { quarantine: storeQuarantine }, { kind: "observation", commandId: "command-store-quarantine", idempotencyKey: "key-store-quarantine" });
  const storedQuarantineResult = await store.mutate({ input: storeQuarantineInput, context: runContext, lock: ownerLock });
  assert.equal(storedQuarantineResult.accepted, true, "store retains a canonical immutable fact whose semantic identity mismatches the expected binding");
  if (storedQuarantineResult.accepted) {
    const storedEntryHash = canonicalHash({ quarantineId: storeQuarantine.quarantineId, fact: storeQuarantine.fact, reason: storeQuarantine.reason, observedBindingHash: storeQuarantine.observedBindingHash, expectedBindingHash: storeQuarantine.expectedBindingHash, observedAt: storeQuarantine.observedAt });
    const storedAuthorityCore = { kind: "quarantine_authority", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, quarantineId: storeQuarantine.quarantineId, factHash: storeQuarantine.fact.hash, quarantineEntryHash: storedEntryHash, decision: "adopt", issuedBy: "user", issuedAt: NOW };
    const storedAuthority = { ...storedAuthorityCore, hash: canonicalHash(storedAuthorityCore) };
    const storedResolutionCore = { kind: "quarantine_resolution", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, quarantineId: storeQuarantine.quarantineId, factHash: storeQuarantine.fact.hash, quarantineEntryHash: storedEntryHash, authorityReceiptHash: storedAuthority.hash, disposition: "adopted", rationaleHash: H("4") };
    const storedResolution = { ...storedResolutionCore, hash: canonicalHash(storedResolutionCore) };
    await store.putImmutableFact(storedAuthority); await store.putImmutableFact(storedResolution);
    const storedAdoptionContext = { ...runContext, facts: { [storedResolution.hash]: storedResolution }, authorityReceipts: { [storedAuthority.hash]: storedAuthority } };
    const storedAdoptionPayload = { quarantineId: storeQuarantine.quarantineId, adoptionReceipt: storedResolution.hash };
    const storedAdoption = await store.mutate({ input: reducerInput(storedQuarantineResult.state, "adopt_quarantined_fact", storedAdoptionPayload, { kind: "observation", commandId: "command-store-adopt-quarantine", idempotencyKey: "key-store-adopt-quarantine" }), context: storedAdoptionContext, lock: ownerLock });
    assert.equal(storedAdoption.accepted && (await store.read(runContext)).quarantine[storeQuarantine.quarantineId].state, "adopted", "trusted external adoption receipts persist and resume from immutable store content");
  }
  const storedLateFactText = await readFile(storedLateFact.path, "utf8");
  await rm(storedLateFact.path);
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "snapshot fails closed when a referenced quarantined immutable fact disappears");
  await writeFile(storedLateFact.path, storedLateFactText);

  const deadTransferState = await store.read(runContext);
  const deadSuccessor = { ownerTokenHash: H("c"), sessionId: "session-dead-transfer", pid: 99999999, processStartIdentity: "linux-proc:0", lockIdentity: H("b") };
  const deadTransferOwnership = ownershipFactFor(deadTransferState, deadSuccessor, "same_manager", H("a"));
  await store.putImmutableFact(deadTransferOwnership);
  const deadTransferPayload = { ...deadSuccessor, ownershipReceipt: deadTransferOwnership.hash, priorOwnerDisposition: "same_manager" };
  const deadTransferContext = { ...runContext, facts: { [deadTransferOwnership.hash]: deadTransferOwnership } };
  const deadTransferInput = reducerInput(deadTransferState, "transfer_owner", deadTransferPayload, { commandId: "command-dead-transfer", idempotencyKey: "key-dead-transfer" });
  assert.equal((await store.mutate({ input: deadTransferInput, context: deadTransferContext, lock: ownerLock })).accepted, true, "exact current owner explicitly authorizes direct same-manager successor transfer");
  const deadTransferReplay = await store.mutate({ input: deadTransferInput, context: runContext, lock: ownerLock });
  assert.equal(deadTransferReplay.accepted && deadTransferReplay.duplicate, true, "direct-transfer acknowledgement replay resolves before stale successor-owner checks");
  const abandonedLock = { ...deadSuccessor, acquiredAt: NOW };
  assert.equal(await store.inspectLock(), null, "successful transfer releases operation lock before successor dies idle");
  const currentBeforeRecovery = await store.read(runContext);
  const replacementSuccessor = { ownerTokenHash: H("1"), sessionId: "session-replacement", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("2") };
  const deadOwnerProof = await createDagRunStoreDeadOwnerProofV1(abandonedLock, NOW);
  const replacementOwnershipFact = ownershipFactFor(currentBeforeRecovery, replacementSuccessor, "dead", null, deadOwnerProof.observationHash);
  const storedReplacementOwnership = await store.putImmutableFact(replacementOwnershipFact);
  const replacementAttachPayload = { ...replacementSuccessor, ownershipReceipt: replacementOwnershipFact.hash, priorOwnerDisposition: "dead" };
  const replacementContext = { ...runContext, facts: { ...runContext.facts, [replacementOwnershipFact.hash]: replacementOwnershipFact } };
  const replacementLock = { lockIdentity: replacementAttachPayload.lockIdentity, ownerTokenHash: replacementAttachPayload.ownerTokenHash, sessionId: replacementAttachPayload.sessionId, pid: replacementAttachPayload.pid, processStartIdentity: replacementAttachPayload.processStartIdentity, acquiredAt: NOW };
  const replacementInput = reducerInput(currentBeforeRecovery, "attach_owner", replacementAttachPayload, { commandId: "command-owner-recovery", idempotencyKey: "key-owner-recovery", kind: "observation" });
  await assert.rejects(() => store.reattachAfterDeadOwner(deadOwnerProof, replacementInput, replacementContext, replacementLock, async () => false), /not proven/, "stale metadata alone never authorizes lock takeover");
  const recoveredOwner = await store.reattachAfterDeadOwner(deadOwnerProof, replacementInput, replacementContext, replacementLock, async () => true);
  assert.match(recoveredOwner.quarantinedLockPath, /quarantined-locks/, "proven-dead idle owner recovery retains durable process evidence");
  assert.equal(recoveredOwner.result.accepted && recoveredOwner.result.state.owner.ownerEpoch, 3, "dead-owner reattach atomically fences the prior owner epoch before releasing recovery lock");
  const storedReplacementOwnershipText = await readFile(storedReplacementOwnership.path, "utf8");
  await rm(storedReplacementOwnership.path);
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "attached owner becomes corrupt if its immutable ownership proof disappears");
  await writeFile(storedReplacementOwnership.path, storedReplacementOwnershipText);

  for (const [index, recoveryCrashPoint] of ["after_recovery_intent", "after_stale_lock_quarantine", "after_replacement_lock", "after_snapshot_rename"].entries()) {
    const recoveryCrashStore = new DagRunSnapshotStoreV1(join(storeRoot, `recovery-crash-${index}`), run.runId);
    await seedBaselineFacts(recoveryCrashStore);
    await recoveryCrashStore.initialize(run, runContext, initializationLock);
    await recoveryCrashStore.putImmutableFact(attachOwnershipFact);
    assert.equal((await recoveryCrashStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock })).accepted, true, `recovery crash fixture ${recoveryCrashPoint} attaches initial owner`);
    const fixtureTransferState = await recoveryCrashStore.read(runContext);
    const fixtureDeadSuccessor = { lockIdentity: H(String((index + 1) % 10)), ownerTokenHash: H(String((index + 2) % 10)), sessionId: `session-fixture-transfer-${index}`, pid: 99999990 + index, processStartIdentity: "linux-proc:0", acquiredAt: NOW };
    const fixtureTransferOwnership = ownershipFactFor(fixtureTransferState, fixtureDeadSuccessor, "same_manager", H(String((index + 3) % 10)));
    await recoveryCrashStore.putImmutableFact(fixtureTransferOwnership);
    const fixtureTransferPayload = { ownerTokenHash: fixtureDeadSuccessor.ownerTokenHash, sessionId: fixtureDeadSuccessor.sessionId, pid: fixtureDeadSuccessor.pid, processStartIdentity: fixtureDeadSuccessor.processStartIdentity, lockIdentity: fixtureDeadSuccessor.lockIdentity, ownershipReceipt: fixtureTransferOwnership.hash, priorOwnerDisposition: "same_manager" };
    const fixtureTransferContext = { ...runContext, facts: { [fixtureTransferOwnership.hash]: fixtureTransferOwnership } };
    const fixtureTransferInput = reducerInput(fixtureTransferState, "transfer_owner", fixtureTransferPayload, { commandId: `command-fixture-transfer-${index}`, idempotencyKey: `key-fixture-transfer-${index}` });
    assert.equal((await recoveryCrashStore.mutate({ input: fixtureTransferInput, context: fixtureTransferContext, lock: ownerLock })).accepted, true, `recovery crash fixture ${recoveryCrashPoint} is explicitly transferred by current owner`);
    const fixtureAbandonedLock = fixtureDeadSuccessor;
    await mkdir(recoveryCrashStore.lockDirectory);
    await writeFile(recoveryCrashStore.lockMetadataPath, JSON.stringify(fixtureAbandonedLock));
    const recoveryCrashState = await recoveryCrashStore.read(runContext);
    const recoveryCrashProof = await createDagRunStoreDeadOwnerProofV1(fixtureAbandonedLock, NOW);
    const recoveryObservationHash = recoveryCrashProof.observationHash;
    const recoveryTemplateLock = { lockIdentity: H(String((index + 4) % 10)), ownerTokenHash: H(String((index + 5) % 10)), sessionId: `session-recovery-child-${index}`, pid: 1, processStartIdentity: "linux-proc:0", acquiredAt: NOW };
    const recoveryTemplatePayload = { ownerTokenHash: recoveryTemplateLock.ownerTokenHash, sessionId: recoveryTemplateLock.sessionId, pid: 1, processStartIdentity: "linux-proc:0", lockIdentity: recoveryTemplateLock.lockIdentity, ownershipReceipt: H("a"), priorOwnerDisposition: "dead" };
    const recoveryTemplateInput = reducerInput(recoveryCrashState, "attach_owner", recoveryTemplatePayload, { commandId: `command-recovery-crash-${index}`, idempotencyKey: `key-recovery-crash-${index}`, kind: "observation" });
    const recoveryContextPath = join(storeRoot, `recovery-context-${index}.json`); const recoveryInputPath = join(storeRoot, `recovery-input-${index}.json`); const recoveryLockPath = join(storeRoot, `recovery-lock-${index}.json`); const recoveryProofPath = join(storeRoot, `recovery-proof-${index}.json`);
    await writeFile(recoveryContextPath, JSON.stringify(runContext)); await writeFile(recoveryInputPath, JSON.stringify(recoveryTemplateInput)); await writeFile(recoveryLockPath, JSON.stringify(recoveryTemplateLock)); await writeFile(recoveryProofPath, JSON.stringify(recoveryCrashProof));
    await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", recoveryCrashStore.rootDirectory, run.runId, recoveryContextPath, recoveryInputPath, recoveryLockPath, recoveryCrashPoint, "recover-auto", recoveryProofPath], { cwd: process.cwd() }), (error) => error?.code === 86, `real child exits at recovery point ${recoveryCrashPoint}`);
    const stateAfterRecoveryCrash = await recoveryCrashStore.read(runContext);
    const visibleAfterRecoveryCrash = await recoveryCrashStore.inspectLock();
    const retryPriorLock = stateAfterRecoveryCrash.owner.lockIdentity === fixtureAbandonedLock.lockIdentity ? fixtureAbandonedLock : visibleAfterRecoveryCrash;
    const retryObservationHash = stateAfterRecoveryCrash.owner.lockIdentity === fixtureAbandonedLock.lockIdentity ? recoveryObservationHash : H(String((index + 7) % 10));
    const retryProof = await createDagRunStoreDeadOwnerProofV1(retryPriorLock, NOW);
    const retrySuccessor = { ownerTokenHash: H(String((index + 6) % 10)), sessionId: `session-recovery-parent-${index}`, pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H(String((index + 8) % 10)) };
    const retryOwnership = ownershipFactFor(stateAfterRecoveryCrash, retrySuccessor, "dead", null, retryProof.observationHash);
    await recoveryCrashStore.putImmutableFact(retryOwnership);
    const retryPayload = { ...retrySuccessor, ownershipReceipt: retryOwnership.hash, priorOwnerDisposition: "dead" };
    const retryContext = { ...runContext, facts: { [retryOwnership.hash]: retryOwnership } };
    const retryLock = { ...retrySuccessor, acquiredAt: NOW };
    const retryInput = reducerInput(stateAfterRecoveryCrash, "attach_owner", retryPayload, { commandId: `command-recovery-retry-${index}`, idempotencyKey: `key-recovery-retry-${index}`, kind: "observation" });
    const retryResult = await recoveryCrashStore.reattachAfterDeadOwner(retryProof, retryInput, retryContext, retryLock, async () => true);
    assert.equal(retryResult.result.accepted, true, `recovery resumes safely after ${recoveryCrashPoint}`);
  }

  const effectFactStore = new DagRunSnapshotStoreV1(join(storeRoot, "effect-fact-loss"), run.runId);
  await seedBaselineFacts(effectFactStore);
  await effectFactStore.initialize(run, runContext, initializationLock);
  await effectFactStore.putImmutableFact(attachOwnershipFact);
  await effectFactStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  const effectFactOwnerState = await effectFactStore.read(runContext);
  const durableEffect = { effectId: "effect-durable-observation", kind: "run_procedure", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("7"), boundOwnerEpoch: 1, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("8"), state: "intended", dispatchCount: 0, createdRevision: effectFactOwnerState.revision + 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
  const durableIntent = await effectFactStore.mutate({ input: reducerInput(effectFactOwnerState, "put_effect_intent", { effect: durableEffect }, { commandId: "command-durable-effect", idempotencyKey: "key-durable-effect" }), context: runContext, lock: ownerLock });
  if (!durableIntent.accepted) throw new Error(`durable effect intent failed: ${durableIntent.message}`);
  const durableDispatch = await effectFactStore.mutate({ input: reducerInput(durableIntent.state, "mark_effect_dispatching", { effectId: durableEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-dispatch-durable-effect", idempotencyKey: "key-dispatch-durable-effect" }), context: runContext, lock: ownerLock });
  if (!durableDispatch.accepted) throw new Error(`durable effect dispatch failed: ${durableDispatch.message}`);
  const durableReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: durableEffect.effectId, requestHash: durableEffect.requestHash, reconciliation: "applied_exact" };
  const durableReconciliationFact = { ...durableReconciliationCore, hash: canonicalHash(durableReconciliationCore) };
  const storedDurableReconciliation = await effectFactStore.putImmutableFact(durableReconciliationFact);
  const durableObservationContext = { ...runContext, facts: { [durableReconciliationFact.hash]: durableReconciliationFact } };
  const durableObservationPayload = { effectId: durableEffect.effectId, observationHash: durableReconciliationFact.hash, reconciliation: "applied_exact", terminalState: "reconciled" };
  const durableObservationInput = reducerInput(durableDispatch.state, "record_effect_observation", durableObservationPayload, { commandId: "command-observe-durable-effect", idempotencyKey: "key-observe-durable-effect", kind: "observation" });
  const durableObservation = await effectFactStore.mutate({ input: durableObservationInput, context: durableObservationContext, lock: ownerLock });
  assert.equal(durableObservation.accepted, true, "effect observation cannot become authoritative before immutable reconciliation fact publication");
  const durableObservationReplay = await effectFactStore.mutate({ input: durableObservationInput, context: durableObservationContext, lock: ownerLock });
  assert.equal(durableObservationReplay.accepted && durableObservationReplay.duplicate, true, "effect observation exact replay is idempotent before stale CAS");
  const durableReconciliationText = await readFile(storedDurableReconciliation.path, "utf8");
  await rm(storedDurableReconciliation.path);
  await assert.rejects(() => effectFactStore.read(runContext), DagRunStoreCorruptError, "reconciled effect fails closed when immutable observation disappears");
  await writeFile(storedDurableReconciliation.path, durableReconciliationText);

  const stableReadRoot = join(storeRoot, "stable-read-race");
  const stableReadWriter = new DagRunSnapshotStoreV1(stableReadRoot, run.runId);
  await seedBaselineFacts(stableReadWriter); await stableReadWriter.initialize(run, runContext, initializationLock); await stableReadWriter.putImmutableFact(attachOwnershipFact);
  await stableReadWriter.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  const stableAttached = await stableReadWriter.read(runContext);
  const stablePauseInput = reducerInput(stableAttached, "set_desired_run", pausePayload, { commandId: "command-stable-pause", idempotencyKey: "key-stable-pause" });
  await stableReadWriter.mutate({ input: stablePauseInput, context: runContext, lock: ownerLock });
  let releaseStableRead; let announceStableRead; let stableReadArmed = true;
  const stableReadEntered = new Promise((resolve) => { announceStableRead = resolve; });
  const stableReadRelease = new Promise((resolve) => { releaseStableRead = resolve; });
  const stableReadReader = new DagRunSnapshotStoreV1(stableReadRoot, run.runId, { failpoint: async (point) => { if (stableReadArmed && point === "after_snapshot_read") { stableReadArmed = false; announceStableRead(); await stableReadRelease; } } });
  const racingRead = stableReadReader.read(runContext);
  await stableReadEntered;
  const stablePaused = await stableReadWriter.read(runContext);
  const stableResumePayload = { desired: "running", reason: null, requestedBy: "user" };
  const stableResume = await stableReadWriter.mutate({ input: reducerInput(stablePaused, "set_desired_run", stableResumePayload, { commandId: "command-stable-resume", idempotencyKey: "key-stable-resume" }), context: runContext, lock: ownerLock });
  releaseStableRead();
  const stableReadResult = await racingRead;
  assert.equal(stableResume.accepted && stableReadResult.snapshotHash, stableResume.accepted ? stableResume.state.snapshotHash : null, "lock-free reader retries a healthy snapshot when concurrent pruning removes its captured predecessor");

  const chainState = await store.read(runContext);
  const chainPreviousPath = join(store.snapshotsDirectory, `${chainState.previousSnapshotHash.slice("sha256:".length)}.json`);
  const chainPreviousText = await readFile(chainPreviousPath, "utf8");
  await writeFile(chainPreviousPath, "{}");
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "snapshot read verifies exact archived predecessor hash/revision chain");
  await writeFile(chainPreviousPath, chainPreviousText);
  const validStoredText = await readFile(store.statePath, "utf8");
  await writeFile(store.statePath, JSON.stringify(JSON.parse(validStoredText), null, 2));
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "noncanonical current snapshot bytes fail closed");
  await writeFile(store.statePath, `${validStoredText.slice(0, -1)},"corrupt":true}`);
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "corrupt current snapshot fails closed rather than falling back silently");
  await writeFile(store.statePath, validStoredText);
  const currentArchivePath = join(store.snapshotsDirectory, `${chainState.snapshotHash.slice("sha256:".length)}.json`);
  const currentArchiveText = await readFile(currentArchivePath, "utf8");
  await rm(currentArchivePath);
  await assert.rejects(() => store.read(runContext), DagRunStoreCorruptError, "missing immutable archive for current snapshot fails closed");
  await writeFile(currentArchivePath, currentArchiveText);

  if (!attachedResult.accepted) throw new Error("attached reducer fixture unexpectedly failed");
  const crashPauseInput = reducerInput(attachedResult.state, "set_desired_run", pausePayload, { commandId: "command-crash-pause", idempotencyKey: "key-crash-pause" });
  const processCrashStore = new DagRunSnapshotStoreV1(join(storeRoot, "process-crash"), run.runId);
  await seedBaselineFacts(processCrashStore);
  await processCrashStore.initialize(run, runContext, initializationLock);
  const processCrashContextPath = join(storeRoot, "process-crash-context.json");
  const processCrashInputPath = join(storeRoot, "process-crash-input.json");
  const processCrashLockPath = join(storeRoot, "process-crash-lock.json");
  const processCrashLockTemplate = { lockIdentity: H("8"), ownerTokenHash: H("9"), sessionId: "session-process-crash-child", pid: 1, processStartIdentity: "linux-proc:0", acquiredAt: NOW };
  await writeFile(processCrashContextPath, JSON.stringify(runContext)); await writeFile(processCrashInputPath, JSON.stringify(crashPauseInput)); await writeFile(processCrashLockPath, JSON.stringify(processCrashLockTemplate));
  await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", processCrashStore.rootDirectory, run.runId, processCrashContextPath, processCrashInputPath, processCrashLockPath, "after_snapshot_rename", "attach-auto"], { cwd: process.cwd() }), (error) => error?.code === 86, "real child process exits after owner-transfer snapshot rename before lock-finally/acknowledgement");
  const committedAfterCrash = await processCrashStore.read(runContext);
  assert.equal(committedAfterCrash.revision, run.revision + 1, "post-rename process crash leaves a hash-chain-valid committed owner-transfer snapshot");
  const crashedLock = await processCrashStore.inspectLock();
  const crashRecoverySuccessor = { ownerTokenHash: H("4"), sessionId: "session-crash-recovery", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("5") };
  const crashRecoveryProof = await createDagRunStoreDeadOwnerProofV1(crashedLock, NOW);
  const crashRecoveryOwnershipFact = ownershipFactFor(committedAfterCrash, crashRecoverySuccessor, "dead", null, crashRecoveryProof.observationHash);
  await processCrashStore.putImmutableFact(crashRecoveryOwnershipFact);
  const crashRecoveryPayload = { ...crashRecoverySuccessor, ownershipReceipt: crashRecoveryOwnershipFact.hash, priorOwnerDisposition: "dead" };
  const crashRecoveryContext = { ...runContext, facts: { ...runContext.facts, [crashRecoveryOwnershipFact.hash]: crashRecoveryOwnershipFact } };
  const crashRecoveryLock = { lockIdentity: crashRecoveryPayload.lockIdentity, ownerTokenHash: crashRecoveryPayload.ownerTokenHash, sessionId: crashRecoveryPayload.sessionId, pid: crashRecoveryPayload.pid, processStartIdentity: crashRecoveryPayload.processStartIdentity, acquiredAt: NOW };
  const crashRecoveryInput = reducerInput(committedAfterCrash, "attach_owner", crashRecoveryPayload, { commandId: "command-crash-recovery", idempotencyKey: "key-crash-recovery", kind: "observation" });
  await processCrashStore.reattachAfterDeadOwner(crashRecoveryProof, crashRecoveryInput, crashRecoveryContext, crashRecoveryLock, async () => true);
  const crashTransferInput = JSON.parse(await readFile(processCrashInputPath, "utf8"));
  const crashReplay = await processCrashStore.mutate({ input: crashTransferInput, context: runContext, lock: crashRecoveryLock });
  assert.equal(crashReplay.accepted && crashReplay.duplicate, true, "post-crash exact owner-transfer replay reconciles through durable natural slot before stale owner/CAS checks");

  let beforeRenameArmed = false;
  const beforeRenameStore = new DagRunSnapshotStoreV1(join(storeRoot, "crash-before"), run.runId, { failpoint: async (point) => { if (beforeRenameArmed && point === "after_snapshot_temp_sync") throw new Error("failpoint-before-rename"); } });
  await seedBaselineFacts(beforeRenameStore);
  await beforeRenameStore.initialize(run, runContext, initializationLock);
  await beforeRenameStore.putImmutableFact(attachOwnershipFact);
  const beforeCrashAttach = await beforeRenameStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  assert.equal(beforeCrashAttach.accepted, true, "crash fixture establishes attached owner through revision-zero initialization");
  beforeRenameArmed = true;
  await assert.rejects(() => beforeRenameStore.mutate({ input: crashPauseInput, context: runContext, lock: ownerLock }), /failpoint-before-rename/, "crash before rename surfaces uncertainty");
  assert.equal((await beforeRenameStore.read(runContext)).revision, attachedResult.state.revision, "crash before rename preserves old authoritative snapshot");
  beforeRenameArmed = false;
  assert.equal((await beforeRenameStore.mutate({ input: crashPauseInput, context: runContext, lock: ownerLock })).accepted, true, "same command safely resumes after pre-rename crash");

  let afterRenameArmed = false;
  const afterRenameStore = new DagRunSnapshotStoreV1(join(storeRoot, "crash-after"), run.runId, { failpoint: async (point) => { if (afterRenameArmed && point === "after_snapshot_rename") throw new Error("failpoint-after-rename"); } });
  await seedBaselineFacts(afterRenameStore);
  await afterRenameStore.initialize(run, runContext, initializationLock);
  await afterRenameStore.putImmutableFact(attachOwnershipFact);
  const afterCrashAttach = await afterRenameStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: ownerLock });
  assert.equal(afterCrashAttach.accepted, true, "post-rename crash fixture establishes attached owner");
  afterRenameArmed = true;
  await assert.rejects(() => afterRenameStore.mutate({ input: crashPauseInput, context: runContext, lock: ownerLock }), /failpoint-after-rename/, "crash after rename surfaces uncertain acknowledgement");
  const afterRenameState = await afterRenameStore.read(runContext);
  assert.equal(afterRenameState.revision, attachedResult.state.revision + 1, "renamed snapshot remains authoritative after acknowledgement crash");
  afterRenameArmed = false;
  const reconciledReplay = await afterRenameStore.mutate({ input: crashPauseInput, context: runContext, lock: ownerLock });
  assert.equal(reconciledReplay.accepted && reconciledReplay.duplicate, true, "exact replay reconciles crash-after-commit as a no-op");
} finally {
  await rm(storeRoot, { recursive: true, force: true });
}

const schedulableRun = clone(run);
schedulableRun.owner = { ...schedulableRun.owner, ownerEpoch: 1, ownerTokenHash: H("8"), sessionId: "scheduler-session", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("9"), attachedAt: NOW, lastHeartbeatAt: NOW, ownershipReceipt: H("a") };
schedulableRun.current.run = "active";
const schedulerIndex = buildSchedulerPlanIndexV1(plan);
schedulableRun.scheduler.policyHash = DAG_SCHEDULER_POLICY_HASH_V1;
schedulableRun.scheduler.normalizedIndexHash = schedulerIndex.indexHash;
rehashRun(schedulableRun);
const schedulerDecision = scheduleDagRunV1(plan, schedulableRun);
assert.equal(schedulerDecision.notice, "RESERVATIONS_PROPOSED", "scheduler proposes work only from exact current run inputs");
assert.deepEqual(schedulerDecision.selected.map(({ workItemId, stage, operationKind }) => ({ workItemId, stage, operationKind })), [{ workItemId: "item-api", stage: "F0", operationKind: "conductor" }], "scheduler admits the fixed F0 conductor slot");
assert.equal(schedulerDecision.policyHash, DAG_SCHEDULER_POLICY_HASH_V1, "scheduler decision binds the accepted policy hash");
const attachedSchedulerRun = clone(attachedResult.state);
attachedSchedulerRun.workItems["item-api"].current = "ready"; attachedSchedulerRun.current.run = "active"; attachedSchedulerRun.current.readyWorkItemIds = ["item-api"];
attachedSchedulerRun.scheduler.policyHash = DAG_SCHEDULER_POLICY_HASH_V1; attachedSchedulerRun.scheduler.normalizedIndexHash = schedulerIndex.indexHash; rehashRun(attachedSchedulerRun);
const attachedSchedulerDecision = scheduleDagRunV1(plan, attachedSchedulerRun);
const reservePayload = { decisionHash: attachedSchedulerDecision.decisionHash, decisionSequence: attachedSchedulerDecision.decisionSequence, policyHash: attachedSchedulerDecision.policyHash, normalizedIndexHash: attachedSchedulerDecision.normalizedIndexHash, inputSnapshotHash: attachedSchedulerRun.snapshotHash, reservations: attachedSchedulerDecision.selected, bypassSlotIds: attachedSchedulerDecision.bypassIncrements };
const schedulerReducerContext = { ...ownerContext, normalizedSchedulerIndexHash: schedulerIndex.indexHash };
const forgedReservation = { ...reservePayload.reservations[0], stage: "F8" }; const forgedBatch = reduceDagRunV1(attachedSchedulerRun, reducerInput(attachedSchedulerRun, "reserve_scheduler_batch", { ...reservePayload, reservations: [forgedReservation] }, { commandId: "command-scheduler-forged", idempotencyKey: "key-scheduler-forged" }), schedulerReducerContext); assert.equal(forgedBatch.accepted, false, "reducer exact-recomputes scheduler authority and rejects a forged F8 reservation under an honest decision hash");
const reservedResult = reduceDagRunV1(attachedSchedulerRun, reducerInput(attachedSchedulerRun, "reserve_scheduler_batch", reservePayload, { commandId: "command-scheduler-reserve", idempotencyKey: "key-scheduler-reserve" }), schedulerReducerContext);
assert.equal(reservedResult.accepted, true, "scheduler batch reservation commits sticky lane and leases through the closed reducer");
assert.equal(reservedResult.accepted && reservedResult.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, null, "scheduler lane remains sticky after operation reservation");
assert.equal(reservedResult.accepted && Object.keys(reservedResult.state.scheduler.reservations).length, 1, "scheduler reducer records one exact reservation");
if (reservedResult.accepted) {
  const reservationId = attachedSchedulerDecision.selected[0].reservationId;
  const releasePayload = { reservationId, disposition: "released", reason: "operation observation reconciled" };
  const releasedResult = reduceDagRunV1(reservedResult.state, reducerInput(reservedResult.state, "release_scheduler_reservation", releasePayload, { kind: "observation", commandId: "command-scheduler-release", idempotencyKey: "key-scheduler-release" }), schedulerReducerContext);
  assert.equal(releasedResult.accepted, true, "scheduler reservation release reconciles exact leases through reducer");
  assert.equal(releasedResult.accepted && releasedResult.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, null, "operation release does not release the sticky active-node lane");
}
const schedulerReplay = scheduleDagRunV1(plan, clone(schedulableRun));
assert.equal(schedulerReplay.decisionHash, schedulerDecision.decisionHash, "scheduler replay is byte-deterministic for one snapshot");
const pausedSchedulerRun = clone(schedulableRun); pausedSchedulerRun.desired.run = "paused"; pausedSchedulerRun.current.run = "paused"; rehashRun(pausedSchedulerRun);
const pausedDecision = scheduleDagRunV1(plan, pausedSchedulerRun);
assert.equal(pausedDecision.notice, "PAUSED"); assert.equal(pausedDecision.selected.length, 0, "pause blocks new scheduler admission");
const operationallyBlockedRun = clone(schedulableRun); operationallyBlockedRun.workItems["item-api"].stages.F0.state = "passed"; operationallyBlockedRun.scheduler.operationalCapacities["worker.process"].observedCapacity = 0; rehashRun(operationallyBlockedRun);
const operationallyBlockedDecision = scheduleDagRunV1(plan, operationallyBlockedRun);
assert.equal(operationallyBlockedDecision.selected.length, 0, "missing exact worker capacity blocks external F1 admission");
assert(operationallyBlockedDecision.frontier[0].correctnessReady && operationallyBlockedDecision.frontier[0].admissionCodes.includes("OPERATIONAL_CAPACITY"), "operational capacity remains separate from correctness readiness");
assert.equal(operationallyBlockedDecision.bypassIncrements.length, 0, "polling without a competing admission never ages fairness debt");

const lanePlan = twoItemPlan;
const laneRun = clone(schedulableRun);
laneRun.identity.planHash = lanePlan.planHash;
laneRun.workItems["item-api"].planEntityHash = lanePlan.workItems.find(({ workItemId }) => workItemId === "item-api").contentHash;
const secondTemplate = clone(laneRun.workItems["item-api"]);
secondTemplate.workItemId = "item-worker"; secondTemplate.planEntityHash = lanePlan.workItems.find(({ workItemId }) => workItemId === "item-worker").contentHash; secondTemplate.implementationLineageHash = H("b"); secondTemplate.integrationEntryId = null;
laneRun.workItems["item-worker"] = secondTemplate;
laneRun.scheduler.normalizedIndexHash = buildSchedulerPlanIndexV1(lanePlan).indexHash;
laneRun.scheduler.maxActiveNodes = 1;
rehashRun(laneRun);
const laneDecision = scheduleDagRunV1(lanePlan, laneRun);
assert.equal(laneDecision.selected.length, 1, "explicit maxActiveNodes admits only one new sticky lane");
assert.equal(laneDecision.frontier.filter(({ correctnessReady }) => true).length, 2, "lane capacity does not erase correctness readiness");
assert(laneDecision.frontier.some(({ admissionCodes }) => admissionCodes.includes("LANE_CAPACITY")), "non-admitted correctness-ready node explains lane capacity");
const fairnessRun = clone(laneRun); fairnessRun.workItems["item-api"].current = "active"; fairnessRun.workItems["item-api"].currentStage = "F0"; fairnessRun.workItems["item-api"].laneAdmissionSequence = 1; fairnessRun.workItems["item-api"].admittedAt = NOW; fairnessRun.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null }; fairnessRun.scheduler.nextReservationSequence = 2; fairnessRun.scheduler.bypassCounters["item-worker:0:F0:conductor"] = 8; fairnessRun.current.activeWorkItemIds = ["item-api"]; fairnessRun.current.readyWorkItemIds = ["item-worker"]; rehashRun(fairnessRun); const fairnessDecision = scheduleDagRunV1(lanePlan, fairnessRun); assert.equal(fairnessDecision.selected[0]?.workItemId, "item-api", "fairness reservation cannot deadlock the sticky incumbent required to free the lane"); assert.equal(fairnessDecision.bypassIncrements.includes("item-worker:0:F0:conductor"), false, "lane-capacity waiting alone does not accrue fairness debt");
const exclusionRun = clone(laneRun); exclusionRun.scheduler.maxActiveNodes = 2; for (const [id, sequence] of [["item-api", 1], ["item-worker", 2]]) { exclusionRun.workItems[id].current = "active"; exclusionRun.workItems[id].currentStage = "F0"; exclusionRun.workItems[id].laneAdmissionSequence = sequence; exclusionRun.workItems[id].admittedAt = NOW; exclusionRun.scheduler.activeNodeLanes[id] = { workItemId: id, admissionSequence: sequence, admittedAt: NOW, releaseDisposition: null, releasedAt: null }; } exclusionRun.scheduler.dynamicExclusions["exclusion-lanes"] = { exclusionId: "exclusion-lanes", workItemIds: ["item-api", "item-worker"], phases: ["F0"], repositoryIds: ["repo-main"], reason: "observed_incompatibility", evidenceHash: H("7"), creator: "conductor", releasePredicateHash: H("8"), state: "active", createdAt: NOW, releasedAt: null }; exclusionRun.current.activeWorkItemIds = ["item-api", "item-worker"]; exclusionRun.current.readyWorkItemIds = []; exclusionRun.scheduler.nextReservationSequence = 3; rehashRun(exclusionRun); const exclusionDecision = scheduleDagRunV1(lanePlan, exclusionRun); assert.equal(exclusionDecision.selected[0]?.workItemId, "item-api", "earlier sticky lane wins newly evidenced dynamic incompatibility"); assert(exclusionDecision.frontier.find(({ workItemId }) => workItemId === "item-worker").admissionCodes.includes("DYNAMIC_EXCLUSION"), "later sticky lane remains admitted but waits between operations until exact exclusion release");

const executionProjection = projectDagExecutionV1(plan, schedulableRun, schedulerDecision, { projectionHash: H("c"), workers: [{ storageId: "unbound", launchOwnerSessionId: "unbound-session", workerId: "generic-worker", attemptNumber: 1, attemptNonce: "0123456789abcdef", configHash: H("d"), terminalStatus: "succeeded", processDisposition: "dead", retrySafe: true, resultHash: H("e") }] });
assert.equal(executionProjection.nodes[0].glyph, ">", "execution projection derives ready glyph from canonical run state");
for (const attemptState of ["failed", "lost", "ambiguous", "quarantined"]) { const terminal = clone(schedulableRun); terminal.workItems["item-api"].current = "active"; terminal.workItems["item-api"].currentStage = "F0"; terminal.workItems["item-api"].stages.F0.state = "failed"; terminal.workItems["item-api"].stages.F0.currentAttemptId = `attempt-${attemptState}`; terminal.stageAttempts[`attempt-${attemptState}`] = { stageAttemptId: `attempt-${attemptState}`, state: attemptState }; rehashRun(terminal); const projected = projectDagExecutionV1(plan, terminal, scheduleDagRunV1(plan, terminal)); assert.equal(projected.nodes[0].glyph, "!", `terminal ${attemptState} attempts render attention rather than in-flight`); }
const postExternalAttemptId = "attempt-post-external-reserved"; const postExternalInputCore = { kind: "stage_attempt_input", planHash: plan.planHash, runId: reservedResult.state.runId, runNonce: reservedResult.state.runNonce, workItemId: "item-api", stage: "F0", stageAttemptId: postExternalAttemptId, candidateGeneration: 0, candidateHash: null, authorizationSetHash: reservedResult.state.identity.authorizationSet.hash, producerKind: "conductor", implementationLineageHash: null }; const postExternalInputFact = { ...postExternalInputCore, hash: canonicalHash(postExternalInputCore) }; const postExternalInputRef = ref("stage_attempt_input", postExternalAttemptId, postExternalInputFact.hash); const postExternalContext = { ...schedulerReducerContext, facts: { ...schedulerReducerContext.facts, [postExternalInputFact.hash]: postExternalInputFact } };
function projectionAttemptBoundary(attemptState, externalState) { const boundary = clone(reservedResult.state); const reservation = Object.values(boundary.scheduler.reservations)[0]; if (externalState === "active_reservation") reservation.state = "active"; else { for (const leaseId of reservation.leaseIds) { delete boundary.leases[leaseId]; boundary.workItems["item-api"].activeLeaseIds = boundary.workItems["item-api"].activeLeaseIds.filter((id) => id !== leaseId); } delete boundary.scheduler.reservations[reservation.reservationId]; } boundary.workItems["item-api"].stages.F0.state = "active"; boundary.workItems["item-api"].stages.F0.currentAttemptId = postExternalAttemptId; boundary.workItems["item-api"].stages.F0.attemptIds = [postExternalAttemptId]; boundary.stageAttempts[postExternalAttemptId] = { stageAttemptId: postExternalAttemptId, workItemId: "item-api", stage: "F0", ordinal: 1, producerKind: "conductor", implementationLineageHash: null, inputGeneration: 0, reservedOutputGeneration: null, attemptInput: postExternalInputRef, authorizationSetHash: boundary.identity.authorizationSet.hash, state: attemptState, launchIntentId: null, leaseIds: [], workerResult: null, evidence: null, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: ["launching", "running", "settling", "cancelling"].includes(attemptState) ? null : NOW }; boundary.evidenceIndex.stageAttemptInputs[postExternalAttemptId] = postExternalInputRef; if (externalState === "dispatching_effect") boundary.effects["effect-projection-boundary"] = { effectId: "effect-projection-boundary", kind: "put_immutable_fact", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("a"), boundOwnerEpoch: boundary.owner.ownerEpoch, boundAuthorizationSetHash: boundary.identity.authorizationSet.hash, boundFreshnessReceiptHash: boundary.freshness.receipt.hash, boundCandidateGeneration: boundary.workItems["item-api"].candidateGeneration, boundGateEpochHash: H("b"), state: "dispatching", dispatchCount: 1, createdRevision: boundary.revision, createdAt: NOW, lastDispatchAt: NOW, observationHash: null, reconciliation: "not_started", blockerId: null }; rehashRun(boundary); return boundary; }
for (const attemptState of ["result_observed", "evidence_pending", "sealed"]) for (const externalState of ["active_reservation", "dispatching_effect"]) { const boundary = projectionAttemptBoundary(attemptState, externalState); const boundaryValidation = validateDagRunStateV1(boundary, postExternalContext); assert(boundaryValidation.ok, `${attemptState} plus ${externalState} is a schema-valid projection boundary: ${JSON.stringify(boundaryValidation.issues)}`); assert.equal(projectDagExecutionV1(plan, boundary, scheduleDagRunV1(plan, boundary)).nodes[0].glyph, ":", `an exact ${attemptState} attempt suppresses a stale ${externalState} in-flight glyph`); }
for (const attemptState of ["launching", "running", "settling", "cancelling"]) { const boundary = projectionAttemptBoundary(attemptState, "attempt_only"); const boundaryValidation = validateDagRunStateV1(boundary, postExternalContext); assert(boundaryValidation.ok, `${attemptState} is a schema-valid in-flight projection boundary: ${JSON.stringify(boundaryValidation.issues)}`); assert.equal(projectDagExecutionV1(plan, boundary, scheduleDagRunV1(plan, boundary)).nodes[0].glyph, "*", `${attemptState} renders as genuinely externally in-flight`); }
assert.equal(executionProjection.nodes[0].worker, null, "unbound generic worker cannot infer DAG execution state");
assert.equal(executionProjection.projectionHash, projectDagExecutionV1(plan, schedulableRun, schedulerDecision, { projectionHash: H("f"), workers: [] }).projectionHash, "unbound generic workers do not affect semantic execution projection");
const retryProjectionState = clone(schedulableRun); const retryKey = H("9"); retryProjectionState.retryLedger[retryKey] = { retryKey, workItemId: "item-api", stage: "F1", dimension: "integration", procedureId: null, failureClass: "integration", fingerprint: H("8"), count: 1, ceiling: 2, authorizationSetHash: retryProjectionState.identity.authorizationSet.hash, candidateTrees: [], repairCommitTrees: [], progressHashes: [], failureSequence: [], stop: "none", lastRetryCommandId: "command-retry-projection" }; rehashRun(retryProjectionState); const retryDecision = scheduleDagRunV1(plan, retryProjectionState); const retryProjection = projectDagExecutionV1(plan, retryProjectionState, retryDecision); assert.equal(retryProjection.nodes.find(({ workItemId }) => workItemId === "item-api").retryCount, 1, "nonempty canonical retry ledgers project direct count fields without crashing");
for (const [width, rows] of [[50, 18], [80, 24], [120, 60]]) {
  const layout = renderDagWidgetV1(executionProjection, width, rows, NOW);
  assert(layout.lines.length <= Math.min(12, Math.max(4, Math.floor(rows / 3))), `widget respects row budget at ${width} columns`);
  assert(layout.lines.every((line) => visibleWidth(line) <= width), `widget respects width ${width}`);
  if (width === 120) assert(layout.lines.includes("active: none"), "widget active row never labels selected non-lane nodes as active");
  assert(layout.lines.at(-1).includes("read-only"), "widget is an explicitly passive surface");
}
const widgetNode = executionProjection.nodes[0]; const omissionProjection = { ...executionProjection, nodes: [1, 2, 3].map((ordinal) => ({ ...widgetNode, alias: `N0${ordinal}`, workItemId: `widget-${ordinal}`, title: `Widget ${ordinal}`, glyph: ordinal < 3 ? "!" : ".", activeLane: ordinal === 1, laneAdmissionSequence: ordinal === 1 ? 1 : null })), precedence: [], summary: { ...executionProjection.summary, attention: 2, activeLanes: 1 } }; const omissionLayout = renderDagWidgetV1(omissionProjection, 120, 12, NOW); const omissionLine = omissionLayout.lines.find((line) => line.startsWith("omitted:")); assert(omissionLine?.includes("1 nodes") && !omissionLine.includes("active N03"), "mandatory omission metadata distinguishes omitted ordinary nodes from omitted active lanes");
const denseNodes = Array.from({ length: 9 }, (_, index) => ({ ...widgetNode, alias: `N${String(index + 1).padStart(2, "0")}`, workItemId: `dense-${index + 1}`, title: `Dense ${index + 1}`, glyph: "!" })); const denseProjection = { ...executionProjection, nodes: denseNodes, precedence: denseNodes.flatMap((left, leftIndex) => denseNodes.slice(leftIndex + 1).map((right) => ({ from: left.workItemId, to: right.workItemId }))), summary: { ...executionProjection.summary, attention: 9 } }; const denseLayout = renderDagWidgetV1(denseProjection, 120, 36, NOW); assert(denseLayout.lines.some((line) => line.startsWith("omitted:") && /edges [1-9][0-9]* cut/.test(line)), "dense branch topology always reports bounded edge cuts even when every node is selected");

function buildIntegrationReadyFixture(fixturePlan, attachedState, baseContext, options = {}) {
  const generation = options.generation ?? 1; const prefix = options.prefix ?? "integration"; const startStageOrdinal = options.startStageOrdinal ?? 0; const preserveTrain = options.preserveTrain ?? false;
  const state = clone(attachedState); const facts = { ...baseContext.facts }; const procedures = procedureCatalogFixture(); const checkAggregates = { ...baseContext.catalog.checkAggregates };
  state.workItems["item-api"].candidateGeneration = generation;
  const candidateCore = { kind: "candidate", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", generation, candidateId: `candidate-${prefix}`, base: fixturePlan.repositories[0].baseline, git: { repositoryId: "repo-main", commit: O(generation === 1 ? "c" : "7"), tree: O(generation === 1 ? "d" : "6") }, patchIdentityHash: canonicalHash({ prefix, patch: true }), producedByStageAttemptId: `attempt-${prefix}-f1`, lineageHash: state.workItems["item-api"].implementationLineageHash };
  const candidateFact = { ...candidateCore, hash: canonicalHash(candidateCore) }; facts[candidateFact.hash] = candidateFact;
  state.evidenceIndex.candidates[candidateFact.hash] = ref("candidate", `candidate-${prefix}`, candidateFact.hash);
  state.workItems["item-api"].candidate = { generation, candidateId: `candidate-${prefix}`, candidateHash: candidateFact.hash, base: candidateFact.base, git: candidateFact.git, patchIdentityHash: candidateFact.patchIdentityHash, producedByStageAttemptId: candidateFact.producedByStageAttemptId, lineageHash: candidateFact.lineageHash };
  for (const stage of PLAN_STAGE_IDS.slice(startStageOrdinal)) {
    const attemptId = `attempt-${prefix}-${stage.toLowerCase()}`; const producerKind = ({ F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" })[stage];
    const inputGeneration = stage === "F0" ? 0 : stage === "F1" ? generation - 1 : generation; const inputCandidateHash = ["F0", "F1"].includes(stage) ? null : candidateFact.hash; const implementationLineageHash = ["F1", "F3"].includes(stage) ? state.workItems["item-api"].implementationLineageHash : null;
    const inputCore = { kind: "stage_attempt_input", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, candidateGeneration: inputGeneration, candidateHash: inputCandidateHash, authorizationSetHash: state.identity.authorizationSet.hash, producerKind, implementationLineageHash };
    const inputFact = { ...inputCore, hash: canonicalHash(inputCore) }; facts[inputFact.hash] = inputFact; const inputRef = ref("stage_attempt_input", attemptId, inputFact.hash); state.evidenceIndex.stageAttemptInputs[attemptId] = inputRef;
    let workerResultHash = null; let workerResultRef = null; let launchIntentId = null;
    if (producerKind === "owned_worker") {
      const workerCore = { kind: "worker_result", workerStorageId: "storage-integration", launchOwnerSessionId: `session-${prefix}-${stage.toLowerCase()}`, workerId: `worker-${prefix}-${stage.toLowerCase()}`, attemptNumber: 1, attemptNonce: `nonce-${stage.toLowerCase()}-0123456789`, configHash: canonicalHash({ stage, config: true }), completionId: `completion-${prefix}-${stage.toLowerCase()}`, terminalStatus: "succeeded", processDisposition: "dead", retrySafe: true };
      const workerFact = { ...workerCore, hash: canonicalHash(workerCore) }; facts[workerFact.hash] = workerFact; workerResultHash = workerFact.hash; workerResultRef = ref("worker_result", `result-${stage.toLowerCase()}`, workerFact.hash); state.evidenceIndex.workerResults[workerFact.hash] = workerResultRef;
      const effectId = `effect-launch-${prefix}-${stage.toLowerCase()}`; state.effects[effectId] = { effectId, kind: "launch_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: canonicalHash({ stage, launch: true }), boundOwnerEpoch: 0, boundAuthorizationSetHash: state.identity.authorizationSet.hash, boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: inputGeneration, boundGateEpochHash: H("3"), state: "reconciled", dispatchCount: 1, createdRevision: 0, createdAt: NOW, lastDispatchAt: NOW, observationHash: null, reconciliation: "applied_exact", blockerId: null };
      launchIntentId = `launch-${prefix}-${stage.toLowerCase()}`; state.launchIntents[launchIntentId] = { launchIntentId, effectId, stageAttemptId: attemptId, state: "closed", adapter: "owned-worker-v1", launchKey: `launch-key-${prefix}-${stage.toLowerCase()}`, workerId: workerCore.workerId, expectedAttemptNumber: 1, taskPacketHash: H("4"), cwdRepositoryId: "repo-main", configRequestHash: H("5"), dispatchCount: 1, lastDispatchAt: NOW, boundAt: NOW, ambiguityReason: null };
      state.workerBindings[attemptId] = { stageAttemptId: attemptId, launchIntentId, workerStorageId: workerCore.workerStorageId, launchOwnerSessionId: workerCore.launchOwnerSessionId, workerId: workerCore.workerId, attemptNumber: 1, attemptNonce: workerCore.attemptNonce, configHash: workerCore.configHash, configRef: ref("verification", `config-${stage.toLowerCase()}`, workerCore.configHash), supervisorPid: 1000 + PLAN_STAGE_IDS.indexOf(stage), supervisorStartIdentity: `proc:${stage}:supervisor`, childPid: 2000 + PLAN_STAGE_IDS.indexOf(stage), childStartIdentity: `proc:${stage}:child`, mailboxHash: H("6"), heartbeatAt: NOW, completionId: workerCore.completionId, resultHash: workerFact.hash, processDisposition: "dead", retrySafe: true };
    }
    const procedure = Object.values(procedures).find((value) => value.stages.includes(stage));
    const assertions = [];
    if (stage === "F2") {
      const assertion = fixturePlan.acceptanceOracles[0].assertions[0]; const oracleCore = { kind: "oracle_assertion", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage: "F2", oracleId: fixturePlan.acceptanceOracles[0].oracleId, assertionId: assertion.assertionId, procedureId: assertion.procedureId, environmentProfileId: assertion.environmentProfileId, observationMethod: assertion.observationMethod, requiredEvidenceClass: assertion.requiredEvidenceClass, disposition: "PASS", observationHash: workerResultHash };
      const oracleFact = { ...oracleCore, hash: canonicalHash(oracleCore) }; facts[oracleFact.hash] = oracleFact; state.evidenceIndex.oracleAssertions[oracleFact.hash] = ref("oracle_assertion", "oracle-assert-integration", oracleFact.hash); assertions.push({ oracleId: oracleCore.oracleId, assertionId: oracleCore.assertionId, evidenceHash: oracleFact.hash });
    }
    const aggregateCore = { workItemId: "item-api", stage, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: "PASS", oracleIds: state.workItems["item-api"].planEntityHash ? fixturePlan.workItems[0].oracleIds : [], assertions, checks: stage === "F2" ? [{ checkId: "check-api", disposition: "PASS", applicabilityEvidenceHashes: [] }] : [] };
    const aggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) }; checkAggregates[aggregate.hash] = aggregate;
    const evidenceCore = { kind: "stage_evidence", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, authorizationSetHash: state.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: aggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration: stage === "F0" ? 0 : generation, candidateHash: stage === "F0" ? null : candidateFact.hash, producerKind, producerResultHash: workerResultHash, disposition: "PASS", freshIndependent: ["F2", "F5"].includes(stage), readOnly: procedure.readOnly, cleanEnvironment: stage === "F7" };
    const evidenceFact = { ...evidenceCore, hash: canonicalHash(evidenceCore) }; facts[evidenceFact.hash] = evidenceFact; const evidenceRef = ref("stage_evidence", `evidence-${stage.toLowerCase()}`, evidenceFact.hash); state.evidenceIndex.stageEvidence[evidenceFact.hash] = evidenceRef;
    state.stageAttempts[attemptId] = { stageAttemptId: attemptId, workItemId: "item-api", stage, ordinal: 1, producerKind, implementationLineageHash, inputGeneration, reservedOutputGeneration: stage === "F1" ? generation : null, attemptInput: inputRef, authorizationSetHash: state.identity.authorizationSet.hash, state: "sealed", launchIntentId, leaseIds: [], workerResult: workerResultRef, evidence: evidenceRef, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: NOW };
    state.workItems["item-api"].stages[stage] = { stage, state: "passed", attemptIds: [attemptId], currentAttemptId: attemptId, currentEvidence: evidenceFact.hash, adoptionReceipt: null, invalidationIds: [], lastDisposition: "PASS", blockerIds: [] };
  }
  const f8EvidenceHash = state.workItems["item-api"].stages.F8.currentEvidence; const readyCore = { kind: "integration_ready", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", candidateGeneration: generation, candidateHash: candidateFact.hash, f8EvidenceHash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true };
  const readyFact = { ...readyCore, hash: canonicalHash(readyCore) }; facts[readyFact.hash] = readyFact; state.evidenceIndex.integrationReady["item-api"] = ref("integration_ready", "item-api", readyFact.hash);
  const item = state.workItems["item-api"]; item.current = "integration_ready"; item.currentStage = "F8"; item.integrationReadyReceipt = readyFact.hash; item.laneAdmissionSequence = 1; item.admittedAt = NOW;
  state.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null }; state.scheduler.nextReservationSequence = 2;
  state.current.run = "integration"; state.current.activeWorkItemIds = ["item-api"]; state.current.readyWorkItemIds = []; state.current.integrationReadyWorkItemIds = ["item-api"];
  if (!preserveTrain) { state.integrationTrains["repo-main"].entryOrder = []; state.integrationTrains["repo-main"].entries = {}; state.integrationTrains["repo-main"].acceptedPrefixOrdinal = 0; }
  rehashRun(state);
  const context = { ...baseContext, catalog: { ...baseContext.catalog, procedures, checkAggregates }, facts };
  return { state, context };
}

const integrationReadyFixture = buildIntegrationReadyFixture(plan, attachedResult.state, ownerContext);
assert.equal(validateDagRunStateV1(integrationReadyFixture.state, integrationReadyFixture.context).ok, true, `full integration-ready fixture is valid: ${JSON.stringify(validateDagRunStateV1(integrationReadyFixture.state, integrationReadyFixture.context).issues)}`);
let integrationState = integrationReadyFixture.state;
let integrationContext = integrationReadyFixture.context;
const gitFact = (core) => { const bound = core.kind === "git_transaction" ? { authorizationSetHash: core.authorizationSetHash ?? integrationState.identity.authorizationSet.hash, ownerEpoch: core.ownerEpoch ?? integrationState.owner.ownerEpoch, worktreeIdentityHash: core.worktreeIdentityHash ?? H("3"), gitConfigHash: core.gitConfigHash ?? H("4"), gitVersionHash: core.gitVersionHash ?? canonicalHash("git version 2.test"), objectFormat: core.objectFormat ?? "sha1", ...core } : core; return { ...bound, hash: canonicalHash(bound) }; };
const repositoryBinding = gitFact({ kind: "git_transaction", factType: "repository_binding", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: null, effectId: null, requestHash: H("1"), commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: O("a"), tree: O("b"), parentCommit: null, reconciliation: "applied_exact", detailsHash: H("3"), observedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [repositoryBinding.hash]: repositoryBinding } };
const compositionEffect = { effectId: "integration-001-compose", kind: "compose_candidate", subject: { kind: "train", id: "train-main" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: H("4"), boundOwnerEpoch: integrationState.owner.ownerEpoch, boundAuthorizationSetHash: integrationState.identity.authorizationSet.hash, boundFreshnessReceiptHash: integrationState.freshness.receipt.hash, boundCandidateGeneration: 1, boundGateEpochHash: H("5"), state: "intended", dispatchCount: 0, createdRevision: integrationState.revision + 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
const reserveIntegrationPayload = { integrationAttemptId: "integration-001", entryId: "entry-000-item-api", repositoryId: "repo-main", workItemId: "item-api", retryOrdinal: 0, retryAuthorizationKey: null, sourceCandidateHash: integrationState.workItems["item-api"].candidate.candidateHash, sourceBase: integrationState.workItems["item-api"].candidate.base, sourceCandidate: integrationState.workItems["item-api"].candidate.git, expectedPrefix: integrationState.integrationTrains["repo-main"].acceptedPrefix, expectedTarget: integrationState.integrationTrains["repo-main"].expectedTarget, temporaryRef: "refs/pi-dag/v1/objects/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/composed", repositoryBindingFactHash: repositoryBinding.hash, lockLeaseId: "lease-integration-001", compositionEffect };
let conflictState = integrationState;
let conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "reserve_integration_attempt", { ...reserveIntegrationPayload, integrationAttemptId: "integration-conflict", lockLeaseId: "lease-integration-conflict", compositionEffect: { ...compositionEffect, effectId: "integration-conflict-compose" } }, { commandId: "command-conflict-reserve", idempotencyKey: "conflict-reserve" }), integrationContext); assert.equal(conflictTransition.accepted, true); conflictState = conflictTransition.state;
conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "mark_effect_dispatching", { effectId: "integration-conflict-compose", expectedDispatchCount: 0 }, { commandId: "command-conflict-dispatch", idempotencyKey: "conflict-dispatch" }), integrationContext); assert.equal(conflictTransition.accepted, true); conflictState = conflictTransition.state;
const conflictFact = gitFact({ kind: "git_transaction", factType: "composition", planHash: plan.planHash, runId: conflictState.runId, runNonce: conflictState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-conflict", effectId: "integration-conflict-compose", requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: null, commit: null, tree: null, parentCommit: O("a"), reconciliation: "conflict", detailsHash: H("0"), observedAt: NOW });
const conflictContext = { ...integrationContext, facts: { ...integrationContext.facts, [conflictFact.hash]: conflictFact } };
conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "record_git_composition_conflict", { integrationAttemptId: "integration-conflict", compositionFactHash: conflictFact.hash, conflictClass: "mechanical" }, { kind: "observation", commandId: "command-conflict-observe", idempotencyKey: "conflict-observe" }), conflictContext);
assert.equal(conflictTransition.accepted, true, "exact composition conflict releases only the integration lock while retaining the sticky lane");
assert.equal(conflictTransition.accepted && conflictTransition.state.repositories["repo-main"].integrationLockLeaseId, null); assert.equal(conflictTransition.accepted && conflictTransition.state.workItems["item-api"].current, "active"); assert.equal(conflictTransition.accepted && conflictTransition.state.workItems["item-api"].currentStage, "F1"); assert.equal(conflictTransition.accepted && conflictTransition.state.workItems["item-api"].candidateGeneration, 2); assert.equal(conflictTransition.accepted && conflictTransition.state.workItems["item-api"].candidate, null); assert.equal(conflictTransition.accepted && conflictTransition.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, null);
if (conflictTransition.accepted) {
  const unauthorizedEffect = { ...compositionEffect, effectId: "integration-unauthorized-compose", boundCandidateGeneration: 2, createdRevision: conflictTransition.state.revision + 1 }; const unauthorized = reduceDagRunV1(conflictTransition.state, reducerInput(conflictTransition.state, "reserve_integration_attempt", { ...reserveIntegrationPayload, integrationAttemptId: "integration-unauthorized", retryOrdinal: 1, sourceCandidateHash: H("f"), retryAuthorizationKey: null, lockLeaseId: "lease-integration-unauthorized", compositionEffect: unauthorizedEffect }, { commandId: "command-conflict-unauthorized", idempotencyKey: "conflict-unauthorized" }), conflictContext); assert.equal(unauthorized.accepted, false, "a terminal composition conflict cannot bypass fresh-candidate and retry authorization");
  const retryKey = H("7"); const retrySeed = clone(conflictTransition.state); retrySeed.retryLedger[retryKey] = { retryKey, workItemId: "item-api", stage: "F1", dimension: "integration", procedureId: null, failureClass: "integration", fingerprint: H("8"), count: 0, ceiling: 2, authorizationSetHash: retrySeed.identity.authorizationSet.hash, candidateTrees: [], repairCommitTrees: [], progressHashes: [], failureSequence: [], stop: "none", lastRetryCommandId: null }; rehashRun(retrySeed);
  const repaired = buildIntegrationReadyFixture(plan, retrySeed, conflictContext, { generation: 2, prefix: "repair2", startStageOrdinal: 1, preserveTrain: true }); assert.equal(validateDagRunStateV1(repaired.state, repaired.context).ok, true, `fresh post-conflict F1-F8 state is valid: ${JSON.stringify(validateDagRunStateV1(repaired.state, repaired.context).issues)}`);
  const authorized = reduceDagRunV1(repaired.state, reducerInput(repaired.state, "authorize_retry", { retryKey, expectedCount: 0, workItemId: "item-api", stage: "F1", dimension: "integration", fingerprint: H("8"), candidateGeneration: 2 }, { commandId: "command-retry-authorize", idempotencyKey: "retry-authorize" }), repaired.context); assert.equal(authorized.accepted, true, "post-conflict integration retry consumes one exact authorization count");
  if (authorized.accepted) { const retryEffect = { ...compositionEffect, effectId: "integration-retry-compose", requestHash: H("a"), boundCandidateGeneration: 2, createdRevision: authorized.state.revision + 1 }; const retryPayload = { ...reserveIntegrationPayload, integrationAttemptId: "integration-retry", retryOrdinal: 1, retryAuthorizationKey: retryKey, sourceCandidateHash: authorized.state.workItems["item-api"].candidate.candidateHash, sourceBase: authorized.state.workItems["item-api"].candidate.base, sourceCandidate: authorized.state.workItems["item-api"].candidate.git, lockLeaseId: "lease-integration-retry", compositionEffect: retryEffect }; let retried = reduceDagRunV1(authorized.state, reducerInput(authorized.state, "reserve_integration_attempt", retryPayload, { commandId: "command-conflict-retry", idempotencyKey: "conflict-retry" }), repaired.context); assert.equal(retried.accepted, true, "fresh sealed F1 candidate and exact one-shot authorization permit integration retry");
    if (retried.accepted) { retried = reduceDagRunV1(retried.state, reducerInput(retried.state, "mark_effect_dispatching", { effectId: retryEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-retry-dispatch", idempotencyKey: "retry-dispatch" }), repaired.context); assert.equal(retried.accepted, true); if (retried.accepted) { const retryConflictFact = gitFact({ kind: "git_transaction", factType: "composition", planHash: plan.planHash, runId: retried.state.runId, runNonce: retried.state.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-retry", effectId: retryEffect.effectId, requestHash: retryEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: null, commit: null, tree: null, parentCommit: O("a"), reconciliation: "conflict", detailsHash: H("9"), observedAt: NOW }); const retryConflictContext = { ...repaired.context, facts: { ...repaired.context.facts, [retryConflictFact.hash]: retryConflictFact } }; const conflictedAgain = reduceDagRunV1(retried.state, reducerInput(retried.state, "record_git_composition_conflict", { integrationAttemptId: "integration-retry", compositionFactHash: retryConflictFact.hash, conflictClass: "mechanical" }, { kind: "observation", commandId: "command-retry-conflict", idempotencyKey: "retry-conflict" }), retryConflictContext); assert.equal(conflictedAgain.accepted, true); if (conflictedAgain.accepted) { const repairedAgain = buildIntegrationReadyFixture(plan, conflictedAgain.state, retryConflictContext, { generation: 3, prefix: "repair3", startStageOrdinal: 1, preserveTrain: true }); const reuseEffect = { ...compositionEffect, effectId: "integration-reuse-compose", boundCandidateGeneration: 3, createdRevision: repairedAgain.state.revision + 1 }; const reuse = reduceDagRunV1(repairedAgain.state, reducerInput(repairedAgain.state, "reserve_integration_attempt", { ...retryPayload, integrationAttemptId: "integration-reuse", sourceCandidateHash: repairedAgain.state.workItems["item-api"].candidate.candidateHash, sourceBase: repairedAgain.state.workItems["item-api"].candidate.base, sourceCandidate: repairedAgain.state.workItems["item-api"].candidate.git, lockLeaseId: "lease-integration-reuse", compositionEffect: reuseEffect }, { commandId: "command-retry-reuse", idempotencyKey: "retry-reuse" }), repairedAgain.context); assert.equal(reuse.accepted, false, "one integration retry authorization ordinal cannot be reused after another conflict"); } } }
  }
}
let transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "reserve_integration_attempt", reserveIntegrationPayload, { commandId: "command-integration-reserve", idempotencyKey: "integration-reserve" }), integrationContext);
assert.equal(transition.accepted, true, "integration head reservation atomically binds lock, entry, attempt, and composition intent"); integrationState = transition.state;
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "mark_effect_dispatching", { effectId: compositionEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-compose-dispatch", idempotencyKey: "compose-dispatch" }), integrationContext);
assert.equal(transition.accepted, true, "composition dispatch requires persisted effect intent"); integrationState = transition.state;
const composed = { repositoryId: "repo-main", commit: O("e"), tree: O("f") };
const compositionFact = gitFact({ kind: "git_transaction", factType: "composition", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: compositionEffect.effectId, requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: null, commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: H("6"), observedAt: NOW });
const privateRefs = Object.fromEntries(["baseline", "candidate", "prefix", "composed", "proposal"].map((role) => [role, `refs/pi-dag/v1/transactions/test/${role}`]));
const privateRefFacts = Object.entries(privateRefs).map(([role, targetRef]) => gitFact({ kind: "git_transaction", factType: "private_ref", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: compositionEffect.effectId, requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef, commit: ["baseline", "prefix"].includes(role) ? O("a") : role === "candidate" ? O("c") : composed.commit, tree: ["baseline", "prefix"].includes(role) ? O("b") : role === "candidate" ? O("d") : composed.tree, parentCommit: ["composed", "proposal"].includes(role) ? O("a") : null, reconciliation: "applied_exact", detailsHash: canonicalHash(role), observedAt: NOW }));
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [compositionFact.hash]: compositionFact, ...Object.fromEntries(privateRefFacts.map((fact) => [fact.hash, fact])) } };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_composition", { integrationAttemptId: "integration-001", compositionFactHash: compositionFact.hash, composedTree: composed, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), conflictClass: "none", privateRefFactHashes: privateRefFacts.map(({ hash }) => hash) }, { kind: "observation", commandId: "command-compose-observe", idempotencyKey: "compose-observe" }), integrationContext);
assert.equal(transition.accepted, true, "exact composition observation advances only the current integration attempt"); integrationState = transition.state;
const prefixVerification = gitFact({ kind: "verification", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", trainId: "train-main", integrationAttemptId: "integration-001", phase: "prefix", profileId: "checks-prefix", profileHash: H("e"), tree: composed, disposition: "PASS" });
const finalVerification = gitFact({ kind: "verification", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", trainId: "train-main", integrationAttemptId: "integration-001", phase: "final", profileId: "checks-final", profileHash: H("f"), tree: composed, disposition: "PASS" });
const environmentClosureHash = H("6"); const proposalVerificationDetailsHash = canonicalHash({ prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], environmentClosureHash });
const proposalVerification = gitFact({ kind: "git_transaction", factType: "proposal_verification", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: compositionEffect.effectId, requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: proposalVerificationDetailsHash, observedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [prefixVerification.hash]: prefixVerification, [finalVerification.hash]: finalVerification, [proposalVerification.hash]: proposalVerification } };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_proposal_verification", { integrationAttemptId: "integration-001", proposalVerificationFactHash: proposalVerification.hash, prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], environmentClosureHash }, { kind: "observation", commandId: "command-proposal-verify", idempotencyKey: "proposal-verify" }), integrationContext);
assert.equal(transition.accepted, true, "prefix and final verification bind exact future composed state"); integrationState = transition.state;
const landingEffect = { ...compositionEffect, effectId: "integration-001-land", kind: "land_target", subject: { kind: "repository", id: "repo-main" }, requestHash: H("a"), createdRevision: integrationState.revision + 1 };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "prepare_git_landing", { integrationAttemptId: "integration-001", landingEffect, intendedLandedTree: composed }, { commandId: "command-landing-prepare", idempotencyKey: "landing-prepare" }), integrationContext);
assert.equal(transition.accepted, true, "landing intent is durable only after exact proposal verification"); integrationState = transition.state;
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "mark_effect_dispatching", { effectId: landingEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-landing-dispatch", idempotencyKey: "landing-dispatch" }), integrationContext);
assert.equal(transition.accepted, true, "landing dispatch is separately guarded after intent"); integrationState = transition.state;
const thirdTargetFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: O("9"), tree: O("8"), parentCommit: O("a"), reconciliation: "conflict", detailsHash: H("7"), observedAt: NOW });
const thirdTargetContext = { ...integrationContext, facts: { ...integrationContext.facts, [thirdTargetFact.hash]: thirdTargetFact } };
const thirdTarget = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: thirdTargetFact.hash, reconciliation: "conflict" }, { kind: "observation", commandId: "command-landing-third", idempotencyKey: "landing-third" }), thirdTargetContext);
assert.equal(thirdTarget.accepted, true, "exact third-target observation blocks without overwrite and releases the repository lock"); assert.equal(thirdTarget.accepted && thirdTarget.state.workItems["item-api"].current, "integration_ready"); assert.equal(thirdTarget.accepted && thirdTarget.state.repositories["repo-main"].integrationLockLeaseId, null); assert.equal(thirdTarget.accepted && thirdTarget.state.blockers["integration-target-third-integration-001"].release, "successor_plan");
const targetObservationHash = canonicalHash({ targetRef: "refs/heads/main", observed: { commit: composed.commit, tree: composed.tree }, expectedOld: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, intendedNew: composed });
const landingFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: targetObservationHash, observedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [landingFact.hash]: landingFact } };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: landingFact.hash, reconciliation: "applied_exact" }, { kind: "observation", commandId: "command-landing-observe", idempotencyKey: "landing-observe" }), integrationContext);
assert.equal(transition.accepted, true, "target new observation reconciles exact landing without claiming completion"); integrationState = transition.state;
const transactionReceiptCore = { schemaVersion: 1, kind: "IntegrationReceiptV1", transactionId: "integration-001", runId: integrationState.runId, runNonce: integrationState.runNonce, planHash: plan.planHash, authorizationSetHash: integrationState.identity.authorizationSet.hash, ownerEpoch: integrationState.owner.ownerEpoch, repositoryId: "repo-main", commonDirIdentityHash: H("2"), worktreeIdentityHash: H("3"), gitVersion: "git version 2.test", configHash: H("4"), objectFormat: "sha1", targetRef: "refs/heads/main", sourceBase: integrationState.workItems["item-api"].candidate.base, candidate: integrationState.workItems["item-api"].candidate.git, expectedPrefix: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, composed, workItemId: "item-api", candidateGeneration: 1, compositionProfileHash: H("d"), prefixValidationProfileHash: H("e"), finalValidationProfileHash: H("f"), prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], environmentClosureHash, privateRefs, landing: { expectedOldOid: O("a"), newOid: composed.commit, reconciliation: "applied_exact", targetObservationHash }, sealedAt: NOW };
const transactionReceipt = { ...transactionReceiptCore, receiptHash: canonicalHash(transactionReceiptCore) }; const transactionReceiptHash = transactionReceipt.receiptHash;
const transactionReceiptFactCore = { kind: "git_integration_receipt", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, authorizationSetHash: integrationState.identity.authorizationSet.hash, repositoryId: "repo-main", integrationAttemptId: "integration-001", transactionReceiptHash, receipt: transactionReceipt }; const transactionReceiptFact = { ...transactionReceiptFactCore, hash: canonicalHash(transactionReceiptFactCore) }; const transactionReceiptFactHash = transactionReceiptFact.hash;
const integrationReceipt = gitFact({ kind: "integration", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, authorizationSetHash: integrationState.identity.authorizationSet.hash, workItemId: "item-api", repositoryId: "repo-main", integrationAttemptId: "integration-001", candidateHash: integrationState.workItems["item-api"].candidate.candidateHash, strategy: "merge_tree_one_parent", compositionProfileHash: H("d"), expectedPrefix: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, expectedTarget: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], environmentClosureHash, sourceBase: integrationState.workItems["item-api"].candidate.base, sourceCandidate: integrationState.workItems["item-api"].candidate.git, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), landed: composed, combinedStateVerified: true, reconciled: true, acceptingOwnerEpoch: integrationState.owner.ownerEpoch, commonDirIdentityHash: H("2"), worktreeIdentityHash: H("3"), gitConfigHash: H("4"), gitVersionHash: canonicalHash("git version 2.test"), objectFormat: "sha1", transactionReceiptHash, transactionReceiptFactHash, landingObservationHash: landingFact.hash });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [transactionReceiptFact.hash]: transactionReceiptFact, [integrationReceipt.hash]: integrationReceipt } };
const inventedReceipt = reduceDagRunV1(integrationState, reducerInput(integrationState, "accept_integration_receipt", { integrationAttemptId: "integration-001", integrationReceiptHash: integrationReceipt.hash, transactionReceiptHash: H("c"), transactionReceiptFactHash: H("d") }, { kind: "observation", commandId: "command-integration-invented", idempotencyKey: "integration-invented" }), integrationContext); assert.equal(inventedReceipt.accepted, false, "an invented transaction receipt hash cannot become canonical integration authority");
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "accept_integration_receipt", { integrationAttemptId: "integration-001", integrationReceiptHash: integrationReceipt.hash, transactionReceiptHash, transactionReceiptFactHash }, { kind: "observation", commandId: "command-integration-accept", idempotencyKey: "integration-accept" }), integrationContext);
assert.equal(transition.accepted, true, "only exact immutable transaction and integration receipts atomically mark completion");
assert.equal(transition.accepted && transition.state.workItems["item-api"].current, "complete");
assert.equal(transition.accepted && transition.state.integrationTrains["repo-main"].acceptedPrefix.commit, composed.commit);
assert.equal(transition.accepted && transition.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, "integrated", "receipt acceptance releases sticky lane only after exact landing");

const conductorRoot = await mkdtemp(join(tmpdir(), "pi-dag-conductor-v1-"));
try {
  await execFileAsync("git", ["init", "-b", "main"], { cwd: conductorRoot });
  const artifactsDir = join(conductorRoot, ".ai", "start-artifacts"); await mkdir(artifactsDir, { recursive: true });
  const conductorGenesis = clone(run);
  const conductorIndex = buildSchedulerPlanIndexV1(plan);
  conductorGenesis.scheduler.policyHash = DAG_SCHEDULER_POLICY_HASH_V1; conductorGenesis.scheduler.normalizedIndexHash = conductorIndex.indexHash;
  conductorGenesis.workItems["item-api"].current = "ready"; conductorGenesis.current.run = "active"; conductorGenesis.current.readyWorkItemIds = ["item-api"];
  rehashRun(conductorGenesis);
  const conductorContext = { ...runContext, plan, normalizedSchedulerIndexHash: conductorIndex.indexHash, facts: {}, seedFacts: baselineFactValues };
  await writeFile(join(artifactsDir, "plan.json"), canonicalStringify(plan));
  await writeFile(join(artifactsDir, "genesis.json"), canonicalStringify(conductorGenesis));
  await writeFile(join(artifactsDir, "context.json"), canonicalStringify(conductorContext));
  let exactDispatches = 0; const conductor = new DagConductorServiceV1({ async dispatchEffect(request, state) { requireSchedulerDispatchIntentV1(state, request); exactDispatches += 1; } });
  const conductorCtx = { cwd: conductorRoot, sessionManager: { getSessionId: () => "session-conductor", getSessionFile: () => null, getHeader: () => ({ type: "session", id: "session-conductor", cwd: conductorRoot }) } };
  const started = await conductor.start(conductorCtx, { runId: conductorGenesis.runId, runNonce: conductorGenesis.runNonce, planHash: plan.planHash, planPath: ".ai/start-artifacts/plan.json", genesisPath: ".ai/start-artifacts/genesis.json", contextPath: ".ai/start-artifacts/context.json", maxActiveNodes: 1, occurredAt: NOW });
  assert.equal(started.state.owner.sessionId, "session-conductor", "dag_run_start attaches one exact current session owner"); assert.equal(Object.values(started.state.scheduler.reservations)[0]?.state, "active", "dag_run_start durably reserves, dispatches, and observes the first deterministic scheduler operation"); assert.equal(exactDispatches, 1, "the production scheduler adapter accepts exactly the committed dispatch_intent identity before active observation");
  assert.equal((await conductor.binding(conductorCtx)).runId, conductorGenesis.runId, "session binding names one exact run instead of inferring latest");
  const conductorStatus = await conductor.status(conductorCtx, conductorGenesis.runId);
  assert.equal(conductorStatus.projection.runSnapshotHash, conductorStatus.state.snapshotHash, "conductor status uses a stable exact projection join"); assert.equal(conductorStatus.projection.nodes[0].glyph, "*", "an exact active scheduler reservation renders as genuinely in-flight");
  const pauseGuard = { runId: conductorStatus.state.runId, runNonce: conductorStatus.state.runNonce, expectedRevision: conductorStatus.state.revision, expectedSnapshotHash: conductorStatus.state.snapshotHash, ownerEpoch: conductorStatus.state.owner.ownerEpoch, commandId: "command-conductor-pause", idempotencyKey: "conductor-pause", occurredAt: NOW };
  const conductorPaused = await conductor.control(conductorCtx, pauseGuard, "pause", "test pause");
  assert.equal(conductorPaused.current.run, "paused", "conductor control compiles pause through guarded reducer CAS");
  const resumeGuard = { ...pauseGuard, expectedRevision: conductorPaused.revision, expectedSnapshotHash: conductorPaused.snapshotHash, ownerEpoch: conductorPaused.owner.ownerEpoch, commandId: "command-conductor-resume", idempotencyKey: "conductor-resume" };
  const conductorResumed = await conductor.control(conductorCtx, resumeGuard, "resume", "test resume");
  assert.equal(conductorResumed.current.run, "active", "conductor control resumes only the exact paused revision");
  await assert.rejects(() => conductor.control(conductorCtx, pauseGuard, "pause", "stale pause"), /guard is stale/, "stale conductor mutation guards fail closed");
} finally { await rm(conductorRoot, { recursive: true, force: true }); }

const conductorPi = { tools: [], handlers: new Map(), registerTool(tool) { this.tools.push(tool); }, on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); } };
registerCanonicalDagRuntime(conductorPi, { async binding() { return null; } });
assert.deepEqual(conductorPi.tools.map(({ name }) => name).sort(), ["dag_run_control", "dag_run_diagram", "dag_run_explain", "dag_run_inspect", "dag_run_reattach", "dag_run_retry", "dag_run_start", "dag_run_status", "dag_run_tail"], "conductor exposes exactly five read-only and four guarded mutation tools");
assert(conductorPi.tools.every(({ parameters }) => parameters.additionalProperties === false), "all nine conductor tools reject unknown fields");
for (const handler of conductorPi.handlers.get("session_start") ?? []) await handler({}, { hasUI: false, cwd: "/tmp", sessionManager: { getSessionId: () => "headless" }, ui: {} });
const tuiCalls = []; const tuiContext = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "tui-session" }, ui: { setWidget(id, value) { tuiCalls.push({ id, value }); } } }; for (const handler of conductorPi.handlers.get("session_start") ?? []) { await handler({}, tuiContext); await handler({}, tuiContext); } assert.equal(tuiCalls.filter(({ value }) => typeof value === "function").length, 2, "unbound TUI sessions install the passive widget and repeated session starts replace it deterministically"); for (const handler of conductorPi.handlers.get("session_start") ?? []) await handler({}, { hasUI: false, mode: "rpc", cwd: "/tmp", sessionManager: { getSessionId: () => "headless-after-tui" }, ui: {} }); assert.equal(tuiCalls.at(-1).value, undefined, "a subsequent headless session start removes the prior TUI widget before returning"); for (const handler of conductorPi.handlers.get("session_shutdown") ?? []) await handler(); assert.equal(tuiCalls.at(-1).value, undefined, "session shutdown tears down the installed widget after clearing the current timer");
const failClosedPi = { tools: [], handlers: new Map(), registerTool(tool) { this.tools.push(tool); }, on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); } }; let statusReads = 0; registerCanonicalDagRuntime(failClosedPi, { async binding() { return { runId: executionProjection.runId }; }, async advance() {}, async status() { statusReads += 1; if (statusReads > 1) throw new Error("identity mismatch/corrupt projection"); return { projection: executionProjection, stale: null, state: { completion: { state: "open" }, current: { run: "active" } } }; } }); let widgetFactory = null; const failClosedContext = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "fail-closed" }, ui: { setWidget(_id, value) { if (typeof value === "function") widgetFactory = value; } } }; for (const handler of failClosedPi.handlers.get("session_start") ?? []) await handler({}, failClosedContext); const failClosedComponent = widgetFactory({ terminal: { rows: 24 }, requestRender() {} }); assert(failClosedComponent.render(120).some((line) => line.startsWith("DAG ")), "widget renders a valid exact projection before a read error"); for (const handler of failClosedPi.handlers.get("agent_end") ?? []) await handler(); const failedLines = failClosedComponent.render(120); assert(failedLines.some((line) => line.includes("FAIL-CLOSED")) && !failedLines.some((line) => line.includes(" | lanes ") || line.startsWith("read-only;")), "corruption and identity errors suppress the prior graph instead of mixing it with diagnostics"); for (const handler of failClosedPi.handlers.get("session_shutdown") ?? []) await handler();

console.log("Canonical DAG plan and run-state schema tests OK; scheduler, projection, conductor, tools, and widget tests OK");
