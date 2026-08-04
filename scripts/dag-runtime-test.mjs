import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_RUN_INPUT_SCHEMA_HASH,
  DAG_RUN_STATE_SCHEMA_HASH,
  DagRunSnapshotStoreV1,
  DagRunStoreCorruptError,
  DagRunStoreLockedError,
  PLAN_STAGE_IDS,
  canonicalHash,
  canonicalStringify,
  createDagRunStoreDeadOwnerProofV1,
  parseCanonicalDagPlanV1,
  parseDagRunStateV1,
  parseStrictJson,
  sealCanonicalDagPlanV1,
  reduceDagRunV1,
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
    lifecycleBinding: { profileId: "lifecycle-v1", profileHash: lifecycleProfileHash, checkCatalogHash, retryPolicyHash: H("b"), schedulerPolicyVersion: "sticky-lanes-v1", schedulerPolicyHash: H("5"), stages: [...PLAN_STAGE_IDS] },
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
    scheduler: { policyVersion: "sticky-lanes-v1", policyHash: H("5"), normalizedIndexHash: H("6"), maxActiveNodes: 1, decisionSequence: 0, nextReservationSequence: 1, lastDecisionCommandId: null, activeNodeLanes: {}, reservations: {}, bypassCounters: {}, fairnessCounters: {}, dynamicExclusions: {}, providerHoldIds: [] },
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
activeCancellationRun.scheduler.reservations["reservation-f1"] = { reservationId: "reservation-f1", reservationSequence: 1, workItemId: "item-api", stage: "F1", attemptOrdinal: 1, operationKind: "implementation", state: "active", candidateGeneration: 0, ownerEpoch: 0, authorizationSetHash: run.identity.authorizationSet.hash, normalizedRequestHash: H("1"), leaseIds: [], mutexGroupIds: [], resourceUnits: {}, workerRole: "implementation", repositoryId: "repo-main", createdAt: NOW, releasedAt: null };
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
activeCancellationRun.workItems["item-api"].activeLeaseIds = ["lease-f1"];
activeCancellationRun.workItems["item-api"].stages.F1 = { stage: "F1", state: "active", attemptIds: [f1AttemptId], currentAttemptId: f1AttemptId, currentEvidence: null, adoptionReceipt: null, invalidationIds: [], lastDisposition: null, blockerIds: [] };
activeCancellationRun.scheduler.reservations["reservation-f1"].leaseIds = ["lease-f1"];
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

console.log("Canonical DAG plan and run-state schema tests OK");
