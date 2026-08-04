import assert from "node:assert/strict";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_RUN_STATE_SCHEMA_HASH,
  PLAN_STAGE_IDS,
  canonicalHash,
  parseCanonicalDagPlanV1,
  parseDagRunStateV1,
  parseStrictJson,
  sealCanonicalDagPlanV1,
  sealDagRunStateV1,
  validateCanonicalDagPlanV1,
  validateDagRunStateV1,
} from "../extensions/dag-workflow/dag-runtime/index.ts";

const H = (char) => `sha256:${char.repeat(64)}`;
const O = (char) => char.repeat(40);
const NOW = "2026-08-04T15:00:00.000Z";
const content = (value) => ({ ...value, contentHash: canonicalHash(value) });
const recontent = (value) => {
  const copy = { ...value };
  delete copy.contentHash;
  return content(copy);
};
const rehashPlan = (value) => { value.planHash = canonicalHash(Object.fromEntries(Object.entries(value).filter(([key]) => key !== "planHash"))); return value; };
const rehashRun = (value) => { value.snapshotHash = canonicalHash(Object.fromEntries(Object.entries(value).filter(([key]) => key !== "snapshotHash"))); return value; };
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
    targetRef: "refs/heads/main", observedTarget: plan.repositories[0].baseline, observedTargetAt: NOW, observationReceipt: H("d"),
    workspace: { state: "unmaterialized", locator: null, gitCommonDirIdentityHash: null, gitWorktreeIdentityHash: null, branchRef: null, base: null, expectedHead: null, ownerLeaseId: null, processDisposition: "not_applicable", observationReceipt: null },
    integrationLockLeaseId: null, blockerIds: [],
  };
  const item = {
    workItemId: "item-api", planEntityHash: plan.workItems[0].contentHash, writeRepositoryId: "repo-main", desired: "run", current: "pending",
    authorizedStages: [...PLAN_STAGE_IDS], currentStage: null, implementationLineageHash: H("7"), candidateGeneration: 0, candidate: null, stages: emptyStages,
    precedenceIds: [], gateIds: [], laneAdmissionSequence: null, admittedAt: null, activeLeaseIds: [], blockerIds: [], openFindingIds: [], integrationReadyReceipt: null,
    integrationEntryId: null, integrationReceipt: null, completedAt: null,
  };
  const review = ref("plan_review", "review-plan", H("e"));
  const authorization = ref("plan_authorization", "authorization-plan", H("f"));
  const freshness = ref("staleness", "freshness", H("1"));
  const authorizationContext = authorizationBinding(plan, review.hash, [authorization.hash], null);
  const authorizationSet = ref("authorization_set", "authorization-set", authorizationContext.hash);
  return sealDagRunStateV1({
    schemaVersion: 1, kind: "DagRunStateV1", canonicalization: "jcs-v1", runId: "run-api", runNonce: "0123456789abcdef",
    revision: 0, previousSnapshotHash: null, createdAt: NOW, updatedAt: NOW,
    identity: { projectId: "project", planId: plan.planId, planRevision: plan.revision, planHash: plan.planHash, planSchemaHash: CANONICAL_DAG_PLAN_SCHEMA_HASH, lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, artifactPolicyHash: plan.artifactPolicy.profileHash, reviewReceipt: review, authorizationReceipts: [authorization], authorizationSet, previousRunId: null, supersededByRunId: null },
    owner: { ownerEpoch: 0, ownerTokenHash: null, sessionId: null, pid: 0, processStartIdentity: null, lockIdentity: null, attachedAt: null, lastHeartbeatAt: null, ownershipReceipt: null },
    desired: { run: "running", reason: null, requestedAt: NOW, requestedBy: "user" },
    current: { run: "initializing", readyWorkItemIds: [], activeWorkItemIds: [], blockedWorkItemIds: [], integrationReadyWorkItemIds: [], updatedByCommandId: "create-run" },
    repositories: { "repo-main": repository }, workItems: { "item-api": item }, gates: {}, precedence: {}, resourcePools: {}, mutexes: {}, leases: {},
    stageAttempts: {}, launchIntents: {}, workerBindings: {},
    evidenceIndex: { stageAttemptInputs: {}, workerResults: {}, candidates: {}, stageEvidence: {}, checkDispositions: {}, verifications: {}, oracleAssertions: {}, findings: {}, findingResolutions: {}, waivers: {}, invalidations: {}, adoptions: {}, effectReconciliations: {}, integrationReady: {}, integrationReceipts: {}, stalenessReceipts: { [freshness.hash]: freshness }, gateReceipts: {} },
    findingClosures: {}, retryLedger: {}, blockers: {}, effects: {}, cancellations: {}, quarantine: {},
    integrationTrains: { "repo-main": { repositoryId: "repo-main", planTrainHash: plan.constraints.integrationTrains[0].contentHash, strategy: "merge_tree_one_parent", targetRef: "refs/heads/main", expectedTarget: plan.repositories[0].baseline, acceptedPrefix: plan.repositories[0].baseline, acceptedPrefixOrdinal: 0, acceptedPrefixReceipt: null, entryOrder: [], entries: {}, activeIntegrationAttemptId: null, lockLeaseId: null, blockerIds: [] } },
    integrationAttempts: {},
    scheduler: { policyVersion: "sticky-lanes-v1", policyHash: H("5"), normalizedIndexHash: H("6"), maxActiveNodes: 1, decisionSequence: 0, nextReservationSequence: 1, lastDecisionCommandId: null, activeNodeLanes: {}, reservations: {}, bypassCounters: {}, fairnessCounters: {}, dynamicExclusions: {}, providerHoldIds: [] },
    freshness: { class: "valid_exact", receipt: freshness, evaluatedPlanHash: plan.planHash, modelClosureHash: plan.modelBinding.closure.closureHash, repositoryObservationHashes: { "repo-main": H("d") }, affectedWorkItemIds: [], blocksNewLaunches: false, blocksIntegration: false, evaluatedAt: NOW },
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
rehashPlan(twoItemPlan);
assert.equal(validateCanonicalDagPlanV1(twoItemPlan).ok, true, "valid two-item plan supports ordered integration after independent authoring");

const run = runFixture(plan);
const runContext = { plan, authorization: authorizationBinding(plan, run.identity.reviewReceipt.hash, run.identity.authorizationReceipts.map(({ hash }) => hash), run.identity.authorizationSet.hash), historicalAuthorizations: {}, catalog: catalogBinding(plan), normalizedSchedulerIndexHash: run.scheduler.normalizedIndexHash, facts: {} };
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

console.log("Canonical DAG plan and run-state schema tests OK");
