import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { visibleWidth } from "@earendil-works/pi-tui";
import {
  CANONICAL_DAG_PLAN_SCHEMA_HASH,
  DAG_RUN_INPUT_SCHEMA_HASH,
  DAG_SCHEDULER_POLICY_HASH_V1,
  DAG_RUN_STATE_SCHEMA_HASH,
  RUN_EVALUATION_CLOCK_POLICY_HASH_V1,
  RUN_EVALUATION_PROFILE_HASH_V1,
  DagConductorServiceV1,
  attemptForReservationV1,
  DagLifecycleRuntimeV1,
  DagRunSnapshotStoreV1,
  DagRunStoreCorruptError,
  DagRunStoreLockedError,
  PLAN_STAGE_IDS,
  canonicalHash,
  canonicalStringify,
  createDagRunStoreDeadOwnerProofV1,
  dagRunStoreLockIdentityFromOwner,
  dagRunIdentityHashV1,
  integrationValidationEffectRequestV1,
  buildSchedulerPlanIndexV1,
  parseCanonicalDagPlanV1,
  parseDagRunStateV1,
  parseStrictJson,
  sealCanonicalDagPlanV1,
  reduceDagRunV1,
  renderDagWidgetV1,
  projectDagExecutionV1,
  projectDagExecutionV2,
  registerCanonicalDagRuntime,
  scheduleDagRunV1,
  sealDagRunStateV1,
  validateCanonicalDagPlanV1,
  validateDagRunStateV1,
} from "../extensions/dag-workflow/dag-runtime/index.ts";
import { WorkerManager } from "../extensions/dag-workflow/worker-runtime/manager.mjs";
import { attemptPaths, withResultHash, writeImmutableJson } from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { privateCandidateRefV1, sealPrivateCandidateRefV1 } from "../extensions/dag-workflow/index.ts";

const execFileAsync = promisify(execFile);
const H = (char) => `sha256:${char.repeat(64)}`;
const O = (char) => char.repeat(40);

const retryFrontierProbe = { workItems: { item: { stages: { F2: { currentAttemptId: "attempt-old" } } } }, stageAttempts: { "attempt-old": { leaseIds: ["lease-old"] } } };
assert.equal(attemptForReservationV1(retryFrontierProbe, { workItemId: "item", stage: "F2", leaseIds: ["lease-retry"] }), null, "a retry reservation does not reuse the sealed attempt from a prior lease");
assert.equal(attemptForReservationV1(retryFrontierProbe, { workItemId: "item", stage: "F2", leaseIds: ["lease-old"] }).leaseIds[0], "lease-old", "an outstanding reservation still resolves its exact current attempt");
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
const rehashFact = (value) => { const core = { ...value }; delete core.hash; return { ...core, hash: canonicalHash(core) }; };
const noWorkerGitOutput = () => ({ outputRepositoryId: null, outputCommonDirIdentityHash: null, outputWorktreeIdentityHash: null, outputSourceBase: null, outputCommit: null, outputTree: null, outputObjectFormat: null, candidateObservedAt: null });
const exactWorkerGitOutput = (sourceBase, commit, tree, observedAt = NOW) => ({ outputRepositoryId: "repo-main", outputCommonDirIdentityHash: canonicalHash({ fixture: "common-dir" }), outputWorktreeIdentityHash: canonicalHash({ fixture: "worktree", commit }), outputSourceBase: sourceBase, outputCommit: commit, outputTree: tree, outputObjectFormat: commit.length === 40 ? "sha1" : "sha256", candidateObservedAt: observedAt });
const procedureCatalogFixture = () => {
  const entries = PLAN_STAGE_IDS.map((stage) => {
    const readOnly = !["F1", "F3", "F6"].includes(stage);
    const environmentProfileHash = H("b");
    const executable = { executableArtifactHash: H("c"), argv: ["/usr/bin/true"], cwdMode: "repository_root", environmentProfileId: "test-exact", environmentProfileHash, environmentHash: canonicalHash({ LC_ALL: "C" }), timeoutMs: 1000, readOnly, noEdit: readOnly };
    const input = { procedureId: `procedure-${stage.toLowerCase()}`, purpose: "lifecycle", stages: [stage], producerKinds: [({ F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" })[stage]], readOnly, environmentProfileHash, executable };
    const hash = canonicalHash(input);
    return [hash, { ...input, hash }];
  });
  const environmentProfileHash = H("b");
  const executable = { executableArtifactHash: H("d"), argv: ["/usr/bin/true", "--delta-attestation"], cwdMode: "attempt_worktree", environmentProfileId: "test-exact", environmentProfileHash, environmentHash: canonicalHash({ LC_ALL: "C", PI_DAG_MODE: "delta" }), timeoutMs: 1000, readOnly: true, noEdit: true };
  const deltaInput = { procedureId: "procedure-f3-delta-attestation", purpose: "evidence_only_delta_attestation", stages: ["F3"], producerKinds: ["deterministic_runner"], readOnly: true, environmentProfileHash, executable };
  const deltaHash = canonicalHash(deltaInput); entries.push([deltaHash, { ...deltaInput, hash: deltaHash }]);
  return Object.fromEntries(entries);
};
const catalogBinding = (plan) => ({ lifecycleProfileHash: plan.lifecycleBinding.profileHash, checkCatalogHash: plan.lifecycleBinding.checkCatalogHash, procedures: procedureCatalogFixture(), checkAggregates: {} });
const validationEnvironment = { LC_ALL: "C" };
const validationProfile = (profileId) => ({ profileId, executableArtifactHash: H("c"), argv: ["/usr/bin/true"], cwdMode: "detached_proposal_worktree", environmentProfileId: "test-validation-env", environmentProfileHash: canonicalHash({ profileId: "test-validation-env", environment: validationEnvironment }), environment: validationEnvironment, environmentHash: canonicalHash(validationEnvironment), timeoutMs: 1000, readOnly: true, noEdit: true });
const PREFIX_VALIDATION_PROFILE = validationProfile("checks-prefix"); const PREFIX_VALIDATION_PROFILE_HASH = canonicalHash(PREFIX_VALIDATION_PROFILE);
const FINAL_VALIDATION_PROFILE = validationProfile("checks-final"); const FINAL_VALIDATION_PROFILE_HASH = canonicalHash(FINAL_VALIDATION_PROFILE);
const INTEGRATION_VALIDATION_PROFILES = { [PREFIX_VALIDATION_PROFILE_HASH]: PREFIX_VALIDATION_PROFILE, [FINAL_VALIDATION_PROFILE_HASH]: FINAL_VALIDATION_PROFILE };
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

function planFixture(repositoryBaseline = { repositoryId: "repo-main", commit: O("a"), tree: O("b") }) {
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
    baseline: repositoryBaseline, targetRef: "refs/heads/main",
  });
  const train = content({
    trainId: "train-main", repositoryId: "repo-main", strategy: "merge_tree_one_parent",
    members: [{ workItemId: "item-api", ordinal: 0 }], partialIntegrationPrecedenceIds: [], compositionProfileHash: H("d"),
    prefixValidationProfileId: "checks-prefix", prefixValidationProfileHash: PREFIX_VALIDATION_PROFILE_HASH, finalValidationProfileId: "checks-final", finalValidationProfileHash: FINAL_VALIDATION_PROFILE_HASH,
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
    evidenceIndex: { stageAttemptInputs: {}, workerResults: {}, candidates: {}, stageEvidence: {}, checkAggregates: {}, checkDispositions: {}, verifications: {}, oracleAssertions: {}, findings: {}, findingResolutions: {}, waivers: {}, invalidations: {}, adoptions: {}, effectReconciliations: {}, integrationReady: {}, integrationReceipts: {}, stalenessReceipts: { [freshness.hash]: freshness }, gateReceipts: {} },
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

function managerLifecycleAdapters(manager, validationContext, terminalOverrides = {}) {
  const launches = [];
  const workerOutputs = new Map();
  const terminalStatuses = new Map();
  const procedureCalls = new Map();
  const allowlistedProcedureHashes = Object.keys(validationContext.catalog.procedures).sort();
  return {
    launches,
    procedureCalls,
    options: {
      worker: {
        async launchExact(request, state) {
          const attempt = Object.values(state.stageAttempts).find((candidate) => candidate.launchIntentId && state.launchIntents[candidate.launchIntentId]?.workerId === request.workerId);
          const stage = attempt?.stage;
          if (stage === "F2") {
            const implementation = launches.find((entry) => entry.stage === "F1");
            assert(implementation, "real F2 manager launch observes a prior exact F1 launch");
            await assert.rejects(() => manager.launchOwnedAttempt({ ...request, launchKey: implementation.request.launchKey, worktreeKey: implementation.request.worktreeKey }), /replay conflicts|Launch key conflict/, "cross-attempt reuse of the F1 launch key/intent fails closed before F2 dispatch");
          }
          const observation = await manager.launchOwnedAttempt(request);
          launches.push({ stage, request: structuredClone(request), observation: structuredClone(observation) });
          return observation;
        },
        async cancelExact(binding, input) {
          const result = await manager.cancelBinding(binding, `canonical DAG cancellation ${input.effectId}`);
          return result.alreadyTerminal ? "proven_absent" : "applied_exact";
        },
        async readTerminalExact(binding, state, _signal, input) {
          const terminal = await manager.terminalResultForBinding(binding, { reconcile: input?.reconcile === true });
          if (!terminal) return null;
          const attempt = state.stageAttempts[binding.stageAttemptId];
          const terminalStatus = terminalOverrides[attempt.stage] ?? terminal.terminalStatus;
          terminalStatuses.set(attempt.stageAttemptId, terminalStatus);
          if (terminalStatus !== "succeeded") return { ...terminal, terminalStatus };
          const exact = await manager.inspectBinding(binding); const cwd = exact.worker.cwd;
          const env = { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
          const commit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd, env })).stdout.trim();
          const tree = (await execFileAsync("git", ["rev-parse", "HEAD^{tree}"], { cwd, env })).stdout.trim();
          const commonRaw = (await execFileAsync("git", ["rev-parse", "--git-common-dir"], { cwd, env })).stdout.trim();
          const commonDir = await realpath(resolve(cwd, commonRaw)); const cwdIdentity = await stat(cwd);
          const item = state.workItems[attempt.workItemId]; const sourceBase = attempt.stage === "F1" ? state.repositories[item.writeRepositoryId].baseline : item.candidate?.git ?? state.repositories[item.writeRepositoryId].baseline;
          const workerOutput = { outputRepositoryId: item.writeRepositoryId, outputCommonDirIdentityHash: canonicalHash({ realPath: commonDir, objectFormat: "sha1" }), outputWorktreeIdentityHash: canonicalHash({ realPath: await realpath(cwd), dev: String(cwdIdentity.dev), ino: String(cwdIdentity.ino) }), outputSourceBase: sourceBase, outputCommit: commit, outputTree: tree, outputObjectFormat: "sha1", candidateObservedAt: state.updatedAt };
          workerOutputs.set(attempt.stageAttemptId, workerOutput);
          return { ...terminal, terminalStatus, workerOutput };
        },
      },
      candidate: {
        async inspectAndSealCandidate({ plan: exactPlan, state, attempt, repositoryId }) {
          const item = state.workItems[attempt.workItemId]; const workerOutput = workerOutputs.get(attempt.stageAttemptId);
          if (!workerOutput) return null;
          const base = attempt.stage === "F1" ? exactPlan.repositories.find((repository) => repository.repositoryId === repositoryId).baseline : item.candidate.git;
          const git = { repositoryId, commit: workerOutput.outputCommit, tree: workerOutput.outputTree };
          const core = { kind: "candidate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: item.workItemId, generation: item.candidateGeneration + 1, candidateId: `candidate-${attempt.stageAttemptId}`, base, git, patchIdentityHash: canonicalHash({ base, git }), producedByStageAttemptId: attempt.stageAttemptId, lineageHash: item.implementationLineageHash };
          return { candidate: { ...core, hash: canonicalHash(core) }, workerOutput };
        },
      },
      procedure: {
        adapterKind: "immutable-catalog-command-v1",
        allowlistedProcedureHashes,
        allowlistHash: canonicalHash(allowlistedProcedureHashes),
        async executeExact({ plan: exactPlan, state, attempt, procedure }) {
          procedureCalls.set(attempt.stageAttemptId, (procedureCalls.get(attempt.stageAttemptId) ?? 0) + 1);
          if (PLAN_STAGE_IDS.indexOf(attempt.stage) > PLAN_STAGE_IDS.indexOf("F5")) return null;
          const item = state.workItems[attempt.workItemId]; const candidateGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
          const terminalStatus = terminalStatuses.get(attempt.stageAttemptId) ?? null;
          const disposition = ["cancelled", "lost"].includes(terminalStatus) ? "BLOCKED" : ["failed", "needs_attention"].includes(terminalStatus) ? "FAIL" : "PASS";
          let workspaceMaterialization; let environmentObservation;
          const workerOutput = workerOutputs.get(attempt.stageAttemptId);
          if (["F2", "F5"].includes(attempt.stage) && workerOutput) {
            const materializationCore = { kind: "workspace_materialization", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, commonDirIdentityHash: workerOutput.outputCommonDirIdentityHash, worktreeIdentityHash: workerOutput.outputWorktreeIdentityHash, materializedAt: NOW };
            workspaceMaterialization = { ...materializationCore, hash: canonicalHash(materializationCore) };
            const observationCore = { kind: "environment_observation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: workspaceMaterialization.hash, commonDirIdentityHash: workerOutput.outputCommonDirIdentityHash, worktreeIdentityHash: workerOutput.outputWorktreeIdentityHash, cleanliness: "clean", observedAt: NOW };
            environmentObservation = { ...observationCore, hash: canonicalHash(observationCore) };
          }
          const oracleAssertions = [];
          if (attempt.stage === "F2") { const expected = exactPlan.acceptanceOracles[0].assertions[0]; const core = { kind: "oracle_assertion", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: "F2", stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, oracleId: exactPlan.acceptanceOracles[0].oracleId, assertionId: expected.assertionId, procedureId: expected.procedureId, environmentProfileId: expected.environmentProfileId, observationMethod: expected.observationMethod, requiredEvidenceClass: expected.requiredEvidenceClass, disposition: "PASS", observationHash: attempt.workerResult.hash }; oracleAssertions.push({ ...core, hash: canonicalHash(core) }); }
          const checkExecutions = [];
          if (attempt.stage === "F2") { const core = { kind: "check_execution", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, candidateGeneration, candidateHash: item.candidate.candidateHash, checkId: "check-api", procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation.hash, executionId: `execution-${attempt.stageAttemptId}`, disposition: "PASS", startedAt: NOW, completedAt: NOW }; checkExecutions.push({ ...core, hash: canonicalHash(core) }); }
          const aggregateCore = { kind: "check_aggregate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition, oracleIds: ["oracle-api"], assertions: oracleAssertions.map((fact) => ({ oracleId: fact.oracleId, assertionId: fact.assertionId, evidenceHash: fact.hash })), checks: attempt.stage === "F2" ? [{ checkId: "check-api", disposition: "PASS", executionEvidenceHash: checkExecutions[0].hash, applicabilityEvidenceHashes: [] }] : [] };
          const checkAggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) };
          const evidenceCore = { kind: "stage_evidence", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: checkAggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration, candidateHash: attempt.stage === "F0" ? null : item.candidate?.candidateHash ?? null, producerKind: attempt.producerKind, producerResultHash: attempt.workerResult?.hash ?? null, disposition, environmentObservationHash: environmentObservation?.hash ?? null, producedAt: NOW, readOnly: procedure.readOnly };
          const evidence = { ...evidenceCore, hash: canonicalHash(evidenceCore) };
          return { checkAggregate, evidence, oracleAssertions, checkDispositions: [], checkExecutions, checkAuthorities: [], ...(workspaceMaterialization ? { workspaceMaterialization, environmentObservation } : {}) };
        },
      },
    },
  };
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
const runContext = { plan, authorization: authorizationBinding(plan, run.identity.reviewReceipt.hash, run.identity.authorizationReceipts.map(({ hash }) => hash), run.identity.authorizationSet.hash), historicalAuthorizations: {}, catalog: catalogBinding(plan), normalizedSchedulerIndexHash: run.scheduler.normalizedIndexHash, facts: {}, integrationValidationProfiles: INTEGRATION_VALIDATION_PROFILES };
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
const f0AggregateCatalogInput = { workItemId: "item-api", stage: "F0", procedureHash: f0Procedure.hash, environmentProfileHash: f0Procedure.environmentProfileHash, disposition: "PASS", oracleIds: ["oracle-api"], assertions: [], checks: [] };
const f0AggregateCatalogHash = canonicalHash(f0AggregateCatalogInput);
f0Catalog.checkAggregates[f0AggregateCatalogHash] = { ...f0AggregateCatalogInput, hash: f0AggregateCatalogHash };
const f0AggregateCore = { kind: "check_aggregate", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, authorizationSetHash: run.identity.authorizationSet.hash, workItemId: "item-api", stage: "F0", stageAttemptId: "attempt-f0", attemptInputHash: f0Input.hash, procedureHash: f0Procedure.hash, environmentProfileHash: f0Procedure.environmentProfileHash, disposition: "PASS", oracleIds: ["oracle-api"], assertions: [], checks: [] };
const f0AggregateFact = { ...f0AggregateCore, hash: canonicalHash(f0AggregateCore) };
const f0Fact = { kind: "stage_evidence", hash: f0EvidenceHash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F0", stageAttemptId: "attempt-f0", attemptInputHash: f0Input.hash, authorizationSetHash: run.identity.authorizationSet.hash, procedureHash: f0Procedure.hash, environmentProfileHash: f0Procedure.environmentProfileHash, checkAggregateHash: f0AggregateFact.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration: 0, candidateHash: null, producerKind: "conductor", producerResultHash: null, disposition: "PASS", producedAt: NOW, freshIndependent: false, readOnly: true, cleanEnvironment: false };
f0InputFact.hash = canonicalHash(Object.fromEntries(Object.entries(f0InputFact).filter(([key]) => key !== "hash")));
f0Input.hash = f0InputFact.hash;
f0AggregateFact.attemptInputHash = f0Input.hash;
f0AggregateFact.hash = canonicalHash(Object.fromEntries(Object.entries(f0AggregateFact).filter(([key]) => key !== "hash")));
f0Fact.attemptInputHash = f0Input.hash;
f0Fact.checkAggregateHash = f0AggregateFact.hash;
f0Fact.hash = canonicalHash(Object.fromEntries(Object.entries(f0Fact).filter(([key]) => key !== "hash")));
passedF0.stageAttempts["attempt-f0"].attemptInput.hash = f0Input.hash;
passedF0.stageAttempts["attempt-f0"].evidence.hash = f0Fact.hash;
passedF0.evidenceIndex.stageAttemptInputs["attempt-f0"].hash = f0Input.hash;
passedF0.evidenceIndex.checkAggregates[f0AggregateFact.hash] = ref("check_aggregate", "aggregate-f0", f0AggregateFact.hash);
delete passedF0.evidenceIndex.stageEvidence[f0EvidenceHash];
passedF0.evidenceIndex.stageEvidence[f0Fact.hash] = { ...f0Evidence, hash: f0Fact.hash };
passedF0.workItems["item-api"].stages.F0.currentEvidence = f0Fact.hash;
rehashRun(passedF0);
assert.equal(validateDagRunStateV1(passedF0, { ...runContext, catalog: f0Catalog, facts: { [f0Input.hash]: f0InputFact, [f0AggregateFact.hash]: f0AggregateFact, [f0Fact.hash]: f0Fact } }).ok, true, "exact sealed conductor evidence can pass F0 without generic worker authority");
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, facts: { [f0Input.hash]: f0InputFact, [f0Fact.hash]: f0Fact } }), passedF0, "stage evidence cannot use arbitrary unbound procedure, environment, or check hashes");
const blockedWithoutLane = clone(passedF0); blockedWithoutLane.workItems["item-api"].current = "blocked"; blockedWithoutLane.current.activeWorkItemIds = []; blockedWithoutLane.current.blockedWorkItemIds = ["item-api"]; delete blockedWithoutLane.scheduler.activeNodeLanes["item-api"]; rehashRun(blockedWithoutLane); expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, catalog: f0Catalog, facts: { [f0Input.hash]: f0InputFact, [f0AggregateFact.hash]: f0AggregateFact, [f0Fact.hash]: f0Fact } }), blockedWithoutLane, "blocked admitted work cannot release its sticky lane");
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
expectInvalid((value) => validateDagRunStateV1(value, { ...runContext, facts: { [forgedEvidenceHash]: { kind: "stage_evidence", hash: forgedEvidenceHash, planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F8", stageAttemptId: "missing-attempt", attemptInputHash: H("1"), authorizationSetHash: run.identity.authorizationSet.hash, procedureHash: H("2"), environmentProfileHash: H("3"), checkAggregateHash: H("4"), findingHashes: [], effectReconciliationHashes: [], candidateGeneration: 0, candidateHash: null, producerKind: "conductor", producerResultHash: null, disposition: "PASS", producedAt: NOW, freshIndependent: false, readOnly: true, cleanEnvironment: true } } }), forgedIndexedPass, "indexed hashes cannot forge out-of-order F8 passage without a sealed exact attempt");
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
const lateFactInput = { kind: "worker_result", workerStorageId: "storage-late", launchOwnerSessionId: "session-late", workerId: "worker-late", attemptNumber: 1, attemptNonce: "0123456789abcdef", configHash: H("6"), completionId: "completion-late", terminalStatus: "succeeded" };
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

const ownershipFacts = new Map();
const ownershipFactFor = (state, successor, disposition, lineageHash = null, priorObservationHash = null) => {
  const exactLineageHash = disposition === "same_manager" ? canonicalHash({ kind: "direct_owner_transfer", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity }) : lineageHash;
  const priorOwnershipReceiptHash = state.owner.ownershipReceipt; const predecessor = priorOwnershipReceiptHash ? ownershipFacts.get(priorOwnershipReceiptHash) : null; const ownerEpoch = state.owner.ownerEpoch + 1;
  const chainHash = canonicalHash({ kind: "ownership_chain", runId: state.runId, runNonce: state.runNonce, ownerEpoch, priorOwnershipReceiptHash, priorChainHash: predecessor?.chainHash ?? null, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity });
  const input = { kind: "ownership", runId: state.runId, runNonce: state.runNonce, priorSessionId: state.owner.sessionId, priorOwnerTokenHash: state.owner.ownerTokenHash, priorPid: state.owner.pid, priorProcessStartIdentity: state.owner.processStartIdentity, priorLockIdentity: state.owner.lockIdentity, priorAttachedAt: state.owner.attachedAt, disposition, priorObservationHash, priorOwnershipReceiptHash, ownerEpoch, successorSessionId: successor.sessionId, successorPid: successor.pid, successorProcessStartIdentity: successor.processStartIdentity, successorLockIdentity: successor.lockIdentity, lineageHash: exactLineageHash, chainHash };
  const fact = { ...input, hash: canonicalHash(input) }; ownershipFacts.set(fact.hash, fact); return fact;
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
activeCancellationRun.owner = clone(attachedResult.state.owner);
activeCancellationRun.scheduler.reservations["reservation-f1"] = { reservationId: "reservation-f1", reservationSequence: 1, workItemId: "item-api", stage: "F1", attemptOrdinal: 1, operationKind: "implementation", state: "active", candidateGeneration: 0, ownerEpoch: activeCancellationRun.owner.ownerEpoch, authorizationSetHash: run.identity.authorizationSet.hash, normalizedRequestHash: H("1"), leaseIds: [], mutexGroupIds: [], resourceUnits: {}, operationalUnits: { "worker.process": 1, "role:implementation": 1, "repository-worktree:repo-main": 1 }, workerRole: "implementation", repositoryId: "repo-main", createdAt: NOW, releasedAt: null };
activeCancellationRun.scheduler.nextReservationSequence = 2;
const f1AttemptId = "attempt-f1-active";
const f1InputCore = { kind: "stage_attempt_input", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F1", stageAttemptId: f1AttemptId, candidateGeneration: 0, candidateHash: null, authorizationSetHash: run.identity.authorizationSet.hash, producerKind: "owned_worker", implementationLineageHash: activeCancellationRun.workItems["item-api"].implementationLineageHash };
const f1InputFact = { ...f1InputCore, hash: canonicalHash(f1InputCore) };
const f1InputRef = ref("stage_attempt_input", f1AttemptId, f1InputFact.hash);
const launchEffect = { effectId: "effect-launch-f1", kind: "launch_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: H("0"), boundOwnerEpoch: activeCancellationRun.owner.ownerEpoch, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("1"), state: "reconciled", dispatchCount: 1, createdRevision: 0, createdAt: NOW, lastDispatchAt: NOW, observationHash: null, reconciliation: "applied_exact", blockerId: null };
activeCancellationRun.effects[launchEffect.effectId] = launchEffect;
activeCancellationRun.effects["effect-old-intent"] = { effectId: "effect-old-intent", kind: "run_procedure", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("2"), boundOwnerEpoch: activeCancellationRun.owner.ownerEpoch, boundAuthorizationSetHash: run.identity.authorizationSet.hash, boundFreshnessReceiptHash: run.freshness.receipt.hash, boundCandidateGeneration: 0, boundGateEpochHash: H("3"), state: "intended", dispatchCount: 0, createdRevision: 0, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
const launchIntentId = "launch-f1-active";
activeCancellationRun.launchIntents[launchIntentId] = { launchIntentId, effectId: launchEffect.effectId, stageAttemptId: f1AttemptId, state: "bound", adapter: "owned-worker-v1", launchKey: "launch-key-f1", workerId: "worker-f1", expectedAttemptNumber: 1, taskPacketHash: H("2"), cwdRepositoryId: "repo-main", configRequestHash: H("3"), dispatchCount: 1, lastDispatchAt: NOW, boundAt: NOW, ambiguityReason: null };
activeCancellationRun.stageAttempts[f1AttemptId] = { stageAttemptId: f1AttemptId, workItemId: "item-api", stage: "F1", ordinal: 1, producerKind: "owned_worker", implementationLineageHash: activeCancellationRun.workItems["item-api"].implementationLineageHash, inputGeneration: 0, reservedOutputGeneration: null, attemptInput: f1InputRef, authorizationSetHash: run.identity.authorizationSet.hash, state: "running", launchIntentId, leaseIds: ["lease-f1"], workerResult: null, evidence: null, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: null };
activeCancellationRun.evidenceIndex.stageAttemptInputs[f1AttemptId] = f1InputRef;
const activeWorkerConfig = { storageId: "storage-f1", ownerSessionId: activeCancellationRun.owner.sessionId, workerId: "worker-f1", attemptNumber: 1, attemptNonce: "0123456789abcdef", launchKey: "launch-key-f1", requestHash: H("3"), launchOwner: { sessionId: activeCancellationRun.owner.sessionId, pid: activeCancellationRun.owner.pid, processStartIdentity: activeCancellationRun.owner.processStartIdentity } };
const activeWorkerConfigHash = canonicalHash(activeWorkerConfig); const activeWorkerConfigFactCore = { kind: "worker_config", configHash: activeWorkerConfigHash, config: activeWorkerConfig }; const activeWorkerConfigFact = { ...activeWorkerConfigFactCore, hash: canonicalHash(activeWorkerConfigFactCore) };
activeCancellationRun.workerBindings[f1AttemptId] = { stageAttemptId: f1AttemptId, launchIntentId, workerStorageId: activeWorkerConfig.storageId, launchOwnerSessionId: activeWorkerConfig.ownerSessionId, workerId: activeWorkerConfig.workerId, attemptNumber: 1, attemptNonce: activeWorkerConfig.attemptNonce, configHash: activeWorkerConfigHash, configRef: { ...ref("worker_config", "config-f1", activeWorkerConfigFact.hash), bytes: Buffer.byteLength(canonicalStringify(activeWorkerConfigFact)) }, supervisorPid: 12345, supervisorStartIdentity: "linux-proc:12345", childPid: 12346, childStartIdentity: "linux-proc:12346", mailboxHash: H("6"), heartbeatAt: NOW, completionId: null, resultHash: null };
const activeLaunchObservationCore = { kind: "worker_launch_observation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, authorizationSetHash: run.identity.authorizationSet.hash, ownerEpoch: activeCancellationRun.owner.ownerEpoch, effectId: launchEffect.effectId, requestHash: launchEffect.requestHash, launchIntentId, launchKey: "launch-key-f1", workerStorageId: activeWorkerConfig.storageId, launchOwnerSessionId: activeWorkerConfig.ownerSessionId, workerId: activeWorkerConfig.workerId, attemptNumber: 1, attemptNonce: activeWorkerConfig.attemptNonce, configHash: activeWorkerConfigHash, supervisorPid: 12345, supervisorStartIdentity: "linux-proc:12345", reconciliation: "applied_exact", observedAt: NOW }; const activeLaunchObservation = { ...activeLaunchObservationCore, hash: canonicalHash(activeLaunchObservationCore) }; launchEffect.observationHash = activeLaunchObservation.hash;
activeCancellationRun.leases["lease-f1"] = { leaseId: "lease-f1", kind: "stage_claim", subject: { kind: "work_item", id: "item-api" }, holderStageAttemptId: f1AttemptId, holderIntegrationAttemptId: null, candidateGeneration: 0, units: 0, ownerEpoch: activeCancellationRun.owner.ownerEpoch, state: "active", acquiredAt: NOW, expiresAt: null, releasedAt: null, releaseReason: null };
const operationalLeaseIds = [];
for (const namespace of ["worker.process", "role:implementation", "repository-worktree:repo-main"]) {
  const leaseId = `lease-operational-${namespace.replaceAll(/[^A-Za-z0-9]/g, "-")}`; operationalLeaseIds.push(leaseId);
  activeCancellationRun.leases[leaseId] = { leaseId, kind: "operational", subject: { kind: "resource", id: namespace }, holderStageAttemptId: f1AttemptId, holderIntegrationAttemptId: null, candidateGeneration: 0, units: 1, ownerEpoch: activeCancellationRun.owner.ownerEpoch, state: "active", acquiredAt: NOW, expiresAt: null, releasedAt: null, releaseReason: null };
  activeCancellationRun.scheduler.operationalCapacities[namespace].allocatedUnits = 1; activeCancellationRun.scheduler.operationalCapacities[namespace].reservationIds = ["reservation-f1"];
}
activeCancellationRun.workItems["item-api"].activeLeaseIds = ["lease-f1", ...operationalLeaseIds].sort();
activeCancellationRun.workItems["item-api"].stages.F1 = { stage: "F1", state: "active", attemptIds: [f1AttemptId], currentAttemptId: f1AttemptId, currentEvidence: null, adoptionReceipt: null, invalidationIds: [], lastDisposition: null, blockerIds: [] };
activeCancellationRun.scheduler.reservations["reservation-f1"].leaseIds = ["lease-f1", ...operationalLeaseIds].sort();
rehashRun(activeCancellationRun);
const activeBinding = activeCancellationRun.workerBindings[f1AttemptId];
const activeCancelRequestHash = canonicalHash({ kind: "cancel_worker", runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stageAttemptId: f1AttemptId, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, fencedGeneration: 1 });
const activeCancelEffect = { ...cancelEffect, effectId: "effect-cancel-active", requestHash: activeCancelRequestHash, boundOwnerEpoch: activeCancellationRun.owner.ownerEpoch };
const activeCancelPayload = { cancellationId: "cancel-active", scope: "run", subjectId: run.runId, reason: "Stop active run", workItemIds: ["item-api"], effects: [activeCancelEffect] };
const activeCancellationContext = { ...ownerContext, catalog: f0Catalog, facts: { ...ownerContext.facts, [f0Input.hash]: f0InputFact, [f0AggregateFact.hash]: f0AggregateFact, [f0Fact.hash]: f0Fact, [f1InputFact.hash]: f1InputFact, [activeWorkerConfigFact.hash]: activeWorkerConfigFact, [activeLaunchObservation.hash]: activeLaunchObservation } };
const activeCancellationFixtureValidation = validateDagRunStateV1(activeCancellationRun, activeCancellationContext);
assert.equal(activeCancellationFixtureValidation.ok, true, `active cancellation fixture is legal: ${JSON.stringify(activeCancellationFixtureValidation.issues)}`);
const activeWorkerProjection = { projectionHash: H("4"), workers: [{ storageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, terminalStatus: null, resultHash: activeBinding.resultHash }] };
const activeProjectionDecision = { planHash: plan.planHash, inputSnapshotHash: activeCancellationRun.snapshotHash, frontier: [], decisionHash: H("5") };
const activeExecutionProjectionV2 = projectDagExecutionV2(plan, activeCancellationRun, activeProjectionDecision, activeWorkerProjection);
assert.equal(activeExecutionProjectionV2.nodes.find(({ workItemId }) => workItemId === "item-api").worker?.terminalStatus, null, "V2 exposes the exact joined active-worker terminal status");
const lineageBindingRun = clone(activeCancellationRun); delete lineageBindingRun.workerBindings[f1AttemptId]; delete lineageBindingRun.effects["effect-old-intent"]; lineageBindingRun.stageAttempts[f1AttemptId].state = "launching"; lineageBindingRun.launchIntents[launchIntentId].state = "dispatching"; lineageBindingRun.launchIntents[launchIntentId].boundAt = null; lineageBindingRun.effects[launchEffect.effectId].state = "dispatching"; lineageBindingRun.effects[launchEffect.effectId].reconciliation = "not_started"; lineageBindingRun.effects[launchEffect.effectId].observationHash = null; rehashRun(lineageBindingRun);
const intendedPreBindRun = clone(lineageBindingRun); intendedPreBindRun.effects[launchEffect.effectId].state = "intended"; intendedPreBindRun.effects[launchEffect.effectId].dispatchCount = 0; intendedPreBindRun.effects[launchEffect.effectId].lastDispatchAt = null; intendedPreBindRun.launchIntents[launchIntentId].state = "reserved"; intendedPreBindRun.launchIntents[launchIntentId].dispatchCount = 0; intendedPreBindRun.launchIntents[launchIntentId].lastDispatchAt = null; rehashRun(intendedPreBindRun);
const intendedPreBindPayload = { cancellationId: "cancel-intended-prebind", scope: "run", subjectId: run.runId, reason: "cancel before durable launch dispatch", workItemIds: ["item-api"], effects: [] };
const intendedPreBindCancellation = reduceDagRunV1(intendedPreBindRun, reducerInput(intendedPreBindRun, "request_cancellation", intendedPreBindPayload, { commandId: "command-cancel-intended-prebind", idempotencyKey: "cancel-intended-prebind" }), activeCancellationContext);
assert.equal(intendedPreBindCancellation.accepted, true, `intended pre-bind launch cancellation is accepted: ${JSON.stringify(intendedPreBindCancellation)}`);
if (intendedPreBindCancellation.accepted) {
  assert.deepEqual({ state: intendedPreBindCancellation.state.effects[launchEffect.effectId].state, reconciliation: intendedPreBindCancellation.state.effects[launchEffect.effectId].reconciliation, dispatchCount: intendedPreBindCancellation.state.effects[launchEffect.effectId].dispatchCount }, { state: "cancelled", reconciliation: "proven_absent", dispatchCount: 0 }, "durable undispatched launch intent canonically proves external absence immediately");
  assert.equal(intendedPreBindCancellation.state.blockers[`cancellation-effect-${launchEffect.effectId}`], undefined, "undispatched launch cancellation creates no ambiguity blocker");
  const restartedIntended = parseDagRunStateV1(canonicalStringify(intendedPreBindCancellation.state), activeCancellationContext);
  const intendedClosureCore = { cancellationId: intendedPreBindPayload.cancellationId, effectObservations: [], workerResults: [] };
  const intendedClosure = reduceDagRunV1(restartedIntended, reducerInput(restartedIntended, "record_cancellation", { ...intendedClosureCore, resultHash: canonicalHash(intendedClosureCore) }, { commandId: "command-close-intended-prebind", idempotencyKey: "close-intended-prebind", kind: "observation" }), activeCancellationContext);
  assert.equal(intendedClosure.accepted && intendedClosure.state.current.run, "cancelled", "restart closes intended pre-dispatch cancellation without inventing worker or external absence evidence");
  assert.equal(intendedClosure.accepted && intendedClosure.state.launchIntents[launchIntentId].state, "not_started", "intended pre-dispatch closure records the launch as canonically not started");
  let intendedRuntimeState = restartedIntended;
  const intendedRestartStore = {
    async read() { return intendedRuntimeState; },
    async mutate({ input, context }) { const result = reduceDagRunV1(intendedRuntimeState, input, context); if (result.accepted) intendedRuntimeState = result.state; return result; },
  };
  const restartedRuntime = new DagLifecycleRuntimeV1(intendedRestartStore, plan, activeCancellationContext, dagRunStoreLockIdentityFromOwner(restartedIntended.owner), process.cwd());
  const runtimeClosure = await restartedRuntime.reconcileOne(NOW);
  assert.equal(runtimeClosure.state.cancellations[intendedPreBindPayload.cancellationId].state, "closed", "restarted lifecycle closes intended pre-dispatch cancellation without requiring a worker adapter");
}
const dispatchedPreBindPayload = { cancellationId: "cancel-dispatched-prebind", scope: "run", subjectId: run.runId, reason: "cancel after durable dispatch", workItemIds: ["item-api"], effects: [] };
const dispatchedPreBindCancellation = reduceDagRunV1(lineageBindingRun, reducerInput(lineageBindingRun, "request_cancellation", dispatchedPreBindPayload, { commandId: "command-cancel-dispatched-prebind", idempotencyKey: "cancel-dispatched-prebind" }), activeCancellationContext);
assert.equal(dispatchedPreBindCancellation.accepted, true, `dispatch-recorded pre-bind cancellation is accepted: ${JSON.stringify(dispatchedPreBindCancellation)}`);
if (dispatchedPreBindCancellation.accepted) {
  assert.equal(dispatchedPreBindCancellation.state.effects[launchEffect.effectId].state, "ambiguous");
  assert.equal(dispatchedPreBindCancellation.state.blockers[`cancellation-effect-${launchEffect.effectId}`].active, true, "dispatched pre-bind launch remains explicitly blocked until the exact opaque operation is recovered");
  const forbiddenAbsenceCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: launchEffect.effectId, requestHash: launchEffect.requestHash, reconciliation: "proven_absent", closedAt: NOW }; const forbiddenAbsence = { ...forbiddenAbsenceCore, hash: canonicalHash(forbiddenAbsenceCore) };
  const forbiddenAbsenceContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [forbiddenAbsence.hash]: forbiddenAbsence } }; const forbiddenAbsenceCorePayload = { cancellationId: dispatchedPreBindPayload.cancellationId, effectObservations: [{ effectId: launchEffect.effectId, observationHash: forbiddenAbsence.hash }], workerResults: [] };
  const forbiddenAbsenceClose = reduceDagRunV1(dispatchedPreBindCancellation.state, reducerInput(dispatchedPreBindCancellation.state, "record_cancellation", { ...forbiddenAbsenceCorePayload, resultHash: canonicalHash(forbiddenAbsenceCorePayload) }, { commandId: "command-forbid-dispatched-absence", idempotencyKey: "forbid-dispatched-absence", kind: "observation" }), forbiddenAbsenceContext);
  assert.equal(forbiddenAbsenceClose.accepted, false, "even a self-consistent absence fact cannot bypass exact recovery/binding for a durably dispatched launch operation");
  const restartedDispatched = parseDagRunStateV1(canonicalStringify(dispatchedPreBindCancellation.state), activeCancellationContext);
  const legacyCancellationRuntime = new DagLifecycleRuntimeV1({
    async read() { return restartedDispatched; },
    async readImmutableFact(hash) { return activeCancellationContext.facts[hash] ?? null; },
  }, plan, activeCancellationContext, dagRunStoreLockIdentityFromOwner(restartedDispatched.owner), process.cwd());
  const legacyCancellationPackets = await legacyCancellationRuntime.readyPackets(restartedDispatched);
  assert.equal(legacyCancellationPackets.length, 1, "legacy dispatched pre-bind cancellation exposes one explicit recovery packet");
  assert.equal(legacyCancellationPackets[0].dispatchProtocolVersion, 0, "legacy cancellation recovery preserves its original dispatch protocol identity");
  const recoveredLaunchObservationCore = { ...activeLaunchObservationCore, ownerEpoch: restartedDispatched.owner.ownerEpoch, observedAt: NOW }; const recoveredLaunchObservation = { ...recoveredLaunchObservationCore, hash: canonicalHash(recoveredLaunchObservationCore) };
  const recoveredLaunchContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [recoveredLaunchObservation.hash]: recoveredLaunchObservation } };
  const recoveredBindPayload = { stageAttemptId: f1AttemptId, binding: activeBinding, launchObservation: { ...ref("worker_launch_observation", "recovered-cancel-launch", recoveredLaunchObservation.hash), bytes: Buffer.byteLength(canonicalStringify(recoveredLaunchObservation)) } };
  const recoveredBind = reduceDagRunV1(restartedDispatched, reducerInput(restartedDispatched, "bind_worker_attempt", recoveredBindPayload, { commandId: "command-bind-dispatched-prebind", idempotencyKey: "bind-dispatched-prebind", kind: "observation" }), recoveredLaunchContext);
  assert.equal(recoveredBind.accepted, true, `restart binds the exact already-authorized launch operation under cancellation recovery: ${JSON.stringify(recoveredBind)}`);
  if (recoveredBind.accepted) {
    const recoveredCancelEffect = Object.values(recoveredBind.state.effects).find((effect) => effect.kind === "cancel_worker");
    assert(recoveredCancelEffect, "exact recovered binding atomically persists its cancellation intent");
    assert.equal(recoveredBind.state.effects[launchEffect.effectId].reconciliation, "applied_exact"); assert.equal(recoveredBind.state.blockers[`cancellation-effect-${launchEffect.effectId}`].active, false, "exact recovered launch observation releases only its immutable launch blocker");
    assert.equal(recoveredBind.state.stageAttempts[f1AttemptId].state, "cancelling"); assert.equal(recoveredBind.state.launchIntents[launchIntentId].state, "cancel_requested", "recovery binding never revives normal execution authority");
    const expectedRecoveredCancelHash = canonicalHash({ kind: "cancel_worker", runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stageAttemptId: f1AttemptId, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, fencedGeneration: 1 });
    assert.equal(recoveredCancelEffect.requestHash, expectedRecoveredCancelHash, "recovered cancellation targets the exact adopted attempt and fenced generation");
    const prematureRecoveredCloseCore = { cancellationId: dispatchedPreBindPayload.cancellationId, effectObservations: [], workerResults: [] };
    const prematureRecoveredClose = reduceDagRunV1(recoveredBind.state, reducerInput(recoveredBind.state, "record_cancellation", { ...prematureRecoveredCloseCore, resultHash: canonicalHash(prematureRecoveredCloseCore) }, { commandId: "command-premature-recovered-close", idempotencyKey: "premature-recovered-close", kind: "observation" }), recoveredLaunchContext);
    assert.equal(prematureRecoveredClose.accepted, false, "recovered dispatched launch cannot close cancellation before exact cancel-effect and terminal-worker reconciliation");
  }
}
const lineageB = { ownerTokenHash: H("a"), sessionId: "session-lineage-b", pid: 7002, processStartIdentity: "linux-proc:7002", lockIdentity: H("b") }; const lineageBFact = ownershipFactFor(lineageBindingRun, lineageB, "same_manager");
let lineageContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [lineageBFact.hash]: lineageBFact } }; let lineageTransition = reduceDagRunV1(lineageBindingRun, reducerInput(lineageBindingRun, "transfer_owner", { ...lineageB, ownershipReceipt: lineageBFact.hash, priorOwnerDisposition: "same_manager" }, { commandId: "command-lineage-b", idempotencyKey: "lineage-b" }), lineageContext); assert.equal(lineageTransition.accepted, true, `A→B owner chain transfer succeeds: ${JSON.stringify(lineageTransition)}`);
const lineageBState = lineageTransition.accepted ? lineageTransition.state : lineageBindingRun; const lineageC = { ownerTokenHash: H("c"), sessionId: "session-lineage-c", pid: 7003, processStartIdentity: "linux-proc:7003", lockIdentity: H("d") }; const lineageCFact = ownershipFactFor(lineageBState, lineageC, "same_manager"); lineageContext = { ...lineageContext, facts: { ...lineageContext.facts, [lineageCFact.hash]: lineageCFact } }; lineageTransition = reduceDagRunV1(lineageBState, reducerInput(lineageBState, "transfer_owner", { ...lineageC, ownershipReceipt: lineageCFact.hash, priorOwnerDisposition: "same_manager" }, { commandId: "command-lineage-c", idempotencyKey: "lineage-c" }), lineageContext); assert.equal(lineageTransition.accepted, true, "B→C owner chain transfer succeeds");
if (lineageTransition.accepted) {
  const lineageState = lineageTransition.state; const lineageEffect = lineageState.effects[launchEffect.effectId]; const lineageObservationCore = { kind: "worker_launch_observation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, authorizationSetHash: run.identity.authorizationSet.hash, ownerEpoch: lineageState.owner.ownerEpoch, effectId: lineageEffect.effectId, requestHash: lineageEffect.requestHash, launchIntentId, launchKey: activeWorkerConfig.launchKey, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, supervisorPid: activeBinding.supervisorPid, supervisorStartIdentity: activeBinding.supervisorStartIdentity, reconciliation: "applied_exact", observedAt: NOW }; const lineageObservation = { ...lineageObservationCore, hash: canonicalHash(lineageObservationCore) }; const lineageBindContext = { ...lineageContext, facts: { ...lineageContext.facts, [lineageObservation.hash]: lineageObservation } };
  const lineageBind = reduceDagRunV1(lineageState, reducerInput(lineageState, "bind_worker_attempt", { stageAttemptId: f1AttemptId, binding: activeBinding, launchObservation: { ...ref("worker_launch_observation", "lineage-launch", lineageObservation.hash), bytes: Buffer.byteLength(canonicalStringify(lineageObservation)) } }, { kind: "observation", commandId: "command-lineage-bind", idempotencyKey: "lineage-bind" }), lineageBindContext);
  assert.equal(lineageBind.accepted, true, `C can bind the exact A-launched worker only through the complete A→B→C receipt chain: ${JSON.stringify(lineageBind)}`);
}
const activePause = reduceDagRunV1(activeCancellationRun, reducerInput(activeCancellationRun, "set_desired_run", pausePayload, { commandId: "command-pause-active-worker", idempotencyKey: "key-pause-active-worker" }), activeCancellationContext);
assert.equal(activePause.accepted && activePause.state.current.run, "paused", "pause preserves active worker/lease history while blocking new dispatch");
if (activePause.accepted) {
  const pausedDispatch = reduceDagRunV1(activePause.state, reducerInput(activePause.state, "mark_effect_dispatching", { effectId: "effect-old-intent", expectedDispatchCount: 0 }, { commandId: "command-paused-dispatch", idempotencyKey: "key-paused-dispatch" }), activeCancellationContext);
  assert.equal(pausedDispatch.accepted, false, "paused active run rejects new external dispatch without rewriting its frontier");
}
const activeCancellation = reduceDagRunV1(activeCancellationRun, reducerInput(activeCancellationRun, "request_cancellation", activeCancelPayload, { commandId: "command-cancel-active", idempotencyKey: "key-cancel-active" }), activeCancellationContext);
assert.equal(activeCancellation.accepted, true, "generation-first cancellation coherently fences an active reserved/effectful item");
if (activeCancellation.accepted) {
  assert.equal(activeCancellation.state.scheduler.reservations["reservation-f1"].state, "release_requested", "live cancellation requests release without dropping active process authority");
  assert.equal(activeCancellation.state.effects["effect-old-intent"].state, "cancelled", "undispatched old-generation effect becomes proven absent");
  assert.equal(activeCancellation.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, null, "sticky lane remains until cancellation terminal observation");
  assert.equal(activeCancellation.state.leases["lease-f1"].state, "release_requested", "active stage lease remains release-requested until exact worker death reconciliation");
  const cancellationSuccessor = { ownerTokenHash: H("7"), sessionId: "session-cancel-successor", pid: 4567, processStartIdentity: "linux-proc:4567", lockIdentity: H("8") };
  const cancellationTransferFact = ownershipFactFor(activeCancellation.state, cancellationSuccessor, "same_manager");
  const cancellationTransferContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [cancellationTransferFact.hash]: cancellationTransferFact } };
  const cancellationTransfer = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "transfer_owner", { ...cancellationSuccessor, ownershipReceipt: cancellationTransferFact.hash, priorOwnerDisposition: "same_manager" }, { commandId: "command-cancel-owner-transfer", idempotencyKey: "cancel-owner-transfer" }), cancellationTransferContext);
  assert.equal(cancellationTransfer.accepted, true, `proven owner transfer reattaches outstanding cancellation authority: ${JSON.stringify(cancellationTransfer)}`);
  assert.equal(cancellationTransfer.accepted && cancellationTransfer.state.effects[activeCancelEffect.effectId].boundOwnerEpoch, activeCancellation.state.owner.ownerEpoch + 1, "outstanding opaque cancellation effect rebinds only to the proven successor epoch");
  assert.equal(cancellationTransfer.accepted && cancellationTransfer.state.effects[activeCancelEffect.effectId].dispatchCount, 0, "owner reattach preserves cancellation dispatch count and ambiguity boundary");
  assert.equal(cancellationTransfer.accepted && cancellationTransfer.state.scheduler.reservations["reservation-f1"].state, "release_requested", "owner reattach preserves live cancellation release_requested authority");
  const absentWorkerCore = { kind: "worker_result", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F1", stageAttemptId: f1AttemptId, launchIntentId, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, completionId: "completion-f1-already-dead", terminalStatus: "cancelled", ...noWorkerGitOutput() };
  const absentWorkerFact = { ...absentWorkerCore, hash: canonicalHash(absentWorkerCore) };
  const absentReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: activeCancelEffect.effectId, requestHash: activeCancelEffect.requestHash, reconciliation: "proven_absent", closedAt: NOW };
  const absentReconciliationFact = { ...absentReconciliationCore, hash: canonicalHash(absentReconciliationCore) };
  const absentResultRef = { ...ref("worker_result", absentWorkerFact.completionId, absentWorkerFact.hash), bytes: Buffer.byteLength(canonicalStringify(absentWorkerFact)) };
  const absentObservationCore = { cancellationId: "cancel-active", effectObservations: [{ effectId: activeCancelEffect.effectId, observationHash: absentReconciliationFact.hash }], workerResults: [{ stageAttemptId: f1AttemptId, result: absentResultRef }] };
  const absentObservationPayload = { ...absentObservationCore, resultHash: canonicalHash({ cancellationId: absentObservationCore.cancellationId, effectObservations: absentObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: absentWorkerFact.hash }] }) };
  const absentObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [absentWorkerFact.hash]: absentWorkerFact, [absentReconciliationFact.hash]: absentReconciliationFact } };
  const absentClosed = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "record_cancellation", absentObservationPayload, { commandId: "command-close-absent-cancel", idempotencyKey: "key-close-absent-cancel", kind: "observation" }), absentObservationContext);
  assert.equal(absentClosed.accepted && absentClosed.state.effects[activeCancelEffect.effectId].reconciliation, "proven_absent", `already-dead worker permits exact proven-absent cancellation-effect closure without dispatch: ${JSON.stringify(absentClosed)}`);
  const racedWorkerCore = { ...absentWorkerCore, completionId: "completion-f1-raced-success", terminalStatus: "succeeded", ...exactWorkerGitOutput(plan.repositories[0].baseline, O("c"), O("d")) };
  const racedWorkerFact = { ...racedWorkerCore, hash: canonicalHash(racedWorkerCore) };
  const racedWorkerText = canonicalStringify(racedWorkerFact);
  const racedWorkerRef = { ...ref("worker_result", racedWorkerFact.completionId, racedWorkerFact.hash), bytes: Buffer.byteLength(racedWorkerText) };
  const racedObservationCore = { cancellationId: "cancel-active", effectObservations: absentObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, result: racedWorkerRef }] };
  const racedObservationPayload = { ...racedObservationCore, resultHash: canonicalHash({ cancellationId: racedObservationCore.cancellationId, effectObservations: racedObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: racedWorkerFact.hash }] }) };
  const racedObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [racedWorkerFact.hash]: racedWorkerFact, [absentReconciliationFact.hash]: absentReconciliationFact } };
  const racedClosed = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "record_cancellation", racedObservationPayload, { commandId: "command-close-raced-cancel", idempotencyKey: "key-close-raced-cancel", kind: "observation" }), racedObservationContext);
  const racedQuarantine = racedClosed.accepted ? Object.values(racedClosed.state.quarantine).find(({ fact }) => fact.hash === racedWorkerFact.hash) : null;
  assert.equal(racedClosed.accepted && racedClosed.state.current.run === "cancelled" && racedQuarantine?.state, "held", "late successful terminal result proves process death but remains quarantined from semantic adoption");
  assert.equal(racedClosed.accepted && racedClosed.state.stageAttempts[f1AttemptId].workerResult === null && racedClosed.state.workerBindings[f1AttemptId].resultHash === null && racedClosed.state.evidenceIndex.workerResults[racedWorkerFact.hash] === undefined, true, "quarantined late success never becomes current attempt, binding, or evidence authority");
  const activeCancelDispatch = reduceDagRunV1(activeCancellation.state, reducerInput(activeCancellation.state, "mark_effect_dispatching", { effectId: activeCancelEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-dispatch-active-cancel", idempotencyKey: "key-dispatch-active-cancel" }), activeCancellationContext);
  assert.equal(activeCancelDispatch.accepted && activeCancelDispatch.effects.length, 1, "persisted active-worker cancellation intent dispatches only after generation fence");
  if (activeCancelDispatch.accepted) {
    const cancelledWorkerCore = { kind: "worker_result", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, workItemId: "item-api", stage: "F1", stageAttemptId: f1AttemptId, launchIntentId, workerStorageId: activeBinding.workerStorageId, launchOwnerSessionId: activeBinding.launchOwnerSessionId, workerId: activeBinding.workerId, attemptNumber: activeBinding.attemptNumber, attemptNonce: activeBinding.attemptNonce, configHash: activeBinding.configHash, completionId: "completion-f1-cancelled", terminalStatus: "cancelled", ...noWorkerGitOutput() };
    const cancelledWorkerFact = { ...cancelledWorkerCore, hash: canonicalHash(cancelledWorkerCore) };
    const cancelReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: activeCancelEffect.effectId, requestHash: activeCancelEffect.requestHash, reconciliation: "applied_exact", closedAt: NOW };
    const cancelReconciliationFact = { ...cancelReconciliationCore, hash: canonicalHash(cancelReconciliationCore) };
    const workerResultRef = { ...ref("worker_result", cancelledWorkerFact.completionId, cancelledWorkerFact.hash), bytes: Buffer.byteLength(canonicalStringify(cancelledWorkerFact)) };
    const activeObservationCore = { cancellationId: "cancel-active", effectObservations: [{ effectId: activeCancelEffect.effectId, observationHash: cancelReconciliationFact.hash }], workerResults: [{ stageAttemptId: f1AttemptId, result: workerResultRef }] };
    const activeObservationPayload = { ...activeObservationCore, resultHash: canonicalHash({ cancellationId: activeObservationCore.cancellationId, effectObservations: activeObservationCore.effectObservations, workerResults: [{ stageAttemptId: f1AttemptId, resultHash: cancelledWorkerFact.hash }] }) };
    const activeObservationContext = { ...activeCancellationContext, facts: { ...activeCancellationContext.facts, [cancelledWorkerFact.hash]: cancelledWorkerFact, [cancelReconciliationFact.hash]: cancelReconciliationFact } };
    const preReconciledCancelPayload = { effectId: activeCancelEffect.effectId, observationHash: cancelReconciliationFact.hash, reconciliation: "applied_exact", terminalState: "reconciled" };
    const preReconciledCancel = reduceDagRunV1(activeCancelDispatch.state, reducerInput(activeCancelDispatch.state, "record_effect_observation", preReconciledCancelPayload, { commandId: "command-pre-reconcile-cancel", idempotencyKey: "key-pre-reconcile-cancel", kind: "observation" }), activeObservationContext);
    assert.equal(preReconciledCancel.accepted, true, "cancel effect may legally reconcile through generic effect observation before cancellation closure");
    const activeClosed = reduceDagRunV1(preReconciledCancel.accepted ? preReconciledCancel.state : activeCancelDispatch.state, reducerInput(preReconciledCancel.accepted ? preReconciledCancel.state : activeCancelDispatch.state, "record_cancellation", activeObservationPayload, { commandId: "command-close-active-cancel", idempotencyKey: "key-close-active-cancel", kind: "observation" }), activeObservationContext);
    assert.equal(activeClosed.accepted && activeClosed.state.current.run, "cancelled", "active cancellation closes only after exact worker identity reports cancelled and dead");
    assert.equal(activeClosed.accepted && activeClosed.state.leases["lease-f1"].state, "released", "release-requested lease becomes terminal only after retry-safe worker death observation");
    assert.equal(activeClosed.accepted && activeClosed.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, "terminal_cancelled", "sticky lane releases only at terminal cancellation");
  }
}
const ambiguousCancellationRun = clone(activeCancellationRun);
ambiguousCancellationRun.effects[launchEffect.effectId].procedureClass = "non_repeatable";
rehashRun(ambiguousCancellationRun);
const ambiguousCancelEffect = { ...activeCancelEffect, effectId: "effect-cancel-ambiguous", requestHash: activeCancelRequestHash };
const ambiguousCancelPayload = { ...activeCancelPayload, cancellationId: "cancel-ambiguous", effects: [ambiguousCancelEffect] };
const ambiguousCancellation = reduceDagRunV1(ambiguousCancellationRun, reducerInput(ambiguousCancellationRun, "request_cancellation", ambiguousCancelPayload, { commandId: "command-cancel-ambiguous", idempotencyKey: "key-cancel-ambiguous" }), activeCancellationContext);
assert.equal(ambiguousCancellation.accepted && ambiguousCancellation.state.blockers[`cancellation-effect-${launchEffect.effectId}`] === undefined, true, "already reconciled launch effect does not create a spurious cancellation blocker");

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
  const failedEffectReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: failedRunEffect.effectId, requestHash: failedRunEffect.requestHash, reconciliation: "compensated", closedAt: NOW };
  const failedEffectReconciliation = { ...failedEffectReconciliationCore, hash: canonicalHash(failedEffectReconciliationCore) };
  const failedEffectObservationFor = (fact) => {
    const core = { cancellationId: "cancel-failed-effect", effectObservations: [{ effectId: failedRunEffect.effectId, observationHash: fact.hash }], workerResults: [] };
    return { ...core, resultHash: canonicalHash(core) };
  };
  for (const [label, closedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
    const forgedCore = { ...failedEffectReconciliationCore, closedAt }; const forged = { ...forgedCore, hash: canonicalHash(forgedCore) };
    const forgedContext = { ...runContext, facts: { [forged.hash]: forged } };
    const forgedClosure = reduceDagRunV1(failedEffectCancellation.state, reducerInput(failedEffectCancellation.state, "record_cancellation", failedEffectObservationFor(forged), { commandId: `command-close-failed-effect-${label}`, idempotencyKey: `key-close-failed-effect-${label}`, kind: "observation" }), forgedContext);
    assert.equal(forgedClosure.accepted, false, `${label} cancellation reconciliation closedAt cannot enter the reducer`);
  }
  const failedEffectObservation = failedEffectObservationFor(failedEffectReconciliation);
  const failedEffectContext = { ...runContext, facts: { [failedEffectReconciliation.hash]: failedEffectReconciliation } };
  const failedEffectClosed = reduceDagRunV1(failedEffectCancellation.state, reducerInput(failedEffectCancellation.state, "record_cancellation", failedEffectObservation, { commandId: "command-close-failed-effect", idempotencyKey: "key-close-failed-effect", kind: "observation" }), failedEffectContext);
  assert.equal(failedEffectClosed.accepted && failedEffectClosed.state.current.run, "cancelled", "valid bounded cancellation reconciliation closes every affected external effect");
  if (failedEffectClosed.accepted) for (const [label, closedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
    const forgedCore = { ...failedEffectReconciliationCore, closedAt }; const forged = { ...forgedCore, hash: canonicalHash(forgedCore) };
    const forgedState = clone(failedEffectClosed.state); const closedEffect = forgedState.effects[failedRunEffect.effectId]; closedEffect.observationHash = forged.hash;
    const attribution = Object.values(forgedState.idempotencySlots).find((slot) => slot.appliedRevision === closedEffect.reconciliationRevision); attribution.reconciliationBindings[0].observationHash = forged.hash;
    rehashRun(forgedState);
    const forgedContext = { ...runContext, facts: { [forged.hash]: forged } };
    assert.equal(validateDagRunStateV1(forgedState, forgedContext).ok, false, `${label} cancellation reconciliation closedAt fails closed at rest`);
  }
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

  const evaluationIdentity = {
    projectIdentityHash: canonicalHash({ projectId: run.identity.projectId }),
    runIdentityHash: dagRunIdentityHashV1(run),
    runNonceHash: canonicalHash(run.runNonce),
    planHash: run.identity.planHash,
    evaluationProfileHash: RUN_EVALUATION_PROFILE_HASH_V1,
    clockPolicyHash: RUN_EVALUATION_CLOCK_POLICY_HASH_V1,
    creditContextHash: canonicalHash({ acceptedIntegrationLineages: [], actionableFindingDispositions: [] }),
  };
  assert.equal(evaluationIdentity.runIdentityHash, canonicalHash({ runId: run.runId, runNonce: run.runNonce }), "run evaluation identity derivation is deterministic and binds only exact run identity fields");
  assert.throws(() => new DagRunSnapshotStoreV1(join(storeRoot, "observer-invalid-shape"), run.runId, {
    postCommitEvaluationObserver: { identity: { ...evaluationIdentity, creditContextHash: undefined }, offerCommittedSnapshot() {} },
  }), /complete immutable bound identity|canonical hash/, "post-commit observer option requires the complete closed hash identity");
  const mismatchedEvaluationStore = new DagRunSnapshotStoreV1(join(storeRoot, "observer-mismatched-binding"), run.runId, {
    postCommitEvaluationObserver: { identity: { ...evaluationIdentity, planHash: H("0") }, offerCommittedSnapshot() {} },
  });
  await assert.rejects(() => mismatchedEvaluationStore.initialize(run, runContext, initializationLock), /exact planHash\/runNonceHash\/runIdentityHash/, "genesis rejects an observer not bound to the exact run plan/nonce/identity");
  await assert.rejects(() => readFile(mismatchedEvaluationStore.statePath), { code: "ENOENT" }, "observer binding rejection occurs before genesis durability");

  const observerOffers = [];
  const postCommitEvaluationObserver = {
    identity: evaluationIdentity,
    offerCommittedSnapshot(offered) {
      const prefixStartedAt = Date.now();
      if (offered.revision === 0 || offered.revision === 2) while (Date.now() - prefixStartedAt < 150) { /* deliberate synchronous observer prefix */ }
      observerOffers.push(offered);
      if (offered.revision === 1) throw new Error("deliberate observer failure");
    },
  };
  const observerStore = new DagRunSnapshotStoreV1(join(storeRoot, "observer-isolation"), run.runId, { postCommitEvaluationObserver });
  await seedBaselineFacts(observerStore);
  await observerStore.initialize(run, runContext, initializationLock);
  assert.equal(observerOffers.length, 0, "genesis return completes before a 150ms synchronous observer prefix begins");
  for (let attempts = 0; observerOffers.length < 1 && attempts < 50; attempts += 1) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(observerOffers.length, 1, "newly durable genesis emits exactly one observer offer");
  assert.equal(Object.isFrozen(observerOffers[0]), true, "observer receives a fresh immutable closed offer");
  assert.deepEqual(Object.keys(observerOffers[0]).sort(), [...Object.keys(evaluationIdentity), "revision", "snapshotHash"].sort(), "observer offer contains identities only");
  assert.deepEqual(observerOffers[0], { ...evaluationIdentity, revision: 0, snapshotHash: run.snapshotHash }, "genesis observer offer binds exact evaluation and snapshot identities");

  await observerStore.putImmutableFact(attachOwnershipFact);
  const observerAttach = await observerStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: { lockIdentity: attachPayload.lockIdentity, ownerTokenHash: attachPayload.ownerTokenHash, sessionId: attachPayload.sessionId, pid: attachPayload.pid, processStartIdentity: attachPayload.processStartIdentity, acquiredAt: NOW } });
  assert.equal(observerAttach.accepted, true, "observer failure cannot reject a durable mutation");
  assert.equal((await observerStore.read(runContext)).revision, 1, "observer failure cannot roll back committed state");
  for (let attempts = 0; observerOffers.length < 2 && attempts < 50; attempts += 1) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(observerOffers[1], { ...evaluationIdentity, revision: 1, snapshotHash: observerAttach.state.snapshotHash }, "mutation offer preserves every exact bound identity");
  const offersAfterMutation = observerOffers.length;
  const observerOwnerLock = { lockIdentity: attachPayload.lockIdentity, ownerTokenHash: attachPayload.ownerTokenHash, sessionId: attachPayload.sessionId, pid: attachPayload.pid, processStartIdentity: attachPayload.processStartIdentity, acquiredAt: NOW };
  const observerAttachReplay = await observerStore.mutate({ input: reducerInput(run, "attach_owner", attachPayload, { kind: "observation" }), context: ownerContext, lock: observerOwnerLock });
  assert.equal(observerAttachReplay.accepted && observerAttachReplay.duplicate, true, "observer fixture duplicate is accepted without durability");
  const observerRejected = await observerStore.mutate({ input: reducerInput(run, "set_desired_run", { desired: "paused", reason: "stale", requestedBy: "user" }, { commandId: "observer-stale", idempotencyKey: "observer-stale" }), context: runContext, lock: observerOwnerLock });
  assert.equal(observerRejected.accepted, false, "observer fixture stale mutation is rejected");
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(observerOffers.length, offersAfterMutation, "duplicate and rejected mutations emit no observer offers");

  const observerPauseInput = reducerInput(observerAttach.state, "set_desired_run", { desired: "paused", reason: "observer exact", requestedBy: "user" }, { commandId: "observer-pause", idempotencyKey: "observer-pause" });
  const observerPause = await observerStore.mutate({ input: observerPauseInput, context: runContext, lock: observerOwnerLock });
  assert.equal(observerOffers.length, offersAfterMutation, "mutation return completes before a 150ms synchronous observer prefix and never backpressures lock release");
  for (let attempts = 0; observerOffers.length < offersAfterMutation + 1 && attempts < 50; attempts += 1) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(observerOffers.at(-1), { ...evaluationIdentity, revision: 2, snapshotHash: observerPause.state.snapshotHash }, "newly durable mutation emits one exact identity-only offer");

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
  const durableAbort = new AbortController(); durableAbort.abort(); const factsBeforeAbort = (await readdir(store.factsDirectory)).sort();
  await assert.rejects(() => store.putImmutableFact({ kind: "abort_probe", value: "must-not-publish" }, durableAbort.signal), (error) => error?.name === "AbortError", "pre-aborted fact publication stops before its immutable durable boundary");
  await assert.rejects(() => store.mutate({ input: reducerInput(attachedState, "set_desired_run", pausePayload, { commandId: "command-aborted-store", idempotencyKey: "key-aborted-store" }), context: runContext, lock: ownerLock, signal: durableAbort.signal }), (error) => error?.name === "AbortError", "pre-aborted reducer mutation stops before lock/reducer/snapshot side effects");
  assert.deepEqual((await readdir(store.factsDirectory)).sort(), factsBeforeAbort); assert.equal((await store.read(runContext)).revision, attachedState.revision, "aborted publication and mutation leave no hidden durable state");
  const forgedLiveTakeoverPayload = { ...attachPayload, ownerTokenHash: H("4"), sessionId: "forged-takeover", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("5"), ownershipReceipt: H("6"), priorOwnerDisposition: "dead" };
  const forgedLiveTakeoverLock = { lockIdentity: forgedLiveTakeoverPayload.lockIdentity, ownerTokenHash: forgedLiveTakeoverPayload.ownerTokenHash, sessionId: forgedLiveTakeoverPayload.sessionId, pid: forgedLiveTakeoverPayload.pid, processStartIdentity: forgedLiveTakeoverPayload.processStartIdentity, acquiredAt: NOW };
  const forgedLiveTakeoverInput = reducerInput(attachedState, "attach_owner", forgedLiveTakeoverPayload, { commandId: "command-forged-takeover", idempotencyKey: "key-forged-takeover", kind: "observation" });
  await assert.rejects(() => store.mutate({ input: forgedLiveTakeoverInput, context: runContext, lock: forgedLiveTakeoverLock }), DagRunStoreCorruptError, "ordinary mutation API cannot replace an attached live conductor using a missing forged ownership fact");
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
  const restartedOwnerStore = new DagRunSnapshotStoreV1(storeRoot, run.runId); const restartedOwnerState = await restartedOwnerStore.read(runContext);
  assert.equal(restartedOwnerState.owner.ownerEpoch, 3, "A→B→C restart recursively hydrates the complete bounded predecessor receipt chain without caller fact injection");
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
    await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", recoveryCrashStore.rootDirectory, run.runId, recoveryContextPath, recoveryInputPath, recoveryLockPath, recoveryCrashPoint, "recover-auto", recoveryProofPath], { cwd: process.cwd() }), (error) => error?.code === 86, `real child exits at recovery point ${recoveryCrashPoint} after publishing a fully chained recovery receipt`);
    const stateAfterRecoveryCrash = await recoveryCrashStore.read(runContext);
    if (stateAfterRecoveryCrash.owner.ownershipReceipt && !ownershipFacts.has(stateAfterRecoveryCrash.owner.ownershipReceipt)) ownershipFacts.set(stateAfterRecoveryCrash.owner.ownershipReceipt, await recoveryCrashStore.readImmutableFact(stateAfterRecoveryCrash.owner.ownershipReceipt));
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
  const durableReconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: run.runId, runNonce: run.runNonce, effectId: durableEffect.effectId, requestHash: durableEffect.requestHash, reconciliation: "applied_exact", closedAt: NOW };
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
  await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/dag-store-child.mjs", processCrashStore.rootDirectory, run.runId, processCrashContextPath, processCrashInputPath, processCrashLockPath, "after_snapshot_rename", "attach-auto"], { cwd: process.cwd() }), (error) => error?.code === 86, "child publishes a fully chained owner receipt before the post-rename crash boundary");
  const processCrashAttached = await processCrashStore.read(runContext);
  assert.equal(processCrashAttached.revision, run.revision + 1, "post-rename owner attach survives child crash");
  assert.equal(processCrashAttached.owner.ownerEpoch, 1);

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
  assert.equal(releasedResult.accepted, false, "generic scheduler release cannot drop live reservation/lease authority without an exact terminal attempt");
}
// Closed lifecycle kernel: after this schema-valid genesis, every F0-F8 lifecycle change is reducer-produced.
const lifecycleGenesis = clone(run);
const lifecycleSchedulerIndex = buildSchedulerPlanIndexV1(plan);
lifecycleGenesis.workItems["item-api"].current = "ready"; lifecycleGenesis.current.run = "active"; lifecycleGenesis.current.readyWorkItemIds = ["item-api"];
lifecycleGenesis.scheduler.policyHash = DAG_SCHEDULER_POLICY_HASH_V1; lifecycleGenesis.scheduler.normalizedIndexHash = lifecycleSchedulerIndex.indexHash; rehashRun(lifecycleGenesis);
let lifecycleContext = { ...runContext, normalizedSchedulerIndexHash: lifecycleSchedulerIndex.indexHash, facts: {} };
const lifecycleOwner = { ownerTokenHash: H("1"), sessionId: "session-lifecycle", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("2") };
const lifecycleOwnership = ownershipFactFor(lifecycleGenesis, lifecycleOwner, "absent"); lifecycleContext = { ...lifecycleContext, facts: { [lifecycleOwnership.hash]: lifecycleOwnership } };
let lifecycleTransition = reduceDagRunV1(lifecycleGenesis, reducerInput(lifecycleGenesis, "attach_owner", { ...lifecycleOwner, ownershipReceipt: lifecycleOwnership.hash, priorOwnerDisposition: "absent" }, { kind: "observation", commandId: "lifecycle-attach", idempotencyKey: "lifecycle-attach" }), lifecycleContext);
assert.equal(lifecycleTransition.accepted, true, `closed lifecycle genesis owner attach succeeds: ${JSON.stringify(lifecycleTransition)}`);
let lifecycleState = lifecycleTransition.accepted ? lifecycleTransition.state : lifecycleGenesis;
const lifecycleRef = (kind, id, fact) => ({ ...ref(kind, id, fact.hash), bytes: Buffer.byteLength(canonicalStringify(fact)) });
const stageProducer = { F0: "conductor", F1: "owned_worker", F2: "owned_worker", F3: "owned_worker", F4: "deterministic_runner", F5: "owned_worker", F6: "owned_worker", F7: "deterministic_runner", F8: "conductor" };
let lifecycleCandidate = null;
let firstAttemptState = null; let firstF8Payload = null; let sealedF0State = null; let sealedF0Context = null; let lifecycleStageEffect = null;
let reservedF1Fixture = null; let pendingF0EffectFixture = null; let runningF1Fixture = null;
const nonPassFixtures = [];
for (const stage of PLAN_STAGE_IDS) {
  const decision = scheduleDagRunV1(plan, lifecycleState);
  assert.equal(decision.selected[0]?.stage, stage, `scheduler reaches ${stage} from reducer-produced prior closure`);
  const reserve = { decisionHash: decision.decisionHash, decisionSequence: decision.decisionSequence, policyHash: decision.policyHash, normalizedIndexHash: decision.normalizedIndexHash, inputSnapshotHash: lifecycleState.snapshotHash, reservations: decision.selected, bypassSlotIds: decision.bypassIncrements };
  lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "reserve_scheduler_batch", reserve, { commandId: `lifecycle-${stage}-reserve`, idempotencyKey: `lifecycle-${stage}-reserve` }), lifecycleContext);
  assert.equal(lifecycleTransition.accepted, true, `${stage} reservation succeeds`); lifecycleState = lifecycleTransition.state;
  const reservation = decision.selected[0];
  if (stage === "F1") reservedF1Fixture = { state: clone(lifecycleState), context: { ...lifecycleContext, facts: { ...lifecycleContext.facts } }, reservation: clone(reservation) };
  lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "mark_scheduler_reservation_dispatch", { reservationId: reservation.reservationId, normalizedRequestHash: reservation.normalizedRequestHash }, { commandId: `lifecycle-${stage}-dispatch`, idempotencyKey: `lifecycle-${stage}-dispatch` }), lifecycleContext); assert.equal(lifecycleTransition.accepted, true); lifecycleState = lifecycleTransition.state;
  lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_scheduler_reservation_dispatch", { reservationId: reservation.reservationId, normalizedRequestHash: reservation.normalizedRequestHash, disposition: "active" }, { kind: "observation", commandId: `lifecycle-${stage}-active`, idempotencyKey: `lifecycle-${stage}-active` }), lifecycleContext); assert.equal(lifecycleTransition.accepted, true); lifecycleState = lifecycleTransition.state;
  const attemptId = `lifecycle-attempt-${stage.toLowerCase()}`; const producerKind = stageProducer[stage]; const inputCandidate = ["F0", "F1"].includes(stage) ? null : lifecycleCandidate;
  const inputCore = { kind: "stage_attempt_input", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, candidateGeneration: lifecycleState.workItems["item-api"].candidateGeneration, candidateHash: inputCandidate?.hash ?? null, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, producerKind, implementationLineageHash: ["F1", "F3"].includes(stage) ? lifecycleState.workItems["item-api"].implementationLineageHash : null };
  const inputFact = { ...inputCore, hash: canonicalHash(inputCore) }; const inputReference = lifecycleRef("stage_attempt_input", attemptId, inputFact); lifecycleContext.facts[inputFact.hash] = inputFact;
  const owned = producerKind === "owned_worker"; const launchIntentId = `lifecycle-launch-${stage.toLowerCase()}`; const launchEffectId = `lifecycle-launch-effect-${stage.toLowerCase()}`;
  const launchEffect = owned ? { effectId: launchEffectId, kind: "launch_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: canonicalHash({ stage, launch: true }), boundOwnerEpoch: lifecycleState.owner.ownerEpoch, boundAuthorizationSetHash: lifecycleState.identity.authorizationSet.hash, boundFreshnessReceiptHash: lifecycleState.freshness.receipt.hash, boundCandidateGeneration: lifecycleState.workItems["item-api"].candidateGeneration, boundGateEpochHash: H("3"), state: "intended", dispatchCount: 0, createdRevision: lifecycleState.revision + 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null } : null;
  const launchIntent = owned ? { launchIntentId, effectId: launchEffectId, state: "reserved", adapter: "owned-worker-v1", launchKey: `lifecycle-key-${stage.toLowerCase()}`, workerId: `lifecycle-worker-${stage.toLowerCase()}`, expectedAttemptNumber: 1, taskPacketHash: H("4"), cwdRepositoryId: "repo-main", configRequestHash: H("5"), dispatchCount: 0, lastDispatchAt: null, boundAt: null, ambiguityReason: null } : null;
  if (stage === "F0") {
    const staleInputCore = { ...inputCore, stage: "F1" }; const staleInputFact = { ...staleInputCore, hash: canonicalHash(staleInputCore) }; const staleInputRef = lifecycleRef("stage_attempt_input", "wrong-generation", staleInputFact); lifecycleContext.facts[staleInputFact.hash] = staleInputFact;
    assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "begin_stage_attempt", { reservationId: reservation.reservationId, stageAttemptId: attemptId, attemptInput: staleInputRef, launchIntent: null, launchEffect: null }, { commandId: "lifecycle-wrong-input", idempotencyKey: "lifecycle-wrong-input" }), lifecycleContext).accepted, false, "wrong stage/generation attempt input fails closed");
  }
  const beginPayload = { reservationId: reservation.reservationId, stageAttemptId: attemptId, attemptInput: inputReference, launchIntent, launchEffect };
  const beginInput = reducerInput(lifecycleState, "begin_stage_attempt", beginPayload, { commandId: `lifecycle-${stage}-begin`, idempotencyKey: `lifecycle-${stage}-begin` });
  lifecycleTransition = reduceDagRunV1(lifecycleState, beginInput, lifecycleContext); assert.equal(lifecycleTransition.accepted, true, `${stage} begins through reducer: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
  assert.equal(reduceDagRunV1(lifecycleState, beginInput, lifecycleContext).duplicate, true, `${stage} begin exact replay is naturally idempotent`);
  if (stage === "F0") firstAttemptState = lifecycleState;
  const stageEffectReconciliations = [];
  if (stage === "F0") {
    lifecycleStageEffect = { effectId: "lifecycle-f0-stage-effect", kind: "put_immutable_fact", subject: { kind: "work_item", id: "item-api" }, boundStageAttemptId: attemptId, boundWorkerResultHash: null, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "pure", requestHash: H("e"), boundOwnerEpoch: lifecycleState.owner.ownerEpoch, boundAuthorizationSetHash: lifecycleState.identity.authorizationSet.hash, boundFreshnessReceiptHash: lifecycleState.freshness.receipt.hash, boundCandidateGeneration: lifecycleState.workItems["item-api"].candidateGeneration, boundGateEpochHash: H("f"), state: "intended", dispatchCount: 0, createdRevision: lifecycleState.revision + 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "put_effect_intent", { effect: lifecycleStageEffect }, { commandId: "lifecycle-f0-effect-intent", idempotencyKey: "lifecycle-f0-effect-intent" }), lifecycleContext);
    assert.equal(lifecycleTransition.accepted, true, `pre-seal exact-attempt effect intent is admitted: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
    pendingF0EffectFixture = { state: clone(lifecycleState), context: { ...lifecycleContext, facts: { ...lifecycleContext.facts } }, effectId: lifecycleStageEffect.effectId };
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "mark_effect_dispatching", { effectId: lifecycleStageEffect.effectId, expectedDispatchCount: 0 }, { commandId: "lifecycle-f0-effect-dispatch", idempotencyKey: "lifecycle-f0-effect-dispatch" }), lifecycleContext);
    assert.equal(lifecycleTransition.accepted, true, "pre-seal exact-attempt effect dispatch is durably authorized"); lifecycleState = lifecycleTransition.state;
    const stageEffectObservationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, effectId: lifecycleStageEffect.effectId, requestHash: lifecycleStageEffect.requestHash, reconciliation: "applied_exact", closedAt: NOW };
    const stageEffectObservation = { ...stageEffectObservationCore, hash: canonicalHash(stageEffectObservationCore) }; lifecycleContext.facts[stageEffectObservation.hash] = stageEffectObservation;
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_effect_observation", { effectId: lifecycleStageEffect.effectId, observationHash: stageEffectObservation.hash, reconciliation: "applied_exact", terminalState: "reconciled" }, { kind: "observation", commandId: "lifecycle-f0-effect-observation", idempotencyKey: "lifecycle-f0-effect-observation" }), lifecycleContext);
    assert.equal(lifecycleTransition.accepted, true, "pre-seal exact-attempt effect reconciles before aggregate seal"); lifecycleState = lifecycleTransition.state;
    stageEffectReconciliations.push(lifecycleRef("effect_reconciliation", lifecycleStageEffect.effectId, stageEffectObservation));
  }
  let workerResult = null;
  if (owned) {
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "mark_effect_dispatching", { effectId: launchEffectId, expectedDispatchCount: 0 }, { commandId: `lifecycle-${stage}-launch-dispatch`, idempotencyKey: `lifecycle-${stage}-launch-dispatch` }), lifecycleContext); assert.equal(lifecycleTransition.accepted, true, `${stage} persists exact launch dispatch before external observation: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
    const attemptNonce = `nonce-${stage.toLowerCase()}-0123456789`; const workerStorageId = "lifecycle-manager-storage"; const config = { storageId: workerStorageId, ownerSessionId: lifecycleState.owner.sessionId, workerId: launchIntent.workerId, attemptNumber: 1, attemptNonce, launchKey: launchIntent.launchKey, requestHash: launchIntent.configRequestHash, launchOwner: { sessionId: lifecycleState.owner.sessionId, pid: lifecycleState.owner.pid, processStartIdentity: lifecycleState.owner.processStartIdentity } }; const configHash = canonicalHash(config); const configFactCore = { kind: "worker_config", configHash, config }; const configFact = { ...configFactCore, hash: canonicalHash(configFactCore) }; lifecycleContext.facts[configFact.hash] = configFact;
    const binding = { stageAttemptId: attemptId, launchIntentId, workerStorageId, launchOwnerSessionId: lifecycleState.owner.sessionId, workerId: launchIntent.workerId, attemptNumber: 1, attemptNonce, configHash, configRef: lifecycleRef("worker_config", `config-${stage.toLowerCase()}`, configFact), supervisorPid: process.pid, supervisorStartIdentity: PROCESS_START_IDENTITY, childPid: process.pid + 1, childStartIdentity: `child-${stage}`, mailboxHash: H("6"), heartbeatAt: NOW, completionId: null, resultHash: null };
    const dispatchedEffect = lifecycleState.effects[launchEffectId]; const launchObservationCore = { kind: "worker_launch_observation", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, ownerEpoch: lifecycleState.owner.ownerEpoch, effectId: launchEffectId, requestHash: dispatchedEffect.requestHash, launchIntentId, launchKey: launchIntent.launchKey, workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: 1, attemptNonce, configHash, supervisorPid: binding.supervisorPid, supervisorStartIdentity: binding.supervisorStartIdentity, reconciliation: "applied_exact", observedAt: NOW }; const launchObservation = { ...launchObservationCore, hash: canonicalHash(launchObservationCore) }; lifecycleContext.facts[launchObservation.hash] = launchObservation;
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "bind_worker_attempt", { stageAttemptId: attemptId, binding, launchObservation: lifecycleRef("worker_launch_observation", `launch-${stage.toLowerCase()}`, launchObservation) }, { kind: "observation", commandId: `lifecycle-${stage}-bind`, idempotencyKey: `lifecycle-${stage}-bind` }), lifecycleContext); assert.equal(lifecycleTransition.accepted, true, `${stage} binds exact worker identity: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
    const exactF5ReadOnlyOutput = stage === "F5" ? { ...exactWorkerGitOutput(lifecycleCandidate.git, lifecycleCandidate.git.commit, lifecycleCandidate.git.tree), outputCommonDirIdentityHash: canonicalHash({ lifecycle: "common-dir" }), outputWorktreeIdentityHash: canonicalHash({ stage, attemptId, worktree: true }) } : null;
    const resultCore = { kind: "worker_result", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, launchIntentId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, completionId: `completion-${stage.toLowerCase()}`, terminalStatus: "succeeded", ...(["F1", "F3"].includes(stage) ? exactWorkerGitOutput(stage === "F1" ? plan.repositories[0].baseline : lifecycleCandidate.git, O("c"), O("d")) : exactF5ReadOnlyOutput ?? noWorkerGitOutput()) };
    workerResult = { ...resultCore, hash: canonicalHash(resultCore) }; const resultReference = lifecycleRef("worker_result", workerResult.completionId, workerResult); lifecycleContext.facts[workerResult.hash] = workerResult;
    if (stage === "F1") runningF1Fixture = { state: clone(lifecycleState), context: { ...lifecycleContext, facts: { ...lifecycleContext.facts } }, result: clone(resultReference) };
    if (["F2", "F5"].includes(stage)) {
      const divergentCore = { ...resultCore, ...exactWorkerGitOutput(lifecycleCandidate.git, O("e"), O("f")) }; const divergentResult = { ...divergentCore, hash: canonicalHash(divergentCore) }; lifecycleContext.facts[divergentResult.hash] = divergentResult;
      const divergentInput = reducerInput(lifecycleState, "record_worker_result", { stageAttemptId: attemptId, result: lifecycleRef("worker_result", `divergent-${stage.toLowerCase()}`, divergentResult) }, { kind: "observation", commandId: `lifecycle-${stage}-divergent-result`, idempotencyKey: `lifecycle-${stage}-divergent-result` });
      assert.equal(reduceDagRunV1(lifecycleState, divergentInput, lifecycleContext).accepted, false, `${stage} reducer ingest rejects successful read-only output that diverges from the immutable input candidate boundary`);
      if (stage === "F2") {
        const failedDivergentCore = { ...divergentCore, completionId: "completion-f2-failed-divergent", terminalStatus: "failed" }; const failedDivergent = { ...failedDivergentCore, hash: canonicalHash(failedDivergentCore) }; lifecycleContext.facts[failedDivergent.hash] = failedDivergent;
        assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_worker_result", { stageAttemptId: attemptId, result: lifecycleRef("worker_result", "failed-divergent-f2", failedDivergent) }, { kind: "observation", commandId: "lifecycle-f2-failed-divergent-result", idempotencyKey: "lifecycle-f2-failed-divergent-result" }), lifecycleContext).accepted, false, "failed F2 output cannot claim a changed authoritative candidate");
      }
    }
    if (stage === "F1") {
      const wrongResultCore = { ...resultCore, attemptNonce: "wrong-nonce-0123456789" }; const wrongResult = { ...wrongResultCore, hash: canonicalHash(wrongResultCore) }; lifecycleContext.facts[wrongResult.hash] = wrongResult;
      assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_worker_result", { stageAttemptId: attemptId, result: lifecycleRef("worker_result", "wrong-result", wrongResult) }, { kind: "observation", commandId: "lifecycle-wrong-result", idempotencyKey: "lifecycle-wrong-result" }), lifecycleContext).accepted, false, "wrong worker identity/result fails closed");
      const missingResultRef = { ...resultReference, hash: H("9") }; assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_worker_result", { stageAttemptId: attemptId, result: missingResultRef }, { kind: "observation", commandId: "lifecycle-missing-result", idempotencyKey: "lifecycle-missing-result" }), lifecycleContext).accepted, false, "missing immutable result fact fails closed");
    }
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_worker_result", { stageAttemptId: attemptId, result: resultReference }, { kind: "observation", commandId: `lifecycle-${stage}-result`, idempotencyKey: `lifecycle-${stage}-result` }), lifecycleContext); assert.equal(lifecycleTransition.accepted, true, `${stage} records exact terminal worker result`); lifecycleState = lifecycleTransition.state;
    assert.equal(lifecycleState.workItems["item-api"].stages[stage].state, "active", "generic worker success alone never advances a stage");
  }
  if (stage === "F1") {
    const candidateCore = { kind: "candidate", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", generation: 1, candidateId: "lifecycle-candidate", base: plan.repositories[0].baseline, git: { repositoryId: "repo-main", commit: O("c"), tree: O("d") }, patchIdentityHash: H("7"), producedByStageAttemptId: attemptId, lineageHash: lifecycleState.workItems["item-api"].implementationLineageHash };
    const badGenerationCore = { ...candidateCore, generation: 2 }; const badGeneration = { ...badGenerationCore, hash: canonicalHash(badGenerationCore) }; lifecycleContext.facts[badGeneration.hash] = badGeneration;
    assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_candidate", { stageAttemptId: attemptId, candidate: lifecycleRef("candidate", "bad-generation", badGeneration) }, { kind: "observation", commandId: "lifecycle-bad-candidate", idempotencyKey: "lifecycle-bad-candidate" }), lifecycleContext).accepted, false, "wrong candidate generation fails closed");
    lifecycleCandidate = { ...candidateCore, hash: canonicalHash(candidateCore) }; lifecycleContext.facts[lifecycleCandidate.hash] = lifecycleCandidate;
  }
  if (stage === "F3") {
    const priorCandidate = lifecycleCandidate; const priorF2Evidence = lifecycleState.workItems["item-api"].stages.F2.currentEvidence; const priorF2Fact = lifecycleContext.facts[priorF2Evidence];
    const candidateCore = { kind: "candidate", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", generation: 2, candidateId: "lifecycle-candidate-f3", base: priorCandidate.git, git: { repositoryId: "repo-main", commit: O("c"), tree: O("d") }, patchIdentityHash: priorCandidate.patchIdentityHash, producedByStageAttemptId: attemptId, lineageHash: lifecycleState.workItems["item-api"].implementationLineageHash };
    const nextCandidate = { ...candidateCore, hash: canonicalHash(candidateCore) }; lifecycleContext.facts[nextCandidate.hash] = nextCandidate;
    const deltaProcedure = Object.values(lifecycleContext.catalog.procedures).find(({ purpose }) => purpose === "evidence_only_delta_attestation");
    const executionCore = { kind: "procedure_execution", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, workItemId: "item-api", stage: "F3", stageAttemptId: attemptId, attemptInputHash: inputFact.hash, fromCandidateGeneration: 1, fromCandidateHash: priorCandidate.hash, toCandidateGeneration: 2, toCandidateHash: nextCandidate.hash, procedureHash: deltaProcedure.hash, environmentProfileHash: deltaProcedure.environmentProfileHash, executableArtifactHash: deltaProcedure.executable.executableArtifactHash, environmentHash: deltaProcedure.executable.environmentHash, executionId: "lifecycle-f3-delta-execution", disposition: "PASS", startedAt: NOW, completedAt: NOW, occurredAt: NOW };
    const execution = { ...executionCore, hash: canonicalHash(executionCore) }; lifecycleContext.facts[execution.hash] = execution;
    const adoptionCore = { kind: "adoption", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage: "F2", fromCandidateGeneration: 1, fromCandidateHash: priorCandidate.hash, toCandidateGeneration: 2, toCandidateHash: nextCandidate.hash, f3StageAttemptId: attemptId, evidenceHash: priorF2Evidence, sourceEvidenceProcedureHash: priorF2Fact.procedureHash, deltaAttestationProcedureHash: deltaProcedure.hash, environmentProfileHash: priorF2Fact.environmentProfileHash, deltaAttestationExecutionHash: execution.hash, occurredAt: NOW, evidenceOnlyDelta: true };
    const adoption = { ...adoptionCore, hash: canonicalHash(adoptionCore) }; lifecycleContext.facts[adoption.hash] = adoption;
    const candidateReference = lifecycleRef("candidate", nextCandidate.candidateId, nextCandidate); const adoptionReference = lifecycleRef("adoption", "lifecycle-f2-adoption", adoption); const executionReference = lifecycleRef("procedure_execution", execution.executionId, execution);
    const forgedWithoutExecution = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_candidate", { stageAttemptId: attemptId, candidate: candidateReference, f2Transition: adoptionReference }, { kind: "observation", commandId: "lifecycle-f3-forged-adoption", idempotencyKey: "lifecycle-f3-forged-adoption" }), lifecycleContext);
    assert.equal(forgedWithoutExecution.accepted, false, "F3 adoption cannot use a mapping/self-hash without exact executable delta execution");
    const futureExecutionCore = { ...executionCore, executionId: "lifecycle-f3-future-execution", startedAt: "2099-01-01T00:00:00.000Z", completedAt: "2099-01-01T00:00:01.000Z", occurredAt: "2099-01-01T00:00:02.000Z" }; const futureExecution = { ...futureExecutionCore, hash: canonicalHash(futureExecutionCore) }; const futureAdoptionCore = { ...adoptionCore, deltaAttestationExecutionHash: futureExecution.hash, occurredAt: futureExecution.occurredAt }; const futureAdoption = { ...futureAdoptionCore, hash: canonicalHash(futureAdoptionCore) }; lifecycleContext.facts[futureExecution.hash] = futureExecution; lifecycleContext.facts[futureAdoption.hash] = futureAdoption;
    const futureAdoptionResult = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_candidate", { stageAttemptId: attemptId, candidate: candidateReference, f2Transition: lifecycleRef("adoption", "lifecycle-f2-future-adoption", futureAdoption), procedureExecution: lifecycleRef("procedure_execution", futureExecution.executionId, futureExecution) }, { kind: "observation", commandId: "lifecycle-f3-future-adoption", idempotencyKey: "lifecycle-f3-future-adoption" }), lifecycleContext);
    assert.equal(futureAdoptionResult.accepted, false, "future delta observations cannot adopt F2 evidence");
    lifecycleTransition = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "record_candidate", { stageAttemptId: attemptId, candidate: candidateReference, f2Transition: adoptionReference, procedureExecution: executionReference }, { kind: "observation", commandId: "lifecycle-f3-candidate", idempotencyKey: "lifecycle-f3-candidate" }), lifecycleContext);
    assert.equal(lifecycleTransition.accepted, true, `F3 candidate/adoption/executable PASS delta execution ingest atomically: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state; lifecycleCandidate = nextCandidate;
  }
  const procedure = Object.values(lifecycleContext.catalog.procedures).find((entry) => entry.purpose === "lifecycle" && entry.stages.includes(stage)); const assertions = [];
  const evidenceGeneration = stage === "F0" ? 0 : stage === "F1" ? 1 : lifecycleState.workItems["item-api"].candidateGeneration;
  let materialization = null; let environmentObservation = null;
  if (["F2", "F5", "F7"].includes(stage)) {
    const commonDirIdentityHash = canonicalHash({ lifecycle: "common-dir" }); const worktreeIdentityHash = canonicalHash({ stage, attemptId, worktree: true });
    const materializationCore = { kind: "workspace_materialization", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stageAttemptId: attemptId, repositoryId: "repo-main", candidateGeneration: evidenceGeneration, candidateHash: lifecycleCandidate.hash, candidateTree: lifecycleCandidate.git, commonDirIdentityHash, worktreeIdentityHash, materializedAt: NOW };
    materialization = { ...materializationCore, hash: canonicalHash(materializationCore) }; lifecycleContext.facts[materialization.hash] = materialization;
    const observationCore = { kind: "environment_observation", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, repositoryId: "repo-main", candidateGeneration: evidenceGeneration, candidateHash: lifecycleCandidate.hash, candidateTree: lifecycleCandidate.git, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: materialization.hash, commonDirIdentityHash, worktreeIdentityHash, cleanliness: "clean", observedAt: NOW };
    environmentObservation = { ...observationCore, hash: canonicalHash(observationCore) }; lifecycleContext.facts[environmentObservation.hash] = environmentObservation;
  }
  if (stage === "F2") { const expected = plan.acceptanceOracles[0].assertions[0]; const oracleCore = { kind: "oracle_assertion", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage: "F2", stageAttemptId: attemptId, attemptInputHash: inputFact.hash, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, oracleId: "oracle-api", assertionId: expected.assertionId, procedureId: expected.procedureId, environmentProfileId: expected.environmentProfileId, observationMethod: expected.observationMethod, requiredEvidenceClass: expected.requiredEvidenceClass, disposition: "PASS", observationHash: workerResult.hash }; const oracle = { ...oracleCore, hash: canonicalHash(oracleCore) }; lifecycleContext.facts[oracle.hash] = oracle; assertions.push({ oracleId: oracle.oracleId, assertionId: oracle.assertionId, evidenceHash: oracle.hash, reference: lifecycleRef("oracle_assertion", oracle.assertionId, oracle) }); }
  const executions = [];
  if (stage === "F2") { const executionCore = { kind: "check_execution", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, candidateGeneration: evidenceGeneration, candidateHash: lifecycleCandidate.hash, checkId: "check-api", procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation.hash, executionId: `execution-${attemptId}`, disposition: "PASS", startedAt: NOW, completedAt: NOW }; const execution = { ...executionCore, hash: canonicalHash(executionCore) }; lifecycleContext.facts[execution.hash] = execution; executions.push(execution); }
  const aggregateCore = { kind: "check_aggregate", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: "PASS", oracleIds: ["oracle-api"], assertions: assertions.map(({ reference, ...assertion }) => assertion), checks: stage === "F2" ? [{ checkId: "check-api", disposition: "PASS", executionEvidenceHash: executions[0].hash, applicabilityEvidenceHashes: [] }] : [] };
  const aggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) }; lifecycleContext.facts[aggregate.hash] = aggregate;
  const evidenceCore = { kind: "stage_evidence", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, authorizationSetHash: lifecycleState.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: aggregate.hash, findingHashes: [], effectReconciliationHashes: stageEffectReconciliations.map(({ hash }) => hash), candidateGeneration: evidenceGeneration, candidateHash: stage === "F0" ? null : lifecycleCandidate?.hash ?? null, producerKind, producerResultHash: workerResult?.hash ?? null, disposition: "PASS", environmentObservationHash: environmentObservation?.hash ?? null, producedAt: NOW, readOnly: procedure.readOnly };
  const evidence = { ...evidenceCore, hash: canonicalHash(evidenceCore) }; lifecycleContext.facts[evidence.hash] = evidence;
  const sealBase = { stageAttemptId: attemptId, evidence: lifecycleRef("stage_evidence", `evidence-${stage}`, evidence), checkAggregate: lifecycleRef("check_aggregate", `aggregate-${stage}`, aggregate), oracleAssertions: assertions.map(({ reference }) => reference), checkDispositions: [], checkExecutions: executions.map((execution) => lifecycleRef("check_execution", execution.executionId, execution)), checkAuthorities: [], effectReconciliations: stageEffectReconciliations, ...(environmentObservation ? { environmentObservation: lifecycleRef("environment_observation", attemptId, environmentObservation), workspaceMaterialization: lifecycleRef("workspace_materialization", attemptId, materialization) } : {}) };
  if (stage === "F2") {
    const futureMaterializationCore = { ...materialization, materializedAt: "2099-01-01T00:00:00.000Z" }; delete futureMaterializationCore.hash; const futureMaterialization = { ...futureMaterializationCore, hash: canonicalHash(futureMaterializationCore) };
    const futureObservationCore = { ...environmentObservation, workspaceMaterializationHash: futureMaterialization.hash, observedAt: "2099-01-01T00:00:01.000Z" }; delete futureObservationCore.hash; const futureObservation = { ...futureObservationCore, hash: canonicalHash(futureObservationCore) };
    const futureExecutionCore = { ...executions[0], environmentObservationHash: futureObservation.hash, startedAt: "2099-01-01T00:00:01.000Z", completedAt: "2099-01-01T00:00:02.000Z" }; delete futureExecutionCore.hash; const futureExecution = { ...futureExecutionCore, hash: canonicalHash(futureExecutionCore) };
    const futureAggregateCore = { ...aggregate, checks: [{ ...aggregate.checks[0], executionEvidenceHash: futureExecution.hash }] }; delete futureAggregateCore.hash; const futureAggregate = { ...futureAggregateCore, hash: canonicalHash(futureAggregateCore) };
    const futureEvidenceCore = { ...evidence, checkAggregateHash: futureAggregate.hash, environmentObservationHash: futureObservation.hash }; delete futureEvidenceCore.hash; const futureEvidence = { ...futureEvidenceCore, hash: canonicalHash(futureEvidenceCore) }; Object.assign(lifecycleContext.facts, { [futureMaterialization.hash]: futureMaterialization, [futureObservation.hash]: futureObservation, [futureExecution.hash]: futureExecution, [futureAggregate.hash]: futureAggregate, [futureEvidence.hash]: futureEvidence });
    const futureSeal = { ...sealBase, evidence: lifecycleRef("stage_evidence", "future-evidence-f2", futureEvidence), checkAggregate: lifecycleRef("check_aggregate", "future-aggregate-f2", futureAggregate), checkExecutions: [lifecycleRef("check_execution", futureExecution.executionId, futureExecution)], environmentObservation: lifecycleRef("environment_observation", attemptId, futureObservation), workspaceMaterialization: lifecycleRef("workspace_materialization", attemptId, futureMaterialization) };
    assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_stage_attempt", futureSeal, { kind: "observation", commandId: "lifecycle-f2-future-observation", idempotencyKey: "lifecycle-f2-future-observation" }), lifecycleContext).accepted, false, "future workspace/environment/check observations cannot become stage evidence");
    for (const disposition of ["FAIL", "BLOCKED", "BUDGET_EXHAUSTED"]) {
      const suffix = disposition.toLowerCase().replaceAll("_", "-");
      const nonPassExecutionCore = { ...executions[0], executionId: `execution-f2-${suffix}`, disposition }; delete nonPassExecutionCore.hash;
      const nonPassExecution = { ...nonPassExecutionCore, hash: canonicalHash(nonPassExecutionCore) }; lifecycleContext.facts[nonPassExecution.hash] = nonPassExecution;
      const nonPassAggregateCore = { ...aggregate, disposition, checks: [{ ...aggregate.checks[0], disposition, executionEvidenceHash: nonPassExecution.hash }] }; delete nonPassAggregateCore.hash;
      const nonPassAggregate = { ...nonPassAggregateCore, hash: canonicalHash(nonPassAggregateCore) }; lifecycleContext.facts[nonPassAggregate.hash] = nonPassAggregate;
      const nonPassEvidenceCore = { ...evidence, checkAggregateHash: nonPassAggregate.hash, disposition }; delete nonPassEvidenceCore.hash;
      const nonPassEvidence = { ...nonPassEvidenceCore, hash: canonicalHash(nonPassEvidenceCore) }; lifecycleContext.facts[nonPassEvidence.hash] = nonPassEvidence;
      const nonPassSeal = { ...sealBase, evidence: lifecycleRef("stage_evidence", `evidence-f2-${suffix}`, nonPassEvidence), checkAggregate: lifecycleRef("check_aggregate", `aggregate-f2-${suffix}`, nonPassAggregate), checkExecutions: [lifecycleRef("check_execution", nonPassExecution.executionId, nonPassExecution)] };
      const unknownProcedureHash = H("0");
      const unknownExecutionCore = { ...nonPassExecution, procedureHash: unknownProcedureHash, executionId: `execution-f2-${suffix}-noncatalog` }; delete unknownExecutionCore.hash;
      const unknownExecution = { ...unknownExecutionCore, hash: canonicalHash(unknownExecutionCore) }; lifecycleContext.facts[unknownExecution.hash] = unknownExecution;
      const unknownAggregateCore = { ...nonPassAggregate, procedureHash: unknownProcedureHash, checks: [{ ...nonPassAggregate.checks[0], executionEvidenceHash: unknownExecution.hash }] }; delete unknownAggregateCore.hash;
      const unknownAggregate = { ...unknownAggregateCore, hash: canonicalHash(unknownAggregateCore) }; lifecycleContext.facts[unknownAggregate.hash] = unknownAggregate;
      const unknownEvidenceCore = { ...nonPassEvidence, procedureHash: unknownProcedureHash, checkAggregateHash: unknownAggregate.hash }; delete unknownEvidenceCore.hash;
      const unknownEvidence = { ...unknownEvidenceCore, hash: canonicalHash(unknownEvidenceCore) }; lifecycleContext.facts[unknownEvidence.hash] = unknownEvidence;
      const unknownSeal = { ...nonPassSeal, evidence: lifecycleRef("stage_evidence", `evidence-f2-${suffix}-noncatalog`, unknownEvidence), checkAggregate: lifecycleRef("check_aggregate", `aggregate-f2-${suffix}-noncatalog`, unknownAggregate), checkExecutions: [lifecycleRef("check_execution", unknownExecution.executionId, unknownExecution)] };
      assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_stage_attempt", unknownSeal, { kind: "observation", commandId: `lifecycle-f2-${suffix}-noncatalog`, idempotencyKey: `lifecycle-f2-${suffix}-noncatalog` }), lifecycleContext).accepted, false, `noncatalog ${disposition} lifecycle procedure rejects at reducer ingestion`);
      const sealedNonPass = reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_stage_attempt", nonPassSeal, { kind: "observation", commandId: `lifecycle-f2-${suffix}-seal`, idempotencyKey: `lifecycle-f2-${suffix}-seal` }), lifecycleContext);
      assert.equal(sealedNonPass.accepted, true, `reducer seals exact catalog-bound ${disposition} F2 authority: ${JSON.stringify(sealedNonPass)}`);
      if (sealedNonPass.accepted) {
        const sealedContext = { ...lifecycleContext, facts: { ...lifecycleContext.facts } };
        assert.equal(validateDagRunStateV1(sealedNonPass.state, sealedContext).ok, true, `reducer-sealed ${disposition} F2 snapshot validates before forgery`);
        nonPassFixtures.push({ disposition, state: sealedNonPass.state, context: sealedContext, evidenceHash: nonPassEvidence.hash, aggregateHash: nonPassAggregate.hash, executionHash: nonPassExecution.hash });
      }
    }
  }
  if (stage === "F5") {
    const divergentMaterializationCore = { ...materialization, commonDirIdentityHash: H("e"), worktreeIdentityHash: H("f") }; delete divergentMaterializationCore.hash;
    const divergentMaterialization = { ...divergentMaterializationCore, hash: canonicalHash(divergentMaterializationCore) }; lifecycleContext.facts[divergentMaterialization.hash] = divergentMaterialization;
    const divergentObservationCore = { ...environmentObservation, workspaceMaterializationHash: divergentMaterialization.hash, commonDirIdentityHash: divergentMaterialization.commonDirIdentityHash, worktreeIdentityHash: divergentMaterialization.worktreeIdentityHash }; delete divergentObservationCore.hash;
    const divergentObservation = { ...divergentObservationCore, hash: canonicalHash(divergentObservationCore) }; lifecycleContext.facts[divergentObservation.hash] = divergentObservation;
    const divergentEvidenceCore = { ...evidence, environmentObservationHash: divergentObservation.hash }; delete divergentEvidenceCore.hash;
    const divergentEvidence = { ...divergentEvidenceCore, hash: canonicalHash(divergentEvidenceCore) }; lifecycleContext.facts[divergentEvidence.hash] = divergentEvidence;
    const divergentSeal = { ...sealBase, evidence: lifecycleRef("stage_evidence", "evidence-f5-divergent-materialization", divergentEvidence), environmentObservation: lifecycleRef("environment_observation", attemptId, divergentObservation), workspaceMaterialization: lifecycleRef("workspace_materialization", attemptId, divergentMaterialization) };
    assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_stage_attempt", divergentSeal, { kind: "observation", commandId: "lifecycle-f5-divergent-materialization", idempotencyKey: "lifecycle-f5-divergent-materialization" }), lifecycleContext).accepted, false, "F5 seal ingest rejects worker Git identities that diverge from the exact materialization/environment authority");
  }
  if (stage === "F0") { const bogusReadyCore = { kind: "integration_ready", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", candidateGeneration: 0, candidateHash: H("8"), f8EvidenceHash: evidence.hash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true }; const bogusReady = { ...bogusReadyCore, hash: canonicalHash(bogusReadyCore) }; lifecycleContext.facts[bogusReady.hash] = bogusReady; const noChainPayload = { ...sealBase, integrationReady: lifecycleRef("integration_ready", "bogus-ready", bogusReady) }; firstF8Payload = noChainPayload; assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_f8_integration_ready", noChainPayload, { kind: "observation", commandId: "lifecycle-f8-no-chain", idempotencyKey: "lifecycle-f8-no-chain" }), lifecycleContext).accepted, false, "F8 readiness cannot bypass the F0-F7 chain/current F8 attempt"); }
  if (stage === "F1") {
    assert.equal(reduceDagRunV1(lifecycleState, reducerInput(lifecycleState, "seal_stage_attempt", sealBase, { kind: "observation", commandId: "lifecycle-f1-without-candidate", idempotencyKey: "lifecycle-f1-without-candidate" }), lifecycleContext).accepted, false, "F1 cannot pass on generic success without a candidate");
    const candidateInput = reducerInput(lifecycleState, "record_candidate", { stageAttemptId: attemptId, candidate: lifecycleRef("candidate", lifecycleCandidate.candidateId, lifecycleCandidate) }, { kind: "observation", commandId: "lifecycle-f1-candidate", idempotencyKey: "lifecycle-f1-candidate" });
    lifecycleTransition = reduceDagRunV1(lifecycleState, candidateInput, lifecycleContext); assert.equal(lifecycleTransition.accepted, true, `F1 candidate records: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
    assert.equal(reduceDagRunV1(lifecycleState, candidateInput, lifecycleContext).duplicate, true, "candidate exact replay is idempotent before stale CAS");
    const conflictingCandidate = { ...candidateInput, payloadHash: canonicalHash({ ...candidateInput.payload, candidate: { ...candidateInput.payload.candidate, id: "conflict" } }), payload: { ...candidateInput.payload, candidate: { ...candidateInput.payload.candidate, id: "conflict" } } }; assert.equal(reduceDagRunV1(lifecycleState, conflictingCandidate, lifecycleContext).accepted, false, "candidate natural-slot conflicting replay is rejected");
  }
  if (stage === "F8") { const readyCore = { kind: "integration_ready", planHash: plan.planHash, runId: lifecycleState.runId, runNonce: lifecycleState.runNonce, workItemId: "item-api", candidateGeneration: lifecycleState.workItems["item-api"].candidateGeneration, candidateHash: lifecycleCandidate.hash, f8EvidenceHash: evidence.hash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true }; const ready = { ...readyCore, hash: canonicalHash(readyCore) }; lifecycleContext.facts[ready.hash] = ready; Object.assign(sealBase, { integrationReady: lifecycleRef("integration_ready", "item-api", ready) }); }
  const sealType = stage === "F8" ? "seal_f8_integration_ready" : "seal_stage_attempt"; const sealInput = reducerInput(lifecycleState, sealType, sealBase, { kind: "observation", commandId: `lifecycle-${stage}-seal`, idempotencyKey: `lifecycle-${stage}-seal` });
  lifecycleTransition = reduceDagRunV1(lifecycleState, sealInput, lifecycleContext);
  assert.equal(lifecycleTransition.accepted, true, `${stage} seals only from exact immutable aggregate/evidence: ${JSON.stringify(lifecycleTransition)}`); lifecycleState = lifecycleTransition.state;
  assert.equal(reduceDagRunV1(lifecycleState, sealInput, lifecycleContext).duplicate, true, `${stage} exact seal replay is a no-op before stale CAS`);
  if (stage === "F0") {
    const conflictingSealPayload = { ...sealInput.payload, evidence: { ...sealInput.payload.evidence, id: "conflicting-evidence-ref" } }; const conflictingSeal = { ...sealInput, payload: conflictingSealPayload, payloadHash: canonicalHash(conflictingSealPayload) }; assert.equal(reduceDagRunV1(lifecycleState, conflictingSeal, lifecycleContext).accepted, false, "same stage/evidence natural seal slot rejects conflicting content");
    sealedF0State = lifecycleState; sealedF0Context = { ...lifecycleContext, facts: { ...lifecycleContext.facts } };
    assert.deepEqual(sealedF0State.workItems["item-api"].stages.F0.currentEvidence && sealedF0Context.facts[sealedF0State.workItems["item-api"].stages.F0.currentEvidence].effectReconciliationHashes, [sealedF0State.effects[lifecycleStageEffect.effectId].observationHash], "pre-seal effect reconciliation is included exactly in sealed stage evidence");
    const lateEffect = { ...lifecycleStageEffect, effectId: "lifecycle-f0-post-seal-effect", requestHash: H("0"), state: "intended", dispatchCount: 0, createdRevision: sealedF0State.revision + 1, lastDispatchAt: null, observationHash: null, reconciliation: "not_started" };
    const lateIntentInput = reducerInput(sealedF0State, "put_effect_intent", { effect: lateEffect }, { commandId: "lifecycle-f0-post-seal-intent", idempotencyKey: "lifecycle-f0-post-seal-intent" });
    assert.equal(reduceDagRunV1(sealedF0State, lateIntentInput, sealedF0Context).accepted, false, "post-seal exact-attempt effect intent is rejected");
    assert.equal(reduceDagRunV1(sealedF0State, reducerInput(sealedF0State, "mark_effect_dispatching", { effectId: lifecycleStageEffect.effectId, expectedDispatchCount: 1 }, { commandId: "lifecycle-f0-post-seal-dispatch", idempotencyKey: "lifecycle-f0-post-seal-dispatch" }), sealedF0Context).accepted, false, "post-seal exact-attempt effect cannot acquire new dispatch authority");
    const forgedLateIntent = clone(sealedF0State); forgedLateIntent.effects[lateEffect.effectId] = lateEffect;
    const lateSlotId = canonicalHash({ type: "put_effect_intent", naturalIdentity: lateEffect.effectId }); forgedLateIntent.idempotencySlots[lateSlotId] = { slotId: lateSlotId, inputType: "put_effect_intent", commandId: lateIntentInput.commandId, idempotencyKey: lateIntentInput.idempotencyKey, payloadHash: lateIntentInput.payloadHash, inputHash: canonicalHash(lateIntentInput), appliedRevision: lateEffect.createdRevision };
    forgedLateIntent.previousSnapshotHash = sealedF0State.snapshotHash; forgedLateIntent.revision = lateEffect.createdRevision; forgedLateIntent.current.updatedByCommandId = lateIntentInput.commandId; rehashRun(forgedLateIntent);
    expectInvalid((value) => validateDagRunStateV1(value, sealedF0Context), forgedLateIntent, "at-rest validation rejects a canonically slotted stage-bound intent first authorized after attempt seal");
    for (const [forgedStateValue, forgedReconciliation] of [["failed", "applied_exact"], ["failed", "conflict"], ["ambiguous", "unknown"], ["cancelled", "unknown"]]) {
      const forgedState = clone(sealedF0State); const forgedFacts = { ...sealedF0Context.facts };
      const forgedEffect = forgedState.effects[lifecycleStageEffect.effectId]; const oldObservation = forgedFacts[forgedEffect.observationHash];
      const forgedObservation = rehashFact({ ...oldObservation, reconciliation: forgedReconciliation }); forgedFacts[forgedObservation.hash] = forgedObservation;
      forgedEffect.state = forgedStateValue; forgedEffect.reconciliation = forgedReconciliation; forgedEffect.observationHash = forgedObservation.hash;
      const projection = forgedState.workItems["item-api"].stages.F0; const attempt = forgedState.stageAttempts[projection.currentAttemptId]; const oldEvidence = forgedFacts[projection.currentEvidence];
      const forgedEvidence = rehashFact({ ...oldEvidence, effectReconciliationHashes: [forgedObservation.hash] }); forgedFacts[forgedEvidence.hash] = forgedEvidence;
      attempt.evidence = lifecycleRef("stage_evidence", `forged-${forgedStateValue}-${forgedReconciliation}`, forgedEvidence); projection.currentEvidence = forgedEvidence.hash;
      forgedState.evidenceIndex.stageEvidence[forgedEvidence.hash] = clone(attempt.evidence); forgedState.evidenceIndex.effectReconciliations[forgedEffect.effectId] = lifecycleRef("effect_reconciliation", forgedEffect.effectId, forgedObservation); rehashRun(forgedState);
      expectInvalid((value) => validateDagRunStateV1(value, { ...sealedF0Context, facts: forgedFacts }), forgedState, `PASS stage rejects ${forgedStateValue}/${forgedReconciliation} effect closure forgery at rest`);
    }
  }
  const releasedReservation = lifecycleState.scheduler.reservations[reservation.reservationId]; assert.equal(releasedReservation.state, "released", `${stage} releases its reservation exactly`);
  assert(releasedReservation.leaseIds.every((leaseId) => lifecycleState.leases[leaseId].state === "released"), `${stage} releases every exact reservation lease`);
  assert.deepEqual(lifecycleState.workItems["item-api"].activeLeaseIds, [], `${stage} removes exactly its released leases from the work item`);
  for (const capacity of Object.values(lifecycleState.scheduler.operationalCapacities)) { assert.equal(capacity.allocatedUnits, 0, `${stage} release restores exact operational allocation`); assert(!capacity.reservationIds.includes(reservation.reservationId), `${stage} release removes its exact operational reservation identity`); }
}

const recordBlockingPlanFinding = (fixture, findingId, attemptId) => {
  const attempt = fixture.state.stageAttempts[attemptId];
  const evidenceHash = attempt.evidence?.hash ?? attempt.attemptInput.hash;
  const core = { kind: "finding", planHash: plan.planHash, runId: fixture.state.runId, runNonce: fixture.state.runNonce, authorizationSetHash: fixture.state.identity.authorizationSet.hash, findingId, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, evidenceHash, findingKind: "architecture_issue", severity: "blocking", materiality: "plan_affecting", fingerprint: canonicalHash({ findingId, fingerprint: true }), semanticSubjectId: "subject-api", observedAt: NOW };
  const fact = { ...core, hash: canonicalHash(core) };
  const context = { ...fixture.context, facts: { ...fixture.context.facts, [fact.hash]: fact } };
  const transition = reduceDagRunV1(fixture.state, reducerInput(fixture.state, "record_finding", { finding: lifecycleRef("finding", findingId, fact) }, { kind: "observation", commandId: `command-${findingId}`, idempotencyKey: `key-${findingId}` }), context);
  assert.equal(transition.accepted, true, `${findingId} records and holds atomically: ${JSON.stringify(transition)}`);
  return { state: transition.state, context, fact };
};

assert(reservedF1Fixture && pendingF0EffectFixture && runningF1Fixture, "replan fixtures cover reserved dispatch, intended effects, and a running worker");
const reservedF1Held = recordBlockingPlanFinding(reservedF1Fixture, "finding-replan-reserved-f1", "lifecycle-attempt-f0");
assert.equal(reservedF1Held.state.desired.run, "needs_replan", "blocking plan finding atomically changes desired run state");
assert.equal(reservedF1Held.state.current.run, "needs_replan", "blocking plan finding is immediately scheduler-visible");
const heldWorkerDecision = scheduleDagRunV1(plan, reservedF1Held.state);
assert.equal(heldWorkerDecision.notice, "NEEDS_REPLAN", "scheduler reports the whole-run replan hold");
assert.equal(heldWorkerDecision.selected.length, 0, "replan hold proposes no new worker or procedure reservation");
assert(heldWorkerDecision.frontier.every(({ blockerCodes }) => blockerCodes.includes("NEEDS_REPLAN")), "every runnable frontier slot exposes the replan blocker");
const heldReservation = reservedF1Fixture.reservation;
const heldSchedulerDispatch = reduceDagRunV1(reservedF1Held.state, reducerInput(reservedF1Held.state, "mark_scheduler_reservation_dispatch", { reservationId: heldReservation.reservationId, normalizedRequestHash: heldReservation.normalizedRequestHash }, { commandId: "command-held-f1-dispatch", idempotencyKey: "key-held-f1-dispatch" }), reservedF1Held.context);
assert.equal(heldSchedulerDispatch.accepted, false, "replan hold blocks dispatch of a previously reserved worker/procedure operation");

const pendingEffectHeld = recordBlockingPlanFinding(pendingF0EffectFixture, "finding-replan-pending-effect", "lifecycle-attempt-f0");
const heldEffectDispatch = reduceDagRunV1(pendingEffectHeld.state, reducerInput(pendingEffectHeld.state, "mark_effect_dispatching", { effectId: pendingF0EffectFixture.effectId, expectedDispatchCount: 0 }, { commandId: "command-held-effect-dispatch", idempotencyKey: "key-held-effect-dispatch" }), pendingEffectHeld.context);
assert.equal(heldEffectDispatch.accepted, false, "replan hold blocks new effect dispatch");

const runningWorkerHeld = recordBlockingPlanFinding(runningF1Fixture, "finding-replan-running-worker", "lifecycle-attempt-f1");
const runningSettlement = reduceDagRunV1(runningWorkerHeld.state, reducerInput(runningWorkerHeld.state, "record_worker_result", { stageAttemptId: "lifecycle-attempt-f1", result: runningF1Fixture.result }, { kind: "observation", commandId: "command-held-running-result", idempotencyKey: "key-held-running-result" }), runningWorkerHeld.context);
assert.equal(runningSettlement.accepted, true, "already-running current-generation worker may settle while held");
assert.equal(runningSettlement.accepted && runningSettlement.state.stageAttempts["lifecycle-attempt-f1"].state, "result_observed", "held running result is retained without dispatching successor work");
assert.equal(runningSettlement.accepted && runningSettlement.state.current.run, "needs_replan", "running settlement does not clear the whole-run hold");

const integrationHeld = recordBlockingPlanFinding({ state: lifecycleState, context: lifecycleContext }, "finding-replan-integration-ready", "lifecycle-attempt-f0");
const heldIntegrationDecision = scheduleDagRunV1(plan, integrationHeld.state);
assert.equal(heldIntegrationDecision.selected.length, 0, "integration-ready work cannot reserve or dispatch while replanning is required");
assert.equal(heldIntegrationDecision.frontier[0].operationKind, "integration", "held frontier remains visibly integration-ready");
assert(heldIntegrationDecision.frontier[0].blockerCodes.includes("NEEDS_REPLAN"), "integration readiness carries the whole-run replan blocker");

const dispositionFor = (held, disposition, suffix) => {
  const finding = held.state.findingClosures[held.fact.findingId];
  const core = { kind: "finding_resolution", planHash: plan.planHash, runId: held.state.runId, runNonce: held.state.runNonce, authorizationSetHash: held.state.identity.authorizationSet.hash, findingId: finding.findingId, findingHash: finding.findingHash, workItemId: finding.workItemId, stage: finding.stage, stageAttemptId: finding.stageAttemptId, attemptInputHash: finding.attemptInputHash, disposition, supersedingEvidenceHash: null, resolvedAt: NOW };
  const fact = { ...core, hash: canonicalHash(core) };
  const context = { ...held.context, facts: { ...held.context.facts, [fact.hash]: fact } };
  return reduceDagRunV1(held.state, reducerInput(held.state, "record_finding_resolution", { resolution: lifecycleRef("finding_resolution", finding.findingId, fact) }, { kind: "observation", commandId: `command-${suffix}`, idempotencyKey: `key-${suffix}` }), context);
};
const dismissedReplan = dispositionFor(integrationHeld, "dismissed", "replan-dismissed");
assert.equal(dismissedReplan.accepted, true, "explicit dismissed disposition clears a misclassified plan hold");
assert.equal(dismissedReplan.accepted && dismissedReplan.state.desired.run, "running", "dismissed finding resumes existing run intent");
assert.equal(dismissedReplan.accepted && dismissedReplan.state.current.run, "integration", "dismissed finding restores integration readiness");
assert.equal(dismissedReplan.accepted && scheduleDagRunV1(plan, dismissedReplan.state).selected[0]?.operationKind, "integration", "dismissed finding makes existing integration-ready work dispatchable again");
const misclassifiedReplan = dispositionFor(integrationHeld, "misclassified", "replan-misclassified");
assert.equal(misclassifiedReplan.accepted && misclassifiedReplan.state.current.run, "integration", "explicit misclassified disposition downgrades the finding and resumes the existing run");
const confirmedReplan = dispositionFor(integrationHeld, "successor_plan_required", "replan-confirmed");
assert.equal(confirmedReplan.accepted, true, "explicit confirmed semantic disposition is retained");
assert.equal(confirmedReplan.accepted && confirmedReplan.state.current.run, "needs_replan", "successor-required disposition remains held");
assert.equal(confirmedReplan.accepted && scheduleDagRunV1(plan, confirmedReplan.state).selected.length, 0, "confirmed successor requirement cannot resume current-plan dispatch");

assert.equal(nonPassFixtures.length, 3, "reducer produced FAIL/BLOCKED/BUDGET F2 snapshot fixtures");
for (const fixture of nonPassFixtures) {
  const closureForgeryState = clone(fixture.state); const closureForgeryFacts = { ...fixture.context.facts };
  const closureProjection = closureForgeryState.workItems["item-api"].stages.F2; const closureAttempt = closureForgeryState.stageAttempts[closureProjection.currentAttemptId];
  const closureEffect = { ...lifecycleStageEffect, effectId: `forged-${fixture.disposition.toLowerCase()}-effect`, boundStageAttemptId: closureAttempt.stageAttemptId, requestHash: canonicalHash({ fixture: fixture.disposition, conflict: true }), state: "failed", dispatchCount: 0, createdRevision: Math.max(1, closureForgeryState.revision - 1), lastDispatchAt: null, reconciliation: "conflict" };
  const conflictObservationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: closureForgeryState.runId, runNonce: closureForgeryState.runNonce, effectId: closureEffect.effectId, requestHash: closureEffect.requestHash, reconciliation: "conflict", closedAt: NOW };
  const conflictObservation = { ...conflictObservationCore, hash: canonicalHash(conflictObservationCore) }; closureForgeryFacts[conflictObservation.hash] = conflictObservation;
  closureEffect.observationHash = conflictObservation.hash; closureForgeryState.effects[closureEffect.effectId] = closureEffect;
  const conflictEvidence = rehashFact({ ...closureForgeryFacts[closureProjection.currentEvidence], effectReconciliationHashes: [conflictObservation.hash] }); closureForgeryFacts[conflictEvidence.hash] = conflictEvidence;
  closureAttempt.evidence = lifecycleRef("stage_evidence", `forged-${fixture.disposition.toLowerCase()}-effect-closure`, conflictEvidence); closureProjection.currentEvidence = conflictEvidence.hash;
  closureForgeryState.evidenceIndex.stageEvidence[conflictEvidence.hash] = clone(closureAttempt.evidence); closureForgeryState.evidenceIndex.effectReconciliations[closureEffect.effectId] = lifecycleRef("effect_reconciliation", closureEffect.effectId, conflictObservation); rehashRun(closureForgeryState);
  expectInvalid((value) => validateDagRunStateV1(value, { ...fixture.context, facts: closureForgeryFacts }), closureForgeryState, `${fixture.disposition} non-PASS stage rejects failed/conflict effect closure forgery at rest`);

  const forgedState = clone(fixture.state); const forgedFacts = { ...fixture.context.facts }; const unknownProcedureHash = H("0");
  const priorExecution = forgedFacts[fixture.executionHash]; const forgedExecutionCore = { ...priorExecution, procedureHash: unknownProcedureHash, executionId: `${priorExecution.executionId}-forged-noncatalog` }; delete forgedExecutionCore.hash;
  const forgedExecution = { ...forgedExecutionCore, hash: canonicalHash(forgedExecutionCore) }; forgedFacts[forgedExecution.hash] = forgedExecution;
  const priorAggregate = forgedFacts[fixture.aggregateHash]; const forgedAggregateCore = { ...priorAggregate, procedureHash: unknownProcedureHash, checks: [{ ...priorAggregate.checks[0], executionEvidenceHash: forgedExecution.hash }] }; delete forgedAggregateCore.hash;
  const forgedAggregate = { ...forgedAggregateCore, hash: canonicalHash(forgedAggregateCore) }; forgedFacts[forgedAggregate.hash] = forgedAggregate;
  const priorEvidence = forgedFacts[fixture.evidenceHash]; const forgedEvidenceCore = { ...priorEvidence, procedureHash: unknownProcedureHash, checkAggregateHash: forgedAggregate.hash }; delete forgedEvidenceCore.hash;
  const forgedEvidence = { ...forgedEvidenceCore, hash: canonicalHash(forgedEvidenceCore) }; forgedFacts[forgedEvidence.hash] = forgedEvidence;
  const attempt = forgedState.stageAttempts[forgedEvidence.stageAttemptId]; const stageProjection = forgedState.workItems[forgedEvidence.workItemId].stages[forgedEvidence.stage];
  attempt.evidence = lifecycleRef("stage_evidence", `forged-${fixture.disposition.toLowerCase()}-evidence`, forgedEvidence); stageProjection.currentEvidence = forgedEvidence.hash;
  forgedState.evidenceIndex.stageEvidence[forgedEvidence.hash] = clone(attempt.evidence);
  forgedState.evidenceIndex.checkAggregates[forgedAggregate.hash] = lifecycleRef("check_aggregate", `forged-${fixture.disposition.toLowerCase()}-aggregate`, forgedAggregate);
  forgedState.evidenceIndex.checkExecutions[forgedExecution.hash] = lifecycleRef("check_execution", forgedExecution.executionId, forgedExecution);
  rehashRun(forgedState);
  expectInvalid((value) => validateDagRunStateV1(value, { ...fixture.context, facts: forgedFacts }), forgedState, `forged self-consistent noncatalog ${fixture.disposition} snapshot fails at-rest lifecycle catalog validation`);
}
const nonPassEnvironmentFixture = nonPassFixtures.find(({ disposition }) => disposition === "FAIL");
assert(nonPassEnvironmentFixture, "reducer-produced non-PASS F2 environment fixture exists");
const forgeNonPassEnvironment = ({ removeEnvironment = false, divergentMaterialization = false, readOnly = null } = {}) => {
  const state = clone(nonPassEnvironmentFixture.state); const facts = { ...nonPassEnvironmentFixture.context.facts };
  const priorEvidence = facts[nonPassEnvironmentFixture.evidenceHash]; const priorAggregate = facts[nonPassEnvironmentFixture.aggregateHash]; const priorExecution = facts[nonPassEnvironmentFixture.executionHash];
  let environmentObservationHash = priorEvidence.environmentObservationHash;
  if (divergentMaterialization) {
    const priorObservation = facts[environmentObservationHash]; const priorMaterialization = facts[priorObservation.workspaceMaterializationHash];
    const materializationCore = { ...priorMaterialization, commonDirIdentityHash: H("8"), worktreeIdentityHash: H("9") }; delete materializationCore.hash;
    const materialization = { ...materializationCore, hash: canonicalHash(materializationCore) }; facts[materialization.hash] = materialization;
    const observationCore = { ...priorObservation, workspaceMaterializationHash: materialization.hash, commonDirIdentityHash: materialization.commonDirIdentityHash, worktreeIdentityHash: materialization.worktreeIdentityHash }; delete observationCore.hash;
    const observation = { ...observationCore, hash: canonicalHash(observationCore) }; facts[observation.hash] = observation; environmentObservationHash = observation.hash;
    state.evidenceIndex.workspaceMaterializations[materialization.hash] = lifecycleRef("workspace_materialization", materialization.stageAttemptId, materialization);
    state.evidenceIndex.environmentObservations[observation.hash] = lifecycleRef("environment_observation", observation.stageAttemptId, observation);
  }
  if (removeEnvironment) environmentObservationHash = null;
  const executionCore = { ...priorExecution, executionId: `${priorExecution.executionId}-${removeEnvironment ? "null-environment" : divergentMaterialization ? "divergent-materialization" : "read-only"}`, environmentObservationHash }; delete executionCore.hash;
  const execution = { ...executionCore, hash: canonicalHash(executionCore) }; facts[execution.hash] = execution;
  const aggregateCore = { ...priorAggregate, checks: [{ ...priorAggregate.checks[0], executionEvidenceHash: execution.hash }] }; delete aggregateCore.hash;
  const aggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) }; facts[aggregate.hash] = aggregate;
  const evidenceCore = { ...priorEvidence, checkAggregateHash: aggregate.hash, environmentObservationHash, ...(readOnly === null ? {} : { readOnly }) }; delete evidenceCore.hash;
  const evidence = { ...evidenceCore, hash: canonicalHash(evidenceCore) }; facts[evidence.hash] = evidence;
  const attempt = state.stageAttempts[evidence.stageAttemptId]; const projection = state.workItems[evidence.workItemId].stages[evidence.stage];
  attempt.evidence = lifecycleRef("stage_evidence", `forged-nonpass-${execution.executionId}`, evidence); projection.currentEvidence = evidence.hash;
  state.evidenceIndex.stageEvidence[evidence.hash] = clone(attempt.evidence); state.evidenceIndex.checkAggregates[aggregate.hash] = lifecycleRef("check_aggregate", `forged-${aggregate.hash.slice(7, 19)}`, aggregate); state.evidenceIndex.checkExecutions[execution.hash] = lifecycleRef("check_execution", execution.executionId, execution);
  rehashRun(state); return { state, context: { ...nonPassEnvironmentFixture.context, facts } };
};
const nullNonPassEnvironment = forgeNonPassEnvironment({ removeEnvironment: true });
expectInvalid((value) => validateDagRunStateV1(value, nullNonPassEnvironment.context), nullNonPassEnvironment.state, "non-PASS F2 cannot null its exact environment/materialization authority at rest");
const divergentNonPassMaterialization = forgeNonPassEnvironment({ divergentMaterialization: true });
expectInvalid((value) => validateDagRunStateV1(value, divergentNonPassMaterialization.context), divergentNonPassMaterialization.state, "non-PASS F2 cannot replace exact common-dir/worktree materialization identities at rest");
const writableNonPassEvidence = forgeNonPassEnvironment({ readOnly: false });
expectInvalid((value) => validateDagRunStateV1(value, writableNonPassEvidence.context), writableNonPassEvidence.state, "non-PASS F2 cannot bypass read-only procedure/output closure at rest");

const divergentF5State = clone(lifecycleState); const divergentF5Facts = { ...lifecycleContext.facts }; const divergentF5Attempt = divergentF5State.stageAttempts[divergentF5State.workItems["item-api"].stages.F5.currentAttemptId];
const priorF5Result = divergentF5Facts[divergentF5Attempt.workerResult.hash]; const divergentF5ResultCore = { ...priorF5Result, outputCommonDirIdentityHash: H("f") }; delete divergentF5ResultCore.hash;
const divergentF5Result = { ...divergentF5ResultCore, hash: canonicalHash(divergentF5ResultCore) }; divergentF5Facts[divergentF5Result.hash] = divergentF5Result;
divergentF5Attempt.workerResult = lifecycleRef("worker_result", divergentF5Result.completionId, divergentF5Result); divergentF5State.workerBindings[divergentF5Attempt.stageAttemptId].resultHash = divergentF5Result.hash; divergentF5State.evidenceIndex.workerResults[divergentF5Result.hash] = clone(divergentF5Attempt.workerResult);
const priorF5Evidence = divergentF5Facts[divergentF5Attempt.evidence.hash]; const divergentF5EvidenceCore = { ...priorF5Evidence, producerResultHash: divergentF5Result.hash }; delete divergentF5EvidenceCore.hash;
const divergentF5Evidence = { ...divergentF5EvidenceCore, hash: canonicalHash(divergentF5EvidenceCore) }; divergentF5Facts[divergentF5Evidence.hash] = divergentF5Evidence;
divergentF5Attempt.evidence = lifecycleRef("stage_evidence", "forged-f5-divergent-output", divergentF5Evidence); divergentF5State.workItems["item-api"].stages.F5.currentEvidence = divergentF5Evidence.hash; divergentF5State.evidenceIndex.stageEvidence[divergentF5Evidence.hash] = clone(divergentF5Attempt.evidence); rehashRun(divergentF5State);
expectInvalid((value) => validateDagRunStateV1(value, { ...lifecycleContext, facts: divergentF5Facts }), divergentF5State, "forged canonical F5 result cannot diverge from exact materialization common-dir authority at rest");
assert.equal(lifecycleState.workItems["item-api"].current, "integration_ready", "reducer-only F0-F8 chain reaches integration readiness");
assert.deepEqual(lifecycleState.integrationTrains["repo-main"].entryOrder, ["entry-000-item-api"], "F8 creates one pristine plan-ordered train entry atomically");
assert.equal(lifecycleState.integrationTrains["repo-main"].entries["entry-000-item-api"].state, "eligible", "plan head enqueue is pristine and eligible");
const reachableIntegration = scheduleDagRunV1(plan, lifecycleState); assert.equal(reachableIntegration.selected[0]?.operationKind, "integration", "F8 atomic enqueue makes scheduler integration reachable");
assert(firstAttemptState && firstF8Payload, "negative lifecycle fixtures were reducer-produced from genesis");
const firstF0Attempt = firstAttemptState.stageAttempts["lifecycle-attempt-f0"]; const firstF0Evidence = Object.values(lifecycleContext.facts).find((fact) => fact.kind === "stage_evidence" && fact.stageAttemptId === firstF0Attempt.stageAttemptId);
const findingCore = { kind: "finding", planHash: plan.planHash, runId: firstAttemptState.runId, runNonce: firstAttemptState.runNonce, authorizationSetHash: firstAttemptState.identity.authorizationSet.hash, findingId: "finding-resolution-probe", workItemId: "item-api", stage: "F0", stageAttemptId: firstF0Attempt.stageAttemptId, attemptInputHash: firstF0Attempt.attemptInput.hash, evidenceHash: firstF0Evidence.hash, findingKind: "product_defect", severity: "advisory", materiality: "local", fingerprint: H("1"), semanticSubjectId: "subject-api", observedAt: NOW }; const findingFact = { ...findingCore, hash: canonicalHash(findingCore) }; const findingContext = { ...lifecycleContext, facts: { ...lifecycleContext.facts, [findingFact.hash]: findingFact } };
const findingRecorded = reduceDagRunV1(firstAttemptState, reducerInput(firstAttemptState, "record_finding", { finding: lifecycleRef("finding", findingFact.findingId, findingFact) }, { kind: "observation", commandId: "command-finding-probe", idempotencyKey: "finding-probe" }), findingContext); assert.equal(findingRecorded.accepted, true, "finding probe records exact attempt-bound authority");
if (findingRecorded.accepted) {
  const crossItemCorrectionCore = { kind: "finding_correction", planHash: plan.planHash, runId: firstAttemptState.runId, runNonce: firstAttemptState.runNonce, authorizationSetHash: firstAttemptState.identity.authorizationSet.hash, findingId: findingFact.findingId, findingHash: findingFact.hash, workItemId: "item-other", stage: "F0", stageAttemptId: firstF0Attempt.stageAttemptId, attemptInputHash: firstF0Attempt.attemptInput.hash, candidateGeneration: 0, candidateHash: null, observedAt: NOW }; const crossItemCorrection = { ...crossItemCorrectionCore, hash: canonicalHash(crossItemCorrectionCore) };
  const resolutionCore = { kind: "finding_resolution", planHash: plan.planHash, runId: firstAttemptState.runId, runNonce: firstAttemptState.runNonce, authorizationSetHash: firstAttemptState.identity.authorizationSet.hash, findingId: findingFact.findingId, findingHash: findingFact.hash, workItemId: "item-api", stage: "F0", stageAttemptId: firstF0Attempt.stageAttemptId, attemptInputHash: firstF0Attempt.attemptInput.hash, disposition: "corrected", supersedingEvidenceHash: crossItemCorrection.hash, resolvedAt: NOW }; const resolutionFact = { ...resolutionCore, hash: canonicalHash(resolutionCore) }; const resolutionContext = { ...findingContext, facts: { ...findingContext.facts, [crossItemCorrection.hash]: crossItemCorrection, [resolutionFact.hash]: resolutionFact } };
  const crossItemResolution = reduceDagRunV1(findingRecorded.state, reducerInput(findingRecorded.state, "record_finding_resolution", { resolution: lifecycleRef("finding_resolution", findingFact.findingId, resolutionFact) }, { kind: "observation", commandId: "command-cross-item-resolution", idempotencyKey: "cross-item-resolution" }), resolutionContext);
  assert.equal(crossItemResolution.accepted, false, "cross-item correction evidence cannot resolve a finding");
}

const hydrationRoot = await mkdtemp(join(tmpdir(), "pi-dag-lifecycle-hydration-"));
try {
  const hydrationStore = new DagRunSnapshotStoreV1(hydrationRoot, lifecycleGenesis.runId); const hydrationContext = { ...runContext, normalizedSchedulerIndexHash: lifecycleSchedulerIndex.indexHash, facts: {} };
  const hydrationInitializationLock = { lockIdentity: H("3"), ownerTokenHash: H("4"), sessionId: "hydration-init", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, acquiredAt: NOW }; await seedBaselineFacts(hydrationStore); await hydrationStore.initialize(lifecycleGenesis, hydrationContext, hydrationInitializationLock); await hydrationStore.putImmutableFact(lifecycleOwnership);
  const hydrationLock = { ...lifecycleOwner, acquiredAt: NOW };
  let hydrationResult = await hydrationStore.mutate({ input: reducerInput(lifecycleGenesis, "attach_owner", { ...lifecycleOwner, ownershipReceipt: lifecycleOwnership.hash, priorOwnerDisposition: "absent" }, { kind: "observation", commandId: "hydration-attach", idempotencyKey: "hydration-attach" }), context: hydrationContext, lock: hydrationLock }); assert.equal(hydrationResult.accepted, true);
  let hydrationState = hydrationResult.state; const hydrationDecision = scheduleDagRunV1(plan, hydrationState); const hydrationReserve = { decisionHash: hydrationDecision.decisionHash, decisionSequence: hydrationDecision.decisionSequence, policyHash: hydrationDecision.policyHash, normalizedIndexHash: hydrationDecision.normalizedIndexHash, inputSnapshotHash: hydrationState.snapshotHash, reservations: hydrationDecision.selected, bypassSlotIds: hydrationDecision.bypassIncrements };
  hydrationResult = await hydrationStore.mutate({ input: reducerInput(hydrationState, "reserve_scheduler_batch", hydrationReserve, { commandId: "hydration-reserve", idempotencyKey: "hydration-reserve" }), context: hydrationContext, lock: hydrationLock }); hydrationState = hydrationResult.state; const hydrationReservation = hydrationDecision.selected[0];
  hydrationResult = await hydrationStore.mutate({ input: reducerInput(hydrationState, "mark_scheduler_reservation_dispatch", { reservationId: hydrationReservation.reservationId, normalizedRequestHash: hydrationReservation.normalizedRequestHash }, { commandId: "hydration-dispatch", idempotencyKey: "hydration-dispatch" }), context: hydrationContext, lock: hydrationLock }); hydrationState = hydrationResult.state;
  hydrationResult = await hydrationStore.mutate({ input: reducerInput(hydrationState, "record_scheduler_reservation_dispatch", { reservationId: hydrationReservation.reservationId, normalizedRequestHash: hydrationReservation.normalizedRequestHash, disposition: "active" }, { kind: "observation", commandId: "hydration-active", idempotencyKey: "hydration-active" }), context: hydrationContext, lock: hydrationLock }); hydrationState = hydrationResult.state;
  const hydrationInputCore = { kind: "stage_attempt_input", planHash: plan.planHash, runId: hydrationState.runId, runNonce: hydrationState.runNonce, workItemId: "item-api", stage: "F0", stageAttemptId: "hydration-attempt-f0", candidateGeneration: 0, candidateHash: null, authorizationSetHash: hydrationState.identity.authorizationSet.hash, producerKind: "conductor", implementationLineageHash: null }; const hydrationFact = { ...hydrationInputCore, hash: canonicalHash(hydrationInputCore) }; const hydrationReference = lifecycleRef("stage_attempt_input", "hydration-attempt-f0", hydrationFact); const hydrationBeginPayload = { reservationId: hydrationReservation.reservationId, stageAttemptId: "hydration-attempt-f0", attemptInput: hydrationReference, launchIntent: null, launchEffect: null };
  const missingHydrationPayload = { ...hydrationBeginPayload, attemptInput: { ...hydrationReference, hash: H("9") } }; await assert.rejects(() => hydrationStore.mutate({ input: reducerInput(hydrationState, "begin_stage_attempt", missingHydrationPayload, { commandId: "hydration-missing", idempotencyKey: "hydration-missing" }), context: hydrationContext, lock: hydrationLock }), DagRunStoreCorruptError, "store fails closed when an exact per-input new fact is missing");
  const storedHydrationFact = await hydrationStore.putImmutableFact(hydrationFact); const hydrationFactText = await readFile(storedHydrationFact.path, "utf8"); await writeFile(storedHydrationFact.path, "{corrupt");
  await assert.rejects(() => hydrationStore.mutate({ input: reducerInput(hydrationState, "begin_stage_attempt", hydrationBeginPayload, { commandId: "hydration-corrupt", idempotencyKey: "hydration-corrupt" }), context: hydrationContext, lock: hydrationLock }), DagRunStoreCorruptError, "store fails closed when exact per-input fact hydration is corrupt");
  await writeFile(storedHydrationFact.path, hydrationFactText);
  hydrationResult = await hydrationStore.mutate({ input: reducerInput(hydrationState, "begin_stage_attempt", hydrationBeginPayload, { commandId: "hydration-valid", idempotencyKey: "hydration-valid" }), context: hydrationContext, lock: hydrationLock });
  assert.equal(hydrationResult.accepted, true, "store hydrates exactly the input's newly referenced immutable fact without caller context injection");
} finally { await rm(hydrationRoot, { recursive: true, force: true }); }

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

const unboundWorkerProjection = { projectionHash: H("c"), workers: [{ storageId: "unbound", launchOwnerSessionId: "unbound-session", workerId: "generic-worker", attemptNumber: 1, attemptNonce: "0123456789abcdef", configHash: H("d"), terminalStatus: "succeeded", resultHash: H("e") }] };
const executionProjection = projectDagExecutionV1(plan, schedulableRun, schedulerDecision, unboundWorkerProjection);
const executionProjectionV2 = projectDagExecutionV2(plan, schedulableRun, schedulerDecision, unboundWorkerProjection);
assert.equal(executionProjection.nodes[0].glyph, ">", "execution projection derives ready glyph from canonical run state");
assert.equal(executionProjectionV2.kind, "DagExecutionProjectionV2", "widget projection is explicitly versioned instead of widening V1");
assert.deepEqual(executionProjectionV2.nodes[0].stages.map(({ stage }) => stage), PLAN_STAGE_IDS, "widget projection exposes ordered F0-F8 stage states");
assert.equal(executionProjectionV2.nodes[0].candidateGeneration, schedulableRun.workItems["item-api"].candidateGeneration, "widget projection exposes exact candidate generation");
assert.equal(executionProjection.nodes[0].stages, undefined, "legacy V1 projection remains unchanged");
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
      const workerStorageId = `storage-${prefix}`; const workerId = `worker-${prefix}-${stage.toLowerCase()}`; const attemptNonce = `nonce-${stage.toLowerCase()}-0123456789`; const launchKey = `launch-key-${prefix}-${stage.toLowerCase()}`; const configRequestHash = H("5");
      const config = { storageId: workerStorageId, ownerSessionId: state.owner.sessionId, workerId, attemptNumber: 1, attemptNonce, launchKey, requestHash: configRequestHash, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity } }; const configHash = canonicalHash(config); const configFactCore = { kind: "worker_config", configHash, config }; const configFact = { ...configFactCore, hash: canonicalHash(configFactCore) }; facts[configFact.hash] = configFact;
      const workerCore = { kind: "worker_result", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, launchIntentId: `launch-${prefix}-${stage.toLowerCase()}`, workerStorageId, launchOwnerSessionId: state.owner.sessionId, workerId, attemptNumber: 1, attemptNonce, configHash, completionId: `completion-${prefix}-${stage.toLowerCase()}`, terminalStatus: "succeeded", ...(["F1", "F3"].includes(stage) ? exactWorkerGitOutput(stage === "F1" ? fixturePlan.repositories[0].baseline : candidateFact.git, candidateFact.git.commit, candidateFact.git.tree) : noWorkerGitOutput()) };
      const workerFact = { ...workerCore, hash: canonicalHash(workerCore) }; facts[workerFact.hash] = workerFact; workerResultHash = workerFact.hash; workerResultRef = ref("worker_result", `result-${stage.toLowerCase()}`, workerFact.hash); state.evidenceIndex.workerResults[workerFact.hash] = workerResultRef;
      const effectId = `effect-launch-${prefix}-${stage.toLowerCase()}`; const effectRequestHash = canonicalHash({ stage, launch: true }); const supervisorPid = 1000 + PLAN_STAGE_IDS.indexOf(stage); const supervisorStartIdentity = `proc:${stage}:supervisor`;
      launchIntentId = `launch-${prefix}-${stage.toLowerCase()}`; const launchObservationCore = { kind: "worker_launch_observation", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, ownerEpoch: state.owner.ownerEpoch, effectId, requestHash: effectRequestHash, launchIntentId, launchKey, workerStorageId, launchOwnerSessionId: state.owner.sessionId, workerId, attemptNumber: 1, attemptNonce, configHash, supervisorPid, supervisorStartIdentity, reconciliation: "applied_exact", observedAt: NOW }; const launchObservation = { ...launchObservationCore, hash: canonicalHash(launchObservationCore) }; facts[launchObservation.hash] = launchObservation;
      state.effects[effectId] = { effectId, kind: "launch_worker", subject: { kind: "work_item", id: "item-api" }, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: effectRequestHash, boundOwnerEpoch: state.owner.ownerEpoch, boundAuthorizationSetHash: state.identity.authorizationSet.hash, boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: inputGeneration, boundGateEpochHash: H("3"), state: "reconciled", dispatchCount: 1, createdRevision: 0, createdAt: NOW, lastDispatchAt: NOW, observationHash: launchObservation.hash, reconciliation: "applied_exact", blockerId: null };
      state.launchIntents[launchIntentId] = { launchIntentId, effectId, stageAttemptId: attemptId, state: "closed", adapter: "owned-worker-v1", launchKey, workerId, expectedAttemptNumber: 1, taskPacketHash: H("4"), cwdRepositoryId: "repo-main", configRequestHash, dispatchCount: 1, lastDispatchAt: NOW, boundAt: NOW, ambiguityReason: null };
      state.workerBindings[attemptId] = { stageAttemptId: attemptId, launchIntentId, workerStorageId, launchOwnerSessionId: state.owner.sessionId, workerId, attemptNumber: 1, attemptNonce, configHash, configRef: { ...ref("worker_config", `config-${stage.toLowerCase()}`, configFact.hash), bytes: Buffer.byteLength(canonicalStringify(configFact)) }, supervisorPid, supervisorStartIdentity, childPid: 2000 + PLAN_STAGE_IDS.indexOf(stage), childStartIdentity: `proc:${stage}:child`, mailboxHash: H("6"), heartbeatAt: NOW, completionId: workerCore.completionId, resultHash: workerFact.hash };
    }
    const procedure = Object.values(procedures).find((value) => value.stages.includes(stage));
    const assertions = [];
    let environmentObservation = null;
    if (["F2", "F5", "F7"].includes(stage)) {
      const commonDirIdentityHash = canonicalHash({ fixture: prefix, commonDir: true });
      const worktreeIdentityHash = canonicalHash({ fixture: prefix, stage, attemptId });
      const materializationCore = { kind: "workspace_materialization", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stageAttemptId: attemptId, repositoryId: "repo-main", candidateGeneration: generation, candidateHash: candidateFact.hash, candidateTree: candidateFact.git, commonDirIdentityHash, worktreeIdentityHash, materializedAt: NOW };
      const materialization = { ...materializationCore, hash: canonicalHash(materializationCore) }; facts[materialization.hash] = materialization;
      state.evidenceIndex.workspaceMaterializations ??= {}; state.evidenceIndex.workspaceMaterializations[materialization.hash] = ref("workspace_materialization", attemptId, materialization.hash);
      const observationCore = { kind: "environment_observation", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, repositoryId: "repo-main", candidateGeneration: generation, candidateHash: candidateFact.hash, candidateTree: candidateFact.git, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: materialization.hash, commonDirIdentityHash, worktreeIdentityHash, cleanliness: "clean", observedAt: NOW };
      environmentObservation = { ...observationCore, hash: canonicalHash(observationCore) }; facts[environmentObservation.hash] = environmentObservation;
      state.evidenceIndex.environmentObservations ??= {}; state.evidenceIndex.environmentObservations[environmentObservation.hash] = ref("environment_observation", attemptId, environmentObservation.hash);
    }
    if (stage === "F2") {
      const assertion = fixturePlan.acceptanceOracles[0].assertions[0]; const oracleCore = { kind: "oracle_assertion", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage: "F2", stageAttemptId: attemptId, attemptInputHash: inputFact.hash, authorizationSetHash: state.identity.authorizationSet.hash, oracleId: fixturePlan.acceptanceOracles[0].oracleId, assertionId: assertion.assertionId, procedureId: assertion.procedureId, environmentProfileId: assertion.environmentProfileId, observationMethod: assertion.observationMethod, requiredEvidenceClass: assertion.requiredEvidenceClass, disposition: "PASS", observationHash: workerResultHash };
      const oracleFact = { ...oracleCore, hash: canonicalHash(oracleCore) }; facts[oracleFact.hash] = oracleFact; state.evidenceIndex.oracleAssertions[oracleFact.hash] = ref("oracle_assertion", "oracle-assert-integration", oracleFact.hash); assertions.push({ oracleId: oracleCore.oracleId, assertionId: oracleCore.assertionId, evidenceHash: oracleFact.hash });
    }
    let checkExecution = null;
    if (stage === "F2") { const executionCore = { kind: "check_execution", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, candidateGeneration: generation, candidateHash: candidateFact.hash, checkId: "check-api", procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation.hash, executionId: `execution-${attemptId}`, disposition: "PASS", startedAt: NOW, completedAt: NOW }; checkExecution = { ...executionCore, hash: canonicalHash(executionCore) }; facts[checkExecution.hash] = checkExecution; state.evidenceIndex.checkExecutions ??= {}; state.evidenceIndex.checkExecutions[checkExecution.hash] = ref("check_execution", checkExecution.executionId, checkExecution.hash); }
    const checks = stage === "F2" ? [{ checkId: "check-api", disposition: "PASS", executionEvidenceHash: checkExecution.hash, applicabilityEvidenceHashes: [] }] : [];
    const aggregateCatalogCore = { workItemId: "item-api", stage, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: "PASS", oracleIds: state.workItems["item-api"].planEntityHash ? fixturePlan.workItems[0].oracleIds : [], assertions, checks };
    const aggregateCatalogEntry = { ...aggregateCatalogCore, hash: canonicalHash(aggregateCatalogCore) }; checkAggregates[aggregateCatalogEntry.hash] = aggregateCatalogEntry;
    const aggregateCore = { kind: "check_aggregate", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: "PASS", oracleIds: aggregateCatalogCore.oracleIds, assertions, checks: aggregateCatalogCore.checks };
    const aggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) }; facts[aggregate.hash] = aggregate; state.evidenceIndex.checkAggregates[aggregate.hash] = ref("check_aggregate", `aggregate-${stage.toLowerCase()}`, aggregate.hash);
    const evidenceCore = { kind: "stage_evidence", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", stage, stageAttemptId: attemptId, attemptInputHash: inputFact.hash, authorizationSetHash: state.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: aggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration: stage === "F0" ? 0 : generation, candidateHash: stage === "F0" ? null : candidateFact.hash, producerKind, producerResultHash: workerResultHash, disposition: "PASS", environmentObservationHash: environmentObservation?.hash ?? null, producedAt: NOW, readOnly: procedure.readOnly };
    const evidenceFact = { ...evidenceCore, hash: canonicalHash(evidenceCore) }; facts[evidenceFact.hash] = evidenceFact; const evidenceRef = ref("stage_evidence", `evidence-${stage.toLowerCase()}`, evidenceFact.hash); state.evidenceIndex.stageEvidence[evidenceFact.hash] = evidenceRef;
    state.stageAttempts[attemptId] = { stageAttemptId: attemptId, workItemId: "item-api", stage, ordinal: 1, producerKind, implementationLineageHash, inputGeneration, reservedOutputGeneration: stage === "F1" ? generation : null, attemptInput: inputRef, authorizationSetHash: state.identity.authorizationSet.hash, state: "sealed", launchIntentId, leaseIds: [], workerResult: workerResultRef, evidence: evidenceRef, failure: null, createdAt: NOW, updatedAt: NOW, terminalAt: NOW };
    state.workItems["item-api"].stages[stage] = { stage, state: "passed", attemptIds: [attemptId], currentAttemptId: attemptId, currentEvidence: evidenceFact.hash, adoptionReceipt: null, invalidationIds: [], lastDisposition: "PASS", blockerIds: [] };
  }
  const f8EvidenceHash = state.workItems["item-api"].stages.F8.currentEvidence; const readyCore = { kind: "integration_ready", planHash: fixturePlan.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: "item-api", candidateGeneration: generation, candidateHash: candidateFact.hash, f8EvidenceHash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true };
  const readyFact = { ...readyCore, hash: canonicalHash(readyCore) }; facts[readyFact.hash] = readyFact; state.evidenceIndex.integrationReady["item-api"] = ref("integration_ready", "item-api", readyFact.hash);
  const item = state.workItems["item-api"]; item.current = "integrating"; item.currentStage = "F8"; item.integrationReadyReceipt = readyFact.hash; item.laneAdmissionSequence = 1; item.admittedAt = NOW;
  state.scheduler.activeNodeLanes["item-api"] = { workItemId: "item-api", admissionSequence: 1, admittedAt: NOW, releaseDisposition: null, releasedAt: null }; state.scheduler.nextReservationSequence = 2;
  state.current.run = "integration"; state.current.activeWorkItemIds = ["item-api"]; state.current.readyWorkItemIds = []; state.current.integrationReadyWorkItemIds = [];
  if (!preserveTrain) { const entryId = "entry-000-item-api"; state.integrationTrains["repo-main"].entryOrder = [entryId]; state.integrationTrains["repo-main"].entries = { [entryId]: { entryId, workItemId: "item-api", ordinal: 0, state: "eligible", integrationReadyHash: readyFact.hash, sourceCandidate: clone(item.candidate), attemptIds: [], currentAttemptId: null, integrationReceipt: null, blockerIds: [] } }; state.integrationTrains["repo-main"].acceptedPrefixOrdinal = 0; item.integrationEntryId = entryId; }
  const integrationReservationId = `reservation-${prefix}-integration`; const integrationSequence = state.scheduler.nextReservationSequence;
  const integrationLeaseId = `lease-${prefix}-integration`;
  state.leases[integrationLeaseId] = { leaseId: integrationLeaseId, kind: "stage_claim", subject: { kind: "work_item", id: "item-api" }, holderStageAttemptId: null, holderIntegrationAttemptId: null, candidateGeneration: generation, units: 0, ownerEpoch: state.owner.ownerEpoch, state: "active", acquiredAt: NOW, expiresAt: null, releasedAt: null, releaseReason: null };
  state.workItems["item-api"].activeLeaseIds = [integrationLeaseId];
  state.scheduler.reservations[integrationReservationId] = { reservationId: integrationReservationId, reservationSequence: integrationSequence, workItemId: "item-api", stage: "F8", attemptOrdinal: 2, operationKind: "integration", state: "active", candidateGeneration: generation, ownerEpoch: state.owner.ownerEpoch, authorizationSetHash: state.identity.authorizationSet.hash, normalizedRequestHash: canonicalHash({ prefix, integration: true }), leaseIds: [integrationLeaseId], mutexGroupIds: [], resourceUnits: {}, operationalUnits: {}, workerRole: "none", repositoryId: "repo-main", createdAt: NOW, releasedAt: null }; state.scheduler.nextReservationSequence += 1;
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
for (const [label, observedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
  const forgedBinding = rehashFact({ ...repositoryBinding, observedAt });
  const forgedPayload = { ...reserveIntegrationPayload, repositoryBindingFactHash: forgedBinding.hash };
  const forgedContext = { ...integrationContext, facts: { ...integrationContext.facts, [forgedBinding.hash]: forgedBinding } };
  assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "reserve_integration_attempt", forgedPayload, { commandId: `command-${label}-repository-binding`, idempotencyKey: `${label}-repository-binding` }), forgedContext).accepted, false, `${label} repository binding observation cannot reserve integration authority`);
}
let conflictState = integrationState;
let conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "reserve_integration_attempt", { ...reserveIntegrationPayload, integrationAttemptId: "integration-conflict", lockLeaseId: "lease-integration-conflict", compositionEffect: { ...compositionEffect, effectId: "integration-conflict-compose" } }, { commandId: "command-conflict-reserve", idempotencyKey: "conflict-reserve" }), integrationContext); assert.equal(conflictTransition.accepted, true); conflictState = conflictTransition.state;
conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "mark_effect_dispatching", { effectId: "integration-conflict-compose", expectedDispatchCount: 0 }, { commandId: "command-conflict-dispatch", idempotencyKey: "conflict-dispatch" }), integrationContext); assert.equal(conflictTransition.accepted, true); conflictState = conflictTransition.state;
const conflictFact = gitFact({ kind: "git_transaction", factType: "composition", planHash: plan.planHash, runId: conflictState.runId, runNonce: conflictState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-conflict", effectId: "integration-conflict-compose", requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: null, commit: null, tree: null, parentCommit: O("a"), reconciliation: "conflict", detailsHash: H("0"), observedAt: NOW });
const conflictContext = { ...integrationContext, facts: { ...integrationContext.facts, [conflictFact.hash]: conflictFact } };
conflictTransition = reduceDagRunV1(conflictState, reducerInput(conflictState, "record_git_composition_conflict", { integrationAttemptId: "integration-conflict", compositionFactHash: conflictFact.hash, conflictClass: "mechanical" }, { kind: "observation", commandId: "command-conflict-observe", idempotencyKey: "conflict-observe" }), conflictContext);
assert.equal(conflictTransition.accepted, true, `exact composition conflict releases only the integration lock while retaining the sticky lane: ${JSON.stringify(conflictTransition)}`);
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
const integrationSuccessor = { ownerTokenHash: H("1"), sessionId: "session-integration-successor", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("0") };
const integrationOwnership = ownershipFactFor(integrationState, integrationSuccessor, "same_manager");
const integrationTransferPayload = { ...integrationSuccessor, ownershipReceipt: integrationOwnership.hash, priorOwnerDisposition: "same_manager" };
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [integrationOwnership.hash]: integrationOwnership } };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "transfer_owner", integrationTransferPayload, { commandId: "command-integration-owner-transfer", idempotencyKey: "integration-owner-transfer" }), integrationContext);
assert.equal(transition.accepted, true, "owner successor safely rebinds an already-dispatching composition operation");
assert.equal(transition.accepted && transition.state.effects[compositionEffect.effectId].dispatchCount, 1, "composition takeover preserves dispatch count and operation identity");
assert.equal(transition.accepted && transition.state.effects[compositionEffect.effectId].boundOwnerEpoch, integrationState.owner.ownerEpoch + 1);
if (transition.accepted) integrationState = transition.state;
const composed = { repositoryId: "repo-main", commit: O("e"), tree: O("f") };
const compositionFact = gitFact({ kind: "git_transaction", factType: "composition", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: compositionEffect.effectId, requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: null, commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: H("6"), observedAt: NOW });
const privateRefs = Object.fromEntries(["baseline", "candidate", "prefix", "composed", "proposal"].map((role) => [role, `refs/pi-dag/v1/transactions/test/${role}`]));
const privateRefFacts = Object.entries(privateRefs).map(([role, targetRef]) => gitFact({ kind: "git_transaction", factType: "private_ref", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: compositionEffect.effectId, requestHash: compositionEffect.requestHash, commonDirIdentityHash: H("2"), targetRef, commit: ["baseline", "prefix"].includes(role) ? O("a") : role === "candidate" ? O("c") : composed.commit, tree: ["baseline", "prefix"].includes(role) ? O("b") : role === "candidate" ? O("d") : composed.tree, parentCommit: ["composed", "proposal"].includes(role) ? O("a") : null, reconciliation: "applied_exact", detailsHash: canonicalHash(role), observedAt: NOW }));
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [compositionFact.hash]: compositionFact, ...Object.fromEntries(privateRefFacts.map((fact) => [fact.hash, fact])) } };
for (const [label, observedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
  const forgedComposition = rehashFact({ ...compositionFact, observedAt }); const forgedRefs = privateRefFacts.map((fact) => rehashFact({ ...fact, observedAt }));
  const forgedContext = { ...integrationContext, facts: { ...integrationContext.facts, [forgedComposition.hash]: forgedComposition, ...Object.fromEntries(forgedRefs.map((fact) => [fact.hash, fact])) } };
  const forgedPayload = { integrationAttemptId: "integration-001", compositionFactHash: forgedComposition.hash, composedTree: composed, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), conflictClass: "none", privateRefFactHashes: forgedRefs.map(({ hash }) => hash) };
  assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_composition", forgedPayload, { kind: "observation", commandId: `command-compose-${label}`, idempotencyKey: `compose-${label}` }), forgedContext).accepted, false, `${label} composition/private-ref facts cannot advance integration`);
}
const orderedComposition = rehashFact({ ...compositionFact, observedAt: "2026-08-04T15:00:00.100Z" }); const latePrivateRefs = privateRefFacts.map((fact, index) => rehashFact({ ...fact, observedAt: index === 0 ? "2026-08-04T15:00:00.200Z" : "2026-08-04T15:00:00.100Z" }));
const invertedPrivateContext = { ...integrationContext, facts: { ...integrationContext.facts, [orderedComposition.hash]: orderedComposition, ...Object.fromEntries(latePrivateRefs.map((fact) => [fact.hash, fact])) } };
assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_composition", { integrationAttemptId: "integration-001", compositionFactHash: orderedComposition.hash, composedTree: composed, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), conflictClass: "none", privateRefFactHashes: latePrivateRefs.map(({ hash }) => hash) }, { kind: "observation", commandId: "command-compose-private-inversion", idempotencyKey: "compose-private-inversion", occurredAt: "2026-08-04T15:00:00.300Z" }), invertedPrivateContext).accepted, false, "private-ref observation later than composition is a rejected temporal inversion even before the accepting input");
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_composition", { integrationAttemptId: "integration-001", compositionFactHash: compositionFact.hash, composedTree: composed, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), conflictClass: "none", privateRefFactHashes: privateRefFacts.map(({ hash }) => hash) }, { kind: "observation", commandId: "command-compose-observe", idempotencyKey: "compose-observe" }), integrationContext);
assert.equal(transition.accepted, true, "exact composition observation advances only the current integration attempt"); integrationState = transition.state;
const verificationFor = (phase, profileHash, profile, effect) => gitFact({ kind: "verification", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, authorizationSetHash: effect.boundAuthorizationSetHash, ownerEpoch: effect.boundOwnerEpoch, freshnessReceiptHash: effect.boundFreshnessReceiptHash, effectId: effect.effectId, requestHash: effect.requestHash, requestIdentityHash: effect.requestHash, repositoryId: "repo-main", trainId: "train-main", integrationAttemptId: "integration-001", phase, profileId: profile.profileId, profileHash, executableArtifactHash: profile.executableArtifactHash, argvHash: canonicalHash(profile.argv), cwdMode: profile.cwdMode, environmentProfileId: profile.environmentProfileId, environmentProfileHash: profile.environmentProfileHash, environmentHash: profile.environmentHash, timeoutMs: profile.timeoutMs, readOnly: true, noEdit: true, tree: composed, commonDirIdentityHash: H("2"), worktreeIdentityHash: H("9"), objectFormat: "sha1", executionId: `verification-${phase}`, exitCode: 0, signal: null, outputHash: H("6"), stdoutHash: H("7"), stderrHash: H("8"), outputBytes: 22, parser: "strict-json-disposition-v1", parserDisposition: "PASS", parsedResultHash: canonicalHash({ disposition: "PASS" }), startedAt: NOW, completedAt: NOW, disposition: "PASS" });
const validationFacts = {};
for (const [phase, profileHash, profile] of [["prefix", PREFIX_VALIDATION_PROFILE_HASH, PREFIX_VALIDATION_PROFILE], ["final", FINAL_VALIDATION_PROFILE_HASH, FINAL_VALIDATION_PROFILE]]) {
  const executionRequest = integrationValidationEffectRequestV1(integrationState, integrationContext, "integration-001", phase);
  const effectId = `integration-001-verify-${phase}`;
  const effect = { effectId, kind: "verify_prefix", subject: { kind: "train", id: "train-main" }, boundStageAttemptId: null, boundIntegrationAttemptId: "integration-001", boundWorkerResultHash: null, executionRequest, executionObservationHash: null, effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash: canonicalHash(executionRequest), boundOwnerEpoch: integrationState.owner.ownerEpoch, boundAuthorizationSetHash: integrationState.identity.authorizationSet.hash, boundFreshnessReceiptHash: integrationState.freshness.receipt.hash, boundCandidateGeneration: 1, boundGateEpochHash: H("5"), state: "intended", dispatchCount: 0, createdRevision: integrationState.revision + 1, createdAt: NOW, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
  transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "put_effect_intent", { effect }, { commandId: `command-${phase}-validation-intent`, idempotencyKey: `${phase}-validation-intent` }), integrationContext); assert.equal(transition.accepted, true); integrationState = transition.state;
  transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "mark_effect_dispatching", { effectId, expectedDispatchCount: 0 }, { commandId: `command-${phase}-validation-dispatch`, idempotencyKey: `${phase}-validation-dispatch` }), integrationContext); assert.equal(transition.accepted, true); integrationState = transition.state;
  const verification = verificationFor(phase, profileHash, profile, integrationState.effects[effectId]); integrationContext.facts[verification.hash] = verification;
  if (phase === "prefix") for (const [label, startedAt, completedAt, occurredAt] of [["predated", "2020-01-01T00:00:00.000Z", "2020-01-01T00:00:01.000Z", NOW], ["future", "2099-01-01T00:00:00.000Z", "2099-01-01T00:00:01.000Z", NOW], ["reversed", "2026-08-04T15:00:00.200Z", "2026-08-04T15:00:00.100Z", "2026-08-04T15:00:00.300Z"]]) {
    const forgedVerification = rehashFact({ ...verification, executionId: `verification-prefix-${label}`, startedAt, completedAt }); const forgedContext = { ...integrationContext, facts: { ...integrationContext.facts, [forgedVerification.hash]: forgedVerification } };
    assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "record_effect_execution", { effectId, executionObservationHash: forgedVerification.hash }, { kind: "observation", commandId: `command-prefix-validation-${label}`, idempotencyKey: `prefix-validation-${label}`, occurredAt }), forgedContext).accepted, false, `${label} validation execution timestamps cannot enter effect closure`);
  }
  transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_effect_execution", { effectId, executionObservationHash: verification.hash }, { kind: "observation", commandId: `command-${phase}-validation-result`, idempotencyKey: `${phase}-validation-result` }), integrationContext); assert.equal(transition.accepted, true); integrationState = transition.state;
  const reconciliationCore = { kind: "effect_reconciliation", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, effectId, requestHash: effect.requestHash, reconciliation: "applied_exact", executionObservationHash: verification.hash, resultIdentityHash: verification.hash, closedAt: verification.completedAt };
  const reconciliation = { ...reconciliationCore, hash: canonicalHash(reconciliationCore) }; integrationContext.facts[reconciliation.hash] = reconciliation;
  transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_effect_observation", { effectId, observationHash: reconciliation.hash, reconciliation: "applied_exact", terminalState: "reconciled" }, { kind: "observation", commandId: `command-${phase}-validation-reconcile`, idempotencyKey: `${phase}-validation-reconcile` }), integrationContext); assert.equal(transition.accepted, true); integrationState = transition.state;
  validationFacts[phase] = { verification, reconciliation };
}
const cancellationAttributionState = clone(integrationState);
const attributionEffect = cancellationAttributionState.effects["integration-001-verify-prefix"];
const executionRevision = Object.values(cancellationAttributionState.idempotencySlots).find((slot) => slot.inputType === "record_effect_execution" && slot.appliedRevision < attributionEffect.reconciliationRevision).appliedRevision;
const originalAttributionEntry = Object.entries(cancellationAttributionState.idempotencySlots).find(([, slot]) => slot.appliedRevision === attributionEffect.reconciliationRevision && slot.inputType === "record_effect_observation");
assert(originalAttributionEntry, "exact execution fixture retains its accepted reconciliation command record");
const installClosedCancellationRecord = (state, cancellationId, resultHash, appliedRevision, reconciliationBindings = []) => {
  state.cancellations[cancellationId] = { cancellationId, scope: "integration_attempt", subjectId: "integration-001", fencedGenerations: {}, state: "closed", reason: `attribution fixture ${cancellationId}`, requestedAt: NOW, effectIds: [], resultHash };
  const slotId = canonicalHash({ type: "record_cancellation", naturalIdentity: `${cancellationId}/${resultHash}` });
  state.idempotencySlots[slotId] = { slotId, inputType: "record_cancellation", commandId: `command-${cancellationId}`, idempotencyKey: `key-${cancellationId}`, payloadHash: canonicalHash({ cancellationId, resultHash, reconciliationBindings }), inputHash: canonicalHash({ command: cancellationId, resultHash, reconciliationBindings }), appliedRevision, reconciliationCancellationId: cancellationId, ...(reconciliationBindings.length ? { reconciliationBindings: clone(reconciliationBindings) } : {}) };
  return slotId;
};
const firstHistoricalSlot = Object.entries(cancellationAttributionState.idempotencySlots).find(([, slot]) => slot.appliedRevision === 1); assert(firstHistoricalSlot); delete cancellationAttributionState.idempotencySlots[firstHistoricalSlot[0]];
installClosedCancellationRecord(cancellationAttributionState, "cancel-attribution-first-unrelated", H("1"), 1);
const [, originalAttribution] = originalAttributionEntry; delete cancellationAttributionState.idempotencySlots[originalAttributionEntry[0]];
const secondAttributionSlotId = installClosedCancellationRecord(cancellationAttributionState, "cancel-attribution-second-execution", H("2"), originalAttribution.appliedRevision, originalAttribution.reconciliationBindings);
rehashRun(cancellationAttributionState);
const cancellationAttributionValidation = validateDagRunStateV1(cancellationAttributionState, integrationContext);
assert.equal(cancellationAttributionValidation.ok, true, `two sequential cancellations pass when the second specific transaction closes the execution effect after an unrelated first cancellation: ${JSON.stringify(cancellationAttributionValidation.issues)}`);
const missingAttributionState = clone(cancellationAttributionState); delete missingAttributionState.idempotencySlots[secondAttributionSlotId].reconciliationBindings; rehashRun(missingAttributionState);
assert.equal(validateDagRunStateV1(missingAttributionState, integrationContext).ok, false, "missing per-effect cancellation reconciliation attribution fails closed");
const ambiguousAttributionState = clone(cancellationAttributionState); ambiguousAttributionState.idempotencySlots[secondAttributionSlotId].reconciliationBindings.push(clone(ambiguousAttributionState.idempotencySlots[secondAttributionSlotId].reconciliationBindings[0])); rehashRun(ambiguousAttributionState);
assert.equal(validateDagRunStateV1(ambiguousAttributionState, integrationContext).ok, false, "ambiguous duplicate cancellation reconciliation attribution fails closed");
const invertedAttributionState = clone(cancellationAttributionState); invertedAttributionState.effects[attributionEffect.effectId].reconciliationRevision = executionRevision; invertedAttributionState.idempotencySlots[secondAttributionSlotId].appliedRevision = executionRevision; rehashRun(invertedAttributionState);
assert.equal(validateDagRunStateV1(invertedAttributionState, integrationContext).ok, false, "forged execution-before-reconciliation revision ordering fails closed");
const prefixVerification = validationFacts.prefix.verification; const finalVerification = validationFacts.final.verification;
const prefixReconciliation = validationFacts.prefix.reconciliation; const finalReconciliation = validationFacts.final.reconciliation;
const environmentClosureHash = H("6"); const proposalClosure = { prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], prefixEffectReconciliationHashes: [prefixReconciliation.hash], finalEffectReconciliationHashes: [finalReconciliation.hash], environmentClosureHash };
const proposalVerificationRequestHash = canonicalHash({ kind: "proposal_verification", integrationAttemptId: "integration-001", closure: proposalClosure });
const proposalVerification = gitFact({ kind: "git_transaction", factType: "proposal_verification", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: null, requestHash: proposalVerificationRequestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: canonicalHash(proposalClosure), observedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [proposalVerification.hash]: proposalVerification } };
const verificationInput = reducerInput(integrationState, "record_proposal_verification", { integrationAttemptId: "integration-001", proposalVerificationFactHash: proposalVerification.hash, ...proposalClosure }, { kind: "observation", commandId: "command-proposal-verify", idempotencyKey: "proposal-verify" });
for (const [label, observedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
  const forgedProposal = rehashFact({ ...proposalVerification, observedAt }); const forgedContext = { ...integrationContext, facts: { ...integrationContext.facts, [forgedProposal.hash]: forgedProposal } };
  assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "record_proposal_verification", { integrationAttemptId: "integration-001", proposalVerificationFactHash: forgedProposal.hash, ...proposalClosure }, { kind: "observation", commandId: `command-proposal-${label}`, idempotencyKey: `proposal-${label}` }), forgedContext).accepted, false, `${label} proposal verification cannot authorize landing`);
}
assert.equal(reduceDagRunV1(integrationState, verificationInput, { ...integrationContext, integrationValidationProfiles: {} }).accepted, false, "production verification fails closed when the exact plan-hashed mapping is absent");
const forgedExecution = { ...prefixVerification, exitCode: 1 }; forgedExecution.hash = canonicalHash(Object.fromEntries(Object.entries(forgedExecution).filter(([key]) => key !== "hash")));
const forgedInput = reducerInput(integrationState, "record_proposal_verification", { integrationAttemptId: "integration-001", proposalVerificationFactHash: proposalVerification.hash, ...proposalClosure, prefixEvidenceHashes: [forgedExecution.hash] }, { kind: "observation", commandId: "command-proposal-forged", idempotencyKey: "proposal-forged" });
assert.equal(reduceDagRunV1(integrationState, forgedInput, { ...integrationContext, facts: { ...integrationContext.facts, [forgedExecution.hash]: forgedExecution } }).accepted, false, "a synthesized PASS with nonzero exact execution cannot authorize landing");
transition = reduceDagRunV1(integrationState, verificationInput, integrationContext);
assert.equal(transition.accepted, true, "prefix and final verification bind exact plan-hashed mapping, execution, parser, environment, composed state, and terminal effects"); integrationState = transition.state;
const landingEffect = { ...compositionEffect, effectId: "integration-001-land", kind: "land_target", subject: { kind: "repository", id: "repo-main" }, requestHash: H("a"), boundOwnerEpoch: integrationState.owner.ownerEpoch, createdRevision: integrationState.revision + 1 };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "prepare_git_landing", { integrationAttemptId: "integration-001", landingEffect, intendedLandedTree: composed }, { commandId: "command-landing-prepare", idempotencyKey: "landing-prepare" }), integrationContext);
assert.equal(transition.accepted, true, "landing intent is durable only after exact proposal verification"); integrationState = transition.state;
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "mark_effect_dispatching", { effectId: landingEffect.effectId, expectedDispatchCount: 0 }, { commandId: "command-landing-dispatch", idempotencyKey: "landing-dispatch" }), integrationContext);
assert.equal(transition.accepted, true, "landing dispatch is separately guarded after intent"); integrationState = transition.state;
{
  let historyState = integrationState; let historyContext = integrationContext;
  const oldOwnerEpoch = historyState.owner.ownerEpoch;
  const absentLandingFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: historyState.runId, runNonce: historyState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, ownerEpoch: oldOwnerEpoch, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: O("a"), tree: O("b"), parentCommit: O("a"), reconciliation: "proven_absent", detailsHash: H("5"), observedAt: NOW });
  historyContext = { ...historyContext, facts: { ...historyContext.facts, [absentLandingFact.hash]: absentLandingFact } };
  let historyTransition = reduceDagRunV1(historyState, reducerInput(historyState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: absentLandingFact.hash, reconciliation: "proven_absent" }, { kind: "observation", commandId: "command-history-old-absent", idempotencyKey: "history-old-absent" }), historyContext);
  assert.equal(historyTransition.accepted, true, `old-owner proven-absent landing observation is accepted: ${JSON.stringify(historyTransition)}`); historyState = historyTransition.state;
  assert.equal(validateDagRunStateV1(historyState, historyContext).ok, true, "committed old-owner landing history validates before transfer");
  const historySuccessor = { ownerTokenHash: H("6"), sessionId: "session-history-successor", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("7") };
  const historyOwnership = ownershipFactFor(historyState, historySuccessor, "same_manager"); historyContext = { ...historyContext, facts: { ...historyContext.facts, [historyOwnership.hash]: historyOwnership } };
  historyTransition = reduceDagRunV1(historyState, reducerInput(historyState, "transfer_owner", { ...historySuccessor, ownershipReceipt: historyOwnership.hash, priorOwnerDisposition: "same_manager" }, { commandId: "command-history-transfer", idempotencyKey: "history-transfer" }), historyContext);
  assert.equal(historyTransition.accepted, true, `exact successor transfer preserves committed old-owner landing history: ${JSON.stringify(historyTransition)}`); historyState = historyTransition.state;
  assert.equal(historyState.effects[landingEffect.effectId].boundOwnerEpoch, oldOwnerEpoch + 1, "replayable landing effect alone rebinds to successor dispatch authority");
  assert.equal(historyState.integrationAttempts["integration-001"].landingObservationFactHash, absentLandingFact.hash, "owner transfer never rewrites the historical landing fact");
  assert.equal(validateDagRunStateV1(historyState, historyContext).ok, true, "at-rest validation resolves the old fact through its accepted command/owner/dispatch rather than current effect authority");
  historyTransition = reduceDagRunV1(historyState, reducerInput(historyState, "retry_effect_dispatch", { effectId: landingEffect.effectId, expectedDispatchCount: 1, reason: "uncertain_acknowledgement" }, { commandId: "command-history-successor-retry", idempotencyKey: "history-successor-retry" }), historyContext);
  assert.equal(historyTransition.accepted && historyTransition.effects.length, 1, "successor receives one explicit idempotent landing redispatch"); historyState = historyTransition.state;
  const successorLandingFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: historyState.runId, runNonce: historyState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, ownerEpoch: historyState.owner.ownerEpoch, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: H("8"), observedAt: NOW });
  const forgedOwnerLanding = rehashFact({ ...successorLandingFact, ownerEpoch: historyState.owner.ownerEpoch + 1 });
  assert.equal(reduceDagRunV1(historyState, reducerInput(historyState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: forgedOwnerLanding.hash, reconciliation: "applied_exact" }, { kind: "observation", commandId: "command-history-forged-owner", idempotencyKey: "history-forged-owner" }), { ...historyContext, facts: { ...historyContext.facts, [forgedOwnerLanding.hash]: forgedOwnerLanding } }).accepted, false, "a future/forged owner epoch cannot claim the current redispatch");
  historyContext = { ...historyContext, facts: { ...historyContext.facts, [successorLandingFact.hash]: successorLandingFact } };
  historyTransition = reduceDagRunV1(historyState, reducerInput(historyState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: successorLandingFact.hash, reconciliation: "applied_exact" }, { kind: "observation", commandId: "command-history-successor-applied", idempotencyKey: "history-successor-applied" }), historyContext);
  assert.equal(historyTransition.accepted, true, `successor applied-exact fact closes its own redispatch: ${JSON.stringify(historyTransition)}`); historyState = historyTransition.state;
  const absentSlotId = canonicalHash({ type: "record_git_landing_reconciliation", naturalIdentity: `integration-001/${absentLandingFact.hash}` });
  assert.deepEqual({ ownerEpoch: historyState.idempotencySlots[absentSlotId].landingObservationBinding.ownerEpoch, dispatchCount: historyState.idempotencySlots[absentSlotId].landingObservationBinding.dispatchCount }, { ownerEpoch: oldOwnerEpoch, dispatchCount: 1 }, "old observation retains exact accepted owner and first-dispatch attribution after successor closure");
  assert.equal(validateDagRunStateV1(historyState, historyContext).ok, true, "both immutable landing command histories remain valid after successor reconciliation");
  const forgedHistory = clone(historyState); forgedHistory.idempotencySlots[absentSlotId].landingObservationBinding.ownerEpoch = historyState.owner.ownerEpoch; rehashRun(forgedHistory);
  assert.equal(validateDagRunStateV1(forgedHistory, historyContext).ok, false, "forged rebinding of historical landing authority fails closed");
  const thirdSuccessor = { ownerTokenHash: H("9"), sessionId: "session-history-third", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("0") };
  const thirdOwnership = ownershipFactFor(historyState, thirdSuccessor, "same_manager"); historyContext = { ...historyContext, facts: { ...historyContext.facts, [thirdOwnership.hash]: thirdOwnership } };
  historyTransition = reduceDagRunV1(historyState, reducerInput(historyState, "transfer_owner", { ...thirdSuccessor, ownershipReceipt: thirdOwnership.hash, priorOwnerDisposition: "same_manager" }, { commandId: "command-history-third-transfer", idempotencyKey: "history-third-transfer" }), historyContext);
  assert.equal(historyTransition.accepted, true, "an exact third ownership epoch may take over the run without receiving completed landing authority"); historyState = historyTransition.state;
  assert.equal(historyState.effects[landingEffect.effectId].boundOwnerEpoch, oldOwnerEpoch + 1, "applied landing remains bound to the successor dispatch that completed it");
  assert.equal(reduceDagRunV1(historyState, reducerInput(historyState, "retry_effect_dispatch", { effectId: landingEffect.effectId, expectedDispatchCount: 2, reason: "uncertain_acknowledgement" }, { commandId: "command-history-third-retry", idempotencyKey: "history-third-retry" }), historyContext).accepted, false, "third epoch cannot mint redispatch authority after applied-exact closure");
}
const landingSuccessor = { ownerTokenHash: H("2"), sessionId: "session-landing-successor", pid: process.pid, processStartIdentity: PROCESS_START_IDENTITY, lockIdentity: H("3") };
const landingOwnership = ownershipFactFor(integrationState, landingSuccessor, "same_manager");
const landingTransferPayload = { ...landingSuccessor, ownershipReceipt: landingOwnership.hash, priorOwnerDisposition: "same_manager" };
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [landingOwnership.hash]: landingOwnership } };
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "transfer_owner", landingTransferPayload, { commandId: "command-landing-owner-transfer", idempotencyKey: "landing-owner-transfer" }), integrationContext);
assert.equal(transition.accepted, true, "owner successor safely rebinds an already-dispatching landing operation");
assert.equal(transition.accepted && transition.state.effects[landingEffect.effectId].dispatchCount, 1, "landing takeover preserves dispatch count and operation identity");
if (transition.accepted) integrationState = transition.state;
const thirdTargetFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: O("9"), tree: O("8"), parentCommit: O("a"), reconciliation: "conflict", detailsHash: H("7"), observedAt: NOW });
const thirdTargetContext = { ...integrationContext, facts: { ...integrationContext.facts, [thirdTargetFact.hash]: thirdTargetFact } };
const thirdTarget = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: thirdTargetFact.hash, reconciliation: "conflict" }, { kind: "observation", commandId: "command-landing-third", idempotencyKey: "landing-third" }), thirdTargetContext);
assert.equal(thirdTarget.accepted, true, "exact third-target observation blocks without overwrite and releases the repository lock"); assert.equal(thirdTarget.accepted && thirdTarget.state.workItems["item-api"].current, "integration_ready"); assert.equal(thirdTarget.accepted && thirdTarget.state.repositories["repo-main"].integrationLockLeaseId, null); assert.equal(thirdTarget.accepted && thirdTarget.state.blockers["integration-target-third-integration-001"].release, "successor_plan");
const targetObservationHash = canonicalHash({ targetRef: "refs/heads/main", observed: { commit: composed.commit, tree: composed.tree }, expectedOld: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, intendedNew: composed });
const landingFact = gitFact({ kind: "git_transaction", factType: "landing", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, repositoryId: "repo-main", integrationAttemptId: "integration-001", effectId: landingEffect.effectId, requestHash: landingEffect.requestHash, commonDirIdentityHash: H("2"), targetRef: "refs/heads/main", commit: composed.commit, tree: composed.tree, parentCommit: O("a"), reconciliation: "applied_exact", detailsHash: targetObservationHash, observedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [landingFact.hash]: landingFact } };
for (const [label, observedAt] of [["future", "2099-01-01T00:00:00.000Z"], ["predated", "2020-01-01T00:00:00.000Z"]]) {
  const forgedLanding = rehashFact({ ...landingFact, observedAt }); const forgedContext = { ...integrationContext, facts: { ...integrationContext.facts, [forgedLanding.hash]: forgedLanding } };
  assert.equal(reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: forgedLanding.hash, reconciliation: "applied_exact" }, { kind: "observation", commandId: `command-landing-${label}`, idempotencyKey: `landing-${label}` }), forgedContext).accepted, false, `${label} landing observation cannot reconcile the target effect`);
}
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "record_git_landing_reconciliation", { integrationAttemptId: "integration-001", landingObservationFactHash: landingFact.hash, reconciliation: "applied_exact" }, { kind: "observation", commandId: "command-landing-observe", idempotencyKey: "landing-observe" }), integrationContext);
assert.equal(transition.accepted, true, "target new observation reconciles exact landing without claiming completion"); integrationState = transition.state;
const transactionReceiptCore = { schemaVersion: 1, kind: "IntegrationReceiptV1", transactionId: "integration-001", runId: integrationState.runId, runNonce: integrationState.runNonce, planHash: plan.planHash, authorizationSetHash: integrationState.identity.authorizationSet.hash, ownerEpoch: integrationState.owner.ownerEpoch, repositoryId: "repo-main", commonDirIdentityHash: H("2"), worktreeIdentityHash: H("3"), gitVersion: "git version 2.test", configHash: H("4"), objectFormat: "sha1", targetRef: "refs/heads/main", sourceBase: integrationState.workItems["item-api"].candidate.base, candidate: integrationState.workItems["item-api"].candidate.git, expectedPrefix: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, composed, workItemId: "item-api", candidateGeneration: 1, compositionProfileHash: H("d"), prefixValidationProfileHash: PREFIX_VALIDATION_PROFILE_HASH, finalValidationProfileHash: FINAL_VALIDATION_PROFILE_HASH, prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], prefixEffectReconciliationHashes: [prefixReconciliation.hash], finalEffectReconciliationHashes: [finalReconciliation.hash], environmentClosureHash, privateRefs, landing: { expectedOldOid: O("a"), newOid: composed.commit, reconciliation: "applied_exact", targetObservationHash }, sealedAt: NOW };
const transactionReceipt = { ...transactionReceiptCore, receiptHash: canonicalHash(transactionReceiptCore) }; const transactionReceiptHash = transactionReceipt.receiptHash;
const transactionReceiptFactCore = { kind: "git_integration_receipt", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, authorizationSetHash: integrationState.identity.authorizationSet.hash, repositoryId: "repo-main", integrationAttemptId: "integration-001", transactionReceiptHash, receipt: transactionReceipt }; const transactionReceiptFact = { ...transactionReceiptFactCore, hash: canonicalHash(transactionReceiptFactCore) }; const transactionReceiptFactHash = transactionReceiptFact.hash;
const integrationReceipt = gitFact({ kind: "integration", planHash: plan.planHash, runId: integrationState.runId, runNonce: integrationState.runNonce, authorizationSetHash: integrationState.identity.authorizationSet.hash, workItemId: "item-api", repositoryId: "repo-main", integrationAttemptId: "integration-001", candidateHash: integrationState.workItems["item-api"].candidate.candidateHash, strategy: "merge_tree_one_parent", compositionProfileHash: H("d"), expectedPrefix: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, expectedTarget: { repositoryId: "repo-main", commit: O("a"), tree: O("b") }, prefixEvidenceHashes: [prefixVerification.hash], finalEvidenceHashes: [finalVerification.hash], prefixEffectReconciliationHashes: [prefixReconciliation.hash], finalEffectReconciliationHashes: [finalReconciliation.hash], environmentClosureHash, sourceBase: integrationState.workItems["item-api"].candidate.base, sourceCandidate: integrationState.workItems["item-api"].candidate.git, syntheticParentCommit: O("a"), sourceToIntegratedLineageHash: H("8"), landed: composed, combinedStateVerified: true, reconciled: true, acceptingOwnerEpoch: integrationState.owner.ownerEpoch, commonDirIdentityHash: H("2"), worktreeIdentityHash: H("3"), gitConfigHash: H("4"), gitVersionHash: canonicalHash("git version 2.test"), objectFormat: "sha1", transactionReceiptHash, transactionReceiptFactHash, landingObservationHash: landingFact.hash, sealedAt: NOW });
integrationContext = { ...integrationContext, facts: { ...integrationContext.facts, [transactionReceiptFact.hash]: transactionReceiptFact, [integrationReceipt.hash]: integrationReceipt } };
const forgedReceiptAuthority = (transactionSealedAt, integrationSealedAt, label) => {
  const transactionCore = { ...transactionReceipt, sealedAt: transactionSealedAt }; delete transactionCore.receiptHash;
  const transaction = { ...transactionCore, receiptHash: canonicalHash(transactionCore) };
  const transactionFactCore = { ...transactionReceiptFact, transactionReceiptHash: transaction.receiptHash, receipt: transaction }; delete transactionFactCore.hash;
  const transactionFact = { ...transactionFactCore, hash: canonicalHash(transactionFactCore) };
  const integration = rehashFact({ ...integrationReceipt, transactionReceiptHash: transaction.receiptHash, transactionReceiptFactHash: transactionFact.hash, sealedAt: integrationSealedAt });
  return { label, transactionFact, integration, context: { ...integrationContext, facts: { ...integrationContext.facts, [transactionFact.hash]: transactionFact, [integration.hash]: integration } } };
};
for (const forged of [forgedReceiptAuthority("2099-01-01T00:00:00.000Z", "2099-01-01T00:00:01.000Z", "future"), forgedReceiptAuthority("2020-01-01T00:00:00.000Z", "2020-01-01T00:00:01.000Z", "predated"), forgedReceiptAuthority("2026-08-04T15:00:00.200Z", "2026-08-04T15:00:00.100Z", "inverted"), forgedReceiptAuthority("2026-99-01T00:00:00.000Z", NOW, "invalid")]) {
  const occurredAt = forged.label === "inverted" ? "2026-08-04T15:00:00.300Z" : NOW;
  const result = reduceDagRunV1(integrationState, reducerInput(integrationState, "accept_integration_receipt", { integrationAttemptId: "integration-001", integrationReceiptHash: forged.integration.hash, transactionReceiptHash: forged.transactionFact.transactionReceiptHash, transactionReceiptFactHash: forged.transactionFact.hash }, { kind: "observation", commandId: `command-integration-${forged.label}-time`, idempotencyKey: `integration-${forged.label}-time`, occurredAt }), forged.context);
  assert.equal(result.accepted, false, `${forged.label} transaction/integration receipt sealing cannot complete integration`);
}
const inventedReceipt = reduceDagRunV1(integrationState, reducerInput(integrationState, "accept_integration_receipt", { integrationAttemptId: "integration-001", integrationReceiptHash: integrationReceipt.hash, transactionReceiptHash: H("c"), transactionReceiptFactHash: H("d") }, { kind: "observation", commandId: "command-integration-invented", idempotencyKey: "integration-invented" }), integrationContext); assert.equal(inventedReceipt.accepted, false, "an invented transaction receipt hash cannot become canonical integration authority");
transition = reduceDagRunV1(integrationState, reducerInput(integrationState, "accept_integration_receipt", { integrationAttemptId: "integration-001", integrationReceiptHash: integrationReceipt.hash, transactionReceiptHash, transactionReceiptFactHash }, { kind: "observation", commandId: "command-integration-accept", idempotencyKey: "integration-accept" }), integrationContext);
assert.equal(transition.accepted, true, "only exact immutable transaction and integration receipts atomically mark completion");
assert.equal(transition.accepted && transition.state.workItems["item-api"].current, "complete");
assert.equal(transition.accepted && transition.state.integrationTrains["repo-main"].acceptedPrefix.commit, composed.commit);
assert.equal(transition.accepted && transition.state.scheduler.activeNodeLanes["item-api"].releaseDisposition, "integrated", "receipt acceptance releases sticky lane only after exact landing");
if (transition.accepted) {
  const exactCompleted = transition.state;
  const prefixEffect = Object.values(exactCompleted.effects).find((effect) => effect.kind === "verify_prefix" && effect.executionRequest?.phase === "prefix");
  const mismatchedRequest = clone(exactCompleted); mismatchedRequest.effects[prefixEffect.effectId].executionRequest.argvHash = H("0"); rehashRun(mismatchedRequest);
  expectInvalid((value) => validateDagRunStateV1(value, integrationContext), mismatchedRequest, "at-rest validation rejects a validation effect whose persisted canonical request identity no longer matches its profile/argv/environment/tree authority");
  const missingValidationEffect = clone(exactCompleted); delete missingValidationEffect.effects[prefixEffect.effectId]; rehashRun(missingValidationEffect);
  expectInvalid((value) => validateDagRunStateV1(value, integrationContext), missingValidationEffect, "at-rest proposal/receipt validation rejects a missing terminal profile effect even when execution facts remain");
  const missingExecutionObservation = { ...integrationContext, facts: { ...integrationContext.facts } }; delete missingExecutionObservation.facts[prefixEffect.executionObservationHash];
  expectInvalid((value) => validateDagRunStateV1(value, missingExecutionObservation), exactCompleted, "at-rest validation rejects a missing immutable profile execution observation");
  const compositionAuthorityMisuse = clone(exactCompleted); const proposal = { ...integrationContext.facts[compositionAuthorityMisuse.integrationAttempts["integration-001"].proposalVerificationFactHash], effectId: compositionEffect.effectId }; proposal.hash = canonicalHash(Object.fromEntries(Object.entries(proposal).filter(([key]) => key !== "hash"))); compositionAuthorityMisuse.integrationAttempts["integration-001"].proposalVerificationFactHash = proposal.hash; rehashRun(compositionAuthorityMisuse);
  expectInvalid((value) => validateDagRunStateV1(value, { ...integrationContext, facts: { ...integrationContext.facts, [proposal.hash]: proposal } }), compositionAuthorityMisuse, "proposal verification cannot borrow composition-effect authority");
  const futureCompositionState = clone(exactCompleted); const futureCompositionFacts = { ...integrationContext.facts }; const exactAttempt = futureCompositionState.integrationAttempts["integration-001"];
  const futureComposition = rehashFact({ ...futureCompositionFacts[exactAttempt.compositionFactHash], observedAt: "2099-01-01T00:00:00.000Z" }); futureCompositionFacts[futureComposition.hash] = futureComposition; exactAttempt.compositionFactHash = futureComposition.hash; rehashRun(futureCompositionState);
  expectInvalid((value) => validateDagRunStateV1(value, { ...integrationContext, facts: futureCompositionFacts }), futureCompositionState, "at-rest integration rejects a future composition fact even when every identity/hash reference is self-consistent");
  const predatedReceiptAuthority = forgedReceiptAuthority("2020-01-01T00:00:00.000Z", "2020-01-01T00:00:01.000Z", "at-rest-predated"); const predatedReceiptState = clone(exactCompleted); const predatedAttempt = predatedReceiptState.integrationAttempts["integration-001"]; const predatedEntry = predatedReceiptState.integrationTrains["repo-main"].entries[predatedAttempt.entryId];
  predatedAttempt.integrationReceipt = predatedReceiptAuthority.integration.hash; predatedEntry.integrationReceipt = predatedReceiptAuthority.integration.hash; predatedReceiptState.workItems["item-api"].integrationReceipt = predatedReceiptAuthority.integration.hash; predatedReceiptState.integrationTrains["repo-main"].acceptedPrefixReceipt = predatedReceiptAuthority.integration.hash; predatedReceiptState.evidenceIndex.integrationReceipts["integration-001"].hash = predatedReceiptAuthority.integration.hash; rehashRun(predatedReceiptState);
  expectInvalid((value) => validateDagRunStateV1(value, predatedReceiptAuthority.context), predatedReceiptState, "at-rest integration rejects self-consistent receipts sealed before landing");
}

const conductorRoot = await mkdtemp(join(tmpdir(), "pi-dag-conductor-v1-"));
let conductorOwnershipChild = null;
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
  let launches = 0; let integrationDelegations = 0;
  const conductor = new DagConductorServiceV1({ lifecycle: {
    worker: {
      async launchExact(request, state) {
        launches += 1;
        const attemptNonce = `nonce-${request.workerId}-0123456789`; const config = { storageId: "deterministic-manager-storage", ownerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, launchKey: request.launchKey, requestHash: request.configRequestHash, task: request.task, launchOwner: { sessionId: state.owner.sessionId, pid: state.owner.pid, processStartIdentity: state.owner.processStartIdentity } };
        const configHash = canonicalHash(config); const configFactCore = { kind: "worker_config", configHash, config };
        return { workerStorageId: config.storageId, launchOwnerSessionId: state.owner.sessionId, workerId: request.workerId, attemptNumber: request.expectedAttemptNumber, attemptNonce, configHash, configFact: { ...configFactCore, hash: canonicalHash(configFactCore) }, supervisorPid: process.pid, supervisorStartIdentity: PROCESS_START_IDENTITY, childPid: null, childStartIdentity: null, mailboxHash: null, heartbeatAt: NOW };
      },
      async readTerminalExact(binding, state) { const attempt = state.stageAttempts[binding.stageAttemptId]; const item = state.workItems[attempt.workItemId]; const sourceBase = attempt.stage === "F1" ? state.repositories[item.writeRepositoryId].baseline : item.candidate?.git ?? state.repositories[item.writeRepositoryId].baseline; return { completionId: `completion-${binding.workerId}`, terminalStatus: "succeeded", workerOutput: ["F1", "F3"].includes(attempt.stage) ? exactWorkerGitOutput(sourceBase, O("c"), O("d")) : noWorkerGitOutput() }; },
    },
    candidate: {
      async inspectAndSealCandidate({ plan: exactPlan, state, attempt, repositoryId }) {
        const item = state.workItems[attempt.workItemId]; const base = attempt.stage === "F1" ? exactPlan.repositories.find((repository) => repository.repositoryId === repositoryId).baseline : item.candidate.git; const git = { repositoryId, commit: O("c"), tree: O("d") };
        const core = { kind: "candidate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: item.workItemId, generation: item.candidateGeneration + 1, candidateId: `candidate-${attempt.stageAttemptId}`, base, git, patchIdentityHash: canonicalHash({ base, git }), producedByStageAttemptId: attempt.stageAttemptId, lineageHash: item.implementationLineageHash };
        return { candidate: { ...core, hash: canonicalHash(core) }, workerOutput: exactWorkerGitOutput(base, git.commit, git.tree) };
      },
    },
    procedure: {
      adapterKind: "immutable-catalog-command-v1",
      allowlistedProcedureHashes: Object.keys(conductorContext.catalog.procedures).sort(),
      allowlistHash: canonicalHash(Object.keys(conductorContext.catalog.procedures).sort()),
      async executeExact({ plan: exactPlan, state, attempt, procedure }) {
        const item = state.workItems[attempt.workItemId]; const candidateGeneration = attempt.reservedOutputGeneration ?? attempt.inputGeneration;
        let workspaceMaterialization; let environmentObservation;
        if (["F2", "F5", "F7"].includes(attempt.stage)) {
          const commonDirIdentityHash = canonicalHash({ conductor: "common-dir" }); const worktreeIdentityHash = canonicalHash({ conductor: attempt.stageAttemptId });
          const materializationCore = { kind: "workspace_materialization", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, commonDirIdentityHash, worktreeIdentityHash, materializedAt: NOW };
          workspaceMaterialization = { ...materializationCore, hash: canonicalHash(materializationCore) };
          const observationCore = { kind: "environment_observation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, repositoryId: item.writeRepositoryId, candidateGeneration, candidateHash: item.candidate.candidateHash, candidateTree: item.candidate.git, environmentProfileHash: procedure.environmentProfileHash, workspaceMaterializationHash: workspaceMaterialization.hash, commonDirIdentityHash, worktreeIdentityHash, cleanliness: "clean", observedAt: NOW };
          environmentObservation = { ...observationCore, hash: canonicalHash(observationCore) };
        }
        const oracleAssertions = [];
        if (attempt.stage === "F2") { const expected = exactPlan.acceptanceOracles[0].assertions[0]; const core = { kind: "oracle_assertion", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: "F2", stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, oracleId: exactPlan.acceptanceOracles[0].oracleId, assertionId: expected.assertionId, procedureId: expected.procedureId, environmentProfileId: expected.environmentProfileId, observationMethod: expected.observationMethod, requiredEvidenceClass: expected.requiredEvidenceClass, disposition: "PASS", observationHash: attempt.workerResult.hash }; oracleAssertions.push({ ...core, hash: canonicalHash(core) }); }
        const checkExecutions = [];
        if (attempt.stage === "F2") { const core = { kind: "check_execution", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, candidateGeneration, candidateHash: item.candidate.candidateHash, checkId: "check-api", procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, environmentObservationHash: environmentObservation.hash, executionId: `execution-${attempt.stageAttemptId}`, disposition: "PASS", startedAt: NOW, completedAt: NOW }; checkExecutions.push({ ...core, hash: canonicalHash(core) }); }
        const aggregateCore = { kind: "check_aggregate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, disposition: "PASS", oracleIds: ["oracle-api"], assertions: oracleAssertions.map((fact) => ({ oracleId: fact.oracleId, assertionId: fact.assertionId, evidenceHash: fact.hash })), checks: attempt.stage === "F2" ? [{ checkId: "check-api", disposition: "PASS", executionEvidenceHash: checkExecutions[0].hash, applicabilityEvidenceHashes: [] }] : [] };
        const checkAggregate = { ...aggregateCore, hash: canonicalHash(aggregateCore) };
        const evidenceCore = { kind: "stage_evidence", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, stage: attempt.stage, stageAttemptId: attempt.stageAttemptId, attemptInputHash: attempt.attemptInput.hash, authorizationSetHash: state.identity.authorizationSet.hash, procedureHash: procedure.hash, environmentProfileHash: procedure.environmentProfileHash, checkAggregateHash: checkAggregate.hash, findingHashes: [], effectReconciliationHashes: [], candidateGeneration, candidateHash: attempt.stage === "F0" ? null : item.candidate.candidateHash, producerKind: attempt.producerKind, producerResultHash: attempt.workerResult?.hash ?? null, disposition: "PASS", environmentObservationHash: environmentObservation?.hash ?? null, producedAt: NOW, readOnly: procedure.readOnly };
        const evidence = { ...evidenceCore, hash: canonicalHash(evidenceCore) }; const output = { checkAggregate, evidence, oracleAssertions, checkDispositions: [], checkExecutions, checkAuthorities: [], ...(workspaceMaterialization ? { workspaceMaterialization, environmentObservation } : {}) };
        if (attempt.stage === "F8") { const readyCore = { kind: "integration_ready", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: attempt.workItemId, candidateGeneration: item.candidateGeneration, candidateHash: item.candidate.candidateHash, f8EvidenceHash: evidence.hash, allRequiredChecksPassed: true, effectsReconciled: true, findingsClosed: true }; output.integrationReady = { ...readyCore, hash: canonicalHash(readyCore) }; }
        return output;
      },
    },
    integration: { async reconcileExact({ reservation }) { assert.equal(reservation.operationKind, "integration"); integrationDelegations += 1; } },
  } });
  const conductorCtx = { cwd: conductorRoot, sessionManager: { getSessionId: () => "session-conductor", getSessionFile: () => null, getHeader: () => ({ type: "session", id: "session-conductor", cwd: conductorRoot }) } };

  const exerciseCrossProcessAdapterWindow = async (adapterKind, ownerResumeBoundary = null, crossSession = false) => {
    const root = await mkdtemp(join(tmpdir(), `pi-dag-owner-${adapterKind}-`)); let identityChild = null;
    try {
      await execFileAsync("git", ["init", "-b", "main"], { cwd: root });
      const artifacts = join(root, ".ai", "start-artifacts"); await mkdir(artifacts, { recursive: true });
      await writeFile(join(artifacts, "plan.json"), canonicalStringify(plan)); await writeFile(join(artifacts, "genesis.json"), canonicalStringify(conductorGenesis)); await writeFile(join(artifacts, "context.json"), canonicalStringify(conductorContext));
      const sessionId = `session-owner-${adapterKind}`; const ctx = { cwd: root, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null, getHeader: () => ({ type: "session", id: sessionId, cwd: root }) } };
      const successorSessionId = crossSession ? `${sessionId}-successor` : sessionId;
      const successorCtx = { cwd: root, sessionManager: { getSessionId: () => successorSessionId, getSessionFile: () => null, getHeader: () => ({ type: "session", id: successorSessionId, cwd: root }) } };
      const baseLifecycle = conductor.lifecycle; let initialCalls = 0; let successorCalls = 0;
      const initialLifecycle = adapterKind === "procedure"
        ? { ...baseLifecycle, procedure: { ...baseLifecycle.procedure, async executeExact() { initialCalls += 1; throw new Error("task113 procedure window"); } } }
        : { ...baseLifecycle, worker: { ...baseLifecycle.worker, async launchExact() { initialCalls += 1; throw new Error("task113 worker window"); } } };
      const starter = new DagConductorServiceV1({ lifecycle: initialLifecycle });
      await starter.startPrepared(ctx, { runId: conductorGenesis.runId, runNonce: conductorGenesis.runNonce, planHash: plan.planHash, maxActiveNodes: 1, occurredAt: NOW, plan, genesis: conductorGenesis, context: conductorContext, seedFacts: baselineFactValues, sourcePlanningPlanId: `planning-${adapterKind}`, sourcePlanningPlanHash: canonicalHash({ planning: adapterKind }) });
      await assert.rejects(() => starter.activate(ctx, conductorGenesis.runId, NOW), new RegExp(`task113 ${adapterKind} window`), `${adapterKind} fixture stops after durable dispatch and before external acknowledgement`);
      assert.equal(initialCalls, 1, `${adapterKind} fixture reaches exactly one external adapter call`);
      const binding = await starter.binding(ctx); const store = new DagRunSnapshotStoreV1(join(root, ".ai", "dag-runs-v1"), conductorGenesis.runId); const persistedContext = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")); const state = await store.read(persistedContext);
      const dispatchingEffects = Object.values(state.effects).filter((effect) => effect.state === "dispatching");
      if (adapterKind === "procedure") assert(dispatchingEffects.some((effect) => effect.kind === "run_procedure" && effect.boundStageAttemptId), "procedure window retains exact durable dispatch authority");
      else assert(dispatchingEffects.some((effect) => effect.kind === "launch_worker") && Object.keys(state.workerBindings).length === 0, "worker window retains launch dispatch without a worker binding");

      const childRoot = join(root, ".ai", "owner-child"); await mkdir(childRoot, { recursive: true }); const childInputPath = join(childRoot, "input.json"); const childLockPath = join(childRoot, "lock.json"); const releasePath = join(childRoot, "release");
      await writeFile(childInputPath, canonicalStringify(reducerInput(state, "set_desired_run", { desired: "running", reason: null, requestedBy: "conductor" }, { commandId: `owner-${adapterKind}-placeholder`, idempotencyKey: `owner-${adapterKind}-placeholder` })));
      await writeFile(childLockPath, canonicalStringify({ ownerTokenHash: canonicalHash({ adapterKind, token: true }), sessionId, pid: 1, processStartIdentity: "pending", lockIdentity: canonicalHash({ adapterKind, lock: true }), acquiredAt: NOW }));
      identityChild = spawn(process.execPath, ["scripts/fixtures/dag-store-child.mjs", store.rootDirectory, state.runId, join(store.runDirectory, "authority", "context.json"), childInputPath, childLockPath, "", "hold-identity", releasePath], { cwd: resolve("."), stdio: ["ignore", "pipe", "pipe"] });
      const childLock = await new Promise((resolveReady, rejectReady) => { let stdout = ""; let stderr = ""; identityChild.stdout.on("data", (chunk) => { stdout += chunk; const line = stdout.split("\n").find(Boolean); if (line) { try { resolveReady(JSON.parse(line)); } catch {} } }); identityChild.stderr.on("data", (chunk) => { stderr += chunk; }); identityChild.once("error", rejectReady); identityChild.once("exit", (code) => { if (code !== null && code !== 0) rejectReady(new Error(`owner ${adapterKind} child exited ${code}: ${stderr}`)); }); });
      const currentOwnership = await store.readImmutableFact(state.owner.ownershipReceipt); ownershipFacts.set(currentOwnership.hash, currentOwnership);
      const childOwner = { ownerTokenHash: childLock.ownerTokenHash, sessionId: childLock.sessionId, pid: childLock.pid, processStartIdentity: childLock.processStartIdentity, lockIdentity: childLock.lockIdentity }; const ownership = ownershipFactFor(state, childOwner, "same_manager"); await store.putImmutableFact(ownership);
      const transferred = await store.mutate({ input: reducerInput(state, "transfer_owner", { ...childOwner, ownershipReceipt: ownership.hash, priorOwnerDisposition: "same_manager" }, { commandId: `owner-${adapterKind}-transfer`, idempotencyKey: `owner-${adapterKind}-transfer` }), context: { ...persistedContext, facts: { ...(persistedContext.facts ?? {}), [ownership.hash]: ownership } }, lock: dagRunStoreLockIdentityFromOwner(state.owner) }); assert.equal(transferred.accepted, true, `real child assumes ${adapterKind} window authority: ${JSON.stringify(transferred)}`);
      const childBindingCore = { ...binding, ownerEpoch: transferred.state.owner.ownerEpoch, ownershipReceiptHash: transferred.state.owner.ownershipReceipt, lineage: { kind: "direct_fork", priorBindingHash: binding.bindingHash, priorSessionId: binding.sessionId, proofHash: ownership.hash }, boundAt: NOW }; delete childBindingCore.bindingHash; const childBinding = { ...childBindingCore, bindingHash: canonicalHash(childBindingCore) }; await store.putImmutableFact(childBinding); const [bindingName] = await readdir(join(root, ".ai", "dag-session-bindings-v1")); await writeFile(join(root, ".ai", "dag-session-bindings-v1", bindingName), canonicalStringify(childBinding));
      await writeFile(releasePath, "release\n"); await new Promise((resolveExit, rejectExit) => { identityChild.once("exit", (code) => code === 0 ? resolveExit() : rejectExit(new Error(`owner ${adapterKind} child exited ${code}`))); identityChild.once("error", rejectExit); }); identityChild = null;

      const marker = join(root, `.task113-${adapterKind}-marker`);
      const successorLifecycle = adapterKind === "procedure"
        ? { ...baseLifecycle, procedure: { ...baseLifecycle.procedure, async executeExact() { successorCalls += 1; await writeFile(marker, "procedure\n"); return null; } } }
        : { ...baseLifecycle, worker: { ...baseLifecycle.worker, async launchExact(request, exactState) { successorCalls += 1; await writeFile(marker, "worker\n"); return baseLifecycle.worker.launchExact(request, exactState); } } };
      let ownerResumeFailed = false;
      let successor = new DagConductorServiceV1({ lifecycle: successorLifecycle, ownerResumeFailpoint(point) { if (!ownerResumeFailed && point === ownerResumeBoundary) { ownerResumeFailed = true; throw new Error(`explicit-owner-resume-crash:${point}`); } } });
      await assert.rejects(() => successor.advance(successorCtx, transferred.state.runId, NOW), /Current conductor service does not own the exact DAG epoch|No exact current-session binding/, `${adapterKind} successor is fenced before external work`);
      assert.equal(successorCalls, 0, `${adapterKind} adapter is not invoked before current-process ownership proof`); await assert.rejects(() => readFile(marker, "utf8"), /ENOENT/, `${adapterKind} marker is absent before reattach`);
      const guard = { runId: transferred.state.runId, runNonce: transferred.state.runNonce, expectedRevision: transferred.state.revision, expectedSnapshotHash: transferred.state.snapshotHash, ownerEpoch: transferred.state.owner.ownerEpoch, commandId: `owner-${adapterKind}-reattach`, idempotencyKey: `owner-${adapterKind}-reattach`, occurredAt: NOW };
      let reattached;
      if (ownerResumeBoundary) {
        await assert.rejects(() => successor.reattach(successorCtx, guard), new RegExp(`explicit-owner-resume-crash:${ownerResumeBoundary}`));
        await successor.detach();
        successor = new DagConductorServiceV1({ lifecycle: successorLifecycle });
      }
      reattached = await successor.reattach(successorCtx, guard);
      const recoveredStartIdentity = await successor.startIdentity(successorCtx, reattached.runId);
      assert.equal(recoveredStartIdentity.sourcePlanningPlanId, `planning-${adapterKind}`, `${ownerResumeBoundary ?? "ordinary"} explicit reattach preserves the exact active start identity`);
      await successor.advance(successorCtx, reattached.runId, NOW);
      assert(successorCalls >= 1, `${adapterKind} external recovery runs only after exact dead-owner reattach`); assert.equal((await readFile(marker, "utf8")).trim(), adapterKind);
    } finally { identityChild?.kill("SIGKILL"); await rm(root, { recursive: true, force: true }); }
  };
  await exerciseCrossProcessAdapterWindow("procedure");
  // Owned-worker launch recovery is intentionally excluded here: stale owner packets fail closed and
  // only a current agent-visible dag_run_dispatch packet may cross the fresh-launch boundary.
  await exerciseCrossProcessAdapterWindow("procedure", null, true);
  for (const boundary of ["after_owner_transfer", "after_owner_binding", "after_owner_start_identity"]) await exerciseCrossProcessAdapterWindow("procedure", boundary, true);

  const conductorStarted = await conductor.start(conductorCtx, { runId: conductorGenesis.runId, runNonce: conductorGenesis.runNonce, planHash: plan.planHash, planPath: ".ai/start-artifacts/plan.json", genesisPath: ".ai/start-artifacts/genesis.json", contextPath: ".ai/start-artifacts/context.json", maxActiveNodes: 1, occurredAt: NOW });
  assert.equal(conductorStarted.state.owner.ownerEpoch, 1, "conductor emits the fully chained epoch-one ownership receipt");
  assert.equal(conductorStarted.state.owner.sessionId, "session-conductor");
  for (let pass = 0; pass < 100 && integrationDelegations === 0; pass += 1) {
    const status = await conductor.status(conductorCtx, conductorGenesis.runId);
    for (const packet of status.readyPackets) await conductor.dispatch(conductorCtx, packet, null, NOW);
    await conductor.activate(conductorCtx, conductorGenesis.runId, NOW);
  }
  assert(integrationDelegations > 0, "explicit agent dispatches advance owned stages until integration reconciliation is actionable");

  const conductorStore = new DagRunSnapshotStoreV1(join(conductorRoot, ".ai", "dag-runs-v1"), conductorGenesis.runId);
  const persistedConductorContext = parseStrictJson(await readFile(join(conductorStore.runDirectory, "authority", "context.json"), "utf8"));
  const priorState = await conductorStore.read(persistedConductorContext); const priorBinding = conductorStarted.binding;
  const priorOwnership = await conductorStore.readImmutableFact(priorState.owner.ownershipReceipt); ownershipFacts.set(priorOwnership.hash, priorOwnership);
  const childFixtureRoot = join(conductorRoot, ".ai", "owner-child"); await mkdir(childFixtureRoot, { recursive: true });
  const childContextPath = join(conductorStore.runDirectory, "authority", "context.json"); const childInputPath = join(childFixtureRoot, "input.json"); const childLockPath = join(childFixtureRoot, "lock.json"); const childReleasePath = join(childFixtureRoot, "release");
  await writeFile(childInputPath, canonicalStringify(reducerInput(priorState, "set_desired_run", { desired: "running", reason: null, requestedBy: "conductor" }, { commandId: "owner-child-placeholder", idempotencyKey: "owner-child-placeholder" })));
  await writeFile(childLockPath, canonicalStringify({ ownerTokenHash: H("9"), sessionId: "session-conductor", pid: 1, processStartIdentity: "pending", lockIdentity: H("8"), acquiredAt: NOW }));
  conductorOwnershipChild = spawn(process.execPath, ["scripts/fixtures/dag-store-child.mjs", conductorStore.rootDirectory, priorState.runId, childContextPath, childInputPath, childLockPath, "", "hold-identity", childReleasePath], { cwd: resolve("."), stdio: ["ignore", "pipe", "pipe"] });
  const abandonedChildLock = await new Promise((resolveReady, rejectReady) => {
    let stdout = ""; let stderr = "";
    conductorOwnershipChild.stdout.on("data", (chunk) => { stdout += chunk; const line = stdout.split("\n").find(Boolean); if (line) { try { resolveReady(JSON.parse(line)); } catch {} } });
    conductorOwnershipChild.stderr.on("data", (chunk) => { stderr += chunk; });
    conductorOwnershipChild.once("error", rejectReady); conductorOwnershipChild.once("exit", (code) => { if (code !== null && code !== 0) rejectReady(new Error(`owner identity child exited ${code}: ${stderr}`)); });
  });
  const abandonedSameSessionOwner = { ownerTokenHash: abandonedChildLock.ownerTokenHash, sessionId: abandonedChildLock.sessionId, pid: abandonedChildLock.pid, processStartIdentity: abandonedChildLock.processStartIdentity, lockIdentity: abandonedChildLock.lockIdentity };
  const abandonedOwnership = ownershipFactFor(priorState, abandonedSameSessionOwner, "same_manager"); await conductorStore.putImmutableFact(abandonedOwnership);
  const abandonedContext = { ...persistedConductorContext, facts: { ...(persistedConductorContext.facts ?? {}), [abandonedOwnership.hash]: abandonedOwnership } };
  const abandonedTransfer = await conductorStore.mutate({ input: reducerInput(priorState, "transfer_owner", { ...abandonedSameSessionOwner, ownershipReceipt: abandonedOwnership.hash, priorOwnerDisposition: "same_manager" }, { commandId: "conductor-abandon-same-session", idempotencyKey: "conductor-abandon-same-session" }), context: abandonedContext, lock: dagRunStoreLockIdentityFromOwner(priorState.owner) });
  assert.equal(abandonedTransfer.accepted, true, `same-session authority transfers to a distinct live child process identity: ${JSON.stringify(abandonedTransfer)}`);
  await writeFile(childReleasePath, "release\n"); await new Promise((resolveExit, rejectExit) => { conductorOwnershipChild.once("exit", (code) => code === 0 ? resolveExit() : rejectExit(new Error(`owner identity child exited ${code}`))); conductorOwnershipChild.once("error", rejectExit); }); conductorOwnershipChild = null;
  const abandonedBindingCore = { ...priorBinding, ownerEpoch: abandonedTransfer.state.owner.ownerEpoch, ownershipReceiptHash: abandonedTransfer.state.owner.ownershipReceipt, lineage: { kind: "direct_fork", priorBindingHash: priorBinding.bindingHash, priorSessionId: priorBinding.sessionId, proofHash: abandonedOwnership.hash }, boundAt: NOW }; delete abandonedBindingCore.bindingHash;
  const abandonedBinding = { ...abandonedBindingCore, bindingHash: canonicalHash(abandonedBindingCore) }; await conductorStore.putImmutableFact(abandonedBinding);
  const bindingDirectory = join(conductorRoot, ".ai", "dag-session-bindings-v1"); const [sameSessionBindingName] = await readdir(bindingDirectory);
  await writeFile(join(bindingDirectory, sameSessionBindingName), canonicalStringify(abandonedBinding));

  const integrationMarkerRef = "refs/pi-dag/v1/task113-owner-window"; const integrationMarkerPath = join(childFixtureRoot, "integration-marker.txt"); await writeFile(integrationMarkerPath, "task113 exact-owner integration marker\n"); const integrationMarkerOid = (await execFileAsync("git", ["hash-object", "-w", integrationMarkerPath], { cwd: conductorRoot })).stdout.trim();
  const restartedLifecycle = { ...conductor.lifecycle, integration: { async reconcileExact({ reservation }) { assert.equal(reservation.operationKind, "integration"); integrationDelegations += 1; await execFileAsync("git", ["update-ref", integrationMarkerRef, integrationMarkerOid], { cwd: conductorRoot }); } } };
  const restartedConductor = new DagConductorServiceV1({ lifecycle: restartedLifecycle });
  const delegationsBeforeOwnershipProof = integrationDelegations;
  await assert.rejects(() => restartedConductor.advance(conductorCtx, abandonedTransfer.state.runId, NOW), /Current conductor service does not own the exact DAG epoch/, "same-session successor cannot enter automatic integration work before explicit dead-owner reattach");
  assert.equal(integrationDelegations, delegationsBeforeOwnershipProof, "current ownership proof runs before invoking the integration adapter");
  await assert.rejects(() => execFileAsync("git", ["rev-parse", "--verify", integrationMarkerRef], { cwd: conductorRoot }), "successor cannot change any Git ref before exact reattach");
  const abandonedGuard = { runId: abandonedTransfer.state.runId, runNonce: abandonedTransfer.state.runNonce, expectedRevision: abandonedTransfer.state.revision, expectedSnapshotHash: abandonedTransfer.state.snapshotHash, ownerEpoch: abandonedTransfer.state.owner.ownerEpoch, commandId: "conductor-same-session-reattach", idempotencyKey: "conductor-same-session-reattach", occurredAt: NOW };
  const reattachedState = await restartedConductor.reattach(conductorCtx, abandonedGuard);
  const successorBinding = await restartedConductor.binding(conductorCtx);
  assert.equal(successorBinding.ownerEpoch, reattachedState.owner.ownerEpoch); assert.equal(successorBinding.ownershipReceiptHash, reattachedState.owner.ownershipReceipt);
  assert.equal(successorBinding.lineage.priorBindingHash, abandonedBinding.bindingHash, "same-session successor binding hash-binds the exact pre-transfer binding");
  assert.equal(successorBinding.lineage.proofHash, reattachedState.owner.ownershipReceipt, "same-session successor binding hash-binds the exact dead-owner ownership receipt");
  assert.equal((await restartedConductor.status(conductorCtx, reattachedState.runId)).state.snapshotHash, reattachedState.snapshotHash, "same-session restart status resolves through the canonical successor binding");
  const advancedAfterReattach = await restartedConductor.advance(conductorCtx, reattachedState.runId, NOW);
  assert.equal((await execFileAsync("git", ["rev-parse", "--verify", integrationMarkerRef], { cwd: conductorRoot })).stdout.trim(), integrationMarkerOid, "exact dead-owner reattach permits the previously fenced integration ref operation");
  const cancelGuard = { runId: advancedAfterReattach.state.runId, runNonce: advancedAfterReattach.state.runNonce, expectedRevision: advancedAfterReattach.state.revision, expectedSnapshotHash: advancedAfterReattach.state.snapshotHash, ownerEpoch: advancedAfterReattach.state.owner.ownerEpoch, commandId: "conductor-same-session-cancel", idempotencyKey: "conductor-same-session-cancel", occurredAt: NOW };
  const cancelledAfterReattach = await restartedConductor.control(conductorCtx, cancelGuard, "cancel", "same-session restart cancellation test");
  assert.equal(cancelledAfterReattach.desired.run, "cancelled", "same-session successor binding authorizes guarded advance and cancellation");

  const conflictingBindingCore = { ...successorBinding, ownerEpoch: abandonedBinding.ownerEpoch, ownershipReceiptHash: abandonedBinding.ownershipReceiptHash, boundAt: "2026-08-10T00:00:00.000Z" }; delete conflictingBindingCore.bindingHash;
  const conflictingBinding = { ...conflictingBindingCore, bindingHash: canonicalHash(conflictingBindingCore) }; await writeFile(join(bindingDirectory, sameSessionBindingName), canonicalStringify(conflictingBinding));
  const conflictRevision = cancelledAfterReattach.revision;
  await assert.rejects(() => restartedConductor.reattach(conductorCtx, { runId: cancelledAfterReattach.runId, runNonce: cancelledAfterReattach.runNonce, expectedRevision: cancelledAfterReattach.revision, expectedSnapshotHash: cancelledAfterReattach.snapshotHash, ownerEpoch: cancelledAfterReattach.owner.ownerEpoch, commandId: "conductor-conflicting-binding", idempotencyKey: "conductor-conflicting-binding", occurredAt: NOW }), /binding conflicts|pre-transfer authority|No exact current-session binding/, "conflicting same-session binding is rejected before owner transfer");
  assert.equal((await conductorStore.read(persistedConductorContext)).revision, conflictRevision, "conflicting binding rejection cannot advance owner authority");
} finally { conductorOwnershipChild?.kill("SIGKILL"); await rm(conductorRoot, { recursive: true, force: true }); }

async function exerciseRealManagerLifecycle(label, terminalOverrides = {}, launchCrash = null, stopAfterOwnedBinding = false) {
  const root = await mkdtemp(join(tmpdir(), `pi-dag-real-${label}-`));
  let manager;
  const priorMode = process.env.FAKE_WORKER_RPC_MODE;
  try {
    await execFileAsync("git", ["init", "-b", "main"], { cwd: root });
    await execFileAsync("git", ["config", "user.name", "DAG Test"], { cwd: root }); await execFileAsync("git", ["config", "user.email", "dag-test@example.invalid"], { cwd: root });
    await writeFile(join(root, "tracked.txt"), "baseline\n"); await execFileAsync("git", ["add", "tracked.txt"], { cwd: root }); await execFileAsync("git", ["commit", "-m", "baseline"], { cwd: root });
    const commit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd: root })).stdout.trim(); const tree = (await execFileAsync("git", ["rev-parse", "HEAD^{tree}"], { cwd: root })).stdout.trim();
    const exactPlan = planFixture({ repositoryId: "repo-main", commit, tree }); const exactIndex = buildSchedulerPlanIndexV1(exactPlan); const genesis = clone(runFixture(exactPlan));
    genesis.runId = `run-real-${label}`; genesis.runNonce = `nonce-real-${label}-0123456789`; genesis.scheduler.policyHash = DAG_SCHEDULER_POLICY_HASH_V1; genesis.scheduler.normalizedIndexHash = exactIndex.indexHash;
    genesis.workItems["item-api"].current = "ready"; genesis.current.run = "active"; genesis.current.readyWorkItemIds = ["item-api"]; rehashRun(genesis);
    const authorization = authorizationBinding(exactPlan, genesis.identity.reviewReceipt.hash, genesis.identity.authorizationReceipts.map(({ hash }) => hash), genesis.identity.authorizationSet.hash);
    const exactContext = { plan: exactPlan, authorization, historicalAuthorizations: {}, catalog: catalogBinding(exactPlan), normalizedSchedulerIndexHash: exactIndex.indexHash, facts: {}, integrationValidationProfiles: INTEGRATION_VALIDATION_PROFILES };
    const seedFacts = [simpleFact("plan_review", "review-plan"), simpleFact("plan_authorization", "authorization-plan"), simpleFact("staleness", "freshness"), simpleFact("repository_observation", "repo-main-observation"), authorization];
    const artifacts = join(root, ".ai", "start-artifacts"); await mkdir(artifacts, { recursive: true });
    await writeFile(join(artifacts, "plan.json"), canonicalStringify(exactPlan)); await writeFile(join(artifacts, "genesis.json"), canonicalStringify(genesis)); await writeFile(join(artifacts, "context.json"), canonicalStringify({ ...exactContext, seedFacts }));
    const sessionId = `real-manager-${label}`; const sessionFile = join(root, "session.jsonl"); await writeFile(sessionFile, `${JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: NOW, cwd: root })}\n`);
    const ctx = { cwd: root, model: { provider: "fake-provider", id: "fake-model" }, thinkingLevel: "off", sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile, getHeader: () => ({ type: "session", id: sessionId, cwd: root }) } };
    process.env.FAKE_WORKER_RPC_MODE = "valid";
    manager = new WorkerManager({ getActiveTools: () => ["read", "subagent_report"], sendMessage() {} }, { piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"), watchIntervalMs: 60_000 });
    await manager.attach(ctx);
    const adapters = managerLifecycleAdapters(manager, exactContext, terminalOverrides); const conductor = new DagConductorServiceV1({ lifecycle: adapters.options });
    let launchCrashTriggered = false;
    if (launchCrash) {
      const launchExact = adapters.options.worker.launchExact;
      adapters.options.worker.launchExact = async (request, state) => {
        const attempt = Object.values(state.stageAttempts).find((candidate) => candidate.launchIntentId && state.launchIntents[candidate.launchIntentId]?.workerId === request.workerId);
        const shouldCrash = !launchCrashTriggered && attempt?.stage === "F1";
        if (shouldCrash && launchCrash === "dispatch_recorded") { launchCrashTriggered = true; throw new Error("simulated crash after durable dispatch before external launch"); }
        const observation = await launchExact(request, state);
        if (shouldCrash && launchCrash === "launch_returned") { launchCrashTriggered = true; throw new Error("simulated crash after launch return before DAG binding"); }
        return observation;
      };
    }
    let state; let crashError = null; let crashPacket = null;
    state = (await conductor.start(ctx, { runId: genesis.runId, runNonce: genesis.runNonce, planHash: exactPlan.planHash, planPath: ".ai/start-artifacts/plan.json", genesisPath: ".ai/start-artifacts/genesis.json", contextPath: ".ai/start-artifacts/context.json", maxActiveNodes: 1, occurredAt: NOW })).state;
    const dispatchReady = async () => {
      const status = await conductor.status(ctx, genesis.runId);
      for (const packet of status.readyPackets) {
        try { state = (await conductor.dispatch(ctx, packet, null, NOW)).state; }
        catch (error) {
          if (!launchCrash || !launchCrashTriggered || !/simulated crash/.test(error.message)) throw error;
          crashError = error; crashPacket = packet; state = (await conductor.status(ctx, genesis.runId)).state; return;
        }
      }
    };
    await dispatchReady();
    if (!crashError) for (let pass = 0; pass < 150; pass += 1) {
      const done = terminalOverrides.F1 === "cancelled" ? state.workItems["item-api"].stages.F1.state === "blocked" : state.workItems["item-api"].stages.F5.state === "passed";
      if (done || (stopAfterOwnedBinding && Object.keys(state.workerBindings).length > 0)) break;
      await new Promise((resolveWait) => setTimeout(resolveWait, 20));
      state = (await conductor.advance(ctx, genesis.runId, NOW)).state;
      await dispatchReady();
      if (crashError) break;
    }
    return { root, manager, ctx, exactPlan, exactContext, conductor, adapters, state, priorMode, crashError, crashPacket };
  } catch (error) {
    if (manager) await manager.detach();
    await rm(root, { recursive: true, force: true });
    if (priorMode === undefined) delete process.env.FAKE_WORKER_RPC_MODE; else process.env.FAKE_WORKER_RPC_MODE = priorMode;
    throw error;
  }
}

const realLifecycle = await exerciseRealManagerLifecycle("f0-f5");
try {
  assert.equal(realLifecycle.state.workItems["item-api"].stages.F5.state, "passed", "real WorkerManager lifecycle advances through exact F0-F5 closure");
  const independent = ["F1", "F2", "F5"].map((stage) => realLifecycle.state.workerBindings[realLifecycle.state.workItems["item-api"].stages[stage].currentAttemptId]);
  assert.equal(new Set(independent.map(({ workerStorageId }) => workerStorageId)).size, 1, "F1/F2/F5 share one real manager session storage identity");
  assert.equal(new Set(independent.map(({ workerId }) => workerId)).size, 3); assert.equal(new Set(independent.map(({ attemptNonce }) => attemptNonce)).size, 3); assert.equal(new Set(independent.map(({ configHash }) => configHash)).size, 3, "independent lifecycle roles retain distinct exact worker/nonce/config identities in shared storage");
  const implementation = realLifecycle.adapters.launches.find(({ stage }) => stage === "F1"); const replay = await realLifecycle.manager.launchOwnedAttempt(implementation.request);
  assert.equal(replay.attemptNonce, implementation.observation.attemptNonce); assert.equal(replay.configHash, implementation.observation.configHash);
  const managerState = await realLifecycle.manager.store.load(); assert.equal(managerState.workers[implementation.request.workerId].attempts.length, 1, "exact same-launch replay preserves the original attempt while cross-attempt launch-key reuse already failed closed");
} finally {
  await realLifecycle.manager.detach(); await rm(realLifecycle.root, { recursive: true, force: true });
  if (realLifecycle.priorMode === undefined) delete process.env.FAKE_WORKER_RPC_MODE; else process.env.FAKE_WORKER_RPC_MODE = realLifecycle.priorMode;
}

const pausedLifecycle = await exerciseRealManagerLifecycle("paused-inflight", {}, null, true);
try {
  const initialChoices = await pausedLifecycle.conductor.nextAction(pausedLifecycle.ctx, pausedLifecycle.state.runId);
  const pauseAction = initialChoices.controls.find((candidate) => candidate.operation === "pause");
  assert(pauseAction, "an active run exposes pause as admission control");
  let paused = (await pausedLifecycle.conductor.pauseSemantic(pausedLifecycle.ctx, pausedLifecycle.state.runId, pauseAction.actionId, "hold new admission while accepting the in-flight worker")).state;
  assert.equal(paused.desired.run, "paused");
  let completionAction = null;
  for (let pass = 0; pass < 150 && !completionAction; pass += 1) {
    await new Promise((resolveWait) => setTimeout(resolveWait, 20));
    const choices = await pausedLifecycle.conductor.nextAction(pausedLifecycle.ctx, paused.runId);
    completionAction = choices.frontier.find((candidate) => candidate.operation === "record_completion");
    assert.equal(choices.frontier.some((candidate) => ["start_work", "run_checks", "integrate", "retry"].includes(candidate.operation)), false, "pause suppresses fresh reservation launch/attempt creation as well as fresh admission choices");
    assert(choices.frontier.every((candidate) => ["record_completion", "finalize"].includes(candidate.operation)), "paused frontier contains only observation/finalization for work with an actual worker binding");
  }
  assert(completionAction, "paused run exposes an in-flight durable worker completion without background ingestion");
  paused = (await pausedLifecycle.conductor.recordCompletion(pausedLifecycle.ctx, paused.runId, completionAction.actionId, completionAction.stageAttemptId, completionAction.completionId)).state;
  const finalizationChoices = await pausedLifecycle.conductor.nextAction(pausedLifecycle.ctx, paused.runId);
  assert(finalizationChoices.frontier.some((candidate) => candidate.operation === "finalize" && candidate.stageAttemptId === completionAction.stageAttemptId), "paused run keeps the recorded in-flight worker result finalizable");
} finally {
  await pausedLifecycle.manager.detach(); await rm(pausedLifecycle.root, { recursive: true, force: true });
  if (pausedLifecycle.priorMode === undefined) delete process.env.FAKE_WORKER_RPC_MODE; else process.env.FAKE_WORKER_RPC_MODE = pausedLifecycle.priorMode;
}

const cancelledLifecycle = await exerciseRealManagerLifecycle("cancelled", { F1: "cancelled" });
try {
  const cancelledAttemptId = cancelledLifecycle.state.workItems["item-api"].stages.F1.currentAttemptId;
  assert.equal(cancelledLifecycle.state.workItems["item-api"].stages.F1.state, "blocked"); assert.equal(cancelledLifecycle.state.workItems["item-api"].stages.F1.lastDisposition, "BLOCKED", "retry-safe cancelled terminal canonically seals BLOCKED");
  assert.equal(cancelledLifecycle.adapters.procedureCalls.get(cancelledAttemptId), 1, "cancelled terminal seals in one lifecycle procedure pass");
  const revision = cancelledLifecycle.state.revision; await cancelledLifecycle.conductor.detach(); const restarted = new DagConductorServiceV1({ lifecycle: cancelledLifecycle.adapters.options });
  const afterRestart = await restarted.activate(cancelledLifecycle.ctx, cancelledLifecycle.state.runId, new Date(Date.parse(cancelledLifecycle.state.updatedAt) + 1).toISOString());
  assert.equal(afterRestart.state.revision, revision); assert.equal(afterRestart.state.owner.ownerEpoch, cancelledLifecycle.state.owner.ownerEpoch, "fresh same-process service derives the durable owner lock without process-local generation ceremony"); assert.equal(cancelledLifecycle.adapters.procedureCalls.get(cancelledAttemptId), 1, "restart observes the sealed cancelled attempt without a retry loop");
} finally {
  await cancelledLifecycle.manager.detach(); await rm(cancelledLifecycle.root, { recursive: true, force: true });
  if (cancelledLifecycle.priorMode === undefined) delete process.env.FAKE_WORKER_RPC_MODE; else process.env.FAKE_WORKER_RPC_MODE = cancelledLifecycle.priorMode;
}

for (const launchCrash of ["dispatch_recorded", "launch_returned"]) {
  const recoveryLifecycle = await exerciseRealManagerLifecycle(`cancel-${launchCrash}`, {}, launchCrash);
  try {
    assert.match(recoveryLifecycle.crashError?.message ?? "", /simulated crash/, `${launchCrash} fixture crosses its exact pre-bind crash boundary`);
    const attemptId = recoveryLifecycle.state.workItems["item-api"].stages.F1.currentAttemptId;
    const attempt = recoveryLifecycle.state.stageAttempts[attemptId]; const launch = recoveryLifecycle.state.launchIntents[attempt.launchIntentId]; const launchEffectBeforeCancellation = recoveryLifecycle.state.effects[launch.effectId];
    assert.equal(launchEffectBeforeCancellation.state, "dispatching"); assert.equal(launchEffectBeforeCancellation.dispatchCount, 1); assert.equal(recoveryLifecycle.state.workerBindings[attemptId], undefined, `${launchCrash} crash leaves durable dispatch authority without a DAG worker binding`);
    const restarted = recoveryLifecycle.conductor;
    const recoveryStatus = await restarted.status(recoveryLifecycle.ctx, recoveryLifecycle.state.runId);
    const recoveryPacket = recoveryStatus.readyPackets.find((packet) => packet.stageAttemptId === attemptId);
    assert(recoveryPacket, `${launchCrash} exposes an exact current recovery packet without a background launch`);
    const preCancelChoices = await restarted.nextAction(recoveryLifecycle.ctx, recoveryLifecycle.state.runId);
    const cancelAction = preCancelChoices.controls.find((candidate) => candidate.operation === "cancel");
    assert(cancelAction, `${launchCrash} exposes cancellation before the pre-bind launch is recovered`);
    let cancelled = (await restarted.cancelSemantic(recoveryLifecycle.ctx, recoveryLifecycle.state.runId, cancelAction.actionId, `cancel exact pre-bind worker after ${launchCrash}`)).state;
    const cancellationId = Object.values(cancelled.cancellations).find((candidate) => candidate.state !== "closed").cancellationId;
    const cancellationChoices = await restarted.nextAction(recoveryLifecycle.ctx, recoveryLifecycle.state.runId);
    const recoveryAction = cancellationChoices.frontier.find((candidate) => candidate.operation === "start_work" && candidate.stageAttemptId === attemptId);
    assert(recoveryAction, `${launchCrash} exposes exact dag_start_work replay while cancellation is pending`);
    assert.equal(recoveryAction.concurrency.independent, undefined, "revision-bound semantic choices do not advertise stale siblings as independently composable");
    const reconciledDispatch = await restarted.startWork(recoveryLifecycle.ctx, recoveryLifecycle.state.runId, recoveryAction.actionId, attempt.workItemId, attempt.stage, undefined);
    assert(reconciledDispatch.state.workerBindings[attemptId], `${launchCrash} exact semantic replay binds the one intended worker under pending cancellation`);
    await assert.rejects(() => restarted.finalizeSemantic(recoveryLifecycle.ctx, recoveryLifecycle.state.runId, cancellationChoices.frontier.find((candidate) => candidate.operation === "finalize").actionId), /stale|consumed/, "a sibling choice from the pre-replay revision must be refreshed");
    for (let pass = 0; pass < 150 && cancelled.cancellations[cancellationId].state !== "closed"; pass += 1) {
      await new Promise((resolveWait) => setTimeout(resolveWait, 20));
      const choices = await restarted.nextAction(recoveryLifecycle.ctx, recoveryLifecycle.state.runId);
      const finalize = choices.frontier.find((candidate) => candidate.operation === "finalize" && candidate.finalizationKind === "cancellation");
      assert(finalize, "pending cancellation remains explicitly finalizable after pre-bind replay");
      cancelled = (await restarted.finalizeSemantic(recoveryLifecycle.ctx, recoveryLifecycle.state.runId, finalize.actionId)).state;
    }
    assert.equal(cancelled.cancellations[cancellationId].state, "closed", `${launchCrash} cancellation-first agent replay recovers, binds, cancels, and exactly closes the pre-bind worker`);
    assert.equal(cancelled.effects[launch.effectId].reconciliation, "applied_exact", "the original launch effect is reconciled under its unchanged durable identity");
    const exactBinding = cancelled.workerBindings[attemptId]; assert(exactBinding, "cancellation recovery binds the exact worker attempt before issuing cancellation");
    const cancellationEffects = cancelled.cancellations[cancellationId].effectIds.map((effectId) => cancelled.effects[effectId]);
    assert.equal(cancellationEffects.length, 1); assert.equal(cancellationEffects[0].kind, "cancel_worker"); assert(["applied_exact", "proven_absent"].includes(cancellationEffects[0].reconciliation), "exact worker cancellation is terminally reconciled");
    assert(cancelled.stageAttempts[attemptId].terminalAt !== null, "cancellation closes only after observing the exact terminal worker result");
    assert(cancelled.stageAttempts[attemptId].leaseIds.every((leaseId) => cancelled.leases[leaseId].state === "released"), "all exact stage/resource authority releases only after worker/effect reconciliation");
    const managerState = await recoveryLifecycle.manager.store.load(); const launchRecord = managerState.launchRecords.find((record) => record.launchKey === launch.launchKey); const managerWorker = launchRecord ? managerState.workers[launchRecord.workerId] : null;
    assert(launchRecord && managerWorker); assert.equal(managerWorker.attempts.length, 1, "old dispatch and cancellation recovery converge on one manager attempt without an untracked worker race");
    const recoveryCalls = recoveryLifecycle.adapters.launches.filter(({ request }) => request.launchKey === launch.launchKey);
    assert.equal(new Set(recoveryCalls.map(({ request }) => canonicalHash(request))).size, 1, "every recovery call reuses the exact launchKey/request identity instead of minting fresh authority");
    assert.equal(managerWorker.attempts[0].attemptNonce, exactBinding.attemptNonce); assert.equal(managerWorker.attempts[0].configHash, exactBinding.configHash, "the sole manager attempt is the exact DAG-bound cancellation target");
  } finally {
    await recoveryLifecycle.manager.detach(); await rm(recoveryLifecycle.root, { recursive: true, force: true });
    if (recoveryLifecycle.priorMode === undefined) delete process.env.FAKE_WORKER_RPC_MODE; else process.env.FAKE_WORKER_RPC_MODE = recoveryLifecycle.priorMode;
  }
}

const ownedIdentityRoot = await mkdtemp(join(tmpdir(), "pi-dag-owned-identity-"));
try {
  await execFileAsync("git", ["init", "-b", "main"], { cwd: ownedIdentityRoot });
  await execFileAsync("git", ["config", "user.name", "DAG Test"], { cwd: ownedIdentityRoot }); await execFileAsync("git", ["config", "user.email", "dag-test@example.invalid"], { cwd: ownedIdentityRoot });
  await writeFile(join(ownedIdentityRoot, "tracked.txt"), "baseline\n"); await execFileAsync("git", ["add", "tracked.txt"], { cwd: ownedIdentityRoot }); await execFileAsync("git", ["commit", "-m", "baseline"], { cwd: ownedIdentityRoot });
  const baseCommit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd: ownedIdentityRoot })).stdout.trim();
  const unsafeSchemaNonce = "nonce with spaces/~^?0123456789"; const unsafeNonceCandidateRef = privateCandidateRefV1(unsafeSchemaNonce, "attempt-safe-nonce-ref");
  assert(!unsafeNonceCandidateRef.includes(unsafeSchemaNonce) && !/[ ~^?]/.test(unsafeNonceCandidateRef), "every dynamic private candidate ref segment is digest-encoded rather than embedding a schema-valid unsafe run nonce");
  assert.equal(await sealPrivateCandidateRefV1(ownedIdentityRoot, unsafeSchemaNonce, "attempt-safe-nonce-ref", baseCommit), unsafeNonceCandidateRef);
  await execFileAsync("git", ["check-ref-format", unsafeNonceCandidateRef], { cwd: ownedIdentityRoot }); assert.equal((await execFileAsync("git", ["rev-parse", "--verify", unsafeNonceCandidateRef], { cwd: ownedIdentityRoot })).stdout.trim(), baseCommit, "unsafe schema nonce seals one valid immutable private Git ref");
  assert.equal(await sealPrivateCandidateRefV1(ownedIdentityRoot, unsafeSchemaNonce, "attempt-safe-nonce-ref", baseCommit), unsafeNonceCandidateRef, "private candidate seal replays exactly");
  const sessionFile = join(ownedIdentityRoot, "session.jsonl"); await writeFile(sessionFile, `${JSON.stringify({ type: "session", version: 3, id: "owned-identity-session", timestamp: NOW, cwd: ownedIdentityRoot })}\n`);
  const pi = { getActiveTools: () => ["read", "subagent_report"], sendMessage() {} }; let ownedSpawnCount = 0; const spawnOwnedSupervisor = async () => { ownedSpawnCount += 1; return { pid: process.pid, unref() {} }; }; const manager = new WorkerManager(pi, { piCliPath: process.execPath, watchIntervalMs: 60_000, observeUninspectableProcesses: async () => ({ status: "observed", processes: [] }), spawnSupervisor: spawnOwnedSupervisor });
  const managerCtx = { cwd: ownedIdentityRoot, model: { provider: "test", id: "test" }, thinkingLevel: "off", sessionManager: { getSessionId: () => "owned-identity-session", getSessionFile: () => sessionFile, getHeader: () => ({ type: "session", id: "owned-identity-session", cwd: ownedIdentityRoot }) } }; await manager.attach(managerCtx);
  const launchRequest = { launchKey: "dag-owned-identity", workerId: "worker-owned-identity", expectedAttemptNumber: 1, configRequestHash: H("4"), baseCommit, worktreeKey: "dag-owned-identity", label: "owned identity", task: "Return exact bounded evidence." };
  const identity = await manager.launchOwnedAttempt(launchRequest, managerCtx); const replay = await manager.launchOwnedAttempt(launchRequest, managerCtx); const originalOwnedStore = manager.store;
  assert.equal(identity.workerStorageId, "owned-identity-session"); assert.equal(identity.launchOwnerSessionId, "owned-identity-session"); assert.equal(identity.workerId, launchRequest.workerId); assert.equal(identity.attemptNumber, 1); assert.equal(identity.configFact.kind, "worker_config"); assert.equal(identity.configFact.config.requestHash, launchRequest.configRequestHash, "real manager path binds exact launch/config identity in a per-node disposable Git worktree"); assert.deepEqual(replay, identity, "real opaque launch-key replay returns the same full worker identity without another generation");
  const pureReadBefore = canonicalHash(await manager.store.load());
  const pureProjection = await manager.readBoundAttempts([identity]); const pureTerminal = await manager.terminalResultForBinding(identity); const pureInspection = await manager.inspectBindingReadOnly(identity);
  assert.equal(pureProjection.length, 1); assert.equal(pureTerminal, null); assert.equal(pureInspection.attempt.attemptNonce, identity.attemptNonce);
  assert.equal(canonicalHash(await manager.store.load()), pureReadBefore, "frontier worker projection, exact inspection, and terminal lookup never scan, launch, ingest, or mutate worker state");
  await writeFile(join(ownedIdentityRoot, "wrong.txt"), "wrong base\n"); await execFileAsync("git", ["add", "wrong.txt"], { cwd: ownedIdentityRoot }); await execFileAsync("git", ["commit", "-m", "wrong existing head"], { cwd: ownedIdentityRoot });
  const wrongHead = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd: ownedIdentityRoot })).stdout.trim(); const wrongRoot = join(ownedIdentityRoot, ".ai", "worker-roots", "dag-owned-wrong-head");
  await execFileAsync("git", ["worktree", "add", "--detach", wrongRoot, wrongHead], { cwd: ownedIdentityRoot });
  await assert.rejects(() => manager.launchOwnedAttempt({ ...launchRequest, launchKey: "dag-owned-wrong-head", workerId: "worker-owned-wrong-head", worktreeKey: "dag-owned-wrong-head" }, managerCtx), /does not match exact base/, "existing disposable worktree with the wrong clean detached HEAD is rejected before launch authority");
  await manager.detach();
  await originalOwnedStore.mutate((draft) => { draft.quarantinedArtifacts.push({ quarantineId: "unbound-launch-conflict", workerId: identity.workerId, attemptNumber: identity.attemptNumber, attemptNonce: identity.attemptNonce, configHash: identity.configHash, kind: "test-conflict", reason: "exact unbound recovery must fail closed", sourcePath: "test", retainedPath: "test", envelopePath: "test", factHash: H("7"), byteLength: 1, retainedComplete: true }); });
  const successorSessionFile = join(ownedIdentityRoot, "successor-session.jsonl"); await writeFile(successorSessionFile, `${JSON.stringify({ type: "session", version: 3, id: "owned-identity-successor", timestamp: NOW, cwd: ownedIdentityRoot })}\n`);
  const successorManager = new WorkerManager(pi, { piCliPath: process.execPath, watchIntervalMs: 60_000, observeUninspectableProcesses: async () => ({ status: "observed", processes: [] }), spawnSupervisor: spawnOwnedSupervisor });
  const successorCtx = { ...managerCtx, sessionManager: { getSessionId: () => "owned-identity-successor", getSessionFile: () => successorSessionFile, getHeader: () => ({ type: "session", id: "owned-identity-successor", cwd: ownedIdentityRoot }) } };
  await successorManager.attach(successorCtx);
  await assert.rejects(() => successorManager.launchOwnedAttempt(launchRequest, successorCtx), /quarantined conflicting artifacts|failed closed/, "unbound cross-store recovery fails closed when the otherwise exact launch has quarantine conflict authority");
  await originalOwnedStore.mutate((draft) => { draft.quarantinedArtifacts = draft.quarantinedArtifacts.filter(({ quarantineId }) => quarantineId !== "unbound-launch-conflict"); });
  const recoveredUnbound = await successorManager.launchOwnedAttempt(launchRequest, successorCtx);
  assert.equal(recoveredUnbound.workerStorageId, identity.workerStorageId, "successor with no direct-parent lineage recovers the original manager storage before a new DAG launch reservation");
  assert.equal(recoveredUnbound.attemptNonce, identity.attemptNonce); assert.equal(recoveredUnbound.configHash, identity.configHash); assert.equal(recoveredUnbound.supervisorPid, identity.supervisorPid); assert.equal(recoveredUnbound.supervisorStartIdentity, identity.supervisorStartIdentity);
  const historicalState = await originalOwnedStore.load(); const historicalAttempt = historicalState.workers[identity.workerId].attempts[0]; const historicalPaths = attemptPaths(ownedIdentityRoot, identity.workerStorageId, identity.workerId, identity.attemptNumber);
  const unrelatedRequest = { ...historicalState.workers[identity.workerId].normalizedRequest, requestedWorkerId: "worker-unrelated-zero-attempt", requestedLabel: "unrelated zero attempt", task: "This unrelated reservation must never launch during targeted reconciliation." }; const unrelatedRequestHash = canonicalHash(unrelatedRequest); const unrelatedReservedAt = new Date().toISOString();
  await originalOwnedStore.mutate((draft) => { draft.workers["worker-unrelated-zero-attempt"] = { id: "worker-unrelated-zero-attempt", label: "unrelated zero attempt", task: unrelatedRequest.task, cwd: unrelatedRequest.cwd, status: "launching", createdAt: unrelatedReservedAt, updatedAt: unrelatedReservedAt, currentAttempt: 0, attempts: [], launchKey: "unrelated-zero-attempt", requestHash: unrelatedRequestHash, normalizedRequest: unrelatedRequest, launchOptions: { reportRepairAttempts: unrelatedRequest.reportRepairAttempts } }; draft.launchRecords.push({ launchKey: "unrelated-zero-attempt", requestHash: unrelatedRequestHash, workerId: "worker-unrelated-zero-attempt", reservedAt: unrelatedReservedAt }); });
  const historicalTerminal = withResultHash({ schemaVersion: 1, completionId: "completion-historical-explicit-reconcile", storageId: identity.workerStorageId, ownerSessionId: identity.launchOwnerSessionId, workerId: identity.workerId, attemptNumber: identity.attemptNumber, attemptNonce: identity.attemptNonce, configHash: identity.configHash, terminalStatus: "cancelled", reportStatus: "missing", startedAt: historicalAttempt.createdAt, endedAt: new Date().toISOString(), runtime: { recovery: true } });
  await writeImmutableJson(historicalPaths.recoveryResult, historicalTerminal);
  const historicalBeforePureReads = canonicalHash(await originalOwnedStore.load());
  await successorManager.readBoundAttempts([recoveredUnbound]); await successorManager.inspectBindingReadOnly(recoveredUnbound); const observedHistoricalTerminal = await successorManager.terminalResultForBinding(recoveredUnbound);
  assert.equal(observedHistoricalTerminal.completionId, historicalTerminal.completionId, "pure historical lookup exposes an exact terminal file that has not yet been ingested");
  assert.equal(canonicalHash(await originalOwnedStore.load()), historicalBeforePureReads, "cross-store projection, exact inspection, and terminal reads never transfer historical ownership, append lineage, or ingest");
  assert.equal((await successorManager.summary()).storageId, "owned-identity-successor", "pure historical reads never replace the successor manager's attached store");
  const reconciledHistoricalTerminal = await successorManager.terminalResultForBinding(recoveredUnbound, { reconcile: true });
  assert.equal(reconciledHistoricalTerminal.completionId, historicalTerminal.completionId, "explicit semantic reconciliation ingests the exact historical terminal");
  const historicalAfterReconcile = await originalOwnedStore.load();
  assert(historicalAfterReconcile.workers[identity.workerId].attempts[0].ingestedAt && historicalAfterReconcile.lineage.length === 1, "explicit reconciliation transfers historical ownership and appends one successor lineage receipt");
  assert.equal(historicalAfterReconcile.workers["worker-unrelated-zero-attempt"].currentAttempt, 0); assert.deepEqual(historicalAfterReconcile.workers["worker-unrelated-zero-attempt"].attempts, [], "targeted historical reconciliation cannot launch or dispatch an unrelated zero-attempt reservation");
  assert.equal(ownedSpawnCount, 1, "crash after manager launch return but before DAG bind never duplicates the supervisor spawn");
  const worktreeEntries = (await execFileAsync("git", ["worktree", "list", "--porcelain"], { cwd: ownedIdentityRoot })).stdout.split(/\r?\n/).filter((line) => line === `worktree ${join(ownedIdentityRoot, ".ai", "worker-roots", launchRequest.worktreeKey)}`);
  assert.equal(worktreeEntries.length, 1, "unbound launch recovery adopts one original disposable worktree without duplication");
  const priorSessionInspection = await successorManager.inspectBindingReadOnly(recoveredUnbound);
  assert.equal(priorSessionInspection.attempt.attemptNonce, identity.attemptNonce, "successor manager opens and transfers the exact recovered prior-session storage/attempt instead of using its current store");
  const cancellation = await successorManager.cancelBinding(recoveredUnbound, "test recovered unbound launch cancellation");
  assert.equal(cancellation.status, "cancelled"); assert.equal(cancellation.alreadyTerminal, true, "the explicitly reconciled historical worker remains exactly binding-addressable and terminal");
  assert.equal((await successorManager.summary()).storageId, "owned-identity-successor", "binding-scoped recovery/cancellation restores the successor's current manager store");
  await successorManager.detach();
} finally { await rm(ownedIdentityRoot, { recursive: true, force: true }); }

const conductorPi = { tools: [], handlers: new Map(), registerTool(tool) { this.tools.push(tool); }, on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); } };
let conductorResumeCalls = 0;
registerCanonicalDagRuntime(conductorPi, { async binding() { return null; }, async resumeBound() { conductorResumeCalls += 1; return null; }, detach() {} });
assert.deepEqual(conductorPi.tools.map(({ name }) => name).sort(), ["dag_cancel", "dag_finalize", "dag_integrate", "dag_next_action", "dag_pause", "dag_record_completion", "dag_resume", "dag_retry", "dag_run_checks", "dag_run_diagram", "dag_run_explain", "dag_run_inspect", "dag_run_start", "dag_run_status", "dag_run_tail", "dag_start_work"], "runtime exposes historical readers and named semantic orchestration operations without guarded packet transport");
assert(conductorPi.tools.every(({ parameters }) => parameters.additionalProperties === false), "all canonical DAG tools reject unknown fields");
const internalGuardFields = new Set(["runNonce", "expectedRevision", "expectedSnapshotHash", "ownerEpoch", "commandId", "idempotencyKey", "occurredAt", "readyPacket"]);
for (const tool of conductorPi.tools.filter(({ name }) => ["dag_start_work", "dag_run_checks", "dag_record_completion", "dag_integrate", "dag_retry", "dag_pause", "dag_resume", "dag_cancel", "dag_finalize"].includes(name))) {
  assert.equal(Object.keys(tool.parameters.properties ?? {}).some((key) => internalGuardFields.has(key)), false, `${tool.name} derives routine guard fields internally`);
  assert(tool.parameters.required.includes("actionId"), `${tool.name} requires the exact agent-visible semantic action identity`);
}
for (const handler of conductorPi.handlers.get("session_start") ?? []) await handler({}, { hasUI: false, cwd: "/tmp", sessionManager: { getSessionId: () => "headless" }, ui: {} });
assert.equal(conductorResumeCalls, 0, "session attachment never pumps or mutates canonical DAG state");
assert.equal(conductorPi.handlers.has("agent_end"), false, "agent_end has no canonical DAG pump");
const guidancePi = { tools: [], handlers: new Map(), registerTool(tool) { this.tools.push(tool); }, on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); } };
registerCanonicalDagRuntime(guidancePi, { async binding() { return { runId: "run-guidance" }; }, detach() {} });
const guidanceHandler = guidancePi.handlers.get("before_agent_start")?.[0]; const guided = await guidanceHandler({ systemPrompt: "base" }, { cwd: "/tmp", sessionManager: { getSessionId: () => "guidance" } });
assert.match(guided.systemPrompt, /Call dag_next_action/); assert.match(guided.systemPrompt, /dag_start_work/); assert.match(guided.systemPrompt, /recovery, recording, or finalization/); assert.match(guided.systemPrompt, /Every choice is revision-bound/); assert.doesNotMatch(guided.systemPrompt, /independent semantic frontier/); assert.match(guided.systemPrompt, /never invent or transport revisions, hashes, epochs, locks/); assert.match(guided.systemPrompt, /no autonomous timer, session, agent_end, or completion mutation pump/i); assert.match(guided.systemPrompt, /without an arbitrary timeout/);
const tuiCalls = []; const tuiContext = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "tui-session" }, ui: { setWidget(id, value) { tuiCalls.push({ id, value }); } } }; for (const handler of conductorPi.handlers.get("session_start") ?? []) { await handler({}, tuiContext); await handler({}, tuiContext); } assert.equal(tuiCalls.filter(({ value }) => typeof value === "function").length, 2, "unbound TUI sessions install the passive widget and repeated session starts replace it deterministically"); for (const handler of conductorPi.handlers.get("session_start") ?? []) await handler({}, { hasUI: false, mode: "rpc", cwd: "/tmp", sessionManager: { getSessionId: () => "headless-after-tui" }, ui: {} }); assert.equal(tuiCalls.at(-1).value, undefined, "a subsequent headless session start removes the prior TUI widget before returning"); for (const handler of conductorPi.handlers.get("session_shutdown") ?? []) await handler(); assert.equal(tuiCalls.at(-1).value, undefined, "session shutdown tears down the installed widget without conductor timer ceremony");

console.log("Canonical DAG plan and run-state schema tests OK; scheduler/store/worker-binding/integration probes and chained conductor ownership receipts pass");
