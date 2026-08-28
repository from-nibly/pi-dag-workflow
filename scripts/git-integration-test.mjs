import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { acquireGitIntegrationLockV1, canonicalHash, composeGitProposalV1, DurableGitIntegrationRuntimeV1, ensurePrivateGitRefV1, ExactGitIntegrationV1, GitIntegrationBlockedError, landOrReconcileBoundWorktreeV1, preflightBoundRepositoryV1, readRepositoryBindingIdentityV1 } from "../extensions/dag-workflow/dag-runtime/index.ts";

const execFileAsync = promisify(execFile);
const H = (char) => `sha256:${char.repeat(64)}`;
const AT = "2026-08-05T03:00:00.000Z";
const root = await mkdtemp(join(tmpdir(), "pi-dag-git-v1-"));

try {
  const happy = await fixture(root, "happy");
  const operationBinding = await preflightBoundRepositoryV1(happy.request);
  const operationPayload = { sourceBase: happy.request.sourceBase, candidate: happy.request.candidate, expectedPrefix: happy.request.expectedPrefix, compositionProfileHash: happy.request.compositionProfileHash, ownerEpoch: happy.request.ownerEpoch };
  const operationProposal = await composeGitProposalV1(happy.request, operationBinding, { effectId: "effect-operation-compose", requestHash: canonicalHash({ kind: "compose", payload: operationPayload }), ownerEpoch: happy.request.ownerEpoch });
  assert.equal(operationProposal.composed.tree, happy.candidate.tree, "operation-sized composition API binds exact explicit-base result");
  const aborted = new AbortController(); aborted.abort();
  const abortRef = "refs/pi-dag/v1/abort-probe";
  await assert.rejects(() => preflightBoundRepositoryV1(happy.request, aborted.signal), (error) => error?.name === "AbortError");
  await assert.rejects(() => acquireGitIntegrationLockV1(happy.request, operationBinding, { effectId: "abort-lock", requestHash: canonicalHash({ kind: "acquire_lock", payload: { transactionId: happy.request.transactionId, repositoryId: happy.request.repositoryId, commonDirIdentityHash: operationBinding.commonDirIdentityHash, ownerEpoch: happy.request.ownerEpoch } }), ownerEpoch: happy.request.ownerEpoch }, aborted.signal), (error) => error?.name === "AbortError");
  await assert.rejects(() => ensurePrivateGitRefV1(operationBinding, abortRef, happy.base.commit, { effectId: "abort-ref", requestHash: canonicalHash({ kind: "anchor_ref", payload: { commonDirIdentityHash: operationBinding.commonDirIdentityHash, refHash: canonicalHash(abortRef), oid: happy.base.commit } }), ownerEpoch: happy.request.ownerEpoch }, aborted.signal), (error) => error?.name === "AbortError");
  await assert.rejects(() => composeGitProposalV1(happy.request, operationBinding, { effectId: "abort-compose", requestHash: canonicalHash({ kind: "compose", payload: operationPayload }), ownerEpoch: happy.request.ownerEpoch }, aborted.signal), (error) => error?.name === "AbortError");
  await assert.rejects(() => landOrReconcileBoundWorktreeV1(operationBinding, happy.request.targetRef, happy.request.expectedPrefix, operationProposal.composed, { effectId: "abort-land", requestHash: canonicalHash({ kind: "land", payload: { commonDirIdentityHash: operationBinding.commonDirIdentityHash, targetRef: happy.request.targetRef, expectedOld: happy.request.expectedPrefix, intended: operationProposal.composed } }), ownerEpoch: happy.request.ownerEpoch }, aborted.signal), (error) => error?.name === "AbortError");
  await assert.rejects(() => gitRaw(happy.repo, ["show-ref", "--verify", "--hash", abortRef]), "aborted ref/effect boundary publishes no hidden ref");
  assert.equal(await git(happy.repo, ["rev-parse", "HEAD"]), happy.base.commit, "aborted lock/compose/land boundaries leave the target unchanged");
  const recorder = runtime();
  const transaction = new ExactGitIntegrationV1(recorder);
  const receipt = await transaction.execute(happy.request);
  assert.equal(await git(happy.repo, ["rev-parse", "HEAD"]), receipt.composed.commit);
  assert.equal(await git(happy.repo, ["rev-parse", "HEAD^{tree}"]), receipt.composed.tree);
  assert.equal((await git(happy.repo, ["show", "-s", "--format=%P", receipt.composed.commit])).split(/\s+/).length, 1);
  assert.equal((await git(happy.repo, ["show", "-s", "--format=%P", receipt.composed.commit])), happy.base.commit);
  assert.equal(receipt.landing.targetObservationHash, canonicalHash({ targetRef: happy.request.targetRef, observed: { commit: receipt.composed.commit, tree: receipt.composed.tree }, expectedOld: happy.request.expectedPrefix, intendedNew: receipt.composed }), "runtime receipt uses the exact target-observation identity joined by canonical landing facts");
  assert(recorder.intents.length > 8 && recorder.observations.length > 8, "every Git operation is preceded by an intent and followed by observation");
  for (const ref of Object.values(receipt.privateRefs)) assert.equal(await git(happy.repo, ["show-ref", "--verify", "--hash", ref]), ref.includes("baseline") ? happy.base.commit : ref.includes("candidates") ? happy.candidate.commit : ref.includes("prefix") ? happy.base.commit : receipt.composed.commit);
  const replayReceipt = await new ExactGitIntegrationV1(runtime()).execute(happy.request);
  assert.equal(replayReceipt.receiptHash, receipt.receiptHash, "exact replay produces the same synthetic commit and immutable receipt");
  const takeoverReceipt = await new ExactGitIntegrationV1(runtime()).execute({ ...happy.request, ownerEpoch: 2 });
  assert.notEqual(takeoverReceipt.receiptHash, receipt.receiptHash, "integration receipt binds the exact accepting owner epoch");
  await gitRaw(happy.repo, ["update-ref", "-d", receipt.privateRefs.baseline]); await gitRaw(happy.repo, ["update-ref", "refs/heads/attacker-anchor", happy.base.commit]); await gitRaw(happy.repo, ["symbolic-ref", receipt.privateRefs.baseline, "refs/heads/attacker-anchor"]);
  await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(happy.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "PRIVATE_REF_SYMBOLIC", "symbolic aliases cannot satisfy immutable private-ref authority");

  const conflict = await conflictFixture(root, "conflict"); const conflictRuntime = runtime();
  await assert.rejects(() => new ExactGitIntegrationV1(conflictRuntime).execute(conflict.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "COMPOSITION_CONFLICT");
  assert.equal(await git(conflict.repo, ["rev-parse", "HEAD"]), conflict.prefix.commit, "composition conflict never moves the target"); assert(conflictRuntime.observations.some(({ disposition }) => disposition === "conflict"), "composition conflicts durably reconcile their persisted effect intent");

  const drift = await fixture(root, "drift");
  await writeFile(join(drift.repo, "third.txt"), "third\n");
  await gitRaw(drift.repo, ["add", "third.txt"]); await gitRaw(drift.repo, ["commit", "-m", "chore: concurrent target movement"]);
  const third = await gitRef(drift.repo, "HEAD");
  await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(drift.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "TARGET_DRIFT");
  assert.equal(await git(drift.repo, ["rev-parse", "HEAD"]), third.commit, "third target identity is never overwritten");

  const duplicate = await fixture(root, "duplicate-checkout");
  const duplicatePath = join(root, "duplicate-main-worktree");
  await gitRaw(duplicate.repo, ["worktree", "add", "--force", duplicatePath, "main"]);
  const duplicateRuntime = runtime(); await assert.rejects(() => new ExactGitIntegrationV1(duplicateRuntime).execute(duplicate.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "TARGET_MULTIPLE_CHECKOUT");
  assert.equal(await git(duplicate.repo, ["rev-parse", "HEAD"]), duplicate.base.commit); assert(duplicateRuntime.observations.some(({ disposition }) => disposition === "blocked"), "landing safety failures reconcile their exact intent as blocked");

  const unsupported = await fixture(root, "unsupported-config");
  await gitRaw(unsupported.repo, ["config", "merge.custom.driver", "false"]);
  await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(unsupported.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "UNSUPPORTED_GIT_CONFIG");
  const unsupportedTree = await fixture(root, "unsupported-tree"); const unsupportedAuthor = join(root, "unsupported-tree-author-2"); await gitRaw(unsupportedTree.repo, ["worktree", "add", "-b", "unsupported-tree-candidate-2", unsupportedAuthor, unsupportedTree.base.commit]); await writeFile(join(unsupportedAuthor, ".gitattributes"), "*.bin filter=evil\n"); await gitRaw(unsupportedAuthor, ["add", ".gitattributes"]); await gitRaw(unsupportedAuthor, ["commit", "-m", "feat: add unsupported attributes"]); unsupportedTree.request.candidate = { repositoryId: "repo-main", ...(await gitRef(unsupportedAuthor, "HEAD")) }; await gitRaw(unsupportedTree.repo, ["worktree", "remove", unsupportedAuthor]);
  await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(unsupportedTree.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "UNSUPPORTED_ATTRIBUTES");
  const gitlinkTree = await fixture(root, "gitlink-tree"); const gitlinkAuthor = join(root, "gitlink-tree-author-2"); await gitRaw(gitlinkTree.repo, ["worktree", "add", "-b", "gitlink-tree-candidate-2", gitlinkAuthor, gitlinkTree.base.commit]); await gitRaw(gitlinkAuthor, ["update-index", "--add", "--cacheinfo", `160000,${gitlinkTree.base.commit},vendor/module`]); await gitRaw(gitlinkAuthor, ["commit", "-m", "feat: add raw gitlink"]); gitlinkTree.request.candidate = { repositoryId: "repo-main", ...(await gitRef(gitlinkAuthor, "HEAD")) }; await gitRaw(gitlinkTree.repo, ["worktree", "remove", "--force", gitlinkAuthor]); await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(gitlinkTree.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "UNSUPPORTED_GITLINK");
  const bindingMismatch = await fixture(root, "binding-mismatch"); await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute({ ...bindingMismatch.request, expectedRepositoryBinding: { ...bindingMismatch.request.expectedRepositoryBinding, commonDirIdentityHash: H("f") } }), (error) => error instanceof GitIntegrationBlockedError && error.code === "REPOSITORY_AUTHORITY_MISMATCH");

  const dirtyReplay = await fixture(root, "dirty-worktree-replay"); const dirtyRequestPath = join(root, "request-dirty-worktree-replay.json"); await writeFile(dirtyRequestPath, JSON.stringify(dirtyReplay.request)); await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/git-integration-crash-child.mjs", dirtyRequestPath, "after_worktree"], { cwd: process.cwd() }), (error) => error?.code === 86); const [dirtyWorkspaceName] = await readdir(join(dirtyReplay.request.controlRoot, "worktrees")); await writeFile(join(dirtyReplay.request.controlRoot, "worktrees", dirtyWorkspaceName, "poison"), "untrusted\n"); let dirtyVerifierCalled = false; await assert.rejects(() => new ExactGitIntegrationV1(runtime(undefined, async () => { dirtyVerifierCalled = true; return evidence(); })).execute(dirtyReplay.request), (error) => error instanceof GitIntegrationBlockedError && error.code === "VERIFICATION_WORKTREE_DIRTY"); assert.equal(dirtyVerifierCalled, false, "dirty replay workspace is fenced before verification dispatch");

  const failpoints = [
    "after_lock_intent", "after_lock_acquired", "after_preflight", "after_baseline_anchor", "after_candidate_anchor",
    "after_prefix_anchor", "after_composition", "after_commit_tree", "after_composed_anchor", "after_worktree",
    "after_verification", "after_proposal_anchor", "after_landing_intent", "after_fast_forward",
    "after_landing_observation", "after_receipt",
  ];
  const crashFailpoints = [...failpoints, "before_cleanup", "after_cleanup_side_effect", "after_cleanup", "after_lock_release_side_effect"];
  for (const point of failpoints) {
    const subject = await fixture(root, `failpoint-${point}`);
    let fired = false;
    const failing = runtime(async (observed) => { if (observed === point && !fired) { fired = true; throw new Error(`failpoint:${point}`); } });
    await assert.rejects(() => new ExactGitIntegrationV1(failing).execute(subject.request), new RegExp(`failpoint:${point}`));
    assert(fired, `${point} was reached`);
    const recovered = await new ExactGitIntegrationV1(runtime()).execute(subject.request);
    assert.equal(await git(subject.repo, ["rev-parse", "HEAD"]), recovered.composed.commit, `${point} recovers to exact applied target`);
    assert.equal((await readFile(join(subject.request.artifactRoot, `${recovered.receiptHash.slice(7)}.integration-receipt.json`), "utf8")).trim().length > 0, true);
  }

  for (const point of crashFailpoints) {
    const subject = await fixture(root, `process-crash-${point}`); const requestPath = join(root, `request-${point}.json`); const journalRoot = join(root, `journal-${point}`); await writeFile(requestPath, JSON.stringify(subject.request));
    await assert.rejects(() => execFileAsync(process.execPath, ["scripts/fixtures/git-integration-crash-child.mjs", requestPath, point, journalRoot], { cwd: process.cwd() }), (error) => error?.code === 86);
    const recovered = await new ExactGitIntegrationV1(new DurableGitIntegrationRuntimeV1(journalRoot, runtime())).execute(subject.request);
    assert.equal(await git(subject.repo, ["rev-parse", "HEAD"]), recovered.composed.commit, `real process death at ${point} reconciles exact old/new state and stale lock`);
    const intentFiles = await readdir(join(journalRoot, "intents")); const observationFiles = await readdir(join(journalRoot, "observations")); const observedEffectIds = new Set(await Promise.all(observationFiles.map(async (name) => JSON.parse(await readFile(join(journalRoot, "observations", name), "utf8")).effectId))); for (const name of intentFiles) { const intent = JSON.parse(await readFile(join(journalRoot, "intents", name), "utf8")); assert(observedEffectIds.has(intent.effectId), `durable intent ${intent.effectId} is terminally observed after ${point} recovery`); }
  }

  const cleanupSubject = await fixture(root, "cleanup-quarantine"); const cleanupRuntime = runtime(async (point) => { if (point === "after_receipt") { const [workspaceName] = await readdir(join(cleanupSubject.request.controlRoot, "worktrees")); await writeFile(join(cleanupSubject.request.controlRoot, "worktrees", workspaceName, "post-receipt-poison"), "dirty\n"); } }); const cleanupReceipt = await new ExactGitIntegrationV1(cleanupRuntime).execute(cleanupSubject.request); assert.equal(await git(cleanupSubject.repo, ["rev-parse", "HEAD"]), cleanupReceipt.composed.commit); assert((await readdir(cleanupSubject.request.artifactRoot)).some((name) => name.endsWith(".workspace-quarantine.json")), "dirty post-receipt workspace is durably quarantined without revoking accepted landing");

  const lockSubject = await fixture(root, "concurrent-lock");
  let releaseVerification;
  let verificationStarted;
  const verificationGate = new Promise((resolve) => { releaseVerification = resolve; });
  const verificationReady = new Promise((resolve) => { verificationStarted = resolve; });
  const blockedRuntime = runtime(undefined, async () => { verificationStarted(); await verificationGate; return evidence(); });
  const first = new ExactGitIntegrationV1(blockedRuntime).execute(lockSubject.request);
  await verificationReady;
  const competingControlRoot = join(root, "competing-control-root"); const competingArtifactRoot = join(root, "competing-artifacts"); await mkdir(competingControlRoot, { recursive: true }); await mkdir(competingArtifactRoot, { recursive: true });
  const competingRequest = { ...lockSubject.request, transactionId: "transaction-competitor", controlRoot: competingControlRoot, artifactRoot: competingArtifactRoot };
  await assert.rejects(() => new ExactGitIntegrationV1(runtime()).execute(competingRequest), (error) => error instanceof GitIntegrationBlockedError && error.code === "INTEGRATION_LOCKED");
  releaseVerification();
  await first;

  console.log("Exact real-Git integration transaction and failpoint matrix OK");
} finally {
  await rm(root, { recursive: true, force: true });
}

function runtime(onFailpoint, verify = async () => evidence()) {
  const intentBytes = new Map();
  const observationBytes = new Map();
  return {
    intents: [], observations: [],
    async recordIntent(intent) {
      const bytes = JSON.stringify(intent); const prior = intentBytes.get(intent.effectId);
      if (prior && prior !== bytes) throw new Error(`conflicting intent ${intent.effectId}`);
      intentBytes.set(intent.effectId, bytes); this.intents.push(intent);
    },
    async recordObservation(observation) {
      const bytes = JSON.stringify(observation); const key = `${observation.effectId}:${observation.disposition}`; const prior = observationBytes.get(key);
      if (prior && prior !== bytes) throw new Error(`conflicting observation ${key}`);
      observationBytes.set(key, bytes); this.observations.push(observation);
    },
    async assertAuthority() {},
    async acceptReceipt() {},
    verify,
    async failpoint(point) { await onFailpoint?.(point); },
    now: () => AT,
  };
}

function evidence() { return { prefixEvidenceHashes: [H("a")], finalEvidenceHashes: [H("b")], environmentClosureHash: H("c") }; }

async function fixture(parent, name) {
  const repo = join(parent, name); await mkdir(repo, { recursive: true });
  await gitRaw(repo, ["init", "-b", "main"]); await gitRaw(repo, ["config", "user.name", "Test User"]); await gitRaw(repo, ["config", "user.email", "test@example.invalid"]);
  await writeFile(join(repo, "base.txt"), "base\n"); await gitRaw(repo, ["add", "base.txt"]); await gitRaw(repo, ["commit", "-m", "chore: establish baseline"]);
  const base = await gitRef(repo, "HEAD");
  const author = join(parent, `${name}-author`); await gitRaw(repo, ["worktree", "add", "-b", `${name}-candidate`, author, base.commit]);
  await writeFile(join(author, "candidate.txt"), `${name}\n`); await gitRaw(author, ["add", "candidate.txt"]); await gitRaw(author, ["commit", "-m", "feat: add candidate"]);
  const candidate = await gitRef(author, "HEAD"); await gitRaw(repo, ["worktree", "remove", author]);
  const request = await requestFor(parent, name, repo, base, candidate, base);
  return { repo, base, candidate, request };
}

async function conflictFixture(parent, name) {
  const baseFixture = await fixture(parent, name);
  await writeFile(join(baseFixture.repo, "base.txt"), "prefix\n"); await gitRaw(baseFixture.repo, ["add", "base.txt"]); await gitRaw(baseFixture.repo, ["commit", "-m", "feat: change prefix"]);
  const prefix = await gitRef(baseFixture.repo, "HEAD");
  const author = join(parent, `${name}-conflict-author`); await gitRaw(baseFixture.repo, ["worktree", "add", "-b", `${name}-conflict-candidate`, author, baseFixture.base.commit]);
  await writeFile(join(author, "base.txt"), "candidate\n"); await gitRaw(author, ["add", "base.txt"]); await gitRaw(author, ["commit", "-m", "feat: conflicting candidate"]);
  const candidate = await gitRef(author, "HEAD"); await gitRaw(baseFixture.repo, ["worktree", "remove", author]);
  return { repo: baseFixture.repo, base: baseFixture.base, candidate, prefix, request: await requestFor(parent, `${name}-conflict`, baseFixture.repo, baseFixture.base, candidate, prefix) };
}

async function requestFor(parent, name, repo, sourceBase, candidate, expectedPrefix) {
  const expectedRepositoryBinding = await readRepositoryBindingIdentityV1(repo);
  return {
    schemaVersion: 1, transactionId: `transaction-${name}`, runId: `run-${name}`, runNonce: `run-nonce-${name}-0123456789`, planHash: H("1"), authorizationSetHash: H("2"),
    repositoryId: "repo-main", repositoryRoot: repo, controlRoot: join(parent, `${name}-control`), artifactRoot: join(parent, `${name}-artifacts`),
    targetRef: "refs/heads/main", sourceBase: { repositoryId: "repo-main", ...sourceBase }, candidate: { repositoryId: "repo-main", ...candidate },
    expectedPrefix: { repositoryId: "repo-main", ...expectedPrefix }, workItemId: "item-main", candidateGeneration: 1, planCreatedAt: AT,
    commitSubject: "feat(dag): integrate candidate", compositionProfileHash: H("3"), prefixValidationProfileHash: H("4"), finalValidationProfileHash: H("5"), ownerEpoch: 1, expectedRepositoryBinding,
  };
}

async function gitRef(repo, ref) { return { commit: await git(repo, ["rev-parse", ref]), tree: await git(repo, ["rev-parse", `${ref}^{tree}`]) }; }
async function git(repo, args) { return (await gitRaw(repo, args)).trim(); }
async function gitRaw(repo, args) { const result = await execFileAsync("git", args, { cwd: repo, encoding: "utf8", maxBuffer: 4 * 1024 * 1024 }); return result.stdout; }
