import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { link, mkdir, open, readFile, readdir, realpath, rename, rm, stat } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { canonicalHash, canonicalStringify, type GitTreeRefV1 } from "./common.ts";

const execFileAsync = promisify(execFile);
const HASH_RE = /^sha256:[0-9a-f]{64}$/;
const OID_RE = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;
const TARGET_RE = /^refs\/heads\/[A-Za-z0-9][A-Za-z0-9._/-]{0,240}$/;
const ZERO_OID: Record<string, string> = { sha1: "0".repeat(40), sha256: "0".repeat(64) };
const MAX_GIT_OUTPUT = 4 * 1024 * 1024;
const FALLBACK_PROCESS_START_IDENTITY = `process-start:${process.pid}:${Math.round(Date.now() - process.uptime() * 1000)}`;

export type GitIntegrationFailpointV1 =
  | "after_lock_intent" | "after_lock_acquired" | "after_preflight" | "after_baseline_anchor"
  | "after_candidate_anchor" | "after_prefix_anchor" | "after_composition" | "after_commit_tree"
  | "after_composed_anchor" | "after_worktree" | "after_verification" | "after_proposal_anchor"
  | "after_landing_intent" | "after_fast_forward" | "after_landing_observation" | "after_receipt"
  | "before_cleanup" | "after_cleanup_side_effect" | "after_cleanup" | "after_lock_release_side_effect";

export interface GitIntegrationIntentV1 {
  schemaVersion: 1;
  kind: "GitIntegrationIntentV1";
  effectId: string;
  operation: "acquire_lock" | "release_lock" | "anchor_ref" | "compose" | "materialize_worktree" | "verify" | "land" | "publish_receipt" | "cleanup";
  transactionId: string;
  runId: string;
  runNonce: string;
  planHash: string;
  authorizationSetHash: string;
  ownerEpoch: number;
  repositoryId: string;
  repositoryBindingHash: string;
  requestHash: string;
  createdAt: string;
  payload: Record<string, unknown>;
}

export interface GitIntegrationObservationV1 {
  schemaVersion: 1;
  kind: "GitIntegrationObservationV1";
  effectId: string;
  transactionId: string;
  runId: string;
  runNonce: string;
  planHash: string;
  authorizationSetHash: string;
  ownerEpoch: number;
  repositoryId: string;
  repositoryBindingHash: string;
  disposition: "applied_exact" | "proven_absent" | "blocked" | "conflict" | "quarantined";
  observedAt: string;
  observationHash: string;
  payload: Record<string, unknown>;
}

export interface GitVerificationResultV1 {
  prefixEvidenceHashes: string[];
  finalEvidenceHashes: string[];
  environmentClosureHash: string;
}

export interface GitIntegrationRequestV1 {
  schemaVersion: 1;
  transactionId: string;
  runId: string;
  runNonce: string;
  planHash: string;
  authorizationSetHash: string;
  repositoryId: string;
  repositoryRoot: string;
  controlRoot: string;
  artifactRoot: string;
  targetRef: string;
  sourceBase: GitTreeRefV1;
  candidate: GitTreeRefV1;
  expectedPrefix: GitTreeRefV1;
  workItemId: string;
  candidateGeneration: number;
  planCreatedAt: string;
  commitSubject: string;
  compositionProfileHash: string;
  prefixValidationProfileHash: string;
  finalValidationProfileHash: string;
  ownerEpoch: number;
  expectedRepositoryBinding: { commonDirIdentityHash: string; worktreeIdentityHash: string; objectFormat: "sha1" | "sha256"; gitVersion: string; configHash: string };
}

export interface GitIntegrationReceiptV1 {
  schemaVersion: 1;
  kind: "IntegrationReceiptV1";
  transactionId: string;
  runId: string;
  runNonce: string;
  planHash: string;
  authorizationSetHash: string;
  ownerEpoch: number;
  repositoryId: string;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  gitVersion: string;
  configHash: string;
  objectFormat: "sha1" | "sha256";
  targetRef: string;
  sourceBase: GitTreeRefV1;
  candidate: GitTreeRefV1;
  expectedPrefix: GitTreeRefV1;
  composed: GitTreeRefV1;
  workItemId: string;
  candidateGeneration: number;
  compositionProfileHash: string;
  prefixValidationProfileHash: string;
  finalValidationProfileHash: string;
  prefixEvidenceHashes: string[];
  finalEvidenceHashes: string[];
  environmentClosureHash: string;
  privateRefs: Record<string, string>;
  landing: {
    expectedOldOid: string;
    newOid: string;
    reconciliation: "applied_exact";
    targetObservationHash: string;
  };
  sealedAt: string;
  receiptHash: string;
}

export interface GitIntegrationRuntimeV1 {
  recordIntent(intent: GitIntegrationIntentV1): Promise<void>;
  recordObservation(observation: GitIntegrationObservationV1): Promise<void>;
  assertAuthority(request: GitIntegrationRequestV1): Promise<void>;
  acceptReceipt(receipt: GitIntegrationReceiptV1): Promise<void>;
  verify(input: { workspace: string; commit: string; tree: string; request: GitIntegrationRequestV1 }): Promise<GitVerificationResultV1>;
  failpoint?(point: GitIntegrationFailpointV1, context: Record<string, unknown>): Promise<void> | void;
  reconcilePending?(request: GitIntegrationRequestV1, recovery?: { releasedLockIdentityHashes?: string[]; commonDirLockAbsent?: boolean }): Promise<void>;
  now?(): string;
}

export type GitIntegrationRuntimeDelegateV1 = Pick<GitIntegrationRuntimeV1, "assertAuthority" | "acceptReceipt" | "verify" | "failpoint" | "now">;

export class DurableGitIntegrationRuntimeV1 implements GitIntegrationRuntimeV1 {
  readonly journalRoot: string; readonly delegate: GitIntegrationRuntimeDelegateV1;
  constructor(journalRoot: string, delegate: GitIntegrationRuntimeDelegateV1) { this.journalRoot = resolve(journalRoot); this.delegate = delegate; }
  async recordIntent(intent: GitIntegrationIntentV1): Promise<void> { await publishImmutableJson(join(this.journalRoot, "intents", `${digestHex(intent.effectId)}.json`), intent); }
  async recordObservation(observation: GitIntegrationObservationV1): Promise<void> { await publishImmutableJson(join(this.journalRoot, "observations", `${observation.observationHash.slice(7)}.json`), observation); }
  async reconcilePending(request: GitIntegrationRequestV1, recovery: { releasedLockIdentityHashes?: string[]; commonDirLockAbsent?: boolean } = {}): Promise<void> {
    const intentDir = join(this.journalRoot, "intents"); const observationDir = join(this.journalRoot, "observations");
    const intentNames = await readdir(intentDir).catch((error: any) => error?.code === "ENOENT" ? [] : Promise.reject(error)); const observationNames = await readdir(observationDir).catch((error: any) => error?.code === "ENOENT" ? [] : Promise.reject(error));
    const observed = new Set<string>(); for (const name of observationNames) { const value = await readJson(join(observationDir, name)) as any; if (value?.effectId) observed.add(value.effectId); }
    for (const name of intentNames) { const intent = await readJson(join(intentDir, name)) as GitIntegrationIntentV1; if (observed.has(intent.effectId) || intent.transactionId !== request.transactionId || intent.runNonce !== request.runNonce) continue; let payload: Record<string, unknown> | null = null;
      if (intent.operation === "cleanup" && intent.payload.disposition !== "quarantine" && typeof intent.payload.workspaceKey === "string" && !(await exists(join(request.controlRoot, "worktrees", intent.payload.workspaceKey)))) payload = { absent: true, workspaceIdentityHash: intent.payload.workspaceIdentityHash, recovered: true };
      if (intent.operation === "release_lock" && ((recovery.releasedLockIdentityHashes ?? []).includes(String(intent.payload.lockIdentityHash)) || (recovery.commonDirLockAbsent && intent.payload.commonDirIdentityHash === request.expectedRepositoryBinding.commonDirIdentityHash))) payload = { released: true, lockIdentityHash: intent.payload.lockIdentityHash, recoveredByTakeover: !recovery.commonDirLockAbsent };
      if (payload) { const core = { schemaVersion: 1 as const, kind: "GitIntegrationObservationV1" as const, effectId: intent.effectId, transactionId: intent.transactionId, runId: intent.runId, runNonce: intent.runNonce, planHash: intent.planHash, authorizationSetHash: intent.authorizationSetHash, ownerEpoch: intent.ownerEpoch, repositoryId: intent.repositoryId, repositoryBindingHash: intent.repositoryBindingHash, disposition: "applied_exact" as const, observedAt: this.now(), payload }; await this.recordObservation({ ...core, observationHash: canonicalHash(core) }); }
    }
  }
  async assertAuthority(request: GitIntegrationRequestV1): Promise<void> { await this.delegate.assertAuthority(request); }
  async acceptReceipt(receipt: GitIntegrationReceiptV1): Promise<void> { await this.delegate.acceptReceipt(receipt); }
  async verify(input: { workspace: string; commit: string; tree: string; request: GitIntegrationRequestV1 }): Promise<GitVerificationResultV1> { return this.delegate.verify(input); }
  async failpoint(point: GitIntegrationFailpointV1, context: Record<string, unknown>): Promise<void> { await this.delegate.failpoint?.(point, context); }
  now(): string { return this.delegate.now?.() ?? new Date().toISOString(); }
}

export class GitIntegrationBlockedError extends Error {
  readonly code: string;
  readonly details: Record<string, unknown>;
  constructor(code: string, message: string, details: Record<string, unknown> = {}) {
    super(message); this.name = "GitIntegrationBlockedError"; this.code = code; this.details = details;
  }
}

export interface RepositoryBindingV1 {
  repositoryRoot: string;
  commonDir: string;
  gitDir: string;
  commonDirIdentityHash: string;
  worktreeIdentityHash: string;
  objectFormat: "sha1" | "sha256";
  gitVersion: string;
  configHash: string;
}

export interface GitOperationGuardV1 { effectId: string; requestHash: string; ownerEpoch: number; }
export interface GitIntegrationLockHandleV1 { binding: RepositoryBindingV1; identityHash: string; recoveredStaleIdentityHashes: string[]; release(): Promise<void>; }

export async function readRepositoryBindingIdentityV1(repositoryRoot: string): Promise<GitIntegrationRequestV1["expectedRepositoryBinding"]> {
  const root = await realpath(repositoryRoot); const commonDir = await realpath(resolve(root, (await git(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).trim())); const gitDir = await realpath(resolve(root, (await git(root, ["rev-parse", "--path-format=absolute", "--git-dir"])).trim()));
  const commonStat = await stat(commonDir); const gitStat = await stat(gitDir); const objectFormat = (await git(root, ["rev-parse", "--show-object-format"])).trim() as "sha1" | "sha256"; const gitVersion = (await git(root, ["--version"])).trim(); const configText = await git(root, ["config", "--local", "--null", "--list"], { allowExit: [0, 1] });
  return { commonDirIdentityHash: canonicalHash({ pathHash: canonicalHash(commonDir), dev: String(commonStat.dev), ino: String(commonStat.ino), objectFormat }), worktreeIdentityHash: canonicalHash({ pathHash: canonicalHash(root), gitDirHash: canonicalHash(gitDir), dev: String(gitStat.dev), ino: String(gitStat.ino) }), objectFormat, gitVersion, configHash: canonicalHash(configText) };
}
export async function preflightBoundRepositoryV1(request: GitIntegrationRequestV1): Promise<RepositoryBindingV1> { assertRequest(request); return preflightRepository(request); }
export async function acquireGitIntegrationLockV1(request: GitIntegrationRequestV1, binding: RepositoryBindingV1, guard: GitOperationGuardV1): Promise<GitIntegrationLockHandleV1> {
  assertOperationGuard(guard, "acquire_lock", { transactionId: request.transactionId, repositoryId: request.repositoryId, commonDirIdentityHash: binding.commonDirIdentityHash, ownerEpoch: request.ownerEpoch });
  const key = digestHex({ commonDirIdentityHash: binding.commonDirIdentityHash });
  const lock = await IntegrationDirectoryLockV1.acquire(join(binding.commonDir, "pi-dag-v1", "integration-locks", `${key}.lock`), request.transactionId, request.planCreatedAt);
  return { binding, identityHash: lock.identityHash, recoveredStaleIdentityHashes: lock.recoveredStaleIdentityHashes, release: () => lock.release() };
}
export async function ensurePrivateGitRefV1(binding: RepositoryBindingV1, ref: string, oid: string, guard: GitOperationGuardV1): Promise<"created" | "exact"> {
  assertOperationGuard(guard, "anchor_ref", { commonDirIdentityHash: binding.commonDirIdentityHash, refHash: canonicalHash(ref), oid });
  return createImmutableRef(binding, ref, oid);
}
export async function composeGitProposalV1(request: GitIntegrationRequestV1, binding: RepositoryBindingV1, guard: GitOperationGuardV1): Promise<{ composed: GitTreeRefV1; messageHash: string }> {
  const payload = { sourceBase: request.sourceBase, candidate: request.candidate, expectedPrefix: request.expectedPrefix, compositionProfileHash: request.compositionProfileHash, ownerEpoch: request.ownerEpoch };
  assertOperationGuard(guard, "compose", payload);
  const tree = await mergeTree(binding, request.sourceBase.commit, request.expectedPrefix.commit, request.candidate.commit); const message = integrationCommitMessage(request);
  const commit = await commitTree(binding, tree, request.expectedPrefix.commit, message, request.planCreatedAt); await assertCommit(binding, commit, tree, request.expectedPrefix.commit);
  return { composed: { repositoryId: request.repositoryId, commit, tree }, messageHash: canonicalHash(message) };
}
export async function landOrReconcileBoundWorktreeV1(binding: RepositoryBindingV1, targetRef: string, expectedOld: GitTreeRefV1, intended: GitTreeRefV1, guard: GitOperationGuardV1): Promise<"applied_exact" | "proven_absent"> {
  assertOperationGuard(guard, "land", { commonDirIdentityHash: binding.commonDirIdentityHash, targetRef, expectedOld, intended });
  const before = observeTarget(binding, targetRef); const disposition = classifyTarget(await before, expectedOld, intended);
  if (disposition === "third") throw new GitIntegrationBlockedError("TARGET_DRIFT", "Target is neither exact old nor exact intended new");
  if (disposition === "old") { await assertBoundSessionFastForwardSafe(binding, targetRef, expectedOld.commit); await ordinaryFastForward(binding, intended.commit); }
  const after = await observeTarget(binding, targetRef); const reconciled = classifyTarget(after, expectedOld, intended);
  if (reconciled === "new") { await assertBoundSessionLandedExact(binding, targetRef, intended.commit, intended.tree); return "applied_exact"; }
  if (reconciled === "old") return "proven_absent";
  throw new GitIntegrationBlockedError("LANDING_AMBIGUOUS", "Landing reconciled to a third target identity");
}

export class ExactGitIntegrationV1 {
  readonly runtime: GitIntegrationRuntimeV1;
  constructor(runtime: GitIntegrationRuntimeV1) { this.runtime = runtime; }

  async execute(input: GitIntegrationRequestV1): Promise<GitIntegrationReceiptV1> {
    assertRequest(input);
    const request = structuredClone(input);
    const now = () => request.planCreatedAt;
    await this.runtime.assertAuthority(request);
    const initialBinding = await preflightRepository(request);
    const refBase = privateRefBase(request);
    const refs = {
      baseline: `${refBase}/baseline`,
      candidate: `${refBase}/candidates/${digestHex(request.workItemId)}/g${request.candidateGeneration}`,
      prefix: `${refBase}/transactions/${digestHex(request.transactionId)}/prefix`,
      composed: `${refBase}/transactions/${digestHex(request.transactionId)}/composed`,
      proposal: `${refBase}/transactions/${digestHex(request.transactionId)}/proposal`,
    };
    const initialTarget = await observeTarget(initialBinding, request.targetRef);
    if (initialTarget.commit !== request.expectedPrefix.commit || initialTarget.tree !== request.expectedPrefix.tree) {
      const proposalOid = (await git(initialBinding.repositoryRoot, ["show-ref", "--verify", "--hash", refs.proposal], { allowExit: [0, 1, 2, 128] })).trim();
      if (!proposalOid || proposalOid !== initialTarget.commit) throw new GitIntegrationBlockedError("TARGET_DRIFT", "Target differs from expected prefix without an exact protected transaction proposal", { observed: initialTarget });
      const proposalTree = (await git(initialBinding.repositoryRoot, ["rev-parse", `${proposalOid}^{tree}`])).trim();
      const proposalParents = (await git(initialBinding.repositoryRoot, ["show", "-s", "--format=%P", proposalOid])).trim().split(/\s+/).filter(Boolean);
      if (proposalTree !== initialTarget.tree || proposalParents.length !== 1 || proposalParents[0] !== request.expectedPrefix.commit) throw new GitIntegrationBlockedError("TARGET_DRIFT", "Protected replay proposal does not prove the exact expected-prefix landing", { observed: initialTarget });
    }
    const lockKey = digestHex({ commonDirIdentityHash: initialBinding.commonDirIdentityHash }); const lockPath = join(initialBinding.commonDir, "pi-dag-v1", "integration-locks", `${lockKey}.lock`);
    await this.runtime.reconcilePending?.(request, { commonDirLockAbsent: !(await exists(lockPath)) });
    const lockIntent = await this.#intent(request, "acquire_lock", { lockKey, commonDirIdentityHash: initialBinding.commonDirIdentityHash }, now());
    await this.#hit("after_lock_intent", { transactionId: request.transactionId });
    let lock: IntegrationDirectoryLockV1;
    try { lock = await IntegrationDirectoryLockV1.acquire(lockPath, request.transactionId, now()); }
    catch (error) { await this.#observation(lockIntent, "blocked", { error: boundedError(error) }, now()); throw error; }
    try {
      await this.#observation(lockIntent, "applied_exact", { lockIdentityHash: lock.identityHash, recoveredStaleIdentityHashes: lock.recoveredStaleIdentityHashes }, now());
      await this.runtime.reconcilePending?.(request, { releasedLockIdentityHashes: lock.recoveredStaleIdentityHashes });
      await this.#hit("after_lock_acquired", { transactionId: request.transactionId });
      await this.runtime.assertAuthority(request);
      const binding = await preflightRepository(request);
      if (canonicalHash(binding) !== canonicalHash(initialBinding)) throw new GitIntegrationBlockedError("REPOSITORY_BINDING_CHANGED", "Repository identity changed between read-only preflight and integration lock acquisition");
      await this.#hit("after_preflight", { binding });

      await this.runtime.assertAuthority(request);
      await this.#anchor(request, binding, refs.baseline, request.sourceBase.commit, "baseline", now());
      await this.#hit("after_baseline_anchor", { ref: refs.baseline });
      await this.#anchor(request, binding, refs.candidate, request.candidate.commit, "candidate", now());
      await this.#hit("after_candidate_anchor", { ref: refs.candidate });
      await this.#anchor(request, binding, refs.prefix, request.expectedPrefix.commit, "prefix", now());
      await this.#hit("after_prefix_anchor", { ref: refs.prefix });

      await this.runtime.assertAuthority(request);
      const composeIntent = await this.#intent(request, "compose", {
        sourceBase: request.sourceBase, candidate: request.candidate, expectedPrefix: request.expectedPrefix,
        compositionProfileHash: request.compositionProfileHash,
      }, now());
      let composedTreeOid: string;
      try { composedTreeOid = await mergeTree(binding, request.sourceBase.commit, request.expectedPrefix.commit, request.candidate.commit); await assertCoreOnlyTree(binding.repositoryRoot, composedTreeOid); }
      catch (error) { await this.#observation(composeIntent, "conflict", { error: boundedError(error) }, now()); throw error; }
      await this.#hit("after_composition", { composedTree: composedTreeOid });

      const message = integrationCommitMessage(request); let composedCommit: string;
      try { composedCommit = await commitTree(binding, composedTreeOid, request.expectedPrefix.commit, message, request.planCreatedAt); await assertCommit(binding, composedCommit, composedTreeOid, request.expectedPrefix.commit); }
      catch (error) { await this.#observation(composeIntent, "blocked", { composedTree: composedTreeOid, error: boundedError(error) }, now()); throw error; }
      const composed: GitTreeRefV1 = { repositoryId: request.repositoryId, commit: composedCommit, tree: composedTreeOid };
      await this.#observation(composeIntent, "applied_exact", { composed, syntheticParentCommit: request.expectedPrefix.commit }, now());
      await this.#hit("after_commit_tree", { composedCommit, composedTree: composedTreeOid });
      await this.#anchor(request, binding, refs.composed, composedCommit, "composed", now());
      await this.#hit("after_composed_anchor", { ref: refs.composed, composedCommit });

      const workspaceKey = digestHex({ transactionId: request.transactionId, composedCommit });
      const workspace = join(request.controlRoot, "worktrees", workspaceKey);
      await this.runtime.assertAuthority(request);
      const worktreeIntent = await this.#intent(request, "materialize_worktree", { workspaceKey, composedCommit }, now());
      let workspaceReceipt: Record<string, unknown>;
      try { workspaceReceipt = await materializeVerificationWorktree(binding, workspace, composedCommit); await assertCleanWorkspace(binding, workspace, composedCommit, composedTreeOid); }
      catch (error) { await this.#observation(worktreeIntent, "blocked", { error: boundedError(error) }, now()); await this.#quarantineWorkspace(request, binding, workspace, composedCommit, String((error as Error).message), now()); throw error; }
      await this.#observation(worktreeIntent, "applied_exact", workspaceReceipt, now());
      await this.#hit("after_worktree", { workspace, composedCommit });

      let verification: GitVerificationResultV1;
      await this.runtime.assertAuthority(request);
      const verifyIntent = await this.#intent(request, "verify", {
        composed, prefixValidationProfileHash: request.prefixValidationProfileHash,
        finalValidationProfileHash: request.finalValidationProfileHash,
      }, now());
      try {
        verification = normalizeVerification(await this.runtime.verify({ workspace, commit: composedCommit, tree: composedTreeOid, request }));
        await assertCleanWorkspace(binding, workspace, composedCommit, composedTreeOid);
        await this.#observation(verifyIntent, "applied_exact", verification as unknown as Record<string, unknown>, now());
      } catch (error) {
        await this.#observation(verifyIntent, "blocked", { error: boundedError(error) }, now());
        await this.#quarantineWorkspace(request, binding, workspace, composedCommit, String((error as Error).message), now());
        throw error;
      }
      await this.#hit("after_verification", { verification });
      await this.#anchor(request, binding, refs.proposal, composedCommit, "proposal", now());
      await this.#hit("after_proposal_anchor", { ref: refs.proposal, composedCommit });

      await this.runtime.assertAuthority(request);
      const landingIntent = await this.#intent(request, "land", {
        commonDirIdentityHash: binding.commonDirIdentityHash, targetRef: request.targetRef,
        expectedOldOid: request.expectedPrefix.commit, expectedOldTree: request.expectedPrefix.tree,
        newOid: composedCommit, newTree: composedTreeOid, proposalRef: refs.proposal,
      }, now());
      await this.#hit("after_landing_intent", { targetRef: request.targetRef, composedCommit });
      let preLanding: GitTreeRefV1;
      try { await assertNoGitOperationInProgress(binding.repositoryRoot); preLanding = await observeTarget(binding, request.targetRef); }
      catch (error) { await this.#observation(landingIntent, "blocked", { error: boundedError(error) }, now()); throw error; }
      let reconciliation: "old" | "new" | "third" = classifyTarget(preLanding, request.expectedPrefix, composed);
      if (reconciliation === "third") { await this.#observation(landingIntent, "conflict", { observed: preLanding, expectedOld: request.expectedPrefix, intendedNew: composed }, now()); throw new GitIntegrationBlockedError("TARGET_DRIFT", "Target moved to a third identity before landing", { observed: preLanding }); }
      if (reconciliation === "old") {
        try { await assertBoundSessionFastForwardSafe(binding, request.targetRef, request.expectedPrefix.commit); await ordinaryFastForward(binding, composedCommit); }
        catch (error) { await this.#observation(landingIntent, "blocked", { error: boundedError(error) }, now()); throw error; }
        await this.#hit("after_fast_forward", { targetRef: request.targetRef, composedCommit });
      }
      let landed: Awaited<ReturnType<typeof observeTarget>>;
      try { landed = await observeTarget(binding, request.targetRef); reconciliation = classifyTarget(landed, request.expectedPrefix, composed); if (reconciliation === "new") await assertBoundSessionLandedExact(binding, request.targetRef, composedCommit, composedTreeOid); }
      catch (error) { await this.#observation(landingIntent, "blocked", { error: boundedError(error), expectedOld: request.expectedPrefix, intendedNew: composed }, now()); throw error; }
      if (reconciliation !== "new") { await this.#observation(landingIntent, reconciliation === "third" ? "conflict" : "blocked", { observed: landed, expectedOld: request.expectedPrefix, intendedNew: composed }, now()); throw new GitIntegrationBlockedError("LANDING_AMBIGUOUS", "Landing did not reconcile to the exact proposed commit/tree", { observed: landed, reconciliation }); }
      const targetObservationCore = { targetRef: request.targetRef, observed: landed, expectedOld: request.expectedPrefix, intendedNew: composed };
      const targetObservationHash = canonicalHash(targetObservationCore);
      await this.#observation(landingIntent, "applied_exact", { ...targetObservationCore, targetObservationHash }, now());
      await this.#hit("after_landing_observation", { targetObservationHash });

      const receiptCore = {
        schemaVersion: 1 as const, kind: "IntegrationReceiptV1" as const,
        transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash,
        authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId,
        commonDirIdentityHash: binding.commonDirIdentityHash, worktreeIdentityHash: binding.worktreeIdentityHash, gitVersion: binding.gitVersion, configHash: binding.configHash, objectFormat: binding.objectFormat, targetRef: request.targetRef,
        sourceBase: request.sourceBase, candidate: request.candidate, expectedPrefix: request.expectedPrefix,
        composed, workItemId: request.workItemId, candidateGeneration: request.candidateGeneration,
        compositionProfileHash: request.compositionProfileHash,
        prefixValidationProfileHash: request.prefixValidationProfileHash,
        finalValidationProfileHash: request.finalValidationProfileHash,
        prefixEvidenceHashes: verification.prefixEvidenceHashes,
        finalEvidenceHashes: verification.finalEvidenceHashes,
        environmentClosureHash: verification.environmentClosureHash,
        privateRefs: refs,
        landing: { expectedOldOid: request.expectedPrefix.commit, newOid: composedCommit, reconciliation: "applied_exact" as const, targetObservationHash },
        sealedAt: now(),
      };
      const receipt: GitIntegrationReceiptV1 = { ...receiptCore, receiptHash: canonicalHash(receiptCore) };
      const receiptIntent = await this.#intent(request, "publish_receipt", { receiptHash: receipt.receiptHash }, receipt.sealedAt);
      try { await publishImmutableJson(join(request.artifactRoot, `${receipt.receiptHash.slice("sha256:".length)}.integration-receipt.json`), receipt); await this.#observation(receiptIntent, "applied_exact", { receiptHash: receipt.receiptHash }, now()); }
      catch (error) { await this.#observation(receiptIntent, "blocked", { receiptHash: receipt.receiptHash, error: boundedError(error) }, now()); throw error; }
      await this.runtime.acceptReceipt(receipt);
      await this.#hit("after_receipt", { receiptHash: receipt.receiptHash });
      try { await this.#hit("before_cleanup", { workspace, composedCommit }); await cleanupVerificationWorktree(request, binding, workspace, composedCommit, this.runtime, now()); await this.#hit("after_cleanup", { workspace, composedCommit }); }
      catch (error) { await this.#quarantineWorkspace(request, binding, workspace, composedCommit, `Cleanup failed after accepted landing: ${String((error as Error).message)}`, now()).catch(() => undefined); }
      return receipt;
    } finally {
      const releaseIntent = await this.#intent(request, "release_lock", { lockIdentityHash: lock.identityHash, commonDirIdentityHash: initialBinding.commonDirIdentityHash }, now());
      try { await lock.release(); await this.#hit("after_lock_release_side_effect", { lockIdentityHash: lock.identityHash }); await this.#observation(releaseIntent, "applied_exact", { released: true, lockIdentityHash: lock.identityHash }, now()); }
      catch (error) { await this.#observation(releaseIntent, "blocked", { error: boundedError(error), lockIdentityHash: lock.identityHash }, now()); throw error; }
    }
  }

  async #anchor(request: GitIntegrationRequestV1, binding: RepositoryBindingV1, ref: string, oid: string, role: string, at: string): Promise<void> {
    const intent = await this.#intent(request, "anchor_ref", { refHash: canonicalHash(ref), oid, role }, at);
    try { const disposition = await createImmutableRef(binding, ref, oid); await this.#observation(intent, "applied_exact", { refHash: canonicalHash(ref), oid, role, disposition }, at); }
    catch (error) { await this.#observation(intent, error instanceof GitIntegrationBlockedError && ["PRIVATE_REF_CONFLICT", "PRIVATE_REF_SYMBOLIC"].includes(error.code) ? "conflict" : "blocked", { refHash: canonicalHash(ref), oid, role, error: boundedError(error) }, at); throw error; }
  }

  async #intent(request: GitIntegrationRequestV1, operation: GitIntegrationIntentV1["operation"], payload: Record<string, unknown>, at: string): Promise<GitIntegrationIntentV1> {
    const requestHash = canonicalHash({ transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: canonicalHash(request.expectedRepositoryBinding), operation, payload });
    const intent: GitIntegrationIntentV1 = {
      schemaVersion: 1, kind: "GitIntegrationIntentV1", effectId: `git-${digestHex(requestHash).slice(0, 24)}`,
      operation, transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: canonicalHash(request.expectedRepositoryBinding), requestHash, createdAt: at, payload,
    };
    await this.runtime.recordIntent(intent);
    return intent;
  }

  async #observation(intent: GitIntegrationIntentV1, disposition: GitIntegrationObservationV1["disposition"], payload: Record<string, unknown>, at: string): Promise<void> {
    const core = { schemaVersion: 1 as const, kind: "GitIntegrationObservationV1" as const, effectId: intent.effectId, transactionId: intent.transactionId, runId: intent.runId, runNonce: intent.runNonce, planHash: intent.planHash, authorizationSetHash: intent.authorizationSetHash, ownerEpoch: intent.ownerEpoch, repositoryId: intent.repositoryId, repositoryBindingHash: intent.repositoryBindingHash, disposition, observedAt: at, payload };
    await this.runtime.recordObservation({ ...core, observationHash: canonicalHash(core) });
  }

  async #quarantineWorkspace(request: GitIntegrationRequestV1, binding: RepositoryBindingV1, workspace: string, expectedHead: string, reason: string, at: string): Promise<void> {
    const intent = await this.#intent(request, "cleanup", { workspaceIdentityHash: canonicalHash({ workspace: resolve(workspace), expectedHead }), disposition: "quarantine" }, at);
    const quarantineFile = join(request.artifactRoot, `${digestHex({ workspace, expectedHead })}.workspace-quarantine.json`);
    const record = { schemaVersion: 1, kind: "GitWorkspaceQuarantineV1", transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, commonDirIdentityHash: binding.commonDirIdentityHash, worktreeIdentityHash: binding.worktreeIdentityHash, workspaceIdentityHash: canonicalHash({ workspace: resolve(workspace), expectedHead }), expectedHead, reason, observedAt: at };
    try { await publishImmutableJson(quarantineFile, { ...record, quarantineHash: canonicalHash(record) }); await this.#observation(intent, "quarantined", { quarantineHash: canonicalHash(record), reason }, at); }
    catch (error) { await this.#observation(intent, "blocked", { quarantineHash: canonicalHash(record), reason, error: boundedError(error) }, at); throw error; }
  }

  async #hit(point: GitIntegrationFailpointV1, context: Record<string, unknown>): Promise<void> { await this.runtime.failpoint?.(point, context); }
}

async function preflightRepository(request: GitIntegrationRequestV1): Promise<RepositoryBindingV1> {
  const root = await realpath(request.repositoryRoot);
  const top = await git(root, ["rev-parse", "--show-toplevel"]);
  if (await realpath(top.trim()) !== root) throw new GitIntegrationBlockedError("WRONG_WORKTREE_ROOT", "Repository root must be the exact bound session worktree root");
  const commonDir = await realpath(resolve(root, (await git(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).trim()));
  const gitDir = await realpath(resolve(root, (await git(root, ["rev-parse", "--path-format=absolute", "--git-dir"])).trim()));
  const commonStat = await stat(commonDir); const gitStat = await stat(gitDir);
  const objectFormat = (await git(root, ["rev-parse", "--show-object-format"])).trim() as "sha1" | "sha256";
  if (!(objectFormat in ZERO_OID)) throw new GitIntegrationBlockedError("UNSUPPORTED_OBJECT_FORMAT", `Unsupported Git object format ${objectFormat}`);
  if ((await git(root, ["rev-parse", "--is-shallow-repository"])).trim() !== "false") throw new GitIntegrationBlockedError("UNSUPPORTED_SHALLOW_REPOSITORY", "Shallow repositories are unsupported");
  const configText = await git(root, ["config", "--local", "--null", "--list"], { allowExit: [0, 1] });
  const configLower = configText.toLowerCase();
  if (/(^|\0)(?:filter\.|merge\..*\.driver|extensions\.partialclone|extensions\.worktreeconfig|remote\..*\.promisor|submodule\.|core\.hookspath(?:\n|=)|core\.fsmonitor(?:\n|=))/.test(configLower)) throw new GitIntegrationBlockedError("UNSUPPORTED_GIT_CONFIG", "Custom filters, merge drivers, partial clones, submodules, hooks paths, and filesystem monitors are unsupported");
  if ((await git(root, ["for-each-ref", "--format=%(refname)", "refs/replace"])).trim()) throw new GitIntegrationBlockedError("UNSUPPORTED_REPLACE_REFS", "Replace refs are unsupported");
  const alternates = join(commonDir, "objects", "info", "alternates"); const httpAlternates = join(commonDir, "objects", "info", "http-alternates");
  if (await exists(alternates) || await exists(httpAlternates)) throw new GitIntegrationBlockedError("UNSUPPORTED_ALTERNATES", "Object alternates are unsupported");
  const grafts = join(commonDir, "info", "grafts");
  if (await exists(grafts) && (await stat(grafts)).size > 0) throw new GitIntegrationBlockedError("UNSUPPORTED_GRAFTS", "Legacy grafts are unsupported");
  await assertCoreOnlyTree(root, request.sourceBase.commit);
  await assertCoreOnlyTree(root, request.candidate.commit);
  await assertCoreOnlyTree(root, request.expectedPrefix.commit);
  if (!TARGET_RE.test(request.targetRef) || request.targetRef.includes("..") || request.targetRef.endsWith(".") || request.targetRef.includes("@{")) throw new GitIntegrationBlockedError("INVALID_TARGET_REF", "Target must be a direct fully qualified branch ref");
  for (const treeRef of [request.sourceBase, request.candidate, request.expectedPrefix]) {
    await assertObject(root, treeRef.commit, "commit"); await assertObject(root, treeRef.tree, "tree");
    const tree = (await git(root, ["rev-parse", `${treeRef.commit}^{tree}`])).trim();
    if (tree !== treeRef.tree) throw new GitIntegrationBlockedError("TREE_IDENTITY_MISMATCH", `Commit ${treeRef.commit} does not resolve to declared tree ${treeRef.tree}`);
  }
  await git(root, ["merge-base", "--is-ancestor", request.sourceBase.commit, request.candidate.commit]);
  await git(root, ["merge-base", "--is-ancestor", request.sourceBase.commit, request.expectedPrefix.commit]);
  const currentTarget = await observeTarget({ repositoryRoot: root } as RepositoryBindingV1, request.targetRef);
  const candidateTarget = [request.expectedPrefix.commit];
  if (!candidateTarget.includes(currentTarget.commit)) {
    // Deterministic replay after landing is allowed only after composition proves the intended new OID.
    if (currentTarget.commit === "missing") throw new GitIntegrationBlockedError("TARGET_MISSING", "Target ref is missing");
  }
  const gitVersion = (await git(root, ["--version"])).trim();
  const binding = {
    repositoryRoot: root, commonDir, gitDir, objectFormat, gitVersion, configHash: canonicalHash(configText),
    commonDirIdentityHash: canonicalHash({ pathHash: canonicalHash(commonDir), dev: String(commonStat.dev), ino: String(commonStat.ino), objectFormat }),
    worktreeIdentityHash: canonicalHash({ pathHash: canonicalHash(root), gitDirHash: canonicalHash(gitDir), dev: String(gitStat.dev), ino: String(gitStat.ino) }),
  };
  const observedAuthority = { commonDirIdentityHash: binding.commonDirIdentityHash, worktreeIdentityHash: binding.worktreeIdentityHash, objectFormat: binding.objectFormat, gitVersion: binding.gitVersion, configHash: binding.configHash };
  if (canonicalHash(observedAuthority) !== canonicalHash(request.expectedRepositoryBinding)) throw new GitIntegrationBlockedError("REPOSITORY_AUTHORITY_MISMATCH", "Observed repository/worktree/common-dir binding differs from exact run authority", { observedBindingHash: canonicalHash(observedAuthority), expectedBindingHash: canonicalHash(request.expectedRepositoryBinding) });
  return binding;
}

async function assertCoreOnlyTree(root: string, commit: string): Promise<void> {
  const entries = (await git(root, ["ls-tree", "-r", "-z", "--format=%(objectmode) %(objecttype) %(path)", commit])).split("\0").filter(Boolean);
  if (entries.some((entry) => entry.startsWith("160000 commit "))) throw new GitIntegrationBlockedError("UNSUPPORTED_GITLINK", "Gitlink/submodule tree entries are unsupported even without .gitmodules");
  const files = entries.map((entry) => entry.split(" ").slice(2).join(" "));
  if (files.includes(".gitmodules")) throw new GitIntegrationBlockedError("UNSUPPORTED_SUBMODULES", "Submodules are unsupported");
  for (const path of files.filter((value) => value === ".gitattributes" || value.endsWith("/.gitattributes"))) {
    const text = await git(root, ["show", `${commit}:${path}`]);
    if (/(?:^|\s)(?:filter=|merge=|diff=lfs|working-tree-encoding=|-filter(?:\s|$))/m.test(text)) throw new GitIntegrationBlockedError("UNSUPPORTED_ATTRIBUTES", `Unsupported Git attributes in ${path}`);
  }
}

async function mergeTree(binding: RepositoryBindingV1, base: string, prefix: string, candidate: string): Promise<string> {
  let output: string;
  try { output = await git(binding.repositoryRoot, ["merge-tree", "--write-tree", "--no-messages", `--merge-base=${base}`, prefix, candidate]); }
  catch (error) { throw new GitIntegrationBlockedError("COMPOSITION_CONFLICT", "merge-tree did not produce an accepted composed tree", { diagnostics: boundedError(error) }); }
  const oid = output.trim();
  if (!OID_RE.test(oid)) throw new GitIntegrationBlockedError("COMPOSITION_AMBIGUOUS", "merge-tree output was not exactly one tree OID", { output: output.slice(0, 4096) });
  await assertObject(binding.repositoryRoot, oid, "tree");
  return oid;
}

async function commitTree(binding: RepositoryBindingV1, tree: string, parent: string, message: string, timestamp: string): Promise<string> {
  const env = gitEnvironment({
    GIT_AUTHOR_NAME: "Pi DAG Integration", GIT_AUTHOR_EMAIL: "pi-dag@localhost.invalid",
    GIT_COMMITTER_NAME: "Pi DAG Integration", GIT_COMMITTER_EMAIL: "pi-dag@localhost.invalid",
    GIT_AUTHOR_DATE: timestamp, GIT_COMMITTER_DATE: timestamp,
  });
  const output = await git(binding.repositoryRoot, ["commit-tree", tree, "-p", parent, "-m", message], { env });
  const oid = output.trim();
  if (!OID_RE.test(oid)) throw new GitIntegrationBlockedError("COMMIT_TREE_AMBIGUOUS", "commit-tree output was not exactly one commit OID");
  return oid;
}

async function assertCommit(binding: RepositoryBindingV1, commit: string, tree: string, parent: string): Promise<void> {
  await assertObject(binding.repositoryRoot, commit, "commit");
  const actualTree = (await git(binding.repositoryRoot, ["rev-parse", `${commit}^{tree}`])).trim();
  const parents = (await git(binding.repositoryRoot, ["show", "-s", "--format=%P", commit])).trim().split(/\s+/).filter(Boolean);
  if (actualTree !== tree || parents.length !== 1 || parents[0] !== parent) throw new GitIntegrationBlockedError("SYNTHETIC_COMMIT_INVALID", "Synthetic commit tree or parent identity is invalid");
}

async function createImmutableRef(binding: RepositoryBindingV1, ref: string, oid: string): Promise<"created" | "exact"> {
  await git(binding.repositoryRoot, ["check-ref-format", ref]);
  const existing = (await git(binding.repositoryRoot, ["show-ref", "--verify", "--hash", ref], { allowExit: [0, 1, 2, 128] })).trim();
  if (existing) {
    if ((await git(binding.repositoryRoot, ["symbolic-ref", "-q", ref], { allowExit: [0, 1] })).trim()) throw new GitIntegrationBlockedError("PRIVATE_REF_SYMBOLIC", "Immutable private ref must be a direct ref", { refHash: canonicalHash(ref) });
    if (existing !== oid) throw new GitIntegrationBlockedError("PRIVATE_REF_CONFLICT", "Immutable private ref points to another OID", { refHash: canonicalHash(ref), existing, expected: oid });
    return "exact";
  }
  try { await git(binding.repositoryRoot, ["update-ref", "--create-reflog", "-m", "pi-dag v1 immutable anchor", ref, oid, ZERO_OID[binding.objectFormat]]); }
  catch (error) {
    const raced = (await git(binding.repositoryRoot, ["show-ref", "--verify", "--hash", ref], { allowExit: [0, 1, 2, 128] })).trim();
    if (raced !== oid) throw error;
  }
  if ((await git(binding.repositoryRoot, ["symbolic-ref", "-q", ref], { allowExit: [0, 1] })).trim() || (await git(binding.repositoryRoot, ["show-ref", "--verify", "--hash", ref])).trim() !== oid) throw new GitIntegrationBlockedError("PRIVATE_REF_PUBLICATION_AMBIGUOUS", "Immutable private ref publication did not produce one exact direct ref");
  return "created";
}

async function materializeVerificationWorktree(binding: RepositoryBindingV1, workspace: string, commit: string): Promise<Record<string, unknown>> {
  const expectedTree = (await git(binding.repositoryRoot, ["rev-parse", `${commit}^{tree}`])).trim();
  if (await exists(workspace)) {
    const observed = await inspectWorkspace(binding, workspace);
    if (!observed.commonDirMatches || observed.head !== commit || observed.tree !== expectedTree || !observed.detached) throw new GitIntegrationBlockedError("WORKTREE_COLLISION", "Existing verification worktree identity conflicts with intent", observed);
    return observed;
  }
  await mkdir(dirname(workspace), { recursive: true });
  await git(binding.repositoryRoot, ["worktree", "add", "--detach", workspace, commit]);
  const observed = await inspectWorkspace(binding, workspace);
  if (!observed.commonDirMatches || observed.head !== commit || observed.tree !== expectedTree || !observed.detached) throw new GitIntegrationBlockedError("WORKTREE_IDENTITY_MISMATCH", "Materialized verification worktree does not match intent", observed);
  return observed;
}

async function inspectWorkspace(binding: RepositoryBindingV1, workspace: string): Promise<Record<string, any>> {
  const canonical = await realpath(workspace);
  const common = await realpath((await git(canonical, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).trim());
  const gitDir = await realpath((await git(canonical, ["rev-parse", "--path-format=absolute", "--git-dir"])).trim());
  const info = await stat(canonical); const gitInfo = await stat(gitDir);
  return {
    workspaceIdentityHash: canonicalHash({ pathHash: canonicalHash(canonical), dev: String(info.dev), ino: String(info.ino), gitDirHash: canonicalHash(gitDir), gitDev: String(gitInfo.dev), gitIno: String(gitInfo.ino) }),
    commonDirMatches: common === binding.commonDir, head: (await git(canonical, ["rev-parse", "HEAD"])).trim(),
    tree: (await git(canonical, ["rev-parse", "HEAD^{tree}"])).trim(), detached: !(await git(canonical, ["symbolic-ref", "-q", "HEAD"], { allowExit: [0, 1] })).trim(),
  };
}

async function assertCleanWorkspace(binding: RepositoryBindingV1, workspace: string, commit: string, tree: string): Promise<void> {
  const observed = await inspectWorkspace(binding, workspace);
  const status = await git(workspace, ["status", "--porcelain=v2", "-z", "--untracked-files=all"]);
  const indexTree = (await git(workspace, ["write-tree"])).trim();
  if (!observed.commonDirMatches || observed.head !== commit || observed.tree !== tree || !observed.detached || status.length || indexTree !== tree) throw new GitIntegrationBlockedError("VERIFICATION_WORKTREE_DIRTY", "Verification changed the exact composed worktree", { observed, indexTree, statusHash: canonicalHash(status) });
}

async function assertNoGitOperationInProgress(root: string): Promise<void> {
  for (const marker of ["MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "REBASE_HEAD"]) {
    const path = (await git(root, ["rev-parse", "--path-format=absolute", "--git-path", marker])).trim();
    if (await exists(path)) throw new GitIntegrationBlockedError("GIT_OPERATION_IN_PROGRESS", `Bound session worktree has active Git operation marker ${marker}`);
  }
  const gitDir = (await git(root, ["rev-parse", "--path-format=absolute", "--git-dir"])).trim();
  for (const directory of ["rebase-merge", "rebase-apply", "sequencer"]) if (await exists(join(gitDir, directory))) throw new GitIntegrationBlockedError("GIT_OPERATION_IN_PROGRESS", `Bound session worktree has active Git operation directory ${directory}`);
}

async function assertBoundSessionFastForwardSafe(binding: RepositoryBindingV1, targetRef: string, expectedOld: string): Promise<void> {
  await assertNoGitOperationInProgress(binding.repositoryRoot);
  const branch = (await git(binding.repositoryRoot, ["symbolic-ref", "-q", "HEAD"], { allowExit: [0, 1] })).trim();
  const head = (await git(binding.repositoryRoot, ["rev-parse", "HEAD"])).trim();
  if (branch !== targetRef || head !== expectedOld) throw new GitIntegrationBlockedError("SESSION_BINDING_CHANGED", "Bound session branch or HEAD changed before landing", { branch, head });
  const status = await git(binding.repositoryRoot, ["status", "--porcelain=v2", "-z", "--untracked-files=all"]);
  if (status.length) throw new GitIntegrationBlockedError("SESSION_WORKTREE_DIRTY", "Bound session worktree must be clean before automatic fast-forward", { statusHash: canonicalHash(status) });
  const records = parseWorktreeList(await git(binding.repositoryRoot, ["worktree", "list", "--porcelain", "-z"]));
  const targetRecords = records.filter((record) => record.branch === targetRef);
  if (targetRecords.length !== 1 || await realpath(targetRecords[0].worktree) !== binding.repositoryRoot) throw new GitIntegrationBlockedError("TARGET_MULTIPLE_CHECKOUT", "Target branch is checked out outside the exact bound session worktree", { checkoutCount: targetRecords.length });
}

async function ordinaryFastForward(binding: RepositoryBindingV1, newOid: string): Promise<void> {
  await git(binding.repositoryRoot, ["merge", "--ff-only", "--no-edit", newOid]);
}

async function assertBoundSessionLandedExact(binding: RepositoryBindingV1, targetRef: string, commit: string, tree: string): Promise<void> {
  const branch = (await git(binding.repositoryRoot, ["symbolic-ref", "-q", "HEAD"], { allowExit: [0, 1] })).trim();
  const head = (await git(binding.repositoryRoot, ["rev-parse", "HEAD"])).trim();
  const headTree = (await git(binding.repositoryRoot, ["rev-parse", "HEAD^{tree}"])).trim();
  const indexTree = (await git(binding.repositoryRoot, ["write-tree"])).trim();
  const status = await git(binding.repositoryRoot, ["status", "--porcelain=v2", "-z", "--untracked-files=all"]);
  if (branch !== targetRef || head !== commit || headTree !== tree || indexTree !== tree || status.length) throw new GitIntegrationBlockedError("LANDING_WORKTREE_MISMATCH", "Target ref moved but bound session worktree/index did not reconcile to the exact clean proposal", { branch, head, headTree, indexTree, statusHash: canonicalHash(status) });
}

async function observeTarget(binding: Pick<RepositoryBindingV1, "repositoryRoot">, ref: string): Promise<{ commit: string; tree: string | null }> {
  const commit = (await git(binding.repositoryRoot, ["show-ref", "--verify", "--hash", ref], { allowExit: [0, 1, 2, 128] })).trim();
  if (!commit) return { commit: "missing", tree: null };
  const tree = (await git(binding.repositoryRoot, ["rev-parse", `${commit}^{tree}`])).trim();
  return { commit, tree };
}

function classifyTarget(observed: { commit: string; tree: string | null }, old: GitTreeRefV1, intended: GitTreeRefV1): "old" | "new" | "third" {
  if (observed.commit === old.commit && observed.tree === old.tree) return "old";
  if (observed.commit === intended.commit && observed.tree === intended.tree) return "new";
  return "third";
}

async function cleanupVerificationWorktree(request: GitIntegrationRequestV1, binding: RepositoryBindingV1, workspace: string, expectedHead: string, runtime: GitIntegrationRuntimeV1, at: string): Promise<void> {
  if (!(await exists(workspace))) return;
  const observed = await inspectWorkspace(binding, workspace);
  const status = await git(workspace, ["status", "--porcelain=v2", "-z", "--untracked-files=all"]);
  if (!observed.commonDirMatches || observed.head !== expectedHead || !observed.detached || status.length) throw new GitIntegrationBlockedError("CLEANUP_IDENTITY_AMBIGUOUS", "Verification worktree changed identity or became dirty before cleanup", { observed, statusHash: canonicalHash(status) });
  const payload = { transactionId: request.transactionId, workspaceKey: basename(workspace), workspaceIdentityHash: observed.workspaceIdentityHash, expectedHead };
  const requestHash = canonicalHash({ transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: canonicalHash(request.expectedRepositoryBinding), operation: "cleanup", payload });
  const intent: GitIntegrationIntentV1 = { schemaVersion: 1, kind: "GitIntegrationIntentV1", effectId: `git-${digestHex(requestHash).slice(0, 24)}`, operation: "cleanup", transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: canonicalHash(request.expectedRepositoryBinding), requestHash, createdAt: at, payload };
  await runtime.recordIntent(intent);
  try {
    await git(binding.repositoryRoot, ["worktree", "remove", workspace]); await runtime.failpoint?.("after_cleanup_side_effect", { workspaceKey: basename(workspace), expectedHead });
    const core = { schemaVersion: 1 as const, kind: "GitIntegrationObservationV1" as const, effectId: intent.effectId, transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: intent.repositoryBindingHash, disposition: "applied_exact" as const, observedAt: at, payload: { absent: !(await exists(workspace)), workspaceIdentityHash: observed.workspaceIdentityHash } };
    await runtime.recordObservation({ ...core, observationHash: canonicalHash(core) });
  } catch (error) {
    const core = { schemaVersion: 1 as const, kind: "GitIntegrationObservationV1" as const, effectId: intent.effectId, transactionId: request.transactionId, runId: request.runId, runNonce: request.runNonce, planHash: request.planHash, authorizationSetHash: request.authorizationSetHash, ownerEpoch: request.ownerEpoch, repositoryId: request.repositoryId, repositoryBindingHash: intent.repositoryBindingHash, disposition: "blocked" as const, observedAt: at, payload: { workspaceIdentityHash: observed.workspaceIdentityHash, error: boundedError(error) } };
    await runtime.recordObservation({ ...core, observationHash: canonicalHash(core) }); throw error;
  }
}

function normalizeVerification(value: GitVerificationResultV1): GitVerificationResultV1 {
  const prefixEvidenceHashes = [...new Set(value.prefixEvidenceHashes)].sort();
  const finalEvidenceHashes = [...new Set(value.finalEvidenceHashes)].sort();
  if (!prefixEvidenceHashes.length || !finalEvidenceHashes.length || ![...prefixEvidenceHashes, ...finalEvidenceHashes, value.environmentClosureHash].every((hash) => HASH_RE.test(hash))) throw new GitIntegrationBlockedError("INVALID_VERIFICATION_EVIDENCE", "Verification must return exact nonempty hash-bound prefix and final evidence");
  return { prefixEvidenceHashes, finalEvidenceHashes, environmentClosureHash: value.environmentClosureHash };
}

function integrationCommitMessage(request: GitIntegrationRequestV1): string {
  if (!/^(?:feat|fix|refactor|perf|test|build|ci|docs|chore)(?:\([a-z0-9._/-]+\))?!?: [^\r\n\0]{1,200}$/.test(request.commitSubject)) throw new GitIntegrationBlockedError("INVALID_COMMIT_SUBJECT", "Integration subject must be one Conventional Commit line");
  return [request.commitSubject, "", `Pi-DAG-Plan: ${request.planHash}`, `Pi-DAG-Run: ${canonicalHash({ runId: request.runId, runNonce: request.runNonce })}`, `Pi-DAG-Work-Item: ${request.workItemId}`, `Pi-DAG-Candidate: ${request.candidate.commit}`, `Pi-DAG-Candidate-Tree: ${request.candidate.tree}`].join("\n");
}

function privateRefBase(request: GitIntegrationRequestV1): string {
  return `refs/pi-dag/v1/transactions/${digestHex({ planHash: request.planHash, runId: request.runId, runNonce: request.runNonce, repositoryId: request.repositoryId })}`;
}

function assertOperationGuard(guard: GitOperationGuardV1, kind: string, payload: unknown): void {
  if (!ID_RE.test(guard.effectId) || !Number.isInteger(guard.ownerEpoch) || guard.ownerEpoch < 0 || guard.requestHash !== canonicalHash({ kind, payload })) throw new GitIntegrationBlockedError("EFFECT_GUARD_MISMATCH", "Git operation does not match its exact persisted effect request hash/owner epoch");
}

function assertRequest(value: GitIntegrationRequestV1): void {
  if (value?.schemaVersion !== 1) throw new TypeError("GitIntegrationRequestV1 schemaVersion must be 1");
  for (const id of [value.transactionId, value.runId, value.repositoryId, value.workItemId]) if (!ID_RE.test(id)) throw new TypeError(`Invalid Git integration ID: ${id}`);
  if (typeof value.runNonce !== "string" || value.runNonce.length < 16 || value.runNonce.length > 256) throw new TypeError("Invalid Git integration run nonce");
  for (const hash of [value.planHash, value.authorizationSetHash, value.compositionProfileHash, value.prefixValidationProfileHash, value.finalValidationProfileHash]) if (!HASH_RE.test(hash)) throw new TypeError(`Invalid Git integration hash: ${hash}`);
  if (!Number.isInteger(value.candidateGeneration) || value.candidateGeneration < 1 || !Number.isInteger(value.ownerEpoch) || value.ownerEpoch < 0) throw new TypeError("Invalid Git integration generation/owner epoch");
  if (!Number.isFinite(Date.parse(value.planCreatedAt)) || !value.planCreatedAt.endsWith("Z")) throw new TypeError("Invalid planCreatedAt");
  for (const ref of [value.sourceBase, value.candidate, value.expectedPrefix]) if (ref.repositoryId !== value.repositoryId || !OID_RE.test(ref.commit) || !OID_RE.test(ref.tree)) throw new TypeError("Invalid or cross-repository Git tree identity");
  const expected = value.expectedRepositoryBinding;
  if (!expected || !HASH_RE.test(expected.commonDirIdentityHash) || !HASH_RE.test(expected.worktreeIdentityHash) || !HASH_RE.test(expected.configHash) || !["sha1", "sha256"].includes(expected.objectFormat) || typeof expected.gitVersion !== "string" || !expected.gitVersion.startsWith("git version ")) throw new TypeError("Invalid expected repository binding authority");
}

class IntegrationDirectoryLockV1 {
  readonly path: string; readonly retiredPath: string; readonly identityHash: string; readonly recoveredStaleIdentityHashes: string[]; readonly directoryIdentity: string; #released = false;
  private constructor(path: string, retiredPath: string, identityHash: string, recoveredStaleIdentityHashes: string[], directoryIdentity: string) { this.path = path; this.retiredPath = retiredPath; this.identityHash = identityHash; this.recoveredStaleIdentityHashes = recoveredStaleIdentityHashes; this.directoryIdentity = directoryIdentity; }
  static async acquire(path: string, transactionId: string, acquiredAt: string): Promise<IntegrationDirectoryLockV1> {
    await mkdir(dirname(path), { recursive: true });
    const recoveredStaleIdentityHashes: string[] = [];
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        const processStartIdentity = await currentProcessStartIdentity();
        const metadata = { schemaVersion: 1, kind: "GitIntegrationLockV1", transactionId, pid: process.pid, processStartIdentity, acquiredAt, nonce: randomUUID() };
        const pending = `${path}.initializing-${process.pid}-${randomUUID()}`;
        await mkdir(pending); await durableWrite(join(pending, "metadata.json"), `${canonicalStringify(metadata)}\n`); await fsyncDirectory(pending);
        try { await rename(pending, path); await fsyncDirectory(dirname(path)); }
        catch (publishError: any) { await rm(pending, { recursive: true, force: true }); if (!["EEXIST", "ENOTEMPTY"].includes(publishError?.code)) throw publishError; throw Object.assign(new Error("lock exists"), { code: "EEXIST" }); }
        const identityHash = canonicalHash(metadata); const publishedStat = await stat(path); const directoryIdentity = `${publishedStat.dev}:${publishedStat.ino}`;
        return new IntegrationDirectoryLockV1(path, `${path}.retired-${digestHex({ identityHash, nonce: randomUUID() })}`, identityHash, recoveredStaleIdentityHashes.sort(), directoryIdentity);
      } catch (error: any) {
        if (error?.code !== "EEXIST") throw error;
        const observedStat = await stat(path).catch(() => null); const metadata = await readJson(join(path, "metadata.json")).catch(() => null) as any;
        if (!observedStat || !metadata || typeof metadata.pid !== "number" || typeof metadata.processStartIdentity !== "string") throw new GitIntegrationBlockedError("INTEGRATION_LOCK_AMBIGUOUS", "Integration lock metadata is missing or corrupt");
        const disposition = await processIdentityDisposition(metadata.pid, metadata.processStartIdentity);
        if (disposition === "live" || disposition === "ambiguous") throw new GitIntegrationBlockedError("INTEGRATION_LOCKED", "Another exact live or ambiguous integration owner holds the repository lock", { disposition });
        const currentStat = await stat(path).catch(() => null); if (!currentStat || currentStat.dev !== observedStat.dev || currentStat.ino !== observedStat.ino) continue;
        const staleIdentityHash = canonicalHash(metadata); const stale = `${path}.stale-${digestHex({ metadata, nonce: randomUUID() })}`;
        try { await rename(path, stale); await fsyncDirectory(dirname(path)); recoveredStaleIdentityHashes.push(staleIdentityHash); } catch (renameError: any) { if (renameError?.code !== "ENOENT") throw renameError; }
      }
    }
    throw new GitIntegrationBlockedError("INTEGRATION_LOCK_RACE", "Could not establish one exact integration lock owner");
  }
  async release(): Promise<void> {
    if (this.#released) return; this.#released = true;
    const metadata = await readJson(join(this.path, "metadata.json")).catch(() => null) as any; const directoryStat = await stat(this.path).catch(() => null); const currentStart = await currentProcessStartIdentity();
    if (!metadata || canonicalHash(metadata) !== this.identityHash || metadata.pid !== process.pid || metadata.processStartIdentity !== currentStart || !directoryStat || `${directoryStat.dev}:${directoryStat.ino}` !== this.directoryIdentity) throw new GitIntegrationBlockedError("INTEGRATION_LOCK_RELEASE_AMBIGUOUS", "Refusing to release a lock no longer owned by this exact process/directory identity");
    try { await rename(this.path, this.retiredPath); await fsyncDirectory(dirname(this.path)); } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
    await rm(this.retiredPath, { recursive: true, force: true }); await fsyncDirectory(dirname(this.path));
  }
}

async function currentProcessStartIdentity(): Promise<string> {
  if (process.platform === "linux") {
    const text = await readFile(`/proc/${process.pid}/stat`, "utf8");
    return `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
  }
  return FALLBACK_PROCESS_START_IDENTITY;
}

async function processIdentityDisposition(pid: number, expected: string): Promise<"live" | "dead" | "reused" | "ambiguous"> {
  if (process.platform !== "linux") { try { process.kill(pid, 0); return "ambiguous"; } catch (error: any) { return error?.code === "ESRCH" ? "dead" : "ambiguous"; } }
  try {
    const text = await readFile(`/proc/${pid}/stat`, "utf8");
    const actual = `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
    return actual === expected ? "live" : "reused";
  } catch (error: any) { return error?.code === "ENOENT" ? "dead" : "ambiguous"; }
}

function parseWorktreeList(text: string): Array<{ worktree: string; head: string | null; branch: string | null; detached: boolean }> {
  return text.split("\0\0").filter(Boolean).map((record) => {
    const fields = record.split("\0").filter(Boolean); const output = { worktree: "", head: null as string | null, branch: null as string | null, detached: false };
    for (const field of fields) {
      const space = field.indexOf(" "); const key = space < 0 ? field : field.slice(0, space); const value = space < 0 ? "" : field.slice(space + 1);
      if (key === "worktree") output.worktree = value; else if (key === "HEAD") output.head = value; else if (key === "branch") output.branch = value; else if (key === "detached") output.detached = true;
    }
    if (!output.worktree) throw new GitIntegrationBlockedError("WORKTREE_LIST_CORRUPT", "Git worktree listing omitted a worktree path");
    return output;
  });
}

async function git(cwd: string, args: string[], options: { allowExit?: number[]; env?: NodeJS.ProcessEnv } = {}): Promise<string> {
  const allowExit = options.allowExit ?? [0];
  try {
    const result = await execFileAsync("git", ["-c", "core.pager=cat", "-c", "rerere.enabled=false", "-c", "core.hooksPath=/dev/null", "-c", "core.fsync=all", "-c", "core.fsyncMethod=fsync", ...args], { cwd, env: options.env ?? gitEnvironment(), encoding: "utf8", maxBuffer: MAX_GIT_OUTPUT, windowsHide: true });
    return result.stdout;
  } catch (error: any) {
    if (allowExit.includes(error?.code)) return String(error?.stdout ?? "");
    throw error;
  }
}

function gitEnvironment(extra: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  const inherited = { ...process.env };
  for (const key of Object.keys(inherited)) if (key.startsWith("GIT_")) delete inherited[key];
  return { ...inherited, LC_ALL: "C", LANG: "C", TZ: "UTC", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: process.platform === "win32" ? "NUL" : "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_NO_REPLACE_OBJECTS: "1", GIT_PAGER: "cat", GIT_EDITOR: "false", ...extra };
}

async function assertObject(root: string, oid: string, type: "commit" | "tree"): Promise<void> {
  if (!OID_RE.test(oid) || (await git(root, ["cat-file", "-t", oid])).trim() !== type) throw new GitIntegrationBlockedError("GIT_OBJECT_IDENTITY", `Expected ${type} object ${oid}`);
}

async function publishImmutableJson(path: string, value: unknown): Promise<void> {
  const text = `${canonicalStringify(value)}\n`;
  if (await exists(path)) { if (await readFile(path, "utf8") !== text) throw new GitIntegrationBlockedError("IMMUTABLE_ARTIFACT_CONFLICT", "Immutable integration artifact path contains conflicting bytes"); return; }
  await mkdir(dirname(path), { recursive: true });
  const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await durableWrite(temp, text);
  try { await link(temp, path); } catch (error: any) {
    if (error?.code !== "EEXIST") throw error;
    if (await readFile(path, "utf8") !== text) throw new GitIntegrationBlockedError("IMMUTABLE_ARTIFACT_CONFLICT", "Immutable integration artifact raced with conflicting bytes");
  } finally { await rm(temp, { force: true }); }
  await fsyncDirectory(dirname(path));
}

async function durableWrite(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const handle = await open(path, "wx");
  try { await handle.writeFile(text, "utf8"); await handle.sync(); } finally { await handle.close(); }
  await fsyncDirectory(dirname(path));
}

async function fsyncDirectory(path: string): Promise<void> { const handle = await open(path, "r"); try { await handle.sync(); } finally { await handle.close(); } }
async function exists(path: string): Promise<boolean> { try { await stat(path); return true; } catch (error: any) { if (error?.code === "ENOENT") return false; throw error; } }
async function readJson(path: string): Promise<unknown> { return JSON.parse(await readFile(path, "utf8")); }
function digestHex(value: unknown): string { return createHash("sha256").update(typeof value === "string" ? value : canonicalStringify(value)).digest("hex"); }
function boundedError(error: unknown): Record<string, unknown> { const value = error as any; return { message: String(value?.message ?? error).slice(0, 4096), stdout: String(value?.stdout ?? "").slice(0, 4096), stderr: String(value?.stderr ?? "").slice(0, 4096) }; }
