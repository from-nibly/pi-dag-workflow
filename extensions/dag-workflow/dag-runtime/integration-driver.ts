import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdir, readFile, readdir, realpath, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { canonicalHash, parseStrictJson } from "./common.ts";
import {
  acquireGitIntegrationLockV1,
  composeGitProposalV1,
  ensurePrivateGitRefV1,
  GitIntegrationBlockedError,
  landOrReconcileBoundWorktreeV1,
  preflightBoundRepositoryV1,
  readRepositoryBindingIdentityV1,
  type GitIntegrationRequestV1,
  type RepositoryBindingV1,
} from "./git-integration.ts";
import type { DagIntegrationReconciliationAdapterV1 } from "./lifecycle-runtime.ts";
import type { CanonicalDagPlanV1 } from "./plan.ts";
import type { DagRunInputV1 } from "./reducer.ts";
import { integrationValidationEffectRequestV1, type DagRunStateV1, type DagRunValidationContextV1, type IntegrationValidationProfileMappingV1 } from "./run-state.ts";
import { DagRunSnapshotStoreV1, type DagRunStoreLockIdentityV1 } from "./store.ts";

const run = promisify(execFile);
const HASH_PREFIX = "sha256:";
const MAX_VALIDATION_OUTPUT = 1024 * 1024;

export type DagGitIntegrationDriverFailpointV1 =
  | "after_repository_fact"
  | "after_integration_reserve"
  | "after_composition_dispatch"
  | "after_composition_git"
  | "after_composition_facts"
  | "after_composition_commit"
  | "after_prefix_validation_intent"
  | "after_prefix_validation_dispatch"
  | "after_prefix_validation_result"
  | "after_prefix_validation_reconcile"
  | "after_final_validation_intent"
  | "after_final_validation_dispatch"
  | "after_final_validation_result"
  | "after_final_validation_reconcile"
  | "after_verification_facts"
  | "after_landing_intent"
  | "after_landing_dispatch"
  | "before_landing"
  | "after_landing_git"
  | "after_landing_fact"
  | "after_landing_proven_absent_commit"
  | "after_receipt_facts";

export interface DagReducerGitIntegrationDriverOptionsV1 {
  store: DagRunSnapshotStoreV1;
  context: DagRunValidationContextV1;
  lock: DagRunStoreLockIdentityV1;
  now?: () => string;
  failpoint?: (point: DagGitIntegrationDriverFailpointV1, context: Readonly<Record<string, unknown>>) => Promise<void> | void;
}

/**
 * Reducer-aware adapter around the operation-sized primitives used by ExactGitIntegrationV1.
 * It never edits a snapshot directly: every lifecycle transition is a store CAS through the reducer.
 */
export class DagReducerGitIntegrationDriverV1 implements DagIntegrationReconciliationAdapterV1 {
  readonly store: DagRunSnapshotStoreV1;
  readonly context: DagRunValidationContextV1;
  readonly lock: DagRunStoreLockIdentityV1;
  readonly now: () => string;
  readonly failpoint?: DagReducerGitIntegrationDriverOptionsV1["failpoint"];

  constructor(options: DagReducerGitIntegrationDriverOptionsV1) {
    this.store = options.store;
    this.context = options.context;
    this.lock = options.lock;
    this.now = options.now ?? (() => new Date().toISOString());
    this.failpoint = options.failpoint;
  }

  async reconcileExact(input: Parameters<DagIntegrationReconciliationAdapterV1["reconcileExact"]>[0]): Promise<void> {
    input.signal?.throwIfAborted?.();
    const state = await this.readExact(input.state, input.reservation.reservationId, input.signal);
    const reservation = state.scheduler.reservations[input.reservation.reservationId];
    const item = state.workItems[reservation.workItemId];
    const train = state.integrationTrains[reservation.repositoryId];
    if (!item || !train) throw new Error("Integration reservation does not resolve its exact item and train");

    const attemptId = train.activeIntegrationAttemptId;
    if (!attemptId) {
      await this.reserve(state, input.plan, reservation, input.repositoryRoot, input.signal);
      return;
    }
    const attempt = state.integrationAttempts[attemptId];
    const entry = attempt ? train.entries[attempt.entryId] : null;
    if (!attempt || !entry || entry.currentAttemptId !== attemptId) throw new Error("Active integration attempt projection is incomplete");
    const compositionEffect = state.effects[attempt.compositionEffectId];

    if (entry.state === "composing" && compositionEffect?.state === "intended") {
      await this.mutate(state, "mark_effect_dispatching", "command", { effectId: compositionEffect.effectId, expectedDispatchCount: compositionEffect.dispatchCount }, `dispatch-${compositionEffect.effectId}-${compositionEffect.dispatchCount}`, input.signal);
      await this.hit("after_composition_dispatch", { integrationAttemptId: attemptId }, input.signal);
      return;
    }
    if (entry.state === "composing" && compositionEffect?.state === "dispatching") {
      await this.compose(state, input.plan, reservation, attemptId, input.repositoryRoot, input.signal);
      return;
    }
    if (entry.state === "verifying_prefix") {
      await this.verify(state, input.plan, reservation, attemptId, input.repositoryRoot, input.signal);
      return;
    }
    if (entry.state === "landing" && !attempt.landingEffectId) {
      await this.prepareLanding(state, reservation, attemptId, input.signal);
      return;
    }
    if (entry.state === "landing" && attempt.landingEffectId) {
      const landingEffect = state.effects[attempt.landingEffectId];
      if (landingEffect?.state === "intended") {
        await this.mutate(state, "mark_effect_dispatching", "command", { effectId: landingEffect.effectId, expectedDispatchCount: landingEffect.dispatchCount }, `dispatch-${landingEffect.effectId}-${landingEffect.dispatchCount}`, input.signal);
        await this.hit("after_landing_dispatch", { integrationAttemptId: attemptId }, input.signal);
        return;
      }
      if (landingEffect?.state === "reconciled" && landingEffect.reconciliation === "proven_absent") {
        await this.mutate(state, "retry_effect_dispatch", "command", { effectId: landingEffect.effectId, expectedDispatchCount: landingEffect.dispatchCount, reason: "uncertain_acknowledgement" }, `retry-${landingEffect.effectId}-${landingEffect.dispatchCount}`, input.signal);
        return;
      }
      if (landingEffect?.state === "dispatching") {
        await this.land(state, input.plan, reservation, attemptId, input.repositoryRoot, input.signal);
        return;
      }
    }
    if (attempt.landingState === "reconciled") {
      await this.accept(state, input.plan, reservation, attemptId, input.repositoryRoot, input.signal);
      return;
    }
    if (["landed"].includes(attempt.landingState) || item.current === "complete") return;
    throw new Error(`Integration attempt ${attemptId} is not in a reconcilable reducer phase (${entry.state}/${attempt.landingState})`);
  }

  private async reserve(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, repositoryRoot: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const item = state.workItems[reservation.workItemId];
    const train = state.integrationTrains[reservation.repositoryId];
    const member = plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === reservation.repositoryId)?.members.find(({ workItemId }) => workItemId === item.workItemId);
    if (!member || !item.candidate || !item.integrationEntryId || item.current !== "integrating") throw new Error("F8 integration reservation is not exactly integration-ready");
    const integrationAttemptId = opaqueId("integration", { runNonce: state.runNonce, reservationId: reservation.reservationId, generation: item.candidateGeneration });
    const request = await this.request(state, plan, integrationAttemptId, item, train, repositoryRoot, undefined, signal);
    const binding = await preflightBoundRepositoryV1(request, signal);
    const observed = await observeTarget(repositoryRoot, state.repositories[reservation.repositoryId].targetRef, signal);
    const bindingFact = gitFact(state, {
      factType: "repository_binding", repositoryId: reservation.repositoryId, integrationAttemptId: null, effectId: null,
      requestHash: canonicalHash({ repositoryId: reservation.repositoryId, targetRef: state.repositories[reservation.repositoryId].targetRef, expectedTarget: train.expectedTarget, expectedRepositoryBinding: request.expectedRepositoryBinding }),
      binding, targetRef: state.repositories[reservation.repositoryId].targetRef, commit: observed.commit, tree: observed.tree,
      parentCommit: null, reconciliation: sameTree(observed, train.expectedTarget) ? "applied_exact" : "conflict",
      detailsHash: canonicalHash({ observed, expectedTarget: train.expectedTarget }), observedAt: state.updatedAt,
    });
    await this.publishFact(bindingFact, signal);
    await this.hit("after_repository_fact", { integrationAttemptId, repositoryBindingFactHash: bindingFact.hash }, signal);
    if (!sameTree(observed, train.expectedTarget)) throw new GitIntegrationBlockedError("TARGET_DRIFT", "Target differs from the exact reducer-authorized integration target", { observed });

    const fresh = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, train.expectedTarget, signal);
    const compositionPayload = { sourceBase: item.candidate.base, candidate: item.candidate.git, expectedPrefix: train.acceptedPrefix, compositionProfileHash: plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === reservation.repositoryId)!.compositionProfileHash, ownerEpoch: fresh.owner.ownerEpoch };
    const effect = effectIntent(fresh, `${integrationAttemptId}-compose`, "compose_candidate", { kind: "train", id: plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === reservation.repositoryId)!.trainId }, canonicalHash({ kind: "compose", payload: compositionPayload }), item.candidateGeneration, canonicalHash(item.gateIds.map((id) => fresh.gates[id])), fresh.updatedAt);
    const refs = privateRefs(request);
    await this.mutate(fresh, "reserve_integration_attempt", "command", {
      integrationAttemptId, entryId: item.integrationEntryId, repositoryId: reservation.repositoryId, workItemId: item.workItemId,
      retryOrdinal: 0, retryAuthorizationKey: null, sourceCandidateHash: item.candidate.candidateHash,
      sourceBase: item.candidate.base, sourceCandidate: item.candidate.git, expectedPrefix: train.acceptedPrefix, expectedTarget: train.expectedTarget,
      temporaryRef: refs.composed, repositoryBindingFactHash: bindingFact.hash, lockLeaseId: `${integrationAttemptId}-lock`, compositionEffect: effect,
    }, `reserve-${integrationAttemptId}`, signal);
    await this.hit("after_integration_reserve", { integrationAttemptId }, signal);
  }

  private async compose(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, attemptId: string, repositoryRoot: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const attempt = state.integrationAttempts[attemptId];
    const item = state.workItems[reservation.workItemId];
    const effect = state.effects[attempt.compositionEffectId];
    const request = await this.request(state, plan, attemptId, item, state.integrationTrains[reservation.repositoryId], repositoryRoot, attempt, signal);
    const binding = await preflightBoundRepositoryV1(request, signal);
    const lock = await this.acquireOperationLock(state, reservation.reservationId, request, binding, attempt.expectedTarget, `lock-compose-${attemptId}`, signal);
    try {
      const refs = privateRefs(request);
      const anchors: Array<[keyof typeof refs, string, string]> = [
        ["baseline", refs.baseline, request.sourceBase.commit], ["candidate", refs.candidate, request.candidate.commit], ["prefix", refs.prefix, request.expectedPrefix.commit],
      ];
      for (const [role, ref, oid] of anchors) {
        await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
        await ensurePrivateGitRefV1(binding, ref, oid, operationGuard(`${effect.effectId}-${role}`, state.owner.ownerEpoch, "anchor_ref", { commonDirIdentityHash: binding.commonDirIdentityHash, refHash: canonicalHash(ref), oid }), signal);
      }
      await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
      let proposal: Awaited<ReturnType<typeof composeGitProposalV1>>;
      try {
        const successorGuardPayload = { sourceBase: request.sourceBase, candidate: request.candidate, expectedPrefix: request.expectedPrefix, compositionProfileHash: request.compositionProfileHash, ownerEpoch: request.ownerEpoch };
        proposal = await composeGitProposalV1(request, binding, { effectId: effect.effectId, requestHash: canonicalHash({ kind: "compose", payload: successorGuardPayload }), ownerEpoch: state.owner.ownerEpoch }, signal);
      } catch (error) {
        if (!(error instanceof GitIntegrationBlockedError) || !["COMPOSITION_CONFLICT", "COMPOSITION_AMBIGUOUS"].includes(error.code)) throw error;
        const conflictFact = gitFact(state, {
          factType: "composition", repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, effectId: effect.effectId, requestHash: effect.requestHash,
          binding, targetRef: null, commit: null, tree: null, parentCommit: attempt.expectedPrefix.commit, reconciliation: "conflict",
          detailsHash: canonicalHash({ code: error.code, details: error.details }), observedAt: state.updatedAt,
        });
        await this.publishFact(conflictFact, signal);
        const fresh = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
        await this.mutate(fresh, "record_git_composition_conflict", "observation", { integrationAttemptId: attemptId, compositionFactHash: conflictFact.hash, conflictClass: error.code === "COMPOSITION_CONFLICT" ? "mechanical" : "ambiguous" }, `composition-conflict-${attemptId}`, signal);
        return;
      }
      await this.hit("after_composition_git", { integrationAttemptId: attemptId, composedCommit: proposal.composed.commit }, signal);
      for (const [role, ref] of [["composed", refs.composed], ["proposal", refs.proposal]] as const) {
        await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
        await ensurePrivateGitRefV1(binding, ref, proposal.composed.commit, operationGuard(`${effect.effectId}-${role}`, state.owner.ownerEpoch, "anchor_ref", { commonDirIdentityHash: binding.commonDirIdentityHash, refHash: canonicalHash(ref), oid: proposal.composed.commit }), signal);
      }
      const roleCommits = { baseline: request.sourceBase, candidate: request.candidate, prefix: request.expectedPrefix, composed: proposal.composed, proposal: proposal.composed };
      const privateFacts: any[] = [];
      for (const [role, targetRef] of Object.entries(refs)) {
        signal?.throwIfAborted?.();
        const identity = roleCommits[role as keyof typeof roleCommits];
        const fact = gitFact(state, {
          factType: "private_ref", repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, effectId: effect.effectId, requestHash: effect.requestHash,
          binding, targetRef, commit: identity.commit, tree: identity.tree, parentCommit: ["composed", "proposal"].includes(role) ? attempt.expectedPrefix.commit : null,
          reconciliation: "applied_exact", detailsHash: canonicalHash({ role, targetRef, commit: identity.commit }), observedAt: state.updatedAt,
        });
        await this.publishFact(fact, signal); privateFacts.push(fact);
      }
      const lineageHash = canonicalHash({ sourceCandidateHash: attempt.sourceCandidateHash, sourceCommit: attempt.sourceCandidate.commit, expectedPrefix: attempt.expectedPrefix, composed: proposal.composed });
      const compositionFact = gitFact(state, {
        factType: "composition", repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, effectId: effect.effectId, requestHash: effect.requestHash,
        binding, targetRef: null, commit: proposal.composed.commit, tree: proposal.composed.tree, parentCommit: attempt.expectedPrefix.commit,
        reconciliation: "applied_exact", detailsHash: canonicalHash({ messageHash: proposal.messageHash, lineageHash }), observedAt: state.updatedAt,
      });
      await this.publishFact(compositionFact, signal);
      await this.hit("after_composition_facts", { integrationAttemptId: attemptId, compositionFactHash: compositionFact.hash }, signal);
      const fresh = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
      await this.mutate(fresh, "record_git_composition", "observation", { integrationAttemptId: attemptId, compositionFactHash: compositionFact.hash, composedTree: proposal.composed, syntheticParentCommit: attempt.expectedPrefix.commit, sourceToIntegratedLineageHash: lineageHash, conflictClass: "none", privateRefFactHashes: privateFacts.map(({ hash }) => hash) }, `composition-${attemptId}`, signal);
      await this.hit("after_composition_commit", { integrationAttemptId: attemptId, compositionFactHash: compositionFact.hash }, signal);
    } finally {
      await lock.release();
    }
  }

  private async verify(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, attemptId: string, repositoryRoot: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const prefixComplete = await this.reconcileValidationPhase(state, plan, reservation, attemptId, repositoryRoot, "prefix", signal);
    if (!prefixComplete) return;
    const afterPrefix = await this.store.read(this.context);
    const finalComplete = await this.reconcileValidationPhase(afterPrefix, plan, reservation, attemptId, repositoryRoot, "final", signal);
    if (!finalComplete) return;

    const freshState = await this.store.read(this.context);
    const attempt = freshState.integrationAttempts[attemptId];
    const trainPlan = plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === reservation.repositoryId)!;
    const request = await this.request(freshState, plan, attemptId, freshState.workItems[reservation.workItemId], freshState.integrationTrains[reservation.repositoryId], repositoryRoot, attempt, signal);
    const binding = await preflightBoundRepositoryV1(request, signal);
    await this.assertAuthority(freshState, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
    const prefixEffect = this.validationEffect(freshState, attemptId, "prefix");
    const finalEffect = this.validationEffect(freshState, attemptId, "final");
    const prefix = await this.store.readImmutableFact(prefixEffect.executionObservationHash) as any;
    const final = await this.store.readImmutableFact(finalEffect.executionObservationHash) as any;
    const prefixReconciliation = await this.store.readImmutableFact(prefixEffect.observationHash) as any;
    const finalReconciliation = await this.store.readImmutableFact(finalEffect.observationHash) as any;
    const phases = [{ phase: "prefix", profileHash: trainPlan.prefixValidationProfileHash }, { phase: "final", profileHash: trainPlan.finalValidationProfileHash }];
    const facts = [prefix, final];
    const environmentClosureHash = canonicalHash({ driver: "reducer-aware-real-git-validation-v1", repositoryBinding: { commonDirIdentityHash: binding.commonDirIdentityHash, worktreeIdentityHash: binding.worktreeIdentityHash, gitVersionHash: canonicalHash(binding.gitVersion), configHash: binding.configHash, objectFormat: binding.objectFormat }, profiles: phases, executions: facts.map(({ hash, executionId, outputHash, startedAt, completedAt }) => ({ hash, executionId, outputHash, startedAt, completedAt })), effectReconciliations: [prefixReconciliation.hash, finalReconciliation.hash] });
    const closure = { prefixEvidenceHashes: [prefix.hash], finalEvidenceHashes: [final.hash], prefixEffectReconciliationHashes: [prefixReconciliation.hash], finalEffectReconciliationHashes: [finalReconciliation.hash], environmentClosureHash };
    const verificationRequestHash = canonicalHash({ kind: "proposal_verification", integrationAttemptId: attemptId, closure });
    const fact = gitFact(freshState, { factType: "proposal_verification", repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, effectId: null, requestHash: verificationRequestHash, binding, targetRef: freshState.repositories[reservation.repositoryId].targetRef, commit: attempt.composedTree!.commit, tree: attempt.composedTree!.tree, parentCommit: attempt.expectedPrefix.commit, reconciliation: "applied_exact", detailsHash: canonicalHash(closure), observedAt: freshState.updatedAt });
    await this.publishFact(fact, signal);
    await this.hit("after_verification_facts", { integrationAttemptId: attemptId, proposalVerificationFactHash: fact.hash }, signal);
    const fresh = await this.assertAuthority(freshState, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
    await this.mutate(fresh, "record_proposal_verification", "observation", { integrationAttemptId: attemptId, proposalVerificationFactHash: fact.hash, ...closure }, `verification-${attemptId}`, signal);
  }

  private async reconcileValidationPhase(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, attemptId: string, repositoryRoot: string, phase: "prefix" | "final", signal?: AbortSignal): Promise<boolean> {
    signal?.throwIfAborted?.();
    const attempt = state.integrationAttempts[attemptId];
    const trainPlan = plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === reservation.repositoryId)!;
    const profileHash = phase === "prefix" ? trainPlan.prefixValidationProfileHash : trainPlan.finalValidationProfileHash;
    const profileId = phase === "prefix" ? trainPlan.prefixValidationProfileId : trainPlan.finalValidationProfileId;
    const profile = this.context.integrationValidationProfiles?.[profileHash];
    if (!profile || canonicalHash(profile) !== profileHash || profile.profileId !== profileId) throw new GitIntegrationBlockedError("VALIDATION_PROFILE_UNAVAILABLE", `Exact ${phase} validation mapping is absent or does not match the plan profile hash`);
    const effectId = `${attemptId}-verify-${phase}`;
    let effect = Object.values(state.effects).find((candidate: any) => candidate.kind === "verify_prefix" && candidate.boundIntegrationAttemptId === attemptId && candidate.executionRequest?.phase === phase) as any;
    let executionRequest = integrationValidationEffectRequestV1(state, this.context, attemptId, phase);
    if (effect) executionRequest = { ...executionRequest, ownerEpoch: effect.boundOwnerEpoch, authorizationSetHash: effect.boundAuthorizationSetHash, freshnessReceiptHash: effect.boundFreshnessReceiptHash };
    const requestHash = canonicalHash(executionRequest);
    if (!effect) {
      const item = state.workItems[reservation.workItemId];
      effect = effectIntent(state, effectId, "verify_prefix", { kind: "train", id: trainPlan.trainId }, requestHash, item.candidateGeneration, canonicalHash(item.gateIds.map((id) => state.gates[id])), state.updatedAt, executionRequest, attemptId);
      await this.mutate(state, "put_effect_intent", "command", { effect }, `validation-intent-${effectId}`, signal);
      await this.hit(`after_${phase}_validation_intent` as DagGitIntegrationDriverFailpointV1, { integrationAttemptId: attemptId, phase, effectId, requestHash }, signal);
      return false;
    }
    if (effect.effectId !== effectId || effect.requestHash !== requestHash || canonicalHash(effect.executionRequest) !== requestHash) throw new GitIntegrationBlockedError("VALIDATION_EFFECT_CONFLICT", `Persisted ${phase} validation effect request identity conflicts with the exact current attempt`);
    if (effect.state === "intended") {
      await this.mutate(state, "mark_effect_dispatching", "command", { effectId, expectedDispatchCount: effect.dispatchCount }, `validation-dispatch-${effectId}-${effect.dispatchCount}`, signal);
      await this.hit(`after_${phase}_validation_dispatch` as DagGitIntegrationDriverFailpointV1, { integrationAttemptId: attemptId, phase, effectId, requestHash }, signal);
      return false;
    }
    if (effect.state === "dispatching") {
      let execution = await this.findDurableValidationObservation(effect, signal);
      if (!execution) {
        const request = await this.request(state, plan, attemptId, state.workItems[reservation.workItemId], state.integrationTrains[reservation.repositoryId], repositoryRoot, attempt, signal);
        const binding = await preflightBoundRepositoryV1(request, signal);
        await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.expectedTarget, signal);
        execution = await executeValidationProfile(profile, { phase, profileId, profileHash }, state, trainPlan.trainId, reservation.repositoryId, attemptId, attempt.composedTree!, binding, request.controlRoot, repositoryRoot, () => state.updatedAt, effect, signal);
        await this.publishFact(execution, signal);
        await this.hit(`after_${phase}_validation_result` as DagGitIntegrationDriverFailpointV1, { integrationAttemptId: attemptId, phase, effectId, requestHash, executionObservationHash: execution.hash }, signal);
      }
      const fresh = await this.store.read(this.context);
      await this.mutate(fresh, "record_effect_execution", "observation", { effectId, executionObservationHash: execution.hash }, `validation-result-${effectId}-${execution.hash.slice(HASH_PREFIX.length, HASH_PREFIX.length + 12)}`, signal);
      return false;
    }
    if (effect.state === "observed") {
      const execution = await this.store.readImmutableFact(effect.executionObservationHash) as any;
      const reconciliation = withHash({ kind: "effect_reconciliation", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, effectId, requestHash, reconciliation: "applied_exact", executionObservationHash: execution.hash, resultIdentityHash: execution.hash, closedAt: execution.completedAt });
      await this.publishFact(reconciliation, signal);
      const fresh = await this.store.read(this.context);
      await this.mutate(fresh, "record_effect_observation", "observation", { effectId, observationHash: reconciliation.hash, reconciliation: "applied_exact", terminalState: "reconciled" }, `validation-reconcile-${effectId}`, signal);
      await this.hit(`after_${phase}_validation_reconcile` as DagGitIntegrationDriverFailpointV1, { integrationAttemptId: attemptId, phase, effectId, requestHash, reconciliationHash: reconciliation.hash }, signal);
      return false;
    }
    if (effect.state !== "reconciled" || effect.reconciliation !== "applied_exact" || !effect.executionObservationHash || !effect.observationHash) throw new GitIntegrationBlockedError("VALIDATION_EFFECT_CONFLICT", `${phase} validation effect is not exactly reconciled`);
    const execution = await this.store.readImmutableFact(effect.executionObservationHash) as any;
    if (execution.disposition !== "PASS") throw new GitIntegrationBlockedError("VALIDATION_FAILED", `${phase} validation failed closed`, { verificationFactHash: execution.hash, effectReconciliationHash: effect.observationHash, exitCode: execution.exitCode, parserDisposition: execution.parserDisposition });
    return true;
  }

  private validationEffect(state: DagRunStateV1, attemptId: string, phase: "prefix" | "final"): any {
    const matches = Object.values(state.effects).filter((candidate: any) => candidate.kind === "verify_prefix" && candidate.boundIntegrationAttemptId === attemptId && candidate.executionRequest?.phase === phase);
    if (matches.length !== 1) throw new GitIntegrationBlockedError("VALIDATION_EFFECT_CONFLICT", `Expected one exact ${phase} validation effect`);
    const effect: any = matches[0];
    if (effect.state !== "reconciled" || effect.reconciliation !== "applied_exact" || !effect.executionObservationHash || !effect.observationHash) throw new GitIntegrationBlockedError("VALIDATION_EFFECT_CONFLICT", `${phase} validation effect is not terminally reconciled`);
    return effect;
  }

  private async findDurableValidationObservation(effect: any, signal?: AbortSignal): Promise<any | null> {
    signal?.throwIfAborted?.();
    const entries = (await readdir(this.store.factsDirectory)).filter((name) => /^[0-9a-f]{64}\.json$/.test(name)).sort();
    if (entries.length > 20_000) throw new GitIntegrationBlockedError("VALIDATION_OBSERVATION_CONFLICT", "Immutable fact scan exceeds the bounded validation-recovery limit");
    const matches: any[] = [];
    for (const name of entries) {
      signal?.throwIfAborted?.();
      const fact = await this.store.readImmutableFact(`${HASH_PREFIX}${name.slice(0, 64)}`) as any;
      if (fact?.kind === "verification" && fact.effectId === effect.effectId && fact.requestHash === effect.requestHash) matches.push(fact);
    }
    if (matches.length > 1) throw new GitIntegrationBlockedError("VALIDATION_OBSERVATION_CONFLICT", `Conflicting immutable validation observations exist for ${effect.effectId}/${effect.requestHash}`);
    return matches[0] ?? null;
  }

  private async prepareLanding(state: DagRunStateV1, reservation: any, attemptId: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const attempt = state.integrationAttempts[attemptId];
    const bindingFact: any = await this.store.readImmutableFact(attempt.repositoryBindingFactHash);
    const payload = { commonDirIdentityHash: bindingFact.commonDirIdentityHash, targetRef: state.repositories[reservation.repositoryId].targetRef, expectedOld: attempt.expectedTarget, intended: attempt.composedTree };
    const item = state.workItems[reservation.workItemId];
    const effect = effectIntent(state, `${attemptId}-land`, "land_target", { kind: "repository", id: reservation.repositoryId }, canonicalHash({ kind: "land", payload }), item.candidateGeneration, canonicalHash(item.gateIds.map((id) => state.gates[id])), state.updatedAt);
    await this.mutate(state, "prepare_git_landing", "command", { integrationAttemptId: attemptId, landingEffect: effect, intendedLandedTree: attempt.composedTree }, `landing-intent-${attemptId}`, signal);
    await this.hit("after_landing_intent", { integrationAttemptId: attemptId }, signal);
  }

  private async land(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, attemptId: string, repositoryRoot: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const attempt = state.integrationAttempts[attemptId];
    const effect = state.effects[attempt.landingEffectId!];
    const request = await this.request(state, plan, attemptId, state.workItems[reservation.workItemId], state.integrationTrains[reservation.repositoryId], repositoryRoot, attempt, signal);
    const binding = await preflightBoundRepositoryV1(request, signal);
    const lock = await this.acquireOperationLock(state, reservation.reservationId, request, binding, null, `lock-land-${attemptId}`, signal);
    try {
      await this.hit("before_landing", { integrationAttemptId: attemptId }, signal);
      const freshBefore = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, null, signal);
      const before = await observeTarget(repositoryRoot, state.repositories[reservation.repositoryId].targetRef, signal);
      let reconciliation: "applied_exact" | "proven_absent" | "conflict";
      if (sameTree(before, attempt.intendedLandedTree!)) reconciliation = "applied_exact";
      else if (!sameTree(before, attempt.expectedTarget)) reconciliation = "conflict";
      else reconciliation = await landOrReconcileBoundWorktreeV1(binding, state.repositories[reservation.repositoryId].targetRef, attempt.expectedTarget, attempt.intendedLandedTree!, { effectId: effect.effectId, requestHash: effect.requestHash, ownerEpoch: freshBefore.owner.ownerEpoch }, signal);
      const observed = await observeTarget(repositoryRoot, state.repositories[reservation.repositoryId].targetRef, signal);
      if (reconciliation === "applied_exact" && !sameTree(observed, attempt.intendedLandedTree!)) reconciliation = sameTree(observed, attempt.expectedTarget) ? "proven_absent" : "conflict";
      if (reconciliation === "proven_absent" && !sameTree(observed, attempt.expectedTarget)) reconciliation = "conflict";
      await this.hit("after_landing_git", { integrationAttemptId: attemptId, reconciliation }, signal);
      const detailsHash = canonicalHash({ targetRef: state.repositories[reservation.repositoryId].targetRef, observed, expectedOld: attempt.expectedTarget, intendedNew: attempt.intendedLandedTree });
      const fact = gitFact(state, { factType: "landing", repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, effectId: effect.effectId, requestHash: effect.requestHash, binding, targetRef: state.repositories[reservation.repositoryId].targetRef, commit: observed.commit === "missing" ? null : observed.commit, tree: observed.tree, parentCommit: attempt.expectedPrefix.commit, reconciliation, detailsHash, observedAt: state.updatedAt });
      await this.publishFact(fact, signal);
      await this.hit("after_landing_fact", { integrationAttemptId: attemptId, landingObservationFactHash: fact.hash }, signal);
      const fresh = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, null, signal);
      await this.mutate(fresh, "record_git_landing_reconciliation", "observation", { integrationAttemptId: attemptId, landingObservationFactHash: fact.hash, reconciliation }, `landing-observation-${attemptId}-${fact.hash.slice(HASH_PREFIX.length, HASH_PREFIX.length + 12)}`, signal);
      if (reconciliation === "proven_absent") await this.hit("after_landing_proven_absent_commit", { integrationAttemptId: attemptId, landingObservationFactHash: fact.hash }, signal);
    } finally {
      await lock.release();
    }
  }

  private async accept(state: DagRunStateV1, plan: CanonicalDagPlanV1, reservation: any, attemptId: string, repositoryRoot: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const attempt = state.integrationAttempts[attemptId];
    const item = state.workItems[reservation.workItemId];
    const bindingFact: any = await this.store.readImmutableFact(attempt.repositoryBindingFactHash);
    signal?.throwIfAborted?.();
    const observedBinding = await readRepositoryBindingIdentityV1(repositoryRoot, signal);
    if (bindingFact.commonDirIdentityHash !== observedBinding.commonDirIdentityHash || bindingFact.worktreeIdentityHash !== observedBinding.worktreeIdentityHash || bindingFact.objectFormat !== observedBinding.objectFormat || bindingFact.gitConfigHash !== observedBinding.configHash || bindingFact.gitVersionHash !== canonicalHash(observedBinding.gitVersion)) throw new GitIntegrationBlockedError("REPOSITORY_AUTHORITY_MISMATCH", "Receipt publication requires the exact reducer-reserved Git binding");
    const landingFact: any = await this.store.readImmutableFact(attempt.landingObservationFactHash!);
    const privateFacts: any[] = [];
    for (const hash of attempt.privateRefFactHashes) { signal?.throwIfAborted?.(); privateFacts.push(await this.store.readImmutableFact(hash)); }
    const refs = Object.fromEntries(privateFacts.map((fact) => [privateRole(fact.targetRef), fact.targetRef]));
    const receiptCore = {
      schemaVersion: 1, kind: "IntegrationReceiptV1", transactionId: attemptId, runId: state.runId, runNonce: state.runNonce,
      planHash: state.identity.planHash, authorizationSetHash: state.identity.authorizationSet.hash, ownerEpoch: state.owner.ownerEpoch,
      repositoryId: reservation.repositoryId, commonDirIdentityHash: bindingFact.commonDirIdentityHash, worktreeIdentityHash: bindingFact.worktreeIdentityHash,
      gitVersion: observedBinding.gitVersion, configHash: bindingFact.gitConfigHash, objectFormat: bindingFact.objectFormat,
      targetRef: state.repositories[reservation.repositoryId].targetRef, sourceBase: attempt.sourceBase, candidate: attempt.sourceCandidate, expectedPrefix: attempt.expectedPrefix,
      composed: attempt.intendedLandedTree, workItemId: item.workItemId, candidateGeneration: item.candidateGeneration,
      compositionProfileHash: attempt.compositionProfileHash, prefixValidationProfileHash: attempt.prefixValidationProfileHash, finalValidationProfileHash: attempt.finalValidationProfileHash,
      prefixEvidenceHashes: attempt.prefixEvidenceHashes, finalEvidenceHashes: attempt.finalEvidenceHashes,
      prefixEffectReconciliationHashes: attempt.prefixEffectReconciliationHashes, finalEffectReconciliationHashes: attempt.finalEffectReconciliationHashes, environmentClosureHash: attempt.environmentClosureHash,
      privateRefs: refs, landing: { expectedOldOid: attempt.expectedTarget.commit, newOid: attempt.intendedLandedTree!.commit, reconciliation: "applied_exact", targetObservationHash: landingFact.detailsHash }, sealedAt: state.updatedAt,
    } as const;
    const receipt = { ...receiptCore, receiptHash: canonicalHash(receiptCore) };
    const transactionFact = withHash({ kind: "git_integration_receipt", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, transactionReceiptHash: receipt.receiptHash, receipt });
    await this.publishFact(transactionFact, signal);
    const integrationFact = withHash({
      kind: "integration", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash,
      workItemId: item.workItemId, repositoryId: reservation.repositoryId, integrationAttemptId: attemptId, candidateHash: attempt.sourceCandidateHash,
      strategy: "merge_tree_one_parent", compositionProfileHash: attempt.compositionProfileHash, expectedPrefix: attempt.expectedPrefix, expectedTarget: attempt.expectedTarget,
      prefixEvidenceHashes: attempt.prefixEvidenceHashes, finalEvidenceHashes: attempt.finalEvidenceHashes,
      prefixEffectReconciliationHashes: attempt.prefixEffectReconciliationHashes, finalEffectReconciliationHashes: attempt.finalEffectReconciliationHashes, environmentClosureHash: attempt.environmentClosureHash,
      sourceBase: attempt.sourceBase, sourceCandidate: attempt.sourceCandidate, syntheticParentCommit: attempt.syntheticParentCommit,
      sourceToIntegratedLineageHash: attempt.sourceToIntegratedLineageHash, landed: attempt.intendedLandedTree, combinedStateVerified: true, reconciled: true,
      acceptingOwnerEpoch: state.owner.ownerEpoch, commonDirIdentityHash: bindingFact.commonDirIdentityHash, worktreeIdentityHash: bindingFact.worktreeIdentityHash,
      gitConfigHash: bindingFact.gitConfigHash, gitVersionHash: bindingFact.gitVersionHash, objectFormat: bindingFact.objectFormat,
      transactionReceiptHash: receipt.receiptHash, transactionReceiptFactHash: transactionFact.hash, landingObservationHash: landingFact.hash, sealedAt: state.updatedAt,
    });
    await this.publishFact(integrationFact, signal);
    await this.hit("after_receipt_facts", { integrationAttemptId: attemptId, transactionReceiptFactHash: transactionFact.hash, integrationReceiptHash: integrationFact.hash }, signal);
    const fresh = await this.assertAuthority(state, reservation.reservationId, repositoryRoot, attempt.intendedLandedTree!, signal);
    await this.mutate(fresh, "accept_integration_receipt", "observation", { integrationAttemptId: attemptId, integrationReceiptHash: integrationFact.hash, transactionReceiptHash: receipt.receiptHash, transactionReceiptFactHash: transactionFact.hash }, `accept-${attemptId}`, signal);
  }

  private async request(state: DagRunStateV1, plan: CanonicalDagPlanV1, attemptId: string, item: any, train: any, repositoryRoot: string, attempt?: any, signal?: AbortSignal): Promise<GitIntegrationRequestV1> {
    signal?.throwIfAborted?.();
    const expectedRepositoryBinding = await readRepositoryBindingIdentityV1(repositoryRoot, signal);
    if (attempt?.repositoryBindingFactHash) {
      const authority: any = await this.store.readImmutableFact(attempt.repositoryBindingFactHash);
      const exact = authority?.commonDirIdentityHash === expectedRepositoryBinding.commonDirIdentityHash
        && authority?.worktreeIdentityHash === expectedRepositoryBinding.worktreeIdentityHash
        && authority?.objectFormat === expectedRepositoryBinding.objectFormat
        && authority?.gitConfigHash === expectedRepositoryBinding.configHash
        && authority?.gitVersionHash === canonicalHash(expectedRepositoryBinding.gitVersion);
      if (!exact) throw new GitIntegrationBlockedError("REPOSITORY_AUTHORITY_MISMATCH", "Current Git binding differs from the reducer-reserved repository binding");
    }
    const trainPlan = plan.constraints.integrationTrains.find(({ repositoryId }) => repositoryId === train.repositoryId)!;
    const sourceBase = attempt?.sourceBase ?? item.candidate.base;
    const candidate = attempt?.sourceCandidate ?? item.candidate.git;
    const expectedPrefix = attempt?.expectedPrefix ?? train.acceptedPrefix;
    return {
      schemaVersion: 1, transactionId: attemptId, runId: state.runId, runNonce: state.runNonce, planHash: state.identity.planHash,
      authorizationSetHash: state.identity.authorizationSet.hash, repositoryId: train.repositoryId, repositoryRoot: await realpath(repositoryRoot),
      controlRoot: join(this.store.runDirectory, "git-control"), artifactRoot: join(this.store.runDirectory, "git-receipts"), targetRef: state.repositories[train.repositoryId].targetRef,
      sourceBase, candidate, expectedPrefix, workItemId: item.workItemId, candidateGeneration: item.candidateGeneration, planCreatedAt: plan.createdAt,
      commitSubject: `feat(dag): integrate ${item.workItemId}`, compositionProfileHash: trainPlan.compositionProfileHash,
      prefixValidationProfileHash: trainPlan.prefixValidationProfileHash, finalValidationProfileHash: trainPlan.finalValidationProfileHash,
      ownerEpoch: state.owner.ownerEpoch, expectedRepositoryBinding,
    };
  }

  private async acquireOperationLock(state: DagRunStateV1, reservationId: string, request: GitIntegrationRequestV1, binding: RepositoryBindingV1, expectedTarget: any, effectId: string, signal?: AbortSignal) {
    signal?.throwIfAborted?.();
    await mkdir(request.controlRoot, { recursive: true }); await mkdir(request.artifactRoot, { recursive: true });
    await this.assertAuthority(state, reservationId, request.repositoryRoot, expectedTarget, signal);
    const payload = { transactionId: request.transactionId, repositoryId: request.repositoryId, commonDirIdentityHash: binding.commonDirIdentityHash, ownerEpoch: state.owner.ownerEpoch };
    signal?.throwIfAborted?.();
    return acquireGitIntegrationLockV1(request, binding, operationGuard(effectId, state.owner.ownerEpoch, "acquire_lock", payload), signal);
  }

  private async readExact(delegated: DagRunStateV1, reservationId: string, signal?: AbortSignal): Promise<DagRunStateV1> {
    signal?.throwIfAborted?.();
    const fresh = await this.store.read(this.context);
    if (fresh.revision !== delegated.revision || fresh.snapshotHash !== delegated.snapshotHash) throw new Error("Delegated integration snapshot is stale");
    const matches = Object.values(fresh.scheduler.reservations).filter((candidate) => candidate.reservationId === reservationId && candidate.stage === "F8" && candidate.operationKind === "integration" && candidate.state === "active");
    if (matches.length !== 1) throw new Error("Integration driver requires one exact active F8 integration reservation");
    const supplied = delegated.scheduler.reservations[reservationId];
    if (!supplied || canonicalHash(supplied) !== canonicalHash(matches[0])) throw new Error("Delegated F8 reservation identity differs from current reducer authority");
    return fresh;
  }

  private async assertAuthority(expected: DagRunStateV1, reservationId: string, repositoryRoot: string, expectedTarget: any | null, signal?: AbortSignal): Promise<DagRunStateV1> {
    signal?.throwIfAborted?.();
    const fresh = await this.store.read(this.context);
    const reservation = fresh.scheduler.reservations[reservationId];
    if (fresh.revision !== expected.revision || fresh.snapshotHash !== expected.snapshotHash || fresh.owner.ownerEpoch !== expected.owner.ownerEpoch || fresh.owner.ownerTokenHash !== expected.owner.ownerTokenHash || fresh.identity.authorizationSet.hash !== expected.identity.authorizationSet.hash || !reservation || reservation.state !== "active" || reservation.stage !== "F8" || reservation.operationKind !== "integration" || reservation.ownerEpoch !== fresh.owner.ownerEpoch || reservation.authorizationSetHash !== fresh.identity.authorizationSet.hash) throw new Error("Integration authority changed before irreversible Git operation");
    if (expectedTarget) {
      const target = await observeTarget(repositoryRoot, fresh.repositories[reservation.repositoryId].targetRef, signal);
      if (!sameTree(target, expectedTarget)) throw new GitIntegrationBlockedError("TARGET_DRIFT", "Target changed before irreversible Git operation", { observed: target, expected: expectedTarget });
    }
    return fresh;
  }

  private async publishFact(fact: any, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted?.();
    const stored = await this.store.putImmutableFact(fact, signal);
    if (stored.hash !== fact.hash) throw new Error("Published integration fact does not bind its exact self-hash");
    signal?.throwIfAborted?.();
    (this.context.facts as Record<string, any>)[fact.hash] = fact;
  }

  private async mutate(state: DagRunStateV1, type: string, kind: "command" | "observation", payload: any, slot: string, signal?: AbortSignal): Promise<DagRunStateV1> {
    signal?.throwIfAborted?.();
    const input: DagRunInputV1 = { schemaVersion: 1, kind, type, commandId: slot, idempotencyKey: `${state.runNonce}:${slot}`, payloadHash: canonicalHash(payload), runId: state.runId, runNonce: state.runNonce, expectedRevision: state.revision, expectedSnapshotHash: state.snapshotHash, ownerEpoch: state.owner.ownerEpoch, occurredAt: state.updatedAt, payload } as DagRunInputV1;
    const result = await this.store.mutate({ input, context: this.context, lock: this.lock, signal });
    if (!result.accepted) throw new Error(`Integration ${type} rejected: ${result.code}: ${result.message}`);
    return result.state;
  }

  private async hit(point: DagGitIntegrationDriverFailpointV1, context: Record<string, unknown>, signal?: AbortSignal): Promise<void> { signal?.throwIfAborted?.(); await this.failpoint?.(point, context); signal?.throwIfAborted?.(); }
}

function effectIntent(state: DagRunStateV1, effectId: string, kind: string, subject: { kind: "train" | "repository"; id: string }, requestHash: string, candidateGeneration: number, gateEpochHash: string, at: string, executionRequest?: Record<string, unknown>, boundIntegrationAttemptId?: string): any {
  return { effectId, kind, subject, ...(executionRequest ? { boundStageAttemptId: null, boundIntegrationAttemptId: boundIntegrationAttemptId ?? null, boundWorkerResultHash: null, executionRequest, executionObservationHash: null } : {}), effectScopeId: null, effectScopeKind: null, provider: null, procedureClass: "idempotent", requestHash, boundOwnerEpoch: state.owner.ownerEpoch, boundAuthorizationSetHash: state.identity.authorizationSet.hash, boundFreshnessReceiptHash: state.freshness.receipt.hash, boundCandidateGeneration: candidateGeneration, boundGateEpochHash: gateEpochHash, state: "intended", dispatchCount: 0, createdRevision: state.revision + 1, createdAt: at, lastDispatchAt: null, observationHash: null, reconciliation: "not_started", blockerId: null };
}

function gitFact(state: DagRunStateV1, input: any): any {
  const binding = input.binding;
  const core = { kind: "git_transaction", factType: input.factType, planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, authorizationSetHash: state.identity.authorizationSet.hash, repositoryId: input.repositoryId, integrationAttemptId: input.integrationAttemptId, effectId: input.effectId, requestHash: input.requestHash, ownerEpoch: state.owner.ownerEpoch, commonDirIdentityHash: binding.commonDirIdentityHash, worktreeIdentityHash: binding.worktreeIdentityHash, gitConfigHash: binding.configHash, gitVersionHash: canonicalHash(binding.gitVersion), objectFormat: binding.objectFormat, targetRef: input.targetRef, commit: input.commit, tree: input.tree, parentCommit: input.parentCommit, reconciliation: input.reconciliation, detailsHash: input.detailsHash, observedAt: input.observedAt };
  return withHash(core);
}

async function executeValidationProfile(
  profile: IntegrationValidationProfileMappingV1,
  phase: { phase: "prefix" | "final"; profileId: string; profileHash: string },
  state: DagRunStateV1,
  trainId: string,
  repositoryId: string,
  integrationAttemptId: string,
  tree: { repositoryId: string; commit: string; tree: string },
  binding: RepositoryBindingV1,
  controlRoot: string,
  repositoryRoot: string,
  now: () => string,
  effect: any,
  abortSignal?: AbortSignal,
): Promise<any> {
  abortSignal?.throwIfAborted?.();
  if (profile.cwdMode !== "detached_proposal_worktree" || profile.readOnly !== true || profile.noEdit !== true || profile.environmentHash !== canonicalHash(profile.environment) || profile.environmentProfileHash !== canonicalHash({ profileId: profile.environmentProfileId, environment: profile.environment }) || !profile.argv.length || !profile.argv[0].startsWith("/")) throw new GitIntegrationBlockedError("VALIDATION_PROFILE_INVALID", "Validation profile is not an exact closed read-only argv mapping");
  const executable = await realpath(profile.argv[0]);
  if (!await validationExecutableIdentityMatchesV1(profile, abortSignal)) throw new GitIntegrationBlockedError("VALIDATION_EXECUTABLE_MISMATCH", "Validation executable or absolute argv artifact bytes differ from the plan-bound mapping");
  const worktreeRoot = join(controlRoot, "proposal-worktrees", digest({ integrationAttemptId, phase: phase.phase, profileHash: phase.profileHash }));
  await mkdir(join(controlRoot, "proposal-worktrees"), { recursive: true });
  let materialized = false;
  try {
    try {
      const existing = await stat(worktreeRoot);
      if (!existing.isDirectory()) throw new GitIntegrationBlockedError("VALIDATION_WORKTREE_CONFLICT", "Proposal worktree path is not a directory");
      const existingCommit = await git(worktreeRoot, ["rev-parse", "HEAD"], [0], abortSignal);
      if (existingCommit !== tree.commit) throw new GitIntegrationBlockedError("VALIDATION_WORKTREE_CONFLICT", "Existing proposal worktree has a different commit");
      materialized = true;
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      await git(repositoryRoot, ["worktree", "add", "--detach", worktreeRoot, tree.commit], [0], abortSignal);
      materialized = true;
    }
    const identity = await stat(worktreeRoot); const canonicalWorktree = await realpath(worktreeRoot);
    const before = await validationWorktreeIdentity(worktreeRoot, abortSignal);
    if (before.status !== "" || before.commit !== tree.commit || before.tree !== tree.tree) throw new GitIntegrationBlockedError("VALIDATION_WORKTREE_DIRTY", "Detached proposal worktree is not the exact clean composed tree");
    const startedAt = now();
    let stdout = ""; let stderr = ""; let exitCode: number | null = 0; let signal: string | null = null;
    try {
      const result = await run(executable, profile.argv.slice(1), { cwd: worktreeRoot, env: { ...profile.environment }, timeout: profile.timeoutMs, maxBuffer: MAX_VALIDATION_OUTPUT, encoding: "utf8", windowsHide: true, signal: abortSignal });
      stdout = result.stdout; stderr = result.stderr;
    } catch (error: any) {
      if (abortSignal?.aborted || error?.name === "AbortError" || error?.code === "ABORT_ERR") throw error;
      stdout = String(error?.stdout ?? ""); stderr = String(error?.stderr ?? "");
      exitCode = Number.isInteger(error?.code) && error.code >= 0 ? error.code : null;
      signal = typeof error?.signal === "string" ? error.signal : null;
      if (Buffer.byteLength(stdout) + Buffer.byteLength(stderr) > MAX_VALIDATION_OUTPUT) { stdout = stdout.slice(0, MAX_VALIDATION_OUTPUT / 2); stderr = stderr.slice(0, MAX_VALIDATION_OUTPUT / 2); }
    }
    abortSignal?.throwIfAborted?.();
    const completedAt = now();
    const after = await validationWorktreeIdentity(worktreeRoot, abortSignal);
    if (canonicalHash(after) !== canonicalHash(before)) throw new GitIntegrationBlockedError("VALIDATION_NO_EDIT_VIOLATION", "Validation command changed the exact detached proposal worktree");
    let parsed: any = null; let parserDisposition: "PASS" | "FAIL" | "BLOCKED" = "BLOCKED";
    try {
      parsed = parseStrictJson(stdout);
      const keys = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? Object.keys(parsed).sort() : [];
      if (JSON.stringify(keys) === JSON.stringify(["disposition"]) && ["PASS", "FAIL", "BLOCKED"].includes(parsed.disposition)) parserDisposition = parsed.disposition;
    } catch {}
    const disposition = exitCode === 0 && signal === null && parserDisposition === "PASS" ? "PASS" : parserDisposition === "FAIL" ? "FAIL" : "BLOCKED";
    const outputBytes = Buffer.byteLength(stdout) + Buffer.byteLength(stderr);
    const core = {
      kind: "verification" as const, planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce,
      authorizationSetHash: effect.boundAuthorizationSetHash, ownerEpoch: effect.boundOwnerEpoch, freshnessReceiptHash: effect.boundFreshnessReceiptHash,
      effectId: effect.effectId, requestHash: effect.requestHash, requestIdentityHash: effect.requestHash, repositoryId, trainId, integrationAttemptId,
      phase: phase.phase, profileId: phase.profileId, profileHash: phase.profileHash,
      executableArtifactHash: profile.executableArtifactHash, argvHash: canonicalHash(profile.argv), cwdMode: profile.cwdMode,
      environmentProfileId: profile.environmentProfileId, environmentProfileHash: profile.environmentProfileHash,
      environmentHash: profile.environmentHash, timeoutMs: profile.timeoutMs, readOnly: true as const, noEdit: true as const,
      tree, commonDirIdentityHash: binding.commonDirIdentityHash,
      worktreeIdentityHash: canonicalHash({ realPath: canonicalWorktree, dev: String(identity.dev), ino: String(identity.ino) }),
      objectFormat: binding.objectFormat, executionId: `verify-${digest({ integrationAttemptId, phase: phase.phase, profileHash: phase.profileHash }).slice(0, 32)}`,
      exitCode, signal, outputHash: canonicalHash({ stdout, stderr, exitCode, signal }), stdoutHash: canonicalHash(stdout), stderrHash: canonicalHash(stderr), outputBytes,
      parser: "strict-json-disposition-v1" as const, parserDisposition, parsedResultHash: parsed && parserDisposition !== "BLOCKED" ? canonicalHash(parsed) : null,
      startedAt, completedAt, disposition,
    };
    return withHash(core);
  } finally {
    if (materialized) await git(repositoryRoot, ["worktree", "remove", "--force", worktreeRoot]).catch(async () => { await rm(worktreeRoot, { recursive: true, force: true }); throw new GitIntegrationBlockedError("VALIDATION_WORKTREE_CLEANUP_FAILED", "Detached proposal worktree cleanup could not be reconciled exactly"); });
  }
}

async function validationWorktreeIdentity(cwd: string, signal?: AbortSignal): Promise<{ status: string; commit: string; tree: string }> {
  const status = await git(cwd, ["status", "--porcelain=v2", "--untracked-files=all"], [0], signal);
  const commit = await git(cwd, ["rev-parse", "HEAD"], [0], signal); const tree = await git(cwd, ["rev-parse", "HEAD^{tree}"], [0], signal);
  return { status, commit, tree };
}

function withHash<T extends Record<string, unknown>>(core: T): T & { hash: string } { return { ...core, hash: canonicalHash(core) }; }
function opaqueId(prefix: string, value: unknown): string { return `${prefix}-${canonicalHash(value).slice(HASH_PREFIX.length, HASH_PREFIX.length + 32)}`; }
function digest(value: unknown): string { return canonicalHash(value).slice(HASH_PREFIX.length); }
function privateRefs(request: GitIntegrationRequestV1): Record<"baseline" | "candidate" | "prefix" | "composed" | "proposal", string> {
  const base = `refs/pi-dag/v1/transactions/${digest({ planHash: request.planHash, runId: request.runId, runNonce: request.runNonce, repositoryId: request.repositoryId })}`;
  return { baseline: `${base}/baseline`, candidate: `${base}/candidates/${digest(request.workItemId)}/g${request.candidateGeneration}`, prefix: `${base}/transactions/${digest(request.transactionId)}/prefix`, composed: `${base}/transactions/${digest(request.transactionId)}/composed`, proposal: `${base}/transactions/${digest(request.transactionId)}/proposal` };
}
function privateRole(ref: string): string { if (ref.endsWith("/baseline")) return "baseline"; if (ref.includes("/candidates/")) return "candidate"; if (ref.endsWith("/prefix")) return "prefix"; if (ref.endsWith("/composed")) return "composed"; if (ref.endsWith("/proposal")) return "proposal"; throw new Error("Unknown protected private ref role"); }
function operationGuard(effectId: string, ownerEpoch: number, kind: string, payload: unknown): { effectId: string; requestHash: string; ownerEpoch: number } { return { effectId, ownerEpoch, requestHash: canonicalHash({ kind, payload }) }; }
function sameTree(left: any, right: any): boolean { return left?.commit === right?.commit && left?.tree === right?.tree; }
async function observeTarget(repositoryRoot: string, targetRef: string, signal?: AbortSignal): Promise<{ commit: string; tree: string | null }> { signal?.throwIfAborted?.(); const commit = await git(repositoryRoot, ["show-ref", "--verify", "--hash", targetRef], [0, 1, 2, 128], signal); return commit ? { commit, tree: await git(repositoryRoot, ["rev-parse", `${commit}^{tree}`], [0], signal) } : { commit: "missing", tree: null }; }
export async function validationExecutableIdentityMatchesV1(profile: IntegrationValidationProfileMappingV1, signal?: AbortSignal): Promise<boolean> {
  signal?.throwIfAborted?.();
  let executableArtifactHash: string;
  try {
    const executable = await realpath(profile.argv[0]);
    executableArtifactHash = `${HASH_PREFIX}${createHash("sha256").update(await readFile(executable)).digest("hex")}`;
  } catch (error) { signal?.throwIfAborted?.(); return false; }
  signal?.throwIfAborted?.();
  if (executableArtifactHash === profile.executableArtifactHash) return true;
  const argvArtifacts: Array<{ index: number; hash: string }> = [];
  for (let index = 1; index < profile.argv.length; index += 1) {
    signal?.throwIfAborted?.();
    if (!profile.argv[index].startsWith("/")) continue;
    try {
      const artifact = await realpath(profile.argv[index]);
      argvArtifacts.push({ index, hash: `${HASH_PREFIX}${createHash("sha256").update(await readFile(artifact)).digest("hex")}` });
    } catch {
      signal?.throwIfAborted?.();
      // A non-file absolute argument remains ordinary argv and contributes through the profile hash.
    }
  }
  signal?.throwIfAborted?.();
  return argvArtifacts.length > 0 && canonicalHash({ executableHash: executableArtifactHash, argvArtifacts }) === profile.executableArtifactHash;
}

async function git(cwd: string, args: string[], allowExit: number[] = [0], signal?: AbortSignal): Promise<string> { signal?.throwIfAborted?.(); try { const result = await run("git", args, { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, env: { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" }, signal }); return result.stdout.trim(); } catch (error: any) { if (allowExit.includes(error?.code)) return String(error?.stdout ?? "").trim(); throw error; } }
