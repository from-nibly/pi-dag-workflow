import { mkdir, lstat, realpath } from "node:fs/promises";
import { join } from "node:path";
import { canonicalHash, parseStrictJson } from "../dag-runtime/common.ts";
import { requireV2, sameV2 } from "../planning/v2.ts";
import { bindGitV2, eligibleGitV2, nativeGitV2, assertGitAttributesV2 } from "./git-native.ts";
import type { ReservationV2, WorkerBindingV2 } from "./state.ts";
import type { WorkersV2 } from "./service.ts";
import type { CommandRunnerV2 } from "./command-runner.ts";

/** WorkerManager is the sole process owner. The request, not mutable host model/tool
 * context, determines keyed replay; its exact durable attempt is retained by V2. */
export class ProductWorkersV2 implements WorkersV2 {
  readonly manager: any;
  readonly root: string;
  readonly inspector: CommandRunnerV2;
  constructor(manager: any, root: string, inspector: CommandRunnerV2) { this.manager = manager; this.root = root; this.inspector = inspector; }
  async ensure(reservation: Readonly<ReservationV2>) {
    requireV2(this.manager.context?.cwd === this.root, "WORKER_MANAGER_ROOT_MISMATCH");
    const request = parseStrictJson(reservation.request) as any;
    requireV2(request?.kind === "product_worker_v2" && request.explicitDispatchRecovery === true && typeof request.task === "string" && /^[0-9a-f]{40}([0-9a-f]{24})?$/.test(request.baseCommit), "INVALID_PRODUCT_WORKER_REQUEST");
    const id = `v2-${canonicalHash(reservation.operationId).slice(7)}`, cwd = join(this.root, ".ai", "worker-roots", id);
    // Materialize with the same native profile as verification, before the generic
    // manager validates and takes ownership. Never invoke repository hooks here.
    const binding = await bindGitV2(this.root);
    requireV2(`repo-${canonicalHash(binding).slice(7)}` === request.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    await eligibleGitV2(binding, [{ commit: request.baseCommit, tree: nativeGitV2(this.root, "rev-parse", `${request.baseCommit}^{tree}`) }]);
    try { await lstat(cwd); } catch (error: any) {
      if (error.code !== "ENOENT") throw error;
      await mkdir(join(this.root, ".ai", "worker-roots"), { recursive: true });
      nativeGitV2(this.root, "worktree", "add", "--detach", cwd, request.baseCommit);
    }
    const exact = await this.manager.launchOwnedAttempt({ workerId: id, launchKey: reservation.operationId, expectedAttemptNumber: 1,
      configRequestHash: canonicalHash(request), explicitDispatchRecovery: true, baseCommit: request.baseCommit, worktreeKey: id, label: id, task: request.task }, this.manager.context);
    const retained: WorkerBindingV2 = { workerStorageId: exact.workerStorageId, launchOwnerSessionId: exact.launchOwnerSessionId, workerId: exact.workerId,
      attemptNumber: exact.attemptNumber, attemptNonce: exact.attemptNonce, configHash: exact.configHash };
    requireV2(retained.workerId === id && retained.attemptNumber === 1, "WORKER_ATTEMPT_CONFLICT");
    return { workerId: id, binding: retained };
  }
  async recoverBinding(reservation: Readonly<ReservationV2>): Promise<WorkerBindingV2> {
    const request = parseStrictJson(reservation.request), id = `v2-${canonicalHash(reservation.operationId).slice(7)}`;
    const exact = await this.manager.attemptIdentityByLaunchKey(reservation.operationId);
    requireV2(exact && exact.workerId === id && exact.attemptNumber === 1, "EXACT_EXISTING_ATTEMPT_REQUIRED");
    const value: WorkerBindingV2 = { workerStorageId: exact.workerStorageId, launchOwnerSessionId: exact.launchOwnerSessionId, workerId: exact.workerId, attemptNumber: exact.attemptNumber, attemptNonce: exact.attemptNonce, configHash: exact.configHash };
    const observed = await this.manager.inspectBindingReadOnly(value);
    requireV2(observed.worker.currentAttempt === 1 && observed.worker.normalizedRequest.explicitDispatchRecovery === true && observed.worker.normalizedRequest.boundConfigRequestHash === canonicalHash(request), "IMMUTABLE_WORKER_REQUEST_MISMATCH");
    requireV2(observed.attempt.dispatchClaimedAt || observed.attempt.ingestedAt, "DISPATCH_NOT_YET_ATTEMPTED: recover with dag_start_work while active, or cancel while fenced");
    return value;
  }
  async terminal(binding: Readonly<WorkerBindingV2>, reconcile = false) {
    return this.manager.terminalResultForBinding(binding, { reconcile });
  }
  async settled(reservation: Readonly<ReservationV2>) {
    if (reservation.state === "reserved") return;
    if (!reservation.binding && (await this.manager.cancelUnlaunchedByLaunchKey(reservation.operationId, canonicalHash(parseStrictJson(reservation.request)))).settled) return;
    const binding = reservation.binding ?? await this.recoverBinding(reservation);
    const terminal = await this.terminal(binding, true);
    requireV2(terminal, "WORKER_SETTLEMENT_REQUIRED");
    const exact = await this.manager.inspectBindingReadOnly(binding);
    requireV2(exact.attempt.ingestedAt && exact.attempt.resultPath && exact.worker.currentAttempt === binding.attemptNumber, "WORKER_RESULT_NOT_CURRENT_INGESTED");
  }
  async candidate(reservation: Readonly<ReservationV2>, completionId: string) {
    requireV2(reservation.binding && reservation.completion?.completionId === completionId && reservation.completion.terminalStatus === "succeeded", "SUCCESSFUL_EXACT_COMPLETION_REQUIRED");
    const terminal = await this.terminal(reservation.binding);
    requireV2(sameV2(terminal, reservation.completion), "WORKER_COMPLETION_CHANGED");
    const exact = await this.manager.inspectBindingReadOnly(reservation.binding), cwd = exact.worker.cwd;
    const workingRoot = exact.worker.normalizedRequest?.workingRoot, stat = await lstat(cwd);
    requireV2(workingRoot && await realpath(cwd) === workingRoot.realPath && !stat.isSymbolicLink() && stat.isDirectory() && String(stat.dev) === workingRoot.dev && String(stat.ino) === workingRoot.ino, "WORKER_WORKTREE_IDENTITY_DRIFT");
    requireV2(exact.attempt.ingestedAt && exact.attempt.resultPath && exact.worker.currentAttempt === reservation.binding.attemptNumber, "WORKER_ATTEMPT_NOT_CURRENT_SETTLED");
    assertGitAttributesV2(cwd);
    requireV2(!nativeGitV2(cwd, "status", "--porcelain=v2", "--untracked-files=all"), "WORKER_CANDIDATE_DIRTY");
    requireV2(nativeGitV2(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir") === nativeGitV2(this.root, "rev-parse", "--path-format=absolute", "--git-common-dir"), "WORKER_REPOSITORY_MISMATCH");
    requireV2(nativeGitV2(cwd, "rev-parse", "--abbrev-ref", "HEAD") === "HEAD", "WORKER_MUST_REMAIN_DETACHED");
    const candidate = { commit: nativeGitV2(cwd, "rev-parse", "HEAD^{commit}"), tree: nativeGitV2(cwd, "rev-parse", "HEAD^{tree}") };
    const request = parseStrictJson(reservation.request) as any;
    nativeGitV2(cwd, "merge-base", "--is-ancestor", request.baseCommit, candidate.commit);
    requireV2(Array.isArray(request.sourcePaths) && request.sourcePaths.includes("project-model/model.json") && request.sourcePaths.every((p: unknown) => typeof p === "string"), "FROZEN_SOURCE_PATHS_REQUIRED");
    requireV2(!nativeGitV2(cwd, "diff", "--no-ext-diff", "--no-textconv", "--name-only", request.baseCommit, candidate.commit, "--", ...request.sourcePaths.map((p: string) => `:(literal)${p}`)), "WORKER_CHANGED_FROZEN_MODEL_OR_SPEC: use model direction and a successor plan");
    const binding = await bindGitV2(this.root);
    requireV2(`repo-${canonicalHash(binding).slice(7)}` === request.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    await eligibleGitV2(binding, [candidate]);
    await this.inspector.inspectCleanWorkspace(candidate, cwd);
    return candidate;
  }
  async cancel(reservation: Readonly<ReservationV2>) {
    if (reservation.state === "reserved") return;
    // A dispatching operation must be recovered by immutable key, never replaced.
    // ensure is not used here: cancellation must not launch an unlaunched intent.
    if (!reservation.binding && (await this.manager.cancelUnlaunchedByLaunchKey(reservation.operationId, canonicalHash(parseStrictJson(reservation.request)))).settled) return;
    const binding = reservation.binding ?? await this.recoverBinding(reservation);
    await this.manager.cancelBinding(binding, "V2 run generation fenced");
  }
}
