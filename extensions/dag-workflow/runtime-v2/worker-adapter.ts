import { mkdir, lstat, realpath } from "node:fs/promises";
import { join, basename } from "node:path";
import { withWorkspaceOwnership, inspectWorkspaceRoot, inspectNodeWorkspaceBinding } from "../worker-runtime/workspace-ownership.mjs";
import { canonicalHash, parseStrictJson } from "../dag-runtime/common.ts";
import { requireV2, sameV2 } from "../planning/v2.ts";
import { bindGitV2, eligibleGitV2, nativeGitV2, assertGitAttributesV2 } from "./git-native.ts";
import type { ReservationV2, WorkerBindingV2, NodeWorkspaceBindingV2 } from "./state.ts";
import type { WorkersV2 } from "./service.ts";
import type { CommandRunnerV2 } from "./command-runner.ts";

/** WorkerManager is the sole process owner. The request, not mutable host model/tool
 * context, determines keyed replay; its exact durable attempt is retained by V2. */
export class ProductWorkersV2 implements WorkersV2 {
  readonly manager: any;
  readonly root: string;
  readonly inspector: CommandRunnerV2;
  constructor(manager: any, root: string, inspector: CommandRunnerV2) { this.manager = manager; this.root = root; this.inspector = inspector; }
  async prepareWorkspace(reservation: Readonly<ReservationV2>, workspace?: NodeWorkspaceBindingV2): Promise<NodeWorkspaceBindingV2> {
    requireV2(this.manager.context?.cwd === this.root, "WORKER_MANAGER_ROOT_MISMATCH");
    const request = parseStrictJson(reservation.request) as any;
    requireV2(request?.kind === "product_worker_v2" && request.explicitDispatchRecovery === true && typeof request.task === "string" && /^[0-9a-f]{40}([0-9a-f]{24})?$/.test(request.baseCommit), "INVALID_PRODUCT_WORKER_REQUEST");
    const nodeId = `${reservation.runId}/${reservation.itemId}`;
    const id = `v2-${canonicalHash(request.workspaceProtocol === "node-owned-v1" ? nodeId : reservation.operationId).slice(7)}`;
    const cwd = workspace?.cwd ?? request.nodeWorkspace?.cwd ?? join(this.root, ".ai", "worker-roots", id);
    requireV2(!workspace || workspace.nodeId === nodeId, "NODE_WORKSPACE_BINDING_MISMATCH");
    requireV2(!request.nodeWorkspace || request.nodeWorkspace.cwd === cwd && request.nodeWorkspace.nodeId === nodeId, "NODE_WORKSPACE_BINDING_MISMATCH");
    if (request.nodeWorkspace) await this.inspector.reconcileNodeWorkspace(cwd);
    if (request.nodeWorkspace) await withWorkspaceOwnership(this.root, cwd, async (ownership: any, publish: any) => {
      requireV2(ownership && ownership.nodeId === request.nodeWorkspace.nodeId && ownership.epoch === request.nodeWorkspace.epoch
        && ownership.execution === null && (ownership.launchKey === null || ownership.launchKey === reservation.operationId), "NODE_WORKSPACE_HANDOFF_CONFLICT");
      requireV2(sameV2(ownership.root, await inspectWorkspaceRoot(cwd)), "WORKSPACE_IDENTITY_DRIFT");
      const observed = await inspectNodeWorkspaceBinding(cwd, nodeId);
      requireV2((!workspace || sameV2(workspace, observed)) && (!ownership.workspace || sameV2(ownership.workspace, observed)), "NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT");
      const borrowedRequestHash = request.workspaceProtocol === "node-owned-v1" ? canonicalHash(request) : undefined;
      requireV2(!ownership.borrowedRequestHash || ownership.borrowedRequestHash === borrowedRequestHash, "NODE_WORKSPACE_REQUEST_CONFLICT");
      if (!ownership.launchKey) {
        await this.inspector.inspectCleanWorkspace({ commit: request.baseCommit, tree: nativeGitV2(cwd, "rev-parse", `${request.baseCommit}^{tree}`) }, cwd);
        ownership.launchKey = reservation.operationId;
        ownership.workspace ??= observed;
        if (borrowedRequestHash) ownership.borrowedRequestHash = borrowedRequestHash;
        await publish(ownership);
      }
    });
    if (request.nodeWorkspace) await this.manager.options.failpoint?.("after_node_workspace_launch_claim", { cwd, operationId: reservation.operationId, requestHash: canonicalHash(request) });
    // Materialize with the same native profile as verification, before the generic
    // manager validates the launch. Directory ownership stays with the node.
    // Never invoke repository hooks here.
    const binding = await bindGitV2(this.root);
    requireV2(`repo-${canonicalHash(binding).slice(7)}` === request.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    await eligibleGitV2(binding, [{ commit: request.baseCommit, tree: nativeGitV2(this.root, "rev-parse", `${request.baseCommit}^{tree}`) }]);
    if (!request.nodeWorkspace) await withWorkspaceOwnership(this.root, cwd, async (ownership: any, publish: any) => {
      const nodeId = reservation.operationId.slice(0, reservation.operationId.lastIndexOf("/"));
      if (ownership) {
        requireV2(ownership.nodeId === nodeId && (ownership.launchKey === reservation.operationId || ownership.launchKey === null && ownership.binding?.workerId === id), "NODE_WORKSPACE_HANDOFF_CONFLICT");
        return;
      }
      requireV2(!workspace, "NODE_WORKSPACE_OWNERSHIP_MISSING");
      await this.manager.assertNodeWorkspaceAvailable(cwd);
      try { await lstat(cwd); } catch (error: any) {
        if (error.code !== "ENOENT") throw error;
        await mkdir(join(this.root, ".ai", "worker-roots"), { recursive: true });
        nativeGitV2(this.root, "worktree", "add", "--detach", cwd, request.baseCommit);
        await this.manager.options.failpoint?.("after_node_workspace_materialization", { cwd, nodeId });
      }
      requireV2(nativeGitV2(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir") === binding.common.path, "WORKER_REPOSITORY_MISMATCH");
      await this.inspector.inspectCleanWorkspace({ commit: request.baseCommit, tree: nativeGitV2(cwd, "rev-parse", `${request.baseCommit}^{tree}`) }, cwd);
      const allocated = await inspectNodeWorkspaceBinding(cwd, nodeId);
      await publish({ version: 1, repository: this.root, cwd, root: allocated.identity.root, nodeId, epoch: 1,
        binding: null, launchKey: reservation.operationId, execution: null, handoffs: [], workspace: allocated,
        ...(request.workspaceProtocol === "node-owned-v1" ? { borrowedRequestHash: canonicalHash(request) } : {}) });
    });
    return withWorkspaceOwnership(this.root, cwd, async (ownership: any, publish: any) => {
      requireV2(ownership?.nodeId === nodeId, "NODE_WORKSPACE_BINDING_MISMATCH");
      const observed = await inspectNodeWorkspaceBinding(cwd, nodeId);
      requireV2((!workspace || sameV2(workspace, observed)) && (!ownership.workspace || sameV2(ownership.workspace, observed)), "NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT");
      requireV2(!ownership.borrowedRequestHash || ownership.borrowedRequestHash === canonicalHash(request), "NODE_WORKSPACE_REQUEST_CONFLICT");
      const borrowedRequestHash = request.workspaceProtocol === "node-owned-v1" ? canonicalHash(request) : undefined;
      if (!ownership.workspace || ownership.borrowedRequestHash !== borrowedRequestHash) {
        ownership.workspace = observed;
        if (borrowedRequestHash) ownership.borrowedRequestHash = borrowedRequestHash;
        await publish(ownership);
      }
      await this.manager.options.failpoint?.("after_node_workspace_publication", { cwd, nodeId });
      return ownership.workspace;
    });
  }
  async ensure(reservation: Readonly<ReservationV2>, workspace?: NodeWorkspaceBindingV2) {
    const request = parseStrictJson(reservation.request) as any;
    // The runtime publishes this immutable node receipt before the manager may
    // reserve/spawn a worker. Historical requests keep their approval protocol.
    requireV2(workspace || request.workspaceProtocol !== "node-owned-v1", "DURABLE_NODE_WORKSPACE_REQUIRED");
    const id = `v2-${canonicalHash(reservation.operationId).slice(7)}`;
    const cwd = workspace?.cwd ?? request.nodeWorkspace?.cwd ?? join(this.root, ".ai", "worker-roots", id);
    const capability = request.workspaceProtocol === "node-owned-v1" ? await withWorkspaceOwnership(this.root, cwd, async (ownership: any) => {
      requireV2(sameV2(ownership?.workspace, workspace) && ownership.launchKey === reservation.operationId && ownership.execution === null, "NODE_WORKSPACE_HANDOFF_CONFLICT");
      return { workspace, epoch: ownership.epoch, requestHash: canonicalHash(request) };
    }) : undefined;
    const exact = await this.manager.launchOwnedAttempt({ workerId: id, launchKey: reservation.operationId, expectedAttemptNumber: 1,
      configRequestHash: canonicalHash(request), explicitDispatchRecovery: true, baseCommit: request.baseCommit, worktreeKey: basename(cwd), label: id, task: request.task,
      ...(capability ? { nodeWorkspace: capability } : {}) }, this.manager.context);
    const retained: WorkerBindingV2 = { workerStorageId: exact.workerStorageId, launchOwnerSessionId: exact.launchOwnerSessionId, workerId: exact.workerId,
      attemptNumber: exact.attemptNumber, attemptNonce: exact.attemptNonce, configHash: exact.configHash };
    requireV2(retained.workerId === id && retained.attemptNumber === 1, "WORKER_ATTEMPT_CONFLICT");
    return { workerId: id, binding: retained };
  }
  async recoverBinding(reservation: Readonly<ReservationV2>, workspace?: NodeWorkspaceBindingV2): Promise<WorkerBindingV2> {
    const request = parseStrictJson(reservation.request), id = `v2-${canonicalHash(reservation.operationId).slice(7)}`;
    const exact = await this.manager.attemptIdentityByLaunchKey(reservation.operationId);
    requireV2(exact, "EXACT_EXISTING_ATTEMPT_REQUIRED: no durable attempt exists; a prelaunch failure needs dag_start_work for this same generation while active, not replacement or acknowledgement recovery");
    requireV2(exact.workerId === id && exact.attemptNumber === 1, "EXACT_EXISTING_ATTEMPT_REQUIRED");
    const value: WorkerBindingV2 = { workerStorageId: exact.workerStorageId, launchOwnerSessionId: exact.launchOwnerSessionId, workerId: exact.workerId, attemptNumber: exact.attemptNumber, attemptNonce: exact.attemptNonce, configHash: exact.configHash };
    const observed = await this.manager.inspectBindingReadOnly(value);
    if (workspace) {
      requireV2(workspace.cwd === observed.worker.cwd && workspace.nodeId === `${reservation.runId}/${reservation.itemId}`
        && sameV2(workspace, await inspectNodeWorkspaceBinding(workspace.cwd, workspace.nodeId)), "NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT");
      await withWorkspaceOwnership(this.root, workspace.cwd, async (owner: any) => {
        requireV2(sameV2(owner?.workspace, workspace), "NODE_WORKSPACE_BINDING_MISMATCH");
      });
      if (observed.worker.normalizedRequest.workingRoot.kind === "borrowed_node")
        requireV2(sameV2(observed.worker.normalizedRequest.workingRoot.workspace, workspace), "NODE_WORKSPACE_BINDING_MISMATCH");
    }
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
    requireV2(exact.worker.launchKey === reservation.operationId && exact.worker.normalizedRequest.boundConfigRequestHash === canonicalHash(parseStrictJson(reservation.request)), "IMMUTABLE_WORKER_REQUEST_MISMATCH");
  }
  async evidence(reservation: Readonly<ReservationV2>) {
    requireV2(reservation.binding, "EXACT_WORKER_BINDING_REQUIRED");
    await this.settled(reservation);
    const evidence = await this.manager.terminalResultForBinding(reservation.binding, { evidence: true });
    requireV2(evidence && reservation.completion && evidence.completionId === reservation.completion.completionId && evidence.terminalStatus === reservation.completion.terminalStatus, "EXACT_WORKER_COMPLETION_REQUIRED");
    // A native ESM manager retained across extension reload can still return the
    // old identity-only projection. Never render it as if full evidence was read.
    requireV2(typeof evidence.resultPath === "string" && evidence.resultPath.length > 0 && /^sha256:[0-9a-f]{64}$/.test(evidence.resultHash)
      && Object.hasOwn(evidence, "report") && Object.hasOwn(evidence, "reportStatus") && Array.isArray(evidence.artifacts)
      && Object.hasOwn(evidence, "runtime") && Object.hasOwn(evidence, "process"),
    "WORKER_EVIDENCE_PROJECTION_REQUIRED: manager did not return full terminal evidence; restart Pi preserving this session and run binding if the extension was updated in-process");
    return evidence;
  }
  async candidate(reservation: Readonly<ReservationV2>, completionId: string, repair = false, observe?: (value: unknown) => void) {
    const identity = await this.candidateIdentity(reservation, completionId, repair);
    return this.candidateArtifact(identity, observe);
  }
  async workspace(reservation: Readonly<ReservationV2>, workspace?: NodeWorkspaceBindingV2, retain?: (binding: NodeWorkspaceBindingV2) => Promise<void>) {
    await this.settled(reservation);
    requireV2(reservation.binding, "EXACT_WORKER_BINDING_REQUIRED");
    const nodeId = reservation.operationId.slice(0, reservation.operationId.lastIndexOf("/"));
    const ownership = await this.manager.relinquishNodeWorkspace(reservation.binding, nodeId);
    await this.inspector.reconcileNodeWorkspace(ownership.cwd);
    const binding = await withWorkspaceOwnership(this.root, ownership.cwd, async (current: any, publish: any) => {
      const observed = await inspectNodeWorkspaceBinding(current.cwd, nodeId);
      requireV2((!workspace || sameV2(workspace, observed)) && (!current.workspace || sameV2(current.workspace, observed)), "NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT");
      if (!current.workspace) { current.workspace = observed; await publish(current); }
      return current.workspace;
    });
    await retain?.(binding);
    return { cwd: ownership.cwd, nodeId, epoch: ownership.epoch };
  }
  async candidateIdentity(reservation: Readonly<ReservationV2>, completionId: string, repair = false) {
    requireV2(reservation.binding && reservation.completion?.completionId === completionId && (repair || reservation.completion.terminalStatus === "succeeded"), "SUCCESSFUL_EXACT_COMPLETION_REQUIRED");
    const terminal = await this.terminal(reservation.binding);
    requireV2(sameV2(terminal, reservation.completion), "WORKER_COMPLETION_CHANGED");
    const exact = await this.manager.inspectBindingReadOnly(reservation.binding), cwd = exact.worker.cwd;
    const workingRoot = exact.worker.normalizedRequest?.workingRoot, stat = await lstat(cwd);
    requireV2(workingRoot && await realpath(cwd) === workingRoot.realPath && !stat.isSymbolicLink() && stat.isDirectory() && String(stat.dev) === workingRoot.dev && String(stat.ino) === workingRoot.ino, "WORKER_WORKTREE_IDENTITY_DRIFT");
    requireV2(exact.attempt.ingestedAt && exact.attempt.resultPath && exact.worker.currentAttempt === reservation.binding.attemptNumber, "WORKER_ATTEMPT_NOT_CURRENT_SETTLED");
    const request = parseStrictJson(reservation.request) as any;
    requireV2(exact.worker.normalizedRequest.boundConfigRequestHash === canonicalHash(request), "IMMUTABLE_WORKER_REQUEST_MISMATCH");
    await withWorkspaceOwnership(this.root, cwd, async (ownership: any) => {
      requireV2(!ownership || ownership.launchKey === null && sameV2(ownership.binding, reservation.binding)
        || ownership?.launchKey === reservation.operationId && ownership.nodeId === reservation.operationId.slice(0, reservation.operationId.lastIndexOf("/")) && (!request.nodeWorkspace || ownership.epoch === request.nodeWorkspace.epoch), "NODE_WORKSPACE_STALE_ATTEMPT");
    });
    return { cwd, request, binding: reservation.binding, operationId: reservation.operationId };
  }
  async candidateArtifact(identity: { cwd: string; request: any; binding: WorkerBindingV2; operationId: string }, observe?: (value: unknown) => void) {
    const { cwd, request, binding, operationId } = identity;
    await this.inspector.reconcileNodeWorkspace(cwd);
    return withWorkspaceOwnership(this.root, cwd, async (ownership: any) => {
      requireV2(!ownership || ownership.execution === null, "NODE_WORKSPACE_EXECUTION_BUSY");
      requireV2(!ownership || ownership.launchKey === null && sameV2(ownership.binding, binding)
        || ownership?.launchKey === operationId && ownership.nodeId === operationId.slice(0, operationId.lastIndexOf("/")) && (!request.nodeWorkspace || ownership.epoch === request.nodeWorkspace.epoch), "NODE_WORKSPACE_STALE_ATTEMPT");
      if (ownership) requireV2(sameV2(ownership.root, await inspectWorkspaceRoot(cwd))
        && (!ownership.workspace || sameV2(ownership.workspace, await inspectNodeWorkspaceBinding(cwd, ownership.nodeId))), "WORKSPACE_IDENTITY_DRIFT");
      return this.inspectCandidateArtifact(identity, observe);
    });
  }
  private async inspectCandidateArtifact({ cwd, request }: { cwd: string; request: any }, observe?: (value: unknown) => void) {
    assertGitAttributesV2(cwd);
    requireV2(nativeGitV2(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir") === nativeGitV2(this.root, "rev-parse", "--path-format=absolute", "--git-common-dir"), "WORKER_REPOSITORY_MISMATCH");
    const candidate = { commit: nativeGitV2(cwd, "rev-parse", "HEAD^{commit}"), tree: nativeGitV2(cwd, "rev-parse", "HEAD^{tree}") };
    const status = nativeGitV2(cwd, "status", "--porcelain=v2", "--untracked-files=all"), head = nativeGitV2(cwd, "rev-parse", "--abbrev-ref", "HEAD");
    observe?.({ cwd, candidate, head, status: status.slice(0, 2000), statusTruncated: status.length > 2000, admission: "observation only; safety inspection not yet passed" });
    requireV2(!status, "WORKER_CANDIDATE_DIRTY");
    requireV2(head === "HEAD", "WORKER_MUST_REMAIN_DETACHED");
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
