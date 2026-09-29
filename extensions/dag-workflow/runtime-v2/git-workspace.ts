import { mkdtemp, lstat, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withWorkspaceOwnership, inspectWorkspaceRoot } from "../worker-runtime/workspace-ownership.mjs";
import { requireV2, sameV2 } from "../planning/v2.ts";
import { CommandRunnerV2, runArgvV2, readOutcomeV2 } from "./command-runner.ts";
import { eligibleGitV2, verifyBindingV2, nativeGitV2, gitOptionsV2 } from "./git-native.ts";
import type { GitOperationV2 } from "./git-schema.ts";
import type { CandidateV2 } from "./lifecycle-schema.ts";
import type { StoreV2 } from "./store.ts";

/** The execution claim belongs to the operation, not an individual check. It is
 * never expired on owner death and remains held between checkout/check/restore. */
export class GitWorkspaceV2 {
  readonly store: StoreV2;
  readonly repository: string;
  readonly runId: string;
  readonly operationId: string;
  readonly lockFd: number;
  readonly failpoint?: (point: string, op: Readonly<GitOperationV2>) => Promise<void>;
  constructor(store: StoreV2, repository: string, runId: string, operationId: string,
    lockFd: number, failpoint?: (point: string, op: Readonly<GitOperationV2>) => Promise<void>) {
    this.store = store; this.repository = repository; this.runId = runId; this.operationId = operationId;
    this.lockFd = lockFd; this.failpoint = failpoint;
  }
  async read() { return (await this.store.read()).runs[this.runId].gitOperations!.find(o => o.operationId === this.operationId)!; }
  async update(fn: (op: GitOperationV2) => void) {
    return this.store.transaction(async (s, publish) => {
      const run = s.runs[this.runId], op = run.gitOperations!.find(o => o.operationId === this.operationId)!;
      fn(op); run.revision++; await publish(); return structuredClone(op);
    });
  }
  async claim(release = false) {
    const op = await this.read(), w = op.workspace!;
    if (release) requireV2(w.phase === "restored" && ["closed", "accepted"].includes(op.phase), "GIT_NODE_RELEASE_BEFORE_SETTLEMENT");
    await withWorkspaceOwnership(this.repository, w.node.cwd, async (owner: any, publish: any) => {
      if (release && owner?.execution !== op.operationId) return;
      requireV2(owner && owner.nodeId === w.node.nodeId && owner.epoch === w.node.epoch && owner.launchKey === null
        && sameV2(owner.root, w.binding.root) && sameV2(owner.root, await inspectWorkspaceRoot(w.node.cwd)), "GIT_NODE_OWNERSHIP_DRIFT");
      requireV2(!owner.workspace || sameV2(owner.workspace.identity, { root: w.binding.root, common: w.binding.common, admin: w.binding.admin }), "NODE_WORKSPACE_NATIVE_IDENTITY_DRIFT");
      requireV2(owner.execution === null || owner.execution === op.operationId, "NODE_WORKSPACE_EXECUTION_BUSY");
      if (release) {
        if (owner.execution === op.operationId) { owner.execution = null; await publish(owner); }
      } else if (!owner.execution) { owner.execution = op.operationId; await publish(owner); }
    });
  }
  private async clean(op: GitOperationV2, candidate: CandidateV2) {
    await verifyBindingV2(op.workspace!.binding);
    await eligibleGitV2(op.workspace!.binding, op.proposal ? [op.candidate, op.proposal] : [op.candidate]);
    await this.assertLocks(op);
    await new CommandRunnerV2(this.store, this.repository, new Map(), "node-local", gitOptionsV2).inspectCleanWorkspace(candidate, op.workspace!.node.cwd);
  }
  private async assertLocks(op: GitOperationV2) {
    for (const name of ["index.lock", "HEAD.lock", "ORIG_HEAD.lock", "MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-apply", "rebase-merge", "sequencer", "BISECT_START"]) {
      let exists = true; try { await lstat(join(op.workspace!.binding.admin.path, name)); } catch (e: any) { if (e.code !== "ENOENT") throw e; exists = false; }
      requireV2(!exists, `GIT_NODE_LOCK_OR_OPERATION: ${name}`);
    }
  }
  private async preflight(op: GitOperationV2, target: CandidateV2, partial = false) {
    const w = op.workspace!, cwd = w.node.cwd;
    await eligibleGitV2(w.binding, [op.candidate, op.proposal!]);
    await this.assertLocks(op);
    // No excludes: ignored dependencies are user bytes too. Git's checkout can
    // overwrite ignored paths by default; reject every file/directory collision
    // before dispatch in addition to its native --no-overwrite-ignore safeguard.
    const owned = partial ? new Set([op.candidate, op.proposal!].flatMap(c => nativeGitV2(cwd, "ls-tree", "-rz", "--name-only", c.tree).split("\0").filter(Boolean))) : new Set<string>();
    const untracked = nativeGitV2(cwd, "ls-files", "--others", "-z").split("\0").filter(p => p && !owned.has(p)).map(p => p.replace(/\/$/, ""));
    const paths = nativeGitV2(cwd, "ls-tree", "-rz", "--name-only", target.tree).split("\0").filter(Boolean);
    for (const path of paths) requireV2(!untracked.some(p => p === path || p.startsWith(`${path}/`) || path.startsWith(`${p}/`)), `GIT_NODE_PATH_COLLISION: ${path}; retained user bytes; move the colliding artifact aside then retry/close the operation`);
  }
  async settle() {
    let op = await this.read(), w = op.workspace!;
    if (!w.transition || w.transition.settled) return op;
    if (w.transition.supervisor) requireV2(await readOutcomeV2(w.transition.directory, w.node.cwd, w.transition.supervisor), "GIT_NODE_TRANSITION_SETTLEMENT_REQUIRED: retain ownership until positive subtree extinction");
    // No saved supervisor means the launch gate never opened. A settled outcome
    // permits observation, not a claim that Git completed an atomic checkout.
    return this.update(o => { o.workspace!.transition!.settled = true; });
  }
  async materialize(signal?: AbortSignal) {
    await this.claim(); let op = await this.settle(), w = op.workspace!;
    requireV2(!w.closing, "GIT_NODE_RESTORE_REQUIRED: close the operation before retry");
    if (w.phase === "composed") { await this.clean(op, op.proposal!); return; }
    requireV2(["original", "switching"].includes(w.phase), "GIT_NODE_RESTORE_REQUIRED: close the operation before retry");
    if (w.phase === "switching") {
      // A lost acknowledgement can have left either exact endpoint. Anything
      // else remains owned for explicit recovery, never overwritten by checkout.
      try { await this.clean(op, op.proposal!); await this.update(o => { o.workspace!.phase = "composed"; }); return; } catch {}
    }
    await this.clean(op, op.candidate); await this.preflight(op, op.proposal!);
    await this.transition(op.proposal!, "switching", signal);
    op = await this.read(); await this.clean(op, op.proposal!);
    await this.update(o => { o.workspace!.phase = "composed"; });
    await this.failpoint?.("node-composed", await this.read());
  }
  async restore(signal?: AbortSignal) {
    await this.claim();
    // Fence all check launch gates in the same store transaction that proves
    // existing jobs settled. No new check may enter between that proof and Git.
    await this.store.transaction(async (s, publish) => {
      const run = s.runs[this.runId], current = run.gitOperations!.find(o => o.operationId === this.operationId)!;
      requireV2(current.checks.every(c => !s.executions?.[c.id] || s.executions[c.id].status === "settled"), "GIT_VALIDATION_UNRESOLVED");
      current.workspace!.closing = true; run.revision++; await publish();
    });
    let op = await this.settle(), w = op.workspace!;
    if (w.phase === "restored") { await this.clean(op, op.candidate); return; }
    try {
      await this.clean(op, op.candidate);
      await this.update(o => { o.workspace!.phase = "restored"; }); return;
    } catch {}
    try { await this.clean(op, op.proposal!); }
    catch (error) {
      // A mixed checkout is repairable only when our journal proves Git was
      // actually invoked and is now extinct. A dirty completed check is NOT a
      // partial checkout and must never be silently restored over.
      requireV2(["switching", "restoring"].includes(w.phase) && w.transition?.supervisor
        && (await readOutcomeV2(w.transition.directory, w.node.cwd, w.transition.supervisor))?.invoked, `GIT_NODE_RESTORE_BLOCKED: ${String(error)}; preserve edits and use dag_close_git_operation after resolving drift`);
      const runner = new CommandRunnerV2(this.store, this.repository, new Map(), "node-local", gitOptionsV2);
      const created = await runner.inspectPartialTransition(w.node.cwd, op.candidate, op.proposal!);
      // Preserve unknown untracked/ignored files; only remove exact new blobs
      // attributable to this launched checkout that never reached the index.
      await this.preflight(op, op.candidate, true);
      for (const path of created) await unlink(join(w.node.cwd, path));
      await this.transition(op.candidate, "restoring", signal, ["restore", `--source=${op.candidate.commit}`, "--staged", "--worktree", "--no-overlay", "--", ":/"]);
    }
    await this.preflight(await this.read(), op.candidate);
    await this.transition(op.candidate, "restoring", signal);
    op = await this.read(); await this.clean(op, op.candidate);
    await this.update(o => { o.workspace!.phase = "restored"; });
    await this.failpoint?.("node-restored", await this.read());
  }
  private async transition(target: CandidateV2, phase: "switching" | "restoring", signal?: AbortSignal,
    argv = ["checkout", "--detach", "--no-overwrite-ignore", target.commit]) {
    const directory = await mkdtemp(join(tmpdir(), "dag-v2-node-transition-"));
    let op = await this.update(o => { o.workspace!.phase = phase; o.workspace!.transition = { directory, target, settled: false }; });
    await this.failpoint?.(`node-${phase}-intent`, op);
    const outcome = await runArgvV2(["git", ...gitOptionsV2, ...argv], op.workspace!.node.cwd, signal, {
      protocolDirectory: directory, inheritedLockFd: this.lockFd, disableGitHooks: true, timeoutMs: 3_600_000,
      launch: async (identity, launch) => {
        await this.store.transaction(async (s, publish) => {
          const run = s.runs[this.runId], current = run.gitOperations!.find(o => o.operationId === this.operationId)!;
          requireV2(sameV2(current.workspace, op.workspace), "GIT_NODE_TRANSITION_CHANGED");
          // Restoration is permitted when paused/cancelling; materialization is not.
          requireV2(phase === "restoring" || run.status === "active" && sameV2(run.lease, current.lease), "GIT_NODE_TRANSITION_FENCED");
          current.workspace!.transition!.supervisor = { ...identity, token: identity.token! }; run.revision++; await publish(); launch();
        });
      },
    });
    await this.failpoint?.(`node-${phase}-exited`, await this.read());
    requireV2(outcome.settled, "GIT_NODE_TRANSITION_SETTLEMENT_REQUIRED");
    await this.update(o => { o.workspace!.transition!.settled = true; });
    requireV2(outcome.exitCode === 0 && !outcome.signal, `GIT_NODE_TRANSITION_FAILED: ${outcome.stderr}; retained workspace; close operation to restore`);
  }
}
