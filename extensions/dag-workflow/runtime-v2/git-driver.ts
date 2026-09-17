import { randomUUID } from "node:crypto";
import { mkdtemp, open, readFile, rename } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { requireV2, sameV2, type PlanV2 } from "../planning/v2.ts";
import { RuntimeV2, type MutationV2, type IntegrationsV2 } from "./service.ts";
import { StoreV2, processIdentityV2 } from "./store.ts";
import { runPlanV2, assertScopeV2, type RunV2, type IntegrationV2, type SnapshotV2 } from "./state.ts";
import { assertReadyV2, consumeRetryV2 } from "./lifecycle.ts";
import { CommandRunnerV2, runArgvV2, readOutcomeV2 } from "./command-runner.ts";
import type { CandidateV2, ExecutionRequestV2 } from "./lifecycle-schema.ts";
import type { GitOperationV2 } from "./git-schema.ts";
import { assertGitChecksV2 } from "./git-state.ts";
import { lockGitCommonV2 } from "./git-lock.ts";
import { bindGitV2, verifyBindingV2, eligibleGitV2, composeGitV2, privateRefV2, assertTargetV2, observeGitV2, makeGuardV2, rejectLegacyGitV2, gitOptionsV2 } from "./git-native.ts";

/** Native local adapter for N05. No callback may substitute for composition,
 * actual command execution, or native reconciliation. Lock order: common → store.
 * Worktree use during landing must be quiescent (not an OS sandbox). */
export class GitDriverV2 implements IntegrationsV2 {
  private activeLock?: Awaited<ReturnType<typeof lockGitCommonV2>>;
  readonly runtime: RuntimeV2;
  readonly repository: string;
  readonly options: { failpoint?: (point: string, op: Readonly<GitOperationV2>) => Promise<void> };
  constructor(runtime: RuntimeV2, repository: string,
    options: { failpoint?: (point: string, op: Readonly<GitOperationV2>) => Promise<void> } = {}) { this.runtime = runtime; this.repository = repository; this.options = options; }
  get store() { return this.runtime.store; }
  private async current(s: SnapshotV2, m: MutationV2, op?: GitOperationV2): Promise<RunV2> {
    const run = s.runs[m.runId];
    requireV2(run && run.lease && sameV2(run.lease, m.lease) && run.lease.pid === process.pid && run.lease.processStart === await processIdentityV2(), "STALE_LEASE");
    requireV2(run.status === "active", "RUN_NOT_ACTIVE"); assertScopeV2(runPlanV2(s, run), run.start.authority, Date.now());
    if (op) {
      const n = run.nodes[op.itemId];
      requireV2(n?.generation === op.generation && n.reservation?.operationId === op.reservation && n.reservation.state === "bound", "STALE_GIT_GENERATION");
      assertReadyV2(run, runPlanV2(s, run), op.itemId, op.candidate);
    }
    return run;
  }
  private async update(runId: string, operationId: string, fn: (op: GitOperationV2, run: RunV2, s: SnapshotV2) => Promise<void> | void): Promise<GitOperationV2> {
    return this.store.transaction(async (s, publish) => {
      const run = s.runs[runId], op = run?.gitOperations?.find(o => o.operationId === operationId);
      requireV2(op, "GIT_INTENT_MISSING"); await fn(op, run, s); run.revision++; await publish(); return structuredClone(op);
    });
  }
  private async claim(op: GitOperationV2, runId: string): Promise<void> {
    const path = join(op.binding.common.path, "pi-dag-v2-owned");
    const value = { repository: dirname(dirname(this.store.directory)), runId, operationId: op.operationId };
    let previous: typeof value | undefined;
    try { previous = JSON.parse(await readFile(path, "utf8")); } catch (e: any) { if (e.code !== "ENOENT") throw e; }
    if (previous && !sameV2(previous, value)) {
      requireV2(typeof previous.repository === "string" && typeof previous.runId === "string" && typeof previous.operationId === "string", "COMMON_GIT_CLAIM_CORRUPT");
      const prior = await new StoreV2(previous.repository).read();
      requireV2(prior.runs[previous.runId]?.gitOperations?.some(o => o.operationId === previous!.operationId && ["accepted", "closed"].includes(o.phase)), "COMMON_GIT_OPERATION_UNRESOLVED");
    }
    if (sameV2(previous ?? null, value)) return;
    const temp = `${path}.${randomUUID()}.tmp`, f = await open(temp, "wx", 0o600);
    try { await f.writeFile(JSON.stringify(value)); await f.sync(); } finally { await f.close(); }
    await rename(temp, path); const dir = await open(op.binding.common.path, "r"); try { await dir.sync(); } finally { await dir.close(); }
  }
  private checks(run: RunV2, plan: PlanV2, op: GitOperationV2): ExecutionRequestV2[] {
    return [...plan.integration.prefixCommands.map(c => ({ ...c, phase: "prefix" })), ...plan.integration.finalCommands.map(c => ({ ...c, phase: "final" }))].map(c => ({
      id: `git-${randomUUID()}`, plan: run.start.selection, runId: run.runId, itemId: op.itemId, generation: op.generation,
      attempt: op.operationId, round: 1, stage: 7, candidate: op.proposal!, implementationWorkerId: run.nodes[op.itemId].reservation!.workerId!,
      check: { id: `integration-${c.phase}`, stage: 7, expectation: `Combined ${c.phase} command ${c.id}`, sourceRefs: [plan.source.refs[0].ref],
        applicability: { kind: "required" }, procedure: { kind: "command", argv: c.argv }, environment: "node-local", replay: "idempotent" },
      authority: { effect: "repository_local", expiresAt: run.start.authority.expiresAt },
    }));
  }
  async integrate(m: MutationV2, itemId: string, generation: number, candidate: CandidateV2, signal?: AbortSignal): Promise<RunV2> {
    const initial = await this.store.read(), run = initial.runs[m.runId];
    requireV2(run && run.revision === m.expectedRevision, "STALE_REVISION");
    requireV2(sameV2(run.lease, m.lease) && m.lease.pid === process.pid && m.lease.processStart === await processIdentityV2(), "STALE_LEASE");
    const operationId = `${m.runId}/${itemId}/${generation}/integration`;
    const prior = run.gitOperations?.find(o => o.operationId === operationId);
    if (prior) requireV2(sameV2(prior.candidate, candidate), "GIT_REQUEST_CONFLICT");
    const binding = run.gitBinding ?? await bindGitV2(this.repository);
    requireV2(binding.root.path === this.repository, "GIT_ROOT_BINDING_MISMATCH"); await verifyBindingV2(binding);
    const lock = await lockGitCommonV2(binding.common.path);
    this.activeLock = lock;
    try {
      await rejectLegacyGitV2(binding.common.path); await lock.verify();
      let op = await this.store.transaction(async (s, publish) => {
        const r = s.runs[m.runId]; requireV2(r.revision === m.expectedRevision, "STALE_REVISION");
        const existing = r.gitOperations?.find(o => o.operationId === operationId);
        if (existing) { requireV2(sameV2(existing.candidate, candidate), "GIT_REQUEST_CONFLICT"); return structuredClone(existing); }
        await this.current(s, m); const plan = runPlanV2(s, r), node = r.nodes[itemId];
        requireV2(node?.generation === generation && node.reservation?.state === "bound", "BOUND_WORKER_REQUIRED"); assertReadyV2(r, plan, itemId, candidate);
        requireV2(!(r.gitOperations ?? []).some(o => !["accepted", "closed"].includes(o.phase)), "UNRESOLVED_GIT_OPERATION");
        for (const other of Object.values(s.runs)) if (other.gitBinding && runPlanV2(s, other).repository.repositoryId === plan.repository.repositoryId) requireV2(sameV2(other.gitBinding, binding), "PERSISTED_GIT_BINDING_DRIFT");
        const expected = r.gitOperations?.filter(o => o.phase === "accepted").at(-1)?.proposal ?? { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree };
        const operation: GitOperationV2 = { operationId, itemId, generation, reservation: node.reservation.operationId, lease: m.lease,
          candidate, sourceBase: { commit: plan.repository.baselineCommit, tree: plan.repository.baselineTree }, expected, targetRef: plan.repository.targetBranch,
          binding, profile: "ordinary-ff-v2-1", phase: "intent", checks: [], dispatches: 0 };
        // Publish the common-dir lane claim first: a crash between these stores
        // must block other writers, never leave a durable unclaimed operation.
        await this.claim(operation, m.runId);
        r.gitBinding = binding; (r.gitOperations ??= []).push(operation); r.revision++; await publish(); return structuredClone(operation);
      });
      if (op.phase === "accepted") return (await this.store.read()).runs[m.runId];
      await this.claim(op, m.runId);
      await this.options.failpoint?.("intent", op);
      // Observation is not authority to dispatch: cancellation/lease loss may
      // still close an already-running landing without releasing consumers.
      if (op.landing && !["blocked", "closed"].includes(op.phase)) op = await this.reconcile(m.runId, op);
      if (op.phase === "blocked" || op.phase === "closed") throw Error(`GIT_OPERATION_BLOCKED: ${op.diagnostic ?? op.observation}`);
      await this.current(await this.store.read(), m, op);
      if (op.proposal) {
        await eligibleGitV2(binding, [op.sourceBase, op.candidate, op.expected, op.proposal]);
        requireV2(sameV2(composeGitV2(op), op.proposal), "PERSISTED_GIT_PROPOSAL_MISMATCH");
      }
      if (op.phase === "intent") {
        let proposal: CandidateV2;
        try {
          await eligibleGitV2(binding, [op.sourceBase, candidate, op.expected]); await assertTargetV2(op, op.expected);
          proposal = composeGitV2(op); await eligibleGitV2(binding, [proposal]);
          const refBase = `refs/pi-dag-v2/${m.runId}/${itemId}/${generation}`;
          for (const [name, value] of [["base", op.sourceBase], ["candidate", op.candidate], ["prefix", op.expected], ["proposal", proposal]] as const) privateRefV2(this.repository, `${refBase}/${name}`, value.commit);
        } catch (error) {
          await this.update(m.runId, operationId, o => { o.phase = "blocked"; o.diagnostic = String(error).slice(-16384); });
          throw error;
        }
        op = await this.update(m.runId, operationId, async (o, r, s) => { await this.current(s, m, o); o.proposal = proposal; o.checks = this.checks(r, runPlanV2(s, r), o); o.phase = "composed"; });
        await this.options.failpoint?.("composed", op);
      }
      if (op.phase === "composed") {
        // Hydrate the persisted exact requests on every fresh service instance.
        op = await this.update(m.runId, operationId, async (o, _r, s) => { await this.current(s, m, o); o.lease = m.lease; });
        await eligibleGitV2(binding, [op.proposal!]);
        const runner = new CommandRunnerV2(this.store, this.repository, new Map(), "node-local", gitOptionsV2, lock.fd);
        for (const request of op.checks) {
          await runner.ensure(request, signal); const result = await runner.read(request);
          requireV2(result, `GIT_VALIDATION_UNRESOLVED: ${request.id}`);
          requireV2(result.disposition === "PASS", `GIT_CHECK_NONPASS ${request.check.expectation}: ${result.diagnostic}`);
        }
        op = await this.update(m.runId, operationId, async (o, _r, s) => { await this.current(s, m, o); assertGitChecksV2(s, o); o.phase = "validated"; });
        await this.options.failpoint?.("validated", op);
      }
      if (op.phase === "validated") {
        await eligibleGitV2(binding, [op.proposal!]); await assertTargetV2(op, op.expected); await lock.verify();
        signal?.throwIfAborted(); requireV2(op.dispatches < 2, "GIT_REDISPATCH_LIMIT");
        const directory = await mkdtemp(join(tmpdir(), "dag-v2-landing-"));
        const hooks = await makeGuardV2(op, directory), processStart = await processIdentityV2(); requireV2(processStart, "PROCESS_IDENTITY_UNAVAILABLE");
        op = await this.update(m.runId, operationId, async (o, _r, s) => {
          await this.current(s, m, o); assertGitChecksV2(s, o);
          o.phase = "landing"; o.dispatches++; o.landing = { directory, owner: { pid: process.pid, processStart }, settled: false };
        });
        await this.options.failpoint?.("landing-intent", op);
        const observed = await runArgvV2(["git", ...gitOptionsV2, "-c", `core.hooksPath=${hooks}`, "merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore", "--no-edit", op.proposal!.commit], this.repository, signal, {
          protocolDirectory: directory, inheritedLockFd: lock.fd, timeoutMs: Math.max(1, run.start.authority.expiresAt - Date.now()),
          launch: async (identity, launch) => {
            await this.store.transaction(async (s, publish) => {
              const r = await this.current(s, m, op), current = r.gitOperations!.find(o => o.operationId === operationId)!;
              await assertTargetV2(current, current.expected); assertGitChecksV2(s, current); await lock.verify();
              current.landing!.supervisor = { ...identity, token: identity.token! }; r.revision++; await publish();
              launch();
            });
          },
        });
        await this.options.failpoint?.("git-exited", op);
        op = await this.update(m.runId, operationId, o => { o.landing!.settled = observed.settled; o.diagnostic = observed.stderr.slice(-16384); });
        op = await this.reconcile(m.runId, op);
      }
      if (op.phase !== "landed") throw Error(`GIT_NOT_LANDED: ${op.observation ?? op.phase}; ${op.diagnostic ?? ""}`);
      await this.options.failpoint?.("landed", op);
      const current = (await this.store.read()).runs[m.runId];
      await this.current(await this.store.read(), m, op);
      return await this.runtime.integrate({ ...m, expectedRevision: current.revision }, { operationId, runId: m.runId, itemId, generation, candidate, target: op.proposal! }, this);
    } finally { this.activeLock = undefined; await lock.close(); }
  }
  private async reconcile(runId: string, op: GitOperationV2): Promise<GitOperationV2> {
    requireV2(op.landing, "GIT_LANDING_INTENT_REQUIRED");
    if (!op.landing.settled) {
      if (op.landing.supervisor) {
        const outcome = await readOutcomeV2(op.landing.directory, op.binding.root.path, op.landing.supervisor);
        requireV2(outcome, "GIT_PROCESS_SETTLEMENT_REQUIRED: missing subtree extinction outcome; retain operation and all locks");
      }
      // Without a saved supervisor identity, the durable launch gate never sent
      // launch. Common lock exclusion also covers a supervisor awaiting that gate.
      op = await this.update(runId, op.operationId, o => { o.landing!.settled = true; });
    }
    const observation = await observeGitV2(op);
    return this.update(runId, op.operationId, o => {
      o.observation = observation;
      if (observation === "new-clean") o.phase = "landed";
      else if (observation === "old-clean" && o.dispatches < 2) o.phase = "validated";
      else { o.phase = "blocked"; o.diagnostic = `${o.diagnostic ?? ""}\nReconciliation: ${observation}; retained index/files/locks; no automatic repair`.slice(-16384); }
    });
  }
  /** Close only observed effect settlement. Never cleans a workspace or accepts
   * a cancelled node. Ambiguous/dirty/third target states remain unresolved. */
  async closeOperation(m: MutationV2, operationId: string): Promise<void> {
    const snapshot = await this.store.read(), run = snapshot.runs[m.runId], op = run?.gitOperations?.find(o => o.operationId === operationId);
    requireV2(op && run.revision === m.expectedRevision && sameV2(run.lease, m.lease) && m.lease.pid === process.pid && m.lease.processStart === await processIdentityV2(), "STALE_GIT_CLOSURE");
    if (op.phase === "closed") return;
    requireV2(op.phase !== "accepted", "GIT_OPERATION_ALREADY_ACCEPTED");
    const lock = await lockGitCommonV2(op.binding.common.path);
    try {
      await this.claim(op, m.runId);
      const current = op.landing ? await this.reconcile(m.runId, op) : op;
      const observation = await observeGitV2(current);
      requireV2(observation === "old-clean" || run.status === "cancelling" && observation === "new-clean", "GIT_CLOSURE_UNRESOLVED");
      await this.update(m.runId, operationId, (o, r, s) => {
        requireV2(sameV2(r.lease, m.lease) && ["active", "paused", "needs_replan", "cancelling"].includes(r.status), "STALE_GIT_CLOSURE");
        requireV2(o.checks.every(c => !s.executions?.[c.id] || s.executions[c.id].status === "settled"), "GIT_VALIDATION_UNRESOLVED");
        if (r.status !== "cancelling") consumeRetryV2(r, o.itemId, "integration", 0, "native-git", "operation-closed");
        o.observation = observation; o.phase = "closed";
      });
    } finally { await lock.close(); }
  }
  /** Called under RuntimeV2's short acceptance transaction, with common lock
   * still held by integrate(). Rehydrate, never trust an in-memory completion. */
  async verify(run: Readonly<RunV2>, _plan: Readonly<PlanV2>, integration: Readonly<IntegrationV2>): Promise<void> {
    requireV2(this.activeLock, "GIT_COMMON_SERIALIZATION_REQUIRED: call GitDriverV2.integrate"); await this.activeLock.verify();
    const s = await this.store.read(), op = s.runs[run.runId]?.gitOperations?.find(o => o.operationId === integration.operationId);
    requireV2(op?.phase === "landed" && op.landing?.settled && sameV2(op.candidate, integration.candidate) && sameV2(op.proposal, integration.target), "NATIVE_LANDING_REQUIRED");
    assertGitChecksV2(s, op); await assertTargetV2(op, op.proposal!);
  }
}
