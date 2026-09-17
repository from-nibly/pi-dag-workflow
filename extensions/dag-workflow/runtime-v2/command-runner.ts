import { spawn, execFileSync } from "node:child_process";
import { lstatSync, realpathSync, readFileSync, readlinkSync } from "node:fs";
import { mkdtemp, rm, readFile, open, rename } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { requireV2, sameV2, validateShapeV2 } from "../planning/v2.ts";
import { StoreV2, processIdentityV2 } from "./store.ts";
import { ExecutionRequestV2Schema, ExecutionResultV2Schema, type CandidateV2, type ExecutionRequestV2, type ExecutionResultV2, type CommandJobV2, type FindingV2 } from "./lifecycle-schema.ts";
import { auditResultV2 } from "./lifecycle.ts";
import { currentGitExecutionV2 } from "./git-state.ts";
import { bindGitV2, eligibleGitV2, configuredGitHooksV2, safeGitOptionsV2 } from "./git-native.ts";

/** Local trusted implementations are code, not worker-supplied attestations.
 * run is actually invoked, once, in the exact isolated candidate workspace.
 * A worker-manager producer must await/hydrate its durable exact worker result;
 * a generic worker's completed flag is not a producer observation. */
export interface TrustedProducerV2 {
  run(context: { cwd: string; request: Readonly<ExecutionRequestV2>; signal?: AbortSignal }): Promise<{
    disposition: "PASS" | "FAIL" | "BLOCKED"; observation: string; findings: FindingV2[];
    // F2/F5 adapters must hydrate the actual fresh evaluator/reviewer context.
    // An invocation of this JS callback is not itself a fresh worker context.
    context?: { id: string; lineage: string[] };
  }>;
}
export interface ResultsV2 {
  ensure(request: Readonly<ExecutionRequestV2>, signal?: AbortSignal): Promise<void>;
  read(request: Readonly<ExecutionRequestV2>): Promise<ExecutionResultV2 | null>;
}
export interface CandidateInspectorV2 { inspect(candidate: Readonly<CandidateV2>): Promise<void> }
const bounded = (text: string) => text.slice(-16384);

/** One durable natural request, before execution, and one durable result. A lost
 * acknowledgement reads the stored result; an interrupted invocation NEVER
 * blindly respawns. An owner-dead or process-ambiguous job requires an explicit
 * observed settlement via reconcileInterrupted (BLOCKED, not invented PASS).
 * Commands are argv-only and run outside the run/store lock. */
export class CommandRunnerV2 implements ResultsV2, CandidateInspectorV2 {
  readonly store: StoreV2;
  readonly repository: string;
  readonly producers: ReadonlyMap<string, TrustedProducerV2>;
  readonly environment: string;
  readonly gitOptions: readonly string[];
  readonly inheritedLockFd?: number;
  constructor(store: StoreV2, repository: string, producers: ReadonlyMap<string, TrustedProducerV2> = new Map(), environment = "node-local", gitOptions: readonly string[] = [], inheritedLockFd?: number) {
    this.store = store; this.repository = repository; this.producers = producers; this.environment = environment; this.gitOptions = gitOptions; this.inheritedLockFd = inheritedLockFd;
  }
  private git(cwd: string, ...args: string[]): string {
    return execFileSync("git", [...(this.gitOptions.length > 0 ? safeGitOptionsV2(cwd, this.gitOptions) : this.gitOptions), ...args], { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: gitEnvironmentV2() }).trim();
  }
  async inspect(candidate: Readonly<CandidateV2>): Promise<void> {
    requireV2(this.git(this.repository, "rev-parse", "--verify", `${candidate.commit}^{commit}`) === candidate.commit
      && this.git(this.repository, "rev-parse", `${candidate.commit}^{tree}`) === candidate.tree, "NATIVE_CANDIDATE_MISMATCH");
  }
  async inspectCleanWorkspace(candidate: Readonly<CandidateV2>, cwd: string): Promise<void> {
    await this.inspect(candidate);
    requireV2(await this.clean(cwd, candidate), "UNCLEAN_CANDIDATE_WORKSPACE: raw bytes/index/modes must match the committed candidate");
  }
  async read(request: Readonly<ExecutionRequestV2>): Promise<ExecutionResultV2 | null> {
    const job = (await this.store.read()).executions?.[request.id];
    if (!job) return null;
    requireV2(sameV2(job.request, request), "EXECUTION_REQUEST_CONFLICT");
    return job.result ?? null;
  }
  async ensure(input: Readonly<ExecutionRequestV2>, signal?: AbortSignal): Promise<void> {
    validateShapeV2(ExecutionRequestV2Schema, input);
    const request = structuredClone(input), identity = await processIdentityV2(); requireV2(identity, "PROCESS_IDENTITY_UNAVAILABLE");
    const launch = await this.store.transaction(async (s, publish) => {
      const jobs = s.executions ??= {}, existing = jobs[request.id];
      if (existing) { requireV2(sameV2(existing.request, request), "EXECUTION_REQUEST_CONFLICT"); return false; }
      // Only service-persisted current intents can cause execution. A late ensure
      // after cancellation/replacement cannot launch an obsolete command.
      const run = s.runs[request.runId], node = run?.nodes[request.itemId];
      requireV2(currentGitExecutionV2(s, request) || (run?.status === "active" && node?.generation === request.generation && node.lifecycle?.round === request.round
        && node.lifecycle.executions.some(e => e.status === "intent" && sameV2(e.request, request))), "CURRENT_EXECUTION_INTENT_REQUIRED");
      requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
      jobs[request.id] = { request, owner: { pid: process.pid, processStart: identity }, status: "running" };
      await publish(); return true;
    });
    if (!launch) return;
    const { result, protocolDirectory } = await this.execute(request, signal);
    // Only publication is retried on lock contention, never the invocation.
    for (let retry = 0; ; retry++) {
      try {
        await this.store.transaction(async (s, publish) => {
          const job = s.executions![request.id]; requireV2(sameV2(job.request, request), "EXECUTION_RESULT_CONFLICT");
          if (job.result) { requireV2(sameV2(job.result, result), "EXECUTION_RESULT_CONFLICT"); return; }
          job.result = result; job.status = "settled"; await publish();
        });
        break;
      } catch (e) { if ((e as Error).message !== "STORE_BUSY" || retry >= 500) throw e; await delay(20); }
    }
    // Only protocol metadata remains here; candidate cleanup already succeeded.
    // Keep the receipt until lifecycle publication is durable, including crashes
    // between workspace removal and publication. Metadata retention is harmless.
    if (protocolDirectory) await rm(protocolDirectory, { recursive: true, force: true }).catch(() => {});
  }
  /** Under the same launch lock, absence of a job proves this executor never
   * invoked the obsolete intent. Record that fact so cancellation can finish. */
  async reconcileUnlaunched(request: ExecutionRequestV2): Promise<void> {
    await this.store.transaction(async (s, publish) => {
      const jobs = s.executions ??= {};
      const existing = jobs[request.id];
      if (existing) {
        requireV2(sameV2(existing.request, request) && existing.result && !existing.result.executor.invoked, "EXECUTION_ALREADY_LAUNCHED"); return;
      }
      const run = s.runs[request.runId], node = run?.nodes[request.itemId];
      requireV2(node?.lifecycle?.executions.some(e => e.status === "quarantined" && sameV2(e.request, request)), "OBSOLETE_INTENT_REQUIRED");
      const identity = await processIdentityV2(); requireV2(identity, "PROCESS_IDENTITY_UNAVAILABLE");
      const result = this.emptyResult(request); result.diagnostic = "Obsolete intent was never invoked (no executor job under launch lock).";
      jobs[request.id] = { request: structuredClone(request), owner: { pid: process.pid, processStart: identity }, status: "settled", result };
      await publish();
    });
  }
  /** Caller must establish process-tree/effect settlement independently. Parent
   * death alone is insufficient: a detached child may still be running. */
  async reconcileInterrupted(request: ExecutionRequestV2, settled: (request: Readonly<ExecutionRequestV2>, job: Readonly<CommandJobV2>) => Promise<void>): Promise<void> {
    await this.store.transaction(async (s, publish) => {
      const job = s.executions?.[request.id]; requireV2(job && sameV2(job.request, request), "EXECUTION_MISSING");
      if (job.result) return;
      const journal = job.workspace && request.check.procedure.kind === "command" ? await readProcessJournalV2(job.workspace) : null;
      if (journal) requireV2(journal.requestId === request.id, "COMMAND_PROCESS_JOURNAL_MISMATCH");
      requireV2(await processIdentityV2(job.owner.pid) !== job.owner.processStart || journal?.state === "BLOCKED", "EXECUTOR_STILL_ALIVE");
      // No negative /proc scan can prove extinction. A live supervisor must
      // finish its reaping protocol. If it died without an outcome, the callback
      // below is the mandatory *independent* process/effect settlement proof.
      const outcome = journal ? await readProcessOutcomeV2(job.workspace!, journal.identity) : null;
      if (journal && !outcome) {
        requireV2(await processIdentityV2(journal.identity.pid) !== journal.identity.processStart, "COMMAND_PROCESS_SETTLEMENT_REQUIRED");
      }
      await settled(structuredClone(request), structuredClone(job));
      const result = this.emptyResult(request);
      if (outcome) { result.executor.invoked = outcome.invoked; result.exitCode = outcome.exitCode; result.signal = outcome.signal; }
      result.diagnostic = "Executor died without a durable lifecycle result; independent process tree/effects settlement confirmed. Explicit bounded infrastructure retry required.";
      result.findings = [{ id: "executor-lost", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id, fingerprint: "executor-lost", detail: result.diagnostic }];
      job.result = result; job.status = "settled"; await publish();
    });
  }
  /** Product recovery for the supported command profile: a durable kernel reaping
   * outcome plus exact native workspace settlement, never a negative process
   * scan or a caller's claim. Dirty/replaced workspaces remain blocked. */
  async reconcileExtinctCommand(request: ExecutionRequestV2): Promise<void> {
    requireV2(request.check.procedure.kind === "command", "COMMAND_RECOVERY_ONLY");
    await this.reconcileInterrupted(request, async (_request, job) => {
      requireV2(job.workspace && job.workspaceIdentity, "RECOVERY_WORKSPACE_REQUIRED");
      const journal = await readProcessJournalV2(job.workspace);
      requireV2(journal?.requestId === request.id && await readProcessOutcomeV2(job.workspace, journal.identity), "COMMAND_EXTINCTION_REQUIRED");
      const binding = await bindGitV2(this.repository);
      requireV2(sameV2(binding.common, job.workspaceIdentity.common), "RECOVERY_REPOSITORY_IDENTITY_DRIFT");
      let exists = true;
      try { lstatSync(job.workspace); } catch (error: any) { if (error.code === "ENOENT") exists = false; else throw error; }
      if (exists) {
        requireV2(sameV2(this.workspaceIdentity(job.workspace), job.workspaceIdentity), "RECOVERY_WORKSPACE_IDENTITY_DRIFT");
        requireV2(await this.clean(job.workspace, request.candidate), "RECOVERY_WORKSPACE_DIRTY");
      } else {
        // Cleanup may have completed before result publication. The positive
        // extinction outcome survives that boundary; require native deregistration
        // too, rather than treating a vanished pathname alone as settlement.
        requireV2(!this.git(this.repository, "worktree", "list", "--porcelain", "-z").split("\0").includes(`worktree ${job.workspace}`), "RECOVERY_WORKSPACE_STILL_REGISTERED");
      }
    });
  }
  /** Recheck the generation while holding the launch lock through actual start.
   * The workspace is durable before invocation so dead-owner reconciliation can
   * locate the exact process workspace even when there is no result yet. */
  private async invoke<T>(request: ExecutionRequestV2, cwd: string, start: () => Promise<T>, beforeStart?: () => Promise<void>): Promise<T> {
    let pending: Promise<T> | undefined, gateError: unknown;
    try {
      await this.store.transaction(async (s, publish) => {
        const run = s.runs[request.runId], node = run?.nodes[request.itemId], job = s.executions?.[request.id];
        requireV2(job && sameV2(job.request, request) && !job.result, "EXECUTION_REQUEST_CONFLICT");
        requireV2(currentGitExecutionV2(s, request) || (run.status === "active" && node?.generation === request.generation && node.lifecycle?.round === request.round
          && node.lifecycle.executions.some(e => e.status === "intent" && sameV2(e.request, request))), "EXECUTION_FENCED_BEFORE_INVOCATION");
        requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
        const identity = this.workspaceIdentity(cwd);
        requireV2(!job.workspaceIdentity || sameV2(job.workspaceIdentity, identity), "EXECUTION_WORKSPACE_IDENTITY_DRIFT");
        job.workspace = cwd; job.workspaceIdentity = identity; await publish();
        requireV2(await this.clean(cwd, request.candidate), "UNCLEAN_EXECUTION_WORKSPACE");
        await beforeStart?.();
        pending = start();
      });
    } catch (error) { gateError = error; }
    // Even if acknowledgement/identity verification fails after starting, wait
    // for settlement before recording a failure or considering workspace cleanup.
    const value = pending ? await pending : undefined;
    if (gateError) throw gateError;
    requireV2(pending, "EXECUTION_NOT_INVOKED"); return value as T;
  }
  private emptyResult(request: ExecutionRequestV2): ExecutionResultV2 {
    const now = Date.now();
    return { request, startedAt: now, endedAt: now, durationMs: 0, disposition: "BLOCKED", exitCode: null, signal: null,
      stdout: "", stderr: "", truncated: false, diagnostic: "", findings: [],
      environment: { profile: this.environment, platform: `${process.platform}/${process.arch}`, runtime: `node ${process.version}` },
      executor: { kind: request.check.procedure.kind, identity: request.check.procedure.kind === "command" ? request.check.procedure.argv[0] : request.check.procedure.producerId,
        contextId: `context-${request.id}`, lineage: [], invoked: false },
      workspace: { candidate: request.candidate, cleanBefore: false, cleanAfter: false, isolated: true } };
  }
  private async execute(request: ExecutionRequestV2, signal?: AbortSignal): Promise<{ result: ExecutionResultV2; protocolDirectory?: string }> {
    const result = this.emptyResult(request), start = performance.now();
    let root: string | undefined, cwd: string | undefined, protocolDirectory: string | undefined, added = false, unsettled = false;
    let identity: ReturnType<CommandRunnerV2["workspaceIdentity"]> | undefined;
    try {
      requireV2(request.check.environment === this.environment, "EXECUTION_ENVIRONMENT_UNAVAILABLE");
      await this.inspect(request.candidate);
      if (this.gitOptions.length > 0) await eligibleGitV2(await bindGitV2(this.repository), [request.candidate]);
      requireV2(!signal?.aborted, "EXECUTION_CANCELLED");
      root = await mkdtemp(join(tmpdir(), "dag-v2-check-")); cwd = join(root, "candidate");
      this.git(this.repository, "-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach", cwd, request.candidate.commit); added = true;
      identity = this.workspaceIdentity(cwd);
      result.workspace.cleanBefore = await this.clean(cwd, request.candidate);
      requireV2(result.workspace.cleanBefore, "UNCLEAN_EXECUTION_WORKSPACE");
      requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
      const procedure = request.check.procedure;
      if (procedure.kind === "command") {
        const observed = await this.invoke(request, cwd, () => runArgvV2(procedure.argv, cwd!, signal, {
          timeoutMs: Math.max(1, request.authority.expiresAt - Date.now()),
          protocolDirectory: dirname(cwd!), inheritedLockFd: this.inheritedLockFd, disableGitHooks: this.gitOptions.length > 0,
          launch: async (identity, launch) => {
            // The gated session leader cannot invoke argv before this journal is
            // synced and the current intent has been rechecked under the lock.
            for (let retry = 0; ; retry++) {
              try {
                await this.invoke(request, cwd!, async () => { launch(); },
                  () => writeProcessJournalV2(cwd!, { version: 2, requestId: request.id, identity, state: "running" }));
                break;
              } catch (e) { if ((e as Error).message !== "STORE_BUSY" || retry >= 500) throw e; await delay(20); }
            }
          },
        }).then(observed => {
          result.executor.invoked = observed.invoked;
          unsettled = !observed.settled;
          return observed;
        }));
        unsettled ||= !observed.settled;
        if (unsettled) {
          const journal = await readProcessJournalV2(cwd);
          if (journal) await writeProcessJournalV2(cwd, { ...journal, state: "BLOCKED", diagnostic: observed.stderr });
          throw Error(`COMMAND_PROCESS_SETTLEMENT_REQUIRED: ${observed.stderr}`);
        }
        result.stdout = observed.stdout; result.stderr = observed.stderr; result.truncated = observed.truncated;
        result.exitCode = observed.exitCode; result.signal = observed.signal;
        result.executor.invoked = observed.invoked;
        result.disposition = !observed.invoked ? "BLOCKED" : observed.exitCode === 0 && !observed.signal && !observed.interrupted ? "PASS" : "FAIL";
        if (!observed.invoked) result.findings = [{ id: "command-unavailable", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id, fingerprint: `command-unavailable:${procedure.argv[0]}`, detail: observed.stderr || "Command was not spawned" }];
        const argv = JSON.stringify(procedure.argv);
        result.diagnostic = `argv=${argv.slice(0, 8000)}${argv.length > 8000 ? "… (full argv retained in request)" : ""} exit=${observed.exitCode} signal=${observed.signal ?? "none"}`;
        if (result.disposition !== "PASS") result.diagnostic += `; ${observed.stderr.slice(-4000) || observed.stdout.slice(-4000)}`;
      } else {
        const producer = this.producers.get(procedure.producerId);
        requireV2(producer, `PRODUCER_UNAVAILABLE: ${procedure.producerId}`);
        const observed = await this.invoke(request, cwd, () => {
          result.executor.invoked = true;
          return producer.run({ cwd: cwd!, request: structuredClone(request), signal });
        });
        requireV2(typeof observed.observation === "string" && observed.observation.trim().length > 0, "PRODUCER_OBSERVATION_REQUIRED");
        if (request.stage === 2 || request.stage === 5 || request.stage === 7) requireV2(observed.context, "PRODUCER_INDEPENDENT_CONTEXT_REQUIRED");
        if (observed.context) { result.executor.contextId = observed.context.id; result.executor.lineage = observed.context.lineage; }
        result.disposition = observed.disposition; result.stdout = bounded(observed.observation); result.truncated = observed.observation.length > 16384;
        result.findings = observed.findings; result.diagnostic = `producer=${procedure.producerId}: ${bounded(observed.observation)}`;
      }
      result.workspace.cleanAfter = sameV2(identity, this.workspaceIdentity(cwd)) && await this.clean(cwd, request.candidate);
      if (!result.workspace.cleanAfter) { result.disposition = "FAIL"; result.diagnostic += "; candidate/workspace changed during no-edit verification"; }
      if (result.disposition === "PASS" && result.findings.some(f => f.severity === "blocking")) result.disposition = "FAIL";
    } catch (error) {
      result.disposition = "BLOCKED"; result.diagnostic = bounded(`${result.diagnostic}\n${String(error)}`);
      result.findings = [{ id: "execution-unavailable", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id,
        fingerprint: `execution-unavailable:${request.check.procedure.kind}`, detail: result.diagnostic }];
    } finally {
      // Never force-clean a dirty check worktree: preserve it for diagnosis.
      if (!unsettled && added && cwd && result.workspace.cleanAfter) {
        try {
          requireV2(sameV2(identity, this.workspaceIdentity(cwd)) && await this.clean(cwd, request.candidate), "EXECUTION_WORKSPACE_IDENTITY_DRIFT");
          this.git(this.repository, "worktree", "remove", cwd);
          if (root && request.check.procedure.kind === "command") protocolDirectory = root;
          else if (root) await rm(root, { recursive: true });
        }
        catch (e) {
          result.disposition = "BLOCKED"; result.workspace.cleanAfter = false;
          result.diagnostic += `; cleanup retained (cleanliness/cleanup could not be confirmed): ${String(e)}`;
        }
      } else if (!added && root) await rm(root, { recursive: true });
      else if (cwd) result.diagnostic += `; retained workspace: ${cwd}`;
      result.endedAt = Date.now(); result.durationMs = Math.max(0, Math.round(performance.now() - start)); result.diagnostic = bounded(result.diagnostic);
    }
    // A BLOCKED outcome would still be a settled/retryable job in the lifecycle
    // schema. Keep ambiguous jobs running with no result, and the BLOCKED journal
    // plus workspace for explicit reconciliation instead.
    if (unsettled) throw Error(`COMMAND_PROCESS_SETTLEMENT_REQUIRED: ${result.diagnostic}`);
    try { validateShapeV2(ExecutionResultV2Schema, result); auditResultV2(request, result); return { result, protocolDirectory }; }
    catch (error) {
      // A malformed producer response is an observed protocol failure, not a
      // permanently running job or an excuse to invoke the producer again.
      const failure = this.emptyResult(request);
      failure.startedAt = result.startedAt; failure.endedAt = result.endedAt; failure.durationMs = result.durationMs;
      failure.executor.invoked = result.executor.invoked;
      failure.workspace = result.workspace;
      failure.diagnostic = bounded(`Invalid execution response: ${String(error)}`);
      failure.findings = [{ id: "producer-protocol", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id,
        fingerprint: "producer-protocol", detail: failure.diagnostic }];
      return { result: failure, protocolDirectory };
    }
  }
  private workspaceIdentity(cwd: string) {
    const identity = (path: string) => { const stat = lstatSync(path, { bigint: true }); requireV2(stat.isDirectory() && realpathSync(path) === path, "UNSAFE_EXECUTION_WORKSPACE"); return { path, dev: stat.dev.toString(), ino: stat.ino.toString() }; };
    return { root: identity(resolve(cwd)), common: identity(this.git(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")), admin: identity(this.git(cwd, "rev-parse", "--absolute-git-dir")) };
  }
  private async clean(cwd: string, candidate: CandidateV2): Promise<boolean> {
    // Verification argv may have installed attributes/config. Its local command
    // authority does not authorize filters during our observation or cleanup.
    if (this.gitOptions.length > 0) await eligibleGitV2(await bindGitV2(cwd), [candidate]);
    return this.git(cwd, "rev-parse", "--abbrev-ref", "HEAD") === "HEAD"
      && resolve(this.git(cwd, "rev-parse", "--show-toplevel")) === resolve(cwd)
      && this.git(cwd, "rev-parse", "HEAD") === candidate.commit && this.git(cwd, "rev-parse", "HEAD^{tree}") === candidate.tree
      // Inspect the persisted index first: disabling fsmonitor in Git can hide
      // its valid bits from ls-files. Reject extensions that carry hidden state.
      && this.ordinaryIndex(cwd, candidate)
      && ["-v", "-f"].every(flag => this.git(cwd, "ls-files", flag, "-z").split("\0").filter(Boolean).every(row => row.startsWith("H ")))
      && this.git(cwd, "write-tree") === candidate.tree
      && this.trackedBytesMatch(cwd, candidate)
      && this.git(cwd, "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false", "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching") === "";
  }
  private ordinaryIndex(cwd: string, candidate: CandidateV2): boolean {
    const index = readFileSync(resolve(cwd, this.git(cwd, "rev-parse", "--git-path", "index")));
    const hashBytes = candidate.commit.length / 2, end = index.length - hashBytes;
    if (end < 12 || index.toString("ascii", 0, 4) !== "DIRC") return false;
    const version = index.readUInt32BE(4), count = index.readUInt32BE(8);
    if (![2, 3, 4].includes(version)) return false;
    let offset = 12;
    for (let i = 0; i < count; i++) {
      const start = offset, flagsAt = start + 40 + hashBytes;
      if (flagsAt + 2 > end) return false;
      // Only the path-length bits are ordinary: no assume-valid, extended
      // (skip-worktree/intent-to-add), or unmerged-stage entries.
      if (index.readUInt16BE(flagsAt) & 0xf000) return false;
      offset = flagsAt + 2;
      if (version === 4) {
        let bytes = 0;
        do { if (offset >= end || ++bytes > 5) return false; } while (index[offset++] & 0x80);
      }
      const nul = index.indexOf(0, offset);
      if (nul < offset || nul >= end) return false;
      offset = version === 4 ? nul + 1 : start + Math.ceil((nul + 1 - start) / 8) * 8;
    }
    while (offset < end) {
      if (offset + 8 > end) return false;
      const extension = index.toString("ascii", offset, offset + 4), size = index.readUInt32BE(offset + 4);
      // Cache-tree and entry-offset accelerators do not suppress file checks.
      // In particular FSMN, split/sparse indexes and untracked caches are not
      // ordinary verification evidence, even if Git would silently ignore them.
      if (!["TREE", "EOIE", "IEOT"].includes(extension)) return false;
      offset += 8 + size;
    }
    return offset === end;
  }
  private trackedBytesMatch(cwd: string, candidate: CandidateV2): boolean {
    // Do not trust cached stat data, core.fileMode, or clean filters as no-edit
    // evidence. This profile requires raw checkout bytes/modes to match the tree.
    // This is a settled-workspace check, not a sandbox against same-UID writers.
    const tree = execFileSync("git", [...(this.gitOptions.length > 0 ? safeGitOptionsV2(cwd, this.gitOptions) : this.gitOptions), "ls-tree", "-rz", candidate.tree], { cwd, env: gitEnvironmentV2(), stdio: ["ignore", "pipe", "pipe"] });
    const text = tree.toString("utf8");
    if (!Buffer.from(text).equals(tree)) return false; // Ambiguous path decoding: retain.
    return text.split("\0").filter(Boolean).every(row => {
      const entry = /^(100644|100755|120000) blob ([0-9a-f]+)\t([\s\S]+)$/.exec(row);
      if (!entry) return false;
      const [, mode, oid, path] = entry, parts = path.split("/");
      if (parts.some(part => !part || part === "." || part === "..")) return false;
      for (let i = 1; i < parts.length; i++) if (!lstatSync(join(cwd, ...parts.slice(0, i))).isDirectory()) return false;
      const file = join(cwd, path), stat = lstatSync(file);
      if (mode === "120000" ? !stat.isSymbolicLink() : !stat.isFile() || Boolean(stat.mode & 0o100) !== (mode === "100755")) return false;
      const bytes = mode === "120000" ? readlinkSync(file, { encoding: "buffer" }) : readFileSync(file);
      const hash = createHash(candidate.commit.length === 64 ? "sha256" : "sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
      return hash === oid;
    });
  }
}

/** Drop all inherited Git selectors/config injection (including indexed config
 * entries), not just GIT_WORK_TREE. Cwd selects the repository. User/system Git
 * config is disabled for both native inspection and command descendants; local
 * repository config remains part of the trusted repository profile. */
export function gitEnvironmentV2(): NodeJS.ProcessEnv {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
  return { ...env, PI_DAG_V2_GIT_HOOK_ENABLED: "false", GIT_TERMINAL_PROMPT: "0", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null", GIT_NO_REPLACE_OBJECTS: "1", GIT_NO_LAZY_FETCH: "1", GIT_ATTR_NOSYSTEM: "1" };
}

export type SessionIdentityV2 = { pid: number; processStart: string; token?: string };
type ProcessJournalV2 = { version: 1 | 2; requestId: string; identity: SessionIdentityV2; state: "running" | "BLOCKED"; diagnostic?: string };
const journalPathV2 = (cwd: string) => join(dirname(cwd), "command-process.json");
async function writeProcessJournalV2(cwd: string, journal: ProcessJournalV2): Promise<void> {
  const path = journalPathV2(cwd), temp = `${path}.tmp`;
  const file = await open(temp, "w", 0o600);
  try { await file.writeFile(JSON.stringify(journal)); await file.sync(); } finally { await file.close(); }
  await rename(temp, path);
  for (const path of [dirname(cwd), dirname(dirname(cwd))]) {
    const dir = await open(path, "r"); try { await dir.sync(); } finally { await dir.close(); }
  }
}
async function readProcessJournalV2(cwd: string): Promise<ProcessJournalV2 | null> {
  try {
    const journal = JSON.parse(await readFile(journalPathV2(cwd), "utf8"));
    requireV2((journal.version === 1 || journal.version === 2 && validTokenV2(journal.identity?.token)) && Number.isSafeInteger(journal.identity?.pid) && journal.identity.pid > 0 && journal.identity.pid <= 2147483647
      && typeof journal.identity.processStart === "string" && /^[0-9a-f-]{36}:\d+$/.test(journal.identity.processStart)
      && ["running", "BLOCKED"].includes(journal.state), "INVALID_COMMAND_PROCESS_JOURNAL");
    return journal;
  } catch (e: any) { if (e.code === "ENOENT") return null; throw e; }
}

const validTokenV2 = (token: unknown): token is string => typeof token === "string" && /^[0-9a-f-]{36}$/.test(token);
const outcomePathV2 = (directory: string, token: string) => join(directory, `command-outcome-${token}.json`);
type ProcessOutcomeV2 = {
  version: 2; token: string; identity: { pid: number; processStart: string }; cwd: string; extinct: true;
  invoked: boolean; exitCode: number | null; signal: string | null; interrupted: boolean; diagnostic: string;
};
export async function readOutcomeV2(directory: string, cwd: string, identity: SessionIdentityV2): Promise<ProcessOutcomeV2 | null> {
  if (!validTokenV2(identity.token)) return null; // Legacy journals have no reaping proof.
  try {
    const value = JSON.parse(await readFile(outcomePathV2(directory, identity.token), "utf8"));
    requireV2(value.version === 2 && value.token === identity.token && value.identity?.pid === identity.pid
      && value.identity.processStart === identity.processStart && value.cwd === resolve(cwd) && value.extinct === true
      && typeof value.invoked === "boolean" && typeof value.interrupted === "boolean" && typeof value.diagnostic === "string"
      && (value.exitCode === null || Number.isInteger(value.exitCode) && value.exitCode >= 0 && value.exitCode <= 255)
      && (value.signal === null || typeof value.signal === "string" && /^SIG[A-Z0-9]+$/.test(value.signal))
      && !(value.exitCode !== null && value.signal !== null)
      && (!value.invoked || value.exitCode !== null || value.signal !== null), "INVALID_COMMAND_PROCESS_OUTCOME");
    // Complete durability ourselves if the supervisor died between rename and
    // directory fsync. The immutable receipt is written only after ECHILD.
    const file = await open(outcomePathV2(directory, identity.token), "r");
    try { await file.sync(); } finally { await file.close(); }
    const dir = await open(directory, "r"); try { await dir.sync(); } finally { await dir.close(); }
    return value;
  } catch (e: any) { if (e.code === "ENOENT") return null; throw e; }
}
const readProcessOutcomeV2 = (cwd: string, identity: SessionIdentityV2) => readOutcomeV2(dirname(cwd), cwd, identity);

type ArgvObservationV2 = {
  stdout: string; stderr: string; truncated: boolean; exitCode: number | null; signal: string | null; invoked: boolean;
  settled: boolean; interrupted: boolean;
};
/** Linux/Python3 subreaper, trusted same-session commands, not a sandbox. Only a
 * nonce/identity-bound, fsynced ECHILD outcome proves descendant extinction.
 * The helper path is package-relative, never relative to a candidate/worktree. */
export async function runArgvV2(argv: readonly string[], cwd: string, signal?: AbortSignal, options: {
  timeoutMs?: number;
  protocolDirectory?: string;
  inheritedLockFd?: number;
  disableGitHooks?: boolean;
  launch?: (identity: SessionIdentityV2, launch: () => void) => Promise<void>;
} = {}): Promise<ArgvObservationV2> {
  requireV2(process.platform === "linux", "UNSUPPORTED_COMMAND_PROCESS_PROFILE");
  const directory = options.protocolDirectory ?? await mkdtemp(join(tmpdir(), "dag-v2-command-"));
  const token = randomUUID();
  let stdout = "", stderr = "", truncated = false, interrupted = false, launched = false, settled = false;
  let ready = false, exited = false, closed = false, failure: unknown, readyText = "";
  let identity: SessionIdentityV2 | undefined, abortAt: number | undefined, outcome: ProcessOutcomeV2 | null = null;
  // Directory hooks and every already-configured hook are disabled for argv's
  // descendants, including events unused by native integration (e.g. pre-push).
  // Commands are trusted local code, not a sandbox: new config is rechecked by
  // the runner after settlement, before any observation/cleanup Git operation.
  const hookConfig = options.disableGitHooks ? [
    ["core.hooksPath", "/dev/null"], ["core.fsmonitor", "false"],
    ...configuredGitHooksV2(cwd).map(name => [`hook.${name}.enabled`, "false"]),
  ] : [];
  const hookEnvironment = options.disableGitHooks ? Object.fromEntries([
    ["GIT_CONFIG_COUNT", String(hookConfig.length)],
    ...hookConfig.flatMap(([key, value], i) => [[`GIT_CONFIG_KEY_${i}`, key], [`GIT_CONFIG_VALUE_${i}`, value]]),
  ]) : {};
  const child = spawn("python3", ["-I", "-B", fileURLToPath(new URL("./command-supervisor.py", import.meta.url)), JSON.stringify({
    argv, token, outcome: outcomePathV2(directory, token), timeoutMs: options.timeoutMs ?? null,
  })], { cwd, env: { ...gitEnvironmentV2(), ...hookEnvironment }, shell: false, detached: true, stdio: ["pipe", "pipe", "pipe", "pipe", ...(options.inheritedLockFd === undefined ? [] : [options.inheritedLockFd])] });
  const append = (old: string, next: Buffer) => { const text = old + next.toString("utf8"); truncated ||= text.length > 16384; return bounded(text); };
  child.stdout!.on("data", b => stdout = append(stdout, b)); child.stderr!.on("data", b => stderr = append(stderr, b));
  child.stdio[3]!.on("data", (b: Buffer) => {
    readyText += b.toString("utf8");
    if (readyText.includes("\n")) {
      try {
        const value = JSON.parse(readyText);
        requireV2(!ready && value.ready?.pid === child.pid && typeof value.ready.processStart === "string", "INVALID_COMMAND_SUPERVISOR_READY");
        identity = { ...value.ready, token }; ready = true;
      } catch (e) { failure = e; }
    }
  });
  child.on("error", error => { failure = error; });
  child.stdin!.on("error", error => { failure ??= error; });
  child.once("exit", () => { exited = true; });
  child.once("close", () => { closed = true; });
  const send = (message: object) => { if (!child.stdin!.destroyed) child.stdin!.write(`${JSON.stringify(message)}\n`); };
  const abort = () => {
    interrupted = true;
    if (abortAt === undefined) { abortAt = performance.now(); send({ action: "abort" }); }
  };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  const deadline = options.timeoutMs === undefined ? Infinity : performance.now() + options.timeoutMs;
  const started = performance.now();
  try {
    while (true) {
      if (performance.now() >= deadline) abort();
      if (ready && !launched && !interrupted && !failure && !exited) {
        requireV2(identity && await processIdentityV2(identity.pid) === identity.processStart, "COMMAND_SUPERVISOR_IDENTITY_MISMATCH");
        const launch = () => {
          if (signal?.aborted || performance.now() >= deadline) { abort(); return; }
          requireV2(!launched, "COMMAND_ALREADY_LAUNCHED"); launched = true;
          send({ action: "launch", token });
        };
        try { if (options.launch) await options.launch(identity, launch); else launch(); }
        catch (e) { failure = e; abort(); }
      }
      if (failure) abort();
      if (!ready && performance.now() - started > 10000) { failure = Error("COMMAND_SUPERVISOR_NOT_READY"); abort(); }
      if (identity) outcome = await readOutcomeV2(directory, cwd, identity);
      // Exit can race the preceding ENOENT read. Once exit is observed, read the
      // durable receipt again rather than depending on a lost IPC acknowledgement.
      if (!outcome && identity && (exited || closed)) outcome = await readOutcomeV2(directory, cwd, identity);
      if (outcome && closed) { settled = true; break; }
      if (!outcome && (exited || closed)) {
        // No launch message means argv could not have run, including missing
        // Python/capabilities. After launch, death without ECHILD is ambiguous.
        settled = !launched;
        failure ??= Error(launched ? "COMMAND_SUPERVISOR_LOST_WITHOUT_EXTINCTION" : "COMMAND_SUPERVISOR_UNAVAILABLE");
        break;
      }
      if (abortAt !== undefined && performance.now() - abortAt > 5000) throw Error("COMMAND_REAPING_STILL_PENDING");
      await delay(20);
    }
  } catch (e) { failure = e; }
  finally {
    signal?.removeEventListener("abort", abort);
    child.stdin!.destroy();
    if (!settled) {
      // EOF asks the independent supervisor to finish TERM/KILL and reaping,
      // even when this owner cannot observe it. Never kill the reaper itself.
      child.stdout?.destroy(); child.stderr?.destroy(); child.stdio[3]?.destroy(); child.unref();
    }
    if (settled && !options.protocolDirectory) await rm(directory, { recursive: true });
  }
  interrupted ||= outcome?.interrupted ?? false;
  if (outcome?.diagnostic) stderr = bounded(`${stderr}\n${outcome.diagnostic}`);
  if (failure) stderr = bounded(`${stderr}\n${String(failure)}`);
  if (interrupted) stderr = bounded(`${stderr}\nCommand aborted or execution deadline expired; descendant reaping ${settled ? "confirmed" : "BLOCKED"}.`);
  return { stdout, stderr, truncated, exitCode: outcome?.exitCode ?? null, signal: outcome?.signal ?? null,
    invoked: outcome?.invoked ?? launched, settled, interrupted };
}
