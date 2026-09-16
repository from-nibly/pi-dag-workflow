import { spawn, execFileSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { requireV2, sameV2, validateShapeV2 } from "../planning/v2.ts";
import { StoreV2, processIdentityV2 } from "./store.ts";
import { ExecutionRequestV2Schema, ExecutionResultV2Schema, type CandidateV2, type ExecutionRequestV2, type ExecutionResultV2, type CommandJobV2, type FindingV2 } from "./lifecycle-schema.ts";
import { auditResultV2 } from "./lifecycle.ts";

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
 * blindly respawns. The owner-dead ambiguous job requires an explicit observed
 * settlement via reconcileInterrupted (and remains BLOCKED, not invented PASS).
 * Commands are argv-only and run outside the run/store lock. */
export class CommandRunnerV2 implements ResultsV2, CandidateInspectorV2 {
  readonly store: StoreV2;
  readonly repository: string;
  readonly producers: ReadonlyMap<string, TrustedProducerV2>;
  readonly environment: string;
  constructor(store: StoreV2, repository: string, producers: ReadonlyMap<string, TrustedProducerV2> = new Map(), environment = "node-local") {
    this.store = store; this.repository = repository; this.producers = producers; this.environment = environment;
  }
  private git(cwd: string, ...args: string[]): string {
    return execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } }).trim();
  }
  async inspect(candidate: Readonly<CandidateV2>): Promise<void> {
    requireV2(this.git(this.repository, "rev-parse", "--verify", `${candidate.commit}^{commit}`) === candidate.commit
      && this.git(this.repository, "rev-parse", `${candidate.commit}^{tree}`) === candidate.tree, "NATIVE_CANDIDATE_MISMATCH");
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
      requireV2(run?.status === "active" && node?.generation === request.generation && node.lifecycle?.round === request.round
        && node.lifecycle.executions.some(e => e.status === "intent" && sameV2(e.request, request)), "CURRENT_EXECUTION_INTENT_REQUIRED");
      requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
      jobs[request.id] = { request, owner: { pid: process.pid, processStart: identity }, status: "running" };
      await publish(); return true;
    });
    if (!launch) return;
    const result = await this.execute(request, signal);
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
      requireV2(await processIdentityV2(job.owner.pid) !== job.owner.processStart, "EXECUTOR_STILL_ALIVE");
      await settled(structuredClone(request), structuredClone(job));
      const result = this.emptyResult(request);
      result.diagnostic = "Executor died without a durable outcome; process tree/effects settled. Explicit bounded infrastructure retry required.";
      result.findings = [{ id: "executor-lost", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id, fingerprint: "executor-lost", detail: result.diagnostic }];
      job.result = result; job.status = "settled"; await publish();
    });
  }
  /** Recheck the generation while holding the launch lock through actual start.
   * The workspace is durable before invocation so dead-owner reconciliation can
   * locate the exact process workspace even when there is no result yet. */
  private async invoke<T>(request: ExecutionRequestV2, cwd: string, start: () => Promise<T>): Promise<T> {
    let pending: Promise<T> | undefined, gateError: unknown;
    try {
      await this.store.transaction(async (s, publish) => {
        const run = s.runs[request.runId], node = run?.nodes[request.itemId], job = s.executions?.[request.id];
        requireV2(job && sameV2(job.request, request) && !job.result, "EXECUTION_REQUEST_CONFLICT");
        requireV2(run.status === "active" && node?.generation === request.generation && node.lifecycle?.round === request.round
          && node.lifecycle.executions.some(e => e.status === "intent" && sameV2(e.request, request)), "EXECUTION_FENCED_BEFORE_INVOCATION");
        requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
        job.workspace = cwd; await publish();
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
  private async execute(request: ExecutionRequestV2, signal?: AbortSignal): Promise<ExecutionResultV2> {
    const result = this.emptyResult(request), start = performance.now();
    let root: string | undefined, cwd: string | undefined, added = false;
    try {
      requireV2(request.check.environment === this.environment, "EXECUTION_ENVIRONMENT_UNAVAILABLE");
      await this.inspect(request.candidate);
      requireV2(!signal?.aborted, "EXECUTION_CANCELLED");
      root = await mkdtemp(join(tmpdir(), "dag-v2-check-")); cwd = join(root, "candidate");
      this.git(this.repository, "-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach", cwd, request.candidate.commit); added = true;
      result.workspace.cleanBefore = this.clean(cwd, request.candidate);
      requireV2(result.workspace.cleanBefore, "UNCLEAN_EXECUTION_WORKSPACE");
      requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
      const procedure = request.check.procedure;
      if (procedure.kind === "command") {
        const observed = await this.invoke(request, cwd, () => runArgvV2(procedure.argv, cwd!, signal).then(observed => {
          result.executor.invoked = observed.invoked; return observed;
        }));
        result.stdout = observed.stdout; result.stderr = observed.stderr; result.truncated = observed.truncated;
        result.exitCode = observed.exitCode; result.signal = observed.signal;
        result.executor.invoked = observed.invoked;
        result.disposition = !observed.invoked ? "BLOCKED" : observed.exitCode === 0 && !observed.signal ? "PASS" : "FAIL";
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
        if (request.stage === 2 || request.stage === 5) requireV2(observed.context, "PRODUCER_INDEPENDENT_CONTEXT_REQUIRED");
        if (observed.context) { result.executor.contextId = observed.context.id; result.executor.lineage = observed.context.lineage; }
        result.disposition = observed.disposition; result.stdout = bounded(observed.observation); result.truncated = observed.observation.length > 16384;
        result.findings = observed.findings; result.diagnostic = `producer=${procedure.producerId}: ${bounded(observed.observation)}`;
      }
      result.workspace.cleanAfter = this.clean(cwd, request.candidate);
      if (!result.workspace.cleanAfter) { result.disposition = "FAIL"; result.diagnostic += "; candidate/workspace changed during no-edit verification"; }
      if (result.disposition === "PASS" && result.findings.some(f => f.severity === "blocking")) result.disposition = "FAIL";
    } catch (error) {
      result.disposition = "BLOCKED"; result.diagnostic = bounded(`${result.diagnostic}\n${String(error)}`);
      result.findings = [{ id: "execution-unavailable", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id,
        fingerprint: `execution-unavailable:${request.check.procedure.kind}`, detail: result.diagnostic }];
    } finally {
      // Never force-clean a dirty check worktree: preserve it for diagnosis.
      if (added && cwd && result.workspace.cleanAfter) {
        try { this.git(this.repository, "worktree", "remove", cwd); if (root) await rm(root, { recursive: true }); }
        catch (e) { result.diagnostic += `; cleanup retained: ${String(e)}`; }
      } else if (!added && root) await rm(root, { recursive: true });
      else if (cwd) result.diagnostic += `; retained workspace: ${cwd}`;
      result.endedAt = Date.now(); result.durationMs = Math.max(0, Math.round(performance.now() - start)); result.diagnostic = bounded(result.diagnostic);
    }
    try { validateShapeV2(ExecutionResultV2Schema, result); auditResultV2(request, result); return result; }
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
      return failure;
    }
  }
  private clean(cwd: string, candidate: CandidateV2): boolean {
    return this.git(cwd, "rev-parse", "HEAD") === candidate.commit && this.git(cwd, "rev-parse", "HEAD^{tree}") === candidate.tree
      && this.git(cwd, "status", "--porcelain=v1", "--untracked-files=all") === "";
  }
}

export async function runArgvV2(argv: readonly string[], cwd: string, signal?: AbortSignal): Promise<{
  stdout: string; stderr: string; truncated: boolean; exitCode: number | null; signal: string | null; invoked: boolean;
}> {
  return new Promise(resolve => {
    let stdout = "", stderr = "", truncated = false, invoked = false;
    const child = spawn(argv[0], argv.slice(1), { cwd, shell: false, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    const append = (old: string, next: Buffer) => { const text = old + next.toString("utf8"); truncated ||= text.length > 16384; return bounded(text); };
    child.stdout.on("data", b => stdout = append(stdout, b)); child.stderr.on("data", b => stderr = append(stderr, b));
    child.once("spawn", () => { invoked = true; if (signal?.aborted) abort(); });
    child.once("error", e => { stderr = bounded(`${stderr}\n${e.message}`); });
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    const abort = () => {
      if (!child.pid) return;
      try { process.kill(-child.pid, "SIGTERM"); } catch {}
      killTimer ??= setTimeout(() => { try { process.kill(-child.pid!, "SIGKILL"); } catch {} }, 1000);
    };
    signal?.addEventListener("abort", abort, { once: true });
    child.once("close", (exitCode, killedBy) => { signal?.removeEventListener("abort", abort); if (killTimer) clearTimeout(killTimer); resolve({ stdout, stderr, truncated, exitCode, signal: killedBy, invoked }); });
  });
}
