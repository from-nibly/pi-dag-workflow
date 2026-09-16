import { spawn, execFileSync } from "node:child_process";
import { mkdtemp, rm, readFile, readdir, open, rename } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
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
 * blindly respawns. An owner-dead or process-ambiguous job requires an explicit
 * observed settlement via reconcileInterrupted (BLOCKED, not invented PASS).
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
    return execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: gitEnvironmentV2() }).trim();
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
      const journal = job.workspace && request.check.procedure.kind === "command" ? await readProcessJournalV2(job.workspace) : null;
      if (journal) requireV2(journal.requestId === request.id, "COMMAND_PROCESS_JOURNAL_MISMATCH");
      requireV2(await processIdentityV2(job.owner.pid) !== job.owner.processStart || journal?.state === "BLOCKED", "EXECUTOR_STILL_ALIVE");
      await settled(structuredClone(request), structuredClone(job));
      // A caller's effect reconciliation cannot override a known live command group.
      if (journal) requireV2((await sessionMembersV2(journal.identity)).length === 0, "COMMAND_PROCESS_SETTLEMENT_REQUIRED");
      const result = this.emptyResult(request);
      result.diagnostic = "Executor died without a durable outcome; process tree/effects settled. Explicit bounded infrastructure retry required.";
      result.findings = [{ id: "executor-lost", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id, fingerprint: "executor-lost", detail: result.diagnostic }];
      job.result = result; job.status = "settled"; await publish();
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
        requireV2(run.status === "active" && node?.generation === request.generation && node.lifecycle?.round === request.round
          && node.lifecycle.executions.some(e => e.status === "intent" && sameV2(e.request, request)), "EXECUTION_FENCED_BEFORE_INVOCATION");
        requireV2(Date.now() < request.authority.expiresAt, "AUTHORITY_EXPIRED");
        job.workspace = cwd; await publish();
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
  private async execute(request: ExecutionRequestV2, signal?: AbortSignal): Promise<ExecutionResultV2> {
    const result = this.emptyResult(request), start = performance.now();
    let root: string | undefined, cwd: string | undefined, added = false, unsettled = false;
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
        const observed = await this.invoke(request, cwd, () => runArgvV2(procedure.argv, cwd!, signal, {
          timeoutMs: Math.max(1, request.authority.expiresAt - Date.now()),
          launch: async (identity, launch) => {
            // The gated session leader cannot invoke argv before this journal is
            // synced and the current intent has been rechecked under the lock.
            for (let retry = 0; ; retry++) {
              try {
                await this.invoke(request, cwd!, async () => { launch(); },
                  () => writeProcessJournalV2(cwd!, { version: 1, requestId: request.id, identity, state: "running" }));
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
      result.workspace.cleanAfter = this.clean(cwd, request.candidate);
      if (!result.workspace.cleanAfter) { result.disposition = "FAIL"; result.diagnostic += "; candidate/workspace changed during no-edit verification"; }
      if (result.disposition === "PASS" && result.findings.some(f => f.severity === "blocking")) result.disposition = "FAIL";
    } catch (error) {
      result.disposition = "BLOCKED"; result.diagnostic = bounded(`${result.diagnostic}\n${String(error)}`);
      result.findings = [{ id: "execution-unavailable", kind: "infrastructure_failure", severity: "blocking", materiality: "local", subject: request.check.id,
        fingerprint: `execution-unavailable:${request.check.procedure.kind}`, detail: result.diagnostic }];
    } finally {
      // Never force-clean a dirty check worktree: preserve it for diagnosis.
      if (!unsettled && added && cwd && result.workspace.cleanAfter) {
        try { this.git(this.repository, "worktree", "remove", cwd); if (root) await rm(root, { recursive: true }); }
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
    return resolve(this.git(cwd, "rev-parse", "--show-toplevel")) === resolve(cwd)
      && this.git(cwd, "rev-parse", "HEAD") === candidate.commit && this.git(cwd, "rev-parse", "HEAD^{tree}") === candidate.tree
      && this.git(cwd, "status", "--porcelain=v1", "--untracked-files=all") === "";
  }
}

/** Drop all inherited Git selectors/config injection (including indexed config
 * entries), not just GIT_WORK_TREE. Cwd selects the repository. User/system Git
 * config is disabled for both native inspection and command descendants; local
 * repository config remains part of the trusted repository profile. */
export function gitEnvironmentV2(): NodeJS.ProcessEnv {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
  return { ...env, GIT_TERMINAL_PROMPT: "0", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null" };
}

type SessionIdentityV2 = { pid: number; processStart: string };
type ProcessJournalV2 = { version: 1; requestId: string; identity: SessionIdentityV2; state: "running" | "BLOCKED"; diagnostic?: string };
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
    requireV2(journal.version === 1 && Number.isSafeInteger(journal.identity?.pid) && journal.identity.pid > 0 && journal.identity.pid <= 2147483647
      && typeof journal.identity.processStart === "string" && /^[0-9a-f-]{36}:\d+$/.test(journal.identity.processStart)
      && ["running", "BLOCKED"].includes(journal.state), "INVALID_COMMAND_PROCESS_JOURNAL");
    return journal;
  } catch (e: any) { if (e.code === "ENOENT") return null; throw e; }
}

/** Linux, same PID namespace, readable /proc, cooperative commands that do not
 * escape their session. The live session anchor prevents ID reuse while argv
 * runs. Session membership also covers descendants that change process groups.
 * Zombies have no executable effects. This is process management, NOT a sandbox
 * against hostile same-UID code, setsid/namespace escape or external effects. */
async function sessionMembersV2(identity: SessionIdentityV2): Promise<{ pid: number; group: number }[]> {
  requireV2(process.platform === "linux", "UNSUPPORTED_COMMAND_PROCESS_PROFILE");
  const boot = (await readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim();
  if (!identity.processStart.startsWith(`${boot}:`)) return [];
  const current = await processIdentityV2(identity.pid);
  requireV2(!current || current === identity.processStart, "COMMAND_SESSION_IDENTITY_REUSED");
  const members: { pid: number; group: number }[] = [];
  for (const name of await readdir("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    let stat: string;
    try { stat = await readFile(`/proc/${name}/stat`, "utf8"); }
    catch (e: any) { if (e.code === "ENOENT" || e.code === "ESRCH") continue; throw e; }
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    requireV2(fields.length >= 20 && /^\d+$/.test(fields[3]) && /^\d+$/.test(fields[2]), "INVALID_COMMAND_PROCESS_STAT");
    if (Number(fields[3]) === identity.pid && !["Z", "X"].includes(fields[0])) members.push({ pid: Number(name), group: Number(fields[2]) });
  }
  return members;
}

// A gate prevents the actual argv from starting before its session identity is
// durable. The anchor stays alive after the command closes, pins the session ID,
// and leaves the original command's exit/signal intact in the IPC observation.
const commandAnchorV2 = `
const {spawn}=require('node:child_process');
let launched=false;
process.on('SIGTERM',()=>{});
process.on('disconnect',()=>{ if(!launched)process.exit(0); });
process.on('message',message=>{
  if(message==='finish')process.exit(0);
  if(message!=='launch'||launched)return;
  launched=true;
  let invoked=false;
  const child=spawn(process.argv[1],process.argv.slice(2),{stdio:['ignore',1,2]});
  child.once('spawn',()=>{invoked=true;});
  child.once('error',error=>{process.stderr.write(error.message+'\\n');});
  child.once('close',(exitCode,signal)=>{
    if(process.connected)process.send({exitCode,signal,invoked});
  });
});
process.send('ready');
`;

type ArgvObservationV2 = {
  stdout: string; stderr: string; truncated: boolean; exitCode: number | null; signal: string | null; invoked: boolean;
  settled: boolean; interrupted: boolean;
};
export async function runArgvV2(argv: readonly string[], cwd: string, signal?: AbortSignal, options: {
  timeoutMs?: number;
  launch?: (identity: SessionIdentityV2, launch: () => void) => Promise<void>;
} = {}): Promise<ArgvObservationV2> {
  requireV2(process.platform === "linux", "UNSUPPORTED_COMMAND_PROCESS_PROFILE");
  let stdout = "", stderr = "", truncated = false, invoked = false, interrupted = false;
  let exitCode: number | null = null, killedBy: string | null = null;
  let ready = false, exited = false, closed = false, observed = false, finishing = false, failure: unknown;
  let identity: SessionIdentityV2 | undefined, abortAt: number | undefined, launched = false;
  const child = spawn(process.execPath, ["-e", commandAnchorV2, "--", ...argv], {
    cwd, env: gitEnvironmentV2(), shell: false, detached: true, stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const append = (old: string, next: Buffer) => { const text = old + next.toString("utf8"); truncated ||= text.length > 16384; return bounded(text); };
  child.stdout!.on("data", b => stdout = append(stdout, b)); child.stderr!.on("data", b => stderr = append(stderr, b));
  child.on("message", message => {
    if (message === "ready") ready = true;
    else if (message && typeof message === "object" && "invoked" in message) {
      const result = message as { exitCode: number | null; signal: string | null; invoked: boolean };
      observed = true; invoked = result.invoked; exitCode = result.exitCode; killedBy = result.signal;
    }
  });
  child.on("error", error => { failure = error; });
  child.once("exit", () => { exited = true; });
  child.once("close", () => { closed = true; });
  const abort = () => { interrupted = true; abortAt ??= performance.now(); };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  const deadline = options.timeoutMs === undefined ? Infinity : performance.now() + options.timeoutMs;
  const started = performance.now();
  let termSent = false, killSent = false, settled = false;
  try {
    while (true) {
      if (performance.now() >= deadline) abort();
      if (!identity && child.pid) {
        const processStart = await processIdentityV2(child.pid);
        if (processStart) identity = { pid: child.pid, processStart };
      }
      if (ready && !launched && !interrupted && !failure) {
        requireV2(identity, "COMMAND_SESSION_IDENTITY_UNAVAILABLE");
        const launch = () => { if (signal?.aborted || performance.now() >= deadline) { abort(); return; } launched = true; child.send("launch"); };
        try { if (options.launch) await options.launch(identity, launch); else launch(); }
        catch (e) { failure = e; abort(); }
      }
      if (failure) abort();
      if (!ready && performance.now() - started > 10000) { failure = Error("COMMAND_ANCHOR_NOT_READY"); abort(); }
      const members = identity ? await sessionMembersV2(identity) : [];
      // The observation is not settlement: the disconnected-stdio descendant
      // may still be live. Keep both monitoring and escalation after leader close.
      if (identity && observed && members.every(p => p.pid === identity!.pid) && !finishing) {
        finishing = true;
        if (child.connected) child.send("finish");
      }
      if (abortAt !== undefined && identity) {
        const kill = performance.now() - abortAt >= 1000;
        if (kill || !termSent) {
          for (const group of new Set(members.map(p => p.group))) {
            try { process.kill(-group, kill ? "SIGKILL" : "SIGTERM"); }
            catch (e: any) { if (e.code !== "ESRCH") throw e; }
          }
          if (kill) killSent = true; else termSent = true;
        }
      }
      if (closed && members.length === 0) { settled = true; break; }
      if (exited && !finishing && !interrupted) { failure = Error("COMMAND_ANCHOR_LOST"); abort(); }
      if (abortAt !== undefined && performance.now() - abortAt > 5000) throw Error("COMMAND_SESSION_STILL_LIVE_AFTER_SIGKILL");
      await delay(20);
    }
  } catch (e) {
    // Never convert an unreadable/reused/unkillable session to a durable outcome.
    // Leave the anchor/workspace/identity intact for independent reconciliation.
    failure = e;
  } finally {
    signal?.removeEventListener("abort", abort);
    if (!settled) {
      child.stdout?.destroy(); child.stderr?.destroy();
      if (child.connected) child.disconnect();
      child.unref();
    }
  }
  if (failure) stderr = bounded(`${stderr}\n${String(failure)}`);
  if (interrupted) stderr = bounded(`${stderr}\nCommand aborted or execution deadline expired; process-session settlement ${settled ? "confirmed" : "BLOCKED"}.`);
  // If SIGKILL took the IPC anchor before its observation, invocation/outcome is
  // ambiguous, never a success. Preserve the real command result when available.
  if (!observed && launched) { invoked = true; killedBy = killSent ? "SIGKILL" : killedBy; }
  return { stdout, stderr, truncated, exitCode, signal: killedBy, invoked, settled, interrupted };
}
