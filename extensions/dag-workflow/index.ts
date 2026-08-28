import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { readFile, realpath, stat } from "node:fs/promises";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { findLatestRun, loadRun, readDag, runDir, summarizeRun, validateDag } from "./dag.ts";
import { renderDagDiagram } from "./diagram.ts";
import { registerCanonicalDagRuntime } from "./dag-runtime/integration.ts";
import { DagConductorServiceV1 } from "./dag-runtime/conductor.ts";
import { DagReducerGitIntegrationDriverV1 } from "./dag-runtime/integration-driver.ts";
import { canonicalHash, parseStrictJson } from "./dag-runtime/common.ts";
import { createBuiltInLifecycleProcedureAdapterV1, registerDagPlanningIntegrationV1 } from "./planning/index.ts";
import { registerProjectModelIntegration } from "./project-model/integration.ts";
import { isWorkerChildRole, registerWorkerChild } from "./worker-runtime/child-report.ts";
import { registerWorkerRuntime } from "./worker-runtime/integration.ts";
import { listWorkerRecords, readLogTail } from "./sessions.ts";
import { DEFAULT_DAG_PATH } from "./types.ts";

type CommandContext = any;
const execFileAsync = promisify(execFile);

export function privateCandidateRefV1(runNonce: string, stageAttemptId: string): string {
  const nonceSegment = canonicalHash({ kind: "dag_private_candidate_run", runNonce }).slice("sha256:".length);
  const attemptSegment = canonicalHash({ kind: "dag_private_candidate_attempt", stageAttemptId }).slice("sha256:".length);
  return `refs/pi-dag/candidates/${nonceSegment}/${attemptSegment}`;
}

export async function sealPrivateCandidateRefV1(cwd: string, runNonce: string, stageAttemptId: string, commit: string, signal?: AbortSignal): Promise<string> {
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(commit)) throw new Error("Private candidate ref requires one exact Git object ID");
  const privateRef = privateCandidateRefV1(runNonce, stageAttemptId);
  const env = { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
  await execFileAsync("git", ["check-ref-format", privateRef], { cwd, env, maxBuffer: 1024 * 1024, signal });
  const existingRef = await execFileAsync("git", ["rev-parse", "--verify", privateRef], { cwd, env, maxBuffer: 1024 * 1024, signal }).then(({ stdout }) => stdout.trim(), (error) => { if (error?.code === 128) return null; throw error; });
  if (existingRef && existingRef !== commit) throw new Error("Private candidate ref exists with conflicting immutable candidate identity");
  if (!existingRef) await execFileAsync("git", ["update-ref", privateRef, commit, "0".repeat(commit.length)], { cwd, env, maxBuffer: 1024 * 1024, signal });
  return privateRef;
}

function splitArgs(input: string): string[] {
  const values: string[] = [];
  const pattern = /"([^"]*)"|'([^']*)'|([^\s]+)/g;
  let match;
  while ((match = pattern.exec(input))) values.push(match[1] ?? match[2] ?? match[3]);
  return values;
}

function parseArgs(args: string) {
  const tokens = splitArgs(args);
  const command = tokens.shift() ?? "help";
  const options: Record<string, string | boolean> = {};
  const positional: string[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index];
    if (!token.startsWith("--")) { positional.push(token); continue; }
    const name = token.slice(2);
    const next = tokens[index + 1];
    if (next && !next.startsWith("--")) { options[name] = next; index += 1; }
    else options[name] = true;
  }
  return { command, rest: positional.join(" "), options };
}

function ok(content: string, details: Record<string, unknown> = {}) {
  return { content: [{ type: "text" as const, text: content }], details };
}

function catalogProcedureAdapter(workerManager: any) {
  const builtIn = () => workerManager.context?.cwd ? createBuiltInLifecycleProcedureAdapterV1({ repositoryRoot: workerManager.context.cwd }) : null;
  const exactEnvironment = {
    LC_ALL: "C",
    LANG: "C",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_TERMINAL_PROMPT: "0",
    GIT_OPTIONAL_LOCKS: "0",
  };
  return {
    adapterKind: "immutable-catalog-command-v1" as const,
    allowsProcedure(procedure: any) {
      const internal = builtIn();
      if (internal?.allowsProcedure?.(procedure)) return true;
      const executable = procedure?.executable;
      return Boolean(executable && executable.argv?.length > 0 && executable.argv[0].startsWith("/") && executable.environmentHash === canonicalHash(exactEnvironment) && executable.environmentProfileHash === procedure.environmentProfileHash && executable.readOnly === procedure.readOnly && executable.noEdit === procedure.readOnly && executable.timeoutMs > 0);
    },
    async executeExact(input: any) {
      const { state, attempt, procedure } = input;
      const internal = builtIn();
      if (internal?.allowsProcedure?.(procedure)) return internal.executeExact(input);
      const executable = procedure.executable;
      if (!this.allowsProcedure(procedure)) throw new Error(`Immutable command mapping is absent or invalid for ${procedure.procedureId}/${procedure.hash}`);
      const artifactPath = await realpath(executable.argv[0]);
      const artifactHash = `sha256:${createHash("sha256").update(await readFile(artifactPath)).digest("hex")}`;
      if (artifactHash !== executable.executableArtifactHash) throw new Error(`Executable artifact identity mismatch for ${procedure.procedureId}/${procedure.hash}`);
      let cwd: string;
      if (executable.cwdMode === "repository_root") {
        if (!workerManager.context?.cwd) throw new Error("Catalog procedure requires an attached repository root");
        cwd = await realpath(workerManager.context.cwd);
      } else if (executable.cwdMode === "attempt_worktree") {
        const binding = state.workerBindings[attempt.stageAttemptId];
        if (!binding) throw new Error(`Catalog procedure ${procedure.procedureId} requires an exact bound worker attempt`);
        const exact = await workerManager.inspectBindingReadOnly(binding);
        if (exact?.attempt?.attemptNumber !== binding.attemptNumber || exact?.attempt?.attemptNonce !== binding.attemptNonce || exact?.attempt?.configHash !== binding.configHash) throw new Error(`Catalog procedure ${procedure.procedureId} worktree does not bind the exact worker attempt`);
        cwd = await realpath(exact.worker.cwd);
      } else throw new Error("Catalog procedure run_root mapping is unavailable to the production extension and is blocked");
      const before = executable.noEdit ? await gitNoEditIdentity(cwd, exactEnvironment) : null;
      const { stdout } = await execFileAsync(artifactPath, executable.argv.slice(1), { cwd, env: exactEnvironment, timeout: executable.timeoutMs, maxBuffer: 4 * 1024 * 1024, encoding: "utf8", signal: input.signal });
      if (before) {
        const after = await gitNoEditIdentity(cwd, exactEnvironment);
        if (canonicalHash(after) !== canonicalHash(before)) throw new Error(`Catalog procedure ${procedure.procedureId} violated its exact no-edit boundary`);
      }
      const parsed = parseStrictJson(stdout);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(`Catalog procedure ${procedure.procedureId} did not emit one strict JSON evidence object`);
      return parsed as any;
    },
  };
}

async function gitNoEditIdentity(cwd: string, env: Record<string, string>) {
  const status = (await execFileAsync("/usr/bin/git", ["status", "--porcelain=v2", "--untracked-files=all"], { cwd, env, maxBuffer: 1024 * 1024 })).stdout;
  const head = (await execFileAsync("/usr/bin/git", ["rev-parse", "HEAD"], { cwd, env, maxBuffer: 1024 * 1024 })).stdout.trim();
  const tree = (await execFileAsync("/usr/bin/git", ["rev-parse", "HEAD^{tree}"], { cwd, env, maxBuffer: 1024 * 1024 })).stdout.trim();
  return { status, head, tree };
}

async function latestOrProvidedRun(cwd: string, runId?: string): Promise<string> {
  const id = runId ?? await findLatestRun(cwd);
  if (!id) throw new Error("No runId provided and no latest legacy run found");
  return id;
}

export default function dagWorkflow(pi: ExtensionAPI) {
  if (isWorkerChildRole()) {
    registerWorkerChild(pi);
    return;
  }

  const modelIntegration = registerProjectModelIntegration(pi);
  const workerManager = registerWorkerRuntime(pi);
  const conductor = new DagConductorServiceV1({
    async workerProjection(bindings) {
      if (!workerManager.store || bindings.length === 0) return { projectionHash: canonicalHash({ bindings: [] }), workers: [] };
      const workers = await workerManager.readBoundAttempts(bindings);
      return { projectionHash: canonicalHash({ bindings, workers }), workers };
    },
    integrationFactory({ store, context, lock }) {
      return new DagReducerGitIntegrationDriverV1({ store, context, lock });
    },
    onPumpError({ runId, error }) {
      pi.sendMessage({
        customType: "dag-conductor-error",
        content: `Canonical DAG conductor ${runId} stopped on an exact error and will not retry automatically. Diagnose the durable run before explicitly retrying. Error: ${error.message}`,
        display: true,
        details: { runId, error: error.message },
      }, { triggerTurn: true, deliverAs: "followUp" });
    },
    lifecycle: {
      worker: {
        async launchExact(request, _state, signal) {
          return workerManager.launchOwnedAttempt(request, workerManager.context, signal);
        },
        async readTerminalExact(binding, state, signal, input) {
          const terminal = await workerManager.terminalResultForBinding(binding, { reconcile: input?.reconcile === true, signal });
          const attempt = state.stageAttempts[binding.stageAttemptId];
          if (!terminal || !attempt) return terminal;
          const exact = await workerManager.inspectBindingReadOnly(binding); const cwd = exact.worker.cwd;
          if (exact?.attempt?.attemptNumber !== binding.attemptNumber || exact?.attempt?.attemptNonce !== binding.attemptNonce || exact?.attempt?.configHash !== binding.configHash) throw new Error("Terminal worktree observation does not bind the exact worker attempt");
          const env = { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
          const status = (await execFileAsync("git", ["status", "--porcelain=v1", "--untracked-files=all"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout;
          const commit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const tree = (await execFileAsync("git", ["rev-parse", "HEAD^{tree}"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          if (terminal.terminalStatus === "succeeded" && ["F2", "F5", "F6"].includes(attempt.stage)) {
            const expected = state.workItems[attempt.workItemId].candidate?.git.commit;
            if (status.trim() || !expected || commit !== expected) throw new Error(`${attempt.stage} fresh independent role violated its exact read-only candidate boundary`);
          }
          const item = state.workItems[attempt.workItemId]; const repository = state.repositories[item.writeRepositoryId];
          const sourceBase = attempt.stage === "F1" ? repository.baseline : item.candidate?.git ?? repository.baseline;
          const cwdCommonRaw = (await execFileAsync("git", ["rev-parse", "--git-common-dir"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const cwdCommon = await realpath(resolve(cwd, cwdCommonRaw)); const objectFormat = (await execFileAsync("git", ["rev-parse", "--show-object-format"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim(); const cwdIdentity = await stat(cwd);
          return { ...terminal, workerOutput: { outputRepositoryId: item.writeRepositoryId, outputCommonDirIdentityHash: canonicalHash({ realPath: cwdCommon, objectFormat }), outputWorktreeIdentityHash: canonicalHash({ realPath: await realpath(cwd), dev: String(cwdIdentity.dev), ino: String(cwdIdentity.ino) }), outputSourceBase: sourceBase, outputCommit: commit, outputTree: tree, outputObjectFormat: objectFormat, candidateObservedAt: state.updatedAt } };
        },
        async cancelExact(binding, input, _state, signal) {
          const result = await workerManager.cancelBinding(binding, `canonical DAG cancellation ${input.effectId}`, signal);
          return result.alreadyTerminal ? "proven_absent" : "applied_exact";
        },
        async cleanupExact(binding, input, _state, signal) {
          return workerManager.cleanupOwnedWorktreeForBinding(binding, input, signal);
        },
      },
      candidate: {
        async inspectAndSealCandidate({ plan, state, attempt, binding, repositoryId, signal }) {
          const exact = await workerManager.inspectBindingReadOnly(binding);
          if (!exact?.worker || exact.attempt?.attemptNumber !== binding.attemptNumber || exact.attempt?.attemptNonce !== binding.attemptNonce || exact.attempt?.configHash !== binding.configHash) throw new Error("Candidate inspection does not bind the exact owned-worker attempt");
          const cwd = exact.worker.cwd;
          const env = { ...process.env, LC_ALL: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
          const status = (await execFileAsync("git", ["status", "--porcelain=v2", "--untracked-files=all"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout;
          if (status.trim()) throw new Error(`${attempt.stage} candidate worktree must be clean and committed before candidate sealing`);
          const detached = await execFileAsync("git", ["symbolic-ref", "-q", "HEAD"], { cwd, env, maxBuffer: 1024 * 1024, signal }).then(() => false, (error) => { if (error?.code === 1) return true; throw error; });
          if (!detached) throw new Error("Candidate worktree must remain detached");
          const commit = (await execFileAsync("git", ["rev-parse", "HEAD"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const tree = (await execFileAsync("git", ["rev-parse", "HEAD^{tree}"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const item = state.workItems[attempt.workItemId];
          const base = attempt.stage === "F1" ? plan.repositories.find((repository) => repository.repositoryId === repositoryId)?.baseline : item.candidate?.git;
          if (!base) throw new Error(`${attempt.stage} candidate base is unavailable from the exact plan/run authority`);
          await execFileAsync("git", ["merge-base", "--is-ancestor", base.commit, commit], { cwd, env, maxBuffer: 1024 * 1024, signal });
          const cwdCommonRaw = (await execFileAsync("git", ["rev-parse", "--git-common-dir"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const cwdCommon = await realpath(resolve(cwd, cwdCommonRaw));
          const repositoryRoot = workerManager.context?.cwd;
          if (!repositoryRoot) throw new Error("Candidate sealing requires the attached exact repository root");
          const rootCommonRaw = (await execFileAsync("git", ["rev-parse", "--git-common-dir"], { cwd: repositoryRoot, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const rootCommon = await realpath(resolve(repositoryRoot, rootCommonRaw));
          if (cwdCommon !== rootCommon) throw new Error("Candidate worktree does not share the exact plan repository common-dir");
          const objectFormat = (await execFileAsync("git", ["rev-parse", "--show-object-format"], { cwd, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          const rootObjectFormat = (await execFileAsync("git", ["rev-parse", "--show-object-format"], { cwd: repositoryRoot, env, maxBuffer: 1024 * 1024, signal })).stdout.trim();
          if (objectFormat !== rootObjectFormat) throw new Error("Candidate worktree object format conflicts with the plan repository");
          await sealPrivateCandidateRefV1(cwd, state.runNonce, attempt.stageAttemptId, commit, signal);
          const git = { repositoryId, commit, tree };
          const core = { kind: "candidate", planHash: state.identity.planHash, runId: state.runId, runNonce: state.runNonce, workItemId: item.workItemId, generation: item.candidate === null && item.candidateGeneration > 0 ? item.candidateGeneration : item.candidateGeneration + 1, candidateId: `candidate-${attempt.stageAttemptId}`, base, git, patchIdentityHash: canonicalHash({ base, git }), producedByStageAttemptId: attempt.stageAttemptId, lineageHash: item.implementationLineageHash };
          const cwdIdentity = await stat(cwd);
          return {
            candidate: { ...core, hash: canonicalHash(core) },
            workerOutput: {
              outputRepositoryId: repositoryId,
              outputCommonDirIdentityHash: canonicalHash({ realPath: cwdCommon, objectFormat }),
              outputWorktreeIdentityHash: canonicalHash({ realPath: await realpath(cwd), dev: String(cwdIdentity.dev), ino: String(cwdIdentity.ino) }),
              outputSourceBase: base,
              outputCommit: commit,
              outputTree: tree,
              outputObjectFormat: objectFormat,
              candidateObservedAt: state.updatedAt,
            },
          };
        },
      },
      procedure: catalogProcedureAdapter(workerManager),
    },
  });
  registerCanonicalDagRuntime(pi, conductor);
  const planningIntegration = registerDagPlanningIntegrationV1(pi, {
    getActiveFocus: modelIntegration.getActiveFocus,
    conductor,
  });
  workerManager.onTerminalResult(async (event) => {
    const context = workerManager.context;
    if (!context) return;
    try {
      const completion = await conductor.completionNotice(context, event);
      if (!completion) return;
      pi.sendMessage({
        customType: "dag-owned-worker-completion",
        content: completion.requiresBinding
          ? `Owned canonical DAG worker completed before its launch acknowledgement was canonically bound. This notification has not changed canonical DAG state. Call dag_next_action, select the exact dag_start_work recovery action for ${completion.workItemId}/${completion.stage}, and pass its actionId; that replay binds the already-launched worker without changing launch identity. Then call dag_next_action again and use the exact dag_record_completion action for stageAttemptId=${completion.stageAttemptId}, completionId=${completion.completionId}. Do not use generic subagent tools.`
          : `Owned canonical DAG worker completed. This notification has not changed canonical DAG state. Call dag_next_action and select the current exact follow-up for stageAttemptId=${completion.stageAttemptId}, completionId=${completion.completionId}; the canonical callback guidance determines whether recovery, recording, or finalization comes next. After every mutation call dag_next_action again because prior choices are revision-bound. Do not use generic subagent tools.`,
        display: true,
        details: completion,
      }, { triggerTurn: true, deliverAs: "followUp" });
    } catch (error) {
      console.error(`DAG worker-completion notification failed: ${(error as Error).message}`);
    }
  });

  pi.registerCommand("dag", {
    description: "Project-model migration, brainstorming, planning, exact inspection, and session-bound DAG execution",
    handler: async (args: string, ctx: CommandContext) => {
      const { command, rest, options } = parseArgs(args);
      if (command === "brainstorm") {
        await modelIntegration.handleBrainstormCommand(rest, ctx);
        return;
      }
      if (command === "migrate") {
        if (Object.keys(options).length) { ctx.ui.notify("Usage: /dag migrate", "error"); return; }
        await modelIntegration.handleMigrateCommand(rest, ctx);
        return;
      }
      if (await planningIntegration.handleCommand(command, { rest, options }, ctx)) return;
      if (modelIntegration.isActive()) modelIntegration.suspend(ctx);

      if (command === "chunk") {
        ctx.ui.notify("Chunking is an internal phase of /dag plan, not a separate command.", "info");
        return;
      }
      if (["review", "retro", "archive", "grillme"].includes(command)) {
        ctx.ui.notify(`Model-aware /dag ${command} is not implemented.`, "warning");
        return;
      }
      if (command === "validate") {
        const result = await validateDag(ctx.cwd, String(options.dag ?? DEFAULT_DAG_PATH));
        ctx.ui.notify(`[legacy read-only] ${result.valid ? "DAG valid" : `DAG invalid: ${result.errors.join("; ")}`}`, result.valid ? "info" : "error");
        return;
      }
      if (command === "status") {
        const runId = rest.trim() || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No legacy DAG run found", "warning"); return; }
        const state = await loadRun(ctx.cwd, runId);
        ctx.ui.notify(`[legacy read-only] ${summarizeRun(state)}`, "info");
        return;
      }
      if (command === "workers") {
        const runId = rest.trim() || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No legacy DAG run found", "warning"); return; }
        const workers = await listWorkerRecords(ctx.cwd, runId);
        ctx.ui.notify(`[legacy read-only] ${workers.length} worker records`, "info");
        return;
      }
      if (command === "inspect") {
        const [runArg, nodeArg] = rest.trim().split(/\s+/).filter(Boolean);
        const runId = runArg || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No legacy DAG run found", "warning"); return; }
        const state = await loadRun(ctx.cwd, runId);
        const details = nodeArg ? state.nodes[nodeArg] : state;
        ctx.ui.notify(`[legacy read-only]\n${JSON.stringify(details, null, 2).slice(0, 4000)}`, "info");
        return;
      }
      if (command === "tail") {
        const [runArg, fileArg] = rest.trim().split(/\s+/).filter(Boolean);
        const runId = runArg || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No legacy DAG run found", "warning"); return; }
        const path = fileArg ?? join(runDir(ctx.cwd, runId), "events.jsonl");
        ctx.ui.notify(`[legacy read-only]\n${(await readLogTail(path, 40)).slice(-4000) || "No log output"}`, "info");
        return;
      }
      ctx.ui.notify("Usage: /dag migrate | brainstorm [new|resume|list|stop] | plan | show | run | validate | status | workers | inspect | tail", "info");
    },
  });

  pi.registerTool({
    name: "dag_validate",
    label: "Legacy DAG Validate (read-only)",
    description: "Validate an existing legacy .ai/dag.json without mutating run state.",
    parameters: Type.Object({ dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const result = await validateDag(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      return ok(result.valid ? "[legacy read-only] DAG valid" : `[legacy read-only] DAG invalid\n${result.errors.join("\n")}`, result as any);
    },
  });

  pi.registerTool({
    name: "dag_diagram",
    label: "Legacy DAG Diagram (read-only)",
    description: "Render a compact dependency diagram for an existing legacy DAG file.",
    parameters: Type.Object({ dagPath: Type.Optional(Type.String()), width: Type.Optional(Type.Number()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const dagPath = params.dagPath ?? DEFAULT_DAG_PATH;
      const dag = await readDag(ctx.cwd, dagPath);
      const validation = await validateDag(ctx.cwd, dagPath);
      const diagram = renderDagDiagram(dag, { width: params.width });
      const warnings = [...validation.warnings, ...diagram.warnings];
      const lines = [`[legacy read-only] ${validation.valid ? "DAG valid" : "DAG invalid"}: ${dagPath}`];
      if (validation.errors.length) lines.push("", "Validation errors:", ...validation.errors.map((error) => `- ${error}`));
      lines.push("", diagram.text);
      if (warnings.length) lines.push("", "Warnings:", ...warnings.map((warning) => `- ${warning}`));
      return ok(lines.join("\n"), { dagPath, valid: validation.valid, errors: validation.errors, warnings, diagram: diagram.text });
    },
  });

  pi.registerTool({
    name: "dag_status",
    label: "Legacy DAG Status (read-only)",
    description: "Show status for an existing legacy DAG run without mutating it.",
    parameters: Type.Object({ runId: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const runId = await latestOrProvidedRun(ctx.cwd, params.runId);
      const state = await loadRun(ctx.cwd, runId);
      return ok(`[legacy read-only] ${summarizeRun(state)}`, { state });
    },
  });
}
