import { execFile } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { findLatestRun, loadRun, readDag, runDir, summarizeRun, validateDag } from "./dag.ts";
import { renderDagDiagram } from "./diagram.ts";
import { canonicalHash } from "./dag-runtime/common.ts";
import { registerProductV2 } from "./runtime-v2/registration.ts";
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

async function latestOrProvidedRun(cwd: string, runId?: string): Promise<string> {
  const id = runId ?? await findLatestRun(cwd);
  if (!id) throw new Error("No runId provided and no latest legacy run found");
  return id;
}

export default function dagWorkflow(pi: ExtensionAPI, options: { workerRuntime?: Record<string, unknown> } = {}) {
  if (isWorkerChildRole()) {
    registerWorkerChild(pi);
    return;
  }

  const modelIntegration = registerProjectModelIntegration(pi);
  const workerManager = registerWorkerRuntime(pi, { ...options.workerRuntime, autoRecoverOwned: false });
  const planningIntegration = registerProductV2(pi, { workerManager });

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
      if (await planningIntegration.handleCommand(command, { rest, options, raw: args.trim().slice(command.length).trim() }, ctx)) return;

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
  return { planningIntegration, workerManager };
}
