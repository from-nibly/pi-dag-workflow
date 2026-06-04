import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import {
  appendEvent,
  createRun,
  findLatestRun,
  getFlow,
  getNode,
  getReadyNodeIds,
  loadRun,
  readDag,
  resolveStep,
  runDir,
  saveRunState,
  stepKey,
  summarizeRun,
  validateDag,
} from "./dag.ts";
import { ensureNodeWorktree, mergeNode } from "./worktrees.ts";
import { buildSubagentParams, extractVerdict } from "./subagents.ts";
import { listWorkerRecords, readLogTail, writeMetricsArtifact, writeWorkerRecord } from "./sessions.ts";
import { DEFAULT_DAG_PATH, type DagStep } from "./types.ts";
import { createGrillMe, currentQuestion, getActiveGrillMe, loadLatestGrillMe, saveGrillMe, setActiveGrillMe } from "./grillme/state.ts";
import { installGrillMeEditor, requestGrillMeRender } from "./grillme/editor.ts";
import { registerGrillMeTools } from "./grillme/tools.ts";

const extensionDir = dirname(fileURLToPath(import.meta.url));

type CommandContext = any;

function splitArgs(input: string): string[] {
  const out: string[] = [];
  let current = "";
  let quote: string | undefined;
  for (const char of input.trim()) {
    if (quote) { if (char === quote) quote = undefined; else current += char; continue; }
    if (char === '"' || char === "'") { quote = char; continue; }
    if (/\s/.test(char)) { if (current) out.push(current); current = ""; continue; }
    current += char;
  }
  if (current) out.push(current);
  return out;
}

function parseArgs(args: string) {
  const tokens = splitArgs(args);
  const command = tokens.shift() ?? "help";
  const rest = tokens.join(" ");
  const options: Record<string, string | boolean> = {};
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (!token.startsWith("--")) continue;
    const name = token.slice(2);
    const next = tokens[i + 1];
    if (next && !next.startsWith("--")) { options[name] = next; i++; }
    else options[name] = true;
  }
  return { command, rest, options };
}

async function promptText(name: string): Promise<string> {
  return await readFile(join(extensionDir, "command-prompts", `${name}.md`), "utf8");
}

function sendPrompt(pi: ExtensionAPI, ctx: CommandContext, name: string, args: string) {
  return promptText(name).then((text) => {
    pi.sendUserMessage(`${text}\n\nUser arguments: ${args || "(none)"}`);
  }).catch((error) => ctx.ui.notify(`Failed to load ${name} prompt: ${error.message}`, "error"));
}

function ok(content: string, details: Record<string, unknown> = {}) {
  return { content: [{ type: "text" as const, text: content }], details };
}

function err(message: string) { throw new Error(message); }

async function latestOrProvidedRun(cwd: string, runId?: string): Promise<string> {
  const id = runId ?? await findLatestRun(cwd);
  if (!id) throw new Error("No runId provided and no latest run found");
  return id;
}

function currentAttemptFor(state: any, nodeId: string, flowIndex: number, stepId: string): number {
  return (state.nodes[nodeId]?.attempts ?? []).filter((a: any) => a.flowIndex === flowIndex && a.stepId === stepId).length + 1;
}

async function nextAction(cwd: string, dagPath: string, runId: string) {
  const dag = await readDag(cwd, dagPath);
  const state = await loadRun(cwd, runId);
  if (state.manifest.status !== "running") return { action: "finalized", runId, summary: summarizeRun(state) };
  const running = Object.values(state.nodes).filter((n: any) => n.status === "running").map((n: any) => n.id);
  const availableSlots = Math.max(0, (dag.run.maxConcurrency ?? 1) - running.length);
  const ready = getReadyNodeIds(dag, state).slice(0, availableSlots);
  if (ready.length > 0) return { action: "start_nodes", runId, nodeIds: ready };
  if (running.length > 0) return { action: "await_workers", runId, nodeIds: running };
  const needs = Object.values(state.nodes).filter((n: any) => n.status === "needs_decision").map((n: any) => n.id);
  if (needs.length > 0) return { action: "needs_decision", runId, nodeIds: needs };
  const mergeReady = Object.values(state.nodes).filter((n: any) => n.status === "merge_ready").map((n: any) => n.id);
  if (mergeReady.length > 0) return { action: "merge_nodes", runId, nodeIds: mergeReady };
  const allMerged = Object.values(state.nodes).every((n: any) => n.status === "merged");
  if (allMerged) return { action: "finalize", runId };
  return { action: "blocked", runId, summary: summarizeRun(state) };
}

export default function dagWorkflow(pi: ExtensionAPI) {
  registerGrillMeTools(pi);

  pi.registerCommand("dag", {
    description: "DAG workflow commands: brainstorm, grillme, plan, chunk, validate, run, status, workers, inspect, tail, review, retro, archive",
    handler: async (args: string, ctx: CommandContext) => {
      const { command, rest, options } = parseArgs(args);
      if (["brainstorm", "plan", "chunk", "run", "review", "retro", "archive"].includes(command)) {
        if (command === "run" && !pi.getAllTools().some((tool) => tool.name === "subagent")) {
          ctx.ui.notify("/dag run requires the pi-subagents package to be loaded. Install/enable npm:pi-subagents@0.25.0, then reload Pi.", "error");
          return;
        }
        await sendPrompt(pi, ctx, command, rest);
        return;
      }
      if (command === "grillme") {
        let session = await loadLatestGrillMe(ctx.cwd);
        if (!session) session = await createGrillMe(ctx.cwd);
        setActiveGrillMe(session);
        installGrillMeEditor(ctx, (completedSession) => {
          pi.sendUserMessage(
            `GrillMe ${completedSession.fileNumber} is complete. Use the dag_grillme_get_answers tool to read the filtered answered questions from the GrillMe JSON state (id, title, body, answer only; discarded questions excluded). Inspect answers for explicit or implied research requests, follow up on that research when possible, then synthesize answers, findings, remaining uncertainty, and conflicts into .ai/project.md using dag_grillme_record_understanding.`,
            { deliverAs: "followUp" },
          );
        });
        await saveGrillMe(ctx, session);
        requestGrillMeRender();
        ctx.ui.notify("GrillMe mode started. Press c in nav mode to chat; the agent can populate questions with dag_grillme_set_questions.", "info");
        await sendPrompt(pi, ctx, "grillme", rest);
        return;
      }
      if (command === "validate") {
        const result = await validateDag(ctx.cwd, String(options.dag ?? DEFAULT_DAG_PATH));
        ctx.ui.notify(result.valid ? "DAG valid" : `DAG invalid: ${result.errors.join("; ")}`, result.valid ? "info" : "error");
        return;
      }
      if (command === "status") {
        const runId = rest.trim() || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No DAG run found", "warning"); return; }
        const state = await loadRun(ctx.cwd, runId);
        ctx.ui.notify(summarizeRun(state), "info");
        return;
      }
      if (command === "workers") {
        const runId = rest.trim() || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No DAG run found", "warning"); return; }
        const workers = await listWorkerRecords(ctx.cwd, runId);
        ctx.ui.notify(`${workers.length} worker records`, "info");
        return;
      }
      if (command === "inspect") {
        const [runArg, nodeArg] = rest.trim().split(/\s+/).filter(Boolean);
        const runId = runArg || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No DAG run found", "warning"); return; }
        const state = await loadRun(ctx.cwd, runId);
        const details = nodeArg ? state.nodes[nodeArg] : state;
        ctx.ui.notify(JSON.stringify(details, null, 2).slice(0, 4000), "info");
        return;
      }
      if (command === "tail") {
        const [runArg, fileArg] = rest.trim().split(/\s+/).filter(Boolean);
        const runId = runArg || await findLatestRun(ctx.cwd);
        if (!runId) { ctx.ui.notify("No DAG run found", "warning"); return; }
        const path = fileArg ?? join(runDir(ctx.cwd, runId), "events.jsonl");
        ctx.ui.notify((await readLogTail(path, 40)).slice(-4000) || "No log output", "info");
        return;
      }
      ctx.ui.notify("Usage: /dag brainstorm|grillme|plan|chunk|validate|run|status|workers|inspect|tail|review|retro|archive", "info");
    },
  });

  pi.on("before_agent_start", async (event) => {
    const session = getActiveGrillMe();
    if (!session || session.mode !== "chat") return;
    const q = currentQuestion(session);
    if (!q) return;
    return {
      systemPrompt: `${event.systemPrompt}\n\nThe user is in GrillMe chat mode. Current question: ${q.title}\n${q.body}\nWhy this matters: ${q.why ?? ""}\nOptions: ${(q.options ?? []).map((o, i) => `${i + 1}. ${o.label}: ${o.text}`).join(" | ")}\nAnswer their question. Do not record an answer unless explicitly asked.`,
    };
  });

  pi.registerTool({
    name: "dag_validate",
    label: "DAG Validate",
    description: "Validate a .ai/dag.json DAG file.",
    parameters: Type.Object({ dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const result = await validateDag(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      return ok(result.valid ? "DAG valid" : `DAG invalid\n${result.errors.join("\n")}`, result as any);
    },
  });

  pi.registerTool({
    name: "dag_init",
    label: "DAG Init",
    description: "Initialize durable run state for a DAG.",
    parameters: Type.Object({ dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const state = await createRun(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      await appendEvent(ctx.cwd, state.manifest.runId, { type: "run_initialized" });
      return ok(`Initialized DAG run ${state.manifest.runId}`, { runId: state.manifest.runId, state });
    },
  });

  pi.registerTool({
    name: "dag_next_action",
    label: "DAG Next Action",
    description: "Return the next allowed DAG conductor action.",
    parameters: Type.Object({ runId: Type.Optional(Type.String()), dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const runId = await latestOrProvidedRun(ctx.cwd, params.runId);
      const action = await nextAction(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH, runId);
      return ok(JSON.stringify(action, null, 2), action as any);
    },
  });

  pi.registerTool({
    name: "dag_ready",
    label: "DAG Ready Nodes",
    description: "Return node ids ready to launch.",
    parameters: Type.Object({ runId: Type.String(), dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const dag = await readDag(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      const state = await loadRun(ctx.cwd, params.runId);
      const nodeIds = getReadyNodeIds(dag, state);
      return ok(nodeIds.join("\n"), { nodeIds });
    },
  });

  pi.registerTool({
    name: "dag_start_node",
    label: "DAG Start Node",
    description: "Create/reuse node worktree, mark next step running, and return subagent launch params.",
    parameters: Type.Object({ runId: Type.String(), nodeId: Type.String(), dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const dag = await readDag(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      const state = await loadRun(ctx.cwd, params.runId);
      const node = getNode(dag, params.nodeId) ?? err(`Unknown node ${params.nodeId}`);
      await ensureNodeWorktree(ctx.cwd, state, node);
      const nodeState = state.nodes[node.id];
      const flowIndex = nodeState.currentFlowIndex;
      const step = resolveStep(dag, node, flowIndex);
      if (step.kind === "merge") { nodeState.status = "merge_ready"; await saveRunState(ctx.cwd, state); return ok(`Node ${node.id} merge ready`, { action: "merge", nodeId: node.id }); }
      const attempt = currentAttemptFor(state, node.id, flowIndex, step.id);
      const attemptState = { stepId: step.id, flowIndex, attempt, status: "running" as const, startedAt: new Date().toISOString(), model: step.model };
      nodeState.status = "running";
      nodeState.attempts.push(attemptState);
      await saveRunState(ctx.cwd, state);
      const subagentParams = await buildSubagentParams({ dag, node, step, cwd: nodeState.worktree!, parentCwd: ctx.cwd, runId: params.runId, flowIndex, attempt });
      return ok(`Launch ${node.id} ${flowIndex}:${step.id} attempt ${attempt}`, { runId: params.runId, nodeId: node.id, flowIndex, stepId: step.id, attempt, subagentParams });
    },
  });

  pi.registerTool({
    name: "dag_record_worker_result",
    label: "DAG Record Worker Result",
    description: "Record a completed DAG step worker result.",
    parameters: Type.Object({ runId: Type.String(), nodeId: Type.String(), flowIndex: Type.Optional(Type.Number()), stepId: Type.Optional(Type.String()), attempt: Type.Optional(Type.Number()), outputPath: Type.Optional(Type.String()), finalText: Type.Optional(Type.String()), exitCode: Type.Optional(Type.Number()), sessionFile: Type.Optional(Type.String()), model: Type.Optional(Type.String()), errorMessage: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const dag = await readDag(ctx.cwd, DEFAULT_DAG_PATH);
      const state = await loadRun(ctx.cwd, params.runId);
      const node = getNode(dag, params.nodeId) ?? err(`Unknown node ${params.nodeId}`);
      const nodeState = state.nodes[node.id];
      const flowIndex = params.flowIndex ?? nodeState.currentFlowIndex;
      const step = resolveStep(dag, node, flowIndex);
      const stepId = params.stepId ?? step.id;
      const attempt = params.attempt ?? currentAttemptFor(state, node.id, flowIndex, stepId) - 1;
      const rec = nodeState.attempts.find((a) => a.flowIndex === flowIndex && a.stepId === stepId && a.attempt === attempt) ?? { flowIndex, stepId, attempt, status: "running" as const };
      rec.status = params.exitCode && params.exitCode !== 0 ? "failed" : "passed";
      rec.endedAt = new Date().toISOString();
      rec.finalText = params.finalText;
      rec.artifactPath = params.outputPath;
      rec.exitCode = params.exitCode;
      rec.sessionFile = params.sessionFile;
      rec.model = params.model;
      rec.verdict = extractVerdict(params.finalText);
      if (!nodeState.attempts.includes(rec as any)) nodeState.attempts.push(rec as any);
      const failed = rec.status === "failed" || rec.verdict === "FAIL";
      if (failed) nodeState.status = "needs_decision";
      else { nodeState.currentFlowIndex = flowIndex + 1; nodeState.status = nodeState.currentFlowIndex >= getFlow(dag, node).length ? "merge_ready" : "pending"; }
      await writeWorkerRecord(ctx.cwd, params.runId, stepKey(node.id, stepId, flowIndex, attempt), rec);
      await saveRunState(ctx.cwd, state);
      const action = await nextAction(ctx.cwd, DEFAULT_DAG_PATH, params.runId);
      return ok(`Recorded ${node.id} ${flowIndex}:${stepId} ${rec.status}`, { nextAction: action, record: rec });
    },
  });

  pi.registerTool({
    name: "dag_merge_node",
    label: "DAG Merge Node",
    description: "Merge one merge-ready node into the parent branch.",
    parameters: Type.Object({ runId: Type.String(), nodeId: Type.String(), dagPath: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const dag = await readDag(ctx.cwd, params.dagPath ?? DEFAULT_DAG_PATH);
      const state = await loadRun(ctx.cwd, params.runId);
      const node = getNode(dag, params.nodeId) ?? err(`Unknown node ${params.nodeId}`);
      if (state.nodes[node.id]?.status !== "merge_ready") throw new Error(`Node ${node.id} is not merge_ready`);
      const output = await mergeNode(ctx.cwd, state, node);
      state.nodes[node.id].status = "merged";
      state.nodes[node.id].mergedAt = new Date().toISOString();
      await saveRunState(ctx.cwd, state);
      return ok(`Merged ${node.id}`, { output });
    },
  });

  pi.registerTool({
    name: "dag_finalize",
    label: "DAG Finalize",
    description: "Finalize a DAG run.",
    parameters: Type.Object({ runId: Type.String() }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const state = await loadRun(ctx.cwd, params.runId);
      state.manifest.status = Object.values(state.nodes).every((node) => node.status === "merged") ? "completed" : "failed";
      await saveRunState(ctx.cwd, state);
      return ok(summarizeRun(state), { state });
    },
  });

  pi.registerTool({
    name: "dag_status",
    label: "DAG Status",
    description: "Show DAG run status.",
    parameters: Type.Object({ runId: Type.Optional(Type.String()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const runId = await latestOrProvidedRun(ctx.cwd, params.runId);
      const state = await loadRun(ctx.cwd, runId);
      return ok(summarizeRun(state), { state });
    },
  });

  pi.registerTool({
    name: "dag_parse_sessions",
    label: "DAG Parse Sessions",
    description: "Parse worker records and write simple metrics artifact.",
    parameters: Type.Object({ runId: Type.String() }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const workers = await listWorkerRecords(ctx.cwd, params.runId);
      const path = await writeMetricsArtifact(ctx.cwd, params.runId, `# DAG Metrics\n\nWorker records: ${workers.length}\n`);
      return ok(`Wrote ${path}`, { path, workers: workers.length });
    },
  });
}
