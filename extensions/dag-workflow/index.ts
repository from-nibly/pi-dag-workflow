import { join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { findLatestRun, loadRun, readDag, runDir, summarizeRun, validateDag } from "./dag.ts";
import { renderDagDiagram } from "./diagram.ts";
import { registerCanonicalDagRuntime } from "./dag-runtime/integration.ts";
import { DagConductorServiceV1 } from "./dag-runtime/conductor.ts";
import { canonicalHash } from "./dag-runtime/common.ts";
import { requireSchedulerDispatchIntentV1 } from "./dag-runtime/scheduler.ts";
import { registerProjectModelIntegration } from "./project-model/integration.ts";
import { isWorkerChildRole, registerWorkerChild } from "./worker-runtime/child-report.ts";
import { registerWorkerRuntime } from "./worker-runtime/integration.ts";
import { listWorkerRecords, readLogTail } from "./sessions.ts";
import { DEFAULT_DAG_PATH } from "./types.ts";

type CommandContext = any;

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
    async dispatchEffect(request, runState) {
      if (request.kind.startsWith("scheduler_")) {
        const reservation = requireSchedulerDispatchIntentV1(runState, request);
        if (["conductor", "integration"].includes(reservation.operationKind)) return;
        const item = runState.workItems[reservation.workItemId]; await workerManager.launch({ launchKey: `dag:${runState.runId}:${reservation.reservationId}`, label: `${reservation.workItemId}/${reservation.stage}`, task: `Execute canonical DAG ${runState.runId} work item ${reservation.workItemId} stage ${reservation.stage} as the fixed ${reservation.operationKind} role. Work from the repository state provided by the owning session. Return bounded evidence and diagnostics only; generic worker completion is not DAG stage authority. Do not infer readiness, advance lifecycle state, integrate, or land Git changes.`, cwd: workerManager.store ? (await workerManager.store.load()).repositoryRoot : undefined }); return;
      }
      if (request.kind !== "cancel_worker" || !workerManager.store) throw new Error(`Unsupported canonical DAG effect adapter: ${request.kind}`);
      const workerState = await workerManager.store.load();
      const matches = Object.values(runState.stageAttempts).filter((attempt: any) => {
        const binding = runState.workerBindings[attempt.stageAttemptId]; if (!binding || binding.workerStorageId !== workerState.storageId) return false;
        const expected = canonicalHash({ kind: "cancel_worker", runId: runState.runId, runNonce: runState.runNonce, workItemId: attempt.workItemId, stageAttemptId: attempt.stageAttemptId, workerStorageId: binding.workerStorageId, launchOwnerSessionId: binding.launchOwnerSessionId, workerId: binding.workerId, attemptNumber: binding.attemptNumber, attemptNonce: binding.attemptNonce, configHash: binding.configHash, fencedGeneration: runState.workItems[attempt.workItemId].candidateGeneration });
        return expected === request.requestHash;
      });
      if (matches.length !== 1) throw new Error("DAG cancellation effect does not bind exactly one current owned-worker attempt");
      const binding = runState.workerBindings[(matches[0] as any).stageAttemptId];
      await workerManager.cancel(binding.workerId, `canonical DAG cancellation ${request.effectId}`);
    },
  });
  registerCanonicalDagRuntime(pi, conductor);

  pi.registerCommand("dag", {
    description: "Model brainstorming plus read-only inspection of legacy DAG artifacts",
    handler: async (args: string, ctx: CommandContext) => {
      const { command, rest, options } = parseArgs(args);
      if (command === "brainstorm") {
        await modelIntegration.handleBrainstormCommand(rest, ctx);
        return;
      }
      if (modelIntegration.isActive()) modelIntegration.suspend(ctx);

      if (["plan", "chunk", "review", "retro", "archive", "grillme"].includes(command)) {
        ctx.ui.notify("Model-aware planning remains deferred; canonical execution is available through the exact dag_run_* conductor tools.", "warning");
        return;
      }
      if (command === "run") {
        ctx.ui.notify("Canonical runs start through dag_run_start with exact plan, genesis, context, and authorization identities.", "info");
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
      ctx.ui.notify("Usage: /dag brainstorm [new|resume|list|stop] | validate | status | workers | inspect | tail", "info");
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
