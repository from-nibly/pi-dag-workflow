import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { DagConductorServiceV1, type DagMutationGuardV1 } from "./conductor.ts";
import { renderDagWidgetV1 } from "./widget.ts";

const Id = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$" });
const RunId = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" });
const Hash = Type.String({ pattern: "^sha256:[0-9a-f]{64}$" });
const Timestamp = Type.String({ pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z$" });
const strict = <T extends Record<string, any>>(properties: T) => Type.Object(properties, { additionalProperties: false });
const ReadBinding = strict({ runId: RunId });
const MutationGuard = {
  runId: RunId, runNonce: Type.String({ minLength: 16, maxLength: 256 }), expectedRevision: Type.Integer({ minimum: 0 }), expectedSnapshotHash: Hash,
  ownerEpoch: Type.Integer({ minimum: 0 }), commandId: Id, idempotencyKey: Type.String({ minLength: 1, maxLength: 256 }), occurredAt: Timestamp,
};

export function registerCanonicalDagRuntime(pi: ExtensionAPI, service = new DagConductorServiceV1()): DagConductorServiceV1 {
  const ok = (text: string, details: Record<string, unknown>) => ({ content: [{ type: "text" as const, text }], details });
  let widgetProjection: Awaited<ReturnType<typeof service.status>>["projection"] | null = null;
  let widgetDiagnostic: string | null = null;
  let widgetTimer: ReturnType<typeof setInterval> | null = null;
  let conductorTimer: ReturnType<typeof setInterval> | null = null;
  let conductorContext: any = null;
  let widgetContext: any = null;
  let widgetTui: any = null;

  const refreshWidget = async () => {
    if (!widgetContext?.hasUI || typeof widgetContext.ui?.setWidget !== "function") return;
    const binding = await service.binding(widgetContext).catch(() => null);
    if (!binding) { widgetProjection = null; widgetDiagnostic = null; widgetTui?.requestRender?.(); return; }
    try {
      const status = await service.status(widgetContext, binding.runId);
      if (status.state.completion.state !== "open" && ["completed", "cancelled", "superseded"].includes(status.state.current.run)) {
        widgetProjection = null; widgetDiagnostic = null; widgetContext.ui.setWidget("canonical-dag-run", undefined); return;
      }
      widgetProjection = status.projection; widgetDiagnostic = status.stale ? `STALE READ-ONLY | source r${status.stale.sourceRevision} ${status.stale.sourceSnapshotHash.slice(0, 18)} | observed r${status.stale.newerObservedRevision} | cached ${status.stale.cachedAt}` : null;
    } catch (error) { widgetProjection = null; widgetDiagnostic = `DAG ${binding.runId} | projection unavailable: ${String((error as Error).message).slice(0, 160)}`; }
    widgetTui?.requestRender?.();
  };

  const advanceConductor = async () => { if (!conductorContext) return; const binding = await service.binding(conductorContext).catch(() => null); if (binding) await service.advance(conductorContext, binding.runId).catch(() => undefined); };

  pi.on("session_start", async (_event, ctx: any) => {
    if (conductorTimer) clearInterval(conductorTimer); conductorContext = ctx; conductorTimer = setInterval(() => { void advanceConductor(); }, 1000); conductorTimer.unref?.(); await advanceConductor();
    if (widgetTimer) clearInterval(widgetTimer); widgetTimer = null; widgetContext?.ui?.setWidget?.("canonical-dag-run", undefined); widgetContext = null; widgetTui = null; widgetProjection = null; widgetDiagnostic = null;
    if (!ctx.hasUI || (ctx.mode && ctx.mode !== "tui") || typeof ctx.ui?.setWidget !== "function") return;
    widgetContext = ctx;
    ctx.ui.setWidget("canonical-dag-run", (tui: any) => {
      widgetTui = tui;
      return {
        render(width: number) {
          if (widgetDiagnostic && !widgetProjection) return [clip(widgetDiagnostic, width), clip("FAIL-CLOSED; inspect or reattach the exact run", width)];
          if (!widgetProjection) return [];
          const lines = renderDagWidgetV1(widgetProjection, Math.max(20, width), Math.max(4, tui.terminal?.rows ?? 24), new Date().toISOString()).lines;
          return widgetDiagnostic ? [clip(widgetDiagnostic, width), ...lines].slice(0, Math.max(4, Math.min(12, Math.floor((tui.terminal?.rows ?? 24) / 3)))) : lines;
        },
        invalidate() {},
      };
    });
    widgetTimer = setInterval(() => { void refreshWidget(); }, 1000); widgetTimer.unref?.();
    await refreshWidget();
  });
  pi.on("agent_end", async () => { await advanceConductor(); await refreshWidget(); });
  pi.on("session_shutdown", async () => {
    if (conductorTimer) clearInterval(conductorTimer); conductorTimer = null; conductorContext = null;
    if (widgetTimer) clearInterval(widgetTimer); widgetTimer = null;
    widgetContext?.ui?.setWidget("canonical-dag-run", undefined); widgetContext = null; widgetTui = null; widgetProjection = null; widgetDiagnostic = null;
  });

  pi.registerTool({
    name: "dag_run_status", label: "Canonical DAG Run Status", description: "Read one exact session-bound canonical DAG run and its stable execution projection.", parameters: ReadBinding,
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId);
      const summary = result.projection.summary;
      return ok(`DAG ${params.runId} r${result.state.revision} ${result.state.current.run}/${result.state.completion.state}\nready=${summary.ready} active=${summary.activeLanes} attention=${summary.attention} integrationReady=${summary.integrationReady} complete=${summary.complete}`, result as any);
    },
  });
  pi.registerTool({
    name: "dag_run_diagram", label: "Canonical DAG Run Diagram", description: "Render a bounded read-only diagram from one exact canonical execution projection.", parameters: strict({ runId: RunId, width: Type.Optional(Type.Integer({ minimum: 20, maximum: 400 })), terminalRows: Type.Optional(Type.Integer({ minimum: 4, maximum: 300 })), observedAt: Timestamp }),
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId); const layout = renderDagWidgetV1(result.projection, params.width ?? 100, params.terminalRows ?? 36, params.observedAt);
      return ok(layout.lines.join("\n"), { layout, projectionHash: result.projection.projectionHash });
    },
  });
  pi.registerTool({
    name: "dag_run_inspect", label: "Canonical DAG Run Inspect", description: "Inspect an exact run, stable node alias, work item, attempt, reservation, effect, blocker, or quarantine identity.", parameters: strict({ runId: RunId, subjectId: Type.Union([Id, Type.Null()]) }),
    async execute(_id, params, _signal, _update, ctx) { const value = await service.inspect(ctx, params.runId, params.subjectId); return ok(JSON.stringify(value, null, 2).slice(0, 48_000), { value }); },
  });
  pi.registerTool({
    name: "dag_run_tail", label: "Canonical DAG Run Tail", description: "Read a bounded page of immutable canonical run snapshots by exact revision cursor.", parameters: strict({ runId: RunId, limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 100 })), beforeRevision: Type.Union([Type.Integer({ minimum: 0 }), Type.Null()]) }),
    async execute(_id, params, _signal, _update, ctx) { const page = await service.tail(ctx, params.runId, params.limit ?? 20, params.beforeRevision); return ok(page.snapshots.map((item) => `r${item.revision} ${item.current} ${item.snapshotHash}`).join("\n") || "No snapshots", page); },
  });
  pi.registerTool({
    name: "dag_run_explain", label: "Canonical DAG Run Explain", description: "Explain readiness, admission, blockers, and priority for one exact work item or stable alias.", parameters: strict({ runId: RunId, subjectId: Id }),
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId); const node = result.projection.nodes.find(({ alias, workItemId }) => alias === params.subjectId || workItemId === params.subjectId);
      if (!node) throw new Error(`Unknown DAG subject: ${params.subjectId}`);
      const slot = result.decision.frontier.find(({ workItemId }) => workItemId === node.workItemId);
      return ok(`${node.alias} ${node.workItemId} ${node.glyph}\nready=${node.correctnessReady} admissible=${node.admissible}\nblockers=${node.blockerCodes.join(",") || "none"}\nadmission=${slot?.admissionCodes.join(",") || "none"}\npriority=${slot ? JSON.stringify(slot.priority) : "terminal"}`, { node, slot, decisionHash: result.decision.decisionHash });
    },
  });

  pi.registerTool({
    name: "dag_run_start", label: "Canonical DAG Run Start", description: "Initialize and bind one pre-authorized canonical DAG run from exact repository-local plan, genesis, and context artifacts.",
    parameters: strict({ runId: RunId, runNonce: Type.String({ minLength: 16, maxLength: 256 }), planHash: Hash, planPath: Type.String({ minLength: 1, maxLength: 1024 }), genesisPath: Type.String({ minLength: 1, maxLength: 1024 }), contextPath: Type.String({ minLength: 1, maxLength: 1024 }), maxActiveNodes: Type.Integer({ minimum: 1 }), occurredAt: Timestamp }),
    async execute(_id, params, _signal, _update, ctx) { const result = await service.start(ctx, params); await refreshWidget(); return ok(`Started canonical DAG ${result.state.runId} at r${result.state.revision}; ${result.decision.notice}`, result as any); },
  });
  pi.registerTool({
    name: "dag_run_control", label: "Canonical DAG Run Control", description: "Compile explicit pause, resume, or cancel intent to the guarded closed run reducer.", parameters: strict({ ...MutationGuard, action: Type.Enum(["pause", "resume", "cancel"]), reason: Type.String({ minLength: 1, maxLength: 4096 }) }),
    async execute(_id, params, _signal, _update, ctx) { const { action, reason, ...guard } = params; const state = await service.control(ctx, guard as DagMutationGuardV1, action, reason); await refreshWidget(); return ok(`DAG ${state.runId} r${state.revision} ${state.current.run}`, { state }); },
  });
  pi.registerTool({
    name: "dag_run_retry", label: "Canonical DAG Run Retry", description: "Authorize one exact existing retry-ledger dimension, fingerprint, generation, and count through guarded reducer CAS.",
    parameters: strict({ ...MutationGuard, retryKey: Hash, expectedCount: Type.Integer({ minimum: 0 }), workItemId: Id, stage: Type.Enum(["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"]), dimension: Type.Enum(["product_repair", "test_rework", "review_rework", "hardening_rework", "infrastructure", "worker_replacement", "integration"]), fingerprint: Hash, candidateGeneration: Type.Integer({ minimum: 0 }) }),
    async execute(_id, params, _signal, _update, ctx) {
      const { retryKey, expectedCount, workItemId, stage, dimension, fingerprint, candidateGeneration, ...guard } = params;
      const state = await service.retry(ctx, guard as DagMutationGuardV1, { retryKey, expectedCount, workItemId, stage, dimension, fingerprint, candidateGeneration }); await refreshWidget();
      return ok(`Authorized exact retry for ${workItemId}/${stage} at r${state.revision}`, { state });
    },
  });
  pi.registerTool({
    name: "dag_run_reattach", label: "Canonical DAG Run Reattach", description: "Explicitly reattach one exact run only after canonical prior PID/start-identity death proof and guarded owner-epoch CAS.", parameters: strict(MutationGuard),
    async execute(_id, params, _signal, _update, ctx) { const state = await service.reattach(ctx, params as DagMutationGuardV1); await refreshWidget(); return ok(`Reattached DAG ${state.runId} at owner epoch ${state.owner.ownerEpoch}`, { state }); },
  });
  return service;
}

function clip(value: string, width: number): string { return value.length <= width ? value : `${value.slice(0, Math.max(0, width - 1))}…`; }
