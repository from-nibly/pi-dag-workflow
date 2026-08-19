import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { Type } from "typebox";
import { DagConductorServiceV1, type DagMutationGuardV1 } from "./conductor.ts";
import { renderDagWidgetV2 } from "./widget.ts";
import { DagWidgetControllerV2 } from "./widget-controller.ts";

const Id = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$" });
const RunId = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" });
const Hash = Type.String({ pattern: "^sha256:[0-9a-f]{64}$" });
const Timestamp = Type.String({ pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z$" });
const strict = <T extends Record<string, any>>(properties: T) => Type.Object(properties, { additionalProperties: false });
const ReadBinding = strict({ runId: RunId });
const ReadyPacket = strict({
  schemaVersion: Type.Literal(1), kind: Type.Literal("DagOwnedWorkerReadyPacketV1"), runId: RunId, runNonce: Type.String({ minLength: 16, maxLength: 256 }),
  expectedRevision: Type.Integer({ minimum: 0 }), expectedSnapshotHash: Hash, ownerEpoch: Type.Integer({ minimum: 1 }),
  reservationId: Id, stageAttemptId: Id, launchIntentId: Id, effectId: Id, workItemId: Id,
  stage: Type.Enum(["F1", "F2", "F3", "F5", "F6"]), taskPacketHash: Hash, configRequestHash: Hash, requestHash: Hash,
  packet: Type.Record(Type.String({ minLength: 1, maxLength: 256 }), Type.Unknown()), dispatchProtocolVersion: Type.Union([Type.Literal(0), Type.Literal(1)]),
  recoveryDirective: Type.Optional(Type.Union([Type.String({ maxLength: 2_000 }), Type.Null()])), readyPacketHash: Hash,
});
const MutationGuard = {
  runId: RunId, runNonce: Type.String({ minLength: 16, maxLength: 256 }), expectedRevision: Type.Integer({ minimum: 0 }), expectedSnapshotHash: Hash,
  ownerEpoch: Type.Integer({ minimum: 0 }), commandId: Id, idempotencyKey: Type.String({ minLength: 1, maxLength: 256 }), occurredAt: Timestamp,
};

export function registerCanonicalDagRuntime(pi: ExtensionAPI, service = new DagConductorServiceV1()): DagConductorServiceV1 {
  const ok = (text: string, details: Record<string, unknown>) => ({ content: [{ type: "text" as const, text }], details });
  let conductorTimer: ReturnType<typeof setInterval> | null = null;
  let conductorContext: any = null;
  let conductorGeneration = 0;
  let lastConductorDiagnostic: string | null = null;
  let widgetContext: any = null;
  let widgetTui: any = null;
  let widgetController: DagWidgetControllerV2 | null = null;

  const refreshWidget = async () => { await widgetController?.refresh(); };
  const disposeWidget = () => {
    const context = widgetContext;
    widgetController?.dispose();
    widgetController = null;
    widgetContext = null;
    widgetTui = null;
    context?.ui?.setWidget?.("canonical-dag-run", undefined);
  };

  const advanceConductor = async (generation = conductorGeneration) => {
    const context = conductorContext;
    if (!context) return;
    try {
      await service.resumeBound(context);
      if (generation === conductorGeneration) lastConductorDiagnostic = null;
    } catch (error) {
      if (generation !== conductorGeneration) return;
      const message = `DAG conductor wake failed: ${String((error as Error).message).slice(0, 512)}`;
      if (message !== lastConductorDiagnostic) console.error(message);
      lastConductorDiagnostic = message;
      widgetController?.failClosed(message);
    }
  };
  const hydrateConductor = async (generation: number) => {
    await advanceConductor(generation);
    if (generation !== conductorGeneration || conductorTimer) return;
    conductorTimer = setInterval(() => { void advanceConductor(generation); }, 1000);
    conductorTimer.unref?.();
  };

  pi.on("session_start", async (_event, ctx: any) => {
    if (conductorTimer) clearInterval(conductorTimer);
    conductorTimer = null;
    disposeWidget();
    conductorGeneration += 1;
    const generation = conductorGeneration;
    conductorContext = ctx;
    void hydrateConductor(generation);
    if (!ctx.hasUI || (ctx.mode && ctx.mode !== "tui") || typeof ctx.ui?.setWidget !== "function") return;
    widgetContext = ctx;
    let controller!: DagWidgetControllerV2;
    controller = new DagWidgetControllerV2({
      read: async () => {
        const binding = await service.binding(ctx).catch(() => null);
        if (!binding) return { kind: "empty" };
        const status = await service.status(ctx, binding.runId);
        if (status.state.completion.state !== "open" && ["completed", "cancelled", "superseded"].includes(status.state.current.run)) return { kind: "terminal" };
        const diagnostic = status.stale ? `STALE READ-ONLY | source r${status.stale.sourceRevision} ${status.stale.sourceSnapshotHash.slice(0, 18)} | observed r${status.stale.newerObservedRevision} | cached ${status.stale.cachedAt}` : null;
        return { kind: "projection", projection: status.projection, fresh: status.stale === null, diagnostic };
      },
      requestRender: () => widgetTui?.requestRender?.(),
      onTerminal: () => {
        if (widgetController !== controller) return;
        controller.dispose();
        widgetController = null;
        widgetContext = null;
        widgetTui = null;
        ctx.ui.setWidget("canonical-dag-run", undefined);
      },
    });
    widgetController = controller;
    ctx.ui.setWidget("canonical-dag-run", (tui: any) => {
      widgetTui = tui;
      return {
        render(width: number) {
          if (!Number.isInteger(width) || width <= 0) return [];
          const safeWidth = width;
          const terminalRows = Math.max(4, tui.terminal?.rows ?? 24);
          const snapshot = controller.snapshot();
          if (snapshot.diagnostic && !snapshot.projection) return [clip(snapshot.diagnostic, safeWidth), clip("FAIL-CLOSED; inspect or reattach the exact run", safeWidth)];
          if (!snapshot.projection) return [];
          try {
            const layout = renderDagWidgetV2(snapshot.projection, safeWidth, terminalRows, new Date().toISOString(), snapshot);
            controller.noteSelectedAliases(layout.activityAliases);
            return layout.lines;
          } catch (error) {
            return [clip(`DAG widget render failed: ${String((error as Error).message).slice(0, 160)}`, safeWidth), clip("FAIL-CLOSED; inspect the exact run", safeWidth)];
          }
        },
        invalidate() {},
      };
    });
    void controller.start().catch((error) => {
      if (widgetController !== controller) return;
      try { controller.failClosed(`DAG widget hydration failed: ${String((error as Error).message).slice(0, 160)}`); } catch { /* widget callbacks must not block session startup */ }
    });
    await new Promise<void>((resolveYield) => setImmediate(resolveYield));
  });
  pi.on("before_agent_start", async (event: any, ctx: any) => {
    const binding = await service.binding(ctx).catch(() => null);
    if (!binding) return;
    const status = await service.status(ctx, binding.runId).catch(() => null);
    if (!status || status.stale || status.state.completion.state !== "open") return;
    const guidance = [
      "CANONICAL DAG ORCHESTRATION MODE:",
      `Bound run: ${binding.runId}. Call dag_run_status to read its exact current readyPackets and reconciliation state.`,
      "Owned-worker launches are agent-driven only. For each actionable packet, call dag_run_dispatch with that one unchanged packet and an optional bounded tactical directive, then refresh status before dispatching another packet.",
      "Never use generic subagent tools for canonical DAG work. Timer, session, agent_end, and worker-completion wakes only reconcile durable state and never launch a fresh worker.",
      "Continue only orchestration work independent of running workers. When remaining progress depends on them, keep the parent task in progress and end the turn; completion follow-ups resume orchestration automatically.",
    ].join("\n");
    return { systemPrompt: `${event.systemPrompt}\n\n${guidance}` };
  });
  pi.on("agent_end", async () => { await advanceConductor(); await refreshWidget(); });
  pi.on("session_shutdown", async () => {
    if (conductorTimer) clearInterval(conductorTimer);
    conductorTimer = null;
    conductorGeneration += 1;
    conductorContext = null;
    disposeWidget();
    await service.detach();
  });

  pi.registerTool({
    name: "dag_run_status", label: "Canonical DAG Run Status", description: "Read one exact session-bound canonical DAG run and its stable execution projection.", parameters: ReadBinding,
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId);
      const summary = result.projection.summary;
      const dispatch = result.readyPackets.length ? `\nownedWorkerReady=${result.readyPackets.length}; dispatch one unchanged readyPacket with dag_run_dispatch, then refresh dag_run_status before another (never generic subagent)` : "\nownedWorkerReady=0; no owned-worker dispatch is currently actionable";
      return ok(`DAG ${params.runId} r${result.state.revision} ${result.state.current.run}/${result.state.completion.state}\nready=${summary.ready} active=${summary.activeLanes} attention=${summary.attention} integrationReady=${summary.integrationReady} complete=${summary.complete}${dispatch}`, result as any);
    },
  });
  pi.registerTool({
    name: "dag_run_dispatch", label: "Canonical DAG Owned-Worker Dispatch", description: "The only operation allowed to cross exact mark-dispatching, owned-worker launch, and binding for one unchanged agent-visible ready packet. Dispatch one packet, then refresh dag_run_status before dispatching another. Replays are idempotent only with the identical normalized tactical directive and canonical prompt.",
    parameters: strict({ readyPacket: ReadyPacket, tacticalDirective: Type.Optional(Type.String({ maxLength: 2_000 })), occurredAt: Timestamp }),
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.dispatch(ctx, params.readyPacket as any, params.tacticalDirective, params.occurredAt);
      await refreshWidget();
      return ok(`${result.idempotentReplay ? "Replayed" : "Dispatched"} exact owned worker ${result.binding.workerId} for ${result.binding.stageAttemptId}; durable binding at r${result.state.revision}. End the turn at the dependency barrier; the completion follow-up will resume canonical orchestration after durable reconciliation.`, result as any);
    },
  });
  pi.registerTool({
    name: "dag_run_diagram", label: "Canonical DAG Run Diagram", description: "Render a bounded read-only diagram from one exact canonical execution projection.", parameters: strict({ runId: RunId, width: Type.Optional(Type.Integer({ minimum: 20, maximum: 400 })), terminalRows: Type.Optional(Type.Integer({ minimum: 4, maximum: 300 })), observedAt: Timestamp }),
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId); const layout = renderDagWidgetV2(result.projection, params.width ?? 100, params.terminalRows ?? 36, params.observedAt);
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
    async execute(_id, params, _signal, _update, ctx) { const result = await service.start(ctx, params); await refreshWidget(); return ok(`Initialized canonical DAG ${result.state.runId} at r${result.state.revision}. Inspect dag_run_status, dispatch one actionable readyPacket, then refresh status before another; never use generic subagent. Scheduler notice: ${result.decision.notice}`, result as any); },
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

function clip(value: string, width: number): string {
  if (width <= 0) return "";
  return visibleWidth(value) <= width ? value : truncateToWidth(value, width, "…");
}
