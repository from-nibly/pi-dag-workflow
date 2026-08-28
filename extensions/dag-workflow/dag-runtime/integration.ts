import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { Type } from "typebox";
import { DagConductorServiceV1 } from "./conductor.ts";
import { renderDagWidgetV2 } from "./widget.ts";
import { DagWidgetControllerV2 } from "./widget-controller.ts";

const Id = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$" });
const RunId = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" });
const Hash = Type.String({ pattern: "^sha256:[0-9a-f]{64}$" });
const Timestamp = Type.String({ pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z$" });
const strict = <T extends Record<string, any>>(properties: T) => Type.Object(properties, { additionalProperties: false });
const ReadBinding = strict({ runId: RunId });
const Stage = Type.Enum(["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"]);

export function registerCanonicalDagRuntime(pi: ExtensionAPI, service = new DagConductorServiceV1()): DagConductorServiceV1 {
  const ok = (text: string, details: Record<string, unknown>) => ({ content: [{ type: "text" as const, text }], details });
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


  pi.on("session_start", async (_event, ctx: any) => {
    disposeWidget();
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
    const guidance = [
      "CANONICAL DAG ORCHESTRATION MODE:",
      `Bound run: ${binding.runId}. Call dag_next_action to read the full current semantic choices.`,
      "You are the visible orchestrator. Choose admissible frontier actions and invoke only the named DAG semantic tools: dag_start_work, dag_run_checks, dag_record_completion, dag_integrate, dag_retry, dag_pause, dag_resume, dag_cancel, and dag_finalize.",
      "Pass the exact actionId printed for the selected action. Never use generic subagent tools for canonical DAG work and never invent or transport revisions, hashes, epochs, locks, packets, or idempotency fields; each actionId binds and revalidates them internally.",
      "Worker completion follow-ups are notifications only. Follow their exact recovery guidance, then use dag_next_action to select the current recovery, recording, or finalization action. There is no autonomous timer, session, agent_end, or completion mutation pump.",
      "Every choice is revision-bound: invoke one mutation, then call dag_next_action again before selecting another. When progress depends on running workers, keep the parent task in progress and end the turn; an owned-worker completion callback will wake you without an arbitrary timeout.",
    ].join("\n");
    return { systemPrompt: `${event.systemPrompt}\n\n${guidance}` };
  });
  pi.on("session_shutdown", async () => {
    disposeWidget();
    await service.detach();
  });

  pi.registerTool({
    name: "dag_next_action", label: "Canonical DAG Next Action", description: "Read the full current semantic choices for one exact session-bound canonical DAG run. Choices share one revision and must be refreshed after each mutation. This operation is read-only.", parameters: ReadBinding,
    async execute(_id, params, signal, _update, ctx) {
      const result = await service.nextAction(ctx, params.runId, signal);
      const lines = result.frontier.map((item) => `${item.actionId} ${item.operation} ${item.workItemId ?? "run"}${item.stage ? `/${item.stage}` : ""}${item.completionId ? ` completion=${item.completionId}` : ""} — ${item.explanation}`);
      const controls = result.controls.map((item) => `${item.actionId} ${item.operation} run — ${item.explanation}`);
      return ok(`DAG ${params.runId} r${result.revision} choices=${result.frontier.length}${result.waiting ? " waiting on owned workers or external authority" : ""}\nChoose one mutation, then refresh dag_next_action.\n${[...lines, ...controls].join("\n") || `No semantic action; scheduler=${result.notice}`}`, result as any);
    },
  });
  pi.registerTool({
    name: "dag_run_status", label: "Canonical DAG Run Status", description: "Historical read-only exact run projection. Use dag_next_action for orchestration.", parameters: ReadBinding,
    async execute(_id, params, _signal, _update, ctx) {
      const result = await service.status(ctx, params.runId); const summary = result.projection.summary;
      return ok(`DAG ${params.runId} r${result.state.revision} ${result.state.current.run}/${result.state.completion.state}\nready=${summary.ready} active=${summary.activeLanes} attention=${summary.attention} integrationReady=${summary.integrationReady} complete=${summary.complete}\nUse dag_next_action for the semantic frontier.`, result as any);
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
    async execute(_id, params, _signal, _update, ctx) { const result = await service.start(ctx, params); await refreshWidget(); return ok(`Initialized canonical DAG ${result.state.runId} at r${result.state.revision}. Call dag_next_action; no lifecycle work runs autonomously. Scheduler notice: ${result.decision.notice}`, result as any); },
  });
  pi.registerTool({
    name: "dag_start_work", label: "Canonical DAG Start Work", description: "Reserve and launch one currently admissible owned-worker stage. Internal packet, revision, hash, epoch, lock, and idempotency guards are derived and revalidated by the tool.",
    parameters: strict({ runId: RunId, actionId: Id, workItemId: Id, stage: Stage, tacticalDirective: Type.Optional(Type.Union([Type.String({ maxLength: 2_000 }), Type.Null()])) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.startWork(ctx, params.runId, params.actionId, params.workItemId, params.stage, params.tacticalDirective, signal); await refreshWidget(); return ok(`Started exact owned work ${params.workItemId}/${params.stage} as ${result.binding.workerId}; canonical binding at r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_run_checks", label: "Canonical DAG Run Checks", description: "Run and canonically close one currently admissible synchronous F0-F8 check operation with internally derived guards.",
    parameters: strict({ runId: RunId, actionId: Id, workItemId: Id, stage: Stage }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.runChecks(ctx, params.runId, params.actionId, params.workItemId, params.stage, signal); await refreshWidget(); return ok(`Ran canonical checks for ${params.workItemId}/${params.stage}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_record_completion", label: "Canonical DAG Record Completion", description: "Validate one exact durable owned-worker binding and record exactly its notified completion. The callback itself never mutates DAG state.",
    parameters: strict({ runId: RunId, actionId: Id, stageAttemptId: Id, completionId: Id }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.recordCompletion(ctx, params.runId, params.actionId, params.stageAttemptId, params.completionId, signal); await refreshWidget(); return ok(`Recorded exact completion ${params.completionId} for ${params.stageAttemptId}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_integrate", label: "Canonical DAG Integrate", description: "Run one currently admissible exact Git integration operation with durable integration locks and CAS derived internally.",
    parameters: strict({ runId: RunId, actionId: Id, workItemId: Id, stage: Type.Optional(Stage) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.integrateSemantic(ctx, params.runId, params.actionId, params.workItemId, params.stage ?? "F8", signal); await refreshWidget(); return ok(`Integrated exact canonical prefix for ${params.workItemId}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_retry", label: "Canonical DAG Retry", description: "Authorize one frontier retry by durable retry-key; count, fingerprint, generation, CAS, and idempotency are derived internally.",
    parameters: strict({ runId: RunId, actionId: Id, retryKey: Hash }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.retrySemantic(ctx, params.runId, params.actionId, params.retryKey, signal); await refreshWidget(); return ok(`Authorized exact retry ${params.retryKey}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_pause", label: "Canonical DAG Pause", description: "Pause new work admission for the exact action snapshot while preserving in-flight durable authority.",
    parameters: strict({ runId: RunId, actionId: Id, reason: Type.String({ minLength: 1, maxLength: 4096 }) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.pauseSemantic(ctx, params.runId, params.actionId, params.reason, signal); await refreshWidget(); return ok(`Paused canonical DAG ${params.runId}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_resume", label: "Canonical DAG Resume", description: "Resume one exact paused canonical run with internally derived mutation guards.",
    parameters: strict({ runId: RunId, actionId: Id, reason: Type.String({ minLength: 1, maxLength: 4096 }) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.resumeSemantic(ctx, params.runId, params.actionId, params.reason, signal); await refreshWidget(); return ok(`Resumed canonical DAG ${params.runId}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_cancel", label: "Canonical DAG Cancel", description: "Request cancellation of the exact bound run and derive every canonical cancellation guard and worker fence internally.",
    parameters: strict({ runId: RunId, actionId: Id, reason: Type.String({ minLength: 1, maxLength: 4096 }) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.cancelSemantic(ctx, params.runId, params.actionId, params.reason, signal); await refreshWidget(); return ok(`Requested exact cancellation of ${params.runId}; r${result.state.revision}.`, result as any); },
  });
  pi.registerTool({
    name: "dag_finalize", label: "Canonical DAG Finalize", description: "Finalize one exact recorded worker result or pending run-level cancellation/cleanup operation.",
    parameters: strict({ runId: RunId, actionId: Id, stageAttemptId: Type.Optional(Type.Union([Id, Type.Null()])) }),
    async execute(_id, params, signal, _update, ctx) { const result = await service.finalizeSemantic(ctx, params.runId, params.actionId, params.stageAttemptId, signal); await refreshWidget(); return ok(`Finalized canonical DAG state for ${params.stageAttemptId ?? params.runId}; r${result.state.revision}.`, result as any); },
  });
  return service;
}

function clip(value: string, width: number): string {
  if (width <= 0) return "";
  return visibleWidth(value) <= width ? value : truncateToWidth(value, width, "…");
}
