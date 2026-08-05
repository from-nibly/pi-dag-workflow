import { access } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { WorkerManager } from "./manager.mjs";

export function registerWorkerRuntime(pi: ExtensionAPI) {
  const manager = new WorkerManager(pi);
  let attachError: string | null = null;

  pi.registerTool({
    name: "subagent",
    label: "Launch Async Subagent",
    description: "Launch an always-asynchronous process-isolated Pi worker. Returns immediately; completion is delivered later through the serial completion queue.",
    parameters: Type.Object({
      task: Type.String({ minLength: 1, maxLength: 65536 }),
      launchKey: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })),
      label: Type.Optional(Type.String({ maxLength: 256 })),
      cwd: Type.Optional(Type.String()),
      disposableRootToken: Type.Optional(Type.String({ minLength: 1 })),
      provider: Type.Optional(Type.String()),
      model: Type.Optional(Type.String()),
      thinking: Type.Optional(Type.Union([Type.Literal("off"), Type.Literal("minimal"), Type.Literal("low"), Type.Literal("medium"), Type.Literal("high"), Type.Literal("xhigh"), Type.Literal("max")])),
      reportRepairAttempts: Type.Optional(Type.Integer({ minimum: 0, maximum: 2 })),
    }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      assertAttached(attachError);
      const result = await manager.launch(params, ctx);
      return toolResult(`${result.idempotentReplay ? "Reused" : "Started"} asynchronous worker ${result.workerId} (attempt ${result.attemptNumber}).`, result);
    },
  });

  pi.registerTool({
    name: "subagent_approve_disposable_root",
    label: "Approve Disposable Worker Root",
    description: "Approve an exact path/inode below a configured disposable-root parent and return an owner-bound launch token.",
    parameters: Type.Object({ cwd: Type.String({ minLength: 1 }) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.approveDisposableWorkingRoot(params.cwd);
      return jsonToolResult(result, { approvalId: result.approvalId });
    },
  });

  pi.registerTool({
    name: "subagent_retire_disposable_root",
    label: "Retire Disposable Worker Root",
    description: "Retire an owner-bound disposable-root approval after no active worker uses it.",
    parameters: Type.Object({ disposableRootToken: Type.String({ minLength: 1 }) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.retireDisposableWorkingRoot(params.disposableRootToken);
      return jsonToolResult(result, result);
    },
  });

  pi.registerTool({
    name: "subagent_results",
    label: "Enumerate Durable Worker Results",
    description: "Enumerate durable immutable worker results independently of completion delivery and acknowledgement.",
    parameters: Type.Object({ launchKey: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.listResults(params);
      return jsonToolResult(result, { count: result.length });
    },
  });

  pi.registerTool({
    name: "subagent_result_by_launch_key",
    label: "Read Worker Result By Launch Key",
    description: "Read the latest immutable terminal result for an opaque launch key.",
    parameters: Type.Object({ launchKey: Type.String({ minLength: 1, maxLength: 512 }) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.resultByLaunchKey(params.launchKey);
      return jsonToolResult(result, { found: result !== null });
    },
  });

  pi.registerTool({
    name: "subagent_status",
    label: "Subagent Status",
    description: "List workers for this top-level Pi session or show one worker's compact status.",
    parameters: Type.Object({ workerId: Type.Optional(Type.String()) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.status(params.workerId);
      return jsonToolResult(result, { result });
    },
  });

  pi.registerTool({
    name: "subagent_inspect",
    label: "Inspect Subagent",
    description: "Inspect one worker or completion, including its bounded immutable terminal result when available.",
    parameters: Type.Object({ workerId: Type.String({ minLength: 1 }) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = publicInspection(await manager.inspect(params.workerId));
      return jsonToolResult(result, { workerId: result.worker.id, status: result.worker.status }, 96 * 1024);
    },
  });

  pi.registerTool({
    name: "subagent_tail",
    label: "Tail Subagent Diagnostics",
    description: "Read a bounded tail of selected worker lifecycle/tool diagnostics. Cumulative message_update events are never logged.",
    parameters: Type.Object({
      workerId: Type.String({ minLength: 1 }),
      attemptNumber: Type.Optional(Type.Integer({ minimum: 1 })),
      lines: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000 })),
      maxBytes: Type.Optional(Type.Integer({ minimum: 1024, maximum: 262144 })),
    }),
    async execute(_id, params) {
      assertAttached(attachError);
      const text = await manager.tail(params.workerId, params);
      return toolResult(text || "No diagnostic output.", { workerId: params.workerId, attemptNumber: params.attemptNumber ?? null });
    },
  });

  pi.registerTool({
    name: "subagent_cancel",
    label: "Cancel Subagent",
    description: "Request cancellation after proving the current detached supervisor's PID/start identity and attempt generation.",
    parameters: Type.Object({ workerId: Type.String({ minLength: 1 }), reason: Type.Optional(Type.String({ maxLength: 1024 })) }),
    async execute(_id, params) {
      assertAttached(attachError);
      const result = await manager.cancel(params.workerId, params.reason);
      return toolResult(`Worker ${params.workerId}: ${result.status}.`, result);
    },
  });

  pi.registerTool({
    name: "subagent_retry",
    label: "Retry Subagent",
    description: "Explicitly launch a new attempt for a terminal worker. The runtime never retries task work automatically.",
    parameters: Type.Object({ workerId: Type.String({ minLength: 1 }) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      assertAttached(attachError);
      const result = await manager.retry(params.workerId, ctx);
      return toolResult(`Started retry attempt ${result.attemptNumber} for ${params.workerId}.`, result);
    },
  });

  pi.registerCommand("workers", {
    description: "Manage asynchronous workers: list|inspect|tail|cancel|retry",
    handler: async (args: string, ctx: any) => {
      try {
        assertAttached(attachError);
        const [command = "list", workerId, ...rest] = splitArgs(args);
        if (command === "list") {
          const workers = await manager.status();
          ctx.ui.notify(workers.length ? workers.map((worker: any) => `${worker.id} — ${worker.status} — ${worker.label}`).join("\n") : "No workers for this session.", "info");
          return;
        }
        if (!workerId) throw new Error(`Usage: /workers ${command} <worker-id>`);
        if (command === "inspect") ctx.ui.notify(boundedJson(publicInspection(await manager.inspect(workerId)), 8000), "info");
        else if (command === "tail") ctx.ui.notify((await manager.tail(workerId, { lines: Number(rest[0] ?? 80) })).slice(-8000) || "No diagnostic output.", "info");
        else if (command === "cancel") ctx.ui.notify(`Worker ${workerId}: ${(await manager.cancel(workerId, rest.join(" ") || "user command")).status}`, "info");
        else if (command === "retry") ctx.ui.notify(`Worker ${workerId}: retry attempt ${(await manager.retry(workerId, ctx)).attemptNumber} started`, "info");
        else throw new Error("Usage: /workers list|inspect|tail|cancel|retry");
      } catch (error: any) { ctx.ui.notify(error.message, "error"); }
    },
  });

  pi.on("session_start", async (_event: any, ctx: any) => {
    try {
      await manager.attach(ctx);
      attachError = null;
      const legacyArtifacts = await findLegacyArtifacts(ctx);
      if (legacyArtifacts.length) ctx.ui.notify(`Legacy pi-subagents artifacts were not adopted or deleted:\n${legacyArtifacts.join("\n")}\nRemove them only after confirming no legacy process is live.`, "warning");
    } catch (error: any) {
      attachError = error.message;
      ctx.ui.notify(`Worker runtime unavailable: ${error.message}`, "error");
    }
  });
  pi.on("session_shutdown", async () => { await manager.detach(); });
  pi.on("agent_settled", async () => { await manager.onAgentSettled(); });

  return manager;
}

function assertAttached(error: string | null) {
  if (error) throw new Error(`Worker runtime is unavailable: ${error}`);
}

function publicInspection(value: any) {
  const copy = structuredClone(value);
  if (copy.result) delete copy.result.resultHash;
  if (copy.attempt) delete copy.attempt.resultHash;
  for (const attempt of copy.worker?.attempts ?? []) delete attempt.resultHash;
  return copy;
}

function toolResult(text: string, details: Record<string, unknown>) {
  return { content: [{ type: "text" as const, text }], details };
}

function jsonToolResult(value: unknown, details: Record<string, unknown>, maxBytes = 64 * 1024) {
  return toolResult(boundedJson(value, maxBytes), details);
}

function boundedJson(value: unknown, maxBytes: number) {
  const text = JSON.stringify(value, null, 2);
  const buffer = Buffer.from(text);
  if (buffer.length <= maxBytes) return text;
  return `${buffer.subarray(0, Math.max(0, maxBytes - 64)).toString()}\n…[truncated; use narrower inspection]`;
}

async function findLegacyArtifacts(ctx: any): Promise<string[]> {
  const paths: string[] = [];
  const sessionFile = ctx.sessionManager.getSessionFile?.();
  if (sessionFile) {
    const sibling = join(dirname(sessionFile), `${basename(sessionFile, ".jsonl")}-dag-subagents`);
    try { await access(sibling); paths.push(sibling); } catch {}
  }
  return paths;
}

function splitArgs(input: string): string[] {
  const values: string[] = [];
  const pattern = /"([^"]*)"|'([^']*)'|([^\s]+)/g;
  let match;
  while ((match = pattern.exec(input))) values.push(match[1] ?? match[2] ?? match[3]);
  return values;
}
