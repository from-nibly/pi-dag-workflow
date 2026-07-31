import { readFileSync } from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { assertAttemptConfig, MAX_REPORT_BYTES } from "./core.mjs";

const REPORT_TOOL = "subagent_report";
const OMITTED_PARENT_TOOLS = new Set(["subagent", "subagent_status", "subagent_inspect", "subagent_tail", "subagent_cancel", "subagent_retry"]);

export function isWorkerChildRole(): boolean { return process.env.PI_DAG_WORKER_ROLE === "child"; }

export function registerWorkerChild(pi: ExtensionAPI): void {
  const config = loadChildConfig();
  pi.registerTool({
    name: REPORT_TOOL,
    label: "Submit Subagent Report",
    description: "Submit the bounded terminal worker report. This must be your final action and ends the worker run.",
    parameters: Type.Object({
      outcome: Type.Union([Type.Literal("completed"), Type.Literal("needs_attention")]),
      summary: Type.String({ minLength: 1, maxLength: 8192 }),
      details: Type.Optional(Type.String({ maxLength: 32768 })),
      artifacts: Type.Optional(Type.Array(Type.Object({
        path: Type.String({ minLength: 1, maxLength: 4096 }),
        label: Type.Optional(Type.String({ maxLength: 256 })),
      }), { maxItems: 32 })),
      nextSteps: Type.Optional(Type.Array(Type.String({ minLength: 1, maxLength: 1024 }), { maxItems: 16 })),
    }),
    async execute(_toolCallId, params) {
      const report = {
        outcome: params.outcome,
        summary: params.summary.trim(),
        ...(params.details?.trim() ? { details: params.details.trim() } : {}),
        ...(params.artifacts?.length ? { artifacts: params.artifacts.map((artifact) => ({ path: artifact.path, ...(artifact.label?.trim() ? { label: artifact.label.trim() } : {}) })) } : {}),
        ...(params.nextSteps?.length ? { nextSteps: params.nextSteps.map((step) => step.trim()).filter(Boolean) } : {}),
      };
      if (!report.summary) throw new Error("subagent_report summary must not be empty");
      const bytes = Buffer.byteLength(JSON.stringify(report));
      if (bytes > MAX_REPORT_BYTES) throw new Error(`subagent_report exceeds ${MAX_REPORT_BYTES} bytes`);
      return {
        content: [{ type: "text" as const, text: "Terminal subagent report accepted." }],
        details: { schemaVersion: 1, report, bytes },
        terminate: true,
      };
    },
  });

  pi.on("session_start", async () => {
    const registered = new Set((pi.getAllTools?.() ?? []).map((tool: any) => typeof tool === "string" ? tool : tool.name));
    const active = config.activeTools
      .filter((name: string) => registered.has(name))
      .filter((name: string) => !isParentOrchestrationTool(name));
    if (registered.has(REPORT_TOOL)) active.push(REPORT_TOOL);
    pi.setActiveTools([...new Set(active)]);
  });

  pi.on("before_agent_start", (event: any) => ({
    systemPrompt: `${event.systemPrompt}\n\nYou are an asynchronous child worker. Complete only the assigned task. Do not delegate to subagents, operate DAG orchestration, or mutate project-model authority. When finished—or when blocked—call ${REPORT_TOOL} exactly once as your final action. Use outcome=needs_attention for incomplete or unsafe work. Do not merely print a completion summary: the terminating report tool is required.`,
  }));
}

function isParentOrchestrationTool(name: string): boolean {
  return OMITTED_PARENT_TOOLS.has(name) || (name.startsWith("subagent_") && name !== REPORT_TOOL) || name.startsWith("dag_") || name.startsWith("dag_model_");
}

function loadChildConfig(): any {
  const path = process.env.PI_DAG_WORKER_CONFIG;
  if (!path) throw new Error("PI_DAG_WORKER_CONFIG is required in child role");
  const config = JSON.parse(readFileSync(path, "utf8"));
  assertAttemptConfig(config);
  return config;
}
