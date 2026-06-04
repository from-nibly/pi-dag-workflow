import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { DagFile, DagNode, DagStep } from "./types.ts";
import { getFlow, stepKey } from "./dag.ts";

const extensionDir = dirname(fileURLToPath(import.meta.url));

export function resolveAgentName(agent: string | undefined): string {
  if (!agent) return "worker";
  if (agent === "builtin:worker") return "worker";
  if (agent === "builtin:reviewer") return "reviewer";
  if (agent.startsWith("builtin:")) return agent.slice("builtin:".length);
  return agent;
}

export async function loadPrompt(prompt: string | undefined): Promise<string> {
  if (!prompt) return "";
  if (prompt.startsWith("builtin:")) {
    const name = prompt.slice("builtin:".length);
    const path = join(extensionDir, "step-prompts", `${name}.md`);
    return await readFile(path, "utf8").catch(() => "");
  }
  return prompt;
}

export function extractVerdict(text: string | undefined): "PASS" | "FAIL" | undefined {
  const match = text?.match(/VERDICT:\s*(PASS|FAIL)/i);
  return match ? (match[1]!.toUpperCase() as "PASS" | "FAIL") : undefined;
}

export async function buildStepTask(dag: DagFile, node: DagNode, step: DagStep): Promise<string> {
  const prompt = await loadPrompt(step.prompt);
  const parts = [
    prompt,
    `# DAG Step\n\nStep id: ${step.id}\nKind: ${step.kind}`,
    step.input ? `## Input\n\n${step.input}` : undefined,
    step.output ? `## Expected output\n\n${step.output}` : undefined,
    step.requires?.length ? `## Requires\n\n${step.requires.map((item) => `- ${item}`).join("\n")}` : undefined,
    `## Node\n\nNode id: ${node.id}\nTitle: ${node.title}\nChunk file: ${node.chunkFile}`,
    node.setupInstructions ? `## Setup instructions\n\n${node.setupInstructions}` : undefined,
    node.implementationInstructions ? `## Implementation instructions\n\n${node.implementationInstructions}` : undefined,
    node.validationInstructions ? `## Validation instructions\n\n${node.validationInstructions}` : undefined,
    `## Run context\n\nPlan: ${dag.run.plan}\nBrief: ${dag.run.brief ?? "(none)"}`,
  ];
  return parts.filter(Boolean).join("\n\n");
}

export async function buildSubagentParams(args: {
  dag: DagFile;
  node: DagNode;
  step: DagStep;
  cwd: string;
  parentCwd?: string;
  runId: string;
  flowIndex: number;
  attempt: number;
}) {
  const { dag, node, step, cwd, runId, flowIndex, attempt } = args;
  const parentCwd = args.parentCwd ?? cwd;
  const key = stepKey(node.id, step.id, flowIndex, attempt);
  const reads = [dag.run.plan, node.chunkFile, dag.run.brief].filter((item): item is string => typeof item === "string");
  const params: Record<string, unknown> = {
    agent: resolveAgentName(step.agent),
    task: await buildStepTask(dag, node, step),
    cwd,
    async: true,
    context: "fresh",
    sessionDir: `${parentCwd}/.ai/runs/${runId}/subagent-sessions/${key}`,
    output: `${parentCwd}/.ai/runs/${runId}/artifacts/${key}.md`,
    outputMode: "file-only",
    reads,
  };
  if (step.model) params.model = step.model;
  if (step.thinking) params.thinking = step.thinking;
  return params;
}

export function describeFlow(dag: DagFile, node: DagNode): string {
  const flow = getFlow(dag, node);
  return [...flow.map((step, i) => `${i}:${step.id}`), `${flow.length}:merge`].join(" -> ");
}
