import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { randomBytes } from "node:crypto";
import type {
  DagFile,
  DagFlowStep,
  DagNode,
  DagStep,
  NodeRunState,
  RunManifest,
  RunState,
  ValidationResult,
} from "./types.ts";
import { AI_DIR, DEFAULT_DAG_PATH } from "./types.ts";

export function nowIso(): string {
  return new Date().toISOString();
}

export function slugify(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || "dag-run";
}

export function toAbsolute(cwd: string, path: string): string {
  return isAbsolute(path) ? path : resolve(cwd, path);
}

export function runDir(cwd: string, runId: string): string {
  return join(cwd, AI_DIR, "runs", runId);
}

export async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

async function readJson<T>(path: string): Promise<T> {
  return JSON.parse(await readFile(path, "utf8")) as T;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await ensureDir(dirname(path));
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export async function readDag(cwd: string, dagPath = DEFAULT_DAG_PATH): Promise<DagFile> {
  const dag = await readJson<DagFile>(toAbsolute(cwd, dagPath));
  dag.edges ??= [];
  dag.defaults ??= { flow: "default" };
  dag.steps ??= [];
  dag.flows ??= {};
  dag.nodeFlowOverrides ??= [];
  dag.merge ??= { id: "merge", kind: "merge" };
  return dag;
}

function asStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function validateStep(step: Partial<DagStep>, label: string, errors: string[]) {
  if (!step.id || typeof step.id !== "string") errors.push(`${label}: id is required`);
  if (step.kind !== undefined && step.kind !== "agent" && step.kind !== "merge") errors.push(`${label}: unsupported kind ${String(step.kind)}`);
  if (step.requires !== undefined && !asStringArray(step.requires)) errors.push(`${label}: requires must be an array of strings`);
}

export async function validateDag(cwd: string, dagPath = DEFAULT_DAG_PATH): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];
  let dag: DagFile | undefined;
  try {
    dag = await readDag(cwd, dagPath);
  } catch (error) {
    return { valid: false, errors: [`Could not read ${dagPath}: ${error instanceof Error ? error.message : String(error)}`], warnings };
  }

  if (dag.schemaVersion !== 1) errors.push(`Unsupported schemaVersion: ${String(dag.schemaVersion)}`);
  if (!dag.run?.name) errors.push("run.name is required");
  if (!dag.run?.plan) errors.push("run.plan is required");
  if (!Number.isInteger(dag.run?.maxConcurrency) || dag.run.maxConcurrency < 1) errors.push("run.maxConcurrency must be a positive integer");
  if (!Array.isArray(dag.nodes) || dag.nodes.length === 0) errors.push("nodes must be a non-empty array");
  if (!Array.isArray(dag.edges)) errors.push("edges must be an array");
  if (!Array.isArray(dag.steps)) errors.push("steps must be an array");

  const stepIds = new Set<string>();
  for (const [index, step] of (dag.steps ?? []).entries()) {
    validateStep(step, `step at index ${index}`, errors);
    if (step.id) {
      if (stepIds.has(step.id)) errors.push(`Duplicate step id: ${step.id}`);
      stepIds.add(step.id);
    }
  }

  validateStep(dag.merge, "merge", errors);
  if (dag.merge?.kind !== "merge") errors.push("merge.kind must be merge");
  if (stepIds.has("merge")) errors.push("merge must be top-level only; do not include a merge step in steps");

  for (const [flowName, flow] of Object.entries(dag.flows ?? {})) {
    if (!Array.isArray(flow)) {
      errors.push(`flow ${flowName}: must be an array`);
      continue;
    }
    for (const [flowIndex, flowStep] of flow.entries()) {
      validateStep(flowStep, `flow ${flowName}[${flowIndex}]`, errors);
      if (flowStep.id === "merge") errors.push(`flow ${flowName}[${flowIndex}]: merge is implicit and must not be listed in flows`);
      if (flowStep.id && !stepIds.has(flowStep.id)) errors.push(`flow ${flowName}[${flowIndex}]: unknown step id ${flowStep.id}`);
    }
  }

  if (dag.nodeFlowOverrides !== undefined && !Array.isArray(dag.nodeFlowOverrides)) errors.push("nodeFlowOverrides must be an array");
  for (const [index, override] of Array.isArray(dag.nodeFlowOverrides) ? dag.nodeFlowOverrides.entries() : []) {
    if (!override?.match || typeof override.match !== "string") errors.push(`nodeFlowOverrides[${index}]: match is required`);
    if (!override?.flow || typeof override.flow !== "string") errors.push(`nodeFlowOverrides[${index}]: flow is required`);
    else if (!dag.flows?.[override.flow]) errors.push(`nodeFlowOverrides[${index}]: flow references unknown flow ${override.flow}`);
  }

  const defaultFlow = dag.defaults?.flow ?? "default";
  if (!dag.flows?.[defaultFlow]) errors.push(`defaults.flow references unknown flow ${defaultFlow}`);

  const nodeIds = new Set<string>();
  for (const [index, node] of (dag.nodes ?? []).entries()) {
    const label = node.id ? `node ${node.id}` : `node at index ${index}`;
    if (!node.id) errors.push(`${label}: id is required`);
    if (node.id && nodeIds.has(node.id)) errors.push(`Duplicate node id: ${node.id}`);
    if (node.id) nodeIds.add(node.id);
    if (!node.title) errors.push(`${label}: title is required`);
    if (!node.chunkFile) errors.push(`${label}: chunkFile is required`);
    if (!asStringArray(node.dependsOn)) errors.push(`${label}: dependsOn must be an array`);
    if (!asStringArray(node.ownedFiles)) errors.push(`${label}: ownedFiles must be an array`);
    if (!asStringArray(node.forbiddenFiles)) errors.push(`${label}: forbiddenFiles must be an array`);
    if (node.chunkFile && !existsSync(toAbsolute(cwd, node.chunkFile))) errors.push(`${label}: chunkFile does not exist: ${node.chunkFile}`);
    const flow = getNodeFlowName(dag, node);
    if (!dag.flows?.[flow]) errors.push(`${label}: flow references unknown flow ${flow}`);
  }

  for (const edge of dag.edges ?? []) {
    if (!nodeIds.has(edge.from)) errors.push(`edge: unknown from node ${edge.from}`);
    if (!nodeIds.has(edge.to)) errors.push(`edge: unknown to node ${edge.to}`);
  }

  for (const node of dag.nodes ?? []) {
    for (const dep of node.dependsOn ?? []) {
      if (!nodeIds.has(dep)) errors.push(`node ${node.id}: dependsOn references unknown node ${dep}`);
    }
  }

  return { valid: errors.length === 0, errors, warnings, dag };
}

export function getNode(dag: DagFile, nodeId: string): DagNode | undefined {
  return dag.nodes.find((node) => node.id === nodeId);
}

function globishToRegExp(pattern: string): RegExp {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^${escaped.replace(/\*/g, ".*").replace(/\?/g, ".")}$`, "i");
}

function matchesNodeFlowOverride(match: string, node: DagNode): boolean {
  const trimmed = match.trim();
  if (!trimmed) return false;
  const values = [node.id, node.title, node.chunkFile, basename(node.chunkFile)].filter(Boolean);
  if (values.some((value) => value === trimmed)) return true;
  if (!/[?*]/.test(trimmed)) return false;
  const pattern = globishToRegExp(trimmed);
  return values.some((value) => pattern.test(value));
}

export function getNodeFlowName(dag: DagFile, node: DagNode): string {
  let overrideFlow: string | undefined;
  for (const override of dag.nodeFlowOverrides ?? []) {
    if (typeof override?.flow === "string" && typeof override.match === "string" && matchesNodeFlowOverride(override.match, node)) {
      overrideFlow = override.flow;
    }
  }
  return overrideFlow ?? node.flow ?? dag.defaults.flow;
}

export function getFlow(dag: DagFile, node: DagNode): DagFlowStep[] {
  return dag.flows[getNodeFlowName(dag, node)] ?? [];
}

export function resolveStep(dag: DagFile, node: DagNode, flowIndex: number): DagStep {
  const flow = getFlow(dag, node);
  if (flowIndex < flow.length) {
    const flowStep = flow[flowIndex];
    const base = dag.steps.find((step) => step.id === flowStep.id);
    if (!base) throw new Error(`Flow step references unknown step: ${flowStep.id}`);
    return { ...base, ...flowStep };
  }
  if (flowIndex === flow.length) return dag.merge;
  throw new Error(`Step index out of range: ${flowIndex}`);
}

export function stepKey(nodeId: string, stepId: string, flowIndex: number, attempt: number): string {
  return `${nodeId}-${flowIndex}-${stepId}-${attempt}`;
}

export async function createRun(cwd: string, dagPath = DEFAULT_DAG_PATH): Promise<RunState> {
  const dag = await readDag(cwd, dagPath);
  const runId = `${slugify(dag.run.name)}-${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}-${randomBytes(3).toString("hex")}`;
  const manifest: RunManifest = {
    runId,
    dagPath,
    dagName: dag.run.name,
    cwd,
    parentBranch: "",
    baseCommit: "",
    createdAt: nowIso(),
    updatedAt: nowIso(),
    status: "running",
  };
  const nodes: Record<string, NodeRunState> = {};
  for (const node of dag.nodes) {
    nodes[node.id] = {
      id: node.id,
      status: "pending",
      flow: getNodeFlowName(dag, node),
      currentFlowIndex: 0,
      attempts: [],
      updatedAt: nowIso(),
    };
  }
  const state = { manifest, nodes };
  await saveRunState(cwd, state);
  return state;
}

export async function saveRunState(cwd: string, state: RunState): Promise<void> {
  state.manifest.updatedAt = nowIso();
  const dir = runDir(cwd, state.manifest.runId);
  await writeJson(join(dir, "manifest.json"), state.manifest);
  await writeJson(join(dir, "nodes.json"), state.nodes);
}

export async function loadRun(cwd: string, runId: string): Promise<RunState> {
  const dir = runDir(cwd, runId);
  return {
    manifest: await readJson<RunManifest>(join(dir, "manifest.json")),
    nodes: await readJson<Record<string, NodeRunState>>(join(dir, "nodes.json")),
  };
}

export async function findLatestRun(cwd: string): Promise<string | undefined> {
  const runsRoot = join(cwd, AI_DIR, "runs");
  try {
    const entries = await readdir(runsRoot, { withFileTypes: true });
    const dirs = entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
    return dirs.at(-1);
  } catch {
    return undefined;
  }
}

export function dependenciesSatisfied(state: RunState, node: DagNode): boolean {
  return (node.dependsOn ?? []).every((dep) => state.nodes[dep]?.status === "merged");
}

export function getReadyNodeIds(dag: DagFile, state: RunState): string[] {
  return dag.nodes
    .filter((node) => state.nodes[node.id]?.status === "pending" && dependenciesSatisfied(state, node))
    .map((node) => node.id);
}

export function summarizeRun(state: RunState): string {
  const counts = Object.values(state.nodes).reduce<Record<string, number>>((acc, node) => {
    acc[node.status] = (acc[node.status] ?? 0) + 1;
    return acc;
  }, {});
  return `run ${state.manifest.runId} ${state.manifest.status} ${JSON.stringify(counts)}`;
}

export async function appendEvent(cwd: string, runId: string, event: unknown): Promise<void> {
  const path = join(runDir(cwd, runId), "events.jsonl");
  await ensureDir(dirname(path));
  const line = `${JSON.stringify({ timestamp: nowIso(), ...event })}\n`;
  const previous = existsSync(path) ? await readFile(path, "utf8") : "";
  await writeFile(path, previous + line, "utf8");
}
