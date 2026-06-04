import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import type { DagFile, DagStep, DagWorkflowConfig } from "./types.ts";
import { PACKAGE_DEFAULT_CONFIG } from "./defaults.ts";

export function expandHome(path: string): string {
  return path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path;
}

export function resolvePath(cwd: string, path: string): string {
  const expanded = expandHome(path);
  return isAbsolute(expanded) ? expanded : resolve(cwd, expanded);
}

export async function readJsonIfExists<T>(path: string): Promise<T | undefined> {
  if (!existsSync(path)) return undefined;
  return JSON.parse(await readFile(path, "utf8")) as T;
}

function mergeObject<T extends Record<string, unknown>>(base: T | undefined, patch: Partial<T> | undefined): T | undefined {
  if (!base && !patch) return undefined;
  return { ...(base ?? {}), ...(patch ?? {}) } as T;
}

function mergeStepArray(base: DagWorkflowConfig["steps"] = [], patch: DagWorkflowConfig["steps"] = []) {
  const order: string[] = [];
  const byId = new Map<string, Record<string, unknown>>();
  for (const step of [...base, ...patch]) {
    if (!step?.id) continue;
    if (!byId.has(step.id)) order.push(step.id);
    byId.set(step.id, { ...(byId.get(step.id) ?? {}), ...step });
  }
  return order.map((id) => byId.get(id) as DagStep);
}

export function mergeConfig(base: DagWorkflowConfig, patch: DagWorkflowConfig | undefined): DagWorkflowConfig {
  if (!patch) return base;
  return {
    ...base,
    ...patch,
    defaults: mergeObject(base.defaults as Record<string, unknown> | undefined, patch.defaults as Record<string, unknown> | undefined) as DagWorkflowConfig["defaults"],
    steps: mergeStepArray(base.steps, patch.steps),
    merge: { id: "merge", kind: "merge", ...(base.merge ?? {}), ...(patch.merge ?? {}) },
    flows: { ...(base.flows ?? {}), ...(patch.flows ?? {}) },
    nodeFlowOverrides: [...(base.nodeFlowOverrides ?? []), ...(patch.nodeFlowOverrides ?? [])],
  };
}

export async function loadWorkflowConfig(cwd: string, inline?: DagWorkflowConfig): Promise<DagWorkflowConfig> {
  const user = await readJsonIfExists<DagWorkflowConfig>(expandHome("~/.pi/agent/extensions/dag-workflow/config.json"));
  const project = await readJsonIfExists<DagWorkflowConfig>(resolve(cwd, ".ai/dag.config.json"));
  return mergeConfig(mergeConfig(mergeConfig(PACKAGE_DEFAULT_CONFIG, user), project), inline);
}

export function configToDagBase(config: DagWorkflowConfig): Pick<DagFile, "defaults" | "steps" | "merge" | "flows"> {
  return {
    defaults: { flow: "default", ...(config.defaults ?? {}) } as DagFile["defaults"],
    steps: (config.steps ?? []) as DagStep[],
    merge: { id: "merge", kind: "merge", ...(config.merge ?? {}) } as DagStep,
    flows: config.flows ?? {},
  };
}

export function packageFile(cwd: string, ...parts: string[]): string {
  return resolve(cwd, ...parts);
}

export function parentDir(path: string): string {
  return dirname(path);
}
