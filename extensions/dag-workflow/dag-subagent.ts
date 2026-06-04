import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { discoverAgents } from "pi-subagents/src/agents/agents.ts";
import { createSubagentExecutor, type SubagentParamsLike } from "pi-subagents/src/runs/foreground/subagent-executor.ts";
import { createResultWatcher } from "pi-subagents/src/runs/background/result-watcher.ts";
import registerSubagentNotify from "pi-subagents/src/runs/background/notify.ts";
import { cleanupAllArtifactDirs, getArtifactsDir } from "pi-subagents/src/shared/artifacts.ts";
import { resolveCurrentSessionId } from "pi-subagents/src/shared/session-identity.ts";
import { cleanupOldChainDirs } from "pi-subagents/src/shared/settings.ts";
import {
  ASYNC_DIR,
  DEFAULT_ARTIFACT_CONFIG,
  RESULTS_DIR,
  SUBAGENT_ASYNC_COMPLETE_EVENT,
  SUBAGENT_ASYNC_STARTED_EVENT,
  type AsyncStartedEvent,
  type Details,
  type SubagentState,
} from "pi-subagents/src/shared/types.ts";
import { SubagentParams } from "pi-subagents/src/extension/schemas.ts";
import { loadConfig } from "pi-subagents/src/extension/config.ts";

function getSubagentSessionRoot(parentSessionFile: string | null): string {
  if (parentSessionFile) {
    const baseName = path.basename(parentSessionFile, ".jsonl");
    const sessionsDir = path.dirname(parentSessionFile);
    return path.join(sessionsDir, `${baseName}-dag-subagents`);
  }
  return path.join(os.tmpdir(), "pi-dag-subagent-sessions");
}

function expandTilde(p: string): string {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  return p;
}

function ensureAccessibleDir(dirPath: string): void {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
    fs.accessSync(dirPath, fs.constants.R_OK | fs.constants.W_OK);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`DAG subagent directory is not accessible: ${dirPath}\n${message}`);
  }
}

function rewriteToolHints(text: string): string {
  return text.replace(/\bsubagent\(/g, "dag_subagent(");
}

function rewriteResultToolHints(result: Awaited<ReturnType<ReturnType<typeof createSubagentExecutor>["execute"]>>) {
  return {
    ...result,
    content: result.content.map((item) => item.type === "text" ? { ...item, text: rewriteToolHints(item.text) } : item),
  };
}

export function registerDagSubagentTool(pi: ExtensionAPI): void {
  const globalStore = globalThis as Record<string, unknown>;
  const cleanupStoreKey = "__piDagSubagentCleanup";
  const previousCleanup = globalStore[cleanupStoreKey];
  if (typeof previousCleanup === "function") {
    try {
      previousCleanup();
    } catch {
      // Best effort cleanup for stale watchers/listeners after extension reload.
    }
  }
  const cleanupFns: Array<() => void> = [];
  const cleanup = () => {
    for (const fn of cleanupFns.splice(0)) {
      try {
        fn();
      } catch {
        // Best effort cleanup; stale listeners should not block reload/shutdown.
      }
    }
  };
  globalStore[cleanupStoreKey] = cleanup;

  ensureAccessibleDir(RESULTS_DIR);
  ensureAccessibleDir(ASYNC_DIR);
  cleanupOldChainDirs();
  cleanupAllArtifactDirs(DEFAULT_ARTIFACT_CONFIG.cleanupDays);

  const config = loadConfig();
  const asyncByDefault = config.asyncByDefault === true;
  const tempArtifactsDir = getArtifactsDir(null);
  const state: SubagentState = {
    baseCwd: "",
    currentSessionId: null,
    asyncJobs: new Map(),
    foregroundRuns: new Map(),
    foregroundControls: new Map(),
    lastForegroundControlId: null,
    pendingForegroundControlNotices: new Map(),
    cleanupTimers: new Map(),
    lastUiContext: null,
    poller: null,
    completionSeen: new Map(),
    watcher: null,
    watcherRestartTimer: null,
    resultFileCoalescer: {
      schedule: () => false,
      clear: () => {},
    },
  };

  const { startResultWatcher, primeExistingResults, stopResultWatcher } = createResultWatcher(
    pi,
    state,
    RESULTS_DIR,
    10 * 60 * 1000,
  );
  startResultWatcher();
  primeExistingResults();
  cleanupFns.push(stopResultWatcher);
  registerSubagentNotify(pi);

  const executor = createSubagentExecutor({
    pi,
    state,
    config,
    asyncByDefault,
    tempArtifactsDir,
    getSubagentSessionRoot,
    expandTilde,
    discoverAgents,
  });

  const asyncStartedUnsubscribe = pi.events.on(SUBAGENT_ASYNC_STARTED_EVENT, (data: unknown) => {
    const info = data as AsyncStartedEvent;
    if (!info.id) return;
    const now = Date.now();
    state.asyncJobs.set(info.id, {
      asyncId: info.id,
      asyncDir: info.asyncDir ?? path.join(ASYNC_DIR, info.id),
      status: "queued",
      pid: typeof info.pid === "number" ? info.pid : undefined,
      ...(typeof info.sessionId === "string" ? { sessionId: info.sessionId } : {}),
      mode: info.mode ?? (info.chain ? "chain" : "single"),
      agents: info.agents?.length ? info.agents : info.chain?.length ? info.chain : info.agent ? [info.agent] : undefined,
      chainStepCount: info.chainStepCount,
      parallelGroups: info.parallelGroups,
      nestedRoute: info.nestedRoute,
      startedAt: now,
      updatedAt: now,
    });
  });

  cleanupFns.push(asyncStartedUnsubscribe);

  const asyncCompleteUnsubscribe = pi.events.on(SUBAGENT_ASYNC_COMPLETE_EVENT, (data: unknown) => {
    const result = data as { id?: string; success?: boolean; asyncDir?: string };
    if (!result.id) return;
    const job = state.asyncJobs.get(result.id);
    if (!job) return;
    job.status = result.success ? "complete" : "failed";
    job.updatedAt = Date.now();
    if (result.asyncDir) job.asyncDir = result.asyncDir;
  });
  cleanupFns.push(asyncCompleteUnsubscribe);

  pi.on("session_start", (_event, ctx: ExtensionContext) => {
    state.baseCwd = ctx.cwd;
    state.currentSessionId = resolveCurrentSessionId(ctx.sessionManager);
    state.lastUiContext = ctx;
  });

  pi.on("session_shutdown", () => {
    cleanup();
    for (const timer of state.cleanupTimers.values()) clearTimeout(timer);
    state.cleanupTimers.clear();
    state.asyncJobs.clear();
    state.foregroundControls.clear();
    state.foregroundRuns?.clear();
    state.lastForegroundControlId = null;
    if (state.poller) clearInterval(state.poller);
    state.poller = null;
    if (globalStore[cleanupStoreKey] === cleanup) delete globalStore[cleanupStoreKey];
  });

  const tool: ToolDefinition<typeof SubagentParams, Details> = {
    name: "dag_subagent",
    label: "DAG Subagent",
    description: `DAG-owned subagent runner backed by pi-subagents internals. Use this for /dag run worker/reviewer execution instead of the generic subagent tool.

Supports the same execution and management parameters as pi-subagents: single agent, parallel tasks, chains, and actions list/get/status/interrupt/resume/doctor/create/update/delete. For DAG node execution, pass the subagentParams object returned by dag_start_node directly to this tool.`,
    parameters: SubagentParams,
    async execute(id, params, signal, onUpdate, ctx) {
      state.lastUiContext = ctx;
      const result = await executor.execute(id, params as SubagentParamsLike, signal, onUpdate, ctx);
      return rewriteResultToolHints(result);
    },
  };

  pi.registerTool(tool);
}
