#!/usr/bin/env node
import { resolve } from "node:path";
import { WorkerManager } from "../../extensions/dag-workflow/worker-runtime/manager.mjs";

const [repositoryRoot, sessionFile, failpoint] = process.argv.slice(2);
const pi = {
  getActiveTools: () => ["read", "bash", "subagent"],
  sendMessage() {},
};
const manager = new WorkerManager(pi, {
  piCliPath: resolve("scripts/fixtures/fake-worker-rpc.mjs"),
  watchIntervalMs: 60_000,
  failpoint(name) { if (name === failpoint) process.exit(61); },
});
const context = {
  cwd: repositoryRoot,
  model: { provider: "fake-provider", id: "fake-model" },
  thinkingLevel: "off",
  sessionManager: {
    getSessionId: () => "manager-crash-session",
    getSessionFile: () => sessionFile,
    getHeader: () => ({ type: "session", id: "manager-crash-session", cwd: repositoryRoot }),
  },
};
await manager.attach(context);
await manager.launch({ task: "Recover this launch after the manager crashes.", launchKey: `crash:${failpoint}` });
throw new Error(`Failpoint was not reached: ${failpoint}`);
