#!/usr/bin/env node
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { WorkerSessionStore, newNonce, processStartIdentity, workerStorageRoot, writeImmutableJson } from "../../extensions/dag-workflow/worker-runtime/core.mjs";

const [mode, repositoryRoot, storageId, key] = process.argv.slice(2);
const store = new WorkerSessionStore(repositoryRoot, storageId);

if (mode === "mutate") {
  for (let pass = 0; pass < 200; pass++) {
    try {
      await store.mutate(async (state) => {
        await new Promise((resolve) => setTimeout(resolve, Number(key) % 7));
        state.workers[`child-${key}`] = { id: `child-${key}` };
      });
      process.exit(0);
    } catch (error) {
      if (error?.code !== "WORKER_SESSION_LOCKED") throw error;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  throw new Error("Timed out acquiring worker-session lock");
}

if (mode === "crash-lock") {
  await store.mutate(() => process.exit(47));
}

if (mode === "orphan-recovery-lock") {
  const token = newNonce();
  const path = join(workerStorageRoot(repositoryRoot, storageId), ".worker-session-lock-recovery");
  await mkdir(path);
  await writeImmutableJson(join(path, "metadata.json"), { schemaVersion: 1, storageId, purpose: "recovery", token, pid: process.pid, processStartIdentity: await processStartIdentity(), acquiredAt: new Date().toISOString() });
  process.exit(48);
}

throw new Error(`Unknown worker-store child mode: ${mode}`);
