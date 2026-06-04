import { existsSync } from "node:fs";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { ensureDir, runDir } from "./dag.ts";

export async function listWorkerRecords(cwd: string, runId: string): Promise<unknown[]> {
  const dir = join(runDir(cwd, runId), "workers");
  try {
    const files = await readdir(dir);
    return await Promise.all(files.filter((file) => file.endsWith(".json")).map(async (file) => JSON.parse(await readFile(join(dir, file), "utf8"))));
  } catch {
    return [];
  }
}

export async function writeWorkerRecord(cwd: string, runId: string, workerId: string, record: unknown): Promise<string> {
  const path = join(runDir(cwd, runId), "workers", `${workerId}.json`);
  await ensureDir(dirname(path));
  await writeFile(path, `${JSON.stringify(record, null, 2)}\n`, "utf8");
  return path;
}

export async function readLogTail(path: string, lines = 80): Promise<string> {
  if (!existsSync(path)) return "";
  return (await readFile(path, "utf8")).split(/\r?\n/).slice(-lines).join("\n");
}

export async function writeMetricsArtifact(cwd: string, runId: string, content: string): Promise<string> {
  const path = join(runDir(cwd, runId), "artifacts", "metrics.md");
  await ensureDir(dirname(path));
  await writeFile(path, content, "utf8");
  return path;
}
