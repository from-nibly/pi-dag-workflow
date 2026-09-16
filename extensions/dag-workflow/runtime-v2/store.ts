import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { mkdir, open, readFile, rename, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { canonicalStringify, parseStrictJson } from "../dag-runtime/common.ts";
import { requireV2, validateShapeV2 } from "../planning/v2.ts";
import { auditSnapshotV2, emptySnapshotV2, SnapshotV2Schema, type SnapshotV2 } from "./state.ts";

export type FailpointV2 = "locked" | "temp_synced" | "renamed" | "directory_synced";
export interface StoreOptionsV2 { failpoint?: (point: FailpointV2) => void | Promise<void> }
/** Linux/local-filesystem profile. flock acts on an inherited open-file description;
 * the parent retains the descriptor, so the OS releases the lock on process death.
 * Never unlink the lock inode (including during recovery).
 */
export class StoreV2 {
  readonly directory: string;
  readonly statePath: string;
  readonly options: StoreOptionsV2;
  constructor(repositoryDirectory: string, options: StoreOptionsV2 = {}) {
    this.directory = join(resolve(repositoryDirectory), ".ai", "dag-workflow-v2");
    this.statePath = join(this.directory, "state.json"); this.options = options;
  }
  async read(): Promise<SnapshotV2> {
    let raw: string;
    try { raw = await readFile(this.statePath, "utf8"); }
    catch (e: any) { if (e.code === "ENOENT") return emptySnapshotV2(); throw e; }
    return auditSnapshotV2(parseStrictJson(raw));
  }
  async transaction<T>(update: (snapshot: SnapshotV2, publish: () => Promise<void>) => Promise<T>): Promise<T> {
    requireV2(process.platform === "linux", "UNSUPPORTED_V2_LOCK_PROFILE");
    await mkdir(this.directory, { recursive: true });
    const lock = await open(join(this.directory, "writer.lock"), constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
    try {
      const result = spawnSync("flock", ["--exclusive", "--nonblock", "--conflict-exit-code", "75", "3"], { stdio: ["ignore", "ignore", "pipe", lock.fd] });
      requireV2(!result.error && result.status === 0, result.status === 75 ? "STORE_BUSY" : `LOCK_UNAVAILABLE: ${result.error?.message ?? result.stderr?.toString()}`);
      await this.options.failpoint?.("locked");
      const state = await this.read();
      // A previous writer may have died after rename but before directory fsync.
      // Reconcile publication durability before acknowledging even an exact replay.
      for (const path of [this.directory, join(this.directory, ".."), join(this.directory, "..", "..")]) {
        const directory = await open(path, "r");
        try { await directory.sync(); } finally { await directory.close(); }
      }
      return await update(state, async () => {
        state.revision++;
        // Strict shape on output; semantic checks belong at ingress and each operation.
        await this.publish(state);
      });
    } finally { await lock.close(); }
  }
  private async publish(state: SnapshotV2): Promise<void> {
    validateShapeV2(SnapshotV2Schema, state);
    const path = join(this.directory, `.state-${randomUUID()}.tmp`);
    let renamed = false;
    try {
      const file = await open(path, "wx", 0o600);
      try { await file.writeFile(canonicalStringify(state)); await file.sync(); } finally { await file.close(); }
      await this.options.failpoint?.("temp_synced");
      await rename(path, this.statePath); renamed = true;
      await this.options.failpoint?.("renamed");
      const directory = await open(this.directory, "r");
      try { await directory.sync(); } finally { await directory.close(); }
      await this.options.failpoint?.("directory_synced");
    } finally { if (!renamed) await rm(path, { force: true }); }
  }
}
export async function processIdentityV2(pid = process.pid): Promise<string | null> {
  const boot = (await readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim();
  try {
    const stat = await readFile(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    // A zombie cannot own a live conductor; PID reuse is fenced by start ticks.
    if (fields[0] === "Z") return null;
    return `${boot}:${fields[19]}`;
  } catch (e: any) { if (e.code === "ENOENT" || e.code === "ESRCH") return null; throw e; }
}
