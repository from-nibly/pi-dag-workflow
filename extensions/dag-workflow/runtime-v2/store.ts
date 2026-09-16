import { randomUUID } from "node:crypto";
import { constants, type BigIntStats } from "node:fs";
import { lstat, mkdir, open, readFile, rename, unlink, type FileHandle } from "node:fs/promises";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { canonicalStringify, parseStrictJson } from "../dag-runtime/common.ts";
import { requireV2 } from "../planning/v2.ts";
import { auditSnapshotV2, emptySnapshotV2, type SnapshotV2 } from "./state.ts";

export type FailpointV2 = "locked" | "temp_synced" | "renamed" | "directory_synced";
export interface StoreOptionsV2 { failpoint?: (point: FailpointV2) => void | Promise<void> }
const directoryFlags = constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW;
const fileFlags = constants.O_NOFOLLOW | constants.O_NONBLOCK;
const fdPath = (file: FileHandle, name: string) => `/proc/self/fd/${file.fd}/${name}`;
const sameIdentity = (a: BigIntStats | null, b: BigIntStats | null) => a === null || b === null ? a === b : a.dev === b.dev && a.ino === b.ino;
function regularFile(stat: BigIntStats): void {
  requireV2(stat.isFile() && stat.nlink === 1n, "UNSAFE_STORE_FILE");
}
async function fileIdentity(path: string): Promise<BigIntStats | null> {
  try { const stat = await lstat(path, { bigint: true }); regularFile(stat); return stat; }
  catch (e: any) { if (e.code === "ENOENT") return null; throw e; }
}

/** Keep every ancestor open. All subsequent operations resolve through these
 * descriptors, never through a re-resolved repository pathname. Identity checks
 * detect namespace drift; descriptor anchoring prevents a swap redirecting I/O.
 */
class StoreDirectory {
  readonly chain: { file: FileHandle; stat: BigIntStats; name: string }[] = [];
  get file(): FileHandle { return this.chain.at(-1)!.file; }
  path(name: string): string { return fdPath(this.file, name); }
  async close(): Promise<void> { for (const entry of this.chain.toReversed()) await entry.file.close(); }
  async verify(): Promise<void> {
    for (let i = 1; i < this.chain.length; i++) {
      const entry = this.chain[i];
      const current = await lstat(fdPath(this.chain[i - 1].file, entry.name), { bigint: true });
      requireV2(current.isDirectory() && sameIdentity(current, entry.stat), "STORE_DIRECTORY_REPLACED");
    }
  }
  async sync(): Promise<void> {
    // Store, .ai, repository: also persist creation of the store hierarchy.
    for (const entry of this.chain.slice(-3).toReversed()) await entry.file.sync();
  }
  static async open(directory: string, create: boolean): Promise<StoreDirectory | null> {
    requireV2(process.platform === "linux", "UNSUPPORTED_V2_LOCK_PROFILE");
    const anchor = new StoreDirectory();
    try {
      const root = await open("/", directoryFlags);
      anchor.chain.push({ file: root, stat: await root.stat({ bigint: true }), name: "" });
      const components = directory.split("/").filter(Boolean);
      for (let i = 0; i < components.length; i++) {
        await anchor.verify();
        const name = components[i], path = anchor.path(name);
        let file: FileHandle;
        try { file = await open(path, directoryFlags); }
        catch (e: any) {
          if (e.code !== "ENOENT") throw e;
          if (!create) { await anchor.verify(); await anchor.close(); return null; }
          // Never create a missing repository or its ancestors.
          requireV2(i >= components.length - 2, "REPOSITORY_DIRECTORY_MISSING");
          await anchor.verify();
          try { await mkdir(path, { mode: 0o700 }); } catch (e: any) { if (e.code !== "EEXIST") throw e; }
          file = await open(path, directoryFlags);
        }
        anchor.chain.push({ file, stat: await file.stat({ bigint: true }), name });
      }
      await anchor.verify(); return anchor;
    } catch (e) { await anchor.close(); throw e; }
  }
}

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
  private async readAnchored(anchor: StoreDirectory): Promise<{ state: SnapshotV2; identity: BigIntStats | null }> {
    const path = anchor.path("state.json");
    let file: FileHandle;
    try { file = await open(path, constants.O_RDONLY | fileFlags); }
    catch (e: any) { if (e.code === "ENOENT") return { state: emptySnapshotV2(), identity: null }; throw e; }
    try {
      const identity = await file.stat({ bigint: true }); regularFile(identity);
      const state = auditSnapshotV2(parseStrictJson(await file.readFile("utf8")));
      requireV2(sameIdentity(identity, await fileIdentity(path)), "STORE_STATE_REPLACED");
      return { state, identity };
    } finally { await file.close(); }
  }
  async read(): Promise<SnapshotV2> {
    const anchor = await StoreDirectory.open(this.directory, false);
    if (!anchor) return emptySnapshotV2();
    try {
      await fileIdentity(anchor.path("writer.lock"));
      const { state } = await this.readAnchored(anchor);
      await anchor.verify(); return state;
    } finally { await anchor.close(); }
  }
  async transaction<T>(update: (snapshot: SnapshotV2, publish: () => Promise<void>) => Promise<T>): Promise<T> {
    const anchor = (await StoreDirectory.open(this.directory, true))!;
    let lock: FileHandle | undefined;
    try {
      await anchor.verify();
      lock = await open(anchor.path("writer.lock"), constants.O_CREAT | constants.O_RDWR | fileFlags, 0o600);
      const lockIdentity = await lock.stat({ bigint: true }); regularFile(lockIdentity);
      const result = spawnSync("flock", ["--exclusive", "--nonblock", "--conflict-exit-code", "75", "3"], { stdio: ["ignore", "ignore", "pipe", lock.fd] });
      requireV2(!result.error && result.status === 0, result.status === 75 ? "STORE_BUSY" : `LOCK_UNAVAILABLE: ${result.error?.message ?? result.stderr?.toString()}`);
      const verifyLock = async () => {
        await anchor.verify();
        requireV2(sameIdentity(lockIdentity, await fileIdentity(anchor.path("writer.lock"))), "STORE_LOCK_REPLACED");
      };
      await this.options.failpoint?.("locked");
      await verifyLock();
      const loaded = await this.readAnchored(anchor), state = loaded.state;
      let identity = loaded.identity;
      const verify = async () => {
        await verifyLock();
        requireV2(sameIdentity(identity, await fileIdentity(anchor.path("state.json"))), "STORE_STATE_REPLACED");
      };
      await verify();
      // Reconcile durability after a previous writer died between rename/fsync,
      // including exact replays that do not publish a new revision.
      await anchor.sync();
      const output = await update(state, async () => {
        state.revision++;
        auditSnapshotV2(state);
        await verify();
        identity = await this.publish(anchor, state, verify);
      });
      await verify(); return output;
    } finally { try { await lock?.close(); } finally { await anchor.close(); } }
  }
  private async publish(anchor: StoreDirectory, state: SnapshotV2, verify: () => Promise<void>): Promise<BigIntStats> {
    const path = anchor.path(`.state-${randomUUID()}.tmp`);
    const file = await open(path, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | fileFlags, 0o600);
    const identity = await file.stat({ bigint: true });
    let renamed = false;
    try {
      await file.writeFile(canonicalStringify(state)); await file.sync();
      await this.options.failpoint?.("temp_synced");
      await verify();
      requireV2(sameIdentity(identity, await fileIdentity(path)), "STORE_TEMP_REPLACED");
      await rename(path, anchor.path("state.json")); renamed = true;
      await this.options.failpoint?.("renamed");
      await anchor.verify();
      requireV2(sameIdentity(identity, await fileIdentity(anchor.path("state.json"))), "STORE_STATE_REPLACED");
      await anchor.sync();
      await this.options.failpoint?.("directory_synced");
      return identity;
    } finally {
      await file.close();
      if (!renamed && sameIdentity(identity, await fileIdentity(path))) await unlink(path);
    }
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
