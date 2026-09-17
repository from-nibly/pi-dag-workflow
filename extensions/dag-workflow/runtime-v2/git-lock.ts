import { constants } from "node:fs";
import { open, lstat, type FileHandle } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { join } from "node:path";

/** Shared by participating V1 and V2 adapters. Never unlink this inode. The V2
 * supervisor inherits the open description, retaining exclusion after owner death. */
export async function lockGitCommonV2(common: string): Promise<{ fd: number; verify(): Promise<void>; close(): Promise<void> }> {
  if (process.platform !== "linux") throw Error("UNSUPPORTED_GIT_LOCK_PROFILE");
  const dir = await open(common, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  let file: FileHandle | undefined;
  try {
    const d = await dir.stat({ bigint: true }), path = `/proc/self/fd/${dir.fd}/pi-dag-integration.lock`;
    file = await open(path, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW | constants.O_NONBLOCK, 0o600);
    const f = await file.stat({ bigint: true });
    if (!f.isFile() || f.nlink !== 1n) throw Error("UNSAFE_GIT_LOCK");
    const result = spawnSync("flock", ["-x", "-n", "-E", "75", "3"], { stdio: ["ignore", "ignore", "pipe", file.fd] });
    if (result.status !== 0) throw Error(result.status === 75 ? "GIT_COMMON_BUSY" : "GIT_LOCK_UNAVAILABLE");
    const verify = async () => {
      const a = await lstat(common, { bigint: true }), b = await lstat(join(common, "pi-dag-integration.lock"), { bigint: true });
      if (!a.isDirectory() || a.dev !== d.dev || a.ino !== d.ino || !b.isFile() || b.dev !== f.dev || b.ino !== f.ino) throw Error("GIT_LOCK_IDENTITY_DRIFT");
    };
    await verify();
    const locked = file;
    return { fd: file.fd, verify, close: async () => { await locked.close(); await dir.close(); } };
  } catch (e) { await file?.close(); await dir.close(); throw e; }
}
