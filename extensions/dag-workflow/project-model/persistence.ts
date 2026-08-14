import { randomUUID } from "node:crypto";
import { open, mkdir, readFile, rename, rm } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

interface LockOwner {
  schemaVersion: 1;
  token: string;
  pid: number;
  processStartIdentity: string | null;
  acquiredAt: string;
}

interface OwnedLock {
  lockPath: string;
  parent: string;
  owner: LockOwner;
}

const LOCK_WAIT_MS = 10_000;
const LOCK_RETRY_MS = 10;

export async function withFileLock<T>(targetPath: string, operation: () => Promise<T>): Promise<T> {
  const lock = await acquireFileLock(targetPath);
  let operationError: unknown;
  try { return await operation(); }
  catch (error) { operationError = error; throw error; }
  finally {
    try { await releaseFileLock(lock); }
    catch (releaseError) { if (operationError === undefined) throw releaseError; }
  }
}

export async function durableReplaceJson(path: string, value: unknown, options: { enableTestCrashPoints?: boolean } = {}): Promise<void> {
  await ensureDirectory(dirname(path));
  const text = `${JSON.stringify(value, null, 2)}\n`;
  const temporary = `${path}.${process.pid}.${Date.now()}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o666);
  try {
    await handle.writeFile(text, "utf8");
    await handle.sync();
  } finally { await handle.close(); }
  if (options.enableTestCrashPoints) crashAt("after-temp-fsync");
  try {
    await rename(temporary, path);
    if (options.enableTestCrashPoints) crashAt("after-rename");
    await fsyncDirectory(dirname(path));
  } catch (error) {
    await rm(temporary, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function acquireFileLock(targetPath: string): Promise<OwnedLock> {
  const parent = dirname(targetPath);
  await ensureDirectory(parent);
  const lockPath = join(parent, `.${basename(targetPath)}.lock`);
  const recoveryPath = `${lockPath}.recovery`;
  const deadline = Date.now() + LOCK_WAIT_MS;
  while (Date.now() <= deadline) {
    await recoverDeadRecoveryLock(recoveryPath, parent);
    const claim = await createLockClaim(lockPath, "writer");
    try {
      await rename(claim.path, lockPath);
      await fsyncDirectory(parent);
      return { lockPath, parent, owner: claim.owner };
    } catch (error: any) {
      await rm(claim.path, { recursive: true, force: true });
      if (!["EEXIST", "ENOTEMPTY", "EPERM"].includes(error?.code)) throw error;
    }
    const recovered = await recoverDeadWriterLock(lockPath, recoveryPath, parent);
    if (!recovered) await delay(LOCK_RETRY_MS);
  }
  throw new Error(`Timed out waiting for project-model lock: ${lockPath}`);
}

async function recoverDeadRecoveryLock(recoveryPath: string, parent: string): Promise<void> {
  let observed: LockOwner;
  try { observed = await readLockOwner(recoveryPath); }
  catch (error: any) { if (error?.code === "ENOENT") return; throw error; }
  if (!await ownerIsGone(observed)) return;
  let current: LockOwner;
  try { current = await readLockOwner(recoveryPath); }
  catch (error: any) { if (error?.code === "ENOENT") return; throw error; }
  if (!sameOwner(current, observed) || !await ownerIsGone(current)) return;
  const retired = `${recoveryPath}.retired.${current.token}`;
  try {
    await rename(recoveryPath, retired);
    await fsyncDirectory(parent);
  } catch (error: any) { if (error?.code !== "ENOENT") throw error; return; }
  await rm(retired, { recursive: true, force: true });
}

async function recoverDeadWriterLock(lockPath: string, recoveryPath: string, parent: string): Promise<boolean> {
  let observed: LockOwner;
  try { observed = await readLockOwner(lockPath); }
  catch (error: any) { if (error?.code === "ENOENT") return true; throw error; }
  if (!await ownerIsGone(observed)) return false;

  const recoveryClaim = await createLockClaim(lockPath, "recovery");
  try {
    await rename(recoveryClaim.path, recoveryPath);
    await fsyncDirectory(parent);
  } catch (error: any) {
    await rm(recoveryClaim.path, { recursive: true, force: true });
    if (["EEXIST", "ENOTEMPTY", "EPERM"].includes(error?.code)) return false;
    throw error;
  }

  const recovery: OwnedLock = { lockPath: recoveryPath, parent, owner: recoveryClaim.owner };
  try {
    const current = await readLockOwner(lockPath);
    if (!sameOwner(current, observed) || !await ownerIsGone(current)) return false;
    const retired = `${lockPath}.retired.${current.token}`;
    await rename(lockPath, retired);
    await fsyncDirectory(parent);
    await rm(retired, { recursive: true, force: true });
    return true;
  } catch (error: any) {
    if (error?.code === "ENOENT") return true;
    throw error;
  } finally { await releaseFileLock(recovery); }
}

async function createLockClaim(lockPath: string, purpose: "writer" | "recovery"): Promise<{ path: string; owner: LockOwner }> {
  const owner: LockOwner = {
    schemaVersion: 1,
    token: randomUUID(),
    pid: process.pid,
    processStartIdentity: await processStartIdentity(process.pid),
    acquiredAt: new Date().toISOString(),
  };
  const path = `${lockPath}.claim.${purpose}.${owner.pid}.${owner.token}`;
  await mkdir(path, { mode: 0o700 });
  try {
    await durableReplaceJson(join(path, "owner.json"), owner);
    await fsyncDirectory(path);
    return { path, owner };
  } catch (error) {
    await rm(path, { recursive: true, force: true });
    throw error;
  }
}

async function readLockOwner(lockPath: string): Promise<LockOwner> {
  const owner = JSON.parse(await readFile(join(lockPath, "owner.json"), "utf8")) as LockOwner;
  if (owner?.schemaVersion !== 1 || !Number.isSafeInteger(owner.pid) || owner.pid <= 0 || typeof owner.token !== "string" || (owner.processStartIdentity !== null && typeof owner.processStartIdentity !== "string")) {
    throw new Error(`Malformed project-model lock owner: ${lockPath}`);
  }
  return owner;
}

async function releaseFileLock(lock: OwnedLock): Promise<void> {
  const current = await readLockOwner(lock.lockPath);
  const ownStartIdentity = await processStartIdentity(process.pid);
  if (!sameOwner(current, lock.owner) || current.pid !== process.pid || current.processStartIdentity !== ownStartIdentity) {
    throw new Error(`Project-model lock ownership changed before release: ${lock.lockPath}`);
  }
  const retired = `${lock.lockPath}.retired.${current.token}`;
  await rename(lock.lockPath, retired);
  await fsyncDirectory(lock.parent);
  await rm(retired, { recursive: true, force: true });
}

async function ownerIsGone(owner: LockOwner): Promise<boolean> {
  const observation = await inspectProcessIdentity(owner.pid);
  if (observation.state === "dead") return true;
  return owner.processStartIdentity !== null && observation.identity !== null && observation.identity !== owner.processStartIdentity;
}

async function processStartIdentity(pid: number): Promise<string | null> {
  return (await inspectProcessIdentity(pid)).identity;
}

async function inspectProcessIdentity(pid: number): Promise<{ state: "live" | "dead"; identity: string | null }> {
  try {
    const text = await readFile(`/proc/${pid}/stat`, "utf8");
    const fields = text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { state: "live", identity: /^\d+$/.test(fields[19] ?? "") ? `linux-proc:${fields[19]}` : null };
  } catch (error: any) {
    if (error?.code !== "ENOENT" && error?.code !== "ESRCH") {
      try { process.kill(pid, 0); return { state: "live", identity: null }; }
      catch (killError: any) { return { state: killError?.code === "ESRCH" ? "dead" : "live", identity: null }; }
    }
    try { process.kill(pid, 0); return { state: "live", identity: null }; }
    catch (killError: any) { return { state: killError?.code === "ESRCH" ? "dead" : "live", identity: null }; }
  }
}

function sameOwner(a: LockOwner, b: LockOwner): boolean {
  return a.token === b.token && a.pid === b.pid && a.processStartIdentity === b.processStartIdentity;
}

async function ensureDirectory(path: string): Promise<void> {
  try {
    await mkdir(path);
    await fsyncDirectory(dirname(path));
  } catch (error: any) {
    if (error?.code === "ENOENT") { await ensureDirectory(dirname(path)); return ensureDirectory(path); }
    if (error?.code !== "EEXIST") throw error;
  }
}

async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}

function crashAt(point: "after-temp-fsync" | "after-rename"): void {
  if (process.env.PI_PROJECT_MODEL_TEST_CRASH_POINT === point) process.kill(process.pid, "SIGKILL");
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
