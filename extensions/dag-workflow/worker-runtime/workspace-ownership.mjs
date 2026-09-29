import { constants } from "node:fs";
import { open, mkdir, lstat, realpath, rename, readdir } from "node:fs/promises";
import { spawnSync, execFileSync } from "node:child_process";
import { join, resolve, dirname, basename, relative, isAbsolute, sep } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { canonicalStringify } from "./core.mjs";

const check = (value, message) => { if (!value) throw Error(message); };
const key = value => createHash("sha256").update(value).digest("hex");
const exactKeys = (value, keys) => value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === keys.length && keys.every(k => Object.hasOwn(value, k));
const bindingValid = value => exactKeys(value, ["workerStorageId", "launchOwnerSessionId", "workerId", "attemptNumber", "attemptNonce", "configHash"])
  && ["workerStorageId", "launchOwnerSessionId", "workerId", "attemptNonce"].every(k => typeof value[k] === "string" && value[k].length > 0)
  && Number.isInteger(value.attemptNumber) && value.attemptNumber > 0 && /^sha256:[0-9a-f]{64}$/.test(value.configHash);
function auditOwnership(state, repository, cwd) {
  check(exactKeys(state, ["version", "repository", "cwd", "root", "nodeId", "epoch", "binding", "launchKey", "execution", "handoffs", ...(state.workspace ? ["workspace"] : []), ...(state.borrowedRequestHash ? ["borrowedRequestHash"] : [])])
    && state.version === 1 && state.cwd === cwd && state.repository === repository && typeof state.nodeId === "string" && state.nodeId.length > 0
    && Number.isSafeInteger(state.epoch) && state.epoch > 0 && state.epoch <= 1000000
    && exactKeys(state.root, ["path", "dev", "ino"]) && state.root.path === cwd && /^\d+$/.test(state.root.dev) && /^\d+$/.test(state.root.ino)
    && (state.binding === null || bindingValid(state.binding))
    && (state.launchKey === null || typeof state.launchKey === "string" && state.launchKey.startsWith(`${state.nodeId}/`))
    && (state.execution === null || typeof state.execution === "string" && state.execution.length > 0 && state.launchKey === null)
    && Array.isArray(state.handoffs) && state.handoffs.every(h => exactKeys(h, ["binding", "completion", "at"]) && bindingValid(h.binding)
      && exactKeys(h.completion, ["completionId", "terminalStatus"]) && typeof h.completion.completionId === "string"
      && ["succeeded", "needs_attention", "failed", "cancelled", "lost"].includes(h.completion.terminalStatus) && typeof h.at === "string" && Number.isFinite(Date.parse(h.at)))
    && (state.binding === null ? state.launchKey !== null && state.handoffs.length === 0
      : state.handoffs.length > 0 && Object.keys(state.binding).every(k => state.binding[k] === state.handoffs.at(-1).binding[k])), "INVALID_WORKSPACE_OWNERSHIP");
  check(!state.borrowedRequestHash || state.workspace && state.launchKey && /^sha256:[0-9a-f]{64}$/.test(state.borrowedRequestHash), "INVALID_NODE_WORKSPACE_CLAIM");
  if (state.workspace) {
    const w = state.workspace;
    check(exactKeys(w, ["nodeId", "cwd", "identity"]) && w.nodeId === state.nodeId && w.cwd === cwd
      && exactKeys(w.identity, ["root", "common", "admin"]) && [w.identity.root, w.identity.common, w.identity.admin].every(d =>
        exactKeys(d, ["path", "dev", "ino"]) && typeof d.path === "string" && isAbsolute(d.path) && /^\d+$/.test(d.dev) && /^\d+$/.test(d.ino))
      && canonicalStringify(w.identity.root) === canonicalStringify(state.root), "INVALID_NODE_WORKSPACE_BINDING");
  }
}

export async function inspectNodeWorkspaceBinding(cwd, nodeId) {
  const env = { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_"))), GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null" };
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd, env, encoding: "utf8" }).trim();
  return { nodeId, cwd, identity: { root: await inspectWorkspaceRoot(cwd),
    common: await inspectWorkspaceRoot(git("rev-parse", "--path-format=absolute", "--git-common-dir")),
    admin: await inspectWorkspaceRoot(git("rev-parse", "--absolute-git-dir")) } };
}

async function canonicalPath(path) {
  try { return await realpath(path); }
  catch (error) {
    if (error.code !== "ENOENT" || dirname(path) === path) throw error;
    // Cleanup/recovery can name a removed root through an existing parent alias.
    return join(await canonicalPath(dirname(path)), basename(path));
  }
}

export function workspacePathsOverlap(left, right) {
  const contains = (parent, child) => {
    const path = relative(parent, child);
    return path === "" || !isAbsolute(path) && path !== ".." && !path.startsWith(`..${sep}`);
  };
  return contains(left, right) || contains(right, left);
}

export async function ownershipWorkspacePath(cwd) {
  cwd = await canonicalPath(resolve(cwd));
  try { await lstat(cwd); } catch (error) { if (error.code === "ENOENT") return cwd; throw error; }
  // Git roots describe mutation scope, not independent authority: a nested Git
  // dependency still overlaps its enclosing node. Callers compare both scopes.
  const env = { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_"))), GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null" };
  try { return await realpath(execFileSync("git", ["rev-parse", "--show-toplevel"], { cwd, env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim()); }
  catch (error) { if (error.status === 128 && String(error.stderr).includes("not a git repository")) return cwd; throw error; }
}

/** One repository admission lock covers overlap discovery, worker reservation,
 * dispatch/cleanup claims and ownership publication across manager stores. Per-
 * root locks cannot make an absent-ancestor check atomic with ancestor adoption.
 * Commands run outside this lock; disjoint nodes retain concurrent execution.
 * The lock is NOT a lease: owner death never releases persisted claims. */
export async function withWorkspaceOwnership(repository, cwd, operation) {
  repository = resolve(repository);
  check(await realpath(repository) === repository, "WORKSPACE_REPOSITORY_ALIAS");
  const directory = join(repository, ".ai", "node-workspace-ownership");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  check(await realpath(directory) === directory, "WORKSPACE_OWNERSHIP_ALIAS");
  const lockPath = join(directory, "registry.lock");
  const lock = await open(lockPath, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
  try {
    const identity = await lock.stat();
    check(identity.isFile() && identity.nlink === 1, "UNSAFE_WORKSPACE_LOCK");
    for (;;) {
      const acquired = spawnSync("flock", ["--exclusive", "--nonblock", "--conflict-exit-code", "75", "3"], { stdio: ["ignore", "ignore", "pipe", lock.fd] });
      if (acquired.status === 0) break;
      check(acquired.status === 75, "WORKSPACE_LOCK_UNAVAILABLE");
      // Yield rather than blocking Node: another callback in this process may
      // own the lock. This waits for the writer, never expires workspace rights.
      await delay(20);
    }
    const verify = async () => {
      const current = await lstat(lockPath);
      check(current.dev === identity.dev && current.ino === identity.ino && await realpath(directory) === directory, "WORKSPACE_LOCK_REPLACED");
    };
    await verify();
    cwd = await ownershipWorkspacePath(cwd);
    const path = join(directory, `${key(cwd)}.json`);
    let state = null;
    for (const name of await readdir(directory)) {
      if (!name.endsWith(".json")) continue;
      check(/^[0-9a-f]{64}\.json$/.test(name), "INVALID_WORKSPACE_OWNERSHIP");
      const file = await open(join(directory, name), constants.O_RDONLY | constants.O_NOFOLLOW);
      let owned;
      try {
        const info = await file.stat();
        check(info.isFile() && info.nlink === 1, "UNSAFE_WORKSPACE_OWNERSHIP");
        owned = JSON.parse(await file.readFile("utf8"));
      } finally { await file.close(); }
      check(typeof owned?.cwd === "string" && resolve(owned.cwd) === owned.cwd && name === `${key(owned.cwd)}.json`, "INVALID_WORKSPACE_OWNERSHIP");
      auditOwnership(owned, repository, owned.cwd);
      if (owned.cwd === cwd) state = owned;
      else check(!workspacePathsOverlap(owned.cwd, cwd), "NODE_WORKSPACE_OVERLAP");
    }
    let immutableWorkspace = state?.workspace && canonicalStringify(state.workspace);
    const publish = async value => {
      auditOwnership(value, repository, cwd);
      check(!immutableWorkspace || canonicalStringify(value.workspace) === immutableWorkspace, "NODE_WORKSPACE_BINDING_IMMUTABLE");
      await verify();
      const temp = `${path}.${randomUUID()}.tmp`, file = await open(temp, "wx", 0o600);
      try { await file.writeFile(JSON.stringify(value)); await file.sync(); } finally { await file.close(); }
      await rename(temp, path);
      const dir = await open(directory, "r"); try { await dir.sync(); } finally { await dir.close(); }
      state = value;
      immutableWorkspace ??= value.workspace && canonicalStringify(value.workspace);
    };
    const result = await operation(state, publish);
    await verify(); return result;
  } finally { await lock.close(); }
}

export function assertWorkspaceLaunch(state, launchKey, root) {
  check(!state || state.launchKey === launchKey && state.execution === null, "NODE_WORKSPACE_LAUNCH_RELINQUISHED");
  if (root?.kind === "borrowed_node" || state?.borrowedRequestHash || state?.workspace && root?.kind !== "approved_disposable") {
    check(state?.workspace && root?.kind === "borrowed_node" && root.nodeId === state.nodeId && root.epoch === state.epoch
      && state.borrowedRequestHash === root.requestHash && root.realPath === state.cwd && root.dev === state.root.dev && root.ino === state.root.ino
      && canonicalStringify(root.workspace) === canonicalStringify(state.workspace), "NODE_WORKSPACE_CAPABILITY_MISMATCH");
  }
}

export async function inspectWorkspaceRoot(cwd) {
  const info = await lstat(cwd);
  check(info.isDirectory() && !info.isSymbolicLink() && await realpath(cwd) === cwd, "WORKSPACE_IDENTITY_DRIFT");
  return { path: cwd, dev: String(info.dev), ino: String(info.ino) };
}
