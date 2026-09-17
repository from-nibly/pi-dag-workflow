// N04 landing capability characterization, NOT V2 integration certification.
// These disposable-repository regressions deliberately demonstrate why an
// unguarded precheck-plus-merge adapter cannot enforce expected-old, and why
// even guarded ordinary merge must retain partial checkout effects on failure.
// Native V2 acceptance lives in dag-v2-git-acceptance-test.mjs; keep these unsafe
// characterizations unchanged rather than treating their PASS as certification.
import assert from "node:assert/strict";
import { execFileSync, spawnSync, spawn } from "node:child_process";
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const environment = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
Object.assign(environment, { GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_NO_REPLACE_OBJECTS: "1", GIT_NO_LAZY_FETCH: "1" });
const git = (cwd, ...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd, env: environment, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
const observation = async f => ({ target: git(f.root, "rev-parse", "refs/heads/main"), index: git(f.root, "write-tree"), file: await readFile(join(f.root, "file"), "utf8"), status: git(f.root, "status", "--porcelain=v1") });
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "dag-v2-git-capability-"));
  git(root, "init", "-b", "main");
  git(root, "config", "user.name", "N04 disposable fixture");
  git(root, "config", "user.email", "n04@example.invalid");
  await writeFile(join(root, "file"), "old\n"); git(root, "add", "file"); git(root, "commit", "-m", "base");
  const third = git(root, "rev-parse", "HEAD");
  git(root, "commit", "--allow-empty", "-m", "expected old");
  const old = git(root, "rev-parse", "HEAD"), oldTree = git(root, "rev-parse", "HEAD^{tree}");
  const worktree = join(root, "..", `${root.split("/").at(-1)}-candidate`);
  git(root, "worktree", "add", "--detach", worktree, old);
  await writeFile(join(worktree, "file"), "proposal\n"); git(worktree, "add", "file"); git(worktree, "commit", "-m", "proposal");
  const proposal = git(worktree, "rev-parse", "HEAD"), proposalTree = git(worktree, "rev-parse", "HEAD^{tree}");
  git(root, "worktree", "remove", worktree);
  return { root, old, oldTree, third, proposal, proposalTree };
}
const tests = [], test = (name, fn) => tests.push([name, fn]);
const merge = f => spawnSync("git", ["-c", "core.hooksPath=/dev/null", "merge", "--ff-only", "--no-edit", "--no-autostash", "--no-overwrite-ignore", f.proposal], { cwd: f.root, env: environment, encoding: "utf8" });

test("ref lock failure does not prevent ordinary merge from changing index/files", async f => {
  const before = await observation(f); assert.equal(before.target, f.old); assert.equal(before.index, f.oldTree); assert.equal(before.status, "");
  const lock = join(f.root, ".git", "refs", "heads", "main.lock");
  await writeFile(lock, "fixture-owned blocking ref lock\n", { flag: "wx" });
  const result = merge(f), after = await observation(f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /cannot lock ref|Unable to create/);
  assert.equal(after.target, f.old);
  assert.equal(after.index, f.proposalTree); assert.equal(after.file, "proposal\n");
  assert.notEqual(after.status, "");
  assert.equal(await readFile(lock, "utf8"), "fixture-owned blocking ref lock\n");
  console.log(JSON.stringify({ commandExit: result.status, before, after, stderr: result.stderr.trim() }));
});

test("a precheck is not expected-old CAS: backward third target is silently accepted", async f => {
  assert.equal(git(f.root, "rev-parse", "HEAD"), f.old);
  assert.equal(git(f.root, "status", "--porcelain=v1"), "");
  // Fault injection only: simulate a nonparticipating ref writer after precheck.
  // All refs and files belong to this newly-created disposable repository.
  git(f.root, "update-ref", "refs/heads/main", f.third, f.old);
  assert.equal(git(f.root, "status", "--porcelain=v1"), "");
  const result = merge(f), after = await observation(f);
  assert.equal(result.status, 0); assert.equal(after.target, f.proposal); assert.equal(after.status, "");
  console.log(JSON.stringify({ expectedOld: f.old, actualOld: f.third, commandExit: result.status, after }));
});

test("during-command ref drift is detected only after proposal files/index are installed", async f => {
  const hooks = join(f.root, ".git", "fixture-hooks"); await mkdir(hooks);
  const ready = join(f.root, ".git", "fixture-ready"), release = join(f.root, ".git", "fixture-release");
  // A fixture-only hook gives a deterministic boundary inside the real merge.
  // Production hooks must remain disabled; this is not a proposed solution.
  const hook = join(hooks, "post-index-change");
  await writeFile(hook, '#!/bin/bash\n: > "$N04_FIXTURE_READY"\ni=0\nwhile [ ! -e "$N04_FIXTURE_RELEASE" ]; do\n  i=$((i + 1))\n  [ "$i" -lt 1000 ] || exit 1\n  sleep 0.01\ndone\n'); await chmod(hook, 0o700);
  const child = spawn("git", ["-c", `core.hooksPath=${hooks}`, "merge", "--ff-only", "--no-edit", "--no-autostash", "--no-overwrite-ignore", f.proposal], { cwd: f.root, env: { ...environment, N04_FIXTURE_READY: ready, N04_FIXTURE_RELEASE: release }, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = ""; child.stdout.on("data", b => stdout += b); child.stderr.on("data", b => stderr += b);
  const watchdog = setTimeout(() => child.kill("SIGKILL"), 15000);
  const result = new Promise((resolve, reject) => { child.once("error", reject); child.once("close", (code, signal) => resolve({ code, signal })); });
  let exit;
  try {
    let seen = false;
    for (let i = 0; i < 500; i++) { try { await readFile(ready); seen = true; break; } catch { await delay(10); } }
    assert.ok(seen, "merge did not reach post-index-change boundary");
    git(f.root, "update-ref", "refs/heads/main", f.third, f.old);
  } finally {
    try { await writeFile(release, "release\n"); exit = await result; }
    finally { clearTimeout(watchdog); }
  }
  const after = await observation(f);
  assert.notEqual(exit.code, 0); assert.equal(exit.signal, null); assert.match(stderr, /is at .* but expected/);
  assert.equal(after.target, f.third); assert.equal(after.index, f.proposalTree); assert.equal(after.file, "proposal\n");
  assert.notEqual(after.status, "");
  console.log(JSON.stringify({ commandExit: exit.code, after, stdout: stdout.trim(), stderr: stderr.trim() }));
});

test("reference-transaction prepared rejection is too late to protect index/files", async f => {
  const hooks = join(f.root, ".git", "fixture-hooks"); await mkdir(hooks);
  const hook = join(hooks, "reference-transaction");
  await writeFile(hook, '#!/bin/bash\nwhile read old new ref; do\n  if [ "$1" = prepared ] && [ "$ref" = refs/heads/main ]; then exit 1; fi\ndone\nexit 0\n');
  await chmod(hook, 0o700);
  const result = spawnSync("git", ["-c", `core.hooksPath=${hooks}`, "merge", "--ff-only", "--no-edit", "--no-autostash", "--no-overwrite-ignore", f.proposal], { cwd: f.root, env: environment, encoding: "utf8", timeout: 15000 });
  const after = await observation(f);
  assert.notEqual(result.status, 0); assert.equal(result.signal, null); assert.match(result.stderr, /aborted by .*hook/);
  assert.equal(after.target, f.old); assert.equal(after.index, f.proposalTree); assert.equal(after.file, "proposal\n");
  assert.notEqual(after.status, "");
  console.log(JSON.stringify({ commandExit: result.status, after, stderr: result.stderr.trim() }));
});

console.log(`N04 capability characterization: ${git(process.cwd(), "--version")}`);
for (const [name, fn] of tests) {
  const f = await fixture();
  try { await fn(f); console.log(`PASS (unsafe primitive characterized): ${name}`); }
  finally { await rm(f.root, { recursive: true, force: true }); }
}
console.log(`${tests.length} characterization tests passed; N04 implementation remains BLOCKED, not certified.`);
