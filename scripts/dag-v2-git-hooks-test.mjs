// Git 2.54 configured hooks bypass hooksPath. Markers are armed only after
// fixture preparation; assertions never use unisolated content/index commands.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { bindGitV2, assertTargetV2, observeGitV2, composeGitV2, privateRefV2, nativeGitV2, configuredGitHooksV2 } from "../extensions/dag-workflow/runtime-v2/git-native.ts";
import { gitEnvironmentV2, runArgvV2, CommandRunnerV2 } from "../extensions/dag-workflow/runtime-v2/command-runner.ts";
const tests = [], test = (name, fn) => tests.push([name, fn]);
async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), "v2-hook-regression-")), root = join(dir, "repo"), marker = join(dir, "HOOK-RAN"); await mkdir(root);
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd: root, env: gitEnvironmentV2(), encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  git("init", "-b", "main"); git("config", "user.name", "hook fixture"); git("config", "user.email", "hooks@example.invalid");
  await writeFile(join(root, "file"), "old\n"); git("add", "."); git("commit", "-m", "base");
  const third = git("rev-parse", "HEAD"); git("commit", "--allow-empty", "-m", "expected");
  const old = { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") }, binding = await bindGitV2(root);
  const op = { operationId: "hooks", binding, targetRef: "refs/heads/main", expected: old, sourceBase: old, candidate: old, profile: "ordinary-ff-v2-1" };
  op.proposal = composeGitV2(op);
  return { dir, root, marker, git, old, third, op, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
async function absent(f) { await assert.rejects(readFile(f.marker), { code: "ENOENT" }); }
function arm(f, { scope = "local", name = "Unapproved.with.dots", event = "post-index-change", enabled } = {}) {
  const file = join(f.dir, "hook-config"), args = scope === "local" ? [] : scope === "worktree" ? ["--worktree"] : ["--file", file];
  if (scope === "worktree") f.git("config", "extensions.worktreeConfig", "true");
  for (const [key, value] of [["command", `printf hook >> ${JSON.stringify(f.marker)}`], ["event", "pre-push"], ["event", event], ...(enabled === undefined ? [] : [["enabled", enabled]])]) f.git("config", ...args, "--add", `hook.${name}.${key}`, value);
  if (scope === "include") {
    const parent = join(f.dir, "parent-config"); f.git("config", "--file", parent, "include.path", "hook-config"); f.git("config", "include.path", parent);
  }
  if (scope === "conditional") f.git("config", "includeIf.onbranch:main.path", file);
  if (scope === "dormant") f.git("config", "includeIf.gitdir:**/worktrees/**.path", file);
  return name;
}
for (const scope of ["local", "include", "conditional", "worktree"]) test(`${scope} configured hook: real raw control fires, isolated native status does not`, async () => {
  const f = await fixture(); try {
    arm(f, { scope }); await absent(f);
    f.git("hook", "run", "post-index-change", "--", "0", "0"); assert.match(await readFile(f.marker, "utf8"), /hook/); await rm(f.marker);
    await writeFile(join(f.root, "file"), "dirty user bytes\n");
    assert.match(nativeGitV2(f.root, "status", "--porcelain=v1"), /file/); await absent(f);
    await assert.rejects(assertTargetV2(f.op, f.old), scope === "worktree" ? /worktreeconfig/ : /TARGET_DIRTY/); await absent(f);
  } finally { await f.cleanup(); }
});
test("non-UTF8 configured hook identities fail closed before dirty observation", async () => {
  const f = await fixture(); try {
    const config = join(f.root, ".git", "config");
    const original = await readFile(config);
    await writeFile(config, Buffer.concat([original, Buffer.from('\n[hook "raw-'), Buffer.from([0xff]),
      Buffer.from(`"]\n command = printf hook >> ${JSON.stringify(f.marker)}\n event = post-index-change\n`)]));
    f.git("hook", "run", "post-index-change", "--", "0", "0");
    assert.match(await readFile(f.marker, "utf8"), /hook/); await rm(f.marker);
    await writeFile(join(f.root, "file"), "dirty retained bytes\n");
    const index = await readFile(join(f.op.binding.admin.path, "index"));
    assert.throws(() => configuredGitHooksV2(f.root), /GIT_CONFIG_ENCODING_UNSUPPORTED/);
    await assert.rejects(assertTargetV2(f.op, f.old), /GIT_CONFIG_ENCODING_UNSUPPORTED/);
    await absent(f);
    assert.deepEqual(await readFile(join(f.op.binding.admin.path, "index")), index);
    assert.equal(await readFile(join(f.root, "file"), "utf8"), "dirty retained bytes\n");
  } finally { await f.cleanup(); }
});
for (const state of ["old", "new", "third"]) test(`${state} recovery observes without executing configured index hooks`, async () => {
  const f = await fixture(); try {
    if (state !== "old") f.git("update-ref", "refs/heads/main", state === "new" ? f.op.proposal.commit : f.third);
    arm(f); const index = await readFile(join(f.op.binding.admin.path, "index"));
    for (let i = 0; i < 2; i++) assert.equal(await observeGitV2(f.op), state === "third" ? "third" : `${state}-clean`);
    await absent(f); assert.equal(await readFile(join(f.root, "file"), "utf8"), "old\n");
    if (state === "third") assert.deepEqual(await readFile(join(f.op.binding.admin.path, "index")), index);
  } finally { await f.cleanup(); }
});
for (const event of ["reference-transaction", "post-checkout", "post-merge"]) test(`${event} configured hook disabled during private refs and materialization`, async () => {
  const f = await fixture(); try {
    arm(f, { event }); const ref = "refs/pi-dag-v2/hooks/proposal";
    privateRefV2(f.root, ref, f.op.proposal.commit); assert.equal(nativeGitV2(f.root, "rev-parse", ref), f.op.proposal.commit);
    nativeGitV2(f.root, "worktree", "add", "--detach", join(f.dir, "linked"), f.old.commit);
    nativeGitV2(f.root, "merge", "--ff-only", f.op.proposal.commit); await absent(f);
  } finally { await f.cleanup(); }
});
for (const enabled of ["false", "no", "off", "0"]) test(`explicit disabled=${enabled} config remains eligible`, async () => {
  const f = await fixture(); try {
    arm(f, { enabled }); await assertTargetV2(f.op, f.old); assert.equal(await observeGitV2(f.op), "old-clean"); await absent(f);
  } finally { await f.cleanup(); }
});
test("unused pre-push stays eligible but is disabled in validation descendants", async () => {
  const f = await fixture(); try {
    arm(f, { event: "pre-push" }); await assertTargetV2(f.op, f.old);
    const result = await runArgvV2([process.execPath, "-e", "require('node:child_process').execFileSync('git',['hook','run','pre-push'])"], f.root, undefined, { disableGitHooks: true });
    assert.equal(result.exitCode, 0, result.stderr); assert.equal(result.settled, true); await absent(f);
  } finally { await f.cleanup(); }
});
test("dormant conditional include cannot activate in worktree-add children or argv descendants", async () => {
  const f = await fixture(); try {
    arm(f, { scope: "dormant", event: "reference-transaction" }); arm(f, { scope: "dormant", name: "index", event: "post-index-change" });
    const linked = join(f.dir, "linked"); nativeGitV2(f.root, "worktree", "add", "--detach", linked, f.old.commit); await absent(f);
    const result = await runArgvV2(["git", "worktree", "add", "--detach", join(f.dir, "argv-linked"), f.old.commit], f.root, undefined, { disableGitHooks: true });
    assert.equal(result.exitCode, 0, result.stderr); await absent(f);
  } finally { await f.cleanup(); }
});
test("hook names retain case, dots, equals and spaces when disabled", async () => {
  const f = await fixture(); try {
    const name = arm(f, { name: "Case.sensitive=name with spaces" }); assert(configuredGitHooksV2(f.root).includes(name));
    await assertTargetV2(f.op, f.old); await absent(f);
  } finally { await f.cleanup(); }
});
test("a runner's cached options cannot miss configuration installed after construction", async () => {
  const f = await fixture(); try {
    const runner = new CommandRunnerV2({}, f.root, new Map(), "node-local", ["-c", "core.hooksPath=/dev/null"]);
    arm(f, { event: "reference-transaction" });
    // Exercise the actual wrapper used for materialization and cleanup.
    runner.git(f.root, "update-ref", "refs/pi-dag-v2/cached", f.old.commit); await absent(f);
  } finally { await f.cleanup(); }
});
let failed = 0;
for (const [name, fn] of tests) { const start = Date.now(); try { await fn(); console.log(`PASS ${name} (${Date.now() - start}ms)`); } catch (error) { failed++; console.error(`FAIL ${name}`, error); } }
console.log(`${tests.length - failed}/${tests.length} configured-hook regressions passed`); if (failed) process.exitCode = 1;
