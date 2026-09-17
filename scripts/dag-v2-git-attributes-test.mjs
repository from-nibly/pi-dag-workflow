// Real-Git regressions: inspecting a dirty/recovering target is not authority to
// run a configured filter. No status/diff is used by fixture assertions.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { bindGitV2, eligibleGitV2, assertTargetV2, observeGitV2, composeGitV2 } from "../extensions/dag-workflow/runtime-v2/git-native.ts";
import { gitEnvironmentV2 } from "../extensions/dag-workflow/runtime-v2/command-runner.ts";
const tests = [], test = (name, fn) => tests.push([name, fn]);
async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), "v2-attribute-regression-")), root = join(dir, "repo"), marker = join(dir, "FILTER-RAN"); await mkdir(root);
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd: root, env: gitEnvironmentV2(), encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  git("init", "-b", "main"); git("config", "user.name", "attribute fixture"); git("config", "user.email", "attributes@example.invalid");
  await mkdir(join(root, "nested")); await writeFile(join(root, "file"), "old\n"); await writeFile(join(root, "nested/file"), "old\n");
  git("add", "."); git("commit", "-m", "base"); const third = git("rev-parse", "HEAD"); git("commit", "--allow-empty", "-m", "expected");
  const old = { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") }, binding = await bindGitV2(root);
  const op = { operationId: "attributes", binding, targetRef: "refs/heads/main", expected: old, sourceBase: old, candidate: old, profile: "ordinary-ff-v2-1" };
  const proposal = composeGitV2(op); op.proposal = proposal;
  return { dir, root, marker, git, old, third, op, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
function arm(f) {
  f.git("config", "filter.unsafe.clean", `printf clean >> ${JSON.stringify(f.marker)}; cat`);
  f.git("config", "filter.unsafe.smudge", `printf smudge >> ${JSON.stringify(f.marker)}; cat`);
}
async function absent(f) { await assert.rejects(readFile(f.marker), { code: "ENOENT" }); }
async function dirty(f) { await writeFile(join(f.root, "file"), "user bytes\n"); await writeFile(join(f.root, "nested/file"), "user bytes\n"); }
async function rejected(f) {
  arm(f); await absent(f);
  const index = await readFile(join(f.op.binding.admin.path, "index"));
  await assert.rejects(eligibleGitV2(f.op.binding, [f.old]), /UNSUPPORTED_GIT_CAPABILITY:.*attributes/); await absent(f);
  await assert.rejects(assertTargetV2(f.op, f.old), /UNSUPPORTED_GIT_CAPABILITY:.*attributes/); await absent(f);
  assert.throws(() => composeGitV2(f.op), /UNSUPPORTED_GIT_CAPABILITY:.*attributes/); await absent(f);
  for (let i = 0; i < 2; i++) { assert.equal(await observeGitV2(f.op), "old-dirty"); await absent(f); }
  f.git("update-ref", f.op.targetRef, f.op.proposal.commit, f.old.commit);
  assert.equal(await observeGitV2(f.op), "new-dirty"); await absent(f);
  f.git("update-ref", f.op.targetRef, f.third, f.op.proposal.commit);
  assert.equal(await observeGitV2(f.op), "third"); await absent(f);
  await assert.rejects(assertTargetV2(f.op, f.old)); await absent(f);
  assert.deepEqual(await readFile(join(f.op.binding.admin.path, "index")), index);
  assert.equal(await readFile(join(f.root, "file"), "utf8"), "user bytes\n");
}
for (const scope of ["root", "nested", "ignored-directory"]) for (const state of ["untracked", "ignored", "staged", "index-only", "skip-worktree", "assume-unchanged"]) {
  test(`${scope} ${state} attributes: eligibility, dirty old/new recovery and third target run no filter`, async () => {
    const f = await fixture(); try {
      const directory = scope === "root" ? "" : scope === "nested" ? "nested/" : "ignored/";
      if (scope === "ignored-directory") await mkdir(join(f.root, "ignored"));
      const path = `${directory}.gitattributes`; await writeFile(join(f.root, path), "file filter=unsafe\n");
      if (["staged", "index-only", "skip-worktree", "assume-unchanged"].includes(state)) f.git("add", "-f", "--", path);
      if (state === "index-only") await rm(join(f.root, path));
      if (state === "skip-worktree" || state === "assume-unchanged") f.git("update-index", `--${state}`, "--", path);
      if (state === "ignored" || scope === "ignored-directory") await writeFile(join(f.op.binding.common.path, "info/exclude"), scope === "ignored-directory" ? "ignored/\n" : `${path}\n`);
      await dirty(f); await rejected(f);
    } finally { await f.cleanup(); }
  });
}
test("unmerged index attribute entries are rejected without refreshing the index", async () => {
  const f = await fixture(); try {
    await writeFile(join(f.root, ".gitattributes"), "file filter=unsafe\n"); const blob = f.git("hash-object", "-w", "--no-filters", ".gitattributes");
    execFileSync("git", ["update-index", "--index-info"], { cwd: f.root, env: gitEnvironmentV2(), input: `100644 ${blob} 1\t.gitattributes\n100644 ${blob} 2\t.gitattributes\n` });
    await rm(join(f.root, ".gitattributes")); await dirty(f); await rejected(f);
  } finally { await f.cleanup(); }
});
for (const linked of [false, true]) test(`${linked ? "linked" : "primary"} info attributes reject before any filter process`, async () => {
  const f = await fixture(); try {
    if (linked) {
      const root = join(f.dir, "linked"); f.git("checkout", "--detach", f.old.commit); f.git("worktree", "add", root, "main");
      f.root = root; f.op.binding = await bindGitV2(root);
    }
    await writeFile(join(f.op.binding.common.path, "info/attributes"), "file filter=unsafe\n"); await dirty(f); await rejected(f);
  } finally { await f.cleanup(); }
});
test("configured long-running filter.process is never launched by rejected observations", async () => {
  const f = await fixture(); try {
    await writeFile(join(f.root, ".gitattributes"), "file filter=unsafe\n"); f.git("add", ".gitattributes");
    f.git("config", "filter.unsafe.process", `printf process >> ${JSON.stringify(f.marker)}; exit 1`);
    await dirty(f); await rejected(f);
  } finally { await f.cleanup(); }
});
test("dirty target with unused filters and disabled fsmonitor/external diff runs no external command", async () => {
  const f = await fixture(); try {
    arm(f); const hook = join(f.dir, "monitor"); await writeFile(hook, `#!/bin/sh\nprintf monitor >> ${JSON.stringify(f.marker)}\nexit 1\n`, { mode: 0o700 });
    f.git("config", "core.fsmonitor", hook); f.git("config", "diff.external", hook); f.git("config", "color.ui", "always");
    await eligibleGitV2(f.op.binding, [f.old]); await dirty(f);
    await assert.rejects(assertTargetV2(f.op, f.old), /TARGET_DIRTY/); assert.equal(await observeGitV2(f.op), "old-dirty"); await absent(f);
  } finally { await f.cleanup(); }
});
test("global/default and configured attributes are disabled, not unrelated repository bans", async () => {
  const f = await fixture(), previous = { HOME: process.env.HOME, XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME, GIT_CONFIG_GLOBAL: process.env.GIT_CONFIG_GLOBAL, GIT_ATTR_SOURCE: process.env.GIT_ATTR_SOURCE };
  try {
    arm(f); const home = join(f.dir, "home"), xdg = join(f.dir, "xdg"), attributes = join(f.dir, "attributes"); await mkdir(home); await mkdir(join(xdg, "git"), { recursive: true });
    await writeFile(attributes, "file filter=unsafe\n"); await writeFile(join(xdg, "git/attributes"), "file filter=unsafe\n");
    await writeFile(join(home, ".gitconfig"), `[core]\nattributesFile = ${attributes}\n[filter \"unsafe\"]\nclean = false\n`);
    f.git("config", "core.attributesFile", attributes);
    Object.assign(process.env, { HOME: home, XDG_CONFIG_HOME: xdg, GIT_CONFIG_GLOBAL: join(home, ".gitconfig"), GIT_ATTR_SOURCE: f.third });
    await eligibleGitV2(f.op.binding, [f.old]); await assertTargetV2(f.op, f.old); assert.equal(await observeGitV2(f.op), "old-clean"); await absent(f);
    await dirty(f); await assert.rejects(assertTargetV2(f.op, f.old), /TARGET_DIRTY/); await absent(f);
  } finally { for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } await f.cleanup(); }
});
let failed = 0;
for (const [name, fn] of tests) { const start = Date.now(); try { await fn(); console.log(`PASS ${name} (${Date.now() - start}ms)`); } catch (error) { failed++; console.error(`FAIL ${name}`, error); } }
console.log(`${tests.length - failed}/${tests.length} attribute regressions passed`); if (failed) process.exitCode = 1;
