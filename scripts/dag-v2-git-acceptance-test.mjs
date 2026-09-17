import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, rm, rename, cp, symlink, chmod, readdir } from "node:fs/promises";
import { execFileSync, spawnSync, spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { RuntimeV2, StoreV2, GitDriverV2, selectorV2, gitEnvironmentV2, runArgvV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";
import { bindGitV2, eligibleGitV2, composeGitV2, assertTargetV2, observeGitV2, makeGuardV2, privateRefV2, nativeGitV2, gitOptionsV2 } from "../extensions/dag-workflow/runtime-v2/git-native.ts";
import { lockGitCommonV2 } from "../extensions/dag-workflow/runtime-v2/git-lock.ts";
import { fixtureLifecycleV2, fixtureSourceV2, finishLifecycleV2, mutationV2 } from "./fixtures/dag-v2-lifecycle.mjs";
const tests = [], test = (name, fn) => tests.push([name, fn]);
const env = gitEnvironmentV2();
async function fixture(format = "sha1") {
  const dir = await mkdtemp(join(tmpdir(), "v2-git-acceptance-")), root = join(dir, "repo"); await mkdir(root);
  const git = (...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd: root, env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  git("init", "-b", "main", `--object-format=${format}`); git("config", "user.name", "fixture"); git("config", "user.email", "fixture@example.invalid");
  await writeFile(join(root, "file"), "baseline\n"); git("add", "file"); git("commit", "-m", "base");
  const third = git("rev-parse", "HEAD"); git("commit", "--allow-empty", "-m", "expected old");
  const old = { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") };
  await writeFile(join(root, "file"), "integrated a\n"); await writeFile(join(root, "added"), "added\n"); git("add", ".");
  const tree = git("write-tree"), candidate = { commit: git("commit-tree", tree, "-p", old.commit, "-m", "candidate"), tree };
  git("restore", "--source=HEAD", "--staged", "--worktree", ".");
  const binding = await bindGitV2(root);
  const op = { operationId: "run/a/1/integration", itemId: "a", generation: 1, reservation: "run/a/1", lease: { sessionId: "s", pid: process.pid, processStart: "fixture", generation: 1 },
    sourceBase: old, expected: old, candidate, binding, targetRef: "refs/heads/main", profile: "ordinary-ff-v2-1", phase: "intent", checks: [], dispatches: 0 };
  op.proposal = composeGitV2(op);
  return { dir, root, git, old, third, candidate, op, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
async function guard(f) { const dir = await mkdtemp(join(f.dir, "guard-")); return makeGuardV2(f.op, dir); }
function merge(f, hooks, extraEnv = {}) { return spawnSync("git", [...gitOptionsV2, "-c", `core.hooksPath=${hooks}`, "merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore", "--no-edit", f.op.proposal.commit], { cwd: f.root, env: { ...env, ...extraEnv }, encoding: "utf8", timeout: 15000 }); }
async function observe(f) { return { ref: f.git("rev-parse", "refs/heads/main"), index: f.git("write-tree"), file: await readFile(join(f.root, "file"), "utf8") }; }
for (const format of ["sha1", "sha256"]) test(`${format}: deterministic explicit-base single parent, safe config and real guarded positive landing`, async () => {
  const f = await fixture(format); try {
    f.git("config", "i18n.commitEncoding", "ISO-8859-1");
    f.git("config", "merge.renormalize", "true"); f.git("config", "merge.renames", "true"); f.git("config", "merge.unused.driver", "exit 42"); f.git("config", "color.ui", "always");
    assert.deepEqual(composeGitV2(f.op), f.op.proposal); assert.equal(f.git("rev-list", "--parents", "-n", "1", f.op.proposal.commit), `${f.op.proposal.commit} ${f.old.commit}`);
    await eligibleGitV2(f.op.binding, [f.old, f.candidate, f.op.proposal]);
    const hooks = await guard(f), result = merge(f, hooks); assert.equal(result.status, 0, result.stderr); assert.equal(await observeGitV2(f.op), "new-clean");
    const before = f.git("reflog", "show", "--format=%H", "main"); assert.equal(await observeGitV2(f.op), "new-clean"); assert.equal(f.git("reflog", "show", "--format=%H", "main"), before);
  } finally { await f.cleanup(); }
});
test("linked bound worktree lands through its own native admin paths and guarded ancillary transactions", async () => {
  const f = await fixture(); try {
    const primary = f.root, linked = join(f.dir, "linked-target"); f.git("checkout", "--detach", f.old.commit); f.git("worktree", "add", linked, "main");
    f.root = linked; f.op.binding = await bindGitV2(linked); await eligibleGitV2(f.op.binding, [f.old, f.candidate, f.op.proposal]); await assertTargetV2(f.op, f.old);
    const result = merge(f, await guard(f)); assert.equal(result.status, 0, result.stderr); assert.equal(await observeGitV2(f.op), "new-clean");
    assert.equal(await readFile(join(primary, "file"), "utf8"), "baseline\n");
  } finally { await f.cleanup(); }
});
test("native guard rejects wrong old/new/ref, HEAD-only, symbolic/detached binding and malformed/missing context", async () => {
  const f = await fixture(); try {
    const hooks = await guard(f), hook = join(hooks, "reference-transaction");
    const invoke = text => spawnSync(hook, ["prepared"], { cwd: f.root, env, input: text, encoding: "utf8" });
    const row = (old, next, ref) => `${old} ${next} ${ref}\n`;
    assert.equal(invoke(row(f.old.commit, f.op.proposal.commit, "refs/heads/main")).status, 0);
    for (const text of [row(f.third, f.op.proposal.commit, "refs/heads/main"), row(f.old.commit, f.third, "refs/heads/main"), row(f.old.commit, f.op.proposal.commit, "refs/heads/other"), row(f.old.commit, f.op.proposal.commit, "HEAD"), row("0".repeat(40), f.op.proposal.commit, "refs/heads/main"), "malformed\n"]) assert.notEqual(invoke(text).status, 0, text);
    f.git("symbolic-ref", "refs/heads/alias", "refs/heads/main"); f.git("symbolic-ref", "HEAD", "refs/heads/alias"); assert.notEqual(invoke(row(f.old.commit, f.op.proposal.commit, "refs/heads/main")).status, 0);
    f.git("symbolic-ref", "HEAD", "refs/heads/main"); f.git("checkout", "--detach", f.old.commit); assert.notEqual(invoke(row(f.old.commit, f.op.proposal.commit, "refs/heads/main")).status, 0);
    await writeFile(join(hooks, "context.json"), "{}"); assert.notEqual(invoke(row(f.old.commit, f.op.proposal.commit, "refs/heads/main")).status, 0);
    await rm(join(hooks, "context.json")); assert.notEqual(invoke(row(f.old.commit, f.op.proposal.commit, "refs/heads/main")).status, 0);
  } finally { await f.cleanup(); }
});
test("same-tree backward drift rejected by ORIG_HEAD guard before checkout; final guard independently rejects wrong-old", async () => {
  const f = await fixture(); try {
    const hooks = await guard(f); f.git("update-ref", "refs/heads/main", f.third, f.old.commit); const before = await observe(f);
    const result = merge(f, hooks); assert.notEqual(result.status, 0); assert.match(result.stderr, /captured starting HEAD/); assert.deepEqual(await observe(f), before);
    // Fault fixture disables only the early guard. Production hook is unchanged.
    const path = join(hooks, "reference-transaction"), text = await readFile(path, "utf8");
    await writeFile(path, text.replace('if len(rows) != 1 or after != old:', 'if False:'));
    const late = merge(f, hooks); assert.notEqual(late.status, 0); assert.match(late.stderr, /expected-old\/new/);
    assert.equal(f.git("rev-parse", "main"), f.third); assert.equal(f.git("write-tree"), f.op.proposal.tree); assert.equal(await observeGitV2(f.op), "third");
  } finally { await f.cleanup(); }
});
for (const differentTree of [false, true]) test(`during-command ${differentTree ? "different-tree" : "same-tree"} ref race preserves third OID and partial OWN checkout`, async () => {
  const f = await fixture(); try {
    const hooks = await guard(f), hook = join(hooks, "reference-transaction"), text = await readFile(hook, "utf8");
    const third = differentTree ? f.git("commit-tree", f.candidate.tree, "-p", f.old.commit, "-m", "foreign") : f.third;
    // A trusted fixture races immediately after ORIG_HEAD commits, before target locking.
    await writeFile(hook, text.replace('check(context, sys.argv[1], sys.stdin.read(65537))', `payload = sys.stdin.read(65537)\n        check(context, sys.argv[1], payload)\n        if sys.argv[1] == "committed" and payload.strip().endswith(" ORIG_HEAD"):\n            subprocess.check_call(["git", "-c", "core.hooksPath=/dev/null", "update-ref", "refs/heads/main", "${third}", "${f.old.commit}"])`));
    const result = merge(f, hooks); assert.notEqual(result.status, 0); assert.equal(f.git("rev-parse", "main"), third); assert.equal(f.git("write-tree"), f.op.proposal.tree);
    await writeFile(join(f.root, "file"), "third party after partial failure\n");
    for (let i = 0; i < 3; i++) { assert.equal(await observeGitV2(f.op), "third"); assert.equal(await readFile(join(f.root, "file"), "utf8"), "third party after partial failure\n"); }
  } finally { await f.cleanup(); }
});
for (const name of ["refs/heads/main.lock", "HEAD.lock", "index.lock"]) test(`foreign ${name} retained, no speculative removal`, async () => {
  const f = await fixture(); try {
    const path = join(f.root, ".git", name); await writeFile(path, "foreign-lock\n");
    await assert.rejects(assertTargetV2(f.op, f.old), /LOCK/);
    const result = merge(f, await guard(f)); assert.notEqual(result.status, 0); assert.equal(await readFile(path, "utf8"), "foreign-lock\n"); assert.equal(f.git("rev-parse", "main"), f.old.commit);
    assert.notEqual(await observeGitV2(f.op), "new-clean");
  } finally { await f.cleanup(); }
});
for (const kind of ["tracked", "index", "untracked", "ignored", "directory", "symlink", "sequencer", "branch", "duplicate"]) test(`preexisting ${kind} state retained and not accepted`, async () => {
  const f = await fixture(); try {
    if (kind === "tracked" || kind === "index") { await writeFile(join(f.root, "file"), "USER BYTES\n"); if (kind === "index") f.git("add", "file"); }
    if (["untracked", "ignored"].includes(kind)) { await writeFile(join(f.root, "added"), "USER BYTES\n"); if (kind === "ignored") await writeFile(join(f.root, ".git/info/exclude"), "added\n"); }
    if (kind === "directory") { await mkdir(join(f.root, "added")); await writeFile(join(f.root, "added/user"), "USER BYTES\n"); }
    if (kind === "symlink") { await writeFile(join(f.dir, "outside"), "USER BYTES\n"); await symlink(join(f.dir, "outside"), join(f.root, "added")); }
    if (kind === "sequencer") await mkdir(join(f.root, ".git/sequencer"));
    if (kind === "branch") f.git("checkout", "-b", "other");
    if (kind === "duplicate") f.git("worktree", "add", "--force", join(f.dir, "duplicate"), "main");
    if (kind !== "ignored") await assert.rejects(assertTargetV2(f.op, f.old));
    if (["tracked", "index", "untracked", "ignored", "directory", "symlink"].includes(kind)) {
      assert.notEqual(merge(f, await guard(f)).status, 0); assert.equal(f.git("rev-parse", "main"), f.old.commit);
      const path = ["tracked", "index"].includes(kind) ? join(f.root, "file") : kind === "directory" ? join(f.root, "added/user") : join(f.root, "added");
      assert.equal(await readFile(path, "utf8"), "USER BYTES\n");
    }
  } finally { await f.cleanup(); }
});
for (const ignored of [false, true]) test(`${ignored ? "ignored" : "untracked"} working attributes cannot activate unapproved filters`, async () => {
  const f = await fixture(); try {
    const marker = join(f.dir, "FILTER-RAN"); f.git("config", "filter.unsafe.clean", `printf ran > ${JSON.stringify(marker)}; cat`);
    await writeFile(join(f.root, ".gitattributes"), "* filter=unsafe\n");
    if (ignored) await writeFile(join(f.root, ".git/info/exclude"), ".gitattributes\n");
    await assert.rejects(eligibleGitV2(f.op.binding, [f.old, f.candidate]), /worktree attributes/);
    await assert.rejects(readFile(marker), { code: "ENOENT" }); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
  } finally { await f.cleanup(); }
});
test("private refs use exact expected-absent CAS and reject symbolic/conflicting replay", async () => {
  const f = await fixture(); try {
    privateRefV2(f.root, "refs/pi-dag-v2/test/proposal", f.op.proposal.commit); privateRefV2(f.root, "refs/pi-dag-v2/test/proposal", f.op.proposal.commit);
    assert.throws(() => privateRefV2(f.root, "refs/pi-dag-v2/test/proposal", f.old.commit), /CONFLICT/);
    f.git("symbolic-ref", "refs/pi-dag-v2/test/alias", "refs/heads/main"); assert.throws(() => privateRefV2(f.root, "refs/pi-dag-v2/test/alias", f.old.commit), /SYMBOLIC/);
  } finally { await f.cleanup(); }
});
test("common-dir lock serializes linked worktrees and native common/admin/root replacement rejects", async () => {
  const f = await fixture(); try {
    const linked = join(f.dir, "linked"); f.git("worktree", "add", "-b", "linked", linked); const binding = await bindGitV2(linked);
    const lock = await lockGitCommonV2(binding.common.path); try { await assert.rejects(lockGitCommonV2(f.op.binding.common.path), /BUSY/); } finally { await lock.close(); }
    await rename(binding.common.path, binding.common.path + ".old"); await cp(binding.common.path + ".old", binding.common.path, { recursive: true });
    await assert.rejects(eligibleGitV2(binding, [f.old]), /IDENTITY/); assert.equal(await observeGitV2(f.op), "identity-drift");
  } finally { await f.cleanup(); }
});
test("a copied linked-worktree gitfile cannot impersonate the sole registered branch checkout", async () => {
  const f = await fixture(); try {
    const linked = join(f.dir, "linked"), copied = join(f.dir, "copied"); f.git("worktree", "add", "-b", "linked", linked); await cp(linked, copied, { recursive: true });
    const binding = await bindGitV2(copied);
    await assert.rejects(assertTargetV2({ ...f.op, binding, targetRef: "refs/heads/linked" }, f.old), /BOUND_WORKTREE_NOT_REGISTERED/);
    assert.equal(f.git("rev-parse", "refs/heads/linked"), f.old.commit);
  } finally { await f.cleanup(); }
});
test("conflict and required unsupported attributes fail closed without target movement", async () => {
  const f = await fixture(); try {
    await writeFile(join(f.root, "file"), "other\n"); f.git("add", "file"); const tree = f.git("write-tree"), commit = f.git("commit-tree", tree, "-p", f.old.commit, "-m", "prefix");
    const marker = join(f.dir, "UNSAFE-MERGE-DRIVER"); f.git("config", "merge.default", "unsafe"); f.git("config", "merge.unsafe.driver", `printf ran > ${JSON.stringify(marker)}; true`);
    assert.throws(() => composeGitV2({ ...f.op, expected: { commit, tree } }), /GIT_FAILED|CONFLICT/); assert.equal(f.git("rev-parse", "main"), f.old.commit);
    await assert.rejects(readFile(marker), { code: "ENOENT" });
    f.git("config", "merge.text.driver", `printf ran > ${JSON.stringify(marker)}; true`);
    assert.throws(() => composeGitV2({ ...f.op, expected: { commit, tree } }), /UNSUPPORTED_GIT_CAPABILITY: merge.text.driver/);
    await assert.rejects(readFile(marker), { code: "ENOENT" });
    await writeFile(join(f.root, ".gitattributes"), "* filter=custom\n"); f.git("add", ".gitattributes"); const badTree = f.git("write-tree"), bad = { tree: badTree, commit: f.git("commit-tree", badTree, "-p", f.old.commit, "-m", "attributes") };
    await assert.rejects(eligibleGitV2(f.op.binding, [bad]), /attributes/);
  } finally { await f.cleanup(); }
});
async function runtimeFixture({ nodes = 1, finalFail = false, prefixDelay = 0, prefixScript = () => "" } = {}) {
  const f = await fixture(); const storeRoot = join(f.dir, "store"); await mkdir(storeRoot);
  const store = new StoreV2(storeRoot), fresh = { current: async p => ({ repository: p.repository, source: p.source }) }, runtime = new RuntimeV2(store, fresh);
  const workItem = (id, dependsOn) => ({ id, title: id, objective: id, outcomeIds: ["result"], context: [], checks: ["actual verification"], dependsOn, risk: "low", riskNotes: [], resources: {}, gates: [], lifecycle: fixtureLifecycleV2() });
  const counter = join(f.dir, "validation-count");
  const command = (phase, fail = false) => ({ id: phase, argv: [process.execPath, "-e", `${phase === "prefix" ? prefixScript(f) : ""}require('node:fs').appendFileSync(${JSON.stringify(counter)},${JSON.stringify(phase + "\n")}); console.log(require('node:child_process').execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'})); setTimeout(()=>process.exit(${fail ? 7 : 0}),${phase === "prefix" ? prefixDelay : 0})`] });
  const plan = await runtime.save({ planId: "git", title: "native integration", repository: { repositoryId: "native", baselineCommit: f.old.commit, baselineTree: f.old.tree, targetBranch: "refs/heads/main" },
    source: { governingClosure: `sha256:${"a".repeat(64)}`, refs: [fixtureSourceV2], scopeSummary: "repository local" }, architecture: { outcomes: [{ id: "result", description: "actual integration" }], nonGoals: ["publication"], notes: [], risks: [] },
    workItems: nodes === 2 ? [workItem("a", []), workItem("b", ["a"])] : [workItem("a", [])], constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] },
    integration: { strategy: "serial", checks: ["prefix"], finalChecks: ["final"], prefixCommands: [command("prefix")], finalCommands: [command("final", finalFail)] } }, 0);
  let run = await runtime.start({ intent: "run", sessionId: "session", selection: selectorV2(plan), authority: { scope: plan.workItems.map(n => n.id), maxConcurrency: 1, effects: ["repository_local"], expiresAt: Date.now() + 600000 } }, 1);
  run = await runtime.acquireLease(run.runId, "session", run.revision);
  const prepare = async (runtime, run, id, candidate) => {
    run = await runtime.reserve(mutationV2(run), id, 1, `implement ${id}`); run = await runtime.dispatch(mutationV2(run), id, 1, { ensure: async () => ({ workerId: `worker-${id}` }) });
    return finishLifecycleV2(runtime, plan, run, id, candidate, f.root);
  };
  run = await prepare(runtime, run, "a", f.candidate);
  return { ...f, store, storeRoot, fresh, runtime, plan, run, prepare, counter, reload: () => new RuntimeV2(new StoreV2(storeRoot), fresh) };
}
function installConfiguredHooks(f, enabled = "true") {
  const marker = join(f.dir, "CONFIG-HOOK-RAN");
  for (const event of ["post-index-change", "reference-transaction", "post-checkout", "post-merge"]) {
    f.git("config", `hook.${event}.command`, `printf ${event} >> ${JSON.stringify(marker)}`);
    f.git("config", "--add", `hook.${event}.event`, "pre-push");
    f.git("config", "--add", `hook.${event}.event`, event);
    f.git("config", `hook.${event}.enabled`, enabled);
  }
  return marker;
}
for (const point of ["initial", "landing-intent"]) test(`configured hooks: complete actual driver disables hooks installed at ${point}`, async () => {
  const f = await runtimeFixture(); let marker;
  try {
    if (point === "initial") marker = installConfiguredHooks(f);
    const run = await new GitDriverV2(f.runtime, f.root, { failpoint: async current => {
      if (point === "landing-intent" && current === point) marker = installConfiguredHooks(f);
    } }).integrate(mutationV2(f.run), "a", 1, f.candidate);
    assert.equal(run.status, "complete"); assert.equal(f.git("rev-parse", "HEAD"), run.gitOperations[0].proposal.commit);
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n");
    await assert.rejects(readFile(marker), { code: "ENOENT" });
    for (const request of run.gitOperations[0].checks) assert.equal((await f.store.read()).executions[request.id].result.disposition, "PASS");
  } finally { await f.cleanup(); }
});
test("configured hooks: initial dirty rejection runs no hook or validation command", async () => {
  const f = await runtimeFixture(); try {
    const marker = installConfiguredHooks(f); await writeFile(join(f.root, "file"), "USER BYTES\n");
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate), /TARGET_DIRTY/);
    await assert.rejects(readFile(marker), { code: "ENOENT" }); await assert.rejects(readFile(f.counter), { code: "ENOENT" });
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); assert.equal(await readFile(join(f.root, "file"), "utf8"), "USER BYTES\n");
  } finally { await f.cleanup(); }
});
for (const state of ["old", "new"]) test(`configured hooks: ${state} fresh-service recovery executes no hook`, async () => {
  const f = await runtimeFixture(); try {
    const point = state === "old" ? "landing-intent" : "git-exited";
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async current => { if (current === point) throw Error("hook recovery restart"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /hook recovery restart/);
    const marker = installConfiguredHooks(f), saved = (await f.store.read()).runs[f.run.runId];
    const run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(saved), "a", 1, f.candidate);
    assert.equal(run.status, "complete"); await assert.rejects(readFile(marker), { code: "ENOENT" });
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n");
    assert.equal(f.git("reflog", "show", "--format=%H", "main").split("\n").filter(oid => oid === run.gitOperations[0].proposal.commit).length, 1);
  } finally { await f.cleanup(); }
});
test("configured hooks: fresh-service composed closure runs no hook", async () => {
  const f = await runtimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "composed") throw Error("close here"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /close here/);
    const marker = installConfiguredHooks(f), run = (await f.store.read()).runs[f.run.runId];
    await new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), run.gitOperations[0].operationId);
    assert.equal((await f.store.read()).runs[run.runId].gitOperations[0].phase, "closed");
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); await assert.rejects(readFile(marker), { code: "ENOENT" });
  } finally { await f.cleanup(); }
});
for (const scope of ["local", "include", "worktree"]) test(`configured hooks: validation-installed ${scope} config is disabled before observation, cleanup, recovery and closure`, async () => {
  const prefixScript = f => {
    const marker = join(f.dir, "CONFIG-HOOK-RAN"), file = join(f.dir, "validation-hook-config");
    const args = scope === "include" ? ["--file", file] : scope === "worktree" ? ["--worktree"] : [];
    return `const cp=require('node:child_process'), fs=require('node:fs');
      ${scope === "worktree" ? "cp.execFileSync('git',['config','extensions.worktreeConfig','true']);" : ""}
      for (const event of ['post-index-change','reference-transaction','post-checkout','post-merge']) {
        cp.execFileSync('git',['config',...${JSON.stringify(args)},'hook.installed-'+event+'.command',${JSON.stringify(`printf ran >> ${JSON.stringify(marker)}`)}]);
        cp.execFileSync('git',['config',...${JSON.stringify(args)},'hook.installed-'+event+'.event',event]);
      }
      ${scope === "include" ? `cp.execFileSync('git',['config','include.path',${JSON.stringify(file)}]);` : ""}
      require('node:assert/strict').equal(fs.existsSync(${JSON.stringify(marker)}),false);`;
  };
  const f = await runtimeFixture({ prefixScript }); let retained;
  try {
    const driver = new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "git-exited") throw Error("installed hooks restart"); } });
    await assert.rejects(driver.integrate(mutationV2(f.run), "a", 1, f.candidate), scope === "worktree" ? /GIT_CHECK_NONPASS.*worktreeconfig/s : /installed hooks restart/);
    let run = (await f.store.read()).runs[f.run.runId];
    const job = (await f.store.read()).executions[run.gitOperations[0].checks[0].id];
    assert.equal(job.result.executor.invoked, true); assert.equal(job.result.exitCode, 0);
    if (scope === "worktree") {
      retained = join(job.workspace, ".."); assert.equal(job.result.disposition, "BLOCKED"); assert.equal(job.result.workspace.cleanAfter, false);
      assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "integrated a\n");
      await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate), /worktreeconfig/);
      run = (await f.store.read()).runs[f.run.runId];
      await assert.rejects(new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), run.gitOperations[0].operationId), /GIT_CLOSURE_UNRESOLVED/);
    } else {
      assert.equal(job.result.disposition, "PASS"); assert.equal(job.result.workspace.cleanAfter, true);
      await assert.rejects(readFile(join(job.workspace, "file")), { code: "ENOENT" });
      run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate); assert.equal(run.status, "complete");
    }
    await assert.rejects(readFile(join(f.dir, "CONFIG-HOOK-RAN")), { code: "ENOENT" });
  } finally { if (retained) await rm(retained, { recursive: true, force: true }); await f.cleanup(); }
});
test("fresh closure rejects replacement common directory before creating any lock, claim or files", async () => {
  const f = await runtimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "composed") throw Error("replacement restart"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /replacement restart/);
    const run = (await f.store.read()).runs[f.run.runId], common = run.gitBinding.common.path;
    await rename(common, common + ".original"); f.git("init", "-b", "unrelated");
    const contents = async () => (await Promise.all((await readdir(common, { recursive: true, withFileTypes: true })).map(async entry => [
      join(entry.parentPath, entry.name).slice(common.length), entry.isDirectory() ? "directory" : (await readFile(join(entry.parentPath, entry.name))).toString("base64"),
    ]))).sort((a, b) => a[0].localeCompare(b[0]));
    const before = await contents(), snapshot = await readFile(f.store.statePath);
    await assert.rejects(readFile(join(common, "pi-dag-v2-owned")), { code: "ENOENT" });
    await assert.rejects(new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), run.gitOperations[0].operationId), /GIT_NATIVE_IDENTITY_DRIFT/);
    assert.deepEqual(await contents(), before); assert.deepEqual(await readFile(f.store.statePath), snapshot);
    for (const name of ["pi-dag-v2-owned", "pi-dag-integration.lock"]) await assert.rejects(readFile(join(common, name)), { code: "ENOENT" });
    assert.equal((await f.store.read()).runs[run.runId].gitOperations[0].phase, "composed");
  } finally { await f.cleanup(); }
});
async function installFilterAttributes(f, { staged = false } = {}) {
  const marker = join(f.dir, "FILTER-RAN");
  await writeFile(join(f.root, ".gitattributes"), "file filter=unsafe\n");
  if (staged) f.git("add", ".gitattributes");
  else await writeFile(join(f.root, ".git/info/exclude"), ".gitattributes\n");
  await writeFile(join(f.root, "file"), "user bytes\n");
  f.git("config", "filter.unsafe.clean", `printf ran >> ${JSON.stringify(marker)}; cat`);
  await assert.rejects(readFile(marker), { code: "ENOENT" }); return marker;
}
test("attribute guard rejects staged attributes on initial driver dispatch without running filters", async () => {
  const f = await runtimeFixture(); try {
    const marker = await installFilterAttributes(f, { staged: true });
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate), /UNSUPPORTED_GIT_CAPABILITY:.*attributes/);
    const run = (await f.store.read()).runs[f.run.runId]; assert.equal(run.gitOperations[0].phase, "blocked");
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); await assert.rejects(readFile(marker), { code: "ENOENT" });
  } finally { await f.cleanup(); }
});
for (const state of ["old", "new", "third", "no-landing"]) test(`attribute guard blocks ${state} fresh recovery and closure before filter execution`, async () => {
  const f = await runtimeFixture(); try {
    const stop = state === "new" ? "git-exited" : state === "no-landing" ? "composed" : "landing-intent";
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === stop) throw Error("restart for attributes"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /restart for attributes/);
    const marker = await installFilterAttributes(f);
    if (state === "third") f.git("update-ref", "refs/heads/main", f.third);
    const head = f.git("rev-parse", "HEAD"), index = await readFile(join(f.root, ".git/index"));
    let run = (await f.store.read()).runs[f.run.runId];
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate), /BLOCKED|UNSUPPORTED_GIT_CAPABILITY/);
    await assert.rejects(readFile(marker), { code: "ENOENT" }); run = (await f.store.read()).runs[f.run.runId];
    await assert.rejects(new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), run.gitOperations[0].operationId), /GIT_CLOSURE_UNRESOLVED/);
    await assert.rejects(readFile(marker), { code: "ENOENT" }); assert.equal(f.git("rev-parse", "HEAD"), head);
    assert.deepEqual(await readFile(join(f.root, ".git/index")), index); assert.equal(await readFile(join(f.root, "file"), "utf8"), "user bytes\n");
  } finally { await f.cleanup(); }
});
test("attribute guard rechecks the durable landing launch boundary", async () => {
  const f = await runtimeFixture(); let marker; try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "landing-intent") marker = await installFilterAttributes(f, { staged: true }); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /GIT_NOT_LANDED|UNSUPPORTED_GIT_CAPABILITY/);
    assert(marker); await assert.rejects(readFile(marker), { code: "ENOENT" }); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
  } finally { await f.cleanup(); }
});
for (const staged of [false, true]) test(`attribute guard rejects validation-installed ${staged ? "index-only" : "ignored"} attributes before post-command observation`, async () => {
  const prefixScript = f => `const fs=require('node:fs'), cp=require('node:child_process'); fs.writeFileSync('.gitattributes','file filter=unsafe\\n'); ${staged ? "cp.execFileSync('git',['add','.gitattributes']); fs.unlinkSync('.gitattributes');" : ""} fs.writeFileSync('file','verification edit\\n'); cp.execFileSync('git',['config','filter.unsafe.clean',${JSON.stringify(`printf ran >> ${JSON.stringify(join(f.dir, "FILTER-RAN"))}; cat`)}]);`;
  const f = await runtimeFixture({ prefixScript }); try {
    const marker = join(f.dir, "FILTER-RAN");
    await writeFile(join(f.root, ".git/info/exclude"), staged ? "" : ".gitattributes\n");
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate), /GIT_CHECK_NONPASS.*attributes/);
    await assert.rejects(readFile(marker), { code: "ENOENT" }); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    const s = await f.store.read(), op = s.runs[f.run.runId].gitOperations[0], job = s.executions[op.checks[0].id];
    assert.equal(job.result.disposition, "BLOCKED"); assert.equal(job.result.executor.invoked, true); assert.equal(job.result.workspace.cleanAfter, false);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "verification edit\n");
    assert.equal(s.executions[op.checks[1].id], undefined);
    // This fixture owns the deliberately retained temporary workspace.
    await rm(join(job.workspace, ".."), { recursive: true, force: true });
  } finally { await f.cleanup(); }
});
test("real two-node integration across fresh services: isolated prefix/final, exact proposal, acceptance once and successor readiness", async () => {
  const f = await runtimeFixture({ nodes: 2 }); try {
    let run = await new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate);
    assert.equal(run.nodes.a.status, "complete"); assert.equal(run.status, "active"); assert.equal(f.git("rev-parse", "HEAD"), run.nodes.a.integration.target.commit);
    const runtime = f.reload(); assert.deepEqual(await runtime.frontier(run.runId), ["b"]);
    const before = f.git("reflog", "show", "--format=%H", "main");
    const replay = await new GitDriverV2(runtime, f.root).integrate(mutationV2(run), "a", 1, f.candidate); assert.equal(replay.revision, run.revision); assert.equal(f.git("reflog", "show", "--format=%H", "main"), before);
    const b = { ...f.old, commit: f.git("commit-tree", f.old.tree, "-p", f.old.commit, "-m", "unchanged candidate b") };
    run = await f.prepare(runtime, run, "b", b); run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "b", 1, b);
    assert.equal(run.status, "complete"); assert.equal((await f.store.read()).runs[run.runId].status, "complete");
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\nprefix\nfinal\n");
    for (const op of run.gitOperations) for (const check of op.checks) { const result = (await f.store.read()).executions[check.id].result; assert.equal(result.disposition, "PASS"); assert.match(result.stdout, new RegExp(op.proposal.commit)); assert.equal(result.workspace.isolated, true); }
  } finally { await f.cleanup(); }
});
test("actual final nonPASS blocks landing, retains exact results on replay, cannot bypass lifecycle or generation", async () => {
  const f = await runtimeFixture({ finalFail: true }); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate), /GIT_CHECK_NONPASS/);
    let run = (await f.store.read()).runs[f.run.runId]; assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); const revision = run.revision;
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate), /GIT_CHECK_NONPASS/);
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n"); run = (await f.store.read()).runs[run.runId];
    await assert.rejects(f.runtime.replace(mutationV2(run), "a", 1, async () => {}), /UNRESOLVED_GIT/);
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(run), "a", 1, f.old), /REQUEST_CONFLICT/);
    assert(run.revision >= revision);
  } finally { await f.cleanup(); }
});
for (const point of ["landing-intent", "git-exited", "landed"]) test(`durable ${point} acknowledgement loss: fresh reload reconciles once`, async () => {
  const f = await runtimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async p => { if (p === point) throw Error("lost acknowledgement"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /lost acknowledgement/);
    let run = (await f.store.read()).runs[f.run.runId]; run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate);
    assert.equal(run.status, "complete"); const proposal = run.nodes.a.integration.target.commit;
    assert.equal(f.git("reflog", "show", "--format=%H", "main").split("\n").filter(oid => oid === proposal).length, 1); assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n");
  } finally { await f.cleanup(); }
});
test("pause/cancellation during actual combined check persists immediately (no long snapshot lock) and never lands", async () => {
  const f = await runtimeFixture({ prefixDelay: 3000 }); try {
    const driver = new GitDriverV2(f.runtime, f.root);
    const running = driver.integrate(mutationV2(f.run), "a", 1, f.candidate);
    const rejected = assert.rejects(running, /RUN_NOT_ACTIVE|CURRENT_EXECUTION_INTENT_REQUIRED/);
    for (let i = 0; ; i++) { try { await readFile(f.counter); break; } catch { assert(i < 1000); await delay(10); } }
    const before = Date.now(), active = (await f.store.read()).runs[f.run.runId]; await f.runtime.cancel(mutationV2(active));
    assert(Date.now() - before < 2000, "cancellation must not wait for a long check under the store lock");
    await rejected;
    let run = (await f.store.read()).runs[f.run.runId]; assert.equal(run.status, "cancelling"); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    await assert.rejects(f.runtime.reconcileCancellation(mutationV2(run), async () => {}), /UNRESOLVED_GIT/);
    await driver.closeOperation(mutationV2(run), run.gitOperations[0].operationId); run = (await f.store.read()).runs[run.runId];
    run = await f.runtime.reconcileCancellation(mutationV2(run), async () => {}); assert.equal(run.status, "cancelled"); assert.notEqual(run.nodes.a.status, "complete");
  } finally { await f.cleanup(); }
});
test("supervisor retains common-dir exclusion after owner descriptor closure until complete subtree settlement", async () => {
  const f = await fixture(); let lock; try {
    lock = await lockGitCommonV2(f.op.binding.common.path); const marker = join(f.dir, "child-alive"), abort = new AbortController();
    const running = runArgvV2([process.execPath, "-e", `require('node:fs').writeFileSync(${JSON.stringify(marker)},'alive'); setInterval(()=>{},1000)`], f.root, abort.signal, { inheritedLockFd: lock.fd });
    for (let i = 0; ; i++) { try { await readFile(marker); break; } catch { assert(i < 1000); await delay(10); } }
    await lock.close(); lock = undefined; await assert.rejects(lockGitCommonV2(f.op.binding.common.path), /BUSY/);
    abort.abort(); const result = await running; assert.equal(result.settled, true); lock = await lockGitCommonV2(f.op.binding.common.path);
  } finally { await lock?.close(); await f.cleanup(); }
});
test("persisted native binding rejects copied common directory after a fresh service restart", async () => {
  const f = await runtimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "validated") throw Error("restart here"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /restart here/);
    const run = (await f.store.read()).runs[f.run.runId], common = run.gitBinding.common.path;
    await rename(common, common + ".original"); await cp(common + ".original", common, { recursive: true });
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate), /IDENTITY_DRIFT/);
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n");
  } finally { await f.cleanup(); }
});
test("stored combined evidence rejects stale candidate/prefix corruption; lease takeover fences dispatch without repeating checks", async () => {
  const f = await runtimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => {
      if (point !== "validated") return;
      const bytes = await readFile(f.store.statePath, "utf8");
      await assert.rejects(f.store.transaction(async (s, publish) => { s.runs[f.run.runId].gitOperations[0].checks[0].candidate = f.old; await publish(); }), /EXECUTOR_WITHOUT_LIFECYCLE_INTENT/);
      assert.equal(await readFile(f.store.statePath, "utf8"), bytes);
      await assert.rejects(f.store.transaction(async (s, publish) => { s.runs[f.run.runId].gitOperations[0].expected.commit = f.third; await publish(); }), /PREFIX_MISMATCH/);
      assert.equal(await readFile(f.store.statePath, "utf8"), bytes);
      const r = (await f.store.read()).runs[f.run.runId]; await f.runtime.acquireLease(r.runId, "session", r.revision);
    } }).integrate(mutationV2(f.run), "a", 1, f.candidate), /STALE_LEASE/);
    const r = (await f.store.read()).runs[f.run.runId]; assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    const run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(r), "a", 1, f.candidate); assert.equal(run.status, "complete"); assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\n");
  } finally { await f.cleanup(); }
});
test("repository user hooks never run, including configured hook directories", async () => {
  const f = await fixture(); try {
    const hooks = join(f.dir, "user-hooks"); await mkdir(hooks); const marker = join(f.dir, "USER-HOOK-RAN");
    for (const name of ["reference-transaction", "post-merge"]) await writeFile(join(hooks, name), `#!/bin/bash\nprintf ran > ${JSON.stringify(marker)}\n`, { mode: 0o700 });
    await writeFile(join(hooks, "sitecustomize.py"), `open(${JSON.stringify(marker)},'w').write('untrusted Python startup')\n`);
    f.git("config", "core.hooksPath", hooks); const result = merge(f, await guard(f), { PYTHONPATH: hooks }); assert.equal(result.status, 0, result.stderr);
    await assert.rejects(readFile(marker), { code: "ENOENT" });
  } finally { await f.cleanup(); }
});
for (const part of ["root", "admin"]) test(`native ${part} replacement retains original binding instead of adopting copied resources`, async () => {
  const f = await fixture(); try {
    const linked = join(f.dir, "linked"); f.git("worktree", "add", "-b", "linked", linked); const binding = await bindGitV2(linked);
    const path = binding[part].path; await rename(path, path + ".original"); await cp(path + ".original", path, { recursive: true });
    await assert.rejects(eligibleGitV2(binding, [f.old]), /IDENTITY/);
  } finally { await f.cleanup(); }
});
for (const killPoint of ["before-checkout", "after-checkout", "after-ref"]) test(`real Git SIGKILL ${killPoint}: retain effects and reconcile without rollback`, async () => {
  const f = await runtimeFixture(); try {
    const driver = new GitDriverV2(f.runtime, f.root, { failpoint: async (point, op) => {
      if (point !== "landing-intent") return;
      const hook = join(op.landing.directory, "hooks", "reference-transaction"), text = await readFile(hook, "utf8");
      const condition = killPoint === "before-checkout" ? 'sys.argv[1] == "prepared" and payload.strip().endswith(" ORIG_HEAD")'
        : killPoint === "after-checkout" ? 'sys.argv[1] == "prepared" and " refs/heads/main" in payload' : 'sys.argv[1] == "committed" and " refs/heads/main" in payload';
      await writeFile(hook, text.replace('check(context, sys.argv[1], sys.stdin.read(65537))', `payload = sys.stdin.read(65537)\n        check(context, sys.argv[1], payload)\n        if ${condition}:\n            os.kill(os.getppid(), 9)`));
    } });
    if (killPoint === "after-ref") {
      const run = await driver.integrate(mutationV2(f.run), "a", 1, f.candidate); assert.equal(run.status, "complete"); assert.equal(f.git("rev-parse", "HEAD"), run.nodes.a.integration.target.commit);
    } else {
      await assert.rejects(driver.integrate(mutationV2(f.run), "a", 1, f.candidate), /GIT_NOT_LANDED/);
      let run = (await f.store.read()).runs[f.run.runId]; const op = run.gitOperations[0]; assert.equal(op.phase, "blocked"); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
      assert.equal(f.git("write-tree"), killPoint === "before-checkout" ? f.old.tree : op.proposal.tree);
      await writeFile(join(f.root, "file"), "retained user edit after crash\n");
      await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate), /BLOCKED/);
      assert.equal(await readFile(join(f.root, "file"), "utf8"), "retained user edit after crash\n");
    }
  } finally { await f.cleanup(); }
});
test("successful no-op command acknowledgement is old-clean, never completion; bounded real redispatch", async () => {
  const f = await runtimeFixture(), originalPath = process.env.PATH; try {
    const bin = join(f.dir, "bin"); await mkdir(bin); const actualGit = execFileSync("which", ["git"], { encoding: "utf8" }).trim();
    await writeFile(join(bin, "git"), `#!/bin/bash\nfor arg in "$@"; do if [ "$arg" = merge ]; then exit 0; fi; done\nexec ${JSON.stringify(actualGit)} "$@"\n`, { mode: 0o700 });
    process.env.PATH = `${bin}:${originalPath}`;
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate), /GIT_NOT_LANDED: old-clean/);
    let run = (await f.store.read()).runs[f.run.runId]; assert.equal(run.nodes.a.status, "active"); assert.equal(run.gitOperations[0].dispatches, 1);
    process.env.PATH = originalPath; run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate);
    assert.equal(run.status, "complete"); assert.equal(run.gitOperations[0].dispatches, 2);
  } finally { process.env.PATH = originalPath; await f.cleanup(); }
});
for (const point of ["landing-intent", "git-exited"]) test(`real owner SIGKILL at ${point}, fresh lease and service recover one landing`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "v2-owner-crash-")), info = join(directory, "info.json"); let data;
  try {
    const child = spawn(process.execPath, [resolve(import.meta.filename), "--crash-owner", point, info], { stdio: ["ignore", "pipe", "pipe"] });
    let output = ""; child.stdout.on("data", b => output += b); child.stderr.on("data", b => output += b);
    const result = await new Promise((done, reject) => { child.on("error", reject); child.on("exit", (code, signal) => done({ code, signal })); });
    assert.equal(result.signal, "SIGKILL", output); data = JSON.parse(await readFile(info, "utf8"));
    const store = new StoreV2(data.storeRoot), runtime = new RuntimeV2(store, { current: async p => ({ repository: p.repository, source: p.source }) });
    let run = (await store.read()).runs[data.runId]; run = await runtime.acquireLease(run.runId, "session", run.revision);
    run = await new GitDriverV2(runtime, data.root).integrate(mutationV2(run), "a", 1, data.candidate); assert.equal(run.status, "complete");
    const refs = nativeGitV2(data.root, "reflog", "show", "--format=%H", "main").split("\n"); assert.equal(refs.filter(oid => oid === run.nodes.a.integration.target.commit).length, 1);
  } finally { if (data) await rm(data.dir, { recursive: true, force: true }); await rm(directory, { recursive: true, force: true }); }
});
if (process.argv[2] === "--crash-owner") {
  const f = await runtimeFixture(); await writeFile(process.argv[4], JSON.stringify({ dir: f.dir, root: f.root, storeRoot: f.storeRoot, candidate: f.candidate, runId: f.run.runId }));
  await new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === process.argv[3]) process.kill(process.pid, "SIGKILL"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate);
  throw Error("owner crash failpoint not reached");
}
const selected = process.argv[2] === "--test-name" ? tests.filter(([name]) => name.includes(process.argv[3])) : tests;
assert(selected.length > 0, "no matching acceptance test");
let failed = 0;
for (const [name, fn] of selected) { const started = Date.now(); try { await fn(); console.log(`PASS ${name} (${Date.now() - started}ms)`); } catch (e) { failed++; console.error(`FAIL ${name}`, e); } }
console.log(`${selected.length - failed}/${selected.length} native acceptance tests passed`); if (failed) process.exitCode = 1;
