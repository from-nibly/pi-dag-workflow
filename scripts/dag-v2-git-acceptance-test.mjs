import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, rm, rename, cp, symlink, chmod, readdir, lstat } from "node:fs/promises";
import childProcess, { execFileSync, spawnSync, spawn } from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { RuntimeV2, StoreV2, GitDriverV2, selectorV2, gitEnvironmentV2, runArgvV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";
import { bindGitV2, eligibleGitV2, composeGitV2, acceptedPrefixBaseV2, assertTargetV2, observeGitV2, makeGuardV2, privateRefV2, nativeGitV2, gitOptionsV2, configuredGitHooksV2 } from "../extensions/dag-workflow/runtime-v2/git-native.ts";
import { lockGitCommonV2 } from "../extensions/dag-workflow/runtime-v2/git-lock.ts";
import { fixtureLifecycleV2, fixtureSourceV2, finishLifecycleV2, mutationV2 } from "./fixtures/dag-v2-lifecycle.mjs";
import { withWorkspaceOwnership, inspectWorkspaceRoot } from "../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs";
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
async function runtimeFixture({ nodes = 1, finalFail = false, prefixDelay = 0, finalDelay = 0, prefixScript = () => "" } = {}) {
  const f = await fixture(); const storeRoot = join(f.dir, "store"); await mkdir(storeRoot);
  const store = new StoreV2(storeRoot), fresh = { current: async p => ({ repository: p.repository, source: p.source }) }, runtime = new RuntimeV2(store, fresh);
  const workItem = (id, dependsOn) => ({ id, title: id, objective: id, outcomeIds: ["result"], context: [], checks: ["actual verification"], dependsOn, risk: "low", riskNotes: [], resources: {}, gates: [], lifecycle: fixtureLifecycleV2() });
  const counter = join(f.dir, "validation-count");
  const command = (phase, fail = false) => ({ id: phase, argv: [process.execPath, "-e", `${phase === "prefix" ? prefixScript(f) : ""}require('node:fs').appendFileSync(${JSON.stringify(counter)},${JSON.stringify(phase + "\n")}); console.log(require('node:child_process').execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'})); setTimeout(()=>process.exit(${fail ? 7 : 0}),${phase === "prefix" ? prefixDelay : finalDelay})`] });
  const plan = await runtime.save({ planId: "git", title: "native integration", repository: { repositoryId: "native", baselineCommit: f.old.commit, baselineTree: f.old.tree, targetBranch: "refs/heads/main" },
    source: { governingClosure: `sha256:${"a".repeat(64)}`, refs: [fixtureSourceV2], scopeSummary: "repository local" }, architecture: { outcomes: [{ id: "result", description: "actual integration" }], nonGoals: ["publication"], notes: [], risks: [] },
    workItems: nodes === 2 ? [workItem("a", []), workItem("b", ["a"])] : [workItem("a", [])], constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] },
    integration: { strategy: "serial", checks: ["prefix"], finalChecks: ["final"], prefixCommands: [command("prefix")], finalCommands: [command("final", finalFail)] } }, 0);
  let run = await runtime.start({ intent: "run", sessionId: "session", selection: selectorV2(plan), authority: { scope: plan.workItems.map(n => n.id), maxConcurrency: 1, effects: ["repository_local"] } }, 1);
  run = await runtime.acquireLease(run.runId, "session", run.revision);
  const prepare = async (runtime, run, id, candidate) => {
    run = await runtime.reserve(mutationV2(run), id, 1, `implement ${id}`); run = await runtime.dispatch(mutationV2(run), id, 1, { ensure: async () => ({ workerId: `worker-${id}` }) });
    return finishLifecycleV2(runtime, plan, run, id, candidate, f.root);
  };
  run = await prepare(runtime, run, "a", f.candidate);
  return { ...f, store, storeRoot, fresh, runtime, plan, run, prepare, counter, reload: () => new RuntimeV2(new StoreV2(storeRoot), fresh) };
}
async function nodeRuntimeFixture(options) {
  const f = await runtimeFixture(options), cwd = join(f.dir, "node");
  f.git("worktree", "add", "--detach", cwd, f.candidate.commit);
  await writeFile(join(f.root, ".git/info/exclude"), ".ai/\ncache/\n");
  await mkdir(join(cwd, "cache")); await writeFile(join(cwd, "cache/dependency"), "preserved");
  const node = { cwd, nodeId: `${f.run.runId}/a`, epoch: 1 };
  const binding = { workerStorageId: "fixture", launchOwnerSessionId: "fixture", workerId: "worker-a", attemptNumber: 1, attemptNonce: "nonce", configHash: `sha256:${"a".repeat(64)}` };
  await withWorkspaceOwnership(f.root, cwd, async (_owner, publish) => publish({ version: 1, repository: f.root, cwd, root: await inspectWorkspaceRoot(cwd), nodeId: node.nodeId, epoch: 1,
    binding, launchKey: null, execution: null, handoffs: [{ binding, completion: { completionId: "fixture", terminalStatus: "succeeded" }, at: new Date().toISOString() }] }));
  return { ...f, node };
}
for (const phase of ["prefix", "final"]) test(`verification timeout terminates native ${phase} subprocess without landing`, async () => {
  const f = await nodeRuntimeFixture({ [phase + "Delay"]: 30000 });
  const original = childProcess.spawn, observed = [];
  try {
    const before = await readFile(f.store.statePath, "utf8");
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node, 0), /INVALID_VERIFICATION_COMMAND_TIMEOUT/);
    assert.equal(await readFile(f.store.statePath, "utf8"), before);
    childProcess.spawn = (command, args, ...rest) => {
      if (command === "python3" && args.some(a => String(a).endsWith("command-supervisor.py"))) {
        const request = JSON.parse(args.at(-1)); observed.push({ argv: request.argv, timeoutMs: request.timeoutMs });
      }
      return original(command, args, ...rest);
    }; syncBuiltinESMExports();
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node, 5000), /GIT_CHECK_NONPASS/);
    const snapshot = await f.store.read(), run = snapshot.runs[f.run.runId], op = run.gitOperations[0];
    assert.equal(op.commandTimeoutMs, 5000); assert(op.checks.every(c => c.commandTimeoutMs === 5000));
    const result = snapshot.executions[op.checks[phase === "prefix" ? 0 : 1].id].result;
    assert.equal(result.disposition, "FAIL", result.diagnostic); assert(result.executor.invoked); assert(result.workspace.cleanAfter);
    assert.equal(result.request.commandTimeoutMs, 5000); assert.match(result.diagnostic, /timeoutMs=5000/);
    assert.match(result.stderr, /deadline expired/); assert(result.durationMs < 15000);
    assert(observed.filter(o => o.argv[0] === process.execPath).every(o => o.timeoutMs === 5000));
    assert(observed.some(o => o.argv[0] === process.execPath));
    assert(observed.filter(o => o.argv[0] === "git").every(o => o.timeoutMs === 3600000), "native checkout timeout is unrelated");
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    const count = await readFile(f.counter, "utf8");
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate, undefined, f.node, 14400000), /GIT_CHECK_NONPASS/);
    assert.equal(await readFile(f.counter, "utf8"), count, "new config never reruns a failed exact request");
    const current = (await f.store.read()).runs[f.run.runId];
    await new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(current), op.operationId);
  } finally { childProcess.spawn = original; syncBuiltinESMExports(); await f.cleanup(); }
});

for (const legacy of [false, true]) test(`verification timeout native intent survives actual owner death (${legacy ? "legacy absence" : "custom long budget"})`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "native-timeout-recovery-")), info = join(directory, "info.json"); let data;
  try {
    const child = spawn(process.execPath, [resolve(import.meta.filename), "--timeout-crash-owner", String(legacy), info], { stdio: ["ignore", "pipe", "pipe"] });
    let output = ""; child.stdout.on("data", b => output += b); child.stderr.on("data", b => output += b);
    const exit = await new Promise(r => child.on("exit", (code, signal) => r({ code, signal })));
    assert.equal(exit.signal, "SIGKILL", output); data = JSON.parse(await readFile(info, "utf8"));
    const store = new StoreV2(data.storeRoot), runtime = new RuntimeV2(store, { current: async p => ({ repository: p.repository, source: p.source }) });
    const bytes = await readFile(store.statePath, "utf8"); let run = (await store.read()).runs[data.runId];
    assert.equal(await readFile(store.statePath, "utf8"), bytes);
    const frozen = run.gitOperations[0].commandTimeoutMs;
    assert.equal(frozen, legacy ? undefined : 14400000); assert.equal(run.gitOperations[0].checks.length, 0);
    run = await runtime.acquireLease(run.runId, "session", run.revision);
    run = await new GitDriverV2(runtime, data.root).integrate(mutationV2(run), "a", 1, data.candidate, undefined, undefined, 1);
    assert.equal(run.status, "complete"); const snapshot = await store.read(), op = run.gitOperations[0];
    for (const check of op.checks) {
      assert.equal(check.commandTimeoutMs, frozen); const result = snapshot.executions[check.id].result;
      assert.equal(result.disposition, "PASS"); assert.deepEqual(result.request, check);
      assert.match(result.diagnostic, new RegExp(`timeoutMs=${frozen ?? 3600000}`));
    }
    const before = await readFile(store.statePath, "utf8");
    await assert.rejects(store.transaction(async (s, publish) => { s.runs[run.runId].gitOperations[0].commandTimeoutMs = 2; await publish(); }), /GIT_CHECK_REQUEST_MISMATCH/);
    assert.equal(await readFile(store.statePath, "utf8"), before);
  } finally { if (data) await rm(data.dir, { recursive: true, force: true }); await rm(directory, { recursive: true, force: true }); }
});

test("same-node configured hooks stay disabled during checkout, checks, restoration and landing", async () => {
  const f = await nodeRuntimeFixture(); try {
    const marker = installConfiguredHooks(f);
    const run = await new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node);
    assert.equal(run.status, "complete"); await assert.rejects(readFile(marker), { code: "ENOENT" });
    assert.equal(await readFile(join(f.node.cwd, "cache/dependency"), "utf8"), "preserved");
  } finally { await f.cleanup(); }
});
test("same-node tracked check tampering blocks restoration and target movement without overwriting user bytes", async () => {
  const f = await nodeRuntimeFixture({ prefixScript: () => "require('node:fs').writeFileSync('file','USER CHECK EDIT\\n');" }); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node), /GIT_CHECK_NONPASS/);
    let run = (await f.store.read()).runs[f.run.runId], op = run.gitOperations[0];
    const job = (await f.store.read()).executions[op.checks[0].id];
    assert.equal(job.workspace, f.node.cwd); assert.equal(job.result.exitCode, 0); assert.equal(job.result.workspace.cleanAfter, false);
    await assert.rejects(new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), op.operationId), /GIT_NODE_RESTORE_BLOCKED/);
    assert.equal(await readFile(join(f.node.cwd, "file"), "utf8"), "USER CHECK EDIT\n"); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    await withWorkspaceOwnership(f.root, f.node.cwd, owner => assert.equal(owner.execution, op.operationId));
    await writeFile(join(f.node.cwd, "file"), "integrated a\n");
    run = (await f.store.read()).runs[f.run.runId]; await new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), op.operationId);
    assert.equal((await f.store.read()).runs[f.run.runId].gitOperations[0].phase, "closed");
  } finally { await f.cleanup(); }
});
test("same-node closure retains a foreign lock even at the exact original endpoint", async () => {
  const f = await nodeRuntimeFixture(); try {
    await assert.rejects(new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === "node-switching-intent") throw Error("before checkout"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node), /before checkout/);
    const lock = join(nativeGitV2(f.node.cwd, "rev-parse", "--absolute-git-dir"), "index.lock"); await writeFile(lock, "foreign lock");
    let run = (await f.store.read()).runs[f.run.runId], op = run.gitOperations[0];
    await assert.rejects(new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), op.operationId), /GIT_NODE_LOCK_OR_OPERATION/);
    assert.equal(await readFile(lock, "utf8"), "foreign lock"); await withWorkspaceOwnership(f.root, f.node.cwd, owner => assert.equal(owner.execution, op.operationId));
    await rm(lock); run = (await f.store.read()).runs[f.run.runId];
    await new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), op.operationId);
    assert.equal((await f.store.read()).runs[f.run.runId].gitOperations[0].phase, "closed");
  } finally { await f.cleanup(); }
});
test("same-node ignored attributes block before checkout without invoking filters or deleting artifacts", async () => {
  const f = await nodeRuntimeFixture(); try {
    const marker = join(f.dir, "FILTER-RAN"); f.git("config", "filter.bad.clean", `touch ${marker}; cat`);
    await writeFile(join(f.root, ".git/info/exclude"), ".ai/\ncache/\n.gitattributes\n");
    await writeFile(join(f.node.cwd, ".gitattributes"), "* filter=bad\n");
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node), /attributes/);
    assert.equal(await readFile(join(f.node.cwd, ".gitattributes"), "utf8"), "* filter=bad\n"); await assert.rejects(readFile(marker), { code: "ENOENT" });
    assert.equal(nativeGitV2(f.node.cwd, "rev-parse", "HEAD"), f.candidate.commit); assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
  } finally { await f.cleanup(); }
});
for (const point of ["node-switching-intent", "node-switching-exited", "node-composed", "node-restoring-intent", "node-restoring-exited"]) test(`same-node actual owner SIGKILL at ${point} retains claim and reload restores before CAS`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "v2-node-crash-")), path = join(directory, "fixture.json"); let data;
  try {
    const child = spawnSync(process.execPath, [resolve("scripts/dag-v2-git-acceptance-test.mjs"), "--crash-node-owner", point, path], { env, encoding: "utf8", timeout: 120000 });
    assert.equal(child.signal, "SIGKILL", child.stderr); data = JSON.parse(await readFile(path, "utf8"));
    const store = new StoreV2(data.storeRoot), fresh = { current: async p => ({ repository: p.repository, source: p.source }) }, runtime = new RuntimeV2(store, fresh);
    let run = (await store.read()).runs[data.runId];
    await withWorkspaceOwnership(data.root, data.node.cwd, owner => { assert.equal(owner.execution, `${data.runId}/a/1/integration`); });
    assert.equal(nativeGitV2(data.root, "rev-parse", "HEAD"), data.old.commit);
    run = await runtime.acquireLease(run.runId, "session", run.revision);
    run = await new GitDriverV2(runtime, data.root).integrate(mutationV2(run), "a", 1, data.candidate);
    assert.equal(run.status, "complete"); assert.equal(run.gitOperations[0].workspace.phase, "restored");
    assert.equal(nativeGitV2(data.node.cwd, "rev-parse", "HEAD"), data.candidate.commit);
    assert.equal(await readFile(join(data.node.cwd, "cache/dependency"), "utf8"), "preserved");
    assert.equal(nativeGitV2(data.root, "worktree", "list", "--porcelain").split("\n").filter(l => l.startsWith("worktree ")).length, 2);
    await withWorkspaceOwnership(data.root, data.node.cwd, owner => { assert.equal(owner.execution, null); });
    for (const request of run.gitOperations[0].checks) {
      const job = (await store.read()).executions[request.id]; assert.equal(job.workspace, data.node.cwd); assert.equal(job.result.disposition, "PASS");
    }
  } finally { if (data) await rm(data.dir, { recursive: true, force: true }); await rm(directory, { recursive: true, force: true }); }
});
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
    assert.equal(run.gitOperations[1].sourceBase.commit, f.old.commit); assert.equal(run.gitOperations[1].composition.accepted.length, 1);
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\nprefix\nfinal\n");
    for (const op of run.gitOperations) for (const check of op.checks) { const result = (await f.store.read()).executions[check.id].result; assert.equal(result.disposition, "PASS"); assert.match(result.stdout, new RegExp(op.proposal.commit)); assert.equal(result.workspace.isolated, true); }
  } finally { await f.cleanup(); }
});
test("accepted-prefix composition: sequential overlapping W1/W2 uses incorporated accepted proposal", async () => {
  const f = await runtimeFixture({ nodes: 2 }); try {
    let run = await new GitDriverV2(f.runtime, f.root).integrate(mutationV2(f.run), "a", 1, f.candidate);
    const first = JSON.stringify(run.gitOperations[0]), prefix = run.nodes.a.integration.target;
    await writeFile(join(f.root, "file"), "integrated b\n"); f.git("add", "file");
    const tree = f.git("write-tree"), b = { tree, commit: f.git("commit-tree", tree, "-p", prefix.commit, "-m", "W2 overlaps W1") };
    f.git("restore", "--source=HEAD", "--staged", "--worktree", ".");
    run = await f.prepare(f.reload(), run, "b", b);
    run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "b", 1, b);
    assert.equal(run.status, "complete"); assert.deepEqual({ ...run.gitOperations[1].sourceBase }, prefix);
    assert.equal(run.gitOperations[1].proposal.tree, b.tree); assert.equal(JSON.stringify(run.gitOperations[0]), first);
    assert.equal(await readFile(f.counter, "utf8"), "prefix\nfinal\nprefix\nfinal\n");
  } finally { await f.cleanup(); }
});
test("accepted-prefix native authority: retained unlanded repair, parallel earlier prefix, genuine conflicts and tampering", async () => {
  const f = await fixture(); try {
    const p = f.op.proposal;
    const commit = async (parent, text, label) => {
      await writeFile(join(f.root, "file"), text); f.git("add", "file"); const tree = f.git("write-tree");
      const value = { tree, commit: f.git("commit-tree", tree, "-p", parent.commit, "-m", label) };
      f.git("restore", "--source=HEAD", "--staged", "--worktree", "."); return value;
    };
    const c = await commit(p, "integrated b\n", "unlanded C"), repair = await commit(c, "integrated c\n", "repair on unlanded C");
    const receipt = { version: "accepted-prefix-v1", baseline: f.old, accepted: [{ operationId: "run/a/1/integration", proposal: p }] };
    const op = { ...f.op, profile: "ordinary-ff-v2-2", composition: receipt, candidate: repair, expected: p, sourceBase: p };
    assert.equal(acceptedPrefixBaseV2(f.root, receipt, repair, p).commit, p.commit);
    assert.equal(composeGitV2(op).tree, repair.tree); assert.throws(() => composeGitV2({ ...op, sourceBase: c }), /BASE_MISMATCH/);
    assert.throws(() => composeGitV2({ ...op, sourceBase: f.old }), /BASE_MISMATCH/);
    const q = await commit(p, "integrated d\n", "accepted Q conflicts with C");
    const chain = { ...receipt, accepted: [...receipt.accepted, { operationId: "run/q/1/integration", proposal: q }] };
    assert.equal(acceptedPrefixBaseV2(f.root, chain, c, q).commit, p.commit);
    assert.throws(() => composeGitV2({ ...op, composition: chain, expected: q, candidate: c }), /GIT_COMPOSITION_CONFLICT/);
    assert.throws(() => composeGitV2({ ...op, composition: chain, expected: q, candidate: c, sourceBase: q }), /BASE_MISMATCH/);
    assert.equal(acceptedPrefixBaseV2(f.root, chain, f.candidate, q).commit, f.old.commit);
    f.git("read-tree", p.tree); await writeFile(join(f.root, "parallel"), "latest prefix only\n"); f.git("add", "parallel");
    const parallelTree = f.git("write-tree"), parallel = { tree: parallelTree, commit: f.git("commit-tree", parallelTree, "-p", p.commit, "-m", "disjoint later prefix") };
    f.git("restore", "--source=HEAD", "--staged", "--worktree", ".");
    const parallelReceipt = { ...receipt, accepted: [...receipt.accepted, { operationId: "run/parallel/1/integration", proposal: parallel }] };
    const combined = composeGitV2({ ...op, composition: parallelReceipt, candidate: c, expected: parallel });
    assert.equal(f.git("show", `${combined.commit}:file`), "integrated b"); assert.equal(f.git("show", `${combined.commit}:parallel`), "latest prefix only");
    assert.throws(() => acceptedPrefixBaseV2(f.root, { ...receipt, accepted: [{ operationId: "fake", proposal: repair }] }, c, repair), /CHAIN_MISMATCH/);
    assert.throws(() => composeGitV2({ ...op, composition: { ...receipt, baseline: { ...f.old, tree: c.tree } } }), /NATIVE_CANDIDATE_MISMATCH/);
    assert.throws(() => composeGitV2({ ...op, profile: "ordinary-ff-v2-1" }), /PROFILE/);
    assert.deepEqual(composeGitV2(f.op), f.op.proposal);
  } finally { await f.cleanup(); }
});
test("accepted-prefix state binds exact accepted chain; historical blocked operation closes then fresh generation, never reopens", async () => {
  const f = await runtimeFixture({ nodes: 2 }); try {
    // Manufacture a historical fixture at its initial intent, not a recovery action.
    const historical = id => new GitDriverV2(f.reload(), f.root, { failpoint: async point => {
      if (point !== "intent") return;
      await f.store.transaction(async (s, publish) => { const op = s.runs[f.run.runId].gitOperations.find(o => o.itemId === id); op.profile = "ordinary-ff-v2-1"; delete op.composition; op.sourceBase = f.old; await publish(); });
      throw Error("historical fixture saved");
    } });
    await assert.rejects(historical("a").integrate(mutationV2(f.run), "a", 1, f.candidate), /historical fixture/);
    let run = (await f.store.read()).runs[f.run.runId];
    run = await new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "a", 1, f.candidate);
    const first = JSON.stringify(run.gitOperations[0]), p = run.gitOperations[0].proposal;
    await writeFile(join(f.root, "file"), "integrated b\n"); f.git("add", "file"); const tree = f.git("write-tree");
    const c = { tree, commit: f.git("commit-tree", tree, "-p", p.commit, "-m", "unlanded W2") }; f.git("restore", "--source=HEAD", "--staged", "--worktree", ".");
    run = await f.prepare(f.reload(), run, "b", c);
    await assert.rejects(historical("b").integrate(mutationV2(run), "b", 1, c), /historical fixture/);
    run = (await f.store.read()).runs[f.run.runId];
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "b", 1, c), /GIT_COMPOSITION_CONFLICT/);
    run = (await f.store.read()).runs[f.run.runId]; const blocked = JSON.stringify(run.gitOperations[1]);
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "b", 1, c), /GIT_OPERATION_BLOCKED/);
    assert.equal(JSON.stringify((await f.store.read()).runs[run.runId].gitOperations[1]), blocked);
    await new GitDriverV2(f.reload(), f.root).closeOperation(mutationV2(run), run.gitOperations[1].operationId);
    run = (await f.store.read()).runs[f.run.runId]; const closed = JSON.stringify(run.gitOperations[1]);
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(mutationV2(run), "b", 1, c), /GIT_OPERATION_BLOCKED/);
    const runtime = f.reload(); run = await runtime.replace(mutationV2(run), "b", 1, async () => {}, JSON.stringify({ baseCommit: c.commit, repair: true }));
    run = await runtime.dispatch(mutationV2(run), "b", 2, { ensure: async () => ({ workerId: "repair-b" }) });
    run = await finishLifecycleV2(runtime, f.plan, run, "b", c, f.root);
    run = await new GitDriverV2(runtime, f.root, { failpoint: async (point, op) => {
      if (point !== "composed") return;
      const bytes = await readFile(f.store.statePath, "utf8");
      for (const mutate of [o => { o.sourceBase = c; }, o => { o.composition.accepted[0].operationId = "unaccepted"; }, o => { o.composition.accepted.push({ operationId: "unaccepted", proposal: c }); }, o => { o.composition.baseline = c; }]) {
        await assert.rejects(f.store.transaction(async (s, publish) => { mutate(s.runs[run.runId].gitOperations.at(-1)); await publish(); }), /COMPOSITION/);
        assert.equal(await readFile(f.store.statePath, "utf8"), bytes);
      }
      assert.equal(op.sourceBase.commit, p.commit);
    } }).integrate(mutationV2(run), "b", 2, c);
    assert.equal(run.status, "complete"); assert.equal(run.gitOperations[2].profile, "ordinary-ff-v2-2");
    assert.equal(JSON.stringify(run.gitOperations[0]), first); assert.equal(JSON.stringify(run.gitOperations[1]), closed);
    assert.equal(run.gitOperations[2].proposal.tree, c.tree);
  } finally { await f.cleanup(); }
});
for (const code of ["ENOTDIR", "ENAMETOOLONG"]) for (const api of ["configuredGitHooksV2", "nativeGitV2"]) test(`diagnostics sanitize dormant include ${code} through ${api}`, async () => {
  const f = await fixture(); try {
    const secret = "INCLUDE_DIAGNOSTIC_SYNTHETIC_SECRET", config = join(f.root, ".git", "config");
    await writeFile(join(f.root, "not-a-directory"), "ordinary file\n");
    const path = code === "ENOTDIR" ? join(f.root, "not-a-directory", `Authorization: Bearer ${secret}`)
      : join(f.root, `Authorization: Bearer ${secret}-${"x".repeat(12000)}`);
    await assert.rejects(lstat(path), { code });
    await writeFile(config, `${await readFile(config, "utf8")}\n[includeIf "gitdir:/nonexistent-include-diagnostic-condition/"]\n path = "${path}"\n`);
    // Git itself skips the dormant include; our discovery must inspect it and fail closed.
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
    const call = () => api === "configuredGitHooksV2" ? configuredGitHooksV2(f.root) : nativeGitV2(f.root, "rev-parse", "HEAD");
    assert.throws(call, error => {
      assert.equal(error.message, `GIT_FAILED config status=null signal=null code=${code} argv=omitted stdout=omitted stderr=omitted truncated=false`);
      assert(error.message.length < 256); assert(!/[\x00-\x1f]/.test(error.message));
      for (const value of [secret, "Authorization", path, f.root, "lstat"]) assert(!error.message.includes(value));
      assert.equal(error.path, undefined); assert.equal(error.cause, undefined); return true;
    });
  } finally { await f.cleanup(); }
});
test("diagnostics preserve dormant include ENOENT skip through discovery and native wrapper", async () => {
  const f = await fixture(); try {
    const config = join(f.root, ".git", "config"), path = join(f.root, "absent-include.config");
    await assert.rejects(lstat(path), { code: "ENOENT" });
    await writeFile(config, `${await readFile(config, "utf8")}\n[includeIf "gitdir:/nonexistent-include-diagnostic-condition/"]\n path = "${path}"\n`);
    assert.deepEqual(configuredGitHooksV2(f.root), []);
    assert.equal(nativeGitV2(f.root, "rev-parse", "HEAD"), f.old.commit);
  } finally { await f.cleanup(); }
});
test("diagnostics suppress synthetic Authorization on real config output overflow", async () => {
  const f = await fixture(); try {
    const secret = "DIAGNOSTIC_SYNTHETIC_SECRET", config = join(f.root, ".git", "config"), original = await readFile(config, "utf8");
    const oversized = `\n[http]\n extraHeader = Authorization: Bearer ${secret}\n[diagnostic]\n huge = ${"x".repeat(17 * 1024 * 1024)}\n`;
    const check = fn => assert.throws(fn, error => {
      assert(!error.message.includes(secret)); assert(!error.message.includes("Authorization"));
      assert.match(error.message, /^GIT_FAILED config status=null signal=SIGTERM code=ENOBUFS/);
      assert.match(error.message, /stdout=omitted/); assert.match(error.message, /truncated=true/);
      assert(error.message.length < 9000); assert(!/[\x00-\x1f]/.test(error.message)); return true;
    });
    await writeFile(config, original + oversized);
    check(() => nativeGitV2(f.root, "rev-parse", "HEAD"));
    check(() => configuredGitHooksV2(f.root));
    check(() => composeGitV2(f.op));
    await writeFile(config, original);
    const external = join(f.dir, "oversized.config"); await writeFile(external, oversized);
    check(() => nativeGitV2(f.root, "config", "--file", external, "--null", "--list"));
  } finally { await f.cleanup(); }
});
test("diagnostics omit arbitrary argv, stdout, stderr and fallback message", async () => {
  const f = await fixture(); try {
    const secret = "DIAGNOSTIC_SYNTHETIC_SECRET", header = `Authorization: Bearer ${secret}`;
    f.git("config", "diagnostic.value", header);
    for (const args of [
      ["config", "--type=bool", "--get", "diagnostic.value"],
      ["rev-parse", header], [secret], ["-c", `http.extraHeader=${header}`, secret],
      ["merge-tree", header], ["commit-tree", header],
    ]) assert.throws(() => nativeGitV2(f.root, ...args), error => {
      assert(!error.message.includes(secret)); assert(!error.message.includes("Authorization"));
      assert.match(error.message, /^GIT_FAILED .*status=\d+/); assert.match(error.message, /argv=omitted/); return true;
    });
    // Exercise empty-stderr Error.message fallback and unstructured stream payloads
    // without reading real credentials or relying on platform-specific spawn errors.
    const original = childProcess.execFileSync;
    try {
      childProcess.execFileSync = (_file, args) => {
        if (args.includes("config")) return Buffer.alloc(0);
        throw Object.assign(Error(header), { status: null, signal: "SIGTERM", code: "ETIMEDOUT", stdout: header, stderr: "" });
      }; syncBuiltinESMExports();
      for (const args of [["rev-parse", "HEAD"], ["merge-tree", "--write-tree", "--no-messages", `--merge-base=${f.old.commit}`, f.old.commit, f.candidate.commit], ["commit-tree", f.old.tree, "-p", f.old.commit, "-m", header]]) {
        assert.throws(() => nativeGitV2(f.root, ...args), error => {
          assert(!error.message.includes(secret)); assert(!error.message.includes("Authorization"));
          assert.match(error.message, /status=null signal=SIGTERM code=ETIMEDOUT/); return true;
        });
      }
    } finally { childProcess.execFileSync = original; syncBuiltinESMExports(); }
  } finally { await f.cleanup(); }
});
test("diagnostics preserve safe commit-tree object errors and bounded merge conflict records", async () => {
  const f = await fixture(); try {
    const missing = "0".repeat(40);
    assert.throws(() => nativeGitV2(f.root, "commit-tree", missing, "-p", f.old.commit, "-m", "synthetic message"), error => {
      assert.match(error.message, /^GIT_FAILED commit-tree status=128/);
      assert.match(error.message, /stderr=.*not a valid object/); assert(!error.message.includes("synthetic message")); return true;
    });
    const paths = Array.from({ length: 80 }, (_, i) => `conflict-${String(i).padStart(3, "0")}-${"x".repeat(100)}`);
    const commit = async (parent, content) => {
      f.git("read-tree", parent.tree);
      for (const path of paths) await writeFile(join(f.root, path), content);
      f.git("add", "--", ...paths); const tree = f.git("write-tree");
      return { tree, commit: f.git("commit-tree", tree, "-p", parent.commit, "-m", content) };
    };
    const base = await commit(f.old, "base\n"), left = await commit(base, "left\n"), right = await commit(base, "right\n");
    assert.throws(() => nativeGitV2(f.root, "merge-tree", "--write-tree", "--no-messages", `--merge-base=${base.commit}`, left.commit, right.commit), error => {
      assert.match(error.message, /^GIT_COMPOSITION_CONFLICT merge-tree status=1 signal=null/);
      assert.match(error.message, /stdout=.*conflict-000/); assert.match(error.message, /truncated=true/);
      assert(error.message.length < 9000); assert(!/[\x00-\x1f]/.test(error.message)); return true;
    });
  } finally { await f.cleanup(); }
});
test("empty-stderr merge conflicts preserve status and conflict paths without a proposal", async () => {
  const f = await fixture(); try {
    await writeFile(join(f.root, "file"), "divergent\n"); f.git("add", "file"); const tree = f.git("write-tree");
    const expected = { tree, commit: f.git("commit-tree", tree, "-p", f.old.commit, "-m", "conflicting prefix") };
    const raw = spawnSync("git", [...gitOptionsV2, "merge-tree", "--write-tree", "--no-messages", `--merge-base=${f.old.commit}`, expected.commit, f.candidate.commit], { cwd: f.root, env, encoding: "utf8" });
    assert.equal(raw.status, 1); assert.equal(raw.stderr, "");
    assert.throws(() => composeGitV2({ ...f.op, expected }), /GIT_COMPOSITION_CONFLICT.*status=1.*stdout=.*file/s);
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit);
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
if (process.argv[2] === "--timeout-crash-owner") {
  const f = await runtimeFixture();
  await writeFile(process.argv[4], JSON.stringify({ dir: f.dir, root: f.root, storeRoot: f.storeRoot, candidate: f.candidate, runId: f.run.runId }));
  await new GitDriverV2(f.runtime, f.root, { failpoint: async point => {
    if (point !== "intent") return;
    if (process.argv[3] === "true") await f.store.transaction(async (s, publish) => { delete s.runs[f.run.runId].gitOperations[0].commandTimeoutMs; await publish(); });
    process.kill(process.pid, "SIGKILL");
  } }).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, undefined, 14400000);
  throw Error("timeout crash failpoint not reached");
}
if (process.argv[2] === "--crash-node-owner") {
  const f = await nodeRuntimeFixture();
  await writeFile(process.argv[4], JSON.stringify({ dir: f.dir, root: f.root, storeRoot: f.storeRoot, candidate: f.candidate, old: f.old, node: f.node, runId: f.run.runId }));
  await new GitDriverV2(f.runtime, f.root, { failpoint: async point => { if (point === process.argv[3]) process.kill(process.pid, "SIGKILL"); } }).integrate(mutationV2(f.run), "a", 1, f.candidate, undefined, f.node);
  throw Error("node owner crash failpoint not reached");
}
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
