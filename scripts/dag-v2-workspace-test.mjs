import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink, chmod } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { RuntimeV2, StoreV2, CommandRunnerV2, GitDriverV2, selectorV2, gitEnvironmentV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";
import { fixtureLifecycleV2, fixtureSourceV2, finishLifecycleV2, mutationV2 as m } from "./fixtures/dag-v2-lifecycle.mjs";

const tests = [], test = (name, fn) => tests.push([name, fn]);
const env = gitEnvironmentV2(), fresh = { current: async p => ({ repository: p.repository, source: p.source }) };
const gitAt = (cwd, ...args) => execFileSync("git", ["-c", "core.hooksPath=/dev/null", ...args], { cwd, env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
async function fixture({ phase = "prefix", body = "", lifecycleBody, format = "sha1", extraFiles = false } = {}) {
  const dir = await mkdtemp(join(tmpdir(), "v2-workspace-test-")), root = join(dir, "repo"), storeRoot = join(dir, "store");
  await mkdir(root); await mkdir(storeRoot);
  const git = (...args) => gitAt(root, ...args);
  git("init", "-b", "main", `--object-format=${format}`); git("config", "user.name", "fixture"); git("config", "user.email", "fixture@example.invalid");
  await writeFile(join(root, "file"), "baseline\n");
  if (extraFiles) {
    await mkdir(join(root, "nested")); await writeFile(join(root, "nested", "white\tspace\nfile"), "raw\0bytes\n");
    await writeFile(join(root, "executable"), "#!/bin/sh\nexit 0\n"); await chmod(join(root, "executable"), 0o755);
    await symlink("file", join(root, "link"));
  }
  git("add", "."); git("commit", "-m", "base");
  const old = { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") };
  await writeFile(join(root, "file"), "integrated a\n"); git("add", "file");
  const tree = git("write-tree"), candidate = { tree, commit: git("commit-tree", tree, "-p", old.commit, "-m", "candidate") };
  git("restore", "--source=HEAD", "--staged", "--worktree", ".");
  const store = new StoreV2(storeRoot), runtime = new RuntimeV2(store, fresh), counter = join(dir, "count"), lifecycle = fixtureLifecycleV2();
  const command = (id, code) => ({ id, argv: [process.execPath, "-e", `require('node:fs').appendFileSync(${JSON.stringify(counter)},${JSON.stringify(id + "\n")}); ${code}`] });
  if (lifecycleBody !== undefined) lifecycle.checks[0].procedure = { kind: "command", argv: command("static", lifecycleBody).argv };
  const plan = await runtime.save({ planId: "workspace", title: "Workspace no-edit verification",
    repository: { repositoryId: "workspace", baselineCommit: old.commit, baselineTree: old.tree, targetBranch: "refs/heads/main" },
    source: { governingClosure: `sha256:${"a".repeat(64)}`, refs: [fixtureSourceV2], scopeSummary: "disposable repository" },
    architecture: { outcomes: [{ id: "result", description: "No hidden workspace edits" }], nonGoals: ["sandboxing"], notes: [], risks: [] },
    workItems: [{ id: "a", title: "a", objective: "a", outcomeIds: ["result"], context: [], checks: ["actual verification"], dependsOn: [], risk: "low", riskNotes: [], resources: {}, gates: [], lifecycle }],
    constraints: { maxConcurrency: 1, resources: {}, mutexGroups: [], gates: [] },
    integration: { strategy: "serial", checks: ["prefix"], finalChecks: ["final"], prefixCommands: [command("prefix", phase === "prefix" ? body : "")], finalCommands: [command("final", phase === "final" ? body : "")] } }, 0);
  let run = await runtime.start({ intent: "run", sessionId: "session", selection: selectorV2(plan), authority: { scope: ["a"], maxConcurrency: 1, effects: ["repository_local"], expiresAt: Date.now() + 3600000 } }, 1);
  run = await runtime.acquireLease(run.runId, "session", run.revision);
  run = await runtime.reserve(m(run), "a", 1, "implement a"); run = await runtime.dispatch(m(run), "a", 1, { ensure: async () => ({ workerId: "worker-a" }) });
  return { dir, root, git, old, candidate, store, storeRoot, runtime, plan, run, counter, reload: () => new RuntimeV2(new StoreV2(storeRoot), fresh), cleanup: async () => {
    // The assertions must observe retention first. Only the fixture owner then
    // removes its disposable diagnostic workspaces, never production cleanup.
    for (const job of Object.values((await store.read()).executions ?? {})) if (job.workspace) await rm(dirname(job.workspace), { recursive: true, force: true });
    await rm(dir, { recursive: true, force: true });
  } };
}
const hideEdit = flag => `const fs=require('node:fs'), cp=require('node:child_process'); cp.execFileSync('git',['update-index',${JSON.stringify(flag)},'file']); fs.writeFileSync('file','no-edit violation\\n'); console.log('DIRTY_BYTES='+fs.readFileSync('file','utf8')); require('node:assert/strict').equal(cp.execFileSync('git',['status','--porcelain=v1'],{encoding:'utf8'}),'');`;
for (const flag of ["--assume-unchanged", "--skip-worktree"]) for (const phase of ["prefix", "final"]) test(`native ${phase} ${flag}: zero-exit hidden edit is nonPASS, retained, never landed or replayed`, async () => {
  const f = await fixture({ phase, body: hideEdit(flag) }); try {
    f.run = await finishLifecycleV2(f.runtime, f.plan, f.run, "a", f.candidate, f.root);
    const before = f.git("reflog", "show", "--format=%H", "main");
    await assert.rejects(new GitDriverV2(f.runtime, f.root).integrate(m(f.run), "a", 1, f.candidate), /GIT_CHECK_NONPASS/);
    let state = await f.store.read(), run = state.runs[f.run.runId], op = run.gitOperations[0];
    const request = op.checks.find(c => c.check.id === `integration-${phase}`), job = state.executions[request.id], result = job.result;
    assert.equal(job.status, "settled"); assert.equal(result.executor.invoked, true); assert.equal(result.exitCode, 0);
    assert.equal(result.workspace.cleanBefore, true); assert.equal(result.workspace.cleanAfter, false); assert.equal(result.disposition, "FAIL");
    assert.match(result.stdout, /DIRTY_BYTES=no-edit violation/); assert.match(result.diagnostic, /changed during no-edit.*retained workspace/s);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "no-edit violation\n");
    assert.equal(gitAt(job.workspace, "status", "--porcelain=v1"), ""); assert.equal(gitAt(job.workspace, "write-tree"), op.proposal.tree);
    const durableJob = JSON.stringify(job);
    await assert.rejects(new GitDriverV2(f.reload(), f.root).integrate(m(run), "a", 1, f.candidate), /GIT_CHECK_NONPASS/);
    state = await new StoreV2(f.storeRoot).read(); run = state.runs[run.runId]; op = run.gitOperations[0];
    assert.equal(JSON.stringify(state.executions[request.id]), durableJob); assert.equal(op.dispatches, 0); assert.equal(op.landing, undefined);
    assert.notEqual(op.phase, "accepted"); assert.notEqual(run.nodes.a.status, "complete"); assert.notEqual(run.status, "complete");
    assert.equal(f.git("rev-parse", "HEAD"), f.old.commit); assert.equal(f.git("reflog", "show", "--format=%H", "main"), before);
    assert.equal(await readFile(join(f.root, "file"), "utf8"), "baseline\n");
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "no-edit violation\n");
    assert.equal(await readFile(f.counter, "utf8"), phase === "prefix" ? "prefix\n" : "prefix\nfinal\n");
    if (phase === "prefix") assert.equal(state.executions[op.checks[1].id], undefined);
  } finally { await f.cleanup(); }
});

async function lifecycleRequest(f, runner) {
  f.run = await f.runtime.setCandidate(m(f.run), "a", 1, f.candidate, runner);
  f.run = await f.runtime.prepareCheck(m(f.run), "a", 1, "static");
  return f.run.nodes.a.lifecycle.executions.at(-1).request;
}
for (const flag of ["--assume-unchanged", "--skip-worktree"]) test(`lifecycle ${flag} without byte edits is still ambiguous, nonPASS and retained on reload`, async () => {
  const f = await fixture({ lifecycleBody: `require('node:child_process').execFileSync('git',['update-index',${JSON.stringify(flag)},'file'])` }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner);
    await runner.ensure(request); const result = await runner.read(request), job = (await f.store.read()).executions[request.id];
    assert.equal(result.exitCode, 0); assert.equal(result.disposition, "FAIL"); assert.equal(result.workspace.cleanAfter, false);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "integrated a\n");
    const reloaded = new CommandRunnerV2(new StoreV2(f.storeRoot), f.root); await reloaded.ensure(request); assert.deepEqual(await reloaded.read(request), result);
    f.run = await f.reload().recordResult(m(f.run), "a", request.id, reloaded);
    await assert.rejects(f.runtime.advanceLifecycle(m(f.run), "a", 1, 1), /CHECK_NOT_PASSED/);
    assert.equal(await readFile(f.counter, "utf8"), "static\n");
  } finally { await f.cleanup(); }
});

test("lifecycle fsmonitor-valid is rejected even when inspection disables fsmonitor", async () => {
  const body = `const fs=require('node:fs'), cp=require('node:child_process'), path=require('node:path'), hook=path.join(path.dirname(process.cwd()),'fsmonitor-hook');
    fs.writeFileSync(hook, '#!/bin/sh\\nprintf "token\\\\0"\\n', {mode:0o700});
    for (const args of [['config','core.fsmonitor',hook],['config','core.fsmonitorHookVersion','2'],['update-index','--fsmonitor'],['update-index','--fsmonitor-valid','file']]) cp.execFileSync('git',['-c','core.fsmonitor='+hook,...args]);
    require('node:assert/strict').equal(cp.execFileSync('git',['-c','core.fsmonitor='+hook,'ls-files','-f'],{encoding:'utf8'}),'h file\\n');
    require('node:assert/strict').equal(cp.execFileSync('git',['-c','core.fsmonitor=false','ls-files','-f'],{encoding:'utf8'}),'H file\\n');`;
  const f = await fixture({ lifecycleBody: body }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner);
    await runner.ensure(request); const result = await runner.read(request), job = (await f.store.read()).executions[request.id];
    assert.equal(result.exitCode, 0, result.stderr); assert.equal(result.disposition, "FAIL"); assert.equal(result.workspace.cleanAfter, false);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "integrated a\n");
    assert.match(gitAt(job.workspace, "ls-files", "-f"), /^h /);
  } finally { await f.cleanup(); }
});

for (const flag of ["--split-index", "--untracked-cache"]) test(`lifecycle ${flag} state is nonordinary and retained`, async () => {
  const f = await fixture({ lifecycleBody: `require('node:child_process').execFileSync('git',['update-index',${JSON.stringify(flag)}])` }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner); await runner.ensure(request);
    const result = await runner.read(request), job = (await f.store.read()).executions[request.id];
    assert.equal(result.exitCode, 0); assert.equal(result.disposition, "FAIL"); assert.equal(result.workspace.cleanAfter, false);
    assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "integrated a\n");
  } finally { await f.cleanup(); }
});

test("lifecycle raw bytes defeat a forged clean stat cache without index flags", async () => {
  // Refresh with an old mtime first so Git's racy-timestamp fallback cannot
  // accidentally make this cache-blindness reproduction depend on wall time.
  const python = "import os,subprocess\ns=os.stat('file')\nsubprocess.check_call(['git','config','core.trustctime','false'])\nsubprocess.check_call(['git','config','core.checkstat','minimal'])\nos.utime('file',ns=(s.st_atime_ns,s.st_mtime_ns-60000000000))\nsubprocess.check_call(['git','update-index','--refresh'])\ns=os.stat('file')\nopen('file','wb').write(b'modified now\\n')\nos.utime('file',ns=(s.st_atime_ns,s.st_mtime_ns))\nassert subprocess.check_output(['git','status','--porcelain=v1']) == b''\nassert subprocess.check_output(['git','ls-files','-v']) == b'H file\\n'\n";
  const f = await fixture({ lifecycleBody: `require('node:child_process').execFileSync('python3',['-I','-c',${JSON.stringify(python)}]); console.log('status and ordinary flags still clean')` }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner); await runner.ensure(request);
    const result = await runner.read(request), job = (await f.store.read()).executions[request.id];
    assert.equal(result.exitCode, 0, result.stderr); assert.equal(result.disposition, "FAIL"); assert.equal(result.workspace.cleanAfter, false);
    assert.match(result.stdout, /ordinary flags still clean/); assert.equal(await readFile(join(job.workspace, "file"), "utf8"), "modified now\n");
  } finally { await f.cleanup(); }
});

for (const format of ["sha1", "sha256"]) for (const indexVersion of [2, 3, 4]) test(`${format} index v${indexVersion}: ordinary raw files, executable, symlink and unusual paths PASS and clean up`, async () => {
  const f = await fixture({ lifecycleBody: `require('node:child_process').execFileSync('git',['update-index','--index-version=${indexVersion}'])`, format, extraFiles: true }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner); await runner.ensure(request);
    const result = await runner.read(request), job = (await f.store.read()).executions[request.id];
    assert.equal(result.disposition, "PASS", result.diagnostic); assert.equal(result.workspace.cleanAfter, true);
    await assert.rejects(readFile(join(job.workspace, "file")), { code: "ENOENT" });
  } finally { await f.cleanup(); }
});

test("lifecycle ignores core.fileMode=false as evidence of unchanged executable mode", async () => {
  const f = await fixture({ lifecycleBody: "const cp=require('node:child_process'); cp.execFileSync('git',['config','core.fileMode','false']); require('node:fs').chmodSync('file',0o755); require('node:assert/strict').equal(cp.execFileSync('git',['status','--porcelain=v1'],{encoding:'utf8'}),'')" }); try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner); await runner.ensure(request);
    const result = await runner.read(request); assert.equal(result.exitCode, 0); assert.equal(result.disposition, "FAIL"); assert.equal(result.workspace.cleanAfter, false);
  } finally { await f.cleanup(); }
});

for (const point of ["before-verification", "before-cleanup"]) test(`${point}: an ambiguous workspace is never invoked/deleted based on an earlier clean observation`, async () => {
  const f = await fixture({ lifecycleBody: "" }); let workspace;
  try {
    const runner = new CommandRunnerV2(f.store, f.root), request = await lifecycleRequest(f, runner);
    const original = runner.clean.bind(runner); let calls = 0;
    // Fault injection only in this runner instance. First clean is materialization;
    // fourth is after the real argv's supervised settlement. Dirty immediately
    // after that observation to exercise the independent invocation/cleanup gate.
    runner.clean = async (cwd, candidate) => {
      workspace = cwd; const clean = await original(cwd, candidate); calls++;
      if (calls === (point === "before-verification" ? 1 : 4)) {
        gitAt(cwd, "update-index", "--assume-unchanged", "file");
        execFileSync(process.execPath, ["-e", "require('node:fs').writeFileSync('file','retained between checks\\n')"], { cwd });
      }
      return clean;
    };
    await runner.ensure(request); const result = await runner.read(request);
    assert.equal(result.disposition, "BLOCKED", result.diagnostic); assert.equal(result.workspace.cleanAfter, false);
    assert.equal(result.executor.invoked, point === "before-cleanup");
    assert.equal(await readFile(join(workspace, "file"), "utf8"), "retained between checks\n");
    assert.match(result.diagnostic, point === "before-cleanup" ? /cleanup retained/ : /UNCLEAN_EXECUTION_WORKSPACE/);
    if (point === "before-verification") await assert.rejects(readFile(f.counter), { code: "ENOENT" });
  } finally { if (workspace) await rm(dirname(workspace), { recursive: true, force: true }); await f.cleanup(); }
});

let failed = 0;
for (const [name, fn] of tests) { const start = performance.now(); try { await fn(); console.log(`ok - ${name} (${Math.round(performance.now() - start)}ms)`); } catch (error) { failed++; console.error(`not ok - ${name}`, error); } }
console.log(`${tests.length - failed}/${tests.length} workspace tests passed`); if (failed) process.exitCode = 1;
