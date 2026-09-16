import assert from "node:assert/strict";
import { link, mkdtemp, mkdir, readFile, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import { RuntimeV2, StoreV2, selectorV2, createPlanV2, renderPlanV2, inspectPlanFileV2, auditSnapshotV2 } from "../extensions/dag-workflow/runtime-v2/index.ts";

import { createDagPlanningPlanV1 } from "../extensions/dag-workflow/planning/artifact.ts";

const tests = [], test = (name, fn) => tests.push([name, fn]);
const NOW = 1000;
const moduleUrl = pathToFileURL(resolve("extensions/dag-workflow/runtime-v2/index.ts")).href;
function input(planId = "plan-a") {
  const node = (id, dependsOn = []) => ({ id, title: id, objective: `Implement ${id}`, outcomeIds: ["outcome"], context: [], checks: ["actual check"], dependsOn, risk: "low", riskNotes: [], resources: { cpu: 1 }, gates: [] });
  return { planId, title: "V2 test plan", repository: { repositoryId: "repo", baselineCommit: "1".repeat(40), baselineTree: "2".repeat(40), targetBranch: "refs/heads/main" },
    source: { governingClosure: `sha256:${"3".repeat(64)}`, refs: [], scopeSummary: "local changes only" }, architecture: { outcomes: [{ id: "outcome", description: "working changes" }], nonGoals: ["publication"], notes: [], risks: [] },
    workItems: [node("a"), node("b", ["a"]), node("c"), node("d")], constraints: { maxConcurrency: 2, resources: { cpu: 2 }, mutexGroups: [], gates: [] },
    integration: { strategy: "dependency_order", checks: ["Verify native Git identity"], finalChecks: ["Verify landed identity"], prefixCommands: [{ id: "prefix", argv: ["git", "rev-parse", "--verify", "HEAD"] }], finalCommands: [{ id: "final", argv: ["git", "rev-parse", "--verify", "HEAD"] }] } };
}
const fresh = { current: async p => ({ repository: p.repository, source: p.source }) };
const mutation = r => ({ runId: r.runId, expectedRevision: r.revision, lease: r.lease });
const request = (p, scope = p.workItems.map(n => n.id), sessionId = "session") => ({ intent: "run", sessionId, selection: selectorV2(p), authority: { scope, maxConcurrency: 2, effects: ["repository_local"], expiresAt: 100000 } });
async function fixture(options = {}) {
  const dir = await mkdtemp(join(tmpdir(), "dag-v2-"));
  const store = new StoreV2(dir, options), runtime = new RuntimeV2(store, fresh, () => NOW);
  const plan = await runtime.save(input(), 0);
  return { dir, store, runtime, plan, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
async function start(f, scope) {
  const r = await f.runtime.start(request(f.plan, scope), (await f.store.read()).revision);
  return f.runtime.acquireLease(r.runId, "session", r.revision);
}
function child(code, args = []) {
  const p = spawn(process.execPath, ["--input-type=module", "-e", `import {RuntimeV2,StoreV2} from ${JSON.stringify(moduleUrl)}; ${code}`, ...args], { stdio: ["ignore", "pipe", "pipe"] });
  let out = "", err = ""; p.stdout.on("data", b => out += b); p.stderr.on("data", b => err += b);
  const watchdog = setTimeout(() => p.kill("SIGKILL"), 15000);
  p.result = new Promise((resolve, reject) => { p.on("error", reject); p.on("close", (code, signal) => { clearTimeout(watchdog); resolve({ code, signal, out, err }); }); });
  return p;
}
async function waitFile(path) { for (let i = 0; i < 500; i++) { try { await readFile(path); return; } catch { await delay(10); } } throw new Error(`Timed out: ${path}`); }
const echoWorkers = { ensure: async r => ({ workerId: `worker-${r.itemId}-${r.generation}` }) };
const evidence = (r, id) => ({ operationId: `${r.nodes[id].reservation.operationId}/integration`, runId: r.runId, itemId: id, generation: r.nodes[id].generation, candidate: { commit: "4".repeat(40), tree: "5".repeat(40) }, target: { commit: "6".repeat(40), tree: "5".repeat(40) } });

// These fixtures implement the trusted boundary explicitly; production evidence/Git
// adapters belong to N03/N04, not to worker completion claims.
test("save/show/revise are inert; current explicit run has no extra plan transition", async () => {
  const f = await fixture(); try {
    const original = JSON.stringify(f.plan);
    assert.match(renderPlanV2(await f.runtime.show()), /V2 test plan/);
    assert.equal(Object.keys((await f.store.read()).runs).length, 0);
    const p2 = await f.runtime.save({ ...input(), title: "Revised" }, 1);
    await assert.rejects(f.runtime.start(request(f.plan), 2), /PLAN_SELECTION_STALE/);
    await assert.rejects(f.runtime.start({ ...request(p2), intent: "preview" }, 2), /INVALID_V2/);
    const run = await f.runtime.start(request(p2), 2);
    assert.equal(run.status, "active"); assert.equal(run.revision, 0);
    assert.deepEqual(JSON.parse(JSON.stringify((await f.store.read()).plans[f.plan.planId][0])), JSON.parse(original));
    assert.equal((await f.store.read()).plans[f.plan.planId].length, 2);
    assert.equal((await f.runtime.start(request(p2), 3)).runId, run.runId);
    for (const forbidden of ["approval", "approvalReceipt", "approvalRevision", "status"]) {
      assert.throws(() => createPlanV2({ ...input(), [forbidden]: "approved" }, 1), /INVALID_V2/);
    }
    await f.runtime.save(input("plan-other"), 3);
    await assert.rejects(f.runtime.show(), /PLAN_SELECTION_REQUIRED/);
    await assert.rejects(f.runtime.start(request(p2), 3), /STALE_REVISION/);
  } finally { await f.cleanup(); }
});
test("closed plan inputs reject invalid record keys, values and envelope fields without publication", async () => {
  const f = await fixture(); try {
    const original = await readFile(f.store.statePath, "utf8");
    for (const key of ["bad/key", "constructor", "prototype", "__proto__"]) {
      for (const value of [1, "not-a-count"]) for (const resource of [p => p.constraints.resources, p => p.workItems[0].resources]) {
        const data = input(); Object.defineProperty(resource(data), key, { value, enumerable: true });
        assert.throws(() => createPlanV2(data, 2), /INVALID_V2/);
        await assert.rejects(f.runtime.save(data, 1), /INVALID_V2/);
        assert.equal(await readFile(f.store.statePath, "utf8"), original);
      }
    }
    for (const [key, value] of [["kind", "dag_plan_v1"], ["kind", "dag_plan_v2"], ["schemaVersion", 1], ["schemaVersion", 2], ["revision", 99], ["planHash", "garbage"]]) {
      const data = { ...input(), [key]: value };
      assert.throws(() => createPlanV2(data, 2), /INVALID_V2/);
      await assert.rejects(f.runtime.save(data, 1), /INVALID_V2/);
      assert.equal(await readFile(f.store.statePath, "utf8"), original);
    }
    const invalid = input(); invalid.constraints.resources.cpu = "not-a-count";
    await assert.rejects(f.runtime.save(invalid, 1), /INVALID_V2/);
    assert.equal(await readFile(f.store.statePath, "utf8"), original);
    assert.equal((await f.store.read()).revision, 1);
    // Rejected poison cannot make the next otherwise-valid start unreloadable.
    const r = await start(f); assert.equal((await f.store.read()).runs[r.runId].status, "active");
  } finally { await f.cleanup(); }
});
test("every snapshot record is closed and semantic output failures preserve bytes and revision", async () => {
  const f = await fixture(); try {
    const r = await start(f), before = await f.store.read();
    const original = await readFile(f.store.statePath, "utf8");
    const records = [s => s.plans, s => s.runs, s => s.bindings, s => s.runs[r.runId].nodes];
    for (const record of records) for (const key of ["bad/key", "constructor", "prototype", "__proto__"]) {
      const bad = structuredClone(before); Object.defineProperty(record(bad), key, { value: "unvalidated", enumerable: true });
      assert.throws(() => auditSnapshotV2(bad), /INVALID_V2/);
      await assert.rejects(f.store.transaction(async (s, publish) => {
        Object.defineProperty(record(s), key, { value: "unvalidated", enumerable: true }); await publish();
      }), /INVALID_V2/);
      assert.equal(await readFile(f.store.statePath, "utf8"), original);
    }
    await assert.rejects(f.store.transaction(async (s, publish) => {
      s.bindings.session = "missing-run"; await publish();
    }), /BINDING_MISMATCH/);
    await assert.rejects(f.store.transaction(async (s, publish) => {
      s.runs[r.runId].nodes.a.status = "active"; await publish();
    }), /ACTIVE_WITHOUT_RESERVATION/);
    assert.equal(await readFile(f.store.statePath, "utf8"), original);
    assert.equal((await f.store.read()).revision, before.revision);
  } finally { await f.cleanup(); }
});
test("freshness, closed scope and restricted effects fail before creation", async () => {
  const f = await fixture(); try {
    const stale = new RuntimeV2(f.store, { current: async p => ({ repository: { ...p.repository, baselineCommit: "9".repeat(40) }, source: p.source }) }, () => NOW);
    await assert.rejects(stale.start(request(f.plan), 1), /PLAN_STALE/);
    await assert.rejects(f.runtime.start(request(f.plan, ["b"]), 1), /SCOPE_NOT_DEPENDENCY_CLOSED/);
    await assert.rejects(f.runtime.start({ ...request(f.plan), authority: { ...request(f.plan).authority, effects: ["publish"] } }, 1), /INVALID_V2/);
    await assert.rejects(f.runtime.start({ ...request(f.plan), authority: { ...request(f.plan).authority, expiresAt: NOW } }, 1), /AUTHORITY_EXPIRED/);
    let r = await start(f, ["a"]);
    await assert.rejects(f.runtime.reserve(mutation(r), "c", 1, "excluded"), /ITEM_NOT_ADMISSIBLE/);
    assert.deepEqual(await f.runtime.frontier(r.runId), ["a"]);
    assert.equal(r.nodes.c.status, "excluded");
  } finally { await f.cleanup(); }
});
test("dependencies, sticky concurrency, resources, mutexes and gates govern admission", async () => {
  const f = await fixture(); try {
    const data = input(); data.constraints.mutexGroups = [{ id: "shared", workItemIds: ["a", "c"], reason: "shared mutation" }];
    data.constraints.gates = ["environment"]; data.workItems[3].gates = ["environment"];
    f.plan = await f.runtime.save(data, 1);
    let r = await start(f);
    assert.deepEqual(await f.runtime.frontier(r.runId), ["a", "c"]);
    r = await f.runtime.reserve(mutation(r), "a", 1, "work a");
    await assert.rejects(f.runtime.reserve(mutation(r), "c", 1, "work c"), /ITEM_NOT_ADMISSIBLE/);
    await assert.rejects(f.runtime.reserve(mutation(r), "b", 1, "work b"), /ITEM_NOT_ADMISSIBLE/);
    r = await f.runtime.releaseGate(mutation(r), "environment", async () => {});
    r = await f.runtime.reserve(mutation(r), "d", 1, "work d");
    assert.deepEqual(await f.runtime.frontier(r.runId), []);
    // Separate resource limit narrows concurrency without changing semantic edges.
    const g = await fixture(); try {
      const d = input(); d.constraints.resources.cpu = 1; g.plan = await g.runtime.save(d, 1);
      let q = await start(g); q = await g.runtime.reserve(mutation(q), "a", 1, "a");
      await assert.rejects(g.runtime.reserve(mutation(q), "c", 1, "c"), /ITEM_NOT_ADMISSIBLE/);
    } finally { await g.cleanup(); }
  } finally { await f.cleanup(); }
});
test("revision/lease/generation fences and exact reservation request prevent duplicates", async () => {
  const f = await fixture(); try {
    let r = await start(f); const old = structuredClone(r);
    r = await f.runtime.reserve(mutation(r), "a", 1, "payload");
    await assert.rejects(f.runtime.reserve(mutation(old), "c", 1, "other"), /STALE_REVISION/);
    assert.equal((await f.runtime.reserve(mutation(r), "a", 1, "payload")).revision, r.revision);
    await assert.rejects(f.runtime.reserve(mutation(r), "a", 1, "changed"), /RESERVATION_REQUEST_CONFLICT/);
    let calls = 0;
    r = await f.runtime.dispatch(mutation(r), "a", 1, { ensure: async () => { calls++; return { workerId: "worker" }; } });
    r = await f.runtime.dispatch(mutation(r), "a", 1, { ensure: async () => { throw new Error("duplicate dispatch"); } });
    assert.equal(calls, 1);
    const late = evidence(r, "a");
    r = await f.runtime.replace(mutation(r), "a", 1, async () => {});
    await assert.rejects(f.runtime.integrate(mutation(r), late, { verify: async () => { throw Error("must not reach verifier"); } }), /STALE_GENERATION/);
    await assert.rejects(f.runtime.dispatch(mutation(r), "a", 1, echoWorkers), /STALE_GENERATION/);
    const oldLease = r.lease;
    r = await f.runtime.acquireLease(r.runId, "session", r.revision);
    await assert.rejects(f.runtime.reserve({ ...mutation(r), lease: oldLease }, "c", 1, "c"), /STALE_LEASE/);
    assert.equal(r.nodes.a.status, "active");
  } finally { await f.cleanup(); }
});
test("atomic creation recovers both sides of rename; lease/reservation survive reload", async () => {
  for (const point of ["temp_synced", "renamed", "directory_synced"]) {
    const f = await fixture(); try {
      const crashing = new RuntimeV2(new StoreV2(f.dir, { failpoint: p => { if (p === point) throw new Error(`crash ${p}`); } }), fresh, () => NOW);
      await assert.rejects(crashing.start(request(f.plan), 1), /crash/);
      const s = await f.store.read();
      assert.equal(Object.keys(s.runs).length, point === "temp_synced" ? 0 : 1);
      const r = await f.runtime.start(request(f.plan), s.revision);
      assert.equal(Object.keys((await f.store.read()).runs).length, 1);
      let owned = await f.runtime.acquireLease(r.runId, "session", r.revision);
      owned = await f.runtime.reserve(mutation(owned), "a", 1, "recoverable");
      const reloaded = new RuntimeV2(new StoreV2(f.dir), fresh, () => NOW);
      const recovered = (await reloaded.store.read()).runs[r.runId];
      assert.deepEqual(JSON.parse(JSON.stringify(recovered)), JSON.parse(JSON.stringify(owned)));
      assert.equal((await reloaded.reserve(mutation(recovered), "a", 1, "recoverable")).revision, owned.revision);
    } finally { await f.cleanup(); }
  }
});
test("launch acknowledgement loss uses same durable natural identity after recovery", async () => {
  const f = await fixture(); try {
    let r = await start(f); r = await f.runtime.reserve(mutation(r), "a", 1, "work");
    const workers = new Map(); let launches = 0, loseAck = true;
    const adapter = { ensure: async x => { if (!workers.has(x.operationId)) { workers.set(x.operationId, "worker-1"); launches++; } if (loseAck) { loseAck = false; throw new Error("ack lost"); } return { workerId: workers.get(x.operationId) }; } };
    await assert.rejects(f.runtime.dispatch(mutation(r), "a", 1, adapter), /ack lost/);
    r = (await new StoreV2(f.dir).read()).runs[r.runId];
    assert.equal(r.nodes.a.reservation.state, "dispatching");
    r = await f.runtime.dispatch(mutation(r), "a", 1, adapter);
    assert.equal(launches, 1); assert.equal(r.nodes.a.reservation.workerId, "worker-1");
  } finally { await f.cleanup(); }
});
test("absent store inspection is inert; static symlinks and nonregular files fail closed", async () => {
  const unsafe = /ELOOP|ENOTDIR|UNSAFE_STORE_FILE/;
  for (const target of [".ai", ".ai/dag-workflow-v2", ".ai/dag-workflow-v2/state.json", ".ai/dag-workflow-v2/writer.lock"]) {
    const root = await mkdtemp(join(tmpdir(), "dag-v2-link-")), outside = await mkdtemp(join(tmpdir(), "dag-v2-outside-"));
    try {
      const store = new StoreV2(root), rt = new RuntimeV2(store, fresh, () => NOW);
      assert.equal((await store.read()).revision, 0); assert.deepEqual(await readdir(root), []);
      const isFile = /json$|lock$/.test(target), victim = join(outside, "victim");
      await writeFile(victim, "untouched\n");
      const parts = target.split("/"); await mkdir(join(root, ...parts.slice(0, -1)), { recursive: true });
      await symlink(isFile ? victim : outside, join(root, target));
      const before = await readdir(outside);
      await assert.rejects(store.read(), unsafe);
      await assert.rejects(rt.save(input(), 0), unsafe);
      assert.equal(await readFile(victim, "utf8"), "untouched\n");
      assert.deepEqual(await readdir(outside), before);
    } finally { await rm(root, { recursive: true, force: true }); await rm(outside, { recursive: true, force: true }); }
  }
  for (const name of ["state.json", "writer.lock"]) for (const kind of ["directory", "fifo", "hardlink"]) {
    const root = await mkdtemp(join(tmpdir(), "dag-v2-special-"));
    try {
      const store = new StoreV2(root); await mkdir(store.directory, { recursive: true });
      const path = join(store.directory, name);
      if (kind === "directory") await mkdir(path);
      if (kind === "fifo") execFileSync("mkfifo", [path]);
      if (kind === "hardlink") { await writeFile(join(root, "victim"), "untouched"); await link(join(root, "victim"), path); }
      await assert.rejects(store.read(), /UNSAFE_STORE_FILE/);
      await assert.rejects(new RuntimeV2(store, fresh).save(input(), 0), /UNSAFE_STORE_FILE|EISDIR/);
      if (kind === "hardlink") assert.equal(await readFile(join(root, "victim"), "utf8"), "untouched");
    } finally { await rm(root, { recursive: true, force: true }); }
  }
});
test("ancestor replacement cannot redirect a locked transaction or publication", async () => {
  for (const level of ["repository", ".ai", "store"]) for (const point of ["locked", "temp_synced"]) for (const replacement of ["directory", "symlink"]) {
    const f = await fixture(), outside = await mkdtemp(join(tmpdir(), "dag-v2-swap-"));
    const path = level === "repository" ? f.dir : level === ".ai" ? join(f.dir, ".ai") : f.store.directory;
    const detached = `${path}-detached`;
    const suffix = level === "repository" ? ".ai/dag-workflow-v2" : level === ".ai" ? "dag-workflow-v2" : "";
    const original = await readFile(f.store.statePath, "utf8");
    let swapped = false;
    try {
      await writeFile(join(outside, "sentinel"), "unrelated");
      const store = new StoreV2(f.dir, { failpoint: async p => {
        if (p !== point || swapped) return; swapped = true;
        await rename(path, detached);
        if (replacement === "symlink") await symlink(outside, path); else await mkdir(path);
      } });
      await assert.rejects(new RuntimeV2(store, fresh).save({ ...input(), title: "must not publish" }, 1), /STORE_DIRECTORY_REPLACED/);
      assert(swapped);
      assert.equal(await readFile(join(detached, suffix, "state.json"), "utf8"), original);
      assert.deepEqual((await readdir(join(detached, suffix))).sort(), ["state.json", "writer.lock"]);
      assert.deepEqual(await readdir(outside), ["sentinel"]);
      assert.equal(await readFile(join(outside, "sentinel"), "utf8"), "unrelated");
      if (replacement === "directory") {
        assert.equal((await store.read()).revision, 0);
        assert.deepEqual(await readdir(path), []); // No repair/creation by views.
      } else await assert.rejects(store.read(), /ELOOP|ENOTDIR/);
    } finally { await rm(detached, { recursive: true, force: true }); await f.cleanup(); await rm(outside, { recursive: true, force: true }); }
  }
});
test("state and lock replacement before publication preserve original and replacement bytes", async () => {
  for (const name of ["state.json", "writer.lock"]) for (const replacement of ["file", "symlink"]) {
    const f = await fixture();
    try {
      const path = join(f.store.directory, name), original = await readFile(path, "utf8");
      const before = await readFile(f.store.statePath, "utf8"), victim = join(f.dir, "victim");
      await writeFile(victim, "unrelated");
      const store = new StoreV2(f.dir, { failpoint: async point => {
        if (point !== "temp_synced") return;
        await rename(path, `${path}.original`);
        if (replacement === "file") await writeFile(path, "replacement"); else await symlink(victim, path);
      } });
      await assert.rejects(new RuntimeV2(store, fresh).save({ ...input(), title: "rejected" }, 1), /STORE_STATE_REPLACED|STORE_LOCK_REPLACED|UNSAFE_STORE_FILE/);
      assert.equal(await readFile(`${path}.original`, "utf8"), original);
      assert.equal(await readFile(path, "utf8"), replacement === "file" ? "replacement" : "unrelated");
      assert.equal(await readFile(victim, "utf8"), "unrelated");
      if (name === "writer.lock") assert.equal(await readFile(f.store.statePath, "utf8"), before);
    } finally { await f.cleanup(); }
  }
});
test("process-shared OS lock blocks concurrent writers and releases after SIGKILL", async () => {
  const f = await fixture(); try {
    const signal = join(f.dir, "locked");
    const p = child(`import {writeFile} from 'node:fs/promises'; const store=new StoreV2(process.argv[1]); await store.transaction(async()=>{ await writeFile(process.argv[2],'locked'); await new Promise(()=>{setInterval(()=>{},1000)}); });`, [f.dir, signal]);
    await waitFile(signal);
    await assert.rejects(f.runtime.save(input("second"), 1), /STORE_BUSY/);
    p.kill("SIGKILL"); assert.equal((await p.result).signal, "SIGKILL");
    await f.runtime.save(input("second"), 1);
    assert.equal((await f.store.read()).revision, 2);
  } finally { await f.cleanup(); }
});
test("live cross-process lease blocks takeover; proven-dead lease recovers and fences", async () => {
  const f = await fixture(); try {
    let r = await f.runtime.start(request(f.plan), 1);
    const signal = join(f.dir, "leased");
    const p = child(`import {writeFile} from 'node:fs/promises'; const rt=new RuntimeV2(new StoreV2(process.argv[1]),{current:async p=>({repository:p.repository,source:p.source})},()=>1000); const r=await rt.acquireLease(process.argv[2],'session',0); await writeFile(process.argv[3],JSON.stringify(r)); setInterval(()=>{},1000);`, [f.dir, r.runId, signal]);
    await waitFile(signal); const old = JSON.parse(await readFile(signal, "utf8"));
    await assert.rejects(f.runtime.acquireLease(r.runId, "session", old.revision), /LIVE_LEASE_CONFLICT/);
    p.kill("SIGKILL"); await p.result;
    r = await f.runtime.acquireLease(r.runId, "session", old.revision);
    assert.equal(r.lease.generation, old.lease.generation + 1);
    await assert.rejects(f.runtime.reserve({ ...mutation(r), lease: old.lease }, "a", 1, "a"), /STALE_LEASE/);
  } finally { await f.cleanup(); }
});
test("competing process CAS saves cannot overwrite the winning content revision", async () => {
  const f = await fixture(); try {
    const code = `const rt=new RuntimeV2(new StoreV2(process.argv[1]),{current:async()=>{throw Error('unused')}}); try {const p=JSON.parse(process.argv[2]);await rt.save(p,1); console.log('saved');}catch(e){console.log(e.message);}`;
    const ps = ["one", "two"].map(title => child(code, [f.dir, JSON.stringify({ ...input(), title })]));
    const results = await Promise.all(ps.map(p => p.result));
    assert.equal(results.filter(r => r.out.includes("saved")).length, 1, JSON.stringify(results));
    assert(results.some(r => /STORE_BUSY|STALE_REVISION/.test(r.out)));
    assert.equal((await f.store.read()).plans["plan-a"].length, 2);
  } finally { await f.cleanup(); }
});
test("real Git producer landing and fresh persisted reload release dependent successor", async () => {
  const f = await fixture(); try {
    const repo = join(f.dir, "git"); await mkdir(repo);
    const git = (...args) => execFileSync("git", args, { cwd: repo, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
    git("init", "-b", "main"); git("config", "user.email", "test@example.invalid"); git("config", "user.name", "V2 test");
    await writeFile(join(repo, "file"), "baseline\n"); git("add", "."); git("commit", "-m", "baseline");
    const data = input(); data.repository.baselineCommit = git("rev-parse", "HEAD"); data.repository.baselineTree = git("rev-parse", "HEAD^{tree}");
    f.plan = await f.runtime.save(data, 1);
    let r = await start(f, ["a", "b"]);
    for (const id of ["a", "b"]) {
      const reloaded = new RuntimeV2(new StoreV2(f.dir), fresh, () => NOW);
      r = (await reloaded.store.read()).runs[r.runId];
      assert.deepEqual(await reloaded.frontier(r.runId), [id]);
      r = await reloaded.reserve(mutation(r), id, 1, `work ${id}`);
      r = await reloaded.dispatch(mutation(r), id, 1, echoWorkers);
      git("checkout", "-b", `candidate-${id}`);
      await writeFile(join(repo, "file"), `integrated ${id}\n`); git("add", "."); git("commit", "-m", id);
      const candidate = { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") };
      git("checkout", "main"); const expectedOld = git("rev-parse", "HEAD");
      const e = { ...evidence(r, id), candidate, target: candidate };
      await assert.rejects(reloaded.integrate(mutation(r), e, { verify: async () => { throw new Error("missing checks"); } }), /missing checks/);
      r = await reloaded.integrate(mutation(r), e, { verify: async (_run, _plan, exact) => {
        assert.equal(git("rev-parse", "HEAD"), expectedOld);
        assert.equal(git("rev-parse", `${exact.candidate.commit}^{tree}`), exact.candidate.tree);
        git("merge", "--ff-only", exact.candidate.commit);
        assert.equal(git("rev-parse", "HEAD"), exact.target.commit);
      } });
      assert.equal(r.nodes[id].status, "complete");
      if (id === "a") assert.equal(r.status, "active");
    }
    assert.equal(r.status, "complete");
    const current = await f.store.read(); const bytes = JSON.stringify(current.runs[r.runId]);
    // Exact completion replay is read-only even after the last node completes.
    assert.equal((await f.runtime.integrate(mutation(r), r.nodes.b.integration, { verify: async () => { throw Error("duplicate"); } })).revision, r.revision);
    const next = await f.runtime.save({ ...input("successor"), predecessor: selectorV2(f.plan) }, current.revision);
    const successor = await f.runtime.start(request(next), (await f.store.read()).revision);
    assert.notEqual(successor.runId, r.runId); assert.equal(successor.predecessorRunId, r.runId);
    assert.equal(JSON.stringify((await f.store.read()).runs[r.runId]), bytes);
    const conflict = await f.runtime.save({ ...input("conflict"), predecessor: selectorV2(next) }, (await f.store.read()).revision);
    await assert.rejects(f.runtime.start(request(conflict), (await f.store.read()).revision), /ACTIVE_BINDING_CONFLICT/);
    auditSnapshotV2(await f.store.read());
  } finally { await f.cleanup(); }
});
test("serial admission protects the prefix from later independent sticky lanes", async () => {
  for (const capacity of [1, 2]) {
    const f = await fixture(); try {
      const data = input(); data.integration.strategy = "serial"; data.constraints.maxConcurrency = capacity;
      f.plan = await f.runtime.save(data, 1);
      const req = request(f.plan); req.authority.maxConcurrency = capacity;
      let r = await f.runtime.start(req, 2); r = await f.runtime.acquireLease(r.runId, "session", r.revision);
      const before = await readFile(f.store.statePath, "utf8");
      await assert.rejects(f.runtime.reserve(mutation(r), "c", 1, "later independent"), /ITEM_NOT_ADMISSIBLE/);
      assert.equal(await readFile(f.store.statePath, "utf8"), before);
      for (const id of ["a", "b", "c", "d"]) {
        const rt = new RuntimeV2(new StoreV2(f.dir), fresh, () => NOW);
        r = (await rt.store.read()).runs[r.runId];
        assert.deepEqual(await rt.frontier(r.runId), [id]);
        r = await rt.reserve(mutation(r), id, 1, id);
        assert.deepEqual(await rt.frontier(r.runId), []);
        r = await rt.dispatch(mutation(r), id, 1, echoWorkers);
        r = await rt.integrate(mutation(r), evidence(r, id), { verify: async () => {} });
        auditSnapshotV2(await rt.store.read());
      }
      assert.equal(r.status, "complete");
    } finally { await f.cleanup(); }
  }
});
test("serial ordering rejects later dependencies and audits out-of-prefix state before publication", async () => {
  const f = await fixture(); try {
    const original = await readFile(f.store.statePath, "utf8");
    const invalid = input(); invalid.integration.strategy = "serial"; invalid.workItems[0].dependsOn = ["c"];
    assert.throws(() => createPlanV2(invalid, 2), /SERIAL_DEPENDENCY_ORDER/);
    await assert.rejects(f.runtime.save(invalid, 1), /SERIAL_DEPENDENCY_ORDER/);
    assert.equal(await readFile(f.store.statePath, "utf8"), original);
    assert.equal((await f.store.read()).revision, 1);
    // The same acyclic topology is legal when integration is dependency-ordered.
    invalid.integration.strategy = "dependency_order"; createPlanV2(invalid, 2);
    const data = input(); data.integration.strategy = "serial"; f.plan = await f.runtime.save(data, 1);
    const r = await start(f), before = await readFile(f.store.statePath, "utf8");
    await assert.rejects(f.store.transaction(async (s, publish) => {
      const n = s.runs[r.runId].nodes.c;
      n.status = "active"; n.reservation = { operationId: `${r.runId}/c/1`, runId: r.runId, itemId: "c", generation: 1, request: "later", state: "reserved" };
      await publish();
    }), /SERIAL_PREFIX_NOT_COMPLETE/);
    assert.equal(await readFile(f.store.statePath, "utf8"), before);
  } finally { await f.cleanup(); }
});
test("serial safe prefix, pause and replan hold; explicit disposition resumes unchanged plan", async () => {
  const f = await fixture(); try {
    const data = input(); data.integration.strategy = "serial"; f.plan = await f.runtime.save(data, 1);
    await assert.rejects(f.runtime.start(request(f.plan, ["c"]), 2), /SCOPE_NOT_INTEGRATION_CLOSED/);
    let r = await start(f, ["a", "b"]);
    r = await f.runtime.control(mutation(r), "pause"); assert.deepEqual(await f.runtime.frontier(r.runId), []);
    await assert.rejects(f.runtime.reserve(mutation(r), "a", 1, "work"), /RUN_NOT_ACTIVE/);
    r = await f.runtime.control(mutation(r), "needs_replan");
    r = await f.runtime.control(mutation(r), "pause");
    assert.equal(r.status, "needs_replan");
    await assert.rejects(f.runtime.control(mutation(r), "resume"), /REPLAN_DISPOSITION_REQUIRED/);
    r = await f.runtime.control(mutation(r), "resume", "Finding dismissed: no semantic change");
    assert.match(r.replanDisposition, /dismissed/); assert.deepEqual(await f.runtime.frontier(r.runId), ["a"]);
  } finally { await f.cleanup(); }
});
test("unsupported historical input stays byte/path preserving and cannot start V2", async () => {
  const f = await fixture(); try {
    // Inspect real V1 schema bytes through the version-dispatched reader, without
    // creating any store artifacts, then reject unsupported future versions.
    const history = join(f.dir, "history"); await mkdir(history);
    const path = join(history, "old.json"), raw = '{ "kind": "dag_plan_v9", "schemaVersion": 9 }\n';
    const base = input("old-plan");
    const legacy = createDagPlanningPlanV1({ ...base, status: "draft", focusId: null,
      source: { refs: [{ kind: "external", ref: "fixture:source" }], scopeSummary: base.source.scopeSummary },
      workItems: base.workItems.map(({ resources, gates, ...n }) => n),
      constraints: { maxConcurrency: 2, mutexGroups: [] },
      approval: { status: "pending", by: null, at: null, note: null },
      authorization: { status: "not_authorized", by: null, at: null, scope: [], maxConcurrency: null, note: null },
    }, "2026-01-01T00:00:00.000Z");
    const legacyPath = join(history, "v1.json"), legacyBytes = JSON.stringify(legacy, null, 3) + "\n";
    await writeFile(legacyPath, legacyBytes);
    const legacyBefore = await readdir(history);
    assert.equal((await inspectPlanFileV2(legacyPath)).version, 1);
    assert.equal(await readFile(legacyPath, "utf8"), legacyBytes);
    assert.deepEqual(await readdir(history), legacyBefore);
    await assert.rejects(f.runtime.start({ ...request(f.plan), selection: { planId: legacy.planId, revision: legacy.revision, planHash: legacy.planHash } }, 1), /PLAN_SELECTION_STALE/);
    await writeFile(path, raw); const before = await readdir(history);
    await assert.rejects(inspectPlanFileV2(path), /UNSUPPORTED_PLAN_VERSION/);
    assert.equal(await readFile(path, "utf8"), raw); assert.deepEqual(await readdir(history), before);
    await assert.rejects(f.runtime.start({ ...request(f.plan), approval: { status: "approved" } }, 1), /INVALID_V2/);
  } finally { await f.cleanup(); }
});
test("actual process death during creation, lease and reservation publication recovers", async () => {
  for (const operation of ["start", "lease", "reserve"]) for (const point of ["temp_synced", "renamed"]) {
    const f = await fixture(); try {
      if (operation !== "start") await f.runtime.start(request(f.plan), 1);
      const code = `const root=process.argv[1], operation=process.argv[2], point=process.argv[3];
        const store=new StoreV2(root); const fresh={current:async p=>({repository:p.repository,source:p.source})};
        let rt=new RuntimeV2(store,fresh,()=>1000); let s=await store.read(); let r=Object.values(s.runs)[0];
        if(operation==='reserve') r=await rt.acquireLease(r.runId,'session',r.revision);
        rt=new RuntimeV2(new StoreV2(root,{failpoint:p=>{if(p===point)process.exit(88)}}),fresh,()=>1000);
        if(operation==='start') await rt.start(JSON.parse(process.argv[4]),s.revision);
        if(operation==='lease') await rt.acquireLease(r.runId,'session',r.revision);
        if(operation==='reserve') await rt.reserve({runId:r.runId,expectedRevision:r.revision,lease:r.lease},'a',1,'work');`;
      const result = await child(code, [f.dir, operation, point, JSON.stringify(request(f.plan))]).result;
      assert.equal(result.code, 88, result.err);
      let s = await f.store.read();
      let r = Object.values(s.runs)[0];
      if (operation === "start") r = await f.runtime.start(request(f.plan), s.revision);
      r = await f.runtime.acquireLease(r.runId, "session", r.revision);
      r = await f.runtime.reserve(mutation(r), "a", 1, "work");
      assert.equal(Object.keys((await f.store.read()).runs).length, 1);
      assert.equal(r.nodes.a.reservation.operationId, `${r.runId}/a/1`);
      assert.equal(Object.values(r.nodes).filter(n => n.status === "active").length, 1);
    } finally { await f.cleanup(); }
  }
});
test("concurrent admissions recheck capacity under the shared transaction lock", async () => {
  const f = await fixture(); try {
    let r = await start(f);
    const results = await Promise.allSettled([f.runtime.reserve(mutation(r), "a", 1, "a"), f.runtime.reserve(mutation(r), "c", 1, "c")]);
    assert.equal(results.filter(x => x.status === "fulfilled").length, 1);
    assert.match(results.find(x => x.status === "rejected").reason.message, /STORE_BUSY|STALE_REVISION/);
    r = (await f.store.read()).runs[r.runId];
    const next = r.nodes.a.status === "active" ? "c" : "a";
    r = await f.runtime.reserve(mutation(r), next, 1, next);
    await assert.rejects(f.runtime.reserve(mutation(r), "d", 1, "d"), /ITEM_NOT_ADMISSIBLE/);
    auditSnapshotV2(await f.store.read());
  } finally { await f.cleanup(); }
});
test("cancel fences before signalling; successor waits for terminal reconciliation", async () => {
  const f = await fixture(); try {
    let r = await start(f); r = await f.runtime.reserve(mutation(r), "a", 1, "work");
    r = await f.runtime.dispatch(mutation(r), "a", 1, echoWorkers);
    const late = evidence(r, "a"), oldOperation = r.nodes.a.reservation.operationId;
    r = await f.runtime.cancel(mutation(r));
    assert.equal(r.status, "cancelling"); assert.equal(r.nodes.a.generation, 2);
    assert.equal(r.nodes.a.reservation.operationId, oldOperation);
    await assert.rejects(f.runtime.integrate(mutation(r), late, { verify: async () => {} }), /STALE_GENERATION/);
    const next = await f.runtime.save({ ...input("after-cancel"), predecessor: selectorV2(f.plan) }, (await f.store.read()).revision);
    await assert.rejects(f.runtime.start(request(next), (await f.store.read()).revision), /ACTIVE_BINDING_CONFLICT/);
    await assert.rejects(f.runtime.reconcileCancellation(mutation(r), async () => { throw Error("worker still alive"); }), /worker still alive/);
    r = await f.runtime.reconcileCancellation(mutation(r), async observed => { assert.equal(observed.nodes.a.generation, 2); });
    const bytes = JSON.stringify((await f.store.read()).runs[r.runId]);
    const successor = await f.runtime.start(request(next), (await f.store.read()).revision);
    assert.equal(successor.predecessorRunId, r.runId);
    assert.equal(JSON.stringify((await f.store.read()).runs[r.runId]), bytes);
  } finally { await f.cleanup(); }
});
test("process death after keyed worker creation cannot launch a duplicate", async () => {
  const f = await fixture(); try {
    const initial = await f.runtime.start(request(f.plan), 1);
    const workerPath = join(f.dir, "durable-worker.json");
    const result = await child(`import {open} from 'node:fs/promises';
      const rt=new RuntimeV2(new StoreV2(process.argv[1]),{current:async p=>({repository:p.repository,source:p.source})},()=>1000);
      let r=await rt.acquireLease(process.argv[2],'session',0);
      r=await rt.reserve({runId:r.runId,expectedRevision:r.revision,lease:r.lease},'a',1,'work');
      await rt.dispatch({runId:r.runId,expectedRevision:r.revision,lease:r.lease},'a',1,{ensure:async reservation=>{
        const f=await open(process.argv[3],'wx'); await f.writeFile(JSON.stringify({operationId:reservation.operationId,request:reservation.request,workerId:'durable-worker'})); await f.sync(); await f.close(); process.exit(88);
      }});`, [f.dir, initial.runId, workerPath]).result;
    assert.equal(result.code, 88, result.err);
    let r = (await f.store.read()).runs[initial.runId];
    assert.equal(r.nodes.a.reservation.state, "dispatching");
    r = await f.runtime.acquireLease(r.runId, "session", r.revision);
    const original = await readFile(workerPath, "utf8");
    r = await f.runtime.dispatch(mutation(r), "a", 1, { ensure: async reservation => {
      const worker = JSON.parse(await readFile(workerPath, "utf8"));
      assert.equal(reservation.operationId, worker.operationId); assert.equal(reservation.request, worker.request);
      return { workerId: worker.workerId };
    } });
    assert.equal(r.nodes.a.reservation.workerId, "durable-worker"); assert.equal(await readFile(workerPath, "utf8"), original);
  } finally { await f.cleanup(); }
});
let failed = 0;
for (const [name, fn] of tests) {
  const at = performance.now();
  try { await fn(); console.log(`PASS ${name} (${((performance.now() - at) / 1000).toFixed(2)}s)`); }
  catch (error) { failed++; console.error(`FAIL ${name}`, error); }
}
console.log(`${tests.length - failed}/${tests.length} passed`);
process.exitCode = failed ? 1 : 0;
