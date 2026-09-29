import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { withWorkspaceOwnership, assertWorkspaceLaunch, inspectWorkspaceRoot } from "../extensions/dag-workflow/worker-runtime/workspace-ownership.mjs";

const repository = await mkdtemp(join(tmpdir(), "node-workspace-lock-test-"));
const cwd = join(repository, "node"); await mkdir(cwd);
const moduleUrl = pathToFileURL(resolve("extensions/dag-workflow/worker-runtime/workspace-ownership.mjs")).href;
let child;
try {
  const state = { version: 1, repository, cwd, root: await inspectWorkspaceRoot(cwd), nodeId: "run/a", epoch: 1,
    binding: null, launchKey: "run/a/1", execution: null, handoffs: [] };
  await withWorkspaceOwnership(repository, cwd, async (_, publish) => publish(state));
  let output = "", errors = "";
  child = spawn(process.execPath, ["--input-type=module", "-e", `
    import {withWorkspaceOwnership} from ${JSON.stringify(moduleUrl)};
    await withWorkspaceOwnership(process.argv[1],process.argv[2],async(state,publish)=>{
      state.binding={workerStorageId:'fixture',launchOwnerSessionId:'fixture',workerId:'fixture',attemptNumber:1,attemptNonce:'fixture',configHash:'sha256:'+ 'a'.repeat(64)};
      state.handoffs=[{binding:state.binding,completion:{completionId:'fixture',terminalStatus:'succeeded'},at:new Date().toISOString()}];
      state.launchKey=null; state.execution='execution-one'; await publish(state);
      process.stdout.write('published-and-locked\\n');
      await new Promise(resolve=>process.stdin.once('data',resolve));
    });
  `, repository, cwd], { stdio: ["pipe", "pipe", "pipe"] });
  child.stdout.on("data", bytes => output += bytes); child.stderr.on("data", bytes => errors += bytes);
  const exited = new Promise(resolve => child.once("exit", (code, signal) => resolve({ code, signal })));
  for (let i = 0; !output.includes("published-and-locked"); i++) { assert(i < 500, errors); await delay(10); }
  let entered = false;
  const waiting = withWorkspaceOwnership(repository, cwd, async state => {
    entered = true;
    assert.equal(state.execution, "execution-one", "process death must not release durable mutation ownership");
    assert.throws(() => assertWorkspaceLaunch(state, "run/a/1"), /NODE_WORKSPACE_LAUNCH_RELINQUISHED/);
    assert.throws(() => assertWorkspaceLaunch(state, "run/a/2"), /NODE_WORKSPACE_LAUNCH_RELINQUISHED/);
  });
  await delay(100); assert.equal(entered, false, "another process cannot pass the handoff lock");
  child.kill("SIGKILL"); assert.equal((await exited).signal, "SIGKILL"); await waiting;
  // No recovery timestamp, TTL, or dead-owner override is involved. The lock is
  // reusable after death, while the execution claim continues to block writers.
  await withWorkspaceOwnership(repository, cwd, async (state, publish) => {
    assert.equal(state.execution, "execution-one");
    await assert.rejects(publish({ ...state, epoch: 0 }), /INVALID_WORKSPACE_OWNERSHIP/);
  });
  console.log("PASS cross-process handoff serialization, death retains execution ownership, invalid ownership fails audit");
  for (const first of ["ancestor", "nested"]) {
    const ancestor = join(repository, first), nested = join(ancestor, "dependency"), alias = join(repository, `${first}-alias`);
    await mkdir(nested, { recursive: true });
    for (const path of [ancestor, nested]) execFileSync("git", ["init", path], { stdio: "ignore" });
    await symlink(nested, alias);
    const claimed = first === "ancestor" ? ancestor : nested, waitingPath = first === "ancestor" ? alias : ancestor;
    output = ""; errors = "";
    child = spawn(process.execPath, ["--input-type=module", "-e", `
      import {withWorkspaceOwnership,inspectWorkspaceRoot} from ${JSON.stringify(moduleUrl)};
      const [repository,cwd]=process.argv.slice(1);
      await withWorkspaceOwnership(repository,cwd,async(state,publish)=>{
        if(state)throw Error('expected no ownership');
        process.stdout.write('locked-before-publish\\n');
        await new Promise(resolve=>process.stdin.once('data',resolve));
        await publish({version:1,repository,cwd,root:await inspectWorkspaceRoot(cwd),nodeId:'race/a',epoch:1,binding:null,launchKey:'race/a/1',execution:null,handoffs:[]});
      });
    `, repository, claimed], { stdio: ["pipe", "pipe", "pipe"] });
    child.stdout.on("data", bytes => output += bytes); child.stderr.on("data", bytes => errors += bytes);
    const done = new Promise(resolve => child.once("exit", (code, signal) => resolve({ code, signal })));
    for (let i = 0; !output.includes("locked-before-publish"); i++) { assert(i < 500, errors); await delay(10); }
    let completed = false, entered = false;
    const waiting = assert.rejects(withWorkspaceOwnership(repository, waitingPath, async () => { entered = true; }), /NODE_WORKSPACE_OVERLAP/).finally(() => { completed = true; });
    await delay(100); assert.equal(entered, false); assert.equal(completed, false, "overlap lookup must wait for adoption, even when no journal exists yet");
    child.stdin.end("publish"); assert.equal((await done).code, 0, errors); await waiting; assert.equal(entered, false);
    if (first === "ancestor") await assert.rejects(withWorkspaceOwnership(repository, join(alias, "removed-root"), async () => {}), /NODE_WORKSPACE_OVERLAP/);
    console.log(`PASS ${first}-first cross-process absent-journal race and canonical alias overlap`);
  }
} finally { child?.kill("SIGKILL"); await rm(repository, { recursive: true, force: true }); }
