import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { CommandRunnerV2, stageChecksV2 } from "../../extensions/dag-workflow/runtime-v2/index.ts";

export const mutationV2 = r => ({ runId: r.runId, expectedRevision: r.revision, lease: r.lease });
export const fixtureContractV2 = "The committed fixture is a nonempty newline-terminated baseline or integration record, never a conflict marker. This plain record has no database schema.";
export const fixtureSourceV2 = { ref: "fixture:independent-record-contract", digest: `sha256:${createHash("sha256").update(fixtureContractV2).digest("hex")}` };
export const fixtureLifecycleV2 = () => ({
  oracle: { statement: fixtureContractV2, sourceRefs: ["fixture:independent-record-contract"], checkIds: ["behavior"] },
  checks: [
    ["static", "assert.equal(typeof text, 'string'); assert(text.length > 0)"],
    ["behavior", "assert.match(text, /^(baseline|integrated [a-z])\\n$/)"],
    ["codification", "assert(text.endsWith('\\n')); assert.equal(text.split('\\n').length, 2)"],
    ["accumulated", "assert(text.length > 1); assert(!text.includes('\\0'))"],
    ["review", "assert(!/<<<<<<<|=======|>>>>>>>/.test(text)); assert.equal(text.trim().split(' ').length <= 2, true)"],
    ["hardening", "assert.equal(Buffer.from(text).toString('utf8'), text); assert(!text.includes('\\ufffd'))"],
    ["final", "assert.match(text, /^(baseline|integrated [a-z])\\n$/); assert(!text.includes('\\0'))"],
  ].map(([id, assertion], i) => ({ id, stage: i + 1, expectation: assertion, sourceRefs: ["fixture:independent-record-contract"], applicability: { kind: "required" },
    procedure: { kind: "command", argv: [process.execPath, "--input-type=module", "-e", `import assert from 'node:assert/strict'; import {readFileSync} from 'node:fs'; const text=readFileSync('file','utf8'); ${assertion}; console.log(${JSON.stringify(id + ": verified record")});`] },
    environment: "node-local", replay: "pure" })),
});
export async function fixtureGitV2(root) {
  const repository = join(root, "lifecycle-repo"); await mkdir(repository);
  const git = (...args) => execFileSync("git", args, { cwd: repository, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  git("init", "-b", "main"); git("config", "user.email", "test@example.invalid"); git("config", "user.name", "V2 lifecycle test");
  await writeFile(join(repository, "file"), "baseline\n"); git("add", "."); git("commit", "-m", "baseline");
  return { repository, git, candidate: { commit: git("rev-parse", "HEAD"), tree: git("rev-parse", "HEAD^{tree}") } };
}
export async function finishLifecycleV2(runtime, plan, run, itemId, candidate, repository) {
  const runner = new CommandRunnerV2(runtime.store, repository);
  run = await runtime.setCandidate(mutationV2(run), itemId, run.nodes[itemId].generation, candidate, runner);
  for (let stage = 1; stage <= 7; stage++) {
    for (const check of stageChecksV2(plan, itemId, stage)) {
      run = await runtime.prepareCheck(mutationV2(run), itemId, run.nodes[itemId].generation, check.id);
      const execution = run.nodes[itemId].lifecycle.executions.at(-1);
      await runner.ensure(execution.request);
      run = await runtime.recordResult(mutationV2(run), itemId, execution.request.id, runner);
    }
    run = await runtime.advanceLifecycle(mutationV2(run), itemId, run.nodes[itemId].generation, stage);
  }
  return runtime.advanceLifecycle(mutationV2(run), itemId, run.nodes[itemId].generation, 8);
}
