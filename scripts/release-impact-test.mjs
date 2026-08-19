import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { classifyReleaseImpact, fullReleaseImpact, releaseBaseAndChangedPaths } from "./release-impact.mjs";

const run = promisify(execFile);

const guidance = classifyReleaseImpact(["extensions/dag-workflow/worker-runtime/integration.ts", "README.md", "spec/owned-worker-runtime/spec.md"]);
assert.deepEqual(guidance.focused, ["test:workers"]);
assert.deepEqual(guidance.dogfoodGroups, []);
assert.deepEqual(guidance.portfolioTemplates, []);
assert.equal(guidance.full, false);

const planning = classifyReleaseImpact(["extensions/dag-workflow/planning/integration.ts"]);
assert.deepEqual(planning.focused, ["test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start"]);
assert.equal(planning.full, false);
assert.deepEqual(classifyReleaseImpact(["extensions/dag-workflow/command-prompts/plan.md"]).focused, planning.focused, "plan prompt changes retain planning coverage");
assert.deepEqual(classifyReleaseImpact(["spec/prototypes/dag-widget-activity-lanes/render.mjs"]).focused, ["test:dag-widget"], "widget prototype changes retain widget coverage");
assert.deepEqual(classifyReleaseImpact(["project-model/migrations/brainstorm-v2-candidate.md"]).focused, ["test:model"], "model migration changes retain model coverage");

const git = classifyReleaseImpact(["extensions/dag-workflow/dag-runtime/git-integration.ts"]);
assert.deepEqual(git.dogfoodGroups, ["baseline", "composition", "validation", "landing"]);
assert.deepEqual(git.portfolioTemplates, ["integration-train"]);
assert.deepEqual(git.recoveryDrills, ["target_drift_conflict"]);

const conductor = classifyReleaseImpact(["extensions/dag-workflow/dag-runtime/conductor.ts"]);
assert.deepEqual(conductor.dogfoodGroups, ["lifecycle", "landing", "cleanup"]);
assert.deepEqual(conductor.portfolioTemplates, ["recovery-sensitive"]);
assert.deepEqual(conductor.recoveryDrills, ["conductor_crash_resume"]);

const modelOnly = classifyReleaseImpact(["project-model/model.json", "spec/model-aware-dag-runtime/spec.md"]);
assert.deepEqual(modelOnly.focused, ["test:model"]);
assert.equal(modelOnly.full, false);

for (const path of ["extensions/dag-workflow/dag-runtime/reducer.ts", "unknown/new-runtime.xyz"]) assert.equal(classifyReleaseImpact([path]).full, true, `${path} fails closed to a full gate`);
const full = fullReleaseImpact();
assert.equal(full.focused.length, 11);
assert.equal(full.dogfoodGroups.length, 6);
assert.equal(full.portfolioTemplates.length, 6);
assert.equal(full.recoveryDrills.length, 3);

const repository = await mkdtemp(join(tmpdir(), "pi-release-impact-"));
try {
  await run("git", ["init", "-b", "main"], { cwd: repository }); await run("git", ["config", "user.name", "Release Impact Test"], { cwd: repository }); await run("git", ["config", "user.email", "release-impact@example.invalid"], { cwd: repository });
  await writeFile(join(repository, "README.md"), "base\n"); await run("git", ["add", "."], { cwd: repository }); await run("git", ["commit", "-m", "base"], { cwd: repository }); await run("git", ["tag", "v0.1.0"], { cwd: repository });
  const workerDirectory = join(repository, "extensions", "dag-workflow", "worker-runtime"); await mkdir(workerDirectory, { recursive: true }); await writeFile(join(workerDirectory, "integration.ts"), "export {};\n"); await run("git", ["add", "."], { cwd: repository }); await run("git", ["commit", "-m", "worker guidance"], { cwd: repository });
  const selection = await releaseBaseAndChangedPaths(repository);
  assert.equal(selection.base, "v0.1.0"); assert.deepEqual(selection.paths, ["extensions/dag-workflow/worker-runtime/integration.ts"]);
} finally { await rm(repository, { recursive: true, force: true }); }

console.log("Release impact classification tests OK");
