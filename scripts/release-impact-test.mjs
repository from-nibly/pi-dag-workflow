import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { classifyReleaseImpact, fullReleaseImpact, releaseBaseAndChangedPaths, V2_FOCUSED_SUITES, V2_REQUIRED_PACKAGE_FILES, PRODUCT_PACKAGE_SMOKE_PATH, RELEASE_SUITE_TIMEOUT_MS, RELEASE_AGGREGATE_BUDGET_SECONDS, RELEASE_CACHE_INPUTS, RELEASE_CACHE_POLICY, runtimePackageSources } from "./release-impact.mjs";

const run = promisify(execFile);

const guidance = classifyReleaseImpact(["extensions/dag-workflow/worker-runtime/integration.ts", "README.md", "spec/owned-worker-runtime/spec.md"]);
assert.deepEqual(guidance.focused, ["test:workers", ...V2_FOCUSED_SUITES]);
assert.deepEqual(guidance.dogfoodGroups, []);
assert.deepEqual(guidance.portfolioTemplates, []);
assert.equal(guidance.full, false);

const planning = classifyReleaseImpact(["extensions/dag-workflow/planning/integration.ts"]);
assert.deepEqual(planning.focused, ["test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start", ...V2_FOCUSED_SUITES]);
assert.equal(planning.full, false);
assert.deepEqual(classifyReleaseImpact(["extensions/dag-workflow/command-prompts/plan.md"]).focused, planning.focused, "plan prompt changes retain planning coverage");
assert.deepEqual(classifyReleaseImpact(["spec/prototypes/dag-widget-activity-lanes/render.mjs"]).focused, ["test:dag-widget", "test:dag-v2-product"], "widget changes retain widget and product coverage");
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
assert.deepEqual(full.focused, [
  "test:release-impact", "test:model", "test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start",
  "test:dag-runtime", "test:dag-widget", "test:dag-evaluation", "test:git-integration", "test:workers",
  "test:dag-v2-state", "test:dag-v2-lifecycle", "test:dag-v2-context", "test:dag-v2-historical-evaluation", "test:dag-v2-git-acceptance",
  "test:dag-v2-git-attributes", "test:dag-v2-git-hooks", "test:dag-v2-workspace", "test:dag-v2-product",
]);
assert.equal(new Set(full.focused).size, full.focused.length);
assert(!full.focused.includes("test:dag-v2-git-characterization"), "unsafe Git characterization is optional, not V2 acceptance");
assert.equal(full.dogfoodGroups.length, 6);
assert.equal(full.portfolioTemplates.length, 6);
assert.equal(full.recoveryDrills.length, 3);

for (const path of ["extensions/dag-workflow/index.ts", "scripts/smoke-test.mjs"]) {
  const impact = classifyReleaseImpact([path]);
  assert.equal(impact.full, false);
  assert.deepEqual(impact.focused, ["test:dag-planning-command", "test:dag-widget", "test:workers", ...V2_FOCUSED_SUITES], `${path}: wiring-only changes cannot rely on package smoke for focused suites`);
}
for (const path of ["extensions/dag-workflow/planning/v2.ts", "extensions/dag-workflow/planning/freshness-v2.ts", "extensions/dag-workflow/worker-runtime/manager.mjs"]) {
  for (const suite of V2_FOCUSED_SUITES) assert(classifyReleaseImpact([path]).focused.includes(suite), `${path} selects ${suite}`);
}
for (const file of ["state.ts", "service.ts", "git-driver.ts", "git-native.ts", "command-runner.ts", "command-supervisor.py", "reference-transaction.py", "product.ts"]) {
  const impact = classifyReleaseImpact([`extensions/dag-workflow/runtime-v2/${file}`]);
  assert.equal(impact.full, true);
  assert.deepEqual(impact.focused, full.focused);
}
for (const path of [...V2_FOCUSED_SUITES.map(script => `scripts/${script.slice(5)}-test.mjs`), PRODUCT_PACKAGE_SMOKE_PATH, "scripts/fixtures/command-fork-handoff.py"]) {
  assert.deepEqual(classifyReleaseImpact([path]).focused, V2_FOCUSED_SUITES, `${path} selects V2 coverage`);
}
assert.equal(classifyReleaseImpact(["scripts/dag-v2-git-test.mjs"]).full, true);
assert(RELEASE_SUITE_TIMEOUT_MS >= 3_600_000);
assert(RELEASE_AGGREGATE_BUDGET_SECONDS >= 28_800);
assert.equal(RELEASE_CACHE_POLICY, "release-input-policy-v2");
for (const path of ["extensions/dag-workflow/index.ts", "extensions/dag-workflow/runtime-v2/product.ts", "extensions/dag-workflow/planning/v2.ts", "extensions/dag-workflow/worker-runtime/manager.mjs", "scripts/dag-v2-product-test.mjs", "scripts/release-readiness.mjs"]) {
  assert(RELEASE_CACHE_INPUTS.some(input => path === input || path.startsWith(`${input}/`)), `${path} must invalidate cache inputs`);
}
const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
for (const suite of V2_FOCUSED_SUITES) assert.equal(packageJson.scripts[suite], `node scripts/${suite.slice(5)}-test.mjs`);
assert.equal(packageJson.scripts["test:dag-v2-git-characterization"], "node scripts/dag-v2-git-test.mjs");
assert.equal(packageJson.scripts["release:full"], "node scripts/release-readiness.mjs --full --no-cache");
for (const path of V2_REQUIRED_PACKAGE_FILES) await access(new URL(`../${path}`, import.meta.url));
for (const helper of ["command-supervisor.py", "reference-transaction.py", "git-native.ts", "git-lock.ts"]) assert(V2_REQUIRED_PACKAGE_FILES.includes(`extensions/dag-workflow/runtime-v2/${helper}`));
// The product suite/hook are joined integration prerequisites, not manufactured
// tests here. This test checks selection, not their existence or a product PASS.
const repository = await mkdtemp(join(tmpdir(), "pi-release-impact-"));
try {
  await run("git", ["init", "-b", "main"], { cwd: repository }); await run("git", ["config", "user.name", "Release Impact Test"], { cwd: repository }); await run("git", ["config", "user.email", "release-impact@example.invalid"], { cwd: repository });
  await writeFile(join(repository, "README.md"), "base\n"); await run("git", ["add", "."], { cwd: repository }); await run("git", ["commit", "-m", "base"], { cwd: repository }); await run("git", ["tag", "v0.1.0"], { cwd: repository });
  const workerDirectory = join(repository, "extensions", "dag-workflow", "worker-runtime"); await mkdir(workerDirectory, { recursive: true }); await writeFile(join(workerDirectory, "integration.ts"), "export {};\n"); await run("git", ["add", "."], { cwd: repository }); await run("git", ["commit", "-m", "worker guidance"], { cwd: repository });
  const selection = await releaseBaseAndChangedPaths(repository);
  assert.equal(selection.base, "v0.1.0"); assert.deepEqual(selection.paths, ["extensions/dag-workflow/worker-runtime/integration.ts"]);
  const nativeDirectory = join(repository, "extensions/dag-workflow/runtime-v2/nested"); await mkdir(nativeDirectory, { recursive: true });
  await writeFile(join(nativeDirectory, "new-helper.py"), "# package-owned helper\n");
  await writeFile(join(nativeDirectory, "new-native.ts"), "export {};\n");
  await writeFile(join(nativeDirectory, "README.md"), "not source\n");
  assert.deepEqual(await runtimePackageSources(repository), ["extensions/dag-workflow/runtime-v2/nested/new-helper.py", "extensions/dag-workflow/runtime-v2/nested/new-native.ts", "extensions/dag-workflow/worker-runtime/integration.ts"], "source inventory discovers new nested native/Python helpers without Git metadata");
  const tree = async () => (await run("git", ["ls-tree", "-r", "HEAD", "--", ...RELEASE_CACHE_INPUTS], { cwd: repository })).stdout;
  const previous = await tree();
  await run("git", ["add", "."], { cwd: repository }); await run("git", ["commit", "-m", "new native helpers"], { cwd: repository });
  assert.notEqual(await tree(), previous, "new V2 sources change actual tracked cache input tree");
} finally { await rm(repository, { recursive: true, force: true }); }

console.log("Release impact classification tests OK");
