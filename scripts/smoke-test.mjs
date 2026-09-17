import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile, readFile, access, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import { renderDagDiagram } from "../extensions/dag-workflow/diagram.ts";
import { configToDagBase, mergeConfig } from "../extensions/dag-workflow/config.ts";
import { PACKAGE_DEFAULT_CONFIG } from "../extensions/dag-workflow/defaults.ts";
import { getNodeFlowName } from "../extensions/dag-workflow/dag.ts";
import { ensureNodeWorktree, execGit, isConventionalCommitSubject, mergeNode, refreshNodeWorktreeFromParent } from "../extensions/dag-workflow/worktrees.ts";
import { V2_FOCUSED_SUITES, V2_REQUIRED_PACKAGE_FILES, PRODUCT_PACKAGE_SMOKE_PATH, RELEASE_SUITE_TIMEOUT_MS, runtimePackageSources } from "./release-impact.mjs";

const args = process.argv.slice(2);
if (args.some(arg => !["--package", "--package-static"].includes(arg)) || args.length > 1) throw new Error("Usage: smoke-test.mjs [--package | --package-static]");
// Diagnostic only: readiness always uses --package and cannot bypass product smoke.
const staticOnly = args.includes("--package-static");
const packageMode = args.includes("--package") || staticOnly;

const files = [
  ...V2_REQUIRED_PACKAGE_FILES, ...await runtimePackageSources(process.cwd()),
  "package.json",
  "extensions/dag-workflow/index.ts",
  "extensions/dag-workflow/types.ts",
  "extensions/dag-workflow/dag.ts",
  "extensions/dag-workflow/diagram.ts",
  "extensions/dag-workflow/config.ts",
  "extensions/dag-workflow/project-model/types.ts",
  "extensions/dag-workflow/project-model/model.ts",
  "extensions/dag-workflow/project-model/store.ts",
  "extensions/dag-workflow/project-model/sessions.ts",
  "extensions/dag-workflow/project-model/persistence.ts",
  "extensions/dag-workflow/project-model/projector.ts",
  "extensions/dag-workflow/project-model/domain.ts",
  "extensions/dag-workflow/project-model/integration.ts",
  "extensions/dag-workflow/project-model/review-turn.ts",
  "extensions/dag-workflow/project-model/review-renderer.ts",
  "extensions/dag-workflow/project-model/lavish-cli.ts",
  "extensions/dag-workflow/project-model/review-presentation.ts",
  "extensions/dag-workflow/project-model/migration.ts",
  "extensions/dag-workflow/project-model/migration-workflow.ts",
  "extensions/dag-workflow/dag-runtime/common.ts",
  "extensions/dag-workflow/dag-runtime/plan.ts",
  "extensions/dag-workflow/dag-runtime/run-state.ts",
  "extensions/dag-workflow/dag-runtime/reducer.ts",
  "extensions/dag-workflow/dag-runtime/store.ts",
  "extensions/dag-workflow/dag-runtime/git-integration.ts",
  "extensions/dag-workflow/dag-runtime/scheduler.ts",
  "extensions/dag-workflow/dag-runtime/conductor.ts",
  "extensions/dag-workflow/dag-runtime/integration.ts",
  "extensions/dag-workflow/dag-runtime/widget.ts",
  "extensions/dag-workflow/dag-runtime/widget-controller.ts",
  "extensions/dag-workflow/dag-runtime/index.ts",
  "extensions/dag-workflow/planning/types.ts",
  "extensions/dag-workflow/planning/artifact.ts",
  "extensions/dag-workflow/planning/store.ts",
  "extensions/dag-workflow/planning/selectors.ts",
  "extensions/dag-workflow/planning/projections.ts",
  "extensions/dag-workflow/planning/runtime-adapter.ts",
  "extensions/dag-workflow/planning/integration.ts",
  "extensions/dag-workflow/planning/integration-validation-pass.mjs",
  "extensions/dag-workflow/command-prompts/plan.md",
  "extensions/dag-workflow/worker-runtime/core.mjs",
  "extensions/dag-workflow/worker-runtime/child-report.ts",
  "extensions/dag-workflow/worker-runtime/supervisor.mjs",
  "extensions/dag-workflow/worker-runtime/manager.mjs",
  "extensions/dag-workflow/worker-runtime/integration.ts",
  "scripts/project-model-test.mjs",
  "scripts/dag-planning-test.mjs",
  "scripts/dag-planning-runtime-test.mjs",
  "scripts/dag-planning-command-test.mjs",
  "scripts/dag-prepared-start-test.mjs",
  "scripts/release-readiness.mjs",
  "scripts/release-impact.mjs",
  "scripts/release-impact-test.mjs",
  "scripts/dag-runtime-test.mjs",
  "scripts/dag-widget-test.mjs",
  "scripts/git-integration-test.mjs",
  "scripts/worker-runtime-test.mjs",
  "scripts/fixtures/fake-worker-rpc.mjs",
  "scripts/fixtures/dag-store-child.mjs",
  "scripts/fixtures/git-integration-crash-child.mjs",
  "scripts/migrate-brainstorm-to-project-model.mjs",
  "project-model/model.json",
  "project-model/migrations/brainstorm-v2-candidate.md",
  "project-model/migrations/brainstorm-v2-overrides.json",
  "spec/prototypes/brainstorm-pi-adapter/README.md",
  "spec/prototypes/brainstorm-pi-adapter/adapter.mjs",
  "spec/prototypes/brainstorm-pi-adapter/scenario.mjs",
  "spec/prototypes/lavish-turn-renderer/README.md",
  "spec/prototypes/lavish-turn-renderer/contract.md",
  "spec/prototypes/lavish-turn-renderer/renderer.mjs",
  "spec/prototypes/lavish-turn-renderer/scenario.mjs",
  "spec/prototypes/lavish-turn-renderer/sample-turn.html",
  "spec/prototypes/dag-widget-activity-lanes/README.md",
  "spec/prototypes/dag-widget-activity-lanes/render.mjs",
  "spec/prototypes/dag-widget-activity-lanes/scenario.mjs",
  "spec/prototypes/dag-widget-activity-lanes/prototype.html",
];

for (const file of files) await access(file);

const execFilePromise = promisify(execFile);
async function execFileAsync(executable, argv, options = {}) {
  const started = performance.now();
  try {
    const result = await execFilePromise(executable, argv, { timeout: RELEASE_SUITE_TIMEOUT_MS, maxBuffer: 8 * 1024 * 1024, ...options });
    console.log(JSON.stringify({ kind: "SmokeCommandResult", argv: [executable, ...argv], code: 0, signal: null, elapsedMs: Math.round(performance.now() - started) }));
    return result;
  } catch (error) {
    process.stdout.write(error.stdout ?? ""); process.stderr.write(error.stderr ?? "");
    console.error(JSON.stringify({ kind: "SmokeCommandResult", argv: [executable, ...argv], code: error.code ?? null, signal: error.signal ?? null, elapsedMs: Math.round(performance.now() - started) }));
    throw error;
  }
}
const extension = await import("../extensions/dag-workflow/index.ts");
if (typeof extension.default !== "function") throw new Error("Packed DAG workflow extension entrypoint is not loadable");
const runtimeV2 = await import("../extensions/dag-workflow/runtime-v2/index.ts");
for (const name of ["RuntimeV2", "StoreV2", "CommandRunnerV2"]) assert(typeof runtimeV2[name] === "function", `V2 export ${name} is loadable`);
await import("../extensions/dag-workflow/runtime-v2/git-native.ts");
await import("../extensions/dag-workflow/runtime-v2/git-lock.ts");
const pythonSources = [...new Set(files.filter(path => path.endsWith(".py")))];
await execFileAsync("python3", ["-I", "-B", "-c", "import ast,pathlib,sys; [ast.parse(pathlib.Path(p).read_bytes(), filename=p) for p in sys.argv[1:]]", ...pythonSources]);
const releaseImpact = await execFileAsync(process.execPath, ["scripts/release-impact-test.mjs"]);
assertIncludes(releaseImpact.stdout, "Release impact classification tests OK", "release impact mapping tests pass");
if (!packageMode) {
  const productionModel = await execFileAsync(process.execPath, ["scripts/project-model-test.mjs"]);
  assertIncludes(productionModel.stdout, "Project model production tests OK", "production project-model tests pass");
  const planning = await execFileAsync(process.execPath, ["scripts/dag-planning-test.mjs"]);
  assertIncludes(planning.stdout, "planning tests passed", "strict planning store/projection tests pass");
  const planningRuntime = await execFileAsync(process.execPath, ["scripts/dag-planning-runtime-test.mjs"]);
  assertIncludes(planningRuntime.stdout, "DAG planning runtime adapter tests passed", "thin-plan runtime compatibility tests pass");
  const planningCommands = await execFileAsync(process.execPath, ["scripts/dag-planning-command-test.mjs"]);
  assertIncludes(planningCommands.stdout, "DAG planning command integration tests passed", "product plan/show/run tests pass");
  const preparedStart = await execFileAsync(process.execPath, ["scripts/dag-prepared-start-test.mjs"]);
  assertIncludes(preparedStart.stdout, "dag prepared start tests passed", "prepared-start crash recovery tests pass");
  const dagRuntime = await execFileAsync(process.execPath, ["scripts/dag-runtime-test.mjs"]);
  assertIncludes(dagRuntime.stdout, "Canonical DAG plan and run-state schema tests OK", "canonical DAG schema tests pass");
  const dagWidget = await execFileAsync(process.execPath, ["scripts/dag-widget-test.mjs"]);
  assertIncludes(dagWidget.stdout, "DAG widget V2 tests OK", "responsive DAG widget/controller tests pass");
  const widgetPrototype = await execFileAsync(process.execPath, ["spec/prototypes/dag-widget-activity-lanes/scenario.mjs"]);
  assertIncludes(widgetPrototype.stdout, "DAG widget activity-lane prototype OK", "DAG widget visual prototype evidence still executes");
  const gitIntegration = await execFileAsync(process.execPath, ["scripts/git-integration-test.mjs"]);
  assertIncludes(gitIntegration.stdout, "Exact real-Git integration transaction and failpoint matrix OK", "real-Git integration failpoint matrix passes");
  const workerRuntime = await execFileAsync(process.execPath, ["scripts/worker-runtime-test.mjs"]);
  assertIncludes(workerRuntime.stdout, "Owned worker core, supervisor, and manager tests OK", "owned worker runtime tests pass");
  const adapterPrototype = await execFileAsync(process.execPath, ["spec/prototypes/brainstorm-pi-adapter/scenario.mjs"]);
  assertIncludes(adapterPrototype.stdout, "Brainstorm Pi adapter prototype OK", "legacy adapter evidence still executes");
  const lavishPrototype = await execFileAsync(process.execPath, ["spec/prototypes/lavish-turn-renderer/scenario.mjs"]);
  assertIncludes(lavishPrototype.stdout, "Lavish turn-renderer prototype OK", "Lavish turn-renderer prototype scenario passes");
  for (const script of V2_FOCUSED_SUITES) await execFileAsync("npm", ["run", script]);
}

const sampleDag = {
  schemaVersion: 1,
  run: { name: "smoke", maxConcurrency: 2 },
  defaults: { flow: "default" },
  steps: [],
  merge: { id: "merge", kind: "merge" },
  flows: { default: [] },
  nodes: [
    { id: "chunk-1", title: "Add renderer helper", chunkFile: ".ai/chunks/chunk-1.md", dependsOn: [] },
    { id: "chunk-2", title: "Update chunk prompt", chunkFile: ".ai/chunks/chunk-2.md", dependsOn: [] },
    {
      id: "chunk-3",
      title: "Document diagram output",
      chunkFile: ".ai/chunks/chunk-3.md",
      dependsOn: ["chunk-1", "chunk-2"],
    },
    { id: "chunk-4", title: "Add smoke checks", chunkFile: ".ai/chunks/chunk-4.md", dependsOn: ["chunk-3"] },
  ],
  edges: [
    { from: "chunk-1", to: "chunk-3", type: "hard" },
    { from: "chunk-2", to: "chunk-3", type: "hard" },
    { from: "chunk-3", to: "chunk-4", type: "hard" },
  ],
};

const diagram = renderDagDiagram(sampleDag);
assertIncludes(diagram.text, "Dependency sketch:", "renderer emits sketch heading");
assertIncludes(diagram.text, "chunk-1 ─┐", "renderer emits fan-in top connector");
assertIncludes(diagram.text, "├─> chunk-3", "renderer emits fan-in target connector");
assertIncludes(diagram.text, "chunk-3 ──> chunk-4", "renderer emits chained dependency");
assertIncludes(diagram.text, "First ready: chunk-1, chunk-2", "renderer emits first-ready summary");
assertIncludes(diagram.text, "maxConcurrency: 2", "renderer emits maxConcurrency summary");

const mismatchDag = {
  ...sampleDag,
  edges: [{ from: "chunk-1", to: "chunk-4", type: "hard" }],
};
const mismatch = renderDagDiagram(mismatchDag);
assert(mismatch.warnings.some((warning) => warning.includes("hard edge mismatch: chunk-1 -> chunk-4")), "renderer warns on hard edge mismatch");
assert(isConventionalCommitSubject("feat(dag): add rebase merge flow"), "accepts scoped Conventional Commit subjects");
assert(isConventionalCommitSubject("refactor!: replace merge commits"), "accepts breaking Conventional Commit subjects");
assert(!isConventionalCommitSubject("Merge DAG node chunk-1"), "rejects generated merge commit subjects");
assert(PACKAGE_DEFAULT_CONFIG.flows.default.map((step) => step.id).join(" -> ") === "setup -> execute -> validate", "default DAG flow remains setup -> execute -> validate");

testConfigMergeAndDagBase();
testNodeFlowOverrides();
await testWorktreeRefresh();

const readme = await readFile("README.md", "utf8");
assertIncludes(readme, "project-model/model.json", "README documents the shared model authority");
assertIncludes(readme, "/dag brainstorm", "README documents model brainstorming");
assertIncludes(readme, "/dag migrate", "README documents guided project-model migration");
assertIncludes(readme, "dag_model_record_direction", "README documents the direct-authority boundary");
assertIncludes(readme, "/dag plan [--new", "README documents architecture-first planning");
assertIncludes(readme, "/dag show --run", "README documents exact live inspection");
assertIncludes(readme, "/dag run [--plan", "README documents explicit product execution");
assert(!readme.includes("Model-aware DAG planning and execution remain deferred"), "README no longer advertises the shipped product workflow as deferred");
assertIncludes(readme, "subagent_report", "README documents the owned worker report boundary");
assertIncludes(readme, ".ai/worker-sessions/", "README documents durable worker state");
const packageJson = JSON.parse(await readFile("package.json", "utf8"));
assert(!packageJson.dependencies?.["pi-subagents"], "pi-subagents dependency is removed");
for (const script of [...V2_FOCUSED_SUITES, "test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start", "release:ready"]) assert(packageJson.scripts?.[script], `package exposes ${script}`);
const packed = JSON.parse((await execFileAsync("npm", ["pack", "--dry-run", "--json"], { maxBuffer: 8 * 1024 * 1024 })).stdout)[0];
const packedPaths = new Set(packed.files.map(({ path }) => path));
for (const path of [
  ...files, ...(!staticOnly ? ["scripts/dag-v2-product-test.mjs"] : []),
  "extensions/dag-workflow/planning/integration.ts",
  "extensions/dag-workflow/planning/runtime-adapter.ts",
  "extensions/dag-workflow/command-prompts/plan.md",
  "project-model/model.json",
  "project-model/migrations/brainstorm-v2-overrides.json",
  "spec/model-aware-dag-runtime/spec.md",
  "spec/prototypes/brainstorm-pi-adapter/scenario.mjs",
  "spec/prototypes/lavish-turn-renderer/scenario.mjs",
]) assert(packedPaths.has(path), `package includes ${path}`);

if (staticOnly) {
  console.log("Static package checks OK; V2 production registration/save/show/run UNRUN (--package-static is NOT release certification).");
} else {
  // Integration hook deliberately fails closed until the joined product supplies
  // real assertions. It must import the shipped default entrypoint, reject
  // dag_plan_decide/actionId writer schemas, and save/show/run V2 in a temp repo.
  // Do not point this at the full product suite or mutate the extracted package.
  assert(packedPaths.has(PRODUCT_PACKAGE_SMOKE_PATH), `joined bounded V2 product smoke is missing: ${PRODUCT_PACKAGE_SMOKE_PATH}; static checks alone are not release certification`);
  const productSmoke = await execFileAsync(process.execPath, [PRODUCT_PACKAGE_SMOKE_PATH]);
  process.stdout.write(productSmoke.stdout); process.stderr.write(productSmoke.stderr);
  console.log(packageMode
    ? "Package smoke OK: required sources/helpers, imports, package policy, legacy helper regressions and bounded V2 product smoke passed; full suites were not repeated."
    : "Smoke OK: source suites, required sources/helpers, package policy, legacy helper regressions and bounded V2 product smoke passed.");
}

function testConfigMergeAndDagBase() {
  const userConfig = {
    steps: [
      { id: "execute", model: "user-model" },
      { id: "user-validate", kind: "agent", agent: "builtin:reviewer" },
    ],
    flows: { userFlow: [{ id: "setup" }, { id: "user-validate" }] },
    nodeFlowOverrides: [{ match: "chunk-*", flow: "userFlow" }],
  };
  const projectConfig = {
    steps: [
      { id: "execute", model: "project-model", thinking: "low" },
      { id: "project-validate", kind: "agent", agent: "builtin:reviewer" },
    ],
    flows: { projectFlow: [{ id: "setup" }, { id: "execute" }, { id: "project-validate" }] },
    nodeFlowOverrides: [{ match: "chunk-7", flow: "projectFlow" }],
  };
  const merged = mergeConfig(mergeConfig(PACKAGE_DEFAULT_CONFIG, userConfig), projectConfig);
  const execute = merged.steps.find((step) => step.id === "execute");
  assert(execute.model === "project-model", "project config overrides user step fields by id");
  assert(execute.thinking === "low", "project config adds step fields by id");
  assert(merged.steps.some((step) => step.id === "user-validate"), "user step additions are preserved");
  assert(merged.steps.some((step) => step.id === "project-validate"), "project step additions are preserved");
  assert(merged.flows.userFlow.length === 2, "user flow additions are preserved");
  assert(merged.flows.projectFlow.length === 3, "project flow additions are preserved");
  assert(merged.nodeFlowOverrides.at(-1).flow === "projectFlow", "project nodeFlowOverrides are appended after user overrides");

  const dagBase = configToDagBase(merged);
  assert(dagBase.nodeFlowOverrides.length === 2, "configToDagBase materializes nodeFlowOverrides");
}

function testNodeFlowOverrides() {
  const dag = {
    schemaVersion: 1,
    run: { name: "flow-smoke", plan: ".ai/plan.md", maxConcurrency: 1 },
    defaults: { flow: "default" },
    steps: [],
    merge: { id: "merge", kind: "merge" },
    flows: { default: [], special: [], titleFlow: [], chunkFileFlow: [], exactFlow: [] },
    nodeFlowOverrides: [
      { match: "chunk-*", flow: "special" },
      { match: "*Review*", flow: "titleFlow" },
      { match: "flow-target.md", flow: "chunkFileFlow" },
      { match: "chunk-42", flow: "exactFlow" },
    ],
    nodes: [],
    edges: [],
  };
  assert(getNodeFlowName(dag, { ...node("chunk-9"), flow: "default" }) === "special", "glob-ish id override beats node flow");
  assert(getNodeFlowName(dag, { ...node("manual"), title: "Review API behavior" }) === "titleFlow", "glob-ish title override matches");
  assert(getNodeFlowName(dag, { ...node("manual"), chunkFile: ".ai/chunks/flow-target.md" }) === "chunkFileFlow", "chunk filename override matches");
  assert(getNodeFlowName(dag, node("chunk-42")) === "exactFlow", "last matching exact node id override wins");
  assert(getNodeFlowName(dag, node("other")) === "default", "default flow remains unchanged without override");
}

async function testWorktreeRefresh() {
  await withTempRepo("clean-ff", async ({ repo, baseCommit }) => {
    const b = node("B");
    const state = stateFor("clean-ff", repo, baseCommit, [b]);
    await ensureNodeWorktree(repo, state, b);
    await writeFile(join(repo, "fixture.txt"), "parent update\n");
    await git(repo, ["add", "fixture.txt"]);
    await git(repo, ["commit", "-m", "feat(parent): update fixture"]);

    const refresh = await refreshNodeWorktreeFromParent(repo, state, b);
    assert(refresh.refreshed && !refresh.blocked, "clean dependent worktree fast-forwards to parent");
    assertIncludes(await readFile(join(state.nodes.B.worktree, "fixture.txt"), "utf8"), "parent update", "refreshed worktree sees parent update");
  });

  await withTempRepo("dirty-block", async ({ repo, baseCommit }) => {
    const b = node("B");
    const state = stateFor("dirty-block", repo, baseCommit, [b]);
    await ensureNodeWorktree(repo, state, b);
    await writeFile(join(state.nodes.B.worktree, "dirty.txt"), "uncommitted\n");
    await writeFile(join(repo, "fixture.txt"), "parent update\n");
    await git(repo, ["add", "fixture.txt"]);
    await git(repo, ["commit", "-m", "feat(parent): update fixture"]);

    const refresh = await refreshNodeWorktreeFromParent(repo, state, b);
    assert(refresh.blocked, "dirty dependent worktree blocks refresh");
    assertIncludes(refresh.message, "worktree is dirty", "dirty refresh explains blocker");
    assertIncludes(refresh.message, "dag_start_node", "dirty refresh tells conductor what to retry");
  });

  await withTempRepo("dependency-visible", async ({ repo, baseCommit }) => {
    const a = node("A");
    const b = { ...node("B"), dependsOn: ["A"] };
    const state = stateFor("dependency-visible", repo, baseCommit, [a, b]);
    await ensureNodeWorktree(repo, state, b);
    assertIncludes(await readFile(join(state.nodes.B.worktree, "fixture.txt"), "utf8"), "base", "B starts from the base before dependency merge");

    await ensureNodeWorktree(repo, state, a);
    await writeFile(join(state.nodes.A.worktree, "fixture.txt"), "from dependency A\n");
    await git(state.nodes.A.worktree, ["add", "fixture.txt"]);
    await git(state.nodes.A.worktree, ["commit", "-m", "feat(a): update dependency fixture"]);
    state.nodes.A.status = "merge_ready";
    await mergeNode(repo, state, a);
    state.nodes.A.status = "merged";

    const refresh = await refreshNodeWorktreeFromParent(repo, state, b);
    assert(refresh.refreshed && !refresh.blocked, "dependent worktree refreshes after dependency merge");
    assertIncludes(await readFile(join(state.nodes.B.worktree, "fixture.txt"), "utf8"), "from dependency A", "B sees A's merged file before setup without manual fast-forward");
  });
}

async function withTempRepo(name, fn) {
  const repo = await mkdtemp(join(tmpdir(), `pi-dag-${name}-`));
  try {
    await git(repo, ["init"]);
    await git(repo, ["config", "user.email", "dag-smoke@example.com"]);
    await git(repo, ["config", "user.name", "DAG Smoke"]);
    await mkdir(join(repo, ".ai"), { recursive: true });
    await writeFile(join(repo, "fixture.txt"), "base\n");
    await git(repo, ["add", "fixture.txt"]);
    await git(repo, ["commit", "-m", "chore: initial"]);
    const baseCommit = (await git(repo, ["rev-parse", "HEAD"])).stdout.trim();
    await fn({ repo, baseCommit });
  } finally {
    await rm(repo, { recursive: true, force: true });
  }
}

function stateFor(runId, cwd, baseCommit, nodes) {
  return {
    manifest: {
      runId,
      dagPath: ".ai/dag.json",
      dagName: runId,
      cwd,
      parentBranch: "",
      baseCommit,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      status: "running",
    },
    nodes: Object.fromEntries(nodes.map((dagNode) => [dagNode.id, {
      id: dagNode.id,
      status: "pending",
      flow: "default",
      currentFlowIndex: 0,
      attempts: [],
      updatedAt: new Date().toISOString(),
    }])),
  };
}

function node(id) {
  return {
    id,
    title: `Node ${id}`,
    chunkFile: `.ai/chunks/${id}.md`,
    dependsOn: [],
    ownedFiles: [],
    forbiddenFiles: [],
  };
}

async function git(cwd, args) {
  const result = await execGit(cwd, args);
  if (result.code !== 0) throw new Error(`git ${args.join(" ")} failed in ${cwd}: ${result.stderr || result.stdout}`);
  return result;
}

function assertIncludes(text, expected, label) {
  assert(text.includes(expected), `${label}: expected ${JSON.stringify(expected)}`);
}

function assert(condition, message) {
  if (!condition) throw new Error(`Smoke failed: ${message}`);
}
