import { mkdtemp, rm, writeFile, readFile, access, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { renderDagDiagram } from "../extensions/dag-workflow/diagram.ts";
import { configToDagBase, mergeConfig } from "../extensions/dag-workflow/config.ts";
import { PACKAGE_DEFAULT_CONFIG } from "../extensions/dag-workflow/defaults.ts";
import { getNodeFlowName } from "../extensions/dag-workflow/dag.ts";
import { ensureNodeWorktree, execGit, isConventionalCommitSubject, mergeNode, refreshNodeWorktreeFromParent } from "../extensions/dag-workflow/worktrees.ts";

const files = [
  "package.json",
  "extensions/dag-workflow/index.ts",
  "extensions/dag-workflow/dag-subagent.ts",
  "extensions/dag-workflow/types.ts",
  "extensions/dag-workflow/dag.ts",
  "extensions/dag-workflow/diagram.ts",
  "extensions/dag-workflow/config.ts",
  "extensions/dag-workflow/grillme/editor.ts",
  "extensions/dag-workflow/command-prompts/archive.md",
  "extensions/dag-workflow/command-prompts/brainstorm.md",
  "extensions/dag-workflow/command-prompts/chunk.md",
  "extensions/dag-workflow/command-prompts/grillme.md",
  "extensions/dag-workflow/command-prompts/plan.md",
  "extensions/dag-workflow/command-prompts/retro.md",
  "extensions/dag-workflow/command-prompts/review.md",
  "extensions/dag-workflow/command-prompts/run.md",
  "extensions/dag-workflow/step-prompts/conflict-resolver.md",
  "extensions/dag-workflow/step-prompts/executor.md",
  "extensions/dag-workflow/step-prompts/reviewer.md",
  "extensions/dag-workflow/step-prompts/session-retrospector.md",
  "extensions/dag-workflow/step-prompts/setup.md",
  "extensions/dag-workflow/step-prompts/validator.md",
];

for (const file of files) await access(file);

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

const chunkPrompt = await readFile("extensions/dag-workflow/command-prompts/chunk.md", "utf8");
assertIncludes(chunkPrompt, "dag_diagram", "chunk prompt wires dag_diagram");
assertIncludes(chunkPrompt, "without adding a heading", "chunk prompt avoids diagram headings");
assertIncludes(chunkPrompt, "Do not wrap the diagram in Markdown code fences", "chunk prompt avoids diagram code fences");
assertIncludes(chunkPrompt, "nodeFlowOverrides", "chunk prompt preserves nodeFlowOverrides");

const validatorPrompt = await readFile("extensions/dag-workflow/step-prompts/validator.md", "utf8");
assertIncludes(validatorPrompt, "unit/static", "validator prompt classifies unit/static validation");
assertIncludes(validatorPrompt, "help smoke", "validator prompt classifies help smoke validation");
assertIncludes(validatorPrompt, "mocked behavioral", "validator prompt classifies mocked behavioral validation");
assertIncludes(validatorPrompt, "live external", "validator prompt classifies live external validation");
assertIncludes(validatorPrompt, "Do not perform live external validation unless", "validator prompt keeps live validation opt-in");
assertIncludes(validatorPrompt, "call that out explicitly", "validator prompt calls out skipped external workflows");

const reviewPrompt = await readFile("extensions/dag-workflow/command-prompts/review.md", "utf8");
assertIncludes(reviewPrompt, "unit/static", "review prompt classifies validation evidence");
assertIncludes(reviewPrompt, "external workflow that was not live-tested", "review prompt calls out external workflows not live-tested");

const readme = await readFile("README.md", "utf8");
assertIncludes(readme, "Dependency sketch:", "README documents diagram sample");
assertIncludes(readme, "/dag chunk", "README documents /dag chunk diagram output");
assertIncludes(readme, "directly in the terminal output", "README documents direct terminal output");
assertIncludes(readme, "~/.pi/agent/extensions/dag-workflow/config.json", "README documents user config path");
assertIncludes(readme, ".ai/dag.config.json", "README documents project config path");
assertIncludes(readme, "package defaults → user-global config → project config", "README documents config merge order");
assertIncludes(readme, "wci-ci-loop-validate", "README documents opt-in wci-ci-loop-validate pattern");
assertIncludes(readme, "External side-effect validation should be opt-in", "README keeps external validation opt-in");

console.log(`Smoke OK: ${files.length} required files exist and DAG reliability checks passed`);

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
