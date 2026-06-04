import { access, readFile } from "node:fs/promises";
import { renderDagDiagram } from "../extensions/dag-workflow/diagram.ts";
import { isConventionalCommitSubject } from "../extensions/dag-workflow/worktrees.ts";

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

const chunkPrompt = await readFile("extensions/dag-workflow/command-prompts/chunk.md", "utf8");
assertIncludes(chunkPrompt, "dag_diagram", "chunk prompt wires dag_diagram");
assertIncludes(chunkPrompt, "without adding a heading", "chunk prompt avoids diagram headings");
assertIncludes(chunkPrompt, "Do not wrap the diagram in Markdown code fences", "chunk prompt avoids diagram code fences");

const readme = await readFile("README.md", "utf8");
assertIncludes(readme, "Dependency sketch:", "README documents diagram sample");
assertIncludes(readme, "/dag chunk", "README documents /dag chunk diagram output");
assertIncludes(readme, "directly in the terminal output", "README documents direct terminal output");

console.log(`Smoke OK: ${files.length} required files exist and DAG diagram checks passed`);

function assertIncludes(text, expected, label) {
  assert(text.includes(expected), `${label}: expected ${JSON.stringify(expected)}`);
}

function assert(condition, message) {
  if (!condition) throw new Error(`Smoke failed: ${message}`);
}
