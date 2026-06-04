import { access } from "node:fs/promises";

const files = [
  "package.json",
  "extensions/dag-workflow/index.ts",
  "extensions/dag-workflow/dag-subagent.ts",
  "extensions/dag-workflow/types.ts",
  "extensions/dag-workflow/dag.ts",
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
console.log(`Smoke OK: ${files.length} required files exist`);
