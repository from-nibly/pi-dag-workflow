import { access } from "node:fs/promises";

const files = [
  "package.json",
  "extensions/dag-workflow/index.ts",
  "extensions/dag-workflow/types.ts",
  "extensions/dag-workflow/dag.ts",
  "extensions/dag-workflow/config.ts",
  "extensions/dag-workflow/grillme/editor.ts",
];

for (const file of files) await access(file);
console.log(`Smoke OK: ${files.length} required files exist`);
