import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { migrateLegacyBrainstorm } from "../extensions/dag-workflow/project-model/migration.ts";
import { SpecProjector } from "../extensions/dag-workflow/project-model/projector.ts";
import { ProjectModelStore } from "../extensions/dag-workflow/project-model/store.ts";

const root = process.cwd();
const force = process.argv.includes("--force");
const source = resolve(root, ".ai/brainstorm/structured-brainstorming.json");
const reportPath = resolve(root, "project-model/migrations/brainstorm-v2-candidate.md");
const previewDirectory = resolve(root, ".ai/model-migration/candidate");
const store = new ProjectModelStore(root);

let previousModelRaw;
if (await store.exists()) {
  previousModelRaw = await readFile(store.path, "utf8");
  const previousModel = JSON.parse(previousModelRaw);
  if (previousModel?.project?.mode === "authoritative") throw new Error("Refusing to replace an authoritative project model from the legacy brainstorm snapshot");
  if (!force) throw new Error("project-model/model.json already exists; pass --force only to regenerate the non-authoritative candidate");
}

const legacy = JSON.parse(await readFile(source, "utf8"));
const result = await migrateLegacyBrainstorm(root, legacy);

// Complete all fallible derivation before replacing either durable candidate artifact.
await rm(previewDirectory, { recursive: true, force: true });
const preview = await new SpecProjector(root).preview(result.model, ".ai/model-migration/candidate");
let previousReport;
try { previousReport = await readFile(reportPath, "utf8"); } catch (error) { if (error?.code !== "ENOENT") throw error; }

try {
  await store.write(result.model);
  await atomicWrite(reportPath, result.report);
} catch (error) {
  if (previousModelRaw !== undefined) await atomicWrite(store.path, previousModelRaw);
  else await rm(store.path, { force: true });
  if (previousReport !== undefined) await atomicWrite(reportPath, previousReport);
  else await rm(reportPath, { force: true });
  throw error;
}

console.log(JSON.stringify({
  modelPath: "project-model/model.json",
  reportPath: "project-model/migrations/brainstorm-v2-candidate.md",
  previewDirectory: preview.directory.slice(`${root}/`.length),
  generatedSpecs: preview.rendered.map(({ path }) => path),
  mapped: result.mappings.filter(({ disposition }) => disposition === "mapped").length,
  omitted: result.mappings.filter(({ disposition }) => disposition === "omitted").length,
  warnings: result.warnings,
}, null, 2));

async function atomicWrite(path, content) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, content, "utf8");
  await rename(temporary, path);
}
