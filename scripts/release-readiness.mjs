import { execFile, spawn } from "node:child_process";
import { mkdtemp, readFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";

const run = promisify(execFile);
const root = process.cwd();
const allowDirty = process.argv.includes("--allow-dirty");
const releaseEnv = { ...process.env };
for (const key of Object.keys(releaseEnv)) if (key.startsWith("PI_DAG_WORKER_")) delete releaseEnv[key];

await command("npm", ["run", "smoke"], 30 * 60_000);
await command("npm", ["run", "test:dag-evaluation"], 30 * 60_000);
await command("npm", ["run", "test:dag-dogfood"], 90 * 60_000);
await command("npm", ["run", "test:dag-dogfood-portfolio"], 180 * 60_000);

const specs = await new ProjectModelDomain(root).specs({ action: "check" });
if (!specs.ok) throw new Error(`Generated specification drift: ${[...specs.driftPaths, ...specs.stalePaths].join(", ")}`);

await run("git", ["diff", "--check"], { cwd: root });
const packageJson = JSON.parse(await readFile("package.json", "utf8"));
const readme = await readFile("README.md", "utf8");
if (!readme.includes(`@v${packageJson.version}`)) throw new Error(`README install version does not match package ${packageJson.version}`);

const packed = JSON.parse((await run("npm", ["pack", "--dry-run", "--json"], { cwd: root, maxBuffer: 8 * 1024 * 1024 })).stdout)[0];
if (packed.version !== packageJson.version) throw new Error("npm pack version does not match package.json");
for (const path of [
  "extensions/dag-workflow/planning/integration.ts",
  "extensions/dag-workflow/project-model/migration-workflow.ts",
  "extensions/dag-workflow/planning/runtime-adapter.ts",
  "extensions/dag-workflow/command-prompts/plan.md",
  "project-model/model.json",
  "project-model/migrations/brainstorm-v2-overrides.json",
  "spec/model-aware-dag-runtime/spec.md",
  "spec/prototypes/brainstorm-pi-adapter/scenario.mjs",
  "spec/prototypes/lavish-turn-renderer/scenario.mjs",
]) {
  if (!packed.files.some((file) => file.path === path)) throw new Error(`Packed artifact is missing ${path}`);
}

const packageStage = await mkdtemp(join(tmpdir(), "pi-dag-release-package-"));
try {
  const artifact = JSON.parse((await run("npm", ["pack", "--json", "--pack-destination", packageStage], { cwd: root, maxBuffer: 8 * 1024 * 1024 })).stdout)[0];
  await run("tar", ["-xzf", join(packageStage, artifact.filename), "-C", packageStage]);
  const extracted = join(packageStage, "package");
  await symlink(join(root, "node_modules"), join(extracted, "node_modules"));
  await command("npm", ["run", "smoke"], 30 * 60_000, extracted);
} finally { await rm(packageStage, { recursive: true, force: true }); }

const status = (await run("git", ["status", "--porcelain=v1", "--untracked-files=all"], { cwd: root })).stdout.trim();
if (status && !allowDirty) throw new Error("Release readiness requires a clean Git tree");
if (status && allowDirty) process.stderr.write("warning: clean-tree release gate skipped by --allow-dirty\n");

console.log(`Release readiness OK for pi-dag-workflow ${packageJson.version}${allowDirty ? " (dirty-tree gate skipped)" : ""}`);

function command(executable, args, timeout, cwd = root) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { cwd, stdio: "inherit", env: releaseEnv, timeout });
    child.once("error", reject);
    child.once("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`${executable} ${args.join(" ")} failed (${code ?? signal})`)));
  });
}
