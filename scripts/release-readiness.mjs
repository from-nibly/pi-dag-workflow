import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { homedir, platform } from "node:os";
import { mkdir, mkdtemp, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { classifyReleaseImpact, fullReleaseImpact, releaseBaseAndChangedPaths } from "./release-impact.mjs";

const run = promisify(execFile);
const root = process.cwd();
const args = process.argv.slice(2);
for (let index = 0; index < args.length; index += 1) { const arg = args[index]; if (arg === "--base") { if (!args[++index]) throw new Error("--base requires a Git ref"); } else if (!["--allow-dirty", "--full", "--no-cache"].includes(arg)) throw new Error(`Unknown release-readiness argument: ${arg}`); }
const allowDirty = args.includes("--allow-dirty");
const forceFull = args.includes("--full");
const noCache = args.includes("--no-cache") || forceFull || allowDirty;
const baseIndex = args.indexOf("--base");
const explicitBase = baseIndex < 0 ? null : args[baseIndex + 1];
const releaseEnv = { ...process.env };
for (const key of Object.keys(releaseEnv)) if (key.startsWith("PI_DAG_WORKER_")) delete releaseEnv[key];

await preflightCleanTree();
await run("git", ["diff", "--check"], { cwd: root });
const packageJson = JSON.parse(await readFile("package.json", "utf8"));
const readme = await readFile("README.md", "utf8");
if (!readme.includes(`@v${packageJson.version}`)) throw new Error(`README install version does not match package ${packageJson.version}`);
const specs = await new ProjectModelDomain(root).specs({ action: "check" });
if (!specs.ok) throw new Error(`Generated specification drift: ${[...specs.driftPaths, ...specs.stalePaths].join(", ")}`);

let base = explicitBase; let changedPaths = [];
let impact;
if (forceFull) impact = fullReleaseImpact();
else {
  const selection = await releaseBaseAndChangedPaths(root, explicitBase); base = selection.base; changedPaths = selection.paths;
  if (allowDirty) changedPaths = [...new Set([...changedPaths, ...await workingTreePaths()])].sort();
  impact = classifyReleaseImpact(changedPaths);
}
process.stdout.write(`${JSON.stringify({ kind: "ReleaseImpactPlanV1", version: packageJson.version, base, changedPathCount: changedPaths.length, changedPaths, impact }, null, 2)}\n`);

for (const script of impact.focused) await command("npm", ["run", script], focusedTimeout(script));
if (impact.dogfoodGroups.length) {
  const dogfoodArgs = ["run", "test:dag-dogfood", "--", ...impact.dogfoodGroups.flatMap((group) => ["--group", group])];
  await cachedCommand("dag-dogfood", "npm", dogfoodArgs, 90 * 60_000, ["extensions/dag-workflow/dag-runtime", "scripts/dag-dogfood-test.mjs", "package.json"]);
}
if (impact.portfolioTemplates.length || impact.recoveryDrills.length) {
  const portfolioArgs = ["run", "test:dag-dogfood-portfolio", "--", ...impact.portfolioTemplates.flatMap((template) => ["--template", template]), ...impact.recoveryDrills.flatMap((drill) => ["--drill", drill])];
  await cachedCommand("dag-dogfood-portfolio", "npm", portfolioArgs, 180 * 60_000, ["extensions/dag-workflow/dag-runtime", "scripts/dag-dogfood-test.mjs", "scripts/dag-dogfood-portfolio.mjs", "scripts/fixtures/dag-evaluation-portfolio-v1.json", "package.json"]);
} else if (impact.portfolioIdentity) await command("npm", ["run", "test:dag-dogfood-portfolio", "--", "--portfolio-only"], 10 * 60_000);

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
  "scripts/release-impact.mjs",
  "scripts/release-impact-test.mjs",
  "spec/prototypes/brainstorm-pi-adapter/scenario.mjs",
  "spec/prototypes/lavish-turn-renderer/scenario.mjs",
]) if (!packed.files.some((file) => file.path === path)) throw new Error(`Packed artifact is missing ${path}`);

const packageStage = await mkdtemp(join(tmpdir(), "pi-dag-release-package-"));
try {
  const artifact = JSON.parse((await run("npm", ["pack", "--json", "--pack-destination", packageStage], { cwd: root, maxBuffer: 8 * 1024 * 1024 })).stdout)[0];
  await run("tar", ["-xzf", join(packageStage, artifact.filename), "-C", packageStage]);
  const extracted = join(packageStage, "package");
  await symlink(join(root, "node_modules"), join(extracted, "node_modules"));
  await command("npm", ["run", "smoke", "--", "--package"], 15 * 60_000, extracted);
} finally { await rm(packageStage, { recursive: true, force: true }); }

await preflightCleanTree();
console.log(`Release readiness OK for pi-dag-workflow ${packageJson.version} (${forceFull ? "full" : `impact-aware from ${base}`}${noCache ? ", cache bypassed" : ""}${allowDirty ? ", dirty-tree gate skipped" : ""})`);

async function cachedCommand(gateId, executable, commandArgs, timeout, trackedPaths) {
  if (noCache) return command(executable, commandArgs, timeout);
  const inputHash = await gateInputHash(gateId, executable, commandArgs, trackedPaths);
  const cacheRoot = process.env.XDG_CACHE_HOME ? join(process.env.XDG_CACHE_HOME, "pi-dag-workflow", "release-evidence-v1") : join(homedir(), ".cache", "pi-dag-workflow", "release-evidence-v1");
  const receiptPath = join(cacheRoot, `${gateId}-${inputHash}.json`);
  try {
    const receipt = JSON.parse(await readFile(receiptPath, "utf8"));
    const receiptCore = { schemaVersion: receipt.schemaVersion, kind: receipt.kind, gateId: receipt.gateId, inputHash: receipt.inputHash, status: receipt.status, command: receipt.command, completedAt: receipt.completedAt };
    const receiptValid = receipt.receiptHash === createHash("sha256").update(JSON.stringify(receiptCore)).digest("hex");
    if (receiptValid && receipt.schemaVersion === 1 && receipt.kind === "ReleaseGateReceiptV1" && receipt.gateId === gateId && receipt.inputHash === inputHash && receipt.status === "passed") { console.log(`release evidence reused: ${gateId} ${inputHash.slice(0, 12)}`); return; }
  } catch (error) { if (error?.code !== "ENOENT") throw error; }
  await command(executable, commandArgs, timeout);
  await mkdir(cacheRoot, { recursive: true });
  const receiptCore = { schemaVersion: 1, kind: "ReleaseGateReceiptV1", gateId, inputHash, status: "passed", command: [executable, ...commandArgs], completedAt: new Date().toISOString() };
  const receipt = { ...receiptCore, receiptHash: createHash("sha256").update(JSON.stringify(receiptCore)).digest("hex") };
  const temporary = `${receiptPath}.tmp-${process.pid}`; await writeFile(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 }); await rename(temporary, receiptPath);
}

async function gateInputHash(gateId, executable, commandArgs, trackedPaths) {
  const tree = (await run("git", ["ls-tree", "-r", "HEAD", "--", ...trackedPaths], { cwd: root, encoding: "utf8", maxBuffer: 8 * 1024 * 1024 })).stdout;
  const gitVersion = (await run("git", ["--version"], { encoding: "utf8" })).stdout.trim();
  const gitPath = (await run("sh", ["-c", "command -v git"], { encoding: "utf8" })).stdout.trim(); const shellPath = (await run("sh", ["-c", "command -v sh"], { encoding: "utf8" })).stdout.trim(); const truePath = "/usr/bin/true";
  const [nodeExecutableHash, gitExecutableHash, shellExecutableHash, trueExecutableHash] = await Promise.all([hashFile(process.execPath), hashFile(gitPath), hashFile(shellPath), hashFile(truePath)]);
  const kernel = (await run("uname", ["-srmo"], { encoding: "utf8" })).stdout.trim();
  return createHash("sha256").update(JSON.stringify({ gateId, executable, commandArgs, tree, node: process.version, execPath: process.execPath, nodeExecutableHash, gitVersion, gitPath, gitExecutableHash, shellPath, shellExecutableHash, truePath, trueExecutableHash, kernel, platform: platform(), arch: process.arch, locale: releaseEnv.LC_ALL ?? releaseEnv.LANG ?? null, timezone: releaseEnv.TZ ?? null })).digest("hex");
}

function hashFile(path) { return new Promise((resolveHash, reject) => { const hash = createHash("sha256"); const stream = createReadStream(path); stream.on("error", reject); stream.on("data", (chunk) => hash.update(chunk)); stream.on("end", () => resolveHash(hash.digest("hex"))); }); }

async function workingTreePaths() {
  const modified = (await run("git", ["diff", "--name-only", "HEAD"], { cwd: root, encoding: "utf8" })).stdout.split("\n").filter(Boolean);
  const untracked = (await run("git", ["ls-files", "--others", "--exclude-standard"], { cwd: root, encoding: "utf8" })).stdout.split("\n").filter(Boolean);
  return [...new Set([...modified, ...untracked])];
}

async function preflightCleanTree() {
  const status = (await run("git", ["status", "--porcelain=v1", "--untracked-files=all"], { cwd: root })).stdout.trim();
  if (status && !allowDirty) throw new Error("Release readiness requires a clean Git tree");
  if (status && allowDirty) process.stderr.write("warning: clean-tree release gate skipped by --allow-dirty\n");
}

function focusedTimeout(script) { return script === "test:workers" ? 15 * 60_000 : script === "test:git-integration" ? 10 * 60_000 : script === "test:dag-evaluation" ? 30 * 60_000 : 20 * 60_000; }
function command(executable, commandArgs, timeout, cwd = root) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, commandArgs, { cwd, stdio: "inherit", env: releaseEnv, timeout });
    child.once("error", reject);
    child.once("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`${executable} ${commandArgs.join(" ")} failed (${code ?? signal})`)));
  });
}
