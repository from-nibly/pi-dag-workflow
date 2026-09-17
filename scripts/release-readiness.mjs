import { createHash } from "node:crypto";
import { execFile, spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { homedir, platform } from "node:os";
import { mkdir, mkdtemp, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { classifyReleaseImpact, fullReleaseImpact, releaseBaseAndChangedPaths, RELEASE_SUITE_TIMEOUT_MS, RELEASE_AGGREGATE_BUDGET_SECONDS, RELEASE_CACHE_INPUTS, RELEASE_CACHE_POLICY, V2_REQUIRED_PACKAGE_FILES, PRODUCT_PACKAGE_SMOKE_PATH, runtimePackageSources } from "./release-impact.mjs";

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
for (const key of Object.keys(releaseEnv)) if (key.startsWith("PI_DAG_WORKER_") || key.startsWith("DAG_V2_PRODUCT_")) delete releaseEnv[key];

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
const candidateHead = (await run("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" })).stdout.trim();
process.stdout.write(`${JSON.stringify({ kind: "ReleaseImpactPlanV1", version: packageJson.version, candidateHead, base, changedPathCount: changedPaths.length, changedPaths, impact, minimumOuterBudgetSeconds: RELEASE_AGGREGATE_BUDGET_SECONDS }, null, 2)}\n`);
console.log(`Allow at least ${RELEASE_AGGREGATE_BUDGET_SECONDS}s for the aggregate gate; increase for measured serial-suite duration. Legacy dogfood/portfolio are compatibility evidence, not V2 product acceptance.`);

for (const script of impact.focused) await command("npm", ["run", script], RELEASE_SUITE_TIMEOUT_MS);
for (const group of impact.dogfoodGroups) await cachedCommand(`dag-dogfood-group-${group}`, "npm", ["run", "test:dag-dogfood", "--", "--group", group], RELEASE_SUITE_TIMEOUT_MS, RELEASE_CACHE_INPUTS);
if (impact.portfolioIdentity) await cachedCommand("dag-dogfood-portfolio-identity", "npm", ["run", "test:dag-dogfood-portfolio", "--", "--portfolio-only"], RELEASE_SUITE_TIMEOUT_MS, RELEASE_CACHE_INPUTS);
for (const template of impact.portfolioTemplates) await cachedCommand(`dag-dogfood-portfolio-template-${template}`, "npm", ["run", "test:dag-dogfood-portfolio", "--", "--template", template], RELEASE_SUITE_TIMEOUT_MS, RELEASE_CACHE_INPUTS);
for (const drill of impact.recoveryDrills) await cachedCommand(`dag-dogfood-portfolio-drill-${drill}`, "npm", ["run", "test:dag-dogfood-portfolio", "--", "--drill", drill], RELEASE_SUITE_TIMEOUT_MS, RELEASE_CACHE_INPUTS);

const packed = JSON.parse((await run("npm", ["pack", "--dry-run", "--json"], { cwd: root, maxBuffer: 8 * 1024 * 1024 })).stdout)[0];
if (packed.version !== packageJson.version) throw new Error("npm pack version does not match package.json");
for (const path of [
  ...V2_REQUIRED_PACKAGE_FILES, ...await runtimePackageSources(root),
  "scripts/dag-v2-product-test.mjs", PRODUCT_PACKAGE_SMOKE_PATH,
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
  await command("npm", ["run", "smoke", "--", "--package"], RELEASE_SUITE_TIMEOUT_MS, extracted);
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
  const pythonPath = (await run("python3", ["-I", "-B", "-c", "import os,sys; print(os.path.realpath(sys.executable))"], { encoding: "utf8" })).stdout.trim();
  const [nodeExecutableHash, gitExecutableHash, shellExecutableHash, trueExecutableHash, pythonExecutableHash] = await Promise.all([hashFile(process.execPath), hashFile(gitPath), hashFile(shellPath), hashFile(truePath), hashFile(pythonPath)]);
  const kernel = (await run("uname", ["-srmo"], { encoding: "utf8" })).stdout.trim();
  return createHash("sha256").update(JSON.stringify({ policy: RELEASE_CACHE_POLICY, gateId, executable, commandArgs, tree, node: process.version, execPath: process.execPath, nodeExecutableHash, gitVersion, gitPath, gitExecutableHash, shellPath, shellExecutableHash, truePath, trueExecutableHash, pythonPath, pythonExecutableHash, kernel, platform: platform(), arch: process.arch, locale: releaseEnv.LC_ALL ?? releaseEnv.LANG ?? null, timezone: releaseEnv.TZ ?? null })).digest("hex");
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

function command(executable, commandArgs, timeout, cwd = root) {
  return new Promise((resolve, reject) => {
    const startedAt = new Date().toISOString(), started = performance.now();
    console.log(JSON.stringify({ kind: "ReleaseCommandStart", candidateHead, argv: [executable, ...commandArgs], cwd, timeoutMs: timeout, startedAt }));
    const child = spawn(executable, commandArgs, { cwd, stdio: "inherit", env: releaseEnv, timeout });
    let spawnError;
    child.once("error", error => { spawnError = error; });
    child.once("close", (code, signal) => {
      console.log(JSON.stringify({ kind: "ReleaseCommandResult", candidateHead, argv: [executable, ...commandArgs], cwd, startedAt, elapsedMs: Math.round(performance.now() - started), code, signal, error: spawnError?.message ?? null }));
      if (!spawnError && code === 0 && signal === null) resolve();
      else reject(spawnError ?? new Error(`${executable} ${commandArgs.join(" ")} failed (${code ?? signal})`));
    });
  });
}
