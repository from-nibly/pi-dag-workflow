import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { readdir } from "node:fs/promises";
import { join } from "node:path";

const run = promisify(execFile);

export const DOGFOOD_GROUPS = ["baseline", "lifecycle", "composition", "validation", "landing", "cleanup"];
export const PORTFOLIO_TEMPLATES = ["fanout-alpha", "fanout-beta", "constraint-contract", "constraint-architecture", "integration-train", "recovery-sensitive"];
export const RECOVERY_DRILLS = ["provider_worker_loss", "conductor_crash_resume", "target_drift_conflict"];

export const V2_FOCUSED_SUITES = [
  "test:dag-v2-state", "test:dag-v2-lifecycle", "test:dag-v2-context", "test:dag-v2-historical-evaluation", "test:dag-v2-git-acceptance",
  "test:dag-v2-git-attributes", "test:dag-v2-git-hooks", "test:dag-v2-workspace", "test:dag-v2-product",
];
export const RELEASE_SUITE_TIMEOUT_MS = 3_600_000;
export const RELEASE_AGGREGATE_BUDGET_SECONDS = 28_800;
// Broad cache closure includes product wiring, V2/native/Python, planning, workers,
// fixtures and gate policy. Full/dirty runs still bypass every cached receipt.
export const RELEASE_CACHE_INPUTS = ["extensions/dag-workflow", "scripts", "project-model", "spec", "package.json"];
export const RELEASE_CACHE_POLICY = "release-input-policy-v2";
// Joined follow-up must implement this bounded production registration/save/show/run
// test in a disposable repository. Never substitute the full product matrix here.
export const PRODUCT_PACKAGE_SMOKE_PATH = "scripts/dag-v2-package-smoke.mjs";
export const V2_REQUIRED_PACKAGE_FILES = [
  "extensions/dag-workflow/planning/v2.ts",
  ...["index.ts", "state.ts", "store.ts", "service.ts", "lifecycle.ts", "lifecycle-schema.ts", "command-runner.ts",
    "command-supervisor.py", "git-driver.ts", "git-lock.ts", "git-native.ts", "git-schema.ts", "git-state.ts", "reference-transaction.py"]
    .map(name => `extensions/dag-workflow/runtime-v2/${name}`),
  ...V2_FOCUSED_SUITES.filter(script => script !== "test:dag-v2-product").map(script => `scripts/${script.slice(5)}-test.mjs`),
  "scripts/fixtures/dag-v2-lifecycle.mjs", "scripts/fixtures/command-fork-handoff.py", "scripts/fixtures/command-owner-subreaper.py",
];

// Detect newly added source/helpers as well as the fixed mandatory inventory.
// In readiness this runs on source BEFORE extraction, so omitted packed files fail.
export async function runtimePackageSources(root) {
  const sources = [];
  async function visit(relative) {
    for (const entry of await readdir(join(root, relative), { withFileTypes: true })) {
      const path = `${relative}/${entry.name}`;
      if (entry.isDirectory()) await visit(path);
      else if (/\.(ts|mjs|py)$/.test(entry.name)) sources.push(path);
    }
  }
  await visit("extensions/dag-workflow");
  return sources.sort();
}

const focusedOrder = [
  "test:release-impact", "test:model", "test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start",
  "test:dag-runtime", "test:dag-widget", "test:dag-evaluation", "test:git-integration", "test:workers", ...V2_FOCUSED_SUITES,
];

export function fullReleaseImpact(reason = "explicit full release gate") {
  return { full: true, reasons: [reason], focused: [...focusedOrder], dogfoodGroups: [...DOGFOOD_GROUPS], portfolioTemplates: [...PORTFOLIO_TEMPLATES], recoveryDrills: [...RECOVERY_DRILLS], portfolioIdentity: true };
}

export function classifyReleaseImpact(paths) {
  const plan = { full: false, reasons: [], focused: [], dogfoodGroups: [], portfolioTemplates: [], recoveryDrills: [], portfolioIdentity: false };
  for (const path of [...new Set(paths)].sort()) classifyOne(path, plan);
  return normalize(plan);
}

export async function releaseBaseAndChangedPaths(cwd = process.cwd(), explicitBase = null) {
  const base = explicitBase ?? process.env.PI_RELEASE_BASE ?? await latestPriorReleaseTag(cwd);
  await run("git", ["rev-parse", "--verify", `${base}^{commit}`], { cwd });
  const { stdout } = await run("git", ["diff", "--name-status", "--find-renames", "-z", `${base}...HEAD`], { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024 });
  return { base, paths: parseNameStatus(stdout) };
}

async function latestPriorReleaseTag(cwd) {
  const head = (await run("git", ["rev-parse", "HEAD"], { cwd, encoding: "utf8" })).stdout.trim();
  const tags = (await run("git", ["tag", "--merged", "HEAD", "--sort=-version:refname"], { cwd, encoding: "utf8" })).stdout.split("\n").filter((tag) => /^v\d+\.\d+\.\d+$/.test(tag));
  for (const tag of tags) if ((await run("git", ["rev-parse", `${tag}^{commit}`], { cwd, encoding: "utf8" })).stdout.trim() !== head) return tag;
  throw new Error("No prior semantic release tag is reachable from HEAD; pass --base <ref> or PI_RELEASE_BASE");
}

function parseNameStatus(value) {
  const fields = value.split("\0"); const paths = [];
  for (let index = 0; index < fields.length && fields[index];) {
    const status = fields[index++];
    if (/^[RC]/.test(status)) { paths.push(fields[index++], fields[index++]); }
    else paths.push(fields[index++]);
  }
  return [...new Set(paths.filter(Boolean))].sort();
}

function classifyOne(path, plan) {
  const add = (reason, { focused = [], dogfood = [], templates = [], drills = [], portfolioIdentity = false } = {}) => {
    plan.reasons.push(`${path}: ${reason}`); plan.focused.push(...focused); plan.dogfoodGroups.push(...dogfood); plan.portfolioTemplates.push(...templates); plan.recoveryDrills.push(...drills); plan.portfolioIdentity ||= portfolioIdentity;
  };
  if (/^(package\.json|scripts\/release-(readiness|impact)(-test)?\.mjs)$/.test(path)) return add("release/package policy; packed smoke and release-impact tests", { focused: ["test:release-impact"] });
  if (/^(project-model\/(model\.json|migrations\/)|extensions\/dag-workflow\/project-model\/|scripts\/(project-model-test|migrate-brainstorm-to-project-model)\.mjs|spec\/(mixed-initiative-project-model|model-aware-dag-runtime|structured-brainstorming)\/|spec\/spec\.md)/.test(path)) return add("project-model semantics", { focused: ["test:model"] });
  if (/^extensions\/dag-workflow\/runtime-v2\//.test(path)) { plan.full = true; return add("V2 runtime/product/native primitive; full gate including V2 required"); }
  if (/^scripts\/(dag-v2-.*-test|dag-v2-package-smoke|fixtures\/dag-v2-.*)\.mjs$/.test(path) || /^scripts\/fixtures\/command-.*\.py$/.test(path)) {
    if (path === "scripts/dag-v2-git-test.mjs") { plan.full = true; return add("unsafe Git characterization changed; require actual V2 acceptance (characterization is not certification)"); }
    return add("V2 suite, product smoke or execution fixture", { focused: V2_FOCUSED_SUITES });
  }
  if (/^(extensions\/dag-workflow\/worker-runtime\/|extensions\/dag-workflow\/(workers|subagents)\.ts|scripts\/(worker-runtime-test|fixtures\/(fake-worker-rpc|worker-manager-crash-child|worker-store-child))\.mjs|spec\/owned-worker-runtime\/)/.test(path)) return add("owned worker runtime and V2 product boundary", { focused: ["test:workers", ...V2_FOCUSED_SUITES] });
  if (/^(extensions\/dag-workflow\/planning\/|scripts\/dag-planning(-runtime|-command)?-test\.mjs|scripts\/dag-prepared-start-test\.mjs|scripts\/fixtures\/dag-chunk-diagram\/|extensions\/dag-workflow\/command-prompts\/plan\.md)/.test(path)) return add("planning, prepared-start and V2 product surfaces", { focused: ["test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start", ...V2_FOCUSED_SUITES] });
  if (/^(extensions\/dag-workflow\/dag-runtime\/widget(-controller)?\.ts|scripts\/dag-widget-test\.mjs|spec\/prototypes\/dag-widget-activity-lanes\/)/.test(path)) return add("DAG widget and V2 product projection", { focused: ["test:dag-widget", "test:dag-v2-product"] });
  if (/^(README\.md|LICENSE|\.gitignore|spec\/prototypes\/|extensions\/dag-workflow\/(command-prompts|step-prompts)\/)/.test(path)) return add("documentation or retained projection; packed smoke only");
  if (/^(extensions\/dag-workflow\/dag-runtime\/evaluation(-store)?\.ts|scripts\/(dag-evaluation-test|fixtures\/dag-evaluation-portfolio-v1\.json)\.mjs?|scripts\/fixtures\/dag-evaluation-portfolio-v1\.json)/.test(path)) return add("evaluation schema or fold", { focused: ["test:dag-evaluation", "test:dag-v2-historical-evaluation"], portfolioIdentity: true });
  if (/^extensions\/dag-workflow\/dag-runtime\/(git-integration|integration-driver|integration)\.ts$/.test(path) || /^scripts\/(git-integration-test|fixtures\/git-integration-crash-child)\.mjs$/.test(path)) return add("Git/integration transaction", { focused: ["test:git-integration", "test:dag-runtime"], dogfood: ["baseline", "composition", "validation", "landing"], templates: ["integration-train"], drills: ["target_drift_conflict"] });
  if (/^extensions\/dag-workflow\/dag-runtime\/(conductor|lifecycle-runtime)\.ts$/.test(path)) return add("conductor or lifecycle recovery", { focused: ["test:dag-runtime", "test:dag-prepared-start", "test:dag-planning-command"], dogfood: ["lifecycle", "landing", "cleanup"], templates: ["recovery-sensitive"], drills: ["conductor_crash_resume"] });
  if (/^extensions\/dag-workflow\/dag-runtime\/(common|plan|run-state|reducer|store|scheduler)\.ts$/.test(path) || /^scripts\/(dag-runtime-test|fixtures\/dag-store-child)\.mjs$/.test(path)) { plan.full = true; return add("broad canonical runtime primitive; full gate required"); }
  if (/^scripts\/dag-dogfood-test\.mjs$/.test(path)) return add("canonical dogfood harness", { dogfood: DOGFOOD_GROUPS });
  if (/^(scripts\/dag-dogfood-portfolio\.mjs|scripts\/fixtures\/dag-dogfood-portfolio-evidence-v1\.json)$/.test(path)) return add("portfolio harness or evidence", { templates: PORTFOLIO_TEMPLATES, drills: RECOVERY_DRILLS, portfolioIdentity: true });
  if (/^(extensions\/dag-workflow\/(index|dag|diagram|worktrees|config|defaults|package-paths|sessions|types)\.ts|extensions\/dag-workflow\/dag-runtime\/index\.ts)$/.test(path) || /^scripts\/smoke-test\.mjs$/.test(path)) return add("top-level package wiring; explicit focused coverage (packed smoke is bounded)", { focused: ["test:dag-planning-command", "test:dag-widget", "test:workers", ...V2_FOCUSED_SUITES] });
  plan.full = true; add("unclassified path; fail closed to full gate");
}

function normalize(plan) {
  if (plan.full) return { ...fullReleaseImpact(plan.reasons.join("; ") || "full impact"), reasons: [...new Set(plan.reasons)] };
  const order = (values, canonical) => canonical.filter((value) => new Set(values).has(value));
  return {
    full: false,
    reasons: [...new Set(plan.reasons)].sort(),
    focused: order(plan.focused, focusedOrder),
    dogfoodGroups: order(plan.dogfoodGroups, DOGFOOD_GROUPS),
    portfolioTemplates: order(plan.portfolioTemplates, PORTFOLIO_TEMPLATES),
    recoveryDrills: order(plan.recoveryDrills, RECOVERY_DRILLS),
    portfolioIdentity: Boolean(plan.portfolioIdentity),
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2); const baseIndex = args.indexOf("--base"); const explicitBase = baseIndex < 0 ? null : args[baseIndex + 1];
  if (baseIndex >= 0 && !explicitBase) throw new Error("--base requires a Git ref");
  if (args.includes("--full")) process.stdout.write(`${JSON.stringify(fullReleaseImpact(), null, 2)}\n`);
  else { const selection = await releaseBaseAndChangedPaths(process.cwd(), explicitBase); process.stdout.write(`${JSON.stringify({ ...selection, impact: classifyReleaseImpact(selection.paths) }, null, 2)}\n`); }
}
