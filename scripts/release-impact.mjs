import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";

const run = promisify(execFile);

export const DOGFOOD_GROUPS = ["baseline", "lifecycle", "composition", "validation", "landing", "cleanup"];
export const PORTFOLIO_TEMPLATES = ["fanout-alpha", "fanout-beta", "constraint-contract", "constraint-architecture", "integration-train", "recovery-sensitive"];
export const RECOVERY_DRILLS = ["provider_worker_loss", "conductor_crash_resume", "target_drift_conflict"];

const focusedOrder = [
  "test:release-impact", "test:model", "test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start",
  "test:dag-runtime", "test:dag-widget", "test:dag-evaluation", "test:git-integration", "test:workers",
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
  if (/^(extensions\/dag-workflow\/worker-runtime\/|extensions\/dag-workflow\/(workers|subagents)\.ts|scripts\/(worker-runtime-test|fixtures\/(fake-worker-rpc|worker-manager-crash-child|worker-store-child))\.mjs|spec\/owned-worker-runtime\/)/.test(path)) return add("owned worker runtime", { focused: ["test:workers"] });
  if (/^(extensions\/dag-workflow\/planning\/|scripts\/dag-planning(-runtime|-command)?-test\.mjs|scripts\/dag-prepared-start-test\.mjs|scripts\/fixtures\/dag-chunk-diagram\/|extensions\/dag-workflow\/command-prompts\/plan\.md)/.test(path)) return add("planning and prepared-start surfaces", { focused: ["test:dag-planning", "test:dag-planning-runtime", "test:dag-planning-command", "test:dag-prepared-start"] });
  if (/^(extensions\/dag-workflow\/dag-runtime\/widget(-controller)?\.ts|scripts\/dag-widget-test\.mjs|spec\/prototypes\/dag-widget-activity-lanes\/)/.test(path)) return add("DAG widget", { focused: ["test:dag-widget"] });
  if (/^(README\.md|LICENSE|\.gitignore|spec\/prototypes\/|extensions\/dag-workflow\/(command-prompts|step-prompts)\/)/.test(path)) return add("documentation or retained projection; packed smoke only");
  if (/^(extensions\/dag-workflow\/dag-runtime\/evaluation(-store)?\.ts|scripts\/(dag-evaluation-test|fixtures\/dag-evaluation-portfolio-v1\.json)\.mjs?|scripts\/fixtures\/dag-evaluation-portfolio-v1\.json)/.test(path)) return add("evaluation schema or fold", { focused: ["test:dag-evaluation"], portfolioIdentity: true });
  if (/^extensions\/dag-workflow\/dag-runtime\/(git-integration|integration-driver|integration)\.ts$/.test(path) || /^scripts\/(git-integration-test|fixtures\/git-integration-crash-child)\.mjs$/.test(path)) return add("Git/integration transaction", { focused: ["test:git-integration", "test:dag-runtime"], dogfood: ["baseline", "composition", "validation", "landing"], templates: ["integration-train"], drills: ["target_drift_conflict"] });
  if (/^extensions\/dag-workflow\/dag-runtime\/(conductor|lifecycle-runtime)\.ts$/.test(path)) return add("conductor or lifecycle recovery", { focused: ["test:dag-runtime", "test:dag-prepared-start", "test:dag-planning-command"], dogfood: ["lifecycle", "landing", "cleanup"], templates: ["recovery-sensitive"], drills: ["conductor_crash_resume"] });
  if (/^extensions\/dag-workflow\/dag-runtime\/(common|plan|run-state|reducer|store|scheduler)\.ts$/.test(path) || /^scripts\/(dag-runtime-test|fixtures\/dag-store-child)\.mjs$/.test(path)) { plan.full = true; return add("broad canonical runtime primitive; full gate required"); }
  if (/^scripts\/dag-dogfood-test\.mjs$/.test(path)) return add("canonical dogfood harness", { dogfood: DOGFOOD_GROUPS });
  if (/^(scripts\/dag-dogfood-portfolio\.mjs|scripts\/fixtures\/dag-dogfood-portfolio-evidence-v1\.json)$/.test(path)) return add("portfolio harness or evidence", { templates: PORTFOLIO_TEMPLATES, drills: RECOVERY_DRILLS, portfolioIdentity: true });
  if (/^(extensions\/dag-workflow\/(index|dag|diagram|worktrees|config|defaults|package-paths|sessions|types)\.ts|extensions\/dag-workflow\/dag-runtime\/index\.ts)$/.test(path) || /^scripts\/smoke-test\.mjs$/.test(path)) return add("top-level package integration; focused coverage is selected by packed smoke");
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
