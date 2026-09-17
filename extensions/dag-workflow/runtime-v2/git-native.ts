import { execFileSync, spawnSync } from "node:child_process";
import { lstat, realpath, readFile, readdir, open, mkdir, copyFile, chmod } from "node:fs/promises";
import { lstatSync, readFileSync, readdirSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { requireV2, sameV2 } from "../planning/v2.ts";
import { gitEnvironmentV2 } from "./command-runner.ts";
import type { CandidateV2 } from "./lifecycle-schema.ts";
import type { GitBindingV2, GitOperationV2 } from "./git-schema.ts";

// Base profile only: configured hooks also require per-invocation discovery via
// safeGitOptionsV2 or runArgvV2's disableGitHooks environment (including landing).
export const gitOptionsV2 = ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "core.attributesFile=/dev/null",
  "-c", "core.autocrlf=false", "-c", "core.safecrlf=false", "-c", "core.excludesFile=/dev/null", "-c", "maintenance.auto=false",
  "-c", "gc.auto=0", "-c", "rerere.enabled=false", "-c", "merge.renormalize=false", "-c", "merge.renames=false",
  "-c", "merge.directoryRenames=false", "-c", "merge.default=text", "-c", "merge.autoStash=false", "-c", "commit.gpgSign=false",
  "-c", "i18n.commitEncoding=UTF-8", "-c", "i18n.logOutputEncoding=UTF-8", "-c", "core.fsync=committed"];
export function nativeGitV2(root: string, ...args: string[]): string {
  try { return execFileSync("git", [...safeGitOptionsV2(root), ...args], { cwd: root, env: gitEnvironmentV2(), encoding: "utf8", timeout: 60000, maxBuffer: 16 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] }).trim(); }
  catch (e: any) { throw Error(`GIT_FAILED ${args[0]}: ${String(e.stderr ?? e.message).slice(-8000)}`); }
}
/** Config hooks bypass core.hooksPath in Git 2.54. Discover names afresh,
 * including dormant conditional includes: worktree-add's Git children can have
 * a different gitdir/onbranch context. Disable names, not whole config files,
 * so safe config, disabled hooks and unused events remain supported. */
export function configuredGitHooksV2(root: string, options: readonly string[] = gitOptionsV2): string[] {
  const config = (...args: string[]) => {
    const bytes = execFileSync("git", [...options, "config", ...args], {
      cwd: root, env: gitEnvironmentV2(), timeout: 60000, maxBuffer: 16 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"],
    });
    const text = bytes.toString("utf8");
    requireV2(Buffer.from(text, "utf8").equals(bytes), "GIT_CONFIG_ENCODING_UNSUPPORTED");
    return text;
  };
  const names = new Set<string>(), files = new Set<string>(), includes = new Set<string>();
  const scan = (text: string) => {
    const entries = text.split("\0");
    for (let i = 0; i + 1 < entries.length; i += 2) {
      const origin = entries[i], entry = entries[i + 1], split = entry.indexOf("\n");
      const key = split < 0 ? entry : entry.slice(0, split);
      const hook = /^hook\.([\s\S]*)\.(command|event|enabled)$/.exec(key);
      if (hook) names.add(hook[1]);
      if (key !== "include.path" && !/^includeif\.[\s\S]*\.path$/.test(key)) continue;
      const source = origin.startsWith("file:") ? resolve(root, origin.slice(5)) : undefined;
      const id = JSON.stringify([source, key]); if (includes.has(id)) continue; includes.add(id);
      // Git parses/expands quoted, tilde and %(prefix) paths. Relative include
      // paths are relative to the including file, not the worktree cwd.
      const paths = config(...(source ? ["--file", source, "--no-includes"] : []), "--path", "--null", "--get-all", key);
      for (const path of paths.split("\0").filter(Boolean)) {
        const file = resolve(source ? dirname(source) : root, path);
        if (files.has(file)) continue; files.add(file);
        try { lstatSync(file); } catch (e: any) { if (e.code === "ENOENT") continue; throw e; }
        scan(config("--file", file, "--no-includes", "--show-origin", "--null", "--list"));
      }
    }
  };
  scan(config("--includes", "--show-origin", "--null", "--list"));
  return [...names];
}
export function safeGitOptionsV2(root: string, options: readonly string[] = gitOptionsV2): string[] {
  // --config-env splits at the last '=', so legal subsection names containing
  // '=' cannot be mistaken for the value as they would be with '-c key=value'.
  return [...options, ...configuredGitHooksV2(root, options).map(name => `--config-env=hook.${name}.enabled=PI_DAG_V2_GIT_HOOK_ENABLED`)];
}
async function exists(path: string): Promise<boolean> { try { await lstat(path); return true; } catch (e: any) { if (e.code === "ENOENT") return false; throw e; } }
async function identity(path: string) {
  path = resolve(path); requireV2(await realpath(path) === path, "GIT_SYMLINK_BINDING_UNSUPPORTED");
  const s = await lstat(path, { bigint: true }); requireV2(s.isDirectory(), "GIT_DIRECTORY_REQUIRED");
  return { path, dev: s.dev.toString(), ino: s.ino.toString() };
}
export async function bindGitV2(root: string): Promise<GitBindingV2> {
  const binding = { root: await identity(root), common: await identity(nativeGitV2(root, "rev-parse", "--path-format=absolute", "--git-common-dir")),
    admin: await identity(nativeGitV2(root, "rev-parse", "--absolute-git-dir")), objectFormat: nativeGitV2(root, "rev-parse", "--show-object-format"),
    refBackend: nativeGitV2(root, "rev-parse", "--show-ref-format"), gitVersion: nativeGitV2(root, "--version") };
  requireV2(nativeGitV2(root, "rev-parse", "--show-toplevel") === binding.root.path, "BOUND_ROOT_REQUIRED");
  // This deliberately narrow version profile is tested, not a guessed fallback.
  requireV2(binding.gitVersion === "git version 2.54.0" && binding.refBackend === "files" && ["sha1", "sha256"].includes(binding.objectFormat), "UNSUPPORTED_GIT_CAPABILITY: requires Git 2.54.0 files backend sha1/sha256");
  return binding as GitBindingV2;
}
export async function verifyBindingV2(binding: GitBindingV2): Promise<void> {
  requireV2(sameV2(await bindGitV2(binding.root.path), binding), "GIT_NATIVE_IDENTITY_DRIFT");
}
export function inspectGitCandidateV2(root: string, candidate: CandidateV2): void {
  requireV2(nativeGitV2(root, "rev-parse", "--verify", `${candidate.commit}^{commit}`) === candidate.commit
    && nativeGitV2(root, "rev-parse", `${candidate.commit}^{tree}`) === candidate.tree, "NATIVE_CANDIDATE_MISMATCH");
}
/** Inspect names/metadata only: status/diff can execute a clean filter even when
 * they ultimately reject dirty bytes. Include the index (also Git's fallback
 * when a working attribute file is absent), not just untracked/ignored files.
 * Global/system attributes are disabled by gitOptionsV2/gitEnvironmentV2. */
export function assertGitAttributesV2(root: string): void {
  for (const options of [["--cached"], ["--others", "--exclude-standard"], ["--others", "--ignored", "--exclude-standard"]]) {
    const paths = nativeGitV2(root, "ls-files", "-z", ...options, "--", ".gitattributes", ":(glob)**/.gitattributes").split("\0").filter(Boolean);
    for (const path of paths) {
      // Git emits an opaque nested worktree directory for this pathspec, even
      // when it contains no attributes. Inspect it rather than treating its
      // mere existence as an attribute (owned implementation roots live here).
      requireV2(path.endsWith("/") && !path.endsWith(".gitattributes/"), "UNSUPPORTED_GIT_CAPABILITY: index/worktree attributes");
      const inspect = (directory: string) => {
        requireV2(lstatSync(directory).isDirectory() && !lstatSync(directory).isSymbolicLink(), "UNSUPPORTED_GIT_CAPABILITY: opaque attributes path");
        for (const entry of readdirSync(directory, { withFileTypes: true })) {
          requireV2(entry.name !== ".gitattributes", "UNSUPPORTED_GIT_CAPABILITY: nested worktree attributes");
          if (entry.isDirectory() && entry.name !== ".git") inspect(join(directory, entry.name));
        }
      };
      inspect(join(root, path));
    }
  }
  const path = nativeGitV2(root, "rev-parse", "--path-format=absolute", "--git-path", "info/attributes");
  try {
    const stat = lstatSync(path);
    requireV2(stat.isFile() && readFileSync(path).length === 0, "UNSUPPORTED_GIT_CAPABILITY: info/attributes");
  } catch (error: any) { if (error.code !== "ENOENT") throw error; }
}
function inspectGitTreesV2(root: string, candidates: CandidateV2[]): void {
  for (const candidate of candidates) {
    inspectGitCandidateV2(root, candidate);
    const entries = nativeGitV2(root, "ls-tree", "-r", "-z", candidate.tree).split("\0").filter(Boolean);
    requireV2(entries.every(e => !e.startsWith("160000 ") && !/(?:\t|\/)\.gitattributes$/.test(e)), "UNSUPPORTED_GIT_CAPABILITY: gitlinks or attributes in materialized tree");
  }
}
export async function eligibleGitV2(binding: GitBindingV2, candidates: CandidateV2[]): Promise<void> {
  await verifyBindingV2(binding); const root = binding.root.path;
  assertGitAttributesV2(root);
  const config = nativeGitV2(root, "config", "--null", "--list");
  for (const entry of config.split("\0")) {
    const [key, value = ""] = entry.split("\n");
    // Unused drivers/hooks and unrelated safe config are allowed. Attributes
    // selecting drivers/filters are unsupported below, before materialization.
    if (/^(extensions\.partialclone|remote\..*\.promisor)$/.test(key)
      || /^(core\.(sparsecheckout|ignorecase)|extensions\.worktreeconfig)$/.test(key) && value !== "false"
      || key === "core.symlinks" && value === "false") throw Error(`UNSUPPORTED_GIT_CAPABILITY: ${key}`);
  }
  for (const path of ["shallow", "info/grafts", "objects/info/alternates"]) {
    const file = join(binding.common.path, path);
    requireV2(!await exists(file) || (await readFile(file)).length === 0, `UNSUPPORTED_GIT_CAPABILITY: ${path}`);
  }
  inspectGitTreesV2(root, candidates);
}
export function composeGitV2(op: GitOperationV2): CandidateV2 {
  const root = op.binding.root.path;
  assertGitAttributesV2(root); inspectGitTreesV2(root, [op.sourceBase, op.expected, op.candidate]);
  // Git lets a configured driver shadow even the built-in name "text".
  // Pinning merge.default alone does not disable that external execution.
  requireV2(!nativeGitV2(root, "config", "--null", "--list").split("\0").some(entry => entry.split("\n")[0] === "merge.text.driver"), "UNSUPPORTED_GIT_CAPABILITY: merge.text.driver overrides the required built-in driver");
  for (const candidate of [op.candidate, op.expected]) nativeGitV2(root, "merge-base", "--is-ancestor", op.sourceBase.commit, candidate.commit);
  const tree = nativeGitV2(root, "merge-tree", "--write-tree", "--no-messages", `--merge-base=${op.sourceBase.commit}`, op.expected.commit, op.candidate.commit);
  requireV2(new RegExp(`^[0-9a-f]{${op.expected.commit.length}}$`).test(tree), "MERGE_TREE_CONFLICT_OR_UNSUPPORTED");
  const env = { ...gitEnvironmentV2(), GIT_AUTHOR_NAME: "Pi integration", GIT_AUTHOR_EMAIL: "integration@pi.invalid", GIT_COMMITTER_NAME: "Pi integration", GIT_COMMITTER_EMAIL: "integration@pi.invalid", GIT_AUTHOR_DATE: "2000-01-01T00:00:00Z", GIT_COMMITTER_DATE: "2000-01-01T00:00:00Z" };
  const commit = execFileSync("git", [...safeGitOptionsV2(root), "commit-tree", tree, "-p", op.expected.commit, "-m", `V2 integration ${op.operationId}\n\nCandidate: ${op.candidate.commit}\nProfile: ${op.profile}`], { cwd: root, env, encoding: "utf8", timeout: 60000 }).trim();
  requireV2(nativeGitV2(root, "rev-list", "--parents", "-n", "1", commit) === `${commit} ${op.expected.commit}`, "COMPOSITION_PARENT_MISMATCH");
  return { commit, tree };
}
export function privateRefV2(root: string, ref: string, oid: string): void {
  requireV2(/^refs\/pi-dag-v2\/[A-Za-z0-9/._-]+$/.test(ref), "INVALID_PRIVATE_REF"); nativeGitV2(root, "check-ref-format", ref);
  const direct = spawnSync("git", [...safeGitOptionsV2(root), "symbolic-ref", "--quiet", "--no-recurse", ref], { cwd: root, env: gitEnvironmentV2() });
  requireV2(direct.status === 1, "PRIVATE_REF_SYMBOLIC_OR_UNREADABLE");
  const old = spawnSync("git", [...safeGitOptionsV2(root), "show-ref", "--verify", "--hash", ref], { cwd: root, env: gitEnvironmentV2(), encoding: "utf8" });
  if (old.status === 0) { requireV2(old.stdout.trim() === oid, "PRIVATE_REF_CONFLICT"); return; }
  nativeGitV2(root, "update-ref", "--no-deref", ref, oid, "0".repeat(oid.length));
}
export async function assertTargetV2(op: Pick<GitOperationV2, "binding" | "targetRef">, candidate: CandidateV2, checkLocks = true): Promise<void> {
  // Recovery/closure call this without initial dispatch eligibility. Recheck at
  // every observation boundary before any index refresh or content comparison.
  await eligibleGitV2(op.binding, [candidate]); const root = op.binding.root.path;
  requireV2(op.targetRef.startsWith("refs/heads/"), "DIRECT_BRANCH_REQUIRED"); nativeGitV2(root, "check-ref-format", op.targetRef);
  requireV2(nativeGitV2(root, "symbolic-ref", "--no-recurse", "HEAD") === op.targetRef, "BOUND_BRANCH_CHANGED");
  const direct = spawnSync("git", [...safeGitOptionsV2(root), "symbolic-ref", "--quiet", "--no-recurse", op.targetRef], { cwd: root, env: gitEnvironmentV2() });
  requireV2(direct.status === 1, "TARGET_NOT_DIRECT");
  requireV2(nativeGitV2(root, "rev-parse", op.targetRef) === candidate.commit && nativeGitV2(root, "rev-parse", "HEAD^{tree}") === candidate.tree, "TARGET_DRIFT");
  const worktrees = nativeGitV2(root, "worktree", "list", "--porcelain", "-z").split("\0\0");
  const checkouts = worktrees.filter(w => w.split("\0").includes(`branch ${op.targetRef}`));
  requireV2(checkouts.length === 1, "DUPLICATE_BRANCH_CHECKOUT");
  requireV2(checkouts[0].split("\0").includes(`worktree ${root}`), "BOUND_WORKTREE_NOT_REGISTERED");
  for (const name of ["MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-apply", "rebase-merge", "sequencer", "BISECT_START", ...(checkLocks ? ["index.lock", "HEAD.lock", "ORIG_HEAD.lock"] : [])]) {
    requireV2(!await exists(join(op.binding.admin.path, name)), `GIT_OPERATION_OR_LOCK: ${name}`);
  }
  if (checkLocks) requireV2(!await exists(join(op.binding.common.path, `${op.targetRef}.lock`)), "FOREIGN_TARGET_LOCK");
  // Ignored bytes are retained too. Rejecting them is conservative and avoids
  // cleanup assumptions. Store metadata must live in an excluded admin/store root.
  requireV2(nativeGitV2(root, "status", "--porcelain=v1", "--untracked-files=all") === ""
    && nativeGitV2(root, "write-tree") === candidate.tree, "TARGET_DIRTY");
  const flags = nativeGitV2(root, "ls-files", "-v").split("\n");
  requireV2(flags.every(line => !line || line.startsWith("H ")), "UNSUPPORTED_INDEX_FLAGS");
}
export async function observeGitV2(op: GitOperationV2): Promise<NonNullable<GitOperationV2["observation"]>> {
  try { await verifyBindingV2(op.binding); } catch { return "identity-drift"; }
  let oid: string; try { oid = nativeGitV2(op.binding.root.path, "rev-parse", op.targetRef); } catch { return "third"; }
  const kind = oid === op.proposal?.commit ? "new" : oid === op.expected.commit ? "old" : "third";
  if (kind === "third") return kind;
  try { await assertTargetV2(op, kind === "new" ? op.proposal! : op.expected); return `${kind}-clean`; } catch { return `${kind}-dirty`; }
}
export async function makeGuardV2(op: GitOperationV2, directory: string): Promise<string> {
  requireV2(op.proposal, "PROPOSAL_REQUIRED"); const hooks = join(directory, "hooks"); await mkdir(hooks, { mode: 0o700 });
  const context = await open(join(hooks, "context.json"), "wx", 0o600);
  try { await context.writeFile(JSON.stringify({ binding: op.binding, targetRef: op.targetRef, expected: op.expected, proposal: op.proposal })); await context.sync(); } finally { await context.close(); }
  await copyFile(fileURLToPath(new URL("./reference-transaction.py", import.meta.url)), join(hooks, "reference-transaction"));
  await chmod(join(hooks, "reference-transaction"), 0o700);
  for (const path of [join(hooks, "reference-transaction"), hooks, dirname(hooks)]) { const f = await open(path, "r"); try { await f.sync(); } finally { await f.close(); } }
  return hooks;
}
export async function rejectLegacyGitV2(common: string): Promise<void> {
  const dir = join(common, "pi-dag-v1", "integration-locks");
  if (await exists(dir)) requireV2((await readdir(dir)).every(name => !name.endsWith(".lock") && !name.includes(".initializing-")), "V1_INTEGRATION_UNRESOLVED");
}
