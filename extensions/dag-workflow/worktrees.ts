import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { DagNode, RunState } from "./types.ts";

export async function execGit(cwd: string, args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = await import("node:child_process");
  return await new Promise((resolve) => {
    child.execFile("git", args, { cwd }, (error, stdout, stderr) => {
      resolve({ code: error ? (error as any).code ?? 1 : 0, stdout: String(stdout), stderr: String(stderr) });
    });
  });
}

export async function getCurrentCommit(cwd: string): Promise<string> {
  const result = await execGit(cwd, ["rev-parse", "HEAD"]);
  return result.stdout.trim();
}

export async function getCurrentBranch(cwd: string): Promise<string> {
  const result = await execGit(cwd, ["branch", "--show-current"]);
  return result.stdout.trim() || "HEAD";
}

export function worktreesRoot(cwd: string, runId: string): string {
  return join(cwd, ".ai", "runs", runId, "worktrees");
}

export function nodeBranch(runId: string, nodeId: string): string {
  return `dag/${runId}/${nodeId}`;
}

export function nodeWorktreePath(cwd: string, runId: string, nodeId: string): string {
  return join(worktreesRoot(cwd, runId), nodeId);
}

export async function ensureNodeWorktree(cwd: string, state: RunState, node: DagNode): Promise<{ branch: string; worktree: string }> {
  const branch = nodeBranch(state.manifest.runId, node.id);
  const worktree = nodeWorktreePath(cwd, state.manifest.runId, node.id);
  await mkdir(worktreesRoot(cwd, state.manifest.runId), { recursive: true });
  if (!state.manifest.baseCommit) state.manifest.baseCommit = await getCurrentCommit(cwd).catch(() => "");
  if (!state.manifest.parentBranch) state.manifest.parentBranch = await getCurrentBranch(cwd).catch(() => "");
  if (!state.nodes[node.id]) throw new Error(`Unknown node ${node.id}`);
  if (!state.nodes[node.id].worktree) {
    const add = await execGit(cwd, ["worktree", "add", "-B", branch, worktree, state.manifest.baseCommit || "HEAD"]);
    if (add.code !== 0) throw new Error(`git worktree add failed: ${add.stderr || add.stdout}`);
    state.nodes[node.id].branch = branch;
    state.nodes[node.id].worktree = worktree;
    state.nodes[node.id].baseCommit = state.manifest.baseCommit;
  }
  return { branch, worktree };
}

const CONVENTIONAL_COMMIT_SUBJECT = /^(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\([A-Za-z0-9._/-]+\))?!?: \S.*$/;

export function isConventionalCommitSubject(subject: string): boolean {
  return CONVENTIONAL_COMMIT_SUBJECT.test(subject);
}

function gitFailure(command: string, result: { stdout: string; stderr: string }): string {
  return `${command} failed: ${result.stderr || result.stdout}`;
}

export async function mergeNode(cwd: string, state: RunState, node: DagNode): Promise<string> {
  const nodeState = state.nodes[node.id];
  if (!nodeState) throw new Error(`Unknown node ${node.id}`);
  const branch = nodeState.branch ?? nodeBranch(state.manifest.runId, node.id);
  if (!nodeState.worktree) throw new Error(`Node ${node.id} has no worktree to rebase`);
  const worktreeBranch = await getCurrentBranch(nodeState.worktree);
  if (worktreeBranch !== branch) throw new Error(`Node ${node.id} worktree is on ${worktreeBranch}, expected ${branch}`);

  const parent = await execGit(cwd, ["rev-parse", "HEAD"]);
  if (parent.code !== 0) throw new Error(gitFailure("git rev-parse HEAD", parent));
  const parentCommit = parent.stdout.trim();

  const rebase = await execGit(nodeState.worktree, ["rebase", parentCommit]);
  if (rebase.code !== 0) {
    throw new Error(`${gitFailure(`git rebase ${parentCommit}`, rebase)}\nResolve the rebase in ${nodeState.worktree}, keep commit subjects in Conventional Commits format, then retry dag_merge_node.`);
  }

  const log = await execGit(nodeState.worktree, ["log", "--format=%s", `${parentCommit}..HEAD`]);
  if (log.code !== 0) throw new Error(gitFailure("git log", log));
  const subjects = log.stdout.split("\n").map((line) => line.trim()).filter(Boolean);
  const invalidSubjects = subjects.filter((subject) => !isConventionalCommitSubject(subject));
  if (invalidSubjects.length > 0) {
    throw new Error(`Node ${node.id} has non-Conventional Commit subject(s):\n${invalidSubjects.map((subject) => `- ${subject}`).join("\n")}\nReword them in ${nodeState.worktree} before retrying dag_merge_node.`);
  }

  const result = await execGit(cwd, ["merge", "--ff-only", branch]);
  if (result.code !== 0) throw new Error(gitFailure("git merge --ff-only", result));
  return [rebase.stdout + rebase.stderr, `Validated ${subjects.length} Conventional Commit subject(s).`, result.stdout + result.stderr].filter(Boolean).join("\n");
}
