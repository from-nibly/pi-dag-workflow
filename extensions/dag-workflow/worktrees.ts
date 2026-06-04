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

export async function mergeNode(cwd: string, state: RunState, node: DagNode): Promise<string> {
  const branch = state.nodes[node.id]?.branch ?? nodeBranch(state.manifest.runId, node.id);
  const result = await execGit(cwd, ["merge", "--no-ff", branch, "-m", `Merge DAG node ${node.id}`]);
  if (result.code !== 0) throw new Error(`git merge failed: ${result.stderr || result.stdout}`);
  return result.stdout + result.stderr;
}
