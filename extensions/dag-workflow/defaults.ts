import type { DagWorkflowConfig } from "./types.ts";

export const PACKAGE_DEFAULT_CONFIG: DagWorkflowConfig = {
  defaults: {
    flow: "default",
    stashDirtyParent: true,
    mergeStrategy: "rebase-ff",
  },
  steps: [
    {
      id: "setup",
      kind: "agent",
      agent: "builtin:worker",
      prompt: "builtin:setup",
      input: "Prepare this node worktree using node.setupInstructions.",
      output: "Report setup actions, skipped setup, generated ignored paths, and blockers.",
      requires: [
        "Worktree dependencies/tools are ready or setup blockers are clearly reported.",
        "Tracked source files are not changed unless explicitly owned by the node.",
      ],
      onFail: "needs-decision",
    },
    {
      id: "execute",
      kind: "agent",
      agent: "builtin:worker",
      prompt: "builtin:executor",
      input: "Implement the chunk described by node.implementationInstructions and the chunk file.",
      output: "Summarize changed files, validation attempted, blockers, worktree cleanliness, commit hash(es), and Conventional Commit subject(s).",
      requires: [
        "Implementation changes are committed with Conventional Commit messages.",
        "Worktree is clean or remaining dirty files are explained.",
      ],
      onFail: "needs-decision",
    },
    {
      id: "validate",
      kind: "agent",
      agent: "builtin:reviewer",
      prompt: "builtin:validator",
      input: "Validate using node.validationInstructions and project conventions.",
      output: "Report validation actions/results, substitutions, risks, and end with VERDICT: PASS or VERDICT: FAIL.",
      requires: [
        "Validation evidence is described.",
        "Final output includes VERDICT: PASS or VERDICT: FAIL.",
      ],
      onFail: "retry:execute",
    },
  ],
  merge: {
    id: "merge",
    kind: "merge",
    onConflict: "resolve",
  },
  flows: {
    default: [{ id: "setup" }, { id: "execute" }, { id: "validate" }],
  },
};
