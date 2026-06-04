You are running `/dag chunk`.

Read `.ai/plan.md`, `.ai/project.md`, user-global DAG config, project `.ai/dag.config.json`, repository context, and inline instructions. Write `.ai/chunks/chunk-N.md` and `.ai/dag.json`.

Treat chunking as ownership and dependency design, not just splitting a list.

Chunking guidance:

- Each chunk should have one clear deliverable and a scope a worker can implement independently.
- Prefer independent chunks when that is real, but preserve actual dependencies.
- Avoid over-parallelizing changes that will touch the same files, shared APIs, migrations, or tightly coupled behavior.
- Assign `ownedFiles` and `forbiddenFiles` conservatively to reduce scope drift and merge conflicts.
- Put project/chunk-specific setup, implementation, and validation details on node `setupInstructions`, `implementationInstructions`, and `validationInstructions`.
- Include validation instructions that can produce useful evidence for the validator.
- Preserve user/global/project config choices unless they conflict with the plan or schema.

Parallelization guidance:

- Before finalizing chunks, do a semantic parallelization pass.
- Do not split work just because it touches multiple files. Instead, identify independent intent lanes: parts of the plan with different goals, user workflows, invariants, validation evidence, and low need for shared terminology or product decisions.
- Prefer separate chunks when each candidate chunk:
  - has one coherent purpose or user-facing behavior;
  - can be implemented and validated without another worker making the same conceptual decisions;
  - owns mostly disjoint files or an isolated section of a shared file;
  - has a clear dependency relationship if it consumes another chunk's API, prompt, config, or artifact;
  - can fail or be revised without forcing unrelated chunks to be rewritten.
- Keep work together when files are semantically coupled, even if there are many files. Coupling signs include:
  - the files describe the same workflow or user concept;
  - the files must use the same terminology, policy, or safety rule;
  - one file is the command/entrypoint and the other is the prompt or contract it invokes;
  - two changes must be reviewed together to know whether the behavior is coherent;
  - splitting would cause two agents to independently decide the same product question.
- For prompt, documentation, or configuration-heavy work, group by intent lane rather than by file extension. Examples include discovery/interrogation, planning/chunking, execution/conducting, validation/review/retro, archive/history/cleanup, and final docs/smoke/package verification.
- Use final dependent chunks for shared documentation, smoke tests, generated indexes, or package manifests that should reflect all earlier chunks.

DAG shape requirements:

- Use top-level `steps`.
- Use top-level `flows`.
- Use top-level step-shaped `merge`.
- Each node should use `flow` or inherit `defaults.flow`.
- Use `defaults.mergeStrategy: "rebase-ff"` when including a merge strategy.
- Do not include `merge` inside any flow.
- Keep project/chunk-specific setup and validation details on nodes, not in builtin prompt markdown.

After writing `.ai/chunks/*` and `.ai/dag.json`, call the `dag_diagram` tool. It validates the DAG and renders the compact Style C dependency diagram from the saved `.ai/dag.json`; you may call `dag_validate` separately if you need an additional validation check, but `dag_diagram` output is the source of truth for the final diagram.

In your final response:

- Print the `dag_diagram` text directly as the primary output, without adding a heading such as `dag_diagram output:`.
- Do not wrap the diagram in Markdown code fences; Pi's terminal output is already monospaced, and the diagram should feel like command output.
- Include validation errors, validation warnings, or renderer warnings after the diagram when present, including hard-edge mismatch warnings.
- Do not hand-draw or rewrite a separate dependency diagram when `dag_diagram` succeeds.
- If `dag_diagram` is unavailable or errors, say diagram rendering failed and include validation status instead of inventing a confident diagram.

Before finishing, self-review for missing dependencies, overlapping ownership, vague validation, invalid paths, invalid flow references, and any accidental `merge` entries inside flows.
