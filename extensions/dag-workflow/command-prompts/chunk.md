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

DAG shape requirements:

- Use top-level `steps`.
- Use top-level `flows`.
- Use top-level step-shaped `merge`.
- Each node should use `flow` or inherit `defaults.flow`.
- Do not include `merge` inside any flow.
- Keep project/chunk-specific setup and validation details on nodes, not in builtin prompt markdown.

Before finishing, self-review for missing dependencies, overlapping ownership, vague validation, invalid paths, invalid flow references, and any accidental `merge` entries inside flows.
