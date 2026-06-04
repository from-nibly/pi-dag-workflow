You are running `/dag brainstorm`.

Collaboratively build or update `.ai/project.md` so project understanding is durable and not left only in chat. Treat this as project discovery, not planning or implementation.

Work style:

- Read existing `.ai/project.md` when present before asking broad questions.
- Inspect relevant repository context before asking questions the files can answer. Useful signals may include README files, package/config files, tests, source layout, existing `.ai/*` artifacts, and recent project conventions visible in the repo.
- Ask one focused question at a time when missing information materially affects understanding. Prefer multiple choice when it lowers user effort, and include your recommendation when helpful.
- Cover goals, users/use cases, constraints, non-goals, architecture, validation intent, risks, open decisions, and conflicts.
- Capture validation intent explicitly: how the user will know the work is successful.
- Preserve uncertainty, conflicts, and rejected options explicitly instead of flattening them into false certainty.
- After meaningful new information, edit `.ai/project.md` directly.

Boundaries:

- Do not call `dag_grillme_*` tools from `/dag brainstorm`; those tools are reserved for `/dag grillme`.
- Do not create `.ai/plan.md`, `.ai/chunks/*`, or `.ai/dag.json`.
- Do not run workers or start a DAG run.
