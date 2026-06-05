You are running `/dag brainstorm`.

Build or update `.ai/project.md` so project understanding is durable and not left only in chat. Treat this as project discovery and research-backed sensemaking, not planning, implementation, or a user interview.

Work style:

- Read existing `.ai/project.md` when present before doing broad discovery.
- Inspect relevant repository context before asking questions the files can answer. Useful signals may include README files, package/config files, tests, source layout, existing `.ai/*` artifacts, and recent project conventions visible in the repo.
- Act as a research partner and concept-mapping helper. When the project seed references a known tool, domain, protocol, framework, UX pattern, or ecosystem convention, research or summarize the relevant background before generating suggestions.
- Prefer synthesizing current understanding, identifying interesting tangents, and recording uncertainty over interrogating the user.
- Do not use the user question tool by default. Ask the user directly only when progress is blocked because there is no project seed, no useful repository or domain evidence, and no safe way to make an informed assumption.
- Cover goals, users/use cases, constraints, non-goals, architecture, validation intent, risks, open decisions, and conflicts.
- Capture validation intent explicitly: how the user will know the work is successful.
- Preserve uncertainty, conflicts, and rejected options explicitly instead of flattening them into false certainty. Label material as known, inferred, assumed, conflicting, unknown, or speculative when useful.
- After meaningful new information, edit `.ai/project.md` directly.
- If you identify possible future GrillMe topics, record them as exploratory seeds under a section such as `Suggested GrillMe Starting Points`, not as final questions or a required queue. Prefer areas, tensions, and decision dimensions over polished questionnaire items, and make clear GrillMe may keep, merge, split, rewrite, drop, or add questions later.
- Final chat output should briefly summarize where the project stands, what changed in `.ai/project.md`, useful tangent areas to explore, and which uncertainties may be good candidates for a future `/dag grillme` pass.

Boundaries:

- Do not call `dag_grillme_*` tools from `/dag brainstorm`; those tools are reserved for `/dag grillme`.
- Do not create `.ai/plan.md`, `.ai/chunks/*`, or `.ai/dag.json`.
- Do not run workers or start a DAG run.
