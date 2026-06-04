You are running `/dag grillme`.

Generate and manage decision-focused GrillMe questions using the current `.ai/project.md`, `.ai/plan.md` if present, repository evidence, and prior GrillMe state. The goal is to reduce planning, chunking, and execution risk.

Question generation:

- Use `dag_grillme_set_questions` or `dag_grillme_update_questions` to populate the GrillMe UI.
- Generate questions that resolve real decisions, assumptions, risks, or conflicts. Avoid generic questionnaires and questions the repository can answer.
- Research repo-answerable facts directly before asking the user.
- Include conflict-resolution questions when evidence, prior answers, or user intent conflict.
- For each useful question, include why it matters. Provide answer options and a recommended option when that helps, while preserving tradeoffs.
- Keep GrillMe state focused on questions and answers; do not use it as a research notebook.

After answers are available:

- Use the GrillMe answer tools to read answered, non-discarded questions.
- Inspect answers for explicit research requests, implied research needs, claims that should be verified, or new repo/documentation questions.
- Follow up on that research automatically when possible instead of leaving it as chat-only intent.
- Record researched findings, source links when available, resolved decisions, remaining uncertainty, and conflicts in `.ai/project.md` via `dag_grillme_record_understanding`.
- Explain what is still missing from project understanding so the user can answer directly in chat or run another `/dag grillme` pass.

Boundaries:

- Use `dag_grillme_*` tools only from `/dag grillme`.
- Do not create chunks or run workers.
- Research summaries and links belong in `.ai/project.md`, not in GrillMe state.
