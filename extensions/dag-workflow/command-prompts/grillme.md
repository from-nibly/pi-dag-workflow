You are running `/dag grillme`.

Generate and manage decision-focused GrillMe questions using the current `.ai/project.md`, `.ai/plan.md` if present, repository evidence, and prior GrillMe answers when needed. The goal is to reduce planning, chunking, and execution risk.

Question generation:

- First do the repo/project research needed to generate the right questions; do not call a GrillMe question tool until the question set is ready to show to the user.
- Each `/dag grillme` starts a new GrillMe file by default. Treat prior GrillMe answers as context only; do not keep adding questions to a completed GrillMe unless the user explicitly asked to continue or reopen it.
- If prior GrillMe answers would help and you have not already read them in this pass, use `dag_grillme_get_answers` so answers are filtered to the safe keys. Do not read `.ai/grillme/*.json` files directly.
- When `.ai/project.md` contains suggested GrillMe starting points, decision seeds, tangent areas, or similar brainstorm output, treat them as non-binding raw material. They are not a required checklist and not the complete question set.
- Use brainstorm seeds to understand likely uncertainty, then generate the best current question set from all available evidence: `.ai/project.md`, `.ai/plan.md` if present, repository context, prior GrillMe answers, user instructions, and your own research.
- You may keep a seed if it still represents a real decision, merge several seeds into one better question, split a seed when it hides multiple decisions, rewrite it completely, drop it if already answered/low-value/premature/superseded, or add questions not mentioned in the seed section.
- Do not preserve brainstorm wording, ordering, or scope just because it exists.
- Use `dag_grillme_set_questions` or `dag_grillme_update_questions` to save the final question set and open the GrillMe UI.
- Generate questions that resolve real decisions, assumptions, risks, or conflicts. Avoid generic questionnaires and questions the repository can answer.
- Research repo-answerable facts directly before asking the user.
- Include conflict-resolution questions when evidence, prior answers, or user intent conflict.
- For each useful question, include why it matters. Provide answer options and a recommended option when that helps, while preserving tradeoffs.
- Keep GrillMe state focused on questions and answers; do not use it as a research notebook.

After answers are available:

- Use `dag_grillme_get_answers` to read answered, non-discarded questions if you have not already read them in this pass.
- Inspect answers for explicit research requests, implied research needs, claims that should be verified, or new repo/documentation questions.
- Follow up on that research automatically when possible instead of leaving it as chat-only intent.
- Record researched findings, source links when available, resolved decisions, remaining uncertainty, and conflicts in `.ai/project.md` via `dag_grillme_record_understanding`.
- Explain what is still missing from project understanding so the user can answer directly in chat or run another `/dag grillme` pass.

Boundaries:

- Use `dag_grillme_*` tools only from `/dag grillme`.
- Do not create chunks or run workers.
- Research summaries and links belong in `.ai/project.md`, not in GrillMe state.
