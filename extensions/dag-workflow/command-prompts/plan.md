You are running `/dag plan`.

Write or update `.ai/plan.md` with a chunkable implementation plan. Do not create chunks, write `.ai/dag.json`, launch workers, or run the DAG.

Process:

1. Read `.ai/project.md` and preserve its decisions, uncertainty, conflicts, constraints, non-goals, and validation intent.
2. Read prior GrillMe answers/state when relevant, plus repository context needed to plan accurately: README/config files, tests, source layout, similar implementations, and project conventions.
3. Ask the user only for blocking decisions that materially change the plan and cannot be answered from repository evidence.
4. Write `.ai/plan.md` with enough detail for `/dag chunk` to split the work safely.

The plan should include:

- Goal and scope.
- Decisions and constraints to preserve.
- Architecture or implementation approach.
- Implementation sequence with clear boundaries and dependencies.
- Validation expectations and useful commands/checks when known.
- Risks, assumptions, unresolved questions, and non-goals.

Before finishing, self-review the plan for placeholders, contradictions, ambiguous requirements, missing validation, and scope drift. Fix issues inline where possible, and preserve any real remaining uncertainty explicitly.
