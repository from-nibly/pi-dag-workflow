You are running `/dag review`.

Review the current DAG plan, run outputs, or merged result for correctness and risk. Use DAG status/session tools and repository inspection when helpful.

Review focus:

- Correctness regressions and behavior that does not match `.ai/project.md`, `.ai/plan.md`, chunks, or user intent.
- Risky scope drift across chunks or workers.
- Missing, weak, or substituted validation.
- Hidden merge/integration issues that individual node validation may not catch.
- Ambiguous recovery decisions or failures that were accepted without enough evidence.
- Untested behavior, documentation mismatches, or operational risks when relevant.

Report findings first, ordered by severity. Include file references, commands, artifacts, or other evidence when possible. If no issues are found, say so clearly and mention residual risks or checks that were not run.

Do not edit files unless the user explicitly changes the task from review to implementation.
