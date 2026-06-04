You are running `/dag archive`.

Create a durable history record for completed or paused DAG work, then ask whether the user wants to clean up old working artifacts. History creation comes first; cleanup is optional and requires explicit confirmation after the history file exists.

Read available context:

- `.ai/project.md`
- `.ai/plan.md`
- `.ai/dag.json`
- `.ai/chunks/*.md`
- `.ai/grillme/*` when relevant
- `.ai/review.md`
- `.ai/retro.md`
- Latest `.ai/runs/<runId>/` status, worker records, artifacts, and logs when present
- Relevant repository files when needed to understand what actually changed

If these artifacts do not explain the intent or outcome well enough, ask the user one focused question and wait for the answer before writing the history file.

Create a history file under `.ai/history/` using this filename shape:

```text
YYYY-MM-DD-HH-MM-<type>-<hyphenated-description>.md
```

Choose `<type>` from:

- `feat` for feature work
- `fix` for bug fixes
- `chore` for cleanup, build, or configuration work
- `tweak` for small behavior or UI adjustments
- `docs` for documentation-only work

The first line of the file must be a comma-separated list of relevant keywords.

Include concise but useful sections for:

- Intent and background
- Decisions and constraints
- DAG/chunk structure
- What changed
- Validation performed and validation gaps
- Conflicts, failures, retries, or recoveries
- Remaining follow-up
- Source artifacts summarized

After the history file has been written, ask whether the user wants to clean up old files. Offer clear choices such as:

1. Keep everything.
2. Remove obsolete planning artifacts.
3. Archive/copy old artifacts into a timestamped directory.
4. Custom cleanup.

Do not delete, move, or overwrite `.ai/project.md`, `.ai/plan.md`, `.ai/dag.json`, `.ai/chunks/*`, `.ai/grillme/*`, `.ai/runs/*`, review/retro files, or other artifacts unless the user explicitly confirms a cleanup option after the history file exists.
