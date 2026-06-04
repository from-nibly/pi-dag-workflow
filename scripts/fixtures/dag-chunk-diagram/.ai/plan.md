# Fixture Plan: Task Summary Improvements

## Goal

Make the tiny task summary fixture slightly more useful while producing a DAG shape that demonstrates parallel chunk output.

## Requested implementation areas

Please chunk this plan into four implementation chunks with the dependency shape described below.

### 1. Task data helpers

Add small helper logic for normalizing task objects. This work should own `src/main.js` or a new helper file if needed.

Validation intent:

- Task objects with missing `done` values are treated as incomplete.
- Existing `summarizeTasks` behavior still works.

### 2. Summary formatting

Add formatting support for summary text, such as including a percentage complete. This can be designed independently from docs/tests but should depend on the task data helper if it consumes normalized data.

Validation intent:

- `summarizeTasks([{ done: true }, { done: false }])` can produce useful count and percentage information.

### 3. CLI output polish

Improve the direct `node src/main.js` output to use the summary behavior. This should depend on the summary formatting work.

Validation intent:

- `npm test` or `node src/main.js` prints a clear task summary.

### 4. README and smoke validation

Update fixture documentation and add or adjust lightweight validation instructions. This should run after the behavior chunks so docs and smoke checks reflect the final shape.

Validation intent:

- README explains the fixture behavior.
- `npm test` or equivalent command succeeds.

## Desired DAG shape for `/dag chunk`

Prefer this shape unless the chunker finds a strong reason to adjust it:

```text
chunk-1  Task data helpers       deps: -
chunk-2  Summary formatting      deps: chunk-1
chunk-3  CLI output polish       deps: chunk-2
chunk-4  README/smoke validation deps: chunk-3
```

If the chunker can safely make docs independent, it may make chunk-4 depend on chunk-2 instead of chunk-3, but it should still produce at least four chunks so the diagram is easy to inspect.

## Non-goals

- Do not add dependencies.
- Do not create a real application.
- Do not optimize this fixture for production quality.
