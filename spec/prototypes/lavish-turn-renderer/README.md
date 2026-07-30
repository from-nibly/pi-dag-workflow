# Lavish turn-renderer prototype

## Purpose

This user-requested prototype made the accepted agent–human turn renderer concrete before production integration. It renders a narrow model-review projection into a deterministic, self-contained HTML artifact and exercises the Lavish CLI lifecycle through an injectable subprocess seam. The resulting production implementation now lives in `extensions/dag-workflow/project-model/review-*.ts` and `lavish-cli.ts`; this directory remains mutable behavioral evidence rather than a runtime dependency.

It is independent from production extension registration and never reads or mutates `project-model/model.json`.

## Run

```bash
node spec/prototypes/lavish-turn-renderer/scenario.mjs
```

Expected output:

```text
Lavish turn-renderer prototype OK: projection, shell, rich context, controls, feedback caps, CLI seam, resume/end, and cleanup verified.
```

Inspect the committed deterministic artifact directly:

```text
spec/prototypes/lavish-turn-renderer/sample-turn.html
```

The sample is visibly labeled **Prototype fixture**. Its representative decisions exercise multi-point interaction and never resolve repository authority.

## Optional live exercise

The live exercise intentionally uses `npx -y lavish-axi` only as an opt-in prototype convenience. Production is intended to resolve a tested optional package dependency instead.

Render without opening Lavish:

```bash
node spec/prototypes/lavish-turn-renderer/live.mjs --render-only
```

Open and wait for browser feedback:

```bash
node spec/prototypes/lavish-turn-renderer/live.mjs
```

If polling is interrupted, the artifact and presentation metadata remain under:

```text
.ai/model-sessions/focus-lavish-renderer/lavish/review-renderer-boundary.*
```

Resume with:

```bash
node spec/prototypes/lavish-turn-renderer/live.mjs --resume
```

A user-ended Lavish session will not reopen unless `--reopen` is also explicitly passed.

## Prototype files

- `contract.md` — projection, lifecycle, feedback, and non-goal boundary.
- `projection.mjs` — runtime projection validation and safe deterministic paths.
- `renderer.mjs` — package-owned offline shell and standard feedback controls.
- `lavish-cli.mjs` — abortable CLI subprocess and bounded AXI/TOON recognition.
- `lifecycle.mjs` — render/open/poll/resume/end/cleanup state machine.
- `fixtures/whole-turn.json` — representative whole-turn projection.
- `fixtures/poll-*.toon` — recorded bounded protocol fixtures.
- `fixtures/fake-lavish-cli.mjs` — deterministic open/poll/end executable.
- `scenario.mjs` — automated behavioral checks.
- `live.mjs` — opt-in real Lavish exercise.
- `sample-turn.html` — committed deterministic visual result.

## Demonstrated behavior

The prototype verifies:

- a renderer can operate on a narrow versioned projection without full-model reads;
- Current understanding, delta, frontier, awareness, decisions, and rich context fit one coherent shell;
- standard controls queue review ID, point ID, option ID, and hash context without auto-sending;
- complete decision prose is visible directly in option cards while internal authority serialization stays hidden;
- multiple decisions render and queue independently;
- every decision offers an explicit **Other** radio path plus a separate always-available response box, and relies on Lavish's window-level send control;
- the frontier explains how the agent selects the next material unresolved work after the turn;
- package-owned CSS/JavaScript can be self-contained and network-independent;
- trusted HTML and scripts remain available without sanitization or isolation;
- prompt count, prompt bytes, total bytes, and severe layout warnings can be bounded explicitly;
- DOM snapshots and Lavish `next_step` prose need not enter agent context;
- aborting a poll preserves the stable artifact and resumable metadata;
- user-ended sessions are not implicitly reopened;
- resolution cleanup can best-effort end Lavish and remove disposable files.

## Findings to evaluate through dogfood

The concrete prototype exposes several values that should be tuned from real turns rather than settled abstractly:

1. Which delta/frontier fields earn permanent space in the projection.
2. Whether rich blocks need more placement targets than before/after review and one point.
3. Suitable prompt and total-byte caps for annotations and whiteboard feedback.
4. Whether a bespoke TOON recognizer is sufficient or Lavish should expose a machine-readable mode.
5. How much presentation metadata belongs in the focus session versus derivation from file/session status.
6. How the production adapter should recognize exact review presentation from a visible tool result.

## Definition boundary

This is mutable behavioral evidence, not production authority or a second renderer implementation to maintain indefinitely. The production adapter should reuse learned contracts deliberately rather than import this prototype wholesale.
