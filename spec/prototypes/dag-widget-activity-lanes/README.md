# DAG widget activity-lane prototype

## Purpose

This user-requested disposable prototype compares four terminal-native ways to render the accepted activity-centered DAG widget before production implementation:

1. **Flow lanes** — one compact horizontal row per active or attention anchor.
2. **Graph branches** — each anchor routes visible edges to immediate dependent nodes.
3. **Split rail** — topology on one row and activity/progress detail below it.
4. **Mini-cards** — boxed nodes arranged side by side where width permits.

It exercises 50-, 80-, and 120-column viewports; live versus stale worker motion; F0–F8 progress; fan-out; attention detail; omission summaries; and the twelve-row ceiling. It is behavioral evidence only. It does not read or mutate `project-model/model.json`, launch workers, or register a Pi extension.

## Run checks

```bash
node spec/prototypes/dag-widget-activity-lanes/scenario.mjs
```

Expected output begins:

```text
DAG widget activity-lane prototype OK
```

## Review interactively

Open the comparison artifact through Lavish:

```bash
lavish-open spec/prototypes/dag-widget-activity-lanes/prototype.html
```

The page lets the reviewer switch widths and scenarios, pause motion, compare all three variants, and queue a preferred direction plus freeform feedback.

## Files

- `render.mjs` — pure deterministic prototype renderers and fixtures.
- `scenario.mjs` — responsive width/height, determinism, and motion checks.
- `prototype.html` — interactive terminal-like comparison and Lavish feedback controls.

## Definition boundary

The prototype intentionally does not settle production ANSI styling, exact spinner frames, final breakpoint constants, or controller timing. Accepted project-model decisions remain authority; feedback from this prototype must return through a formal brainstorm review before it changes product direction.
