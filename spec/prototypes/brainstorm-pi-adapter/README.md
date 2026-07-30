# Brainstorm Pi adapter prototype

## Purpose

This user-requested prototype tests the production integration boundary around the accepted five-tool structured-brainstorm core before the existing Pi extension is replaced. It focuses on command lifecycle, dynamic tool mode, serialized file mutation, review presentation transport, failure recovery, and promotion handoff.

It is independent from:

- the production extension under `extensions/dag-workflow/`;
- the domain-oriented [object-tools prototype](../object-tools/README.md);
- a real Pi process or real Lavish server.

## Run

```bash
node spec/prototypes/brainstorm-pi-adapter/scenario.mjs
```

Expected output:

```text
Brainstorm Pi adapter prototype OK: command selection, dynamic mode/tools, queued mutations, review transport/recovery, promotion, compact receipts, and GrillMe retirement verified.
```

An assertion failure exits nonzero.

## Expected observations

The scenario uses a temporary project, fake Pi API/context, deterministic fake UI, injectable domain operations, and an injectable whole-document renderer. It verifies:

- all five brainstorm tools register but begin inactive;
- unrelated extension tools survive brainstorm activation and suspension;
- headless `/dag brainstorm` requires explicit `--continue` or `--new` selection;
- interactive mode asks Resume versus New and selects among multiple snapshots;
- malformed disposable snapshots do not poison resume selection;
- mode-specific system guidance appears only while brainstorming is active;
- another `/dag` workflow suspends only the five brainstorm tools;
- two concurrent updates to one snapshot serialize without a lost write;
- context/update/review/resolve/promote adapters return compact Pi-shaped results;
- mutation receipts do not echo stored or authored context;
- renderer failure leaves the review active;
- successful renderer submission returns through tool details but is not persisted;
- `resolve_review` stores interpreted decisions and replaces Current understanding;
- promotion remains prepare → agent-authored spec edit → record;
- GrillMe commands and tools are absent.

## Findings embodied by the prototype

1. A small adapter can keep `.ai/brainstorm/` authoritative while Pi tool results remain presentation/session records.
2. Dynamic activation can preserve tools owned by other extensions by adding/removing only the five brainstorm names.
3. A per-snapshot mutation queue is required even for a single agent because Pi may execute sibling tool calls concurrently.
4. The review operation can own presentation choice without storing renderer semantics: renderer failure and transport feedback do not resolve or contaminate semantic state.
5. Command-native interactive selection and explicit headless flags can share one lifecycle without an agent-owned selection turn.
6. Compact receipts can carry revision/change metadata without echoing full state or authored prose.

## Demonstrated behavior

This prototype defines the intended integration behavior for:

- command-side new/resume selection;
- in-memory brainstorm runtime mode;
- additive tool activation and selective suspension;
- mode-specific prompt injection;
- five Pi-shaped adapter calls;
- serialized mutation execution;
- renderer transport/failure boundaries;
- promotion handoff;
- GrillMe retirement.

## Out of scope

It does not define:

- final TypeBox schemas;
- final production state/domain implementation;
- real `ExtensionAPI` registration;
- final TUI tool-row components;
- actual Lavish HTML generation, browser launch, polling, or rendering engine;
- session replacement internals beyond explicit-resume behavior;
- renderer visual design;
- agent prompt quality evaluation;
- planning handoff;
- durable migration for old GrillMe or prototype state.

## Definition boundary

`adapter.mjs` and `scenario.mjs` are mutable behavioral evidence, not production imports. Planning may cite this README and scenario when implementation needs to preserve the demonstrated lifecycle. Production code should use Pi's real TypeBox schemas and file-mutation queue rather than copying the fake interfaces literally.
