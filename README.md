# pi-dag-workflow

Pi extension for mixed-initiative project-model brainstorming. The current production slice focuses on research, intent clarification, semantic review, and deterministic generated specifications.

Model-aware planning and execution are deferred while this workflow is dogfooded.

## Authority

One repository-wide tracked snapshot owns project meaning:

```text
project-model/model.json
```

It may contain both governing and non-authoritative objects:

- workstreams;
- intent and concepts;
- evidence and assumptions;
- questions and enduring tensions;
- scenarios and proposals;
- decisions and commitments;
- discoveries from research, prototypes, repository inspection, or later execution.

Human authority is explicit. Accepted intent, concepts, scenarios, decisions, and commitments carry content-bound receipts. Agent findings and derived implications remain non-authoritative until accepted.

Tracked Markdown under `spec/` is a deterministic readable projection of accepted model-owned prose, not another source of truth. Rendered review turns, frontiers, deltas, ledgers, and consequence views remain ephemeral by default.

## Install

```nu
pi install git:git@github.com:from-nibly/pi-dag-workflow@v0.1.0
```

## Model brainstorming commands

```text
/dag brainstorm                         # interactive New/Resume selector
/dag brainstorm new <name>              # create a resumable focus session
/dag brainstorm resume <focus-id>       # resume an exact focus
/dag brainstorm list                    # list focus sessions
/dag brainstorm stop                    # suspend model mode
```

A focus session is ignored presentation state under `.ai/model-sessions/`. It contains selected workstreams, one active review turn, and one replaceable previous-review snapshot. Optional Lavish HTML and adjacent lifecycle metadata live under `<focus-id>/lavish/`. None of these files owns unique project meaning.

Reloading, resuming, forking, or cloning a linked Pi conversation restores the exact focus. A new unlinked Pi session starts inactive. Concurrent multi-agent use of one focus is unsupported and has no locking or merge protocol.

## Model tools

The seven tools register once and are activated only after Pi's extension runtime has initialized and a model brainstorming focus is active:

- `dag_model_context` — read narrow orientation, entity, frontier, delta, review, or governing projections.
- `dag_model_update` — record non-authoritative findings, relationships, routing metadata, or Current understanding. It cannot grant authority or rewrite accepted semantics.
- `dag_model_record_direction` — record unambiguous direct user authority with content-bound receipts.
- `dag_model_review` — create an exact hash-bound review turn with **For awareness** and **Decisions needed**; its exact visible tool result records successful presentation.
- `dag_model_present_review` — optionally render and `present`, `resume`, or `end` the active review through Lavish while returning bounded feedback for agent interpretation.
- `dag_model_resolve_review` — apply independent fresh outcomes while preserving stale, omitted, or ambiguous points.
- `dag_model_specs` — preview, check, or explicitly recover deterministic generated specs.

Routine successful semantic mutations automatically synchronize affected current specs without making a Git commit.

Lavish presentation uses the pinned optional dependency `lavish-axi@0.1.43`; it never falls back to ambient `npx`. The generated shell supports multiple independent decision points, complete visible option prose, an explicit **Other** radio, and a separate response box. The renderer does not resolve semantic state automatically: the agent validates returned review/point/option hashes and invokes `dag_model_resolve_review` from a bound human turn.

## Mixed-initiative loop

```text
Orient
  → Explore and record coherent non-authoritative findings
  → Consolidate material model changes
  → Stress-test with representative/boundary/failure cases
  → Present a materiality-based review turn
  → Apply exact explicit outcomes
  → Regenerate affected specs
  → Continue, change focus, or stop
```

Direct, unambiguous user direction commits once. Silence, generic praise, ambiguity, and agent-derived consequences never commit. Reconsidering accepted content does not revoke it automatically; generated specs retain still-governing content with an **Under review** marker until it is explicitly suspended, retired, or superseded.

New behavioral prototypes require explicit user request. Hand-authored prototype evidence lives under `spec/prototypes/<slug>/` and is protected from spec generation.

## Generated specifications

Project-specific non-semantic routing metadata in the model declares output paths, sections, short summaries, and object order. Every accepted object's full body has one canonical generated placement; other specs link to it.

V1 deliberately uses minimal one-way safety:

- generated files are marked;
- rendering occurs in a temporary location;
- `dag_model_specs check` regenerates and compares output;
- unknown target collisions fail;
- prototype directories are never overwritten;
- stale generated paths are reported conservatively.

There is no generated-file ownership manifest, editable generated region, reverse synchronization, or automatic deletion framework in V1.

## Deferred workflows

These commands currently report that model-aware replacements are deferred:

```text
/dag plan
/dag chunk
/dag run
/dag review
/dag retro
/dag archive
```

GrillMe, promotion, legacy prompt workflows, subagent execution registration, and model-unaware mutating DAG tools are removed.

Clearly labeled read-only diagnostics remain for pre-cutover artifacts:

```text
/dag validate
/dag status
/dag workers
/dag inspect
/dag tail
dag_validate
dag_diagram
dag_status
```

They cannot create or advance execution.

## Candidate migration

The repository includes a one-time importer for the previous structured-brainstorm snapshot:

```nu
node scripts/migrate-brainstorm-to-project-model.mjs
```

It writes:

```text
project-model/model.json                              # candidate mode
project-model/migrations/brainstorm-v2-candidate.md  # mapping/omission audit
.ai/model-migration/candidate/spec/**                 # ignored generated preview
```

The candidate does not become authoritative until its semantic mappings, omissions, generated specs, and exact manifest hash receive explicit human approval. Cutover is an isolated `dag_model_record_direction` operation with `{ "cutover": { "candidateManifestHash": "sha256:…" } }`; it creates `migration_cutover` receipts, changes model mode, and replaces the declared hand-maintained projection targets as one recoverable transaction. The importer refuses to replace an authoritative model, even with `--force`.

## Validation

```nu
npm run smoke
node scripts/project-model-test.mjs
# Only while project-model/model.json is still a non-authoritative candidate:
node scripts/migrate-brainstorm-to-project-model.mjs --force
```

The production tests cover model validation, acceptance boundaries, focus sessions, sparse/stale review resolution, deterministic projections, Pi activation and fork restoration, disabled legacy workflows, and migration candidate generation.
