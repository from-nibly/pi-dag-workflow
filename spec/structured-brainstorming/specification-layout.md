# Specification layout

> **Migration evidence:** Hand-maintained canonical-spec authority is superseded by deterministic projections from the [mixed-initiative project model](../mixed-initiative-project-model/spec.md). Prototype-directory protections remain relevant.

## Canonical root

Canonical product behavior lives under `spec/`. OpenSpec is a conceptual precedent for domain-oriented current-state specs only. The project does not use the OpenSpec package, commands, skills, schemas, change folders, sync, archive, or planning workflow.

Active brainstorm, plan, chunk, and DAG state remains under `.ai/` and is never a canonical contract.

## Functional hierarchy

Directories reflect natural product functionality rather than brainstorming sessions, implementation order, or numbered changes.

Every directory beneath `spec/` contains `spec.md` except individual `spec/prototypes/<slug>/` directories, which use `README.md`.

Planning begins with the `spec.md` for the relevant functional scope. `spec/spec.md` is used for orientation when scope is unclear; it is not a mandatory first read for every narrow plan.

When a functional contract becomes large, supporting Markdown may carry detailed behavior. The parent `spec.md` links each relevant document with only a very short orientation summary and does not duplicate its specification content.

**Rationale:** predictable entry points help agents discover contracts, while short parent links avoid synchronization-heavy duplication.

## Content conventions

Specs may use explicit requirements and concrete scenarios wherever they improve precision. Exact OpenSpec grammar is not required. Domain-appropriate rationale, invariants, flows, constraints, diagrams, and links remain valid.

Important decisions include concise reasoning near the contract they constrain.

Durable specification documents are Markdown. Non-Markdown files beneath `spec/` are prototype assets rather than a separate durable artifact layer.

Use ordinary relative Markdown links. Prototype source comments that reference specs use Git-repository-root-relative paths. Do not commit machine-specific Lavish session URLs or permanent navigation manifests.

## Support collections

- `spec/prototypes/` contains user-requested behavioral references.
- `spec/history/` contains timestamped repository-evolution history written by archive.

**Rationale:** prototypes and chronological history are cross-cutting support collections rather than functional product domains.
