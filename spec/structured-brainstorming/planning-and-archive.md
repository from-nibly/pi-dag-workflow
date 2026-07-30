# Planning, promotion, and archive boundaries

> **Migration evidence:** Promotion, archive, and model-unaware planning are superseded or deferred by the [mixed-initiative project model](../mixed-initiative-project-model/spec.md). Retain this document only for migration traceability.

## Lifecycle authority

The canonical authority split is defined in the [object model](object-model.md#authority-and-lifetime). Planning and chunking must remain possible after all brainstorm state is deleted.

**Rationale:** mutable working memory supports exploration; tracked specs provide stable implementation contracts.

## Promotion

Review resolution and promotion are separate:

1. `dag_brainstorm_resolve_review` updates ephemeral semantic state and retires a review packet.
2. `dag_brainstorm_promote prepare` selects settled decisions and likely target specs.
3. The agent reads and edits canonical Markdown with normal coding tools.
4. `dag_brainstorm_promote record` records decision-to-spec paths.

Promotion may happen whenever a coherent spec update is useful. It does not need to wait until the entire brainstorm ends, and resolving a review does not force immediate file churn.

## Planning authorization

Only the user starts `/dag plan`. The agent never decides that brainstorming is sufficiently complete.

If open questions or selectable tangents remain, the planning prompt lists them and asks for confirmation. If the user confirms, planning proceeds without another readiness gate.

Planning consumes canonical specs and repository context and writes disposable plan/chunk/DAG state under `.ai/`. Brainstorm state may inform the warning about unresolved exploration but is not itself an implementation contract.

Prototype handoff follows the [prototype policy](prototypes.md#planning-and-chunking).

**Rationale:** unresolved exploration is useful informed-consent context, not an agent-owned blocker.

## Archive

Archive performs no verification gate. The user invokes it only after implementation and review are satisfactory.

Before invoking archive, the agent/user workflow is responsible for promoting important durable contracts. Archive does not verify that responsibility. It synthesizes remaining `.ai` context into a timestamped tracked history document under `spec/history/`, then may clean ephemeral `.ai` state. Git and history documents preserve repository evolution; the brainstorm datastore does not maintain event sourcing or durable provenance.

**Rationale:** verification belongs to implementation/review workflows and human acceptance, not archival cleanup.
