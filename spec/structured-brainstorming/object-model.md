# Brainstorm object model

> **Migration evidence:** This legacy ephemeral-state contract is superseded by the [mixed-initiative project model](../mixed-initiative-project-model/spec.md). Retain it for prototype and migration traceability; it will not govern after cutover.

Status: historical pre-production prototype contract.

## Authority and lifetime

Brainstorm state is ephemeral working memory for one agent in one worktree. It retains unresolved exploration while attention moves between topics. It is not canonical, is not tracked in Git, and is removed by archive after useful contracts are promoted.

- Brainstorm JSON owns active workflow objects.
- `.ai/project.md` owns concise cross-workflow repository understanding.
- `spec/` owns canonical product behavior.

The datastore is one mutable snapshot. It has no event log, replay guarantee, optimistic revision rejection, lock, merge protocol, branch attachment, or cross-worktree coordination. Revision identifies render freshness only.

**Rationale:** one agent and worktree provide the required isolation; concurrency and history machinery would add complexity without an accepted use case.

## Active objects

- `neighborhoods` — broad contexts connecting related exploration.
- `tangents` — research-worthy branches and their lifecycle status.
- `questions` — durable unresolved uncertainties linked to tangents and evidence.
- `evidence` — currently useful repository, research, experiment, prototype, or user observations.
- `proposals` — candidate answers linked to questions.
- `probes` — research, diagrams, spikes, or requested prototypes that reduce uncertainty.
- `decisions` — explicit semantic outcomes with contract and rationale.
- `reviews` — disposable renderer-neutral packets containing contextual discussion points.
- `promotions` — records linking decisions to canonical spec paths.
- `currentUnderstanding` — one mutable agent-authored reasoning proof used by orientation and review projections. It contains a freeform body, an Initial/Current label, source type, relevant decision/evidence references, and update time.

The datastore retains only agent-interpreted semantic outcomes, never verbatim chat responses or unprocessed renderer submissions. Superseded understanding proofs are not retained as history.

## Relationships

```text
Neighborhood
  └── Tangent
        ├── Evidence
        └── Question
              └── Proposal
                    └── Review point
                          └── Decision
                                └── Promotion → spec path
```

Relationships may be many-to-many. Links requiring meaning are explicit agent-provided references; tools never infer them from prose.

## Lifecycle invariants

1. Questions survive after the review points that presented them are removed.
2. Resolving a review applies agent-interpreted outcomes, then deletes the packet.
3. Ignored or ambiguous points remain unresolved unless the agent sends an explicit semantic outcome.
4. A decision resolves or disposes one or more questions and records both contract and rationale.
5. Contradictions are open questions with explicit conflicting references.
6. Settled review points do not remain on the active surface or silently reappear.
7. Incorrect or stale ephemeral objects may be corrected or hard-deleted; historical reconstruction is not a goal.
8. Derived badges, counts, deltas, Markdown, and optional rich views regenerate from state and are never authoritative.
9. Unsupported ephemeral schemas may be discarded or minimally upgraded; no durable migration framework is required.
10. The workflow is responsible for promoting important decisions before archive so specs stand alone without brainstorm state; archive itself does not verify or gate this responsibility.
11. Initial review creation requires an Initial understanding proof; later review and resume projections use the one current proof.
12. A semantic review resolution supplies a replacement proof in the same tool call. Semantic updates may replace it with explicit decision/evidence references.
13. Tools validate proof presence, source type, freshness lineage, and references; they do not judge whether its prose demonstrates comprehension.

## Related contracts

- [Workflow and interaction](workflow.md)
- [Tool contract](tools.md)
- [Prototype policy](prototypes.md)
- [Specification layout](specification-layout.md)
- [Planning, promotion, and archive](planning-and-archive.md)

## Runnable reference

The runnable prototype lives at [spec/prototypes/object-tools/](../prototypes/object-tools/README.md). It dogfoods schema v2 and the five accepted tool boundaries:

- `dag_brainstorm_context`
- `dag_brainstorm_update`
- `dag_brainstorm_review`
- `dag_brainstorm_resolve_review`
- `dag_brainstorm_promote`

Its CLI-only `start` harness represents the user-command new/resume boundary. It remains behavioral evidence rather than final Pi registration or rendering code.
