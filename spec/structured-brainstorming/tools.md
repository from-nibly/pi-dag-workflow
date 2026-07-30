# Brainstorm tool contract

> **Migration evidence:** These five legacy brainstorm tools are replaced by the six [project-model tools](../mixed-initiative-project-model/spec.md#production-brainstorming-integration). This document no longer describes production mutation authority.

## Ownership boundary

- **Agent instructions** own research, semantic judgment, tangent usefulness and overlap, probing-question quality, recommendations, response interpretation, contradiction detection, and applying prototype/spec context.
- **Tools** own structural integrity, typed IDs, defaults, explicit references, atomic mutation, derived state, renderer-neutral projections, and cleanup.
- **Users** own exploration focus and scope, prototype requests, ambiguity resolution, and planning authorization.
- **Optional renderers** own presentation controls and response transport.

**Rationale:** deterministic code should enforce invariants it can know, not pretend to understand product meaning.

## Production tools

### `dag_brainstorm_context`

Returns only a requested narrow view:

- orientation, including the current understanding proof;
- tangent frontier;
- active review, including the proof projected above review points;
- explicitly selected entities;
- promotion-ready decision summaries.

There is no accidental full-state or complete-document default.

### `dag_brainstorm_update`

Atomically records evidence, tangents, questions, proposals, corrections, merges, explicit lifecycle dispositions, and agent signals outside review resolution. When research or another non-review update materially changes the semantic model, the agent may replace Current understanding in the same call using explicit evidence or decision references.

### `dag_brainstorm_review`

Creates or replaces one renderer-neutral review packet from stored references and novel context. The first review supplies an Initial understanding proof grounded in seed/repository evidence; later reviews project the stored current proof above their points. It may atomically add discoveries needed by the packet. Its result may suggest ordinary Markdown or optional Lavish rendering.

### `dag_brainstorm_resolve_review`

Applies agent-interpreted semantic outcomes to ephemeral state and retires the packet. In the same call, the agent supplies the replacement Current understanding proof generated from those outcomes; the tool links it to the resolution and generated decisions. It can accept stored proposals, apply explicitly modified decisions, preserve unresolved substantive points, add follow-ups, and perform designed status transitions. Informational points never become unresolved questions. It retains only agent-interpreted semantic outcomes—not verbatim chat responses or unprocessed renderer submissions—and never edits canonical specs.

### `dag_brainstorm_promote`

Promotion uses a separate prepare → agent edits canonical Markdown → record flow. A promoted decision whose contract or rationale changes becomes promotion-ready again until a newer record links that revision to updated specs. See [Planning, promotion, and archive](planning-and-archive.md#promotion) for the lifecycle contract.

**Rationale:** state resolution is mechanical, while merging decisions into evolving Markdown requires agent judgment.

## User-command boundary

`/dag brainstorm` commands—not agent tools—own listing unfinished snapshots, asking resume versus new, selecting a state, initialization/loading, activating brainstorm runtime mode, and opening optional presentation surfaces. The legacy discovery prompt and `/dag grillme` workflow are replaced by this single structured workflow. See [Pi extension integration](pi-integration.md) for production command, activation, instruction, and renderer behavior.

## Mechanical inference

Agents provide semantic content and explicit references. Tools may generate or derive only:

- canonical typed IDs from optional short keys;
- timestamps and revision;
- operation-specific initial statuses;
- seed and reverse links from explicit IDs;
- generic review conventions such as `Other`;
- badges and counts;
- explicitly named lifecycle transitions.

Tools never infer semantic links, recommendation quality, user intent, rationale, contract prose, tangent equivalence, or product meaning from prose. Agent instructions silently prompt tangent scanning; tools validate only candidate identity, allowed status, required candidate context, and explicit references. They never require a candidate or a `no new tangents` record.

**Rationale:** reference traversal removes duplicated payload safely; prose interpretation would encode unreliable judgment.

## Output layers

1. **Datastore:** renderer-neutral JSON under `.ai/brainstorm/`.
2. **Model-facing content:** compact requested state, revision, changed IDs/counts, and likely-next-action reminders.
3. **Tool details/custom rows:** minimal metadata needed for compact Pi rendering or expansion.
4. **User surface:** agent-authored Markdown or an optional rendered artifact.

Complete state and HTML do not enter model context by default. Review mutation receipts do not echo prose the agent just supplied. Renderer calls return artifact/session references rather than document bodies.

Repeated presentation is component-driven. The optional whole-document renderer owns the standard shell: Current understanding, cards, checkbox/radio controls, navigation, badges, comments, turn-level actions, layout, spacing, and default styles. Content-token length is not the primary optimization target—boilerplate presentation tokens are.

Inside each question card, the context slot is a deliberate unrestricted escape hatch. The agent may choose Markdown; optional helpers for callouts, code, tables, comparisons, and diagrams; arbitrary HTML/CSS/scripts; interactive controls or prototypes; or a linked artifact. Helpers reduce routine boilerplate but never form a ceiling or a general page-description language. Custom context interaction may use renderer transport, while standard shell controls remain convenient defaults.

Tools do not require one context representation to project across all renderers or require a Markdown fallback for every rich section. The user and agent choose the interaction modality for the session/review. Agent-authored context is trusted local content, so fragment sanitization/isolation is not a tool invariant; ordinary layout diagnostics may still protect usability.

Next-action reminders are derived from structural state and are non-binding. Examples include presenting a review, resolving a review after response, researching selected tangents, or promoting selected decisions when useful.

## Serialization

JSON remains canonical storage and tool input. Compact domain-shaped outputs and narrow reads are the primary token optimization. TOON is not a required dependency; alternate output encodings may be benchmarked later.

**Rationale:** local TOON savings were modest for heterogeneous state, while removing duplicated semantics and broad projections produced much greater leverage.

## Validation and diagnostics

Every mutation validates schema, identities, statuses, and explicit references atomically. Understanding-proof validation is structural: nonempty body, Initial/Current lifecycle label, recognized source type, freshness lineage, and valid decision/evidence references. Agent instructions require causal reasoning rather than answer recap; behavioral fixtures evaluate that semantic quality. A separate human diagnostic command may inspect state, but routine agent flow does not need a standalone validation tool.

The state is single-writer and worktree-local. Revision indicates freshness but does not reject a mutation. No locks, optimistic merge, event replay, or cross-worktree protocol is required.
