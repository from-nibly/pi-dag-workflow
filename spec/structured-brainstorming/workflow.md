# Brainstorm workflow

> **Migration evidence:** This legacy promotion-oriented loop is superseded by the [mixed-initiative project model](../mixed-initiative-project-model/spec.md). Retain it only for interaction-design and migration traceability.

## Purpose

Structured brainstorming is a distinct discovery mode that preserves unresolved exploration, exposes research-worthy tangents, and turns explicit user decisions into durable specifications. It borrows domain-oriented spec conventions from OpenSpec but does not use OpenSpec tooling or change folders.

**Rationale:** ordinary chat loses pending branches easily, while a rigid requirements interview narrows the problem too early.

## Exploration loop

The operating loop is:

1. **Orient** from relevant specs, repository evidence, and active state.
2. **Diverge** across the user-selected tangent set.
3. **Research or probe** in a bounded coherent burst.
4. **Synthesize** evidence, proposals, and genuine uncertainties.
5. **Review** several contextually related decisions together.
6. **Resolve** explicit semantic outcomes and preserve unresolved questions.
7. Repeat, change tangent focus, or promote settled contracts.

The agent returns for review when a coherent cluster is decision-ready, evidence changes direction, user preference blocks progress, or the user requests a checkpoint. There is no fixed section, tool-call, or time quota.

**Rationale:** bounded bursts preserve momentum and deep user focus without allowing a long autonomous sweep to drift from user intent.

## Before asking the user

The agent exhausts relevant available information before presenting a decision. It reads repository specs, source, tests, configuration, and prior state; it performs web research when the user's framing shows uncertainty, unfamiliarity, conflicting possibilities, or an explicit research suggestion.

User attention is reserved for judgments that can materially change direction, scope, purpose, methodology, style, behavior, contract, or research direction, or for ambiguity only the user can resolve.

The agent generally does not ask about:

- facts available in the repository;
- documented behavior of an external tool or standard that research can establish;
- IDs, timestamps, links, statuses, and other mechanical state;
- routine validation commands or observed test results;
- cheap, reversible implementation details already governed by selected methodology/style and repository conventions;
- uncertainty whose plausible answers do not change the work;
- confirmation of an explicit correction or decision.

**Rationale:** software admits many valid product and implementation directions, so the user must set intent; objective evidence and routine mechanics should not consume the same scarce attention.

## Tangents

A tangent is an area requiring research that may generate or reshape open questions. The initial tangent set is never presumed complete.

The agent silently watches for useful emergent tangents throughout research, prototyping, feedback resolution, and ordinary reasoning. New candidates include:

- why the area appeared;
- what needs research;
- how it connects to current scope.

Candidates are recorded when noticed and shown at the next natural review checkpoint, unless they invalidate current reasoning and need immediate attention. After the checkpoint, untouched Proposed tangents become Tracked. Removing one from accepted scope requires an explicit user **Out of scope** action. That exclusion applies only to the current brainstorm: other brainstorms assess their own scope, and the user may explicitly restore the tangent here.

Before adding a tangent, the agent compares its meaning against Proposed, Tracked, Active, Deferred, Closed, merged, and Out-of-scope branches. It reuses, links, or merges substantial overlap and asks the user when a meaningful distinction remains unclear.

Tangent statuses are:

- **Proposed** — agent-suggested and awaiting the next checkpoint;
- **Tracked** — retained for possible exploration;
- **Active** — part of the current user-selected focus;
- **Deferred** — in scope but intentionally postponed;
- **Out of scope** — excluded from this brainstorm by the user;
- **Closed** — explored and settled or no longer useful.

The default flat frontier shows Proposed, Tracked, Active, and Deferred tangents. Out-of-scope and Closed tangents remain inspectable but do not clutter selection.

The user may select any number or combination of selectable tangents and may add, merge, defer, close, or explicitly mark tangents Out of scope. Selection establishes focus; it is not authorization, has no expiration, and needs no renewal. The agent never selects focus automatically or silently adds an unselected tangent.

If the agent finds no new useful tangents, it says so and shows the updated frontier. It does not perform a forced completeness ritual or invent branches to satisfy a quota.

**Rationale:** continuous discovery prevents the initial map from becoming an accidental boundary, while explicit user scope keeps exploration under product-direction control.

## Tangent badges

Cards may show fixed semantic flags such as **Foundation**, **Risk**, **Needs clarity**, **Contradiction**, **Integration**, and **Prototype candidate**, plus factual badges such as open-question count, Deferred, Partially settled, and New.

Badges are quick agent-owned observations. Users do not edit or approve them and may ignore them. They never constrain selection or become specification authority.

## Questions, review points, and decisions

- A **question** is durable unresolved uncertainty linked to one or more tangents.
- A **review point** is disposable presentation of context, evidence, a clear question, and optional choices.
- A **proposal** is a candidate answer to one or more questions.
- A **decision** records an explicit semantic outcome, contract, and rationale.

A review may address many related questions. There is no one-question or one-root-decision limit. Each substantive point uses concise freeform context to explain what triggered it, current evidence/understanding, the precise remaining uncertainty, and why the answer matters. Obvious ingredients may be omitted, and existing specs are linked rather than repeated.

The complete brainstorm-turn document may use a consistent optional rich renderer whose shell owns Current understanding, navigation, question cards, badges, standard choices, comments, and turn-level actions. Within each question card, however, the freeform context slot is agent-controlled: it may use Markdown, semantic helpers, arbitrary HTML/CSS/scripts, an interactive example, or a linked/generated artifact. Standard shell controls remain available even when context adds its own interaction.

Structured options appear only when alternatives are plausible and materially distinct. There is no required option count. If the option space is immature, the agent asks an open question or proposes more research rather than inventing choices. `Other` remains available whenever structured options exist.

**Rationale:** artificial options turn brainstorming into a test-taking exercise where the user searches for the agent's intended answer. Repeated low-value choices also create recommendation fatigue. Every presented question should remain a credible opportunity for user judgment.

Recommendations are optional. When present, the selected option is visibly marked `(Recommended)`, its rationale is explained, and concrete alternatives remain available alongside `Other`. The agent stays exploratory when evidence is immature rather than manufacturing a recommendation.

Ignored review points remain unresolved. The agent applies clear outcomes while ambiguous or contradictory responses stay open. Settled points leave the active surface immediately and are not silently resurrected. New implications become new questions; a prior question is reopened only when evidence genuinely challenges its resolution.

When the user rejects a premise, the agent first compares the correction with active decisions and prior explicit user direction. If it corrects only an unsupported agent assumption, the agent updates state and retires the flawed point without a confirmation-only turn. If it conflicts with something the user previously established, the agent does not silently overwrite either statement: it creates or reopens a contradiction question and presents the conflict with relevant context in the next normal brainstorm review. It creates another follow-up only when a distinct unresolved consequence remains.

Every brainstorm document begins with an understanding proof. Before the first resolved review it is labeled **Initial understanding** and grounds the agent's model in the user seed, repository/spec research, known constraints, and explicit uncertainty so flawed initial framing can be corrected early. After meaningful semantic change it is labeled **Current understanding** and explains the deeper reasoning and causal model behind current decisions. It is neither an answer recap nor a request for confirmation.

The proof refreshes when decisions, corrections, contradiction resolutions, or material research change the semantic model—not for acknowledgments, navigation, or unchanged conversation. Review resolution authors the next proof in the same reasoning pass as semantic outcomes. Research-driven updates may also replace it. The next review and resume orientation project the stored proof at the top.

Only one mutable proof is retained. It records whether it came from initial orientation, review resolution, or a semantic update and links relevant decisions/evidence. Superseded proofs are discarded rather than becoming another history surface. User corrections follow the normal correction and contradiction behavior above.

**Rationale:** broad contextual reviews support sparse, high-quality feedback, but silence and ambiguity must never become fabricated decisions.

## Renderer-neutral review

Review packets contain no Lavish-specific lifecycle state. At session/review construction time, the agent chooses ordinary Markdown, an optional rich renderer, or a linked artifact according to user preference and explanatory need. Lavish is never required by the workflow, but a rich question-context section does not require a parallel decision-complete Markdown fallback. In a Markdown-only interaction, the agent instead authors suitable Markdown, a crude textual mockup, or a link to a separately opened artifact.

Agent-authored question context is trusted local content written for the user; the workflow does not require sanitization, nested sandboxing, Shadow DOM, CSS scoping, or a security allowlist. Renderers may still apply layout containment, namespacing, and diagnostics for usability without restricting expressive capability.

A renderer may collect selections, annotations, comments, and submission metadata, including through custom interactive context. Those unprocessed submissions are disposable. The agent interprets chat or renderer responses and sends only semantic outcomes to review resolution; verbatim responses are not persisted.

AXI-like next suggestions may recommend Markdown presentation or optional Lavish rendering. They are guidance, not workflow gates.

## Resume and recovery

When unfinished brainstorm state exists, `/dag brainstorm` always asks whether to resume or start a new brainstorm. If several states exist and resume is chosen, it asks which one.

Resume shows a compact orientation:

- current selected focus;
- active review, if any;
- open questions;
- flat tangent frontier with badges;
- latest settled changes.

Before substantive work, the agent refreshes repository/spec evidence for the user-selected focus rather than scanning the entire repository for drift.

If ephemeral state is missing or unreadable, the agent explains the loss, loads canonical specs and reusable project understanding, asks what to resume, and starts a fresh snapshot. It does not attempt event replay or forensic repair.

**Rationale:** the current semantic snapshot—not retired conversation history—is the useful continuity boundary.
